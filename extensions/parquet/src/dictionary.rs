//! Dictionary-encoded column chunks: decode the dictionary page once, gather
//! every row from it, and answer the pushed predicates per dictionary entry.
//!
//! After DuckDB's `extension/parquet/decoder/dictionary_decoder.cpp`
//! (`DictionaryDecoder`): `InitializeDictionary` reads the dictionary page into
//! one reusable vector and evaluates a pushed filter once per entry
//! (`filter_result`), `InitializePage` opens a page's RLE/bit-packed ids, and
//! reads are selections over the shared dictionary rather than a copy of a
//! value per row. The engine has no dictionary vector, so the gather happens
//! here, at the scan's boundary, and emits ordinary vectors.

use std::sync::Arc;

use parquet::basic::Encoding;
use parquet::column::page::Page;
use parquet::file::reader::RowGroupReader;
use pinkdb::attached::{PruneOp, PrunePredicate};
use pinkdb::strings::StrVec;
use pinkdb::{DataType, PinkDbError, Result, Value, Vector, VectorData};

use crate::scan::ChunkLens;
use crate::schema_map::FileColumn;

/// Whether a column chunk may take the dictionary path: the gather builds a
/// string vector, so only a plain `BYTE_ARRAY` column read as VARCHAR does.
pub(crate) fn decodes(column: &FileColumn) -> bool {
    column.physical == parquet::basic::Type::BYTE_ARRAY
        && column.ty == DataType::Varchar
        && column.shred.is_none()
        && column.leaves == 1
}

/// One column chunk's dictionary and the id of every row.
pub(crate) struct Dictionary {
    /// The chunk's distinct values, decoded once (UTF-8 checked per entry).
    entries: StrVec,
    /// One entry per chunk of the row group, in chunk order.
    chunks: Vec<Ids>,
}

/// The rows of one chunk: every row's dictionary id and its validity.
struct Ids {
    /// `ids[row]` is row's entry — meaningless where the row is NULL.
    ids: Vec<u32>,
    validity: Vec<u64>,
    nulls: usize,
}

impl Ids {
    #[inline]
    fn has(&self, row: usize) -> bool {
        self.validity[row / 64] >> (row % 64) & 1 == 1
    }
}

impl Dictionary {
    /// Read `column`'s pages if every data page is dictionary-encoded, into a
    /// dictionary and one id per row of `lens` (whose lengths stay an
    /// iterator: they are sized from a footer's row count). `None` means the chunk is not
    /// (or not only) dictionary-encoded — a PLAIN page in the same chunk, a
    /// missing dictionary page, another encoding — and the caller reads the
    /// chunk with the typed reader instead, which keeps today's errors and
    /// value decoding for those files.
    pub(crate) fn read(
        row_group: &dyn RowGroupReader,
        column: &FileColumn,
        lens: ChunkLens,
    ) -> Result<Option<Dictionary>> {
        if !decodes(column) || lens.rows() == 0 {
            return Ok(None);
        }
        // A chunk whose writer fell back to PLAIN pages partway (a dictionary
        // that outgrew its page, as for high-cardinality text) would be read
        // here up to its first PLAIN page and then decoded again whole by the
        // caller: twice the decompression for no gain. The footer's page
        // encoding statistics say so up front; without them the pages decide.
        let meta = row_group.metadata().column(column.leaf);
        if meta.page_encoding_stats_mask().is_some_and(|mask| {
            !(mask.is_only(Encoding::RLE_DICTIONARY) || mask.is_only(Encoding::PLAIN_DICTIONARY))
        }) {
            return Ok(None);
        }
        // A footer's row count is untrusted: `Rows` grows per page, so this
        // only decides when to stop reading pages.
        let rows = lens.rows();
        let mut pages = row_group.get_column_page_reader(column.leaf).map_err(|e| {
            PinkDbError::Execution(format!(
                "cannot read column \"{}\" (leaf {}): {e}",
                column.name, column.leaf
            ))
        })?;
        let mut entries: Option<StrVec> = None;
        let mut decoded = Rows::new();
        while decoded.len() < rows {
            let Some(page) = pages.get_next_page().map_err(|e| decode_error(column, e))? else {
                break;
            };
            match page {
                Page::DictionaryPage {
                    buf,
                    num_values,
                    encoding,
                    ..
                } => {
                    // A second dictionary or a data page before it is not the
                    // plain shape this path decodes.
                    if entries.is_some() || !decoded.is_empty() {
                        return Ok(None);
                    }
                    if encoding != Encoding::PLAIN {
                        return Ok(None);
                    }
                    entries = Some(dictionary_page(&buf, num_values as usize, column)?);
                }
                Page::DataPage {
                    buf,
                    num_values,
                    encoding,
                    ..
                } => {
                    let Some(entries) = &entries else {
                        return Ok(None);
                    };
                    if !is_dictionary(encoding) {
                        return Ok(None);
                    }
                    let buf = &buf[..];
                    let (levels, values) = match column.optional {
                        true => match buf.get(0..4) {
                            Some(len) => {
                                let len = u32::from_le_bytes(len.try_into().expect("four bytes"))
                                    as usize;
                                let (levels, values) =
                                    buf[4..].split_at_checked(len).ok_or_else(|| {
                                        decode_error(
                                            column,
                                            "the data page ended inside its definition levels",
                                        )
                                    })?;
                                (levels, values)
                            }
                            None => {
                                return Err(decode_error(
                                    column,
                                    "the data page ended inside its definition levels",
                                ));
                            }
                        },
                        false => (&buf[..0], buf),
                    };
                    decoded.page(levels, values, num_values as usize, entries.len(), column)?;
                }
                Page::DataPageV2 {
                    buf,
                    num_values,
                    encoding,
                    def_levels_byte_len,
                    rep_levels_byte_len,
                    ..
                } => {
                    let Some(entries) = &entries else {
                        return Ok(None);
                    };
                    if !is_dictionary(encoding) {
                        return Ok(None);
                    }
                    let (levels, values) = buf[..]
                        .split_at_checked(rep_levels_byte_len as usize)
                        .and_then(|(_, rest)| rest.split_at_checked(def_levels_byte_len as usize))
                        .ok_or_else(|| {
                            decode_error(column, "the data page ended inside its levels")
                        })?;
                    decoded.page(levels, values, num_values as usize, entries.len(), column)?;
                }
            }
        }
        if decoded.len() < rows {
            return Err(decode_error(
                column,
                format!("ended after {} of {rows} rows", decoded.len()),
            ));
        }
        let Some(entries) = entries else {
            return Ok(None);
        };
        Ok(Some(Dictionary {
            entries,
            chunks: decoded.split(lens),
        }))
    }

    /// The rows of every chunk the pushed `filters` keep, or `None` when a
    /// filter cannot be settled from the dictionary or drops no row (a
    /// selection of every row would cost a gather and save nothing).
    ///
    /// A filter is evaluated once per entry, like DuckDB's `filter_result`, so
    /// a chunk's selection is one lookup per row. A NULL row passes only an
    /// `IS NULL` test: a comparison and an `IN` list keep rows that are
    /// present, and a NULL *in* an `IN` list matches nothing.
    pub(crate) fn kept_rows(&self, filters: &[&PrunePredicate]) -> Option<Vec<Vec<u32>>> {
        // Per entry: whether every comparison holds for it.
        let mut entries = vec![true; self.entries.len()];
        // `IS [NOT] NULL` cannot be decided per entry (`true` = IS NULL).
        let mut null_tests: Vec<bool> = Vec::new();
        let mut compares = false;
        for filter in filters {
            match filter.op {
                PruneOp::IsNull => null_tests.push(true),
                PruneOp::IsNotNull => null_tests.push(false),
                PruneOp::In => {
                    compares = true;
                    let mut hit = vec![false; entries.len()];
                    for value in &filter.values {
                        let literal = match value {
                            Value::Varchar(literal) => literal.as_str(),
                            _ if value.is_null() => continue,
                            _ => return None,
                        };
                        for (entry, hit) in hit.iter_mut().enumerate() {
                            *hit |= self.entry(entry) == literal;
                        }
                    }
                    for (keep, hit) in entries.iter_mut().zip(hit) {
                        *keep &= hit;
                    }
                }
                op => {
                    compares = true;
                    let Some(Value::Varchar(literal)) = filter.value.as_ref() else {
                        return None;
                    };
                    for (entry, keep) in entries.iter_mut().enumerate() {
                        let order = self.entry(entry).cmp(literal.as_str());
                        *keep &= match op {
                            PruneOp::Eq => order.is_eq(),
                            PruneOp::NotEq => !order.is_eq(),
                            PruneOp::Lt => order.is_lt(),
                            PruneOp::LtEq => !order.is_gt(),
                            PruneOp::Gt => order.is_gt(),
                            PruneOp::GtEq => !order.is_lt(),
                            _ => return None,
                        };
                    }
                }
            }
        }
        let mut dropped = false;
        let mut kept = Vec::with_capacity(self.chunks.len());
        for ids in &self.chunks {
            let mut rows: Vec<u32> = Vec::with_capacity(ids.ids.len());
            for row in 0..ids.ids.len() {
                let has = ids.has(row);
                let mut keep = !compares || (has && entries[ids.ids[row] as usize]);
                for &is_null in &null_tests {
                    keep &= if is_null { !has } else { has };
                }
                if keep {
                    rows.push(row as u32);
                }
            }
            dropped |= rows.len() < ids.ids.len();
            kept.push(rows);
        }
        dropped.then_some(kept)
    }

    /// The chunk vectors, gathering `kept[i]`'s rows of chunk `i` (`None`
    /// keeps every row). The strings are views into the dictionary: a gathered
    /// row copies a view, never bytes.
    pub(crate) fn gather(&self, kept: Option<&[Vec<u32>]>) -> Result<Vec<Vector>> {
        if self.entries.is_empty() {
            // A dictionary page with no entry means every row is NULL (the
            // writer only read data pages' NULLs); anything else is corrupt.
            let mut out = Vec::with_capacity(self.chunks.len());
            for (index, ids) in self.chunks.iter().enumerate() {
                let n = kept.map_or(ids.ids.len(), |kept| kept[index].len());
                if ids.nulls < ids.ids.len() {
                    return Err(self.out_of_bounds(0));
                }
                out.push(Vector::from_raw(
                    VectorData::VarChar(StrVec::repeat("", n)),
                    vec![0u64; n.div_ceil(64)],
                ));
            }
            return Ok(out);
        }
        let mut out = Vec::with_capacity(self.chunks.len());
        for (index, ids) in self.chunks.iter().enumerate() {
            let (picks, validity) = match kept {
                Some(kept) => self.picked(ids, &kept[index])?,
                None => self.all(ids)?,
            };
            let data = VectorData::VarChar(self.entries.take(picks));
            out.push(Vector::from_raw(data, validity));
        }
        Ok(out)
    }

    /// The picks and validity of every row of `ids`.
    fn all(&self, ids: &Ids) -> Result<(Vec<usize>, Vec<u64>)> {
        let mut picks = Vec::with_capacity(ids.ids.len());
        for &id in &ids.ids {
            picks.push(self.entry_index(id)?);
        }
        let validity = match ids.nulls {
            0 => vec![u64::MAX; ids.ids.len().div_ceil(64)],
            _ => ids.validity.clone(),
        };
        Ok((picks, validity))
    }

    /// The picks and validity of `rows` (ascending) of `ids`.
    fn picked(&self, ids: &Ids, rows: &[u32]) -> Result<(Vec<usize>, Vec<u64>)> {
        let mut picks = Vec::with_capacity(rows.len());
        if ids.nulls == 0 {
            for &row in rows {
                picks.push(self.entry_index(ids.ids[row as usize])?);
            }
            return Ok((picks, vec![u64::MAX; rows.len().div_ceil(64)]));
        }
        let mut validity = vec![0u64; rows.len().div_ceil(64)];
        for (out, &row) in rows.iter().enumerate() {
            let row = row as usize;
            if !ids.has(row) {
                // The pick is unmaterialized: the row is NULL by validity.
                picks.push(0);
                continue;
            }
            validity[out / 64] |= 1u64 << (out % 64);
            picks.push(self.entry_index(ids.ids[row])?);
        }
        Ok((picks, validity))
    }

    fn entry(&self, entry: usize) -> &str {
        self.entries.get(entry)
    }

    fn entry_index(&self, id: u32) -> Result<usize> {
        usize::try_from(id)
            .ok()
            .filter(|id| *id < self.entries.len())
            .ok_or_else(|| self.out_of_bounds(id))
    }

    fn out_of_bounds(&self, id: u32) -> PinkDbError {
        PinkDbError::Execution(format!(
            "dictionary index out of bounds: the dictionary has {} entries but the index is {id}",
            self.entries.len()
        ))
    }
}

/// The rows decoded so far, appended page by page.
struct Rows {
    ids: Vec<u32>,
    validity: Vec<u64>,
    nulls: usize,
    /// One page's ids and presence, reused across pages.
    page_ids: Vec<u32>,
    present: Vec<u32>,
}

impl Rows {
    fn new() -> Self {
        // Nothing is sized from the footer's row count: a page grows these.
        Self {
            ids: Vec::new(),
            validity: Vec::new(),
            nulls: 0,
            page_ids: Vec::new(),
            present: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.ids.len()
    }

    fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Append one data page: `rows` values, its `levels` (empty for a required
    /// column) and its dictionary-id stream, whose length must be the number
    /// of present values.
    fn page(
        &mut self,
        levels: &[u8],
        values: &[u8],
        rows: usize,
        entries: usize,
        column: &FileColumn,
    ) -> Result<()> {
        self.present.clear();
        if !levels.is_empty() {
            decode_ids(levels, 1, rows, &mut self.present, None, column)?;
        }
        let present = match self.present.is_empty() {
            true => rows,
            false => self.present.iter().filter(|&&level| level != 0).count(),
        };
        let Some((&width, ids)) = values.split_first() else {
            return Err(decode_error(column, "the data page has no dictionary ids"));
        };
        if width > 32 {
            return Err(decode_error(
                column,
                format!("the dictionary id width {width} is out of range"),
            ));
        }
        self.page_ids.clear();
        decode_ids(
            ids,
            width as usize,
            present,
            &mut self.page_ids,
            Some(entries),
            column,
        )?;
        let first = self.ids.len();
        let end = first + rows;
        if self.validity.len() < end.div_ceil(64) {
            self.validity.resize(end.div_ceil(64), 0);
        }
        if self.present.is_empty() || self.present.iter().all(|&level| level != 0) {
            self.ids.extend_from_slice(&self.page_ids[..present]);
            for at in first..end {
                self.validity[at / 64] |= 1u64 << (at % 64);
            }
            return Ok(());
        }
        self.ids.resize(end, 0);
        let mut next = 0usize;
        for row in 0..rows {
            let at = first + row;
            if self.present[row] != 0 {
                self.ids[at] = self.page_ids[next];
                next += 1;
                self.validity[at / 64] |= 1u64 << (at % 64);
            } else {
                self.nulls += 1;
            }
        }
        Ok(())
    }

    /// Split the flat rows into `lens`' chunks.
    fn split(self, lens: ChunkLens) -> Vec<Ids> {
        let mut chunks = Vec::with_capacity(lens.chunks().min(pinkdb::CHUNK_SIZE));
        let mut start = 0usize;
        for n in lens {
            let validity = window(&self.validity, start, n);
            chunks.push(Ids {
                ids: self.ids[start..start + n].to_vec(),
                nulls: n - ones(&validity, n),
                validity,
            });
            start += n;
        }
        chunks
    }
}

/// The dictionary page's values: a PLAIN `BYTE_ARRAY` sequence of 4-byte
/// little-endian lengths and bytes, checked as UTF-8 once per entry. The
/// values are copied, without their length prefixes, into one shared `str`
/// buffer the vector's views point into.
fn dictionary_page(buf: &[u8], entries: usize, column: &FileColumn) -> Result<StrVec> {
    // A page header's entry count is untrusted too: each entry needs 4 bytes
    // of length prefix, so the page bounds the hint.
    let mut ranges = Vec::with_capacity(entries.min(buf.len() / 4));
    let mut text = String::with_capacity(buf.len());
    let mut at = 0usize;
    for _ in 0..entries {
        let len = buf
            .get(at..at + 4)
            .ok_or_else(|| decode_error(column, "the dictionary page ended inside a value"))?;
        let len = u32::from_le_bytes(len.try_into().expect("four bytes")) as usize;
        at += 4;
        let bytes = buf
            .get(at..at + len)
            .ok_or_else(|| decode_error(column, "the dictionary page ended inside a value"))?;
        let value = std::str::from_utf8(bytes)
            .map_err(|_| decode_error(column, "the bytes are not valid UTF-8"))?;
        ranges.push((text.len(), len));
        text.push_str(value);
        at += len;
    }
    let buffer: Arc<str> = Arc::from(text);
    Ok(StrVec::over_buffer(buffer, ranges.into_iter()))
}

/// Decode `count` values of `width` bits from an RLE/bit-packed stream (`data`
/// starts at a run header) into `out`, checking every value against `bounds`
/// when the values are dictionary ids.
///
/// The parquet spec's hybrid encoding: a ULEB128 header, a repeated value when
/// its low bit is 0, a group of 8 values packed LSB-first otherwise. A final
/// bit-packed group may be truncated (the writer pads it to 8 values, a
/// reader may deliver fewer bytes).
fn decode_ids(
    data: &[u8],
    width: usize,
    count: usize,
    out: &mut Vec<u32>,
    bounds: Option<usize>,
    column: &FileColumn,
) -> Result<()> {
    if width == 0 {
        if let Some(entries) = bounds
            && count > 0
            && entries == 0
        {
            return Err(out_of_bounds(entries, 0));
        }
        out.resize(count, 0);
        return Ok(());
    }
    let byte_width = width.div_ceil(8);
    let mut at = 0usize;
    while out.len() < count {
        let Some(header) = uleb128(data, &mut at) else {
            return Err(short_stream(column, out.len(), count));
        };
        let run = (header >> 1) as usize;
        if header & 1 == 0 {
            let Some(value) = data.get(at..at + byte_width) else {
                return Err(short_stream(column, out.len(), count));
            };
            at += byte_width;
            let mut id = 0u32;
            for (i, byte) in value.iter().enumerate() {
                id |= (*byte as u32) << (8 * i);
            }
            if let Some(entries) = bounds
                && id as usize >= entries
            {
                return Err(out_of_bounds(entries, id));
            }
            out.resize((out.len() + run).min(count), id);
        } else {
            // `run` groups of 8; a short final group is the end of the stream.
            let take = (run * 8).min(count - out.len());
            let bytes = data.len().saturating_sub(at);
            let have = (bytes * 8) / width;
            let take = take.min(have);
            for value in 0..take {
                let bit = value * width;
                let mut id = 0u32;
                for shift in 0..width {
                    let index = bit + shift;
                    let byte = data[at + index / 8];
                    id |= ((byte >> (index % 8)) as u32 & 1) << shift;
                }
                if let Some(entries) = bounds
                    && id as usize >= entries
                {
                    return Err(out_of_bounds(entries, id));
                }
                out.push(id);
            }
            at += (run * width).min(bytes);
        }
    }
    Ok(())
}

/// The ULEB128 varint at `at`, advancing it; `None` at the end of the data.
fn uleb128(data: &[u8], at: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    while let Some(&byte) = data.get(*at) {
        *at += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

/// `n` validity bits of `words` starting at bit `start`, as words of their own.
fn window(words: &[u64], start: usize, n: usize) -> Vec<u64> {
    let word = start / 64;
    let shift = start % 64;
    let mut out = vec![0u64; n.div_ceil(64)];
    for (i, out) in out.iter_mut().enumerate() {
        let low = words.get(word + i).copied().unwrap_or(0);
        if shift == 0 {
            *out = low;
        } else {
            let high = words.get(word + i + 1).copied().unwrap_or(0);
            *out = (low >> shift) | (high << (64 - shift));
        }
    }
    out
}

/// How many of the first `n` bits of `words` are set.
fn ones(words: &[u64], n: usize) -> usize {
    let mut set = 0usize;
    for (i, &word) in words.iter().enumerate() {
        let width = (n - i * 64).min(64);
        let bits = match width {
            64 => u64::MAX,
            _ => (1u64 << width) - 1,
        };
        set += (word & bits).count_ones() as usize;
    }
    set
}

fn is_dictionary(encoding: Encoding) -> bool {
    matches!(
        encoding,
        Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY
    )
}

fn out_of_bounds(entries: usize, id: u32) -> PinkDbError {
    PinkDbError::Execution(format!(
        "dictionary index out of bounds: the dictionary has {entries} entries but the index is {id}"
    ))
}

fn short_stream(column: &FileColumn, got: usize, count: usize) -> PinkDbError {
    decode_error(
        column,
        format!("the dictionary ids ended after {got} of {count} values"),
    )
}

fn decode_error(column: &FileColumn, why: impl std::fmt::Display) -> PinkDbError {
    PinkDbError::Execution(format!("column \"{}\": {why}", column.name))
}
