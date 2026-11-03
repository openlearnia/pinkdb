//! The scan itself: statistics-based row-group pruning, decoding, and the
//! chunk assembly the engine consumes.
//!
//! ## Pushdown
//!
//! The engine hands the scan the statement's projection (a column set) and
//! the `WHERE` conjuncts it could prove over the scan. Both are used:
//!
//! A file resolved from a [`crate::ranged::RangeSource`] (a remote object or a
//! lake data file) is read through parquet's `ChunkReader` over that source;
//! the local/remote split below only decides how the bytes arrive.
//!
//! - **Projection**: only the named output columns are decoded. Every other
//!   column is a zero-length placeholder in the emitted chunks, exactly like
//!   an attached-lake scan's absent columns.
//! - **Filter**: for each row group, a column chunk's min/max and NULL count
//!   decide whether any row can match. `col = v` prunes when `v` lies outside
//!   `[min, max]`; `<`, `<=`, `>`, `>=` prune against the bound the
//!   comparison needs; `IS NULL` prunes when the null count is 0; `IS NOT
//!   NULL` when it covers every row. `<>` prunes only when `min == max == v`
//!   (and never for float columns: NaN). A column whose statistics are not in
//!   the engine's order (unsigned integers) or missing contributes no
//!   pruning — pruning is never allowed to drop a row the filter would keep,
//!   and a wrong bound could.
//! - **File** level: a predicate over a *constant* column (a hive key, the
//!   file name) is evaluated per file, so `WHERE region = 'eu'` touches only
//!   the matching files.
//!
//! `filters` are hints: the engine re-applies the full predicate above the
//! scan, so ignoring one (or all) changes only how much is decoded. The
//! counters ([`TableScanOutput`]) report what actually happened.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use parquet::bloom_filter::Sbbf;
use parquet::column::page::PageReader;
use parquet::column::reader::{ColumnReader, get_typed_column_reader};
use parquet::data_type::{
    BoolType, ByteArray, ByteArrayType, DoubleType, FixedLenByteArray, FixedLenByteArrayType,
    FloatType, Int32Type, Int64Type, Int96Type,
};
use parquet::file::metadata::page_index::RowGroupPageIndex;
use parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData,
};
use parquet::file::page_index::offset_index::PageLocation;
use parquet::file::properties::{ReaderProperties, ReaderPropertiesPtr};
use parquet::file::reader::{ChunkReader, FileReader, RowGroupReader, SerializedFileReader};
use parquet::file::serialized_reader::{SerializedPageReader, SerializedRowGroupReader};
use parquet::record::reader::RowIter;
use parquet::schema::types::Type as SchemaType;
use pinkdb::attached::{PruneOp, PrunePredicate};
use pinkdb::strings::StrVec;
use pinkdb::types::Interval;
use pinkdb::{
    CHUNK_SIZE, DataChunk, DataType, PinkDbError, Result, TableScanOutput, Value, Vector,
    VectorData,
};
use rayon::prelude::*;

use crate::dictionary::Dictionary;
use crate::ranged::RemoteReadStats;
use crate::resolve::{FileResolved, OutKind, Resolved, convert_value, file_constant_value};
use crate::schema_map::FileColumn;

/// Execute one resolved scan.
///
/// Returns the engine's chunks/counters plus the remote transport counters
/// for this scan (all zero for a local scan).
pub fn scan(
    resolved: &Resolved,
    projection: Option<&[usize]>,
    filters: &[PrunePredicate],
) -> Result<(TableScanOutput, RemoteReadStats)> {
    let mut chunks = Vec::new();
    let (out, stats) = scan_into(resolved, projection, filters, false, &mut |chunk| {
        chunks.push(chunk);
        Ok(())
    })?;
    Ok((TableScanOutput { chunks, ..out }, stats))
}

/// As [`scan`], but every local file is decoded on the calling thread.
///
/// The worker pool only changes how fast the file is read: rows, values and
/// chunk boundaries are the same either way, which is what the round-trip
/// tests assert by scanning a fixture both ways.
#[cfg(test)]
pub fn scan_serial(
    resolved: &Resolved,
    projection: Option<&[usize]>,
    filters: &[PrunePredicate],
) -> Result<TableScanOutput> {
    let mut chunks = Vec::new();
    let (out, _) = scan_into(resolved, projection, filters, true, &mut |chunk| {
        chunks.push(chunk);
        Ok(())
    })?;
    Ok(TableScanOutput { chunks, ..out })
}

/// As [`scan`], but each chunk goes to `sink` as soon as its row group is
/// decoded instead of being collected (wave 98c). A remote scan then holds
/// one prefetch window and the row groups being decoded, not the file.
pub fn scan_streamed(
    resolved: &Resolved,
    projection: Option<&[usize]>,
    filters: &[PrunePredicate],
    sink: &mut dyn FnMut(DataChunk) -> Result<()>,
) -> Result<(TableScanOutput, RemoteReadStats)> {
    scan_into(resolved, projection, filters, false, sink)
}

fn scan_into(
    resolved: &Resolved,
    projection: Option<&[usize]>,
    filters: &[PrunePredicate],
    serial: bool,
    sink: &mut dyn FnMut(DataChunk) -> Result<()>,
) -> Result<(TableScanOutput, RemoteReadStats)> {
    let width = resolved.schema.len();
    // Which output columns to decode. A projection that names no column
    // (`COUNT(*)`) is not forced onto column 0 (wave 98a): a scan that only
    // needs row counts is answered from the footer without opening a reader
    // or fetching a data page.
    let mut needed = vec![false; width];
    match projection {
        Some(cols) => {
            for &c in cols {
                if c < width {
                    needed[c] = true;
                }
            }
        }
        None => needed.iter_mut().for_each(|n| *n = true),
    }

    let transport = Arc::new(crate::ranged::TransportCounters::default());

    let mut out = TableScanOutput::default();
    // One shared placeholder per absent output column.
    let placeholders: Vec<Option<Arc<Vector>>> = (0..width)
        .map(|i| (!needed[i]).then(|| Arc::new(Vector::new_empty(resolved.schema[i].1))))
        .collect();

    let plan = DecodePlan {
        resolved,
        filters,
        needed,
        placeholders,
    };

    for file in &resolved.files {
        if !file_survives(resolved, file, filters) {
            continue;
        }
        out.files += 1;
        // A footer's counts size every buffer below, so they are checked
        // before anything is allocated from them: a corrupt file is an error,
        // never an abort (see `FileMeta::validate`).
        file.meta.validate()?;
        // A scan that references no file column (COUNT(*), COUNT(filename),
        // a hive-only projection) is answered from the footer's row counts
        // and virtual columns; no reader is opened and no page is fetched.
        if !needs_file_decode(file, &plan) {
            scan_metadata_only(file, &plan, &mut out, sink)?;
            continue;
        }
        // Ranged objects (a remote URL or a lake data file) are read through
        // parquet's `ChunkReader` over their byte-range source. The default
        // path prefetches whole projected column-chunk ranges concurrently;
        // `PINKDB_HTTPFS_PARALLELISM=1` keeps the lazy 128 KiB serial reads.
        if let Some(spec) = &file.remote {
            let remote_file =
                crate::ranged::RemoteFile::with_counters(spec, Arc::clone(&transport));
            let cache = remote_file.cache();
            let reader = SerializedFileReader::new(remote_file).map_err(|e| {
                PinkDbError::Execution(format!(
                    "cannot read parquet file \"{}\": {e}",
                    file.meta.path
                ))
            })?;
            if spec.parallelism > 1 && spec.whole.is_none() {
                scan_remote_parallel(
                    &reader,
                    &cache,
                    spec.source.as_ref(),
                    spec.parallelism,
                    &transport,
                    file,
                    &plan,
                    &mut out,
                    sink,
                )?;
            } else {
                scan_reader(&reader, file, &plan, &mut out, sink)?;
            }
            continue;
        }
        if serial {
            let reader =
                SerializedFileReader::new(std::fs::File::open(&file.meta.path).map_err(|e| {
                    PinkDbError::Io(std::io::Error::new(
                        e.kind(),
                        format!("cannot open \"{}\": {e}", file.meta.path),
                    ))
                })?)
                .map_err(|e| {
                    PinkDbError::Execution(format!(
                        "cannot read parquet file \"{}\": {e}",
                        file.meta.path
                    ))
                })?;
            scan_reader(&reader, file, &plan, &mut out, sink)?;
        } else {
            scan_local_parallel(file, &plan, &mut out, sink)?;
        }
    }
    Ok((out, transport.snapshot()))
}

/// Whether this file has a projected column that must be decoded from data
/// pages. `filename`, `file_row_number`, hive columns and an empty projection
/// need only metadata.
fn needs_file_decode(file: &FileResolved, plan: &DecodePlan<'_>) -> bool {
    (0..plan.resolved.schema.len()).any(|out| {
        plan.needed[out]
            && plan.resolved.kind[out] == OutKind::FileColumn
            && file.out_to_file.get(out).copied().flatten().is_some()
    })
}

/// The per-statement decode plan the reader helper consumes.
struct DecodePlan<'a> {
    resolved: &'a Resolved,
    filters: &'a [PrunePredicate],
    needed: Vec<bool>,
    placeholders: Vec<Option<Arc<Vector>>>,
}

/// The output columns this file decodes, as (output, column index, column).
fn decoded_columns<'a>(
    file: &'a FileResolved,
    plan: &DecodePlan<'_>,
) -> Vec<(usize, usize, &'a FileColumn)> {
    let resolved = plan.resolved;
    (0..resolved.schema.len())
        .filter(|out| plan.needed[*out] && resolved.kind[*out] == OutKind::FileColumn)
        .filter_map(|out| {
            let at = file.out_to_file.get(out).copied().flatten()?;
            Some((out, at, &file.meta.columns[at]))
        })
        .collect()
}

/// The chunk sizes a row group of `rows` rows decodes into when it starts
/// `offset` rows into its file: every chunk but the last starts on a
/// `CHUNK_SIZE` boundary of the file, so a whole-file scan hands the writer
/// chunk-aligned batches and its append shares them instead of copying them
/// into a ragged tail. A row-group boundary is invisible to the chunking; a
/// chunk never exceeds `CHUNK_SIZE` rows, which is all a vector may hold.
///
/// The lengths are iterated, never collected: they are sized from a footer's
/// row count, which is untrusted (a file that claims 2^63 rows must not size
/// a 36 PB buffer before its first page is even read).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkLens {
    left: usize,
    at: usize,
    total: usize,
}

impl ChunkLens {
    /// The rows these chunks cover in all.
    pub(crate) fn rows(self) -> usize {
        self.total
    }

    /// How many chunks there are. Only used to bound a hint, never to size
    /// a buffer on its own (see the callers' `min(CHUNK_SIZE)`).
    pub(crate) fn chunks(self) -> usize {
        if self.left == 0 {
            return 0;
        }
        let first = (CHUNK_SIZE - self.at % CHUNK_SIZE).min(self.left);
        1 + (self.left - first).div_ceil(CHUNK_SIZE)
    }
}

impl Iterator for ChunkLens {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        if self.left == 0 {
            return None;
        }
        let n = (CHUNK_SIZE - self.at % CHUNK_SIZE).min(self.left);
        self.left -= n;
        self.at += n;
        Some(n)
    }
}

fn chunk_lens(rows: usize, offset: usize) -> ChunkLens {
    ChunkLens {
        left: rows,
        at: offset,
        total: rows,
    }
}

/// Decode one opened file serially (local `File`, or a remote `RemoteFile`
/// on the serial path): the row groups that survive pruning, the projected
/// columns, and the engine chunks.
fn scan_reader<R: ChunkReader + 'static>(
    reader: &SerializedFileReader<R>,
    file: &FileResolved,
    plan: &DecodePlan<'_>,
    out: &mut TableScanOutput,
    sink: &mut dyn FnMut(DataChunk) -> Result<()>,
) -> Result<()> {
    let decoded = decoded_columns(file, plan);
    let filtering = drops_rows(plan.resolved, plan);
    let filter_only = filter_only_columns(file, plan, filtering);
    let filters: &[PrunePredicate] = match filtering {
        true => plan.filters,
        false => &[],
    };
    let mut offset = 0usize;
    for (rg_index, rg) in file.meta.row_groups.iter().enumerate() {
        if !row_group_survives(plan.resolved, file, rg_index, plan.filters) {
            out.row_groups_pruned += 1;
            continue;
        }
        out.row_groups += 1;
        let rows = rg.rows as usize;
        let lens = chunk_lens(rows, offset);
        offset = offset.saturating_add(rows);
        if decoded.is_empty() {
            assemble_row_group(file, plan, lens, &decoded, Vec::new(), sink)?;
            continue;
        }
        let (pieces, kept) = decode_row_group(
            reader,
            rg_index,
            &decoded,
            &filter_only,
            filters,
            lens,
            &file.meta.path,
        )?;
        out.columns_decoded += decoded.len() as u64;
        match kept {
            Some(kept) => assemble_row_group(file, plan, kept, &decoded, pieces, sink)?,
            None => assemble_row_group(file, plan, lens, &decoded, pieces, sink)?,
        }
    }
    Ok(())
}

/// Emit chunks for the surviving row groups of a file without decoding any
/// column: `COUNT(*)` (and virtual-column-only projections) are answered from
/// the footer's row counts.
fn scan_metadata_only(
    file: &FileResolved,
    plan: &DecodePlan<'_>,
    out: &mut TableScanOutput,
    sink: &mut dyn FnMut(DataChunk) -> Result<()>,
) -> Result<()> {
    let mut offset = 0usize;
    for (rg_index, rg) in file.meta.row_groups.iter().enumerate() {
        if !row_group_survives(plan.resolved, file, rg_index, plan.filters) {
            out.row_groups_pruned += 1;
            continue;
        }
        out.row_groups += 1;
        let rows = rg.rows as usize;
        let lens = chunk_lens(rows, offset);
        offset = offset.saturating_add(rows);
        assemble_row_group(file, plan, lens, &[], Vec::new(), sink)?;
    }
    Ok(())
}

/// Decode a local file with the engine's worker pool.
///
/// The unit of work is one (row group, projected column) column chunk, so a
/// file that holds one row group — every file `COPY … TO parquet` writes here
/// — still fills the pool, and a file of many narrow row groups fills it from
/// row groups. Results are assembled strictly in row-group then column order,
/// which is the serial path's order, so rows and values are identical
/// whichever way the pool schedules the tasks (wave 136w5).
///
/// DuckDB's parquet reader splits the same way, but at row-group granularity
/// (duckdb/extension/parquet/parquet_reader.cpp, `ParquetReader::Scan`);
/// columns are the only independent unit left inside a single row group.
fn scan_local_parallel(
    file: &FileResolved,
    plan: &DecodePlan<'_>,
    out: &mut TableScanOutput,
    sink: &mut dyn FnMut(DataChunk) -> Result<()>,
) -> Result<()> {
    let decoded = decoded_columns(file, plan);
    if decoded.is_empty() {
        return scan_metadata_only(file, plan, out, sink);
    }
    let mut surviving: Vec<(usize, usize, usize)> = Vec::new();
    let mut offset = 0usize;
    for (rg_index, rg) in file.meta.row_groups.iter().enumerate() {
        if !row_group_survives(plan.resolved, file, rg_index, plan.filters) {
            out.row_groups_pruned += 1;
            continue;
        }
        out.row_groups += 1;
        let rows = rg.rows as usize;
        surviving.push((rg_index, rows, offset));
        offset = offset.saturating_add(rows);
    }
    let workers = rayon::current_num_threads().max(1);
    let open = || -> Result<SerializedFileReader<std::fs::File>> {
        SerializedFileReader::new(std::fs::File::open(&file.meta.path).map_err(|e| {
            PinkDbError::Io(std::io::Error::new(
                e.kind(),
                format!("cannot open \"{}\": {e}", file.meta.path),
            ))
        })?)
        .map_err(|e| {
            PinkDbError::Execution(format!(
                "cannot read parquet file \"{}\": {e}",
                file.meta.path
            ))
        })
    };
    // A reader parses the footer once; one per worker covers the pool.
    let pool: std::sync::Mutex<Vec<SerializedFileReader<std::fs::File>>> =
        std::sync::Mutex::new(Vec::new());
    let take_reader = || -> Result<SerializedFileReader<std::fs::File>> {
        match pool.lock().unwrap_or_else(|e| e.into_inner()).pop() {
            Some(reader) => Ok(reader),
            None => open(),
        }
    };
    let give_reader = |reader| pool.lock().unwrap_or_else(|e| e.into_inner()).push(reader);
    let filtering = drops_rows(plan.resolved, plan);
    let filter_only = filter_only_columns(file, plan, filtering);
    let filters: &[PrunePredicate] = match filtering {
        true => plan.filters,
        false => &[],
    };
    let mut at = 0usize;
    while at < surviving.len() {
        // A window bounds the decoded bytes in flight: enough row groups to
        // fill the pool four times over, but never a whole file at once.
        let mut end = at + 1;
        while end < surviving.len() && (end - at) * decoded.len() < workers * 4 {
            end += 1;
        }
        let window = &surviving[at..end];
        if filtering {
            // A row selection spans the projected columns, so with pushed
            // filters the row group is the unit of work.
            let results: Vec<Result<RowGroupDecode>> = window
                .par_iter()
                .map(|&(rg_index, rows, start)| {
                    let reader = take_reader()?;
                    let lens = chunk_lens(rows, start);
                    let result = decode_row_group(
                        &reader,
                        rg_index,
                        &decoded,
                        &filter_only,
                        filters,
                        lens,
                        &file.meta.path,
                    );
                    // A failed decode hands its reader back for the tasks after it.
                    give_reader(reader);
                    result
                })
                .collect();
            for (&(_, rows, start), result) in window.iter().zip(results) {
                let (pieces, kept) = result?;
                out.columns_decoded += decoded.len() as u64;
                match kept {
                    Some(kept) => assemble_row_group(file, plan, kept, &decoded, pieces, sink)?,
                    None => {
                        let lens = chunk_lens(rows, start);
                        assemble_row_group(file, plan, lens, &decoded, pieces, sink)?;
                    }
                }
            }
            at = end;
            continue;
        }
        let tasks: Vec<(usize, usize, ChunkLens)> = window
            .iter()
            .flat_map(|&(rg_index, rows, start)| {
                let lens = chunk_lens(rows, start);
                (0..decoded.len()).map(move |slot| (rg_index, slot, lens))
            })
            .collect();
        let results: Vec<Result<Vec<Vector>>> = tasks
            .par_iter()
            .map(|(rg_index, slot, lens)| {
                let reader = take_reader()?;
                let pieces = decode_in_row_group(
                    &reader,
                    *rg_index,
                    decoded[*slot].2,
                    *lens,
                    &file.meta.path,
                );
                // A failed decode hands its reader back for the tasks after it.
                give_reader(reader);
                pieces
            })
            .collect();
        let mut results = results.into_iter();
        for &(_, rows, start) in window {
            let mut pieces = Vec::with_capacity(decoded.len());
            for _ in 0..decoded.len() {
                let Some(result) = results.next() else {
                    unreachable!("scan_local_parallel makes one task per projected column")
                };
                pieces.push(result?);
            }
            out.columns_decoded += decoded.len() as u64;
            let lens = chunk_lens(rows, start);
            assemble_row_group(file, plan, lens, &decoded, pieces, sink)?;
        }
        at = end;
    }
    Ok(())
}

/// Decode one projected column of one row group.
fn decode_in_row_group<R: ChunkReader + 'static>(
    reader: &SerializedFileReader<R>,
    rg_index: usize,
    column: &FileColumn,
    lens: ChunkLens,
    path: &str,
) -> Result<Vec<Vector>> {
    let row_group_reader = reader.get_row_group(rg_index).map_err(|e| {
        PinkDbError::Execution(format!(
            "cannot read row group {rg_index} of \"{path}\": {e}"
        ))
    })?;
    decode_column(&*row_group_reader, column, lens)
}

/// Decode the projected columns of one row group into chunk-sized vectors,
/// with the pushed `filters` applied to the rows.
#[allow(clippy::too_many_arguments)]
/// The decoded pieces of one row group: the columns, and the rows each chunk
/// kept when a pushed predicate dropped some (`None` when nothing dropped).
type RowGroupDecode = (Vec<Vec<Vector>>, Option<Vec<usize>>);

fn decode_row_group<R: ChunkReader + 'static>(
    reader: &SerializedFileReader<R>,
    rg_index: usize,
    decoded: &[(usize, usize, &FileColumn)],
    filter_only: &[(usize, usize, &FileColumn)],
    filters: &[PrunePredicate],
    lens: ChunkLens,
    path: &str,
) -> Result<RowGroupDecode> {
    let row_group_reader = reader.get_row_group(rg_index).map_err(|e| {
        PinkDbError::Execution(format!(
            "cannot read row group {rg_index} of \"{path}\": {e}"
        ))
    })?;
    decode_row_group_reader(
        &*row_group_reader,
        decoded,
        filter_only,
        filters,
        lens,
        path,
    )
}

/// Whether the scan may drop the rows its pushed predicates reject.
///
/// A projection that asks for a row number numbers the rows it receives, so a
/// dropped row would renumber every row after it: the predicates then only
/// prune row groups and pages, as before. The engine re-applies the `WHERE`
/// above the scan, so dropping a row it would reject changes no result.
fn drops_rows(resolved: &Resolved, plan: &DecodePlan<'_>) -> bool {
    !plan.filters.is_empty()
        && !(0..plan.needed.len())
            .any(|out| plan.needed[out] && resolved.kind.get(out) == Some(&OutKind::RowNumber))
}

/// The columns a pushed predicate names that the projection does not decode:
/// the row selection needs their dictionary, but no engine vector is emitted
/// for them.
fn filter_only_columns<'a>(
    file: &'a FileResolved,
    plan: &DecodePlan<'_>,
    filtering: bool,
) -> Vec<(usize, usize, &'a FileColumn)> {
    if !filtering {
        return Vec::new();
    }
    let resolved = plan.resolved;
    (0..resolved.schema.len())
        .filter(|out| !plan.needed[*out] && resolved.kind[*out] == OutKind::FileColumn)
        .filter(|out| {
            plan.filters
                .iter()
                .any(|filter| filter.column == *out && filter.path.is_none())
        })
        .filter_map(|out| {
            let at = file.out_to_file.get(out).copied().flatten()?;
            Some((out, at, &file.meta.columns[at]))
        })
        .collect()
}

/// Both selections' rows, chunk by chunk; each keeps rows in order, so a
/// merge walk is an intersection.
fn intersect(a: &[Vec<u32>], b: &[Vec<u32>]) -> Vec<Vec<u32>> {
    a.iter()
        .zip(b)
        .map(|(a, b)| {
            let mut rows = Vec::with_capacity(a.len().min(b.len()));
            let (mut i, mut j) = (0, 0);
            while i < a.len() && j < b.len() {
                match a[i].cmp(&b[j]) {
                    std::cmp::Ordering::Less => i += 1,
                    std::cmp::Ordering::Greater => j += 1,
                    std::cmp::Ordering::Equal => {
                        rows.push(a[i]);
                        i += 1;
                        j += 1;
                    }
                }
            }
            rows
        })
        .collect()
}

/// Decode one row group from any `RowGroupReader` (the serial reader or the
/// page-filtered wrapper).
///
/// A predicate on a dictionary-encoded column is evaluated once per dictionary
/// entry (see [`Dictionary::kept_rows`]); the row selection it yields drops the
/// rejected rows of every projected column before they are materialized, and
/// several predicates intersect. A column whose decode has no dictionary is
/// decoded in full and filtered after, which keeps the decode path unchanged.
#[allow(clippy::too_many_arguments)]
fn decode_row_group_reader(
    row_group_reader: &dyn RowGroupReader,
    decoded: &[(usize, usize, &FileColumn)],
    filter_only: &[(usize, usize, &FileColumn)],
    filters: &[PrunePredicate],
    lens: ChunkLens,
    path: &str,
) -> Result<RowGroupDecode> {
    // The dictionaries the predicates need, read here and reused for the
    // projection (a column that is both projected and filtered reads once).
    let mut dictionaries: Vec<(usize, Dictionary)> = Vec::new();
    let mut selection: Option<Vec<Vec<u32>>> = None;
    for (out, at, column) in decoded.iter().chain(filter_only) {
        if dictionaries.iter().any(|(read, _)| read == at) {
            continue;
        }
        let predicates: Vec<&PrunePredicate> = filters
            .iter()
            .filter(|filter| filter.column == *out && filter.path.is_none())
            .collect();
        if predicates.is_empty() {
            continue;
        }
        let dictionary = Dictionary::read(row_group_reader, column, lens).map_err(|e| {
            PinkDbError::Execution(format!("column \"{}\" of \"{path}\": {e}", column.name))
        })?;
        let Some(dictionary) = dictionary else {
            continue;
        };
        match dictionary.kept_rows(&predicates) {
            Some(kept) => {
                selection = Some(match selection {
                    Some(selection) => intersect(&selection, &kept),
                    None => kept,
                });
            }
            None => continue,
        }
        dictionaries.push((*at, dictionary));
    }
    // The kept rows per chunk, when a predicate dropped any: sized by the
    // rows actually decoded from a validated row group, never by a footer
    // count. `None` keeps `lens`, which is iterated rather than collected.
    let kept_lens: Option<Vec<usize>> = selection
        .as_ref()
        .map(|selection| selection.iter().map(Vec::len).collect());
    // One column at a time: each needed file column decodes into the pieces
    // `kept_lens` describes (a vector never exceeds CHUNK_SIZE rows).
    let mut pieces: Vec<Vec<Vector>> = Vec::with_capacity(decoded.len());
    for (_, at, column) in decoded {
        let chunks = match dictionaries.iter().find(|(read, _)| read == at) {
            Some((_, dictionary)) => dictionary.gather(selection.as_deref()).map_err(|e| {
                PinkDbError::Execution(format!("column \"{}\" of \"{path}\": {e}", column.name))
            })?,
            None => {
                let mut chunks = decode_column(row_group_reader, column, lens).map_err(|e| {
                    PinkDbError::Execution(format!("column \"{}\" of \"{path}\": {e}", column.name))
                })?;
                if let Some(selection) = &selection {
                    for (chunk, rows) in chunks.iter_mut().zip(selection) {
                        if rows.len() < chunk.len() {
                            *chunk = chunk.take(rows);
                        }
                    }
                }
                chunks
            }
        };
        pieces.push(chunks);
    }
    Ok((pieces, kept_lens))
}

/// Assemble one row group's decoded pieces into full-width engine chunks
/// (absent columns stay zero-length placeholders). This is where row order is
/// fixed: row groups are assembled in file order and chunks in row order.
fn assemble_row_group(
    file: &FileResolved,
    plan: &DecodePlan<'_>,
    lens: impl IntoIterator<Item = usize>,
    decoded: &[(usize, usize, &FileColumn)],
    mut pieces: Vec<Vec<Vector>>,
    sink: &mut dyn FnMut(DataChunk) -> Result<()>,
) -> Result<()> {
    let resolved = plan.resolved;
    let placeholders = &plan.placeholders;
    let width = resolved.schema.len();
    let mut start = 0usize;
    for (chunk_index, n) in lens.into_iter().enumerate() {
        if n == 0 {
            // A pushed predicate rejected every row of this chunk.
            continue;
        }
        let mut cols: Vec<Arc<Vector>> = Vec::with_capacity(width);
        for (out_index, placeholder) in placeholders.iter().enumerate() {
            if let Some(placeholder) = placeholder {
                cols.push(Arc::clone(placeholder));
                continue;
            }
            let value = match &resolved.kind[out_index] {
                OutKind::FileColumn => {
                    let dtype = resolved.schema[out_index].1;
                    match decoded.iter().position(|(out, _, _)| *out == out_index) {
                        Some(slot) => {
                            let from = file.out_to_file[out_index]
                                .map(|at| file.meta.columns[at].ty)
                                .expect("a decoded file column has a leaf");
                            let piece = std::mem::replace(
                                &mut pieces[slot][chunk_index],
                                Vector::new_empty(dtype),
                            );
                            if from == dtype {
                                Arc::new(piece)
                            } else {
                                Arc::new(cast_vector(
                                    &piece,
                                    from,
                                    dtype,
                                    &resolved.schema[out_index].0,
                                )?)
                            }
                        }
                        None => {
                            // `union_by_name` with a file that does
                            // not have the column: every row reads
                            // NULL (α's rule).
                            let mut vector = Vector::with_capacity(dtype, n);
                            for _ in 0..n {
                                vector.push_null()?;
                            }
                            Arc::new(vector)
                        }
                    }
                }
                OutKind::RowNumber => {
                    let mut vector = Vector::with_capacity(DataType::Bigint, n);
                    for i in 0..n {
                        vector.push_valid(Value::Bigint((start + i) as i64))?;
                    }
                    Arc::new(vector)
                }
                OutKind::FileName => {
                    let mut vector = Vector::with_capacity(DataType::Varchar, n);
                    for _ in 0..n {
                        vector.push_valid(Value::Varchar(file.meta.path.clone()))?;
                    }
                    Arc::new(vector)
                }
                OutKind::Hive(index) => {
                    let column = &resolved.hive[*index];
                    let value = match file.hive.get(*index).and_then(|v| v.as_ref()) {
                        Some(text) => crate::hive::value_of(column.ty, text),
                        None => Value::Null,
                    };
                    let mut vector = Vector::with_capacity(column.ty, n);
                    for _ in 0..n {
                        vector.push_value(value.clone())?;
                    }
                    Arc::new(vector)
                }
            };
            cols.push(value);
        }
        sink(DataChunk::projected(cols, n))?;
        start += n;
    }
    Ok(())
}

/// The wave-98a ranged path: prefetch the projected column-chunk ranges of
/// the next row groups with a bounded pool, install them in the reader's
/// cache, decode (in parallel when possible) and assemble strictly in
/// row-group order.
#[allow(clippy::too_many_arguments)]
fn scan_remote_parallel(
    reader: &SerializedFileReader<crate::ranged::RemoteFile>,
    cache: &crate::ranged::RangeCache,
    source: &dyn crate::ranged::RangeSource,
    parallelism: usize,
    transport: &Arc<crate::ranged::TransportCounters>,
    file: &FileResolved,
    plan: &DecodePlan<'_>,
    out: &mut TableScanOutput,
    sink: &mut dyn FnMut(DataChunk) -> Result<()>,
) -> Result<()> {
    use crate::ranged::ByteSpan;

    let decoded_columns = decoded_columns(file, plan);
    let decoded = decoded_columns.as_slice();
    if decoded.is_empty() {
        return scan_metadata_only(file, plan, out, sink);
    }
    let filtering = drops_rows(plan.resolved, plan);
    let filter_only = filter_only_columns(file, plan, filtering);
    let filters: &[PrunePredicate] = match filtering {
        true => plan.filters,
        false => &[],
    };

    // The surviving row groups and the coalesced byte ranges each needs.
    let mut surviving: Vec<(usize, usize, usize, Vec<ByteSpan>)> = Vec::new();
    let mut offset = 0usize;
    for (rg_index, rg) in file.meta.row_groups.iter().enumerate() {
        if !row_group_survives(plan.resolved, file, rg_index, plan.filters) {
            out.row_groups_pruned += 1;
            continue;
        }
        out.row_groups += 1;
        let spans: Vec<ByteSpan> = decoded
            .iter()
            .flat_map(|(_, _, column)| {
                (0..column.leaves).filter_map(move |offset| {
                    rg.chunks
                        .get(column.leaf + offset)
                        .and_then(crate::ranged::chunk_span)
                })
            })
            // The single-request cap: a chunk larger than it stays on the
            // lazy path (it cannot be split without chaining reads).
            .filter(|span| span.len <= crate::ranged::MAX_REQUEST_BYTES)
            .collect();
        let rows = rg.rows as usize;
        surviving.push((rg_index, rows, offset, crate::ranged::coalesce_spans(spans)));
        offset = offset.saturating_add(rows);
    }

    let mut at = 0;
    while at < surviving.len() {
        // A window is bounded by MAX_PREFETCH_BYTES (at least one row group).
        let mut end = at;
        let mut window_bytes = 0u64;
        while end < surviving.len() {
            let bytes: u64 = surviving[end].3.iter().map(|span| span.len).sum();
            if end > at && window_bytes.saturating_add(bytes) > crate::ranged::MAX_PREFETCH_BYTES {
                break;
            }
            window_bytes = window_bytes.saturating_add(bytes);
            end += 1;
        }
        let window = &surviving[at..end];
        let spans: Vec<ByteSpan> = window
            .iter()
            .flat_map(|(_, _, _, spans)| spans.iter().copied())
            .collect();
        let fetched = crate::ranged::fetch_spans(source, &spans, parallelism, transport);
        for (span, result) in spans.iter().zip(fetched) {
            let bytes = result
                .map_err(|e| PinkDbError::Execution(format!("remote parquet read failed: {e}")))?;
            if bytes.len() as u64 != span.len {
                return Err(PinkDbError::Execution(format!(
                    "remote parquet read failed: GET {} (range {}..{}) returned {} bytes, \
                     expected {}",
                    file.meta.path,
                    span.start,
                    span.end(),
                    bytes.len(),
                    span.len
                )));
            }
            cache.insert(span.start, bytes);
        }

        // Decode row groups concurrently when the window has more than one;
        // assembly below still runs in row-group order.
        let indexes: Vec<(usize, ChunkLens)> = window
            .iter()
            .map(|(rg_index, rows, start, _)| (*rg_index, chunk_lens(*rows, *start)))
            .collect();
        let results: Vec<Result<RowGroupDecode>> = if indexes.len() > 1 {
            let filter_only = &filter_only;
            std::thread::scope(|scope| {
                let mut handles = Vec::with_capacity(indexes.len());
                for (rg_index, lens) in &indexes {
                    handles.push(scope.spawn(move || {
                        decode_row_group(
                            reader,
                            *rg_index,
                            decoded,
                            filter_only,
                            filters,
                            *lens,
                            &file.meta.path,
                        )
                    }));
                }
                handles
                    .into_iter()
                    .map(|handle| handle.join().expect("row-group decode worker panicked"))
                    .collect()
            })
        } else {
            indexes
                .iter()
                .map(|(rg_index, lens)| {
                    decode_row_group(
                        reader,
                        *rg_index,
                        decoded,
                        &filter_only,
                        filters,
                        *lens,
                        &file.meta.path,
                    )
                })
                .collect()
        };

        for (&(rg_index, lens), result) in indexes.iter().zip(results) {
            let (pieces, kept) = result?;
            out.columns_decoded += decoded.len() as u64;
            let _ = rg_index;
            match kept {
                Some(kept) => assemble_row_group(file, plan, kept, decoded, pieces, sink)?,
                None => assemble_row_group(file, plan, lens, decoded, pieces, sink)?,
            }
        }

        let starts: Vec<u64> = spans.iter().map(|span| span.start).collect();
        cache.remove(&starts);
        at = end;
    }
    Ok(())
}

/// Cast one decoded column to the scan's output type (the conversions
/// [`castable`](crate::resolve::castable) admits). A value that does not fit
/// is a hard error naming the column — never a silently wrong value.
fn cast_vector(vector: &Vector, from: DataType, to: DataType, column: &str) -> Result<Vector> {
    let _ = from;
    let mut out = Vector::with_capacity(to, vector.len());
    for i in 0..vector.len() {
        let value = vector.get(i);
        let converted = convert_value(value, to)
            .map_err(|why| PinkDbError::Execution(format!("column \"{column}\": {why}")))?;
        out.push_value(converted)?;
    }
    Ok(out)
}

/// Whether a predicate's literal may be compared to a column of this type
/// (the engine only pushes such predicates, but a defensive check keeps a
/// foreign caller from fabricating an ordering).
fn comparable(column: DataType, literal: &Value) -> bool {
    let Some(literal_type) = literal.data_type() else {
        return false;
    };
    if column == literal_type {
        return true;
    }
    column.is_numeric() && literal_type.is_numeric()
}

/// Whether the file may contribute any row: every constant-column predicate
/// must be able to hold for the file's constant value.
fn file_survives(resolved: &Resolved, file: &FileResolved, filters: &[PrunePredicate]) -> bool {
    for filter in filters {
        let Some(constant) = file_constant_value(resolved, file, filter.column) else {
            continue;
        };
        if !constant_keeps(&constant, filter, resolved.schema[filter.column].1) {
            return false;
        }
    }
    true
}

/// Whether a constant value can satisfy one predicate.
fn constant_keeps(constant: &Value, filter: &PrunePredicate, column: DataType) -> bool {
    match filter.op {
        PruneOp::IsNull => constant.is_null(),
        PruneOp::IsNotNull => !constant.is_null(),
        PruneOp::In => {
            if constant.is_null() {
                return false;
            }
            filter.values.iter().any(|value| {
                !value.is_null()
                    && comparable(column, value)
                    && pinkdb::plan::compare_values(constant, value).is_eq()
            })
        }
        _ => {
            let Some(literal) = filter.value.as_ref().filter(|_| {
                // A NULL literal never satisfies a comparison.
                true
            }) else {
                return true;
            };
            if literal.is_null() {
                // `col <op> NULL` is NULL for every row: nothing matches, but
                // the executor's filter drops them anyway; keep the file so
                // the pruning decision stays purely an optimization.
                return true;
            }
            if constant.is_null() || !comparable(column, literal) {
                return true;
            }
            let ord = pinkdb::plan::compare_values(constant, literal);
            match filter.op {
                PruneOp::Eq => ord == std::cmp::Ordering::Equal,
                PruneOp::NotEq => ord != std::cmp::Ordering::Equal,
                PruneOp::Lt => ord == std::cmp::Ordering::Less,
                PruneOp::LtEq => ord != std::cmp::Ordering::Greater,
                PruneOp::Gt => ord == std::cmp::Ordering::Greater,
                PruneOp::GtEq => ord != std::cmp::Ordering::Less,
                PruneOp::IsNull | PruneOp::IsNotNull | PruneOp::In => unreachable!("handled above"),
            }
        }
    }
}

/// Whether a row group may contribute rows under `filters`.
fn row_group_survives(
    resolved: &Resolved,
    file: &FileResolved,
    rg_index: usize,
    filters: &[PrunePredicate],
) -> bool {
    let Some(rg) = file.meta.row_groups.get(rg_index) else {
        return true;
    };
    for filter in filters {
        if resolved.kind.get(filter.column) != Some(&OutKind::FileColumn) {
            continue;
        }
        let Some(at) = file.out_to_file.get(filter.column).copied().flatten() else {
            continue;
        };
        if let Some(path) = &filter.path {
            // A path-qualified predicate prunes on the shredded leaf's
            // statistics; a row group without them cannot be excluded.
            let stats = rg
                .paths
                .iter()
                .find(|(column, at_path, _)| *column == at && at_path == path.as_ref())
                .map(|(_, _, stats)| stats);
            if let Some(stats) = stats
                && !path_keeps(stats, filter, rg.rows)
            {
                return false;
            }
            continue;
        }
        let column = &file.meta.columns[at];
        let Some(stats) = rg.stats.get(at).and_then(|s| s.as_ref()) else {
            continue;
        };
        if !stats_keeps(
            stats,
            column,
            filter,
            resolved.schema[filter.column].1,
            rg.rows,
        ) {
            return false;
        }
    }
    true
}

/// Whether one path's statistics prove the row group may hold a matching
/// row: the same rules as [`stats_keeps`], over VARIANT bounds.
///
/// The bounds are VARIANT values, so the literal is compared in the engine's
/// VARIANT order (`1` = `1.0` = `DECIMAL 1.0`, and a path typed as another
/// kind sorts as that kind) — the same order the filter's own comparison
/// uses, which is what makes pruning by path sound.
fn path_keeps(stats: &crate::file_meta::ColStats, filter: &PrunePredicate, rows: u64) -> bool {
    use pinkdb::variant::{Variant, cmp, from_value};

    let to_variant = |value: &Value| -> Option<Variant> {
        match value {
            Value::Variant(v) => Some((**v).clone()),
            other => from_value(other).ok(),
        }
    };
    // NaN is unordered against every bound, so no bound proves anything
    // about a NaN literal or a NaN bound.
    let compare = |bound: &Value, literal: &Value| -> Option<std::cmp::Ordering> {
        let bound = to_variant(bound)?;
        let literal = to_variant(literal)?;
        if let (Variant::Double(a), Variant::Double(b)) = (&bound, &literal)
            && (a.is_nan() || b.is_nan())
        {
            return None;
        }
        Some(cmp(&bound, &literal))
    };
    match filter.op {
        PruneOp::IsNull => stats.null_count != Some(0),
        PruneOp::IsNotNull => stats.null_count != Some(rows),
        // An `IN` list is only pushed on a column's own value, not a path.
        PruneOp::In => true,
        PruneOp::NotEq => {
            // `<>` is safe only when every value of the path equals the
            // literal; without both bounds, keep the row group.
            let (Some(min), Some(max)) = (stats.min.as_ref(), stats.max.as_ref()) else {
                return true;
            };
            let Some(literal) = filter.value.as_ref() else {
                return true;
            };
            if min != max {
                return true;
            }
            compare(min, literal).is_none_or(|order| !order.is_eq())
        }
        _ => {
            let Some(literal) = filter.value.as_ref() else {
                return true;
            };
            match filter.op {
                PruneOp::Eq => {
                    if compare_literal(&stats.min, literal, &compare).is_some_and(|o| o.is_gt()) {
                        return false;
                    }
                    if compare_literal(&stats.max, literal, &compare).is_some_and(|o| o.is_lt()) {
                        return false;
                    }
                    true
                }
                PruneOp::Lt => stats
                    .min
                    .as_ref()
                    .is_none_or(|min| compare(min, literal).is_none_or(|o| o.is_lt())),
                PruneOp::LtEq => stats
                    .min
                    .as_ref()
                    .is_none_or(|min| compare(min, literal).is_none_or(|o| !o.is_gt())),
                PruneOp::Gt => stats
                    .max
                    .as_ref()
                    .is_none_or(|max| compare(max, literal).is_none_or(|o| o.is_gt())),
                PruneOp::GtEq => stats
                    .max
                    .as_ref()
                    .is_none_or(|max| compare(max, literal).is_none_or(|o| !o.is_lt())),
                PruneOp::IsNull | PruneOp::IsNotNull | PruneOp::NotEq | PruneOp::In => {
                    unreachable!("handled above")
                }
            }
        }
    }
}

/// Whether any non-NULL `IN` list value lies inside `[min, max]`: a row can
/// only match an `IN` by equalling one of them. An order the comparator
/// cannot decide (`None`, a NaN against VARIANT bounds) proves nothing and
/// keeps the chunk.
fn in_bounds(
    values: &[Value],
    min: &Value,
    max: &Value,
    compare: impl Fn(&Value, &Value) -> Option<std::cmp::Ordering>,
) -> bool {
    for value in values.iter().filter(|value| !value.is_null()) {
        match (compare(min, value), compare(value, max)) {
            (Some(low), Some(high)) if !low.is_gt() && !high.is_gt() => return true,
            (Some(_), Some(_)) => {}
            _ => return true,
        }
    }
    false
}

/// `compare` applied to an optional bound (an absent bound excludes nothing).
fn compare_literal(
    bound: &Option<Value>,
    literal: &Value,
    compare: &impl Fn(&Value, &Value) -> Option<std::cmp::Ordering>,
) -> Option<std::cmp::Ordering> {
    compare(bound.as_ref()?, literal)
}

/// Whether the statistics prove the row group may hold a matching row.
fn stats_keeps(
    stats: &crate::file_meta::ColStats,
    column: &FileColumn,
    filter: &PrunePredicate,
    _column_type: DataType,
    rows: u64,
) -> bool {
    match filter.op {
        PruneOp::IsNull => stats.null_count != Some(0),
        PruneOp::IsNotNull => stats.null_count != Some(rows),
        PruneOp::In => {
            // A row matches only by equalling a non-NULL list value; NaN is
            // never inside Parquet's bounds, and a NaN list value equals
            // only a NaN row the bounds do not cover.
            for value in filter.values.iter().filter(|value| !value.is_null()) {
                if !comparable(column.ty, value) {
                    return true;
                }
                if matches!(value, Value::Double(x) if x.is_nan()) {
                    return true;
                }
            }
            let (Some(min), Some(max)) = (stats.min.as_ref(), stats.max.as_ref()) else {
                return true;
            };
            in_bounds(&filter.values, min, max, |a, b| {
                Some(pinkdb::plan::compare_values(a, b))
            })
        }
        _ => {
            // `<>` needs both bounds and must be sure *all* values equal the
            // literal; floats are excluded (the bounds do not cover NaN, and
            // NaN rows would match `<>` any non-NaN literal).
            if filter.op == PruneOp::NotEq {
                // A NaN equals only NaN, so a row group holding one could
                // still match `<>`; floats never prune on it. Decimals
                // compare exactly, so they may.
                if column.ty == DataType::Double {
                    return true;
                }
                let (Some(min), Some(max)) = (stats.min.as_ref(), stats.max.as_ref()) else {
                    return true;
                };
                let Some(literal) = filter.value.as_ref() else {
                    return true;
                };
                if !comparable(column.ty, literal) {
                    return true;
                }
                return !(min == max && pinkdb::plan::compare_values(min, literal).is_eq());
            }
            let Some(literal) = filter.value.as_ref() else {
                return true;
            };
            if literal.is_null() || !comparable(column.ty, literal) {
                return true;
            }
            // NaN sorts above every other double (`num_cmp`, DuckDB order),
            // but Parquet DOUBLE statistics never cover NaN (writers skip it
            // and no NaN count is recorded): the max cannot bound `>`/`>=`,
            // and no bound excludes `= NaN`. `<`/`<=` never match a NaN row.
            if column.ty == DataType::Double
                && (matches!(filter.op, PruneOp::Gt | PruneOp::GtEq)
                    || matches!(literal, Value::Double(x) if x.is_nan()))
            {
                return true;
            }
            match filter.op {
                PruneOp::Eq => {
                    if let Some(min) = &stats.min
                        && pinkdb::plan::compare_values(literal, min).is_lt()
                    {
                        return false;
                    }
                    if let Some(max) = &stats.max
                        && pinkdb::plan::compare_values(literal, max).is_gt()
                    {
                        return false;
                    }
                    true
                }
                PruneOp::Lt => stats
                    .min
                    .as_ref()
                    .is_none_or(|min| pinkdb::plan::compare_values(min, literal).is_lt()),
                PruneOp::LtEq => stats
                    .min
                    .as_ref()
                    .is_none_or(|min| !pinkdb::plan::compare_values(min, literal).is_gt()),
                PruneOp::Gt => stats
                    .max
                    .as_ref()
                    .is_none_or(|max| pinkdb::plan::compare_values(max, literal).is_gt()),
                PruneOp::GtEq => stats
                    .max
                    .as_ref()
                    .is_none_or(|max| !pinkdb::plan::compare_values(max, literal).is_lt()),
                PruneOp::IsNull | PruneOp::IsNotNull | PruneOp::NotEq | PruneOp::In => {
                    unreachable!("handled above")
                }
            }
        }
    }
}

/// Decode one column chunk into chunk-sized engine vectors (≤ CHUNK_SIZE
/// rows each, in row order).
fn decode_column(
    row_group: &dyn RowGroupReader,
    column: &FileColumn,
    lens: ChunkLens,
) -> Result<Vec<Vector>> {
    if let Some(shred) = &column.shred {
        return crate::variant_read::read_column(row_group, column.leaf, shred, lens, &column.name);
    }
    if let DataType::Array { .. } = column.ty {
        return read_array_column(row_group, column, lens);
    }
    if let Some(dictionary) = Dictionary::read(row_group, column, lens)? {
        return dictionary.gather(None);
    }
    let leaf = column.leaf;
    let (precision, scale) = match column.ty {
        DataType::Decimal { precision, scale } => (precision, scale),
        _ => (0, 0),
    };
    let optional = column.optional;
    let reader = row_group.get_column_reader(leaf).map_err(|e| {
        PinkDbError::Execution(format!(
            "cannot read column \"{}\" (leaf {leaf}): {e}",
            column.name
        ))
    })?;
    let name = column.name.clone();
    let chunks = match (column.physical, reader) {
        (parquet::basic::Type::BOOLEAN, ColumnReader::BoolColumnReader(r)) => {
            let mut r = get_typed_column_reader::<BoolType>(ColumnReader::BoolColumnReader(r));
            read_flat_chunks(&mut r, lens, optional, &name, bool_chunk)?
        }
        (parquet::basic::Type::INT32, ColumnReader::Int32ColumnReader(r)) => {
            let mut r = get_typed_column_reader::<Int32Type>(ColumnReader::Int32ColumnReader(r));
            match column.ty {
                // UINT_32 values above i32::MAX cannot be represented.
                DataType::Integer if column.unsigned => read_flat_chunks(
                    &mut r,
                    lens,
                    optional,
                    &name,
                    |values, levels, n, column| {
                        for value in values.iter() {
                            if i32::try_from(*value as u32).is_err() {
                                return Err(decode_error(
                                    column,
                                    format!("{value} does not fit in INTEGER"),
                                ));
                            }
                        }
                        int32_chunk(values, levels, n, column)
                    },
                )?,
                DataType::Integer => read_flat_chunks(&mut r, lens, optional, &name, int32_chunk)?,
                DataType::Date => read_flat_chunks(&mut r, lens, optional, &name, date_chunk)?,
                DataType::Decimal { .. } => read_flat_chunks(
                    &mut r,
                    lens,
                    optional,
                    &name,
                    |values, levels, n, column| {
                        let mut unscaled: Vec<i128> = values.iter().map(|&v| v as i128).collect();
                        decimal_chunk(&mut unscaled, levels, n, column, precision, scale)
                    },
                )?,
                other => return Err(unsupported_decode(&name, other)),
            }
        }
        (parquet::basic::Type::INT64, ColumnReader::Int64ColumnReader(r)) => {
            let mut r = get_typed_column_reader::<Int64Type>(ColumnReader::Int64ColumnReader(r));
            match column.ty {
                // UINT_64 values above i64::MAX cannot be represented.
                DataType::Bigint if column.unsigned => read_flat_chunks(
                    &mut r,
                    lens,
                    optional,
                    &name,
                    |values, levels, n, column| {
                        for value in values.iter() {
                            if i64::try_from(*value as u64).is_err() {
                                return Err(decode_error(
                                    column,
                                    format!("{value} does not fit in BIGINT"),
                                ));
                            }
                        }
                        int64_chunk(values, levels, n, column)
                    },
                )?,
                DataType::Bigint => read_flat_chunks(&mut r, lens, optional, &name, int64_chunk)?,
                DataType::Timestamp => {
                    read_flat_chunks(&mut r, lens, optional, &name, timestamp_chunk)?
                }
                DataType::Decimal { .. } => read_flat_chunks(
                    &mut r,
                    lens,
                    optional,
                    &name,
                    |values, levels, n, column| {
                        let mut unscaled: Vec<i128> = values.iter().map(|&v| v as i128).collect();
                        decimal_chunk(&mut unscaled, levels, n, column, precision, scale)
                    },
                )?,
                other => return Err(unsupported_decode(&name, other)),
            }
        }
        (parquet::basic::Type::INT96, ColumnReader::Int96ColumnReader(r)) => {
            let mut r = get_typed_column_reader::<Int96Type>(ColumnReader::Int96ColumnReader(r));
            read_flat_chunks(
                &mut r,
                lens,
                optional,
                &name,
                |values, levels, n, column| {
                    let mut micros: Vec<i64> = values
                        .iter()
                        .map(|v| {
                            let d = v.data();
                            let nanos = (d[0] as u64) | ((d[1] as u64) << 32);
                            let epoch_days = d[2] as i64 - 2_440_588;
                            epoch_days * 86_400_000_000 + (nanos / 1_000) as i64
                        })
                        .collect();
                    timestamp_chunk(&mut micros, levels, n, column)
                },
            )?
        }
        (parquet::basic::Type::FLOAT, ColumnReader::FloatColumnReader(r)) => {
            let mut r = get_typed_column_reader::<FloatType>(ColumnReader::FloatColumnReader(r));
            read_flat_chunks(
                &mut r,
                lens,
                optional,
                &name,
                |values, levels, n, column| {
                    let mut widened: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    f64_chunk(&mut widened, levels, n, column)
                },
            )?
        }
        (parquet::basic::Type::DOUBLE, ColumnReader::DoubleColumnReader(r)) => {
            let mut r = get_typed_column_reader::<DoubleType>(ColumnReader::DoubleColumnReader(r));
            read_flat_chunks(&mut r, lens, optional, &name, f64_chunk)?
        }
        (parquet::basic::Type::BYTE_ARRAY, ColumnReader::ByteArrayColumnReader(r)) => {
            let mut r =
                get_typed_column_reader::<ByteArrayType>(ColumnReader::ByteArrayColumnReader(r));
            read_flat_chunks(&mut r, lens, optional, &name, varchar_chunk)?
        }
        (
            parquet::basic::Type::FIXED_LEN_BYTE_ARRAY,
            ColumnReader::FixedLenByteArrayColumnReader(r),
        ) => {
            let mut r = get_typed_column_reader::<FixedLenByteArrayType>(
                ColumnReader::FixedLenByteArrayColumnReader(r),
            );
            match column.ty {
                DataType::Decimal { .. } => read_flat_chunks(
                    &mut r,
                    lens,
                    optional,
                    &name,
                    |values, levels, n, column| {
                        let mut unscaled: Vec<i128> = values
                            .iter()
                            .map(|v| crate::schema_map::fixed_decimal_value(v.data()))
                            .collect();
                        decimal_chunk(&mut unscaled, levels, n, column, precision, scale)
                    },
                )?,
                DataType::Interval => {
                    read_flat_chunks(&mut r, lens, optional, &name, interval_chunk)?
                }
                // Parquet stores a UUID's 16 bytes in RFC 4122 order (the
                // parquet-format UUID annotation says so), so big-endian.
                DataType::Uuid => read_flat_chunks(
                    &mut r,
                    lens,
                    optional,
                    &name,
                    |values, levels, n, column| {
                        let mut uuids = Vec::with_capacity(values.len());
                        for value in values.iter() {
                            let bytes: [u8; 16] = value.data().try_into().map_err(|_| {
                                decode_error(column, "the UUID column is not 16 bytes wide")
                            })?;
                            uuids.push(u128::from_be_bytes(bytes));
                        }
                        uuid_chunk(&mut uuids, levels, n, column)
                    },
                )?,
                other => return Err(unsupported_decode(&name, other)),
            }
        }
        (physical, _) => {
            return Err(PinkDbError::Execution(format!(
                "column \"{name}\" has physical type {physical:?}, which does not match its \
                 engine type"
            )));
        }
    };
    Ok(chunks)
}

/// Decode one ARRAY column chunk: `read_array_chunks` does the work, this
/// only picks the element reader from the physical type.
fn read_array_column(
    row_group: &dyn RowGroupReader,
    column: &FileColumn,
    lens: ChunkLens,
) -> Result<Vec<Vector>> {
    let leaf = column.leaf;
    let ty = column.ty;
    let name = column.name.clone();
    let reader = row_group.get_column_reader(leaf).map_err(|e| {
        PinkDbError::Execution(format!("cannot read column \"{name}\" (leaf {leaf}): {e}"))
    })?;
    match (column.physical, reader) {
        (parquet::basic::Type::INT32, ColumnReader::Int32ColumnReader(r)) => {
            let mut r = get_typed_column_reader::<Int32Type>(ColumnReader::Int32ColumnReader(r));
            read_array_chunks(&mut r, lens, ty, &name, |v| Value::Integer(*v))
        }
        (parquet::basic::Type::INT64, ColumnReader::Int64ColumnReader(r)) => {
            let mut r = get_typed_column_reader::<Int64Type>(ColumnReader::Int64ColumnReader(r));
            read_array_chunks(&mut r, lens, ty, &name, |v| Value::Bigint(*v))
        }
        (parquet::basic::Type::FLOAT, ColumnReader::FloatColumnReader(r)) => {
            let mut r = get_typed_column_reader::<FloatType>(ColumnReader::FloatColumnReader(r));
            read_array_chunks(&mut r, lens, ty, &name, |v| Value::Double(*v as f64))
        }
        (parquet::basic::Type::DOUBLE, ColumnReader::DoubleColumnReader(r)) => {
            let mut r = get_typed_column_reader::<DoubleType>(ColumnReader::DoubleColumnReader(r));
            read_array_chunks(&mut r, lens, ty, &name, |v| Value::Double(*v))
        }
        (physical, _) => Err(PinkDbError::Execution(format!(
            "column \"{name}\" has physical type {physical:?}, which is not an ARRAY element type"
        ))),
    }
}

/// Decode a LIST-encoded ARRAY column. Every row holds exactly `len` level
/// entries — a NULL array keeps its slots at definition level 0, a NULL element
/// is level 1 — so rows are regrouped by the repetition level and a row of any
/// other width is a variable-length LIST, refused rather than misread.
fn read_array_chunks<T>(
    reader: &mut parquet::column::reader::ColumnReaderImpl<T>,
    lens: ChunkLens,
    dtype: DataType,
    column: &str,
    convert: impl Fn(&T::T) -> Value,
) -> Result<Vec<Vector>>
where
    T: parquet::data_type::DataType,
{
    let DataType::Array { elem, len } = dtype else {
        unreachable!("read_array_chunks is called for ARRAY columns only")
    };
    let len = len as usize;
    let rows = lens.rows();
    let mut out: Vec<Vector> = Vec::with_capacity(lens.chunks().min(CHUNK_SIZE));
    let mut vector = Vector::with_capacity(dtype, CHUNK_SIZE.min(rows));
    let mut in_chunk = 0usize;
    let mut done = 0usize;
    let mut values: Vec<T::T> = Vec::new();
    let mut levels: Vec<i16> = Vec::new();
    let mut reps: Vec<i16> = Vec::new();
    // The row being assembled, one slot per element (`Value::Null` for a NULL
    // element) and `null_row` for a NULL array. A batch can end mid-row. The
    // ARRAY length comes from the footer's key-value metadata, so the row's
    // buffer is hinted, not sized: a hostile length must not allocate it.
    let mut row: Vec<Value> = Vec::with_capacity(len.min(CHUNK_SIZE));
    let mut null_row = false;
    for n in lens {
        let want = n.saturating_mul(len);
        values.clear();
        levels.clear();
        reps.clear();
        let read = reader
            .read_records(want, Some(&mut levels), Some(&mut reps), &mut values)
            .map_err(|e| {
                PinkDbError::Execution(format!(
                    "cannot decode column \"{column}\" at row {done}: {e}"
                ))
            })?;
        // `read_records` returns `(rows, values, levels)`; its row count is
        // the parquet reader's own accounting, so the batch is walked by its
        // level entries (`levels.len()`) instead. A batch may end mid-row.
        let records = read.0;
        if records == 0 {
            return Err(PinkDbError::Execution(format!(
                "column \"{column}\" ended after {done} of {rows} rows"
            )));
        }
        let mut next_value = 0usize;
        for record in 0..levels.len() {
            let rep = reps.get(record).copied().unwrap_or(0);
            if rep == 0 && !row.is_empty() {
                return Err(width_error(column, row.len(), len));
            }
            // Definition levels are 1 for a NULL array (`v` is OPTIONAL and
            // its `list` child REPEATED), 2 for a NULL element, 3 for a value.
            let level = levels.get(record).copied().unwrap_or(0);
            let value = match level {
                0 | 1 => {
                    if row.is_empty() {
                        null_row = true;
                    }
                    Value::Null
                }
                2 => Value::Null,
                _ => {
                    let Some(value) = values.get(next_value) else {
                        return Err(PinkDbError::Execution(format!(
                            "column \"{column}\": a definition level has no value"
                        )));
                    };
                    next_value += 1;
                    convert(value)
                }
            };
            row.push(value);
            if row.len() > len {
                return Err(width_error(column, row.len(), len));
            }
            if row.len() < len {
                continue;
            }
            if null_row {
                vector.push_null()?;
            } else {
                let array = pinkdb::array::ArrayValue::from_values(elem, len as u32, &row)?;
                vector.push_value(Value::Array(std::sync::Arc::new(array)))?;
            }
            row.clear();
            null_row = false;
            in_chunk += 1;
            done += 1;
            if in_chunk == n {
                out.push(std::mem::replace(
                    &mut vector,
                    Vector::with_capacity(dtype, CHUNK_SIZE),
                ));
                in_chunk = 0;
            }
        }
    }
    if in_chunk > 0 {
        out.push(vector);
    }
    Ok(out)
}

fn width_error(column: &str, found: usize, len: usize) -> PinkDbError {
    PinkDbError::Type(format!(
        "column \"{column}\" holds a row of {found} elements, not the {len} of its ARRAY type; \
         a variable-length list is not supported (wave 107a2)"
    ))
}

fn unsupported_decode(name: &str, ty: DataType) -> PinkDbError {
    PinkDbError::Execution(format!(
        "column \"{name}\": no decoder for the mapped type {ty}"
    ))
}

/// The parquet INTERVAL encoding: 12 little-endian bytes, months/days/millis.
fn interval_parts(bytes: &[u8]) -> Option<Interval> {
    if bytes.len() != 12 {
        return None;
    }
    let months = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i32;
    let days = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as i32;
    let millis = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as i64;
    Some(Interval {
        months,
        days,
        micros: millis * 1_000,
    })
}

/// A decode error about `column`; `why` is the bare reason.
fn decode_error(column: &str, why: impl std::fmt::Display) -> PinkDbError {
    PinkDbError::Execution(format!("column \"{column}\": {why}"))
}

/// Read `rows` records of a flat column into chunk-sized engine vectors: one
/// typed batch per chunk, and `finish` turns the batch's decoded values (in row
/// order), its definition levels and its row count into the vector.
///
/// This is the decoder's hot path, so no [`Value`] is built and no cell is
/// pushed one at a time: parquet's buffers land in the `VectorData` payload
/// directly, which also lets a VARCHAR chunk be copied into a `StrVec` without
/// a `String` per cell. A chunk is gathered across page boundaries (parquet's
/// `read_records` appends to its buffers), the way DuckDB's parquet reader
/// decodes a whole batch into one column vector
/// (duckdb/extension/parquet/column_reader.cpp, `ColumnReader::Read`).
fn read_flat_chunks<T>(
    reader: &mut parquet::column::reader::ColumnReaderImpl<T>,
    lens: ChunkLens,
    optional: bool,
    column: &str,
    mut finish: impl FnMut(&mut Vec<T::T>, &[i16], usize, &str) -> Result<Vector>,
) -> Result<Vec<Vector>>
where
    T: parquet::data_type::DataType,
{
    let rows = lens.rows();
    let mut out = Vec::with_capacity(lens.chunks().min(CHUNK_SIZE));
    let mut done = 0usize;
    let mut values: Vec<T::T> = Vec::new();
    let mut levels: Vec<i16> = Vec::new();
    for n in lens {
        values.clear();
        levels.clear();
        let mut filled = 0usize;
        while filled < n {
            let want = n - filled;
            // A REQUIRED column has no definition levels to collect.
            let levels_out = if optional { Some(&mut levels) } else { None };
            let read = reader
                .read_records(want, levels_out, None, &mut values)
                .map_err(|e| {
                    PinkDbError::Execution(format!(
                        "cannot decode column \"{column}\" ({want} more rows): {e}"
                    ))
                })?;
            let records = read.0;
            if records == 0 {
                return Err(PinkDbError::Execution(format!(
                    "column \"{column}\" ended after {} of {rows} rows",
                    done + filled
                )));
            }
            filled += records;
        }
        out.push(finish(&mut values, &levels, n, column)?);
        done += n;
    }
    Ok(out)
}

/// Whether each of a chunk's `n` rows has a value, from its definition levels
/// (empty for a REQUIRED column).
fn present_rows(levels: &[i16], n: usize, column: &str) -> Result<Vec<bool>> {
    if levels.is_empty() {
        return Ok(vec![true; n]);
    }
    if levels.len() < n {
        return Err(PinkDbError::Execution(format!(
            "column \"{column}\" ended after {} of {n} rows",
            levels.len()
        )));
    }
    Ok(levels[..n].iter().map(|&level| level >= 1).collect())
}

/// One chunk of a flat column as a payload in row order, with `fill` in the
/// NULL rows: `Vector::from_raw` takes the length from the payload, so a NULL
/// row still needs an element.
fn flat_chunk<T: Copy>(
    present: &mut Vec<T>,
    levels: &[i16],
    n: usize,
    column: &str,
    fill: T,
) -> Result<(Vec<T>, Vec<u64>)> {
    let has = present_rows(levels, n, column)?;
    if has.iter().all(|&has_value| has_value) {
        if present.len() != n {
            return Err(decode_error(column, "a definition level has no value"));
        }
        return Ok((std::mem::take(present), vec![u64::MAX; n.div_ceil(64)]));
    }
    let mut values = Vec::with_capacity(n);
    let mut validity = vec![u64::MAX; n.div_ceil(64)];
    let mut next = 0usize;
    for (row, &has_value) in has.iter().enumerate() {
        if !has_value {
            values.push(fill);
            validity[row / 64] &= !(1u64 << (row % 64));
            continue;
        }
        let Some(value) = present.get(next) else {
            return Err(decode_error(column, "a definition level has no value"));
        };
        values.push(*value);
        next += 1;
    }
    Ok((values, validity))
}

fn bool_chunk(values: &mut Vec<bool>, levels: &[i16], n: usize, column: &str) -> Result<Vector> {
    let (values, validity) = flat_chunk(values, levels, n, column, false)?;
    Ok(Vector::from_raw(VectorData::Boolean(values), validity))
}

fn int32_chunk(values: &mut Vec<i32>, levels: &[i16], n: usize, column: &str) -> Result<Vector> {
    let (values, validity) = flat_chunk(values, levels, n, column, 0)?;
    Ok(Vector::from_raw(VectorData::Int32(values), validity))
}

fn date_chunk(values: &mut Vec<i32>, levels: &[i16], n: usize, column: &str) -> Result<Vector> {
    let (values, validity) = flat_chunk(values, levels, n, column, 0)?;
    Ok(Vector::from_raw(VectorData::Date32(values), validity))
}

fn int64_chunk(values: &mut Vec<i64>, levels: &[i16], n: usize, column: &str) -> Result<Vector> {
    let (values, validity) = flat_chunk(values, levels, n, column, 0)?;
    Ok(Vector::from_raw(VectorData::Int64(values), validity))
}

fn timestamp_chunk(
    values: &mut Vec<i64>,
    levels: &[i16],
    n: usize,
    column: &str,
) -> Result<Vector> {
    let (values, validity) = flat_chunk(values, levels, n, column, 0)?;
    Ok(Vector::from_raw(
        VectorData::TimestampMicros(values),
        validity,
    ))
}

fn f64_chunk(values: &mut Vec<f64>, levels: &[i16], n: usize, column: &str) -> Result<Vector> {
    let (values, validity) = flat_chunk(values, levels, n, column, 0.0)?;
    Ok(Vector::from_raw(VectorData::Float64(values), validity))
}

fn uuid_chunk(values: &mut Vec<u128>, levels: &[i16], n: usize, column: &str) -> Result<Vector> {
    let (values, validity) = flat_chunk(values, levels, n, column, 0)?;
    Ok(Vector::from_raw(VectorData::Uuid(values), validity))
}

/// DECIMAL: the unscaled values plus the column's own precision and scale, so
/// the vector carries the declared `DECIMAL(p,s)`.
fn decimal_chunk(
    values: &mut Vec<i128>,
    levels: &[i16],
    n: usize,
    column: &str,
    precision: u8,
    scale: u8,
) -> Result<Vector> {
    let (values, validity) = flat_chunk(values, levels, n, column, 0)?;
    Ok(Vector::from_raw(
        VectorData::Decimal {
            values,
            precision,
            scale,
        },
        validity,
    ))
}

// Every column of a row group is finished by the same callback, which hands
// out the decoder's buffer by `&mut Vec` (`flat_chunk` takes it over); this
// one only reads it.
#[allow(clippy::ptr_arg)]
fn varchar_chunk(
    values: &mut Vec<ByteArray>,
    levels: &[i16],
    n: usize,
    column: &str,
) -> Result<Vector> {
    let has = present_rows(levels, n, column)?;
    let mut strings = StrVec::with_capacity(n);
    let mut validity = vec![u64::MAX; n.div_ceil(64)];
    let mut next = 0usize;
    for (row, &has_value) in has.iter().enumerate() {
        if !has_value {
            // Any payload does for a NULL row; the validity bit decides it.
            strings.push("");
            validity[row / 64] &= !(1u64 << (row % 64));
            continue;
        }
        let Some(value) = values.get(next) else {
            return Err(decode_error(column, "a definition level has no value"));
        };
        next += 1;
        strings.push(
            std::str::from_utf8(value.data())
                .map_err(|_| decode_error(column, "the bytes are not valid UTF-8"))?,
        );
    }
    Ok(Vector::from_raw(VectorData::VarChar(strings), validity))
}

#[allow(clippy::ptr_arg)]
fn interval_chunk(
    values: &mut Vec<FixedLenByteArray>,
    levels: &[i16],
    n: usize,
    column: &str,
) -> Result<Vector> {
    let has = present_rows(levels, n, column)?;
    let mut intervals = Vec::with_capacity(n);
    let mut validity = vec![u64::MAX; n.div_ceil(64)];
    let mut next = 0usize;
    for (row, &has_value) in has.iter().enumerate() {
        let mut interval = None;
        if has_value {
            let Some(value) = values.get(next) else {
                return Err(decode_error(column, "a definition level has no value"));
            };
            next += 1;
            interval = interval_parts(value.data());
        }
        // Bytes that are not 12 wide hold no interval: the row is NULL.
        match interval {
            Some(interval) => intervals.push(interval),
            None => {
                intervals.push(Interval::default());
                validity[row / 64] &= !(1u64 << (row % 64));
            }
        }
    }
    Ok(Vector::from_raw(VectorData::Interval(intervals), validity))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolve::Options;
    use parquet::basic::{Compression, Repetition};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type as SchemaType;
    use pinkdb::DataTable;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_parquet_scan_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn chunk_lens_fills_vectors_and_keeps_the_file_aligned() {
        // A row group that starts mid-chunk emits a short first piece; every
        // later piece is full, so the next row group starts aligned again.
        let lens: Vec<usize> = chunk_lens(5000, 1000).collect();
        assert_eq!(lens[0], CHUNK_SIZE - 1000);
        assert_eq!(&lens[1..], &[CHUNK_SIZE, 1904]);
        assert_eq!(lens.iter().sum::<usize>(), 5000);
        // Aligned starts keep the start aligned, and a ragged tail is last.
        assert_eq!(
            chunk_lens(3000, 0).collect::<Vec<usize>>(),
            vec![CHUNK_SIZE, 952]
        );
        assert_eq!(
            chunk_lens(0, 7).collect::<Vec<usize>>(),
            Vec::<usize>::new()
        );
        for (rows, offset) in [(1, 0), (2048, 0), (2049, 5), (9000, 2047), (77, 3)] {
            let lens = chunk_lens(rows, offset);
            assert_eq!(lens.rows(), rows);
            assert_eq!(lens.chunks(), lens.collect::<Vec<usize>>().len());
            assert!(lens.into_iter().all(|n| n > 0 && n <= CHUNK_SIZE));
            // Only the first piece of a row group may be short: it ends on the
            // boundary the previous row group left open.
            let mut at = offset;
            for (i, n) in lens.enumerate() {
                if i > 0 {
                    assert!(
                        at.is_multiple_of(CHUNK_SIZE),
                        "chunk {i} of {rows} rows at {offset}"
                    );
                }
                at += n;
            }
        }
    }

    #[test]
    fn chunk_lens_counts_without_allocating_for_a_hostile_row_count() {
        // A footer can claim any row count; lengths must be arithmetic, not a
        // 4.5e15-element vector (the `huge_rg_num_rows.parquet` abort).
        let lens = chunk_lens(usize::MAX / 2, 0);
        assert_eq!(lens.chunks(), (usize::MAX / 2).div_ceil(CHUNK_SIZE));
        assert_eq!(lens.into_iter().next(), Some(CHUNK_SIZE));
    }

    #[test]
    fn page_locations_outside_the_file_are_refused() {
        let page = |offset, size: i32, row: i64| PageLocation {
            offset,
            compressed_page_size: size,
            first_row_index: row,
        };
        assert!(page_locations_fit(
            &[page(4, 10, 0), page(14, 10, 5)],
            10,
            24
        ));
        // A size or offset past the end of the file (or an i32::MAX page).
        assert!(!page_locations_fit(&[page(4, i32::MAX, 0)], 10, 24));
        assert!(!page_locations_fit(&[page(20, 10, 0)], 10, 24));
        assert!(!page_locations_fit(&[page(-1, 10, 0)], 10, 24));
        // Row indexes must start at 0, increase, and stay below the rows.
        assert!(!page_locations_fit(&[page(4, 10, 1)], 10, 24));
        assert!(!page_locations_fit(
            &[page(4, 10, 0), page(14, 10, 0)],
            10,
            24
        ));
        assert!(!page_locations_fit(
            &[page(4, 10, 0), page(14, 10, 10)],
            10,
            24
        ));
    }

    /// Every row of the file behind `resolved`, read either on the pool or on
    /// one thread, with the row counts of the chunks that mode emitted.
    fn read(resolved: &Resolved, serial: bool) -> (Vec<Vec<Value>>, Vec<usize>) {
        let output = if serial {
            scan_serial(resolved, None, &[]).expect("serial scan")
        } else {
            scan(resolved, None, &[]).expect("parallel scan").0
        };
        let mut rows = Vec::new();
        let mut lens = Vec::new();
        for chunk in &output.chunks {
            lens.push(chunk.num_rows);
            for r in 0..chunk.num_rows {
                rows.push(
                    (0..chunk.columns.len())
                        .map(|c| chunk.columns[c].get(r))
                        .collect(),
                );
            }
        }
        (rows, lens)
    }

    /// Read `path` both ways: identical rows in identical order and identical
    /// chunks, and every chunk starts on a `CHUNK_SIZE` boundary or at a row
    /// group's first row (which is where the alignment of the previous group's
    /// ragged tail was picked up).
    fn assert_both_modes_agree(path: &Path) {
        let resolved = crate::resolve::resolve(&path.display().to_string(), &Options::default())
            .expect("resolve");
        assert_eq!(resolved.files.len(), 1, "one file per fixture");
        let mut group_starts = Vec::new();
        let mut at = 0usize;
        for rg in &resolved.files[0].meta.row_groups {
            group_starts.push(at);
            at += rg.rows as usize;
        }
        let (parallel, parallel_lens) = read(&resolved, false);
        let (serial, serial_lens) = read(&resolved, true);
        assert_eq!(parallel_lens, serial_lens, "chunk row counts");
        let mut start = 0usize;
        for &n in &parallel_lens {
            assert!(
                start.is_multiple_of(CHUNK_SIZE) || group_starts.contains(&start),
                "chunk at row {start} starts neither on a boundary nor at a row group: \
                 {parallel_lens:?}"
            );
            start += n;
        }
        assert_eq!(start, parallel.len(), "chunk rows sum to the file");
        assert_eq!(parallel.len(), serial.len(), "row count");
        for (r, (want, got)) in serial.iter().zip(&parallel).enumerate() {
            assert_eq!(got, want, "row {r}");
        }
    }

    /// A three-column file (`i` nullable INT32, `s` dictionary BYTE_ARRAY,
    /// `b` RLE BOOLEAN) laid out in row groups of the given sizes.
    fn write_groups(path: &Path, groups: &[usize]) {
        let optional = |name: &str, physical| {
            Arc::new(
                SchemaType::primitive_type_builder(name, physical)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap(),
            )
        };
        let schema = Arc::new(
            SchemaType::group_type_builder("pinkdb_schema")
                .with_fields(vec![
                    optional("i", parquet::basic::Type::INT32),
                    optional("s", parquet::basic::Type::BYTE_ARRAY),
                    optional("b", parquet::basic::Type::BOOLEAN),
                ])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let file = std::fs::File::create(path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut written = 0usize;
        for &rows in groups {
            let window = written..written + rows;
            let levels = |step: usize| -> Vec<i16> {
                window
                    .clone()
                    .map(|r| if r % step == 0 { 0 } else { 1 })
                    .collect()
            };
            let mut group = writer.next_row_group().unwrap();
            let mut column = group.next_column().unwrap().unwrap();
            let values: Vec<i32> = window
                .clone()
                .filter(|r| r % 5 != 0)
                .map(|r| r as i32)
                .collect();
            column
                .typed::<Int32Type>()
                .write_batch(&values, Some(&levels(5)), None)
                .unwrap();
            column.close().unwrap();
            let mut column = group.next_column().unwrap().unwrap();
            let values: Vec<ByteArray> = window
                .clone()
                .filter(|r| r % 7 != 0)
                .map(|r| ByteArray::from(["a", "bb", "ccc"][r % 3].as_bytes()))
                .collect();
            column
                .typed::<ByteArrayType>()
                .write_batch(&values, Some(&levels(7)), None)
                .unwrap();
            column.close().unwrap();
            let mut column = group.next_column().unwrap().unwrap();
            let values: Vec<bool> = window
                .clone()
                .filter(|r| r % 11 != 0)
                .map(|r| r % 3 == 0)
                .collect();
            column
                .typed::<BoolType>()
                .write_batch(&values, Some(&levels(11)), None)
                .unwrap();
            column.close().unwrap();
            group.close().unwrap();
            written += rows;
        }
        writer.close().unwrap();
    }

    /// Row groups of different sizes chunk into one aligned stream, and both
    /// decode modes return the same rows in the same order.
    #[test]
    fn row_groups_of_different_sizes_read_the_same_in_both_modes() {
        let groups = [1usize, 2047, 3000, 5, 2048, 977, 922];
        assert_eq!(groups.iter().sum::<usize>(), 9000);
        let dir = scratch("groups");
        let path = dir.join("groups.parquet");
        write_groups(&path, &groups);
        assert_both_modes_agree(&path);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Every engine type the decoder builds from a payload (BOOLEAN, integers,
    /// DOUBLE, dictionary VARCHAR, DATE, TIMESTAMP, DECIMAL at both widths,
    /// NULLs throughout) reads the same on the pool and on one thread.
    #[test]
    fn every_engine_type_reads_the_same_in_both_modes() {
        let schema: Vec<(String, DataType)> = [
            ("b", DataType::Boolean),
            ("i", DataType::Integer),
            ("g", DataType::Bigint),
            ("d", DataType::Double),
            ("s", DataType::Varchar),
            ("dt", DataType::Date),
            ("ts", DataType::Timestamp),
            (
                "dc9",
                DataType::Decimal {
                    precision: 9,
                    scale: 2,
                },
            ),
            (
                "dc38",
                DataType::Decimal {
                    precision: 38,
                    scale: 6,
                },
            ),
        ]
        .into_iter()
        .map(|(name, ty)| (name.to_string(), ty))
        .collect();
        let mut table = DataTable::new("all_types".to_string(), schema).expect("table");
        for r in 0..3000usize {
            table
                .append_row(vec![
                    if r % 5 == 0 {
                        Value::Null
                    } else {
                        Value::Boolean(r % 3 == 0)
                    },
                    Value::Integer(r as i32 - 1500),
                    if r % 7 == 0 {
                        Value::Null
                    } else {
                        Value::Bigint(-3 * r as i64)
                    },
                    if r % 9 == 0 {
                        Value::Null
                    } else {
                        Value::Double(r as f64 / 7.0)
                    },
                    if r % 4 == 0 {
                        Value::Null
                    } else {
                        Value::Varchar(format!("v{r}-{}", "x".repeat(r % 20)))
                    },
                    Value::Date(19_000 + r as i32 % 400),
                    Value::Timestamp(1_700_000_000_000_000 + r as i64),
                    Value::decimal(r as i128 % 9000 - 4500, 2),
                    Value::Decimal {
                        value: 10i128.pow(30) + r as i128,
                        precision: 38,
                        scale: 6,
                    },
                ])
                .unwrap();
        }
        let bytes = crate::write::write_table(&table).expect("write");
        let dir = scratch("types");
        let path = dir.join("types.parquet");
        std::fs::write(&path, &bytes).unwrap();
        assert_both_modes_agree(&path);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A one-column VARCHAR file: `distinct` seeded values, a NULL every 13th
    /// row and an empty string every 17th.
    fn write_dictionary(
        path: &Path,
        rows: usize,
        distinct: usize,
        props: WriterProperties,
    ) -> Vec<Value> {
        let field = Arc::new(
            SchemaType::primitive_type_builder("s", parquet::basic::Type::BYTE_ARRAY)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .unwrap(),
        );
        let schema = Arc::new(
            SchemaType::group_type_builder("pinkdb_schema")
                .with_fields(vec![field])
                .build()
                .unwrap(),
        );
        let file = std::fs::File::create(path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props)).unwrap();
        let mut expected = Vec::with_capacity(rows);
        let mut values = Vec::new();
        let mut levels = Vec::with_capacity(rows);
        for row in 0..rows {
            if row % 13 == 0 {
                levels.push(0);
                expected.push(Value::Null);
            } else {
                levels.push(1);
                let text = if row % 17 == 0 {
                    String::new()
                } else {
                    format!("{:0>4}", (row * 7919) % distinct)
                };
                values.push(ByteArray::from(text.as_bytes()));
                expected.push(Value::Varchar(text));
            }
        }
        let mut group = writer.next_row_group().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        column
            .typed::<ByteArrayType>()
            .write_batch(&values, Some(&levels), None)
            .unwrap();
        column.close().unwrap();
        group.close().unwrap();
        writer.close().unwrap();
        expected
    }

    /// The encodings of a column's pages, dictionary page included.
    fn page_encodings(path: &Path, column: usize) -> Vec<parquet::basic::Encoding> {
        use parquet::column::page::Page;
        let reader = SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap();
        let group = reader.get_row_group(0).unwrap();
        let mut pages = group.get_column_page_reader(column).unwrap();
        let mut encodings = Vec::new();
        while let Some(page) = pages.get_next_page().unwrap() {
            encodings.push(match page {
                Page::DictionaryPage { encoding, .. } => encoding,
                Page::DataPage { encoding, .. } => encoding,
                Page::DataPageV2 { encoding, .. } => encoding,
            });
        }
        encodings
    }

    fn resolve_one(path: &Path) -> Resolved {
        crate::resolve::resolve(&path.display().to_string(), &Options::default()).expect("resolve")
    }

    /// Every row a scan with `filters` emits, in the order it emits them.
    fn read_filtered(
        resolved: &Resolved,
        projection: Option<&[usize]>,
        filters: &[PrunePredicate],
        serial: bool,
    ) -> Vec<Vec<Value>> {
        let output = if serial {
            scan_serial(resolved, projection, filters).expect("serial scan")
        } else {
            scan(resolved, projection, filters)
                .expect("parallel scan")
                .0
        };
        let mut rows = Vec::new();
        for chunk in &output.chunks {
            for r in 0..chunk.num_rows {
                // Columns the projection does not read are zero-length
                // placeholders.
                rows.push(
                    (0..chunk.columns.len())
                        .filter(|&c| !chunk.columns[c].is_empty())
                        .map(|c| chunk.columns[c].get(r))
                        .collect(),
                );
            }
        }
        rows
    }

    /// Whether `row` passes `filter`, i.e. the predicate is TRUE for it. A
    /// NULL cell or NULL literal is never TRUE, and `IN` matches no row for an
    /// empty list or a NULL list element.
    fn holds(row: &[Value], filter: &PrunePredicate) -> bool {
        let cell = &row[filter.column];
        match filter.op {
            PruneOp::IsNull => cell.is_null(),
            PruneOp::IsNotNull => !cell.is_null(),
            PruneOp::In => {
                !cell.is_null()
                    && filter
                        .values
                        .iter()
                        .any(|value| !value.is_null() && value == cell)
            }
            op => {
                let literal = filter.value.as_ref().expect("comparison literal");
                if cell.is_null() || literal.is_null() {
                    return false;
                }
                let order = pinkdb::plan::compare_values(cell, literal);
                match op {
                    PruneOp::Eq => order.is_eq(),
                    PruneOp::NotEq => !order.is_eq(),
                    PruneOp::Lt => order.is_lt(),
                    PruneOp::LtEq => !order.is_gt(),
                    PruneOp::Gt => order.is_gt(),
                    PruneOp::GtEq => !order.is_lt(),
                    PruneOp::IsNull | PruneOp::IsNotNull | PruneOp::In => {
                        unreachable!("handled above")
                    }
                }
            }
        }
    }

    /// Several data pages in one chunk, a dictionary page, NULLs and empty
    /// strings: every row comes back unchanged in both decode modes.
    #[test]
    fn dictionary_pages_round_trip_in_both_modes() {
        let dir = scratch("dict_pages");
        let path = dir.join("dict.parquet");
        let props = WriterProperties::builder()
            .set_data_page_size_limit(1024)
            .build();
        let expected = write_dictionary(&path, 5000, 500, props);
        let encodings = page_encodings(&path, 0);
        assert!(
            encodings
                .iter()
                .any(|e| matches!(e, parquet::basic::Encoding::PLAIN_DICTIONARY)
                    || matches!(e, parquet::basic::Encoding::RLE_DICTIONARY)),
            "the fixture has no dictionary-encoded data page: {encodings:?}"
        );
        assert!(encodings.len() > 2, "one page only: {encodings:?}");
        let resolved = resolve_one(&path);
        for serial in [false, true] {
            let (rows, lens) = read(&resolved, serial);
            assert!(lens.len() > 1, "one chunk only: {lens:?}");
            assert_eq!(rows.len(), expected.len());
            for (r, (got, want)) in rows.iter().zip(&expected).enumerate() {
                assert_eq!(
                    got.as_slice(),
                    std::slice::from_ref(want),
                    "row {r} (serial: {serial})"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A pushed filter keeps exactly the rows the same predicate marks TRUE:
    /// every comparison, both NULL tests and several `IN` lists, on a column
    /// whose chunk takes both the dictionary path and the PLAIN fallback.
    #[test]
    fn dictionary_filters_keep_exactly_the_matching_rows() {
        let dir = scratch("dict_filters");
        for (tag, props, exact) in [
            (
                "dictionary",
                WriterProperties::builder()
                    .set_data_page_size_limit(1024)
                    .build(),
                true,
            ),
            (
                "dictionary_and_plain",
                WriterProperties::builder()
                    .set_data_page_size_limit(1024)
                    .set_dictionary_page_size_limit(4096)
                    .build(),
                false,
            ),
        ] {
            let path = dir.join(format!("{tag}.parquet"));
            let expected = write_dictionary(&path, 4000, 4000, props);
            let resolved = resolve_one(&path);
            let filters = [
                PrunePredicate::compare(0, PruneOp::Eq, Value::Varchar("0042".into())),
                PrunePredicate::compare(0, PruneOp::NotEq, Value::Varchar("0042".into())),
                PrunePredicate::compare(0, PruneOp::Lt, Value::Varchar("0500".into())),
                PrunePredicate::compare(0, PruneOp::LtEq, Value::Varchar("0500".into())),
                PrunePredicate::compare(0, PruneOp::Gt, Value::Varchar("3500".into())),
                PrunePredicate::compare(0, PruneOp::GtEq, Value::Varchar("3500".into())),
                PrunePredicate::null_test(0, false),
                PrunePredicate::null_test(0, true),
                PrunePredicate::in_list(
                    0,
                    ["0042", "0007", "1234", "", "3999"]
                        .iter()
                        .map(|s| Value::Varchar((*s).into()))
                        .collect(),
                ),
                PrunePredicate::in_list(0, vec![Value::Varchar("0042".into()), Value::Null]),
                PrunePredicate::in_list(
                    0,
                    vec![Value::Varchar("0042".into()), Value::Varchar("0042".into())],
                ),
                PrunePredicate::in_list(0, Vec::new()),
            ];
            for filter in &filters {
                let want: Vec<Vec<Value>> = expected
                    .iter()
                    .filter(|cell| holds(std::slice::from_ref(*cell), filter))
                    .map(|cell| vec![cell.clone()])
                    .collect();
                for serial in [false, true] {
                    let got = read_filtered(&resolved, None, std::slice::from_ref(filter), serial);
                    if exact {
                        assert_eq!(got, want, "{tag}: {filter:?} (serial: {serial})");
                    } else {
                        // A chunk that spills to PLAIN pages has no dictionary
                        // to select from, so the scan returns a superset; the
                        // engine's `WHERE` above the scan drops the rest.
                        let kept: Vec<Vec<Value>> = got
                            .iter()
                            .filter(|row| holds(row, filter))
                            .cloned()
                            .collect();
                        assert_eq!(kept, want, "{tag}: {filter:?} (serial: {serial})");
                    }
                }
            }
            let _ = std::fs::remove_file(&path);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A filter on a column the projection does not read still drops rows, and
    /// the projected column stays aligned with them; a predicate on a column
    /// that has no dictionary keeps the same contract.
    #[test]
    fn dictionary_filter_on_an_unprojected_column_keeps_rows_aligned() {
        let dir = scratch("dict_filter_only");
        let path = dir.join("filter_only.parquet");
        write_groups(&path, &[4000]);
        let resolved = resolve_one(&path);
        let (all, _) = read(&resolved, false);
        let filters = [
            PrunePredicate::in_list(
                1,
                vec![Value::Varchar("bb".into()), Value::Varchar("ccc".into())],
            ),
            PrunePredicate::compare(1, PruneOp::Eq, Value::Varchar("bb".into())),
            PrunePredicate::compare(1, PruneOp::Gt, Value::Varchar("a".into())),
            PrunePredicate::null_test(1, false),
            PrunePredicate::in_list(0, vec![Value::Integer(7), Value::Integer(11)]),
            PrunePredicate::compare(0, PruneOp::LtEq, Value::Integer(3)),
        ];
        for filter in &filters {
            let want: Vec<Vec<Value>> = all
                .iter()
                .filter(|row| holds(row, filter))
                .map(|row| row[..2].to_vec())
                .collect();
            assert!(
                !want.is_empty() && want.len() < all.len(),
                "{filter:?} keeps everything or nothing"
            );
            for projection in [&[0usize][..], &[1][..], &[0, 1][..]] {
                for serial in [false, true] {
                    let got = read_filtered(
                        &resolved,
                        Some(projection),
                        std::slice::from_ref(filter),
                        serial,
                    );
                    let want: Vec<Vec<Value>> = want
                        .iter()
                        .map(|row| projection.iter().map(|&c| row[c].clone()).collect())
                        .collect();
                    if filter.column == 1 {
                        // A dictionary-encoded predicate column selects rows
                        // even when the projection does not read it.
                        assert_eq!(
                            got, want,
                            "{filter:?} projected {projection:?} (serial: {serial})"
                        );
                    } else {
                        // An INTEGER column decodes through the typed path and
                        // has no dictionary to select from: the scan returns
                        // every row and the engine's `WHERE` drops the
                        // rejected ones.
                        let everything: Vec<Vec<Value>> = all
                            .iter()
                            .map(|row| projection.iter().map(|&c| row[c].clone()).collect())
                            .collect();
                        assert_eq!(
                            got, everything,
                            "{filter:?} projected {projection:?} (serial: {serial})"
                        );
                    }
                }
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A dictionary that spills to PLAIN pages mid-chunk still reads back
    /// exactly, in both modes.
    #[test]
    fn a_plain_fallback_page_still_round_trips() {
        let dir = scratch("dict_fallback");
        let path = dir.join("fallback.parquet");
        let props = WriterProperties::builder()
            .set_data_page_size_limit(1024)
            .set_dictionary_page_size_limit(4096)
            .build();
        let expected = write_dictionary(&path, 4000, 4000, props);
        let encodings = page_encodings(&path, 0);
        assert!(
            encodings
                .iter()
                .any(|e| matches!(e, parquet::basic::Encoding::PLAIN)),
            "the dictionary never spilled to PLAIN pages: {encodings:?}"
        );
        let resolved = resolve_one(&path);
        for serial in [false, true] {
            let (rows, _) = read(&resolved, serial);
            assert_eq!(rows.len(), expected.len());
            for (r, (got, want)) in rows.iter().zip(&expected).enumerate() {
                assert_eq!(
                    got.as_slice(),
                    std::slice::from_ref(want),
                    "row {r} (serial: {serial})"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Invalid UTF-8 in a dictionary entry fails with the text a value-by-value
    /// decode produces today.
    #[test]
    fn invalid_utf8_in_a_dictionary_entry_keeps_todays_error() {
        let dir = scratch("dict_utf8");
        let path = dir.join("utf8.parquet");
        let field = Arc::new(
            SchemaType::primitive_type_builder("s", parquet::basic::Type::BYTE_ARRAY)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .unwrap(),
        );
        let schema = Arc::new(
            SchemaType::group_type_builder("pinkdb_schema")
                .with_fields(vec![field])
                .build()
                .unwrap(),
        );
        let mut writer = SerializedFileWriter::new(
            std::fs::File::create(&path).unwrap(),
            schema,
            Arc::new(WriterProperties::builder().build()),
        )
        .unwrap();
        let mut group = writer.next_row_group().unwrap();
        let mut column = group.next_column().unwrap().unwrap();
        let values = [ByteArray::from(&[0xffu8, 0xfe][..])];
        let levels = [1i16];
        column
            .typed::<ByteArrayType>()
            .write_batch(&values, Some(&levels), None)
            .unwrap();
        column.close().unwrap();
        group.close().unwrap();
        writer.close().unwrap();
        assert!(
            page_encodings(&path, 0).iter().any(|e| matches!(
                e,
                parquet::basic::Encoding::PLAIN_DICTIONARY
            ) || matches!(
                e,
                parquet::basic::Encoding::RLE_DICTIONARY
            )),
            "the fixture is not dictionary-encoded"
        );
        let resolved = resolve_one(&path);
        for serial in [false, true] {
            let err = if serial {
                scan_serial(&resolved, None, &[]).unwrap_err()
            } else {
                match scan(&resolved, None, &[]) {
                    Ok(_) => panic!("valid UTF-8 came out of an invalid dictionary entry"),
                    Err(err) => err,
                }
            };
            // The serial reader wraps a column's error with the file path;
            // the parallel reader reports the column alone — today's texts.
            let want = if serial {
                format!(
                    "column \"s\" of \"{}\": column \"s\": the bytes are not valid UTF-8",
                    path.display()
                )
            } else {
                "column \"s\": the bytes are not valid UTF-8".to_string()
            };
            assert_eq!(err.to_string(), want, "serial: {serial}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Gap merged across page spans: a page is tens of kilobytes, so a
/// row-group-sized gap would fetch the bytes of pruned pages between
/// survivors.
const PAGE_COALESCE_GAP: u64 = 256 * 1024;

/// One data file's row groups as a morsel plan (wave 133b): the lake claims a
/// row group and decodes it, so a scan holds one row group per worker and
/// reads only the surviving pages' bytes.
///
/// The plan is metadata only — the footer, the page index and the projection —
/// so it outlives the scan that built it, and several workers can decode
/// different row groups of the same file at once. A decode fetches its own row
/// group's spans into the plan's read cache, so the fetched bytes are shared
/// but no decode waits on another's work.
pub struct RowGroupMorsels {
    path: String,
    resolved: Resolved,
    filters: Vec<PrunePredicate>,
    needed: Vec<bool>,
    placeholders: Vec<Option<Arc<Vector>>>,
    /// The file's positioned reader, when it has one.
    remote: Option<crate::ranged::RemoteSpec>,
    reader: FileSource,
    metadata: Arc<ParquetMetaData>,
    props: ReaderPropertiesPtr,
    transport: Arc<crate::ranged::TransportCounters>,
    groups: Vec<GroupPlan>,
    /// The rows each morsel's pushed predicates rejected, per morsel.
    dropped: Vec<AtomicU64>,
    counters: MorselCounters,
}

/// One surviving row group: what its decode reads and how many rows it yields.
struct GroupPlan {
    /// The row group's index in the file's footer.
    index: usize,
    /// The rows the decode produces (fewer than the row group's when the page
    /// index proved pages cannot match).
    rows: usize,
    /// How many kept rows the row groups before this one decode to: chunk
    /// lengths stay on the file's `CHUNK_SIZE` grid across row groups
    /// ([`chunk_lens`]), whichever worker decodes this morsel.
    offset: usize,
    /// The row group's byte ranges, coalesced and ready to fetch.
    spans: Vec<crate::ranged::ByteSpan>,
    /// The page selection that keeps the decode to the surviving pages.
    selection: Option<PageSelection>,
}

/// The file's reader: positioned reads over object storage, or a local file.
enum FileSource {
    Remote(Arc<crate::ranged::RemoteFile>),
    Local(Box<SerializedFileReader<std::fs::File>>),
}

/// What a row-group plan settled: the row groups and pages it will read, and
/// the ones its statistics proved no row can match.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MorselCounters {
    pub row_groups: u64,
    pub row_groups_pruned: u64,
    pub pages: u64,
    pub pages_pruned: u64,
}

impl RowGroupMorsels {
    /// How many morsels the plan hands out.
    pub fn count(&self) -> usize {
        self.groups.len()
    }

    /// The rows morsel `index` decodes to.
    pub fn rows(&self, index: usize) -> u64 {
        self.groups.get(index).map_or(0, |group| group.rows as u64)
    }

    /// The rows morsel `index` dropped because a pushed predicate rejected
    /// them (they are not in [`rows`](Self::rows)' output).
    pub fn dropped(&self, index: usize) -> u64 {
        self.dropped
            .get(index)
            .map_or(0, |dropped| dropped.load(AtomicOrdering::Relaxed))
    }

    /// What the plan pruned, and what it will read.
    pub fn counters(&self) -> MorselCounters {
        self.counters
    }

    /// What the decodes issued so far: transport requests, bytes and wait.
    pub fn stats(&self) -> crate::RemoteReadStats {
        self.transport.snapshot()
    }

    /// Decode morsel `index` (one row group), handing `sink` its chunks.
    pub fn decode(
        &self,
        index: usize,
        sink: &mut dyn FnMut(DataChunk) -> Result<()>,
    ) -> Result<()> {
        let group = self.groups.get(index).ok_or_else(|| {
            PinkDbError::Execution(format!(
                "parquet morsel {index} is out of range for \"{}\" ({} morsels)",
                self.path,
                self.groups.len()
            ))
        })?;
        let file = &self.resolved.files[0];
        let plan = self.decode_plan();
        let decoded = decoded_columns(file, &plan);
        let filtering = drops_rows(&self.resolved, &plan);
        let filter_only = filter_only_columns(file, &plan, filtering);
        let filters: &[PrunePredicate] = match filtering {
            true => &self.filters,
            false => &[],
        };
        let lens = chunk_lens(group.rows, group.offset);
        if decoded.is_empty() {
            return assemble_row_group(file, &plan, lens, &decoded, Vec::new(), sink);
        }
        let (pieces, kept) = match (&self.reader, &self.remote) {
            (FileSource::Remote(chunk), Some(spec)) => {
                self.fetch(chunk, spec, group)?;
                let rg_meta = self.metadata.row_group(group.index);
                let page_index = self.metadata.page_index_for_row_group(group.index);
                let row_group = SerializedRowGroupReader::new(
                    Arc::clone(chunk),
                    rg_meta,
                    page_index,
                    Arc::clone(&self.props),
                )
                .map_err(|e| {
                    PinkDbError::Execution(format!(
                        "cannot read row group {} of \"{}\": {e}",
                        group.index, self.path
                    ))
                })?;
                match &group.selection {
                    Some(selection) => {
                        let filtered = PageRowGroup {
                            inner: row_group,
                            chunk: Arc::clone(chunk),
                            meta: rg_meta,
                            props: Arc::clone(&self.props),
                            locations: selection.locations.clone(),
                        };
                        decode_row_group_reader(
                            &filtered,
                            &decoded,
                            &filter_only,
                            filters,
                            lens,
                            &self.path,
                        )?
                    }
                    None => decode_row_group_reader(
                        &row_group,
                        &decoded,
                        &filter_only,
                        filters,
                        lens,
                        &self.path,
                    )?,
                }
            }
            (FileSource::Local(reader), _) => decode_row_group(
                reader,
                group.index,
                &decoded,
                &filter_only,
                filters,
                lens,
                &self.path,
            )?,
            (FileSource::Remote(_), None) => {
                return Err(PinkDbError::Execution(format!(
                    "the parquet morsel plan for \"{}\" lost its range source",
                    self.path
                )));
            }
        };
        let dropped = match &kept {
            Some(kept) => group.rows - kept.iter().sum::<usize>(),
            None => 0,
        };
        self.dropped[index].fetch_add(dropped as u64, AtomicOrdering::Relaxed);
        match kept {
            Some(kept) => assemble_row_group(file, &plan, kept, &decoded, pieces, sink),
            None => assemble_row_group(file, &plan, lens, &decoded, pieces, sink),
        }
    }

    fn decode_plan(&self) -> DecodePlan<'_> {
        DecodePlan {
            resolved: &self.resolved,
            filters: &self.filters,
            needed: self.needed.clone(),
            placeholders: self.placeholders.clone(),
        }
    }

    /// Fetch this row group's byte ranges into the read cache.
    fn fetch(
        &self,
        chunk: &Arc<crate::ranged::RemoteFile>,
        spec: &crate::ranged::RemoteSpec,
        group: &GroupPlan,
    ) -> Result<()> {
        if group.spans.is_empty() {
            return Ok(());
        }
        let cache = chunk.cache();
        let fetched = crate::ranged::fetch_spans(
            spec.source.as_ref(),
            &group.spans,
            spec.parallelism.max(1),
            &self.transport,
        );
        for (span, result) in group.spans.iter().zip(fetched) {
            let bytes = result
                .map_err(|e| PinkDbError::Execution(format!("remote parquet read failed: {e}")))?;
            if bytes.len() as u64 != span.len {
                return Err(PinkDbError::Execution(format!(
                    "remote parquet read failed: GET {} (range {}..{}) returned {} bytes, \
                     expected {}",
                    self.path,
                    span.start,
                    span.end(),
                    bytes.len(),
                    span.len
                )));
            }
            cache.insert(span.start, bytes);
        }
        Ok(())
    }
}

/// Plan a resolved file's row groups for morsel-by-morsel decoding.
///
/// Pruning happens here, once: the surviving row groups, and the pages their
/// statistics leave alive, are settled before any data page is read, so a scan
/// can report what it pruned before it reads anything.
pub fn plan_row_groups(
    resolved: Resolved,
    projection: Option<&[usize]>,
    filters: &[PrunePredicate],
) -> Result<RowGroupMorsels> {
    let width = resolved.schema.len();
    let mut needed = vec![false; width];
    match projection {
        Some(cols) => {
            for &c in cols {
                if c < width {
                    needed[c] = true;
                }
            }
        }
        None => needed.iter_mut().for_each(|n| *n = true),
    }
    let placeholders: Vec<Option<Arc<Vector>>> = (0..width)
        .map(|i| (!needed[i]).then(|| Arc::new(Vector::new_empty(resolved.schema[i].1))))
        .collect();
    let mut resolved = resolved;
    // The morsel plan sizes buffers from the footer's counts, and a lake data
    // file's footer is as untrusted as any other: check it once here, before
    // any decode (the scan entry points do the same, see `scan_into`).
    for file in &resolved.files {
        file.meta.validate()?;
    }
    let path = resolved
        .files
        .first()
        .map_or_else(String::new, |file| file.meta.path.clone());
    let remote = resolved
        .files
        .get_mut(0)
        .and_then(|file| file.remote.take());
    let transport = Arc::new(crate::ranged::TransportCounters::default());
    let (reader, metadata) = match &remote {
        Some(spec) => {
            let chunk_reader = Arc::new(crate::ranged::RemoteFile::with_counters(
                spec,
                Arc::clone(&transport),
            ));
            let metadata = ParquetMetaDataReader::new()
                .with_page_index_policy(PageIndexPolicy::Optional)
                .parse_and_finish(chunk_reader.as_ref())
                .map_err(|e| {
                    PinkDbError::Execution(format!("cannot read parquet file \"{path}\": {e}"))
                })?;
            (FileSource::Remote(chunk_reader), Arc::new(metadata))
        }
        None => {
            let reader = SerializedFileReader::new(std::fs::File::open(&path).map_err(|e| {
                PinkDbError::Io(std::io::Error::new(
                    e.kind(),
                    format!("cannot open \"{path}\": {e}"),
                ))
            })?)
            .map_err(|e| {
                PinkDbError::Execution(format!("cannot read parquet file \"{path}\": {e}"))
            })?;
            let metadata = Arc::new(reader.metadata().clone());
            (FileSource::Local(Box::new(reader)), metadata)
        }
    };
    let props: ReaderPropertiesPtr = Arc::new(ReaderProperties::builder().build());
    let mut counters = MorselCounters::default();
    let mut groups = Vec::new();
    if let Some(file) = resolved.files.first() {
        let plan = DecodePlan {
            resolved: &resolved,
            filters,
            needed: needed.clone(),
            placeholders: placeholders.clone(),
        };
        let decoded = decoded_columns(file, &plan);
        let mut kept_offset = 0usize;
        for (rg_index, rg) in file.meta.row_groups.iter().enumerate() {
            if !row_group_survives(&resolved, file, rg_index, filters) {
                counters.row_groups_pruned += 1;
                continue;
            }
            counters.row_groups += 1;
            let rows = rg.rows as usize;
            let index = metadata.page_index_for_row_group(rg_index);
            let selection = if decoded.is_empty() || remote.is_none() {
                None
            } else {
                page_selection(&resolved, file, rg_index, rows, &index, &decoded, filters)
            };
            let kept_rows = selection.as_ref().map_or(rows, |selection| selection.rows);
            if let Some(selection) = &selection {
                counters.pages += selection.pages_read;
                counters.pages_pruned += selection.pages_pruned;
            }
            let spans = match &selection {
                Some(selection) => selection.spans.clone(),
                None => {
                    // No page plan: the whole chunk is read, so every indexed
                    // page is read too.
                    counters.pages += decoded
                        .iter()
                        .filter_map(|(_, at, _)| index.offset_index(*at))
                        .map(|offset| offset.page_locations().len() as u64)
                        .sum::<u64>();
                    chunk_spans(rg, &decoded)
                }
            };
            // Page spans are one page each: coalesce with a page-sized gap so
            // surviving pages far apart do not pull the file between them.
            let spans = if selection.is_some() {
                crate::ranged::coalesce_spans_with_gap(spans, PAGE_COALESCE_GAP)
            } else {
                crate::ranged::coalesce_spans(spans)
            };
            groups.push(GroupPlan {
                index: rg_index,
                rows: kept_rows,
                offset: kept_offset,
                spans,
                selection,
            });
            kept_offset = kept_offset.saturating_add(kept_rows);
        }
    }
    let dropped = groups.iter().map(|_| AtomicU64::new(0)).collect();
    Ok(RowGroupMorsels {
        path,
        resolved,
        filters: filters.to_vec(),
        needed,
        placeholders,
        remote,
        reader,
        metadata,
        props,
        transport,
        groups,
        dropped,
        counters,
    })
}

/// The coalesced whole-column-chunk ranges a row group decodes without a page
/// plan.
fn chunk_spans(
    rg: &crate::file_meta::RowGroupMeta,
    decoded: &[(usize, usize, &FileColumn)],
) -> Vec<crate::ranged::ByteSpan> {
    decoded
        .iter()
        .flat_map(|(_, _, column)| {
            (0..column.leaves).filter_map(move |offset| {
                rg.chunks
                    .get(column.leaf + offset)
                    .and_then(crate::ranged::chunk_span)
            })
        })
        .filter(|span| span.len <= crate::ranged::MAX_REQUEST_BYTES)
        .collect()
}

/// One row group's page selection: which decoded pages survive, the byte
/// ranges to prefetch, and the row count they cover.
struct PageSelection {
    rows: usize,
    /// Data pages read and pruned, counted once per decoded column.
    pages_read: u64,
    pages_pruned: u64,
    /// Per file column: the page locations to read, with contiguous
    /// `first_row_index` values so the page reader's row counts stay right.
    locations: Vec<Option<Vec<PageLocation>>>,
    spans: Vec<crate::ranged::ByteSpan>,
}

/// Prune pages of one row group through the column index.
///
/// The rules are DuckDB's (`parquet_reader.cpp`/`column_reader.cpp`): a page is
/// skipped only when its own statistics prove no row can satisfy a predicate,
/// with the same NULL and float rules as row-group pruning; page 0 always
/// stays (it anchors the dictionary page and keeps every decoded column's row
/// ranges identical). Decoded columns must share their page boundaries —
/// REQUIRED for dropping rows — while a filter column may have finer or
/// coarser pages, mapped onto the decoded pages by row range.
fn page_selection(
    resolved: &Resolved,
    file: &FileResolved,
    rg_index: usize,
    rows: usize,
    index: &RowGroupPageIndex,
    decoded: &[(usize, usize, &FileColumn)],
    filters: &[PrunePredicate],
) -> Option<PageSelection> {
    // Repeated/shredded columns encode several leaves per row, so a page's
    // row count is not its value count; leave them at row-group granularity.
    if decoded.iter().any(|(_, _, column)| {
        column.leaves != 1 || matches!(column.ty, DataType::Array { .. } | DataType::Variant)
    }) {
        return None;
    }
    let mut decoded_pages: Vec<&[PageLocation]> = Vec::with_capacity(decoded.len());
    for (_, at, _) in decoded {
        decoded_pages.push(index.offset_index(*at)?.page_locations().as_slice());
    }
    let first = decoded_pages.first()?;
    if first.is_empty() {
        return None;
    }
    // Dropping rows needs the same page boundaries in every decoded column.
    if decoded_pages.iter().any(|locations| {
        locations.len() != first.len()
            || locations
                .iter()
                .zip(first.iter())
                .any(|(a, b)| a.first_row_index != b.first_row_index)
    }) {
        return None;
    }
    let page_count = first.len();
    // A page index is untrusted footer bytes too: an offset or size outside
    // the file would size a read, so an implausible index is ignored and the
    // row group is read whole (parquet then checks each page against the
    // chunk's remaining bytes).
    if decoded_pages
        .iter()
        .any(|locations| !page_locations_fit(locations, rows, file.meta.file_size))
    {
        return None;
    }
    let mut bounds: Vec<usize> = first
        .iter()
        .map(|page| page.first_row_index as usize)
        .collect();
    bounds.push(rows);
    let mut kept = vec![true; page_count];
    let mut pruned_any = false;
    for filter in filters {
        if filter.path.is_some() {
            continue;
        }
        let Some(at) = file.out_to_file.get(filter.column).copied().flatten() else {
            continue;
        };
        let (Some(column_index), Some(offset_index)) =
            (index.column_index(at), index.offset_index(at))
        else {
            continue;
        };
        let locations = offset_index.page_locations();
        if column_index.num_pages() as usize != locations.len() {
            continue;
        }
        let column = &file.meta.columns[at];
        let out_type = resolved.schema[filter.column].1;
        for (page, location) in locations.iter().enumerate() {
            let start = location.first_row_index as usize;
            let end = locations
                .get(page + 1)
                .map(|next| next.first_row_index as usize)
                .unwrap_or(rows);
            if end <= start {
                continue;
            }
            let stats = crate::file_meta::page_stats(column_index, page, column);
            if stats_keeps(&stats, column, filter, out_type, (end - start) as u64) {
                continue;
            }
            for page in 1..page_count {
                if kept[page] && bounds[page] < end && bounds[page + 1] > start {
                    kept[page] = false;
                    pruned_any = true;
                }
            }
        }
    }
    if !pruned_any {
        return None;
    }

    // Contiguous synthetic row indexes: the page reader derives each page's
    // row count from the next location, and skipped pages must not widen it.
    let mut starts = vec![0i64; page_count];
    let mut kept_rows = 0i64;
    for page in 0..page_count {
        starts[page] = kept_rows;
        if kept[page] {
            kept_rows += (bounds[page + 1] - bounds[page]) as i64;
        }
    }
    let mut locations: Vec<Option<Vec<PageLocation>>> = vec![None; file.meta.columns.len()];
    let mut spans = Vec::new();
    for (slot, (_, at, _)) in decoded.iter().enumerate() {
        let pages = decoded_pages[slot];
        let mut kept_locations = Vec::with_capacity(page_count);
        for (page, location) in pages.iter().enumerate() {
            if !kept[page] {
                continue;
            }
            kept_locations.push(PageLocation {
                offset: location.offset,
                compressed_page_size: location.compressed_page_size,
                first_row_index: starts[page],
            });
            spans.push(crate::ranged::ByteSpan {
                start: location.offset as u64,
                len: location.compressed_page_size as u64,
            });
        }
        // The dictionary page sits before page 0; the page reader fetches it
        // from the gap, so prefetch it too.
        let chunk = file.meta.row_groups[rg_index]
            .chunks
            .get(decoded[slot].2.leaf);
        if let Some(chunk) = chunk.filter(|chunk| chunk_starts_before(chunk, pages[0].offset)) {
            let start = chunk_start(chunk);
            spans.push(crate::ranged::ByteSpan {
                start,
                len: pages[0].offset as u64 - start,
            });
        }
        locations[*at] = Some(kept_locations);
    }
    let kept_pages = kept.iter().filter(|kept| **kept).count() as u64;
    let columns = decoded.len() as u64;
    Some(PageSelection {
        rows: kept_rows as usize,
        pages_read: kept_pages * columns,
        pages_pruned: (page_count as u64 - kept_pages) * columns,
        locations,
        spans,
    })
}

/// Whether a column's page index describes pages of a row group of `rows`
/// rows that all lie inside a `file_size`-byte file: page 0 starts the row
/// group, row indexes strictly increase, and every page's byte range is
/// inside the file. Anything else is corrupt metadata and must not size a
/// read.
fn page_locations_fit(locations: &[PageLocation], rows: usize, file_size: u64) -> bool {
    let inside = |location: &PageLocation| {
        location.compressed_page_size > 0
            && location.offset >= 0
            && (location.offset as u64)
                .checked_add(location.compressed_page_size as u64)
                .is_some_and(|end| end <= file_size)
    };
    let rows = rows as u64;
    locations
        .first()
        .is_some_and(|first| first.first_row_index == 0)
        && locations.iter().all(inside)
        && locations.iter().all(|location| {
            location.first_row_index >= 0 && (location.first_row_index as u64) < rows
        })
        && locations
            .windows(2)
            .all(|pair| pair[0].first_row_index < pair[1].first_row_index)
}

fn chunk_start(chunk: &crate::file_meta::ChunkInfo) -> u64 {
    match chunk.dictionary_page_offset {
        Some(offset) if offset >= 0 => offset as u64,
        _ => chunk.data_page_offset as u64,
    }
}

fn chunk_starts_before(chunk: &crate::file_meta::ChunkInfo, offset: i64) -> bool {
    chunk_start(chunk) < offset as u64
}

/// A row group whose decoded columns read only the surviving page locations.
struct PageRowGroup<'a, R: ChunkReader + 'static> {
    inner: SerializedRowGroupReader<'a, R>,
    chunk: Arc<R>,
    meta: &'a RowGroupMetaData,
    props: ReaderPropertiesPtr,
    locations: Vec<Option<Vec<PageLocation>>>,
}

impl<R: ChunkReader + 'static> RowGroupReader for PageRowGroup<'_, R> {
    fn metadata(&self) -> &RowGroupMetaData {
        self.inner.metadata()
    }

    fn num_columns(&self) -> usize {
        self.inner.num_columns()
    }

    fn get_column_page_reader(
        &self,
        column: usize,
    ) -> parquet::errors::Result<Box<dyn PageReader>> {
        match self.locations.get(column).and_then(|pages| pages.clone()) {
            Some(pages) => {
                let reader = SerializedPageReader::new_with_properties(
                    Arc::clone(&self.chunk),
                    self.meta.column(column),
                    self.meta.num_rows() as usize,
                    Some(pages),
                    Arc::clone(&self.props),
                )?;
                Ok(Box::new(reader))
            }
            None => self.inner.get_column_page_reader(column),
        }
    }

    fn get_column_bloom_filter(&self, column: usize) -> Option<&Sbbf> {
        self.inner.get_column_bloom_filter(column)
    }

    fn get_row_iter(&self, projection: Option<SchemaType>) -> parquet::errors::Result<RowIter<'_>> {
        self.inner.get_row_iter(projection)
    }
}
