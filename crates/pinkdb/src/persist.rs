//! Persistence: a single-file on-disk columnar format (`.pink`).
//!
//! Layout v3 (all integers little-endian):
//! ```text
//! magic "PINKDB03" (8 bytes)
//! db_count u32
//! per database:
//!   name_len u32 + name bytes
//!   table_count u32
//!   per table:
//!     name_len u32 + name bytes
//!     col_count u32
//!     per column: name_len u32 + name bytes, dtype u8
//!     num_rows u64
//!     per column: chunk_count u32
//!       per chunk:
//!         encoding u8 (0=Plain, 1=RLE, 2=Dict, 3=Bitpack)
//!         validity_len u32 + validity bytes (bit set = valid)
//!         payload (encoding-specific)
//! crc32 u32 (IEEE, over every preceding byte including the magic)
//! ```
//!
//! v2 files (`PINKDB02`) are the same layout without the CRC trailer; v1
//! files (`PINKDB01`) hold a flat table list and load as a single `main`
//! database. `open` accepts all three; writes are always v3. Views are
//! session-local and never written.
//!
//! Payloads store every row including NULL placeholders; validity is applied
//! afterwards. Per-chunk encoding is picked by size (smallest wins):
//! - Integers: Plain (raw LE) vs RLE (low run count) vs Bitpack (small range,
//!   stored as min + width + packed `(value - min)` bits).
//! - Doubles: Plain (raw bits) vs RLE.
//! - Booleans: Plain (bitpacked) vs RLE.
//! - Varchars: Plain (offsets + data) vs Dictionary (low cardinality, u32
//!   codes) vs RLE.
//!
//! The reader never panics on corrupt input: every read is bounds-checked and
//! surfaces `PinkDbError::Execution("...")`, and no allocation is sized from
//! an unvalidated length (chunk row counts are capped at [`CHUNK_SIZE`], the
//! writer's maximum; per-table column counts by the bytes left). A v3 CRC
//! mismatch is reported before any decoding.

use std::collections::HashMap;

use crate::crypto::EncryptionKey;
use crate::storage::PinkDb;
use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{Vector, CHUNK_SIZE};

const MAGIC: &[u8; 8] = b"PINKDB01";
/// Version 2 adds the database dimension: one file holds every database.
const MAGIC_V2: &[u8; 8] = b"PINKDB02";
/// Version 3 = v2 + a trailing CRC-32 over the whole image (bit-rot check).
const MAGIC_V3: &[u8; 8] = b"PINKDB03";

const ENC_PLAIN: u8 = 0;
const ENC_RLE: u8 = 1;
const ENC_DICT: u8 = 2;
const ENC_BITPACK: u8 = 3;

fn dtype_tag(t: DataType) -> u8 {
    match t {
        DataType::Boolean => 0,
        DataType::Integer => 1,
        DataType::Bigint => 2,
        DataType::Double => 3,
        DataType::Varchar => 4,
        DataType::Date => 5,
        DataType::Timestamp => 6,
    }
}

fn dtype_from_tag(b: u8) -> Result<DataType> {
    match b {
        0 => Ok(DataType::Boolean),
        1 => Ok(DataType::Integer),
        2 => Ok(DataType::Bigint),
        3 => Ok(DataType::Double),
        4 => Ok(DataType::Varchar),
        5 => Ok(DataType::Date),
        6 => Ok(DataType::Timestamp),
        _ => Err(PinkDbError::Execution(format!(
            "corrupt file: bad dtype tag {b}"
        ))),
    }
}

// ---------- writer ----------

struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn bytes(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }

    fn str(&mut self, s: &str) {
        self.u32(s.len() as u32);
        self.bytes(s.as_bytes());
    }
}

// ---------- reader (bounds-checked, never panics) ----------

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn err<T>(&self, what: &str) -> Result<T> {
        Err(PinkDbError::Execution(format!(
            "corrupt file: {what} at offset {}",
            self.pos
        )))
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| PinkDbError::Execution("corrupt file: offset overflow".to_string()))?;
        if end > self.data.len() {
            return self.err("unexpected end of file");
        }
        let out = &self.data[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(self.u32()? as i32)
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(self.u64()? as i64)
    }

    fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.u64()?))
    }

    fn str(&mut self) -> Result<String> {
        let n = self.u32()? as usize;
        let b = self.take(n)?;
        std::str::from_utf8(b)
            .map(|s| s.to_string())
            .map_err(|_| PinkDbError::Execution("corrupt file: bad UTF-8".to_string()))
    }

    fn end(&self) -> bool {
        self.pos == self.data.len()
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    /// A per-chunk element count (rows, runs, dictionary entries): the writer
    /// never emits more than [`CHUNK_SIZE`], so anything larger is corrupt
    /// and is refused before it can size an allocation.
    fn chunk_count(&mut self, what: &str) -> Result<usize> {
        let n = self.u32()? as usize;
        if n > CHUNK_SIZE {
            return self.err(&format!("{what} {n} exceeds the chunk size {CHUNK_SIZE}"));
        }
        Ok(n)
    }
}

// ---------- value helpers ----------

fn as_i64(v: &Value) -> i64 {
    match v {
        Value::Integer(i) => *i as i64,
        Value::Bigint(i) => *i,
        Value::Date(d) => *d as i64,
        Value::Timestamp(us) => *us,
        _ => 0, // only called on int/date-typed vectors
    }
}

/// Bits needed to store `0..=range`.
fn bits_needed(range: u64) -> u32 {
    (64 - range.leading_zeros()).max(1)
}

fn pack_bits(writer: &mut Writer, values: &[u64], width: u32) {
    let mut acc: u64 = 0;
    let mut acc_bits: u32 = 0;
    for &v in values {
        acc |= v << acc_bits;
        acc_bits += width;
        while acc_bits >= 8 {
            writer.u8((acc & 0xFF) as u8);
            acc >>= 8;
            acc_bits -= 8;
        }
    }
    if acc_bits > 0 {
        writer.u8((acc & 0xFF) as u8);
    }
}

fn unpack_bits(reader: &mut Reader, count: usize, width: u32) -> Result<Vec<u64>> {
    let mut out = Vec::with_capacity(count);
    let mut acc: u64 = 0;
    let mut acc_bits: u32 = 0;
    let mask = if width == 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    };
    while out.len() < count {
        while acc_bits < width {
            acc |= (reader.u8()? as u64) << acc_bits;
            acc_bits += 8;
        }
        out.push(acc & mask);
        acc >>= width;
        acc_bits -= width;
    }
    Ok(out)
}

/// Encode a validity mask the way [`Vector`] exposes it: ceil(len/64) u64 LE.
fn encode_validity(writer: &mut Writer, v: &Vector) {
    let words = v.len().div_ceil(64);
    for w in 0..words {
        let mut word: u64 = 0;
        for b in 0..64 {
            let i = w * 64 + b;
            if i < v.len() && v.is_valid(i) {
                word |= 1u64 << b;
            }
        }
        writer.u64(word);
    }
    let _ = writer;
}

fn decode_validity(reader: &mut Reader, len: usize) -> Result<Vec<bool>> {
    let words = len.div_ceil(64);
    let mut out = Vec::with_capacity(len);
    for w in 0..words {
        let word = reader.u64()?;
        for b in 0..64 {
            if w * 64 + b < len {
                out.push((word >> b) & 1 == 1);
            }
        }
    }
    Ok(out)
}

// ---------- chunk encode ----------

/// Encode one column chunk: returns (encoding tag, payload bytes).
fn encode_chunk(dtype: DataType, values: &[Value]) -> (u8, Vec<u8>) {
    let mut best: Option<(u8, Vec<u8>)> = None;
    let mut consider = |tag: u8, payload: Vec<u8>| match &best {
        Some((_, b)) if b.len() <= payload.len() => {}
        _ => best = Some((tag, payload)),
    };
    match dtype {
        DataType::Integer | DataType::Bigint => {
            let nums: Vec<i64> = values.iter().map(as_i64).collect();
            consider(ENC_PLAIN, encode_ints_plain(dtype, &nums));
            consider(ENC_RLE, encode_ints_rle(dtype, &nums));
            consider(ENC_BITPACK, encode_ints_bitpack(&nums));
        }
        // Dates reuse the integer codecs bit-for-bit; the dtype tag on the
        // chunk keeps them distinct from INTEGER/BIGINT on decode.
        DataType::Date => {
            let nums: Vec<i64> = values.iter().map(as_i64).collect();
            consider(ENC_PLAIN, encode_ints_plain(DataType::Integer, &nums));
            consider(ENC_RLE, encode_ints_rle(DataType::Integer, &nums));
            consider(ENC_BITPACK, encode_ints_bitpack(&nums));
        }
        DataType::Timestamp => {
            let nums: Vec<i64> = values.iter().map(as_i64).collect();
            consider(ENC_PLAIN, encode_ints_plain(DataType::Bigint, &nums));
            consider(ENC_RLE, encode_ints_rle(DataType::Bigint, &nums));
            consider(ENC_BITPACK, encode_ints_bitpack(&nums));
        }
        DataType::Double => {
            let bits: Vec<u64> = values
                .iter()
                .map(|v| match v {
                    Value::Double(x) => x.to_bits(),
                    _ => 0.0f64.to_bits(),
                })
                .collect();
            consider(ENC_PLAIN, encode_u64_plain(&bits));
            consider(ENC_RLE, encode_u64_rle(&bits));
        }
        DataType::Boolean => {
            let bits: Vec<bool> = values
                .iter()
                .map(|v| matches!(v, Value::Boolean(true)))
                .collect();
            consider(ENC_PLAIN, encode_bool_plain(&bits));
            consider(ENC_RLE, encode_bool_rle(&bits));
        }
        DataType::Varchar => {
            let strs: Vec<&str> = values
                .iter()
                .map(|v| match v {
                    Value::Varchar(s) => s.as_str(),
                    _ => "",
                })
                .collect();
            consider(ENC_PLAIN, encode_strs_plain(&strs));
            consider(ENC_DICT, encode_strs_dict(&strs));
            consider(ENC_RLE, encode_strs_rle(&strs));
        }
    }
    best.expect("at least plain encoding")
}

fn encode_ints_plain(dtype: DataType, nums: &[i64]) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(nums.len() as u32);
    match dtype {
        DataType::Integer => {
            for &n in nums {
                w.u32(n as i32 as u32);
            }
        }
        _ => {
            for &n in nums {
                w.u64(n as u64);
            }
        }
    }
    w.buf
}

fn runs_of<T: PartialEq>(items: &[T]) -> Vec<(usize, usize)> {
    // (start, len) runs.
    let mut runs = Vec::new();
    let mut i = 0;
    while i < items.len() {
        let mut j = i + 1;
        while j < items.len() && items[j] == items[i] {
            j += 1;
        }
        runs.push((i, j - i));
        i = j;
    }
    runs
}

fn encode_ints_rle(dtype: DataType, nums: &[i64]) -> Vec<u8> {
    let mut w = Writer::new();
    let runs = runs_of(nums);
    w.u32(nums.len() as u32);
    w.u32(runs.len() as u32);
    for (start, len) in runs {
        match dtype {
            DataType::Integer => w.u32(nums[start] as i32 as u32),
            _ => w.u64(nums[start] as u64),
        }
        w.u32(len as u32);
    }
    w.buf
}

fn encode_ints_bitpack(nums: &[i64]) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(nums.len() as u32);
    if nums.is_empty() {
        w.u64(0);
        w.u8(0);
        return w.buf;
    }
    let min = *nums.iter().min().unwrap();
    let max = *nums.iter().max().unwrap();
    // Wrapping: max >= min, so this is the exact range mod 2^64 (full i64
    // span, e.g. MIN..MAX, needs all 64 bits and must not panic in debug).
    let width = bits_needed(max.wrapping_sub(min) as u64);
    w.u64(min as u64);
    w.u8(width.min(64) as u8);
    let shifted: Vec<u64> = nums.iter().map(|&n| n.wrapping_sub(min) as u64).collect();
    pack_bits(&mut w, &shifted, width.min(64));
    w.buf
}

fn encode_u64_plain(bits: &[u64]) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(bits.len() as u32);
    for &b in bits {
        w.u64(b);
    }
    w.buf
}

fn encode_u64_rle(bits: &[u64]) -> Vec<u8> {
    let mut w = Writer::new();
    let runs = runs_of(bits);
    w.u32(bits.len() as u32);
    w.u32(runs.len() as u32);
    for (start, len) in runs {
        w.u64(bits[start]);
        w.u32(len as u32);
    }
    w.buf
}

fn encode_bool_plain(bits: &[bool]) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(bits.len() as u32);
    let mut acc: u8 = 0;
    let mut n: u32 = 0;
    for &b in bits {
        if b {
            acc |= 1 << n;
        }
        n += 1;
        if n == 8 {
            w.u8(acc);
            acc = 0;
            n = 0;
        }
    }
    if n > 0 {
        w.u8(acc);
    }
    w.buf
}

fn encode_bool_rle(bits: &[bool]) -> Vec<u8> {
    let mut w = Writer::new();
    let runs = runs_of(bits);
    w.u32(bits.len() as u32);
    w.u32(runs.len() as u32);
    for (start, len) in runs {
        w.u8(bits[start] as u8);
        w.u32(len as u32);
    }
    w.buf
}

/// Plain strings: count + (count+1) offsets + data bytes.
fn encode_strs_offsets(w: &mut Writer, strs: &[&str]) {
    let mut off: u32 = 0;
    w.u32(off);
    for s in strs {
        off += s.len() as u32;
        w.u32(off);
    }
    for s in strs {
        w.bytes(s.as_bytes());
    }
}

fn encode_strs_plain(strs: &[&str]) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(strs.len() as u32);
    encode_strs_offsets(&mut w, strs);
    w.buf
}

fn encode_strs_dict(strs: &[&str]) -> Vec<u8> {
    let mut w = Writer::new();
    let mut dict: Vec<&str> = Vec::new();
    let mut index: HashMap<&str, u32> = HashMap::new();
    let mut codes = Vec::with_capacity(strs.len());
    for s in strs {
        let code = match index.get(s) {
            Some(&c) => c,
            None => {
                let c = dict.len() as u32;
                dict.push(s);
                index.insert(s, c);
                c
            }
        };
        codes.push(code);
    }
    w.u32(strs.len() as u32);
    w.u32(dict.len() as u32);
    encode_strs_offsets(&mut w, &dict);
    for c in codes {
        w.u32(c);
    }
    w.buf
}

fn encode_strs_rle(strs: &[&str]) -> Vec<u8> {
    let mut w = Writer::new();
    let runs = runs_of(strs);
    w.u32(strs.len() as u32);
    w.u32(runs.len() as u32);
    let vals: Vec<&str> = runs.iter().map(|&(s, _)| strs[s]).collect();
    encode_strs_offsets(&mut w, &vals);
    for &(_, len) in &runs {
        w.u32(len as u32);
    }
    w.buf
}

// ---------- chunk decode ----------

fn decode_strs(reader: &mut Reader, count: usize) -> Result<Vec<String>> {
    // Reads count+1 offsets then the data block; returns owned strings.
    let mut offsets = Vec::with_capacity(count + 1);
    for _ in 0..count + 1 {
        offsets.push(reader.u32()? as usize);
    }
    let total = *offsets.last().unwrap_or(&0);
    let data = reader.take(total)?;
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let (a, b) = (offsets[i], offsets[i + 1]);
        if b < a || b > data.len() {
            return reader.err("bad string offsets");
        }
        out.push(
            std::str::from_utf8(&data[a..b])
                .map_err(|_| PinkDbError::Execution("corrupt file: bad UTF-8".to_string()))?
                .to_string(),
        );
    }
    Ok(out)
}

fn apply_validity(values: Vec<Value>, valid: &[bool]) -> Result<Vec<Value>> {
    if values.len() != valid.len() {
        return Err(PinkDbError::Execution(
            "corrupt file: validity/values length mismatch".to_string(),
        ));
    }
    Ok(values
        .into_iter()
        .zip(valid.iter())
        .map(|(v, &ok)| if ok { v } else { Value::Null })
        .collect())
}

fn decode_chunk(dtype: DataType, tag: u8, reader: &mut Reader) -> Result<Vec<Value>> {
    // Validity first (encoder writes it before the payload).
    let vlen = reader.u32()? as usize;
    let vbytes = reader.take(vlen)?;
    let mut vr = Reader::new(vbytes);
    // Decode payload into placeholder values + separate validity.
    let (values, valid) = match (dtype, tag) {
        (DataType::Integer, ENC_PLAIN) => {
            let n = reader.chunk_count("chunk length")?;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                out.push(Value::Integer(reader.i32()?));
            }
            (out, decode_validity(&mut vr, n)?)
        }
        (DataType::Date, ENC_PLAIN) => {
            let n = reader.chunk_count("chunk length")?;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                out.push(Value::Date(reader.i32()?));
            }
            (out, decode_validity(&mut vr, n)?)
        }
        (DataType::Bigint, ENC_PLAIN) => {
            let n = reader.chunk_count("chunk length")?;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                out.push(Value::Bigint(reader.i64()?));
            }
            (out, decode_validity(&mut vr, n)?)
        }
        (DataType::Timestamp, ENC_PLAIN) => {
            let n = reader.chunk_count("chunk length")?;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                out.push(Value::Timestamp(reader.i64()?));
            }
            (out, decode_validity(&mut vr, n)?)
        }
        (DataType::Integer, ENC_RLE)
        | (DataType::Bigint, ENC_RLE)
        | (DataType::Date, ENC_RLE)
        | (DataType::Timestamp, ENC_RLE) => {
            let n = reader.chunk_count("chunk length")?;
            let runs = reader.chunk_count("run count")?;
            let mut out = Vec::with_capacity(n);
            for _ in 0..runs {
                let v = if dtype == DataType::Integer {
                    Value::Integer(reader.i32()?)
                } else if dtype == DataType::Date {
                    Value::Date(reader.i32()?)
                } else if dtype == DataType::Timestamp {
                    Value::Timestamp(reader.i64()?)
                } else {
                    Value::Bigint(reader.i64()?)
                };
                let len = reader.u32()? as usize;
                if out.len() + len > n {
                    return reader.err("RLE run overflows chunk length");
                }
                out.extend(std::iter::repeat_n(v, len));
            }
            if out.len() != n {
                return reader.err("RLE runs underfill chunk length");
            }
            (out, decode_validity(&mut vr, n)?)
        }
        (DataType::Integer, ENC_BITPACK)
        | (DataType::Bigint, ENC_BITPACK)
        | (DataType::Date, ENC_BITPACK)
        | (DataType::Timestamp, ENC_BITPACK) => {
            let n = reader.chunk_count("chunk length")?;
            let min = reader.i64()?;
            let width = reader.u8()? as u32;
            if width > 64 {
                return reader.err("bad bitpack width");
            }
            let shifted = unpack_bits(reader, n, width)?;
            let out = shifted
                .into_iter()
                .map(|s| {
                    let v = min.wrapping_add(s as i64);
                    if dtype == DataType::Integer {
                        Value::Integer(v as i32)
                    } else if dtype == DataType::Date {
                        Value::Date(v as i32)
                    } else if dtype == DataType::Timestamp {
                        Value::Timestamp(v)
                    } else {
                        Value::Bigint(v)
                    }
                })
                .collect();
            (out, decode_validity(&mut vr, n)?)
        }
        (DataType::Double, ENC_PLAIN) => {
            let n = reader.chunk_count("chunk length")?;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                out.push(Value::Double(reader.f64()?));
            }
            (out, decode_validity(&mut vr, n)?)
        }
        (DataType::Double, ENC_RLE) => {
            let n = reader.chunk_count("chunk length")?;
            let runs = reader.chunk_count("run count")?;
            let mut out = Vec::with_capacity(n);
            for _ in 0..runs {
                let v = Value::Double(reader.f64()?);
                let len = reader.u32()? as usize;
                if out.len() + len > n {
                    return reader.err("RLE run overflows chunk length");
                }
                out.extend(std::iter::repeat_n(v, len));
            }
            if out.len() != n {
                return reader.err("RLE runs underfill chunk length");
            }
            (out, decode_validity(&mut vr, n)?)
        }
        (DataType::Boolean, ENC_PLAIN) => {
            let n = reader.chunk_count("chunk length")?;
            let need = n.div_ceil(8);
            let bytes = reader.take(need)?;
            let out = (0..n)
                .map(|i| Value::Boolean((bytes[i / 8] >> (i % 8)) & 1 == 1))
                .collect();
            (out, decode_validity(&mut vr, n)?)
        }
        (DataType::Boolean, ENC_RLE) => {
            let n = reader.chunk_count("chunk length")?;
            let runs = reader.chunk_count("run count")?;
            let mut out = Vec::with_capacity(n);
            for _ in 0..runs {
                let v = Value::Boolean(reader.u8()? != 0);
                let len = reader.u32()? as usize;
                if out.len() + len > n {
                    return reader.err("RLE run overflows chunk length");
                }
                out.extend(std::iter::repeat_n(v, len));
            }
            if out.len() != n {
                return reader.err("RLE runs underfill chunk length");
            }
            (out, decode_validity(&mut vr, n)?)
        }
        (DataType::Varchar, ENC_PLAIN) => {
            let n = reader.chunk_count("chunk length")?;
            let strs = decode_strs(reader, n)?;
            (
                strs.into_iter().map(Value::Varchar).collect(),
                decode_validity(&mut vr, n)?,
            )
        }
        (DataType::Varchar, ENC_DICT) => {
            let n = reader.chunk_count("chunk length")?;
            let dict_len = reader.chunk_count("dictionary size")?;
            let dict = decode_strs(reader, dict_len)?;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                let code = reader.u32()? as usize;
                out.push(Value::Varchar(dict.get(code).cloned().ok_or_else(
                    || PinkDbError::Execution("corrupt file: bad dict code".to_string()),
                )?));
            }
            (out, decode_validity(&mut vr, n)?)
        }
        (DataType::Varchar, ENC_RLE) => {
            let n = reader.chunk_count("chunk length")?;
            let runs = reader.chunk_count("run count")?;
            let vals = decode_strs(reader, runs)?;
            let mut out = Vec::with_capacity(n);
            for v in vals {
                let len = reader.u32()? as usize;
                if out.len() + len > n {
                    return reader.err("RLE run overflows chunk length");
                }
                out.extend(std::iter::repeat_n(Value::Varchar(v), len));
            }
            if out.len() != n {
                return reader.err("RLE runs underfill chunk length");
            }
            (out, decode_validity(&mut vr, n)?)
        }
        _ => {
            return Err(PinkDbError::Execution(format!(
                "corrupt file: encoding {tag} invalid for {dtype}"
            )));
        }
    };
    if !vr.end() {
        return Err(PinkDbError::Execution(
            "corrupt file: trailing validity bytes".to_string(),
        ));
    }
    apply_validity(values, &valid)
}

// ---------- public API ----------

impl PinkDb {
    /// Serialize every database (and every table in it) in the `.pink`
    /// columnar format. Databases and tables are stored in sorted name order
    /// (deterministic bytes). Views are session-local and never persisted.
    fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        w.bytes(MAGIC_V3);
        let mut dbs = self.list_databases();
        dbs.sort();
        w.u32(dbs.len() as u32);
        for dbname in &dbs {
            w.str(dbname);
            let mut names = self.list_tables_in(dbname);
            names.sort();
            w.u32(names.len() as u32);
            for name in &names {
                let t = self.table_in(dbname, name)?;
                let schema = t.schema();
                w.str(&t.name);
                w.u32(schema.len() as u32);
                for (col, dtype) in &schema {
                    w.str(col);
                    w.u8(dtype_tag(*dtype));
                }
                w.u64(t.num_rows() as u64);
                for (ci, (_, dtype)) in schema.iter().enumerate() {
                    let col_idx = ci;
                    let n_chunks = t.num_chunks();
                    w.u32(n_chunks as u32);
                    for chunk_idx in 0..n_chunks {
                        let chunk = t.scan_chunk(chunk_idx).ok_or_else(|| {
                            PinkDbError::Execution("storage changed during save".to_string())
                        })?;
                        let vec = &chunk.columns[col_idx];
                        debug_assert_eq!(vec.dtype(), *dtype);
                        // Materialize placeholder values for NULLs (validity separate).
                        let values: Vec<Value> = (0..vec.len()).map(|i| vec.get(i)).collect();
                        let plain_for_null: Vec<Value> = values
                            .into_iter()
                            .map(|v| match v {
                                Value::Null => match dtype {
                                    DataType::Boolean => Value::Boolean(false),
                                    DataType::Integer => Value::Integer(0),
                                    DataType::Bigint => Value::Bigint(0),
                                    DataType::Double => Value::Double(0.0),
                                    DataType::Varchar => Value::Varchar(String::new()),
                                    DataType::Date => Value::Date(0),
                                    DataType::Timestamp => Value::Timestamp(0),
                                },
                                v => v,
                            })
                            .collect();
                        let (tag, payload) = encode_chunk(*dtype, &plain_for_null);
                        w.u8(tag);
                        let mut vw = Writer::new();
                        encode_validity(&mut vw, vec);
                        w.u32(vw.buf.len() as u32);
                        w.bytes(&vw.buf);
                        w.bytes(&payload);
                    }
                }
            }
        }
        let crc = crate::wal::crc32(&w.buf);
        w.u32(crc);
        Ok(w.buf)
    }

    /// Write every database to `path` in the `.pink` columnar format.
    /// Databases and tables are stored in sorted name order (deterministic
    /// bytes). Views are session-local and never persisted.
    ///
    /// Always writes plaintext; use [`PinkDb::save_with_key`] (feature
    /// `encryption`) for an encrypted container.
    pub fn save(&self, path: &str) -> Result<()> {
        // Never serialize a store holding a non-durable write (fail-stop).
        // (Checkpoints check the same flag under the WAL lock they hold.)
        self.ensure_wal_healthy()?;
        std::fs::write(path, self.to_bytes()?)?;
        Ok(())
    }

    /// [`PinkDb::save`] with at-rest encryption: the v2 bytes are sealed into
    /// a `PINKENC1` container (AES-256-GCM, Argon2id for passphrases). Without
    /// the `encryption` feature this fails with a clear "not compiled in"
    /// error instead of writing plaintext.
    pub fn save_with_key(&self, path: &str, key: &EncryptionKey) -> Result<()> {
        #[cfg(not(feature = "encryption"))]
        {
            let _ = (path, key);
            Err(crate::crypto::unavailable())
        }
        #[cfg(feature = "encryption")]
        {
            self.ensure_wal_healthy()?;
            let sealed = crate::crypto::seal_container(&self.to_bytes()?, key, 0)?;
            std::fs::write(path, sealed)?;
            Ok(())
        }
    }

    /// [`PinkDb::save`] plus `fsync` on the written file — the checkpoint
    /// primitive (the WAL is only rolled once this returns).
    pub(crate) fn save_sync(&self, path: &std::path::Path) -> Result<()> {
        Self::write_sync(path, &self.to_bytes()?)
    }

    /// [`PinkDb::save_sync`] with at-rest encryption (checkpoint primitive for
    /// a keyed durable database; the WAL is only rolled once this returns).
    #[cfg(feature = "encryption")]
    pub(crate) fn save_sync_with_key(
        &self,
        path: &std::path::Path,
        key: &EncryptionKey,
    ) -> Result<()> {
        let sealed = crate::crypto::seal_container(&self.to_bytes()?, key, 0)?;
        Self::write_sync(path, &sealed)
    }

    fn write_sync(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(path)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        Ok(())
    }

    /// Load a database from a `.pink` file written by [`PinkDb::save`].
    /// Accepts the v1 format (a single, implicit `main` database) and the v2
    /// format (all databases). Truncated or malformed files return errors,
    /// never panics. An encrypted (`PINKENC1`) file is rejected: without the
    /// `encryption` feature the error says to rebuild with it; with the
    /// feature it points at [`PinkDb::open_with_key`].
    pub fn open(path: &str) -> Result<PinkDb> {
        let data = std::fs::read(path)?;
        if crate::crypto::is_container(&data) {
            return Err(crate::crypto::encrypted_error(path));
        }
        Self::load_bytes(&data)
    }

    /// [`PinkDb::open`] for an encrypted container, decrypting with `key`.
    /// The file must be encrypted (a `PINKENC1` container); a plaintext
    /// `.pink` is refused so a key can never be silently ignored. A wrong key
    /// fails the key-check block and reports `wrong key`. Without the
    /// `encryption` feature this fails with a clear "not compiled in" error.
    pub fn open_with_key(path: &str, key: &EncryptionKey) -> Result<PinkDb> {
        #[cfg(not(feature = "encryption"))]
        {
            let _ = (path, key);
            Err(crate::crypto::unavailable())
        }
        #[cfg(feature = "encryption")]
        {
            let data = std::fs::read(path)?;
            if !crate::crypto::is_container(&data) {
                return Err(PinkDbError::Execution(format!(
                    "{path}: file is not encrypted (a key was supplied); open it without a key"
                )));
            }
            let plain = crate::crypto::open_container(&data, key, 0)?;
            let mut db = Self::load_bytes(&plain)?;
            db.encryption_key = Some(key.clone());
            Ok(db)
        }
    }

    /// Decode a serialized `.pink` image (v1, v2 or v3 magic).
    fn load_bytes(data: &[u8]) -> Result<PinkDb> {
        let data = if data.starts_with(MAGIC_V3) {
            // Verify the trailer before decoding anything.
            if data.len() < MAGIC_V3.len() + 4 {
                return Err(PinkDbError::Execution(
                    "corrupt file: truncated before the checksum".to_string(),
                ));
            }
            let (body, trailer) = data.split_at(data.len() - 4);
            let want = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
            if crate::wal::crc32(body) != want {
                return Err(PinkDbError::Execution(
                    "corrupt file: checksum mismatch (the file is damaged)".to_string(),
                ));
            }
            body
        } else {
            data
        };
        let mut r = Reader::new(data);
        let magic = r.take(8)?;
        let v2 = if magic == MAGIC_V2 || magic == MAGIC_V3 {
            true
        } else if magic == MAGIC {
            false
        } else {
            return Err(PinkDbError::Execution(
                "not a .pink file (bad magic)".to_string(),
            ));
        };
        // Start from an empty store: a v2 file may legitimately have no
        // `main` (it was dropped) and `open` must round-trip exactly.
        let mut db = PinkDb::with_databases(Vec::<String>::new());
        if v2 {
            let n_dbs = r.u32()?;
            for _ in 0..n_dbs {
                let dbname = r.str()?;
                db.create_database(&dbname)?;
                let n_tables = r.u32()?;
                for _ in 0..n_tables {
                    load_table(&mut r, &mut db, &dbname)?;
                }
            }
            if db.list_databases().is_empty() {
                db.create_database("main")?;
            }
        } else {
            // v1: every table lived in the single implicit database.
            db.create_database("main")?;
            let n_tables = r.u32()?;
            for _ in 0..n_tables {
                load_table(&mut r, &mut db, "main")?;
            }
        }
        if !r.end() {
            return Err(PinkDbError::Execution(
                "corrupt file: trailing bytes after last table".to_string(),
            ));
        }
        // The session starts on `main` when it exists, else the first
        // database in sorted order.
        let current = db.fallback_db();
        db.set_current_db(&current);
        Ok(db)
    }
}

/// Decode one table from `r` into `db`'s `dbname` database.
fn load_table(r: &mut Reader, db: &mut PinkDb, dbname: &str) -> Result<()> {
    let name = r.str()?;
    let n_cols = r.u32()? as usize;
    // Each column costs at least 5 bytes (name length + dtype tag).
    if n_cols > r.remaining() / 5 {
        return r.err("column count exceeds the remaining file");
    }
    let mut schema = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        let col = r.str()?;
        let dtype = dtype_from_tag(r.u8()?)?;
        schema.push((col, dtype));
    }
    let num_rows = r.u64()? as usize;
    // Decode per-column chunks, then append row-wise.
    let mut columns: Vec<Vec<Value>> = vec![Vec::new(); n_cols];
    for (ci, (_, dtype)) in schema.iter().enumerate() {
        let n_chunks = r.u32()? as usize;
        let mut decoded = 0usize;
        for _ in 0..n_chunks {
            let tag = r.u8()?;
            let vals = decode_chunk(*dtype, tag, r)?;
            decoded += vals.len();
            columns[ci].extend(vals);
        }
        if decoded != num_rows {
            return Err(PinkDbError::Execution(format!(
                "corrupt file: column {ci} has {decoded} values, want {num_rows}"
            )));
        }
    }
    // Sanity: chunk rows must stay small (guards giant allocations).
    for col in &columns {
        if col.len() != num_rows {
            return Err(PinkDbError::Execution(
                "corrupt file: column length mismatch".to_string(),
            ));
        }
    }
    db.create_table_in(dbname, &name, schema)?;
    let t = db.table_mut_in(dbname, &name)?;
    for i in 0..num_rows {
        let row: Vec<Value> = columns.iter().map(|c| c[i].clone()).collect();
        t.append_row(row)?;
    }
    Ok(())
}

/// Re-export for tests: chunk encode/decode round trip.
#[cfg(test)]
pub(crate) fn roundtrip_chunk(dtype: DataType, values: Vec<Value>, tag: u8) -> Vec<Value> {
    // Split NULLs (validity) from placeholder payload values.
    let plain: Vec<Value> = values
        .iter()
        .map(|v| match v {
            Value::Null => match dtype {
                DataType::Boolean => Value::Boolean(false),
                DataType::Integer => Value::Integer(0),
                DataType::Bigint => Value::Bigint(0),
                DataType::Double => Value::Double(0.0),
                DataType::Varchar => Value::Varchar(String::new()),
                DataType::Date => Value::Date(0),
                DataType::Timestamp => Value::Timestamp(0),
            },
            v => v.clone(),
        })
        .collect();
    let valid: Vec<bool> = values.iter().map(|v| !v.is_null()).collect();
    let mut vw = Writer::new();
    for w0 in 0..plain.len().div_ceil(64) {
        let mut word: u64 = 0;
        for b in 0..64 {
            let i = w0 * 64 + b;
            if i < valid.len() && valid[i] {
                word |= 1u64 << b;
            }
        }
        vw.u64(word);
    }
    // Payload forced to the requested tag.
    let ints: Vec<i64> = plain.iter().map(as_i64).collect();
    let strs: Vec<&str> = plain
        .iter()
        .map(|v| match v {
            Value::Varchar(s) => s.as_str(),
            _ => "",
        })
        .collect();
    let forced = match (dtype, tag) {
        (DataType::Integer, ENC_PLAIN)
        | (DataType::Bigint, ENC_PLAIN)
        | (DataType::Date, ENC_PLAIN)
        | (DataType::Timestamp, ENC_PLAIN) => {
            let enc_dtype = match dtype {
                DataType::Date => DataType::Integer,
                DataType::Timestamp => DataType::Bigint,
                t => t,
            };
            encode_ints_plain(enc_dtype, &ints)
        }
        (DataType::Double, ENC_PLAIN) => encode_u64_plain(
            &plain
                .iter()
                .map(|v| match v {
                    Value::Double(x) => x.to_bits(),
                    _ => 0,
                })
                .collect::<Vec<_>>(),
        ),
        (DataType::Boolean, ENC_PLAIN) => encode_bool_plain(
            &plain
                .iter()
                .map(|v| matches!(v, Value::Boolean(true)))
                .collect::<Vec<_>>(),
        ),
        (DataType::Varchar, ENC_PLAIN) => encode_strs_plain(&strs),
        (DataType::Integer, ENC_RLE)
        | (DataType::Bigint, ENC_RLE)
        | (DataType::Date, ENC_RLE)
        | (DataType::Timestamp, ENC_RLE) => {
            let enc_dtype = match dtype {
                DataType::Date => DataType::Integer,
                DataType::Timestamp => DataType::Bigint,
                t => t,
            };
            encode_ints_rle(enc_dtype, &ints)
        }
        (DataType::Integer, ENC_BITPACK)
        | (DataType::Bigint, ENC_BITPACK)
        | (DataType::Date, ENC_BITPACK)
        | (DataType::Timestamp, ENC_BITPACK) => encode_ints_bitpack(&ints),
        (DataType::Double, ENC_RLE) => encode_u64_rle(
            &plain
                .iter()
                .map(|v| match v {
                    Value::Double(x) => x.to_bits(),
                    _ => 0,
                })
                .collect::<Vec<_>>(),
        ),
        (DataType::Boolean, ENC_RLE) => encode_bool_rle(
            &plain
                .iter()
                .map(|v| matches!(v, Value::Boolean(true)))
                .collect::<Vec<_>>(),
        ),
        (DataType::Varchar, ENC_DICT) => encode_strs_dict(&strs),
        (DataType::Varchar, ENC_RLE) => encode_strs_rle(&strs),
        _ => panic!("roundtrip_chunk: unsupported forced tag {tag} for {dtype:?}"),
    };
    let mut w = Writer::new();
    w.u8(tag);
    w.u32(vw.buf.len() as u32);
    w.bytes(&vw.buf);
    w.bytes(&forced);
    let mut r = Reader::new(&w.buf);
    let t = r.u8().unwrap();
    decode_chunk(dtype, t, &mut r)
        .unwrap_or_else(|e| panic!("CTX dtype={dtype:?} tag={t} n={} err={e}", values.len()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::CHUNK_SIZE;

    fn roundtrip(db: &PinkDb) -> PinkDb {
        let path = temp_file("rt");
        db.save(path.to_str().unwrap()).unwrap();
        let data = std::fs::read(&path).unwrap();
        assert!(data.starts_with(MAGIC_V3));
        let back = PinkDb::open(path.to_str().unwrap()).unwrap();
        std::fs::remove_file(&path).ok();
        back
    }

    /// A unique temp file per call: pid + a process-wide counter, so
    /// concurrent tests *and* concurrent `cargo test` invocations cannot
    /// race on one path.
    fn temp_file(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "pinkdb_{tag}_{}_{}.pink",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn all_values(db: &PinkDb, table: &str) -> Vec<Vec<Value>> {
        let t = db.table(table).unwrap();
        (0..t.num_rows())
            .map(|r| (0..t.num_cols()).map(|c| t.get(r, c).unwrap()).collect())
            .collect()
    }

    #[test]
    fn roundtrip_all_types_and_nulls() {
        let mut db = PinkDb::new();
        db.create_table(
            "t",
            vec![
                ("b".to_string(), DataType::Boolean),
                ("i".to_string(), DataType::Integer),
                ("g".to_string(), DataType::Bigint),
                ("d".to_string(), DataType::Double),
                ("v".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            t.append_row(vec![
                Value::Boolean(true),
                Value::Integer(-5),
                Value::Bigint(9_999_999_999),
                Value::Double(-0.0),
                Value::Varchar("hi".to_string()),
            ])
            .unwrap();
            t.append_row(vec![
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ])
            .unwrap();
            t.append_row(vec![
                Value::Boolean(false),
                Value::Integer(i32::MAX),
                Value::Bigint(i64::MIN),
                Value::Double(f64::NAN),
                Value::Varchar("héllo wörld".to_string()),
            ])
            .unwrap();
        }
        let back = roundtrip(&db);
        let a = all_values(&db, "t");
        let b = all_values(&back, "t");
        assert_eq!(a.len(), b.len());
        for (ra, rb) in a.iter().zip(b.iter()) {
            for (va, vb) in ra.iter().zip(rb.iter()) {
                match (va, vb) {
                    // NaN != NaN: compare by bits.
                    (Value::Double(x), Value::Double(y)) if x.is_nan() && y.is_nan() => {}
                    _ => assert_eq!(va, vb),
                }
            }
        }
        // -0.0 round-trips exactly (storage is bit-exact).
        assert_eq!(b[0][3], Value::Double(-0.0));
        assert!(matches!(b[0][3], Value::Double(x) if x.to_bits() == (-0.0f64).to_bits()));
    }

    #[test]
    fn roundtrip_empty_and_multichunk() {
        let mut db = PinkDb::new();
        db.create_table("e", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        db.create_table("big", vec![("y".to_string(), DataType::Varchar)])
            .unwrap();
        {
            let t = db.table_mut("big").unwrap();
            for i in 0..(CHUNK_SIZE + 100) {
                // Low cardinality → dictionary + multi-chunk.
                t.append_row(vec![Value::Varchar(format!("region{}", i % 3))])
                    .unwrap();
            }
        }
        let back = roundtrip(&db);
        assert_eq!(back.table("e").unwrap().num_rows(), 0);
        assert_eq!(back.table("big").unwrap().num_rows(), CHUNK_SIZE + 100);
        assert_eq!(all_values(&db, "big"), all_values(&back, "big"));
    }

    #[test]
    fn forced_encodings_roundtrip() {
        let ints: Vec<Value> = (0..100).map(|i| Value::Integer(i / 10)).collect();
        assert_eq!(
            roundtrip_chunk(DataType::Integer, ints.clone(), ENC_RLE),
            ints
        );
        assert_eq!(
            roundtrip_chunk(DataType::Integer, ints.clone(), ENC_BITPACK),
            ints
        );
        assert_eq!(
            roundtrip_chunk(DataType::Integer, ints.clone(), ENC_PLAIN),
            ints
        );
        let neg: Vec<Value> = vec![Value::Integer(-3), Value::Null, Value::Integer(5)];
        assert_eq!(
            roundtrip_chunk(DataType::Integer, neg.clone(), ENC_BITPACK),
            neg
        );
        let strs: Vec<Value> = vec![
            Value::Varchar("a".to_string()),
            Value::Varchar("a".to_string()),
            Value::Null,
            Value::Varchar("b".to_string()),
        ];
        assert_eq!(
            roundtrip_chunk(DataType::Varchar, strs.clone(), ENC_DICT),
            strs
        );
        assert_eq!(
            roundtrip_chunk(DataType::Varchar, strs.clone(), ENC_RLE),
            strs
        );
        let bools: Vec<Value> = vec![Value::Boolean(true), Value::Null, Value::Boolean(true)];
        assert_eq!(
            roundtrip_chunk(DataType::Boolean, bools.clone(), ENC_RLE),
            bools
        );
        let dbls: Vec<Value> = vec![Value::Double(1.5), Value::Double(1.5), Value::Null];
        assert_eq!(
            roundtrip_chunk(DataType::Double, dbls.clone(), ENC_RLE),
            dbls
        );
    }

    #[test]
    fn encoding_choice_picks_smallest() {
        // Constant run → RLE or bitpack must beat plain.
        let run: Vec<Value> = vec![Value::Integer(7); 100];
        let (tag, payload) = encode_chunk(DataType::Integer, &run);
        assert_ne!(tag, ENC_PLAIN, "run of 100 should not be plain");
        assert!(payload.len() < 400);
        // Incompressible full-range data → plain (bitpack needs 32 bits + header).
        let mut rnd: Vec<Value> = Vec::new();
        let mut x: u64 = 42;
        for _ in 0..100 {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            rnd.push(Value::Integer((x >> 32) as i32));
        }
        let (tag, _) = encode_chunk(DataType::Integer, &rnd);
        assert_eq!(tag, ENC_PLAIN);
        // Low-cardinality strings → dictionary.
        let strs: Vec<Value> = (0..100)
            .map(|i| Value::Varchar(format!("k{}", i % 4)))
            .collect();
        let (tag, _) = encode_chunk(
            DataType::Varchar,
            &strs
                .iter()
                .map(|v| match v {
                    Value::Varchar(s) => Value::Varchar(s.clone()),
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
        );
        assert_eq!(tag, ENC_DICT);
    }

    #[test]
    fn corrupt_files_error_without_panic() {
        let mut db = PinkDb::new();
        db.create_table("t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(1)])
            .unwrap();
        let path = temp_file("corrupt");
        db.save(path.to_str().unwrap()).unwrap();
        let full = std::fs::read(&path).unwrap();
        // Every truncation errors (never panics).
        for cut in [0, 4, 8, 9, 12, 20, full.len() / 2, full.len() - 1] {
            std::fs::write(&path, &full[..cut]).unwrap();
            assert!(
                PinkDb::open(path.to_str().unwrap()).is_err(),
                "cut at {cut}"
            );
        }
        // Garbage magic + flipped bytes error.
        std::fs::write(&path, b"GARBAGE!junkjunkjunk").unwrap();
        assert!(PinkDb::open(path.to_str().unwrap()).is_err());
        let mut bad = full.clone();
        bad[10] = 0xFF;
        std::fs::write(&path, &bad).unwrap();
        assert!(PinkDb::open(path.to_str().unwrap()).is_err());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn v3_checksum_catches_bit_rot_and_v2_still_loads() {
        let mut db = PinkDb::new();
        db.create_table("t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(7)])
            .unwrap();
        let full = db.to_bytes().unwrap();
        assert!(full.starts_with(MAGIC_V3));
        // A flipped payload bit is a checksum error, not a silent wrong value.
        for i in MAGIC_V3.len()..full.len() {
            let mut bad = full.clone();
            bad[i] ^= 0x01;
            let err = PinkDb::load_bytes(&bad).expect_err("bit flip must be detected");
            assert!(
                err.to_string().contains("checksum") || i >= full.len() - 4,
                "{i}: {err}"
            );
        }
        // The same image without the trailer and with the v2 magic still loads.
        let mut v2 = full[..full.len() - 4].to_vec();
        v2[..8].copy_from_slice(MAGIC_V2);
        let back = PinkDb::load_bytes(&v2).unwrap();
        assert_eq!(back.table("t").unwrap().num_rows(), 1);
    }

    #[test]
    fn forged_lengths_never_size_allocations() {
        // v2 has no checksum, so forged lengths reach the decoder: plant
        // u32::MAX at every offset of a multi-type image. Each load must
        // return (Ok or Err) instead of aborting on a giant allocation.
        let mut db = PinkDb::new();
        crate::csv::import_csv_string(&mut db, "s", "a,b,c,d\n1,x,1.5,true\n2,y,2.5,false\n")
            .unwrap();
        let full = db.to_bytes().unwrap();
        let mut v2 = full[..full.len() - 4].to_vec();
        v2[..8].copy_from_slice(MAGIC_V2);
        // (Overcommitting allocators may not abort on the old code path, so
        // also require that the chunk-length cap actually fires.)
        let mut capped = 0;
        for i in 8..v2.len().saturating_sub(3) {
            let mut bad = v2.clone();
            bad[i..i + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            if let Err(e) = PinkDb::load_bytes(&bad) {
                if e.to_string().contains("exceeds the chunk size") {
                    capped += 1;
                }
            }
        }
        assert!(
            capped >= 4,
            "every column's chunk length must hit the cap ({capped})"
        );
    }

    #[test]
    fn save_load_via_sql() {
        let mut db = PinkDb::new();
        crate::csv::import_csv_string(&mut db, "s", "a,b\n1,x\n2,\n").unwrap();
        db.execute("INSERT INTO s VALUES (3, 'z');").unwrap();
        let path = temp_file("sql");
        db.save(path.to_str().unwrap()).unwrap();
        let mut back = PinkDb::open(path.to_str().unwrap()).unwrap();
        let rows = back
            .execute("SELECT a, b FROM s ORDER BY a;")
            .unwrap()
            .pop()
            .unwrap();
        match rows {
            crate::QueryResult::Selected { rows, .. } => {
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[1][1], Value::Null);
            }
            _ => panic!(),
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn roundtrip_multiple_databases_and_empty_ones() {
        let mut db = PinkDb::new();
        db.execute("CREATE DATABASE analytics;").unwrap();
        db.execute("CREATE DATABASE empty;").unwrap();
        db.execute("CREATE TABLE main.t (a INTEGER, b VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO main.t VALUES (1, 'x'), (2, NULL);")
            .unwrap();
        db.execute("CREATE TABLE analytics.u (k BIGINT);").unwrap();
        db.execute("INSERT INTO analytics.u VALUES (7);").unwrap();
        // Same table name in two databases, different shapes.
        db.execute("CREATE TABLE analytics.t (z DOUBLE);").unwrap();
        db.execute("INSERT INTO analytics.t VALUES (1.5);").unwrap();

        let back = roundtrip(&db);
        assert_eq!(back.list_databases(), vec!["analytics", "empty", "main"]);
        assert_eq!(back.current_db(), "main");
        assert_eq!(all_values(&back, "t").len(), 2);
        assert_eq!(all_values(&back, "t")[1][1], Value::Null);
        assert_eq!(back.list_tables_in("analytics"), vec!["t", "u"]);
        assert_eq!(back.table_in("analytics", "u").unwrap().num_rows(), 1);
        assert_eq!(
            back.table_in("analytics", "t").unwrap().get(0, 0),
            Some(Value::Double(1.5))
        );
        assert!(back.list_tables_in("empty").is_empty());
        assert!(back.database_exists("empty"));
    }

    /// A `.pink` file written by the v1 code path (committed fixture) must
    /// still load: every table lands in a single `main` database.
    #[test]
    fn v1_fixture_loads_as_single_main_database() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/v1_single_db.pink"
        );
        let data = std::fs::read(path).unwrap();
        assert!(data.starts_with(MAGIC), "fixture must be the v1 format");
        let db = PinkDb::open(path).unwrap();
        assert_eq!(db.list_databases(), vec!["main".to_string()]);
        assert_eq!(db.list_tables(), vec!["empty".to_string(), "t".to_string()]);
        let t = db.table("t").unwrap();
        assert_eq!(t.num_rows(), 3);
        assert_eq!(t.get(0, 0), Some(Value::Integer(1)));
        assert_eq!(t.get(0, 1), Some(Value::Varchar("x".to_string())));
        assert_eq!(t.get(1, 1), Some(Value::Null));
        assert_eq!(t.get(2, 0), Some(Value::Integer(3)));
        assert_eq!(db.table("empty").unwrap().num_rows(), 0);
        // A v1 file re-saved becomes v2 and round-trips.
        let back = roundtrip(&db);
        assert_eq!(back.list_databases(), vec!["main".to_string()]);
        assert_eq!(all_values(&back, "t"), all_values(&db, "t"));
    }

    /// A store whose `main` was dropped persists without resurrecting it.
    #[test]
    fn roundtrip_without_main_keeps_the_remaining_databases() {
        let mut db = PinkDb::new();
        db.execute("CREATE DATABASE db1; CREATE TABLE db1.t (a INTEGER);")
            .unwrap();
        db.execute("INSERT INTO db1.t VALUES (5);").unwrap();
        db.execute("DROP DATABASE main;").unwrap();
        assert_eq!(db.current_db(), "db1");
        let back = roundtrip(&db);
        assert_eq!(back.list_databases(), vec!["db1".to_string()]);
        assert_eq!(
            back.current_db(),
            "db1",
            "session starts on the only database"
        );
        assert_eq!(all_values(&back, "t"), vec![vec![Value::Integer(5)]]);
    }

    /// A hand-built but structurally valid `PINKENC1` header (raw-key KDF).
    #[cfg(not(feature = "encryption"))]
    fn fake_encrypted_container() -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(b"PINKENC1");
        v.extend_from_slice(&1u32.to_le_bytes()); // container version
        v.push(1u8); // cipher: AES-256-GCM
        v.push(0u8); // kdf: raw
        v.extend_from_slice(&0u64.to_le_bytes()); // context: checkpoint
        v.extend_from_slice(&[0x42u8; 8]); // nonce prefix
        v.extend_from_slice(&30u32.to_le_bytes()); // key-check plaintext length
        v.extend_from_slice(&[0xEEu8; 46]); // key-check ciphertext + tag
        v.extend_from_slice(b"\x05\0\0\0hello"); // one bogus payload frame
        v
    }

    /// With the `encryption` feature off, opening an encrypted file must say
    /// exactly how to get support instead of "corrupt file".
    #[cfg(not(feature = "encryption"))]
    #[test]
    fn encrypted_file_errors_clearly_without_the_feature() {
        let path = temp_file("plain_enc");
        std::fs::write(&path, fake_encrypted_container()).unwrap();
        let err = PinkDb::open(path.to_str().unwrap()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("file is encrypted; build with the encryption feature"),
            "{msg}"
        );
        std::fs::remove_file(&path).ok();
    }

    /// A keyed open in a crypto-free build names the missing support before
    /// it even looks at the file.
    #[cfg(not(feature = "encryption"))]
    #[test]
    fn keyed_open_without_the_feature_names_the_missing_support() {
        let key = EncryptionKey::from_passphrase("sesame");
        let err = PinkDb::open_with_key("does-not-matter.pink", &key).unwrap_err();
        assert!(
            err.to_string().contains("not compiled in"),
            "unexpected: {err}"
        );
        let err = PinkDb::open_durable_with_key("does-not-matter.pink", &key).unwrap_err();
        assert!(
            err.to_string().contains("not compiled in"),
            "unexpected: {err}"
        );
    }

    /// With the feature on, an encrypted file opened without a key points at
    /// the keyed API; `open_with_key` on a plaintext file is refused rather
    /// than silently ignoring the key.
    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_file_open_paths_points_at_the_key_api() {
        let enc = temp_file("enc_paths");
        let plain = temp_file("plain_paths");
        let key = EncryptionKey::from_raw([5u8; 32]);
        let mut db = PinkDb::new();
        db.create_table("t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(1)])
            .unwrap();
        db.save_with_key(enc.to_str().unwrap(), &key).unwrap();
        db.save(plain.to_str().unwrap()).unwrap();

        let err = PinkDb::open(enc.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("open it with a key"), "{err}");
        let err = PinkDb::open_with_key(plain.to_str().unwrap(), &key).unwrap_err();
        assert!(err.to_string().contains("not encrypted"), "{err}");
        std::fs::remove_file(&enc).ok();
        std::fs::remove_file(&plain).ok();
    }
}
