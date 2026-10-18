//! Persistence: a single-file on-disk columnar format (`.pink`).
//!
//! Layout v5 (all integers little-endian):
//! ```text
//! magic "PINKDB05" (8 bytes)
//! flags u32                 (0 = no optional sections; bit 0 = tables carry
//!                            a constraint section, bit 1 = a column-default
//!                            section, bit 2 = each database carries a
//!                            materialized-view section, bit 4 = a
//!                            stored-procedure section, bit 5 = a
//!                            store-level RBAC section; unknown bits are a
//!                            hard error; bit 3 is reserved for index
//!                            persistence)
//! db_count u32
//! per database:
//!   name_len u32 + name bytes
//!   table_count u32
//!   per table:
//!     name_len u32 + name bytes
//!     col_count u32
//!     per column: name_len u32 + name bytes, dtype u8 (tag 7 = DECIMAL:
//!       precision u8 + scale u8 follow; tag 8 = INTERVAL)
//!     if flags bit 0:
//!       constraint_count u32
//!       per constraint:
//!         kind u8 (0=PRIMARY KEY, 1=UNIQUE, 2=FOREIGN KEY, 3=NOT NULL)
//!         name_len u32 + name bytes (0 = unnamed; NOT NULL names are not
//!                                    persisted)
//!         col_count u32 + col_count × col_idx u32
//!         if kind == 2 (FOREIGN KEY):
//!           ref_db_len u32 + ref_db bytes (lowercased; non-empty)
//!           ref_table_len u32 + ref_table bytes (lowercased; non-empty)
//!           ref_col_count u32 + ref_col_count × ref_col_idx u32
//!     num_rows u64
//!     per column: chunk_count u32
//!       per chunk:
//!         encoding u8 (0=Plain, 1=RLE, 2=Dict, 3=Bitpack, 4=DeltaBitpack,
//!                      5=DictBitpack, 6=ALP, 8=LZ frame; 7 and 9 reserved)
//!         validity_len u32 + validity bytes (bit set = valid)
//!         payload (encoding-specific)
//! crc32 u32 (IEEE, over every preceding byte including the magic)
//! ```
//!
//! With `FLAG_RBAC` set (wave 48), the CRC is preceded by the store-global
//! RBAC section (see [`write_rbac`]): users (display name + Argon2id PHC
//! password hash), roles, role memberships, table grants, database grants
//! and the global `ADMIN` holders — every name in canonical (lowercased)
//! key form, sorted. The plaintext of a password is never written: the
//! hashed form is the only one the catalog (and the WAL) ever see.
//!
//! v2 files (`PINKDB02`) are the v4 container without the `flags` word and
//! the CRC trailer; v3 files (`PINKDB03`) are v2 + a CRC; v1 files
//! (`PINKDB01`) hold a flat table list and load as a single `main` database;
//! v4 files (`PINKDB04`) are the v5 container without the DECIMAL/INTERVAL
//! dtype tags and without the constraint section. `open` accepts all five;
//! writes are always v5.
//!
//! The constraint section carries declarative `PRIMARY KEY` / `UNIQUE` /
//! `FOREIGN KEY` / `NOT NULL` declarations (readers of v4-and-older simply
//! see unconstrained tables). Constraints are metadata: they are never
//! enforced on INSERT/UPDATE, only reported by `DESCRIBE` and trusted by the
//! optimizer. `RELY` / `[NOT] ENFORCED` suffixes are dropped at parse time;
//! a named `NOT NULL`'s name is not persisted (it is not observable).
//!
//! Payloads store every row including NULL placeholders; validity is applied
//! afterwards. Per-chunk encoding is picked by size (smallest wins), with a
//! fixed candidate order (first wins ties):
//! - Integers (INTEGER/BIGINT/DATE/TIMESTAMP): Plain (raw LE) vs RLE (low run
//!   count) vs Bitpack (small range, min + width + packed bits) vs delta
//!   bitpack (first value + min delta + width + packed deltas).
//! - Doubles: Plain (raw bits) vs RLE vs ALP (decimal-scaled integer codes
//!   plus a bit-exact exception list; non-finite, `-0.0`, subnormals and
//!   out-of-range values ride the exceptions).
//! - Booleans: Plain (bitpacked) vs RLE.
//! - Varchars: Plain (offsets + data) vs Dictionary (first-occurrence codes)
//!   vs RLE vs dictionary-bitpack (the dictionary with minimal-width codes)
//!   vs an LZ frame (tag 8) around the plain layout — attempted only when a
//!   bounded prefix probe says the string bytes repeat.
//!
//! The reader never panics on corrupt input: every read is bounds-checked and
//! surfaces `PinkDbError::Execution("...")`, and no allocation is sized from
//! an unvalidated length (chunk row counts are capped at [`CHUNK_SIZE`], the
//! writer's maximum; per-table column counts by the bytes left). A v3/v4 CRC
//! mismatch is reported before any decoding.
//!
//! Both directions are typed: `encode_chunk_typed` reads the [`Vector`]'s
//! payload directly (NULL placeholders substituted only when the chunk has
//! nulls) and `decode_chunk_typed` rebuilds a [`Vector`] via
//! [`Vector::from_raw`], so no per-row [`Value`] round trip is needed.
//! Tables are encoded chunk-parallel (order-preserving) and loaded
//! chunk-wise through `DataTable::append_shared_chunk`.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use rayon::prelude::*;

use crate::crypto::EncryptionKey;
use crate::storage::{
    Column, DataTable, ForeignKeyConstraint, KeyConstraint, KeyKind, PinkDb, TableConstraints,
};
use crate::types::{DataType, Interval, PinkDbError, Result, Value};
use crate::vector::{CHUNK_SIZE, Vector, VectorData};

const MAGIC: &[u8; 8] = b"PINKDB01";
/// Version 2 adds the database dimension: one file holds every database.
const MAGIC_V2: &[u8; 8] = b"PINKDB02";
/// Version 3 = v2 + a trailing CRC-32 over the whole image (bit-rot check).
const MAGIC_V3: &[u8; 8] = b"PINKDB03";
/// Version 4 = v3 + a `flags` word and the typed codecs (delta bitpack,
/// bitpacked dictionary codes, ALP, the all-valid validity flag).
const MAGIC_V4: &[u8; 8] = b"PINKDB04";
/// Version 5 = v4 + the DECIMAL/INTERVAL dtype tags (7/8; DECIMAL carries
/// its precision and scale in the schema) and a per-table declarative-
/// constraint section, gated by `FLAG_CONSTRAINTS` in the flags word (a
/// constraint-free, tag-free v5 body is byte-identical to a v4 body). Older
/// builds refuse it cleanly through the shared newer-magic path.
const MAGIC_V5: &[u8; 8] = b"PINKDB05";

/// Flags bit 0 (v5+): every table carries the constraint section described in
/// the module docs. Set iff at least one table has constraints.
const FLAG_CONSTRAINTS: u32 = 0x1;

/// Flags bit 1 (v5+): every table carries the column-default section (wave
/// 65) described in the module docs. Set iff at least one column has a
/// DEFAULT. An older v5 build refuses the file cleanly through the unknown-
/// flags check.
const FLAG_DEFAULTS: u32 = 0x2;

/// Flags bit 2 (v5+): every database carries the materialized-view section
/// (wave 80: name, refresh policy, defining SQL, last refresh stamp, refresh
/// count) after its tables. Set iff at least one database has a materialized
/// view, so stores without them keep v4-shaped bodies byte for byte. An older
/// v5 build refuses the file cleanly through the unknown-flags check — bit 3
/// (`0x8`) is reserved for index persistence (wave 79), so stored procedures
/// (wave 44) take bit 4.
const FLAG_MATERIALIZED_VIEWS: u32 = 0x4;

/// [`FLAG_PROCEDURES`], visible to the crate's tests (the store's flags word
/// is asserted from `crate::procedure`'s persistence tests).
#[cfg(test)]
pub(crate) const FLAG_PROCEDURES_FOR_TEST: u32 = FLAG_PROCEDURES;

/// Flags bit 4 (v5+): every database carries the stored-procedure section
/// (wave 44: name, typed parameters, language tag, body text) after its
/// tables and materialized views. Set iff at least one database has a
/// procedure, so stores without them keep their exact previous bytes. An
/// older v5 build refuses the file cleanly through the unknown-flags check;
/// the next format-gated section takes bit 5 (`0x20`).
const FLAG_PROCEDURES: u32 = 0x10;

/// Flags bit 5 (v5+): the file ends with the store-global RBAC section
/// (wave 48: users with their Argon2id PHC password hashes, roles, role
/// memberships, table and database grants, and the global `ADMIN` holders)
/// after every database. Set iff the store has any RBAC data, so stores
/// without users keep their exact previous bytes. An older v5 build refuses
/// the file cleanly through the unknown-flags check (the RBAC section is
/// unknown to it either way); the next format-gated section takes bit 6
/// (`0x40`).
const FLAG_RBAC: u32 = 0x20;

/// Column-default value tags (one byte per non-NULL default).
const VD_BOOL: u8 = 0;
const VD_I32: u8 = 1;
const VD_I64: u8 = 2;
const VD_F64_BITS: u8 = 3;
const VD_VARCHAR: u8 = 4;
const VD_DATE: u8 = 5;
const VD_TIMESTAMP: u8 = 6;
const VD_DECIMAL: u8 = 7;
const VD_INTERVAL: u8 = 8;

/// Constraint-section kinds (one byte per constraint).
const CK_PRIMARY_KEY: u8 = 0;
const CK_UNIQUE: u8 = 1;
const CK_FOREIGN_KEY: u8 = 2;
const CK_NOT_NULL: u8 = 3;

const ENC_PLAIN: u8 = 0;
const ENC_RLE: u8 = 1;
const ENC_DICT: u8 = 2;
const ENC_BITPACK: u8 = 3;
const ENC_DELTA_BITPACK: u8 = 4;
const ENC_DICT_BITPACK: u8 = 5;
const ENC_ALP: u8 = 6;
/// Reserved (a corpus never showed a win); readers reject it cleanly.
const ENC_DICT_RLE: u8 = 7;
/// LZ77 frame (tag 8): a plain payload wrapped in a token stream, used only
/// when it shrinks the chunk (see [`enc_lz_frame`]). Decode = decompress,
/// then decode the inner payload.
const ENC_LZ_FRAME: u8 = 8;
/// Reserved stretch tag (FSST); readers reject it cleanly.
const ENC_FSST: u8 = 9;

/// Tables with fewer chunks than this encode serially: the rayon fan-out
/// costs more than it saves on small tables.
const PARALLEL_SAVE_MIN_CHUNKS: usize = 32;

// ---------- save-time statistics ----------
//
// [`PinkDb::to_pink_bytes_with_stats`] collects exact per-column statistics
// during the same chunk pass that encodes a table (no second scan of the
// store, no change to the `.pink` bytes). The lake layer publishes them in
// its manifest; the engine itself never reads them back.

/// Exact per-column statistics the `.pink` writer observed while saving one
/// column: NULL count, the min/max of the non-NULL values (first-seen wins
/// ties, exactly like the engine's MIN/MAX accumulators), and the exact NaN
/// count (non-zero only for DOUBLE columns).
///
/// These are *facts*, not yet gated for use: a DOUBLE min/max with any NaN —
/// or one that is zero (`-0.0 == 0.0` in the engine's comparator) — is still
/// recorded here and refused later by the same exactness gate the Vortex
/// footer path uses (`pink-lake`'s attach layer), so the two formats cannot
/// drift apart.
#[derive(Debug, Clone, PartialEq)]
pub struct SavedColumnStats {
    /// Column name, as stored in the data file.
    pub name: String,
    /// Column type, as stored in the data file.
    pub dtype: DataType,
    /// Exact number of NULL rows.
    pub null_count: u64,
    /// Exact min of the non-NULL values, or `None` when the column has no
    /// non-NULL value (empty table or all-NULL column).
    pub min: Option<Value>,
    /// Exact max of the non-NULL values, or `None` like `min`.
    pub max: Option<Value>,
    /// Exact number of NaN values (doubles; always 0 for other types).
    pub nan_count: u64,
}

/// Exact statistics of one saved table, in data-file order.
#[derive(Debug, Clone, PartialEq)]
pub struct SavedTableStats {
    /// Database name the table lives in.
    pub db: String,
    /// Table name.
    pub table: String,
    /// Exact row count.
    pub rows: u64,
    /// Per-column statistics in schema (scan) order.
    pub columns: Vec<SavedColumnStats>,
}

/// One chunk's contribution to a column's exact statistics (merged across
/// chunks into a [`SavedColumnStats`]).
#[derive(Debug, Clone, PartialEq)]
struct ChunkColumnStats {
    null_count: u64,
    min: Option<Value>,
    max: Option<Value>,
    nan_count: u64,
}

/// The running merge of a column's chunk statistics (few comparisons per
/// chunk; uses the engine's own [`crate::plan::compare_values`] so ties and
/// NaN behave exactly like the MIN/MAX accumulators).
#[derive(Debug, Clone, PartialEq)]
struct ColumnStatsAcc {
    null_count: u64,
    min: Option<Value>,
    max: Option<Value>,
    nan_count: u64,
}

impl ColumnStatsAcc {
    fn new() -> Self {
        Self {
            null_count: 0,
            min: None,
            max: None,
            nan_count: 0,
        }
    }

    fn merge(&mut self, chunk: &ChunkColumnStats) {
        self.null_count += chunk.null_count;
        self.nan_count += chunk.nan_count;
        if let Some(v) = &chunk.min
            && self
                .min
                .as_ref()
                .is_none_or(|m| crate::plan::compare_values(v, m).is_lt())
        {
            self.min = Some(v.clone());
        }
        if let Some(v) = &chunk.max
            && self
                .max
                .as_ref()
                .is_none_or(|m| crate::plan::compare_values(v, m).is_gt())
        {
            self.max = Some(v.clone());
        }
    }
}

/// Exact statistics of one chunk of one column: the same strict comparisons
/// the MIN/MAX accumulators use (a tie never replaces the first-seen value).
fn chunk_column_stats(v: &Vector) -> ChunkColumnStats {
    let null_count = v.null_count() as u64;
    match v.data() {
        VectorData::Boolean(data) => {
            let (mut has_false, mut has_true) = (false, false);
            let mut see = |x: bool| {
                if x {
                    has_true = true;
                } else {
                    has_false = true;
                }
            };
            if v.has_nulls() {
                for (i, &x) in data.iter().enumerate() {
                    if v.is_valid(i) {
                        see(x);
                    }
                }
            } else {
                for &x in data {
                    see(x);
                }
            }
            // Min/max are the actual extreme values (DuckDB semantics): a
            // column of only `true` is min = max = true, and only `false` is
            // min = max = false — never "one of min/max" (a stats block must
            // carry both or neither).
            let any = has_false || has_true;
            ChunkColumnStats {
                null_count,
                min: any.then_some(Value::Boolean(!has_false)),
                max: any.then_some(Value::Boolean(has_true)),
                nan_count: 0,
            }
        }
        VectorData::Int32(data) => {
            let (min, max) = int_stats(v, data);
            ChunkColumnStats {
                null_count,
                min: min.map(Value::Integer),
                max: max.map(Value::Integer),
                nan_count: 0,
            }
        }
        VectorData::Int64(data) => {
            let (min, max) = int_stats(v, data);
            ChunkColumnStats {
                null_count,
                min: min.map(Value::Bigint),
                max: max.map(Value::Bigint),
                nan_count: 0,
            }
        }
        VectorData::Date32(data) => {
            let (min, max) = int_stats(v, data);
            ChunkColumnStats {
                null_count,
                min: min.map(Value::Date),
                max: max.map(Value::Date),
                nan_count: 0,
            }
        }
        VectorData::TimestampMicros(data) => {
            let (min, max) = int_stats(v, data);
            ChunkColumnStats {
                null_count,
                min: min.map(Value::Timestamp),
                max: max.map(Value::Timestamp),
                nan_count: 0,
            }
        }
        VectorData::Float64(data) => {
            let (mut min, mut max): (Option<f64>, Option<f64>) = (None, None);
            let mut nan_count = 0u64;
            let mut see = |x: f64| {
                if x.is_nan() {
                    nan_count += 1;
                    return;
                }
                if min.is_none_or(|m| x < m) {
                    min = Some(x);
                }
                if max.is_none_or(|m| x > m) {
                    max = Some(x);
                }
            };
            if v.has_nulls() {
                for (i, &x) in data.iter().enumerate() {
                    if v.is_valid(i) {
                        see(x);
                    }
                }
            } else {
                for &x in data {
                    see(x);
                }
            }
            ChunkColumnStats {
                null_count,
                min: min.map(Value::Double),
                max: max.map(Value::Double),
                nan_count,
            }
        }
        VectorData::VarChar(data) => {
            let (min, max) = str_stats(v, data);
            ChunkColumnStats {
                null_count,
                min: min.map(|s| Value::Varchar(s.to_string())),
                max: max.map(|s| Value::Varchar(s.to_string())),
                nan_count: 0,
            }
        }
        VectorData::Decimal { values, scale, .. } => {
            let (min, max) = int_stats(v, values);
            ChunkColumnStats {
                null_count,
                min: min.map(|x| Value::Decimal(x, *scale)),
                max: max.map(|x| Value::Decimal(x, *scale)),
                nan_count: 0,
            }
        }
        VectorData::Interval(data) => {
            let (mut min, mut max): (Option<Interval>, Option<Interval>) = (None, None);
            let mut see = |x: Interval| {
                if min.is_none_or(|m| crate::types::interval_cmp(x, m) == std::cmp::Ordering::Less)
                {
                    min = Some(x);
                }
                if max
                    .is_none_or(|m| crate::types::interval_cmp(x, m) == std::cmp::Ordering::Greater)
                {
                    max = Some(x);
                }
            };
            if v.has_nulls() {
                for (i, &x) in data.iter().enumerate() {
                    if v.is_valid(i) {
                        see(x);
                    }
                }
            } else {
                for &x in data {
                    see(x);
                }
            }
            ChunkColumnStats {
                null_count,
                min: min.map(|iv| Value::Interval {
                    months: iv.months,
                    days: iv.days,
                    micros: iv.micros,
                }),
                max: max.map(|iv| Value::Interval {
                    months: iv.months,
                    days: iv.days,
                    micros: iv.micros,
                }),
                nan_count: 0,
            }
        }
    }
}

/// min/max over the valid elements of an integer chunk, first-seen on ties.
fn int_stats<T: Copy + Ord>(v: &Vector, data: &[T]) -> (Option<T>, Option<T>) {
    let (mut min, mut max): (Option<T>, Option<T>) = (None, None);
    let mut see = |x: T| {
        if min.is_none_or(|m| x < m) {
            min = Some(x);
        }
        if max.is_none_or(|m| x > m) {
            max = Some(x);
        }
    };
    if v.has_nulls() {
        for (i, &x) in data.iter().enumerate() {
            if v.is_valid(i) {
                see(x);
            }
        }
    } else {
        for &x in data {
            see(x);
        }
    }
    (min, max)
}

/// min/max over the valid elements of a string chunk, first-seen on ties
/// (byte order, exactly `Value::Varchar`'s `compare_values`).
fn str_stats<'a>(v: &Vector, data: &'a [Arc<str>]) -> (Option<&'a str>, Option<&'a str>) {
    let (mut min, mut max): (Option<&'a str>, Option<&'a str>) = (None, None);
    let mut see = |x: &'a str| {
        if min.is_none_or(|m| x < m) {
            min = Some(x);
        }
        if max.is_none_or(|m| x > m) {
            max = Some(x);
        }
    };
    if v.has_nulls() {
        for (i, s) in data.iter().enumerate() {
            if v.is_valid(i) {
                see(s);
            }
        }
    } else {
        for s in data {
            see(s);
        }
    }
    (min, max)
}

fn dtype_tag(t: DataType) -> u8 {
    match t {
        DataType::Boolean => 0,
        DataType::Integer => 1,
        DataType::Bigint => 2,
        DataType::Double => 3,
        DataType::Varchar => 4,
        DataType::Date => 5,
        DataType::Timestamp => 6,
        // DECIMAL and INTERVAL are tags 7/8. DECIMAL's tag is incomplete: the
        // schema writer stores precision and scale immediately after it, and
        // the schema reader assembles the type before any chunk is read.
        DataType::Decimal { .. } => 7,
        DataType::Interval => 8,
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

    /// Signed integers use their two's-complement byte pattern (identical to
    /// the unsigned writer for the same bit width).
    fn i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    fn i64(&mut self, v: i64) {
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

    /// Bulk little-endian readers (element counts are capped by
    /// [`Reader::chunk_count`] before these are called).
    fn i32_vec(&mut self, n: usize) -> Result<Vec<i32>> {
        let bytes = self.take(n * 4)?;
        Ok(bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|c| i32::from_le_bytes(*c))
            .collect())
    }

    fn i64_vec(&mut self, n: usize) -> Result<Vec<i64>> {
        let bytes = self.take(n * 8)?;
        Ok(bytes
            .as_chunks::<8>()
            .0
            .iter()
            .map(|c| i64::from_le_bytes(*c))
            .collect())
    }

    fn f64_vec(&mut self, n: usize) -> Result<Vec<f64>> {
        let bytes = self.take(n * 8)?;
        Ok(bytes
            .as_chunks::<8>()
            .0
            .iter()
            .map(|c| f64::from_bits(u64::from_le_bytes(*c)))
            .collect())
    }
}

// ---------- value helpers ----------

/// Bits needed to store `0..=range` (at least 1; the classic bitpack rule).
fn bits_needed(range: u64) -> u32 {
    (64 - range.leading_zeros()).max(1)
}

/// Exact bits needed for `range`, with `bits_needed_exact(0) == 0` (width 0
/// means "constant", for the delta codec).
fn bits_needed_exact(range: u64) -> u32 {
    64 - range.leading_zeros()
}

/// Pack `values` little-endian bitwise, `width` bits each (LSB first).
///
/// The accumulator is `u128`: after a flush the accumulator holds fewer than
/// 8 bits, so a 64-bit value can temporarily push it to 71 bits. A `u64`
/// accumulator would silently drop the top bits for widths above 57 — the
/// pre-wave-45k bug this widening fixes (bytes are unchanged for every width
/// that fit before).
fn pack_bits(writer: &mut Writer, values: &[u64], width: u32) {
    let mut acc: u128 = 0;
    let mut acc_bits: u32 = 0;
    for &v in values {
        acc |= (v as u128) << acc_bits;
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

/// Unpack `count` little-endian bitwise values of `width` bits (LSB first).
///
/// Like [`pack_bits`], the accumulator is `u128`: loading bytes can push it
/// past 64 bits while the remainder for the next element is still valid, and
/// widths above 56 need those high bits (the pre-wave-45k `u64` accumulator
/// silently dropped them).
fn unpack_bits(reader: &mut Reader, count: usize, width: u32) -> Result<Vec<u64>> {
    let mut out = Vec::with_capacity(count);
    let mut acc: u128 = 0;
    let mut acc_bits: u32 = 0;
    let mask = if width == 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    };
    while out.len() < count {
        while acc_bits < width {
            acc |= (reader.u8()? as u128) << acc_bits;
            acc_bits += 8;
        }
        out.push((acc as u64) & mask);
        acc >>= width;
        acc_bits -= width;
    }
    Ok(out)
}

/// The validity mask as the file stores it: ceil(len/64) u64 LE words with
/// bit `i` set = row `i` valid; bits past `len` are masked off, exactly the
/// semantics of the old per-row `is_valid` loop.
///
/// A chunk with no NULLs writes zero bytes (the all-valid flag): the reader
/// treats `validity_len == 0` with `n > 0` as "every row valid", which no
/// v1–v3 writer ever emitted (they always wrote `ceil(n/64)*8 >= 8` bytes
/// for `n > 0`).
fn encode_validity(v: &Vector) -> Vec<u8> {
    if !v.has_nulls() {
        return Vec::new();
    }
    let words = v.len().div_ceil(64);
    let mask = if v.len().is_multiple_of(64) {
        u64::MAX
    } else {
        (1u64 << (v.len() % 64)) - 1
    };
    let src = v.validity_words();
    let mut out = Vec::with_capacity(words * 8);
    for w in 0..words {
        let mut word = src.get(w).copied().unwrap_or(0);
        if w + 1 == words {
            word &= mask;
        }
        out.extend_from_slice(&word.to_le_bytes());
    }
    out
}

/// Rebuild the validity words from the file bytes once `n` is known.
///
/// `bytes.is_empty() && n > 0` means "every row valid": v4's all-valid flag.
/// It is safe for v1–v3 input too — those writers always emitted
/// `ceil(n/64)*8 >= 8` bytes for `n > 0`, so no old file can carry it.
fn decode_validity(bytes: &[u8], n: usize) -> Result<Vec<u64>> {
    if n == 0 {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        return Err(PinkDbError::Execution(
            "corrupt file: trailing validity bytes".to_string(),
        ));
    }
    let words = n.div_ceil(64);
    if bytes.is_empty() {
        return Ok(vec![u64::MAX; words]);
    }
    if bytes.len() > words * 8 {
        return Err(PinkDbError::Execution(
            "corrupt file: trailing validity bytes".to_string(),
        ));
    }
    if bytes.len() < words * 8 {
        return Err(PinkDbError::Execution(
            "corrupt file: truncated validity bitmap".to_string(),
        ));
    }
    Ok(bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|c| u64::from_le_bytes(*c))
        .collect())
}

#[inline]
fn valid_at(words: &[u64], i: usize) -> bool {
    (words.get(i / 64).copied().unwrap_or(0) >> (i % 64)) & 1 == 1
}

// ---------- typed payload extraction (placeholder substitution) ----------
//
// The payload holds one value per row; NULLs keep a canonical placeholder
// (their stored bits, which `set_value(.., Null)` left in place, are NOT
// reused — this is the byte-identity contract with the old Value path).
// Substitution only happens when the chunk actually has nulls: without them
// the vector's buffer is encoded in place.

fn int32_payload(v: &Vector) -> Cow<'_, [i32]> {
    match v.data() {
        VectorData::Int32(data) | VectorData::Date32(data) => {
            if v.has_nulls() {
                let words = v.validity_words();
                Cow::Owned(
                    data.iter()
                        .enumerate()
                        .map(|(i, &x)| if valid_at(words, i) { x } else { 0 })
                        .collect(),
                )
            } else {
                Cow::Borrowed(data.as_slice())
            }
        }
        _ => unreachable!("int32_payload on a non-INTEGER vector"),
    }
}

fn int64_payload(v: &Vector) -> Cow<'_, [i64]> {
    match v.data() {
        VectorData::Int64(data) | VectorData::TimestampMicros(data) => {
            if v.has_nulls() {
                let words = v.validity_words();
                Cow::Owned(
                    data.iter()
                        .enumerate()
                        .map(|(i, &x)| if valid_at(words, i) { x } else { 0 })
                        .collect(),
                )
            } else {
                Cow::Borrowed(data.as_slice())
            }
        }
        _ => unreachable!("int64_payload on a non-BIGINT vector"),
    }
}

fn float64_payload(v: &Vector) -> Cow<'_, [f64]> {
    match v.data() {
        VectorData::Float64(data) => {
            if v.has_nulls() {
                let words = v.validity_words();
                Cow::Owned(
                    data.iter()
                        .enumerate()
                        .map(|(i, &x)| if valid_at(words, i) { x } else { 0.0 })
                        .collect(),
                )
            } else {
                Cow::Borrowed(data.as_slice())
            }
        }
        _ => unreachable!("float64_payload on a non-DOUBLE vector"),
    }
}

fn bool_payload(v: &Vector) -> Cow<'_, [bool]> {
    match v.data() {
        VectorData::Boolean(data) => {
            if v.has_nulls() {
                let words = v.validity_words();
                Cow::Owned(
                    data.iter()
                        .enumerate()
                        .map(|(i, &x)| valid_at(words, i) && x)
                        .collect(),
                )
            } else {
                Cow::Borrowed(data.as_slice())
            }
        }
        _ => unreachable!("bool_payload on a non-BOOLEAN vector"),
    }
}

fn varchar_payload(v: &Vector) -> Cow<'_, [Arc<str>]> {
    match v.data() {
        VectorData::VarChar(data) => {
            if v.has_nulls() {
                let words = v.validity_words();
                let empty: Arc<str> = Arc::from("");
                Cow::Owned(
                    data.iter()
                        .enumerate()
                        .map(|(i, s)| {
                            if valid_at(words, i) {
                                s.clone()
                            } else {
                                empty.clone()
                            }
                        })
                        .collect(),
                )
            } else {
                Cow::Borrowed(data.as_slice())
            }
        }
        _ => unreachable!("varchar_payload on a non-VARCHAR vector"),
    }
}

// ---------- chunk encode (typed) ----------

/// Storage integers: the format's two widths. One generic codec per family.
trait IntElem: Copy + Ord {
    /// Write one element in its storage width.
    fn put(w: &mut Writer, v: Self);
    /// Sign-extend to the codec domain.
    fn to_i64(self) -> i64;
}

impl IntElem for i32 {
    fn put(w: &mut Writer, v: Self) {
        w.u32(v as u32);
    }
    fn to_i64(self) -> i64 {
        self as i64
    }
}

impl IntElem for i64 {
    fn put(w: &mut Writer, v: Self) {
        w.u64(v as u64);
    }
    fn to_i64(self) -> i64 {
        self
    }
}

fn enc_ints_plain<T: IntElem>(nums: &[T]) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(nums.len() as u32);
    for &n in nums {
        T::put(&mut w, n);
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

fn enc_ints_rle<T: IntElem>(nums: &[T]) -> Vec<u8> {
    let mut w = Writer::new();
    let runs = runs_of(nums);
    w.u32(nums.len() as u32);
    w.u32(runs.len() as u32);
    for (start, len) in runs {
        T::put(&mut w, nums[start]);
        w.u32(len as u32);
    }
    w.buf
}

fn enc_ints_bitpack<T: IntElem>(nums: &[T]) -> Vec<u8> {
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
    let width = bits_needed(max.to_i64().wrapping_sub(min.to_i64()) as u64);
    w.u64(min.to_i64() as u64);
    w.u8(width.min(64) as u8);
    let shifted: Vec<u64> = nums
        .iter()
        .map(|&n| n.to_i64().wrapping_sub(min.to_i64()) as u64)
        .collect();
    pack_bits(&mut w, &shifted, width.min(64));
    w.buf
}

/// Delta + bitpack: first value as the base, then the per-chunk minimum
/// delta and a bit width over the deltas.
fn enc_ints_delta<T: IntElem>(nums: &[T]) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(nums.len() as u32);
    if nums.is_empty() {
        w.u64(0);
        return w.buf;
    }
    let first = nums[0].to_i64();
    w.u64(first as u64);
    if nums.len() >= 2 {
        let mut delta_min = i64::MAX;
        let mut delta_max = i64::MIN;
        let mut prev = first;
        for &n in &nums[1..] {
            let cur = n.to_i64();
            let d = cur.wrapping_sub(prev);
            delta_min = delta_min.min(d);
            delta_max = delta_max.max(d);
            prev = cur;
        }
        w.u64(delta_min as u64);
        // Exact width: 0 means "every delta equals delta_min" (constant
        // sequence, e.g. ids and monotone timestamps → 21-byte chunks).
        let width = bits_needed_exact(delta_max.wrapping_sub(delta_min) as u64);
        w.u8(width as u8);
        if width > 0 {
            let mut shifted = Vec::with_capacity(nums.len() - 1);
            let mut prev = first;
            for &n in &nums[1..] {
                let cur = n.to_i64();
                shifted.push(cur.wrapping_sub(prev).wrapping_sub(delta_min) as u64);
                prev = cur;
            }
            pack_bits(&mut w, &shifted, width);
        }
    }
    w.buf
}

/// 10^0 ..= 10^18, all exactly representable as f64 (used by ALP: the code
/// for row `i` is `round(v_i * POW10[e])` and the decoder divides it back).
const POW10: [f64; 19] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18,
];

/// `-2^63` (inclusive) and `2^63` (exclusive), exact as f64.
const I64_MIN_F: f64 = -9_223_372_036_854_775_808.0;
const I64_MAX_PLUS_ONE_F: f64 = 9_223_372_036_854_775_808.0;

/// The ALP code for `v` at `scale`, or `None` when the value cannot be
/// reconstructed bit-exactly as `code / scale`. `f64::round` (ties away from
/// zero) is part of the format's determinism contract.
fn alp_code(v: f64, scale: f64) -> Option<i64> {
    if !v.is_finite() {
        return None; // NaN payloads and ±INF are always exceptions
    }
    let m = v * scale;
    if !m.is_finite() || !(I64_MIN_F..I64_MAX_PLUS_ONE_F).contains(&m) {
        return None; // out of i64 range
    }
    let code = m.round() as i64;
    // What the decoder computes; -0.0 lands here (code 0 → +0.0 bits).
    let recon = (code as f64) / scale;
    if recon.to_bits() == v.to_bits() {
        Some(code)
    } else {
        None
    }
}

/// One ALP candidate for scale `e`: FOR-bitpack body + exception list.
/// Exception rows contribute code 0.
///
/// `limit` is the smallest payload length found so far (including PLAIN and
/// RLE): once the header + exception bytes alone reach it, this scale cannot
/// be strictly smaller and the scan bails (`None`) — a bound, not a
/// heuristic, so size selection is unchanged.
fn alp_body(values: &[f64], e: usize, limit: usize) -> Option<(Vec<u8>, usize)> {
    let scale = POW10[e];
    let mut codes: Vec<i64> = Vec::with_capacity(values.len());
    let mut exceptions: Vec<(u32, u64)> = Vec::new();
    for (i, &v) in values.iter().enumerate() {
        match alp_code(v, scale) {
            Some(code) => codes.push(code),
            None => {
                exceptions.push((i as u32, v.to_bits()));
                codes.push(0);
                // Header (n, e, codec, code_min, width, exception_count) +
                // at least the exception bytes.
                if 19 + 12 * exceptions.len() >= limit {
                    return None;
                }
            }
        }
    }
    let mut w = Writer::new();
    w.u32(values.len() as u32);
    w.u8(e as u8);
    w.u8(0); // int_codec: FOR bitpack (v4.0)
    if codes.is_empty() {
        w.u64(0);
        w.u8(0);
    } else {
        let min = *codes.iter().min().unwrap();
        let max = *codes.iter().max().unwrap();
        w.u64(min as u64);
        let width = bits_needed_exact(max.wrapping_sub(min) as u64);
        w.u8(width as u8);
        if width > 0 {
            let shifted: Vec<u64> = codes.iter().map(|&c| c.wrapping_sub(min) as u64).collect();
            pack_bits(&mut w, &shifted, width);
        }
    }
    w.u32(exceptions.len() as u32);
    for (i, bits) in &exceptions {
        w.u32(*i);
        w.u64(*bits);
    }
    Some((w.buf, exceptions.len()))
}

/// ALP: try every scale, keep the smallest payload (ties → smallest `e`).
/// Stops at the first `e` with no exceptions: scaling exact integer codes by
/// `10^(e'-e)` can only widen the range (or overflow into exceptions).
/// Returns `None` when every scale was ruled out by `limit`.
fn enc_alp(values: &[f64], limit: usize) -> Option<Vec<u8>> {
    let mut best: Option<(usize, Vec<u8>)> = None;
    for e in 0..=18usize {
        let Some((payload, exceptions)) = alp_body(values, e, limit) else {
            continue;
        };
        if best.as_ref().is_none_or(|(_, b)| payload.len() < b.len()) {
            best = Some((e, payload));
        }
        if exceptions == 0 {
            break;
        }
    }
    best.map(|(_, payload)| payload)
}

fn enc_u64_plain(bits: &[u64]) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(bits.len() as u32);
    for &b in bits {
        w.u64(b);
    }
    w.buf
}

fn enc_u64_rle(bits: &[u64]) -> Vec<u8> {
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

fn enc_bool_plain(bits: &[bool]) -> Vec<u8> {
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

fn enc_bool_rle(bits: &[bool]) -> Vec<u8> {
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
/// The offsets are `u32`; callers must reject a chunk whose total string bytes
/// exceed that (see [`varchar_offsets_fit`]).
fn encode_strs_offsets<T: AsRef<str>>(w: &mut Writer, strs: &[T]) {
    let mut off: u32 = 0;
    w.u32(off);
    for s in strs {
        off += s.as_ref().len() as u32;
        w.u32(off);
    }
    for s in strs {
        w.bytes(s.as_ref().as_bytes());
    }
}

/// Whether a chunk's total VARCHAR bytes fit the v4 `u32` string offsets.
fn varchar_offsets_fit(total: u64) -> bool {
    total <= u32::MAX as u64
}

/// Dictionary (first-occurrence order) + one code per row.
fn dict_and_codes<T: AsRef<str>>(strs: &[T]) -> (Vec<&str>, Vec<u32>) {
    let mut dict: Vec<&str> = Vec::new();
    let mut index: HashMap<&str, u32> = HashMap::new();
    let mut codes = Vec::with_capacity(strs.len());
    for s in strs {
        let s = s.as_ref();
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
    (dict, codes)
}

fn enc_strs_plain<T: AsRef<str>>(strs: &[T]) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(strs.len() as u32);
    encode_strs_offsets(&mut w, strs);
    w.buf
}

fn enc_strs_dict<T: AsRef<str>>(strs: &[T]) -> Vec<u8> {
    let mut w = Writer::new();
    let (dict, codes) = dict_and_codes(strs);
    w.u32(strs.len() as u32);
    w.u32(dict.len() as u32);
    encode_strs_offsets(&mut w, &dict);
    for c in codes {
        w.u32(c);
    }
    w.buf
}

/// Bitpacked dictionary codes: the same dictionary (first-occurrence order,
/// `encode_strs_offsets` bytes) with `bits_needed(dict_len - 1)`-bit codes.
fn enc_strs_dict_bitpack<T: AsRef<str> + PartialEq>(strs: &[T]) -> Vec<u8> {
    let mut w = Writer::new();
    let (dict, codes) = dict_and_codes(strs);
    w.u32(strs.len() as u32);
    w.u32(dict.len() as u32);
    encode_strs_offsets(&mut w, &dict);
    // 0 when the chunk has at most one distinct value; 1..=11 otherwise
    // (CHUNK_SIZE = 2048 distinct values need 11 bits).
    let width = if dict.len() <= 1 {
        0
    } else {
        bits_needed((dict.len() - 1) as u64)
    };
    w.u8(width as u8);
    if width > 0 {
        let shifted: Vec<u64> = codes.iter().map(|&c| c as u64).collect();
        pack_bits(&mut w, &shifted, width);
    }
    w.buf
}

fn enc_strs_rle<T: AsRef<str> + PartialEq>(strs: &[T]) -> Vec<u8> {
    let mut w = Writer::new();
    let runs = runs_of(strs);
    w.u32(strs.len() as u32);
    w.u32(runs.len() as u32);
    let vals: Vec<&str> = runs.iter().map(|&(s, _)| strs[s].as_ref()).collect();
    encode_strs_offsets(&mut w, &vals);
    for &(_, len) in &runs {
        w.u32(len as u32);
    }
    w.buf
}

/// Keep the smaller payload; ties keep the earlier candidate (candidate
/// order is part of the format contract).
fn consider_best(best: &mut Option<(u8, Vec<u8>)>, tag: u8, payload: Vec<u8>) {
    match best {
        Some((_, b)) if b.len() <= payload.len() => {}
        _ => *best = Some((tag, payload)),
    }
}

// ---------- LZ frame (tag 8) ----------
//
// A hand-rolled LZ77 in the spirit of LZ4: greedy longest-match over a
// 64 KiB window with a 4-byte hash table; tokens are a control byte per
// literal run / match, then the raw bytes / a u16 offset.
//
//   payload = u32 raw_len | u8 base_tag | u32 token_len | tokens
//
// `raw_len` is the untouched length of the inner payload; decompression must
// land exactly on it, and the inner payload (decoded from the decompressed
// bytes) must consume them exactly. The frame is only ever emitted when it
// is strictly smaller than the base payload.

/// Shortest match worth a token (3 bytes + control overhead).
const LZ_MIN_MATCH: usize = 4;
/// Longest match a single token can encode: `len - 3` in 7 bits.
const LZ_MAX_MATCH: usize = 130;
/// Longest literal run a single token can encode: `len - 1` in 7 bits.
const LZ_MAX_LITERAL: usize = 128;
/// Back-reference window = the u16 offset range.
const LZ_WINDOW: usize = u16::MAX as usize;
/// Hash table entries for the full encode pass (2^16).
const LZ_HASH_BITS: u32 = 16;
/// Hash table entries for the prefix probe (2^12, stack-allocated).
const LZ_PROBE_HASH_BITS: u32 = 12;
/// Bytes of the string data region the probe compresses.
const LZ_PROBE_BYTES: usize = 4096;
/// Hard cap on the inner payload size of a frame (decode safety: no
/// allocation is sized from an unvalidated length).
const LZ_MAX_RAW: usize = 1 << 24;
/// LZ is only tried when the best existing candidate is at least this big:
/// below it the frame header plus one token cannot win, so the scan would be
/// pure cost (sales: the VARCHAR dictionary wins at ~300 B/chunk and never
/// triggers a probe, keeping the 1M/10M save time at baseline).
const LZ_MIN_ATTEMPT: usize = 1024;

#[inline]
fn lz_hash4(a: &[u8], bits: u32) -> usize {
    let v = u32::from_le_bytes([a[0], a[1], a[2], a[3]]);
    (v.wrapping_mul(0x9E37_79B1) >> (32 - bits)) as usize
}

/// Greedy LZ77 into a fresh token stream. `table` is scratch (any length);
/// it is cleared first. Deterministic: fixed hash, last-occurrence matches,
/// no randomness.
fn lz_compress_into(data: &[u8], table: &mut [u32]) -> Vec<u8> {
    debug_assert!(table.len().is_power_of_two(), "LZ scratch must be 2^n");
    table.fill(u32::MAX);
    let n = data.len();
    let mut out = Vec::with_capacity(n / 2 + 16);
    let bits = table.len().trailing_zeros();
    let mut lit_start = 0usize;
    let mut i = 0usize;
    while i + LZ_MIN_MATCH <= n {
        let h = lz_hash4(&data[i..i + 4], bits);
        let cand = table[h];
        table[h] = i as u32;
        let mut matched = false;
        if cand != u32::MAX {
            let p = cand as usize;
            if i - p <= LZ_WINDOW && data[p..p + LZ_MIN_MATCH] == data[i..i + LZ_MIN_MATCH] {
                let mut len = LZ_MIN_MATCH;
                while len < LZ_MAX_MATCH && i + len < n && data[p + len] == data[i + len] {
                    len += 1;
                }
                // Flush the pending literals, then the match token.
                let mut l = lit_start;
                while l < i {
                    let take = (i - l).min(LZ_MAX_LITERAL);
                    out.push(((take - 1) as u8) << 1);
                    out.extend_from_slice(&data[l..l + take]);
                    l += take;
                }
                out.push((((len - 3) as u8) << 1) | 1);
                out.extend_from_slice(&((i - p) as u16).to_le_bytes());
                // Index every position the match skipped so later matches can
                // still find it (each position is inserted at most once).
                let end = i + len;
                let mut k = i + 1;
                while k < end && k + LZ_MIN_MATCH <= n {
                    table[lz_hash4(&data[k..k + 4], bits)] = k as u32;
                    k += 1;
                }
                i = end;
                lit_start = i;
                matched = true;
            }
        }
        if !matched {
            i += 1;
        }
    }
    let mut l = lit_start;
    while l < n {
        let take = (n - l).min(LZ_MAX_LITERAL);
        out.push(((take - 1) as u8) << 1);
        out.extend_from_slice(&data[l..l + take]);
        l += take;
    }
    out
}

fn lz_compress(data: &[u8]) -> Vec<u8> {
    let mut table = vec![u32::MAX; 1 << LZ_HASH_BITS];
    lz_compress_into(data, &mut table)
}

/// Compressibility probe for the LZ candidate: compresses the first
/// [`LZ_PROBE_BYTES`] of the string data region with a small table and
/// reports whether it saved at least 10%. Bounded cost (4 KiB scan + a 16 KiB
/// stack table) regardless of payload size; a false negative only costs
/// bytes (the frame is merely a candidate), never correctness.
///
/// The probe deliberately looks at the string bytes, not the offsets block:
/// offsets measured as incompressible by this token format (4-byte matches
/// cost more in control bytes than they save), so a payload whose repeats
/// live only in its offsets — random strings — must not pay for a full LZ
/// scan.
fn lz_probe(data: &[u8]) -> bool {
    let n = data.len().min(LZ_PROBE_BYTES);
    if n < 512 {
        return false;
    }
    let mut table = [u32::MAX; 1 << LZ_PROBE_HASH_BITS];
    let compressed = lz_compress_into(&data[..n], &mut table);
    compressed.len() * 10 < n * 9
}

/// LZ frame around `base` (the base payload), or `None` when it does not
/// strictly shrink it. Caller-side gating (probe, size) is separate so the
/// decode path can accept any valid base tag.
fn enc_lz_frame(base_tag: u8, base: &[u8]) -> Option<Vec<u8>> {
    if base.len() > LZ_MAX_RAW {
        return None;
    }
    let tokens = lz_compress(base);
    // 4 (raw_len) + 1 (base_tag) + 4 (token_len).
    if tokens.len() + 9 >= base.len() {
        return None;
    }
    let mut w = Writer::new();
    w.u32(base.len() as u32);
    w.u8(base_tag);
    w.u32(tokens.len() as u32);
    w.bytes(&tokens);
    Some(w.buf)
}

/// Expand an LZ token stream into exactly `raw_len` bytes. Every read is
/// bounds-checked; malformed streams (a claimed length larger than the
/// tokens, a zero or out-of-range back-reference, output over-run, leftover
/// tokens) are clear errors.
fn lz_decompress(tokens: &[u8], raw_len: usize) -> Result<Vec<u8>> {
    let corrupt = |what: &str| PinkDbError::Execution(format!("corrupt file: {what}"));
    // Never size an allocation from the declared length alone; grow as the
    // tokens are consumed. `raw_len` is capped by the caller.
    let mut out: Vec<u8> = Vec::with_capacity(raw_len.min(1 << 16));
    let mut pos = 0usize;
    while out.len() < raw_len {
        let ctrl = *tokens
            .get(pos)
            .ok_or_else(|| corrupt("truncated LZ token stream"))?;
        pos += 1;
        if ctrl & 1 == 0 {
            let len = usize::from(ctrl >> 1) + 1;
            if out.len() + len > raw_len {
                return Err(corrupt("LZ literal run overruns the frame length"));
            }
            let end = pos + len;
            let bytes = tokens
                .get(pos..end)
                .ok_or_else(|| corrupt("truncated LZ literal run"))?;
            out.extend_from_slice(bytes);
            pos = end;
        } else {
            let len = usize::from(ctrl >> 1) + 3;
            let off_bytes = tokens
                .get(pos..pos + 2)
                .ok_or_else(|| corrupt("truncated LZ match"))?;
            let off = usize::from(u16::from_le_bytes([off_bytes[0], off_bytes[1]]));
            pos += 2;
            if off == 0 || off > out.len() {
                return Err(corrupt("LZ back-reference out of range"));
            }
            if out.len() + len > raw_len {
                return Err(corrupt("LZ match overruns the frame length"));
            }
            // Overlapping copy (off < len) must read byte by byte.
            let start = out.len() - off;
            for k in 0..len {
                out.push(out[start + k]);
            }
        }
    }
    if pos != tokens.len() {
        return Err(corrupt("trailing bytes in LZ token stream"));
    }
    Ok(out)
}

/// Encode one column chunk straight from the [`Vector`]: returns
/// (encoding tag, payload bytes). Candidate order is part of the format
/// contract; ties keep the earlier candidate.
fn encode_chunk_typed(v: &Vector) -> (u8, Vec<u8>) {
    let dtype = v.dtype();
    let mut best: Option<(u8, Vec<u8>)> = None;
    match dtype {
        DataType::Integer | DataType::Date => {
            let nums = int32_payload(v);
            consider_best(&mut best, ENC_PLAIN, enc_ints_plain(&nums));
            consider_best(&mut best, ENC_RLE, enc_ints_rle(&nums));
            consider_best(&mut best, ENC_BITPACK, enc_ints_bitpack(&nums));
            consider_best(&mut best, ENC_DELTA_BITPACK, enc_ints_delta(&nums));
        }
        // Bigint and timestamp share the 64-bit codecs; the dtype tag on the
        // chunk keeps them distinct on decode.
        DataType::Bigint | DataType::Timestamp => {
            let nums = int64_payload(v);
            consider_best(&mut best, ENC_PLAIN, enc_ints_plain(&nums));
            consider_best(&mut best, ENC_RLE, enc_ints_rle(&nums));
            consider_best(&mut best, ENC_BITPACK, enc_ints_bitpack(&nums));
            consider_best(&mut best, ENC_DELTA_BITPACK, enc_ints_delta(&nums));
        }
        // 128-bit fixed-point DECIMAL: `u32 row count | raw unscaled values`
        // (the schema carries precision/scale on disk). Invalid rows are
        // zeroed so the bytes stay deterministic; validity wins on decode.
        DataType::Decimal { .. } => {
            let VectorData::Decimal { values, .. } = v.data() else {
                unreachable!("decimal chunk has a non-decimal payload")
            };
            let nulls = v.has_nulls();
            let mut w = Writer::new();
            w.u32(values.len() as u32);
            for (i, &x) in values.iter().enumerate() {
                let x = if nulls && !v.is_valid(i) { 0 } else { x };
                w.bytes(&x.to_le_bytes());
            }
            best = Some((ENC_PLAIN, w.buf));
        }
        // INTERVAL: `u32 row count | months(i32) days(i32) micros(i64)` each.
        DataType::Interval => {
            let VectorData::Interval(values) = v.data() else {
                unreachable!("interval chunk has a non-interval payload")
            };
            let nulls = v.has_nulls();
            let mut w = Writer::new();
            w.u32(values.len() as u32);
            for (i, iv) in values.iter().enumerate() {
                let (months, days, micros) = if nulls && !v.is_valid(i) {
                    (0i32, 0i32, 0i64)
                } else {
                    (iv.months, iv.days, iv.micros)
                };
                w.bytes(&months.to_le_bytes());
                w.bytes(&days.to_le_bytes());
                w.bytes(&micros.to_le_bytes());
            }
            best = Some((ENC_PLAIN, w.buf));
        }
        DataType::Double => {
            let values = float64_payload(v);
            let bits: Vec<u64> = values.iter().map(|x| x.to_bits()).collect();
            consider_best(&mut best, ENC_PLAIN, enc_u64_plain(&bits));
            consider_best(&mut best, ENC_RLE, enc_u64_rle(&bits));
            // ALP is the expensive candidate: pass the best payload so far
            // so a scale that cannot win bails out early.
            let limit = best.as_ref().map_or(usize::MAX, |(_, b)| b.len());
            if let Some(alp) = enc_alp(&values, limit) {
                consider_best(&mut best, ENC_ALP, alp);
            }
        }
        DataType::Boolean => {
            let bits = bool_payload(v);
            consider_best(&mut best, ENC_PLAIN, enc_bool_plain(&bits));
            consider_best(&mut best, ENC_RLE, enc_bool_rle(&bits));
        }
        DataType::Varchar => {
            let strs = varchar_payload(v);
            let plain = enc_strs_plain(&strs);
            let dict = enc_strs_dict(&strs);
            let rle = enc_strs_rle(&strs);
            let dict_bp = enc_strs_dict_bitpack(&strs);
            // LZ (the last candidate) wraps the plain layout. It is only
            // tried when the best structural candidate is still big enough
            // to have room for a win (`LZ_MIN_ATTEMPT`) and a bounded prefix
            // probe says the string bytes repeat: sales' low-cardinality
            // VARCHAR chunks win with a ~300-byte bitpacked dictionary and
            // never pay for a scan. The frame is only kept when strictly
            // smaller, and ties keep the earlier candidate, so files that
            // never win with LZ stay byte-identical to the pre-tag-8 writer.
            let lz = {
                let best_len = plain
                    .len()
                    .min(dict.len())
                    .min(rle.len())
                    .min(dict_bp.len());
                let data_start = 4 + (strs.len() + 1) * 4;
                if best_len >= LZ_MIN_ATTEMPT && lz_probe(&plain[data_start..]) {
                    enc_lz_frame(ENC_PLAIN, &plain)
                } else {
                    None
                }
            };
            consider_best(&mut best, ENC_PLAIN, plain);
            consider_best(&mut best, ENC_DICT, dict);
            consider_best(&mut best, ENC_RLE, rle);
            consider_best(&mut best, ENC_DICT_BITPACK, dict_bp);
            if let Some(frame) = lz {
                consider_best(&mut best, ENC_LZ_FRAME, frame);
            }
        }
    }
    best.expect("at least plain encoding")
}

// ---------- chunk decode (typed) ----------

/// Reads `count+1` offsets then the data block; returns one shared `Arc<str>`
/// per entry (dictionary entries are shared by every row that uses them).
fn decode_strs(reader: &mut Reader, count: usize) -> Result<Vec<Arc<str>>> {
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
        let s = std::str::from_utf8(&data[a..b])
            .map_err(|_| PinkDbError::Execution("corrupt file: bad UTF-8".to_string()))?;
        out.push(Arc::from(s));
    }
    Ok(out)
}

/// Decode payload codes for the two integer widths.
fn decode_rle_i32(r: &mut Reader, n: usize, runs: usize) -> Result<Vec<i32>> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..runs {
        let v = r.i32()?;
        let len = r.u32()? as usize;
        if out.len() + len > n {
            return r.err("RLE run overflows chunk length");
        }
        out.extend(std::iter::repeat_n(v, len));
    }
    if out.len() != n {
        return r.err("RLE runs underfill chunk length");
    }
    Ok(out)
}

fn decode_rle_i64(r: &mut Reader, n: usize, runs: usize) -> Result<Vec<i64>> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..runs {
        let v = r.i64()?;
        let len = r.u32()? as usize;
        if out.len() + len > n {
            return r.err("RLE run overflows chunk length");
        }
        out.extend(std::iter::repeat_n(v, len));
    }
    if out.len() != n {
        return r.err("RLE runs underfill chunk length");
    }
    Ok(out)
}

fn decode_rle_f64(r: &mut Reader, n: usize, runs: usize) -> Result<Vec<f64>> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..runs {
        let v = r.f64()?;
        let len = r.u32()? as usize;
        if out.len() + len > n {
            return r.err("RLE run overflows chunk length");
        }
        out.extend(std::iter::repeat_n(v, len));
    }
    if out.len() != n {
        return r.err("RLE runs underfill chunk length");
    }
    Ok(out)
}

fn decode_rle_bool(r: &mut Reader, n: usize, runs: usize) -> Result<Vec<bool>> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..runs {
        let v = r.u8()? != 0;
        let len = r.u32()? as usize;
        if out.len() + len > n {
            return r.err("RLE run overflows chunk length");
        }
        out.extend(std::iter::repeat_n(v, len));
    }
    if out.len() != n {
        return r.err("RLE runs underfill chunk length");
    }
    Ok(out)
}

fn decode_rle_strs(r: &mut Reader, n: usize, runs: usize) -> Result<Vec<Arc<str>>> {
    let vals = decode_strs(r, runs)?;
    let mut out = Vec::with_capacity(n);
    for v in vals {
        let len = r.u32()? as usize;
        if out.len() + len > n {
            return r.err("RLE run overflows chunk length");
        }
        out.extend(std::iter::repeat_n(v, len));
    }
    if out.len() != n {
        return r.err("RLE runs underfill chunk length");
    }
    Ok(out)
}

/// Delta+bitpack body: `base` was already read; the remaining values are
/// `base + cumsum(delta)`. All arithmetic wraps (bit-exact for i32/i64
/// extremes and deltas that overflow i64).
fn decode_delta(r: &mut Reader, n: usize, base: i64) -> Result<Vec<i64>> {
    let mut out = Vec::with_capacity(n);
    if n == 0 {
        return Ok(out);
    }
    out.push(base);
    if n >= 2 {
        let delta_min = r.i64()?;
        let width = r.u8()? as u32;
        if width > 64 {
            return r.err("bad delta width");
        }
        let shifted = unpack_bits(r, n - 1, width)?;
        let mut x = base;
        for s in shifted {
            let d = if width == 0 {
                delta_min
            } else {
                s.wrapping_add(delta_min as u64) as i64
            };
            x = x.wrapping_add(d);
            out.push(x);
        }
    }
    Ok(out)
}

/// The error for a tag that has no meaning on this dtype (or is reserved).
fn tag_error(dtype: DataType, tag: u8, v4: bool) -> PinkDbError {
    let reserved = matches!(tag, ENC_DICT_RLE | ENC_FSST) || tag > ENC_FSST;
    if v4 && reserved {
        PinkDbError::Execution(format!("unsupported v4 encoding tag {tag}; upgrade pinkdb"))
    } else {
        PinkDbError::Execution(format!("corrupt file: encoding {tag} invalid for {dtype}"))
    }
}

/// Decode one chunk payload into a typed buffer + its row count.
fn decode_payload(
    dtype: DataType,
    tag: u8,
    r: &mut Reader,
    v4: bool,
) -> Result<(VectorData, usize)> {
    match (dtype, tag) {
        (DataType::Integer, ENC_PLAIN) => {
            let n = r.chunk_count("chunk length")?;
            Ok((VectorData::Int32(r.i32_vec(n)?), n))
        }
        (DataType::Date, ENC_PLAIN) => {
            let n = r.chunk_count("chunk length")?;
            Ok((VectorData::Date32(r.i32_vec(n)?), n))
        }
        (DataType::Bigint, ENC_PLAIN) => {
            let n = r.chunk_count("chunk length")?;
            Ok((VectorData::Int64(r.i64_vec(n)?), n))
        }
        (DataType::Timestamp, ENC_PLAIN) => {
            let n = r.chunk_count("chunk length")?;
            Ok((VectorData::TimestampMicros(r.i64_vec(n)?), n))
        }
        (DataType::Decimal { precision, scale }, ENC_PLAIN) => {
            let n = r.chunk_count("chunk length")?;
            let mut values = Vec::with_capacity(n);
            for _ in 0..n {
                let bytes = r.take(16)?;
                let mut buf = [0u8; 16];
                buf.copy_from_slice(bytes);
                values.push(i128::from_le_bytes(buf));
            }
            Ok((
                VectorData::Decimal {
                    values,
                    precision,
                    scale,
                },
                n,
            ))
        }
        (DataType::Interval, ENC_PLAIN) => {
            let n = r.chunk_count("chunk length")?;
            let mut values = Vec::with_capacity(n);
            for _ in 0..n {
                let months = r.i32()?;
                let days = r.i32()?;
                let micros = r.i64()?;
                values.push(Interval {
                    months,
                    days,
                    micros,
                });
            }
            Ok((VectorData::Interval(values), n))
        }
        (DataType::Integer, ENC_RLE) | (DataType::Date, ENC_RLE) => {
            let n = r.chunk_count("chunk length")?;
            let runs = r.chunk_count("run count")?;
            let nums = decode_rle_i32(r, n, runs)?;
            if dtype == DataType::Date {
                Ok((VectorData::Date32(nums), n))
            } else {
                Ok((VectorData::Int32(nums), n))
            }
        }
        (DataType::Bigint, ENC_RLE) | (DataType::Timestamp, ENC_RLE) => {
            let n = r.chunk_count("chunk length")?;
            let runs = r.chunk_count("run count")?;
            let nums = decode_rle_i64(r, n, runs)?;
            if dtype == DataType::Timestamp {
                Ok((VectorData::TimestampMicros(nums), n))
            } else {
                Ok((VectorData::Int64(nums), n))
            }
        }
        (DataType::Integer, ENC_BITPACK) | (DataType::Date, ENC_BITPACK) => {
            let n = r.chunk_count("chunk length")?;
            let min = r.i64()?;
            let width = r.u8()? as u32;
            if width > 64 {
                return r.err("bad bitpack width");
            }
            let shifted = unpack_bits(r, n, width)?;
            let nums: Vec<i32> = shifted
                .into_iter()
                .map(|s| min.wrapping_add(s as i64) as i32)
                .collect();
            if dtype == DataType::Date {
                Ok((VectorData::Date32(nums), n))
            } else {
                Ok((VectorData::Int32(nums), n))
            }
        }
        (DataType::Bigint, ENC_BITPACK) | (DataType::Timestamp, ENC_BITPACK) => {
            let n = r.chunk_count("chunk length")?;
            let min = r.i64()?;
            let width = r.u8()? as u32;
            if width > 64 {
                return r.err("bad bitpack width");
            }
            let shifted = unpack_bits(r, n, width)?;
            let nums: Vec<i64> = shifted
                .into_iter()
                .map(|s| min.wrapping_add(s as i64))
                .collect();
            if dtype == DataType::Timestamp {
                Ok((VectorData::TimestampMicros(nums), n))
            } else {
                Ok((VectorData::Int64(nums), n))
            }
        }
        (DataType::Integer, ENC_DELTA_BITPACK) | (DataType::Date, ENC_DELTA_BITPACK) => {
            let n = r.chunk_count("chunk length")?;
            let base = r.i64()?;
            let nums: Vec<i32> = decode_delta(r, n, base)?
                .into_iter()
                .map(|x| x as i32)
                .collect();
            if dtype == DataType::Date {
                Ok((VectorData::Date32(nums), n))
            } else {
                Ok((VectorData::Int32(nums), n))
            }
        }
        (DataType::Bigint, ENC_DELTA_BITPACK) | (DataType::Timestamp, ENC_DELTA_BITPACK) => {
            let n = r.chunk_count("chunk length")?;
            let base = r.i64()?;
            let nums = decode_delta(r, n, base)?;
            if dtype == DataType::Timestamp {
                Ok((VectorData::TimestampMicros(nums), n))
            } else {
                Ok((VectorData::Int64(nums), n))
            }
        }
        (DataType::Double, ENC_ALP) => {
            let n = r.chunk_count("chunk length")?;
            let e = r.u8()? as usize;
            if e > 18 {
                return r.err("bad ALP exponent");
            }
            let int_codec = r.u8()?;
            if int_codec != 0 {
                // int_codec 1 (a delta body) is reserved and not defined yet.
                return r.err("unsupported ALP integer codec");
            }
            let code_min = r.i64()?;
            let width = r.u8()? as u32;
            if width > 64 {
                return r.err("bad ALP code width");
            }
            let scale = POW10[e];
            let mut out: Vec<f64> = unpack_bits(r, n, width)?
                .into_iter()
                .map(|s| code_min.wrapping_add(s as i64) as f64 / scale)
                .collect();
            let exceptions = r.chunk_count("exception count")?;
            if exceptions > n {
                return r.err("exception count exceeds chunk length");
            }
            let mut prev: Option<u32> = None;
            for _ in 0..exceptions {
                let row = r.u32()?;
                let bits = r.u64()?;
                if row as usize >= n {
                    return r.err("ALP exception row out of range");
                }
                if prev.is_some_and(|p| row <= p) {
                    return r.err("ALP exception rows not strictly increasing");
                }
                prev = Some(row);
                out[row as usize] = f64::from_bits(bits);
            }
            Ok((VectorData::Float64(out), n))
        }
        (DataType::Double, ENC_PLAIN) => {
            let n = r.chunk_count("chunk length")?;
            Ok((VectorData::Float64(r.f64_vec(n)?), n))
        }
        (DataType::Double, ENC_RLE) => {
            let n = r.chunk_count("chunk length")?;
            let runs = r.chunk_count("run count")?;
            Ok((VectorData::Float64(decode_rle_f64(r, n, runs)?), n))
        }
        (DataType::Boolean, ENC_PLAIN) => {
            let n = r.chunk_count("chunk length")?;
            let need = n.div_ceil(8);
            let bytes = r.take(need)?;
            let out = (0..n).map(|i| (bytes[i / 8] >> (i % 8)) & 1 == 1).collect();
            Ok((VectorData::Boolean(out), n))
        }
        (DataType::Boolean, ENC_RLE) => {
            let n = r.chunk_count("chunk length")?;
            let runs = r.chunk_count("run count")?;
            Ok((VectorData::Boolean(decode_rle_bool(r, n, runs)?), n))
        }
        (DataType::Varchar, ENC_PLAIN) => {
            let n = r.chunk_count("chunk length")?;
            Ok((VectorData::VarChar(decode_strs(r, n)?), n))
        }
        (DataType::Varchar, ENC_DICT) => {
            let n = r.chunk_count("chunk length")?;
            let dict_len = r.chunk_count("dictionary size")?;
            let dict = decode_strs(r, dict_len)?;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                let code = r.u32()? as usize;
                out.push(dict.get(code).cloned().ok_or_else(|| {
                    PinkDbError::Execution("corrupt file: bad dict code".to_string())
                })?);
            }
            Ok((VectorData::VarChar(out), n))
        }
        (DataType::Varchar, ENC_DICT_BITPACK) => {
            let n = r.chunk_count("chunk length")?;
            let dict_len = r.chunk_count("dictionary size")?;
            let dict = decode_strs(r, dict_len)?;
            let width = r.u8()? as u32;
            // Strict equality: a corrupt width must not desynchronize the
            // bitstream (it is derived from dict_len by the writer).
            let expected = if dict_len <= 1 {
                0
            } else {
                bits_needed((dict_len - 1) as u64)
            };
            if width != expected {
                return r.err("bad dictionary bit width");
            }
            let mut out = Vec::with_capacity(n);
            if width == 0 {
                // At most one dictionary entry; every row (if any) is it.
                let v = dict.first().cloned();
                for _ in 0..n {
                    out.push(v.clone().ok_or_else(|| {
                        PinkDbError::Execution("corrupt file: bad dict code".to_string())
                    })?);
                }
            } else {
                for c in unpack_bits(r, n, width)? {
                    out.push(dict.get(c as usize).cloned().ok_or_else(|| {
                        PinkDbError::Execution("corrupt file: bad dict code".to_string())
                    })?);
                }
            }
            Ok((VectorData::VarChar(out), n))
        }
        (DataType::Varchar, ENC_RLE) => {
            let n = r.chunk_count("chunk length")?;
            let runs = r.chunk_count("run count")?;
            Ok((VectorData::VarChar(decode_rle_strs(r, n, runs)?), n))
        }
        (dtype, ENC_LZ_FRAME) => decode_lz_frame(dtype, r, v4),
        _ => Err(tag_error(dtype, tag, v4)),
    }
}

/// Decode an LZ frame (tag 8): `u32 raw_len | u8 base_tag | u32 token_len |
/// tokens`. The decompressed bytes are the inner payload, which must be
/// valid for the chunk's dtype and must consume the frame exactly.
///
/// `base_tag` is restricted to the payload tags (0..=6): the reserved and
/// frame tags (7..=9 and above) are refused so a frame cannot nest or
/// smuggle an unimplemented codec.
fn decode_lz_frame(dtype: DataType, r: &mut Reader, v4: bool) -> Result<(VectorData, usize)> {
    let raw_len = r.u32()? as usize;
    let base_tag = r.u8()?;
    let token_len = r.u32()? as usize;
    if !(4..=LZ_MAX_RAW).contains(&raw_len) {
        return r.err("bad LZ frame length");
    }
    if base_tag > ENC_ALP {
        return r.err("unsupported LZ base encoding");
    }
    let tokens = r.take(token_len)?;
    let raw = lz_decompress(tokens, raw_len)?;
    let mut inner = Reader::new(&raw);
    let (data, n) = decode_payload(dtype, base_tag, &mut inner, v4)?;
    if !inner.end() {
        return Err(PinkDbError::Execution(
            "corrupt file: trailing bytes in LZ frame".to_string(),
        ));
    }
    Ok((data, n))
}

/// Decode one framed chunk (validity + payload) into a [`Vector`].
fn decode_chunk_typed(dtype: DataType, tag: u8, r: &mut Reader, v4: bool) -> Result<Vector> {
    // Validity comes first (the encoder writes it before the payload); the
    // payload decode is what knows the row count.
    let vlen = r.u32()? as usize;
    let vbytes = r.take(vlen)?;
    let (data, n) = decode_payload(dtype, tag, r, v4)?;
    let validity = decode_validity(vbytes, n)?;
    Ok(Vector::from_raw(data, validity))
}

// ---------- public API ----------

/// One encoded column chunk, ready to append in order.
struct EncodedCol {
    tag: u8,
    /// Serialized validity words (or empty for the all-valid flag, §format).
    validity: Vec<u8>,
    payload: Vec<u8>,
}

/// Encode every column of one chunk (raw payloads, no framing).
fn encode_columns(cols: &[Arc<Vector>], schema: &[(String, DataType)]) -> Vec<EncodedCol> {
    debug_assert_eq!(cols.len(), schema.len(), "chunk/schema column mismatch");
    cols.iter()
        .zip(schema)
        .map(|(v, (_, dtype))| {
            debug_assert_eq!(v.dtype(), *dtype, "chunk/schema dtype mismatch");
            let (tag, payload) = encode_chunk_typed(v);
            EncodedCol {
                tag,
                validity: encode_validity(v),
                payload,
            }
        })
        .collect()
}

/// One chunk encoded, plus its per-column statistics when the caller asked
/// for them (the `.lake` stats block). `collect_stats` is constant per save,
/// so the branch is free in the common `.save` path.
fn encode_chunk_with_stats(
    cols: &[Arc<Vector>],
    schema: &[(String, DataType)],
    collect_stats: bool,
) -> (Vec<EncodedCol>, Vec<ChunkColumnStats>) {
    let encoded = encode_columns(cols, schema);
    let stats = if collect_stats {
        cols.iter().map(|v| chunk_column_stats(v)).collect()
    } else {
        Vec::new()
    };
    (encoded, stats)
}

/// Exact per-column statistics of one table's rows, collected without
/// encoding anything (wave 66).
///
/// `pink-lake` uses this to publish a **Vortex** data file's zone map in the
/// catalog: the same [`SavedTableStats`] facts a `.pink` encode collects
/// during its pass, computed here by one linear scan of the table's chunk
/// payloads (no per-row `Value` materialization except the per-chunk
/// min/max). The facts are ungated (`nan_count` and zero doubles are kept);
/// `pink-lake` applies the one exactness gate shared with the Vortex footer
/// path, so the two formats cannot drift apart.
pub fn table_stats(db: &str, table: &str, src: &DataTable) -> SavedTableStats {
    let schema = src.schema();
    let mut accs: Vec<ColumnStatsAcc> = schema.iter().map(|_| ColumnStatsAcc::new()).collect();
    for c in 0..src.num_chunks() {
        if let Some(chunk) = src.scan_chunk(c) {
            for (acc, v) in accs.iter_mut().zip(chunk.columns.iter()) {
                acc.merge(&chunk_column_stats(v));
            }
        }
    }
    SavedTableStats {
        db: db.to_string(),
        table: table.to_string(),
        rows: src.num_rows() as u64,
        columns: schema
            .iter()
            .zip(accs)
            .map(|((col, dtype), acc)| SavedColumnStats {
                name: col.clone(),
                dtype: *dtype,
                null_count: acc.null_count,
                min: acc.min,
                max: acc.max,
                nan_count: acc.nan_count,
            })
            .collect(),
    }
}

impl PinkDb {
    /// Serialize every database (and every table in it) in the `.pink`
    /// columnar format. Databases and tables are stored in sorted name order
    /// (deterministic bytes). Views are session-local and never persisted.
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.to_bytes_inner(true, false)?.0)
    }

    /// [`PinkDb::to_bytes`] with the chunk-parallel encode disabled (test
    /// switch: both paths must produce identical bytes).
    #[cfg(test)]
    fn to_bytes_impl(&self, parallel: bool) -> Result<Vec<u8>> {
        Ok(self.to_bytes_inner(parallel, false)?.0)
    }

    /// The one serialization path: `.pink` v4 bytes, optionally with the
    /// exact per-table statistics collected in the same chunk pass. The
    /// statistics never influence the bytes (`to_bytes_inner(_, false).0` is
    /// bit-identical to `to_bytes_inner(_, true).0`).
    fn to_bytes_inner(
        &self,
        parallel: bool,
        collect_stats: bool,
    ) -> Result<(Vec<u8>, Vec<SavedTableStats>)> {
        let mut w = Writer::new();
        w.bytes(MAGIC_V5);
        // The constraint and column-default sections exist iff some table
        // needs them (the flags word is written before the tables, so decide
        // it up front).
        let with_constraints = self.list_databases().iter().any(|db| {
            self.list_tables_in(db).iter().any(|t| {
                self.table_in(db, t)
                    .is_ok_and(|t| !t.constraints.is_empty())
            })
        });
        let with_defaults = self.list_databases().iter().any(|db| {
            self.list_tables_in(db).iter().any(|t| {
                self.table_in(db, t)
                    .is_ok_and(|t| t.columns.iter().any(|c| c.default.is_some()))
            })
        });
        let with_mvs = self.has_materialized_views();
        let with_procedures = self.has_procedures();
        let with_rbac = self.has_rbac_data();
        w.u32(
            if with_constraints {
                FLAG_CONSTRAINTS
            } else {
                0
            } | if with_defaults { FLAG_DEFAULTS } else { 0 }
                | if with_mvs { FLAG_MATERIALIZED_VIEWS } else { 0 }
                | if with_procedures { FLAG_PROCEDURES } else { 0 }
                | if with_rbac { FLAG_RBAC } else { 0 },
        );
        let mut saved: Vec<SavedTableStats> = Vec::new();
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
                    // DECIMAL is the one tag whose parameters ride along.
                    if let DataType::Decimal { precision, scale } = dtype {
                        w.u8(*precision);
                        w.u8(*scale);
                    }
                }
                if with_constraints {
                    write_constraints(&mut w, &t.constraints);
                }
                if with_defaults {
                    write_defaults(&mut w, &t.columns);
                }
                w.u64(t.num_rows() as u64);
                let n_chunks = t.num_chunks();
                // Share the chunk vectors first (Arc clones only), then
                // encode per chunk. Order-preserving collect: bytes are
                // identical to a serial pass.
                let mut scans: Vec<Vec<Arc<Vector>>> = Vec::with_capacity(n_chunks);
                for chunk_idx in 0..n_chunks {
                    let chunk = t.scan_chunk(chunk_idx).ok_or_else(|| {
                        PinkDbError::Execution("storage changed during save".to_string())
                    })?;
                    scans.push(chunk.columns);
                }
                // The v4 string layout stores offsets as `u32`; a chunk whose
                // total VARCHAR bytes exceed that cannot be encoded faithfully
                // (the offsets would wrap and the reader would reject the file).
                // Refuse before writing instead of emitting a corrupt file.
                for cols in &scans {
                    for v in cols {
                        if let VectorData::VarChar(strs) = v.data() {
                            let total: u64 = strs.iter().map(|s| s.len() as u64).sum();
                            if !varchar_offsets_fit(total) {
                                return Err(PinkDbError::Execution(format!(
                                    "table \"{dbname}.{name}\" has a VARCHAR chunk of {total} \
                                     bytes, over the .pink format's u32 string-offset limit"
                                )));
                            }
                        }
                    }
                }
                let per_chunk: Vec<(Vec<EncodedCol>, Vec<ChunkColumnStats>)> =
                    if parallel && n_chunks >= PARALLEL_SAVE_MIN_CHUNKS {
                        scans
                            .par_iter()
                            .map(|cols| encode_chunk_with_stats(cols, &schema, collect_stats))
                            .collect()
                    } else {
                        scans
                            .iter()
                            .map(|cols| encode_chunk_with_stats(cols, &schema, collect_stats))
                            .collect()
                    };
                let (encoded, chunk_stats): (Vec<_>, Vec<_>) = per_chunk.into_iter().unzip();
                if collect_stats {
                    let mut accs: Vec<ColumnStatsAcc> =
                        schema.iter().map(|_| ColumnStatsAcc::new()).collect();
                    for chunk in &chunk_stats {
                        for (acc, stats) in accs.iter_mut().zip(chunk) {
                            acc.merge(stats);
                        }
                    }
                    saved.push(SavedTableStats {
                        db: dbname.clone(),
                        table: name.clone(),
                        rows: t.num_rows() as u64,
                        columns: schema
                            .iter()
                            .zip(accs)
                            .map(|((col, dtype), acc)| SavedColumnStats {
                                name: col.clone(),
                                dtype: *dtype,
                                null_count: acc.null_count,
                                min: acc.min,
                                max: acc.max,
                                nan_count: acc.nan_count,
                            })
                            .collect(),
                    });
                }
                for ci in 0..schema.len() {
                    w.u32(n_chunks as u32);
                    for chunk in &encoded {
                        let col = &chunk[ci];
                        w.u8(col.tag);
                        w.u32(col.validity.len() as u32);
                        w.bytes(&col.validity);
                        w.bytes(&col.payload);
                    }
                }
            }
            if with_mvs {
                write_materialized_views(&mut w, self, dbname);
            }
            if with_procedures {
                write_procedures(&mut w, self, dbname);
            }
        }
        // The store-global RBAC section rides after every database (users,
        // roles and grants are not per-database).
        if with_rbac {
            write_rbac(&mut w, self);
        }
        let crc = crate::wal::crc32(&w.buf);
        w.u32(crc);
        Ok((w.buf, saved))
    }

    /// The bytes [`PinkDb::save`] would write: every database in the `.pink`
    /// columnar format, databases and tables in sorted name order
    /// (deterministic bytes), views never persisted.
    ///
    /// For writers that need the serialized image in memory instead of a
    /// file on disk (pink-lake's v1 `.pink` data files). The same fail-stop
    /// check as [`PinkDb::save`] applies: a store holding a non-durable write
    /// is refused.
    pub fn to_pink_bytes(&self) -> Result<Vec<u8>> {
        // Never serialize a store holding a non-durable write (fail-stop).
        // (Checkpoints check the same flag under the WAL lock they hold.)
        self.ensure_wal_healthy()?;
        self.to_bytes()
    }

    /// [`PinkDb::to_pink_bytes`] plus the exact per-column statistics of
    /// every table, collected in the same chunk pass as the encode — the
    /// bytes are bit-identical to [`PinkDb::to_pink_bytes`] and the added
    /// cost is one linear scan of the chunk payloads (no second walk of the
    /// store, no per-row `Value` materialization except the per-chunk
    /// min/max).
    ///
    /// For the `.lake` layer: a `pinklake` save publishes these
    /// facts in the v1 manifest's statistics block, and `.lake attach`
    /// answers exact `COUNT`/`MIN`/`MAX` from them without decoding a data
    /// file. The facts are ungated (`nan_count` and zero doubles are kept);
    /// `pink-lake` applies the one exactness gate shared with the Vortex
    /// footer path. [`PinkDb::save`]/[`PinkDb::to_pink_bytes`] never pay
    /// this cost.
    pub fn to_pink_bytes_with_stats(&self) -> Result<(Vec<u8>, Vec<SavedTableStats>)> {
        self.ensure_wal_healthy()?;
        self.to_bytes_inner(true, true)
    }

    /// Write every database to `path` in the `.pink` columnar format.
    /// Databases and tables are stored in sorted name order (deterministic
    /// bytes). Views are session-local and never persisted.
    ///
    /// Always writes plaintext; use [`PinkDb::save_with_key`] (feature
    /// `encryption`) for an encrypted container. The write is atomic (a temp
    /// sibling renamed over the target), and it refuses inside a transaction or
    /// over a target whose sidecar WAL still holds committed records.
    pub fn save(&self, path: &str) -> Result<()> {
        self.ensure_wal_healthy()?;
        self.guard_snapshot_target(path)?;
        let bytes = self.to_pink_bytes()?;
        Self::write_atomic(std::path::Path::new(path), &bytes)
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
            self.guard_snapshot_target(path)?;
            let sealed = crate::crypto::seal_container(&self.to_bytes()?, key, 0)?;
            Self::write_atomic(std::path::Path::new(path), &sealed)
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

    /// Write `bytes` to `path` atomically: a unique temp sibling is written and
    /// fsynced, then renamed over the target, then the parent directory is
    /// fsynced. An interrupted write leaves the previous target intact; a
    /// failure removes the temp file.
    fn write_atomic(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
        use std::io::Write;
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(format!(".tmp-{}-{seq}", std::process::id()));
        let tmp = std::path::PathBuf::from(tmp);
        let write = (|| -> Result<()> {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)?;
            f.write_all(bytes)?;
            f.sync_all()?;
            Ok(())
        })();
        if let Err(e) = write {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        if let Err(e) = std::fs::rename(&tmp, path) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e.into());
        }
        crate::wal::sync_parent_dir(path);
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

    /// Decode a serialized `.pink` image (v1, v2, v3, v4 or v5 magic).
    fn load_bytes(data: &[u8]) -> Result<PinkDb> {
        let data = if data.starts_with(MAGIC_V3)
            || data.starts_with(MAGIC_V4)
            || data.starts_with(MAGIC_V5)
        {
            // Verify the trailer before decoding anything.
            if data.len() < MAGIC_V5.len() + 4 {
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
        let (v2, flags, v5) = if magic == MAGIC_V5 {
            (true, true, true)
        } else if magic == MAGIC_V4 {
            (true, true, false)
        } else if magic == MAGIC_V3 || magic == MAGIC_V2 {
            (true, false, false)
        } else if magic == MAGIC {
            (false, false, false)
        } else if magic.starts_with(b"PINKDB0") && magic[7] > b'5' && magic[7].is_ascii_digit() {
            return Err(PinkDbError::Execution(format!(
                "written by a newer pinkdb (format v{}); upgrade pinkdb",
                magic[7] - b'0'
            )));
        } else {
            return Err(PinkDbError::Execution(
                "not a .pink file (bad magic)".to_string(),
            ));
        };
        let mut constraints = false;
        let mut defaults = false;
        let mut mvs = false;
        let mut procedures = false;
        let mut rbac = false;
        if flags {
            let f = r.u32()?;
            if v5 {
                if f & !(FLAG_CONSTRAINTS
                    | FLAG_DEFAULTS
                    | FLAG_MATERIALIZED_VIEWS
                    | FLAG_PROCEDURES
                    | FLAG_RBAC)
                    != 0
                {
                    return Err(PinkDbError::Execution(format!(
                        "unsupported .pink v5 flags 0x{f:08x}; upgrade pinkdb"
                    )));
                }
                constraints = f & FLAG_CONSTRAINTS != 0;
                defaults = f & FLAG_DEFAULTS != 0;
                mvs = f & FLAG_MATERIALIZED_VIEWS != 0;
                procedures = f & FLAG_PROCEDURES != 0;
                rbac = f & FLAG_RBAC != 0;
            } else if f != 0 {
                return Err(PinkDbError::Execution(format!(
                    "unsupported .pink v4 flags 0x{f:08x}; upgrade pinkdb"
                )));
            }
        }
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
                    load_table(&mut r, &mut db, &dbname, v5, constraints, defaults)?;
                }
                if v5 && mvs {
                    load_materialized_views(&mut r, &mut db, &dbname)?;
                }
                if v5 && procedures {
                    load_procedures(&mut r, &mut db, &dbname)?;
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
                load_table(&mut r, &mut db, "main", false, false, false)?;
            }
        }
        if v5 && rbac {
            load_rbac(&mut r, &mut db)?;
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

/// Encode one table's declarative constraints (see the module-level layout).
/// Keys are written first (declaration order), then foreign keys, then the
/// NOT NULL column list — the cross-category order is not observable, so it is
/// not preserved.
fn write_constraints(w: &mut Writer, c: &TableConstraints) {
    w.u32((c.keys.len() + c.foreign_keys.len() + c.not_null.len()) as u32);
    for k in &c.keys {
        w.u8(match k.kind {
            KeyKind::Primary => CK_PRIMARY_KEY,
            KeyKind::Unique => CK_UNIQUE,
        });
        w.str(k.name.as_deref().unwrap_or(""));
        w.u32(k.columns.len() as u32);
        for col in &k.columns {
            w.u32(*col as u32);
        }
    }
    for fk in &c.foreign_keys {
        w.u8(CK_FOREIGN_KEY);
        w.str(fk.name.as_deref().unwrap_or(""));
        w.u32(fk.columns.len() as u32);
        for col in &fk.columns {
            w.u32(*col as u32);
        }
        w.str(&fk.ref_db);
        w.str(&fk.ref_table);
        w.u32(fk.ref_columns.len() as u32);
        for col in &fk.ref_columns {
            w.u32(*col as u32);
        }
    }
    for col in &c.not_null {
        w.u8(CK_NOT_NULL);
        w.str("");
        w.u32(1);
        w.u32(*col as u32);
    }
}

/// Policy tags in a materialized-view section (wave 80).
const MV_MANUAL: u8 = 0;
const MV_EVERY: u8 = 1;
const MV_ON_COMMIT: u8 = 2;

/// One database's materialized-view section (`FLAG_MATERIALIZED_VIEWS`): the
/// count, then per view, sorted by lowercased name,
/// `name, policy tag [, months, days, micros], defining SQL, last refresh
/// stamp (epoch ms), refresh count`. The **bound query is not stored**: only
/// the defining SQL text, which the loader rebinds (a bound plan cannot be
/// rendered back to SQL, and a definition whose base tables are missing must
/// still load). A non-refreshable definition is not a write error, so this is
/// infallible.
fn write_materialized_views(w: &mut Writer, db: &PinkDb, dbname: &str) {
    let views = db.materialized_views_in(dbname);
    w.u32(views.len() as u32);
    for (_, def) in &views {
        w.str(&def.name);
        match def.policy {
            crate::types::RefreshPolicy::Manual => w.u8(MV_MANUAL),
            crate::types::RefreshPolicy::Every { interval } => {
                w.u8(MV_EVERY);
                w.i32(interval.months);
                w.i32(interval.days);
                w.i64(interval.micros);
            }
            crate::types::RefreshPolicy::OnCommit => w.u8(MV_ON_COMMIT),
        }
        w.str(&def.query_sql);
        w.i64(def.last_refreshed_ms);
        w.u64(def.refresh_count);
    }
}

/// Decode one database's materialized-view section and install the
/// definitions. The defining query is rebound from its persisted SQL; a
/// definition that cannot rebind (missing base table/view) still loads, with
/// no bound plan — a refresh retries later and reports the failure, and
/// `DROP MATERIALIZED VIEW` still removes it.
fn load_materialized_views(r: &mut Reader, db: &mut PinkDb, dbname: &str) -> Result<()> {
    let n = r.u32()? as usize;
    // Each view costs at least 9 bytes (empty policy/name/… fields); do not
    // size an allocation from the unvalidated count.
    if n > r.remaining() / 9 + 1 {
        return r.err("materialized-view count exceeds the remaining file");
    }
    for _ in 0..n {
        let name = r.str()?;
        let policy = match r.u8()? {
            MV_MANUAL => crate::types::RefreshPolicy::Manual,
            MV_EVERY => {
                let months = r.i32()?;
                let days = r.i32()?;
                let micros = r.i64()?;
                crate::types::RefreshPolicy::Every {
                    interval: crate::types::Interval {
                        months,
                        days,
                        micros,
                    },
                }
            }
            MV_ON_COMMIT => crate::types::RefreshPolicy::OnCommit,
            other => return r.err(&format!("unknown materialized-view policy tag {other}")),
        };
        let query_sql = r.str()?;
        let last_refreshed_ms = r.i64()?;
        let refresh_count = r.u64()?;
        let query = crate::mv::rebind_query(db, dbname, &query_sql).ok();
        db.materialized_views_insert_in(
            dbname,
            name.to_ascii_lowercase(),
            crate::storage::MaterializedViewDef {
                name,
                query,
                query_sql,
                policy,
                last_refreshed_ms,
                // The attempt stamp is not persisted: treat the last success
                // as the last attempt (an `EVERY` view becomes due one full
                // interval after its last success).
                last_attempt_ms: last_refreshed_ms,
                refresh_count,
                last_error: None,
            },
        )?;
    }
    Ok(())
}

/// The store-global RBAC section (`FLAG_RBAC`, wave 48): the users (display
/// name + Argon2id PHC password hash), the roles, the role memberships, the
/// table grants, the database grants and the global `ADMIN` holders, each
/// list sorted by its canonical key.
///
/// Only the **hash** of a password is ever written — the DDL statement's
/// plaintext exists in process memory and nowhere else (the WAL logs the
/// rewritten `PASSWORD_HASH '<phc>'` form for the same reason). `BTreeMap`
/// iteration is sorted by key, which is exactly the deterministic write
/// order, so no explicit sort is needed.
fn write_rbac(w: &mut Writer, db: &PinkDb) {
    let rbac = db.rbac();
    w.u32(rbac.users.len() as u32);
    for user in rbac.users.values() {
        w.str(&user.name);
        w.str(&user.password_hash);
    }
    w.u32(rbac.roles.len() as u32);
    for role in rbac.roles.values() {
        w.str(role);
    }
    let memberships: usize = rbac.memberships.values().map(|r| r.len()).sum();
    w.u32(memberships as u32);
    for (member, roles) in &rbac.memberships {
        for role in roles {
            w.str(member);
            w.str(role);
        }
    }
    w.u32(rbac.table_grants.len() as u32);
    for ((dbname, table), principals) in &rbac.table_grants {
        w.str(dbname);
        w.str(table);
        write_principal_grants(w, principals);
    }
    w.u32(rbac.database_grants.len() as u32);
    for (dbname, principals) in &rbac.database_grants {
        w.str(dbname);
        write_principal_grants(w, principals);
    }
    w.u32(rbac.admins.len() as u32);
    for principal in &rbac.admins {
        w.str(principal);
    }
}

/// One object's `principal → privileges` map: the count, then per principal,
/// `principal, privilege count, privileges`.
fn write_principal_grants(
    w: &mut Writer,
    principals: &std::collections::BTreeMap<
        String,
        std::collections::BTreeSet<crate::rbac::Privilege>,
    >,
) {
    w.u32(principals.len() as u32);
    for (principal, privs) in principals {
        w.str(principal);
        w.u32(privs.len() as u32);
        for p in privs {
            w.u8(p.tag());
        }
    }
}

/// Decode the store-global RBAC section and install it.
///
/// Every count is validated against the bytes remaining before it sizes an
/// allocation (exactly like the other optional sections), unknown privilege
/// tags are a corrupt-file error, and a user's password hash must parse as a
/// PHC `argon2id` string — a store that loaded would otherwise hold a user
/// nobody can log in as. Grants that name a principal or table the catalog no
/// longer defines load as-is (they are inert): the file records what was
/// granted, not what exists today.
fn load_rbac(r: &mut Reader, db: &mut PinkDb) -> Result<()> {
    let mut rbac = crate::rbac::RbacCatalog::default();
    let n_users = r.u32()? as usize;
    // Each user costs at least 8 bytes (two empty strings' length prefixes).
    if n_users > r.remaining() / 8 + 1 {
        return r.err("RBAC user count exceeds the remaining file");
    }
    for _ in 0..n_users {
        let name = r.str()?;
        let password_hash = r.str()?;
        crate::rbac::validate_password_hash(&password_hash).map_err(|e| {
            PinkDbError::Execution(format!(
                "corrupt file: user \"{name}\" has an invalid password hash ({e})"
            ))
        })?;
        rbac.users.insert(
            name.to_ascii_lowercase(),
            crate::rbac::UserDef {
                name,
                password_hash,
            },
        );
    }
    let n_roles = r.u32()? as usize;
    if n_roles > r.remaining() / 4 + 1 {
        return r.err("RBAC role count exceeds the remaining file");
    }
    for _ in 0..n_roles {
        let name = r.str()?;
        rbac.roles.insert(name.to_ascii_lowercase(), name);
    }
    let n_memberships = r.u32()? as usize;
    if n_memberships > r.remaining() / 8 + 1 {
        return r.err("RBAC membership count exceeds the remaining file");
    }
    for _ in 0..n_memberships {
        let member = r.str()?;
        let role = r.str()?;
        rbac.memberships
            .entry(member.to_ascii_lowercase())
            .or_default()
            .insert(role.to_ascii_lowercase());
    }
    let n_table_grants = r.u32()? as usize;
    if n_table_grants > r.remaining() / 8 + 1 {
        return r.err("RBAC table-grant count exceeds the remaining file");
    }
    for _ in 0..n_table_grants {
        let dbname = r.str()?;
        let table = r.str()?;
        let principals = read_principal_grants(r)?;
        rbac.table_grants.insert(
            (dbname.to_ascii_lowercase(), table.to_ascii_lowercase()),
            principals,
        );
    }
    let n_db_grants = r.u32()? as usize;
    if n_db_grants > r.remaining() / 4 + 1 {
        return r.err("RBAC database-grant count exceeds the remaining file");
    }
    for _ in 0..n_db_grants {
        let dbname = r.str()?;
        let principals = read_principal_grants(r)?;
        rbac.database_grants
            .insert(dbname.to_ascii_lowercase(), principals);
    }
    let n_admins = r.u32()? as usize;
    if n_admins > r.remaining() / 4 + 1 {
        return r.err("RBAC admin count exceeds the remaining file");
    }
    for _ in 0..n_admins {
        let principal = r.str()?;
        rbac.admins.insert(principal.to_ascii_lowercase());
    }
    *db.rbac_mut() = rbac;
    Ok(())
}

/// One object's `principal → privileges` map (the reader side of
/// [`write_principal_grants`]).
fn read_principal_grants(
    r: &mut Reader,
) -> Result<std::collections::BTreeMap<String, std::collections::BTreeSet<crate::rbac::Privilege>>>
{
    let n = r.u32()? as usize;
    // Each principal costs at least 8 bytes (name and privilege count).
    if n > r.remaining() / 8 + 1 {
        return r.err("RBAC grant principal count exceeds the remaining file");
    }
    let mut out = std::collections::BTreeMap::new();
    for _ in 0..n {
        let principal = r.str()?;
        let n_privs = r.u32()? as usize;
        if n_privs > r.remaining() + 1 {
            return r.err("RBAC privilege count exceeds the remaining file");
        }
        let mut privs = std::collections::BTreeSet::new();
        for _ in 0..n_privs {
            privs.insert(crate::rbac::Privilege::from_tag(r.u8()?)?);
        }
        out.insert(principal.to_ascii_lowercase(), privs);
    }
    Ok(out)
}

/// One database's stored-procedure section (`FLAG_PROCEDURES`, wave 44): the
/// count, then per procedure, sorted by lowercased name,
/// `name, language tag, parameter count × (name, dtype tag [, precision,
/// scale]), body text`. The **parsed body is not stored**: only the text,
/// which the loader re-parses (a body is validated at `CREATE`, and a stored
/// body that no longer parses is a corrupt file). JavaScript bodies are opaque
/// text and load in every build — running one still needs the `js` feature, so
/// a build without it reports the refusal at `CALL` time.
fn write_procedures(w: &mut Writer, db: &PinkDb, dbname: &str) {
    let procs = db.procedures_in(dbname);
    w.u32(procs.len() as u32);
    for (_, def) in &procs {
        w.str(&def.name);
        w.u8(def.language.tag());
        w.u32(def.params.len() as u32);
        for (name, dtype) in &def.params {
            w.str(name);
            w.u8(dtype_tag(*dtype));
            // DECIMAL rides its precision/scale, exactly like the schema.
            if let DataType::Decimal { precision, scale } = dtype {
                w.u8(*precision);
                w.u8(*scale);
            }
        }
        w.str(&def.source);
    }
}

/// Decode one database's stored-procedure section and install the
/// definitions. A SQL body is re-parsed here; a parse failure is a corrupt
/// file (the body was validated when the definition was created).
fn load_procedures(r: &mut Reader, db: &mut PinkDb, dbname: &str) -> Result<()> {
    let n = r.u32()? as usize;
    // Each procedure costs at least 7 bytes (language, empty counts/strings);
    // do not size an allocation from the unvalidated count.
    if n > r.remaining() / 7 + 1 {
        return r.err("procedure count exceeds the remaining file");
    }
    for _ in 0..n {
        let name = r.str()?;
        let language = crate::procedure::ProcLanguage::from_tag(r.u8()?)?;
        let n_params = r.u32()? as usize;
        if n_params > r.remaining() / 5 + 1 {
            return r.err("procedure parameter count exceeds the remaining file");
        }
        let mut params = Vec::with_capacity(n_params);
        for _ in 0..n_params {
            let pname = r.str()?;
            let tag = r.u8()?;
            let dtype = if tag == 7 {
                let precision = r.u8()?;
                let scale = r.u8()?;
                DataType::Decimal { precision, scale }
            } else {
                dtype_from_tag(tag)?
            };
            params.push((pname, dtype));
        }
        let source = r.str()?;
        let body = match language {
            crate::procedure::ProcLanguage::Sql => {
                let param_names: Vec<String> = params.iter().map(|(n, _)| n.clone()).collect();
                Some(
                    crate::procedure::parse_body(&name, &param_names, &source).map_err(|e| {
                        PinkDbError::Execution(format!(
                            "corrupt file: procedure \"{name}\" (database \"{dbname}\") \
                             does not parse: {e}"
                        ))
                    })?,
                )
            }
            crate::procedure::ProcLanguage::JavaScript => None,
        };
        db.procedures_insert_in(
            dbname,
            name.to_ascii_lowercase(),
            crate::procedure::ProcedureDef {
                name,
                params,
                language,
                source,
                body,
            },
        )?;
    }
    Ok(())
}

/// Encode every column's DEFAULT (wave 65): one presence byte per column,
/// then the value bytes of the ones that have a default (a NULL default is
/// simply absent — it means the same on read).
fn write_defaults(w: &mut Writer, columns: &[Column]) {
    for col in columns {
        match &col.default {
            Some(v) if !v.is_null() => {
                w.u8(1);
                write_default_value(w, v);
            }
            _ => w.u8(0),
        }
    }
}

/// One default value's bytes: a tag byte plus its payload.
fn write_default_value(w: &mut Writer, v: &Value) {
    match v {
        // Unreachable: `write_defaults` treats a NULL default as absent. A
        // total match keeps the encoder honest.
        Value::Null => w.u8(VD_BOOL),
        Value::Boolean(b) => {
            w.u8(VD_BOOL);
            w.u8(*b as u8);
        }
        Value::Integer(i) => {
            w.u8(VD_I32);
            w.u32(*i as u32);
        }
        Value::Bigint(i) => {
            w.u8(VD_I64);
            w.u64(*i as u64);
        }
        Value::Double(x) => {
            w.u8(VD_F64_BITS);
            w.u64(x.to_bits());
        }
        Value::Varchar(s) => {
            w.u8(VD_VARCHAR);
            w.str(s);
        }
        Value::Date(d) => {
            w.u8(VD_DATE);
            w.u32(*d as u32);
        }
        Value::Timestamp(t) => {
            w.u8(VD_TIMESTAMP);
            w.u64(*t as u64);
        }
        Value::Decimal(x, scale) => {
            w.u8(VD_DECIMAL);
            w.bytes(&x.to_le_bytes());
            w.u8(*scale);
        }
        Value::Interval {
            months,
            days,
            micros,
        } => {
            w.u8(VD_INTERVAL);
            w.u32(*months as u32);
            w.u32(*days as u32);
            w.u64(*micros as u64);
        }
    }
}

/// Decode the per-column default section (`FLAG_DEFAULTS`). Every tag is
/// validated; an unknown tag or a truncated payload is a corrupt-file error.
fn read_defaults(r: &mut Reader, n_cols: usize) -> Result<Vec<Option<Value>>> {
    let mut out = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        if r.u8()? == 0 {
            out.push(None);
            continue;
        }
        let tag = r.u8()?;
        let v = match tag {
            VD_BOOL => Value::Boolean(match r.u8()? {
                0 => false,
                1 => true,
                _ => return r.err("bad BOOLEAN default byte"),
            }),
            VD_I32 => Value::Integer(r.i32()?),
            VD_I64 => Value::Bigint(r.i64()?),
            VD_F64_BITS => Value::Double(f64::from_bits(r.u64()?)),
            VD_VARCHAR => Value::Varchar(r.str()?),
            VD_DATE => Value::Date(r.i32()?),
            VD_TIMESTAMP => Value::Timestamp(r.i64()?),
            VD_DECIMAL => {
                let b = r.take(16)?;
                let mut arr = [0u8; 16];
                arr.copy_from_slice(b);
                let x = i128::from_le_bytes(arr);
                let scale = r.u8()?;
                if scale > 38 {
                    return r.err("bad DECIMAL default scale");
                }
                Value::Decimal(x, scale)
            }
            VD_INTERVAL => Value::Interval {
                months: r.i32()?,
                days: r.i32()?,
                micros: r.i64()?,
            },
            other => {
                return Err(PinkDbError::Execution(format!(
                    "corrupt file: unknown default value tag {other}"
                )));
            }
        };
        out.push(Some(v));
    }
    Ok(out)
}

/// Whether a decoded default value has exactly its column's type (a tampered
/// file cannot smuggle a mistyped default into later INSERTs).
fn default_matches(v: &Value, dtype: DataType) -> bool {
    match (v, dtype) {
        (Value::Boolean(_), DataType::Boolean)
        | (Value::Integer(_), DataType::Integer)
        | (Value::Bigint(_), DataType::Bigint)
        | (Value::Double(_), DataType::Double)
        | (Value::Varchar(_), DataType::Varchar)
        | (Value::Date(_), DataType::Date)
        | (Value::Timestamp(_), DataType::Timestamp)
        | (Value::Interval { .. }, DataType::Interval) => true,
        (Value::Decimal(_, s), DataType::Decimal { scale, .. }) => *s == scale,
        _ => false,
    }
}

/// Decode one table's constraint section from `r` (v5 files with flags bit 0
/// set). `n_cols` bounds every column index; almost-empty constraints and
/// out-of-range indices are corrupt-file errors, never panics.
fn read_constraints(r: &mut Reader, n_cols: usize) -> Result<TableConstraints> {
    let count = r.u32()? as usize;
    // The smallest constraint costs 13 bytes (kind + empty name + one
    // column); never size an allocation from the unvalidated count.
    if count > r.remaining() / 13 + 1 {
        return r.err("constraint count exceeds the remaining file");
    }
    let mut out = TableConstraints::default();
    for _ in 0..count {
        let kind = r.u8()?;
        let name = r.str()?;
        let name = (!name.is_empty()).then_some(name);
        let n = r.u32()? as usize;
        if n > r.remaining() / 4 + 1 {
            return r.err("constraint column count exceeds the remaining file");
        }
        let mut cols = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            let c = r.u32()? as usize;
            if c >= n_cols {
                return r.err("constraint column index out of range");
            }
            cols.push(c);
        }
        match kind {
            CK_PRIMARY_KEY | CK_UNIQUE => {
                if cols.is_empty() {
                    return r.err("key constraint with no columns");
                }
                out.keys.push(KeyConstraint {
                    name,
                    kind: if kind == CK_PRIMARY_KEY {
                        KeyKind::Primary
                    } else {
                        KeyKind::Unique
                    },
                    columns: cols,
                });
            }
            CK_FOREIGN_KEY => {
                if cols.is_empty() {
                    return r.err("foreign key with no columns");
                }
                let ref_db = r.str()?;
                let ref_table = r.str()?;
                if ref_db.is_empty() || ref_table.is_empty() {
                    return r.err("foreign key with an empty reference");
                }
                let rn = r.u32()? as usize;
                if rn > r.remaining() / 4 + 1 {
                    return r.err("foreign key reference count exceeds the remaining file");
                }
                if rn != cols.len() {
                    return r.err("foreign key referencing/referenced column count mismatch");
                }
                let mut ref_cols = Vec::with_capacity(rn.min(1024));
                for _ in 0..rn {
                    ref_cols.push(r.u32()? as usize);
                }
                out.foreign_keys.push(ForeignKeyConstraint {
                    name,
                    columns: cols,
                    ref_db: ref_db.to_ascii_lowercase(),
                    ref_table: ref_table.to_ascii_lowercase(),
                    ref_columns: ref_cols,
                });
            }
            CK_NOT_NULL => {
                if cols.len() != 1 {
                    return r.err("NOT NULL constraint must name exactly one column");
                }
                out.not_null.push(cols[0]);
            }
            other => {
                return r.err(&format!("unknown constraint kind {other}"));
            }
        }
    }
    Ok(out)
}

/// Decode one table from `r` into `db`'s `dbname` database. `v4` selects the
/// v4-only error messages (reserved tags, unknown flags).
fn load_table(
    r: &mut Reader,
    db: &mut PinkDb,
    dbname: &str,
    v4: bool,
    constraints: bool,
    defaults: bool,
) -> Result<()> {
    let name = r.str()?;
    let n_cols = r.u32()? as usize;
    // Each column costs at least 5 bytes (name length + dtype tag).
    if n_cols > r.remaining() / 5 {
        return r.err("column count exceeds the remaining file");
    }
    let mut schema = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        let col = r.str()?;
        let tag = r.u8()?;
        let dtype = match tag {
            7 => {
                let precision = r.u8()?;
                let scale = r.u8()?;
                crate::types::decimal_type(precision, scale).ok_or_else(|| {
                    PinkDbError::Execution("corrupt file: bad DECIMAL precision/scale".to_string())
                })?
            }
            8 => DataType::Interval,
            other => dtype_from_tag(other)?,
        };
        schema.push((col, dtype));
    }
    let constraints = if constraints {
        read_constraints(r, n_cols)?
    } else {
        TableConstraints::default()
    };
    let defaults = if defaults {
        let decoded = read_defaults(r, n_cols)?;
        for ((name, dtype), d) in schema.iter().zip(decoded.iter()) {
            if let Some(v) = d
                && !default_matches(v, *dtype)
            {
                return r.err(&format!(
                    "default value of column \"{name}\" does not match its type"
                ));
            }
        }
        decoded
    } else {
        vec![None; n_cols]
    };
    let num_rows = r.u64()? as usize;
    // Decode each column's chunks into shared vectors (no per-row Value
    // materialization, no transposition).
    let mut columns: Vec<Vec<Arc<Vector>>> = Vec::with_capacity(n_cols);
    for (ci, (_, dtype)) in schema.iter().enumerate() {
        let n_chunks = r.u32()? as usize;
        // Each chunk costs at least 9 bytes (tag + validity length); do not
        // size an allocation from the unvalidated count.
        let mut chunks: Vec<Arc<Vector>> = Vec::with_capacity(n_chunks.min(r.remaining() / 9 + 1));
        let mut decoded = 0usize;
        for _ in 0..n_chunks {
            let tag = r.u8()?;
            let v = decode_chunk_typed(*dtype, tag, r, v4)?;
            decoded += v.len();
            chunks.push(Arc::new(v));
        }
        if decoded != num_rows {
            return Err(PinkDbError::Execution(format!(
                "corrupt file: column {ci} has {decoded} values, want {num_rows}"
            )));
        }
        columns.push(chunks);
    }
    // All columns must share chunk boundaries (the writer's invariant);
    // a file whose columns disagree is refused instead of row-merged.
    let n_chunks = columns.first().map(|c| c.len()).unwrap_or(0);
    for (ci, col) in columns.iter().enumerate() {
        if col.len() != n_chunks {
            return Err(PinkDbError::Execution(format!(
                "corrupt file: column {ci} has {} chunks, want {n_chunks}",
                col.len()
            )));
        }
    }
    db.create_table_with_constraints_in(dbname, &name, schema, constraints)?;
    let t = db.table_mut_in(dbname, &name)?;
    for (col, d) in t.columns.iter_mut().zip(defaults) {
        col.default = d;
    }
    for ci in 0..n_chunks {
        let chunk: Vec<Arc<Vector>> = columns.iter().map(|c| Arc::clone(&c[ci])).collect();
        t.append_shared_chunk(&chunk)?;
    }
    Ok(())
}

/// Build a test [`Vector`] from [`Value`]s (NULLs become placeholders +
/// cleared validity, exactly like `push_value`).
#[cfg(test)]
fn vector_from_values(dtype: DataType, values: &[Value]) -> Vector {
    let mut v = Vector::new_empty(dtype);
    for val in values {
        v.push_value(val.clone()).unwrap();
    }
    v
}

/// Re-export for tests: chunk encode from [`Value`]s (byte-identical to the
/// typed path by construction).
#[cfg(test)]
fn encode_chunk(dtype: DataType, values: &[Value]) -> (u8, Vec<u8>) {
    encode_chunk_typed(&vector_from_values(dtype, values))
}

/// The PLAIN payload of a vector (test helper: forced-tag LZ round trips).
#[cfg(test)]
fn plain_payload(v: &Vector) -> Vec<u8> {
    match v.dtype() {
        DataType::Integer | DataType::Date => enc_ints_plain(&int32_payload(v)),
        DataType::Bigint | DataType::Timestamp => enc_ints_plain(&int64_payload(v)),
        DataType::Double => {
            let values = float64_payload(v);
            let bits: Vec<u64> = values.iter().map(|x| x.to_bits()).collect();
            enc_u64_plain(&bits)
        }
        DataType::Boolean => enc_bool_plain(&bool_payload(v)),
        DataType::Varchar => enc_strs_plain(&varchar_payload(v)),
        DataType::Decimal { .. } => {
            let VectorData::Decimal { values, .. } = v.data() else {
                unreachable!()
            };
            let nulls = v.has_nulls();
            let mut w = Writer::new();
            w.u32(values.len() as u32);
            for (i, &x) in values.iter().enumerate() {
                let x = if nulls && !v.is_valid(i) { 0 } else { x };
                w.bytes(&x.to_le_bytes());
            }
            w.buf
        }
        DataType::Interval => {
            let VectorData::Interval(values) = v.data() else {
                unreachable!()
            };
            let nulls = v.has_nulls();
            let mut w = Writer::new();
            w.u32(values.len() as u32);
            for (i, iv) in values.iter().enumerate() {
                let (m, d, us) = if nulls && !v.is_valid(i) {
                    (0i32, 0i32, 0i64)
                } else {
                    (iv.months, iv.days, iv.micros)
                };
                w.bytes(&m.to_le_bytes());
                w.bytes(&d.to_le_bytes());
                w.bytes(&us.to_le_bytes());
            }
            w.buf
        }
    }
}

/// Re-export for tests: chunk encode/decode round trip with a forced tag.
#[cfg(test)]
pub(crate) fn roundtrip_chunk(dtype: DataType, values: Vec<Value>, tag: u8) -> Vec<Value> {
    let v = vector_from_values(dtype, &values);
    let payload = match (dtype, tag) {
        (DataType::Integer | DataType::Date, ENC_PLAIN) => {
            if dtype == DataType::Date {
                match v.data() {
                    VectorData::Date32(d) => enc_ints_plain(d.as_slice()),
                    _ => unreachable!(),
                }
            } else {
                enc_ints_plain(&int32_payload(&v))
            }
        }
        (DataType::Bigint | DataType::Timestamp, ENC_PLAIN) => {
            if dtype == DataType::Timestamp {
                match v.data() {
                    VectorData::TimestampMicros(d) => enc_ints_plain(d.as_slice()),
                    _ => unreachable!(),
                }
            } else {
                enc_ints_plain(&int64_payload(&v))
            }
        }
        (DataType::Integer | DataType::Date, ENC_RLE) => {
            if dtype == DataType::Date {
                match v.data() {
                    VectorData::Date32(d) => enc_ints_rle(d.as_slice()),
                    _ => unreachable!(),
                }
            } else {
                enc_ints_rle(&int32_payload(&v))
            }
        }
        (DataType::Bigint | DataType::Timestamp, ENC_RLE) => {
            if dtype == DataType::Timestamp {
                match v.data() {
                    VectorData::TimestampMicros(d) => enc_ints_rle(d.as_slice()),
                    _ => unreachable!(),
                }
            } else {
                enc_ints_rle(&int64_payload(&v))
            }
        }
        (DataType::Integer | DataType::Date, ENC_BITPACK) => {
            if dtype == DataType::Date {
                match v.data() {
                    VectorData::Date32(d) => enc_ints_bitpack(d.as_slice()),
                    _ => unreachable!(),
                }
            } else {
                enc_ints_bitpack(&int32_payload(&v))
            }
        }
        (DataType::Bigint | DataType::Timestamp, ENC_BITPACK) => {
            if dtype == DataType::Timestamp {
                match v.data() {
                    VectorData::TimestampMicros(d) => enc_ints_bitpack(d.as_slice()),
                    _ => unreachable!(),
                }
            } else {
                enc_ints_bitpack(&int64_payload(&v))
            }
        }
        (DataType::Integer | DataType::Date, ENC_DELTA_BITPACK) => {
            if dtype == DataType::Date {
                match v.data() {
                    VectorData::Date32(d) => enc_ints_delta(d.as_slice()),
                    _ => unreachable!(),
                }
            } else {
                enc_ints_delta(&int32_payload(&v))
            }
        }
        (DataType::Bigint | DataType::Timestamp, ENC_DELTA_BITPACK) => {
            if dtype == DataType::Timestamp {
                match v.data() {
                    VectorData::TimestampMicros(d) => enc_ints_delta(d.as_slice()),
                    _ => unreachable!(),
                }
            } else {
                enc_ints_delta(&int64_payload(&v))
            }
        }
        (DataType::Double, ENC_PLAIN) => enc_u64_plain(
            &float64_payload(&v)
                .iter()
                .map(|x| x.to_bits())
                .collect::<Vec<_>>(),
        ),
        (DataType::Double, ENC_ALP) => enc_alp(&float64_payload(&v), usize::MAX)
            .expect("no limit: ALP always yields a payload"),
        (DataType::Double, ENC_RLE) => enc_u64_rle(
            &float64_payload(&v)
                .iter()
                .map(|x| x.to_bits())
                .collect::<Vec<_>>(),
        ),
        (DataType::Boolean, ENC_PLAIN) => enc_bool_plain(&bool_payload(&v)),
        (DataType::Boolean, ENC_RLE) => enc_bool_rle(&bool_payload(&v)),
        (DataType::Varchar, ENC_PLAIN) => enc_strs_plain(&varchar_payload(&v)),
        (DataType::Varchar, ENC_DICT) => enc_strs_dict(&varchar_payload(&v)),
        (DataType::Varchar, ENC_RLE) => enc_strs_rle(&varchar_payload(&v)),
        (DataType::Varchar, ENC_DICT_BITPACK) => enc_strs_dict_bitpack(&varchar_payload(&v)),
        // Forced frame: bypass the size/probe gates (they only decide
        // *selection*), always wrap the plain payload.
        (_, ENC_LZ_FRAME) => {
            let base = plain_payload(&v);
            let tokens = lz_compress(&base);
            let mut w = Writer::new();
            w.u32(base.len() as u32);
            w.u8(ENC_PLAIN);
            w.u32(tokens.len() as u32);
            w.bytes(&tokens);
            w.buf
        }
        _ => panic!("roundtrip_chunk: unsupported forced tag {tag} for {dtype:?}"),
    };
    let validity = encode_validity(&v);
    let mut w = Writer::new();
    w.u8(tag);
    w.u32(validity.len() as u32);
    w.bytes(&validity);
    w.bytes(&payload);
    let mut r = Reader::new(&w.buf);
    let t = r.u8().unwrap();
    let decoded = decode_chunk_typed(dtype, t, &mut r, true)
        .unwrap_or_else(|e| panic!("CTX dtype={dtype:?} tag={t} n={} err={e}", values.len()));
    (0..decoded.len()).map(|i| decoded.get(i)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::CHUNK_SIZE;

    #[test]
    fn varchar_offsets_bound_the_u32_limit() {
        assert!(varchar_offsets_fit(0));
        assert!(varchar_offsets_fit(u32::MAX as u64));
        assert!(!varchar_offsets_fit(u32::MAX as u64 + 1));
    }

    /// Pinned v5 size and body CRC of [`golden_db`]'s image (see
    /// `golden_v5_bytes_are_pinned`). The CRC is over the body *without* the
    /// trailer: a CRC over the whole file (trailer included) is the fixed
    /// data+trailer identity constant and would not notice content drift.
    const GOLDEN_V5_LEN: usize = 111775;
    const GOLDEN_V5_BODY_CRC: u32 = 1766647982;

    fn roundtrip(db: &PinkDb) -> PinkDb {
        let path = temp_file("rt");
        db.save(path.to_str().unwrap()).unwrap();
        let data = std::fs::read(&path).unwrap();
        assert!(data.starts_with(MAGIC_V5));
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
        // Date and Timestamp reuse the integer codecs.
        let dates: Vec<Value> = vec![
            Value::Date(i32::MIN),
            Value::Null,
            Value::Date(i32::MAX),
            Value::Date(0),
        ];
        assert_eq!(
            roundtrip_chunk(DataType::Date, dates.clone(), ENC_BITPACK),
            dates
        );
        let stamps: Vec<Value> = vec![
            Value::Timestamp(i64::MIN),
            Value::Timestamp(i64::MAX),
            Value::Null,
        ];
        assert_eq!(
            roundtrip_chunk(DataType::Timestamp, stamps.clone(), ENC_PLAIN),
            stamps
        );
    }

    /// Delta+bitpack layout: constant-delta chunks are 21 bytes (12 for
    /// `n <= 1`), full-span extremes need width 64, and wrapping deltas
    /// (`i64::MAX → i64::MIN`) round-trip bit-exactly.
    #[test]
    fn delta_bitpack_layout_and_roundtrip() {
        // Monotone +1 over a full chunk: base(8) + delta_min(8) + width(1) + n(4).
        let seq: Vec<Value> = (0..CHUNK_SIZE)
            .map(|i| Value::Integer(1000 + i as i32))
            .collect();
        let v = vector_from_values(DataType::Integer, &seq);
        let payload = enc_ints_delta(&int32_payload(&v));
        assert_eq!(payload.len(), 21, "constant-delta chunk must be 21 bytes");
        assert_eq!(
            roundtrip_chunk(DataType::Integer, seq.clone(), ENC_DELTA_BITPACK),
            seq
        );
        // n == 1 and n == 0: base only (12 bytes).
        let one = vec![Value::Bigint(7)];
        let v1 = vector_from_values(DataType::Bigint, &one);
        assert_eq!(enc_ints_delta(&int64_payload(&v1)).len(), 12);
        assert_eq!(
            roundtrip_chunk(DataType::Bigint, one.clone(), ENC_DELTA_BITPACK),
            one
        );
        let empty: Vec<i64> = Vec::new();
        assert_eq!(enc_ints_delta(&empty).len(), 12);
        // i64 extremes: deltas wrap (MAX→MIN is +1 wrapping) and the span
        // needs width 64.
        let ex: Vec<Value> = vec![
            Value::Bigint(i64::MAX),
            Value::Bigint(i64::MIN),
            Value::Bigint(i64::MAX),
            Value::Null,
            Value::Bigint(0),
            Value::Bigint(-1),
        ];
        assert_eq!(
            roundtrip_chunk(DataType::Bigint, ex.clone(), ENC_DELTA_BITPACK),
            ex
        );
        let ei: Vec<Value> = vec![
            Value::Integer(i32::MAX),
            Value::Integer(i32::MIN),
            Value::Integer(0),
            Value::Null,
        ];
        assert_eq!(
            roundtrip_chunk(DataType::Integer, ei.clone(), ENC_DELTA_BITPACK),
            ei
        );
        let ts: Vec<Value> = (0..100)
            .map(|i| Value::Timestamp(1_700_000_000_000_000 + i * 1000))
            .collect();
        assert_eq!(
            roundtrip_chunk(DataType::Timestamp, ts.clone(), ENC_DELTA_BITPACK),
            ts
        );
        let dt: Vec<Value> = (-50..50).map(Value::Date).collect();
        assert_eq!(
            roundtrip_chunk(DataType::Date, dt.clone(), ENC_DELTA_BITPACK),
            dt
        );
    }

    /// Full-span BIGINT values force bitpack width 64: the accumulator must
    /// be cleared, not shifted by 64 (a latent bug in the v3 reader).
    #[test]
    fn bitpack_width_64_roundtrips() {
        let ex: Vec<Value> = vec![
            Value::Bigint(i64::MIN),
            Value::Bigint(i64::MAX),
            Value::Bigint(0),
            Value::Null,
            Value::Bigint(i64::MIN),
        ];
        assert_eq!(
            roundtrip_chunk(DataType::Bigint, ex.clone(), ENC_BITPACK),
            ex
        );
    }

    /// DICT_BITPACK: every dictionary size class, empty/single-entry chunks,
    /// the 2048-entry (11-bit) ceiling, and the strict width rule.
    #[test]
    fn dict_bitpack_roundtrips_and_picks_smallest() {
        // Round-trip every dictionary size class (forced tag), including the
        // 11-bit ceiling; selection is asserted separately (with all-distinct
        // values the plain offsets+data encoder is smaller).
        for dict_len in [1usize, 2, 3, 4, 5, 2047, 2048] {
            let strs: Vec<Value> = (0..CHUNK_SIZE)
                .map(|i| Value::Varchar(format!("value-{}", i % dict_len)))
                .collect();
            assert_eq!(
                roundtrip_chunk(DataType::Varchar, strs.clone(), ENC_DICT_BITPACK),
                strs,
                "dict_len={dict_len}"
            );
        }
        // Low cardinality: DICT_BITPACK must beat the u32-code dictionary.
        for dict_len in [2usize, 3, 4, 5] {
            let strs: Vec<Value> = (0..CHUNK_SIZE)
                .map(|i| Value::Varchar(format!("value-{}", i % dict_len)))
                .collect();
            let (tag, _) = encode_chunk(DataType::Varchar, &strs);
            assert_eq!(tag, ENC_DICT_BITPACK, "dict_len={dict_len}");
        }
        // A single distinct value → width 0, zero code bytes.
        let same: Vec<Value> = vec![Value::Varchar("only".to_string()); CHUNK_SIZE];
        let v = vector_from_values(DataType::Varchar, &same);
        let payload = enc_strs_dict_bitpack(&varchar_payload(&v));
        // n(4) + dict_len(4) + offsets(8) + data(4) + width(1) = 21.
        assert_eq!(payload.len(), 21);
        // Empty chunk.
        let empty: Vec<Value> = Vec::new();
        assert_eq!(
            roundtrip_chunk(DataType::Varchar, empty.clone(), ENC_DICT_BITPACK),
            empty
        );
        // Nulls ride the validity mask (placeholder "" participates in the
        // dictionary, like the old u32-code dictionary).
        let with_nulls: Vec<Value> = vec![
            Value::Varchar("a".to_string()),
            Value::Null,
            Value::Varchar("b".to_string()),
            Value::Null,
            Value::Varchar("a".to_string()),
        ];
        assert_eq!(
            roundtrip_chunk(DataType::Varchar, with_nulls.clone(), ENC_DICT_BITPACK),
            with_nulls
        );
        // High-cardinality strings with repeated structure (shared prefixes)
        // pick the LZ frame; a note on the candidate order: with all-distinct
        // short strings the plain offsets+data layout is smaller than the
        // 11-bit dictionary + dict bytes, and LZ wraps that plain layout.
        let distinct: Vec<Value> = (0..CHUNK_SIZE)
            .map(|i| Value::Varchar(format!("distinct-{i}-{:04}", i * 7919 % 10007)))
            .collect();
        let v = vector_from_values(DataType::Varchar, &distinct);
        let plain = enc_strs_plain(&varchar_payload(&v));
        let (tag, payload) = encode_chunk(DataType::Varchar, &distinct);
        assert_eq!(tag, ENC_LZ_FRAME, "repetitive distinct strings pick LZ");
        assert!(
            payload.len() < plain.len(),
            "LZ frame {} must beat plain {}",
            payload.len(),
            plain.len()
        );
        // Strings whose bytes never repeat (seeded hex randoms: 4-byte
        // collisions exist but pay for themselves) stay PLAIN.
        let mut x: u64 = 0x1234_5678_9abc_def0;
        let random: Vec<Value> = (0..CHUNK_SIZE)
            .map(|_| {
                x = x
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                Value::Varchar(format!("{x:016x}"))
            })
            .collect();
        let (tag, _) = encode_chunk(DataType::Varchar, &random);
        assert_eq!(tag, ENC_PLAIN, "random strings must stay plain");
    }

    /// A forged dictionary bit width is rejected (strict equality with the
    /// width derived from dict_len), so a corrupt file cannot desynchronize
    /// the bitstream.
    #[test]
    fn dict_bitpack_rejects_bad_width() {
        let strs: Vec<Value> = vec![Value::Varchar("a".to_string()); 40];
        let v = vector_from_values(DataType::Varchar, &strs);
        let payload = enc_strs_dict_bitpack(&varchar_payload(&v));
        // Layout: n(4) dict_len(4) offsets(8) data(1) width(1) codes...
        let width_at = 4 + 4 + 8 + 1;
        assert_eq!(payload[width_at], 0, "single entry → width 0");
        for bad in [1u8, 2, 11, 64] {
            let mut bad_payload = payload.clone();
            bad_payload[width_at] = bad;
            let mut w = Writer::new();
            w.u8(ENC_DICT_BITPACK);
            w.u32(0); // no validity bytes (all-valid flag)
            w.bytes(&bad_payload);
            let mut r = Reader::new(&w.buf);
            let tag = r.u8().unwrap();
            let err = decode_chunk_typed(DataType::Varchar, tag, &mut r, true).unwrap_err();
            assert!(
                err.to_string().contains("bad dictionary bit width"),
                "width {bad}: {err}"
            );
        }
    }

    /// The candidate order picks DELTA_BITPACK for monotone/gapped integer
    /// sequences and never over PLAIN for incompressible full-range data.
    #[test]
    fn delta_is_chosen_for_monotone_sequences() {
        let seq: Vec<Value> = (0..CHUNK_SIZE).map(|i| Value::Integer(i as i32)).collect();
        let (tag, payload) = encode_chunk(DataType::Integer, &seq);
        assert_eq!(tag, ENC_DELTA_BITPACK);
        assert_eq!(payload.len(), 21);
        // Gapped: 1-in-10 steps, still far below bitpack (range 20480 needs
        // 15 bits vs 4 bits of delta).
        let gapped: Vec<Value> = (0..CHUNK_SIZE)
            .map(|i| Value::Bigint(i as i64 * 10))
            .collect();
        let (tag, payload) = encode_chunk(DataType::Bigint, &gapped);
        assert_eq!(tag, ENC_DELTA_BITPACK);
        assert!(payload.len() < 21 + CHUNK_SIZE * 4 / 8 + 32);
        // Random full-range ints stay PLAIN (delta is never smaller here).
        let mut rnd: Vec<Value> = Vec::new();
        let mut x: u64 = 42;
        for _ in 0..CHUNK_SIZE {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            rnd.push(Value::Integer((x >> 32) as i32));
        }
        let (tag, _) = encode_chunk(DataType::Integer, &rnd);
        assert_eq!(tag, ENC_PLAIN);
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
        // Low-cardinality strings → the bitpacked dictionary.
        let strs: Vec<Value> = (0..100)
            .map(|i| Value::Varchar(format!("k{}", i % 4)))
            .collect();
        let (tag, _) = encode_chunk(DataType::Varchar, &strs);
        assert_eq!(tag, ENC_DICT_BITPACK);
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

    /// A v4 payload relabeled as v2 (magic + payload, no flags, no trailer)
    /// still loads — the old layout has no flags word.
    fn relabel_as_v2(full: &[u8]) -> Vec<u8> {
        let mut v2 = Vec::with_capacity(full.len());
        v2.extend_from_slice(MAGIC_V2);
        // Skip the v4 magic (8) and flags word (4); drop the CRC trailer (4).
        v2.extend_from_slice(&full[12..full.len() - 4]);
        v2
    }

    /// Repoint the CRC trailer after mutating the body (test helper: v4
    /// verifies the checksum before decoding, so forged bytes need a valid
    /// trailer to reach the decoder).
    fn fix_crc(bytes: &mut [u8]) {
        let len = bytes.len();
        let crc = crate::wal::crc32(&bytes[..len - 4]);
        bytes[len - 4..].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn v4_checksum_catches_bit_rot_and_v2_still_loads() {
        let mut db = PinkDb::new();
        db.create_table("t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(7)])
            .unwrap();
        let full = db.to_bytes().unwrap();
        assert!(full.starts_with(MAGIC_V5));
        // A flipped payload bit is a checksum error, not a silent wrong value.
        for i in MAGIC_V5.len()..full.len() {
            let mut bad = full.clone();
            bad[i] ^= 0x01;
            let err = PinkDb::load_bytes(&bad).expect_err("bit flip must be detected");
            assert!(
                err.to_string().contains("checksum") || i >= full.len() - 4,
                "{i}: {err}"
            );
        }
        // The v4 payload under the v2 magic (no flags, no trailer) still
        // loads: v1/v2/v3 layout compatibility.
        let v2 = relabel_as_v2(&full);
        let back = PinkDb::load_bytes(&v2).unwrap();
        assert_eq!(back.table("t").unwrap().num_rows(), 1);
    }

    /// A magic with a newer version digit is a clear "upgrade pinkdb" error,
    /// distinct from a bad magic.
    #[test]
    fn newer_format_magic_is_an_upgrade_error() {
        let mut db = PinkDb::new();
        db.create_table("t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(7)])
            .unwrap();
        let full = db.to_bytes().unwrap();
        for (magic, version) in [(b"PINKDB06", 6u8), (b"PINKDB09", 9)] {
            let mut data = full.clone();
            data[..8].copy_from_slice(magic);
            let err = PinkDb::load_bytes(&data).unwrap_err().to_string();
            assert!(
                err.contains(&format!("written by a newer pinkdb (format v{version})")),
                "{err}"
            );
        }
        // Not a digit → plain bad magic.
        let mut data = full.clone();
        data[..8].copy_from_slice(b"PINKDB0X");
        let err = PinkDb::load_bytes(&data).unwrap_err().to_string();
        assert!(err.contains("bad magic"), "{err}");
        // Unknown v5 flags bits are a hard error (re-checksummed so the flags
        // check, not the CRC, is what fires). Bits 0 (constraint section) and
        // 1 (column defaults) and 2 (materialized views) are known; their
        // absence/presence is exercised by the constraint/default and
        // materialized-view round-trip tests. Bit 3 is reserved for the next
        // format-gated section (index persistence).
        let mut data = full.clone();
        data[8..12].copy_from_slice(&8u32.to_le_bytes());
        fix_crc(&mut data);
        let err = PinkDb::load_bytes(&data).unwrap_err().to_string();
        assert!(err.contains("unsupported .pink v5 flags"), "{err}");
    }

    /// Reserved v4 tags (7..=9) are rejected with an upgrade hint, distinct
    /// from "invalid for dtype".
    #[test]
    fn reserved_v4_tags_error_with_upgrade_hint() {
        let mut db = PinkDb::new();
        db.create_table("t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(7)])
            .unwrap();
        let full = db.to_bytes().unwrap();
        // Plant tag 7 (DICT_RLE) and tag 9 (FSST) — the still-reserved tags —
        // wherever a tag byte might be read, repairing the CRC so the
        // reserved-tag error (not the checksum) fires.
        let mut seen = 0;
        for reserved in [ENC_DICT_RLE, ENC_FSST] {
            for i in 0..full.len() {
                if matches!(full[i], ENC_PLAIN | ENC_BITPACK | ENC_DELTA_BITPACK) {
                    let mut bad = full.clone();
                    bad[i] = reserved;
                    fix_crc(&mut bad);
                    if let Err(e) = PinkDb::load_bytes(&bad) {
                        let msg = e.to_string();
                        if msg.contains(&format!("unsupported v4 encoding tag {reserved}")) {
                            seen += 1;
                        }
                        assert!(
                            !msg.contains("invalid for"),
                            "reserved tag must not report 'invalid for': {msg}"
                        );
                    }
                }
            }
        }
        assert!(seen >= 1, "reserved tags must be reachable ({seen})");
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
        let mut v2 = relabel_as_v2(&full);
        v2[..8].copy_from_slice(MAGIC_V2);
        // (Overcommitting allocators may not abort on the old code path, so
        // also require that the chunk-length cap actually fires.)
        let mut capped = 0;
        for i in 8..v2.len().saturating_sub(3) {
            let mut bad = v2.clone();
            bad[i..i + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            if let Err(e) = PinkDb::load_bytes(&bad)
                && e.to_string().contains("exceeds the chunk size")
            {
                capped += 1;
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
        // A v1 file re-saved becomes the current format and round-trips.
        let back = roundtrip(&db);
        assert_eq!(back.list_databases(), vec!["main".to_string()]);
        assert_eq!(all_values(&back, "t"), all_values(&db, "t"));
    }

    /// Committed v2 and v3 fixtures (generated from history before the v4
    /// writer existed) load, keep their values bit-exactly, and re-save as
    /// v4 that round-trips.
    #[test]
    fn v2_and_v3_fixtures_load_and_resave() {
        // (dbname, table) → rows, doubles compared by bits.
        fn values_in(db: &PinkDb, dbname: &str, table: &str) -> Vec<Vec<Value>> {
            let t = db.table_in(dbname, table).unwrap();
            (0..t.num_rows())
                .map(|r| (0..t.num_cols()).map(|c| t.get(r, c).unwrap()).collect())
                .collect()
        }
        fn assert_same_rows(a: &[Vec<Value>], b: &[Vec<Value>], what: &str) {
            assert_eq!(a.len(), b.len(), "{what} rows");
            for (ra, rb) in a.iter().zip(b) {
                for (va, vb) in ra.iter().zip(rb) {
                    match (va, vb) {
                        (Value::Double(x), Value::Double(y)) => {
                            assert_eq!(x.to_bits(), y.to_bits(), "{what} double bits");
                        }
                        _ => assert_eq!(va, vb, "{what}"),
                    }
                }
            }
        }
        for (name, magic) in [
            ("v2_multi_db.pink", MAGIC_V2),
            ("v3_multi_db.pink", MAGIC_V3),
        ] {
            let path = format!("{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), name);
            let data = std::fs::read(&path).unwrap();
            assert!(data.starts_with(magic), "{name} magic");
            let db = PinkDb::open(&path).unwrap();
            assert_eq!(
                db.list_databases(),
                vec!["analytics".to_string(), "main".to_string()],
                "{name}"
            );
            let t = db.table_in("main", "fid").unwrap();
            assert_eq!(t.num_rows(), 4, "{name}");
            assert_eq!(t.get(0, 1), Some(Value::Integer(i32::MAX)), "{name}");
            assert_eq!(t.get(1, 1), Some(Value::Integer(i32::MIN)), "{name}");
            assert_eq!(t.get(0, 2), Some(Value::Bigint(i64::MAX)), "{name}");
            assert_eq!(t.get(1, 2), Some(Value::Bigint(i64::MIN)), "{name}");
            assert_eq!(t.get(3, 1), Some(Value::Null), "{name}");
            assert_eq!(db.table_in("main", "big2").unwrap().num_rows(), 3000);
            assert_eq!(db.table_in("analytics", "u").unwrap().num_rows(), 1);
            assert_eq!(db.table_in("main", "empty").unwrap().num_rows(), 0);
            // Re-save (v4) and compare every value bit-exactly.
            let back = roundtrip(&db);
            for dbname in db.list_databases() {
                for table in db.list_tables_in(&dbname) {
                    let what = format!("{name}: {dbname}.{table}");
                    assert_same_rows(
                        &values_in(&db, &dbname, &table),
                        &values_in(&back, &dbname, &table),
                        &what,
                    );
                }
            }
        }
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

    /// Values for the round-trip corpora, per dtype: nulls on `null_every`
    /// (every Nth row), varied payloads covering the codec corners.
    fn corpus_value(dtype: DataType, i: usize, null_every: usize) -> Value {
        if null_every > 0 && i % null_every == null_every - 1 {
            return Value::Null;
        }
        match dtype {
            DataType::Boolean => Value::Boolean(i.is_multiple_of(3)),
            DataType::Decimal { scale, .. } => Value::Decimal(
                match i % 6 {
                    0 => 0,
                    1 => -1,
                    2 => i128::from(i as i64) * 100_003,
                    3 => -i128::from(i as i64) * 7,
                    4 => 999_999_999,
                    _ => i128::from(i as i64) % 97 - 48,
                },
                scale,
            ),
            DataType::Interval => Value::Interval {
                months: match i % 5 {
                    0 => 0,
                    1 => 12,
                    2 => -3,
                    3 => i32::MAX,
                    _ => (i as i32) % 120,
                },
                days: (i as i32) % 31 - 15,
                micros: (i as i64).wrapping_mul(1_000_003) % 86_400_000_000,
            },
            DataType::Integer => Value::Integer(match i % 8 {
                0 => i32::MIN,
                1 => i32::MAX,
                2 => 0,
                3 => -1,
                4 => 1,
                5 => (i as i32).wrapping_mul(1_000_003),
                6 => 42,
                _ => -(i as i32) * 7,
            }),
            DataType::Bigint => Value::Bigint(match i % 8 {
                0 => i64::MIN,
                1 => i64::MAX,
                2 => 0,
                3 => -1,
                4 => i as i64,
                5 => (i as i64).wrapping_mul(1_000_003),
                6 => 9_999_999_999,
                _ => -(i as i64) * 13,
            }),
            DataType::Double => Value::Double(match i % 10 {
                0 => f64::from_bits(0x7ff8_0000_0000_0001), // NaN payload
                1 => f64::from_bits(0xfff8_0000_0000_0042), // negative NaN
                2 => f64::INFINITY,
                3 => f64::NEG_INFINITY,
                4 => -0.0,
                5 => f64::from_bits(1), // subnormal
                6 => f64::MIN_POSITIVE,
                7 => 9_007_199_254_740_993.0, // 2^53 + 1
                8 => 1e300,
                _ => (i as f64) / 100.0, // exact-decimal money values
            }),
            DataType::Varchar => Value::Varchar(match i % 7 {
                0 => String::new(),
                1 => "héllo wörld 🌍".to_string(),
                2 => "x".repeat(900),
                3 => format!("k{}", i % 5),   // dictionary
                4 => format!("distinct-{i}"), // high cardinality
                5 => "\0embedded\0".to_string(),
                _ => "same".to_string(),
            }),
            DataType::Date => Value::Date(match i % 5 {
                0 => i32::MIN,
                1 => i32::MAX,
                2 => 0,
                3 => 19_000,
                _ => i as i32 - 10_000,
            }),
            DataType::Timestamp => Value::Timestamp(match i % 5 {
                0 => i64::MIN,
                1 => i64::MAX,
                2 => 0,
                3 => 1_700_000_000_000_000,
                _ => 1_700_000_000_000_000 + (i as i64) * 1000,
            }),
        }
    }

    /// Compare two stores bit-exactly (NaN payloads and -0.0 included).
    fn assert_bits_equal(a: &Value, b: &Value) {
        match (a, b) {
            (Value::Double(x), Value::Double(y)) => {
                assert_eq!(x.to_bits(), y.to_bits(), "{a:?} != {b:?} (bit-exact)");
            }
            _ => assert_eq!(a, b),
        }
    }

    /// T1: every dtype × sizes around the chunk/word boundaries × NULL
    /// densities, saved and reopened with bit-exact values.
    #[test]
    fn roundtrip_corpora_are_bit_exact() {
        let sizes = [0usize, 1, 2, 63, 64, 65, 2047, 2048, 2049, 4096 + 576];
        for null_every in [0usize, 100, 2, 1] {
            for &n in &sizes {
                let mut db = PinkDb::new();
                db.create_table(
                    "t",
                    vec![
                        ("b".to_string(), DataType::Boolean),
                        ("i".to_string(), DataType::Integer),
                        ("g".to_string(), DataType::Bigint),
                        ("d".to_string(), DataType::Double),
                        ("v".to_string(), DataType::Varchar),
                        ("dt".to_string(), DataType::Date),
                        ("ts".to_string(), DataType::Timestamp),
                    ],
                )
                .unwrap();
                {
                    let t = db.table_mut("t").unwrap();
                    for i in 0..n {
                        t.append_row(vec![
                            corpus_value(DataType::Boolean, i, null_every),
                            corpus_value(DataType::Integer, i, null_every),
                            corpus_value(DataType::Bigint, i, null_every),
                            corpus_value(DataType::Double, i, null_every),
                            corpus_value(DataType::Varchar, i, null_every),
                            corpus_value(DataType::Date, i, null_every),
                            corpus_value(DataType::Timestamp, i, null_every),
                        ])
                        .unwrap();
                    }
                }
                let back = roundtrip(&db);
                let ctx = format!("n={n} null_every={null_every}");
                let a = all_values(&db, "t");
                let b = all_values(&back, "t");
                assert_eq!(a.len(), b.len(), "{ctx}");
                for (ra, rb) in a.iter().zip(b.iter()) {
                    for (ci, (va, vb)) in ra.iter().zip(rb.iter()).enumerate() {
                        match (va, vb) {
                            (Value::Double(x), Value::Double(y)) => {
                                assert_eq!(
                                    x.to_bits(),
                                    y.to_bits(),
                                    "{ctx} col {ci}: {x:?} != {y:?} (bit-exact)"
                                );
                            }
                            _ => assert_eq!(va, vb, "{ctx} col {ci}"),
                        }
                    }
                }
                // Column-wise get() parity too (bit-exact on doubles).
                let ta = db.table("t").unwrap();
                let tb = back.table("t").unwrap();
                for r in 0..n {
                    for c in 0..7 {
                        assert_bits_equal(&ta.get(r, c).unwrap(), &tb.get(r, c).unwrap());
                    }
                }
            }
        }
    }

    /// Compare two value vectors bit-exactly (NaN payloads, -0.0).
    fn assert_values_bit_equal(a: &[Value], b: &[Value], ctx: &str) {
        assert_eq!(a.len(), b.len(), "{ctx}: length");
        for (va, vb) in a.iter().zip(b) {
            match (va, vb) {
                (Value::Double(x), Value::Double(y)) => {
                    assert_eq!(x.to_bits(), y.to_bits(), "{ctx}: {x:?} != {y:?}");
                }
                _ => assert_eq!(va, vb, "{ctx}"),
            }
        }
    }

    /// ALP: bit-exact round trips for the double corners, the exception path
    /// (NaN payloads, ±INF, -0.0, subnormals, out-of-range), the zero-
    /// exception early exit on decimal money values, and the PLAIN fallback
    /// on adversarial doubles.
    #[test]
    fn alp_roundtrips_exceptions_and_selection() {
        // Exact-decimal money values: zero exceptions at e = 2.
        let money: Vec<Value> = (0..CHUNK_SIZE)
            .map(|i| Value::Double((i as f64) / 100.0))
            .collect();
        let v = vector_from_values(DataType::Double, &money);
        let payload = enc_alp(&float64_payload(&v), usize::MAX).unwrap();
        // n(4) e(1) codec(1) code_min(8) width(1) codes + exceptions(4).
        assert!(
            payload.len() < 4 + 1 + 1 + 8 + 1 + CHUNK_SIZE * 2 + 4 + 16,
            "money payload {}",
            payload.len()
        );
        assert_eq!(payload[4], 2, "smallest zero-exception scale is e = 2");
        assert_eq!(
            roundtrip_chunk(DataType::Double, money.clone(), ENC_ALP),
            money
        );
        let (tag, _) = encode_chunk(DataType::Double, &money);
        assert_eq!(tag, ENC_ALP);
        // Constant double: ALP (19 bytes) beats RLE (20 bytes).
        let same: Vec<Value> = vec![Value::Double(1.5); CHUNK_SIZE];
        let (tag, payload) = encode_chunk(DataType::Double, &same);
        assert_eq!(tag, ENC_ALP);
        assert_eq!(payload.len(), 4 + 1 + 1 + 8 + 1 + 4);
        // The corners: every one must survive bit-exactly, through whatever
        // codec wins (ALP exceptions when it is chosen, PLAIN otherwise).
        let corners: Vec<Value> = vec![
            Value::Double(f64::from_bits(0x7ff8_0000_0000_0001)),
            Value::Double(f64::from_bits(0xfff8_0000_0000_0042)),
            Value::Double(f64::INFINITY),
            Value::Double(f64::NEG_INFINITY),
            Value::Double(-0.0),
            Value::Double(0.0),
            Value::Double(f64::from_bits(1)),
            Value::Double(f64::MIN_POSITIVE),
            Value::Double(9_007_199_254_740_993.0),
            Value::Double(1e300),
            Value::Double(1e-300),
            Value::Double(f64::MAX),
            Value::Double(f64::MIN),
            Value::Null,
            Value::Double(123.45),
        ];
        assert_values_bit_equal(
            &roundtrip_chunk(DataType::Double, corners.clone(), ENC_ALP),
            &corners,
            "ALP corners",
        );
        assert_values_bit_equal(
            &roundtrip_chunk(DataType::Double, corners.clone(), ENC_PLAIN),
            &corners,
            "PLAIN corners",
        );
        // Adversarial randoms never get smaller under ALP: PLAIN wins.
        let mut rnd: Vec<Value> = Vec::new();
        let mut x: u64 = 7;
        for _ in 0..CHUNK_SIZE {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            // A random finite double (skip NaN/INF bit patterns).
            let bits = x & 0x7fef_ffff_ffff_ffff;
            rnd.push(Value::Double(f64::from_bits(bits)));
        }
        let (tag, _) = encode_chunk(DataType::Double, &rnd);
        assert_eq!(tag, ENC_PLAIN, "adversarial doubles must fall back");
    }

    /// ALP decode safety: bad exponent, reserved int codec, forged exception
    /// counts/rows and bad widths are all clear errors.
    #[test]
    fn alp_decode_rejects_malformed_bodies() {
        let values: Vec<Value> = vec![Value::Double(1.25), Value::Double(2.5), Value::Double(3.75)];
        let v = vector_from_values(DataType::Double, &values);
        let payload = enc_alp(&float64_payload(&v), usize::MAX).unwrap();
        let frame = |payload: &[u8]| {
            let mut w = Writer::new();
            w.u8(ENC_ALP);
            w.u32(0); // validity: all-valid flag
            w.bytes(payload);
            w.buf
        };
        let decode = |bytes: &[u8]| {
            let mut r = Reader::new(bytes);
            let tag = r.u8().unwrap();
            decode_chunk_typed(DataType::Double, tag, &mut r, true)
        };
        // Sanity: the untouched payload decodes.
        assert_eq!(
            (0..3)
                .map(|i| decode(&frame(&payload)).unwrap().get(i))
                .collect::<Vec<_>>(),
            values
        );
        // e > 18.
        let mut bad = payload.clone();
        bad[4] = 19;
        assert!(
            decode(&frame(&bad))
                .unwrap_err()
                .to_string()
                .contains("exponent")
        );
        // Reserved int_codec.
        let mut bad = payload.clone();
        bad[5] = 1;
        assert!(
            decode(&frame(&bad))
                .unwrap_err()
                .to_string()
                .contains("integer codec")
        );
        // width > 64: byte 4 + 1 + 1 + 8 = 14.
        let mut bad = payload.clone();
        bad[14] = 65;
        assert!(
            decode(&frame(&bad))
                .unwrap_err()
                .to_string()
                .contains("width")
        );
        // exception_count > n (the u32 right after the packed codes).
        let width = payload[14] as usize;
        let ex_at = 15 + (3 * width).div_ceil(8);
        let mut bad = payload.clone();
        bad[ex_at..ex_at + 4].copy_from_slice(&9u32.to_le_bytes());
        assert!(
            decode(&frame(&bad))
                .unwrap_err()
                .to_string()
                .contains("exception count exceeds")
        );
        // Exception row out of range and non-increasing rows.
        let with_ex: Vec<Value> = vec![
            Value::Double(f64::NAN),
            Value::Double(f64::NAN),
            Value::Double(1.0),
        ];
        let v = vector_from_values(DataType::Double, &with_ex);
        let payload = enc_alp(&float64_payload(&v), usize::MAX).unwrap();
        let rows_at = payload.len() - 2 * 12;
        let mut bad = payload.clone();
        bad[rows_at..rows_at + 4].copy_from_slice(&7u32.to_le_bytes());
        assert!(
            decode(&frame(&bad))
                .unwrap_err()
                .to_string()
                .contains("row out of range")
        );
        let mut bad = payload.clone();
        bad[rows_at + 12..rows_at + 16].copy_from_slice(&0u32.to_le_bytes());
        assert!(
            decode(&frame(&bad))
                .unwrap_err()
                .to_string()
                .contains("strictly increasing")
        );
    }

    /// S4: a chunk with no NULLs stores no validity bytes at all, and the
    /// reader rebuilds an all-valid mask; a null-free column shrinks by
    /// `ceil(n/64)*8` bytes per chunk.
    #[test]
    fn all_valid_chunks_write_no_validity_bytes() {
        let v = vector_from_values(
            DataType::Integer,
            &(0..CHUNK_SIZE)
                .map(|i| Value::Integer(i as i32))
                .collect::<Vec<_>>(),
        );
        assert_eq!(encode_validity(&v).len(), 0);
        let empty = Vector::new_empty(DataType::Integer);
        assert_eq!(encode_validity(&empty).len(), 0);
        // A null anywhere keeps the full bitmap.
        let mut with_null: Vec<Value> = (0..CHUNK_SIZE).map(|i| Value::Integer(i as i32)).collect();
        with_null[CHUNK_SIZE - 1] = Value::Null;
        let v = vector_from_values(DataType::Integer, &with_null);
        assert_eq!(encode_validity(&v).len(), CHUNK_SIZE / 64 * 8);
        // A partial chunk masks bits past len: 66 rows → two words, the
        // second keeps only the two real bits.
        let short: Vec<Value> = (0..65i32).map(Value::Integer).collect();
        let mut v = vector_from_values(DataType::Integer, &short);
        v.push_null().unwrap();
        let bytes = encode_validity(&v);
        assert_eq!(bytes.len(), 16);
        assert_eq!(
            u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            u64::MAX
        );
        assert_eq!(u64::from_le_bytes(bytes[8..16].try_into().unwrap()), 0b01);
        // Round-trip an all-valid multi-chunk table through a file.
        let mut db = PinkDb::new();
        db.create_table("t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            for i in 0..(CHUNK_SIZE * 2 + 7) {
                t.append_row(vec![Value::Integer(i as i32)]).unwrap();
            }
        }
        let back = roundtrip(&db);
        assert_eq!(all_values(&db, "t"), all_values(&back, "t"));
        let bytes = db.to_bytes().unwrap();
        // The whole file carries no validity words for this table: 3 chunks
        // × (1 tag + 4 validity_len + payload) — check the framed length is 0
        // by decoding it back (the payload itself is checked above).
        assert!(bytes.starts_with(MAGIC_V5));
    }

    /// A fixed multi-dtype corpus, used by the golden/identity tests.
    fn golden_db() -> PinkDb {
        let mut db = PinkDb::new();
        db.create_table(
            "t",
            vec![
                ("b".to_string(), DataType::Boolean),
                ("i".to_string(), DataType::Integer),
                ("g".to_string(), DataType::Bigint),
                ("d".to_string(), DataType::Double),
                ("v".to_string(), DataType::Varchar),
                ("dt".to_string(), DataType::Date),
                ("ts".to_string(), DataType::Timestamp),
            ],
        )
        .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            for i in 0..3000 {
                t.append_row(vec![
                    corpus_value(DataType::Boolean, i, 7),
                    corpus_value(DataType::Integer, i, 7),
                    corpus_value(DataType::Bigint, i, 7),
                    corpus_value(DataType::Double, i, 7),
                    corpus_value(DataType::Varchar, i, 7),
                    corpus_value(DataType::Date, i, 7),
                    corpus_value(DataType::Timestamp, i, 7),
                ])
                .unwrap();
            }
        }
        db.execute("CREATE DATABASE analytics;").unwrap();
        db.execute("CREATE TABLE analytics.u (k BIGINT, s VARCHAR);")
            .unwrap();
        db.execute("CREATE TABLE main.empty (x INTEGER);").unwrap();
        {
            let t = db.table_mut_in("analytics", "u").unwrap();
            for i in 0..100 {
                t.append_row(vec![
                    Value::Bigint(i * 1000),
                    Value::Varchar(format!("u{}", i % 3)),
                ])
                .unwrap();
            }
        }
        db
    }

    /// T2: the format is pinned — a fixed corpus has a fixed length and
    /// CRC-32, deterministic across parallel/serial encodes and across
    /// save→open→save.
    /// Tag 8: the LZ frame round-trips every dtype bit-exactly (the frame is
    /// dtype-agnostic: decode decompresses, then decodes the inner payload),
    /// is chosen for repetitive strings, and is byte-deterministic.
    #[test]
    fn lz_frame_roundtrips_and_is_selected() {
        let values: Vec<Value> = (0..CHUNK_SIZE)
            .map(|i| {
                Value::Varchar(format!(
                    "{{\"event\":\"click\",\"user_id\":{},\"page\":\"/home\",\"ok\":true}}",
                    i * 7919
                ))
            })
            .collect();
        let v = vector_from_values(DataType::Varchar, &values);
        let plain = enc_strs_plain(&varchar_payload(&v));
        let (tag, payload) = encode_chunk_typed(&v);
        assert_eq!(tag, ENC_LZ_FRAME, "repetitive JSON strings must pick LZ");
        assert!(payload.len() < plain.len());
        // Layout: raw_len | base_tag (PLAIN) | token_len | tokens.
        let raw_len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
        let token_len = u32::from_le_bytes(payload[5..9].try_into().unwrap()) as usize;
        assert_eq!(raw_len, plain.len());
        assert_eq!(payload[4], ENC_PLAIN);
        assert_eq!(payload.len(), 9 + token_len);
        // Deterministic: the same chunk encodes to the same frame.
        let (tag2, payload2) = encode_chunk_typed(&v);
        assert_eq!(tag, tag2);
        assert_eq!(payload, payload2);
        // Forced round trips for every dtype, bit-exact (doubles by bits).
        for dtype in [
            DataType::Boolean,
            DataType::Integer,
            DataType::Bigint,
            DataType::Double,
            DataType::Varchar,
            DataType::Date,
            DataType::Timestamp,
        ] {
            let vals: Vec<Value> = (0..200).map(|i| corpus_value(dtype, i, 9)).collect();
            let out = roundtrip_chunk(dtype, vals.clone(), ENC_LZ_FRAME);
            assert_values_bit_equal(&out, &vals, &format!("LZ {dtype:?}"));
        }
        // A frame never wraps a base it cannot shrink: incompressible bytes
        // produce no frame at all.
        let mut x: u64 = 0x1234_5678_9abc_def0;
        let random: Vec<u8> = (0..8192)
            .map(|_| {
                x = x
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (x >> 56) as u8
            })
            .collect();
        assert!(enc_lz_frame(ENC_PLAIN, &random).is_none());
    }

    /// Helper: a chunk frame around a raw payload (test).
    fn framed_chunk(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut w = Writer::new();
        w.u8(tag);
        w.u32(0); // all-valid
        w.bytes(payload);
        w.buf
    }

    /// Helper: an LZ payload with explicit header fields (test).
    fn lz_payload(raw_len: u32, base_tag: u8, tokens: &[u8]) -> Vec<u8> {
        let mut w = Writer::new();
        w.u32(raw_len);
        w.u8(base_tag);
        w.u32(tokens.len() as u32);
        w.bytes(tokens);
        w.buf
    }

    /// Helper: decode one VARCHAR chunk frame (test).
    fn decode_framed(bytes: &[u8]) -> Result<Vector> {
        let mut r = Reader::new(bytes);
        let tag = r.u8().unwrap();
        decode_chunk_typed(DataType::Varchar, tag, &mut r, true)
    }

    /// Tag 8 decode safety: every malformed shape is a clear error and never
    /// a panic or a hung allocation (the declared lengths are capped).
    #[test]
    fn lz_frame_decode_rejects_malformed_bodies() {
        let values: Vec<Value> = (0..100)
            .map(|i| Value::Varchar(format!("row-{}", i % 7)))
            .collect();
        let v = vector_from_values(DataType::Varchar, &values);
        let base = enc_strs_plain(&varchar_payload(&v));
        let tokens = lz_compress(&base);
        let good = lz_payload(base.len() as u32, ENC_PLAIN, &tokens);
        assert_eq!(
            decode_framed(&framed_chunk(ENC_LZ_FRAME, &good))
                .unwrap()
                .len(),
            100
        );
        let err = |payload: &[u8]| {
            decode_framed(&framed_chunk(ENC_LZ_FRAME, payload))
                .unwrap_err()
                .to_string()
        };
        // raw_len: 0, below the minimum, and above the cap.
        for bad_len in [0u32, 3, (1 << 24) + 1, u32::MAX] {
            let mut bad = good.clone();
            bad[0..4].copy_from_slice(&bad_len.to_le_bytes());
            assert!(
                err(&bad).contains("LZ frame length"),
                "{bad_len}: {}",
                err(&bad)
            );
        }
        // Reserved / frame base tags and a tag invalid for the dtype.
        for base_tag in [ENC_DICT_RLE, ENC_LZ_FRAME, ENC_FSST, 10, 200, 255] {
            let bad = lz_payload(base.len() as u32, base_tag, &tokens);
            assert!(
                err(&bad).contains("unsupported LZ base encoding"),
                "base {base_tag}: {}",
                err(&bad)
            );
        }
        let bad = lz_payload(base.len() as u32, ENC_ALP, &tokens);
        assert!(err(&bad).contains("invalid for VARCHAR"), "{}", err(&bad));
        // token_len beyond the buffer.
        let mut bad = good.clone();
        bad[5..9].copy_from_slice(&(tokens.len() as u32 + 100).to_le_bytes());
        assert!(
            err(&bad).contains("unexpected end of file"),
            "{}",
            err(&bad)
        );
        // Truncated token stream.
        let bad = lz_payload(base.len() as u32, ENC_PLAIN, &tokens[..tokens.len() - 2]);
        assert!(err(&bad).contains("truncated"), "{}", err(&bad));
        // Trailing token bytes after an otherwise complete output.
        let mut extra = tokens.clone();
        extra.push(0); // a one-byte literal token
        extra.push(b'x');
        let bad = lz_payload(base.len() as u32, ENC_PLAIN, &extra);
        assert!(err(&bad).contains("trailing bytes"), "{}", err(&bad));
        // A decompressed payload with bytes the inner codec does not consume.
        let mut long_base = base.clone();
        long_base.extend_from_slice(b"junk");
        let mut long_tokens = Vec::new();
        for chunk in long_base.chunks(LZ_MAX_LITERAL) {
            long_tokens.push(((chunk.len() - 1) as u8) << 1);
            long_tokens.extend_from_slice(chunk);
        }
        let bad = lz_payload(long_base.len() as u32, ENC_PLAIN, &long_tokens);
        assert!(
            err(&bad).contains("trailing bytes in LZ frame"),
            "{}",
            err(&bad)
        );
        // Literal run overruns the declared length (raw_len 4, literal 128).
        let mut toks = vec![0xFEu8];
        toks.extend(std::iter::repeat_n(b'x', 128));
        let bad = lz_payload(4, ENC_PLAIN, &toks);
        assert!(err(&bad).contains("literal run overruns"), "{}", err(&bad));
        // Zero and out-of-range back-references.
        let bad = lz_payload(4, ENC_PLAIN, &[0x03, 0x00, 0x00]);
        assert!(
            err(&bad).contains("back-reference out of range"),
            "{}",
            err(&bad)
        );
        let bad = lz_payload(4, ENC_PLAIN, &[0x03, 0x05, 0x00]);
        assert!(
            err(&bad).contains("back-reference out of range"),
            "{}",
            err(&bad)
        );
        // Match overruns the declared length (1 literal + a 129-byte match
        // into raw_len 4).
        let bad = lz_payload(4, ENC_PLAIN, &[0x00, b'A', 0xFD, 0x01, 0x00]);
        assert!(err(&bad).contains("match overruns"), "{}", err(&bad));
    }

    /// A file whose VARCHAR chunks use the LZ frame errors on truncation at
    /// (sampled) every offset: no cut may decode as a wrong value, and the
    /// frame's exact-length checks are what enforce it.
    #[test]
    fn lz_file_truncation_errors_at_every_sampled_offset() {
        let mut db = PinkDb::new();
        db.create_table("t", vec![("v".to_string(), DataType::Varchar)])
            .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            for i in 0..3000 {
                t.append_row(vec![Value::Varchar(format!(
                    "{{\"kind\":\"event\",\"row\":{},\"pad\":\"aaaaaaaaaaaaaaaa\"}}",
                    i * 7919
                ))])
                .unwrap();
            }
        }
        // The chunks really do use the frame.
        let t = db.table("t").unwrap();
        let chunk = t.scan_chunk(0).unwrap();
        assert_eq!(
            encode_chunk_typed(&chunk.columns[0]).0,
            ENC_LZ_FRAME,
            "the truncation corpus must exercise tag 8"
        );
        let bytes = db.to_bytes().unwrap();
        let mut cuts: Vec<usize> = (0..bytes.len()).step_by(101).collect();
        cuts.extend(bytes.len().saturating_sub(256)..bytes.len());
        for cut in cuts {
            assert!(
                PinkDb::load_bytes(&bytes[..cut]).is_err(),
                "truncation at {cut}/{} must error",
                bytes.len()
            );
        }
        assert!(PinkDb::load_bytes(&bytes).is_ok());
        // A decoded LZ file re-encodes to the same bytes (deterministic
        // probe + frame choice).
        let back = PinkDb::load_bytes(&bytes).unwrap();
        assert_eq!(back.to_bytes().unwrap(), bytes, "open→save drift with LZ");
    }

    /// The LZ probe is bounded and selective: repetitive text fires, random
    /// or near-random bytes (4-byte matches that do not pay for their
    /// control bytes) do not, and tiny payloads are never probed.
    #[test]
    fn lz_probe_is_selective() {
        let json = "{\"event\":\"click\",\"user_id\":42}".repeat(200);
        assert!(lz_probe(json.as_bytes()), "repetitive JSON must fire");
        // 62-alphabet random strings: no 4-byte repeat at all.
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let alpha = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        let text: String = (0..8000)
            .map(|_| {
                x = x
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                alpha[(x >> 33) as usize % alpha.len()] as char
            })
            .collect();
        assert!(!lz_probe(text.as_bytes()), "random text must not fire");
        // Hex randoms: 4-byte collisions exist but cost more than they save.
        let mut x: u64 = 0x1234_5678_9abc_def0;
        let mut hex = String::new();
        for _ in 0..4000 {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            hex.push_str(&format!("{x:016x}"));
        }
        assert!(!lz_probe(hex.as_bytes()), "hex randoms must not fire");
        // Small payloads are never probed.
        assert!(!lz_probe(b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"));
        assert!(!lz_probe(&[]));
    }

    /// Regression (pre-wave-45k): bitpacked payloads whose width exceeds 56
    /// bits lost the high bits of each element — the `u64` pack/unpack
    /// accumulator overflowed while a wide element was being shifted in.
    /// A full-width BITPACK chunk and a 63-bit DELTA_BITPACK chunk must
    /// round-trip bit-exactly; the same bug silently corrupted `.pink` v4
    /// data files for exactly these value patterns (it was found by the wave
    /// 45k lake-attach equivalence corpus).
    #[test]
    fn wide_bitpack_roundtrips_exactly() {
        // i64::MIN/MAX force a 64-bit bitpack, and deltas of -1, -i64::MAX,
        // 0, -3, ... force a 63-bit delta codec.
        let mut vals: Vec<Value> = vec![
            Value::Bigint(i64::MIN),
            Value::Bigint(i64::MAX),
            Value::Bigint(0),
        ];
        for i in 0..2045i64 {
            vals.push(Value::Bigint(-i * 3));
        }
        for tag in [ENC_BITPACK, ENC_DELTA_BITPACK] {
            let out = roundtrip_chunk(DataType::Bigint, vals.clone(), tag);
            assert_eq!(out, vals, "tag {tag} lost values");
        }
        // TIMESTAMP shares the 64-bit codecs.
        let mut ts: Vec<Value> = vec![
            Value::Timestamp(i64::MIN),
            Value::Timestamp(i64::MAX),
            Value::Timestamp(0),
        ];
        for i in 0..2045i64 {
            ts.push(Value::Timestamp(i64::MAX - i * 7));
        }
        for tag in [ENC_BITPACK, ENC_DELTA_BITPACK] {
            let out = roundtrip_chunk(DataType::Timestamp, ts.clone(), tag);
            assert_eq!(out, ts, "timestamp tag {tag} lost values");
        }
    }

    #[test]
    fn golden_v5_bytes_are_pinned() {
        let db = golden_db();
        let bytes = db.to_bytes().unwrap();
        // Recomputed once when the v5 container stabilized (v4 codecs + the
        // v5 magic; the golden store has no constraints and no DECIMAL/
        // INTERVAL columns, so its body doubles as the v4-shape pin); guards
        // accidental format/content drift (encoders are pure by contract).
        assert_eq!(bytes.len(), GOLDEN_V5_LEN, "file length drifted");
        assert_eq!(
            crate::wal::crc32(&bytes[..bytes.len() - 4]),
            GOLDEN_V5_BODY_CRC,
            "file bytes drifted"
        );
        assert_eq!(db.to_bytes_impl(false).unwrap(), bytes, "serial ≠ parallel");
        let back = PinkDb::load_bytes(&bytes).unwrap();
        assert_eq!(back.to_bytes().unwrap(), bytes, "open→save drift");
    }

    /// T4: truncating a v4 file at *every* offset errors (never panics, never
    /// silently returns wrong values). Exhaustive on a small all-types file;
    /// sampled (plus every offset in the last 512 bytes) on a bigger one.
    #[test]
    fn v4_truncation_at_every_offset_errors() {
        let mut db = PinkDb::new();
        db.create_table(
            "fid",
            vec![
                ("b".to_string(), DataType::Boolean),
                ("i".to_string(), DataType::Integer),
                ("g".to_string(), DataType::Bigint),
                ("d".to_string(), DataType::Double),
                ("v".to_string(), DataType::Varchar),
                ("dt".to_string(), DataType::Date),
                ("ts".to_string(), DataType::Timestamp),
            ],
        )
        .unwrap();
        {
            let t = db.table_mut("fid").unwrap();
            for i in 0..8 {
                t.append_row(vec![
                    corpus_value(DataType::Boolean, i, 5),
                    corpus_value(DataType::Integer, i, 5),
                    corpus_value(DataType::Bigint, i, 5),
                    corpus_value(DataType::Double, i, 5),
                    corpus_value(DataType::Varchar, i, 5),
                    corpus_value(DataType::Date, i, 5),
                    corpus_value(DataType::Timestamp, i, 5),
                ])
                .unwrap();
            }
        }
        let small = db.to_bytes().unwrap();
        assert!(small.len() < 4096, "{}", small.len());
        for cut in 0..small.len() {
            assert!(
                PinkDb::load_bytes(&small[..cut]).is_err(),
                "truncation at {cut}/{} must error",
                small.len()
            );
        }
        assert!(PinkDb::load_bytes(&small).is_ok());
        // Bigger image: sampled offsets plus the full tail.
        let big = golden_db().to_bytes().unwrap();
        let mut cuts: Vec<usize> = (0..big.len()).step_by(401).collect();
        cuts.extend(big.len().saturating_sub(512)..big.len());
        for cut in cuts {
            assert!(
                PinkDb::load_bytes(&big[..cut]).is_err(),
                "truncation at {cut}/{} must error",
                big.len()
            );
        }
    }

    /// Forged counts in the v4-only headers: dictionary length, ALP
    /// exception count, delta/chunk lengths (v2 relabel skips the CRC so the
    /// forged values reach the decoder).
    #[test]
    fn forged_v4_counts_are_capped_and_rejected() {
        // Dictionary length: build a DICT_BITPACK chunk, plant u32::MAX.
        let strs: Vec<Value> = (0..100)
            .map(|i| Value::Varchar(format!("v{}", i % 4)))
            .collect();
        let v = vector_from_values(DataType::Varchar, &strs);
        let payload = enc_strs_dict_bitpack(&varchar_payload(&v));
        let mut bad = payload.clone();
        bad[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        let mut w = Writer::new();
        w.u8(ENC_DICT_BITPACK);
        w.u32(0);
        w.bytes(&bad);
        let mut r = Reader::new(&w.buf);
        let tag = r.u8().unwrap();
        let err = decode_chunk_typed(DataType::Varchar, tag, &mut r, true).unwrap_err();
        assert!(err.to_string().contains("exceeds the chunk size"), "{err}");
        // ALP exception count: build an ALP chunk and plant u32::MAX.
        let dbls: Vec<Value> = (0..100).map(|i| Value::Double(i as f64 / 100.0)).collect();
        let v = vector_from_values(DataType::Double, &dbls);
        let payload = enc_alp(&float64_payload(&v), usize::MAX).unwrap();
        let width = payload[14] as usize;
        let ex_at = 15 + (100 * width).div_ceil(8);
        let mut bad = payload.clone();
        bad[ex_at..ex_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let mut w = Writer::new();
        w.u8(ENC_ALP);
        w.u32(0);
        w.bytes(&bad);
        let mut r = Reader::new(&w.buf);
        let tag = r.u8().unwrap();
        let err = decode_chunk_typed(DataType::Double, tag, &mut r, true).unwrap_err();
        assert!(err.to_string().contains("exceeds the chunk size"), "{err}");
    }

    /// T4: a seeded encode/decode loop over random small vectors, all dtypes.
    /// Every case must round-trip bit-exactly (whatever codec wins).
    #[test]
    fn seeded_encode_decode_loop_is_bit_exact() {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            x >> 33
        };
        for case in 0..2000 {
            let n = (next() % 300) as usize;
            let dtype = match next() % 9 {
                0 => DataType::Boolean,
                1 => DataType::Integer,
                2 => DataType::Bigint,
                3 => DataType::Double,
                4 => DataType::Varchar,
                5 => DataType::Date,
                6 => DataType::Timestamp,
                7 => DataType::Decimal {
                    precision: 18,
                    scale: 3,
                },
                _ => DataType::Interval,
            };
            let mut values: Vec<Value> = Vec::with_capacity(n);
            for _ in 0..n {
                let r = next();
                let val = match dtype {
                    DataType::Boolean => Value::Boolean(r % 2 == 0),
                    DataType::Integer => Value::Integer(r as i32),
                    DataType::Bigint => Value::Bigint(r as i64),
                    DataType::Double => Value::Double(f64::from_bits(r & 0x7fef_ffff_ffff_ffff)),
                    DataType::Varchar => {
                        if r % 5 == 0 {
                            Value::Varchar(String::new())
                        } else {
                            Value::Varchar(format!("s{}", r % 97))
                        }
                    }
                    DataType::Date => Value::Date(r as i32),
                    DataType::Timestamp => Value::Timestamp(r as i64),
                    DataType::Decimal { scale, .. } => Value::Decimal(r as i128, scale),
                    DataType::Interval => Value::Interval {
                        months: (r % 1000) as i32,
                        days: (r % 60) as i32,
                        micros: r as i64 % 86_400_000_000,
                    },
                };
                values.push(if r % 11 == 0 { Value::Null } else { val });
            }
            let v = vector_from_values(dtype, &values);
            let (tag, payload) = encode_chunk_typed(&v);
            let validity = encode_validity(&v);
            let mut w = Writer::new();
            w.u8(tag);
            w.u32(validity.len() as u32);
            w.bytes(&validity);
            w.bytes(&payload);
            let mut r = Reader::new(&w.buf);
            let t = r.u8().unwrap();
            let decoded = decode_chunk_typed(dtype, t, &mut r, true)
                .unwrap_or_else(|e| panic!("case {case}: dtype={dtype:?} tag={t} err={e}"));
            assert_eq!(decoded.len(), n, "case {case}");
            for i in 0..n {
                let (a, b) = (v.get(i), decoded.get(i));
                match (&a, &b) {
                    (Value::Double(x), Value::Double(y)) => {
                        assert_eq!(x.to_bits(), y.to_bits(), "case {case} row {i}");
                    }
                    _ => assert_eq!(a, b, "case {case} row {i} dtype={dtype:?} tag={t}"),
                }
            }
        }
    }

    /// The typed encoder and the (test) Value-built vector path produce the
    /// same bytes: a vector rebuilt from `get(i)` values must re-encode
    /// identically, including after an UPDATE→NULL (placeholders).
    #[test]
    fn typed_encoding_is_deterministic_across_value_rebuilds() {
        let mut db = PinkDb::new();
        db.create_table(
            "t",
            vec![
                ("b".to_string(), DataType::Boolean),
                ("i".to_string(), DataType::Integer),
                ("g".to_string(), DataType::Bigint),
                ("d".to_string(), DataType::Double),
                ("v".to_string(), DataType::Varchar),
                ("dt".to_string(), DataType::Date),
                ("ts".to_string(), DataType::Timestamp),
            ],
        )
        .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            for i in 0..300 {
                t.append_row(vec![
                    Value::Boolean(i % 3 == 0),
                    Value::Integer(i * 7 - 100),
                    Value::Bigint(i as i64 * 1_000_003),
                    Value::Double(i as f64 / 8.0),
                    Value::Varchar(format!("v{}", i % 5)),
                    Value::Date(i - 150),
                    Value::Timestamp(i as i64 * 1_000_000),
                ])
                .unwrap();
            }
            // NULL rows plus an UPDATE→NULL (placeholder bits stay).
            t.append_row(vec![Value::Null; 7]).unwrap();
        }
        let t = db.table("t").unwrap();
        for chunk_idx in 0..t.num_chunks() {
            let chunk = t.scan_chunk(chunk_idx).unwrap();
            for (col, vec) in chunk.columns.iter().enumerate() {
                let (tag_a, payload_a) = encode_chunk_typed(vec);
                let values: Vec<Value> = (0..vec.len()).map(|i| vec.get(i)).collect();
                let (tag_b, payload_b) = encode_chunk(vec.dtype(), &values);
                assert_eq!(tag_a, tag_b, "tag col {col} chunk {chunk_idx}");
                assert_eq!(payload_a, payload_b, "payload col {col} chunk {chunk_idx}");
            }
        }
    }

    /// Parallel and serial saves must produce identical bytes.
    #[test]
    fn parallel_and_serial_saves_are_byte_identical() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (i INTEGER, v VARCHAR, d DOUBLE);")
            .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            for i in 0..(CHUNK_SIZE * 2 + 7) {
                t.append_row(vec![
                    Value::Integer(i as i32),
                    Value::Varchar(format!("k{}", i % 40)),
                    Value::Double(i as f64 * 0.25),
                ])
                .unwrap();
            }
        }
        // A second table whose VARCHAR chunks pick the LZ frame: the
        // order-preserving parallel collect must still match the serial
        // bytes exactly.
        db.execute("CREATE TABLE z (j VARCHAR);").unwrap();
        {
            let t = db.table_mut("z").unwrap();
            for i in 0..(CHUNK_SIZE * 2 + 7) {
                t.append_row(vec![Value::Varchar(format!(
                    "{{\"kind\":\"row\",\"n\":{},\"pad\":\"aaaaaaaaaaaaaaaaaaaa\"}}",
                    i * 7919
                ))])
                .unwrap();
            }
        }
        let t = db.table("z").unwrap();
        assert_eq!(
            encode_chunk_typed(&t.scan_chunk(0).unwrap().columns[0]).0,
            ENC_LZ_FRAME,
            "the identity corpus must exercise tag 8"
        );
        let a = db.to_bytes_impl(true).unwrap();
        let b = db.to_bytes_impl(false).unwrap();
        assert_eq!(a, b, "parallel and serial saves differ");
    }

    /// save → open → save must reproduce the same bytes.
    #[test]
    fn open_save_is_byte_identity() {
        let mut db = PinkDb::new();
        db.execute("CREATE DATABASE analytics;").unwrap();
        db.execute("CREATE TABLE t (i INTEGER, v VARCHAR, d DOUBLE, b BOOLEAN);")
            .unwrap();
        db.execute(
            "INSERT INTO t VALUES (1, 'a', 1.5, true), (2, NULL, -0.0, false), \
             (NULL, 'b', 1e300, NULL);",
        )
        .unwrap();
        db.execute("CREATE TABLE analytics.empty (x INTEGER);")
            .unwrap();
        let first = db.to_bytes().unwrap();
        let back = PinkDb::load_bytes(&first).unwrap();
        let second = back.to_bytes().unwrap();
        assert_eq!(first, second, "open→save is not byte-identity");
    }

    /// Save-time statistics cost probe (run deliberately):
    ///
    /// ```text
    /// cargo test --release -p pinkdb --lib stats_collection_cost -- --ignored --nocapture
    /// ```
    ///
    /// Builds a 10M-row sales-shaped table, times the plain encode and the
    /// encode-with-stats (best of three each), and prints the added cost. The
    /// bytes must be identical either way.
    #[test]
    #[ignore = "timing probe: run with --release --ignored --nocapture"]
    fn stats_collection_cost_at_10m_rows() {
        let rows: usize = 10_000_000;
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE sales (id BIGINT, region VARCHAR, total DOUBLE, active BOOLEAN);")
            .unwrap();
        {
            let t = db.table_mut("sales").unwrap();
            let mut state = 0x9E37_79B9_7F4A_7C15u64;
            let mut next = move || {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                state
            };
            let regions = ["west", "east", "north", "south"];
            for i in 0..rows as i64 {
                let r = next();
                t.append_row(vec![
                    Value::Bigint(i + 1),
                    Value::Varchar(regions[(r % 4) as usize].to_string()),
                    if r % 100 == 0 {
                        Value::Null
                    } else {
                        Value::Double((r % 999_000) as f64 / 1000.0 + 1.0)
                    },
                    Value::Boolean(r % 10 < 7),
                ])
                .unwrap();
            }
        }
        let mut plain = f64::MAX;
        let mut with_stats = f64::MAX;
        let mut plain_bytes = Vec::new();
        for i in 0..3 {
            let t0 = std::time::Instant::now();
            let bytes = db.to_pink_bytes().unwrap();
            plain = plain.min(t0.elapsed().as_secs_f64());
            if i == 0 {
                plain_bytes = bytes;
            } else {
                assert_eq!(bytes, plain_bytes, "stats must not change the bytes");
            }
            let t0 = std::time::Instant::now();
            let (bytes, stats) = db.to_pink_bytes_with_stats().unwrap();
            with_stats = with_stats.min(t0.elapsed().as_secs_f64());
            assert_eq!(bytes, plain_bytes, "stats must not change the bytes");
            assert_eq!(stats[0].rows, rows as u64);
        }
        println!(
            "10M rows: encode {plain:.3}s, encode+stats {with_stats:.3}s, stats cost {:+.1} ms",
            (with_stats - plain) * 1000.0
        );
    }

    /// Boolean stats carry true min/max *values* (DuckDB semantics), never
    /// "seen a false/true" flags: a column of only `true` is min = max =
    /// `true`, only `false` is min = max = `false`, all-NULL is neither — so a
    /// stats block always carries both extremes or neither (the lake
    /// statistics-block validator refuses anything else).
    #[test]
    fn boolean_stats_are_true_min_max_values() {
        let mut db = PinkDb::new();
        db.execute(
            "CREATE TABLE t (b BOOLEAN); \
             INSERT INTO t VALUES (true), (NULL);",
        )
        .unwrap();
        let t = db.table_in("main", "t").unwrap();
        let stats = table_stats("main", "t", t);
        let col = &stats.columns[0];
        assert_eq!(col.min, Some(Value::Boolean(true)), "{col:?}");
        assert_eq!(col.max, Some(Value::Boolean(true)), "{col:?}");

        db.execute("CREATE TABLE f (b BOOLEAN); INSERT INTO f VALUES (false), (false);")
            .unwrap();
        let f = db.table_in("main", "f").unwrap();
        let stats = table_stats("main", "f", f);
        let col = &stats.columns[0];
        assert_eq!(col.min, Some(Value::Boolean(false)), "{col:?}");
        assert_eq!(col.max, Some(Value::Boolean(false)), "{col:?}");

        db.execute("CREATE TABLE n (b BOOLEAN); INSERT INTO n VALUES (NULL);")
            .unwrap();
        let n = db.table_in("main", "n").unwrap();
        let stats = table_stats("main", "n", n);
        let col = &stats.columns[0];
        assert_eq!(col.min, None, "{col:?}");
        assert_eq!(col.max, None, "{col:?}");
    }

    /// `to_pink_bytes_with_stats` reports exact facts and leaves the bytes
    /// untouched: NULL counts, first-seen min/max, NaN counts, all-NULL and
    /// empty handling, multi-chunk merging — checked against the decoded
    /// table with the engine's own comparator.
    #[test]
    fn saved_stats_are_exact_and_bytes_are_identical() {
        let mut db = PinkDb::new();
        db.create_table(
            "t",
            vec![
                ("b".to_string(), DataType::Boolean),
                ("i".to_string(), DataType::Integer),
                ("g".to_string(), DataType::Bigint),
                ("d".to_string(), DataType::Double),
                ("v".to_string(), DataType::Varchar),
                ("dt".to_string(), DataType::Date),
                ("ts".to_string(), DataType::Timestamp),
                ("n".to_string(), DataType::Integer),
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
                Value::Varchar("b".to_string()),
                Value::Date(0),
                Value::Timestamp(-1),
                Value::Null,
            ])
            .unwrap();
            t.append_row(vec![
                Value::Boolean(false),
                Value::Integer(9),
                Value::Bigint(i64::MIN),
                Value::Double(f64::INFINITY),
                Value::Varchar("a".to_string()),
                Value::Date(19_000),
                Value::Timestamp(1_700_000_000_000_000),
                Value::Null,
            ])
            .unwrap();
            t.append_row(vec![
                Value::Boolean(true),
                Value::Integer(i32::MIN),
                Value::Bigint(i64::MAX),
                Value::Double(f64::from_bits(0x7ff8_0000_cafe_0001)),
                Value::Varchar(String::new()),
                Value::Date(i32::MIN),
                Value::Timestamp(i64::MAX),
                Value::Null,
            ])
            .unwrap();
            // A second chunk (multi-chunk merge), still all-NULL for `n`.
            for i in 0..CHUNK_SIZE {
                t.append_row(vec![
                    Value::Boolean(i % 2 == 0),
                    Value::Integer(i as i32 - 100),
                    Value::Bigint(-(i as i64) * 7),
                    Value::Double((i as f64) / 16.0 - 3.0),
                    Value::Varchar(format!("s{:04}", i)),
                    Value::Date(i as i32),
                    Value::Timestamp(i as i64 * 13),
                    Value::Null,
                ])
                .unwrap();
            }
        }
        db.create_table("empty", vec![("x".to_string(), DataType::Integer)])
            .unwrap();

        let (bytes, stats) = db.to_pink_bytes_with_stats().unwrap();
        assert_eq!(
            bytes,
            db.to_pink_bytes().unwrap(),
            "stats collection must not change the bytes"
        );
        let back = PinkDb::load_bytes(&bytes).unwrap();
        let mut seen = 0;
        for table in &stats {
            seen += 1;
            let decoded = back.table_in(&table.db, &table.table).unwrap();
            assert_eq!(table.rows, decoded.num_rows() as u64);
            assert_eq!(table.columns.len(), decoded.num_cols());
            for (i, col) in table.columns.iter().enumerate() {
                let (name, dtype) = decoded.schema()[i].clone();
                assert_eq!(col.name, name);
                assert_eq!(col.dtype, dtype);
                let mut nulls = 0u64;
                let mut nan_count = 0u64;
                let (mut min, mut max): (Option<Value>, Option<Value>) = (None, None);
                for row in 0..decoded.num_rows() {
                    let v = decoded.get(row, i).unwrap();
                    if v.is_null() {
                        nulls += 1;
                        continue;
                    }
                    if matches!(v, Value::Double(x) if x.is_nan()) {
                        nan_count += 1;
                    }
                    if min
                        .as_ref()
                        .is_none_or(|m| crate::plan::compare_values(&v, m).is_lt())
                    {
                        min = Some(v.clone());
                    }
                    if max
                        .as_ref()
                        .is_none_or(|m| crate::plan::compare_values(&v, m).is_gt())
                    {
                        max = Some(v);
                    }
                }
                assert_eq!(col.null_count, nulls, "{name} nulls");
                assert_eq!(col.nan_count, nan_count, "{name} NaN count");
                assert_eq!(col.min, min, "{name} min");
                assert_eq!(col.max, max, "{name} max");
            }
        }
        assert_eq!(seen, 2, "one entry per table");
        // The empty table: schema present, no min/max, zero counts.
        let empty = stats.iter().find(|s| s.table == "empty").unwrap();
        assert_eq!(empty.rows, 0);
        assert_eq!(empty.columns.len(), 1);
        assert_eq!(empty.columns[0].null_count, 0);
        assert_eq!(empty.columns[0].min, None);
        assert_eq!(empty.columns[0].max, None);
        assert_eq!(empty.columns[0].nan_count, 0);
        // The all-NULL column: exact count, no min/max.
        let n = stats
            .iter()
            .flat_map(|s| &s.columns)
            .find(|c| c.name == "n")
            .unwrap();
        assert_eq!(n.null_count, 3 + CHUNK_SIZE as u64);
        assert_eq!(n.min, None);
        assert_eq!(n.max, None);
    }

    // ---------- wave 41: declarative constraints in the v5 section ----------

    /// A constrained store keeps its constraints exactly through save/open,
    /// and the v5 flags bit 0 marks the constraint section.
    #[test]
    fn wave41_constraints_roundtrip_through_v5() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, v INTEGER);")
            .unwrap();
        db.execute("CREATE TABLE c (cid INTEGER, pid INTEGER NOT NULL REFERENCES p(id), note VARCHAR UNIQUE);")
            .unwrap();
        db.execute("CREATE TABLE u (a INTEGER, b INTEGER, UNIQUE (a, b));")
            .unwrap();
        db.execute("CREATE TABLE free (x INTEGER);").unwrap();
        let bytes = db.to_bytes().unwrap();
        assert_eq!(&bytes[..8], MAGIC_V5);
        assert_eq!(
            u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            FLAG_CONSTRAINTS,
            "a constrained store sets flags bit 0"
        );
        let back = PinkDb::load_bytes(&bytes).unwrap();
        for name in ["p", "c", "u", "free"] {
            assert_eq!(
                db.table(name).unwrap().constraints,
                back.table(name).unwrap().constraints,
                "constraints of {name}"
            );
        }
        assert_eq!(back.to_bytes().unwrap(), bytes, "open→save drift");
        // A constraint-free store keeps flags 0 (the body stays v4-shaped).
        let mut plain = PinkDb::new();
        plain
            .create_table("t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        let plain_bytes = plain.to_bytes().unwrap();
        assert_eq!(&plain_bytes[8..12], 0u32.to_le_bytes());
        assert_eq!(
            PinkDb::load_bytes(&plain_bytes)
                .unwrap()
                .to_bytes()
                .unwrap(),
            plain_bytes
        );
        // DESCRIBE answers identically after the reopen.
        let mut back = back;
        let before = db.execute("DESCRIBE c;").unwrap().pop().unwrap();
        let after = back.execute("DESCRIBE c;").unwrap().pop().unwrap();
        assert_eq!(before, after);
    }

    // ---------- wave 80: materialized views in the v5 MV section ----------

    /// One-column integer result (this module's tests).
    fn ints(db: &mut PinkDb, sql: &str) -> Vec<i64> {
        match db.execute(sql).unwrap().pop().unwrap() {
            crate::QueryResult::Selected { rows, .. } => rows
                .into_iter()
                .map(|r| match &r[0] {
                    Value::Integer(i) => *i as i64,
                    Value::Bigint(i) => *i,
                    other => panic!("expected an integer, got {other:?}"),
                })
                .collect(),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// A store with materialized views sets flags bit 2 and round-trips the
    /// definitions (schedule, defining SQL, stamps, counters), the backing
    /// tables and — bit for bit — the file itself.
    #[test]
    fn wave80_materialized_views_roundtrip_through_v5() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2);").unwrap();
        db.execute(
            "CREATE MATERIALIZED VIEW mv REFRESH EVERY INTERVAL '5 minutes' AS SELECT a FROM t;",
        )
        .unwrap();
        db.execute("CREATE MATERIALIZED VIEW om REFRESH ON COMMIT AS SELECT COUNT(*) AS n FROM t;")
            .unwrap();
        db.execute("CREATE MATERIALIZED VIEW plain AS SELECT a * 2 AS a2 FROM t;")
            .unwrap();
        db.execute("INSERT INTO t VALUES (3);").unwrap();
        db.execute("REFRESH MATERIALIZED VIEW mv;").unwrap();
        let bytes = db.to_bytes().unwrap();
        assert_eq!(&bytes[..8], MAGIC_V5);
        let flags = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        assert_eq!(flags, FLAG_MATERIALIZED_VIEWS, "MV stores set flags bit 2");

        let mut back = PinkDb::load_bytes(&bytes).unwrap();
        assert_eq!(back.to_bytes().unwrap(), bytes, "open→save drift");
        let mv = back.materialized_view_in("main", "mv").unwrap();
        assert_eq!(
            mv.policy,
            crate::types::RefreshPolicy::Every {
                interval: crate::types::Interval {
                    months: 0,
                    days: 0,
                    micros: 300_000_000
                }
            }
        );
        assert_eq!(mv.refresh_count, 1);
        assert_eq!(mv.name, "mv");
        assert_eq!(
            back.materialized_view_in("main", "om").unwrap().policy,
            crate::types::RefreshPolicy::OnCommit
        );
        assert_eq!(
            back.materialized_view_in("main", "plain").unwrap().policy,
            crate::types::RefreshPolicy::Manual
        );
        // The rebound definition is live: refresh after a further write.
        back.execute("INSERT INTO t VALUES (4);").unwrap();
        back.execute("REFRESH MATERIALIZED VIEW mv;").unwrap();
        assert_eq!(ints(&mut back, "SELECT a FROM mv;"), vec![1, 2, 3, 4]);
        // ... and the ON COMMIT policy survived as a policy, not just a label.
        back.execute("INSERT INTO t VALUES (5);").unwrap();
        assert_eq!(ints(&mut back, "SELECT n FROM om;"), vec![5]);
        // DROPs are not silently resurrected: the section is authoritative.
        back.execute("DROP MATERIALIZED VIEW plain;").unwrap();
        let bytes2 = back.to_bytes().unwrap();
        let again = PinkDb::load_bytes(&bytes2).unwrap();
        assert!(again.materialized_view_in("main", "plain").is_err());
    }

    /// A definition whose base table is missing still loads (with no bound
    /// plan), reports a clear refresh error, heals once the table exists
    /// again, and drops cleanly. This is the one case the SQL text is the
    /// only rebuild source.
    #[test]
    fn wave80_broken_definition_loads_and_heals() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.execute("CREATE MATERIALIZED VIEW mv AS SELECT a FROM t;")
            .unwrap();
        db.execute("DROP TABLE t;").unwrap();
        let bytes = db.to_bytes().unwrap();
        let mut back = PinkDb::load_bytes(&bytes).unwrap();
        assert_eq!(back.to_bytes().unwrap(), bytes, "broken defs round-trip");
        assert!(
            back.materialized_view_in("main", "mv")
                .unwrap()
                .query
                .is_none()
        );
        assert_eq!(ints(&mut back, "SELECT a FROM mv;"), vec![1], "data kept");
        let err = back.execute("REFRESH MATERIALIZED VIEW mv;").unwrap_err();
        assert!(err.to_string().contains("t"), "{err}");
        // Once the base table is back, the persisted SQL rebinds the
        // definition and the view heals.
        back.execute("CREATE TABLE t (a INTEGER);").unwrap();
        back.execute("INSERT INTO t VALUES (7);").unwrap();
        back.execute("REFRESH MATERIALIZED VIEW mv;").unwrap();
        assert_eq!(ints(&mut back, "SELECT a FROM mv;"), vec![7]);
        back.execute("DROP MATERIALIZED VIEW mv;").unwrap();
        assert!(back.materialized_view_in("main", "mv").is_err());
    }

    /// Wave 48: the RBAC section round-trips (users with their PHC hashes,
    /// roles, memberships, table/database grants, admins), is byte-identical
    /// across saves, and a store without RBAC data keeps flags `0` — the
    /// section is opt-in exactly like every other format-gated one.
    #[test]
    fn wave48_rbac_section_round_trips_and_is_deterministic() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute(
            "CREATE USER alice PASSWORD_HASH '$argon2id$v=19$m=19456,t=2,p=1\
$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';",
        )
        .unwrap();
        db.execute("CREATE ROLE analyst;").unwrap();
        db.execute("CREATE ROLE reader;").unwrap();
        db.execute("GRANT reader TO analyst;").unwrap();
        db.execute("GRANT analyst TO alice;").unwrap();
        db.execute("GRANT SELECT ON TABLE t TO reader;").unwrap();
        db.execute("GRANT CREATE ON DATABASE main TO analyst;")
            .unwrap();
        db.execute("GRANT ADMIN TO analyst;").unwrap();
        let one = db.to_bytes().unwrap();
        let two = db.to_bytes().unwrap();
        assert_eq!(one, two, "deterministic bytes");
        assert_eq!(&one[..8], MAGIC_V5);
        let flags = u32::from_le_bytes([one[8], one[9], one[10], one[11]]);
        assert_eq!(flags, FLAG_RBAC, "the RBAC flag bit is set");
        // No constraints/defaults/MVs/procedures here: the body stays
        // v4-shaped apart from the section.
        let mut with_flags_cleared = one.clone();
        with_flags_cleared[8..12].copy_from_slice(&0u32.to_le_bytes());
        fix_crc(&mut with_flags_cleared);
        let err = PinkDb::load_bytes(&with_flags_cleared)
            .unwrap_err()
            .to_string();
        assert!(err.contains("trailing bytes"), "{err}");

        let back = PinkDb::load_bytes(&one).unwrap();
        assert!(back.has_users());
        assert_eq!(
            back.effective_table_privileges("alice", "main", "t"),
            vec![crate::rbac::Privilege::Select]
        );
        assert_eq!(
            back.effective_database_privileges("alice", "main"),
            vec![crate::rbac::Privilege::Create]
        );
        assert!(back.principal_is_admin("alice"));
        assert_eq!(
            back.user_password_hash("ALICE"),
            Some(
                "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA\
$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            )
        );
    }

    /// A store with no RBAC data writes no section and flags `0` (so every
    /// pre-wave-48 `.pink` body stays byte-for-byte identical).
    #[test]
    fn wave48_rbac_free_stores_keep_flags_zero() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("CREATE ROLE r; DROP ROLE r;").unwrap();
        let bytes = db.to_bytes().unwrap();
        assert_eq!(&bytes[..8], MAGIC_V5);
        assert_eq!(
            u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            0
        );
        // A dropped role left nothing behind: the store loads as before.
        let back = PinkDb::load_bytes(&bytes).unwrap();
        assert!(!back.has_users());
        assert!(!back.has_rbac_data());
    }

    /// Corrupt RBAC sections are clean errors, never a panic or a silent
    /// empty catalog: a bad PHC hash, an unknown privilege tag, a count that
    /// cannot fit, and a truncated section.
    #[test]
    fn wave48_corrupt_rbac_sections_error() {
        // A pristine v5 image with an RBAC section appended by hand, so each
        // case corrupts exactly one field.
        let image = |body: &dyn Fn(&mut Writer)| -> Vec<u8> {
            let mut db = PinkDb::new();
            db.execute("CREATE TABLE t (a INTEGER);").unwrap();
            let base = db.to_bytes().unwrap();
            let mut bytes = base[..base.len() - 4].to_vec();
            bytes[8..12].copy_from_slice(&FLAG_RBAC.to_le_bytes());
            let mut w = Writer::new();
            body(&mut w);
            bytes.extend_from_slice(&w.buf);
            bytes.extend_from_slice(&[0u8; 4]); // CRC placeholder
            fix_crc(&mut bytes);
            bytes
        };
        // One user, no roles and no memberships (the head of a section).
        let user_head = |w: &mut Writer| {
            w.u32(1);
            w.str("alice");
            w.str(
                "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA\
$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            );
            w.u32(0); // roles
            w.u32(0); // memberships
        };
        let user = |w: &mut Writer| {
            user_head(w);
            w.u32(0); // table grants
            w.u32(0); // database grants
            w.u32(0); // admins
        };
        // A hash that does not parse.
        let bad_hash = image(&|w: &mut Writer| {
            w.u32(1);
            w.str("alice");
            w.str("not-a-hash");
            w.u32(0);
            w.u32(0);
            w.u32(0);
            w.u32(0);
            w.u32(0);
        });
        let err = PinkDb::load_bytes(&bad_hash).unwrap_err().to_string();
        assert!(err.contains("invalid password hash"), "bad_hash: {err}");
        assert!(err.contains("alice"), "bad_hash: {err}");
        // An unknown privilege tag.
        let bad_tag = image(&|w: &mut Writer| {
            user_head(w);
            w.u32(1); // table grants: one entry
            w.str("main");
            w.str("t");
            w.u32(1); // one principal
            w.str("alice");
            w.u32(1); // one privilege
            w.u8(9); // ← unknown
            w.u32(0); // database grants
            w.u32(0); // admins
        });
        let err = PinkDb::load_bytes(&bad_tag).unwrap_err().to_string();
        assert!(err.contains("unknown privilege tag 9"), "bad_tag: {err}");
        // A user count that cannot fit in the remaining bytes.
        let bad_count = image(&|w: &mut Writer| {
            w.u32(0xffff_ffff);
        });
        let err = PinkDb::load_bytes(&bad_count).unwrap_err().to_string();
        assert!(
            err.contains("user count exceeds the remaining file"),
            "bad_count: {err}"
        );
        // A section cut short in the middle of the trailing admin list.
        let mut truncated = image(&|w: &mut Writer| {
            user(w);
            w.u32(1); // admins: one entry
            w.str("alice");
        });
        truncated.truncate(truncated.len() - 4 - 6);
        truncated.extend_from_slice(&[0u8; 4]); // CRC placeholder
        fix_crc(&mut truncated);
        let err = PinkDb::load_bytes(&truncated).unwrap_err().to_string();
        assert!(err.contains("corrupt file"), "truncated: {err}");
    }

    /// The next format-gated section (bit 3, `0x8`: index persistence) is
    /// refused by this build through the shared unknown-flags check — the
    /// "clean newer-format refusal" rule this section relies on for older
    /// builds.
    #[test]
    fn wave80_unknown_v5_flag_bits_are_refused() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        let mut bytes = db.to_bytes().unwrap();
        bytes[8..12].copy_from_slice(&0x8u32.to_le_bytes());
        let n = bytes.len();
        let crc = crate::wal::crc32(&bytes[..n - 4]);
        bytes[n - 4..].copy_from_slice(&crc.to_le_bytes());
        let err = PinkDb::load_bytes(&bytes).unwrap_err().to_string();
        assert!(err.contains("upgrade pinkdb"), "{err}");
        assert!(err.contains("0x00000008"), "{err}");
    }

    /// A corrupt constraint section is a clean error, never a panic.
    #[test]
    fn wave41_corrupt_constraints_error() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, CONSTRAINT kz PRIMARY KEY (a));")
            .unwrap();
        let full = db.to_bytes().unwrap();
        // Locate the constraint by its (distinctive) name; the kind byte sits
        // 5 bytes before the name (kind u8 + name_len u32).
        let at = full
            .windows(2)
            .position(|w| w == b"kz")
            .expect("constraint name in the image");
        assert_eq!(full[at - 5], CK_PRIMARY_KEY);
        // Unknown kind.
        let mut bad = full.clone();
        bad[at - 5] = 9;
        fix_crc(&mut bad);
        let err = PinkDb::load_bytes(&bad).unwrap_err().to_string();
        assert!(err.contains("unknown constraint kind 9"), "{err}");
        // Out-of-range column index (the table has one column): the single
        // col_idx u32 follows the name + col_count u32.
        let mut bad = full.clone();
        bad[at + 2 + 4..at + 2 + 8].copy_from_slice(&7u32.to_le_bytes());
        fix_crc(&mut bad);
        let err = PinkDb::load_bytes(&bad).unwrap_err().to_string();
        assert!(
            err.contains("constraint column index out of range"),
            "{err}"
        );
    }

    /// A durable reopen re-derives the constraints from the logged DDL text:
    /// both from the checkpoint and from WAL replay alone.
    #[test]
    fn wave41_durable_reopen_replays_constraints() {
        let ckpt = temp_file("w41_ckpt");
        let ckpt = std::path::PathBuf::from(format!("{}.db", ckpt.display()));
        let wal = std::path::PathBuf::from(format!("{}.wal", ckpt.display()));
        // Case 1: everything in the WAL (no checkpoint).
        {
            let mut db = PinkDb::open_durable(ckpt.to_str().unwrap()).unwrap();
            db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, v INTEGER);")
                .unwrap();
            db.execute(
                "CREATE TABLE c (cid INTEGER PRIMARY KEY, \
                 pid INTEGER NOT NULL REFERENCES p(id));",
            )
            .unwrap();
            db.execute("INSERT INTO c VALUES (1, 1);").unwrap();
        }
        {
            let mut db = PinkDb::open_durable(ckpt.to_str().unwrap()).unwrap();
            let rows = db.execute("DESCRIBE c;").unwrap().pop().unwrap();
            let crate::QueryResult::Selected { rows, .. } = rows else {
                panic!()
            };
            // cid is the primary key; pid is a NOT NULL foreign key (no key
            // marker, DuckDB's rule).
            assert_eq!(rows[0][3], Value::Varchar("PRI".to_string()));
            assert_eq!(rows[0][2], Value::Varchar("NO".to_string()));
            assert_eq!(rows[1][3], Value::Null);
            assert_eq!(rows[1][2], Value::Varchar("NO".to_string()));
            assert_eq!(
                db.table("c").unwrap().constraints.foreign_keys[0].ref_table,
                "p"
            );
            // Case 2: checkpointed, then reopened (constraints come from the
            // .pink section).
            db.execute("CHECKPOINT;").unwrap();
        }
        {
            let db = PinkDb::open_durable(ckpt.to_str().unwrap()).unwrap();
            assert_eq!(db.table("c").unwrap().constraints.foreign_keys.len(), 1);
            assert_eq!(
                db.table("p")
                    .unwrap()
                    .constraints
                    .primary_key()
                    .unwrap()
                    .columns,
                vec![0]
            );
        }
        let _ = std::fs::remove_file(&ckpt);
        let _ = std::fs::remove_file(&wal);
    }
}
