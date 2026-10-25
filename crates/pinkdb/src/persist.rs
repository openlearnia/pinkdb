//! Persistence: a single-file on-disk columnar format (`.pink`).
//!
//! # `PINKDB06` — the one `.pink` format
//!
//! The file is a **data region** of column chunks followed by a **footer**
//! and a fixed 20-byte **trailer**:
//!
//! ```text
//! data region: for every table (databases and tables in sorted name order)
//!   for every row group (122,880 rows = 60 engine chunks of 2,048)
//!     for every column (schema order)
//!       column chunk = page index + pages
//!
//! footer: store metadata (databases, tables, schemas, column defaults,
//!   constraints, index definitions + statistics, materialized views,
//!   procedures, macros, RBAC) and, per table, the row count and the
//!   row-group directory:
//!     row groups [ { rows,
//!                    per column: offset, length, page count,
//!                                 encoding summary, crc32c,
//!                                 stats { min, max, null_count, nan_count } } ]
//!
//! trailer (20 bytes): footer_crc32c u32 | footer_len u64 | "PINKDB06"
//! ```
//!
//! A **page** is one engine chunk (2,048 rows) encoded with the existing
//! per-chunk encoder, so decoding stays chunk-native; a **column chunk** is
//! the 60 pages of one (row group, column) pair. Every column chunk carries
//! its own **CRC32C**, so a reader verifies any chunk on its own — there is
//! **no whole-file CRC**. The footer carries its own CRC32C.
//!
//! A reader needs the 20-byte trailer plus the footer: two small range
//! requests (the lake's `pink` provider reads exactly those, plus the
//! projected column chunks of the row groups that survive pruning). The
//! engine's `open` reads the whole file (it materializes every column
//! eagerly, as before) but decodes the column chunks **in parallel**.
//!
//! ## Row groups and statistics
//!
//! A row group holds 122,880 rows (60 engine chunks). The footer records,
//! per column chunk, the **exact** statistics of that chunk: its min and max
//! (first-seen wins ties, exactly like the engine's MIN/MAX accumulators),
//! its NULL count and its NaN count. They feed zone-map pruning and the
//! lake's exact `COUNT`/`MIN`/`MAX`.
//!
//! ## Encodings
//!
//! Per-chunk encoding is picked by size (smallest wins; ties keep the
//! earlier candidate — the candidate order is part of the format contract):
//!
//! - Integers (INTEGER/BIGINT/DATE/TIMESTAMP): Plain (raw LE) vs RLE (low
//!   run count) vs Bitpack (small range, min + width + packed bits) vs
//!   delta bitpack (first value + min delta + width + packed deltas) vs
//!   **FastLanes bit-packing** (1,024-value blocks, transposed lanes, a
//!   frame of reference per block).
//! - Doubles: Plain (raw bits) vs RLE vs ALP (decimal-scaled integer codes
//!   plus a bit-exact exception list; non-finite, `-0.0`, subnormals and
//!   out-of-range values ride the exceptions).
//! - Booleans: Plain (bitpacked) vs RLE.
//! - Varchars: Plain (offsets + data) vs Dictionary (first-occurrence codes)
//!   vs RLE vs dictionary-bitpack (the dictionary with minimal-width codes,
//!   packed in the FastLanes transposed layout) vs a **compression frame**
//!   (tag 8) around the plain layout vs **FSST** (tag 9: a symbol table
//!   that rewrites the string bytes, then the second compression stage).
//!
//! The **second compression stage** (the general-purpose compressor a chunk
//! frame rides on) is chosen by **measurement** on the TPC-H SF1 set: the
//! hand-rolled LZ77 of wave 45j against the pure-Rust `lz4_flex`. Over the
//! 18,569 VARCHAR chunks of that store (569.6 MB of plain payloads) LZ77
//! compressed to 321,112,231 B in 1.172 s (1.559 s with the decompression),
//! `lz4_flex` to 318,692,386 B in 0.507 s (0.614 s with the decompression) —
//! smaller *and* 2.3x faster, so `lz4_flex` is the default. Default builds
//! link no C code either way; `PINKDB_COMPRESS=lz|lz4` overrides the choice
//! for a measurement run.
//!
//! ## Refusing older files
//!
//! `PINKDB01`–`PINKDB05` are **not** readable: their readers, fixtures and
//! compatibility tests are gone. Such a file is refused with
//! `this .pink file was written by an older pinkdb (PINKDB0N); this build
//! reads PINKDB06 only`.
//!
//! ## Safety
//!
//! The reader never panics on corrupt input: every read is bounds-checked
//! and surfaces `PinkDbError::Execution("...")`, and no allocation is sized
//! from an unvalidated length (chunk row counts are capped at
//! [`CHUNK_SIZE`], the writer's maximum; footer counts by the bytes left).
//! The footer checksum is verified before a single byte of it is decoded.
//!
//! Both directions are typed: `encode_chunk_typed` reads the [`Vector`]'s
//! payload directly (NULL placeholders substituted only when the chunk has
//! nulls) and `decode_chunk_typed` rebuilds a [`Vector`] via
//! [`Vector::from_raw`], so no per-row [`Value`] round trip is needed.
//! Tables are encoded chunk-parallel (order-preserving) and loaded
//! chunk-wise through `DataTable::append_shared_chunk`.
//!
//! The engine loads every column eagerly (as it always did) even though the
//! layout now allows reading one column chunk on its own: lazy column
//! loading for very large local stores is a recorded follow-up.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use rayon::prelude::*;

use crate::crypto::EncryptionKey;
use crate::storage::{
    DataTable, ForeignKeyConstraint, Index, KeyConstraint, KeyKind, PinkDb, TableConstraints,
};
#[cfg(test)]
use crate::types::ArrayElem;
use crate::types::{DataType, Interval, PinkDbError, Result, Value};
use crate::vector::{CHUNK_SIZE, Vector, VectorData};

/// The trailer magic of the current `.pink` format (`PINKDB06`).
///
/// The magic lives at the **end** of the file (the trailer), so a reader can
/// find the footer with two small range requests and never has to read the
/// data region to learn what the file is.
pub const MAGIC_V6: &[u8; 8] = b"PINKDB06";

/// The trailer's size: `footer_crc32c u32 | footer_len u64 | magic (8)`.
pub const TRAILER_LEN: usize = 20;

/// The rows per row group: 60 engine chunks of 2,048 rows.
pub const ROW_GROUP_ROWS: usize = 122_880;

/// The engine chunks per row group (60 × 2,048 = 122,880 rows).
pub const CHUNKS_PER_ROW_GROUP: usize = ROW_GROUP_ROWS / CHUNK_SIZE;

/// The footer's own version word (bumped when the footer layout changes).
const FOOTER_VERSION: u32 = 1;

/// Flags bit 0: every table carries the constraint section described in the
/// module docs. Set iff at least one table has constraints.
const FLAG_CONSTRAINTS: u32 = 0x1;

/// Flags bit 1: every table carries the column-default section (wave 65)
/// described in the module docs. Set iff at least one column has a
/// DEFAULT.
const FLAG_DEFAULTS: u32 = 0x2;

/// Flags bit 2: every database carries the materialized-view section
/// (wave 80: name, refresh policy, defining SQL, last refresh stamp, refresh
/// count) after its tables. Set iff at least one database has a materialized
/// view.
const FLAG_MATERIALIZED_VIEWS: u32 = 0x4;

/// Flags bit 3: every table carries the hash-index section (definitions +
/// statistics, see the module docs) after its row-group directory. Set iff
/// at least one table has an index.
const FLAG_INDEXES: u32 = 0x8;

/// [`FLAG_PROCEDURES`], visible to the crate's tests (the store's flags word
/// is asserted from `crate::procedure`'s persistence tests).
#[cfg(test)]
pub(crate) const FLAG_PROCEDURES_FOR_TEST: u32 = FLAG_PROCEDURES;

/// Flags bit 4: every database carries the stored-procedure section (wave
/// 44: name, typed parameters, language tag, body text) after its tables
/// and materialized views. Set iff at least one database has a procedure.
const FLAG_PROCEDURES: u32 = 0x10;

/// Flags bit 5: the file's footer carries the store-global RBAC section
/// (wave 48: users with their Argon2id PHC password hashes, roles, role
/// memberships, table and database grants, and the global `ADMIN` holders)
/// after every database. Set iff the store has any RBAC data.
const FLAG_RBAC: u32 = 0x20;

/// Flags bit 6: every database carries the SQL-macro section (wave 77: the
/// name and the full `CREATE MACRO` statement text) after its tables and
/// before its materialized-view section. Set iff at least one database has
/// a macro.
///
/// The macros are written **before** the materialized views on purpose: an
/// MV's defining SQL may call a macro and is rebound at load, so the macro
/// must already be in the catalog when that rebind runs.
const FLAG_MACROS: u32 = 0x40;

/// Wave 100: the file carries a per-database trigger section. Each trigger is
/// stored as the `CREATE TRIGGER …` text it was created from — a definition is
/// re-parsed on load, exactly like a macro or a materialized view, so nothing
/// is duplicated between the catalog and the file.
const FLAG_TRIGGERS: u32 = 0x80;

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
const VD_VARIANT: u8 = 9;
/// ARRAY rides [`crate::array::encode_cell`]'s self-describing bytes
/// (`u32 length | element kind | elements`).
const VD_ARRAY: u8 = 10;

/// Statistics value tags in the footer's per-chunk statistics block: the
/// same shapes as the default-value tags, plus an explicit "no value" tag
/// (an empty or all-NULL column has neither a min nor a max).
const SV_NONE: u8 = 0;
const SV_BOOL: u8 = 1;
const SV_I32: u8 = 2;
const SV_I64: u8 = 3;
const SV_F64: u8 = 4;
const SV_VARCHAR: u8 = 5;
const SV_DATE: u8 = 6;
const SV_TIMESTAMP: u8 = 7;
const SV_DECIMAL: u8 = 8;
const SV_INTERVAL: u8 = 9;

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
/// Compression frame (tag 8): a plain payload wrapped in a token stream,
/// used only when it shrinks the chunk (see [`enc_compressed_frame`]).
/// Decode = decompress, then decode the inner payload.
const ENC_LZ_FRAME: u8 = 8;
/// FSST (tag 9): a symbol table that rewrites the chunk's string bytes,
/// then the second compression stage (see [`enc_strs_fsst`]).
const ENC_FSST: u8 = 9;
/// FastLanes-style bit-packing (tag 10): 1,024-value blocks, a frame of
/// reference per block, transposed lanes (see [`enc_ints_fastlanes`]).
const ENC_FASTLANES: u8 = 10;
/// ARRAY (tag 11): one flat element buffer per chunk, byte-planed per
/// element kind, with the chunk-wide NULL-element bitmap in front (see
/// [`array_payload`]).
const ENC_ARRAY: u8 = 11;

/// The element kind byte inside an ARRAY page or schema entry.
/// The second compression stage of a chunk frame: the codec that turns the
/// chunk's inner payload into bytes.
///
/// The choice is a **measurement**, not a guess: wave 102 measured the
/// hand-rolled LZ77 of wave 45j against the pure-Rust `lz4_flex` on the
/// TPC-H SF1 set (see the module docs). Both are pure Rust — a default
/// build links no C code.
///
/// `PINKDB_COMPRESS=lz|lz4` overrides the default for a measurement run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecondStage {
    /// The hand-rolled greedy LZ77 (wave 45j).
    Lz,
    /// `lz4_flex`'s block format (pure Rust).
    Lz4,
}

impl SecondStage {
    /// The frame's codec tag.
    fn tag(self) -> u8 {
        match self {
            SecondStage::Lz => 1,
            SecondStage::Lz4 => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            1 => Ok(SecondStage::Lz),
            2 => Ok(SecondStage::Lz4),
            other => Err(PinkDbError::Execution(format!(
                "corrupt file: unknown compression codec {other}"
            ))),
        }
    }

    /// The default stage: the measured winner.
    fn default_stage() -> SecondStage {
        // The measurement (wave 102, TPC-H SF1) put `lz4_flex` ahead of the
        // hand-rolled LZ77 on both size and speed, so it is the default.
        // `PINKDB_COMPRESS` overrides it for a measurement run.
        static STAGE: std::sync::OnceLock<SecondStage> = std::sync::OnceLock::new();
        *STAGE.get_or_init(|| match std::env::var("PINKDB_COMPRESS").as_deref() {
            Ok("lz") => SecondStage::Lz,
            Ok("lz4") => SecondStage::Lz4,
            _ => SecondStage::Lz4,
        })
    }

    /// Compress `raw` (the chunk's inner payload).
    fn compress(self, raw: &[u8]) -> Vec<u8> {
        match self {
            SecondStage::Lz => lz_compress(raw),
            SecondStage::Lz4 => lz4_flex::compress(raw),
        }
    }

    /// Decompress `data` back into exactly `raw_len` bytes.
    fn decompress(self, data: &[u8], raw_len: usize) -> Result<Vec<u8>> {
        match self {
            SecondStage::Lz => lz_decompress(data, raw_len),
            SecondStage::Lz4 => lz4_flex::decompress(data, raw_len).map_err(|e| {
                PinkDbError::Execution(format!("corrupt file: LZ4 frame is broken ({e})"))
            }),
        }
    }
}

/// Tables with fewer chunks than this encode serially: the rayon fan-out
/// costs more than it saves on small tables.
const PARALLEL_SAVE_MIN_CHUNKS: usize = 32;

// ---------- save-time statistics ----------
//
// [`PinkDb::to_pink_bytes_with_stats`] collects exact per-column statistics
// during the same chunk pass that encodes a table (no second scan of the
// store, no change to the `.pink` bytes). The lake layer publishes them in
// its manifest; the engine itself never reads them back.
//
// The same per-chunk facts are always computed: the footer carries them
// per column chunk (zone-map pruning).

/// Exact per-column statistics the `.pink` writer observed while saving one
/// column: NULL count, the min/max of the non-NULL, non-NaN values
/// (first-seen wins ties, exactly like the engine's MIN/MAX accumulators),
/// and the exact NaN count (non-zero only for DOUBLE columns; NaN sorts
/// above every other double, so with any NaN the engine's MAX is NaN).
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
    /// Per-path statistics of a VARIANT column (empty for every other type).
    pub paths: Vec<SavedPathStats>,
}

/// Exact statistics of one **path** of a VARIANT column: the facts a filter
/// on `v['a']` is pruned with.
///
/// Only paths a shredding policy would type are recorded
/// ([`crate::variant_shred::infer_policy`]), and only when every non-NULL
/// value of the path had the policy's kind — otherwise `min`/`max` would not
/// bound the path's values and the entry is dropped.
#[derive(Debug, Clone, PartialEq)]
pub struct SavedPathStats {
    /// The statistics key ([`crate::variant::stats_path_text`]) — the same
    /// spelling DuckLake's `ducklake_file_variant_stats.variant_path` uses.
    pub path: String,
    /// The `shredded_type` name the bounds are published under
    /// ([`crate::variant_shred::ShredKind::stats_type`]).
    pub kind: String,
    /// Rows where the path is SQL NULL (key absent, or `Variant::Null`).
    pub null_count: u64,
    /// Exact min/max of the path's non-NULL values, as catalog text
    /// ([`crate::variant_shred::stats_bound_text`]).
    pub min: Option<String>,
    pub max: Option<String>,
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
/// chunk; uses the engine's own [`crate::plan::compare_values`] so ties
/// behave exactly like the MIN/MAX accumulators; NaN never reaches it — the
/// chunk stats count NaN separately).
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
        // VARIANT: no min/max. The total order spans kinds, so a chunk extreme
        // says nothing about a later point lookup; the row-group prune would
        // have to compare a VARIANT literal with the same code either way.
        VectorData::Variant(_) => ChunkColumnStats {
            null_count,
            min: None,
            max: None,
            nan_count: 0,
        },
        // ARRAY: no min/max for the same reason as VARIANT (element-wise
        // order, NULL elements sort last) — a bound would need the whole
        // element comparator to prune with.
        VectorData::Array { .. } => ChunkColumnStats {
            null_count,
            min: None,
            max: None,
            nan_count: 0,
        },
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
        // VARIANT's payload is self-describing, so the tag carries no schema
        // parameters.
        DataType::Variant => 9,
        // ARRAY's tag is incomplete like DECIMAL's: the schema writer stores
        // the element kind and length immediately after it.
        DataType::Array { .. } => 10,
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
        9 => Ok(DataType::Variant),
        _ => Err(PinkDbError::Execution(format!(
            "corrupt file: bad dtype tag {b}"
        ))),
    }
}

// ---------- CRC32C (Castagnoli) ----------
//
// The format's integrity checks are CRC32C, not the IEEE CRC-32 the WAL
// uses: it is the checksum every columnar format uses, and it is cheap
// enough to verify every column chunk of a large store on open.
//
// The implementation is a portable slicing-by-8 (no `unsafe`, no
// dependency): the eight tables are derived from the reflected Castagnoli
// polynomial 0x82F63B78 at compile time.

const fn crc32c_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
            bit += 1;
        }
        tables[0][i] = crc;
        i += 1;
    }
    let mut t = 1usize;
    while t < 8 {
        let mut i = 0usize;
        while i < 256 {
            let prev = tables[t - 1][i];
            tables[t][i] = (prev >> 8) ^ tables[0][(prev & 0xFF) as usize];
            i += 1;
        }
        t += 1;
    }
    tables
}

static CRC32C_TABLES: [[u32; 256]; 8] = crc32c_tables();

/// The CRC32C (Castagnoli) checksum of `bytes`.
///
/// The check value of the standard test vector (`"123456789"`) is
/// `0xE3069283`.
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    let (chunks, remainder) = bytes.as_chunks::<8>();
    for c in chunks {
        let lo = u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        let hi = u32::from_le_bytes([c[4], c[5], c[6], c[7]]);
        crc ^= lo;
        crc = CRC32C_TABLES[7][(crc & 0xFF) as usize]
            ^ CRC32C_TABLES[6][((crc >> 8) & 0xFF) as usize]
            ^ CRC32C_TABLES[5][((crc >> 16) & 0xFF) as usize]
            ^ CRC32C_TABLES[4][(crc >> 24) as usize]
            ^ CRC32C_TABLES[3][(hi & 0xFF) as usize]
            ^ CRC32C_TABLES[2][((hi >> 8) & 0xFF) as usize]
            ^ CRC32C_TABLES[1][((hi >> 16) & 0xFF) as usize]
            ^ CRC32C_TABLES[0][(hi >> 24) as usize];
    }
    for &b in remainder {
        crc = (crc >> 8) ^ CRC32C_TABLES[0][((crc ^ b as u32) & 0xFF) as usize];
    }
    !crc
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
/// treats `validity_len == 0` with `n > 0` as "every row valid".
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
/// `bytes.is_empty() && n > 0` means "every row valid" (the all-valid flag).
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

/// The largest DECIMAL precision whose unscaled values always fit an `i64`
/// (`10^18 - 1 < 2^63`): such chunks use the BIGINT encodings.
const DECIMAL_I64_MAX_PRECISION: u8 = 18;

/// A DECIMAL(p <= 18) chunk's unscaled values as `i64` (invalid rows zeroed,
/// like [`int64_payload`]). The precision bound makes the narrowing exact:
/// every engine write path enforces `types::decimal_fits`, so a value that
/// does not fit is a broken invariant — fail loudly, never store a wrong
/// number.
fn decimal_i64_payload(v: &Vector) -> Vec<i64> {
    let VectorData::Decimal { values, .. } = v.data() else {
        unreachable!("decimal_i64_payload on a non-DECIMAL vector")
    };
    let words = v.has_nulls().then(|| v.validity_words());
    values
        .iter()
        .enumerate()
        .map(|(i, &x)| match words {
            Some(words) if !valid_at(words, i) => 0,
            _ => i64::try_from(x).expect(
                "DECIMAL(p <= 18) value outside i64: the engine's precision invariant is broken",
            ),
        })
        .collect()
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

/// Whether a chunk's total VARCHAR bytes fit the `u32` string offsets.
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
///
/// The codes themselves are packed in the **FastLanes transposed layout**
/// (see [`fastlanes_pack`]): 1,024-value blocks whose lanes are transposed,
/// so the decode is branch-free.
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
        fastlanes_pack(&mut w, &shifted, width);
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

// ---------- FastLanes-style bit-packing (tag 10) ----------
//
// The layout is the FastLanes one: a block of 1,024 values is **transposed**
// into one *lane* per bit — lane `p` holds bit `p` of every value of the
// block, eight values per byte. The decode is then a branch-free loop over
// the lanes (and the layout is what an autovectorized decoder wants: each
// lane is a contiguous byte array).
//
// The values are stored as a **frame of reference**: each block carries its
// own minimum and the bit width of (value − block minimum), so a block whose
// values are close together costs far less than a global bit width.

/// The FastLanes block size: 1,024 values.
const FASTLANES_BLOCK: usize = 1024;

/// Pack `values` (`width` bits each) in the FastLanes transposed layout.
///
/// The output is, per 1,024-value block, `width` lanes of `ceil(n/8)` bytes:
/// lane `p`'s byte `j` holds bit `p` of the values `8j .. 8j+8` (bit `i % 8`
/// of the byte is value `8j + i % 8`'s bit `p`).
fn fastlanes_pack(w: &mut Writer, values: &[u64], width: u32) {
    debug_assert!(width > 0 && width <= 64);
    for block in values.chunks(FASTLANES_BLOCK) {
        let n = block.len();
        let bytes = n.div_ceil(8);
        for p in 0..width {
            let mut lane = vec![0u8; bytes];
            for (i, &v) in block.iter().enumerate() {
                if (v >> p) & 1 == 1 {
                    lane[i / 8] |= 1 << (i % 8);
                }
            }
            w.bytes(&lane);
        }
    }
}

/// Unpack `count` values of `width` bits from the FastLanes transposed
/// layout (the reader side of [`fastlanes_pack`]).
fn fastlanes_unpack(r: &mut Reader, count: usize, width: u32) -> Result<Vec<u64>> {
    let mut out = Vec::with_capacity(count);
    for block_start in (0..count).step_by(FASTLANES_BLOCK) {
        let n = (count - block_start).min(FASTLANES_BLOCK);
        let bytes = n.div_ceil(8);
        // One lane per bit, read as a contiguous byte array.
        let mut lanes: Vec<Vec<u8>> = Vec::with_capacity(width as usize);
        for _ in 0..width {
            lanes.push(r.take(bytes)?.to_vec());
        }
        for i in 0..n {
            let mut v: u64 = 0;
            for (p, lane) in lanes.iter().enumerate() {
                if (lane[i / 8] >> (i % 8)) & 1 == 1 {
                    v |= 1u64 << p;
                }
            }
            out.push(v);
        }
    }
    Ok(out)
}

/// The FastLanes bit-packing payload: per 1,024-value block, the frame of
/// reference (block minimum + width) and the transposed lanes.
///
/// The candidate wins only when it is *smaller* than the plain bit-packing
/// (which pays one global minimum and one global width) — a block whose
/// values are locally clustered is much cheaper this way.
fn enc_ints_fastlanes<T: IntElem>(nums: &[T]) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(nums.len() as u32);
    w.u32(FASTLANES_BLOCK as u32);
    for block in nums.chunks(FASTLANES_BLOCK) {
        let min = block.iter().map(|n| n.to_i64()).min().unwrap_or(0);
        let max = block.iter().map(|n| n.to_i64()).max().unwrap_or(0);
        let width = bits_needed_exact(max.wrapping_sub(min) as u64);
        w.u64(min as u64);
        w.u8(width as u8);
        if width > 0 {
            let shifted: Vec<u64> = block
                .iter()
                .map(|&n| n.to_i64().wrapping_sub(min) as u64)
                .collect();
            fastlanes_pack(&mut w, &shifted, width);
        }
    }
    w.buf
}

/// Decode the FastLanes bit-packing payload (the reader side of
/// [`enc_ints_fastlanes`]).
fn decode_fastlanes(r: &mut Reader, n: usize) -> Result<Vec<i64>> {
    let block_size = r.u32()? as usize;
    if block_size == 0 || block_size > CHUNK_SIZE {
        return r.err("bad FastLanes block size");
    }
    let mut out = Vec::with_capacity(n);
    let mut left = n;
    while left > 0 {
        let min = r.i64()?;
        let width = r.u8()? as u32;
        if width > 64 {
            return r.err("bad FastLanes width");
        }
        let take = left.min(block_size);
        if width == 0 {
            out.extend(std::iter::repeat_n(min, take));
        } else {
            for v in fastlanes_unpack(r, take, width)? {
                out.push(min.wrapping_add(v as i64));
            }
        }
        left -= take;
    }
    Ok(out)
}

// ---------- FSST (tag 9) ----------
//
// FSST ("Fast Static Symbol Table") rewrites a byte stream in an alphabet
// the compressor likes: the most frequent bytes of the sample become
// *symbols* (one byte each), and a byte that is not in the table is written
// as the escape symbol followed by the literal byte.
//
// The rewrite alone never shrinks anything (it is a bijection on the byte
// stream, plus the escape bytes), so the FSST payload is always handed to the
// **second compression stage**: the point of the symbol table is to make the
// stream more compressible, and the second stage does the compressing.

/// The FSST escape symbol: the symbol id that means "the next byte is a
/// literal". It is one past the last table entry, so it never collides with
/// a table symbol.
const FSST_ESCAPE: u8 = 255;

/// The largest symbol table: 254 symbols plus the escape byte (the table's
/// length is one byte, so it must fit in 255 entries).
const FSST_MAX_TABLE: usize = 254;

/// The bytes of a sample FSST builds its table from (the head of the data).
const FSST_SAMPLE_BYTES: usize = 4096;

/// Build the FSST symbol table over `data`: the most frequent bytes first,
/// the escape symbol last.
///
/// The table maps a symbol id to a byte: symbol `i` is `table[i]`. The byte
/// chosen as the escape is the *least* frequent byte of the sample (so the
/// escape is rare, exactly as FSST prescribes).
fn fsst_build_table(data: &[u8]) -> Vec<u8> {
    let sample = &data[..data.len().min(FSST_SAMPLE_BYTES)];
    let mut counts = [0u32; 256];
    for &b in sample {
        counts[b as usize] += 1;
    }
    // The escape byte: the least frequent one (ties → the smaller byte, so
    // the table is deterministic).
    let mut escape = 0usize;
    for b in 1..256usize {
        if counts[b] < counts[escape] {
            escape = b;
        }
    }
    // The table: the most frequent bytes first, the escape byte last.
    let mut order: Vec<usize> = (0..256).filter(|&b| b != escape).collect();
    order.sort_by(|&a, &b| counts[b].cmp(&counts[a]).then(a.cmp(&b)));
    order.truncate(FSST_MAX_TABLE);
    let mut table: Vec<u8> = order.iter().map(|&b| b as u8).collect();
    table.push(escape as u8);
    debug_assert!(table.len() <= FSST_MAX_TABLE + 1);
    table
}

/// Rewrite `data` in the symbol table's alphabet: a byte in the table
/// becomes its symbol, a byte that is not becomes the escape symbol
/// followed by the literal byte.
fn fsst_escape(data: &[u8], table: &[u8]) -> Vec<u8> {
    // The last table entry is the escape byte itself; the escape symbol is
    // `FSST_ESCAPE`.
    let escape_byte = *table.last().unwrap_or(&0);
    let mut map = [FSST_ESCAPE; 256];
    for (sym, &byte) in table[..table.len().saturating_sub(1)].iter().enumerate() {
        map[byte as usize] = sym as u8;
    }
    let mut out = Vec::with_capacity(data.len() + data.len() / 16);
    for &b in data {
        let sym = map[b as usize];
        out.push(sym);
        if sym == FSST_ESCAPE {
            out.push(b);
        }
    }
    let _ = escape_byte;
    out
}

/// The reader side of [`fsst_escape`]: the symbol stream back into bytes.
fn fsst_unescape(stream: &[u8], table: &[u8], out_len: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(out_len.min(1 << 16));
    let mut i = 0usize;
    while i < stream.len() {
        let sym = stream[i];
        i += 1;
        if sym == FSST_ESCAPE {
            let Some(&b) = stream.get(i) else {
                return Err(PinkDbError::Execution(
                    "corrupt file: truncated FSST escape".to_string(),
                ));
            };
            i += 1;
            out.push(b);
        } else {
            let Some(&byte) = table.get(sym as usize) else {
                return Err(PinkDbError::Execution(format!(
                    "corrupt file: FSST symbol {sym} is not in the table"
                )));
            };
            out.push(byte);
        }
    }
    if out.len() != out_len {
        return Err(PinkDbError::Execution(
            "corrupt file: FSST stream has the wrong length".to_string(),
        ));
    }
    Ok(out)
}

/// The FSST payload of a VARCHAR chunk: the plain layout's offsets (the
/// strings themselves are rewritten by the symbol table), the symbol table,
/// and the escaped stream handed to the second compression stage.
fn enc_strs_fsst<T: AsRef<str>>(strs: &[T]) -> Option<Vec<u8>> {
    let (offsets, data) = strs_offsets_and_data(strs);
    let stage = SecondStage::default_stage();
    // An empty chunk has no bytes to rewrite: the table holds the escape
    // byte alone and the escaped stream is empty.
    let (table, escaped) = if data.is_empty() {
        (vec![0u8], Vec::new())
    } else {
        let table = fsst_build_table(&data);
        let escaped = fsst_escape(&data, &table);
        (table, escaped)
    };
    let compressed = if escaped.is_empty() {
        Vec::new()
    } else {
        stage.compress(&escaped)
    };
    let mut w = Writer::new();
    w.u32(strs.len() as u32);
    for off in &offsets {
        w.u32(*off);
    }
    w.u8(table.len() as u8);
    w.bytes(&table);
    w.u32(escaped.len() as u32);
    w.u8(stage.tag());
    w.u32(compressed.len() as u32);
    w.bytes(&compressed);
    Some(w.buf)
}

/// The plain layout's offsets and data bytes.
fn strs_offsets_and_data<T: AsRef<str>>(strs: &[T]) -> (Vec<u32>, Vec<u8>) {
    let mut offsets = Vec::with_capacity(strs.len() + 1);
    let mut data = Vec::new();
    let mut off: u32 = 0;
    offsets.push(off);
    for s in strs {
        let bytes = s.as_ref().as_bytes();
        off += bytes.len() as u32;
        offsets.push(off);
        data.extend_from_slice(bytes);
    }
    (offsets, data)
}

// ---------- LZ frame (tag 8) and the second compression stage ----------
//
// A hand-rolled LZ77 in the spirit of LZ4: greedy longest-match over a
// 64 KiB window with a 4-byte hash table; tokens are a control byte per
// literal run / match, then the raw bytes / a u16 offset.
//
//   payload = u32 raw_len | u8 base_tag | u8 codec | u32 comp_len | comp
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

/// Compressibility probe for the compression candidates: compresses the first
/// [`LZ_PROBE_BYTES`] of the string data region with the **second stage**
/// (the codec that would really run) and reports whether it saved at least
/// 10%. Bounded cost (4 KiB scan) regardless of payload size; a false
/// negative only costs bytes (the frame is merely a candidate), never
/// correctness.
///
/// The probe deliberately looks at the string bytes, not the offsets block:
/// offsets measured as incompressible by the token format (4-byte matches
/// cost more in control bytes than they save), so a payload whose repeats
/// live only in its offsets — random strings — must not pay for a full
/// compression scan.
fn lz_probe(data: &[u8]) -> bool {
    let n = data.len().min(LZ_PROBE_BYTES);
    if n < 512 {
        return false;
    }
    let stage = SecondStage::default_stage();
    let compressed = stage.compress(&data[..n]);
    compressed.len() * 10 < n * 9
}

/// A compression frame around `base` (the base payload), or `None` when it
/// does not strictly shrink it. Caller-side gating (probe, size) is separate
/// so the decode path can accept any valid base tag.
fn enc_compressed_frame(base_tag: u8, base: &[u8]) -> Option<Vec<u8>> {
    if base.len() > LZ_MAX_RAW {
        return None;
    }
    let stage = SecondStage::default_stage();
    let compressed = stage.compress(base);
    // 4 (raw_len) + 1 (base_tag) + 1 (codec) + 4 (comp_len).
    if compressed.len() + 10 >= base.len() {
        return None;
    }
    let mut w = Writer::new();
    w.u32(base.len() as u32);
    w.u8(base_tag);
    w.u8(stage.tag());
    w.u32(compressed.len() as u32);
    w.bytes(&compressed);
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
            if out
                .len()
                .checked_add(len)
                .is_none_or(|total| total > raw_len)
            {
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
            if out
                .len()
                .checked_add(len)
                .is_none_or(|total| total > raw_len)
            {
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
            consider_best(&mut best, ENC_FASTLANES, enc_ints_fastlanes(&nums));
        }
        // Bigint and timestamp share the 64-bit codecs; the dtype tag on the
        // chunk keeps them distinct on decode.
        DataType::Bigint | DataType::Timestamp => {
            let nums = int64_payload(v);
            consider_best(&mut best, ENC_PLAIN, enc_ints_plain(&nums));
            consider_best(&mut best, ENC_RLE, enc_ints_rle(&nums));
            consider_best(&mut best, ENC_BITPACK, enc_ints_bitpack(&nums));
            consider_best(&mut best, ENC_DELTA_BITPACK, enc_ints_delta(&nums));
            consider_best(&mut best, ENC_FASTLANES, enc_ints_fastlanes(&nums));
        }
        // DECIMAL with precision <= 18: every unscaled value fits an i64
        // (|x| < 10^18 < 2^63), so the chunk is encoded exactly like a BIGINT
        // chunk — same candidates, same payloads — and decode widens back to
        // i128 using the schema's precision. TPC-H money columns
        // (DECIMAL(15,2)) shrink from 16 raw bytes per value to a few bits.
        DataType::Decimal { precision, .. } if precision <= DECIMAL_I64_MAX_PRECISION => {
            let nums = decimal_i64_payload(v);
            consider_best(&mut best, ENC_PLAIN, enc_ints_plain(&nums));
            consider_best(&mut best, ENC_RLE, enc_ints_rle(&nums));
            consider_best(&mut best, ENC_BITPACK, enc_ints_bitpack(&nums));
            consider_best(&mut best, ENC_DELTA_BITPACK, enc_ints_delta(&nums));
            consider_best(&mut best, ENC_FASTLANES, enc_ints_fastlanes(&nums));
        }
        // Wider DECIMAL (precision > 18): `u32 row count | raw unscaled
        // i128 values` (the schema carries precision/scale on disk). Invalid
        // rows are zeroed so the bytes stay deterministic; validity wins on
        // decode.
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
        // VARIANT: `u32 row count | u32 offsets (count + 1) | values`. A NULL
        // row contributes no bytes at all (its offset is unchanged), so the
        // validity bitmap alone decides NULL-ness and `Variant::Null` stays a
        // real value.
        // VARIANT: the payload repeats its tag bytes and key dictionaries from
        // value to value, so the second stage usually shrinks it (a
        // JSON-shaped 1M-row column goes from 107 MB to ~40 MB). The probe
        // keeps chunks it cannot help byte-identical to the unframed writer.
        DataType::Variant => {
            let plain = variant_payload(v);
            // Probe the value region, not the offset table in front of it: an
            // ascending offset table has no 4-byte repeat to find, so a probe
            // over the whole payload would never fire on a large chunk.
            let rows = variant_rows(v);
            let data_start = 4 + (rows + 1) * 4;
            let frame = if plain.len() >= LZ_MIN_ATTEMPT
                && data_start < plain.len()
                && lz_probe(&plain[data_start..])
            {
                enc_compressed_frame(ENC_PLAIN, &plain)
            } else {
                None
            };
            best = Some((ENC_PLAIN, plain));
            if let Some(frame) = frame {
                consider_best(&mut best, ENC_LZ_FRAME, frame);
            }
        }
        // ARRAY: a flat element buffer per chunk. Byte-planing groups an
        // embedding's exponent bytes together (they repeat far more than the
        // mantissas) and the row-major layout wins on small integers, so both
        // are built and the smaller one is written; the frame tag carries the
        // choice. The second stage then usually shrinks the planed exponent
        // planes further.
        DataType::Array { .. } => {
            let row_major = array_payload(v, 0);
            let planed = array_payload(v, 1);
            let plain = if planed.len() < row_major.len() {
                planed
            } else {
                row_major
            };
            let frame = if plain.len() >= LZ_MIN_ATTEMPT {
                enc_compressed_frame(ENC_ARRAY, &plain)
            } else {
                None
            };
            best = Some((ENC_ARRAY, plain));
            if let Some(frame) = frame {
                consider_best(&mut best, ENC_LZ_FRAME, frame);
            }
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
            // The compression frame (the last structural candidate) wraps the
            // plain layout. It is only tried when the best structural
            // candidate is still big enough to have room for a win
            // (`LZ_MIN_ATTEMPT`) and a bounded prefix probe says the string
            // bytes repeat: sales' low-cardinality VARCHAR chunks win with a
            // ~300-byte bitpacked dictionary and never pay for a scan. The
            // frame is only kept when strictly smaller, and ties keep the
            // earlier candidate, so files that never win with it stay
            // byte-identical to the pre-frame writer.
            let (frame, compressible) = {
                let best_len = plain
                    .len()
                    .min(dict.len())
                    .min(rle.len())
                    .min(dict_bp.len());
                let data_start = 4 + (strs.len() + 1) * 4;
                let compressible = best_len >= LZ_MIN_ATTEMPT && lz_probe(&plain[data_start..]);
                let frame = if compressible {
                    enc_compressed_frame(ENC_PLAIN, &plain)
                } else {
                    None
                };
                (frame, compressible)
            };
            // FSST: the symbol table rewrites the string bytes so the second
            // compression stage sees a smaller alphabet. It is the last
            // candidate, it rides the same probe as the compression frame
            // (a symbol table over incompressible bytes is pure cost), and it
            // only wins when it is strictly smaller.
            let fsst = {
                let best_len = plain
                    .len()
                    .min(dict.len())
                    .min(rle.len())
                    .min(dict_bp.len())
                    .min(frame.as_ref().map_or(usize::MAX, |f| f.len()));
                if best_len >= LZ_MIN_ATTEMPT && compressible {
                    enc_strs_fsst(&strs)
                } else {
                    None
                }
            };
            consider_best(&mut best, ENC_PLAIN, plain);
            consider_best(&mut best, ENC_DICT, dict);
            consider_best(&mut best, ENC_RLE, rle);
            consider_best(&mut best, ENC_DICT_BITPACK, dict_bp);
            if let Some(frame) = frame {
                consider_best(&mut best, ENC_LZ_FRAME, frame);
            }
            if let Some(fsst) = fsst {
                consider_best(&mut best, ENC_FSST, fsst);
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
        if out.len().checked_add(len).is_none_or(|total| total > n) {
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
        if out.len().checked_add(len).is_none_or(|total| total > n) {
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
        if out.len().checked_add(len).is_none_or(|total| total > n) {
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
        if out.len().checked_add(len).is_none_or(|total| total > n) {
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
        if out.len().checked_add(len).is_none_or(|total| total > n) {
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
fn tag_error(dtype: DataType, tag: u8) -> PinkDbError {
    let reserved = matches!(tag, ENC_DICT_RLE) || tag > ENC_ARRAY;
    if reserved {
        PinkDbError::Execution(format!(
            "unsupported .pink encoding tag {tag}; upgrade pinkdb"
        ))
    } else {
        PinkDbError::Execution(format!("corrupt file: encoding {tag} invalid for {dtype}"))
    }
}

/// Decode one chunk payload into a typed buffer + its row count.
/// The PLAIN payload of a VARIANT chunk: `u32 row count | u32 offsets
/// (count + 1) | encoded values`. A NULL row contributes no bytes (its offset
/// equals the next one).
/// The row count of a VARIANT chunk.
fn variant_rows(v: &Vector) -> usize {
    match v.data() {
        VectorData::Variant(values) => values.len(),
        _ => unreachable!("variant chunk has a non-variant payload"),
    }
}

fn variant_payload(v: &Vector) -> Vec<u8> {
    let VectorData::Variant(values) = v.data() else {
        unreachable!("variant chunk has a non-variant payload")
    };
    let mut w = Writer::new();
    w.u32(values.len() as u32);
    let mut data = Vec::new();
    let mut offsets = Vec::with_capacity(values.len() + 1);
    for (i, x) in values.iter().enumerate() {
        offsets.push(data.len() as u32);
        if !v.has_nulls() || v.is_valid(i) {
            crate::variant::encode(x, &mut data);
        }
    }
    offsets.push(data.len() as u32);
    for o in offsets {
        w.u32(o);
    }
    w.bytes(&data);
    w.buf
}

/// One ARRAY chunk's PLAIN payload: `u32 rows | u8 layout | u8 element kind |
/// u32 bitmap_len | bitmap | values`, where `layout` 1 stores the element
/// bytes byte-planed (plane `j` holds byte `j` of every element), which is
/// what makes embedding chunks compress. NULL rows hold zeros so the bytes
/// stay deterministic; a NULL row's element bits are never read.
fn array_payload(v: &Vector, layout: u8) -> Vec<u8> {
    let VectorData::Array {
        elem,
        len,
        values,
        valid,
    } = v.data()
    else {
        unreachable!("array chunk has a non-array payload")
    };
    let n = *len as usize;
    let width = array_width(*elem);
    let rows = values.len() / n;
    let nulls = v.has_nulls();
    let mut w = Writer::new();
    w.u32(rows as u32);
    w.u8(layout);
    w.u8(elem.tag());
    let bits = valid.as_deref().unwrap_or(&[]);
    w.u32(bits.len() as u32);
    if !bits.is_empty() {
        w.bytes(bits);
    }
    let mut raw = Vec::with_capacity(rows * n * width);
    for row in 0..rows {
        let zero = nulls && !v.is_valid(row);
        match values {
            crate::array::ArrayElems::Ints(vals) => {
                for &x in &vals[row * n..row * n + n] {
                    let x = if zero { 0 } else { x };
                    // INTEGER elements are 4 bytes wide on disk.
                    if width == 4 {
                        raw.extend_from_slice(&(x as i32).to_le_bytes());
                    } else {
                        raw.extend_from_slice(&x.to_le_bytes());
                    }
                }
            }
            crate::array::ArrayElems::Floats(vals) => {
                for &x in &vals[row * n..row * n + n] {
                    let x = if zero { 0.0 } else { x };
                    raw.extend_from_slice(&x.to_le_bytes());
                }
            }
            crate::array::ArrayElems::Floats32(vals) => {
                for &x in &vals[row * n..row * n + n] {
                    let x = if zero { 0.0 } else { x };
                    raw.extend_from_slice(&x.to_le_bytes());
                }
            }
        }
    }
    if layout == 0 {
        w.bytes(&raw);
    } else {
        let m = rows * n;
        let mut planed = vec![0u8; raw.len()];
        for i in 0..m {
            for j in 0..width {
                planed[j * m + i] = raw[i * width + j];
            }
        }
        w.bytes(&planed);
    }
    w.buf
}

/// The on-disk element width of an ARRAY type.
fn array_width(elem: crate::types::ArrayElem) -> usize {
    match elem {
        crate::types::ArrayElem::Integer | crate::types::ArrayElem::Float => 4,
        crate::types::ArrayElem::Bigint | crate::types::ArrayElem::Double => 8,
    }
}

fn decode_array_payload(r: &mut Reader, len: u32) -> Result<(VectorData, usize)> {
    let rows = r.chunk_count("array chunk length")?;
    let layout = r.u8()?;
    let elem_tag = r.u8()?;
    let elem = crate::types::ArrayElem::from_tag(elem_tag).ok_or_else(|| {
        PinkDbError::Execution(format!("corrupt file: bad ARRAY element kind {elem_tag}"))
    })?;
    let bits_len = r.u32()? as usize;
    let bits = r.take(bits_len)?;
    let width = array_width(elem);
    let values_len = rows * len as usize;
    let raw = r.take(values_len * width)?;
    let mut values = vec![0u8; raw.len()];
    match layout {
        0 => values.copy_from_slice(raw),
        1 => {
            for i in 0..values_len {
                for j in 0..width {
                    values[i * width + j] = raw[j * values_len + i];
                }
            }
        }
        other => {
            return Err(PinkDbError::Execution(format!(
                "corrupt file: bad ARRAY payload layout {other}"
            )));
        }
    }
    let mut out = match elem {
        crate::types::ArrayElem::Double => {
            crate::array::ArrayElems::Floats(Vec::with_capacity(values_len))
        }
        crate::types::ArrayElem::Integer | crate::types::ArrayElem::Bigint => {
            crate::array::ArrayElems::Ints(Vec::with_capacity(values_len))
        }
        crate::types::ArrayElem::Float => {
            crate::array::ArrayElems::Floats32(Vec::with_capacity(values_len))
        }
    };
    for chunk in values.chunks_exact(width) {
        match &mut out {
            crate::array::ArrayElems::Ints(v) => {
                if width == 4 {
                    v.push(i32::from_le_bytes(chunk.try_into().unwrap()) as i64);
                } else {
                    v.push(i64::from_le_bytes(chunk.try_into().unwrap()));
                }
            }
            crate::array::ArrayElems::Floats(v) => {
                v.push(f64::from_le_bytes(chunk.try_into().unwrap()))
            }
            crate::array::ArrayElems::Floats32(v) => {
                v.push(f32::from_le_bytes(chunk.try_into().unwrap()))
            }
        }
    }
    let valid = (!bits.is_empty()).then(|| bits.to_vec());
    Ok((
        VectorData::Array {
            elem,
            len,
            values: out,
            valid,
        },
        rows,
    ))
}

fn decode_payload(dtype: DataType, tag: u8, r: &mut Reader) -> Result<(VectorData, usize)> {
    match (dtype, tag) {
        (DataType::Array { len, .. }, ENC_ARRAY) => decode_array_payload(r, len),
        (DataType::Variant, ENC_PLAIN) => {
            let n = r.chunk_count("variant chunk length")?;
            let mut offsets = Vec::with_capacity(n + 1);
            for _ in 0..=n {
                offsets.push(r.u32()? as usize);
            }
            let total = *offsets.last().unwrap_or(&0);
            let data = r.take(total)?;
            let mut out = Vec::with_capacity(n);
            for w in offsets.windows(2) {
                let (from, to) = (w[0], w[1]);
                let bytes = data.get(from..to).ok_or_else(|| {
                    PinkDbError::Execution("corrupt file: bad VARIANT chunk offsets".to_string())
                })?;
                // A row with no bytes is NULL (validity says so); the
                // placeholder kind is irrelevant but must not be a decode
                // error.
                out.push(Arc::new(if bytes.is_empty() {
                    crate::variant::Variant::Null
                } else {
                    crate::variant::decode(bytes)?
                }));
            }
            Ok((VectorData::Variant(out), n))
        }
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
        // DECIMAL(p <= 18): the chunk was encoded as BIGINT (see
        // `encode_chunk_typed`); decode it that way and widen to i128.
        (DataType::Decimal { precision, scale }, _) if precision <= DECIMAL_I64_MAX_PRECISION => {
            let (data, n) = decode_payload(DataType::Bigint, tag, r)?;
            let VectorData::Int64(ints) = data else {
                return r.err("DECIMAL chunk did not decode as 64-bit integers");
            };
            Ok((
                VectorData::Decimal {
                    values: ints.into_iter().map(i128::from).collect(),
                    precision,
                    scale,
                },
                n,
            ))
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
            let nums: Vec<i64> = decode_delta(r, n, base)?;
            if dtype == DataType::Timestamp {
                Ok((VectorData::TimestampMicros(nums), n))
            } else {
                Ok((VectorData::Int64(nums), n))
            }
        }
        (DataType::Integer, ENC_FASTLANES) | (DataType::Date, ENC_FASTLANES) => {
            let n = r.chunk_count("chunk length")?;
            let nums = decode_fastlanes(r, n)?;
            let nums: Vec<i32> = nums.into_iter().map(|x| x as i32).collect();
            if dtype == DataType::Date {
                Ok((VectorData::Date32(nums), n))
            } else {
                Ok((VectorData::Int32(nums), n))
            }
        }
        (DataType::Bigint, ENC_FASTLANES) | (DataType::Timestamp, ENC_FASTLANES) => {
            let n = r.chunk_count("chunk length")?;
            let nums = decode_fastlanes(r, n)?;
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
                for c in fastlanes_unpack(r, n, width)? {
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
        (DataType::Varchar, ENC_FSST) => decode_fsst(r),
        (dtype, ENC_LZ_FRAME) => decode_compressed_frame(dtype, r),
        _ => Err(tag_error(dtype, tag)),
    }
}

/// Decode a compression frame (tag 8): `u32 raw_len | u8 base_tag | u8 codec |
/// u32 comp_len | comp`. The decompressed bytes are the inner payload, which
/// must be valid for the chunk's dtype and must consume the frame exactly.
///
/// `base_tag` is restricted to the payload tags (0..=6): the reserved and
/// frame tags (7..=9 and above) are refused so a frame cannot nest or
/// smuggle an unimplemented codec.
fn decode_compressed_frame(dtype: DataType, r: &mut Reader) -> Result<(VectorData, usize)> {
    let raw_len = r.u32()? as usize;
    let base_tag = r.u8()?;
    let codec = SecondStage::from_tag(r.u8()?)?;
    let comp_len = r.u32()? as usize;
    if !(4..=LZ_MAX_RAW).contains(&raw_len) {
        return r.err("bad compressed frame length");
    }
    // A framed payload wraps a plain encoding: the reserved tag and the
    // framed/entropy ones (LZ, FSST, fastlanes) cannot be a base.
    if base_tag > ENC_ARRAY
        || matches!(
            base_tag,
            ENC_DICT_RLE | ENC_LZ_FRAME | ENC_FSST | ENC_FASTLANES
        )
    {
        return r.err("unsupported compression base encoding");
    }
    let comp = r.take(comp_len)?;
    let raw = codec.decompress(comp, raw_len)?;
    let mut inner = Reader::new(&raw);
    let (data, n) = decode_payload(dtype, base_tag, &mut inner)?;
    if !inner.end() {
        return Err(PinkDbError::Execution(
            "corrupt file: trailing bytes in compressed frame".to_string(),
        ));
    }
    Ok((data, n))
}

/// Decode the FSST payload (tag 9): the offsets, the symbol table and the
/// escaped stream (which the second compression stage rebuilds).
fn decode_fsst(r: &mut Reader) -> Result<(VectorData, usize)> {
    let n = r.chunk_count("chunk length")?;
    let mut offsets = Vec::with_capacity(n + 1);
    for _ in 0..n + 1 {
        offsets.push(r.u32()? as usize);
    }
    let table_len = r.u8()? as usize;
    if table_len == 0 || table_len > FSST_MAX_TABLE + 1 {
        return r.err("bad FSST table length");
    }
    let table = r.take(table_len)?.to_vec();
    let raw_len = r.u32()? as usize;
    if raw_len > LZ_MAX_RAW {
        return r.err("bad FSST stream length");
    }
    let codec = SecondStage::from_tag(r.u8()?)?;
    let comp_len = r.u32()? as usize;
    let comp = r.take(comp_len)?;
    let data_len = *offsets.last().unwrap_or(&0);
    let data = if raw_len == 0 {
        Vec::new()
    } else {
        let escaped = codec.decompress(comp, raw_len)?;
        fsst_unescape(&escaped, &table, data_len)?
    };
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let (a, b) = (offsets[i], offsets[i + 1]);
        if b < a || b > data.len() {
            return r.err("bad string offsets");
        }
        let s = std::str::from_utf8(&data[a..b])
            .map_err(|_| PinkDbError::Execution("corrupt file: bad UTF-8".to_string()))?;
        out.push(Arc::from(s));
    }
    Ok((VectorData::VarChar(out), n))
}

/// Decode one framed chunk (validity + payload) into a [`Vector`].
fn decode_chunk_typed(dtype: DataType, tag: u8, r: &mut Reader) -> Result<Vector> {
    // Validity comes first (the encoder writes it before the payload); the
    // payload decode is what knows the row count.
    let vlen = r.u32()? as usize;
    let vbytes = r.take(vlen)?;
    let (data, n) = decode_payload(dtype, tag, r)?;
    let validity = decode_validity(vbytes, n)?;
    Ok(Vector::from_raw(data, validity))
}
// ---------- the footer: store metadata + the row-group directory ----------

/// One column chunk's directory entry: where its bytes are, how many pages
/// it holds, which encodings those pages use, its own CRC32C and its exact
/// statistics.
#[derive(Debug, Clone)]
struct ColumnChunkDir {
    /// The column chunk's absolute offset in the file.
    offset: u64,
    /// The column chunk's length in bytes (page index + pages).
    length: u64,
    /// The number of pages (engine chunks) in it.
    page_count: u32,
    /// A bitmask of the encoding tags the pages use.
    encodings: u32,
    /// The column chunk's CRC32C (over the page index and the pages).
    crc32c: u32,
    /// The exact NULL count of the chunk.
    null_count: u64,
    /// The exact NaN count of the chunk (doubles only).
    nan_count: u64,
    /// The exact minimum of the chunk (None when it has no non-NULL value).
    min: Option<Value>,
    /// The exact maximum of the chunk (None like `min`).
    max: Option<Value>,
}

/// One row group's directory entry: its row count and one entry per column.
#[derive(Debug, Clone)]
struct RowGroupDir {
    /// The row group's exact row count.
    rows: u64,
    /// One entry per column, in schema order.
    columns: Vec<ColumnChunkDir>,
}

/// One index definition (the map itself is always rebuilt from the rows).
#[derive(Debug, Clone)]
struct IndexDef {
    name: String,
    column: usize,
    distinct_keys: u64,
    indexed_rows: u64,
}

/// One table's footer entry: its schema, its catalog metadata and the
/// row-group directory of its data.
#[derive(Debug, Clone)]
struct TableDir {
    name: String,
    schema: Vec<(String, DataType)>,
    constraints: TableConstraints,
    defaults: Vec<Option<Value>>,
    indexes: Vec<IndexDef>,
    row_count: u64,
    row_groups: Vec<RowGroupDir>,
}

/// One database's footer entry: its tables and its catalog sections.
#[derive(Debug)]
struct DbDir {
    name: String,
    tables: Vec<TableDir>,
    macros: Vec<crate::macros::MacroDef>,
    materialized_views: Vec<crate::storage::MaterializedViewDef>,
    procedures: Vec<crate::procedure::ProcedureDef>,
    triggers: Vec<crate::trigger::TriggerDef>,
}

/// The parsed footer of a `PINKDB06` file.
#[derive(Debug)]
struct StoreFooter {
    databases: Vec<DbDir>,
    rbac: Option<crate::rbac::RbacCatalog>,
}

/// One encoded page (one engine chunk): its framed bytes and its statistics.
struct EncodedPage {
    /// The page frame: `u8 tag | u32 validity_len | validity | payload`.
    frame: Vec<u8>,
    /// The page's exact statistics.
    stats: ChunkColumnStats,
    /// The page's row count.
    rows: usize,
}

/// Encode one page (one engine chunk) from its [`Vector`].
fn encode_page(v: &Vector) -> EncodedPage {
    let (tag, payload) = encode_chunk_typed(v);
    let validity = encode_validity(v);
    let mut w = Writer::new();
    w.u8(tag);
    w.u32(validity.len() as u32);
    w.bytes(&validity);
    w.bytes(&payload);
    EncodedPage {
        frame: w.buf,
        stats: chunk_column_stats(v),
        rows: v.len(),
    }
}

/// Write one statistics value (the footer's per-chunk min/max): a tag byte
/// plus the value's payload.
fn write_stat_value(w: &mut Writer, v: Option<&Value>) {
    match v {
        None => w.u8(SV_NONE),
        Some(Value::Array(_)) => w.u8(SV_NONE),
        Some(Value::Boolean(b)) => {
            w.u8(SV_BOOL);
            w.u8(*b as u8);
        }
        Some(Value::Integer(i)) => {
            w.u8(SV_I32);
            w.i32(*i);
        }
        Some(Value::Bigint(i)) => {
            w.u8(SV_I64);
            w.i64(*i);
        }
        Some(Value::Double(x)) => {
            w.u8(SV_F64);
            w.u64(x.to_bits());
        }
        Some(Value::Varchar(s)) => {
            w.u8(SV_VARCHAR);
            w.str(s);
        }
        Some(Value::Date(d)) => {
            w.u8(SV_DATE);
            w.i32(*d);
        }
        Some(Value::Timestamp(t)) => {
            w.u8(SV_TIMESTAMP);
            w.i64(*t);
        }
        Some(Value::Decimal(x, scale)) => {
            w.u8(SV_DECIMAL);
            w.bytes(&x.to_le_bytes());
            w.u8(*scale);
        }
        Some(Value::Interval {
            months,
            days,
            micros,
        }) => {
            w.u8(SV_INTERVAL);
            w.i32(*months);
            w.i32(*days);
            w.i64(*micros);
        }
        Some(Value::Null) => w.u8(SV_NONE),
        // VARIANT columns carry no min/max: the total order spans kinds, so the
        // extremes prune nothing a reader would not compare anyway. Their
        // null_count is still exact.
        Some(Value::Variant(_)) => w.u8(SV_NONE),
    }
}

/// Read one statistics value (the reader side of [`write_stat_value`]).
fn read_stat_value(r: &mut Reader) -> Result<Option<Value>> {
    let tag = r.u8()?;
    let v = match tag {
        SV_NONE => None,
        SV_BOOL => Some(Value::Boolean(match r.u8()? {
            0 => false,
            1 => true,
            other => {
                return Err(PinkDbError::Execution(format!(
                    "corrupt file: bad BOOLEAN statistics byte {other}"
                )));
            }
        })),
        SV_I32 => Some(Value::Integer(r.i32()?)),
        SV_I64 => Some(Value::Bigint(r.i64()?)),
        SV_F64 => Some(Value::Double(f64::from_bits(r.u64()?))),
        SV_VARCHAR => Some(Value::Varchar(r.str()?)),
        SV_DATE => Some(Value::Date(r.i32()?)),
        SV_TIMESTAMP => Some(Value::Timestamp(r.i64()?)),
        SV_DECIMAL => {
            let b = r.take(16)?;
            let mut arr = [0u8; 16];
            arr.copy_from_slice(b);
            let x = i128::from_le_bytes(arr);
            let scale = r.u8()?;
            if scale > 38 {
                return r.err("bad DECIMAL statistics scale");
            }
            Some(Value::Decimal(x, scale))
        }
        SV_INTERVAL => Some(Value::Interval {
            months: r.i32()?,
            days: r.i32()?,
            micros: r.i64()?,
        }),
        other => {
            return Err(PinkDbError::Execution(format!(
                "corrupt file: unknown statistics value tag {other}"
            )));
        }
    };
    Ok(v)
}

/// The index definitions of one table (the map itself is never stored: the
/// loader rebuilds it from the decoded column).
fn index_defs(t: &DataTable) -> Vec<IndexDef> {
    let mut indexes: Vec<&Index> = t
        .indexes
        .iter()
        .filter(|i| i.column < t.columns.len())
        .collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    indexes
        .into_iter()
        .map(|index| {
            let column = &t.columns[index.column];
            let distinct = if t.indexes_stale {
                crate::storage::index_map_for(column).len()
            } else {
                index.map.len()
            };
            let indexed: usize = column.chunks.iter().map(|c| c.len() - c.null_count()).sum();
            IndexDef {
                name: index.name.clone(),
                column: index.column,
                distinct_keys: distinct as u64,
                indexed_rows: indexed as u64,
            }
        })
        .collect()
}

/// The metadata image's index entries: the same definitions with their row
/// statistics cleared. The statistics describe rows the image does not carry
/// (they live in the lake's data files, whose chunks the loader appends after
/// decoding), and the `.pink` reader refuses statistics larger than the table.
fn index_defs_without_rows(t: &DataTable) -> Vec<IndexDef> {
    index_defs(t)
        .into_iter()
        .map(|def| IndexDef {
            distinct_keys: 0,
            indexed_rows: 0,
            ..def
        })
        .collect()
}

/// Write one table's index definitions (the reader side of [`index_defs`]).
fn write_index_defs(w: &mut Writer, defs: &[IndexDef]) {
    w.u32(defs.len() as u32);
    for d in defs {
        w.str(&d.name);
        w.u32(d.column as u32);
        w.u64(d.distinct_keys);
        w.u64(d.indexed_rows);
    }
}

/// Read one table's index definitions.
fn read_index_defs(r: &mut Reader) -> Result<Vec<IndexDef>> {
    let n = r.u32()? as usize;
    // Each index costs at least 24 bytes (name length, column, two counts).
    if n > r.remaining() / 24 {
        return r.err("index count exceeds the remaining file");
    }
    let mut out: Vec<IndexDef> = Vec::with_capacity(n);
    for _ in 0..n {
        let name = r.str()?;
        if name.is_empty() {
            return r.err("empty index name");
        }
        if let Some(prev) = out.last()
            && prev.name >= name
        {
            return r.err(&format!(
                "index \"{name}\" is duplicated or out of name order"
            ));
        }
        let column = r.u32()? as usize;
        let distinct_keys = r.u64()?;
        let indexed_rows = r.u64()?;
        out.push(IndexDef {
            name,
            column,
            distinct_keys,
            indexed_rows,
        });
    }
    Ok(out)
}

/// Rebuild one table's index maps from its decoded rows and install the
/// definitions.
///
/// The maps are **never** read from the file: an index can therefore never
/// disagree with the rows it indexes, whatever the file claims. The
/// statistics are only bounded by the table's real row count.
fn apply_indexes(t: &mut DataTable, defs: &[IndexDef]) -> Result<()> {
    let mut out: Vec<Index> = Vec::with_capacity(defs.len());
    for def in defs {
        let Some(col) = t.columns.get(def.column) else {
            return Err(PinkDbError::Execution(format!(
                "corrupt file: index \"{}\" names column {} of a {}-column table",
                def.name,
                def.column,
                t.columns.len()
            )));
        };
        if def.indexed_rows > t.num_rows() as u64 || def.distinct_keys > def.indexed_rows {
            return Err(PinkDbError::Execution(format!(
                "corrupt file: index \"{}\" statistics exceed the table",
                def.name
            )));
        }
        out.push(Index {
            name: def.name.clone(),
            column: def.column,
            map: crate::storage::index_map_for(col),
        });
    }
    t.indexes = out;
    t.indexes_stale = false;
    Ok(())
}

// ---------- the public API ----------

impl PinkDb {
    /// Serialize every database (and every table in it) in the `.pink`
    /// columnar format (`PINKDB06`). Databases and tables are stored in
    /// sorted name order (deterministic bytes). Views are session-local and
    /// never persisted.
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.to_bytes_inner(true, false, false)?.0)
    }

    /// The engine catalog image (wave 96b): every database, macro, materialized
    /// view, procedure, trigger and RBAC entry, with each table's schema,
    /// constraints, defaults and index definitions but **no rows**. The lake
    /// holds the rows; this is the metadata a stateless server restores from
    /// the catalog.
    pub(crate) fn to_metadata_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.to_bytes_inner(true, false, true)?.0)
    }

    /// [`PinkDb::to_bytes`] with the chunk-parallel encode disabled (test
    /// switch: both paths must produce identical bytes).
    #[cfg(test)]
    fn to_bytes_impl(&self, parallel: bool) -> Result<Vec<u8>> {
        Ok(self.to_bytes_inner(parallel, false, false)?.0)
    }

    /// The one serialization path: `PINKDB06` bytes, optionally with the
    /// exact per-table statistics collected in the same chunk pass. The
    /// statistics never influence the bytes (`to_bytes_inner(_, false).0` is
    /// bit-identical to `to_bytes_inner(_, true).0`).
    fn to_bytes_inner(
        &self,
        parallel: bool,
        collect_stats: bool,
        omit_rows: bool,
    ) -> Result<(Vec<u8>, Vec<SavedTableStats>)> {
        // The optional catalog sections exist iff some object needs them (the
        // flags word is written in the footer, so decide it up front).
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
        let with_triggers = self.has_triggers();
        let with_macros = self.has_macros();
        let with_rbac = self.has_rbac_data();
        let with_indexes = self.list_databases().iter().any(|db| {
            self.list_tables_in(db)
                .iter()
                .any(|t| self.table_in(db, t).is_ok_and(|t| !t.indexes.is_empty()))
        });
        let flags = if with_constraints {
            FLAG_CONSTRAINTS
        } else {
            0
        } | if with_defaults { FLAG_DEFAULTS } else { 0 }
            | if with_mvs { FLAG_MATERIALIZED_VIEWS } else { 0 }
            | if with_indexes { FLAG_INDEXES } else { 0 }
            | if with_procedures { FLAG_PROCEDURES } else { 0 }
            | if with_triggers { FLAG_TRIGGERS } else { 0 }
            | if with_macros { FLAG_MACROS } else { 0 }
            | if with_rbac { FLAG_RBAC } else { 0 };

        // The data region.
        let mut data = Writer::new();
        let mut saved: Vec<SavedTableStats> = Vec::new();
        let mut dbs = self.list_databases();
        dbs.sort();
        let mut db_dirs: Vec<DbDir> = Vec::with_capacity(dbs.len());
        for dbname in &dbs {
            let mut names = self.list_tables_in(dbname);
            names.sort();
            let mut tables: Vec<TableDir> = Vec::with_capacity(names.len());
            for name in &names {
                let t = self.table_in(dbname, name)?;
                // Wave 96b: the engine catalog image carries schemas and
                // table metadata only; the rows live in the lake.
                let dir = if omit_rows {
                    TableDir {
                        name: name.clone(),
                        schema: t.schema(),
                        constraints: t.constraints.clone(),
                        defaults: t.columns.iter().map(|c| c.default.clone()).collect(),
                        indexes: index_defs_without_rows(t),
                        row_count: 0,
                        row_groups: Vec::new(),
                    }
                } else {
                    write_table(
                        &mut data,
                        dbname,
                        name,
                        t,
                        parallel,
                        collect_stats,
                        &mut saved,
                    )?
                };
                tables.push(dir);
            }
            db_dirs.push(DbDir {
                name: dbname.clone(),
                tables,
                macros: if with_macros {
                    self.macros_in(dbname).into_iter().map(|(_, d)| d).collect()
                } else {
                    Vec::new()
                },
                materialized_views: if with_mvs {
                    self.materialized_views_in(dbname)
                        .into_iter()
                        .map(|(_, d)| d)
                        .collect()
                } else {
                    Vec::new()
                },
                procedures: if with_procedures {
                    self.procedures_in(dbname)
                        .into_iter()
                        .map(|(_, d)| d)
                        .collect()
                } else {
                    Vec::new()
                },
                // Creation order is the firing order, so the slice is written
                // as stored — never sorted.
                triggers: if with_triggers {
                    self.triggers_in(dbname).to_vec()
                } else {
                    Vec::new()
                },
            });
        }

        // The footer.
        let mut fw = Writer::new();
        fw.u32(FOOTER_VERSION);
        fw.u32(flags);
        fw.u32(db_dirs.len() as u32);
        for dbdir in &db_dirs {
            fw.str(&dbdir.name);
            fw.u32(dbdir.tables.len() as u32);
            for tdir in &dbdir.tables {
                fw.str(&tdir.name);
                fw.u32(tdir.schema.len() as u32);
                for (col, dtype) in &tdir.schema {
                    fw.str(col);
                    fw.u8(dtype_tag(*dtype));
                    if let DataType::Decimal { precision, scale } = dtype {
                        fw.u8(*precision);
                        fw.u8(*scale);
                    }
                    if let DataType::Array { elem, len } = dtype {
                        fw.u8(elem.tag());
                        fw.u32(*len);
                    }
                }
                if with_constraints {
                    write_constraints(&mut fw, &tdir.constraints);
                }
                if with_defaults {
                    write_defaults(&mut fw, &tdir.defaults);
                }
                if with_indexes {
                    write_index_defs(&mut fw, &tdir.indexes);
                }
                fw.u64(tdir.row_count);
                fw.u32(tdir.row_groups.len() as u32);
                for rg in &tdir.row_groups {
                    fw.u64(rg.rows);
                    for col in &rg.columns {
                        fw.u64(col.offset);
                        fw.u64(col.length);
                        fw.u32(col.page_count);
                        fw.u32(col.encodings);
                        fw.u32(col.crc32c);
                        fw.u64(col.null_count);
                        fw.u64(col.nan_count);
                        write_stat_value(&mut fw, col.min.as_ref());
                        write_stat_value(&mut fw, col.max.as_ref());
                    }
                }
            }
            if with_macros {
                write_macro_defs(&mut fw, &dbdir.macros);
            }
            if with_mvs {
                write_materialized_view_defs(&mut fw, &dbdir.materialized_views);
            }
            if with_procedures {
                write_procedure_defs(&mut fw, &dbdir.procedures);
            }
            if with_triggers {
                write_trigger_defs(&mut fw, &dbdir.triggers);
            }
        }
        if with_rbac {
            write_rbac(&mut fw, self);
        }
        let footer = fw.buf;
        // The footer, then the trailer: the footer's CRC32C, its length and
        // the magic.
        let crc = crc32c(&footer);
        data.bytes(&footer);
        data.u32(crc);
        data.u64(footer.len() as u64);
        data.bytes(MAGIC_V6);
        Ok((data.buf, saved))
    }

    /// The bytes [`PinkDb::save`] would write: every database in the `.pink`
    /// columnar format, databases and tables in sorted name order
    /// (deterministic bytes), views never persisted.
    ///
    /// For writers that need the serialized image in memory instead of a
    /// file on disk (pink-lake's `.pink` data files). The same fail-stop
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
    /// For the `.lake` layer: a `pinklake` save publishes these facts in the
    /// manifest's statistics block, and `.lake attach` answers exact
    /// `COUNT`/`MIN`/`MAX` from them without decoding a data file.
    pub fn to_pink_bytes_with_stats(&self) -> Result<(Vec<u8>, Vec<SavedTableStats>)> {
        self.ensure_wal_healthy()?;
        self.to_bytes_inner(true, true, false)
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

    /// [`PinkDb::save`] with at-rest encryption: the `PINKDB06` bytes are
    /// sealed into a `PINKENC1` container (AES-256-GCM, Argon2id for
    /// passphrases). Without the `encryption` feature this fails with a
    /// clear "not compiled in" error instead of writing plaintext.
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
        // M-5: checkpoints (and the `.tmp` staging file they are written to)
        // are owner-only, never umask-derived.
        crate::fsutil::tighten(path);
        let mut f = crate::fsutil::create_private(path)?;
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
            let mut f = crate::fsutil::create_new_private(&tmp)?;
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
    ///
    /// Only the current format (`PINKDB06`) is readable: a file written by
    /// an older pinkdb is refused with a clear "written by an older pinkdb"
    /// error. Truncated or malformed files return errors, never panics. An
    /// encrypted (`PINKENC1`) file is rejected: without the `encryption`
    /// feature the error says to rebuild with it; with the feature it points
    /// at [`PinkDb::open_with_key`].
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

    /// Load a database from an in-memory `.pink` image (the bytes
    /// [`PinkDb::to_pink_bytes`] returns, or a `.pink` file read into memory).
    ///
    /// A thin wrapper over the same loader [`PinkDb::open`] uses: only the
    /// current format is accepted, a footer/chunk checksum mismatch or any
    /// malformed section is a clean error (never a panic), and an image
    /// written by an older pinkdb is refused with the "written by an older
    /// pinkdb" message. An encrypted (`PINKENC1`) container is not a plain
    /// image and is refused as a bad magic.
    pub fn from_pink_bytes(data: &[u8]) -> Result<PinkDb> {
        Self::load_bytes(data)
    }

    /// Decode a serialized `.pink` image (`PINKDB06`).
    fn load_bytes(data: &[u8]) -> Result<PinkDb> {
        let footer = read_store_footer(data)?;
        // Start from an empty store: a file may legitimately have no `main`
        // (it was dropped) and `open` must round-trip exactly.
        let mut db = PinkDb::with_databases(Vec::<String>::new());
        for dbdir in &footer.databases {
            db.create_database(&dbdir.name)?;
            for tdir in &dbdir.tables {
                db.create_table_with_constraints_in(
                    &dbdir.name,
                    &tdir.name,
                    tdir.schema.clone(),
                    tdir.constraints.clone(),
                )?;
                let t = db.table_mut_in(&dbdir.name, &tdir.name)?;
                for (col, d) in t.columns.iter_mut().zip(tdir.defaults.iter()) {
                    col.default = d.clone();
                }
            }
        }
        // The data region: every column chunk is verified on its own CRC32C
        // and decoded in parallel.
        for dbdir in &footer.databases {
            for tdir in &dbdir.tables {
                load_table_data(data, &mut db, &dbdir.name, tdir)?;
            }
        }
        // The index definitions (the maps are rebuilt from the rows).
        for dbdir in &footer.databases {
            for tdir in &dbdir.tables {
                if tdir.indexes.is_empty() {
                    continue;
                }
                let t = db.table_mut_in(&dbdir.name, &tdir.name)?;
                apply_indexes(t, &tdir.indexes)?;
            }
        }
        // The catalog sections: macros first (an MV's defining SQL may call
        // a macro and is rebound at load), then the materialized views, then
        // the procedures, then the store-global RBAC section.
        for dbdir in &footer.databases {
            for def in &dbdir.macros {
                db.macros_insert_in(&dbdir.name, def.name.to_ascii_lowercase(), def.clone())?;
            }
            for def in &dbdir.materialized_views {
                let mut def = def.clone();
                def.query = crate::mv::rebind_query(&db, &dbdir.name, &def.query_sql).ok();
                db.materialized_views_insert_in(&dbdir.name, def.name.to_ascii_lowercase(), def)?;
            }
            for def in &dbdir.procedures {
                db.procedures_insert_in(&dbdir.name, def.name.to_ascii_lowercase(), def.clone())?;
            }
            // Triggers last: a definition is only used when a statement fires
            // it, so its table must be loaded first (it already is).
            for def in &dbdir.triggers {
                db.triggers_insert_in(&dbdir.name, def.clone())?;
            }
        }
        if let Some(rbac) = footer.rbac {
            *db.rbac_mut() = rbac;
        }
        if db.list_databases().is_empty() {
            db.create_database("main")?;
        }
        // The session starts on `main` when it exists, else the first
        // database in sorted order.
        let current = db.fallback_db();
        db.set_current_db(&current);
        Ok(db)
    }
}

/// Whether `bytes` is a `PINKDB06` image (the trailer magic).
///
/// The magic lives at the **end** of the file: the last 20 bytes are the
/// trailer (`footer_crc32c u32 | footer_len u64 | "PINKDB06"`).
pub fn is_pinkdb06(bytes: &[u8]) -> bool {
    bytes.len() >= TRAILER_LEN && &bytes[bytes.len() - 8..] == MAGIC_V6
}

/// Write one table's data region (its column chunks) and collect its footer
/// entry.
fn write_table(
    data: &mut Writer,
    dbname: &str,
    name: &str,
    t: &DataTable,
    parallel: bool,
    collect_stats: bool,
    saved: &mut Vec<SavedTableStats>,
) -> Result<TableDir> {
    let schema = t.schema();
    let n_chunks = t.num_chunks();
    // Share the chunk vectors first (Arc clones only), then encode per
    // chunk. Order-preserving collect: the bytes are identical to a
    // serial pass.
    let mut scans: Vec<Vec<Arc<Vector>>> = Vec::with_capacity(n_chunks);
    for chunk_idx in 0..n_chunks {
        let chunk = t
            .scan_chunk(chunk_idx)
            .ok_or_else(|| PinkDbError::Execution("storage changed during save".to_string()))?;
        scans.push(chunk.columns);
    }
    // The string layout stores offsets as `u32`; a chunk whose total
    // VARCHAR bytes exceed that cannot be encoded faithfully (the
    // offsets would wrap and the reader would reject the file). Refuse
    // before writing instead of emitting a corrupt file.
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
    // Encode every page (one engine chunk per column) in parallel.
    let pages: Vec<Vec<EncodedPage>> = if parallel && n_chunks >= PARALLEL_SAVE_MIN_CHUNKS {
        scans
            .par_iter()
            .map(|cols| cols.iter().map(|v| encode_page(v)).collect())
            .collect()
    } else {
        scans
            .iter()
            .map(|cols| cols.iter().map(|v| encode_page(v)).collect())
            .collect()
    };
    // The per-column statistics of the whole table (the lake's stats
    // block).
    let mut accs: Vec<ColumnStatsAcc> = schema.iter().map(|_| ColumnStatsAcc::new()).collect();
    for chunk in &pages {
        for (acc, page) in accs.iter_mut().zip(chunk.iter()) {
            acc.merge(&page.stats);
        }
    }
    if collect_stats {
        let mut entry = SavedTableStats {
            db: dbname.to_string(),
            table: name.to_string(),
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
                    paths: Vec::new(),
                })
                .collect(),
        };
        variant_path_stats(t, &mut entry);
        saved.push(entry);
    }
    // The pages, column by column: `by_column[ci]` holds the column's pages
    // in chunk order.
    let mut by_column: Vec<Vec<&EncodedPage>> = vec![Vec::with_capacity(n_chunks); schema.len()];
    for chunk in &pages {
        for (ci, page) in chunk.iter().enumerate() {
            by_column[ci].push(page);
        }
    }
    // The row groups: 60 engine chunks each.
    let n_groups = n_chunks.div_ceil(CHUNKS_PER_ROW_GROUP);
    let mut row_groups: Vec<RowGroupDir> = Vec::with_capacity(n_groups);
    for g in 0..n_groups {
        let first = g * CHUNKS_PER_ROW_GROUP;
        let last = (first + CHUNKS_PER_ROW_GROUP).min(n_chunks);
        let mut columns: Vec<ColumnChunkDir> = Vec::with_capacity(schema.len());
        let rows: u64 = by_column
            .first()
            .map(|c| c[first..last].iter().map(|p| p.rows as u64).sum())
            .unwrap_or(0);
        for column_pages in &by_column {
            // The column chunk: the page index, then the pages.
            let group = &column_pages[first..last];
            let page_count = group.len() as u32;
            let index_len = 4 + page_count as usize * 8;
            let mut chunk = Writer::new();
            chunk.u32(page_count);
            let mut rel = index_len as u32;
            for page in group {
                let len = page.frame.len() as u32;
                chunk.u32(rel);
                chunk.u32(len);
                rel += len;
            }
            for page in group {
                chunk.bytes(&page.frame);
            }
            let bytes = chunk.buf;
            let crc = crc32c(&bytes);
            let offset = data.buf.len() as u64;
            let length = bytes.len() as u64;
            data.bytes(&bytes);
            // The chunk's exact statistics and encoding summary.
            let mut acc = ColumnStatsAcc::new();
            let mut encodings = 0u32;
            for page in group {
                acc.merge(&page.stats);
                encodings |= 1u32 << page.frame[0];
            }
            columns.push(ColumnChunkDir {
                offset,
                length,
                page_count,
                encodings,
                crc32c: crc,
                null_count: acc.null_count,
                nan_count: acc.nan_count,
                min: acc.min,
                max: acc.max,
            });
        }
        row_groups.push(RowGroupDir { rows, columns });
    }
    Ok(TableDir {
        name: name.to_string(),
        schema,
        constraints: t.constraints.clone(),
        defaults: t.columns.iter().map(|c| c.default.clone()).collect(),
        indexes: index_defs(t),
        row_count: t.num_rows() as u64,
        row_groups,
    })
}

/// The footer's byte range in a `PINKDB06` image: `(offset, length)`.
///
/// A ranged reader needs the 20-byte trailer and then exactly this window:
/// two small requests, no read of the data region.
pub fn footer_span(bytes: &[u8]) -> Result<(u64, u64)> {
    let (offset, len) = footer_span_raw(bytes)?;
    Ok((offset as u64, len as u64))
}

/// The footer's byte range, without the format check (the internal helper).
fn footer_span_raw(bytes: &[u8]) -> Result<(usize, usize)> {
    if bytes.len() < TRAILER_LEN {
        return Err(PinkDbError::Execution(
            "not a .pink file (bad magic)".to_string(),
        ));
    }
    let trailer = &bytes[bytes.len() - TRAILER_LEN..];
    let len = u64::from_le_bytes([
        trailer[4],
        trailer[5],
        trailer[6],
        trailer[7],
        trailer[8],
        trailer[9],
        trailer[10],
        trailer[11],
    ]) as usize;
    let start = bytes
        .len()
        .checked_sub(TRAILER_LEN)
        .and_then(|s| s.checked_sub(len))
        .ok_or_else(|| {
            PinkDbError::Execution("corrupt file: the footer length is out of range".to_string())
        })?;
    Ok((start, len))
}

/// The footer's flags word: which optional catalog sections the image
/// carries (constraints, column defaults, materialized views, index
/// definitions, procedures, macros and RBAC).
pub fn footer_flags(bytes: &[u8]) -> Result<u32> {
    let (start, len) = footer_span_raw(bytes)?;
    let footer = &bytes[start..start + len];
    let mut r = Reader::new(footer);
    let version = r.u32()?;
    if version != FOOTER_VERSION {
        return Err(PinkDbError::Execution(format!(
            "unsupported .pink footer version {version}; upgrade pinkdb"
        )));
    }
    r.u32()
}

/// The refusal for a file written by an older pinkdb.
fn older_pinkdb_error(version: u8) -> PinkDbError {
    PinkDbError::Execution(format!(
        "this .pink file was written by an older pinkdb (PINKDB0{version}); \
         this build reads PINKDB06 only"
    ))
}

// ---------- the projected-read API (wave 102) ----------
//
// A reader that must not download a whole `.pink` file needs three things
// from this module: the footer's **location** (so it can fetch exactly the
// footer), the footer's **layout** (the row-group directory: where every
// column chunk is, and what it proves), and a way to **decode** the column
// chunks it fetched. The three functions below are that surface; they are
// the only ones a projected reader needs, and they are used by the lake's
// `pink` provider (crates/pink-lake).

/// One column chunk of a `PINKDB06` file, as a reader outside the crate
/// sees it: where its bytes are, and what they prove.
///
/// A chunk is self-describing: it carries its own CRC32C (so a reader
/// verifies exactly the bytes it fetched, with no whole-file checksum) and
/// its exact statistics (so a reader can prune the row group it belongs to
/// without decoding a single page).
#[derive(Debug, Clone)]
pub struct PinkColumnChunk {
    /// The chunk's absolute offset in the file.
    pub offset: u64,
    /// The chunk's length in bytes (the page index plus the pages).
    pub length: u64,
    /// The number of pages (engine chunks) in it.
    pub page_count: u32,
    /// A bitmask of the encoding tags the chunk's pages use.
    pub encodings: u32,
    /// The chunk's CRC32C, over the page index and the pages.
    pub crc32c: u32,
    /// The chunk's exact NULL count.
    pub null_count: u64,
    /// The chunk's exact NaN count (doubles only).
    pub nan_count: u64,
    /// The chunk's exact minimum (`None` when it holds no non-NULL value).
    pub min: Option<Value>,
    /// The chunk's exact maximum (`None` like `min`).
    pub max: Option<Value>,
}

/// One row group of a `PINKDB06` file: its row count and one entry per
/// column, in schema order.
#[derive(Debug, Clone)]
pub struct PinkRowGroup {
    /// The row group's exact row count.
    pub rows: u64,
    /// One entry per column, in schema order.
    pub columns: Vec<PinkColumnChunk>,
}

/// One table of a `PINKDB06` file: its identity, its physical schema and
/// its row-group directory.
#[derive(Debug, Clone)]
pub struct PinkTableLayout {
    /// The database the file's metadata names.
    pub db: String,
    /// The table the file's metadata names.
    pub table: String,
    /// The table's physical schema, in scan order.
    pub schema: Vec<(String, DataType)>,
    /// The table's exact row count.
    pub row_count: u64,
    /// The table's row groups, in file order.
    pub row_groups: Vec<PinkRowGroup>,
}

/// The parsed layout of a `PINKDB06` file: every table it holds, with its
/// schema and its row-group directory.
#[derive(Debug, Clone, Default)]
pub struct PinkLayout {
    /// The file's tables, in footer order.
    pub tables: Vec<PinkTableLayout>,
}

impl PinkLayout {
    /// The table the file's metadata names, if it holds one. The comparison
    /// is case-insensitive (the engine's catalog rules).
    pub fn table(&self, db: &str, table: &str) -> Option<&PinkTableLayout> {
        self.tables
            .iter()
            .find(|t| t.db.eq_ignore_ascii_case(db) && t.table.eq_ignore_ascii_case(table))
    }
}

/// One fetched byte window of a `PINKDB06` file: the bytes at `offset`.
///
/// A projected reader fetches exactly the byte ranges it needs and hands
/// them back here; [`decode_projected_columns`] slices the column chunks out
/// of them.
#[derive(Debug, Clone)]
pub struct PinkByteWindow {
    /// The window's offset in the file.
    pub offset: u64,
    /// The window's bytes.
    pub bytes: Vec<u8>,
}

/// The footer's location and checksum, read from a `PINKDB06` file's
/// 20-byte trailer.
///
/// Returns `(offset, length, crc32c)`: the footer's byte range in the file
/// and the CRC32C the trailer records for it. A reader that already knows
/// where the footer is (the lake's catalog records it, `format_version` 7)
/// needs no trailer read at all — this function is the fallback for a
/// reader that does not.
pub fn footer_location(bytes: &[u8]) -> Result<(u64, u64, u32)> {
    if bytes.len() < TRAILER_LEN {
        return Err(PinkDbError::Execution(
            "not a .pink file (bad magic)".to_string(),
        ));
    }
    let trailer = &bytes[bytes.len() - TRAILER_LEN..];
    check_magic(bytes, &trailer[12..20])?;
    let (start, len) = footer_span_raw(bytes)?;
    Ok((
        start as u64,
        len as u64,
        u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]),
    ))
}

/// Parse a `PINKDB06` file's 20-byte trailer: the footer's `(offset,
/// length)` and its CRC32C.
///
/// `trailer` is the file's last 20 bytes (`footer_crc32c u32 | footer_len
/// u64 | "PINKDB06"`); `file_len` is the file's total length (the trailer
/// names the footer's *length*, so its offset follows from it). A reader
/// that fetched only the trailer (the lake's ranged read) uses this instead
/// of [`footer_location`].
pub fn parse_trailer(trailer: &[u8], file_len: u64) -> Result<(u64, u64, u32)> {
    if trailer.len() < TRAILER_LEN {
        return Err(PinkDbError::Execution(
            "not a .pink file (bad magic)".to_string(),
        ));
    }
    if &trailer[12..20] != MAGIC_V6 {
        return Err(PinkDbError::Execution(
            "not a .pink file (bad magic)".to_string(),
        ));
    }
    let crc32c = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
    let length = u64::from_le_bytes([
        trailer[4],
        trailer[5],
        trailer[6],
        trailer[7],
        trailer[8],
        trailer[9],
        trailer[10],
        trailer[11],
    ]);
    let offset = file_len
        .checked_sub(TRAILER_LEN as u64)
        .and_then(|s| s.checked_sub(length))
        .ok_or_else(|| {
            PinkDbError::Execution("corrupt file: the footer length is out of range".to_string())
        })?;
    Ok((offset, length, crc32c))
}

/// Parse the footer region of a `PINKDB06` file into its layout.
///
/// `region` is the footer's bytes (the `length` bytes the trailer points
/// at). `crc32c` is the checksum the trailer records for them; pass `None`
/// to skip the check (a reader that fetched the footer region but not the
/// trailer — the lake's catalog-recorded location). A mismatch is refused
/// before a single byte of the footer is decoded.
pub fn read_layout(region: &[u8], crc32c: Option<u32>) -> Result<PinkLayout> {
    if let Some(want) = crc32c
        && crate::persist::crc32c(region) != want
    {
        return Err(PinkDbError::Execution(
            "corrupt file: the footer checksum does not match (the file is damaged)".to_string(),
        ));
    }
    let footer = parse_store_footer(region)?;
    Ok(PinkLayout {
        tables: footer
            .databases
            .iter()
            .flat_map(|db| {
                db.tables.iter().map(move |t| PinkTableLayout {
                    db: db.name.clone(),
                    table: t.name.clone(),
                    schema: t.schema.clone(),
                    row_count: t.row_count,
                    row_groups: t
                        .row_groups
                        .iter()
                        .map(|rg| PinkRowGroup {
                            rows: rg.rows,
                            columns: rg
                                .columns
                                .iter()
                                .map(|c| PinkColumnChunk {
                                    offset: c.offset,
                                    length: c.length,
                                    page_count: c.page_count,
                                    encodings: c.encodings,
                                    crc32c: c.crc32c,
                                    null_count: c.null_count,
                                    nan_count: c.nan_count,
                                    min: c.min.clone(),
                                    max: c.max.clone(),
                                })
                                .collect(),
                        })
                        .collect(),
                })
            })
            .collect(),
    })
}

/// Decode the given physical columns of one table from the given byte
/// windows.
///
/// `columns` are ascending physical column indices of `table` (the table's
/// own schema order). `row_groups` are the indices of the row groups to
/// decode, ascending (a reader that pruned row groups passes only the
/// survivors). `windows` are the byte ranges the reader fetched; together
/// they must cover every requested column chunk — a chunk that is not
/// covered is a clean error, never a silent empty column.
///
/// The decoded table carries the file's **full physical schema**: an
/// unprojected column is an all-NULL placeholder of the right length, so
/// the result is chunk-aligned with a full decode and can be merged into
/// one. The column chunks are decoded **in parallel** and assembled in
/// file order, so the values and their order are exactly a full decode's.
pub fn decode_projected_columns(
    table: &PinkTableLayout,
    columns: &[usize],
    row_groups: &[usize],
    windows: &[PinkByteWindow],
) -> Result<DataTable> {
    let n_cols = table.schema.len();
    if columns.is_empty()
        || columns.iter().any(|&i| i >= n_cols)
        || columns.windows(2).any(|w| w[0] >= w[1])
    {
        return Err(PinkDbError::Execution(format!(
            "corrupt file: invalid column projection {columns:?} for the {n_cols}-column table \
             \"{}.{}\"",
            table.db, table.table
        )));
    }
    if row_groups.windows(2).any(|w| w[0] >= w[1])
        || row_groups.iter().any(|&g| g >= table.row_groups.len())
    {
        return Err(PinkDbError::Execution(format!(
            "corrupt file: invalid row-group selection {row_groups:?} for the table \
             \"{}.{}\"",
            table.db, table.table
        )));
    }
    // The pages of one column, across the selected row groups: `pages[column]`
    // is the column's page list, in file order.
    let pages: Vec<Vec<Arc<Vector>>> = columns
        .par_iter()
        .map(|&ci| {
            row_groups
                .par_iter()
                .map(|&group| {
                    let rg = &table.row_groups[group];
                    let dir = &rg.columns[ci];
                    let bytes = window_bytes(windows, dir.offset, dir.length)?;
                    decode_chunk_bytes(bytes, dir.page_count, dir.crc32c, table.schema[ci].1)
                })
                .collect::<Result<Vec<Vec<Arc<Vector>>>>>()
                .map(|groups| groups.into_iter().flatten().collect())
        })
        .collect::<Result<Vec<_>>>()?;
    // The page boundaries are shared by every column (the writer's
    // invariant), so the pages line up: page `p` of one column holds the
    // same rows as page `p` of another.
    let page_counts: Vec<usize> = pages.iter().map(|p| p.len()).collect();
    if page_counts.windows(2).any(|w| w[0] != w[1]) {
        return Err(PinkDbError::Execution(format!(
            "corrupt file: the columns of table \"{}.{}\" disagree on their page count \
             ({page_counts:?})",
            table.db, table.table
        )));
    }
    let total_pages = page_counts.first().copied().unwrap_or(0);
    let total_rows: usize = row_groups
        .iter()
        .map(|&g| table.row_groups[g].rows as usize)
        .sum();
    if row_groups.len() == table.row_groups.len() && total_rows != table.row_count as usize {
        return Err(PinkDbError::Execution(format!(
            "corrupt file: table \"{}.{}\" claims {} rows but its row groups hold {total_rows}",
            table.db, table.table, table.row_count
        )));
    }
    // The row count of every page (shared by all columns: the writer's
    // invariant). An unprojected column is one NULL chunk per page, so the
    // table stays chunk-aligned with a full decode.
    let page_lengths: Vec<usize> = pages
        .first()
        .map(|p| p.iter().map(|v| v.len()).collect())
        .unwrap_or_default();
    // Assemble the table: the projected columns carry their decoded pages,
    // the others an all-NULL placeholder of the same length.
    let mut out: Vec<crate::storage::Column> = Vec::with_capacity(n_cols);
    for (ci, (name, dtype)) in table.schema.iter().enumerate() {
        let mut chunks: Vec<Arc<Vector>> = Vec::with_capacity(total_pages);
        match columns.iter().position(|&c| c == ci) {
            Some(slot) => {
                for page in &pages[slot] {
                    chunks.push(Arc::clone(page));
                }
            }
            None => {
                for &len in &page_lengths {
                    chunks.push(Arc::new(null_vector(*dtype, len)));
                }
            }
        }
        out.push(crate::storage::Column {
            name: name.clone(),
            dtype: *dtype,
            default: None,
            chunks,
        });
    }
    Ok(DataTable::from_columns(
        table.table.clone(),
        out,
        total_rows,
    ))
}

/// The bytes of one column chunk, sliced out of the reader's windows.
fn window_bytes(windows: &[PinkByteWindow], offset: u64, length: u64) -> Result<&[u8]> {
    let end = offset.checked_add(length).ok_or_else(|| {
        PinkDbError::Execution("corrupt file: the column chunk range overflows".to_string())
    })?;
    for window in windows {
        let wend = window.offset.saturating_add(window.bytes.len() as u64);
        if window.offset <= offset && end <= wend {
            let from = usize::try_from(offset - window.offset).map_err(|_| {
                PinkDbError::Execution(
                    "corrupt file: the column chunk offset is out of range".to_string(),
                )
            })?;
            let to = from
                + usize::try_from(length).map_err(|_| {
                    PinkDbError::Execution(
                        "corrupt file: the column chunk length is out of range".to_string(),
                    )
                })?;
            return Ok(&window.bytes[from..to]);
        }
    }
    Err(PinkDbError::Execution(format!(
        "corrupt file: the column chunk at offset {offset} ({length} bytes) was not fetched"
    )))
}

/// An all-NULL vector of `dtype` with `rows` rows: the placeholder an
/// unprojected column holds in a projected decode. No reader ever sees its
/// values (it exists so the table's chunking is identical to a full
/// decode's).
fn null_vector(dtype: DataType, rows: usize) -> Vector {
    let data = match dtype {
        DataType::Boolean => VectorData::Boolean(vec![false; rows]),
        DataType::Integer => VectorData::Int32(vec![0; rows]),
        DataType::Bigint => VectorData::Int64(vec![0; rows]),
        DataType::Double => VectorData::Float64(vec![0.0; rows]),
        DataType::Varchar => VectorData::VarChar(vec![Arc::from(""); rows]),
        DataType::Date => VectorData::Date32(vec![0; rows]),
        DataType::Timestamp => VectorData::TimestampMicros(vec![0; rows]),
        DataType::Decimal { precision, scale } => VectorData::Decimal {
            values: vec![0; rows],
            precision,
            scale,
        },
        DataType::Interval => VectorData::Interval(vec![
            crate::types::Interval {
                months: 0,
                days: 0,
                micros: 0,
            };
            rows
        ]),
        DataType::Variant => {
            let null = Arc::new(crate::variant::Variant::Null);
            VectorData::Variant(vec![null; rows])
        }
        DataType::Array { elem, len } => {
            let n = rows * len as usize;
            VectorData::Array {
                elem,
                len,
                values: match elem {
                    crate::types::ArrayElem::Integer | crate::types::ArrayElem::Bigint => {
                        crate::array::ArrayElems::Ints(vec![0; n])
                    }
                    crate::types::ArrayElem::Double => {
                        crate::array::ArrayElems::Floats(vec![0.0; n])
                    }
                    crate::types::ArrayElem::Float => {
                        crate::array::ArrayElems::Floats32(vec![0.0; n])
                    }
                },
                valid: None,
            }
        }
    };
    // No validity words: every row reads NULL.
    Vector::from_raw(data, Vec::new())
}

/// Read (and verify) the footer of a `PINKDB06` image.
fn read_store_footer(data: &[u8]) -> Result<StoreFooter> {
    if data.len() < TRAILER_LEN {
        return Err(PinkDbError::Execution(
            "not a .pink file (bad magic)".to_string(),
        ));
    }
    let trailer = &data[data.len() - TRAILER_LEN..];
    let want_crc = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
    check_magic(data, &trailer[12..20])?;
    let (start, len) = footer_span_raw(data)?;
    let footer = &data[start..start + len];
    if crc32c(footer) != want_crc {
        return Err(PinkDbError::Execution(
            "corrupt file: the footer checksum does not match (the file is damaged)".to_string(),
        ));
    }
    parse_store_footer(footer)
}

/// Check a file's magic: the trailer's for a `PINKDB06` image, the file's
/// first bytes for an older layout (whose magic sat at the start).
///
/// The two refusals are the format's contract: an older pinkdb's file is
/// never guessed at, and a newer one asks for an upgrade.
fn check_magic(data: &[u8], trailer_magic: &[u8]) -> Result<()> {
    if trailer_magic == MAGIC_V6 {
        return Ok(());
    }
    // A file from a newer pinkdb?
    if trailer_magic.starts_with(b"PINKDB0")
        && trailer_magic[7].is_ascii_digit()
        && trailer_magic[7] > b'6'
    {
        return Err(PinkDbError::Execution(format!(
            "written by a newer pinkdb (format v{}); upgrade pinkdb",
            trailer_magic[7] - b'0'
        )));
    }
    // A file from an older pinkdb? Its magic is at the *start*.
    if data.len() >= 8 && data.starts_with(b"PINKDB0") && data[7].is_ascii_digit() {
        let v = data[7];
        if v <= b'6' {
            return Err(older_pinkdb_error(v - b'0'));
        }
        return Err(PinkDbError::Execution(format!(
            "written by a newer pinkdb (format v{}); upgrade pinkdb",
            v - b'0'
        )));
    }
    Err(PinkDbError::Execution(
        "not a .pink file (bad magic)".to_string(),
    ))
}

/// Parse a `PINKDB06` footer region (the bytes the trailer points at) into
/// the store's metadata and row-group directory. The region's checksum is
/// **not** checked here — the caller checks it (see [`read_store_footer`]
/// and [`read_layout`]).
fn parse_store_footer(footer: &[u8]) -> Result<StoreFooter> {
    let mut r = Reader::new(footer);
    let version = r.u32()?;
    if version != FOOTER_VERSION {
        return Err(PinkDbError::Execution(format!(
            "unsupported .pink footer version {version}; upgrade pinkdb"
        )));
    }
    let flags = r.u32()?;
    if flags
        & !(FLAG_CONSTRAINTS
            | FLAG_DEFAULTS
            | FLAG_MATERIALIZED_VIEWS
            | FLAG_INDEXES
            | FLAG_PROCEDURES
            | FLAG_MACROS
            | FLAG_TRIGGERS
            | FLAG_RBAC)
        != 0
    {
        return Err(PinkDbError::Execution(format!(
            "unsupported .pink footer flags 0x{flags:08x}; upgrade pinkdb"
        )));
    }
    let constraints = flags & FLAG_CONSTRAINTS != 0;
    let defaults = flags & FLAG_DEFAULTS != 0;
    let indexes = flags & FLAG_INDEXES != 0;
    let macros = flags & FLAG_MACROS != 0;
    let mvs = flags & FLAG_MATERIALIZED_VIEWS != 0;
    let procedures = flags & FLAG_PROCEDURES != 0;
    let triggers = flags & FLAG_TRIGGERS != 0;
    let rbac = flags & FLAG_RBAC != 0;

    let n_dbs = r.u32()? as usize;
    // Each database costs at least 8 bytes (its name's length prefix).
    if n_dbs > r.remaining() / 8 + 1 {
        return r.err("database count exceeds the remaining file");
    }
    let mut databases = Vec::with_capacity(n_dbs);
    for _ in 0..n_dbs {
        let name = r.str()?;
        let n_tables = r.u32()? as usize;
        if n_tables > r.remaining() / 8 + 1 {
            return r.err("table count exceeds the remaining file");
        }
        let mut tables = Vec::with_capacity(n_tables);
        for _ in 0..n_tables {
            let tname = r.str()?;
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
                            PinkDbError::Execution(
                                "corrupt file: bad DECIMAL precision/scale".to_string(),
                            )
                        })?
                    }
                    8 => DataType::Interval,
                    10 => {
                        let elem_tag = r.u8()?;
                        let elem =
                            crate::types::ArrayElem::from_tag(elem_tag).ok_or_else(|| {
                                PinkDbError::Execution(format!(
                                    "corrupt file: bad ARRAY element kind {elem_tag}"
                                ))
                            })?;
                        let len = r.u32()?;
                        crate::types::array_type(elem, len).ok_or_else(|| {
                            PinkDbError::Execution(
                                "corrupt file: bad ARRAY element count".to_string(),
                            )
                        })?
                    }
                    other => dtype_from_tag(other)?,
                };
                schema.push((col, dtype));
            }
            let tconstraints = if constraints {
                read_constraints(&mut r, n_cols)?
            } else {
                TableConstraints::default()
            };
            let tdefaults = if defaults {
                let decoded = read_defaults(&mut r, n_cols)?;
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
            let tindexes = if indexes {
                read_index_defs(&mut r)?
            } else {
                Vec::new()
            };
            let row_count = r.u64()?;
            let n_groups = r.u32()? as usize;
            // Each row group costs at least 8 bytes (its row count) plus one
            // entry per column.
            if n_groups > r.remaining() / 8 + 1 {
                return r.err("row-group count exceeds the remaining file");
            }
            let mut row_groups = Vec::with_capacity(n_groups);
            for _ in 0..n_groups {
                let rows = r.u64()?;
                let mut columns = Vec::with_capacity(n_cols);
                for _ in 0..n_cols {
                    let offset = r.u64()?;
                    let length = r.u64()?;
                    let page_count = r.u32()?;
                    let encodings = r.u32()?;
                    let crc = r.u32()?;
                    let null_count = r.u64()?;
                    let nan_count = r.u64()?;
                    let min = read_stat_value(&mut r)?;
                    let max = read_stat_value(&mut r)?;
                    columns.push(ColumnChunkDir {
                        offset,
                        length,
                        page_count,
                        encodings,
                        crc32c: crc,
                        null_count,
                        nan_count,
                        min,
                        max,
                    });
                }
                row_groups.push(RowGroupDir { rows, columns });
            }
            tables.push(TableDir {
                name: tname,
                schema,
                constraints: tconstraints,
                defaults: tdefaults,
                indexes: tindexes,
                row_count,
                row_groups,
            });
        }
        let dmacros = if macros {
            read_macro_defs(&mut r)?
        } else {
            Vec::new()
        };
        let dmvs = if mvs {
            read_materialized_view_defs(&mut r)?
        } else {
            Vec::new()
        };
        let dprocs = if procedures {
            read_procedure_defs(&mut r)?
        } else {
            Vec::new()
        };
        let dtriggers = if triggers {
            read_trigger_defs(&mut r, &name)?
        } else {
            Vec::new()
        };
        databases.push(DbDir {
            name,
            tables,
            macros: dmacros,
            materialized_views: dmvs,
            procedures: dprocs,
            triggers: dtriggers,
        });
    }
    let rbac = if rbac { Some(read_rbac(&mut r)?) } else { None };
    if !r.end() {
        return Err(PinkDbError::Execution(
            "corrupt file: trailing bytes after the footer".to_string(),
        ));
    }
    Ok(StoreFooter { databases, rbac })
}

/// Load one table's data region: every column chunk is read, its CRC32C is
/// verified and its pages are decoded. The columns are decoded in parallel.
fn load_table_data(data: &[u8], db: &mut PinkDb, dbname: &str, tdir: &TableDir) -> Result<()> {
    let n_cols = tdir.schema.len();
    // The decoded pages per column: `pages[column][row group][page]`.
    let decoded: Vec<Vec<Vec<Arc<Vector>>>> = (0..n_cols)
        .into_par_iter()
        .map(|ci| {
            tdir.row_groups
                .par_iter()
                .map(|rg| decode_column_chunk(data, &rg.columns[ci], tdir.schema[ci].1))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    // The row count must agree with the row groups.
    let total_rows: usize = tdir.row_groups.iter().map(|rg| rg.rows as usize).sum();
    if total_rows != tdir.row_count as usize {
        return Err(PinkDbError::Execution(format!(
            "corrupt file: table \"{}\" claims {} rows but its row groups hold {total_rows}",
            tdir.name, tdir.row_count
        )));
    }
    // Assemble the table chunk by chunk (every column shares the chunk
    // boundaries: the writer's invariant).
    let t = db.table_mut_in(dbname, &tdir.name)?;
    for (g, rg) in tdir.row_groups.iter().enumerate() {
        let n_pages = rg.columns[0].page_count as usize;
        for p in 0..n_pages {
            let chunk: Vec<Arc<Vector>> = decoded
                .iter()
                .map(|column| Arc::clone(&column[g][p]))
                .collect();
            t.append_shared_chunk(&chunk)?;
        }
    }
    Ok(())
}

/// Read one column chunk, verify its CRC32C and decode its pages.
fn decode_column_chunk(
    data: &[u8],
    dir: &ColumnChunkDir,
    dtype: DataType,
) -> Result<Vec<Arc<Vector>>> {
    let start = usize::try_from(dir.offset).map_err(|_| {
        PinkDbError::Execution("corrupt file: the column chunk offset is out of range".to_string())
    })?;
    let len = usize::try_from(dir.length).map_err(|_| {
        PinkDbError::Execution("corrupt file: the column chunk length is out of range".to_string())
    })?;
    let end = start.checked_add(len).ok_or_else(|| {
        PinkDbError::Execution("corrupt file: the column chunk range overflows".to_string())
    })?;
    if end > data.len() {
        return Err(PinkDbError::Execution(
            "corrupt file: the column chunk is outside the file".to_string(),
        ));
    }
    decode_chunk_bytes(&data[start..end], dir.page_count, dir.crc32c, dtype)
}

/// Decode one column chunk's bytes: verify its CRC32C, then decode its
/// pages (the page index plus the pages themselves).
///
/// The chunk's bytes are the *whole* chunk: the page index (the page count
/// plus one `(offset, length)` pair per page) followed by the pages. A
/// caller that fetched only the chunk (a projected, ranged reader) hands
/// exactly those bytes here.
fn decode_chunk_bytes(
    bytes: &[u8],
    expected_pages: u32,
    crc: u32,
    dtype: DataType,
) -> Result<Vec<Arc<Vector>>> {
    if crc32c(bytes) != crc {
        return Err(PinkDbError::Execution(
            "corrupt file: a column chunk fails its checksum (the file is damaged)".to_string(),
        ));
    }
    // The page index, then the pages.
    let mut r = Reader::new(bytes);
    let page_count = r.u32()? as usize;
    if page_count != expected_pages as usize {
        return Err(PinkDbError::Execution(
            "corrupt file: the column chunk's page count disagrees with the footer".to_string(),
        ));
    }
    // Each page-index entry is 8 bytes: never size an allocation from the
    // unvalidated count.
    if page_count > r.remaining() / 8 {
        return r.err("the column chunk's page count exceeds the chunk");
    }
    let mut index = Vec::with_capacity(page_count);
    for _ in 0..page_count {
        let off = r.u32()? as usize;
        let len = r.u32()? as usize;
        index.push((off, len));
    }
    let mut out: Vec<Arc<Vector>> = Vec::with_capacity(page_count);
    for (off, len) in index {
        let end = off.checked_add(len).ok_or_else(|| {
            PinkDbError::Execution("corrupt file: a page range overflows".to_string())
        })?;
        if end > bytes.len() {
            return Err(PinkDbError::Execution(
                "corrupt file: a page is outside the column chunk".to_string(),
            ));
        }
        let mut pr = Reader::new(&bytes[off..end]);
        let tag = pr.u8()?;
        let v = decode_chunk_typed(dtype, tag, &mut pr)?;
        if !pr.end() {
            return Err(PinkDbError::Execution(
                "corrupt file: trailing bytes after a page".to_string(),
            ));
        }
        out.push(Arc::new(v));
    }
    Ok(out)
}

// ---------- the catalog sections of the footer ----------

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

/// Decode one table's constraint section. `n_cols` bounds every column index;
/// almost-empty constraints and out-of-range indices are corrupt-file errors,
/// never panics.
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

/// Encode every column's DEFAULT (wave 65): one presence byte per column,
/// then the value bytes of the ones that have a default (a NULL default is
/// simply absent — it means the same on read).
fn write_defaults(w: &mut Writer, defaults: &[Option<Value>]) {
    for d in defaults {
        match d {
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
        Value::Variant(v) => {
            let mut bytes = Vec::new();
            crate::variant::encode(v, &mut bytes);
            w.u8(VD_VARIANT);
            w.u32(bytes.len() as u32);
            w.bytes(&bytes);
        }
        Value::Array(a) => {
            let bytes = crate::array::encode_cell(a);
            w.u8(VD_ARRAY);
            w.u32(bytes.len() as u32);
            w.bytes(&bytes);
        }
    }
}

/// Decode the per-column default section. Every tag is validated; an unknown
/// tag or a truncated payload is a corrupt-file error.
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
            VD_VARIANT => {
                let n = r.u32()? as usize;
                let bytes = r.take(n)?;
                match crate::variant::decode(bytes) {
                    Ok(v) => Value::Variant(Arc::new(v)),
                    Err(_) => return r.err("bad VARIANT default"),
                }
            }
            VD_ARRAY => {
                let n = r.u32()? as usize;
                let bytes = r.take(n)?;
                match crate::array::decode_cell(bytes) {
                    Ok(v) => Value::Array(Arc::new(v)),
                    Err(_) => return r.err("bad ARRAY default"),
                }
            }
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
        (Value::Variant(_), DataType::Variant) => true,
        _ => false,
    }
}

/// Policy tags in a materialized-view section (wave 80).
const MV_MANUAL: u8 = 0;
const MV_EVERY: u8 = 1;
const MV_ON_COMMIT: u8 = 2;

/// Encode one database's macro section: the count, then per macro, sorted by
/// lowercased name, `name, source` where `source` is the full `CREATE MACRO
/// ...` statement text. The **parsed template is not stored**: the loader
/// re-parses the statement, exactly like a procedure body or an MV
/// definition (a template cannot be rendered back to SQL).
fn write_macro_defs(w: &mut Writer, defs: &[crate::macros::MacroDef]) {
    w.u32(defs.len() as u32);
    for def in defs {
        w.str(&def.name);
        w.str(&def.source);
    }
}

/// Decode one database's macro section. A source text that is not exactly
/// one `CREATE MACRO` statement is a corrupt file (the executor refuses to
/// store a definition without its statement text).
fn read_macro_defs(r: &mut Reader) -> Result<Vec<crate::macros::MacroDef>> {
    let n = r.u32()? as usize;
    // Each macro costs at least 8 bytes (two empty string length prefixes);
    // never size an allocation from the unvalidated count.
    if n > r.remaining() / 8 + 1 {
        return r.err("macro count exceeds the remaining file");
    }
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let name = r.str()?;
        let source = r.str()?;
        let mut stmts = crate::sql::parse_statements(&source).map_err(|e| {
            PinkDbError::Execution(format!(
                "corrupt file: macro \"{name}\" does not parse: {e}"
            ))
        })?;
        if stmts.len() != 1 {
            return r.err(&format!(
                "macro \"{name}\" definition is not exactly one statement"
            ));
        }
        let Some(crate::sql::ast::Statement::CreateMacro {
            name: parsed_name,
            params,
            body,
            ..
        }) = stmts.pop()
        else {
            return r.err(&format!(
                "macro \"{name}\" definition is not a CREATE MACRO statement"
            ));
        };
        if !parsed_name.eq_ignore_ascii_case(&name) {
            return r.err(&format!(
                "macro section names \"{name}\" but its definition creates \"{parsed_name}\""
            ));
        }
        out.push(crate::macros::MacroDef {
            name: parsed_name,
            params: params
                .into_iter()
                .map(|p| crate::macros::MacroParamDef {
                    name: p.name,
                    default: p.default,
                })
                .collect(),
            body,
            source,
        });
    }
    Ok(out)
}

/// Encode one database's materialized-view section: the count, then per view,
/// sorted by lowercased name, `name, policy tag [, months, days, micros],
/// defining SQL, last refresh stamp (epoch ms), refresh count`. The **bound
/// query is not stored**: only the defining SQL text, which the loader
/// rebinds (a bound plan cannot be rendered back to SQL, and a definition
/// whose base tables are missing must still load).
fn write_materialized_view_defs(w: &mut Writer, defs: &[crate::storage::MaterializedViewDef]) {
    w.u32(defs.len() as u32);
    for def in defs {
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

/// Decode one database's materialized-view section. The defining query is
/// rebound when the definition is installed; a definition that cannot rebind
/// (missing base table/view) still loads, with no bound plan — a refresh
/// retries later and reports the failure, and `DROP MATERIALIZED VIEW` still
/// removes it.
fn read_materialized_view_defs(r: &mut Reader) -> Result<Vec<crate::storage::MaterializedViewDef>> {
    let n = r.u32()? as usize;
    // Each view costs at least 9 bytes (empty policy/name/… fields); do not
    // size an allocation from the unvalidated count.
    if n > r.remaining() / 9 + 1 {
        return r.err("materialized-view count exceeds the remaining file");
    }
    let mut out = Vec::with_capacity(n);
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
        out.push(crate::storage::MaterializedViewDef {
            name,
            query: None,
            query_sql,
            policy,
            last_refreshed_ms,
            // The attempt stamp is not persisted: treat the last success as
            // the last attempt (an `EVERY` view becomes due one full interval
            // after its last success).
            last_attempt_ms: last_refreshed_ms,
            refresh_count,
            last_error: None,
        });
    }
    Ok(out)
}

/// Encode one database's stored-procedure section: the count, then per
/// procedure, sorted by lowercased name, `name, language tag, parameter count
/// × (name, dtype tag [, precision, scale]), body text`. The **parsed body is
/// not stored**: only the text, which the loader re-parses (a body is
/// validated at `CREATE`, and a stored body that no longer parses is a
/// corrupt file). JavaScript bodies are opaque text and load in every build.
fn write_procedure_defs(w: &mut Writer, defs: &[crate::procedure::ProcedureDef]) {
    w.u32(defs.len() as u32);
    for def in defs {
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
            if let DataType::Array { elem, len } = dtype {
                w.u8(elem.tag());
                w.u32(*len);
            }
        }
        w.str(&def.source);
    }
}

/// Encode one database's trigger section: the count, then per trigger (in
/// creation order, which is the firing order) `name, source text`. Everything
/// else is re-derived by re-parsing `source` on load.
fn write_trigger_defs(w: &mut Writer, defs: &[crate::trigger::TriggerDef]) {
    w.u32(defs.len() as u32);
    for def in defs {
        w.str(&def.name);
        w.str(&def.source);
    }
}

/// Decode one database's trigger section, re-parsing each `CREATE TRIGGER`
/// text. A body that no longer parses is a corrupt file (it parsed when the
/// trigger was created).
fn read_trigger_defs(r: &mut Reader, dbname: &str) -> Result<Vec<crate::trigger::TriggerDef>> {
    let n = r.u32()? as usize;
    // Each trigger costs at least 8 bytes (two empty strings' length prefixes).
    if n > r.remaining() / 8 + 1 {
        return r.err("trigger count exceeds the remaining file");
    }
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let name = r.str()?;
        let source = r.str()?;
        let mut statements = crate::sql::parse_statements(&source).map_err(|e| {
            PinkDbError::Execution(format!(
                "corrupt file: trigger \"{name}\" does not parse: {e}"
            ))
        })?;
        let stmt = match statements.len() {
            1 => statements.remove(0),
            _ => {
                return Err(PinkDbError::Execution(format!(
                    "corrupt file: trigger \"{name}\" is not one statement"
                )));
            }
        };
        let mut def = crate::trigger::def_from_statement(dbname, &stmt).map_err(|e| {
            PinkDbError::Execution(format!(
                "corrupt file: trigger \"{name}\" does not parse: {e}"
            ))
        })?;
        def.source = source;
        out.push(def);
    }
    Ok(out)
}

/// Decode one database's stored-procedure section. A SQL body is re-parsed
/// here; a parse failure is a corrupt file (the body was validated when the
/// definition was created).
fn read_procedure_defs(r: &mut Reader) -> Result<Vec<crate::procedure::ProcedureDef>> {
    let n = r.u32()? as usize;
    // Each procedure costs at least 7 bytes (language, empty counts/strings);
    // do not size an allocation from the unvalidated count.
    if n > r.remaining() / 7 + 1 {
        return r.err("procedure count exceeds the remaining file");
    }
    let mut out = Vec::with_capacity(n);
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
            let dtype = match tag {
                7 => {
                    let precision = r.u8()?;
                    let scale = r.u8()?;
                    DataType::Decimal { precision, scale }
                }
                10 => {
                    let elem_tag = r.u8()?;
                    let elem = crate::types::ArrayElem::from_tag(elem_tag).ok_or_else(|| {
                        PinkDbError::Execution(format!(
                            "corrupt file: bad ARRAY element kind {elem_tag}"
                        ))
                    })?;
                    let len = r.u32()?;
                    crate::types::array_type(elem, len).ok_or_else(|| {
                        PinkDbError::Execution("corrupt file: bad ARRAY element count".to_string())
                    })?
                }
                other => dtype_from_tag(other)?,
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
                            "corrupt file: procedure \"{name}\" does not parse: {e}"
                        ))
                    })?,
                )
            }
            crate::procedure::ProcLanguage::JavaScript => None,
        };
        out.push(crate::procedure::ProcedureDef {
            name,
            params,
            language,
            source,
            body,
        });
    }
    Ok(out)
}

/// Encode the store-global RBAC section: the users (display name + Argon2id
/// PHC password hash), the roles, the role memberships, the table grants, the
/// database grants and the global `ADMIN` holders, each list sorted by its
/// canonical key.
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

/// Decode the store-global RBAC section.
///
/// Every count is validated against the bytes remaining before it sizes an
/// allocation (exactly like the other optional sections), unknown privilege
/// tags are a corrupt-file error, and a user's password hash must parse as a
/// PHC `argon2id` string — a store that loaded would otherwise hold a user
/// nobody can log in as. Grants that name a principal or table the catalog no
/// longer defines load as-is (they are inert): the file records what was
/// granted, not what exists today.
fn read_rbac(r: &mut Reader) -> Result<crate::rbac::RbacCatalog> {
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
    Ok(rbac)
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

/// Exact per-column statistics of one table's rows, collected without
/// encoding anything (wave 66).
///
/// `pink-lake` uses this to publish a data file's zone map in the catalog:
/// the same [`SavedTableStats`] facts a `.pink` encode collects during its
/// pass, computed here by one linear scan of the table's chunk payloads (no
/// per-row `Value` materialization except the per-chunk min/max). The facts
/// are ungated (`nan_count` and zero doubles are kept); `pink-lake` applies
/// the one exactness gate shared with its footer path, so the two cannot
/// drift apart.
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
    let mut saved = SavedTableStats {
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
                paths: Vec::new(),
            })
            .collect(),
    };
    variant_path_stats(src, &mut saved);
    saved
}

/// Fill in every VARIANT column's per-path statistics: one policy pass over
/// the whole column (so a typed path is a *complete* bound), then one
/// accumulate pass. Two passes keep the policy exact; the alternative
/// (sampling) would let a later row's other-kind value silently invalidate
/// min/max.
fn variant_path_stats(src: &DataTable, saved: &mut SavedTableStats) {
    use crate::variant::Variant;

    let mut policy_rows: Vec<Option<Vec<std::sync::Arc<Variant>>>> = Vec::new();
    for column in &src.columns {
        if column.dtype != DataType::Variant {
            policy_rows.push(None);
            continue;
        }
        let mut values: Vec<std::sync::Arc<Variant>> = Vec::new();
        for chunk in &column.chunks {
            let VectorData::Variant(items) = chunk.data() else {
                continue;
            };
            for (index, item) in items.iter().enumerate() {
                if chunk.is_valid(index) {
                    values.push(std::sync::Arc::clone(item));
                }
            }
        }
        policy_rows.push(Some(values));
    }

    for (index, values) in policy_rows.iter().enumerate() {
        let Some(values) = values else { continue };
        let refs: Vec<&Variant> = values.iter().map(|v| v.as_ref()).collect();
        let policy = crate::variant_shred::infer_policy(&refs);
        if policy.is_empty() {
            continue;
        }
        let mut acc = crate::variant_shred::PathStatsAcc::new(&policy);
        for chunk in &src.columns[index].chunks {
            let VectorData::Variant(items) = chunk.data() else {
                continue;
            };
            for (row, item) in items.iter().enumerate() {
                if chunk.is_valid(row) {
                    acc.see(item);
                } else {
                    acc.see_null();
                }
            }
        }
        saved.columns[index].paths = acc
            .finish()
            .into_iter()
            .map(|p| SavedPathStats {
                kind: p.kind.stats_type().to_string(),
                path: p.path,
                null_count: p.null_count,
                min: p
                    .min
                    .and_then(|v| crate::variant_shred::stats_bound_text(p.kind, &v)),
                max: p
                    .max
                    .and_then(|v| crate::variant_shred::stats_bound_text(p.kind, &v)),
            })
            .collect();
    }
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

/// The PLAIN payload of a vector (test helper: forced-tag round trips).
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
        DataType::Variant => variant_payload(v),
        DataType::Array { .. } => array_payload(v, 1),
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
        (DataType::Integer | DataType::Date, ENC_FASTLANES) => {
            if dtype == DataType::Date {
                match v.data() {
                    VectorData::Date32(d) => enc_ints_fastlanes(d.as_slice()),
                    _ => unreachable!(),
                }
            } else {
                enc_ints_fastlanes(&int32_payload(&v))
            }
        }
        (DataType::Bigint | DataType::Timestamp, ENC_FASTLANES) => {
            if dtype == DataType::Timestamp {
                match v.data() {
                    VectorData::TimestampMicros(d) => enc_ints_fastlanes(d.as_slice()),
                    _ => unreachable!(),
                }
            } else {
                enc_ints_fastlanes(&int64_payload(&v))
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
        (DataType::Varchar, ENC_FSST) => {
            enc_strs_fsst(&varchar_payload(&v)).expect("the FSST payload is always produced")
        }
        // Forced frame: bypass the size/probe gates (they only decide
        // *selection*), always wrap the plain payload.
        (_, ENC_LZ_FRAME) => {
            let base = plain_payload(&v);
            let stage = SecondStage::default_stage();
            let comp = stage.compress(&base);
            let mut w = Writer::new();
            w.u32(base.len() as u32);
            w.u8(ENC_PLAIN);
            w.u8(stage.tag());
            w.u32(comp.len() as u32);
            w.bytes(&comp);
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
    let decoded = decode_chunk_typed(dtype, t, &mut r)
        .unwrap_or_else(|e| panic!("CTX dtype={dtype:?} tag={t} n={} err={e}", values.len()));
    (0..decoded.len()).map(|i| decoded.get(i)).collect()
}

/// The pinned length and footer checksum of the golden corpus's `PINKDB06`
/// image (see `golden_pinkdb06_bytes_are_pinned`).
#[cfg(test)]
const GOLDEN_V6_LEN: usize = 113_321;
#[cfg(test)]
const GOLDEN_V6_FOOTER_CRC: u32 = 3_044_878_001;

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

    /// The CRC32C check value of the standard test vector.
    #[test]
    fn crc32c_matches_the_standard_check_value() {
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b"a"), 0xC1D0_4330);
    }

    fn roundtrip(db: &PinkDb) -> PinkDb {
        let path = temp_file("rt");
        db.save(path.to_str().unwrap()).unwrap();
        let data = std::fs::read(&path).unwrap();
        assert!(
            is_pinkdb06(&data),
            "the image must carry the PINKDB06 trailer"
        );
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

    /// The footer's bytes of an image (the window a ranged reader fetches).
    fn footer_bytes(data: &[u8]) -> Vec<u8> {
        let (offset, len) = footer_span(data).unwrap();
        data[offset as usize..(offset + len) as usize].to_vec()
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
    fn from_pink_bytes_round_trips_and_refuses_garbage() {
        let mut db = PinkDb::new();
        db.execute(
            "CREATE TABLE t (a INTEGER, b VARCHAR); \
             INSERT INTO t VALUES (1, 'x'), (2, NULL);",
        )
        .unwrap();
        let bytes = db.to_pink_bytes().unwrap();
        let mut back = PinkDb::from_pink_bytes(&bytes).unwrap();
        assert_eq!(
            back.execute("SELECT * FROM t ORDER BY a;").unwrap(),
            db.execute("SELECT * FROM t ORDER BY a;").unwrap()
        );
        // Garbage is a clean error, never a panic.
        let err = match PinkDb::from_pink_bytes(b"not a pink file") {
            Ok(_) => panic!("garbage must be refused"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("bad magic"), "{err}");
    }

    /// A file written by an older pinkdb (`PINKDB01`–`PINKDB05`) is refused
    /// with the documented message: this build reads `PINKDB06` only.
    #[test]
    fn older_pink_files_are_refused_with_the_documented_message() {
        for version in 1..=5u8 {
            let mut bytes = format!("PINKDB0{version}").into_bytes();
            bytes.extend_from_slice(&[0xABu8; 64]);
            let err = PinkDb::from_pink_bytes(&bytes).unwrap_err().to_string();
            assert_eq!(
                err,
                format!(
                    "this .pink file was written by an older pinkdb (PINKDB0{version}); \
                     this build reads PINKDB06 only"
                ),
                "version {version}"
            );
        }
    }

    /// A file from a *newer* pinkdb is a different error: "upgrade pinkdb".
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
        // The trailer's magic (the file's last 8 bytes).
        let mut bad = full.clone();
        let n = bad.len();
        bad[n - 8..].copy_from_slice(b"PINKDB07");
        let err = PinkDb::load_bytes(&bad).unwrap_err().to_string();
        assert!(
            err.contains("written by a newer pinkdb (format v7)"),
            "{err}"
        );
        // The magic lives at the end: a file whose *first* bytes claim
        // another version is a damaged image, and the column chunk's own
        // checksum is what catches it.
        let mut bad = full.clone();
        bad[..8].copy_from_slice(b"PINKDB09");
        let err = PinkDb::load_bytes(&bad).unwrap_err().to_string();
        assert!(err.contains("checksum"), "{err}");
        // Not a digit → plain bad magic.
        let mut bad = full.clone();
        let n = bad.len();
        bad[n - 8..].copy_from_slice(b"PINKDB0X");
        let err = PinkDb::load_bytes(&bad).unwrap_err().to_string();
        assert!(err.contains("bad magic"), "{err}");
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

    /// A table with more rows than one row group: the row-group directory
    /// must carry every group, and the rows must survive the round trip.
    #[test]
    fn multi_row_group_tables_round_trip() {
        let rows = ROW_GROUP_ROWS + 137;
        let mut db = PinkDb::new();
        db.create_table(
            "t",
            vec![
                ("i".to_string(), DataType::Integer),
                ("v".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            for i in 0..rows {
                t.append_row(vec![
                    Value::Integer(i as i32),
                    Value::Varchar(format!("row-{i}")),
                ])
                .unwrap();
            }
        }
        let bytes = db.to_bytes().unwrap();
        let footer = read_store_footer(&bytes).unwrap();
        let tdir = &footer.databases[0].tables[0];
        assert_eq!(tdir.row_count, rows as u64);
        assert_eq!(tdir.row_groups.len(), 2, "122,880 + 137 rows = two groups");
        assert_eq!(tdir.row_groups[0].rows, ROW_GROUP_ROWS as u64);
        assert_eq!(tdir.row_groups[1].rows, 137);
        // The first group holds 60 pages (one engine chunk each).
        assert_eq!(tdir.row_groups[0].columns[0].page_count, 60);
        assert_eq!(tdir.row_groups[1].columns[0].page_count, 1);
        // The chunks' statistics are exact.
        let first = &tdir.row_groups[0].columns[0];
        assert_eq!(first.null_count, 0);
        assert_eq!(first.min, Some(Value::Integer(0)));
        assert_eq!(first.max, Some(Value::Integer(ROW_GROUP_ROWS as i32 - 1)));
        let back = roundtrip(&db);
        assert_eq!(back.table("t").unwrap().num_rows(), rows);
        assert_eq!(all_values(&db, "t"), all_values(&back, "t"));
    }

    #[test]
    fn rle_decoders_reject_lengths_that_would_overflow() {
        // Two runs: the first fills one slot, the second claims u32::MAX
        // more. On a 32-bit target `1 + u32::MAX` wraps to 0, so the old
        // `out.len() + len > n` guard let a multi-gigabyte `repeat_n`
        // through; the checked add must report a clean corruption error.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&7i32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&7i32.to_le_bytes());
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        let mut r = Reader::new(&bytes);
        let err = decode_rle_i32(&mut r, 1, 2).unwrap_err();
        assert!(
            err.to_string().contains("RLE run overflows chunk length"),
            "{err}"
        );
        assert!(!err.to_string().contains("panicked"), "{err}");
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
        assert_eq!(
            roundtrip_chunk(DataType::Integer, ints.clone(), ENC_FASTLANES),
            ints
        );
        let neg: Vec<Value> = vec![Value::Integer(-3), Value::Null, Value::Integer(5)];
        assert_eq!(
            roundtrip_chunk(DataType::Integer, neg.clone(), ENC_BITPACK),
            neg
        );
        assert_eq!(
            roundtrip_chunk(DataType::Integer, neg.clone(), ENC_FASTLANES),
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
        assert_eq!(
            roundtrip_chunk(DataType::Varchar, strs.clone(), ENC_FSST),
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
        assert_eq!(
            roundtrip_chunk(DataType::Date, dates.clone(), ENC_FASTLANES),
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
        assert_eq!(
            roundtrip_chunk(DataType::Timestamp, stamps.clone(), ENC_FASTLANES),
            stamps
        );
    }

    /// The FastLanes encoding: 1,024-value blocks, a frame of reference per
    /// block, transposed lanes. Every block boundary must round-trip.
    #[test]
    fn fastlanes_blocks_round_trip_exactly() {
        for n in [0usize, 1, 7, 8, 9, 1023, 1024, 1025, 2047, 2048] {
            let vals: Vec<Value> = (0..n)
                .map(|i| Value::Bigint((i as i64 * 7919) % 100_003))
                .collect();
            assert_eq!(
                roundtrip_chunk(DataType::Bigint, vals.clone(), ENC_FASTLANES),
                vals,
                "n={n}"
            );
        }
        // The extremes of the integer range.
        let ex: Vec<Value> = vec![
            Value::Bigint(i64::MIN),
            Value::Bigint(i64::MAX),
            Value::Bigint(0),
            Value::Null,
            Value::Bigint(i64::MIN),
        ];
        assert_eq!(
            roundtrip_chunk(DataType::Bigint, ex.clone(), ENC_FASTLANES),
            ex
        );
    }

    /// The FastLanes encoding is a *candidate*: it wins when the per-block
    /// frame of reference is smaller than the global bit width.
    #[test]
    fn fastlanes_wins_on_locally_clustered_values() {
        // Two clusters far apart: the global range needs 32 bits, the
        // per-block frame of reference needs a handful.
        let vals: Vec<Value> = (0..CHUNK_SIZE)
            .map(|i| {
                if i < CHUNK_SIZE / 2 {
                    Value::Integer(i as i32)
                } else {
                    Value::Integer(1_000_000 + i as i32)
                }
            })
            .collect();
        let (tag, payload) = encode_chunk(DataType::Integer, &vals);
        assert_eq!(tag, ENC_FASTLANES, "the clustered chunk picks FastLanes");
        let bitpack = enc_ints_bitpack(&int32_payload(&vector_from_values(
            DataType::Integer,
            &vals,
        )));
        assert!(
            payload.len() < bitpack.len(),
            "FastLanes {} must beat the global bitpack {}",
            payload.len(),
            bitpack.len()
        );
    }

    /// The FSST encoding: a symbol table that rewrites the string bytes,
    /// then the second compression stage.
    #[test]
    fn fsst_round_trips_and_rebuilds_the_bytes() {
        let vals: Vec<Value> = (0..500)
            .map(|i| Value::Varchar(format!("value-{}", i % 37)))
            .collect();
        let out = roundtrip_chunk(DataType::Varchar, vals.clone(), ENC_FSST);
        assert_eq!(out, vals);
        // The symbol table really is a rewrite: the escaped stream decodes
        // back to the original bytes.
        let (offsets, data) = strs_offsets_and_data(
            &vals
                .iter()
                .map(|v| match v {
                    Value::Varchar(s) => s.clone(),
                    _ => unreachable!(),
                })
                .collect::<Vec<_>>(),
        );
        let table = fsst_build_table(&data);
        let escaped = fsst_escape(&data, &table);
        assert_eq!(
            fsst_unescape(&escaped, &table, data.len()).unwrap(),
            data,
            "the FSST rewrite must be lossless"
        );
        assert_eq!(offsets.last().copied(), Some(data.len() as u32));
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
        // pick the compression frame.
        let distinct: Vec<Value> = (0..CHUNK_SIZE)
            .map(|i| Value::Varchar(format!("distinct-{i}-{:04}", i * 7919 % 10007)))
            .collect();
        let v = vector_from_values(DataType::Varchar, &distinct);
        let plain = enc_strs_plain(&varchar_payload(&v));
        let (tag, payload) = encode_chunk(DataType::Varchar, &distinct);
        assert_eq!(tag, ENC_LZ_FRAME, "repetitive distinct strings pick LZ");
        assert!(
            payload.len() < plain.len(),
            "the compressed frame {} must beat plain {}",
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
            let err = decode_chunk_typed(DataType::Varchar, tag, &mut r).unwrap_err();
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

    /// A damaged file must fail cleanly: the chunk checksums and the footer
    /// checksum catch the damage before any value is decoded.
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
        std::fs::write(&path, b"GARBAGE!junkjunkjunkjunkjunk").unwrap();
        assert!(PinkDb::open(path.to_str().unwrap()).is_err());
        let mut bad = full.clone();
        bad[10] = 0xFF;
        std::fs::write(&path, &bad).unwrap();
        assert!(PinkDb::open(path.to_str().unwrap()).is_err());
        std::fs::remove_file(&path).ok();
    }

    /// A flipped byte in the **data region** is caught by the column chunk's
    /// own CRC32C — no whole-file checksum is needed.
    #[test]
    fn per_chunk_corruption_is_detected() {
        let mut db = PinkDb::new();
        db.create_table(
            "t",
            vec![
                ("i".to_string(), DataType::Integer),
                ("v".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            for i in 0..500i32 {
                t.append_row(vec![
                    Value::Integer(i),
                    Value::Varchar(format!("value-{i}")),
                ])
                .unwrap();
            }
        }
        let bytes = db.to_bytes().unwrap();
        // The data region is everything before the footer.
        let (footer_at, _) = footer_span(&bytes).unwrap();
        let mut detected = 0;
        for i in 0..footer_at as usize {
            let mut bad = bytes.clone();
            bad[i] ^= 0x01;
            let err = PinkDb::load_bytes(&bad).unwrap_err().to_string();
            assert!(err.contains("checksum"), "byte {i}: {err}");
            detected += 1;
        }
        assert!(detected > 0, "the data region must be covered");
    }

    /// A flipped byte in the **footer** is caught by the footer's own
    /// CRC32C.
    #[test]
    fn footer_corruption_is_detected() {
        let mut db = PinkDb::new();
        db.create_table("t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(1)])
            .unwrap();
        let bytes = db.to_bytes().unwrap();
        let (footer_at, footer_len) = footer_span(&bytes).unwrap();
        for i in 0..footer_len {
            let mut bad = bytes.clone();
            bad[footer_at as usize + i as usize] ^= 0x01;
            let err = PinkDb::load_bytes(&bad).unwrap_err().to_string();
            assert!(
                err.contains("footer checksum") || err.contains("corrupt file"),
                "footer byte {i}: {err}"
            );
        }
        // The trailer's own CRC field is part of the check.
        let mut bad = bytes.clone();
        let n = bad.len();
        bad[n - TRAILER_LEN] ^= 0x01;
        let err = PinkDb::load_bytes(&bad).unwrap_err().to_string();
        assert!(err.contains("footer checksum"), "{err}");
    }

    /// Truncating the file at *every* offset errors (never panics, never
    /// silently returns wrong values).
    #[test]
    fn truncation_at_every_offset_errors() {
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

    /// The encrypted container wraps the whole `PINKDB06` image: a keyed
    /// save/open round-trips the data.
    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_container_round_trips_the_pinkdb06_image() {
        let enc = temp_file("enc_roundtrip");
        let key = EncryptionKey::from_raw([9u8; 32]);
        let mut db = PinkDb::new();
        db.create_table(
            "t",
            vec![
                ("i".to_string(), DataType::Integer),
                ("v".to_string(), DataType::Varchar),
                ("d".to_string(), DataType::Double),
            ],
        )
        .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            for i in 0..1000i32 {
                t.append_row(vec![
                    Value::Integer(i),
                    Value::Varchar(format!("row-{i}")),
                    Value::Double(f64::from(i) / 8.0),
                ])
                .unwrap();
            }
        }
        db.save_with_key(enc.to_str().unwrap(), &key).unwrap();
        let back = PinkDb::open_with_key(enc.to_str().unwrap(), &key).unwrap();
        assert_eq!(all_values(&db, "t"), all_values(&back, "t"));
        std::fs::remove_file(&enc).ok();
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
            // One NULL element on every seventh row exercises the element
            // bitmap next to the row validity.
            DataType::Array { elem, len } => {
                let values: Vec<Value> = (0..len)
                    .map(|k| {
                        if i % 7 == 3 && k == 1 {
                            return Value::Null;
                        }
                        match elem {
                            ArrayElem::Integer => Value::Integer(i as i32 + k as i32),
                            ArrayElem::Bigint => Value::Bigint(i as i64 - k as i64),
                            // A FLOAT element is a `Double` cell narrowed on the
                            // way in, exactly like the cast path.
                            ArrayElem::Float | ArrayElem::Double => {
                                Value::Double(i as f64 / 8.0 + f64::from(k))
                            }
                        }
                    })
                    .collect();
                Value::Array(Arc::new(
                    crate::array::ArrayValue::from_values(elem, len, &values).unwrap(),
                ))
            }
            DataType::Variant => Value::Variant(Arc::new(match i % 7 {
                0 => crate::variant::Variant::Null,
                1 => crate::variant::Variant::Bool(i.is_multiple_of(2)),
                2 => crate::variant::Variant::Int(i as i64),
                3 => crate::variant::Variant::Double(i as f64 / 8.0),
                4 => crate::variant::Variant::Str(Arc::from(format!("s{i}").as_str())),
                5 => crate::variant::Variant::array(vec![crate::variant::Variant::Int(i as i64)])
                    .unwrap(),
                _ => crate::variant::Variant::object(vec![
                    (Arc::from("k"), crate::variant::Variant::Int(i as i64)),
                    (Arc::from("n"), crate::variant::Variant::Null),
                ])
                .unwrap(),
            })),
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
                        (
                            "dec".to_string(),
                            DataType::Decimal {
                                precision: 18,
                                scale: 3,
                            },
                        ),
                        ("iv".to_string(), DataType::Interval),
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
                            corpus_value(
                                DataType::Decimal {
                                    precision: 18,
                                    scale: 3,
                                },
                                i,
                                null_every,
                            ),
                            corpus_value(DataType::Interval, i, null_every),
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
                    for c in 0..9 {
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
            decode_chunk_typed(DataType::Double, tag, &mut r)
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

    /// The format is pinned: a fixed corpus has a fixed length and a fixed
    /// footer checksum, deterministic across parallel/serial encodes and
    /// across save→open→save.
    #[test]
    fn golden_pinkdb06_bytes_are_pinned() {
        let db = golden_db();
        let bytes = db.to_bytes().unwrap();
        assert_eq!(bytes.len(), GOLDEN_V6_LEN, "file length drifted");
        assert_eq!(
            crc32c(&footer_bytes(&bytes)),
            GOLDEN_V6_FOOTER_CRC,
            "the footer bytes drifted"
        );
        assert_eq!(db.to_bytes_impl(false).unwrap(), bytes, "serial ≠ parallel");
        let back = PinkDb::load_bytes(&bytes).unwrap();
        assert_eq!(back.to_bytes().unwrap(), bytes, "open→save drift");
    }

    /// The trailer is the last 20 bytes and the footer sits right before it:
    /// a reader needs exactly two range requests.
    #[test]
    fn the_trailer_and_the_footer_are_the_only_metadata_reads() {
        let db = golden_db();
        let bytes = db.to_bytes().unwrap();
        assert_eq!(&bytes[bytes.len() - 8..], MAGIC_V6);
        // The trailer: footer_crc32c u32 | footer_len u64 | magic.
        let n = bytes.len();
        let want_crc = u32::from_le_bytes(
            bytes[n - TRAILER_LEN..n - TRAILER_LEN + 4]
                .try_into()
                .unwrap(),
        );
        let want_len = u64::from_le_bytes(
            bytes[n - TRAILER_LEN + 4..n - TRAILER_LEN + 12]
                .try_into()
                .unwrap(),
        ) as usize;
        let (offset, len) = footer_span(&bytes).unwrap();
        assert_eq!(len as usize, want_len);
        assert_eq!(offset as usize, n - TRAILER_LEN - want_len);
        assert_eq!(
            crc32c(&bytes[offset as usize..(offset + len) as usize]),
            want_crc
        );
        // The data region starts at 0: the footer records absolute offsets.
        let footer = read_store_footer(&bytes).unwrap();
        let dir = &footer.databases[0].tables[0].row_groups[0].columns[0];
        assert_eq!(dir.offset, 0, "the first column chunk starts the file");
    }

    /// The compression frame (tag 8) round-trips every dtype bit-exactly
    /// (the frame is dtype-agnostic: decode decompresses, then decodes the
    /// inner payload), is chosen for repetitive strings, and is
    /// byte-deterministic.
    #[test]
    fn compressed_frame_roundtrips_and_is_selected() {
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
        // Layout: raw_len | base_tag | codec | comp_len | comp.
        let raw_len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
        let comp_len = u32::from_le_bytes(payload[6..10].try_into().unwrap()) as usize;
        assert_eq!(raw_len, plain.len());
        assert_eq!(payload[4], ENC_PLAIN);
        assert_eq!(payload.len(), 10 + comp_len);
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
            assert_values_bit_equal(&out, &vals, &format!("frame {dtype:?}"));
        }
    }

    /// Helper: a chunk frame around a raw payload (test).
    fn framed_chunk(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut w = Writer::new();
        w.u8(tag);
        w.u32(0); // all-valid
        w.bytes(payload);
        w.buf
    }

    /// Helper: a compression frame with explicit header fields (test).
    fn compressed_payload(raw_len: u32, base_tag: u8, codec: u8, comp: &[u8]) -> Vec<u8> {
        let mut w = Writer::new();
        w.u32(raw_len);
        w.u8(base_tag);
        w.u8(codec);
        w.u32(comp.len() as u32);
        w.bytes(comp);
        w.buf
    }

    /// Helper: decode one VARCHAR chunk frame (test).
    fn decode_framed(bytes: &[u8]) -> Result<Vector> {
        let mut r = Reader::new(bytes);
        let tag = r.u8().unwrap();
        decode_chunk_typed(DataType::Varchar, tag, &mut r)
    }

    /// The compression frame's decode safety: every malformed shape is a
    /// clear error and never a panic or a hung allocation (the declared
    /// lengths are capped).
    #[test]
    fn compressed_frame_decode_rejects_malformed_bodies() {
        let values: Vec<Value> = (0..100)
            .map(|i| Value::Varchar(format!("row-{}", i % 7)))
            .collect();
        let v = vector_from_values(DataType::Varchar, &values);
        let base = enc_strs_plain(&varchar_payload(&v));
        let tokens = lz_compress(&base);
        let good = compressed_payload(base.len() as u32, ENC_PLAIN, SecondStage::Lz.tag(), &tokens);
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
                err(&bad).contains("compressed frame length"),
                "{bad_len}: {}",
                err(&bad)
            );
        }
        // Reserved / frame base tags and a tag invalid for the dtype.
        for base_tag in [
            ENC_DICT_RLE,
            ENC_LZ_FRAME,
            ENC_FSST,
            ENC_FASTLANES,
            10,
            200,
            255,
        ] {
            let bad =
                compressed_payload(base.len() as u32, base_tag, SecondStage::Lz.tag(), &tokens);
            assert!(
                err(&bad).contains("unsupported compression base encoding"),
                "base {base_tag}: {}",
                err(&bad)
            );
        }
        let bad = compressed_payload(base.len() as u32, ENC_ALP, SecondStage::Lz.tag(), &tokens);
        assert!(err(&bad).contains("invalid for VARCHAR"), "{}", err(&bad));
        // An unknown codec tag.
        let bad = compressed_payload(base.len() as u32, ENC_PLAIN, 99, &tokens);
        assert!(
            err(&bad).contains("unknown compression codec"),
            "{}",
            err(&bad)
        );
        // comp_len beyond the buffer.
        let mut bad = good.clone();
        bad[6..10].copy_from_slice(&(tokens.len() as u32 + 100).to_le_bytes());
        assert!(
            err(&bad).contains("unexpected end of file"),
            "{}",
            err(&bad)
        );
        // Truncated token stream.
        let bad = compressed_payload(
            base.len() as u32,
            ENC_PLAIN,
            SecondStage::Lz.tag(),
            &tokens[..tokens.len() - 2],
        );
        assert!(err(&bad).contains("truncated"), "{}", err(&bad));
        // Trailing token bytes after an otherwise complete output.
        let mut extra = tokens.clone();
        extra.push(0); // a one-byte literal token
        extra.push(b'x');
        let bad = compressed_payload(base.len() as u32, ENC_PLAIN, SecondStage::Lz.tag(), &extra);
        assert!(err(&bad).contains("trailing bytes"), "{}", err(&bad));
        // A decompressed payload with bytes the inner codec does not consume.
        let mut long_base = base.clone();
        long_base.extend_from_slice(b"junk");
        let mut long_tokens = Vec::new();
        for chunk in long_base.chunks(LZ_MAX_LITERAL) {
            long_tokens.push(((chunk.len() - 1) as u8) << 1);
            long_tokens.extend_from_slice(chunk);
        }
        let bad = compressed_payload(
            long_base.len() as u32,
            ENC_PLAIN,
            SecondStage::Lz.tag(),
            &long_tokens,
        );
        assert!(
            err(&bad).contains("trailing bytes in compressed frame"),
            "{}",
            err(&bad)
        );
    }

    /// The LZ decoder's own safety net: a literal run or a match that
    /// overruns the declared length, and an out-of-range back-reference, are
    /// clear errors.
    #[test]
    fn lz_decode_rejects_malformed_token_streams() {
        let corrupt = |what: &str| PinkDbError::Execution(format!("corrupt file: {what}"));
        assert!(lz_decompress(&[0xFEu8], 4).is_err());
        // Literal run overruns the declared length (raw_len 4, literal 128).
        let mut toks = vec![0xFEu8];
        toks.extend(std::iter::repeat_n(b'x', 128));
        let err = lz_decompress(&toks, 4).unwrap_err().to_string();
        assert!(err.contains("literal run overruns"), "{err}");
        // Zero and out-of-range back-references.
        let err = lz_decompress(&[0x03, 0x00, 0x00], 4)
            .unwrap_err()
            .to_string();
        assert!(err.contains("back-reference out of range"), "{err}");
        let err = lz_decompress(&[0x03, 0x05, 0x00], 4)
            .unwrap_err()
            .to_string();
        assert!(err.contains("back-reference out of range"), "{err}");
        // Match overruns the declared length (1 literal + a 129-byte match
        // into raw_len 4).
        let err = lz_decompress(&[0x00, b'A', 0xFD, 0x01, 0x00], 4)
            .unwrap_err()
            .to_string();
        assert!(err.contains("match overruns"), "{err}");
        // A truncated token stream.
        let err = lz_decompress(&[], 4).unwrap_err().to_string();
        assert!(err.contains("truncated"), "{err}");
        let _ = corrupt("unused");
    }

    /// The second compression stage: both codecs round-trip, and the frame
    /// names the codec it used.
    #[test]
    fn both_second_stages_round_trip() {
        let data: Vec<u8> = (0..4096u32).map(|i| (i % 61) as u8).collect();
        for stage in [SecondStage::Lz, SecondStage::Lz4] {
            let compressed = stage.compress(&data);
            assert!(
                compressed.len() < data.len(),
                "{stage:?} must shrink the repetitive data"
            );
            let back = stage.decompress(&compressed, data.len()).unwrap();
            assert_eq!(back, data, "{stage:?}");
        }
    }

    /// A file whose VARCHAR chunks use the compression frame errors on
    /// truncation at (sampled) every offset: no cut may decode as a wrong
    /// value.
    #[test]
    fn compressed_file_truncation_errors_at_every_sampled_offset() {
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
            "the truncation corpus must exercise the compression frame"
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
        // A decoded file re-encodes to the same bytes (deterministic
        // probe + frame choice).
        let back = PinkDb::load_bytes(&bytes).unwrap();
        assert_eq!(back.to_bytes().unwrap(), bytes, "open→save drift with LZ");
    }

    /// The compression probe is bounded and selective: repetitive text fires,
    /// random or near-random bytes (4-byte matches that do not pay for their
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
        for tag in [ENC_BITPACK, ENC_DELTA_BITPACK, ENC_FASTLANES] {
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
        for tag in [ENC_BITPACK, ENC_DELTA_BITPACK, ENC_FASTLANES] {
            let out = roundtrip_chunk(DataType::Timestamp, ts.clone(), tag);
            assert_eq!(out, ts, "timestamp tag {tag} lost values");
        }
    }

    /// Forged counts in the footer's headers are capped and rejected.
    #[test]
    fn forged_counts_are_capped_and_rejected() {
        let mut db = PinkDb::new();
        crate::csv::import_csv_string(&mut db, "s", "a,b,c,d\n1,x,1.5,true\n2,y,2.5,false\n")
            .unwrap();
        let full = db.to_bytes().unwrap();
        // Plant u32::MAX at every offset of the footer: each load must
        // return an error instead of aborting on a giant allocation.
        let (footer_at, footer_len) = footer_span(&full).unwrap();
        let footer = full[footer_at as usize..(footer_at + footer_len) as usize].to_vec();
        let mut capped = 0;
        for i in 0..footer.len().saturating_sub(3) {
            let mut bad_footer = footer.clone();
            bad_footer[i..i + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            let mut bad = full.clone();
            bad[footer_at as usize..(footer_at + footer_len) as usize].copy_from_slice(&bad_footer);
            // The footer's checksum now fails, so re-write it: the point is
            // that the *decoder* never panics or hangs.
            let crc = crc32c(&bad_footer);
            let n = bad.len();
            bad[n - TRAILER_LEN..n - TRAILER_LEN + 4].copy_from_slice(&crc.to_le_bytes());
            if PinkDb::load_bytes(&bad).is_err() {
                capped += 1;
            }
        }
        assert!(capped > 0, "the forged footers must all be refused");
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
            let dtype = match next() % 10 {
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
                8 => DataType::Interval,
                _ => DataType::Variant,
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
                    DataType::Array { elem, len } => {
                        let values: Vec<Value> = (0..len)
                            .map(|k| {
                                if r % 9 == 4 && k == 0 {
                                    return Value::Null;
                                }
                                match elem {
                                    ArrayElem::Integer => Value::Integer(r as i32 ^ k as i32),
                                    ArrayElem::Bigint => Value::Bigint(r as i64 + k as i64),
                                    ArrayElem::Float | ArrayElem::Double => Value::Double(
                                        f64::from_bits(r & 0x3fef_ffff_ffff_ffff) + f64::from(k),
                                    ),
                                }
                            })
                            .collect();
                        Value::Array(Arc::new(
                            crate::array::ArrayValue::from_values(elem, len, &values).unwrap(),
                        ))
                    }
                    DataType::Variant => Value::Variant(Arc::new(match r % 5 {
                        0 => crate::variant::Variant::Null,
                        1 => crate::variant::Variant::Bool(r % 3 == 0),
                        2 => crate::variant::Variant::Int(r as i64),
                        3 => crate::variant::Variant::Double(r as f64 / 3.0),
                        _ => crate::variant::Variant::array(vec![crate::variant::Variant::Int(
                            r as i64,
                        )])
                        .unwrap(),
                    })),
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
            let decoded = decode_chunk_typed(dtype, t, &mut r)
                .unwrap_or_else(|e| panic!("case {case}: dtype={dtype:?} tag={t} err={e}"));
            assert_eq!(decoded.len(), n, "case {case}");
            for i in 0..n {
                let (a, b) = (v.get(i), decoded.get(i));
                match (&a, &b) {
                    (Value::Double(x), Value::Double(y)) => {
                        assert_eq!(x.to_bits(), y.to_bits(), "case {case} row {i}");
                    }
                    // VARIANT equality is numeric (1 = 1.0), so compare the
                    // encodings: that pins the kind too.
                    (Value::Variant(x), Value::Variant(y)) => {
                        let (mut a, mut b) = (Vec::new(), Vec::new());
                        crate::variant::encode(x, &mut a);
                        crate::variant::encode(y, &mut b);
                        assert_eq!(a, b, "case {case} row {i}");
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
        // A second table whose VARCHAR chunks pick the compression frame: the
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
            "the identity corpus must exercise the compression frame"
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

    /// Every catalog section survives a save/open round trip: constraints,
    /// column defaults, index definitions, materialized views, procedures,
    /// macros and RBAC.
    #[test]
    fn every_metadata_section_round_trips() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, v INTEGER);")
            .unwrap();
        db.execute(
            "CREATE TABLE c (cid INTEGER, pid INTEGER NOT NULL REFERENCES p(id), \
             note VARCHAR UNIQUE);",
        )
        .unwrap();
        db.execute("ALTER TABLE c ADD COLUMN d INTEGER DEFAULT 42;")
            .unwrap();
        db.execute("INSERT INTO p VALUES (1, 2);").unwrap();
        db.execute("INSERT INTO c VALUES (1, 1, 'x', 7);").unwrap();
        db.execute("CREATE INDEX i_c_pid ON c(pid);").unwrap();
        db.execute("CREATE MATERIALIZED VIEW mv AS SELECT id FROM p;")
            .unwrap();
        db.execute("CREATE MACRO twice(x) AS x + x;").unwrap();
        db.execute(
            "CREATE USER alice PASSWORD_HASH '$argon2id$v=19$m=19456,t=2,p=1\
             $AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';",
        )
        .unwrap();
        db.execute("CREATE ROLE analyst;").unwrap();
        db.execute("GRANT analyst TO alice;").unwrap();
        db.execute("GRANT SELECT ON TABLE c TO analyst;").unwrap();
        db.execute("GRANT ADMIN TO analyst;").unwrap();

        let bytes = db.to_bytes().unwrap();
        let back = PinkDb::load_bytes(&bytes).unwrap();
        // The constraints.
        assert_eq!(
            db.table("c").unwrap().constraints,
            back.table("c").unwrap().constraints
        );
        // The column default.
        assert_eq!(
            back.table("c").unwrap().columns[3].default,
            Some(Value::Integer(42))
        );
        // The index definition (the map is rebuilt from the rows).
        let t = back.table("c").unwrap();
        assert_eq!(t.indexes.len(), 1);
        assert_eq!(t.indexes[0].name, "i_c_pid");
        assert_eq!(t.indexes[0].column, 1);
        assert_eq!(
            t.indexes[0].map,
            crate::storage::index_map_for(&t.columns[1]),
            "the map is rebuilt from the rows"
        );
        // The materialized view.
        assert!(back.materialized_view_in("main", "mv").is_ok());
        // The macro.
        assert!(back.macro_in("main", "twice").is_ok());
        // The RBAC section.
        assert!(back.has_users());
        assert!(back.principal_is_admin("alice"));
        assert_eq!(
            back.effective_table_privileges("alice", "main", "c"),
            vec![crate::rbac::Privilege::Select]
        );
        // The bytes are stable.
        assert_eq!(back.to_bytes().unwrap(), bytes, "open→save drift");
    }

    /// A durable reopen after a checkpoint restores the data (the checkpoint
    /// is a `PINKDB06` image).
    #[test]
    fn durable_reopen_after_checkpoint_restores_the_rows() {
        let ckpt = temp_file("durable");
        let ckpt = std::path::PathBuf::from(format!("{}.db", ckpt.display()));
        let wal = std::path::PathBuf::from(format!("{}.wal", ckpt.display()));
        {
            let mut db = PinkDb::open_durable(ckpt.to_str().unwrap()).unwrap();
            db.execute("CREATE TABLE t (a INTEGER, b VARCHAR);")
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, 'x'), (2, NULL);")
                .unwrap();
            db.execute("CHECKPOINT;").unwrap();
        }
        {
            let db = PinkDb::open_durable(ckpt.to_str().unwrap()).unwrap();
            assert_eq!(db.table("t").unwrap().num_rows(), 2);
            assert_eq!(db.table("t").unwrap().get(1, 1), Some(Value::Null));
            // The checkpoint is a PINKDB06 image.
            let bytes = std::fs::read(&ckpt).unwrap();
            assert!(is_pinkdb06(&bytes), "the checkpoint must be PINKDB06");
        }
        let _ = std::fs::remove_file(&ckpt);
        let _ = std::fs::remove_file(&wal);
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

    /// The **second compression stage**'s measurement probe (wave 102).
    ///
    /// Compresses every VARCHAR chunk's plain payload of a TPC-H SF1 store
    /// with both second stages — the hand-rolled LZ77 of wave 45j and the
    /// pure-Rust `lz4_flex` — and prints the sizes and the times. This is
    /// the measurement that picked the default.
    ///
    /// ```text
    /// PINKDB_TPCH_DIR=/tmp/w102 cargo test --release -p pinkdb --lib \
    ///     second_stage_measurement -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore = "measurement probe: run with --release --ignored --nocapture"]
    fn second_stage_measurement_probe() {
        let dir = std::env::var("PINKDB_TPCH_DIR")
            .expect("PINKDB_TPCH_DIR must point at the TPC-H CSV directory");
        let mut db = PinkDb::new();
        let mut paths: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "csv"))
            .collect();
        paths.sort();
        for path in paths {
            let stem = path.file_stem().unwrap().to_string_lossy().to_string();
            crate::csv::import_csv(&mut db, path.to_str().unwrap(), Some(&stem)).unwrap();
        }
        // Every VARCHAR chunk's plain payload (the frame's inner payload).
        let mut payloads: Vec<Vec<u8>> = Vec::new();
        for dbname in db.list_databases() {
            for tname in db.list_tables_in(&dbname) {
                let t = db.table_in(&dbname, &tname).unwrap();
                for c in 0..t.num_chunks() {
                    let chunk = t.scan_chunk(c).unwrap();
                    for v in chunk.columns.iter() {
                        if let VectorData::VarChar(_) = v.data() {
                            payloads.push(enc_strs_plain(&varchar_payload(v)));
                        }
                    }
                }
            }
        }
        let plain: usize = payloads.iter().map(|p| p.len()).sum();
        println!("VARCHAR chunks: {}", payloads.len());
        println!("plain bytes:    {plain}");
        for stage in [SecondStage::Lz, SecondStage::Lz4] {
            let mut total = 0usize;
            let t0 = std::time::Instant::now();
            for p in &payloads {
                total += stage.compress(p).len();
            }
            let compress = t0.elapsed().as_secs_f64();
            let t0 = std::time::Instant::now();
            for p in &payloads {
                let c = stage.compress(p);
                let back = stage.decompress(&c, p.len()).unwrap();
                assert_eq!(back.len(), p.len());
            }
            let round = t0.elapsed().as_secs_f64();
            println!(
                "{stage:?}: {total} bytes ({:.3}× the plain), compress {compress:.3}s, \
                 compress+decompress {round:.3}s",
                total as f64 / plain as f64
            );
        }
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
                    // NaN is counted, not ranked: the min/max facts cover
                    // the non-NaN values (NaN would always be the max).
                    if matches!(v, Value::Double(x) if x.is_nan()) {
                        nan_count += 1;
                        continue;
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

    // ---------- the declarative constraints ----------

    /// A constrained store keeps its constraints exactly through save/open.
    #[test]
    fn constraints_round_trip_through_the_footer() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, v INTEGER);")
            .unwrap();
        db.execute("CREATE TABLE c (cid INTEGER, pid INTEGER NOT NULL REFERENCES p(id), note VARCHAR UNIQUE);")
            .unwrap();
        db.execute("CREATE TABLE u (a INTEGER, b INTEGER, UNIQUE (a, b));")
            .unwrap();
        db.execute("CREATE TABLE free (x INTEGER);").unwrap();
        let bytes = db.to_bytes().unwrap();
        let back = PinkDb::load_bytes(&bytes).unwrap();
        for name in ["p", "c", "u", "free"] {
            assert_eq!(
                db.table(name).unwrap().constraints,
                back.table(name).unwrap().constraints,
                "constraints of {name}"
            );
        }
        assert_eq!(back.to_bytes().unwrap(), bytes, "open→save drift");
        // DESCRIBE answers identically after the reopen.
        let mut back = back;
        let before = db.execute("DESCRIBE c;").unwrap().pop().unwrap();
        let after = back.execute("DESCRIBE c;").unwrap().pop().unwrap();
        assert_eq!(before, after);
    }

    // ---------- the materialized views ----------

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

    /// A store with materialized views round-trips the definitions (schedule,
    /// defining SQL, stamps, counters), the backing tables and — bit for bit
    /// — the file itself.
    #[test]
    fn materialized_views_round_trip_through_the_footer() {
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
    fn broken_definition_loads_and_heals() {
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

    // ---------- the RBAC section ----------

    /// The RBAC section round-trips (users with their PHC hashes, roles,
    /// memberships, table/database grants, admins) and is byte-identical
    /// across saves.
    #[test]
    fn rbac_section_round_trips_and_is_deterministic() {
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

    /// A store with no RBAC data writes no section (the footer stays
    /// minimal).
    #[test]
    fn rbac_free_stores_keep_the_footer_minimal() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("CREATE ROLE r; DROP ROLE r;").unwrap();
        let bytes = db.to_bytes().unwrap();
        let footer = read_store_footer(&bytes).unwrap();
        assert!(footer.rbac.is_none());
        // The store loads as before.
        let back = PinkDb::load_bytes(&bytes).unwrap();
        assert!(!back.has_users());
        assert!(!back.has_rbac_data());
    }

    /// Corrupt RBAC sections are clean errors, never a panic or a silent
    /// empty catalog.
    #[test]
    fn corrupt_rbac_sections_error() {
        // An image with an empty data region (a table with no rows) and a
        // hand-built RBAC section, so each case corrupts exactly one field.
        let image = |rbac: &[u8]| -> Vec<u8> {
            let mut fw = Writer::new();
            fw.u32(FOOTER_VERSION);
            fw.u32(FLAG_RBAC);
            fw.u32(1); // one database
            fw.str("main");
            fw.u32(1); // one table
            fw.str("t");
            fw.u32(1); // one column
            fw.str("a");
            fw.u8(1); // INTEGER
            fw.u64(0); // row count
            fw.u32(0); // no row groups
            fw.bytes(rbac);
            let footer = fw.buf;
            let mut out = Vec::new();
            out.extend_from_slice(&footer);
            out.extend_from_slice(&crc32c(&footer).to_le_bytes());
            out.extend_from_slice(&(footer.len() as u64).to_le_bytes());
            out.extend_from_slice(MAGIC_V6);
            out
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
        let bad_hash = image(&{
            let mut w = Writer::new();
            w.u32(1);
            w.str("alice");
            w.str("not-a-hash");
            w.u32(0);
            w.u32(0);
            w.u32(0);
            w.u32(0);
            w.u32(0);
            w.buf
        });
        let err = PinkDb::load_bytes(&bad_hash).unwrap_err().to_string();
        assert!(err.contains("invalid password hash"), "bad_hash: {err}");
        assert!(err.contains("alice"), "bad_hash: {err}");
        // An unknown privilege tag.
        let bad_tag = image(&{
            let mut w = Writer::new();
            user_head(&mut w);
            w.u32(1); // table grants: one entry
            w.str("main");
            w.str("t");
            w.u32(1); // one principal
            w.str("alice");
            w.u32(1); // one privilege
            w.u8(9); // ← unknown
            w.u32(0); // database grants
            w.u32(0); // admins
            w.buf
        });
        let err = PinkDb::load_bytes(&bad_tag).unwrap_err().to_string();
        assert!(err.contains("unknown privilege tag 9"), "bad_tag: {err}");
        // A user count that cannot fit in the remaining bytes.
        let bad_count = image(&{
            let mut w = Writer::new();
            w.u32(0xffff_ffff);
            w.buf
        });
        let err = PinkDb::load_bytes(&bad_count).unwrap_err().to_string();
        assert!(
            err.contains("user count exceeds the remaining file"),
            "bad_count: {err}"
        );
        // A section cut short in the middle of the trailing admin list.
        let truncated = image(&{
            let mut w = Writer::new();
            user(&mut w);
            w.u32(1); // admins: one entry
            w.str("alice");
            w.buf
        });
        let err = PinkDb::load_bytes(&truncated).unwrap_err().to_string();
        assert!(err.contains("corrupt file"), "truncated: {err}");
    }

    // ---------- the index section ----------

    /// The index definitions of `t (a INTEGER)` holding `1, 2, 2, NULL` with
    /// one index on `a`: the map is rebuilt from the rows, never read from
    /// the file.
    #[test]
    fn index_definitions_round_trip_and_the_map_is_rebuilt() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2), (2), (NULL);")
            .unwrap();
        db.execute("CREATE INDEX i ON t(a);").unwrap();
        let bytes = db.to_bytes().unwrap();
        let footer = read_store_footer(&bytes).unwrap();
        let tdir = &footer.databases[0].tables[0];
        assert_eq!(tdir.indexes.len(), 1);
        assert_eq!(tdir.indexes[0].name, "i");
        assert_eq!(tdir.indexes[0].column, 0);
        assert_eq!(tdir.indexes[0].distinct_keys, 2);
        assert_eq!(tdir.indexes[0].indexed_rows, 3);
        let loaded = PinkDb::load_bytes(&bytes).unwrap();
        let t = loaded.table_in("main", "t").unwrap();
        assert_eq!(t.indexes.len(), 1);
        assert!(!t.indexes_stale);
        assert_eq!(
            t.indexes[0].map,
            crate::storage::index_map_for(&t.columns[0])
        );
    }

    /// A malformed index definition is a clean "corrupt file" error, never
    /// a panic.
    #[test]
    fn corrupt_index_definitions_are_refused() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (2), (2), (NULL);")
            .unwrap();
        db.execute("CREATE INDEX i ON t(a);").unwrap();
        let good = db.to_bytes().unwrap();
        let (footer_at, footer_len) = footer_span(&good).unwrap();
        let footer = good[footer_at as usize..(footer_at + footer_len) as usize].to_vec();
        // The index section: count(4) + name_len(4) + "i"(1) + column(4) +
        // two u64 statistics. Locate it by its bytes.
        let mut needle = Vec::new();
        needle.extend_from_slice(&1u32.to_le_bytes());
        needle.extend_from_slice(&1u32.to_le_bytes());
        needle.push(b'i');
        needle.extend_from_slice(&0u32.to_le_bytes());
        let at = footer
            .windows(needle.len())
            .position(|w| w == needle.as_slice())
            .expect("the index section in the footer");
        for (what, patch) in [
            ("empty name", 0usize),
            ("column out of range", 1),
            ("indexed rows exceed table", 2),
            ("distinct > indexed", 3),
        ] {
            let mut bad_footer = footer.clone();
            match patch {
                0 => bad_footer[at + 4..at + 8].copy_from_slice(&0u32.to_le_bytes()),
                1 => bad_footer[at + 9..at + 13].copy_from_slice(&5u32.to_le_bytes()),
                2 => bad_footer[at + 21..at + 29].copy_from_slice(&5u64.to_le_bytes()),
                _ => bad_footer[at + 13..at + 21].copy_from_slice(&4u64.to_le_bytes()),
            }
            let mut bad = good.clone();
            bad[footer_at as usize..(footer_at + footer_len) as usize].copy_from_slice(&bad_footer);
            let n = bad.len();
            bad[n - TRAILER_LEN..n - TRAILER_LEN + 4]
                .copy_from_slice(&crc32c(&bad_footer).to_le_bytes());
            let err = PinkDb::load_bytes(&bad)
                .map(|_| ())
                .unwrap_err()
                .to_string();
            assert!(err.contains("corrupt file"), "{what}: {err}");
        }
        // Two indexes whose names are out of order are refused (the writer
        // emits names strictly ascending).
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, b INTEGER);")
            .unwrap();
        db.execute("CREATE INDEX x ON t(a);").unwrap();
        db.execute("CREATE INDEX y ON t(b);").unwrap();
        let good = db.to_bytes().unwrap();
        let (footer_at, footer_len) = footer_span(&good).unwrap();
        let footer = good[footer_at as usize..(footer_at + footer_len) as usize].to_vec();
        // The first index's name is the byte after its count and name length.
        let mut needle = Vec::new();
        needle.extend_from_slice(&2u32.to_le_bytes());
        needle.extend_from_slice(&1u32.to_le_bytes());
        needle.push(b'x');
        let at = footer
            .windows(needle.len())
            .position(|w| w == needle.as_slice())
            .expect("the index section in the footer");
        let mut bad_footer = footer.clone();
        bad_footer[at + 8..at + 9].copy_from_slice(b"z");
        let mut bad = good.clone();
        bad[footer_at as usize..(footer_at + footer_len) as usize].copy_from_slice(&bad_footer);
        let n = bad.len();
        bad[n - TRAILER_LEN..n - TRAILER_LEN + 4]
            .copy_from_slice(&crc32c(&bad_footer).to_le_bytes());
        let err = PinkDb::load_bytes(&bad)
            .map(|_| ())
            .unwrap_err()
            .to_string();
        assert!(err.contains("out of name order"), "{err}");
    }
}
