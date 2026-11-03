//! Persistence: a single-file on-disk columnar format (`.pink`).
//!
//! # `PINKDB07` — the one `.pink` format
//!
//! The file starts with two **header slots**, then a **data region**, then a
//! **footer**; nothing at the end of the file is special:
//!
//! ```text
//! offset 0      header slot 0   (4,096 bytes)
//! offset 4,096  header slot 1   (4,096 bytes)
//! offset 8,192  data region: for every table (databases and tables in
//!               sorted name order)
//!                 for every row group (122,880 rows = 60 engine chunks of 2,048)
//!                   for every column (schema order)
//!                     column chunk = page index + pages
//! ...           footer (anywhere after the data it names)
//!
//! header slot (little-endian, zero padded to 4,096 bytes):
//!   magic "PINKDB07" (8) | iteration u64 | footer_offset u64 |
//!   footer_len u64 | footer_crc32c u32 | file_len u64 | slot_crc32c u32
//!
//! footer: store metadata (databases, their schemas, tables, column
//!   defaults as SQL text, constraints, index definitions + statistics, materialized
//!   views, procedures, macros, RBAC), the data-region ranges no row group
//!   references (`free_ranges`), and, per table, the row count and the
//!   row-group directory:
//!     row groups [ { rows, per chunk: rows,
//!                    per column: offset, length, page count,
//!                                 encoding summary, crc32c,
//!                                 stats { min, max, null_count, nan_count } } ]
//! ```
//!
//! A **page** is one engine chunk (2,048 rows) encoded with the existing
//! per-chunk encoder, so decoding stays chunk-native; a **column chunk** is
//! the 60 pages of one (row group, column) pair. Every column chunk carries
//! its own **CRC32C**, so a reader verifies any chunk on its own — there is
//! **no whole-file CRC**. The footer carries its own CRC32C, recorded in the
//! header slot. Row-group directory offsets are **absolute file offsets**.
//!
//! ## Reading
//!
//! A reader validates both header slots (magic, slot CRC, and a footer range
//! inside the `file_len` the slot records), takes the valid slot with the
//! higher `iteration`, then reads and CRC-checks the footer it names: two
//! small range requests, no read of the data region (the lake's `pink`
//! provider does exactly that, plus the projected column chunks of the row
//! groups that survive pruning). A torn write therefore leaves the previous
//! slot valid, naming a footer and a file length that are both on disk. The
//! engine's `open` reads the whole file (it materializes every column eagerly,
//! as before) but decodes the column chunks **in parallel**.
//!
//! `free_ranges` is a sorted `(offset, len)` list of data-region ranges no row
//! group references — the holes an incremental checkpoint leaves behind. The
//! full rewrite writes it empty; a reader range-checks it (inside the data
//! region, sorted, non-overlapping, clear of every row group) and otherwise
//! ignores it.
//!
//! ## Writing
//!
//! A **write** streams the data region: each table is read, encoded and
//! written one row group at a time through a positioned [`Sink`] (a
//! `BufWriter<File>` plus the running offset) for file targets, or a `Vec`
//! sink for the in-memory `to_pink_bytes*` API — the same code, so the bytes
//! are identical either way. The full rewrite writes `2 * HEADER_SLOT_LEN`
//! zero bytes, then the data region, then the footer, then slot 0 with
//! `iteration = 1`; slot 1 stays zero, so it is invalid. Peak memory is the
//! decoded store plus one row group, not the whole file image. Encrypted saves
//! still seal the whole image in memory: the `PINKENC1` container seals the
//! plain image whole.
//!
//! An **incremental checkpoint** (136w3) keeps every clean row group where it
//! is: its footer entry is reused and none of its bytes move. A row group with
//! a dirty chunk (or a changed schema, or a new table) is re-encoded into a
//! hole from the current footer's `free_ranges`, else after `file_len`. The
//! superseded groups and the old footer become holes in the new footer. The
//! commit order is:
//!
//! 1. write the new groups and footer;
//! 2. fsync;
//! 3. write a marker naming the checkpoint, then the *other* header slot with
//!    `iteration + 1`, the new footer location and the new `file_len`;
//! 4. fsync;
//! 5. roll the WAL.
//!
//! Until step 3 is on disk the current slot still names the old footer, which
//! the file still holds: only ranges free in that footer are overwritten. The
//! marker lets recovery finish a checkpoint whose slot landed but whose WAL
//! roll did not, without replaying the folded records twice.
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
//! `PINKDB01`–`PINKDB06` are **not** readable: their readers, fixtures and
//! compatibility tests are gone. Such a file (a v6 file ends with its
//! `PINKDB06` trailer magic, older layouts start with theirs) is refused with
//! `this .pink file was written by an older pinkdb (PINKDB0N); this build
//! reads PINKDB07 only`.
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
    DataTable, ForeignKeyConstraint, Index, KeyConstraint, KeyKind, PinkDb, SchemaPath,
    TableConstraints,
};
use crate::strings::StrVec;
#[cfg(test)]
use crate::types::ArrayElem;
use crate::types::{DataType, Interval, PinkDbError, Result, Value};
use crate::vector::{CHUNK_SIZE, Vector, VectorData};

/// The magic of the current `.pink` format: the first 8 bytes of both header
/// slots.
pub const MAGIC_V7: &[u8; 8] = b"PINKDB07";

/// One header slot's size. The file starts with two of them.
pub const HEADER_SLOT_LEN: usize = 4096;

/// Where the data region starts: right after the two header slots.
pub const DATA_START: u64 = 2 * HEADER_SLOT_LEN as u64;

/// One header slot's recorded bytes: `magic (8) | iteration u64 |
/// footer_offset u64 | footer_len u64 | footer_crc32c u32 | file_len u64 |
/// slot_crc32c u32`. The rest of the 4,096-byte slot is zero padding, and the
/// slot CRC covers everything before it.
const SLOT_LEN: usize = 48;

/// One header slot: where the footer is, and how long the file was when the
/// slot was written.
///
/// A slot is valid when its magic and CRC are right and the footer it names
/// lies inside its own `file_len`. The newest valid slot wins; see
/// [`read_slots`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Slot {
    /// Increases with every write of the file: the higher valid one wins.
    pub(crate) iteration: u64,
    /// The footer's absolute offset and length in the file.
    pub(crate) footer_offset: u64,
    pub(crate) footer_len: u64,
    /// The CRC32C of the footer's bytes.
    pub(crate) footer_crc32c: u32,
    /// The file's length when the slot was written: a footer range past it is
    /// a torn slot, not a short file.
    pub(crate) file_len: u64,
}

impl Slot {
    /// The slot's recorded bytes, the CRC32C last.
    fn encode(&self) -> [u8; SLOT_LEN] {
        let mut out = [0u8; SLOT_LEN];
        out[0..8].copy_from_slice(MAGIC_V7);
        out[8..16].copy_from_slice(&self.iteration.to_le_bytes());
        out[16..24].copy_from_slice(&self.footer_offset.to_le_bytes());
        out[24..32].copy_from_slice(&self.footer_len.to_le_bytes());
        out[32..36].copy_from_slice(&self.footer_crc32c.to_le_bytes());
        out[36..44].copy_from_slice(&self.file_len.to_le_bytes());
        let crc = crc32c(&out[..44]);
        out[44..48].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// Parse one slot, when it is valid on its own: the magic, the slot CRC
    /// and a footer range inside the recorded file length. Whether that file
    /// length is the file's actual length is the caller's check.
    fn parse(bytes: &[u8]) -> Option<Slot> {
        let slot = bytes.get(..SLOT_LEN)?;
        if &slot[0..8] != MAGIC_V7 {
            return None;
        }
        let crc = u32::from_le_bytes(slot[44..48].try_into().ok()?);
        if crc32c(&slot[..44]) != crc {
            return None;
        }
        let slot = Slot {
            iteration: u64::from_le_bytes(slot[8..16].try_into().ok()?),
            footer_offset: u64::from_le_bytes(slot[16..24].try_into().ok()?),
            footer_len: u64::from_le_bytes(slot[24..32].try_into().ok()?),
            footer_crc32c: u32::from_le_bytes(slot[32..36].try_into().ok()?),
            file_len: u64::from_le_bytes(slot[36..44].try_into().ok()?),
        };
        // The footer sits after the header slab and inside the file the slot
        // was written for.
        if slot.footer_offset < DATA_START
            || slot.footer_offset.checked_add(slot.footer_len)? > slot.file_len
        {
            return None;
        }
        Some(slot)
    }
}

/// The valid header slot with the highest `iteration`: `(index, slot)`.
///
/// `header` is a file's first `2 * HEADER_SLOT_LEN` bytes. A zeroed slot (the
/// full rewrite leaves slot 1 zero), a bad magic, a bad slot CRC and a footer
/// range past the slot's `file_len` all make a slot invalid; if neither slot
/// is valid there is nothing to read.
///
/// The full rewrite writes slot 0 only; an incremental checkpoint writes the
/// *other* slot with `iteration + 1` once the bytes it names are on disk.
pub(crate) fn read_slots(header: &[u8]) -> Option<(usize, Slot)> {
    pick_slot(header, u64::MAX)
}

/// [`read_slots`] for a reader that knows the file's length: a slot recording
/// a longer file is the torn half of a write and does not count.
fn valid_slot(header: &[u8], file_len: u64) -> Option<(usize, Slot)> {
    pick_slot(header, file_len)
}

fn pick_slot(header: &[u8], file_len: u64) -> Option<(usize, Slot)> {
    let mut best: Option<(usize, Slot)> = None;
    for idx in 0..2 {
        let start = idx * HEADER_SLOT_LEN;
        let Some(bytes) = header.get(start..start + HEADER_SLOT_LEN) else {
            continue;
        };
        let Some(slot) = Slot::parse(bytes) else {
            continue;
        };
        if slot.file_len > file_len {
            continue;
        }
        if best.is_none_or(|(_, best)| slot.iteration > best.iteration) {
            best = Some((idx, slot));
        }
    }
    best
}

/// Write `slot` into header slot `idx` (0 or 1) of `file`, positioned, and
/// leave the rest of the slot zero.
///
/// The caller decides when: the full rewrite writes slot 0 after the data
/// region and the footer are on disk, and an append-only checkpoint writes
/// the *other* slot after the new footer, with `iteration + 1`. Either way a
/// slot only ever names bytes the file already holds, so a torn write leaves
/// the previous slot valid.
pub(crate) fn write_slot(file: &mut std::fs::File, idx: usize, slot: &Slot) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let mut buf = [0u8; HEADER_SLOT_LEN];
    buf[..SLOT_LEN].copy_from_slice(&slot.encode());
    file.seek(SeekFrom::Start((idx * HEADER_SLOT_LEN) as u64))?;
    file.write_all(&buf)?;
    Ok(())
}

/// The rows per row group: 60 engine chunks of 2,048 rows.
pub const ROW_GROUP_ROWS: usize = 122_880;

/// The engine chunks per row group (60 × 2,048 = 122,880 rows).
pub const CHUNKS_PER_ROW_GROUP: usize = ROW_GROUP_ROWS / CHUNK_SIZE;

/// The footer's own version word (bumped when the footer layout changes).
/// Version 7 (wave 136) adds the per-chunk row counts to every row group, so
/// a lazy open knows every chunk's row count without decoding it, and the
/// `free_ranges` section. Version 6
/// (wave 132b) always carries the store's persistent-secret section and a
/// lake's declared data-file format; 5 (wave 139a) stores a column DEFAULT as
/// its SQL text (the text is rebound at load); 4 (wave 131a) puts every
/// database's catalog sections under its schemas. Files written by older
/// builds are refused.
const FOOTER_VERSION: u32 = 7;

/// A database's storage tag in the footer: rows in the file.
const STORAGE_LOCAL: u8 = 0;
/// A database's storage tag in the footer: a lake (its location, its options
/// and the data-file format it declares for new tables follow, and the
/// database carries no tables or catalog sections in the file). Tag 1 — the
/// pre-wave-132b lake row — is not read, like every other older layout.
const STORAGE_LAKE: u8 = 2;

/// Flags bit 0: every table carries the constraint section described in the
/// module docs. Set iff at least one table has constraints.
const FLAG_CONSTRAINTS: u32 = 0x1;

/// Flags bit 1: every table carries the column-default section (wave 65)
/// described in the module docs. Set iff at least one column has a
/// DEFAULT.
const FLAG_DEFAULTS: u32 = 0x2;

/// Flags bit 2: every schema carries the materialized-view section
/// (wave 80: name, refresh policy, defining SQL, last refresh stamp, refresh
/// count) after its tables. Set iff at least one database has a materialized
/// view.
const FLAG_MATERIALIZED_VIEWS: u32 = 0x4;

/// Flags bit 3: every table carries the hash-index section (definitions +
/// statistics, see the module docs) after its row-group directory. Set iff
/// at least one table has an index.
const FLAG_INDEXES: u32 = 0x8;
/// Vector index definitions + serialized graphs (wave 107b).
const FLAG_VECTOR_INDEXES: u32 = 0x100;

/// [`FLAG_PROCEDURES`], visible to the crate's tests (the store's flags word
/// is asserted from `crate::procedure`'s persistence tests).
#[cfg(test)]
pub(crate) const FLAG_PROCEDURES_FOR_TEST: u32 = FLAG_PROCEDURES;

/// Flags bit 4: every schema carries the stored-procedure section (wave
/// 44: name, typed parameters, language tag, body text) after its tables
/// and materialized views. Set iff at least one database has a procedure.
const FLAG_PROCEDURES: u32 = 0x10;

/// Flags bit 5: the file's footer carries the store-global RBAC section
/// (wave 48: users with their Argon2id PHC password hashes, roles, role
/// memberships, table and database grants, and the global `ADMIN` holders)
/// after every database. Set iff the store has any RBAC data.
const FLAG_RBAC: u32 = 0x20;

/// Flags bit 6: every schema carries the SQL-macro section (wave 77: the
/// name and the full `CREATE MACRO` statement text) after its tables and
/// before its materialized-view section. Set iff at least one database has
/// a macro.
///
/// The macros are written **before** the materialized views on purpose: an
/// MV's defining SQL may call a macro and is rebound at load, so the macro
/// must already be in the catalog when that rebind runs.
const FLAG_MACROS: u32 = 0x40;

/// Wave 100: the file carries a per-schema trigger section. Each trigger is
/// stored as the `CREATE TRIGGER …` text it was created from — a definition is
/// re-parsed on load, exactly like a macro or a materialized view, so nothing
/// is duplicated between the catalog and the file.
const FLAG_TRIGGERS: u32 = 0x80;

/// Statistics value tags in the footer's per-chunk statistics block, plus an
/// explicit "no value" tag (an empty or all-NULL column has neither a min nor
/// a max).
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
const SV_UUID: u8 = 10;

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
    /// Schema name (the catalog key) the table lives in.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Exact row count.
    pub rows: u64,
    /// Per-column statistics in schema (scan) order.
    pub columns: Vec<SavedColumnStats>,
}

/// The zone map type the `.pink` writer collects while encoding: the same
/// [`crate::stats::Zone`] the local tables keep and the scans prune with.
type ChunkColumnStats = crate::stats::Zone;

fn dtype_tag(t: DataType) -> Result<u8> {
    Ok(match t {
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
        // UUID is a fixed 16-byte payload, so the tag carries no parameters.
        DataType::Uuid => 11,
        // The 128-bit integers have no layout in this format yet (step 5 of
        // `docs/slt-3-hugeint-plan.md`). Refusing here keeps every writer
        // from emitting a tag a reader would reject as corrupt.
        DataType::Hugeint | DataType::Uhugeint => {
            return Err(PinkDbError::Execution(format!(
                "{} is not yet supported in .pink storage",
                t.name()
            )));
        }
    })
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
        11 => Ok(DataType::Uuid),
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

/// Where a serialization's bytes go: an in-memory buffer or the file itself.
///
/// The data region is written through this one path, so a `save`'s file and
/// the lake's in-memory `to_pink_bytes*` images are byte-identical by
/// construction.
trait Sink {
    fn bytes(&mut self, b: &[u8]) -> Result<()>;

    /// The offset the next byte lands at: the row-group directory records
    /// absolute file offsets, and the slot records the file length.
    fn offset(&self) -> u64;

    /// Write header slot `idx` (see [`Slot`]).
    ///
    /// Only the last write of an image uses it, so a file sink may seek away
    /// from the end here.
    fn write_slot(&mut self, idx: usize, slot: &Slot) -> Result<()>;
}

/// A [`Sink`] over a `Vec`: the `to_pink_bytes*` API and pink-lake's data
/// files.
struct VecSink {
    buf: Vec<u8>,
}

impl VecSink {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }
}

impl Sink for VecSink {
    fn bytes(&mut self, b: &[u8]) -> Result<()> {
        self.buf.extend_from_slice(b);
        Ok(())
    }

    fn offset(&self) -> u64 {
        self.buf.len() as u64
    }

    fn write_slot(&mut self, idx: usize, slot: &Slot) -> Result<()> {
        // The image starts with both slots zeroed, so the padding is already
        // zero and only the recorded bytes change.
        let start = idx * HEADER_SLOT_LEN;
        debug_assert!(self.buf.len() >= start + HEADER_SLOT_LEN);
        self.buf[start..start + SLOT_LEN].copy_from_slice(&slot.encode());
        Ok(())
    }
}

/// A positioned [`Sink`] over an open file: buffered appends plus the running
/// offset. `save`/checkpoint write the data region through it, so only one
/// row group is ever held in memory.
struct FileSink {
    out: std::io::BufWriter<std::fs::File>,
    pos: u64,
}

impl FileSink {
    fn new(file: std::fs::File) -> Self {
        // 1 MiB: large enough that the column chunks reach the file in a few
        // writes, small enough to stay far below a row group's payload.
        Self {
            out: std::io::BufWriter::with_capacity(1 << 20, file),
            pos: 0,
        }
    }

    /// Flush the buffered bytes and hand the file back for the fsync.
    fn finish(self) -> Result<std::fs::File> {
        Ok(self.out.into_inner().map_err(|e| e.into_error())?)
    }
}

impl Sink for FileSink {
    fn bytes(&mut self, b: &[u8]) -> Result<()> {
        use std::io::Write;
        self.out.write_all(b)?;
        self.pos += b.len() as u64;
        Ok(())
    }

    fn offset(&self) -> u64 {
        self.pos
    }

    fn write_slot(&mut self, idx: usize, slot: &Slot) -> Result<()> {
        use std::io::Write;
        // Flush the buffered tail first: the slot is the last write of the
        // image, so nothing is appended through the buffer afterwards.
        self.out.flush()?;
        write_slot(self.out.get_mut(), idx, slot)
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

fn varchar_payload(v: &Vector) -> Vec<&str> {
    match v.data() {
        VectorData::VarChar(data) => {
            if v.has_nulls() {
                let words = v.validity_words();
                data.iter()
                    .enumerate()
                    .map(|(i, s)| if valid_at(words, i) { s } else { "" })
                    .collect()
            } else {
                data.iter().collect()
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
        // UUID: `u32 row count | raw 16-byte little-endian values`, the same
        // shape as wide DECIMAL. Invalid rows are zeroed so the bytes stay
        // deterministic; the validity bitmap wins on decode.
        DataType::Uuid => {
            let VectorData::Uuid(values) = v.data() else {
                unreachable!("uuid chunk has a non-uuid payload")
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
        // Guarded by `write_table`'s refusal: a table with a 128-bit column
        // never reaches the encoder (step 5 gives it a layout).
        DataType::Hugeint | DataType::Uhugeint => {
            unreachable!("128-bit integer chunks have no .pink layout yet")
        }
    }
    best.expect("at least plain encoding")
}

// ---------- chunk decode (typed) ----------

/// Reads `count+1` offsets then the data block; the strings stay in one
/// shared buffer (dictionary rows point into it, wave 128a).
fn decode_strs(reader: &mut Reader, count: usize) -> Result<StrVec> {
    let mut offsets = Vec::with_capacity(count + 1);
    for _ in 0..count + 1 {
        offsets.push(reader.u32()? as usize);
    }
    let total = *offsets.last().unwrap_or(&0);
    let data = reader.take(total)?;
    strs_over(reader, &offsets, data)
}

/// The strings at `offsets` (`count + 1` boundaries) of `data`, validated as
/// UTF-8 and kept in one shared buffer (wave 128a: rows of a chunk point
/// into it instead of each owning a copy).
fn strs_over(reader: &Reader, offsets: &[usize], data: &[u8]) -> Result<StrVec> {
    let count = offsets.len().saturating_sub(1);
    for i in 0..count {
        let (a, b) = (offsets[i], offsets[i + 1]);
        if b < a || b > data.len() {
            return reader.err("bad string offsets");
        }
        std::str::from_utf8(&data[a..b])
            .map_err(|_| PinkDbError::Execution("corrupt file: bad UTF-8".to_string()))?;
    }
    let buffer: Arc<[u8]> = Arc::from(data);
    Ok(StrVec::over_buffer(
        buffer,
        (0..count).map(|i| (offsets[i], offsets[i + 1] - offsets[i])),
    ))
}

/// Rows of a dictionary-coded chunk: each row shares its entry's bytes.
fn dict_rows(dict: &StrVec, codes: &[usize]) -> Result<StrVec> {
    if codes.iter().any(|&c| c >= dict.len()) {
        return Err(PinkDbError::Execution(
            "corrupt file: bad dict code".to_string(),
        ));
    }
    Ok(dict.take(codes.iter().copied()))
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

fn decode_rle_strs(r: &mut Reader, n: usize, runs: usize) -> Result<StrVec> {
    let vals = decode_strs(r, runs)?;
    let mut codes = Vec::with_capacity(n);
    for v in 0..vals.len() {
        let len = r.u32()? as usize;
        if codes.len().checked_add(len).is_none_or(|total| total > n) {
            return r.err("RLE run overflows chunk length");
        }
        codes.extend(std::iter::repeat_n(v, len));
    }
    if codes.len() != n {
        return r.err("RLE runs underfill chunk length");
    }
    dict_rows(&vals, &codes)
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
        (DataType::Uuid, ENC_PLAIN) => {
            let n = r.chunk_count("chunk length")?;
            let mut values = Vec::with_capacity(n);
            for _ in 0..n {
                let b = r.take(16)?;
                let mut arr = [0u8; 16];
                arr.copy_from_slice(b);
                values.push(u128::from_le_bytes(arr));
            }
            Ok((VectorData::Uuid(values), n))
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
            let mut codes = Vec::with_capacity(n);
            for _ in 0..n {
                codes.push(r.u32()? as usize);
            }
            Ok((VectorData::VarChar(dict_rows(&dict, &codes)?), n))
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
            let codes: Vec<usize> = if width == 0 {
                // At most one dictionary entry; every row (if any) is it.
                vec![0; n]
            } else {
                fastlanes_unpack(r, n, width)?
                    .into_iter()
                    .map(|c| c as usize)
                    .collect()
            };
            Ok((VectorData::VarChar(dict_rows(&dict, &codes)?), n))
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
    Ok((VectorData::VarChar(strs_over(r, &offsets, &data)?), n))
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

/// One row group's directory entry: its row count, its per-chunk row counts
/// and one entry per column.
#[derive(Debug, Clone)]
struct RowGroupDir {
    /// The row group's exact row count.
    rows: u64,
    /// The rows in each engine chunk of the group, in order: every column
    /// shares these boundaries, so a lazy open builds the handles from them
    /// without decoding a page.
    chunk_rows: Vec<u32>,
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

/// One vector index definition and its serialized graph.
#[derive(Debug, Clone)]
struct VectorIndexFile {
    name: String,
    column: usize,
    provider: String,
    options: Vec<(String, String)>,
    blob: Vec<u8>,
}

/// One table's footer entry: its schema, its catalog metadata and the
/// row-group directory of its data.
#[derive(Debug, Clone)]
struct TableDir {
    name: String,
    schema: Vec<(String, DataType)>,
    constraints: TableConstraints,
    defaults: Vec<Option<String>>,
    indexes: Vec<IndexDef>,
    vector_indexes: Vec<VectorIndexFile>,
    row_count: u64,
    row_groups: Vec<RowGroupDir>,
    /// Catalog statistics and per-chunk zone maps (wave 122), written after
    /// the row groups. A rows-free catalog image carries
    /// [`crate::stats::TableStats::unknown`] and no zones.
    stats: crate::stats::TableStats,
    zones: Vec<Vec<crate::stats::Zone>>,
}

/// One schema's footer entry: its tables and its catalog sections.
#[derive(Debug)]
struct SchemaDir {
    /// The display name (the key is its lowercased form).
    name: String,
    tables: Vec<TableDir>,
    macros: Vec<crate::macros::MacroDef>,
    materialized_views: Vec<crate::storage::MaterializedViewDef>,
    procedures: Vec<crate::procedure::ProcedureDef>,
    triggers: Vec<crate::trigger::TriggerDef>,
}

/// One database's footer entry: its storage and its schemas (sorted by key).
#[derive(Debug)]
struct DbDir {
    name: String,
    storage: crate::lake_db::DbStorage,
    schemas: Vec<SchemaDir>,
}

/// The parsed footer of a `PINKDB07` file.
#[derive(Debug)]
struct StoreFooter {
    databases: Vec<DbDir>,
    rbac: Option<crate::rbac::RbacCatalog>,
    secrets: Vec<crate::secret::Secret>,
    /// The data-region holes no row group references. An incremental
    /// checkpoint allocates from these; a full rewrite always writes empty.
    free_ranges: Vec<(u64, u64)>,
}

/// One encoded page (one engine chunk): its framed bytes and its statistics.
struct EncodedPage {
    /// The page frame: `u8 tag | u32 validity_len | validity | payload`.
    frame: Vec<u8>,
    /// The page's exact statistics.
    stats: ChunkColumnStats,
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
        stats: crate::stats::Zone::of_vector(v),
    }
}

/// Write one statistics value (the footer's per-chunk min/max): a tag byte
/// plus the value's payload.
/// Write one table's catalog statistics and per-chunk zone maps: the
/// footer's stats block (wave 122).
///
/// One entry per column: the NULL count, the min/max bounds, whether the
/// distinct sketch is an upper bound after a delete (`dirty`), and the sketch
/// itself (`hll_len` 0 = unknown). Then the zone maps: one entry per engine
/// chunk, in chunk order, so a load restores the per-chunk pruning facts
/// without rescanning the data (`RowGroupDir`'s entries are per *row group*,
/// far too coarse to prune a 2048-row chunk with).
fn write_table_stats(
    w: &mut Writer,
    stats: &crate::stats::TableStats,
    zones: &[Vec<crate::stats::Zone>],
) {
    for col in &stats.cols {
        w.u64(col.null_count);
        write_stat_value(w, col.min.as_ref());
        write_stat_value(w, col.max.as_ref());
        w.u8(u8::from(col.dirty));
        match &col.hll {
            Some(hll) => {
                let bytes = hll.to_bytes();
                w.u32(bytes.len() as u32);
                w.bytes(&bytes);
            }
            // Unknown (a rows-free catalog image, or a column whose sketch
            // was never built): readers treat the distinct count as unknown
            // rather than as zero.
            None => w.u32(0),
        }
    }
    w.u32(zones.len() as u32);
    for col in zones {
        w.u32(col.len() as u32);
        for zone in col {
            w.u64(zone.null_count);
            write_stat_value(w, zone.min.as_ref());
            write_stat_value(w, zone.max.as_ref());
            w.u64(zone.nan_count);
        }
    }
}

/// Read [`write_table_stats`]; every count is checked against the remaining
/// bytes before it sizes an allocation.
fn read_table_stats(
    r: &mut Reader,
    n_cols: usize,
    rows: u64,
) -> Result<(crate::stats::TableStats, Vec<Vec<crate::stats::Zone>>)> {
    let mut cols = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        let null_count = r.u64()?;
        let min = read_stat_value(r)?;
        let max = read_stat_value(r)?;
        let dirty = match r.u8()? {
            0 => false,
            1 => true,
            other => return r.err(&format!("bad statistics dirty byte {other}")),
        };
        let hll_len = r.u32()? as usize;
        // A sketch is either absent or within one precision's bound.
        if hll_len > crate::stats::HLL_MAX_BYTES {
            return r.err(&format!("bad distinct sketch length {hll_len}"));
        }
        let hll = if hll_len == 0 {
            None
        } else {
            let sketch = crate::stats::Hll::from_bytes(r.take(hll_len)?)
                .ok_or_else(|| PinkDbError::Execution("corrupt file: bad sketch".to_string()))?;
            Some(std::sync::Arc::new(sketch))
        };
        cols.push(crate::stats::ColStats {
            null_count,
            min,
            max,
            hll,
            dirty,
        });
    }
    let n_zone_cols = r.u32()? as usize;
    if n_zone_cols != n_cols {
        return r.err("zone-map column count does not match the schema");
    }
    let mut zones = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        let n_zones = r.u32()? as usize;
        // Each zone costs at least 18 bytes (NULL count, two empty bounds,
        // the NaN count).
        if n_zones > r.remaining() / 18 + 1 {
            return r.err("zone count exceeds the remaining file");
        }
        let mut col = Vec::with_capacity(n_zones);
        for _ in 0..n_zones {
            col.push(crate::stats::Zone {
                null_count: r.u64()?,
                min: read_stat_value(r)?,
                max: read_stat_value(r)?,
                nan_count: r.u64()?,
            });
        }
        zones.push(col);
    }
    Ok((crate::stats::TableStats { rows, cols }, zones))
}

fn write_stat_value(w: &mut Writer, v: Option<&Value>) {
    match v {
        None => w.u8(SV_NONE),
        Some(Value::Array(_)) => w.u8(SV_NONE),
        // Guarded by `write_table`'s refusal, and no reader can produce a
        // 128-bit value from a saved zone map: keep the file self-consistent
        // rather than inventing a tag before step 5.
        Some(Value::Hugeint(_) | Value::Uhugeint(_)) => w.u8(SV_NONE),
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
        Some(Value::Decimal {
            value: x, scale, ..
        }) => {
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
        Some(Value::Uuid(bits)) => {
            w.u8(SV_UUID);
            w.bytes(&bits.to_le_bytes());
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
            Some(Value::decimal(x, scale))
        }
        SV_INTERVAL => Some(Value::Interval {
            months: r.i32()?,
            days: r.i32()?,
            micros: r.i64()?,
        }),
        SV_UUID => {
            let b = r.take(16)?;
            let mut arr = [0u8; 16];
            arr.copy_from_slice(b);
            Some(Value::Uuid(u128::from_le_bytes(arr)))
        }
        other => {
            return Err(PinkDbError::Execution(format!(
                "corrupt file: unknown statistics value tag {other}"
            )));
        }
    };
    Ok(v)
}

/// The index definitions of one table (the map itself is never stored: a lazy
/// open leaves every map unbuilt and the first indexed lookup builds it).
///
/// `null_counts` are the exact NULL counts of the table's columns (the write
/// pass observed them), so the definition does not read a single row.
fn index_defs(t: &DataTable, null_counts: &[u64]) -> Vec<IndexDef> {
    let mut indexes: Vec<&Index> = t
        .indexes
        .iter()
        .filter(|i| i.column < t.columns.len())
        .collect();
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    indexes
        .into_iter()
        .map(|index| {
            // A map that was never built still has a distinct count: save may
            // decode the indexed column, so two loads of the same rows write
            // the same bytes.
            let distinct = match index.map().as_ref() {
                Some(map) => map.len() as u64,
                None => crate::storage::index_map_for(&t.columns[index.column])
                    .map_or(0, |map| map.len() as u64),
            };
            let nulls = null_counts.get(index.column).copied().unwrap_or(0);
            let indexed = (t.num_rows() as u64).saturating_sub(nulls);
            IndexDef {
                name: index.name.clone(),
                column: index.column,
                distinct_keys: distinct,
                indexed_rows: indexed,
            }
        })
        .collect()
}

/// The metadata image's index entries: the same definitions with their row
/// statistics cleared. The statistics describe rows the image does not carry
/// (they live in the lake's data files, whose chunks the loader appends after
/// decoding), and the `.pink` reader refuses statistics larger than the table.
fn index_defs_without_rows(t: &DataTable) -> Vec<IndexDef> {
    let mut defs: Vec<IndexDef> = t
        .indexes
        .iter()
        .filter(|i| i.column < t.columns.len())
        .map(|i| IndexDef {
            name: i.name.clone(),
            column: i.column,
            distinct_keys: 0,
            indexed_rows: 0,
        })
        .collect();
    defs.sort_by(|a, b| a.name.cmp(&b.name));
    defs
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

/// Serialize one table's vector indexes (graphs included). Sorted by name so
/// a save is deterministic.
fn vector_index_defs(t: &DataTable) -> Result<Vec<VectorIndexFile>> {
    let mut defs: Vec<&crate::storage::VectorIndexDef> = t
        .vector_indexes
        .iter()
        .filter(|i| i.column < t.columns.len())
        .collect();
    defs.sort_by(|a, b| a.name.cmp(&b.name));
    let mut out = Vec::with_capacity(defs.len());
    for def in defs {
        // Mutations are refused while a graph is pending, so its stored blob
        // is still current: a default build can save such a store untouched.
        let blob = match (&def.index, &def.blob) {
            (Some(index), _) => index.serialize()?,
            (None, Some(blob)) => blob.to_vec(),
            (None, None) => return Err(def.pending_error(&t.name)),
        };
        out.push(VectorIndexFile {
            name: def.name.clone(),
            column: def.column,
            provider: def.provider.clone(),
            options: def.options.clone(),
            blob,
        });
    }
    Ok(out)
}

fn write_vector_index_defs(w: &mut Writer, defs: &[VectorIndexFile]) {
    w.u32(defs.len() as u32);
    for d in defs {
        w.str(&d.name);
        w.u32(d.column as u32);
        w.str(&d.provider);
        w.u32(d.options.len() as u32);
        for (k, v) in &d.options {
            w.str(k);
            w.str(v);
        }
        w.u64(d.blob.len() as u64);
        w.bytes(&d.blob);
    }
}

fn read_vector_index_defs(r: &mut Reader) -> Result<Vec<VectorIndexFile>> {
    let n = r.u32()? as usize;
    if n > r.remaining() / 16 {
        return r.err("vector index count exceeds the remaining file");
    }
    let mut out: Vec<VectorIndexFile> = Vec::with_capacity(n);
    for _ in 0..n {
        let name = r.str()?;
        if name.is_empty() {
            return r.err("empty vector index name");
        }
        if let Some(prev) = out.last()
            && prev.name >= name
        {
            return r.err(&format!(
                "vector index \"{name}\" is duplicated or out of name order"
            ));
        }
        let column = r.u32()? as usize;
        let provider = r.str()?;
        if provider.is_empty() {
            return r.err("empty vector index provider");
        }
        let n_options = r.u32()? as usize;
        if n_options > r.remaining() / 8 + 1 {
            return r.err("vector index option count exceeds the remaining file");
        }
        let mut options = Vec::with_capacity(n_options);
        for _ in 0..n_options {
            options.push((r.str()?, r.str()?));
        }
        let blob_len = r.u64()?;
        let blob_len = usize::try_from(blob_len)
            .map_err(|_| PinkDbError::Execution("corrupt file: vector graph too large".into()))?;
        let blob = r.take(blob_len)?.to_vec();
        out.push(VectorIndexFile {
            name,
            column,
            provider,
            options,
            blob,
        });
    }
    Ok(out)
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

/// Install one table's index definitions, with every map left unbuilt.
///
/// The maps are **never** read from the file and never built here: each one is
/// built from the live rows by the first indexed lookup, so it can never
/// disagree with the rows it indexes, whatever the file claims. The
/// statistics are only bounded by the table's real row count.
fn apply_indexes(t: &mut DataTable, defs: &[IndexDef]) -> Result<()> {
    let mut out: Vec<Index> = Vec::with_capacity(defs.len());
    for def in defs {
        if def.column >= t.columns.len() {
            return Err(PinkDbError::Execution(format!(
                "corrupt file: index \"{}\" names column {} of a {}-column table",
                def.name,
                def.column,
                t.columns.len()
            )));
        }
        if def.indexed_rows > t.num_rows() as u64 || def.distinct_keys > def.indexed_rows {
            return Err(PinkDbError::Execution(format!(
                "corrupt file: index \"{}\" statistics exceed the table",
                def.name
            )));
        }
        out.push(Index::new(def.name.clone(), def.column));
    }
    t.indexes = out;
    t.indexes_stale = false;
    Ok(())
}

// ---------- the public API ----------

impl PinkDb {
    /// Serialize every database (and every table in it) in the `.pink`
    /// columnar format (`PINKDB07`). Databases and tables are stored in
    /// sorted name order (deterministic bytes). Views are session-local and
    /// never persisted.
    fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.to_image_inner(true, false, false)?.0)
    }

    /// The engine catalog image (wave 96b): every database, macro, materialized
    /// view, procedure, trigger and RBAC entry, with each table's schema,
    /// constraints, defaults and index definitions but **no rows**. The lake
    /// holds the rows; this is the metadata a stateless server restores from
    /// the catalog.
    pub(crate) fn to_metadata_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.to_image_inner(true, false, true)?.0)
    }

    /// [`PinkDb::to_bytes`] with the chunk-parallel encode disabled (test
    /// switch: both paths must produce identical bytes).
    #[cfg(test)]
    fn to_bytes_impl(&self, parallel: bool) -> Result<Vec<u8>> {
        Ok(self.to_image_inner(parallel, false, false)?.0)
    }

    /// The one serialization path: a `PINKDB07` image, optionally with the
    /// exact per-table statistics collected in the same chunk pass. The
    /// statistics never influence the bytes (`to_image_inner(_, false).0` is
    /// bit-identical to `to_image_inner(_, true).0`).
    fn to_image_inner(
        &self,
        parallel: bool,
        collect_stats: bool,
        omit_rows: bool,
    ) -> Result<(Vec<u8>, Vec<SavedTableStats>)> {
        let mut data = VecSink::new();
        let saved = self.write_image(&mut data, parallel, collect_stats, omit_rows)?;
        Ok((data.buf, saved))
    }

    /// [`PinkDb::to_image_inner`]'s image, written through `data`: the one
    /// serialization path, shared by the `Vec` sink of the in-memory API and
    /// the file sink of `save`/checkpoint.
    fn write_image<S: Sink>(
        &self,
        data: &mut S,
        parallel: bool,
        collect_stats: bool,
        omit_rows: bool,
    ) -> Result<Vec<SavedTableStats>> {
        #[cfg(test)]
        let hooks = Hooks::default();
        #[cfg(not(test))]
        let hooks = Hooks;
        self.write_image_hooked(data, parallel, collect_stats, omit_rows, &hooks)
    }

    /// [`PinkDb::write_image`] with the test-only knobs: a non-empty
    /// `free_ranges` section, or a second header slot with a newer iteration.
    fn write_image_hooked<S: Sink>(
        &self,
        data: &mut S,
        parallel: bool,
        collect_stats: bool,
        omit_rows: bool,
        hooks: &Hooks,
    ) -> Result<Vec<SavedTableStats>> {
        // Both header slots, zero: neither is valid until the slot at the end
        // names a footer that is already on disk.
        data.bytes(&[0u8; 2 * HEADER_SLOT_LEN])?;
        // The optional catalog sections exist iff some object needs them (the
        // flags word is written in the footer, so decide it up front).
        let all_tables: Vec<&crate::storage::DataTable> = self
            .list_databases()
            .iter()
            .flat_map(|db| self.all_tables_of(db))
            .map(|(_, _, t)| t)
            .collect();
        let flags = self.image_flags(&all_tables);

        // The data region. Every table this pass wrote, with the new home of
        // each chunk it re-encoded (see `write_table`).
        let mut saved: Vec<SavedTableStats> = Vec::new();
        let mut carry: Vec<u8> = Vec::new();
        let mut repoints: Vec<(&DataTable, TableCarry)> = Vec::new();
        let db_dirs = self.catalog_dirs(flags, |ns, name, t| {
            if omit_rows {
                Ok(TableDir {
                    name: name.to_string(),
                    schema: t.schema(),
                    constraints: t.constraints.clone(),
                    defaults: t
                        .columns
                        .iter()
                        .map(|c| c.default.as_ref().map(|d| d.text.clone()))
                        .collect(),
                    indexes: index_defs_without_rows(t),
                    // A rows-free catalog image cannot carry a graph whose nodes
                    // are row ids; vector indexes are only built on native
                    // tables, whose rows are omitted too.
                    vector_indexes: Vec::new(),
                    row_count: 0,
                    row_groups: Vec::new(),
                    // No rows, no statistics: readers treat the column facts as
                    // unknown until the rows arrive.
                    stats: crate::stats::TableStats::unknown(t.columns.len(), 0),
                    zones: vec![Vec::new(); t.columns.len()],
                })
            } else {
                let (dir, carried) = write_table(
                    data,
                    &mut carry,
                    ns,
                    name,
                    t,
                    parallel,
                    collect_stats,
                    &mut saved,
                )?;
                repoints.push((t, carried));
                Ok(dir)
            }
        })?;

        // The footer. A full rewrite leaves no free ranges; the incremental
        // checkpoint path builds its own footer from the same function.
        let free_ranges: &[(u64, u64)] = {
            #[cfg(test)]
            {
                &hooks.free_ranges
            }
            #[cfg(not(test))]
            {
                let _ = hooks;
                &[]
            }
        };
        let footer = self.build_footer(flags, free_ranges, &db_dirs)?;
        // The footer, then slot 0: the footer is on disk before any slot
        // names it, and slot 1 stays zero, so a torn write leaves no valid
        // slot that names a missing footer.
        let footer_offset = data.offset();
        let crc = crc32c(&footer);
        data.bytes(&footer)?;
        let slot = Slot {
            iteration: 1,
            footer_offset,
            footer_len: footer.len() as u64,
            footer_crc32c: crc,
            file_len: data.offset(),
        };
        data.write_slot(0, &slot)?;
        #[cfg(test)]
        if hooks.second_slot {
            data.write_slot(
                1,
                &Slot {
                    iteration: 2,
                    ..slot
                },
            )?;
        }
        // The handles of the chunks this pass wrote point at the carried
        // column bytes: a saved write's chunk is stored (and evictable)
        // without a reopen, whether it was re-encoded or copied through.
        // An encrypted store's plaintext image is overwritten when its last
        // handle drops it, exactly like the image a keyed open decrypts.
        Self::repoint_carried(carry, repoints, self.encryption_key.is_some());
        Ok(saved)
    }

    /// Point re-encoded chunks at the retained image built from `carry`.
    ///
    /// `carry` holds only the data region, while chunk offsets (and the
    /// footer's) are absolute file offsets: prefix the two header slots so
    /// [`copy_through`] sees the same coordinate space after a reopen and
    /// after an in-process save.
    fn repoint_carried(carry: Vec<u8>, repoints: Vec<(&DataTable, TableCarry)>, encrypted: bool) {
        if carry.is_empty() {
            return;
        }
        let mut bytes = vec![0u8; DATA_START as usize];
        bytes.extend_from_slice(&carry);
        let image = crate::storage::StoreImage::new(bytes, encrypted);
        for (t, carried) in repoints {
            for (column, chunks) in t.columns.iter().zip(carried) {
                for (i, offset, length, rows) in chunks {
                    column.repoint_chunk(i, &image, DATA_START + offset, length, rows);
                }
            }
        }
    }

    /// Build the footer's database/schema/table directories in sorted name
    /// order, letting `table_dir` decide each table's row-group entry (a full
    /// rewrite writes it, an incremental checkpoint may reuse it).
    fn catalog_dirs<'a, F>(&'a self, flags: u32, mut table_dir: F) -> Result<Vec<DbDir>>
    where
        F: FnMut(&SchemaPath, &str, &'a DataTable) -> Result<TableDir>,
    {
        let with_mvs = flags & FLAG_MATERIALIZED_VIEWS != 0;
        let with_procedures = flags & FLAG_PROCEDURES != 0;
        let with_triggers = flags & FLAG_TRIGGERS != 0;
        let with_macros = flags & FLAG_MACROS != 0;
        let mut dbs = self.list_databases();
        dbs.sort();
        let mut db_dirs: Vec<DbDir> = Vec::with_capacity(dbs.len());
        for dbname in &dbs {
            let storage = self
                .database_storage(dbname)
                .unwrap_or(crate::lake_db::DbStorage::Local);
            // A lake database's tables and catalog objects live in its lake:
            // the file records only where that is.
            let lake = matches!(storage, crate::lake_db::DbStorage::Lake(_));
            let mut schemas: Vec<SchemaDir> = Vec::new();
            if !lake {
                let cat = self
                    .databases
                    .get(&dbname.to_ascii_lowercase())
                    .ok_or_else(|| {
                        PinkDbError::Catalog(format!("database \"{dbname}\" not found"))
                    })?;
                for (skey, scat) in &cat.schemas {
                    let ns = SchemaPath::new(dbname, skey);
                    let mut names = self.list_tables_in(&ns);
                    names.sort();
                    let mut tables: Vec<TableDir> = Vec::with_capacity(names.len());
                    for name in &names {
                        let t = self.table_in(&ns, name)?;
                        tables.push(table_dir(&ns, name, t)?);
                    }
                    schemas.push(SchemaDir {
                        name: scat.name.clone(),
                        tables,
                        // Name order, not map order: a reopened store's
                        // tables must be byte-identical to the saved ones.
                        macros: if with_macros {
                            self.macros_in(&ns).into_iter().map(|(_, d)| d).collect()
                        } else {
                            Vec::new()
                        },
                        materialized_views: if with_mvs {
                            self.materialized_views_in(&ns)
                                .into_iter()
                                .map(|(_, d)| d)
                                .collect()
                        } else {
                            Vec::new()
                        },
                        procedures: if with_procedures {
                            self.procedures_in(&ns)
                                .into_iter()
                                .map(|(_, d)| d)
                                .collect()
                        } else {
                            Vec::new()
                        },
                        // Creation order is the firing order, so the slice is
                        // written as stored — never sorted.
                        triggers: if with_triggers {
                            scat.triggers.clone()
                        } else {
                            Vec::new()
                        },
                    });
                }
            }
            db_dirs.push(DbDir {
                name: dbname.clone(),
                storage,
                schemas,
            });
        }
        Ok(db_dirs)
    }

    /// The footer's flags word, from the tables the image will carry.
    fn image_flags(&self, all_tables: &[&DataTable]) -> u32 {
        let with_constraints = all_tables.iter().any(|t| !t.constraints.is_empty());
        let with_defaults = all_tables
            .iter()
            .any(|t| t.columns.iter().any(|c| c.default.is_some()));
        let with_indexes = all_tables.iter().any(|t| !t.indexes.is_empty());
        let with_vectors = all_tables.iter().any(|t| !t.vector_indexes.is_empty());
        (if with_constraints {
            FLAG_CONSTRAINTS
        } else {
            0
        }) | if with_defaults { FLAG_DEFAULTS } else { 0 }
            | if self.has_materialized_views() {
                FLAG_MATERIALIZED_VIEWS
            } else {
                0
            }
            | if with_indexes { FLAG_INDEXES } else { 0 }
            | if with_vectors { FLAG_VECTOR_INDEXES } else { 0 }
            | if self.has_procedures() {
                FLAG_PROCEDURES
            } else {
                0
            }
            | if self.has_triggers() {
                FLAG_TRIGGERS
            } else {
                0
            }
            | if self.has_macros() { FLAG_MACROS } else { 0 }
            | if self.has_rbac_data() { FLAG_RBAC } else { 0 }
    }

    /// Build the footer bytes for `db_dirs`, including the free-range section:
    /// the one footer writer shared by the full rewrite and the incremental
    /// checkpoint.
    fn build_footer(
        &self,
        flags: u32,
        free_ranges: &[(u64, u64)],
        db_dirs: &[DbDir],
    ) -> Result<Vec<u8>> {
        let with_constraints = flags & FLAG_CONSTRAINTS != 0;
        let with_defaults = flags & FLAG_DEFAULTS != 0;
        let with_indexes = flags & FLAG_INDEXES != 0;
        let with_vectors = flags & FLAG_VECTOR_INDEXES != 0;
        let with_macros = flags & FLAG_MACROS != 0;
        let with_mvs = flags & FLAG_MATERIALIZED_VIEWS != 0;
        let with_procedures = flags & FLAG_PROCEDURES != 0;
        let with_triggers = flags & FLAG_TRIGGERS != 0;
        let with_rbac = flags & FLAG_RBAC != 0;
        let mut fw = Writer::new();
        fw.u32(FOOTER_VERSION);
        fw.u32(flags);
        fw.u32(free_ranges.len() as u32);
        for &(offset, len) in free_ranges {
            fw.u64(offset);
            fw.u64(len);
        }
        fw.u32(db_dirs.len() as u32);
        for dbdir in db_dirs {
            fw.str(&dbdir.name);
            write_storage(&mut fw, &dbdir.storage);
            fw.u32(dbdir.schemas.len() as u32);
            for sdir in &dbdir.schemas {
                fw.str(&sdir.name);
                fw.u32(sdir.tables.len() as u32);
                for tdir in &sdir.tables {
                    fw.str(&tdir.name);
                    fw.u32(tdir.schema.len() as u32);
                    for (col, dtype) in &tdir.schema {
                        fw.str(col);
                        fw.u8(dtype_tag(*dtype)?);
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
                    if with_vectors {
                        write_vector_index_defs(&mut fw, &tdir.vector_indexes);
                    }
                    fw.u64(tdir.row_count);
                    fw.u32(tdir.row_groups.len() as u32);
                    for rg in &tdir.row_groups {
                        fw.u64(rg.rows);
                        fw.u32(rg.chunk_rows.len() as u32);
                        for &rows in &rg.chunk_rows {
                            fw.u32(rows);
                        }
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
                    write_table_stats(&mut fw, &tdir.stats, &tdir.zones);
                }
                if with_macros {
                    write_macro_defs(&mut fw, &sdir.macros);
                }
                if with_mvs {
                    write_materialized_view_defs(&mut fw, &sdir.materialized_views);
                }
                if with_procedures {
                    write_procedure_defs(&mut fw, &sdir.procedures)?;
                }
                if with_triggers {
                    write_trigger_defs(&mut fw, &sdir.triggers);
                }
            }
        }
        if with_rbac {
            write_rbac(&mut fw, self);
        }
        // Wave 132b: unconditional — a section that is present even when empty
        // is what lets a load replace the checkpoint's secrets instead of
        // keeping stale ones.
        write_secrets(&mut fw, self);
        Ok(fw.buf)
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

    /// The bytes [`PinkDb::save_with_key`] would write, without staging a
    /// file. Used by object-backed checkpoint stores.
    pub fn to_pink_bytes_with_key(&self, key: &EncryptionKey) -> Result<Vec<u8>> {
        #[cfg(not(feature = "encryption"))]
        {
            let _ = key;
            Err(crate::crypto::unavailable())
        }
        #[cfg(feature = "encryption")]
        {
            self.ensure_wal_healthy()?;
            crate::crypto::seal_container(&self.to_bytes()?, key, 0)
        }
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
        self.to_image_inner(true, true, false)
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
        Self::write_atomic(std::path::Path::new(path), |sink| {
            self.write_image(sink, true, false, false).map(|_| ())
        })
    }

    /// [`PinkDb::save`] with at-rest encryption: the `PINKDB07` bytes are
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
            // Encrypted stores take the in-memory path: the `PINKENC1`
            // container seals the whole image, so it cannot be written while
            // it is still being produced (136w3 adds a block container that
            // can).
            let sealed = crate::crypto::seal_container(&self.to_bytes()?, key, 0)?;
            Self::write_atomic(std::path::Path::new(path), |sink| sink.bytes(&sealed))
        }
    }

    /// [`PinkDb::save`] plus `fsync` on the written file — the checkpoint
    /// primitive (the WAL is only rolled once this returns).
    pub(crate) fn save_sync(&self, path: &std::path::Path) -> Result<()> {
        Self::write_sync(path, |sink| {
            self.write_image(sink, true, false, false).map(|_| ())
        })
    }

    /// The checkpoint primitive: refresh the target in place when it is a
    /// plaintext v7 file with room, otherwise write a full image to `tmp`.
    /// `tmp` is what the WAL publishes in the full-rewrite case; the in-place
    /// case commits through the other header slot and leaves `tmp` untouched.
    /// `probe` decides whether freed ranges may be overwritten (see
    /// [`crate::wal::HoleProbe`]).
    pub(crate) fn save_checkpoint_sync(
        &self,
        tmp: &std::path::Path,
        target: &std::path::Path,
        wal_generation: u64,
        next_generation: u64,
        probe: &crate::wal::HoleProbe<'_>,
    ) -> Result<CheckpointSave> {
        #[cfg(test)]
        LAST_CHECKPOINT_REUSED.with(|last| last.set(None));
        if self.encryption_key.is_some() {
            return Err(PinkDbError::Execution(
                "an encrypted store cannot take an in-place checkpoint".to_string(),
            ));
        }
        let opened = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(target);
        let file = match opened {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.save_sync(tmp)?;
                return Ok(CheckpointSave::Full);
            }
            Err(e) => return Err(e.into()),
        };
        let physical_len = file.metadata()?.len();
        let Some((slot_idx, slot, footer)) = read_checkpoint_file(&file, physical_len)? else {
            drop(file);
            self.save_sync(tmp)?;
            return Ok(CheckpointSave::Full);
        };
        let free_bytes: u64 = footer.free_ranges.iter().map(|&(_, len)| len).sum();
        if free_bytes.saturating_mul(100) > slot.file_len.saturating_mul(COMPACT_FREE_PERCENT) {
            drop(file);
            self.save_sync(tmp)?;
            return Ok(CheckpointSave::Full);
        }
        let Some(iteration) = slot.iteration.checked_add(1) else {
            drop(file);
            self.save_sync(tmp)?;
            return Ok(CheckpointSave::Full);
        };
        self.checkpoint_in_place(
            file,
            target,
            slot_idx,
            slot,
            footer,
            iteration,
            wal_generation,
            next_generation,
            probe,
        )?;
        Ok(CheckpointSave::Incremental)
    }

    /// Rewrite `file` in place: clean row groups keep their footer entries,
    /// dirty ones are re-encoded into the old footer's free ranges (or the
    /// end of the file), and the new footer is written after every row group
    /// before the other slot names it. Holes are reused only while `probe`
    /// holds the store's exclusive open-lock; otherwise the pass appends.
    #[allow(clippy::too_many_arguments)]
    fn checkpoint_in_place(
        &self,
        file: std::fs::File,
        target: &std::path::Path,
        slot_idx: usize,
        old_slot: Slot,
        old_footer: StoreFooter,
        iteration: u64,
        wal_generation: u64,
        next_generation: u64,
        probe: &crate::wal::HoleProbe<'_>,
    ) -> Result<()> {
        let reuse = probe.exclusive();
        #[cfg(test)]
        LAST_CHECKPOINT_REUSED.with(|last| last.set(Some(reuse)));
        let mut old_tables: HashMap<(String, String, String), &TableDir> = HashMap::new();
        for db in &old_footer.databases {
            for schema in &db.schemas {
                for table in &schema.tables {
                    old_tables.insert(
                        (
                            db.name.to_ascii_lowercase(),
                            schema.name.to_ascii_lowercase(),
                            table.name.to_ascii_lowercase(),
                        ),
                        table,
                    );
                }
            }
        }
        let all_tables: Vec<&DataTable> = self
            .list_databases()
            .iter()
            .flat_map(|db| self.all_tables_of(db))
            .map(|(_, _, t)| t)
            .collect();
        let flags = self.image_flags(&all_tables);
        let mut writer = CheckpointWriter {
            file,
            cursor: old_slot.file_len,
            free: old_footer.free_ranges.clone(),
            reuse,
        };
        let mut carry: Vec<u8> = Vec::new();
        let mut repoints: Vec<(&DataTable, TableCarry)> = Vec::new();
        let mut freed: Vec<(u64, u64)> = Vec::new();
        let mut seen_old: std::collections::HashSet<(String, String, String)> =
            std::collections::HashSet::new();
        let mut saved: Vec<SavedTableStats> = Vec::new();
        let db_dirs = self.catalog_dirs(flags, |ns, name, t| {
            let key = (
                ns.db().to_ascii_lowercase(),
                ns.schema().to_ascii_lowercase(),
                name.to_ascii_lowercase(),
            );
            let old = old_tables.get(&key).copied();
            if old.is_some() {
                seen_old.insert(key.clone());
            }
            let schema = t.schema();
            let mut row_groups = Vec::new();
            let mut accs: Vec<ChunkColumnStats> =
                schema.iter().map(|_| ChunkColumnStats::empty()).collect();
            let n_chunks = t.num_chunks();
            let n_groups = n_chunks.div_ceil(CHUNKS_PER_ROW_GROUP);
            let mut reused = vec![false; old.map_or(0, |o| o.row_groups.len()).max(n_groups)];
            for (g, reused_group) in reused.iter_mut().enumerate().take(n_groups) {
                let first = g * CHUNKS_PER_ROW_GROUP;
                let last = (first + CHUNKS_PER_ROW_GROUP).min(n_chunks);
                if let Some(old) = old
                    && let Some(old_group) = old.row_groups.get(g)
                    && old.schema == schema
                    && reusable_group(t, old_group, first, last)
                {
                    *reused_group = true;
                    for (ci, column) in old_group.columns.iter().enumerate() {
                        let mut acc = ChunkColumnStats::empty();
                        acc.null_count = column.null_count;
                        acc.nan_count = column.nan_count;
                        acc.min = column.min.clone();
                        acc.max = column.max.clone();
                        accs[ci].merge(&acc);
                    }
                    row_groups.push(old_group.clone());
                    continue;
                }
                let group = encode_row_group(ns, name, t, first, last, true)?;
                let (group, table_carry) = write_group(&mut writer, group, &mut carry)?;
                for (ci, acc) in group.accs.iter().enumerate() {
                    accs[ci].merge(acc);
                }
                row_groups.push(RowGroupDir {
                    rows: group.rows,
                    chunk_rows: group.chunk_rows,
                    columns: group.columns,
                });
                repoints.push((t, table_carry));
            }
            if let Some(old) = old {
                for (g, group) in old.row_groups.iter().enumerate() {
                    if reused.get(g).copied().unwrap_or(false) {
                        continue;
                    }
                    for column in &group.columns {
                        freed.push((column.offset, column.length));
                    }
                }
            }
            finish_table_dir(ns, name, t, row_groups, &accs, false, &mut saved)
        })?;
        // A table that disappeared takes all of its row groups with it.
        for db in &old_footer.databases {
            for schema in &db.schemas {
                for table in &schema.tables {
                    let key = (
                        db.name.to_ascii_lowercase(),
                        schema.name.to_ascii_lowercase(),
                        table.name.to_ascii_lowercase(),
                    );
                    if seen_old.contains(&key) {
                        continue;
                    }
                    for group in &table.row_groups {
                        for column in &group.columns {
                            freed.push((column.offset, column.length));
                        }
                    }
                }
            }
        }
        // The old footer is free once the new one replaces it, and so is every
        // row group this pass superseded. Old `free_ranges` were already
        // allocatable; these join the new footer's list.
        freed.push((old_slot.footer_offset, old_slot.footer_len));
        let freed = merge_ranges(freed);
        let mut footer_free = writer.free.clone();
        footer_free.extend_from_slice(&freed);
        footer_free = merge_ranges(footer_free);
        let max_group_end = max_group_end(&db_dirs);
        let footer = self.build_footer(flags, &footer_free, &db_dirs)?;
        let estimate = footer.len() as u64;
        // Only ranges free in the *current* footer are safe to overwrite
        // before the new slot lands; the old footer and superseded groups
        // still belong to the checkpoint the other slot names.
        let allocatable = writer.free.clone();
        let (footer_offset, mut final_free) =
            alloc_footer(&mut writer, &allocatable, estimate, max_group_end);
        final_free.extend_from_slice(&freed);
        final_free.retain(|&(offset, len)| offset.saturating_add(len) <= footer_offset);
        final_free = merge_ranges(final_free);
        let footer = self.build_footer(flags, &final_free, &db_dirs)?;
        debug_assert!(footer.len() as u64 <= estimate);
        writer.write(footer_offset, &footer)?;
        let file_len = footer_offset + footer.len() as u64;
        writer.cursor = writer.cursor.max(file_len);
        #[cfg(test)]
        if checkpoint_fault() == Some(CheckpointFault::AfterData) {
            return Err(PinkDbError::Execution(
                "injected checkpoint fault after data".to_string(),
            ));
        }
        writer.file.sync_all()?;
        #[cfg(test)]
        if checkpoint_fault() == Some(CheckpointFault::AfterFirstSync) {
            return Err(PinkDbError::Execution(
                "injected checkpoint fault after first sync".to_string(),
            ));
        }
        let marker = crate::wal::CheckpointMarker {
            iteration,
            wal_generation,
            next_generation,
        };
        crate::wal::write_checkpoint_marker(target, marker)?;
        let new_slot = Slot {
            iteration,
            footer_offset,
            footer_len: footer.len() as u64,
            footer_crc32c: crc32c(&footer),
            file_len,
        };
        let other = 1 - slot_idx;
        #[cfg(test)]
        if checkpoint_fault() == Some(CheckpointFault::TornSlot) {
            let mut buf = [0u8; HEADER_SLOT_LEN];
            buf[..SLOT_LEN].copy_from_slice(&new_slot.encode());
            pwrite_all(&writer.file, (other * HEADER_SLOT_LEN) as u64, &buf[..20])?;
            writer.file.sync_all()?;
            return Err(PinkDbError::Execution(
                "injected torn slot write".to_string(),
            ));
        }
        write_slot(&mut writer.file, other, &new_slot)?;
        writer.file.sync_all()?;
        #[cfg(test)]
        if checkpoint_fault() == Some(CheckpointFault::AfterSlot) {
            return Err(PinkDbError::Execution(
                "injected checkpoint fault after slot".to_string(),
            ));
        }
        // The new slot names `file_len`; the old one would now be invalid if
        // the file were shorter, so truncate only after the slot is durable.
        if writer.file.metadata()?.len() > file_len {
            writer.file.set_len(file_len)?;
        }
        Self::repoint_carried(carry, repoints, false);
        Ok(())
    }

    /// [`PinkDb::save_sync`] with at-rest encryption (checkpoint primitive for
    /// a keyed durable database; the WAL is only rolled once this returns).
    #[cfg(feature = "encryption")]
    pub(crate) fn save_sync_with_key(
        &self,
        path: &std::path::Path,
        key: &EncryptionKey,
    ) -> Result<()> {
        // See `save_with_key`: the whole-buffer container is sealed in memory.
        let sealed = crate::crypto::seal_container(&self.to_bytes()?, key, 0)?;
        Self::write_sync(path, |sink| sink.bytes(&sealed))
    }

    /// Stream the image into `path` and fsync it. `path` is a checkpoint's
    /// staging file: a failure removes it, so a partial image is never left
    /// where recovery could trust it.
    fn write_sync<F>(path: &std::path::Path, write: F) -> Result<()>
    where
        F: FnOnce(&mut FileSink) -> Result<()>,
    {
        // M-5: checkpoints (and the `.tmp` staging file they are written to)
        // are owner-only, never umask-derived.
        crate::fsutil::tighten(path);
        let written = (|| -> Result<()> {
            let f = crate::fsutil::create_private(path)?;
            let mut sink = FileSink::new(f);
            write(&mut sink)?;
            sink.finish()?.sync_all()?;
            Ok(())
        })();
        if written.is_err() {
            let _ = std::fs::remove_file(path);
        }
        written
    }

    /// Write the image `write` produces to `path` atomically: a unique temp
    /// sibling is written and fsynced, then renamed over the target, then the
    /// parent directory is fsynced. An interrupted write leaves the previous
    /// target intact; a failure removes the temp file.
    fn write_atomic<F>(path: &std::path::Path, write: F) -> Result<()>
    where
        F: FnOnce(&mut FileSink) -> Result<()>,
    {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(format!(".tmp-{}-{seq}", std::process::id()));
        let tmp = std::path::PathBuf::from(tmp);
        let written = (|| -> Result<()> {
            let f = crate::fsutil::create_new_private(&tmp)?;
            let mut sink = FileSink::new(f);
            write(&mut sink)?;
            sink.finish()?.sync_all()?;
            Ok(())
        })();
        if let Err(e) = written {
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
    /// Only the current format (`PINKDB07`) is readable: a file written by
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
        Self::load_image(crate::storage::StoreImage::new(data, false))
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
            let mut db = Self::load_image(crate::storage::StoreImage::new(plain, true))?;
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

    /// Load a `PINKENC1` checkpoint image from memory.
    pub fn from_pink_bytes_with_key(data: &[u8], key: &EncryptionKey) -> Result<PinkDb> {
        #[cfg(not(feature = "encryption"))]
        {
            let _ = (data, key);
            Err(crate::crypto::unavailable())
        }
        #[cfg(feature = "encryption")]
        {
            if !crate::crypto::is_container(data) {
                return Err(PinkDbError::Execution(
                    "catalog checkpoint is not encrypted (a key was supplied)".to_string(),
                ));
            }
            let plain = crate::crypto::open_container(data, key, 0)?;
            let mut db = Self::load_image(crate::storage::StoreImage::new(plain, true))?;
            db.encryption_key = Some(key.clone());
            Ok(db)
        }
    }

    /// Decode a serialized `.pink` image (`PINKDB07`): copy it into the
    /// retained store image a lazily opened store decodes from.
    fn load_bytes(data: &[u8]) -> Result<PinkDb> {
        Self::load_image(crate::storage::StoreImage::new(data.to_vec(), false))
    }

    /// The one loader: the footer, then every table's chunk handles over the
    /// retained image (see [`load_table_data`]). Nothing is decoded here.
    fn load_image(image: Arc<crate::storage::StoreImage>) -> Result<PinkDb> {
        let data = image.bytes();
        let footer = read_store_footer(data)?;
        // Start from an empty store: a file may legitimately have no `main`
        // (it was dropped) and `open` must round-trip exactly.
        let mut db = PinkDb::with_databases(Vec::<String>::new());
        for dbdir in &footer.databases {
            db.create_database(&dbdir.name)?;
            if let Some(cat) = db.databases.get_mut(&dbdir.name.to_ascii_lowercase()) {
                cat.storage = dbdir.storage.clone();
            }
            for sdir in &dbdir.schemas {
                let ns = SchemaPath::new(&dbdir.name, &sdir.name);
                db.create_schema(&ns, true)?;
                db.set_schema_display(&ns, &sdir.name)?;
                for tdir in &sdir.tables {
                    db.create_table_with_constraints_in(
                        &ns,
                        &tdir.name,
                        tdir.schema.clone(),
                        tdir.constraints.clone(),
                    )?;
                    // Defaults are stored as SQL text; rebind them against
                    // the restored schema so a clock default is ready to
                    // evaluate on later inserts.
                    let rebound: Vec<Option<crate::plan::ColumnDefault>> = tdir
                        .defaults
                        .iter()
                        .zip(tdir.schema.iter())
                        .map(|(text, (_, dtype))| match text {
                            Some(text) => Ok(Some(crate::sql::binder::bind_stored_default(
                                &db, &ns, text, *dtype,
                            )?)),
                            None => Ok(None),
                        })
                        .collect::<Result<_>>()?;
                    let t = db.table_mut_in(&ns, &tdir.name)?;
                    for (col, d) in t.columns.iter_mut().zip(rebound) {
                        col.default = d;
                    }
                }
            }
        }
        // The data region: every column chunk is verified on its own CRC32C
        // and installed as encoded handles, in parallel.
        for dbdir in &footer.databases {
            for sdir in &dbdir.schemas {
                let ns = SchemaPath::new(&dbdir.name, &sdir.name);
                for tdir in &sdir.tables {
                    load_table_data(&image, &mut db, &ns, tdir)?;
                }
            }
        }
        // The index definitions (the maps are rebuilt from the rows).
        for dbdir in &footer.databases {
            for sdir in &dbdir.schemas {
                let ns = SchemaPath::new(&dbdir.name, &sdir.name);
                for tdir in &sdir.tables {
                    if tdir.indexes.is_empty() {
                        continue;
                    }
                    let t = db.table_mut_in(&ns, &tdir.name)?;
                    apply_indexes(t, &tdir.indexes)?;
                }
            }
        }
        // Vector index definitions keep their serialized graph until the
        // provider extension is loaded (CLI/server do that right after open;
        // any mutation or indexed query before that is a clear refusal).
        for dbdir in &footer.databases {
            for sdir in &dbdir.schemas {
                let ns = SchemaPath::new(&dbdir.name, &sdir.name);
                for tdir in &sdir.tables {
                    if tdir.vector_indexes.is_empty() {
                        continue;
                    }
                    let t = db.table_mut_in(&ns, &tdir.name)?;
                    let mut defs = Vec::with_capacity(tdir.vector_indexes.len());
                    for def in &tdir.vector_indexes {
                        if def.column >= t.columns.len() {
                            return Err(PinkDbError::Execution(format!(
                                "corrupt file: vector index \"{}\" names column {} of a {}-column \
                                 table",
                                def.name,
                                def.column,
                                t.columns.len()
                            )));
                        }
                        if t.indexes.iter().any(|i| i.name == def.name)
                            || defs
                                .iter()
                                .any(|d: &crate::storage::VectorIndexDef| d.name == def.name)
                        {
                            return Err(PinkDbError::Execution(format!(
                                "corrupt file: index \"{}\" is defined twice",
                                def.name
                            )));
                        }
                        defs.push(crate::storage::VectorIndexDef {
                            name: def.name.clone(),
                            column: def.column,
                            provider: def.provider.to_ascii_lowercase(),
                            options: def.options.clone(),
                            index: None,
                            blob: Some(std::sync::Arc::from(def.blob.clone().into_boxed_slice())),
                        });
                    }
                    t.vector_indexes = defs;
                }
            }
        }
        // The catalog sections: macros first (an MV's defining SQL may call
        // a macro and is rebound at load), then the materialized views, then
        // the procedures, then the store-global RBAC section. Every object
        // lands in the schema it was saved in.
        for dbdir in &footer.databases {
            for sdir in &dbdir.schemas {
                let ns = SchemaPath::new(&dbdir.name, &sdir.name);
                for def in &sdir.macros {
                    db.macros_insert_in(&ns, def.name.to_ascii_lowercase(), def.clone())?;
                }
                for def in &sdir.materialized_views {
                    let mut def = def.clone();
                    def.query = crate::mv::rebind_query(&db, &ns, &def.query_sql).ok();
                    db.materialized_views_insert_in(&ns, def.name.to_ascii_lowercase(), def)?;
                }
                for def in &sdir.procedures {
                    db.procedures_insert_in(&ns, def.name.to_ascii_lowercase(), def.clone())?;
                }
                // Triggers last: a definition is only used when a statement
                // fires it, so its table must be loaded first (it already is).
                for def in &sdir.triggers {
                    db.triggers_insert_in(&ns, def.clone())?;
                }
            }
        }
        if let Some(rbac) = footer.rbac {
            *db.rbac_mut() = rbac;
        }
        // Wave 132b: the store's secrets must exist before the checkpoint's
        // lakes are reopened (a lake's postgres catalog password lives in
        // one), and `reopen_lake_databases` runs right after this load.
        db.install_persistent_secrets(footer.secrets);
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

/// Whether `bytes` is a `PINKDB07` image (the magic at offset 0, the start
/// of the first header slot).
pub fn is_pinkdb(bytes: &[u8]) -> bool {
    bytes.len() >= 8 && &bytes[..8] == MAGIC_V7
}

/// Where one re-encoded column chunk sits in the save's carry buffer: `(chunk
/// index, offset of the page, page length, rows)`.
type CarriedChunk = (usize, u64, u64, usize);

/// Every re-encoded column chunk's [`CarriedChunk`]s, per column.
type TableCarry = Vec<Vec<CarriedChunk>>;

/// The test-only knobs of a write: a non-empty `free_ranges` section, and a
/// second header slot with a newer iteration.
#[cfg(test)]
#[derive(Default)]
struct Hooks {
    free_ranges: Vec<(u64, u64)>,
    second_slot: bool,
}

#[cfg(not(test))]
#[derive(Default)]
struct Hooks;

/// One column chunk's bytes in a freshly encoded row group: either copied
/// from the retained image or built by the encoder.
enum GroupPieceSource {
    Borrowed {
        image: Arc<crate::storage::StoreImage>,
        start: u64,
        end: u64,
    },
    Owned(Vec<u8>),
}

struct GroupPiece {
    source: GroupPieceSource,
}

/// One row group encoded but not yet placed: its column offsets are relative
/// to the group start, so the incremental checkpoint can move the whole group
/// into a free range.
struct GroupBuild {
    rows: u64,
    chunk_rows: Vec<u32>,
    columns: Vec<ColumnChunkDir>,
    pieces: Vec<GroupPiece>,
    /// Per column: `(chunk index, offset relative to the piece, length, rows)`
    /// for every chunk the save will repoint at the carried image.
    carry: Vec<Vec<(usize, u64, u64, usize)>>,
    accs: Vec<ChunkColumnStats>,
}

/// Encode chunks `first..last` of one table into a [`GroupBuild`], copying a
/// clean column's bytes through instead of decoding it.
fn encode_row_group(
    ns: &crate::storage::SchemaPath,
    name: &str,
    t: &DataTable,
    first: usize,
    last: usize,
    parallel: bool,
) -> Result<GroupBuild> {
    let chunk_rows: Vec<u32> = (first..last).map(|i| t.chunk_rows(i) as u32).collect();
    let rows: u64 = chunk_rows.iter().map(|&r| r as u64).sum();
    let copies: Vec<Option<CopyThrough>> = t
        .columns
        .iter()
        .map(|column| copy_through(column, first, last))
        .collect();
    let work: Vec<(usize, usize)> = copies
        .iter()
        .enumerate()
        .filter(|(_, copy)| copy.is_none())
        .flat_map(|(ci, _)| (first..last).map(move |i| (ci, i)))
        .collect();
    // The string layout stores offsets as `u32`; a chunk whose total VARCHAR
    // bytes exceed that cannot be encoded faithfully (the offsets would wrap
    // and the reader would reject the file). Refuse before writing instead of
    // emitting a corrupt file.
    let encode = |ci: usize, i: usize| -> Result<EncodedPage> {
        let v = t.columns[ci].chunk(i)?;
        if let VectorData::VarChar(strs) = v.data() {
            let total: u64 = strs.iter().map(|s| s.len() as u64).sum();
            if !varchar_offsets_fit(total) {
                return Err(PinkDbError::Execution(format!(
                    "table \"{ns}.{name}\" has a VARCHAR chunk of {total} \
                     bytes, over the .pink format's u32 string-offset limit"
                )));
            }
        }
        Ok(encode_page(&v))
    };
    // Encode the pages that cannot be copied in parallel; the order-preserving
    // collect keeps the bytes identical to a serial pass.
    let encoded: Vec<EncodedPage> = if parallel && work.len() >= PARALLEL_SAVE_MIN_CHUNKS {
        work.par_iter()
            .map(|&(ci, i)| encode(ci, i))
            .collect::<Result<Vec<_>>>()?
    } else {
        work.iter()
            .map(|&(ci, i)| encode(ci, i))
            .collect::<Result<Vec<_>>>()?
    };
    let mut encoded = encoded.into_iter();
    let mut columns: Vec<ColumnChunkDir> = Vec::with_capacity(t.columns.len());
    let mut pieces = Vec::with_capacity(t.columns.len());
    let mut carry: Vec<Vec<(usize, u64, u64, usize)>> = Vec::with_capacity(t.columns.len());
    let mut accs: Vec<ChunkColumnStats> = t
        .columns
        .iter()
        .map(|_| ChunkColumnStats::empty())
        .collect();
    let mut running = 0u64;
    for (ci, copy) in copies.into_iter().enumerate() {
        let Some(copy) = copy else {
            let pages: Vec<EncodedPage> = encoded.by_ref().take(last - first).collect();
            debug_assert_eq!(pages.len(), last - first);
            // The column chunk: the page index, then the pages.
            let page_count = pages.len() as u32;
            let index_len = 4 + page_count as usize * 8;
            let mut chunk = Writer::new();
            chunk.u32(page_count);
            let mut rel = index_len as u32;
            for page in &pages {
                let len = page.frame.len() as u32;
                chunk.u32(rel);
                chunk.u32(len);
                rel += len;
            }
            for page in &pages {
                chunk.bytes(&page.frame);
            }
            let bytes = chunk.buf;
            let length = bytes.len() as u64;
            // The column chunk's exact statistics and encoding summary.
            let mut acc = ChunkColumnStats::empty();
            let mut encodings = 0u32;
            for page in &pages {
                acc.merge(&page.stats);
                encodings |= 1u32 << page.frame[0];
            }
            accs[ci].merge(&acc);
            columns.push(ColumnChunkDir {
                offset: running,
                length,
                page_count,
                encodings,
                crc32c: crc32c(&bytes),
                null_count: acc.null_count,
                nan_count: acc.nan_count,
                min: acc.min,
                max: acc.max,
            });
            running += length;
            // Keep every encoded column in the retained image, including
            // chunks built in memory: the next checkpoint can then copy this
            // row group through instead of re-encoding it. Repointing drops
            // the decoded payload, so this does not double the resident data.
            let mut carried = Vec::new();
            let mut page_offset = index_len as u64;
            for (i, page) in pages.iter().enumerate() {
                carried.push((
                    first + i,
                    page_offset,
                    page.frame.len() as u64,
                    chunk_rows[i] as usize,
                ));
                page_offset += page.frame.len() as u64;
            }
            carry.push(carried);
            pieces.push(GroupPiece {
                source: GroupPieceSource::Owned(bytes),
            });
            continue;
        };
        let bytes = &copy.image.bytes()[copy.start as usize..copy.end as usize];
        let mut acc = ChunkColumnStats::empty();
        let mut encodings = 0u32;
        for (i, &(rel, _)) in copy.pages.iter().enumerate() {
            acc.merge(t.zone(ci, first + i));
            encodings |= 1u32 << bytes[rel as usize];
        }
        accs[ci].merge(&acc);
        columns.push(ColumnChunkDir {
            offset: running,
            length: bytes.len() as u64,
            page_count: copy.pages.len() as u32,
            encodings,
            crc32c: crc32c(bytes),
            null_count: acc.null_count,
            nan_count: acc.nan_count,
            min: acc.min,
            max: acc.max,
        });
        running += bytes.len() as u64;
        // The group is copied to a new offset; its clean chunks move with it,
        // so they need the same repointing as the encoded ones.
        carry.push(
            copy.pages
                .iter()
                .enumerate()
                .map(|(i, &(rel, len))| (first + i, rel as u64, len as u64, chunk_rows[i] as usize))
                .collect(),
        );
        pieces.push(GroupPiece {
            source: GroupPieceSource::Borrowed {
                image: copy.image,
                start: copy.start,
                end: copy.end,
            },
        });
    }
    Ok(GroupBuild {
        rows,
        chunk_rows,
        columns,
        pieces,
        carry,
        accs,
    })
}

/// Write one table's data region (its column chunks) and collect its footer
/// entry.
///
/// One row group is read, encoded and written at a time: a clean group's
/// bytes are copied through, the rest are encoded in parallel, written
/// through the sink, then dropped. The sink is a [`VecSink`] for the
/// in-memory API and a [`FileSink`] for `save`/checkpoint, so both paths
/// produce the same bytes.
///
/// The second value is the new home of every chunk this pass re-encoded,
/// relative to `carry`; `write_image` repoints the handles there.
#[allow(clippy::too_many_arguments)]
fn write_table<S: Sink>(
    data: &mut S,
    carry: &mut Vec<u8>,
    ns: &crate::storage::SchemaPath,
    name: &str,
    t: &DataTable,
    parallel: bool,
    collect_stats: bool,
    saved: &mut Vec<SavedTableStats>,
) -> Result<(TableDir, TableCarry)> {
    let schema = t.schema();
    let n_chunks = t.num_chunks();
    // The .pink format has no 128-bit integer layout yet (step 5 of
    // `docs/slt-3-hugeint-plan.md`). Refuse before encoding any page, so a
    // table with such a column cannot reach the writer at all.
    for (col, dtype) in &schema {
        if matches!(dtype, DataType::Hugeint | DataType::Uhugeint) {
            return Err(PinkDbError::Execution(format!(
                "{} is not yet supported in .pink storage (column \"{col}\")",
                dtype.name()
            )));
        }
    }
    // The per-column statistics of the whole table (the lake's stats
    // block), accumulated as each row group is encoded.
    let mut accs: Vec<ChunkColumnStats> =
        schema.iter().map(|_| ChunkColumnStats::empty()).collect();
    // The row groups: 60 engine chunks each.
    let n_groups = n_chunks.div_ceil(CHUNKS_PER_ROW_GROUP);
    let mut row_groups: Vec<RowGroupDir> = Vec::with_capacity(n_groups);
    let mut table_carry: TableCarry = vec![Vec::new(); schema.len()];
    for g in 0..n_groups {
        let first = g * CHUNKS_PER_ROW_GROUP;
        let last = (first + CHUNKS_PER_ROW_GROUP).min(n_chunks);
        let group = encode_row_group(ns, name, t, first, last, parallel)?;
        let GroupBuild {
            rows,
            chunk_rows,
            columns: relative,
            pieces,
            carry: group_carry,
            accs: group_accs,
        } = group;
        let mut columns = Vec::with_capacity(relative.len());
        for (ci, piece) in pieces.into_iter().enumerate() {
            let offset = data.offset();
            match &piece.source {
                GroupPieceSource::Borrowed { image, start, end } => {
                    let bytes = &image.bytes()[*start as usize..*end as usize];
                    data.bytes(bytes)?;
                    let base = carry.len() as u64;
                    carry.extend_from_slice(bytes);
                    table_carry[ci].extend(
                        group_carry[ci]
                            .iter()
                            .map(|&(i, rel, len, rows)| (i, base + rel, len, rows)),
                    );
                }
                GroupPieceSource::Owned(bytes) => {
                    data.bytes(bytes)?;
                    let base = carry.len() as u64;
                    carry.extend_from_slice(bytes);
                    table_carry[ci].extend(
                        group_carry[ci]
                            .iter()
                            .map(|&(i, rel, len, rows)| (i, base + rel, len, rows)),
                    );
                }
            }
            columns.push(ColumnChunkDir {
                offset,
                ..relative[ci].clone()
            });
        }
        for (ci, acc) in group_accs.iter().enumerate() {
            accs[ci].merge(acc);
        }
        row_groups.push(RowGroupDir {
            rows,
            chunk_rows,
            columns,
        });
    }
    Ok((
        finish_table_dir(ns, name, t, row_groups, &accs, collect_stats, saved)?,
        table_carry,
    ))
}

/// The footer table directory for an already-built row-group list.
fn finish_table_dir(
    ns: &SchemaPath,
    name: &str,
    t: &DataTable,
    row_groups: Vec<RowGroupDir>,
    accs: &[ChunkColumnStats],
    collect_stats: bool,
    saved: &mut Vec<SavedTableStats>,
) -> Result<TableDir> {
    let schema = t.schema();
    let null_counts: Vec<u64> = accs.iter().map(|acc| acc.null_count).collect();
    if collect_stats {
        let mut entry = SavedTableStats {
            db: ns.db().to_string(),
            schema: ns.schema().to_string(),
            table: name.to_string(),
            rows: t.num_rows() as u64,
            columns: schema
                .iter()
                .zip(accs)
                .map(|((col, dtype), acc)| SavedColumnStats {
                    name: col.clone(),
                    dtype: *dtype,
                    null_count: acc.null_count,
                    min: acc.min.clone(),
                    max: acc.max.clone(),
                    nan_count: acc.nan_count,
                    paths: Vec::new(),
                })
                .collect(),
        };
        variant_path_stats(t, &mut entry);
        saved.push(entry);
    }
    Ok(TableDir {
        name: name.to_string(),
        schema,
        constraints: t.constraints.clone(),
        defaults: t
            .columns
            .iter()
            .map(|c| c.default.as_ref().map(|d| d.text.clone()))
            .collect(),
        indexes: index_defs(t, &null_counts),
        vector_indexes: vector_index_defs(t)?,
        row_count: t.num_rows() as u64,
        row_groups,
        stats: t.stats().clone(),
        zones: t.zones().to_vec(),
    })
}

/// What a checkpoint wrote: a full image in the staging file, or an in-place
/// commit through the other header slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckpointSave {
    Full,
    Incremental,
}

/// A full rewrite takes over once the current footer's holes are this much of
/// the file; an incremental checkpoint would otherwise leave most of it
/// sparse. Fifty percent is the default and is deliberately not a setting:
/// a smaller file is the only reason to rewrite.
const COMPACT_FREE_PERCENT: u64 = 50;

/// The fault-injection points the checkpoint crash tests stop at.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckpointFault {
    AfterData,
    AfterFirstSync,
    AfterSlot,
    TornSlot,
}

#[cfg(test)]
thread_local! {
    static CHECKPOINT_FAULT: std::cell::Cell<Option<CheckpointFault>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(crate) fn set_checkpoint_fault(fault: Option<CheckpointFault>) {
    CHECKPOINT_FAULT.with(|f| f.set(fault));
}

#[cfg(test)]
fn checkpoint_fault() -> Option<CheckpointFault> {
    CHECKPOINT_FAULT.with(|f| f.get())
}

#[cfg(test)]
thread_local! {
    static LAST_CHECKPOINT_REUSED: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

/// Whether the last plaintext checkpoint on this thread reused freed ranges
/// (`true`) or appended only (`false`); `None` when it was a full rewrite, or
/// before the first one.
#[cfg(test)]
pub(crate) fn last_checkpoint_reused_holes() -> Option<bool> {
    LAST_CHECKPOINT_REUSED.with(|last| last.get())
}

/// A file opened for an in-place checkpoint: writes go at explicit offsets,
/// and `free` is the old footer's hole list, the only space an encoded row
/// group may take without growing the file. `reuse == false` (another handle
/// has the store open somewhere) keeps every write past the end instead: a
/// hole might still be a live reader's bytes.
struct CheckpointWriter {
    file: std::fs::File,
    cursor: u64,
    free: Vec<(u64, u64)>,
    reuse: bool,
}

impl CheckpointWriter {
    /// First-fit allocation from the old holes; `cursor` is the end of the
    /// old logical file, so an append never reuses a superseded range in the
    /// same pass (those become free for the next checkpoint).
    fn alloc(&mut self, len: u64) -> u64 {
        if self.reuse
            && let Some(at) = first_fit(&mut self.free, len, 0)
        {
            return at;
        }
        let at = self.cursor;
        self.cursor += len;
        at
    }

    fn write(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        pwrite_all(&self.file, offset, bytes)?;
        Ok(())
    }
}

/// Write an encoded row group at its allocated offset and remember where its
/// image-backed chunks landed, so their handles can be repointed on success.
fn write_group(
    writer: &mut CheckpointWriter,
    group: GroupBuild,
    carry: &mut Vec<u8>,
) -> Result<(GroupBuild, TableCarry)> {
    let GroupBuild {
        rows,
        chunk_rows,
        columns: relative,
        pieces,
        carry: specs,
        accs,
    } = group;
    let len = relative
        .last()
        .map_or(0, |column| column.offset + column.length);
    let base = writer.alloc(len);
    let mut columns = Vec::with_capacity(relative.len());
    let mut table_carry = Vec::with_capacity(relative.len());
    for (ci, piece) in pieces.into_iter().enumerate() {
        let offset = base + relative[ci].offset;
        match &piece.source {
            GroupPieceSource::Borrowed { image, start, end } => {
                let bytes = &image.bytes()[*start as usize..*end as usize];
                writer.write(offset, bytes)?;
                let base = carry.len() as u64;
                carry.extend_from_slice(bytes);
                table_carry.push(
                    specs[ci]
                        .iter()
                        .map(|&(i, rel, len, rows)| (i, base + rel, len, rows))
                        .collect(),
                );
            }
            GroupPieceSource::Owned(bytes) => {
                writer.write(offset, bytes)?;
                let base = carry.len() as u64;
                carry.extend_from_slice(bytes);
                table_carry.push(
                    specs[ci]
                        .iter()
                        .map(|&(i, rel, len, rows)| (i, base + rel, len, rows))
                        .collect(),
                );
            }
        }
        columns.push(ColumnChunkDir {
            offset,
            ..relative[ci].clone()
        });
    }
    Ok((
        GroupBuild {
            rows,
            chunk_rows,
            columns,
            pieces: Vec::new(),
            carry: Vec::new(),
            accs,
        },
        table_carry,
    ))
}

/// A row group can stay where it is when the table's schema is unchanged, its
/// chunk boundaries still match, and every column's stored bytes are exactly
/// what the old footer says this group occupies.
fn reusable_group(t: &DataTable, old: &RowGroupDir, first: usize, last: usize) -> bool {
    let expected = last - first;
    if old.chunk_rows.len() != expected || old.columns.len() != t.columns.len() {
        return false;
    }
    let mut rows = 0u64;
    for (i, &chunk_rows) in old.chunk_rows.iter().enumerate() {
        if chunk_rows as usize != t.chunk_rows(first + i) {
            return false;
        }
        rows += chunk_rows as u64;
    }
    if rows != old.rows {
        return false;
    }
    for (ci, column) in t.columns.iter().enumerate() {
        let Some(copy) = copy_through(column, first, last) else {
            return false;
        };
        let dir = &old.columns[ci];
        if copy.start != dir.offset
            || copy.end != dir.offset.saturating_add(dir.length)
            || copy.pages.len() != dir.page_count as usize
        {
            return false;
        }
    }
    true
}

fn first_fit(free: &mut Vec<(u64, u64)>, len: u64, min_offset: u64) -> Option<u64> {
    let mut i = 0;
    while i < free.len() {
        let (offset, available) = free[i];
        if offset >= min_offset && available >= len {
            if available == len {
                free.remove(i);
            } else {
                free[i] = (offset + len, available - len);
            }
            return Some(offset);
        }
        i += 1;
    }
    None
}

fn alloc_footer(
    writer: &mut CheckpointWriter,
    free: &[(u64, u64)],
    len: u64,
    min_offset: u64,
) -> (u64, Vec<(u64, u64)>) {
    let free: &[(u64, u64)] = if writer.reuse { free } else { &[] };
    for (i, &(offset, available)) in free.iter().enumerate() {
        if offset >= min_offset && available >= len {
            // Everything at or after the footer is outside the data region;
            // only the holes before it can stay in the new free list.
            return (offset, free[..i].to_vec());
        }
    }
    let offset = writer.cursor.max(min_offset);
    writer.cursor = offset + len;
    (offset, free.to_vec())
}

fn merge_ranges(mut ranges: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    ranges.retain(|&(_, len)| len > 0);
    ranges.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for (offset, len) in ranges {
        if let Some(last) = merged.last_mut() {
            let end = last.0 + last.1;
            if offset <= end {
                last.1 = last.1.max(offset + len - last.0);
                continue;
            }
        }
        merged.push((offset, len));
    }
    merged
}

/// The end of the last row group, which the footer must not precede.
fn max_group_end(db_dirs: &[DbDir]) -> u64 {
    db_dirs
        .iter()
        .flat_map(|db| &db.schemas)
        .flat_map(|schema| &schema.tables)
        .flat_map(|table| &table.row_groups)
        .flat_map(|group| &group.columns)
        .map(|column| column.offset + column.length)
        .max()
        .unwrap_or(DATA_START)
}

/// Read the header and the footer a checkpoint is about to replace. `None`
/// means the target is not a valid v7 file (including an empty/missing one),
/// which makes the caller take the full-rewrite path.
fn read_checkpoint_file(
    file: &std::fs::File,
    physical_len: u64,
) -> Result<Option<(usize, Slot, StoreFooter)>> {
    if physical_len < (2 * HEADER_SLOT_LEN) as u64 {
        return Ok(None);
    }
    let mut header = [0u8; 2 * HEADER_SLOT_LEN];
    pread_exact(file, 0, &mut header)?;
    let Some((idx, slot)) = valid_slot(&header, physical_len) else {
        return Ok(None);
    };
    let len = usize::try_from(slot.footer_len)
        .map_err(|_| PinkDbError::Execution("corrupt file: footer too large".to_string()))?;
    let mut footer = vec![0u8; len];
    pread_exact(file, slot.footer_offset, &mut footer)?;
    if crc32c(&footer) != slot.footer_crc32c {
        return Err(PinkDbError::Execution(
            "corrupt file: footer checksum mismatch".to_string(),
        ));
    }
    let footer = parse_store_footer(&footer, slot.footer_offset)?;
    Ok(Some((idx, slot, footer)))
}

fn pread_exact(file: &std::fs::File, mut offset: u64, mut buf: &mut [u8]) -> Result<()> {
    while !buf.is_empty() {
        #[cfg(unix)]
        let read = std::os::unix::fs::FileExt::read_at(file, buf, offset);
        #[cfg(windows)]
        let read = std::os::windows::fs::FileExt::seek_read(file, buf, offset);
        #[cfg(not(any(unix, windows)))]
        let read: std::io::Result<usize> = Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "positioned reads are not supported on this platform",
        ));
        let read = read?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "checkpoint target ended before its footer",
            )
            .into());
        }
        offset += read as u64;
        buf = &mut buf[read..];
    }
    Ok(())
}

fn pwrite_all(file: &std::fs::File, mut offset: u64, mut buf: &[u8]) -> Result<()> {
    while !buf.is_empty() {
        #[cfg(unix)]
        let written = std::os::unix::fs::FileExt::write_at(file, buf, offset);
        #[cfg(windows)]
        let written = std::os::windows::fs::FileExt::seek_write(file, buf, offset);
        #[cfg(not(any(unix, windows)))]
        let written: std::io::Result<usize> = Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "positioned writes are not supported on this platform",
        ));
        let written = written?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "checkpoint target accepted no bytes",
            )
            .into());
        }
        offset += written as u64;
        buf = &buf[written..];
    }
    Ok(())
}

/// A row group's column chunk as it sits in the retained store image: the
/// bytes' range there and each page's `(rel, len)` index entry.
struct CopyThrough {
    image: Arc<crate::storage::StoreImage>,
    start: u64,
    end: u64,
    pages: Vec<(u32, u32)>,
}

/// The copy-through form of chunks `first..last` of `column`, when every one
/// of them is clean, they still share one image, and the stored page index
/// points at the pages the footer describes. `None` falls back to decoding and
/// encoding the group.
fn copy_through(column: &crate::storage::Column, first: usize, last: usize) -> Option<CopyThrough> {
    let (image, chunks) = column.stored_group(first, last)?;
    // The page index sits immediately before the first page, as the encoder
    // wrote it: a 4-byte page count, then `(rel, len)` per page.
    let index_len = 4 + chunks.len() * 8;
    let start = chunks.first()?.offset.checked_sub(index_len as u64)?;
    let last_chunk = chunks.last()?;
    let end = last_chunk.offset.checked_add(last_chunk.length)?;
    let bytes = image.bytes().get(start as usize..end as usize)?;
    let mut r = Reader::new(bytes);
    if usize::try_from(r.u32().ok()?).ok()? != chunks.len() {
        return None;
    }
    let mut pages = Vec::with_capacity(chunks.len());
    let mut rel = index_len as u64;
    for chunk in &chunks {
        let at = u64::from(r.u32().ok()?);
        let len = u64::from(r.u32().ok()?);
        if at != rel || len != chunk.length {
            return None;
        }
        pages.push((u32::try_from(at).ok()?, u32::try_from(len).ok()?));
        rel += len;
    }
    // The pages must fill the chunk exactly; anything else is not a chunk this
    // save can copy.
    if rel != end - start {
        return None;
    }
    Some(CopyThrough {
        image,
        start,
        end,
        pages,
    })
}

/// The footer's byte range in a `PINKDB07` image: `(offset, length)`.
///
/// A ranged reader needs the first `2 * HEADER_SLOT_LEN` bytes and then
/// exactly this window: two small requests, no read of the data region.
pub fn footer_span(bytes: &[u8]) -> Result<(u64, u64)> {
    let (offset, len, _) = footer_location(bytes, bytes.len() as u64)?;
    Ok((offset, len))
}

/// The footer's flags word: which optional catalog sections the image
/// carries (constraints, column defaults, materialized views, index
/// definitions, procedures, macros and RBAC).
pub fn footer_flags(bytes: &[u8]) -> Result<u32> {
    let (offset, len, crc) = footer_location(bytes, bytes.len() as u64)?;
    let start = offset as usize;
    let end = start
        .checked_add(len as usize)
        .filter(|&end| end <= bytes.len())
        .ok_or_else(|| corrupt_file("the footer is out of range"))?;
    let footer = &bytes[start..end];
    if crc32c(footer) != crc {
        return Err(corrupt_file(
            "the footer checksum does not match (the file is damaged)",
        ));
    }
    let mut r = Reader::new(footer);
    let version = r.u32()?;
    check_footer_version(version)?;
    r.u32()
}

/// Refuse a footer this build does not write: an older one (no legacy
/// readers) or a newer one.
fn check_footer_version(version: u32) -> Result<()> {
    if version < FOOTER_VERSION {
        return Err(PinkDbError::Execution(format!(
            "this .pink file was written by an older pinkdb (footer version {version}); \
             this build reads footer version {FOOTER_VERSION} only"
        )));
    }
    if version != FOOTER_VERSION {
        return Err(PinkDbError::Execution(format!(
            "unsupported .pink footer version {version}; upgrade pinkdb"
        )));
    }
    Ok(())
}

/// A database's storage, after its name in the footer:
/// `u8 STORAGE_LOCAL` or `u8 STORAGE_LAKE | u8 has_catalog [str catalog] |
/// u8 has_data [str data] | u8 read_only | u8 has_version [u64 version] |
/// has_data_inlining_row_limit [u64] | create_if_not_exists |
/// has_data_file_format [str]`.
fn write_storage(fw: &mut Writer, storage: &crate::lake_db::DbStorage) {
    match storage {
        crate::lake_db::DbStorage::Local => fw.u8(STORAGE_LOCAL),
        crate::lake_db::DbStorage::Lake(spec) => {
            fw.u8(STORAGE_LAKE);
            for text in [&spec.catalog, &spec.data] {
                match text {
                    Some(t) => {
                        fw.u8(1);
                        fw.str(t);
                    }
                    None => fw.u8(0),
                }
            }
            fw.u8(u8::from(spec.read_only));
            match spec.version {
                Some(v) => {
                    fw.u8(1);
                    fw.u64(v);
                }
                None => fw.u8(0),
            }
            match spec.data_inlining_row_limit {
                Some(n) => {
                    fw.u8(1);
                    fw.u64(n);
                }
                None => fw.u8(0),
            }
            fw.u8(u8::from(spec.create_if_not_exists));
            match spec.data_file_format.as_deref() {
                Some(id) => {
                    fw.u8(1);
                    fw.str(id);
                }
                None => fw.u8(0),
            }
        }
    }
}

fn read_storage(r: &mut Reader<'_>) -> Result<crate::lake_db::DbStorage> {
    let tag = r.u8()?;
    match tag {
        STORAGE_LOCAL => Ok(crate::lake_db::DbStorage::Local),
        STORAGE_LAKE => {
            let mut text = || -> Result<Option<String>> {
                match r.u8()? {
                    0 => Ok(None),
                    1 => Ok(Some(r.str()?)),
                    t => Err(PinkDbError::Execution(format!(
                        "corrupt file: bad lake location flag {t}"
                    ))),
                }
            };
            let catalog = text()?;
            let data = text()?;
            let read_only = match r.u8()? {
                0 => false,
                1 => true,
                t => {
                    return Err(PinkDbError::Execution(format!(
                        "corrupt file: bad read-only flag {t}"
                    )));
                }
            };
            let version = match r.u8()? {
                0 => None,
                1 => Some(r.u64()?),
                t => {
                    return Err(PinkDbError::Execution(format!(
                        "corrupt file: bad lake version flag {t}"
                    )));
                }
            };
            let data_inlining_row_limit = match r.u8()? {
                0 => None,
                1 => Some(r.u64()?),
                t => {
                    return Err(PinkDbError::Execution(format!(
                        "corrupt file: bad data-inlining row limit flag {t}"
                    )));
                }
            };
            let create_if_not_exists = match r.u8()? {
                0 => false,
                1 => true,
                t => {
                    return Err(PinkDbError::Execution(format!(
                        "corrupt file: bad create-if-not-exists flag {t}"
                    )));
                }
            };
            let data_file_format = match r.u8()? {
                0 => None,
                1 => Some(r.str()?),
                t => {
                    return Err(PinkDbError::Execution(format!(
                        "corrupt file: bad data-file format flag {t}"
                    )));
                }
            };
            Ok(crate::lake_db::DbStorage::Lake(crate::lake_db::LakeSpec {
                catalog,
                data,
                read_only,
                version,
                data_inlining_row_limit,
                create_if_not_exists,
                data_file_format,
            }))
        }
        t => Err(PinkDbError::Execution(format!(
            "corrupt file: unknown database storage tag {t}"
        ))),
    }
}

/// The refusal for a file written by an older pinkdb.
fn older_pinkdb_error(version: u8) -> PinkDbError {
    PinkDbError::Execution(format!(
        "this .pink file was written by an older pinkdb (PINKDB0{version}); \
         this build reads PINKDB07 only"
    ))
}

/// The refusal for a file written by a newer pinkdb.
fn newer_pinkdb_error(version: u8) -> PinkDbError {
    PinkDbError::Execution(format!(
        "written by a newer pinkdb (format v{version}); upgrade pinkdb"
    ))
}

/// A corrupt-file refusal with the format's fixed prefix.
fn corrupt_file(why: &str) -> PinkDbError {
    PinkDbError::Execution(format!("corrupt file: {why}"))
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

/// One column chunk of a `PINKDB07` file, as a reader outside the crate
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

/// One row group of a `PINKDB07` file: its row count and one entry per
/// column, in schema order.
#[derive(Debug, Clone)]
pub struct PinkRowGroup {
    /// The row group's exact row count.
    pub rows: u64,
    /// One entry per column, in schema order.
    pub columns: Vec<PinkColumnChunk>,
}

/// One table of a `PINKDB07` file: its identity, its physical schema and
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

/// The parsed layout of a `PINKDB07` file: every table it holds, with its
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

/// One fetched byte window of a `PINKDB07` file: the bytes at `offset`.
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

/// The footer's location and checksum, read from a `PINKDB07` file's header
/// slots.
///
/// `bytes` is the file's first `2 * HEADER_SLOT_LEN` bytes (`8,192`); a ranged
/// reader fetches exactly that in one request. `file_len` is the file's total
/// length: a slot that records a longer file is the torn half of a write and
/// is ignored. Returns `(offset, length, crc32c)` of the footer the newer
/// valid slot names. A reader that already knows where the footer is (the
/// lake's catalog records it) needs no header read at all — this function is
/// the fallback for a reader that does not.
pub fn footer_location(bytes: &[u8], file_len: u64) -> Result<(u64, u64, u32)> {
    // The whole file is at hand when the caller passes an image: a v6 file's
    // trailer magic is the one older layout whose magic is not at offset 0.
    if bytes.len() > 2 * HEADER_SLOT_LEN && bytes.ends_with(b"PINKDB06") {
        return Err(older_pinkdb_error(6));
    }
    let header = &bytes[..bytes.len().min(2 * HEADER_SLOT_LEN)];
    let Some((_, slot)) = valid_slot(header, file_len) else {
        return Err(match header.first_chunk::<8>() {
            Some(magic) if &magic[..7] == b"PINKDB0" && magic[7].is_ascii_digit() => {
                if magic[7] < b'7' {
                    older_pinkdb_error(magic[7] - b'0')
                } else if magic[7] > b'7' {
                    newer_pinkdb_error(magic[7] - b'0')
                } else {
                    // The right magic with no valid slot: the header itself
                    // is damaged.
                    corrupt_file("no header slot is valid (the file is damaged)")
                }
            }
            _ => PinkDbError::Execution("not a .pink file (bad magic)".to_string()),
        });
    };
    Ok((slot.footer_offset, slot.footer_len, slot.footer_crc32c))
}

/// Re-seal the header slot that names the footer after the footer's bytes were
/// edited in place, so a test can forge a footer (an old version, an unknown
/// flag bit) without re-encoding the whole store.
///
/// Test only: the writer never edits a footer it already sealed.
#[doc(hidden)]
pub fn reseal_footer(bytes: &mut [u8]) -> Result<()> {
    let (offset, len, _) = footer_location(bytes, bytes.len() as u64)?;
    let crc = crc32c(&bytes[offset as usize..(offset + len) as usize]);
    let Some((idx, slot)) = read_slots(&bytes[..bytes.len().min(2 * HEADER_SLOT_LEN)]) else {
        return Err(corrupt_file(
            "no header slot is valid (the file is damaged)",
        ));
    };
    let mut sealed = [0u8; HEADER_SLOT_LEN];
    sealed[..SLOT_LEN].copy_from_slice(
        &Slot {
            footer_crc32c: crc,
            ..slot
        }
        .encode(),
    );
    let start = idx * HEADER_SLOT_LEN;
    bytes[start..start + HEADER_SLOT_LEN].copy_from_slice(&sealed);
    Ok(())
}

/// Parse the footer region of a `PINKDB07` file into its layout.
///
/// `region` is the footer's bytes (the `length` bytes the header slot points
/// at) and `footer_offset` is where they sit in the file (the data region
/// ends there). `crc32c` is the checksum the header slot records for them;
/// pass `None` to skip the check (a reader that fetched the footer region but
/// not the header — the lake's catalog-recorded location). A mismatch is
/// refused before a single byte of the footer is decoded.
pub fn read_layout(region: &[u8], crc32c: Option<u32>, footer_offset: u64) -> Result<PinkLayout> {
    if let Some(want) = crc32c
        && crate::persist::crc32c(region) != want
    {
        return Err(corrupt_file(
            "the footer checksum does not match (the file is damaged)",
        ));
    }
    let footer = parse_store_footer(region, footer_offset)?;
    Ok(PinkLayout {
        tables: footer
            .databases
            .iter()
            .flat_map(|db| {
                db.schemas
                    .iter()
                    .flat_map(|s| s.tables.iter())
                    .map(move |t| PinkTableLayout {
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
        out.push(crate::storage::Column::from_chunks(
            name.clone(),
            *dtype,
            None,
            chunks,
        )?);
    }
    DataTable::from_columns(table.table.clone(), out, total_rows)
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
        DataType::Varchar => VectorData::VarChar(StrVec::repeat("", rows)),
        DataType::Date => VectorData::Date32(vec![0; rows]),
        DataType::Timestamp => VectorData::TimestampMicros(vec![0; rows]),
        DataType::Decimal { precision, scale } => VectorData::Decimal {
            values: vec![0; rows],
            precision,
            scale,
        },
        DataType::Uuid => VectorData::Uuid(vec![0; rows]),
        DataType::Hugeint => VectorData::Int128(vec![0; rows]),
        DataType::Uhugeint => VectorData::UInt128(vec![0; rows]),
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

/// Read (and verify) the footer of a `PINKDB07` image.
#[cfg(test)]
pub(crate) fn footer_free_ranges_for_test(path: &std::path::Path) -> Result<Vec<(u64, u64)>> {
    let data = std::fs::read(path)?;
    Ok(read_store_footer(&data)?.free_ranges)
}

fn read_store_footer(data: &[u8]) -> Result<StoreFooter> {
    let (offset, len, crc) = footer_location(data, data.len() as u64)?;
    let start = offset as usize;
    let end = start
        .checked_add(len as usize)
        .filter(|&end| end <= data.len())
        .ok_or_else(|| corrupt_file("the footer is out of range"))?;
    let footer = &data[start..end];
    if crc32c(footer) != crc {
        return Err(corrupt_file(
            "the footer checksum does not match (the file is damaged)",
        ));
    }
    parse_store_footer(footer, offset)
}

/// Parse a `PINKDB07` footer region (the bytes its header slot points at)
/// into the store's metadata and row-group directory. The region's checksum is
/// **not** checked here — the caller checks it (see [`read_store_footer`]
/// and [`read_layout`]). `data_end` is the footer's absolute offset: the data
/// region ends there, which is what range-checks `free_ranges`.
fn parse_store_footer(footer: &[u8], data_end: u64) -> Result<StoreFooter> {
    let mut r = Reader::new(footer);
    let version = r.u32()?;
    check_footer_version(version)?;
    let flags = r.u32()?;
    if flags
        & !(FLAG_CONSTRAINTS
            | FLAG_DEFAULTS
            | FLAG_MATERIALIZED_VIEWS
            | FLAG_INDEXES
            | FLAG_VECTOR_INDEXES
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
    let vectors = flags & FLAG_VECTOR_INDEXES != 0;
    let macros = flags & FLAG_MACROS != 0;
    let mvs = flags & FLAG_MATERIALIZED_VIEWS != 0;
    let procedures = flags & FLAG_PROCEDURES != 0;
    let triggers = flags & FLAG_TRIGGERS != 0;
    let rbac = flags & FLAG_RBAC != 0;

    let n_free = r.u32()? as usize;
    // Each range costs 16 bytes.
    if n_free > r.remaining() / 16 {
        return r.err("free-range count exceeds the remaining file");
    }
    let mut free_ranges = Vec::with_capacity(n_free);
    for _ in 0..n_free {
        let offset = r.u64()?;
        let len = r.u64()?;
        free_ranges.push((offset, len));
    }

    let n_dbs = r.u32()? as usize;
    // Each database costs at least 8 bytes (its name's length prefix).
    if n_dbs > r.remaining() / 8 + 1 {
        return r.err("database count exceeds the remaining file");
    }
    let mut databases = Vec::with_capacity(n_dbs);
    for _ in 0..n_dbs {
        let name = r.str()?;
        let storage = read_storage(&mut r)?;
        let lake = matches!(storage, crate::lake_db::DbStorage::Lake(_));
        let n_schemas = r.u32()? as usize;
        // Each schema costs at least 8 bytes (its name's length prefix).
        if n_schemas > r.remaining() / 8 + 1 {
            return r.err("schema count exceeds the remaining file");
        }
        let mut schemas = Vec::with_capacity(n_schemas);
        // The writer emits schema keys strictly ascending; a repeated key
        // would merge two schemas' objects on load instead of failing.
        let mut last_schema: Option<String> = None;
        for _ in 0..n_schemas {
            let sname = r.str()?;
            let key = sname.to_ascii_lowercase();
            if last_schema
                .as_deref()
                .is_some_and(|prev| prev >= key.as_str())
            {
                return r.err("schema names are not in strictly ascending order");
            }
            last_schema = Some(key);
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
                    read_defaults(&mut r, n_cols)?
                } else {
                    vec![None; n_cols]
                };
                let tindexes = if indexes {
                    read_index_defs(&mut r)?
                } else {
                    Vec::new()
                };
                let tvectors = if vectors {
                    read_vector_index_defs(&mut r)?
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
                    let chunk_count = r.u32()? as usize;
                    if chunk_count > r.remaining() / 4 {
                        return r.err("the row group's chunk count exceeds the remaining file");
                    }
                    let mut chunk_rows = Vec::with_capacity(chunk_count);
                    for _ in 0..chunk_count {
                        chunk_rows.push(r.u32()?);
                    }
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
                    if columns
                        .iter()
                        .any(|c| c.page_count as usize != chunk_rows.len())
                        || chunk_rows.iter().map(|&n| n as u64).sum::<u64>() != rows
                    {
                        return Err(PinkDbError::Execution(
                            "corrupt file: a row group's chunk row counts disagree with its pages"
                                .to_string(),
                        ));
                    }
                    row_groups.push(RowGroupDir {
                        rows,
                        chunk_rows,
                        columns,
                    });
                }
                let (tstats, tzones) = read_table_stats(&mut r, n_cols, row_count)?;
                tables.push(TableDir {
                    name: tname,
                    schema,
                    constraints: tconstraints,
                    defaults: tdefaults,
                    indexes: tindexes,
                    vector_indexes: tvectors,
                    row_count,
                    row_groups,
                    stats: tstats,
                    zones: tzones,
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
                read_trigger_defs(&mut r, &name, &sname)?
            } else {
                Vec::new()
            };
            schemas.push(SchemaDir {
                name: sname,
                tables,
                macros: dmacros,
                materialized_views: dmvs,
                procedures: dprocs,
                triggers: dtriggers,
            });
        }
        // A local database always has its `main` schema, and a schema name
        // never repeats (the catalog keys on the lowercased name).
        if !lake
            && !schemas
                .iter()
                .any(|sd| sd.name.eq_ignore_ascii_case("main"))
        {
            return Err(PinkDbError::Execution(format!(
                "corrupt file: database \"{name}\" has no main schema"
            )));
        }
        for (i, sd) in schemas.iter().enumerate() {
            if schemas[..i]
                .iter()
                .any(|o| o.name.eq_ignore_ascii_case(&sd.name))
            {
                return Err(PinkDbError::Execution(format!(
                    "corrupt file: database \"{name}\" defines schema \"{}\" twice",
                    sd.name
                )));
            }
        }
        databases.push(DbDir {
            name,
            storage,
            schemas,
        });
    }
    let rbac = if rbac { Some(read_rbac(&mut r)?) } else { None };
    let secrets = read_secrets(&mut r)?;
    if !r.end() {
        return Err(PinkDbError::Execution(
            "corrupt file: trailing bytes after the footer".to_string(),
        ));
    }
    check_free_ranges(&free_ranges, &databases, data_end)?;
    Ok(StoreFooter {
        databases,
        rbac,
        secrets,
        free_ranges,
    })
}

/// Refuse a `free_ranges` list a reader cannot trust.
///
/// The list is only a hint for an incremental checkpoint, so the full rewrite
/// writes it empty and a reader ignores it — but a corrupted one is a damaged
/// footer, not something to guess at: it must be sorted, disjoint, inside the
/// data region and clear of every row group.
fn check_free_ranges(ranges: &[(u64, u64)], databases: &[DbDir], data_end: u64) -> Result<()> {
    for (i, &(offset, len)) in ranges.iter().enumerate() {
        if i > 0 && ranges[i - 1].0 >= offset {
            return Err(corrupt_file("the free ranges are not sorted"));
        }
        let Some(end) = offset.checked_add(len) else {
            return Err(corrupt_file("a free range is outside the data region"));
        };
        if offset < DATA_START || end > data_end {
            return Err(corrupt_file("a free range is outside the data region"));
        }
        if let Some((prev_offset, prev_len)) = ranges.get(i.wrapping_sub(1))
            && prev_offset + prev_len > offset
        {
            return Err(corrupt_file("the free ranges overlap"));
        }
        for db in databases {
            for sdir in &db.schemas {
                for tdir in &sdir.tables {
                    for rg in &tdir.row_groups {
                        for c in &rg.columns {
                            if offset < c.offset + c.length && c.offset < end {
                                return Err(corrupt_file("a free range overlaps a row group"));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Load one table's data region: verify every column chunk (CRC32C and page
/// index) and install its handles while the pages stay encoded in the store
/// image. A page is decoded the first time a read touches its chunk.
///
/// The verification sweep is what keeps a corrupt file failing at open: the
/// checksum covers exactly the bytes the chunk will later decode from.
fn load_table_data(
    image: &Arc<crate::storage::StoreImage>,
    db: &mut PinkDb,
    ns: &SchemaPath,
    tdir: &TableDir,
) -> Result<()> {
    let data = image.bytes();
    let n_cols = tdir.schema.len();
    // The verified page ranges per column: `pages[column][row group][page]`,
    // absolute `(offset, length)` pairs in the image.
    let pages: Vec<Vec<Vec<(u64, u64)>>> = (0..n_cols)
        .into_par_iter()
        .map(|ci| {
            tdir.row_groups
                .par_iter()
                .map(|rg| verify_column_chunk(data, &rg.columns[ci]))
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
    // Assemble the handles chunk by chunk: every column shares the boundaries
    // the footer recorded, so nothing needs decoding here.
    let mut columns: Vec<Vec<Arc<crate::storage::ChunkHandle>>> = Vec::with_capacity(n_cols);
    for column_pages in &pages {
        let mut handles = Vec::with_capacity(tdir.row_groups.len() * CHUNKS_PER_ROW_GROUP);
        for (g, rg) in tdir.row_groups.iter().enumerate() {
            for (p, &rows) in rg.chunk_rows.iter().enumerate() {
                let (offset, length) = column_pages[g][p];
                handles.push(Arc::new(crate::storage::ChunkHandle::new_encoded(
                    Arc::clone(image),
                    offset,
                    length,
                    rows as usize,
                )));
            }
        }
        columns.push(handles);
    }
    // The footer carries the statistics the writer observed, and its new
    // per-chunk row counts are exactly the loaded boundaries, so a load
    // restores both instead of reading a single row.
    let t = db.table_mut_in(ns, &tdir.name)?;
    t.set_loaded_chunks(columns, total_rows)?;
    t.set_loaded_stats(tdir.stats.clone(), tdir.zones.clone(), true);
    Ok(())
}

/// Verify one column chunk without decoding it: its bounds, its CRC32C and its
/// page index. Returns every page's absolute byte range in the image.
fn verify_column_chunk(data: &[u8], dir: &ColumnChunkDir) -> Result<Vec<(u64, u64)>> {
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
    let bytes = &data[start..end];
    if crc32c(bytes) != dir.crc32c {
        return Err(PinkDbError::Execution(
            "corrupt file: a column chunk fails its checksum (the file is damaged)".to_string(),
        ));
    }
    // The page index, then the pages.
    let mut r = Reader::new(bytes);
    let page_count = r.u32()? as usize;
    if page_count != dir.page_count as usize {
        return Err(PinkDbError::Execution(
            "corrupt file: the column chunk's page count disagrees with the footer".to_string(),
        ));
    }
    // Each page-index entry is 8 bytes: never size an allocation from the
    // unvalidated count.
    if page_count > r.remaining() / 8 {
        return r.err("the column chunk's page count exceeds the chunk");
    }
    let mut pages = Vec::with_capacity(page_count);
    for _ in 0..page_count {
        let off = r.u32()? as usize;
        let len = r.u32()? as usize;
        let end = off.checked_add(len).ok_or_else(|| {
            PinkDbError::Execution("corrupt file: a page range overflows".to_string())
        })?;
        if end > bytes.len() {
            return Err(PinkDbError::Execution(
                "corrupt file: a page is outside the column chunk".to_string(),
            ));
        }
        pages.push((dir.offset + off as u64, len as u64));
    }
    Ok(pages)
}

/// Decode one page's bytes: its encoding tag, then its payload. The enclosing
/// chunk's CRC32C was verified when the store opened (or when the bytes were
/// read).
pub(crate) fn decode_page(bytes: &[u8], dtype: DataType) -> Result<Vector> {
    let mut pr = Reader::new(bytes);
    let tag = pr.u8()?;
    let v = decode_chunk_typed(dtype, tag, &mut pr)?;
    if !pr.end() {
        return Err(PinkDbError::Execution(
            "corrupt file: trailing bytes after a page".to_string(),
        ));
    }
    Ok(v)
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
        out.push(Arc::new(decode_page(&bytes[off..end], dtype)?));
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

/// Encode every column's DEFAULT: one presence byte per column, then the
/// expression text of the ones that have a default. A NULL default is absent.
fn write_defaults(w: &mut Writer, defaults: &[Option<String>]) {
    for d in defaults {
        match d {
            Some(text) => {
                w.u8(1);
                w.str(text);
            }
            None => w.u8(0),
        }
    }
}

/// Decode the per-column default section; the text is rebound to a typed
/// expression when the table is loaded.
fn read_defaults(r: &mut Reader, n_cols: usize) -> Result<Vec<Option<String>>> {
    let mut out = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        out.push(if r.u8()? == 0 { None } else { Some(r.str()?) });
    }
    Ok(out)
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
fn write_procedure_defs(w: &mut Writer, defs: &[crate::procedure::ProcedureDef]) -> Result<()> {
    w.u32(defs.len() as u32);
    for def in defs {
        w.str(&def.name);
        w.u8(def.language.tag());
        w.u32(def.params.len() as u32);
        for (name, dtype) in &def.params {
            w.str(name);
            w.u8(dtype_tag(*dtype)?);
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
    Ok(())
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
fn read_trigger_defs(
    r: &mut Reader,
    dbname: &str,
    schema: &str,
) -> Result<Vec<crate::trigger::TriggerDef>> {
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
        let mut def = crate::trigger::def_from_statement(
            &crate::storage::SchemaPath::new(dbname, schema),
            &stmt,
        )
        .map_err(|e| {
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

/// Encode the store-global secrets section (wave 132b). The values live here
/// and nowhere else on disk: the footer is sealed as a whole when the store
/// is keyed, and a WAL record carries the same list in its own section.
/// Statement text never does (see `rbac::wal_statement_text`).
fn write_secrets(w: &mut Writer, db: &PinkDb) {
    let secrets = db.persistent_secrets();
    w.u32(secrets.len() as u32);
    for secret in secrets {
        w.str(secret.name());
        w.u8(secret.type_().tag());
        w.u32(secret.scopes().len() as u32);
        for scope in secret.scopes() {
            w.str(scope);
        }
        w.u32(secret.fields().len() as u32);
        for (key, value) in secret.fields() {
            w.str(key);
            w.str(value);
        }
    }
}

fn read_secrets(r: &mut Reader) -> Result<Vec<crate::secret::Secret>> {
    let n = r.u32()? as usize;
    if n > r.remaining() / 8 + 1 {
        return r.err("secret count exceeds the remaining file");
    }
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let name = r.str()?;
        let Some(type_) = crate::secret::SecretType::from_tag(r.u8()?) else {
            return r.err("unknown secret type tag");
        };
        let n_scopes = r.u32()? as usize;
        if n_scopes > r.remaining() / 4 + 1 {
            return r.err("secret scope count exceeds the remaining file");
        }
        let mut scopes = Vec::with_capacity(n_scopes);
        for _ in 0..n_scopes {
            scopes.push(r.str()?);
        }
        let n_fields = r.u32()? as usize;
        if n_fields > r.remaining() / 8 + 1 {
            return r.err("secret field count exceeds the remaining file");
        }
        let mut fields = std::collections::BTreeMap::new();
        for _ in 0..n_fields {
            fields.insert(r.str()?, r.str()?);
        }
        out.push(crate::secret::Secret::from_stored(
            name, type_, fields, scopes,
        ));
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
    for ((dbname, schema, table), principals) in &rbac.table_grants {
        w.str(dbname);
        w.str(schema);
        w.str(table);
        write_principal_grants(w, principals);
    }
    w.u32(rbac.schema_grants.len() as u32);
    for ((dbname, schema), principals) in &rbac.schema_grants {
        w.str(dbname);
        w.str(schema);
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
        let schema = r.str()?;
        let table = r.str()?;
        let principals = read_principal_grants(r)?;
        rbac.table_grants.insert(
            (
                dbname.to_ascii_lowercase(),
                schema.to_ascii_lowercase(),
                table.to_ascii_lowercase(),
            ),
            principals,
        );
    }
    let n_schema_grants = r.u32()? as usize;
    if n_schema_grants > r.remaining() / 8 + 1 {
        return r.err("RBAC schema-grant count exceeds the remaining file");
    }
    for _ in 0..n_schema_grants {
        let dbname = r.str()?;
        let schema = r.str()?;
        let principals = read_principal_grants(r)?;
        rbac.schema_grants.insert(
            (dbname.to_ascii_lowercase(), schema.to_ascii_lowercase()),
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
/// Folds a table's statistics. The table is materialized first: the lake
/// folds over tables it has just read, and a table cloned out of a lazily
/// opened store still carries encoded chunks.
pub fn table_stats(
    db: &str,
    schema_name: &str,
    table: &str,
    src: &DataTable,
) -> Result<SavedTableStats> {
    src.decode_all()?;
    let schema = src.schema();
    let mut accs: Vec<ChunkColumnStats> =
        schema.iter().map(|_| ChunkColumnStats::empty()).collect();
    for (ci, column) in src.columns.iter().enumerate() {
        for chunk in column.chunks_decoded() {
            accs[ci].merge(&crate::stats::Zone::of_vector(&chunk));
        }
    }
    let mut saved = SavedTableStats {
        db: db.to_string(),
        schema: schema_name.to_string(),
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
    Ok(saved)
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
        for chunk in column.chunks_decoded() {
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
        for chunk in src.columns[index].chunks_decoded() {
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
        // `write_table` refuses 128-bit columns before any page is encoded.
        DataType::Hugeint | DataType::Uhugeint => {
            unreachable!("128-bit integers have no PLAIN layout")
        }
        DataType::Uuid => {
            let VectorData::Uuid(values) = v.data() else {
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

/// The pinned length and footer checksum of the golden corpus's `PINKDB07`
/// image (see `golden_pinkdb07_bytes_are_pinned`).
#[cfg(test)]
const GOLDEN_LEN: usize = 151_946;
#[cfg(test)]
const GOLDEN_FOOTER_CRC: u32 = 2_922_773_562;

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
            is_pinkdb(&data),
            "the image must start with the PINKDB07 magic"
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
            .map(|r| {
                (0..t.num_cols())
                    .map(|c| t.get(r, c).unwrap().unwrap())
                    .collect()
            })
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

    /// A file written by an older pinkdb (`PINKDB01`–`PINKDB06`) is refused
    /// with the documented message: this build reads `PINKDB07` only.
    #[test]
    fn older_pink_files_are_refused_with_the_documented_message() {
        for version in 1..=6u8 {
            let mut bytes = format!("PINKDB0{version}").into_bytes();
            bytes.extend_from_slice(&[0xABu8; 64]);
            let err = PinkDb::from_pink_bytes(&bytes).unwrap_err().to_string();
            assert_eq!(
                err,
                format!(
                    "this .pink file was written by an older pinkdb (PINKDB0{version}); \
                     this build reads PINKDB07 only"
                ),
                "version {version}"
            );
        }
    }

    /// Relabel a `.pink` file's footer as `version`, fixing its slot CRC.
    fn relabel_footer_version(bytes: &mut [u8], version: u32) {
        let (offset, _, _) = footer_location(bytes, bytes.len() as u64).unwrap();
        let start = offset as usize;
        bytes[start..start + 4].copy_from_slice(&version.to_le_bytes());
        resign(bytes);
    }

    /// Overwrite header slot `idx` with `slot`, zero paddings included.
    fn put_slot(image: &mut [u8], idx: usize, slot: &Slot) {
        let start = idx * HEADER_SLOT_LEN;
        image[start..start + HEADER_SLOT_LEN].fill(0);
        image[start..start + SLOT_LEN].copy_from_slice(&slot.encode());
    }

    /// Re-sync the winning slot's footer CRC after a test patched the footer
    /// bytes in place.
    fn resign(image: &mut [u8]) {
        let (idx, slot) = read_slots(&image[..2 * HEADER_SLOT_LEN]).unwrap();
        let start = slot.footer_offset as usize;
        let crc = crc32c(&image[start..start + slot.footer_len as usize]);
        put_slot(
            image,
            idx,
            &Slot {
                footer_crc32c: crc,
                ..slot
            },
        );
    }

    /// A complete v7 image around a hand-built `footer`: both slots zeroed,
    /// the footer, then slot 0 with `iteration = 1`.
    fn image_with_footer(footer: &[u8]) -> Vec<u8> {
        let mut data = VecSink::new();
        data.bytes(&[0u8; 2 * HEADER_SLOT_LEN]).unwrap();
        let footer_offset = data.offset();
        data.bytes(footer).unwrap();
        data.write_slot(
            0,
            &Slot {
                iteration: 1,
                footer_offset,
                footer_len: footer.len() as u64,
                footer_crc32c: crc32c(footer),
                file_len: data.offset(),
            },
        )
        .unwrap();
        data.buf
    }

    /// Wave 124: footer version 3 records every database's storage. A
    /// version-2 footer (no storage) is refused as an older pinkdb's; later
    /// waves bumped the footer for schemas (4), default expressions (5),
    /// secret sections and the lake data-file format (6) and per-chunk row
    /// counts (7), so it refuses 2 the same way.
    #[test]
    fn a_version_two_footer_is_refused_as_an_older_pinkdbs() {
        let mut db = PinkDb::new();
        db.create_table("t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        let mut bytes = db.to_bytes().unwrap();
        relabel_footer_version(&mut bytes, 2);
        let err = PinkDb::load_bytes(&bytes).unwrap_err().to_string();
        assert_eq!(
            err,
            format!(
                "this .pink file was written by an older pinkdb (footer version 2); this build \
                 reads footer version {FOOTER_VERSION} only"
            )
        );
    }

    /// Wave 136b-2: a version-6 footer has no per-chunk row counts, which the
    /// lazy open needs to place a chunk's rows. There is no reader for v6 and
    /// no conversion: it is refused as an older pinkdb's.
    #[test]
    fn a_version_six_footer_is_refused_as_an_older_pinkdbs() {
        let mut db = PinkDb::new();
        db.create_table("t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        let mut bytes = db.to_bytes().unwrap();
        relabel_footer_version(&mut bytes, 6);
        let err = PinkDb::load_bytes(&bytes).unwrap_err().to_string();
        assert_eq!(
            err,
            format!(
                "this .pink file was written by an older pinkdb (footer version 6); this build \
                 reads footer version {FOOTER_VERSION} only"
            )
        );
    }

    /// Wave 131a: a version-3 footer (no schema level) is refused, never read
    /// as a version-4 layout. Version 4 predates default expressions.
    #[test]
    fn a_version_three_footer_is_refused_as_an_older_pinkdbs() {
        let mut db = PinkDb::new();
        db.create_table("t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        let mut bytes = db.to_bytes().unwrap();
        relabel_footer_version(&mut bytes, 3);
        let err = PinkDb::load_bytes(&bytes).unwrap_err().to_string();
        assert_eq!(
            err,
            format!(
                "this .pink file was written by an older pinkdb (footer version 3); this build \
                 reads footer version {FOOTER_VERSION} only"
            )
        );
    }

    /// A footer that repeats a schema key (case-insensitively) would merge
    /// two schemas' objects on load, so it is refused as corrupt.
    #[test]
    fn duplicate_schema_keys_are_refused() {
        let mut fw = Writer::new();
        fw.u32(FOOTER_VERSION);
        fw.u32(0); // no optional catalog sections
        fw.u32(0); // no free ranges
        fw.u32(1); // one database
        fw.str("main");
        fw.u8(STORAGE_LOCAL);
        fw.u32(2); // two schemas
        fw.str("Foo");
        fw.u32(0); // no tables
        fw.str("foo");
        fw.u32(0);
        let footer = fw.buf;
        let bytes = image_with_footer(&footer);
        let err = PinkDb::load_bytes(&bytes).unwrap_err().to_string();
        assert!(
            err.contains("schema names are not in strictly ascending order"),
            "{err}"
        );
    }

    /// Wave 131a: the footer records every database's schemas, and a reopen
    /// finds every object in the schema it was saved in — and none in `main`.
    #[test]
    fn schemas_and_their_objects_round_trip() {
        let mut db = PinkDb::new();
        db.execute(
            "CREATE DATABASE app; USE app; \
             CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1); \
             CREATE MACRO add1(x) AS x + 1; \
             CREATE MATERIALIZED VIEW mv AS SELECT 42 AS y; \
             CREATE PROCEDURE p() AS 'BEGIN SELECT 1; END'; \
             CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW \
             EXECUTE INSERT INTO t VALUES (9);",
        )
        .unwrap();
        let ns = SchemaPath::new("app", "s");
        db.create_schema(&ns, false).unwrap();
        // The grammar that names a schema arrives in wave 131b, so the test
        // moves the objects the SQL created in `main` into `s` through the
        // store's catalog maps — the checkpoint then has to put them back.
        let cat = db.databases.get_mut("app").unwrap();
        let main = cat.schemas.get_mut("main").unwrap();
        let (table, mv, proc, trigger, macro_def) = (
            main.tables.remove("t").unwrap(),
            main.materialized_views.remove("mv").unwrap(),
            main.procedures.remove("p").unwrap(),
            main.triggers.remove(0),
            main.macros.remove("add1").unwrap(),
        );
        let s = cat.schemas.get_mut("s").unwrap();
        s.tables.insert("t".into(), table);
        s.materialized_views.insert("mv".into(), mv);
        s.procedures.insert("p".into(), proc);
        s.triggers.push(trigger);
        s.macros.insert("add1".into(), macro_def);

        let back = PinkDb::load_bytes(&db.to_bytes().unwrap()).unwrap();
        assert_eq!(
            back.list_schemas("app"),
            vec!["main".to_string(), "s".to_string()]
        );
        assert!(back.table_in(&ns, "t").is_ok());
        assert_eq!(back.table_in(&ns, "t").unwrap().num_rows(), 1);
        assert!(back.materialized_view_in(&ns, "mv").is_ok());
        // The defining query rebound at load (in the schema it was saved in).
        assert!(
            back.materialized_view_in(&ns, "mv")
                .unwrap()
                .query
                .is_some()
        );
        assert!(back.procedure_in(&ns, "p").is_ok());
        assert_eq!(back.triggers_in(&ns).len(), 1);
        assert!(back.macro_in(&ns, "add1").is_ok());
        // Nothing stayed behind in `main`.
        let main = SchemaPath::main("app");
        assert!(back.table_in(&main, "t").is_err());
        assert!(back.materialized_view_in(&main, "mv").is_err());
        assert!(back.procedure_in(&main, "p").is_err());
        assert!(back.triggers_in(&main).is_empty());
        assert!(back.macro_in(&main, "add1").is_err());
    }

    /// Wave 124: a lake database round-trips through the footer as its
    /// location only — never its tables.
    #[test]
    fn a_lake_database_is_persisted_as_its_location() {
        let mut db = PinkDb::new();
        db.create_database("lk").unwrap();
        db.create_table_in("lk", "t", vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        let spec = crate::lake_db::LakeSpec {
            catalog: Some("sqlite:/srv/lk.sqlite".to_string()),
            data: Some("s3://bucket/lk".to_string()),
            read_only: true,
            version: Some(7),
            data_inlining_row_limit: Some(1234),
            create_if_not_exists: true,
            data_file_format: Some("parquet".to_string()),
        };
        db.databases.get_mut("lk").unwrap().storage = crate::lake_db::DbStorage::Lake(spec.clone());
        let back = PinkDb::load_bytes(&db.to_bytes().unwrap()).unwrap();
        assert_eq!(
            back.database_storage("lk"),
            Some(crate::lake_db::DbStorage::Lake(spec))
        );
        assert!(back.list_tables_in("lk").is_empty());
        assert_eq!(
            back.database_storage("main"),
            Some(crate::lake_db::DbStorage::Local)
        );
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
        // The magic starts the file, in header slot 0.
        let mut bad = full.clone();
        bad[..8].copy_from_slice(b"PINKDB09");
        let err = PinkDb::load_bytes(&bad).unwrap_err().to_string();
        assert!(
            err.contains("written by a newer pinkdb (format v9)"),
            "{err}"
        );
        // A damaged slot (its checksum no longer matches) is a corrupt file,
        // not a version error.
        let mut bad = full.clone();
        bad[SLOT_LEN - 4] ^= 0x01;
        let err = PinkDb::load_bytes(&bad).unwrap_err().to_string();
        assert!(err.contains("no header slot is valid"), "{err}");
        // Not a digit → plain bad magic.
        let mut bad = full.clone();
        bad[..8].copy_from_slice(b"PINKDB0X");
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
        let tdir = &footer.databases[0].schemas[0].tables[0];
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
        // The data region is everything between the header slots and the
        // footer; the slots' own bytes are covered by their checksums.
        let (footer_at, _) = footer_span(&bytes).unwrap();
        let mut detected = 0;
        for i in DATA_START as usize..footer_at as usize {
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
        // The slot's own CRC field is part of the check: the slot stops being
        // valid, and slot 1 was never written.
        let mut bad = bytes.clone();
        bad[SLOT_LEN - 4] ^= 0x01;
        let err = PinkDb::load_bytes(&bad).unwrap_err().to_string();
        assert!(err.contains("no header slot is valid"), "{err}");
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
        // Small enough for the byte-by-byte loop below. The footer carries an
        // exact distinct sketch per column (wave 122), so a 7-column table
        // pays a few hundred bytes per column even when tiny.
        assert!(small.len() < 3 * 8192, "{}", small.len());
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
            back.table_in("analytics", "t").unwrap().get(0, 0).unwrap(),
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

    /// The encrypted container wraps the whole `PINKDB07` image: a keyed
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
            DataType::Decimal { scale, .. } => Value::decimal(
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
            DataType::Uuid => Value::Uuid(match i % 5 {
                0 => 0,
                1 => u128::MAX,
                2 => 0x550e_8400_e29b_41d4_a716_4466_5544_0000,
                3 => 0x8000_0000_0000_4000_8000_0000_0000_0000,
                _ => (i as u128) << 64 | i as u128,
            }),
            // Not in the corpus dtype list: no .pink layout exists yet.
            DataType::Hugeint | DataType::Uhugeint => {
                unreachable!("128-bit integers have no .pink round trip")
            }
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
                        assert_bits_equal(
                            &ta.get(r, c).unwrap().unwrap(),
                            &tb.get(r, c).unwrap().unwrap(),
                        );
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
    fn golden_pinkdb07_bytes_are_pinned() {
        let db = golden_db();
        let bytes = db.to_bytes().unwrap();
        assert_eq!(bytes.len(), GOLDEN_LEN, "file length drifted");
        assert_eq!(
            crc32c(&footer_bytes(&bytes)),
            GOLDEN_FOOTER_CRC,
            "the footer bytes drifted"
        );
        assert_eq!(db.to_bytes_impl(false).unwrap(), bytes, "serial ≠ parallel");
        let back = PinkDb::load_bytes(&bytes).unwrap();
        assert_eq!(back.to_bytes().unwrap(), bytes, "open→save drift");
    }

    /// The header slot is the only metadata the writer positions last, and the
    /// footer sits after the data: a reader needs exactly two range requests.
    #[test]
    fn the_header_slot_and_the_footer_are_the_only_metadata_reads() {
        let db = golden_db();
        let bytes = db.to_bytes().unwrap();
        assert_eq!(&bytes[..8], MAGIC_V7);
        // Slot 0: the only valid one after a full rewrite.
        let (idx, slot) = read_slots(&bytes[..2 * HEADER_SLOT_LEN]).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(slot.iteration, 1);
        assert_eq!(slot.file_len as usize, bytes.len());
        let (offset, len) = footer_span(&bytes).unwrap();
        assert_eq!((offset, len), (slot.footer_offset, slot.footer_len));
        assert_eq!(
            crc32c(&bytes[offset as usize..(offset + len) as usize]),
            slot.footer_crc32c
        );
        // The data region starts after the two slots: the footer records
        // absolute offsets.
        let footer = read_store_footer(&bytes).unwrap();
        let dir = &footer.databases[0].schemas[0].tables[0].row_groups[0].columns[0];
        assert_eq!(
            dir.offset, DATA_START,
            "the first column chunk follows the slots"
        );
    }

    /// A saved image with the test-only writer hooks turned on.
    fn image_with_hooks(db: &PinkDb, free_ranges: Vec<(u64, u64)>, second_slot: bool) -> Vec<u8> {
        let mut data = VecSink::new();
        db.write_image_hooked(
            &mut data,
            true,
            false,
            false,
            &Hooks {
                free_ranges,
                second_slot,
            },
        )
        .unwrap();
        data.buf
    }

    fn tiny_db(name: &str) -> PinkDb {
        let mut db = PinkDb::new();
        db.create_table(name, vec![("x".to_string(), DataType::Integer)])
            .unwrap();
        db.table_mut(name)
            .unwrap()
            .append_row(vec![Value::Integer(1)])
            .unwrap();
        db
    }

    /// An image whose slots name two different footers: slot 0 at iteration 1,
    /// slot 1 at iteration 2.
    fn two_footer_image(first: &PinkDb, second: &PinkDb) -> Vec<u8> {
        let a = first.to_bytes().unwrap();
        let b = second.to_bytes().unwrap();
        let (a_offset, a_len, a_crc) = footer_location(&a, a.len() as u64).unwrap();
        let (b_offset, b_len, b_crc) = footer_location(&b, b.len() as u64).unwrap();
        // The stores differ only in the table name, so their data regions (and
        // the absolute chunk offsets in the second footer) are identical.
        assert_eq!(
            &a[DATA_START as usize..a_offset as usize],
            &b[DATA_START as usize..b_offset as usize]
        );
        let mut out = a;
        let second_at = out.len() as u64;
        out.extend_from_slice(&b[b_offset as usize..(b_offset + b_len) as usize]);
        let file_len = out.len() as u64;
        put_slot(
            &mut out,
            0,
            &Slot {
                iteration: 1,
                footer_offset: a_offset,
                footer_len: a_len,
                footer_crc32c: a_crc,
                file_len: a_offset + a_len,
            },
        );
        put_slot(
            &mut out,
            1,
            &Slot {
                iteration: 2,
                footer_offset: second_at,
                footer_len: b_len,
                footer_crc32c: b_crc,
                file_len,
            },
        );
        out
    }

    /// The valid slot with the higher iteration wins; a torn newer slot (bad
    /// CRC, or a file length past this file's) falls back to the older one.
    #[test]
    fn the_newer_header_slot_wins_and_a_torn_one_falls_back() {
        let bytes = two_footer_image(&tiny_db("a"), &tiny_db("b"));
        let (idx, slot) = read_slots(&bytes[..2 * HEADER_SLOT_LEN]).unwrap();
        assert_eq!((idx, slot.iteration), (1, 2));
        let back = PinkDb::load_bytes(&bytes).unwrap();
        assert!(back.table("b").is_ok(), "the newer slot's footer wins");
        assert!(back.table("a").is_err());

        // The newer slot's own CRC tore: the older slot still names a footer
        // that is there, so the file loads.
        let mut torn = bytes.clone();
        torn[HEADER_SLOT_LEN + SLOT_LEN - 4] ^= 0x01;
        let (idx, slot) = read_slots(&torn[..2 * HEADER_SLOT_LEN]).unwrap();
        assert_eq!((idx, slot.iteration), (0, 1));
        assert!(PinkDb::load_bytes(&torn).unwrap().table("a").is_ok());

        // A slot that records a longer file than this one is torn too.
        let mut longer = bytes.clone();
        put_slot(
            &mut longer,
            1,
            &Slot {
                file_len: u64::MAX,
                ..read_slots(&bytes[..2 * HEADER_SLOT_LEN]).unwrap().1
            },
        );
        assert!(PinkDb::load_bytes(&longer).unwrap().table("a").is_ok());
    }

    /// Two invalid slots are a corrupt file, and a valid slot whose footer CRC
    /// fails is one too: the slot names that footer, so there is nothing to
    /// fall back to.
    #[test]
    fn invalid_header_slots_are_a_corrupt_file() {
        let bytes = tiny_db("t").to_bytes().unwrap();
        let mut both = bytes.clone();
        both[SLOT_LEN - 4] ^= 0x01;
        let err = PinkDb::load_bytes(&both).unwrap_err().to_string();
        assert!(err.contains("no header slot is valid"), "{err}");

        let mut bad = bytes.clone();
        let (offset, _, _) = footer_location(&bad, bad.len() as u64).unwrap();
        bad[offset as usize] ^= 0x01;
        let err = PinkDb::load_bytes(&bad).unwrap_err().to_string();
        assert!(err.contains("footer checksum"), "{err}");
    }

    /// A full rewrite writes `free_ranges` empty; a non-empty section is
    /// parsed, range-checked and otherwise ignored.
    #[test]
    fn free_ranges_are_validated_and_otherwise_ignored() {
        let db = tiny_db("t");
        let bytes = db.to_bytes().unwrap();
        let (offset, _, _) = footer_location(&bytes, bytes.len() as u64).unwrap();
        let mut r = Reader::new(&bytes[offset as usize..]);
        r.u32().unwrap(); // footer version
        r.u32().unwrap(); // flags
        assert_eq!(r.u32().unwrap(), 0, "a full rewrite writes no free ranges");

        // A zero-length range at the data region's start is valid: it is clear
        // of the first chunk, which starts there.
        let with_ranges = image_with_hooks(&db, vec![(DATA_START, 0)], false);
        assert_eq!(
            PinkDb::load_bytes(&with_ranges)
                .unwrap()
                .to_bytes()
                .unwrap(),
            bytes,
            "the section is ignored"
        );

        for (what, ranges) in [
            ("unsorted", vec![(DATA_START + 8, 0), (DATA_START, 0)]),
            ("overlapping", vec![(DATA_START, 16), (DATA_START + 8, 16)]),
            ("before the data region", vec![(0, 8)]),
            ("past the footer", vec![(DATA_START, u64::MAX / 2)]),
            ("over a row group", vec![(DATA_START, 8)]),
        ] {
            let bad = image_with_hooks(&db, ranges, false);
            let err = PinkDb::load_bytes(&bad).unwrap_err().to_string();
            assert!(err.contains("free range"), "{what}: {err}");
        }
    }

    /// A file ending in the v6 trailer magic is an older pinkdb's: v6 kept its
    /// magic at the end, v7 starts with it.
    #[test]
    fn a_v6_trailer_is_refused_as_an_older_version() {
        let mut v6 = vec![0xABu8; 9000];
        v6.extend_from_slice(b"PINKDB06");
        let err = PinkDb::from_pink_bytes(&v6).unwrap_err().to_string();
        assert!(err.contains("older pinkdb (PINKDB06)"), "{err}");
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
        let chunk = t.scan_chunk(0).unwrap().unwrap();
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
            resign(&mut bad);
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
            let dtype = match next() % 11 {
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
                9 => DataType::Uuid,
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
                    DataType::Uuid => Value::Uuid(u128::from(r) << 64 | u128::from(r >> 1)),
                    // `write_table` refuses these before any page is encoded.
                    DataType::Hugeint | DataType::Uhugeint => {
                        unreachable!("128-bit integers have no .pink layout")
                    }
                    DataType::Decimal { scale, .. } => Value::decimal(r as i128, scale),
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
            let chunk = t.scan_chunk(chunk_idx).unwrap().unwrap();
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
            encode_chunk_typed(&t.scan_chunk(0).unwrap().unwrap().columns[0]).0,
            ENC_LZ_FRAME,
            "the identity corpus must exercise the compression frame"
        );
        let a = db.to_bytes_impl(true).unwrap();
        let b = db.to_bytes_impl(false).unwrap();
        assert_eq!(a, b, "parallel and serial saves differ");
    }

    /// A store wide enough to exercise every footer section and a table of
    /// more than one row group: VARCHAR/DECIMAL/NULL-heavy columns, indexes,
    /// constraints, macros, an empty table, two databases and two schemas.
    fn multi_row_group_corpus() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE big (i INTEGER, d DECIMAL(18,4), v VARCHAR, n INTEGER);")
            .unwrap();
        {
            let t = db.table_mut("big").unwrap();
            for i in 0..(ROW_GROUP_ROWS + 137) {
                t.append_row(vec![
                    Value::Integer(i as i32),
                    if i % 5 == 0 {
                        Value::Null
                    } else {
                        Value::decimal(i as i128 * 12_345, 4)
                    },
                    if i % 7 == 0 {
                        Value::Null
                    } else {
                        Value::Varchar(format!("row-{}-{}", i % 41, "x".repeat(i % 13)))
                    },
                    if i % 3 == 0 {
                        Value::Null
                    } else {
                        Value::Integer((i % 97) as i32)
                    },
                ])
                .unwrap();
            }
        }
        db.execute("CREATE INDEX i_big_v ON big(v);").unwrap();
        db.execute("CREATE TABLE main.small (k BIGINT PRIMARY KEY, s VARCHAR UNIQUE);")
            .unwrap();
        db.execute("INSERT INTO main.small VALUES (1, 'one'), (2, NULL), (3, 'three');")
            .unwrap();
        db.execute("CREATE MACRO twice(x) AS x + x;").unwrap();
        db.execute("CREATE TABLE main.empty (x INTEGER);").unwrap();
        db.execute("CREATE DATABASE analytics;").unwrap();
        db.execute("CREATE SCHEMA analytics.audit;").unwrap();
        db.execute("CREATE TABLE analytics.audit.events (at TIMESTAMP, msg VARCHAR);")
            .unwrap();
        db.execute(
            "INSERT INTO analytics.audit.events VALUES \
             (TIMESTAMP '2026-01-01 00:00:00', 'start'), (NULL, NULL);",
        )
        .unwrap();
        db
    }

    /// `save` streams the image through the file sink; the bytes must be
    /// exactly the in-memory image, for the serial encode too.
    #[test]
    fn streamed_save_writes_the_in_memory_bytes() {
        let db = multi_row_group_corpus();
        let want = db.to_pink_bytes().unwrap();
        assert_eq!(db.to_bytes_impl(false).unwrap(), want, "serial ≠ parallel");

        // The parallel file path (`save`, atomic).
        let path = temp_file("stream_parallel");
        db.save(path.to_str().unwrap()).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            want,
            "streamed save ≠ to_pink_bytes"
        );
        std::fs::remove_file(&path).ok();

        // The serial file path: the same staging writer `save_sync` uses,
        // with the test-only serial switch on.
        let path = temp_file("stream_serial");
        PinkDb::write_sync(&path, |sink| {
            db.write_image(sink, false, false, false).map(|_| ())
        })
        .unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            want,
            "serial streamed save ≠ to_pink_bytes"
        );
        std::fs::remove_file(&path).ok();

        // The checkpoint primitive writes the same bytes.
        let path = temp_file("stream_checkpoint");
        db.save_sync(&path).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), want);
        std::fs::remove_file(&path).ok();

        assert_eq!(
            PinkDb::load_bytes(&want).unwrap().to_pink_bytes().unwrap(),
            want,
            "the streamed image must round-trip"
        );
    }

    /// A save that fails after bytes have reached the staging file leaves the
    /// previous target intact and no temp file behind.
    #[test]
    fn a_failed_save_keeps_the_previous_target() {
        let path = temp_file("fail_save");
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (x INTEGER)").unwrap();
        db.table_mut("t")
            .unwrap()
            .append_row(vec![Value::Integer(1)])
            .unwrap();
        db.save(path.to_str().unwrap()).unwrap();
        let before = std::fs::read(&path).unwrap();

        // Tables are written in sorted order, so `a`'s data region lands in
        // the staging file before `z`'s row group is encoded; the pending
        // vector index serializes to an error at the end of `z`.
        let mut bad = PinkDb::new();
        bad.execute("CREATE TABLE a (x INTEGER);").unwrap();
        {
            let t = bad.table_mut("a").unwrap();
            for i in 0..(CHUNK_SIZE + 1) {
                t.append_row(vec![Value::Integer(i as i32)]).unwrap();
            }
        }
        bad.execute("CREATE TABLE z (y INTEGER);").unwrap();
        {
            let t = bad.table_mut("z").unwrap();
            t.append_row(vec![Value::Integer(7)]).unwrap();
            t.vector_indexes.push(crate::storage::VectorIndexDef {
                name: "v".to_string(),
                column: 0,
                provider: "vss".to_string(),
                options: Vec::new(),
                index: None,
                blob: None,
            });
        }
        let err = bad.save(path.to_str().unwrap()).unwrap_err();
        assert!(
            err.to_string().contains("needs the \"vss\" extension"),
            "{err}"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "a failed save must keep the previous target"
        );
        let stem = path.file_name().unwrap().to_string_lossy().to_string();
        let leftovers: Vec<String> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.starts_with(&format!("{stem}.tmp-")))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp files left behind: {leftovers:?}"
        );
        std::fs::remove_file(&path).ok();
    }

    /// save → open → save must reproduce the same bytes.
    #[test]
    fn open_save_is_byte_identity() {
        let mut db = PinkDb::new();
        db.execute("CREATE DATABASE analytics;").unwrap();
        db.execute("CREATE TABLE t (i INTEGER, v VARCHAR, d DOUBLE, b BOOLEAN);")
            .unwrap();
        db.execute(
            "INSERT INTO t VALUES (1, 'a', 1.5, true), (2, NULL, -0.0e0, false), \
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
            Some(crate::plan::ColumnDefault::constant(Value::Integer(42)))
        );
        // The index definition (the map is rebuilt from the rows).
        let t = back.table("c").unwrap();
        assert_eq!(t.indexes.len(), 1);
        assert_eq!(t.indexes[0].name, "i_c_pid");
        assert_eq!(t.indexes[0].column, 1);
        t.indexes[0].ensure_map(&t.columns[1]).unwrap();
        assert_eq!(
            *t.indexes[0].map(),
            Some(crate::storage::index_map_for(&t.columns[1]).unwrap()),
            "the map is built from the rows"
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
    /// is a `PINKDB07` image).
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
            assert_eq!(db.table("t").unwrap().get(1, 1).unwrap(), Some(Value::Null));
            // The checkpoint is a PINKDB07 image.
            let bytes = std::fs::read(&ckpt).unwrap();
            assert!(is_pinkdb(&bytes), "the checkpoint must be PINKDB07");
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
                    let chunk = t.scan_chunk(c).unwrap().unwrap();
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
        let stats = table_stats("main", "main", "t", t).unwrap();
        let col = &stats.columns[0];
        assert_eq!(col.min, Some(Value::Boolean(true)), "{col:?}");
        assert_eq!(col.max, Some(Value::Boolean(true)), "{col:?}");

        db.execute("CREATE TABLE f (b BOOLEAN); INSERT INTO f VALUES (false), (false);")
            .unwrap();
        let f = db.table_in("main", "f").unwrap();
        let stats = table_stats("main", "main", "f", f).unwrap();
        let col = &stats.columns[0];
        assert_eq!(col.min, Some(Value::Boolean(false)), "{col:?}");
        assert_eq!(col.max, Some(Value::Boolean(false)), "{col:?}");

        db.execute("CREATE TABLE n (b BOOLEAN); INSERT INTO n VALUES (NULL);")
            .unwrap();
        let n = db.table_in("main", "n").unwrap();
        let stats = table_stats("main", "main", "n", n).unwrap();
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
                    let v = decoded.get(row, i).unwrap().unwrap();
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
            fw.u32(0); // no free ranges
            fw.u32(1); // one database
            fw.str("main");
            fw.u8(STORAGE_LOCAL);
            fw.u32(1); // one schema
            fw.str("main");
            fw.u32(1); // one table
            fw.str("t");
            fw.u32(1); // one column
            fw.str("a");
            fw.u8(1); // INTEGER
            fw.u64(0); // row count
            fw.u32(0); // no row groups
            // The stats block: unknown sketch, no bounds, one empty zone.
            fw.u64(0);
            write_stat_value(&mut fw, None);
            write_stat_value(&mut fw, None);
            fw.u8(1);
            fw.u32(0);
            fw.u32(1);
            fw.u32(0);
            fw.bytes(rbac);
            image_with_footer(&fw.buf)
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
            w.u32(0); // schema grants
            w.u32(0); // database grants
            w.u32(0); // admins
        };
        // A hash that does not parse.
        let bad_hash = image(&{
            let mut w = Writer::new();
            w.u32(1);
            w.str("alice");
            w.str("not-a-hash");
            w.u32(0); // roles
            w.u32(0); // memberships
            w.u32(0); // table grants
            w.u32(0); // schema grants
            w.u32(0); // database grants
            w.u32(0); // admins
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
            w.str("main"); // schema
            w.str("t");
            w.u32(1); // one principal
            w.str("alice");
            w.u32(1); // one privilege
            w.u8(9); // ← unknown
            w.u32(0); // schema grants
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
        let tdir = &footer.databases[0].schemas[0].tables[0];
        assert_eq!(tdir.indexes.len(), 1);
        assert_eq!(tdir.indexes[0].name, "i");
        assert_eq!(tdir.indexes[0].column, 0);
        assert_eq!(tdir.indexes[0].distinct_keys, 2);
        assert_eq!(tdir.indexes[0].indexed_rows, 3);
        let loaded = PinkDb::load_bytes(&bytes).unwrap();
        let t = loaded.table_in("main", "t").unwrap();
        assert_eq!(t.indexes.len(), 1);
        assert!(!t.indexes_stale);
        assert!(t.indexes[0].map().is_none(), "a lazy open builds no map");
        t.indexes[0].ensure_map(&t.columns[0]).unwrap();
        assert_eq!(
            *t.indexes[0].map(),
            Some(crate::storage::index_map_for(&t.columns[0]).unwrap())
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
            resign(&mut bad);
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
        resign(&mut bad);
        let err = PinkDb::load_bytes(&bad)
            .map(|_| ())
            .unwrap_err()
            .to_string();
        assert!(err.contains("out of name order"), "{err}");
    }

    // ---------- lazy open (wave 136b-2) ----------

    fn wide_db(rows: usize) -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE w (a INTEGER, b INTEGER, c VARCHAR);")
            .unwrap();
        db.execute(&format!(
            "INSERT INTO w SELECT i::INTEGER, (i % 7)::INTEGER, 'v' || i::VARCHAR \
             FROM range(0, {rows}) t(i);"
        ))
        .unwrap();
        db
    }

    fn select(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
        match db.execute(sql).unwrap().pop().unwrap() {
            crate::QueryResult::Selected { rows, .. } => rows,
            other => panic!("expected rows from {sql}, got {other:?}"),
        }
    }

    /// Opening decodes nothing; a projected read decodes that column's chunks
    /// only, and a zone-pruned scan decodes the surviving chunk only.
    #[test]
    fn open_is_lazy_and_projected_scans_decode_only_what_they_read() {
        let db = wide_db(5000);
        let bytes = db.to_bytes().unwrap();

        // A fresh open for each query: a chunk stays decoded once touched.
        let mut pruned = PinkDb::load_bytes(&bytes).unwrap();
        assert_eq!(
            pruned.table("w").unwrap().decoded_chunks(),
            0,
            "an open must not decode a chunk"
        );
        assert_eq!(
            select(&mut pruned, "SELECT a FROM w WHERE a = 0;"),
            vec![vec![Value::Integer(0)]]
        );
        assert_eq!(
            pruned.table("w").unwrap().decoded_chunks(),
            1,
            "the zone map prunes the other two chunks before they are decoded"
        );

        let mut projected = PinkDb::load_bytes(&bytes).unwrap();
        assert_eq!(select(&mut projected, "SELECT a FROM w;").len(), 5000);
        assert_eq!(
            projected.table("w").unwrap().columns[0].decoded_chunks(),
            3,
            "5000 rows are three chunks of the projected column"
        );
    }

    /// A lazily opened store answers exactly like one whose chunks were all
    /// decoded up front.
    #[test]
    fn lazy_open_answers_like_a_fully_decoded_open() {
        let mut db = wide_db(5000);
        db.execute("CREATE TABLE d (k INTEGER, w INTEGER);")
            .unwrap();
        db.execute("INSERT INTO d SELECT i::INTEGER, (i % 50)::INTEGER FROM range(0, 1000) t(i);")
            .unwrap();
        db.execute("CREATE INDEX i_d_k ON d(k);").unwrap();
        let bytes = db.to_bytes().unwrap();
        let queries = [
            "SELECT a, count(*) FROM w GROUP BY a ORDER BY a;",
            "SELECT sum(b) FROM w WHERE a BETWEEN 100 AND 900;",
            "SELECT c FROM w WHERE a = 4242;",
            "SELECT count(*) FROM w WHERE b = 3;",
            "SELECT d.k, d.w FROM d JOIN w ON w.a = d.k WHERE d.k < 20 ORDER BY d.k;",
            "SELECT k FROM d WHERE k = 777;",
        ];
        let mut lazy = PinkDb::load_bytes(&bytes).unwrap();
        let expected: Vec<Vec<Vec<Value>>> = queries.iter().map(|q| select(&mut lazy, q)).collect();

        let mut eager = PinkDb::load_bytes(&bytes).unwrap();
        for name in ["w", "d"] {
            let t = eager.table(name).unwrap();
            for col in &t.columns {
                for i in 0..col.num_chunks() {
                    col.chunk(i).unwrap();
                }
            }
        }
        let got: Vec<Vec<Vec<Value>>> = queries.iter().map(|q| select(&mut eager, q)).collect();
        assert_eq!(got, expected, "lazy and eager opens must agree");
    }

    /// A v3 footer is refused with the older-version message (no reader).
    #[test]
    fn footer_v3_is_refused_with_the_older_version_message() {
        let db = wide_db(10);
        let mut old = db.to_bytes().unwrap();
        relabel_footer_version(&mut old, 3);
        let err = PinkDb::load_bytes(&old)
            .map(|_| ())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("older pinkdb (footer version 3)"),
            "v3 footer: {err}"
        );
    }

    /// The open-time CRC sweep catches a damaged chunk without decoding it.
    #[test]
    fn a_corrupt_chunk_fails_at_open() {
        let db = wide_db(100);
        let bytes = db.to_bytes().unwrap();
        let footer = read_store_footer(&bytes).unwrap();
        let dir = &footer.databases[0].schemas[0].tables[0].row_groups[0].columns[0];
        let mut bad = bytes.clone();
        bad[dir.offset as usize + 4] ^= 0xff;
        let err = PinkDb::load_bytes(&bad)
            .map(|_| ())
            .unwrap_err()
            .to_string();
        assert!(err.contains("fails its checksum"), "corrupt chunk: {err}");
    }

    /// An index map is built on first use (decoding only the indexed column)
    /// and unloaded by a mutation instead of being rebuilt eagerly.
    #[test]
    fn an_index_map_is_lazy_and_unloaded_by_a_mutation() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (k INTEGER, v VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(2,'bb'),(3,'c');")
            .unwrap();
        db.execute("CREATE INDEX i ON t(k);").unwrap();
        let bytes = db.to_bytes().unwrap();

        let mut back = PinkDb::load_bytes(&bytes).unwrap();
        {
            let t = back.table("t").unwrap();
            assert!(t.indexes[0].map().is_none(), "an open keeps it unloaded");
            let mut key = Vec::new();
            crate::exec::aggregate::encode_value(&mut key, &Value::Integer(2));
            assert_eq!(t.indexes[0].lookup(&t.columns[0], &key).unwrap(), [1, 2]);
        }
        assert!(
            back.table("t").unwrap().indexes[0].map().is_some(),
            "the first use builds the map"
        );
        assert_eq!(
            back.table("t").unwrap().decoded_chunks(),
            1,
            "building the map decodes the indexed column only"
        );
        assert_eq!(
            select(&mut back, "SELECT v FROM t WHERE k = 2;"),
            vec![
                vec![Value::Varchar("b".into())],
                vec![Value::Varchar("bb".into())]
            ]
        );

        back.execute("UPDATE t SET v = 'b2' WHERE k = 2;").unwrap();
        assert!(
            back.table("t").unwrap().indexes[0].map().is_none(),
            "a mutation unloads the map instead of rebuilding it"
        );
        assert_eq!(
            select(&mut back, "SELECT v FROM t WHERE k = 2 ORDER BY v;"),
            vec![
                vec![Value::Varchar("b2".into())],
                vec![Value::Varchar("b2".into())]
            ]
        );
    }
}
