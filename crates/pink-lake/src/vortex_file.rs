//! Vortex data files: one Vortex file per table (the v2 lake default).
//!
//! A data file is a plain Vortex file holding one struct array (one field per
//! table column, in schema order). The writer feeds the engine's chunks in
//! order and lets Vortex group them into row blocks of [`WRITE_BLOCK_ROWS`]
//! rows (~[`WRITE_BLOCK_BYTES`]; `format_version` 3). Format-version-2 files
//! used one block per engine chunk ([`pinkdb::CHUNK_SIZE`] rows) and are
//! still read. Either way the loader slices stored blocks back into
//! `CHUNK_SIZE`-row engine chunks, so a loaded table is densely chunked (a
//! saved table with ragged chunks — after deletes, say — loads compacted;
//! every value and the row order are unchanged).
//!
//! Column values map to native Vortex types:
//!
//! | pinkdb    | Vortex                                |
//! |-----------|---------------------------------------|
//! | BOOLEAN   | `bool`                                |
//! | INTEGER   | `i32`                                 |
//! | BIGINT    | `i64`                                 |
//! | DOUBLE    | `f64` (raw bits: NaN payloads survive)|
//! | VARCHAR   | `utf8`                                |
//! | DATE      | `i32` (days since 1970-01-01)         |
//! | TIMESTAMP | `i64` (microseconds since epoch)      |
//!
//! DATE and TIMESTAMP are stored as their raw storage integers, not the
//! native `vortex.date`/`vortex.timestamp` extension dtypes. The timestamp
//! extension cannot represent the engine's full `i64` microsecond range: its
//! min/max statistics path converts values through a calendar type whose
//! span is about ±20 000 years and *panics* beyond it, so a table with
//! `TIMESTAMP i64::MIN`/`i64::MAX` (or any `|v| > 6.3e17` µs) would abort a
//! save. Raw `i64` storage round-trips every bit pattern losslessly, and no
//! value is ever changed. The exact pinkdb schema (column names + types)
//! travels in a user metadata segment, `pinkdb.table`, together with the
//! table identity:
//!
//! ```json
//! { "db": "main", "table": "t", "columns": [
//!     { "name": "a", "type": "bigint" }, { "name": "b", "type": "timestamp" } ] }
//! ```
//!
//! The identity lets the loader reject a data file whose table does not match
//! the catalog key (the v1 `.pink` files embedded their names the same way),
//! and the schema makes the loader independent of Vortex type inference: the
//! Vortex dtypes are checked against it, never trusted over it.
//!
//! # Corrupt files must not kill the process
//!
//! The Vortex decoder has several `panic!`/debug-assertion sites on malformed
//! layouts (the review found them for single-byte flips inside a valid file).
//! The whole decoding boundary — `open_buffer` (footer + metadata decode),
//! the `pinkdb.table` metadata parse, the scan/stream setup and every array
//! decode — runs under [`std::panic::catch_unwind`], and a caught panic is
//! reported as a clean [`LakeError::Corrupt`] naming the data file
//! ("decoder panic caught"), the same class of error as any other malformed
//! metadata. The default panic hook still prints one line to stderr before
//! the caught error; the library does not install or swap the global hook
//! (that would race other threads), so that residual stderr line is accepted
//! and documented. An [`AssertUnwindSafe`] boundary is fine here: the session,
//! runtime and partial store are all dropped per call, and nothing is reused
//! after a caught panic.

use std::any::Any;
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Arc;

use rayon::prelude::*;

use pinkdb::{ColumnStats, DataChunk, DataTable, DataType, PinkDb, Value, Vector, VectorData};
use serde::{Deserialize, Serialize};
use vortex::VortexSessionDefault;
use vortex::array::arrays::bool::BoolArrayExt;
use vortex::array::arrays::struct_::StructArrayExt;
use vortex::array::arrays::{BoolArray, PrimitiveArray, StructArray, VarBinViewArray};
use vortex::array::stats::StatsSet;
use vortex::array::validity::Validity;
use vortex::array::{ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute};
use vortex::buffer::{BitBuffer, Buffer, ByteBuffer};
use vortex::dtype::{DType, FieldNames, Nullability, PType, StructFields};
use vortex::expr::stats::{Precision, Stat};
use vortex::expr::{root, select};
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex::io::runtime::BlockingRuntime;
use vortex::io::runtime::current::{CurrentThreadRuntime, CurrentThreadWorkerPool};
use vortex::io::session::RuntimeSessionExt;
use vortex::scalar::ScalarValue;
use vortex::session::VortexSession;

use crate::{LakeError, Result, empty_single_table_store};

/// The user metadata segment key holding the table identity + pinkdb schema.
pub(crate) const TABLE_METADATA_KEY: &str = "pinkdb.table";

/// The largest `pinkdb.table` metadata segment the loader parses. A schema
/// for any representable table is far below this; the cap stops a tampered
/// file from making the loader deserialize an arbitrarily large segment.
pub(crate) const MAX_TABLE_METADATA: usize = 1 << 20;

/// One column of the pinkdb schema stored in a data file.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColumnMeta {
    name: String,
    #[serde(rename = "type")]
    kind: TypeTag,
}

/// The pinkdb type of one column, serialized as a lowercase name. The tag
/// list is the engine's type list; a file carrying anything else is corrupt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TypeTag {
    Boolean,
    Integer,
    Bigint,
    Double,
    Varchar,
    Date,
    Timestamp,
}

impl TypeTag {
    fn of(t: DataType) -> TypeTag {
        match t {
            DataType::Boolean => TypeTag::Boolean,
            DataType::Integer => TypeTag::Integer,
            DataType::Bigint => TypeTag::Bigint,
            DataType::Double => TypeTag::Double,
            DataType::Varchar => TypeTag::Varchar,
            DataType::Date => TypeTag::Date,
            DataType::Timestamp => TypeTag::Timestamp,
            // Refused at the encode entry: there is no Vortex mapping yet.
            DataType::Decimal { .. } | DataType::Interval => {
                unreachable!("DECIMAL/INTERVAL are refused at the Vortex encode entry")
            }
        }
    }

    fn to_pinkdb(self) -> DataType {
        match self {
            TypeTag::Boolean => DataType::Boolean,
            TypeTag::Integer => DataType::Integer,
            TypeTag::Bigint => DataType::Bigint,
            TypeTag::Double => DataType::Double,
            TypeTag::Varchar => DataType::Varchar,
            TypeTag::Date => DataType::Date,
            TypeTag::Timestamp => DataType::Timestamp,
        }
    }
}

/// The table identity + schema stored in (and required from) every v2 data
/// file.
#[derive(Debug, Serialize, Deserialize)]
struct TableMetadata {
    db: String,
    table: String,
    columns: Vec<ColumnMeta>,
}

/// The decoding side's runtime + session. Created per call: a lake read is
/// dominated by decoding column chunk into engine vectors, and the session
/// registration cost is negligible against that. (The writer uses
/// [`write_session`], which adds the worker pool its compression needs.)
fn session() -> (CurrentThreadRuntime, VortexSession) {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    (runtime, session)
}

/// The writing side's runtime: the same current-thread runtime, plus — when
/// `parallel` — a worker pool that drives the writer's spawned CPU tasks
/// (per-block compression) on background threads. Without the pool every
/// compression job runs inline on the calling thread and a table save is
/// single-threaded; with it the main-thread `block_on` still runs the
/// writer's sequential state machine while the pool executes the compression
/// cascade in parallel. The encoding itself is deterministic and
/// scheduling-independent, so the file bytes are unchanged
/// (`parallel_writing_emits_identical_bytes`).
fn write_session(parallel: bool) -> (CurrentThreadRuntime, CurrentThreadWorkerPool, VortexSession) {
    let runtime = CurrentThreadRuntime::new();
    let pool = runtime.new_pool();
    if parallel {
        pool.set_workers_to_available_parallelism();
    }
    let session = VortexSession::default().with_handle(runtime.handle());
    (runtime, pool, session)
}

fn corrupt(path: &Path, message: String) -> LakeError {
    LakeError::Corrupt {
        path: path.to_path_buf(),
        message,
    }
}

fn vortex_error(context: &str, e: impl std::fmt::Display) -> LakeError {
    LakeError::Vortex {
        context: context.to_string(),
        message: e.to_string(),
    }
}

/// Rows per Vortex row block (format_version 3; Vortex's own default, pinned
/// so a dependency upgrade cannot silently change the layout). Fewer, larger
/// blocks cut the writer's per-block statistics and compression-scheme work.
const WRITE_BLOCK_ROWS: usize = 8192;

/// Target bytes per data block (Vortex's default coalescing, pinned).
///
/// The target was re-measured against 8 MiB and 16 MiB (wave-A A/B: interleaved
/// best-of-N on this repo's CLI, `bench/gen_sales.py` corpus, 10-core M-series
/// host; exact numbers in the wave-A report) and stays at 1 MiB:
///
/// | target | 1M rows: bytes / save / open | 10M rows: bytes / save / open |
/// |--------|------------------------------|-------------------------------|
/// | 1 MiB  | 2 638 079 B / 52 ms / 13 ms  | 26 310 679 B / 254 ms / 119 ms |
/// | 8 MiB  | 2 634 223 B / 59 ms / 24 ms  | 26 272 343 B / 255 ms / 229 ms |
/// | 16 MiB | 2 634 223 B / 60 ms / 23 ms  | 26 269 647 B / 267 ms / 241 ms |
///
/// A larger target saves only 0.15% of the bytes and makes `.lake open` about
/// 2x slower: the reader sub-splits stored chunks at ~100k rows, and at these
/// targets most stored chunks end on a row count that is not a multiple of the
/// engine's `CHUNK_SIZE` (2048). Each such ragged tail makes the *next* engine
/// chunk take the element-wise merge path in `append_chunk` instead of the
/// move fast path, which accounted for essentially all of the regression
/// (loader phase timing at 10M: append 0.7 ms at 1 MiB vs 110 ms at 8/16 MiB).
///
/// The scheme-exclusion half of that A/B (RunEnd / IntRLE / ALPRD / FloatRLE)
/// could not be expressed through the `vortex` facade: `exclude_schemes` takes
/// `SchemeId`s, whose only public constructor (`SchemeExt::id()`) and whose
/// scheme types (`vortex_btrblocks::schemes::*`) are not re-exported by the
/// `vortex` crate with our feature set, and the facade-only reconstruction
/// (`BtrBlocksCompressorBuilder::empty()` + `with_new_scheme`) is not faithful
/// — `build()` re-refines with `AllowedIds::all()`, selecting the FoR v2
/// variant whose serialized ID the write context then rejects (the 10M save
/// aborts with "Serialized array ID fastlanes.for.v2 not permitted by ctx").
/// The exclusions would have been byte-neutral here anyway: the 1M exclusion
/// arm is byte-identical to 8/16 MiB, and the 10M files contain no
/// `vortex.runend`, `vortex.alprd`, `fastlanes.rle` or `fastlanes.delta`
/// encoding at all (they are, recursively: sequence, dict, fsst, alp, bool,
/// bitpacked, primitive, shared, slice, struct).
///
/// The target is a layout/performance knob, not part of the format contract:
/// files written at any target read back identically (the loader slices stored
/// chunks by row count, never by layout), `format_version` stays 3, and
/// `parallel_writing_emits_identical_bytes` keeps pinning serial == parallel
/// bytes for a fixed target.
const WRITE_BLOCK_BYTES: u64 = 1 << 20;

/// The longest scan chunk the loader accepts. The Vortex reader sub-splits
/// stored layout chunks into scan ranges of at most its ideal split size
/// (100,000 rows in Vortex 0.88), so every chunk a valid file yields is well
/// below this; a tampered length is rejected here (bounding the slicing work
/// it could ask for) rather than trusted.
const MAX_STORED_CHUNK_ROWS: usize = 1 << 20;

/// Engine chunks converted per parallel window while streaming into the
/// writer: large enough to keep every core busy converting, small enough
/// that only a window's worth of converted arrays is alive at once and the
/// writer's compression of window *k* overlaps the conversion of *k + 1*.
const CONVERT_WINDOW: usize = 128;

/// Slots of the per-decode VARCHAR interning cache (a power of two). Big
/// enough that the handful of distinct strings a chunk repeats find their
/// own slots, small enough to live in L1 (128 × 24 bytes).
const INTERN_SLOTS: usize = 128;

/// [`INTERN_SLOTS`] as a shift: `slot = hash >> INTERN_SHIFT`.
const INTERN_SHIFT: u32 = 64 - INTERN_SLOTS.trailing_zeros();

/// The direct-mapped cache slot for a raw Vortex view key: fold the 16-byte
/// key to 64 bits and mix once (Fibonacci hashing). The *high* bits of the
/// product depend on every input bit — unlike the low bits, which for short
/// inlined views only see the string length — and index the table.
#[inline]
fn intern_slot(key: u128) -> usize {
    let h = (key as u64 ^ (key >> 64) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (h >> INTERN_SHIFT) as usize
}

/// The `pinkdb.table` metadata segment for a table.
fn table_metadata(dbname: &str, table: &str, src: &DataTable) -> Result<Vec<u8>> {
    let metadata = TableMetadata {
        db: dbname.to_string(),
        table: table.to_string(),
        columns: src
            .schema()
            .iter()
            .map(|(name, t)| ColumnMeta {
                name: name.clone(),
                kind: TypeTag::of(*t),
            })
            .collect(),
    };
    serde_json::to_vec(&metadata).map_err(|e| LakeError::Internal {
        message: format!("serializing table metadata for \"{dbname}.{table}\": {e}"),
    })
}

/// Encode one table as a Vortex file (with its `pinkdb.table` segment) and
/// return its bytes.
#[cfg(test)]
pub(crate) fn encode_table(dbname: &str, table: &str, src: &DataTable) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    encode_table_into(dbname, table, src, &mut bytes)?;
    Ok(bytes)
}

/// Encode one table as a Vortex file (with its `pinkdb.table` segment)
/// straight into `sink` (the lake streams it into the data file's temp
/// file instead of materializing the whole file in memory).
pub(crate) fn encode_table_into(
    dbname: &str,
    table: &str,
    src: &DataTable,
    sink: &mut (dyn Write + Send),
) -> Result<()> {
    let metadata = table_metadata(dbname, table, src)?;
    encode_table_with_metadata(dbname, table, src, Some(metadata), sink, true)
}

/// The writer core: stream `src` into `sink` as a Vortex file with an
/// optional `pinkdb.table` segment. The loader requires the segment; `None`
/// exists so tests can craft a file that lacks it (a tamper the loader must
/// reject). `parallel` selects the worker-pool runtime (production) or the
/// bare current-thread runtime (test-only pin); both emit identical bytes.
///
/// Engine chunks are converted to Vortex arrays in parallel windows of
/// [`CONVERT_WINDOW`] chunks and pushed in order, so conversion overlaps the
/// writer's compression instead of running as a serial phase before it. The
/// pushed sequence of arrays is exactly the one the former single
/// `ChunkedArray` held, so the file bytes are unchanged.
fn encode_table_with_metadata(
    dbname: &str,
    table: &str,
    src: &DataTable,
    metadata: Option<Vec<u8>>,
    sink: &mut (dyn Write + Send),
    parallel: bool,
) -> Result<()> {
    let schema = src.schema();
    if schema.is_empty() {
        return Err(LakeError::NotRepresentable {
            message: format!(
                "table \"{dbname}.{table}\" has no columns and cannot be encoded as \
                 a Vortex file"
            ),
        });
    }
    // DECIMAL and INTERVAL have no Vortex storage mapping yet: the engine's
    // persistence (`.pink` v5) and the `pinklake` lake format support
    // both. Refuse at the encode entry instead of mis-encoding.
    if let Some((name, dtype)) = schema
        .iter()
        .find(|(_, t)| matches!(t, DataType::Decimal { .. } | DataType::Interval))
    {
        return Err(LakeError::NotRepresentable {
            message: format!(
                "column \"{name}\" ({dtype}) is not supported by the Vortex lake format \
                 yet; use the `pinklake` build or the engine's `.pink` persistence"
            ),
        });
    }
    // One pass over the chunks computes both the per-chunk NULL flags (used
    // to build each chunk's validity) and the table-level nullability (all
    // chunks of a table share one dtype, so one nullable chunk makes the
    // column nullable everywhere).
    let (nulls, chunk_nulls) = column_nullability(src, &schema, dbname, table)?;
    let dtype = struct_dtype(&schema, &nulls);

    let (runtime, _pool, session) = write_session(parallel);
    let strategy = WriteStrategyBuilder::from_session(&session)
        .with_row_block_size(WRITE_BLOCK_ROWS)
        .with_data_block_target_bytes(Some(WRITE_BLOCK_BYTES))
        .build();
    let mut options = session.write_options().with_strategy(strategy);
    if let Some(metadata) = metadata {
        options =
            options.with_metadata_segment(TABLE_METADATA_KEY, ByteBuffer::copy_from(metadata));
    }
    let write_error = |e| vortex_error(&format!("writing table \"{dbname}.{table}\""), e);
    let mut writer = options.blocking(&runtime).writer(sink, dtype);

    if chunk_nulls.is_empty() {
        // An empty table: one 0-row struct, so the file still carries the
        // schema. The loader appends nothing back (0 chunks).
        let children = schema
            .iter()
            .zip(&nulls)
            .map(|((_, t), n)| encode_column(&Vector::new_empty(*t), *t, *n, false))
            .collect::<Result<Vec<_>>>()?;
        let empty = StructArray::try_new(field_names(&schema), children, 0, Validity::NonNullable)
            .map_err(|e| vortex_error(&format!("encoding empty table \"{dbname}.{table}\""), e))?
            .into_array();
        writer.push(empty).map_err(write_error)?;
    }
    for window in (0..chunk_nulls.len()).step_by(CONVERT_WINDOW) {
        let end = (window + CONVERT_WINDOW).min(chunk_nulls.len());
        let arrays: Vec<Result<ArrayRef>> = (window..end)
            .into_par_iter()
            .map(|i| {
                let chunk = src.scan_chunk(i).ok_or_else(|| LakeError::Internal {
                    message: format!(
                        "chunk {i} of \"{dbname}.{table}\" disappeared while encoding"
                    ),
                })?;
                encode_chunk(&chunk, &schema, &nulls, &chunk_nulls[i], dbname, table)
            })
            .collect();
        for array in arrays {
            writer.push(array?).map_err(write_error)?;
        }
    }
    writer.finish().map_err(write_error)?;
    Ok(())
}

/// Decode a Vortex data file into a one-table [`PinkDb`] (the same shape a
/// v1 `.pink` data file opens as, so the loader's checks are identical).
///
/// Every Vortex decoder entry point lives inside [`decode_buffer`] and is
/// reached only through [`with_decoder_guard`], so a malformed file can
/// produce a clean [`LakeError::Corrupt`] but never an escaping panic.
///
/// The in-memory entry point exists for tests and for callers holding a
/// borrowed slice; the lake itself hands its buffer over to
/// [`decode_table_owned`] so it is never copied.
#[cfg(test)]
pub(crate) fn decode_table(path: &Path, bytes: &[u8]) -> Result<PinkDb> {
    decode_buffer(path, ByteBuffer::copy_from(bytes))
}

/// [`decode_table`] for a buffer the caller already owns: the bytes are
/// *moved* into the Vortex byte buffer (zero copy), not copied again. This
/// is the object-storage entry (the whole object arrives as a `Vec<u8>`)
/// and the local entry (the file is read into exactly this buffer).
pub(crate) fn decode_table_owned(path: &Path, bytes: Vec<u8>) -> Result<PinkDb> {
    decode_buffer(path, ByteBuffer::from(bytes))
}

/// Decode only the `projection` columns (ascending schema indices) of one
/// data file into a one-table [`PinkDb`] over the **full** schema: every
/// unprojected column holds a NULL placeholder with the chunk lengths of the
/// decoded ones, so row counts and chunk boundaries are exactly those of a
/// full decode. Returns the store plus, per column, whether it was decoded.
///
/// Locally the file is opened with positioned reads (`open_path`): the
/// unprojected columns' data segments are never read, let alone decoded.
/// Over object storage (no ranged reads in this build) — and for an
/// encrypted lake, where the whole container is unsealed first — the object
/// is read whole, still decoding only `projection`.
///
/// Chunks are streamed and decoded in bounded windows
/// ([`CONVERT_WINDOW`]-many engine chunks at a time); nothing collects the
/// whole file's encoded data, unlike the full decode path (which builds the
/// table it returns anyway).
pub(crate) fn decode_projected(
    path: &Path,
    storage: &dyn crate::storage::Storage,
    rel: &str,
    projection: &[usize],
) -> Result<(PinkDb, Vec<bool>)> {
    with_decoder_guard(path, "opening and decoding it", || {
        let (runtime, session) = session();
        let file = match storage.local_path(rel) {
            // Local: positioned reads of the footer plus exactly the
            // projected fields' segments.
            Some(local) => runtime
                .block_on(session.open_options().include_metadata().open_path(&local))
                .map_err(|e| corrupt(path, format!("not a valid Vortex file: {e}")))?,
            // Object storage: whole-object read, still no unprojected decode.
            None => {
                let bytes = storage.read(rel, &format!("loading data file \"{rel}\""))?;
                session
                    .open_options()
                    .include_metadata()
                    .open_buffer(ByteBuffer::from(bytes))
                    .map_err(|e| corrupt(path, format!("not a valid Vortex file: {e}")))?
            }
        };
        let metadata = parse_metadata(path, &file)?;
        let schema = schema_of(path, &file, &metadata)?;
        decode_projected_file(
            path, &runtime, &session, file, &metadata, &schema, projection,
        )
    })
}

/// The decode core of [`decode_projected`] (already inside the guard).
///
/// The projection is a non-empty ascending subset of `schema`'s indices; the
/// caller (the attach layer) validates its bound, this function checks the
/// rest defensively.
fn decode_projected_file(
    path: &Path,
    runtime: &CurrentThreadRuntime,
    session: &VortexSession,
    file: vortex::file::VortexFile,
    metadata: &TableMetadata,
    schema: &[(String, DataType)],
    projection: &[usize],
) -> Result<(PinkDb, Vec<bool>)> {
    let n_cols = schema.len();
    if projection.is_empty()
        || projection.iter().any(|&i| i >= n_cols)
        || projection.windows(2).any(|w| w[0] >= w[1])
    {
        return Err(corrupt(
            path,
            format!(
                "internal error: invalid decode projection {projection:?} for a {n_cols}-column \
                 file"
            ),
        ));
    }
    let mut present = vec![false; n_cols];
    for &i in projection {
        present[i] = true;
    }
    // The streamed struct carries exactly the projected fields, in the
    // order they were requested; map each schema index to its field slot.
    let mut field_of: Vec<Option<usize>> = vec![None; n_cols];
    for (fi, &i) in projection.iter().enumerate() {
        field_of[i] = Some(fi);
    }
    let names: Vec<&str> = projection.iter().map(|&i| schema[i].0.as_str()).collect();
    let bound = select(names, root())
        .bind(file.dtype())
        .and_then(|expr| expr.optimize_recursive())
        .map_err(|e| corrupt(path, format!("building the scan projection: {e}")))?;
    let mut stream = file
        .scan()
        .map_err(|e| corrupt(path, format!("opening a Vortex scan: {e}")))?
        .with_projection(bound)
        .into_array_stream()
        .map_err(|e| corrupt(path, format!("opening a Vortex scan: {e}")))?;

    // One dense chunk builder per projected column, advanced in lockstep:
    // the chunk lengths come out identical to a full decode's
    // ([`pinkdb::CHUNK_SIZE`] each, one shorter tail) whatever the file's
    // block boundaries are. That matters because the reader's block
    // boundaries depend on *which* fields a scan references, so a later
    // projected decode of other columns must still land on the same chunk
    // boundaries to merge.
    let mut builders: Vec<ColumnBuilder> = projection
        .iter()
        .map(|&i| ColumnBuilder::new(schema[i].1))
        .collect();

    let mut pending: Vec<ArrayRef> = Vec::new();
    for item in runtime.block_on_stream(&mut stream) {
        let block = item.map_err(|e| corrupt(path, format!("reading a Vortex chunk: {e}")))?;
        let len = block.len();
        if len > MAX_STORED_CHUNK_ROWS {
            return Err(corrupt(
                path,
                format!(
                    "a stored chunk has {len} rows, over the {MAX_STORED_CHUNK_ROWS}-row limit"
                ),
            ));
        }
        if len <= pinkdb::CHUNK_SIZE {
            pending.push(block);
        } else {
            for start in (0..len).step_by(pinkdb::CHUNK_SIZE) {
                let end = (start + pinkdb::CHUNK_SIZE).min(len);
                pending.push(
                    block
                        .slice(start..end)
                        .map_err(|e| corrupt(path, format!("slicing a Vortex chunk: {e}")))?,
                );
            }
        }
        if pending.len() >= CONVERT_WINDOW {
            accumulate_projected(
                path,
                session,
                schema,
                projection,
                &mut pending,
                &mut builders,
            )?;
        }
    }
    accumulate_projected(
        path,
        session,
        schema,
        projection,
        &mut pending,
        &mut builders,
    )?;

    // Assemble the full-schema table: decoded columns from the builders,
    // NULL placeholders (shared per distinct chunk length, so a multi-chunk
    // read allocates a couple of vectors per absent column, not one per
    // chunk) for the rest.
    let decoded: Vec<(usize, Vec<Arc<Vector>>)> = projection
        .iter()
        .copied()
        .zip(builders.into_iter().map(ColumnBuilder::finish))
        .collect();
    let rows: usize = decoded
        .first()
        .map(|(_, chunks)| chunks.iter().map(|c| c.len()).sum())
        .unwrap_or(0);
    let lens: Vec<usize> = decoded
        .first()
        .map(|(_, chunks)| chunks.iter().map(|c| c.len()).collect())
        .unwrap_or_default();
    let mut placeholders: std::collections::HashMap<(usize, usize), Arc<Vector>> =
        std::collections::HashMap::new();
    let mut columns: Vec<pinkdb::storage::Column> = Vec::with_capacity(n_cols);
    let mut decoded = decoded.into_iter();
    for (i, (name, dtype)) in schema.iter().enumerate() {
        let chunks = if field_of[i].is_some() {
            decoded
                .next()
                .map(|(_, chunks)| chunks)
                .expect("one decoded column per projected field")
        } else {
            lens.iter()
                .map(|&len| placeholder(&mut placeholders, i, *dtype, len))
                .collect()
        };
        columns.push(pinkdb::storage::Column {
            name: name.clone(),
            dtype: *dtype,
            default: None,
            chunks,
        });
    }
    let mut mini = empty_single_table_store(&metadata.db, &metadata.table, schema.to_vec())?;
    *mini
        .table_mut_in(&metadata.db, &metadata.table)
        .map_err(|e| LakeError::Engine {
            context: format!("loading data file \"{}\"", path.display()),
            source: e,
        })? = DataTable::from_columns(metadata.table.clone(), columns, rows);
    Ok((mini, present))
}

/// One projected column's dense chunk builder. Decoded pieces arrive at the
/// file's block boundaries (not chunk-aligned); a piece that already covers
/// a full engine chunk is adopted as-is, rows that straddle a boundary are
/// copied into a carried chunk. Output chunk lengths are exactly a full
/// decode's, independent of block boundaries (see [`decode_projected_file`]).
struct ColumnBuilder {
    dtype: DataType,
    chunks: Vec<Arc<Vector>>,
    /// The chunk under construction (< [`pinkdb::CHUNK_SIZE`] rows).
    carry: Vector,
}

impl ColumnBuilder {
    fn new(dtype: DataType) -> Self {
        Self {
            dtype,
            chunks: Vec::new(),
            carry: Vector::with_capacity(dtype, pinkdb::CHUNK_SIZE),
        }
    }

    /// Append one decoded piece (≤ [`pinkdb::CHUNK_SIZE`] rows).
    fn push(&mut self, v: Vector) {
        debug_assert!(v.len() <= pinkdb::CHUNK_SIZE);
        // Fast path: a full piece with nothing carried becomes its own chunk
        // uncopied.
        if self.carry.is_empty() && v.len() == pinkdb::CHUNK_SIZE {
            self.chunks.push(Arc::new(v));
            return;
        }
        let mut offset = 0;
        while offset < v.len() {
            if self.carry.len() == pinkdb::CHUNK_SIZE {
                self.promote();
            }
            let take = (pinkdb::CHUNK_SIZE - self.carry.len()).min(v.len() - offset);
            self.carry.extend_from(&v, offset, take);
            offset += take;
        }
        if self.carry.len() == pinkdb::CHUNK_SIZE {
            self.promote();
        }
    }

    fn promote(&mut self) {
        let full = std::mem::replace(
            &mut self.carry,
            Vector::with_capacity(self.dtype, pinkdb::CHUNK_SIZE),
        );
        self.chunks.push(Arc::new(full));
    }

    /// The column's chunks: a short tail stays short (a full decode's
    /// chunking leaves one shorter final chunk); a zero-row column keeps one
    /// empty chunk like [`DataTable::new`].
    fn finish(mut self) -> Vec<Arc<Vector>> {
        if !self.carry.is_empty() {
            let tail = std::mem::replace(&mut self.carry, Vector::new_empty(self.dtype));
            self.chunks.push(Arc::new(tail));
        }
        if self.chunks.is_empty() {
            self.chunks.push(Arc::new(Vector::new_empty(self.dtype)));
        }
        self.chunks
    }
}

/// Decode one window of still-encoded chunks in parallel and feed them into
/// the dense builders (in file order) — the bounded-window half of
/// [`decode_projected_file`].
fn accumulate_projected(
    path: &Path,
    session: &VortexSession,
    schema: &[(String, DataType)],
    projection: &[usize],
    pending: &mut Vec<ArrayRef>,
    builders: &mut [ColumnBuilder],
) -> Result<()> {
    let decoded: Vec<Result<Option<Vec<Vector>>>> = pending
        .par_iter()
        .map_init(
            || session.create_execution_ctx(),
            |ctx, chunk| decode_projected_chunk(path, schema, projection, chunk, ctx),
        )
        .collect();
    pending.clear();
    for piece in decoded {
        let Some(piece) = piece? else {
            continue;
        };
        for (builder, column) in builders.iter_mut().zip(piece) {
            builder.push(column);
        }
    }
    Ok(())
}

/// Decode one projected stored chunk into the projected columns, in
/// projection order (`None` for a 0-row chunk).
fn decode_projected_chunk(
    path: &Path,
    schema: &[(String, DataType)],
    projection: &[usize],
    chunk: &ArrayRef,
    ctx: &mut ExecutionCtx,
) -> Result<Option<Vec<Vector>>> {
    let len = chunk.len();
    if len == 0 {
        return Ok(None);
    }
    let fields = chunk
        .clone()
        .execute::<StructArray>(ctx)
        .map_err(|e| corrupt(path, format!("decoding a Vortex chunk: {e}")))?;
    if fields.struct_fields().nfields() != projection.len() {
        return Err(corrupt(
            path,
            format!(
                "a projected Vortex chunk has {} fields, expected {}",
                fields.struct_fields().nfields(),
                projection.len()
            ),
        ));
    }
    let mut columns: Vec<Vector> = Vec::with_capacity(projection.len());
    for (fi, &i) in projection.iter().enumerate() {
        let (name, dtype) = &schema[i];
        let field = fields.unmasked_field(fi).clone();
        columns.push(decode_column(path, name, &field, *dtype, ctx)?);
    }
    Ok(Some(columns))
}

/// The shared all-NULL placeholder for column `column` at chunk length
/// `len`, allocated once per (column, length).
fn placeholder(
    placeholders: &mut std::collections::HashMap<(usize, usize), Arc<Vector>>,
    column: usize,
    dtype: DataType,
    len: usize,
) -> Arc<Vector> {
    Arc::clone(
        placeholders
            .entry((column, len))
            .or_insert_with(|| Arc::new(null_vector(dtype, len))),
    )
}

/// An all-NULL vector of `dtype` with `rows` rows: every validity bit clear.
/// The placeholder an unprojected column holds in a projected decode; no
/// reader ever sees its values (it exists so the table's chunking is
/// identical to a full decode's).
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
            pinkdb::types::Interval {
                months: 0,
                days: 0,
                micros: 0,
            };
            rows
        ]),
    };
    // No validity words: every row reads NULL.
    Vector::from_raw(data, Vec::new())
}

/// What a footer-only read of one data file yields: identity, schema, exact
/// row count, and the exact per-column statistics the executor can answer
/// aggregates from. No column data is read or decoded.
#[derive(Debug)]
pub(crate) struct FileFooter {
    pub(crate) db: String,
    pub(crate) table: String,
    pub(crate) rows: u64,
    pub(crate) schema: Vec<(String, DataType)>,
    pub(crate) stats: Vec<ColumnStats>,
}

/// Open one data file *footer-only*: the footer, the `pinkdb.table` metadata
/// segment and the stored statistics — no data segments, no decode.
///
/// Locally this uses positioned reads (`open_path`); over object storage (no
/// ranged reads in this build) — and for an **encrypted** lake, where the
/// container must be unsealed before anything can be parsed — the whole
/// object is read into memory through [`Storage::read`] and opened in place,
/// which still skips the decode. (Footer-range reads over unsealed segments
/// are the documented follow-up.) Runs inside the decoder guard, so a
/// malformed file is a clean [`LakeError::Corrupt`] naming the data file,
/// never an escaping panic.
///
/// Cross-file validation (the `pinkdb.table` identity matching the catalog
/// key, the row count matching the manifest, one file per table) is the
/// caller's job — this entry point only reports what the file itself says.
pub(crate) fn read_footer(
    path: &Path,
    storage: &dyn crate::storage::Storage,
    rel: &str,
) -> Result<FileFooter> {
    with_decoder_guard(path, "opening it footer-only", || {
        let (runtime, session) = session();
        let file = match storage.local_path(rel) {
            // Local: positioned reads of the postscript + footer + metadata
            // segments; the file's data segments are never touched.
            Some(local) => runtime
                .block_on(session.open_options().include_metadata().open_path(&local))
                .map_err(|e| corrupt(path, format!("not a valid Vortex file: {e}")))?,
            // Object storage: whole-object read, still no decode.
            None => {
                let bytes = storage.read(rel, &format!("loading data file \"{rel}\""))?;
                session
                    .open_options()
                    .include_metadata()
                    .open_buffer(ByteBuffer::from(bytes))
                    .map_err(|e| corrupt(path, format!("not a valid Vortex file: {e}")))?
            }
        };
        let metadata = parse_metadata(path, &file)?;
        let schema = schema_of(path, &file, &metadata)?;
        let stats = file_column_stats(path, &file, &schema)?;
        Ok(FileFooter {
            db: metadata.db,
            table: metadata.table,
            rows: file.row_count(),
            schema,
            stats,
        })
    })
}

/// The per-column statistics of a footer-opened file, gated for exactness.
fn file_column_stats(
    path: &Path,
    file: &vortex::file::VortexFile,
    schema: &[(String, DataType)],
) -> Result<Vec<ColumnStats>> {
    let Some(file_stats) = file.file_stats() else {
        return Ok(schema.iter().map(|_| ColumnStats::unknown()).collect());
    };
    let sets = file_stats.stats_sets();
    if sets.len() != schema.len() {
        return Err(corrupt(
            path,
            format!(
                "the file's statistics cover {} fields but the schema has {} columns",
                sets.len(),
                schema.len()
            ),
        ));
    }
    // `schema_of` has already checked the root is a non-nullable struct whose
    // fields match the metadata; nullability per field decides `COUNT(col)`
    // without a stored NULL count.
    let DType::Struct(fields, _) = file.dtype() else {
        return Err(corrupt(
            path,
            "Vortex data file root type is not a struct".to_string(),
        ));
    };
    let mut out = Vec::with_capacity(schema.len());
    for (i, (_name, dtype)) in schema.iter().enumerate() {
        let non_nullable = fields
            .fields()
            .nth(i)
            .is_some_and(|f| f.nullability() == Nullability::NonNullable);
        out.push(column_stats(*dtype, non_nullable, &sets[i]));
    }
    Ok(out)
}

/// One column's exact statistics, or [`ColumnStats::unknown`] for anything
/// that cannot be proved exactly.
///
/// Exactness rules (any doubt ⇒ the field stays `None` and the executor
/// materializes the table instead of answering from statistics):
///
/// - a statistic must be `Precision::Exact`: the file-level min/max of a
///   truncated VARCHAR statistic is written `Inexact`, so truncation needs no
///   separate check here;
/// - a DOUBLE column's min/max additionally needs an exact `NaNCount == 0`
///   (the engine's comparator treats NaN as equal to everything and keeps the
///   first-seen value, which a min/max statistic cannot reproduce) and a
///   nonzero statistic (`-0.0 == 0.0` in the same comparator, so the sign of
///   a first-seen zero is not recoverable);
/// - a column that cannot contain NULLs has an exact `null_count` of 0 even
///   when no NULL-count statistic was written.
fn column_stats(dtype: DataType, non_nullable: bool, set: &StatsSet) -> ColumnStats {
    let null_count = if non_nullable {
        Some(0)
    } else {
        exact_u64(set.get(Stat::NullCount))
    };
    let (min, max) = match dtype {
        DataType::Double => {
            let no_nan = exact_u64(set.get(Stat::NaNCount)) == Some(0);
            if no_nan {
                (double_stat(set, Stat::Min), double_stat(set, Stat::Max))
            } else {
                (None, None)
            }
        }
        _ => (
            scalar_stat(set, Stat::Min, dtype),
            scalar_stat(set, Stat::Max, dtype),
        ),
    };
    ColumnStats {
        null_count,
        min,
        max,
    }
}

/// The exact `u64` value of a count-like statistic.
fn exact_u64(precision: Precision<ScalarValue>) -> Option<u64> {
    match precision {
        Precision::Exact(ScalarValue::Primitive(p)) => p.as_u64(),
        _ => None,
    }
}

/// An exact non-DOUBLE min/max statistic as an engine value, in the column's
/// own type. A statistic whose shape does not match the column type (which a
/// file written by this writer cannot contain) is unusable, not an error.
fn scalar_stat(set: &StatsSet, stat: Stat, dtype: DataType) -> Option<Value> {
    let Precision::Exact(value) = set.get(stat) else {
        return None;
    };
    Some(match (dtype, value) {
        (DataType::Boolean, ScalarValue::Bool(b)) => Value::Boolean(b),
        (DataType::Integer, ScalarValue::Primitive(p)) => Value::Integer(p.as_i32()?),
        (DataType::Date, ScalarValue::Primitive(p)) => Value::Date(p.as_i32()?),
        (DataType::Bigint, ScalarValue::Primitive(p)) => Value::Bigint(p.as_i64()?),
        (DataType::Timestamp, ScalarValue::Primitive(p)) => Value::Timestamp(p.as_i64()?),
        (DataType::Varchar, ScalarValue::Utf8(s)) => Value::Varchar(s.as_str().to_string()),
        _ => return None,
    })
}

/// An exact DOUBLE min/max statistic, refusing zeros (see [`column_stats`]).
fn double_stat(set: &StatsSet, stat: Stat) -> Option<Value> {
    let Precision::Exact(ScalarValue::Primitive(p)) = set.get(stat) else {
        return None;
    };
    let x = p.as_f64()?;
    // `-0.0` is caught by the same check (`-0.0 == 0.0`), and a NaN
    // statistic must never be served (the NaN-count gate should have caught
    // it already).
    if x == 0.0 || x.is_nan() {
        return None;
    }
    Some(Value::Double(x))
}

/// The in-memory entry: one `ByteBuffer` holding the whole file.
fn decode_buffer(path: &Path, buffer: ByteBuffer) -> Result<PinkDb> {
    with_decoder_guard(path, "opening and decoding it", || {
        let (runtime, session) = session();
        let file = session
            .open_options()
            .include_metadata()
            .open_buffer(buffer)
            .map_err(|e| corrupt(path, format!("not a valid Vortex file: {e}")))?;
        decode_open_file(path, &runtime, &session, file)
    })
}

/// Run `f` (a stretch of the Vortex decoding boundary) and map any panic to a
/// clean `Corrupt` error naming the data file: `open_buffer` decodes the
/// footer + metadata segments, `parse_metadata`/`schema_of` check the pinkdb
/// schema, and the scan/stream + per-column `execute` calls decode arrays —
/// all of them have panic sites in vortex 0.88 for malformed layouts.
fn with_decoder_guard<T>(path: &Path, what: &str, f: impl FnOnce() -> Result<T>) -> Result<T> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => Err(corrupt(
            path,
            format!(
                "the data file is corrupt (decoder panic caught while {what}): {}",
                panic_message(payload.as_ref())
            ),
        )),
    }
}

/// A panic payload rendered as text (`panic!` with a string; anything else
/// gets a generic note, mirroring `std`'s own hook behavior).
fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "unknown panic payload".to_string()
}

fn decode_open_file(
    path: &Path,
    runtime: &CurrentThreadRuntime,
    session: &VortexSession,
    file: vortex::file::VortexFile,
) -> Result<PinkDb> {
    let metadata = parse_metadata(path, &file)?;
    let schema = schema_of(path, &file, &metadata)?;
    let mut mini = empty_single_table_store(&metadata.db, &metadata.table, schema.clone())?;

    let mut stream = file
        .scan()
        .map_err(|e| corrupt(path, format!("opening a Vortex scan: {e}")))?
        .into_array_stream()
        .map_err(|e| corrupt(path, format!("opening a Vortex scan: {e}")))?;
    // Read the (still encoded) chunks in file order, then decode them in
    // parallel — each worker with its own execution context — and append
    // them in file order. Decoding dominates a load; reading is cheap.
    // Stored blocks may hold several engine chunks (format_version 3): slice
    // them into `CHUNK_SIZE`-row pieces (a cheap view; decoding happens per
    // piece below).
    let mut raw: Vec<ArrayRef> = Vec::new();
    for item in runtime.block_on_stream(&mut stream) {
        let block = item.map_err(|e| corrupt(path, format!("reading a Vortex chunk: {e}")))?;
        let len = block.len();
        if len > MAX_STORED_CHUNK_ROWS {
            return Err(corrupt(
                path,
                format!(
                    "a stored chunk has {len} rows, over the {MAX_STORED_CHUNK_ROWS}-row limit"
                ),
            ));
        }
        if len <= pinkdb::CHUNK_SIZE {
            raw.push(block);
            continue;
        }
        for start in (0..len).step_by(pinkdb::CHUNK_SIZE) {
            let end = (start + pinkdb::CHUNK_SIZE).min(len);
            raw.push(
                block
                    .slice(start..end)
                    .map_err(|e| corrupt(path, format!("slicing a Vortex chunk: {e}")))?,
            );
        }
    }
    let decoded: Vec<Result<Option<Vec<Vector>>>> = raw
        .par_iter()
        .map_init(
            || session.create_execution_ctx(),
            |ctx, chunk| decode_chunk(path, &schema, chunk, ctx),
        )
        .collect();
    for columns in decoded {
        let Some(columns) = columns? else {
            continue;
        };
        let dst = mini
            .table_mut_in(&metadata.db, &metadata.table)
            .map_err(|e| LakeError::Engine {
                context: format!("loading data file \"{}\"", path.display()),
                source: e,
            })?;
        dst.append_chunk(columns).map_err(|e| LakeError::Engine {
            context: format!("loading data file \"{}\"", path.display()),
            source: e,
        })?;
    }
    Ok(mini)
}

/// Decode one stored chunk into engine columns (`None` for a 0-row chunk).
fn decode_chunk(
    path: &Path,
    schema: &[(String, DataType)],
    chunk: &ArrayRef,
    ctx: &mut ExecutionCtx,
) -> Result<Option<Vec<Vector>>> {
    let len = chunk.len();
    let fields = chunk
        .clone()
        .execute::<StructArray>(ctx)
        .map_err(|e| corrupt(path, format!("decoding a Vortex chunk: {e}")))?;
    let mut columns = Vec::with_capacity(schema.len());
    for (i, (name, dtype)) in schema.iter().enumerate() {
        let field = fields.unmasked_field(i).clone();
        columns.push(decode_column(path, name, &field, *dtype, ctx)?);
    }
    Ok((len > 0).then_some(columns))
}

/// The identity + schema metadata of a data file, or a clear corrupt-file
/// error.
fn parse_metadata(path: &Path, file: &vortex::file::VortexFile) -> Result<TableMetadata> {
    let Some(meta) = file.metadata_segment(TABLE_METADATA_KEY) else {
        return Err(corrupt(
            path,
            format!(
                "Vortex data file carries no \"{TABLE_METADATA_KEY}\" table metadata \
                 (not written by pink-lake v2)"
            ),
        ));
    };
    if meta.len() > MAX_TABLE_METADATA {
        return Err(corrupt(
            path,
            format!(
                "Vortex data file \"{TABLE_METADATA_KEY}\" metadata segment is {} bytes, \
                 over the {MAX_TABLE_METADATA}-byte limit",
                meta.len()
            ),
        ));
    }
    let metadata: TableMetadata = serde_json::from_slice(meta.as_slice()).map_err(|e| {
        corrupt(
            path,
            format!("Vortex data file has invalid \"{TABLE_METADATA_KEY}\" metadata: {e}"),
        )
    })?;
    if metadata.db.is_empty() || metadata.columns.is_empty() {
        return Err(corrupt(
            path,
            format!(
                "Vortex data file has empty \"{TABLE_METADATA_KEY}\" metadata \
                 (a data file must name its database and at least one column)"
            ),
        ));
    }
    Ok(metadata)
}

/// The pinkdb schema of a Vortex file: from the file's own metadata, with the
/// Vortex root dtype checked against it (never trusted over it).
fn schema_of(
    path: &Path,
    file: &vortex::file::VortexFile,
    metadata: &TableMetadata,
) -> Result<Vec<(String, DataType)>> {
    let DType::Struct(fields, nullability) = file.dtype() else {
        return Err(corrupt(
            path,
            format!(
                "Vortex data file root type is not a struct: {}",
                file.dtype()
            ),
        ));
    };
    if *nullability != Nullability::NonNullable {
        return Err(corrupt(
            path,
            "Vortex data file has a nullable root struct (row-level validity is \
             not a pink-lake concept)"
                .to_string(),
        ));
    }
    let names = fields.names();
    if names.len() != metadata.columns.len() {
        return Err(corrupt(
            path,
            format!(
                "Vortex data file has {} struct fields but its metadata declares {} columns",
                names.len(),
                metadata.columns.len()
            ),
        ));
    }
    let mut schema = Vec::with_capacity(metadata.columns.len());
    for (i, column) in metadata.columns.iter().enumerate() {
        let field_name = names.get(i).map(|n| n.as_ref()).unwrap_or_default();
        if field_name != column.name {
            return Err(corrupt(
                path,
                format!(
                    "Vortex data file field {i} is \"{field_name}\" but its metadata declares \
                     \"{}\"",
                    column.name
                ),
            ));
        }
        let actual_dtype = fields
            .fields()
            .nth(i)
            .ok_or_else(|| corrupt(path, format!("Vortex data file has no field {i}")))?;
        let expected = field_dtype(column.kind.to_pinkdb(), Nullability::NonNullable);
        if !same_storage_type(&expected, &actual_dtype) {
            return Err(corrupt(
                path,
                format!(
                    "Vortex data file column \"{}\" is {actual_dtype} but its metadata says \
                     {} (expected {expected})",
                    column.name,
                    column.kind.to_pinkdb().name()
                ),
            ));
        }
        schema.push((column.name.clone(), column.kind.to_pinkdb()));
    }
    Ok(schema)
}

/// Whether a data file's actual field type is the one the metadata declares
/// (nullability aside: the metadata is authoritative).
fn same_storage_type(expected: &DType, actual: &DType) -> bool {
    match (expected, actual) {
        (DType::Bool(_), DType::Bool(_)) => true,
        (DType::Primitive(a, _), DType::Primitive(b, _)) => a == b,
        (DType::Utf8(_), DType::Utf8(_)) => true,
        _ => false,
    }
}

/// The Vortex storage dtype a pinkdb column type is encoded as (inverse of
/// [`TypeTag::to_pinkdb`]). DATE and TIMESTAMP are raw storage integers; see
/// the module docs for why the native extensions are not used.
fn field_dtype(t: DataType, nullability: Nullability) -> DType {
    match t {
        DataType::Boolean => DType::Bool(nullability),
        DataType::Integer | DataType::Date => DType::Primitive(PType::I32, nullability),
        DataType::Bigint | DataType::Timestamp => DType::Primitive(PType::I64, nullability),
        DataType::Double => DType::Primitive(PType::F64, nullability),
        DataType::Varchar => DType::Utf8(nullability),
        // Refused at the encode entry: there is no Vortex mapping yet.
        DataType::Decimal { .. } | DataType::Interval => {
            unreachable!("DECIMAL/INTERVAL are refused at the Vortex encode entry")
        }
    }
}

/// The root struct dtype of every data file is deliberately always
/// `NonNullable`: pinkdb has no row-level validity, and the Vortex layout
/// writer refuses/`panic!`s on nullable top-level structs. The loader rejects
/// a nullable root the same way (`schema_of`), so this invariant must hold on
/// both sides; `root_struct_is_always_non_nullable` pins it.
fn struct_dtype(schema: &[(String, DataType)], nulls: &[Nullability]) -> DType {
    let fields = schema
        .iter()
        .zip(nulls)
        .map(|((name, t), n)| (name.as_str(), field_dtype(*t, *n)))
        .collect::<StructFields>();
    DType::Struct(fields, Nullability::NonNullable)
}

fn field_names(schema: &[(String, DataType)]) -> FieldNames {
    schema
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<FieldNames>()
}

/// One pass over the chunks of `src`: the table-level nullability of every
/// column (all chunks of a table must share one dtype, so one nullable chunk
/// makes the column nullable everywhere) plus, per chunk, whether each column
/// actually contains a NULL there (the flag [`chunk_validity`] needs).
fn column_nullability(
    src: &DataTable,
    schema: &[(String, DataType)],
    dbname: &str,
    table: &str,
) -> Result<(Vec<Nullability>, Vec<Vec<bool>>)> {
    let mut nulls = vec![Nullability::NonNullable; schema.len()];
    let mut chunk_nulls = Vec::with_capacity(src.num_chunks());
    for i in 0..src.num_chunks() {
        let chunk = src.scan_chunk(i).ok_or_else(|| LakeError::Internal {
            message: format!("chunk {i} of \"{dbname}.{table}\" disappeared while encoding"),
        })?;
        let mut flags = Vec::with_capacity(schema.len());
        for (c, table_null) in nulls.iter_mut().enumerate() {
            let column = chunk.columns.get(c).ok_or_else(|| LakeError::Internal {
                message: format!("column {c} of \"{dbname}.{table}\" disappeared while encoding"),
            })?;
            let any_null = column.has_nulls();
            if any_null {
                *table_null = Nullability::Nullable;
            }
            flags.push(any_null);
        }
        chunk_nulls.push(flags);
    }
    Ok((nulls, chunk_nulls))
}

fn encode_chunk(
    chunk: &DataChunk,
    schema: &[(String, DataType)],
    nulls: &[Nullability],
    any_null: &[bool],
    dbname: &str,
    table: &str,
) -> Result<ArrayRef> {
    let mut children = Vec::with_capacity(chunk.columns.len());
    for (i, column) in chunk.columns.iter().enumerate() {
        children.push(encode_column(column, schema[i].1, nulls[i], any_null[i])?);
    }
    StructArray::try_new(
        field_names(schema),
        children,
        chunk.num_rows,
        Validity::NonNullable,
    )
    .map(|a| a.into_array())
    .map_err(|e| vortex_error(&format!("encoding a chunk of \"{dbname}.{table}\""), e))
}

/// The validity for one column of one chunk, matching the column-level
/// `nullability` exactly (all chunks of a table must agree on the dtype):
/// a nullable column whose chunk has no NULLs (`any_null == false`) stays
/// nullable and uses `Validity::AllValid`.
fn chunk_validity(column: &Vector, nullability: Nullability, any_null: bool) -> Validity {
    match nullability {
        Nullability::NonNullable => Validity::NonNullable,
        Nullability::Nullable => {
            if !any_null {
                return Validity::AllValid;
            }
            Validity::from(bits_from_words(column.validity_words(), column.len()))
        }
    }
}

/// A Vortex bit buffer over the first `len` bits of little-endian validity
/// words (bit `i` of word `i / 64` ⇒ LSB-first bit `i` of the byte stream —
/// exactly Arrow/Vortex bit order), built word-wise instead of per row.
fn bits_from_words(words: &[u64], len: usize) -> BitBuffer {
    let mut bytes = Vec::with_capacity(words.len() * 8);
    for w in words {
        bytes.extend_from_slice(&w.to_le_bytes());
    }
    bytes.truncate(len.div_ceil(8));
    // Bits past `len` in the last byte are outside the view; clear them so
    // the buffer is canonical regardless of the engine's spare bits.
    if !len.is_multiple_of(8)
        && let Some(last) = bytes.last_mut()
    {
        *last &= (1u8 << (len % 8)) - 1;
    }
    BitBuffer::new(ByteBuffer::from(bytes), len)
}

/// Encode one engine column chunk as a canonical Vortex array.
fn encode_column(
    column: &Vector,
    dtype: DataType,
    nullability: Nullability,
    any_null: bool,
) -> Result<ArrayRef> {
    let validity = chunk_validity(column, nullability, any_null);
    let array: ArrayRef = match (dtype, column.data()) {
        (DataType::Boolean, VectorData::Boolean(values)) => {
            BoolArray::new(BitBuffer::from_iter(values.iter().copied()), validity).into_array()
        }
        (DataType::Integer, VectorData::Int32(values))
        | (DataType::Date, VectorData::Date32(values)) => {
            PrimitiveArray::new(Buffer::copy_from(values), validity).into_array()
        }
        (DataType::Bigint, VectorData::Int64(values))
        | (DataType::Timestamp, VectorData::TimestampMicros(values)) => {
            PrimitiveArray::new(Buffer::copy_from(values), validity).into_array()
        }
        (DataType::Double, VectorData::Float64(values)) => {
            // `Buffer<f64>` keeps the raw IEEE-754 bits: NaN payloads and
            // `-0.0` round-trip bit-exactly (verified by the tests).
            PrimitiveArray::new(Buffer::copy_from(values), validity).into_array()
        }
        (DataType::Varchar, VectorData::VarChar(values)) => {
            // NULL slots must not leak the placeholder/stale bytes they hold
            // in the engine vector into the file: collect `""` for them.
            match nullability {
                Nullability::NonNullable => {
                    VarBinViewArray::from_iter_str((0..column.len()).map(|i| values[i].as_ref()))
                        .into_array()
                }
                Nullability::Nullable => VarBinViewArray::from_iter_nullable_str(
                    (0..column.len()).map(|i| column.is_valid(i).then(|| values[i].as_ref())),
                )
                .into_array(),
            }
        }
        (dtype, _data) => {
            return Err(LakeError::Internal {
                message: format!("column dtype {dtype:?} does not match its data layout"),
            });
        }
    };
    Ok(array)
}

/// Decode one canonical Vortex column into an engine vector of `len` rows,
/// in bulk: payloads are copied whole and validity is built word-wise, then
/// NULL slots get the engine's placeholder (`0`, `false`, `""`) — exactly
/// what a row-by-row `push_null` would have stored, so loaded vectors are
/// bit-identical to the old per-row decode.
fn decode_column(
    path: &Path,
    name: &str,
    array: &ArrayRef,
    dtype: DataType,
    ctx: &mut ExecutionCtx,
) -> Result<Vector> {
    let len = array.len();
    if len > pinkdb::CHUNK_SIZE {
        return Err(corrupt(
            path,
            format!(
                "column \"{name}\" chunk has {len} rows, over the engine's CHUNK_SIZE {}",
                pinkdb::CHUNK_SIZE
            ),
        ));
    }
    let mask = if array.dtype().nullability() == Nullability::Nullable {
        Some(
            array
                .validity()
                .map_err(|e| corrupt(path, format!("reading validity of column \"{name}\": {e}")))?
                .execute_mask(len, ctx)
                .map_err(|e| {
                    corrupt(path, format!("reading validity of column \"{name}\": {e}"))
                })?,
        )
    } else {
        None
    };
    // Validity words (bit set ⇒ valid) and whether any row is NULL.
    let mut words = vec![u64::MAX; len.div_ceil(64)];
    let mut any_null = false;
    if let Some(m) = &mask {
        for i in 0..len {
            if !m.value(i) {
                words[i / 64] &= !(1u64 << (i % 64));
                any_null = true;
            }
        }
    }
    let valid = |i: usize| words[i / 64] >> (i % 64) & 1 == 1;

    let data = match dtype {
        DataType::Boolean => {
            let values = bool_values(path, name, array, ctx)?;
            VectorData::Boolean((0..len).map(|i| valid(i) && values.value(i)).collect())
        }
        DataType::Varchar => {
            let values = var_bin_values(path, name, array, ctx)?;
            let views = values.views();
            let mut out: Vec<std::sync::Arc<str>> = Vec::with_capacity(len);
            // Repeated strings — dictionary-encoded columns, low-cardinality
            // fields — are allocated once per chunk instead of once per row:
            // the 16-byte Vortex view is an exact key (two identical views
            // always describe identical bytes), and a fixed direct-mapped
            // cache of recently decoded values is enough for the repeats a
            // chunk actually holds. The cache never grows and never
            // allocates for itself; a high-cardinality column pays one
            // multiply, one probe and one compare of cache overhead per row
            // (plus the allocation it had to do anyway) and nothing more.
            let mut cache: [Option<(u128, std::sync::Arc<str>)>; INTERN_SLOTS] =
                std::array::from_fn(|_| None);
            let empty: std::sync::Arc<str> = std::sync::Arc::from("");
            for (i, view) in views.iter().enumerate() {
                if !valid(i) {
                    // One shared placeholder, exactly like `push_null`.
                    out.push(std::sync::Arc::clone(&empty));
                    continue;
                }
                let key = view.as_u128();
                let slot = intern_slot(key);
                if let Some((k, s)) = &cache[slot]
                    && *k == key
                {
                    out.push(std::sync::Arc::clone(s));
                    continue;
                }
                let bytes = values.bytes_at(i);
                let s = std::str::from_utf8(bytes.as_slice()).map_err(|e| {
                    corrupt(
                        path,
                        format!("column \"{name}\" row {i} is not valid UTF-8: {e}"),
                    )
                })?;
                let arc: std::sync::Arc<str> = std::sync::Arc::from(s);
                cache[slot] = Some((key, std::sync::Arc::clone(&arc)));
                out.push(arc);
            }
            VectorData::VarChar(out)
        }
        DataType::Integer | DataType::Date => {
            let values = primitive(path, name, array, ctx)?;
            let mut v = values.to_buffer::<i32>().as_slice().to_vec();
            if v.len() != len {
                return Err(corrupt(
                    path,
                    format!("column \"{name}\" payload length mismatch"),
                ));
            }
            if any_null {
                for (i, x) in v.iter_mut().enumerate() {
                    if !valid(i) {
                        *x = 0;
                    }
                }
            }
            if dtype == DataType::Date {
                VectorData::Date32(v)
            } else {
                VectorData::Int32(v)
            }
        }
        DataType::Bigint | DataType::Timestamp => {
            let values = primitive(path, name, array, ctx)?;
            let mut v = values.to_buffer::<i64>().as_slice().to_vec();
            if v.len() != len {
                return Err(corrupt(
                    path,
                    format!("column \"{name}\" payload length mismatch"),
                ));
            }
            if any_null {
                for (i, x) in v.iter_mut().enumerate() {
                    if !valid(i) {
                        *x = 0;
                    }
                }
            }
            if dtype == DataType::Timestamp {
                VectorData::TimestampMicros(v)
            } else {
                VectorData::Int64(v)
            }
        }
        DataType::Double => {
            let values = primitive(path, name, array, ctx)?;
            let mut v = values.to_buffer::<f64>().as_slice().to_vec();
            if v.len() != len {
                return Err(corrupt(
                    path,
                    format!("column \"{name}\" payload length mismatch"),
                ));
            }
            if any_null {
                for (i, x) in v.iter_mut().enumerate() {
                    if !valid(i) {
                        *x = 0.0;
                    }
                }
            }
            VectorData::Float64(v)
        }
        // Never written: the encode entry refuses these types, so no file
        // this build reads carries them.
        DataType::Decimal { .. } | DataType::Interval => {
            unreachable!("DECIMAL/INTERVAL are refused at the Vortex encode entry")
        }
    };
    Ok(Vector::from_raw(data, words))
}

fn primitive(
    path: &Path,
    name: &str,
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
) -> Result<PrimitiveArray> {
    array
        .clone()
        .execute::<PrimitiveArray>(ctx)
        .map_err(|e| corrupt(path, format!("decoding column \"{name}\": {e}")))
}

fn bool_values(
    path: &Path,
    name: &str,
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
) -> Result<BitBuffer> {
    let values = array
        .clone()
        .execute::<BoolArray>(ctx)
        .map_err(|e| corrupt(path, format!("decoding column \"{name}\": {e}")))?;
    Ok(BoolArrayExt::to_bit_buffer(&values))
}

fn var_bin_values(
    path: &Path,
    name: &str,
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
) -> Result<VarBinViewArray> {
    array
        .clone()
        .execute::<VarBinViewArray>(ctx)
        .map_err(|e| corrupt(path, format!("decoding column \"{name}\": {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pinkdb::{CHUNK_SIZE, PinkDb, Value};

    /// Fresh-temp-dir counter for the footer tests.
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    use std::sync::atomic::Ordering;

    /// A 40-row table with all seven engine types (the review's bit-flip
    /// victim shape, rebuilt deterministically).
    fn seven_type_victim() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute(
            "CREATE TABLE t (b BOOLEAN, i INTEGER, g BIGINT, d DOUBLE, v VARCHAR, \
             dt DATE, ts TIMESTAMP);",
        )
        .unwrap();
        let t = db.table_mut("t").unwrap();
        for i in 0..40i64 {
            if i % 7 == 3 {
                t.append_row(vec![Value::Null; 7]).unwrap();
                continue;
            }
            t.append_row(vec![
                Value::Boolean(i % 2 == 0),
                Value::Integer((i * 37 - 500) as i32),
                Value::Bigint(i * 1_000_003 - 7),
                Value::Double(i as f64 / 7.0 - 2.5),
                Value::Varchar(format!("row-{i}")),
                Value::Date((i - 20) as i32),
                Value::Timestamp(i * 86_400_000_000 - 1),
            ])
            .unwrap();
        }
        db
    }

    /// A 40-row string-heavy table: `VarBinViewArray` inline views are where
    /// the decoder's debug-only assertions fire.
    fn string_victim() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (k INTEGER, s1 VARCHAR, s2 VARCHAR, s3 VARCHAR);")
            .unwrap();
        let t = db.table_mut("t").unwrap();
        for i in 0..40i64 {
            let s1 = if i % 5 == 0 {
                Value::Null
            } else {
                Value::Varchar(format!("alpha-{i}-zz"))
            };
            let s2 = Value::Varchar("x".repeat((i as usize * 7) % 40));
            let s3 = if i % 11 == 0 {
                Value::Varchar(format!(
                    "héllo wörld 🌍 {i} with a longer tail {}",
                    "ab".repeat(i as usize % 9)
                ))
            } else {
                Value::Varchar((i % 3).to_string())
            };
            t.append_row(vec![Value::Integer(i as i32), s1, s2, s3])
                .unwrap();
        }
        db
    }

    /// A tiny two-column table for the crafted-file tests (one NULL row, so
    /// both a non-nullable and a nullable column shape are covered).
    fn tiny_table() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, s VARCHAR);")
            .unwrap();
        {
            let t = db.table_mut("t").unwrap();
            t.append_row(vec![Value::Integer(1), Value::Varchar("x".into())])
                .unwrap();
            t.append_row(vec![Value::Null, Value::Null]).unwrap();
        }
        db
    }

    fn victim_bytes(db: &PinkDb) -> Vec<u8> {
        encode_table("main", "t", db.table("t").unwrap()).unwrap()
    }

    /// Encode `src` into memory with an explicit metadata segment and runtime.
    fn encode_with(src: &DataTable, metadata: Option<Vec<u8>>, parallel: bool) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        encode_table_with_metadata("main", "t", src, metadata, &mut bytes, parallel)?;
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<PinkDb> {
        decode_table(Path::new("crafted.vortex"), bytes)
    }

    fn decode_with_metadata(src: &DataTable, metadata: &[u8]) -> Result<PinkDb> {
        let bytes = encode_with(src, Some(metadata.to_vec()), true).unwrap();
        decode(&bytes)
    }

    fn metadata_json(db: &str, table: &str, columns: &str) -> Vec<u8> {
        format!(r#"{{"db":"{db}","table":"{table}","columns":[{columns}]}}"#).into_bytes()
    }

    fn splitmix64(mut z: u64) -> u64 {
        z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// The writer must never emit a nullable top-level struct: pinkdb has no
    /// row-level validity and the Vortex layout writer has a panic site for
    /// nullable top-level structs. The loader rejects one too, so this
    /// invariant is pinned here (minor review point).
    #[test]
    fn root_struct_is_always_non_nullable() {
        for db in [seven_type_victim(), string_victim()] {
            let bytes = victim_bytes(&db);
            let (runtime, session) = session();
            drop(runtime);
            let file = session
                .open_options()
                .open_buffer(ByteBuffer::copy_from(bytes))
                .unwrap();
            let DType::Struct(fields, nullability) = file.dtype() else {
                panic!("root dtype is not a struct");
            };
            assert_eq!(*nullability, Nullability::NonNullable);
            assert!(
                fields
                    .fields()
                    .any(|f| f.nullability() == Nullability::Nullable),
                "the fixture should contain nullable columns"
            );
        }
    }

    /// Parallel writing (the worker-pool runtime) must be byte-identical to
    /// single-threaded writing: compression *scheduling* may not change the
    /// encoding, or every lake file made on one machine would differ from the
    /// same data saved on another. Runs the writer both ways over a table
    /// wide enough to span several chunks and several compressions.
    #[test]
    fn parallel_writing_emits_identical_bytes() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (i INTEGER, g BIGINT, d DOUBLE, s VARCHAR, b BOOLEAN);")
            .unwrap();
        let t = db.table_mut("t").unwrap();
        for i in 0..5_000i64 {
            // A mix of shapes (runs, repeats, nulls, long strings) so the
            // compressor has real work and real decisions to make.
            t.append_row(vec![
                Value::Integer((i % 1000) as i32),
                Value::Bigint(if i % 97 == 0 { 0 } else { i * 2654435761 }),
                Value::Double(if i % 41 == 0 { 0.0 } else { i as f64 / 3.0 }),
                Value::Varchar(if i % 13 == 0 {
                    "x".repeat((i % 40) as usize)
                } else {
                    format!("row-{}-{}", i % 250, i % 7)
                }),
                Value::Boolean(i % 2 == 0),
            ])
            .unwrap();
        }
        let src = db.table("t").unwrap();
        assert!(src.num_chunks() >= 3, "fixture should span several chunks");
        let meta = table_metadata("main", "t", src).unwrap();
        let serial = encode_with(src, Some(meta.clone()), false).unwrap();
        let parallel = encode_with(src, Some(meta.clone()), true).unwrap();
        let parallel_again = encode_with(src, Some(meta), true).unwrap();
        assert_eq!(
            serial,
            parallel,
            "a worker-pool write changed the file bytes ({} bytes vs {})",
            serial.len(),
            parallel.len()
        );
        assert_eq!(
            parallel_again, parallel,
            "parallel writes must be deterministic"
        );
        // And the bytes still decode to the same table (all rows/chunks).
        let out = decode(&serial).unwrap();
        assert_eq!(out.table("t").unwrap().num_rows(), 5000);
    }

    /// The whole decode boundary is guarded: flipping a byte at an offset the
    /// pre-guard decoder panicked on must yield a clean `Corrupt` error that
    /// names the data file and says the panic was caught.
    #[test]
    fn decoder_panics_are_caught_and_named() {
        // Offsets found by sweeping the exact writer output of these
        // fixtures with vortex 0.88. The ±32-byte window keeps the pin alive
        // across small layout shifts; "no panic found" means the Vortex
        // writer layout changed and this pin must be revisited. The string
        // victim's decoder panics are `#[cfg(debug_assertions)]` assertions,
        // so in a release build that pin may simply not panic — the
        // seven-type victim's release-visible panic must be caught in both
        // profiles.
        for (name, db, pinned, required) in [
            ("seven_type_40", seven_type_victim(), 74usize, true),
            (
                "string_heavy_40",
                string_victim(),
                128usize,
                cfg!(debug_assertions),
            ),
        ] {
            let bytes = victim_bytes(&db);
            let mut found = None;
            let last = (pinned + 32).min(bytes.len() - 1);
            for pos in pinned.saturating_sub(32)..=last {
                for bit in [7u32, 0u32] {
                    let mut b = bytes.clone();
                    b[pos] ^= 1 << bit;
                    if let Err(e) = decode_table(Path::new("victim.vortex"), &b)
                        && e.to_string().contains("decoder panic caught")
                    {
                        found = Some((pos, bit, e));
                        break;
                    }
                }
                if found.is_some() {
                    break;
                }
            }
            let Some((pos, bit, err)) = found else {
                assert!(
                    !required,
                    "{name}: no caught decoder panic near pinned offset {pinned}; the Vortex \
                     writer layout changed — re-run the bit-flip sweep and re-pin"
                );
                continue;
            };
            let msg = err.to_string();
            assert!(
                msg.contains("corrupt lake metadata"),
                "{name} @{pos} bit {bit}: {msg}"
            );
            assert!(
                msg.contains("victim.vortex"),
                "{name} @{pos} bit {bit}: {msg}"
            );
            assert!(
                matches!(err, LakeError::Corrupt { .. }),
                "{name} @{pos} bit {bit}: {err:?}"
            );
        }
    }

    /// The committed sweep: fixed seed, ~2,500 single-bit flips over two
    /// victim shapes. A panic escaping the decoder guard fails the test
    /// (debug builds also catch the decoder's `debug_assert!` sites). The
    /// sweep is dense enough to hit known panic offsets in both profiles.
    #[test]
    fn bit_flip_sweep_never_panics() {
        const FLIPS_PER_VICTIM: usize = 1250;
        // Offsets that panicked the pre-guard decoder: the seven-type
        // `DType bool? is not a primitive type` panic (release too) and two
        // debug-only `VarBinViewArray` assertions.
        let mut total_caught = 0usize;
        for (name, db, pinned) in [
            ("seven_type_40", seven_type_victim(), &[74usize, 1984][..]),
            ("string_heavy_40", string_victim(), &[128usize][..]),
        ] {
            let bytes = victim_bytes(&db);
            let mut flips: Vec<(usize, u32)> = pinned.iter().map(|&pos| (pos, 7u32)).collect();
            for k in 0..FLIPS_PER_VICTIM {
                let r = splitmix64(0x5EED_5EED_0000_0000 ^ k as u64);
                flips.push(((r % bytes.len() as u64) as usize, ((r >> 32) % 8) as u32));
            }
            let mut errors = 0usize;
            let mut caught = 0usize;
            for (pos, bit) in flips {
                let mut b = bytes.clone();
                b[pos] ^= 1 << bit;
                if let Err(e) = decode(&b) {
                    errors += 1;
                    if e.to_string().contains("decoder panic caught") {
                        caught += 1;
                    }
                }
            }
            assert!(errors > 0, "{name}: no flip produced an error");
            assert!(
                errors < bytes.len(),
                "{name}: every flip errored — the fixture is not a valid file"
            );
            total_caught += caught;
            if cfg!(debug_assertions) {
                assert!(
                    caught > 0,
                    "{name}: no flip exercised the caught-panic path (debug build should hit \
                     the decoder's assertions; re-pin if the Vortex writer layout changed)"
                );
            }
        }
        assert!(
            total_caught > 0,
            "no flip in the whole sweep exercised the caught-panic path; if vortex fixed these \
             panics, retire or re-pin the targeted decoder test"
        );
    }

    #[test]
    fn missing_table_metadata_is_corrupt() {
        let db = tiny_table();
        let bytes = encode_with(db.table("t").unwrap(), None, true).unwrap();
        let err = decode(&bytes).unwrap_err().to_string();
        assert!(err.contains("carries no \"pinkdb.table\""), "{err}");
        assert!(err.contains("crafted.vortex"), "{err}");
    }

    #[test]
    fn invalid_table_metadata_is_corrupt() {
        let db = tiny_table();
        for bad in [
            &b"not json"[..],
            b"[1,2,3]",
            b"42",
            b"null",
            b"{\"db\":\"main\"}",
        ] {
            let err = decode_with_metadata(db.table("t").unwrap(), bad)
                .unwrap_err()
                .to_string();
            assert!(err.contains("invalid \"pinkdb.table\" metadata"), "{err}");
            assert!(err.contains("crafted.vortex"), "{err}");
        }
    }

    #[test]
    fn unknown_type_tag_is_corrupt() {
        let db = tiny_table();
        let meta = metadata_json(
            "main",
            "t",
            r#"{"name":"a","type":"float"},{"name":"s","type":"varchar"}"#,
        );
        let err = decode_with_metadata(db.table("t").unwrap(), &meta)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown variant"), "{err}");
    }

    #[test]
    fn empty_identity_or_zero_columns_metadata_is_corrupt() {
        let db = tiny_table();
        for meta in [
            metadata_json(
                "",
                "t",
                r#"{"name":"a","type":"integer"},{"name":"s","type":"varchar"}"#,
            ),
            metadata_json("main", "t", ""),
        ] {
            let err = decode_with_metadata(db.table("t").unwrap(), &meta)
                .unwrap_err()
                .to_string();
            assert!(err.contains("empty \"pinkdb.table\" metadata"), "{err}");
        }
    }

    #[test]
    fn field_name_count_and_dtype_mismatches_are_corrupt() {
        let db = tiny_table();
        let src = db.table("t").unwrap();
        let cases = [
            // A renamed field.
            (
                metadata_json(
                    "main",
                    "t",
                    r#"{"name":"x","type":"integer"},{"name":"s","type":"varchar"}"#,
                ),
                "field 0 is \"a\"",
            ),
            // One extra declared column.
            (
                metadata_json(
                    "main",
                    "t",
                    r#"{"name":"a","type":"integer"},{"name":"s","type":"varchar"},{"name":"z","type":"double"}"#,
                ),
                "but its metadata declares 3 columns",
            ),
            // A column type that disagrees with the Vortex storage.
            (
                metadata_json(
                    "main",
                    "t",
                    r#"{"name":"a","type":"bigint"},{"name":"s","type":"varchar"}"#,
                ),
                "metadata says BIGINT",
            ),
        ];
        for (meta, needle) in cases {
            let err = decode_with_metadata(src, &meta).unwrap_err().to_string();
            assert!(err.contains(needle), "{needle}: {err}");
        }
    }

    #[test]
    fn oversized_metadata_segment_is_corrupt() {
        let db = tiny_table();
        let meta = vec![b'x'; MAX_TABLE_METADATA + 1];
        let err = decode_with_metadata(db.table("t").unwrap(), &meta)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("over the 1048576-byte limit"),
            "metadata segments are capped before parsing: {err}"
        );
    }

    #[test]
    fn chunk_over_chunk_size_is_corrupt() {
        let (_runtime, session) = session();
        let mut ctx = session.create_execution_ctx();
        let values = vec![7i32; CHUNK_SIZE + 1];
        let array: ArrayRef =
            PrimitiveArray::new(Buffer::copy_from(&values), Validity::NonNullable).into_array();
        let err = decode_column(
            Path::new("crafted.vortex"),
            "a",
            &array,
            DataType::Integer,
            &mut ctx,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("over the engine's CHUNK_SIZE"), "{err}");
    }

    /// Write `bytes` as `rel` under a fresh temp dir and read its footer
    /// through the real local storage path (`open_path`, positioned reads).
    fn try_footer_of(tag: &str, bytes: Vec<u8>) -> Result<FileFooter> {
        let dir = std::env::temp_dir().join(format!(
            "pink_lake_footer_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let rel = "data/t/x.vortex";
        std::fs::create_dir_all(dir.join("data/t")).unwrap();
        std::fs::write(dir.join(rel), bytes).unwrap();
        let storage = crate::local::LocalStorage::new(&dir);
        let out = read_footer(&dir.join(rel), &storage, rel);
        std::fs::remove_dir_all(&dir).ok();
        out
    }

    fn footer_of(tag: &str, bytes: Vec<u8>) -> FileFooter {
        try_footer_of(tag, bytes).unwrap_or_else(|e| panic!("read_footer failed: {e}"))
    }

    #[test]
    fn footer_stats_refuse_nan_and_zero_doubles() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (d DOUBLE, z DOUBLE, ok DOUBLE);")
            .unwrap();
        let t = db.table_mut("t").unwrap();
        // Row 0: -0.0 and NaN; row 1: 0.0 and 1.0; row 2: 2.5 and 1.0.
        for (d, z, ok) in [(-0.0, f64::NAN, 5.5), (0.0, 1.0, -1.5), (2.5, 1.0, 0.0)] {
            t.append_row(vec![Value::Double(d), Value::Double(z), Value::Double(ok)])
                .unwrap();
        }
        let footer = footer_of("doubles", victim_bytes(&db));
        // A zero *statistic* is ambiguous (`-0.0 == 0.0`, first-seen wins in
        // the engine): the min statistic here is a zero (really `-0.0`), so
        // it is not served — but the column's nonzero maximum is exact and
        // is.
        assert_eq!(footer.stats[0].min, None);
        assert_eq!(footer.stats[0].max, Some(Value::Double(2.5)));
        assert_eq!(footer.stats[0].null_count, Some(0));
        // A column with any NaN proves nothing about MIN/MAX.
        assert_eq!(footer.stats[1].min, None);
        assert_eq!(footer.stats[1].max, None);
        // Nonzero, NaN-free statistics are exact — even though the column
        // contains a zero (which only matters when it *is* the extreme).
        assert_eq!(footer.stats[2].min, Some(Value::Double(-1.5)));
        assert_eq!(footer.stats[2].max, Some(Value::Double(5.5)));
        assert_eq!(footer.stats[2].null_count, Some(0));
    }

    #[test]
    fn footer_stats_of_empty_and_all_null_columns() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, n INTEGER);")
            .unwrap();
        let footer = footer_of("empty", victim_bytes(&db));
        assert_eq!(footer.rows, 0);
        // No rows: every statistic is absent (the writer drops all-null stat
        // columns); the executor derives NaN-free NULL/0 answers from `rows`.
        assert_eq!(footer.stats[0].min, None);
        assert_eq!(footer.stats[0].max, None);
        assert_eq!(footer.stats[0].null_count, Some(0)); // non-nullable
        assert_eq!(footer.stats[1].null_count, Some(0)); // non-nullable too

        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER, n INTEGER);")
            .unwrap();
        let t = db.table_mut("t").unwrap();
        for _ in 0..3 {
            t.append_row(vec![Value::Integer(1), Value::Null]).unwrap();
        }
        let footer = footer_of("all_null", victim_bytes(&db));
        // The nullable column is all NULL: an exact NULL count, no min/max.
        assert_eq!(footer.stats[1].null_count, Some(3));
        assert_eq!(footer.stats[1].min, None);
        assert_eq!(footer.stats[1].max, None);
        // The non-nullable column has exact statistics.
        assert_eq!(footer.stats[0].null_count, Some(0));
        assert_eq!(footer.stats[0].min, Some(Value::Integer(1)));
        assert_eq!(footer.stats[0].max, Some(Value::Integer(1)));
    }

    #[test]
    fn footer_stats_refuse_truncated_strings_but_serve_short_ones() {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (s VARCHAR, l VARCHAR);")
            .unwrap();
        let t = db.table_mut("t").unwrap();
        for i in 0..8i64 {
            t.append_row(vec![
                Value::Varchar(format!("key-{}", i % 3)),
                // Long enough that the writer truncates the max statistic.
                Value::Varchar(format!("long-{i}-{}", "x".repeat(120))),
            ])
            .unwrap();
        }
        let footer = footer_of("strings", victim_bytes(&db));
        // Short strings are exact on both ends.
        assert_eq!(footer.stats[0].min, Some(Value::Varchar("key-0".into())));
        assert_eq!(footer.stats[0].max, Some(Value::Varchar("key-2".into())));
        // Every long string exceeds the truncation length, so both bounds
        // are inexact — never served as exact answers.
        assert_eq!(footer.stats[1].min, None);
        assert_eq!(footer.stats[1].max, None);
        assert_eq!(footer.stats[1].null_count, Some(0));
    }

    /// Every served statistic equals the decoded min/max under the engine's
    /// own comparator, for a fixture with all types.
    #[test]
    fn footer_stats_match_decoded_min_max() {
        let db = seven_type_victim();
        let footer = footer_of("seven", victim_bytes(&db));
        let decoded = decode(&victim_bytes(&db)).unwrap();
        let t = decoded.table("t").unwrap();
        assert_eq!(footer.rows as usize, t.num_rows());
        assert_eq!(footer.schema, t.schema());
        for (i, (name, _)) in footer.schema.iter().enumerate() {
            let mut nulls = 0u64;
            let (mut min, mut max): (Option<Value>, Option<Value>) = (None, None);
            for row in 0..t.num_rows() {
                let v = t.get(row, i).unwrap();
                if v.is_null() {
                    nulls += 1;
                    continue;
                }
                if min
                    .as_ref()
                    .is_none_or(|m| pinkdb::plan::compare_values(&v, m).is_lt())
                {
                    min = Some(v.clone());
                }
                if max
                    .as_ref()
                    .is_none_or(|m| pinkdb::plan::compare_values(&v, m).is_gt())
                {
                    max = Some(v);
                }
            }
            assert_eq!(footer.stats[i].null_count, Some(nulls), "{name}");
            if let Some(stat) = &footer.stats[i].min {
                assert_eq!(Some(stat), min.as_ref(), "{name} min");
            }
            if let Some(stat) = &footer.stats[i].max {
                assert_eq!(Some(stat), max.as_ref(), "{name} max");
            }
        }
    }

    #[test]
    fn footer_read_of_garbage_is_corrupt() {
        let err = try_footer_of("garbage", b"not a vortex file at all".to_vec())
            .unwrap_err()
            .to_string();
        assert!(err.contains("not a valid Vortex file"), "{err}");
    }

    /// A byte flip anywhere in the footer region must yield `Ok` or a clean
    /// error — never an escaping panic (the footer read runs inside the same
    /// decoder guard as the full decode).
    #[test]
    fn footer_read_never_panics_on_flipped_bytes() {
        let db = seven_type_victim();
        let bytes = victim_bytes(&db);
        let mut flips = 0;
        for offset in bytes.len().saturating_sub(256)..bytes.len() {
            let mut copy = bytes.clone();
            copy[offset] ^= 0x01;
            if let Ok(footer) = try_footer_of("flip", copy) {
                // A flip that still parses must not claim an absurd row
                // count or a schema that disagrees with the metadata.
                assert_eq!(footer.schema.len(), 7);
            }
            flips += 1;
        }
        assert!(flips > 0);
    }

    #[test]
    fn non_struct_root_is_corrupt() {
        let (runtime, session) = session();
        let values = vec![1i32, 2, 3];
        let array: ArrayRef =
            PrimitiveArray::new(Buffer::copy_from(&values), Validity::NonNullable).into_array();
        let metadata = metadata_json("main", "t", r#"{"name":"a","type":"integer"}"#);
        let mut bytes = Vec::new();
        session
            .write_options()
            .with_metadata_segment(TABLE_METADATA_KEY, ByteBuffer::copy_from(metadata))
            .blocking(&runtime)
            .write(&mut bytes, array.to_array_iterator())
            .unwrap();
        let err = decode(&bytes).unwrap_err().to_string();
        assert!(err.contains("root type is not a struct"), "{err}");
    }
}
