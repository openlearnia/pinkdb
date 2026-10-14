//! Vortex data files: one Vortex file per table (the v2 lake default).
//!
//! A v2 data file is a plain Vortex file holding one struct array (one field
//! per table column, in schema order) with the engine's chunk boundaries
//! preserved: the writer feeds one Vortex stream element per engine chunk and
//! fixes the row block size to [`pinkdb::CHUNK_SIZE`], so loading reproduces
//! the same chunking and a loaded table re-saves to the same `.pink` bytes.
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
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;

use pinkdb::{DataChunk, DataTable, DataType, PinkDb, Vector, VectorData};
use serde::{Deserialize, Serialize};
use vortex::VortexSessionDefault;
use vortex::array::arrays::bool::BoolArrayExt;
use vortex::array::arrays::struct_::StructArrayExt;
use vortex::array::arrays::{
    BoolArray, ChunkedArray, PrimitiveArray, StructArray, VarBinViewArray,
};
use vortex::array::validity::Validity;
use vortex::array::{ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute};
use vortex::buffer::{BitBuffer, Buffer, ByteBuffer};
use vortex::dtype::{DType, FieldNames, Nullability, PType, StructFields};
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex::io::runtime::BlockingRuntime;
use vortex::io::runtime::current::{CurrentThreadRuntime, CurrentThreadWorkerPool};
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

use crate::{empty_single_table_store, LakeError, Result};

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

/// Encode one table as a Vortex file (with its `pinkdb.table` segment) and
/// return its bytes.
pub(crate) fn encode_table(dbname: &str, table: &str, src: &DataTable) -> Result<Vec<u8>> {
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
    let metadata = serde_json::to_vec(&metadata).map_err(|e| LakeError::Internal {
        message: format!("serializing table metadata for \"{dbname}.{table}\": {e}"),
    })?;
    encode_table_with_metadata(dbname, table, src, Some(metadata))
}

/// The writer core: encode `src` as a Vortex file with an optional
/// `pinkdb.table` segment. The loader requires the segment; `None` exists so
/// tests can craft a file that lacks it (a tamper the loader must reject).
fn encode_table_with_metadata(
    dbname: &str,
    table: &str,
    src: &DataTable,
    metadata: Option<Vec<u8>>,
) -> Result<Vec<u8>> {
    let schema = src.schema();
    if schema.is_empty() {
        return Err(LakeError::NotRepresentable {
            message: format!(
                "table \"{dbname}.{table}\" has no columns and cannot be encoded as \
                 a Vortex file"
            ),
        });
    }
    // One pass over the chunks computes both the per-chunk NULL flags (used
    // to build each chunk's validity) and the table-level nullability (all
    // chunks of a table share one dtype, so one nullable chunk makes the
    // column nullable everywhere).
    let (nulls, chunk_nulls) = column_nullability(src, &schema, dbname, table)?;
    let dtype = struct_dtype(&schema, &nulls);
    let fields = field_names(&schema);

    let mut chunks: Vec<ArrayRef> = Vec::with_capacity(src.num_chunks().max(1));
    for (i, any_null) in chunk_nulls.iter().enumerate() {
        let chunk = src.scan_chunk(i).ok_or_else(|| LakeError::Internal {
            message: format!("chunk {i} of \"{dbname}.{table}\" disappeared while encoding"),
        })?;
        chunks.push(encode_chunk(
            &chunk,
            &schema,
            &nulls,
            any_null,
            dbname,
            table,
        )?);
    }
    if chunks.is_empty() {
        // An empty table: one 0-row struct, so the file still carries the
        // schema. The loader appends nothing back (0 chunks).
        let children = schema
            .iter()
            .zip(&nulls)
            .map(|((_, t), n)| encode_column(&Vector::new_empty(*t), *t, *n, false))
            .collect::<Result<Vec<_>>>()?;
        chunks.push(
            StructArray::try_new(fields.clone(), children, 0, Validity::NonNullable)
                .map_err(|e| {
                    vortex_error(&format!("encoding empty table \"{dbname}.{table}\""), e)
                })?
                .into_array(),
        );
    }

    let chunked = ChunkedArray::try_new(chunks, dtype)
        .map_err(|e| vortex_error(&format!("assembling table \"{dbname}.{table}\""), e))?;

    write_chunked(dbname, table, chunked.into_array(), metadata, true)
}

/// The writer tail: strategy + metadata segment + the blocking write.
/// `parallel` selects the worker-pool runtime (production) or the bare
/// current-thread runtime (test-only pin); both must emit identical bytes.
fn write_chunked(
    dbname: &str,
    table: &str,
    chunked: ArrayRef,
    metadata: Option<Vec<u8>>,
    parallel: bool,
) -> Result<Vec<u8>> {
    let (runtime, _pool, session) = write_session(parallel);
    // One stream element per engine chunk, and row blocks of exactly
    // `CHUNK_SIZE` rows: chunk boundaries round-trip unchanged.
    let strategy = WriteStrategyBuilder::from_session(&session)
        .with_row_block_size(pinkdb::CHUNK_SIZE)
        .with_data_block_target_bytes(None)
        .build();
    let mut options = session.write_options().with_strategy(strategy);
    if let Some(metadata) = metadata {
        options =
            options.with_metadata_segment(TABLE_METADATA_KEY, ByteBuffer::copy_from(metadata));
    }
    let mut bytes: Vec<u8> = Vec::new();
    options
        .blocking(&runtime)
        .write(&mut bytes, chunked.to_array_iterator())
        .map_err(|e| vortex_error(&format!("writing table \"{dbname}.{table}\""), e))?;
    Ok(bytes)
}

/// Decode a Vortex data file into a one-table [`PinkDb`] (the same shape a
/// v1 `.pink` data file opens as, so the loader's checks are identical).
///
/// Every Vortex decoder entry point lives inside [`decode_table_inner`] and
/// is reached only through [`with_decoder_guard`], so a malformed file can
/// produce a clean [`LakeError::Corrupt`] but never an escaping panic.
pub(crate) fn decode_table(path: &Path, bytes: &[u8]) -> Result<PinkDb> {
    with_decoder_guard(path, "opening and decoding it", || {
        decode_table_inner(path, bytes)
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

fn decode_table_inner(path: &Path, bytes: &[u8]) -> Result<PinkDb> {
    let (runtime, session) = session();
    let file = session
        .open_options()
        .include_metadata()
        .open_buffer(ByteBuffer::copy_from(bytes))
        .map_err(|e| corrupt(path, format!("not a valid Vortex file: {e}")))?;

    let metadata = parse_metadata(path, &file)?;
    let schema = schema_of(path, &file, &metadata)?;
    let mut mini = empty_single_table_store(&metadata.db, &metadata.table, schema.clone())?;

    let mut stream = file
        .scan()
        .map_err(|e| corrupt(path, format!("opening a Vortex scan: {e}")))?
        .into_array_stream()
        .map_err(|e| corrupt(path, format!("opening a Vortex scan: {e}")))?;
    let mut ctx = session.create_execution_ctx();
    for item in runtime.block_on_stream(&mut stream) {
        let chunk = item.map_err(|e| corrupt(path, format!("reading a Vortex chunk: {e}")))?;
        let len = chunk.len();
        let fields = chunk
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(|e| corrupt(path, format!("decoding a Vortex chunk: {e}")))?;
        let mut columns = Vec::with_capacity(schema.len());
        for (i, (name, dtype)) in schema.iter().enumerate() {
            let field = fields.unmasked_field(i).clone();
            columns.push(decode_column(path, name, &field, *dtype, &mut ctx)?);
        }
        if len == 0 {
            continue;
        }
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
            let any_null = (0..column.len()).any(|row| column.is_null(row));
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
            Validity::from_iter((0..column.len()).map(|i| column.is_valid(i)))
        }
    }
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
                Nullability::NonNullable => VarBinViewArray::from_iter_str(
                    (0..column.len()).map(|i| values[i].as_ref()),
                )
                .into_array(),
                Nullability::Nullable => VarBinViewArray::from_iter_nullable_str(
                    (0..column.len()).map(|i| column.is_valid(i).then(|| values[i].as_ref())),
                )
                .into_array(),
            }
        }
        (dtype, _data) => {
            return Err(LakeError::Internal {
                message: format!("column dtype {dtype:?} does not match its data layout"),
            })
        }
    };
    Ok(array)
}

/// Decode one canonical Vortex column into an engine vector of `len` rows.
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
    let valid = |i: usize| mask.as_ref().is_none_or(|m| m.value(i));

    let mut out = Vector::with_capacity(dtype, len);
    match dtype {
        DataType::Boolean => {
            let values = bool_values(path, name, array, ctx)?;
            for i in 0..len {
                push_bool(name, &mut out, valid(i), values.value(i))?;
            }
        }
        DataType::Varchar => {
            let values = var_bin_values(path, name, array, ctx)?;
            for i in 0..len {
                if valid(i) {
                    push_str(path, name, &mut out, &values, i)?;
                } else {
                    out.push_null().map_err(|e| column_error(name, e))?;
                }
            }
        }
        DataType::Integer => {
            let values = primitive(path, name, array, ctx)?;
            let buffer = values.to_buffer::<i32>();
            for i in 0..len {
                if valid(i) {
                    out.push_value(pinkdb::Value::Integer(buffer[i]))
                        .map_err(|e| column_error(name, e))?;
                } else {
                    out.push_null().map_err(|e| column_error(name, e))?;
                }
            }
        }
        DataType::Date => {
            let values = primitive(path, name, array, ctx)?;
            let buffer = values.to_buffer::<i32>();
            for i in 0..len {
                if valid(i) {
                    out.push_value(pinkdb::Value::Date(buffer[i]))
                        .map_err(|e| column_error(name, e))?;
                } else {
                    out.push_null().map_err(|e| column_error(name, e))?;
                }
            }
        }
        DataType::Bigint => {
            let values = primitive(path, name, array, ctx)?;
            let buffer = values.to_buffer::<i64>();
            for i in 0..len {
                if valid(i) {
                    out.push_value(pinkdb::Value::Bigint(buffer[i]))
                        .map_err(|e| column_error(name, e))?;
                } else {
                    out.push_null().map_err(|e| column_error(name, e))?;
                }
            }
        }
        DataType::Timestamp => {
            let values = primitive(path, name, array, ctx)?;
            let buffer = values.to_buffer::<i64>();
            for i in 0..len {
                if valid(i) {
                    out.push_value(pinkdb::Value::Timestamp(buffer[i]))
                        .map_err(|e| column_error(name, e))?;
                } else {
                    out.push_null().map_err(|e| column_error(name, e))?;
                }
            }
        }
        DataType::Double => {
            let values = primitive(path, name, array, ctx)?;
            let buffer = values.to_buffer::<f64>();
            for i in 0..len {
                if valid(i) {
                    out.push_value(pinkdb::Value::Double(buffer[i]))
                        .map_err(|e| column_error(name, e))?;
                } else {
                    out.push_null().map_err(|e| column_error(name, e))?;
                }
            }
        }
    }
    Ok(out)
}

fn column_error(name: &str, e: pinkdb::PinkDbError) -> LakeError {
    LakeError::Engine {
        context: format!("building column \"{name}\""),
        source: e,
    }
}

fn push_bool(name: &str, out: &mut Vector, valid: bool, value: bool) -> Result<()> {
    if valid {
        out.push_value(pinkdb::Value::Boolean(value))
            .map_err(|e| column_error(name, e))
    } else {
        out.push_null().map_err(|e| column_error(name, e))
    }
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

fn push_str(
    path: &Path,
    name: &str,
    out: &mut Vector,
    values: &VarBinViewArray,
    i: usize,
) -> Result<()> {
    let bytes = values.bytes_at(i);
    let s = std::str::from_utf8(bytes.as_slice()).map_err(|e| {
        corrupt(
            path,
            format!("column \"{name}\" row {i} is not valid UTF-8: {e}"),
        )
    })?;
    out.push_str(s).map_err(|e| column_error(name, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pinkdb::{PinkDb, Value, CHUNK_SIZE};

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

    fn decode(bytes: &[u8]) -> Result<PinkDb> {
        decode_table(Path::new("crafted.vortex"), bytes)
    }

    fn decode_with_metadata(src: &DataTable, metadata: &[u8]) -> Result<PinkDb> {
        let bytes = encode_table_with_metadata("main", "t", src, Some(metadata.to_vec())).unwrap();
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
        db.execute(
            "CREATE TABLE t (i INTEGER, g BIGINT, d DOUBLE, s VARCHAR, b BOOLEAN);",
        )
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
        let schema = src.schema();
        assert!(src.num_chunks() >= 3, "fixture should span several chunks");
        let (nulls, chunk_nulls) = column_nullability(src, &schema, "main", "t").unwrap();
        let dtype = struct_dtype(&schema, &nulls);
        let mut chunks: Vec<ArrayRef> = Vec::new();
        for (i, any_null) in chunk_nulls.iter().enumerate() {
            let chunk = src.scan_chunk(i).unwrap();
            chunks.push(encode_chunk(&chunk, &schema, &nulls, any_null, "main", "t").unwrap());
        }
        let chunked = ChunkedArray::try_new(chunks, dtype).unwrap().into_array();
        let meta = metadata_json(
            "main",
            "t",
            r#"{"name":"i","type":"integer"},{"name":"g","type":"bigint"},{"name":"d","type":"double"},{"name":"s","type":"varchar"},{"name":"b","type":"boolean"}"#,
        );
        // The metadata segment content is irrelevant to layout; reuse one.
        let serial = write_chunked("main", "t", chunked.clone(), Some(meta.clone()), false)
            .unwrap();
        let parallel = write_chunked("main", "t", chunked.clone(), Some(meta.clone()), true)
            .unwrap();
        let parallel_again = write_chunked("main", "t", chunked, Some(meta), true).unwrap();
        assert_eq!(
            serial, parallel,
            "a worker-pool write changed the file bytes ({} bytes vs {})",
            serial.len(),
            parallel.len()
        );
        assert_eq!(parallel_again, parallel, "parallel writes must be deterministic");
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
            ("string_heavy_40", string_victim(), 128usize, cfg!(debug_assertions)),
        ] {
            let bytes = victim_bytes(&db);
            let mut found = None;
            let last = (pinned + 32).min(bytes.len() - 1);
            for pos in pinned.saturating_sub(32)..=last {
                for bit in [7u32, 0u32] {
                    let mut b = bytes.clone();
                    b[pos] ^= 1 << bit;
                    if let Err(e) = decode_table(Path::new("victim.vortex"), &b) {
                        if e.to_string().contains("decoder panic caught") {
                            found = Some((pos, bit, e));
                            break;
                        }
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
        let bytes = encode_table_with_metadata("main", "t", db.table("t").unwrap(), None).unwrap();
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
