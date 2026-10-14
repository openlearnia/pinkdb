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
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

use crate::{empty_single_table_store, LakeError, Result};

/// The user metadata segment key holding the table identity + pinkdb schema.
pub(crate) const TABLE_METADATA_KEY: &str = "pinkdb.table";

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

/// A blocking runtime + a session bound to it. The session is created per
/// call: a lake operation is I/O dominated and the registration cost is
/// negligible next to encoding/compressing a table.
fn session() -> (CurrentThreadRuntime, VortexSession) {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    (runtime, session)
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

/// Encode one table as a Vortex file and return its bytes.
pub(crate) fn encode_table(dbname: &str, table: &str, src: &DataTable) -> Result<Vec<u8>> {
    let schema = src.schema();
    if schema.is_empty() {
        return Err(LakeError::NotRepresentable {
            message: format!(
                "table \"{dbname}.{table}\" has no columns and cannot be encoded as \
                 a Vortex file"
            ),
        });
    }
    let nulls: Vec<Nullability> = (0..schema.len())
        .map(|c| column_nullability(src, c, dbname, table))
        .collect::<Result<_>>()?;
    let dtype = struct_dtype(&schema, &nulls);
    let fields = field_names(&schema);

    let mut chunks: Vec<ArrayRef> = Vec::with_capacity(src.num_chunks().max(1));
    for i in 0..src.num_chunks() {
        let chunk = src.scan_chunk(i).ok_or_else(|| LakeError::Internal {
            message: format!("chunk {i} of \"{dbname}.{table}\" disappeared while encoding"),
        })?;
        chunks.push(encode_chunk(&chunk, &schema, &nulls, dbname, table)?);
    }
    if chunks.is_empty() {
        // An empty table: one 0-row struct, so the file still carries the
        // schema. The loader appends nothing back (0 chunks).
        let children = schema
            .iter()
            .zip(&nulls)
            .map(|((_, t), n)| encode_column(&Vector::new_empty(*t), *t, *n))
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
    let metadata = TableMetadata {
        db: dbname.to_string(),
        table: table.to_string(),
        columns: schema
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

    let (runtime, session) = session();
    // One stream element per engine chunk, and row blocks of exactly
    // `CHUNK_SIZE` rows: chunk boundaries round-trip unchanged.
    let strategy = WriteStrategyBuilder::from_session(&session)
        .with_row_block_size(pinkdb::CHUNK_SIZE)
        .with_data_block_target_bytes(None)
        .build();
    let mut bytes: Vec<u8> = Vec::new();
    session
        .write_options()
        .with_strategy(strategy)
        .with_metadata_segment(TABLE_METADATA_KEY, ByteBuffer::copy_from(metadata))
        .blocking(&runtime)
        .write(&mut bytes, chunked.into_array().to_array_iterator())
        .map_err(|e| vortex_error(&format!("writing table \"{dbname}.{table}\""), e))?;
    Ok(bytes)
}

/// Decode a Vortex data file into a one-table [`PinkDb`] (the same shape a
/// v1 `.pink` data file opens as, so the loader's checks are identical).
pub(crate) fn decode_table(path: &Path, bytes: &[u8]) -> Result<PinkDb> {
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

/// Whether column `c` contains a NULL in any chunk of `src`: all chunks of a
/// table must share one dtype, so one nullable chunk makes the column
/// nullable everywhere.
fn column_nullability(
    src: &DataTable,
    c: usize,
    dbname: &str,
    table: &str,
) -> Result<Nullability> {
    for i in 0..src.num_chunks() {
        let chunk = src.scan_chunk(i).ok_or_else(|| LakeError::Internal {
            message: format!("chunk {i} of \"{dbname}.{table}\" disappeared while encoding"),
        })?;
        let column = chunk.columns.get(c).ok_or_else(|| LakeError::Internal {
            message: format!("column {c} of \"{dbname}.{table}\" disappeared while encoding"),
        })?;
        for row in 0..column.len() {
            if column.is_null(row) {
                return Ok(Nullability::Nullable);
            }
        }
    }
    Ok(Nullability::NonNullable)
}

fn encode_chunk(
    chunk: &DataChunk,
    schema: &[(String, DataType)],
    nulls: &[Nullability],
    dbname: &str,
    table: &str,
) -> Result<ArrayRef> {
    let mut children = Vec::with_capacity(chunk.columns.len());
    for (i, column) in chunk.columns.iter().enumerate() {
        children.push(encode_column(column, schema[i].1, nulls[i])?);
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
/// a nullable column with no NULLs in *this* chunk stays nullable and uses
/// `Validity::AllValid`.
fn chunk_validity(column: &Vector, nullability: Nullability) -> Validity {
    match nullability {
        Nullability::NonNullable => Validity::NonNullable,
        Nullability::Nullable => {
            if column.is_empty() {
                return Validity::AllValid;
            }
            let any_null = (0..column.len()).any(|i| column.is_null(i));
            if !any_null {
                return Validity::AllValid;
            }
            Validity::from_iter((0..column.len()).map(|i| column.is_valid(i)))
        }
    }
}

/// Encode one engine column chunk as a canonical Vortex array.
fn encode_column(column: &Vector, dtype: DataType, nullability: Nullability) -> Result<ArrayRef> {
    let validity = chunk_validity(column, nullability);
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
