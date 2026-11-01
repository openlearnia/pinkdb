//! The **write** half of the parquet extension (wave 87): encode one
//! [`DataTable`] as a flat parquet file the crate's own reader (and any
//! standards-compliant reader) reads back exactly.
//!
//! # Type mapping
//!
//! The inverse of [`crate::schema_map`], pinned by round-trip tests against
//! the existing reader:
//!
//! | pinkdb | parquet |
//! |---|---|
//! | BOOLEAN | BOOLEAN |
//! | INTEGER | INT32 |
//! | BIGINT | INT64 |
//! | DOUBLE | DOUBLE (exact bits: NaN, `-0.0`, ±inf preserved) |
//! | VARCHAR | BYTE_ARRAY + STRING (UTF-8) |
//! | DATE | INT32 + DATE |
//! | TIMESTAMP | INT64 + TIMESTAMP(MICROS), `is_adjusted_to_utc = false` |
//! | DECIMAL(p,s) | INT32 (p ≤ 9) / INT64 (p ≤ 18) / FIXED_LEN_BYTE_ARRAY(16) + DECIMAL(p,s) |
//! | VARIANT | GROUP + `Variant` logical type: `metadata`, `value`, and a `typed_value` group with one typed column per shredded key ([`variant_shred`]) |
//! | ARRAY | the three-level LIST group DuckDB 2.0 writes, whose length is only in the number of element slots per row (plus a `pinkdb.array.<name>` key-value entry) |
//!
//! Every column is `OPTIONAL`; NULLs travel as definition levels. INTERVAL is
//! refused — parquet's INTERVAL is millisecond precision and pinkdb's is
//! microseconds, so writing one would be lossy — with an error naming the
//! column.
//!
//! One row group is written per [`DEFAULT_ROW_GROUP_ROWS`] rows. Chunk-level
//! column statistics (min/max/null count) are enabled, so the file is
//! self-describing for pruning by other readers; the lake additionally
//! publishes its own exact per-column facts from the table (see the
//! `pink-lake-parquet` bridge crate).
//!
//! Files written for the lake also carry `pinkdb.db` / `pinkdb.table`
//! key-value metadata ([`write_table_with_identity`], read back by
//! [`read_identity`]): parquet has no native table identity, and the lake's
//! loader checks that a data file holds the catalog's database/table.

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use parquet::basic::Type as PhysicalType;
use parquet::basic::{Compression, ConvertedType, LogicalType, Repetition, TimeUnit};
use parquet::column::writer::ColumnWriterImpl;
use parquet::data_type::{
    BoolType, ByteArray, ByteArrayType, DataType as ParquetDataType, DoubleType, FixedLenByteArray,
    FixedLenByteArrayType, FloatType, Int32Type, Int64Type,
};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::{
    SerializedColumnWriter, SerializedFileWriter, SerializedRowGroupWriter,
};
use parquet::schema::types::Type;
use pinkdb::variant::Variant;
use pinkdb::variant_shred::{
    ShredKind, ShredPolicy, decode_metadata, encode_metadata, encode_value, infer_policy, shred_row,
};
use pinkdb::{ArrayElem, DataTable, DataType, Value, Vector};

/// The footer key naming the database a lake data file belongs to.
pub const DB_METADATA_KEY: &str = "pinkdb.db";

/// The footer key naming the table a lake data file belongs to.
pub const TABLE_METADATA_KEY: &str = "pinkdb.table";

/// Rows per row group. Bounded so a large table is never one giant group
/// (parallel/pruned reads keep working) and a tiny one is still one group.
pub const DEFAULT_ROW_GROUP_ROWS: usize = 64 * 1024;

/// The fixed byte width of a DECIMAL wider than INT64 (precision up to 38
/// fits in 16 two's-complement bytes: 10^38 < 2^127).
const DECIMAL_FIXED_LEN: i32 = 16;

/// A parquet write failure: an unsupported column, a value that does not
/// match its column, or a parquet/integrity error.
#[derive(Debug)]
pub enum WriteError {
    /// The column's type has no exact parquet representation (INTERVAL).
    UnsupportedType { column: String, message: String },
    /// A value in the column does not match the column's declared type.
    InvalidValue { column: String, message: String },
    /// The parquet encoder rejected the data.
    Parquet(parquet::errors::ParquetError),
    /// A staging read (the identity metadata scan) failed.
    Io(std::io::Error),
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteError::UnsupportedType { column, message } => {
                write!(
                    f,
                    "column \"{column}\" cannot be written to parquet: {message}"
                )
            }
            WriteError::InvalidValue { column, message } => {
                write!(f, "column \"{column}\": {message}")
            }
            WriteError::Parquet(e) => write!(f, "parquet encoding error: {e}"),
            WriteError::Io(e) => write!(f, "parquet file i/o error: {e}"),
        }
    }
}

impl std::error::Error for WriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WriteError::Parquet(e) => Some(e),
            WriteError::Io(e) => Some(e),
            WriteError::UnsupportedType { .. } | WriteError::InvalidValue { .. } => None,
        }
    }
}

impl From<parquet::errors::ParquetError> for WriteError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        WriteError::Parquet(e)
    }
}

impl From<std::io::Error> for WriteError {
    fn from(e: std::io::Error) -> Self {
        WriteError::Io(e)
    }
}

/// Encode `table` as one complete parquet file.
///
/// The identity check the lake performs needs [`write_table_with_identity`];
/// this plain form is for standalone use (tests, COPY-shaped writers).
pub fn write_table(table: &DataTable) -> Result<Vec<u8>, WriteError> {
    write_table_with_key_values(table, Vec::new(), &[])
}

/// Encode `table` as one complete parquet file and record the lake identity
/// (`db` / `table`) in the footer, so the lake's loader can verify that a
/// data file holds the catalog's table (parquet itself has no table name).
pub fn write_table_with_identity(
    src: &DataTable,
    db: &str,
    table: &str,
) -> Result<Vec<u8>, WriteError> {
    write_table_with_identity_and_fields(src, db, table, &[])
}

/// [`write_table_with_identity`] with the lake's catalog column ids, so a
/// DuckLake reader can match the children of a nested column.
pub fn write_table_with_identity_and_fields(
    src: &DataTable,
    db: &str,
    table: &str,
    field_ids: &[(i32, i32)],
) -> Result<Vec<u8>, WriteError> {
    write_table_with_key_values(
        src,
        vec![
            KeyValue::new(DB_METADATA_KEY.to_string(), Some(db.to_string())),
            KeyValue::new(TABLE_METADATA_KEY.to_string(), Some(table.to_string())),
        ],
        field_ids,
    )
}

/// Encode `table` as one complete parquet file with explicit `field_id`s: one
/// `(column, array_child)` pair per column, `0` meaning "write none".
///
/// The lake records `field_id` = the catalog's `ducklake_column.column_id`
/// on every top-level column, and on an ARRAY's `element` leaf, because a
/// DuckLake reader matches the children of a nested column by id — a parquet
/// list has no name of its own. A standalone file (`COPY … TO`,
/// `write_parquet`) writes none, exactly as DuckDB does there.
pub fn write_table_with_fields(
    table: &DataTable,
    key_values: Vec<KeyValue>,
    field_ids: &[(i32, i32)],
) -> Result<Vec<u8>, WriteError> {
    write_table_with_key_values(table, key_values, field_ids)
}

/// The lake identity stored in a parquet footer by
/// [`write_table_with_identity`], if both keys are present.
pub fn read_identity(path: &Path) -> Result<Option<(String, String)>, WriteError> {
    let file = std::fs::File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata().file_metadata();
    let Some(pairs) = metadata.key_value_metadata() else {
        return Ok(None);
    };
    let mut db = None;
    let mut table = None;
    for kv in pairs {
        match kv.key.as_str() {
            DB_METADATA_KEY => db = kv.value.clone(),
            TABLE_METADATA_KEY => table = kv.value.clone(),
            _ => {}
        }
    }
    Ok(match (db, table) {
        (Some(db), Some(table)) => Some((db, table)),
        _ => None,
    })
}

/// Dictionary page limit per column chunk. Once a chunk's dictionary outgrows
/// it, the writer falls back to PLAIN encoding for the rest of that chunk
/// (per column chunk, as DuckDB does), so a high-cardinality column never
/// pays a dictionary it cannot exploit.
pub const DICTIONARY_PAGE_LIMIT: usize = 1024 * 1024;

/// The page codec. Snappy is DuckDB's Parquet default and is the reason a
/// pinkdb-written file is comparable with a DuckDB one; a build without the
/// codec feature (never the extension's default) writes plain pages.
#[cfg(feature = "snap")]
const PAGE_COMPRESSION: Compression = Compression::SNAPPY;
#[cfg(not(feature = "snap"))]
const PAGE_COMPRESSION: Compression = Compression::UNCOMPRESSED;

fn write_table_with_key_values(
    table: &DataTable,
    key_values: Vec<KeyValue>,
    field_ids: &[(i32, i32)],
) -> Result<Vec<u8>, WriteError> {
    let schema = table.schema();
    let policies = column_policies(table)?;
    // Tell readers that every VARIANT column was shredded with a policy
    // inferred over the whole column, so each typed leaf bounds every value of
    // its path (`crate::variant_stats` trusts this key, and nothing else).
    let mut key_values = key_values;
    if schema
        .iter()
        .any(|(_, dtype)| matches!(dtype, DataType::Variant))
        && !key_values
            .iter()
            .any(|kv| kv.key == crate::variant_stats::COMPLETE_KEY)
    {
        key_values.push(KeyValue::new(
            crate::variant_stats::COMPLETE_KEY.to_string(),
            crate::variant_stats::COMPLETE_VALUE.to_string(),
        ));
    }
    // A parquet list group has no length of its own, so record each ARRAY
    // column's. A DuckDB reader ignores the key; ours needs it when the file
    // has no row groups to infer from (see `crate::schema_map::array_length`).
    for (name, dtype) in &schema {
        if let DataType::Array { len, .. } = dtype {
            key_values.push(KeyValue::new(
                crate::schema_map::array_length_key(name),
                len.to_string(),
            ));
        }
    }
    let root = build_schema(&schema, &policies, field_ids)?;
    let mut builder = WriterProperties::builder()
        // Chunk statistics travel in the footer (min/max/NULL count), so the
        // file is self-describing for other readers and for pruning.
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_compression(PAGE_COMPRESSION)
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(DICTIONARY_PAGE_LIMIT);
    if !key_values.is_empty() {
        builder = builder.set_key_value_metadata(Some(key_values));
    }
    let properties = Arc::new(builder.build());

    // A cursor per column, kept across row groups so encoding is one linear
    // pass over the table (no per-row `DataTable::get` chunk walk).
    let mut cursors: Vec<ColumnCursor> = table
        .columns
        .iter()
        .map(|column| ColumnCursor {
            chunks: (0..column.num_chunks())
                .map(|i| column.chunk(i).expect("a built table decodes"))
                .collect(),
            chunk: 0,
            offset: 0,
        })
        .collect();

    let mut bytes = Vec::new();
    {
        let mut writer = SerializedFileWriter::new(&mut bytes, Arc::new(root), properties)?;
        let total = table.num_rows();
        let mut start = 0;
        while start < total {
            let end = (start + DEFAULT_ROW_GROUP_ROWS).min(total);
            let rows = end - start;
            let mut group = writer.next_row_group()?;
            for (index, (name, dtype)) in schema.iter().enumerate() {
                if matches!(dtype, DataType::Variant) {
                    write_variant_column(
                        &mut group,
                        &policies[index],
                        rows,
                        &mut cursors[index],
                        name,
                    )?;
                    continue;
                }
                if matches!(dtype, DataType::Array { .. }) {
                    write_array_column(&mut group, dtype, rows, &mut cursors[index], name)?;
                    continue;
                }
                let column = group.next_column()?.ok_or_else(|| skipped(name))?;
                write_column(column, dtype, rows, &mut cursors[index], name)?;
            }
            group.close()?;
            start = end;
        }
        // Zero-row tables write a schema-only file (no row groups); the
        // reader binds the schema and decodes no rows.
        writer.close()?;
    }
    Ok(bytes)
}

/// One column's position while walking the table's chunks in row order.
struct ColumnCursor {
    chunks: Vec<Arc<Vector>>,
    chunk: usize,
    offset: usize,
}

impl ColumnCursor {
    fn next(&mut self) -> Value {
        while self.chunk < self.chunks.len() && self.offset >= self.chunks[self.chunk].len() {
            self.chunk += 1;
            self.offset = 0;
        }
        let chunk = self
            .chunks
            .get(self.chunk)
            .expect("a table column ended before the table's row count");
        let value = chunk.get(self.offset);
        self.offset += 1;
        value
    }
}

/// The physical type of one engine type, or a refusal naming the column.
fn physical_type(dtype: DataType, column: &str) -> Result<PhysicalType, WriteError> {
    Ok(match dtype {
        DataType::Boolean => PhysicalType::BOOLEAN,
        DataType::Integer | DataType::Date => PhysicalType::INT32,
        DataType::Bigint | DataType::Timestamp => PhysicalType::INT64,
        DataType::Double => PhysicalType::DOUBLE,
        DataType::Varchar => PhysicalType::BYTE_ARRAY,
        DataType::Decimal { precision, .. } => {
            if precision <= 9 {
                PhysicalType::INT32
            } else if precision <= 18 {
                PhysicalType::INT64
            } else {
                PhysicalType::FIXED_LEN_BYTE_ARRAY
            }
        }
        DataType::Interval => {
            return Err(WriteError::UnsupportedType {
                column: column.to_string(),
                message: "parquet's INTERVAL is millisecond precision and pinkdb's is \
                          microseconds; refusing to write an interval lossily"
                    .to_string(),
            });
        }
        // Written as the `Variant` logical type; see `write_variant_column`.
        DataType::Variant => return Err(skipped(column)),
        // Written as the LIST group; see `write_array_column`.
        DataType::Array { .. } => return Err(skipped(column)),
    })
}

/// The parquet writer's own column order moved on (`next_column()` returned
/// `None`): a schema/row-group mismatch, never reachable from a well-formed
/// table.
fn skipped(column: &str) -> WriteError {
    WriteError::InvalidValue {
        column: column.to_string(),
        message: "the parquet writer skipped a schema column".to_string(),
    }
}

/// Shredding policies for the table's VARIANT columns (empty for the rest),
/// inferred once from every row of the column.
///
/// A Parquet schema cannot change between row groups, so the policy is fixed
/// at the file level: this is why the plan asks for a *sampled* policy rather
/// than an exact one.
fn column_policies(table: &DataTable) -> Result<Vec<ShredPolicy>, WriteError> {
    table
        .columns
        .iter()
        .map(|column| {
            if column.dtype != DataType::Variant {
                return Ok(ShredPolicy::default());
            }
            // The policy is inferred over the **whole** column, not a sample:
            // a typed leaf is a complete bound for its path only if no later
            // row holds another kind there, and the per-path statistics of
            // both lake formats rely on that (see `crate::variant_stats`).
            let mut values: Vec<Arc<Variant>> = Vec::new();
            for i in 0..column.num_chunks() {
                let chunk = column.chunk(i).map_err(|e| WriteError::InvalidValue {
                    column: column.name.clone(),
                    message: format!("chunk {i} could not be decoded: {e}"),
                })?;
                let pinkdb::VectorData::Variant(items) = chunk.data() else {
                    continue;
                };
                for (index, item) in items.iter().enumerate() {
                    if chunk.is_valid(index) {
                        values.push(Arc::clone(item));
                    }
                }
            }
            let refs: Vec<&Variant> = values.iter().map(|value| value.as_ref()).collect();
            Ok(infer_policy(&refs))
        })
        .collect()
}

/// The flat parquet schema: every column OPTIONAL (definition levels carry
/// the NULLs), with the logical annotation the existing reader maps back.
fn build_schema(
    schema: &[(String, DataType)],
    policies: &[ShredPolicy],
    field_ids: &[(i32, i32)],
) -> Result<Type, WriteError> {
    let mut fields = Vec::with_capacity(schema.len());
    for (index, (name, dtype)) in schema.iter().enumerate() {
        let ids = field_ids.get(index).copied().unwrap_or((0, 0));
        if matches!(dtype, DataType::Variant) {
            fields.push(Arc::new(variant_group(name, &policies[index])?));
            continue;
        }
        if let DataType::Array { elem, .. } = dtype {
            fields.push(Arc::new(array_group(name, *elem, ids)?));
            continue;
        }
        let physical = physical_type(*dtype, name)?;
        let mut builder =
            Type::primitive_type_builder(name, physical).with_repetition(Repetition::OPTIONAL);
        if ids.0 != 0 {
            builder = builder.with_id(Some(ids.0));
        }
        match dtype {
            DataType::Boolean | DataType::Integer | DataType::Bigint | DataType::Double => {}
            DataType::Varchar => {
                builder = builder
                    .with_converted_type(ConvertedType::UTF8)
                    .with_logical_type(Some(LogicalType::String));
            }
            DataType::Date => {
                builder = builder
                    .with_converted_type(ConvertedType::DATE)
                    .with_logical_type(Some(LogicalType::Date));
            }
            DataType::Timestamp => {
                builder = builder
                    .with_converted_type(ConvertedType::TIMESTAMP_MICROS)
                    // Naive microseconds: `is_adjusted_to_utc = false`, so no
                    // reader shifts the value by a time zone.
                    .with_logical_type(Some(LogicalType::timestamp(false, TimeUnit::MICROS)));
            }
            DataType::Decimal { precision, scale } => {
                builder = builder
                    .with_converted_type(ConvertedType::DECIMAL)
                    .with_precision(i32::from(*precision))
                    .with_scale(i32::from(*scale))
                    // `LogicalType::decimal` takes (scale, precision).
                    .with_logical_type(Some(LogicalType::decimal(
                        i32::from(*scale),
                        i32::from(*precision),
                    )));
                if *precision > 18 {
                    builder = builder.with_length(DECIMAL_FIXED_LEN);
                }
            }
            // Refused by `physical_type`, and VARIANT/ARRAY never reach the
            // flat primitive path above.
            DataType::Interval | DataType::Variant | DataType::Array { .. } => {
                unreachable!("refused by physical_type")
            }
        }
        fields.push(Arc::new(builder.build()?));
    }
    Ok(Type::group_type_builder("schema")
        .with_fields(fields)
        .build()?)
}

/// Encode one column chunk's `rows` values and close it.
fn write_column(
    mut column: SerializedColumnWriter<'_>,
    dtype: &DataType,
    rows: usize,
    cursor: &mut ColumnCursor,
    name: &str,
) -> Result<(), WriteError> {
    match dtype {
        DataType::Boolean => write_typed(
            column.typed::<BoolType>(),
            rows,
            cursor,
            name,
            |v| match v {
                Value::Boolean(b) => Ok(*b),
                _ => Err(wrong_type(v, DataType::Boolean)),
            },
        )?,
        DataType::Integer => {
            write_typed(
                column.typed::<Int32Type>(),
                rows,
                cursor,
                name,
                |v| match v {
                    Value::Integer(i) => Ok(*i),
                    _ => Err(wrong_type(v, DataType::Integer)),
                },
            )?
        }
        DataType::Bigint => write_typed(
            column.typed::<Int64Type>(),
            rows,
            cursor,
            name,
            |v| match v {
                Value::Bigint(i) => Ok(*i),
                _ => Err(wrong_type(v, DataType::Bigint)),
            },
        )?,
        DataType::Double => {
            write_typed(
                column.typed::<DoubleType>(),
                rows,
                cursor,
                name,
                |v| match v {
                    // `f64` is copied bit-exactly: NaN payloads, -0.0 and ±inf
                    // survive the parquet PLAIN encoding unchanged.
                    Value::Double(x) => Ok(*x),
                    _ => Err(wrong_type(v, DataType::Double)),
                },
            )?
        }
        DataType::Varchar => write_typed(
            column.typed::<ByteArrayType>(),
            rows,
            cursor,
            name,
            |v| match v {
                Value::Varchar(s) => Ok(ByteArray::from(s.as_bytes().to_vec())),
                _ => Err(wrong_type(v, DataType::Varchar)),
            },
        )?,
        DataType::Date => write_typed(
            column.typed::<Int32Type>(),
            rows,
            cursor,
            name,
            |v| match v {
                Value::Date(d) => Ok(*d),
                _ => Err(wrong_type(v, DataType::Date)),
            },
        )?,
        DataType::Timestamp => {
            write_typed(
                column.typed::<Int64Type>(),
                rows,
                cursor,
                name,
                |v| match v {
                    Value::Timestamp(us) => Ok(*us),
                    _ => Err(wrong_type(v, DataType::Timestamp)),
                },
            )?
        }
        DataType::Decimal { precision, scale } => {
            let scale = *scale;
            let decimal_error = |v: &Value| wrong_type(v, *dtype);
            if *precision <= 9 {
                write_typed(
                    column.typed::<Int32Type>(),
                    rows,
                    cursor,
                    name,
                    |v| match v {
                        Value::Decimal(unscaled, value_scale) => {
                            decimal_scale_check(*value_scale, scale)?;
                            i32::try_from(*unscaled).map_err(|_| {
                                format!(
                                    "decimal value {unscaled} does not fit the column's \
                                     INT32 parquet storage"
                                )
                            })
                        }
                        _ => Err(decimal_error(v)),
                    },
                )?;
            } else if *precision <= 18 {
                write_typed(
                    column.typed::<Int64Type>(),
                    rows,
                    cursor,
                    name,
                    |v| match v {
                        Value::Decimal(unscaled, value_scale) => {
                            decimal_scale_check(*value_scale, scale)?;
                            i64::try_from(*unscaled).map_err(|_| {
                                format!(
                                    "decimal value {unscaled} does not fit the column's \
                                     INT64 parquet storage"
                                )
                            })
                        }
                        _ => Err(decimal_error(v)),
                    },
                )?;
            } else {
                write_typed(
                    column.typed::<FixedLenByteArrayType>(),
                    rows,
                    cursor,
                    name,
                    |v| match v {
                        Value::Decimal(unscaled, value_scale) => {
                            decimal_scale_check(*value_scale, scale)?;
                            // Big-endian two's complement, sign extended to
                            // the fixed width the schema declares.
                            Ok(FixedLenByteArray::from(unscaled.to_be_bytes().to_vec()))
                        }
                        _ => Err(decimal_error(v)),
                    },
                )?;
            }
        }
        // Refused in `physical_type`/`build_schema` before any row is read.
        DataType::Interval => unreachable!("interval columns are refused before writing"),
        DataType::Variant => unreachable!("variant columns use write_variant_column"),
        DataType::Array { .. } => unreachable!("ARRAY columns use write_array_column"),
    }
    column.close()?;
    Ok(())
}

fn decimal_scale_check(value_scale: u8, column_scale: u8) -> Result<(), String> {
    if value_scale == column_scale {
        Ok(())
    } else {
        Err(format!(
            "a DECIMAL value with scale {value_scale} cannot be written to a column with \
             scale {column_scale}"
        ))
    }
}

/// Collect one typed column chunk (`rows` values, NULLs as definition level
/// 0) and write it.
fn write_typed<T: ParquetDataType>(
    writer: &mut ColumnWriterImpl<T>,
    rows: usize,
    cursor: &mut ColumnCursor,
    column: &str,
    convert: impl Fn(&Value) -> Result<T::T, String>,
) -> Result<(), WriteError> {
    let mut values = Vec::with_capacity(rows);
    let mut levels = Vec::with_capacity(rows);
    for _ in 0..rows {
        let value = cursor.next();
        if value.is_null() {
            levels.push(0);
        } else {
            match convert(&value) {
                Ok(value) => {
                    levels.push(1);
                    values.push(value);
                }
                Err(message) => {
                    return Err(WriteError::InvalidValue {
                        column: column.to_string(),
                        message,
                    });
                }
            }
        }
    }
    writer.write_batch(&values, Some(&levels), None)?;
    Ok(())
}

fn wrong_type(value: &Value, expected: DataType) -> String {
    format!(
        "expected {expected}, found {}",
        value
            .data_type()
            .map(|t| t.name())
            .unwrap_or_else(|| "NULL".to_string())
    )
}

// ---------------------------------------------------------------------------
// VARIANT: the Parquet `Variant` logical type
// ---------------------------------------------------------------------------

/// The specification version the `Variant` logical type advertises, and the
/// one DuckDB 2.0 writes.
const VARIANT_SPEC_VERSION: i8 = 1;

/// One VARIANT column's Parquet group, per the spec:
///
/// ```text
/// <name> GROUP OPTIONAL (Variant)
///   metadata    BYTE_ARRAY REQUIRED
///   value       BYTE_ARRAY OPTIONAL
///   typed_value GROUP OPTIONAL          -- only when the policy shreds
///     <key> GROUP REQUIRED
///       value       BYTE_ARRAY OPTIONAL
///       typed_value <type>     OPTIONAL
/// ```
fn variant_group(name: &str, policy: &ShredPolicy) -> Result<Type, WriteError> {
    let mut fields = vec![
        Arc::new(variant_leaf("metadata", PhysicalType::BYTE_ARRAY, None)?),
        Arc::new(variant_leaf("value", PhysicalType::BYTE_ARRAY, None)?),
    ];
    if !policy.is_empty() {
        let mut typed = Vec::with_capacity(policy.fields.len());
        for field in &policy.fields {
            // The spec's group shape: a `value` buffer beside `typed_value`.
            // This writer puts every value that is not of the policy's kind
            // (and every explicit null) in the top-level residual object, so
            // the per-key `value` leaf stays empty — but both DuckDB's reader
            // and ours require the leaf to exist.
            let children = vec![
                Arc::new(variant_leaf("value", PhysicalType::BYTE_ARRAY, None)?),
                Arc::new(variant_leaf(
                    "typed_value",
                    typed_physical(field.kind),
                    Some(field.kind),
                )?),
            ];
            typed.push(Arc::new(
                Type::group_type_builder(field.name.as_ref())
                    .with_repetition(Repetition::REQUIRED)
                    .with_fields(children)
                    .build()?,
            ));
        }
        fields.push(Arc::new(
            Type::group_type_builder("typed_value")
                .with_repetition(Repetition::OPTIONAL)
                .with_fields(typed)
                .build()?,
        ));
    }
    Ok(Type::group_type_builder(name)
        .with_repetition(Repetition::OPTIONAL)
        .with_logical_type(Some(LogicalType::variant(Some(VARIANT_SPEC_VERSION))))
        .with_fields(fields)
        .build()?)
}

// ---------------------------------------------------------------------------
// ARRAY: the three-level LIST parquet has
// ---------------------------------------------------------------------------

/// One ARRAY column's Parquet group. Parquet has no fixed-size list type, so
/// this is the standard three-level LIST — the same group DuckDB 2.0 writes
/// for an `ARRAY` (its converted type is `LIST`, with no logical type):
///
/// ```text
/// <name> GROUP OPTIONAL (LIST)
///   list     GROUP REPEATED
///     element <type> OPTIONAL
/// ```
fn array_group(name: &str, elem: ArrayElem, ids: (i32, i32)) -> Result<Type, WriteError> {
    let mut element =
        Type::primitive_type_builder("element", crate::schema_map::array_physical(elem))
            .with_repetition(Repetition::OPTIONAL);
    if ids.1 != 0 {
        element = element.with_id(Some(ids.1));
    }
    let element = element.build()?;
    let list = Type::group_type_builder("list")
        .with_repetition(Repetition::REPEATED)
        .with_fields(vec![Arc::new(element)])
        .build()?;
    let mut group = Type::group_type_builder(name)
        .with_repetition(Repetition::OPTIONAL)
        .with_converted_type(ConvertedType::LIST)
        .with_fields(vec![Arc::new(list)]);
    if ids.0 != 0 {
        group = group.with_id(Some(ids.0));
    }
    Ok(group.build()?)
}

/// Write one ARRAY column chunk: exactly `len` level entries per row, so a
/// reader recovers the length from `num_values` even when the row is NULL.
/// The definition levels are 0 for a NULL array, 2 for a NULL element (the
/// `element` leaf is OPTIONAL) and 3 for a value; the first entry of every row
/// has repetition level 0 and the rest 1.
fn write_array_column<W: std::io::Write + Send>(
    group: &mut SerializedRowGroupWriter<'_, W>,
    dtype: &DataType,
    rows: usize,
    cursor: &mut ColumnCursor,
    name: &str,
) -> Result<(), WriteError> {
    let DataType::Array { elem, len } = *dtype else {
        unreachable!("write_array_column is called for ARRAY columns only")
    };
    let len = len as usize;
    let mut ints: Vec<i32> = Vec::new();
    let mut longs: Vec<i64> = Vec::new();
    let mut doubles: Vec<f64> = Vec::new();
    let mut singles: Vec<f32> = Vec::new();
    let mut levels: Vec<i16> = Vec::with_capacity(rows * len);
    let mut reps: Vec<i16> = Vec::with_capacity(rows * len);
    for _ in 0..rows {
        let value = cursor.next();
        let array = match &value {
            Value::Null => None,
            Value::Array(array) if array.elem == elem && array.len() == len => Some(array),
            other => {
                return Err(WriteError::InvalidValue {
                    column: name.to_string(),
                    message: wrong_type(other, *dtype),
                });
            }
        };
        for slot in 0..len {
            reps.push(if slot == 0 { 0 } else { 1 });
            let Some(element) = array else {
                // Definition level 0: the group itself is NULL, which a
                // parquet reader reports as a NULL list (level 1 would be an
                // empty list).
                levels.push(0);
                continue;
            };
            if !element.is_valid(slot) {
                levels.push(2);
                continue;
            }
            levels.push(3);
            let slot_value = element.elem_value(slot);
            match (elem, &slot_value) {
                (ArrayElem::Integer, Value::Integer(x)) => ints.push(*x),
                (ArrayElem::Bigint, Value::Bigint(x)) => longs.push(*x),
                (ArrayElem::Double, Value::Double(x)) => doubles.push(*x),
                // A FLOAT element is narrowable exactly: the cell form holds
                // it in a `Double`.
                (ArrayElem::Float, Value::Double(x)) => singles.push(*x as f32),
                _ => {
                    return Err(WriteError::InvalidValue {
                        column: name.to_string(),
                        message: wrong_type(&slot_value, elem.dtype()),
                    });
                }
            }
        }
    }
    let mut column = group.next_column()?.ok_or_else(|| skipped(name))?;
    match elem {
        ArrayElem::Integer => {
            column
                .typed::<Int32Type>()
                .write_batch(&ints, Some(&levels), Some(&reps))?
        }
        ArrayElem::Bigint => {
            column
                .typed::<Int64Type>()
                .write_batch(&longs, Some(&levels), Some(&reps))?
        }
        ArrayElem::Double => {
            column
                .typed::<DoubleType>()
                .write_batch(&doubles, Some(&levels), Some(&reps))?
        }
        ArrayElem::Float => {
            column
                .typed::<FloatType>()
                .write_batch(&singles, Some(&levels), Some(&reps))?
        }
    };
    column.close()?;
    Ok(())
}

/// The physical type of one shredded kind.
fn typed_physical(kind: ShredKind) -> PhysicalType {
    match kind {
        ShredKind::Bool => PhysicalType::BOOLEAN,
        ShredKind::Int64 => PhysicalType::INT64,
        ShredKind::Double => PhysicalType::DOUBLE,
        ShredKind::Str => PhysicalType::BYTE_ARRAY,
        ShredKind::Date => PhysicalType::INT32,
        ShredKind::Timestamp => PhysicalType::INT64,
    }
}

/// One leaf of a variant group. `kind` adds the logical annotation a typed
/// leaf needs (STRING, DATE, TIMESTAMP); the byte buffers carry none.
fn variant_leaf(
    name: &str,
    physical: PhysicalType,
    kind: Option<ShredKind>,
) -> Result<Type, WriteError> {
    let mut builder = Type::primitive_type_builder(name, physical)
        // `metadata` is the only REQUIRED leaf; the buffers are optional so a
        // NULL row needs no bytes.
        .with_repetition(if name == "metadata" {
            Repetition::REQUIRED
        } else {
            Repetition::OPTIONAL
        });
    match kind {
        Some(ShredKind::Str) => {
            builder = builder
                .with_converted_type(ConvertedType::UTF8)
                .with_logical_type(Some(LogicalType::String));
        }
        Some(ShredKind::Date) => {
            builder = builder
                .with_converted_type(ConvertedType::DATE)
                .with_logical_type(Some(LogicalType::Date));
        }
        Some(ShredKind::Timestamp) => {
            builder = builder
                .with_converted_type(ConvertedType::TIMESTAMP_MICROS)
                .with_logical_type(Some(LogicalType::timestamp(false, TimeUnit::MICROS)));
        }
        Some(ShredKind::Bool | ShredKind::Int64 | ShredKind::Double) | None => {}
    }
    Ok(builder.build()?)
}

/// Shred the row group's VARIANT values, then write the group's leaves in
/// schema order: `metadata`, `value`, then each shredded key's `value` (always
/// NULL here) and `typed_value`.
fn write_variant_column<W: std::io::Write + Send>(
    group: &mut SerializedRowGroupWriter<'_, W>,
    policy: &ShredPolicy,
    rows: usize,
    cursor: &mut ColumnCursor,
    name: &str,
) -> Result<(), WriteError> {
    let mut values: Vec<Option<Variant>> = Vec::with_capacity(rows);
    for _ in 0..rows {
        match cursor.next() {
            Value::Null => values.push(None),
            Value::Variant(value) => values.push(Some((*value).clone())),
            other => {
                return Err(WriteError::InvalidValue {
                    column: name.to_string(),
                    message: wrong_type(&other, DataType::Variant),
                });
            }
        }
    }

    let mut residuals: Vec<Option<Variant>> = Vec::with_capacity(rows);
    let mut typed: Vec<Vec<Option<Variant>>> = Vec::with_capacity(rows);
    let mut dict: Vec<Arc<str>> = Vec::new();
    for value in &values {
        let Some(value) = value else {
            residuals.push(None);
            typed.push(vec![None; policy.fields.len()]);
            continue;
        };
        let (residual, fields) = shred_row(value, policy);
        if let Some(residual) = &residual {
            collect_keys(residual, &mut dict);
        }
        residuals.push(residual);
        typed.push(fields);
    }
    // One dictionary for the whole column, shared by every row of the group
    // (the plan's "shared per-column key dictionary"). It is complete before
    // any value is encoded, because field ids index into it.
    dict.extend(policy.fields.iter().map(|field| field.name.clone()));
    dict.sort_unstable();
    dict.dedup();
    let metadata = encode_metadata(&dict);
    let keys = decode_metadata(&metadata).map_err(|e| WriteError::InvalidValue {
        column: name.to_string(),
        message: format!("{e}"),
    })?;

    write_variant_metadata(group, &values, &metadata, name)?;
    write_variant_value(group, &values, &residuals, &keys, name)?;
    for (index, field) in policy.fields.iter().enumerate() {
        write_variant_key_value(group, &values, name)?;
        let field_values: Vec<Option<Variant>> =
            typed.iter().map(|row| row[index].clone()).collect();
        write_variant_typed(
            group,
            field.kind,
            &values,
            &field_values,
            name,
            field.name.as_ref(),
        )?;
    }
    Ok(())
}

/// Every object key a value holds, at any depth: nested objects index the same
/// per-column dictionary, so a key missing from it would be unencodable.
fn collect_keys(value: &Variant, out: &mut Vec<Arc<str>>) {
    match value {
        Variant::Object(entries) => {
            for (key, item) in entries.iter() {
                out.push(key.clone());
                collect_keys(item, out);
            }
        }
        Variant::Array(items) => {
            for item in items.iter() {
                collect_keys(item, out);
            }
        }
        _ => {}
    }
}

/// The `metadata` leaf: one dictionary copy per defined row (REQUIRED inside
/// the optional group, so a NULL row is definition level 0).
fn write_variant_metadata<W: std::io::Write + Send>(
    group: &mut SerializedRowGroupWriter<'_, W>,
    values: &[Option<Variant>],
    metadata: &[u8],
    column: &str,
) -> Result<(), WriteError> {
    let mut levels = Vec::with_capacity(values.len());
    let mut batch = Vec::with_capacity(values.len());
    for value in values {
        levels.push(i16::from(u8::from(value.is_some())));
        if value.is_some() {
            batch.push(ByteArray::from(metadata.to_vec()));
        }
    }
    write_leaf::<W, ByteArrayType>(group, &batch, &levels, column)
}

/// The residual `value` leaf: level 0 is a NULL row, 1 a row with nothing
/// left over, 2 the residual object.
fn write_variant_value<W: std::io::Write + Send>(
    group: &mut SerializedRowGroupWriter<'_, W>,
    values: &[Option<Variant>],
    residuals: &[Option<Variant>],
    keys: &[Arc<str>],
    column: &str,
) -> Result<(), WriteError> {
    let mut levels = Vec::with_capacity(values.len());
    let mut batch = Vec::with_capacity(values.len());
    for (index, value) in values.iter().enumerate() {
        match (value, &residuals[index]) {
            (None, _) => levels.push(0),
            (Some(_), None) => levels.push(1),
            (Some(_), Some(residual)) => {
                levels.push(2);
                let mut bytes = Vec::new();
                encode_value(residual, keys, &mut bytes).map_err(|e| WriteError::InvalidValue {
                    column: column.to_string(),
                    message: format!("{e}"),
                })?;
                batch.push(ByteArray::from(bytes));
            }
        }
    }
    write_leaf::<W, ByteArrayType>(group, &batch, &levels, column)
}

/// One shredded key's `typed_value` leaf: level 0 NULL row, 2 absent key,
/// 3 a typed value.
/// A shredded key's `value` leaf: the spec requires the buffer, but this
/// writer never fills it (unshredded values stay in the top-level residual).
fn write_variant_key_value<W: std::io::Write + Send>(
    group: &mut SerializedRowGroupWriter<'_, W>,
    rows: &[Option<Variant>],
    column: &str,
) -> Result<(), WriteError> {
    // Definition levels: 0 is a SQL NULL row, 1 a row whose value is not an
    // object (nothing is shredded, so `typed_value` is absent and the whole
    // value sits in `value`), 2 a shredded object row with this leaf unset.
    let levels: Vec<i16> = rows
        .iter()
        .map(|row| match row {
            None => 0,
            Some(Variant::Object(_)) => 2,
            Some(_) => 1,
        })
        .collect();
    write_leaf::<W, ByteArrayType>(group, &[], &levels, column)
}

fn write_variant_typed<W: std::io::Write + Send>(
    group: &mut SerializedRowGroupWriter<'_, W>,
    kind: ShredKind,
    rows: &[Option<Variant>],
    values: &[Option<Variant>],
    column: &str,
    field: &str,
) -> Result<(), WriteError> {
    // Both buffers are `(definition levels, values)`; the levels are the same
    // for every kind, only the value representation differs.
    let levels = |values: &[Option<Variant>]| -> Vec<i16> {
        rows.iter()
            .zip(values)
            .map(|(row, value)| match row {
                // Only an object is shredded: a scalar or array row keeps its
                // whole value in `value` with `typed_value` absent, which is
                // what the spec (and DuckDB's reader) requires of a
                // partially-shredded column.
                None => 0,
                Some(Variant::Object(_)) if value.is_some() => 3,
                Some(Variant::Object(_)) => 2,
                Some(_) => 1,
            })
            .collect()
    };
    let levels = levels(values);
    let wrong = |value: &Variant| WriteError::InvalidValue {
        column: column.to_string(),
        message: format!(
            "shredded field \"{field}\" holds {} but its column is {}",
            value.type_name(),
            kind.name()
        ),
    };
    match kind {
        ShredKind::Bool => {
            let mut batch = Vec::new();
            for row in rows.iter().zip(values) {
                if let (Some(_), Some(value)) = row {
                    match value {
                        Variant::Bool(b) => batch.push(*b),
                        other => return Err(wrong(other)),
                    }
                }
            }
            write_leaf::<W, BoolType>(group, &batch, &levels, column)
        }
        ShredKind::Int64 => {
            let mut batch = Vec::new();
            for (row, value) in rows.iter().zip(values) {
                if let (Some(_), Some(value)) = (row, value) {
                    match value {
                        Variant::Int(i) => batch.push(*i),
                        other => return Err(wrong(other)),
                    }
                }
            }
            write_leaf::<W, Int64Type>(group, &batch, &levels, column)
        }
        ShredKind::Double => {
            let mut batch = Vec::new();
            for (row, value) in rows.iter().zip(values) {
                if let (Some(_), Some(value)) = (row, value) {
                    match value {
                        Variant::Double(x) => batch.push(*x),
                        other => return Err(wrong(other)),
                    }
                }
            }
            write_leaf::<W, DoubleType>(group, &batch, &levels, column)
        }
        ShredKind::Str => {
            let mut batch = Vec::new();
            for (row, value) in rows.iter().zip(values) {
                if let (Some(_), Some(value)) = (row, value) {
                    match value {
                        Variant::Str(s) => batch.push(ByteArray::from(s.as_bytes().to_vec())),
                        other => return Err(wrong(other)),
                    }
                }
            }
            write_leaf::<W, ByteArrayType>(group, &batch, &levels, column)
        }
        ShredKind::Date => {
            let mut batch = Vec::new();
            for (row, value) in rows.iter().zip(values) {
                if let (Some(_), Some(value)) = (row, value) {
                    match value {
                        Variant::Date(d) => batch.push(*d),
                        other => return Err(wrong(other)),
                    }
                }
            }
            write_leaf::<W, Int32Type>(group, &batch, &levels, column)
        }
        ShredKind::Timestamp => {
            let mut batch = Vec::new();
            for (row, value) in rows.iter().zip(values) {
                if let (Some(_), Some(value)) = (row, value) {
                    match value {
                        Variant::Timestamp(us) => batch.push(*us),
                        other => return Err(wrong(other)),
                    }
                }
            }
            write_leaf::<W, Int64Type>(group, &batch, &levels, column)
        }
    }
}

/// Write one leaf chunk of a variant group and close it.
fn write_leaf<W: std::io::Write + Send, T: ParquetDataType>(
    group: &mut SerializedRowGroupWriter<'_, W>,
    values: &[T::T],
    levels: &[i16],
    column: &str,
) -> Result<(), WriteError> {
    let mut writer = group.next_column()?.ok_or_else(|| skipped(column))?;
    writer
        .typed::<T>()
        .write_batch(values, Some(levels), None)?;
    writer.close()?;
    Ok(())
}
