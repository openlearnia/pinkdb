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
    FixedLenByteArrayType, Int32Type, Int64Type,
};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::{SerializedColumnWriter, SerializedFileWriter};
use parquet::schema::types::Type;
use pinkdb::{DataTable, DataType, Value, Vector};

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
    write_table_with_key_values(table, Vec::new())
}

/// Encode `table` as one complete parquet file and record the lake identity
/// (`db` / `table`) in the footer, so the lake's loader can verify that a
/// data file holds the catalog's table (parquet itself has no table name).
pub fn write_table_with_identity(
    src: &DataTable,
    db: &str,
    table: &str,
) -> Result<Vec<u8>, WriteError> {
    write_table_with_key_values(
        src,
        vec![
            KeyValue::new(DB_METADATA_KEY.to_string(), Some(db.to_string())),
            KeyValue::new(TABLE_METADATA_KEY.to_string(), Some(table.to_string())),
        ],
    )
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

fn write_table_with_key_values(
    table: &DataTable,
    key_values: Vec<KeyValue>,
) -> Result<Vec<u8>, WriteError> {
    let schema = table.schema();
    let root = build_schema(&schema)?;
    let mut builder = WriterProperties::builder()
        // Chunk statistics travel in the footer (min/max/NULL count), so the
        // file is self-describing for other readers and for pruning.
        .set_statistics_enabled(EnabledStatistics::Chunk)
        // Uncompressed is available in every build of the `parquet` crate,
        // independent of the optional codec features.
        .set_compression(Compression::UNCOMPRESSED);
    if !key_values.is_empty() {
        builder = builder.set_key_value_metadata(Some(key_values));
    }
    let properties = Arc::new(builder.build());

    // A cursor per column, kept across row groups so encoding is one linear
    // pass over the table (no per-row `DataTable::get` chunk walk).
    let mut cursors: Vec<ColumnCursor<'_>> = table
        .columns
        .iter()
        .map(|column| ColumnCursor {
            chunks: &column.chunks,
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
                let column = group
                    .next_column()?
                    .ok_or_else(|| WriteError::InvalidValue {
                        column: name.clone(),
                        message: "the parquet writer skipped a schema column".to_string(),
                    })?;
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
struct ColumnCursor<'a> {
    chunks: &'a [Arc<Vector>],
    chunk: usize,
    offset: usize,
}

impl ColumnCursor<'_> {
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
    })
}

/// The flat parquet schema: every column OPTIONAL (definition levels carry
/// the NULLs), with the logical annotation the existing reader maps back.
fn build_schema(schema: &[(String, DataType)]) -> Result<Type, WriteError> {
    let mut fields = Vec::with_capacity(schema.len());
    for (name, dtype) in schema {
        let physical = physical_type(*dtype, name)?;
        let mut builder =
            Type::primitive_type_builder(name, physical).with_repetition(Repetition::OPTIONAL);
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
            DataType::Interval => unreachable!("refused by physical_type"),
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
    cursor: &mut ColumnCursor<'_>,
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
    cursor: &mut ColumnCursor<'_>,
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
