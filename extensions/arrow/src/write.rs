//! Writing Arrow IPC **file** format (`ARROW1` magic + footer).
//!
//! One record batch per engine chunk (≤ 2048 rows — DuckDB's vector size),
//! uncompressed (pyarrow's and DuckDB's default). Writing is a two-pass
//! operation: the file is created first, so a type the format cannot express
//! fails at the schema step before any byte reaches the target.

use std::io::BufWriter;
use std::path::Path;

use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
    IntervalMonthDayNanoArray, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_buffer::{Buffer, IntervalMonthDayNano as ArrowMonthDayNano, NullBuffer, ScalarBuffer};
use arrow_ipc::writer::FileWriter;
use arrow_schema::Schema;
use pinkdb::types::Interval;
use pinkdb::types::Result;
use pinkdb::vector::{DataChunk, Vector, VectorData};
use pinkdb::{DataTable, PinkDbError};

use crate::types::arrow_field;

/// Write `table` to `path` (created or truncated), uncompressed.
pub fn write(path: &Path, table: &DataTable) -> Result<()> {
    let fields: Vec<_> = table
        .schema()
        .into_iter()
        .map(|(name, dtype)| arrow_field(&name, dtype))
        .collect::<Result<_>>()?;
    let schema = Schema::new(fields);
    // M-3: never `File::create` at the client-named path. The shared helper
    // writes a `create_new` temp sibling (0600), refuses a symlinked target,
    // and atomically renames it into place.
    pinkdb::write_atomic_private(path, |file| {
        let mut writer =
            FileWriter::try_new_buffered(BufWriter::new(file), &schema).map_err(|e| {
                PinkDbError::Execution(format!("cannot write \"{}\": {e}", path.display()))
            })?;
        for i in 0..table.num_chunks() {
            let Some(chunk) = table.scan_chunk(i) else {
                continue;
            };
            let batch = build_batch(&schema, &chunk)?;
            writer.write(&batch).map_err(|e| {
                PinkDbError::Execution(format!("cannot write \"{}\": {e}", path.display()))
            })?;
        }
        writer.finish().map_err(|e| {
            PinkDbError::Execution(format!("cannot finish \"{}\": {e}", path.display()))
        })
    })
}

/// One engine chunk as one record batch (columns in schema order).
fn build_batch(schema: &Schema, chunk: &DataChunk) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(chunk.columns.len());
    for (i, column) in chunk.columns.iter().enumerate() {
        let field = schema.field(i);
        arrays.push(to_array(column, field.data_type())?);
    }
    RecordBatch::try_new(std::sync::Arc::new(schema.clone()), arrays)
        .map_err(|e| PinkDbError::Execution(format!("cannot build an arrow record batch: {e}")))
}

/// The Arrow null buffer for a vector, or `None` when every row is valid
/// (the all-valid fast path arrow prefers).
fn nulls_from(v: &Vector) -> Option<NullBuffer> {
    let len = v.len();
    if (0..len).all(|i| v.is_valid(i)) {
        return None;
    }
    Some(NullBuffer::new(arrow_buffer::BooleanBuffer::new(
        Buffer::from_slice_ref(v.validity_words()),
        0,
        len,
    )))
}

/// The per-row `Option` view for the variable-width / interval layouts that
/// have no bulk constructor.
fn to_array(v: &Vector, arrow: &arrow_schema::DataType) -> Result<ArrayRef> {
    use arrow_schema::DataType as ArrowType;
    let nulls = nulls_from(v);
    let array: ArrayRef = match (v.data(), arrow) {
        (VectorData::Boolean(values), ArrowType::Boolean) => {
            let items: Vec<Option<bool>> = (0..v.len())
                .map(|i| v.is_valid(i).then(|| values[i]))
                .collect();
            std::sync::Arc::new(BooleanArray::from(items))
        }
        (VectorData::Int32(values), ArrowType::Int32) => {
            std::sync::Arc::new(Int32Array::new(ScalarBuffer::from(values.clone()), nulls))
        }
        (VectorData::Int64(values), ArrowType::Int64) => {
            std::sync::Arc::new(Int64Array::new(ScalarBuffer::from(values.clone()), nulls))
        }
        (VectorData::Float64(values), ArrowType::Float64) => {
            std::sync::Arc::new(Float64Array::new(ScalarBuffer::from(values.clone()), nulls))
        }
        (VectorData::VarChar(values), ArrowType::Utf8) => {
            let items: Vec<Option<&str>> = (0..v.len())
                .map(|i| v.is_valid(i).then(|| values[i].as_ref()))
                .collect();
            std::sync::Arc::new(StringArray::from(items))
        }
        (VectorData::Date32(values), ArrowType::Date32) => {
            std::sync::Arc::new(Date32Array::new(ScalarBuffer::from(values.clone()), nulls))
        }
        (VectorData::TimestampMicros(values), ArrowType::Timestamp(_, _)) => std::sync::Arc::new(
            TimestampMicrosecondArray::new(ScalarBuffer::from(values.clone()), nulls),
        ),
        (VectorData::Decimal { values, scale, .. }, ArrowType::Decimal128(p, s)) => {
            debug_assert_eq!(*scale as i8, *s);
            let arr = Decimal128Array::new(ScalarBuffer::from(values.clone()), nulls)
                .with_precision_and_scale(*p, *s)
                .map_err(|e| {
                    PinkDbError::Execution(format!("cannot write DECIMAL({p},{s}): {e}"))
                })?;
            std::sync::Arc::new(arr)
        }
        (VectorData::Interval(values), ArrowType::Interval(_)) => {
            let items: Vec<Option<ArrowMonthDayNano>> = values
                .iter()
                .enumerate()
                .map(|(i, iv): (usize, &Interval)| {
                    if !v.is_valid(i) {
                        return Ok(None);
                    }
                    Ok(Some(ArrowMonthDayNano::new(
                        iv.months,
                        iv.days,
                        interval_nanos(iv)?,
                    )))
                })
                .collect::<Result<_>>()?;
            std::sync::Arc::new(IntervalMonthDayNanoArray::from_iter(items))
        }
        (_, arrow) => {
            return Err(PinkDbError::Type(format!(
                "internal: no writer conversion for arrow type {arrow}"
            )));
        }
    };
    Ok(array)
}

/// Interval microseconds as Arrow's nanoseconds — exact or a clean error
/// (Arrow's month-day-nano interval is the only layout that round-trips
/// DuckDB's three fields, and its nanosecond range bounds the day part).
fn interval_nanos(iv: &Interval) -> Result<i64> {
    iv.micros.checked_mul(1_000).ok_or_else(|| {
        PinkDbError::Type(format!(
            "interval micros {} do not fit arrow's nanosecond interval",
            iv.micros
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_micros_scale_to_nanos_exactly() {
        let iv = Interval {
            months: 2,
            days: -3,
            micros: 1_500_000,
        };
        assert_eq!(interval_nanos(&iv).unwrap(), 1_500_000_000);
        let huge = Interval {
            months: 0,
            days: 0,
            micros: i64::MAX,
        };
        let err = interval_nanos(&huge).unwrap_err().to_string();
        assert!(err.contains("nanosecond interval"), "{err}");
    }
}
