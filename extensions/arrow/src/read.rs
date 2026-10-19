//! Reading Arrow IPC **file** format (`ARROW1` magic + footer).
//!
//! The reader accepts any record-batch sizes and re-chunks the rows into
//! engine chunks of `CHUNK_SIZE`; every column is decoded into a typed engine
//! vector (a slice copy for the fixed-width layouts whose representation
//! matches the engine's, a per-value extraction loop for the ones that do
//! not). A file is fully materialized in memory before the scan produces
//! rows — projection pushdown, predicate pushdown and streaming are
//! documented follow-ups.

use std::fs::File;
#[cfg(feature = "remote")]
use std::io::Cursor;
use std::io::{BufReader, Read, Seek};
use std::path::Path;
use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float16Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeStringArray, RecordBatch, StringArray,
    StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
};
use arrow_buffer::NullBuffer;
use arrow_ipc::reader::FileReader;
use arrow_schema::{DataType as ArrowType, TimeUnit};
use pinkdb::types::Interval;
use pinkdb::types::Result;
use pinkdb::vector::{CHUNK_SIZE, Vector, VectorData};
use pinkdb::{DataTable, DataType, PinkDbError};

use crate::types::{date64_to_days, engine_schema, interval_fields, timestamp_to_micros};

/// The IPC file format's magic bytes (the first six bytes of an IPC file).
const FILE_MAGIC: &[u8; 6] = b"ARROW1";

/// The IPC *stream* format's continuation marker: stream files begin with
/// `0xFFFFFFFF` and carry no footer. Detected only to refuse them precisely.
const STREAM_MARKER: [u8; 4] = [0xFF, 0xFF, 0xFF, 0xFF];

/// The file's schema (footer only; no column data decoded).
pub fn schema(path: &Path) -> Result<Vec<(String, DataType)>> {
    schema_of(open(path)?, &path.display().to_string())
}

/// The whole file as an engine table, named after the file stem.
pub fn read(path: &Path) -> Result<DataTable> {
    let name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("arrow")
        .to_string();
    read_of(open(path)?, &path.display().to_string(), name)
}

/// A remote IPC object's schema (whole object; IPC has no ranged footer
/// protocol — see [`crate::remote`]).
#[cfg(feature = "remote")]
pub fn schema_bytes(bytes: &[u8], label: &str) -> Result<Vec<(String, DataType)>> {
    check_magic(bytes, label)?;
    schema_of(Cursor::new(bytes), label)
}

/// A remote IPC object as an engine table (whole object materialized).
#[cfg(feature = "remote")]
pub fn read_bytes(bytes: &[u8], label: &str, name: String) -> Result<DataTable> {
    check_magic(bytes, label)?;
    read_of(Cursor::new(bytes), label, name)
}

fn schema_of<R: Read + Seek>(reader: R, label: &str) -> Result<Vec<(String, DataType)>> {
    let reader = FileReader::try_new_buffered(reader, None).map_err(|e| format_error(label, &e))?;
    engine_schema(reader.schema().as_ref())
}

fn read_of<R: Read + Seek>(reader: R, label: &str, name: String) -> Result<DataTable> {
    let reader = FileReader::try_new_buffered(reader, None).map_err(|e| format_error(label, &e))?;
    let schema = reader.schema();
    let columns = engine_schema(schema.as_ref())?;
    let mut table = DataTable::new(name, columns)?;
    for batch in reader {
        let batch = batch.map_err(|e| format_error(label, &e))?;
        append_batch(&mut table, &batch, label)?;
    }
    Ok(table)
}

/// The IPC *file* magic check, shared by the local and remote readers, so a
/// stream file (or a non-Arrow file) gets a precise message instead of
/// arrow-ipc's footer error.
fn check_magic(bytes: &[u8], label: &str) -> Result<()> {
    if bytes.len() < 6 || &bytes[..6] != FILE_MAGIC {
        if bytes.len() >= 4 && bytes[..4] == STREAM_MARKER {
            return Err(PinkDbError::Execution(format!(
                "\"{label}\" is an Arrow IPC stream, not an IPC file (no ARROW1 magic); \
                 the arrow extension reads/writes the IPC file format \
                 (`ipc.write_file`; convert a stream with pyarrow's `ipc.open_file`)"
            )));
        }
        return Err(PinkDbError::Execution(format!(
            "\"{label}\" is not an Arrow IPC file (missing the ARROW1 magic)"
        )));
    }
    Ok(())
}

/// Open the file and check the IPC *file* magic up front.
fn open(path: &Path) -> Result<BufReader<File>> {
    let file = File::open(path)
        .map_err(|e| PinkDbError::Execution(format!("cannot open \"{}\": {e}", path.display())))?;
    let mut probe = BufReader::new(file);
    let mut magic = [0u8; 6];
    let read = probe.read(&mut magic).unwrap_or(0);
    check_magic(&magic[..read], &path.display().to_string())?;
    // The probe read only checks; arrow-ipc starts from offset 0.
    let file = File::open(path)
        .map_err(|e| PinkDbError::Execution(format!("cannot open \"{}\": {e}", path.display())))?;
    Ok(BufReader::new(file))
}

fn format_error(label: &str, e: &arrow_schema::ArrowError) -> PinkDbError {
    PinkDbError::Execution(format!("cannot read \"{label}\": {e}"))
}

/// Convert one record batch and append it as engine chunks (≤ [`CHUNK_SIZE`]
/// rows each).
fn append_batch(table: &mut DataTable, batch: &RecordBatch, label: &str) -> Result<()> {
    let rows = batch.num_rows();
    if rows == 0 {
        return Ok(());
    }
    let dtypes: Vec<DataType> = table.schema().into_iter().map(|(_, t)| t).collect();
    if batch.num_columns() != dtypes.len() {
        return Err(PinkDbError::Execution(format!(
            "cannot read \"{label}\": record batch has {} columns but the file schema has {}",
            batch.num_columns(),
            dtypes.len()
        )));
    }
    let mut start = 0;
    while start < rows {
        let end = (start + CHUNK_SIZE).min(rows);
        let mut chunk = Vec::with_capacity(dtypes.len());
        for (col, dtype) in dtypes.iter().enumerate() {
            chunk.push(convert_column(
                batch.column(col).as_ref(),
                *dtype,
                start,
                end,
            )?);
        }
        table.append_chunk(chunk)?;
        start = end;
    }
    Ok(())
}

/// Validity words for `[start, end)` of an array: all-ones when the array has
/// no nulls, else one bit per row.
fn null_words(nulls: Option<&NullBuffer>, start: usize, end: usize) -> Vec<u64> {
    let n = end - start;
    let mut words = vec![0u64; n.div_ceil(64).max(1)];
    match nulls {
        None => words.fill(u64::MAX),
        Some(nb) => {
            for i in 0..n {
                if nb.is_valid(start + i) {
                    words[i / 64] |= 1u64 << (i % 64);
                }
            }
        }
    }
    words
}

/// Build a vector from per-row extraction closures: `is_valid(i)` and
/// `value(i)` apply to the *current chunk's* row indices (`0..n`), and
/// `wrap` assembles the `VectorData` (the engine's payload plus its validity
/// words). The placeholder a masked-out NULL row pushes is unobservable
/// (`Vector::get` never reads an invalid slot), so it can be any default.
fn build_column<T>(
    n: usize,
    is_valid: impl Fn(usize) -> bool,
    placeholder: impl Fn() -> T,
    value: impl Fn(usize) -> Result<T>,
    wrap: impl Fn(Vec<T>, Vec<u64>) -> Vector,
) -> Result<Vector> {
    let mut values = Vec::with_capacity(n);
    let mut words = vec![0u64; n.div_ceil(64).max(1)];
    for i in 0..n {
        if is_valid(i) {
            words[i / 64] |= 1u64 << (i % 64);
            values.push(value(i)?);
        } else {
            values.push(placeholder());
        }
    }
    Ok(wrap(values, words))
}

/// Downcast + copy a fixed-width slice of an array into an engine vector
/// whose representation matches the Arrow layout exactly.
macro_rules! prim {
    ($array:expr, $ty:ty, $variant:ident, $start:expr, $end:expr) => {{
        let a = $array
            .as_any()
            .downcast_ref::<$ty>()
            .expect("array downcast matches its data type");
        Vector::from_raw(
            VectorData::$variant(a.values()[$start..$end].to_vec()),
            null_words(a.nulls(), $start, $end),
        )
    }};
}

/// Convert `[start, end)` of one arrow array into an engine vector of
/// `dtype` (the schema mapping already validated the pairing).
fn convert_column(array: &dyn Array, dtype: DataType, start: usize, end: usize) -> Result<Vector> {
    let n = end - start;
    match (array.data_type(), dtype) {
        (ArrowType::Boolean, DataType::Boolean) => {
            let a = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean array");
            build_column(
                n,
                |i| a.is_valid(start + i),
                || false,
                |i| Ok(a.value(start + i)),
                |v, w| Vector::from_raw(VectorData::Boolean(v), w),
            )
        }
        (ArrowType::Int8, _) => prim_i32!(array, Int8Array, start, end),
        (ArrowType::Int16, _) => prim_i32!(array, Int16Array, start, end),
        (ArrowType::Int32, _) => Ok(prim!(array, Int32Array, Int32, start, end)),
        (ArrowType::Int64, _) => Ok(prim!(array, Int64Array, Int64, start, end)),
        (ArrowType::UInt8, _) => prim_i32!(array, UInt8Array, start, end),
        (ArrowType::UInt16, _) => prim_i32!(array, UInt16Array, start, end),
        (ArrowType::UInt32, _) => {
            let a = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("uint32 array");
            build_column(
                n,
                |i| a.is_valid(start + i),
                || 0,
                |i| Ok(i64::from(a.value(start + i))),
                |v, w| Vector::from_raw(VectorData::Int64(v), w),
            )
        }
        (ArrowType::Float16, _) => {
            let a = array
                .as_any()
                .downcast_ref::<Float16Array>()
                .expect("float16 array");
            build_column(
                n,
                |i| a.is_valid(start + i),
                || 0.0,
                |i| Ok(a.value(start + i).to_f64()),
                |v, w| Vector::from_raw(VectorData::Float64(v), w),
            )
        }
        (ArrowType::Float32, _) => {
            let a = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("float32 array");
            build_column(
                n,
                |i| a.is_valid(start + i),
                || 0.0,
                |i| Ok(f64::from(a.value(start + i))),
                |v, w| Vector::from_raw(VectorData::Float64(v), w),
            )
        }
        (ArrowType::Float64, _) => Ok(prim!(array, Float64Array, Float64, start, end)),
        (ArrowType::Utf8, DataType::Varchar) => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 array");
            str_column(a.nulls(), start, end, |i| a.value(i))
        }
        (ArrowType::LargeUtf8, DataType::Varchar) => {
            let a = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("large utf8 array");
            str_column(a.nulls(), start, end, |i| a.value(i))
        }
        (ArrowType::Utf8View, DataType::Varchar) => {
            let a = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("utf8 view array");
            str_column(a.nulls(), start, end, |i| a.value(i))
        }
        (ArrowType::Date32, DataType::Date) => Ok(prim!(array, Date32Array, Date32, start, end)),
        (ArrowType::Date64, DataType::Date) => {
            let a = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .expect("date64 array");
            build_column(
                n,
                |i| a.is_valid(start + i),
                || 0,
                |i| date64_to_days(a.value(start + i)),
                |v, w| Vector::from_raw(VectorData::Date32(v), w),
            )
        }
        (ArrowType::Timestamp(unit, _), DataType::Timestamp) => {
            let unit = *unit;
            let (values, nulls) = timestamp_values(array, unit);
            build_column(
                n,
                |i| nulls.is_none_or(|nb| nb.is_valid(start + i)),
                || 0,
                |i| timestamp_to_micros(values[start + i], unit),
                |v, w| Vector::from_raw(VectorData::TimestampMicros(v), w),
            )
        }
        (ArrowType::Interval(unit), DataType::Interval) => {
            let unit = *unit;
            let raw = interval_values(array, unit);
            let valid = interval_validity(array);
            build_column(
                n,
                |i| valid.is_none_or(|nb| nb.is_valid(start + i)),
                Interval::default,
                |i| {
                    let (m, d, ns) = raw(start + i);
                    let (months, days, micros) = interval_fields(m, d, ns);
                    Ok(Interval {
                        months,
                        days,
                        micros,
                    })
                },
                |v, w| Vector::from_raw(VectorData::Interval(v), w),
            )
        }
        (ArrowType::Decimal128(_, _), DataType::Decimal { precision, scale }) => {
            let a = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("decimal128 array");
            decimal_column(n, a.nulls(), start, precision, scale, |i| a.value(i))
        }
        (ArrowType::Decimal64(_, _), DataType::Decimal { precision, scale }) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::Decimal64Array>()
                .expect("decimal64 array");
            decimal_column(n, a.nulls(), start, precision, scale, |i| {
                i128::from(a.value(i))
            })
        }
        (ArrowType::Decimal32(_, _), DataType::Decimal { precision, scale }) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow_array::Decimal32Array>()
                .expect("decimal32 array");
            decimal_column(n, a.nulls(), start, precision, scale, |i| {
                i128::from(a.value(i))
            })
        }
        (arrow, _) => Err(PinkDbError::Type(format!(
            "internal: no reader conversion for arrow type {arrow}"
        ))),
    }
}

/// `Int8`/`Int16`/`UInt8`/`UInt16` widen into `INTEGER`.
macro_rules! prim_i32 {
    ($array:expr, $ty:ty, $start:expr, $end:expr) => {{
        let a = $array
            .as_any()
            .downcast_ref::<$ty>()
            .expect("array downcast matches its data type");
        build_column(
            $end - $start,
            |i| a.is_valid($start + i),
            || 0,
            |i| Ok(i32::from(a.value($start + i))),
            |v, w| Vector::from_raw(VectorData::Int32(v), w),
        )
    }};
}
use prim_i32;

/// One `Arc<str>` per value for the three string layouts.
fn str_column<'a>(
    nulls: Option<&NullBuffer>,
    start: usize,
    end: usize,
    value: impl Fn(usize) -> &'a str,
) -> Result<Vector> {
    let n = end - start;
    build_column(
        n,
        |i| nulls.is_none_or(|nb| nb.is_valid(start + i)),
        || Arc::from(""),
        |i| Ok(Arc::from(value(start + i))),
        |v, w| Vector::from_raw(VectorData::VarChar(v), w),
    )
}

/// The unscaled values of one decimal column, with the column's declared
/// precision/scale (the engine stores `(i128, precision, scale)` in the
/// payload, from which the vector's dtype is reconstructed).
fn decimal_column(
    n: usize,
    nulls: Option<&NullBuffer>,
    start: usize,
    precision: u8,
    scale: u8,
    value: impl Fn(usize) -> i128,
) -> Result<Vector> {
    build_column(
        n,
        |i| nulls.is_none_or(|nb| nb.is_valid(start + i)),
        || 0i128,
        |i| Ok(value(start + i)),
        |v, w| {
            Vector::from_raw(
                VectorData::Decimal {
                    values: v,
                    precision,
                    scale,
                },
                w,
            )
        },
    )
}

/// The i64 values and nulls of one of the four timestamp layouts.
fn timestamp_values(array: &dyn Array, unit: TimeUnit) -> (&[i64], Option<&NullBuffer>) {
    let a = array.as_any();
    match unit {
        TimeUnit::Second => {
            let a = a
                .downcast_ref::<TimestampSecondArray>()
                .expect("timestamp-second array");
            (a.values(), a.nulls())
        }
        TimeUnit::Millisecond => {
            let a = a
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("timestamp-millisecond array");
            (a.values(), a.nulls())
        }
        TimeUnit::Microsecond => {
            let a = a
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp-microsecond array");
            (a.values(), a.nulls())
        }
        TimeUnit::Nanosecond => {
            let a = a
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("timestamp-nanosecond array");
            (a.values(), a.nulls())
        }
    }
}

/// `(months, days, nanoseconds)` extractor for the three interval layouts.
fn interval_values(
    array: &dyn Array,
    unit: arrow_schema::IntervalUnit,
) -> Box<dyn Fn(usize) -> (i32, i32, i64) + '_> {
    match unit {
        arrow_schema::IntervalUnit::YearMonth => {
            let a = array
                .as_any()
                .downcast_ref::<IntervalYearMonthArray>()
                .expect("year-month array");
            Box::new(move |i| (a.value(i), 0, 0))
        }
        arrow_schema::IntervalUnit::DayTime => {
            let a = array
                .as_any()
                .downcast_ref::<IntervalDayTimeArray>()
                .expect("day-time array");
            Box::new(move |i| {
                let v = a.value(i);
                (0, v.days, i64::from(v.milliseconds) * 1_000)
            })
        }
        arrow_schema::IntervalUnit::MonthDayNano => {
            let a = array
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .expect("month-day-nano array");
            Box::new(move |i| {
                let v = a.value(i);
                (v.months, v.days, v.nanoseconds)
            })
        }
    }
}

fn interval_validity(array: &dyn Array) -> Option<&NullBuffer> {
    array.nulls()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pinkdb::types::Value;

    #[test]
    fn null_slots_are_masked_in_every_layout() {
        let v = Vector::from_raw(
            VectorData::Int64(vec![1, 999, 3]),
            null_words(Some(&NullBuffer::from(vec![true, false, true])), 0, 3),
        );
        assert_eq!(v.get(1), Value::Null);
        assert!(v.is_valid(0) && v.is_valid(2));
    }

    #[test]
    fn null_words_cover_the_range() {
        let nb = NullBuffer::from(vec![true, false, true, true, false]);
        let words = null_words(Some(&nb), 1, 5);
        // Rows 1..5 = [false, true, true, false].
        assert_eq!(words, vec![0b0110]);
    }
}
