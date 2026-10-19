//! The scan itself: statistics-based row-group pruning, decoding, and the
//! chunk assembly the engine consumes.
//!
//! ## Pushdown
//!
//! The engine hands the scan the statement's projection (a column set) and
//! the `WHERE` conjuncts it could prove over the scan. Both are used:
//!
//! - **Projection**: only the named output columns are decoded. Every other
//!   column is a zero-length placeholder in the emitted chunks, exactly like
//!   an attached-lake scan's absent columns.
//! - **Filter**: for each row group, a column chunk's min/max and NULL count
//!   decide whether any row can match. `col = v` prunes when `v` lies outside
//!   `[min, max]`; `<`, `<=`, `>`, `>=` prune against the bound the
//!   comparison needs; `IS NULL` prunes when the null count is 0; `IS NOT
//!   NULL` when it covers every row. `<>` prunes only when `min == max == v`
//!   (and never for float columns: NaN). A column whose statistics are not in
//!   the engine's order (unsigned integers) or missing contributes no
//!   pruning — pruning is never allowed to drop a row the filter would keep,
//!   and a wrong bound could.
//! - **File** level: a predicate over a *constant* column (a hive key, the
//!   file name) is evaluated per file, so `WHERE region = 'eu'` touches only
//!   the matching files.
//!
//! `filters` are hints: the engine re-applies the full predicate above the
//! scan, so ignoring one (or all) changes only how much is decoded. The
//! counters ([`TableScanOutput`]) report what actually happened.

use std::sync::Arc;

use parquet::column::reader::{ColumnReader, get_typed_column_reader};
use parquet::data_type::{
    BoolType, ByteArrayType, DoubleType, FixedLenByteArrayType, FloatType, Int32Type, Int64Type,
    Int96Type,
};
use parquet::file::reader::{ChunkReader, FileReader, RowGroupReader, SerializedFileReader};
use pinkdb::attached::{PruneOp, PrunePredicate};
use pinkdb::{
    CHUNK_SIZE, DataChunk, DataType, PinkDbError, Result, TableScanOutput, Value, Vector,
};

use crate::resolve::{FileResolved, OutKind, Resolved, convert_value, file_constant_value};
use crate::schema_map::FileColumn;

/// Execute one resolved scan.
pub fn scan(
    resolved: &Resolved,
    projection: Option<&[usize]>,
    filters: &[PrunePredicate],
) -> Result<TableScanOutput> {
    let width = resolved.schema.len();
    // Which output columns to decode. An empty bound still needs one column
    // decoded: the chunk shape is full-width, and a chunk with every column
    // absent would carry rows nothing proves.
    let mut needed = vec![false; width];
    match projection {
        Some(cols) => {
            for &c in cols {
                if c < width {
                    needed[c] = true;
                }
            }
            if !needed.iter().any(|n| *n) && width > 0 {
                needed[0] = true;
            }
        }
        None => needed.iter_mut().for_each(|n| *n = true),
    }

    let mut out = TableScanOutput::default();
    let mut chunks: Vec<DataChunk> = Vec::new();
    // One shared placeholder per absent output column.
    let placeholders: Vec<Option<Arc<Vector>>> = (0..width)
        .map(|i| (!needed[i]).then(|| Arc::new(Vector::new_empty(resolved.schema[i].1))))
        .collect();

    let plan = DecodePlan {
        resolved,
        filters,
        needed,
        placeholders,
    };

    for file in &resolved.files {
        if !file_survives(resolved, file, filters) {
            continue;
        }
        out.files += 1;
        // Remote objects are read through parquet's `ChunkReader` over the
        // shared transport (ranged footer/row groups); local files keep the
        // page cache.
        #[cfg(feature = "remote")]
        if let Some(spec) = &file.remote {
            let reader = SerializedFileReader::new(crate::remote::RemoteFile::from_spec(spec))
                .map_err(|e| {
                    PinkDbError::Execution(format!(
                        "cannot read parquet file \"{}\": {e}",
                        file.meta.path
                    ))
                })?;
            scan_reader(&reader, file, &plan, &mut out, &mut chunks)?;
            continue;
        }
        let reader =
            SerializedFileReader::new(std::fs::File::open(&file.meta.path).map_err(|e| {
                PinkDbError::Io(std::io::Error::new(
                    e.kind(),
                    format!("cannot open \"{}\": {e}", file.meta.path),
                ))
            })?)
            .map_err(|e| {
                PinkDbError::Execution(format!(
                    "cannot read parquet file \"{}\": {e}",
                    file.meta.path
                ))
            })?;

        scan_reader(&reader, file, &plan, &mut out, &mut chunks)?;
    }
    out.chunks = chunks;
    Ok(out)
}

/// The per-statement decode plan the reader helper consumes.
struct DecodePlan<'a> {
    resolved: &'a Resolved,
    filters: &'a [PrunePredicate],
    needed: Vec<bool>,
    placeholders: Vec<Option<Arc<Vector>>>,
}

/// Decode one opened file (local `File` or remote `RemoteFile`): the row
/// groups that survive pruning, the projected columns, and the engine chunks.
fn scan_reader<R: ChunkReader + 'static>(
    reader: &SerializedFileReader<R>,
    file: &FileResolved,
    plan: &DecodePlan<'_>,
    out: &mut TableScanOutput,
    chunks: &mut Vec<DataChunk>,
) -> Result<()> {
    let resolved = plan.resolved;
    let needed = &plan.needed;
    let placeholders = &plan.placeholders;
    let width = resolved.schema.len();
    // The output columns this file decodes, as (output, leaf, column).
    let decoded: Vec<(usize, usize, &FileColumn)> = (0..width)
        .filter(|out| needed[*out] && resolved.kind[*out] == OutKind::FileColumn)
        .filter_map(|out| {
            let leaf = file.out_to_file.get(out).copied().flatten()?;
            Some((out, leaf, &file.meta.columns[leaf]))
        })
        .collect();

    for (rg_index, rg) in file.meta.row_groups.iter().enumerate() {
        if !row_group_survives(plan.resolved, file, rg_index, plan.filters) {
            out.row_groups_pruned += 1;
            continue;
        }
        out.row_groups += 1;
        let rows = rg.rows as usize;
        let row_group_reader = reader.get_row_group(rg_index).map_err(|e| {
            PinkDbError::Execution(format!(
                "cannot read row group {rg_index} of \"{}\": {e}",
                file.meta.path
            ))
        })?;
        // One column at a time: each needed file column decodes into
        // chunk-sized pieces (a vector cannot exceed CHUNK_SIZE rows).
        let mut pieces: Vec<Vec<Vector>> = Vec::with_capacity(decoded.len());
        for (_, leaf, column) in &decoded {
            out.columns_decoded += 1;
            pieces.push(decode_column(&*row_group_reader, *leaf, column, rows)?);
        }

        for start in (0..rows).step_by(CHUNK_SIZE) {
            let chunk_index = start / CHUNK_SIZE;
            let n = CHUNK_SIZE.min(rows - start);
            let mut cols: Vec<Arc<Vector>> = Vec::with_capacity(width);
            for (out_index, placeholder) in placeholders.iter().enumerate() {
                if let Some(placeholder) = placeholder {
                    cols.push(Arc::clone(placeholder));
                    continue;
                }
                let value = match &resolved.kind[out_index] {
                    OutKind::FileColumn => {
                        let dtype = resolved.schema[out_index].1;
                        match decoded.iter().position(|(out, _, _)| *out == out_index) {
                            Some(slot) => {
                                let from = file.out_to_file[out_index]
                                    .map(|leaf| file.meta.columns[leaf].ty)
                                    .expect("a decoded file column has a leaf");
                                let piece = std::mem::replace(
                                    &mut pieces[slot][chunk_index],
                                    Vector::new_empty(dtype),
                                );
                                if from == dtype {
                                    Arc::new(piece)
                                } else {
                                    Arc::new(cast_vector(
                                        &piece,
                                        from,
                                        dtype,
                                        &resolved.schema[out_index].0,
                                    )?)
                                }
                            }
                            None => {
                                // `union_by_name` with a file that does
                                // not have the column: every row reads
                                // NULL (α's rule).
                                let mut vector = Vector::with_capacity(dtype, n);
                                for _ in 0..n {
                                    vector.push_null()?;
                                }
                                Arc::new(vector)
                            }
                        }
                    }
                    OutKind::RowNumber => {
                        let mut vector = Vector::with_capacity(DataType::Bigint, n);
                        for i in 0..n {
                            vector.push_valid(Value::Bigint((start + i) as i64))?;
                        }
                        Arc::new(vector)
                    }
                    OutKind::FileName => {
                        let mut vector = Vector::with_capacity(DataType::Varchar, n);
                        for _ in 0..n {
                            vector.push_valid(Value::Varchar(file.meta.path.clone()))?;
                        }
                        Arc::new(vector)
                    }
                    OutKind::Hive(index) => {
                        let column = &resolved.hive[*index];
                        let value = match file.hive.get(*index).and_then(|v| v.as_ref()) {
                            Some(text) => crate::hive::value_of(column.ty, text),
                            None => Value::Null,
                        };
                        let mut vector = Vector::with_capacity(column.ty, n);
                        for _ in 0..n {
                            vector.push_value(value.clone())?;
                        }
                        Arc::new(vector)
                    }
                };
                cols.push(value);
            }
            chunks.push(DataChunk::projected(cols, n));
        }
    }
    Ok(())
}

/// Cast one decoded column to the scan's output type (the conversions
/// [`castable`](crate::resolve::castable) admits). A value that does not fit
/// is a hard error naming the column — never a silently wrong value.
fn cast_vector(vector: &Vector, from: DataType, to: DataType, column: &str) -> Result<Vector> {
    let _ = from;
    let mut out = Vector::with_capacity(to, vector.len());
    for i in 0..vector.len() {
        let value = vector.get(i);
        let converted = convert_value(value, to)
            .map_err(|why| PinkDbError::Execution(format!("column \"{column}\": {why}")))?;
        out.push_value(converted)?;
    }
    Ok(out)
}

/// Whether a predicate's literal may be compared to a column of this type
/// (the engine only pushes such predicates, but a defensive check keeps a
/// foreign caller from fabricating an ordering).
fn comparable(column: DataType, literal: &Value) -> bool {
    let Some(literal_type) = literal.data_type() else {
        return false;
    };
    if column == literal_type {
        return true;
    }
    column.is_numeric() && literal_type.is_numeric()
}

/// Whether the file may contribute any row: every constant-column predicate
/// must be able to hold for the file's constant value.
fn file_survives(resolved: &Resolved, file: &FileResolved, filters: &[PrunePredicate]) -> bool {
    for filter in filters {
        let Some(constant) = file_constant_value(resolved, file, filter.column) else {
            continue;
        };
        if !constant_keeps(&constant, filter, resolved.schema[filter.column].1) {
            return false;
        }
    }
    true
}

/// Whether a constant value can satisfy one predicate.
fn constant_keeps(constant: &Value, filter: &PrunePredicate, column: DataType) -> bool {
    match filter.op {
        PruneOp::IsNull => constant.is_null(),
        PruneOp::IsNotNull => !constant.is_null(),
        _ => {
            let Some(literal) = filter.value.as_ref().filter(|_| {
                // A NULL literal never satisfies a comparison.
                true
            }) else {
                return true;
            };
            if literal.is_null() {
                // `col <op> NULL` is NULL for every row: nothing matches, but
                // the executor's filter drops them anyway; keep the file so
                // the pruning decision stays purely an optimization.
                return true;
            }
            if constant.is_null() || !comparable(column, literal) {
                return true;
            }
            let ord = pinkdb::plan::compare_values(constant, literal);
            match filter.op {
                PruneOp::Eq => ord == std::cmp::Ordering::Equal,
                PruneOp::NotEq => ord != std::cmp::Ordering::Equal,
                PruneOp::Lt => ord == std::cmp::Ordering::Less,
                PruneOp::LtEq => ord != std::cmp::Ordering::Greater,
                PruneOp::Gt => ord == std::cmp::Ordering::Greater,
                PruneOp::GtEq => ord != std::cmp::Ordering::Less,
                PruneOp::IsNull | PruneOp::IsNotNull => unreachable!(),
            }
        }
    }
}

/// Whether a row group may contribute rows under `filters`.
fn row_group_survives(
    resolved: &Resolved,
    file: &FileResolved,
    rg_index: usize,
    filters: &[PrunePredicate],
) -> bool {
    let Some(rg) = file.meta.row_groups.get(rg_index) else {
        return true;
    };
    for filter in filters {
        if resolved.kind.get(filter.column) != Some(&OutKind::FileColumn) {
            continue;
        }
        let Some(leaf) = file.out_to_file.get(filter.column).copied().flatten() else {
            continue;
        };
        let column = &file.meta.columns[leaf];
        let Some(stats) = rg.stats.get(leaf).and_then(|s| s.as_ref()) else {
            continue;
        };
        if !stats_keeps(
            stats,
            column,
            filter,
            resolved.schema[filter.column].1,
            rg.rows,
        ) {
            return false;
        }
    }
    true
}

/// Whether the statistics prove the row group may hold a matching row.
fn stats_keeps(
    stats: &crate::file_meta::ColStats,
    column: &FileColumn,
    filter: &PrunePredicate,
    _column_type: DataType,
    rows: u64,
) -> bool {
    match filter.op {
        PruneOp::IsNull => stats.null_count != Some(0),
        PruneOp::IsNotNull => stats.null_count != Some(rows),
        _ => {
            // `<>` needs both bounds and must be sure *all* values equal the
            // literal; floats are excluded (a NaN never equals, and NaN rows
            // would match `<>`).
            if filter.op == PruneOp::NotEq {
                // A NaN never equals a literal, so a row group holding one
                // could still match `<>`; floats never prune on it. Decimals
                // compare exactly, so they may.
                if column.ty == DataType::Double {
                    return true;
                }
                let (Some(min), Some(max)) = (stats.min.as_ref(), stats.max.as_ref()) else {
                    return true;
                };
                let Some(literal) = filter.value.as_ref() else {
                    return true;
                };
                if !comparable(column.ty, literal) {
                    return true;
                }
                return !(min == max && pinkdb::plan::compare_values(min, literal).is_eq());
            }
            let Some(literal) = filter.value.as_ref() else {
                return true;
            };
            if literal.is_null() || !comparable(column.ty, literal) {
                return true;
            }
            match filter.op {
                PruneOp::Eq => {
                    if let Some(min) = &stats.min
                        && pinkdb::plan::compare_values(literal, min).is_lt()
                    {
                        return false;
                    }
                    if let Some(max) = &stats.max
                        && pinkdb::plan::compare_values(literal, max).is_gt()
                    {
                        return false;
                    }
                    true
                }
                PruneOp::Lt => stats
                    .min
                    .as_ref()
                    .is_none_or(|min| pinkdb::plan::compare_values(min, literal).is_lt()),
                PruneOp::LtEq => stats
                    .min
                    .as_ref()
                    .is_none_or(|min| !pinkdb::plan::compare_values(min, literal).is_gt()),
                PruneOp::Gt => stats
                    .max
                    .as_ref()
                    .is_none_or(|max| pinkdb::plan::compare_values(max, literal).is_gt()),
                PruneOp::GtEq => stats
                    .max
                    .as_ref()
                    .is_none_or(|max| !pinkdb::plan::compare_values(max, literal).is_lt()),
                PruneOp::IsNull | PruneOp::IsNotNull | PruneOp::NotEq => unreachable!(),
            }
        }
    }
}

/// Decode one column chunk into chunk-sized engine vectors (≤ CHUNK_SIZE
/// rows each, in row order).
fn decode_column(
    row_group: &dyn RowGroupReader,
    leaf: usize,
    column: &FileColumn,
    rows: usize,
) -> Result<Vec<Vector>> {
    let scale = match column.ty {
        DataType::Decimal { scale, .. } => scale,
        _ => 0,
    };
    let optional = column.optional;
    let reader = row_group.get_column_reader(leaf).map_err(|e| {
        PinkDbError::Execution(format!(
            "cannot read column \"{}\" (leaf {leaf}): {e}",
            column.name
        ))
    })?;
    let name = column.name.clone();
    let chunks = match (column.physical, reader) {
        (parquet::basic::Type::BOOLEAN, ColumnReader::BoolColumnReader(r)) => {
            let mut r = get_typed_column_reader::<BoolType>(ColumnReader::BoolColumnReader(r));
            read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                Ok(Value::Boolean(*v))
            })?
        }
        (parquet::basic::Type::INT32, ColumnReader::Int32ColumnReader(r)) => {
            let mut r = get_typed_column_reader::<Int32Type>(ColumnReader::Int32ColumnReader(r));
            match column.ty {
                DataType::Integer => {
                    if column.unsigned {
                        read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                            i32::try_from(*v as u32)
                                .map(Value::Integer)
                                .map_err(|_| format!("{v} does not fit in INTEGER"))
                        })?
                    } else {
                        read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                            Ok(Value::Integer(*v))
                        })?
                    }
                }
                DataType::Date => read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                    Ok(Value::Date(*v))
                })?,
                DataType::Decimal { .. } => {
                    read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                        Ok(crate::schema_map::int_decimal_value(*v as i64, scale))
                    })?
                }
                other => return Err(unsupported_decode(&name, other)),
            }
        }
        (parquet::basic::Type::INT64, ColumnReader::Int64ColumnReader(r)) => {
            let mut r = get_typed_column_reader::<Int64Type>(ColumnReader::Int64ColumnReader(r));
            match column.ty {
                DataType::Bigint if column.unsigned => {
                    // UINT_64 values above i64::MAX cannot be represented.
                    read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                        i64::try_from(*v as u64)
                            .map(Value::Bigint)
                            .map_err(|_| format!("{v} does not fit in BIGINT"))
                    })?
                }
                DataType::Bigint => read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                    Ok(Value::Bigint(*v))
                })?,
                DataType::Timestamp => {
                    read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                        Ok(Value::Timestamp(*v))
                    })?
                }
                DataType::Decimal { .. } => {
                    read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                        Ok(crate::schema_map::int_decimal_value(*v, scale))
                    })?
                }
                other => return Err(unsupported_decode(&name, other)),
            }
        }
        (parquet::basic::Type::INT96, ColumnReader::Int96ColumnReader(r)) => {
            let mut r = get_typed_column_reader::<Int96Type>(ColumnReader::Int96ColumnReader(r));
            read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                let d = v.data();
                let nanos = (d[0] as u64) | ((d[1] as u64) << 32);
                let days = d[2] as i64;
                let epoch_days = days - 2_440_588;
                Ok(Value::Timestamp(
                    epoch_days * 86_400_000_000 + (nanos / 1_000) as i64,
                ))
            })?
        }
        (parquet::basic::Type::FLOAT, ColumnReader::FloatColumnReader(r)) => {
            let mut r = get_typed_column_reader::<FloatType>(ColumnReader::FloatColumnReader(r));
            read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                Ok(Value::Double(*v as f64))
            })?
        }
        (parquet::basic::Type::DOUBLE, ColumnReader::DoubleColumnReader(r)) => {
            let mut r = get_typed_column_reader::<DoubleType>(ColumnReader::DoubleColumnReader(r));
            read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                Ok(Value::Double(*v))
            })?
        }
        (parquet::basic::Type::BYTE_ARRAY, ColumnReader::ByteArrayColumnReader(r)) => {
            let mut r =
                get_typed_column_reader::<ByteArrayType>(ColumnReader::ByteArrayColumnReader(r));
            read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                std::str::from_utf8(v.data())
                    .map(|s| Value::Varchar(s.to_string()))
                    .map_err(|_| "the bytes are not valid UTF-8".to_string())
            })?
        }
        (
            parquet::basic::Type::FIXED_LEN_BYTE_ARRAY,
            ColumnReader::FixedLenByteArrayColumnReader(r),
        ) => {
            let mut r = get_typed_column_reader::<FixedLenByteArrayType>(
                ColumnReader::FixedLenByteArrayColumnReader(r),
            );
            match column.ty {
                DataType::Decimal { .. } => {
                    read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                        Ok(Value::Decimal(
                            crate::schema_map::fixed_decimal_value(v.data()),
                            scale,
                        ))
                    })?
                }
                DataType::Interval => read_chunks(&mut r, rows, optional, column.ty, &name, |v| {
                    Ok(interval_value(v.data()))
                })?,
                other => return Err(unsupported_decode(&name, other)),
            }
        }
        (physical, _) => {
            return Err(PinkDbError::Execution(format!(
                "column \"{name}\" has physical type {physical:?}, which does not match its \
                 engine type"
            )));
        }
    };
    Ok(chunks)
}

fn unsupported_decode(name: &str, ty: DataType) -> PinkDbError {
    PinkDbError::Execution(format!(
        "column \"{name}\": no decoder for the mapped type {ty}"
    ))
}

/// The parquet INTERVAL encoding: 12 little-endian bytes, months/days/millis.
fn interval_value(bytes: &[u8]) -> Value {
    if bytes.len() != 12 {
        return Value::Null;
    }
    let months = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i32;
    let days = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as i32;
    let millis = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as i64;
    Value::Interval {
        months,
        days,
        micros: millis * 1_000,
    }
}

/// Read `rows` records of a flat column into chunk-sized vectors, mapping
/// every defined value through `convert` and pushing NULLs where the
/// definition level says absent.
fn read_chunks<T>(
    reader: &mut parquet::column::reader::ColumnReaderImpl<T>,
    rows: usize,
    optional: bool,
    dtype: DataType,
    column: &str,
    convert: impl Fn(&T::T) -> std::result::Result<Value, String>,
) -> Result<Vec<Vector>>
where
    T: parquet::data_type::DataType,
{
    let mut out = Vec::with_capacity(rows.div_ceil(CHUNK_SIZE));
    let mut done = 0usize;
    let mut values: Vec<T::T> = Vec::new();
    let mut levels: Vec<i16> = Vec::new();
    while done < rows {
        let n = CHUNK_SIZE.min(rows - done);
        let mut vector = Vector::with_capacity(dtype, n);
        let mut filled = 0usize;
        while filled < n {
            values.clear();
            levels.clear();
            let read = reader
                .read_records(n - filled, Some(&mut levels), None, &mut values)
                .map_err(|e| {
                    PinkDbError::Execution(format!(
                        "cannot decode column \"{column}\" ({} more rows): {e}",
                        n - filled
                    ))
                })?;
            let records = read.0;
            if records == 0 {
                return Err(PinkDbError::Execution(format!(
                    "column \"{column}\" ended after {} of {rows} rows",
                    done + filled
                )));
            }
            // `levels` holds one entry per record for a nullable column; an
            // absent record (`level < 1`) has no value in `values`.
            let mut next = 0usize;
            for record in 0..records {
                if optional {
                    let level = levels.get(record).copied().unwrap_or(0);
                    if level >= 1 {
                        let Some(value) = values.get(next) else {
                            return Err(PinkDbError::Execution(format!(
                                "column \"{column}\": a definition level has no value"
                            )));
                        };
                        next += 1;
                        push_entry(&mut vector, convert(value), column)?;
                    } else {
                        vector.push_null()?;
                    }
                } else {
                    match values.get(next) {
                        Some(value) => {
                            next += 1;
                            push_entry(&mut vector, convert(value), column)?;
                        }
                        None => {
                            return Err(PinkDbError::Execution(format!(
                                "column \"{column}\": a REQUIRED record has no value"
                            )));
                        }
                    }
                }
            }
            filled += records;
        }
        out.push(vector);
        done += n;
    }
    Ok(out)
}

fn push_entry(
    out: &mut Vector,
    value: std::result::Result<Value, String>,
    column: &str,
) -> Result<()> {
    let value =
        value.map_err(|why| PinkDbError::Execution(format!("column \"{column}\": {why}")))?;
    out.push_value(value)
}
