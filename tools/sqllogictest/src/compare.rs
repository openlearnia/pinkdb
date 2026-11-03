//! Result rendering and comparison, ported from DuckDB's
//! `test/sqlite/result_helper.cpp` (MIT licensed).
//!
//! Values are compared as the strings DuckDB's runner produces: `NULL`,
//! booleans as `1`/`0`, empty strings as `(empty)`, an embedded NUL as the
//! two characters `\0`, and a tab between columns. Numeric columns are
//! re-compared by value (DuckDB's `CompareValues`), which is what makes
//! floating point results pass with the 1% `R` tolerance.

use md5::{Digest, Md5};
use pinkdb::array::ArrayValue;
use pinkdb::{ArrayElem, DataType, Value};

use crate::parser::SortMode;

/// One result cell, formatted the way DuckDB's sqllogictest runner compares it.
pub fn format_value(value: &Value) -> String {
    format_value_typed(value, &value.data_type().unwrap_or(DataType::Varchar))
}

/// Format a cell using the column's declared type. DuckDB casts by result
/// type, which matters for arrays of floats (`[nan]`, not `[NaN]`).
pub fn format_value_typed(value: &Value, dtype: &DataType) -> String {
    if let (Value::Array(array), DataType::Array { elem, .. }) = (value, dtype) {
        return format_array(array, *elem);
    }
    if value.is_null() {
        return "NULL".to_string();
    }
    let text = match value {
        Value::Boolean(true) => "1".to_string(),
        Value::Boolean(false) => "0".to_string(),
        // DuckDB's CastAs(VARCHAR) keeps the fractional zero for integral
        // doubles (`65.0`) and lowercases non-finite values.
        Value::Double(value) => duckdb_double(*value),
        // DuckDB trims trailing zeros in a timestamp's fractional seconds.
        Value::Timestamp(_) => trim_fraction(&value.to_string()),
        other => other.to_string(),
    };
    if text.is_empty() {
        "(empty)".to_string()
    } else {
        text.replace('\0', "\\0")
    }
}

/// Sort the result values for the query's sort mode and flatten them
/// row-major. `rowsort` orders rows lexicographically; `valuesort` orders
/// every value independently (DuckDB's `SortQueryResult`).
pub fn prepare_values(rows: Vec<Vec<String>>, mode: SortMode) -> Vec<String> {
    match mode {
        SortMode::NoSort => rows.into_iter().flatten().collect(),
        SortMode::RowSort => {
            let mut rows = rows;
            rows.sort();
            rows.into_iter().flatten().collect()
        }
        SortMode::ValueSort => {
            let mut values: Vec<String> = rows.into_iter().flatten().collect();
            values.sort();
            values
        }
    }
}

/// `"<n> values hashing to <md5>"`, the MD5 over each value and a newline.
pub fn result_hash(values: &[String]) -> String {
    let mut hasher = Md5::new();
    for value in values {
        hasher.update(value.as_bytes());
        hasher.update(b"\n");
    }
    let digest: String = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("{} values hashing to {digest}", values.len())
}

/// Whether an expected result line is DuckDB's hash form.
pub fn is_hash_result(line: &str) -> bool {
    let Some((count, digest)) = line.split_once(" values hashing to ") else {
        return false;
    };
    !count.is_empty()
        && count.bytes().all(|byte| byte.is_ascii_digit())
        && digest.len() == 32
        && digest.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

/// The text DuckDB's `CastAs(VARCHAR)` produces for a fixed-size array: each
/// element cast by its declared type and separated by `, `.
fn format_array(array: &ArrayValue, elem: ArrayElem) -> String {
    let elem_type = match elem {
        ArrayElem::Integer => DataType::Integer,
        ArrayElem::Bigint => DataType::Bigint,
        ArrayElem::Double => DataType::Double,
        ArrayElem::Float => DataType::Double,
    };
    let mut out = String::from("[");
    for index in 0..array.len() {
        if index > 0 {
            out.push_str(", ");
        }
        let value = array.elem_value(index);
        if value.is_null() {
            out.push_str("NULL");
        } else if elem == ArrayElem::Float {
            if let Value::Double(value) = value {
                out.push_str(&format_float32(value as f32));
            }
        } else {
            out.push_str(&format_value_typed(&value, &elem_type));
        }
    }
    out.push(']');
    out
}

fn format_float32(value: f32) -> String {
    if value.is_nan() {
        "nan".to_string()
    } else if value.is_infinite() {
        if value.is_sign_negative() {
            "-inf".to_string()
        } else {
            "inf".to_string()
        }
    } else if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

/// The text DuckDB's `CastAs(VARCHAR)` produces for a double. Only the common
/// forms are matched exactly; exponents keep Rust's spelling.
fn duckdb_double(value: f64) -> String {
    if value.is_nan() {
        "nan".to_string()
    } else if value.is_infinite() {
        if value.is_sign_negative() {
            "-inf".to_string()
        } else {
            "inf".to_string()
        }
    } else if value.fract() == 0.0 && value.abs() < 1e16 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

/// Drop trailing zeros from the fractional part of a timestamp/date string.
fn trim_fraction(text: &str) -> String {
    let Some((whole, fraction)) = text.split_once('.') else {
        return text.to_string();
    };
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{fraction}")
    }
}

/// Load a `<FILE>:` expected-result block: a pipe-separated CSV whose first
/// row is a header. DuckDB reads every column as VARCHAR and compares the
/// cells row-major, so quotes only guard separators and doubled quotes.
pub fn load_result_file(path: &std::path::Path) -> Result<(Vec<String>, usize), String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let mut lines = text.lines().filter(|line| !line.trim().is_empty());
    let header = lines
        .next()
        .ok_or_else(|| format!("result file {} is empty", path.display()))?;
    let columns = split_csv_line(header).len();
    if columns == 0 {
        return Err(format!("result file {} has no columns", path.display()));
    }
    let mut values = Vec::new();
    for (index, line) in lines.enumerate() {
        let cells = split_csv_line(line);
        if cells.len() != columns {
            return Err(format!(
                "result file {} row {} has {} columns, expected {columns}",
                path.display(),
                index + 2,
                cells.len()
            ));
        }
        values.extend(cells);
    }
    Ok((values, columns))
}

/// Split one pipe-separated CSV line, stripping surrounding quotes and
/// unescaping doubled quotes.
fn split_csv_line(line: &str) -> Vec<String> {
    let mut cells = Vec::new();
    let mut cell = String::new();
    let mut quoted = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' if quoted && chars.peek() == Some(&'"') => {
                cell.push('"');
                chars.next();
            }
            '"' => quoted = !quoted,
            '|' if !quoted => {
                cells.push(std::mem::take(&mut cell));
            }
            other => cell.push(other),
        }
    }
    cells.push(cell);
    cells
}

/// The comparison DuckDB does for one expected/actual pair. Exact strings win
/// first; then numeric types are compared by value and booleans by truth.
pub fn value_matches(expected: &str, actual: &str, data_type: &DataType) -> bool {
    if expected == actual {
        return true;
    }
    match data_type {
        DataType::Boolean => parse_bool(expected)
            .zip(parse_bool(actual))
            .is_some_and(|(left, right)| left == right),
        DataType::Integer | DataType::Bigint | DataType::Hugeint | DataType::Uhugeint => {
            parse_integer_text(expected)
                .zip(parse_integer_text(actual))
                .is_some_and(|(left, right)| left == right)
        }
        DataType::Double => expected
            .parse::<f64>()
            .ok()
            .zip(actual.parse::<f64>().ok())
            .is_some_and(|(left, right)| approx_equal(left, right)),
        DataType::Decimal { scale, .. } => decimal_scaled(expected, *scale)
            .zip(decimal_scaled(actual, *scale))
            .is_some_and(|(left, right)| left == right),
        _ => false,
    }
}

/// DuckDB's `ApproxEqual` for `R` columns. `CompareValues` passes the actual
/// result first and the expected text second, so the 1% tolerance is relative
/// to the actual value.
fn approx_equal(expected: f64, actual: f64) -> bool {
    if expected.is_nan() || actual.is_nan() {
        return expected.is_nan() && actual.is_nan();
    }
    if !expected.is_finite() || !actual.is_finite() {
        return expected == actual;
    }
    (expected - actual).abs() <= actual.abs() * 0.01 + 1e-8
}

fn parse_bool(text: &str) -> Option<bool> {
    match text.to_ascii_lowercase().as_str() {
        "1" | "true" => Some(true),
        "0" | "false" => Some(false),
        _ => None,
    }
}

/// Parse a numeric literal the way DuckDB's cast to an integer column does:
/// decimal fractions round half away from zero and exponents are accepted, so
/// `"65.000000"` equals `65`. pinkdb's own string-to-integer cast is stricter
/// than DuckDB's, so a numeric parse is used instead of a `CAST` round trip.
fn parse_integer_text(text: &str) -> Option<i128> {
    let (negative, digits, exponent) = parse_decimal_parts(text)?;
    let value = round_decimal(digits, exponent)?;
    Some(if negative { -value } else { value })
}

/// Parse a decimal string at a fixed scale, rounding the part beyond the scale
/// the way a cast to `DECIMAL(p, s)` does.
fn decimal_scaled(text: &str, scale: u8) -> Option<i128> {
    let (negative, digits, exponent) = parse_decimal_parts(text)?;
    let value = round_decimal(digits, exponent + i32::from(scale))?;
    Some(if negative { -value } else { value })
}

/// Split a numeric literal into sign, significant digits and a base-10
/// exponent (`digits * 10^exponent`). Only plain decimal/exponent syntax is
/// accepted; NaN and infinities are not numeric here (DuckDB's string cast
/// rejects them too).
fn parse_decimal_parts(text: &str) -> Option<(bool, i128, i32)> {
    let text = text.trim();
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
        None => (unsigned, 0),
    };
    let (integer, fraction) = match mantissa.split_once('.') {
        Some(parts) => parts,
        None => (mantissa, ""),
    };
    if integer.is_empty() && fraction.is_empty() {
        return None;
    }
    if !integer.bytes().all(|b| b.is_ascii_digit()) || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let mut digits = String::with_capacity(integer.len() + fraction.len());
    digits.push_str(if integer.is_empty() { "0" } else { integer });
    digits.push_str(fraction);
    let digits: i128 = digits.parse().ok()?;
    Some((
        negative,
        digits,
        exponent.checked_sub(fraction.len() as i32)?,
    ))
}

/// Round `digits * 10^exponent` to an integer, half away from zero.
fn round_decimal(digits: i128, exponent: i32) -> Option<i128> {
    if exponent >= 0 {
        return digits.checked_mul(10i128.checked_pow(exponent as u32)?);
    }
    let divisor = 10i128.checked_pow(exponent.unsigned_abs())?;
    let quotient = digits / divisor;
    let remainder = digits % divisor;
    if remainder * 2 >= divisor {
        quotient.checked_add(1)
    } else {
        Some(quotient)
    }
}

/// Compare an expected result block against the (already sorted) actual
/// values. Returns a one-line reason on mismatch.
///
/// The row-wise/flat guess is DuckDB's: a block whose line count equals the
/// row count while there is more than one column is row-wise; otherwise it is
/// row-wise only if every line contains a tab; otherwise it is one value per
/// line.
pub fn compare_result(
    expected: &[String],
    actual: &[String],
    column_types: &[DataType],
) -> Result<(), String> {
    let columns = column_types.len();
    if columns == 0 {
        return Err("query returned zero columns".to_string());
    }
    if !actual.len().is_multiple_of(columns) {
        return Err(format!(
            "result value count {} is not divisible by {columns} columns",
            actual.len()
        ));
    }
    let rows = actual.len() / columns;
    let mut row_wise = columns > 1 && expected.len() == rows;
    if !row_wise {
        row_wise = !expected.is_empty() && expected.iter().all(|line| line.contains('\t'));
    }
    let expected_values: Vec<String> = if row_wise {
        if expected.len() != rows {
            return Err(format!("expected {} rows, got {rows}", expected.len()));
        }
        let mut values = Vec::with_capacity(actual.len());
        for (index, line) in expected.iter().enumerate() {
            let split: Vec<&str> = line.split('\t').collect();
            if split.len() != columns {
                return Err(format!(
                    "expected row {} has {} columns, expected {columns}",
                    index + 1,
                    split.len()
                ));
            }
            values.extend(split.into_iter().map(str::to_string));
        }
        values
    } else {
        if !expected.len().is_multiple_of(columns) {
            return Err(format!(
                "expected value count {} is not divisible by {columns} columns",
                expected.len()
            ));
        }
        let expected_rows = expected.len() / columns;
        if expected_rows != rows {
            return Err(format!("expected {expected_rows} rows, got {rows}"));
        }
        expected.to_vec()
    };
    for (index, (expected, actual)) in expected_values.iter().zip(actual).enumerate() {
        let column = index % columns;
        if !value_matches(expected, actual, &column_types[column]) {
            return Err(format!(
                "value {} differs: expected {:?}, got {:?}",
                index + 1,
                expected,
                actual
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn integers(values: &[i64]) -> Vec<String> {
        values.iter().map(|v| format!("{v}")).collect()
    }

    #[test]
    fn formats_null_boolean_empty_and_nul() {
        assert_eq!(format_value(&Value::Null), "NULL");
        assert_eq!(format_value(&Value::Boolean(true)), "1");
        assert_eq!(format_value(&Value::Boolean(false)), "0");
        assert_eq!(format_value(&Value::Varchar(String::new())), "(empty)");
        assert_eq!(format_value(&Value::Varchar("a\0b".to_string())), "a\\0b");
        assert_eq!(format_value(&Value::Varchar("a\nb".to_string())), "a\nb");
    }

    #[test]
    fn formats_arrays_by_element_type() {
        let array = ArrayValue::new(
            ArrayElem::Double,
            2,
            pinkdb::array::ArrayElems::Floats(vec![f64::NAN, 1.0]),
            None,
        )
        .unwrap();
        assert_eq!(
            format_value(&Value::Array(std::sync::Arc::new(array))),
            "[nan, 1.0]"
        );
    }

    #[test]
    fn formats_doubles_and_timestamps_like_duckdb_casts() {
        assert_eq!(format_value(&Value::Double(65.0)), "65.0");
        assert_eq!(format_value(&Value::Double(f64::NAN)), "nan");
        assert_eq!(
            format_value(&Value::Timestamp(1_669_075_260_871_000)),
            "2022-11-22 00:01:00.871"
        );
        assert_eq!(
            format_value(&Value::Timestamp(1_669_075_260_000_000)),
            "2022-11-22 00:01:00"
        );
    }

    #[test]
    fn sorts_rows_and_values() {
        let rows = vec![
            vec!["b".to_string(), "2".to_string()],
            vec!["a".to_string(), "1".to_string()],
        ];
        assert_eq!(
            prepare_values(rows.clone(), SortMode::RowSort),
            vec!["a", "1", "b", "2"]
        );
        assert_eq!(
            prepare_values(rows, SortMode::ValueSort),
            vec!["1", "2", "a", "b"]
        );
    }

    #[test]
    fn hashes_are_md5_over_value_and_newline() {
        let hash = result_hash(&integers(&[1, 2, 3]));
        assert!(is_hash_result(&hash));
        assert_eq!(hash, "3 values hashing to c0710d6b4f15dfa88f600b0e6b624077");
        assert!(!is_hash_result("3 values hashing to short"));
        assert!(!is_hash_result("3 values is not a hash"));
    }

    #[test]
    fn compares_flat_and_row_wise_blocks() {
        let types = [DataType::Integer, DataType::Integer];
        let actual = integers(&[1, 2, 3, 4]);
        assert_eq!(
            compare_result(&integers(&[1, 2, 3, 4]), &actual, &types),
            Ok(())
        );
        assert_eq!(
            compare_result(&["1\t2".to_string(), "3\t4".to_string()], &actual, &types),
            Ok(())
        );
        assert_eq!(
            compare_result(&["1\t2".to_string(), "3\t5".to_string()], &actual, &types),
            Err("value 4 differs: expected \"5\", got \"4\"".to_string())
        );
    }

    #[test]
    fn reports_row_and_column_mismatches() {
        let types = [DataType::Integer];
        assert_eq!(
            compare_result(&integers(&[1, 2]), &integers(&[1]), &types),
            Err("expected 2 rows, got 1".to_string())
        );
        let types = [DataType::Integer, DataType::Integer];
        assert_eq!(
            compare_result(
                &["1".to_string(), "2".to_string(), "3".to_string()],
                &integers(&[1, 2]),
                &types
            ),
            Err("expected value count 3 is not divisible by 2 columns".to_string())
        );
        assert_eq!(
            compare_result(&[], &integers(&[1, 2]), &types),
            Err("expected 0 rows, got 1".to_string())
        );
    }

    #[test]
    fn integer_tolerance_is_exact_and_float_tolerance_is_one_percent() {
        assert!(value_matches("1", "1", &DataType::Bigint));
        assert!(!value_matches("1", "2", &DataType::Bigint));
        assert!(value_matches("1.0", "1.004", &DataType::Double));
        assert!(!value_matches("1.0", "1.02", &DataType::Double));
        assert!(value_matches("0", "1e-9", &DataType::Double));
        assert!(value_matches("NaN", "NaN", &DataType::Double));
        assert!(!value_matches("inf", "-inf", &DataType::Double));
    }

    #[test]
    fn integer_columns_accept_decimal_text_like_duckdb_casts() {
        assert!(value_matches("65.000000", "65", &DataType::Bigint));
        assert!(value_matches("65.5", "66", &DataType::Bigint));
        assert!(value_matches("-65.5", "-66", &DataType::Integer));
        assert!(value_matches("1e3", "1000", &DataType::Bigint));
        assert!(value_matches(" 65 ", "65", &DataType::Bigint));
        assert!(value_matches("65.4", "65", &DataType::Bigint));
        assert!(!value_matches("65.6", "65", &DataType::Bigint));
    }

    #[test]
    fn float_tolerance_is_relative_to_the_actual_value() {
        // 100 vs 101 is within 1% of 101 but not of 100.
        assert!(value_matches("100", "101", &DataType::Double));
        assert!(!value_matches("100", "99", &DataType::Double));
    }

    #[test]
    fn loads_file_results_as_pipe_separated_values() {
        let path = std::env::temp_dir().join(format!(
            "pinkdb-slt-csv-{}-{}.csv",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, "\"a\"|\"b\"\n1|2\n3|4\n").unwrap();
        let (values, columns) = load_result_file(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(columns, 2);
        assert_eq!(values, ["1", "2", "3", "4"]);
    }

    #[test]
    fn decimal_comparison_uses_the_column_scale() {
        let ty = DataType::Decimal {
            precision: 10,
            scale: 2,
        };
        assert!(value_matches("1.50", "1.5", &ty));
        assert!(value_matches("-0.05", "-0.05", &ty));
        assert!(!value_matches("1.50", "1.51", &ty));
        assert!(!value_matches("1.505", "1.50", &ty));
    }

    #[test]
    fn booleans_compare_by_truth() {
        assert!(value_matches("1", "true", &DataType::Boolean));
        assert!(value_matches("0", "false", &DataType::Boolean));
        assert!(!value_matches("1", "false", &DataType::Boolean));
        assert!(!value_matches("1", "yes", &DataType::Boolean));
    }

    #[test]
    fn text_and_null_compare_exactly() {
        assert!(value_matches("NULL", "NULL", &DataType::Varchar));
        assert!(!value_matches("NULL", "(empty)", &DataType::Varchar));
        assert!(value_matches("(empty)", "(empty)", &DataType::Varchar));
        assert!(!value_matches("a", "A", &DataType::Varchar));
    }
}
