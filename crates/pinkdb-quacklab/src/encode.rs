//! Cell encoding: the `rows` of a response are Go's `json.Marshal` of the
//! driver's values (`docs/wave-138-plan.md`, "Result payloads").
//!
//! serde_json is not used for numbers or strings. Go prints a DOUBLE in its
//! shortest round-trip form, in exponent notation outside 1e-6..1e21
//! (`1e+300`), where serde_json's ryu prints `1e300`; and Go's encoder
//! HTML-escapes `<`, `>` and `&`. So cells are written into the body text
//! here, and every string goes through [`push_json_string`].

use pinkdb::types::{DataType, Value, format_date, split_timestamp};

/// A cell Go's `json.Marshal` would refuse (NaN, ±Infinity).
#[derive(Debug)]
pub(crate) struct EncodeError(pub String);

/// Append one row as a JSON array. `types` is the row's column types: a
/// DECIMAL cell reports the column's width and scale, not the value's.
pub(crate) fn write_row(
    out: &mut String,
    row: &[Value],
    types: &[DataType],
) -> Result<(), EncodeError> {
    out.push('[');
    for (index, cell) in row.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_cell(out, cell, types.get(index))?;
    }
    out.push(']');
    Ok(())
}

fn write_cell(out: &mut String, value: &Value, ty: Option<&DataType>) -> Result<(), EncodeError> {
    match value {
        Value::Null => out.push_str("null"),
        Value::Boolean(flag) => out.push_str(if *flag { "true" } else { "false" }),
        Value::Integer(n) => out.push_str(&n.to_string()),
        Value::Bigint(n) => out.push_str(&n.to_string()),
        // Go's driver scans HUGEINT/UHUGEINT into a `*big.Int`, whose
        // `MarshalJSON` writes the plain number (`SUM` over integers is a
        // HUGEINT in both engines).
        Value::Hugeint(n) => out.push_str(&n.to_string()),
        Value::Uhugeint(n) => out.push_str(&n.to_string()),
        Value::Double(x) => out.push_str(&go_float(*x)?),
        Value::Varchar(s) => push_json_string(out, s),
        Value::Date(days) => {
            push_json_string(out, &format!("{}T00:00:00Z", format_date(*days)));
        }
        Value::Timestamp(us) => push_json_string(out, &rfc3339_nano(*us)),
        Value::Decimal {
            value,
            precision,
            scale,
        } => {
            let (width, scale) = match ty {
                Some(DataType::Decimal { precision, scale }) => (*precision, *scale),
                _ => (*precision, *scale),
            };
            out.push_str(&format!(
                "{{\"Width\":{width},\"Scale\":{scale},\"Value\":{value}}}"
            ));
        }
        // The plan's key order: days, months, micros.
        Value::Interval {
            months,
            days,
            micros,
        } => out.push_str(&format!(
            "{{\"days\":{days},\"months\":{months},\"micros\":{micros}}}"
        )),
        Value::Uuid(bits) => push_json_string(out, &pinkdb::uuid::format(*bits)),
        // The same JSON the store prints for a VARIANT value.
        Value::Variant(variant) => out.push_str(&variant.to_json()),
        Value::Array(array) => {
            out.push('[');
            for index in 0..array.len() {
                if index > 0 {
                    out.push(',');
                }
                write_cell(out, &array.elem_value(index), Some(&array.elem.dtype()))?;
            }
            out.push(']');
        }
    }
    Ok(())
}

/// Append `s` as a JSON string the way Go's `encoding/json` writes it:
/// control characters escaped (`\n`, `\r`, `\t`, `\u00XX`), and `<`, `>`, `&`
/// and U+2028/U+2029 as `\uXXXX`.
pub(crate) fn push_json_string(out: &mut String, s: &str) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Go's `encoding/json` float rule: `'f'` (no exponent) for
/// `1e-6 <= |x| < 1e21`, `'e'` otherwise, both with the shortest digits that
/// round-trip. Rust's `Display` is that `'f'` form and `LowerExp` that `'e'`
/// form; only the exponent's spelling differs (Go wants `e+07` and `e-7`).
fn go_float(x: f64) -> Result<String, EncodeError> {
    if x.is_nan() {
        return Err(EncodeError("json: unsupported value: NaN".to_string()));
    }
    if x.is_infinite() {
        let which = if x > 0.0 { "+Inf" } else { "-Inf" };
        return Err(EncodeError(format!("json: unsupported value: {which}")));
    }
    if x == 0.0 {
        return Ok(if x.is_sign_negative() { "-0" } else { "0" }.to_string());
    }
    let magnitude = x.abs();
    if (1e-6..1e21).contains(&magnitude) {
        return Ok(format!("{x}"));
    }
    let exponential = format!("{x:e}");
    let (mantissa, exponent) = exponential
        .split_once('e')
        .expect("LowerExp always prints an exponent");
    let exponent: i32 = exponent.parse().expect("LowerExp prints an integer");
    let sign = if exponent < 0 { '-' } else { '+' };
    let digits = exponent.unsigned_abs();
    // Go pads a positive exponent to two digits and strips the single leading
    // zero of a negative one ("1e+07", "1e+300", "1e-7").
    let digits = if exponent > 0 && digits < 10 {
        format!("0{digits}")
    } else {
        digits.to_string()
    };
    Ok(format!("{mantissa}e{sign}{digits}"))
}

/// Go's `time.Time.MarshalJSON` (`RFC3339Nano`, UTC): fractional seconds only
/// when nonzero, trailing zeros of the fraction dropped.
fn rfc3339_nano(micros: i64) -> String {
    let (days, hour, minute, second, micro) = split_timestamp(micros);
    let mut out = format!("{}T{hour:02}:{minute:02}:{second:02}", format_date(days));
    if micro != 0 {
        let fraction = format!("{micro:06}");
        out.push('.');
        out.push_str(fraction.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(value: &Value, ty: DataType) -> String {
        let mut out = String::new();
        write_cell(&mut out, value, Some(&ty)).expect("the cell encodes");
        out
    }

    #[test]
    fn doubles_print_like_go() {
        assert_eq!(go_float(1.0).unwrap(), "1");
        assert_eq!(go_float(1.5).unwrap(), "1.5");
        assert_eq!(go_float(-0.0).unwrap(), "-0");
        assert_eq!(go_float(0.0).unwrap(), "0");
        assert_eq!(go_float(1e300).unwrap(), "1e+300");
        assert_eq!(go_float(1e-7).unwrap(), "1e-7");
        assert_eq!(go_float(1e-6).unwrap(), "0.000001");
        assert_eq!(go_float(1e20).unwrap(), "100000000000000000000");
        assert_eq!(go_float(1e21).unwrap(), "1e+21");
        assert_eq!(go_float(1.5e-10).unwrap(), "1.5e-10");
        assert!(go_float(f64::NAN).unwrap_err().0.contains("NaN"));
        assert!(go_float(f64::INFINITY).unwrap_err().0.contains("+Inf"));
    }

    #[test]
    fn decimals_carry_the_column_width_and_scale() {
        let value = Value::Decimal {
            value: 12345,
            precision: 18,
            scale: 3,
        };
        assert_eq!(
            cell(
                &value,
                DataType::Decimal {
                    precision: 18,
                    scale: 3
                }
            ),
            "{\"Width\":18,\"Scale\":3,\"Value\":12345}"
        );
        let negative = Value::Decimal {
            value: -5,
            precision: 3,
            scale: 2,
        };
        assert_eq!(
            cell(
                &negative,
                DataType::Decimal {
                    precision: 3,
                    scale: 2
                }
            ),
            "{\"Width\":3,\"Scale\":2,\"Value\":-5}"
        );
    }

    #[test]
    fn dates_are_midnight_utc() {
        // 2026-01-02 is 20455 days after 1970-01-01.
        assert_eq!(
            cell(&Value::Date(20455), DataType::Date),
            "\"2026-01-02T00:00:00Z\""
        );
    }

    #[test]
    fn timestamps_drop_a_zero_fraction_and_keep_a_short_one() {
        // 2026-01-02T03:04:05Z and one with .123456.
        let whole = cell(
            &Value::Timestamp(1_767_323_045_000_000),
            DataType::Timestamp,
        );
        assert_eq!(whole, "\"2026-01-02T03:04:05Z\"");
        let fraction = cell(
            &Value::Timestamp(1_767_323_045_123_456),
            DataType::Timestamp,
        );
        assert_eq!(fraction, "\"2026-01-02T03:04:05.123456Z\"");
        let zeros = cell(
            &Value::Timestamp(1_767_323_045_000_100),
            DataType::Timestamp,
        );
        assert_eq!(zeros, "\"2026-01-02T03:04:05.0001Z\"");
    }

    #[test]
    fn intervals_print_days_months_micros() {
        let value = Value::Interval {
            months: 2,
            days: 3,
            micros: 4,
        };
        assert_eq!(
            cell(&value, DataType::Interval),
            "{\"days\":3,\"months\":2,\"micros\":4}"
        );
    }

    #[test]
    fn uuids_are_canonical_text() {
        // 00000000-0000-0000-0000-000000000001
        assert_eq!(
            cell(&Value::Uuid(1), DataType::Uuid),
            "\"00000000-0000-0000-0000-000000000001\""
        );
    }

    #[test]
    fn hugeints_are_plain_numbers_like_go_big_int() {
        assert_eq!(
            cell(&Value::Hugeint(i128::MAX), DataType::Hugeint),
            "170141183460469231731687303715884105727"
        );
        assert_eq!(cell(&Value::Hugeint(-5), DataType::Hugeint), "-5");
        assert_eq!(
            cell(&Value::Uhugeint(u128::MAX), DataType::Uhugeint),
            "340282366920938463463374607431768211455"
        );
    }

    #[test]
    fn strings_escape_like_go() {
        let mut out = String::new();
        push_json_string(&mut out, "it's <a> & b\n\u{2028}");
        assert_eq!(out, "\"it's \\u003ca\\u003e \\u0026 b\\n\\u2028\"");
    }
}
