//! The parsed JSON value and its serialization back to JSON text.
//!
//! The model mirrors what DuckDB's parser keeps, which is *not* a normalized
//! JSON tree:
//!
//! * **object entries keep their source order and their duplicates** —
//!   `{"a":1,"a":2}` round-trips verbatim and a lookup returns the first
//!   match (probed against DuckDB α46057);
//! * **numbers that fit** an `i64`/`u64`/`f64` are rendered canonically
//!   (`-0` → `0`, `1.50` → `1.5`, `1E+3` → `1000.0`), while integers outside
//!   `u64`/`i64`, exponents that overflow `f64` (`1e400`), and the lenient
//!   `NaN`/`Infinity` literals are kept as [`Json::Raw`] text and written
//!   back byte for byte;
//! * **strings are decoded** (surrogate pairs, `\uXXXX`) and re-escaped on
//!   output with the same spelling DuckDB uses (short escapes for
//!   `\b \t \n \f \r`, uppercase `\u00XX` for other control characters,
//!   everything else — DEL included — as raw UTF-8).

/// One JSON value (see the module docs for the fidelity rules).
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    /// `null`.
    Null,
    /// `true` / `false`.
    Bool(bool),
    /// An integer literal that fits `i64` (rendered canonically, so `-0`
    /// becomes `0`).
    Int(i64),
    /// A positive integer literal that does not fit `i64` but fits `u64`.
    UInt(u64),
    /// A number written with `.` or an exponent that fits `f64`.
    Float(f64),
    /// A number-like token kept verbatim: an integer wider than `u64`, an
    /// exponent that overflows `f64` (`1e400`), or a lenient
    /// `NaN`/`Infinity`/`inf` literal.
    Raw(String),
    /// A JSON string (decoded).
    Str(String),
    /// An array, in source order.
    Array(Vec<Json>),
    /// An object's entries, in source order, duplicates preserved.
    Object(Vec<(String, Json)>),
}

impl Json {
    /// Whether this is a JSON array.
    pub fn is_array(&self) -> bool {
        matches!(self, Json::Array(_))
    }

    /// The number of array elements, when this is an array.
    pub fn array_len(&self) -> Option<usize> {
        match self {
            Json::Array(items) => Some(items.len()),
            _ => None,
        }
    }
}

/// Serialize `value` to JSON text (no surrounding whitespace).
pub fn to_string(value: &Json) -> String {
    let mut out = String::new();
    write(&mut out, value);
    out
}

/// Append `value`'s JSON text to `out`.
pub fn write(out: &mut String, value: &Json) {
    match value {
        Json::Null => out.push_str("null"),
        Json::Bool(true) => out.push_str("true"),
        Json::Bool(false) => out.push_str("false"),
        Json::Int(i) => out.push_str(itoa(*i).as_str()),
        Json::UInt(u) => out.push_str(utoa(*u).as_str()),
        Json::Float(f) => out.push_str(&format_f64(*f)),
        Json::Raw(text) => out.push_str(text),
        Json::Str(s) => write_string(out, s),
        Json::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write(out, item);
            }
            out.push(']');
        }
        Json::Object(entries) => {
            out.push('{');
            for (i, (key, item)) in entries.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(out, key);
                out.push(':');
                write(out, item);
            }
            out.push('}');
        }
    }
}

/// `i64` to decimal, without a `format!` allocation.
fn itoa(n: i64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let negative = n < 0;
    let mut digits = Vec::with_capacity(20);
    // `unsigned_abs` handles `i64::MIN` without overflow.
    let mut m = n.unsigned_abs();
    while m > 0 {
        digits.push(b'0' + (m % 10) as u8);
        m /= 10;
    }
    if negative {
        digits.push(b'-');
    }
    digits.reverse();
    String::from_utf8(digits).expect("ascii digits")
}

/// `u64` to decimal.
fn utoa(mut n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let mut digits = Vec::with_capacity(20);
    while n > 0 {
        digits.push(b'0' + (n % 10) as u8);
        n /= 10;
    }
    digits.reverse();
    String::from_utf8(digits).expect("ascii digits")
}

/// Write a JSON string: quotes, the two mandatory escapes, the five short
/// control escapes, and `\u00XX` (uppercase hex) for the rest of C0. Every
/// other code point — DEL, U+2028, emoji — is written as raw UTF-8.
pub fn write_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => {
                let n = c as u32;
                out.push_str("\\u00");
                out.push(hex_digit((n >> 4) as u8));
                out.push(hex_digit((n & 0xf) as u8));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn hex_digit(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        _ => (b'A' + (n - 10)) as char,
    }
}

/// Format a finite double the way DuckDB's JSON writer does.
///
/// The significant digits are the shortest decimal that round-trips (the
/// same digits Rust's `{:?}` produces — verified against DuckDB α46057 on a
/// probe corpus). The notation window differs: DuckDB writes plain decimal
/// while the decimal exponent of the leading digit is in `-6..=20` and
/// scientific notation otherwise (`1e-7`, `5e-324`, `1e21`, `1.5e300`),
/// with an integral value getting a trailing `.0` (`1000.0`, `1e15` →
/// `1000000000000000.0`).
fn format_f64(v: f64) -> String {
    debug_assert!(v.is_finite(), "NaN/Infinity are kept as raw text");
    // Rust's Debug output is shortest-round-trip; recover (digits, exponent)
    // from it instead of reimplementing Grisu.
    let debug = format!("{v:?}");
    let (negative, rest) = match debug.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, debug.as_str()),
    };
    let (mantissa, exp) = match rest.split_once(['e', 'E']) {
        Some((m, e)) => (
            m,
            e.parse::<i32>()
                .expect("Rust writes a plain decimal exponent"),
        ),
        None => (rest, 0),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((i, f)) => (i, f),
        None => (mantissa, ""),
    };
    let mut digits = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);
    // `exp10` is the exponent of the leading digit: value = 0.digits * 10^(exp10+1).
    let exp10 = int_part.len() as i32 - 1 + exp;
    // Drop the `.0`-style padding Rust adds; keep at least one digit.
    let trimmed = digits.trim_end_matches('0');
    let digits = if trimmed.is_empty() { "0" } else { trimmed };

    let mut out = String::with_capacity(digits.len() + 8);
    if negative {
        out.push('-');
    }
    let n = digits.len() as i32;
    if (-6..=20).contains(&exp10) {
        if exp10 >= n - 1 {
            // Integral: digits, zero padding, then `.0`. A zero has one digit
            // and exponent 0, so it lands here and prints `0.0` (`-0.0`).
            out.push_str(digits);
            for _ in 0..(exp10 - (n - 1)) {
                out.push('0');
            }
            out.push_str(".0");
        } else if exp10 >= 0 {
            let split = (exp10 + 1) as usize;
            out.push_str(&digits[..split]);
            out.push('.');
            out.push_str(&digits[split..]);
        } else {
            out.push_str("0.");
            for _ in 0..(-exp10 - 1) {
                out.push('0');
            }
            out.push_str(digits);
        }
    } else {
        out.push_str(&digits[..1]);
        if n > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push_str(&exp10.to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Number rendering pinned against DuckDB α46057
    /// (`SELECT json_extract('[<lit>]', '$[0]')`).
    #[test]
    fn doubles_match_the_duckdb_writer() {
        let cases: &[(f64, &str)] = &[
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1.0, "1.0"),
            (1.5, "1.5"),
            (-0.5, "-0.5"),
            (1000.0, "1000.0"),
            (1e-7, "1e-7"),
            (1e-6, "0.000001"),
            (1.5e-6, "0.0000015"),
            (1.0000000000000002e-6, "0.0000010000000000000002"),
            (1e-4, "0.0001"),
            (5e-324, "5e-324"),
            (1.5e300, "1.5e300"),
            (1e15, "1000000000000000.0"),
            (1e16, "10000000000000000.0"),
            (1e17, "100000000000000000.0"),
            (1e18, "1000000000000000000.0"),
            (1e19, "10000000000000000000.0"),
            (1e20, "100000000000000000000.0"),
            (1e21, "1e21"),
            (1e100, "1e100"),
            (1e308, "1e308"),
            (0.30000000000000004, "0.30000000000000004"),
            (2.567, "2.567"),
            (123456789.5, "123456789.5"),
            (9.9e20, "990000000000000000000.0"),
            (1.2345678901234567e20, "123456789012345670000.0"),
            (1.2345678901234568e29, "1.2345678901234568e29"),
            (123456789012345680.0, "123456789012345680.0"),
            (1.0 / 3.0, "0.3333333333333333"),
        ];
        for (value, expected) in cases {
            assert_eq!(&format_f64(*value), expected, "for {value:?}");
        }
    }

    #[test]
    fn integers_are_canonical() {
        assert_eq!(to_string(&Json::Int(-0)), "0");
        assert_eq!(to_string(&Json::Int(-42)), "-42");
        assert_eq!(to_string(&Json::Int(i64::MIN)), "-9223372036854775808");
        assert_eq!(to_string(&Json::UInt(u64::MAX)), "18446744073709551615");
    }

    #[test]
    fn strings_escape_like_duckdb() {
        let cases: &[(&str, &str)] = &[
            ("plain", "\"plain\""),
            ("a\"b", "\"a\\\"b\""),
            ("a\\b", "\"a\\\\b\""),
            ("tab\there", "\"tab\\there\""),
            ("n\ne", "\"n\\ne\""),
            ("\u{8}\u{c}\r", "\"\\b\\f\\r\""),
            ("\u{0}\u{7}\u{1f}", "\"\\u0000\\u0007\\u001F\""),
            ("\u{7f}", "\"\u{7f}\""),
            ("/", "\"/\""),
            ("héllo😀", "\"héllo😀\""),
        ];
        for (input, expected) in cases {
            let mut out = String::new();
            write_string(&mut out, input);
            assert_eq!(&out, expected, "for {input:?}");
        }
    }

    #[test]
    fn structures_keep_order_and_duplicates() {
        let value = Json::Object(vec![
            ("a".to_string(), Json::Int(1)),
            (
                "b".to_string(),
                Json::Array(vec![Json::Bool(true), Json::Null]),
            ),
            ("a".to_string(), Json::Int(2)),
        ]);
        assert_eq!(to_string(&value), r#"{"a":1,"b":[true,null],"a":2}"#);
    }
}
