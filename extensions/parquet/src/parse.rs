//! Small literal parsers shared by the hive inference and the argument layer:
//! strict `i64`, `YYYY-MM-DD`, and boolean spellings. The date parser is the
//! engine's own civil-calendar rule (proleptic Gregorian, days since
//! 1970-01-01), so a hive DATE cell is exactly a DATE literal.

/// A strict `i64` (no whitespace, no `+`, no leading zeros beyond "0").
pub fn parse_i64(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let (negative, digits) = match bytes[0] {
        b'-' => (true, &text[1..]),
        _ => (false, text),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let magnitude = digits.parse::<i64>().ok()?;
    // `-9223372036854775808` is representable; the positive magnitude of the
    // most negative value is not, so handle it explicitly.
    Some(if negative { -magnitude } else { magnitude })
}

/// `YYYY-MM-DD` → days since 1970-01-01 (the engine's DATE domain).
pub fn parse_date(text: &str) -> Option<i32> {
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let year: i32 = text[0..4].parse().ok()?;
    let month: u32 = text[5..7].parse().ok()?;
    let day: u32 = text[8..10].parse().ok()?;
    if !(1..=9999).contains(&year) || !(1..=12).contains(&month) || day == 0 {
        return None;
    }
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if leap {
                29
            } else {
                28
            }
        }
    };
    if day > max_day {
        return None;
    }
    Some(days_from_civil(year, month, day))
}

/// Howard Hinnant's `days_from_civil` (the engine's own algorithm).
fn days_from_civil(y: i32, m: u32, d: u32) -> i32 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i32 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `true`/`false` in any case (DuckDB's boolean literal spellings for the
/// table-function options).
pub fn parse_bool(text: &str) -> Option<bool> {
    match text.to_ascii_lowercase().as_str() {
        "true" | "t" | "1" => Some(true),
        "false" | "f" | "0" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_are_strict() {
        assert_eq!(parse_i64("0"), Some(0));
        assert_eq!(parse_i64("2024"), Some(2024));
        assert_eq!(parse_i64("-3"), Some(-3));
        assert_eq!(parse_i64("01"), Some(1));
        assert_eq!(parse_i64("1.5"), None);
        assert_eq!(parse_i64(""), None);
        assert_eq!(parse_i64("-"), None);
        assert_eq!(parse_i64("1 "), None);
        assert_eq!(parse_i64("9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_i64("9223372036854775808"), None);
    }

    #[test]
    fn dates_are_validated() {
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(parse_date("2024-01-01"), Some(19723));
        assert_eq!(parse_date("2024-02-29"), Some(19782));
        assert_eq!(parse_date("2023-02-29"), None);
        assert_eq!(parse_date("2024-13-01"), None);
        assert_eq!(parse_date("2024-01-32"), None);
        assert_eq!(parse_date("2024-1-1"), None);
        assert_eq!(parse_date("us"), None);
    }

    #[test]
    fn booleans_parse() {
        assert_eq!(parse_bool("TRUE"), Some(true));
        assert_eq!(parse_bool("false"), Some(false));
        assert_eq!(parse_bool("yes"), None);
    }
}
