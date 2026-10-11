//! Core types: [`DataType`], [`Value`], [`PinkDbError`].
//!
//! v1 types: BOOLEAN, INTEGER (i32), BIGINT (i64), DOUBLE (f64), VARCHAR,
//! DATE (i32 days since 1970-01-01), TIMESTAMP (i64 microseconds since
//! 1970-01-01 00:00:00, naive — no time zone).

use std::fmt;

// ---------- fast deterministic hashing ----------
// FNV-1a for short group/join keys: much cheaper than SipHash RandomState
// and deterministic across runs (stable chunk-partitioning behavior).

#[derive(Clone, Default)]
pub struct FnvBuild;

pub struct FnvHash(u64);

impl std::hash::BuildHasher for FnvBuild {
    type Hasher = FnvHash;
    fn build_hasher(&self) -> FnvHash {
        FnvHash(0xcbf29ce484222325)
    }
}

impl std::hash::Hasher for FnvHash {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= b as u64;
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// HashMap with FNV-1a hashing for hot grouping paths.
pub type FastMap<K, V> = std::collections::HashMap<K, V, FnvBuild>;

/// HashSet with FNV-1a hashing (IN-sets, DISTINCT).
pub type FastSet<T> = std::collections::HashSet<T, FnvBuild>;

/// Column data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Boolean,
    Integer,
    Bigint,
    Double,
    Varchar,
    Date,
    Timestamp,
}

impl DataType {
    /// DuckDB-style display name.
    pub fn name(self) -> &'static str {
        match self {
            DataType::Boolean => "BOOLEAN",
            DataType::Integer => "INTEGER",
            DataType::Bigint => "BIGINT",
            DataType::Double => "DOUBLE",
            DataType::Varchar => "VARCHAR",
            DataType::Date => "DATE",
            DataType::Timestamp => "TIMESTAMP",
        }
    }

    /// Parse a type name from `CREATE TABLE` (case-insensitive).
    /// Aliases: INT → INTEGER, TEXT/STRING → VARCHAR, FLOAT → DOUBLE,
    /// DATETIME → TIMESTAMP.
    pub fn from_sql(s: &str) -> Option<DataType> {
        match s.to_ascii_uppercase().as_str() {
            "BOOLEAN" | "BOOL" => Some(DataType::Boolean),
            "INTEGER" | "INT" => Some(DataType::Integer),
            "BIGINT" | "LONG" => Some(DataType::Bigint),
            "DOUBLE" | "FLOAT" | "REAL" => Some(DataType::Double),
            "VARCHAR" | "TEXT" | "STRING" => Some(DataType::Varchar),
            "DATE" => Some(DataType::Date),
            "TIMESTAMP" | "DATETIME" => Some(DataType::Timestamp),
            _ => None,
        }
    }

    pub fn is_numeric(self) -> bool {
        matches!(self, DataType::Integer | DataType::Bigint | DataType::Double)
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A single scalar value. `Null` carries no type; the column does.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i32),
    Bigint(i64),
    Double(f64),
    Varchar(String),
    /// Days since 1970-01-01 (proleptic Gregorian).
    Date(i32),
    /// Microseconds since 1970-01-01 00:00:00 (naive, no time zone).
    Timestamp(i64),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Integer(_) => Some(DataType::Integer),
            Value::Bigint(_) => Some(DataType::Bigint),
            Value::Double(_) => Some(DataType::Double),
            Value::Varchar(_) => Some(DataType::Varchar),
            Value::Date(_) => Some(DataType::Date),
            Value::Timestamp(_) => Some(DataType::Timestamp),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{b}"),
            Value::Integer(i) => write!(f, "{i}"),
            Value::Bigint(i) => write!(f, "{i}"),
            Value::Double(x) => write!(f, "{x}"),
            Value::Varchar(s) => write!(f, "{s}"),
            Value::Date(d) => write!(f, "{}", format_date(*d)),
            Value::Timestamp(us) => write!(f, "{}", format_timestamp(*us)),
        }
    }
}

// ---------- calendar ----------
// Proleptic Gregorian algorithms (Howard Hinnant's civil calendar math):
// days_from_civil: (y, m, d) → days since 1970-01-01;
// civil_from_days inverts it. All pure integer arithmetic.

/// Days since 1970-01-01 for a civil date. `m` is 1-based.
pub fn days_from_civil(y: i32, m: u32, d: u32) -> i32 {
    let y = if m <= 2 { y as i64 - 1 } else { y as i64 };
    let era = y.div_euclid(400);
    let yoe = y - era * 400; // [0, 399]
    let mp = (m as i64 + 9) % 12; // [0, 11], March-based
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146097 + doe - 719468) as i32
}

/// Civil (year, 1-based month, day) for days since 1970-01-01.
pub fn civil_from_days(z: i32) -> (i32, u32, u32) {
    let z = z as i64 + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    ((if m <= 2 { y + 1 } else { y }) as i32, m, d)
}

fn is_leap(y: i32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Parse `YYYY-MM-DD` (exactly 10 chars) → days since epoch. Strict:
/// correct length, digits, and a real calendar date.
pub fn parse_date(s: &str) -> Option<i32> {
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let num = |lo: usize, hi: usize| s[lo..hi].parse::<i32>().ok();
    let (y, m, d) = (num(0, 4)?, num(5, 7)? as u32, num(8, 10)? as u32);
    if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) {
        return None;
    }
    Some(days_from_civil(y, m, d))
}

/// Parse `YYYY-MM-DD HH:MM:SS[.ffffff]` → microseconds since epoch.
/// Date and time parts are both strict; fractional seconds pad/truncate to 6
/// digits. A `T` separator is accepted as well as a space.
pub fn parse_timestamp(s: &str) -> Option<i64> {
    let (date_part, time_part) = s.split_once([' ', 'T'])?;
    let days = parse_date(date_part)? as i64;
    // HH:MM:SS with optional .fraction.
    let (hms, frac) = match time_part.split_once('.') {
        Some((h, f)) => (h, Some(f)),
        None => (time_part, None),
    };
    if hms.len() != 8 || hms.as_bytes()[2] != b':' || hms.as_bytes()[5] != b':' {
        return None;
    }
    let num = |lo: usize, hi: usize| hms[lo..hi].parse::<i64>().ok();
    let (h, mi, sec) = (num(0, 2)?, num(3, 5)?, num(6, 8)?);
    if h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    let micros = match frac {
        None => 0,
        Some(f) => {
            if f.is_empty() || f.len() > 6 || !f.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let mut v: i64 = f.parse().ok()?;
            for _ in f.len()..6 {
                v *= 10;
            }
            v
        }
    };
    Some(days * 86_400_000_000 + h * 3_600_000_000 + mi * 60_000_000 + sec * 1_000_000 + micros)
}

/// `YYYY-MM-DD`.
pub fn format_date(days: i32) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// `YYYY-MM-DD HH:MM:SS[.ffffff]` (fraction omitted when zero).
pub fn format_timestamp(us: i64) -> String {
    let (days, h, mi, sec, micro) = split_timestamp(us);
    if micro == 0 {
        format!("{} {h:02}:{mi:02}:{sec:02}", format_date(days))
    } else {
        format!("{} {h:02}:{mi:02}:{sec:02}.{micro:06}", format_date(days))
    }
}

/// Split microseconds since epoch into (days, hour, minute, second, micro).
/// Uses floored division so pre-epoch values stay consistent.
pub fn split_timestamp(us: i64) -> (i32, u32, u32, u32, u32) {
    let days = us.div_euclid(86_400_000_000) as i32;
    let rest = us.rem_euclid(86_400_000_000);
    let (h, rest) = (rest / 3_600_000_000, rest % 3_600_000_000);
    let (mi, rest) = (rest / 60_000_000, rest % 60_000_000);
    let (sec, micro) = (rest / 1_000_000, rest % 1_000_000);
    (days, h as u32, mi as u32, sec as u32, micro as u32)
}

/// All engine errors. Lean: a few string-carrying variants + transparent
/// IO/CSV wrappers. No extra deps (no thiserror).
#[derive(Debug)]
pub enum PinkDbError {
    Catalog(String),
    Type(String),
    Parse(String),
    Execution(String),
    Io(std::io::Error),
    Csv(csv::Error),
}

impl fmt::Display for PinkDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PinkDbError::Catalog(msg)
            | PinkDbError::Type(msg)
            | PinkDbError::Parse(msg)
            | PinkDbError::Execution(msg) => write!(f, "{msg}"),
            PinkDbError::Io(e) => write!(f, "IO error: {e}"),
            PinkDbError::Csv(e) => write!(f, "CSV error: {e}"),
        }
    }
}

impl std::error::Error for PinkDbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PinkDbError::Io(e) => Some(e),
            PinkDbError::Csv(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for PinkDbError {
    fn from(e: std::io::Error) -> Self {
        PinkDbError::Io(e)
    }
}

impl From<csv::Error> for PinkDbError {
    fn from(e: csv::Error) -> Self {
        PinkDbError::Csv(e)
    }
}

pub type Result<T> = std::result::Result<T, PinkDbError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_names_roundtrip() {
        assert_eq!(DataType::from_sql("integer"), Some(DataType::Integer));
        assert_eq!(DataType::from_sql("INT"), Some(DataType::Integer));
        assert_eq!(DataType::from_sql("bigint"), Some(DataType::Bigint));
        assert_eq!(DataType::from_sql("double"), Some(DataType::Double));
        assert_eq!(DataType::from_sql("varchar"), Some(DataType::Varchar));
        assert_eq!(DataType::from_sql("text"), Some(DataType::Varchar));
        assert_eq!(DataType::from_sql("boolean"), Some(DataType::Boolean));
        assert_eq!(DataType::from_sql("nope"), None);
        assert_eq!(DataType::Bigint.name(), "BIGINT");
    }

    #[test]
    fn value_display() {
        assert_eq!(Value::Null.to_string(), "NULL");
        assert_eq!(Value::Integer(-3).to_string(), "-3");
        assert_eq!(Value::Varchar("hi".to_string()).to_string(), "hi");
    }

    #[test]
    fn calendar_roundtrips() {
        // Known anchors + leap day + pre-epoch.
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(parse_date("2024-02-29"), Some(days_from_civil(2024, 2, 29)));
        assert_eq!(parse_date("1969-12-31"), Some(-1));
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        // Spot-check 400-year cycle boundaries against each other.
        for d in [-719468, -1, 0, 1, 365, 366, 146097, 200000] {
            let (y, m, dd) = civil_from_days(d);
            assert_eq!(days_from_civil(y, m, dd), d, "day {d}");
        }
        // Strictness.
        assert_eq!(parse_date("2023-02-29"), None);
        assert_eq!(parse_date("2024-13-01"), None);
        assert_eq!(parse_date("2024-1-1"), None);
        assert_eq!(parse_date("not-a-date"), None);
        assert_eq!(format_date(0), "1970-01-01");
    }

    #[test]
    fn timestamp_parse_format() {
        assert_eq!(parse_timestamp("1970-01-01 00:00:00"), Some(0));
        assert_eq!(parse_timestamp("1970-01-01T00:00:01.5"), Some(1_500_000));
        assert_eq!(parse_timestamp("2024-03-15 12:30:45"), parse_timestamp("2024-03-15T12:30:45"));
        assert_eq!(format_timestamp(0), "1970-01-01 00:00:00");
        assert_eq!(format_timestamp(1_500_000), "1970-01-01 00:00:01.500000");
        assert_eq!(parse_timestamp("2024-03-15"), None);
        assert_eq!(parse_timestamp("2024-03-15 25:00:00"), None);
        assert_eq!(parse_timestamp("2024-03-15 12:00:00."), None);
        // Format→parse round trip.
        for us in [0, 1, -1, 86_400_000_000, 1_234_567_890_123_456] {
            assert_eq!(parse_timestamp(&format_timestamp(us)), Some(us), "us {us}");
        }
    }
}
