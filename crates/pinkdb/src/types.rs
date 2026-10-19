//! Core types: [`DataType`], [`Value`], [`PinkDbError`].
//!
//! v1 types: BOOLEAN, INTEGER (i32), BIGINT (i64), DOUBLE (f64), VARCHAR,
//! DATE (i32 days since 1970-01-01), TIMESTAMP (i64 microseconds since
//! 1970-01-01 00:00:00, naive — no time zone), DECIMAL (i128 unscaled value +
//! scale, DuckDB `DECIMAL(p,s)` semantics) and INTERVAL (months/days/micros,
//! the DuckDB `interval_t` layout).

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
    /// Byte-slice keys (encoded group/join keys, composite row keys) mix one
    /// whole 8-byte word per step, with the ≤ 7-byte tail folded in as one
    /// partially-filled word — the same single-step mix `write_u64` uses.
    /// Every byte still feeds the hash; this is just as deterministic (no
    /// per-run seed), so results never depend on hash order.
    fn write(&mut self, bytes: &[u8]) {
        let mut words = bytes.chunks_exact(8);
        for word in &mut words {
            self.0 ^= u64::from_le_bytes(word.try_into().expect("chunks_exact(8)"));
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
        let tail = words.remainder();
        if !tail.is_empty() {
            let mut word = 0u64;
            for (i, &b) in tail.iter().enumerate() {
                word |= (b as u64) << (8 * i);
            }
            self.0 ^= word;
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
    }

    /// One FNV step over the whole word instead of the default 8-iteration
    /// byte loop: integer-keyed maps (i64 join keys) hash through here.
    fn write_u64(&mut self, x: u64) {
        self.0 ^= x;
        self.0 = self.0.wrapping_mul(0x100000001b3);
    }

    /// `i64: Hash` calls `write_i64`, not `write_u64`: funnel it to the same
    /// single-step mix.
    fn write_i64(&mut self, x: i64) {
        self.write_u64(x as u64);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// HashMap with FNV-1a hashing for hot grouping paths.
pub type FastMap<K, V> = std::collections::HashMap<K, V, FnvBuild>;

/// HashSet with FNV-1a hashing (IN-sets, DISTINCT).
pub type FastSet<T> = std::collections::HashSet<T, FnvBuild>;

/// Maximum DECIMAL precision (total digits), DuckDB's cap.
pub const DECIMAL_MAX_PRECISION: u8 = 38;
/// `DECIMAL` without parameters is `DECIMAL(18,3)` (DuckDB rule).
pub const DECIMAL_DEFAULT_PRECISION: u8 = 18;
/// See [`DECIMAL_DEFAULT_PRECISION`].
pub const DECIMAL_DEFAULT_SCALE: u8 = 3;

/// Powers of ten as i128: `POW10_I128[s] == 10^s`, exact up to 10^38 (which
/// still fits in i128; 10^39 would not).
pub(crate) const POW10_I128: [i128; DECIMAL_MAX_PRECISION as usize + 1] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
    10_000_000_000_000_000_000,
    100_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000_000,
    1_000_000_000_000_000_000_000_000_000_000_000_000,
    10_000_000_000_000_000_000_000_000_000_000_000_000,
    100_000_000_000_000_000_000_000_000_000_000_000_000,
];

/// The same powers as f64 (DuckDB's `DOUBLE_POWERS_OF_TEN`). Values above
/// 10^22 are the nearest double, exactly like DuckDB's table.
pub(crate) const POW10_F64: [f64; DECIMAL_MAX_PRECISION as usize + 1] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31, 1e32,
    1e33, 1e34, 1e35, 1e36, 1e37, 1e38,
];

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
    /// Fixed-point numeric: `precision` total digits (1..=38), `scale` digits
    /// after the point (`0..=precision`). The stored value is the i128
    /// unscaled integer `value * 10^scale`.
    Decimal {
        precision: u8,
        scale: u8,
    },
    /// Calendar interval (months, days, microseconds). DuckDB's three-field
    /// interval representation.
    Interval,
}

impl DataType {
    /// DuckDB-style display name.
    pub fn name(self) -> String {
        match self {
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Integer => "INTEGER".to_string(),
            DataType::Bigint => "BIGINT".to_string(),
            DataType::Double => "DOUBLE".to_string(),
            DataType::Varchar => "VARCHAR".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::Timestamp => "TIMESTAMP".to_string(),
            DataType::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
            DataType::Interval => "INTERVAL".to_string(),
        }
    }

    /// Parse a bare type name (`CREATE TABLE`/`CAST`), case-insensitive.
    /// Aliases: INT → INTEGER, TEXT/STRING → VARCHAR, FLOAT → DOUBLE,
    /// DATETIME → TIMESTAMP, NUMERIC/DEC → DECIMAL (default `(18,3)`).
    pub fn from_sql(s: &str) -> Option<DataType> {
        match s.to_ascii_uppercase().as_str() {
            "BOOLEAN" | "BOOL" => Some(DataType::Boolean),
            "INTEGER" | "INT" => Some(DataType::Integer),
            "BIGINT" | "LONG" => Some(DataType::Bigint),
            "DOUBLE" | "FLOAT" | "REAL" => Some(DataType::Double),
            "VARCHAR" | "TEXT" | "STRING" => Some(DataType::Varchar),
            "DATE" => Some(DataType::Date),
            "TIMESTAMP" | "DATETIME" => Some(DataType::Timestamp),
            "DECIMAL" | "NUMERIC" | "DEC" => Some(DataType::Decimal {
                precision: DECIMAL_DEFAULT_PRECISION,
                scale: DECIMAL_DEFAULT_SCALE,
            }),
            "INTERVAL" => Some(DataType::Interval),
            _ => None,
        }
    }

    /// Parse a type name with optional parameters: `DECIMAL(p,s)`,
    /// `DECIMAL(p)`, `NUMERIC(p,s)`, or a bare name (see [`Self::from_sql`]).
    /// Parameterized names are only meaningful for DECIMAL; a parameter list
    /// on any other type is rejected (`None`).
    pub fn from_sql_params(name: &str, params: &[i64]) -> Option<DataType> {
        if params.is_empty() {
            return Self::from_sql(name);
        }
        match name.to_ascii_uppercase().as_str() {
            "DECIMAL" | "NUMERIC" | "DEC" => match params {
                [p] => {
                    let precision = u8::try_from(*p).ok()?;
                    decimal_type(precision, 0)
                }
                [p, s] => {
                    let precision = u8::try_from(*p).ok()?;
                    let scale = u8::try_from(*s).ok()?;
                    decimal_type(precision, scale)
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// True for the numeric types (DECIMAL included): values that support
    /// arithmetic and ordered comparison.
    pub fn is_numeric(self) -> bool {
        matches!(
            self,
            DataType::Integer | DataType::Bigint | DataType::Double | DataType::Decimal { .. }
        )
    }

    /// The (precision, scale) of a DECIMAL, else `None`.
    pub fn decimal_params(self) -> Option<(u8, u8)> {
        match self {
            DataType::Decimal { precision, scale } => Some((precision, scale)),
            _ => None,
        }
    }
}

/// Validate DECIMAL parameters like DuckDB (precision 1..=38, scale ≤
/// precision).
pub fn decimal_type(precision: u8, scale: u8) -> Option<DataType> {
    if precision == 0 || precision > DECIMAL_MAX_PRECISION || scale > precision {
        return None;
    }
    Some(DataType::Decimal { precision, scale })
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name())
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
    /// Unscaled fixed-point value and its scale: the value is
    /// `value / 10^scale`.
    Decimal(i128, u8),
    /// Calendar interval (DuckDB's three-field layout).
    Interval {
        months: i32,
        days: i32,
        micros: i64,
    },
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
            // A bare DECIMAL value carries its scale; its precision is the
            // minimal width that holds it (the authoritative precision lives
            // in the column/bound-expression type).
            Value::Decimal(v, scale) => Some(DataType::Decimal {
                precision: decimal_digits(*v)
                    .max(*scale as u32)
                    .clamp(1, DECIMAL_MAX_PRECISION as u32) as u8,
                scale: *scale,
            }),
            Value::Interval { .. } => Some(DataType::Interval),
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
            Value::Decimal(v, scale) => write!(f, "{}", format_decimal(*v, *scale)),
            Value::Interval {
                months,
                days,
                micros,
            } => write!(f, "{}", format_interval(*months, *days, *micros)),
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

/// `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` → microseconds since the Unix epoch: the
/// ISO-8601 UTC form pink-lake manifests store as `updated` (millisecond
/// precision). A missing fraction and up to 6 fractional digits are accepted;
/// the trailing `Z` is required. Anything else is `None` — callers report a
/// malformed record rather than guessing an instant.
///
/// This is deliberately separate from [`parse_timestamp`]: that one parses the
/// engine's naive TIMESTAMP literals (no zone suffix). It exists so the
/// `AT (TIMESTAMP => …)` clause can accept the exact string `.lake snapshots`
/// prints, and so the lake layer validates manifest times with the engine's
/// own date rules.
pub fn parse_iso8601_utc(s: &str) -> Option<i64> {
    let date = s.strip_suffix('Z')?;
    let (date_part, time_part) = date.split_once('T')?;
    let shaped = date_part.len() == 10
        && date_part.bytes().enumerate().all(|(i, b)| {
            matches!(i, 4 | 7)
                .then(|| b == b'-')
                .unwrap_or(b.is_ascii_digit())
        });
    if !shaped || time_part.len() < 8 {
        return None;
    }
    // Full calendar validation (leap days included) through the same parser
    // DATE literals use, so a lake time can never disagree with a query.
    let days = parse_date(date_part)? as i64;
    let (hms, frac) = match time_part.split_once('.') {
        Some((h, f)) => {
            if f.is_empty() || f.len() > 6 || !f.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            (h, Some(f))
        }
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

// ---------- decimal ----------
// DECIMAL is an i128 unscaled value plus a scale (`value / 10^scale`).
// Precision/scale promotion, rounding (half away from zero) and the 38-digit
// cap follow DuckDB 2.0.

/// Number of decimal digits of `|v|` (`0` ⇒ 1, `-5` ⇒ 1, `-10` ⇒ 2).
pub fn decimal_digits(v: i128) -> u32 {
    let a = v.unsigned_abs();
    let mut digits = 1u32;
    let mut p: u128 = 10;
    while digits < 39 {
        if a < p {
            return digits;
        }
        digits += 1;
        p = match p.checked_mul(10) {
            Some(next) => next,
            None => return digits,
        };
    }
    digits
}

/// True when the unscaled value `v` fits in `DECIMAL(precision, *)`:
/// `digits(v) <= precision`.
pub fn decimal_fits(v: i128, precision: u8) -> bool {
    v.unsigned_abs() < POW10_I128[precision as usize] as u128
}

/// Divide by a positive power of ten with half-away-from-zero rounding.
fn div_pow10_round(v: i128, p: i128) -> Option<i128> {
    debug_assert!(p > 0);
    if p == 1 {
        return Some(v);
    }
    let half = p / 2;
    if v >= 0 {
        let (q, r) = (v / p, v % p);
        Some(if r >= half { q.checked_add(1)? } else { q })
    } else {
        let pos = div_pow10_round(v.checked_neg()?, p)?;
        Some(-pos)
    }
}

/// Rescale `v / 10^from` to a `to`-digit scale. Scaling up is exact (`None`
/// on i128 overflow — the caller reports a DECIMAL overflow); scaling down
/// rounds half away from zero (DuckDB's cast rule).
pub fn rescale_decimal(v: i128, from: u8, to: u8) -> Option<i128> {
    if from == to {
        return Some(v);
    }
    if to > from {
        v.checked_mul(POW10_I128[(to - from) as usize])
    } else {
        div_pow10_round(v, POW10_I128[(from - to) as usize])
    }
}

/// Display form: always exactly `scale` digits after the point (DuckDB
/// prints trailing zeros; scale 0 has no point at all).
pub fn format_decimal(v: i128, scale: u8) -> String {
    if scale == 0 {
        return v.to_string();
    }
    let a = v.unsigned_abs();
    let p = POW10_I128[scale as usize] as u128;
    let (int, frac) = (a / p, a % p);
    let sign = if v < 0 { "-" } else { "" };
    format!("{sign}{int}.{frac:0width$}", width = scale as usize)
}

/// DECIMAL → DOUBLE: `v as f64 / 10^scale` (the same conversion DuckDB uses,
/// including its divide-by-a-power-of-ten rounding).
pub fn decimal_to_f64(v: i128, scale: u8) -> f64 {
    v as f64 / POW10_F64[scale as usize]
}

/// DOUBLE → DECIMAL: multiply by 10^scale, round half away from zero, then
/// check the precision. `None` for NaN/Inf or an out-of-range result
/// (DuckDB errors on both).
pub fn f64_to_decimal(x: f64, precision: u8, scale: u8) -> Option<i128> {
    if !x.is_finite() {
        return None;
    }
    let scaled = (x * POW10_F64[scale as usize]).round();
    if scaled.abs() >= POW10_F64[38].min(i128::MAX as f64) {
        // Anything ≥ 10^38 cannot fit any DECIMAL; i128 covers it exactly.
        return None;
    }
    let v = scaled as i128;
    if decimal_fits(v, precision) {
        Some(v)
    } else {
        None
    }
}

/// INTEGER/BIGINT → DECIMAL(precision, scale).
pub fn int_to_decimal(i: i128, precision: u8, scale: u8) -> Option<i128> {
    let v = i.checked_mul(POW10_I128[scale as usize])?;
    if decimal_fits(v, precision) {
        Some(v)
    } else {
        None
    }
}

/// Numeric ordering of two decimals that may carry different scales
/// (comparisons, ORDER BY, MIN/MAX, `IN`). Cross-multiplies by the scale
/// difference; an i128 overflow there means the scaled side provably exceeds
/// anything the other side can hold, so its sign decides.
pub fn decimal_cmp(a: i128, sa: u8, b: i128, sb: u8) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if sa == sb {
        return a.cmp(&b);
    }
    if sa < sb {
        match a.checked_mul(POW10_I128[(sb - sa) as usize]) {
            Some(x) => x.cmp(&b),
            None => {
                if a > 0 {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
        }
    } else {
        match b.checked_mul(POW10_I128[(sa - sb) as usize]) {
            Some(y) => a.cmp(&y),
            None => {
                if b > 0 {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
        }
    }
}

/// Parse a VARCHAR cell into the unscaled value of `DECIMAL(precision,
/// scale)` with DuckDB's rules: surrounding whitespace trimmed, optional
/// sign, `.5` / `5.` accepted, scientific notation accepted, rounding half
/// away from zero when the fraction is longer than `scale`, and an error
/// when the rounded value does not fit the precision.
pub fn parse_decimal_str(s: &str, precision: u8, scale: u8) -> std::result::Result<i128, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err(format!(
            "cannot parse \"{s}\" as DECIMAL({precision},{scale})"
        ));
    }
    let bad = || format!("cannot parse \"{s}\" as DECIMAL({precision},{scale})");
    let (neg, body) = match t.as_bytes()[0] {
        b'-' => (true, &t[1..]),
        b'+' => (false, &t[1..]),
        _ => (false, t),
    };
    let (mantissa, exp) = match body.split_once(['e', 'E']) {
        Some((m, e)) => {
            let e: i32 = e.parse().map_err(|_| bad())?;
            (m, e)
        }
        None => (body, 0),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((i, f)) => (i, f),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(bad());
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(bad());
    }
    // Effective number of fraction digits after the exponent shift.
    let frac_digits = frac_part.len() as i64 - exp as i64;
    let digits: String = format!("{int_part}{frac_part}");
    let mut value: i128 = digits.parse().map_err(|_| bad())?;
    if neg {
        value = -value;
    }
    match frac_digits.cmp(&(scale as i64)) {
        std::cmp::Ordering::Less => {
            // Too few fraction digits: scale up exactly.
            let shift = (scale as i64 - frac_digits) as u32;
            let mut p = 1i128;
            for _ in 0..shift {
                p = p
                    .checked_mul(10)
                    .ok_or_else(|| format!("cannot parse \"{s}\": out of range"))?;
            }
            value = value
                .checked_mul(p)
                .ok_or_else(|| format!("cannot parse \"{s}\": out of range"))?;
        }
        std::cmp::Ordering::Greater => {
            // Too many fraction digits: round half away from zero.
            let shift = (frac_digits - scale as i64) as u32;
            if shift <= 38 {
                value = div_pow10_round(value, POW10_I128[shift as usize])
                    .ok_or_else(|| format!("cannot parse \"{s}\": out of range"))?;
            } else if value != 0 {
                // Dropping more than 38 digits always rounds to zero unless
                // the value itself is 0 — but a nonzero value that small
                // rounds to zero too (it is < 0.5 * 10^-scale at most).
                value = 0;
            }
        }
        std::cmp::Ordering::Equal => {}
    }
    if !decimal_fits(value, precision) {
        return Err(format!(
            "could not convert string \"{s}\" to DECIMAL({precision},{scale})"
        ));
    }
    Ok(value)
}

// ---------- interval ----------
// DuckDB's interval_t: months + days + microseconds, displayed with each
// field's own sign. Comparisons normalize months to 30-day months and days
// to microseconds (DuckDB rule: `1 month = 30 days` exactly).

/// One calendar interval (`months`, `days`, `micros`), the payload layout of
/// an INTERVAL vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

const US_PER_DAY: i64 = 86_400_000_000;
/// The interval comparison equivalence: `months * 30 days + days + micros`.
pub(crate) fn interval_key(months: i32, days: i32, micros: i64) -> i128 {
    months as i128 * 30 * US_PER_DAY as i128 + days as i128 * US_PER_DAY as i128 + micros as i128
}

/// Ordering of two intervals under DuckDB's comparison rule (months → 30
/// days, days → microseconds, then a plain integer compare).
pub fn interval_cmp(a: Interval, b: Interval) -> std::cmp::Ordering {
    interval_key(a.months, a.days, a.micros).cmp(&interval_key(b.months, b.days, b.micros))
}

/// `-iv`, field-wise (wrapping like DuckDB's i32/i64 negation overflow).
pub fn interval_negate(iv: Interval) -> Interval {
    Interval {
        months: iv.months.wrapping_neg(),
        days: iv.days.wrapping_neg(),
        micros: iv.micros.wrapping_neg(),
    }
}

/// `a + b`, field-wise with checked overflow.
pub fn interval_add(a: Interval, b: Interval) -> Option<Interval> {
    Some(Interval {
        months: a.months.checked_add(b.months)?,
        days: a.days.checked_add(b.days)?,
        micros: a.micros.checked_add(b.micros)?,
    })
}

/// `a - b`, field-wise with checked overflow.
pub fn interval_sub(a: Interval, b: Interval) -> Option<Interval> {
    interval_add(a, interval_negate(b))
}

// ---------- materialized-view refresh policy (wave 80) ----------

/// How a materialized view keeps itself fresh.
///
/// The policy is part of the view's catalog entry and is persisted in `.pink`
/// (`CREATE MATERIALIZED VIEW v REFRESH <policy> AS SELECT ...`). Enforcement
/// is daemonless: `Every` and `OnCommit` views are recomputed at statement
/// boundaries, never inside an open transaction. [`crate::mv`] documents the
/// exact freshness guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefreshPolicy {
    /// Only an explicit `REFRESH` recomputes the view (the default).
    Manual,
    /// Recompute at the first statement boundary where at least `interval`
    /// elapsed since the last refresh attempt. Calendar fields follow DuckDB's
    /// interval comparison rule (1 month = 30 days, 1 day = 24 h).
    Every { interval: Interval },
    /// Recompute at the first statement boundary after a committed unit wrote
    /// one of the view's base tables (or refreshed a base view).
    OnCommit,
}

impl RefreshPolicy {
    /// The `Every` interval in microseconds under the interval comparison
    /// rule (months → 30 days, days → 24 h), if this policy has one.
    pub fn interval_micros(&self) -> Option<i128> {
        match self {
            RefreshPolicy::Every { interval } => Some(interval_key(
                interval.months,
                interval.days,
                interval.micros,
            )),
            _ => None,
        }
    }

    /// Is an `Every` view due `elapsed_us` microseconds after its last
    /// attempt? A non-positive interval is due immediately (never true for
    /// `Manual`/`OnCommit`).
    pub fn due(&self, elapsed_us: i128) -> bool {
        self.interval_micros().is_some_and(|iv| elapsed_us >= iv)
    }
}

impl std::fmt::Display for RefreshPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefreshPolicy::Manual => write!(f, "manual"),
            RefreshPolicy::OnCommit => write!(f, "on commit"),
            RefreshPolicy::Every { interval } => write!(
                f,
                "every {}",
                format_interval(interval.months, interval.days, interval.micros)
            ),
        }
    }
}

/// Interval × float: months and days multiply field-wise, and each fractional
/// carry is pushed down (fractional months → 30-day days, fractional days →
/// microseconds); micros round half to even (DuckDB's rule, pinned by
/// `INTERVAL '1 microsecond' / 2` → `00:00:00`).
fn interval_scale_fields(months: f64, days: f64, micros: f64) -> Option<Interval> {
    let whole_months = months.trunc();
    let frac_days = (months - whole_months) * 30.0;
    let total_days = days + frac_days;
    let whole_days = total_days.trunc();
    let frac_micros = (total_days - whole_days) * US_PER_DAY as f64;
    let total_micros = micros + frac_micros;
    if !(-2_147_483_648.0..2_147_483_648.0).contains(&whole_months) {
        return None;
    }
    if !(-2_147_483_648.0..2_147_483_648.0).contains(&whole_days) {
        return None;
    }
    if !total_micros.is_finite() || total_micros.abs() >= 9_223_372_036_854_775_808.0 {
        return None;
    }
    Some(Interval {
        months: whole_months as i32,
        days: whole_days as i32,
        micros: total_micros.round_ties_even() as i64,
    })
}

/// `iv * f`.
pub fn interval_mul(iv: Interval, f: f64) -> Option<Interval> {
    interval_scale_fields(
        iv.months as f64 * f,
        iv.days as f64 * f,
        iv.micros as f64 * f,
    )
}

/// `iv / f` (`None` for a zero divisor — the executor reports division by
/// zero).
pub fn interval_div(iv: Interval, f: f64) -> Option<Interval> {
    if f == 0.0 {
        return None;
    }
    interval_mul(iv, 1.0 / f)
}

fn plural(n: i64) -> &'static str {
    if n == 1 || n == -1 { "" } else { "s" }
}

/// `HH:MM:SS[.ffffff]` with trailing fraction zeros trimmed; hours may run
/// past 23 (`26:00:00`) and the sign is the micros field's own.
fn format_interval_time(micros: i64) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let a = micros.unsigned_abs();
    let (h, rest) = (a / 3_600_000_000, a % 3_600_000_000);
    let (m, rest) = (rest / 60_000_000, rest % 60_000_000);
    let (s, us) = (rest / 1_000_000, rest % 1_000_000);
    if us == 0 {
        return format!("{sign}{h:02}:{m:02}:{s:02}");
    }
    let mut frac = format!("{us:06}");
    while frac.ends_with('0') {
        frac.pop();
    }
    format!("{sign}{h:02}:{m:02}:{s:02}.{frac}")
}

/// DuckDB's interval display: `<n> year[s] <n> month[s] <n> day[s]
/// HH:MM:SS[.fff]`, each field signed on its own, the time part omitted when
/// all calendar fields cover it, and `00:00:00` for the zero interval.
pub fn format_interval(months: i32, days: i32, micros: i64) -> String {
    let years = months / 12;
    let rem_months = months % 12;
    let mut parts: Vec<String> = Vec::new();
    if years != 0 {
        parts.push(format!("{years} year{}", plural(years as i64)));
    }
    if rem_months != 0 {
        parts.push(format!("{rem_months} month{}", plural(rem_months as i64)));
    }
    if days != 0 {
        parts.push(format!("{days} day{}", plural(days as i64)));
    }
    if parts.is_empty() || micros != 0 {
        parts.push(format_interval_time(micros));
    }
    parts.join(" ")
}

fn interval_unit_multiplier(unit: &str) -> Option<f64> {
    Some(match unit.to_ascii_lowercase().as_str() {
        "microsecond" | "microseconds" | "us" | "usec" | "usecs" => 1.0,
        "millisecond" | "milliseconds" | "ms" | "msec" | "msecs" => 1_000.0,
        "second" | "seconds" | "sec" | "secs" | "s" => 1_000_000.0,
        "minute" | "minutes" | "min" | "mins" | "m" => 60_000_000.0,
        "hour" | "hours" | "hr" | "hrs" | "h" => 3_600_000_000.0,
        "day" | "days" | "d" => 86_400_000_000.0,
        "week" | "weeks" | "w" | "wk" | "wks" => 7.0 * 86_400_000_000.0,
        "month" | "months" | "mon" | "mons" => 30.0 * 86_400_000_000.0,
        "year" | "years" | "y" | "yr" | "yrs" => 360.0 * 86_400_000_000.0,
        "decade" | "decades" => 3_600.0 * 86_400_000_000.0,
        "century" | "centuries" => 36_000.0 * 86_400_000_000.0,
        "millennium" | "millennia" | "millenium" | "milleniums" | "millenniums" => {
            360_000.0 * 86_400_000_000.0
        }
        _ => return None,
    })
}

/// `HH:MM:SS[.ffffff]` with an optional sign → microseconds.
fn parse_interval_time_part(t: &str) -> Option<i64> {
    let (neg, body) = match t.as_bytes().first()? {
        b'-' => (true, &t[1..]),
        b'+' => (false, &t[1..]),
        _ => (false, t),
    };
    let mut fields = body.split(':');
    let h: i64 = fields.next()?.parse().ok()?;
    let m: i64 = fields.next()?.parse().ok()?;
    let sec_part = fields.next()?;
    if fields.next().is_some() {
        return None;
    }
    let (s, frac_us) = match sec_part.split_once('.') {
        Some((s, f)) => {
            if f.is_empty() || f.len() > 6 || !f.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let mut us: i64 = f.parse().ok()?;
            for _ in f.len()..6 {
                us *= 10;
            }
            (s.parse::<i64>().ok()?, us)
        }
        None => (sec_part.parse::<i64>().ok()?, 0),
    };
    if h < 0 || !(0..=59).contains(&m) || !(0..=60).contains(&s) {
        return None;
    }
    let total = h
        .checked_mul(3_600_000_000)?
        .checked_add(m.checked_mul(60_000_000)?)?
        .checked_add(s.checked_mul(1_000_000)?)?
        .checked_add(frac_us)?;
    Some(if neg { -total } else { total })
}

/// Parse DuckDB's interval strings: space-separated `<number> <unit>` parts
/// (`1 day 2 hours`, `90 minutes`, `1.5 days`, `1 year 2 months`), a
/// `HH:MM:SS[.f]` time part, a bare number (seconds), and the `ago` suffix
/// (negates). Fractional units carry down exactly like DuckDB: `1.5 years`
/// = 18 months, `0.5 months` = 15 days, `1.5 days` = 1 day 12:00:00.
pub fn parse_interval(s: &str) -> Option<Interval> {
    let mut text = s.trim();
    if text.is_empty() {
        return None;
    }
    let mut negate = false;
    if text.len() > 3
        && text
            .get(text.len() - 3..)
            .is_some_and(|s| s.eq_ignore_ascii_case("ago"))
    {
        negate = true;
        text = text[..text.len() - 3].trim_end();
    }
    let mut months = 0f64;
    let mut days = 0f64;
    let mut micros = 0f64;
    let mut tokens = text.split_whitespace().peekable();
    let mut saw_any = false;
    while let Some(tok) = tokens.next() {
        saw_any = true;
        if tok.contains(':') {
            let us = parse_interval_time_part(tok)?;
            micros += us as f64;
            continue;
        }
        let value: f64 = tok.parse().ok()?;
        if !value.is_finite() {
            return None;
        }
        // The unit is the *next* token when it is one; a number with no unit
        // is seconds (DuckDB's `CAST('5' AS INTERVAL)`).
        let unit_us = match tokens.peek().copied().and_then(interval_unit_multiplier) {
            Some(mult) => {
                tokens.next();
                mult
            }
            None => 1_000_000.0,
        };
        let us = value * unit_us;
        // Route the contribution to the field its unit belongs to so
        // fractional carries behave like DuckDB's.
        if unit_us >= 30.0 * 86_400_000_000.0 {
            months += us / (30.0 * 86_400_000_000.0);
        } else if unit_us >= 86_400_000_000.0 {
            days += us / 86_400_000_000.0;
        } else {
            micros += us;
        }
    }
    if !saw_any {
        return None;
    }
    let iv = interval_scale_fields(months, days, micros)?;
    Some(if negate { interval_negate(iv) } else { iv })
}

/// Add `months` to a day count, clamping the day to the target month's last
/// day (DuckDB month-end rule: 2024-01-31 + 1 month = 2024-02-29).
pub fn add_months_clamped(days: i32, months: i32) -> Option<i32> {
    let (y, m, d) = civil_from_days(days);
    let total = (y as i64) * 12 + (m as i64 - 1) + months as i64;
    let ny = total.div_euclid(12);
    let nm = (total.rem_euclid(12) + 1) as u32;
    if !(-999_999..=999_999).contains(&ny) {
        return None;
    }
    let nd = d.min(days_in_month(ny as i32, nm));
    Some(days_from_civil(ny as i32, nm, nd))
}

/// `date + interval` → microseconds since epoch (DuckDB: months first, with
/// day clamping, then days, then the time part).
pub fn date_add_interval(days: i32, iv: Interval) -> Option<i64> {
    let d = add_months_clamped(days, iv.months)? as i64;
    let d = d.checked_add(iv.days as i64)?;
    if !(i32::MIN as i64..=i32::MAX as i64).contains(&d) {
        return None;
    }
    d.checked_mul(US_PER_DAY)?.checked_add(iv.micros)
}

/// `timestamp + interval` → microseconds since epoch. The month part clamps
/// the date and keeps the time of day.
pub fn timestamp_add_interval(us: i64, iv: Interval) -> Option<i64> {
    let days = us.div_euclid(US_PER_DAY) as i32;
    let time = us.rem_euclid(US_PER_DAY);
    let d = add_months_clamped(days, iv.months)? as i64;
    let d = d.checked_add(iv.days as i64)?;
    d.checked_mul(US_PER_DAY)?
        .checked_add(time)?
        .checked_add(iv.micros)
}

// ---------- extract/date_part fields ----------

/// A recognized `extract(field FROM x)` / `date_part('field', x)` specifier.
/// Only the fields this engine implements are listed (DuckDB has more:
/// timezone/era/julian/...).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Epoch,
    DayOfWeek,
    IsoDayOfWeek,
    DayOfYear,
    Week,
    Quarter,
    IsoYear,
    Century,
    Decade,
    Millennium,
}

/// Resolve a DuckDB field spelling (case-insensitive, singular or plural,
/// `ms`/`us`/`sec` shorthands accepted) to an [`ExtractField`].
pub fn extract_field(part: &str) -> std::result::Result<ExtractField, String> {
    let p = part.trim().to_ascii_lowercase();
    Ok(match p.as_str() {
        "year" | "years" | "y" | "yr" | "yrs" => ExtractField::Year,
        "month" | "months" | "mon" | "mons" => ExtractField::Month,
        "day" | "days" | "d" => ExtractField::Day,
        "hour" | "hours" | "h" | "hr" | "hrs" => ExtractField::Hour,
        "minute" | "minutes" | "min" | "mins" => ExtractField::Minute,
        "second" | "seconds" | "sec" | "secs" | "s" => ExtractField::Second,
        "millisecond" | "milliseconds" | "ms" | "msec" | "msecs" => ExtractField::Millisecond,
        "microsecond" | "microseconds" | "us" | "usec" | "usecs" => ExtractField::Microsecond,
        "epoch" | "epochs" => ExtractField::Epoch,
        "dow" | "dayofweek" => ExtractField::DayOfWeek,
        "isodow" => ExtractField::IsoDayOfWeek,
        "doy" | "dayofyear" => ExtractField::DayOfYear,
        "week" | "weeks" | "isoweek" => ExtractField::Week,
        "quarter" | "quarters" => ExtractField::Quarter,
        "isoyear" => ExtractField::IsoYear,
        "century" | "centuries" => ExtractField::Century,
        "decade" | "decades" => ExtractField::Decade,
        "millennium" | "millenniums" | "millennia" | "milleniums" | "millenia" => {
            ExtractField::Millennium
        }
        _ => return Err(format!("extract specifier \"{part}\" not recognized")),
    })
}

/// All engine errors. Lean: a few string-carrying variants + transparent
/// IO/CSV wrappers. No extra deps (no thiserror).
#[derive(Debug)]
pub enum PinkDbError {
    Catalog(String),
    Type(String),
    Parse(String),
    /// A prepared-statement parameter failed to bind: missing/extra values,
    /// an untypable `?`, or a value that cannot convert to the inferred type.
    Parameter(String),
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
            | PinkDbError::Parameter(msg)
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
    fn pow10_tables_are_exact() {
        let mut p: i128 = 1;
        for (i, want) in POW10_I128.iter().enumerate() {
            assert_eq!(*want, p, "POW10_I128[{i}]");
            if i + 1 < POW10_I128.len() {
                p = p.checked_mul(10).expect("powers below 10^38 fit");
            }
        }
        assert!(p.checked_mul(10).is_none(), "10^39 must overflow i128");
        // f64 powers of ten are representable only within rounding: compare
        // against repeated multiplication with a tight relative tolerance
        // (the tables feed double↔decimal conversions, where 1 ulp is noise).
        let mut f: f64 = 1.0;
        for (i, want) in POW10_F64.iter().enumerate() {
            let rel = (want - f).abs() / want.abs().max(1.0);
            assert!(rel <= 1e-15, "POW10_F64[{i}]: {want} vs {f} (rel {rel})");
            f *= 10.0;
        }
        assert_eq!(POW10_F64[38], 1e38);
    }

    #[test]
    fn decimal_type_params_validate() {
        assert_eq!(
            decimal_type(4, 2),
            Some(DataType::Decimal {
                precision: 4,
                scale: 2
            })
        );
        assert_eq!(decimal_type(0, 0), None);
        assert_eq!(decimal_type(39, 2), None);
        assert_eq!(decimal_type(2, 3), None);
        assert_eq!(
            DataType::from_sql("decimal"),
            Some(DataType::Decimal {
                precision: 18,
                scale: 3
            })
        );
        assert_eq!(
            DataType::from_sql_params("DECIMAL", &[4, 2]),
            Some(DataType::Decimal {
                precision: 4,
                scale: 2
            })
        );
        assert_eq!(
            DataType::from_sql_params("DECIMAL", &[4]),
            Some(DataType::Decimal {
                precision: 4,
                scale: 0
            })
        );
        assert_eq!(DataType::from_sql_params("INTEGER", &[4]), None);
        assert_eq!(DataType::from_sql_params("DECIMAL", &[40, 2]), None);
        assert_eq!(
            DataType::Decimal {
                precision: 4,
                scale: 2
            }
            .name(),
            "DECIMAL(4,2)"
        );
        assert_eq!(DataType::Interval.name(), "INTERVAL");
    }

    #[test]
    fn decimal_format_and_parse() {
        assert_eq!(format_decimal(1230, 2), "12.30");
        assert_eq!(format_decimal(-5, 2), "-0.05");
        assert_eq!(format_decimal(5, 0), "5");
        assert_eq!(format_decimal(0, 3), "0.000");
        // Rounding: half away from zero (DuckDB).
        assert_eq!(parse_decimal_str("12.345", 4, 2).unwrap(), 1235);
        assert_eq!(parse_decimal_str("-12.345", 4, 2).unwrap(), -1235);
        assert_eq!(parse_decimal_str("0.00001", 6, 2).unwrap(), 0);
        assert_eq!(parse_decimal_str("-0.005", 6, 2).unwrap(), -1);
        assert_eq!(parse_decimal_str("1e2", 6, 2).unwrap(), 10_000);
        assert_eq!(parse_decimal_str(" 12.34 ", 6, 2).unwrap(), 1234);
        assert_eq!(parse_decimal_str(".5", 6, 2).unwrap(), 50);
        assert_eq!(parse_decimal_str("5.", 6, 2).unwrap(), 500);
        assert_eq!(parse_decimal_str("1.2e-1", 6, 2).unwrap(), 12);
        // Precision overflow (after rounding) is an error.
        assert!(parse_decimal_str("123.45", 4, 2).is_err());
        assert!(parse_decimal_str("999.999", 4, 2).is_err());
        assert!(parse_decimal_str("abc", 4, 2).is_err());
        assert!(parse_decimal_str("", 4, 2).is_err());
        // rescale
        assert_eq!(rescale_decimal(1234, 2, 3).unwrap(), 12340);
        assert_eq!(rescale_decimal(1235, 2, 1).unwrap(), 124);
        assert_eq!(rescale_decimal(-15, 1, 0).unwrap(), -2);
        assert_eq!(rescale_decimal(14, 1, 0).unwrap(), 1);
        assert_eq!(
            decimal_cmp(150, 2, 15, 1),
            std::cmp::Ordering::Equal,
            "1.50 == 1.5"
        );
        assert_eq!(decimal_cmp(1, 0, 999, 3), std::cmp::Ordering::Greater);
        assert_eq!(decimal_cmp(-1, 0, -999, 3), std::cmp::Ordering::Less);
    }

    #[test]
    fn interval_parse_and_format() {
        let cases = [
            ("1 day", "1 day"),
            ("2 months", "2 months"),
            ("12 months", "1 year"),
            ("13 months", "1 year 1 month"),
            ("-13 months", "-1 year -1 month"),
            ("36 months", "3 years"),
            ("1 day 02:00:00", "1 day 02:00:00"),
            ("0 days", "00:00:00"),
            ("5", "00:00:05"),
            ("90 minutes", "01:30:00"),
            ("90 seconds", "00:01:30"),
            ("1.5 days", "1 day 12:00:00"),
            ("1.5 years", "1 year 6 months"),
            ("0.5 months", "15 days"),
            (
                "1 year 2 months 3 days 04:05:06.789",
                "1 year 2 months 3 days 04:05:06.789",
            ),
            ("01:02:03.5", "01:02:03.5"),
            ("1:2:3", "01:02:03"),
            ("2 days ago", "-2 days"),
            ("-1.5 days", "-1 day -12:00:00"),
            ("1 month -31 days", "1 month -31 days"),
            ("1 microsecond", "00:00:00.000001"),
            ("26 hours", "26:00:00"),
            ("100 hours", "100:00:00"),
            ("1 day 2 hours", "1 day 02:00:00"),
            ("1 d 2 h", "1 day 02:00:00"),
            ("2 hrs", "02:00:00"),
            ("1 week", "7 days"),
            ("  1 day  ", "1 day"),
        ];
        for (input, want) in cases {
            let iv = parse_interval(input).unwrap_or_else(|| panic!("parse {input:?}"));
            assert_eq!(
                format_interval(iv.months, iv.days, iv.micros),
                want,
                "input {input:?}"
            );
        }
        for bad in ["", "1-2", "1 day, 2 hours", "abc", "1 foo"] {
            assert!(parse_interval(bad).is_none(), "should reject {bad:?}");
        }
        // Comparison normalizes months to 30 days.
        let one_month = parse_interval("1 month").unwrap();
        let thirty_days = parse_interval("30 days").unwrap();
        let thirty_one_days = parse_interval("31 days").unwrap();
        let year = parse_interval("1 year").unwrap();
        let days_360 = parse_interval("360 days").unwrap();
        assert_eq!(
            interval_cmp(one_month, thirty_days),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            interval_cmp(one_month, thirty_one_days),
            std::cmp::Ordering::Less
        );
        assert_eq!(interval_cmp(year, days_360), std::cmp::Ordering::Equal);
        // Multiplication carries fractions down (DuckDB rules).
        let iv = interval_mul(parse_interval("1 day").unwrap(), 2.5).unwrap();
        assert_eq!(
            format_interval(iv.months, iv.days, iv.micros),
            "2 days 12:00:00"
        );
        let iv = interval_mul(parse_interval("1 month").unwrap(), 1.5).unwrap();
        assert_eq!(
            format_interval(iv.months, iv.days, iv.micros),
            "1 month 15 days"
        );
        let iv = interval_div(parse_interval("1 microsecond").unwrap(), 2.0).unwrap();
        assert_eq!(iv.micros, 0, "0.5 micros rounds to even (DuckDB)");
        let iv = interval_div(parse_interval("1 day").unwrap(), 2.0).unwrap();
        assert_eq!(format_interval(iv.months, iv.days, iv.micros), "12:00:00");
    }

    #[test]
    fn interval_date_arithmetic_clamps_months() {
        let m1 = parse_interval("1 month").unwrap();
        let d1 = parse_interval("1 day").unwrap();
        let jan31 = parse_date("2024-01-31").unwrap();
        // 2024-01-31 + 1 month → 2024-02-29 (month-end clamp).
        let us = date_add_interval(jan31, m1).unwrap();
        assert_eq!(format_timestamp(us), "2024-02-29 00:00:00");
        // A second month starts from Feb 29, so it lands on Mar 29 (no
        // clamp-back-up).
        let d = (us.div_euclid(86_400_000_000)) as i32;
        let us2 = date_add_interval(d, m1).unwrap();
        assert_eq!(format_timestamp(us2), "2024-03-29 00:00:00");
        // 2024-01-30 + 1 month → 2024-02-29.
        let jan30 = parse_date("2024-01-30").unwrap();
        assert_eq!(
            format_timestamp(date_add_interval(jan30, m1).unwrap()),
            "2024-02-29 00:00:00"
        );
        // 2024-03-31 + 1 month → 2024-04-30.
        let mar31 = parse_date("2024-03-31").unwrap();
        assert_eq!(
            format_timestamp(date_add_interval(mar31, m1).unwrap()),
            "2024-04-30 00:00:00"
        );
        // 2024-02-29 + 1 year → 2025-02-28.
        let leap = parse_date("2024-02-29").unwrap();
        let y1 = parse_interval("1 year").unwrap();
        assert_eq!(
            format_timestamp(date_add_interval(leap, y1).unwrap()),
            "2025-02-28 00:00:00"
        );
        // Time part rides along for timestamps.
        let ts = parse_timestamp("2024-01-31 10:00:00").unwrap();
        assert_eq!(
            format_timestamp(timestamp_add_interval(ts, m1).unwrap()),
            "2024-02-29 10:00:00"
        );
        // date + interval keeps the time part of the interval.
        let m1d2h = parse_interval("1 month 02:00:00").unwrap();
        assert_eq!(
            format_timestamp(date_add_interval(jan31, m1d2h).unwrap()),
            "2024-02-29 02:00:00"
        );
        // date - interval = date + (-interval).
        let prev = interval_negate(d1);
        assert_eq!(
            format_timestamp(date_add_interval(jan31, prev).unwrap()),
            "2024-01-30 00:00:00"
        );
    }

    #[test]
    fn interval_add_sub_is_field_wise() {
        let a = parse_interval("1 day 02:00:00").unwrap();
        let b = parse_interval("1 day 02:00:00").unwrap();
        let s = interval_add(a, b).unwrap();
        assert_eq!(
            format_interval(s.months, s.days, s.micros),
            "2 days 04:00:00"
        );
        let d = interval_sub(a, b).unwrap();
        assert_eq!(format_interval(d.months, d.days, d.micros), "00:00:00");
        // Field-wise: 2 days + 25 hours keeps 25 hours (DuckDB).
        let a = parse_interval("2 days").unwrap();
        let b = parse_interval("25 hours").unwrap();
        let s = interval_add(a, b).unwrap();
        assert_eq!(
            format_interval(s.months, s.days, s.micros),
            "2 days 25:00:00"
        );
    }

    #[test]
    fn decimal_arithmetic_helpers() {
        assert_eq!(decimal_digits(0), 1);
        assert_eq!(decimal_digits(9), 1);
        assert_eq!(decimal_digits(10), 2);
        assert_eq!(decimal_digits(-123), 3);
        assert!(decimal_fits(9_999, 4));
        assert!(!decimal_fits(10_000, 4));
        assert_eq!(decimal_to_f64(1234, 2), 12.34);
        assert_eq!(
            f64_to_decimal(1.005, 6, 2),
            Some(100),
            "binary 1.005 < 1.005"
        );
        assert_eq!(
            f64_to_decimal(2.675, 6, 2),
            Some(268),
            "2.675*100 rounds to 267.5"
        );
        assert_eq!(f64_to_decimal(12.345, 4, 2), Some(1235));
        assert_eq!(f64_to_decimal(f64::NAN, 4, 2), None);
        assert_eq!(f64_to_decimal(1e20, 4, 2), None, "must not fit");
        assert_eq!(int_to_decimal(1, 4, 2), Some(100));
        assert_eq!(int_to_decimal(100, 4, 2), None);
    }

    #[test]
    fn timestamp_parse_format() {
        assert_eq!(parse_timestamp("1970-01-01 00:00:00"), Some(0));
        assert_eq!(parse_timestamp("1970-01-01T00:00:01.5"), Some(1_500_000));
        assert_eq!(
            parse_timestamp("2024-03-15 12:30:45"),
            parse_timestamp("2024-03-15T12:30:45")
        );
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

    /// The manifest `updated` form: strict `Z`-suffixed ISO-8601 UTC.
    #[test]
    fn iso8601_utc_parse() {
        assert_eq!(parse_iso8601_utc("1970-01-01T00:00:00.000Z"), Some(0));
        assert_eq!(
            parse_iso8601_utc("2026-10-17T12:30:36.029Z"),
            Some(1_792_240_236_029_000)
        );
        // A leap day, consistent with `parse_date`.
        assert_eq!(
            parse_iso8601_utc("2020-02-29T00:00:00Z"),
            parse_timestamp("2020-02-29 00:00:00")
        );
        // The exact inverse of the lake formatter for a spread of instants.
        for us in [
            0,
            1_000, // 1 ms
            86_400_000_000,
            1_792_240_236_029_000,
        ] {
            let (days, h, mi, sec, micro) = split_timestamp(us);
            let iso = format!(
                "{}T{h:02}:{mi:02}:{sec:02}.{:03}Z",
                format_date(days),
                micro / 1_000
            );
            assert_eq!(parse_iso8601_utc(&iso), Some(us / 1_000 * 1_000), "{iso}");
        }
        // Malformed or non-UTC input is a clean None, never a guess.
        for bad in [
            "2020-02-29T00:00:00",       // missing Z
            "2020-02-29T00:00:00+05:30", // offsets are not this format
            "2020-02-30T00:00:00.000Z",
            "2021-02-29T00:00:00.000Z",
            "2020-13-01T00:00:00.000Z",
            "2020-02-29 00:00:00.000Z",
            "2020-02-29T24:00:00.000Z",
            "2020-02-29T00:00:00.1234567Z",
            "2020-02-29T00:00:00.Z",
            "2020-0x-29T00:00:00Z",
        ] {
            assert_eq!(parse_iso8601_utc(bad), None, "{bad}");
        }
    }
}
