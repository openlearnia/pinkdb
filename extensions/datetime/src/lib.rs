//! pinkdb's core **datetime** extension (wave 56): fourteen DuckDB date/time
//! functions over naive `DATE`/`TIMESTAMP` values, authored with the Rust
//! extension SDK.
//!
//! | function | arguments | returns |
//! |---|---|---|
//! | `strftime(ts, format)` | TIMESTAMP, VARCHAR | VARCHAR |
//! | `strptime(text, format)` | VARCHAR, VARCHAR | TIMESTAMP |
//! | `epoch(ts)` | TIMESTAMP | DOUBLE |
//! | `epoch_ms(ts)` | TIMESTAMP | BIGINT |
//! | `dayofweek(ts)` | TIMESTAMP | BIGINT (0 = Sunday) |
//! | `dayofmonth(ts)` | TIMESTAMP | BIGINT (1-based) |
//! | `dayofyear(ts)` | TIMESTAMP | BIGINT (1-based) |
//! | `week(ts)` / `weekofyear(ts)` | TIMESTAMP | BIGINT (ISO-8601, 1..=53) |
//! | `date_diff(part, start, end)` / `datediff(…)` | VARCHAR, TIMESTAMP, TIMESTAMP | BIGINT |
//! | `last_day(ts)` | TIMESTAMP | DATE |
//! | `age(end, start)` | TIMESTAMP, TIMESTAMP | INTERVAL |
//! | `make_date(y, m, d)` | INTEGER ×3 | DATE |
//!
//! Everything is pinned against DuckDB **v2.0.0-alpha46057** with probes
//! (the same build `bench/duckdiff.py` compares against); the unit tests in
//! this crate carry the pinned values, and the `duckdiff` battery has a
//! datetime section.
//!
//! # Semantics (pinned)
//!
//! * **`epoch`** returns DOUBLE seconds with the microsecond fraction
//!   (`epoch(TIMESTAMP '1969-12-31 23:59:59.5')` = `-0.5`) — the pinned
//!   build's shape. **`epoch_ms`** returns the millisecond count *rounded to
//!   nearest, ties away from zero* (`-1 µs` → `0`, `-500 µs` → `-1`,
//!   `-999999 µs` → `-1000`), exactly the pinned build.
//! * **`dayofweek`** is 0 = Sunday … 6 = Saturday; **`dayofmonth`** is
//!   1..=31; **`dayofyear`** is 1-based; **`week`**/**`weekofyear`** is the
//!   ISO-8601 week (weeks start Monday, week 1 contains the first Thursday):
//!   2021-01-01 is 2020-W53, 2023-01-01 is 2022-W52.
//! * **`date_diff(part, start, end)`** (and DuckDB's alias `datediff`)
//!   counts the **boundaries crossed**, not whole units: year, quarter and
//!   month from the calendar fields (`date_diff('month', DATE '2020-01-31',
//!   DATE '2020-02-01')` is 1), week from whole-day differences divided by 7,
//!   day from calendar-day differences, and hour/minute/second/millisecond
//!   from the absolute unit boundaries each timestamp falls in
//!   (`date_diff('hour', TIMESTAMP '2020-01-01 00:30:00', TIMESTAMP
//!   '2020-01-01 00:59:00')` is 0). Part
//!   names are DuckDB's, case-insensitively and with its abbreviations; an
//!   unknown part is an error.
//! * **`last_day`** returns the last day of the input's month as a DATE
//!   (2024-02-01 → 2024-02-29), for DATE and TIMESTAMP inputs alike in
//!   DuckDB; here the input is a TIMESTAMP, so wrap a DATE in `::TIMESTAMP`.
//! * **`strftime`** supports the pinned build's specifier set minus time
//!   zones (`%z %Z`), nanosecond fields (`%n %N`) and its broken `%g`; every
//!   unsupported specifier fails with an error naming the supported set.
//!   `%G %V %U %W %j %c %x %X %T %f` all match the pinned build
//!   (`%f` is 6 zero-padded digits).
//! * **`strptime`** parses with the same set; week-number-to-date specifiers
//!   (`%U %W %V %G`) are refused in v1 (see the crate's honest limits).
//!   Numeric fields are greedy 1..=N-digit runs, the input must be consumed
//!   exactly, unset fields default to 1900-01-01 00:00:00, `%y` pivots at 69
//!   (00-68 → 2000s, 69-99 → 1900s), `%j` accepts 1..=366 and rolls into the
//!   next year, and `%j` wins over `%m`/`%d` when both are given.
//! * **`age(end, start)`** decomposes `end - start` exactly like the pinned
//!   build: month difference, then day difference borrowing from the months
//!   with the *start* date's month length, then the time part borrowing a day
//!   first; the arguments swap and the whole interval is negated when
//!   `end < start`. `age(DATE '2024-03-01', DATE '2023-01-15')` is
//!   `1 year 1 month 17 days` (`months = 13, days = 17`).
//! * **`make_date`** validates month/day against the calendar (leap days
//!   included) and the `i32` day range, erroring like the pinned build
//!   ("date out of range: 2023-2-29"); any NULL argument is NULL.
//! * NULL in → NULL out for every function; all fourteen are deterministic.
//!
//! # Divergences from DuckDB (documented, by design)
//!
//! * **Naive timestamps only**: no time zone types, no `TIMESTAMPTZ`, no
//!   `%z`/`%Z`, and no DST-aware arithmetic. UTC semantics in DuckDB's
//!   timezone-naive mode are identical to these functions' results.
//! * **Some argument types are exact**: the engine widens an INTEGER into a
//!   BIGINT parameter and a DATE into a TIMESTAMP one (so
//!   `dayofweek(DATE '2024-01-07')` works like DuckDB); nothing else is
//!   coerced, so a VARCHAR timestamp needs an explicit CAST.
//! * **`make_date` takes INTEGER**: `make_date(2024, 2, 29)` binds directly
//!   (DuckDB's signature is BIGINT with an implicit INTEGER→BIGINT cast); a
//!   BIGINT argument needs `CAST(x AS INTEGER)`.
//! * **`epoch` is the pinned build's DOUBLE**, not DuckDB 1.x's BIGINT:
//!   install the pinned reference build to compare (the duckdiff job does).
//! * **`strptime` refuses `%U %W %V %G`** (week-number-to-date parsing) and
//!   the timezone/nanosecond specifiers; the pinned build supports the week
//!   fields. `%g` is refused because the pinned build's `%g` output is
//!   broken (`123` for 2024); `%G` is correct and supported.
//! * **BC dates**: `make_date(-1, 1, 1)` and `strptime` with a negative `%Y`
//!   work, but the engine renders year 0 and earlier without DuckDB's
//!   "`(BC)`" suffix (the engine's existing DATE display).
//! * **`date_diff` part set**: the calendar and sub-day parts above are
//!   pinned; DuckDB's `decade`/`century`/`millennium` are refused here (it
//!   accepts them) and its `timezone` part is refused there too.
//! * **Bare `NULL` binds to any parameter**, so `dayofmonth(NULL)` is NULL
//!   here; DuckDB's binder needs a typed NULL for these (`dayofmonth(CAST(NULL
//!   AS TIMESTAMP))`), which works on both sides.
//!
//! # Activation
//!
//! The extension is published to the process-global **linked registry**
//! (`pinkdb_ext_loader::linked`), the loading model `LOAD EXTENSION '<name>'`
//! resolves on wasm and as a native fallback:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! pinkdb_datetime::ensure_registered()?;           // host bootstrap, once
//! let mut db = pinkdb::PinkDb::new();
//! db.load_linked_extension("datetime")?;           // or: LOAD EXTENSION 'datetime'
//! # Ok(())
//! # }
//! ```
//!
//! The CLI's default `datetime` feature does both at startup, so the fourteen
//! functions work out of the box; `--no-default-features` and the other
//! core extensions' features leave them out. The crate links no dependencies
//! beyond the SDK (calendar math and the format engine are hand rolled).

mod civil;
mod format;

use pinkdb_ext_sdk::linked::{self, LinkedExtension};
use pinkdb_ext_sdk::{Date, ExtError, Interval, Timestamp, pinkdb_extension};

/// The registry name: `LOAD EXTENSION 'datetime'`.
pub const EXTENSION_NAME: &str = "datetime";

// ---------- the pure functions (pure, unit-testable) ----------
//
// Each takes `Option` arguments and returns `Option`/`Result<Option<_>>` —
// NULL in, NULL out — and contains no ambient state. The `pinkdb_extension!`
// declaration at the bottom of this file registers thin wrappers with exactly
// these names as the SQL functions.

/// `epoch(ts)`: fractional seconds since the Unix epoch, as DOUBLE.
pub fn epoch_of(ts: i64) -> f64 {
    ts as f64 / 1_000_000.0
}

/// `epoch_ms(ts)`: milliseconds since the Unix epoch, rounded to nearest with
/// ties away from zero (pinned: `-1 µs` → `0`, `+500 µs` → `1`).
pub fn epoch_ms_of(ts: i64) -> i64 {
    let us = i128::from(ts);
    if us >= 0 {
        ((us + 500) / 1000) as i64
    } else {
        -((us.abs() + 500) / 1000) as i64
    }
}

/// `dayofweek(ts)`: 0 = Sunday … 6 = Saturday.
pub fn dayofweek_of(ts: i64) -> i64 {
    let (days, _) = civil::split_timestamp(ts);
    civil::weekday(days) as i64
}

/// `dayofmonth(ts)`: day of the month, 1..=31.
pub fn dayofmonth_of(ts: i64) -> i64 {
    let (days, _) = civil::split_timestamp(ts);
    let (_, _, day) = civil::civil_from_days(days);
    i64::from(day)
}

/// `dayofyear(ts)`: 1-based day of year (366 in a leap year's last day).
pub fn dayofyear_of(ts: i64) -> i64 {
    let (days, _) = civil::split_timestamp(ts);
    civil::day_of_year(days) as i64
}

/// `week(ts)`: ISO-8601 week number, 1..=53.
pub fn week_of(ts: i64) -> i64 {
    let (days, _) = civil::split_timestamp(ts);
    civil::iso_week(days).1 as i64
}

/// `date_diff(part, start, end)`: the number of `part` boundaries crossed
/// between two timestamps (DuckDB counts boundaries, not whole units).
///
/// `start`/`end` are naive microseconds since the epoch. Year, quarter and
/// month differences come from the calendar fields; week and day from whole
/// calendar days (`week` divides that difference by 7, truncating toward
/// zero); the sub-day parts from the absolute unit boundaries each timestamp
/// falls in. The
/// part name is case-insensitive and accepts DuckDB's abbreviations; anything
/// else is an error, like DuckDB's `extract specifier … not recognized`.
pub fn date_diff_of(part: &str, start: i64, end: i64) -> Result<i64, String> {
    let (start_days, _) = civil::split_timestamp(start);
    let (end_days, _) = civil::split_timestamp(end);
    let (start_year, start_month, _) = civil::civil_from_days(start_days);
    let (end_year, end_month, _) = civil::civil_from_days(end_days);
    // DuckDB's sub-day parts count the absolute unit boundaries each stamp
    // falls in (`00:30 → 00:59` is 0 hours, `00:00 → 22:30` the day before is
    // -2), which is the floor division of each side.
    let boundaries = |us: i64, per: i64| us.div_euclid(per);
    let part = part.to_ascii_lowercase();
    let diff = match part.as_str() {
        "year" | "years" | "y" | "yr" | "yrs" => i64::from(end_year - start_year),
        "isoyear" => i64::from(civil::iso_week(end_days).0 - civil::iso_week(start_days).0),
        "quarter" | "quarters" => {
            let quarters = |year: i32, month: u32| i64::from(year) * 4 + i64::from(month - 1) / 3;
            quarters(end_year, end_month) - quarters(start_year, start_month)
        }
        "month" | "months" | "mon" | "mons" => {
            let months = |year: i32, month: u32| i64::from(year) * 12 + i64::from(month - 1);
            months(end_year, end_month) - months(start_year, start_month)
        }
        "week" | "weeks" | "w" => (end_days - start_days) / 7,
        "day" | "days" | "d" | "dow" | "isodow" | "doy" | "weekday" => end_days - start_days,
        "hour" | "hours" | "h" | "hr" | "hrs" => {
            boundaries(end, 3_600_000_000) - boundaries(start, 3_600_000_000)
        }
        "minute" | "minutes" | "min" | "mins" | "m" => {
            boundaries(end, 60_000_000) - boundaries(start, 60_000_000)
        }
        "second" | "seconds" | "s" | "sec" | "secs" | "epoch" => {
            boundaries(end, 1_000_000) - boundaries(start, 1_000_000)
        }
        "millisecond" | "milliseconds" | "ms" | "msec" | "msecs" => {
            boundaries(end, 1_000) - boundaries(start, 1_000)
        }
        "microsecond" | "microseconds" | "us" | "usec" | "usecs" => {
            // `i128` so two extreme timestamps cannot overflow the subtraction.
            i64::try_from(i128::from(end) - i128::from(start))
                .map_err(|_| "date_diff: difference out of range".to_string())?
        }
        other => return Err(format!("extract specifier \"{other}\" not recognized")),
    };
    Ok(diff)
}

/// `last_day(ts)`: the last day of the timestamp's month as a DATE (days
/// since the epoch).
pub fn last_day_of(ts: i64) -> i32 {
    let (days, _) = civil::split_timestamp(ts);
    let (y, m, _) = civil::civil_from_days(days);
    let last = civil::days_in_month(y, m);
    civil::days_from_civil(y, m, last) as i32
}

/// `strftime(ts, format)`: render a naive timestamp.
pub fn strftime_of(ts: i64, format: &str) -> Result<String, String> {
    format::strftime(ts, format)
}

/// `strptime(text, format)`: parse a naive timestamp.
pub fn strptime_of(text: &str, format: &str) -> Result<i64, String> {
    format::strptime(text, format)
}

/// `age(end, start)`: the `end - start` interval decomposition, exactly the
/// pinned build's: borrow the time part from the days first, then the day
/// part from the months using the *start* date's month length; swap and
/// negate when `end < start`.
pub fn age_of(end: i64, start: i64) -> Interval {
    if end == start {
        return Interval {
            months: 0,
            days: 0,
            micros: 0,
        };
    }
    let (negate, later, earlier) = if end > start {
        (false, end, start)
    } else {
        (true, start, end)
    };
    let (ldays, lt) = civil::split_timestamp(later);
    let (edays, et) = civil::split_timestamp(earlier);
    let (ly, lm, ldom) = civil::civil_from_days(ldays);
    let (ey, em, edom) = civil::civil_from_days(edays);

    let mut months = (ly - ey) * 12 + (lm as i32 - em as i32);
    let mut days = ldom as i32 - edom as i32;
    let mut micros = lt - et;
    if micros < 0 {
        micros += 86_400_000_000;
        days -= 1;
    }
    if days < 0 {
        months -= 1;
        days += civil::days_in_month(ey, em) as i32;
    }
    if negate {
        months = -months;
        days = -days;
        micros = -micros;
    }
    Interval {
        months,
        days,
        micros,
    }
}

/// `make_date(year, month, day)`: a DATE from its fields; out-of-range
/// fields (month 0/13, April 31, a non-leap Feb 29, a day count outside the
/// DATE range) are an error, like the pinned build.
pub fn make_date_of(year: i32, month: i32, day: i32) -> Result<i32, String> {
    civil::checked_date(year, month, day)
        .ok_or_else(|| format!("make_date: date out of range: {year}-{month}-{day}"))
}

// ---------- the SDK declaration ----------

pinkdb_extension! {
    name = "datetime",
    functions = [
        /// `epoch(ts)` → DOUBLE seconds since the Unix epoch (fractional).
        fn epoch(ts: Option<Timestamp>) -> Option<f64> {
            ts.map(|t| epoch_of(t.0))
        },
        /// `epoch_ms(ts)` → BIGINT milliseconds since the Unix epoch.
        fn epoch_ms(ts: Option<Timestamp>) -> Option<i64> {
            ts.map(|t| epoch_ms_of(t.0))
        },
        /// `dayofweek(ts)` → BIGINT, 0 = Sunday.
        fn dayofweek(ts: Option<Timestamp>) -> Option<i64> {
            ts.map(|t| dayofweek_of(t.0))
        },
        /// `dayofmonth(ts)` → BIGINT, day of month (1..=31).
        fn dayofmonth(ts: Option<Timestamp>) -> Option<i64> {
            ts.map(|t| dayofmonth_of(t.0))
        },
        /// `dayofyear(ts)` → BIGINT, 1-based.
        fn dayofyear(ts: Option<Timestamp>) -> Option<i64> {
            ts.map(|t| dayofyear_of(t.0))
        },
        /// `week(ts)` → BIGINT, ISO-8601 week (1..=53).
        fn week(ts: Option<Timestamp>) -> Option<i64> {
            ts.map(|t| week_of(t.0))
        },
        /// `weekofyear(ts)` → BIGINT, DuckDB's name for the ISO-8601 week.
        fn weekofyear(ts: Option<Timestamp>) -> Option<i64> {
            ts.map(|t| week_of(t.0))
        },
        /// `last_day(ts)` → DATE, the last day of the timestamp's month.
        fn last_day(ts: Option<Timestamp>) -> Option<Date> {
            ts.map(|t| Date(last_day_of(t.0)))
        },
        /// `strftime(ts, format)` → VARCHAR.
        fn strftime(ts: Option<Timestamp>, format: Option<&str>) -> Result<Option<String>, String> {
            match (ts, format) {
                (Some(t), Some(format)) => strftime_of(t.0, format).map(Some),
                _ => Ok(None),
            }
        },
        /// `strptime(text, format)` → TIMESTAMP.
        fn strptime(text: Option<&str>, format: Option<&str>) -> Result<Option<Timestamp>, String> {
            match (text, format) {
                (Some(text), Some(format)) => strptime_of(text, format).map(|us| Some(Timestamp(us))),
                _ => Ok(None),
            }
        },
        /// `age(end, start)` → INTERVAL.
        fn age(end: Option<Timestamp>, start: Option<Timestamp>) -> Option<Interval> {
            match (end, start) {
                (Some(end), Some(start)) => Some(age_of(end.0, start.0)),
                _ => None,
            }
        },
        /// `make_date(year, month, day)` → DATE.
        fn make_date(year: Option<i32>, month: Option<i32>, day: Option<i32>) -> Result<Option<Date>, String> {
            match (year, month, day) {
                (Some(y), Some(m), Some(d)) => make_date_of(y, m, d).map(|days| Some(Date(days))),
                _ => Ok(None),
            }
        },
        /// `date_diff(part, start, end)` → BIGINT, the number of `part`
        /// boundaries crossed between `start` and `end`.
        fn date_diff(
            part: Option<&str>,
            start: Option<Timestamp>,
            end: Option<Timestamp>,
        ) -> Result<Option<i64>, String> {
            match (part, start, end) {
                (Some(part), Some(start), Some(end)) => date_diff_of(part, start.0, end.0).map(Some),
                _ => Ok(None),
            }
        },
        /// `datediff(part, start, end)`: DuckDB's spelling of `date_diff`.
        fn datediff(
            part: Option<&str>,
            start: Option<Timestamp>,
            end: Option<Timestamp>,
        ) -> Result<Option<i64>, String> {
            date_diff(part, start, end)
        },
    ],
}

/// Publish this extension in the process-global linked registry. One-shot per
/// process (a host bootstrap step); a second call for an extension with the
/// same shape is a no-op, so every test binary and the CLI can call it
/// unconditionally.
pub fn ensure_registered() -> Result<(), ExtError> {
    match pinkdb_register_linked() {
        Ok(()) => Ok(()),
        Err(error) => match linked::get(EXTENSION_NAME) {
            Some(existing) if same_shape(&existing) => Ok(()),
            _ => Err(error),
        },
    }
}

/// Whether `existing` declares exactly our function names (used to tell a
/// second bootstrap of *this* extension from a foreign owner of the name).
fn same_shape(existing: &LinkedExtension) -> bool {
    let Ok(ours) = pinkdb_linked_extension() else {
        return false;
    };
    let mut a: Vec<&str> = ours.functions().iter().map(|f| f.name()).collect();
    let mut b: Vec<&str> = existing.functions().iter().map(|f| f.name()).collect();
    a.sort_unstable();
    b.sort_unstable();
    a == b
}

#[cfg(test)]
mod tests {
    use super::*;
    use pinkdb_ext_sdk::{ExtArg, ExtValue};

    /// A naive timestamp from civil fields.
    fn ts(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32, us: u32) -> i64 {
        civil::days_from_civil(y, m, d) * 86_400_000_000
            + (h as i64 * 3600 + mi as i64 * 60 + s as i64) * 1_000_000
            + us as i64
    }

    fn linked(name: &str) -> pinkdb_ext_sdk::LinkedFunction {
        pinkdb_linked_extension()
            .unwrap()
            .functions()
            .iter()
            .find(|f| f.name() == name)
            .unwrap()
            .clone()
    }

    // ---------- epoch / epoch_ms (pinned against α46057) ----------

    #[test]
    fn epoch_seconds_are_fractional_doubles() {
        assert_eq!(epoch_of(0), 0.0);
        assert_eq!(epoch_of(500_000), 0.5);
        assert_eq!(epoch_of(-500_000), -0.5);
        assert_eq!(epoch_of(-1), -0.000001);
        assert_eq!(epoch_of(ts(2024, 1, 1, 0, 0, 0, 0)), 1_704_067_200.0);
        assert_eq!(epoch_of(ts(2024, 1, 1, 12, 0, 0, 0)), 1_704_110_400.0);
        assert_eq!(epoch_of(ts(1969, 7, 20, 20, 17, 40, 0)), -14_182_940.0);
    }

    #[test]
    fn epoch_ms_rounds_half_away_from_zero() {
        for (us, want) in [
            (0, 0),
            (-1, 0),
            (500, 1),
            (-500, -1),
            (-501, -1),
            (1_500, 2),
            (-1_500, -2),
            (-998_500, -999),
            (-999_500, -1_000),
            (-999_501, -1_000),
            (-999_999, -1_000),
            (-500_000, -500),
            (19_723 * 86_400_000_000, 1_704_067_200_000),
        ] {
            assert_eq!(epoch_ms_of(us), want, "{us} µs");
        }
        // The i64 extremes do not overflow the convergent math.
        assert_eq!(epoch_ms_of(i64::MIN), -9_223_372_036_854_776);
        assert_eq!(epoch_ms_of(i64::MAX), 9_223_372_036_854_776);
    }

    // ---------- calendar fields ----------

    #[test]
    fn day_fields_match_duckdb() {
        let d = |y, m, day| ts(y, m, day, 0, 0, 0, 0);
        assert_eq!(dayofweek_of(d(2024, 1, 7)), 0); // Sunday
        assert_eq!(dayofweek_of(d(2024, 1, 8)), 1);
        assert_eq!(dayofweek_of(ts(2024, 1, 7, 12, 0, 0, 0)), 0);
        assert_eq!(dayofweek_of(d(1969, 12, 31)), 3);
        assert_eq!(dayofyear_of(d(2024, 3, 1)), 61);
        assert_eq!(dayofyear_of(d(2023, 3, 1)), 60);
        assert_eq!(dayofyear_of(d(2024, 12, 31)), 366);
        assert_eq!(week_of(d(2021, 1, 1)), 53);
        assert_eq!(week_of(d(2021, 1, 4)), 1);
        assert_eq!(week_of(d(2020, 12, 31)), 53);
        assert_eq!(week_of(d(2024, 1, 1)), 1);
        assert_eq!(week_of(d(2023, 1, 1)), 52);
        assert_eq!(week_of(d(2023, 1, 2)), 1);
        assert_eq!(week_of(d(2024, 12, 31)), 1); // 2025-W01
    }

    #[test]
    fn last_day_returns_the_months_end() {
        let d = |y, m, day| ts(y, m, day, 0, 0, 0, 0);
        assert_eq!(last_day_of(d(2024, 2, 1)), 19_782); // 2024-02-29
        assert_eq!(last_day_of(d(2023, 2, 1)), 19_416); // 2023-02-28
        assert_eq!(last_day_of(d(2024, 1, 31)), 19_753); // unchanged
        assert_eq!(last_day_of(d(2024, 12, 1)), 20_088); // 2024-12-31
        assert_eq!(last_day_of(ts(2024, 2, 1, 13, 14, 15, 0)), 19_782);
        assert_eq!(last_day_of(d(2000, 2, 15)), 11_016); // 2000-02-29
    }

    // ---------- dayofmonth / weekofyear ----------

    #[test]
    fn dayofmonth_and_weekofyear_match_duckdb() {
        assert_eq!(dayofmonth_of(ts(2020, 1, 15, 0, 0, 0, 0)), 15);
        assert_eq!(dayofmonth_of(ts(2020, 2, 29, 13, 0, 0, 0)), 29);
        assert_eq!(dayofmonth_of(ts(2020, 12, 31, 23, 59, 59, 999_999)), 31);
        assert_eq!(dayofmonth_of(ts(1970, 1, 1, 0, 0, 0, 0)), 1);
        // `weekofyear` is `week` under DuckDB's other name.
        for (stamp, want) in [
            (ts(2021, 1, 1, 0, 0, 0, 0), 53),
            (ts(2023, 1, 1, 0, 0, 0, 0), 52),
            (ts(2020, 12, 31, 12, 0, 0, 0), 53),
            (ts(2024, 6, 15, 0, 0, 0, 0), 24),
            (ts(2024, 1, 4, 0, 0, 0, 0), 1),
        ] {
            assert_eq!(week_of(stamp), want);
            assert_eq!(weekofyear(Some(Timestamp(stamp))), Some(want));
        }
    }

    // ---------- date_diff ----------

    #[test]
    fn date_diff_counts_boundaries_and_truncates_like_duckdb() {
        let d = |y, m, day| ts(y, m, day, 0, 0, 0, 0);
        // Calendar parts count boundaries crossed, not whole units.
        assert_eq!(date_diff_of("month", d(2020, 1, 31), d(2020, 2, 1)), Ok(1));
        assert_eq!(date_diff_of("month", d(2020, 1, 31), d(2020, 1, 1)), Ok(0));
        assert_eq!(
            date_diff_of("month", d(2020, 1, 1), d(2019, 12, 31)),
            Ok(-1)
        );
        assert_eq!(date_diff_of("year", d(2020, 2, 29), d(2021, 2, 28)), Ok(1));
        assert_eq!(date_diff_of("year", d(2021, 3, 1), d(2020, 3, 1)), Ok(-1));
        assert_eq!(date_diff_of("quarter", d(2020, 1, 1), d(2020, 4, 1)), Ok(1));
        assert_eq!(
            date_diff_of("quarter", d(2020, 1, 1), d(2020, 3, 31)),
            Ok(0)
        );
        assert_eq!(
            date_diff_of("quarter", d(2020, 1, 1), d(2019, 10, 1)),
            Ok(-1)
        );
        // Week and day use whole calendar days; weeks truncate toward zero.
        assert_eq!(date_diff_of("day", d(2020, 1, 1), d(2020, 1, 8)), Ok(7));
        assert_eq!(date_diff_of("day", d(2020, 1, 2), d(2020, 1, 1)), Ok(-1));
        assert_eq!(
            date_diff_of(
                "day",
                ts(2020, 1, 1, 23, 0, 0, 0),
                ts(2020, 1, 2, 1, 0, 0, 0)
            ),
            Ok(1)
        );
        assert_eq!(date_diff_of("week", d(2020, 1, 1), d(2020, 1, 8)), Ok(1));
        assert_eq!(date_diff_of("week", d(2020, 1, 7), d(2020, 1, 1)), Ok(0));
        assert_eq!(date_diff_of("week", d(2020, 1, 14), d(2020, 1, 1)), Ok(-1));
        assert_eq!(
            date_diff_of(
                "week",
                ts(2020, 1, 8, 1, 0, 0, 0),
                ts(2020, 1, 1, 23, 0, 0, 0)
            ),
            Ok(-1)
        );
        // Sub-day parts subtract the absolute unit boundaries, so a span that
        // crosses midnight backwards can lose an extra unit.
        assert_eq!(
            date_diff_of("hour", ts(2020, 1, 1, 0, 59, 0, 0), d(2020, 1, 1)),
            Ok(0)
        );
        assert_eq!(
            date_diff_of("hour", d(2020, 1, 1), ts(2020, 1, 1, 1, 59, 0, 0)),
            Ok(1)
        );
        assert_eq!(
            date_diff_of("hour", d(2020, 1, 1), ts(2019, 12, 31, 22, 30, 0, 0)),
            Ok(-2)
        );
        assert_eq!(
            date_diff_of(
                "hour",
                ts(2020, 1, 1, 0, 30, 0, 0),
                ts(2020, 1, 1, 0, 59, 0, 0)
            ),
            Ok(0)
        );
        assert_eq!(
            date_diff_of("minute", d(2020, 1, 1), ts(2020, 1, 1, 0, 0, 30, 0)),
            Ok(0)
        );
        assert_eq!(
            date_diff_of("second", d(2020, 1, 1), ts(2020, 1, 1, 0, 0, 0, 500_000)),
            Ok(0)
        );
        assert_eq!(
            date_diff_of("millisecond", d(2020, 1, 1), ts(2020, 1, 1, 0, 0, 0, 500)),
            Ok(0)
        );
        assert_eq!(
            date_diff_of(
                "second",
                ts(2020, 1, 1, 0, 0, 0, 500_000),
                ts(2020, 1, 1, 0, 0, 2, 400_000)
            ),
            Ok(2)
        );
        assert_eq!(
            date_diff_of("minute", ts(2019, 12, 31, 23, 59, 30, 0), d(2020, 1, 1)),
            Ok(1)
        );
        assert_eq!(
            date_diff_of(
                "millisecond",
                ts(2019, 12, 31, 23, 59, 59, 999_500),
                d(2020, 1, 1)
            ),
            Ok(1)
        );
        assert_eq!(
            date_diff_of("microsecond", d(2020, 1, 1), ts(2020, 1, 1, 0, 0, 0, 500)),
            Ok(500)
        );
        // DuckDB's abbreviations, case-insensitively; `m` is minutes.
        assert_eq!(date_diff_of("DAYS", d(2020, 1, 1), d(2020, 1, 3)), Ok(2));
        assert_eq!(
            date_diff_of("m", d(2020, 1, 1), ts(2020, 1, 1, 0, 1, 0, 0)),
            Ok(1)
        );
        assert_eq!(date_diff_of("mons", d(2020, 1, 1), d(2020, 3, 1)), Ok(2));
        assert_eq!(
            date_diff_of("hrs", d(2020, 1, 1), ts(2020, 1, 1, 2, 0, 0, 0)),
            Ok(2)
        );
        assert_eq!(
            date_diff_of("epoch", d(2020, 1, 1), ts(2020, 1, 1, 0, 0, 2, 0)),
            Ok(2)
        );
        assert_eq!(date_diff_of("dow", d(2020, 1, 1), d(2020, 1, 3)), Ok(2));
        // ISO week years: 2021-01-01 belongs to 2020-W53.
        assert_eq!(date_diff_of("isoyear", d(2021, 1, 1), d(2020, 1, 1)), Ok(0));
        assert_eq!(date_diff_of("isoyear", d(2020, 1, 1), d(2022, 1, 1)), Ok(1));
        // Unknown parts error like DuckDB.
        assert!(
            date_diff_of("bogus", 0, 0)
                .unwrap_err()
                .contains("not recognized")
        );
        assert!(date_diff_of("qtr", 0, 0).is_err());
    }

    // ---------- make_date ----------

    #[test]
    fn make_date_validates_like_duckdb() {
        assert_eq!(make_date_of(2024, 2, 29).unwrap(), 19_782);
        assert_eq!(make_date_of(2023, 12, 31).unwrap(), 19_722);
        assert_eq!(make_date_of(1970, 1, 1).unwrap(), 0);
        assert!(make_date_of(2023, 2, 29).is_err());
        assert!(make_date_of(2023, 13, 1).is_err());
        assert!(make_date_of(2023, 0, 1).is_err());
        assert!(make_date_of(2023, 1, 32).is_err());
        assert!(make_date_of(2023, 1, 0).is_err());
        assert!(make_date_of(2023, 4, 31).is_err());
        let err = make_date_of(2023, 2, 29).unwrap_err();
        assert!(err.contains("2023-2-29"), "{err}");
        // The DATE range is the engine's i32 day count; DuckDB's positive
        // maximum is one day lower (5881580-07-10), i.e. only a date within
        // one day of ±5.88M years can differ.
        assert!(make_date_of(5_874_898, 1, 1).is_ok());
        assert!(make_date_of(5_881_580, 7, 10).is_ok());
        assert!(make_date_of(6_000_000, 1, 1).is_err());
        assert!(make_date_of(-5_881_580, 1, 1).is_err());
        // BC years compute (the engine's display omits DuckDB's "(BC)").
        assert_eq!(make_date_of(-1, 1, 1).unwrap(), -719_893);
    }

    // ---------- age ----------

    #[test]
    fn age_decomposes_like_duckdb() {
        let d = |y, m, day| ts(y, m, day, 0, 0, 0, 0);
        let cases = [
            // (end, start, months, days, micros) — pinned probe matrix.
            (d(2024, 3, 1), d(2024, 2, 29), 0, 1, 0),
            (d(2024, 3, 1), d(2024, 1, 31), 1, 1, 0),
            (d(2024, 3, 1), d(2024, 1, 30), 1, 2, 0),
            (d(2024, 3, 1), d(2023, 12, 31), 2, 1, 0),
            (d(2024, 3, 1), d(2023, 3, 1), 12, 0, 0),
            (d(2024, 3, 1), d(2023, 2, 28), 12, 1, 0),
            (d(2024, 3, 1), d(2023, 1, 31), 13, 1, 0),
            (d(2024, 3, 1), d(2023, 1, 15), 13, 17, 0),
            (d(2024, 3, 1), d(2024, 1, 1), 2, 0, 0),
            (d(2024, 3, 1), d(2023, 1, 1), 14, 0, 0),
            (d(2024, 3, 1), d(2024, 3, 31), 0, -30, 0),
            (d(2024, 3, 1), d(2024, 4, 30), -1, -29, 0),
            (d(2024, 2, 29), d(2023, 12, 31), 1, 29, 0),
            (d(2024, 2, 29), d(2023, 1, 31), 12, 29, 0),
            (d(2024, 2, 29), d(2023, 3, 1), 11, 28, 0),
            (d(2024, 1, 31), d(2023, 12, 31), 1, 0, 0),
            (d(2024, 1, 31), d(2023, 3, 1), 10, 30, 0),
            (d(2024, 1, 31), d(2023, 2, 28), 11, 3, 0),
            (d(2024, 12, 31), d(2024, 1, 1), 11, 30, 0),
            (d(2000, 2, 29), d(1999, 2, 28), 12, 1, 0),
            (d(2001, 3, 1), d(2000, 2, 29), 12, 1, 0),
            (d(2024, 2, 28), d(2024, 1, 31), 0, 28, 0),
            // Time components borrow a day (and then a month) first.
            (
                ts(2024, 3, 1, 6, 0, 0, 0),
                ts(2023, 1, 15, 18, 30, 0, 0),
                13,
                16,
                11 * 3_600_000_000 + 30 * 60_000_000,
            ),
            (
                ts(2024, 3, 1, 6, 0, 0, 0),
                ts(2024, 1, 31, 18, 0, 0, 0),
                1,
                0,
                12 * 3_600_000_000,
            ),
            (
                ts(1970, 1, 1, 1, 0, 0, 0),
                ts(1969, 12, 31, 23, 0, 0, 0),
                0,
                0,
                2 * 3_600_000_000,
            ),
            (
                ts(2024, 3, 1, 0, 0, 0, 0),
                ts(2024, 3, 1, 0, 0, 1, 0),
                0,
                0,
                -1_000_000,
            ),
            (
                ts(2024, 3, 1, 0, 0, 1, 0),
                ts(2024, 3, 1, 0, 0, 0, 0),
                0,
                0,
                1_000_000,
            ),
        ];
        for (end, start, months, days, micros) in cases {
            let got = age_of(end, start);
            assert_eq!(
                (got.months, got.days, got.micros),
                (months, days, micros),
                "age({end}, {start})"
            );
            // The negated direction is the exact negation.
            let back = age_of(start, end);
            assert_eq!(
                (back.months, back.days, back.micros),
                (-months, -days, -micros),
                "age({start}, {end})"
            );
        }
        assert_eq!(age_of(5, 5).months, 0);
    }

    // ---------- strftime ----------

    fn render(format: &str) -> String {
        strftime_of(ts(2024, 3, 5, 14, 7, 9, 123_456), format).unwrap()
    }

    #[test]
    fn strftime_renders_every_supported_specifier() {
        assert_eq!(render("%a|%A|%b|%B|%h"), "Tue|Tuesday|Mar|March|Mar");
        assert_eq!(render("%c"), "2024-03-05 14:07:09");
        assert_eq!(render("%x|%X|%T"), "2024-03-05|14:07:09|14:07:09");
        assert_eq!(render("%d|%m|%Y|%y"), "05|03|2024|24");
        assert_eq!(render("%H|%I|%M|%S|%f"), "14|02|07|09|123456");
        assert_eq!(render("%p"), "PM");
        assert_eq!(render("%j"), "065");
        assert_eq!(render("%u|%w"), "2|2");
        assert_eq!(render("%U|%W|%V|%G"), "09|10|10|2024");
        assert_eq!(render("%%"), "%");
        assert_eq!(render("plain 100%% text"), "plain 100% text");
    }

    #[test]
    fn strftime_specifier_edge_cases_match_duckdb() {
        // Midnight and noon AM/PM; %y and %f padding; ISO year boundaries.
        assert_eq!(
            strftime_of(ts(2024, 3, 5, 0, 0, 0, 0), "%I %p").unwrap(),
            "12 AM"
        );
        assert_eq!(
            strftime_of(ts(2024, 3, 5, 12, 0, 0, 0), "%I %p").unwrap(),
            "12 PM"
        );
        assert_eq!(
            strftime_of(ts(2024, 3, 5, 14, 7, 9, 0), "%f").unwrap(),
            "000000"
        );
        assert_eq!(
            strftime_of(ts(2024, 3, 5, 14, 7, 9, 100_000), "%f").unwrap(),
            "100000"
        );
        assert_eq!(
            strftime_of(ts(1, 1, 1, 0, 0, 0, 0), "%Y|%y").unwrap(),
            "0001|01"
        );
        assert_eq!(
            strftime_of(ts(2023, 1, 1, 0, 0, 0, 0), "%G %V %U %W %j").unwrap(),
            "2022 52 01 00 001"
        );
        assert_eq!(
            strftime_of(ts(1969, 12, 31, 23, 59, 59, 0), "%G %V").unwrap(),
            "1970 01"
        );
        assert_eq!(
            strftime_of(ts(2000, 2, 29, 0, 0, 0, 0), "%j %G %V").unwrap(),
            "060 2000 09"
        );
        assert_eq!(
            strftime_of(ts(2024, 12, 31, 0, 0, 0, 0), "%G %V").unwrap(),
            "2025 01"
        );
    }

    #[test]
    fn strftime_refuses_unsupported_specifiers_clearly() {
        for (format, needle) in [
            ("%e", "unsupported format specifier %e"),
            ("%k", "unsupported format specifier %k"),
            ("%s", "unsupported format specifier %s"),
            ("%z", "no time zones"),
            ("%Z", "no time zones"),
            ("%n", "no nanosecond fields"),
            ("%N", "no nanosecond fields"),
            ("%g", "use %G"),
        ] {
            let err = strftime_of(0, format).unwrap_err();
            assert!(err.contains(needle), "{format}: {err}");
            assert!(err.contains("supported:"), "{format}: {err}");
        }
        assert!(strftime_of(0, "x%").unwrap_err().contains("trailing '%'"));
        assert!(strftime_of(0, "").unwrap_err().contains("empty format"));
    }

    // ---------- strptime ----------

    #[test]
    fn strptime_parses_the_full_date_time_format() {
        let us = strptime_of("2024-03-05 14:07:09.123456", "%Y-%m-%d %H:%M:%S.%f").unwrap();
        assert_eq!(us, ts(2024, 3, 5, 14, 7, 9, 123_456));
        // Lenient widths: 1-digit month/day/hour, 1..6-digit %f.
        assert_eq!(
            strptime_of("2024-3-5 4:5:6", "%Y-%m-%d %H:%M:%S").unwrap(),
            ts(2024, 3, 5, 4, 5, 6, 0)
        );
        assert_eq!(
            strptime_of("2024-03-05 14:07:09.1", "%Y-%m-%d %H:%M:%S.%f").unwrap(),
            ts(2024, 3, 5, 14, 7, 9, 100_000)
        );
        // Unset fields default to 1900-01-01 00:00:00.
        assert_eq!(
            strptime_of("14:07:09", "%H:%M:%S").unwrap(),
            ts(1900, 1, 1, 14, 7, 9, 0)
        );
        assert_eq!(strptime_of("05", "%d").unwrap(), ts(1900, 1, 5, 0, 0, 0, 0));
        assert!(strptime_of("", "").is_err()); // empty format
    }

    #[test]
    fn strptime_pinned_edge_cases() {
        // %y pivots at 69; %j rolls over; %I/%p; %p alone.
        assert_eq!(
            strptime_of("69-01-01", "%y-%m-%d").unwrap(),
            ts(1969, 1, 1, 0, 0, 0, 0)
        );
        assert_eq!(
            strptime_of("68-01-01", "%y-%m-%d").unwrap(),
            ts(2068, 1, 1, 0, 0, 0, 0)
        );
        assert_eq!(
            strptime_of("00-01-01", "%y-%m-%d").unwrap(),
            ts(2000, 1, 1, 0, 0, 0, 0)
        );
        assert_eq!(
            strptime_of("24-3-5", "%Y-%m-%d").unwrap(),
            ts(24, 3, 5, 0, 0, 0, 0)
        );
        assert_eq!(
            strptime_of("2024-065", "%Y-%j").unwrap(),
            ts(2024, 3, 5, 0, 0, 0, 0)
        );
        assert_eq!(
            strptime_of("2024-366", "%Y-%j").unwrap(),
            ts(2024, 12, 31, 0, 0, 0, 0)
        );
        assert_eq!(
            strptime_of("2023-366", "%Y-%j").unwrap(),
            ts(2024, 1, 1, 0, 0, 0, 0)
        );
        assert_eq!(
            strptime_of("366", "%j").unwrap(),
            ts(1901, 1, 1, 0, 0, 0, 0)
        );
        assert_eq!(
            strptime_of("4:05 PM", "%I:%M %p").unwrap(),
            ts(1900, 1, 1, 16, 5, 0, 0)
        );
        assert_eq!(
            strptime_of("12:05 AM", "%I:%M %p").unwrap(),
            ts(1900, 1, 1, 0, 5, 0, 0)
        );
        assert_eq!(
            strptime_of("12:05 PM", "%I:%M %p").unwrap(),
            ts(1900, 1, 1, 12, 5, 0, 0)
        );
        assert_eq!(strptime_of("AM", "%p").unwrap(), ts(1900, 1, 1, 0, 0, 0, 0));
        assert_eq!(
            strptime_of("Pm", "%p").unwrap(),
            ts(1900, 1, 1, 12, 0, 0, 0)
        );
        // Month and weekday names: abbreviations and full names are distinct.
        assert_eq!(
            strptime_of("Mar 05 2024", "%b %d %Y").unwrap(),
            ts(2024, 3, 5, 0, 0, 0, 0)
        );
        assert_eq!(
            strptime_of("MARCH 05 2024", "%B %d %Y").unwrap(),
            ts(2024, 3, 5, 0, 0, 0, 0)
        );
        assert_eq!(
            strptime_of("Tue 2024-03-05", "%a %Y-%m-%d").unwrap(),
            ts(2024, 3, 5, 0, 0, 0, 0)
        );
        // Weekday names are not checked against the date (pinned).
        assert_eq!(
            strptime_of("Mon 2024-03-05", "%a %Y-%m-%d").unwrap(),
            ts(2024, 3, 5, 0, 0, 0, 0)
        );
        // Composites.
        assert_eq!(
            strptime_of("2024-3-5 4:5:6", "%c").unwrap(),
            ts(2024, 3, 5, 4, 5, 6, 0)
        );
        assert_eq!(
            strptime_of("2024-03-05", "%x").unwrap(),
            ts(2024, 3, 5, 0, 0, 0, 0)
        );
        assert_eq!(
            strptime_of("14:07:09", "%T").unwrap(),
            ts(1900, 1, 1, 14, 7, 9, 0)
        );
        assert!(strptime_of("2024-03-05z", "%Y-%m-%d%").is_err());
    }

    #[test]
    fn strptime_rejects_bad_input_with_clear_errors() {
        for (input, format, needle) in [
            ("2024-13-05", "%Y-%m-%d", "month 13 out of range"),
            ("2024-00-01", "%Y-%m-%d", "month 0 out of range"),
            ("2024-01-00", "%Y-%m-%d", "day 0 out of range"),
            ("2024-01-32", "%Y-%m-%d", "day 32 out of range"),
            ("2023-02-29", "%Y-%m-%d", "date out of range: 2023-2-29"),
            ("2024-04-31", "%Y-%m-%d", "date out of range"),
            ("24:00", "%H:%M", "hour 24 out of range"),
            ("60", "%S", "second 60 out of range"),
            ("13:05 PM", "%I:%M %p", "12-hour hour 13 out of range"),
            ("8", "%u", "ISO weekday 8 out of range"),
            ("7", "%w", "weekday 7 out of range"),
            ("400", "%j", "day of year 400 out of range"),
            ("20245", "%Y", "trailing character"),
            ("2024-03-05xx", "%Y-%m-%d", "trailing character"),
            ("2024-03-05", "%Y-%m-%dxx", "literal"),
            ("nope", "%Y", "expects a year"),
            ("2024-03-05", "%Y-%m-%d %H:%M:%S", "literal"),
            ("Xyz", "%a", "weekday name"),
            ("March", "%b", "abbreviated month name"),
            ("Mar", "%B", "full month name"),
            ("Xyz", "%p", "expects AM or PM"),
            (
                "2024-03-05 14:07:09.1234567",
                "%Y-%m-%d %H:%M:%S.%f",
                "trailing character",
            ),
            ("14:07:09.", "%H:%M:%S.%f", "expects a number"),
        ] {
            let err = strptime_of(input, format).unwrap_err();
            assert!(err.contains(needle), "{input:?} / {format}: {err}");
        }
    }

    #[test]
    fn strptime_refuses_unsupported_specifiers_clearly() {
        for (format, needle) in [
            ("%F", "unsupported format specifier %F"),
            ("%e", "unsupported format specifier %e"),
            ("%V", "week-based fields"),
            ("%U", "week-based fields"),
            ("%W", "week-based fields"),
            ("%G", "week-based fields"),
            ("%z", "no time zones"),
            ("%n", "no nanosecond fields"),
            ("%g", "use %G"),
            ("x%", "trailing '%'"),
        ] {
            let err = strptime_of("x", format).unwrap_err();
            assert!(err.contains(needle), "{format}: {err}");
        }
    }

    /// Every specifier `strftime` can emit round-trips through `strptime` for
    /// the formats that carry a whole date/time; the partial fields are
    /// checked in their natural combinations.
    #[test]
    fn strftime_strptime_round_trips() {
        // Formats with an exact fraction round-trip every stamp …
        let exact = [
            ts(2024, 3, 5, 14, 7, 9, 123_456),
            ts(1970, 1, 1, 0, 0, 0, 0),
            ts(1969, 12, 31, 23, 59, 59, 999_999),
            ts(2000, 2, 29, 12, 0, 0, 500_000),
            ts(2024, 12, 31, 23, 59, 59, 0),
            ts(1900, 1, 1, 0, 0, 0, 0),
        ];
        let with_fraction = [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%j %H:%M:%S.%f",
            "%Y%%%m%%%d %T.%f",
        ];
        for stamp in exact {
            for format in with_fraction {
                let rendered = strftime_of(stamp, format).unwrap();
                let back = strptime_of(&rendered, format)
                    .unwrap_or_else(|e| panic!("{rendered:?} / {format}: {e}"));
                assert_eq!(back, stamp, "{format}: {rendered:?}");
            }
        }
        // … while second-resolution formats need a whole-second stamp.
        let whole = [
            ts(1970, 1, 1, 0, 0, 0, 0),
            ts(2000, 2, 29, 12, 0, 0, 0),
            ts(2024, 12, 31, 23, 59, 59, 0),
            ts(1900, 1, 1, 0, 0, 0, 0),
        ];
        let without_fraction = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%j %H:%M:%S",
            "%b %d %Y %I:%M:%S %p",
            "%B %d %Y %T",
            "%c",
            "%x %X",
        ];
        for stamp in whole {
            for format in without_fraction {
                let rendered = strftime_of(stamp, format).unwrap();
                let back = strptime_of(&rendered, format)
                    .unwrap_or_else(|e| panic!("{rendered:?} / {format}: {e}"));
                assert_eq!(back, stamp, "{format}: {rendered:?}");
            }
        }
        // %y round-trips inside the 2000s window only (the pinned pivot).
        let stamp = ts(2024, 3, 5, 14, 7, 9, 0);
        let rendered = strftime_of(stamp, "%y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(rendered, "24-03-05 14:07:09");
        assert_eq!(strptime_of(&rendered, "%y-%m-%d %H:%M:%S").unwrap(), stamp);
    }

    // ---------- registration metadata + callbacks ----------

    #[test]
    fn extension_metadata_is_pinned() {
        let ext = pinkdb_linked_extension().unwrap();
        assert_eq!(ext.name(), "datetime");
        let names: Vec<&str> = ext.functions().iter().map(|f| f.name()).collect();
        assert_eq!(
            names,
            vec![
                "epoch",
                "epoch_ms",
                "dayofweek",
                "dayofmonth",
                "dayofyear",
                "week",
                "weekofyear",
                "last_day",
                "strftime",
                "strptime",
                "age",
                "make_date",
                "date_diff",
                "datediff"
            ]
        );
        for f in ext.functions() {
            assert!(!f.is_volatile(), "{} must be deterministic", f.name());
            assert!(!f.is_variadic());
        }
        let f = linked("epoch");
        assert_eq!(f.ret_type(), pinkdb_ext_sdk::ExtType::Double);
        assert_eq!(f.arg_types(), &[pinkdb_ext_sdk::ExtType::Timestamp]);
        assert_eq!(
            linked("epoch_ms").ret_type(),
            pinkdb_ext_sdk::ExtType::Bigint
        );
        assert_eq!(linked("last_day").ret_type(), pinkdb_ext_sdk::ExtType::Date);
        assert_eq!(
            linked("strptime").ret_type(),
            pinkdb_ext_sdk::ExtType::Timestamp
        );
        assert_eq!(linked("age").ret_type(), pinkdb_ext_sdk::ExtType::Interval);
        assert_eq!(
            linked("make_date").ret_type(),
            pinkdb_ext_sdk::ExtType::Date
        );
        assert_eq!(
            linked("strftime").arg_types(),
            &[
                pinkdb_ext_sdk::ExtType::Timestamp,
                pinkdb_ext_sdk::ExtType::Varchar
            ]
        );
        // The calendar-field functions DuckDB spells differently but returns
        // as BIGINT, and the boundary-counting `date_diff` pair.
        for name in ["dayofmonth", "weekofyear"] {
            assert_eq!(linked(name).ret_type(), pinkdb_ext_sdk::ExtType::Bigint);
            assert_eq!(
                linked(name).arg_types(),
                &[pinkdb_ext_sdk::ExtType::Timestamp]
            );
        }
        for name in ["date_diff", "datediff"] {
            assert_eq!(linked(name).ret_type(), pinkdb_ext_sdk::ExtType::Bigint);
            assert_eq!(
                linked(name).arg_types(),
                &[
                    pinkdb_ext_sdk::ExtType::Varchar,
                    pinkdb_ext_sdk::ExtType::Timestamp,
                    pinkdb_ext_sdk::ExtType::Timestamp
                ]
            );
        }
        assert_eq!(
            linked("make_date").arg_types(),
            &[
                pinkdb_ext_sdk::ExtType::Integer,
                pinkdb_ext_sdk::ExtType::Integer,
                pinkdb_ext_sdk::ExtType::Integer
            ]
        );
    }

    #[test]
    fn callbacks_marshal_arguments_and_nulls() {
        let call = |name: &str, args: &[ExtArg<'_>]| linked(name).call(args).unwrap();
        assert_eq!(
            call("epoch", &[ExtArg::Timestamp(1_500_000)]),
            ExtValue::Double(1.5)
        );
        assert_eq!(
            call("epoch", &[ExtArg::Null(pinkdb_ext_sdk::ExtType::Timestamp)]),
            ExtValue::Null
        );
        assert_eq!(
            call(
                "strftime",
                &[ExtArg::Timestamp(0), ExtArg::Varchar(b"%Y-%m-%d")]
            ),
            ExtValue::Varchar(b"1970-01-01".to_vec())
        );
        assert_eq!(
            call(
                "strftime",
                &[
                    ExtArg::Null(pinkdb_ext_sdk::ExtType::Timestamp),
                    ExtArg::Varchar(b"%e")
                ]
            ),
            ExtValue::Null,
            "a NULL timestamp returns NULL before the format is validated"
        );
        assert_eq!(
            call(
                "strptime",
                &[ExtArg::Varchar(b"2024-03-05"), ExtArg::Varchar(b"%Y-%m-%d")]
            ),
            ExtValue::Timestamp(ts(2024, 3, 5, 0, 0, 0, 0))
        );
        assert_eq!(
            call(
                "age",
                &[
                    ExtArg::Timestamp(ts(2024, 3, 1, 0, 0, 0, 0)),
                    ExtArg::Timestamp(ts(2023, 1, 15, 0, 0, 0, 0))
                ]
            ),
            ExtValue::Interval {
                months: 13,
                days: 17,
                micros: 0
            }
        );
        assert_eq!(
            call(
                "make_date",
                &[
                    ExtArg::Integer(2024),
                    ExtArg::Integer(2),
                    ExtArg::Integer(29)
                ]
            ),
            ExtValue::Date(19_782)
        );
        assert_eq!(
            call(
                "make_date",
                &[
                    ExtArg::Integer(2023),
                    ExtArg::Null(pinkdb_ext_sdk::ExtType::Integer),
                    ExtArg::Integer(1)
                ]
            ),
            ExtValue::Null
        );
        // Errors carry the function's message through the linked model.
        let err = linked("strftime")
            .call(&[ExtArg::Timestamp(0), ExtArg::Varchar(b"%e")])
            .unwrap_err();
        assert!(
            err.to_string().contains("unsupported format specifier %e"),
            "{err}"
        );
        // Arity is fixed: the wrong count is a clear error.
        let err = linked("week").call(&[]).unwrap_err().to_string();
        assert!(err.contains("expected 1 argument(s), got 0"), "{err}");
    }
}
