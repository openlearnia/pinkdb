//! Proleptic Gregorian calendar arithmetic for the datetime extension.
//!
//! Howard Hinnant's civil-calendar integer algorithms (the same math the
//! engine's `types.rs` uses — this crate cannot depend on the engine, so the
//! ~60 lines are mirrored here) plus the ISO-week and day-of-year helpers the
//! format layer needs. Everything is pure integer arithmetic; the only
//! `i64`-wrong inputs are years so large that the resulting day count leaves
//! `i32` — callers range-check before storing a DATE.

/// Days since 1970-01-01 for a civil date. `m` is 1-based. The result is
/// `i64` because `make_date` accepts years whose `i32` day count overflows
/// (DuckDB's range check, not ours).
pub fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y as i64 - 1 } else { y as i64 };
    let era = y.div_euclid(400);
    let yoe = y - era * 400; // [0, 399]
    let mp = (m as i64 + 9) % 12; // [0, 11], March-based
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Civil (year, 1-based month, day) for days since 1970-01-01.
pub fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719468;
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

pub fn is_leap(y: i32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

/// Days in month `m` (1-based); 0 for an invalid month.
pub fn days_in_month(y: i32, m: u32) -> u32 {
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

/// 0 = Sunday … 6 = Saturday. 1970-01-01 was a Thursday.
pub fn weekday(days: i64) -> u32 {
    (days + 4).rem_euclid(7) as u32
}

/// 1 = Monday … 7 = Sunday (ISO-8601).
pub fn iso_weekday(days: i64) -> u32 {
    (days + 3).rem_euclid(7) as u32 + 1
}

/// 1-based day of the year.
pub fn day_of_year(days: i64) -> u32 {
    let (y, _, _) = civil_from_days(days);
    (days - days_from_civil(y, 1, 1) + 1) as u32
}

/// Number of ISO weeks in `y` (52 or 53): the year has 53 weeks when it
/// starts on a Thursday, or when it is leap and starts on a Wednesday.
pub fn iso_weeks_in_year(y: i32) -> u32 {
    let jan1 = days_from_civil(y, 1, 1);
    let dow = iso_weekday(jan1);
    if dow == 4 || (is_leap(y) && dow == 3) {
        53
    } else {
        52
    }
}

/// ISO-8601 week number (`1..=53`) and its ISO year, which can differ from
/// the calendar year around New Year (2021-01-01 is 2020-W53).
pub fn iso_week(days: i64) -> (i32, u32) {
    let (y, _, _) = civil_from_days(days);
    let doy = day_of_year(days) as i64;
    let dow = iso_weekday(days) as i64;
    let w = (doy - dow + 10) / 7;
    if w < 1 {
        let prev = y - 1;
        (prev, iso_weeks_in_year(prev))
    } else if w > iso_weeks_in_year(y) as i64 {
        (y + 1, 1)
    } else {
        (y, w as u32)
    }
}

/// Week of the year, 00–53, where week 1 starts on the first Sunday and
/// days before it are week 0 (C `strftime`'s `%U`).
pub fn week_of_year_sunday(days: i64) -> u32 {
    let (y, _, _) = civil_from_days(days);
    let yday = days - days_from_civil(y, 1, 1); // 0-based
    let wday = weekday(days) as i64;
    ((yday + 7 - wday) / 7) as u32
}

/// Week of the year, 00–53, with weeks starting on Monday (`%W`).
pub fn week_of_year_monday(days: i64) -> u32 {
    let (y, _, _) = civil_from_days(days);
    let yday = days - days_from_civil(y, 1, 1); // 0-based
    let wday = (weekday(days) as i64 + 6) % 7; // 0 = Monday
    ((yday + 7 - wday) / 7) as u32
}

/// Microseconds since midnight and days since the epoch for a naive
/// timestamp, both floored (pre-1970 stamps split consistently).
pub fn split_timestamp(us: i64) -> (i64, i64) {
    (us.div_euclid(86_400_000_000), us.rem_euclid(86_400_000_000))
}

/// Day count of a civil date, or `None` when the three fields are not a real
/// calendar date or the day count leaves `i32` (DuckDB's DATE range).
pub fn checked_date(y: i32, m: i32, d: i32) -> Option<i32> {
    if !(1..=12).contains(&m) {
        return None;
    }
    let d = u32::try_from(d).ok()?;
    if d < 1 || d > days_in_month(y, m as u32) {
        return None;
    }
    let days = days_from_civil(y, m as u32, d);
    i32::try_from(days).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_round_trips() {
        for days in [
            -719_162,
            -1,
            0,
            1,
            10_957,
            19_723,
            2_147_483_647,
            -2_147_483_648,
        ] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days, "{days} -> {y}-{m}-{d}");
        }
    }

    #[test]
    fn leaps_and_month_lengths() {
        assert!(is_leap(2024) && is_leap(2000) && !is_leap(1900) && !is_leap(2023));
        assert_eq!(days_in_month(2024, 2), 29);
        assert_eq!(days_in_month(2023, 2), 28);
        assert_eq!(days_in_month(2024, 4), 30);
        assert_eq!(days_in_month(2024, 12), 31);
        assert_eq!(days_in_month(2024, 13), 0);
    }

    #[test]
    fn weekdays_are_pinned() {
        // 2024-01-07 is a Sunday; 1970-01-01 a Thursday.
        assert_eq!(weekday(19_729), 0);
        assert_eq!(weekday(0), 4);
        assert_eq!(iso_weekday(19_729), 7);
        assert_eq!(iso_weekday(19_730), 1);
        assert_eq!(weekday(-1), 3); // 1969-12-31 was a Wednesday
    }

    #[test]
    fn day_of_year_and_iso_weeks_match_duckdb() {
        let d = |y, m, day| days_from_civil(y, m, day);
        assert_eq!(day_of_year(d(2024, 3, 1)), 61);
        assert_eq!(day_of_year(d(2023, 3, 1)), 60);
        assert_eq!(day_of_year(d(2024, 12, 31)), 366);
        // Pinned: 2021-01-01 -> 2020-W53, 2020-12-31 -> 2020-W53,
        // 2023-01-01 -> 2022-W52, 2024-01-01 -> 2024-W01.
        assert_eq!(iso_week(d(2021, 1, 1)), (2020, 53));
        assert_eq!(iso_week(d(2020, 12, 31)), (2020, 53));
        assert_eq!(iso_week(d(2023, 1, 1)), (2022, 52));
        assert_eq!(iso_week(d(2024, 1, 1)), (2024, 1));
        assert_eq!(iso_week(d(2024, 3, 5)), (2024, 10));
        assert_eq!(iso_week(d(2024, 12, 31)), (2025, 1));
        assert_eq!(iso_week(d(1969, 12, 31)), (1970, 1));
        assert_eq!(iso_weeks_in_year(2020), 53);
        assert_eq!(iso_weeks_in_year(2024), 52);
    }

    #[test]
    fn c_style_week_numbers_match_duckdb() {
        let d = |y, m, day| days_from_civil(y, m, day);
        // Pinned: 2024-03-05 -> %U 09, %W 10; 2023-01-01 -> %U 01, %W 00.
        assert_eq!(week_of_year_sunday(d(2024, 3, 5)), 9);
        assert_eq!(week_of_year_monday(d(2024, 3, 5)), 10);
        assert_eq!(week_of_year_sunday(d(2024, 1, 1)), 0);
        assert_eq!(week_of_year_monday(d(2024, 1, 1)), 1);
        assert_eq!(week_of_year_sunday(d(2023, 1, 1)), 1);
        assert_eq!(week_of_year_monday(d(2023, 1, 1)), 0);
    }

    #[test]
    fn checked_dates_reject_bad_fields() {
        assert_eq!(checked_date(2024, 2, 29), Some(19_782));
        assert_eq!(checked_date(2023, 2, 29), None);
        assert_eq!(checked_date(2024, 13, 1), None);
        assert_eq!(checked_date(2024, 0, 1), None);
        assert_eq!(checked_date(2024, 1, 0), None);
        assert_eq!(checked_date(2024, 1, 32), None);
        // The range is the engine's i32 day count: 5,881,580-07-10 fits
        // (DuckDB's positive maximum), six million years overflows.
        assert!(checked_date(5_881_580, 7, 10).is_some());
        assert!(checked_date(6_000_000, 1, 1).is_none());
        // BC years count.
        assert_eq!(
            checked_date(-1, 1, 1),
            Some(days_from_civil(-1, 1, 1) as i32)
        );
    }
}
