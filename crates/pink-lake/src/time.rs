//! ISO-8601 UTC timestamps for the catalog's `updated` field.
//!
//! `std` has no date formatter, so this is the standard days-to-civil
//! algorithm (Howard Hinnant's `civil_from_days`) over the Unix epoch.

use std::time::{SystemTime, UNIX_EPOCH};

/// `2026-10-14T12:34:56.789Z` (millisecond precision, UTC).
pub(crate) fn iso8601_utc(time: SystemTime) -> String {
    let since = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = since.as_secs() as i64;
    let (year, month, day) = civil_from_days(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    let (hour, minute, second) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{:03}Z",
        since.subsec_millis()
    )
}

/// The AWS SigV4 timestamp pair for `time`: `("20150830T123600Z", "20150830")`
/// (`x-amz-date` and the credential-scope date stamp).
#[cfg(feature = "s3")]
pub(crate) fn amz_date_utc(time: SystemTime) -> (String, String) {
    let since = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = since.as_secs() as i64;
    let (year, month, day) = civil_from_days(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    let (hour, minute, second) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    (
        format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z"),
        format!("{year:04}{month:02}{day:02}"),
    )
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(secs: u64, millis: u32) -> SystemTime {
        UNIX_EPOCH + Duration::new(secs, millis * 1_000_000)
    }

    #[test]
    fn known_epochs_format_correctly() {
        assert_eq!(iso8601_utc(at(0, 0)), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            iso8601_utc(at(1_000_000_000, 0)),
            "2001-09-09T01:46:40.000Z"
        );
        assert_eq!(
            iso8601_utc(at(1_700_000_000, 123)),
            "2023-11-14T22:13:20.123Z"
        );
        // A leap day.
        assert_eq!(
            iso8601_utc(at(1_582_934_400, 0)),
            "2020-02-29T00:00:00.000Z"
        );
        // A pre-epoch clock (test clocks can be wrong); clamps to the epoch.
        assert_eq!(
            iso8601_utc(UNIX_EPOCH - Duration::from_secs(5)),
            "1970-01-01T00:00:00.000Z"
        );
    }

    /// The SigV4 timestamp pair (used by the `s3` feature).
    #[cfg(feature = "s3")]
    #[test]
    fn amz_date_matches_the_aws_format() {
        // 2015-08-30T12:36:00Z, the timestamp of the AWS SigV4 test suite.
        assert_eq!(
            amz_date_utc(at(1_440_938_160, 0)),
            ("20150830T123600Z".to_string(), "20150830".to_string())
        );
        assert_eq!(
            amz_date_utc(at(0, 999)),
            ("19700101T000000Z".to_string(), "19700101".to_string())
        );
    }
}
