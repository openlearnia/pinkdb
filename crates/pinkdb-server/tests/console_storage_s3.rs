//! Wave 121b: the console over a lake whose data lives in (mock) S3. Its own
//! binary: the S3 client reads its endpoint and credentials from the process
//! environment, which this one test sets before any store exists.
#![cfg(all(feature = "ui", feature = "catalog", feature = "lake-s3"))]

#[path = "common/console.rs"]
mod console;

#[path = "../../pink-lake/src/mock_s3.rs"]
mod mock_s3;

#[test]
fn the_console_over_s3_data() {
    let mock = mock_s3::MockS3::start();
    // SAFETY: the only test of this binary; nothing else reads the
    // environment concurrently.
    unsafe {
        for (key, value) in [
            ("AWS_ACCESS_KEY_ID", mock.access_key_id()),
            ("AWS_SECRET_ACCESS_KEY", mock.secret_access_key()),
            ("AWS_ENDPOINT_URL", mock.endpoint()),
            ("AWS_ENDPOINT_URL_S3", mock.endpoint()),
            ("AWS_REGION", "auto".to_string()),
            ("AWS_S3_FORCE_PATH_STYLE", "1".to_string()),
            ("PINKDB_S3_ALLOW_HTTP", "1".to_string()),
        ] {
            std::env::set_var(key, value);
        }
    }
    let dir = console::tempdir("s3");
    let data = mock.url("wave121b", "lake");
    console::lake_round_trip(
        &format!("sqlite:{}", dir.join("catalog.sqlite").display()),
        &data,
        "SQLite",
    );
    // The rows above the inlining limit reached the bucket.
    assert!(
        mock.keys().iter().any(|key| key.contains("/data/")),
        "no data file reached the bucket: {:?}",
        mock.keys()
    );
    // The secret key is never part of what the console shows.
    assert!(!data.contains(&mock.secret_access_key()));
}
