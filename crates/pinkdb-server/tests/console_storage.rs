//! Wave 121b: the console over each storage backend a server can serve — a
//! lake catalog in a `.pink` file, in SQLite, in Postgres (a throwaway local
//! server, when `PINKDB_TEST_POSTGRES_URL` is set) — plus S3 data in
//! `console_storage_s3.rs`. Worksheet statements and an upload publish to
//! the catalog, a reopen under a new lease sees them, and the Storage panel
//! reports the lake without secrets.
#![cfg(all(feature = "ui", feature = "catalog"))]

#[path = "common/console.rs"]
mod console;

use console::{lake_round_trip, tempdir};

#[test]
fn the_console_over_a_pink_catalog() {
    let dir = tempdir("pink");
    lake_round_trip(
        &format!("pink:{}", dir.join("catalog.pink-catalog").display()),
        dir.join("data").to_str().unwrap(),
        "pinkdb (.pink file)",
    );
}

#[test]
fn the_console_over_a_sqlite_catalog() {
    let dir = tempdir("sqlite");
    lake_round_trip(
        &format!("sqlite:{}", dir.join("catalog.sqlite").display()),
        dir.join("data").to_str().unwrap(),
        "SQLite",
    );
}

#[test]
#[cfg(feature = "catalog-postgres")]
fn the_console_over_a_postgres_catalog() {
    let Some(url) = std::env::var("PINKDB_TEST_POSTGRES_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
    else {
        eprintln!("skip: PINKDB_TEST_POSTGRES_URL is not set");
        return;
    };
    let url = url.trim();
    let schema = format!(
        "w121b_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let separator = if url.contains('?') { '&' } else { '?' };
    let dir = tempdir("postgres");
    lake_round_trip(
        &format!("{url}{separator}schema={schema}"),
        dir.join("data").to_str().unwrap(),
        "Postgres",
    );
}
