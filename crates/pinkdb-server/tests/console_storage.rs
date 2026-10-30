//! Wave 121b, reworked in wave 124: the console over a lake database of each
//! catalog backend — a `.pink` file, SQLite, Postgres (a throwaway local
//! server, when `PINKDB_TEST_POSTGRES_URL` is set) — plus S3 data in
//! `console_storage_s3.rs`. Worksheet statements and an upload publish to
//! the lake, a reopen of the data directory sees them, and the catalog
//! reports the lake without secrets.
#![cfg(feature = "ui")]

#[path = "common/console.rs"]
mod console;

use console::{lake_round_trip, tempdir};

#[test]
fn the_console_over_a_pink_catalog() {
    let dir = tempdir("pink");
    lake_round_trip(
        &format!("pink:{}", dir.join("catalog.pink-catalog").display()),
        dir.join("data").to_str().unwrap(),
        "pink:",
    );
}

#[test]
#[cfg(feature = "catalog-sqlite")]
fn the_console_over_a_sqlite_catalog() {
    let dir = tempdir("sqlite");
    lake_round_trip(
        &format!("sqlite:{}", dir.join("catalog.sqlite").display()),
        dir.join("data").to_str().unwrap(),
        "sqlite:",
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
    // The password travels through PGPASSWORD: a lake URL may not carry one.
    let url = url.trim();
    let (url, password) = match url.split_once("://").and_then(|(scheme, rest)| {
        let (userinfo, host) = rest.split_once('@')?;
        let (user, password) = userinfo.split_once(':')?;
        Some((format!("{scheme}://{user}@{host}"), password.to_string()))
    }) {
        Some((url, password)) => (url, Some(password)),
        None => (url.to_string(), None),
    };
    if let Some(password) = password {
        // SAFETY: set before any store or thread of this test reads it; the
        // other tests of this binary do not read PGPASSWORD.
        unsafe { std::env::set_var("PGPASSWORD", password) };
    }
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
        "postgres",
    );
}
