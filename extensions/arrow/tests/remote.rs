//! Remote Arrow IPC reads (`remote` feature) over an in-test HTTP server and
//! the mock S3 service. IPC has no ranged footer protocol, so every remote
//! read is documented as a whole-object `fetch()`; these tests pin values,
//! claims, errors, prepared statements and the durable re-read policy.
//!
//! This file only compiles with `--features remote`; local Arrow coverage is
//! `roundtrip.rs`/`engine.rs`.

#![cfg(feature = "remote")]

#[path = "../../../crates/pink-lake/src/mock_s3.rs"]
mod mock_s3;
#[path = "../../../crates/pinkdb-httpfs/src/test_http.rs"]
mod test_http;

use std::sync::{Mutex, MutexGuard};

use pinkdb::{PinkDb, QueryResult, Value};
use test_http::TestHttpServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvGuard(Vec<&'static str>);

impl EnvGuard {
    fn http_allow() -> (MutexGuard<'static, ()>, Self) {
        let guard = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // SAFETY: the lock serializes every env-mutating test in this binary.
        unsafe { std::env::set_var("PINKDB_HTTPFS_ALLOW_HTTP", "1") };
        (guard, Self(vec!["PINKDB_HTTPFS_ALLOW_HTTP"]))
    }

    fn s3(mock: &mock_s3::MockS3) -> (MutexGuard<'static, ()>, Self) {
        let guard = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // SAFETY: the lock serializes every env-mutating test in this binary.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", mock.access_key_id());
            std::env::set_var("AWS_SECRET_ACCESS_KEY", mock.secret_access_key());
            std::env::set_var("AWS_REGION", "auto");
            std::env::set_var("AWS_ENDPOINT_URL_S3", mock.endpoint());
            std::env::set_var("PINKDB_S3_ALLOW_HTTP", "1");
        }
        (
            guard,
            Self(vec![
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_REGION",
                "AWS_ENDPOINT_URL_S3",
                "PINKDB_S3_ALLOW_HTTP",
            ]),
        )
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: the lock is still held while this guard drops before it.
        unsafe {
            for name in &self.0 {
                std::env::remove_var(name);
            }
        }
    }
}

/// Write a small IPC file with the crate's own sink and return its bytes.
fn arrow_bytes() -> Vec<u8> {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb-arrow-remote-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("probe.arrow");
    let mut db = PinkDb::new();
    pinkdb_arrow::register(&mut db);
    db.execute("CREATE TABLE t (i INTEGER, s VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, 'c');")
        .unwrap();
    db.execute(&format!("COPY t TO '{}' (FORMAT arrow);", path.display()))
        .unwrap();
    let bytes = std::fs::read(&path).unwrap();
    std::fs::remove_dir_all(&dir).ok();
    bytes
}

fn select(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut out = db.execute(sql).expect("query");
    match out.pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    pinkdb_arrow::register(&mut db);
    db
}

fn temp_store(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb-arrow-remote-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let store = dir.join("store.pink");
    (dir, store)
}

#[test]
fn http_arrow_reads_whole_object() {
    let (_lock, _env) = EnvGuard::http_allow();
    let bytes = arrow_bytes();
    let server = TestHttpServer::start();
    server.put("/probe.arrow", &bytes);
    let url = server.url("/probe.arrow");

    let mut db = store();
    assert_eq!(
        select(
            &mut db,
            &format!("SELECT COUNT(*), SUM(i), COUNT(s) FROM read_arrow('{url}');")
        ),
        vec![vec![Value::Bigint(3), Value::Bigint(6), Value::Bigint(2)]]
    );
    // The replacement scan claims the remote .arrow path.
    assert_eq!(
        select(&mut db, &format!("SELECT i FROM '{url}' ORDER BY i;")),
        vec![
            vec![Value::Integer(1)],
            vec![Value::Integer(2)],
            vec![Value::Integer(3)]
        ]
    );
    // Both statements are whole-object GETs (no Range header), one per
    // bind→scan pair thanks to the source's one-statement cache.
    let ranges = server.ranges();
    let gets: Vec<&String> = server
        .requests()
        .iter()
        .zip(&ranges)
        .filter(|(r, _)| r.starts_with("GET "))
        .map(|(_, range)| range)
        .collect();
    assert_eq!(gets.len(), 2, "{:?}", server.requests());
    assert!(gets.iter().all(|range| range.is_empty()), "{gets:?}");
}

#[test]
fn http_arrow_query_string_error_and_prepared() {
    let (_lock, _env) = EnvGuard::http_allow();
    let server = TestHttpServer::start();
    server.put("/probe.arrow", &arrow_bytes());
    let url = format!("{}?token=abc", server.url("/probe.arrow"));

    let mut db = store();
    let stmt = db
        .prepare(&format!(
            "SELECT COUNT(*) FROM read_arrow('{url}') WHERE i >= ?;"
        ))
        .unwrap();
    for (bound, expected) in [(1, 3i64), (2, 2)] {
        match db
            .execute_prepared(&stmt, &[Value::Integer(bound)])
            .unwrap()
        {
            QueryResult::Selected { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Bigint(expected)]]);
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    let err = db
        .execute(&format!(
            "SELECT * FROM read_arrow('{}');",
            server.url("/missing.arrow")
        ))
        .unwrap_err()
        .to_string();
    assert!(err.contains("404") || err.contains("not found"), "{err}");
}

#[test]
fn http_arrow_durable_replay_re_reads() {
    let (_lock, _env) = EnvGuard::http_allow();
    let server = TestHttpServer::start();
    server.put("/probe.arrow", &arrow_bytes());
    let url = server.url("/probe.arrow");
    let (dir, path) = temp_store("durable");

    pinkdb_arrow::ensure_registered();
    let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    db.execute("CREATE TABLE dst (i INTEGER);").unwrap();
    db.execute(&format!(
        "INSERT INTO dst SELECT i FROM read_arrow('{url}');"
    ))
    .unwrap();
    assert_eq!(
        select(&mut db, "SELECT COUNT(*), SUM(i) FROM dst;"),
        vec![vec![Value::Bigint(3), Value::Bigint(6)]]
    );
    drop(db);

    // Recovery re-reads the remote object.
    let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    assert_eq!(
        select(&mut db, "SELECT COUNT(*), SUM(i) FROM dst;"),
        vec![vec![Value::Bigint(3), Value::Bigint(6)]]
    );
    drop(db);

    // The object vanished: replay is a hard error.
    drop(server);
    let err = PinkDb::open_durable(path.to_str().unwrap())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("probe.arrow") || err.contains("cannot open"),
        "{err}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn s3_arrow_reads_whole_object() {
    let mock = mock_s3::MockS3::start();
    mock.put_object("lake/probe.arrow", &arrow_bytes());
    let (_lock, _env) = EnvGuard::s3(&mock);

    let mut db = store();
    assert_eq!(
        select(
            &mut db,
            "SELECT COUNT(*), SUM(i), COUNT(s) FROM read_arrow('s3://bucket/lake/probe.arrow');"
        ),
        vec![vec![Value::Bigint(3), Value::Bigint(6), Value::Bigint(2)]]
    );
    let ranges: Vec<String> = mock
        .requests()
        .into_iter()
        .zip(mock.ranges())
        .filter(|(r, _)| r.starts_with("GET "))
        .map(|(_, range)| range)
        .collect();
    assert!(!ranges.is_empty());
    assert!(ranges.iter().all(|r| r.is_empty()), "{ranges:?}");
    assert!(mock.verified_requests() >= 1);
}
