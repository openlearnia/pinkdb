//! Remote parquet reads (`remote` feature) over an in-test HTTP server and
//! the mock S3 service: value correctness, ranged footer/row-group reads,
//! replacement scans, metadata functions, errors, prepared statements and the
//! durable re-read policy.
//!
//! This file only compiles with `--features remote`; the local reader is
//! covered by `read_parquet.rs`.

#![cfg(feature = "remote")]

#[path = "../../../crates/pink-lake/src/mock_s3.rs"]
mod mock_s3;
#[path = "../../../crates/pinkdb-httpfs/src/test_http.rs"]
mod test_http;

use std::sync::{Mutex, MutexGuard};

use pinkdb::{PinkDb, QueryResult, Value};
use test_http::TestHttpServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Serialize tests that mutate process-global transport configuration
/// (`PINKDB_HTTPFS_ALLOW_HTTP`, `AWS_*`) and restore it afterwards.
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
        // SAFETY: see EnvGuard::http_allow/s3 (the lock is still held while
        // this guard drops before it).
        unsafe {
            for name in &self.0 {
                std::env::remove_var(name);
            }
        }
    }
}

fn fixture() -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../bench/parquet_fixtures/rg_stats.parquet");
    std::fs::read(&path).expect("the committed rg_stats fixture")
}

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    db.register_table_source(pinkdb_parquet::source());
    db
}

fn select(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut out = db.execute(sql).expect("query");
    match out.pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

/// GET request lines + their `Range` headers (parallel vectors).
fn gets(server: &TestHttpServer) -> Vec<(String, String)> {
    server
        .requests()
        .into_iter()
        .zip(server.ranges())
        .filter(|(request, _)| request.starts_with("GET "))
        .collect()
}

fn range(spec: &str) -> Option<(u64, u64)> {
    let spec = spec.strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

fn temp_store(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb-parquet-remote-{tag}-{}-{}",
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
fn http_parquet_reads_ranged_and_never_whole_file() {
    let (_lock, _env) = EnvGuard::http_allow();
    let bytes = fixture();
    let len = bytes.len() as u64;
    let server = TestHttpServer::start();
    server.put("/rg.parquet", &bytes);
    let url = server.url("/rg.parquet");

    let mut db = store();
    assert_eq!(
        select(
            &mut db,
            &format!("SELECT COUNT(*), MIN(id), MAX(id) FROM read_parquet('{url}');")
        ),
        vec![vec![
            Value::Bigint(8192),
            Value::Integer(0),
            Value::Integer(8191)
        ]]
    );
    // Pruning still works through the remote reader: only one row group
    // survives `id >= 7000` (8192 rows, 4 groups of 2048).
    assert_eq!(
        select(
            &mut db,
            &format!("SELECT COUNT(*) FROM read_parquet('{url}') WHERE id >= 7000;")
        ),
        vec![vec![Value::Bigint(1192)]]
    );

    let gets = gets(&server);
    assert!(!gets.is_empty(), "no GET requests recorded");
    assert!(
        gets.iter().all(|(_, r)| !r.is_empty()),
        "a whole-object GET was issued: {gets:?}"
    );
    // The footer is read as an 8-byte range at the end, never the whole file.
    assert!(
        gets.iter()
            .any(|(_, r)| range(r) == Some((len - 8, len - 1))),
        "no 8-byte footer range: {gets:?}"
    );
    // Row-group/page reads are ranges too, and every one is smaller than the
    // object (a whole download would have no Range header at all).
    assert!(
        gets.iter()
            .any(|(_, r)| range(r).is_some_and(|(start, end)| end - start + 1 > 8)),
        "no data range: {gets:?}"
    );
    assert!(
        gets.iter()
            .all(|(_, r)| { range(r).is_some_and(|(start, end)| start < len && end < len) }),
        "range outside the object: {gets:?}"
    );
}

#[test]
fn http_parquet_replacement_scan_metadata_and_query_string() {
    let (_lock, _env) = EnvGuard::http_allow();
    let server = TestHttpServer::start();
    server.put("/rg.parquet", &fixture());
    // The query is ignored for the claim/extension decision but retained for
    // the transport (the test server strips it when routing).
    let url = format!("{}?token=abc", server.url("/rg.parquet"));

    let mut db = store();
    assert_eq!(
        select(
            &mut db,
            &format!("SELECT COUNT(*), MIN(id), MAX(id) FROM '{url}';")
        ),
        vec![vec![
            Value::Bigint(8192),
            Value::Integer(0),
            Value::Integer(8191)
        ]]
    );
    let rows = select(
        &mut db,
        &format!("SELECT COUNT(*) FROM parquet_metadata('{url}');"),
    );
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Bigint(n) if n > 0));
    // The ranged trailer parse yields the same footer facts as the local read.
    assert_eq!(
        select(
            &mut db,
            &format!("SELECT footer_size, num_row_groups FROM parquet_file_metadata('{url}');")
        ),
        vec![vec![Value::Bigint(452), Value::Bigint(4)]]
    );
}

#[test]
fn http_parquet_errors_are_clear() {
    let (_lock, _env) = EnvGuard::http_allow();
    let server = TestHttpServer::start();
    server.put_status("/denied.parquet", 403, b"no");
    let mut db = store();

    let err = db
        .execute(&format!(
            "SELECT * FROM read_parquet('{}');",
            server.url("/missing.parquet")
        ))
        .unwrap_err()
        .to_string();
    assert!(err.contains("404") || err.contains("not found"), "{err}");
    let err = db
        .execute(&format!(
            "SELECT * FROM read_parquet('{}');",
            server.url("/denied.parquet")
        ))
        .unwrap_err()
        .to_string();
    assert!(err.contains("403") || err.contains("AccessDenied"), "{err}");
    // A glob against a remote URL is refused before any request.
    let err = db
        .execute(&format!(
            "SELECT * FROM read_parquet('{}/*.parquet');",
            server.base()
        ))
        .unwrap_err()
        .to_string();
    assert!(err.contains("single objects"), "{err}");
}

#[test]
fn http_parquet_unknown_size_falls_back_to_one_whole_fetch() {
    let (_lock, _env) = EnvGuard::http_allow();
    let server = TestHttpServer::start();
    server.put("/rg.parquet", &fixture());
    server.set_head_unsupported(true);
    let url = server.url("/rg.parquet");

    let mut db = store();
    assert_eq!(
        select(
            &mut db,
            &format!("SELECT COUNT(*), MIN(id), MAX(id) FROM read_parquet('{url}');")
        ),
        vec![vec![
            Value::Bigint(8192),
            Value::Integer(0),
            Value::Integer(8191)
        ]]
    );
    let gets = gets(&server);
    assert_eq!(gets.len(), 1, "the whole object is fetched once: {gets:?}");
    assert!(gets[0].1.is_empty(), "the fallback is a whole GET");
}

#[test]
fn http_parquet_prepared_statements_re_read() {
    let (_lock, _env) = EnvGuard::http_allow();
    let server = TestHttpServer::start();
    server.put("/rg.parquet", &fixture());
    let url = server.url("/rg.parquet");

    let mut db = store();
    let stmt = db
        .prepare(&format!(
            "SELECT COUNT(*) FROM read_parquet('{url}') WHERE id >= ?;"
        ))
        .unwrap();
    for (bound, expected) in [(7000, 1192i64), (8000, 192)] {
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
    // Each execution is a fresh bind + scan, so the object is re-read.
    assert!(gets(&server).len() >= 2);
}

#[test]
fn http_parquet_durable_replay_re_reads_and_missing_is_hard() {
    let (_lock, _env) = EnvGuard::http_allow();
    let server = TestHttpServer::start();
    server.put("/rg.parquet", &fixture());
    let url = server.url("/rg.parquet");
    let (dir, path) = temp_store("durable");

    pinkdb_parquet::ensure_registered();
    let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    db.execute("CREATE TABLE dst (id INTEGER);").unwrap();
    db.execute(&format!(
        "INSERT INTO dst SELECT id FROM read_parquet('{url}') WHERE id < 5;"
    ))
    .unwrap();
    assert_eq!(
        select(&mut db, "SELECT COUNT(*), MIN(id), MAX(id) FROM dst;"),
        vec![vec![Value::Bigint(5), Value::Integer(0), Value::Integer(4)]]
    );
    drop(db);

    // Recovery re-reads the remote object.
    let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    assert_eq!(
        select(&mut db, "SELECT COUNT(*) FROM dst;"),
        vec![vec![Value::Bigint(5)]]
    );
    drop(db);

    // The object vanished: replay is a hard error, never a silent drop.
    drop(server);
    let err = PinkDb::open_durable(path.to_str().unwrap())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("rg.parquet") || err.contains("cannot read"),
        "{err}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn s3_parquet_reads_ranged_through_the_mock() {
    let mock = mock_s3::MockS3::start();
    let bytes = fixture();
    let len = bytes.len() as u64;
    mock.put_object("lake/rg.parquet", &bytes);
    let (_lock, _env) = EnvGuard::s3(&mock);

    let mut db = store();
    assert_eq!(
        select(
            &mut db,
            "SELECT COUNT(*), MIN(id), MAX(id) FROM read_parquet('s3://bucket/lake/rg.parquet');"
        ),
        vec![vec![
            Value::Bigint(8192),
            Value::Integer(0),
            Value::Integer(8191)
        ]]
    );
    let gets: Vec<(String, String)> = mock
        .requests()
        .into_iter()
        .zip(mock.ranges())
        .filter(|(request, _)| request.starts_with("GET "))
        .collect();
    assert!(!gets.is_empty());
    assert!(gets.iter().all(|(_, r)| !r.is_empty()), "{gets:?}");
    assert!(
        gets.iter()
            .any(|(_, r)| range(r) == Some((len - 8, len - 1))),
        "no ranged footer over S3: {gets:?}"
    );
    assert!(mock.verified_requests() >= 1, "every request is signed");
}
