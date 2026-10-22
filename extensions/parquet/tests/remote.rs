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
/// (`PINKDB_HTTPFS_ALLOW_HTTP`, `PINKDB_HTTPFS_PARALLELISM`, `AWS_*`) and
/// restore it afterwards.
struct EnvGuard(Vec<&'static str>);

impl EnvGuard {
    fn http_allow() -> (MutexGuard<'static, ()>, Self) {
        Self::http_allow_parallel(None)
    }

    /// `Some(n)` pins `PINKDB_HTTPFS_PARALLELISM`; `None` leaves the default.
    fn http_allow_parallel(parallelism: Option<usize>) -> (MutexGuard<'static, ()>, Self) {
        let guard = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // SAFETY: the lock serializes every env-mutating test in this binary.
        unsafe {
            std::env::set_var("PINKDB_HTTPFS_ALLOW_HTTP", "1");
            match parallelism {
                Some(n) => std::env::set_var("PINKDB_HTTPFS_PARALLELISM", n.to_string()),
                None => std::env::remove_var("PINKDB_HTTPFS_PARALLELISM"),
            }
        }
        (
            guard,
            Self(vec![
                "PINKDB_HTTPFS_ALLOW_HTTP",
                "PINKDB_HTTPFS_PARALLELISM",
            ]),
        )
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

/// A deliberately *large* multi-row-group fixture (wave 98a's request-count
/// tests): `groups` row groups of `rows` rows, seven unprojected/typed
/// columns, one of them a wide VARCHAR so a serial 128 KiB reader needs many
/// requests per chunk.
fn big_fixture(groups: usize, rows: usize) -> Vec<u8> {
    use parquet::basic::Compression;
    use parquet::data_type::{ByteArray, ByteArrayType, DoubleType, Int32Type, Int64Type};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;

    let schema = std::sync::Arc::new(
        parse_message_type(
            "message schema {
               REQUIRED INT32 id;
               REQUIRED INT64 seq;
               REQUIRED DOUBLE score;
               REQUIRED BOOLEAN flag;
               REQUIRED BINARY label (UTF8);
               REQUIRED INT32 day (DATE);
               REQUIRED INT64 ts (TIMESTAMP(MICROS,false));
             }",
        )
        .expect("the big fixture schema"),
    );
    let props = std::sync::Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build(),
    );
    let mut out = Vec::new();
    let mut writer = SerializedFileWriter::new(&mut out, schema, props).expect("fixture writer");
    for group in 0..groups {
        let base = group * rows;
        let mut rg = writer.next_row_group().expect("fixture row group");

        let ids: Vec<i32> = (0..rows).map(|i| (base + i) as i32).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        col.typed::<Int32Type>()
            .write_batch(&ids, None, None)
            .unwrap();
        col.close().unwrap();

        let seqs: Vec<i64> = (0..rows).map(|i| (base + i) as i64 * 3).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        col.typed::<Int64Type>()
            .write_batch(&seqs, None, None)
            .unwrap();
        col.close().unwrap();

        let scores: Vec<f64> = (0..rows).map(|i| (base + i) as f64 * 0.25).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        col.typed::<DoubleType>()
            .write_batch(&scores, None, None)
            .unwrap();
        col.close().unwrap();

        let flags: Vec<bool> = (0..rows).map(|i| (base + i).is_multiple_of(3)).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        col.typed::<parquet::data_type::BoolType>()
            .write_batch(&flags, None, None)
            .unwrap();
        col.close().unwrap();

        // 512 bytes per row: a ~1.5 MiB chunk at 3000 rows, well over the
        // legacy 128 KiB read block.
        let labels: Vec<ByteArray> = (0..rows)
            .map(|i| {
                let mut text = format!("{:0>16}-", base + i).into_bytes();
                text.resize(512, b'x');
                ByteArray::from(text)
            })
            .collect();
        let mut col = rg.next_column().unwrap().unwrap();
        col.typed::<ByteArrayType>()
            .write_batch(&labels, None, None)
            .unwrap();
        col.close().unwrap();

        let days: Vec<i32> = (0..rows)
            .map(|i| 19_000 + (base + i) as i32 % 365)
            .collect();
        let mut col = rg.next_column().unwrap().unwrap();
        col.typed::<Int32Type>()
            .write_batch(&days, None, None)
            .unwrap();
        col.close().unwrap();

        let stamps: Vec<i64> = (0..rows)
            .map(|i| 1_700_000_000_000_000 + (base + i) as i64)
            .collect();
        let mut col = rg.next_column().unwrap().unwrap();
        col.typed::<Int64Type>()
            .write_batch(&stamps, None, None)
            .unwrap();
        col.close().unwrap();
        rg.close().unwrap();
    }
    writer.close().unwrap();
    out
}

fn write_temp(name: &str, bytes: &[u8]) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb-parquet-remote-fixture-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, bytes).unwrap();
    path
}

fn store() -> PinkDb {
    store_with_source().0
}

fn store_with_source() -> (PinkDb, std::sync::Arc<pinkdb_parquet::ParquetSource>) {
    let mut db = PinkDb::new();
    let source = std::sync::Arc::new(pinkdb_parquet::ParquetSource::new());
    db.register_table_source(source.clone());
    (db, source)
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

/// The GETs that read object data rather than the footer: a footer range
/// always starts in the last few KiB of the object.
fn data_gets(server: &TestHttpServer, len: u64) -> Vec<(u64, u64)> {
    gets(server)
        .into_iter()
        .filter_map(|(_, spec)| range(&spec))
        .filter(|(start, _)| *start < len.saturating_sub(4096))
        .collect()
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

/// Wave 98a's headline measurement: a multi-row-group file read with a
/// narrow projection issues at most `row_groups × columns` data requests on
/// the default (read-ahead) path, while `PINKDB_HTTPFS_PARALLELISM=1` keeps
/// today's lazy 128 KiB pattern. Values and row order must be identical.
#[test]
fn remote_request_pattern_before_and_after() {
    let bytes = big_fixture(3, 3000);
    let len = bytes.len() as u64;
    let local = write_temp("big.parquet", &bytes);
    let local_sql = format!(
        "SELECT id, label FROM read_parquet('{}') WHERE id >= 3000;",
        local.display()
    );

    let run = |parallelism: Option<usize>| {
        let (_lock, _env) = EnvGuard::http_allow_parallel(parallelism);
        let server = TestHttpServer::start();
        server.put("/big.parquet", &bytes);
        let (mut db, source) = store_with_source();
        let url = server.url("/big.parquet");
        let rows = select(
            &mut db,
            &format!("SELECT id, label FROM read_parquet('{url}') WHERE id >= 3000;"),
        );
        let data = data_gets(&server, len);
        (rows, data, source.remote_stats())
    };

    // The legacy serial path (parallelism 1) is the "before" measurement.
    let (serial_rows, serial_data, serial_stats) = run(Some(1));
    // The new default (8) is the "after" measurement.
    let (fast_rows, fast_data, fast_stats) = run(None);

    let (mut db, _) = store_with_source();
    let local_rows = select(&mut db, &local_sql);
    assert_eq!(serial_rows, local_rows, "serial values/order changed");
    assert_eq!(fast_rows, local_rows, "read-ahead values/order changed");
    assert_eq!(fast_rows.len(), 6000);

    // Two row groups survive `id >= 3000`; each projects two chunks (merged
    // into one request by the gap coalescing).
    assert!(
        fast_data.len() <= 2 * 2,
        "expected at most 4 data requests, saw {}: {fast_data:?}",
        fast_data.len()
    );
    assert!(
        serial_data.len() > 2 * 2,
        "the serial path should need one request per 128 KiB block, saw {}: \
         {serial_data:?}",
        serial_data.len()
    );
    assert!(
        fast_data.len() < serial_data.len(),
        "read-ahead did not reduce requests: {} vs {}",
        fast_data.len(),
        serial_data.len()
    );
    assert!(fast_stats.requests > 0, "{fast_stats:?}");
    assert!(fast_stats.bytes > 0, "{fast_stats:?}");
    assert!(fast_stats.wait_nanos > 0, "{fast_stats:?}");
    println!(
        "wave 98a local test server: serial={} data requests ({serial_stats:?}), \
         read-ahead={} data requests ({fast_stats:?})",
        serial_data.len(),
        fast_data.len()
    );
    let _ = std::fs::remove_dir_all(local.parent().unwrap());
}

/// `COUNT(*)` is answered from the footer's row counts: zero data-page
/// requests, for local and remote files.
#[test]
fn count_star_reads_only_the_footer() {
    let bytes = big_fixture(3, 3000);
    let len = bytes.len() as u64;
    let local = write_temp("count.parquet", &bytes);

    let (_lock, _env) = EnvGuard::http_allow_parallel(None);
    let server = TestHttpServer::start();
    server.put("/count.parquet", &bytes);
    let (mut db, source) = store_with_source();
    let url = server.url("/count.parquet");

    assert_eq!(
        select(
            &mut db,
            &format!("SELECT COUNT(*) FROM read_parquet('{url}');")
        ),
        vec![vec![Value::Bigint(9000)]]
    );
    let delta = source.remote_stats();
    assert!(
        delta.requests <= 8,
        "COUNT(*) made {} transport calls (expected footer reads only)",
        delta.requests
    );
    assert!(
        data_gets(&server, len).is_empty(),
        "COUNT(*) fetched data pages: {:?}",
        data_gets(&server, len)
    );
    let stats = db.table_source_stats().pop().unwrap();
    assert_eq!((stats.row_groups, stats.columns_decoded), (3, 0));

    // The local file takes the same metadata-only path (no data decode).
    let mut db = store();
    let before = db.table_source_stats().pop().unwrap();
    assert_eq!(
        select(
            &mut db,
            &format!("SELECT COUNT(*) FROM read_parquet('{}');", local.display())
        ),
        vec![vec![Value::Bigint(9000)]]
    );
    let after = db.table_source_stats().pop().unwrap();
    assert_eq!(after.row_groups - before.row_groups, 3);
    assert_eq!(after.columns_decoded - before.columns_decoded, 0);
    let _ = std::fs::remove_dir_all(local.parent().unwrap());
}

/// The in-flight request limit is respected (and the serial path stays at
/// one request at a time). The test server delays responses so overlapping
/// requests are observable.
#[test]
fn read_ahead_inflight_limit_is_respected() {
    let bytes = big_fixture(3, 1500);
    let len = bytes.len() as u64;
    let run = |parallelism: usize| {
        let (_lock, _env) = EnvGuard::http_allow_parallel(Some(parallelism));
        let server = TestHttpServer::start();
        server.put("/big.parquet", &bytes);
        server.set_delay(Some(std::time::Duration::from_millis(20)));
        let mut db = store();
        let url = server.url("/big.parquet");
        let rows = select(
            &mut db,
            &format!("SELECT id, label FROM read_parquet('{url}');"),
        );
        assert_eq!(rows.len(), 4500);
        (server.max_active(), data_gets(&server, len).len())
    };

    let (serial_max, _serial_requests) = run(1);
    assert_eq!(serial_max, 1, "the serial path issued concurrent requests");

    let (fast_max, fast_requests) = run(4);
    assert!(fast_requests > 1, "no read-ahead happened");
    assert!(
        fast_max > 1 && fast_max <= 4,
        "in-flight limit 4 was not respected (max {fast_max}, {fast_requests} requests)"
    );
}

/// A transient range failure is retried by the transport; a persistent one
/// and a short range body both fail the statement cleanly.
#[test]
fn remote_range_errors_retry_and_fail_cleanly() {
    let bytes = big_fixture(2, 1000);
    let query = |port: &TestHttpServer| {
        format!(
            "SELECT id, label FROM read_parquet('{}');",
            port.url("/big.parquet")
        )
    };

    {
        let (_lock, _env) = EnvGuard::http_allow_parallel(None);
        let server = TestHttpServer::start();
        server.put("/big.parquet", &bytes);
        server.fail_next("/big.parquet", 2, 503);
        let mut db = store();
        assert_eq!(
            select(&mut db, &query(&server)).len(),
            2000,
            "a 503 was not retried into a successful scan"
        );
        assert!(
            server
                .requests()
                .iter()
                .filter(|line| line.contains("/big.parquet"))
                .count()
                >= 3,
            "the two injected failures were not requested: {:?}",
            server.requests()
        );
    }

    {
        let (_lock, _env) = EnvGuard::http_allow_parallel(None);
        let server = TestHttpServer::start();
        server.put_status("/big.parquet", 500, b"boom");
        let mut db = store();
        let err = db.execute(&query(&server)).unwrap_err().to_string();
        assert!(
            err.contains("500") || err.contains("remote parquet read failed"),
            "{err}"
        );
    }

    {
        let (_lock, _env) = EnvGuard::http_allow_parallel(None);
        let server = TestHttpServer::start();
        server.put("/big.parquet", &bytes);
        server.set_short_read(8);
        let mut db = store();
        let err = db.execute(&query(&server)).unwrap_err().to_string();
        assert!(
            err.contains("remote parquet read failed")
                || err.contains("cannot read parquet file")
                || err.contains("EOF"),
            "a short range body did not fail cleanly: {err}"
        );
    }
}

/// All supported types, no pruning: the read-ahead path returns exactly the
/// local reader's values in exactly its row order (and so does the serial
/// path).
#[test]
fn remote_values_and_order_match_local_without_pruning() {
    let bytes = big_fixture(2, 1000);
    let local = write_temp("values.parquet", &bytes);
    let local_sql = format!("SELECT * FROM read_parquet('{}');", local.display());
    let (mut db, _) = store_with_source();
    let local_rows = select(&mut db, &local_sql);
    assert_eq!(local_rows.len(), 2000);

    let run = |parallelism: Option<usize>| {
        let (_lock, _env) = EnvGuard::http_allow_parallel(parallelism);
        let server = TestHttpServer::start();
        server.put("/values.parquet", &bytes);
        let mut db = store();
        let url = server.url("/values.parquet");
        select(&mut db, &format!("SELECT * FROM read_parquet('{url}');"))
    };
    assert_eq!(run(Some(1)), local_rows, "serial values/order changed");
    assert_eq!(run(None), local_rows, "read-ahead values/order changed");
    let _ = std::fs::remove_dir_all(local.parent().unwrap());
}

/// `EXPLAIN` shows the projection-pushed width, not just the schema width.
#[test]
fn explain_shows_the_projected_width() {
    let bytes = big_fixture(2, 1000);
    let path = write_temp("explain.parquet", &bytes);
    let mut db = store();
    let rows = select(
        &mut db,
        &format!(
            "EXPLAIN SELECT id, label FROM read_parquet('{}') WHERE id >= 1000;",
            path.display()
        ),
    );
    let text = rows
        .iter()
        .map(|row| match &row[0] {
            Value::Varchar(line) => line.clone(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("(2 of 7 columns)"), "{text}");

    let rows = select(
        &mut db,
        &format!(
            "EXPLAIN SELECT COUNT(*) FROM read_parquet('{}');",
            path.display()
        ),
    );
    let text = rows
        .iter()
        .map(|row| match &row[0] {
            Value::Varchar(line) => line.clone(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("(0 of 7 columns)"), "{text}");
    let _ = std::fs::remove_dir_all(path.parent().unwrap());
}
