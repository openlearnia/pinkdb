//! `.pink` lake data files (`PINKDB06`, wave 102) are read by range over
//! object storage: attach answers `COUNT(*)` from the catalog, a query reads
//! only the projected column chunks of the row groups that survive its
//! filters, and the catalog's footer location means no trailer probe.
//!
//! Measured against the mock S3 (every request and its `Range` header are
//! recorded). The table has two row groups (200,000 rows; a row group is
//! 122,880 rows) and a monotonic `seq` column, so a `seq` predicate prunes
//! one row group exactly.

#![cfg(feature = "s3")]
#[path = "common/local_lake.rs"]
mod local_lake;
use local_lake::*;

#[path = "../src/mock_s3.rs"]
mod mock_s3;

use mock_s3::MockS3;
use pink_lake::S3Config;
use pinkdb::{PinkDb, QueryResult, Value};

const ROWS: i64 = 200_000;

fn mock_config(mock: &MockS3, prefix: &str) -> S3Config {
    S3Config::new("pink-bucket")
        .with_prefix(prefix)
        .with_region("auto")
        .with_endpoint(mock.endpoint())
        .allow_http(true)
        .with_credentials(mock.access_key_id(), "mock-secret")
}

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (seq BIGINT, i INTEGER, s VARCHAR, d DOUBLE);")
        .unwrap();
    let t = db.table_mut("t").unwrap();
    for row in 0..ROWS {
        t.append_row(vec![
            Value::Bigint(row),
            Value::Integer((row % 1000) as i32),
            Value::Varchar(format!("value-{}-{}", row % 97, row)),
            Value::Double(row as f64 / 3.0),
        ])
        .unwrap();
    }
    db
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// The data-file requests issued since `from`: `(ranged GETs, whole-object
/// GETs)` against `.pink` objects.
fn data_requests(mock: &MockS3, from: usize) -> (usize, usize) {
    let requests = mock.requests();
    let ranges = mock.ranges();
    let mut ranged = 0;
    let mut whole = 0;
    for (request, range) in requests.iter().zip(ranges.iter()).skip(from) {
        if request.starts_with("GET ") && request.contains(".pink") {
            if range.is_empty() {
                whole += 1;
            } else {
                ranged += 1;
            }
        }
    }
    (ranged, whole)
}

/// The bytes requested by ranged GETs of `.pink` objects since `from`.
fn pink_range_bytes(mock: &MockS3, from: usize) -> usize {
    mock.requests()
        .iter()
        .zip(mock.ranges().iter())
        .skip(from)
        .filter(|(request, _)| request.starts_with("GET ") && request.contains(".pink"))
        .filter_map(|(_, range)| {
            let (start, end) = range.strip_prefix("bytes=")?.split_once('-')?;
            Some(end.parse::<usize>().ok()? - start.parse::<usize>().ok()? + 1)
        })
        .sum()
}

/// One fresh attach + query; returns the data requests the query issued
/// (attach itself must issue none) and the result rows.
fn attached_query(mock: &MockS3, prefix: &str, sql: &str) -> ((usize, usize), Vec<Vec<Value>>) {
    let lake = open_s3_lake(mock_config(mock, prefix)).unwrap();
    let before_attach = mock.requests().len();
    let report = lake.attach(None).unwrap();
    assert_eq!(
        data_requests(mock, before_attach),
        (0, 0),
        "attach reads only the catalog for a `.pink` lake"
    );
    let before_query = mock.requests().len();
    let mut db = PinkDb::new();
    db.attach_lake(report.lake).unwrap();
    let result = rows(&mut db, sql);
    (data_requests(mock, before_query), result)
}

#[test]
fn pink_lake_reads_only_projected_chunks_of_surviving_row_groups() {
    let mock = MockS3::start();
    let prefix = "lakes/pink";
    let mut source = store();
    {
        let mut lake = create_s3_lake(mock_config(&mock, prefix)).unwrap();
        lake.set_write_format("pink").unwrap();
        lake.set_data_inlining_row_limit(0);
        lake.write_from_store(&source).unwrap();
    }

    // The catalog records the data file's footer location (format_version 7).
    let lake = open_s3_lake(mock_config(&mock, prefix)).unwrap();
    assert_eq!(lake.format_version(), 7);
    assert_eq!(
        lake.table_format_name("main.t").unwrap().as_deref(),
        Some("pink")
    );
    let info = lake
        .list_files(Some("main.t"), None, false)
        .unwrap()
        .into_iter()
        .next()
        .expect("one data file");
    let file_path = info.path.clone();
    assert!(info.footer_offset.is_some());
    assert!(info.footer_len.is_some());
    let file_len = mock
        .object(&format!("{prefix}/{}", file_path))
        .unwrap()
        .len();

    // COUNT(*) is answered from the catalog: no data-file request at all.
    let ((ranged, whole), got) = attached_query(&mock, prefix, "SELECT COUNT(*) FROM t;");
    assert_eq!(got, vec![vec![Value::Bigint(ROWS)]]);
    assert_eq!((ranged, whole), (0, 0), "COUNT(*) is metadata-only");

    // Two of four columns over both row groups: ranged requests only — the
    // footer (located by the catalog, no trailer probe) plus the projected
    // chunks — never a whole-object download.
    let before = mock.requests().len();
    let sql = "SELECT SUM(i), MAX(d) FROM t;";
    let ((ranged, whole), got) = attached_query(&mock, prefix, sql);
    assert_eq!(got, rows(&mut source, sql), "{sql}");
    assert_eq!(
        whole, 0,
        "a projected `.pink` read never downloads the whole file"
    );
    assert!(ranged >= 2, "footer + at least one chunk range: {ranged}");
    let fetched = pink_range_bytes(&mock, before);
    assert!(
        fetched < file_len * 3 / 4,
        "two of four columns read {fetched} of {file_len} bytes"
    );

    // A `seq` predicate prunes the second row group: fewer bytes still, the
    // same answer as the in-memory store.
    let before = mock.requests().len();
    let sql = "SELECT COUNT(*), SUM(i) FROM t WHERE seq < 1000;";
    let ((_, whole), got) = attached_query(&mock, prefix, sql);
    assert_eq!(got, rows(&mut source, sql), "{sql}");
    assert_eq!(whole, 0);
    let pruned_fetch = pink_range_bytes(&mock, before);
    assert!(
        pruned_fetch < fetched,
        "pruning one of two row groups reads less: {pruned_fetch} vs {fetched}"
    );
}

/// Wave 96a: attach never probes data files (DuckLake's behavior). A
/// statistics-only attach issues no request of any kind against a `.pink`
/// object — no GET and no existence HEAD — so on object storage it costs
/// only the catalog reads. A data file deleted behind the lake's back is
/// therefore not noticed at attach: the statistics-only `COUNT(*)` still
/// answers from the catalog, and the first query that has to read the file
/// fails with a clear error naming it.
#[test]
fn attach_never_probes_data_files_and_a_missing_file_fails_at_first_read() {
    let mock = MockS3::start();
    let prefix = "lakes/no-probe";
    {
        let mut lake = create_s3_lake(mock_config(&mock, prefix)).unwrap();
        lake.set_write_format("pink").unwrap();
        lake.set_data_inlining_row_limit(0);
        lake.write_from_store(&store()).unwrap();
    }

    let lake = open_s3_lake(mock_config(&mock, prefix)).unwrap();
    let before_attach = mock.requests().len();
    let report = lake.attach(None).unwrap();
    let data_touches: Vec<String> = mock.requests()[before_attach..]
        .iter()
        .filter(|request| request.contains("/data/"))
        .cloned()
        .collect();
    assert!(
        data_touches.is_empty(),
        "attach must not touch a data object: {data_touches:?}"
    );
    drop(report);

    // Remove the data file, then attach again: still fine, and COUNT(*) is
    // still metadata-only.
    let data_key = mock
        .keys()
        .into_iter()
        .find(|key| key.starts_with(&format!("{prefix}/data/")) && key.ends_with(".pink"))
        .expect("the lake has a .pink data object");
    mock.remove_object(&data_key);
    let lake = open_s3_lake(mock_config(&mock, prefix)).unwrap();
    let report = lake.attach(None).unwrap();
    let mut db = PinkDb::new();
    db.attach_lake(report.lake).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(ROWS)]]
    );

    // The first real read fails, and says which file.
    let err = db
        .execute("SELECT SUM(i) FROM t;")
        .expect_err("reading a deleted data file must fail")
        .to_string();
    let file_name = data_key.rsplit('/').next().unwrap();
    assert!(
        err.contains(file_name),
        "the error must name the missing data file {file_name}: {err}"
    );
}
