//! Wave 98b: fast lake reads on object storage — the request pattern.
//!
//! These tests pin **how many transport requests and bytes** an attach and a
//! query issue against the hermetic mock S3. The baseline (measured at the
//! wave-98a commit `e9d5275` in a detached worktree, same fixture) was:
//!
//! | phase | requests | bytes |
//! |---|---|---|
//! | open | 8 | manifest only |
//! | attach | 4 | whole data file (361 219 B) |
//! | `COUNT(*)` | 0 | 0 |
//! | `SELECT i, g` | 2 | the object twice (722 438 B) |
//!
//! After wave 98b the attach reads two footer ranges instead of the object,
//! and the query reads only the projected segments (asserted below). The
//! request count of the scan rises because footer/metadata reads are now
//! small separate ranges; bytes are what the plan targets.
//!
//! The fixture is deliberately ordinary: one table, one data file, four
//! columns, 20 000 rows. `COUNT(*)` must stay metadata-only (no data-file
//! request at all); the two-column query is the one that has to read data.

#![cfg(all(feature = "s3", feature = "vortexlake"))]
#[path = "common/local_lake.rs"]
mod local_lake;
use local_lake::*;

#[path = "../src/mock_s3.rs"]
mod mock_s3;

use mock_s3::MockS3;
use pink_lake::S3Config;
use pinkdb::{PinkDb, QueryResult, Value};

fn mock_config(mock: &MockS3, prefix: &str) -> S3Config {
    S3Config::new("fast-bucket")
        .with_prefix(prefix)
        .with_region("auto")
        .with_endpoint(mock.endpoint())
        .allow_http(true)
        .with_credentials(mock.access_key_id(), "mock-secret")
}

fn fast_store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE main.t (i INTEGER, g BIGINT, s VARCHAR, d DOUBLE);")
        .unwrap();
    {
        let t = db.table_mut("t").unwrap();
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state
        };
        for row in 0..20_000i64 {
            let r = next();
            t.append_row(vec![
                Value::Integer((r % 500) as i32 - 250),
                Value::Bigint(r as i64),
                Value::Varchar(format!("k{}-{row}", r % 100)),
                Value::Double((r % 4000) as f64 / 8.0 - 250.0),
            ])
            .unwrap();
        }
    }
    db
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn save_fixture(mock: &MockS3, prefix: &str) {
    let mut lake = create_s3_lake(mock_config(mock, prefix)).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.write_from_store(&fast_store()).unwrap();
}

/// One open + attach + query against a fresh handle; returns the request and
/// byte counts of each phase.
#[allow(clippy::type_complexity)]
fn measure(
    mock: &MockS3,
    prefix: &str,
    sql: &str,
) -> (
    usize,
    usize,
    usize,
    (u64, u64, u64),
    (u64, u64, u64),
    (u64, u64, u64),
    Vec<Vec<Value>>,
) {
    let before = mock.requests().len();
    let lake = open_s3_lake(mock_config(mock, prefix)).unwrap();
    let after_open = mock.requests().len();
    let open_stats = lake.transport_stats();
    let report = lake.attach(None).unwrap();
    let after_attach = mock.requests().len();
    let attach_stats = lake.transport_stats();
    let mut db = PinkDb::new();
    db.attach_lake(report.lake).unwrap();
    let result = rows(&mut db, sql);
    let after_query = mock.requests().len();
    let query_stats = lake.transport_stats();
    assert_eq!(report.requests as usize, after_attach - after_open);
    assert_eq!(
        report.bytes,
        attach_stats.1.saturating_sub(open_stats.1),
        "AttachmentReport::bytes must be the attach's own transport bytes"
    );
    (
        after_open - before,
        after_attach - after_open,
        after_query - after_attach,
        open_stats,
        (report.requests, report.bytes, report.wait_nanos),
        (
            query_stats.0.saturating_sub(attach_stats.0),
            query_stats.1.saturating_sub(attach_stats.1),
            query_stats.2.saturating_sub(attach_stats.2),
        ),
        result,
    )
}

/// The bytes of one lake's Vortex data file (the whole-object baseline).
fn data_file_bytes(mock: &MockS3, prefix: &str) -> usize {
    mock.keys()
        .iter()
        .filter(|key| key.starts_with(prefix) && key.ends_with(".vortex"))
        .map(|key| mock.object(key).unwrap().len())
        .sum()
}

#[test]
fn vortex_lake_attach_and_queries_use_the_ranged_read_path() {
    let mock = MockS3::start();
    save_fixture(&mock, "fast/vortex");
    let file_bytes = data_file_bytes(&mock, "fast/vortex");
    assert!(file_bytes > 100_000, "the fixture must be a real file");
    eprintln!("vortex data file: {file_bytes} B");

    // `COUNT(*)` is answered from the pinned statistics: attach reads the
    // manifest plus each file's footer, and the query reads no data.
    let (open_count, attach_count, query_count, open_stats, attach_stats, query_stats, count_rows) =
        measure(&mock, "fast/vortex", "SELECT COUNT(*) FROM main.t;");
    eprintln!(
        "vortex attach+count(*): open={open_count} req/{} B attach={attach_count} req/{} B \
         query={query_count} req/{} B",
        open_stats.1, attach_stats.1, query_stats.1
    );
    assert_eq!(count_rows, vec![vec![Value::Bigint(20_000)]]);
    assert_eq!(open_count, 0, "the catalog is local, not object storage");
    assert_eq!(
        attach_count, 2,
        "attach: the two footer ranges of the one Vortex file"
    );
    assert_eq!(query_count, 0, "COUNT(*) must not read a data file");
    assert!(
        attach_stats.1 < file_bytes as u64,
        "attach reads footers, not the object: {} vs {file_bytes}",
        attach_stats.1
    );

    // A two-column scan reads only the projected segments, not the object.
    let (open_scan, attach_scan, query_scan, open_stats, attach_stats, query_stats, scan_rows) =
        measure(&mock, "fast/vortex", "SELECT i, g FROM main.t;");
    eprintln!(
        "vortex attach+2col: open={open_scan} req/{} B attach={attach_scan} req/{} B \
         query={query_scan} req/{} B",
        open_stats.1, attach_stats.1, query_stats.1
    );
    assert_eq!(scan_rows.len(), 20_000);
    assert_eq!(open_scan, 0);
    assert_eq!(attach_scan, 2);
    assert!(query_scan >= 1, "the scan reads at least one data range");
    assert!(
        query_stats.1 < file_bytes as u64,
        "a two-column projection reads fewer bytes than the whole file: {} vs {file_bytes}",
        query_stats.1
    );
    // Every value and its order match the eager load exactly.
    let loaded = open_s3_lake(mock_config(&mock, "fast/vortex"))
        .unwrap()
        .load(None)
        .unwrap();
    let table = loaded.table("t").unwrap();
    let expected: Vec<Vec<Value>> = (0..table.num_rows())
        .map(|row| vec![table.get(row, 0).unwrap(), table.get(row, 1).unwrap()])
        .collect();
    assert_eq!(scan_rows, expected);
}
