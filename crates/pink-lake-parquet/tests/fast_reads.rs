//! Wave 98b: the Parquet lake provider's request pattern.
//!
//! Pins how many transport requests and bytes attach and a two-column scan
//! issue over the hermetic mock S3. The baseline (wave-98a commit `e9d5275`,
//! same fixture) was: open 8 requests, attach 3 (manifest + file HEAD, no data
//! bytes), `COUNT(*)` 0, and `SELECT i, g` **one whole-object GET** of the
//! 569 244-byte file. After wave 98b the scan reads the footer and only the
//! projected column-chunk ranges through the shared coalescing/read-ahead
//! core (asserted below).

#[path = "../../pink-lake/src/mock_s3.rs"]
mod mock_s3;

use mock_s3::MockS3;
use pink_lake::{Lake, S3Config};
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

fn s3_catalog(config: &pink_lake::S3Config) -> String {
    let sanitize = |text: &str| -> String {
        text.chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect()
    };
    let dir = std::env::temp_dir()
        .join("pinkdb-s3-catalogs")
        .join(std::process::id().to_string())
        .join(sanitize(&config.bucket))
        .join(sanitize(&config.prefix));
    std::fs::create_dir_all(&dir).ok();
    format!("pink:{}", dir.join("catalog.pink").display())
}

fn create_s3_lake(config: pink_lake::S3Config) -> pink_lake::Result<Lake> {
    let catalog = s3_catalog(&config);
    Lake::create_with_catalog_s3(&catalog, config)
}

fn open_s3_lake(config: pink_lake::S3Config) -> pink_lake::Result<Lake> {
    let catalog = s3_catalog(&config);
    Lake::open_with_catalog_s3(&catalog, config)
}

fn save_fixture(mock: &MockS3, prefix: &str) {
    pink_lake_parquet::register().unwrap();
    let mut lake = create_s3_lake(mock_config(mock, prefix)).unwrap();
    lake.set_data_inlining_row_limit(0);
    lake.set_write_format("parquet").unwrap();
    lake.write_from_store(&fast_store()).unwrap();
}

/// One open + attach + query against a fresh handle; returns request/byte
/// counts and the result rows.
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
    assert_eq!(report.bytes, attach_stats.1.saturating_sub(open_stats.1));
    (
        after_open - before,
        after_attach - after_open,
        after_query - after_attach,
        (report.requests, report.bytes, report.wait_nanos),
        (
            query_stats.0.saturating_sub(attach_stats.0),
            query_stats.1.saturating_sub(attach_stats.1),
            query_stats.2.saturating_sub(attach_stats.2),
        ),
        result,
    )
}

fn data_file_bytes(mock: &MockS3, prefix: &str) -> usize {
    mock.keys()
        .iter()
        .filter(|key| key.starts_with(prefix) && key.ends_with(".parquet"))
        .map(|key| mock.object(key).unwrap().len())
        .sum()
}

#[test]
fn parquet_lake_attach_and_queries_use_the_ranged_read_path() {
    let mock = MockS3::start();
    save_fixture(&mock, "fast/parquet");
    let file_bytes = data_file_bytes(&mock, "fast/parquet");
    assert!(file_bytes > 100_000, "the fixture must be a real file");
    eprintln!("parquet data file: {file_bytes} B");

    let (open_count, attach_count, query_count, attach_stats, query_stats, count_rows) =
        measure(&mock, "fast/parquet", "SELECT COUNT(*) FROM main.t;");
    eprintln!(
        "parquet attach+count(*): open={open_count} attach={attach_count} req/{} B \
         query={query_count} req/{} B",
        attach_stats.1, query_stats.1
    );
    assert_eq!(count_rows, vec![vec![Value::Bigint(20_000)]]);
    assert_eq!(open_count, 0, "the catalog is local, not object storage");
    assert_eq!(
        attach_count, 0,
        "statistics come from the catalog; attach reads no object"
    );
    assert_eq!(query_count, 0, "COUNT(*) must not read a data file");
    assert!(
        attach_stats.1 < file_bytes as u64,
        "attach reads the manifest, not the data file: {} vs {file_bytes}",
        attach_stats.1
    );

    let (open_scan, attach_scan, query_scan, attach_stats, query_stats, scan_rows) =
        measure(&mock, "fast/parquet", "SELECT i, g FROM main.t;");
    eprintln!(
        "parquet attach+2col: open={open_scan} attach={attach_scan} req/{} B \
         query={query_scan} req/{} B",
        attach_stats.1, query_stats.1
    );
    assert_eq!(scan_rows.len(), 20_000);
    assert_eq!(open_scan, 0);
    assert_eq!(attach_scan, 0);
    assert!(query_scan >= 1, "the scan reads at least one data range");
    assert!(
        query_stats.1 < file_bytes as u64,
        "a two-column projection reads fewer bytes than the whole file: {} vs {file_bytes}",
        query_stats.1
    );
    // Every value and its order match the eager load exactly.
    let loaded = open_s3_lake(mock_config(&mock, "fast/parquet"))
        .unwrap()
        .load(None)
        .unwrap();
    let table = loaded.table("t").unwrap();
    let expected: Vec<Vec<Value>> = (0..table.num_rows())
        .map(|row| vec![table.get(row, 0).unwrap(), table.get(row, 1).unwrap()])
        .collect();
    assert_eq!(scan_rows, expected);
}
