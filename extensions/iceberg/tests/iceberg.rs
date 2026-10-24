//! End-to-end tests for the iceberg reader against the committed fixtures
//! (`tests/fixtures`, written by `tests/fixtures/gen.py` with the DuckDB 2.0
//! binary), cross-checked against DuckDB's own `iceberg` extension when that
//! binary is present, and against the same tables served remotely.
//!
//! The fixtures are deliberately small and hand-written: metadata JSON, Avro
//! manifest lists and manifests, and Parquet data files carrying Iceberg
//! field ids. `simple` has two snapshots, `deletes` a positional delete file,
//! `evolution` a renamed and an added column, `partitioned` two partitions.

use std::path::{Path, PathBuf};
use std::process::Command;

use pinkdb::{PinkDb, QueryResult, Value};

/// The DuckDB 2.0 binary the oracle compares against. The oracle tests are
/// skipped with a note when it is absent (a checkout without the reference
/// engine still runs every other test here).
const DUCKDB: &str = "/tmp/duckdb-2.0.0/duckdb";

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn table(name: &str) -> String {
    fixtures().join(name).display().to_string()
}

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    pinkdb_iceberg::register(&mut db);
    db
}

fn select(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut out = db.execute(sql).expect("query");
    match out.pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn stats(db: &PinkDb) -> pinkdb::TableSourceStats {
    db.table_source_stats()
        .pop()
        .expect("the source is registered")
}

/// Rows as text, one `|`-joined line each: the same shape DuckDB's
/// `-csv -sep '|' -noheader` prints, so the oracle compares like for like.
fn lines(db: &mut PinkDb, sql: &str) -> Vec<String> {
    select(db, sql)
        .into_iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    // DuckDB's CSV output spells a null `NULL`.
                    Value::Null => "NULL".to_string(),
                    other => other.to_string(),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[test]
fn scans_every_fixture_table() {
    let mut db = store();

    assert_eq!(
        lines(
            &mut db,
            &format!(
                "SELECT * FROM iceberg_scan('{}') ORDER BY 1",
                table("simple")
            )
        ),
        ["1|ann", "2|bob", "3|cy"]
    );
    assert_eq!(
        lines(
            &mut db,
            &format!(
                "SELECT * FROM iceberg_scan('{}') ORDER BY 1",
                table("deletes")
            )
        ),
        // positions 1 and 3 of the single data file are deleted
        ["1|ann", "3|cy"]
    );
    assert_eq!(
        lines(
            &mut db,
            &format!(
                "SELECT * FROM iceberg_scan('{}') ORDER BY 1",
                table("seqorder")
            )
        ),
        // the delete file's sequence number is below the data file's, so the
        // rewritten file keeps its rows
        ["1|ann", "2|bob"]
    );
    assert_eq!(
        lines(
            &mut db,
            &format!(
                "SELECT * FROM iceberg_scan('{}') ORDER BY 1",
                table("evolution")
            )
        ),
        // `name` was renamed to `full_name` (same field id) and `age` added
        ["1|ann|NULL", "2|bob|NULL", "3|cy|NULL", "4|dee|40"]
    );
    assert_eq!(
        lines(
            &mut db,
            &format!(
                "SELECT * FROM iceberg_scan('{}') ORDER BY 1",
                table("partitioned")
            )
        ),
        ["1|us|1.5", "2|us|2.5", "3|eu|3.5", "4|eu|4.5"]
    );
}

#[test]
fn a_metadata_file_path_and_a_metadata_directory_work() {
    let mut db = store();
    for path in [
        table("simple"),
        format!("{}/metadata", table("simple")),
        format!("{}/metadata/v2.metadata.json", table("simple")),
    ] {
        assert_eq!(
            lines(
                &mut db,
                &format!("SELECT * FROM iceberg_scan('{path}') ORDER BY 1")
            ),
            ["1|ann", "2|bob", "3|cy"],
            "{path}"
        );
    }
}

#[test]
fn time_travel_picks_the_named_snapshot() {
    let mut db = store();
    let current = format!(
        "SELECT * FROM iceberg_scan('{}') ORDER BY 1",
        table("simple")
    );
    // Snapshot 1001 added data-1.parquet, 1002 added data-2.parquet.
    assert_eq!(
        lines(
            &mut db,
            &format!(
                "SELECT * FROM iceberg_scan('{}', snapshot_from_id := 1001) ORDER BY 1",
                table("simple")
            )
        ),
        ["1|ann", "2|bob"]
    );
    assert_eq!(
        lines(
            &mut db,
            &format!(
                "SELECT * FROM iceberg_scan('{}', snapshot_from_timestamp := \
                 TIMESTAMP '2023-11-14 22:13:30') ORDER BY 1",
                table("simple")
            )
        ),
        ["1|ann", "2|bob"]
    );
    // The newest snapshot at or before a later timestamp is the current one.
    assert_eq!(
        lines(
            &mut db,
            &format!(
                "SELECT * FROM iceberg_scan('{}', snapshot_from_timestamp := \
                 TIMESTAMP '2024-01-01 00:00:00') ORDER BY 1",
                table("simple")
            )
        ),
        ["1|ann", "2|bob", "3|cy"]
    );
    assert_eq!(lines(&mut db, &current), ["1|ann", "2|bob", "3|cy"]);

    let err = db
        .execute(&format!(
            "SELECT * FROM iceberg_scan('{}', snapshot_from_id := 9999)",
            table("simple")
        ))
        .unwrap_err()
        .to_string();
    assert!(err.contains("9999") && err.contains("1001"), "{err}");
}

#[test]
fn count_star_comes_from_the_manifests() {
    let mut db = store();
    let rows = select(
        &mut db,
        &format!("SELECT count(*) FROM iceberg_scan('{}')", table("simple")),
    );
    assert_eq!(rows, vec![vec![Value::Bigint(3)]]);
    // No data page was read: only the metadata and the two manifests.
    let stats = stats(&db);
    assert_eq!(stats.rows, 3);
    assert_eq!(stats.columns_decoded, 0, "{stats:?}");

    // A positional delete still has to be read, so the count is exact.
    let rows = select(
        &mut db,
        &format!("SELECT count(*) FROM iceberg_scan('{}')", table("deletes")),
    );
    assert_eq!(rows, vec![vec![Value::Bigint(2)]]);
}

/// The counters accumulate across statements, so a scan whose pruning is
/// asserted gets a store of its own.
fn scan_stats(sql: &str) -> (Vec<String>, pinkdb::TableSourceStats) {
    let mut db = store();
    let rows = lines(&mut db, sql);
    (rows, stats(&db))
}

#[test]
fn partition_summaries_and_bounds_prune_files() {
    let (rows, stats) = scan_stats(&format!(
        "SELECT * FROM iceberg_scan('{}') ORDER BY 1",
        table("partitioned")
    ));
    assert_eq!(rows, ["1|us|1.5", "2|us|2.5", "3|eu|3.5", "4|eu|4.5"]);
    assert_eq!(stats.files, 2);

    let (rows, stats) = scan_stats(&format!(
        "SELECT * FROM iceberg_scan('{}') WHERE region = 'eu' ORDER BY 1",
        table("partitioned")
    ));
    assert_eq!(rows, ["3|eu|3.5", "4|eu|4.5"]);
    assert_eq!(stats.files, 1, "the us partition is pruned");

    // Column bounds prune the file the predicate cannot match: `simple`'s
    // data-2.parquet holds only id 3.
    let (rows, stats) = scan_stats(&format!(
        "SELECT * FROM iceberg_scan('{}') WHERE id = 3 ORDER BY 1",
        table("simple")
    ));
    assert_eq!(rows, ["3|cy"]);
    assert_eq!(stats.files, 1, "data-1.parquet's upper bound is 2");
}

#[test]
fn introspection_lists_snapshots_and_manifest_entries() {
    let mut db = store();
    assert_eq!(
        lines(
            &mut db,
            &format!(
                "SELECT sequence_number, snapshot_id, parent_id, operation \
                 FROM iceberg_snapshots('{}') ORDER BY 2",
                table("simple")
            )
        ),
        ["1|1001|NULL|append", "2|1002|1001|append"]
    );
    assert_eq!(
        lines(
            &mut db,
            &format!(
                "SELECT manifest_sequence_number, manifest_content, status, content, \
                 file_format, record_count, data_sequence_number, file_sequence_number \
                 FROM iceberg_metadata('{}') ORDER BY file_path",
                table("simple")
            )
        ),
        [
            "1|DATA|ADDED|DATA|PARQUET|2|1|1",
            "2|DATA|ADDED|DATA|PARQUET|1|2|2"
        ]
    );
    // The delete manifest is reported with its own content kind.
    assert_eq!(
        lines(
            &mut db,
            &format!(
                "SELECT manifest_content, content, record_count FROM iceberg_metadata('{}') \
                 WHERE content = 'POSITION_DELETES'",
                table("deletes")
            )
        ),
        ["DELETES|POSITION_DELETES|2"]
    );
}

#[test]
fn refuses_what_it_cannot_honour_by_name() {
    let mut db = store();
    let err = db
        .execute(&format!(
            "SELECT * FROM iceberg_scan('{}', snapshot_from_id := 'x')",
            table("simple")
        ))
        .unwrap_err()
        .to_string();
    assert!(err.contains("snapshot_from_id"), "{err}");

    let err = db
        .execute("SELECT * FROM iceberg_scan('/nonexistent/table')")
        .unwrap_err();
    assert!(err.to_string().contains("/nonexistent/table"), "{err}");
}

#[test]
fn equality_deletes_are_refused_by_name() {
    let mut db = store();
    let err = db
        .execute(&format!(
            "SELECT * FROM iceberg_scan('{}')",
            table("equality")
        ))
        .unwrap_err()
        .to_string();
    // The limit is named, with the file that would have to be applied.
    assert!(err.contains("equality delete"), "{err}");
    assert!(err.contains("data/eq-delete.parquet"), "{err}");

    // DuckDB does apply it, which is what the refusal is standing in for.
    if let Some(binary) = duckdb() {
        let rows = duckdb_lines(
            binary,
            &fixtures().join("equality"),
            "SELECT * FROM iceberg_scan('{}') ORDER BY 1",
        );
        assert_eq!(rows, ["1|ann"]);
    }
}

// ---------------------------------------------------------------------------
// The DuckDB oracle
// ---------------------------------------------------------------------------

fn duckdb() -> Option<&'static str> {
    Path::new(DUCKDB).is_file().then_some(DUCKDB)
}

/// Run `query` (with `{}` standing for the table path) in DuckDB with the
/// table directory as the working directory — DuckDB resolves the metadata's
/// relative object paths against the process directory, not the table.
fn duckdb_lines(binary: &str, dir: &Path, query: &str) -> Vec<String> {
    let sql = format!("LOAD iceberg; {}", query.replace("{}", "."));
    let out = Command::new(binary)
        .current_dir(dir)
        .args(["-csv", "-separator", "|", "-noheader", "-c", &sql])
        .output()
        .expect("run duckdb");
    assert!(
        out.status.success(),
        "duckdb failed on {sql}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

#[test]
fn the_duckdb_oracle_agrees() {
    let Some(binary) = duckdb() else {
        eprintln!("note: skipping the DuckDB oracle — {DUCKDB} is absent");
        return;
    };
    // Every query is ordered, so the two engines' row order is comparable.
    let cases: &[(&str, &str)] = &[
        ("simple", "SELECT * FROM iceberg_scan('{}') ORDER BY 1"),
        ("simple", "SELECT count(*) FROM iceberg_scan('{}')"),
        (
            "simple",
            "SELECT * FROM iceberg_scan('{}', snapshot_from_id := 1001) ORDER BY 1",
        ),
        (
            "simple",
            "SELECT * FROM iceberg_scan('{}', snapshot_from_timestamp := \
             TIMESTAMP '2023-11-14 22:13:30') ORDER BY 1",
        ),
        (
            "simple",
            "SELECT sequence_number, snapshot_id, parent_id, operation \
             FROM iceberg_snapshots('{}') ORDER BY 2",
        ),
        (
            "simple",
            "SELECT manifest_path, manifest_sequence_number, manifest_content, status, \
             content, file_path, file_format, record_count, data_sequence_number, \
             file_sequence_number FROM iceberg_metadata('{}') ORDER BY 6",
        ),
        ("deletes", "SELECT * FROM iceberg_scan('{}') ORDER BY 1"),
        ("deletes", "SELECT count(*) FROM iceberg_scan('{}')"),
        ("seqorder", "SELECT * FROM iceberg_scan('{}') ORDER BY 1"),
        ("evolution", "SELECT * FROM iceberg_scan('{}') ORDER BY 1"),
        ("partitioned", "SELECT * FROM iceberg_scan('{}') ORDER BY 1"),
        (
            "partitioned",
            "SELECT * FROM iceberg_scan('{}') WHERE region = 'eu' ORDER BY 1",
        ),
    ];
    let mut db = store();
    let mut compared = 0;
    for (name, query) in cases {
        let dir = fixtures().join(name);
        let expected = duckdb_lines(binary, &dir, query);
        let ours = lines(&mut db, &query.replace("{}", &table(name)));
        assert_eq!(ours, expected, "{name}: {query}");
        compared += 1;
    }
    assert_eq!(compared, cases.len());
}

// ---------------------------------------------------------------------------
// Remote tables over the shared transport (`remote` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "remote")]
#[path = "../../../crates/pink-lake/src/mock_s3.rs"]
mod mock_s3;

#[cfg(feature = "remote")]
mod remote {
    use std::sync::{Mutex, MutexGuard};

    use super::mock_s3;
    use super::{lines, store};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Serialize the tests that set process-global `AWS_*` configuration.
    struct EnvGuard(#[allow(dead_code)] MutexGuard<'static, ()>);

    impl EnvGuard {
        fn s3(mock: &mock_s3::MockS3) -> EnvGuard {
            let guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            // SAFETY: the lock serializes every env-mutating test here.
            unsafe {
                std::env::set_var("AWS_ACCESS_KEY_ID", mock.access_key_id());
                std::env::set_var("AWS_SECRET_ACCESS_KEY", mock.secret_access_key());
                std::env::set_var("AWS_REGION", "auto");
                std::env::set_var("AWS_ENDPOINT_URL_S3", mock.endpoint());
                std::env::set_var("PINKDB_S3_ALLOW_HTTP", "1");
            }
            EnvGuard(guard)
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: see EnvGuard::s3 (the lock is still held).
            unsafe {
                for name in [
                    "AWS_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY",
                    "AWS_REGION",
                    "AWS_ENDPOINT_URL_S3",
                    "PINKDB_S3_ALLOW_HTTP",
                ] {
                    std::env::remove_var(name);
                }
            }
        }
    }

    /// Upload a fixture table under `<table>/…` in the mock's one bucket.
    fn upload(mock: &mock_s3::MockS3, name: &str) {
        let root = super::fixtures().join(name);
        let mut pending = vec![root.clone()];
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(&dir).expect("fixture dir").flatten() {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                let relative = path.strip_prefix(&root).unwrap();
                let key = format!("{name}/{}", relative.display());
                mock.put_object(&key, &std::fs::read(&path).expect("fixture file"));
            }
        }
    }

    #[test]
    fn reads_tables_from_s3_through_the_shared_transport() {
        let mock = mock_s3::MockS3::start();
        let _env = EnvGuard::s3(&mock);
        upload(&mock, "simple");
        upload(&mock, "partitioned");
        let mut db = store();

        let url = mock.url("lake", "simple");
        assert_eq!(
            lines(
                &mut db,
                &format!("SELECT * FROM iceberg_scan('{url}') ORDER BY 1")
            ),
            ["1|ann", "2|bob", "3|cy"]
        );
        // Metadata, manifest list and manifests were ranged reads, not whole
        // objects: the reader asks for the footer/last block first.
        let requests = mock.requests().len();
        assert!(requests > 0, "no request reached the mock");
        assert!(
            mock.ranges().iter().any(|range| !range.is_empty()),
            "the manifest list and the parquet footers must be ranged reads"
        );

        let url = mock.url("lake", "partitioned");
        assert_eq!(
            lines(
                &mut db,
                &format!("SELECT * FROM iceberg_scan('{url}') ORDER BY 1")
            ),
            ["1|us|1.5", "2|us|2.5", "3|eu|3.5", "4|eu|4.5"]
        );
    }

    #[test]
    fn a_remote_metadata_file_reads_and_a_remote_table_without_a_hint_is_refused() {
        let mock = mock_s3::MockS3::start();
        let _env = EnvGuard::s3(&mock);
        upload(&mock, "simple");
        let mut db = store();

        let url = format!("{}/metadata/v2.metadata.json", mock.url("lake", "simple"));
        assert_eq!(
            lines(
                &mut db,
                &format!("SELECT * FROM iceberg_scan('{url}') ORDER BY 1")
            ),
            ["1|ann", "2|bob", "3|cy"]
        );

        mock.put_object("nohint/metadata/v1.metadata.json", b"{}");
        let url = mock.url("lake", "nohint");
        let err = db
            .execute(&format!("SELECT * FROM iceberg_scan('{url}')"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("version-hint.text"), "{err}");
    }
}
