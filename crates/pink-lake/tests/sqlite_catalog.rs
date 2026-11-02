//! The SQLite catalog (`catalog-sqlite`, wave 96a): the shared catalog suite
//! (`common/catalog_suite.rs`) run against `sqlite:<path>`, plus the
//! SQLite-only location validation.
//!
//! Every metadata read here goes through the SQL rows; the data location keeps
//! the ordinary local/S3 paths, so the assertions are about routing,
//! transactions and bytes — not a second copy of the deeper lake suites.

#![cfg(feature = "catalog-sqlite")]
#[path = "common/local_lake.rs"]
mod local_lake;

#[cfg(feature = "s3")]
#[path = "../src/mock_s3.rs"]
mod mock_s3;

// The shared SQLite/Postgres suite. Included directly (not through `common`,
// which other test binaries share) because it needs this binary's `mock_s3`;
// each backend binary uses only part of it.
#[path = "common/catalog_suite.rs"]
mod catalog_suite;

use catalog_suite::SqliteSuite;
use pink_lake::{Lake, LakeError};

/// `sqlite::memory:` and an empty path cannot back a persistent lake.
#[test]
fn sqlite_location_validation() {
    for location in ["sqlite:", "sqlite::memory:"] {
        let err = Lake::open_url(location).unwrap_err();
        assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
        assert!(err.to_string().contains("must persist"), "{err}");
    }
    // Opening a catalog that does not exist is `NotALake` and creates
    // nothing (a mistyped path never leaves an empty database behind).
    let dir = catalog_suite::scratch("missing-catalog");
    let missing = dir.join("typo.db");
    let err = Lake::open_url(&format!("sqlite:{}", missing.display())).unwrap_err();
    assert!(matches!(err, LakeError::NotALake { .. }), "{err:?}");
    assert!(
        !missing.exists(),
        "open must not create {}",
        missing.display()
    );
    catalog_suite::cleanup(&dir);

    // The data half of a split lake cannot itself be a catalog.
    let err = Lake::create_with_catalog("sqlite:/tmp/a.db", "sqlite:/tmp/b.db").unwrap_err();
    assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
    assert!(err.to_string().contains("data location"), "{err}");
}

/// A catalog row that no longer exists fails the open, and the error names the
/// catalog location as given (`sqlite:<path>`), never a JSON file.
#[test]
fn a_catalog_without_its_metadata_names_its_location() {
    use rusqlite::Connection;

    let dir = catalog_suite::scratch("catalog-label");
    let path = dir.join("lake.db");
    let catalog = format!("sqlite:{}", path.display());
    let lake = Lake::create_url(&catalog).unwrap();
    drop(lake);
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("DROP TABLE ducklake_metadata;").unwrap();
    }

    let err = Lake::open_url(&catalog).unwrap_err();
    assert!(matches!(err, LakeError::NotALake { .. }), "{err:?}");
    let message = err.to_string();
    assert!(
        message.contains(&catalog),
        "the error must name {catalog}: {message}"
    );
    // The deleted JSON layout's file name must not appear in a catalog error.
    assert!(!message.contains(concat!("catalog", ".json")), "{message}");
    catalog_suite::cleanup(&dir);
}

#[test]
fn create_save_open_round_trip() {
    catalog_suite::create_save_open_round_trip(&mut SqliteSuite::new());
}

#[test]
fn two_handles_conflict_then_rebase() {
    catalog_suite::two_handles_conflict_then_rebase(&mut SqliteSuite::new());
}

#[test]
fn eight_threads_publish_contiguous_versions() {
    catalog_suite::eight_threads_publish_contiguous_versions(&mut SqliteSuite::new());
}

#[test]
fn time_travel_reads_older_versions() {
    catalog_suite::time_travel_reads_older_versions(&mut SqliteSuite::new());
}

#[test]
fn expire_and_cleanup_on_a_split_lake() {
    catalog_suite::expire_and_cleanup_on_a_split_lake(&mut SqliteSuite::new());
}

#[test]
fn cdc_reports_the_committed_rows() {
    catalog_suite::cdc_reports_the_committed_rows(&mut SqliteSuite::new());
}

#[cfg(feature = "s3")]
#[test]
fn attach_and_count_issue_no_data_requests_over_s3() {
    catalog_suite::attach_and_count_issue_no_data_requests_over_s3(&mut SqliteSuite::new());
}

#[test]
fn replication_state_round_trips() {
    catalog_suite::replication_state_round_trips(&mut SqliteSuite::new());
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_catalog_rows_are_sealed() {
    catalog_suite::encrypted_catalog_rows_are_sealed(&mut SqliteSuite::new());
}

#[test]
fn a_failure_after_the_snapshot_insert_rolls_back_the_publication() {
    catalog_suite::a_failure_after_the_snapshot_insert_rolls_back_the_publication(
        &mut SqliteSuite::new(),
    );
}

#[test]
fn data_location_ownership_is_enforced() {
    catalog_suite::data_location_ownership_is_enforced(&mut SqliteSuite::new());
}

#[cfg(feature = "encryption")]
#[test]
fn flush_moves_inlined_rows_to_a_data_file() {
    catalog_suite::flush_moves_inlined_rows_to_a_data_file(&mut SqliteSuite::new());
}

#[test]
fn merge_adjacent_files_reduces_the_file_count() {
    catalog_suite::merge_adjacent_files_reduces_the_file_count(&mut SqliteSuite::new());
}

#[test]
fn rewrite_with_deletes_drops_the_delete_files() {
    catalog_suite::rewrite_with_deletes_drops_the_delete_files(&mut SqliteSuite::new());
}

#[test]
fn add_data_files_registers_an_existing_file() {
    catalog_suite::add_data_files_registers_an_existing_file(&mut SqliteSuite::new());
}

#[test]
fn partition_and_inlining_settings_publish() {
    catalog_suite::partition_and_inlining_settings_publish(&mut SqliteSuite::new());
}

/// Wave 103 T1a: a commit writes its delta, never the catalog.
///
/// 300 one-row appends (inlining off) produce one `ducklake_data_file` row
/// per commit, no ended rows and no rewritten file rows; the head's
/// `count(*)` reads them all back. This is the storage-growth gate: the
/// catalog grows linearly with files, never files × commits, and a late
/// commit touches exactly the rows an early one did.
#[test]
fn three_hundred_single_row_commits_write_only_the_delta() {
    use std::sync::Arc;

    use pink_lake::{Commit, Lake};
    use pinkdb::{DataTable, DataType, PinkDb, QueryResult, Value};
    use rusqlite::Connection;

    const COMMITS: usize = 300;

    let dir = catalog_suite::scratch("scaling");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data-root");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_data_inlining_row_limit(0);

    let mut initial = PinkDb::new();
    initial
        .execute("CREATE TABLE main.t (i INTEGER, s VARCHAR);")
        .unwrap();
    initial
        .table_mut_in("main", "t")
        .unwrap()
        .append_row(vec![Value::Integer(0), Value::Varchar("v0".to_string())])
        .unwrap();
    assert_eq!(lake.write_from_store(&initial).unwrap(), 2);

    let mut rows_at_ten = 0u64;
    let mut rows_at_last = 0u64;
    for i in 0..COMMITS {
        let mut table = DataTable::new(
            "t".to_string(),
            vec![
                ("i".to_string(), DataType::Integer),
                ("s".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        table
            .append_row(vec![
                Value::Integer(1 + i as i32),
                Value::Varchar(format!("v{}", i + 1)),
            ])
            .unwrap();
        let mut commit = Commit::new();
        commit.append("main.t", Arc::new(table));
        let report = lake.commit(&commit).unwrap();
        assert_eq!(report.version, 3 + i as u64);
        if i == 9 {
            rows_at_ten = pink_lake::sqlite_last_commit_rows_touched();
        }
        if i == COMMITS - 1 {
            rows_at_last = pink_lake::sqlite_last_commit_rows_touched();
        }
    }
    assert!(rows_at_ten > 0, "the diagnostic must observe the commit");
    assert_eq!(
        rows_at_last, rows_at_ten,
        "commit #300 must touch the same catalog rows as commit #10"
    );
    drop(lake);

    // Reopen from the catalog alone and count the visible rows.
    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    let mut db = lake.load(None).unwrap();
    let result = db.execute("SELECT count(*) FROM t").unwrap().pop().unwrap();
    let QueryResult::Selected { rows, .. } = result else {
        panic!("expected a count row");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        match &rows[0][0] {
            Value::Integer(v) => i64::from(*v),
            Value::Bigint(v) => *v,
            other => panic!("expected an integer count, got {other:?}"),
        },
        1 + COMMITS as i64
    );

    // The physical catalog: exactly one data-file row per commit (plus the
    // initial save), none rewritten and none ended. `ducklake_snapshot` has
    // snapshot 0 (the create), the initial save and one row per commit.
    let conn = Connection::open(&catalog_path).unwrap();
    // Measure the database after folding the WAL back in; the benchmark does
    // the same so a live WAL does not make the catalog look larger.
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    let data_file_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM ducklake_data_file", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(data_file_rows as usize, COMMITS + 1);
    let ended_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM ducklake_data_file WHERE end_snapshot IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(ended_rows, 0, "a commit must never rewrite a file row");
    let snapshot_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM ducklake_snapshot", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(snapshot_rows as usize, COMMITS + 2);
    let recorded_rows: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(record_count), 0) FROM ducklake_data_file",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(recorded_rows, 1 + COMMITS as i64);
    let catalog_size = std::fs::metadata(&catalog_path).unwrap().len();
    assert!(
        catalog_size <= 2 * 1024 * 1024,
        "300 delta commits must stay under 2 MB (got {catalog_size} bytes)"
    );
    let bytes_per_file = catalog_size as f64 / data_file_rows as f64;
    assert!(
        bytes_per_file < 1536.0,
        "the catalog must stay under 1.5 KB per data file after 300 commits \
         (got {bytes_per_file:.1} bytes/file, {catalog_size} bytes for {data_file_rows} files)"
    );

    // The `pinkdb_*` side tables may carry typed facts, never a serialized
    // statistics block, commit manifest or inlined values. Inlined values live
    // only in DuckLake's typed tables; no JSON column may exist anywhere.
    let mut table_stmt = conn
        .prepare(
            "SELECT name FROM sqlite_master
             WHERE type = 'table' AND name LIKE 'pinkdb\\_%' ESCAPE '\\'
             ORDER BY name",
        )
        .unwrap();
    let pink_tables: Vec<String> = table_stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert!(
        !pink_tables.iter().any(|name| name == "pinkdb_commit"),
        "row-level changes come from the DuckLake rows, so pinkdb_commit is gone"
    );
    assert!(
        !pink_tables.iter().any(|name| name == "pinkdb_inlined"),
        "inlined values live only in the typed DuckLake inlined tables"
    );
    for table in &pink_tables {
        let mut info = conn
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .unwrap();
        let columns: Vec<String> = info
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        for column in columns {
            for forbidden in [
                "stats_json",
                "changes_json",
                "partition_by_json",
                "entry_stats_json",
                "manifest_json",
                "columns_json",
                "values_json",
            ] {
                assert_ne!(
                    column, forbidden,
                    "{table} must not serialize {forbidden}; store a typed column instead"
                );
            }
            assert!(
                !column.contains("json"),
                "{table}.{column}: SQLite catalogs keep no JSON layout"
            );
        }
    }

    // The SQL delta is the same shape at commit #10 and #300: one
    // `inserted_into_table` change for table 1, no table-wide rewrite.
    let changes_at_ten: String = conn
        .query_row(
            "SELECT changes_made FROM ducklake_snapshot_changes WHERE snapshot_id = 11",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let changes_at_last: String = conn
        .query_row(
            "SELECT changes_made FROM ducklake_snapshot_changes WHERE snapshot_id = 301",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        changes_at_last, changes_at_ten,
        "commit #300 must report the same change set as commit #10"
    );

    catalog_suite::cleanup(&dir);
}

/// Wave 103 T1a follow-up: the typed DuckLake inlined tables are the only
/// copy of an inlined row's values. Every supported pink type survives
/// write → reopen → read byte-identically (canonical form), and the physical
/// SQLite types match what DuckDB itself writes.
#[test]
fn inlined_rows_round_trip_every_type_through_the_typed_ducklake_tables() {
    use std::sync::Arc;

    use pink_lake::{Commit, Lake};
    use pinkdb::{DataTable, DataType, PinkDb, QueryResult, Value};
    use rusqlite::Connection;

    let dir = catalog_suite::scratch("inlined-round-trip");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data-root");
    let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    lake.set_data_inlining_row_limit(10);

    // Save just the schema, then append rows small enough to inline.
    let mut empty = PinkDb::new();
    empty
        .execute(
            "CREATE TABLE main.t (
                 b BOOLEAN, i INTEGER, l BIGINT, d DOUBLE, v VARCHAR,
                 dt DATE, ts TIMESTAMP, dec DECIMAL(18,3), iv INTERVAL
             );",
        )
        .unwrap();
    lake.write_from_store(&empty).unwrap();

    let mut table = DataTable::new(
        "t".to_string(),
        vec![
            ("b".to_string(), DataType::Boolean),
            ("i".to_string(), DataType::Integer),
            ("l".to_string(), DataType::Bigint),
            ("d".to_string(), DataType::Double),
            ("v".to_string(), DataType::Varchar),
            ("dt".to_string(), DataType::Date),
            ("ts".to_string(), DataType::Timestamp),
            (
                "dec".to_string(),
                DataType::Decimal {
                    precision: 18,
                    scale: 3,
                },
            ),
            ("iv".to_string(), DataType::Interval),
        ],
    )
    .unwrap();
    let rows = vec![
        vec![
            Value::Boolean(true),
            Value::Integer(i32::MIN),
            Value::Bigint(i64::MIN),
            Value::Double(-0.0),
            Value::Varchar("héllo 🌸 Δ".to_string()),
            Value::Date(i32::MIN),
            Value::Timestamp(i64::MIN),
            Value::decimal(-999_999_999_999_999_999, 3),
            Value::Interval {
                months: -13,
                days: -2,
                micros: -3,
            },
        ],
        vec![
            Value::Boolean(false),
            Value::Integer(i32::MAX),
            Value::Bigint(i64::MAX),
            Value::Double(f64::NAN),
            Value::Varchar(String::new()),
            Value::Date(i32::MAX),
            Value::Timestamp(i64::MAX),
            Value::decimal(999_999_999_999_999_999, 3),
            Value::Interval {
                months: 0,
                days: 0,
                micros: 0,
            },
        ],
        vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ],
        vec![
            Value::Boolean(true),
            Value::Integer(-1),
            Value::Bigint(-1),
            Value::Double(f64::INFINITY),
            Value::Varchar("plain".to_string()),
            Value::Date(0),
            Value::Timestamp(0),
            Value::decimal(0, 3),
            Value::Interval {
                months: 14,
                days: 3,
                micros: 4,
            },
        ],
        vec![
            Value::Boolean(false),
            Value::Integer(0),
            Value::Bigint(0),
            Value::Double(f64::NEG_INFINITY),
            Value::Varchar("zero".to_string()),
            Value::Date(-1),
            Value::Timestamp(-1),
            Value::decimal(1, 3),
            Value::Interval {
                months: -14,
                days: -3,
                micros: -4,
            },
        ],
    ];
    for row in &rows {
        table.append_row(row.clone()).unwrap();
    }
    let mut commit = Commit::new();
    commit.append("main.t", Arc::new(table));
    let report = lake.commit(&commit).unwrap();
    assert_eq!(report.version, 3);
    drop(lake);

    // The catalog stores the values once, typed, in DuckLake's own table.
    let conn = Connection::open(&catalog_path).unwrap();
    let (table_name, suffix): (String, i64) = conn
        .query_row(
            "SELECT table_name, schema_version FROM ducklake_inlined_data_tables",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(table_name, format!("ducklake_inlined_data_1_{suffix}"));
    let declared: Vec<(String, String)> = {
        let mut info = conn
            .prepare(&format!("PRAGMA table_info(\"{table_name}\")"))
            .unwrap();
        info.query_map([], |row| Ok((row.get(1)?, row.get(2)?)))
            .unwrap()
            .map(|row| row.unwrap())
            .collect()
    };
    for (column, physical) in [
        ("b", "BIGINT"),
        ("i", "BIGINT"),
        ("l", "BIGINT"),
        ("d", "VARCHAR"),
        ("v", "VARCHAR"),
        ("dt", "VARCHAR"),
        ("ts", "VARCHAR"),
        ("dec", "VARCHAR"),
        ("iv", "VARCHAR"),
    ] {
        let found = declared
            .iter()
            .find(|(name, _)| name == column)
            .unwrap_or_else(|| panic!("{column} is missing from the typed inlined table"));
        assert_eq!(
            found.1, physical,
            "DuckDB declares inlined {column} as {physical}"
        );
    }
    let side_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM pinkdb_inlined_schema", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(side_rows, 1, "one typed mapping row per schema version");
    drop(conn);

    // Reopen from the catalog alone: the read path must rebuild the exact
    // canonical values from the typed table.
    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    let mut db = lake.load(None).unwrap();
    let QueryResult::Selected { rows: read, .. } = db
        .execute("SELECT b, i, l, d, v, dt, ts, dec, iv FROM t")
        .unwrap()
        .pop()
        .unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(read.len(), rows.len());
    for (index, (expected, actual)) in rows.iter().zip(&read).enumerate() {
        for (column, (expected, actual)) in expected.iter().zip(actual).enumerate() {
            match (expected, actual) {
                (Value::Double(a), Value::Double(b)) => assert_eq!(
                    a.to_bits(),
                    b.to_bits(),
                    "row {index} column {column}: {a:?} != {b:?}"
                ),
                _ => assert_eq!(
                    expected, actual,
                    "row {index} column {column} did not round-trip"
                ),
            }
        }
    }

    catalog_suite::cleanup(&dir);
}

/// A commit whose only change is a schema change (no files, no deletes) is
/// still a DuckLake schema change: the renamed column's row ends and a new
/// row with the same column id begins at that snapshot, the snapshot's
/// `schema_version` advances with one `ducklake_schema_versions` row, and
/// `changes_made` records `altered_table:<id>`, exactly what DuckDB writes
/// for `ALTER TABLE … RENAME COLUMN`. Reopening the catalog alone shows the
/// new name.
#[test]
fn a_schema_only_commit_is_recorded_like_ducklake() {
    use pink_lake::{CommitSettings, WritableAttachment};
    use pinkdb::{PinkDb, Value};
    use rusqlite::Connection;

    fn materialize(wa: &WritableAttachment) -> PinkDb {
        let mut db = PinkDb::new();
        for (key, _) in wa.tables() {
            let (dbname, table) = key.split_once('.').unwrap();
            let base = wa.base_table(&key).unwrap();
            if !db.database_exists(dbname) {
                db.create_database(dbname).unwrap();
            }
            db.create_table_in(dbname, table, base.schema()).unwrap();
            let target = db.table_mut_in(dbname, table).unwrap();
            for chunk in 0..base.num_chunks() {
                let chunk = base.scan_chunk(chunk).unwrap().unwrap();
                target.append_shared_chunk(&chunk.columns).unwrap();
            }
        }
        db
    }

    let dir = catalog_suite::scratch("schema-only");
    let catalog_path = dir.join("catalog.db");
    let catalog = format!("sqlite:{}", catalog_path.display());
    let data = dir.join("data-root");
    {
        let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE main.t (a INTEGER, b VARCHAR);")
            .unwrap();
        db.table_mut_in("main", "t")
            .unwrap()
            .append_row(vec![Value::Integer(1), Value::Varchar("x".into())])
            .unwrap();
        assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    }
    let mut wa = WritableAttachment::open(&catalog, None).unwrap();
    let mut db = materialize(&wa);
    db.execute("ALTER TABLE t RENAME COLUMN b TO c;").unwrap();
    assert_eq!(
        wa.commit(&db, &CommitSettings::default()).unwrap().version,
        3
    );
    drop(wa);

    let conn = Connection::open(&catalog_path).unwrap();
    let columns: Vec<(i64, String, i64, Option<i64>)> = conn
        .prepare(
            "SELECT column_id, column_name, begin_snapshot, end_snapshot
             FROM ducklake_column ORDER BY begin_snapshot, column_id",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        columns,
        vec![
            (1, "a".to_string(), 1, None),
            (2, "b".to_string(), 1, Some(2)),
            (2, "c".to_string(), 2, None),
        ]
    );
    let (schema_version, changes): (i64, String) = conn
        .query_row(
            "SELECT s.schema_version, c.changes_made FROM ducklake_snapshot s
             JOIN ducklake_snapshot_changes c ON c.snapshot_id = s.snapshot_id
             WHERE s.snapshot_id = 2",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(schema_version, 2);
    assert_eq!(changes, "altered_table:1");
    let versions: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM ducklake_schema_versions WHERE begin_snapshot = 2",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(versions, 1);
    drop(conn);

    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    let mut db = lake.load(None).unwrap();
    let pinkdb::QueryResult::Selected { names, .. } =
        db.execute("SELECT * FROM t").unwrap().pop().unwrap()
    else {
        panic!("expected rows");
    };
    assert_eq!(names, vec!["a".to_string(), "c".to_string()]);
    catalog_suite::cleanup(&dir);
}
