//! The `.pink` lake catalog (wave 103, T1b): the shared catalog suite run
//! against a durable `pink:<path>` database, plus the backend's own
//! reopen/crash and backend-equivalence checks.
//!
//! The `.pink` backend needs no feature: pinkdb is already a pink-lake
//! dependency. Every metadata read goes through the same DuckLake rows and
//! the same SQL core as the SQLite backend; the assertions here are about
//! durability, transactions and equivalence, not a second copy of the deeper
//! lake suites.
#[path = "common/local_lake.rs"]
mod local_lake;

#[cfg(feature = "s3")]
#[path = "../src/mock_s3.rs"]
mod mock_s3;

// The shared SQLite/Postgres/pink suite. Included directly (not through
// `common`, which other test binaries share) because it needs this binary's
// `mock_s3`.
#[path = "common/catalog_suite.rs"]
mod catalog_suite;

use catalog_suite::PinkSuite;
use pink_lake::{Lake, LakeError};

#[test]
fn pink_location_validation() {
    for location in ["pink:", "pink::memory:"] {
        let err = Lake::open_url(location).unwrap_err();
        assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
        assert!(err.to_string().contains("must persist"), "{err}");
    }
    // Opening a catalog that does not exist is `NotALake` and creates
    // nothing (a mistyped path never leaves an empty database behind).
    let dir = catalog_suite::scratch("missing-catalog");
    let missing = dir.join("typo.pink");
    let err = Lake::open_url(&format!("pink:{}", missing.display())).unwrap_err();
    assert!(matches!(err, LakeError::NotALake { .. }), "{err:?}");
    assert!(
        !missing.exists() && !missing.with_extension("pink.wal").exists(),
        "open must not create {}",
        missing.display()
    );
    catalog_suite::cleanup(&dir);

    // The data half of a split lake cannot itself be a catalog.
    let err = Lake::create_with_catalog("pink:/tmp/a.pink", "pink:/tmp/b.pink").unwrap_err();
    assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
    assert!(err.to_string().contains("data location"), "{err}");
}

/// A catalog row that no longer exists fails the open, and the error names the
/// catalog location as given (`pink:<path>`), never a JSON file.
#[test]
fn a_catalog_without_its_metadata_names_its_location() {
    let dir = catalog_suite::scratch("catalog-label");
    let path = dir.join("lake.pink");
    let catalog = format!("pink:{}", path.display());
    let lake = Lake::create_url(&catalog).unwrap();
    drop(lake);
    {
        let mut db = pinkdb::PinkDb::open_durable(&path.display().to_string()).unwrap();
        db.execute("DROP TABLE ducklake_metadata").unwrap();
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
    catalog_suite::create_save_open_round_trip(&mut PinkSuite::new());
}

#[test]
fn two_handles_conflict_then_rebase() {
    catalog_suite::two_handles_conflict_then_rebase(&mut PinkSuite::new());
}

#[test]
fn eight_threads_publish_contiguous_versions() {
    catalog_suite::eight_threads_publish_contiguous_versions(&mut PinkSuite::new());
}

#[test]
fn time_travel_reads_older_versions() {
    catalog_suite::time_travel_reads_older_versions(&mut PinkSuite::new());
}

#[test]
fn expire_and_cleanup_on_a_split_lake() {
    catalog_suite::expire_and_cleanup_on_a_split_lake(&mut PinkSuite::new());
}

#[test]
fn cdc_reports_the_committed_rows() {
    catalog_suite::cdc_reports_the_committed_rows(&mut PinkSuite::new());
}

#[cfg(feature = "s3")]
#[test]
fn attach_and_count_issue_no_data_requests_over_s3() {
    catalog_suite::attach_and_count_issue_no_data_requests_over_s3(&mut PinkSuite::new());
}

#[test]
fn replication_state_round_trips() {
    catalog_suite::replication_state_round_trips(&mut PinkSuite::new());
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_catalog_rows_are_sealed() {
    catalog_suite::encrypted_catalog_rows_are_sealed(&mut PinkSuite::new());
}

#[test]
fn a_failure_after_the_snapshot_insert_rolls_back_the_publication() {
    catalog_suite::a_failure_after_the_snapshot_insert_rolls_back_the_publication(
        &mut PinkSuite::new(),
    );
}

#[test]
fn data_location_ownership_is_enforced() {
    catalog_suite::data_location_ownership_is_enforced(&mut PinkSuite::new());
}

#[cfg(feature = "encryption")]
#[test]
fn flush_moves_inlined_rows_to_a_data_file() {
    catalog_suite::flush_moves_inlined_rows_to_a_data_file(&mut PinkSuite::new());
}

#[test]
fn merge_adjacent_files_reduces_the_file_count() {
    catalog_suite::merge_adjacent_files_reduces_the_file_count(&mut PinkSuite::new());
}

#[test]
fn rewrite_with_deletes_drops_the_delete_files() {
    catalog_suite::rewrite_with_deletes_drops_the_delete_files(&mut PinkSuite::new());
}

#[test]
fn add_data_files_registers_an_existing_file() {
    catalog_suite::add_data_files_registers_an_existing_file(&mut PinkSuite::new());
}

#[test]
fn partition_and_inlining_settings_publish() {
    catalog_suite::partition_and_inlining_settings_publish(&mut PinkSuite::new());
}

/// A committed catalog survives dropping the handle and reopening: the head,
/// its history and a readable table come back from the database alone. (The
/// suite's create/save/open test covers the same path with more metadata
/// assertions; this one exists to pin the `.pink` file/WAL reopen rule.)
#[test]
fn reopen_after_drop_keeps_the_head() {
    use std::sync::Arc;

    use pinkdb::{DataTable, DataType, PinkDb, QueryResult, Value};

    let dir = catalog_suite::scratch("reopen");
    let catalog = format!("pink:{}", dir.join("catalog.pink").display());
    let data = dir.join("data-root");
    {
        let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE main.t (i INTEGER, s VARCHAR);")
            .unwrap();
        db.table_mut_in("main", "t")
            .unwrap()
            .append_row(vec![Value::Integer(1), Value::Varchar("a".into())])
            .unwrap();
        assert_eq!(lake.write_from_store(&db).unwrap(), 2);

        let mut table = DataTable::new(
            "t".to_string(),
            vec![
                ("i".to_string(), DataType::Integer),
                ("s".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        table
            .append_row(vec![Value::Integer(2), Value::Varchar("b".into())])
            .unwrap();
        let mut commit = pink_lake::Commit::new();
        commit.append("main.t", Arc::new(table));
        assert_eq!(lake.commit(&commit).unwrap().version, 3);
    }

    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(lake.current_version(), 3);
    assert_eq!(lake.versions().unwrap(), vec![1, 2, 3]);
    let mut db = lake.load(None).unwrap();
    let QueryResult::Selected { rows, .. } =
        db.execute("SELECT count(*) FROM t").unwrap().pop().unwrap()
    else {
        panic!("expected a count row");
    };
    let count = match &rows[0][0] {
        Value::Integer(v) => i64::from(*v),
        Value::Bigint(v) => *v,
        other => panic!("expected an integer count, got {other:?}"),
    };
    assert_eq!(count, 2);
    catalog_suite::cleanup(&dir);
}

/// Backend equivalence (`catalog-sqlite`): the same operation sequence on a
/// `sqlite:` and a `pink:` catalog produces the same logical catalog at every
/// version, the same snapshot list and the same change data. Volatile facts
/// (publish time, lake/table identity, data-root paths) are normalized; the
/// rows, files, statistics and deltas are compared exactly.
/// The race writer's environment variable: `<catalog>|<commits>|<first value>`.
/// When set, [`race_writer_child`] is one racing writer process; otherwise it
/// is a no-op, so the ordinary test run skips it.
const RACE_WRITER_ENV: &str = "PINK_LAKE_RACE_WRITER";

/// One racing writer, run as a child process by
/// [`four_writers_race_on_one_pink_catalog`] (this test binary re-executed
/// with `--exact race_writer_child`). It uses the typed `Commit` path, the one
/// with DuckLake's automatic retry/rebase loop, and prints one
/// `COMMITTED <version> <retries>` line per landed commit.
#[test]
fn race_writer_child() {
    use std::sync::Arc;

    use pink_lake::{Commit, CommitSettings};
    use pinkdb::{DataTable, DataType, Value};

    let Ok(spec) = std::env::var(RACE_WRITER_ENV) else {
        return;
    };
    let mut fields = spec.split('|');
    let catalog = fields.next().unwrap();
    let commits: i32 = fields.next().unwrap().parse().unwrap();
    let first: i32 = fields.next().unwrap().parse().unwrap();
    let mut lake = Lake::open_with_catalog(catalog, None).unwrap();
    lake.set_data_inlining_row_limit(0);
    let settings = CommitSettings {
        max_retry_count: 100,
        retry_backoff: 1.0,
        retry_wait_ms: 1,
    };
    for i in 0..commits {
        let mut table =
            DataTable::new("t".to_string(), vec![("a".to_string(), DataType::Integer)]).unwrap();
        table.append_row(vec![Value::Integer(first + i)]).unwrap();
        let mut commit = Commit::new();
        commit.append("main.t", Arc::new(table));
        let report = lake.commit_with_settings(&commit, &settings).unwrap();
        println!("COMMITTED {} {}", report.version, report.retries);
    }
}

/// Four OS processes race one `pink:` catalog with 25 commits each, five
/// rounds.
///
/// pinkdb does not enforce PRIMARY KEY, so snapshot uniqueness rests on the
/// engine's COMMIT-time read/write-set validation (every catalog commit reads
/// and writes `ducklake_snapshot`). The snapshot ids must be exactly the
/// contiguous range, no commit may be lost, and every writer must land
/// directly or through the retryable conflict path.
#[test]
fn four_writers_race_on_one_pink_catalog() {
    use std::process::{Command, Stdio};

    use pinkdb::{PinkDb, QueryResult, Value};

    fn integers(db: &mut PinkDb, sql: &str) -> Vec<i64> {
        let QueryResult::Selected { rows, .. } = db.execute(sql).unwrap().pop().unwrap() else {
            panic!("expected rows from {sql}");
        };
        rows.into_iter()
            .map(|row| match &row[0] {
                Value::Integer(v) => i64::from(*v),
                Value::Bigint(v) => *v,
                other => panic!("expected an integer, got {other:?}"),
            })
            .collect()
    }

    let exe = std::env::current_exe().unwrap();
    let mut total_retries = 0u64;
    for round in 0..5 {
        let dir = catalog_suite::scratch(&format!("race{round}"));
        let catalog_path = dir.join("catalog.pink");
        let catalog = format!("pink:{}", catalog_path.display());
        {
            let mut lake =
                Lake::create_with_catalog(&catalog, dir.join("data").to_str().unwrap()).unwrap();
            let mut db = PinkDb::new();
            db.execute("CREATE TABLE main.t (a INTEGER); INSERT INTO main.t VALUES (-1);")
                .unwrap();
            assert_eq!(lake.write_from_store(&db).unwrap(), 2);
        }

        let children: Vec<_> = (0..4)
            .map(|writer| {
                Command::new(&exe)
                    .args(["race_writer_child", "--exact", "--nocapture"])
                    .env(RACE_WRITER_ENV, format!("{catalog}|25|{}", writer * 25))
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .expect("spawn race writer")
            })
            .collect();
        let mut landed = 0usize;
        let mut retries = 0u64;
        for (writer, child) in children.into_iter().enumerate() {
            let output = child.wait_with_output().expect("wait for race writer");
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success(),
                "race writer {writer} failed: {stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            for rest in stdout.lines().filter_map(|l| l.strip_prefix("COMMITTED ")) {
                let mut fields = rest.split_whitespace();
                fields.next().expect("version");
                retries += fields.next().expect("retries").parse::<u64>().unwrap();
                landed += 1;
            }
        }
        assert_eq!(landed, 100, "round {round}: every commit must land");
        total_retries += retries;

        {
            let lake = Lake::open_with_catalog(&catalog, None).unwrap();
            assert_eq!(lake.current_version(), 102, "round {round}");
            let mut db = lake.load(None).unwrap();
            assert_eq!(integers(&mut db, "SELECT count(*) FROM t"), vec![101]);
        }
        let mut db = PinkDb::open_durable(catalog_path.to_str().unwrap()).unwrap();
        assert_eq!(
            integers(
                &mut db,
                "SELECT snapshot_id FROM ducklake_snapshot ORDER BY snapshot_id"
            ),
            (0..102).collect::<Vec<_>>(),
            "round {round}: snapshot ids must be contiguous with no duplicates"
        );
        drop(db);
        println!("race round {round}: 100 commits landed, {retries} retries");
        catalog_suite::cleanup(&dir);
    }
    println!("race total: 500 commits landed, {total_retries} retries");
}

#[cfg(feature = "catalog-sqlite")]
#[test]
fn sqlite_and_pink_catalogs_are_equivalent() {
    use std::sync::Arc;

    use pink_lake::{Commit, CommitSettings, WritableAttachment};
    use pinkdb::{DataTable, DataType, PinkDb, Value};

    fn initial_store() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE main.t (i INTEGER, s VARCHAR);")
            .unwrap();
        for (i, s) in [(100, "a"), (101, "b"), (102, "c")] {
            db.table_mut_in("main", "t")
                .unwrap()
                .append_row(vec![Value::Integer(i), Value::Varchar(s.into())])
                .unwrap();
        }
        db
    }

    fn sub_table(name: &str, rows: &[(i32, &str)]) -> Arc<DataTable> {
        let mut table = DataTable::new(
            name.to_string(),
            vec![
                ("i".to_string(), DataType::Integer),
                ("s".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        for (i, s) in rows {
            table
                .append_row(vec![Value::Integer(*i), Value::Varchar(s.to_string())])
                .unwrap();
        }
        Arc::new(table)
    }

    /// Materialize the writable attachment's tables (the same helper the
    /// schema-evolution suite uses).
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
            for (column, source) in target.columns.iter_mut().zip(&base.columns) {
                column.default = source.default.clone();
            }
            for chunk in 0..base.num_chunks() {
                let chunk = base.scan_chunk(chunk).unwrap();
                target.append_shared_chunk(&chunk.columns).unwrap();
            }
        }
        db
    }

    fn build(backend: &str, dir: &std::path::Path) -> (String, std::path::PathBuf) {
        let catalog = format!("{backend}:{}", dir.join("catalog").display());
        let data = dir.join("data-root");
        let mut lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
        lake.set_data_inlining_row_limit(0);
        assert_eq!(lake.write_from_store(&initial_store()).unwrap(), 2);

        // v3: an inlined row.
        lake.set_data_inlining_row_limit(10);
        let mut commit = Commit::new();
        commit.append("main.t", sub_table("t", &[(200, "inlined")]));
        assert_eq!(lake.commit(&commit).unwrap().version, 3);
        drop(lake);

        // v4: delete an existing (inlined) row through the writable attach.
        let mut wa = WritableAttachment::open(&catalog, None).unwrap();
        let mut db = materialize(&wa);
        db.execute("DELETE FROM t WHERE i = 200;").unwrap();
        assert_eq!(
            wa.commit(&db, &CommitSettings::default()).unwrap().version,
            4
        );
        drop(wa);

        // v5: a schema change.
        let mut wa = WritableAttachment::open(&catalog, None).unwrap();
        let mut db = materialize(&wa);
        db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 7;")
            .unwrap();
        assert_eq!(
            wa.commit(&db, &CommitSettings::default()).unwrap().version,
            5
        );
        drop(wa);

        // v6: a second table.
        let mut lake = Lake::open_with_catalog(&catalog, None).unwrap();
        let mut commit = Commit::new();
        commit.append("main.u", sub_table("u", &[(1, "u1"), (2, "u2")]));
        assert_eq!(lake.commit(&commit).unwrap().version, 6);
        drop(lake);
        (catalog, data)
    }

    /// Normalize the facts that legitimately differ between two catalogs:
    /// publish times, random lake/table identities, and the data-root prefix
    /// embedded in file paths.
    fn normalize(value: &mut serde_json::Value, root: &std::path::Path) {
        let root = root.to_string_lossy().to_string();
        match value {
            serde_json::Value::Object(map) => {
                for volatile in ["updated", "lake_id", "table_id"] {
                    map.remove(volatile);
                }
                for (key, child) in map.iter_mut() {
                    if key == "time" || key == "time_micros" {
                        *child = serde_json::Value::Null;
                    } else {
                        normalize_strings(child, &root);
                    }
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    normalize_strings(item, &root);
                }
            }
            _ => normalize_strings(value, &root),
        }
    }

    fn normalize_strings(value: &mut serde_json::Value, root: &str) {
        match value {
            serde_json::Value::String(text) => {
                if text.contains(root) {
                    *text = text.replace(root, "<data>");
                }
                // Data-file names are freshly generated ids, and a stats
                // block's CRC covers its path; keep the directory, normalize
                // the file name.
                if text.starts_with("data/")
                    && let Some((dir, _)) = text.rsplit_once('/')
                {
                    *text = format!("{dir}/<file>");
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    normalize_strings(item, root);
                }
            }
            serde_json::Value::Object(map) => {
                for (key, child) in map.iter_mut() {
                    if key == "updated"
                        || key == "lake_id"
                        || key == "table_id"
                        || key == "time"
                        || key == "time_micros"
                        || key == "crc32"
                    {
                        *child = serde_json::Value::Null;
                    } else {
                        normalize_strings(child, root);
                    }
                }
            }
            _ => {}
        }
    }

    let dir = catalog_suite::scratch("equivalence");
    let sqlite_dir = dir.join("sqlite");
    let pink_dir = dir.join("pink");
    std::fs::create_dir_all(&sqlite_dir).unwrap();
    std::fs::create_dir_all(&pink_dir).unwrap();
    let (sqlite_catalog, sqlite_data) = build("sqlite", &sqlite_dir);
    let (pink_catalog, pink_data) = build("pink", &pink_dir);

    let sqlite = Lake::open_with_catalog(&sqlite_catalog, None).unwrap();
    let pink = Lake::open_with_catalog(&pink_catalog, None).unwrap();
    assert_eq!(sqlite.current_version(), 6);
    assert_eq!(pink.current_version(), 6);

    for version in 1..=6u64 {
        let a = pink_lake::catalog_snapshot_json(&sqlite_catalog, version)
            .unwrap()
            .unwrap();
        let b = pink_lake::catalog_snapshot_json(&pink_catalog, version)
            .unwrap()
            .unwrap();
        let mut a = a;
        let mut b = b;
        normalize(&mut a, &sqlite_data);
        normalize(&mut b, &pink_data);
        assert_eq!(a, b, "catalog at version {version}");
    }

    let mut a = sqlite.snapshots().unwrap();
    let mut b = pink.snapshots().unwrap();
    for snapshot in a.iter_mut().chain(b.iter_mut()) {
        snapshot.time.clear();
        snapshot.time_micros = None;
    }
    assert_eq!(a, b, "snapshots()");

    assert_eq!(
        sqlite.table_changes("main.t", 2, None).unwrap(),
        pink.table_changes("main.t", 2, None).unwrap(),
        "changes(main.t, 2, head)"
    );
    assert_eq!(
        sqlite.table_changes("main.u", 2, None).unwrap(),
        pink.table_changes("main.u", 2, None).unwrap(),
        "changes(main.u, 2, head)"
    );

    catalog_suite::cleanup(&dir);
}

/// A `.pink` catalog keeps its WAL small: every process that opens the
/// catalog replays the WAL, so commits must trigger checkpoints long before
/// the engine's 64 MiB default. After 300 single-row commits (inlining off,
/// one data file each) the WAL stays near the 1 MiB catalog threshold and the
/// checkpointed main file holds the catalog.
#[test]
fn the_catalog_wal_is_checkpointed_as_commits_accumulate() {
    use std::sync::Arc;

    use pink_lake::Commit;
    use pinkdb::{DataTable, DataType, PinkDb, Value};

    let dir = catalog_suite::scratch("wal-bound");
    let catalog_path = dir.join("catalog.pink");
    let catalog = format!("pink:{}", catalog_path.display());
    let mut lake = Lake::create_with_catalog(&catalog, dir.join("data").to_str().unwrap()).unwrap();
    lake.set_data_inlining_row_limit(0);
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE main.t (i INTEGER, s VARCHAR);")
        .unwrap();
    db.table_mut_in("main", "t")
        .unwrap()
        .append_row(vec![Value::Integer(0), Value::Varchar("v0".into())])
        .unwrap();
    assert_eq!(lake.write_from_store(&db).unwrap(), 2);
    for i in 1..=300 {
        let mut table = DataTable::new(
            "t".to_string(),
            vec![
                ("i".to_string(), DataType::Integer),
                ("s".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        table
            .append_row(vec![Value::Integer(i), Value::Varchar(format!("v{i}"))])
            .unwrap();
        let mut commit = Commit::new();
        commit.append("main.t", Arc::new(table));
        lake.commit(&commit).unwrap();
    }
    drop(lake);
    let wal = std::fs::metadata(format!("{}.wal", catalog_path.display()))
        .map(|m| m.len())
        .unwrap_or(0);
    // One threshold plus at most one commit's worth of log past it.
    assert!(
        wal < 2 * 1024 * 1024,
        "the catalog WAL grew to {wal} bytes; it must be checkpointed near 1 MiB"
    );
    assert!(
        std::fs::metadata(&catalog_path)
            .map(|m| m.len())
            .unwrap_or(0)
            > 0,
        "the checkpointed catalog lives in the main file"
    );
    let lake = Lake::open_with_catalog(&catalog, None).unwrap();
    assert_eq!(lake.current_version(), 302);
    catalog_suite::cleanup(&dir);
}
