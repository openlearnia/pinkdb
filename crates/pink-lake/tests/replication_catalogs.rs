//! Replication against database catalogs (wave 103 T2): the shared suite in
//! `common/replication_catalog_suite.rs`, run against `pink:` always and
//! `sqlite:` with the `catalog-sqlite` feature.
#[path = "common/local_lake.rs"]
mod local_lake;
use local_lake::*;

#[path = "common/replication_catalog_suite.rs"]
mod suite;

use suite::PinkSuite;

macro_rules! pink_test {
    ($name:ident) => {
        #[test]
        fn $name() {
            suite::$name(&mut PinkSuite);
        }
    };
}

pink_test!(store_source_full_and_merge);
pink_test!(snapshot_lake_source_incremental);
pink_test!(watermark_lake_source);
pink_test!(include_exclude_and_ownership);

#[cfg(feature = "catalog-sqlite")]
macro_rules! sqlite_test {
    ($name:ident, $fn_name:ident) => {
        #[test]
        fn $fn_name() {
            suite::$name(&mut suite::SqliteSuite);
        }
    };
}

#[cfg(feature = "catalog-sqlite")]
sqlite_test!(
    store_source_full_and_merge,
    store_source_full_and_merge_sqlite
);
#[cfg(feature = "catalog-sqlite")]
sqlite_test!(
    snapshot_lake_source_incremental,
    snapshot_lake_source_incremental_sqlite
);
#[cfg(feature = "catalog-sqlite")]
sqlite_test!(watermark_lake_source, watermark_lake_source_sqlite);
#[cfg(feature = "catalog-sqlite")]
sqlite_test!(
    include_exclude_and_ownership,
    include_exclude_and_ownership_sqlite
);

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pink_lake::replication::{
    self, CycleGuard, CycleOutcome, JobSpec, JobStatus, Source, Strategy, Supervisor,
};
use pink_lake::{Commit, CommitSettings, ExpireSettings, Lake, WritableAttachment};
use pinkdb::{DataTable, DataType, PinkDb, Value};

#[cfg(feature = "s3")]
#[path = "../src/mock_s3.rs"]
mod mock_s3;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_replication_t1_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// `main.t (i INTEGER, s VARCHAR)` with rows `100..100+n`.
fn store(rows: i64) -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE main.t (i INTEGER, s VARCHAR);")
        .unwrap();
    for row in 0..rows {
        db.table_mut_in("main", "t")
            .unwrap()
            .append_row(vec![
                Value::Integer(100 + row as i32),
                Value::Varchar(format!("v{row}")),
            ])
            .unwrap();
    }
    db
}

fn append_row(db: &mut PinkDb, db_name: &str, table: &str, row: Vec<Value>) {
    db.table_mut_in(db_name, table)
        .unwrap()
        .append_row(row)
        .unwrap();
}

fn data_rows(table: &DataTable) -> Vec<Vec<Value>> {
    (0..table.num_rows())
        .map(|row| {
            (0..table.num_cols())
                .map(|column| table.get(row, column).unwrap().unwrap_or(Value::Null))
                .collect()
        })
        .collect()
}

fn lake_table_rows(lake: &Lake, key: &str) -> Vec<Vec<Value>> {
    let db = lake.load(None).unwrap();
    let (db_name, table) = key.split_once('.').unwrap();
    data_rows(db.table_in(db_name, table).unwrap())
}

fn lake_table_schema(lake: &Lake, key: &str) -> Vec<(String, DataType)> {
    let db = lake.load(None).unwrap();
    let (db_name, table) = key.split_once('.').unwrap();
    db.table_in(db_name, table).unwrap().schema()
}

/// Sorted, comparable row form (row order is not part of the contract).
fn sorted(rows: &[Vec<Value>]) -> Vec<String> {
    let mut out: Vec<String> = rows.iter().map(|row| format!("{row:?}")).collect();
    out.sort();
    out
}

fn assert_same_rows(left: &[Vec<Value>], right: &[Vec<Value>], what: &str) {
    assert_eq!(
        sorted(left),
        sorted(right),
        "{what}: contents differ\nleft={left:?}\nright={right:?}"
    );
}

fn table_state(dest: &Lake, job: u64, key: &str) -> Option<replication::TableState> {
    replication::tables(dest, job)
        .unwrap()
        .into_iter()
        .find(|table| table.source == key)
}

fn job_source(dir: &Path) -> Source {
    Source::Lake {
        location: catalog_url(dir),
    }
}

fn run(dest: &mut Lake, job: u64) -> replication::CatchUpReport {
    replication::catch_up(dest, job, None, None).unwrap()
}

fn store_source_full_and_merge_strategies_body() {
    let dir = scratch("store_strategies");
    let dest_dir = dir.join("dest");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE main.t (i INTEGER, s VARCHAR);")
        .unwrap();
    for row in 0..3 {
        append_row(
            &mut db,
            "main",
            "t",
            vec![Value::Integer(100 + row), Value::Varchar(format!("v{row}"))],
        );
    }
    db.execute("CREATE TABLE main.k (id INTEGER PRIMARY KEY, v VARCHAR);")
        .unwrap();
    for id in 0..3 {
        append_row(
            &mut db,
            "main",
            "k",
            vec![Value::Integer(id), Value::Varchar(format!("k{id}"))],
        );
    }

    let mut dest = create_lake(&dest_dir).unwrap();
    let job = replication::create(
        &mut dest,
        JobSpec::new(Source::Store {
            database: "main".to_string(),
        }),
    )
    .unwrap();
    let report = replication::catch_up(&mut dest, job, Some(&db), None).unwrap();
    assert_eq!(report.tables_synced, 2);
    assert_eq!(report.tables_failed, 0);
    assert_eq!(
        table_state(&dest, job, "main.t").unwrap().strategy,
        Strategy::Full
    );
    assert_eq!(
        table_state(&dest, job, "main.k").unwrap().strategy,
        Strategy::Merge
    );
    let merge_state = table_state(&dest, job, "main.k").unwrap();
    assert_eq!(merge_state.pk_columns.as_deref(), Some("id"));
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &data_rows(db.table_in("main", "t").unwrap()),
        "full seed",
    );
    assert_same_rows(
        &lake_table_rows(&dest, "main.k"),
        &data_rows(db.table_in("main", "k").unwrap()),
        "merge seed",
    );

    // Merge: insert + update + delete; unchanged rows keep their positions.
    let before = dest.current_version();
    db.execute("INSERT INTO k VALUES (9, 'k9');").unwrap();
    db.execute("UPDATE k SET v = 'changed' WHERE id = 1;")
        .unwrap();
    db.execute("DELETE FROM k WHERE id = 0;").unwrap();
    // Full: one insert and one update (full copies source content).
    db.execute("INSERT INTO t VALUES (200, 'x');").unwrap();
    db.execute("UPDATE t SET s = 'U1' WHERE i = 101;").unwrap();
    let report = replication::catch_up(&mut dest, job, Some(&db), None).unwrap();
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_eq!(
        report.rows_written, 4,
        "merge: one insert + one update postimage; full: one insert + one update postimage"
    );
    assert!(dest.current_version() > before);
    assert_same_rows(
        &lake_table_rows(&dest, "main.k"),
        &data_rows(db.table_in("main", "k").unwrap()),
        "merge after DML",
    );
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &data_rows(db.table_in("main", "t").unwrap()),
        "full after DML",
    );

    // A no-change cycle publishes nothing.
    let version = dest.current_version();
    let report = replication::catch_up(&mut dest, job, Some(&db), None).unwrap();
    assert_eq!(dest.current_version(), version);
    assert_eq!(report.rows_written, 0);

    // A merge-only merge diff stays minimal: exactly the changed row's
    // postimage is inserted (unchanged rows keep their positions/files).
    db.execute("UPDATE k SET v = 'only' WHERE id = 2;").unwrap();
    let report = replication::catch_up(&mut dest, job, Some(&db), None).unwrap();
    assert_eq!(report.rows_written, 1, "one updated merge row");
    assert_same_rows(
        &lake_table_rows(&dest, "main.k"),
        &data_rows(db.table_in("main", "k").unwrap()),
        "merge-only change",
    );

    // Additive drift on a merge table extends in place and backfills through
    // the diff: rows whose added column already holds the default are
    // unchanged (a schema-only commit), a real change inserts one postimage.
    db.execute("ALTER TABLE k ADD COLUMN n INTEGER DEFAULT 5;")
        .unwrap();
    let report = replication::catch_up(&mut dest, job, Some(&db), None).unwrap();
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_eq!(
        report.rows_written, 0,
        "the default backfill changes no row"
    );
    assert_eq!(lake_table_schema(&dest, "main.k").len(), 3);
    db.execute("UPDATE k SET n = 6 WHERE id = 2;").unwrap();
    let report = replication::catch_up(&mut dest, job, Some(&db), None).unwrap();
    assert_eq!(report.rows_written, 1);
    assert_same_rows(
        &lake_table_rows(&dest, "main.k"),
        &data_rows(db.table_in("main", "k").unwrap()),
        "merge additive backfill",
    );

    // versions_behind is None for store sources; the job API round-trips.
    assert_eq!(replication::versions_behind(&dest, job).unwrap(), None);
    assert_eq!(replication::jobs(&dest).unwrap().len(), 1);
    cleanup(&dir);
}

fn watermark_store_source_replays_the_boundary_and_reseeds_body() {
    let dir = scratch("watermark");
    let dest_dir = dir.join("dest");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE main.events (ts TIMESTAMP, v VARCHAR);")
        .unwrap();
    let insert = |db: &mut PinkDb, ts: &str, v: &str| {
        db.execute(&format!(
            "INSERT INTO main.events VALUES (TIMESTAMP '{ts}', '{v}');"
        ))
        .unwrap();
    };
    insert(&mut db, "2026-01-01 00:00:00", "a");
    insert(&mut db, "2026-01-02 00:00:00", "b");

    let mut dest = create_lake(&dest_dir).unwrap();
    let mut spec = JobSpec::new(Source::Store {
        database: "main".to_string(),
    });
    spec.watermark_columns = "main.events=ts".to_string();
    let job = replication::create(&mut dest, spec).unwrap();
    let report = replication::catch_up(&mut dest, job, Some(&db), None).unwrap();
    assert_eq!(report.tables_synced, 1, "{}", report.message);
    assert_eq!(
        table_state(&dest, job, "main.events").unwrap().strategy,
        Strategy::Watermark
    );
    assert_same_rows(
        &lake_table_rows(&dest, "main.events"),
        &data_rows(db.table_in("main", "events").unwrap()),
        "watermark seed",
    );

    // Boundary replay: a new row with the previous maximum timestamp must
    // arrive, and a higher one extends the cursor.
    insert(&mut db, "2026-01-02 00:00:00", "b2");
    insert(&mut db, "2026-01-03 00:00:00", "c");
    let report = replication::catch_up(&mut dest, job, Some(&db), None).unwrap();
    assert_eq!(report.rows_written, 2);
    assert_same_rows(
        &lake_table_rows(&dest, "main.events"),
        &data_rows(db.table_in("main", "events").unwrap()),
        "watermark boundary replay",
    );

    // A NULL watermark value refuses the table before any destination change.
    let version = dest.current_version();
    insert_null_ts(&mut db);
    let report = replication::catch_up(&mut dest, job, Some(&db), None).unwrap();
    assert_eq!(report.tables_failed, 1);
    assert!(report.message.contains("NULL"), "{}", report.message);
    assert_eq!(
        dest.current_version(),
        version,
        "a refused table changes no data"
    );
    assert_eq!(
        replication::jobs(&dest).unwrap()[0].status.as_str(),
        "error"
    );
    db.execute("DELETE FROM main.events WHERE ts IS NULL;")
        .unwrap();

    // A high below the cursor (the maximum row was deleted) reseeds.
    db.execute("DELETE FROM main.events WHERE ts = TIMESTAMP '2026-01-03 00:00:00';")
        .unwrap();
    let report = replication::catch_up(&mut dest, job, Some(&db), None).unwrap();
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_same_rows(
        &lake_table_rows(&dest, "main.events"),
        &data_rows(db.table_in("main", "events").unwrap()),
        "watermark backwards reseed",
    );
    cleanup(&dir);
}

fn insert_null_ts(db: &mut PinkDb) {
    append_row(
        db,
        "main",
        "events",
        vec![Value::Null, Value::Varchar("null".to_string())],
    );
}

fn watermark_lake_source_works_too_body() {
    let dir = scratch("watermark_lake");
    let src_dir = dir.join("src");
    let dest_dir = dir.join("dest");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE main.events (ts TIMESTAMP, v VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO main.events VALUES (TIMESTAMP '2026-02-01 00:00:00', 'a');")
        .unwrap();
    save_store_lake(&db, &src_dir).unwrap();

    let mut dest = create_lake(&dest_dir).unwrap();
    let mut spec = JobSpec::new(job_source(&src_dir));
    spec.watermark_columns = "main.events=ts".to_string();
    let job = replication::create(&mut dest, spec).unwrap();
    let report = run(&mut dest, job);
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_eq!(
        table_state(&dest, job, "main.events").unwrap().strategy,
        Strategy::Watermark
    );
    let mut source = open_lake(&src_dir).unwrap();
    let mut commit = Commit::new();
    let mut rows = DataTable::new(
        "events".to_string(),
        vec![
            ("ts".to_string(), DataType::Timestamp),
            ("v".to_string(), DataType::Varchar),
        ],
    )
    .unwrap();
    rows.append_row(vec![
        Value::Timestamp(pinkdb::types::parse_timestamp("2026-02-02 00:00:00").unwrap()),
        Value::Varchar("b".to_string()),
    ])
    .unwrap();
    commit.append("main.events", Arc::new(rows));
    source.commit(&commit).unwrap();

    let report = run(&mut dest, job);
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_same_rows(
        &lake_table_rows(&dest, "main.events"),
        &lake_table_rows(&source, "main.events"),
        "lake watermark",
    );
    assert_eq!(
        table_state(&dest, job, "main.events")
            .unwrap()
            .last_source_version,
        Some(source.current_version())
    );
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Lake source: snapshot strategy
// ---------------------------------------------------------------------------

fn snapshot_lake_source_incremental_and_reseed_paths_body() {
    let dir = scratch("snapshot");
    let src_dir = dir.join("src");
    let dest_dir = dir.join("dest");
    let mut db = store(3);
    save_store_lake(&db, &src_dir).unwrap(); // v2

    let mut dest = create_lake(&dest_dir).unwrap();
    // Keep replication's commits file-backed so the verification-tamper test
    // can address a positional delete (small appends are inlined by default).
    dest.set_data_inlining_row_limit(0);
    let job = replication::create(&mut dest, JobSpec::new(job_source(&src_dir))).unwrap();
    let report = run(&mut dest, job);
    assert_eq!(report.tables_synced, 1, "{}", report.message);
    let seeded = table_state(&dest, job, "main.t").unwrap();
    assert_eq!(seeded.strategy, Strategy::Snapshot);
    assert_eq!(seeded.last_source_version, Some(2));
    assert!(seeded.source_table_id.is_some());
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &lake_table_rows(&open_lake(&src_dir).unwrap(), "main.t"),
        "snapshot seed",
    );
    assert_eq!(replication::versions_behind(&dest, job).unwrap(), Some(0));

    // Incremental: insert + update + delete over one commit.
    let mut wa = WritableAttachment::attach(open_lake(&src_dir).unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    db.execute("INSERT INTO t VALUES (200, 'x'), (201, 'y');")
        .unwrap();
    db.execute("UPDATE t SET s = 'U2' WHERE i = 102;").unwrap();
    db.execute("DELETE FROM t WHERE i = 100;").unwrap();
    wa.commit(&db, &CommitSettings::default()).unwrap();
    drop(wa);
    let source = open_lake(&src_dir).unwrap();
    let high = source.current_version();
    assert_eq!(replication::versions_behind(&dest, job).unwrap(), Some(1));

    let report = run(&mut dest, job);
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_eq!(report.rows_written, 3, "2 inserts + the update postimage");
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &lake_table_rows(&source, "main.t"),
        "snapshot incremental",
    );
    assert_eq!(
        table_state(&dest, job, "main.t")
            .unwrap()
            .last_source_version,
        Some(high)
    );

    // A no-change cycle publishes nothing and keeps the cursor.
    let version = dest.current_version();
    let report = run(&mut dest, job);
    assert_eq!(dest.current_version(), version);
    assert_eq!(report.rows_written, 0);

    // A full-save version inside the range needs no reseed: the change
    // functions report a rewritten table as whole-file deletions plus the new
    // files' insertions, and the multiset check confirms the result, so the
    // full-sync watermark stays at the seed.
    let mut source = open_lake(&src_dir).unwrap();
    db.execute("INSERT INTO t VALUES (300, 'full');").unwrap();
    source.write_from_store(&db).unwrap();
    let full = source.current_version();
    let report = run(&mut dest, job);
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    let state = table_state(&dest, job, "main.t").unwrap();
    assert_eq!(state.last_source_version, Some(full));
    assert_eq!(state.last_full_sync_version, Some(2));
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &lake_table_rows(&source, "main.t"),
        "full-save reseed",
    );

    // A tampered destination (a row removed behind the job's back) is caught
    // by the multiset verification and reseeded.
    let files = dest.list_files(Some("main.t"), None, false).unwrap();
    let victim = files
        .iter()
        .find(|file| file.delete_file.is_none() && file.rows.unwrap_or(0) > 0)
        .expect("a data file");
    let mut tamper = Commit::new();
    tamper.delete_positions("main.t", &victim.path, &[0]);
    dest.commit(&tamper).unwrap();
    assert_ne!(
        sorted(&lake_table_rows(&dest, "main.t")),
        sorted(&lake_table_rows(&source, "main.t")),
        "the tamper must be visible"
    );
    let report = run(&mut dest, job);
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &lake_table_rows(&source, "main.t"),
        "verification reseed",
    );

    // Cursor below the oldest retained version: expire everything up to it.
    let mut wa = WritableAttachment::attach(open_lake(&src_dir).unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    db.execute("INSERT INTO t VALUES (500, 'oldest');").unwrap();
    wa.commit(&db, &CommitSettings::default()).unwrap();
    drop(wa);
    let mut source = open_lake(&src_dir).unwrap();
    let head = source.current_version();
    source
        .expire_snapshots(&ExpireSettings {
            versions: Some((2..head).collect()),
            older_than_micros: None,
            retain_last: None,
            dry_run: false,
        })
        .unwrap();
    assert_eq!(
        source
            .versions()
            .unwrap()
            .into_iter()
            .find(|version| *version > 1),
        Some(head),
        "only the create version and the head remain"
    );
    let report = run(&mut dest, job);
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &lake_table_rows(&source, "main.t"),
        "oldest-version reseed",
    );

    // Identity change: drop + recreate on the source forces a reseed and a
    // new recorded identity.
    let old_id = table_state(&dest, job, "main.t").unwrap().source_table_id;
    let mut source = open_lake(&src_dir).unwrap();
    let mut drop = Commit::new();
    drop.drop_table("main.t");
    source.commit(&drop).unwrap();
    let mut recreate = Commit::new();
    let mut rows = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), DataType::Integer),
            ("s".to_string(), DataType::Varchar),
        ],
    )
    .unwrap();
    rows.append_row(vec![Value::Integer(7), Value::Varchar("new".into())])
        .unwrap();
    recreate.append("main.t", Arc::new(rows));
    source.commit(&recreate).unwrap();
    let new_id = open_lake(&src_dir)
        .unwrap()
        .table_ids()
        .unwrap()
        .into_iter()
        .find(|(key, _)| key == "main.t")
        .and_then(|(_, id)| id)
        .unwrap();
    assert_ne!(old_id.as_deref(), Some(new_id.as_str()));
    let report = run(&mut dest, job);
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    let state = table_state(&dest, job, "main.t").unwrap();
    assert_eq!(state.source_table_id.as_deref(), Some(new_id.as_str()));
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &lake_table_rows(&source, "main.t"),
        "identity reseed",
    );
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Schema drift
// ---------------------------------------------------------------------------

fn schema_drift_same_extend_and_recreate_body() {
    let dir = scratch("schema");
    let src_dir = dir.join("src");
    let dest_dir = dir.join("dest");
    let mut db = store(2);
    save_store_lake(&db, &src_dir).unwrap();

    let mut dest = create_lake(&dest_dir).unwrap();
    let job = replication::create(&mut dest, JobSpec::new(job_source(&src_dir))).unwrap();
    run(&mut dest, job);
    let version = dest.current_version();

    // Identical schema: a no-op cycle.
    run(&mut dest, job);
    assert_eq!(dest.current_version(), version);

    // Additive extension: the new column lands in place with its default and
    // the snapshot table reseeds.
    let mut wa = WritableAttachment::attach(open_lake(&src_dir).unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 7;")
        .unwrap();
    db.execute("UPDATE t SET n = 8 WHERE i = 101;").unwrap();
    wa.commit(&db, &CommitSettings::default()).unwrap();
    drop(wa);
    let source = open_lake(&src_dir).unwrap();
    let report = run(&mut dest, job);
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_eq!(
        lake_table_schema(&dest, "main.t"),
        source
            .load(None)
            .unwrap()
            .table_in("main", "t")
            .unwrap()
            .schema()
    );
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &lake_table_rows(&source, "main.t"),
        "additive drift",
    );

    // Incompatible drift (a dropped column) recreates the table.
    let mut wa = WritableAttachment::attach(open_lake(&src_dir).unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    db.execute("ALTER TABLE t DROP COLUMN s;").unwrap();
    db.execute("INSERT INTO t (i, n) VALUES (9, 9);").unwrap();
    let report = wa.commit(&db, &CommitSettings::default()).unwrap();
    assert!(report.committed, "the source schema changed");
    drop(wa);
    let source = open_lake(&src_dir).unwrap();
    let report = run(&mut dest, job);
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_eq!(
        lake_table_schema(&dest, "main.t"),
        vec![
            ("i".to_string(), DataType::Integer),
            ("n".to_string(), DataType::Integer)
        ]
    );
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &lake_table_rows(&source, "main.t"),
        "incompatible drift",
    );
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Include/exclude, stale drops, listing failures
// ---------------------------------------------------------------------------

fn include_exclude_and_stale_table_drops_body() {
    let dir = scratch("filters");
    let src_dir = dir.join("src");
    let dest_dir = dir.join("dest");
    let mut db = PinkDb::new();
    db.execute("CREATE DATABASE other;").unwrap();
    db.execute("CREATE TABLE main.a (i INTEGER);").unwrap();
    db.execute("CREATE TABLE main.b (i INTEGER);").unwrap();
    db.execute("CREATE TABLE other.c (i INTEGER);").unwrap();
    db.execute("INSERT INTO main.a VALUES (1);").unwrap();
    db.execute("INSERT INTO main.b VALUES (2);").unwrap();
    db.execute("INSERT INTO other.c VALUES (3);").unwrap();
    save_store_lake(&db, &src_dir).unwrap();

    let mut dest = create_lake(&dest_dir).unwrap();
    let mut spec = JobSpec::new(job_source(&src_dir));
    spec.include = "main.*".to_string();
    spec.exclude = "main.b".to_string();
    let job = replication::create(&mut dest, spec).unwrap();
    let report = run(&mut dest, job);
    assert_eq!(report.tables_synced, 1, "{}", report.message);
    assert!(lake_table_rows(&dest, "main.a").len() == 1);
    assert!(
        dest.load(None).unwrap().table_in("main", "b").is_err(),
        "the excluded table is never replicated"
    );
    assert!(
        dest.load(None).unwrap().table_in("other", "c").is_err(),
        "an excluded source table is never replicated"
    );

    // The typed job state records the filter; the destination owns only the
    // included table.
    assert_eq!(replication::jobs(&dest).unwrap()[0].exclude, "main.b");
    assert!(dest.load(None).unwrap().table_in("main", "a").is_ok());

    // A source drop drops the destination table and its state row.
    let mut source = open_lake(&src_dir).unwrap();
    let mut drop = Commit::new();
    drop.drop_table("main.a");
    source.commit(&drop).unwrap();
    let report = run(&mut dest, job);
    assert_eq!(report.tables_dropped, 1, "{}", report.message);
    assert!(table_state(&dest, job, "main.a").is_none());
    assert!(dest.load(None).unwrap().table_in("main", "a").is_err());

    // An unlistable source fails the whole cycle and drops nothing.
    cleanup(&src_dir);
    assert!(
        dest.load(None).unwrap().table_in("main", "b").is_err(),
        "b is excluded; nothing else remains"
    );
    let err = replication::catch_up(&mut dest, job, None, None).unwrap_err();
    assert!(err.to_string().contains("not a pink lake"), "{err}");
    assert_eq!(
        replication::jobs(&dest).unwrap()[0].status.as_str(),
        "error"
    );
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Ownership
// ---------------------------------------------------------------------------

fn ownership_refuses_unmanaged_tables_and_two_jobs_on_one_table_body() {
    let dir = scratch("ownership");
    let src_dir = dir.join("src");
    let unmanaged_dir = dir.join("unmanaged");
    let shared_dir = dir.join("shared");
    let db = store(2);
    save_store_lake(&db, &src_dir).unwrap();

    // An existing destination table with no state row is not ours to touch.
    let mut unmanaged = create_lake(&unmanaged_dir).unwrap();
    unmanaged.write_from_store(&db).unwrap();
    let job = replication::create(&mut unmanaged, JobSpec::new(job_source(&src_dir))).unwrap();
    let report = run(&mut unmanaged, job);
    assert_eq!(report.tables_failed, 1);
    assert!(
        report.message.contains("already exists and is not managed"),
        "{}",
        report.message
    );
    assert!(
        table_state(&unmanaged, job, "main.t").is_none(),
        "no state row is written for an unmanaged destination table"
    );

    // Two jobs on one source/destination table: the first claims it, the
    // second refuses and writes no state row.
    let mut dest = create_lake(&shared_dir).unwrap();
    let first = replication::create(&mut dest, JobSpec::new(job_source(&src_dir))).unwrap();
    let second = replication::create(&mut dest, JobSpec::new(job_source(&src_dir))).unwrap();
    run(&mut dest, first);
    let report = run(&mut dest, second);
    assert_eq!(report.tables_failed, 1);
    assert!(
        report
            .message
            .contains(&format!("already managed by replication job {first}")),
        "{}",
        report.message
    );
    assert!(table_state(&dest, second, "main.t").is_none());
    assert!(table_state(&dest, first, "main.t").is_some());
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Per-table failure isolation, guard, pause
// ---------------------------------------------------------------------------

fn per_table_failure_is_isolated_body() {
    let dir = scratch("isolation");
    let dest_dir = dir.join("dest");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE main.good (i INTEGER);").unwrap();
    db.execute("CREATE TABLE main.bad (i INTEGER);").unwrap();
    db.execute("INSERT INTO main.good VALUES (1);").unwrap();
    db.execute("INSERT INTO main.bad VALUES (2);").unwrap();

    let mut dest = create_lake(&dest_dir).unwrap();
    let mut spec = JobSpec::new(Source::Store {
        database: "main".to_string(),
    });
    spec.watermark_columns = "main.bad=missing".to_string();
    let job = replication::create(&mut dest, spec).unwrap();
    let report = replication::catch_up(&mut dest, job, Some(&db), None).unwrap();
    assert_eq!(report.tables_synced, 1, "{}", report.message);
    assert_eq!(report.tables_failed, 1);
    assert!(report.message.contains("main.bad"));
    assert!(report.message.contains("not found"));
    assert!(dest.load(None).unwrap().table_in("main", "good").is_ok());
    assert!(dest.load(None).unwrap().table_in("main", "bad").is_err());
    assert!(
        table_state(&dest, job, "main.bad")
            .unwrap()
            .last_error
            .unwrap()
            .contains("not found")
    );
    let job_state = &replication::jobs(&dest).unwrap()[0];
    assert_eq!(job_state.status.as_str(), "error");
    assert!(
        job_state
            .last_error
            .as_deref()
            .unwrap()
            .contains("1 table(s) failed")
    );
    assert!(job_state.last_ok.is_none());
    cleanup(&dir);
}

fn cycle_guard_pause_resume_and_status_transitions_body() {
    let dir = scratch("guard");
    let src_dir = dir.join("src");
    let dest_dir = dir.join("dest");
    let db = store(1);
    save_store_lake(&db, &src_dir).unwrap();

    let mut dest = create_lake(&dest_dir).unwrap();
    let err = replication::create(&mut dest, JobSpec::new(job_source(&dest_dir))).unwrap_err();
    assert!(err.to_string().contains("into itself"), "{err}");
    let job = replication::create(&mut dest, JobSpec::new(job_source(&src_dir))).unwrap();

    let guard = CycleGuard::acquire(dest.path(), job).unwrap();
    assert!(CycleGuard::is_active(dest.path(), job));
    let err = replication::catch_up(&mut dest, job, None, None).unwrap_err();
    assert!(err.to_string().contains("already running"), "{err}");
    drop(guard);
    assert!(!CycleGuard::is_active(dest.path(), job));
    let report = run(&mut dest, job);
    assert_eq!(report.tables_synced, 1);

    // A manual success leaves the job `created` (no supervisor in T1).
    assert_eq!(
        replication::jobs(&dest).unwrap()[0].status.as_str(),
        "created"
    );

    replication::pause(&mut dest, job).unwrap();
    let err = replication::catch_up(&mut dest, job, None, None).unwrap_err();
    assert!(err.to_string().contains("paused"), "{err}");
    replication::resume(&mut dest, job).unwrap();
    assert_eq!(
        replication::jobs(&dest).unwrap()[0].status.as_str(),
        "running"
    );
    let report = run(&mut dest, job);
    assert_eq!(report.tables_failed, 0);
    assert_eq!(
        replication::jobs(&dest).unwrap()[0].status.as_str(),
        "created",
        "a successful unsupervised cycle drops back to created"
    );

    replication::pause(&mut dest, job).unwrap();
    let resumed = replication::resume_all(&mut dest).unwrap();
    assert_eq!(resumed, vec![job]);
    assert_eq!(
        replication::jobs(&dest).unwrap()[0].status.as_str(),
        "running"
    );

    // Dropping the job removes its state but leaves the replicated table.
    replication::drop(&mut dest, job).unwrap();
    assert!(replication::jobs(&dest).unwrap().is_empty());
    assert!(dest.load(None).unwrap().table_in("main", "t").is_ok());
    cleanup(&dir);
}

fn maintenance_never_touches_replication_state_body() {
    use pink_lake::CleanupSettings;

    let dir = scratch("maintenance");
    let src_dir = dir.join("src");
    let dest_dir = dir.join("dest");
    let db = store(2);
    save_store_lake(&db, &src_dir).unwrap();

    let mut dest = create_lake(&dest_dir).unwrap();
    let job = replication::create(&mut dest, JobSpec::new(job_source(&src_dir))).unwrap();
    run(&mut dest, job);
    dest.cleanup_old_files(&CleanupSettings {
        older_than_micros: None,
        all: true,
        dry_run: false,
        confirm: true,
    })
    .unwrap();
    dest.delete_orphaned_files(&CleanupSettings {
        older_than_micros: None,
        all: true,
        dry_run: false,
        confirm: true,
    })
    .unwrap();
    assert_eq!(replication::jobs(&dest).unwrap().len(), 1);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Crash convergence
// ---------------------------------------------------------------------------

fn a_lost_cursor_update_converges_on_the_next_cycle_body() {
    let dir = scratch("crash");
    let src_dir = dir.join("src");
    let dest_dir = dir.join("dest");
    let mut db = store(3);
    save_store_lake(&db, &src_dir).unwrap();

    let mut dest = create_lake(&dest_dir).unwrap();
    let job = replication::create(&mut dest, JobSpec::new(job_source(&src_dir))).unwrap();
    run(&mut dest, job);
    let source_before = open_lake(&src_dir).unwrap();
    let cursor = table_state(&dest, job, "main.t")
        .unwrap()
        .last_source_version;
    assert_eq!(cursor, Some(source_before.current_version()));

    // Source moves on; the cycle commits but its state write is "lost".
    let mut wa = WritableAttachment::attach(open_lake(&src_dir).unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    db.execute("INSERT INTO t VALUES (200, 'x');").unwrap();
    db.execute("UPDATE t SET s = 'U2' WHERE i = 102;").unwrap();
    db.execute("DELETE FROM t WHERE i = 100;").unwrap();
    wa.commit(&db, &CommitSettings::default()).unwrap();
    drop(wa);
    run(&mut dest, job);
    let source = open_lake(&src_dir).unwrap();
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &lake_table_rows(&source, "main.t"),
        "first cycle",
    );

    // Restore the pre-cycle cursor (the commit landed, the cursor did not),
    // then run again: the replayed range converges without duplicates.
    pink_lake::catalog_execute(
        &catalog_url(&dest_dir),
        &format!(
            "UPDATE pinkdb_replication_table SET last_source_version = {};",
            cursor.unwrap()
        ),
    )
    .unwrap();
    let report = run(&mut dest, job);
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &lake_table_rows(&source, "main.t"),
        "replayed cycle",
    );
    assert_eq!(
        table_state(&dest, job, "main.t")
            .unwrap()
            .last_source_version,
        Some(source.current_version())
    );
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Encryption
// ---------------------------------------------------------------------------

#[cfg(feature = "encryption")]
fn encrypted_source_and_destination_body() {
    use pink_lake::EncryptionKey;

    let dir = scratch("encrypted");
    let src_dir = dir.join("src");
    let dest_dir = dir.join("dest");
    let source_key = EncryptionKey::from_passphrase("source-key");
    let dest_key = EncryptionKey::from_passphrase("dest-key");

    let db = store(2);
    let mut source = create_lake_with_key(&src_dir, source_key.clone()).unwrap();
    source.write_from_store(&db).unwrap();
    drop(source);

    let mut dest = create_lake_with_key(&dest_dir, dest_key.clone()).unwrap();
    dest.set_data_inlining_row_limit(0);
    let job = replication::create(&mut dest, JobSpec::new(job_source(&src_dir))).unwrap();
    let report = replication::catch_up(&mut dest, job, None, Some(source_key.clone())).unwrap();
    assert_eq!(report.tables_synced, 1, "{}", report.message);
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &data_rows(db.table_in("main", "t").unwrap()),
        "encrypted replication",
    );

    // Replication state is typed catalog rows, protected with the rest of
    // the keyed catalog; the data files are per-file sealed.
    assert_eq!(replication::jobs(&dest).unwrap().len(), 1);
    // The destination opens (and reads) only with its key.
    drop(dest);
    let dest = open_lake_with_key(&dest_dir, dest_key).unwrap();
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &data_rows(db.table_in("main", "t").unwrap()),
        "reopened encrypted destination",
    );
    assert!(
        open_lake(&dest_dir)
            .and_then(|lake| lake.load(None))
            .is_err(),
        "a keyless read of the encrypted destination is refused"
    );
    // versions_behind opens the source keylessly; an encrypted source is a
    // clear keyed-lake error rather than a wrong number.
    assert!(replication::versions_behind(&dest, job).is_err());
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Mock S3 destination
// ---------------------------------------------------------------------------

#[cfg(feature = "s3")]
fn mock_config(mock: &mock_s3::MockS3, bucket: &str, prefix: &str) -> pink_lake::S3Config {
    pink_lake::S3Config::new(bucket)
        .with_prefix(prefix)
        .with_region("auto")
        .with_endpoint(mock.endpoint())
        .allow_http(true)
        .with_credentials(mock.access_key_id(), "mock-secret")
}

#[cfg(feature = "s3")]
fn mock_s3_destination_replicates_and_persists_state_body() {
    let mock = mock_s3::MockS3::start();
    let config = mock_config(&mock, "replication-bucket", "lakes/dest");
    let dir = scratch("mock_s3");
    let src_dir = dir.join("src");
    let db = store(3);
    save_store_lake(&db, &src_dir).unwrap();

    let mut dest = open_or_create_s3_lake(config.clone()).unwrap();
    let job = match replication::jobs(&dest).unwrap().first() {
        Some(job) => job.id,
        None => replication::create(&mut dest, JobSpec::new(job_source(&src_dir))).unwrap(),
    };
    let report = replication::catch_up(&mut dest, job, None, None).unwrap();
    assert_eq!(report.tables_synced, 1, "{}", report.message);
    assert!(
        mock.keys().iter().all(|k| k.starts_with("data/")
            || k.contains("/data/")
            || k.ends_with("lake-data.json")),
        "the destination prefix holds data objects only: {:?}",
        mock.keys()
    );

    let loaded = open_s3_lake(config).unwrap();
    assert_same_rows(
        &lake_table_rows(&loaded, "main.t"),
        &data_rows(db.table_in("main", "t").unwrap()),
        "mock-S3 destination",
    );
    assert_eq!(replication::jobs(&loaded).unwrap().len(), 1);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Supervisor (wave 89 T2)
// ---------------------------------------------------------------------------

/// Poll `check` until it holds, or fail after `timeout`.
fn wait_for(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if check() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out after {timeout:?} waiting for {what}");
}

fn dest_rows(dest_dir: &Path) -> usize {
    let Ok(lake) = open_lake(dest_dir) else {
        return 0;
    };
    let Ok(db) = lake.load(None) else {
        return 0;
    };
    db.table_in("main", "t")
        .map(|table| table.num_rows())
        .unwrap_or(0)
}

fn dest_job(dest_dir: &Path, job: u64) -> replication::Job {
    replication::jobs(&open_lake(dest_dir).unwrap())
        .unwrap()
        .into_iter()
        .find(|candidate| candidate.id == job)
        .unwrap()
}

fn supervisor_status(supervisor: &Supervisor, job: u64) -> replication::SupervisorStatus {
    supervisor
        .status()
        .into_iter()
        .find(|status| status.job_id == job)
        .unwrap_or_else(|| panic!("job {job} is not in the supervisor schedule"))
}

fn supervisor_runs_notifies_pauses_resumes_and_stops_body() {
    let dir = scratch("supervisor_lifecycle");
    let src_dir = dir.join("src");
    let dest_dir = dir.join("dest");
    let db = store(2);
    save_store_lake(&db, &src_dir).unwrap();

    let mut dest = create_lake(&dest_dir).unwrap();
    let mut spec = JobSpec::new(job_source(&src_dir));
    // Long enough that only `notify` can explain a prompt second cycle.
    spec.interval = Duration::from_secs(3600);
    let job = replication::create(&mut dest, spec).unwrap();
    drop(dest);

    let supervisor = Supervisor::start(&catalog_url(&dest_dir), None).unwrap();
    wait_for(
        "the first supervised cycle",
        Duration::from_secs(15),
        || dest_rows(&dest_dir) == 2,
    );
    wait_for(
        "the supervisor status to report the cycle",
        Duration::from_secs(5),
        || {
            matches!(
                supervisor_status(&supervisor, job).last_cycle,
                Some(CycleOutcome::Completed {
                    tables_failed: 0,
                    ..
                })
            )
        },
    );
    assert!(
        supervisor_status(&supervisor, job).next_due.is_some(),
        "a scheduled job has a next due time"
    );

    // Stop joins promptly and releases the registry (a new start succeeds).
    let stopped = Instant::now();
    supervisor.stop();
    assert!(
        stopped.elapsed() < Duration::from_secs(5),
        "stop took {:?}",
        stopped.elapsed()
    );
    let again = Supervisor::start(&catalog_url(&dest_dir), None).unwrap();
    again.stop();
    cleanup(&dir);
}

fn supervisor_backs_off_after_a_failed_cycle_body() {
    let dir = scratch("supervisor_backoff");
    let dest_dir = dir.join("dest");
    let missing = dir.join("no-such-source");
    let mut dest = create_lake(&dest_dir).unwrap();
    let mut spec = JobSpec::new(Source::Lake {
        location: missing.to_str().unwrap().to_string(),
    });
    spec.interval = Duration::from_millis(250);
    let job = replication::create(&mut dest, spec).unwrap();
    drop(dest);

    let run_started = Instant::now();
    let supervisor = Supervisor::start(&catalog_url(&dest_dir), None).unwrap();
    wait_for("the first failed cycle", Duration::from_secs(10), || {
        supervisor_status(&supervisor, job).consecutive_failures >= 1
    });
    let first = supervisor_status(&supervisor, job);
    assert!(
        matches!(first.last_cycle, Some(CycleOutcome::Failed(_))),
        "the missing source must fail the cycle: {first:?}"
    );
    // The job is `error` (runnable) and the next attempt is scheduled — the
    // first failure backs off to 2 × 250 ms, not an immediate retry.
    assert_eq!(dest_job(&dest_dir, job).status, JobStatus::Error);
    wait_for("the backed-off retry", Duration::from_secs(10), || {
        supervisor_status(&supervisor, job).consecutive_failures >= 2
    });
    assert!(
        run_started.elapsed() >= Duration::from_millis(400),
        "the retry came after {:?}, before the 500 ms backoff",
        run_started.elapsed()
    );
    assert!(supervisor_status(&supervisor, job).next_due.is_some());
    supervisor.stop();
    cleanup(&dir);
}

fn supervisor_skips_store_source_jobs_body() {
    let dir = scratch("supervisor_store");
    let dest_dir = dir.join("dest");
    let mut dest = create_lake(&dest_dir).unwrap();
    let job = replication::create(
        &mut dest,
        JobSpec::new(Source::Store {
            database: "main".to_string(),
        }),
    )
    .unwrap();
    drop(dest);

    let supervisor = Supervisor::start(&catalog_url(&dest_dir), None).unwrap();
    let skipped = supervisor.skipped();
    assert_eq!(skipped.len(), 1);
    assert_eq!(skipped[0].0, job);
    assert!(skipped[0].1.contains("not supervised"), "{}", skipped[0].1);
    let status = supervisor_status(&supervisor, job);
    assert!(matches!(status.last_cycle, Some(CycleOutcome::Skipped(_))));
    assert!(status.next_due.is_none(), "a skipped job is never due");
    std::thread::sleep(Duration::from_millis(400));
    let recorded = dest_job(&dest_dir, job);
    assert_eq!(recorded.status, JobStatus::Created);
    assert!(
        recorded.last_run.is_none(),
        "the supervisor must never run a store-source job"
    );
    supervisor.stop();
    cleanup(&dir);
}

fn stop_aborts_an_in_flight_cycle_at_a_table_boundary_body() {
    let dir = scratch("supervisor_abort");
    let src_dir = dir.join("src");
    let dest_dir = dir.join("dest");
    let mut db = PinkDb::new();
    for table in 0..60 {
        db.execute(&format!("CREATE TABLE main.t{table} (i INTEGER);"))
            .unwrap();
        for row in 0..20 {
            append_row(
                &mut db,
                "main",
                &format!("t{table}"),
                vec![Value::Integer(row)],
            );
        }
    }
    save_store_lake(&db, &src_dir).unwrap();

    let mut dest = create_lake(&dest_dir).unwrap();
    let mut spec = JobSpec::new(job_source(&src_dir));
    spec.interval = Duration::from_secs(3600);
    let job = replication::create(&mut dest, spec).unwrap();
    drop(dest);

    let supervisor = Supervisor::start(&catalog_url(&dest_dir), None).unwrap();
    wait_for("the first cycle to start", Duration::from_secs(10), || {
        CycleGuard::is_active(&dest_dir, job)
    });
    // No wall-clock bound (a loaded machine stretches any cycle): the abort is
    // proven by state. `stop` returned with no cycle active, the cycle did not
    // copy every table, and each table it recorded as synced is whole, so it
    // stopped between tables, never inside one.
    supervisor.stop();
    assert!(!CycleGuard::is_active(&dest_dir, job));
    let lake = open_lake(&dest_dir).unwrap();
    let synced = replication::tables(&lake, job).unwrap();
    assert!(
        synced.len() < 60,
        "the stop should abort at a table boundary, but all {} tables were copied",
        synced.len()
    );
    for table in &synced {
        assert_eq!(
            lake_table_rows(&lake, &table.dest).len(),
            20,
            "{} was cut inside the table",
            table.dest
        );
    }
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Backend wrappers: every scenario runs on `pink:` and, with the feature, on
// `sqlite:` (the catalog helper switches on a thread-local backend).
// ---------------------------------------------------------------------------
#[test]
fn store_source_full_and_merge_strategies() {
    with_backend("pink", store_source_full_and_merge_strategies_body);
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn store_source_full_and_merge_strategies_sqlite() {
    with_backend("sqlite", store_source_full_and_merge_strategies_body);
}

#[test]
fn watermark_store_source_replays_the_boundary_and_reseeds() {
    with_backend(
        "pink",
        watermark_store_source_replays_the_boundary_and_reseeds_body,
    );
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn watermark_store_source_replays_the_boundary_and_reseeds_sqlite() {
    with_backend(
        "sqlite",
        watermark_store_source_replays_the_boundary_and_reseeds_body,
    );
}

#[test]
fn watermark_lake_source_works_too() {
    with_backend("pink", watermark_lake_source_works_too_body);
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn watermark_lake_source_works_too_sqlite() {
    with_backend("sqlite", watermark_lake_source_works_too_body);
}

#[test]
fn snapshot_lake_source_incremental_and_reseed_paths() {
    with_backend(
        "pink",
        snapshot_lake_source_incremental_and_reseed_paths_body,
    );
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn snapshot_lake_source_incremental_and_reseed_paths_sqlite() {
    with_backend(
        "sqlite",
        snapshot_lake_source_incremental_and_reseed_paths_body,
    );
}

#[test]
fn schema_drift_same_extend_and_recreate() {
    with_backend("pink", schema_drift_same_extend_and_recreate_body);
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn schema_drift_same_extend_and_recreate_sqlite() {
    with_backend("sqlite", schema_drift_same_extend_and_recreate_body);
}

#[test]
fn include_exclude_and_stale_table_drops() {
    with_backend("pink", include_exclude_and_stale_table_drops_body);
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn include_exclude_and_stale_table_drops_sqlite() {
    with_backend("sqlite", include_exclude_and_stale_table_drops_body);
}

#[test]
fn ownership_refuses_unmanaged_tables_and_two_jobs_on_one_table() {
    with_backend(
        "pink",
        ownership_refuses_unmanaged_tables_and_two_jobs_on_one_table_body,
    );
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn ownership_refuses_unmanaged_tables_and_two_jobs_on_one_table_sqlite() {
    with_backend(
        "sqlite",
        ownership_refuses_unmanaged_tables_and_two_jobs_on_one_table_body,
    );
}

#[test]
fn per_table_failure_is_isolated() {
    with_backend("pink", per_table_failure_is_isolated_body);
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn per_table_failure_is_isolated_sqlite() {
    with_backend("sqlite", per_table_failure_is_isolated_body);
}

#[test]
fn cycle_guard_pause_resume_and_status_transitions() {
    with_backend("pink", cycle_guard_pause_resume_and_status_transitions_body);
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn cycle_guard_pause_resume_and_status_transitions_sqlite() {
    with_backend(
        "sqlite",
        cycle_guard_pause_resume_and_status_transitions_body,
    );
}

#[test]
fn maintenance_never_touches_replication_state() {
    with_backend("pink", maintenance_never_touches_replication_state_body);
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn maintenance_never_touches_replication_state_sqlite() {
    with_backend("sqlite", maintenance_never_touches_replication_state_body);
}

#[test]
fn a_lost_cursor_update_converges_on_the_next_cycle() {
    with_backend(
        "pink",
        a_lost_cursor_update_converges_on_the_next_cycle_body,
    );
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn a_lost_cursor_update_converges_on_the_next_cycle_sqlite() {
    with_backend(
        "sqlite",
        a_lost_cursor_update_converges_on_the_next_cycle_body,
    );
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_source_and_destination() {
    with_backend("pink", encrypted_source_and_destination_body);
}
#[cfg(all(feature = "catalog-sqlite", feature = "encryption"))]
#[test]
fn encrypted_source_and_destination_sqlite() {
    with_backend("sqlite", encrypted_source_and_destination_body);
}

#[cfg(feature = "s3")]
#[test]
fn mock_s3_destination_replicates_and_persists_state() {
    with_backend(
        "pink",
        mock_s3_destination_replicates_and_persists_state_body,
    );
}
// The mock-S3 test's catalog is the local `pink:` database by design, so
// there is no separate sqlite variant.

#[test]
fn supervisor_runs_notifies_pauses_resumes_and_stops() {
    with_backend(
        "pink",
        supervisor_runs_notifies_pauses_resumes_and_stops_body,
    );
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn supervisor_runs_notifies_pauses_resumes_and_stops_sqlite() {
    with_backend(
        "sqlite",
        supervisor_runs_notifies_pauses_resumes_and_stops_body,
    );
}

#[test]
fn supervisor_backs_off_after_a_failed_cycle() {
    with_backend("pink", supervisor_backs_off_after_a_failed_cycle_body);
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn supervisor_backs_off_after_a_failed_cycle_sqlite() {
    with_backend("sqlite", supervisor_backs_off_after_a_failed_cycle_body);
}

#[test]
fn supervisor_skips_store_source_jobs() {
    with_backend("pink", supervisor_skips_store_source_jobs_body);
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn supervisor_skips_store_source_jobs_sqlite() {
    with_backend("sqlite", supervisor_skips_store_source_jobs_body);
}

#[test]
fn stop_aborts_an_in_flight_cycle_at_a_table_boundary() {
    with_backend(
        "pink",
        stop_aborts_an_in_flight_cycle_at_a_table_boundary_body,
    );
}
#[cfg(feature = "catalog-sqlite")]
#[test]
fn stop_aborts_an_in_flight_cycle_at_a_table_boundary_sqlite() {
    with_backend(
        "sqlite",
        stop_aborts_an_in_flight_cycle_at_a_table_boundary_body,
    );
}

fn table_ids_are_assigned_preserved_body() {
    let dir = scratch("table_ids");
    let lake_dir = dir.join("lake");
    let db = store(2);
    let mut lake = create_lake(&lake_dir).unwrap();
    lake.write_from_store(&db).unwrap();

    let ids = lake.table_ids().unwrap();
    assert_eq!(ids.len(), 1);
    let first = ids[0].1.clone().expect("a save assigns an id");
    assert_eq!(first.len(), 32);
    assert!(
        first
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    );

    // A second save and an append preserve the identity.
    lake.write_from_store(&db).unwrap();
    assert_eq!(
        lake.table_ids().unwrap()[0].1.as_deref(),
        Some(first.as_str())
    );
    let mut commit = Commit::new();
    let mut rows = DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), DataType::Integer),
            ("s".to_string(), DataType::Varchar),
        ],
    )
    .unwrap();
    rows.append_row(vec![Value::Integer(500), Value::Varchar("new".into())])
        .unwrap();
    commit.append("main.t", Arc::new(rows));
    lake.commit(&commit).unwrap();
    assert_eq!(
        lake.table_ids().unwrap()[0].1.as_deref(),
        Some(first.as_str())
    );

    // A drop + recreate mints a new identity.
    let mut drop = Commit::new();
    drop.drop_table("main.t");
    lake.commit(&drop).unwrap();
    let mut recreate = Commit::new();
    let mut rows =
        DataTable::new("t".to_string(), vec![("i".to_string(), DataType::Integer)]).unwrap();
    rows.append_row(vec![Value::Integer(1)]).unwrap();
    recreate.append("main.t", Arc::new(rows));
    lake.commit(&recreate).unwrap();
    let second = lake.table_ids().unwrap()[0].1.clone().unwrap();
    assert_ne!(first, second, "a recreated table gets a fresh identity");
    cleanup(&dir);
}

#[test]
fn table_ids_are_assigned_preserved() {
    with_backend("pink", table_ids_are_assigned_preserved_body);
}

#[cfg(feature = "catalog-sqlite")]
#[test]
fn table_ids_are_assigned_preserved_sqlite() {
    with_backend("sqlite", table_ids_are_assigned_preserved_body);
}
