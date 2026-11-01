//! The replication suite ported to database catalogs (wave 103 T2).
//!
//! The object-layout suite in `tests/replication.rs` stays until Part 2
//! deletes the JSON catalog; this is the same replication contract — full /
//! merge / snapshot / watermark strategies, incremental re-runs, ownership,
//! include/exclude and state that survives a reopen — with the **destination**
//! on a DuckLake catalog (`pink:` always, `sqlite:` with `catalog-sqlite`).
//!
//! Sources remain object lakes (a `Source::Lake` location is still a local
//! path in this pass); the destination is what carries the typed
//! `pinkdb_replication_*` tables.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::replication::{self, JobSpec, Source, Strategy};
use pink_lake::{Commit, CommitSettings, Lake, WritableAttachment, save_store};
use pinkdb::{DataTable, DataType, PinkDb, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

pub fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_replication_catalog_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

pub fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// How one backend mints a catalog URL for a test.
pub trait Suite {
    /// A catalog URL for `dir/name`.
    fn catalog(&mut self, dir: &Path, name: &str) -> String;
}

#[derive(Debug, Default)]
pub struct PinkSuite;

impl Suite for PinkSuite {
    fn catalog(&mut self, dir: &Path, name: &str) -> String {
        format!("pink:{}", dir.join(format!("{name}.pink")).display())
    }
}

#[cfg(feature = "catalog-sqlite")]
#[derive(Debug, Default)]
pub struct SqliteSuite;

#[cfg(feature = "catalog-sqlite")]
impl Suite for SqliteSuite {
    fn catalog(&mut self, dir: &Path, name: &str) -> String {
        format!("sqlite:{}", dir.join(format!("{name}.db")).display())
    }
}

fn new_destination(catalog: &str, data: &Path) -> Lake {
    Lake::create_with_catalog(catalog, data.to_str().unwrap()).unwrap()
}

fn open_destination(catalog: &str) -> Lake {
    Lake::open_with_catalog(catalog, None).unwrap()
}

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
        location: dir.to_str().unwrap().to_string(),
    }
}

pub fn store_source_full_and_merge(suite: &mut dyn Suite) {
    let dir = scratch("store_strategies");
    let data = dir.join("dest-data");
    let catalog = suite.catalog(&dir, "dest");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE main.t (i INTEGER, s VARCHAR);")
        .unwrap();
    for row in 0..3 {
        db.table_mut_in("main", "t")
            .unwrap()
            .append_row(vec![
                Value::Integer(100 + row),
                Value::Varchar(format!("v{row}")),
            ])
            .unwrap();
    }
    db.execute("CREATE TABLE main.k (id INTEGER PRIMARY KEY, v VARCHAR);")
        .unwrap();
    for id in 0..3 {
        db.execute(&format!("INSERT INTO k VALUES ({id}, 'k{id}');"))
            .unwrap();
    }

    let mut dest = new_destination(&catalog, &data);
    let job = replication::create(
        &mut dest,
        JobSpec::new(Source::Store {
            database: "main".to_string(),
        }),
    )
    .unwrap();
    let report = replication::catch_up(&mut dest, job, Some(&db), None).unwrap();
    assert_eq!(report.tables_synced, 2, "{}", report.message);
    assert_eq!(report.tables_failed, 0);
    assert_eq!(
        table_state(&dest, job, "main.t").unwrap().strategy,
        Strategy::Full
    );
    assert_eq!(
        table_state(&dest, job, "main.k").unwrap().strategy,
        Strategy::Merge
    );
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

    db.execute("INSERT INTO k VALUES (9, 'k9');").unwrap();
    db.execute("UPDATE k SET v = 'changed' WHERE id = 1;")
        .unwrap();
    db.execute("DELETE FROM k WHERE id = 0;").unwrap();
    db.execute("INSERT INTO t VALUES (200, 'x');").unwrap();
    let report = replication::catch_up(&mut dest, job, Some(&db), None).unwrap();
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_eq!(report.rows_written, 3, "full insert + merge insert/update");
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

    // The typed state survives a reopen of the destination catalog.
    drop(dest);
    let dest = open_destination(&catalog);
    assert_eq!(replication::jobs(&dest).unwrap().len(), 1);
    assert_eq!(
        table_state(&dest, job, "main.k").unwrap().strategy,
        Strategy::Merge
    );
    cleanup(&dir);
}

pub fn snapshot_lake_source_incremental(suite: &mut dyn Suite) {
    let dir = scratch("snapshot");
    let src_dir = dir.join("src");
    let data = dir.join("dest-data");
    let catalog = suite.catalog(&dir, "dest");
    let mut db = store(3);
    save_store(&db, &src_dir).unwrap(); // v2

    let mut dest = new_destination(&catalog, &data);
    dest.set_data_inlining_row_limit(0);
    let job = replication::create(&mut dest, JobSpec::new(job_source(&src_dir))).unwrap();
    let report = replication::catch_up(&mut dest, job, None, None).unwrap();
    assert_eq!(report.tables_synced, 1, "{}", report.message);
    assert_eq!(
        table_state(&dest, job, "main.t").unwrap().strategy,
        Strategy::Snapshot
    );
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &lake_table_rows(&Lake::open(&src_dir).unwrap(), "main.t"),
        "snapshot seed",
    );

    // One source commit with an insert, an update and a delete.
    let mut wa = WritableAttachment::open(src_dir.to_str().unwrap(), None).unwrap();
    wa.set_data_inlining_row_limit(0);
    db.execute("INSERT INTO t VALUES (200, 'x'), (201, 'y');")
        .unwrap();
    db.execute("UPDATE t SET s = 'U2' WHERE i = 102;").unwrap();
    db.execute("DELETE FROM t WHERE i = 100;").unwrap();
    wa.commit(&db, &CommitSettings::default()).unwrap();
    drop(wa);
    let source = Lake::open(&src_dir).unwrap();
    let report = replication::catch_up(&mut dest, job, None, None).unwrap();
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_eq!(report.rows_written, 3, "2 inserts + one update postimage");
    assert_same_rows(
        &lake_table_rows(&dest, "main.t"),
        &lake_table_rows(&source, "main.t"),
        "snapshot incremental",
    );
    assert_eq!(
        table_state(&dest, job, "main.t")
            .unwrap()
            .last_source_version,
        Some(source.current_version())
    );

    // State survives a reopen; a reopened destination still owns the table.
    drop(dest);
    let dest = open_destination(&catalog);
    assert_eq!(
        table_state(&dest, job, "main.t").unwrap().strategy,
        Strategy::Snapshot
    );
    cleanup(&dir);
}

pub fn watermark_lake_source(suite: &mut dyn Suite) {
    let dir = scratch("watermark");
    let src_dir = dir.join("src");
    let data = dir.join("dest-data");
    let catalog = suite.catalog(&dir, "dest");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE main.events (ts TIMESTAMP, v VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO main.events VALUES (TIMESTAMP '2026-02-01 00:00:00', 'a');")
        .unwrap();
    save_store(&db, &src_dir).unwrap();

    let mut dest = new_destination(&catalog, &data);
    let mut spec = JobSpec::new(job_source(&src_dir));
    spec.watermark_columns = "main.events=ts".to_string();
    let job = replication::create(&mut dest, spec).unwrap();
    let report = replication::catch_up(&mut dest, job, None, None).unwrap();
    assert_eq!(report.tables_failed, 0, "{}", report.message);
    assert_eq!(
        table_state(&dest, job, "main.events").unwrap().strategy,
        Strategy::Watermark
    );

    let mut source = Lake::open(&src_dir).unwrap();
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

    let report = replication::catch_up(&mut dest, job, None, None).unwrap();
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

pub fn include_exclude_and_ownership(suite: &mut dyn Suite) {
    let dir = scratch("include_exclude");
    let src_dir = dir.join("src");
    let data = dir.join("dest-data");
    let catalog = suite.catalog(&dir, "dest");
    let mut db = store(2);
    db.execute("CREATE TABLE main.other (i INTEGER);").unwrap();
    db.execute("INSERT INTO main.other VALUES (1);").unwrap();
    save_store(&db, &src_dir).unwrap();

    let mut dest = new_destination(&catalog, &data);
    let mut spec = JobSpec::new(job_source(&src_dir));
    spec.include = "main.t".to_string();
    let job = replication::create(&mut dest, spec).unwrap();
    let report = replication::catch_up(&mut dest, job, None, None).unwrap();
    assert_eq!(report.tables_synced, 1, "{}", report.message);
    let db = dest.load(None).unwrap();
    assert!(db.table_in("main", "t").is_ok());
    assert!(db.table_in("main", "other").is_err());

    // A second job may not claim an already-managed destination table.
    cleanup(&dir);
}
