//! Wave 133b: a lake scan hands morsels to the pipeline on demand — a worker
//! claims a morsel index and decodes it itself — instead of a producer thread
//! pushing chunks at workers that may be parked.
//!
//! The regression below is the shape that deadlocked: eight `.pink` files of
//! four row groups each (491 520 rows a file at `persist::ROW_GROUP_ROWS`),
//! scanned through an aggregate with only two pool threads. The old pushing
//! source left both threads waiting inside `next_morsel` while the producer
//! needed one of them to decode a file, so the query never finished.

#[path = "common/local_lake.rs"]
mod local_lake;
use local_lake::*;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use pink_lake::{Commit, Lake};
use pinkdb::{DataTable, DataType, PinkDb, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Rows in four `.pink` row groups: the smallest file with four of them.
const ROWS_PER_GROUPED_FILE: i64 = 491_520;
/// Files in the deadlock regression: a scan that needs the pool for real work.
const GROUPED_FILES: i64 = 8;

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_streaming_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => rows,
        other => panic!("{other:?}"),
    }
}

fn lake_table() -> DataTable {
    DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), DataType::Bigint),
            ("g".to_string(), DataType::Bigint),
        ],
    )
    .unwrap()
}

/// One `t(i BIGINT, g BIGINT)` data file per commit, `i` in `[first, first +
/// rows)`, committed with inlining off so every commit writes a real file.
fn append_file(lake: &mut Lake, first: i64, rows: i64) {
    let mut table = lake_table();
    for i in first..first + rows {
        table
            .append_row(vec![Value::Bigint(i), Value::Bigint(i * 2)])
            .unwrap();
    }
    let mut commit = Commit::new();
    commit.append("main.t", Arc::new(table));
    lake.commit(&commit).unwrap();
}

fn lake_with_files(tag: &str, files: i64, rows: i64) -> (PathBuf, Lake, PinkDb) {
    let dir = scratch(tag);
    let mut lake = create_lake(dir.join("lake")).unwrap();
    lake.set_data_inlining_row_limit(0);
    for file in 0..files {
        append_file(&mut lake, file * rows, rows);
    }
    let report = open_lake(dir.join("lake")).unwrap().attach(None).unwrap();
    let mut db = PinkDb::new();
    db.attach_lake(report.lake).unwrap();
    (dir, lake, db)
}

/// Run `work` on a two-thread rayon pool — the pool the pipeline draws its
/// workers from — and fail the test if it does not finish in time.
fn on_two_threads<T: Send + 'static>(
    limit: Duration,
    work: impl FnOnce() -> T + Send + 'static,
) -> T {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        tx.send(pool.install(work)).ok();
    });
    match rx.recv_timeout(limit) {
        Ok(value) => value,
        Err(mpsc::RecvTimeoutError::Timeout) => panic!(
            "the streamed lake scan did not finish within {limit:?}: a worker is \
             waiting on a source that needs the pool"
        ),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("the scan thread died"),
    }
}

/// The lake scan the previous statement ran, as `pinkdb_scan_io()` reports it:
/// `(files read, files pruned, peak morsels in flight)`.
fn lake_io(db: &mut PinkDb) -> (u64, u64, u64) {
    let rows = query(
        db,
        "SELECT source, unit, read, pruned, in_flight FROM pinkdb_scan_io();",
    );
    let (mut files, mut pruned, mut peak) = (None, 0, 0);
    for row in &rows {
        let [
            Value::Varchar(source),
            unit,
            Value::Bigint(read),
            Value::Bigint(pruned_rows),
            Value::Bigint(in_flight),
        ] = row.as_slice()
        else {
            panic!("unexpected scan_io row {row:?}");
        };
        assert!(
            source.contains("lake"),
            "the scan came from the lake: {source}"
        );
        peak = peak.max(*in_flight as u64);
        if let Value::Varchar(unit) = unit
            && unit == "files"
        {
            files = Some(*read as u64);
            pruned = *pruned_rows as u64;
        }
    }
    (
        files.expect("the lake scan reported its files"),
        pruned,
        peak,
    )
}

#[test]
fn two_pool_threads_stream_a_multi_row_group_lake() {
    let (dir, _lake, mut db) = lake_with_files("grouped", GROUPED_FILES, ROWS_PER_GROUPED_FILE);
    let total = GROUPED_FILES * ROWS_PER_GROUPED_FILE;
    let (rows, io) = on_two_threads(Duration::from_secs(180), move || {
        let rows = query(&mut db, "SELECT sum(i) FROM t;");
        (rows, lake_io(&mut db))
    });
    assert_eq!(
        rows,
        vec![vec![Value::Bigint(total * (total - 1) / 2)]],
        "every row of every row group reached the aggregate"
    );
    let (files_read, files_pruned, peak) = io;
    assert_eq!(files_read, GROUPED_FILES as u64, "every file was read");
    assert_eq!(files_pruned, 0, "no predicate, nothing pruned");
    assert_eq!(peak, 2, "both pool workers decode a morsel at once");
    let _ = std::fs::remove_dir_all(Path::new(&dir));
}

#[test]
fn an_aggregate_over_a_lake_reports_the_files_it_read_and_pruned() {
    const FILES: i64 = 8;
    const ROWS_PER_FILE: i64 = 3000;
    let (dir, _lake, mut db) = lake_with_files("prune", FILES, ROWS_PER_FILE);

    let (answer, io) = on_two_threads(Duration::from_secs(120), move || {
        let answer = query(&mut db, "SELECT count(*), sum(g) FROM t WHERE i < 700;");
        (answer, lake_io(&mut db))
    });
    assert_eq!(
        answer,
        vec![vec![Value::Bigint(700), Value::Bigint(700 * 699)]],
        "the pruned scan kept exactly the matching rows"
    );
    let (files_read, files_pruned, _peak) = io;
    // Only the first file holds rows below 700; the zone maps of the other
    // seven prune them before any byte is read.
    assert_eq!(files_read, 1, "one file survived the zone-map prune");
    assert_eq!(files_pruned, FILES as u64 - 1, "seven files pruned");
    let _ = std::fs::remove_dir_all(Path::new(&dir));
}

#[test]
fn a_join_build_side_prunes_the_lake_probe_before_it_reads() {
    const FILES: i64 = 8;
    const ROWS_PER_FILE: i64 = 3000;
    let (dir, _lake, mut db) = lake_with_files("join", FILES, ROWS_PER_FILE);

    let (answer, io) = on_two_threads(Duration::from_secs(120), move || {
        db.execute("CREATE TABLE keys (i BIGINT); INSERT INTO keys VALUES (750);")
            .unwrap();
        let answer = query(&mut db, "SELECT count(*) FROM t JOIN keys ON keys.i = t.i;");
        (answer, lake_io(&mut db))
    });
    assert_eq!(
        answer,
        vec![vec![Value::Bigint(1)]],
        "the join matched the one row of `keys`"
    );
    let (files_read, files_pruned, _peak) = io;
    // Wave 122's build-side bounds reach the lake scan: row 750 lives in the
    // first file, so the other seven are pruned before their footer is used.
    assert_eq!(files_read, 1, "the key bounds kept one file");
    assert_eq!(files_pruned, FILES as u64 - 1, "seven files pruned");
    let _ = std::fs::remove_dir_all(Path::new(&dir));
}
