//! Wave 98c: an aggregate over an attached lake table streams data files into
//! the fold instead of materializing the table. The table below is 60 files of
//! 3000 rows (well over the 131 072-row bound under which a table is
//! materialized and cached); the fold runs on a pinned worker count and the
//! executor's counter must stay within that many chunks in flight, while the
//! fold sees every row, and the answers must be the ones a materialized read
//! gives.

#[path = "common/local_lake.rs"]
mod local_lake;
use local_lake::*;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::{Commit, Lake};
use pinkdb::{PinkDb, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

const FILES: i32 = 60;
const ROWS_PER_FILE: i32 = 3000;

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

/// The bright catalog entry of `main.t` (data-file list included).
fn entry(lake_dir: &Path, key: &str) -> serde_json::Value {
    catalog_json(lake_dir)["tables"][key].clone()
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => rows,
        other => panic!("{other:?}"),
    }
}

/// One `t(i INTEGER, g BIGINT)` data file per commit (`i` in
/// `[first, first + ROWS_PER_FILE)`), committed with inlining off so every
/// commit writes a real file.
fn append_file(lake: &mut Lake, first: i32) {
    let mut table = pinkdb::DataTable::new(
        "t".to_string(),
        vec![
            ("i".to_string(), pinkdb::DataType::Integer),
            ("g".to_string(), pinkdb::DataType::Bigint),
        ],
    )
    .unwrap();
    for i in first..first + ROWS_PER_FILE {
        table
            .append_row(vec![Value::Integer(i), Value::Bigint(i as i64 * 2)])
            .unwrap();
    }
    let mut commit = Commit::new();
    commit.append("main.t", Arc::new(table));
    lake.commit(&commit).unwrap();
}

fn lake_with_files(tag: &str) -> (PathBuf, Lake) {
    let dir = scratch(tag);
    let lake_dir = dir.join("lake");
    let mut lake = create_lake(&lake_dir).unwrap();
    lake.set_data_inlining_row_limit(0);
    for file in 0..FILES {
        append_file(&mut lake, file * ROWS_PER_FILE);
    }
    (dir, lake)
}

#[test]
fn an_aggregate_over_a_lake_streams_data_files_into_the_fold() {
    let (dir, _lake) = lake_with_files("aggregate");
    let files = entry(&dir.join("lake"), "main.t")["files"]
        .as_array()
        .unwrap()
        .len();
    assert_eq!(files, FILES as usize, "one data file per commit");

    let report = open_lake(dir.join("lake")).unwrap().attach(None).unwrap();
    let mut db = PinkDb::new();
    db.attach_lake(report.lake).unwrap();

    // Pin the fold's parallelism so the bound below is exact: the fold holds
    // one chunk per worker being folded plus `workers` queued and one blocked
    // on the hand-off. Nothing in the bound depends on the table's size.
    // SAFETY: this test binary has a single test.
    unsafe { std::env::set_var("PINKDB_STREAM_WORKERS", "2") };
    let bound = (2 * PinkDb::stream_fold_workers() + 1) * pinkdb::CHUNK_SIZE;

    db.reset_stream_stats();
    let rows = query(&mut db, "SELECT count(*), sum(g) FROM t WHERE i % 5 <> 0;");
    let (peak, folded) = db.stream_stats();
    let total = (FILES * ROWS_PER_FILE) as u64;
    let kept = (0..total).filter(|i| i % 5 != 0).count() as u64;
    assert_eq!(
        rows,
        vec![vec![
            Value::Bigint(kept as i64),
            Value::Hugeint(
                (0..total)
                    .filter(|i| i % 5 != 0)
                    .map(|i| i128::from(i) * 2)
                    .sum()
            ),
        ]]
    );
    // The counter counts what the scan handed to the fold (the filter runs
    // inside the fold workers, so it is the pre-filter row count).
    assert_eq!(
        folded, total,
        "the fold did not see every row the scan pushed"
    );
    assert!(
        peak <= bound,
        "the fold held {peak} rows at once (bound {bound}): the lake was \
         materialized, not streamed"
    );

    // Zone-map pruning still applies to the streaming path: an `i <` bound
    // skips whole files, and the count is still exact.
    db.reset_stream_stats();
    let rows = query(&mut db, "SELECT count(*), sum(g) FROM t WHERE i < 700;");
    let (peak, folded) = db.stream_stats();
    assert_eq!(
        rows,
        vec![vec![Value::Bigint(700), Value::Hugeint(700 * 699)]]
    );
    // Only the first file (rows 0..2999) survives the zone-map prune, so the
    // fold sees that file whole and the filter keeps 700 of its rows.
    assert_eq!(folded, ROWS_PER_FILE as u64);
    assert!(peak <= bound, "the pruned fold held {peak} rows at once");

    // A full read (not an aggregate) still materializes the same table.
    let rows = query(&mut db, "SELECT count(*), sum(g) FROM t;");
    assert_eq!(
        rows,
        vec![vec![
            Value::Bigint(total as i64),
            Value::Hugeint((0..total).map(|i| i128::from(i) * 2).sum()),
        ]]
    );
    drop(db);
    let _ = std::fs::remove_dir_all(Path::new(&dir));
}
