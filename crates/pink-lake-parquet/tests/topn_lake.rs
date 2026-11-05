//! Top-N T3c: a running `ORDER BY … LIMIT` cut-off skips Parquet row groups
//! by their footer statistics. Whatever the lake scan skips, every result must
//! equal the same query over a store copy of the rows (ties in input order).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::Lake;
use pink_lake_parquet::{FORMAT_ID, register};
use pinkdb::{PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_topn_{tag}_{}_{}",
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

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// `t (i, k, s)`: `i` rises with the row (narrow row-group bounds, pruning
/// bites), `k` repeats (wide bounds, ties), `s` holds some NULLs.
fn store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE t AS SELECT i, i % 7 AS k, \
         CASE WHEN i % 101 = 0 THEN NULL ELSE 's' || (i % 977) END AS s \
         FROM range(300000) r(i);",
    )
    .unwrap();
    db
}

#[test]
fn a_lake_top_n_skips_row_groups_without_changing_results() {
    register().unwrap();
    let dir = scratch("cut");
    let lake_dir = dir.join("lake");
    let mut reference = store();
    // 300K rows: several 64K-row Parquet row groups in one file.
    let mut lake = Lake::create(&lake_dir).unwrap();
    lake.set_write_format(FORMAT_ID).unwrap();
    lake.write_from_store(&reference).unwrap();
    let mut db = PinkDb::new();
    let attachment = Lake::open(&lake_dir).unwrap().attach(None).unwrap();
    db.attach_lake(attachment.lake).unwrap();
    for (order, take, skip) in [
        ("i DESC", 3, 0),
        ("i", 5, 0),
        ("i DESC", 4, 70_000),
        ("k DESC, i", 6, 0),
        ("k", 10, 3),
        ("s DESC", 5, 0),
        ("s NULLS FIRST", 8, 0),
        ("s DESC NULLS FIRST", 4, 0),
    ] {
        let limited = if skip == 0 {
            format!("SELECT i, k, s FROM t ORDER BY {order} LIMIT {take};")
        } else {
            format!("SELECT i, k, s FROM t ORDER BY {order} LIMIT {take} OFFSET {skip};")
        };
        let full = format!("SELECT i, k, s FROM t ORDER BY {order};");
        let want = query(&mut reference, &full)[skip..skip + take].to_vec();
        assert_eq!(query(&mut db, &limited), want, "{limited}");
    }
    cleanup(&dir);
}
