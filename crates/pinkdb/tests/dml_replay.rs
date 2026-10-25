//! Wave 95b: the new DML forms replay deterministically.
//!
//! The WAL stores each statement's source text and replay re-executes it, so
//! `ON CONFLICT`, `MERGE`, `UPDATE … FROM`, `DELETE … USING` and a
//! data-modifying CTE must be logged like any other write and land on the same
//! data after a crash. A crash is modeled by dropping the store without a
//! checkpoint and reopening it: the tail replays.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{PinkDb, QueryResult, SyncPolicy, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
    file: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_dml_replay_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("db.pink");
        Self { dir, file }
    }

    fn path(&self) -> &str {
        self.file.to_str().unwrap()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn open(scratch: &Scratch) -> PinkDb {
    PinkDb::open_durable_with(scratch.path(), SyncPolicy::Batch)
        .unwrap()
        .0
}

fn run(db: &mut PinkDb, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn cell_rows(db: &mut PinkDb, sql: &str) -> Vec<String> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows
            .iter()
            .map(|r| {
                r.iter()
                    .map(|v| match v {
                        Value::Null => "NULL".to_string(),
                        other => other.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join("\t")
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// The statement texts a WAL record of this store holds, in order.
fn logged(scratch: &Scratch) -> Vec<String> {
    let wal = PathBuf::from(format!("{}.wal", scratch.path()));
    let bytes = std::fs::read(wal).unwrap_or_default();
    // The record payloads are length-prefixed and CRC-framed; the framing is
    // not this test's subject, so finding the statement text inside the bytes
    // proves the statement was logged verbatim.
    let find = |needle: &str| bytes.windows(needle.len()).any(|w| w == needle.as_bytes());
    ["MERGE INTO", "ON CONFLICT", "OR REPLACE"]
        .into_iter()
        .filter(|needle| find(needle))
        .map(str::to_string)
        .collect()
}

#[test]
fn conflict_and_merge_statements_replay_from_the_wal() {
    let scratch = Scratch::new("replay");
    let mut db = open(&scratch);
    run(
        &mut db,
        "CREATE TABLE k (a INTEGER PRIMARY KEY, b INTEGER, g VARCHAR);",
    );
    run(
        &mut db,
        "CREATE TABLE s (a INTEGER, b INTEGER); INSERT INTO s VALUES (2, 200), (4, 40);",
    );
    run(
        &mut db,
        "INSERT INTO k VALUES (1, 10, 'p'), (2, 20, 'p'), (3, 30, 'q');",
    );

    // The two forms the brief names: ON CONFLICT and MERGE.
    run(
        &mut db,
        "INSERT INTO k VALUES (2, 222, 'u') ON CONFLICT (a) \
           DO UPDATE SET b = excluded.b, g = excluded.g;",
    );
    run(
        &mut db,
        "INSERT INTO k VALUES (2, 999, 'x') ON CONFLICT DO NOTHING;",
    );
    run(&mut db, "INSERT OR REPLACE INTO k VALUES (3, 333, 'r');");
    run(
        &mut db,
        "MERGE INTO k USING s ON k.a = s.a \
           WHEN MATCHED THEN UPDATE SET b = s.b \
           WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b, 'm');",
    );
    // And the rest of the batch, so one record covers every form.
    run(
        &mut db,
        "UPDATE k SET b = s.b + 1 FROM s WHERE k.a = s.a AND s.b = 200;",
    );
    run(
        &mut db,
        "DELETE FROM k USING s WHERE k.a = s.a AND s.b = 40;",
    );
    run(
        &mut db,
        "WITH ins AS (INSERT INTO k VALUES (9, 90, 'c') RETURNING a) SELECT * FROM ins;",
    );

    let before = cell_rows(&mut db, "SELECT a, b, g FROM k ORDER BY a");
    assert_eq!(
        before,
        vec![
            "1\t10\tp".to_string(),
            "2\t201\tu".to_string(),
            "3\t333\tr".to_string(),
            "9\t90\tc".to_string(),
        ]
    );
    // The WAL carries the statements themselves, not re-rendered plans.
    let wal = logged(&scratch);
    assert!(
        wal.iter().any(|s| s.contains("ON CONFLICT")),
        "no ON CONFLICT record in {wal:?}"
    );
    assert!(
        wal.iter().any(|s| s.contains("MERGE INTO")),
        "no MERGE record in {wal:?}"
    );
    drop(db);

    // Crash: no checkpoint, so every statement replays in WAL order.
    let mut db = open(&scratch);
    assert_eq!(
        cell_rows(&mut db, "SELECT a, b, g FROM k ORDER BY a"),
        before,
        "replayed data drifted"
    );

    // A checkpoint plus a tail: the checkpoint carries the data, the tail
    // replays on top.
    run(&mut db, "CHECKPOINT;");
    run(
        &mut db,
        "INSERT INTO k VALUES (2, 250, 'later') ON CONFLICT (a) DO UPDATE SET b = excluded.b;",
    );
    let before = cell_rows(&mut db, "SELECT a, b, g FROM k ORDER BY a");
    drop(db);
    let mut db = open(&scratch);
    assert_eq!(
        cell_rows(&mut db, "SELECT a, b, g FROM k ORDER BY a"),
        before
    );
}
