//! The QuackLab dialect wave's engine-side contracts (the DuckDB-parity rows
//! live in `bench/duckdiff.py`'s dialect battery).
//!
//! The WAL stores each statement's text and replay re-executes it, so the
//! clock (`now()`, `CURRENT_TIMESTAMP`) must replay as the instant the
//! original statement read, a transaction must read one instant, and
//! `uuid()` — which no replay could reproduce — is refused in a logged
//! statement. A crash is modeled by dropping the store without a checkpoint
//! and reopening it: the tail replays.

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
            "pinkdb_quacklab_dialect_{tag}_{}_{}",
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

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("{sql}: {other:?}"),
    }
}

#[test]
fn the_clock_replays_as_the_instant_the_statement_read() {
    let scratch = Scratch::new("clock");
    let before;
    {
        let mut db = open(&scratch);
        run(
            &mut db,
            "CREATE TABLE b (id VARCHAR, started TIMESTAMP, day DATE);",
        );
        run(
            &mut db,
            "INSERT INTO b VALUES ('one', CURRENT_TIMESTAMP, current_date);",
        );
        // A transaction reads one instant across its statements.
        run(&mut db, "BEGIN;");
        run(&mut db, "INSERT INTO b VALUES ('two', now(), NULL);");
        run(&mut db, "INSERT INTO b VALUES ('three', now(), NULL);");
        run(&mut db, "COMMIT;");
        run(&mut db, "UPDATE b SET day = today() WHERE id = 'two';");
        before = rows(&mut db, "SELECT * FROM b ORDER BY id;");
    }
    assert_eq!(before[1][1], before[2][1], "one instant per transaction");
    std::thread::sleep(std::time::Duration::from_millis(5));
    let mut db = open(&scratch);
    assert_eq!(rows(&mut db, "SELECT * FROM b ORDER BY id;"), before);
}

#[test]
fn uuid_is_refused_in_a_logged_statement_and_fine_in_a_read() {
    let mut db = PinkDb::new();
    run(&mut db, "CREATE TABLE u (id VARCHAR);");
    let err = db
        .execute("INSERT INTO u SELECT uuid();")
        .unwrap_err()
        .to_string();
    assert!(err.contains("uuid() is volatile"), "{err}");
    let out = rows(
        &mut db,
        "SELECT count(DISTINCT gen_random_uuid()), min(length(uuid())) FROM range(100);",
    );
    assert_eq!(out, vec![vec![Value::Bigint(100), Value::Integer(36)]]);
}

#[test]
fn column_defaults_are_expressions_and_a_clock_default_uses_the_statement() {
    let scratch = Scratch::new("default");
    {
        let mut db = open(&scratch);
        run(
            &mut db,
            "CREATE TABLE d (a INTEGER DEFAULT 1 + 1, b VARCHAR NOT NULL DEFAULT 'x', c INTEGER);",
        );
        run(&mut db, "INSERT INTO d (c) VALUES (1);");
    }
    // The defaults survive a replay (the CREATE TABLE re-runs) and a
    // checkpoint (the `.pink` image stores them).
    let mut db = open(&scratch);
    run(&mut db, "INSERT INTO d (c) VALUES (2);");
    db.checkpoint().unwrap();
    drop(db);
    let mut db = open(&scratch);
    run(&mut db, "INSERT INTO d (c) VALUES (3);");
    assert_eq!(
        rows(&mut db, "SELECT a, b, c FROM d ORDER BY c;"),
        (1..=3)
            .map(|c| vec![
                Value::Integer(2),
                Value::Varchar("x".into()),
                Value::Integer(c)
            ])
            .collect::<Vec<_>>()
    );
    // A clock default is an expression: CREATE succeeds, and every row that
    // takes the default reads the INSERT statement's one clock instant.
    run(&mut db, "CREATE TABLE e (t TIMESTAMP DEFAULT now());");
    run(&mut db, "INSERT INTO e DEFAULT VALUES;");
    let first = match rows(&mut db, "SELECT t FROM e;")[0][0] {
        Value::Timestamp(v) => v,
        ref other => panic!("expected TIMESTAMP, got {other:?}"),
    };
    std::thread::sleep(std::time::Duration::from_millis(2));
    run(&mut db, "INSERT INTO e DEFAULT VALUES;");
    let all = rows(&mut db, "SELECT t FROM e ORDER BY t;");
    assert_eq!(all.len(), 2);
    let second = match all[1][0] {
        Value::Timestamp(v) => v,
        ref other => panic!("expected TIMESTAMP, got {other:?}"),
    };
    assert!(
        second > first,
        "clock default did not advance: {first} -> {second}"
    );
}

#[test]
fn create_or_replace_keeps_the_last_definition_after_replay() {
    let scratch = Scratch::new("replace");
    {
        let mut db = open(&scratch);
        run(&mut db, "CREATE TABLE r AS SELECT 1 AS a;");
        run(
            &mut db,
            "CREATE OR REPLACE TABLE r AS SELECT a + 1 AS a FROM r;",
        );
        run(&mut db, "CREATE OR REPLACE TABLE s (x INTEGER);");
        run(&mut db, "CREATE OR REPLACE TABLE s (y VARCHAR);");
        run(&mut db, "INSERT INTO s VALUES ('k');");
    }
    let mut db = open(&scratch);
    assert_eq!(
        rows(&mut db, "SELECT * FROM r;"),
        vec![vec![Value::Integer(2)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT y FROM s;"),
        vec![vec![Value::Varchar("k".into())]]
    );
    let err = db
        .execute("CREATE OR REPLACE TABLE IF NOT EXISTS s (z INTEGER);")
        .unwrap_err()
        .to_string();
    assert!(err.contains("OR REPLACE and IF NOT EXISTS"), "{err}");
}

#[test]
fn create_temp_is_refused_with_a_reason() {
    let mut db = PinkDb::new();
    let err = db
        .execute("CREATE TEMP TABLE t (a INTEGER);")
        .unwrap_err()
        .to_string();
    assert!(err.contains("CREATE TEMP is not supported"), "{err}");
}
