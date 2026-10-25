//! Wave 95c: `SET VARIABLE` state is session state.
//!
//! Two properties matter for a durable store: a variable is never written to
//! the WAL (replay re-binds the logged text against a fresh session, so it
//! could not reproduce the value), and a logged statement may therefore not
//! *read* one — the binder refuses that by name. A crash is modeled by
//! dropping the store without a checkpoint and reopening it.

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
            "pinkdb_session_vars_{tag}_{}_{}",
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

    fn open(&self) -> PinkDb {
        PinkDb::open_durable_with(self.path(), SyncPolicy::Batch)
            .unwrap()
            .0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

fn rows(results: Vec<QueryResult>) -> Vec<Vec<Value>> {
    match results.into_iter().last().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected Selected, got {other:?}"),
    }
}

#[test]
fn variables_are_session_state_and_never_logged() {
    let scratch = Scratch::new("replay");
    {
        let mut db = scratch.open();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("SET VARIABLE x = 5;").unwrap();
        // A write that does not read the variable is logged as usual.
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        assert_eq!(
            rows(db.execute("SELECT $x;").unwrap()),
            vec![vec![Value::Integer(5)]]
        );
        // A logged statement may not read one: replay would bind the text
        // against a fresh session and write a different row.
        for sql in [
            "INSERT INTO t VALUES ($x);",
            "INSERT INTO t SELECT getvariable('x');",
            "CREATE TABLE u AS SELECT $x AS a;",
        ] {
            let err = db.execute(sql).unwrap_err().to_string();
            assert!(
                err.contains("session state") && err.contains("WAL-logs"),
                "{sql}: {err}"
            );
        }
        // A query may read one freely.
        assert_eq!(
            rows(db.execute("SELECT count(*) FROM t WHERE a < $x;").unwrap()),
            vec![vec![Value::Bigint(1)]]
        );
    }
    // Crash: drop without a checkpoint and reopen. The logged INSERT replays;
    // the variable was never logged, so the fresh session does not have it.
    let mut db = scratch.open();
    assert_eq!(
        rows(db.execute("SELECT a FROM t;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    assert_eq!(
        rows(db.execute("SELECT getvariable('x') IS NULL;").unwrap()),
        vec![vec![Value::Boolean(true)]]
    );
    assert!(
        db.execute("SELECT $x;")
            .unwrap_err()
            .to_string()
            .contains("Variable x does not exist")
    );
}

#[test]
fn a_checkpoint_keeps_session_state_out_of_the_file() {
    let scratch = Scratch::new("checkpoint");
    let mut db = scratch.open();
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.execute("SET VARIABLE secret = 'tenant-a';").unwrap();
    db.execute("INSERT INTO t VALUES (1);").unwrap();
    db.execute("CHECKPOINT;").unwrap();
    // The variable is not in the catalog: a query against the store sees the
    // table, and `SHOW TABLES` never lists a variable.
    assert_eq!(
        rows(db.execute("SHOW TABLES;").unwrap()),
        vec![vec![Value::Varchar("t".to_string())]]
    );
    drop(db);
    let mut db = scratch.open();
    assert_eq!(
        rows(db.execute("SELECT getvariable('secret') IS NULL;").unwrap()),
        vec![vec![Value::Boolean(true)]]
    );
}
