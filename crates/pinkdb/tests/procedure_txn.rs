//! Wave 83: `EXCEPTION WHEN OTHERS` handlers, `COMMIT`/`ROLLBACK` inside a
//! standalone `CALL`, `DO` blocks, and how committed segments reach the WAL.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{Database, PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_ptxn_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    fn path(&self) -> String {
        self.dir.join("db.pink").to_string_lossy().to_string()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn run(db: &mut PinkDb, sql: &str) -> QueryResult {
    db.execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .pop()
        .unwrap()
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql)
        .err()
        .unwrap_or_else(|| panic!("expected an error from {sql}"))
        .to_string()
}

fn ints(db: &mut PinkDb, sql: &str) -> Vec<i64> {
    match run(db, sql) {
        QueryResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => i64::from(*i),
                Value::Bigint(i) => *i,
                other => panic!("expected an integer, got {other:?}"),
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn texts(db: &mut PinkDb, sql: &str) -> Vec<String> {
    match run(db, sql) {
        QueryResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|r| match &r[0] {
                Value::Varchar(s) => s.clone(),
                other => panic!("expected text, got {other:?}"),
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (a INTEGER); CREATE TABLE log (msg VARCHAR);")
        .unwrap();
    db
}

// ---------------------------------------------------------------------------
// EXCEPTION handlers
// ---------------------------------------------------------------------------

#[test]
fn a_handler_undoes_its_block_and_sees_the_message() {
    let mut db = store();
    db.execute(
        "CREATE PROCEDURE p() AS $$
         BEGIN
           DECLARE n INTEGER DEFAULT 0;
           INSERT INTO t VALUES (1);
           BEGIN
             INSERT INTO t VALUES (2);
             SET n = 5;
             CREATE TABLE inside (x INTEGER);
             INSERT INTO t VALUES (1 / 0);
           EXCEPTION WHEN OTHERS THEN
             INSERT INTO log VALUES (SQLERRM);
             INSERT INTO t VALUES (n);
           END;
           INSERT INTO t VALUES (3);
         END $$;",
    )
    .unwrap();
    run(&mut db, "CALL p();");
    // Row 2 and the table were rolled back by the savepoint; the variable
    // kept its value (plpgsql: variables are not rolled back).
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 3, 5]);
    assert!(db.execute("SELECT * FROM inside;").is_err());
    let msgs = texts(&mut db, "SELECT msg FROM log;");
    assert_eq!(msgs.len(), 1);
    assert!(
        msgs[0].to_lowercase().contains("division by zero"),
        "{msgs:?}"
    );
}

#[test]
fn handlers_nest_return_and_rethrow() {
    let mut db = store();
    db.execute(
        "CREATE PROCEDURE inner_fail() AS $$
         BEGIN
           BEGIN
             INSERT INTO t VALUES (10);
             RAISE 'inner';
           EXCEPTION WHEN OTHERS THEN
             INSERT INTO t VALUES (11);
             RAISE 'from handler';
           END;
         END $$;",
    )
    .unwrap();
    // A handler's own error propagates; the whole CALL stays atomic.
    let msg = err(&mut db, "CALL inner_fail();");
    assert!(msg.contains("from handler"), "{msg}");
    assert!(ints(&mut db, "SELECT a FROM t;").is_empty());

    db.execute(
        "CREATE PROCEDURE outer_ok() AS $$
         BEGIN
           BEGIN
             CALL inner_fail();
           EXCEPTION WHEN OTHERS THEN
             INSERT INTO log VALUES ('caught');
             RETURN 42;
           END;
           INSERT INTO t VALUES (99);
         END $$;",
    )
    .unwrap();
    match run(&mut db, "CALL outer_ok();") {
        QueryResult::Called {
            result: Some(r), ..
        } => match *r {
            QueryResult::Selected { rows, .. } => assert_eq!(rows, vec![vec![Value::Integer(42)]]),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
    assert!(
        ints(&mut db, "SELECT a FROM t;").is_empty(),
        "RETURN skipped 99"
    );
    assert_eq!(texts(&mut db, "SELECT msg FROM log;"), vec!["caught"]);
}

#[test]
fn handler_syntax_is_checked_at_create_time() {
    let mut db = store();
    for (body, want) in [
        (
            "BEGIN INSERT INTO t VALUES (1); EXCEPTION WHEN division_by_zero THEN INSERT INTO t VALUES (2); END",
            "WHEN OTHERS",
        ),
        (
            "BEGIN INSERT INTO t VALUES (1); EXCEPTION INSERT INTO t VALUES (2); END",
            "WHEN OTHERS",
        ),
        (
            "BEGIN BEGIN COMMIT; EXCEPTION WHEN OTHERS THEN INSERT INTO t VALUES (2); END; END",
            "EXCEPTION",
        ),
        (
            "BEGIN BEGIN INSERT INTO t VALUES (1); EXCEPTION WHEN OTHERS THEN ROLLBACK; END; END",
            "EXCEPTION",
        ),
    ] {
        let msg = err(&mut db, &format!("CREATE PROCEDURE bad() AS $$ {body} $$;"));
        assert!(msg.contains(want), "{body}: {msg}");
    }
    // A top-level handler (the whole body guarded) is fine.
    db.execute(
        "CREATE PROCEDURE top() AS $$ BEGIN INSERT INTO t VALUES (1 / 0); \
         EXCEPTION WHEN OTHERS THEN INSERT INTO t VALUES (7); END $$;",
    )
    .unwrap();
    run(&mut db, "CALL top();");
    assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![7]);
}

/// Recursion through guarded blocks stays within a 2 MiB test-thread stack
/// in debug builds (the savepoint clone is kept out of the recursive frame).
/// Depth 25 leaves debug headroom; release builds run the full
/// `MAX_CALL_DEPTH` (32) through guarded blocks in 512 KiB — see
/// `docs/GAPS.md` for the debug-build limit.
#[test]
fn deep_recursion_through_guarded_blocks_does_not_overflow() {
    let mut db = store();
    db.execute(
        "CREATE PROCEDURE r(n INTEGER) AS $$
         BEGIN
           BEGIN
             IF n < 25 THEN CALL r(n + 1); END IF;
             INSERT INTO t VALUES (n);
           EXCEPTION WHEN OTHERS THEN
             INSERT INTO log VALUES (SQLERRM);
           END;
         END $$;",
    )
    .unwrap();
    run(&mut db, "CALL r(0);");
    assert_eq!(ints(&mut db, "SELECT count(*) FROM t;"), vec![26]);
}

// ---------------------------------------------------------------------------
// COMMIT / ROLLBACK
// ---------------------------------------------------------------------------

#[test]
fn commit_keeps_work_when_the_call_later_fails() {
    let mut db = store();
    db.execute(
        "CREATE PROCEDURE batches() AS $$
         BEGIN
           INSERT INTO t VALUES (1);
           COMMIT;
           INSERT INTO t VALUES (2);
           ROLLBACK;
           INSERT INTO t VALUES (3);
           COMMIT;
           INSERT INTO t VALUES (4);
           RAISE 'boom';
         END $$;",
    )
    .unwrap();
    let msg = err(&mut db, "CALL batches();");
    assert!(msg.contains("boom"), "{msg}");
    // 1 and 3 were committed; 2 was rolled back; 4 died with the failure.
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 3]);
}

#[test]
fn transaction_control_is_refused_at_a_different_scope() {
    let mut db = store();
    db.execute("CREATE PROCEDURE c() AS $$ BEGIN INSERT INTO t VALUES (1); COMMIT; END $$;")
        .unwrap();
    db.execute("BEGIN;").unwrap();
    db.execute("INSERT INTO t VALUES (100);").unwrap();
    let msg = err(&mut db, "CALL c();");
    assert!(msg.contains("different scope"), "{msg}");
    // The failed CALL is undone by itself; the transaction stays open.
    db.execute("COMMIT;").unwrap();
    assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![100]);

    // Nested: only the outermost procedure may commit.
    db.execute("CREATE PROCEDURE outer_c() AS $$ BEGIN CALL c(); END $$;")
        .unwrap();
    let msg = err(&mut db, "CALL outer_c();");
    assert!(msg.contains("outermost"), "{msg}");

    // DO blocks are atomic.
    let msg = err(
        &mut db,
        "DO $$ BEGIN INSERT INTO t VALUES (5); COMMIT; END $$;",
    );
    assert!(msg.contains("DO block"), "{msg}");
    assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![100]);
}

#[test]
fn do_blocks_run_atomically() {
    let mut db = store();
    run(
        &mut db,
        "DO $$ BEGIN DECLARE i INTEGER DEFAULT 0; WHILE i < 3 DO SET i = i + 1; \
         INSERT INTO t VALUES (i); END WHILE; END $$;",
    );
    assert_eq!(ints(&mut db, "SELECT a FROM t ORDER BY a;"), vec![1, 2, 3]);
    let msg = err(
        &mut db,
        "DO 'BEGIN INSERT INTO t VALUES (9); RAISE ''nope''; END';",
    );
    assert!(msg.contains("nope"), "{msg}");
    assert_eq!(ints(&mut db, "SELECT count(*) FROM t;"), vec![3]);
    // `do` stays usable as a name.
    db.execute("CREATE TABLE do (x INTEGER); INSERT INTO do VALUES (1);")
        .unwrap();
    assert_eq!(ints(&mut db, "SELECT x FROM do;"), vec![1]);
}

// ---------------------------------------------------------------------------
// Durability and replay
// ---------------------------------------------------------------------------

/// Committed segments are logged as self-contained blocks: replay never
/// re-runs the procedure, so dropping or redefining it afterwards changes
/// nothing, and the failed tail never comes back.
#[test]
fn committed_segments_replay_without_the_procedure() {
    let s = Scratch::new("segments");
    {
        let mut db = PinkDb::open_durable(&s.path()).unwrap();
        db.execute("CREATE TABLE t (a INTEGER, tag VARCHAR);")
            .unwrap();
        db.execute(
            "CREATE PROCEDURE fill(n INTEGER, tag VARCHAR) AS $$
             BEGIN
               DECLARE i INTEGER DEFAULT 0;
               WHILE i < n DO
                 SET i = i + 1;
                 INSERT INTO t VALUES (i, tag);
                 IF i % 2 = 0 THEN COMMIT; END IF;
               END WHILE;
               RAISE 'stop after the loop';
             END $$;",
        )
        .unwrap();
        let msg = err(&mut db, "CALL fill(5, 'it''s');");
        assert!(msg.contains("stop after"), "{msg}");
        assert_eq!(
            ints(&mut db, "SELECT a FROM t ORDER BY a;"),
            vec![1, 2, 3, 4]
        );
        db.execute("DROP PROCEDURE fill;").unwrap();
        db.execute(
            "CREATE PROCEDURE fill(n INTEGER, tag VARCHAR) AS $$ BEGIN DELETE FROM t; END $$;",
        )
        .unwrap();
    }
    let mut db = PinkDb::open_durable(&s.path()).unwrap();
    assert_eq!(
        ints(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![1, 2, 3, 4]
    );
    assert_eq!(
        texts(&mut db, "SELECT DISTINCT tag FROM t;"),
        vec!["it's".to_string()]
    );
    // And once more through a checkpoint.
    db.checkpoint().unwrap();
    drop(db);
    let mut db = PinkDb::open_durable(&s.path()).unwrap();
    assert_eq!(ints(&mut db, "SELECT count(*) FROM t;"), vec![4]);
}

/// A successful call that used transaction control logs its final segment
/// (not its `CALL` text): exact values of every awkward type and name
/// survive replay, and a guarded block's rolled-back statements never reach
/// the WAL.
#[test]
fn final_segments_and_handlers_replay_exactly() {
    let s = Scratch::new("final");
    let before;
    {
        let mut db = PinkDb::open_durable(&s.path()).unwrap();
        db.execute(
            "CREATE TABLE v (d DOUBLE, x DECIMAL(10,3), dt DATE, iv INTERVAL, s VARCHAR, b BIGINT);",
        )
        .unwrap();
        db.execute(
            r#"CREATE PROCEDURE kinds() AS $$
               BEGIN
                 DECLARE "dätä x" VARCHAR DEFAULT 'quote '' and ; end';
                 DECLARE d DOUBLE DEFAULT CAST('NaN' AS DOUBLE);
                 DECLARE x DECIMAL(10,3) DEFAULT 1.125;
                 DECLARE dt DATE DEFAULT DATE '2026-10-21';
                 DECLARE iv INTERVAL DEFAULT INTERVAL '1 day';
                 DECLARE b BIGINT DEFAULT -9223372036854775807 - 1;
                 INSERT INTO v VALUES (d, x, dt, iv, "dätä x", b);
                 COMMIT;
                 BEGIN
                   INSERT INTO v VALUES (0.0, 0, dt, iv, 'rolled back', 0);
                   RAISE 'undo me';
                 EXCEPTION WHEN OTHERS THEN
                   SET d = -0.0;
                 END;
                 INSERT INTO v VALUES (d, x * 2, dt, iv, "dätä x" || '!', b);
               END $$;"#,
        )
        .unwrap();
        run(&mut db, "CALL kinds();");
        before = match run(&mut db, "SELECT * FROM v ORDER BY s;") {
            QueryResult::Selected { rows, .. } => rows,
            other => panic!("{other:?}"),
        };
        assert_eq!(before.len(), 2);
    }
    let mut db = PinkDb::open_durable(&s.path()).unwrap();
    let after = match run(&mut db, "SELECT * FROM v ORDER BY s;") {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("{other:?}"),
    };
    // NaN != NaN, so compare the debug rendering (bit-exact payloads).
    assert_eq!(format!("{after:?}"), format!("{before:?}"));
}

/// The `Connection` front-end (autocommit and explicit transactions) follows
/// the same rules.
#[test]
fn connections_follow_the_same_rules() {
    let s = Scratch::new("conn");
    {
        let store = Database::open(&s.path()).unwrap();
        let mut c = store.connect();
        c.execute("CREATE TABLE t (a INTEGER);").unwrap();
        c.execute(
            "CREATE PROCEDURE c() AS $$ BEGIN INSERT INTO t VALUES (1); COMMIT; \
             INSERT INTO t VALUES (2); RAISE 'late'; END $$;",
        )
        .unwrap();
        assert!(c.execute("CALL c();").is_err());
        c.execute("BEGIN;").unwrap();
        let msg = c.execute("CALL c();").unwrap_err().to_string();
        assert!(msg.contains("different scope"), "{msg}");
        c.execute("ROLLBACK;").unwrap();
    }
    let mut db = PinkDb::open_durable(&s.path()).unwrap();
    assert_eq!(ints(&mut db, "SELECT a FROM t;"), vec![1]);
}
