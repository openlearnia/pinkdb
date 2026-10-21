//! JavaScript procedures end to end (wave 81): `CREATE PROCEDURE … LANGUAGE
//! JAVASCRIPT` + `CALL` through the engine, against the same rules as the
//! SQL procedural language — bound parameters, statement atomicity,
//! refusals, WAL replay and `.pink` persistence. Needs `--features js`.
#![cfg(feature = "js")]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_js_{tag}_{}_{}",
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

fn last(db: &mut PinkDb, sql: &str) -> QueryResult {
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

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match last(db, sql) {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// `(output, returned value)` of one `CALL`.
fn call(db: &mut PinkDb, sql: &str) -> (Vec<String>, Option<Value>) {
    match last(db, sql) {
        QueryResult::Called { output, result, .. } => {
            let value = result.map(|r| match *r {
                QueryResult::Selected { mut rows, .. } => rows.remove(0).remove(0),
                other => panic!("expected a returned value, got {other:?}"),
            });
            (output, value)
        }
        other => panic!("expected a CALL result from {sql}, got {other:?}"),
    }
}

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, name VARCHAR);")
        .unwrap();
    db
}

#[test]
fn a_procedure_writes_reads_prints_and_returns() {
    let mut db = store();
    db.execute(
        "CREATE PROCEDURE add_many(n INTEGER, prefix VARCHAR) LANGUAGE JAVASCRIPT AS $$
           for (let i = 1; i <= n; i++) {
             db.exec('INSERT INTO t VALUES (?, ?)', [i, prefix + i]);
           }
           const total = db.value('SELECT sum(id) FROM t');
           print('inserted', n, 'rows; sum =', total);
           return total;
         $$;",
    )
    .unwrap();
    let (output, value) = call(&mut db, "CALL add_many(4, 'r');");
    assert_eq!(output, vec!["inserted 4 rows; sum = 10"]);
    assert_eq!(value, Some(Value::Integer(10)));
    assert_eq!(
        rows(&mut db, "SELECT name FROM t ORDER BY id DESC LIMIT 1;"),
        vec![vec![Value::Varchar("r4".into())]]
    );
    // db.query returns rows as arrays; a body without `return` has no result.
    db.execute(
        "CREATE PROCEDURE names() LANGUAGE JAVASCRIPT AS $$
           for (const [id, name] of db.query('SELECT id, name FROM t WHERE id <= ? ORDER BY id', [2])) {
             print(id, name);
           }
         $$;",
    )
    .unwrap();
    let (output, value) = call(&mut db, "CALL names();");
    assert_eq!(output, vec!["1 r1", "2 r2"]);
    assert_eq!(value, None);
}

#[test]
fn parameters_are_values_never_sql_text() {
    let mut db = store();
    db.execute(
        "CREATE PROCEDURE put(s VARCHAR) LANGUAGE JAVASCRIPT AS $$
           return db.exec('INSERT INTO t VALUES (1, ?)', [s]);
         $$;",
    )
    .unwrap();
    let evil = "x'); DROP TABLE t; --";
    let (_, affected) = call(
        &mut db,
        &format!("CALL put('{}');", evil.replace('\'', "''")),
    );
    assert_eq!(affected, Some(Value::Integer(1)));
    assert_eq!(
        rows(&mut db, "SELECT name FROM t;"),
        vec![vec![Value::Varchar(evil.into())]]
    );
}

#[test]
fn a_failing_call_leaves_no_trace_and_keeps_the_transaction() {
    let mut db = store();
    db.execute(
        "CREATE PROCEDURE half() LANGUAGE JAVASCRIPT AS $$
           db.exec('INSERT INTO t VALUES (1, ?)', ['a']);
           db.exec('CREATE TABLE made_inside (x INTEGER)');
           throw new Error('stop here');
         $$;",
    )
    .unwrap();
    let msg = err(&mut db, "CALL half();");
    assert!(msg.contains("stop here"), "{msg}");
    assert!(rows(&mut db, "SELECT * FROM t;").is_empty());
    assert!(db.execute("SELECT * FROM made_inside;").is_err());

    db.execute("BEGIN;").unwrap();
    db.execute("INSERT INTO t VALUES (9, 'kept');").unwrap();
    assert!(err(&mut db, "CALL half();").contains("stop here"));
    db.execute("COMMIT;").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT id FROM t;"),
        vec![vec![Value::Integer(9)]]
    );

    // A SQL error the script does not catch fails the call the same way; a
    // caught one does not.
    db.execute(
        "CREATE PROCEDURE careful() LANGUAGE JAVASCRIPT AS $$
           try { db.exec('INSERT INTO nope VALUES (1)'); } catch (e) { print(e.name); }
           db.exec('INSERT INTO t VALUES (2, ?)', ['ok']);
         $$;",
    )
    .unwrap();
    let (output, _) = call(&mut db, "CALL careful();");
    assert_eq!(output, vec!["PinkDbError"]);
    assert_eq!(
        rows(&mut db, "SELECT count(*) FROM t;"),
        vec![vec![Value::Bigint(2)]]
    );
}

#[test]
fn host_calls_are_checked_like_procedure_statements() {
    let mut db = store();
    for (body, want) in [
        ("db.exec('BEGIN');", "transaction"),
        ("db.exec('SELECT 1; SELECT 2');", "exactly one statement"),
        (
            "db.exec('INSERT INTO t VALUES (?, ?)', [1]);",
            "not provided",
        ),
        ("db.exec('INSERT INTO t VALUES (?)', [1, 2]);", "mismatch"),
        (
            "db.query('INSERT INTO t VALUES (1, ?)', ['x']);",
            "does not return rows",
        ),
        ("db.exec('CREATE DATABASE other');", "DATABASE"),
        (
            "db.exec('INSERT INTO t VALUES (?, ?)', [1, {}]);",
            "parameter 2",
        ),
    ] {
        db.execute(&format!(
            "CREATE OR REPLACE PROCEDURE bad() LANGUAGE JAVASCRIPT AS $${body}$$;"
        ))
        .unwrap();
        let msg = err(&mut db, "CALL bad();");
        assert!(
            msg.to_lowercase().contains(&want.to_lowercase()),
            "{body}: {msg}"
        );
    }
    assert!(rows(&mut db, "SELECT * FROM t;").is_empty());
    // `db.query` refuses a write *before* running it: catching the error
    // does not leave the write behind.
    db.execute(
        "CREATE OR REPLACE PROCEDURE sneaky() LANGUAGE JAVASCRIPT AS $$
           try { db.query('INSERT INTO t VALUES (1, ?)', ['x']); } catch (e) {}
         $$;",
    )
    .unwrap();
    db.execute("CALL sneaky();").unwrap();
    assert!(rows(&mut db, "SELECT * FROM t;").is_empty());
}

#[test]
fn definitions_are_checked_at_create_time() {
    let mut db = store();
    let msg = err(
        &mut db,
        "CREATE PROCEDURE broken() LANGUAGE JAVASCRIPT AS $$ return (; $$;",
    );
    assert!(msg.contains("broken"), "{msg}");
    let msg = err(
        &mut db,
        "CREATE PROCEDURE kw(class INTEGER) LANGUAGE JAVASCRIPT AS $$ return 1; $$;",
    );
    assert!(msg.contains("class"), "{msg}");
    assert!(db.procedure_in("main", "broken").is_err());
    assert!(db.procedure_in("main", "kw").is_err());
}

#[test]
fn sql_values_cross_with_exact_types() {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE PROCEDURE kinds(d DATE, x DECIMAL(10,2), big BIGINT, f DOUBLE, b BOOLEAN) \
         LANGUAGE JAVASCRIPT AS $$
           return [typeof d, d, typeof x, x, typeof big, String(big), typeof f, typeof b].join('|');
         $$;",
    )
    .unwrap();
    let (_, value) = call(
        &mut db,
        "CALL kinds(DATE '2026-10-21', 12.50, 9007199254740993, 1.5, true);",
    );
    assert_eq!(
        value,
        Some(Value::Varchar(
            "string|2026-10-21|string|12.50|bigint|9007199254740993|number|boolean".into()
        ))
    );
}

#[test]
fn runaway_scripts_fail_cleanly_and_change_nothing() {
    let mut db = store();
    db.execute(
        "CREATE PROCEDURE spin() LANGUAGE JAVASCRIPT AS $$
           db.exec('INSERT INTO t VALUES (1, ?)', ['x']);
           while (true) {}
         $$;",
    )
    .unwrap();
    let msg = err(&mut db, "CALL spin();");
    assert!(msg.contains("step limit"), "{msg}");
    assert!(rows(&mut db, "SELECT * FROM t;").is_empty());
}

#[test]
fn javascript_calls_nest_a_bounded_number_of_levels() {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE PROCEDURE inner_p(n INTEGER) LANGUAGE JAVASCRIPT AS $$
           print('inner', n);
           return n + 1;
         $$;",
    )
    .unwrap();
    db.execute(
        "CREATE PROCEDURE outer_p() LANGUAGE JAVASCRIPT AS $$
           const r = db.query('CALL inner_p(?)', [41]);
           print('outer got', r[0][0]);
         $$;",
    )
    .unwrap();
    let (output, _) = call(&mut db, "CALL outer_p();");
    assert_eq!(
        output,
        vec!["inner 41", "outer got 42"],
        "nested output in order"
    );

    db.execute(
        "CREATE PROCEDURE recurse(n INTEGER) LANGUAGE JAVASCRIPT AS $$
           db.exec('CALL recurse(?)', [n + 1]);
         $$;",
    )
    .unwrap();
    let msg = err(&mut db, "CALL recurse(0);");
    assert!(msg.contains("nest at most"), "{msg}");
}

/// A durable store logs the `CALL`; reopening replays it, and `db.random()`
/// draws the same stream on replay, so the data is identical.
#[test]
fn calls_replay_exactly_after_reopen_and_checkpoint() {
    let s = Scratch::new("replay");
    let before;
    {
        let mut db = PinkDb::open_durable(&s.path()).unwrap();
        db.execute("CREATE TABLE r (k INTEGER, x DOUBLE);").unwrap();
        db.execute(
            "CREATE PROCEDURE fill(n INTEGER) LANGUAGE JAVASCRIPT AS $$
               for (let i = 0; i < n; i++) {
                 db.exec('INSERT INTO r VALUES (?, ?)', [i, db.random()]);
               }
             $$;",
        )
        .unwrap();
        db.execute("CALL fill(5);").unwrap();
        db.execute("CALL fill(2);").unwrap();
        before = rows(&mut db, "SELECT k, x FROM r ORDER BY x;");
        assert_eq!(before.len(), 7);
    }
    {
        let mut db = PinkDb::open_durable(&s.path()).unwrap();
        assert_eq!(rows(&mut db, "SELECT k, x FROM r ORDER BY x;"), before);
        db.checkpoint().unwrap();
    }
    let mut db = PinkDb::open_durable(&s.path()).unwrap();
    assert_eq!(rows(&mut db, "SELECT k, x FROM r ORDER BY x;"), before);
    // The definition came back from `.pink` and still runs.
    db.execute("CALL fill(1);").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT count(*) FROM r;"),
        vec![vec![Value::Bigint(8)]]
    );
}
