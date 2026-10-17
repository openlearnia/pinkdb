//! Prepared statements end to end: `?` parsing/positions, bind-time type
//! inference and conversion, the engine bind/execute API on `PinkDb` and
//! `Connection`, transaction interplay, and durable WAL replay of the
//! substituted SQL.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{DataType, Database, PinkDb, PinkDbError, QueryResult, SyncPolicy, Value};

// ---------- scratch durable files ----------

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
    ckpt: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_prep_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        let ckpt = dir.join("db.pink");
        Self { dir, ckpt }
    }

    fn path(&self) -> &str {
        self.ckpt.to_str().unwrap()
    }

    fn wal(&self) -> PathBuf {
        let mut s = self.ckpt.clone().into_os_string();
        s.push(".wal");
        PathBuf::from(s)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ---------- helpers ----------

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn prepared_rows(db: &mut PinkDb, sql: &str, params: &[Value]) -> Vec<Vec<Value>> {
    let stmt = db.prepare(sql).unwrap();
    match db.execute_prepared(&stmt, params).unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn param_err(db: &mut PinkDb, sql: &str, params: &[Value]) -> String {
    let stmt = db.prepare(sql).unwrap();
    db.execute_prepared(&stmt, params)
        .expect_err("bind/execute should fail")
        .to_string()
}

fn one_int(rows: Vec<Vec<Value>>) -> i64 {
    assert_eq!(rows.len(), 1, "{rows:?}");
    match &rows[0][0] {
        Value::Integer(i) => *i as i64,
        Value::Bigint(i) => *i,
        other => panic!("expected an integer, got {other:?}"),
    }
}

// ---------- parser / prepare shape ----------

#[test]
fn prepare_parses_one_statement_and_counts_placeholders() {
    let db = PinkDb::new();
    let stmt = db.prepare("SELECT ?, ?, ?;").unwrap();
    assert_eq!(stmt.parameter_count(), 3);
    assert_eq!(stmt.sql(), "SELECT ?, ?, ?;");
    assert_eq!(db.prepare("SELECT 1;").unwrap().parameter_count(), 0);

    // Exactly one statement: empty, two statements, and a stray trailing one
    // are all refused with a clear parse error.
    for sql in ["", "SELECT 1; SELECT 2;", " ; "] {
        let err = db
            .prepare(sql)
            .expect_err("must be one statement")
            .to_string();
        assert!(err.contains("exactly one statement"), "{sql:?}: {err}");
    }
    // `?` outside a prepared statement is a clear bind-time error, never a
    // parse error.
    let mut db = PinkDb::new();
    let err = db.execute("SELECT ?;").unwrap_err().to_string();
    assert!(
        err.contains("only allowed in a prepared statement"),
        "{err}"
    );
}

#[test]
fn dollar_style_parameters_are_not_accepted() {
    let db = PinkDb::new();
    let err = db
        .prepare("SELECT $1 + 1;")
        .expect_err("no $N support")
        .to_string();
    assert!(err.contains("parse error"), "{err}");
}

#[test]
fn placeholders_number_in_textual_order_across_clauses() {
    // A parameter before a multi-byte literal, a subquery placeholder, and
    // clauses in textual order: the byte offsets/substitution must line up
    // regardless of how the AST nests.
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE t (a INTEGER, b VARCHAR); INSERT INTO t VALUES (1, 'one'), (2, 'two');",
    )
    .unwrap();
    let rows = prepared_rows(
        &mut db,
        "SELECT 'héllo' || ?, (SELECT ? + 1) FROM t WHERE a = ? ORDER BY a;",
        &[
            Value::Varchar("x".into()),
            Value::Integer(40),
            Value::Integer(2),
        ],
    );
    assert_eq!(
        rows,
        vec![vec![Value::Varchar("héllox".into()), Value::Integer(41)]; 1]
    );
}

#[test]
fn placeholders_in_clauses_that_cannot_bind_are_rejected() {
    let db = PinkDb::new();
    for (sql, clause) in [
        ("SELECT a FROM t ORDER BY ?", "ORDER BY"),
        ("SELECT a FROM t GROUP BY ?", "GROUP BY"),
        ("CREATE VIEW v AS SELECT ?", "CREATE VIEW"),
    ] {
        let err = db.prepare(sql).expect_err(clause).to_string();
        assert!(err.contains(clause), "{sql}: {err}");
    }
}

// ---------- inference ----------

#[test]
fn comparison_and_arithmetic_infer_from_the_other_side() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER, big BIGINT, d DOUBLE, v VARCHAR, dt DATE);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (5, 5000000000, 1.5, 'm', DATE '2024-01-10'), (6, 6, 6.5, 'm2', DATE '2024-02-01');")
        .unwrap();

    // `i > ?` takes INTEGER; a numeric string converts (DuckDB-style).
    assert_eq!(
        prepared_rows(
            &mut db,
            "SELECT i FROM t WHERE i > ? ORDER BY i;",
            &[Value::Varchar("5".into())]
        ),
        vec![vec![Value::Integer(6)]]
    );
    // A bad conversion is a clear bind-time error naming the parameter.
    let err = param_err(
        &mut db,
        "SELECT i FROM t WHERE i > ?;",
        &[Value::Varchar("x".into())],
    );
    assert!(err.contains("cannot bind parameter 1 as INTEGER"), "{err}");

    // Arithmetic: `? + 1` infers INTEGER; BIGINT stays exact.
    let stmt = db.prepare("SELECT ? + 1;").unwrap();
    let bound = stmt.bind(&db, "main", &[Value::Bigint(3)]).unwrap();
    assert_eq!(bound.parameter_types(), &[DataType::Bigint]);
    assert_eq!(
        one_int(prepared_rows(&mut db, "SELECT ? + 1;", &[Value::Bigint(3)])),
        4
    );

    // LIKE and `||` pin VARCHAR; AND pins BOOLEAN.
    assert_eq!(
        prepared_rows(
            &mut db,
            "SELECT v FROM t WHERE v LIKE ? ORDER BY v;",
            &[Value::Varchar("%m%".into())]
        ),
        vec![
            vec![Value::Varchar("m".into())],
            vec![Value::Varchar("m2".into())]
        ]
    );
    assert_eq!(
        prepared_rows(&mut db, "SELECT ? || '!' ;", &[Value::Integer(7)]),
        vec![vec![Value::Varchar("7!".into())]]
    );
    assert_eq!(
        prepared_rows(
            &mut db,
            "SELECT 1 WHERE ? AND true;",
            &[Value::Boolean(true)]
        ),
        vec![vec![Value::Integer(1)]]
    );
    // Date arithmetic pins INTEGER days.
    assert_eq!(
        prepared_rows(
            &mut db,
            "SELECT dt + ? FROM t WHERE v = 'm';",
            &[Value::Bigint(1)]
        ),
        vec![vec![Value::Date(
            pinkdb::types::parse_date("2024-01-11").unwrap()
        )]]
    );
}

#[test]
fn cast_pins_the_parameter_type() {
    let mut db = PinkDb::new();
    // CAST pins INTEGER: numeric strings convert, bad ones fail at bind.
    assert_eq!(
        prepared_rows(
            &mut db,
            "SELECT CAST(? AS INTEGER);",
            &[Value::Varchar("42".into())]
        ),
        vec![vec![Value::Integer(42)]]
    );
    // Rounds half away from zero, exactly like CAST(3.7 AS INTEGER).
    assert_eq!(
        prepared_rows(&mut db, "SELECT CAST(? AS INTEGER);", &[Value::Double(3.7)]),
        vec![vec![Value::Integer(4)]]
    );
    let err = param_err(
        &mut db,
        "SELECT CAST(? AS INTEGER);",
        &[Value::Varchar("abc".into())],
    );
    assert!(err.contains("cannot bind parameter 1 as INTEGER"), "{err}");
    // NULL under a CAST keeps its typeless form and still resolves.
    assert_eq!(
        prepared_rows(&mut db, "SELECT CAST(? AS INTEGER);", &[Value::Null]),
        vec![vec![Value::Null]]
    );
    let stmt = db.prepare("SELECT CAST(? AS BIGINT);").unwrap();
    let bound = stmt.bind(&db, "main", &[Value::Null]).unwrap();
    assert_eq!(bound.parameter_types(), &[DataType::Bigint]);
}

#[test]
fn insert_and_update_targets_pin_the_parameter_type() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER, b BIGINT, s VARCHAR, dt DATE);")
        .unwrap();
    let insert = db.prepare("INSERT INTO t VALUES (?, ?, ?, ?);").unwrap();
    insert
        .bind(
            &db,
            "main",
            &[
                Value::Varchar("7".into()),
                Value::Integer(8),
                Value::Integer(9),
                Value::Varchar("2024-03-04".into()),
            ],
        )
        .map(|b| {
            assert_eq!(
                b.parameter_types(),
                &[
                    DataType::Integer,
                    DataType::Bigint,
                    DataType::Varchar,
                    DataType::Date
                ]
            )
        })
        .unwrap();

    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER, b BIGINT, s VARCHAR, dt DATE);")
        .unwrap();
    let insert = db.prepare("INSERT INTO t VALUES (?, ?, ?, ?);").unwrap();
    db.execute_prepared(
        &insert,
        &[
            Value::Varchar("7".into()),
            Value::Integer(8),
            Value::Integer(9),
            Value::Varchar("2024-03-04".into()),
        ],
    )
    .unwrap();
    // The stored values have the column types; the VARCHAR "7" became INTEGER.
    let stmt = db
        .prepare("SELECT i, b, s, dt FROM t WHERE i = ?;")
        .unwrap();
    match db.execute_prepared(&stmt, &[Value::Integer(7)]).unwrap() {
        QueryResult::Selected { rows, types, .. } => {
            assert_eq!(
                types,
                vec![
                    DataType::Integer,
                    DataType::Bigint,
                    DataType::Varchar,
                    DataType::Date
                ]
            );
            assert_eq!(
                rows,
                vec![vec![
                    Value::Integer(7),
                    Value::Bigint(8),
                    Value::Varchar("9".into()),
                    Value::Date(pinkdb::types::parse_date("2024-03-04").unwrap()),
                ]]
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // UPDATE SET infers from the target column too.
    let update = db.prepare("UPDATE t SET i = ? WHERE s = ?;").unwrap();
    let bound = update
        .bind(
            &db,
            "main",
            &[Value::Varchar("11".into()), Value::Integer(9)],
        )
        .unwrap();
    assert_eq!(
        bound.parameter_types(),
        &[DataType::Integer, DataType::Varchar]
    );
    db.execute_bound(&bound).unwrap();
    assert_eq!(one_int(rows(&mut db, "SELECT i FROM t;")), 11);
}

#[test]
fn unresolved_null_parameters_fail_at_bind_with_a_clear_error() {
    let mut db = PinkDb::new();
    // No context, no CAST: NULL cannot be typed.
    let err = param_err(&mut db, "SELECT ?;", &[Value::Null]);
    assert!(
        err.contains("could not determine data type of parameter 1") && err.contains("CAST"),
        "{err}"
    );
    // Two mutually-untyped placeholders are the same case.
    let err = param_err(&mut db, "SELECT ? = ?;", &[Value::Null, Value::Null]);
    assert!(err.contains("could not determine data type"), "{err}");
    // A context type resolves NULL fine (it stays NULL).
    assert_eq!(
        prepared_rows(&mut db, "SELECT ? + 1;", &[Value::Null]),
        vec![vec![Value::Null]]
    );
    // A non-NULL value supplies its own type when no context does.
    assert_eq!(
        prepared_rows(&mut db, "SELECT ?;", &[Value::Varchar("x".into())]),
        vec![vec![Value::Varchar("x".into())]]
    );
}

#[test]
fn rebinding_with_an_incompatible_value_type_errors() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER, v VARCHAR); INSERT INTO t VALUES (1, 'a');")
        .unwrap();
    let stmt = db.prepare("SELECT v FROM t WHERE i = ?;").unwrap();
    assert_eq!(
        db.execute_prepared(&stmt, &[Value::Integer(1)]).unwrap(),
        QueryResult::Selected {
            names: vec!["v".to_string()],
            types: vec![DataType::Varchar],
            rows: vec![vec![Value::Varchar("a".into())]],
        }
    );
    // The second bind converts to the context type: a numeric string works...
    assert_eq!(
        db.execute_prepared(&stmt, &[Value::Varchar("1".into())])
            .unwrap(),
        QueryResult::Selected {
            names: vec!["v".to_string()],
            types: vec![DataType::Varchar],
            rows: vec![vec![Value::Varchar("a".into())]],
        }
    );
    // ...a non-numeric one fails at bind, with the value and target named.
    let err = db
        .execute_prepared(&stmt, &[Value::Varchar("nope".into())])
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("cannot bind parameter 1 as INTEGER") && err.contains("nope"),
        "{err}"
    );
    // The prepared statement is reusable after a failed bind.
    assert!(db.execute_prepared(&stmt, &[Value::Integer(1)]).is_ok());
}

#[test]
fn between_in_and_repeated_uses() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER); INSERT INTO t VALUES (1), (2), (3), (4);")
        .unwrap();
    assert_eq!(
        prepared_rows(
            &mut db,
            "SELECT i FROM t WHERE i BETWEEN ? AND ? ORDER BY i;",
            &[Value::Integer(2), Value::Integer(3)]
        ),
        vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]
    );
    assert_eq!(
        prepared_rows(
            &mut db,
            "SELECT i FROM t WHERE i IN (?, ?) ORDER BY i;",
            &[Value::Integer(1), Value::Integer(4)]
        ),
        vec![vec![Value::Integer(1)], vec![Value::Integer(4)]]
    );
    // A BETWEEN whose bounds disagree on type can still compare (the
    // desugared comparisons are numeric across INTEGER/DOUBLE).
    assert_eq!(
        prepared_rows(
            &mut db,
            "SELECT 1 WHERE ? BETWEEN 1 AND 2.5;",
            &[Value::Integer(1)]
        ),
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn arity_mismatches_name_the_missing_or_excess_positions() {
    let mut db = PinkDb::new();
    let stmt = db.prepare("SELECT ?, ?;").unwrap();
    let err = db
        .execute_prepared(&stmt, &[Value::Integer(1)])
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("values were not provided for prepared statement parameters: 2"),
        "{err}"
    );
    let err = db
        .execute_prepared(
            &stmt,
            &[Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        )
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("parameter count mismatch") && err.contains("3"),
        "{err}"
    );
}

#[test]
fn prepare_reports_its_parameters_in_order() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (a INTEGER, b VARCHAR); INSERT INTO t VALUES (1, 'x');")
        .unwrap();
    let stmt = db
        .prepare("SELECT b FROM t WHERE a = ? AND b = ?;")
        .unwrap();
    let bound = stmt
        .bind(
            &db,
            "main",
            &[Value::Integer(1), Value::Varchar("x".into())],
        )
        .unwrap();
    assert_eq!(bound.sql(), "SELECT b FROM t WHERE a = 1 AND b = 'x';");
    assert_eq!(
        bound.parameter_types(),
        &[DataType::Integer, DataType::Varchar]
    );
    // Bound statements execute through the normal single-statement path.
    assert_eq!(
        db.execute_bound(&bound).unwrap(),
        QueryResult::Selected {
            names: vec!["b".to_string()],
            types: vec![DataType::Varchar],
            rows: vec![vec![Value::Varchar("x".into())]],
        }
    );
}

// ---------- reuse, sessions, transactions ----------

#[test]
fn a_prepared_statement_is_reusable_and_state_is_fresh_per_execute() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER);").unwrap();
    let insert = db.prepare("INSERT INTO t VALUES (?);").unwrap();
    for i in 1..=3 {
        db.execute_prepared(&insert, &[Value::Integer(i)]).unwrap();
    }
    let select = db.prepare("SELECT count(*) FROM t WHERE i > ?;").unwrap();
    match db.execute_prepared(&select, &[Value::Integer(1)]).unwrap() {
        QueryResult::Selected { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bigint(2)]]);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // `USE` is a prepared-capable session statement too.
    db.execute("CREATE DATABASE other;").unwrap();
    let use_stmt = db.prepare("USE other;").unwrap();
    assert!(matches!(
        db.execute_prepared(&use_stmt, &[]).unwrap(),
        QueryResult::Used { db } if db == "other"
    ));
    assert_eq!(db.current_db(), "other");
}

#[test]
fn prepared_statements_run_inside_an_open_transaction() {
    let mut db = PinkDb::new();
    db.execute("BEGIN; CREATE TABLE t (i INTEGER);").unwrap();
    // The prepared insert binds against the transaction's private snapshot:
    // the table exists only there.
    let insert = db.prepare("INSERT INTO t VALUES (?);").unwrap();
    db.execute_prepared(&insert, &[Value::Integer(1)]).unwrap();
    db.execute_prepared(&insert, &[Value::Integer(2)]).unwrap();
    assert_eq!(one_int(rows(&mut db, "SELECT count(*) FROM t;")), 2);
    db.execute("ROLLBACK;").unwrap();
    // Rolled back: the table is gone.
    let err = db
        .execute("SELECT count(*) FROM t;")
        .unwrap_err()
        .to_string();
    assert!(err.contains("not found"), "{err}");
}

#[test]
fn connection_api_binds_executes_and_sees_transactions() {
    let database = Database::new();
    let mut conn = database.connect();
    conn.execute("CREATE TABLE t (i INTEGER, v VARCHAR);")
        .unwrap();
    let insert = conn.prepare("INSERT INTO t VALUES (?, ?);").unwrap();
    assert_eq!(insert.parameter_count(), 2);

    let bound = conn
        .bind(&insert, &[Value::Integer(1), Value::Varchar("it's".into())])
        .unwrap();
    // Substitution quotes strings (the WAL-safe form).
    assert_eq!(bound.sql(), "INSERT INTO t VALUES (1, 'it''s');");
    conn.execute_bound(&bound).unwrap();
    conn.execute_prepared(&insert, &[Value::Integer(2), Value::Varchar("b".into())])
        .unwrap();

    let select = conn.prepare("SELECT v FROM t WHERE i = ?;").unwrap();
    match conn
        .execute_prepared(&select, &[Value::Integer(1)])
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Varchar("it's".into())]])
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // Inside a transaction the bind sees the transaction's snapshot.
    conn.execute("BEGIN; CREATE TABLE u (i INTEGER);").unwrap();
    let u_insert = conn.prepare("INSERT INTO u VALUES (?);").unwrap();
    conn.execute_prepared(&u_insert, &[Value::Integer(9)])
        .unwrap();
    conn.execute("ROLLBACK;").unwrap();
    // After rollback the table is gone, so even a prepared statement on it
    // fails at bind (against the shared store now).
    let err = conn
        .execute_prepared(&u_insert, &[Value::Integer(1)])
        .expect_err("table was rolled back")
        .to_string();
    assert!(err.contains("not found"), "{err}");
}

#[test]
fn injection_shaped_values_are_quoted_not_executed() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (v VARCHAR);").unwrap();
    let insert = db.prepare("INSERT INTO t VALUES (?);").unwrap();
    db.execute_prepared(&insert, &[Value::Varchar("x'); DROP TABLE t; --".into())])
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT v FROM t;"),
        vec![vec![Value::Varchar("x'); DROP TABLE t; --".into())]]
    );
    // The table is still there.
    assert_eq!(one_int(rows(&mut db, "SELECT count(*) FROM t;")), 1);
}

// ---------- durability ----------

/// Read the sidecar WAL bytes (plaintext without the encryption feature).
fn wal_bytes(path: &std::path::Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

#[test]
fn prepared_dml_logs_the_substituted_sql_and_replays() {
    let scratch = Scratch::new("replay");
    {
        let mut db = PinkDb::open_durable(scratch.path()).unwrap();
        db.execute("CREATE TABLE t (i INTEGER, v VARCHAR);")
            .unwrap();
        let insert = db.prepare("INSERT INTO t VALUES (?, ?);").unwrap();
        db.execute_prepared(&insert, &[Value::Integer(1), Value::Varchar("it's".into())])
            .unwrap();
        db.execute_prepared(&insert, &[Value::Integer(2), Value::Null])
            .unwrap();
        let update = db.prepare("UPDATE t SET v = ? WHERE i = ?;").unwrap();
        db.execute_prepared(&update, &[Value::Varchar("two".into()), Value::Integer(2)])
            .unwrap();
        let delete = db.prepare("DELETE FROM t WHERE i = ?;").unwrap();
        db.execute_prepared(&delete, &[Value::Integer(1)]).unwrap();
    }
    // The WAL text was the substituted SQL, not the placeholders.
    let bytes = wal_bytes(&scratch.wal());
    assert!(!bytes.is_empty(), "a durable commit appends WAL records");
    let text = String::from_utf8_lossy(&bytes);
    assert!(
        text.contains("INSERT INTO t VALUES (2, NULL)"),
        "WAL should carry the substituted statement:\n{text}"
    );
    assert!(
        text.contains("INSERT INTO t VALUES (1, 'it''s')"),
        "string values are quoted/doubled:\n{text}"
    );
    assert!(
        text.contains("UPDATE t SET v = 'two' WHERE i = 2"),
        "WAL should carry the substituted update:\n{text}"
    );
    assert!(
        text.contains("DELETE FROM t WHERE i = 1"),
        "WAL should carry the substituted delete:\n{text}"
    );
    assert!(
        !text.contains("VALUES (?, ?)"),
        "no placeholders in the WAL"
    );

    // Reopen (replaying the WAL) and verify the state.
    let mut db = PinkDb::open_durable(scratch.path()).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT i, v FROM t;"),
        vec![vec![Value::Integer(2), Value::Varchar("two".into())]]
    );
    // A checkpoint round-trip too.
    db.checkpoint().unwrap();
    drop(db);
    let mut db = PinkDb::open_durable(scratch.path()).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT i, v FROM t;"),
        vec![vec![Value::Integer(2), Value::Varchar("two".into())]]
    );
}

#[test]
fn prepared_dml_replays_from_the_wal_without_a_checkpoint() {
    let scratch = Scratch::new("walreplay");
    {
        // No explicit checkpoint: recovery must replay the WAL records.
        let (mut db, _) = PinkDb::open_durable_with(scratch.path(), SyncPolicy::Sync).unwrap();
        db.execute("CREATE TABLE t (i INTEGER, v VARCHAR);")
            .unwrap();
        let insert = db.prepare("INSERT INTO t VALUES (?, ?);").unwrap();
        db.execute_prepared(&insert, &[Value::Integer(7), Value::Varchar("a'b".into())])
            .unwrap();
        let update = db.prepare("UPDATE t SET i = ? WHERE v = ?;").unwrap();
        db.execute_prepared(&update, &[Value::Integer(8), Value::Varchar("a'b".into())])
            .unwrap();
    }
    let (mut db, info) = PinkDb::open_durable_with(scratch.path(), SyncPolicy::Sync).unwrap();
    assert!(info.replayed_records > 0, "{info:?}");
    assert_eq!(
        rows(&mut db, "SELECT i, v FROM t;"),
        vec![vec![Value::Integer(8), Value::Varchar("a'b".into())]]
    );
}

#[test]
fn prepared_write_inside_a_durable_transaction_replays() {
    let scratch = Scratch::new("txnreplay");
    {
        let mut db = PinkDb::open_durable(scratch.path()).unwrap();
        db.execute("CREATE TABLE t (i INTEGER); BEGIN;").unwrap();
        let insert = db.prepare("INSERT INTO t VALUES (?);").unwrap();
        db.execute_prepared(&insert, &[Value::Integer(1)]).unwrap();
        db.execute_prepared(&insert, &[Value::Bigint(2)]).unwrap();
        db.execute("COMMIT;").unwrap();
    }
    let mut db = PinkDb::open_durable(scratch.path()).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT i FROM t ORDER BY i;"),
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
    );
}

#[test]
fn prepared_select_on_a_durable_database_logs_nothing() {
    let scratch = Scratch::new("readonly");
    {
        let mut db = PinkDb::open_durable(scratch.path()).unwrap();
        db.execute("CREATE TABLE t (i INTEGER); INSERT INTO t VALUES (1);")
            .unwrap();
    }
    let before = wal_bytes(&scratch.wal()).len();
    let mut db = PinkDb::open_durable(scratch.path()).unwrap();
    let select = db.prepare("SELECT i FROM t WHERE i = ?;").unwrap();
    assert_eq!(
        db.execute_prepared(&select, &[Value::Integer(1)]).unwrap(),
        QueryResult::Selected {
            names: vec!["i".to_string()],
            types: vec![DataType::Integer],
            rows: vec![vec![Value::Integer(1)]],
        }
    );
    // A read-only prepared statement appends nothing.
    assert_eq!(wal_bytes(&scratch.wal()).len(), before);
}

#[test]
fn prepared_double_and_date_values_round_trip_through_the_wal() {
    let scratch = Scratch::new("values");
    {
        let mut db = PinkDb::open_durable(scratch.path()).unwrap();
        db.execute("CREATE TABLE t (d DOUBLE, dt DATE, ts TIMESTAMP, b BIGINT);")
            .unwrap();
        let insert = db.prepare("INSERT INTO t VALUES (?, ?, ?, ?);").unwrap();
        db.execute_prepared(
            &insert,
            &[
                Value::Double(1.0 / 3.0),
                Value::Date(-1),
                Value::Timestamp(-1_500_000),
                Value::Bigint(i64::MIN),
            ],
        )
        .unwrap();
    }
    let mut db = PinkDb::open_durable(scratch.path()).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT d, dt, ts, b FROM t;"),
        vec![vec![
            Value::Double(1.0 / 3.0),
            Value::Date(-1),
            Value::Timestamp(-1_500_000),
            Value::Bigint(i64::MIN),
        ]]
    );
}

#[test]
fn error_variant_is_parameter_for_bind_failures() {
    let mut db = PinkDb::new();
    let stmt = db.prepare("SELECT ?;").unwrap();
    let err = db.execute_prepared(&stmt, &[Value::Null]).unwrap_err();
    assert!(matches!(err, PinkDbError::Parameter(_)), "{err:?}");
}
