//! End-to-end tests for the core math extension through the engine (wave 54):
//! the linked registry → `LOAD EXTENSION 'math'` → binder → executor → WAL.
//!
//! Everything here goes through SQL — the same path a user takes — so the
//! tests pin the *observable* semantics (including the engine's error wording
//! and the numeric→DOUBLE widening), not the pure functions (which
//! `src/lib.rs`'s unit tests cover).
//!
//! The engine's own math built-ins (`sqrt`, `pow`, `floor`, `ceil`, `sign`,
//! `greatest`, `least`) are exercised alongside the extension's functions:
//! an extension cannot shadow a built-in, so those names stay core functions
//! and this file pins that they behave like DuckDB — the whole math surface in
//! one place.

use pinkdb::{PinkDb, QueryResult, Value};

/// The registry is process-global and one-shot: register once for the whole
/// integration test binary, then `LOAD EXTENSION 'math'` per store.
fn registered() {
    pinkdb_math::ensure_registered().expect("math registers");
}

fn db_with_math() -> PinkDb {
    registered();
    let mut db = PinkDb::new();
    db.load_linked_extension("math").expect("math loads");
    db
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn one(db: &mut PinkDb, sql: &str) -> Value {
    let rows = rows(db, sql);
    assert_eq!(rows.len(), 1, "{sql} -> {rows:?}");
    rows[0][0].clone()
}

/// One DOUBLE cell (panics for NULL or another type).
fn dbl(db: &mut PinkDb, sql: &str) -> f64 {
    match one(db, sql) {
        Value::Double(x) => x,
        other => panic!("expected DOUBLE from {sql}, got {other:?}"),
    }
}

fn approx(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() <= 1e-9 * expected.abs().max(1.0),
        "{actual} != {expected}"
    );
}

fn error(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql)
        .expect_err("statement should fail")
        .to_string()
}

// ---------- registration ----------

#[test]
fn load_extension_by_name_registers_the_math_functions() {
    let mut db = db_with_math();
    // Reachable, and case-insensitive like every function name.
    approx(dbl(&mut db, "SELECT EXP(1.0);"), std::f64::consts::E);
    assert_eq!(dbl(&mut db, "SELECT log(100.0);"), 2.0);
    assert_eq!(dbl(&mut db, "SELECT Pi();"), std::f64::consts::PI);
    // The SQL names are DuckDB's — no `ext_math_` prefixes.
    let err = error(&mut db, "SELECT ext_math_exp(1.0);");
    assert!(err.contains("unknown function"), "{err}");
}

#[test]
fn the_sql_load_statement_resolves_the_registry_name() {
    let mut db = PinkDb::new();
    let err = error(&mut db, "SELECT exp(1.0);");
    assert!(err.contains("unknown function"), "{err}");

    registered();
    let result = db.execute("LOAD EXTENSION 'math';").unwrap();
    assert!(
        matches!(&result[0], QueryResult::LoadedExtension { name } if name == "math"),
        "{result:?}"
    );
    approx(dbl(&mut db, "SELECT exp(1.0);"), std::f64::consts::E);

    // Loading twice is the documented duplicate-load error, not a silent
    // second registration.
    let err = error(&mut db, "LOAD EXTENSION 'math';");
    assert!(err.contains("already loaded"), "{err}");
    assert!(db.is_extension_loaded("math"));
    assert!(db.is_extension_loaded("MATH"));
    assert!(!db.is_extension_loaded("nope"));
}

#[test]
fn a_second_load_is_refused_and_changes_nothing() {
    let mut db = db_with_math();
    let err = db.load_linked_extension("math").unwrap_err().to_string();
    assert!(err.contains("is already loaded"), "{err}");
    assert_eq!(dbl(&mut db, "SELECT log10(100.0);"), 2.0);
}

// ---------- the functions, through SQL ----------

#[test]
fn exponential_and_logarithm_family_through_sql() {
    let mut db = db_with_math();
    assert_eq!(dbl(&mut db, "SELECT exp(0.0);"), 1.0);
    approx(dbl(&mut db, "SELECT exp(1.0);"), std::f64::consts::E);
    assert_eq!(dbl(&mut db, "SELECT exp(1000.0);"), f64::INFINITY);
    assert_eq!(dbl(&mut db, "SELECT exp(CAST('-inf' AS DOUBLE));"), 0.0);

    assert_eq!(dbl(&mut db, "SELECT ln(1.0);"), 0.0);
    approx(dbl(&mut db, "SELECT ln(exp(1.0));"), 1.0);
    assert_eq!(dbl(&mut db, "SELECT ln(0.0);"), f64::NEG_INFINITY);
    assert!(dbl(&mut db, "SELECT ln(-1.0);").is_nan());

    assert_eq!(dbl(&mut db, "SELECT log2(8.0);"), 3.0);
    assert_eq!(dbl(&mut db, "SELECT log10(1000.0);"), 3.0);
    assert_eq!(dbl(&mut db, "SELECT log(100.0);"), 2.0);
    assert_eq!(dbl(&mut db, "SELECT log(2.0, 8.0);"), 3.0);
    assert_eq!(
        dbl(&mut db, "SELECT log(10.0, 0.5);"),
        -std::f64::consts::LOG10_2
    );
    assert!(dbl(&mut db, "SELECT log(-1.0);").is_nan());
    assert_eq!(dbl(&mut db, "SELECT log(0.0);"), f64::NEG_INFINITY);

    // NULL propagates through every function, in every position.
    for sql in [
        "SELECT exp(NULL);",
        "SELECT ln(NULL);",
        "SELECT log(NULL);",
        "SELECT log(2.0, NULL);",
        "SELECT log(NULL, 2.0);",
        "SELECT log2(NULL);",
        "SELECT log10(NULL);",
    ] {
        assert_eq!(one(&mut db, sql), Value::Null, "{sql}");
    }
}

#[test]
fn trigonometric_family_through_sql() {
    let mut db = db_with_math();
    assert_eq!(dbl(&mut db, "SELECT sin(0.0);"), 0.0);
    assert_eq!(dbl(&mut db, "SELECT cos(0.0);"), 1.0);
    assert_eq!(dbl(&mut db, "SELECT tan(0.0);"), 0.0);
    assert_eq!(dbl(&mut db, "SELECT sin(CAST(pi() AS DOUBLE) / 2.0);"), 1.0);
    assert_eq!(dbl(&mut db, "SELECT cot(0.0);"), f64::INFINITY);
    assert!(dbl(&mut db, "SELECT asin(2.0);").is_nan());
    assert!(dbl(&mut db, "SELECT acos(2.0);").is_nan());
    assert_eq!(
        dbl(&mut db, "SELECT asin(1.0);"),
        std::f64::consts::FRAC_PI_2
    );
    assert_eq!(dbl(&mut db, "SELECT acos(1.0);"), 0.0);
    assert_eq!(
        dbl(&mut db, "SELECT atan(CAST('inf' AS DOUBLE));"),
        std::f64::consts::FRAC_PI_2
    );
    // atan2(y, x): y first, like DuckDB.
    assert_eq!(dbl(&mut db, "SELECT atan2(0.0, 0.0);"), 0.0);
    assert_eq!(
        dbl(&mut db, "SELECT atan2(1.0, 0.0);"),
        std::f64::consts::FRAC_PI_2
    );
    assert_eq!(
        dbl(&mut db, "SELECT atan2(0.0, -1.0);"),
        std::f64::consts::PI
    );
    assert_eq!(
        dbl(&mut db, "SELECT atan2(-1.0, 0.0);"),
        -std::f64::consts::FRAC_PI_2
    );
    // Conversions and pi.
    assert_eq!(dbl(&mut db, "SELECT degrees(pi());"), 180.0);
    assert_eq!(dbl(&mut db, "SELECT radians(180.0);"), std::f64::consts::PI);
    assert_eq!(dbl(&mut db, "SELECT pi();"), std::f64::consts::PI);
    for sql in [
        "SELECT sin(NULL);",
        "SELECT cos(NULL);",
        "SELECT tan(NULL);",
        "SELECT cot(NULL);",
        "SELECT asin(NULL);",
        "SELECT acos(NULL);",
        "SELECT atan(NULL);",
        "SELECT atan2(NULL, 1.0);",
        "SELECT atan2(1.0, NULL);",
        "SELECT degrees(NULL);",
        "SELECT radians(NULL);",
    ] {
        assert_eq!(one(&mut db, sql), Value::Null, "{sql}");
    }
}

/// The `optional(1)` arity: one argument is `log10`, two are `log(b, x)`, and
/// the engine refuses anything outside 1..=2 before the callback runs.
#[test]
fn log_arity_is_enforced_with_the_ranged_wording() {
    let mut db = db_with_math();
    let err = error(&mut db, "SELECT log();");
    assert!(err.contains("takes between 1 and 2 argument(s)"), "{err}");
    let err = error(&mut db, "SELECT log(1.0, 2.0, 3.0);");
    assert!(err.contains("takes at most 2 argument(s)"), "{err}");
    // The declared type is still DOUBLE at every position; a NULL literal is
    // typeless and allowed (the callback decides).
    assert_eq!(one(&mut db, "SELECT log(NULL);"), Value::Null);
    // `random()` takes no arguments.
    let err = error(&mut db, "SELECT random(1);");
    assert!(err.contains("takes 0 argument(s)"), "{err}");
}

// ---------- numeric coercion (wave 54 design point) ----------

/// A DOUBLE-declared extension parameter accepts any *numeric* argument: the
/// binder inserts the CAST to DOUBLE DuckDB applies implicitly. VARCHAR and
/// the other types stay exact.
#[test]
fn numeric_arguments_widen_into_double_parameters() {
    let mut db = db_with_math();
    // Integer literals and expressions (DuckDB's implicit cast).
    approx(dbl(&mut db, "SELECT exp(1);"), std::f64::consts::E);
    assert_eq!(dbl(&mut db, "SELECT log(100);"), 2.0);
    assert_eq!(dbl(&mut db, "SELECT log(2, 8);"), 3.0);
    assert_eq!(dbl(&mut db, "SELECT sin(0);"), 0.0);
    approx(
        dbl(&mut db, "SELECT atan2(1, 1);"),
        std::f64::consts::FRAC_PI_4,
    );
    // A BIGINT argument widens too.
    assert_eq!(dbl(&mut db, "SELECT log2(CAST(8 AS BIGINT));"), 3.0);
    // DECIMAL widens (it is numeric), like DuckDB.
    assert_eq!(
        dbl(&mut db, "SELECT log10(CAST(100 AS DECIMAL(5,0)));"),
        2.0
    );
    // Columns of every numeric type.
    db.execute(
        "CREATE TABLE nums (i INTEGER, b BIGINT, d DOUBLE, dec DECIMAL(6,2)); \
         INSERT INTO nums VALUES (1, 2, 0.5, 1.50), (2, 8, 2.0, 2.50);",
    )
    .unwrap();
    assert_eq!(
        rows(
            &mut db,
            "SELECT i, exp(i) > 2.0, log2(b), sin(d), log10(dec) FROM nums ORDER BY i;"
        )
        .len(),
        2
    );
    approx(dbl(&mut db, "SELECT log2(b) FROM nums WHERE i = 2;"), 3.0);
    assert_eq!(
        dbl(&mut db, "SELECT log10(dec) FROM nums WHERE i = 2;"),
        0.3979400086720376
    );
    // …and a VARCHAR is still refused, with the CAST hint (DuckDB parses the
    // string; that is a documented divergence).
    let err = error(&mut db, "SELECT exp('1');");
    assert!(
        err.contains("exp argument 1 must be DOUBLE, got VARCHAR") && err.contains("use CAST"),
        "{err}"
    );
    let err = error(&mut db, "SELECT log('2', 8.0);");
    assert!(
        err.contains("log argument 1 must be DOUBLE, got VARCHAR"),
        "{err}"
    );
}

/// The engine's own math built-ins keep DuckDB's behavior: the extension
/// cannot (and does not) shadow them.
#[test]
fn engine_math_builtins_stay_duckdb_pinned() {
    let mut db = db_with_math();
    db.execute(
        "CREATE TABLE emp (id INTEGER, did INTEGER, salary DOUBLE); \
         INSERT INTO emp VALUES (1, 10, 150.5), (2, 20, 50.0), (3, NULL, 200.25), (4, 10, NULL);",
    )
    .unwrap();
    // sqrt: integer/DOUBLE arguments and the NaN edge.
    assert_eq!(dbl(&mut db, "SELECT sqrt(4);"), 2.0);
    assert_eq!(dbl(&mut db, "SELECT sqrt(id) FROM emp WHERE id = 1;"), 1.0);
    assert!(
        (dbl(&mut db, "SELECT sqrt(salary) FROM emp WHERE id = 1;") - 150.5f64.sqrt()).abs()
            < 1e-12
    );
    assert!(dbl(&mut db, "SELECT sqrt(-1);").is_nan());
    assert_eq!(dbl(&mut db, "SELECT sqrt(0);"), 0.0);
    // pow/power.
    assert_eq!(dbl(&mut db, "SELECT pow(2, 10);"), 1024.0);
    assert_eq!(
        dbl(&mut db, "SELECT power(2.0, 0.5);"),
        std::f64::consts::SQRT_2
    );
    assert!(dbl(&mut db, "SELECT pow(-1.0, 0.5);").is_nan());
    assert_eq!(dbl(&mut db, "SELECT pow(0.0, -1.0);"), f64::INFINITY);
    // floor/ceil on DOUBLE and integer columns (DuckDB returns DOUBLE).
    assert_eq!(dbl(&mut db, "SELECT floor(1.7);"), 1.0);
    assert_eq!(dbl(&mut db, "SELECT floor(-1.5);"), -2.0);
    assert_eq!(dbl(&mut db, "SELECT ceil(1.2);"), 2.0);
    assert_eq!(dbl(&mut db, "SELECT ceil(-1.2);"), -1.0);
    assert_eq!(dbl(&mut db, "SELECT floor(id) FROM emp WHERE id = 3;"), 3.0);
    assert_eq!(
        dbl(&mut db, "SELECT ceil(salary) FROM emp WHERE id = 3;"),
        201.0
    );
    // sign (INTEGER here; DuckDB's TINYINT carries the same value).
    assert_eq!(one(&mut db, "SELECT sign(-5);"), Value::Integer(-1));
    assert_eq!(one(&mut db, "SELECT sign(0);"), Value::Integer(0));
    assert_eq!(one(&mut db, "SELECT sign(3.5);"), Value::Integer(1));
    assert_eq!(
        one(&mut db, "SELECT sign(id - 3) FROM emp WHERE id = 2;"),
        Value::Integer(-1)
    );
    // greatest/least: NULLs skipped, all-NULL NULL, mixed numerics widen,
    // NaN orders above everything (DuckDB's ordering).
    assert_eq!(
        rows(
            &mut db,
            "SELECT greatest(1, NULL, 3), least(1, NULL, 3), greatest(NULL, NULL), \
             greatest(1, 2.5), least('a', 'b');"
        ),
        vec![vec![
            Value::Integer(3),
            Value::Integer(1),
            Value::Null,
            Value::Double(2.5),
            Value::Varchar("a".to_string()),
        ]]
    );
    let rows = rows(
        &mut db,
        "SELECT greatest(0.0 / 0.0, 5.0), least(0.0 / 0.0, 5.0);",
    );
    match (&rows[0][0], &rows[0][1]) {
        (Value::Double(high), Value::Double(low)) => {
            assert!(high.is_nan());
            assert_eq!(*low, 5.0);
        }
        other => panic!("unexpected {other:?}"),
    }
    // A non-numeric argument is still refused.
    assert!(db.execute("SELECT sqrt('a');").is_err());
    assert!(db.execute("SELECT sign(true);").is_err());
}

// ---------- columns, WHERE and GROUP BY ----------

#[test]
fn functions_compose_over_columns_in_queries() {
    let mut db = db_with_math();
    db.execute(
        "CREATE TABLE emp (id INTEGER, did INTEGER, salary DOUBLE); \
         INSERT INTO emp VALUES (1, 10, 150.5), (2, 20, 50.0), (3, 20, 200.25), (4, 10, NULL);",
    )
    .unwrap();
    // Projection over columns, including NULLs.
    let projected = rows(
        &mut db,
        "SELECT id, exp(id) > 2.0, ln(salary) IS NULL FROM emp ORDER BY id;",
    );
    assert_eq!(projected.len(), 4);
    assert_eq!(
        projected[3],
        vec![
            Value::Integer(4),
            Value::Boolean(true),
            Value::Boolean(true)
        ]
    );
    // WHERE with a math predicate (and a built-in in the same expression).
    assert_eq!(
        rows(
            &mut db,
            "SELECT id FROM emp WHERE log2(id) > 1.0 AND sqrt(id) < 2.0 ORDER BY id;"
        ),
        vec![vec![Value::Integer(3)]]
    );
    // GROUP BY over a computed key.
    assert_eq!(
        rows(
            &mut db,
            "SELECT floor(salary / 100.0) AS bucket, COUNT(*) FROM emp \
             WHERE salary IS NOT NULL GROUP BY bucket ORDER BY bucket;"
        ),
        vec![
            vec![Value::Double(0.0), Value::Bigint(1)],
            vec![Value::Double(1.0), Value::Bigint(1)],
            vec![Value::Double(2.0), Value::Bigint(1)],
        ]
    );
    // GROUP BY over the extension's own result (a DOUBLE key).
    assert_eq!(
        rows(
            &mut db,
            "SELECT sign(salary - 100.0) AS s, COUNT(*) FROM emp \
             WHERE salary IS NOT NULL GROUP BY s ORDER BY s;"
        ),
        vec![
            vec![Value::Integer(-1), Value::Bigint(1)],
            vec![Value::Integer(1), Value::Bigint(2)],
        ]
    );
    // A view body may call the extension (read-only).
    db.execute("CREATE VIEW loud AS SELECT id, exp(id) AS e FROM emp;")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM loud WHERE e > 2.0;"),
        vec![vec![Value::Bigint(4)]]
    );
}

// ---------- prepared statements ----------

#[test]
fn prepared_statements_bind_numeric_and_ranged_arguments() {
    let mut db = db_with_math();
    // One placeholder into a DOUBLE parameter: an INTEGER value widens.
    let stmt = db.prepare("SELECT exp(?);").unwrap();
    assert_eq!(stmt.parameter_count(), 1);
    let result = db.execute_prepared(&stmt, &[Value::Integer(1)]).unwrap();
    match result {
        QueryResult::Selected { rows, .. } => match rows[0][0] {
            Value::Double(x) => approx(x, std::f64::consts::E),
            ref other => panic!("expected DOUBLE, got {other:?}"),
        },
        other => panic!("expected rows, got {other:?}"),
    }
    // The optional arity: one bound placeholder is the one-argument form,
    // two are `log(b, x)`.
    let short = db.prepare("SELECT log(?);").unwrap();
    let result = db.execute_prepared(&short, &[Value::Integer(100)]).unwrap();
    match result {
        QueryResult::Selected { rows, .. } => assert_eq!(rows, vec![vec![Value::Double(2.0)]]),
        other => panic!("expected rows, got {other:?}"),
    }
    let full = db.prepare("SELECT log(?, ?);").unwrap();
    let result = db
        .execute_prepared(&full, &[Value::Integer(2), Value::Integer(8)])
        .unwrap();
    match result {
        QueryResult::Selected { rows, .. } => assert_eq!(rows, vec![vec![Value::Double(3.0)]]),
        other => panic!("expected rows, got {other:?}"),
    }
    // A VARCHAR value is still refused for a DOUBLE parameter.
    let err = db
        .execute_prepared(&stmt, &[Value::Varchar("1".to_string())])
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("must be DOUBLE") || err.contains("runtime"),
        "{err}"
    );
}

// ---------- volatility ----------

#[test]
fn random_is_readable_but_refused_in_every_wal_logged_statement() {
    let mut db = db_with_math();
    // Read-only: allowed, a DOUBLE in [0, 1).
    let value = dbl(&mut db, "SELECT random();");
    assert!((0.0..1.0).contains(&value), "{value}");
    // Volatile means "may differ on replay", not "constant": the predicate is
    // still deterministic in its *type*, and many calls land in range.
    assert_eq!(
        rows(
            &mut db,
            "SELECT random() >= 0.0 AND random() < 1.0 AND random() <> random();"
        ),
        vec![vec![Value::Boolean(true)]]
    );
    // A read over a table, in WHERE, and in a subquery are all reads.
    db.execute("CREATE TABLE t (x DOUBLE); INSERT INTO t VALUES (0.5);")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t WHERE random() < 1.0;"),
        vec![vec![Value::Bigint(1)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT (SELECT random()) < 1.0;"),
        vec![vec![Value::Boolean(true)]]
    );

    // Every statement class the engine WAL-logs is refused, before it runs.
    for sql in [
        "INSERT INTO t VALUES (random());",
        "INSERT INTO t SELECT random() FROM t;",
        "UPDATE t SET x = random();",
        "DELETE FROM t WHERE random() < 1.0;",
        "CREATE TABLE t2 AS SELECT random();",
        "CREATE TABLE t3 AS SELECT random() FROM t;",
        "INSERT INTO t VALUES (1.0 + random());",
    ] {
        let err = error(&mut db, sql);
        assert!(
            err.contains("random()")
                && err.contains("volatile (nondeterministic)")
                && err.contains("cannot be called in a statement the engine WAL-logs"),
            "{sql}: {err}"
        );
    }
    // Nothing was applied by the refused statements.
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t;"),
        vec![vec![Value::Bigint(1)]]
    );
    let err = error(&mut db, "SELECT COUNT(*) FROM t2;");
    assert!(err.contains("not found"), "{err}");

    // A view *body* is a read: CREATE VIEW only stores the text, and querying
    // the view is a read, so both are allowed. Writing through the view is a
    // WAL-logged statement and is refused (the body's volatile call would be
    // re-evaluated on replay).
    db.execute("CREATE VIEW v AS SELECT random() AS r;")
        .unwrap();
    let value = dbl(&mut db, "SELECT r FROM v;");
    assert!((0.0..1.0).contains(&value), "{value}");
    let err = error(&mut db, "INSERT INTO t SELECT r FROM v;");
    assert!(err.contains("volatile"), "{err}");
}

#[test]
fn deterministic_math_is_wal_logged_and_replays() {
    registered();
    // A LOAD EXTENSION in the same statement as the writes lets the replay
    // re-resolve the extension from the registry; the WAL prefixing is also
    // exercised by the records that follow the checkpoint.
    assert!(pinkdb_math::ensure_registered().is_ok());
    let dir = std::env::temp_dir().join(format!("pinkdb_math_wal_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();

    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        db.load_linked_extension("math").unwrap();
        db.execute("CREATE TABLE t (a DOUBLE, b DOUBLE);").unwrap();
        db.execute(
            "INSERT INTO t VALUES (exp(1), log(100)), (log(2, 8), sqrt(4)), (sin(0), pi());",
        )
        .unwrap();
        // Refused: random is volatile, so it is never logged.
        let err = db
            .execute("INSERT INTO t VALUES (random(), 0.0);")
            .unwrap_err()
            .to_string();
        assert!(err.contains("volatile"), "{err}");
        db.checkpoint().unwrap();
        // After the checkpoint the next touching record is prefixed again.
        db.execute("INSERT INTO t VALUES (log2(8), log10(1000));")
            .unwrap();
        db.flush_wal().unwrap();
        let wal =
            String::from_utf8_lossy(&std::fs::read(format!("{ckpt}.wal")).unwrap()).into_owned();
        assert!(
            wal.contains("LOAD EXTENSION"),
            "the record is prefixed: {wal}"
        );
        assert!(
            !wal.contains("random"),
            "a volatile call is never logged: {wal}"
        );
    }

    {
        // Reopen with the extension published (not loaded in the store):
        // replay resolves `LOAD EXTENSION 'math'` through the registry.
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        assert_eq!(
            rows(&mut db, "SELECT COUNT(*) FROM t;"),
            vec![vec![Value::Bigint(4)]]
        );
        approx(dbl(&mut db, "SELECT a FROM t ORDER BY a LIMIT 1;"), 0.0);
        assert_eq!(
            dbl(&mut db, "SELECT MIN(b) FROM t WHERE a = 3.0;"),
            2.0,
            "log(2, 8) == 3 and sqrt(4) == 2 replay"
        );
        assert_eq!(
            dbl(&mut db, "SELECT b FROM t WHERE a = 0.0;"),
            std::f64::consts::PI
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
