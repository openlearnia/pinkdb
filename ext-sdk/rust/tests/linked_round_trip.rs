//! End-to-end test of the **linked** model driven by the SDK's macros: one
//! `pinkdb_extension!` declaration, published into the registry, loaded by
//! name into a real engine and queried through SQL (binder, optimizer,
//! executor), including the volatile-function rule.
//!
//! This is the same code path a wasm embedder takes, exercised natively.

use pinkdb::{PinkDb, QueryResult, Value};

/// The fixture extension: every ABI v1 value kind, the NULL policies, the
/// error path and one volatile function — all from one macro invocation.
mod fixture {
    use pinkdb_ext_sdk::{Date, Timestamp, pinkdb_extension};

    pinkdb_extension! {
        name = "wave51_rust_sdk_linked",
        functions = [
            /// No arguments.
            fn ext_rs_zero() -> i64 { 7 },

            /// Plain BIGINT in, BIGINT out: deterministic.
            fn ext_rs_double(n: i64) -> i64 { n.wrapping_mul(2) },

            /// Borrowed VARCHAR in, owned out; the `Option` parameters make
            /// NULL a value the function itself decides about.
            fn ext_rs_greet(name: Option<&str>) -> Result<Option<String>, String> {
                match name {
                    Some(n) => Ok(Some(format!("hello {n}"))),
                    None => Ok(None),
                }
            },

            fn ext_rs_bump(n: Option<i64>) -> Option<i64> {
                n.map(|x| x + 1)
            },

            fn ext_rs_is_even(n: i64) -> bool { n % 2 == 0 },
            fn ext_rs_halve(x: f64) -> f64 { x / 2.0 },
            fn ext_rs_next_day(d: Option<Date>) -> Option<Date> {
                d.map(|day| Date(day.0 + 1))
            },
            fn ext_rs_micros(t: Option<Timestamp>) -> Option<i64> {
                t.map(|ts| ts.0)
            },
            /// The error path: `Err` becomes a statement error (the linked
            /// model carries the message through).
            fn ext_rs_fail(n: i64) -> Result<i64, String> {
                Err(format!("no value for {n}"))
            },
        ],
        volatile = [
            /// Nondeterministic: the engine refuses this in WAL-logged
            /// statements.
            fn ext_rs_tick() -> i64 { 1 },
        ],
    }
}

/// Rows of the last statement in `sql` (which must be a SELECT).
fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

/// Error message of a failing `sql`.
fn error(db: &mut PinkDb, sql: &str) -> String {
    match db.execute(sql) {
        Ok(results) => panic!("expected an error for {sql:?}, got {results:?}"),
        Err(e) => e.to_string(),
    }
}

/// A store with the fixture extension loaded; the registry is process-global,
/// so this runs once (registration is one-shot).
fn test_db() -> PinkDb {
    static REGISTER: std::sync::Once = std::sync::Once::new();
    REGISTER.call_once(|| {
        fixture::pinkdb_register_linked().expect("register the fixture extension");
    });
    let mut db = PinkDb::new();
    let name = db.load_linked_extension("wave51_rust_sdk_linked").unwrap();
    assert_eq!(name, "wave51_rust_sdk_linked");
    db
}

#[test]
fn the_generated_specs_carry_the_declaration() {
    let specs = fixture::pinkdb_scalar_specs();
    let names: Vec<&str> = specs.iter().map(|s| s.name).collect();
    assert_eq!(
        names,
        vec![
            "ext_rs_zero",
            "ext_rs_double",
            "ext_rs_greet",
            "ext_rs_bump",
            "ext_rs_is_even",
            "ext_rs_halve",
            "ext_rs_next_day",
            "ext_rs_micros",
            "ext_rs_fail",
            "ext_rs_tick",
        ]
    );
    let by_name = |name: &str| specs.iter().find(|s| s.name == name).unwrap();
    assert_eq!(by_name("ext_rs_zero").args, &[]);
    assert_eq!(
        by_name("ext_rs_double").args,
        &[pinkdb_ext_sdk::ExtType::Bigint]
    );
    assert_eq!(
        by_name("ext_rs_greet").args,
        &[pinkdb_ext_sdk::ExtType::Varchar]
    );
    assert_eq!(by_name("ext_rs_halve").ret, pinkdb_ext_sdk::ExtType::Double);
    assert!(!by_name("ext_rs_double").volatile);
    assert!(by_name("ext_rs_tick").volatile);
    // The linked registration is built from the same list.
    let linked = fixture::pinkdb_linked_extension().unwrap();
    assert_eq!(linked.name(), "wave51_rust_sdk_linked");
    assert_eq!(linked.functions().len(), specs.len());
    assert!(linked.functions()[9].is_volatile());
}

#[test]
fn every_value_kind_round_trips_through_sql() {
    let mut db = test_db();

    assert_eq!(
        rows(&mut db, "SELECT ext_rs_zero();"),
        vec![vec![Value::Bigint(7)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_double(CAST(21 AS BIGINT));"),
        vec![vec![Value::Bigint(42)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_greet('world');"),
        vec![vec![Value::Varchar("hello world".to_string())]]
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_is_even(CAST(4 AS BIGINT));"),
        vec![vec![Value::Boolean(true)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_is_even(CAST(3 AS BIGINT));"),
        vec![vec![Value::Boolean(false)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_halve(5.0);"),
        vec![vec![Value::Double(2.5)]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT ext_rs_next_day(DATE '2024-02-28') = DATE '2024-02-29';"
        ),
        vec![vec![Value::Boolean(true)]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT ext_rs_micros(TIMESTAMP '2024-01-01 00:00:00.5') = 1704067200500000;"
        ),
        vec![vec![Value::Boolean(true)]]
    );
    // Composition with built-ins and with other extension calls.
    assert_eq!(
        rows(&mut db, "SELECT upper(ext_rs_greet('bob'));"),
        vec![vec![Value::Varchar("HELLO BOB".to_string())]]
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_double(CAST(21 AS BIGINT)) + 1;"),
        vec![vec![Value::Bigint(43)]]
    );
}

#[test]
fn null_handling_is_explicit_through_option() {
    let mut db = test_db();
    assert_eq!(
        rows(
            &mut db,
            "SELECT ext_rs_greet(NULL) IS NULL, ext_rs_bump(NULL) IS NULL, \
             ext_rs_next_day(NULL) IS NULL, ext_rs_micros(NULL) IS NULL, \
             ext_rs_bump(CAST(1 AS BIGINT)) IS NOT NULL;"
        ),
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
        ]]
    );
    // A NULL reaching a plain parameter is the statement's error (the
    // declared signature does not accept NULL), never a silent reinterpret.
    let message = error(&mut db, "SELECT ext_rs_double(NULL);");
    assert!(
        message.contains("extension function ext_rs_double() failed"),
        "{message}"
    );
    assert!(message.contains("NULL"), "{message}");
    assert!(message.contains("Option<i64>"), "{message}");
}

#[test]
fn returned_errors_fail_the_statement_with_their_message() {
    let mut db = test_db();
    let message = error(&mut db, "SELECT ext_rs_fail(CAST(3 AS BIGINT));");
    assert_eq!(
        message,
        "extension function ext_rs_fail() failed: no value for 3"
    );
}

#[test]
fn volatile_functions_are_refused_in_wal_logged_statements() {
    let mut db = test_db();
    // Read-only: always allowed.
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_tick();"),
        vec![vec![Value::Bigint(1)]]
    );
    db.execute("CREATE TABLE t (a BIGINT);").unwrap();
    let message = error(&mut db, "INSERT INTO t VALUES (ext_rs_tick());");
    assert!(
        message.contains("ext_rs_tick()")
            && message.contains("volatile")
            && message.contains("cannot be called in a statement the engine WAL-logs"),
        "{message}"
    );
    let message = error(&mut db, "CREATE TABLE t2 AS SELECT ext_rs_tick();");
    assert!(message.contains("volatile"), "{message}");
    // Deterministic functions are unaffected.
    db.execute("INSERT INTO t VALUES (ext_rs_double(CAST(3 AS BIGINT)));")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t;"),
        vec![vec![Value::Bigint(6)]]
    );
}
