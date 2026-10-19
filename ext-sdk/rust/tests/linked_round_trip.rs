//! End-to-end test of the **linked** model driven by the SDK's macros: one
//! `pinkdb_extension!` declaration, published into the registry, loaded by
//! name into a real engine and queried through SQL (binder, optimizer,
//! executor), including the volatile-function rule.
//!
//! This is the same code path a wasm embedder takes, exercised natively.

use pinkdb::{PinkDb, QueryResult, Value};

/// The fixture extension: every ABI v1 value kind, the NULL policies, the
/// error path, one volatile function and both ranged arities (wave 54's
/// `optional(min)` / `variadic(min)`) — all from one macro invocation.
mod fixture {
    use pinkdb_ext_sdk::{Date, Optional, Timestamp, pinkdb_extension};

    pinkdb_extension! {
        name = "wave51_rust_sdk_linked",
        functions = [
            /// No arguments.
            fn ext_rs_zero() -> i64 { 7 },

            /// Plain BIGINT in, BIGINT out: deterministic.
            fn ext_rs_double(n: i64) -> i64 { n.wrapping_mul(2) },

            /// ABI v1 is exact: BIGINT stays BIGINT (unlike a DOUBLE
            /// parameter, which accepts any numeric argument).
            fn ext_rs_halve_big(n: i64) -> i64 { n / 2 },

            /// **Optional arity** (wave 54): 1 or 2 arguments. The three
            /// states of the tail are distinct: absent (one argument),
            /// SQL NULL, or a value.
            optional(1) fn ext_rs_shape(a: i64, b: Optional<i64>) -> Result<i64, String> {
                Ok(match b {
                    Optional::Absent => a,
                    Optional::Null => a.wrapping_mul(10),
                    Optional::Value(v) => a.wrapping_add(v),
                })
            },

            /// **Optional arity with `min = 0`**: the single argument may be
            /// absent entirely.
            optional(0) fn ext_rs_maybe(a: Optional<i64>) -> i64 {
                match a {
                    Optional::Absent => -1,
                    Optional::Null => -2,
                    Optional::Value(v) => v,
                }
            },

            /// **Variadic arity** (wave 54): any number of NULL-able BIGINTs,
            /// at least one; all-NULL (or absent values) yields SQL NULL.
            variadic(1) fn ext_rs_sum(values: Vec<Option<i64>>) -> Result<Option<i64>, String> {
                Ok(values.into_iter().flatten().reduce(i64::wrapping_add))
            },

            /// **Variadic with `min = 0`** over a non-NULL element type: a
            /// NULL argument is a decode error naming the fix.
            variadic(0) fn ext_rs_count(values: Vec<i64>) -> i64 {
                values.len() as i64
            },

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
            "ext_rs_halve_big",
            "ext_rs_shape",
            "ext_rs_maybe",
            "ext_rs_sum",
            "ext_rs_count",
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
    assert_eq!(by_name("ext_rs_zero").min_args, 0);
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

    // The ranged arities carry their declared range (wave 54).
    let shape = by_name("ext_rs_shape");
    assert_eq!(shape.min_args, 1);
    assert_eq!(shape.max_args(), 2);
    assert!(!shape.variadic);
    assert!(shape.is_ranged());
    assert_eq!(
        shape.args,
        &[
            pinkdb_ext_sdk::ExtType::Bigint,
            pinkdb_ext_sdk::ExtType::Bigint
        ]
    );
    let maybe = by_name("ext_rs_maybe");
    assert_eq!((maybe.min_args, maybe.max_args()), (0, 1));
    assert!(maybe.is_ranged());
    let sum = by_name("ext_rs_sum");
    assert_eq!(sum.args, &[pinkdb_ext_sdk::ExtType::Bigint]);
    assert_eq!(sum.min_args, 1);
    assert!(sum.variadic);
    assert_eq!(sum.max_args(), 64);
    assert_eq!(sum.arg_type_at(9), pinkdb_ext_sdk::ExtType::Bigint);
    let count = by_name("ext_rs_count");
    assert_eq!((count.min_args, count.max_args()), (0, 64));
    assert!(count.variadic);
    // Fixed declarations never look ranged.
    assert!(!by_name("ext_rs_greet").is_ranged());

    // The linked registration is built from the same list, through the
    // arity-aware constructors (fixed / optional / variadic).
    let linked = fixture::pinkdb_linked_extension().unwrap();
    assert_eq!(linked.name(), "wave51_rust_sdk_linked");
    assert_eq!(linked.functions().len(), specs.len());
    let by_name = |name: &str| {
        linked
            .functions()
            .iter()
            .find(|f| f.name() == name)
            .unwrap()
            .clone()
    };
    assert_eq!(
        (
            by_name("ext_rs_greet").min_args(),
            by_name("ext_rs_greet").max_args()
        ),
        (1, 1)
    );
    assert_eq!(
        (
            by_name("ext_rs_shape").min_args(),
            by_name("ext_rs_shape").max_args()
        ),
        (1, 2)
    );
    assert_eq!(
        (
            by_name("ext_rs_maybe").min_args(),
            by_name("ext_rs_maybe").max_args()
        ),
        (0, 1)
    );
    assert!(by_name("ext_rs_sum").is_variadic());
    assert_eq!(
        (
            by_name("ext_rs_sum").min_args(),
            by_name("ext_rs_sum").max_args()
        ),
        (1, 64)
    );
    assert!(!by_name("ext_rs_shape").is_variadic());
    assert!(linked.functions().last().unwrap().is_volatile());
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

// ---------- ranged arity (wave 54) ----------

/// `optional(min)`: a call may stop after `min` arguments, and the three
/// states of an `Optional<T>` tail stay distinct through SQL — `absent` (the
/// call is short), `NULL`, and a value.
#[test]
fn optional_arity_binds_a_short_call_and_keeps_absent_distinct_from_null() {
    let mut db = test_db();
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_shape(CAST(3 AS BIGINT));"),
        vec![vec![Value::Bigint(3)]],
        "one argument: the tail is absent"
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_shape(CAST(3 AS BIGINT), NULL);"),
        vec![vec![Value::Bigint(30)]],
        "an explicit NULL is not the absent case"
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT ext_rs_shape(CAST(3 AS BIGINT), CAST(4 AS BIGINT));"
        ),
        vec![vec![Value::Bigint(7)]]
    );
    // `optional(0)`: the whole list may be absent.
    assert_eq!(
        rows(
            &mut db,
            "SELECT ext_rs_maybe(), ext_rs_maybe(NULL), ext_rs_maybe(CAST(5 AS BIGINT));"
        ),
        vec![vec![Value::Bigint(-1), Value::Bigint(-2), Value::Bigint(5),]]
    );
    // Arity is enforced before the callback runs, with the ranged wording.
    let message = error(&mut db, "SELECT ext_rs_shape();");
    assert!(
        message.contains("takes between 1 and 2 argument(s)"),
        "{message}"
    );
    let message = error(
        &mut db,
        "SELECT ext_rs_shape(CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT));",
    );
    assert!(message.contains("takes at most 2 argument(s)"), "{message}");
    let message = error(
        &mut db,
        "SELECT ext_rs_maybe(CAST(1 AS BIGINT), CAST(2 AS BIGINT));",
    );
    assert!(message.contains("takes at most 1 argument(s)"), "{message}");
    // The declared types are still enforced at every position.
    let message = error(&mut db, "SELECT ext_rs_shape('x');");
    assert!(message.contains("argument 1 must be BIGINT"), "{message}");
    let message = error(&mut db, "SELECT ext_rs_shape(CAST(1 AS BIGINT), 'x');");
    assert!(message.contains("argument 2 must be BIGINT"), "{message}");
}

/// `variadic(min)`: any number of arguments from `min` up to the ABI limit,
/// one repeated declared type, decoded into the `Vec<T>` tail.
#[test]
fn variadic_arity_accepts_a_varying_argument_count() {
    let mut db = test_db();
    assert_eq!(
        rows(
            &mut db,
            "SELECT ext_rs_sum(CAST(1 AS BIGINT), NULL, CAST(2 AS BIGINT));"
        ),
        vec![vec![Value::Bigint(3)]],
        "NULL elements are skipped by the callback"
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_sum(CAST(10 AS BIGINT));"),
        vec![vec![Value::Bigint(10)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_sum(NULL, NULL);"),
        vec![vec![Value::Null]],
        "all-NULL: the callback returns None"
    );
    // `min = 0` with a non-NULL element type.
    assert_eq!(
        rows(
            &mut db,
            "SELECT ext_rs_count(), ext_rs_count(CAST(1 AS BIGINT), CAST(2 AS BIGINT));"
        ),
        vec![vec![Value::Bigint(0), Value::Bigint(2)]]
    );
    // Below the minimum: refused before the callback runs.
    let message = error(&mut db, "SELECT ext_rs_sum();");
    assert!(
        message.contains("takes at least 1 argument(s)"),
        "{message}"
    );
    // A NULL element of a non-NULL tail is a decode error naming the fix.
    let message = error(&mut db, "SELECT ext_rs_count(NULL);");
    assert!(
        message.contains("extension function ext_rs_count() failed")
            && message.contains("argument 1")
            && message.contains("Option<i64>"),
        "{message}"
    );
    // A mismatched element type is still a bind error.
    let message = error(&mut db, "SELECT ext_rs_sum('x');");
    assert!(message.contains("argument 1 must be BIGINT"), "{message}");
    let message = error(
        &mut db,
        "SELECT ext_rs_sum(CAST(1 AS BIGINT), CAST(2 AS BIGINT), 'x');",
    );
    assert!(message.contains("argument 3 must be BIGINT"), "{message}");
}

/// Ranged declarations compose with everything else: columns, prepared
/// statements (each call may bind a different argument count), and the WAL.
#[test]
fn ranged_arity_composes_with_columns_prepared_statements_and_writes() {
    let mut db = test_db();
    db.execute("CREATE TABLE nums (a BIGINT, b BIGINT);")
        .unwrap();
    db.execute("INSERT INTO nums VALUES (1, 2), (3, NULL), (5, 4);")
        .unwrap();
    // A column in the required position, the optional one passed or absent:
    // an absent tail behaves like a one-argument call (`a`), a NULL tail like
    // `a * 10`, a value like the sum.
    assert_eq!(
        rows(
            &mut db,
            "SELECT ext_rs_shape(a), ext_rs_shape(a, b) FROM nums ORDER BY a;"
        ),
        vec![
            vec![Value::Bigint(1), Value::Bigint(3)],
            vec![Value::Bigint(3), Value::Bigint(30)],
            vec![Value::Bigint(5), Value::Bigint(9)],
        ]
    );
    // A variadic over a column set.
    assert_eq!(
        rows(&mut db, "SELECT ext_rs_sum(a, b) FROM nums ORDER BY a;"),
        vec![
            vec![Value::Bigint(3)],
            vec![Value::Bigint(3)],
            vec![Value::Bigint(9)],
        ]
    );
    // Prepared: one placeholder is a short call, two are the full one.
    let short = db.prepare("SELECT ext_rs_shape(?);").unwrap();
    assert_eq!(short.parameter_count(), 1);
    let result = db.execute_prepared(&short, &[Value::Bigint(4)]).unwrap();
    match result {
        QueryResult::Selected { rows, .. } => assert_eq!(rows, vec![vec![Value::Bigint(4)]]),
        other => panic!("expected rows, got {other:?}"),
    }
    // The NULL parameter needs an explicit type (the engine's rule for an
    // untyped NULL placeholder, like every other function).
    let full = db
        .prepare("SELECT ext_rs_sum(?, CAST(? AS BIGINT), ?);")
        .unwrap();
    let result = db
        .execute_prepared(&full, &[Value::Bigint(1), Value::Null, Value::Bigint(2)])
        .unwrap();
    match result {
        QueryResult::Selected { rows, .. } => assert_eq!(rows, vec![vec![Value::Bigint(3)]]),
        other => panic!("expected rows, got {other:?}"),
    }
    // Deterministic ranged calls are WAL-loggable like any other function.
    db.execute("INSERT INTO nums VALUES (ext_rs_shape(CAST(2 AS BIGINT), CAST(3 AS BIGINT)), ext_rs_sum(CAST(1 AS BIGINT), CAST(1 AS BIGINT)));")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM nums;"),
        vec![vec![Value::Bigint(4)]]
    );
}
