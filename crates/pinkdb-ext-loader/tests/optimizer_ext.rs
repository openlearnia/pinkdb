//! Dynamic-call call-count pins for the optimizer (review H1).
//!
//! This file deliberately contains a single test: it reads the process-wide
//! `test_ext` call counters exactly, and each integration-test file runs as
//! its own process, so no other test can perturb them.

mod common;

use pinkdb::{PinkDb, QueryResult, Value};

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

fn ints(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i as i64,
            ref other => panic!("expected INTEGER, got {other:?}"),
        })
        .collect()
}

/// An enclosed dynamic call (inside an operator/COALESCE/CASE) must run once
/// per row — never be folded at bind time — and an erroring constant call
/// must run exactly once (not twice at bind plus once at execution).
#[test]
fn enclosed_dynamic_calls_are_never_folded() {
    let path = common::extension_path("test_ext");
    let library = common::extension("test_ext");
    let mut db = PinkDb::new();
    db.load_extension(&path).unwrap();
    db.execute("CREATE TABLE t3 (a INTEGER); INSERT INTO t3 VALUES (1), (2), (3);")
        .unwrap();
    db.execute("CREATE TABLE empty (a INTEGER);").unwrap();

    let ticks = |before: i64| common::symbol_i64(&library, "pinkdb_test_ticks") - before;

    // `ext_tick() + 0`: the old fold rule saw a column-free Binary and froze
    // one literal (after calling the callback twice at bind time).
    let before = common::symbol_i64(&library, "pinkdb_test_ticks");
    let values = ints(&rows(&mut db, "SELECT ext_tick() + 0 FROM t3;"));
    assert_eq!(ticks(before), 3, "one call per row (not frozen)");
    assert!(
        values.windows(2).all(|w| w[0] < w[1]),
        "fresh value per row: {values:?}"
    );

    // COALESCE arm (the enclosing node is column-free).
    let before = common::symbol_i64(&library, "pinkdb_test_ticks");
    let values = ints(&rows(&mut db, "SELECT COALESCE(ext_tick(), 0) FROM t3;"));
    assert_eq!(ticks(before), 3, "COALESCE must not freeze the call");
    assert!(values.windows(2).all(|w| w[0] < w[1]), "{values:?}");

    // CASE arm (condition constant, so the whole tree is column-free).
    let before = common::symbol_i64(&library, "pinkdb_test_ticks");
    let values = ints(&rows(
        &mut db,
        "SELECT CASE WHEN 1 = 1 THEN ext_tick() ELSE 0 END FROM t3;",
    ));
    assert_eq!(ticks(before), 3, "CASE must not freeze the call");
    assert!(values.windows(2).all(|w| w[0] < w[1]), "{values:?}");

    // WHERE shape: a constant dynamic predicate is evaluated per row.
    let before = common::symbol_i64(&library, "pinkdb_test_ticks");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM t3 WHERE ext_tick() > 0;"),
        vec![vec![Value::Bigint(3)]]
    );
    assert_eq!(ticks(before), 3, "WHERE predicate must not fold");

    // Empty input: no rows, no calls (folding used to call twice at bind).
    let before = common::symbol_i64(&library, "pinkdb_test_ticks");
    assert_eq!(
        rows(&mut db, "SELECT ext_tick() + 0 FROM empty;"),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(ticks(before), 0, "empty input must not call the callback");
    let before = common::symbol_i64(&library, "pinkdb_test_ticks");
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM empty WHERE ext_tick() > 0;"),
        vec![vec![Value::Bigint(0)]]
    );
    assert_eq!(ticks(before), 0, "empty input must not call the callback");

    // INSERT ... VALUES with an enclosed dynamic call stays per-row.
    db.execute("CREATE TABLE ins (a INTEGER);").unwrap();
    let before = common::symbol_i64(&library, "pinkdb_test_ticks");
    db.execute("INSERT INTO ins VALUES (ext_tick() + 0), (ext_tick() + 0), (ext_tick() + 0);")
        .unwrap();
    assert_eq!(ticks(before), 3, "one call per inserted row");
    let values = ints(&rows(&mut db, "SELECT a FROM ins ORDER BY a;"));
    assert_eq!(values.len(), 3);
    assert!(
        values.windows(2).all(|w| w[0] < w[1]),
        "frozen values: {values:?}"
    );

    // An erroring constant call runs exactly ONCE (at execution). Before the
    // fix, the two optimizer passes each evaluated it at bind time.
    let before = common::symbol_i64(&library, "pinkdb_test_fail_calls");
    let e = db
        .execute("SELECT ext_fail_on_negative(-3) + 0;")
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("ext_fail_on_negative() failed with code 7"),
        "{e}"
    );
    let calls = common::symbol_i64(&library, "pinkdb_test_fail_calls") - before;
    assert_eq!(calls, 1, "the erroring call must run exactly once");
}
