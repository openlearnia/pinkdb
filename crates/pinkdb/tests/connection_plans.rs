//! `Connection` (the path behind the C API, the bindings and the server) runs
//! the optimizer like `PinkDb::execute`. Without it, an implicit join stays a
//! cross product under one filter: TPC-H q9 took minutes over the server
//! against half a second embedded.
//!
//! Each test caps materialized rows below the cross product (1,000 × 1,000)
//! and well above the hash join's output, so only an optimized plan passes.

use pinkdb::{Database, ExecLimits, QueryResult, Value};

const JOIN: &str = "SELECT count(*) FROM a, b WHERE a.x = b.y";

fn setup() -> Database {
    let db = Database::new();
    let mut conn = db.connect();
    conn.execute(
        "CREATE TABLE a AS SELECT range AS x FROM range(1000);
         CREATE TABLE b AS SELECT range AS y FROM range(1000);",
    )
    .unwrap();
    db
}

fn capped(db: &Database) -> pinkdb::Connection {
    let mut conn = db.connect();
    conn.set_exec_limits(ExecLimits {
        max_result_rows: Some(100_000),
        ..ExecLimits::none()
    });
    conn
}

fn count(mut results: Vec<QueryResult>) -> Value {
    match results.pop().unwrap() {
        QueryResult::Selected { mut rows, .. } => rows.remove(0).remove(0),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn autocommit_statements_are_optimized() {
    let db = setup();
    let mut conn = capped(&db);
    assert_eq!(count(conn.execute(JOIN).unwrap()), Value::Bigint(1000));
}

#[test]
fn statements_in_a_transaction_are_optimized() {
    let db = setup();
    let mut conn = capped(&db);
    conn.execute("BEGIN").unwrap();
    assert_eq!(count(conn.execute(JOIN).unwrap()), Value::Bigint(1000));
    conn.execute("COMMIT").unwrap();
}

#[test]
fn cursor_statements_are_optimized() {
    let db = setup();
    let mut conn = capped(&db);
    let mut cursor = conn
        .query_cursor("SELECT a.x FROM a, b WHERE a.x = b.y")
        .unwrap();
    let mut rows = 0;
    while let Some(chunk) = cursor.next_chunk().unwrap() {
        rows += chunk.num_rows;
    }
    assert_eq!(rows, 1000);
}

#[test]
fn profiled_statements_report_optimize_time() {
    let db = setup();
    let mut conn = capped(&db);
    let mut out = conn.execute_profiled(JOIN).unwrap();
    let (result, profile) = out.pop().unwrap();
    assert_eq!(count(vec![result]), Value::Bigint(1000));
    assert!(profile.optimize > std::time::Duration::ZERO);
}
