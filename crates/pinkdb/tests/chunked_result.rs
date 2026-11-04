//! Wave 112b: `Connection::execute_chunked` hands a SELECT back as the
//! executor's chunks, so the C API can read them without a row round-trip.

use pinkdb::exec::ChunkedResult;
use pinkdb::{Database, QueryResult, Value};

#[test]
fn select_keeps_executor_chunks() {
    let db = Database::new();
    let mut conn = db.connect();
    conn.execute("CREATE TABLE t (i INTEGER, s VARCHAR)")
        .expect("create");
    conn.execute(
        "INSERT INTO t SELECT i, CASE WHEN i % 2 = 0 THEN NULL ELSE 'r' || i END \
         FROM range(0, 5000) r(i)",
    )
    .expect("insert");

    let results = conn.execute_chunked("SELECT i, s FROM t").expect("select");
    let ChunkedResult::Selected {
        names,
        types,
        chunks,
    } = &results[0]
    else {
        panic!("a plain autocommit SELECT must stay chunked");
    };
    assert_eq!(names, &["i".to_string(), "s".to_string()]);
    assert_eq!(types[0], pinkdb::types::DataType::Integer);
    assert_eq!(
        chunks.iter().map(|c| c.num_rows).collect::<Vec<_>>(),
        vec![2048, 2048, 904]
    );

    let first = &chunks[0].columns[0];
    assert_eq!(first.get(0), Value::Integer(0));
    assert!(first.is_valid(0));
    let second = &chunks[0].columns[1];
    assert!(!second.is_valid(0));
    assert_eq!(second.get(1), Value::Varchar("r1".into()));

    let last = &chunks[2];
    assert_eq!(last.num_rows, 904);
    assert_eq!(last.columns[0].get(903), Value::Integer(4999));
    assert_eq!(last.columns[1].get(903), Value::Varchar("r4999".into()));
}

#[test]
fn empty_and_non_select_results() {
    let db = Database::new();
    let mut conn = db.connect();
    conn.execute("CREATE TABLE t (i INTEGER)").expect("create");

    let results = conn
        .execute_chunked("SELECT i FROM t WHERE i < 0")
        .expect("empty select");
    let ChunkedResult::Selected { chunks, .. } = &results[0] else {
        panic!("empty SELECT must stay chunked");
    };
    assert!(chunks.is_empty());

    let results = conn
        .execute_chunked("CREATE TABLE u (i INTEGER)")
        .expect("create");
    assert!(matches!(
        &results[0],
        ChunkedResult::Other(QueryResult::Created)
    ));
}

#[test]
fn transaction_select_is_row_shaped() {
    let db = Database::new();
    let mut conn = db.connect();
    conn.execute("CREATE TABLE t (i INTEGER)").expect("create");
    conn.execute("INSERT INTO t VALUES (1), (2)")
        .expect("insert");

    let results = conn
        .execute_chunked("BEGIN; SELECT i FROM t ORDER BY i; COMMIT")
        .expect("transaction");
    assert!(matches!(
        &results[0],
        ChunkedResult::Other(QueryResult::Began)
    ));
    match &results[1] {
        ChunkedResult::Other(QueryResult::Selected { rows, .. }) => {
            assert_eq!(rows, &[vec![Value::Integer(1)], vec![Value::Integer(2)]]);
        }
        _ => panic!("a SELECT inside a transaction is row-shaped"),
    }
    assert!(matches!(
        &results[2],
        ChunkedResult::Other(QueryResult::Committed)
    ));
}
