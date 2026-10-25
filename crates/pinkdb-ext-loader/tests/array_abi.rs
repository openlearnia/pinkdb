//! The extension ABI's ARRAY type (minor 5): a fixed-size `DOUBLE[N]`
//! argument and result, carried as a pointer-and-length view of contiguous
//! elements.
//!
//! The fixture (`tests/c/array_ext.c`) declares its functions with
//! `PINKDB_EXT_ARRAY_TAG(3)` — the packed `(len << 8) | 11` type tag — and
//! reads the payload as a plain `const double *`, so these tests cover the
//! whole path: declaration, binding, per-row marshalling, the callback and
//! result decoding.

mod common;

use pinkdb::{PinkDb, QueryResult, Value};

// ---------- helpers ----------

fn array_db() -> PinkDb {
    let mut db = PinkDb::new();
    let name = db
        .load_extension(&common::extension_path("array_ext"))
        .unwrap();
    assert_eq!(name, "array_ext");
    db
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

fn text(db: &mut PinkDb, sql: &str) -> String {
    match rows(db, sql).remove(0).remove(0) {
        Value::Varchar(text) => text,
        other => panic!("expected VARCHAR for {sql:?}, got {other:?}"),
    }
}

/// Run a statement that returns no rows.
fn run(db: &mut PinkDb, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("{sql:?}: {e}"));
}

fn error(db: &mut PinkDb, sql: &str) -> String {
    match db.execute(sql) {
        Ok(results) => panic!("expected an error for {sql:?}, got {results:?}"),
        Err(e) => e.to_string(),
    }
}

// ---------- arguments and results ----------

/// An ARRAY goes in and an ARRAY comes out: the length is part of the type on
/// both sides, and the elements are unchanged in order and value.
#[test]
fn array_arguments_and_results_cross_the_boundary() {
    let mut db = array_db();
    assert_eq!(
        text(
            &mut db,
            "SELECT CAST(arr_twice(CAST('[1.0, 2.0, 3.0]' AS DOUBLE[3])) AS VARCHAR)"
        ),
        "[2.0, 4.0, 6.0]"
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT typeof(arr_twice(CAST('[0, 0, 0]' AS DOUBLE[3])))"
        )[0][0],
        Value::Varchar("DOUBLE[3]".to_string())
    );
    // A negative zero survives the boundary bit-for-bit.
    assert_eq!(
        text(&mut db, "SELECT CAST(arr_origin() AS VARCHAR)"),
        "[1.0, 0.5, -0.0]"
    );
    // An ARRAY argument and a DOUBLE result use different channels.
    assert_eq!(
        rows(
            &mut db,
            "SELECT arr_sum(CAST('[1.5, 2.5, 4.0]' AS DOUBLE[3]))"
        )[0][0],
        Value::Double(8.0)
    );
}

/// The per-row path: the callback runs for every row of a real column, and a
/// NULL array crosses as NULL without touching the callback's payload.
#[test]
fn array_columns_cross_every_row() {
    let mut db = array_db();
    run(&mut db, "CREATE TABLE t(id INTEGER, v DOUBLE[3])");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, [1.0, 2.0, 3.0]), (2, NULL), (3, [-1.0, 0.0, 1.0])",
    );
    let got = rows(
        &mut db,
        "SELECT id, CAST(arr_twice(v) AS VARCHAR) FROM t ORDER BY id",
    );
    assert_eq!(got[0][1], Value::Varchar("[2.0, 4.0, 6.0]".to_string()));
    assert_eq!(got[1][1], Value::Null);
    assert_eq!(got[2][1], Value::Varchar("[-2.0, 0.0, 2.0]".to_string()));
    // A NULL array is passed through as NULL, and the function's own NULL
    // result is a NULL row.
    assert_eq!(
        rows(&mut db, "SELECT arr_twice(NULL) IS NULL")[0][0],
        Value::Boolean(true)
    );
    assert_eq!(
        rows(&mut db, "SELECT arr_sum(NULL) IS NULL")[0][0],
        Value::Boolean(true)
    );
}

// ---------- refusals ----------

/// The payload has no per-element NULL channel, so a row holding a NULL
/// element is refused rather than silently zero-filled.
/// The 4-byte element kind crosses the same way: a `FLOAT[N]` argument is a
/// `(const float *, N)` view of the engine's own buffer, and a `FLOAT[N]`
/// result comes back as `N * 4` bytes.
#[test]
fn float_arrays_cross_the_boundary_as_f32_views() {
    let mut db = array_db();
    run(&mut db, "CREATE TABLE t(id INTEGER, v FLOAT[3])");
    run(
        &mut db,
        "INSERT INTO t VALUES (1, [1.5, -0.25, 2.0]), (2, NULL)",
    );
    let got = rows(
        &mut db,
        "SELECT id, typeof(arrf_twice(v)), CAST(arrf_twice(v) AS VARCHAR) FROM t ORDER BY id",
    );
    assert_eq!(got[0][1], Value::Varchar("FLOAT[3]".to_string()));
    assert_eq!(got[0][2], Value::Varchar("[3.0, -0.5, 4.0]".to_string()));
    assert_eq!(got[1][2], Value::Null);
    // A scalar result consumes the same view.
    assert_eq!(
        rows(
            &mut db,
            "SELECT arrf_sum(CAST('[1.5, 2.0, 4.0]' AS FLOAT[3]))"
        )[0][0],
        Value::Double(7.5)
    );
    // The kind is part of the declaration: DOUBLE[3] does not match FLOAT[3].
    let message = error(
        &mut db,
        "SELECT arrf_twice(CAST('[1.0, 2.0, 3.0]' AS DOUBLE[3]))",
    );
    assert!(message.contains("FLOAT[3]"), "{message}");
    // A NULL element has no channel here either.
    let message = error(
        &mut db,
        "SELECT arrf_twice(CAST('[1.0, NULL, 3.0]' AS FLOAT[3]))",
    );
    assert!(message.contains("NULL element"), "{message}");
}

#[test]
fn null_elements_are_refused_at_the_boundary() {
    let mut db = array_db();
    let message = error(
        &mut db,
        "SELECT arr_twice(CAST('[1.0, NULL, 3.0]' AS DOUBLE[3]))",
    );
    assert!(
        message.contains("NULL element") && message.contains("extension ABI"),
        "{message}"
    );
    // A whole NULL array is fine: it is `is_null`, not a NULL element.
    assert_eq!(
        rows(&mut db, "SELECT arr_twice(CAST(NULL AS DOUBLE[3])) IS NULL")[0][0],
        Value::Boolean(true)
    );
}

/// The declared length is part of the type: an ARRAY of another length never
/// reaches the callback, and neither does an element type the engine cannot
/// hand over as doubles.
#[test]
fn declared_types_are_exact() {
    let mut db = array_db();
    let message = error(&mut db, "SELECT arr_twice(CAST('[1.0, 2.0]' AS DOUBLE[2]))");
    assert!(
        message.contains("DOUBLE[3]") && message.contains("DOUBLE[2]"),
        "{message}"
    );
    let message = error(&mut db, "SELECT arr_twice(CAST('[1, 2, 3]' AS INTEGER[3]))");
    assert!(
        message.contains("DOUBLE[3]") && message.contains("INTEGER[3]"),
        "{message}"
    );
}
