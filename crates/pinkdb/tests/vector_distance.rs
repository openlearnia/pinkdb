//! `ARRAY` distance functions (wave 107b) against DuckDB 2.0.0's values.
//!
//! Measured with `/tmp/duckdb-2.0.0/duckdb -no-agent` on the same inputs.
//! DuckDB returns `FLOAT` for two `FLOAT[N]` arguments and `DOUBLE` for a
//! `DOUBLE[N]` or mixed pair; pinkdb always reports `DOUBLE` while computing
//! the `FLOAT` pair in `f32` first, so the numeric results are the same.

use pinkdb::{PinkDb, QueryResult, Value};

fn cell(db: &mut PinkDb, sql: &str) -> Value {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows[0][0].clone(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql).unwrap_err().to_string()
}

fn float_cell(db: &mut PinkDb, sql: &str) -> f32 {
    match cell(db, sql) {
        Value::Double(v) => v as f32,
        other => panic!("expected a number from {sql}, got {other:?}"),
    }
}

fn double_cell(db: &mut PinkDb, sql: &str) -> f64 {
    match cell(db, sql) {
        Value::Double(v) => v,
        other => panic!("expected a number from {sql}, got {other:?}"),
    }
}

const F: &str = "CAST([1.0, 2.0, 3.0] AS FLOAT[3])";
const G: &str = "CAST([4.0, 5.0, 6.0] AS FLOAT[3])";

#[test]
fn float_arrays_match_duckdb() {
    let mut db = PinkDb::new();
    // DuckDB: 5.196152, 0.025368094, 0.9746319, 32.0, -32.0.
    assert_eq!(
        float_cell(&mut db, &format!("SELECT array_distance({F}, {G})")),
        5.196152f32
    );
    // DuckDB 2.0.0 prints 0.025368094 and 0.9746319; a four-lane f32
    // accumulation can differ in the last f32 bit.
    assert!(
        (float_cell(&mut db, &format!("SELECT array_cosine_distance({F}, {G})")) - 0.025368094)
            .abs()
            <= 2e-7
    );
    assert!(
        (float_cell(
            &mut db,
            &format!("SELECT array_cosine_similarity({F}, {G})")
        ) - 0.9746319)
            .abs()
            <= 2e-7
    );
    assert_eq!(
        float_cell(&mut db, &format!("SELECT array_inner_product({F}, {G})")),
        32.0f32
    );
    assert_eq!(
        float_cell(
            &mut db,
            &format!("SELECT array_negative_inner_product({F}, {G})")
        ),
        -32.0f32
    );
    // Pinkdb's alias for the roadmap's name; DuckDB calls it array_distance.
    assert_eq!(
        float_cell(&mut db, &format!("SELECT array_l2_distance({F}, {G})")),
        5.196152f32
    );
}

#[test]
fn double_arrays_match_duckdb() {
    let mut db = PinkDb::new();
    let f = "CAST([1.0, 2.0, 3.0] AS DOUBLE[3])";
    let g = "CAST([4.0, 5.0, 6.0] AS DOUBLE[3])";
    // DuckDB: 5.196152422706632.
    assert_eq!(
        double_cell(&mut db, &format!("SELECT array_distance({f}, {g})")),
        27.0f64.sqrt()
    );
    assert_eq!(
        double_cell(&mut db, &format!("SELECT array_inner_product({f}, {g})")),
        32.0
    );
}

#[test]
fn mixed_arrays_widen_to_double() {
    let mut db = PinkDb::new();
    // DuckDB: 2.8284271247461903.
    let f = "CAST([1.0, 2.0] AS FLOAT[2])";
    let g = "CAST([3.0, 4.0] AS DOUBLE[2])";
    assert_eq!(
        double_cell(&mut db, &format!("SELECT array_distance({f}, {g})")),
        8.0f64.sqrt()
    );
    assert_eq!(
        double_cell(
            &mut db,
            &format!("SELECT array_negative_inner_product({g}, {f})")
        ),
        -11.0
    );
}

#[test]
fn nulls_and_errors_follow_duckdb() {
    let mut db = PinkDb::new();
    // A NULL array is NULL.
    assert!(matches!(
        cell(
            &mut db,
            &format!("SELECT array_distance(CAST(NULL AS FLOAT[3]), {G})")
        ),
        Value::Null
    ));
    // A NULL element is a runtime error naming the argument.
    assert!(
        err(
            &mut db,
            "SELECT array_distance(CAST([1.0, NULL] AS FLOAT[2]), CAST([1.0, 2.0] AS FLOAT[2]))"
        )
        .contains("left argument can not contain NULL values")
    );
    assert!(
        err(
            &mut db,
            "SELECT array_distance(CAST([1.0, 2.0] AS FLOAT[2]), CAST([1.0, NULL] AS FLOAT[2]))"
        )
        .contains("right argument can not contain NULL values")
    );
    // Different lengths are a binder error, as in DuckDB.
    assert!(
        err(
            &mut db,
            "SELECT array_distance(CAST([1.0] AS FLOAT[1]), CAST([1.0, 2.0] AS FLOAT[2]))"
        )
        .contains("Array arguments must be of the same size")
    );
    // Integer arrays are refused at bind time.
    assert!(
        err(
            &mut db,
            "SELECT array_distance(CAST([1, 2] AS INTEGER[2]), CAST([3, 4] AS INTEGER[2]))"
        )
        .contains("Arguments must be arrays of FLOAT or DOUBLE")
    );
}

#[test]
fn zero_vector_cosine_is_nan() {
    let mut db = PinkDb::new();
    let zero = "CAST([0.0, 0.0] AS FLOAT[2])";
    let two = "CAST([1.0, 2.0] AS FLOAT[2])";
    assert!(
        float_cell(
            &mut db,
            &format!("SELECT array_cosine_distance({zero}, {two})")
        )
        .is_nan()
    );
    assert!(
        float_cell(
            &mut db,
            &format!("SELECT array_cosine_similarity({zero}, {two})")
        )
        .is_nan()
    );
}

#[test]
fn distance_works_on_table_columns() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, f FLOAT[2], d DOUBLE[2]);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, [1.0, 2.0], [1.0, 2.0]), (2, [3.0, 4.0], [3.0, 4.0]);")
        .unwrap();
    let result = db
        .execute("SELECT id FROM t ORDER BY array_distance(f, CAST([0.0, 0.0] AS FLOAT[2]))")
        .unwrap()
        .pop()
        .unwrap();
    match result {
        QueryResult::Selected { rows, .. } => assert_eq!(rows[0][0], Value::Integer(1)),
        other => panic!("expected rows, got {other:?}"),
    }
}
