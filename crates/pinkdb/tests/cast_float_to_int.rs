//! DOUBLE → integer casts round half to even, like DuckDB v2.0.0's
//! `nearbyint` (`test/sql/cast/float_integer_cast.test`). Every expected
//! value and error text here was read from the release CLI on the same
//! expression, so a divergence is a bug in this engine, not in the test.

use pinkdb::{PinkDb, QueryResult, Value};

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn row(db: &mut PinkDb, sql: &str) -> Vec<Value> {
    rows(db, sql).pop().expect("one row")
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql)
        .expect_err(&format!("{sql} should fail"))
        .to_string()
}

/// One row rendered the way the CLI prints it.
fn text(db: &mut PinkDb, sql: &str) -> Vec<String> {
    row(db, sql).iter().map(Value::to_string).collect()
}

/// Every row rendered the way the CLI prints it: one string per row.
fn texts(db: &mut PinkDb, sql: &str) -> Vec<String> {
    rows(db, sql)
        .iter()
        .map(|row| {
            row.iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[test]
fn double_to_integer_rounds_half_to_even() {
    let mut db = PinkDb::new();
    assert_eq!(
        text(
            &mut db,
            "SELECT 2.5::DOUBLE::INTEGER, 3.5::DOUBLE::INTEGER, 0.5::DOUBLE::INTEGER, \
             1.5::DOUBLE::INTEGER, 4.5::DOUBLE::INTEGER, -0.5::DOUBLE::INTEGER, \
             -1.5::DOUBLE::INTEGER, -2.5::DOUBLE::INTEGER, -3.5::DOUBLE::INTEGER, \
             -4.5::DOUBLE::INTEGER, CAST(-0.0 AS DOUBLE)::INTEGER;"
        ),
        ["2", "4", "0", "2", "4", "0", "-2", "-2", "-4", "-4", "0"]
    );
}

#[test]
fn double_to_bigint_rounds_half_to_even() {
    let mut db = PinkDb::new();
    assert_eq!(
        text(
            &mut db,
            "SELECT 2.5::DOUBLE::BIGINT, 3.5::DOUBLE::BIGINT, -0.5::DOUBLE::BIGINT, \
             -2.5::DOUBLE::BIGINT, -3.5::DOUBLE::BIGINT, CAST(-0.0 AS DOUBLE)::BIGINT, \
             CAST(-9223372036854775808.0 AS DOUBLE)::BIGINT;"
        ),
        ["2", "4", "0", "-2", "-4", "0", "-9223372036854775808",]
    );
}

#[test]
fn double_to_hugeint_rounds_half_to_even() {
    let mut db = PinkDb::new();
    assert_eq!(
        text(
            &mut db,
            "SELECT 2.5::DOUBLE::HUGEINT, 3.5::DOUBLE::HUGEINT, -0.5::DOUBLE::HUGEINT, \
             -2.5::DOUBLE::HUGEINT, -3.5::DOUBLE::HUGEINT, CAST(-0.0 AS DOUBLE)::HUGEINT;"
        ),
        ["2", "4", "0", "-2", "-4", "0"]
    );
}

#[test]
fn double_to_uhugeint_rounds_half_to_even() {
    let mut db = PinkDb::new();
    assert_eq!(
        text(
            &mut db,
            "SELECT 2.5::DOUBLE::UHUGEINT, 3.5::DOUBLE::UHUGEINT, -0.5::DOUBLE::UHUGEINT, \
             CAST(-0.0 AS DOUBLE)::UHUGEINT;"
        ),
        ["2", "4", "0", "0"]
    );
}

#[test]
fn double_to_integer_errors_match_duckdb() {
    let mut db = PinkDb::new();
    let int32 = "can't be cast because the value is out of range for the destination type INT32";
    assert_eq!(
        err(&mut db, "SELECT CAST(2147483647.5 AS DOUBLE)::INTEGER;"),
        format!("Conversion Error: Type DOUBLE with value 2147483647.5 {int32}")
    );
    assert_eq!(
        err(&mut db, "SELECT CAST(-2147483648.6 AS DOUBLE)::INTEGER;"),
        format!("Conversion Error: Type DOUBLE with value -2147483648.6 {int32}")
    );
    assert_eq!(
        err(&mut db, "SELECT CAST(2147483648.0 AS DOUBLE)::INTEGER;"),
        format!("Conversion Error: Type DOUBLE with value 2147483648.0 {int32}")
    );
    assert_eq!(
        err(&mut db, "SELECT CAST('NaN' AS DOUBLE)::INTEGER;"),
        format!("Conversion Error: Type DOUBLE with value nan {int32}")
    );
    assert_eq!(
        err(&mut db, "SELECT CAST('Infinity' AS DOUBLE)::INTEGER;"),
        format!("Conversion Error: Type DOUBLE with value inf {int32}")
    );
    assert_eq!(
        err(&mut db, "SELECT CAST('-Infinity' AS DOUBLE)::INTEGER;"),
        format!("Conversion Error: Type DOUBLE with value -inf {int32}")
    );
}

#[test]
fn double_to_wider_integer_errors_match_duckdb() {
    let mut db = PinkDb::new();
    let int64 = "can't be cast because the value is out of range for the destination type INT64";
    assert_eq!(
        err(&mut db, "SELECT 9223372036854775807::DOUBLE::BIGINT;"),
        format!("Conversion Error: Type DOUBLE with value 9.223372036854776e+18 {int64}")
    );
    assert_eq!(
        err(&mut db, "SELECT 1e300::DOUBLE::BIGINT;"),
        format!("Conversion Error: Type DOUBLE with value 1e+300 {int64}")
    );
    assert_eq!(
        err(&mut db, "SELECT CAST('NaN' AS DOUBLE)::BIGINT;"),
        format!("Conversion Error: Type DOUBLE with value nan {int64}")
    );

    let int128 = "can't be cast because the value is out of range for the destination type INT128";
    assert_eq!(
        err(&mut db, "SELECT 1e300::DOUBLE::HUGEINT;"),
        format!("Conversion Error: Type DOUBLE with value 1e+300 {int128}")
    );
    assert_eq!(
        err(
            &mut db,
            "SELECT 170141183460469231731687303715884105728.0::DOUBLE::HUGEINT;"
        ),
        format!("Conversion Error: Type DOUBLE with value 1.7014118346046923e+38 {int128}")
    );
    assert_eq!(
        err(&mut db, "SELECT CAST('Infinity' AS DOUBLE)::HUGEINT;"),
        format!("Conversion Error: Type DOUBLE with value inf {int128}")
    );

    let uint128 =
        "can't be cast because the value is out of range for the destination type UINT128";
    assert_eq!(
        err(&mut db, "SELECT CAST(-1.0 AS DOUBLE)::UHUGEINT;"),
        format!("Conversion Error: Type DOUBLE with value -1.0 {uint128}")
    );
    assert_eq!(
        err(
            &mut db,
            "SELECT CAST(3.402823669209385e38 AS DOUBLE)::UHUGEINT;"
        ),
        format!("Conversion Error: Type DOUBLE with value 3.402823669209385e+38 {uint128}")
    );
    assert_eq!(
        err(&mut db, "SELECT CAST('-Infinity' AS DOUBLE)::UHUGEINT;"),
        format!("Conversion Error: Type DOUBLE with value -inf {uint128}")
    );
}

#[test]
fn try_cast_double_to_integer_is_null_out_of_range() {
    let mut db = PinkDb::new();
    assert_eq!(
        text(
            &mut db,
            "SELECT TRY_CAST(2147483647.5::DOUBLE AS INTEGER), \
             TRY_CAST(2.5::DOUBLE AS INTEGER), \
             TRY_CAST(CAST('NaN' AS DOUBLE) AS BIGINT);"
        ),
        ["NULL", "2", "NULL"]
    );
}

#[test]
fn insert_of_double_into_integer_rounds_half_to_even() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER, b BIGINT, h HUGEINT, u UHUGEINT);")
        .unwrap();
    db.execute(
        "INSERT INTO t VALUES \
         (2.5::DOUBLE, 3.5::DOUBLE, -2.5::DOUBLE, 2.5::DOUBLE), \
         (-2.5::DOUBLE, -3.5::DOUBLE, 3.5::DOUBLE, 3.5::DOUBLE), \
         (CAST(-0.5 AS DOUBLE), CAST(-0.5 AS DOUBLE), CAST(-0.0 AS DOUBLE), \
          CAST(-0.5 AS DOUBLE));",
    )
    .unwrap();
    assert_eq!(
        texts(&mut db, "SELECT i, b, h, u FROM t ORDER BY i;"),
        ["-2|-4|4|4", "0|0|0|0", "2|4|-2|2"]
    );
    assert_eq!(
        err(&mut db, "INSERT INTO t (i) VALUES (2147483647.5::DOUBLE);"),
        "Conversion Error: Type DOUBLE with value 2147483647.5 can't be cast because the value \
         is out of range for the destination type INT32"
    );
}

#[test]
fn decimal_and_varchar_integer_casts_still_round_half_away_from_zero() {
    let mut db = PinkDb::new();
    // A decimal literal stays a DECIMAL, where DuckDB rounds away from zero.
    assert_eq!(
        text(
            &mut db,
            "SELECT 2.5::INTEGER, 3.5::INTEGER, -2.5::INTEGER, 2.5::BIGINT, \
             2.5::HUGEINT, 2.5::UHUGEINT;"
        ),
        ["3", "4", "-3", "3", "3", "3"]
    );
    // A VARCHAR holding a fraction rounds the same way.
    assert_eq!(
        text(
            &mut db,
            "SELECT '2.5'::INTEGER, '3.5'::INTEGER, '-2.5'::INTEGER, \
             '2.5e0'::BIGINT, '2.5'::HUGEINT, '2.5'::UHUGEINT;"
        ),
        ["3", "4", "-3", "3", "3", "3"]
    );
}

#[test]
fn variant_double_to_integer_rounds_half_to_even() {
    let mut db = PinkDb::new();
    assert_eq!(
        text(
            &mut db,
            "SELECT CAST(2.5 AS DOUBLE)::VARIANT::INTEGER, \
             CAST(3.5 AS DOUBLE)::VARIANT::INTEGER, \
             CAST(-2.5 AS DOUBLE)::VARIANT::INTEGER;"
        ),
        ["2", "4", "-2"]
    );
}
