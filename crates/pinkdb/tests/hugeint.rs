//! HUGEINT (signed 128-bit integer): steps 1–3 of
//! `docs/slt-3-hugeint-plan.md` — the type model, literals and casts,
//! arithmetic and comparison. Every expected value and every error text here
//! was read from DuckDB 2.0.0-alpha46057 (the reference CLI) on the same
//! expression, so a divergence is a bug in this engine, not in the test.

use pinkdb::{PinkDb, QueryResult, Value};

const MAX: &str = "170141183460469231731687303715884105727";
const MIN: &str = "-170141183460469231731687303715884105728";

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
                .join(" ")
        })
        .collect()
}

#[test]
fn literals_take_the_narrowest_type_that_fits() {
    let mut db = PinkDb::new();
    assert_eq!(
        text(
            &mut db,
            "SELECT typeof(10), typeof(10000000000), \
             typeof(170141183460469231731687303715884105727);"
        ),
        ["INTEGER", "BIGINT", "HUGEINT"]
    );
    assert_eq!(text(&mut db, &format!("SELECT {MAX}, {MIN};")), [MAX, MIN]);
    assert_eq!(
        row(&mut db, &format!("SELECT {MAX};")),
        vec![Value::Hugeint(i128::MAX)]
    );
    assert_eq!(
        row(&mut db, &format!("SELECT {MIN};")),
        vec![Value::Hugeint(i128::MIN)]
    );
    // The sign is part of the literal, so the BIGINT boundary keeps BIGINT
    // on both sides, exactly like DuckDB.
    assert_eq!(
        text(
            &mut db,
            "SELECT typeof(9223372036854775807), typeof(9223372036854775808), \
             typeof(-9223372036854775808), typeof(-9223372036854775809);"
        ),
        ["BIGINT", "HUGEINT", "BIGINT", "HUGEINT"]
    );
    // One digit past i128 is a parse error, not a silent BIGNUM.
    assert!(
        err(&mut db, "SELECT 1701411834604692317316873037158841057280;")
            .contains("integer literal out of range")
    );
}

#[test]
fn declared_hugeint_columns_hold_every_value() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE h (a HUGEINT, b INT128);").unwrap();
    db.execute(&format!("INSERT INTO h VALUES ({MAX}, {MIN}), (NULL, 0);"))
        .unwrap();
    assert_eq!(
        texts(&mut db, "SELECT a, b FROM h ORDER BY a NULLS LAST;"),
        [format!("{MAX} {MIN}"), "NULL 0".to_string()]
    );
    assert_eq!(
        text(&mut db, "SELECT typeof(a) FROM h LIMIT 1;"),
        ["HUGEINT"]
    );
}

#[test]
fn text_to_hugeint_matches_duckdb() {
    let mut db = PinkDb::new();
    assert_eq!(
        text(
            &mut db,
            "SELECT '7'::HUGEINT, '130'::HUGEINT, '924829852'::HUGEINT, \
             '0'::HUGEINT, '-0'::HUGEINT, '-7'::HUGEINT, '-924829852'::HUGEINT;"
        ),
        ["7", "130", "924829852", "0", "0", "-7", "-924829852"]
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT '1267650600228229401496703205376'::HUGEINT, \
             '-1267650600228229401496703205376'::HUGEINT;"
        ),
        [
            "1267650600228229401496703205376",
            "-1267650600228229401496703205376"
        ]
    );
    assert_eq!(
        text(
            &mut db,
            &format!("SELECT '{MAX}'::HUGEINT, '{MIN}'::HUGEINT;")
        ),
        [MAX, MIN]
    );
    // Exponents, fractions (rounded half away from zero), surrounding spaces
    // and digit separators, all exact — no f64 round trip.
    assert_eq!(
        text(
            &mut db,
            "SELECT '3.6e2'::HUGEINT, '1.5'::HUGEINT, '  12  '::HUGEINT, \
             '1e-1'::HUGEINT, '-1.5'::HUGEINT, '1_000'::HUGEINT, \
             '170141183460469231731687303715884105700e0'::HUGEINT;"
        ),
        [
            "360",
            "2",
            "12",
            "0",
            "-2",
            "1000",
            "170141183460469231731687303715884105700"
        ]
    );
    // Hex and binary strings are bit patterns (hex 0x8000… is out of range).
    assert_eq!(
        text(
            &mut db,
            "SELECT '0x07'::HUGEINT, '0b00000000_00101010'::HUGEINT, \
             '0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'::HUGEINT;"
        ),
        ["7", "42", MAX]
    );
    // DuckDB's messages, verbatim.
    assert_eq!(
        err(&mut db, "SELECT 'abc'::HUGEINT;"),
        "Conversion Error: Could not convert string 'abc' to INT128"
    );
    // Beyond 38 fractional digits DuckDB gives up instead of rounding
    // (corpus `test_hugeint_exponent`).
    for v in [
        format!("0.{}1e5", "0".repeat(300)),
        format!("1.{}1e5", "0".repeat(300)),
        format!("1.1{}1e5", "0".repeat(300)),
    ] {
        assert_eq!(
            err(&mut db, &format!("SELECT '{v}'::HUGEINT;")),
            format!("Conversion Error: Could not convert string '{v}' to INT128")
        );
    }
    assert_eq!(
        err(&mut db, "SELECT '1e39'::HUGEINT;"),
        "Conversion Error: Could not convert string '1e39' to INT128"
    );
    assert_eq!(
        err(&mut db, "SELECT ''::HUGEINT;"),
        "Conversion Error: Could not convert string '' to INT128"
    );
    assert_eq!(
        err(
            &mut db,
            "SELECT '170141183460469231731687303715884105728'::HUGEINT;"
        ),
        "Conversion Error: Could not convert string \
         '170141183460469231731687303715884105728' to INT128"
    );
    assert_eq!(
        err(
            &mut db,
            "SELECT '0x80000000000000000000000000000000'::HUGEINT;"
        ),
        "Conversion Error: Could not convert string \
         '0x80000000000000000000000000000000' to INT128"
    );
}

#[test]
fn double_to_hugeint_rounds_half_away_from_zero() {
    let mut db = PinkDb::new();
    assert_eq!(
        text(
            &mut db,
            "SELECT CAST(1.5 AS HUGEINT), CAST(-1.5 AS HUGEINT), CAST(1.9 AS HUGEINT), \
             CAST(2.5 AS HUGEINT), CAST(-2.5 AS HUGEINT), CAST(1e18 AS HUGEINT), \
             CAST(1.2345e20 AS HUGEINT);"
        ),
        [
            "2",
            "-2",
            "2",
            "3",
            "-3",
            "1000000000000000000",
            "123450000000000000000"
        ]
    );
    // DuckDB spells a DOUBLE's exponent with a sign; the test pins that text.
    assert_eq!(
        err(&mut db, "SELECT CAST(1e300 AS HUGEINT);"),
        "Conversion Error: Type DOUBLE with value 1e+300 can't be cast because the value is out \
         of range for the destination type INT128"
    );
    assert_eq!(
        err(&mut db, "SELECT CAST(-1.5e300 AS HUGEINT);"),
        "Conversion Error: Type DOUBLE with value -1.5e+300 can't be cast because the value is \
         out of range for the destination type INT128"
    );
    // The bounds are exclusive, so ±2^127 as a DOUBLE is already out of range
    // (corpus `hugeint_try_cast`).
    assert_eq!(
        err(
            &mut db,
            "SELECT CAST('-170141183460469231731687303715884105729'::DOUBLE AS HUGEINT);"
        ),
        "Conversion Error: Type DOUBLE with value -1.7014118346046923e+38 can't be cast because \
         the value is out of range for the destination type INT128"
    );
    assert_eq!(
        err(&mut db, "SELECT 1.7014118346046923e38::HUGEINT;"),
        "Conversion Error: Type DOUBLE with value 1.7014118346046923e+38 can't be cast because \
         the value is out of range for the destination type INT128"
    );
}

#[test]
fn hugeint_narrows_to_the_smaller_types_or_refuses() {
    let mut db = PinkDb::new();
    assert_eq!(
        text(
            &mut db,
            "SELECT 42::HUGEINT::INTEGER, 42::HUGEINT::BIGINT, 42::HUGEINT::DOUBLE, \
             1::HUGEINT::BOOLEAN, 0::HUGEINT::BOOLEAN, 10000000000000000000::HUGEINT::VARCHAR;"
        ),
        [
            "42",
            "42",
            // pinkdb prints a DOUBLE without a trailing `.0`.
            "42",
            "true",
            "false",
            "10000000000000000000"
        ]
    );
    assert_eq!(
        err(&mut db, "SELECT 9223372036854775808::HUGEINT::BIGINT;"),
        "Conversion Error: Type INT128 with value 9223372036854775808 can't be cast because the \
         value is out of range for the destination type INT64"
    );
    assert_eq!(
        err(&mut db, "SELECT 100000000000::HUGEINT::INTEGER;"),
        "Conversion Error: Type INT128 with value 100000000000 can't be cast because the value is \
         out of range for the destination type INT32"
    );
    assert_eq!(
        err(&mut db, "SELECT '9223372036854775808'::HUGEINT::BIGINT;"),
        "Conversion Error: Type INT128 with value 9223372036854775808 can't be cast because the \
         value is out of range for the destination type INT64"
    );
    // 9223372036854775807 and -9223372036854775808 do fit BIGINT.
    assert_eq!(
        text(
            &mut db,
            "SELECT 9223372036854775807::HUGEINT::BIGINT, \
             '-9223372036854775808'::HUGEINT::BIGINT;"
        ),
        ["9223372036854775807", "-9223372036854775808"]
    );
}

#[test]
fn decimal_and_hugeint_casts_are_exact() {
    let mut db = PinkDb::new();
    assert_eq!(
        text(
            &mut db,
            "SELECT 42::HUGEINT::DECIMAL(38,5), CAST(12.345 AS DECIMAL(10,3))::HUGEINT, \
             CAST(-12.345 AS DECIMAL(10,3))::HUGEINT;"
        ),
        ["42.00000", "12", "-12"]
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT '99999999999999999999999999999999999999'::HUGEINT::DECIMAL(38,0);"
        ),
        ["99999999999999999999999999999999999999"]
    );
    // 10^38 - 1 < i128::MAX, so the extremes cannot become DECIMAL(38,s).
    assert_eq!(
        err(&mut db, &format!("SELECT {MAX}::DECIMAL(38,0);")),
        format!("Conversion Error: Could not cast value {MAX} to DECIMAL(38,0)")
    );
    assert_eq!(
        err(&mut db, &format!("SELECT {MAX}::DECIMAL(38,1);")),
        format!("Conversion Error: Could not cast value {MAX} to DECIMAL(38,1)")
    );
    assert_eq!(
        err(&mut db, &format!("SELECT {MIN}::DECIMAL(38,1);")),
        format!("Conversion Error: Could not cast value {MIN} to DECIMAL(38,1)")
    );
}

#[test]
fn insert_coerces_double_and_text_into_a_hugeint_column() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE agg (g INTEGER, h HUGEINT);")
        .unwrap();
    db.execute(
        "INSERT INTO agg VALUES (1, 42.0), (2, 1267650600228229401496703205376), \
         (2, -439847238974238975238975), (1, '-12'), (1, 7::BIGINT);",
    )
    .unwrap();
    assert_eq!(
        texts(
            &mut db,
            "SELECT g, min(h), max(h) FROM agg GROUP BY g ORDER BY g;"
        ),
        [
            "1 -12 42".to_string(),
            "2 -439847238974238975238975 1267650600228229401496703205376".to_string()
        ]
    );
}

#[test]
fn hugeint_arithmetic_matches_duckdb() {
    let mut db = PinkDb::new();
    assert_eq!(
        text(
            &mut db,
            "SELECT 42::HUGEINT + 42::HUGEINT, 42 + -42, -42 + 100, 3 - 5, -100 - 42, \
             12 - (-12), 100 * -50, -100 * -50;"
        ),
        ["84", "0", "58", "-2", "-142", "24", "-5000", "5000"]
    );
    assert_eq!(
        text(&mut db, "SELECT 90 % 20, -90 % 20, 90 % -20, -90 % -20;"),
        ["10", "-10", "10", "-10"]
    );
    // Result types: HUGEINT with any narrower integer, DOUBLE for `/`.
    assert_eq!(
        text(
            &mut db,
            "SELECT 42::HUGEINT / 4, 42::HUGEINT + 4::INTEGER, typeof(42::HUGEINT + 4), \
             typeof(42::HUGEINT * 2::BIGINT), typeof(42::HUGEINT % 4), \
             typeof(42::HUGEINT + 1e300);"
        ),
        ["10.5", "46", "HUGEINT", "HUGEINT", "HUGEINT", "DOUBLE"]
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT -(100::HUGEINT), -(-(50::HUGEINT)), -(0::HUGEINT), \
             abs('-100'::HUGEINT), abs(0::HUGEINT);"
        ),
        ["-100", "50", "0", "100", "0"]
    );
}

#[test]
fn hugeint_overflow_uses_duckdbs_text() {
    let mut db = PinkDb::new();
    assert_eq!(
        err(&mut db, &format!("SELECT {MAX} + 1;")),
        format!("Out of Range Error: Overflow in addition of INT128 ({MAX} + 1)!")
    );
    assert_eq!(
        err(&mut db, &format!("SELECT 1 + {MAX};")),
        format!("Out of Range Error: Overflow in addition of INT128 (1 + {MAX})!")
    );
    assert_eq!(
        err(&mut db, &format!("SELECT {MIN} - 1;")),
        format!("Out of Range Error: Overflow in subtraction of INT128 ({MIN} - 1)!")
    );
    assert_eq!(
        err(&mut db, &format!("SELECT {MAX} * 2;")),
        format!("Out of Range Error: Overflow in multiplication of INT128 ({MAX} * 2)!")
    );
    assert_eq!(
        err(&mut db, &format!("SELECT {MIN} % -1;")),
        format!("Out of Range Error: Overflow in division of {MIN} / -1")
    );
    assert_eq!(
        err(&mut db, &format!("SELECT -('{MIN}'::HUGEINT);")),
        "Out of Range Error: Overflow in negation of numeric value!"
    );
    assert_eq!(
        err(&mut db, &format!("SELECT abs('{MIN}'::HUGEINT);")),
        format!("Out of Range Error: Overflow on abs({MIN})")
    );
    assert_eq!(
        err(&mut db, "SELECT 5::HUGEINT % 0;"),
        "Invalid Input Error: Division by zero in expression (5 % 0). Use TRY(...) to return NULL \
         for this expression, or SET error_on_division_by_zero=false to return NULL for all \
         divisions by zero."
    );
}

#[test]
fn hugeint_comparisons_and_filters() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE c (h HUGEINT);").unwrap();
    db.execute("INSERT INTO c VALUES (42), ('1267650600228229401496703205376'), (NULL);")
        .unwrap();
    assert_eq!(text(&mut db, "SELECT h FROM c WHERE h < 100; "), ["42"]);
    assert_eq!(text(&mut db, "SELECT count(*) FROM c WHERE h = 42;"), ["1"]);
    assert_eq!(
        text(
            &mut db,
            "SELECT count(*) FROM c WHERE h <> '1267650600228229401496703205376'::HUGEINT;"
        ),
        ["1"]
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT count(*) FROM c WHERE h < '1267650600228229401496703205376';"
        ),
        ["1"]
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT count(*) FROM c WHERE h <= '1267650600228229401496703205376';"
        ),
        ["2"]
    );
    // The literal on the left, and the narrower widths on the right.
    assert_eq!(text(&mut db, "SELECT count(*) FROM c WHERE 42 < h;"), ["1"]);
    assert_eq!(
        text(&mut db, "SELECT count(*) FROM c WHERE h > 41::INTEGER;"),
        ["2"]
    );
    assert_eq!(
        text(&mut db, "SELECT count(*) FROM c WHERE h > 41::BIGINT;"),
        ["2"]
    );
    assert_eq!(
        text(&mut db, "SELECT count(*) FROM c WHERE h < 42.5;"),
        ["1"]
    );
    assert_eq!(
        text(&mut db, "SELECT count(*) FROM c WHERE h = 42.0::DOUBLE;"),
        ["1"]
    );
    // Both sides as HUGEINT vectors take the same-dtype kernel.
    assert_eq!(
        text(
            &mut db,
            "SELECT 1::HUGEINT = 1, 1::HUGEINT = 1.0, 1::HUGEINT < 2, \
             1::HUGEINT = 1::BIGINT, 5::HUGEINT <> 5::HUGEINT;"
        ),
        ["true", "true", "true", "true", "false"]
    );
}

#[test]
fn hugeint_keys_group_with_the_narrower_integers() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE g (k HUGEINT);").unwrap();
    db.execute("INSERT INTO g VALUES (1), ('1'), (1::BIGINT), (2), (-1), (2);")
        .unwrap();
    assert_eq!(
        texts(&mut db, "SELECT k, count(*) FROM g GROUP BY k ORDER BY k;"),
        ["-1 1", "1 3", "2 2"]
    );
    assert_eq!(
        texts(&mut db, "SELECT DISTINCT k FROM g ORDER BY k;"),
        ["-1", "1", "2"]
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT typeof(min(k)), typeof(max(k)), min(k), max(k), count(DISTINCT k) FROM g;"
        ),
        ["HUGEINT", "HUGEINT", "-1", "2", "3"]
    );
}

#[test]
fn hugeint_reaching_a_storage_boundary_names_the_boundary() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE s (h HUGEINT); INSERT INTO s VALUES (1);")
        .unwrap();
    let e = db
        .to_pink_bytes()
        .expect_err("a .pink file has no HUGEINT layout yet")
        .to_string();
    assert!(
        e.contains("HUGEINT is not yet supported in .pink storage"),
        "{e}"
    );
}
