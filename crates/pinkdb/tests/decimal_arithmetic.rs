//! Wave 126: decimal arithmetic fast paths, DuckDB's 18-digit multiplication
//! typing and exact AVG. Every expected value here was read from DuckDB 2.0
//! (the reference CLI) on the same data.

use pinkdb::{PinkDb, QueryResult, Value};

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql)
        .expect_err(&format!("{sql} should fail"))
        .to_string()
}

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    for sql in [
        "CREATE TABLE d (x DECIMAL(15,2), i BIGINT, g INTEGER);",
        "INSERT INTO d VALUES (0.10, 9007199254740993, 1), (0.20, 9007199254740993, 1), \
         (0.10, 1, 1), (12345678901.23, 3, 2), (98765432109.87, 4, 2), (NULL, NULL, 2);",
    ] {
        db.execute(sql).unwrap();
    }
    db
}

#[test]
fn multiplying_two_18_digit_decimals_is_typed_decimal_18() {
    let mut db = PinkDb::new();
    let typ = rows(
        &mut db,
        "SELECT typeof(CAST(1 AS DECIMAL(15,2)) * CAST(1 AS DECIMAL(15,2)));",
    );
    assert_eq!(typ, vec![vec![Value::Varchar("DECIMAL(18,4)".into())]]);
    // A wider input keeps the summed width (capped at 38), as in DuckDB.
    let wide = rows(
        &mut db,
        "SELECT typeof(CAST(1 AS DECIMAL(20,2)) * CAST(1 AS DECIMAL(15,2)));",
    );
    assert_eq!(wide, vec![vec![Value::Varchar("DECIMAL(35,4)".into())]]);
}

#[test]
fn an_18_digit_product_past_its_width_is_duckdbs_overflow_error() {
    let mut db = store();
    let e = err(&mut db, "SELECT x * x FROM d;");
    assert!(
        e.contains(
            "Overflow in multiplication of DECIMAL(18) (1234567890123 * 1234567890123). \
             You might want to add an explicit cast to a bigger decimal."
        ),
        "{e}"
    );
    // Rows that fit compute exactly through the fast path.
    let small = rows(&mut db, "SELECT x * x FROM d WHERE g = 1 ORDER BY x;");
    assert_eq!(
        small.iter().map(|r| r[0].to_string()).collect::<Vec<_>>(),
        ["0.0100", "0.0100", "0.0400"]
    );
}

#[test]
fn a_chunk_with_one_overflowing_row_still_errors() {
    // 3,000 small rows and one that overflows DECIMAL(18) when squared: the
    // chunk bound fails, the checked loop runs and raises the error.
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (x DECIMAL(15,2));").unwrap();
    let mut values: Vec<String> = (0..3000).map(|i| format!("({}.25)", i % 97)).collect();
    values.push("(9999999999999.99)".into());
    db.execute(&format!("INSERT INTO t VALUES {};", values.join(",")))
        .unwrap();
    let e = err(&mut db, "SELECT sum(x * x) FROM t;");
    assert!(
        e.contains("Overflow in multiplication of DECIMAL(18)"),
        "{e}"
    );
    // With the large row filtered out, the query answers: the aggregate
    // reads through the filter's selection, and a row the filter dropped must
    // not raise an error no kept row does (DuckDB answers this query).
    let ok = rows(
        &mut db,
        "SELECT sum(x * x), sum(x + x), sum(x - 1) FROM t WHERE x < 1000;",
    );
    assert_eq!(
        ok[0].iter().map(|v| v.to_string()).collect::<Vec<_>>(),
        ["9297075.0000", "288870.00", "141435.00"]
    );
}

#[test]
fn avg_is_exact_like_duckdb() {
    let mut db = store();
    let out = rows(
        &mut db,
        "SELECT g, avg(x), avg(i) FROM d GROUP BY g ORDER BY g;",
    );
    // DuckDB 2.0: 0.13333333333333333 | 6004799503160663.0 and
    // 55555555505.55 | 3.5. A float sum per row gives a different last digit
    // for the first and loses the integers above 2^53.
    assert_eq!(out[0][1], Value::Double(0.13333333333333333));
    assert_eq!(out[0][2], Value::Double(6004799503160663.0));
    assert_eq!(out[1][1], Value::Double(55555555505.55));
    assert_eq!(out[1][2], Value::Double(3.5));
    // AVG over DOUBLE keeps its float sum.
    let f = rows(&mut db, "SELECT avg(CAST(x AS DOUBLE)) FROM d WHERE g = 1;");
    assert!(matches!(f[0][0], Value::Double(v) if (v - 0.13333333333333333).abs() < 1e-15));
    // Empty and all-NULL groups are NULL.
    let none = rows(&mut db, "SELECT avg(x) FROM d WHERE x IS NULL;");
    assert_eq!(none, vec![vec![Value::Null]]);
}
