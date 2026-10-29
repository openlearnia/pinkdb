//! Wave 127d: a filter's `column <op> literal` runs in the column's own
//! integer domain (a DECIMAL against a DOUBLE literal as an integer range).
//! Every filter must keep exactly the rows the per-row comparison marks
//! TRUE, which the same comparison in a projection (`CASE WHEN …`) shows:
//! both operand orders, every operator, NULL cells, literals that sit on,
//! between and just off the column's grid, and literals out of its range.

use pinkdb::{PinkDb, QueryResult, Value};

fn count(db: &mut PinkDb, sql: &str) -> i64 {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => match &rows[0][0] {
            Value::Bigint(n) => *n,
            Value::Null => 0,
            other => match other.to_string().parse::<f64>() {
                Ok(n) => n as i64,
                Err(_) => panic!("{sql}: {other:?}"),
            },
        },
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (x DECIMAL(15,2), i INTEGER, b BIGINT, d DATE, ts TIMESTAMP);")
        .unwrap();
    let values: Vec<String> = (-3000i64..3000)
        .map(|n| {
            if n % 41 == 0 {
                "(NULL, NULL, NULL, NULL, NULL)".to_string()
            } else {
                let c = n.abs() % 100;
                let x = format!("{}{}.{:02}", if n < 0 { "-" } else { "" }, n.abs() / 100, c);
                let k = n.rem_euclid(336);
                let sec = n.rem_euclid(5000);
                format!(
                    "({x}, {n}, {}, DATE '2024-{:02}-{:02}', TIMESTAMP '2024-01-01 {:02}:{:02}:{:02}')",
                    n * 1_000_000_007,
                    1 + k / 28,
                    1 + k % 28,
                    sec / 3600,
                    sec / 60 % 60,
                    sec % 60
                )
            }
        })
        .collect();
    db.execute(&format!("INSERT INTO t VALUES {};", values.join(",")))
        .unwrap();
    db
}

const OPS: [&str; 6] = ["<", "<=", ">", ">=", "=", "<>"];

/// Every operator in both orders: the filter keeps what the projection
/// marks TRUE.
fn check(db: &mut PinkDb, col: &str, literal: &str) {
    for op in OPS {
        for cond in [
            format!("{col} {op} {literal}"),
            format!("{literal} {op} {col}"),
        ] {
            let kept = count(db, &format!("SELECT count(*) FROM t WHERE {cond};"));
            let marked = count(
                db,
                &format!("SELECT sum(CASE WHEN {cond} THEN 1 ELSE 0 END) FROM t;"),
            );
            assert_eq!(kept, marked, "{cond}");
        }
    }
}

#[test]
fn decimal_against_double_literals_keeps_the_per_row_answer() {
    let mut db = store();
    for literal in [
        "0.05",
        "0.06 - 0.01",
        "0.06 + 0.01",
        "0.055",
        "-0.005",
        "-12.34",
        "29.99",
        "1e300",
        "-1e300",
        "CAST('NaN' AS DOUBLE)",
        "CAST('inf' AS DOUBLE)",
    ] {
        check(&mut db, "x", literal);
    }
}

#[test]
fn decimal_against_decimal_and_integer_literals() {
    let mut db = store();
    for literal in [
        "CAST(0.05 AS DECIMAL(4,2))",
        "CAST(-7.5 AS DECIMAL(3,1))",
        "CAST(0.055 AS DECIMAL(4,3))",
        "24",
        "-3",
        "99999999999999",
    ] {
        check(&mut db, "x", literal);
    }
}

#[test]
fn integer_date_and_timestamp_columns() {
    let mut db = store();
    for literal in ["0", "-2999", "17", "2147483647", "9999999999"] {
        check(&mut db, "i", literal);
    }
    for literal in ["0", "1000000007", "-17", "9223372036854775807"] {
        check(&mut db, "b", literal);
    }
    for literal in [
        "DATE '2024-03-01'",
        "DATE '2023-01-01'",
        "DATE '2025-06-30'",
    ] {
        check(&mut db, "d", literal);
    }
    check(&mut db, "ts", "TIMESTAMP '2024-01-01 00:30:00'");
}
