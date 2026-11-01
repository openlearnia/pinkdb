//! Wave 126a: a point literal is DECIMAL(width, scale) like DuckDB's
//! `NumberToValue` (width counts every written digit, scale the digits after
//! the point); exponents and over-38-digit literals stay DOUBLE. Everything
//! that used to lean on "a literal is a DOUBLE" is pinned here: constant
//! folding, type unification, assignment casts, filter pushdown (the literal
//! resolves against the column's domain at bind time so no per-row decimal
//! conversion of the column is needed) and ARRAY elements.

use pinkdb::{PinkDb, QueryResult, Value};

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn one(db: &mut PinkDb, sql: &str) -> Value {
    let rows = rows(db, sql);
    assert_eq!(rows.len(), 1, "{sql} -> {rows:?}");
    rows[0][0].clone()
}

fn text(db: &mut PinkDb, sql: &str) -> String {
    match one(db, sql) {
        Value::Varchar(s) => s,
        other => panic!("expected VARCHAR from {sql}, got {other:?}"),
    }
}

/// The whole EXPLAIN text of `sql`, one line per plan row.
fn plan(db: &mut PinkDb, sql: &str) -> String {
    rows(db, &format!("EXPLAIN {sql};"))
        .into_iter()
        .map(|r| match &r[0] {
            Value::Varchar(s) => s.clone(),
            other => panic!("EXPLAIN row {other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn dec(value: i128, precision: u8, scale: u8) -> Value {
    Value::Decimal {
        value,
        precision,
        scale,
    }
}

#[test]
fn literal_types_follow_duckdb() {
    let mut db = PinkDb::new();
    // typeof() reads the value's carried precision, so these are exact.
    assert_eq!(text(&mut db, "SELECT typeof(1.5);"), "DECIMAL(2,1)");
    assert_eq!(text(&mut db, "SELECT typeof(0.06);"), "DECIMAL(3,2)");
    assert_eq!(text(&mut db, "SELECT typeof(.5);"), "DECIMAL(1,1)");
    assert_eq!(text(&mut db, "SELECT typeof(1.50);"), "DECIMAL(3,2)");
    assert_eq!(
        text(&mut db, "SELECT typeof(12345678901234567890.5);"),
        "DECIMAL(21,1)"
    );
    // An exponent or more than 38 written digits is DOUBLE.
    assert_eq!(text(&mut db, "SELECT typeof(1e3);"), "DOUBLE");
    assert_eq!(text(&mut db, "SELECT typeof(1.5e2);"), "DOUBLE");
    assert_eq!(text(&mut db, "SELECT typeof(-1.5);"), "DECIMAL(2,1)");
    assert_eq!(
        text(&mut db, "SELECT typeof(1_000.5);"),
        "DECIMAL(5,1)",
        "underscores separate digits without widening the literal"
    );
    // The written scale survives: 1.50 prints with both zeros.
    assert_eq!(one(&mut db, "SELECT 1.50;"), dec(150, 3, 2));
    assert_eq!(one(&mut db, "SELECT .5;"), dec(5, 1, 1));
}

#[test]
fn folding_keeps_the_expression_type() {
    let mut db = PinkDb::new();
    // Both operands are folded, but the folded literal carries the bound
    // result type — not the minimum width of the value.
    assert_eq!(text(&mut db, "SELECT typeof(3 * 0.5);"), "DECIMAL(12,1)");
    assert_eq!(one(&mut db, "SELECT 3 * 0.5;"), dec(15, 12, 1));
    assert_eq!(text(&mut db, "SELECT typeof(1.5 + 1);"), "DECIMAL(12,1)");
    assert_eq!(text(&mut db, "SELECT typeof(0.1 + 0.2);"), "DECIMAL(3,1)");
    assert_eq!(one(&mut db, "SELECT 0.1 + 0.2;"), dec(3, 3, 1));
    assert!(matches!(
        one(&mut db, "SELECT 0.1 + 0.2 = 0.3;"),
        Value::Boolean(true)
    ));
    // Division and the math functions stay DOUBLE.
    assert_eq!(text(&mut db, "SELECT typeof(10 / 4.0);"), "DOUBLE");
    assert_eq!(one(&mut db, "SELECT 10 / 4.0;"), Value::Double(2.5));
    assert_eq!(text(&mut db, "SELECT typeof(sqrt(2.0));"), "DOUBLE");
    assert_eq!(text(&mut db, "SELECT typeof(power(1.5, 2));"), "DOUBLE");
    // Unaliased names keep the literal's written text.
    assert_eq!(
        one(&mut db, "SELECT 1.5 = 1.50;"),
        Value::Boolean(true),
        "(1.5 = 1.50) compares exactly"
    );
}

#[test]
fn mixing_with_other_types_matches_duckdb() {
    let mut db = PinkDb::new();
    // INTEGER widens to DECIMAL(10,0) next to a decimal literal.
    assert_eq!(text(&mut db, "SELECT typeof(1.5 + 1);"), "DECIMAL(12,1)");
    assert_eq!(
        text(
            &mut db,
            "SELECT typeof(CASE WHEN true THEN 1.5 ELSE 2 END);"
        ),
        "DECIMAL(11,1)"
    );
    assert_eq!(
        one(&mut db, "SELECT CASE WHEN true THEN 1.5 ELSE 2 END;"),
        dec(15, 11, 1)
    );
    assert_eq!(
        text(&mut db, "SELECT typeof(least(1.5, 2.25));"),
        "DECIMAL(3,2)"
    );
    assert_eq!(
        text(&mut db, "SELECT typeof(greatest(1.5, 2));"),
        "DECIMAL(11,1)"
    );
    assert_eq!(
        text(&mut db, "SELECT typeof(coalesce(NULL, 1.5));"),
        "DECIMAL(2,1)"
    );
    assert_eq!(text(&mut db, "SELECT typeof(1.5 % 1);"), "DECIMAL(11,1)");
    // IN and BETWEEN are comparisons: BOOLEAN, and exact in the decimal domain.
    assert_eq!(
        rows(
            &mut db,
            "SELECT 1.5 IN (1.5, 2.5), 2 IN (1.5, 2.5), 1.25 BETWEEN 1.5 AND 2.5;"
        ),
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(false),
        ]]
    );
    // UNION unifies the two branches.
    let union_types = text(
        &mut db,
        "SELECT typeof(v) FROM (SELECT 1.5 AS v UNION ALL SELECT 2) u LIMIT 1;",
    );
    assert_eq!(union_types, "DECIMAL(11,1)");
}

#[test]
fn assignment_casts_follow_duckdb() {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE t (d DOUBLE, i INTEGER, b BIGINT, s VARCHAR, \
         m DECIMAL(4,2), n DECIMAL(2,1));",
    )
    .unwrap();
    // DuckDB rounds half away from zero into integers and smaller scales.
    db.execute("INSERT INTO t VALUES (1.5, 1.5, 1.5, 1.5, 1.567, 1.567);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (NULL, 2.5, 2.5, NULL, NULL, NULL);")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT d, i, b, s, m, n FROM t ORDER BY i;"),
        vec![
            vec![
                Value::Double(1.5),
                Value::Integer(2),
                Value::Bigint(2),
                Value::Varchar("1.5".to_string()),
                dec(157, 4, 2),
                dec(16, 2, 1),
            ],
            vec![
                Value::Null,
                Value::Integer(3),
                Value::Bigint(3),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
        ]
    );
    // A decimal that does not fit the column is refused, not truncated.
    assert!(db.execute("INSERT INTO t (n) VALUES (100.0);").is_err());
}

#[test]
fn filters_resolve_the_literal_in_the_column_domain() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (d DOUBLE, i INTEGER, x DECIMAL(15,2));")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1.5, 3, 10.25), (2.5, 7, 0.00);")
        .unwrap();
    db.execute("CREATE INDEX ixi ON t(i);").unwrap();

    // DOUBLE column: the literal is a DOUBLE by the time the filter runs.
    let d_plan = plan(&mut db, "SELECT * FROM t WHERE d > 1.5");
    assert!(d_plan.contains("Filter (#0 > 1.5)"), "{d_plan}");
    assert!(
        !d_plan.contains("CAST"),
        "the column must keep its domain: {d_plan}"
    );
    assert_eq!(
        rows(&mut db, "SELECT i FROM t WHERE d > 1.5 ORDER BY i;"),
        vec![vec![Value::Integer(7)]]
    );

    // INTEGER column: an integral decimal literal becomes an INTEGER probe, so
    // the index scan stays available and no cast wraps the column.
    let i_plan = plan(&mut db, "SELECT * FROM t WHERE i = 2.0");
    assert!(i_plan.contains("Filter (#1 = 2)"), "{i_plan}");
    assert!(!i_plan.contains("CAST"), "{i_plan}");
    assert_eq!(
        rows(&mut db, "SELECT i FROM t WHERE i = 3.0 ORDER BY i;"),
        vec![vec![Value::Integer(3)]]
    );
    assert!(
        db.execute("SELECT * FROM t WHERE i = 2.0;")
            .unwrap()
            .pop()
            .is_some()
    );

    // A fractional literal cannot be an INTEGER probe; the comparison still
    // keeps the column's domain (no cast of the column) and is exact.
    let f_plan = plan(&mut db, "SELECT * FROM t WHERE i < 4.5");
    assert!(!f_plan.contains("CAST"), "{f_plan}");
    assert_eq!(
        rows(&mut db, "SELECT i FROM t WHERE i < 4.5 ORDER BY i;"),
        vec![vec![Value::Integer(3)]]
    );

    // DECIMAL column: the literal stays decimal (exact) and is rescaled by the
    // comparison kernel, not by a cast node.
    let x_plan = plan(&mut db, "SELECT * FROM t WHERE x > 1.5");
    assert!(x_plan.contains("Filter (#2 > 1.5)"), "{x_plan}");
    assert!(!x_plan.contains("CAST"), "{x_plan}");
    assert_eq!(
        rows(&mut db, "SELECT x FROM t WHERE x > 1.5;"),
        vec![vec![dec(1025, 15, 2)]]
    );
}

#[test]
fn array_elements_type_as_double() {
    let mut db = PinkDb::new();
    // ARRAY has no DECIMAL element kind, so decimal literals type as DOUBLE
    // (DuckDB gives DECIMAL(2,1)[] — a documented divergence).
    assert_eq!(text(&mut db, "SELECT typeof([1.5, 2.5]);"), "DOUBLE[2]");
    assert_eq!(
        text(&mut db, "SELECT typeof(array_value(1.5, 2.5));"),
        "DOUBLE[2]"
    );
    assert_eq!(
        text(&mut db, "SELECT typeof([1, 2.5]);"),
        "DOUBLE[2]",
        "an INTEGER element widens to DOUBLE next to a decimal"
    );
    assert_eq!(one(&mut db, "SELECT [1.5, 2.5][1];"), Value::Double(1.5));
    assert_eq!(
        text(&mut db, "SELECT CAST([1.5, 2.5] AS VARCHAR);"),
        "[1.5, 2.5]"
    );
    // Arrays keep working with casts, indexing and storage.
    db.execute("CREATE TABLE a (v DOUBLE[2]);").unwrap();
    db.execute("INSERT INTO a VALUES ([1.5, 2.5]), ([3.0, 4.0]);")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT v[1], v[2] FROM a ORDER BY v[1];"),
        vec![
            vec![Value::Double(1.5), Value::Double(2.5)],
            vec![Value::Double(3.0), Value::Double(4.0)],
        ]
    );
    assert_eq!(
        text(&mut db, "SELECT typeof(CAST([1.5, 2.5] AS DOUBLE[2]));"),
        "DOUBLE[2]"
    );
}

#[test]
fn row_counts_round_decimals_like_duckdb() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE s (i INTEGER);").unwrap();
    db.execute("INSERT INTO s VALUES (1), (2), (3), (4), (5);")
        .unwrap();
    // LIMIT/OFFSET and window frame bounds cast a decimal to BIGINT with
    // DuckDB's half-away-from-zero rounding (2.5 -> 3 rows).
    assert_eq!(
        rows(&mut db, "SELECT i FROM s ORDER BY i LIMIT 2.5;").len(),
        3
    );
    assert_eq!(
        rows(&mut db, "SELECT i FROM s ORDER BY i OFFSET 1.5;"),
        vec![
            vec![Value::Integer(3)],
            vec![Value::Integer(4)],
            vec![Value::Integer(5)]
        ]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT i, sum(i) OVER (ORDER BY i ROWS BETWEEN 2.5 PRECEDING \
             AND CURRENT ROW) AS s FROM s ORDER BY i;"
        ),
        vec![
            vec![Value::Integer(1), Value::Bigint(1)],
            vec![Value::Integer(2), Value::Bigint(3)],
            vec![Value::Integer(3), Value::Bigint(6)],
            vec![Value::Integer(4), Value::Bigint(10)],
            vec![Value::Integer(5), Value::Bigint(14)],
        ]
    );
}

#[test]
fn aggregates_keep_decimal_types() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE m (x DECIMAL(15,2));").unwrap();
    db.execute("INSERT INTO m VALUES (10.25), (0.00);").unwrap();
    assert_eq!(
        text(&mut db, "SELECT typeof(x - 1.5) FROM m LIMIT 1;"),
        "DECIMAL(16,2)"
    );
    assert_eq!(
        one(&mut db, "SELECT x - 1.5 FROM m ORDER BY x DESC LIMIT 1;"),
        dec(875, 16, 2)
    );
    assert_eq!(
        text(&mut db, "SELECT typeof(sum(x - 1.5)) FROM m;"),
        "DECIMAL(38,2)"
    );
    assert_eq!(one(&mut db, "SELECT sum(x - 1.5) FROM m;"), dec(725, 38, 2));
    // avg() is DOUBLE in DuckDB, and the decimal product stays exact.
    assert_eq!(
        text(&mut db, "SELECT typeof(avg(x * 0.1)) FROM m;"),
        "DOUBLE"
    );
    assert_eq!(
        one(&mut db, "SELECT avg(x * 0.1) FROM m;"),
        Value::Double(0.5125)
    );
}
