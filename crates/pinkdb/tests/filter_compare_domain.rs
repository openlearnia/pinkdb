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

/// `column <op> column` of one layout (TPC-H Q4's `l_commitdate <
/// l_receiptdate`) compares in place over the selection: the filter keeps
/// what the projection marks TRUE, NULL on either side never kept.
#[test]
fn column_against_column_of_the_same_type() {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE u (a INTEGER, b INTEGER, c BIGINT, d BIGINT, x DECIMAL(9,2), y DECIMAL(9,2), \
         p DATE, q DATE, s VARCHAR, t VARCHAR);",
    )
    .unwrap();
    let cell = |n: i64, every: i64, v: String| if n % every == 0 { "NULL".into() } else { v };
    let values: Vec<String> = (0..5000i64)
        .map(|n| {
            let (l, r) = (n * 31 % 97, n * 17 % 89);
            format!(
                "({}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                cell(n, 23, l.to_string()),
                cell(n, 29, r.to_string()),
                cell(n, 31, (l * 1_000_000_007).to_string()),
                cell(n, 37, (r * 1_000_000_007).to_string()),
                cell(n, 41, format!("{}.{:02}", l / 10, l % 10 * 7)),
                cell(n, 43, format!("{}.{:02}", r / 10, r % 10 * 7)),
                cell(n, 47, format!("DATE '2024-01-{:02}'", 1 + l % 28)),
                cell(n, 53, format!("DATE '2024-01-{:02}'", 1 + r % 28)),
                cell(n, 59, format!("'v{}'", l % 30)),
                cell(n, 61, format!("'v{}'", r % 30)),
            )
        })
        .collect();
    db.execute(&format!("INSERT INTO u VALUES {};", values.join(",")))
        .unwrap();
    for (l, r) in [("a", "b"), ("c", "d"), ("x", "y"), ("p", "q"), ("s", "t")] {
        for op in OPS {
            let cond = format!("{l} {op} {r}");
            // A first conjunct narrows the selection, so the pair compares
            // through it rather than over the whole chunk.
            for sql in [
                format!("FROM u WHERE {cond}"),
                format!("FROM u WHERE a > 10 AND {cond}"),
            ] {
                let kept = count(&mut db, &format!("SELECT count(*) {sql};"));
                let marked = count(
                    &mut db,
                    &format!(
                        "SELECT sum(CASE WHEN {cond} THEN 1 ELSE 0 END) {};",
                        sql.replace(&format!(" AND {cond}"), "")
                            .replace(&format!("WHERE {cond}"), "")
                    ),
                );
                assert_eq!(kept, marked, "{sql}");
            }
        }
    }
}

/// `e IN (v1, v2, …)` — lowered to `e = v1 OR e = v2 OR …` — evaluates `e`
/// once and looks each row up in the value list (TPC-H Q22's
/// `substring(c_phone, 1, 2) IN (…)`). The filter keeps what the projection
/// marks TRUE: computed and bare-column operands, NULL cells, a list with a
/// NULL or a value of another type (the generic path), a narrowed selection.
#[test]
fn in_lists_evaluate_their_operand_once() {
    let mut db = store();
    db.execute("CREATE TABLE w (s VARCHAR, i INTEGER, b BIGINT, d DATE);")
        .unwrap();
    let values: Vec<String> = (0..4000i64)
        .map(|n| {
            let s = if n % 13 == 0 {
                "NULL".to_string()
            } else {
                format!("'{}-{}'", n * 7 % 40, n % 9)
            };
            let i = if n % 17 == 0 {
                "NULL".to_string()
            } else {
                (n % 50).to_string()
            };
            format!(
                "({s}, {i}, {}, DATE '2024-02-{:02}')",
                n * 1_000_003 % 77,
                1 + n % 28
            )
        })
        .collect();
    db.execute(&format!("INSERT INTO w VALUES {};", values.join(",")))
        .unwrap();
    for list in [
        "substring(s, 1, 2) IN ('13', '31', '2-', '17', '9-')",
        "substring(s, 1, 2) IN ('13', NULL, '31')",
        "s IN ('13-4', '31-1', '0-0', '7-7')",
        "i IN (3, 7, 49, 50)",
        "i IN (3, 7.5)",
        "b IN (0, 3, 76)",
        "d IN (DATE '2024-02-03', DATE '2024-02-28')",
        "i + 1 IN (1, 2, 3)",
    ] {
        for (filter, all) in [
            (format!("WHERE {list}"), String::new()),
            (
                format!("WHERE i > 20 AND {list}"),
                "WHERE i > 20".to_string(),
            ),
        ] {
            let kept = count(&mut db, &format!("SELECT count(*) FROM w {filter};"));
            let marked = count(
                &mut db,
                &format!("SELECT sum(CASE WHEN {list} THEN 1 ELSE 0 END) FROM w {all};"),
            );
            assert_eq!(kept, marked, "{filter}");
        }
    }
}

/// `text_column <op> 'literal'` decides each shared string once (wave
/// 127h): a store reopened from disk gives a dictionary-encoded column's rows
/// shared strings, an in-memory table separate ones, and a high-cardinality
/// column overflows the per-chunk memo. All three keep what the projection
/// marks TRUE.
#[test]
fn text_comparisons_over_shared_and_separate_strings() {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_text_cmp_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("db.pink");
    let path = path.to_str().unwrap();
    let values: Vec<String> = (0..6000i64)
        .map(|n| {
            let flag = if n % 23 == 0 {
                "NULL".to_string()
            } else {
                format!("'{}'", ["R", "A", "N", "", "Rx"][n as usize % 5])
            };
            let wide = if n % 29 == 0 {
                "NULL".to_string()
            } else {
                format!("'w{}'", n % 700)
            };
            format!("({flag}, {wide})")
        })
        .collect();
    {
        let mut db = PinkDb::open_durable(path).unwrap();
        db.execute("CREATE TABLE t (f VARCHAR, w VARCHAR);")
            .unwrap();
        db.execute(&format!("INSERT INTO t VALUES {};", values.join(",")))
            .unwrap();
        db.checkpoint().unwrap();
        for (col, lit) in [("f", "'R'"), ("f", "''"), ("f", "'N'"), ("w", "'w350'")] {
            check(&mut db, col, lit);
        }
    }
    let mut db = PinkDb::open_durable(path).unwrap();
    for (col, lit) in [
        ("f", "'R'"),
        ("f", "''"),
        ("f", "'N'"),
        ("w", "'w350'"),
        ("w", "'w7'"),
    ] {
        check(&mut db, col, lit);
    }
    std::fs::remove_dir_all(&dir).ok();
}

/// A filter copies only the columns its consumers read (wave 127i): a column
/// only the predicate reads stays a placeholder. Joins, aggregates, DISTINCT
/// and a consumer that does read the filtered column all answer as before.
#[test]
fn a_filter_copies_only_the_columns_read_above_it() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE f (k INTEGER, a INTEGER, b DATE, c DATE, s VARCHAR);")
        .unwrap();
    db.execute("CREATE TABLE d (k INTEGER, name VARCHAR);")
        .unwrap();
    let values: Vec<String> = (0..6000i64)
        .map(|n| {
            format!(
                "({}, {}, DATE '2024-01-{:02}', DATE '2024-01-{:02}', 's{}')",
                n % 50,
                n % 97,
                1 + n % 28,
                1 + n * 7 % 28,
                n % 13
            )
        })
        .collect();
    db.execute(&format!("INSERT INTO f VALUES {};", values.join(",")))
        .unwrap();
    let dims: Vec<String> = (0..50).map(|k| format!("({k}, 'n{}')", k % 7)).collect();
    db.execute(&format!("INSERT INTO d VALUES {};", dims.join(",")))
        .unwrap();
    let keep = |n: i64| 1 + n % 28 > 1 + n * 7 % 28;
    let total: i64 = (0..6000).filter(|&n| keep(n)).map(|n| n % 97).sum();
    let kept = (0..6000).filter(|&n| keep(n)).count() as i64;
    // Join above the filter reads neither date.
    assert_eq!(
        count(
            &mut db,
            "SELECT sum(f.a) FROM f, d WHERE f.k = d.k AND f.b > f.c;"
        ),
        total
    );
    // DISTINCT reads whole rows; an aggregate reads one column.
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM (SELECT DISTINCT * FROM f WHERE b > c) q;"
        ),
        kept
    );
    // A consumer that reads a filtered column still sees it.
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM f, d WHERE f.k = d.k AND f.b > f.c AND f.b >= DATE '2024-01-01';"
        ),
        kept
    );
    let max_b = (0..6000)
        .filter(|&n| keep(n))
        .map(|n| 1 + n % 28)
        .max()
        .unwrap();
    let got = match db
        .execute("SELECT max(b) FROM f, d WHERE f.k = d.k AND f.b > f.c;")
        .unwrap()
        .pop()
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => rows[0][0].to_string(),
        other => panic!("{other:?}"),
    };
    assert_eq!(got, format!("2024-01-{max_b:02}"));
}
