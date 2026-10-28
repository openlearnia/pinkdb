//! A filter's conjuncts run cheapest first (DuckDB's `ExpressionHeuristics`
//! costs), an `IN` list appears once even when the optimizer derives it again,
//! and a conjunct that can raise an error keeps its written place.

use pinkdb::{PinkDb, QueryResult, Value};

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn plan(db: &mut PinkDb, sql: &str) -> String {
    rows(db, &format!("EXPLAIN {sql}"))
        .iter()
        .map(|r| {
            r.iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("\t")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn filter_line(plan: &str) -> String {
    plan.lines()
        .find(|l| l.contains("Filter"))
        .unwrap_or_else(|| panic!("no Filter in {plan}"))
        .trim()
        .to_string()
}

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    for sql in [
        "CREATE TABLE t (mode VARCHAR, d DATE, e DATE, v VARCHAR);",
        "INSERT INTO t VALUES ('MAIL', DATE '1994-03-01', DATE '1994-02-01', '5'), \
         ('SHIP', DATE '1993-03-01', DATE '1993-01-01', 'x'), \
         ('AIR', DATE '1994-06-01', DATE '1994-05-01', '7'), \
         ('SHIP', DATE '1994-07-01', DATE '1994-08-01', '9');",
    ] {
        db.execute(sql).unwrap();
    }
    db
}

#[test]
fn cheap_conjuncts_run_first_and_an_in_list_runs_once() {
    let mut db = store();
    let sql = "SELECT count(*) FROM t WHERE mode IN ('MAIL', 'SHIP') AND e < d \
               AND d >= DATE '1994-01-01' AND d < DATE '1995-01-01'";
    let filter = filter_line(&plan(&mut db, sql));
    // The date bounds (column vs literal) come first, the column-vs-column
    // compare next, the string `IN` list last — and only once.
    let bound = filter.find("1994-01-01").expect("lower bound");
    let cmp = filter.find("#2 < #1").expect("e < d");
    let mode = filter.find("'MAIL'").expect("IN list");
    assert!(bound < cmp && cmp < mode, "{filter}");
    assert_eq!(filter.matches("'MAIL'").count(), 1, "{filter}");
    assert!(!filter.contains("false"), "{filter}");
    assert_eq!(rows(&mut db, sql), vec![vec![Value::Bigint(1)]]);
}

#[test]
fn a_fallible_conjunct_keeps_the_written_order() {
    let mut db = store();
    // `v <> 'x'` guards the cast: reordering could cast 'x' first.
    let sql = "SELECT count(*) FROM t WHERE v <> 'x' AND CAST(v AS INTEGER) > 6 \
               AND d >= DATE '1994-01-01'";
    let filter = filter_line(&plan(&mut db, sql));
    let guard = filter.find("'x'").expect("guard");
    let date = filter.find("1994-01-01").expect("date");
    assert!(guard < date, "{filter}");
    assert_eq!(rows(&mut db, sql), vec![vec![Value::Bigint(2)]]);
}
