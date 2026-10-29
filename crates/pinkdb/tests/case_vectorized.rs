//! Wave 127g: `CASE` runs a vector at a time — conditions as selections over
//! the rows no earlier branch took, literal results without evaluation, one
//! typed gather for the output. Every answer is checked against the rows
//! computed here: NULL conditions fall through, a missing `ELSE` and a NULL
//! literal give NULL, an integer literal joins a DECIMAL result, and a
//! branch's error is only raised for rows that reach it.

use pinkdb::{PinkDb, QueryResult, Value};

fn rows(db: &mut PinkDb, sql: &str) -> Vec<String> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|r| r.iter().map(Value::to_string).collect::<Vec<_>>().join("|"))
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

const N: i64 = 5000;

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, c INTEGER, s VARCHAR, x DECIMAL(9,2));")
        .unwrap();
    let values: Vec<String> = (0..N)
        .map(|i| {
            let c = if i % 13 == 0 {
                "NULL".into()
            } else {
                (i % 6).to_string()
            };
            let s = if i % 17 == 0 {
                "NULL".into()
            } else {
                format!("'{}'", ["1-URGENT", "2-HIGH", "3-MEDIUM"][i as usize % 3])
            };
            format!("({i}, {c}, {s}, {}.{:02})", i % 50, i % 100)
        })
        .collect();
    db.execute(&format!("INSERT INTO t VALUES {};", values.join(",")))
        .unwrap();
    db
}

fn c_of(i: i64) -> Option<i64> {
    (i % 13 != 0).then_some(i % 6)
}

fn s_of(i: i64) -> Option<&'static str> {
    (i % 17 != 0).then(|| ["1-URGENT", "2-HIGH", "3-MEDIUM"][i as usize % 3])
}

#[test]
fn branches_nulls_and_missing_else() {
    let mut db = store();
    let got = rows(
        &mut db,
        "SELECT CASE WHEN s = '1-URGENT' OR s = '2-HIGH' THEN 1 WHEN c > 3 THEN 2 \
         WHEN c IS NULL THEN NULL END FROM t ORDER BY id;",
    );
    let want: Vec<String> = (0..N)
        .map(|i| {
            let hi = matches!(s_of(i), Some("1-URGENT" | "2-HIGH"));
            if hi {
                "1".into()
            } else if c_of(i).is_some_and(|c| c > 3) {
                "2".into()
            } else {
                "NULL".into()
            }
        })
        .collect();
    assert_eq!(got, want);
}

#[test]
fn computed_results_and_an_integer_else_in_a_decimal_case() {
    let mut db = store();
    let got = rows(
        &mut db,
        "SELECT CASE WHEN c = 1 THEN x * 2 WHEN c = 2 THEN x ELSE 0 END FROM t ORDER BY id;",
    );
    let want: Vec<String> = (0..N)
        .map(|i| {
            let cents = (i % 50) * 100 + i % 100;
            let v = match c_of(i) {
                Some(1) => cents * 2,
                Some(2) => cents,
                _ => 0,
            };
            format!("{}.{:02}", v / 100, v % 100)
        })
        .collect();
    assert_eq!(got, want);
    // Aggregated, as TPC-H Q12 uses it.
    let sums = rows(
        &mut db,
        "SELECT sum(CASE WHEN s = '1-URGENT' OR s = '2-HIGH' THEN 1 ELSE 0 END), \
         sum(CASE WHEN s <> '1-URGENT' AND s <> '2-HIGH' THEN 1 ELSE 0 END) FROM t;",
    );
    let hi = (0..N)
        .filter(|&i| matches!(s_of(i), Some("1-URGENT" | "2-HIGH")))
        .count();
    let lo = (0..N).filter(|&i| s_of(i) == Some("3-MEDIUM")).count();
    assert_eq!(sums, vec![format!("{hi}|{lo}")]);
}

#[test]
fn a_branch_error_needs_a_row_that_reaches_it() {
    let mut db = store();
    let got = rows(
        &mut db,
        "SELECT sum(CASE WHEN c = 0 OR c IS NULL THEN 0 ELSE 60 / c END) FROM t;",
    );
    let want: i64 = (0..N)
        .filter_map(|i| c_of(i).filter(|&c| c != 0).map(|c| 60 / c))
        .sum();
    assert_eq!(got, vec![want.to_string()]);
    assert!(
        db.execute("SELECT sum(CASE WHEN c IS NULL THEN 0 ELSE 60 / c END) FROM t;")
            .is_err()
    );
}
