//! Wave 142: DECIMAL(p <= 18) is held as a 64-bit payload in memory. These
//! pin the edges where the 64-bit and 128-bit forms meet: the 18-digit bound,
//! products and sums that leave it, casts across it, joins and grouping
//! between the two widths, and storage round trips. Results and error texts
//! must be what the all-`i128` engine produced.

use pinkdb::{PinkDb, QueryResult, Value};

fn text(db: &mut PinkDb, sql: &str) -> Vec<Vec<String>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|r| {
                r.into_iter()
                    .map(|v| match v {
                        Value::Null => "NULL".to_string(),
                        v => v.to_string(),
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    match db.execute(sql) {
        Ok(r) => panic!("expected an error for {sql}, got {r:?}"),
        Err(e) => e.to_string(),
    }
}

fn db() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE d (a DECIMAL(18,0), b DECIMAL(9,2), c DECIMAL(17,0), w DECIMAL(20,2));
         INSERT INTO d VALUES
           (999999999999999999, 9999999.99, 99999999999999999, 123456789012345678.91),
           (-999999999999999999, -9999999.99, -99999999999999999, -0.01),
           (NULL, NULL, NULL, NULL),
           (1, 0.01, 1, 1.00);",
    )
    .unwrap();
    db
}

#[test]
fn the_18_digit_bound_round_trips_and_compares() {
    let mut db = db();
    assert_eq!(
        text(&mut db, "SELECT a FROM d ORDER BY a;"),
        vec![
            vec!["-999999999999999999"],
            vec!["1"],
            vec!["999999999999999999"],
            vec!["NULL"],
        ]
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT count(*) FROM d WHERE a > 999999999999999998 OR a < -999999999999999998;"
        ),
        vec![vec!["2"]]
    );
    let bytes = db.to_pink_bytes().unwrap();
    let mut back = PinkDb::from_pink_bytes(&bytes).unwrap();
    assert_eq!(
        text(&mut back, "SELECT * FROM d ORDER BY a;"),
        text(&mut db, "SELECT * FROM d ORDER BY a;")
    );
}

#[test]
fn a_product_inside_18_digits_is_exact_and_one_past_is_the_same_error() {
    let mut db = db();
    // DECIMAL(9,2) * DECIMAL(9,2) is DECIMAL(18,4).
    assert_eq!(
        text(&mut db, "SELECT b * b FROM d WHERE b > 0 ORDER BY b;"),
        vec![vec!["0.0001"], vec!["99999999800000.0001"]]
    );
    // DECIMAL(18,0) * DECIMAL(9,2) is capped at DECIMAL(18,2): past it is
    // DuckDB's DECIMAL(18) overflow text.
    let e = err(&mut db, "SELECT a * b FROM d WHERE a > 1;");
    assert!(
        e.contains("Overflow in multiplication of DECIMAL(18)"),
        "{e}"
    );
}

#[test]
fn sums_leave_64_bits_exactly() {
    let mut db = db();
    // Two maximal DECIMAL(18,0) values sum past i64 into DECIMAL(38,0).
    db.execute("INSERT INTO d VALUES (999999999999999999, 0, 0, 0);")
        .unwrap();
    assert_eq!(
        text(&mut db, "SELECT sum(a) FROM d WHERE a > 0;"),
        vec![vec!["1999999999999999999"]]
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT sum(a), count(a) FROM d GROUP BY a > 0 ORDER BY 1;"
        ),
        vec![
            vec!["-999999999999999999", "1"],
            vec!["1999999999999999999", "3"],
            vec!["NULL", "0"],
        ]
    );
}

#[test]
fn addition_at_the_edge_stays_or_widens_by_type() {
    let mut db = db();
    // DECIMAL(17,0) + DECIMAL(17,0) is DECIMAL(18,0): stays 64-bit.
    assert_eq!(
        text(&mut db, "SELECT c + c FROM d WHERE c > 1;"),
        vec![vec!["199999999999999998"]]
    );
    // DECIMAL(18,0) + DECIMAL(18,0) is DECIMAL(19,0): the 128-bit form.
    assert_eq!(
        text(&mut db, "SELECT a + a FROM d WHERE a > 1;"),
        vec![vec!["1999999999999999998"]]
    );
    assert_eq!(
        text(&mut db, "SELECT a - 1 FROM d WHERE a > 1;"),
        vec![vec!["999999999999999998"]]
    );
}

#[test]
fn casts_across_the_bound() {
    let mut db = db();
    assert_eq!(
        text(
            &mut db,
            "SELECT CAST(a AS DECIMAL(19,0)) FROM d WHERE a > 1;"
        ),
        vec![vec!["999999999999999999"]]
    );
    assert_eq!(
        text(
            &mut db,
            "SELECT CAST(w AS DECIMAL(18,2)) FROM d WHERE w < 0;"
        ),
        vec![vec!["-0.01"]]
    );
    let e = err(
        &mut db,
        "SELECT CAST(w AS DECIMAL(18,2)) FROM d WHERE w > 1000;",
    );
    assert!(e.contains("DECIMAL"), "{e}");
}

#[test]
fn joins_and_groups_across_widths() {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE n (k DECIMAL(15,2), v INTEGER);
         CREATE TABLE m (k DECIMAL(20,2), v INTEGER);
         INSERT INTO n VALUES (1.50, 1), (2.25, 2), (NULL, 3), (1.50, 4);
         INSERT INTO m VALUES (1.50, 10), (2.25, 20), (9.99, 30);",
    )
    .unwrap();
    assert_eq!(
        text(
            &mut db,
            "SELECT n.v, m.v FROM n JOIN m ON n.k = m.k ORDER BY n.v;"
        ),
        vec![vec!["1", "10"], vec!["2", "20"], vec!["4", "10"]]
    );
    assert_eq!(
        text(&mut db, "SELECT k, count(*) FROM n GROUP BY k ORDER BY k;"),
        vec![vec!["1.50", "2"], vec!["2.25", "1"], vec!["NULL", "1"]]
    );
    assert_eq!(
        text(&mut db, "SELECT k FROM n UNION SELECT k FROM m ORDER BY k;"),
        vec![vec!["1.50"], vec!["2.25"], vec!["9.99"], vec!["NULL"]]
    );
}
