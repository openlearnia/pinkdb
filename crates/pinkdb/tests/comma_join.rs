//! Wave 111: a comma join (`FROM a, b WHERE a.k = b.k`) must hash, not
//! materialize the cross product. The optimizer extracts crossing equalities
//! into hash keys and keeps every other predicate; these tests pin that the
//! answer is unchanged.

use pinkdb::{PinkDb, QueryResult, Value};

fn db() -> PinkDb {
    let mut db = PinkDb::new();
    for stmt in [
        "CREATE TABLE l (k INTEGER, q INTEGER);",
        "INSERT INTO l VALUES (1,10),(2,20),(3,NULL),(4,40);",
        "CREATE TABLE r (k INTEGER, v VARCHAR);",
        "INSERT INTO r VALUES (1,'a'),(2,'b'),(3,'c'),(5,'e'),(NULL,'n');",
    ] {
        db.execute(stmt).unwrap();
    }
    db
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn explain(db: &mut PinkDb, sql: &str) -> String {
    rows(db, &format!("EXPLAIN {sql};"))
        .iter()
        .map(|r| match &r[0] {
            Value::Varchar(s) => s.clone(),
            other => panic!("expected EXPLAIN text, got {other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn comma_join_returns_the_same_rows_as_explicit_join() {
    let mut db = db();
    for (comma, explicit) in [
        (
            "SELECT l.k, r.v FROM l, r WHERE l.k = r.k ORDER BY l.k",
            "SELECT l.k, r.v FROM l JOIN r ON l.k = r.k ORDER BY l.k",
        ),
        (
            "SELECT count(*), sum(l.q) FROM l, r WHERE l.k = r.k AND r.v <> 'b'",
            "SELECT count(*), sum(l.q) FROM l JOIN r ON l.k = r.k WHERE r.v <> 'b'",
        ),
        (
            "SELECT l.k, r.v FROM l, r, l l2 WHERE l.k = r.k AND l2.k = r.k ORDER BY l.k",
            "SELECT l.k, r.v FROM l JOIN r ON l.k = r.k JOIN l l2 ON l2.k = r.k ORDER BY l.k",
        ),
    ] {
        assert_eq!(rows(&mut db, comma), rows(&mut db, explicit), "{comma}");
    }
    // Pinned rows, so a shared drift cannot hide behind the comparison.
    let got = rows(
        &mut db,
        "SELECT l.k, r.v FROM l, r WHERE l.k = r.k ORDER BY l.k",
    );
    let want = vec![
        vec![Value::Integer(1), Value::Varchar("a".into())],
        vec![Value::Integer(2), Value::Varchar("b".into())],
        vec![Value::Integer(3), Value::Varchar("c".into())],
    ];
    assert_eq!(got, want);
}

#[test]
fn comma_join_plans_a_hash_join_and_puts_residuals_on_it() {
    let mut db = db();
    let plan = explain(&mut db, "SELECT l.k FROM l, r WHERE l.k = r.k");
    assert!(plan.contains("Join inner on [#0=#0]"), "{plan}");
    assert!(!plan.contains("NestedLoopJoin"), "{plan}");

    // A predicate that spans both sides is not a key, but it is a predicate on
    // a matched pair, so the join carries it (wave 111c) and only pairs that
    // pass it are emitted.
    let plan = explain(
        &mut db,
        "SELECT l.k FROM l, r WHERE l.k = r.k AND l.q > length(r.v)",
    );
    assert!(plan.contains("Join inner on [#0=#0] residual"), "{plan}");
    assert!(!plan.contains("Filter"), "{plan}");
    assert_eq!(
        rows(
            &mut db,
            "SELECT l.k, r.v FROM l, r WHERE l.k = r.k AND l.q > length(r.v) ORDER BY l.k",
        ),
        vec![
            vec![Value::Integer(1), Value::Varchar("a".into())],
            vec![Value::Integer(2), Value::Varchar("b".into())],
        ]
    );
}

/// An outer join's condition on its *preserved* side must not become a filter
/// on that side: it decides which rows match, not which rows survive. Pinned
/// against DuckDB 2.0 (`a LEFT JOIN b ON a.k = b.k AND a.x > 5` keeps every
/// row of `a`, NULL-extending the ones that fail the condition).
#[test]
fn an_outer_joins_preserved_side_condition_keeps_every_preserved_row() {
    let mut db = PinkDb::new();
    for stmt in [
        "CREATE TABLE a (k INTEGER, x INTEGER);",
        "CREATE TABLE b (k INTEGER, y INTEGER);",
        "INSERT INTO a VALUES (1,1),(2,10);",
        "INSERT INTO b VALUES (1,100),(2,200);",
    ] {
        db.execute(stmt).unwrap();
    }
    let left = rows(
        &mut db,
        "SELECT a.k, a.x, b.y FROM a LEFT JOIN b ON a.k = b.k AND a.x > 5 ORDER BY a.k;",
    );
    assert_eq!(
        left,
        vec![
            vec![Value::Integer(1), Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Integer(10), Value::Integer(200)],
        ]
    );
    let right = rows(
        &mut db,
        "SELECT a.k, b.y FROM b RIGHT JOIN a ON a.k = b.k AND a.x > 5 ORDER BY a.k;",
    );
    assert_eq!(
        right,
        vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Integer(200)],
        ]
    );
    // The null-supplying side's condition may still move into that side: the
    // join hashes and the answer is the same as DuckDB's.
    let pushed = rows(
        &mut db,
        "SELECT a.k, b.y FROM a LEFT JOIN b ON a.k = b.k AND b.y > 150 ORDER BY a.k;",
    );
    assert_eq!(
        pushed,
        vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Integer(200)],
        ]
    );
}

/// The FROM order may cross: here `s` has no equality to `r`, its neighbour.
/// Cost-based ordering picks the join order, and the answer must be the one
/// the explicit order gives.
#[test]
fn a_reordered_chain_returns_the_same_rows() {
    let mut db = db();
    for stmt in [
        "CREATE TABLE s (k INTEGER, w INTEGER);",
        "INSERT INTO s VALUES (1,100),(2,200),(4,400);",
    ] {
        db.execute(stmt).unwrap();
    }
    let comma = "SELECT count(*), sum(l.q), sum(s.w) FROM r, s, l WHERE l.k = r.k AND r.k = s.k";
    let explicit =
        "SELECT count(*), sum(l.q), sum(s.w) FROM l JOIN r ON l.k = r.k JOIN s ON r.k = s.k";
    assert_eq!(rows(&mut db, comma), rows(&mut db, explicit));
    assert_eq!(
        rows(&mut db, comma),
        vec![vec![
            Value::Bigint(2),
            Value::Bigint(30),
            Value::Bigint(300)
        ]]
    );
    // Every neighbouring pair has an equality, so nothing stays a cross join.
    assert!(!explain(&mut db, comma).contains("cross"));
}

/// A predicate that is one `OR` over join conditions derives per-column facts
/// (the `l.q >= 10` bound is the union of the branches). Deriving once and
/// leaving the `OR` above the join must not re-derive and push forever.
#[test]
fn an_or_over_join_conditions_derives_filters_without_looping() {
    let mut db = db();
    let got = rows(
        &mut db,
        "SELECT count(*) FROM l, r WHERE (l.k = r.k AND l.q >= 20) OR (l.k = r.k AND l.q >= 10)",
    );
    assert_eq!(got, vec![vec![Value::Bigint(2)]]);
    let plan = explain(
        &mut db,
        "SELECT l.k FROM l, r WHERE (l.k = r.k AND l.q >= 20) OR (l.k = r.k AND l.q >= 10)",
    );
    assert!(plan.contains("Join inner on [#0=#0]"), "{plan}");
}

#[test]
fn a_chain_under_nested_decorrelated_joins_starts_from_its_filtered_table() {
    // TPC-H Q21's shape: `EXISTS` and `NOT EXISTS` decorrelate into an anti
    // join over a semi join over the comma chain. The chain must still be
    // ordered so the one-row `nat` filter joins `sup` first, not last.
    let mut db = PinkDb::new();
    let nat: Vec<String> = (0..25)
        .map(|i| {
            let name = if i == 7 {
                "TARGET".to_string()
            } else {
                format!("N{i}")
            };
            format!("({i}, '{name}')")
        })
        .collect();
    let sup: Vec<String> = (0..2000).map(|i| format!("({i}, {})", i % 25)).collect();
    let item: Vec<String> = (0..40000)
        .map(|i| format!("({i}, {}, {}, {})", i / 4, (i * 7) % 2000, i % 4 == 0))
        .collect();
    for sql in [
        "CREATE TABLE nat (n INTEGER, name VARCHAR);".to_string(),
        format!("INSERT INTO nat VALUES {};", nat.join(",")),
        "CREATE TABLE sup (s INTEGER, n INTEGER);".to_string(),
        format!("INSERT INTO sup VALUES {};", sup.join(",")),
        "CREATE TABLE item (id INTEGER, o INTEGER, s INTEGER, late BOOLEAN);".to_string(),
        format!("INSERT INTO item VALUES {};", item.join(",")),
    ] {
        db.execute(&sql).unwrap();
    }
    let sql = "SELECT count(*), count(DISTINCT s.s), sum(a.id) FROM sup s, item a, nat n \
               WHERE s.s = a.s AND s.n = n.n AND n.name = 'TARGET' AND a.late \
               AND EXISTS (SELECT 1 FROM item b WHERE b.o = a.o AND b.s <> a.s) \
               AND NOT EXISTS (SELECT 1 FROM item c WHERE c.o = a.o AND c.s <> a.s AND c.late)";
    let plan = explain(&mut db, sql);
    let nat_at = plan.find("Scan main.nat").expect("nat scan");
    let item_at = plan.find("Scan main.item").expect("item scan");
    assert!(
        nat_at < item_at,
        "the filtered table should join first:\n{plan}"
    );
    // DuckDB 2.0 on the same data.
    assert_eq!(
        rows(&mut db, sql),
        vec![vec![
            Value::Bigint(400),
            Value::Bigint(20),
            Value::Bigint(8010400)
        ]]
    );
}
