//! Wave 125d: a join the statement repeats is evaluated once and reused
//! (TPC-H Q17/Q2: the decorrelated subquery repeats the outer join to collect
//! its keys). A result pruned by an enclosing join's filter is never reused,
//! and answers match the per-occurrence evaluation.

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

fn store() -> PinkDb {
    store_at(PinkDb::new())
}

fn store_at(mut db: PinkDb) -> PinkDb {
    db.execute("CREATE TABLE item (pk INTEGER, qty INTEGER, price INTEGER);")
        .unwrap();
    db.execute("CREATE TABLE part (pk INTEGER, brand VARCHAR);")
        .unwrap();
    db.execute("CREATE TABLE tiny (pk INTEGER);").unwrap();
    let items: Vec<String> = (0..20000i64)
        .map(|i| format!("({}, {}, {})", i / 50, i % 50, i % 1000))
        .collect();
    let parts: Vec<String> = (0..400i64)
        .map(|p| format!("({p}, 'B{}')", p % 9))
        .collect();
    db.execute(&format!("INSERT INTO item VALUES {};", items.join(",")))
        .unwrap();
    db.execute(&format!("INSERT INTO part VALUES {};", parts.join(",")))
        .unwrap();
    db.execute("INSERT INTO tiny VALUES (3), (5);").unwrap();
    db
}

#[test]
fn a_correlated_average_over_the_same_join_answers_like_q17() {
    let mut db = store();
    let got = rows(
        &mut db,
        "SELECT sum(price) FROM item, part WHERE part.pk = item.pk AND brand = 'B4' \
         AND qty < (SELECT 0.5 * avg(qty) FROM item i2 WHERE i2.pk = part.pk);",
    );
    // By hand: per part of brand B4, the items below half its average qty.
    let mut total = 0i64;
    for p in (0..400i64).filter(|p| p % 9 == 4) {
        let qs: Vec<(i64, i64)> = (0..20000i64)
            .filter(|i| i / 50 == p)
            .map(|i| (i % 50, i % 1000))
            .collect();
        let avg = qs.iter().map(|q| q.0 as f64).sum::<f64>() / qs.len() as f64;
        total += qs
            .iter()
            .filter(|q| (q.0 as f64) < 0.5 * avg)
            .map(|q| q.1)
            .sum::<i64>();
    }
    assert_eq!(got, vec![total.to_string()]);
}

#[test]
fn a_join_pruned_by_an_enclosing_join_is_not_reused() {
    // A checkpointed store has the per-chunk statistics join filters prune
    // scans with.
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_shared_subplans_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("db.pink");
    let path = path.to_str().unwrap();
    {
        let mut db = store_at(PinkDb::open_durable(path).unwrap());
        db.checkpoint().unwrap();
    }
    let mut db = PinkDb::open_durable(path).unwrap();
    // The first branch's `item ⋈ part` is probed under a join with `tiny`,
    // whose filter prunes its scans to the chunks holding two keys (`item`
    // is stored in key order); the second branch needs every row of the
    // same join.
    let got = rows(
        &mut db,
        "SELECT count(*), sum(pk) FROM ( \
         SELECT x.pk AS pk FROM (SELECT item.pk AS pk FROM item JOIN part ON item.pk = part.pk) x \
         JOIN tiny ON tiny.pk = x.pk \
         UNION ALL SELECT item.pk AS pk FROM item JOIN part ON item.pk = part.pk) u;",
    );
    // 50 rows each for keys 3 and 5, plus all 20,000 joined rows.
    let all: i64 = (0..20000i64).map(|i| i / 50).sum();
    assert_eq!(got, vec![format!("20100|{}", all + 50 * 3 + 50 * 5)]);
    std::fs::remove_dir_all(&dir).ok();
}
