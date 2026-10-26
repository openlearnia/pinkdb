//! Wave 111: uncorrelated subqueries are evaluated inside a parallel scan, so
//! each chunk may build the set. These queries span several chunks; the
//! answers must still match the equivalent set arithmetic.

use pinkdb::{PinkDb, QueryResult, Value};

fn db() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (k INTEGER);").unwrap();
    let values: Vec<String> = (0..5000).map(|i| format!("({i})")).collect();
    db.execute(&format!("INSERT INTO t VALUES {};", values.join(",")))
        .unwrap();
    db.execute("CREATE TABLE n (k INTEGER);").unwrap();
    db.execute("INSERT INTO n VALUES (1),(NULL);").unwrap();
    db
}

fn count(db: &mut PinkDb, sql: &str) -> i64 {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => match &rows[0][0] {
            Value::Bigint(v) => *v,
            Value::Integer(v) => *v as i64,
            other => panic!("expected an integer count from {sql}, got {other:?}"),
        },
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

#[test]
fn multi_chunk_in_subquery_matches_a_join() {
    let mut db = db();
    // 0, 7, …, 4998 => 715 matches out of 5000 rows.
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM t WHERE k IN (SELECT k FROM t WHERE k % 7 = 0)"
        ),
        715
    );
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM t WHERE k NOT IN (SELECT k FROM t WHERE k % 7 = 0)"
        ),
        5000 - 715
    );
}

#[test]
fn multi_chunk_scalar_and_exists_subqueries() {
    let mut db = db();
    assert_eq!(count(&mut db, "SELECT (SELECT count(*) FROM t) + 1"), 5001);
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM t WHERE EXISTS (SELECT 1 FROM t x WHERE x.k < 10)"
        ),
        5000
    );
}

#[test]
fn nulls_keep_three_valued_in_semantics() {
    let mut db = db();
    // A NULL in the set makes a non-match UNKNOWN, so `NOT IN` returns nothing.
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM t WHERE k NOT IN (SELECT k FROM n)"
        ),
        0
    );
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM t WHERE k IN (SELECT k FROM n)"
        ),
        1
    );
}
