//! Wave 125a: a SEMI/ANTI join whose left side is the smaller one hashes the
//! left side and lets the right rows mark it (DuckDB's RIGHT_SEMI /
//! RIGHT_ANTI). The answer must be the left-probing one: each left row once,
//! in left order, kept iff some right row has equal non-NULL keys (ANTI the
//! opposite, so a NULL-keyed left row is kept by NOT EXISTS) — with NULLs and
//! duplicates on the right, one and two keys, and integer and text keys.

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

/// `small` (60 rows) and `big` (9,000 rows): `(k, j, s)` with NULL keys on
/// both sides, `big` holding each of its keys many times.
fn small_row(i: i64) -> (Option<i64>, i64, Option<String>) {
    let k = (i % 13 != 5).then_some(i * 37 % 200);
    let s = (i % 11 != 3).then(|| format!("s{}", i * 7 % 150));
    (k, i % 4, s)
}

fn big_row(i: i64) -> (Option<i64>, i64, Option<String>) {
    let k = (i % 17 != 2).then_some(i % 120);
    let s = (i % 19 != 4).then(|| format!("s{}", i % 90));
    (k, i % 3, s)
}

fn literal(v: &Option<impl ToString>, quote: bool) -> String {
    match v {
        None => "NULL".into(),
        Some(x) if quote => format!("'{}'", x.to_string()),
        Some(x) => x.to_string(),
    }
}

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE small (id INTEGER, k BIGINT, j INTEGER, s VARCHAR);")
        .unwrap();
    db.execute("CREATE TABLE big (k BIGINT, j INTEGER, s VARCHAR);")
        .unwrap();
    let small: Vec<String> = (0..60)
        .map(|i| {
            let (k, j, s) = small_row(i);
            format!("({i}, {}, {j}, {})", literal(&k, false), literal(&s, true))
        })
        .collect();
    let big: Vec<String> = (0..9000)
        .map(|i| {
            let (k, j, s) = big_row(i);
            format!("({}, {j}, {})", literal(&k, false), literal(&s, true))
        })
        .collect();
    db.execute(&format!("INSERT INTO small VALUES {};", small.join(",")))
        .unwrap();
    db.execute(&format!("INSERT INTO big VALUES {};", big.join(",")))
        .unwrap();
    db
}

/// The ids of `small` that `matches` (over all `big` rows) keeps, in order.
fn expected(
    semi: bool,
    matches: impl Fn(&(Option<i64>, i64, Option<String>), &(Option<i64>, i64, Option<String>)) -> bool,
) -> Vec<String> {
    let big: Vec<_> = (0..9000).map(big_row).collect();
    (0..60)
        .filter(|&i| {
            let s = small_row(i);
            big.iter().any(|b| matches(&s, b)) == semi
        })
        .map(|i| i.to_string())
        .collect()
}

#[test]
fn exists_and_not_exists_on_an_integer_key() {
    let mut db = store();
    let same_k = |s: &(Option<i64>, i64, Option<String>),
                  b: &(Option<i64>, i64, Option<String>)| {
        s.0.is_some() && s.0 == b.0
    };
    assert_eq!(
        rows(
            &mut db,
            "SELECT id FROM small WHERE EXISTS (SELECT 1 FROM big WHERE big.k = small.k);"
        ),
        expected(true, same_k)
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT id FROM small WHERE NOT EXISTS (SELECT 1 FROM big WHERE big.k = small.k);"
        ),
        expected(false, same_k)
    );
}

#[test]
fn two_keys_and_text_keys() {
    let mut db = store();
    let two = |s: &(Option<i64>, i64, Option<String>), b: &(Option<i64>, i64, Option<String>)| {
        s.0.is_some() && s.0 == b.0 && s.1 == b.1
    };
    assert_eq!(
        rows(
            &mut db,
            "SELECT id FROM small WHERE EXISTS \
             (SELECT 1 FROM big WHERE big.k = small.k AND big.j = small.j);"
        ),
        expected(true, two)
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT id FROM small WHERE NOT EXISTS \
             (SELECT 1 FROM big WHERE big.k = small.k AND big.j = small.j);"
        ),
        expected(false, two)
    );
    let text = |s: &(Option<i64>, i64, Option<String>), b: &(Option<i64>, i64, Option<String>)| {
        s.2.is_some() && s.2 == b.2
    };
    assert_eq!(
        rows(
            &mut db,
            "SELECT id FROM small WHERE EXISTS (SELECT 1 FROM big WHERE big.s = small.s);"
        ),
        expected(true, text)
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT id FROM small WHERE NOT EXISTS (SELECT 1 FROM big WHERE big.s = small.s);"
        ),
        expected(false, text)
    );
}
