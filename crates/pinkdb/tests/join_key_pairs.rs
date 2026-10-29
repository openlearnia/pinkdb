//! Wave 125c: a join on two integer columns packs both into one BIGINT when
//! the build side's values fit 32 bits and takes the integer path; a probe
//! value outside 32 bits matches nothing, and a build side with such a value
//! keeps the encoded path. Answers must not change either way.

use std::collections::HashMap;

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

/// `(x, y)` keys: NULLs, duplicates, and (when `wide`) values past 32 bits.
fn key(i: i64, wide: bool) -> (Option<i64>, Option<i64>) {
    let x = (i % 13 != 4).then_some(i % 40);
    let y = (i % 17 != 6).then_some(if wide && i % 7 == 0 {
        (1i64 << 33) + i % 3
    } else {
        i % 3
    });
    (x, y)
}

fn lit(v: Option<i64>) -> String {
    v.map_or_else(|| "NULL".into(), |x| x.to_string())
}

fn store(wide_probe: bool, wide_build: bool) -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE p (id INTEGER, x BIGINT, y BIGINT);")
        .unwrap();
    db.execute("CREATE TABLE b (x BIGINT, y BIGINT, v INTEGER);")
        .unwrap();
    let p: Vec<String> = (0..3000)
        .map(|i| {
            let (x, y) = key(i, wide_probe);
            format!("({i}, {}, {})", lit(x), lit(y))
        })
        .collect();
    let b: Vec<String> = (0..500)
        .map(|i| {
            let (x, y) = key(i * 7 + 1, wide_build);
            format!("({}, {}, {i})", lit(x), lit(y))
        })
        .collect();
    db.execute(&format!("INSERT INTO p VALUES {};", p.join(",")))
        .unwrap();
    db.execute(&format!("INSERT INTO b VALUES {};", b.join(",")))
        .unwrap();
    db
}

/// Per probe id: the matching build rows' count and `v` sum.
fn matches(wide_probe: bool, wide_build: bool) -> Vec<(i64, i64, i64)> {
    let mut by_key: HashMap<(i64, i64), (i64, i64)> = HashMap::new();
    for i in 0..500 {
        if let (Some(x), Some(y)) = key(i * 7 + 1, wide_build) {
            let e = by_key.entry((x, y)).or_default();
            e.0 += 1;
            e.1 += i;
        }
    }
    (0..3000)
        .map(|i| match key(i, wide_probe) {
            (Some(x), Some(y)) => {
                let (n, s) = by_key.get(&(x, y)).copied().unwrap_or_default();
                (i, n, s)
            }
            _ => (i, 0, 0),
        })
        .collect()
}

#[test]
fn two_integer_keys_join_the_same_with_or_without_packing() {
    for (wide_probe, wide_build) in [(false, false), (true, false), (false, true), (true, true)] {
        let mut db = store(wide_probe, wide_build);
        let want = matches(wide_probe, wide_build);
        let case = format!("wide probe {wide_probe}, wide build {wide_build}");
        // Inner join (either side may build), as one total per probe row.
        let inner: Vec<String> = want
            .iter()
            .filter(|w| w.1 > 0)
            .map(|w| format!("{}|{}|{}", w.0, w.1, w.2))
            .collect();
        assert_eq!(
            rows(
                &mut db,
                "SELECT p.id, count(*), sum(b.v) FROM p JOIN b ON p.x = b.x AND p.y = b.y \
                 GROUP BY p.id ORDER BY p.id;"
            ),
            inner,
            "{case}"
        );
        assert_eq!(
            rows(
                &mut db,
                "SELECT p.id, count(*), sum(b.v) FROM b JOIN p ON p.x = b.x AND p.y = b.y \
                 GROUP BY p.id ORDER BY p.id;"
            ),
            inner,
            "{case}"
        );
        // LEFT join keeps every probe row.
        let left: Vec<String> = want.iter().map(|w| format!("{}|{}", w.0, w.1)).collect();
        assert_eq!(
            rows(
                &mut db,
                "SELECT p.id, count(b.v) FROM p LEFT JOIN b ON p.x = b.x AND p.y = b.y \
                 GROUP BY p.id ORDER BY p.id;"
            ),
            left,
            "{case}"
        );
        // EXISTS / NOT EXISTS.
        let semi = |keep: bool| -> Vec<String> {
            want.iter()
                .filter(|w| (w.1 > 0) == keep)
                .map(|w| w.0.to_string())
                .collect()
        };
        assert_eq!(
            rows(
                &mut db,
                "SELECT id FROM p WHERE EXISTS (SELECT 1 FROM b WHERE b.x = p.x AND b.y = p.y) ORDER BY id;"
            ),
            semi(true),
            "{case}"
        );
        assert_eq!(
            rows(
                &mut db,
                "SELECT id FROM p WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.x = p.x AND b.y = p.y) ORDER BY id;"
            ),
            semi(false),
            "{case}"
        );
    }
}
