//! Wave 111e: `a.k = b.k AND c.k = b.k` also joins `a` with `c`, so the
//! cost-based order may join those two first (TPC-H Q9 joins `part` with
//! `partsupp` before touching `lineitem`).
//!
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

/// A three-table chain whose only written edges go through the large table
/// (`a.k = big.k AND c.k = big.k`): the order may join `a` and `c` first on
/// the inferred `a.k = c.k`, and the answer is the written query's.
#[test]
fn an_equality_class_joins_its_members_directly() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE a (k BIGINT, f VARCHAR);").unwrap();
    db.execute("CREATE TABLE c (k BIGINT, g INTEGER);").unwrap();
    db.execute("CREATE TABLE big (k BIGINT, v INTEGER);")
        .unwrap();
    let a: Vec<String> = (0..300)
        .map(|i| {
            let k = if i % 11 == 0 {
                "NULL".into()
            } else {
                (i % 150).to_string()
            };
            format!("({k}, '{}')", if i % 5 == 0 { "hot" } else { "cold" })
        })
        .collect();
    let c: Vec<String> = (0..2000)
        .map(|i| {
            let k = if i % 13 == 0 {
                "NULL".into()
            } else {
                (i % 400).to_string()
            };
            format!("({k}, {})", i % 9)
        })
        .collect();
    let big: Vec<String> = (0..20000)
        .map(|i| {
            let k = if i % 17 == 0 {
                "NULL".into()
            } else {
                (i % 1000).to_string()
            };
            format!("({k}, {})", i % 100)
        })
        .collect();
    for (t, v) in [("a", a), ("c", c), ("big", big)] {
        db.execute(&format!("INSERT INTO {t} VALUES {};", v.join(",")))
            .unwrap();
    }
    let sql = "SELECT count(*), sum(v), sum(g) FROM a, big, c \
               WHERE a.k = big.k AND c.k = big.k AND a.f = 'hot';";
    // Reference: the same query written with explicit joins in one fixed
    // order, which the reorder leaves alone (two joins).
    let reference = "SELECT count(*), sum(v), sum(g) FROM a JOIN big ON a.k = big.k \
                     JOIN c ON c.k = big.k WHERE a.f = 'hot';";
    // `a` and `c` join first, on the inferred key, and `big` comes last.
    let plan = rows(&mut db, &format!("EXPLAIN {sql}")).join("\n");
    let (at_c, at_big) = (plan.find("Scan main.c"), plan.find("Scan main.big"));
    assert!(at_c.is_some() && at_c < at_big, "{plan}");
    let got = rows(&mut db, sql);
    assert_eq!(got, rows(&mut db, reference));
    // Count by hand: per key, hot `a` rows × `big` rows × `c` rows.
    let count = |n: i64, modulus: i64, nulls: i64, k: i64| {
        (0..n)
            .filter(|i| i % nulls != 0 && i % modulus == k)
            .count() as i64
    };
    let mut total = 0;
    for k in 0..150 {
        let hot = (0..300)
            .filter(|i| i % 11 != 0 && i % 150 == k && i % 5 == 0)
            .count() as i64;
        total += hot * count(20000, 1000, 17, k) * count(2000, 400, 13, k);
    }
    assert_eq!(got[0].split('|').next().unwrap(), total.to_string());
}
