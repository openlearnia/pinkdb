//! Wave 123d: GROUP BY over fixed-width keys runs on flat tables (keys as
//! words, accumulators in one array, partials laid out by merge partition).
//! Groups must still come out in first-seen order — the order the general
//! path produces and that a query without ORDER BY shows — across enough
//! rows that the merge partitions, with NULL keys, every fixed-width key
//! type, a filter's selection, `FILTER (WHERE …)` and an error only a
//! filtered-out row would raise.

use std::collections::HashMap;

use pinkdb::{PinkDb, QueryResult, Value};

const ROWS: i64 = 60_000;

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<String>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|r| r.iter().map(Value::to_string).collect())
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// Row `i`'s cells, `None` for NULL: `(b, d, ts, x, k, v)` as the SQL
/// literals the table is loaded from.
fn row(i: i64) -> [Option<String>; 6] {
    let on = |every: i64, s: String| (i % every != 0).then_some(s);
    [
        on(17, (i % 3 == 0).to_string()),
        on(23, format!("DATE '2024-01-0{}'", 1 + i % 5)),
        on(29, format!("TIMESTAMP '2024-01-01 0{}:00:00'", i % 4)),
        on(31, format!("{}.50", i % 6 - 3)),
        on(37, (i * 7919 % 20011).to_string()),
        Some((i * 7 % 1000).to_string()),
    ]
}

fn store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE t (b BOOLEAN, d DATE, ts TIMESTAMP, x DECIMAL(10,2), k BIGINT, v INTEGER);",
    )
    .unwrap();
    let values: Vec<String> = (0..ROWS)
        .map(|i| {
            let cells: Vec<String> = row(i)
                .into_iter()
                .map(|c| c.unwrap_or_else(|| "NULL".into()))
                .collect();
            format!("({})", cells.join(","))
        })
        .collect();
    db.execute(&format!("INSERT INTO t VALUES {};", values.join(",")))
        .unwrap();
    db
}

/// The expected `key, count(*), sum(v), min(v), max(v)` rows in first-seen
/// order over the rows `keep` accepts, the key rendered by `key`.
fn expected(keep: impl Fn(i64) -> bool, key: impl Fn(i64) -> String) -> Vec<String> {
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, (i64, i64, i64, i64)> = HashMap::new();
    for i in (0..ROWS).filter(|&i| keep(i)) {
        let v = i * 7 % 1000;
        let k = key(i);
        let g = groups.entry(k.clone()).or_insert_with(|| {
            order.push(k);
            (0, 0, i64::MAX, i64::MIN)
        });
        *g = (g.0 + 1, g.1 + v, g.2.min(v), g.3.max(v));
    }
    order
        .into_iter()
        .map(|k| {
            let (n, s, lo, hi) = groups[&k];
            format!("{k}|{n}|{s}|{lo}|{hi}")
        })
        .collect()
}

fn joined(out: Vec<Vec<String>>) -> Vec<String> {
    out.into_iter().map(|r| r.join("|")).collect()
}

/// The display form of row `i`'s key column `c` (`NULL` for NULL).
fn shown(i: i64, c: usize) -> String {
    match &row(i)[c] {
        None => "NULL".into(),
        Some(s) => s
            .trim_start_matches("DATE ")
            .trim_start_matches("TIMESTAMP ")
            .trim_matches('\'')
            .to_string(),
    }
}

#[test]
fn many_integer_groups_come_out_in_first_seen_order() {
    let mut db = store();
    let out = rows(
        &mut db,
        "SELECT k, count(*), sum(v), min(v), max(v) FROM t GROUP BY k;",
    );
    assert_eq!(joined(out), expected(|_| true, |i| shown(i, 4)));
}

#[test]
fn every_fixed_width_key_type_with_nulls_round_trips() {
    let mut db = store();
    let out = rows(
        &mut db,
        "SELECT b, d, ts, x, count(*), sum(v), min(v), max(v) FROM t GROUP BY b, d, ts, x;",
    );
    let key = |i| (0..4).map(|c| shown(i, c)).collect::<Vec<_>>().join("|");
    assert_eq!(joined(out), expected(|_| true, key));
}

#[test]
fn a_filtered_aggregate_groups_only_the_kept_rows() {
    let mut db = store();
    let out = rows(
        &mut db,
        "SELECT k, count(*), sum(v), min(v), max(v) FROM t WHERE v < 500 GROUP BY k;",
    );
    assert_eq!(
        joined(out),
        expected(|i| i * 7 % 1000 < 500, |i| shown(i, 4))
    );
    // FILTER (WHERE …) and COUNT of a nullable column, per date.
    let out = rows(
        &mut db,
        "SELECT d, count(x), sum(v) FILTER (WHERE v >= 500) FROM t GROUP BY d ORDER BY d NULLS FIRST;",
    );
    let mut want: Vec<(String, i64, i64)> = Vec::new();
    for i in 0..ROWS {
        let d = shown(i, 1);
        let at = match want.iter().position(|w| w.0 == d) {
            Some(at) => at,
            None => {
                want.push((d, 0, 0));
                want.len() - 1
            }
        };
        want[at].1 += i64::from(row(i)[3].is_some());
        let v = i * 7 % 1000;
        if v >= 500 {
            want[at].2 += v;
        }
    }
    want.sort_by_key(|w| (w.0 != "NULL", w.0.clone()));
    let want: Vec<String> = want
        .into_iter()
        .map(|(d, n, s)| format!("{d}|{n}|{s}"))
        .collect();
    assert_eq!(joined(out), want);
}

#[test]
fn a_row_the_filter_dropped_raises_no_error() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (k INTEGER, x DECIMAL(15,2));")
        .unwrap();
    let mut values: Vec<String> = (0..5000)
        .map(|i| format!("({}, {}.25)", i % 7, i % 97))
        .collect();
    values.push("(3, 9999999999999.99)".into());
    db.execute(&format!("INSERT INTO t VALUES {};", values.join(",")))
        .unwrap();
    let e = db
        .execute("SELECT k, sum(x * x) FROM t GROUP BY k;")
        .expect_err("the large row overflows");
    assert!(e.to_string().contains("Overflow in multiplication"), "{e}");
    let out = rows(
        &mut db,
        "SELECT k, count(*) FROM t WHERE x < 1000 GROUP BY k ORDER BY k;",
    );
    assert_eq!(out.len(), 7);
    let out = rows(
        &mut db,
        "SELECT k, sum(x * x) IS NOT NULL FROM t WHERE x < 1000 GROUP BY k ORDER BY k;",
    );
    assert!(out.iter().all(|r| r[1] == "true"), "{out:?}");
}

/// VARCHAR keys (wave 123e): the group keeps its string, an empty string and
/// NULL stay different groups, a text key mixes with integer keys, and a
/// chunk whose key columns hold few values takes the dense coded path —
/// first-seen order and every aggregate as before.
#[test]
fn text_keys_with_nulls_and_empty_strings() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE s (name VARCHAR, flag VARCHAR, n INTEGER, v INTEGER);")
        .unwrap();
    let name = |i: i64| match i % 23 {
        0 => None,
        1 => Some(String::new()),
        _ => Some(format!("name-{}", i * 7919 % 9001)),
    };
    let flag = |i: i64| match i % 7 {
        0 => None,
        1 => Some(String::new()),
        r => Some(["A", "N", "R"][r as usize % 3].to_string()),
    };
    let lit = |s: Option<String>| s.map_or_else(|| "NULL".to_string(), |s| format!("'{s}'"));
    let values: Vec<String> = (0..ROWS)
        .map(|i| {
            format!(
                "({}, {}, {}, {})",
                lit(name(i)),
                lit(flag(i)),
                i % 5,
                i * 7 % 1000
            )
        })
        .collect();
    db.execute(&format!("INSERT INTO s VALUES {};", values.join(",")))
        .unwrap();
    let show = |s: Option<String>| s.unwrap_or_else(|| "NULL".into());
    // Many groups, partitioned merge.
    let out = rows(
        &mut db,
        "SELECT name, count(*), sum(v), min(v), max(v) FROM s GROUP BY name;",
    );
    assert_eq!(joined(out), expected(|_| true, |i| show(name(i))));
    // A text key next to an integer key.
    let out = rows(
        &mut db,
        "SELECT name, n, count(*), sum(v), min(v), max(v) FROM s GROUP BY name, n;",
    );
    assert_eq!(
        joined(out),
        expected(|_| true, |i| format!("{}|{}", show(name(i)), i % 5))
    );
    // Few values per column: the dense coded path.
    let out = rows(
        &mut db,
        "SELECT flag, n, count(*), sum(v), min(v), max(v) FROM s GROUP BY flag, n;",
    );
    assert_eq!(
        joined(out),
        expected(|_| true, |i| format!("{}|{}", show(flag(i)), i % 5))
    );
}
