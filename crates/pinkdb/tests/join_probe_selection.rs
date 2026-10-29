//! Wave 125e: an INNER join whose probe side is a filter probes the
//! unfiltered chunks through the filter's selection (no filtered copy); a
//! sparse selection is compacted first, and a residual gets a compacted
//! chunk. Answers must match the rows computed here: dense and sparse
//! filters, NULL probe keys, duplicate build keys, and a residual.

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

const N: i64 = 30000;

/// `(k, d, v)` of fact row `i`; `k` is NULL every 37th row.
fn fact(i: i64) -> (Option<i64>, i64, i64) {
    ((i % 37 != 0).then_some(i % 700), i % 100, i * 3 % 1000)
}

/// Dimension keys: 0..600, each twice (two rows per key).
fn store() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE fact (k INTEGER, d INTEGER, v INTEGER);")
        .unwrap();
    db.execute("CREATE TABLE dim (k INTEGER, w INTEGER);")
        .unwrap();
    let f: Vec<String> = (0..N)
        .map(|i| {
            let (k, d, v) = fact(i);
            format!("({}, {d}, {v})", k.map_or("NULL".into(), |k| k.to_string()))
        })
        .collect();
    let d: Vec<String> = (0..1200i64)
        .map(|j| format!("({}, {})", j % 600, j))
        .collect();
    db.execute(&format!("INSERT INTO fact VALUES {};", f.join(",")))
        .unwrap();
    db.execute(&format!("INSERT INTO dim VALUES {};", d.join(",")))
        .unwrap();
    db
}

/// count(*), sum(v), sum(w) over fact rows `keep` accepts joined with dim
/// rows `pair` accepts.
fn expected(keep: impl Fn(i64) -> bool, pair: impl Fn(i64, i64) -> bool) -> String {
    let (mut n, mut sv, mut sw) = (0i64, 0i64, 0i64);
    for i in (0..N).filter(|&i| keep(i)) {
        let (k, _, v) = fact(i);
        let Some(k) = k else { continue };
        for w in [k, k + 600].into_iter().filter(|_| k < 600) {
            if pair(v, w) {
                n += 1;
                sv += v;
                sw += w;
            }
        }
    }
    format!("{n}|{sv}|{sw}")
}

#[test]
fn dense_and_sparse_filters_on_the_probe_side() {
    let mut db = store();
    for (cond, keep) in [
        (
            "fact.d < 70",
            Box::new(|i: i64| i % 100 < 70) as Box<dyn Fn(i64) -> bool>,
        ),
        ("fact.d = 7", Box::new(|i: i64| i % 100 == 7)),
        ("fact.d >= 0", Box::new(|_| true)),
    ] {
        let got = rows(
            &mut db,
            &format!(
                "SELECT count(*), sum(v), sum(w) FROM fact, dim WHERE fact.k = dim.k AND {cond};"
            ),
        );
        assert_eq!(got, vec![expected(&keep, |_, _| true)], "{cond}");
        // The same through a projection the aggregate does not fuse with.
        let got = rows(
            &mut db,
            &format!(
                "SELECT count(*), sum(v), sum(w) FROM (SELECT fact.v AS v, dim.w AS w \
                 FROM fact, dim WHERE fact.k = dim.k AND {cond} ORDER BY 1, 2) q;"
            ),
        );
        assert_eq!(
            got,
            vec![expected(&keep, |_, _| true)],
            "{cond} (projected)"
        );
    }
}

#[test]
fn a_residual_on_a_filtered_probe_side() {
    let mut db = store();
    let got = rows(
        &mut db,
        "SELECT count(*), sum(v), sum(w) FROM (SELECT fact.v AS v, dim.w AS w FROM fact \
         JOIN dim ON fact.k = dim.k AND fact.v < dim.w WHERE fact.d < 70 ORDER BY 1) q;",
    );
    assert_eq!(got, vec![expected(|i| i % 100 < 70, |v, w| v < w)]);
}
