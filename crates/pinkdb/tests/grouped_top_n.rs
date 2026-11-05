//! T4: `QUALIFY row_number() OVER (PARTITION BY … ORDER BY …) <= N` runs as a
//! grouped Top-N (per-worker, per-partition bounded heaps) instead of numbering
//! every row. It must return exactly what the window path returns: the same
//! rows, numbers and row order, ties broken by input position, with NULL
//! partition and order keys, on one worker and on ten, streamed from a store
//! and from a materialized input.
//!
//! The reference wraps the row number in a cast (`CAST(rn AS DOUBLE) <= N`),
//! a predicate the grouped Top-N does not recognise, so it takes the window
//! path over the same data.

use pinkdb::{PinkDb, QueryResult, Value};
use rayon::ThreadPoolBuilder;

fn rows(db: &mut PinkDb, sql: &str) -> Vec<String> {
    let results = db.execute(sql).unwrap_or_else(|e| panic!("{sql}\n{e}"));
    match results.into_iter().last().unwrap() {
        QueryResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|r| r.iter().map(Value::to_string).collect::<Vec<_>>().join("|"))
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

const ROWS: i64 = 150_000;

/// `(id, p, q, o, s, d)`: `p` a VARCHAR partition key with NULLs, `q` a small
/// INTEGER one with NULLs, `o` an order key with many ties and NULLs, `s` a
/// VARCHAR tie-breaker, `d` a DOUBLE with ties.
fn table() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute(&format!(
        "CREATE TABLE t AS SELECT i AS id, \
           CASE WHEN i % 41 = 0 THEN NULL ELSE 'p' || CAST(i % 97 AS VARCHAR) END AS p, \
           CASE WHEN i % 13 = 0 THEN NULL ELSE i % 5 END AS q, \
           CASE WHEN i % 29 = 0 THEN NULL ELSE (i * 7919) % 50 END AS o, \
           's' || CAST((i * 31) % 7 AS VARCHAR) AS s, \
           CAST((i * 104729) % 1000 AS DOUBLE) / 8 AS d \
         FROM range({ROWS}) r(i);"
    ))
    .unwrap();
    db
}

/// `(window, bound)` pairs: the grouped query is
/// `QUALIFY row_number() OVER (window) <bound>`; `bound` is written against
/// `rn` and substituted both ways.
const SHAPES: &[(&str, &str)] = &[
    ("PARTITION BY p ORDER BY o", "rn <= 3"),
    ("PARTITION BY p ORDER BY o DESC NULLS FIRST", "rn < 4"),
    ("PARTITION BY p ORDER BY o", "rn = 1"),
    ("PARTITION BY p ORDER BY o DESC, s", "rn <= 5"),
    (
        "PARTITION BY p, q ORDER BY d DESC, o NULLS FIRST",
        "rn <= 2",
    ),
    ("PARTITION BY q ORDER BY s, o DESC", "rn <= 10"),
    // One row per partition: every row is kept, numbered 1.
    ("PARTITION BY id ORDER BY o", "rn <= 2"),
    // One partition.
    ("ORDER BY o, s DESC", "rn <= 7"),
    // No ORDER BY: every row ties, so input order decides.
    ("PARTITION BY q", "rn <= 4"),
    // A newer row always wins, so every worker keeps replacing rows and
    // compacts its chunks.
    ("PARTITION BY id % 7 ORDER BY id DESC", "rn <= 3"),
    // Mirrored and with another conjunct.
    ("PARTITION BY p ORDER BY o", "3 >= rn"),
    ("PARTITION BY p ORDER BY o", "rn <= 3 AND id % 2 = 0"),
    // A bound past every partition's size.
    ("PARTITION BY q ORDER BY o", "rn <= 100000"),
];

fn queries(window: &str, bound: &str) -> Vec<(String, String)> {
    let grouped = bound.replace("rn", &format!("row_number() OVER ({window})"));
    let reference = bound.replace(
        "rn",
        &format!("CAST(row_number() OVER ({window}) AS DOUBLE)"),
    );
    let select = |qualify: &str, wrap: &dyn Fn(String) -> String| {
        wrap(format!(
            "SELECT id, p, q, o, s, d, row_number() OVER ({window}) AS n FROM t QUALIFY {qualify}"
        ))
    };
    let plain = |s: String| format!("{s};");
    let counted = |s: String| format!("SELECT count(*), sum(id), sum(n) FROM ({s});");
    let ordered = |s: String| format!("SELECT * FROM ({s}) ORDER BY n DESC, id LIMIT 50;");
    let empty = |s: String| {
        format!(
            "{};",
            s.replace("FROM t", "FROM (SELECT * FROM t WHERE id < 0) t")
        )
    };
    vec![
        (select(&grouped, &plain), select(&reference, &plain)),
        (select(&grouped, &counted), select(&reference, &counted)),
        (select(&grouped, &ordered), select(&reference, &ordered)),
        (select(&grouped, &empty), select(&reference, &empty)),
        // Without the number in the select list.
        (
            format!("SELECT id, p FROM t QUALIFY {grouped};"),
            format!("SELECT id, p FROM t QUALIFY {reference};"),
        ),
    ]
}

fn check_all(db: &mut PinkDb, label: &str) {
    for (window, bound) in SHAPES {
        for (grouped, reference) in queries(window, bound) {
            let want = rows(db, &reference);
            let got = rows(db, &grouped);
            assert_eq!(got, want, "{label}: {grouped}");
            // The materialized input (no pipeline) takes the same path.
            db.execute("SET pipeline_execution = false;").unwrap();
            let materialized = rows(db, &grouped);
            db.execute("SET pipeline_execution = true;").unwrap();
            assert_eq!(materialized, want, "{label} (materialized): {grouped}");
        }
    }
}

#[test]
fn grouped_top_n_matches_the_window_path_on_one_and_ten_workers() {
    for threads in [1, 10] {
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        pool.install(|| check_all(&mut table(), &format!("{threads} threads, in memory")));
    }
}

/// From a saved store the window's input is the store's streamed scan, with
/// morsels claimed by whichever worker is free.
#[test]
fn grouped_top_n_from_a_store_matches_on_one_and_ten_workers() {
    let dir = std::env::temp_dir().join(format!("pinkdb_grouped_topn_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("t.pink");
    table().save(path.to_str().unwrap()).unwrap();
    let mut per_threads = Vec::new();
    for threads in [1, 10] {
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        pool.install(|| {
            let mut db = PinkDb::open(path.to_str().unwrap()).unwrap();
            check_all(&mut db, &format!("{threads} threads, store"));
            per_threads.push(rows(
                &mut db,
                "SELECT id, row_number() OVER (PARTITION BY p ORDER BY o) AS n FROM t \
                 QUALIFY row_number() OVER (PARTITION BY p ORDER BY o) <= 3;",
            ));
        });
    }
    // Ties resolve by input position, so the thread count changes nothing.
    assert_eq!(per_threads[0], per_threads[1]);
    let _ = std::fs::remove_dir_all(&dir);
}
