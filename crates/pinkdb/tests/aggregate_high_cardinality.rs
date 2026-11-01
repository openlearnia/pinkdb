//! Wave 135d: the flat group table under high cardinality.
//!
//! A high-cardinality GROUP BY makes nearly every row its own group, so a
//! chunk's partial stops reducing: the fold writes each group's key and
//! accumulators once, indexes them by merge partition instead of copying
//! them, and the merge adopts the first occurrence's row rather than copying
//! it into a second table. None of that may change an answer, so every shape
//! here runs through the pipeline and through the materialized path and must
//! produce the same rows in the same order — float sums included, since both
//! paths fold a chunk's rows in row order and merge the chunks in order.
//!
//! The table holds a million rows and every key type the flat table takes,
//! plus a NULL-able key, a key that repeats in every chunk, and a key whose
//! cardinality jumps a third of the way in (the fold's dense code table for
//! the low-cardinality part, its hashed table after that).

use pinkdb::{PinkDb, QueryResult, Value};
use rayon::ThreadPoolBuilder;

fn rows(db: &mut impl QueryTarget, sql: &str) -> Vec<String> {
    let results = db.run(sql).unwrap_or_else(|e| panic!("{sql}\n{e}"));
    match results.into_iter().next().unwrap() {
        QueryResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|r| r.iter().map(Value::to_string).collect::<Vec<_>>().join("|"))
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// A store or a connection: both run a statement and hand back its results.
trait QueryTarget {
    fn run(&mut self, sql: &str) -> pinkdb::Result<Vec<QueryResult>>;
}

impl QueryTarget for PinkDb {
    fn run(&mut self, sql: &str) -> pinkdb::Result<Vec<QueryResult>> {
        self.execute(sql)
    }
}

impl QueryTarget for pinkdb::Connection {
    fn run(&mut self, sql: &str) -> pinkdb::Result<Vec<QueryResult>> {
        self.execute(sql)
    }
}

/// `sql` through the pipeline and through the materialized path.
fn both_paths(db: &mut impl QueryTarget, sql: &str) -> Vec<String> {
    let pipelined = rows(db, sql);
    db.run("SET pipeline_execution = false;").unwrap();
    let materialized = rows(db, sql);
    db.run("SET pipeline_execution = true;").unwrap();
    assert_eq!(pipelined, materialized, "{sql}");
    pipelined
}

const FACT_ROWS: usize = 1_000_000;

/// The fact table: `id` unique (a million groups), `i`/`d`/`vs`/`vl` with a
/// few thousand values each, `nv` NULL every 37th row, and the measures the
/// flat table takes (INTEGER, BIGINT, DOUBLE, DECIMAL).
fn fact() -> PinkDb {
    let mut db = PinkDb::new();
    let seed: Vec<String> = (0..1000).map(|i| format!("({i})")).collect();
    db.execute(&format!(
        "CREATE TABLE n0 (i INTEGER);
         INSERT INTO n0 VALUES {};",
        seed.join(",")
    ))
    .unwrap();
    db.execute(
        "CREATE TABLE big (id BIGINT, i INTEGER, d DATE, vs VARCHAR, vl VARCHAR,
                           f DOUBLE, dec DECIMAL(12,2), n INTEGER, nv INTEGER);",
    )
    .unwrap();
    db.execute(
        "INSERT INTO big
         SELECT r, r % 20011, DATE '2020-01-01' + r % 3000,
                's' || (r % 997)::VARCHAR,
                'long-string-value-' || (r % 991)::VARCHAR || '-tail',
                (r % 977)::DOUBLE / 7.0,
                ((r % 331)::DECIMAL(12, 2)) / 100.0,
                r % 1000,
                CASE WHEN r % 37 = 0 THEN NULL ELSE r % 97 END
         FROM (SELECT a.i * 1000 + b.i AS r FROM n0 a CROSS JOIN n0 b) g;",
    )
    .unwrap();
    assert_eq!(rows(&mut db, "SELECT count(*) FROM big;"), vec!["1000000"]);
    let _ = FACT_ROWS;
    db
}

/// One query per key type the flat table takes, plus the aggregate kinds.
#[test]
fn a_million_distinct_keys_of_every_type() {
    let mut db = fact();
    // INTEGER key, every aggregate kind over INTEGER / BIGINT / DOUBLE /
    // DECIMAL, in first-seen order.
    assert_eq!(
        both_paths(
            &mut db,
            "SELECT i, count(*), count(n), sum(n), sum(id), sum(f), sum(dec),
                    avg(n), avg(id), avg(f), avg(dec), min(n), max(n)
             FROM big GROUP BY i;"
        )
        .len(),
        20011
    );
    // BIGINT key with a million groups (as many groups as rows).
    assert_eq!(
        both_paths(&mut db, "SELECT id, count(*), sum(n) FROM big GROUP BY id;").len(),
        1_000_000
    );
    // DATE key.
    assert_eq!(
        both_paths(
            &mut db,
            "SELECT d, count(*), min(vs), max(vl) FROM big GROUP BY d;"
        )
        .len(),
        3000
    );
    // VARCHAR keys, short and long.
    assert_eq!(
        both_paths(&mut db, "SELECT vs, count(*), sum(i) FROM big GROUP BY vs;").len(),
        997
    );
    assert_eq!(
        both_paths(&mut db, "SELECT vl, count(*), sum(f) FROM big GROUP BY vl;").len(),
        991
    );
    // A million distinct long strings, from an expression.
    assert_eq!(
        both_paths(
            &mut db,
            "SELECT 'row-' || id::VARCHAR || '-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' AS s,
                    count(*)
             FROM big GROUP BY s;"
        )
        .len(),
        1_000_000
    );
}

/// QuackLab's mart grain: several key columns at once, almost one group per
/// row, with every aggregate kind over the row.
#[test]
fn a_mixed_multi_column_grain() {
    let mut db = fact();
    let sql = "SELECT id, i, d, vs, vl, count(*), sum(n), sum(f), sum(dec),
                      min(n), max(n)
               FROM big GROUP BY id, i, d, vs, vl;";
    assert_eq!(both_paths(&mut db, sql).len(), 1_000_000);
}

/// A NULL group key and a key that repeats in every chunk (so every partial
/// holds every group, and the merge folds many partials into one row).
#[test]
fn null_keys_and_groups_that_repeat_across_distant_chunks() {
    let mut db = fact();
    assert_eq!(
        both_paths(
            &mut db,
            "SELECT nv, count(*), count(nv), sum(n), avg(i), min(vs), max(vl)
             FROM big GROUP BY nv;"
        )
        .len(),
        98
    );
    assert_eq!(
        both_paths(
            &mut db,
            "SELECT i % 7 AS b, count(*), sum(f), sum(dec), avg(f), min(vs), max(vl)
             FROM big GROUP BY b;"
        )
        .len(),
        7
    );
}

/// A key whose cardinality jumps mid-input: sixteen groups for the first half
/// of the rows, half a million for the rest.
#[test]
fn cardinality_that_jumps_mid_input() {
    let mut db = fact();
    let sql = "SELECT CASE WHEN id < 500000 THEN id % 16 ELSE id END AS g,
                      count(*), sum(f), sum(n)
               FROM big GROUP BY g;";
    assert_eq!(both_paths(&mut db, sql).len(), 500_016);
}

/// The set consumer (`IN` subquery) asks for no order; a `LIMIT` asks for
/// one, so both plans run and answer the same rows.
#[test]
fn unordered_sets_and_ordered_limits() {
    let mut db = fact();
    assert_eq!(
        both_paths(
            &mut db,
            "SELECT count(*) FROM big WHERE vs IN (SELECT vs FROM big GROUP BY vs);"
        ),
        vec!["1000000"]
    );
    assert_eq!(
        both_paths(
            &mut db,
            "SELECT count(*) FROM big
             WHERE i IN (SELECT i FROM big WHERE n < 500 GROUP BY i);"
        )
        .len(),
        1
    );
    // The IN-subquery's GROUP BY emits in merge-partition order: the row set
    // must still be the whole table's, once each.
    assert_eq!(
        both_paths(
            &mut db,
            "SELECT count(*), sum(c), sum(s) FROM
               (SELECT i, count(*) AS c, sum(n) AS s FROM big GROUP BY i) g
             WHERE i IN (SELECT i FROM big GROUP BY i);"
        ),
        vec!["20011|1000000|499500000"]
    );
    assert_eq!(
        both_paths(&mut db, "SELECT i, count(*) FROM big GROUP BY i LIMIT 3;").len(),
        3
    );
}

/// The same answers with one worker and with eight.
#[test]
fn high_cardinality_matches_with_one_and_eight_workers() {
    let mut db = fact();
    for threads in [1, 8] {
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        pool.install(|| {
            assert_eq!(
                both_paths(&mut db, "SELECT id, count(*), sum(f) FROM big GROUP BY id;").len(),
                1_000_000
            );
            assert_eq!(
                both_paths(
                    &mut db,
                    "SELECT id, i, d, vs, count(*), sum(dec) FROM big GROUP BY id, i, d, vs;"
                )
                .len(),
                1_000_000
            );
            assert_eq!(
                both_paths(
                    &mut db,
                    "SELECT i % 7 AS b, count(*), sum(f) FROM big GROUP BY b;"
                )
                .len(),
                7
            );
        });
    }
}

/// The materialized path shares the flat table, so pin its answers against the
/// general path too: a DOUBLE key is not one the flat table takes, and casting
/// the key to DOUBLE keeps the groups (and their first-seen order) the same.
#[test]
fn the_general_path_agrees_on_every_measure() {
    let mut db = fact();
    let flat = both_paths(
        &mut db,
        "SELECT i, count(*), count(n), sum(n), sum(id), sum(f), sum(dec),
                avg(n), avg(f), avg(dec), min(n), max(vl)
         FROM big GROUP BY i;",
    );
    let general = rows(
        &mut db,
        "SELECT i::DOUBLE AS k, count(*), count(n), sum(n), sum(id), sum(f),
                sum(dec), avg(n), avg(f), avg(dec), min(n), max(vl)
         FROM big GROUP BY k;",
    );
    assert_eq!(flat.len(), 20011);
    let strip = |row: &str| -> String { row.split_once('|').map_or("", |x| x.1).to_string() };
    let flat_cols: Vec<String> = flat.iter().map(|r| strip(r)).collect();
    let general_cols: Vec<String> = general.iter().map(|r| strip(r)).collect();
    assert_eq!(flat_cols, general_cols);
}
