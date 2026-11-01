//! Wave 135a: the pipeline executor answers exactly like the materialized
//! path.
//!
//! `Aggregate`-rooted plans flow through `exec::pipeline` when their input is
//! a chain of `Filter` / `Project` / inner `Join` over any source; the
//! `pipeline_execution` session switch forces the materialized path instead,
//! so every shape here runs both ways and must produce the same rows in the
//! same order — with one worker and with eight. A second group of tests pins
//! the error and deadline behaviour that a streaming executor could silently
//! change (an expression is only ever evaluated on rows that reached its
//! operator), and the EXPLAIN text, which is a plan detail.

use std::time::Duration;

use pinkdb::{Database, ExecLimits, PinkDb, QueryResult, Value};
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

const FACT_ROWS: i64 = 30_000;
const DIM_A_KEYS: i64 = 600;
const DIM_B_KEYS: i64 = 300;

/// `(k_a, k_b, qty, amount, price)`: `k_a`/`k_b` are NULL every 37th/53rd row
/// and reach 100/50 keys the dimensions miss; the decimals cover a repeating
/// cent pattern.
fn fact_row(i: i64) -> String {
    let k_a = if i % 37 == 0 {
        "NULL".to_string()
    } else {
        (i % (DIM_A_KEYS + 100)).to_string()
    };
    let k_b = if i % 53 == 0 {
        "NULL".to_string()
    } else {
        (i % (DIM_B_KEYS + 50)).to_string()
    };
    format!(
        "({k_a}, {k_b}, {}, {:?}, {}.{:02})",
        i % 1000,
        (i % 977) as f64 / 7.0,
        i % 331,
        i % 100
    )
}

/// A fact table and three dimensions (two rows per key for `dim_a`, a missing
/// key range for all three), plus a wide dimension whose single key fans one
/// morsel out over several chunks.
fn mart() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE fact (k_a INTEGER, k_b INTEGER, qty INTEGER, amount DOUBLE,
                            price DECIMAL(10, 2));
         CREATE TABLE dim_a (k INTEGER, region VARCHAR, factor DOUBLE);
         CREATE TABLE dim_b (k INTEGER, label VARCHAR, size INTEGER);
         CREATE TABLE dim_c (k INTEGER, tier VARCHAR);
         CREATE TABLE dim_wide (k INTEGER, w INTEGER);
         CREATE TABLE dim_empty (k INTEGER, z INTEGER);",
    )
    .unwrap();
    let fact: Vec<String> = (0..FACT_ROWS).map(fact_row).collect();
    db.execute(&format!("INSERT INTO fact VALUES {};", fact.join(",")))
        .unwrap();
    let dim_a: Vec<String> = (0..DIM_A_KEYS)
        .flat_map(|k| {
            ["north", "south"]
                .into_iter()
                .map(move |region| format!("({k}, '{region}', {})", (k % 7) as f64 / 2.0))
        })
        .collect();
    db.execute(&format!("INSERT INTO dim_a VALUES {};", dim_a.join(",")))
        .unwrap();
    let dim_b: Vec<String> = (0..DIM_B_KEYS)
        .map(|k| format!("({k}, 'label-{}', {})", k % 13, k * 3))
        .collect();
    db.execute(&format!("INSERT INTO dim_b VALUES {};", dim_b.join(",")))
        .unwrap();
    let dim_c: Vec<String> = (0..50)
        .map(|k| format!("({k}, 'tier-{}')", k % 4))
        .collect();
    db.execute(&format!("INSERT INTO dim_c VALUES {};", dim_c.join(",")))
        .unwrap();
    let wide: Vec<String> = (0..5000).map(|w| format!("(1, {w})")).collect();
    db.execute(&format!("INSERT INTO dim_wide VALUES {};", wide.join(",")))
        .unwrap();
    db
}

/// Every shape the pipeline compiles, one per line of the report's coverage
/// list. Group order matters (no `ORDER BY`), so the comparison is exact.
const SHAPES: &[&str] = &[
    // One dimension, VARCHAR group key from the dimension.
    "SELECT a.region, COUNT(*), SUM(f.qty), MIN(f.amount), MAX(f.amount)
     FROM fact f JOIN dim_a a ON f.k_a = a.k GROUP BY a.region;",
    // AVG over DOUBLE and DECIMAL.
    "SELECT a.region, AVG(f.amount), AVG(f.price), SUM(f.price)
     FROM fact f JOIN dim_a a ON f.k_a = a.k GROUP BY a.region;",
    // INTEGER group key computed from the fact.
    "SELECT f.k_b % 10, COUNT(*), SUM(f.qty)
     FROM fact f JOIN dim_b b ON f.k_b = b.k GROUP BY f.k_b % 10;",
    // A NULL group key.
    "SELECT f.k_a, COUNT(*) FROM fact f JOIN dim_a a ON f.k_a = a.k GROUP BY f.k_a;",
    // Two- and three-dimension chains.
    "SELECT a.region, b.label, COUNT(*), SUM(f.amount)
     FROM fact f JOIN dim_a a ON f.k_a = a.k JOIN dim_b b ON f.k_b = b.k
     GROUP BY a.region, b.label;",
    "SELECT c.tier, COUNT(*), SUM(f.qty)
     FROM fact f JOIN dim_a a ON f.k_a = a.k JOIN dim_b b ON f.k_b = b.k
          JOIN dim_c c ON a.k % 50 = c.k
     GROUP BY c.tier;",
    // Filters on both sides, one of them pushed below its join.
    "SELECT a.region, COUNT(*), SUM(f.qty)
     FROM fact f JOIN dim_a a ON f.k_a = a.k
     WHERE f.qty > 500 AND a.factor > 1.0 GROUP BY a.region;",
    // A filter above the join on both sides at once.
    "SELECT a.region, COUNT(*)
     FROM fact f JOIN dim_a a ON f.k_a = a.k
     WHERE f.qty > 500 AND a.factor > 1.0 AND f.k_a = a.k GROUP BY a.region;",
    // FILTER (WHERE ...) accumulators.
    "SELECT a.region, COUNT(*) FILTER (WHERE f.qty > 500),
            SUM(f.amount) FILTER (WHERE f.qty < 100), COUNT(*)
     FROM fact f JOIN dim_a a ON f.k_a = a.k GROUP BY a.region;",
    // DISTINCT accumulators (the general sink).
    "SELECT a.region, COUNT(DISTINCT b.label), COUNT(DISTINCT f.k_b)
     FROM fact f JOIN dim_a a ON f.k_a = a.k JOIN dim_b b ON f.k_b = b.k
     GROUP BY a.region;",
    // A projection between the aggregate and the join, reordering columns.
    "SELECT region, COUNT(*), SUM(q)
     FROM (SELECT a.region AS region, f.qty + 1 AS q
           FROM fact f JOIN dim_a a ON f.k_a = a.k) s GROUP BY region;",
    // An empty build side: no groups, and the global aggregate's one row.
    "SELECT b.z, COUNT(*) FROM fact f JOIN dim_empty b ON f.k_b = b.k GROUP BY b.z;",
    "SELECT COUNT(*), SUM(f.qty) FROM fact f JOIN dim_empty b ON f.k_b = b.k;",
    // A probe chunk whose matches span more than one output chunk.
    "SELECT COUNT(*), SUM(d.w) FROM fact f JOIN dim_wide d ON f.k_b = d.k;",
    "SELECT d.w % 7, COUNT(*) FROM fact f JOIN dim_wide d ON f.k_b = d.k GROUP BY d.w % 7;",
    // A same-key global aggregate over the fan-out.
    "SELECT SUM(f.amount * d.w) FROM fact f JOIN dim_wide d ON f.k_b = d.k;",
];

fn check_shapes(db: &mut PinkDb) {
    for sql in SHAPES {
        both_paths(db, sql);
    }
    // Independently computed totals: the dimension holds two rows per key, so
    // every matched fact row pairs with two dimension rows.
    let matched = (0..FACT_ROWS)
        .filter(|i| i % 37 != 0 && i % (DIM_A_KEYS + 100) < DIM_A_KEYS)
        .count() as i64;
    assert_eq!(
        both_paths(
            db,
            "SELECT COUNT(*) FROM fact f JOIN dim_a a ON f.k_a = a.k;"
        ),
        vec![format!("{}", matched * 2)]
    );
    assert_eq!(
        both_paths(
            db,
            "SELECT COUNT(*) FROM fact f JOIN dim_b b ON f.k_b = b.k;"
        ),
        vec![format!(
            "{}",
            (0..FACT_ROWS)
                .filter(|i| i % 53 != 0 && i % (DIM_B_KEYS + 50) < DIM_B_KEYS)
                .count()
        )]
    );
    // One morsel of the probe fans out to 5000 build rows: 15 chunks of
    // output, all folded in morsel order. `dim_wide` is keyed 1 only.
    assert_eq!(
        both_paths(
            db,
            "SELECT COUNT(*) FROM fact f JOIN dim_wide d ON f.k_b = d.k;"
        ),
        vec![format!(
            "{}",
            (0..FACT_ROWS)
                .filter(|i| i % 53 != 0 && i % (DIM_B_KEYS + 50) == 1)
                .count() as i64
                * 5000
        )]
    );
}

#[test]
fn pipeline_matches_materialized_with_one_and_eight_workers() {
    for threads in [1, 8] {
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        pool.install(|| check_shapes(&mut mart()));
    }
}

/// An erroring aggregate argument raises only where a match reached it: the
/// last fact row divides by zero and matches, so the statement fails.
#[test]
fn error_in_a_late_morsel_fails_the_statement() {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE fact (k INTEGER, q INTEGER);
         CREATE TABLE dim (k INTEGER);
         INSERT INTO dim VALUES (1);",
    )
    .unwrap();
    let mut insert = String::from("INSERT INTO fact VALUES (1, 1)");
    for _ in 1..FACT_ROWS {
        insert.push_str(", (1, 1)");
    }
    insert.push_str(", (1, 0);");
    db.execute(&insert).unwrap();
    let sql = "SELECT SUM(10 / q) FROM fact JOIN dim ON fact.k = dim.k;";
    // The materialized path raises on the same row, so both fail.
    for pipeline_execution in ["true", "false"] {
        db.execute(&format!("SET pipeline_execution = {pipeline_execution};"))
            .unwrap();
        let e = db
            .execute(sql)
            .expect_err("a matched row with q = 0 must divide by zero")
            .to_string();
        assert!(e.contains("division by zero"), "{e}");
    }
    // The same expression on a row no match reaches does not raise.
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE fact (k INTEGER, q INTEGER);
         CREATE TABLE dim (k INTEGER);
         INSERT INTO dim VALUES (1);
         INSERT INTO fact VALUES (1, 2), (2, 0);",
    )
    .unwrap();
    assert_eq!(
        both_paths(
            &mut db,
            "SELECT SUM(10 / q) FROM fact JOIN dim ON fact.k = dim.k;"
        ),
        vec!["5".to_string()]
    );
}

/// A deadline that expires while the probe pipeline runs fails the statement
/// (the probe checks it at every preserved chunk).
#[test]
fn deadline_expiring_inside_the_pipeline_times_out() {
    let mut store = PinkDb::new();
    store
        .execute(
            "CREATE TABLE probe_side (k INTEGER, v INTEGER);
             CREATE TABLE build_side (k INTEGER, w INTEGER);",
        )
        .unwrap();
    let probe: Vec<String> = (0..FACT_ROWS)
        .map(|i| format!("({}, {i})", i % 350))
        .collect();
    store
        .execute(&format!(
            "INSERT INTO probe_side VALUES {};",
            probe.join(",")
        ))
        .unwrap();
    let build: Vec<String> = (0..350)
        .flat_map(|k| (0..40).map(move |w| format!("({k}, {w})")))
        .collect();
    store
        .execute(&format!(
            "INSERT INTO build_side VALUES {};",
            build.join(",")
        ))
        .unwrap();
    // Limits are a connection policy, so the store is wrapped in a database.
    let db = Database::from_store(store);
    let mut conn = db.connect();
    let sql = "SELECT b.w % 5, COUNT(*), SUM(p.v + b.w)
               FROM probe_side p JOIN build_side b ON p.k = b.k GROUP BY b.w % 5;";
    // 30k probe rows x 40 matches per key: well past 1 ms.
    conn.set_exec_limits(ExecLimits {
        statement_timeout: Some(Duration::from_millis(1)),
        max_result_rows: None,
        max_result_bytes: None,
    });
    let e = conn
        .execute(sql)
        .expect_err("the deadline must expire inside the probe pipeline")
        .to_string();
    assert!(e.contains("statement timeout"), "{e}");
    conn.clear_exec_limits();
    // The same query answers the same rows once the deadline is gone.
    let pipelined = rows(&mut conn, sql);
    conn.execute("SET pipeline_execution = false;").unwrap();
    assert_eq!(pipelined, rows(&mut conn, sql));
    assert_eq!(pipelined.len(), 5);
}

/// The pipeline is an executor detail: EXPLAIN shows the plan, not the
/// strategy, so the text is identical with the pipeline on and off.
#[test]
fn explain_text_ignores_the_pipeline() {
    let mut db = mart();
    for sql in [
        "SELECT a.region, COUNT(*), SUM(f.qty)
         FROM fact f JOIN dim_a a ON f.k_a = a.k GROUP BY a.region;",
        "SELECT COUNT(*) FROM fact f JOIN dim_a a ON f.k_a = a.k
         WHERE f.qty > 500;",
    ] {
        let explained = rows(&mut db, &format!("EXPLAIN {sql}"));
        db.execute("SET pipeline_execution = false;").unwrap();
        assert_eq!(explained, rows(&mut db, &format!("EXPLAIN {sql}")), "{sql}");
        db.execute("SET pipeline_execution = true;").unwrap();
        // And the plan is the unmodified shape: one aggregate over a join.
        let text = explained.join("\n");
        assert!(text.contains("Aggregate"), "{text}");
        assert!(text.contains("Join"), "{text}");
        assert!(!text.to_lowercase().contains("pipeline"), "{text}");
    }
}
