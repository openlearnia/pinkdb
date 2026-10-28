//! A group-by's result must not depend on how phase 2 of aggregation is
//! scheduled (wave 111c made a large merge hash-partitioned and parallel).
//!
//! Every case runs twice in one database and once in DuckDB 2.0: the two pinkdb
//! runs must be identical *including row order* (first-seen group order is part
//! of the contract the serial merge had), and the rows must match DuckDB.
//! Covers more distinct groups than the parallel path needs to trigger, groups
//! whose rows spread over many chunks, NULL keys, every aggregate kind, the
//! DISTINCT variants and `FILTER (WHERE …)`.
//!
//! `W111_AGG_ROWS` changes the row count, `W111_AGG_VERBOSE=1` prints cases.

use std::path::Path;
use std::process::{Command, Stdio};

use pinkdb::{PinkDb, QueryResult, Value};

const DUCKDB: &str = "/tmp/duckdb-2.0.0/duckdb";

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// `t(g, v)`: `g` has many duplicates and a NULL group, `v` a sign mix and a
/// NULL every few rows.
fn setup_sql(rows: usize) -> String {
    let mut rng = Rng::new(7);
    let mut sql = String::from("CREATE TABLE t (g INTEGER, v INTEGER); ");
    for start in (0..rows).step_by(400) {
        sql.push_str("INSERT INTO t VALUES ");
        for i in start..(start + 400).min(rows) {
            if i > start {
                sql.push(',');
            }
            let g = if rng.below(100) < 3 {
                "NULL".to_string()
            } else {
                // ~40k groups: a chunk holds mostly its own keys, so phase 2
                // has several local groups per key to fold.
                (rng.below(40_000) as i64).to_string()
            };
            let v = if rng.below(100) < 5 {
                "NULL".to_string()
            } else {
                (rng.below(21) as i64 - 10).to_string()
            };
            sql.push_str(&format!("({g},{v})"));
        }
        sql.push_str("; ");
    }
    sql
}

/// One aggregation shape; the alias is a valid identifier per case.
const CASES: &[(&str, &str)] = &[
    ("count-star", "SELECT g, count(*) AS a FROM t GROUP BY g"),
    ("sum", "SELECT g, sum(v) AS a FROM t GROUP BY g"),
    (
        "avg-min-max",
        "SELECT g, avg(v) AS a, min(v) AS b, max(v) AS c FROM t GROUP BY g",
    ),
    (
        "count-cols",
        "SELECT g, count(v) AS a, count(DISTINCT v) AS b FROM t GROUP BY g",
    ),
    (
        "distinct-counts",
        "SELECT g, count(DISTINCT v) AS a, count(DISTINCT v || 'x') AS b FROM t GROUP BY g",
    ),
    (
        "filtered",
        "SELECT g, count(*) FILTER (WHERE v > 0) AS a, sum(v) FILTER (WHERE v < 0) AS b \
         FROM t GROUP BY g",
    ),
    (
        "two-keys",
        "SELECT g, v, count(*) AS a, sum(v) FILTER (WHERE v IS NOT NULL) AS b \
         FROM t GROUP BY g, v",
    ),
];

enum Outcome {
    Rows(Vec<String>),
    Err(String),
}

fn run_pinkdb(db: &mut PinkDb, sql: &str) -> Outcome {
    match db.execute(sql) {
        Err(e) => Outcome::Err(e.to_string()),
        Ok(mut results) => match results.pop() {
            Some(QueryResult::Selected { rows, .. }) => Outcome::Rows(
                rows.iter()
                    .map(|r| {
                        r.iter()
                            .map(|v| match v {
                                Value::Null => "NULL".to_string(),
                                Value::Boolean(b) => b.to_string(),
                                Value::Integer(i) => i.to_string(),
                                Value::Bigint(i) => i.to_string(),
                                // Debug prints the shortest round-trip form
                                // with a `.0`, which is how DuckDB prints a
                                // DOUBLE.
                                Value::Double(f) => format!("{f:?}"),
                                other => format!("{other:?}"),
                            })
                            .collect::<Vec<_>>()
                            .join("\t")
                    })
                    .collect(),
            ),
            other => Outcome::Err(format!("not a row result: {other:?}")),
        },
    }
}

fn run_duckdb(setup: &str, sql: &str) -> Outcome {
    let mut child = Command::new(DUCKDB)
        .args([
            "-no-agent",
            "-noheader",
            "-list",
            "-separator",
            "\t",
            "-nullvalue",
            "NULL",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn duckdb");
    {
        use std::io::Write;
        writeln!(child.stdin.as_mut().unwrap(), "{setup}\n{sql};").unwrap();
    }
    let out = child.wait_with_output().unwrap();
    if !out.status.success() {
        return Outcome::Err(String::from_utf8_lossy(&out.stderr).into_owned());
    }
    Outcome::Rows(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(|l| l.trim_end().to_string())
            .collect(),
    )
}

#[test]
fn aggregated_rows_survive_the_parallel_merge() {
    let rows = std::env::var("W111_AGG_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60_000usize);
    let verbose = std::env::var("W111_AGG_VERBOSE").is_ok();
    let setup = setup_sql(rows);
    let mut db = PinkDb::new();
    for stmt in setup.split(';') {
        if !stmt.trim().is_empty() {
            db.execute(stmt).unwrap();
        }
    }
    let duck = Path::new(DUCKDB).exists();
    for (name, sql) in CASES {
        if verbose {
            eprintln!("{name}: {sql}");
        }
        let first = match run_pinkdb(&mut db, sql) {
            Outcome::Rows(rows) => rows,
            Outcome::Err(e) => panic!("{name} failed: {e}"),
        };
        let again = match run_pinkdb(&mut db, sql) {
            Outcome::Rows(rows) => rows,
            Outcome::Err(e) => panic!("{name} failed on rerun: {e}"),
        };
        assert!(
            first == again,
            "{name}: group order changed between runs ({})",
            difference(&first, &again)
        );
        assert!(!first.is_empty(), "{name}: no groups");
        if duck {
            match run_duckdb(&setup, sql) {
                Outcome::Rows(expected) => {
                    let mut ours = first.clone();
                    let mut expected = expected;
                    ours.sort();
                    expected.sort();
                    assert!(
                        ours == expected,
                        "{name}: rows differ from DuckDB ({})",
                        difference(&ours, &expected)
                    );
                }
                Outcome::Err(e) => panic!("{name}: DuckDB error: {e}"),
            }
        }
    }
    eprintln!(
        "{} aggregation shapes matched over {rows} rows",
        CASES.len()
    );
}

/// A thirty-thousand-row assertion message is unreadable, so report the row
/// counts and the first few rows that differ.
fn difference(ours: &[String], theirs: &[String]) -> String {
    let mut out = format!("{} rows vs {} rows", ours.len(), theirs.len());
    let mut shown = 0;
    for (a, b) in ours.iter().zip(theirs) {
        if a != b {
            out.push_str(&format!("\n  ours: {a}\n  theirs: {b}"));
            shown += 1;
            if shown == 5 {
                break;
            }
        }
    }
    out
}
