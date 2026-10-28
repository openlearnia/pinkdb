//! Differential test for joins that carry a residual predicate (wave 111c).
//!
//! A join's residual is evaluated on each candidate key match, so a pair that
//! fails it is not a match: an inner join drops the pair, and `Semi` / `Anti`
//! treat it as absent. Each generated join is checked twice — against a
//! nested-loop reference computed here, and against DuckDB 2.0 — so a residual
//! that is merely *evaluated* (rather than applied per pair) cannot pass.
//!
//! Covers inner joins (`ON` with an equality plus a residual, the shape the
//! binder hands over as a hash join) and `EXISTS` / `NOT EXISTS` (semi / anti)
//! over the same data, with NULLs and duplicate keys on both sides.
//!
//! Seeded and deterministic. `W111_RESIDUAL_SEEDS` (`1,2`) and
//! `W111_RESIDUAL_CASES` widen it; `W111_RESIDUAL_VERBOSE=1` prints cases.

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

/// `ra(id, k, v)` / `rb(id, k, v)`: small key and value ranges make duplicate
/// keys and NULL-key rows common.
fn gen_rows(rng: &mut Rng, n: usize) -> Vec<[Option<i64>; 3]> {
    (0..n)
        .map(|i| {
            let mut col = || {
                if rng.below(100) < 15 {
                    None
                } else {
                    Some(rng.below(5) as i64)
                }
            };
            [Some(i as i64), col(), col()]
        })
        .collect()
}

fn literal(v: Option<i64>) -> String {
    v.map(|v| v.to_string()).unwrap_or("NULL".into())
}

fn setup_sql(ra: &[[Option<i64>; 3]], rb: &[[Option<i64>; 3]]) -> String {
    let mut sql = String::from(
        "CREATE TABLE ra (id INTEGER, k INTEGER, v INTEGER); \
         CREATE TABLE rb (id INTEGER, k INTEGER, v INTEGER); ",
    );
    for (name, rows) in [("ra", ra), ("rb", rb)] {
        for r in rows {
            sql.push_str(&format!(
                "INSERT INTO {name} VALUES ({}, {}, {}); ",
                literal(r[0]),
                literal(r[1]),
                literal(r[2])
            ));
        }
    }
    sql
}

/// Residual predicates over a candidate `ra`/`rb` pair, including ones that are
/// NULL for some pairs (a NULL residual is not a match).
const RESIDUALS: &[&str] = &[
    "rb.v <> ra.v",
    "rb.v > ra.v",
    "ra.v + rb.v > 5",
    "rb.v IS NULL OR rb.v <> ra.v",
    "rb.v <> ra.v AND rb.id <> ra.id",
];

/// `Some(())` when the pair matches: the residual must be TRUE, so a NULL
/// result rejects the pair.
fn residual_holds(sql: &str, a: &[Option<i64>; 3], b: &[Option<i64>; 3]) -> Option<bool> {
    let (av, bv) = (a[2], b[2]);
    Some(match sql {
        "rb.v <> ra.v" => bv? != av?,
        "rb.v > ra.v" => bv? > av?,
        "ra.v + rb.v > 5" => av? + bv? > 5,
        "rb.v IS NULL OR rb.v <> ra.v" => bv.is_none() || (bv? != av?),
        "rb.v <> ra.v AND rb.id <> ra.id" => bv? != av? && b[0]? != a[0]?,
        other => panic!("no reference for residual {other}"),
    })
}

fn key_match(a: &[Option<i64>; 3], b: &[Option<i64>; 3]) -> bool {
    match (a[1], b[1]) {
        // A NULL key never matches, on either side.
        (Some(x), Some(y)) => x == y,
        _ => false,
    }
}

fn reference_inner(ra: &[[Option<i64>; 3]], rb: &[[Option<i64>; 3]], res: &str) -> Vec<String> {
    let mut out = Vec::new();
    for a in ra {
        for b in rb {
            if key_match(a, b) && residual_holds(res, a, b) == Some(true) {
                out.push(format!("{}\t{}", literal(a[0]), literal(b[0])));
            }
        }
    }
    out
}

/// `v_eq` adds the body's second equality correlation (`rb.v = ra.v`).
fn reference_filter(
    ra: &[[Option<i64>; 3]],
    rb: &[[Option<i64>; 3]],
    res: &str,
    want: bool,
    v_eq: bool,
) -> Vec<String> {
    let mut out = Vec::new();
    for a in ra {
        let hit = rb.iter().any(|b| {
            key_match(a, b)
                && (!v_eq || matches!((a[2], b[2]), (Some(x), Some(y)) if x == y))
                && residual_holds(res, a, b) == Some(true)
        });
        if hit == want {
            out.push(literal(a[0]));
        }
    }
    out
}

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

fn sorted(mut rows: Vec<String>) -> Vec<String> {
    rows.sort();
    rows
}

/// Every shape that carries a residual over the same pair of tables.
fn cases(res: &str) -> Vec<(&'static str, String)> {
    let res = &format!("({res})");
    vec![
        (
            "inner",
            format!("SELECT ra.id, rb.id FROM ra INNER JOIN rb ON ra.k = rb.k AND {res}"),
        ),
        (
            "semi",
            format!(
                "SELECT ra.id FROM ra WHERE EXISTS (SELECT 1 FROM rb WHERE rb.k = ra.k AND {res})"
            ),
        ),
        (
            "anti",
            format!(
                "SELECT ra.id FROM ra WHERE NOT EXISTS (SELECT 1 FROM rb WHERE rb.k = ra.k AND {res})"
            ),
        ),
        (
            "semi-two-keys",
            format!(
                "SELECT ra.id FROM ra WHERE EXISTS \
                 (SELECT 1 FROM rb WHERE rb.k = ra.k AND rb.v = ra.v AND {res})"
            ),
        ),
        // Two subqueries sharing one outer key: both unnest against the same
        // duplicate-eliminated key side.
        (
            "two-subqueries",
            format!(
                "SELECT ra.id FROM ra WHERE EXISTS (SELECT 1 FROM rb WHERE rb.k = ra.k AND {res}) \
                 AND NOT EXISTS (SELECT 1 FROM rb WHERE rb.k = ra.k AND rb.v > ra.v)"
            ),
        ),
    ]
}

#[test]
fn residual_joins_match_nested_loop_and_duckdb() {
    let duck = Path::new(DUCKDB).exists();
    if !duck {
        eprintln!("skipping DuckDB reference: {DUCKDB} not present");
    }
    let seeds = std::env::var("W111_RESIDUAL_SEEDS")
        .ok()
        .map(|v| {
            v.split(',')
                .filter_map(|s| s.trim().parse::<u64>().ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![1, 2]);
    let cases_per_seed = std::env::var("W111_RESIDUAL_CASES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(12);
    let verbose = std::env::var("W111_RESIDUAL_VERBOSE").is_ok();
    let mut checked = 0usize;
    for seed in seeds {
        for case in 0..cases_per_seed {
            let mut rng = Rng::new(seed * 1000 + case as u64);
            let n = 1 + rng.below(10);
            let ra = gen_rows(&mut rng, n);
            let m = 1 + rng.below(10);
            let rb = gen_rows(&mut rng, m);
            let setup = setup_sql(&ra, &rb);
            let mut db = PinkDb::new();
            for stmt in setup.split(';') {
                if !stmt.trim().is_empty() {
                    db.execute(stmt).unwrap();
                }
            }
            for res in RESIDUALS {
                for (name, sql) in cases(res) {
                    if verbose {
                        eprintln!("seed {seed} case {case} {name}: {sql}");
                    }
                    let ours = match run_pinkdb(&mut db, &sql) {
                        Outcome::Rows(rows) => rows,
                        Outcome::Err(e) => {
                            panic!("{name} failed: {e}\n  sql: {sql}\n  data: {setup}")
                        }
                    };
                    let expect = if name == "two-subqueries" {
                        // Reference for the pair of subqueries, computed as the
                        // conjunction of the two unnestings.
                        let first = reference_filter(&ra, &rb, res, true, false);
                        let second = reference_filter(&ra, &rb, "rb.v > ra.v", false, false);
                        let ids: Vec<String> = ra
                            .iter()
                            .map(|r| literal(r[0]))
                            .filter(|id| first.contains(id) && second.contains(id))
                            .collect();
                        ids
                    } else if name == "semi-two-keys" {
                        reference_filter(&ra, &rb, res, true, true)
                    } else if name == "semi" {
                        reference_filter(&ra, &rb, res, true, false)
                    } else if name == "anti" {
                        reference_filter(&ra, &rb, res, false, false)
                    } else {
                        reference_inner(&ra, &rb, res)
                    };
                    assert_eq!(
                        sorted(ours.clone()),
                        sorted(expect),
                        "{name}: nested-loop reference differs\n  sql: {sql}\n  data: {setup}"
                    );
                    if duck {
                        match run_duckdb(&setup, &sql) {
                            Outcome::Rows(rows) => assert_eq!(
                                sorted(ours),
                                sorted(rows),
                                "{name}: DuckDB differs\n  sql: {sql}\n  data: {setup}"
                            ),
                            Outcome::Err(e) => panic!("{name}: DuckDB error: {e}\n  sql: {sql}"),
                        }
                    }
                    checked += 1;
                }
            }
        }
    }
    eprintln!("{checked} residual join cases matched");
    assert!(checked > 0);
}

#[test]
fn a_probe_chunk_with_more_candidate_pairs_than_a_chunk_holds() {
    // 2,048 probe rows that each match 4 build rows: 8,192 candidate pairs
    // in one probe chunk, which the residual used to gather into a single
    // vector (an overflow error past 2,048).
    let mut db = PinkDb::new();
    let a: Vec<String> = (0..2048).map(|i| format!("(1, {})", i % 3)).collect();
    for sql in [
        "CREATE TABLE a (k INTEGER, v INTEGER);".to_string(),
        format!("INSERT INTO a VALUES {};", a.join(",")),
        "CREATE TABLE b (k INTEGER, v INTEGER);".to_string(),
        "INSERT INTO b VALUES (1, 0), (1, 1), (1, 2), (1, 3);".to_string(),
    ] {
        db.execute(&sql).unwrap();
    }
    let count = |db: &mut PinkDb, sql: &str| match db.execute(sql).unwrap().pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => rows[0][0].clone(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    };
    // Each probe row equals exactly one build value, so 3 of its 4 pairs pass.
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM a JOIN b ON a.k = b.k AND a.v <> b.v"
        ),
        pinkdb::Value::Bigint(6144)
    );
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.v <> a.v)"
        ),
        pinkdb::Value::Bigint(2048)
    );
    assert_eq!(
        count(
            &mut db,
            "SELECT count(*) FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.v <> a.v)"
        ),
        pinkdb::Value::Bigint(0)
    );
}
