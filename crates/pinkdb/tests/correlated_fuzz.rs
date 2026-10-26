//! Randomized differential test for correlated subqueries (wave 120).
//!
//! The pinned corpus in `correlated_subqueries.rs` covers the forms by hand;
//! this generator covers the spaces between them — random data with NULLs and
//! duplicate keys, correlation on one column or two, equality and range
//! correlation, the operand either equal to or different from the correlation
//! column, and every supported form in `WHERE`, the select list and `HAVING`.
//! Each case is compared against DuckDB 2.0, so a shape that is merely
//! *plausible* (rather than what a real engine answers) cannot pass.
//!
//! Seeded and deterministic: the default run is [`SEEDS`] × [`CASES`] cases.
//! `W120_FUZZ_SEEDS` (`1,2,3`) and `W120_FUZZ_CASES` (`100`) widen that for a
//! soak; `W120_FUZZ_VERBOSE=1` prints every case. Without the DuckDB reference
//! binary the test skips, like `sql_conformance.rs`.

use std::path::Path;
use std::process::{Command, Stdio};

use pinkdb::{PinkDb, QueryResult, Value};

const DUCKDB: &str = "/tmp/duckdb-2.0.0/duckdb";
const SEEDS: &[u64] = &[1, 2, 3, 4, 5];
const CASES: usize = 100;

/// xorshift64*: deterministic and dependency-free. The stream is only used to
/// pick data and forms, so its statistical quality needs to be no better than
/// this.
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

    fn pick<'a>(&mut self, xs: &'a [&'a str]) -> &'a str {
        xs[self.below(xs.len())]
    }

    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
}

struct Table {
    name: &'static str,
    cols: &'static [&'static str],
    rows: Vec<Vec<Option<i64>>>,
}

fn gen_tables(rng: &mut Rng) -> Vec<Table> {
    let specs: [(&'static str, &'static [&'static str]); 3] = [
        ("fa", &["a", "b", "c"]),
        ("fb", &["d", "e", "f"]),
        ("fc", &["g", "h"]),
    ];
    specs
        .iter()
        .map(|(name, cols)| {
            let n = rng.below(9);
            let rows = (0..n)
                .map(|_| {
                    cols.iter()
                        // A small value range makes duplicate keys and empty
                        // correlation groups common; NULLs exercise both the
                        // three-valued rules and the missing-group rules.
                        .map(|_| {
                            if rng.chance(15) {
                                None
                            } else {
                                Some(rng.below(5) as i64)
                            }
                        })
                        .collect()
                })
                .collect();
            Table { name, cols, rows }
        })
        .collect()
}

fn setup_sql(tables: &[Table]) -> String {
    let mut sql = String::new();
    for t in tables {
        sql.push_str(&format!("CREATE TABLE {} (", t.name));
        for (i, c) in t.cols.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("{c} INTEGER"));
        }
        sql.push_str("); ");
        for row in &t.rows {
            sql.push_str(&format!("INSERT INTO {} VALUES (", t.name));
            for (i, v) in row.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&v.map(|v| v.to_string()).unwrap_or("NULL".into()));
            }
            sql.push_str("); ");
        }
    }
    sql
}

/// A predicate over `fb` correlated to the outer `fa` row, and whether it
/// mentions an outer column at all (an uncorrelated body must still work).
fn body_pred(rng: &mut Rng) -> String {
    let mut pred = match rng.below(7) {
        0 => "fb.d = fa.a".to_string(),
        1 => "fb.e = fa.b".to_string(),
        2 => "fb.d < fa.b".to_string(),
        3 => "fb.e BETWEEN fa.a AND fa.c".to_string(),
        4 => "fb.d = fa.a AND fb.e = fa.b".to_string(),
        5 => "fb.f = fa.c".to_string(),
        _ => "fb.d = fa.a + 1".to_string(),
    };
    if rng.chance(30) {
        pred.push_str(&format!(" AND fb.e > {}", rng.below(4)));
    }
    if rng.chance(5) {
        // The body can be empty even for a matching key: `IN` must then be
        // false and `NOT IN` true regardless of the operand.
        pred.push_str(" AND 1 = 0");
    }
    pred
}

fn set_body(rng: &mut Rng) -> String {
    let col = rng.pick(&["fb.d", "fb.e", "fb.f"]);
    format!("SELECT {col} FROM fb WHERE {}", body_pred(rng))
}

fn scalar_body(rng: &mut Rng) -> String {
    // Aggregate-only: a plain column could return more than one row, and
    // DuckDB raises that error only in *some* plan shapes (`SELECT (subq)`
    // errors, `a < (subq)` quietly keeps the first row), so it cannot be
    // compared case by case. `correlated_subqueries.rs` pins that error and
    // DuckDB's message.
    let agg = rng.pick(&[
        "count(*)",
        "count(fb.e)",
        "sum(fb.e)",
        "max(fb.d)",
        "min(fb.e)",
    ]);
    format!("SELECT {agg} FROM fb WHERE {}", body_pred(rng))
}

/// One correlated predicate over `fa`.
fn predicate(rng: &mut Rng) -> String {
    match rng.below(11) {
        0 => format!("EXISTS ({})", set_body(rng)),
        1 => format!("NOT EXISTS ({})", set_body(rng)),
        2 => format!(
            "{} {}IN ({})",
            rng.pick(&["fa.a", "fa.b", "fa.c", "fa.a + 1"]),
            if rng.chance(50) { "" } else { "NOT " },
            set_body(rng)
        ),
        3 => format!(
            "{} {} {} ({})",
            rng.pick(&["fa.a", "fa.b", "fa.c"]),
            rng.pick(&["=", "<>", "<", "<=", ">", ">="]),
            rng.pick(&["ANY", "SOME", "ALL"]),
            set_body(rng)
        ),
        4 | 5 => format!(
            "{} {} ({})",
            rng.pick(&["fa.a", "fa.b", "fa.c", "fa.a + fa.b"]),
            rng.pick(&["=", "<>", "<", "<=", ">", ">="]),
            scalar_body(rng)
        ),
        6 => format!(
            "CASE WHEN {} THEN 1 ELSE 0 END = {}",
            predicate(rng),
            rng.below(2)
        ),
        7 => format!("({}) OR ({})", predicate(rng), predicate(rng)),
        8 => format!("NOT ({})", predicate(rng)),
        9 => "EXISTS (SELECT 1 FROM fb WHERE fb.d = fa.a AND EXISTS \
              (SELECT 1 FROM fc WHERE fc.g = fb.e AND fc.h = fa.b))"
            .to_string(),
        _ => "EXISTS (SELECT 1 FROM (SELECT fb.d AS dd FROM fb WHERE fb.e = fa.b) z \
              WHERE z.dd = fa.a)"
            .to_string(),
    }
}

fn case_sql(rng: &mut Rng) -> String {
    match rng.below(8) {
        0 => format!("SELECT * FROM fa WHERE {}", predicate(rng)),
        1 => format!("SELECT fa.a, ({}) AS p FROM fa", predicate(rng)),
        2 => format!(
            "SELECT fa.a, CASE WHEN {} THEN 1 ELSE 0 END AS c FROM fa",
            predicate(rng)
        ),
        // A correlated aggregate in HAVING, over the group key itself and over
        // a column that is not grouped.
        3 => format!(
            "SELECT fa.a, count(*) AS n FROM fa GROUP BY fa.a HAVING count(*) < ({})",
            scalar_body(rng)
        ),
        4 => format!(
            "SELECT fa.a, count(*) AS n FROM fa GROUP BY fa.a HAVING {}",
            predicate(rng)
        ),
        5 => format!(
            "SELECT fa.a, ({}) AS p FROM fa WHERE fa.b IS NOT NULL",
            predicate(rng)
        ),
        // Two independent subqueries over the same block, one of which is the
        // projection and one the filter.
        6 => format!(
            "SELECT fa.a, ({}) AS p FROM fa WHERE {}",
            predicate(rng),
            predicate(rng)
        ),
        _ => format!("SELECT * FROM fa WHERE {} ORDER BY fa.a", predicate(rng)),
    }
}

enum Outcome {
    Rows(Vec<String>),
    Err(String),
}

impl Outcome {
    /// `MultipleRows` is the one error a generated case can legitimately hit,
    /// and both engines must agree that it is *that* error.
    fn class(&self) -> &'static str {
        match self {
            Outcome::Rows(_) => "rows",
            Outcome::Err(m) => {
                if m.to_lowercase().contains("more than one row") {
                    "multiple rows"
                } else {
                    "error"
                }
            }
        }
    }
}

fn cell(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Boolean(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Bigint(i) => i.to_string(),
        other => format!("{other:?}"),
    }
}

fn run_pinkdb(db: &mut PinkDb, sql: &str) -> Outcome {
    match db.execute(sql) {
        Err(e) => Outcome::Err(e.to_string()),
        Ok(mut results) => match results.pop() {
            Some(QueryResult::Selected { rows, .. }) => Outcome::Rows(
                rows.iter()
                    .map(|r| r.iter().map(cell).collect::<Vec<_>>().join("\t"))
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

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_seeds() -> Vec<u64> {
    match std::env::var("W120_FUZZ_SEEDS") {
        Ok(v) => v
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect::<Vec<u64>>(),
        Err(_) => SEEDS.to_vec(),
    }
}

#[test]
fn generated_cases_match_duckdb() {
    if !Path::new(DUCKDB).exists() {
        eprintln!("skipping: {DUCKDB} not present");
        return;
    }
    let cases = env_usize("W120_FUZZ_CASES", CASES);
    let verbose = std::env::var("W120_FUZZ_VERBOSE").is_ok();
    let mut total = 0usize;
    let mut errored = 0usize;
    for seed in env_seeds() {
        let mut rng = Rng::new(seed);
        let tables = gen_tables(&mut rng);
        let setup = setup_sql(&tables);
        let mut db = PinkDb::new();
        for stmt in setup.split(';') {
            if !stmt.trim().is_empty() {
                db.execute(stmt).unwrap();
            }
        }
        for case in 0..cases {
            let sql = case_sql(&mut rng);
            let ours = run_pinkdb(&mut db, &sql);
            let reference = run_duckdb(&setup, &sql);
            if verbose {
                eprintln!("seed {seed} case {case}: {sql}");
            }
            let same = match (&ours, &reference) {
                (Outcome::Rows(a), Outcome::Rows(b)) => {
                    let mut a = a.clone();
                    let mut b = b.clone();
                    a.sort();
                    b.sort();
                    a == b
                }
                (Outcome::Err(a), Outcome::Err(b)) => {
                    assert_eq!(
                        ours.class(),
                        reference.class(),
                        "seed {seed} case {case}: error kinds differ\n  sql: {sql}\n  \
                         pinkdb: {a}\n  duckdb: {b}\n  data: {setup}"
                    );
                    true
                }
                _ => false,
            };
            if matches!(reference, Outcome::Err(_)) {
                errored += 1;
            }
            assert!(
                same,
                "seed {seed} case {case}: results differ\n  sql: {sql}\n  \
                 pinkdb: {:?}\n  duckdb: {:?}\n  data: {setup}",
                describe(&ours),
                describe(&reference)
            );
            total += 1;
        }
    }
    eprintln!("{total} generated cases matched ({errored} expected errors)");
    assert!(total > 0, "no cases generated");
}

fn describe(o: &Outcome) -> String {
    match o {
        Outcome::Rows(rows) => {
            let mut rows = rows.clone();
            rows.sort();
            format!("{} rows {:?}", rows.len(), rows)
        }
        Outcome::Err(m) => format!("error {m:?}"),
    }
}
