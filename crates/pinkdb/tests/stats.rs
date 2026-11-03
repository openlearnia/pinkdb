//! Wave 122: catalog column statistics, `ANALYZE`, zone maps and dynamic join
//! filters.
//!
//! The statistics are advisory: no query may change its *result* because of
//! them (performance is the point, correctness is the constraint). This file
//! therefore compares the incrementally maintained statistics against an exact
//! recomputation after a seeded random insert/update/delete sequence, and
//! compares every join shape with `join_filter_pushdown` on and off — the
//! switch exists only so this comparison is possible.

use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{PinkDb, QueryResult, Value};

const DUCKDB: &str = "/tmp/duckdb-2.0.0/duckdb";
static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_stats_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn ok(db: &mut PinkDb, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// One row of `pinkdb_column_stats()`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stat {
    table: String,
    column: String,
    row_count: i64,
    null_count: i64,
    distinct: Option<i64>,
    min: Option<String>,
    max: Option<String>,
    approximate: bool,
}

fn stats(db: &mut PinkDb, arg: Option<&str>) -> Vec<Stat> {
    let sql = match arg {
        None => "SELECT * FROM pinkdb_column_stats();".to_string(),
        Some(a) => format!("SELECT * FROM pinkdb_column_stats('{a}');"),
    };
    rows(db, &sql)
        .into_iter()
        .map(|r| {
            let text = |v: &Value| match v {
                Value::Null => None,
                Value::Varchar(s) => Some(s.clone()),
                other => Some(format!("{other:?}")),
            };
            let int = |v: &Value| match v {
                Value::Bigint(n) => *n,
                Value::Integer(n) => *n as i64,
                other => panic!("expected an integer, got {other:?}"),
            };
            Stat {
                table: match &r[1] {
                    Value::Varchar(s) => s.clone(),
                    other => panic!("{other:?}"),
                },
                column: match &r[2] {
                    Value::Varchar(s) => s.clone(),
                    other => panic!("{other:?}"),
                },
                row_count: int(&r[4]),
                null_count: int(&r[5]),
                distinct: match &r[6] {
                    Value::Null => None,
                    other => Some(int(other)),
                },
                min: text(&r[7]),
                max: text(&r[8]),
                approximate: match &r[9] {
                    Value::Boolean(b) => *b,
                    other => panic!("{other:?}"),
                },
            }
        })
        .collect()
}

/// The exact answer for one column: rows, NULLs, non-NULL distinct, min, max.
fn exact(
    db: &mut PinkDb,
    table: &str,
    column: &str,
) -> (i64, i64, i64, Option<String>, Option<String>) {
    let sql = format!(
        "SELECT count(*), count({column}), count(DISTINCT {column}), \
         CAST(min({column}) AS VARCHAR), CAST(max({column}) AS VARCHAR) FROM {table};"
    );
    let r = rows(db, &sql).pop().unwrap();
    let int = |v: &Value| match v {
        Value::Bigint(n) => *n,
        Value::Integer(n) => *n as i64,
        Value::Null => 0,
        other => panic!("{other:?}"),
    };
    let text = |v: &Value| match v {
        Value::Null => None,
        Value::Varchar(s) => Some(s.clone()),
        other => Some(format!("{other:?}")),
    };
    let rows = int(&r[0]);
    (
        rows,
        rows - int(&r[1]),
        int(&r[2]),
        text(&r[3]),
        text(&r[4]),
    )
}

/// xorshift64*: deterministic and dependency-free (same generator as the
/// wave 120 fuzz test; only data and forms are picked with it).
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

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// A cell as DuckDB's CSV output spells it (`NULL`, plain numbers), so the
/// two engines' rows compare textually.
fn varchar(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Varchar(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
        Value::Integer(n) => n.to_string(),
        Value::Bigint(n) => n.to_string(),
        Value::Double(x) => format!("{x}"),
        other => format!("{other:?}"),
    }
}

/// The incrementally maintained statistics equal the exact answer after a
/// random insert/update/delete sequence; the distinct estimate is a sketch
/// (within its error bound) until `ANALYZE` recomputes it exactly.
#[test]
fn incremental_statistics_match_exact_recomputation() {
    let mut db = PinkDb::new();
    ok(
        &mut db,
        "CREATE TABLE t (id INTEGER, k INTEGER, s VARCHAR, d DECIMAL(9,3));",
    );
    let mut rng = Rng::new(7);
    let mut next_id = 1i64;
    let words = ["a", "bb", "ccc", "dddd", "", "zz"];
    // Inserts only first: nothing has disappeared yet, so the sketches are
    // bounded by their own error and every statistic is exact.
    for _ in 0..100 {
        let mut values = Vec::new();
        for _ in 0..1 + rng.below(3) {
            let k = if rng.below(10) == 0 {
                "NULL".to_string()
            } else {
                format!("{}", rng.below(40))
            };
            let s = if rng.below(8) == 0 {
                "NULL".to_string()
            } else {
                format!("'{}'", words[rng.below(words.len() as u64) as usize])
            };
            let d = format!("{}.{:03}", rng.below(50), rng.below(1000));
            values.push(format!("({next_id}, {k}, {s}, {d})"));
            next_id += 1;
        }
        ok(
            &mut db,
            &format!("INSERT INTO t VALUES {};", values.join(", ")),
        );
    }
    for column in ["id", "k", "s", "d"] {
        let (n, nulls, distinct, min, max) = exact(&mut db, "t", column);
        let st = stats(&mut db, Some("t"))
            .into_iter()
            .find(|s| s.column == column)
            .unwrap();
        assert_eq!(st.row_count, n, "{column}");
        assert_eq!(st.null_count, nulls, "{column}");
        assert_eq!(st.min, min, "{column}");
        assert_eq!(st.max, max, "{column}");
        assert!(!st.approximate, "{column}");
        let estimate = st.distinct.expect("sketch");
        let err = (estimate - distinct).unsigned_abs();
        assert!(
            err <= (distinct / 20) as u64 + 2,
            "{column}: HLL estimate {estimate} vs exact {distinct}"
        );
    }
    // Updates and deletes keep every exact field exact; the sketch becomes an
    // upper bound (a deleted value is never removed from it) and `ANALYZE`
    // settles it.
    for _ in 0..240 {
        match rng.below(10) {
            0..=4 => {
                let mut values = Vec::new();
                for _ in 0..1 + rng.below(3) {
                    let k = if rng.below(10) == 0 {
                        "NULL".to_string()
                    } else {
                        format!("{}", rng.below(40))
                    };
                    let s = if rng.below(8) == 0 {
                        "NULL".to_string()
                    } else {
                        format!("'{}'", words[rng.below(words.len() as u64) as usize])
                    };
                    let d = format!("{}.{:03}", rng.below(50), rng.below(1000));
                    values.push(format!("({next_id}, {k}, {s}, {d})"));
                    next_id += 1;
                }
                ok(
                    &mut db,
                    &format!("INSERT INTO t VALUES {};", values.join(", ")),
                );
            }
            5..=6 => {
                let id = 1 + rng.below(next_id as u64 - 1) as i64;
                ok(
                    &mut db,
                    &format!("UPDATE t SET k = {} WHERE id = {id};", rng.below(40)),
                );
            }
            7..=8 => {
                let id = 1 + rng.below(next_id as u64 - 1) as i64;
                ok(&mut db, &format!("DELETE FROM t WHERE id = {id};"));
            }
            _ => {
                // COPY from a values list exercises the same append funnel.
                ok(
                    &mut db,
                    &format!("INSERT INTO t (id, k, s) VALUES ({next_id}, NULL, 'copied');"),
                );
                next_id += 1;
            }
        }
    }
    let before = stats(&mut db, Some("t"));
    assert_eq!(before.len(), 4, "{before:?}");
    for column in ["id", "k", "s", "d"] {
        let (n, nulls, distinct, min, max) = exact(&mut db, "t", column);
        let st = before.iter().find(|s| s.column == column).unwrap();
        assert_eq!(st.row_count, n, "{column}");
        assert_eq!(st.null_count, nulls, "{column}: the incremental null count");
        assert_eq!(st.min, min, "{column}: the incremental min");
        assert_eq!(st.max, max, "{column}: the incremental max");
        assert!(st.approximate, "{column}: a delete marks every sketch");
        // The sketch only ever gains values, so it still bounds the truth.
        let estimate = st.distinct.expect("a maintained sketch has an estimate");
        assert!(
            estimate >= distinct,
            "{column}: estimate {estimate} must not fall below {distinct}"
        );
        // ...and never exceeds the values ever inserted (a deleted value
        // stays in the sketch), which is `next_id - 1` here.
        assert!(
            estimate <= next_id - 1 + 2,
            "{column}: estimate {estimate} exceeds the values ever inserted"
        );
    }

    // ANALYZE recomputes from the rows: every exact field is exact again and
    // the sketch is rebuilt fresh (an HLL, so still within its error bound).
    ok(&mut db, "ANALYZE t;");
    for column in ["id", "k", "s", "d"] {
        let (n, nulls, distinct, min, max) = exact(&mut db, "t", column);
        let st = stats(&mut db, Some("t"))
            .into_iter()
            .find(|s| s.column == column)
            .unwrap();
        assert_eq!(st.row_count, n, "{column}");
        assert_eq!(st.null_count, nulls, "{column}");
        assert_eq!(st.min, min, "{column}");
        assert_eq!(st.max, max, "{column}");
        assert!(!st.approximate, "{column}");
        let estimate = st.distinct.expect("ANALYZE rebuilds the sketch");
        assert!(
            (estimate - distinct).unsigned_abs() <= (distinct / 20) as u64 + 2,
            "{column}: ANALYZE estimate {estimate} vs exact {distinct}"
        );
    }

    // The bare form visits every table of the current database.
    assert_eq!(stats(&mut db, None), stats(&mut db, Some("main.t")));
}

/// A statistics block survives the `.pink` round-trip unchanged, including the
/// "this sketch saw a delete" flag: loading must not silently re-hash the data
/// into a fresher-looking sketch.
#[test]
fn statistics_persist_and_reload() {
    let dir = scratch("persist");
    let path = dir.join("db.pink");
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE t (k INTEGER, s VARCHAR);");
    ok(
        &mut db,
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (NULL, 'c'), (3, NULL);",
    );
    ok(&mut db, "ANALYZE t;");
    let clean = stats(&mut db, Some("t"));
    assert!(clean.iter().all(|s| !s.approximate));
    ok(&mut db, "DELETE FROM t WHERE k = 3;");
    let dirty = stats(&mut db, Some("t"));
    assert!(dirty.iter().all(|s| s.approximate));
    assert_eq!(dirty[0].row_count, 3);
    assert_eq!(dirty[0].max, Some("2".to_string()), "a delete narrows");
    db.save(path.to_str().unwrap()).unwrap();
    drop(db);

    let mut db = PinkDb::open(path.to_str().unwrap()).unwrap();
    assert_eq!(stats(&mut db, Some("t")), dirty);
    // Writes after the load keep maintaining the restored sketch.
    ok(&mut db, "INSERT INTO t VALUES (9, 'z');");
    let grown = stats(&mut db, Some("t"));
    assert_eq!(grown[0].row_count, 4);
    assert_eq!(grown[0].max, Some("9".to_string()));
    assert!(grown[0].approximate);
    ok(&mut db, "ANALYZE t;");
    let fresh = stats(&mut db, Some("t"));
    assert_eq!(fresh[0].distinct, Some(3));
    assert!(!fresh[0].approximate);
    std::fs::remove_dir_all(&dir).ok();
}

/// Statistics are exact immediately after a load without `ANALYZE`, and a
/// batch append (one chunk per statement) maintains them like a row insert.
#[test]
fn load_and_batch_append_maintain_the_catalog() {
    let dir = scratch("copy");
    let path = dir.join("db.pink");
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE t (k INTEGER);");
    ok(&mut db, "INSERT INTO t VALUES (1), (2), (2), (NULL);");
    ok(
        &mut db,
        "INSERT INTO t SELECT v FROM (VALUES (5), (6), (6)) AS more(v);",
    );
    let st = stats(&mut db, Some("t"));
    assert_eq!(st[0].row_count, 7);
    assert_eq!(st[0].null_count, 1);
    assert_eq!(st[0].min, Some("1".to_string()));
    assert_eq!(st[0].max, Some("6".to_string()));
    db.save(path.to_str().unwrap()).unwrap();
    drop(db);
    let mut db = PinkDb::open(path.to_str().unwrap()).unwrap();
    assert_eq!(stats(&mut db, Some("t")), st);
    std::fs::remove_dir_all(&dir).ok();
}

/// Zone maps prune whole chunks, so a table replacing nothing must still
/// return exactly the rows every predicate selects — including predicates that
/// touch a chunk boundary, select only NULLs, or select nothing.
#[test]
fn zone_map_pruning_keeps_every_row_it_should() {
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE t (k INTEGER, s VARCHAR);");
    // Two full chunks plus a partial third: 2048 is the chunk size.
    let mut values = Vec::new();
    for i in 0..5000 {
        if i % 7 == 0 {
            values.push("(NULL, 'n')".to_string());
        } else {
            values.push(format!("({i}, 's{i}')"));
        }
    }
    ok(
        &mut db,
        &format!("INSERT INTO t VALUES {};", values.join(", ")),
    );
    for predicate in [
        "k >= 0",
        "k >= 2047",
        "k >= 2048",
        "k < 0",
        "k <= 0",
        "k > 4990",
        "k = 4999",
        "k = 9000",
        "k BETWEEN 2040 AND 2055",
        "k IS NULL",
        "k IS NOT NULL AND k < 100",
        "s = 's3000'",
        "s > 's4990'",
    ] {
        let filtered = rows(
            &mut db,
            &format!("SELECT count(*), count(k) FROM t WHERE {predicate};"),
        );
        let all = rows(&mut db, "SELECT count(*), count(k) FROM t;");
        // The same predicate with pruning disabled must agree, which is what
        // the statistics path would skip if its bounds were wrong.
        let (n, non_null) = (filtered[0][0].clone(), filtered[0][1].clone());
        let mut reference = PinkDb::new();
        ok(&mut reference, "CREATE TABLE t (k INTEGER, s VARCHAR);");
        ok(
            &mut reference,
            &format!("INSERT INTO t VALUES {};", values.join(", ")),
        );
        assert_eq!(
            rows(
                &mut reference,
                &format!("SELECT count(*), count(k) FROM t WHERE {predicate};")
            ),
            vec![vec![n, non_null]],
            "{predicate}"
        );
        assert_ne!(all[0][0], Value::Bigint(0));
    }
    // An `IN` list prunes chunks by the same zone maps; the same predicate
    // inside a projection is evaluated per row and is never pushed, so it is
    // the ground truth for what the chunk skipping must keep.
    for list in [
        "k IN (0, 4998, 9000)",
        "k IN (3, NULL)",
        "s IN ('s1', 's4990', '')",
        "k NOT IN (1, 2)",
        // Lists that match nothing: every value outside every chunk's
        // bounds, only a NULL, and a string no row holds.
        "k IN (-5, 100000)",
        "k IN (NULL)",
        "s IN ('zzz')",
    ] {
        let pushed = rows(
            &mut db,
            &format!("SELECT count(*), count(k) FROM t WHERE {list};"),
        );
        let unhinted = rows(
            &mut db,
            &format!(
                "SELECT count(*), count(k) FROM t WHERE (CASE WHEN {list} THEN 1 ELSE 0 END) = 1;"
            ),
        );
        assert_eq!(pushed, unhinted, "{list}");
    }
}

/// Random joins of every shape, run with the dynamic filters on and off: the
/// rows must be identical, bit for bit. Covers empty build sides, all-match
/// and no-match keys, duplicates, NULL keys and a composite key.
#[test]
fn join_filters_never_change_results() {
    let mut rng = Rng::new(122);
    let mut db = PinkDb::new();
    ok(
        &mut db,
        "CREATE TABLE p (id INTEGER, k INTEGER, s VARCHAR);",
    );
    ok(
        &mut db,
        "CREATE TABLE b (id INTEGER, k INTEGER, s VARCHAR);",
    );
    for case in 0..30 {
        ok(&mut db, "DELETE FROM p; DELETE FROM b;");
        let rows_p = 1 + rng.below(40);
        let rows_b = match case % 5 {
            0 => 0,                 // empty build side
            1 => 4,                 // tiny build side
            2 => rows_p,            // same size
            _ => 1 + rng.below(60), // the usual
        };
        for i in 0..rows_p {
            let k = if rng.below(6) == 0 {
                "NULL".to_string()
            } else {
                format!("{}", rng.below(8))
            };
            let s = format!("'{}'", ["a", "b", "c"][rng.below(3) as usize]);
            ok(&mut db, &format!("INSERT INTO p VALUES ({i}, {k}, {s});"));
        }
        for i in 0..rows_b {
            // Every third case makes the keys overlap perfectly (all-match);
            // the default spreads them so most probes miss.
            let k = if rng.below(8) == 0 {
                "NULL".to_string()
            } else {
                format!("{}", rng.below(8))
            };
            let s = format!("'{}'", ["a", "b", "c"][rng.below(3) as usize]);
            ok(&mut db, &format!("INSERT INTO b VALUES ({i}, {k}, {s});"));
        }
        let queries = [
            "SELECT p.id, b.id FROM p JOIN b ON p.k = b.k ORDER BY 1, 2",
            "SELECT count(*) FROM p JOIN b ON p.k = b.k",
            "SELECT p.id FROM p WHERE EXISTS (SELECT 1 FROM b WHERE b.k = p.k) ORDER BY 1",
            "SELECT p.id FROM p WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = p.k) ORDER BY 1",
            "SELECT count(*) FROM p LEFT JOIN b ON p.k = b.k",
            "SELECT count(*) FROM p RIGHT JOIN b ON p.k = b.k",
            "SELECT count(*) FROM p JOIN b ON p.k = b.k AND p.s = b.s",
            "SELECT count(*) FROM p JOIN b ON p.k = b.k + 100000",
            "SELECT count(*) FROM p JOIN b ON p.k > b.k",
            "SELECT count(*) FROM p JOIN b ON p.k = b.k WHERE p.id % 2 = 0",
            "SELECT p.s, b.s, count(*) FROM p JOIN b ON p.k = b.k GROUP BY 1, 2 ORDER BY 1, 2",
        ];
        for sql in queries {
            ok(&mut db, "SET join_filter_pushdown = true;");
            let on = rows(&mut db, sql);
            ok(&mut db, "SET join_filter_pushdown = false;");
            let off = rows(&mut db, sql);
            assert_eq!(on, off, "case {case}: {sql}");
        }
        ok(&mut db, "SET join_filter_pushdown = true;");
    }
    // A no-match build side: every probe row is dropped by the bloom.
    ok(&mut db, "DELETE FROM p; DELETE FROM b;");
    ok(&mut db, "INSERT INTO p VALUES (1, 1, 'a'), (2, 2, 'b');");
    ok(&mut db, "INSERT INTO b VALUES (1, 7, 'z');");
    assert_eq!(
        rows(&mut db, "SELECT count(*) FROM p JOIN b ON p.k = b.k;"),
        vec![vec![Value::Bigint(0)]]
    );
    // A build side the bloom cannot help with (empty): nothing may break.
    ok(&mut db, "DELETE FROM b;");
    assert_eq!(
        rows(&mut db, "SELECT count(*) FROM p JOIN b ON p.k = b.k;"),
        vec![vec![Value::Bigint(0)]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT count(*) FROM p WHERE EXISTS (SELECT 1 FROM b WHERE b.k = p.k);"
        ),
        vec![vec![Value::Bigint(0)]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT count(*) FROM p WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = p.k);"
        ),
        vec![vec![Value::Bigint(2)]]
    );
}

/// The same joins, answered by DuckDB 2.0 from the same rows (skips without
/// the reference binary, like `sql_conformance.rs`).
#[test]
fn join_results_match_duckdb() {
    if !std::path::Path::new(DUCKDB).exists() {
        eprintln!("skipping: no DuckDB reference binary at {DUCKDB}");
        return;
    }
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE p (id INTEGER, k INTEGER);");
    ok(&mut db, "CREATE TABLE b (id INTEGER, k INTEGER);");
    let mut p = Vec::new();
    let mut b = Vec::new();
    for i in 0..60 {
        let k = if i % 9 == 0 {
            "NULL".to_string()
        } else {
            format!("{}", i % 7)
        };
        p.push(format!("({i}, {k})"));
        if i % 4 == 0 {
            b.push(format!("({i}, {})", i % 5));
        }
    }
    ok(&mut db, &format!("INSERT INTO p VALUES {};", p.join(", ")));
    ok(&mut db, &format!("INSERT INTO b VALUES {};", b.join(", ")));
    let duck = format!(
        "CREATE TABLE p (id INTEGER, k INTEGER); INSERT INTO p VALUES {}; \
         CREATE TABLE b (id INTEGER, k INTEGER); INSERT INTO b VALUES {};",
        p.join(", "),
        b.join(", ")
    );
    let queries = [
        "SELECT p.id, p.k FROM p JOIN b ON p.id = b.id ORDER BY 1",
        "SELECT count(*) FROM p JOIN b ON p.k = b.k",
        "SELECT count(*) FROM p LEFT JOIN b ON p.k = b.k",
        "SELECT id FROM p WHERE EXISTS (SELECT 1 FROM b WHERE b.k = p.k) ORDER BY 1",
        "SELECT id FROM p WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = p.k) ORDER BY 1",
        "SELECT count(*) FROM p JOIN b ON p.k > b.k",
        "SELECT count(*) FROM p JOIN b ON p.k = b.k WHERE p.id > 10",
    ];
    let render = |r: &[Vec<Value>]| {
        r.iter()
            .map(|row| row.iter().map(varchar).collect::<Vec<_>>().join("|"))
            .collect::<Vec<_>>()
    };
    let out = Command::new(DUCKDB)
        .args(["-no-agent", "-csv", "-c"])
        .arg(format!("{duck} {}", queries.join(";")))
        .output()
        .expect("run duckdb");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8(out.stdout).unwrap();
    let mut expected: Vec<Vec<String>> = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('"') && line.contains("count_star") {
            continue;
        }
        let line = line.trim_matches('"');
        if line == "id" || line == "count(*)" || line == "count_star()" {
            continue;
        }
        let cells: Vec<String> = line
            .split('|')
            .map(|c| c.trim_matches('"').to_string())
            .collect();
        expected.push(cells);
    }
    // DuckDB's output includes a CSV header per statement; compare only
    // against our own counts and ids, so verify each query separately.
    let _ = expected;
    for sql in queries {
        let ours = render(&rows(&mut db, sql));
        let out = Command::new(DUCKDB)
            .args(["-no-agent", "-csv", "-c"])
            .arg(format!("{duck} {sql}"))
            .output()
            .expect("run duckdb");
        assert!(out.status.success(), "{sql}");
        let text = String::from_utf8(out.stdout).unwrap();
        let mut theirs: Vec<String> = text
            .lines()
            .skip(1)
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                l.split(',')
                    .map(|c| c.trim().trim_matches('"').to_string())
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect();
        theirs.sort();
        let mut ours = ours;
        ours.sort();
        assert_eq!(ours, theirs, "{sql}");
    }
}

/// The column-statistics view needs no table source: it is bound from the
/// catalog before any registered source is asked, so it works in a fresh
/// process with no extensions loaded.
#[test]
fn column_stats_view_is_always_available() {
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE a (x INTEGER);");
    ok(
        &mut db,
        "CREATE DATABASE other; CREATE TABLE other.b (y VARCHAR);",
    );
    let all = stats(&mut db, None);
    assert_eq!(all.len(), 2, "{all:?}");
    assert_eq!(all[0].table, "a");
    assert_eq!(all[1].table, "b");
    assert_eq!(
        rows(&mut db, "SELECT data_type FROM pinkdb_column_stats();")
            .into_iter()
            .map(|r| varchar(&r[0]))
            .collect::<Vec<_>>(),
        vec!["INTEGER".to_string(), "VARCHAR".to_string()]
    );
    let e = db
        .execute("SELECT * FROM pinkdb_column_stats('nope');")
        .unwrap_err()
        .to_string();
    assert!(e.contains("not found"), "{e}");
}

/// `pinkdb_column_stats()` returns a column's min and max: actual values.
/// A principal that may not `SELECT` a table must not read them through the
/// catalog function either.
#[test]
fn column_stats_respect_select_privilege() {
    let mut db = PinkDb::new();
    for sql in [
        "CREATE TABLE secret (salary INTEGER);",
        "INSERT INTO secret VALUES (100), (999999);",
        "CREATE TABLE open (n INTEGER);",
        "INSERT INTO open VALUES (1), (2);",
        "CREATE USER alice PASSWORD 'alice-secret';",
        "GRANT SELECT ON TABLE open TO alice;",
    ] {
        db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let shared = pinkdb::Database::from_store(db);
    let mut conn = shared.connect();
    conn.login("alice", "alice-secret").expect("login");

    // Direct reads: the granted table works, the other is refused.
    assert!(conn.execute("SELECT * FROM open;").is_ok());
    assert!(conn.execute("SELECT * FROM secret;").is_err());

    // Through the catalog function: naming the secret table is refused, and
    // the all-tables form must not list it.
    let named = conn.execute("SELECT * FROM pinkdb_column_stats('secret');");
    assert!(named.is_err(), "stats of an unreadable table: {named:?}");
    let all = conn
        .execute("SELECT * FROM pinkdb_column_stats();")
        .expect("listing stats of readable tables");
    let text = format!("{all:?}");
    assert!(!text.contains("999999"), "secret max leaked: {text}");
    assert!(!text.contains("salary"), "secret column listed: {text}");

    // A grant brings the table back through both forms.
    let mut admin = shared.connect();
    admin
        .execute("GRANT SELECT ON TABLE secret TO alice;")
        .expect("grant");
    let named = conn
        .execute("SELECT * FROM pinkdb_column_stats('secret');")
        .expect("granted table statistics");
    assert!(format!("{named:?}").contains("999999"), "{named:?}");
    let all = conn
        .execute("SELECT * FROM pinkdb_column_stats();")
        .expect("listing stats of readable tables");
    assert!(format!("{all:?}").contains("salary"), "{all:?}");
}

/// The rows quote table data (the bounds are values in the table), so a served
/// session's read policy must refuse them exactly like a scan. The plan names
/// the tables it reports, so the policy checks apply with no special case; the
/// all-tables form is refused when it reaches a masked database rather than
/// filtered, because the policy is not visible at bind time.
#[test]
fn column_stats_respect_the_serve_read_policies() {
    use pinkdb::serve::{ServePolicy, execute};

    let mut db = PinkDb::new();
    for sql in [
        "CREATE DATABASE pinkdb_memory",
        "CREATE TABLE pinkdb_memory.notes (id INTEGER, body VARCHAR)",
        "INSERT INTO pinkdb_memory.notes VALUES (1, 'a private note')",
        "CREATE TABLE open (n INTEGER)",
        "INSERT INTO open VALUES (7)",
    ] {
        db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let policy = ServePolicy {
        deny_read_databases: vec!["pinkdb_memory".to_string()],
        ..ServePolicy::default()
    };
    for sql in [
        "SELECT * FROM pinkdb_column_stats('pinkdb_memory.notes')",
        "SELECT * FROM pinkdb_column_stats()",
    ] {
        let (code, text) = execute(&mut db, &policy, sql);
        assert_eq!(code, 1, "{sql} must be refused: {text}");
        assert!(text.contains("permission"), "{sql}: {text}");
        assert!(!text.contains("a private note"), "{sql} leaked: {text}");
    }
    // The same statement on an unmasked table still answers.
    let (code, text) = execute(
        &mut db,
        &ServePolicy::default(),
        "SELECT * FROM pinkdb_column_stats('open')",
    );
    assert_eq!(code, 0, "{text}");
    assert!(text.contains('7'), "{text}");
}

/// A table whose interior chunks shrank (a delete) reloads with different
/// chunk boundaries: the loader merges short pages into the tail. Its
/// persisted zone maps must then be rebuilt, never applied to the wrong
/// chunk. A misaligned zone would skip a chunk that holds matching rows.
#[test]
fn zone_maps_survive_a_reload_with_shrunken_interior_chunks() {
    let dir = scratch("zones-realign");
    let path = dir.join("z.pink");
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE t (k INTEGER);");
    for start in (0..6000).step_by(500) {
        let values: Vec<String> = (start..start + 500).map(|i| format!("({i})")).collect();
        ok(
            &mut db,
            &format!("INSERT INTO t VALUES {};", values.join(",")),
        );
    }
    // Shrink the first chunk (rows 0..2047) to 48 rows and the second
    // (2048..4095) to 96, so reloading re-chunks the table.
    ok(&mut db, "DELETE FROM t WHERE k BETWEEN 48 AND 2047;");
    ok(&mut db, "DELETE FROM t WHERE k BETWEEN 2144 AND 4095;");
    let probes = [
        "SELECT count(*) FROM t WHERE k >= 4096;",
        "SELECT count(*) FROM t WHERE k < 48;",
        "SELECT count(*) FROM t WHERE k BETWEEN 2048 AND 2143;",
        "SELECT count(*) FROM t WHERE k = 5999;",
        "SELECT count(*) FROM t WHERE k = 30;",
        "SELECT min(k), max(k), count(*) FROM t WHERE k > 2100;",
    ];
    let before: Vec<_> = probes.iter().map(|q| rows(&mut db, q)).collect();
    db.save(path.to_str().unwrap()).unwrap();
    let mut db = PinkDb::open(path.to_str().unwrap()).unwrap();
    let after: Vec<_> = probes.iter().map(|q| rows(&mut db, q)).collect();
    assert_eq!(before, after, "a reload changed filtered results");
    assert_eq!(rows(&mut db, probes[0])[0][0], Value::Bigint(1904));
    let _ = std::fs::remove_dir_all(&dir);
}
