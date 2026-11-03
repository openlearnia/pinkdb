//! Wave 111ab regression: a subquery in a projection, `CASE`, aggregate or
//! join condition must not deadlock the parallel executor.
//!
//! The first version memoised an uncorrelated subquery behind a per-key lock
//! taken *across* its evaluation. The body runs on the same worker pool, so a
//! worker that waited on that lock could be holding the pool job the evaluator
//! needed: nine workers parked in the pool's wait and nothing left to finish.
//! The fix primes every uncorrelated subquery before the chunks fan out, so the
//! per-chunk path only reads. These shapes (and 30 repetitions of each) are the
//! ones that hung; a watchdog fails the test instead of hanging CI.

use pinkdb::{PinkDb, QueryResult, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Rows in `t1`. Chunked execution needs many chunks: at 2048 rows a chunk
/// this is ~490 of them, and the subquery is shared by every one.
const ROWS: i64 = 1_000_000;
/// The repro hung in about one run in six, so thirty rounds of each shape make
/// a miss very unlikely; a hang is what the watchdog catches, not a wrong
/// answer.
const ROUNDS: usize = 30;
/// No query may take this long; the watchdog aborts the process past it.
const LIMIT: Duration = Duration::from_secs(20);

fn db() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t1 (k INTEGER, g INTEGER);")
        .unwrap();
    db.execute("CREATE TABLE t2 (k INTEGER);").unwrap();
    for start in (0..ROWS).step_by(5_000) {
        let rows: Vec<String> = (start..(start + 5_000).min(ROWS))
            .map(|k| format!("({k},{})", k % 1000))
            .collect();
        db.execute(&format!("INSERT INTO t1 VALUES {};", rows.join(",")))
            .unwrap();
    }
    let rows: Vec<String> = (0..ROWS).step_by(7).map(|k| format!("({k})")).collect();
    for part in rows.chunks(5_000) {
        db.execute(&format!("INSERT INTO t2 VALUES {};", part.join(",")))
            .unwrap();
    }
    db
}

/// One cell as text, so two query shapes compare by value and not by the Rust
/// type that carries it (`count(*)` is BIGINT, a `sum` over it is HUGEINT).
fn cell(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Integer(i) => i.to_string(),
        Value::Bigint(i) => i.to_string(),
        Value::Hugeint(i) => i.to_string(),
        other => format!("{other:?}"),
    }
}

/// Every result row, formatted, so two query shapes can be compared without
/// knowing their column types.
fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<String>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => {
            rows.iter().map(|r| r.iter().map(cell).collect()).collect()
        }
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn scalar(db: &mut PinkDb, sql: &str) -> i64 {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => match &rows[0][0] {
            Value::Bigint(v) => *v,
            Value::Integer(v) => *v as i64,
            // `sum(...)` over an integer column is HUGEINT; these pins are small.
            Value::Hugeint(v) => i64::try_from(*v).expect("fits in i64"),
            other => panic!("expected an integer count from {sql}, got {other:?}"),
        },
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

#[test]
fn subqueries_outside_filters_do_not_deadlock() {
    let mut db = db();
    let progress = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let watch = {
        let (progress, done) = (progress.clone(), done.clone());
        thread::spawn(move || {
            let mut last = 0;
            let mut since = Instant::now();
            while !done.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(100));
                let now = progress.load(Ordering::Relaxed);
                if now != last {
                    last = now;
                    since = Instant::now();
                } else if since.elapsed() > LIMIT {
                    eprintln!(
                        "subquery_parallel: no query finished for {} s — the executor \
                         is deadlocked (see this file's header)",
                        LIMIT.as_secs()
                    );
                    std::process::abort();
                }
            }
        })
    };

    // The five shapes from the repro: the same shared `t2` set reached from a
    // derived table, a `CASE`, a filter, an aggregate argument and a join
    // condition. Each is pinned against the equivalent join.
    let cases: [(&str, &str); 5] = [
        (
            "SELECT count(*) FROM (SELECT k IN (SELECT k FROM t2) AS f FROM t1) s WHERE f",
            "SELECT count(*) FROM t1 JOIN t2 ON t1.k = t2.k",
        ),
        (
            "SELECT sum(CASE WHEN k IN (SELECT k FROM t2) THEN 1 ELSE 0 END) FROM t1",
            "SELECT count(*) FROM t1 JOIN t2 ON t1.k = t2.k",
        ),
        (
            "SELECT g, count(*) FROM t1 WHERE k IN (SELECT k FROM t2) GROUP BY g ORDER BY g",
            "SELECT g, count(*) FROM t1 JOIN t2 ON t1.k = t2.k GROUP BY g ORDER BY g",
        ),
        (
            "SELECT max(k + (SELECT max(k) FROM t2)) FROM t1",
            "SELECT max(k) + max(k) FROM t1",
        ),
        (
            "SELECT count(*) FROM t1 JOIN t2 ON t1.k = t2.k \
             AND t1.g < (SELECT avg(g) FROM t1)",
            "SELECT count(*) FROM t1 JOIN t2 ON t1.k = t2.k AND t1.g <= 499",
        ),
    ];
    let expected: Vec<Vec<Vec<String>>> = cases
        .iter()
        .map(|(_, plain)| rows(&mut db, plain))
        .collect();
    // Independent pins, so a join form that agrees with a wrong answer cannot
    // hide a wrong answer.
    assert_eq!(
        scalar(&mut db, "SELECT count(*) FROM t1 WHERE k % 7 = 0"),
        142_858
    );
    assert_eq!(
        scalar(
            &mut db,
            "SELECT count(*) FROM t1 WHERE k % 7 = 0 AND g <= 499"
        ),
        scalar(&mut db, cases[4].1)
    );
    assert_eq!(
        scalar(&mut db, "SELECT count(*) FROM t1 WHERE k % 7 = 0"),
        scalar(&mut db, cases[0].1)
    );

    for round in 0..ROUNDS {
        for (i, (subquery, _)) in cases.iter().enumerate() {
            let got = rows(&mut db, subquery);
            assert_eq!(got, expected[i], "round {round} shape {i}: `{subquery}`");
            progress.fetch_add(1, Ordering::Relaxed);
        }
    }
    done.store(true, Ordering::Relaxed);
    watch.join().unwrap();

    assert_eq!(scalar(&mut db, cases[1].0), 142_858);
    assert_eq!(scalar(&mut db, cases[3].0), 1_999_998);
}
