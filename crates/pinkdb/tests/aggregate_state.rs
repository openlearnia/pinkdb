//! A GROUP BY's state is O(groups + chunks in flight), not O(input rows).
//!
//! The pipeline folds a chunk at a time, so a shape whose key makes every row
//! of a chunk its own group holds one entry per row in that chunk's partial.
//! Kept per chunk, that is a state that grows with the input; the dense lane
//! ([`pinkdb::exec::pipeline`]) merges each chunk's partial into one table
//! instead, so the same groups cost the same regardless of how many rows
//! produced them.
//!
//! The rows live in a table rather than in `range()` so the materialized
//! source cannot mask the aggregate's own state.

use pinkdb::PinkDb;
use pinkdb::memory;
use std::sync::MutexGuard;

/// Rows of the small input; the large one is eight times this at the same
/// group count. Above the pipeline's `DENSE_MIN_ROWS` (131,072): smaller inputs
/// are never sampled, since their state is bounded by the input itself.
const ROWS: i64 = 140_000;

/// What one group may hold. A group row is its key words, hash, index slot and
/// accumulator slots plus its VARCHAR key bytes — 400 B here at the widest,
/// with room to spare over the arena's copy.
const MAX_PER_GROUP: u64 = 1024;

/// What a run's window of chunk partials and the allocator may add, whatever
/// the row count.
const SLACK: u64 = 4 << 20;

/// One GROUP BY shape and how many groups it makes of [`ROWS`] rows.
struct Shape {
    name: &'static str,
    sql: &'static str,
    groups: u64,
}

const SHAPES: [Shape; 9] = [
    Shape {
        name: "two VARCHAR keys",
        sql: "SELECT a, b, sum(n), avg(n) FROM t GROUP BY 1, 2",
        groups: 20_000,
    },
    Shape {
        name: "VARCHAR and integer",
        sql: "SELECT a, k, count(*), sum(n) FROM t GROUP BY 1, 2",
        groups: 20_000,
    },
    Shape {
        name: "three mixed keys",
        sql: "SELECT a, b, k, count(*), sum(n) FROM t GROUP BY 1, 2, 3",
        groups: 20_000,
    },
    Shape {
        name: "DATE and VARCHAR",
        sql: "SELECT d, b, count(*), sum(n) FROM t GROUP BY 1, 2",
        groups: 20_000,
    },
    Shape {
        name: "TIMESTAMP and VARCHAR",
        sql: "SELECT ts, a, count(*), avg(n) FROM t GROUP BY 1, 2",
        groups: 1_000,
    },
    Shape {
        name: "DISTINCT over a VARCHAR group",
        sql: "SELECT a, b, count(DISTINCT x) FROM t GROUP BY 1, 2",
        groups: 20_000,
    },
    Shape {
        name: "DISTINCT without GROUP BY",
        sql: "SELECT count(DISTINCT x) FROM t",
        groups: 1,
    },
    Shape {
        name: "VARCHAR keys and HAVING",
        sql: "SELECT a, b, count(*) FROM t GROUP BY 1, 2 HAVING count(*) > 1",
        groups: 20_000,
    },
    Shape {
        name: "ROLLUP over VARCHAR keys",
        sql: "SELECT a, b, count(*) FROM t GROUP BY ROLLUP(a, b)",
        groups: 21_001,
    },
];

/// A database of `rows` rows, held under the memory tests' serial lock.
struct Db {
    db: PinkDb,
    _lock: MutexGuard<'static, ()>,
}

impl Db {
    fn new(rows: i64) -> Db {
        let lock = memory::test_lock();
        let mut db = PinkDb::new();
        db.execute(&format!(
            "CREATE TABLE t AS SELECT \
               'zone_' || ((r % 1000)::VARCHAR) AS a, \
               'do_' || ((((r / 1000)::BIGINT) % 20)::VARCHAR) AS b, \
               ((r / 1000)::BIGINT) % 20 AS k, \
               (r % 97)::BIGINT AS n, \
               DATE '2024-01-01' + (r % 1000)::INTEGER AS d, \
               CAST('2024-01-01 00:00:0' || ((r % 10)::VARCHAR) AS TIMESTAMP) AS ts, \
               (((r % 1000) * 20) + (((r / 1000)::BIGINT) % 20))::BIGINT AS x \
             FROM range(0, {rows}) t(r)"
        ))
        .expect("create the input");
        Db { db, _lock: lock }
    }

    /// The statement's memory peak.
    fn peak(&mut self, sql: &str) -> u64 {
        let mut out = self.db.execute_profiled(sql).expect("run the aggregate");
        let (_, profile) = out.pop().expect("a profile");
        profile.memory_peak
    }

    fn count(&mut self, sql: &str) -> u64 {
        let out = self
            .db
            .execute(&format!("SELECT count(*) FROM ({sql}) s"))
            .expect("count the groups");
        let pinkdb::QueryResult::Selected { rows, .. } = &out[0] else {
            panic!("count did not select")
        };
        match &rows[0][0] {
            pinkdb::Value::Bigint(n) => *n as u64,
            other => panic!("count came back as {other:?}"),
        }
    }
}

fn peaks(sizes: [i64; 2]) -> [Vec<u64>; 2] {
    let mut peaks: [Vec<u64>; 2] = [Vec::new(), Vec::new()];
    for (i, rows) in sizes.into_iter().enumerate() {
        let mut db = Db::new(rows);
        for shape in &SHAPES {
            assert_eq!(
                db.count(shape.sql),
                shape.groups,
                "{} at {rows} rows",
                shape.name
            );
            peaks[i].push(db.peak(shape.sql));
        }
    }
    peaks
}

#[test]
#[ignore = "dense lane disabled until 136c-0b, see ROADMAP"]
fn group_state_is_bounded_by_groups_not_rows() {
    let [small, large] = peaks([ROWS, ROWS * 8]);
    for (i, shape) in SHAPES.iter().enumerate() {
        let (small, large) = (small[i], large[i]);
        assert!(
            small <= shape.groups * MAX_PER_GROUP + SLACK,
            "{}: {small} bytes for {} groups",
            shape.name,
            shape.groups
        );
        // Eight times the rows at the same groups may not cost more than this
        // run's window of chunks and allocator noise.
        assert!(
            large <= small + (small / 4).max(1 << 20),
            "{}: eight times the rows took the peak from {small} to {large} bytes",
            shape.name
        );
    }
}

/// The dense lane must not change what a small aggregate returns: the same
/// rows in the same order whether the pipeline runs or not.
#[test]
fn a_small_group_set_returns_the_same_rows_either_way() {
    let mut db = Db::new(ROWS);
    for sql in [
        "SELECT a, sum(n), avg(n) FROM t GROUP BY 1 ORDER BY 1",
        "SELECT b, count(*), sum(n) FROM t GROUP BY 1 ORDER BY 1",
        "SELECT a, b, count(DISTINCT x) FROM t GROUP BY 1, 2 ORDER BY 1, 2",
    ] {
        let pipelined = db.db.execute(sql).expect("pipelined");
        db.db
            .execute("SET pipeline_execution = false")
            .expect("turn the pipeline off");
        let materialized = db.db.execute(sql).expect("materialized");
        db.db
            .execute("SET pipeline_execution = true")
            .expect("turn the pipeline on");
        let (
            pinkdb::QueryResult::Selected { rows: a, .. },
            pinkdb::QueryResult::Selected { rows: b, .. },
        ) = (&pipelined[0], &materialized[0])
        else {
            panic!("{sql} did not select on both paths")
        };
        assert_eq!(a, b, "{sql}");
    }
}
