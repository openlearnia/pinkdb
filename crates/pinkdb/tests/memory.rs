//! Wave 136a: the process-wide memory pool, `SET memory_limit`, and the
//! operators that reserve from it.
//!
//! The limit is process-wide, so every test here holds [`memory::test_lock`]
//! for its whole run: a tiny limit must never reach a concurrent test.

use std::sync::MutexGuard;

use pinkdb::memory;
use pinkdb::{PinkDb, QueryResult, Value};

/// A `PinkDb` and the process-wide limit lock, restored to the default when the
/// test ends.
struct Pool {
    db: PinkDb,
    _lock: MutexGuard<'static, ()>,
}

impl Pool {
    fn new() -> Pool {
        let lock = memory::test_lock();
        Pool {
            db: PinkDb::new(),
            _lock: lock,
        }
    }

    /// Bound the pool, restoring the default on drop (via `Pool::drop`).
    fn limit(&mut self, limit: &str) {
        self.exec(&format!("SET memory_limit = '{limit}'"));
    }

    fn exec(&mut self, sql: &str) -> QueryResult {
        self.db
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .pop()
            .expect("one statement")
    }

    fn text(&mut self, sql: &str) -> String {
        match self.exec(sql) {
            QueryResult::Selected { rows, .. } => match rows.first().map(|r| &r[0]) {
                Some(Value::Varchar(s)) => s.clone(),
                other => panic!("{sql}: expected a VARCHAR, got {other:?}"),
            },
            other => panic!("{sql}: expected rows, got {other:?}"),
        }
    }

    fn err(&mut self, sql: &str) -> String {
        self.db
            .execute(sql)
            .expect_err(&format!("{sql} should have failed"))
            .to_string()
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        memory::reset_limit();
        memory::reset_cache_limit();
    }
}

#[test]
fn set_and_reset_memory_limit_read_back_like_duckdb() {
    let mut p = Pool::new();
    p.limit("2GB");
    assert_eq!(p.text("SELECT current_setting('memory_limit')"), "1.8 GiB");
    p.limit("512MB");
    assert_eq!(
        p.text("SELECT current_setting('memory_limit')"),
        "488.2 MiB"
    );
    p.limit("1.5GiB");
    assert_eq!(p.text("SELECT current_setting('memory_limit')"), "1.5 GiB");
    p.limit("-1");
    assert_eq!(
        p.text("SELECT current_setting('memory_limit')"),
        "16383.9 PiB"
    );
    p.exec("RESET memory_limit");
    assert_eq!(
        p.text("SELECT current_setting('memory_limit')"),
        memory::human_bytes(memory::default_limit())
    );
    // The alias is the same setting, as in DuckDB.
    p.exec("SET max_memory = '1GB'");
    assert_eq!(
        p.text("SELECT current_setting('memory_limit')"),
        "953.6 MiB"
    );
}

#[test]
fn invalid_limits_are_rejected_with_duckdb_wording() {
    let mut p = Pool::new();
    for (value, expected) in [
        (
            "'1GBX'",
            "Unknown unit for memory: 'gbx' (expected: KB, MB, GB, TB for 1000^i units or KiB, MiB, GiB, TiB for 1024^i units)",
        ),
        (
            "'GB'",
            "Memory limit must have a number (e.g. SET memory_limit=1GB",
        ),
        // A bare number is not a size in the DuckDB build pinkdb matches.
        ("'1024'", "Unknown unit for memory: ''"),
        ("'150%'", "Unable to parse valid percentage (input: 150%)"),
        ("123", "memory_limit takes a string like '2GB', not 123"),
    ] {
        // `SET 1024` would be an integer literal, so the bare-number case is
        // written as a string above.
        let sql = format!("SET memory_limit = {value}");
        let err = p.err(&sql);
        assert!(err.contains(expected), "{sql}: {err}");
    }
    let err = p.err("SELECT current_setting('no_such_setting')");
    assert!(
        err.contains("unrecognized configuration parameter \"no_such_setting\""),
        "{err}"
    );
    let err = p.err("SELECT current_setting(42)");
    assert!(err.contains("takes one constant setting name"), "{err}");
}

/// A hash join's build side, a `GROUP BY` with many groups, a sort, and a CTE
/// read twice each reserve from the pool and fail under a tiny limit; the
/// failed statement releases everything it held.
#[test]
fn operators_fail_over_the_limit_and_release_their_reservation() {
    let mut p = Pool::new();
    let queries = [
        // 1M build rows on the right.
        "SELECT count(*) FROM (SELECT 1 AS x) s \
         JOIN (SELECT i FROM range(0, 1000000) t(i)) b ON s.x = b.i",
        // 1M groups.
        "SELECT count(*) FROM range(0, 1000000) t(i) GROUP BY i",
        // 1M rows to sort.
        "SELECT i FROM range(0, 1000000) t(i) ORDER BY i",
        // A CTE materialized once and read twice.
        "WITH c AS (SELECT i FROM range(0, 1000000) t(i)) \
         SELECT (SELECT count(*) FROM c) + (SELECT count(*) FROM c)",
    ];
    for sql in queries {
        let before = memory::used();
        p.limit("1MB");
        let err = p.err(sql);
        assert!(
            err.starts_with("Out of Memory Error: failed to allocate"),
            "{sql}: {err}"
        );
        assert!(err.contains("/976.5 KiB used)"), "{sql}: {err}");
        assert_eq!(memory::used(), before, "{sql} leaked a reservation");
        // The default limit lets the same statement through, and the pool is
        // empty again afterwards.
        memory::reset_limit();
        p.exec(sql);
        assert_eq!(memory::used(), before, "{sql} leaked a reservation");
    }
}

/// Under a limit that fits one statement but not two, concurrent statements
/// share the pool: at least one fails and the pool never passes the limit.
#[test]
fn concurrent_statements_share_the_pool() {
    let mut p = Pool::new();
    // Two million groups need tens of megabytes each.
    let sql = "SELECT count(*) FROM range(0, 2000000) t(i) GROUP BY i";
    p.limit("24MB");
    let limit = memory::limit();
    let barrier = std::sync::Barrier::new(2);
    let outcomes = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let barrier = &barrier;
                scope.spawn(move || {
                    let mut db = PinkDb::new();
                    barrier.wait();
                    db.execute(sql).map(|_| ()).map_err(|e| e.to_string())
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("worker thread"))
            .collect::<Vec<_>>()
    });
    assert!(
        outcomes.iter().any(|o| o.is_err()),
        "two 2M-group aggregates cannot both fit in 24 MB: {outcomes:?}"
    );
    for err in outcomes.iter().filter_map(|o| o.as_ref().err()) {
        assert!(err.starts_with("Out of Memory Error:"), "{err}");
    }
    // The pool never passes the limit, and nothing outlives the statements.
    assert!(memory::used() <= limit, "{} > {limit}", memory::used());
    assert_eq!(memory::used(), 0, "a statement leaked its reservation");
}

#[test]
fn the_profile_reports_the_limit_and_the_statement_peak() {
    let mut p = Pool::new();
    p.limit("8GB");
    let (_, profile) =
        p.db.execute_profiled("SELECT count(*) FROM range(0, 100000) t(i) GROUP BY i")
            .expect("profiled query")
            .pop()
            .expect("one statement");
    assert_eq!(profile.memory_limit, memory::limit() as u64);
    assert!(
        profile.memory_peak > 0,
        "a 100k-group aggregate peaked at {} bytes",
        profile.memory_peak
    );
    assert!(
        profile.memory_peak <= profile.memory_limit,
        "peak {} passed the limit {}",
        profile.memory_peak,
        profile.memory_limit
    );
    assert_eq!(memory::used(), 0, "the statement's reservation outlived it");
}

/// Growing a reservation charges the account the delta, not the total:
/// the peak is the largest state, not the sum of every resize.
#[test]
fn a_reservation_charges_the_account_only_what_it_holds() {
    let _lock = memory::test_lock();
    let before = memory::used();
    memory::set_limit(memory::UNLIMITED);
    let account = memory::Account::new();
    let mut held = account.reserve(1_000).unwrap();
    assert_eq!(account.peak(), 1_000);
    held.grow(500).unwrap();
    assert_eq!(account.peak(), 1_500);
    held.shrink(1_200);
    assert_eq!(account.peak(), 1_500);
    assert_eq!(memory::used(), before + 300);
    drop(held);
    assert_eq!(memory::used(), before);
    assert_eq!(account.peak(), 1_500);
    memory::reset_limit();
}

/// A failed grow leaves the reservation and the pool unchanged.
#[test]
fn a_failed_grow_keeps_the_pool_where_it_was() {
    let _lock = memory::test_lock();
    let before = memory::used();
    memory::set_limit(before + 1_000);
    let account = memory::Account::new();
    let mut held = account.reserve(800).unwrap();
    let err = held.grow(800).unwrap_err().to_string();
    assert!(
        err.starts_with("Out of Memory Error: failed to allocate data of size 800 bytes"),
        "{err}"
    );
    assert_eq!(held.bytes(), 800);
    assert_eq!(memory::used(), before + 800);
    drop(held);
    assert_eq!(memory::used(), before);
    memory::reset_limit();
}

#[test]
fn decoded_cache_limit_set_reset_and_lives_inside_the_limit() {
    let mut p = Pool::new();
    // No budget of its own by default: clean chunks take the room the
    // operators leave free (DuckDB's buffer pool inside `max_memory`).
    assert_eq!(memory::cache_limit(), memory::limit());
    p.exec("SET decoded_cache_limit = '32MB'");
    assert_eq!(
        p.text("SELECT current_setting('decoded_cache_limit')"),
        "30.5 MiB"
    );
    assert_eq!(memory::cache_limit(), 32_000_000);
    p.exec("RESET decoded_cache_limit");
    assert_eq!(memory::cache_limit(), memory::limit());
    // Shrinking the operator pool shrinks the room the cache may use.
    p.limit("1GB");
    assert_eq!(memory::cache_limit(), 1_000_000_000);
    assert_eq!(
        p.text("SELECT current_setting('decoded_cache_limit')"),
        "953.6 MiB"
    );
    // `0` is "no cache": a bare zero, the only size without a unit.
    p.exec("SET decoded_cache_limit = '0'");
    assert_eq!(memory::cache_limit(), 0);
    p.exec("RESET decoded_cache_limit");
    // -1 makes the cache unbounded, like `memory_limit`.
    p.exec("SET decoded_cache_limit = '-1'");
    assert_eq!(memory::cache_limit(), memory::UNLIMITED);
    // It is a setting, not a session variable: the name is DuckDB's error
    // shape when it does not exist.
    assert!(
        p.err("SET nosuch_cache_limit = '1MB'")
            .contains("unrecognized configuration parameter")
    );
}
