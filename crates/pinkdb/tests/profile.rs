//! Engine-side query execution profiles (wave 84).
//!
//! Profiling is opt-in and must be observationally invisible: identical
//! results and errors, and byte-identical WAL records for the same workload.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pinkdb::{Database, PinkDb, QueryProfile, QueryResult};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Self-cleaning scratch directory for one durable test.
struct Scratch {
    dir: PathBuf,
    ckpt: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_profile_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create test scratch dir");
        let ckpt = dir.join("db.pink");
        Self { dir, ckpt }
    }

    fn path(&self) -> &str {
        self.ckpt.to_str().unwrap()
    }

    /// The sidecar WAL next to the `.pink` checkpoint (`<path>.pink.wal`).
    fn wal(&self) -> PathBuf {
        PathBuf::from(format!("{}.wal", self.path()))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn results_of(pairs: Vec<(QueryResult, QueryProfile)>) -> Vec<QueryResult> {
    pairs.into_iter().map(|(result, _)| result).collect()
}

fn assert_total_covers_phases(profile: &QueryProfile) {
    let phases = profile.bind + profile.optimize + profile.execute + profile.log;
    assert!(
        profile.total >= phases,
        "total {} < phases {} ({profile:?})",
        profile.total.as_nanos(),
        phases.as_nanos()
    );
}

const SCRIPT: &str = "\
CREATE TABLE t (id INTEGER, name VARCHAR);\
INSERT INTO t VALUES (1, 'a'), (2, 'b');\
SELECT id, name FROM t ORDER BY id;\
UPDATE t SET name = 'z' WHERE id = 1;\
DELETE FROM t WHERE id = 2;\
SELECT COUNT(*) FROM t;";

#[test]
fn results_match_execute_for_a_multi_statement_script() {
    let mut plain = PinkDb::new();
    let expected = plain.execute(SCRIPT).unwrap();
    let mut profiled = PinkDb::new();
    let pairs = profiled.execute_profiled(SCRIPT).unwrap();

    assert_eq!(pairs.len(), expected.len());
    assert_eq!(results_of(pairs), expected);
}

#[test]
fn a_failing_statement_returns_the_same_error_and_keeps_earlier_effects() {
    let script = "\
CREATE TABLE t (a INTEGER);\
INSERT INTO t VALUES (1);\
SELECT * FROM does_not_exist;\
INSERT INTO t VALUES (2);";

    let mut plain = PinkDb::new();
    let plain_err = plain.execute(script).unwrap_err().to_string();
    let mut profiled = PinkDb::new();
    let profiled_err = profiled.execute_profiled(script).unwrap_err().to_string();
    assert_eq!(plain_err, profiled_err);

    // The statement before the failure stays applied on both paths.
    for db in [&mut plain, &mut profiled] {
        match db.execute("SELECT COUNT(*) FROM t;").unwrap().pop() {
            Some(QueryResult::Selected { rows, .. }) => assert_eq!(
                rows,
                vec![vec![pinkdb::Value::Bigint(1)]],
                "earlier INSERT must survive"
            ),
            other => panic!("expected rows, got {other:?}"),
        }
    }
}

#[test]
fn profiles_report_kind_rows_columns_and_cover_every_phase() {
    let mut db = PinkDb::new();
    let pairs = db.execute_profiled(SCRIPT).unwrap();
    let profiles: Vec<&QueryProfile> = pairs.iter().map(|(_, p)| p).collect();

    assert_eq!(profiles.len(), 6);
    assert_eq!(profiles[0].kind, "Created");
    assert_eq!((profiles[0].rows, profiles[0].columns), (0, 0));
    assert_eq!(profiles[1].kind, "Inserted");
    assert_eq!((profiles[1].rows, profiles[1].columns), (2, 0));
    assert_eq!(profiles[2].kind, "Selected");
    assert_eq!((profiles[2].rows, profiles[2].columns), (2, 2));
    assert_eq!(profiles[3].kind, "Updated");
    assert_eq!((profiles[3].rows, profiles[3].columns), (1, 0));
    assert_eq!(profiles[4].kind, "Deleted");
    assert_eq!((profiles[4].rows, profiles[4].columns), (1, 0));
    assert_eq!(profiles[5].kind, "Selected");
    assert_eq!((profiles[5].rows, profiles[5].columns), (1, 1));

    let parse = profiles[0].parse;
    for profile in profiles {
        assert_eq!(profile.engine_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(
            profile.parse, parse,
            "one parse is shared by every statement of the text"
        );
        assert_total_covers_phases(profile);
    }
}

#[test]
fn in_transaction_is_true_between_begin_and_commit() {
    let script = "\
CREATE TABLE t (a INTEGER);\
BEGIN;\
INSERT INTO t VALUES (1);\
SELECT * FROM t;\
COMMIT;";
    let mut db = PinkDb::new();
    let pairs = db.execute_profiled(script).unwrap();
    let profiles: Vec<&QueryProfile> = pairs.iter().map(|(_, p)| p).collect();

    assert!(!profiles[0].in_transaction, "CREATE runs autocommitted");
    assert!(!profiles[1].in_transaction, "BEGIN itself is outside");
    assert!(profiles[2].in_transaction);
    assert!(profiles[3].in_transaction);
    assert!(profiles[4].in_transaction, "COMMIT itself is still inside");
    for profile in profiles {
        assert_total_covers_phases(profile);
    }
}

#[test]
fn connection_execute_profiled_matches_execute() {
    let script = "\
CREATE TABLE t (a INTEGER);\
BEGIN;\
INSERT INTO t VALUES (1);\
SELECT * FROM t;\
COMMIT;";

    let plain_db = Database::new();
    let mut plain = plain_db.connect();
    let expected = plain.execute(script).unwrap();

    let profiled_db = Database::new();
    let mut profiled = profiled_db.connect();
    let pairs = profiled.execute_profiled(script).unwrap();

    assert_eq!(pairs.len(), expected.len());
    assert_eq!(results_of(pairs.clone()), expected);

    let profiles: Vec<&QueryProfile> = pairs.iter().map(|(_, p)| p).collect();
    assert_eq!(profiles[1].kind, "Began");
    assert!(!profiles[1].in_transaction);
    assert!(profiles[2].in_transaction);
    assert!(profiles[3].in_transaction);
    assert!(profiles[4].in_transaction);
    for profile in profiles {
        assert_eq!(
            profile.optimize,
            Duration::ZERO,
            "the connection path measures bind+optimize together as bind"
        );
        assert_total_covers_phases(profile);
    }
}

#[test]
fn durable_wal_is_identical_with_and_without_profiling() {
    const DURABLE_SCRIPT: &str = "\
CREATE TABLE t (a INTEGER, b VARCHAR);\
INSERT INTO t VALUES (1, 'x'), (2, 'y');\
UPDATE t SET b = 'z' WHERE a = 1;\
DELETE FROM t WHERE a = 2;";

    let plain_scratch = Scratch::new("plain");
    let mut plain = PinkDb::open_durable(plain_scratch.path()).unwrap();
    let plain_results = plain.execute(DURABLE_SCRIPT).unwrap();
    drop(plain);

    let profiled_scratch = Scratch::new("profiled");
    let mut profiled = PinkDb::open_durable(profiled_scratch.path()).unwrap();
    let pairs = profiled.execute_profiled(DURABLE_SCRIPT).unwrap();
    drop(profiled);

    assert_eq!(results_of(pairs), plain_results);

    let plain_wal = std::fs::read(plain_scratch.wal()).expect("read plain WAL");
    let profiled_wal = std::fs::read(profiled_scratch.wal()).expect("read profiled WAL");
    assert_eq!(plain_wal.len(), profiled_wal.len());

    // A fresh WAL header carries a random incarnation id and the CRC over it
    // (bytes 20..40 of the 96-byte header). Those are deliberately different
    // for two stores; every other byte — the header's format/generation and
    // every logged record — must match exactly.
    const WAL_HEADER_LEN: usize = 96;
    const WAL_INCARNATION: std::ops::Range<usize> = 20..40;
    assert!(
        plain_wal.len() > WAL_HEADER_LEN,
        "the script must log records"
    );
    let mut normalized_plain = plain_wal;
    let mut normalized_profiled = profiled_wal;
    normalized_plain[WAL_INCARNATION.clone()].fill(0);
    normalized_profiled[WAL_INCARNATION].fill(0);
    assert_eq!(normalized_plain, normalized_profiled);
}

/// Overhead report (wave 84): `execute` vs `execute_profiled` on many tiny
/// statements. Not a pass/fail gate (timings are machine-dependent); run with
/// `cargo test --release -p pinkdb --test profile -- --ignored --nocapture`.
#[test]
#[ignore]
fn profiling_overhead_report() {
    use std::time::Instant;
    let n = 20_000;
    let run = |profiled: bool| {
        let mut db = pinkdb::PinkDb::new();
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        let start = Instant::now();
        for i in 0..n {
            let sql = format!("INSERT INTO t VALUES ({i}); SELECT a FROM t WHERE a = {i};");
            if profiled {
                db.execute_profiled(&sql).unwrap();
            } else {
                db.execute(&sql).unwrap();
            }
        }
        start.elapsed()
    };
    // Warm up, then take the best of three for each mode.
    run(false);
    let plain = (0..3).map(|_| run(false)).min().unwrap();
    let profiled = (0..3).map(|_| run(true)).min().unwrap();
    let per = |d: std::time::Duration| d.as_nanos() as f64 / (2 * n) as f64;
    println!(
        "statements {}: execute {:?} ({:.0} ns/stmt), execute_profiled {:?} ({:.0} ns/stmt), overhead {:+.1}%",
        2 * n,
        plain,
        per(plain),
        profiled,
        per(profiled),
        (profiled.as_secs_f64() / plain.as_secs_f64() - 1.0) * 100.0
    );
}
