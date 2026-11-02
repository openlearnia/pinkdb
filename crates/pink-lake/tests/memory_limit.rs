//! Wave 133b: a lake scan charges its decoded chunks to the process-wide
//! memory pool exactly as a store scan does, so `SET memory_limit` bounds a
//! lake scan and a query that must hold the whole table fails cleanly.
//!
//! These tests have their own file because the pool is process-wide: the
//! other lake tests must never run under the tiny limit these set.

#[path = "common/local_lake.rs"]
mod local_lake;
use local_lake::*;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{MutexGuard, mpsc};
use std::time::Duration;

use pink_lake::{Commit, Lake};
use pinkdb::{CHUNK_SIZE, DataTable, DataType, PinkDb, Value, memory};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// The pool the scan draws its workers from, and the morsel bound it implies.
const BATCH: usize = 2;
/// A `.pink` file is one morsel and its decode charges the whole file at
/// once, so two files in flight must fit; the table itself must not.
const CHUNKS_PER_FILE: usize = 8;
const LIMIT: usize = 16 * BATCH * CHUNK_SIZE * 8;
const CHUNKS: usize = 64 * BATCH;
const FILES: i64 = (CHUNKS / CHUNKS_PER_FILE) as i64;
const ROWS_PER_FILE: i64 = (CHUNKS_PER_FILE * CHUNK_SIZE) as i64;

const _: () = assert!(
    CHUNKS * CHUNK_SIZE * 8 >= 4 * LIMIT,
    "the lake is not four times the limit"
);
const _: () = assert!(
    2 * CHUNKS_PER_FILE * CHUNK_SIZE * 8 < LIMIT,
    "two files in flight must fit in the limit"
);

struct Fixture {
    _lock: MutexGuard<'static, ()>,
    dir: PathBuf,
}

impl Fixture {
    fn new(tag: &str) -> Fixture {
        let lock = memory::test_lock();
        memory::reset_limit();
        let dir = std::env::temp_dir().join(format!(
            "pink_lake_memory_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        Fixture { _lock: lock, dir }
    }

    /// A lake of `files` `.pink` files, each with `rows` rows of
    /// `t(i BIGINT)`, attached to a fresh database.
    fn attach(&self, files: i64, rows: i64) -> PinkDb {
        let mut lake: Lake = create_lake(self.dir.join("lake")).unwrap();
        lake.set_data_inlining_row_limit(0);
        for file in 0..files {
            let mut table =
                DataTable::new("t".to_string(), vec![("i".to_string(), DataType::Bigint)]).unwrap();
            let first = file * rows;
            for i in first..first + rows {
                table.append_row(vec![Value::Bigint(i)]).unwrap();
            }
            let mut commit = Commit::new();
            commit.append("main.t", Arc::new(table));
            lake.commit(&commit).unwrap();
        }
        let report = open_lake(self.dir.join("lake"))
            .unwrap()
            .attach(None)
            .unwrap();
        let mut db = PinkDb::new();
        db.attach_lake(report.lake).unwrap();
        db
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        memory::reset_limit();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut results = db.execute(sql).unwrap();
    match results.pop().unwrap() {
        pinkdb::QueryResult::Selected { rows, .. } => rows,
        other => panic!("{other:?}"),
    }
}

fn expected_total() -> (i64, i64) {
    let total = FILES * ROWS_PER_FILE;
    (total, total * (total - 1) / 2)
}

/// Run `work` on the two-thread pool the scan draws its workers from; a lake
/// scan that deadlocks the pool fails here rather than hanging the suite.
fn on_two_threads<T: Send + 'static>(
    limit: Duration,
    work: impl FnOnce() -> T + Send + 'static,
) -> T {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(BATCH)
        .build()
        .unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        tx.send(pool.install(work)).ok();
    });
    match rx.recv_timeout(limit) {
        Ok(value) => value,
        Err(mpsc::RecvTimeoutError::Timeout) => panic!(
            "the lake scan did not finish within {limit:?}: a worker is waiting on \
             a source that needs the pool"
        ),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("the scan thread died"),
    }
}

/// The pool's high-water mark while `f` runs: after the fact `used()` only
/// says the scan released its chunks, not that it never pinned the table.
fn peak_used_during<F: FnOnce()>(f: F) -> usize {
    struct Stop(Arc<AtomicBool>);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        let (flag, high) = (Arc::clone(&stop), Arc::clone(&peak));
        scope.spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                high.fetch_max(memory::used(), Ordering::Relaxed);
                std::thread::sleep(Duration::from_micros(20));
            }
        });
        let _stop = Stop(Arc::clone(&stop));
        f();
    });
    peak.load(Ordering::Relaxed)
}

#[test]
fn a_lake_scan_streams_a_table_larger_than_the_limit() {
    let fixture = Fixture::new("stream");
    let mut db = fixture.attach(FILES, ROWS_PER_FILE);
    memory::set_limit(LIMIT);

    let (answer, high) = on_two_threads(Duration::from_secs(180), move || {
        let mut answer = Vec::new();
        let high = peak_used_during(|| {
            answer = query(&mut db, "SELECT count(*), sum(i) FROM t;");
        });
        (answer, high)
    });

    let (rows, sum) = expected_total();
    assert_eq!(answer, vec![vec![Value::Bigint(rows), Value::Bigint(sum)]]);
    assert!(
        memory::used() <= LIMIT,
        "the scan left the pool over its limit"
    );
    assert!(
        high <= LIMIT,
        "the scan pinned {high} bytes, over the {LIMIT} limit"
    );
}

#[test]
fn a_lake_sort_that_must_hold_the_table_fails_like_a_store_scan() {
    let fixture = Fixture::new("sort");
    let mut db = fixture.attach(FILES, ROWS_PER_FILE);
    memory::set_limit(LIMIT);

    let (err, answer, used) = on_two_threads(Duration::from_secs(180), move || {
        let err = match db.execute("SELECT i FROM t ORDER BY i;") {
            Ok(_) => panic!("the sort held a table four times the limit"),
            Err(e) => e.to_string(),
        };
        let answer = query(&mut db, "SELECT count(*), sum(i) FROM t;");
        (err, answer, memory::used())
    });

    assert!(
        err.starts_with("Out of Memory Error: failed to allocate"),
        "{err}"
    );
    // The failed statement released its reservation: the same table still
    // streams through the pool afterwards.
    let (rows, sum) = expected_total();
    assert_eq!(answer, vec![vec![Value::Bigint(rows), Value::Bigint(sum)]]);
    assert!(
        used <= LIMIT,
        "the failed sort left the pool over its limit"
    );
}
