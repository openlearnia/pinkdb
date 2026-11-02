//! Wave 136b-3: decoded store chunks are charged to the pool and evicted when
//! a reservation would pass the limit.
//!
//! The pool is process-wide, so this file is its own process and every test
//! holds [`memory::test_lock`]: a tiny limit must never reach another test.
//!
//! A chunk is evicted only when nothing else holds its payload, so a `Weak` to
//! the vector is how a test sees an eviction: `upgrade()` is `None` exactly
//! while the chunk is back in its encoded form.

use std::sync::{Arc, MutexGuard};

use pinkdb::memory;
use pinkdb::storage::DataTable;
use pinkdb::{CHUNK_SIZE, PinkDb, QueryResult, Value, Vector};

/// A scratch directory and the pool, restored when the test ends.
struct Store {
    _lock: MutexGuard<'static, ()>,
    dir: std::path::PathBuf,
}

impl Store {
    fn new(tag: &str) -> Store {
        let lock = memory::test_lock();
        memory::reset_limit();
        let dir =
            std::env::temp_dir().join(format!("pinkdb_chunk_cache_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Store { _lock: lock, dir }
    }

    fn path(&self, name: &str) -> String {
        self.dir.join(name).to_str().unwrap().to_string()
    }

    /// Save a `{name} (i BIGINT)` of `chunks` full chunks and return its path.
    fn save(&self, name: &str, chunks: usize) -> String {
        let path = self.path(&format!("{name}.pink"));
        let mut db = PinkDb::new();
        db.execute(&format!("CREATE TABLE {name} (i BIGINT)"))
            .unwrap();
        db.execute(&format!(
            "INSERT INTO {name} SELECT i FROM range(0, {}) r(i)",
            chunks * CHUNK_SIZE
        ))
        .unwrap();
        db.save(&path).unwrap();
        path
    }

    /// A saved store, opened lazily: nothing is decoded until a chunk is read.
    fn table(&self, name: &str, chunks: usize) -> PinkDb {
        PinkDb::open(&self.save(name, chunks)).unwrap()
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        memory::reset_limit();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn int(db: &mut PinkDb, sql: &str) -> i64 {
    match db.execute(sql).unwrap().pop().expect("one statement") {
        QueryResult::Selected { rows, .. } => match rows[0][0] {
            Value::Bigint(v) => v,
            ref other => panic!("{sql}: expected a BIGINT, got {other:?}"),
        },
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

fn range_sum(rows: usize) -> i64 {
    (0..rows as i64).sum()
}

/// The rows of one column, read chunk by chunk (`scan_chunk`), dropping each
/// chunk before decoding the next: the read path the cache exists for.
fn sum_chunks(table: &DataTable) -> i64 {
    let mut sum = 0;
    for i in 0..table.num_chunks() {
        let chunk = table.scan_chunk(i).unwrap().expect("a chunk");
        for row in 0..chunk.num_rows() {
            match chunk.columns[0].get(row) {
                Value::Bigint(v) => sum += v,
                ref other => panic!("expected a BIGINT, got {other:?}"),
            }
        }
    }
    sum
}

fn first_chunk(db: &PinkDb, table: &str) -> Arc<Vector> {
    db.table(table).unwrap().columns[0].chunk(0).unwrap()
}

/// Each table fits in the limit alone, both do not: the second statement
/// evicts the first table's chunks as it decodes, and the third decodes them
/// again — same answers, and the pool never passes the limit.
#[test]
fn a_second_statement_evicts_the_first_tables_chunks() {
    let store = Store::new("scan");
    let chunks = 30;
    let mut a = store.table("a", chunks);
    let mut b = store.table("b", chunks);
    let expected = range_sum(chunks * CHUNK_SIZE);
    // 30 BIGINT chunks are ~500 KB decoded: 700 KiB fits one table, not both.
    memory::set_limit(700 * 1024);
    let limit = memory::limit();

    assert_eq!(int(&mut a, "SELECT sum(i) FROM a"), expected);
    assert!(memory::used() > 0, "the decoded chunks are not charged");
    assert!(memory::used() <= limit);
    let watched = Arc::downgrade(&first_chunk(&a, "a"));
    assert!(watched.upgrade().is_some(), "a's chunk is not decoded");

    assert_eq!(int(&mut b, "SELECT sum(i) FROM b"), expected);
    assert!(memory::used() <= limit);
    assert!(
        watched.upgrade().is_none(),
        "b did not evict a's chunk, so 60 chunks fit in a 30-chunk limit"
    );

    // Decoded again from the retained image, CRC and all.
    assert_eq!(int(&mut a, "SELECT sum(i) FROM a"), expected);
    assert!(memory::used() <= limit);
}

/// A reader holding payloads pins them: a failing reservation cannot evict
/// them, and once it lets go they are the first to go.
#[test]
fn held_chunks_are_not_evicted() {
    let store = Store::new("pin");
    let db = store.table("t", 4);
    let column = &db.table("t").unwrap().columns[0];
    let held: Vec<Arc<Vector>> = (0..column.num_chunks())
        .map(|i| column.chunk(i).unwrap())
        .collect();
    assert!(held.iter().all(|v| v.len() == CHUNK_SIZE));

    // A cache hit returns the charged payload, it does not charge again.
    let used = memory::used();
    assert!(Arc::ptr_eq(&column.chunk(0).unwrap(), &held[0]));
    assert_eq!(memory::used(), used, "a cache hit charged the pool again");

    // One byte of headroom: a 1 KiB reservation can only fit by evicting.
    memory::set_limit(used + 1);
    let err = match memory::reserve(1024) {
        Ok(_) => panic!("a reservation over the limit succeeded"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.starts_with("Out of Memory Error: failed to allocate"),
        "{err}"
    );
    let watched: Vec<_> = held.iter().map(Arc::downgrade).collect();
    assert!(
        watched.iter().all(|w| w.upgrade().is_some()),
        "a chunk a reader holds was evicted"
    );

    // Without the readers the same reservation evicts a chunk.
    drop(held);
    memory::reserve(1024).unwrap();
    assert!(
        watched.iter().any(|w| w.upgrade().is_none()),
        "nothing was evicted after the readers let go"
    );
}

/// A dirty chunk is never a candidate, and `.save` writes the update.
#[test]
fn a_dirty_chunk_is_never_evicted_and_survives_save() {
    let store = Store::new("dirty");
    let mut db = store.table("t", 2);
    db.execute("UPDATE t SET i = -1 WHERE i < 5").unwrap();
    let column = &db.table("t").unwrap().columns[0];
    let dirty = Arc::downgrade(&column.chunk(0).unwrap());
    let clean = Arc::downgrade(&column.chunk(1).unwrap());

    // One byte of headroom: the reservation only fits by evicting the clean
    // second chunk, and the dirty first chunk stays put.
    memory::set_limit(memory::used() + 1);
    let limit = memory::limit();
    memory::reserve(1024).unwrap();
    assert!(dirty.upgrade().is_some(), "the dirty chunk was evicted");
    assert!(
        clean.upgrade().is_none(),
        "the clean chunk was not evicted to make room"
    );
    assert!(memory::used() <= limit);

    // Saving decodes every chunk to encode it, so give it room again.
    memory::reset_limit();
    let out = store.path("out.pink");
    db.save(&out).unwrap();
    drop(db);
    let mut reread = PinkDb::open(&out).unwrap();
    assert_eq!(int(&mut reread, "SELECT count(*) FROM t WHERE i = -1"), 5);
    assert_eq!(
        int(&mut reread, "SELECT sum(i) FROM t"),
        range_sum(2 * CHUNK_SIZE) - 15
    );
}

/// Built and appended chunks are charged too; a save releases them and a lazy
/// open charges nothing, so the pool sees the store's decoded footprint and
/// not its bytes on disk.
#[test]
fn built_chunks_are_charged_and_an_open_charges_nothing() {
    let store = Store::new("charge");
    let before = memory::used();
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i BIGINT)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    assert!(
        memory::used() > before,
        "the built chunk is not charged to the pool"
    );
    let path = store.path("t.pink");
    db.save(&path).unwrap();
    drop(db);
    assert_eq!(memory::used(), before, "a dropped store keeps its charge");

    let mut db = PinkDb::open(&path).unwrap();
    assert_eq!(
        memory::used(),
        before,
        "open charged a chunk it did not decode"
    );
    assert_eq!(int(&mut db, "SELECT sum(i) FROM t"), 3);
    assert!(memory::used() > before, "the decoded chunk is not charged");
}

/// Two threads scan one store chunk by chunk, 50 rounds, while the pool is too
/// small for either whole table: they evict each other's chunks and never
/// deadlock (the evictor `try_lock`s each handle rather than waiting for it).
#[test]
fn concurrent_readers_evict_without_deadlock() {
    let store = Store::new("threads");
    let chunks = 40;
    let path = store.save("t", chunks);
    memory::set_limit(128 * 1024);
    let limit = memory::limit();
    let expected = range_sum(chunks * CHUNK_SIZE);
    for round in 0..50 {
        let answers = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..2)
                .map(|_| {
                    let path = path.as_str();
                    scope.spawn(move || {
                        let db = PinkDb::open(path).unwrap();
                        let sum = sum_chunks(db.table("t").unwrap());
                        assert!(memory::used() <= limit);
                        sum
                    })
                })
                .collect();
            threads
                .into_iter()
                .map(|t| t.join().expect("reader"))
                .collect::<Vec<_>>()
        });
        assert_eq!(answers, vec![expected; 2], "round {round}");
        assert!(memory::used() <= limit);
    }
}
