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
        memory::reset_cache_limit();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn int(db: &mut PinkDb, sql: &str) -> i64 {
    match db.execute(sql).unwrap().pop().expect("one statement") {
        QueryResult::Selected { rows, .. } => match rows[0][0] {
            Value::Bigint(v) => v,
            // SUM returns HUGEINT; these tests only sum small values.
            Value::Hugeint(v) => i64::try_from(v).expect("fits in i64"),
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

/// Weak handles on every decoded chunk of `table`'s first column.
fn watch(db: &PinkDb, table: &str) -> Vec<std::sync::Weak<Vector>> {
    let column = &db.table(table).unwrap().columns[0];
    (0..column.num_chunks())
        .map(|i| Arc::downgrade(&column.chunk(i).unwrap()))
        .collect()
}

fn alive(watched: &[std::sync::Weak<Vector>]) -> usize {
    watched.iter().filter(|w| w.upgrade().is_some()).count()
}

/// Each table fits in the cache alone, both do not (wave 136c-4, scan
/// resistance): the first statement over the second table finds the cache full
/// and decodes its chunks for itself alone, charged to the operator pool and
/// released when it ends — the first table's chunks stay. The second
/// statement over it has read those chunks before, so it admits them by
/// evicting; the first table then decodes again. Same answers throughout, and
/// the cache never passes its limit.
#[test]
fn a_one_pass_scan_does_not_flush_a_full_cache_and_a_second_pass_does() {
    let store = Store::new("scan");
    let chunks = 30;
    let mut a = store.table("a", chunks);
    let mut b = store.table("b", chunks);
    let expected = range_sum(chunks * CHUNK_SIZE);
    // 30 BIGINT chunks are ~500 KB decoded: 700 KiB holds one table.
    memory::set_cache_limit(700 * 1024);
    let limit = memory::cache_limit();

    assert_eq!(int(&mut a, "SELECT sum(i) FROM a"), expected);
    assert!(
        memory::cache_used() > 0,
        "the decoded chunks are not charged to the cache"
    );
    assert!(memory::cache_used() <= limit);
    let watched = watch(&a, "a");
    assert_eq!(alive(&watched), chunks, "a's chunks are not all cached");

    let operators = memory::used();
    assert_eq!(int(&mut b, "SELECT sum(i) FROM b"), expected);
    assert!(memory::cache_used() <= limit);
    assert_eq!(
        alive(&watched),
        chunks,
        "b's first scan flushed a's chunks from a full cache"
    );
    assert_eq!(
        memory::used(),
        operators,
        "b's own chunks stayed charged after the statement"
    );

    assert_eq!(int(&mut b, "SELECT sum(i) FROM b"), expected);
    assert!(memory::cache_used() <= limit);
    // Which of a's chunks go depends on the order parallel workers decoded
    // them and on which worker wins the evictor; that some go does not.
    assert!(
        alive(&watched) < chunks,
        "b's second scan did not admit its chunks over a's"
    );

    // Decoded again from the retained image, CRC and all.
    assert_eq!(int(&mut a, "SELECT sum(i) FROM a"), expected);
    assert!(memory::cache_used() <= limit);
    memory::reset_cache_limit();
}

/// A chunk the cache does not take belongs to its reader: it is charged to
/// the operator pool while the reader holds it and released when the last
/// reader drops it (wave 136c-4).
#[test]
fn a_chunk_the_cache_refuses_is_charged_to_its_reader_until_dropped() {
    let store = Store::new("own");
    let db = store.table("t", 4);
    memory::set_cache_limit(0);
    let column = &db.table("t").unwrap().columns[0];
    let before = memory::used();
    let chunk = column.chunk(0).unwrap();
    assert_eq!(memory::cache_used(), 0, "a refused chunk was cached");
    assert!(
        memory::used() >= before + chunk.memory_size(),
        "a refused chunk is not charged to the operator pool"
    );
    let shared = Arc::clone(&chunk);
    drop(chunk);
    assert!(
        memory::used() > before,
        "the charge left with one reader of two"
    );
    drop(shared);
    assert_eq!(memory::used(), before, "the charge outlived its readers");
    memory::reset_cache_limit();
}

/// A streamed `SELECT *` over a table larger than `memory_limit` runs to the
/// end (wave 136c-4). The cursor's producer works on a snapshot that shares,
/// and so pins, every chunk the inline phase cached: far more than one
/// eviction pass of the cache admissions probes. An operator reservation still
/// finds the evictable chunks behind them.
#[test]
fn a_cursor_streams_a_table_larger_than_memory_limit() {
    let store = Store::new("cursor");
    // 1,600 BIGINT chunks are ~26 MB decoded; the cursor's inline phase
    // buffers 16 MB before it hands off to its producer.
    let chunks = 1600;
    let mut db = store.table("t", chunks);
    memory::set_limit(24 << 20);
    let mut cursor = db.query_cursor("SELECT i FROM t").unwrap();
    let (mut rows, mut sum) = (0usize, 0i64);
    while let Some(chunk) = cursor.next_chunk().unwrap() {
        for r in 0..chunk.num_rows() {
            match chunk.columns[0].get(r) {
                Value::Bigint(v) => sum += v,
                other => panic!("expected a BIGINT, got {other:?}"),
            }
        }
        rows += chunk.num_rows();
    }
    assert_eq!(rows, chunks * CHUNK_SIZE);
    assert_eq!(sum, range_sum(chunks * CHUNK_SIZE));
    assert!(memory::used() + memory::cache_used() <= 24 << 20);
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

    // A cache hit returns the cached payload, it does not charge again.
    let used = memory::cache_used();
    assert!(Arc::ptr_eq(&column.chunk(0).unwrap(), &held[0]));
    assert_eq!(
        memory::cache_used(),
        used,
        "a cache hit charged the budget again"
    );

    // One byte of headroom: a 1 KiB reservation can only fit by evicting.
    memory::set_cache_limit(used + 1);
    assert!(
        memory::reserve_decoded(1024).is_none(),
        "a reservation over the budget succeeded"
    );
    let watched: Vec<_> = held.iter().map(Arc::downgrade).collect();
    assert!(
        watched.iter().all(|w| w.upgrade().is_some()),
        "a chunk a reader holds was evicted"
    );

    // Without the readers the same reservation evicts a chunk.
    drop(held);
    assert!(memory::reserve_decoded(1024).is_some());
    assert!(
        watched.iter().any(|w| w.upgrade().is_none()),
        "nothing was evicted after the readers let go"
    );
    memory::reset_cache_limit();
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

    // One byte of headroom in the cache: the reservation only fits by evicting
    // the clean second chunk, and the dirty first chunk stays put.
    memory::set_cache_limit(memory::cache_used() + 1);
    let limit = memory::cache_limit();
    assert!(memory::reserve_decoded(1024).is_some());
    assert!(dirty.upgrade().is_some(), "the dirty chunk was evicted");
    assert!(
        clean.upgrade().is_none(),
        "the clean chunk was not evicted to make room"
    );
    assert!(memory::cache_used() <= limit);
    memory::reset_cache_limit();

    // Saving writes the update back, re-encoding only the dirty chunk.
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
    let cached = memory::cache_used();
    assert_eq!(
        memory::used(),
        before,
        "open charged a chunk it did not decode"
    );
    assert_eq!(int(&mut db, "SELECT sum(i) FROM t"), 3);
    assert!(
        memory::cache_used() > cached,
        "the decoded chunk is not charged to the cache"
    );
}

/// Two threads scan one store chunk by chunk, 50 rounds, while the pool is too
/// small for either whole table: they evict each other's chunks and never
/// deadlock (the evictor `try_lock`s each handle rather than waiting for it).
#[test]
fn concurrent_readers_evict_without_deadlock() {
    let store = Store::new("threads");
    let chunks = 40;
    let path = store.save("t", chunks);
    memory::set_cache_limit(128 * 1024);
    let limit = memory::cache_limit();
    let expected = range_sum(chunks * CHUNK_SIZE);
    for round in 0..50 {
        let answers = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..2)
                .map(|_| {
                    let path = path.as_str();
                    scope.spawn(move || {
                        let db = PinkDb::open(path).unwrap();
                        let sum = sum_chunks(db.table("t").unwrap());
                        assert!(memory::cache_used() <= limit);
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
        assert!(memory::cache_used() <= limit);
    }
}

/// A table with a VARCHAR, a nullable INTEGER and a BIGINT: the footer's
/// per-chunk stats and zones differ per column, so a copy-through save has to
/// reproduce all three.
fn wide(db: &mut PinkDb, name: &str, chunks: usize) {
    db.execute(&format!(
        "CREATE TABLE {name} (s VARCHAR, i INTEGER, n BIGINT)"
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO {name} SELECT \
           CASE WHEN i % 5 = 0 THEN NULL ELSE 'v' || CAST(i AS VARCHAR) END, \
           CASE WHEN i % 3 = 0 THEN NULL ELSE i END, \
           i * 2 \
         FROM range(0, {}) r(i)",
        chunks * CHUNK_SIZE
    ))
    .unwrap();
}

/// A save copies a clean chunk's bytes through: saving an unchanged store
/// writes the same file and decodes nothing.
#[test]
fn save_of_an_unchanged_store_is_byte_identical_and_decodes_nothing() {
    let store = Store::new("copy");
    let mut db = PinkDb::new();
    wide(&mut db, "t", 4);
    db.execute("CREATE TABLE u (i BIGINT)").unwrap();
    db.execute("INSERT INTO u SELECT i FROM range(0, 5000) r(i)")
        .unwrap();
    let path = store.path("t.pink");
    db.save(&path).unwrap();
    drop(db);

    let db = PinkDb::open(&path).unwrap();
    let before = memory::used();
    let out = store.path("out.pink");
    db.save(&out).unwrap();
    assert_eq!(
        memory::used(),
        before,
        "saving an unchanged store decoded a chunk"
    );
    let (a, b) = (std::fs::read(&path).unwrap(), std::fs::read(&out).unwrap());
    assert!(a == b, "the saved file differs from the store it copied");
}

/// A save re-encodes only the chunks a statement touched. A second save copies
/// those through, so both files are identical — the encoder's bytes are the
/// ones the first save stored — and the changes survive a reopen.
#[test]
fn a_saved_change_becomes_clean_and_reopens_equal() {
    let store = Store::new("changes");
    let path = store.save("t", 4);
    let mut db = PinkDb::open(&path).unwrap();
    db.execute("UPDATE t SET i = i + 1000000 WHERE i < 3")
        .unwrap();
    db.execute("INSERT INTO t VALUES (-1), (-2)").unwrap();
    db.execute("DELETE FROM t WHERE i % 97 = 0").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN w INTEGER DEFAULT 7")
        .unwrap();
    let (rows, sum, w) = (
        int(&mut db, "SELECT count(*) FROM t"),
        int(&mut db, "SELECT sum(i) FROM t"),
        int(&mut db, "SELECT sum(w) FROM t"),
    );
    assert_ne!(
        sum,
        range_sum(4 * CHUNK_SIZE),
        "the fixture changed nothing"
    );

    let a = store.path("a.pink");
    let b = store.path("b.pink");
    db.save(&a).unwrap();
    db.save(&b).unwrap();
    assert!(
        std::fs::read(&a).unwrap() == std::fs::read(&b).unwrap(),
        "the second save re-encoded a chunk the first had stored"
    );
    drop(db);

    let mut back = PinkDb::open(&a).unwrap();
    assert_eq!(int(&mut back, "SELECT count(*) FROM t"), rows);
    assert_eq!(int(&mut back, "SELECT sum(i) FROM t"), sum);
    assert_eq!(int(&mut back, "SELECT sum(w) FROM t"), w);

    let c = store.path("c.pink");
    back.save(&c).unwrap();
    assert!(
        std::fs::read(&a).unwrap() == std::fs::read(&c).unwrap(),
        "a re-save of the reopened store is not byte-identical"
    );
}

/// Saving stores a dirty chunk, so the cache may drop it afterwards: it is
/// clean and evictable without reopening the store.
#[test]
fn a_saved_dirty_chunk_is_evictable() {
    let store = Store::new("saved");
    let path = store.save("t", 1);
    let mut db = PinkDb::open(&path).unwrap();
    db.execute("UPDATE t SET i = i + 1").unwrap();
    let watched = {
        let column = &db.table("t").unwrap().columns[0];
        Arc::downgrade(&column.chunk(0).unwrap())
    };
    assert!(watched.upgrade().is_some());

    let out = store.path("out.pink");
    db.save(&out).unwrap();
    memory::set_cache_limit(memory::cache_used() + 1);
    assert!(memory::reserve_decoded(1024).is_some());
    assert!(
        watched.upgrade().is_none(),
        "a saved chunk is still not evictable"
    );

    memory::reset_cache_limit();
    assert_eq!(
        int(&mut db, "SELECT sum(i) FROM t"),
        range_sum(CHUNK_SIZE) + CHUNK_SIZE as i64
    );
}

/// A save repoints the handles at the new file's image while a reader holds the
/// old payloads: the reader keeps the values it read, the file holds the
/// update, and the store re-reads it from the new image once the reader is
/// gone.
#[test]
fn a_save_while_a_reader_holds_chunks_writes_the_update() {
    let store = Store::new("held_save");
    let path = store.save("t", 3);
    let mut db = PinkDb::open(&path).unwrap();
    db.execute("UPDATE t SET i = -1 WHERE i < 10").unwrap();
    let held: Vec<Arc<Vector>> = {
        let column = &db.table("t").unwrap().columns[0];
        (0..column.num_chunks())
            .map(|i| column.chunk(i).unwrap())
            .collect()
    };
    let out = store.path("out.pink");
    db.save(&out).unwrap();

    // The holder keeps its values: saving does not rewrite the chunks in place.
    assert_eq!(held[0].get(0), Value::Bigint(-1));
    assert_eq!(int(&mut db, "SELECT count(*) FROM t WHERE i = -1"), 10);

    let watched: Vec<_> = held.iter().map(Arc::downgrade).collect();
    drop(held);
    memory::set_cache_limit(memory::cache_used() + 1);
    assert!(memory::reserve_decoded(1024).is_some());
    assert!(
        watched.iter().any(|w| w.upgrade().is_none()),
        "nothing was evicted after the reader let go"
    );
    memory::reset_cache_limit();
    drop(db);

    let mut back = PinkDb::open(&out).unwrap();
    assert_eq!(int(&mut back, "SELECT count(*) FROM t WHERE i = -1"), 10);
    assert_eq!(
        int(&mut back, "SELECT sum(i) FROM t"),
        range_sum(3 * CHUNK_SIZE) - 55
    );
}

/// An encrypted store round-trips through the copy-through save: the container
/// is re-encrypted whole, the chunks inside keep their bytes.
#[cfg(feature = "encryption")]
#[test]
fn an_encrypted_store_round_trips_through_a_save() {
    use pinkdb::EncryptionKey;

    let store = Store::new("enc_save");
    let key = EncryptionKey::from_passphrase("chunk cache test");
    let mut db = PinkDb::new();
    wide(&mut db, "t", 2);
    let path = store.path("t.pink");
    db.save_with_key(&path, &key).unwrap();
    drop(db);

    let mut db = PinkDb::open_with_key(&path, &key).unwrap();
    let (rows, n) = (
        int(&mut db, "SELECT count(*) FROM t"),
        int(&mut db, "SELECT sum(n) FROM t"),
    );
    for name in ["first.pink", "second.pink"] {
        let out = store.path(name);
        db.save_with_key(&out, &key).unwrap();
        let mut back = PinkDb::open_with_key(&out, &key).unwrap();
        assert_eq!(int(&mut back, "SELECT count(*) FROM t"), rows, "{name}");
        assert_eq!(int(&mut back, "SELECT sum(n) FROM t"), n, "{name}");
    }
}

/// Runs `f` while another thread samples the pool, and returns the highest
/// `used` it saw: `memory::peak` is process-wide, and an earlier test of this
/// binary may already have raised it.
fn peak_used_during<F: FnOnce()>(f: F) -> usize {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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
                std::thread::sleep(std::time::Duration::from_micros(20));
            }
        });
        // Declared after the sampler and before `f`, so a panic in `f` still
        // stops the sampler and the scope can join it.
        let _stop = Stop(Arc::clone(&stop));
        f();
    });
    peak.load(Ordering::Relaxed)
}

/// A scan whose operators stream pins one batch of chunks, not the table: a
/// table four times the limit answers and the pool never passes the limit. A
/// hash-join build is a pipeline breaker and still does not fit.
#[test]
fn a_scan_of_a_table_larger_than_the_limit_streams() {
    let store = Store::new("stream");
    // Every worker decodes one chunk at a time, so the pool must hold a
    // worker's worth of chunks on top of the cache's; the table itself is four
    // times the pool, so the scan has to stream rather than pin its chunks.
    let batch = rayon::current_num_threads().max(1);
    let limit = 16 * batch * CHUNK_SIZE * 8;
    let chunks = 64 * batch;
    let path = store.save("big", chunks);
    let mut db = PinkDb::open(&path).unwrap();
    memory::set_limit(limit);
    assert!(
        chunks * CHUNK_SIZE * 8 >= 4 * limit,
        "the table is not four times the limit"
    );

    let rows = (chunks * CHUNK_SIZE) as i64;
    let hits: Vec<i64> = (0..rows).filter(|i| i % 7 == 0).collect();
    let mut answer = (0, 0, 0, 0);
    let high = peak_used_during(|| {
        answer.0 = int(&mut db, "SELECT count(*) FROM big WHERE i % 7 = 0");
        answer.1 = int(&mut db, "SELECT sum(i) FROM big WHERE i % 7 = 0");
        answer.2 = int(&mut db, "SELECT count(*) FROM big");
        answer.3 = int(&mut db, "SELECT sum(i) FROM big");
    });
    assert_eq!(answer.0, hits.len() as i64);
    assert_eq!(answer.1, hits.iter().sum::<i64>());
    assert_eq!(answer.2, rows);
    assert_eq!(answer.3, range_sum(chunks * CHUNK_SIZE));
    assert!(memory::used() <= limit, "the scan left the pool over it");
    // `used` includes the decoded-chunk cache, which fills to the limit and
    // evicts; the scan's morsels hold one chunk per worker, so a reservation never has
    // to fail for the query to finish.
    assert!(
        high <= limit,
        "the scan pinned {high} bytes, over the {limit} limit"
    );
}

/// A hash-join build reads its whole input before probing, so it is a pipeline
/// breaker: with a build larger than the pool the statement must fail rather
/// than pin the table.
#[test]
fn a_hash_join_build_bigger_than_the_limit_fails() {
    let store = Store::new("join_oom");
    let batch = rayon::current_num_threads().max(1);
    let path = store.save("big", 64 * batch);
    let mut db = PinkDb::open(&path).unwrap();
    memory::set_limit(16 * batch * CHUNK_SIZE * 8);
    let err = match db.execute("SELECT count(*) FROM big a JOIN big b ON a.i = b.i") {
        Ok(_) => panic!("the hash-join build fit in the limit"),
        Err(e) => e.to_string(),
    };
    assert!(err.starts_with("Out of Memory Error"), "{err}");
}
