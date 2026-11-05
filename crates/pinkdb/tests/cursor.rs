//! The streaming result cursor (C1): same rows as a materialized query, pulled
//! chunk by chunk from a snapshot; backpressure, early drop, errors before
//! and during the stream, limits, cancellation, transactions, and a cursor
//! that stays open across other writers' updates and checkpoints.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use pinkdb::attached::PrunePredicate;
use pinkdb::table_source::{TableFunctionCall, TableScanOutput, TableSource};
use pinkdb::{
    CancelFlag, DataChunk, DataType, Database, ExecLimits, PinkDb, PinkDbError, QueryCursor,
    QueryResult, Result, Value, Vector,
};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cursor_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn rows_of(mut cursor: QueryCursor) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    while let Some(chunk) = cursor.next_chunk().unwrap() {
        for i in 0..chunk.num_rows() {
            rows.push(chunk.columns.iter().map(|c| c.get(i)).collect());
        }
    }
    rows
}

fn selected(results: Vec<QueryResult>) -> Vec<Vec<Value>> {
    match results.into_iter().last().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

/// A table source that pushes `chunks` chunks of 1,000 BIGINTs, counting
/// what it pushed; it fails after `fail_after` chunks when set.
#[derive(Debug)]
struct Counting {
    chunks: usize,
    fail_after: Option<usize>,
    pushed: Arc<AtomicUsize>,
}

impl Counting {
    fn chunk(start: usize) -> DataChunk {
        let mut v = Vector::with_capacity(DataType::Bigint, 1000);
        for i in 0..1000 {
            v.push_value(Value::Bigint((start + i) as i64)).unwrap();
        }
        DataChunk::new(vec![v]).unwrap()
    }
}

impl TableSource for Counting {
    fn name(&self) -> &str {
        "counting"
    }

    fn bind(&self, call: &TableFunctionCall) -> Result<Option<Vec<(String, DataType)>>> {
        Ok((call.function_name().as_deref() == Some("counting"))
            .then(|| vec![("n".to_string(), DataType::Bigint)]))
    }

    fn scan(
        &self,
        call: &TableFunctionCall,
        projection: Option<&[usize]>,
        filters: &[PrunePredicate],
    ) -> Result<TableScanOutput> {
        let mut chunks = Vec::new();
        self.scan_stream(call, projection, filters, &mut |c| {
            chunks.push(c);
            Ok(())
        })?;
        Ok(TableScanOutput {
            chunks,
            ..Default::default()
        })
    }

    fn scan_stream(
        &self,
        _call: &TableFunctionCall,
        _projection: Option<&[usize]>,
        _filters: &[PrunePredicate],
        sink: &mut dyn FnMut(DataChunk) -> Result<()>,
    ) -> Result<TableScanOutput> {
        for i in 0..self.chunks {
            if self.fail_after == Some(i) {
                return Err(PinkDbError::Execution("counting source failed".to_string()));
            }
            sink(Counting::chunk(i * 1000))?;
            self.pushed.fetch_add(1, Ordering::SeqCst);
        }
        Ok(TableScanOutput::default())
    }
}

fn with_counting(chunks: usize, fail_after: Option<usize>) -> (PinkDb, Arc<AtomicUsize>) {
    let mut db = PinkDb::new();
    let pushed = Arc::new(AtomicUsize::new(0));
    db.register_table_source(Arc::new(Counting {
        chunks,
        fail_after,
        pushed: pushed.clone(),
    }));
    (db, pushed)
}

#[test]
fn a_cursor_returns_the_rows_execute_returns() {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE t (a INTEGER, b VARCHAR);
         INSERT INTO t SELECT i, 'v' || i FROM range(10000) r(i);",
    )
    .unwrap();
    for sql in [
        "SELECT * FROM t",
        "SELECT a * 2, b FROM t WHERE a % 3 = 0",
        "SELECT * FROM t UNION ALL SELECT * FROM t",
        "SELECT * FROM t LIMIT 5",
        // Blocking plans are evaluated whole, then handed out.
        "SELECT b, count(*) FROM t GROUP BY b ORDER BY b",
        "SELECT * FROM t ORDER BY a DESC",
    ] {
        let want = selected(db.execute(sql).unwrap());
        let cursor = db.query_cursor(sql).unwrap();
        assert!(cursor.has_rows());
        assert_eq!(rows_of(cursor), want, "{sql}");
    }
    // The connection path answers the same.
    let database = Database::from_store(db);
    let mut c = database.connect();
    let want = selected(c.execute("SELECT * FROM t WHERE a < 100").unwrap());
    assert_eq!(
        rows_of(c.query_cursor("SELECT * FROM t WHERE a < 100").unwrap()),
        want
    );
}

#[test]
fn a_statement_without_rows_carries_its_outcome() {
    let mut db = PinkDb::new();
    let mut cursor = db.query_cursor("CREATE TABLE t (a INTEGER)").unwrap();
    assert!(!cursor.has_rows());
    assert!(matches!(cursor.result(), Some(QueryResult::Created)));
    assert!(cursor.next_chunk().unwrap().is_none());
    // RETURNING exists only as rows: it comes back as a row set.
    let cursor = db
        .query_cursor("INSERT INTO t VALUES (1), (2) RETURNING a")
        .unwrap();
    assert_eq!(
        rows_of(cursor),
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
    );
    let err = db.query_cursor("SELECT 1; SELECT 2").unwrap_err();
    assert!(err.to_string().contains("exactly one statement"), "{err}");
}

#[test]
fn a_table_function_streams_with_backpressure_and_stops_when_dropped() {
    // 50,000 chunks (400 MB of BIGINT) from a table function: the cursor
    // buffers its inline phase (16 MB), then the same pipeline continues on a
    // producer behind bounded channels.
    let (mut db, pushed) = with_counting(50_000, None);
    let mut cursor = db.query_cursor("SELECT n FROM counting()").unwrap();
    let first = cursor.next_chunk().unwrap().unwrap();
    assert_eq!(first.columns[0].get(0), Value::Bigint(0));
    std::thread::sleep(std::time::Duration::from_millis(300));
    let ahead = pushed.load(Ordering::SeqCst);
    assert!(ahead < 5_000, "the source ran {ahead} chunks ahead");
    drop(cursor);
    let after_drop = pushed.load(Ordering::SeqCst);
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert_eq!(
        pushed.load(Ordering::SeqCst),
        after_drop,
        "the scan stopped"
    );

    // Drained, it delivers every chunk in order.
    let (mut db, _) = with_counting(50, None);
    let rows = rows_of(
        db.query_cursor("SELECT n FROM counting() WHERE n % 2 = 0")
            .unwrap(),
    );
    assert_eq!(rows.len(), 25_000);
    assert_eq!(rows[1], vec![Value::Bigint(2)]);
    assert_eq!(rows.last().unwrap(), &vec![Value::Bigint(49_998)]);
}

/// A large streaming result crosses from the inline phase to the producer
/// mid-pipeline: every row exactly once, in order, from one evaluation.
#[test]
fn a_large_result_streams_every_row_once_in_order() {
    let (mut db, _) = with_counting(6_000, None);
    let mut cursor = db
        .query_cursor("SELECT n, n * 2 AS d FROM counting() WHERE n % 3 <> 1")
        .unwrap();
    let mut expect = (0..6_000_000i64).filter(|n| n % 3 != 1);
    let mut rows = 0u64;
    while let Some(chunk) = cursor.next_chunk().unwrap() {
        for i in 0..chunk.num_rows() {
            let n = expect.next().expect("no more rows than the source has");
            assert_eq!(chunk.columns[0].get(i), Value::Bigint(n));
            assert_eq!(chunk.columns[1].get(i), Value::Bigint(n * 2));
        }
        rows += chunk.num_rows() as u64;
    }
    assert!(expect.next().is_none(), "every row arrived");
    assert_eq!(rows, 4_000_000);
    assert_eq!(cursor.rows_fetched(), 4_000_000);
}

fn big_table(rows: i64) -> Database {
    let mut db = PinkDb::new();
    db.execute(&format!(
        "CREATE TABLE t (a BIGINT, s VARCHAR);
         INSERT INTO t SELECT i, 'value number ' || i FROM range({rows}) r(i);"
    ))
    .unwrap();
    Database::from_store(db)
}

fn sorted_ints(rows: &[Vec<Value>]) -> Vec<i64> {
    let mut out: Vec<i64> = rows
        .iter()
        .map(|r| match r[0] {
            Value::Bigint(a) => a,
            ref other => panic!("{other:?}"),
        })
        .collect();
    out.sort_unstable();
    out
}

/// Writers that commit while a large cursor is open (deletes, updates and
/// inserts, from another connection) change nothing it returns: no row lost
/// or duplicated where the stream is when they commit.
#[test]
fn concurrent_writes_while_a_large_cursor_streams_change_nothing_it_returns() {
    let database = big_table(1_500_000);
    let mut reader = database.connect();
    let mut writer = database.connect();
    let mut cursor = reader.query_cursor("SELECT a, s FROM t").unwrap();
    let mut rows = Vec::new();
    let take = |cursor: &mut QueryCursor, rows: &mut Vec<Vec<Value>>| {
        let chunk = cursor.next_chunk().unwrap().expect("more rows");
        for i in 0..chunk.num_rows() {
            rows.push(chunk.columns.iter().map(|c| c.get(i)).collect());
        }
    };
    take(&mut cursor, &mut rows);
    writer
        .execute("DELETE FROM t WHERE a % 5 = 0; UPDATE t SET s = 'changed' WHERE a % 5 = 1;")
        .unwrap();
    take(&mut cursor, &mut rows);
    writer
        .execute("INSERT INTO t SELECT i, 'new' FROM range(2000000, 2100000) r(i);")
        .unwrap();
    while let Some(chunk) = cursor.next_chunk().unwrap() {
        for i in 0..chunk.num_rows() {
            rows.push(chunk.columns.iter().map(|c| c.get(i)).collect());
        }
    }
    assert_eq!(sorted_ints(&rows), (0..1_500_000).collect::<Vec<_>>());
    assert!(rows.iter().all(|r| match &r[1] {
        Value::Varchar(s) => s.starts_with("value number "),
        other => panic!("{other:?}"),
    }));
}

/// The table a large cursor is streaming is dropped, and then recreated
/// with other rows, by another connection while the cursor is on its
/// producer: the cursor still returns its snapshot's rows.
#[test]
fn a_large_cursor_survives_its_table_being_dropped_and_recreated() {
    let database = big_table(1_500_000);
    let mut reader = database.connect();
    let mut writer = database.connect();
    let mut cursor = reader.query_cursor("SELECT a, s FROM t").unwrap();
    let mut rows = Vec::new();
    let chunk = cursor.next_chunk().unwrap().unwrap();
    for i in 0..chunk.num_rows() {
        rows.push(chunk.columns.iter().map(|c| c.get(i)).collect());
    }
    writer
        .execute("DROP TABLE t; CREATE TABLE t (a BIGINT, s VARCHAR); INSERT INTO t VALUES (-1, 'other');")
        .unwrap();
    while let Some(chunk) = cursor.next_chunk().unwrap() {
        for i in 0..chunk.num_rows() {
            rows.push(chunk.columns.iter().map(|c| c.get(i)).collect());
        }
    }
    assert_eq!(sorted_ints(&rows), (0..1_500_000).collect::<Vec<_>>());
    assert_eq!(
        selected(writer.execute("SELECT count(*) FROM t").unwrap()),
        vec![vec![Value::Bigint(1)]]
    );
}

/// A parallel filter/projection over a large table returns exactly the
/// multiset the materialized query does, and a `UNION ALL` of two such scans
/// both halves.
#[test]
fn a_parallel_scan_streams_the_exact_multiset() {
    let database = big_table(1_200_000);
    let mut c = database.connect();
    for sql in [
        "SELECT a FROM t WHERE a % 7 IN (1, 3, 4)",
        "SELECT a + 1 FROM t WHERE s LIKE '%9%' UNION ALL SELECT a FROM t WHERE a < 1000",
    ] {
        let want = selected(c.execute(sql).unwrap());
        let got = rows_of(c.query_cursor(sql).unwrap());
        assert_eq!(got.len(), want.len(), "{sql}");
        assert_eq!(sorted_ints(&got), sorted_ints(&want), "{sql}");
    }
}

/// `now()` is one instant for the whole statement and `random()` draws once
/// per row, however the rows are split across chunks and threads.
#[test]
fn now_and_random_are_one_evaluation_across_the_stream() {
    let database = big_table(1_200_000);
    let mut c = database.connect();
    let rows = rows_of(
        c.query_cursor("SELECT a, now() AS t, random() AS r FROM t")
            .unwrap(),
    );
    assert_eq!(sorted_ints(&rows), (0..1_200_000).collect::<Vec<_>>());
    let instant = &rows[0][1];
    assert!(
        rows.iter().all(|r| &r[1] == instant),
        "one now() per statement"
    );
    let mut draws: Vec<u64> = rows
        .iter()
        .map(|r| match r[2] {
            Value::Double(x) => {
                assert!((0.0..1.0).contains(&x));
                x.to_bits()
            }
            ref other => panic!("{other:?}"),
        })
        .collect();
    draws.sort_unstable();
    draws.dedup();
    // One draw per row: a seam that re-evaluated part of the stream would
    // repeat or skip a run of rows, which the multiset check above catches;
    // here the draws are (almost surely) all distinct.
    assert!(draws.len() > 1_199_000, "{} distinct draws", draws.len());
}

#[test]
fn errors_surface_at_open_or_mid_stream() {
    // A failure before the first chunk is the query's error.
    let (mut db, _) = with_counting(10, Some(0));
    let err = db.query_cursor("SELECT * FROM counting()").unwrap_err();
    assert!(err.to_string().contains("counting source failed"), "{err}");

    // A failure within the inline phase (16 MB) is the open's too.
    let (mut db, _) = with_counting(20, Some(12));
    assert!(db.query_cursor("SELECT * FROM counting()").is_err());

    // A failure past it arrives after the chunks before it, then ends the
    // cursor. (4,000 chunks of 1,000 BIGINTs is about 32 MB.)
    let (mut db, _) = with_counting(6_000, Some(4_000));
    let mut cursor = db.query_cursor("SELECT * FROM counting()").unwrap();
    let mut chunks = 0;
    let err = loop {
        match cursor.next_chunk() {
            Ok(Some(_)) => chunks += 1,
            Ok(None) => panic!("the error was lost"),
            Err(e) => break e,
        }
    };
    assert!(
        (1_000..=4_000).contains(&chunks),
        "{chunks} chunks before the error"
    );
    assert!(err.to_string().contains("counting source failed"), "{err}");
    assert!(cursor.next_chunk().unwrap().is_none());

    // A bad cast fails on its first pull, so at open.
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (s VARCHAR); INSERT INTO t VALUES ('x');")
        .unwrap();
    assert!(db.query_cursor("SELECT CAST(s AS INTEGER) FROM t").is_err());
}

#[test]
fn limits_count_streamed_rows_and_a_cancel_stops_the_stream() {
    // A cap within the inline phase fails the open; one past it fails the
    // stream once the streamed rows pass it.
    let (mut db, _) = with_counting(100, None);
    let limits = ExecLimits {
        max_result_rows: Some(2_500),
        ..Default::default()
    };
    let err = db
        .query_cursor_with_limits("SELECT * FROM counting()", limits)
        .unwrap_err();
    assert!(err.to_string().contains("result limit exceeded"), "{err}");
    let (mut db, _) = with_counting(10_000, None);
    let limits = ExecLimits {
        max_result_rows: Some(5_000_000),
        ..Default::default()
    };
    let mut cursor = db
        .query_cursor_with_limits("SELECT * FROM counting()", limits)
        .unwrap();
    let mut rows = 0;
    let err = loop {
        match cursor.next_chunk() {
            Ok(Some(chunk)) => rows += chunk.num_rows(),
            Ok(None) => panic!("the result limit was not applied"),
            Err(e) => break e,
        }
    };
    assert!(rows > 1_000_000 && rows <= 5_000_000, "{rows}");
    assert!(err.to_string().contains("result limit exceeded"), "{err}");

    let (db, _) = with_counting(100_000, None);
    let database = Database::from_store(db);
    let mut c = database.connect();
    let cancel = CancelFlag::new();
    c.set_cancel_flag(Some(cancel.clone()));
    let mut cursor = c.query_cursor("SELECT * FROM counting()").unwrap();
    cursor.next_chunk().unwrap().unwrap();
    cancel.cancel();
    let err = loop {
        match cursor.next_chunk() {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("the cancel was ignored"),
            Err(e) => break e,
        }
    };
    assert!(
        err.to_string().contains(pinkdb::STATEMENT_CANCELLED),
        "{err}"
    );
}

#[test]
fn a_cursor_in_a_transaction_sees_the_transactions_view() {
    let database = Database::new();
    let mut a = database.connect();
    let mut b = database.connect();
    a.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);")
        .unwrap();
    a.execute("BEGIN; INSERT INTO t VALUES (2);").unwrap();
    let rows = rows_of(a.query_cursor("SELECT a FROM t ORDER BY a").unwrap());
    assert_eq!(rows, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    // Another session does not see the uncommitted row.
    let rows = rows_of(b.query_cursor("SELECT a FROM t").unwrap());
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);
    a.execute("COMMIT").unwrap();
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// A cursor reads the snapshot it opened on: other connections and other
/// handles of the same file update the table and checkpoint twice, in place
/// (freed ranges reused), and the cursor still returns its snapshot's rows.
#[test]
fn an_open_cursor_keeps_its_snapshot_across_updates_and_checkpoints() {
    let dir = scratch("checkpoints");
    let path = dir.join("store.pink");
    let path = path.to_str().unwrap();
    // Large enough (past the 16 MB inline bound) that the cursor moves to its
    // producer, which reads the snapshot while the writers checkpoint.
    let rows = 1_500_000i64;
    {
        let mut db = PinkDb::open_durable(path).unwrap();
        db.execute(&format!(
            "CREATE TABLE t (a BIGINT, b VARCHAR);
             INSERT INTO t SELECT i, 'row-' || i FROM range({rows}) r(i);
             CHECKPOINT;"
        ))
        .unwrap();
    }
    let database = Database::open(path).unwrap();
    let mut reader = database.connect();
    let mut writer = database.connect();
    // A second handle of the same file, as another process would be.
    let mut other = PinkDb::open_durable(path).unwrap();

    let mut cursor = reader.query_cursor("SELECT a, b FROM t").unwrap();
    let mut seen: Vec<(i64, String)> = Vec::new();
    let take = |cursor: &mut QueryCursor, seen: &mut Vec<(i64, String)>| -> bool {
        match cursor.next_chunk().unwrap() {
            Some(chunk) => {
                for i in 0..chunk.num_rows() {
                    let (Value::Bigint(a), Value::Varchar(b)) =
                        (chunk.columns[0].get(i), chunk.columns[1].get(i))
                    else {
                        panic!("unexpected row");
                    };
                    seen.push((a, b));
                }
                true
            }
            None => false,
        }
    };
    assert!(take(&mut cursor, &mut seen));
    for round in 0..2 {
        writer
            .execute("UPDATE t SET a = a + 1000000, b = b || '-new'; CHECKPOINT;")
            .unwrap();
        other
            .execute(&format!("DELETE FROM t WHERE a % 7 = {round}; CHECKPOINT;"))
            .unwrap();
        assert!(take(&mut cursor, &mut seen));
    }
    while take(&mut cursor, &mut seen) {}
    drop(cursor);
    assert_eq!(seen.len() as i64, rows);
    seen.sort();
    for (i, (a, b)) in seen.iter().enumerate() {
        assert_eq!(*a, i as i64);
        assert_eq!(b, &format!("row-{i}"));
    }
    // The writers' work is all there.
    let n = selected(
        reader
            .execute("SELECT count(*) FROM t WHERE b LIKE '%-new-new'")
            .unwrap(),
    );
    assert!(matches!(n[0][0], Value::Bigint(c) if c > 0));
    drop(other);
    drop(reader);
    drop(writer);
    drop(database);
    cleanup(&dir);
}
