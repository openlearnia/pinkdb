//! Wave 140c: `CREATE TABLE AS` and `INSERT … SELECT` append whole chunks.
//!
//! Both used to stage one `Vec<Value>` per row and call `append_row`. They now
//! append ≤ `CHUNK_SIZE`-row batches, sharing a source column whose type
//! matches the target and coercing value by value only a column whose type
//! differs, so this file pins that the rows, the catalog statistics and the
//! zone maps are the ones the row path produces — and that every case which
//! needs per-row work keeps the row path and behaves as before.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{DataType, PinkDb, QueryResult, SyncPolicy, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
    file: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_bulk_append_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("db.pink");
        Self { dir, file }
    }

    fn path(&self) -> &str {
        self.file.to_str().unwrap()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
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

fn fail(db: &mut PinkDb, sql: &str) -> String {
    match db.execute(sql) {
        Ok(r) => panic!("expected {sql} to fail, got {r:?}"),
        Err(e) => format!("{e}"),
    }
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Bigint(n) => *n,
        Value::Integer(n) => *n as i64,
        // SUM returns HUGEINT; these fixtures only sum small values.
        Value::Hugeint(n) => i64::try_from(*n).expect("fits in i64"),
        other => panic!("expected an integer, got {other:?}"),
    }
}

fn opt_int(v: &Value) -> Option<i64> {
    match v {
        Value::Null => None,
        other => Some(int(other)),
    }
}

/// Statistics render values in the type they were stored in; the SQL side of a
/// comparison goes through the same rendering.
fn text(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Varchar(s) => Some(s.clone()),
        other => Some(format!("{other:?}")),
    }
}

fn one(db: &mut PinkDb, sql: &str) -> Vec<Value> {
    rows(db, sql).pop().unwrap()
}

fn selected(db: &mut PinkDb, sql: &str) -> (Vec<String>, Vec<DataType>, Vec<Vec<Value>>) {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { names, types, rows } => (names, types, rows),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// One row of `pinkdb_column_stats()`, without the table (compared across two
/// tables holding the same data).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stat {
    column: String,
    row_count: i64,
    null_count: i64,
    distinct: Option<i64>,
    min: Option<String>,
    max: Option<String>,
    approximate: bool,
}

fn stats(db: &mut PinkDb, table: &str) -> Vec<Stat> {
    rows(
        db,
        &format!(
            "SELECT column_name, row_count, null_count, distinct_count, min, max, \
             stats_approximate FROM pinkdb_column_stats('{table}')"
        ),
    )
    .into_iter()
    .map(|r| Stat {
        column: match &r[0] {
            Value::Varchar(s) => s.clone(),
            other => panic!("{other:?}"),
        },
        row_count: int(&r[1]),
        null_count: int(&r[2]),
        distinct: opt_int(&r[3]),
        min: text(&r[4]),
        max: text(&r[5]),
        approximate: match &r[6] {
            Value::Boolean(b) => *b,
            other => panic!("{other:?}"),
        },
    })
    .collect()
}

/// The same rows in both tables, duplicates included.
fn assert_same_rows(db: &mut PinkDb, a: &str, b: &str) {
    for (x, y) in [(a, b), (b, a)] {
        let diff = rows(
            db,
            &format!("SELECT * FROM {x} EXCEPT ALL SELECT * FROM {y}"),
        );
        assert!(
            diff.is_empty(),
            "{x} and {y} differ, first: {:?}",
            diff.first()
        );
    }
}

/// Every column's row count, NULL count, min and max are the exact aggregates;
/// the distinct estimate is within the sketch's error.
fn assert_stats_exact(db: &mut PinkDb, table: &str) {
    for st in stats(db, table) {
        let c = &st.column;
        let r = one(
            db,
            &format!(
                "SELECT count(*), count({c}), count(DISTINCT {c}), \
                 CAST(min({c}) AS VARCHAR), CAST(max({c}) AS VARCHAR) FROM {table}"
            ),
        );
        assert_eq!(st.row_count, int(&r[0]), "{table}.{c} rows");
        assert_eq!(st.null_count, int(&r[0]) - int(&r[1]), "{table}.{c} nulls");
        assert_eq!(st.min, text(&r[3]), "{table}.{c} min");
        assert_eq!(st.max, text(&r[4]), "{table}.{c} max");
        assert!(!st.approximate, "{table}.{c} approximate");
        let exact = int(&r[2]);
        let estimate = st.distinct.expect("sketch");
        assert!(
            (estimate - exact).unsigned_abs() <= (exact / 20) as u64 + 2,
            "{table}.{c}: HLL estimate {estimate} vs exact {exact}"
        );
    }
}

/// Four chunks (7000 rows) of INTEGER, VARCHAR, DATE and DOUBLE with NULLs in
/// every other chunk, plus a unique id so a keyed table can replay the same
/// rows through the row path.
fn seed_source(db: &mut PinkDb) {
    ok(
        db,
        "CREATE TABLE src (id INTEGER, a INTEGER, b VARCHAR, c DATE, d DOUBLE);",
    );
    ok(
        db,
        "INSERT INTO src SELECT \
           CAST(i AS INTEGER), \
           CASE WHEN i % 7 = 0 THEN NULL ELSE CAST(i AS INTEGER) END, \
           CASE WHEN i % 5 = 0 THEN NULL ELSE CAST(i % 401 AS VARCHAR) END, \
           DATE '2024-01-01' + CAST(i % 400 AS INTEGER), \
           CASE WHEN i % 9 = 0 THEN NULL ELSE CAST(i AS DOUBLE) / 7 END \
         FROM range(0, 7000) t(i);",
    );
}

#[test]
fn ctas_and_insert_select_match_the_row_path() {
    let mut db = PinkDb::new();
    seed_source(&mut db);

    // The bulk CTAS.
    ok(&mut db, "CREATE TABLE ct AS SELECT * FROM src;");
    // The row path for the same rows: a declared key sends the insert through
    // `insert_rows`, one `append_row` per row.
    ok(
        &mut db,
        "CREATE TABLE rw (id INTEGER PRIMARY KEY, a INTEGER, b VARCHAR, c DATE, d DOUBLE);",
    );
    ok(&mut db, "INSERT INTO rw SELECT * FROM src;");
    // The bulk `INSERT … SELECT`.
    ok(
        &mut db,
        "CREATE TABLE ins (id INTEGER, a INTEGER, b VARCHAR, c DATE, d DOUBLE);",
    );
    ok(&mut db, "INSERT INTO ins SELECT * FROM src;");

    for table in ["ct", "rw", "ins"] {
        assert_eq!(
            int(&one(&mut db, &format!("SELECT count(*) FROM {table}"))[0]),
            7000
        );
    }
    assert_same_rows(&mut db, "ct", "rw");
    assert_same_rows(&mut db, "ins", "rw");
    // Whole-chunk appends leave the statistics (and so the zone maps) of the
    // row path: same rows, NULLs, min/max and sketch registers.
    assert_eq!(stats(&mut db, "ct"), stats(&mut db, "rw"));
    assert_eq!(stats(&mut db, "ins"), stats(&mut db, "rw"));
    assert_stats_exact(&mut db, "ct");
    assert_stats_exact(&mut db, "ins");

    // An aggregate input (a chunk shape that is not a scan's) lands intact.
    ok(
        &mut db,
        "CREATE TABLE groups AS SELECT a.id, count(*) AS n FROM src a GROUP BY a.id;",
    );
    assert_eq!(int(&one(&mut db, "SELECT sum(n) FROM groups")[0]), 7000);
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM groups")[0]), 7000);
    assert_stats_exact(&mut db, "groups");
}

#[test]
fn ctas_with_if_not_exists_and_or_replace() {
    let mut db = PinkDb::new();
    seed_source(&mut db);
    ok(
        &mut db,
        "CREATE TABLE IF NOT EXISTS ct AS SELECT * FROM src LIMIT 10;",
    );
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM ct")[0]), 10);
    // Existing table: the query must not run at all and the table keeps its
    // rows.
    ok(
        &mut db,
        "CREATE TABLE IF NOT EXISTS ct AS SELECT * FROM src;",
    );
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM ct")[0]), 10);
    // OR REPLACE evaluates the query before dropping the old table.
    ok(&mut db, "CREATE OR REPLACE TABLE ct AS SELECT * FROM ct;");
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM ct")[0]), 10);
    // An empty query result still creates the table with the query's schema.
    ok(&mut db, "CREATE TABLE empty AS SELECT * FROM src LIMIT 0;");
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM empty")[0]), 0);
    let st = stats(&mut db, "empty");
    assert_eq!(st.len(), 5);
    assert!(st.iter().all(|s| s.row_count == 0 && s.null_count == 0));
}

#[test]
fn insert_select_column_list_takes_the_defaults() {
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE s (a INTEGER, b VARCHAR);");
    ok(
        &mut db,
        "INSERT INTO s SELECT CAST(i AS INTEGER), 'v' || CAST(i % 5 AS VARCHAR) \
         FROM range(0, 5000) t(i);",
    );
    // `a` is reordered after `b`, and widened from INTEGER to BIGINT; `c`/`d`
    // take their DEFAULT, `e` (no DEFAULT) takes NULL.
    ok(
        &mut db,
        "CREATE TABLE t (a BIGINT, b VARCHAR, c DOUBLE DEFAULT 2.5, \
         d VARCHAR DEFAULT 'dflt', e INTEGER);",
    );
    ok(&mut db, "INSERT INTO t (b, a) SELECT b, a FROM s;");

    let r = one(
        &mut db,
        "SELECT count(*), count(DISTINCT a), count(DISTINCT b), count(DISTINCT c), \
         min(c), max(c), count(DISTINCT d), count(e), min(a), max(a) FROM t",
    );
    assert_eq!(int(&r[0]), 5000);
    assert_eq!(int(&r[1]), 5000);
    assert_eq!(int(&r[2]), 5);
    assert_eq!(int(&r[3]), 1);
    assert_eq!(text(&r[4]), Some("Double(2.5)".to_string()));
    assert_eq!(text(&r[5]), Some("Double(2.5)".to_string()));
    assert_eq!(int(&r[6]), 1);
    assert_eq!(int(&r[7]), 0);
    assert_eq!(text(&r[8]), Some("Bigint(0)".to_string()));
    assert_eq!(text(&r[9]), Some("Bigint(4999)".to_string()));
    // `c` is a DOUBLE and `d` a VARCHAR, the declared types, not the
    // literals' own.
    assert_eq!(
        rows(&mut db, "SELECT c, d, e FROM t LIMIT 1"),
        vec![vec![
            Value::Double(2.5),
            Value::Varchar("dflt".to_string()),
            Value::Null
        ]]
    );
    let sample = rows(
        &mut db,
        "SELECT a, b, c, d, e FROM t WHERE a IN (0, 1234) ORDER BY a",
    );
    assert_eq!(
        sample,
        vec![
            vec![
                Value::Bigint(0),
                Value::Varchar("v0".to_string()),
                Value::Double(2.5),
                Value::Varchar("dflt".to_string()),
                Value::Null,
            ],
            vec![
                Value::Bigint(1234),
                Value::Varchar("v4".to_string()),
                Value::Double(2.5),
                Value::Varchar("dflt".to_string()),
                Value::Null,
            ],
        ]
    );
    assert_stats_exact(&mut db, "t");
}

#[test]
fn widening_and_a_rejected_value_leave_the_table_right() {
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE s (i INTEGER);");
    ok(
        &mut db,
        "INSERT INTO s SELECT CAST(i AS INTEGER) FROM range(0, 5000) t(i);",
    );
    ok(&mut db, "CREATE TABLE to_big (b BIGINT);");
    ok(&mut db, "INSERT INTO to_big SELECT * FROM s;");
    ok(&mut db, "CREATE TABLE to_double (d DOUBLE);");
    ok(&mut db, "INSERT INTO to_double SELECT * FROM s;");

    // Widened value by value into the target's own type.
    assert_eq!(
        rows(&mut db, "SELECT b FROM to_big ORDER BY b LIMIT 1"),
        vec![vec![Value::Bigint(0)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT b FROM to_big ORDER BY b DESC LIMIT 1"),
        vec![vec![Value::Bigint(4999)]]
    );
    let r = one(&mut db, "SELECT sum(b), count(DISTINCT b) FROM to_big");
    assert_eq!(int(&r[0]), 12497500);
    assert_eq!(int(&r[1]), 5000);
    assert_eq!(
        rows(&mut db, "SELECT d FROM to_double ORDER BY d LIMIT 1"),
        vec![vec![Value::Double(0.0)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT d FROM to_double ORDER BY d DESC LIMIT 1"),
        vec![vec![Value::Double(4999.0)]]
    );
    assert_eq!(
        text(&one(&mut db, "SELECT sum(d) FROM to_double")[0]),
        Some("Double(12497500.0)".to_string())
    );
    assert_stats_exact(&mut db, "to_big");
    assert_stats_exact(&mut db, "to_double");

    // A VARCHAR cell that does not parse fails, exactly as the row path does;
    // the bad row sits in the last chunk, so a batch-at-a-time append would
    // have left the first chunk behind.
    ok(&mut db, "CREATE TABLE bad (n INTEGER);");
    ok(&mut db, "INSERT INTO bad VALUES (1);");
    ok(&mut db, "CREATE TABLE vs (v VARCHAR);");
    ok(
        &mut db,
        "INSERT INTO vs SELECT CASE WHEN i = 2999 THEN 'x' ELSE CAST(i AS VARCHAR) END \
         FROM range(0, 3000) t(i);",
    );
    let before = stats(&mut db, "bad");
    let err = fail(&mut db, "INSERT INTO bad SELECT v FROM vs;");
    assert_eq!(err, "cannot CAST \"x\" AS INTEGER");
    assert_eq!(stats(&mut db, "bad"), before);
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM bad")[0]), 1);
}

#[test]
fn keys_row_triggers_and_conflicts_keep_the_row_path() {
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE s (id INTEGER, v VARCHAR);");
    ok(
        &mut db,
        "INSERT INTO s SELECT CAST(i AS INTEGER), 'v' || CAST(i AS VARCHAR) \
         FROM range(0, 3000) t(i);",
    );

    // A declared key: checked through the cached key map, duplicates rejected.
    ok(
        &mut db,
        "CREATE TABLE k (id INTEGER PRIMARY KEY, v VARCHAR);",
    );
    ok(&mut db, "INSERT INTO k SELECT * FROM s;");
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM k")[0]), 3000);
    let err = fail(&mut db, "INSERT INTO k SELECT * FROM s;");
    assert!(err.contains("duplicate key"), "{err}");
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM k")[0]), 3000);

    // A row trigger fires once per inserted row.
    ok(&mut db, "CREATE TABLE log (id INTEGER);");
    ok(&mut db, "CREATE TABLE t (id INTEGER, v VARCHAR);");
    ok(
        &mut db,
        "CREATE TRIGGER t_row AFTER INSERT ON t FOR EACH ROW \
         EXECUTE INSERT INTO log VALUES (NEW.id);",
    );
    ok(&mut db, "INSERT INTO t SELECT * FROM s;");
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM t")[0]), 3000);
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM log")[0]), 3000);

    // RETURNING installs the capture, so it keeps the row path — and now also
    // reports more rows than one chunk holds.
    ok(&mut db, "CREATE TABLE ret (id INTEGER, v VARCHAR);");
    let returned = match db
        .execute("INSERT INTO ret SELECT * FROM s RETURNING v;")
        .unwrap()
        .pop()
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected RETURNING rows, got {other:?}"),
    };
    assert_eq!(returned.len(), 3000);
    assert_eq!(returned[0], vec![Value::Varchar("v0".to_string())]);
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM ret")[0]), 3000);

    // ON CONFLICT DO NOTHING skips the whole existing set.
    ok(
        &mut db,
        "CREATE TABLE u (id INTEGER PRIMARY KEY, v VARCHAR);",
    );
    ok(&mut db, "INSERT INTO u VALUES (1, 'one');");
    ok(
        &mut db,
        "INSERT INTO u SELECT * FROM s ON CONFLICT (id) DO NOTHING;",
    );
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM u")[0]), 3000);
    assert_eq!(
        rows(&mut db, "SELECT v FROM u WHERE id = 1"),
        vec![vec![Value::Varchar("one".to_string())]]
    );
}

#[test]
fn a_statement_trigger_still_fires_around_the_bulk_append() {
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE log (marker VARCHAR);");
    ok(&mut db, "CREATE TABLE t (id INTEGER);");
    ok(
        &mut db,
        "CREATE TRIGGER t_statement AFTER INSERT ON t FOR EACH STATEMENT \
         EXECUTE INSERT INTO log VALUES ('after');",
    );
    ok(
        &mut db,
        "INSERT INTO t SELECT CAST(i AS INTEGER) FROM range(0, 3000) t(i);",
    );
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM t")[0]), 3000);
    assert_eq!(
        rows(&mut db, "SELECT marker FROM log"),
        vec![vec![Value::Varchar("after".to_string())]]
    );
    assert_eq!(
        int(&one(&mut db, "SELECT count(*) FROM t WHERE id = 2999")[0]),
        1
    );
}

/// A bulk append shares the query's vectors with the new table; a later write
/// to either table must not reach the other (every mutation copies on write).
#[test]
fn a_bulk_append_does_not_alias_the_source_table() {
    let mut db = PinkDb::new();
    seed_source(&mut db);
    ok(&mut db, "CREATE TABLE copy AS SELECT * FROM src;");
    ok(
        &mut db,
        "CREATE TABLE ins (id INTEGER, a INTEGER, b VARCHAR, c DATE, d DOUBLE);",
    );
    ok(&mut db, "INSERT INTO ins SELECT * FROM src;");
    let copied = one(&mut db, "SELECT sum(a), sum(d), count(*) FROM copy");
    let inserted = one(&mut db, "SELECT sum(a), sum(d), count(*) FROM ins");

    ok(&mut db, "UPDATE src SET a = -1 WHERE id < 10;");
    ok(&mut db, "DELETE FROM src WHERE id >= 6990;");
    ok(&mut db, "INSERT INTO src (id, a) VALUES (70000, 5);");
    assert_eq!(
        one(&mut db, "SELECT sum(a), sum(d), count(*) FROM copy"),
        copied
    );
    assert_eq!(
        one(&mut db, "SELECT sum(a), sum(d), count(*) FROM ins"),
        inserted
    );

    // The other direction: writing the appended tables leaves the source.
    let source = one(&mut db, "SELECT sum(a), sum(d), count(*) FROM src");
    ok(&mut db, "UPDATE copy SET d = -2.5 WHERE id < 70;");
    ok(&mut db, "DELETE FROM ins WHERE id < 70;");
    assert_eq!(
        one(&mut db, "SELECT sum(a), sum(d), count(*) FROM src"),
        source
    );
}

#[test]
fn returning_more_than_a_chunk() {
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE s (id INTEGER, v VARCHAR);");
    ok(
        &mut db,
        "INSERT INTO s SELECT CAST(i AS INTEGER), 'v' || CAST(i AS VARCHAR) \
         FROM range(0, 5000) t(i);",
    );
    let expected: Vec<Vec<Value>> = (0..5000)
        .map(|i| vec![Value::Integer(i), Value::Varchar(format!("v{i}"))])
        .collect();

    // INSERT … SELECT … RETURNING: `RETURNING` keeps the row path.
    ok(&mut db, "CREATE TABLE ins (id INTEGER, v VARCHAR);");
    let (names, types, got) = selected(&mut db, "INSERT INTO ins SELECT * FROM s RETURNING id, v");
    assert_eq!(names, ["id".to_string(), "v".to_string()]);
    assert_eq!(types, [DataType::Integer, DataType::Varchar]);
    assert_eq!(got, expected);

    let (_, _, got) = selected(&mut db, "UPDATE ins SET v = v RETURNING id, v");
    assert_eq!(got, expected);

    let (_, _, got) = selected(&mut db, "DELETE FROM ins RETURNING id, v");
    assert_eq!(got, expected);
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM ins")[0]), 0);

    // MERGE … RETURNING: every target row matches, so all 5000 are updated.
    ok(&mut db, "CREATE TABLE ms (id INTEGER, v VARCHAR);");
    ok(&mut db, "INSERT INTO ms SELECT * FROM s;");
    let (_, _, got) = selected(
        &mut db,
        "MERGE INTO ms AS t USING s AS x ON t.id = x.id \
         WHEN MATCHED THEN UPDATE SET v = x.v RETURNING id, v",
    );
    assert_eq!(got, expected);

    // A data-modifying CTE carries the same rows through its own output.
    assert_eq!(
        one(
            &mut db,
            "WITH d AS (DELETE FROM ms RETURNING *) \
             SELECT count(*), min(id), max(id) FROM d",
        ),
        vec![Value::Bigint(5000), Value::Integer(0), Value::Integer(4999)]
    );
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM ms")[0]), 0);
}

/// A transition table is a `RETURNING`-shaped output of the same rows: it must
/// hold more rows than one chunk too.
#[test]
fn a_transition_table_holds_more_than_a_chunk() {
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE t (i INTEGER);");
    ok(&mut db, "CREATE TABLE log (n BIGINT, s BIGINT);");
    ok(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t REFERENCING NEW TABLE AS n FOR EACH STATEMENT \
         EXECUTE INSERT INTO log SELECT count(*), sum(i) FROM n;",
    );
    ok(
        &mut db,
        "INSERT INTO t SELECT CAST(i AS INTEGER) FROM range(0, 5000) r(i);",
    );
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM t")[0]), 5000);
    assert_eq!(
        rows(&mut db, "SELECT n, s FROM log"),
        vec![vec![Value::Bigint(5000), Value::Bigint(12_497_500)]]
    );
}

#[test]
fn durable_store_replays_ctas_and_bulk_insert() {
    let scratch = Scratch::new("durable");
    let mut db = PinkDb::open_durable_with(scratch.path(), SyncPolicy::Batch)
        .unwrap()
        .0;
    seed_source(&mut db);
    ok(&mut db, "CREATE TABLE ct AS SELECT * FROM src;");
    ok(
        &mut db,
        "CREATE TABLE ins (id INTEGER, a INTEGER, b VARCHAR, c DATE, d DOUBLE);",
    );
    ok(&mut db, "INSERT INTO ins SELECT * FROM src;");
    let expected = stats(&mut db, "ct");
    assert_eq!(stats(&mut db, "ins"), expected);
    // Drop without a checkpoint: reopening replays the WAL, which stores each
    // statement's text (the rows are recomputed, not read back).
    drop(db);
    let mut db = PinkDb::open_durable_with(scratch.path(), SyncPolicy::Batch)
        .unwrap()
        .0;
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM ct")[0]), 7000);
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM ins")[0]), 7000);
    assert_same_rows(&mut db, "ct", "ins");
    assert_eq!(stats(&mut db, "ct"), expected);
    assert_stats_exact(&mut db, "ct");
}

// Wave 140d: MERGE's `WHEN NOT MATCHED` inserts append whole batches.

/// QuackLab's ETL shape: 7000 rows (four chunks) over VARCHAR, DATE and BIGINT
/// key columns, so the `ON` probes by key and the insert list is wide.
const MERGE_SOURCE: &str = "CREATE TABLE f AS SELECT \
     CAST(i % 300 AS VARCHAR) AS zone_id, \
     CAST(i % 263 AS VARCHAR) AS do_zone_id, \
     DATE '2024-07-01' + CAST(i % 31 AS INTEGER) AS day_id, \
     CAST(i % 24 AS BIGINT) AS hour_id, \
     CAST(i % 5 AS BIGINT) AS payment_type_id, \
     CAST(i % 3 AS BIGINT) AS vendor_id, \
     CAST(i % 7 AS VARCHAR) AS rate_code_id, \
     CAST(i AS BIGINT) AS trips, \
     CAST(i AS DOUBLE) * 1.5 AS fare \
     FROM range(0, 7000) t(i)";

/// The repro's insert column list, for `f` as source and `target` as target.
const MERGE_INSERT: &str = "INSERT (zone_id, do_zone_id, day_id, hour_id, \
     payment_type_id, vendor_id, rate_code_id, trips, fare) \
     VALUES (source.zone_id, source.do_zone_id, source.day_id, source.hour_id, \
       source.payment_type_id, source.vendor_id, source.rate_code_id, \
       source.trips, source.fare)";

/// The repro's target, keyed on `trips` so every insert goes through
/// `insert_rows`/`append_row`: the same rows by the row path.
fn keyed_merge_target(db: &mut PinkDb, name: &str) {
    ok(
        db,
        &format!(
            "CREATE TABLE {name} (zone_id VARCHAR, do_zone_id VARCHAR, day_id DATE, \
             hour_id BIGINT, payment_type_id BIGINT, vendor_id BIGINT, \
             rate_code_id VARCHAR, trips BIGINT PRIMARY KEY, fare DOUBLE);"
        ),
    );
}

#[test]
fn merge_inserts_append_whole_batches() {
    let mut db = PinkDb::new();
    ok(&mut db, MERGE_SOURCE);
    ok(&mut db, "CREATE TABLE bulk AS SELECT * FROM f LIMIT 0;");
    keyed_merge_target(&mut db, "rw");
    // Every source row is unmatched, so the whole statement is inserts.
    let all = format!(
        "MERGE INTO {{t}} AS target USING f AS source \
         ON (target.zone_id = source.zone_id \
             AND target.do_zone_id = source.do_zone_id \
             AND target.day_id = source.day_id AND target.hour_id = source.hour_id \
             AND target.payment_type_id = source.payment_type_id \
             AND target.vendor_id = source.vendor_id \
             AND target.rate_code_id = source.rate_code_id) \
         WHEN NOT MATCHED THEN {MERGE_INSERT};"
    );
    ok(&mut db, &all.replace("{t}", "bulk"));
    ok(&mut db, &all.replace("{t}", "rw"));
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM bulk")[0]), 7000);
    // The same rows in the same positions: both append in source order.
    assert_eq!(
        rows(&mut db, "SELECT * FROM bulk"),
        rows(&mut db, "SELECT * FROM rw")
    );
    assert_eq!(stats(&mut db, "bulk"), stats(&mut db, "rw"));
    assert_stats_exact(&mut db, "bulk");

    // Half filled: the even `trips` match and update, the odd ones append
    // behind three chunks, so the first batch straddles a chunk seam. The `ON`
    // is unique here, since the repro's seven keys are not.
    ok(
        &mut db,
        "CREATE TABLE half AS SELECT * FROM f WHERE trips % 2 = 0;",
    );
    keyed_merge_target(&mut db, "half_rw");
    ok(
        &mut db,
        "INSERT INTO half_rw SELECT * FROM f WHERE trips % 2 = 0;",
    );
    let mixed = format!(
        "MERGE INTO {{t}} AS target USING f AS source \
         ON (target.trips = source.trips) \
         WHEN MATCHED THEN UPDATE SET fare = source.fare + 1 \
         WHEN NOT MATCHED THEN {MERGE_INSERT};"
    );
    ok(&mut db, &mixed.replace("{t}", "half"));
    ok(&mut db, &mixed.replace("{t}", "half_rw"));
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM half")[0]), 7000);
    assert_eq!(
        rows(&mut db, "SELECT * FROM half"),
        rows(&mut db, "SELECT * FROM half_rw")
    );
    // The updates above leave the sketches upper bounds; rebuilding them from
    // the rows makes the comparison exact for the appended batches too.
    ok(&mut db, "ANALYZE half; ANALYZE half_rw;");
    assert_eq!(stats(&mut db, "half"), stats(&mut db, "half_rw"));
    assert_stats_exact(&mut db, "half");

    // The appends share `f`'s vectors; a later write to either table must not
    // reach the other.
    let source = one(&mut db, "SELECT sum(trips), sum(fare), count(*) FROM f");
    ok(&mut db, "UPDATE half SET fare = -2.5 WHERE trips < 10;");
    ok(&mut db, "DELETE FROM bulk WHERE trips >= 6990;");
    assert_eq!(
        one(&mut db, "SELECT sum(trips), sum(fare), count(*) FROM f"),
        source
    );
}

/// Several `WHEN NOT MATCHED` clauses keep the row path and stage clause by
/// clause: the first clause's rows land before the second's.
#[test]
fn several_not_matched_clauses_keep_the_row_path_and_source_order() {
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE t (a INTEGER, b INTEGER);");
    ok(&mut db, "CREATE TABLE s (a INTEGER, b INTEGER);");
    ok(
        &mut db,
        "INSERT INTO s VALUES (1, 10), (2, 20), (3, 30), (4, 40);",
    );
    ok(
        &mut db,
        "MERGE INTO t AS target USING s AS source ON target.a = source.a \
         WHEN NOT MATCHED AND source.b > 25 THEN INSERT (a, b) VALUES (source.a, source.b) \
         WHEN NOT MATCHED THEN INSERT (a, b) VALUES (source.a, source.b + 1000);",
    );
    assert_eq!(
        rows(&mut db, "SELECT a, b FROM t"),
        vec![
            vec![Value::Integer(3), Value::Integer(30)],
            vec![Value::Integer(4), Value::Integer(40)],
            vec![Value::Integer(1), Value::Integer(1010)],
            vec![Value::Integer(2), Value::Integer(1020)],
        ]
    );
}

/// The batch path takes the unlisted columns' defaults and widens a value that
/// does not match its column, exactly as the row path does.
#[test]
fn merge_inserts_take_defaults_and_widen_columns() {
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE s (a INTEGER, b VARCHAR);");
    ok(
        &mut db,
        "INSERT INTO s SELECT CAST(i AS INTEGER), 'v' || CAST(i % 5 AS VARCHAR) \
         FROM range(0, 7000) t(i);",
    );
    // `a` widens INTEGER into BIGINT; `tag` and `n` are unlisted defaults.
    ok(
        &mut db,
        "CREATE TABLE bulk (a BIGINT, b VARCHAR, tag VARCHAR DEFAULT 'dflt', \
         n INTEGER DEFAULT 7);",
    );
    ok(
        &mut db,
        "CREATE TABLE rw (a BIGINT PRIMARY KEY, b VARCHAR, tag VARCHAR DEFAULT 'dflt', \
         n INTEGER DEFAULT 7);",
    );
    let merge = "MERGE INTO {t} AS target USING s AS source ON target.a = source.a \
                 WHEN NOT MATCHED THEN INSERT (a, b) VALUES (source.a, source.b);";
    ok(&mut db, &merge.replace("{t}", "bulk"));
    ok(&mut db, &merge.replace("{t}", "rw"));
    assert_eq!(
        one(
            &mut db,
            "SELECT count(*), count(DISTINCT a), min(a), max(a), count(DISTINCT tag), \
             min(n), max(n) FROM bulk"
        ),
        vec![
            Value::Bigint(7000),
            Value::Bigint(7000),
            Value::Bigint(0),
            Value::Bigint(6999),
            Value::Bigint(1),
            Value::Integer(7),
            Value::Integer(7),
        ]
    );
    assert_eq!(
        rows(&mut db, "SELECT * FROM bulk"),
        rows(&mut db, "SELECT * FROM rw")
    );
    assert_eq!(stats(&mut db, "bulk"), stats(&mut db, "rw"));
    assert_stats_exact(&mut db, "bulk");
}

/// A value the target column cannot take fails the statement after every
/// source row was staged, so nothing is appended — including the rows before
/// the bad one, which sit in earlier batches.
#[test]
fn merge_insert_coercion_failure_leaves_the_table_untouched() {
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE t (a INTEGER, d DATE);");
    ok(&mut db, "INSERT INTO t VALUES (1, DATE '2024-01-01');");
    ok(&mut db, "CREATE TABLE s (a INTEGER, v VARCHAR);");
    ok(
        &mut db,
        "INSERT INTO s SELECT CAST(i AS INTEGER), \
           CASE WHEN i = 2999 THEN 'zzz' \
                ELSE CAST(DATE '2024-01-01' + CAST(i % 400 AS INTEGER) AS VARCHAR) END \
         FROM range(0, 3000) t(i);",
    );
    let before = stats(&mut db, "t");
    let err = fail(
        &mut db,
        "MERGE INTO t AS target USING s AS source ON target.a = source.a \
         WHEN NOT MATCHED THEN INSERT (a, d) VALUES (source.a, source.v);",
    );
    assert_eq!(err, "cannot CAST \"zzz\" AS DATE");
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM t")[0]), 1);
    assert_eq!(stats(&mut db, "t"), before);
}

/// A keyed target, a row trigger and `RETURNING` each keep the row path, over
/// more rows than one chunk holds.
#[test]
fn merge_keys_triggers_and_returning_keep_the_row_path() {
    let mut db = PinkDb::new();
    ok(&mut db, "CREATE TABLE s (id INTEGER, v VARCHAR);");
    ok(
        &mut db,
        "INSERT INTO s SELECT CAST(i AS INTEGER), 'v' || CAST(i AS VARCHAR) \
         FROM range(0, 3000) t(i);",
    );

    // A row trigger fires once per inserted row and rewrites it.
    ok(&mut db, "CREATE TABLE t (id INTEGER, v VARCHAR);");
    ok(
        &mut db,
        "CREATE TRIGGER t_before BEFORE INSERT ON t FOR EACH ROW \
         EXECUTE DO $$ BEGIN SET NEW.v = NEW.v || '!'; END; $$",
    );
    ok(
        &mut db,
        "MERGE INTO t AS target USING s AS source ON target.id = source.id \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, source.v);",
    );
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM t")[0]), 3000);
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE id = 2999"),
        vec![vec![Value::Varchar("v2999!".to_string())]]
    );

    // `RETURNING` reports every inserted row.
    ok(&mut db, "CREATE TABLE r (id INTEGER, v VARCHAR);");
    let (_, _, got) = selected(
        &mut db,
        "MERGE INTO r AS target USING s AS source ON target.id = source.id \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, source.v) \
         RETURNING id, v",
    );
    assert_eq!(got.len(), 3000);
    assert_eq!(
        got[2999],
        vec![Value::Integer(2999), Value::Varchar("v2999".to_string())]
    );

    // A declared key is enforced, so a colliding insert is refused and the
    // table is unchanged.
    ok(
        &mut db,
        "CREATE TABLE k (id INTEGER PRIMARY KEY, v VARCHAR);",
    );
    ok(
        &mut db,
        "MERGE INTO k AS target USING s AS source ON target.id = source.id \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, source.v);",
    );
    assert_eq!(int(&one(&mut db, "SELECT count(*) FROM k")[0]), 3000);
    let before = stats(&mut db, "k");
    let err = fail(
        &mut db,
        "MERGE INTO k AS target USING s AS source ON target.id = source.id + 100000 \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, source.v);",
    );
    assert!(err.contains("duplicate key"), "{err}");
    assert_eq!(stats(&mut db, "k"), before);
}
