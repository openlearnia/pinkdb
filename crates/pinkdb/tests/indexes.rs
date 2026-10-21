//! Hash-index lifecycle, optimizer backstop and probe-key typing.
//!
//! The engine's documented contract (`CHANGELOG.md`) is that an index may
//! change the plan but never the result set. These tests cover the historical
//! failures from the 2026-10-19 security review:
//!
//! - H-3/A.4: an index built before DML must not return the old row (or zero
//!   rows) after INSERT/UPDATE/DELETE.
//! - H-4/A.5: a stale row index must never panic the gather path.
//! - H-5/A.6: the probe key is canonicalized to the indexed column's type, so
//!   a numeric literal of a different type still finds the row.
//! - L-1: `DROP COLUMN` drops an index on that column and shifts the ordinals
//!   of the remaining ones, so a later `ADD COLUMN` cannot make an index point
//!   at the wrong column.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{CHUNK_SIZE, PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Self-cleaning scratch directory for one durable test: every file under it
/// (checkpoint, sidecar WAL, lock file) is removed when the guard drops.
struct Scratch {
    dir: PathBuf,
    ckpt: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_idx_{tag}_{}_{}",
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

    fn ckpt(&self) -> &Path {
        &self.ckpt
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn durable(scratch: &Scratch) -> PinkDb {
    PinkDb::open_durable(scratch.path()).unwrap()
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn explain(db: &mut PinkDb, sql: &str) -> String {
    rows(db, &format!("EXPLAIN {sql};"))
        .iter()
        .map(|r| match &r[0] {
            Value::Varchar(s) => s.clone(),
            other => panic!("expected EXPLAIN text, got {other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn int_rows(values: &[i32]) -> Vec<Vec<Value>> {
    values.iter().map(|v| vec![Value::Integer(*v)]).collect()
}

// ---------------------------------------------------------------------------
// Creation, EXPLAIN and the identical-results contract
// ---------------------------------------------------------------------------

#[test]
fn create_index_plans_an_index_scan_and_returns_the_same_rows() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (k INTEGER, v VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (2, 'bb'), (NULL, 'n'), (3, 'c');")
        .unwrap();

    let query = "SELECT k, v FROM t WHERE k = 2 ORDER BY v;";
    let without = rows(&mut db, query);

    let created = db.execute("CREATE INDEX ix ON t(k);").unwrap();
    assert_eq!(created[0], QueryResult::CreatedIndex { name: "ix".into() });

    let plan = explain(&mut db, query);
    assert!(plan.contains("IndexScan"), "no IndexScan in plan:\n{plan}");
    // The predicate is always re-checked above the index scan (H-3 backstop).
    let filter_at = plan.find("Filter").expect("Filter above IndexScan");
    let scan_at = plan.find("IndexScan").expect("IndexScan");
    assert!(
        filter_at < scan_at,
        "Filter must sit above IndexScan:\n{plan}"
    );

    assert_eq!(
        rows(&mut db, query),
        without,
        "index changed the result set"
    );
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = 99;"),
        Vec::<Vec<Value>>::new()
    );
}

// ---------------------------------------------------------------------------
// Exact review reproductions
// ---------------------------------------------------------------------------

/// A.4 / H-3: `UPDATE` after `CREATE INDEX` must never return the old or the
/// new row under the *old* key.
#[test]
fn a4_update_then_lookup_returns_the_current_row() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE accounts (id INTEGER, owner VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO accounts VALUES (1, 'alice'), (2, 'bob');")
        .unwrap();
    db.execute("CREATE INDEX ix ON accounts(id);").unwrap();
    db.execute("UPDATE accounts SET id = 99 WHERE id = 1;")
        .unwrap();

    assert_eq!(
        rows(&mut db, "SELECT id, owner FROM accounts WHERE id = 1;"),
        Vec::<Vec<Value>>::new(),
        "old key must not return the updated row"
    );
    assert_eq!(
        rows(&mut db, "SELECT id, owner FROM accounts WHERE id = 99;"),
        vec![vec![Value::Integer(99), Value::Varchar("alice".into())]],
        "new key must return the row"
    );
}

/// A.5 / H-4: `DELETE` after `CREATE INDEX` used to panic with `vector index
/// out of bounds`. It must simply return zero rows.
#[test]
fn a5_delete_then_lookup_does_not_panic() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, s VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b');")
        .unwrap();
    db.execute("CREATE INDEX ix ON t(id);").unwrap();
    db.execute("DELETE FROM t WHERE id = 2;").unwrap();

    assert_eq!(
        rows(&mut db, "SELECT * FROM t WHERE id = 2;"),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        rows(&mut db, "SELECT * FROM t WHERE id = 1;"),
        vec![vec![Value::Integer(1), Value::Varchar("a".into())]]
    );
}

/// A.6 / H-5: a BIGINT column indexed, probed with an INTEGER literal.
#[test]
fn a6_bigint_column_with_integer_literal_finds_the_row() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id BIGINT);").unwrap();
    db.execute("INSERT INTO t VALUES (5);").unwrap();

    let query = "SELECT id FROM t WHERE id = 5;";
    let expected = vec![vec![Value::Bigint(5)]];
    assert_eq!(rows(&mut db, query), expected, "unindexed baseline");

    db.execute("CREATE INDEX ix ON t(id);").unwrap();
    assert_eq!(
        rows(&mut db, query),
        expected,
        "indexed lookup with an INTEGER literal must find the BIGINT row"
    );
    assert!(explain(&mut db, query).contains("IndexScan"));
}

// ---------------------------------------------------------------------------
// Type matrix for the probe key
// ---------------------------------------------------------------------------

#[test]
fn probe_key_matches_double_column_with_integer_literal() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (d DOUBLE);").unwrap();
    db.execute("INSERT INTO t VALUES (5.0), (5.5);").unwrap();
    let query = "SELECT d FROM t WHERE d = 5;";
    let expected = vec![vec![Value::Double(5.0)]];
    assert_eq!(rows(&mut db, query), expected);

    db.execute("CREATE INDEX ix ON t(d);").unwrap();
    assert_eq!(rows(&mut db, query), expected);
    assert!(explain(&mut db, query).contains("IndexScan"));
}

/// DOUBLE equality follows DuckDB (NaN = NaN, NaN equals no other value,
/// `-0.0 = 0.0`) on the scan *and* through the hash index, so the index
/// cannot change the result even over NaN payloads and signed zeros. The
/// scan used to treat NaN as equal to everything (`d = 0.0` matched it).
#[test]
fn double_nan_and_signed_zero_index_scan_matches_the_scan() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, d DOUBLE);")
        .unwrap();
    db.execute(
        "INSERT INTO t VALUES (1, '-0.0'::DOUBLE), (2, 0.0), (3, 'NaN'::DOUBLE), (4, 1.5), \
         (5, 0.0 / 0.0), (6, -('NaN'::DOUBLE)), (7, NULL), (8, 'inf'::DOUBLE), (9, '-inf'::DOUBLE);",
    )
    .unwrap();

    let ids = |db: &mut PinkDb, literal: &str| {
        rows(
            db,
            &format!("SELECT id FROM t WHERE d = {literal} ORDER BY id;"),
        )
    };
    let cases = [
        ("0.0", int_rows(&[1, 2])),
        ("'-0.0'::DOUBLE", int_rows(&[1, 2])),
        ("0", int_rows(&[1, 2])),
        ("'NaN'::DOUBLE", int_rows(&[3, 5, 6])),
        ("-('NaN'::DOUBLE)", int_rows(&[3, 5, 6])),
        ("1.5", int_rows(&[4])),
        ("'inf'::DOUBLE", int_rows(&[8])),
        ("'-inf'::DOUBLE", int_rows(&[9])),
        ("2.0", int_rows(&[])),
    ];
    let scanned: Vec<_> = cases.iter().map(|(lit, _)| ids(&mut db, lit)).collect();
    for ((lit, want), got) in cases.iter().zip(&scanned) {
        assert_eq!(got, want, "scan: d = {lit}");
    }
    assert_eq!(
        rows(&mut db, "SELECT count(*) FROM t WHERE d = 0.0;"),
        vec![vec![Value::Bigint(2)]],
        "the reported repro"
    );

    db.execute("CREATE INDEX ix ON t(d);").unwrap();
    for ((lit, _), want) in cases.iter().zip(&scanned) {
        let query = format!("SELECT id FROM t WHERE d = {lit} ORDER BY id;");
        let plan = explain(&mut db, query.trim_end_matches(';'));
        assert!(
            plan.contains("IndexScan"),
            "no IndexScan for d = {lit}:\n{plan}"
        );
        assert_eq!(&ids(&mut db, lit), want, "IndexScan: d = {lit}");
    }
    assert_eq!(
        rows(&mut db, "SELECT count(*) FROM t WHERE d = 0.0;"),
        vec![vec![Value::Bigint(2)]]
    );
}

#[test]
fn probe_key_matches_decimal_column_with_integer_and_other_scales() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (d DECIMAL(10,2));").unwrap();
    db.execute("INSERT INTO t VALUES (1.50), (2.00), (2.25);")
        .unwrap();

    // Integer literal against DECIMAL column.
    let int_query = "SELECT d FROM t WHERE d = 2;";
    let int_expected = vec![vec![Value::Decimal(200, 2)]];
    assert_eq!(rows(&mut db, int_query), int_expected);

    db.execute("CREATE INDEX ix ON t(d);").unwrap();
    assert_eq!(rows(&mut db, int_query), int_expected);

    // Literal with a different DECIMAL scale (one fractional digit).
    let scale_query = "SELECT d FROM t WHERE d = 1.5;";
    let scale_expected = vec![vec![Value::Decimal(150, 2)]];
    assert_eq!(rows(&mut db, scale_query), scale_expected);

    // Explicit three-digit scale literal: DECIMAL(10,3) -> column scale 2.
    let cast_query = "SELECT d FROM t WHERE d = CAST(2.250 AS DECIMAL(10,3));";
    let cast_expected = vec![vec![Value::Decimal(225, 2)]];
    assert_eq!(rows(&mut db, cast_query), cast_expected);
}

#[test]
fn probe_key_matches_integer_column_with_cast_bigint_literal() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER);").unwrap();
    db.execute("INSERT INTO t VALUES (5), (6);").unwrap();
    let query = "SELECT i FROM t WHERE i = CAST(5 AS BIGINT);";
    let expected = int_rows(&[5]);
    assert_eq!(rows(&mut db, query), expected);

    db.execute("CREATE INDEX ix ON t(i);").unwrap();
    assert_eq!(rows(&mut db, query), expected);
    assert!(explain(&mut db, query).contains("IndexScan"));
}

/// Literal/column numeric pairs that the engine compares through `f64`
/// (integer column vs DOUBLE, DECIMAL column vs DOUBLE) collapse distinct
/// values. A single canonical index key would miss rows, so the optimizer must
/// keep the full scan.
#[test]
fn collapsing_numeric_literals_fall_back_to_the_full_scan() {
    // DECIMAL(38,0): the stored value is exact, but its f64 comparison value
    // is shared with other decimals. `1e30` must still find it.
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE d (v DECIMAL(38,0));").unwrap();
    db.execute("INSERT INTO d VALUES (CAST('1000000000000000000000000000000' AS DECIMAL(38,0)));")
        .unwrap();
    let decimal_query = "SELECT v FROM d WHERE v = 1e30;";
    let decimal_expected = vec![vec![Value::Decimal(
        1_000_000_000_000_000_000_000_000_000_000i128,
        0,
    )]];
    assert_eq!(rows(&mut db, decimal_query), decimal_expected);
    db.execute("CREATE INDEX ixd ON d(v);").unwrap();
    assert_eq!(rows(&mut db, decimal_query), decimal_expected);

    // BIGINT above 2^53: 9007199254740993 and 9007199254740992 compare equal
    // as DOUBLE. The index cannot pick one without risking a miss.
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE b (v BIGINT);").unwrap();
    db.execute("INSERT INTO b VALUES (9007199254740993);")
        .unwrap();
    let big_query = "SELECT v FROM b WHERE v = 9007199254740992.0;";
    let big_expected = vec![vec![Value::Bigint(9_007_199_254_740_993)]];
    assert_eq!(rows(&mut db, big_query), big_expected);
    db.execute("CREATE INDEX ixb ON b(v);").unwrap();
    assert_eq!(rows(&mut db, big_query), big_expected);
}

// ---------------------------------------------------------------------------
// Freshness after every mutation path
// ---------------------------------------------------------------------------

#[test]
fn index_is_fresh_after_insert_update_delete_and_bulk_load() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (k INTEGER, v INTEGER);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20);")
        .unwrap();
    db.execute("CREATE INDEX ix ON t(k);").unwrap();

    // INSERT after CREATE INDEX.
    db.execute("INSERT INTO t VALUES (3, 30);").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = 3;"),
        int_rows(&[30])
    );

    // UPDATE the indexed column, then the stored column.
    db.execute("UPDATE t SET k = 7 WHERE k = 3;").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = 3;"),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = 7;"),
        int_rows(&[30])
    );
    db.execute("UPDATE t SET v = 700 WHERE k = 7;").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = 7;"),
        int_rows(&[700])
    );

    // DELETE one of several rows.
    db.execute("DELETE FROM t WHERE k = 2;").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = 2;"),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = 1;"),
        int_rows(&[10])
    );

    // Bulk `INSERT ... SELECT`.
    db.execute("CREATE TABLE src (k INTEGER, v INTEGER);")
        .unwrap();
    db.execute("INSERT INTO src VALUES (8, 80), (9, 90), (9, 91);")
        .unwrap();
    db.execute("INSERT INTO t SELECT k, v FROM src;").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = 9 ORDER BY v;"),
        int_rows(&[90, 91])
    );
}

#[test]
fn index_handles_duplicates_nulls_and_empty_tables() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (k INTEGER, v INTEGER);")
        .unwrap();
    // Index on an empty table: a lookup must return zero rows and later
    // inserts must be visible.
    db.execute("CREATE INDEX ix ON t(k);").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = 1;"),
        Vec::<Vec<Value>>::new()
    );
    db.execute("INSERT INTO t VALUES (1, 10), (1, 11), (2, 20), (NULL, 30);")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = 1 ORDER BY v;"),
        int_rows(&[10, 11])
    );
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = 2;"),
        int_rows(&[20])
    );
    // NULL keys are never stored; `= NULL` still yields no rows.
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = NULL;"),
        Vec::<Vec<Value>>::new()
    );
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k IS NULL;"),
        int_rows(&[30])
    );
}

#[test]
fn index_maps_global_rows_across_a_partially_emptied_chunk() {
    // More than one chunk, then a delete in the first chunk leaves it shorter
    // than CHUNK_SIZE. Global row indices must still map through the real
    // chunk lengths.
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (k INTEGER, v INTEGER);")
        .unwrap();
    let db_name = db.current_db().to_string();
    {
        let t = db.table_mut_in(&db_name, "t").unwrap();
        for i in 0..(CHUNK_SIZE * 2) {
            t.append_row(vec![Value::Integer(i as i32), Value::Integer(i as i32)])
                .unwrap();
        }
    }
    db.execute("CREATE INDEX ix ON t(k);").unwrap();
    db.execute("DELETE FROM t WHERE k = 0;").unwrap();

    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = 2048;"),
        int_rows(&[2048])
    );
    assert_eq!(
        rows(&mut db, "SELECT v FROM t WHERE k = 4095;"),
        int_rows(&[4095])
    );
}

// ---------------------------------------------------------------------------
// Defensive gather path (H-4) and the filter-above-scan invariant
// ---------------------------------------------------------------------------

/// Corrupt an index entry to point past the table: the gather path must skip
/// it, not panic.
#[test]
fn out_of_range_index_entry_is_skipped_without_panicking() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, s VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b');")
        .unwrap();
    db.execute("CREATE INDEX ix ON t(id);").unwrap();

    let db_name = db.current_db().to_string();
    {
        let t = db.table_mut_in(&db_name, "t").unwrap();
        let idx = &mut t.indexes[0];
        // The key whose only row is id = 2 (row 1).
        let key = idx
            .map
            .iter()
            .find(|(_, rows)| rows.as_slice() == [1])
            .map(|(k, _)| k.clone())
            .expect("key for id = 2");
        idx.map.get_mut(&key).unwrap().extend([9999, usize::MAX]);
    }

    assert_eq!(
        rows(&mut db, "SELECT id, s FROM t WHERE id = 2;"),
        vec![vec![Value::Integer(2), Value::Varchar("b".into())]]
    );
}

/// The optimizer keeps the `Filter` above the `IndexScan`, so candidates a
/// stale/approximate index wrongly proposes are filtered out.
#[test]
fn filter_above_index_scan_rejects_wrong_candidates() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (id INTEGER, s VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b');")
        .unwrap();
    db.execute("CREATE INDEX ix ON t(id);").unwrap();

    let db_name = db.current_db().to_string();
    {
        let t = db.table_mut_in(&db_name, "t").unwrap();
        let idx = &mut t.indexes[0];
        // Make the id = 2 key also claim row 0 (which holds id = 1).
        let key = idx
            .map
            .iter()
            .find(|(_, rows)| rows.as_slice() == [1])
            .map(|(k, _)| k.clone())
            .expect("key for id = 2");
        idx.map.get_mut(&key).unwrap().push(0);
    }

    assert_eq!(
        rows(&mut db, "SELECT id, s FROM t WHERE id = 2;"),
        vec![vec![Value::Integer(2), Value::Varchar("b".into())]],
        "the predicate above the scan must reject the bogus candidate"
    );
    let plan = explain(&mut db, "SELECT id FROM t WHERE id = 2;");
    assert!(
        plan.find("Filter") < plan.find("IndexScan"),
        "Filter must stay above IndexScan:\n{plan}"
    );
}

// ---------------------------------------------------------------------------
// L-1: DROP COLUMN / ADD COLUMN ordinals
// ---------------------------------------------------------------------------

#[test]
fn drop_column_drops_its_index_and_shifts_the_remaining_ones() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (a INTEGER, b INTEGER);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20);")
        .unwrap();
    db.execute("CREATE INDEX ix ON t(b);").unwrap();

    db.execute("ALTER TABLE t DROP COLUMN a;").unwrap();
    // The index now addresses ordinal 0 (b). A later ADD COLUMN must not make
    // that ordinal point at the new column.
    db.execute("ALTER TABLE t ADD COLUMN c INTEGER DEFAULT 99;")
        .unwrap();
    db.execute("INSERT INTO t (b) VALUES (30);").unwrap();

    assert_eq!(
        rows(&mut db, "SELECT b, c FROM t ORDER BY b;"),
        vec![
            vec![Value::Integer(10), Value::Integer(99)],
            vec![Value::Integer(20), Value::Integer(99)],
            vec![Value::Integer(30), Value::Integer(99)],
        ]
    );
    // The index still serves b (`IndexScan` on the shifted ordinal).
    assert!(explain(&mut db, "SELECT c FROM t WHERE b = 20;").contains("IndexScan"));
    assert_eq!(
        rows(&mut db, "SELECT c FROM t WHERE b = 20;"),
        int_rows(&[99])
    );
    // c has no index, so this must be a full scan over all three rows — not
    // a stale b-map interpreted as c.
    assert_eq!(
        rows(&mut db, "SELECT b FROM t WHERE c = 99 ORDER BY b;"),
        int_rows(&[10, 20, 30])
    );
}

#[test]
fn drop_column_removes_an_index_on_that_column() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (a INTEGER, b INTEGER);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10);").unwrap();
    db.execute("CREATE INDEX ia ON t(a);").unwrap();
    db.execute("ALTER TABLE t DROP COLUMN a;").unwrap();

    let err = db.execute("DROP INDEX ia;").unwrap_err().to_string();
    assert!(err.contains("not found"), "{err}");
    // The remaining column is still queryable.
    assert_eq!(rows(&mut db, "SELECT b FROM t;"), int_rows(&[10]));
}

// ---------------------------------------------------------------------------
// Durable lifecycle: index DDL survives reopen, checkpoints and replay
// ---------------------------------------------------------------------------

/// The measured wave-79 bug: `CREATE INDEX` was in-memory only, so a reopen
/// silently lost it. Without a checkpoint the definition must come back from
/// the WAL; `DROP INDEX` must persist the same way.
#[test]
fn durable_create_and_drop_index_survive_reopen_without_checkpoint() {
    let p = Scratch::new("reopen");
    let query = "SELECT a FROM t WHERE a = 2;";
    {
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER, s VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (2, 'bb');")
            .unwrap();
        db.execute("CREATE INDEX i ON t(a);").unwrap();
        assert!(explain(&mut db, query).contains("IndexScan"), "in session");
    }
    {
        let mut db = durable(&p);
        assert!(
            explain(&mut db, query).contains("IndexScan"),
            "index lost on reopen without a checkpoint"
        );
        assert_eq!(
            rows(&mut db, "SELECT s FROM t WHERE a = 2 ORDER BY s;"),
            vec![
                vec![Value::Varchar("b".into())],
                vec![Value::Varchar("bb".into())]
            ]
        );
        db.execute("DROP INDEX i;").unwrap();
        assert!(!explain(&mut db, query).contains("IndexScan"));
    }
    let mut db = durable(&p);
    assert!(
        !explain(&mut db, query).contains("IndexScan"),
        "dropped index came back on reopen"
    );
    assert_eq!(
        rows(&mut db, "SELECT a FROM t ORDER BY a;"),
        int_rows(&[1, 2, 2])
    );
}

/// A checkpoint folds the store into `.pink` (no index section) and rolls the
/// WAL. The definition must be re-logged into the fresh generation, exactly
/// once, and repeated checkpoints must neither duplicate nor resurrect it.
#[test]
fn durable_checkpoint_carries_index_and_repeats_do_not_duplicate_it() {
    let p = Scratch::new("ckpt");
    let query = "SELECT a FROM t WHERE a = 2;";
    {
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER, s VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b');")
            .unwrap();
        db.execute("CREATE INDEX i ON t(a);").unwrap();
        db.checkpoint().unwrap();
        assert!(explain(&mut db, query).contains("IndexScan"));
    }
    let mut db = durable(&p);
    assert!(
        explain(&mut db, query).contains("IndexScan"),
        "checkpoint dropped the index"
    );
    assert_eq!(
        db.table_in("main", "t").unwrap().indexes.len(),
        1,
        "exactly one definition after the checkpoint re-log"
    );
    assert_eq!(
        rows(&mut db, "SELECT s FROM t WHERE a = 2;"),
        vec![vec![Value::Varchar("b".into())]]
    );
    // Two more checkpoints: each folds the previous re-log into `.pink` and
    // must write the live set once into the next generation.
    db.checkpoint().unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let mut db = durable(&p);
    assert_eq!(db.table_in("main", "t").unwrap().indexes.len(), 1);
    assert!(explain(&mut db, query).contains("IndexScan"));
}

#[test]
fn durable_checkpoint_then_drop_index_stays_dropped() {
    let p = Scratch::new("ckptdrop");
    let query = "SELECT a FROM t WHERE a = 1;";
    {
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1), (1), (2);").unwrap();
        db.execute("CREATE INDEX i ON t(a);").unwrap();
        db.checkpoint().unwrap();
        db.execute("DROP INDEX i;").unwrap();
        assert!(!explain(&mut db, query).contains("IndexScan"));
        // A later checkpoint must not resurrect the dropped definition.
        db.checkpoint().unwrap();
    }
    let mut db = durable(&p);
    assert!(
        !explain(&mut db, query).contains("IndexScan"),
        "a dropped index was resurrected by the checkpoint re-log"
    );
    assert_eq!(
        db.table_in("main", "t").unwrap().indexes.len(),
        0,
        "a dropped index must not be re-logged"
    );
    assert_eq!(
        rows(&mut db, "SELECT a FROM t ORDER BY a;"),
        int_rows(&[1, 1, 2])
    );
}

#[test]
fn durable_checkpoint_then_drop_table_with_index_reopens_clean() {
    let p = Scratch::new("ckptdroptable");
    {
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER);").unwrap();
        db.execute("CREATE TABLE keep (a INTEGER);").unwrap();
        db.execute("INSERT INTO t VALUES (1);").unwrap();
        db.execute("INSERT INTO keep VALUES (7);").unwrap();
        db.execute("CREATE INDEX i ON t(a);").unwrap();
        db.checkpoint().unwrap();
        db.execute("DROP TABLE t;").unwrap();
    }
    let mut db = durable(&p);
    let err = db.execute("SELECT a FROM t;").unwrap_err().to_string();
    assert!(err.contains("not found"), "{err}");
    assert_eq!(
        rows(&mut db, "SELECT a FROM keep;"),
        int_rows(&[7]),
        "the rest of the store must replay normally"
    );
}

/// Multiple databases/tables/indexes, quoted + unicode identifiers (spaces,
/// keywords, an embedded quote), `IF NOT EXISTS`, and a name dropped then
/// recreated on a different column: every definition must re-bind exactly.
#[test]
fn durable_indexes_round_trip_quoted_unicode_and_recreated_names() {
    let p = Scratch::new("quoted");
    {
        let mut db = durable(&p);
        db.execute("CREATE DATABASE \"dätä base\";").unwrap();
        db.execute("CREATE TABLE t (a INTEGER, b INTEGER);")
            .unwrap();
        db.execute("CREATE TABLE \"dätä base\".\"täble one\" (\"col ü\" INTEGER);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 20);")
            .unwrap();
        db.execute("INSERT INTO \"dätä base\".\"täble one\" VALUES (7), (8);")
            .unwrap();
        db.execute("CREATE INDEX \"i\"\"q\" ON t(a);").unwrap();
        db.execute("CREATE INDEX \"index select\" ON \"dätä base\".\"täble one\"(\"col ü\");")
            .unwrap();
        db.execute("CREATE INDEX IF NOT EXISTS dup ON t(b);")
            .unwrap();
        db.execute("CREATE INDEX IF NOT EXISTS dup ON t(b);")
            .unwrap();
        // Same name, dropped and recreated on another column.
        db.execute("CREATE INDEX r ON t(b);").unwrap();
        db.execute("DROP INDEX r;").unwrap();
        db.execute("CREATE INDEX r ON t(a);").unwrap();
        db.checkpoint().unwrap();
    }
    let mut db = durable(&p);
    let cols = |db: &PinkDb, index: &str| -> usize {
        db.table_in("main", "t")
            .unwrap()
            .indexes
            .iter()
            .find(|i| i.name == index)
            .unwrap_or_else(|| panic!("index {index:?} missing"))
            .column
    };
    assert_eq!(cols(&db, "i\"q"), 0);
    assert_eq!(cols(&db, "dup"), 1);
    assert_eq!(cols(&db, "r"), 0, "recreation must log its final column");
    assert_eq!(
        db.table_in("dätä base", "täble one").unwrap().indexes.len(),
        1
    );
    assert!(
        explain(
            &mut db,
            "SELECT \"col ü\" FROM \"dätä base\".\"täble one\" WHERE \"col ü\" = 7;"
        )
        .contains("IndexScan"),
        "quoted/unicode table index lost"
    );
    assert!(explain(&mut db, "SELECT b FROM t WHERE a = 2;").contains("IndexScan"));
    assert_eq!(
        rows(&mut db, "SELECT b FROM t WHERE a = 2;"),
        int_rows(&[20])
    );
}

/// A second durable handle learns the definition through the touched-set
/// covered frame (and loses it again when the first handle drops it).
#[test]
fn durable_second_handle_refresh_observes_index_ddl() {
    let p = Scratch::new("refresh");
    let query = "SELECT a FROM t WHERE a = 2;";
    let mut a = durable(&p);
    a.execute("CREATE TABLE t (a INTEGER);").unwrap();
    a.execute("INSERT INTO t VALUES (1), (2);").unwrap();
    // `b` is opened before the index exists; its next statement refreshes.
    let mut b = durable(&p);
    a.execute("CREATE INDEX i ON t(a);").unwrap();
    assert!(
        explain(&mut b, query).contains("IndexScan"),
        "refreshing reader missed the index frame"
    );
    assert_eq!(rows(&mut b, "SELECT a FROM t WHERE a = 2;"), int_rows(&[2]));
    // Another handle's checkpoint rolls the WAL (no index section in
    // `.pink`): the refreshing reader must rebuild from the re-log record.
    a.checkpoint().unwrap();
    assert!(
        explain(&mut b, query).contains("IndexScan"),
        "refreshing reader missed the checkpoint re-log"
    );
    a.execute("DROP INDEX i;").unwrap();
    assert!(
        !explain(&mut b, query).contains("IndexScan"),
        "refreshing reader missed the drop frame"
    );
}

/// One transaction's record preserves statement order: create, drop, create
/// the same name; ROLLBACK leaves nothing behind.
#[test]
fn durable_transaction_preserves_index_ddl_order() {
    let p = Scratch::new("txnorder");
    {
        let mut db = durable(&p);
        db.execute("CREATE TABLE t (a INTEGER, b INTEGER);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 20);")
            .unwrap();
        db.execute("BEGIN; CREATE INDEX i ON t(a); DROP INDEX i; CREATE INDEX i ON t(b); COMMIT;")
            .unwrap();
        assert_eq!(db.table_in("main", "t").unwrap().indexes[0].column, 1);
    }
    let mut db = durable(&p);
    assert_eq!(
        db.table_in("main", "t").unwrap().indexes[0].column,
        1,
        "the last create in the transaction must win"
    );
    assert!(explain(&mut db, "SELECT a FROM t WHERE b = 20;").contains("IndexScan"));
    assert!(
        !explain(&mut db, "SELECT b FROM t WHERE a = 2;").contains("IndexScan"),
        "the dropped a-index must not be replayed"
    );
    // A rolled-back drop logs nothing.
    db.execute("BEGIN; DROP INDEX i; ROLLBACK;").unwrap();
    drop(db);
    let mut db = durable(&p);
    assert_eq!(db.table_in("main", "t").unwrap().indexes.len(), 1);
    assert!(explain(&mut db, "SELECT a FROM t WHERE b = 20;").contains("IndexScan"));
}

/// `.pink` has no index section (bit 3 stays clear): creating an index changes
/// neither the serialized bytes nor the flags word.
#[test]
fn durable_index_does_not_change_pink_bytes_or_flags() {
    let p = Scratch::new("bytes");
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2);").unwrap();
    let before = db.to_pink_bytes().unwrap();
    db.execute("CREATE INDEX i ON t(a);").unwrap();
    let after = db.to_pink_bytes().unwrap();
    assert_eq!(
        before, after,
        "the in-memory index must not leak into `.pink` bytes"
    );
    let flags = u32::from_le_bytes(after[8..12].try_into().unwrap());
    assert_eq!(flags & 0x8, 0, "reserved index bit 3 must stay clear");
    // A durable checkpoint of the indexed store also keeps the flag clear.
    db.save(p.path()).unwrap();
    let on_disk = std::fs::read(p.ckpt()).unwrap();
    let flags = u32::from_le_bytes(on_disk[8..12].try_into().unwrap());
    assert_eq!(flags & 0x8, 0);
}
