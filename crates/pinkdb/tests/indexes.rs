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

use pinkdb::{CHUNK_SIZE, PinkDb, QueryResult, Value};

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
