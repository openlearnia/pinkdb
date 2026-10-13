//! End-to-end tests for the extension loader: compile the C test extensions
//! with the system `cc`, load them into a real [`PinkDb`], and drive the
//! dynamic-scalar path through SQL (binder, optimizer, executor, rayon).

mod common;

use std::sync::Arc;

use pinkdb::{Database, PinkDb, QueryResult, Value};
use pinkdb_ext_loader::{ExtArg, ExtType, ExtValue};

// ---------- helpers ----------

/// A fresh in-memory store with `test_ext` loaded.
fn test_db() -> PinkDb {
    let mut db = PinkDb::new();
    let name = db.load_extension(&common::extension_path("test_ext")).unwrap();
    assert_eq!(name, "test_ext");
    db
}

/// Rows of the last statement in `sql` (which must be a SELECT).
fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

/// Error message of a failing `sql`.
fn error(db: &mut PinkDb, sql: &str) -> String {
    match db.execute(sql) {
        Ok(results) => panic!("expected an error for {sql:?}, got {results:?}"),
        Err(e) => e.to_string(),
    }
}

fn ints(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i as i64,
            ref other => panic!("expected INTEGER, got {other:?}"),
        })
        .collect()
}

// ---------- loading and calling ----------

#[test]
fn loads_and_runs_typed_scalar_functions() {
    let mut db = test_db();

    assert_eq!(rows(&mut db, "SELECT ext_double_it(21);"), vec![vec![Value::Integer(42)]]);
    assert_eq!(
        rows(&mut db, "SELECT ext_shout('hey');"),
        vec![vec![Value::Varchar("HEY".to_string())]]
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_concat2('sun', 'day');"),
        vec![vec![Value::Varchar("sunday".to_string())]]
    );
    // Lookup is case-insensitive, like built-in functions.
    assert_eq!(rows(&mut db, "SELECT EXT_DOUBLE_IT(21);"), vec![vec![Value::Integer(42)]]);
    assert_eq!(
        rows(&mut db, "SELECT Ext_Shout('MIXED case');"),
        vec![vec![Value::Varchar("MIXED CASE".to_string())]]
    );

    // Every value tag crosses the ABI: DOUBLE, BOOLEAN, DATE, TIMESTAMP.
    assert_eq!(rows(&mut db, "SELECT ext_halve(5.0);"), vec![vec![Value::Double(2.5)]]);
    assert_eq!(rows(&mut db, "SELECT ext_is_even(4);"), vec![vec![Value::Boolean(true)]]);
    assert_eq!(rows(&mut db, "SELECT ext_is_even(3);"), vec![vec![Value::Boolean(false)]]);
    assert_eq!(
        rows(&mut db, "SELECT ext_plus_day(DATE '2024-01-31') = DATE '2024-02-01';"),
        vec![vec![Value::Boolean(true)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_micros(TIMESTAMP '2024-01-01 00:00:00.5') = 1704067200500000;"),
        vec![vec![Value::Boolean(true)]]
    );

    // The engine's own functions are untouched and extensions compose with
    // them like any other expression.
    assert_eq!(rows(&mut db, "SELECT upper(ext_shout('hey'));"), rows(&mut db, "SELECT 'HEY';"));
    assert_eq!(rows(&mut db, "SELECT ext_double_it(21) + 1;"), vec![vec![Value::Integer(43)]]);
}

#[test]
fn columns_and_nulls_pass_through() {
    let mut db = test_db();
    db.execute("CREATE TABLE t (a INTEGER, s VARCHAR); INSERT INTO t VALUES (1, 'x'), (NULL, NULL), (3, 'z');")
        .unwrap();

    // NULL ordering: ORDER BY a puts NULL last.
    assert_eq!(
        rows(&mut db, "SELECT ext_double_it(a) FROM t ORDER BY a;"),
        vec![
            vec![Value::Integer(2)],
            vec![Value::Integer(6)],
            vec![Value::Null],
        ]
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_shout(s) FROM t ORDER BY a;"),
        vec![
            vec![Value::Varchar("X".to_string())],
            vec![Value::Varchar("Z".to_string())],
            vec![Value::Null],
        ]
    );
    // The callback is called with is_null set and chooses NULL (the header's
    // NULL rule); a bare NULL literal works for any registered argument type.
    assert_eq!(rows(&mut db, "SELECT ext_double_it(NULL);"), vec![vec![Value::Null]]);
    assert_eq!(rows(&mut db, "SELECT ext_shout(NULL);"), vec![vec![Value::Null]]);
    assert_eq!(rows(&mut db, "SELECT ext_concat2(NULL, 'x');"), vec![vec![Value::Null]]);
    // ... and it participates in WHERE / IS NULL like any expression.
    assert_eq!(rows(&mut db, "SELECT COUNT(*) FROM t WHERE ext_double_it(a) > 4;"), vec![vec![Value::Bigint(1)]]);
    assert_eq!(rows(&mut db, "SELECT COUNT(*) FROM t WHERE ext_shout(s) IS NULL;"), vec![vec![Value::Bigint(1)]]);
}

#[test]
fn arity_and_argument_type_mismatches_are_clear_errors() {
    let mut db = test_db();

    let e = error(&mut db, "SELECT ext_double_it(1, 2);");
    assert!(e.contains("ext_double_it takes 1 argument(s)"), "{e}");
    let e = error(&mut db, "SELECT ext_tick(1);");
    assert!(e.contains("ext_tick takes 0 argument(s)"), "{e}");
    let e = error(&mut db, "SELECT ext_concat2('a');");
    assert!(e.contains("ext_concat2 takes 2 argument(s)"), "{e}");

    // ABI v1 is strictly typed: no implicit casts (use CAST).
    let e = error(&mut db, "SELECT ext_double_it('x');");
    assert!(e.contains("ext_double_it argument 1 must be INTEGER, got VARCHAR"), "{e}");
    let e = error(&mut db, "SELECT ext_shout(1);");
    assert!(e.contains("ext_shout argument 1 must be VARCHAR, got INTEGER"), "{e}");
    let e = error(&mut db, "SELECT ext_halve(1);");
    assert!(e.contains("ext_halve argument 1 must be DOUBLE, got INTEGER"), "{e}");
    // The fix is an explicit cast.
    assert_eq!(rows(&mut db, "SELECT ext_halve(CAST(1 AS DOUBLE));"), vec![vec![Value::Double(0.5)]]);
}

#[test]
fn unknown_functions_still_report_unknown() {
    let mut db = test_db();
    // Loaded extensions must not change the error for names nobody provides.
    let e = error(&mut db, "SELECT no_such_function(1);");
    assert_eq!(e, "unknown function \"no_such_function\"");
    let e = error(&mut db, "SELECT abs(-1); SELECT definitely_missing();");
    assert_eq!(e, "unknown function \"definitely_missing\"");
}

#[test]
fn load_failures_are_clear_errors() {
    let mut db = PinkDb::new();

    let e = db.load_extension("/nonexistent/pinkdb/not_here.dylib").unwrap_err().to_string();
    assert!(e.contains("cannot load extension"), "{e}");
    assert!(e.contains("not_here.dylib"), "{e}");

    let e = db.load_extension("").unwrap_err().to_string();
    assert!(e.contains("cannot load extension"), "{e}");

    // A library without the ABI symbol (here: no `pinkdb_extension_abi_version`).
    let e = db.load_extension(&common::extension_path("no_abi_ext")).unwrap_err().to_string();
    assert!(e.contains("does not export \"pinkdb_extension_abi_version\""), "{e}");

    // A future ABI version: the error names both versions.
    let e = db.load_extension(&common::extension_path("wrong_abi_ext")).unwrap_err().to_string();
    assert!(e.contains("ABI version 2"), "{e}");
    assert!(e.contains("host supports version 1"), "{e}");

    // Rejected registrations fail the load even though init returned 0, and
    // nothing is registered.
    let e = db.load_extension(&common::extension_path("bad_register_ext")).unwrap_err().to_string();
    assert!(e.contains("registration failed"), "{e}");
    assert!(e.contains("invalid return type tag 99"), "{e}");
    let e = error(&mut db, "SELECT innocent(1);");
    assert_eq!(e, "unknown function \"innocent\"");

    // A failed load leaves the store fully usable.
    assert_eq!(rows(&mut db, "SELECT 1 + 1;"), vec![vec![Value::Integer(2)]]);
}

#[test]
fn duplicate_and_builtin_registrations_are_refused() {
    let mut db = test_db();
    // Loading the same library again: pinned by extension name.
    let e = db.load_extension(&common::extension_path("test_ext")).unwrap_err().to_string();
    assert_eq!(e, "extension registration failed: extension \"test_ext\" is already loaded");
    // The first registration is untouched.
    assert_eq!(rows(&mut db, "SELECT ext_double_it(21);"), vec![vec![Value::Integer(42)]]);

    // Shadowing a built-in is refused by the engine...
    let e = db.load_extension(&common::extension_path("builtin_ext")).unwrap_err().to_string();
    assert!(e.contains("conflicts with a built-in function"), "{e}");
    // ... and the built-in still wins.
    assert_eq!(rows(&mut db, "SELECT upper('hi');"), vec![vec![Value::Varchar("HI".to_string())]]);
}

// ---------- optimizer ----------

#[test]
fn constant_calls_are_never_constant_folded() {
    let mut db = test_db();
    db.execute("CREATE TABLE t3 (a INTEGER); INSERT INTO t3 VALUES (1), (2), (3);").unwrap();

    // `ext_tick()` has no column references, so the old folding rule would
    // evaluate it once at bind time and broadcast one literal to every row.
    // A fresh value per row proves the call runs in the executor instead.
    let ticks = ints(&rows(&mut db, "SELECT ext_tick() FROM t3;"));
    assert_eq!(ticks.len(), 3);
    assert!(
        ticks.windows(2).all(|w| w[0] < w[1]),
        "each row must invoke the callback (no folding): {ticks:?}"
    );

    // The same holds inside a constant WHERE predicate over many rows.
    let first = ints(&rows(&mut db, "SELECT ext_tick();"))[0];
    let second = ints(&rows(&mut db, "SELECT ext_tick();"))[0];
    assert!(second > first, "two executions must call the callback twice: {first} then {second}");
}

// ---------- parallelism ----------

/// 16k rows across 8 chunks (CHUNK_SIZE = 2048).
const BIG_ROWS: i64 = 16_384;

fn big_table(db: &mut PinkDb) {
    db.execute("CREATE TABLE big (a INTEGER);").unwrap();
    let mut sql = String::from("INSERT INTO big VALUES ");
    for i in 1..=BIG_ROWS {
        if i > 1 {
            sql.push(',');
        }
        sql.push_str(&format!("({i})"));
    }
    sql.push(';');
    db.execute(&sql).unwrap();
}

#[test]
fn execution_from_rayon_workers_is_correct() {
    let mut db = test_db();
    big_table(&mut db);

    let library = common::extension("test_ext");
    let before = common::symbol_i64(&library, "pinkdb_test_probe_calls");

    // WHERE runs per chunk on rayon workers; every row must hit the callback
    // exactly once, with correct values, and the aggregate over the dynamic
    // call must stay exact.
    let expected_sum: i64 = 2 * (BIG_ROWS * (BIG_ROWS + 1) / 2 - 4_000 * 4_001 / 2);
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*), SUM(ext_double_it(a)) FROM big WHERE ext_probe(a) > 4000;"),
        vec![vec![Value::Bigint(BIG_ROWS - 4000), Value::Bigint(expected_sum)]]
    );

    let calls = common::symbol_i64(&library, "pinkdb_test_probe_calls") - before;
    assert_eq!(calls, BIG_ROWS, "one callback per row");

    // The callbacks above ran on the host's rayon workers. On a multi-core
    // host that must be more than one OS thread; on a single-core host a
    // single worker is legitimate. (The deterministic proof of concurrent
    // callbacks is `concurrent_callbacks_are_thread_safe` below.)
    let threads = common::symbol_i32(&library, "pinkdb_test_distinct_threads");
    assert!(threads >= 1, "no callback ran?");
    let workers = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    if workers > 1 {
        assert!(
            threads > 1,
            "8 chunks on a {workers}-CPU host must spread over rayon workers (saw {threads})"
        );
    }
}

#[test]
fn concurrent_callbacks_are_thread_safe() {
    let path = common::extension_path("test_ext");
    let extension = pinkdb_ext_loader::load(&path).unwrap();
    let library = common::extension("test_ext");

    let tap = extension
        .functions()
        .iter()
        .find(|f| f.name() == "ext_tap")
        .expect("ext_tap registered")
        .clone();
    let tap = Arc::new(tap);
    assert!(Arc::strong_count(&tap) >= 1);

    let before = common::symbol_i64(&library, "pinkdb_test_tap_calls");
    // A barrier keeps all four threads alive before any callback runs: the
    // extension identifies threads by the address of a thread-local, and
    // thread-local storage may be recycled once a thread exits.
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let threads: Vec<_> = (1..=4)
        .map(|t| {
            let tap = Arc::clone(&tap);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..500 {
                    let arg = ExtArg::Integer(t * 1000 + i);
                    let got = tap.call(&[arg]).unwrap();
                    assert_eq!(got, ExtValue::Integer(t * 1000 + i));
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }

    // All four host threads ran the shared handle: the `unsafe impl
    // Send + Sync` and the ABI's thread-safety contract hold on the real
    // registration path.
    let calls = common::symbol_i64(&library, "pinkdb_test_tap_calls") - before;
    assert_eq!(calls, 2000);
    let seen = common::symbol_i32(&library, "pinkdb_test_distinct_threads");
    assert!(seen >= 4, "expected at least the four test threads, saw {seen}");
}

#[test]
fn connections_share_the_same_registry() {
    let mut db = test_db();
    db.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2), (3);").unwrap();
    let db = Database::from_store(db);
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let db = db.clone();
            std::thread::spawn(move || {
                let mut conn = db.connect();
                match conn.execute("SELECT SUM(ext_double_it(a)), ext_shout('ok') FROM t;").unwrap().pop().unwrap() {
                    QueryResult::Selected { rows, .. } => rows,
                    other => panic!("expected rows, got {other:?}"),
                }
            })
        })
        .collect();
    for h in handles {
        assert_eq!(
            h.join().unwrap(),
            vec![vec![Value::Bigint(12), Value::Varchar("OK".to_string())]]
        );
    }

    // A transaction works on a private snapshot of the store; the registry
    // clones with it, so dynamic functions resolve inside BEGIN..COMMIT too.
    let mut conn = db.connect();
    let results = conn.execute("BEGIN; SELECT ext_double_it(21) AS twice; COMMIT;").unwrap();
    match &results[1] {
        QueryResult::Selected { rows, .. } => assert_eq!(rows, &vec![vec![Value::Integer(42)]]),
        other => panic!("expected rows, got {other:?}"),
    }
}

// ---------- composition with the rest of the engine ----------

#[test]
fn aggregates_windows_and_joins_accept_dynamic_calls() {
    let mut db = test_db();
    db.execute(
        "CREATE TABLE g (k INTEGER, v DOUBLE); \
         INSERT INTO g VALUES (1, 1.0), (1, 2.0), (2, 4.0);",
    )
    .unwrap();

    // A dynamic call as a GROUP BY key / select item (rebased on the keys).
    assert_eq!(
        rows(&mut db, "SELECT ext_double_it(k) AS d, COUNT(*) FROM g GROUP BY d ORDER BY d;"),
        vec![
            vec![Value::Integer(2), Value::Bigint(2)],
            vec![Value::Integer(4), Value::Bigint(1)],
        ]
    );
    // Inside an aggregate argument, and wrapping an aggregate result.
    assert_eq!(
        rows(&mut db, "SELECT k, SUM(ext_halve(v)) FROM g GROUP BY k ORDER BY k;"),
        vec![vec![Value::Integer(1), Value::Double(1.5)], vec![Value::Integer(2), Value::Double(2.0)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT k, ext_halve(SUM(v)) FROM g GROUP BY k ORDER BY k;"),
        vec![vec![Value::Integer(1), Value::Double(1.5)], vec![Value::Integer(2), Value::Double(2.0)]]
    );
    // HAVING with a dynamic constant.
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM g HAVING COUNT(*) > ext_double_it(0);"),
        vec![vec![Value::Bigint(3)]]
    );
    // A dynamic call inside a window's aggregate argument.
    assert_eq!(
        rows(&mut db, "SELECT k, SUM(ext_halve(v)) OVER (PARTITION BY k) AS w FROM g ORDER BY k;"),
        vec![
            vec![Value::Integer(1), Value::Double(1.5)],
            vec![Value::Integer(1), Value::Double(1.5)],
            vec![Value::Integer(2), Value::Double(2.0)],
        ]
    );

    // Joins: the fused join→aggregate path rejects opaque calls and falls
    // back to the materialized path, but the result is the same.
    db.execute("CREATE TABLE u (k INTEGER); INSERT INTO u VALUES (1), (2);").unwrap();
    // Matches: (1,1), (1,2), (2,4) → SUM(2*k) = 2 + 2 + 4.
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*), SUM(ext_double_it(g.k)) FROM g JOIN u ON g.k = u.k;"),
        vec![vec![Value::Bigint(3), Value::Bigint(8)]]
    );
}

#[test]
fn callback_errors_surface_as_sql_errors() {
    let mut db = test_db();
    assert_eq!(rows(&mut db, "SELECT ext_fail_on_negative(3);"), vec![vec![Value::Integer(3)]]);
    let e = error(&mut db, "SELECT ext_fail_on_negative(-3);");
    assert!(e.contains("ext_fail_on_negative() failed with code 7"), "{e}");

    // The failing call aborts the statement over rows too, and the store
    // stays usable.
    db.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (-1), (2);").unwrap();
    let e = error(&mut db, "SELECT ext_fail_on_negative(a) FROM t;");
    assert!(e.contains("ext_fail_on_negative() failed with code 7"), "{e}");
    assert_eq!(rows(&mut db, "SELECT ext_fail_on_negative(5);"), vec![vec![Value::Integer(5)]]);
}

// ---------- LOAD EXTENSION (SQL surface, wave 29B) ----------

/// Rows of the last statement executed through a `Connection`.
fn conn_rows(conn: &mut pinkdb::Connection, sql: &str) -> Vec<Vec<Value>> {
    match conn.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

#[test]
fn load_extension_statement_loads_and_registers() {
    let path = common::extension_path("test_ext");
    let mut db = PinkDb::new();
    // Nothing is registered before the statement.
    assert_eq!(error(&mut db, "SELECT ext_double_it(21);"), "unknown function \"ext_double_it\"");

    // The statement returns the extension's declared name.
    let results = db.execute(&format!("LOAD EXTENSION '{path}';")).unwrap();
    assert_eq!(
        results,
        vec![QueryResult::LoadedExtension { name: "test_ext".to_string() }]
    );
    // Functions are callable afterwards, case-insensitively.
    assert_eq!(rows(&mut db, "SELECT ext_double_it(21);"), vec![vec![Value::Integer(42)]]);
    assert_eq!(
        rows(&mut db, "SELECT EXT_SHOUT('hey');"),
        vec![vec![Value::Varchar("HEY".to_string())]]
    );

    // A multi-statement string: the LOAD applies to everything after it.
    let mut db2 = PinkDb::new();
    let sql = format!(
        "LOAD EXTENSION '{path}'; CREATE TABLE t (a INTEGER); \
         INSERT INTO t VALUES (1), (2), (3); SELECT SUM(ext_double_it(a)) FROM t;"
    );
    let results = db2.execute(&sql).unwrap();
    match results.last().unwrap() {
        QueryResult::Selected { rows, .. } => {
            assert_eq!(rows, &vec![vec![Value::Bigint(12)]])
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn load_extension_statement_works_through_connections() {
    let path = common::extension_path("test_ext");
    let db = Database::from_store(PinkDb::new());
    let mut c1 = db.connect();
    match c1.execute(&format!("LOAD EXTENSION '{path}';")).unwrap().pop().unwrap() {
        QueryResult::LoadedExtension { name } => assert_eq!(name, "test_ext"),
        other => panic!("expected LoadedExtension, got {other:?}"),
    }
    // The registry is store-level: another connection sees the function.
    let mut c2 = db.connect();
    assert_eq!(
        conn_rows(&mut c2, "SELECT ext_double_it(21);"),
        vec![vec![Value::Integer(42)]]
    );
}

#[test]
fn load_extension_statement_failures_are_clear_errors() {
    let mut db = PinkDb::new();

    // A missing file / bad path is an execution error naming the path.
    let e = error(&mut db, "LOAD EXTENSION '/nonexistent/pinkdb/not_here.dylib';");
    assert!(e.contains("cannot load extension"), "{e}");
    assert!(e.contains("not_here.dylib"), "{e}");

    // A wrong-ABI library names both versions.
    let e = error(
        &mut db,
        &format!("LOAD EXTENSION '{}';", common::extension_path("wrong_abi_ext")),
    );
    assert!(e.contains("ABI version 2"), "{e}");
    assert!(e.contains("host supports version 1"), "{e}");

    // A rejected registration fails the load and registers nothing.
    let e = error(
        &mut db,
        &format!("LOAD EXTENSION '{}';", common::extension_path("bad_register_ext")),
    );
    assert!(e.contains("registration failed"), "{e}");
    assert_eq!(error(&mut db, "SELECT innocent(1);"), "unknown function \"innocent\"");

    // The non-literal path form is refused at parse time (message wording
    // depends on the parser `PINKDB_PARSER` selects; the kind does not).
    match db.execute("LOAD EXTENSION ext;") {
        Err(pinkdb::PinkDbError::Parse(_)) => {}
        other => panic!("expected a Parse error for a non-literal path, got {other:?}"),
    }

    // A failed load leaves the store fully usable.
    assert_eq!(rows(&mut db, "SELECT 1 + 1;"), vec![vec![Value::Integer(2)]]);
}

#[test]
fn reloading_the_same_extension_statement_is_a_clear_error() {
    let path = common::extension_path("test_ext");
    let mut db = PinkDb::new();
    db.execute(&format!("LOAD EXTENSION '{path}';")).unwrap();
    // Reload is *not* silently idempotent: the second load reports the
    // already-loaded extension name (pinned decision, same as the library
    // API).
    let e = error(&mut db, &format!("LOAD EXTENSION '{path}';"));
    assert!(e.contains("extension \"test_ext\" is already loaded"), "{e}");
    // The first registration is untouched.
    assert_eq!(rows(&mut db, "SELECT ext_double_it(21);"), vec![vec![Value::Integer(42)]]);
}

#[test]
fn load_extension_inside_a_transaction_is_refused() {
    let path = common::extension_path("test_ext");

    // Embedded session.
    let mut db = PinkDb::new();
    db.execute("BEGIN;").unwrap();
    let e = error(&mut db, &format!("LOAD EXTENSION '{path}';"));
    assert_eq!(e, "LOAD EXTENSION is not allowed inside a transaction");
    // The statement never ran: the store is unchanged and usable.
    assert_eq!(rows(&mut db, "SELECT 1;"), vec![vec![Value::Integer(1)]]);
    db.execute("ROLLBACK;").unwrap();
    assert_eq!(error(&mut db, "SELECT ext_double_it(21);"), "unknown function \"ext_double_it\"");

    // Multi-writer Connection path (the private snapshot's registry would be
    // discarded at COMMIT, so it is refused the same way).
    let db = Database::from_store(PinkDb::new());
    let mut conn = db.connect();
    conn.execute("BEGIN;").unwrap();
    let e = match conn.execute(&format!("LOAD EXTENSION '{path}';")) {
        Ok(r) => panic!("expected an error, got {r:?}"),
        Err(e) => e.to_string(),
    };
    assert_eq!(e, "LOAD EXTENSION is not allowed inside a transaction");
    conn.execute("ROLLBACK;").unwrap();
    let e = match conn.execute("SELECT ext_double_it(21);") {
        Ok(r) => panic!("expected an error, got {r:?}"),
        Err(e) => e.to_string(),
    };
    assert_eq!(e, "unknown function \"ext_double_it\"");
}

#[test]
fn wal_replay_reloads_the_extension() {
    let path = common::extension_path("test_ext");
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("ext_sql_wal_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 1. Durable store: LOAD + table + rows written through the extension.
    let ckpt = dir.join("reload.pink");
    let ckpt = ckpt.to_str().unwrap();
    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        let sql = format!(
            "LOAD EXTENSION '{path}'; CREATE TABLE t (a INTEGER); \
             INSERT INTO t VALUES (1), (2), (3);"
        );
        db.execute(&sql).unwrap();
        // Insert rows computed by the extension (v1 INSERT takes VALUES only).
        let doubled: Vec<String> = match db.execute("SELECT ext_double_it(a) FROM t;").unwrap().pop().unwrap() {
            QueryResult::Selected { rows, .. } => rows
                .into_iter()
                .map(|r| match r[0] {
                    Value::Integer(i) => format!("({i})"),
                    ref other => panic!("expected INTEGER, got {other:?}"),
                })
                .collect(),
            other => panic!("expected rows, got {other:?}"),
        };
        db.execute(&format!("INSERT INTO t VALUES {};", doubled.join(", "))).unwrap();
        db.flush_wal().unwrap();
    }
    // Reopen: replay re-runs LOAD EXTENSION (a fresh process-level load) and
    // the inserts; the function is callable on the replayed store.
    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        assert_eq!(
            rows(&mut db, "SELECT a FROM t ORDER BY a;"),
            vec![
                vec![Value::Integer(1)],
                vec![Value::Integer(2)],
                vec![Value::Integer(2)],
                vec![Value::Integer(3)],
                vec![Value::Integer(4)],
                vec![Value::Integer(6)],
            ]
        );
        assert_eq!(rows(&mut db, "SELECT SUM(ext_double_it(a)) FROM t;"), vec![vec![Value::Bigint(36)]]);
    }

    // 2. The same round-trip through a *copy* of the library that is deleted
    //    before the reopen: a missing file at replay time is a hard recovery
    //    error naming the path, never a silent drop.
    let copy = dir.join(format!("test_ext_copy{}", std::env::consts::DLL_SUFFIX));
    std::fs::copy(&path, &copy).unwrap();
    let ckpt2 = dir.join("missing.pink");
    let ckpt2 = ckpt2.to_str().unwrap();
    {
        let mut db = PinkDb::open_durable(ckpt2).unwrap();
        db.execute(&format!("LOAD EXTENSION '{}';", copy.display())).unwrap();
        db.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (ext_double_it(10));").unwrap();
        db.flush_wal().unwrap();
    }
    std::fs::remove_file(&copy).unwrap();
    let e = PinkDb::open_durable(ckpt2).unwrap_err().to_string();
    assert!(e.contains("WAL replay failed at lsn"), "{e}");
    assert!(e.contains("cannot load extension"), "{e}");
    assert!(e.contains("test_ext_copy"), "{e}");

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------- registration edges (review H2 + minors) ----------

#[test]
fn null_callback_is_rejected_without_crashing() {
    // The loader itself rejects a NULL `fn` before it can ever be stored: a
    // stored NULL would be jumped to on the first call (the review's
    // SIGSEGV). The extension returns 0 from init; the host must fail the
    // load anyway.
    let path = common::extension_path("null_callback_ext");
    let e = pinkdb_ext_loader::load(&path).unwrap_err().to_string();
    assert!(e.contains("registration failed"), "{e}");
    assert!(e.contains("function \"null_cb\": NULL callback"), "{e}");

    let mut db = PinkDb::new();
    let e = db.load_extension(&path).unwrap_err().to_string();
    assert!(e.contains("function \"null_cb\": NULL callback"), "{e}");
    // Nothing was registered, and the store stays fully usable.
    assert_eq!(error(&mut db, "SELECT null_cb(1);"), "unknown function \"null_cb\"");
    assert_eq!(rows(&mut db, "SELECT 1 + 1;"), vec![vec![Value::Integer(2)]]);
}

#[test]
fn zero_function_extension_loads_and_is_pinned_by_name() {
    let mut db = PinkDb::new();
    let name = db.load_extension(&common::extension_path("empty_ext")).unwrap();
    assert_eq!(name, "empty_ext");
    // The load succeeded and the name is pinned: a second load is refused,
    // which also proves the store kept the handle (an empty extension used to
    // drop its library immediately).
    let e = db.load_extension(&common::extension_path("empty_ext")).unwrap_err().to_string();
    assert_eq!(e, "extension registration failed: extension \"empty_ext\" is already loaded");
    assert_eq!(rows(&mut db, "SELECT 1;"), vec![vec![Value::Integer(1)]]);
}

#[test]
fn dynamic_call_rejects_arity_and_argument_type_mismatches() {
    let path = common::extension_path("test_ext");
    let ext = pinkdb_ext_loader::load(&path).unwrap();
    assert_eq!(ext.path(), path, "the handle records its source path");
    let f = ext
        .functions()
        .iter()
        .find(|f| f.name() == "ext_double_it")
        .expect("ext_double_it registered")
        .clone();
    assert_eq!(f.source_path(), path, "functions record their source path");

    let e = f.call(&[]).unwrap_err();
    assert!(e.to_string().contains("expected 1 argument(s), got 0"), "{e}");
    let e = f.call(&[ExtArg::Integer(1), ExtArg::Integer(2)]).unwrap_err();
    assert!(e.to_string().contains("expected 1 argument(s), got 2"), "{e}");
    // The wrong declared type is refused before the callback runs.
    let e = f.call(&[ExtArg::Varchar(b"x")]).unwrap_err();
    assert!(
        e.to_string().contains("argument 1 has type VARCHAR, registered as INTEGER"),
        "{e}"
    );
    let e = f.call(&[ExtArg::Null(ExtType::Varchar)]).unwrap_err();
    assert!(
        e.to_string().contains("argument 1 has type VARCHAR, registered as INTEGER"),
        "{e}"
    );
}

// ---------- WAL + checkpoint extension prefixing (review C1) ----------

/// A fresh scratch directory for one durable WAL test.
fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("ext_wal_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The C1 repro: `LOAD EXTENSION` is WAL-logged, but a checkpoint rolls the
/// WAL without persisting extensions. A post-checkpoint record that calls an
/// extension must be `LOAD EXTENSION`-prefixed (touched-only), or every
/// reopen dies with `unknown function` during replay.
#[test]
fn autocommit_record_touching_an_extension_survives_a_checkpoint() {
    let path = common::extension_path("test_ext");
    let dir = scratch("autocommit");
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();
    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        db.execute(&format!(
            "LOAD EXTENSION '{path}'; CREATE TABLE t (a INTEGER);"
        ))
        .unwrap();
        db.execute("INSERT INTO t VALUES (ext_double_it(1));").unwrap();
        // The checkpoint rolls away the LOAD record; the extension is *not*
        // written into the checkpoint.
        db.checkpoint().unwrap();
        db.execute("INSERT INTO t VALUES (ext_double_it(2));").unwrap();
    }
    // Reopen: replay re-loads test_ext from the prefixed record and applies
    // the post-checkpoint insert; the pre-checkpoint row comes from the
    // checkpoint itself.
    let mut db = PinkDb::open_durable(ckpt).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![vec![Value::Integer(2)], vec![Value::Integer(4)]]
    );
    assert_eq!(rows(&mut db, "SELECT ext_double_it(21);"), vec![vec![Value::Integer(42)]]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// C1 variant: a multi-writer transaction whose buffered record is committed
/// *after* another connection checkpointed the store.
#[test]
fn transaction_spanning_a_checkpoint_survives_reopen() {
    let path = common::extension_path("test_ext");
    let dir = scratch("txnspan");
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();
    {
        let db = Database::open(ckpt).unwrap();
        let mut c = db.connect();
        c.execute(&format!("LOAD EXTENSION '{path}'; CREATE TABLE t (a INTEGER);"))
            .unwrap();
        // Open the transaction, write through the extension, and let the
        // store checkpoint while the transaction is still open (the txn
        // works on a private snapshot; the checkpoint takes the write lock).
        c.execute("BEGIN; INSERT INTO t VALUES (ext_double_it(1));").unwrap();
        db.checkpoint().unwrap();
        c.execute("INSERT INTO t VALUES (ext_double_it(2)); COMMIT;").unwrap();
    }
    let mut db = PinkDb::open_durable(ckpt).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![vec![Value::Integer(2)], vec![Value::Integer(4)]]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// C1 variant: `USE` + extension across a checkpoint (the record carries both
/// the `USE` and the `LOAD EXTENSION` prefixes).
#[test]
fn use_plus_extension_record_survives_a_checkpoint() {
    let path = common::extension_path("test_ext");
    let dir = scratch("use");
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();
    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        db.execute(&format!(
            "LOAD EXTENSION '{path}'; CREATE DATABASE db1; CREATE TABLE db1.t (a INTEGER);"
        ))
        .unwrap();
        db.execute("USE db1; INSERT INTO t VALUES (ext_double_it(1));").unwrap();
        db.checkpoint().unwrap();
        db.execute("USE db1; INSERT INTO t VALUES (ext_double_it(2));").unwrap();
    }
    let mut db = PinkDb::open_durable(ckpt).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM db1.t ORDER BY a;"),
        vec![vec![Value::Integer(2)], vec![Value::Integer(4)]]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The review's counterexample: extension A is loaded before a checkpoint and
/// its *file is deleted*; extension B is loaded after. A post-checkpoint
/// record uses only B, so replay must never try to re-load A.
#[test]
fn replay_loads_only_the_extensions_the_record_touches() {
    let dir = scratch("touched_only");
    // A: a copy of test_ext under its own path (deleted after the checkpoint).
    let a = dir.join(format!("ext_a{}", std::env::consts::DLL_SUFFIX));
    std::fs::copy(common::extension_path("test_ext"), &a).unwrap();
    let b = common::extension_path("test_ext2");
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();
    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        db.execute(&format!(
            "LOAD EXTENSION '{}'; CREATE TABLE t (a INTEGER);",
            a.display()
        ))
        .unwrap();
        // A is used before the checkpoint (its record is rolled away; the row
        // is in the checkpoint).
        db.execute("INSERT INTO t VALUES (ext_double_it(1));").unwrap();
        db.checkpoint().unwrap();
        // B is loaded after the checkpoint; only B's function is used.
        db.execute(&format!("LOAD EXTENSION '{b}';")).unwrap();
        db.execute("INSERT INTO t VALUES (ext2_triple(2));").unwrap();
    }
    std::fs::remove_file(&a).unwrap();
    // Must reopen without ever touching A's missing file.
    let mut db = PinkDb::open_durable(ckpt).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![vec![Value::Integer(2)], vec![Value::Integer(6)]]
    );
    assert_eq!(rows(&mut db, "SELECT ext2_triple(10);"), vec![vec![Value::Integer(30)]]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A checkpoint with no extension use afterwards leaves the database openable:
/// extensions are process-level and the checkpoint never persists them, so a
/// reopen starts with none loaded (and re-loading works).
#[test]
fn checkpoint_without_extension_use_stays_openable() {
    let path = common::extension_path("test_ext");
    let dir = scratch("ckpt_plain");
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();
    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        db.execute(&format!(
            "LOAD EXTENSION '{path}'; CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2);"
        ))
        .unwrap();
        db.checkpoint().unwrap();
        // No post-checkpoint record touches the extension.
    }
    let mut db = PinkDb::open_durable(ckpt).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]
    );
    // Documented: the extension is gone after the checkpoint until re-loaded.
    assert_eq!(error(&mut db, "SELECT ext_double_it(1);"), "unknown function \"ext_double_it\"");
    db.execute(&format!("LOAD EXTENSION '{path}';")).unwrap();
    assert_eq!(rows(&mut db, "SELECT ext_double_it(21);"), vec![vec![Value::Integer(42)]]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Dynamic calls inside subqueries are collected by the binder too (child
/// scopes share the tracking), so a logged record whose only call sits in a
/// scalar subquery is still prefixed correctly.
#[test]
fn subquery_dynamic_calls_are_prefixed_too() {
    let path = common::extension_path("test_ext");
    let dir = scratch("subquery");
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();
    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        db.execute(&format!(
            "LOAD EXTENSION '{path}'; CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (0);"
        ))
        .unwrap();
        db.checkpoint().unwrap();
        // The call is inside a scalar subquery in INSERT VALUES...
        db.execute("INSERT INTO t VALUES ((SELECT ext_double_it(1)));").unwrap();
        // ... and in an UPDATE SET expression.
        db.execute("UPDATE t SET a = (SELECT ext_double_it(2)) WHERE a = 0;").unwrap();
    }
    let mut db = PinkDb::open_durable(ckpt).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![
            vec![Value::Integer(2)],
            vec![Value::Integer(4)],
        ]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Reopening a live WAL whose generation *did* load an extension must not
/// re-prefix later records (a redundant `LOAD EXTENSION` would fail replay
/// with an already-loaded error). The extension path is recorded in the
/// generation, so after recovery the WAL knows it is covered.
#[test]
fn reopen_does_not_reprefix_extensions_the_generation_loaded() {
    let path = common::extension_path("test_ext");
    let dir = scratch("seed");
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();
    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        db.execute(&format!(
            "LOAD EXTENSION '{path}'; CREATE TABLE t (a INTEGER);"
        ))
        .unwrap();
        db.execute("INSERT INTO t VALUES (ext_double_it(1));").unwrap();
    }
    // First reopen: replays LOAD + INSERT; the generation covers the
    // extension, so a new post-reopen record must not carry a redundant
    // prefix.
    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        db.execute("INSERT INTO t VALUES (ext_double_it(2));").unwrap();
    }
    // Second reopen: replaying LOAD then the post-reopen INSERT must succeed
    // (a redundant prefix would have tried to load test_ext twice).
    let mut db = PinkDb::open_durable(ckpt).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![vec![Value::Integer(2)], vec![Value::Integer(4)]]
    );
    let _ = std::fs::remove_dir_all(&dir);
}
