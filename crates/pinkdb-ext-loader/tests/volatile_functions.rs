//! Volatile (nondeterministic) extension functions and the engine's
//! WAL-divergence guard (wave 51).
//!
//! A function is **deterministic by default** in both loading models, exactly
//! as every pre-wave-51 extension was. An extension may *declare* a function
//! volatile — the native ABI's `PINKDB_EXT_FLAG_VOLATILE` (via
//! `api->register_scalar_flags`) or the linked model's
//! `LinkedFunction::volatile()` — and the engine then refuses to run that
//! call in any statement whose SQL text it WAL-logs: replay re-executes the
//! logged text, and a nondeterministic callback could produce a different
//! value there. Read-only statements never reach the WAL and may call it.
//!
//! The C fixtures under `tests/c/` cover the ABI side: `volatile_ext.c` uses
//! the new registration entry point, `bad_flags_ext.c` proves unknown flag
//! bits are rejected, and `legacy_api_ext.c` reproduces a pre-flag extension
//! binary (the additive-ABI proof).

mod common;

use pinkdb::{Database, PinkDb, QueryResult, Value};
use pinkdb_ext_loader::linked::{self, LinkedExtension, LinkedFunction};
use pinkdb_ext_loader::{ExtArg, ExtType, ExtValue};

// ---------- helpers ----------

/// A fresh in-memory store with `volatile_ext` loaded.
fn volatile_db() -> PinkDb {
    let mut db = PinkDb::new();
    let name = db
        .load_extension(&common::extension_path("volatile_ext"))
        .unwrap();
    assert_eq!(name, "volatile_ext");
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

/// Rows of a query run on a [`Connection`].
fn rows_result(conn: &mut pinkdb::Connection, sql: &str) -> Vec<Vec<Value>> {
    match conn.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

/// The pinned refusal: a volatile call in a statement the engine WAL-logs.
fn assert_volatile_refusal(message: &str, function: &str) {
    assert!(message.contains(function), "{message}");
    assert!(message.contains("volatile"), "{message}");
    assert!(
        message.contains("cannot be called in a statement the engine WAL-logs"),
        "{message}"
    );
    assert!(message.contains("read-only statement"), "{message}");
}

/// A scratch directory for the durable tests.
fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("ext_vol_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------- the flag through the ABI ----------

/// `register_scalar_flags` carries both bits into the registry: the volatile
/// declaration, and the deterministic default for `flags == 0` (identical to
/// `register_scalar`).
#[test]
fn the_flag_is_carried_into_the_function_metadata() {
    let ext = pinkdb_ext_loader::load(&common::extension_path("volatile_ext")).unwrap();
    let by_name = |name: &str| {
        ext.functions()
            .iter()
            .find(|f| f.name() == name)
            .unwrap_or_else(|| panic!("{name} registered"))
    };
    assert!(
        by_name("ext_vol_tick").is_volatile(),
        "the volatile flag must be stored"
    );
    assert!(
        !by_name("ext_vol_stable").is_volatile(),
        "flags == 0 must stay deterministic"
    );
    assert!(
        !by_name("ext_vol_stable_legacy").is_volatile(),
        "register_scalar (the old entry point) must stay deterministic"
    );
}

/// Unknown flag bits fail the registration (and with it the load): a dropped
/// flag would silently make a declared-volatile function deterministic.
#[test]
fn unknown_flag_bits_fail_the_load() {
    let mut db = PinkDb::new();
    let err = db
        .load_extension(&common::extension_path("bad_flags_ext"))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("unknown registration flag bit(s) 0x8"),
        "{err}"
    );
    assert!(err.contains("bad_flags"), "{err}");
}

/// A pre-flag extension (its own view of the five-field host API) loads
/// unchanged and registers deterministic functions: the flag addition is
/// additive, and nothing an existing binary does changes semantics.
#[test]
fn a_legacy_extension_binary_keeps_working_and_stays_deterministic() {
    let mut db = PinkDb::new();
    let name = db
        .load_extension(&common::extension_path("legacy_api_ext"))
        .unwrap();
    assert_eq!(name, "legacy_api_ext");
    assert_eq!(
        rows(&mut db, "SELECT ext_legacy_add_one(41);"),
        vec![vec![Value::Integer(42)]]
    );
    // Deterministic: usable in a WAL-logged statement. (The durable replay
    // proof for legacy registrations is `test_ext` in `extensions.rs`; here
    // the in-memory statement class is the point.)
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    db.execute("INSERT INTO t VALUES (ext_legacy_add_one(1));")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t;"),
        vec![vec![Value::Integer(2)]]
    );
    // The loader-level view of the same fact.
    let ext = pinkdb_ext_loader::load(&common::extension_path("legacy_api_ext")).unwrap();
    assert!(ext.functions().iter().all(|f| !f.is_volatile()));
}

// ---------- the engine rule ----------

/// A volatile function is callable in read-only statements — every call may
/// return a fresh value, which is exactly what it is for.
#[test]
fn volatile_calls_are_allowed_in_pure_reads() {
    let mut db = volatile_db();
    let first = rows(&mut db, "SELECT ext_vol_tick();");
    let second = rows(&mut db, "SELECT ext_vol_tick();");
    assert_ne!(first, second, "a volatile function may vary between calls");
    // It composes like any expression in a read.
    assert_eq!(
        rows(&mut db, "SELECT ext_vol_tick() >= 0;"),
        vec![vec![Value::Boolean(true)]]
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_vol_stable(3);"),
        vec![vec![Value::Integer(6)]]
    );
}

/// Every statement class the engine WAL-logs refuses a volatile call — before
/// executing anything, so the refused statement leaves no trace.
#[test]
fn volatile_calls_are_refused_in_wal_logged_statements() {
    let mut db = volatile_db();
    db.execute("CREATE TABLE t (a BIGINT);").unwrap();

    for sql in [
        "INSERT INTO t VALUES (ext_vol_tick());",
        "INSERT INTO t SELECT ext_vol_tick();",
        "CREATE TABLE t2 AS SELECT ext_vol_tick();",
        "UPDATE t SET a = ext_vol_tick();",
        "DELETE FROM t WHERE ext_vol_tick() >= 0;",
        "CREATE MATERIALIZED VIEW mv AS SELECT ext_vol_tick() AS x;",
    ] {
        let message = error(&mut db, sql);
        assert_volatile_refusal(&message, "ext_vol_tick()");
    }
    // The deterministic function stays usable everywhere.
    db.execute("INSERT INTO t VALUES (ext_vol_stable(21));")
        .unwrap();
    // A view definition is session-local and never logged, so defining one
    // over a volatile call is fine; reading it is fine; *writing from it* is
    // the logged statement, and the guard catches the call through the view.
    db.execute("CREATE VIEW v AS SELECT ext_vol_tick() AS x;")
        .unwrap();
    assert_eq!(rows(&mut db, "SELECT x FROM v;").len(), 1);
    let message = error(&mut db, "INSERT INTO t SELECT x FROM v;");
    assert_volatile_refusal(&message, "ext_vol_tick()");
    // A procedure body refuses any dynamic call in its statements (its own,
    // pre-existing replay rule), so volatility is covered there too — and a
    // volatile call in a *mutating* body statement is caught by this guard
    // first, while a read-only one reaches the procedure's rule.
    let message = error(
        &mut db,
        "CREATE PROCEDURE p() AS $$ BEGIN INSERT INTO t VALUES (ext_vol_tick()); RETURN; END $$; CALL p();",
    );
    assert_volatile_refusal(&message, "ext_vol_tick()");
    let message = error(
        &mut db,
        "CREATE PROCEDURE q() AS $$ BEGIN SELECT ext_vol_tick(); RETURN; END $$; CALL q();",
    );
    assert!(message.contains("loadable-extension function"), "{message}");
    // A volatile call in a procedure *expression* (here: a condition) is
    // refused too: a CALL is WAL-logged as a whole and replay re-evaluates
    // the condition, which could take the other branch.
    let message = error(
        &mut db,
        "CREATE PROCEDURE cond() AS $$ BEGIN IF ext_vol_tick() >= 0 THEN RETURN; END IF; RETURN; END $$; CALL cond();",
    );
    assert!(message.contains("volatile (nondeterministic)"), "{message}");
    assert!(message.contains("procedure body"), "{message}");
    // A *deterministic* call in a procedure expression keeps working exactly
    // as before: it replays identically.
    db.execute(
        "CREATE PROCEDURE detcond() AS $$ BEGIN IF ext_vol_stable(1) = 2 THEN RETURN; END IF; RETURN; END $$;",
    )
    .unwrap();
    db.execute("CALL detcond();").unwrap();

    // Nothing except the one deterministic row was ever applied.
    assert_eq!(
        rows(&mut db, "SELECT a FROM t;"),
        vec![vec![Value::Bigint(42)]]
    );
}

/// The rule holds inside transactions (the statement would be buffered into
/// the transaction's WAL record) and for prepared statements (the refusal is
/// a bind-time error, before any execution).
#[test]
fn volatile_calls_are_refused_in_transactions_and_prepared_statements() {
    let mut db = volatile_db();
    db.execute("CREATE TABLE t (a BIGINT);").unwrap();
    db.execute("BEGIN;").unwrap();
    let message = error(&mut db, "INSERT INTO t VALUES (ext_vol_tick());");
    assert_volatile_refusal(&message, "ext_vol_tick()");
    // The deterministic call inside the same transaction is fine.
    db.execute("INSERT INTO t VALUES (ext_vol_stable(1));")
        .unwrap();
    db.execute("COMMIT;").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t;"),
        vec![vec![Value::Bigint(2)]]
    );

    // Prepared statements go through a connection, which needs a shared store.
    let shared = Database::from_store(db);
    let mut conn = shared.connect();
    let stmt = conn
        .prepare("INSERT INTO t VALUES (ext_vol_tick());")
        .unwrap();
    let err = conn.execute_prepared(&stmt, &[]).unwrap_err().to_string();
    assert_volatile_refusal(&err, "ext_vol_tick()");
    // A deterministic prepared statement through the same connection works,
    // `?` included.
    let ok = conn
        .prepare("INSERT INTO t VALUES (ext_vol_stable(?));")
        .unwrap();
    conn.execute_prepared(&ok, &[Value::Integer(21)]).unwrap();
    assert_eq!(
        rows_result(&mut conn, "SELECT a FROM t ORDER BY a;"),
        vec![vec![Value::Bigint(2)], vec![Value::Bigint(42)]]
    );
}

/// The durable-store round trip: deterministic statements WAL-log and replay
/// identically; the refused volatile statement never reaches the WAL (a
/// logged one would make the reopen below fail hard during replay).
#[test]
fn deterministic_statements_replay_and_volatile_ones_never_reach_the_wal() {
    let path = common::extension_path("volatile_ext");
    let dir = scratch("durable");
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();
    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        // The statement is off by default (it loads and runs native code).
        db.set_allow_sql_load_extension(true);
        db.execute(&format!(
            "LOAD EXTENSION '{path}'; CREATE TABLE t (a BIGINT);"
        ))
        .unwrap();
        db.execute("INSERT INTO t VALUES (ext_vol_stable(21));")
            .unwrap();
        // Refused: nothing is written for this statement.
        let err = db
            .execute("INSERT INTO t VALUES (ext_vol_tick());")
            .unwrap_err()
            .to_string();
        assert_volatile_refusal(&err, "ext_vol_tick()");
        db.flush_wal().unwrap();
        // The WAL text never mentions the refused call.
        let wal =
            String::from_utf8_lossy(&std::fs::read(format!("{ckpt}.wal")).unwrap()).into_owned();
        assert!(
            !wal.contains("ext_vol_tick"),
            "the refused statement must not be logged: {wal}"
        );
        assert!(wal.contains("ext_vol_stable"), "the accepted one is logged");
    }
    // Reopen replays: the deterministic row is there, byte for byte.
    let mut db = PinkDb::open_durable(ckpt).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t;"),
        vec![vec![Value::Bigint(42)]]
    );
    // The extension is loaded again by replay, and a deterministic statement
    // after recovery still works.
    db.execute("INSERT INTO t VALUES (ext_vol_stable(2));")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t ORDER BY a;"),
        vec![vec![Value::Bigint(4)], vec![Value::Bigint(42)]]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A pre-flag registration (`register_scalar`, the C `test_ext` fixture)
/// keeps today's semantics end to end: logged, replayed, identical — the
/// additive-ABI promise at the engine level.
#[test]
fn legacy_registrations_still_wal_log_and_replay() {
    let path = common::extension_path("test_ext");
    let dir = scratch("legacy_replay");
    let ckpt = dir.join("db.pink");
    let ckpt = ckpt.to_str().unwrap();
    {
        let mut db = PinkDb::open_durable(ckpt).unwrap();
        // The statement is off by default (it loads and runs native code).
        db.set_allow_sql_load_extension(true);
        db.execute(&format!(
            "LOAD EXTENSION '{path}'; CREATE TABLE t (a INTEGER);"
        ))
        .unwrap();
        db.execute("INSERT INTO t VALUES (ext_double_it(21));")
            .unwrap();
        db.flush_wal().unwrap();
    }
    let mut db = PinkDb::open_durable(ckpt).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t;"),
        vec![vec![Value::Integer(42)]]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------- the same rule for the linked model ----------

/// The linked model's `volatile()` declaration feeds the same engine check,
/// and the registry metadata exposes it (`linked::get`).
#[test]
fn linked_volatile_functions_obey_the_same_rule() {
    let name = format!("wave51_linked_vol_{}", std::process::id());
    let ext = LinkedExtension::new(
        name.clone(),
        vec![
            LinkedFunction::new(
                "lnk51_tick",
                vec![],
                ExtType::Bigint,
                |_args: &[ExtArg<'_>]| Ok(ExtValue::Bigint(1)),
            )
            .unwrap()
            .volatile(),
            LinkedFunction::new(
                "lnk51_stable",
                vec![ExtType::Integer],
                ExtType::Integer,
                |args: &[ExtArg<'_>]| match args[0] {
                    ExtArg::Integer(i) => Ok(ExtValue::Integer(i * 2)),
                    _ => Ok(ExtValue::Null),
                },
            )
            .unwrap(),
        ],
    )
    .unwrap();
    linked::register(ext).unwrap();
    // The registry metadata carries the declaration.
    let registered = linked::get(&name).expect("registered");
    assert!(registered.functions()[0].is_volatile());
    assert!(!registered.functions()[1].is_volatile());

    let mut db = PinkDb::new();
    assert_eq!(db.load_linked_extension(&name).unwrap(), name);
    // Read-only: allowed.
    assert_eq!(
        rows(&mut db, "SELECT lnk51_tick();"),
        vec![vec![Value::Bigint(1)]]
    );
    db.execute("CREATE TABLE t (a INTEGER);").unwrap();
    // WAL-logged: refused.
    let message = error(
        &mut db,
        "INSERT INTO t VALUES (CAST(lnk51_tick() AS INTEGER));",
    );
    assert_volatile_refusal(&message, "lnk51_tick()");
    let message = error(
        &mut db,
        "INSERT INTO t SELECT CAST(lnk51_tick() AS INTEGER);",
    );
    assert_volatile_refusal(&message, "lnk51_tick()");
    // Deterministic linked functions keep working in logged statements.
    db.execute("INSERT INTO t VALUES (lnk51_stable(21));")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t;"),
        vec![vec![Value::Integer(42)]]
    );

    // The same rule through a Connection's session (a fresh store loading
    // the same registry entry by name).
    let shared = Database::from_store(PinkDb::new());
    // The statement is off by default (it loads and runs native code).
    shared.set_allow_sql_load_extension(true);
    let mut conn = shared.connect();
    conn.execute(&format!("LOAD EXTENSION '{name}';")).unwrap();
    conn.execute("CREATE TABLE t (a INTEGER);").unwrap();
    let err = conn
        .execute("INSERT INTO t VALUES (CAST(lnk51_tick() AS INTEGER));")
        .unwrap_err()
        .to_string();
    assert_volatile_refusal(&err, "lnk51_tick()");
    assert_eq!(
        rows_result(&mut conn, "SELECT lnk51_tick();").len(),
        1,
        "reads stay allowed through a connection"
    );
}
