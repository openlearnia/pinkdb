//! Wave 109: `current_user()` and `current_roles()` — the identity seam the
//! memory extension reads its caller through.
//!
//! Both are bound to literals at bind time (a zero-argument `ScalarFunc` would
//! compute a chunk length from its first argument and produce no rows), and the
//! `$current_user` dependency they record puts them under the existing rule
//! that a statement the engine logs may not read session state.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{Database, PinkDb, QueryResult, SyncPolicy, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_current_user_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cells(results: Vec<QueryResult>) -> Vec<Value> {
    match results.into_iter().last().unwrap() {
        QueryResult::Selected { rows, .. } => rows.into_iter().flatten().collect(),
        other => panic!("expected Selected, got {other:?}"),
    }
}

fn text(results: Vec<QueryResult>) -> String {
    match cells(results).pop().unwrap() {
        Value::Varchar(s) => s,
        other => panic!("expected VARCHAR, got {other:?}"),
    }
}

/// Run one statement and answer its first cell as text.
trait Query {
    fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>, pinkdb::PinkDbError>;
}

impl Query for PinkDb {
    fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>, pinkdb::PinkDbError> {
        PinkDb::execute(self, sql)
    }
}

impl Query for pinkdb::Connection {
    fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>, pinkdb::PinkDbError> {
        pinkdb::Connection::execute(self, sql)
    }
}

fn exec(db: &mut impl Query, sql: &str) -> String {
    text(db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")))
}

/// A store with `alice` (in `ops`, which is in `Analyst` — the as-created
/// spelling proves the display name, not the lowercased key, comes back).
fn store() -> Database {
    let db = Database::from_store(PinkDb::new());
    let mut conn = db.connect();
    for sql in [
        "CREATE USER alice PASSWORD 'alice-secret'",
        "CREATE ROLE Analyst",
        "CREATE ROLE ops",
        "GRANT Analyst TO ops",
        "GRANT ops TO alice",
        "CREATE USER bob PASSWORD 'bob-secret'",
    ] {
        conn.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    db
}

#[test]
fn an_embedded_store_is_the_implicit_principal() {
    let mut db = PinkDb::new();
    assert_eq!(exec(&mut db, "SELECT current_user()"), "pinkdb");
    assert_eq!(exec(&mut db, "SELECT current_roles()"), "");
    // The store's own handle is the trusted session, so this is also what an
    // extension's bookkeeping channel sees when the CLI runs it.
    assert_eq!(
        cells(
            db.execute("SELECT current_user(), current_roles()")
                .unwrap()
        ),
        vec![
            Value::Varchar("pinkdb".to_string()),
            Value::Varchar(String::new())
        ]
    );
}

#[test]
fn a_login_is_the_user_and_roles_are_display_names() {
    let db = store();
    let mut alice = db.connect();
    alice.login("alice", "alice-secret").unwrap();
    assert_eq!(
        cells(
            alice
                .execute("SELECT current_user(), current_roles()")
                .unwrap()
        ),
        vec![
            Value::Varchar("alice".to_string()),
            // Lowercased and sorted keys, rendered as created; `bob` has no
            // role at all, so his list is empty.
            Value::Varchar("Analyst,ops".to_string())
        ]
    );
    let mut bob = db.connect();
    bob.login("bob", "bob-secret").unwrap();
    assert_eq!(exec(&mut bob, "SELECT current_roles()"), "");
    // One connection's login is not another's, and a plain connect stays the
    // trusted embedded session.
    assert_eq!(exec(&mut db.connect(), "SELECT current_user()"), "pinkdb");
}

#[test]
fn logout_forgets_the_principal() {
    let db = store();
    let mut conn = db.connect();
    conn.login("alice", "alice-secret").unwrap();
    assert_eq!(exec(&mut conn, "SELECT current_user()"), "alice");
    conn.logout();
    assert_eq!(exec(&mut conn, "SELECT current_user()"), "pinkdb");
    assert_eq!(exec(&mut conn, "SELECT current_roles()"), "");
}

#[test]
fn both_are_bound_inside_expressions() {
    let db = store();
    let mut trusted = db.connect();
    trusted
        .execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2)")
        .unwrap();
    trusted.execute("GRANT SELECT ON TABLE t TO alice").unwrap();
    let mut conn = db.connect();
    conn.login("alice", "alice-secret").unwrap();
    assert_eq!(
        cells(
            conn.execute("SELECT current_user() FROM t ORDER BY a LIMIT 1")
                .unwrap()
        ),
        vec![Value::Varchar("alice".to_string())]
    );
    assert_eq!(
        cells(
            conn.execute("SELECT count(*) FROM t WHERE current_user() = 'alice'")
                .unwrap()
        ),
        vec![Value::Bigint(2)]
    );
    assert_eq!(
        cells(conn.execute("SELECT current_user() = 'ALICE'").unwrap()),
        vec![Value::Boolean(false)],
        "the answer is the lowercased login name"
    );
}

#[test]
fn an_argument_is_refused() {
    let mut db = PinkDb::new();
    for sql in ["SELECT current_user(1)", "SELECT current_roles('x')"] {
        let err = db.execute(sql).unwrap_err().to_string();
        assert!(err.contains("takes no arguments"), "{sql}: {err}");
    }
}

#[test]
fn a_logged_statement_may_not_read_identity() {
    let dir = scratch("wal");
    let path = dir.join("db.pink");
    let path = path.to_str().unwrap();
    let mut db = PinkDb::open_durable_with(path, SyncPolicy::Batch)
        .unwrap()
        .0;
    db.execute("CREATE TABLE t (who VARCHAR)").unwrap();
    // A query may read the identity freely.
    assert_eq!(exec(&mut db, "SELECT current_user()"), "pinkdb");
    // A logged statement may not: replay binds the logged text against a fresh
    // session, which would write a different owner.
    for sql in [
        "INSERT INTO t VALUES (current_user())",
        "INSERT INTO t SELECT current_roles()",
        "CREATE TABLE u AS SELECT current_user() AS who",
    ] {
        let err = db.execute(sql).unwrap_err().to_string();
        assert!(
            err.contains("session state") && err.contains("WAL-logs"),
            "{sql}: {err}"
        );
    }
    // The refused statements left nothing behind.
    assert_eq!(
        cells(db.execute("SELECT count(*) FROM t").unwrap()),
        vec![Value::Bigint(0)]
    );
    std::fs::remove_dir_all(&dir).ok();
}
