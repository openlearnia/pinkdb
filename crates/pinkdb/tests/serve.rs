//! Wave 105: the host half of the MCP bridge — what a served extension may
//! run, and the text it gets back.
//!
//! The wire protocol is tested in `extensions/mcp/tests/protocol.rs` and the
//! end-to-end path in `crates/pinkdb-cli/tests/mcp.rs`; this file pins the
//! policy where it is enforced, for the embedded store and for a connection
//! (the server's path).

use pinkdb::agent::AgentDisplay;
use pinkdb::serve::{ServePolicy, execute, execute_on};
use pinkdb::{Database, PinkDb};

fn read_only() -> ServePolicy {
    ServePolicy::default()
}

fn writable() -> ServePolicy {
    ServePolicy {
        allow_writes: true,
        ..ServePolicy::default()
    }
}

fn refused(code: i32, text: &str, sql: &str) {
    assert_eq!(code, 1, "{sql} must be refused, got: {text}");
    assert!(
        text.contains("read-only"),
        "{sql} must name the policy: {text}"
    );
}

#[test]
fn read_only_refuses_every_write_path() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (x INTEGER)").unwrap();
    let writes = [
        "INSERT INTO t VALUES (1)",
        "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING",
        "INSERT OR REPLACE INTO t VALUES (1)",
        "INSERT OR IGNORE INTO t VALUES (1)",
        "INSERT INTO t VALUES (1) RETURNING *",
        "UPDATE t SET x = 2",
        "UPDATE t SET x = 2 RETURNING *",
        "DELETE FROM t",
        "DELETE FROM t RETURNING *",
        "WITH x AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM x",
        "WITH x AS (UPDATE t SET x = 1 RETURNING *) SELECT * FROM x",
        "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x",
        "MERGE INTO t USING t AS s ON t.x = s.x WHEN MATCHED THEN DELETE",
        "DROP TABLE t",
        "CREATE TABLE u (x INTEGER)",
        "CREATE VIEW v AS SELECT * FROM t",
        "CREATE INDEX i ON t (x)",
        "COPY t TO '/tmp/pinkdb-wave105-copy.csv'",
        "LOAD EXTENSION '/nonexistent/pinkdb_nope.dylib'",
        "INSTALL math_ext",
        "FORCE INSTALL math_ext FROM 'https://example.test'",
        "LOAD math_ext",
        "BEGIN",
        "CHECKPOINT",
        "CREATE USER alice PASSWORD 'hunter2'",
        "GRANT SELECT ON TABLE t TO alice",
        "CALL p()",
    ];
    for sql in writes {
        let (code, text) = execute(&mut db, &read_only(), sql);
        refused(code, &text, sql);
    }
    // Nothing ran: the table is intact and empty, and `u` was never created.
    let (code, text) = execute(&mut db, &read_only(), "SELECT count(*) AS n FROM t");
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("| 0 |"), "{text}");
    let (code, text) = execute(&mut db, &read_only(), "SELECT * FROM u");
    assert_eq!(code, 1);
    assert!(text.contains("not found"), "{text}");
}

#[test]
fn a_batch_with_one_write_is_refused_whole() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (x INTEGER)").unwrap();
    let (code, text) = execute(
        &mut db,
        &read_only(),
        "INSERT INTO t VALUES (1); SELECT * FROM t",
    );
    refused(code, &text, "INSERT; SELECT");
    let (_, text) = execute(&mut db, &read_only(), "SELECT count(*) AS n FROM t");
    assert!(
        text.contains("| 0 |"),
        "the insert must not have run: {text}"
    );
}

#[test]
fn reads_are_allowed() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (x INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (7)").unwrap();
    for sql in [
        "SELECT x FROM t",
        "EXPLAIN SELECT x FROM t",
        "DESCRIBE t",
        "SHOW TABLES",
        "SHOW DATABASES",
        "SHOW MACROS",
        "SHOW MATERIALIZED VIEWS",
    ] {
        let (code, text) = execute(&mut db, &read_only(), sql);
        assert_eq!(code, 0, "{sql} must be allowed: {text}");
    }
}

#[test]
fn writes_run_when_the_host_allows_them() {
    let mut db = PinkDb::new();
    let (code, text) = execute(&mut db, &writable(), "CREATE TABLE t (x INTEGER)");
    assert_eq!(code, 0, "{text}");
    let (code, text) = execute(&mut db, &writable(), "INSERT INTO t VALUES (1), (2)");
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("Inserted 2 rows"), "{text}");
    let (code, text) = execute(&mut db, &writable(), "SELECT x FROM t ORDER BY x");
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("| x:INTEGER |"), "{text}");

    // With writes allowed the policy lets `LOAD EXTENSION` reach the store,
    // which refuses the SQL statement on its own (off by default) — the
    // refusal is no longer the policy's.
    let (code, text) = execute(&mut db, &writable(), "LOAD EXTENSION '/tmp/nope.dylib'");
    assert_eq!(code, 1, "{text}");
    assert!(!text.contains("read-only"), "{text}");

    // The same holds for the repository statements: the policy lets them
    // through, and the store then refuses them (`INSTALL` needs the `repo`
    // feature here, `LOAD` finds nothing installed).
    for sql in ["INSTALL math_ext", "LOAD math_ext"] {
        let (code, text) = execute(&mut db, &writable(), sql);
        assert_eq!(code, 1, "{sql}: {text}");
        assert!(!text.contains("read-only"), "{sql}: {text}");
    }
}

#[test]
fn the_connection_session_applies_the_same_policy() {
    let db = Database::new();
    let mut conn = db.connect();
    let (code, text) = execute_on(&mut conn, &read_only(), "CREATE TABLE t (x INTEGER)");
    refused(code, &text, "CREATE TABLE");
    let (code, text) = execute_on(&mut conn, &writable(), "CREATE TABLE t (x INTEGER)");
    assert_eq!(code, 0, "{text}");
    let (code, text) = execute_on(&mut conn, &writable(), "INSERT INTO t VALUES (1)");
    assert_eq!(code, 0, "{text}");
    let (code, text) = execute_on(&mut conn, &read_only(), "SELECT x FROM t");
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("| 1 |"), "{text}");
}

#[test]
fn the_display_cap_truncates_and_says_so() {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE big (x INTEGER)").unwrap();
    let values: Vec<String> = (1..=150).map(|n| format!("({n})")).collect();
    db.execute(&format!("INSERT INTO big VALUES {}", values.join(", ")))
        .unwrap();
    let policy = ServePolicy {
        display: AgentDisplay {
            max_rows: 10,
            ..AgentDisplay::DEFAULT
        },
        ..ServePolicy::default()
    };
    let (code, text) = execute(&mut db, &policy, "SELECT x FROM big ORDER BY x");
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("more rows"), "{text}");
    assert!(text.contains("150 total"), "{text}");
    // The cap is a rendering decision; the row budget is a hard stop.
    let small = ServePolicy {
        limits: pinkdb::ExecLimits {
            max_result_rows: Some(5),
            ..pinkdb::ExecLimits::none()
        },
        ..ServePolicy::default()
    };
    let (code, text) = execute(&mut db, &small, "SELECT x FROM big");
    assert_eq!(code, 1, "{text}");
    assert!(text.starts_with('{'), "a JSON error line: {text}");
}

#[test]
fn lake_commands_need_a_host_that_serves_them() {
    let mut db = PinkDb::new();
    let (code, text) = execute(&mut db, &read_only(), ".lake tables '/tmp/lake'");
    assert_eq!(code, 1);
    assert!(text.contains("lake"), "{text}");
}

#[test]
fn errors_are_one_json_line() {
    let mut db = PinkDb::new();
    let (code, text) = execute(&mut db, &read_only(), "SELECT * FROM no_such_table");
    assert_eq!(code, 1);
    assert_eq!(text.lines().count(), 1, "{text}");
    assert!(text.starts_with("{\"error\""), "{text}");
    assert!(text.contains("\"kind\":\"catalog\""), "{text}");
}
