//! Wave 48: the CLI's remote-mode credentials — `--user`/`--password` (and
//! `PINKDB_PASSWORD`), the login handshake, and the refusal paths.

#![cfg(feature = "client")]

mod common;

use std::process::Command;

use pinkdb::Database;
use pinkdb_server::Server;

/// Serve `db` on an ephemeral port; the handle joins on drop.
fn serve(db: Database) -> (pinkdb_server::ServerHandle, String) {
    let server = Server::bind("127.0.0.1:0", db).expect("bind");
    let addr = server.local_addr().to_string();
    let handle = server.spawn().expect("spawn");
    (handle, addr)
}

fn run(args: &[&str], input: &str) -> (String, std::process::ExitStatus) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    command.args(args);
    common::run_merged(command, input)
}

#[test]
fn user_and_password_log_in_and_run_under_that_principal() {
    let db = Database::new();
    {
        // The bootstrap: a trusted connection creates the first user, its
        // role, and the table with a grant to the role.
        let mut conn = db.connect();
        conn.execute(
            "CREATE TABLE t (i INTEGER);\
             INSERT INTO t VALUES (1), (2);\
             CREATE USER alice PASSWORD 'alice-secret';\
             CREATE ROLE analyst;\
             GRANT analyst TO alice;\
             GRANT SELECT ON TABLE t TO analyst;",
        )
        .expect("bootstrap");
    }
    let (_handle, addr) = serve(db.clone());

    // Without credentials the server refuses statements (it has users).
    let (text, status) = run(&["--host", &addr], "SELECT * FROM t;\n.quit\n");
    common::assert_script_exit(status);
    assert!(text.contains("authentication required"), "{text}");

    // With them, the session runs as alice: the granted read works, the
    // ungranted write is refused by the engine, and the CLI says who it is.
    let (text, status) = run(
        &[
            "--host",
            &addr,
            "--user",
            "alice",
            "--password",
            "alice-secret",
        ],
        "SELECT * FROM t ORDER BY i;\nINSERT INTO t VALUES (3);\n.quit\n",
    );
    common::assert_script_exit(status);
    assert!(text.contains("logged in as alice"), "{text}");
    assert!(text.contains("│ 1       │"), "{text}");
    assert!(text.contains("permission denied"), "{text}");
    assert!(
        text.contains("INSERT privilege on table \"main.t\""),
        "{text}"
    );

    // The same credentials through the environment (no password on the
    // command line).
    let mut command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    command
        .args(["--host", &addr, "--user", "alice"])
        .env("PINKDB_PASSWORD", "alice-secret");
    let (text, status) = common::run_merged(command, "SELECT count(*) FROM t;\n.quit\n");
    common::assert_script_exit(status);
    assert!(text.contains("logged in as alice"), "{text}");
    assert!(text.contains("2"), "{text}");
}

#[test]
fn a_wrong_password_or_unknown_user_is_fatal_and_says_so() {
    let db = Database::new();
    db.connect()
        .execute("CREATE USER alice PASSWORD 'alice-secret';")
        .unwrap();
    let (_handle, addr) = serve(db);

    let (text, status) = run(
        &["--host", &addr, "--user", "alice", "--password", "wrong"],
        "SELECT 1;\n",
    );
    assert_eq!(status.code(), Some(1), "{text}");
    assert!(text.contains("cannot log in as alice"), "{text}");
    assert!(text.contains("authentication failed"), "{text}");

    let (text, status) = run(
        &["--host", &addr, "--user", "nobody", "--password", "x"],
        "SELECT 1;\n",
    );
    assert_eq!(status.code(), Some(1), "{text}");
    assert!(text.contains("authentication failed"), "{text}");
}

#[test]
fn credential_flags_are_validated_before_connecting() {
    // Without --host there is nothing to log in to.
    let (text, status) = run(&["--user", "alice", "--password", "x"], "");
    assert_eq!(status.code(), Some(2), "{text}");
    assert!(text.contains("--user/--password require --host"), "{text}");
    // A password without a user is a mistake, not a fallback.
    let (text, status) = run(&["--host", "127.0.0.1:1", "--password", "x"], "");
    assert_eq!(status.code(), Some(2), "{text}");
    assert!(text.contains("--password requires --user"), "{text}");
    // A user without any password source is refused too (no implicit empty
    // password).
    let (text, status) = run(&["--host", "127.0.0.1:1", "--user", "alice"], "");
    assert_eq!(status.code(), Some(2), "{text}");
    assert!(text.contains("--user needs a password"), "{text}");
}

#[test]
fn a_trusted_server_ignores_absent_credentials() {
    // No users: the server is trusted, so a plain --host session runs SQL —
    // and the login flags are not required (they are an opt-in feature).
    let (_handle, addr) = serve(Database::new());
    let (text, status) = run(&["--host", &addr], "CREATE TABLE t (i INTEGER);\n.quit\n");
    common::assert_script_exit(status);
    assert!(text.contains("Table created."), "{text}");
}
