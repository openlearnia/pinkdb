//! Remote mode (`--host`, `client` feature): the REPL against an in-process
//! pinkdb-server, exercising the shared renderer, SQL, `USE`, and the
//! remote-mode dot-command rejections.

#![cfg(feature = "client")]

mod common;

use std::process::{Command, Stdio};

use pinkdb::Database;
use pinkdb_server::Server;

#[test]
fn remote_repl_round_trip() {
    let server = Server::bind("127.0.0.1:0", Database::new()).expect("bind");
    let addr = server.local_addr().to_string();
    let handle = server.spawn().expect("spawn");

    let input = String::from_utf8_lossy(
        b"CREATE TABLE t (a INTEGER, v VARCHAR);\n\
              INSERT INTO t VALUES (1, 'x'), (2, NULL);\n\
              SELECT a, v FROM t ORDER BY a;\n\
              USE main;\n\
              .tables\n\
              .schema t\n\
              .databases\n\
              .import nope.csv\n\
              .save nope.pink\n\
              .open nope.pink\n\
              .lake info nope\n\
              SELECT * FROM missing;\n\
              SELECT 1;\n\
              .quit\n",
    )
    .into_owned();
    let mut command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    command.args(["--host", &addr]);
    let (text, status) = common::run_merged(command, &input);
    common::assert_script_exit(status);

    // SQL ran on the server and rendered with the local renderer.
    assert!(text.contains("Table created."), "{text}");
    assert!(text.contains("Inserted 2 row(s)."), "{text}");
    assert!(text.contains("│ a       │ v       │"), "{text}");
    assert!(text.contains("│ 1       │ x       │"), "{text}");
    assert!(text.contains("│ 2       │ NULL    │"), "{text}");
    assert!(text.contains("2 rows"), "{text}");
    assert!(text.contains("Using database main."), "{text}");
    // Non-interactive runs print no timing unless asked (.timer on).
    assert!(!text.contains("Run Time (s): real "), "{text}");

    // The session survived the SQL error (the next statement still ran).
    assert!(text.contains("Error:"), "{text}");
    assert!(text.contains("missing"), "{text}");

    // File/catalog dot-commands are rejected in remote mode.
    for cmd in [
        ".tables",
        ".schema",
        ".databases",
        ".import",
        ".save",
        ".open",
        ".lake",
    ] {
        assert!(
            text.contains(&format!("Error: {cmd} is not available in remote mode")),
            "missing remote-mode rejection for {cmd}:\n{text}"
        );
    }
    handle.join();
}

#[test]
fn remote_repl_reports_a_failed_connect() {
    // Port 1 on localhost refuses connections (nothing listens there).
    let out = Command::new(env!("CARGO_BIN_EXE_pinkdb"))
        .args(["--host", "127.0.0.1:1"])
        .stdin(Stdio::null())
        .output()
        .expect("spawn pinkdb CLI");
    assert!(!out.status.success());
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(text.contains("cannot connect"), "{text}");
}

/// The remote REPL must render byte-for-byte like the embedded one: same
/// statements, same errors, same box-drawn tables. Only the banner (mode is
/// part of it) and the wall-clock timing lines may differ.
#[test]
fn remote_and_local_rendering_are_identical() {
    let server = Server::bind("127.0.0.1:0", Database::new()).expect("bind");
    let addr = server.local_addr().to_string();
    let handle = server.spawn().expect("spawn");

    let script = "CREATE TABLE t (b BOOLEAN, i INTEGER, d DOUBLE, v VARCHAR, dt DATE, ts TIMESTAMP);\n\
                  INSERT INTO t VALUES (true, 1, 1.5, 'x', DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:01');\n\
                  INSERT INTO t VALUES (NULL, NULL, NULL, NULL, NULL, NULL);\n\
                  SELECT b, i, d, v, dt, ts FROM t ORDER BY i;\n\
                  SELECT COUNT(*) FROM t;\n\
                  SELECT * FROM missing;\n\
                  SELEKT 1;\n\
                  SELECT 1 + 1;\n\
                  .quit\n";

    let run = |args: &[&str]| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
        command.args(args);
        let (out, status) = common::run_merged(command, script);
        common::assert_script_exit(status);
        out
    };

    let local = run(&[]);
    let remote = run(&["--host", &addr]);

    // Drop the mode-specific banner and the wall-clock timing lines.
    let normalize = |text: &str| -> String {
        text.lines()
            .filter(|line| {
                !line.starts_with("pinkdb v0.1.0") && !line.starts_with("Run Time (s): real ")
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let local = normalize(&local);
    let remote = normalize(&remote);
    assert_eq!(local, remote, "remote rendering diverged from local");
    // Sanity: the script actually produced tables and errors.
    assert!(local.contains("│ b"), "{local}");
    assert!(local.contains("Error:"), "{local}");
    handle.join();
}
