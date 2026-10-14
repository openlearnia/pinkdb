//! End-to-end durability through the real `pinkdb-server` binary: rows acked
//! to a client survive a SIGKILL and are replayed from the WAL on restart.
//!
//! Determinism: the server prints its listening line before serving, so the
//! test connects only once the line is observed; the client sees a statement
//! result only after the engine has fsynced the WAL record (SyncPolicy::Sync
//! is the durable default), so killing the process after the ack proves
//! recovery, not luck.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{QueryResult, Value};
use pinkdb_server::Client;

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one test.
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_server_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Spawn the real binary on an ephemeral port and wait for the readiness
/// ("listening") line; returns the child and the address it printed.
fn start_server(file: Option<&Path>) -> (Child, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"));
    cmd.args(["--host", "127.0.0.1", "--port", "0"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if let Some(path) = file {
        cmd.arg("--file").arg(path);
    }
    let mut child = cmd.spawn().expect("spawn pinkdb-server");
    let stdout = child.stdout.take().expect("child stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let n = reader
        .read_line(&mut line)
        .expect("read pinkdb-server stdout");
    assert!(
        n > 0,
        "pinkdb-server exited before printing its listening line"
    );
    let rest = line
        .split_once("listening on ")
        .unwrap_or_else(|| panic!("readiness line missing 'listening on': {line:?}"))
        .1;
    let addr = rest
        .split_whitespace()
        .next()
        .expect("address on readiness line");
    (child, addr.to_string())
}

fn kill(child: &mut Child) {
    child.kill().expect("SIGKILL pinkdb-server");
    let status = child.wait().expect("reap pinkdb-server");
    assert!(
        !status.success(),
        "a SIGKILLed server must not exit cleanly"
    );
}

fn rows(results: Vec<QueryResult>) -> Vec<Vec<Value>> {
    assert_eq!(results.len(), 1);
    match results.into_iter().next().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected Selected, got {other:?}"),
    }
}

fn varchar(s: &str) -> Value {
    Value::Varchar(s.to_string())
}

#[test]
fn durable_rows_survive_kill_and_restart() {
    let dir = scratch("durable");
    let file = dir.join("x.pink");

    // First run: create + insert (acked), then SIGKILL.
    let (mut child, addr) = start_server(Some(&file));
    let mut client = Client::connect(&addr).expect("connect");
    client
        .execute("CREATE TABLE t (a INTEGER, v VARCHAR);")
        .unwrap();
    client
        .execute("INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three');")
        .unwrap();
    client.ping().unwrap();
    drop(client);
    kill(&mut child);

    // Restart on the same file: the WAL replay brings the rows back.
    let (mut child, addr) = start_server(Some(&file));
    let mut client = Client::connect(&addr).expect("connect after restart");
    assert_eq!(
        rows(client.execute("SELECT a, v FROM t ORDER BY a;").unwrap()),
        vec![
            vec![Value::Integer(1), varchar("one")],
            vec![Value::Integer(2), varchar("two")],
            vec![Value::Integer(3), varchar("three")],
        ]
    );
    // A post-restart write is durable too.
    client.execute("INSERT INTO t VALUES (4, 'four');").unwrap();
    drop(client);
    kill(&mut child);

    // Third run: all four rows across two WAL generations of records.
    let (mut child, addr) = start_server(Some(&file));
    let mut client = Client::connect(&addr).expect("connect after second restart");
    assert_eq!(
        rows(client.execute("SELECT COUNT(*) FROM t;").unwrap()),
        vec![vec![Value::Bigint(4)]]
    );
    drop(client);
    kill(&mut child);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn in_memory_server_serves_sql() {
    let (mut child, addr) = start_server(None);
    let mut client = Client::connect(&addr).expect("connect");
    client.execute("CREATE TABLE t (a INTEGER);").unwrap();
    client.execute("INSERT INTO t VALUES (7);").unwrap();
    assert_eq!(
        rows(client.execute("SELECT a FROM t;").unwrap()),
        vec![vec![Value::Integer(7)]]
    );
    drop(client);
    kill(&mut child);
}

#[test]
fn concurrent_clients_survive_kill_with_exactly_once_replay() {
    let dir = scratch("concurrent");
    let file = dir.join("x.pink");

    let (mut child, addr) = start_server(Some(&file));
    let mut setup = Client::connect(&addr).expect("connect");
    setup
        .execute("CREATE TABLE t (id INTEGER, tag VARCHAR);")
        .unwrap();
    drop(setup);

    const CLIENTS: i64 = 4;
    const ROWS_PER_CLIENT: i64 = 25;
    let threads: Vec<_> = (0..CLIENTS)
        .map(|c| {
            let addr = addr.clone();
            std::thread::spawn(move || {
                let mut client = Client::connect(&addr).expect("connect concurrent client");
                for i in 0..ROWS_PER_CLIENT {
                    let id = c * ROWS_PER_CLIENT + i;
                    client
                        .execute(&format!("INSERT INTO t VALUES ({id}, 'c{c}');"))
                        .expect("acked insert");
                }
            })
        })
        .collect();
    for t in threads {
        t.join().expect("client thread");
    }
    // Every insert was acked (and fsynced) before the kill.
    kill(&mut child);

    // Restart: the WAL replay must bring back each acked row exactly once.
    let (mut child, addr) = start_server(Some(&file));
    let mut client = Client::connect(&addr).expect("connect after restart");
    let total = CLIENTS * ROWS_PER_CLIENT;
    assert_eq!(
        rows(client.execute("SELECT COUNT(*) FROM t;").unwrap()),
        vec![vec![Value::Bigint(total)]]
    );
    // The full contents, in order: no duplicate and no lost row.
    let got = rows(
        client
            .execute("SELECT id, tag FROM t ORDER BY id;")
            .unwrap(),
    );
    let expected: Vec<Vec<Value>> = (0..CLIENTS)
        .flat_map(|c| {
            (0..ROWS_PER_CLIENT).map(move |i| {
                vec![
                    Value::Integer((c * ROWS_PER_CLIENT + i) as i32),
                    varchar(&format!("c{c}")),
                ]
            })
        })
        .collect();
    assert_eq!(got, expected, "exactly-once replay");
    let expected_sum: i64 = (0..total).sum();
    assert_eq!(
        rows(client.execute("SELECT SUM(id) FROM t;").unwrap()),
        vec![vec![Value::Bigint(expected_sum)]]
    );
    drop(client);
    kill(&mut child);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn unknown_flag_is_rejected_with_usage() {
    let out = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .arg("--bogus")
        .stdin(Stdio::null())
        .output()
        .expect("run pinkdb-server");
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("unknown argument"), "{stderr}");
    assert!(stderr.contains("Usage: pinkdb-server"), "{stderr}");
}
