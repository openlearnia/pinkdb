//! End-to-end statement counting: the real server reports the v2 `BatchDone`
//! terminator with the exact number of statements the single parser split the
//! batch into, and a zero-statement batch is exactly one `BatchDone { 0 }`
//! frame. The client no longer parses SQL locally, so this is the only
//! statement-count source.

use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};

use pinkdb_server::protocol::{self, PROTOCOL_VERSION, Request, Response};

/// Spawn the real binary, wait for the readiness line and return the child
/// plus its address.
fn start_server() -> (Child, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"));
    cmd.args(["--host", "127.0.0.1", "--port", "0"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = cmd.spawn().expect("spawn pinkdb-server");
    let stdout = child.stdout.take().expect("child stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).expect("readiness line");
    let addr = line
        .split_once("listening on ")
        .unwrap_or_else(|| panic!("readiness line missing 'listening on': {line:?}"))
        .1
        .split_whitespace()
        .next()
        .expect("address")
        .to_string();
    (child, addr)
}

#[test]
fn statement_counts_match_executed_statements() {
    let (mut child, addr) = start_server();
    let stream = TcpStream::connect(&addr).expect("connect");
    let mut writer = stream.try_clone().unwrap();
    protocol::write_request(
        &mut writer,
        &Request::Hello {
            protocol_version: PROTOCOL_VERSION,
        },
    )
    .unwrap();
    let mut reader = BufReader::new(stream);
    assert!(matches!(
        protocol::read_response(&mut reader).unwrap(),
        Response::HelloOk { .. }
    ));

    // Three statements of three different shapes: DDL, DML, SELECT.
    protocol::write_request(
        &mut writer,
        &Request::Execute {
            sql: "CREATE TABLE t (a INTEGER); \
                  INSERT INTO t VALUES (1); \
                  SELECT a FROM t;"
                .to_string(),
        },
    )
    .unwrap();
    let mut dones = 0u32;
    loop {
        match protocol::read_response(&mut reader).unwrap() {
            Response::Done { .. } => dones += 1,
            Response::ResultSet { .. } | Response::Rows { .. } => {}
            Response::BatchDone { statements } => {
                assert_eq!(statements, 3);
                assert_eq!(dones, 3);
                break;
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    // Zero statements (empty and all-empty batches): exactly one frame.
    for sql in ["", "  ;;  "] {
        protocol::write_request(
            &mut writer,
            &Request::Execute {
                sql: sql.to_string(),
            },
        )
        .unwrap();
        assert_eq!(
            protocol::read_response(&mut reader).unwrap(),
            Response::BatchDone { statements: 0 },
            "sql {sql:?}"
        );
    }

    drop(reader);
    drop(writer);
    child.kill().expect("kill pinkdb-server");
    child.wait().expect("reap pinkdb-server");
}
