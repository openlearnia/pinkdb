//! In-process integration tests for the async transport (`async-server`
//! feature): the same [`Client`] and the same raw protocol frames that
//! `tests/server.rs` drives against the blocking server, against
//! [`AsyncServer`] over real TCP.
//!
//! The byte-parity harness at the bottom is the "no protocol change" proof:
//! one script of requests is sent to a blocking server and an async server
//! (fresh, identical databases) and the raw response frames must be
//! byte-identical.

#![cfg(feature = "async-server")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::Barrier;
use std::time::Duration;

use pinkdb::{DataType, Database, PinkDb, QueryResult, Value};
use pinkdb_server::protocol::{
    self, MAX_FRAME_SIZE, PROTOCOL_VERSION, Request, Response, msg_type,
};
use pinkdb_server::{AsyncServer, AsyncServerHandle, Client, Server, ServerHandle};

/// Start an async server on an ephemeral port; the handle joins on drop.
fn start() -> (AsyncServerHandle, String) {
    let server = AsyncServer::bind("127.0.0.1:0", Database::new()).expect("bind ephemeral server");
    let addr = server.local_addr().to_string();
    let handle = server.spawn().expect("spawn async server");
    (handle, addr)
}

/// A brand-new client must be able to run SQL no matter what earlier
/// (possibly malformed) connections did.
fn assert_server_healthy(addr: &str) {
    let mut c = Client::connect(addr).expect("healthy client connects");
    assert_eq!(
        rows(c.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
}

/// Raw protocol client used to inspect exact frames (as in `tests/server.rs`).
struct Raw {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Raw {
    fn connect(addr: &str) -> Raw {
        let stream = TcpStream::connect(addr).expect("connect raw");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut writer = stream.try_clone().unwrap();
        protocol::write_request(
            &mut writer,
            &Request::Hello {
                protocol_version: PROTOCOL_VERSION,
            },
        )
        .unwrap();
        let mut reader = BufReader::new(stream);
        match protocol::read_response(&mut reader).unwrap() {
            Response::HelloOk { .. } => {}
            other => panic!("expected HelloOk, got {other:?}"),
        }
        Raw { reader, writer }
    }

    fn execute(&mut self, sql: &str) {
        protocol::write_request(
            &mut self.writer,
            &Request::Execute {
                sql: sql.to_string(),
            },
        )
        .unwrap();
    }

    fn read(&mut self) -> Response {
        protocol::read_response(&mut self.reader).expect("read response")
    }

    /// The peer closed the connection: the next raw byte read is EOF.
    fn expect_eof(&mut self) {
        let mut byte = [0u8; 1];
        let n = self.reader.read(&mut byte).expect("read after close");
        assert_eq!(n, 0, "expected the server to close the connection");
    }
}

fn single(mut results: Vec<QueryResult>) -> QueryResult {
    assert_eq!(results.len(), 1, "expected exactly one statement result");
    results.pop().unwrap()
}

fn rows(results: Vec<QueryResult>) -> Vec<Vec<Value>> {
    match single(results) {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected Selected, got {other:?}"),
    }
}

fn varchar(s: &str) -> Value {
    Value::Varchar(s.to_string())
}

// ---------- the existing client over the async transport ----------

#[test]
fn create_insert_select_round_trips_every_value_type_and_nulls() {
    let (handle, addr) = start();
    let mut c = Client::connect(&addr).expect("connect");
    c.execute(
        "CREATE TABLE t (b BOOLEAN, i INTEGER, bi BIGINT, d DOUBLE, v VARCHAR, dt DATE, ts TIMESTAMP);",
    )
    .unwrap();
    c.execute(
        "INSERT INTO t VALUES \
         (true, 1, 2, 3.5, 'x', DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:01.5'), \
         (false, -1, -2, -0.5, '', DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59'), \
         (NULL, NULL, NULL, NULL, NULL, NULL, NULL);",
    )
    .unwrap();

    let result = single(
        c.execute("SELECT b, i, bi, d, v, dt, ts FROM t ORDER BY i;")
            .unwrap(),
    );
    let QueryResult::Selected { names, types, rows } = result else {
        panic!("expected Selected");
    };
    assert_eq!(names, vec!["b", "i", "bi", "d", "v", "dt", "ts"]);
    assert_eq!(
        types,
        vec![
            DataType::Boolean,
            DataType::Integer,
            DataType::Bigint,
            DataType::Double,
            DataType::Varchar,
            DataType::Date,
            DataType::Timestamp,
        ]
    );
    assert_eq!(rows.len(), 3);
    // ORDER BY i ascending, NULLS LAST: -1 first, then 1, then the NULL row.
    assert_eq!(
        rows[0],
        vec![
            Value::Boolean(false),
            Value::Integer(-1),
            Value::Bigint(-2),
            Value::Double(-0.5),
            varchar(""),
            Value::Date(pinkdb::types::days_from_civil(1969, 12, 31)),
            Value::Timestamp(pinkdb::types::parse_timestamp("1969-12-31 23:59:59").unwrap()),
        ]
    );
    assert_eq!(
        rows[1],
        vec![
            Value::Boolean(true),
            Value::Integer(1),
            Value::Bigint(2),
            Value::Double(3.5),
            varchar("x"),
            Value::Date(pinkdb::types::days_from_civil(2024, 1, 1)),
            Value::Timestamp(pinkdb::types::parse_timestamp("2024-01-01 00:00:01.5").unwrap()),
        ]
    );
    assert_eq!(rows[2], vec![Value::Null; 7]);
    handle.join();
}

#[test]
fn multi_statement_execute_returns_each_result_in_order() {
    let (handle, addr) = start();
    let mut c = Client::connect(&addr).expect("connect");
    let results = c
        .execute(
            "CREATE TABLE t (a INTEGER); \
             INSERT INTO t VALUES (1), (2); \
             SELECT COUNT(*) FROM t; \
             BEGIN; \
             INSERT INTO t VALUES (3); \
             COMMIT;",
        )
        .unwrap();
    assert_eq!(results.len(), 6);
    assert_eq!(results[0], QueryResult::Created);
    assert_eq!(results[1], QueryResult::Inserted { count: 2 });
    match &results[2] {
        QueryResult::Selected { rows, .. } => {
            assert_eq!(rows, &vec![vec![Value::Bigint(2)]]);
        }
        other => panic!("expected Selected, got {other:?}"),
    }
    assert_eq!(results[3], QueryResult::Began);
    assert_eq!(results[4], QueryResult::Inserted { count: 1 });
    assert_eq!(results[5], QueryResult::Committed);
    assert_eq!(
        rows(c.execute("SELECT COUNT(*) FROM t;").unwrap()),
        vec![vec![Value::Bigint(3)]]
    );
    handle.join();
}

#[test]
fn sql_errors_are_reported_without_dropping_the_connection() {
    let (handle, addr) = start();
    let mut c = Client::connect(&addr).expect("connect");
    c.execute("CREATE TABLE t (a INTEGER);").unwrap();

    let err = c.execute("SELECT * FROM missing;").unwrap_err();
    assert!(err.to_string().contains("missing"), "{err}");

    // The same connection is still usable.
    assert_eq!(
        rows(c.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    handle.join();
}

#[test]
fn use_sessions_are_isolated_per_client() {
    let (handle, addr) = start();
    let mut a = Client::connect(&addr).expect("connect a");
    a.execute(
        "CREATE DATABASE db1; \
         CREATE TABLE main.t (v VARCHAR); \
         CREATE TABLE db1.t (v VARCHAR);",
    )
    .unwrap();
    a.execute("USE db1;").unwrap();
    a.execute("INSERT INTO t VALUES ('a');").unwrap();

    // A second, concurrently live client stays on `main`.
    let addr_b = addr.clone();
    let b_thread = std::thread::spawn(move || {
        let mut b = Client::connect(&addr_b).expect("connect b");
        assert_eq!(b.execute("USE db1;").unwrap().len(), 1);
        b.execute("USE main;").unwrap();
        b.execute("INSERT INTO t VALUES ('b');").unwrap();
        assert_eq!(
            rows(b.execute("SELECT v FROM t;").unwrap()),
            vec![vec![varchar("b")]]
        );
        b
    });
    let mut b = b_thread.join().unwrap();

    // A's session is still db1; B's is still main; qualified names cross.
    assert_eq!(
        rows(a.execute("SELECT v FROM t;").unwrap()),
        vec![vec![varchar("a")]]
    );
    assert_eq!(
        rows(b.execute("SELECT v FROM t;").unwrap()),
        vec![vec![varchar("b")]]
    );
    assert_eq!(
        rows(a.execute("SELECT v FROM main.t;").unwrap()),
        vec![vec![varchar("b")]]
    );
    assert_eq!(
        rows(b.execute("SELECT v FROM db1.t;").unwrap()),
        vec![vec![varchar("a")]]
    );
    handle.join();
}

#[test]
fn ping_round_trips() {
    let (handle, addr) = start();
    let mut raw = Raw::connect(&addr);
    protocol::write_request(&mut raw.writer, &Request::Ping).unwrap();
    assert_eq!(raw.read(), Response::Pong);
    // The connection keeps working after a Ping.
    raw.execute("SELECT 42;");
    assert!(matches!(raw.read(), Response::ResultSet { .. }));
    handle.join();
}

#[test]
fn prepared_statements_round_trip_with_parameters() {
    let (handle, addr) = start();
    let mut c = Client::connect(&addr).unwrap();
    c.execute("CREATE TABLE p (id INTEGER, v VARCHAR);")
        .unwrap();

    // Insert through placeholders: the parameter count comes back from the
    // server, and the statement is reusable with fresh bindings.
    let (insert, params) = c.prepare("INSERT INTO p VALUES (?, ?)").unwrap();
    assert_eq!(params, 2);
    for (id, v) in [(1_i32, "one"), (2, "two")] {
        c.execute_prepared(insert, &[Value::Integer(id), varchar(v)])
            .unwrap();
    }

    // A prepared SELECT with a bound predicate, framed like a normal Execute.
    let (select, params) = c.prepare("SELECT v FROM p WHERE id = ?").unwrap();
    assert_eq!(params, 1);
    assert_eq!(
        rows(c.execute_prepared(select, &[Value::Integer(2)]).unwrap()),
        vec![vec![varchar("two")]]
    );

    // A bind-type error is a normal server error: nothing executes and the
    // connection stays usable.
    assert!(
        c.execute_prepared(insert, &[varchar("x"), Value::Integer(1)])
            .is_err()
    );
    assert_eq!(
        rows(c.execute("SELECT COUNT(*) FROM p;").unwrap()),
        vec![vec![Value::Bigint(2)]]
    );

    // An unknown handle and an argument-count mismatch are clean errors.
    let err = c.execute_prepared(987_654, &[]).unwrap_err();
    assert!(
        err.to_string()
            .contains("unknown prepared statement handle")
    );
    assert!(c.execute_prepared(select, &[]).is_err());

    assert_server_healthy(&addr);
    handle.join();
}

// ---------- handshake and malformed frames ----------

#[test]
fn hello_is_required_first_and_a_mismatch_is_rejected() {
    let (handle, addr) = start();

    // First request is not a Hello: a clean Error frame, then close.
    let stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    protocol::write_request(&mut writer, &Request::Ping).unwrap();
    match protocol::read_response(&mut reader).unwrap() {
        Response::Error { message } => assert!(message.contains("expected a Hello"), "{message}"),
        other => panic!("expected the handshake Error, got {other:?}"),
    }

    // A version mismatch is rejected with the same text as the blocking
    // server, then close.
    let stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    protocol::write_request(
        &mut writer,
        &Request::Hello {
            protocol_version: PROTOCOL_VERSION + 1,
        },
    )
    .unwrap();
    match protocol::read_response(&mut reader).unwrap() {
        Response::Error { message } => {
            assert!(message.contains("protocol version mismatch"), "{message}");
            assert!(
                message.contains(&format!("v{PROTOCOL_VERSION}")),
                "{message}"
            );
        }
        other => panic!("expected the version Error, got {other:?}"),
    }

    // A duplicate Hello on a live connection is a clean error; the
    // connection stays usable.
    let mut raw = Raw::connect(&addr);
    protocol::write_request(
        &mut raw.writer,
        &Request::Hello {
            protocol_version: PROTOCOL_VERSION,
        },
    )
    .unwrap();
    match raw.read() {
        Response::Error { message } => assert!(message.contains("duplicate Hello"), "{message}"),
        other => panic!("expected the duplicate-Hello Error, got {other:?}"),
    }
    raw.execute("SELECT 7;");
    assert!(matches!(raw.read(), Response::ResultSet { .. }));

    assert_server_healthy(&addr);
    handle.join();
}

#[test]
fn malformed_raw_connections_leave_the_server_healthy() {
    let (handle, addr) = start();
    assert_server_healthy(&addr);

    // An announced frame larger than the 64 MiB cap is rejected cleanly.
    {
        let mut raw = Raw::connect(&addr);
        raw.writer
            .write_all(&((MAX_FRAME_SIZE as u32 + 1).to_le_bytes()))
            .unwrap();
        match raw.read() {
            Response::Error { message } => {
                assert!(message.contains("exceeds"), "{message}")
            }
            other => panic!("expected the oversized-frame Error, got {other:?}"),
        }
        raw.expect_eof();
    }

    // An unknown request tag is a clean malformed-frame error.
    {
        let mut raw = Raw::connect(&addr);
        raw.writer.write_all(&1u32.to_le_bytes()).unwrap();
        raw.writer.write_all(&[200]).unwrap();
        match raw.read() {
            Response::Error { message } => {
                assert!(
                    message.contains("unknown request message type"),
                    "{message}"
                )
            }
            other => panic!("expected the malformed-frame Error, got {other:?}"),
        }
        raw.expect_eof();
    }

    // A zero-length frame (no message type) is rejected.
    {
        let mut raw = Raw::connect(&addr);
        raw.writer.write_all(&0u32.to_le_bytes()).unwrap();
        match raw.read() {
            Response::Error { message } => assert!(message.contains("empty frame"), "{message}"),
            other => panic!("expected the empty-frame Error, got {other:?}"),
        }
        raw.expect_eof();
    }

    // A connection that dies mid-prefix (and one closed outright) must not
    // disturb anything.
    {
        let mut stream = TcpStream::connect(&addr).unwrap();
        stream.write_all(&[1, 2]).unwrap();
        stream.shutdown(Shutdown::Both).unwrap();
    }
    {
        let raw = Raw::connect(&addr);
        let _ = raw.writer.shutdown(Shutdown::Both);
    }

    assert_server_healthy(&addr);
    handle.join();
}

// ---------- connection cap ----------

#[test]
fn connection_cap_rejects_excess_and_admits_after_a_slot_frees() {
    let server = AsyncServer::bind_with_max_connections("127.0.0.1:0", Database::new(), 2).unwrap();
    let addr = server.local_addr().to_string();
    let handle = server.spawn().unwrap();

    let mut a = Client::connect(&addr).unwrap();
    let mut b = Client::connect(&addr).unwrap();
    a.execute("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (1);")
        .unwrap();

    // Over the cap: connect fails with the server's limit error.
    let err = Client::connect(&addr).unwrap_err();
    assert!(err.to_string().contains("connection limit"), "{err}");

    // Existing clients are unaffected.
    assert_eq!(
        rows(a.execute("SELECT v FROM t;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    assert_eq!(
        rows(b.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );

    // Freeing a slot admits a new connection (the connection task decrements
    // its guard on EOF).
    drop(b);
    let mut admitted = None;
    for _ in 0..100 {
        match Client::connect(&addr) {
            Ok(c) => {
                admitted = Some(c);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    let mut c = admitted.expect("a slot must free after a client disconnects");
    assert_eq!(
        rows(c.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    handle.join();
}

/// N clients hold the cap while pipelining: each writes a whole batch of
/// requests before reading any response (`SELECT <n>` per client, all
/// written up front), and the cap is enforced while they do it. This is the
/// concurrency stress: overlapping engine calls on the blocking pool, one
/// connection task per client, in-order pipelined responses.
#[test]
fn pipelined_clients_keep_the_cap_enforced_and_responses_in_order() {
    const CLIENTS: usize = 4;
    const REQUESTS: usize = 30;
    let server =
        AsyncServer::bind_with_max_connections("127.0.0.1:0", Database::new(), CLIENTS).unwrap();
    let addr = server.local_addr().to_string();
    let handle = server.spawn().unwrap();

    // Barrier 1: every worker has connected (and holds its slot). Barrier 2:
    // the cap check below is done, workers may pipeline.
    let arrived = std::sync::Arc::new(Barrier::new(CLIENTS + 1));
    let go = std::sync::Arc::new(Barrier::new(CLIENTS + 1));

    let workers: Vec<_> = (0..CLIENTS)
        .map(|client| {
            let addr = addr.clone();
            let arrived = arrived.clone();
            let go = go.clone();
            std::thread::spawn(move || {
                let mut raw = Raw::connect(&addr);
                arrived.wait();
                go.wait();

                // Pipeline: write every request before reading any response.
                for n in 0..REQUESTS {
                    protocol::write_request(
                        &mut raw.writer,
                        &Request::Execute {
                            sql: format!("SELECT {};", client * 1000 + n),
                        },
                    )
                    .unwrap();
                }
                // Each `SELECT` yields ResultSet + Rows + Done, then the
                // batch terminator.
                for n in 0..REQUESTS {
                    assert!(matches!(raw.read(), Response::ResultSet { .. }));
                    match raw.read() {
                        Response::Rows { rows } => {
                            assert_eq!(
                                rows,
                                vec![vec![Value::Integer((client * 1000 + n) as i32)]]
                            );
                        }
                        other => panic!("expected Rows, got {other:?}"),
                    }
                    assert!(matches!(raw.read(), Response::Done { .. }));
                    assert_eq!(
                        raw.read(),
                        Response::BatchDone { statements: 1 },
                        "pipelined batch terminators must arrive in order"
                    );
                }
            })
        })
        .collect();

    arrived.wait();
    // All four slots are held: a fifth connection is rejected.
    let err = Client::connect(&addr).unwrap_err();
    assert!(err.to_string().contains("connection limit"), "{err}");
    go.wait();

    for worker in workers {
        worker.join().expect("worker finishes its pipelines");
    }

    // After the workers disconnect, a fresh client is admitted again.
    let mut c = Client::connect(&addr).unwrap();
    assert_eq!(
        rows(c.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    handle.join();
}

// ---------- idle timeout ----------

#[test]
fn idle_clients_are_disconnected_and_announced_frames_are_not_preallocated() {
    let server = AsyncServer::bind("127.0.0.1:0", Database::new())
        .unwrap()
        .idle_timeout(Some(Duration::from_millis(300)));
    let addr = server.local_addr().to_string();
    let _handle = server.spawn().unwrap();

    // A client that handshakes and then goes silent is dropped.
    let mut idle = Raw::connect(&addr);
    idle.expect_eof();

    // A client that announces a maximum-size frame and sends nothing more:
    // the server must time it out (the body is never sized from the prefix).
    let mut stalled = Raw::connect(&addr);
    stalled
        .writer
        .write_all(&(MAX_FRAME_SIZE as u32).to_le_bytes())
        .unwrap();
    stalled.writer.write_all(&[msg_type::EXECUTE]).unwrap();
    stalled.expect_eof();

    assert_server_healthy(&addr);
}

// ---------- panics are isolated ----------

#[test]
fn statement_panics_are_caught_and_connections_stay_usable() {
    use pinkdb_server::server::STATEMENT_HOOK;
    *STATEMENT_HOOK.lock().unwrap() = Some(|sql: &str| {
        if sql.contains("__PANIC__") {
            panic!("injected test panic");
        }
    });
    let (handle, addr) = start();
    let mut a = Client::connect(&addr).unwrap();
    let mut b = Client::connect(&addr).unwrap();
    a.execute("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (1);")
        .unwrap();

    let err = a.execute("SELECT '__PANIC__';").unwrap_err();
    assert!(
        err.to_string()
            .contains("internal error: statement panicked"),
        "{err}"
    );
    assert!(err.to_string().contains("injected test panic"), "{err}");

    // The same connection, and other connections, keep working.
    assert_eq!(
        rows(a.execute("SELECT v FROM t;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    assert_eq!(
        rows(b.execute("SELECT 2;").unwrap()),
        vec![vec![Value::Integer(2)]]
    );

    // The hook only fires on the marker.
    *STATEMENT_HOOK.lock().unwrap() = None;
    assert_eq!(
        rows(a.execute("SELECT 3;").unwrap()),
        vec![vec![Value::Integer(3)]]
    );
    handle.join();
}

// ---------- oversized response values ----------

#[test]
fn oversized_single_value_errors_cleanly_and_only_closes_that_connection() {
    // One value larger than the 64 MiB frame cap cannot be encoded: the
    // server must report a clean error and close that one connection.
    let big = "x".repeat(MAX_FRAME_SIZE + 1);
    let mut store = PinkDb::new();
    store
        .create_table("t", vec![("v".to_string(), DataType::Varchar)])
        .unwrap();
    store
        .table_mut("t")
        .unwrap()
        .append_row(vec![Value::Varchar(big)])
        .unwrap();
    let server = AsyncServer::bind("127.0.0.1:0", Database::from_store(store)).unwrap();
    let addr = server.local_addr().to_string();
    let handle = server.spawn().unwrap();

    let mut raw = Raw::connect(&addr);
    raw.execute("SELECT v FROM t;");
    match raw.read() {
        Response::ResultSet { names, .. } => assert_eq!(names, vec!["v".to_string()]),
        other => panic!("expected ResultSet, got {other:?}"),
    }
    match raw.read() {
        Response::Error { message } => {
            assert!(message.contains("exceeds"), "unexpected error: {message}");
        }
        other => panic!("expected the oversized-value Error, got {other:?}"),
    }
    raw.expect_eof();

    // The server, and the data, survive for other clients.
    assert_server_healthy(&addr);
    let mut c = Client::connect(&addr).unwrap();
    assert_eq!(
        rows(c.execute("SELECT COUNT(*) FROM t;").unwrap()),
        vec![vec![Value::Bigint(1)]]
    );
    handle.join();
}

// ---------- accept errors under fd pressure ----------

/// The same EMFILE survival the blocking server's integration test pins, on
/// the async accept loop: the real binary under `ulimit -n 48`, far more
/// connections than fds, and the server must keep accepting once the pressure
/// clears.
#[test]
fn accept_errors_from_fd_exhaustion_do_not_kill_the_async_server() {
    use std::process::{Child, Command, Stdio};

    /// Spawn the real async-transport binary under a low fd limit; returns
    /// child + addr.
    fn start_with_low_fd_limit() -> (Child, String) {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("ulimit -n 48; exec \"$0\" --host 127.0.0.1 --port 0 --transport async")
            .arg(env!("CARGO_BIN_EXE_pinkdb-server"))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn pinkdb-server --transport async under ulimit -n 48");
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

    let (mut child, addr) = start_with_low_fd_limit();

    // Open far more connections than the 48-fd limit allows: served sockets
    // answer HelloOk, the rest queue in the backlog and time out. The accept
    // loop must hit EMFILE and keep running.
    let mut sockets = Vec::new();
    let mut served = 0usize;
    for _ in 0..90 {
        let Ok(stream) = TcpStream::connect(&addr) else {
            continue;
        };
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let mut writer = stream.try_clone().unwrap();
        let _ = protocol::write_request(
            &mut writer,
            &Request::Hello {
                protocol_version: PROTOCOL_VERSION,
            },
        );
        let mut reader = BufReader::new(stream);
        if matches!(
            protocol::read_response(&mut reader),
            Ok(Response::HelloOk { .. })
        ) {
            served += 1;
        }
        sockets.push((reader, writer));
    }
    assert!(
        served >= 10,
        "expected fd pressure to serve some connections, got {served}"
    );
    assert!(
        child.try_wait().unwrap().is_none(),
        "the async server must still be running after EMFILE"
    );

    // Free the pressure; a new connection must be admitted.
    drop(sockets);
    std::thread::sleep(Duration::from_millis(500));
    let mut admitted = None;
    for _ in 0..40 {
        let Ok(stream) = TcpStream::connect(&addr) else {
            continue;
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut writer = stream.try_clone().unwrap();
        protocol::write_request(
            &mut writer,
            &Request::Hello {
                protocol_version: PROTOCOL_VERSION,
            },
        )
        .unwrap();
        let mut reader = BufReader::new(stream);
        match protocol::read_response(&mut reader) {
            Ok(Response::HelloOk { .. }) => {
                admitted = Some((reader, writer));
                break;
            }
            _ => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    let (mut reader, mut writer) =
        admitted.expect("the async server must admit a connection after fd pressure clears");
    protocol::write_request(
        &mut writer,
        &Request::Execute {
            sql: "SELECT 1;".to_string(),
        },
    )
    .unwrap();
    let mut got_done = false;
    loop {
        match protocol::read_response(&mut reader).unwrap() {
            Response::Done { .. } => got_done = true,
            Response::BatchDone { statements } => {
                assert_eq!(statements, 1);
                break;
            }
            Response::ResultSet { .. } | Response::Rows { .. } => {}
            other => panic!("unexpected frame {other:?}"),
        }
    }
    assert!(got_done, "the admitted connection must execute SQL");
    assert!(child.try_wait().unwrap().is_none());
    child.kill().unwrap();
    child.wait().unwrap();
}

// ---------- graceful shutdown ----------

#[test]
fn shutdown_stops_accepting_and_ends_in_flight_connections() {
    let server = AsyncServer::bind("127.0.0.1:0", Database::new()).unwrap();
    let addr = server.local_addr().to_string();
    let handle = server.spawn().unwrap();

    let mut raw = Raw::connect(&addr);
    raw.execute("SELECT 1;");
    // Drain the result completely (ResultSet + Rows + Done + BatchDone), so
    // the final byte read below sees connection teardown, not a pending
    // response.
    loop {
        if matches!(raw.read(), Response::BatchDone { .. }) {
            break;
        }
    }

    handle.shutdown();
    // The accept loop stops and the listener closes: connecting eventually
    // fails instead of hanging.
    let mut refused = false;
    for _ in 0..200 {
        match TcpStream::connect(&addr) {
            Ok(stream) => {
                drop(stream);
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(_) => {
                refused = true;
                break;
            }
        }
    }
    assert!(refused, "the listener must stop accepting after shutdown");

    // Joining ends the runtime, and with it the in-flight connection: the
    // client sees EOF instead of hanging.
    handle.join();
    raw.expect_eof();
}

// ---------- byte-parity harness: no protocol change ----------

/// Read one complete response frame (length prefix + type + body) verbatim.
fn read_frame_bytes(reader: &mut BufReader<TcpStream>) -> Option<Vec<u8>> {
    let mut prefix = [0u8; 4];
    let mut got = 0;
    while got < 4 {
        match reader.read(&mut prefix[got..]) {
            Ok(0) => return None,
            Ok(n) => got += n,
            Err(e) => panic!("read frame prefix: {e}"),
        }
    }
    let len = u32::from_le_bytes(prefix) as usize;
    assert!(len > 0 && len <= MAX_FRAME_SIZE, "bad frame length {len}");
    let mut rest = vec![0u8; len];
    reader.read_exact(&mut rest).expect("read frame body");
    let mut frame = prefix.to_vec();
    frame.extend_from_slice(&rest);
    Some(frame)
}

/// Does this frame end its request's response batch?
fn ends_batch(frame: &[u8]) -> bool {
    matches!(
        protocol::decode_response(frame).expect("decode response"),
        Response::BatchDone { .. }
            | Response::Error { .. }
            | Response::Pong
            | Response::Prepared { .. }
            | Response::LoggedIn { .. }
    )
}

/// Run one scripted connection against `addr`; returns every response frame
/// verbatim (handshake first).
fn run_script(addr: &str, requests: &[Request]) -> Vec<Vec<u8>> {
    let stream = TcpStream::connect(addr).expect("connect to parity server");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    protocol::write_request(
        &mut writer,
        &Request::Hello {
            protocol_version: PROTOCOL_VERSION,
        },
    )
    .unwrap();
    let mut frames = vec![read_frame_bytes(&mut reader).expect("HelloOk")];
    for request in requests {
        protocol::write_request(&mut writer, request).unwrap();
        loop {
            let frame = read_frame_bytes(&mut reader).expect("response frame");
            let last = ends_batch(&frame);
            frames.push(frame);
            if last {
                break;
            }
        }
    }
    frames
}

#[test]
fn async_and_blocking_servers_emit_identical_frames() {
    // Fresh, identical databases behind each transport: the same script must
    // produce byte-identical response frames. This is the "no protocol
    // change" proof for the carve-out.
    let blocking = Server::bind("127.0.0.1:0", Database::new()).unwrap();
    let blocking_addr = blocking.local_addr().to_string();
    let blocking_handle: ServerHandle = blocking.spawn().unwrap();
    let server = AsyncServer::bind("127.0.0.1:0", Database::new()).unwrap();
    let async_addr = server.local_addr().to_string();
    let handle = server.spawn().unwrap();

    let script = vec![
        Request::Ping,
        Request::Execute {
            sql: "CREATE TABLE t (b BOOLEAN, i INTEGER, bi BIGINT, d DOUBLE, v VARCHAR, \
                  dt DATE, ts TIMESTAMP);"
                .to_string(),
        },
        Request::Execute {
            sql: "INSERT INTO t VALUES \
                  (true, 1, 2, 3.5, 'x', DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:01.5'), \
                  (NULL, NULL, NULL, NULL, NULL, NULL, NULL);"
                .to_string(),
        },
        Request::Execute {
            sql: "SELECT * FROM t ORDER BY i;".to_string(),
        },
        Request::Execute {
            sql: "SELECT 'héllo ✓' AS s, 1.5 AS f;".to_string(),
        },
        // A SQL error (no BatchDone follows) and a parse error ride the same
        // frames on both transports.
        Request::Execute {
            sql: "SELECT * FROM missing;".to_string(),
        },
        Request::Execute {
            sql: "SELEKT 1;".to_string(),
        },
        // Empty SQL: zero statements, BatchDone{0}.
        Request::Execute { sql: String::new() },
        Request::Execute {
            sql: "CHECKPOINT;".to_string(),
        },
        // Prepared statements: handle numbering and frames are identical.
        Request::Prepare {
            sql: "SELECT ? + ?;".to_string(),
        },
        Request::ExecutePrepared {
            handle: 1,
            values: vec![Value::Integer(2), Value::Integer(3)],
        },
        Request::ExecutePrepared {
            handle: 999,
            values: vec![],
        },
        // Wave 48 (v9): the bootstrap creates the first user (trusted), and
        // the RBAC statement outcomes ride the same Done frames on both
        // transports.
        Request::Execute {
            sql: "CREATE USER parity_user PASSWORD 'parity-secret';\
                  CREATE ROLE parity_role;\
                  GRANT SELECT ON TABLE t TO parity_role;\
                  GRANT parity_role TO parity_user;"
                .to_string(),
        },
        // Once a user exists, an unauthenticated statement is refused with
        // the identical Error frame…
        Request::Execute {
            sql: "SELECT count(*) FROM t;".to_string(),
        },
        // … and the Login exchange (success and failure) is byte-identical.
        Request::Login {
            user: "parity_user".to_string(),
            password: "parity-secret".to_string(),
        },
        Request::Login {
            user: "parity_user".to_string(),
            password: "wrong".to_string(),
        },
        Request::Login {
            user: "nobody".to_string(),
            password: "x".to_string(),
        },
        // Authenticated: the role's SELECT opens the read, INSERT stays
        // refused, and user DDL needs ADMIN.
        Request::Execute {
            sql: "SELECT count(*) FROM t;".to_string(),
        },
        Request::Execute {
            sql: "INSERT INTO t VALUES (1, 1, 1, 1.0, 'a', DATE '2024-01-01', \
                  TIMESTAMP '2024-01-01 00:00:00');"
                .to_string(),
        },
        Request::Execute {
            sql: "ALTER USER parity_user PASSWORD 'rotated';".to_string(),
        },
        Request::Hello {
            protocol_version: PROTOCOL_VERSION,
        },
    ];

    let blocking_frames = run_script(&blocking_addr, &script);
    let async_frames = run_script(&async_addr, &script);

    assert_eq!(
        blocking_frames.len(),
        async_frames.len(),
        "the two transports answered a different number of frames"
    );
    for (i, (blocking, r#async)) in blocking_frames.iter().zip(&async_frames).enumerate() {
        assert_eq!(blocking, r#async, "frame {i} differs between transports");
    }

    // Sanity: the script really exercised a result set, an error and a
    // batch terminator (a trivially empty comparison must not pass).
    let kinds: Vec<Response> = async_frames
        .iter()
        .map(|f| protocol::decode_response(f).unwrap())
        .collect();
    assert!(
        kinds
            .iter()
            .any(|r| matches!(r, Response::BatchDone { .. }))
    );
    assert!(kinds.iter().any(|r| matches!(r, Response::Error { .. })));
    assert!(kinds.iter().any(|r| matches!(r, Response::Rows { .. })));

    handle.join();
    blocking_handle.join();
}
