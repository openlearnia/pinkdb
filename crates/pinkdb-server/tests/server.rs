//! In-process integration tests: an ephemeral-port [`Server`] and real
//! [`Client`] connections over TCP (no child process, no fixed ports).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::time::Duration;

use pinkdb::{DataType, Database, PinkDb, QueryResult, Value};
use pinkdb_server::protocol::{
    self, msg_type, Request, Response, MAX_FRAME_SIZE, PROTOCOL_VERSION,
};
use pinkdb_server::{Client, Server, ServerHandle};

/// Start a server on an ephemeral port; the handle joins on drop.
fn start() -> (ServerHandle, String) {
    let server = Server::bind("127.0.0.1:0", Database::new()).expect("bind ephemeral server");
    let addr = server.local_addr().to_string();
    let handle = server.spawn().expect("spawn server");
    (handle, addr)
}

/// A brand-new client must be able to run SQL no matter what earlier
/// (possibly malformed) connections did.
fn assert_server_healthy(addr: &str) {
    let mut c = Client::connect(addr).expect("healthy client connects");
    assert_eq!(rows(c.execute("SELECT 1;").unwrap()), vec![vec![Value::Integer(1)]]);
}

/// Raw protocol client used to inspect exact frames.
struct Raw {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Raw {
    fn connect(addr: &str) -> Raw {
        let stream = TcpStream::connect(addr).expect("connect raw");
        stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut writer = stream.try_clone().unwrap();
        protocol::write_request(
            &mut writer,
            &Request::Hello { protocol_version: PROTOCOL_VERSION },
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
            &Request::Execute { sql: sql.to_string() },
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

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&((body.len() + 1) as u32).to_le_bytes());
    bytes.push(tag);
    bytes.extend_from_slice(body);
    bytes
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

    let result = single(c.execute("SELECT b, i, bi, d, v, dt, ts FROM t ORDER BY i;").unwrap());
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
            Value::Timestamp(
                pinkdb::types::parse_timestamp("1969-12-31 23:59:59").unwrap()
            ),
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
            Value::Timestamp(
                pinkdb::types::parse_timestamp("2024-01-01 00:00:01.5").unwrap()
            ),
        ]
    );
    assert_eq!(rows[2], vec![Value::Null; 7]);
    handle.join();
}

#[test]
fn empty_results_round_trip() {
    let (handle, addr) = start();
    let mut c = Client::connect(&addr).expect("connect");
    c.execute("CREATE TABLE t (a INTEGER);").unwrap();
    let result = single(c.execute("SELECT a FROM t;").unwrap());
    match result {
        QueryResult::Selected { names, types, rows } => {
            assert_eq!(names, vec!["a"]);
            assert_eq!(types, vec![DataType::Integer]);
            assert!(rows.is_empty());
        }
        other => panic!("expected Selected, got {other:?}"),
    }
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
    assert_eq!(rows(c.execute("SELECT 1;").unwrap()), vec![vec![Value::Integer(1)]]);

    // A failing statement aborts the whole batch (all-or-nothing, like the
    // embedded `Connection::execute`), and the connection survives that too.
    let err = c
        .execute("INSERT INTO t VALUES (1); SELECT * FROM missing;")
        .unwrap_err();
    assert!(err.to_string().contains("missing"), "{err}");
    assert_eq!(rows(c.execute("SELECT COUNT(*) FROM t;").unwrap()), vec![vec![Value::Bigint(1)]]);

    // Parse errors are reported identically (parsed locally by the same
    // engine parser, before any round trip).
    let err = c.execute("SELEKT 1;").unwrap_err();
    assert!(!err.to_string().is_empty());
    assert_eq!(rows(c.execute("SELECT 2;").unwrap()), vec![vec![Value::Integer(2)]]);
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
    assert_eq!(rows(c.execute("SELECT COUNT(*) FROM t;").unwrap()), vec![vec![Value::Bigint(3)]]);
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
        assert_eq!(rows(b.execute("SELECT v FROM t;").unwrap()), vec![vec![varchar("b")]]);
        b
    });
    let mut b = b_thread.join().unwrap();

    // A's session is still db1; B's is still main; qualified names cross.
    assert_eq!(rows(a.execute("SELECT v FROM t;").unwrap()), vec![vec![varchar("a")]]);
    assert_eq!(rows(b.execute("SELECT v FROM t;").unwrap()), vec![vec![varchar("b")]]);
    assert_eq!(rows(a.execute("SELECT v FROM main.t;").unwrap()), vec![vec![varchar("b")]]);
    assert_eq!(rows(b.execute("SELECT v FROM db1.t;").unwrap()), vec![vec![varchar("a")]]);
    handle.join();
}

#[test]
fn large_result_streams_all_rows() {
    let (handle, addr) = start();
    let mut c = Client::connect(&addr).expect("connect");
    c.execute("CREATE TABLE big (a BIGINT, v VARCHAR);").unwrap();

    let mut sql = String::from("INSERT INTO big VALUES ");
    for i in 0..20_000i64 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, 'r{i}')"));
    }
    sql.push(';');
    assert_eq!(single(c.execute(&sql).unwrap()), QueryResult::Inserted { count: 20_000 });

    let out = rows(c.execute("SELECT a, v FROM big ORDER BY a;").unwrap());
    assert_eq!(out.len(), 20_000);
    for (i, row) in out.iter().enumerate() {
        assert_eq!(row, &vec![Value::Bigint(i as i64), varchar(&format!("r{i}"))], "row {i}");
    }
    handle.join();
}

#[test]
fn huge_rows_stream_in_multiple_bounded_frames() {
    let (handle, addr) = start();
    let mut c = Client::connect(&addr).expect("connect");
    c.execute("CREATE TABLE big (v VARCHAR);").unwrap();
    let cell = "x".repeat(2 * 1024 * 1024);
    for _ in 0..5 {
        c.execute(&format!("INSERT INTO big VALUES ('{cell}');")).unwrap();
    }
    drop(c);

    // Raw protocol client: 5 x 2 MiB rows (10 MiB total) must arrive in more
    // than one Rows frame — the server batches by encoded size, not just by
    // row count, so frames stay well under the 64 MiB cap.
    let stream = TcpStream::connect(&addr).unwrap();
    let mut writer = stream.try_clone().unwrap();
    protocol::write_request(
        &mut writer,
        &Request::Hello { protocol_version: PROTOCOL_VERSION },
    )
    .unwrap();
    let mut reader = BufReader::new(stream);
    match protocol::read_response(&mut reader).unwrap() {
        Response::HelloOk { .. } => {}
        other => panic!("expected HelloOk, got {other:?}"),
    }
    protocol::write_request(
        &mut writer,
        &Request::Execute { sql: "SELECT v FROM big;".to_string() },
    )
    .unwrap();
    let mut rows_frames = 0usize;
    let mut total_rows = 0usize;
    loop {
        match protocol::read_response(&mut reader).unwrap() {
            Response::ResultSet { .. } => {}
            Response::Rows { rows } => {
                rows_frames += 1;
                total_rows += rows.len();
                for row in &rows {
                    let Value::Varchar(s) = &row[0] else {
                        panic!("expected a varchar row");
                    };
                    assert_eq!(s.len(), cell.len());
                }
            }
            Response::Done { .. } => break,
            other => panic!("expected a result frame, got {other:?}"),
        }
    }
    assert_eq!(total_rows, 5);
    assert!(rows_frames >= 2, "10 MiB must split into several frames, got {rows_frames}");
    handle.join();
}

#[test]
fn ping_round_trips() {
    let (handle, addr) = start();
    let mut c = Client::connect(&addr).expect("connect");
    c.ping().unwrap();
    c.ping().unwrap();
    handle.join();
}

#[test]
fn hello_is_required_first() {
    let (handle, addr) = start();
    let stream = TcpStream::connect(&addr).unwrap();
    let mut writer = stream.try_clone().unwrap();
    protocol::write_request(&mut writer, &Request::Ping).unwrap();
    let mut reader = BufReader::new(stream);
    match protocol::read_response(&mut reader).unwrap() {
        Response::Error { message } => {
            assert!(message.contains("Hello"), "{message}");
        }
        other => panic!("expected Error, got {other:?}"),
    }
    handle.join();
}

#[test]
fn mismatched_hello_is_rejected() {
    let (handle, addr) = start();
    let stream = TcpStream::connect(&addr).unwrap();
    let mut writer = stream.try_clone().unwrap();
    protocol::write_request(
        &mut writer,
        &Request::Hello { protocol_version: PROTOCOL_VERSION + 1 },
    )
    .unwrap();
    let mut reader = BufReader::new(stream);
    match protocol::read_response(&mut reader).unwrap() {
        Response::Error { message } => {
            assert!(message.contains("protocol version mismatch"), "{message}");
            assert!(message.contains(&format!("v{}", PROTOCOL_VERSION)), "{message}");
        }
        other => panic!("expected Error, got {other:?}"),
    }
    handle.join();
}

#[test]
fn client_rejects_a_wrong_server_protocol_version() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let fake = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = stream;
        let request = protocol::read_request(&mut reader).unwrap();
        assert_eq!(request, Request::Hello { protocol_version: PROTOCOL_VERSION });
        protocol::write_response(
            &mut writer,
            &Response::HelloOk {
                server_version: "fake".to_string(),
                protocol_version: PROTOCOL_VERSION + 1,
            },
        )
        .unwrap();
    });
    let err = Client::connect(addr.to_string()).unwrap_err();
    assert!(err.to_string().contains("protocol version mismatch"), "{err}");
    fake.join().unwrap();
}

#[test]
fn client_reports_a_malformed_server_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let fake = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = stream;
        let _ = protocol::read_request(&mut reader).unwrap();
        protocol::write_response(
            &mut writer,
            &Response::HelloOk {
                server_version: "fake".to_string(),
                protocol_version: PROTOCOL_VERSION,
            },
        )
        .unwrap();
        let _ = protocol::read_request(&mut reader).unwrap();
        // An unknown response type: the client must reject it, not hang.
        protocol::write_frame(&mut writer, 200, &[]).unwrap();
    });
    let mut c = Client::connect(addr.to_string()).unwrap();
    let err = c.execute("SELECT 1;").unwrap_err();
    assert!(err.to_string().contains("unknown response message type"), "{err}");
    fake.join().unwrap();
}

// ---------- v2 batch terminator + zero-statement batches ----------

#[test]
fn zero_statement_execute_returns_batch_done_zero() {
    let (handle, addr) = start();
    // Raw protocol: an empty (or all-empty) batch is exactly one frame.
    let mut raw = Raw::connect(&addr);
    raw.execute("");
    assert_eq!(raw.read(), Response::BatchDone { statements: 0 });
    raw.execute("  ;;  ");
    assert_eq!(raw.read(), Response::BatchDone { statements: 0 });
    // The same connection stays usable.
    protocol::write_request(&mut raw.writer, &Request::Ping).unwrap();
    assert_eq!(raw.read(), Response::Pong);
    drop(raw);

    // The client mirrors that (no local parsing involved).
    let mut c = Client::connect(&addr).unwrap();
    assert!(c.execute("").unwrap().is_empty());
    assert!(c.execute(" ;; ").unwrap().is_empty());
    assert_eq!(rows(c.execute("SELECT 1;").unwrap()), vec![vec![Value::Integer(1)]]);
    handle.join();
}

#[test]
fn execute_batch_ends_with_an_exact_batch_done_count() {
    let (handle, addr) = start();
    let mut raw = Raw::connect(&addr);
    raw.execute(
        "CREATE TABLE t (a INTEGER); \
         INSERT INTO t VALUES (1), (2); \
         SELECT COUNT(*) FROM t;",
    );
    let mut dones = 0u32;
    loop {
        match raw.read() {
            Response::Done { .. } => dones += 1,
            Response::ResultSet { .. } | Response::Rows { .. } => {}
            Response::BatchDone { statements } => {
                assert_eq!(statements, 3, "BatchDone must count the batch");
                assert_eq!(dones, 3, "every statement must have sent its Done");
                break;
            }
            other => panic!("unexpected frame {other:?}"),
        }
    }
    // The connection is still usable after the terminator.
    assert_eq!(raw.read_after_ping(), Response::Pong);
    handle.join();
}

impl Raw {
    fn read_after_ping(&mut self) -> Response {
        protocol::write_request(&mut self.writer, &Request::Ping).unwrap();
        self.read()
    }
}

// ---------- malformed raw sockets never kill the server ----------

#[test]
fn malformed_raw_connections_leave_the_server_healthy() {
    let (handle, addr) = start();
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("single garbage byte", vec![0x00]),
        ("zero-length frame", vec![0, 0, 0, 0]),
        ("oversized length prefix", (MAX_FRAME_SIZE as u32 + 1).to_le_bytes().to_vec()),
        ("unknown request type", frame(200, &[])),
        ("Ping before Hello", protocol::encode_request(&Request::Ping).unwrap()),
        (
            "Hello with trailing byte",
            {
                let mut body = PROTOCOL_VERSION.to_le_bytes().to_vec();
                body.push(0);
                frame(msg_type::HELLO, &body)
            },
        ),
        (
            "truncated Execute body",
            {
                let mut body = 100u32.to_le_bytes().to_vec();
                body.extend_from_slice(b"abc");
                frame(msg_type::EXECUTE, &body)
            },
        ),
        (
            "invalid UTF-8 SQL",
            {
                let mut body = 2u32.to_le_bytes().to_vec();
                body.extend_from_slice(&[0xff, 0xfe]);
                frame(msg_type::EXECUTE, &body)
            },
        ),
        ("Ping with trailing bytes", frame(msg_type::PING, &[1])),
        (
            "mismatched protocol version",
            protocol::encode_request(&Request::Hello {
                protocol_version: PROTOCOL_VERSION + 1,
            })
            .unwrap(),
        ),
    ];
    for (label, bytes) in cases {
        let mut stream = TcpStream::connect(&addr).unwrap_or_else(|e| panic!("{label}: {e}"));
        stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut writer = stream.try_clone().unwrap();
        writer.write_all(&bytes).unwrap_or_else(|e| panic!("{label}: {e}"));
        // Half-close so the server's blocking reads see EOF.
        writer.shutdown(Shutdown::Write).unwrap();
        // Drain whatever the server answers (an Error frame, then EOF).
        let mut sink = Vec::new();
        let _ = stream.read_to_end(&mut sink);
        // Cross-connection health: a fresh client works after every case.
        assert_server_healthy(&addr);
    }
    handle.join();
}

// ---------- oversized response values ----------

#[test]
fn oversized_single_value_errors_cleanly_and_only_closes_that_connection() {
    // One value larger than the 64 MiB frame cap cannot be encoded: the
    // server must report a clean error and close that one connection.
    let big = "x".repeat(MAX_FRAME_SIZE + 1);
    let mut store = PinkDb::new();
    store.create_table("t", vec![("v".to_string(), DataType::Varchar)]).unwrap();
    store
        .table_mut("t")
        .unwrap()
        .append_row(vec![Value::Varchar(big)])
        .unwrap();
    let server = Server::bind("127.0.0.1:0", Database::from_store(store)).unwrap();
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

// ---------- connection cap ----------

#[test]
fn connection_cap_rejects_excess_and_admits_after_a_slot_frees() {
    let server =
        Server::bind_with_max_connections("127.0.0.1:0", Database::new(), 2).unwrap();
    let addr = server.local_addr().to_string();
    let handle = server.spawn().unwrap();

    let mut a = Client::connect(&addr).unwrap();
    let mut b = Client::connect(&addr).unwrap();
    a.execute("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (1);").unwrap();

    // Over the cap: connect fails with the server's limit error.
    let err = Client::connect(&addr).unwrap_err();
    assert!(err.to_string().contains("connection limit"), "{err}");

    // Existing clients are unaffected.
    assert_eq!(rows(a.execute("SELECT v FROM t;").unwrap()), vec![vec![Value::Integer(1)]]);
    assert_eq!(rows(b.execute("SELECT 1;").unwrap()), vec![vec![Value::Integer(1)]]);

    // Freeing a slot admits a new connection (the thread decrements on EOF).
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
    assert_eq!(rows(c.execute("SELECT 1;").unwrap()), vec![vec![Value::Integer(1)]]);
    handle.join();
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
    a.execute("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (1);").unwrap();

    let err = a.execute("SELECT '__PANIC__';").unwrap_err();
    assert!(err.to_string().contains("internal error: statement panicked"), "{err}");
    assert!(err.to_string().contains("injected test panic"), "{err}");

    // The same connection, and other connections, keep working.
    assert_eq!(rows(a.execute("SELECT v FROM t;").unwrap()), vec![vec![Value::Integer(1)]]);
    assert_eq!(rows(b.execute("SELECT 2;").unwrap()), vec![vec![Value::Integer(2)]]);

    // The hook only fires on the marker.
    *STATEMENT_HOOK.lock().unwrap() = None;
    assert_eq!(rows(a.execute("SELECT 3;").unwrap()), vec![vec![Value::Integer(3)]]);
    handle.join();
}

// ---------- C1: deep statements through the real server ----------

#[test]
fn over_deep_statement_is_a_clean_error_and_the_server_stays_healthy() {
    let (handle, addr) = start();
    let mut c = Client::connect(&addr).unwrap();
    c.execute("CREATE TABLE t (v INTEGER);").unwrap();

    // ~10k terms: far past the AST cap and the recursive-drop crash point.
    let mut sql = String::from("SELECT 1");
    for _ in 0..10_000 {
        sql.push_str("+1");
    }
    sql.push(';');
    let err = c.execute(&sql).unwrap_err();
    assert!(err.to_string().contains("expression too deep"), "{err}");

    // The same connection and brand-new connections keep working.
    assert_eq!(rows(c.execute("SELECT 1;").unwrap()), vec![vec![Value::Integer(1)]]);
    assert_server_healthy(&addr);
    let mut fresh = Client::connect(&addr).unwrap();
    assert_eq!(
        rows(fresh.execute("SELECT COUNT(*) FROM t;").unwrap()),
        vec![vec![Value::Bigint(0)]]
    );
    handle.join();
}

// ---------- accept errors (fd exhaustion) are transient ----------

#[test]
fn accept_errors_from_fd_exhaustion_do_not_kill_the_server() {
    use std::process::{Child, Command, Stdio};

    /// Spawn the real binary under a low fd limit; returns child + addr.
    fn start_with_low_fd_limit() -> (Child, String) {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg("ulimit -n 48; exec \"$0\" --host 127.0.0.1 --port 0")
            .arg(env!("CARGO_BIN_EXE_pinkdb-server"))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn pinkdb-server under ulimit -n 48");
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
    for _ in 0..60 {
        let Ok(stream) = TcpStream::connect(&addr) else { continue };
        stream.set_read_timeout(Some(Duration::from_millis(200))).unwrap();
        let mut writer = stream.try_clone().unwrap();
        let _ = protocol::write_request(
            &mut writer,
            &Request::Hello { protocol_version: PROTOCOL_VERSION },
        );
        let mut reader = BufReader::new(stream);
        if matches!(protocol::read_response(&mut reader), Ok(Response::HelloOk { .. })) {
            served += 1;
        }
        sockets.push((reader, writer));
    }
    assert!(served >= 20, "expected fd pressure to serve some connections, got {served}");
    assert!(
        child.try_wait().unwrap().is_none(),
        "the server must still be running after EMFILE"
    );

    // Free the pressure; a new connection must be admitted. (The pre-fix
    // server exited on the first EMFILE and could never serve again.)
    drop(sockets);
    std::thread::sleep(Duration::from_millis(500));
    let mut admitted = None;
    for _ in 0..40 {
        let Ok(stream) = TcpStream::connect(&addr) else { continue };
        stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut writer = stream.try_clone().unwrap();
        protocol::write_request(
            &mut writer,
            &Request::Hello { protocol_version: PROTOCOL_VERSION },
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
        admitted.expect("the server must admit a connection after fd pressure clears");
    protocol::write_request(
        &mut writer,
        &Request::Execute { sql: "SELECT 1;".to_string() },
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

// ---------- client malformed-response matrix ----------

#[test]
fn client_rejects_malformed_responses_and_marks_itself_broken() {
    type Emit = fn(&mut TcpStream);
    let cases: Vec<(&str, Emit)> = vec![
        ("unknown message type", |w| {
            protocol::write_frame(w, 200, &[]).unwrap();
        }),
        ("empty frame", |w| {
            w.write_all(&[0, 0, 0, 0]).unwrap();
        }),
        ("oversized frame", |w| {
            w.write_all(&(MAX_FRAME_SIZE as u32 + 1).to_le_bytes()).unwrap();
        }),
        ("truncated body", |w| {
            let mut bytes = 100u32.to_le_bytes().to_vec();
            bytes.push(msg_type::ROWS);
            bytes.extend_from_slice(&[1, 2, 3]);
            w.write_all(&bytes).unwrap();
        }),
        ("Rows before ResultSet", |w| {
            protocol::write_response(w, &Response::Rows { rows: vec![] }).unwrap();
        }),
        ("nested ResultSet frames", |w| {
            let result_set = Response::ResultSet {
                names: vec!["a".to_string()],
                types: vec![DataType::Integer],
            };
            protocol::write_response(w, &result_set).unwrap();
            protocol::write_response(w, &result_set).unwrap();
        }),
        ("Done(Selected) without ResultSet", |w| {
            protocol::write_response(
                w,
                &Response::Done {
                    kind: pinkdb_server::protocol::DoneKind::Selected,
                    count: 0,
                    name: String::new(),
                },
            )
            .unwrap();
        }),
        ("unknown Done kind", |w| {
            protocol::write_frame(w, msg_type::DONE, &[99]).unwrap();
        }),
        ("unknown value tag", |w| {
            let mut body = 1u32.to_le_bytes().to_vec();
            body.extend_from_slice(&1u32.to_le_bytes());
            body.push(99);
            protocol::write_frame(w, msg_type::ROWS, &body).unwrap();
        }),
        ("invalid UTF-8 column name", |w| {
            let mut body = 1u32.to_le_bytes().to_vec();
            body.extend_from_slice(&2u32.to_le_bytes());
            body.extend_from_slice(&[0xff, 0xfe]);
            body.push(0);
            protocol::write_frame(w, msg_type::RESULT_SET, &body).unwrap();
        }),
        ("BatchDone count mismatch", |w| {
            protocol::write_response(w, &Response::BatchDone { statements: 1 }).unwrap();
        }),
        ("BatchDone inside a result set", |w| {
            protocol::write_response(
                w,
                &Response::ResultSet {
                    names: vec!["a".to_string()],
                    types: vec![DataType::Integer],
                },
            )
            .unwrap();
            protocol::write_response(w, &Response::BatchDone { statements: 1 }).unwrap();
        }),
        ("row wider than the result set", |w| {
            protocol::write_response(
                w,
                &Response::ResultSet {
                    names: vec!["a".to_string()],
                    types: vec![DataType::Integer],
                },
            )
            .unwrap();
            protocol::write_response(
                w,
                &Response::Rows {
                    rows: vec![vec![Value::Integer(1), Value::Integer(2)]],
                },
            )
            .unwrap();
        }),
        ("non-Selected Done after a ResultSet", |w| {
            protocol::write_response(
                w,
                &Response::ResultSet {
                    names: vec!["a".to_string()],
                    types: vec![DataType::Integer],
                },
            )
            .unwrap();
            protocol::write_response(
                w,
                &Response::Done {
                    kind: pinkdb_server::protocol::DoneKind::Inserted,
                    count: 1,
                    name: String::new(),
                },
            )
            .unwrap();
        }),
    ];

    for (label, emit) in cases {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let fake = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let _ = protocol::read_request(&mut reader).unwrap();
            protocol::write_response(
                &mut stream,
                &Response::HelloOk {
                    server_version: "fake".to_string(),
                    protocol_version: PROTOCOL_VERSION,
                },
            )
            .unwrap();
            let _ = protocol::read_request(&mut reader).unwrap();
            emit(&mut stream);
            // Drop closes the connection.
        });
        let mut c = Client::connect(addr.to_string()).unwrap();
        let first = c.execute("SELECT 1;").unwrap_err();
        assert!(!first.to_string().is_empty(), "{label}");
        // After a fatal decode/transport error the client must fail fast
        // instead of reading whatever bytes arrive next.
        let second = c.execute("SELECT 1;").unwrap_err();
        assert!(
            second.to_string().contains("broken"),
            "{label}: second call must report a broken client, got: {second}"
        );
        let _ = fake.join();
    }

    // A server-reported SQL error must NOT break the client: covered against
    // a real server by `sql_errors_are_reported_without_dropping_the_connection`.
}

#[test]
fn load_extension_is_refused_for_remote_clients_by_default() {
    let (_handle, addr) = start();
    let mut c = Client::connect(&addr).unwrap();
    let err = c
        .execute("LOAD EXTENSION '/definitely/not/a/lib.dylib';")
        .expect_err("remote LOAD EXTENSION must be refused");
    let msg = err.to_string();
    assert!(msg.contains("LOAD EXTENSION is disabled"), "{msg}");
    // The refusal happens before any dlopen attempt: an opted-in server
    // instead reports the loader's own error for the missing file.
    let server = Server::bind("127.0.0.1:0", Database::new())
        .unwrap()
        .allow_load_extension(true);
    let addr = server.local_addr().to_string();
    let _handle = server.spawn().unwrap();
    let mut c = Client::connect(&addr).unwrap();
    let msg = c
        .execute("LOAD EXTENSION '/definitely/not/a/lib.dylib';")
        .expect_err("missing library still errors")
        .to_string();
    assert!(!msg.contains("LOAD EXTENSION is disabled"), "{msg}");
}

#[test]
fn idle_clients_are_disconnected_and_announced_frames_are_not_preallocated() {
    let server = Server::bind("127.0.0.1:0", Database::new())
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

#[test]
fn checkpoint_round_trips_as_its_own_done_kind() {
    let (_handle, addr) = start();
    let mut c = Client::connect(&addr).unwrap();
    // In-memory: a no-op that still reports its own statement kind (v4).
    assert_eq!(c.execute("CHECKPOINT;").unwrap(), vec![QueryResult::Checkpointed]);
}
