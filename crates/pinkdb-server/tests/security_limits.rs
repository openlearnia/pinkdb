//! Security remediation pass 5 over the wire: login backoff/close, the
//! global concurrent-verification cap, per-statement deadlines/result caps,
//! and accept-loop refusal handling (H-6 / M-6 / M-11).

use std::io::BufReader;
use std::net::TcpStream;
use std::time::Duration;

use pinkdb::{Database, QueryResult, Value};
use pinkdb_server::protocol::{PROTOCOL_VERSION, Request, Response, read_response, write_request};
use pinkdb_server::{Client, Server, ServerHandle};

#[cfg(feature = "async-server")]
use pinkdb_server::{AsyncServer, AsyncServerHandle};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Flavor {
    Blocking,
    #[cfg(feature = "async-server")]
    Async,
}

fn flavors() -> Vec<Flavor> {
    #[cfg(feature = "async-server")]
    {
        vec![Flavor::Blocking, Flavor::Async]
    }
    #[cfg(not(feature = "async-server"))]
    {
        vec![Flavor::Blocking]
    }
}

enum Handle {
    #[allow(dead_code)]
    Blocking(ServerHandle),
    #[cfg(feature = "async-server")]
    #[allow(dead_code)]
    Async(AsyncServerHandle),
}

fn start(flavor: Flavor, db: Database) -> (Handle, String) {
    match flavor {
        Flavor::Blocking => {
            let server = Server::bind("127.0.0.1:0", db).expect("bind");
            let addr = server.local_addr().to_string();
            (Handle::Blocking(server.spawn().expect("spawn")), addr)
        }
        #[cfg(feature = "async-server")]
        Flavor::Async => {
            let server = AsyncServer::bind("127.0.0.1:0", db).expect("bind async");
            let addr = server.local_addr().to_string();
            (Handle::Async(server.spawn().expect("spawn async")), addr)
        }
    }
}

fn rows(results: Vec<QueryResult>) -> Vec<Vec<Value>> {
    assert_eq!(results.len(), 1);
    match results.into_iter().next().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected Selected, got {other:?}"),
    }
}

fn phc(m: u32, t: u32, p: u32) -> String {
    format!(
        "$argon2id$v=19$m={m},t={t},p={p}$AAAAAAAAAAAAAAAAAAAAAA$\
         AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    )
}

fn ints(n: usize) -> String {
    (0..n)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",")
}

// ---------- H-6: per-connection failed-login limit ----------

#[test]
fn failed_logins_close_the_connection_after_five_attempts() {
    for flavor in flavors() {
        let db = Database::new();
        db.connect()
            .execute("CREATE USER alice PASSWORD 'right';")
            .unwrap();
        let (_handle, addr) = start(flavor, db);
        let mut client = Client::connect(&addr).unwrap();
        for _ in 0..5 {
            let e = client.login("alice", "wrong").unwrap_err().to_string();
            assert!(e.contains("authentication failed"), "{flavor:?}: {e}");
        }
        // The fifth failure closed the connection: the next request sees EOF
        // (the client marks itself unusable rather than hanging).
        assert!(
            client.execute("SELECT 1;").is_err(),
            "{flavor:?}: a connection must close after five failed logins"
        );
    }
}

// ---------- H-6: global concurrent-verification cap ----------

#[test]
fn concurrent_login_cap_refuses_excess_verifications() {
    // A ceiling-work hash keeps the first verification in flight long enough
    // to observe the second request (its 32-byte hash is zeros, so no
    // password authenticates).
    let hash = phc(77824, 4, 1);
    for flavor in flavors() {
        let db = Database::new();
        db.connect()
            .execute(&format!("CREATE USER slow PASSWORD_HASH '{hash}';"))
            .unwrap();
        let (_handle, addr) = match flavor {
            Flavor::Blocking => {
                let server = Server::bind("127.0.0.1:0", db)
                    .unwrap()
                    .max_concurrent_logins(1);
                let addr = server.local_addr().to_string();
                (Handle::Blocking(server.spawn().unwrap()), addr)
            }
            #[cfg(feature = "async-server")]
            Flavor::Async => {
                let server = AsyncServer::bind("127.0.0.1:0", db)
                    .unwrap()
                    .max_concurrent_logins(1);
                let addr = server.local_addr().to_string();
                (Handle::Async(server.spawn().unwrap()), addr)
            }
        };

        let mut saw_cap = false;
        // At most three attempts (six failures) stays well below the per-IP
        // block threshold of 20.
        for _ in 0..3 {
            let mut first = Client::connect(&addr).unwrap();
            let addr2 = addr.clone();
            let login = std::thread::spawn(move || {
                let _ = first.login("slow", "wrong");
            });
            std::thread::sleep(Duration::from_millis(10));
            let mut second = Client::connect(&addr2).unwrap();
            let e = second.login("slow", "wrong").unwrap_err().to_string();
            let _ = login.join();
            if e.contains("too many concurrent logins") {
                saw_cap = true;
                break;
            }
            assert!(e.contains("authentication failed"), "{flavor:?}: {e}");
        }
        assert!(
            saw_cap,
            "{flavor:?}: the second login should have been refused by the one-slot cap"
        );
    }
}

// ---------- M-6: server defaults / flags over the wire ----------

#[test]
fn statement_deadline_is_enforced_and_the_connection_stays_usable() {
    let db = Database::new();
    db.connect()
        .execute(&format!(
            "CREATE TABLE t (i INTEGER); INSERT INTO t VALUES {};",
            ints(50)
        ))
        .unwrap();
    for flavor in flavors() {
        let (_handle, addr) = match flavor {
            Flavor::Blocking => {
                let server = Server::bind("127.0.0.1:0", db.clone())
                    .unwrap()
                    .statement_timeout(Duration::from_millis(5));
                let addr = server.local_addr().to_string();
                (Handle::Blocking(server.spawn().unwrap()), addr)
            }
            #[cfg(feature = "async-server")]
            Flavor::Async => {
                let server = AsyncServer::bind("127.0.0.1:0", db.clone())
                    .unwrap()
                    .statement_timeout(Duration::from_millis(5));
                let addr = server.local_addr().to_string();
                (Handle::Async(server.spawn().unwrap()), addr)
            }
        };
        let mut client = Client::connect(&addr).unwrap();
        let e = client
            .execute("SELECT * FROM t a CROSS JOIN t b CROSS JOIN t c CROSS JOIN t d;")
            .expect_err("a 6.25M-row join must hit the 5 ms deadline")
            .to_string();
        assert!(e.contains("statement timeout"), "{flavor:?}: {e}");
        // The connection is still usable after a statement timeout.
        assert_eq!(
            rows(client.execute("SELECT 1;").unwrap()),
            vec![vec![Value::Integer(1)]]
        );
    }
}

#[test]
fn result_caps_are_enforced_over_the_wire() {
    let db = Database::new();
    {
        let mut conn = db.connect();
        conn.execute(&format!(
            "CREATE TABLE t (i INTEGER); INSERT INTO t VALUES {};",
            ints(20)
        ))
        .unwrap();
        conn.execute("CREATE TABLE ten (i INTEGER); INSERT INTO ten VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);")
            .unwrap();
    }
    for flavor in flavors() {
        let (_handle, addr) = match flavor {
            Flavor::Blocking => {
                let server = Server::bind("127.0.0.1:0", db.clone())
                    .unwrap()
                    .max_result_rows(100);
                let addr = server.local_addr().to_string();
                (Handle::Blocking(server.spawn().unwrap()), addr)
            }
            #[cfg(feature = "async-server")]
            Flavor::Async => {
                let server = AsyncServer::bind("127.0.0.1:0", db.clone())
                    .unwrap()
                    .max_result_rows(100);
                let addr = server.local_addr().to_string();
                (Handle::Async(server.spawn().unwrap()), addr)
            }
        };
        let mut client = Client::connect(&addr).unwrap();
        let e = client
            .execute("SELECT * FROM t a CROSS JOIN t b;")
            .expect_err("400 rows exceed the 100-row cap")
            .to_string();
        assert!(e.contains("result limit exceeded"), "{flavor:?}: {e}");
        // Exactly at the cap is admitted.
        let out = rows(
            client
                .execute("SELECT * FROM ten a CROSS JOIN ten b;")
                .unwrap(),
        );
        assert_eq!(out.len(), 100, "{flavor:?}");
    }
}

// ---------- M-11: refusals never spawn per-connection threads ----------

#[test]
fn non_reading_refused_clients_do_not_stall_the_accept_loop() {
    let server = Server::bind_with_max_connections("127.0.0.1:0", Database::new(), 1).unwrap();
    let addr = server.local_addr().to_string();
    let handle = server.spawn().unwrap();

    // Hold the only slot.
    let held = Client::connect(&addr).unwrap();

    // Refused clients that never read their Error frame. The blocking
    // transport writes each refusal on the accept loop with a bounded write
    // timeout (M-11), so these must not create a thread each or stall the
    // loop.
    let mut refused = Vec::new();
    for _ in 0..25 {
        refused.push(TcpStream::connect(&addr).expect("refused socket connects"));
    }

    // The protocol error is still delivered to a client that does read it.
    let probe = TcpStream::connect(&addr).expect("probe connects");
    let mut probe_writer = probe.try_clone().unwrap();
    write_request(
        &mut probe_writer,
        &Request::Hello {
            protocol_version: PROTOCOL_VERSION,
        },
    )
    .unwrap();
    match read_response(&mut BufReader::new(probe)).expect("read refusal") {
        Response::Error { message } => assert!(message.contains("connection limit"), "{message}"),
        other => panic!("expected a refusal Error, got {other:?}"),
    }

    // Free the slot and prove the accept loop is still responsive.
    drop(held);
    let mut admitted = None;
    for _ in 0..100 {
        match Client::connect(&addr) {
            Ok(client) => {
                admitted = Some(client);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
    let mut client = admitted.expect("the accept loop must admit after a slot frees");
    assert_eq!(
        rows(client.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    drop(refused);
    handle.join();
}
