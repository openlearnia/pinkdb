//! TLS transport tests (crate feature `tls`).
//!
//! Every certificate is generated *at test time* by rcgen (a test-only
//! dependency) into a scratch directory under the system temp dir, so no
//! fixture can expire, no private key is ever checked in, and no test
//! artifact lands in the repository tree.
//!
//! Coverage: round-trip over TLS (plus a multi-frame result and a prepared
//! statement), wrong trust anchor refused, expired / not-yet-valid
//! certificates refused, plaintext-vs-TLS mismatches in both directions,
//! handshake-failure isolation (including stalled handshakes under the idle
//! timeout), the connection cap under TLS, certificate/key startup refusals
//! (library and real binary), and the binary's flag pairing.

#![cfg(feature = "tls")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pinkdb::{Database, QueryResult, Value};
use pinkdb_server::{Client, ClientError, ClientTrust, Server, ServerHandle};

use rcgen::{CertificateParams, KeyPair};

/// A scratch directory for one test's PEM files; removed on drop.
struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new() -> Scratch {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinkdb-tls-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).expect("create scratch directory");
        Scratch { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.path(name);
        std::fs::write(&path, contents).expect("write scratch file");
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Generate a self-signed key pair + certificate for `localhost` and
/// `127.0.0.1` with the given validity window.
fn self_signed(not_before: (i32, u8, u8), not_after: (i32, u8, u8)) -> (String, String) {
    let key = KeyPair::generate().expect("generate key pair");
    let mut params = CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
        .expect("certificate params");
    params.not_before = rcgen::date_time_ymd(not_before.0, not_before.1, not_before.2);
    params.not_after = rcgen::date_time_ymd(not_after.0, not_after.1, not_after.2);
    let cert = params.self_signed(&key).expect("self-sign");
    (cert.pem(), key.serialize_pem())
}

/// A certificate valid "now" (a window far wider than any test run).
fn valid_pair() -> (String, String) {
    self_signed((2020, 1, 1), (4096, 1, 1))
}

/// Write one pair's PEMs into `scratch`.
fn identity(scratch: &Scratch, name: &str, pair: &(String, String)) -> (PathBuf, PathBuf) {
    (
        scratch.write(&format!("{name}.crt"), &pair.0),
        scratch.write(&format!("{name}.key"), &pair.1),
    )
}

fn start_tls(cert: &Path, key: &Path, max_connections: Option<usize>) -> (ServerHandle, String) {
    let bound = match max_connections {
        Some(max) => {
            Server::bind_tls_with_max_connections("127.0.0.1:0", Database::new(), max, cert, key)
        }
        None => Server::bind_tls("127.0.0.1:0", Database::new(), cert, key),
    };
    let server = bound.expect("bind TLS server");
    let addr = server.local_addr().to_string();
    let handle = server.spawn().expect("spawn TLS server");
    (handle, addr)
}

fn tls_client(addr: &str, anchor: &Path) -> Client {
    Client::connect_tls(addr, "127.0.0.1", ClientTrust::CaFile(anchor.to_path_buf()))
        .expect("connect over TLS")
}

fn rows(results: Vec<QueryResult>) -> Vec<Vec<Value>> {
    assert_eq!(results.len(), 1, "expected exactly one statement result");
    match results.into_iter().next().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected Selected, got {other:?}"),
    }
}

// ---------- round-trips ----------

#[test]
fn round_trip_over_tls() {
    let scratch = Scratch::new();
    let pair = valid_pair();
    let (cert, key) = identity(&scratch, "server", &pair);
    let (handle, addr) = start_tls(&cert, &key, None);

    let mut c = tls_client(&addr, &cert);
    assert_eq!(c.server_version(), env!("CARGO_PKG_VERSION"));
    c.ping().expect("ping over TLS");
    c.execute("CREATE TABLE t (a INTEGER, v VARCHAR);").unwrap();
    c.execute("INSERT INTO t VALUES (1, 'x'), (2, NULL);")
        .unwrap();
    assert_eq!(
        rows(c.execute("SELECT a, v FROM t ORDER BY a;").unwrap()),
        vec![
            vec![Value::Integer(1), Value::Varchar("x".to_string())],
            vec![Value::Integer(2), Value::Null],
        ]
    );

    // v5 prepared statements ride the same encrypted frames.
    let (select, params) = c.prepare("SELECT a FROM t WHERE v IS NULL").unwrap();
    assert_eq!(params, 0);
    assert_eq!(
        rows(c.execute_prepared(select, &[]).unwrap()),
        vec![vec![Value::Integer(2)]]
    );
    handle.join();
}

#[test]
fn multi_frame_results_stream_over_tls() {
    // More rows than `ROWS_PER_FRAME` (2048): the result arrives as several
    // encrypted `Rows` frames and must reassemble exactly.
    let scratch = Scratch::new();
    let pair = valid_pair();
    let (cert, key) = identity(&scratch, "server", &pair);
    let (handle, addr) = start_tls(&cert, &key, None);

    let mut c = tls_client(&addr, &cert);
    c.execute("CREATE TABLE big (a INTEGER, v VARCHAR);")
        .unwrap();
    let values = (0..3000)
        .map(|i| format!("({i}, 'row {i}')"))
        .collect::<Vec<_>>()
        .join(", ");
    c.execute(&format!("INSERT INTO big VALUES {values};"))
        .unwrap();

    let out = rows(c.execute("SELECT a, v FROM big;").unwrap());
    assert_eq!(out.len(), 3000);
    assert_eq!(
        out[0],
        vec![Value::Integer(0), Value::Varchar("row 0".to_string())]
    );
    assert_eq!(
        out[2999],
        vec![Value::Integer(2999), Value::Varchar("row 2999".to_string())]
    );
    assert_eq!(
        rows(c.execute("SELECT COUNT(*) FROM big;").unwrap()),
        vec![vec![Value::Bigint(3000)]]
    );
    handle.join();
}

// ---------- trust anchors and validity ----------

#[test]
fn wrong_trust_anchor_is_refused_and_the_server_keeps_serving() {
    let scratch = Scratch::new();
    let pair = valid_pair();
    let other = valid_pair();
    let (cert, key) = identity(&scratch, "server", &pair);
    let other_cert = scratch.write("other.crt", &other.0);
    let (handle, addr) = start_tls(&cert, &key, None);

    // A client trusting a different key refuses the server's certificate
    // (rustls reports the signature mismatch; a differently named CA would
    // report UnknownIssuer — either is a refusal).
    let err = Client::connect_tls(&addr, "127.0.0.1", ClientTrust::CaFile(other_cert))
        .expect_err("a client trusting another certificate must refuse this server");
    let msg = err.to_string();
    assert!(
        msg.contains("BadSignature") || msg.contains("UnknownIssuer"),
        "expected a certificate-chain refusal, got: {msg}"
    );

    // The same certificate under a name it does not cover is refused too.
    let err = Client::connect_tls(&addr, "example.com", ClientTrust::CaFile(cert.clone()))
        .expect_err("a certificate not covering example.com must be refused");
    assert!(matches!(err, ClientError::Protocol(_)), "{err}");

    // Neither refusal harms the server: the right anchor still runs SQL.
    let mut c = tls_client(&addr, &cert);
    assert_eq!(
        rows(c.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    handle.join();
}

#[test]
fn expired_and_not_yet_valid_certificates_are_refused() {
    let scratch = Scratch::new();

    // Expired: the server starts (a server does not validate its own
    // certificate) and the verifying client refuses at handshake time.
    let expired = self_signed((2020, 1, 1), (2021, 1, 1));
    let (cert, key) = identity(&scratch, "expired", &expired);
    let (handle, addr) = start_tls(&cert, &key, None);
    let err = Client::connect_tls(&addr, "127.0.0.1", ClientTrust::CaFile(cert.clone()))
        .expect_err("an expired certificate must be refused");
    assert!(err.to_string().contains("certificate expired"), "{err}");
    // The refusal is the client's validity check; the listener itself is
    // healthy, as an (explicitly unsafe) `--tls-insecure` peer shows.
    let mut c = Client::connect_tls(&addr, "127.0.0.1", ClientTrust::Insecure).unwrap();
    assert_eq!(
        rows(c.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    handle.join();

    // Not yet valid: same shape.
    let future = self_signed((2100, 1, 1), (2101, 1, 1));
    let (cert, key) = identity(&scratch, "future", &future);
    let (handle, addr) = start_tls(&cert, &key, None);
    let err = Client::connect_tls(&addr, "127.0.0.1", ClientTrust::CaFile(cert.clone()))
        .expect_err("a not-yet-valid certificate must be refused");
    assert!(
        err.to_string().contains("certificate not valid yet"),
        "{err}"
    );
    let mut c = Client::connect_tls(&addr, "127.0.0.1", ClientTrust::Insecure).unwrap();
    assert_eq!(
        rows(c.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    handle.join();
}

// ---------- transport mismatches ----------

#[test]
fn plaintext_client_against_a_tls_listener_fails_cleanly() {
    let scratch = Scratch::new();
    let pair = valid_pair();
    let (cert, key) = identity(&scratch, "server", &pair);
    let (handle, addr) = start_tls(&cert, &key, None);

    let t0 = Instant::now();
    let err = Client::connect(&addr).expect_err("plaintext against a TLS listener must fail");
    assert!(matches!(err, ClientError::Protocol(_)), "{err}");
    assert!(
        t0.elapsed() < Duration::from_secs(10),
        "the mismatch must fail promptly, not time out"
    );

    // The listener keeps serving TLS clients.
    let mut c = tls_client(&addr, &cert);
    assert_eq!(
        rows(c.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    handle.join();
}

#[test]
fn tls_client_against_a_plaintext_listener_fails_cleanly() {
    let scratch = Scratch::new();
    let pair = valid_pair();
    let (cert, _key) = identity(&scratch, "client", &pair);
    // A TLS ClientHello reads as a plausible (if bogus) frame length to the
    // plaintext server, which then waits for the announced body. The pinned
    // short idle timeout bounds that wait; with the default 300s it would
    // simply take longer to fail, not succeed.
    let server = Server::bind("127.0.0.1:0", Database::new())
        .unwrap()
        .idle_timeout(Some(Duration::from_millis(300)));
    let addr = server.local_addr().to_string();
    let handle = server.spawn().unwrap();

    let t0 = Instant::now();
    let err = Client::connect_tls(&addr, "127.0.0.1", ClientTrust::CaFile(cert))
        .expect_err("TLS against a plaintext listener must fail");
    assert!(matches!(err, ClientError::Protocol(_)), "{err}");
    assert!(
        t0.elapsed() < Duration::from_secs(10),
        "the mismatch must fail once the server's idle timeout expires"
    );

    // Plaintext clients are unaffected.
    let mut c = Client::connect(&addr).unwrap();
    assert_eq!(
        rows(c.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    handle.join();
}

// ---------- failure isolation ----------

#[test]
fn handshake_failures_are_isolated_and_stalled_handshakes_time_out() {
    let scratch = Scratch::new();
    let pair = valid_pair();
    let (cert, key) = identity(&scratch, "server", &pair);
    // A short idle timeout also bounds a stalled TLS handshake.
    let server = Server::bind_tls("127.0.0.1:0", Database::new(), &cert, &key)
        .unwrap()
        .idle_timeout(Some(Duration::from_millis(300)));
    let addr = server.local_addr().to_string();
    let handle = server.spawn().unwrap();

    for i in 0..16 {
        let mut s = TcpStream::connect(&addr).expect("junk connect");
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        match i % 4 {
            // A plaintext HTTP probe.
            0 => s.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").unwrap(),
            // A truncated TLS record: the handshake stalls and the idle
            // timeout must drop it.
            1 => s.write_all(&[0x16, 0x03, 0x01, 0xFF, 0x01]).unwrap(),
            // A well-formed *plaintext* pinkdb Hello: a TLS listener must not
            // parse it as protocol frames.
            2 => s
                .write_all(&[0x09, 0x00, 0x00, 0x00, 0x01, 0x08, 0x00, 0x00, 0x00])
                .unwrap(),
            // Connect and vanish.
            _ => {
                drop(s);
                continue;
            }
        }
        // The server drops the failed handshake (immediately for 0/2, after
        // the idle timeout for 1). A TLS alert (a handful of bytes, record
        // type 0x15) may precede the close; application frames must not.
        let mut buf = [0u8; 64];
        let mut total = 0usize;
        loop {
            match s.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    total += n;
                    assert!(
                        total <= 1024,
                        "junk connection {i} was served {total} bytes instead of being dropped"
                    );
                }
                Err(e) => panic!("junk connection {i} read failed: {e}"),
            }
        }
    }

    // The server is fully healthy afterwards.
    let mut c = tls_client(&addr, &cert);
    c.ping().unwrap();
    assert_eq!(
        rows(c.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    handle.join();
}

// ---------- caps and timeouts over TLS ----------

#[test]
fn tls_respects_the_connection_cap() {
    let scratch = Scratch::new();
    let pair = valid_pair();
    let (cert, key) = identity(&scratch, "server", &pair);
    let (handle, addr) = start_tls(&cert, &key, Some(2));

    let mut a = tls_client(&addr, &cert);
    let mut b = tls_client(&addr, &cert);
    a.execute("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (1);")
        .unwrap();

    // Over the cap a TLS listener drops the socket (there is no plaintext
    // Error frame to send before a handshake, and completing one just to say
    // "busy" would spend the capacity being defended).
    let mut third = TcpStream::connect(&addr).expect("connect");
    third
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut buf = [0u8; 1];
    if let Ok(n) = third.read(&mut buf) {
        assert_eq!(
            n, 0,
            "an over-cap TLS connection must be dropped, not served"
        );
    }

    // Existing clients are unaffected.
    assert_eq!(
        rows(a.execute("SELECT v FROM t;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    assert_eq!(
        rows(b.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );

    // Freeing a slot admits a new TLS client.
    drop(b);
    let mut admitted = None;
    for _ in 0..100 {
        match Client::connect_tls(&addr, "127.0.0.1", ClientTrust::CaFile(cert.clone())) {
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

#[test]
fn idle_tls_clients_are_disconnected() {
    let scratch = Scratch::new();
    let pair = valid_pair();
    let (cert, key) = identity(&scratch, "server", &pair);
    let server = Server::bind_tls("127.0.0.1:0", Database::new(), &cert, &key)
        .unwrap()
        .idle_timeout(Some(Duration::from_millis(300)));
    let addr = server.local_addr().to_string();
    let handle = server.spawn().unwrap();

    let mut idle = tls_client(&addr, &cert);
    std::thread::sleep(Duration::from_millis(900));
    let err = idle
        .ping()
        .expect_err("an idle TLS client must be disconnected");
    assert!(matches!(err, ClientError::Protocol(_)), "{err}");

    // A fresh client still works.
    let mut fresh = tls_client(&addr, &cert);
    assert_eq!(
        rows(fresh.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    handle.join();
}

// ---------- startup refusals (library) ----------

/// The error from a `bind_tls` that must refuse startup (`Server` is not
/// `Debug`, so `unwrap_err` is not available).
fn bind_err(cert: &Path, key: &Path) -> std::io::Error {
    match Server::bind_tls("127.0.0.1:0", Database::new(), cert, key) {
        Ok(_) => panic!("binding must fail for {cert:?} / {key:?}"),
        Err(e) => e,
    }
}

#[test]
fn bad_certificate_and_key_files_refuse_startup_with_precise_messages() {
    let scratch = Scratch::new();
    let pair = valid_pair();
    let (cert, key) = identity(&scratch, "server", &pair);
    let other = valid_pair();
    let (other_cert, other_key) = identity(&scratch, "other", &other);

    let err = bind_err(&scratch.path("absent.crt"), &key);
    assert!(
        err.to_string().contains("cannot read TLS certificate file"),
        "{err}"
    );
    assert!(err.to_string().contains("absent.crt"), "{err}");

    let err = bind_err(&cert, &scratch.path("absent.key"));
    assert!(
        err.to_string().contains("cannot read TLS private key file"),
        "{err}"
    );

    let empty_cert = scratch.write("empty.crt", "");
    let err = bind_err(&empty_cert, &key);
    assert!(
        err.to_string().contains("no PEM certificates found"),
        "{err}"
    );
    assert!(err.to_string().contains("empty.crt"), "{err}");

    let empty_key = scratch.write("empty.key", "");
    let err = bind_err(&cert, &empty_key);
    assert!(
        err.to_string().contains("no PEM private key found"),
        "{err}"
    );

    // A certificate where a key is expected is not a key.
    let err = bind_err(&cert, &cert);
    assert!(
        err.to_string().contains("no PEM private key found"),
        "{err}"
    );

    // Non-PEM bytes are refused, never half-accepted.
    let garbage = scratch.write("garbage.crt", "this is not a PEM file\n");
    let err = bind_err(&garbage, &key);
    assert!(
        err.to_string().contains("no PEM certificates found"),
        "{err}"
    );

    // A key that does not belong to the certificate is refused at startup
    // (both directions), not at the first handshake.
    let err = bind_err(&cert, &other_key);
    assert!(err.to_string().contains("not a usable pair"), "{err}");
    let err = bind_err(&other_cert, &key);
    assert!(err.to_string().contains("not a usable pair"), "{err}");

    // None of those refusals bound a listener: the valid pair still starts.
    let (handle, addr) = start_tls(&cert, &key, None);
    let mut c = tls_client(&addr, &cert);
    assert_eq!(
        rows(c.execute("SELECT 1;").unwrap()),
        vec![vec![Value::Integer(1)]]
    );
    handle.join();
}

// ---------- the real binary ----------

/// Spawn the real server binary; the caller reads its readiness line.
fn spawn_server(args: &[&str]) -> (Child, BufReader<ChildStdout>) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"));
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = cmd.spawn().expect("spawn pinkdb-server");
    let stdout = BufReader::new(child.stdout.take().expect("child stdout"));
    (child, stdout)
}

#[test]
fn binary_refuses_a_half_configured_tls_pair() {
    for args in [
        ["--tls-cert", "/nonexistent/cert.pem"].as_slice(),
        ["--tls-key", "/nonexistent/key.pem"].as_slice(),
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
            .args(args)
            .stdin(Stdio::null())
            .output()
            .expect("spawn pinkdb-server");
        assert_eq!(out.status.code(), Some(2), "args {args:?}");
        let text = String::from_utf8_lossy(&out.stderr);
        assert!(
            text.contains("--tls-cert and --tls-key must be given together"),
            "{text}"
        );
    }
}

#[test]
fn binary_refuses_a_bad_certificate_before_listening() {
    let scratch = Scratch::new();
    let pair = valid_pair();
    let (_cert, key) = identity(&scratch, "server", &pair);
    let garbage = scratch.write("garbage.crt", "not a certificate\n");
    let out = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .args(["--host", "127.0.0.1", "--port", "0"])
        .arg("--tls-cert")
        .arg(&garbage)
        .arg("--tls-key")
        .arg(&key)
        .stdin(Stdio::null())
        .output()
        .expect("spawn pinkdb-server");
    assert!(!out.status.success());
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(text.contains("no PEM certificates found"), "{text}");
    assert!(text.contains("garbage.crt"), "{text}");
    assert!(
        !String::from_utf8_lossy(&out.stdout).contains("listening"),
        "the server must not have bound a listener"
    );
}

#[test]
fn binary_serves_tls_end_to_end() {
    let scratch = Scratch::new();
    let pair = valid_pair();
    let (cert, key) = identity(&scratch, "server", &pair);
    let (mut child, mut stdout) = spawn_server(&[
        "--host",
        "127.0.0.1",
        "--port",
        "0",
        "--tls-cert",
        cert.to_str().unwrap(),
        "--tls-key",
        key.to_str().unwrap(),
    ]);
    let mut line = String::new();
    stdout.read_line(&mut line).expect("readiness line");
    assert!(line.contains("transport: TLS"), "{line:?}");
    let addr = line
        .split_once("listening on ")
        .unwrap_or_else(|| panic!("readiness line missing 'listening on': {line:?}"))
        .1
        .split_whitespace()
        .next()
        .expect("address")
        .to_string();

    let mut c = tls_client(&addr, &cert);
    c.execute("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (7);")
        .unwrap();
    assert_eq!(
        rows(c.execute("SELECT v FROM t;").unwrap()),
        vec![vec![Value::Integer(7)]]
    );

    child.kill().expect("kill server");
    let _ = child.wait();
}
