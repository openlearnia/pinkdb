//! H-2: the async transport has no TLS path.
//!
//! `--transport async` combined with `--tls-cert`/`--tls-key` must be a usage
//! error (exit code 2) that never binds a listener — in builds with and
//! without the `tls` feature. Accepting the flags and serving plaintext is the
//! silent downgrade reproduced by the 2026-10-19 security review.
//!
//! Gated on `async-server`: without that feature `--transport async` itself is
//! already refused at parse time (covered by the existing binary tests).

#![cfg(feature = "async-server")]

use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{QueryResult, Value};
use pinkdb_server::Client;
use rcgen::{CertificateParams, KeyPair};

/// The exact refusal the binary prints for async + TLS flags (main prefixes
/// it with `Error: `).
const ASYNC_TLS_ERROR: &str = "--transport async does not support TLS; use the default sync \
                               transport with --tls-cert/--tls-key, or remove the TLS flags.";

/// A scratch directory for one test's PEM files; removed on drop.
struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new() -> Scratch {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinkdb-async-tls-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).expect("create scratch directory");
        Scratch { dir }
    }

    fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.dir.join(name);
        std::fs::write(&path, contents).expect("write scratch file");
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A certificate valid "now" (the same wide window the TLS tests use).
fn valid_pair() -> (String, String) {
    let key = KeyPair::generate().expect("generate key pair");
    let mut params = CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
        .expect("certificate params");
    params.not_before = rcgen::date_time_ymd(2020, 1, 1);
    params.not_after = rcgen::date_time_ymd(4096, 1, 1);
    let cert = params.self_signed(&key).expect("self-sign");
    (cert.pem(), key.serialize_pem())
}

/// Write a fresh valid pair and return its (cert, key) paths.
fn identity(scratch: &Scratch) -> (PathBuf, PathBuf) {
    let (cert_pem, key_pem) = valid_pair();
    (
        scratch.write("server.crt", &cert_pem),
        scratch.write("server.key", &key_pem),
    )
}

/// An ephemeral port that was just free. The refusal must happen before any
/// bind, so afterwards nothing may accept on it.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    listener.local_addr().expect("local addr").port()
}

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("spawn pinkdb-server")
}

/// Run the binary with the async transport plus `args` and assert it exits 2
/// with the H-2 message, prints no readiness line, and leaves the port closed.
fn assert_async_tls_refused(args: &[&str]) {
    let port = free_port();
    let port_arg = port.to_string();
    let mut full = vec!["--host", "127.0.0.1", "--port", port_arg.as_str()];
    full.extend_from_slice(args);

    let out = run(&full);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);

    assert_eq!(out.status.code(), Some(2), "args {args:?}: {stderr}");
    assert!(
        stderr.contains(ASYNC_TLS_ERROR),
        "args {args:?}: expected the async TLS refusal, got:\n{stderr}"
    );
    assert!(
        !stdout.contains("listening"),
        "args {args:?}: the server announced a listener:\n{stdout}"
    );
    assert!(
        TcpStream::connect(("127.0.0.1", port)).is_err(),
        "args {args:?}: something is listening on the refused port"
    );
}

#[test]
fn async_transport_refuses_a_valid_certificate_pair() {
    let scratch = Scratch::new();
    let (cert, key) = identity(&scratch);
    assert_async_tls_refused(&[
        "--transport",
        "async",
        "--tls-cert",
        cert.to_str().unwrap(),
        "--tls-key",
        key.to_str().unwrap(),
    ]);
}

#[test]
fn async_transport_refuses_each_tls_flag_alone() {
    let scratch = Scratch::new();
    let (cert, key) = identity(&scratch);
    assert_async_tls_refused(&["--transport", "async", "--tls-cert", cert.to_str().unwrap()]);
    assert_async_tls_refused(&["--transport", "async", "--tls-key", key.to_str().unwrap()]);
}

#[test]
fn async_transport_without_tls_starts_and_serves() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .args(["--host", "127.0.0.1", "--port", "0", "--transport", "async"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn pinkdb-server --transport async");

    let mut stdout = BufReader::new(child.stdout.take().expect("child stdout"));
    let mut line = String::new();
    stdout.read_line(&mut line).expect("readiness line");
    assert!(line.contains("listening on"), "{line:?}");
    assert!(line.contains("transport: async"), "{line:?}");
    assert!(
        !line.contains("TLS"),
        "the async transport is plaintext-only: {line:?}"
    );
    let addr = line
        .split_once("listening on ")
        .unwrap_or_else(|| panic!("readiness line missing 'listening on': {line:?}"))
        .1
        .split_whitespace()
        .next()
        .expect("address")
        .to_string();

    let mut c = Client::connect(&addr).expect("connect to the async transport");
    let results = c
        .execute("SELECT 1;")
        .expect("execute over the async transport");
    let rows = match results.into_iter().next().expect("one result") {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected Selected, got {other:?}"),
    };
    assert_eq!(rows, vec![vec![Value::Integer(1)]]);

    child.kill().expect("kill server");
    let _ = child.wait();
}
