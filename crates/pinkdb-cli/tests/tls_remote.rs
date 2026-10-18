//! Remote-mode TLS (features `client,tls`): the real CLI against an
//! in-process TLS server, with a self-signed certificate generated at test
//! time (rcgen) into a scratch directory outside the repository.
//!
//! The plaintext remote-mode tests live in `remote.rs`; the flag errors for a
//! `client`-only build live in `tls_flag_disabled.rs`.

#![cfg(all(feature = "client", feature = "tls"))]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::Database;
use pinkdb_server::{Server, ServerHandle};
use rcgen::{CertificateParams, KeyPair};

/// A scratch directory for one test's PEM files; removed on drop.
struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new() -> Scratch {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinkdb-cli-tls-test-{}-{}",
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

/// A self-signed certificate for `localhost` / `127.0.0.1` (+ the PEM key).
fn pair() -> (String, String) {
    let key = KeyPair::generate().expect("generate key pair");
    let params = CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
        .expect("certificate params");
    let cert = params.self_signed(&key).expect("self-sign");
    (cert.pem(), key.serialize_pem())
}

fn start_tls(cert: &Path, key: &Path) -> (ServerHandle, String) {
    let server = Server::bind_tls("127.0.0.1:0", Database::new(), cert, key).expect("bind TLS");
    let addr = server.local_addr().to_string();
    let handle = server.spawn().expect("spawn");
    (handle, addr)
}

/// The CLI with TLS flags, its stdout/stderr merged, fed `stdin`.
fn run_cli(args: &[&str], stdin: &str) -> (String, std::process::ExitStatus) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    command.args(args);
    common::run_merged(command, stdin)
}

fn output(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_pinkdb"))
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("spawn pinkdb CLI")
}

fn merged(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
fn remote_repl_over_tls_with_a_ca_certificate() {
    let scratch = Scratch::new();
    let (cert_pem, key_pem) = pair();
    let cert = scratch.write("server.crt", &cert_pem);
    let key = scratch.write("server.key", &key_pem);
    let (handle, addr) = start_tls(&cert, &key);

    let script = "CREATE TABLE t (a INTEGER, v VARCHAR);\n\
                  INSERT INTO t VALUES (1, 'x'), (2, NULL);\n\
                  SELECT a, v FROM t ORDER BY a;\n\
                  .quit\n";
    let (text, status) = run_cli(
        &[
            "--host",
            &addr,
            "--tls",
            "--tls-ca-cert",
            cert.to_str().unwrap(),
        ],
        script,
    );
    assert_eq!(status.code(), Some(0), "{text}");
    assert!(text.contains("Table created."), "{text}");
    assert!(text.contains("Inserted 2 row(s)."), "{text}");
    assert!(text.contains("│ a       │ v       │"), "{text}");
    assert!(text.contains("│ 1       │ x       │"), "{text}");
    assert!(text.contains("│ 2       │ NULL    │"), "{text}");
    // No certificate-verification warning in the verified mode.
    assert!(!text.contains("--tls-insecure"), "{text}");
    handle.join();
}

#[test]
fn remote_repl_over_tls_insecure_prints_a_warning() {
    let scratch = Scratch::new();
    let (cert_pem, key_pem) = pair();
    let cert = scratch.write("server.crt", &cert_pem);
    let key = scratch.write("server.key", &key_pem);
    let (handle, addr) = start_tls(&cert, &key);

    let (text, status) = run_cli(&["--host", &addr, "--tls-insecure"], "SELECT 1 + 1;\n");
    assert_eq!(status.code(), Some(0), "{text}");
    assert!(text.contains("│ 2       │"), "{text}");
    assert!(
        text.contains("warning: --tls-insecure skips server certificate verification"),
        "{text}"
    );
    handle.join();
}

#[test]
fn remote_tls_with_a_wrong_trust_anchor_fails_clearly() {
    let scratch = Scratch::new();
    let (cert_pem, key_pem) = pair();
    let (other_pem, _) = pair();
    let cert = scratch.write("server.crt", &cert_pem);
    let key = scratch.write("server.key", &key_pem);
    let other = scratch.write("other.crt", &other_pem);
    let (handle, addr) = start_tls(&cert, &key);

    let out = output(&[
        "--host",
        &addr,
        "--tls",
        "--tls-ca-cert",
        other.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    let text = merged(&out);
    assert!(text.contains("cannot connect"), "{text}");
    assert!(
        text.contains("BadSignature") || text.contains("UnknownIssuer"),
        "{text}"
    );

    // The server is unaffected for a correctly anchored CLI.
    let (text, status) = run_cli(
        &[
            "--host",
            &addr,
            "--tls",
            "--tls-ca-cert",
            cert.to_str().unwrap(),
        ],
        "SELECT 1;\n",
    );
    assert_eq!(status.code(), Some(0), "{text}");
    assert!(text.contains("│ 1       │"), "{text}");
    handle.join();
}

#[test]
fn remote_plaintext_against_a_tls_server_fails_clearly() {
    let scratch = Scratch::new();
    let (cert_pem, key_pem) = pair();
    let cert = scratch.write("server.crt", &cert_pem);
    let key = scratch.write("server.key", &key_pem);
    let (handle, addr) = start_tls(&cert, &key);

    let out = output(&["--host", &addr]);
    assert!(!out.status.success());
    let text = merged(&out);
    assert!(text.contains("cannot connect"), "{text}");
    handle.join();
}

#[test]
fn tls_flag_errors_are_precise() {
    let cases: &[(&[&str], &str)] = &[
        (
            &["--host", "127.0.0.1:1", "--tls"],
            "--tls needs a trust source",
        ),
        (
            &[
                "--host",
                "127.0.0.1:1",
                "--tls-ca-cert",
                "ca.pem",
                "--tls-insecure",
            ],
            "mutually exclusive",
        ),
        (&["--tls"], "require --host"),
        (&["--tls-ca-cert", "ca.pem"], "require --host"),
        (&["--tls-insecure"], "require --host"),
    ];
    for (args, needle) in cases {
        let out = output(args);
        assert_eq!(out.status.code(), Some(2), "{args:?}");
        let text = merged(&out);
        assert!(text.contains(needle), "{args:?}:\n{text}");
    }
}

#[test]
fn help_lists_the_tls_flags() {
    let out = output(&["--help"]);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("--tls"), "{text}");
    assert!(text.contains("--tls-ca-cert PATH"), "{text}");
    assert!(text.contains("--tls-insecure"), "{text}");
}
