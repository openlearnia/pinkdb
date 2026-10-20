//! The operator-facing text must match the enforced policy: authentication is
//! per store (trusted only while the catalog is empty), not "no
//! authentication". These tests drive the real binary.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

#[test]
fn help_text_describes_the_real_authentication_policy() {
    let out = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .arg("--help")
        .output()
        .expect("run pinkdb-server --help");
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        !text.contains("no authentication"),
        "help still claims there is no authentication:\n{text}"
    );
    assert!(
        text.contains("authentication is opt-in per store"),
        "help does not state the per-store policy:\n{text}"
    );
    assert!(
        text.contains("every statement request requires Login"),
        "help does not state the login requirement:\n{text}"
    );
    assert!(
        text.contains("TLS only encrypts"),
        "help no longer explains what TLS does:\n{text}"
    );
}

#[test]
fn help_text_says_tls_is_sync_only() {
    let out = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .arg("--help")
        .output()
        .expect("run pinkdb-server --help");
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    // The usage text wraps; compare with normalized whitespace.
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        flat.contains("the async transport has no TLS"),
        "help does not state the async transport's TLS limitation:\n{text}"
    );
    assert!(
        flat.contains("--tls-cert/--tls-key are refused at startup"),
        "help does not say the async transport refuses the TLS flags:\n{text}"
    );
}

#[test]
fn startup_banner_does_not_claim_there_is_no_authentication() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .args(["--host", "127.0.0.1", "--port", "0"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pinkdb-server");

    // The readiness line is printed first, then the static auth note.
    let mut stdout = BufReader::new(child.stdout.take().expect("stdout"));
    let mut ready = String::new();
    stdout.read_line(&mut ready).expect("readiness line");
    assert!(ready.contains("listening on"), "{ready}");

    let mut stderr = BufReader::new(child.stderr.take().expect("stderr"));
    let mut banner = String::new();
    stderr.read_line(&mut banner).expect("startup banner");
    let _ = child.kill();
    let _ = child.wait();

    assert!(
        !banner.contains("no authentication"),
        "startup banner still claims there is no authentication: {banner:?}"
    );
    assert!(
        banner.contains("authentication"),
        "startup banner does not mention authentication: {banner:?}"
    );
    assert!(
        banner.contains("once the store has users"),
        "startup banner does not state when authentication applies: {banner:?}"
    );
}
