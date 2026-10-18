//! The default (no-`tls`) server binary must refuse TLS flags with a clear
//! "built without TLS support" message instead of silently serving plaintext;
//! a half-configured pair is refused even before the feature check.
//!
//! With the `tls` feature enabled this file compiles to nothing (the TLS
//! binary tests live in `tls.rs`).

#![cfg(not(feature = "tls"))]

use std::process::{Command, Stdio};

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_pinkdb-server"))
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("spawn pinkdb-server")
}

#[test]
fn tls_flags_are_rejected_without_the_tls_feature() {
    let out = run(&["--tls-cert", "cert.pem", "--tls-key", "key.pem"]);
    assert_eq!(out.status.code(), Some(2));
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(text.contains("built without TLS support"), "{text}");
    assert!(text.contains("--features tls"), "{text}");
}

#[test]
fn half_configured_tls_is_refused_without_the_tls_feature() {
    let out = run(&["--tls-cert", "cert.pem"]);
    assert_eq!(out.status.code(), Some(2));
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(
        text.contains("--tls-cert and --tls-key must be given together"),
        "{text}"
    );
}
