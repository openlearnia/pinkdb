//! TLS flags in a `client`-only build (no `tls` feature): requesting TLS must
//! fail with a precise "rebuild with … --features client,tls" message, exit 2,
//! and never silently fall back to plaintext.
//!
//! With the `tls` feature enabled this file compiles to nothing (the TLS
//! remote-mode tests live in `tls_remote.rs`).

#![cfg(all(feature = "client", not(feature = "tls")))]

use std::process::{Command, Output, Stdio};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_pinkdb"))
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("spawn pinkdb")
}

#[test]
fn tls_flags_are_rejected_without_the_tls_feature() {
    for args in [
        [
            "--host",
            "127.0.0.1:1",
            "--tls",
            "--tls-ca-cert",
            "/nonexistent/ca.pem",
        ]
        .as_slice(),
        ["--host", "127.0.0.1:1", "--tls-insecure"].as_slice(),
    ] {
        let out = run(args);
        assert_eq!(out.status.code(), Some(2), "args {args:?}");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(text.contains("built without the tls feature"), "{text}");
        assert!(text.contains("--features client,tls"), "{text}");
    }
}
