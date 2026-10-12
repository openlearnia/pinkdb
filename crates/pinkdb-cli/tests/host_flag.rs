//! `--host` without the `client` feature must be rejected with a clear
//! message; with the feature enabled this file compiles to nothing (the
//! remote-mode tests live in `remote.rs`).

#![cfg(not(feature = "client"))]

use std::process::{Command, Output, Stdio};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_pinkdb"))
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("spawn pinkdb")
}

#[test]
fn host_flag_without_client_feature_is_rejected() {
    let out = run(&["--host", "127.0.0.1:1"]);
    assert!(!out.status.success(), "--host must fail without the client feature");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        text.contains("built without the client feature"),
        "output was:\n{text}"
    );
}

#[test]
fn unknown_argument_is_rejected_with_usage() {
    let out = run(&["--bogus"]);
    assert_eq!(out.status.code(), Some(2));
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(text.contains("unknown argument"), "{text}");
    assert!(text.contains("Usage: pinkdb"), "{text}");
}

#[test]
fn help_lists_the_host_flag() {
    let out = run(&["--help"]);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("--host HOST:PORT"), "{text}");
}
