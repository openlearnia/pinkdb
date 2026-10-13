//! The dot-commands that need the opt-in `repo` feature must fail with a
//! clear message in a default (feature-less) build.
//!
//! This file only compiles when the feature is OFF; the feature-on behavior
//! is covered by `tests/repo.rs`.

#![cfg(not(feature = "repo"))]

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn repo_dot_commands_report_the_missing_feature() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pinkdb"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn pinkdb CLI");
    child
        .stdin
        .take()
        .expect("child stdin")
        .write_all(b".install math_ext\n.repositories list\n.installed\n.quit\n")
        .expect("write to CLI stdin");
    let out = child.wait_with_output().expect("wait for the CLI");
    assert!(out.status.success(), "CLI exited with {}", out.status);
    let text = String::from_utf8(out.stdout).expect("CLI stdout is UTF-8");
    for cmd in [".install", ".repositories", ".installed"] {
        assert!(
            text.contains(&format!(
                "Error: {cmd} is not available: this pinkdb binary was built without the repo feature;"
            )),
            "missing feature-less message for {cmd}:\n{text}"
        );
    }
    assert!(
        text.contains("rebuild with `cargo build -p pinkdb-cli --features repo`"),
        "{text}"
    );
}
