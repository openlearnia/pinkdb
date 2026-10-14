//! The `.lake` dot-commands must fail with a clear message in a default
//! (feature-less) build.
//!
//! This file only compiles when the `lake` feature is OFF; the feature-on
//! behavior is covered by `tests/lake.rs`.

#![cfg(not(feature = "lake"))]

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn lake_dot_commands_report_the_missing_feature() {
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
        .write_all(b".help\n.lake save /tmp/x\n.lake open /tmp/x\n.lake versions /tmp/x\n.lake tables /tmp/x\n.lake info /tmp/x\n.quit\n")
        .expect("write to CLI stdin");
    let out = child.wait_with_output().expect("wait for the CLI");
    assert!(out.status.success(), "CLI exited with {}", out.status);
    let text = String::from_utf8(out.stdout).expect("CLI stdout is UTF-8");
    assert!(
        text.contains(
            "Error: .lake is not available: this pinkdb binary was built without the lake feature;"
        ),
        "missing feature-less message:\n{text}"
    );
    assert!(
        text.contains("rebuild with `cargo build -p pinkdb-cli --features lake`"),
        "{text}"
    );
    assert!(
        text.contains("(not built; needs the `lake` feature)"),
        "help must mention the missing feature:\n{text}"
    );
}
