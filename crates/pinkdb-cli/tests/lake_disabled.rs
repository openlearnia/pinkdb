//! The `.lake` dot-commands must fail with a clear message in a default
//! (feature-less) build.
//!
//! This file only compiles when the `lake` feature is OFF; the feature-on
//! behavior is covered by `tests/lake.rs`.

#![cfg(not(feature = "lake"))]

mod common;

use std::process::Command;

#[test]
fn lake_dot_commands_report_the_missing_feature() {
    let input = String::from_utf8_lossy(b".help\n.lake save /tmp/x\n.lake open /tmp/x\n.lake versions /tmp/x\n.lake tables /tmp/x\n.lake info /tmp/x\n.quit\n").into_owned();
    let (text, status) = common::run_merged(Command::new(env!("CARGO_BIN_EXE_pinkdb")), &input);
    common::assert_script_exit(status);
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
