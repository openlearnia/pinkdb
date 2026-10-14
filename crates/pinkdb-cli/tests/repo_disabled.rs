//! The dot-commands that need the opt-in `repo` feature must fail with a
//! clear message in a default (feature-less) build.
//!
//! This file only compiles when the feature is OFF; the feature-on behavior
//! is covered by `tests/repo.rs`.

#![cfg(not(feature = "repo"))]

mod common;

use std::process::Command;

#[test]
fn repo_dot_commands_report_the_missing_feature() {
    let input =
        String::from_utf8_lossy(b".install math_ext\n.repositories list\n.installed\n.quit\n")
            .into_owned();
    let (text, status) = common::run_merged(Command::new(env!("CARGO_BIN_EXE_pinkdb")), &input);
    common::assert_script_exit(status);
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
