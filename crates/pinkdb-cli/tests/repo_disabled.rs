//! The dot-commands and SQL statements that need the opt-in `repo` feature
//! must fail with a clear message in a default (feature-less) build.
//!
//! This file only compiles when the feature is OFF; the feature-on behavior
//! is covered by `tests/repo.rs`.

#![cfg(not(feature = "repo"))]

mod common;

use std::process::Command;

const WHY: &str = "this build has no extension repository support (rebuild with --features repo, \
                   or use the pinkdb-repo release variant)";

#[test]
fn repo_dot_commands_and_sql_report_the_missing_feature() {
    let input = ".install math_ext\n.repositories list\n.installed\nINSTALL math_ext;\n\
                 LOAD math_ext;\n.quit\n";
    let (text, status) = common::run_merged(Command::new(env!("CARGO_BIN_EXE_pinkdb")), input);
    common::assert_script_exit(status);
    // `.install` runs the engine's install path (one implementation), so it
    // reports the same reason as the SQL statements instead of a wrapper
    // message; the repository dot-commands name themselves.
    assert!(
        text.contains(&format!("Error: {WHY}\n")),
        "missing feature-less message for .install:\n{text}"
    );
    for cmd in [".repositories", ".installed"] {
        assert!(
            text.contains(&format!("Error: {cmd} is not available: {WHY}.")),
            "missing feature-less message for {cmd}:\n{text}"
        );
    }
    // The SQL statements report the engine's reason before the connection's
    // load gate (which is off by default), so the advice is about the build.
    assert_eq!(
        text.matches(WHY).count(),
        5,
        "expected the reason for both SQL statements too:\n{text}"
    );
    assert!(
        text.contains("Error: this build has no extension repository support"),
        "{text}"
    );
}
