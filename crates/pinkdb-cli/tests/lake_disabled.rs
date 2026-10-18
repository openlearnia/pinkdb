//! The `.lake` dot-commands must fail with a clear message in a default
//! (feature-less) build, and in an `s3`-only build (`s3` is pure storage:
//! it implies neither lake build).
//!
//! This file only compiles when neither lake feature is on; the feature-on
//! behavior is covered by `tests/lake.rs` (vortexlake) and `tests/pinklake.rs`
//! (pinklake).

#![cfg(not(any(feature = "pinklake", feature = "vortexlake")))]

mod common;

use std::process::Command;

#[test]
fn lake_dot_commands_report_the_missing_feature() {
    let input = String::from_utf8_lossy(b".help\n.lake save /tmp/x\n.lake open /tmp/x\n.lake versions /tmp/x\n.lake tables /tmp/x\n.lake info /tmp/x\n.quit\n").into_owned();
    let (text, status) = common::run_merged(Command::new(env!("CARGO_BIN_EXE_pinkdb")), &input);
    common::assert_script_exit(status);
    assert!(
        text.contains(
            "Error: .lake is not available: this pinkdb binary was built without a lake feature;"
        ),
        "missing feature-less message:\n{text}"
    );
    assert!(
        text.contains("rebuild with `cargo build -p pinkdb-cli --features vortexlake`"),
        "{text}"
    );
    assert!(
        text.contains("`--features pinklake` (lean: `.pink` data files)"),
        "the pinklake alternative must be named too:\n{text}"
    );
    assert!(
        text.contains("combine either with `,s3` (`vortexlake,s3` / `pinklake,s3`) for s3://"),
        "the s3-combination rule must be named too:\n{text}"
    );
    assert!(
        text.contains("(not built; needs the `pinklake` or `vortexlake` feature)"),
        "help must mention the missing feature:\n{text}"
    );
}

/// `s3` is a storage mechanism, not a lake build: with `s3` on and no lake
/// feature, `.lake` still reports the missing *lake* feature (the storage
/// client alone provides no commands), and the help agrees.
#[cfg(feature = "s3")]
#[test]
fn s3_alone_does_not_provide_lake_commands() {
    let input =
        String::from_utf8_lossy(b".help\n.lake save s3://bucket/prefix\n.quit\n").into_owned();
    let (text, status) = common::run_merged(Command::new(env!("CARGO_BIN_EXE_pinkdb")), &input);
    common::assert_script_exit(status);
    assert!(
        text.contains("this pinkdb binary was built without a lake feature;"),
        "an s3-only build must not hide the missing lake feature:\n{text}"
    );
    assert!(
        text.contains("(not built; needs the `pinklake` or `vortexlake` feature)"),
        "help must mention the missing feature in an s3-only build:\n{text}"
    );
}
