//! The `httpfs` surfaces must fail with a clear message in a build without
//! the feature: `read_csv` is an unknown table function and a URL import
//! names the missing fetcher instead of treating the URL as a local path.
//! The feature-on behavior lives in `tests/httpfs.rs`.

#![cfg(not(feature = "httpfs"))]

mod common;

use std::process::Command;

#[test]
fn httpfs_surfaces_report_the_missing_feature() {
    let script = "\
SELECT * FROM read_csv('x.csv');
.import https://example.invalid/x.csv
SELECT * FROM 'https://example.invalid/x.csv';
.quit
";
    let command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    let (text, status) = common::run_merged(command, script);
    common::assert_script_exit(status);
    assert!(
        text.contains("unknown table function \"read_csv\""),
        "read_csv must be an unknown function in a feature-less build:\n{text}"
    );
    assert!(
        text.contains("no URL fetcher is registered"),
        "a URL import must name the missing transport:\n{text}"
    );
    assert!(
        text.contains("cannot read \"https://example.invalid/x.csv\" by path"),
        "the URL replacement scan must not be claimed:\n{text}"
    );
}
