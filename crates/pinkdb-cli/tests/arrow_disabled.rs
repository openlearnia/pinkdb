//! The arrow file surfaces must fail with a clear message in a default
//! (feature-less) build: the `arrow` feature is opt-in, so this file only
//! compiles without it. The feature-on behavior lives in `tests/arrow.rs`.

#![cfg(not(feature = "arrow"))]

mod common;

use std::process::Command;

#[test]
fn arrow_surfaces_report_the_missing_feature() {
    let script = "\
.help
CREATE TABLE t (a INTEGER);
SELECT * FROM 'x.arrow';
SELECT * FROM read_arrow('x.arrow');
COPY t TO 'x.arrow' (FORMAT arrow);
.quit
";
    let (text, status) = common::run_merged(Command::new(env!("CARGO_BIN_EXE_pinkdb")), script);
    common::assert_script_exit(status);
    assert!(
        text.contains("Error: no file format is registered for \"x.arrow\""),
        "the replacement scan must name the missing format:\n{text}"
    );
    assert!(
        text.contains("unknown table function \"read_arrow\""),
        "the table-function spelling must be refused too:\n{text}"
    );
    assert!(
        text.contains("Error: no file format \"arrow\" is registered"),
        "COPY must name the missing format:\n{text}"
    );
    assert!(
        text.contains("ARROW: the arrow file format is not built"),
        "help must mention the missing feature:\n{text}"
    );
}
