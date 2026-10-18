//! VARCHAR result-ownership error paths (review minors).
//!
//! This file deliberately contains a single test: it reads the process-wide
//! result-text allocation counters exactly, and each integration-test file
//! runs as its own process, so no other test can perturb them.

mod common;

use pinkdb::{PinkDb, QueryResult, Value};
use pinkdb_ext_loader::{ExtValue, text_allocation_stats};

#[test]
fn ownership_violations_still_free_the_buffer_and_report_cleanly() {
    let path = common::extension_path("ownership_ext");
    let mut db = PinkDb::new();
    db.load_extension(&path).unwrap();
    let library = common::extension("ownership_ext");

    // Three callbacks allocate one buffer each; two allocate nothing. Every
    // allocated buffer must come back through `api->free` — including on the
    // error path and the NULL-result path.
    let before = text_allocation_stats();

    // 1. Error return with a buffer: the code is reported, the buffer freed.
    let e = db
        .execute("SELECT own_err_with_buf();")
        .unwrap_err()
        .to_string();
    assert!(e.contains("own_err_with_buf() failed with code 9"), "{e}");

    // 2. NULL result with a buffer: NULL is returned, the buffer freed.
    let result = db.execute("SELECT own_null_with_buf();").unwrap();
    match result.into_iter().next().unwrap() {
        QueryResult::Selected { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Null]]);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // 3. Invalid UTF-8: reported by the engine's value conversion, freed.
    let e = db
        .execute("SELECT own_bad_utf8();")
        .unwrap_err()
        .to_string();
    assert!(e.contains("returned invalid UTF-8"), "{e}");

    // 4. NULL pointer with a non-zero length: an ABI violation, never read.
    let e = db
        .execute("SELECT own_null_ptr_with_len();")
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("returned a NULL text pointer with text_len 5"),
        "{e}"
    );

    // 5. Zero length is a valid empty string, not a violation.
    let result = db.execute("SELECT own_zero_len();").unwrap();
    match result.into_iter().next().unwrap() {
        QueryResult::Selected { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Varchar(String::new())]]);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // The same decode paths through the direct ABI handle.
    let extension = pinkdb_ext_loader::load(&path).unwrap();
    let call = |name: &str| {
        extension
            .functions()
            .iter()
            .find(|f| f.name() == name)
            .unwrap_or_else(|| panic!("{name} registered"))
            .clone()
    };
    let e = call("own_err_with_buf").call(&[]).unwrap_err();
    assert!(e.to_string().contains("failed with code 9"), "{e}");
    assert_eq!(call("own_null_with_buf").call(&[]).unwrap(), ExtValue::Null);
    let e = call("own_null_ptr_with_len").call(&[]).unwrap_err();
    assert!(
        e.to_string().contains("NULL text pointer with text_len 5"),
        "{e}"
    );
    assert_eq!(
        call("own_zero_len").call(&[]).unwrap(),
        ExtValue::Varchar(Vec::new())
    );

    let after = text_allocation_stats();
    assert_eq!(
        after.allocations - before.allocations,
        5,
        "own_err_with_buf + own_null_with_buf + own_bad_utf8 via SQL, plus the two direct calls"
    );
    assert_eq!(
        after.frees - before.frees,
        5,
        "every extension buffer must be freed by the host, on every path"
    );
    assert_eq!(after.live(), 0, "no outstanding result text");

    // The process-wide counters are also visible through the extension's own
    // allocation count (one per allocating callback, SQL + direct).
    let allocs = common::symbol_i64(&library, "pinkdb_test_ownership_allocs");
    assert_eq!(allocs, 5, "one allocation per allocating call");
}
