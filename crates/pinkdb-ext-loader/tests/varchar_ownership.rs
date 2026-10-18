//! VARCHAR result ownership: extension-allocated text is copied out and freed
//! by the host, with nothing leaked.
//!
//! This file deliberately contains a single test: it reads the process-wide
//! result-text allocation counters, and each integration-test file runs as
//! its own process, so no other test can perturb them.

mod common;

use pinkdb::{PinkDb, QueryResult, Value};
use pinkdb_ext_loader::{ExtArg, ExtValue, text_allocation_stats};

#[test]
fn varchar_results_are_copied_and_freed_without_leaks() {
    let path = common::extension_path("test_ext");
    let mut db = PinkDb::new();
    db.load_extension(&path).unwrap();

    let sql_iterations = 3_000u64;
    let direct_iterations = 5_000u64;
    let before = text_allocation_stats();

    // Through SQL: every row allocates one buffer per VARCHAR result.
    for _ in 0..sql_iterations {
        let result = db
            .execute("SELECT ext_shout('ownership'), ext_concat2('a', 'b');")
            .unwrap();
        match result.into_iter().next().unwrap() {
            QueryResult::Selected { rows, .. } => assert_eq!(
                rows,
                vec![vec![
                    Value::Varchar("OWNERSHIP".to_string()),
                    Value::Varchar("ab".to_string()),
                ]]
            ),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    // And directly through the ABI handle, with a longer string.
    let extension = pinkdb_ext_loader::load(&path).unwrap();
    let shout = extension
        .functions()
        .iter()
        .find(|f| f.name() == "ext_shout")
        .expect("ext_shout registered")
        .clone();
    let long = "x".repeat(1000);
    let upper = "X".repeat(1000);
    for _ in 0..direct_iterations {
        let got = shout.call(&[ExtArg::Varchar(long.as_bytes())]).unwrap();
        assert_eq!(got, ExtValue::Varchar(upper.as_bytes().to_vec()));
    }

    let after = text_allocation_stats();
    let expected = sql_iterations * 2 + direct_iterations;
    assert_eq!(
        after.allocations - before.allocations,
        expected,
        "one allocation per VARCHAR result"
    );
    assert_eq!(
        after.frees - before.frees,
        expected,
        "every extension buffer must be freed by the host"
    );
    assert_eq!(after.live(), 0, "no outstanding result text");
}
