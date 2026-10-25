//! Result-text ownership, measured on all four SDK fixtures.
//!
//! The ABI says a non-NULL VARCHAR result must come from the host's `alloc`,
//! that the host copies `text_len` bytes and frees it immediately, and that
//! nothing is leaked on any path — a successful call, a zero-length result, a
//! NULL result, or a callback that returns an error code after publishing
//! text. Every SDK's fixture therefore has to balance the loader's
//! process-wide allocation counters.
//!
//! This file contains a single test on purpose: the counters are global, and
//! each integration-test file runs as its own process, so nothing else can
//! perturb them (`tests/varchar_ownership.rs` does the same for the C test
//! extensions).

mod common;

use common::conformance::{Fixture, fixtures};
use pinkdb_ext_loader::{ExtArg, ExtType, ExtValue, TextAllocStats, load, text_allocation_stats};

fn function(
    ext: &pinkdb_ext_loader::LoadedExtension,
    name: &str,
) -> pinkdb_ext_loader::DynamicFunction {
    ext.functions()
        .iter()
        .find(|f| f.name() == name)
        .unwrap_or_else(|| panic!("the fixture registered no {name}()"))
        .clone()
}

fn snapshot() -> TextAllocStats {
    let stats = text_allocation_stats();
    TextAllocStats {
        allocations: stats.allocations,
        frees: stats.frees,
    }
}

#[test]
fn every_varchar_result_is_allocated_by_the_host_and_freed_by_it() {
    for fixture in fixtures() {
        let Fixture {
            sdk,
            library,
            fail_allocates,
            ..
        } = fixture;
        let ext = load(library.to_str().expect("UTF-8 fixture path"))
            .unwrap_or_else(|e| panic!("the {sdk} fixture failed to load: {e}"));
        let baseline = snapshot();

        // A successful VARCHAR result: allocated by the fixture through the
        // host's `alloc`, freed by the host right after it copies the bytes.
        assert_eq!(
            function(&ext, "cf_echo")
                .call(&[ExtArg::Varchar(b"abc")])
                .unwrap(),
            ExtValue::Varchar(b"abc".to_vec()),
            "{sdk}"
        );
        let after_echo = snapshot();
        assert_eq!(
            after_echo.allocations - baseline.allocations,
            1,
            "{sdk}: one host allocation for one VARCHAR result"
        );
        assert_eq!(
            after_echo.frees - baseline.frees,
            1,
            "{sdk}: the host frees extension text"
        );

        // Zero-length and NULL results own no buffer at all.
        assert_eq!(
            function(&ext, "cf_empty").call(&[]).unwrap(),
            ExtValue::Varchar(Vec::new()),
            "{sdk}"
        );
        assert_eq!(
            function(&ext, "cf_fill")
                .call(&[ExtArg::Bigint(-1)])
                .unwrap(),
            ExtValue::Null,
            "{sdk}"
        );
        // Whether an SDK allocates a buffer for an empty string is its own
        // choice (the host frees any non-NULL pointer); what must hold is that
        // nothing is left outstanding.
        let after_empty = snapshot();
        assert_eq!(
            after_empty.live(),
            after_echo.live(),
            "{sdk}: an empty or NULL VARCHAR leaks result text"
        );

        // An error path that published text first: the host still owns and
        // frees the buffer. (The Rust SDK's typed writer refuses to publish a
        // text result on an error path, so it allocates nothing — the caps
        // word says which behaviour a fixture has.)
        let error = function(&ext, "cf_fail")
            .call(&[ExtArg::Bigint(1)])
            .expect_err("cf_fail always fails");
        assert!(
            matches!(
                &error,
                pinkdb_ext_loader::ExtError::Callback { code: 1, .. }
            ),
            "{sdk}: {error}"
        );
        let after_fail = snapshot();
        let allocated_on_error = after_fail.allocations - after_empty.allocations;
        if *fail_allocates {
            assert!(
                allocated_on_error >= 1,
                "{sdk}: the caps word says cf_fail allocates before failing"
            );
        } else {
            assert_eq!(
                allocated_on_error, 0,
                "{sdk}: the caps word says cf_fail allocates nothing"
            );
        }
        assert_eq!(
            after_fail.live(),
            after_empty.live(),
            "{sdk}: a text result returned with an error is freed, not leaked"
        );

        // A NULL argument allocates nothing either.
        assert_eq!(
            function(&ext, "cf_echo")
                .call(&[ExtArg::Null(ExtType::Varchar)])
                .unwrap(),
            ExtValue::Null,
            "{sdk}"
        );
        let after_null = snapshot();
        assert_eq!(after_null.live(), baseline.live(), "{sdk}: leaked text");
    }
}
