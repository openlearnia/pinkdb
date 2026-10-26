//! The Postgres catalog (`catalog-postgres`, wave 96a).
//!
//! The same shared catalog suite as SQLite, gated by
//! `PINKDB_TEST_POSTGRES_URL`: unset prints one skip line per test and passes
//! (the local and CI jobs set it to a throwaway server). Every test gets a
//! fresh `w96a_<pid>_<seq>` schema, dropped when its suite is dropped.
//!
//! The Postgres-only assertions (URL redaction, `sslmode` policy, advisory
//! locks) live in `catalog_store/postgres.rs` and run without a server.

#![cfg(feature = "catalog-postgres")]
#[path = "common/local_lake.rs"]
mod local_lake;

#[cfg(feature = "s3")]
#[path = "../src/mock_s3.rs"]
mod mock_s3;

// The shared SQLite/Postgres suite. Included directly (not through `common`,
// which other test binaries share) because it needs this binary's `mock_s3`;
// each backend binary uses only part of it.
#[path = "common/catalog_suite.rs"]
mod catalog_suite;

use catalog_suite::{PostgresSuite, Suite};
use pink_lake::{Lake, LakeError};

macro_rules! postgres_test {
    ($name:ident) => {
        #[test]
        fn $name() {
            let Some(mut suite) = PostgresSuite::from_env(stringify!($name)) else {
                return;
            };
            catalog_suite::$name(&mut suite);
            suite.finish(stringify!($name));
        }
    };
}

postgres_test!(create_save_open_round_trip);
postgres_test!(two_handles_conflict_then_rebase);
postgres_test!(eight_threads_publish_contiguous_versions);
postgres_test!(time_travel_reads_older_versions);
postgres_test!(expire_and_cleanup_on_a_split_lake);
postgres_test!(cdc_reports_the_committed_rows);
postgres_test!(replication_state_round_trips);
postgres_test!(a_failure_after_the_snapshot_insert_rolls_back_the_publication);
postgres_test!(data_location_ownership_is_enforced);
// Catalog migration is the legacy JSON-to-SQL copy operation deleted in
// Part 2 (the SQLite/pink suites already ignore it). It is not a shared-suite
// assertion about the DuckLake tables, so it does not run here.

#[cfg(feature = "s3")]
postgres_test!(attach_and_count_issue_no_data_requests_over_s3);

#[cfg(feature = "encryption")]
postgres_test!(encrypted_catalog_rows_are_sealed);

/// Opening a schema that does not exist is `NotALake` and creates nothing (a
/// mistyped URL never leaves an empty schema behind).
#[test]
fn open_of_a_fresh_schema_creates_nothing() {
    let Some(mut suite) = PostgresSuite::from_env("open_of_a_fresh_schema_creates_nothing") else {
        return;
    };
    let dir = catalog_suite::scratch("fresh-schema");
    let catalog = suite.catalog(&dir, "fresh");
    let err = Lake::open_url(&catalog).unwrap_err();
    assert!(matches!(err, LakeError::NotALake { .. }), "{err:?}");
    assert!(
        !suite.schema_exists(&catalog),
        "opening a fresh catalog must not create its schema"
    );
    catalog_suite::cleanup(&dir);
    suite.finish("open_of_a_fresh_schema_creates_nothing");
}

/// A catalog row that no longer exists fails the open, and the error names the
/// catalog location (the URL as given) with any password redacted.
#[test]
fn a_catalog_without_its_metadata_names_its_redacted_location() {
    let Some(mut suite) =
        PostgresSuite::from_env("a_catalog_without_its_metadata_names_its_redacted_location")
    else {
        return;
    };
    let dir = catalog_suite::scratch("catalog-label");
    let catalog = suite.catalog(&dir, "label");
    let data = dir.join("data");
    let lake = Lake::create_with_catalog(&catalog, data.to_str().unwrap()).unwrap();
    drop(lake);
    suite.sql(&catalog, "DROP TABLE ducklake_metadata");

    let err = Lake::open_url(&catalog).unwrap_err();
    assert!(matches!(err, LakeError::NotALake { .. }), "{err:?}");
    let message = err.to_string();
    assert!(
        message.contains(&pink_lake::redact_location(&catalog)),
        "the error must name the catalog: {message}"
    );
    // The deleted JSON layout's file name must not appear in a catalog error.
    assert!(!message.contains(concat!("catalog", ".json")), "{message}");

    // A password in the URL never reaches the message (a server that rejects
    // the injected password stops the open before the catalog check; then the
    // only assertion is that the secret is scrubbed).
    let with_password = catalog.replacen('@', ":sekret@", 1);
    let err = Lake::open_url(&with_password).unwrap_err();
    let message = err.to_string();
    assert!(
        !message.contains("sekret"),
        "the password leaked: {message}"
    );
    if matches!(err, LakeError::NotALake { .. }) {
        assert!(
            message.contains(&pink_lake::redact_location(&with_password)),
            "the error must name the redacted catalog: {message}"
        );
    }

    catalog_suite::cleanup(&dir);
    suite.finish("a_catalog_without_its_metadata_names_its_redacted_location");
}
