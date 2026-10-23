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
postgres_test!(migrate_object_catalog_to_sql);
postgres_test!(migrate_sql_catalog_to_object);
postgres_test!(migrate_sql_catalog_to_sql);

#[cfg(feature = "s3")]
postgres_test!(attach_and_count_issue_no_data_requests_over_s3);

#[cfg(feature = "encryption")]
postgres_test!(encrypted_catalog_rows_are_sealed);

#[cfg(feature = "encryption")]
postgres_test!(encrypted_migration_keeps_sealed_bytes);

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
