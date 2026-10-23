//! The SQLite catalog (`catalog-sqlite`, wave 96a): the shared catalog suite
//! (`common/catalog_suite.rs`) run against `sqlite:<path>`, plus the
//! SQLite-only location validation.
//!
//! Every metadata read here goes through the SQL rows; the data location keeps
//! the ordinary local/S3 paths, so the assertions are about routing,
//! transactions and bytes — not a second copy of the deeper lake suites.

#![cfg(feature = "catalog-sqlite")]

#[cfg(feature = "s3")]
#[path = "../src/mock_s3.rs"]
mod mock_s3;

// The shared SQLite/Postgres suite. Included directly (not through `common`,
// which other test binaries share) because it needs this binary's `mock_s3`;
// each backend binary uses only part of it.
#[path = "common/catalog_suite.rs"]
mod catalog_suite;

use catalog_suite::SqliteSuite;
use pink_lake::{Lake, LakeError};

/// `sqlite::memory:` and an empty path cannot back a persistent lake.
#[test]
fn sqlite_location_validation() {
    for location in ["sqlite:", "sqlite::memory:"] {
        let err = Lake::open_url(location).unwrap_err();
        assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
        assert!(err.to_string().contains("must persist"), "{err}");
    }
    // Opening a catalog that does not exist is `NotALake` and creates
    // nothing (a mistyped path never leaves an empty database behind).
    let dir = catalog_suite::scratch("missing-catalog");
    let missing = dir.join("typo.db");
    let err = Lake::open_url(&format!("sqlite:{}", missing.display())).unwrap_err();
    assert!(matches!(err, LakeError::NotALake { .. }), "{err:?}");
    assert!(
        !missing.exists(),
        "open must not create {}",
        missing.display()
    );
    catalog_suite::cleanup(&dir);

    // The data half of a split lake cannot itself be a catalog.
    let err = Lake::create_with_catalog("sqlite:/tmp/a.db", "sqlite:/tmp/b.db").unwrap_err();
    assert!(matches!(err, LakeError::BadLocation { .. }), "{err:?}");
    assert!(err.to_string().contains("data location"), "{err}");
}

#[test]
fn create_save_open_round_trip() {
    catalog_suite::create_save_open_round_trip(&mut SqliteSuite::new());
}

#[test]
fn two_handles_conflict_then_rebase() {
    catalog_suite::two_handles_conflict_then_rebase(&mut SqliteSuite::new());
}

#[test]
fn eight_threads_publish_contiguous_versions() {
    catalog_suite::eight_threads_publish_contiguous_versions(&mut SqliteSuite::new());
}

#[test]
fn time_travel_reads_older_versions() {
    catalog_suite::time_travel_reads_older_versions(&mut SqliteSuite::new());
}

#[test]
fn expire_and_cleanup_on_a_split_lake() {
    catalog_suite::expire_and_cleanup_on_a_split_lake(&mut SqliteSuite::new());
}

#[test]
fn cdc_reports_the_committed_rows() {
    catalog_suite::cdc_reports_the_committed_rows(&mut SqliteSuite::new());
}

#[cfg(feature = "s3")]
#[test]
fn attach_and_count_issue_no_data_requests_over_s3() {
    catalog_suite::attach_and_count_issue_no_data_requests_over_s3(&mut SqliteSuite::new());
}

#[test]
fn replication_state_round_trips() {
    catalog_suite::replication_state_round_trips(&mut SqliteSuite::new());
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_catalog_rows_are_sealed() {
    catalog_suite::encrypted_catalog_rows_are_sealed(&mut SqliteSuite::new());
}

#[test]
fn a_failure_after_the_snapshot_insert_rolls_back_the_publication() {
    catalog_suite::a_failure_after_the_snapshot_insert_rolls_back_the_publication(
        &mut SqliteSuite::new(),
    );
}

#[test]
fn data_location_ownership_is_enforced() {
    catalog_suite::data_location_ownership_is_enforced(&mut SqliteSuite::new());
}

#[test]
fn migrate_object_catalog_to_sql() {
    catalog_suite::migrate_object_catalog_to_sql(&mut SqliteSuite::new());
}

#[test]
fn migrate_sql_catalog_to_object() {
    catalog_suite::migrate_sql_catalog_to_object(&mut SqliteSuite::new());
}

#[test]
fn migrate_sql_catalog_to_sql() {
    catalog_suite::migrate_sql_catalog_to_sql(&mut SqliteSuite::new());
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_migration_keeps_sealed_bytes() {
    catalog_suite::encrypted_migration_keeps_sealed_bytes(&mut SqliteSuite::new());
}
