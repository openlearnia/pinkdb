//! Replication against database catalogs (wave 103 T2): the shared suite in
//! `common/replication_catalog_suite.rs`, run against `pink:` always and
//! `sqlite:` with the `catalog-sqlite` feature.

#[path = "common/replication_catalog_suite.rs"]
mod suite;

use suite::PinkSuite;

macro_rules! pink_test {
    ($name:ident) => {
        #[test]
        fn $name() {
            suite::$name(&mut PinkSuite);
        }
    };
}

pink_test!(store_source_full_and_merge);
pink_test!(snapshot_lake_source_incremental);
pink_test!(watermark_lake_source);
pink_test!(include_exclude_and_ownership);

#[cfg(feature = "catalog-sqlite")]
macro_rules! sqlite_test {
    ($name:ident, $fn_name:ident) => {
        #[test]
        fn $fn_name() {
            suite::$name(&mut suite::SqliteSuite);
        }
    };
}

#[cfg(feature = "catalog-sqlite")]
sqlite_test!(
    store_source_full_and_merge,
    store_source_full_and_merge_sqlite
);
#[cfg(feature = "catalog-sqlite")]
sqlite_test!(
    snapshot_lake_source_incremental,
    snapshot_lake_source_incremental_sqlite
);
#[cfg(feature = "catalog-sqlite")]
sqlite_test!(watermark_lake_source, watermark_lake_source_sqlite);
#[cfg(feature = "catalog-sqlite")]
sqlite_test!(
    include_exclude_and_ownership,
    include_exclude_and_ownership_sqlite
);
