//! Without a SQL catalog feature, that location is the library's clear
//! rebuild hint (`FeatureMissing`), never a parse or I/O error. The split
//! catalogs themselves are covered by `tests/sqlite_catalog.rs` and
//! `tests/postgres_catalog.rs`.
#[path = "common/local_lake.rs"]
mod local_lake;

#[cfg(any(not(feature = "catalog-sqlite"), not(feature = "catalog-postgres")))]
use pink_lake::{Lake, LakeError};

#[cfg(any(not(feature = "catalog-sqlite"), not(feature = "catalog-postgres")))]
fn catalog_gate(catalog: &str, feature: &str, data: &str) {
    for result in [
        Lake::open_url(catalog),
        Lake::create_with_catalog(catalog, data),
    ] {
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                LakeError::FeatureMissing {
                    feature: missing,
                    ..
                } if missing == feature
            ),
            "{err:?}"
        );
        assert!(err.to_string().contains(feature), "{err}");
    }
}

#[cfg(not(feature = "catalog-sqlite"))]
#[test]
fn a_sqlite_catalog_needs_the_feature() {
    let dir = std::env::temp_dir().join(format!("pink_lake_catalog_gate_{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    let catalog = format!("sqlite:{}", dir.join("catalog.db").display());
    catalog_gate(
        &catalog,
        "catalog-sqlite",
        dir.join("data").to_str().unwrap(),
    );
}

#[cfg(not(feature = "catalog-postgres"))]
#[test]
fn a_postgres_catalog_needs_the_feature() {
    let dir = std::env::temp_dir().join(format!("pink_lake_postgres_gate_{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    catalog_gate(
        "postgres://lake:secret@127.0.0.1:55432/pinktest?schema=gate",
        "catalog-postgres",
        dir.join("data").to_str().unwrap(),
    );
}
