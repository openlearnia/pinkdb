//! Without `catalog-sqlite`, a `sqlite:` location is the library's clear
//! rebuild hint (`FeatureMissing`), never a parse or I/O error. The split
//! catalog itself is covered by `tests/sqlite_catalog.rs`.

#![cfg(not(feature = "catalog-sqlite"))]

use pink_lake::{Lake, LakeError};

#[test]
fn a_sqlite_catalog_needs_the_feature() {
    let dir = std::env::temp_dir().join(format!("pink_lake_catalog_gate_{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    let catalog = format!("sqlite:{}", dir.join("catalog.db").display());

    for result in [
        Lake::open_url(&catalog),
        Lake::create_with_catalog(&catalog, dir.join("data").to_str().unwrap()),
    ] {
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                LakeError::FeatureMissing {
                    feature: "catalog-sqlite",
                    ..
                }
            ),
            "{err:?}"
        );
        assert!(err.to_string().contains("catalog-sqlite"), "{err}");
    }
}
