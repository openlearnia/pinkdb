//! Test support: a lake rooted at one scratch directory. The catalog is a
//! `pink:<dir>/catalog.pink` database and the data lives in `<dir>/data`, so a
//! test that used to treat the directory as an object-store lake keeps one
//! root for cleanup and path assertions.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

#[cfg(feature = "s3")]
use pink_lake::S3Config;
use pink_lake::{EncryptionKey, Lake, Result};
use pinkdb::PinkDb;

thread_local! {
    static BACKEND: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
}

/// Run one test body against a catalog backend (`pink` or `sqlite`).
pub fn with_backend(kind: &str, body: impl FnOnce()) {
    BACKEND.with(|slot| *slot.borrow_mut() = kind.to_string());
    body();
}

fn backend_kind() -> String {
    BACKEND.with(|slot| {
        let kind = slot.borrow();
        if kind.is_empty() {
            "pink".to_string()
        } else {
            kind.clone()
        }
    })
}

fn helper_catalog_url(dir: &Path) -> String {
    let kind = backend_kind();
    if kind == "sqlite" {
        format!("sqlite:{}", dir.join("catalog.sqlite").display())
    } else {
        format!("pink:{}", dir.join("catalog.pink").display())
    }
}

pub fn catalog_url(dir: &Path) -> String {
    // The scratch directory is usually a directory holding `catalog.pink`;
    // a few tests call the bare-path API directly, which makes the directory
    // path itself the `.pink` catalog (pinkdb keeps its WAL beside it).
    if backend_kind() == "sqlite" {
        return helper_catalog_url(dir);
    }
    let helper = dir.join("catalog.pink");
    let bare_wal = PathBuf::from(format!("{}.wal", dir.display()));
    if helper.exists() || dir.join("catalog.pink.wal").exists() {
        helper_catalog_url(dir)
    } else if bare_wal.exists() || dir.is_file() {
        format!("pink:{}", dir.display())
    } else {
        // A fresh scratch directory: the helper creates `catalog.pink`.
        helper_catalog_url(dir)
    }
}

pub fn data_dir(dir: &Path) -> PathBuf {
    // The old object-store layout used the scratch directory itself as the
    // data root (`<dir>/data/...`); keeping that shape lets the ported tests
    // keep their path assertions while the catalog moves to `<dir>/catalog.pink`.
    dir.to_path_buf()
}

pub fn create_lake(dir: impl AsRef<Path>) -> Result<Lake> {
    let dir = dir.as_ref();
    std::fs::create_dir_all(dir).map_err(|e| pink_lake::LakeError::Io {
        context: format!("creating {}", dir.display()),
        source: e,
    })?;
    Lake::create_with_catalog(&helper_catalog_url(dir), data_dir(dir).to_str().unwrap())
}

pub fn create_lake_with_key(dir: impl AsRef<Path>, key: EncryptionKey) -> Result<Lake> {
    let dir = dir.as_ref();
    std::fs::create_dir_all(dir).map_err(|e| pink_lake::LakeError::Io {
        context: format!("creating {}", dir.display()),
        source: e,
    })?;
    Lake::create_with_catalog_with_key(
        &helper_catalog_url(dir),
        data_dir(dir).to_str().unwrap(),
        key,
    )
}

pub fn open_lake(dir: impl AsRef<Path>) -> Result<Lake> {
    Lake::open_with_catalog(&catalog_url(dir.as_ref()), None)
}

pub fn open_or_create_lake(dir: impl AsRef<Path>) -> Result<Lake> {
    match open_lake(dir.as_ref()) {
        Ok(lake) => Ok(lake),
        Err(pink_lake::LakeError::NotALake { .. }) => create_lake(dir.as_ref()),
        Err(e) => Err(e),
    }
}

pub fn open_lake_with_key(dir: impl AsRef<Path>, key: EncryptionKey) -> Result<Lake> {
    Lake::open_with_catalog_with_key(&catalog_url(dir.as_ref()), None, key)
}

pub fn save_store_lake(db: &PinkDb, dir: impl AsRef<Path>) -> Result<u64> {
    let dir = dir.as_ref();
    let mut lake = match open_lake(dir) {
        Ok(lake) => lake,
        Err(pink_lake::LakeError::NotALake { .. }) => create_lake(dir)?,
        Err(e) => return Err(e),
    };
    lake.write_from_store(db)
}

pub fn save_store_lake_with_key(
    db: &PinkDb,
    dir: impl AsRef<Path>,
    key: EncryptionKey,
) -> Result<u64> {
    let dir = dir.as_ref();
    let mut lake = match open_lake_with_key(dir, key.clone()) {
        Ok(lake) => lake,
        Err(pink_lake::LakeError::NotALake { .. }) => create_lake_with_key(dir, key)?,
        Err(e) => return Err(e),
    };
    lake.write_from_store(db)
}

pub fn lake_location(dir: &Path) -> String {
    catalog_url(dir)
}

/// A temp `pink:` catalog for explicit-S3-config tests.
#[cfg(feature = "s3")]
pub fn s3_catalog(config: &S3Config) -> String {
    let sanitize = |text: &str| -> String {
        text.chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect()
    };
    let dir = std::env::temp_dir()
        .join("pinkdb-s3-catalogs")
        .join(std::process::id().to_string())
        .join(sanitize(&config.bucket))
        .join(sanitize(&config.prefix));
    std::fs::create_dir_all(&dir).ok();
    format!("pink:{}", dir.join("catalog.pink").display())
}

#[cfg(feature = "s3")]
pub fn create_s3_lake(config: S3Config) -> Result<Lake> {
    let catalog = s3_catalog(&config);
    Lake::create_with_catalog_s3(&catalog, config)
}

#[cfg(feature = "s3")]
pub fn open_s3_lake(config: S3Config) -> Result<Lake> {
    let catalog = s3_catalog(&config);
    Lake::open_with_catalog_s3(&catalog, config)
}

#[cfg(feature = "s3")]
pub fn open_or_create_s3_lake(config: S3Config) -> Result<Lake> {
    match open_s3_lake(config.clone()) {
        Ok(lake) => Ok(lake),
        Err(pink_lake::LakeError::NotALake { .. }) => create_s3_lake(config),
        Err(e) => Err(e),
    }
}

/// The typed catalog rendered into the shape the ported tests inspect: each
/// table's engine entry as JSON (`files` with stats/partition, `deletes`,
/// `inlined`, schema facts), exactly as the old catalog carried it.
pub fn catalog_json(dir: &Path) -> serde_json::Value {
    let lake = open_lake(dir).unwrap();
    let mut tables = serde_json::Map::new();
    for info in lake.table_info().unwrap() {
        if let Some(entry) = pink_lake::catalog_table_json(&catalog_url(dir), &info.table).unwrap()
        {
            tables.insert(info.table.clone(), entry);
        }
    }
    let schema_version = lake
        .snapshots()
        .unwrap()
        .last()
        .map(|s| s.schema_version)
        .unwrap_or(0);
    serde_json::json!({
        "version": lake.current_version(),
        "format_version": lake.format_version(),
        "schema_version": schema_version,
        "tables": tables,
    })
}

pub fn entry_json(lake: &Lake, info: &pink_lake::TableInfo) -> serde_json::Value {
    let _ = (lake, info);
    serde_json::Value::Null
}
