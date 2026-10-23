//! The SQLite DuckLake catalog (wave 103).
//!
//! The physical metadata lives in the normalized DuckLake v1.1 tables
//! ([`SqliteMetadataStore`]). This type is the thin compatibility facade the
//! lake code already speaks: `catalog.json`, `snapshots/<v>.json`,
//! `commits/<v>.json`, `lake.json`, `highest.json` and `replication/…` are
//! *rendered* from those rows (or stored as opaque side rows), never kept as
//! a second full-manifest copy.
//!
//! The interesting translation is the publication protocol:
//!
//! - `put_new("commits/<v>.json")` is a create-only existence check: the
//!   commit manifest is derived from the DuckLake rows (files beginning at
//!   the snapshot, ended rows, inlined rows and `snapshot_changes`), never
//!   stored;
//! - `put_new("snapshots/<v>.json")` is a create-only existence check (the
//!   snapshot row itself appears only at the commit point);
//! - `put_if_match("catalog.json", bytes, tag)` is the full-catalog commit
//!   fallback: parse the target [`Catalog`], diff it against the base
//!   DuckLake snapshot in one transaction, and insert the delta rows. The
//!   ordinary commit and save paths bypass this and hand
//!   [`SqliteMetadataStore::commit`] a typed [`crate::metadata::CatalogChange`]
//!   with only the touched tables, so their cost is O(delta), not O(lake).
//!   The `tag` is the rendered head version (`v<version>`), so a moved head is
//!   the same conflict the object layout reports.
//!
//! Reads render the engine-facing catalog from DuckLake's rows
//! (`begin_snapshot` / `end_snapshot`), which is why a catalog written by
//! DuckDB opens here and a catalog written here attaches in DuckDB.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::CatalogStore;
use crate::catalog::Catalog;
use crate::local::CATALOG_FILE;
use crate::metadata::sqlite::SqliteMetadataStore;
use crate::metadata::{CatalogChange, MetadataStore};
use crate::storage::{Head, LocalCopy, ObjectInfo, PublishScope, Storage, VersionTag};
use crate::{HIGHEST_FILE, LAKE_FILE, LakeError, Result};

/// The SQLite catalog facade over one [`SqliteMetadataStore`].
#[derive(Debug)]
pub(crate) struct SqliteCatalog {
    store: Arc<SqliteMetadataStore>,
    url: String,
}

impl SqliteCatalog {
    /// Open the one lake in `path`. With `create`, an absent file (and its
    /// parent directory) is created; without it, an absent file is
    /// `NotALake` and nothing is written.
    pub(crate) fn open(url: &str, path: &Path, create: bool) -> Result<Self> {
        Ok(Self {
            store: Arc::new(SqliteMetadataStore::open(url, path, create)?),
            url: url.to_string(),
        })
    }

    /// Wrap a store the caller already opened (the direct create/open path in
    /// [`crate::Lake`] uses this so it can read or seed the catalog without
    /// opening a second connection).
    pub(crate) fn from_store(url: &str, store: SqliteMetadataStore) -> Self {
        Self {
            store: Arc::new(store),
            url: url.to_string(),
        }
    }

    /// Render the engine-facing catalog at `version` (`None` = head) as the
    /// manifest bytes the rest of the lake code expects.
    fn render(&self, version: Option<u64>) -> Result<Option<Vec<u8>>> {
        let Some(catalog) = self.store.load(version)? else {
            return Ok(None);
        };
        let rel = match version {
            Some(v) => format!("snapshots/{v}.json"),
            None => CATALOG_FILE.to_string(),
        };
        Ok(Some(crate::manifest_json(&catalog, &self.abs(&rel))?))
    }

    /// The identity the catalog's rendered `lake.json`/`highest.json` use.
    fn effective_lake_id(&self) -> Result<Option<String>> {
        match self.store.lake_id()? {
            Some(id) => Ok(Some(id)),
            None => Ok(self.store.load(None)?.and_then(|catalog| catalog.lake_id)),
        }
    }

    /// The virtual value of one metadata path, or `None` when the row/version
    /// does not exist.
    fn object(&self, rel: &str) -> Result<Option<Vec<u8>>> {
        if rel == LAKE_FILE {
            return match self.effective_lake_id()? {
                Some(id) => Ok(Some(crate::identity_json(&id)?)),
                None => Ok(None),
            };
        }
        if rel == HIGHEST_FILE {
            let Some(version) = self.store.head_version()? else {
                return Ok(None);
            };
            let Some(id) = self.effective_lake_id()? else {
                return Ok(None);
            };
            return Ok(Some(crate::highest_json(&id, version)?));
        }
        if rel == CATALOG_FILE {
            return self.render(None);
        }
        if let Some(version) = snapshot_version(rel) {
            return self.render(Some(version));
        }
        if let Some(version) = commit_version(rel) {
            return self.store.commit_manifest(version);
        }
        self.store.get_object(rel)
    }

    /// The content tag of one opaque side object. It only has to change when
    /// the bytes change; replication's read-modify-write compares it.
    fn object_tag(&self, rel: &str) -> Result<Option<VersionTag>> {
        Ok(self
            .object(rel)?
            .map(|bytes| VersionTag(format!("{:016x}", fnv1a(&bytes)))))
    }

    fn path_of(&self, rel: &str) -> PathBuf {
        if rel.is_empty() {
            PathBuf::from(&self.url)
        } else {
            PathBuf::from(format!("{}#{rel}", self.url))
        }
    }
}

impl Storage for SqliteCatalog {
    fn metadata_store(&self) -> Option<Arc<dyn crate::metadata::MetadataStore>> {
        Some(self.store.clone())
    }

    fn abs(&self, rel: &str) -> PathBuf {
        self.path_of(rel)
    }

    fn head(&self, rel: &str) -> Result<Head> {
        let exists = if rel == CATALOG_FILE {
            self.store.head_version()?.is_some()
        } else if rel == LAKE_FILE {
            self.effective_lake_id()?.is_some()
        } else if rel == HIGHEST_FILE {
            self.store.head_version()?.is_some()
        } else if let Some(version) = snapshot_version(rel) {
            self.store.snapshot_exists(version)?
        } else if let Some(version) = commit_version(rel) {
            self.store.commit_manifest_exists(version)?
        } else {
            self.store.object_exists(rel)?
        };
        Ok(if exists { Head::File } else { Head::Missing })
    }

    fn stat(&self, rel: &str) -> Result<Option<ObjectInfo>> {
        let Some(bytes) = self.object(rel)? else {
            return Ok(None);
        };
        Ok(Some(ObjectInfo {
            size: bytes.len() as u64,
            modified_micros: None,
        }))
    }

    fn read(&self, rel: &str, _context: &str) -> Result<Vec<u8>> {
        self.object(rel)?.ok_or_else(|| missing(self.abs(rel)))
    }

    fn read_optional(&self, rel: &str, _context: &str) -> Result<Option<Vec<u8>>> {
        self.object(rel)
    }

    fn read_tagged(&self, rel: &str, context: &str) -> Result<(Vec<u8>, VersionTag)> {
        let bytes = self.read(rel, context)?;
        let tag = if rel == CATALOG_FILE {
            let head = self
                .store
                .head_version()?
                .ok_or_else(|| missing(self.abs(rel)))?;
            VersionTag(format!("v{head}"))
        } else if let Some(version) = snapshot_version(rel) {
            VersionTag(format!("v{version}"))
        } else {
            VersionTag(format!("{:016x}", fnv1a(&bytes)))
        };
        Ok((bytes, tag))
    }

    fn read_range(&self, _rel: &str, _offset: u64, _len: u64, _context: &str) -> Result<Vec<u8>> {
        Err(LakeError::Internal {
            message: "ranged reads are a data-location operation; the catalog store is never \
                      range-read"
                .to_string(),
        })
    }

    fn read_with_stored(&self, rel: &str, context: &str) -> Result<(Vec<u8>, Vec<u8>)> {
        let bytes = self.read(rel, context)?;
        Ok((bytes.clone(), bytes))
    }

    fn create_dir(&self, _rel: &str) -> Result<()> {
        // A catalog has keys, not directories.
        Ok(())
    }

    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        if rel == CATALOG_FILE {
            // Only the direct create/open path publishes a catalog without a
            // commit; it seeds the store itself and never reaches here.
            return Err(LakeError::Internal {
                message: "catalog.json is written through put_if_match, not put".to_string(),
            });
        }
        if let Some(version) = commit_version(rel) {
            self.store.replace_commit_manifest(version, bytes)?;
            return Ok(());
        }
        if snapshot_version(rel).is_some() || rel == LAKE_FILE || rel == HIGHEST_FILE {
            // Rendered from the rows; a rewrite is a no-op.
            return Ok(());
        }
        self.store.put_object(rel, bytes, true)?;
        Ok(())
    }

    fn put_new(&self, rel: &str, bytes: &[u8]) -> Result<bool> {
        if rel == LAKE_FILE {
            let id = crate::parse_identity(self, bytes)?;
            if self.store.lake_id()?.is_some() {
                return Ok(false);
            }
            self.store.set_lake_id(&id)?;
            return Ok(true);
        }
        if rel == HIGHEST_FILE {
            // Rendered from the snapshot table.
            return Ok(true);
        }
        if rel == CATALOG_FILE {
            // A create publishes the catalog through the store's own create
            // transaction; the object-store create path is not used here.
            return Ok(self.store.head_version()?.is_none());
        }
        if let Some(version) = snapshot_version(rel) {
            return Ok(!self.store.snapshot_exists(version)?);
        }
        if let Some(version) = commit_version(rel) {
            return self.store.put_commit_manifest(version, bytes);
        }
        self.store.put_object(rel, bytes, false)
    }

    fn put_if_match(&self, rel: &str, bytes: &[u8], expected: &VersionTag) -> Result<bool> {
        if rel == CATALOG_FILE {
            // The tag is the rendered head version; `ensure_catalog_unchanged`
            // read it just before this call.
            let Some(head) = self.store.head_version()? else {
                return Ok(false);
            };
            if expected.0 != format!("v{head}") {
                return Ok(false);
            }
            let catalog: Catalog =
                serde_json::from_slice(bytes).map_err(|e| LakeError::Corrupt {
                    path: self.abs(rel),
                    message: format!("the commit's catalog target does not parse: {e}"),
                })?;
            let Some(base) = catalog.version.checked_sub(1) else {
                return Ok(false);
            };
            self.store.commit_in_transaction(
                base,
                CatalogChange {
                    manifest: crate::catalog::CommitManifest {
                        format_version: catalog.format_version,
                        version: catalog.version,
                        base_version: base,
                        updated: catalog.updated.clone(),
                        tables: std::collections::BTreeMap::new(),
                    },
                    schema_version: catalog.schema_version,
                    format_version: catalog.format_version,
                    updated: catalog.updated.clone(),
                    lake_id: catalog.lake_id.clone(),
                    author: catalog.author.clone(),
                    commit_message: catalog.commit_message.clone(),
                    catalog: Some(catalog),
                },
            )?;
            return Ok(true);
        }
        match self.object_tag(rel)? {
            Some(tag) if &tag == expected => {
                self.store.put_object(rel, bytes, true)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn list(&self, rel: &str) -> Result<Vec<String>> {
        self.list_at(rel, true)
    }

    fn list_recursive(&self, rel: &str) -> Result<Vec<String>> {
        self.list_at(rel, false)
    }

    fn delete(&self, rel: &str) -> Result<()> {
        if rel == CATALOG_FILE
            || rel == LAKE_FILE
            || rel == HIGHEST_FILE
            || snapshot_version(rel).is_some()
            || commit_version(rel).is_some()
        {
            // Retained catalog rows are removed by maintenance (wave 103 T2);
            // the object facade has no delete for them.
            return Ok(());
        }
        self.store.delete_object(rel)
    }

    fn local_copy(&self, rel: &str, _context: &str) -> Result<LocalCopy> {
        Err(LakeError::Internal {
            message: format!(
                "the catalog object {} is not a data file; local copies are a data-location \
                 operation",
                self.abs(rel).display()
            ),
        })
    }

    fn local_path(&self, _rel: &str) -> Option<PathBuf> {
        None
    }

    fn reconcile_snapshot(&self, _version: u64, _bytes: &[u8]) -> Result<()> {
        // Conditional writes are transactional; nothing to repair.
        Ok(())
    }

    fn uses_degraded_fallback(&self) -> bool {
        false
    }

    fn begin_publish(&self) -> Result<PublishScope> {
        match self.store.begin_transaction()? {
            Some(tx) => {
                let store = Arc::clone(&self.store);
                Ok(PublishScope::new(Box::new(move |commit| {
                    store.finish_transaction(&tx, commit)
                })))
            }
            // A nested publication is part of the outer transaction; only the
            // outermost scope commits or rolls back.
            None => Ok(PublishScope::noop()),
        }
    }

    fn catalog_split(&self) -> bool {
        true
    }

    fn seal_manifest(&self, plain: &[u8], _lake_id: &str, _version: u64) -> Result<Vec<u8>> {
        Ok(plain.to_vec())
    }

    fn tighten_existing(&self) -> Result<()> {
        Ok(())
    }
}

impl SqliteCatalog {
    fn list_at(&self, rel: &str, direct: bool) -> Result<Vec<String>> {
        match rel {
            "snapshots" => Ok(self
                .store
                .snapshot_versions()?
                .into_iter()
                .map(|v| format!("{v}.json"))
                .collect()),
            "commits" => Ok(self
                .store
                .commit_versions()?
                .into_iter()
                .map(|v| format!("{v}.json"))
                .collect()),
            "replication" => self.list_objects_at("replication/", direct),
            "" => {
                let mut out = vec![
                    CATALOG_FILE.to_string(),
                    HIGHEST_FILE.to_string(),
                    LAKE_FILE.to_string(),
                ];
                out.extend(self.list_objects_at("replication/", true)?);
                out.sort();
                Ok(out)
            }
            other if other.starts_with("replication/") => {
                self.list_objects_at(&format!("{}/", other.trim_end_matches('/')), direct)
            }
            _ => Ok(Vec::new()),
        }
    }

    fn list_objects_at(&self, prefix: &str, direct: bool) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for path in self.store.list_objects(prefix)? {
            let Some(tail) = path.strip_prefix(prefix) else {
                continue;
            };
            if direct && tail.contains('/') {
                continue;
            }
            out.push(tail.to_string());
        }
        out.sort();
        out.dedup();
        Ok(out)
    }
}

impl CatalogStore for SqliteCatalog {
    fn data_location(&self) -> Result<Option<String>> {
        MetadataStore::data_location(&*self.store)
    }

    fn set_data_location(&self, location: &str) -> Result<()> {
        self.store.set_data_location(location)
    }

    fn set_lake_id(&self, lake_id: &str) -> Result<()> {
        self.store.set_lake_id(lake_id)
    }

    fn lake_id(&self) -> Result<Option<String>> {
        MetadataStore::lake_id(&*self.store)
    }

    fn moved_to(&self) -> Result<Option<String>> {
        self.store.moved_to()
    }

    fn set_moved_to(&self, target: &str) -> Result<()> {
        self.store.set_moved_to(target)
    }
}

fn snapshot_version(rel: &str) -> Option<u64> {
    let stem = rel.strip_prefix("snapshots/")?.strip_suffix(".json")?;
    crate::canonical_version_number(stem)
}

fn commit_version(rel: &str) -> Option<u64> {
    let stem = rel.strip_prefix("commits/")?.strip_suffix(".json")?;
    crate::canonical_version_number(stem)
}

/// A tiny content tag (FNV-1a). Only equality matters, and it never leaves
/// this process: `put_if_match` compares it against the value `read_tagged`
/// just returned.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

fn missing(path: PathBuf) -> LakeError {
    LakeError::Io {
        context: format!("reading {}", path.display()),
        source: std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("no catalog object at {}", path.display()),
        ),
    }
}
