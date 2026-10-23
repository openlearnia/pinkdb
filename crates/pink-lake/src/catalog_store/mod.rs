//! The split between a lake's catalog and its data (wave 96a).
//!
//! A lake's metadata lives in exactly six key families (`lake.json`,
//! `catalog.json`, `highest.json`, `snapshots/…`, `commits/…`,
//! `replication/…`); everything else (`data/…`, delete files, the
//! `lake-data.json` ownership marker) is data. [`SplitStorage`] routes the
//! metadata families to a [`CatalogStore`] (a SQLite database today) and
//! every data method to the ordinary local/S3 storage unchanged — so data
//! reads keep their ranged/concurrent paths and their transport counters, and
//! the object-store layout stays byte-identical when no split is used.
//!
//! The encryption decorator wraps the split storage, so a keyed lake's
//! metadata rows hold sealed blobs: there is no plaintext metadata in the
//! database for a keyed lake.

use std::path::PathBuf;
use std::sync::Arc;

use crate::local::CATALOG_FILE;
use crate::storage::Location;
use crate::storage::{
    Head, LocalCopy, ObjectInfo, PublishScope, Storage, StorageStats, VersionTag,
};
use crate::{HIGHEST_FILE, LAKE_FILE, LakeError, Result};

pub(crate) mod url;

#[cfg(feature = "catalog-sqlite")]
mod sqlite;

#[cfg(feature = "catalog-sqlite")]
pub(crate) use sqlite::SqliteCatalog;

#[cfg(feature = "catalog-postgres")]
mod postgres;

#[cfg(feature = "catalog-postgres")]
pub(crate) use postgres::PostgresCatalog;

/// Whether `rel` is one of the six catalog metadata families.
///
/// The bare directory names are included because `Storage::list` is called
/// with `"snapshots"` / `"commits"` and must answer from the catalog too.
pub(crate) fn is_metadata_path(rel: &str) -> bool {
    rel == LAKE_FILE
        || rel == CATALOG_FILE
        || rel == HIGHEST_FILE
        || rel == "snapshots"
        || rel == "commits"
        || rel == "replication"
        || rel.starts_with("snapshots/")
        || rel.starts_with("commits/")
        || rel.starts_with("replication/")
}

/// The SQL-facing half of a [`Storage`]: the metadata object methods plus the
/// small `pink_lake_meta` surface a split lake needs to find its data location
/// and identity again. Data methods are never called on a catalog store (the
/// [`SplitStorage`] routes them elsewhere), so their implementations are
/// deliberately inert.
pub(crate) trait CatalogStore: Storage {
    /// The data location recorded at create (`None` when the catalog was
    /// created without one or the row was removed).
    fn data_location(&self) -> Result<Option<String>>;

    /// Record the data location at create; opening a catalog never rewrites
    /// it (an explicit open override is session-local).
    fn set_data_location(&self, location: &str) -> Result<()>;

    /// Record the lake identity in `pink_lake_meta` (the authoritative copy
    /// stays `lake.json`, so an ordinary object-store open still works).
    fn set_lake_id(&self, lake_id: &str) -> Result<()>;

    /// The lake identity recorded by create, if any.
    fn lake_id(&self) -> Result<Option<String>>;

    /// The target a retired catalog was moved to (wave 96a migration). Set on
    /// the source after a successful `migrate_catalog`; opening it then fails
    /// with the clear "the catalog moved to …" error instead of `NotALake`.
    fn moved_to(&self) -> Result<Option<String>>;

    /// Retire this catalog by recording where it moved to. The rows stay (a
    /// hand recovery can copy them back); only `catalog.json` is ignored by
    /// the open path.
    fn set_moved_to(&self, target: &str) -> Result<()>;
}

/// Open a catalog location's backend. `create` is set only by the create
/// path; opening a catalog that does not exist is `NotALake` and leaves no
/// empty database behind. Without `catalog-sqlite`, a `sqlite:` URL is the
/// existing clear rebuild hint rather than a parse or I/O error.
pub(crate) fn open_catalog(
    catalog_url: &str,
    location: &Location,
    create: bool,
) -> Result<Arc<dyn CatalogStore>> {
    match location {
        Location::Sqlite(path) => {
            #[cfg(feature = "catalog-sqlite")]
            {
                Ok(Arc::new(SqliteCatalog::open(catalog_url, path, create)?))
            }
            #[cfg(not(feature = "catalog-sqlite"))]
            {
                let _ = (path, create);
                Err(LakeError::FeatureMissing {
                    feature: "catalog-sqlite",
                    message: format!(
                        "the location {catalog_url} is a SQL catalog; rebuild with the \
                         `catalog-sqlite` feature (for the CLI: `cargo build -p pinkdb-cli \
                         --features pinklake,catalog-sqlite`)"
                    ),
                })
            }
        }
        Location::Postgres(_) => {
            #[cfg(feature = "catalog-postgres")]
            {
                Ok(Arc::new(PostgresCatalog::open(catalog_url, create)?))
            }
            #[cfg(not(feature = "catalog-postgres"))]
            {
                let _ = create;
                Err(LakeError::FeatureMissing {
                    feature: "catalog-postgres",
                    message: format!(
                        "the location {} is a Postgres catalog; rebuild with the \
                         `catalog-postgres` feature (for the CLI: `cargo build -p pinkdb-cli \
                         --features pinklake,catalog-postgres`)",
                        url::redact_location(catalog_url)
                    ),
                })
            }
        }
        other => Err(LakeError::BadLocation {
            location: url::redact_location(catalog_url),
            message: format!(
                "expected a catalog location (`sqlite:<path>` or `postgres://…`), got {}",
                describe(other)
            ),
        }),
    }
}

fn describe(location: &Location) -> String {
    match location {
        Location::Local(path) => format!("the local path {}", path.display()),
        Location::S3 { bucket, prefix } => {
            format!("the object-storage location s3://{bucket}/{prefix}")
        }
        Location::Sqlite(path) => format!("the SQL catalog sqlite:{}", path.display()),
        Location::Postgres(catalog) => {
            format!("the Postgres catalog {}", url::redact_location(catalog))
        }
    }
}

/// A catalog store plus the data storage every non-metadata object goes to.
///
/// Every trait method is forwarded explicitly (the same rule as the
/// encryption decorator): the data half keeps `read_range` / `ranged_reads` /
/// `stats` / `local_path` / `put_streamed`, so an attach or query over the
/// split is exactly as cheap as over the data backend alone.
#[derive(Debug)]
pub(crate) struct SplitStorage {
    catalog: Arc<dyn CatalogStore>,
    /// The same store as a plain `Storage`: one forwarding field avoids a
    /// per-call trait upcast and keeps the `Arc` alive for the call.
    catalog_storage: Arc<dyn Storage>,
    data: Arc<dyn Storage>,
}

impl SplitStorage {
    pub(crate) fn new(catalog: Arc<dyn CatalogStore>, data: Arc<dyn Storage>) -> Self {
        let catalog_storage = Arc::clone(&catalog) as Arc<dyn Storage>;
        Self {
            catalog,
            catalog_storage,
            data,
        }
    }

    /// The half a lake-relative path belongs to.
    fn route(&self, rel: &str) -> &Arc<dyn Storage> {
        if !rel.is_empty() && is_metadata_path(rel) {
            &self.catalog_storage
        } else {
            &self.data
        }
    }
}

impl Storage for SplitStorage {
    fn abs(&self, rel: &str) -> PathBuf {
        // `abs("")` is the lake root the handle exposes (`Lake::path`, the
        // `data_path` option): for a split lake that is the *data* location.
        // Every metadata path names the catalog itself (`sqlite:<path>#<rel>`).
        self.route(rel).abs(rel)
    }

    fn head(&self, rel: &str) -> Result<Head> {
        self.route(rel).head(rel)
    }

    fn stat(&self, rel: &str) -> Result<Option<ObjectInfo>> {
        self.route(rel).stat(rel)
    }

    fn read(&self, rel: &str, context: &str) -> Result<Vec<u8>> {
        self.route(rel).read(rel, context)
    }

    fn read_optional(&self, rel: &str, context: &str) -> Result<Option<Vec<u8>>> {
        self.route(rel).read_optional(rel, context)
    }

    fn read_range(&self, rel: &str, offset: u64, len: u64, context: &str) -> Result<Vec<u8>> {
        self.route(rel).read_range(rel, offset, len, context)
    }

    fn ranged_reads(&self) -> bool {
        // The lake asks this once for the data-file read path; metadata is
        // never range-read.
        self.data.ranged_reads()
    }

    fn stats(&self) -> StorageStats {
        // The catalog may be in a database (its transport counters are zero);
        // the data backend is what an attach/query actually waits on.
        let catalog = self.catalog.stats();
        let data = self.data.stats();
        StorageStats {
            requests: catalog.requests + data.requests,
            bytes: catalog.bytes + data.bytes,
            wait_nanos: catalog.wait_nanos + data.wait_nanos,
        }
    }

    fn read_tagged(&self, rel: &str, context: &str) -> Result<(Vec<u8>, VersionTag)> {
        self.route(rel).read_tagged(rel, context)
    }

    fn read_with_stored(&self, rel: &str, context: &str) -> Result<(Vec<u8>, Vec<u8>)> {
        self.route(rel).read_with_stored(rel, context)
    }

    fn create_dir(&self, rel: &str) -> Result<()> {
        // `create_dir("")` prepares the data root; a metadata path is a key,
        // not a directory.
        self.route(rel).create_dir(rel)
    }

    fn put(&self, rel: &str, bytes: &[u8]) -> Result<()> {
        self.route(rel).put(rel, bytes)
    }

    fn put_streamed(
        &self,
        rel: &str,
        fill: &mut dyn FnMut(&mut (dyn std::io::Write + Send)) -> Result<()>,
    ) -> Result<u64> {
        self.route(rel).put_streamed(rel, fill)
    }

    fn put_new(&self, rel: &str, bytes: &[u8]) -> Result<bool> {
        self.route(rel).put_new(rel, bytes)
    }

    fn put_if_match(&self, rel: &str, bytes: &[u8], expected: &VersionTag) -> Result<bool> {
        self.route(rel).put_if_match(rel, bytes, expected)
    }

    fn list(&self, rel: &str) -> Result<Vec<String>> {
        self.route(rel).list(rel)
    }

    fn list_recursive(&self, rel: &str) -> Result<Vec<String>> {
        self.route(rel).list_recursive(rel)
    }

    fn delete(&self, rel: &str) -> Result<()> {
        self.route(rel).delete(rel)
    }

    fn local_copy(&self, rel: &str, context: &str) -> Result<LocalCopy> {
        self.route(rel).local_copy(rel, context)
    }

    fn local_path(&self, rel: &str) -> Option<PathBuf> {
        self.route(rel).local_path(rel)
    }

    fn exists(&self, rel: &str) -> Result<bool> {
        self.route(rel).exists(rel)
    }

    fn reconcile_snapshot(&self, version: u64, bytes: &[u8]) -> Result<()> {
        self.catalog.reconcile_snapshot(version, bytes)
    }

    fn uses_degraded_fallback(&self) -> bool {
        // A SQL catalog enforces every condition in one transaction; there is
        // no check-then-put window to repair.
        false
    }

    fn encrypted(&self) -> bool {
        false
    }

    fn seal_manifest(&self, plain: &[u8], lake_id: &str, version: u64) -> Result<Vec<u8>> {
        self.catalog.seal_manifest(plain, lake_id, version)
    }

    fn set_manifest_lake_id(&self, lake_id: Option<String>) {
        self.catalog.set_manifest_lake_id(lake_id);
    }

    fn put_highest(&self, rel: &str, bytes: &[u8], lake_id: &str, version: u64) -> Result<()> {
        self.route(rel).put_highest(rel, bytes, lake_id, version)
    }

    fn read_highest(&self, rel: &str, context: &str, lake_id: &str) -> Result<Vec<u8>> {
        self.route(rel).read_highest(rel, context, lake_id)
    }

    fn read_highest_optional(
        &self,
        rel: &str,
        context: &str,
        lake_id: &str,
    ) -> Result<Option<Vec<u8>>> {
        self.route(rel).read_highest_optional(rel, context, lake_id)
    }

    fn tighten_existing(&self) -> Result<()> {
        // Data files are the only objects with file modes; the catalog store
        // has nothing to tighten.
        self.data.tighten_existing()
    }

    fn begin_publish(&self) -> Result<PublishScope> {
        self.catalog.begin_publish()
    }

    fn catalog_split(&self) -> bool {
        true
    }
}
