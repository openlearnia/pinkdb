//! The typed metadata layer of the lake catalog (wave 103).
//!
//! A lake catalog is a database, not a tree of JSON objects. This module is
//! the feature-independent core: a [`MetadataStore`] trait of typed operations
//! on the engine-facing [`Catalog`] model, and the small value types the
//! implementations exchange. No object paths cross this boundary — the store
//! owns the physical layout (DuckLake's tables, ids and `begin_snapshot` /
//! `end_snapshot` visibility rules), while callers keep using the same
//! [`Catalog`] they always did.
//!
//! # Version numbers
//!
//! DuckLake's snapshot ids start at 0 (the schema-creation snapshot); pinkdb's
//! versions have always started at 1 for the empty create catalog. The store
//! keeps DuckLake's ids on disk and converts at this boundary:
//! `pink_version = snapshot_id + 1`. The conversion lives here so a backend
//! never has to remember which side of the boundary it is on.
//!
//! # The commit contract
//!
//! [`MetadataStore::commit`] is one database transaction with DuckLake's
//! commit loop: read the latest snapshot, compare it with the caller's base,
//! insert the new `ducklake_snapshot` row as the commit point, append rows
//! with `begin_snapshot = v` and end removed rows with `end_snapshot = v`.
//! A moving head is a retryable [`LakeError::Conflict`] (the caller rebases
//! exactly as it does for the object-store catalog). Data files are written
//! before the transaction opens; a failed commit can only leave unreferenced
//! files behind.
//!
//! # Dialects
//!
//! The SQL is written once. [`Dialect`] names the small syntactic differences
//! a backend needs (placeholder style, DDL spellings, transaction start).
//! SQLite and the `.pink` database are implemented on the same core; Postgres
//! reuses it in T2.

use crate::Result;
use crate::catalog::{Catalog, CommitManifest, TableEntry};

// The core is compiled in every build (it has no optional dependencies), and
// the `.pink` backend is always available: pinkdb is already a dependency.
// Only the rusqlite half is behind `catalog-sqlite`.
mod conn;
#[allow(dead_code)]
pub(crate) mod dialect;
pub(crate) mod pink;
pub(crate) mod store;

#[cfg(feature = "catalog-sqlite")]
pub(crate) mod sqlite;

#[cfg(feature = "catalog-postgres")]
pub(crate) mod postgres;

/// One commit's typed change.
///
/// A publication either carries the complete target catalog ([`CatalogChange::catalog`],
/// used by the compatibility `catalog.json` CAS and by maintenance rewrites
/// that still build a whole target) or the typed delta the lake already
/// computed ([`CatalogChange::manifest`], the O(delta) path used by commits
/// and saves). `schema_version`, `format_version` and `updated` are the
/// catalog-handle facts that are not part of the per-table delta.
#[derive(Debug, Clone)]
#[allow(dead_code)] // consumed by the SQL backend; the core is compiled in every build
pub(crate) struct CatalogChange {
    /// Complete target catalog. When present the store diffs it against the
    /// base it reads inside the transaction; when absent it applies the
    /// delta manifest directly.
    pub(crate) catalog: Option<Catalog>,
    /// The typed per-table delta (always present; a full-catalog change also
    /// carries its manifest when the caller has one).
    pub(crate) manifest: CommitManifest,
    /// The lake-wide schema version after this commit.
    pub(crate) schema_version: u64,
    /// The catalog format version after this commit.
    pub(crate) format_version: u32,
    /// The publish time in the catalog's `updated` form.
    pub(crate) updated: String,
    pub(crate) lake_id: Option<String>,
    pub(crate) author: Option<String>,
    pub(crate) commit_message: Option<String>,
    /// The engine catalog image to publish with this version (wave 96b). The
    /// engine's metadata and its table data must become visible together, so
    /// the image rides the commit's transaction instead of a second one.
    pub(crate) engine_catalog: Option<std::sync::Arc<Vec<u8>>>,
}

/// The typed metadata surface every catalog database implements.
///
/// Implementations are `Send + Sync` (a lake handle shares one across threads)
/// and must make [`MetadataStore::commit`] atomic.
#[allow(dead_code)] // the trait is the backend contract; a caller reaches most of it through the facade
pub(crate) trait MetadataStore: std::fmt::Debug + Send + Sync {
    /// The absolute data location recorded in the catalog metadata, or `None`
    /// for a catalog that was created without one.
    fn data_location(&self) -> Result<Option<String>>;

    /// The pinkdb lake identity recorded at create, if any.
    fn lake_id(&self) -> Result<Option<String>>;

    /// Store the lake identity (the side table `lake.json` used to hold).
    fn set_lake_id(&self, lake_id: &str) -> Result<()>;

    /// Update the recorded data location (create; open never rewrites it).
    fn set_data_location(&self, location: &str) -> Result<()>;

    /// Record where a retired catalog moved to (wave 96a migration marker).
    fn moved_to(&self) -> Result<Option<String>>;
    fn set_moved_to(&self, target: &str) -> Result<()>;

    /// The latest pinkdb version (`None` for a catalog with no snapshots).
    fn head_version(&self) -> Result<Option<u64>>;

    /// The engine catalog image at the head revision (wave 96b):
    /// `(revision, image)` — `None` for a catalog that never carried one.
    fn engine_catalog(&self) -> Result<Option<(u64, Vec<u8>)>>;

    /// Replace the engine catalog image at `revision` — the standalone
    /// publish for a lake whose tables are already committed (a lake created
    /// by the CLI or a `--file` server, adopted by a catalog server).
    fn put_engine_catalog(&self, revision: u64, image: &[u8]) -> Result<()>;

    /// Every snapshot version, ascending.
    fn snapshot_versions(&self) -> Result<Vec<u64>>;

    /// Whether one snapshot version exists.
    fn snapshot_exists(&self, version: u64) -> Result<bool>;

    /// Build the engine-facing head catalog, **with every table loaded**.
    ///
    /// Lake-wide operations (flush, merge/rewrite, add-files) and any caller
    /// that walks `tables` must use this. It is O(lake), so commit planning
    /// uses [`MetadataStore::head_shell`] plus the tables the delta names.
    fn head(&self) -> Result<Option<Catalog>>;

    /// Build the head catalog handle — version, format, identity, author and
    /// change metadata — with `tables` deliberately left empty.
    ///
    /// Only commit planning may use this: it must then load every table it
    /// operates on. The distinct name keeps a table-less catalog from being
    /// mistaken for an empty lake.
    fn head_shell(&self) -> Result<Option<Catalog>>;

    /// Load one table's engine-facing entry (all files, delete files and
    /// inlined rows) at `version` (`None` = head).
    fn table(&self, key: &str, version: Option<u64>) -> Result<Option<TableEntry>>;

    /// [`MetadataStore::table`] without per-file statistics blocks. The
    /// default keeps the full entry; a backend that stores statistics
    /// separately can skip them, which is what a writable attachment's diff
    /// needs.
    fn table_layout(&self, key: &str, version: Option<u64>) -> Result<Option<TableEntry>> {
        self.table(key, version)
    }

    /// Load one table's non-file facts at `version`: identity, format,
    /// columns, renames, partition spec, inlining limit, row count and
    /// next-row-id. `files`, `delete_files` and `inlined` are left empty.
    /// This is what an append needs; it never walks the table's file list.
    fn table_stub(&self, key: &str, version: Option<u64>) -> Result<Option<TableEntry>>;

    /// Build the engine-facing catalog visible at `version` (`None` = head).
    /// Returns `None` when the snapshot does not exist.
    fn load(&self, version: Option<u64>) -> Result<Option<Catalog>>;

    /// Expire snapshots: delete their snapshot and change rows, then remove
    /// the catalog rows no remaining snapshot can see (DuckLake's rule: an
    /// ended row is dead when no remaining snapshot lies in
    /// `[begin_snapshot, end_snapshot)`) and schedule their files for
    /// deletion. One transaction.
    fn expire_snapshots(&self, versions: &[u64]) -> Result<()>;

    /// Files in `ducklake_files_scheduled_for_deletion`, as
    /// `(id, catalog-relative path)`.
    fn scheduled_files(&self) -> Result<Vec<(i64, String)>>;

    /// Remove rows from `ducklake_files_scheduled_for_deletion` by id (the
    /// files were deleted from the data location).
    fn forget_scheduled(&self, ids: &[i64]) -> Result<()>;

    /// Every data/delete file path any catalog row names, in the two spellings
    /// a data location may use (`data/<schema>/<table>/<file>` and
    /// DuckLake's own `<schema>/<table>/<file>`). The orphan scan treats a
    /// data object as referenced when it matches either.
    fn referenced_file_paths(&self) -> Result<Vec<String>>;

    /// The `field_id`s a data file of `key` must record for `columns`, in
    /// column order.
    ///
    /// A DuckLake reader matches a nested column's children by id, so the
    /// writer needs the catalog's `ducklake_column.column_id`s *before* the
    /// commit that creates the columns. Backends without ids return nothing,
    /// and their files record none.
    fn field_ids(
        &self,
        key: &str,
        columns: &[(String, pinkdb::DataType)],
    ) -> Result<Vec<crate::format::FieldIds>> {
        let _ = (key, columns);
        Ok(Vec::new())
    }

    /// The replication side tables as the in-memory state (`None` before the
    /// first job is created).
    fn replication_state(&self) -> Result<Option<crate::replication::ReplicationState>>;

    /// Replace the replication side tables in one transaction.
    fn store_replication_state(&self, state: &crate::replication::ReplicationState) -> Result<()>;

    /// The per-file key recorded for a data or delete file whose catalog row
    /// names `rel` (the path's basename). `None` for a plaintext file.
    fn file_key(&self, rel: &str) -> Result<Option<Vec<u8>>>;

    /// Every recorded per-file key (diagnostics/tests: prove each file has
    /// its own 256-bit key).
    fn file_keys(&self) -> Result<Vec<Vec<u8>>>;

    /// Whether `ducklake_metadata.encrypted` says the lake's data files are
    /// sealed (DuckLake's own flag).
    fn is_encrypted(&self) -> Result<bool>;

    /// Set `ducklake_metadata.encrypted`.
    fn set_encrypted(&self, encrypted: bool) -> Result<()>;

    /// Publish `change` on top of `base_version` in one transaction.
    ///
    /// A head that moved past `base_version` is [`crate::LakeError::Conflict`]
    /// (the caller rebases). A successful commit returns the new pinkdb
    /// version.
    fn commit(&self, base_version: u64, change: CatalogChange) -> Result<u64>;

    /// The stored commit-delta manifest (`commits/<version>.json`'s logical
    /// content), if the store keeps one. Used by change-data reads.
    fn commit_manifest(&self, version: u64) -> Result<Option<Vec<u8>>>;

    /// Store/read an opaque catalog-side object (replication state, the
    /// `lake.json`/`highest.json` compatibility rows, …). This is not the
    /// old blob layout: the catalog's own tables are the source of truth;
    /// these rows only carry facts DuckLake has no column for.
    fn put_object(&self, rel: &str, bytes: &[u8]) -> Result<bool>;
    /// [`Self::put_object`], replacing an existing row instead of refusing it:
    /// the engine lease's heartbeat updates one row in place (wave 96b).
    fn replace_object(&self, rel: &str, bytes: &[u8]) -> Result<bool>;
    fn get_object(&self, rel: &str) -> Result<Option<Vec<u8>>>;
    fn delete_object(&self, rel: &str) -> Result<()>;
    fn list_objects(&self, prefix: &str) -> Result<Vec<String>>;

    /// Create the catalog: `ducklake_metadata`, snapshot 0
    /// (`created_schema:"main"`) and the side rows. Must be atomic.
    fn create(&self, data_location: &str, created_by: &str, catalog: &Catalog) -> Result<()>;
}

/// pinkdb's version for one DuckLake snapshot id.
#[allow(dead_code)] // the conversion boundary; only a SQL backend calls it today
pub(crate) fn pink_version(snapshot_id: i64) -> u64 {
    snapshot_id.max(0) as u64 + 1
}

/// The DuckLake snapshot id for one pinkdb version.
#[allow(dead_code)] // the conversion boundary; only a SQL backend calls it today
pub(crate) fn snapshot_id(version: u64) -> Result<i64> {
    if version == 0 {
        return Err(crate::LakeError::Internal {
            message: "the lake catalog received version 0 (pinkdb versions start at 1)".to_string(),
        });
    }
    i64::try_from(version - 1).map_err(|_| crate::LakeError::Internal {
        message: format!("lake version {version} does not fit a DuckLake snapshot id"),
    })
}
