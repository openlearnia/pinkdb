//! The backend-generic SQL catalog core (wave 103).
//!
//! One lake per catalog database, laid out exactly like DuckLake v1.1:
//! `ducklake_metadata`, `ducklake_snapshot`, `_snapshot_changes`, `_schema`,
//! `_table`, `_column`, `_data_file`, `_delete_file`, the statistics tables,
//! the partition tables, `_inlined_data_tables` plus the typed
//! `ducklake_inlined_data_<table_id>_<schema_version>` tables. Every row
//! carries DuckLake's `begin_snapshot` / `end_snapshot` visibility pair, and
//! one commit is one write transaction that inserts only the delta.
//!
//! This module owns every statement and the whole commit loop. It never names
//! a backend: [`SqlConnection`] supplies execute/query/transaction and the
//! [`Dialect`] supplies the handful of syntactic differences (see
//! [`super::conn`]). The SQLite adapter (`catalog-sqlite`) and the pinkdb
//! `.pink` adapter are thin; T2's Postgres is a third.
//!
//! pinkdb-only facts live in the `pinkdb_*` side tables, keyed by DuckLake's
//! own ids: the per-table catalog key and pink table identity, the table and
//! schema epochs, footer offsets/CRC, and typed per-commit facts. Statistics
//! blocks and commit manifests are derived from the DuckLake rows, never
//! stored as JSON. DuckLake never reads those tables, so a DuckDB-created
//! catalog opens here without them and a pink-written catalog attaches in
//! DuckDB.
//!
//! A manifest is *rendered* from the rows on read, never stored as a whole.
//! A commit diffs the base catalog it reads inside the transaction against
//! its target and writes only the changed rows.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::DataType;

use super::conn::{SqlConnection, SqlConnectionExt, SqlValue, encode_hex, params, query_one};
use super::dialect::{DUCKLAKE_VERSION, Dialect};
use super::{CatalogChange, MetadataStore, pink_version, snapshot_id};
use crate::catalog::{
    Catalog, ColumnEntry, CommitManifest, DeleteRef, DeltaKind, FileFormat, FileRef, InlinedRow,
    PartitionKey, RenameEntry, TableDelta, TableEntry, TableMeta, TableRuntime,
};
use crate::replication::{Job, JobStatus, ReplicationState, Source, Strategy, TableState};
use crate::stats::{self, StatsBlock};
use crate::{LakeError, Result};

/// The side-table layout revision. A row with another value is a newer (or
/// unrelated) pinkdb catalog: refused, never guessed at.
const SIDE_VERSION: i64 = 1;

/// One live publish transaction on this thread. A thread runs at most one
/// store's publication at a time (the lake code is synchronous).
struct ActiveTx {
    store_id: u64,
    conn: Box<dyn SqlConnection>,
}

thread_local! {
    static ACTIVE_TX: RefCell<Option<ActiveTx>> = const { RefCell::new(None) };
}

/// Process-unique store ids, so the thread-local publish slot can tell two
/// handles on the same file apart.
static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);
thread_local! {
    /// Rows the backend reported as changed by the last successful delta
    /// commit on this thread. The scaling test reads it to prove commit #300
    /// touches exactly as many rows as commit #10 (no table-wide rewrite).
    /// Thread-local keeps parallel tests from clobbering each other's
    /// measurement.
    static LAST_COMMIT_ROWS_TOUCHED: Cell<u64> = const { Cell::new(0) };
}

pub(crate) fn last_commit_rows_touched() -> u64 {
    LAST_COMMIT_ROWS_TOUCHED.with(|rows| rows.get())
}

#[derive(Debug)]
struct Inner {
    id: u64,
    /// The catalog location errors name: the URL as given, password redacted.
    label: PathBuf,
    conn: Box<dyn SqlConnection>,
}

/// The backend-generic SQL metadata store. The adapters in `sqlite.rs` and
/// `pink.rs` construct it with their connection; every operation below is
/// shared.
#[derive(Debug, Clone)]
pub(crate) struct SqlMetadataStore {
    inner: Arc<Inner>,
}

/// The transaction one SQL catalog publication runs in.
pub(crate) struct StoreTransaction {
    store_id: u64,
}

impl std::fmt::Debug for StoreTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreTransaction")
            .field("store_id", &self.store_id)
            .finish()
    }
}

/// A table row visible at one snapshot, with its schema name.
#[derive(Debug, Clone)]
struct TableRow {
    table_id: i64,
    schema_path: String,
    table_path: String,
    table_uuid: String,
    table_key: String,
}

/// The head snapshot's allocation counters.
#[derive(Debug, Clone, Copy)]
struct SnapshotState {
    schema_version: u64,
    next_catalog_id: i64,
    next_file_id: i64,
}

impl SqlMetadataStore {
    /// Open the one lake in `path` over a backend connection. With `create`,
    /// an absent file (and its parent directory, so
    /// `sqlite:/tmp/new-lake/catalog.db` works like a lake directory does) is
    /// created; without it, an absent file is `NotALake` and nothing is
    /// written. `open` is the adapter's constructor: it opens the backend
    /// (durably) and returns the connection this store owns.
    pub(crate) fn open(
        url: &str,
        path: &Path,
        create: bool,
        open: impl FnOnce(&str, &Path, bool) -> Result<Box<dyn SqlConnection>>,
    ) -> Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| LakeError::Io {
                context: format!("creating the catalog directory {}", parent.display()),
                source: e,
            })?;
        }
        Self::open_with(url, path, create, open)
    }

    /// Open a catalog that has no local file (Postgres). The URL is the
    /// catalog's identity; the adapter creates its own schema.
    #[cfg(feature = "catalog-postgres")]
    pub(crate) fn open_remote(
        url: &str,
        _create: bool,
        open: impl FnOnce(&str, &Path, bool) -> Result<Box<dyn SqlConnection>>,
    ) -> Result<Self> {
        let path = PathBuf::from(crate::redact_location(url));
        Self::open_with(url, &path, _create, open)
    }

    fn open_with(
        url: &str,
        path: &Path,
        create: bool,
        open: impl FnOnce(&str, &Path, bool) -> Result<Box<dyn SqlConnection>>,
    ) -> Result<Self> {
        let label = PathBuf::from(crate::redact_location(url));
        let conn = open(url, path, create)?;
        let dialect = conn.dialect();
        let initialized = has_ducklake_tables(&*conn, dialect, &label)?;
        if create {
            conn.execute_batch(&dialect.ducklake_ddl())
                .map_err(|e| sql_error("creating the DuckLake catalog schema", e))?;
            conn.execute_batch(&dialect.side_ddl())
                .map_err(|e| sql_error("creating the pinkdb side tables", e))?;
        } else if !initialized {
            return Err(LakeError::NotALake { path: label });
        } else if !table_exists(&*conn, "pinkdb_meta")?
            || !table_exists(&*conn, "pinkdb_replication_state")?
        {
            // A catalog written by DuckDB has the `ducklake_*` tables and no
            // pinkdb side tables. They are additive (`IF NOT EXISTS`) and
            // DuckLake never reads them, so create them in place: the side
            // tables carry only facts DuckLake has no column for.
            //
            // A catalog this build already wrote has `pinkdb_meta`; running
            // the whole DDL batch again on every open is pure parse/execute
            // cost (pinkdb re-parses each statement), so skip it. The side
            // version below still refuses an incompatible layout.
            conn.execute_batch(&dialect.side_ddl())
                .map_err(|e| sql_error("creating the pinkdb side tables", e))?;
        }
        let store = Self {
            inner: Arc::new(Inner {
                id: NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed),
                label,
                conn,
            }),
        };
        store.init_side_meta()?;
        Ok(store)
    }

    /// Seed/validate `pinkdb_meta` and refuse an unknown side-table revision.
    fn init_side_meta(&self) -> Result<()> {
        self.with_conn(|conn| {
            let existing: Option<String> = conn
                .query_row(
                    "SELECT value FROM pinkdb_meta WHERE key = 'side_version'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|e| sql_error("reading pinkdb_meta", e))?;
            match existing {
                Some(value) => {
                    if value != SIDE_VERSION.to_string() {
                        return Err(LakeError::Unsupported {
                            path: self.label().to_path_buf(),
                            found: value.parse().unwrap_or(0),
                        });
                    }
                }
                None => {
                    conn.execute(
                        "INSERT INTO pinkdb_meta (key, value) VALUES ('side_version', ?1)",
                        params![SIDE_VERSION.to_string()],
                    )
                    .map_err(|e| sql_error("writing pinkdb_meta", e))?;
                }
            }
            Ok(())
        })
    }

    /// The catalog location errors name (redacted URL).
    fn label(&self) -> &Path {
        &self.inner.label
    }

    /// Run `f` on the active publish transaction when this store owns one on
    /// this thread, else on the shared connection.
    pub(crate) fn with_conn<T>(
        &self,
        f: impl FnOnce(&dyn SqlConnection) -> Result<T>,
    ) -> Result<T> {
        ACTIVE_TX.with(|slot| {
            let borrowed = slot.borrow();
            if let Some(tx) = borrowed.as_ref()
                && tx.store_id == self.inner.id
            {
                return f(tx.conn.as_ref());
            }
            drop(borrowed);
            f(self.inner.conn.as_ref())
        })
    }

    /// Open the thread's write transaction. Nested scopes are reported so the
    /// adapter can treat them as part of the outer transaction.
    pub(crate) fn begin_transaction(&self) -> Result<Option<StoreTransaction>> {
        let already_active = ACTIVE_TX.with(|slot| match slot.borrow().as_ref() {
            Some(tx) if tx.store_id == self.inner.id => Ok(true),
            Some(_) => Err(LakeError::Internal {
                message: "another catalog store already has an open publish scope on this thread"
                    .to_string(),
            }),
            None => Ok(false),
        })?;
        if already_active {
            return Ok(None);
        }
        let conn = self
            .inner
            .conn
            .begin()
            .map_err(|e| sql_error("beginning the catalog transaction", e))?;
        let store_id = self.inner.id;
        ACTIVE_TX.with(|slot| {
            *slot.borrow_mut() = Some(ActiveTx { store_id, conn });
        });
        Ok(Some(StoreTransaction { store_id }))
    }

    /// Commit or roll back the thread's active transaction.
    pub(crate) fn finish_transaction(&self, tx: &StoreTransaction, commit: bool) -> Result<()> {
        ACTIVE_TX.with(|slot| {
            let mut slot = slot.borrow_mut();
            let Some(active) = slot.take() else {
                return Ok(());
            };
            if active.store_id != tx.store_id {
                *slot = Some(active);
                return Err(LakeError::Internal {
                    message: "a publish scope finished another store's transaction".to_string(),
                });
            }
            if commit {
                active
                    .conn
                    .commit()
                    .map_err(|e| sql_error("committing the catalog transaction", e))
            } else {
                let _ = active.conn.rollback();
                Ok(())
            }
        })
    }

    // ---------- typed reads ----------

    /// Build the engine-facing catalog visible at `version` (`None` = head).
    pub(crate) fn load(&self, version: Option<u64>) -> Result<Option<Catalog>> {
        self.with_conn(|conn| load_conn_inner(conn, version, true))
    }

    /// The head's handle facts (version, format, identity, schema version)
    /// without assembling its tables or reading any data-file/delete rows.
    ///
    /// Open needs only these to construct a handle; a catalog whose rows are
    /// readable metadata but whose data (or delete format) this build cannot
    /// read must still *open*, with the refusal surfacing on the table read.
    pub(crate) fn head_catalog(&self) -> Result<Option<Catalog>> {
        self.with_conn(|conn| load_conn_inner(conn, None, false))
    }

    /// One table's full entry at `version` (`None` = head).
    pub(crate) fn table(&self, key: &str, version: Option<u64>) -> Result<Option<TableEntry>> {
        self.with_conn(|conn| {
            let sid = resolve_sid(conn, version, self.label())?;
            let Some(table) = find_table(conn, key, sid)? else {
                return Ok(None);
            };
            let schema_version = snapshot_state(conn, sid)?
                .map(|state| state.schema_version)
                .unwrap_or(0);
            Ok(Some(load_table(conn, &table, sid, schema_version)?))
        })
    }

    /// One table's full entry at `version` (`None` = head), without per-file
    /// statistics. This is the layout a writable attachment diffs.
    pub(crate) fn table_layout(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<TableEntry>> {
        self.with_conn(|conn| {
            let sid = resolve_sid(conn, version, self.label())?;
            let Some(table) = find_table(conn, key, sid)? else {
                return Ok(None);
            };
            let schema_version = snapshot_state(conn, sid)?
                .map(|state| state.schema_version)
                .unwrap_or(0);
            Ok(Some(load_table_layout(conn, &table, sid, schema_version)?))
        })
    }

    /// One table's non-file facts at `version` (`None` = head).
    pub(crate) fn table_stub(&self, key: &str, version: Option<u64>) -> Result<Option<TableEntry>> {
        self.with_conn(|conn| {
            let sid = resolve_sid(conn, version, self.label())?;
            let Some(table) = find_table(conn, key, sid)? else {
                return Ok(None);
            };
            let schema_version = snapshot_state(conn, sid)?
                .map(|state| state.schema_version)
                .unwrap_or(0);
            Ok(Some(load_table_stub(conn, &table, sid, schema_version)?))
        })
    }

    /// The commit-delta manifest for `version`, reconstructed from the rows:
    /// files that begin/end at the snapshot, inlined rows that begin/end
    /// there, and DuckLake's own snapshot-change row. There is no stored
    /// manifest blob.
    pub(crate) fn commit_manifest(&self, version: u64) -> Result<Option<Vec<u8>>> {
        self.with_conn(|conn| {
            let Some(manifest) = manifest_from_rows(conn, version)? else {
                return Ok(None);
            };
            serde_json::to_vec(&manifest)
                .map(Some)
                .map_err(|e| LakeError::Internal {
                    message: format!("serializing a derived commit manifest: {e}"),
                })
        })
    }

    /// Whether a snapshot row exists for this pinkdb version.
    pub(crate) fn snapshot_exists(&self, version: u64) -> Result<bool> {
        let sid = snapshot_id(version)?;
        self.with_conn(|conn| {
            let found: Option<i64> = conn
                .query_row(
                    "SELECT 1 FROM ducklake_snapshot WHERE snapshot_id = ?1",
                    params![sid],
                    |row| row.get(0),
                )
                .map_err(|e| sql_error("checking a snapshot", e))?;
            Ok(found.is_some())
        })
    }

    /// Whether the catalog has any snapshot (the create existence test).
    #[allow(dead_code)] // part of the store surface; `create` checks the head transactionally
    pub(crate) fn initialized(&self) -> Result<bool> {
        self.with_conn(|conn| {
            let found: Option<i64> = conn
                .query_row("SELECT 1 FROM ducklake_snapshot LIMIT 1", [], |row| {
                    row.get(0)
                })
                .map_err(|e| sql_error("checking the catalog", e))?;
            Ok(found.is_some())
        })
    }

    /// The latest pinkdb version.
    pub(crate) fn head_version(&self) -> Result<Option<u64>> {
        self.with_conn(head_version_conn)
    }
    /// Execute one SQL statement on the catalog database (tests mutate rows
    /// directly to simulate corruption or a lost cursor update).
    pub(crate) fn execute_sql(&self, sql: &str) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(sql, params![])
                .map(|_| ())
                .map_err(|e| sql_error("executing catalog SQL", e))
        })
    }

    /// Expire snapshots and everything only they could see (DuckLake's
    /// `DeleteSnapshots`). See [`MetadataStore::expire_snapshots`].
    pub(crate) fn expire_snapshots(&self, versions: &[u64]) -> Result<()> {
        self.with_conn(|conn| {
            expire_conn(conn, versions)?;
            conn.maintenance()
        })
    }

    pub(crate) fn scheduled_files(&self) -> Result<Vec<(i64, String)>> {
        self.with_conn(scheduled_files_conn)
    }

    /// Every live schema name at `version` (`None` = head).
    pub(crate) fn schemas(&self, version: Option<u64>) -> Result<Vec<String>> {
        self.with_conn(|conn| {
            let sid = resolve_sid(conn, version, self.label())?;
            schemas_conn(conn, sid)
        })
    }

    /// The `field_id`s a data file of `key` must record, in column order.
    ///
    /// A DuckLake reader matches a nested column's children by id, so the
    /// writer needs the ids before the catalog commit — see
    /// [`planned_field_ids`].
    pub(crate) fn field_ids(
        &self,
        key: &str,
        columns: &[(String, pinkdb::DataType)],
    ) -> Result<Vec<crate::format::FieldIds>> {
        self.with_conn(|conn| planned_field_ids(conn, key, columns, self.label()))
    }

    pub(crate) fn forget_scheduled(&self, ids: &[i64]) -> Result<()> {
        self.with_conn(|conn| {
            for id in ids {
                conn.execute(
                    "DELETE FROM ducklake_files_scheduled_for_deletion WHERE data_file_id = ?1",
                    params![*id],
                )
                .map_err(|e| sql_error("removing a scheduled file", e))?;
            }
            Ok(())
        })
    }

    pub(crate) fn referenced_file_paths(&self) -> Result<Vec<String>> {
        self.with_conn(referenced_paths_conn)
    }

    pub(crate) fn replication_state(&self) -> Result<Option<crate::replication::ReplicationState>> {
        self.with_conn(replication_state_conn)
    }

    pub(crate) fn store_replication_state(
        &self,
        state: &crate::replication::ReplicationState,
    ) -> Result<()> {
        self.with_conn(|conn| store_replication_state_conn(conn, state))
    }

    pub(crate) fn file_key(&self, rel: &str) -> Result<Option<Vec<u8>>> {
        self.with_conn(|conn| {
            let name = path_basename(rel);
            conn.query_row(
                "SELECT encryption_key FROM ducklake_data_file WHERE path = ?1
                 UNION ALL
                 SELECT encryption_key FROM ducklake_delete_file WHERE path = ?1
                 LIMIT 1",
                params![name],
                |row| row.get::<_, Option<Vec<u8>>>(0),
            )
            .map_err(|e| sql_error("reading a file's encryption key", e))
            .map(Option::flatten)
        })
    }

    pub(crate) fn file_keys(&self) -> Result<Vec<Vec<u8>>> {
        self.with_conn(|conn| {
            let stmt = conn
                .prepare(
                    "SELECT encryption_key FROM ducklake_data_file
                     WHERE encryption_key IS NOT NULL
                     UNION ALL
                     SELECT encryption_key FROM ducklake_delete_file
                     WHERE encryption_key IS NOT NULL",
                )
                .map_err(|e| sql_error("listing file encryption keys", e))?;
            let rows = stmt
                .query_map([], |row| row.get::<_, Vec<u8>>(0))
                .map_err(|e| sql_error("listing file encryption keys", e))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(|e| sql_error("listing file encryption keys", e))?);
            }
            Ok(out)
        })
    }

    pub(crate) fn set_table_format(&self, key: &str, format: &str) -> Result<()> {
        self.with_conn(|conn| {
            let sid = head_snapshot_id(conn)?.ok_or_else(|| LakeError::NotALake {
                path: self.label().to_path_buf(),
            })?;
            let table = find_table(conn, key, sid)?.ok_or_else(|| LakeError::Corrupt {
                path: self.label().to_path_buf(),
                message: format!("table \"{key}\" is not in the catalog"),
            })?;
            update_or_insert(
                conn,
                "writing a table format",
                "UPDATE pinkdb_table SET format = ?2 WHERE table_id = ?1",
                params![table.table_id, format],
                "INSERT INTO pinkdb_table
                 (table_id, catalog_key, table_id_hex, format, inlined_limit,
                  table_schema_version)
                 VALUES (?1, ?2, NULL, ?3, NULL, 0)",
                params![table.table_id, key, format],
            )
        })
    }

    pub(crate) fn is_encrypted(&self) -> Result<bool> {
        self.with_conn(|conn| {
            let value: Option<String> = conn
                .query_row(
                    "SELECT value FROM ducklake_metadata WHERE key = 'encrypted'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|e| sql_error("reading the encryption flag", e))?;
            Ok(value.as_deref() == Some("true"))
        })
    }

    pub(crate) fn set_encrypted(&self, encrypted: bool) -> Result<()> {
        self.with_conn(|conn| {
            update_or_insert(
                conn,
                "writing the encryption flag",
                "UPDATE ducklake_metadata SET value = ?1 WHERE key = 'encrypted'",
                params![if encrypted { "true" } else { "false" }],
                "INSERT INTO ducklake_metadata (key, value) VALUES ('encrypted', ?1)",
                params![if encrypted { "true" } else { "false" }],
            )
        })
    }

    /// The version numbers with a snapshot row, ascending.
    pub(crate) fn snapshot_versions(&self) -> Result<Vec<u64>> {
        self.with_conn(|conn| {
            let stmt = conn
                .prepare("SELECT snapshot_id FROM ducklake_snapshot ORDER BY snapshot_id")
                .map_err(|e| sql_error("listing snapshots", e))?;
            let rows = stmt
                .query_map([], |row| row.get::<_, i64>(0))
                .map_err(|e| sql_error("listing snapshots", e))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(pink_version(
                    row.map_err(|e| sql_error("listing snapshots", e))?,
                ));
            }
            Ok(out)
        })
    }

    // ---------- opaque side objects ----------

    pub(crate) fn put_object(&self, rel: &str, bytes: &[u8], replace: bool) -> Result<bool> {
        self.with_conn(|conn| {
            if replace {
                update_or_insert(
                    conn,
                    "replacing a catalog object",
                    "UPDATE pinkdb_object SET bytes = ?2 WHERE path = ?1",
                    params![rel, bytes],
                    "INSERT INTO pinkdb_object (path, bytes) VALUES (?1, ?2)",
                    params![rel, bytes],
                )?;
                return Ok(true);
            }
            insert_if_absent(
                conn,
                "creating a catalog object",
                "SELECT 1 FROM pinkdb_object WHERE path = ?1",
                params![rel],
                "INSERT INTO pinkdb_object (path, bytes) VALUES (?1, ?2)",
                params![rel, bytes],
            )
        })
    }

    pub(crate) fn get_object(&self, rel: &str) -> Result<Option<Vec<u8>>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT bytes FROM pinkdb_object WHERE path = ?1",
                params![rel],
                |row| row.get(0),
            )
            .map_err(|e| sql_error("reading a catalog object", e))
        })
    }

    /// The engine catalog image and the revision it belongs to (wave 96b).
    pub(crate) fn engine_catalog(&self) -> Result<Option<(u64, Vec<u8>)>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT revision, encoded FROM pinkdb_engine_catalog WHERE id = 1",
                [],
                |row| {
                    let revision: i64 = row.get(0)?;
                    Ok((pink_version(revision), row.get(1)?))
                },
            )
            .map_err(|e| sql_error("reading the engine catalog image", e))
        })
    }

    /// Publish the engine catalog image on its own (the first image of a
    /// catalog, or a resync). A commit that changes both data and metadata
    /// must instead pass the image through [`CatalogChange::engine_catalog`],
    /// which publishes both in one transaction.
    pub(crate) fn put_engine_catalog(&self, revision: u64, image: &[u8]) -> Result<()> {
        self.with_conn(|conn| write_engine_catalog(conn, revision, image))
    }

    pub(crate) fn delete_object(&self, rel: &str) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute("DELETE FROM pinkdb_object WHERE path = ?1", params![rel])
                .map_err(|e| sql_error("deleting a catalog object", e))?;
            Ok(())
        })
    }

    pub(crate) fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        self.with_conn(|conn| {
            let stmt = conn
                .prepare("SELECT path FROM pinkdb_object WHERE path LIKE ?1 ORDER BY path")
                .map_err(|e| sql_error("listing catalog objects", e))?;
            let pattern = format!("{}%", prefix.trim_end_matches('/'));
            let rows = stmt
                .query_map(params![pattern], |row| row.get::<_, String>(0))
                .map_err(|e| sql_error("listing catalog objects", e))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(|e| sql_error("listing catalog objects", e))?);
            }
            Ok(out)
        })
    }

    // ---------- create ----------

    /// Create the catalog: `ducklake_metadata`, snapshot 0 and the pink side
    /// rows, atomically. Used by the SQL create path before any object is
    /// published.
    pub(crate) fn create_catalog(
        &self,
        data_location: &str,
        created_by: &str,
        catalog: &Catalog,
    ) -> Result<()> {
        let tx = self.begin_transaction()?;
        let result = self.with_conn(|conn| {
            if head_version_conn(conn)?.is_some() {
                return Err(LakeError::AlreadyALake {
                    path: self.label().to_path_buf(),
                });
            }
            create_in_conn(conn, data_location, created_by, catalog)
        });
        match (result, tx) {
            (Ok(()), Some(tx)) => self.finish_transaction(&tx, true),
            (Ok(()), None) => Ok(()),
            (Err(e), Some(tx)) => {
                let _ = self.finish_transaction(&tx, false);
                Err(e)
            }
            (Err(e), None) => Err(e),
        }
    }

    /// Commit one publication inside the active transaction.
    /// [`MetadataStore::commit`] wraps it in the store's own transaction.
    pub(crate) fn commit_in_transaction(
        &self,
        base_version: u64,
        change: CatalogChange,
    ) -> Result<u64> {
        self.with_conn(|conn| commit_in_conn(conn, base_version, &change, self.label()))
    }

    // ---------- metadata facts ----------

    pub(crate) fn data_location(&self) -> Result<Option<String>> {
        self.with_conn(|conn| {
            let value: Option<String> = conn
                .query_row(
                    "SELECT value FROM ducklake_metadata WHERE key = 'data_path'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|e| sql_error("reading the data location", e))?;
            // DuckLake records a trailing `/` (it composes paths by
            // concatenation); pinkdb's `Location` does not, and `Lake::path`
            // must equal the data root the caller passed to create.
            Ok(value.map(|location| trim_trailing_slash(&location)))
        })
    }

    pub(crate) fn set_data_location(&self, location: &str) -> Result<()> {
        self.with_conn(|conn| {
            update_or_insert(
                conn,
                "writing the data location",
                "UPDATE ducklake_metadata SET value = ?1 WHERE key = 'data_path'",
                params![location],
                "INSERT INTO ducklake_metadata (key, value) VALUES ('data_path', ?1)",
                params![location],
            )
        })
    }

    pub(crate) fn lake_id(&self) -> Result<Option<String>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT value FROM pinkdb_lake WHERE key = 'lake_id'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| sql_error("reading the lake identity", e))
        })
    }

    pub(crate) fn set_lake_id(&self, lake_id: &str) -> Result<()> {
        self.with_conn(|conn| {
            update_or_insert(
                conn,
                "writing the lake identity",
                "UPDATE pinkdb_lake SET value = ?1 WHERE key = 'lake_id'",
                params![lake_id],
                "INSERT INTO pinkdb_lake (key, value) VALUES ('lake_id', ?1)",
                params![lake_id],
            )
        })
    }

    pub(crate) fn moved_to(&self) -> Result<Option<String>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT value FROM pinkdb_lake WHERE key = 'moved_to'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| sql_error("reading the catalog's move marker", e))
        })
    }

    pub(crate) fn set_moved_to(&self, target: &str) -> Result<()> {
        self.with_conn(|conn| {
            update_or_insert(
                conn,
                "writing the catalog's move marker",
                "UPDATE pinkdb_lake SET value = ?1 WHERE key = 'moved_to'",
                params![target],
                "INSERT INTO pinkdb_lake (key, value) VALUES ('moved_to', ?1)",
                params![target],
            )
        })
    }
}

impl MetadataStore for SqlMetadataStore {
    fn data_location(&self) -> Result<Option<String>> {
        SqlMetadataStore::data_location(self)
    }

    fn lake_id(&self) -> Result<Option<String>> {
        SqlMetadataStore::lake_id(self)
    }

    fn set_lake_id(&self, lake_id: &str) -> Result<()> {
        SqlMetadataStore::set_lake_id(self, lake_id)
    }

    fn set_data_location(&self, location: &str) -> Result<()> {
        SqlMetadataStore::set_data_location(self, location)
    }

    fn moved_to(&self) -> Result<Option<String>> {
        SqlMetadataStore::moved_to(self)
    }

    fn set_moved_to(&self, target: &str) -> Result<()> {
        SqlMetadataStore::set_moved_to(self, target)
    }

    fn head_version(&self) -> Result<Option<u64>> {
        SqlMetadataStore::head_version(self)
    }

    fn engine_catalog(&self) -> Result<Option<(u64, Vec<u8>)>> {
        SqlMetadataStore::engine_catalog(self)
    }

    fn put_engine_catalog(&self, revision: u64, image: &[u8]) -> Result<()> {
        SqlMetadataStore::put_engine_catalog(self, revision, image)
    }

    fn snapshot_versions(&self) -> Result<Vec<u64>> {
        SqlMetadataStore::snapshot_versions(self)
    }

    fn snapshot_exists(&self, version: u64) -> Result<bool> {
        SqlMetadataStore::snapshot_exists(self, version)
    }

    fn head(&self) -> Result<Option<Catalog>> {
        // Full: a lake-wide caller must see every table. Commit planning uses
        // `head_shell` explicitly.
        SqlMetadataStore::load(self, None)
    }

    fn head_shell(&self) -> Result<Option<Catalog>> {
        SqlMetadataStore::head_catalog(self)
    }

    fn table(&self, key: &str, version: Option<u64>) -> Result<Option<TableEntry>> {
        SqlMetadataStore::table(self, key, version)
    }

    fn table_layout(&self, key: &str, version: Option<u64>) -> Result<Option<TableEntry>> {
        SqlMetadataStore::table_layout(self, key, version)
    }

    fn table_stub(&self, key: &str, version: Option<u64>) -> Result<Option<TableEntry>> {
        SqlMetadataStore::table_stub(self, key, version)
    }

    fn load(&self, version: Option<u64>) -> Result<Option<Catalog>> {
        SqlMetadataStore::load(self, version)
    }

    fn expire_snapshots(&self, versions: &[u64]) -> Result<()> {
        SqlMetadataStore::expire_snapshots(self, versions)
    }

    fn scheduled_files(&self) -> Result<Vec<(i64, String)>> {
        SqlMetadataStore::scheduled_files(self)
    }

    fn schemas(&self, version: Option<u64>) -> Result<Vec<String>> {
        SqlMetadataStore::schemas(self, version)
    }

    fn field_ids(
        &self,
        key: &str,
        columns: &[(String, pinkdb::DataType)],
    ) -> Result<Vec<crate::format::FieldIds>> {
        SqlMetadataStore::field_ids(self, key, columns)
    }

    fn forget_scheduled(&self, ids: &[i64]) -> Result<()> {
        SqlMetadataStore::forget_scheduled(self, ids)
    }

    fn referenced_file_paths(&self) -> Result<Vec<String>> {
        SqlMetadataStore::referenced_file_paths(self)
    }

    fn replication_state(&self) -> Result<Option<crate::replication::ReplicationState>> {
        SqlMetadataStore::replication_state(self)
    }

    fn store_replication_state(&self, state: &crate::replication::ReplicationState) -> Result<()> {
        SqlMetadataStore::store_replication_state(self, state)
    }

    fn file_key(&self, rel: &str) -> Result<Option<Vec<u8>>> {
        SqlMetadataStore::file_key(self, rel)
    }

    fn file_keys(&self) -> Result<Vec<Vec<u8>>> {
        SqlMetadataStore::file_keys(self)
    }

    fn is_encrypted(&self) -> Result<bool> {
        SqlMetadataStore::is_encrypted(self)
    }

    fn set_encrypted(&self, encrypted: bool) -> Result<()> {
        SqlMetadataStore::set_encrypted(self, encrypted)
    }

    fn commit(&self, base_version: u64, change: CatalogChange) -> Result<u64> {
        let tx = self.begin_transaction()?;
        let result = self.commit_in_transaction(base_version, change);
        match (result, tx) {
            (Ok(version), Some(tx)) => {
                self.finish_transaction(&tx, true)?;
                Ok(version)
            }
            (Ok(version), None) => Ok(version),
            (Err(e), Some(tx)) => {
                let _ = self.finish_transaction(&tx, false);
                Err(e)
            }
            (Err(e), None) => Err(e),
        }
    }

    fn commit_manifest(&self, version: u64) -> Result<Option<Vec<u8>>> {
        SqlMetadataStore::commit_manifest(self, version)
    }

    fn put_object(&self, rel: &str, bytes: &[u8]) -> Result<bool> {
        SqlMetadataStore::put_object(self, rel, bytes, false)
    }

    fn replace_object(&self, rel: &str, bytes: &[u8]) -> Result<bool> {
        SqlMetadataStore::put_object(self, rel, bytes, true)
    }

    fn get_object(&self, rel: &str) -> Result<Option<Vec<u8>>> {
        SqlMetadataStore::get_object(self, rel)
    }

    fn delete_object(&self, rel: &str) -> Result<()> {
        SqlMetadataStore::delete_object(self, rel)
    }

    fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        SqlMetadataStore::list_objects(self, prefix)
    }

    fn create(&self, data_location: &str, created_by: &str, catalog: &Catalog) -> Result<()> {
        self.create_catalog(data_location, created_by, catalog)
    }
}

// ============================ connection helpers ============================

fn has_ducklake_tables(conn: &dyn SqlConnection, _dialect: Dialect, label: &Path) -> Result<bool> {
    if !table_exists(conn, "ducklake_metadata")? {
        return Ok(false);
    }
    // An unknown DuckLake version is refused, never guessed at.
    let version: Option<String> = conn
        .query_row(
            "SELECT value FROM ducklake_metadata WHERE key = 'version'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| sql_error("reading the DuckLake version", e))?;
    match version.as_deref() {
        None | Some(DUCKLAKE_VERSION) => Ok(true),
        Some(other) => Err(LakeError::Unsupported {
            path: label.to_path_buf(),
            found: other
                .split('.')
                .next()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        }),
    }
}

fn head_snapshot_id(conn: &dyn SqlConnection) -> Result<Option<i64>> {
    query_one(
        conn,
        "SELECT MAX(snapshot_id) FROM ducklake_snapshot",
        [],
        |row| row.get::<_, Option<i64>>(0),
    )
    .map_err(|e| sql_error("reading the catalog head", e))
}

fn head_version_conn(conn: &dyn SqlConnection) -> Result<Option<u64>> {
    Ok(head_snapshot_id(conn)?.map(pink_version))
}

fn snapshot_state(conn: &dyn SqlConnection, sid: i64) -> Result<Option<SnapshotState>> {
    conn.query_row(
        "SELECT schema_version, next_catalog_id, next_file_id
         FROM ducklake_snapshot WHERE snapshot_id = ?1",
        params![sid],
        |row| {
            Ok(SnapshotState {
                schema_version: i64_to_u64(row.get::<_, i64>(0)?),
                next_catalog_id: row.get(1)?,
                next_file_id: row.get(2)?,
            })
        },
    )
    .map_err(|e| sql_error("reading a snapshot", e))
}

// ============================== maintenance ================================

/// DuckLake's relative file path: schema path + table path + file path.
/// Pink stores the schema path as `data/`, a DuckDB-created catalog as
/// `main/`; prepending nothing keeps both interoperable with DuckLake's own
/// scheduled-deletion queue.
fn ducklake_relative(schema_path: &str, table_path: &str, file_path: &str) -> String {
    let mut out = String::new();
    for part in [schema_path, table_path, file_path] {
        if part.is_empty() {
            continue;
        }
        if !out.is_empty() && !out.ends_with('/') {
            out.push('/');
        }
        out.push_str(part.trim_start_matches('/'));
    }
    out
}

/// The snapshot ids that still exist. DuckLake's liveness rule is "no
/// remaining snapshot in `[begin_snapshot, end_snapshot)`"; computing it in
/// Rust avoids a correlated subquery pinkdb cannot plan.
fn remaining_snapshots(conn: &dyn SqlConnection) -> Result<Vec<i64>> {
    let stmt = conn
        .prepare("SELECT snapshot_id FROM ducklake_snapshot ORDER BY snapshot_id")
        .map_err(|e| sql_error("listing remaining snapshots", e))?;
    let rows = stmt
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(|e| sql_error("listing remaining snapshots", e))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| sql_error("listing remaining snapshots", e))?);
    }
    Ok(out)
}

fn expire_conn(conn: &dyn SqlConnection, versions: &[u64]) -> Result<()> {
    let now = ducklake_time(&crate::time::iso8601_utc(std::time::SystemTime::now()));
    for version in versions {
        let sid = snapshot_id(*version)?;
        // Snapshot rows first: the liveness predicates below are evaluated
        // against the *remaining* history (DuckLake's rule).
        conn.execute(
            "DELETE FROM ducklake_snapshot_changes WHERE snapshot_id = ?1",
            params![sid],
        )
        .map_err(|e| sql_error("expiring a snapshot change row", e))?;
        conn.execute(
            "DELETE FROM ducklake_snapshot WHERE snapshot_id = ?1",
            params![sid],
        )
        .map_err(|e| sql_error("expiring a snapshot", e))?;
        conn.execute(
            "DELETE FROM pinkdb_snapshot WHERE snapshot_id = ?1",
            params![sid],
        )
        .map_err(|e| sql_error("expiring a snapshot side row", e))?;
        conn.execute(
            "DELETE FROM pinkdb_table_change WHERE snapshot_id = ?1",
            params![sid],
        )
        .map_err(|e| sql_error("expiring a table change row", e))?;
    }

    // A data file is dead when it has ended and no remaining snapshot lies
    // in `[begin_snapshot, end_snapshot)`. Its bytes go to DuckLake's
    // scheduled-deletion queue; the metadata rows go now.
    let remaining = remaining_snapshots(conn)?;
    let ended: Vec<(i64, String)> = {
        let stmt = conn
            .prepare(
                "SELECT f.data_file_id, f.path, s.path, t.path,
                        f.begin_snapshot, f.end_snapshot
                 FROM ducklake_data_file f
                 JOIN ducklake_table t ON t.table_id = f.table_id
                 JOIN ducklake_schema s ON s.schema_id = t.schema_id
                 WHERE f.end_snapshot IS NOT NULL",
            )
            .map_err(|e| sql_error("listing expired data files", e))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    ducklake_relative(
                        &row.get::<_, String>(2)?,
                        &row.get::<_, String>(3)?,
                        &row.get::<_, String>(1)?,
                    ),
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .map_err(|e| sql_error("listing expired data files", e))?;
        let mut out = Vec::new();
        for row in rows {
            let (id, path, begin, end) =
                row.map_err(|e| sql_error("listing expired data files", e))?;
            if remaining.iter().any(|sid| *sid >= begin && *sid < end) {
                continue;
            }
            out.push((id, path));
        }
        out
    };
    for (id, path) in ended {
        schedule_file(conn, id, &path, &now)?;
        for table in [
            "ducklake_file_column_stats",
            "ducklake_file_variant_stats",
            "ducklake_file_partition_value",
            "pinkdb_data_file",
        ] {
            conn.execute(
                &format!("DELETE FROM {table} WHERE data_file_id = ?1"),
                params![id],
            )
            .map_err(|e| sql_error("removing an expired file's rows", e))?;
        }
        // A delete file whose data file is gone can never be applied again.
        let deletes: Vec<(i64, String)> = {
            let stmt = conn
                .prepare(
                    "SELECT d.delete_file_id, d.path, s.path, t.path
                     FROM ducklake_delete_file d
                     JOIN ducklake_table t ON t.table_id = d.table_id
                     JOIN ducklake_schema s ON s.schema_id = t.schema_id
                     WHERE d.data_file_id = ?1",
                )
                .map_err(|e| sql_error("listing a removed file's delete files", e))?;
            let rows = stmt
                .query_map(params![id], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        ducklake_relative(
                            &row.get::<_, String>(2)?,
                            &row.get::<_, String>(3)?,
                            &row.get::<_, String>(1)?,
                        ),
                    ))
                })
                .map_err(|e| sql_error("listing a removed file's delete files", e))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(|e| sql_error("listing a removed file's delete files", e))?);
            }
            out
        };
        for (delete_id, path) in deletes {
            schedule_file(conn, delete_id, &path, &now)?;
            conn.execute(
                "DELETE FROM ducklake_delete_file WHERE delete_file_id = ?1",
                params![delete_id],
            )
            .map_err(|e| sql_error("removing an expired delete file", e))?;
        }
        conn.execute(
            "DELETE FROM ducklake_data_file WHERE data_file_id = ?1",
            params![id],
        )
        .map_err(|e| sql_error("removing an expired data file", e))?;
    }

    // Delete files with their own end marker follow the same liveness rule.
    let ended_deletes: Vec<(i64, String)> = {
        let stmt = conn
            .prepare(
                "SELECT d.delete_file_id, d.path, s.path, t.path,
                        d.begin_snapshot, d.end_snapshot
                 FROM ducklake_delete_file d
                 JOIN ducklake_table t ON t.table_id = d.table_id
                 JOIN ducklake_schema s ON s.schema_id = t.schema_id
                 WHERE d.end_snapshot IS NOT NULL",
            )
            .map_err(|e| sql_error("listing expired delete files", e))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    ducklake_relative(
                        &row.get::<_, String>(2)?,
                        &row.get::<_, String>(3)?,
                        &row.get::<_, String>(1)?,
                    ),
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .map_err(|e| sql_error("listing expired delete files", e))?;
        let mut out = Vec::new();
        for row in rows {
            let (id, path, begin, end) =
                row.map_err(|e| sql_error("listing expired delete files", e))?;
            if remaining.iter().any(|sid| *sid >= begin && *sid < end) {
                continue;
            }
            out.push((id, path));
        }
        out
    };
    for (id, path) in ended_deletes {
        schedule_file(conn, id, &path, &now)?;
        conn.execute(
            "DELETE FROM ducklake_delete_file WHERE delete_file_id = ?1",
            params![id],
        )
        .map_err(|e| sql_error("removing an expired delete file", e))?;
    }
    Ok(())
}

fn schedule_file(conn: &dyn SqlConnection, id: i64, path: &str, now: &str) -> Result<()> {
    insert_if_absent(
        conn,
        "scheduling an expired file",
        "SELECT 1 FROM ducklake_files_scheduled_for_deletion WHERE data_file_id = ?1",
        params![id],
        "INSERT INTO ducklake_files_scheduled_for_deletion
         (data_file_id, path, path_is_relative, schedule_start) VALUES (?1, ?2, 1, ?3)",
        params![id, path, now],
    )?;
    Ok(())
}

fn schemas_conn(conn: &dyn SqlConnection, sid: i64) -> Result<Vec<String>> {
    let stmt = conn
        .prepare(
            "SELECT schema_name FROM ducklake_schema
             WHERE begin_snapshot <= ?1 AND (end_snapshot IS NULL OR end_snapshot > ?1)
             ORDER BY schema_id",
        )
        .map_err(|e| sql_error("listing schemas", e))?;
    let rows = stmt
        .query_map(params![sid], |row| row.get::<_, String>(0))
        .map_err(|e| sql_error("listing schemas", e))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| sql_error("listing schemas", e))?);
    }
    Ok(out)
}

fn scheduled_files_conn(conn: &dyn SqlConnection) -> Result<Vec<(i64, String)>> {
    let stmt = conn
        .prepare(
            "SELECT data_file_id, path FROM ducklake_files_scheduled_for_deletion
             ORDER BY data_file_id",
        )
        .map_err(|e| sql_error("listing scheduled files", e))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|e| sql_error("listing scheduled files", e))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| sql_error("listing scheduled files", e))?);
    }
    Ok(out)
}

/// Both spellings of every file a catalog row names (see
/// [`MetadataStore::referenced_file_paths`]).
fn referenced_paths_conn(conn: &dyn SqlConnection) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for (table, id_column) in [
        ("ducklake_data_file", "data_file_id"),
        ("ducklake_delete_file", "delete_file_id"),
    ] {
        let sql = format!(
            "SELECT s.path, t.path, f.path FROM {table} f
             JOIN ducklake_table t ON t.table_id = f.table_id
             JOIN ducklake_schema s ON s.schema_id = t.schema_id
             WHERE f.{id_column} IS NOT NULL"
        );
        let stmt = conn
            .prepare(&sql)
            .map_err(|e| sql_error("listing referenced files", e))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(ducklake_relative(
                    &row.get::<_, String>(0)?,
                    &row.get::<_, String>(1)?,
                    &row.get::<_, String>(2)?,
                ))
            })
            .map_err(|e| sql_error("listing referenced files", e))?;
        for row in rows {
            let path = row.map_err(|e| sql_error("listing referenced files", e))?;
            if path.starts_with("data/") {
                out.push(path);
            } else {
                out.push(format!("data/{path}"));
                out.push(path);
            }
        }
    }
    Ok(out)
}

// ============================== replication ================================

fn replication_state_conn(conn: &dyn SqlConnection) -> Result<Option<ReplicationState>> {
    let Some((format, next_id)): Option<(i64, i64)> = conn
        .query_row(
            "SELECT replication_format, next_id FROM pinkdb_replication_state WHERE id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|e| sql_error("reading the replication state", e))?
    else {
        return Ok(None);
    };
    let stmt = conn
        .prepare(
            "SELECT job_id, source_kind, source_location, source_database, status, interval_ms,
                    \"include\", \"exclude\", watermark_columns, created, last_run, last_ok,
                    last_error
             FROM pinkdb_replication_job ORDER BY job_id",
        )
        .map_err(|e| sql_error("listing replication jobs", e))?;
    let rows = stmt
        .query_map([], |row| {
            let kind: Option<String> = row.get(1)?;
            let source = match kind.as_deref() {
                Some("store") => Source::Store {
                    database: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                },
                _ => Source::Lake {
                    location: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                },
            };
            Ok(Job {
                id: i64_to_u64(row.get(0)?),
                source,
                status: parse_status(row.get::<_, Option<String>>(4)?.as_deref()),
                interval_ms: i64_to_u64(row.get(5)?),
                include: row.get::<_, Option<String>>(6)?.unwrap_or_default(),
                exclude: row.get::<_, Option<String>>(7)?.unwrap_or_default(),
                watermark_columns: row.get::<_, Option<String>>(8)?.unwrap_or_default(),
                created: row.get::<_, Option<String>>(9)?.unwrap_or_default(),
                last_run: row.get(10)?,
                last_ok: row.get(11)?,
                last_error: row.get(12)?,
                tables: Vec::new(),
            })
        })
        .map_err(|e| sql_error("listing replication jobs", e))?;
    let mut jobs = Vec::new();
    for row in rows {
        jobs.push(row.map_err(|e| sql_error("listing replication jobs", e))?);
    }
    for job in &mut jobs {
        let stmt = conn
            .prepare(
                "SELECT source, dest, strategy, watermark_column, pk_columns,
                        source_table_id, last_watermark, last_source_version,
                        last_full_sync_version, rows_dest, last_error, synced
                 FROM pinkdb_replication_table WHERE job_id = ?1 ORDER BY source",
            )
            .map_err(|e| sql_error("listing replication tables", e))?;
        let rows = stmt
            .query_map(params![i64_from_u64(job.id)?], |row| {
                Ok(TableState {
                    source: row.get(0)?,
                    dest: row.get(1)?,
                    strategy: parse_strategy(row.get::<_, Option<String>>(2)?.as_deref()),
                    watermark_column: row.get(3)?,
                    pk_columns: row.get(4)?,
                    source_table_id: row.get(5)?,
                    last_watermark: row.get(6)?,
                    last_source_version: row.get::<_, Option<i64>>(7)?.map(i64_to_u64),
                    last_full_sync_version: row.get::<_, Option<i64>>(8)?.map(i64_to_u64),
                    rows_dest: row.get::<_, Option<i64>>(9)?.map(i64_to_u64),
                    last_error: row.get(10)?,
                    synced: row.get(11)?,
                })
            })
            .map_err(|e| sql_error("listing replication tables", e))?;
        for row in rows {
            job.tables
                .push(row.map_err(|e| sql_error("listing replication tables", e))?);
        }
    }
    Ok(Some(ReplicationState {
        replication_format: u32::try_from(format).unwrap_or(0),
        next_id: i64_to_u64(next_id),
        jobs,
    }))
}

fn store_replication_state_conn(conn: &dyn SqlConnection, state: &ReplicationState) -> Result<()> {
    // The state is small (one row per job and per replicated table); a
    // replace is simpler than a diff and still one transaction.
    conn.execute("DELETE FROM pinkdb_replication_table", params![])
        .map_err(|e| sql_error("clearing replication tables", e))?;
    conn.execute("DELETE FROM pinkdb_replication_job", params![])
        .map_err(|e| sql_error("clearing replication jobs", e))?;
    conn.execute("DELETE FROM pinkdb_replication_state", params![])
        .map_err(|e| sql_error("clearing the replication state", e))?;
    conn.execute(
        "INSERT INTO pinkdb_replication_state (id, replication_format, next_id)
         VALUES (1, ?1, ?2)",
        params![
            i64::from(state.replication_format),
            i64_from_u64(state.next_id)?
        ],
    )
    .map_err(|e| sql_error("writing the replication state", e))?;
    for job in &state.jobs {
        let (kind, location, database) = match &job.source {
            Source::Lake { location } => ("lake", Some(location.clone()), None),
            Source::Store { database } => ("store", None, Some(database.clone())),
        };
        conn.execute(
            "INSERT INTO pinkdb_replication_job
             (job_id, source_kind, source_location, source_database, status, interval_ms,
              \"include\", \"exclude\", watermark_columns, created, last_run, last_ok,
              last_error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                i64_from_u64(job.id)?,
                kind,
                location,
                database,
                job.status.as_str(),
                i64_from_u64(job.interval_ms)?,
                job.include,
                job.exclude,
                job.watermark_columns,
                job.created,
                job.last_run,
                job.last_ok,
                job.last_error,
            ],
        )
        .map_err(|e| sql_error("writing a replication job", e))?;
        for table in &job.tables {
            let dest_table_id: Option<i64> = conn
                .query_row(
                    "SELECT t.table_id FROM ducklake_table t
                     JOIN ducklake_schema s ON s.schema_id = t.schema_id
                     WHERE s.schema_name || '.' || t.table_name = ?1
                       AND t.end_snapshot IS NULL
                     ORDER BY t.table_id DESC LIMIT 1",
                    params![table.dest],
                    |row| row.get(0),
                )
                .map_err(|e| sql_error("looking up a replicated table's id", e))?;
            conn.execute(
                "INSERT INTO pinkdb_replication_table
                 (job_id, source, dest, dest_table_id, strategy, watermark_column,
                  pk_columns, source_table_id, last_watermark, last_source_version,
                  last_full_sync_version, rows_dest, last_error, synced)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    i64_from_u64(job.id)?,
                    table.source,
                    table.dest,
                    dest_table_id,
                    table.strategy.as_str(),
                    table.watermark_column,
                    table.pk_columns,
                    table.source_table_id,
                    table.last_watermark,
                    table.last_source_version.map(i64_from_u64).transpose()?,
                    table.last_full_sync_version.map(i64_from_u64).transpose()?,
                    table.rows_dest.map(i64_from_u64).transpose()?,
                    table.last_error,
                    table.synced,
                ],
            )
            .map_err(|e| sql_error("writing a replication table", e))?;
        }
    }
    Ok(())
}

fn parse_status(text: Option<&str>) -> JobStatus {
    match text {
        Some("running") => JobStatus::Running,
        Some("paused") => JobStatus::Paused,
        Some("error") => JobStatus::Error,
        _ => JobStatus::Created,
    }
}

fn parse_strategy(text: Option<&str>) -> Strategy {
    match text {
        Some("watermark") => Strategy::Watermark,
        Some("snapshot") => Strategy::Snapshot,
        Some("merge") => Strategy::Merge,
        _ => Strategy::Full,
    }
}

// ================================ create ===================================

fn create_in_conn(
    conn: &dyn SqlConnection,
    data_location: &str,
    created_by: &str,
    catalog: &Catalog,
) -> Result<()> {
    let data_path = with_trailing_slash(data_location);
    for (key, value) in [
        ("version", DUCKLAKE_VERSION.to_string()),
        ("created_by", created_by.to_string()),
        ("data_path", data_path),
        ("encrypted", "false".to_string()),
        ("data_inlining_row_limit", "0".to_string()),
    ] {
        conn.execute(
            "INSERT INTO ducklake_metadata (key, value) VALUES (?1, ?2)",
            params![key, value],
        )
        .map_err(|e| sql_error("writing ducklake_metadata", e))?;
    }
    let schema_uuid = crate::fresh_id();
    conn.execute(
        "INSERT INTO ducklake_schema
         (schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path,
          path_is_relative, parent_schema_id)
         VALUES (0, ?1, 0, NULL, 'main', 'data/', 1, NULL)",
        params![schema_uuid],
    )
    .map_err(|e| sql_error("creating the main schema", e))?;
    conn.execute(
        "INSERT INTO ducklake_snapshot
         (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
         VALUES (0, ?1, 0, 1, 0)",
        params![ducklake_time(&catalog.updated)],
    )
    .map_err(|e| sql_error("creating snapshot 0", e))?;
    conn.execute(
        "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES (0, ?1)",
        params!["created_schema:\"main\""],
    )
    .map_err(|e| sql_error("creating snapshot 0's change row", e))?;
    conn.execute(
        "INSERT INTO pinkdb_snapshot
         (snapshot_id, updated, format_version, lake_id)
         VALUES (0, ?1, ?2, ?3)",
        params![
            catalog.updated,
            i64::from(catalog.format_version),
            catalog.lake_id,
        ],
    )
    .map_err(|e| sql_error("creating snapshot 0's side row", e))?;
    if let Some(lake_id) = &catalog.lake_id {
        conn.execute(
            "INSERT INTO pinkdb_lake (key, value) VALUES ('lake_id', ?1)",
            params![lake_id],
        )
        .map_err(|e| sql_error("writing the lake identity", e))?;
    }
    Ok(())
}

// ================================= load ====================================

fn load_conn(conn: &dyn SqlConnection, version: Option<u64>) -> Result<Option<Catalog>> {
    load_conn_inner(conn, version, true)
}

fn load_conn_inner(
    conn: &dyn SqlConnection,
    version: Option<u64>,
    with_tables: bool,
) -> Result<Option<Catalog>> {
    let head = head_snapshot_id(conn)?;
    let Some(head) = head else {
        return Ok(None);
    };
    let sid = match version {
        None => head,
        Some(version) => {
            let sid = snapshot_id(version)?;
            if sid > head {
                return Ok(None);
            }
            sid
        }
    };
    let Some(state) = snapshot_state(conn, sid)? else {
        return Ok(None);
    };
    let (updated, format_version, mut lake_id): (Option<String>, Option<i64>, Option<String>) =
        conn.query_row(
            "SELECT updated, format_version, lake_id
             FROM pinkdb_snapshot WHERE snapshot_id = ?1",
            params![sid],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|e| sql_error("reading a snapshot's side row", e))?
        .unwrap_or((None, None, None));
    if lake_id.is_none() {
        // A DuckLake-created catalog has no pinkdb_snapshot side row. The
        // lake identity is adopted into `pinkdb_lake` at first open, which is
        // therefore the fallback for every snapshot of such a catalog.
        lake_id = conn
            .query_row(
                "SELECT value FROM pinkdb_lake WHERE key = 'lake_id'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| sql_error("reading the lake identity", e))?;
    }
    let (author, commit_message): (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT author, commit_message FROM ducklake_snapshot_changes WHERE snapshot_id = ?1",
            params![sid],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|e| sql_error("reading a snapshot's change row", e))?
        .unwrap_or((None, None));
    let changes = None;
    let mut tables = std::collections::BTreeMap::new();
    if with_tables {
        for table in visible_tables(conn, sid)? {
            let entry = load_table(conn, &table, sid, state.schema_version)?;
            tables.insert(table.table_key.clone(), entry);
        }
    }
    Ok(Some(Catalog {
        format_version: format_version
            .map(|v| u32::try_from(v).unwrap_or(crate::FORMAT_VERSION))
            .unwrap_or(crate::FORMAT_VERSION),
        updated: updated.unwrap_or_else(|| ducklake_time("")),
        version: pink_version(sid),
        lake_id,
        tables,
        author,
        commit_message,
        changes,
        schema_version: state.schema_version,
    }))
}

fn resolve_sid(conn: &dyn SqlConnection, version: Option<u64>, label: &Path) -> Result<i64> {
    let Some(head) = head_snapshot_id(conn)? else {
        return Err(LakeError::NotALake {
            path: label.to_path_buf(),
        });
    };
    let sid = match version {
        None => head,
        Some(version) => {
            let sid = snapshot_id(version)?;
            if sid > head {
                return Err(LakeError::Conflict {
                    path: label.to_path_buf(),
                    message: format!("version {version} is newer than the catalog head"),
                });
            }
            sid
        }
    };
    Ok(sid)
}

fn find_table(conn: &dyn SqlConnection, key: &str, sid: i64) -> Result<Option<TableRow>> {
    conn.query_row(
        "SELECT t.table_id, s.schema_name, s.path, t.table_name, t.path, t.table_uuid
         FROM ducklake_table t
         JOIN ducklake_schema s ON s.schema_id = t.schema_id
         WHERE s.schema_name || '.' || t.table_name = ?1
           AND t.begin_snapshot <= ?2
           AND (t.end_snapshot IS NULL OR t.end_snapshot > ?2)
           AND s.begin_snapshot <= ?2
           AND (s.end_snapshot IS NULL OR s.end_snapshot > ?2)
         ORDER BY t.table_id DESC LIMIT 1",
        params![key, sid],
        |row| {
            Ok(TableRow {
                table_id: row.get(0)?,
                schema_path: row.get(2)?,
                table_path: row.get(4)?,
                table_uuid: row.get(5)?,
                table_key: format!("{}.{}", row.get::<_, String>(1)?, row.get::<_, String>(3)?),
            })
        },
    )
    .map_err(|e| sql_error("reading a table", e))
}

fn find_table_by_id(conn: &dyn SqlConnection, table_id: i64) -> Result<Option<TableRow>> {
    conn.query_row(
        "SELECT t.table_id, s.schema_name, s.path, t.table_name, t.path, t.table_uuid
         FROM ducklake_table t
         JOIN ducklake_schema s ON s.schema_id = t.schema_id
         WHERE t.table_id = ?1
         ORDER BY t.begin_snapshot DESC, s.begin_snapshot DESC LIMIT 1",
        params![table_id],
        |row| {
            Ok(TableRow {
                table_id: row.get(0)?,
                schema_path: row.get(2)?,
                table_path: row.get(4)?,
                table_uuid: row.get(5)?,
                table_key: format!("{}.{}", row.get::<_, String>(1)?, row.get::<_, String>(3)?),
            })
        },
    )
    .map_err(|e| sql_error("reading a table by id", e))
}

#[derive(Debug, Clone, Copy, Default)]
struct ChangeRow {
    kind: i64,
    partition_changed: bool,
    inlining_changed: bool,
    inlined_limit: Option<u64>,
}

/// Reconstruct one commit's audit manifest from the DuckLake rows. Files,
/// delete files and inlined row versions are selected by their begin/end
/// snapshot; the audit-only facts (kind, partition/inlining changes) come from
/// `pinkdb_table_change`, and author/message/changes_made from DuckLake's own
/// snapshot-change row.
fn manifest_from_rows(conn: &dyn SqlConnection, version: u64) -> Result<Option<CommitManifest>> {
    if version <= 1 {
        return Ok(None);
    }
    let sid = snapshot_id(version)?;
    let Some(state) = snapshot_state(conn, sid)? else {
        return Ok(None);
    };
    let (updated, format_version): (Option<String>, Option<i64>) = conn
        .query_row(
            "SELECT updated, format_version FROM pinkdb_snapshot WHERE snapshot_id = ?1",
            params![sid],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|e| sql_error("reading a snapshot's side row", e))?
        .unwrap_or((None, None));
    let updated = match updated {
        Some(updated) => updated,
        None => conn
            .query_row(
                "SELECT snapshot_time FROM ducklake_snapshot WHERE snapshot_id = ?1",
                params![sid],
                |row| row.get::<_, Option<String>>(0),
            )
            .map_err(|e| sql_error("reading a snapshot's time", e))?
            .flatten()
            .unwrap_or_default(),
    };
    let mut ids: Vec<i64> = Vec::new();
    {
        let stmt = conn
            .prepare(
                "SELECT table_id FROM ducklake_table
                 WHERE begin_snapshot = ?1 OR end_snapshot = ?1
                 UNION SELECT table_id FROM ducklake_data_file
                 WHERE begin_snapshot = ?1 OR end_snapshot = ?1
                 UNION SELECT table_id FROM ducklake_delete_file
                 WHERE begin_snapshot = ?1 OR end_snapshot = ?1",
            )
            .map_err(|e| sql_error("listing a commit's tables", e))?;
        let rows = stmt
            .query_map(params![sid], |row| row.get::<_, i64>(0))
            .map_err(|e| sql_error("listing a commit's tables", e))?;
        for row in rows {
            ids.push(row.map_err(|e| sql_error("listing a commit's tables", e))?);
        }
    }
    ids.sort_unstable();
    ids.dedup();
    // An inline-only change touches no DuckLake file or table row, so the
    // union above cannot see it. Ask every table that has typed inlined
    // storage whether this snapshot begins or ends one of its rows.
    for (table_id,) in {
        let stmt = conn
            .prepare("SELECT DISTINCT table_id FROM ducklake_inlined_data_tables")
            .map_err(|e| sql_error("listing inlined tables", e))?;
        let rows = stmt
            .query_map([], |row| Ok((row.get::<_, i64>(0)?,)))
            .map_err(|e| sql_error("listing inlined tables", e))?;
        rows.into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| sql_error("listing inlined tables", e))?
    } {
        if ids.binary_search(&table_id).is_ok() {
            continue;
        }
        let (begin, end) = inlined_change_counts(conn, table_id, sid)?;
        if begin > 0 || end > 0 {
            ids.push(table_id);
        }
    }
    ids.sort_unstable();
    ids.dedup();
    let mut tables = std::collections::BTreeMap::new();
    for table_id in ids {
        let Some(table) = find_table_by_id(conn, table_id)? else {
            continue;
        };
        let change = conn
            .query_row(
                "SELECT kind, partition_changed, inlining_changed, inlined_limit
                 FROM pinkdb_table_change WHERE snapshot_id = ?1 AND table_id = ?2",
                params![sid, table_id],
                |row| {
                    Ok(ChangeRow {
                        kind: row.get(0)?,
                        partition_changed: row.get::<_, i64>(1)? != 0,
                        inlining_changed: row.get::<_, i64>(2)? != 0,
                        inlined_limit: row.get::<_, Option<i64>>(3)?.map(i64_to_u64),
                    })
                },
            )
            .map_err(|e| sql_error("reading a table change", e))?
            .unwrap_or_default();
        let added_files = load_data_files_added(conn, &table, sid)?;
        let mut compacted_files = Vec::new();
        if change.kind != 1 {
            let stmt = conn
                .prepare(
                    "SELECT path FROM ducklake_data_file
                     WHERE table_id = ?1 AND end_snapshot = ?2 ORDER BY data_file_id",
                )
                .map_err(|e| sql_error("reading ended data files", e))?;
            let rows = stmt
                .query_map(params![table_id, sid], |row| row.get::<_, String>(0))
                .map_err(|e| sql_error("reading ended data files", e))?;
            for row in rows {
                let path = row.map_err(|e| sql_error("reading ended data files", e))?;
                compacted_files.push(compose_path(&table.schema_path, &table.table_path, &path));
            }
        }
        let added_deletes = load_delete_files_added(conn, &table, sid)?;
        let mut removed_delete_files = Vec::new();
        {
            let stmt = conn
                .prepare(
                    "SELECT path FROM ducklake_delete_file
                     WHERE table_id = ?1 AND end_snapshot = ?2 ORDER BY delete_file_id",
                )
                .map_err(|e| sql_error("reading ended delete files", e))?;
            let rows = stmt
                .query_map(params![table_id, sid], |row| row.get::<_, String>(0))
                .map_err(|e| sql_error("reading ended delete files", e))?;
            for row in rows {
                let path = row.map_err(|e| sql_error("reading ended delete files", e))?;
                removed_delete_files.push(compose_path(
                    &table.schema_path,
                    &table.table_path,
                    &path,
                ));
            }
        }
        let (inlined_begin, inlined_end) = inlined_change_counts(conn, table_id, sid)?;
        let mut inlined_flushed = 0u64;
        if change.kind == 3 {
            // A flush commit ends live rows because it materialized them into
            // a data file; DuckLake records the same end, but not why.
            inlined_flushed = inlined_end;
        }
        let added_records: u64 = added_files.iter().filter_map(|f| f.rows()).sum();
        let added_delete_rows: u64 = added_deletes.iter().map(|d| d.count).sum();
        let (rows_added, rows_deleted, rows_inlined, inlined_ended, inlined_flushed) =
            match change.kind {
                1 => (added_records + inlined_begin, 0, 0, 0, 0),
                2 => (0, 0, 0, 0, 0),
                3 => (0, 0, 0, 0, inlined_flushed),
                _ => (
                    added_records + inlined_begin,
                    added_delete_rows + inlined_end,
                    inlined_begin,
                    inlined_end,
                    0,
                ),
            };
        let partition_by = if change.partition_changed {
            load_partition_keys(conn, table_id, sid)?
        } else {
            None
        };
        tables.insert(
            table.table_key,
            TableDelta {
                runtime: TableRuntime::default(),
                data_files: added_files,
                delete_files: added_deletes,
                rows_added,
                rows_deleted,
                replaced: change.kind == 1,
                partition_changed: change.partition_changed,
                partition_by,
                compacted_files,
                removed_delete_files,
                rows_inlined,
                inlined_ended,
                inlined_flushed,
                inlined_limit: if change.inlining_changed {
                    change.inlined_limit
                } else {
                    None
                },
                inlining_changed: change.inlining_changed,
            },
        );
    }
    let _ = state;
    Ok(Some(CommitManifest {
        format_version: format_version
            .map(|v| u32::try_from(v).unwrap_or(crate::FORMAT_VERSION))
            .unwrap_or(crate::FORMAT_VERSION),
        version,
        base_version: version - 1,
        updated,
        tables,
    }))
}

fn load_data_files_added(
    conn: &dyn SqlConnection,
    table: &TableRow,
    sid: i64,
) -> Result<Vec<FileRef>> {
    let stmt = conn
        .prepare(
            "SELECT f.data_file_id, f.path, f.record_count,
                    p.schema_version, p.footer_offset, p.footer_len, p.footer_crc32,
                    f.row_id_start
             FROM ducklake_data_file f
             LEFT JOIN pinkdb_data_file p ON p.data_file_id = f.data_file_id
             WHERE f.table_id = ?1 AND f.begin_snapshot = ?2
             ORDER BY f.data_file_id",
        )
        .map_err(|e| sql_error("reading added data files", e))?;
    let rows = stmt
        .query_map(params![table.table_id, sid], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<i64>>(7)?,
            ))
        })
        .map_err(|e| sql_error("reading added data files", e))?;
    let mut out = Vec::new();
    for row in rows {
        let (file_id, path, records, schema_version, footer_offset, footer_len, footer_crc, start) =
            row.map_err(|e| sql_error("reading added data files", e))?;
        let composed = compose_path(&table.schema_path, &table.table_path, &path);
        let stats = load_file_stats(
            conn,
            file_id,
            table.table_id,
            &table.table_key,
            &composed,
            records,
        )?;
        let stats_sealed = stats.is_some();
        out.push(FileRef::Entry {
            path: composed,
            rows: records.map(i64_to_u64).unwrap_or(0),
            schema_version: schema_version.map(i64_to_u64),
            stats,
            partition: load_partition_values(conn, file_id, table, sid)?,
            row_id_start: start.map(i64_to_u64),
            footer_offset: footer_offset.map(i64_to_u64),
            footer_len: footer_len.map(i64_to_u64),
            footer_crc32c: footer_crc.map(|v| v as u32),
            stats_sealed,
        });
    }
    Ok(out)
}

fn load_delete_files_added(
    conn: &dyn SqlConnection,
    table: &TableRow,
    sid: i64,
) -> Result<Vec<DeleteRef>> {
    let stmt = conn
        .prepare(
            "SELECT d.data_file_id, d.path, d.delete_count, d.format
             FROM ducklake_delete_file d
             WHERE d.table_id = ?1 AND d.begin_snapshot = ?2
             ORDER BY d.delete_file_id",
        )
        .map_err(|e| sql_error("reading added delete files", e))?;
    let rows = stmt
        .query_map(params![table.table_id, sid], |row| {
            Ok((
                row.get::<_, Option<i64>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(|e| sql_error("reading added delete files", e))?;
    let mut out = Vec::new();
    for row in rows {
        let (data_file_id, path, count, format) =
            row.map_err(|e| sql_error("reading added delete files", e))?;
        let file = match data_file_id {
            Some(id) => conn
                .query_row(
                    "SELECT path FROM ducklake_data_file WHERE data_file_id = ?1",
                    params![id],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|e| sql_error("reading a delete file's data file", e))?
                .map(|p| compose_path(&table.schema_path, &table.table_path, &p))
                .unwrap_or_default(),
            None => String::new(),
        };
        out.push(DeleteRef {
            path: compose_path(&table.schema_path, &table.table_path, &path),
            file,
            count: count.map(i64_to_u64).unwrap_or(0),
            format: format.unwrap_or_else(|| "pink".to_string()),
        });
    }
    Ok(out)
}

fn visible_tables(conn: &dyn SqlConnection, sid: i64) -> Result<Vec<TableRow>> {
    let stmt = conn
        .prepare(
            "SELECT t.table_id, t.schema_id, s.schema_name, s.path, t.begin_snapshot,
                    t.table_name, t.path, t.table_uuid
             FROM ducklake_table t
             JOIN ducklake_schema s ON s.schema_id = t.schema_id
             WHERE t.begin_snapshot <= ?1
               AND (t.end_snapshot IS NULL OR t.end_snapshot > ?1)
               AND s.begin_snapshot <= ?1
               AND (s.end_snapshot IS NULL OR s.end_snapshot > ?1)
             ORDER BY t.table_id",
        )
        .map_err(|e| sql_error("reading tables", e))?;
    let rows = stmt
        .query_map(params![sid], |row| {
            Ok(TableRow {
                table_id: row.get(0)?,
                schema_path: row.get(3)?,
                table_path: row.get(6)?,
                table_uuid: row.get(7)?,
                table_key: format!("{}.{}", row.get::<_, String>(2)?, row.get::<_, String>(5)?),
            })
        })
        .map_err(|e| sql_error("reading tables", e))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| sql_error("reading tables", e))?);
    }
    Ok(out)
}

fn load_table(
    conn: &dyn SqlConnection,
    table: &TableRow,
    sid: i64,
    lake_schema_version: u64,
) -> Result<TableEntry> {
    load_table_inner(conn, table, sid, lake_schema_version, true, false)
}

/// A table's rows, files, deletes and schema facts without any per-file
/// statistics blocks. The writable-attachment planning path needs the layout
/// (which logical row is in which file) but never the statistics, so this
/// avoids one stats query per file. It also reads only *live* inlined rows:
/// the diff addresses live row ids, and the historical (ended) rows would
/// otherwise make every commit O(commits) instead of O(live rows).
fn load_table_layout(
    conn: &dyn SqlConnection,
    table: &TableRow,
    sid: i64,
    lake_schema_version: u64,
) -> Result<TableEntry> {
    load_table_inner(conn, table, sid, lake_schema_version, false, true)
}

fn load_table_inner(
    conn: &dyn SqlConnection,
    table: &TableRow,
    sid: i64,
    lake_schema_version: u64,
    with_stats: bool,
    live_inlined_only: bool,
) -> Result<TableEntry> {
    // pinkdb-only table facts (absent for a DuckDB-created catalog).
    let side = load_table_side(conn, table.table_id)?;

    let columns = load_columns(conn, table.table_id, sid, lake_schema_version)?;
    let renames = load_renames(conn, table.table_id)?;
    let partition_by = load_partition_keys(conn, table.table_id, sid)?;
    let files = if with_stats {
        load_files(conn, table, sid, with_stats, partition_by.is_some())?
    } else {
        load_file_layouts(conn, table, sid)?
    };
    let deletes = load_deletes(conn, table, sid)?;
    let inlined = load_inlined(
        conn,
        table,
        sid,
        lake_schema_version,
        live_inlined_only,
        !live_inlined_only,
    )?;
    let data_rows: u64 = files.iter().filter_map(|f| f.rows()).sum();
    let deleted_rows: u64 = deletes.iter().map(|d| d.count).sum();
    let live_inlined = inlined.iter().filter(|r| r.live()).count() as u64;
    let rows = data_rows
        .saturating_sub(deleted_rows)
        .saturating_add(live_inlined);
    let (next_row_id, _file_size) = conn
        .query_row(
            "SELECT next_row_id, file_size_bytes FROM ducklake_table_stats WHERE table_id = ?1",
            params![table.table_id],
            |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?)),
        )
        .map_err(|e| sql_error("reading table statistics", e))?
        .unwrap_or((None, None));
    let format = match side.as_ref().and_then(|s| s.1.clone()) {
        Some(format) => Some(FileFormat::new(format)),
        None => match newest_file_format(conn, table, sid)? {
            Some(format) => Some(format),
            // An inlined-only table (or an empty DuckLake table) declares no
            // data-file format. Pink's model carries one per entry; there is
            // no file to mislabel, and the next pink commit chooses the
            // handle's write format.
            None => Some(FileFormat::new("pink")),
        },
    };
    let table_schema_version = side
        .as_ref()
        .and_then(|s| s.3)
        .map(i64_to_u64)
        .filter(|v| *v > 0)
        .unwrap_or_else(|| {
            query_one(
                conn,
                "SELECT MAX(schema_version) FROM ducklake_schema_versions
                 WHERE table_id = ?1 AND begin_snapshot <= ?2",
                params![table.table_id, sid],
                |row| row.get::<_, Option<i64>>(0),
            )
            .ok()
            .flatten()
            .map(i64_to_u64)
            .unwrap_or(0)
        });
    Ok(TableEntry {
        files,
        rows,
        table_id: side
            .as_ref()
            .and_then(|s| s.0.clone())
            .or_else(|| uuid_to_hex(&table.table_uuid)),
        format,
        stats: None,
        deletes,
        partition_by,
        schema_version: table_schema_version,
        columns: if columns.is_empty() {
            None
        } else {
            Some(columns)
        },
        renames,
        inlined,
        // `Some(0)` means "inlining disabled for this table"; only a NULL
        // side column means "use the handle default". Filtering 0 away here
        // silently re-enabled the default (the JSON layout kept it).
        inlined_limit: side.as_ref().and_then(|s| s.2).map(i64_to_u64),
        next_row_id: next_row_id.map(i64_to_u64).unwrap_or(0),
    })
}

type TableSide = (Option<String>, Option<String>, Option<i64>, Option<i64>);

fn load_table_side(conn: &dyn SqlConnection, table_id: i64) -> Result<Option<TableSide>> {
    conn.query_row(
        "SELECT table_id_hex, format, inlined_limit, table_schema_version
         FROM pinkdb_table WHERE table_id = ?1",
        params![table_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )
    .map_err(|e| sql_error("reading a table's side row", e))
}

/// The non-file facts a commit needs. It reads the table's side row, columns,
/// renames, partition spec and statistics row, but never its data files,
/// delete files or inlined records. An append therefore costs O(columns), not
/// O(files in the table).
fn load_table_stub(
    conn: &dyn SqlConnection,
    table: &TableRow,
    sid: i64,
    lake_schema_version: u64,
) -> Result<TableEntry> {
    let side = load_table_side(conn, table.table_id)?;
    let columns = load_columns(conn, table.table_id, sid, lake_schema_version)?;
    let renames = load_renames(conn, table.table_id)?;
    let partition_by = load_partition_keys(conn, table.table_id, sid)?;
    let (next_row_id, _file_size, record_count) = conn
        .query_row(
            "SELECT next_row_id, file_size_bytes, record_count FROM ducklake_table_stats
             WHERE table_id = ?1",
            params![table.table_id],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .map_err(|e| sql_error("reading table statistics", e))?
        .unwrap_or((None, None, None));
    let deleted_rows: u64 = query_one(
        conn,
        "SELECT CAST(COALESCE(SUM(delete_count), 0) AS BIGINT) FROM ducklake_delete_file
         WHERE table_id = ?1 AND end_snapshot IS NULL",
        params![table.table_id],
        |row| row.get::<_, i64>(0),
    )
    .map_err(|e| sql_error("summing live deletes", e))? as u64;
    let live_inlined = count_live_inlined(conn, table.table_id, sid)?;
    let rows = record_count
        .map(i64_to_u64)
        .unwrap_or(0)
        .saturating_sub(deleted_rows)
        .saturating_add(live_inlined);
    let format = match side.as_ref().and_then(|s| s.1.clone()) {
        Some(format) => Some(FileFormat::new(format)),
        None => newest_file_format(conn, table, sid)?.or_else(|| Some(FileFormat::new("pink"))),
    };
    let table_schema_version = side
        .as_ref()
        .and_then(|s| s.3)
        .map(i64_to_u64)
        .filter(|v| *v > 0)
        .unwrap_or(0);
    Ok(TableEntry {
        files: Vec::new(),
        rows,
        table_id: side
            .as_ref()
            .and_then(|s| s.0.clone())
            .or_else(|| uuid_to_hex(&table.table_uuid)),
        format,
        stats: None,
        deletes: Vec::new(),
        partition_by,
        schema_version: table_schema_version,
        columns: if columns.is_empty() {
            None
        } else {
            Some(columns)
        },
        renames,
        inlined: Vec::new(),
        // `Some(0)` means "inlining disabled for this table"; only a NULL
        // side column means "use the handle default". Filtering 0 away here
        // silently re-enabled the default (the JSON layout kept it).
        inlined_limit: side.as_ref().and_then(|s| s.2).map(i64_to_u64),
        next_row_id: next_row_id.map(i64_to_u64).unwrap_or(0),
    })
}

fn load_columns(
    conn: &dyn SqlConnection,
    table_id: i64,
    sid: i64,
    _lake_schema_version: u64,
) -> Result<Vec<ColumnEntry>> {
    let stmt = conn
        .prepare(
            "SELECT c.column_id, c.column_name, c.column_type,
                    p.dtype, p.default_value, p.added, c.begin_snapshot
             FROM ducklake_column c
             LEFT JOIN pinkdb_column p
               ON p.table_id = c.table_id
              AND p.column_id = c.column_id
              AND p.begin_snapshot = c.begin_snapshot
             WHERE c.table_id = ?1
               AND c.parent_column IS NULL
               AND c.begin_snapshot <= ?2
               AND (c.end_snapshot IS NULL OR c.end_snapshot > ?2)
             ORDER BY c.column_order, c.column_id",
        )
        .map_err(|e| sql_error("reading columns", e))?;
    let schema_versions = load_schema_versions(conn, table_id)?;
    let rows = stmt
        .query_map(params![table_id, sid], |row| {
            let column_type: String = row.get(2)?;
            let side_dtype: Option<String> = row.get(3)?;
            let added: Option<i64> = row.get(5)?;
            let begin: i64 = row.get(6)?;
            // A DuckDB-created catalog has no pink side row. Its column's
            // table epoch is the lake schema version that snapshot carries,
            // which is exactly what `ducklake_schema_versions` records.
            let added = added.map(i64_to_u64).unwrap_or_else(|| {
                schema_versions
                    .iter()
                    .rev()
                    .find(|(begin_snapshot, _)| *begin_snapshot <= begin)
                    .map(|(_, version)| *version)
                    .unwrap_or(1)
            });
            let has_side_dtype = side_dtype.is_some();
            Ok((
                ColumnEntry {
                    name: row.get(1)?,
                    dtype: side_dtype.unwrap_or_else(|| pink_type_name(&column_type)),
                    default: row.get(4)?,
                    added,
                },
                column_type,
                has_side_dtype,
            ))
        })
        .map_err(|e| sql_error("reading columns", e))?;
    let mut out = Vec::new();
    for row in rows {
        let (entry, column_type, has_side_dtype) =
            row.map_err(|e| sql_error("reading columns", e))?;
        if !has_side_dtype && matches!(column_type.as_str(), "list" | "struct" | "map") {
            // Only a nested type reaches here without a side row: DuckDB wrote
            // the lake. Its `list` has no length, so it cannot be a pinkdb
            // ARRAY (wave 107a2).
            return Err(LakeError::NotRepresentable {
                message: format!(
                    "lake table column \"{}\" is a variable-length {}; pinkdb reads fixed-size ARRAY columns only (a LIST is wave 107a2)",
                    entry.name, column_type
                ),
            });
        }
        out.push(entry);
    }
    Ok(out)
}

/// Every column id this table has ever had, latest definition winning. One
/// query per table instead of one per column: on pinkdb every statement walks
/// the table's index list (a per-row scan where SQLite uses an index), so the
/// commit path must not issue O(columns) catalog queries per file.
fn column_ids(
    conn: &dyn SqlConnection,
    table_id: i64,
) -> Result<std::collections::HashMap<String, i64>> {
    let stmt = conn
        .prepare(
            "SELECT column_name, column_id FROM ducklake_column
             WHERE table_id = ?1 AND parent_column IS NULL ORDER BY begin_snapshot",
        )
        .map_err(|e| sql_error("reading column ids", e))?;
    let rows = stmt
        .query_map(params![table_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(|e| sql_error("reading column ids", e))?;
    let mut out = std::collections::HashMap::new();
    for row in rows {
        let (name, id) = row.map_err(|e| sql_error("reading column ids", e))?;
        out.insert(name, id);
    }
    Ok(out)
}

fn load_renames(conn: &dyn SqlConnection, table_id: i64) -> Result<Vec<RenameEntry>> {
    let stmt = conn
        .prepare(
            "SELECT version, from_name, to_name FROM pinkdb_rename
             WHERE table_id = ?1 ORDER BY version, from_name",
        )
        .map_err(|e| sql_error("reading renames", e))?;
    let rows = stmt
        .query_map(params![table_id], |row| {
            Ok(RenameEntry {
                version: i64_to_u64(row.get(0)?),
                from: row.get(1)?,
                to: row.get(2)?,
            })
        })
        .map_err(|e| sql_error("reading renames", e))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| sql_error("reading renames", e))?);
    }
    Ok(out)
}

fn load_partition_keys(
    conn: &dyn SqlConnection,
    table_id: i64,
    sid: i64,
) -> Result<Option<Vec<PartitionKey>>> {
    let partition_id: Option<i64> = conn
        .query_row(
            "SELECT partition_id FROM ducklake_partition_info
             WHERE table_id = ?1 AND begin_snapshot <= ?2
               AND (end_snapshot IS NULL OR end_snapshot > ?2)
             ORDER BY begin_snapshot DESC LIMIT 1",
            params![table_id, sid],
            |row| row.get(0),
        )
        .map_err(|e| sql_error("reading a partition specification", e))?;
    let Some(partition_id) = partition_id else {
        return Ok(None);
    };
    let stmt = conn
        .prepare(
            "SELECT c.column_name, p.\"transform\"
             FROM ducklake_partition_column p
             JOIN ducklake_column c
               ON c.table_id = p.table_id AND c.column_id = p.column_id
              AND c.begin_snapshot <= ?2 AND (c.end_snapshot IS NULL OR c.end_snapshot > ?2)
             WHERE p.table_id = ?1 AND p.partition_id = ?3
             ORDER BY p.partition_key_index",
        )
        .map_err(|e| sql_error("reading partition columns", e))?;
    let rows = stmt
        .query_map(params![table_id, sid, partition_id], |row| {
            Ok(PartitionKey {
                column: row.get(0)?,
                transform: row.get(1)?,
            })
        })
        .map_err(|e| sql_error("reading partition columns", e))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| sql_error("reading partition columns", e))?);
    }
    Ok(if out.is_empty() { None } else { Some(out) })
}

/// The visible data files with only the facts a layout needs: path and row
/// count. The writable-attachment diff maps logical rows to files and never
/// reads a footer or statistics block, so this avoids the side-table join.
fn load_file_layouts(conn: &dyn SqlConnection, table: &TableRow, sid: i64) -> Result<Vec<FileRef>> {
    let stmt = conn
        .prepare(
            "SELECT path, record_count FROM ducklake_data_file
             WHERE table_id = ?1
               AND begin_snapshot <= ?2
               AND (end_snapshot IS NULL OR end_snapshot > ?2)
             ORDER BY data_file_id",
        )
        .map_err(|e| sql_error("reading data-file layouts", e))?;
    let rows = stmt
        .query_map(params![table.table_id, sid], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
        })
        .map_err(|e| sql_error("reading data-file layouts", e))?;
    let mut out = Vec::new();
    for row in rows {
        let (path, record_count) = row.map_err(|e| sql_error("reading data-file layouts", e))?;
        out.push(FileRef::Entry {
            path: compose_path(&table.schema_path, &table.table_path, &path),
            rows: record_count.map(i64_to_u64).unwrap_or(0),
            schema_version: None,
            stats: None,
            partition: None,
            row_id_start: None,
            footer_offset: None,
            footer_len: None,
            footer_crc32c: None,
            stats_sealed: false,
        });
    }
    Ok(out)
}

fn load_schema_versions(conn: &dyn SqlConnection, table_id: i64) -> Result<Vec<(i64, u64)>> {
    let stmt = conn
        .prepare(
            "SELECT begin_snapshot, schema_version FROM ducklake_schema_versions
             WHERE table_id = ?1 ORDER BY begin_snapshot",
        )
        .map_err(|e| sql_error("reading schema versions", e))?;
    let rows = stmt
        .query_map(params![table_id], |row| {
            Ok((row.get::<_, i64>(0)?, i64_to_u64(row.get::<_, i64>(1)?)))
        })
        .map_err(|e| sql_error("reading schema versions", e))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| sql_error("reading schema versions", e))?);
    }
    Ok(out)
}

fn load_files(
    conn: &dyn SqlConnection,
    table: &TableRow,
    sid: i64,
    with_stats: bool,
    partitioned: bool,
) -> Result<Vec<FileRef>> {
    let stmt = conn
        .prepare(
            "SELECT f.data_file_id, f.path, f.file_format, f.record_count,
                    f.file_size_bytes, f.footer_size, f.row_id_start,
                    f.begin_snapshot, p.schema_version, p.footer_offset, p.footer_len,
                    p.footer_crc32
             FROM ducklake_data_file f
             LEFT JOIN pinkdb_data_file p ON p.data_file_id = f.data_file_id
             WHERE f.table_id = ?1
               AND f.begin_snapshot <= ?2
               AND (f.end_snapshot IS NULL OR f.end_snapshot > ?2)
             ORDER BY f.data_file_id",
        )
        .map_err(|e| sql_error("reading data files", e))?;
    let rows = stmt
        .query_map(params![table.table_id, sid], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, Option<i64>>(8)?,
                row.get::<_, Option<i64>>(9)?,
                row.get::<_, Option<i64>>(10)?,
                row.get::<_, Option<i64>>(11)?,
            ))
        })
        .map_err(|e| sql_error("reading data files", e))?;
    let schema_versions = load_schema_versions(conn, table.table_id)?;
    let mut stats_by_file = if with_stats {
        // The visible data-file ids are already in hand; filtering the
        // statistics rows by them in Rust avoids two joins in pinkdb's query
        // engine (the join dominated a 1,000-file attach).
        let visible: std::collections::HashMap<i64, i64> = rows
            .iter()
            .filter_map(|row| row.as_ref().ok().map(|row| (row.0, row.7)))
            .collect();
        Some(load_stats_blocks(conn, table.table_id, &visible)?)
    } else {
        None
    };
    let mut out = Vec::new();
    for row in rows {
        let (
            file_id,
            path,
            _format,
            record_count,
            _size,
            _footer,
            row_id_start,
            begin_snapshot,
            schema_version,
            footer_offset,
            footer_len,
            footer_crc,
        ) = row.map_err(|e| sql_error("reading data files", e))?;
        if record_count.is_some_and(|rows| rows < 0) {
            return Err(LakeError::Corrupt {
                path: PathBuf::from(&table.table_key),
                message: format!(
                    "data file \"{path}\" has a negative record_count ({})",
                    record_count.unwrap()
                ),
            });
        }
        let composed = compose_path(&table.schema_path, &table.table_path, &path);
        let stats = if with_stats {
            build_stats_block(
                &table.table_key,
                &composed,
                record_count,
                stats_by_file
                    .as_mut()
                    .and_then(|blocks| blocks.remove(&file_id))
                    .unwrap_or_default(),
            )?
        } else {
            None
        };
        let stats_sealed = stats.is_some();
        // A DuckDB-written catalog has no pinkdb_data_file side row, but
        // DuckLake records the schema epoch in ducklake_schema_versions. Use
        // it so a file written before a column was added maps forward instead
        // of being read under the table's latest schema.
        let schema_version = schema_version.map(i64_to_u64).or_else(|| {
            schema_versions
                .iter()
                .rev()
                .find(|(begin, _)| *begin <= begin_snapshot)
                .map(|(_, version)| *version)
        });
        out.push(FileRef::Entry {
            path: composed,
            rows: record_count.map(i64_to_u64).unwrap_or(0),
            schema_version,
            stats,
            partition: if partitioned {
                load_partition_values(conn, file_id, table, sid)?
            } else {
                None
            },
            row_id_start: row_id_start.map(i64_to_u64),
            footer_offset: footer_offset.map(i64_to_u64),
            footer_len: footer_len.map(i64_to_u64),
            footer_crc32c: footer_crc.map(|v| v as u32),
            stats_sealed,
        });
    }
    Ok(out)
}

type StatRow = (
    String,
    pinkdb::DataType,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Vec<stats::StatsPath>,
);

/// Every visible file's column statistics for one table, grouped by data-file
/// id. One query per table instead of one per file: attaching a 1,000-file
/// table is then O(rows), not O(files × columns) round trips through the catalog database.
fn load_stats_blocks(
    conn: &dyn SqlConnection,
    table_id: i64,
    visible: &std::collections::HashMap<i64, i64>,
) -> Result<std::collections::HashMap<i64, Vec<StatRow>>> {
    // A statistics row is keyed by column id, and a rename reuses the id for
    // the new name. The file's bytes still carry the name from the schema
    // epoch it was written under, so resolve each stat against the column
    // definition visible at that file's begin snapshot — not the column's
    // current name.
    let stmt = conn
        .prepare(
            // A column's DuckLake type is a coarse physical name (`list` for
            // an ARRAY, for instance); the side row carries the type the
            // engine actually wrote, exactly as `load_columns` reads it.
            "SELECT c.column_id, c.column_order, c.column_name, c.column_type,
                    c.begin_snapshot, c.end_snapshot, p.dtype
             FROM ducklake_column c
             LEFT JOIN pinkdb_column p
               ON p.table_id = c.table_id
              AND p.column_id = c.column_id
              AND p.begin_snapshot = c.begin_snapshot
             WHERE c.table_id = ?1
             ORDER BY c.begin_snapshot, c.column_id",
        )
        .map_err(|e| sql_error("reading statistics columns", e))?;
    let columns = stmt
        .query_map(params![table_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })
        .map_err(|e| sql_error("reading statistics columns", e))?;
    type ColumnHistory = Vec<(i64, Option<i64>, i64, String, pinkdb::DataType)>;
    let mut column_history: std::collections::HashMap<i64, ColumnHistory> =
        std::collections::HashMap::new();
    for row in columns {
        let (id, order, name, ty, begin, end, side_dtype) =
            row.map_err(|e| sql_error("reading statistics columns", e))?;
        let dtype = side_dtype
            .and_then(|text| crate::catalog::parse_dtype_name(&text))
            .unwrap_or_else(|| pink_dtype(&ty));
        column_history
            .entry(id)
            .or_default()
            .push((begin, end, order, name, dtype));
    }
    let stmt = conn
        .prepare(
            "SELECT data_file_id, column_id, null_count, min_value, max_value,
                    contains_nan
             FROM ducklake_file_column_stats WHERE table_id = ?1",
        )
        .map_err(|e| sql_error("reading file statistics", e))?;
    let rows = stmt
        .query_map(params![table_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<i64>>(5)?,
            ))
        })
        .map_err(|e| sql_error("reading file statistics", e))?;
    let mut staged: std::collections::HashMap<i64, Vec<(i64, StatRow)>> =
        std::collections::HashMap::new();
    for row in rows {
        let (file_id, column_id, nulls, min, max, contains_nan) =
            row.map_err(|e| sql_error("reading file statistics", e))?;
        let Some(file_begin) = visible.get(&file_id) else {
            continue;
        };
        let Some(history) = column_history.get(&column_id) else {
            continue;
        };
        let Some((order, name, ty)) = history
            .iter()
            .find(|(begin, end, _, _, _)| {
                begin <= file_begin && end.is_none_or(|end| end > *file_begin)
            })
            .map(|(_, _, order, name, ty)| (order, name, ty))
        else {
            continue;
        };
        staged.entry(file_id).or_default().push((
            *order,
            (name.clone(), *ty, nulls, min, max, contains_nan, Vec::new()),
        ));
    }
    // Path statistics live in their own table (DuckLake v1.1); each row
    // belongs to the column it names, under the name that column had when the
    // file was written (a rename reuses the id).
    let stmt = conn
        .prepare(
            "SELECT data_file_id, column_id, variant_path, shredded_type, null_count,
                    min_value, max_value
             FROM ducklake_file_variant_stats WHERE table_id = ?1
             ORDER BY data_file_id, variant_path",
        )
        .map_err(|e| sql_error("reading file variant statistics", e))?;
    let rows = stmt
        .query_map(params![table_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })
        .map_err(|e| sql_error("reading file variant statistics", e))?;
    for row in rows {
        let (file_id, column_id, path, dtype, nulls, min, max) =
            row.map_err(|e| sql_error("reading file variant statistics", e))?;
        let Some(file_begin) = visible.get(&file_id) else {
            continue;
        };
        let Some(history) = column_history.get(&column_id) else {
            continue;
        };
        let Some(order) = history
            .iter()
            .find(|(begin, end, _, _, _)| {
                begin <= file_begin && end.is_none_or(|end| end > *file_begin)
            })
            .map(|(_, _, order, _, _)| order)
        else {
            continue;
        };
        let Some(entries) = staged.get_mut(&file_id) else {
            continue;
        };
        let Some((_, stat)) = entries.iter_mut().find(|(at, _)| at == order) else {
            continue;
        };
        stat.6.push(stats::StatsPath {
            path,
            dtype,
            nulls: nulls.map(i64_to_u64).unwrap_or(0),
            min,
            max,
        });
    }
    Ok(staged
        .into_iter()
        .map(|(file_id, mut stats)| {
            stats.sort_by_key(|(order, _)| *order);
            (file_id, stats.into_iter().map(|(_, stat)| stat).collect())
        })
        .collect())
}

fn build_stats_block(
    table_key: &str,
    composed: &str,
    record_count: Option<i64>,
    rows: Vec<StatRow>,
) -> Result<Option<StatsBlock>> {
    if rows.is_empty() {
        return Ok(None);
    }
    let mut columns = Vec::with_capacity(rows.len());
    for (name, dtype, nulls, min, max, contains_nan, paths) in rows {
        let column_name = name.clone();
        let decode_stat = |value: Option<String>| -> Result<Option<String>> {
            let Some(value) = value else {
                return Ok(None);
            };
            ducklake_to_canonical(dtype, &value)
                .map(Some)
                .ok_or_else(|| LakeError::Corrupt {
                    path: PathBuf::from(composed),
                    message: format!(
                        "table \"{table_key}\": column \"{column_name}\" has a statistic \
                         \"{value}\" that does not decode as {dtype:?}"
                    ),
                })
        };
        columns.push(stats::StatsColumn {
            name,
            dtype: stats::dtype_name(dtype),
            nulls: nulls.map(i64_to_u64).unwrap_or(0),
            min: decode_stat(min)?,
            max: decode_stat(max)?,
            nan: (dtype == pinkdb::DataType::Double)
                .then_some(contains_nan.unwrap_or(0).max(0) as u64),
            paths,
        });
    }
    let mut block = StatsBlock {
        version: stats::STATS_VERSION,
        table: table_key.to_string(),
        file: composed.to_string(),
        rows: record_count.unwrap_or(0).max(0) as u64,
        columns,
        crc32: 0,
    };
    // The block is rebuilt from catalog rows in this process, and the caller
    // marks it `stats_sealed` (attach then skips `verify_crc`). Sealing it
    // would serialize every column of every file to JSON on each attach —
    // the dominant cost at 10,000 files — and protect against nothing the
    // catalog read did not already establish.
    let _ = &mut block;
    Ok(Some(block))
}

fn load_file_stats(
    conn: &dyn SqlConnection,
    file_id: i64,
    table_id: i64,
    table_key: &str,
    composed: &str,
    record_count: Option<i64>,
) -> Result<Option<StatsBlock>> {
    let stmt = conn
        .prepare(
            // `c.column_type` is the coarse DuckLake name; the side row
            // carries the type the engine wrote (see `load_stats_blocks`).
            "SELECT c.column_name, c.column_type, s.null_count, s.min_value, s.max_value,
                    s.contains_nan, p.dtype
             FROM ducklake_file_column_stats s
             JOIN ducklake_data_file f
               ON f.data_file_id = s.data_file_id
             JOIN ducklake_column c
               ON c.table_id = s.table_id AND c.column_id = s.column_id
              AND c.begin_snapshot <= f.begin_snapshot
              AND (c.end_snapshot IS NULL OR c.end_snapshot > f.begin_snapshot)
             LEFT JOIN pinkdb_column p
               ON p.table_id = c.table_id
              AND p.column_id = c.column_id
              AND p.begin_snapshot = c.begin_snapshot
             WHERE s.data_file_id = ?1 AND s.table_id = ?2
             ORDER BY c.column_order",
        )
        .map_err(|e| sql_error("reading file statistics", e))?;
    let mapped = stmt
        .query_map(params![file_id, table_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })
        .map_err(|e| sql_error("reading file statistics", e))?;
    let mut rows = Vec::new();
    for row in mapped {
        rows.push(row.map_err(|e| sql_error("reading file statistics", e))?);
    }
    let mut rows: Vec<StatRow> = rows
        .into_iter()
        .map(|(name, ty, nulls, min, max, nan, side_dtype)| {
            let dtype = side_dtype
                .and_then(|text| crate::catalog::parse_dtype_name(&text))
                .unwrap_or_else(|| pink_dtype(&ty));
            (name, dtype, nulls, min, max, nan, Vec::new())
        })
        .collect();
    // Path statistics live in DuckLake's own table, joined to the column name
    // the file was written under.
    let stmt = conn
        .prepare(
            "SELECT v.variant_path, v.shredded_type, v.null_count, v.min_value, v.max_value,
                    c.column_name
             FROM ducklake_file_variant_stats v
             JOIN ducklake_data_file f
               ON f.data_file_id = v.data_file_id
             JOIN ducklake_column c
               ON c.table_id = v.table_id AND c.column_id = v.column_id
              AND c.begin_snapshot <= f.begin_snapshot
              AND (c.end_snapshot IS NULL OR c.end_snapshot > f.begin_snapshot)
             WHERE v.data_file_id = ?1 AND v.table_id = ?2
             ORDER BY c.column_order, v.variant_path",
        )
        .map_err(|e| sql_error("reading file variant statistics", e))?;
    let mapped = stmt
        .query_map(params![file_id, table_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
            ))
        })
        .map_err(|e| sql_error("reading file variant statistics", e))?;
    for row in mapped {
        let (path, dtype, nulls, min, max, column) =
            row.map_err(|e| sql_error("reading file variant statistics", e))?;
        let Some(stat) = rows.iter_mut().find(|stat| stat.0 == column) else {
            continue;
        };
        stat.6.push(stats::StatsPath {
            path,
            dtype,
            nulls: nulls.map(i64_to_u64).unwrap_or(0),
            min,
            max,
        });
    }
    // Re-seal the reconstructed block exactly as the writer did, so `.pink`
    // attach can still verify it. The CRC covers the table key and path.
    build_stats_block(table_key, composed, record_count, rows)
}

fn load_partition_values(
    conn: &dyn SqlConnection,
    file_id: i64,
    table: &TableRow,
    sid: i64,
) -> Result<Option<std::collections::BTreeMap<String, Option<String>>>> {
    let keys = load_partition_keys(conn, table.table_id, sid)?;
    let Some(keys) = keys else {
        return Ok(None);
    };
    let stmt = conn
        .prepare(
            "SELECT partition_key_index, partition_value FROM ducklake_file_partition_value
             WHERE data_file_id = ?1 AND table_id = ?2 ORDER BY partition_key_index",
        )
        .map_err(|e| sql_error("reading file partition values", e))?;
    let rows = stmt
        .query_map(params![file_id, table.table_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|e| sql_error("reading file partition values", e))?;
    let mut out = std::collections::BTreeMap::new();
    for row in rows {
        let (index, value) = row.map_err(|e| sql_error("reading file partition values", e))?;
        if let Some(key) = keys.get(index as usize) {
            out.insert(key.key_text(), value);
        }
    }
    Ok(if out.is_empty() { None } else { Some(out) })
}

fn load_deletes(conn: &dyn SqlConnection, table: &TableRow, sid: i64) -> Result<Vec<DeleteRef>> {
    let stmt = conn
        .prepare(
            "SELECT d.delete_file_id, d.data_file_id, d.path, d.format, d.delete_count
             FROM ducklake_delete_file d
             WHERE d.table_id = ?1
               AND d.begin_snapshot <= ?2
               AND (d.end_snapshot IS NULL OR d.end_snapshot > ?2)
             ORDER BY d.delete_file_id",
        )
        .map_err(|e| sql_error("reading delete files", e))?;
    let rows = stmt
        .query_map(params![table.table_id, sid], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })
        .map_err(|e| sql_error("reading delete files", e))?;
    let mut out = Vec::new();
    for row in rows {
        let (_id, data_file_id, path, format, delete_count) =
            row.map_err(|e| sql_error("reading delete files", e))?;
        let data_path = match data_file_id {
            Some(id) => conn
                .query_row(
                    "SELECT path FROM ducklake_data_file WHERE data_file_id = ?1",
                    params![id],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|e| sql_error("reading a delete file's data file", e))?
                .map(|p| compose_path(&table.schema_path, &table.table_path, &p))
                .unwrap_or_default(),
            None => String::new(),
        };
        out.push(DeleteRef {
            path: compose_path(&table.schema_path, &table.table_path, &path),
            file: data_path,
            count: delete_count.map(i64_to_u64).unwrap_or(0),
            format: format.unwrap_or_else(|| "pink".to_string()),
        });
    }
    Ok(out)
}

fn load_inlined(
    conn: &dyn SqlConnection,
    table: &TableRow,
    sid: i64,
    lake_schema_version: u64,
    live_only: bool,
    with_values: bool,
) -> Result<Vec<InlinedRow>> {
    // The typed DuckLake tables are the only copy of the values. Read every
    // per-schema-version table registered for this table, converting each
    // column from DuckDB's physical representation to pink's canonical form.
    //
    // `with_values == false` is the commit-planning layout: it needs only the
    // live row ids (the writable diff addresses rows by id), so it skips the
    // value columns and every per-schema query. That keeps a commit O(live
    // rows), not O(live rows × columns).
    let types = if with_values {
        column_types(conn, table.table_id)?
    } else {
        BTreeMap::new()
    };
    let schema_versions = if with_values {
        inlined_schema_versions(conn, table.table_id)?
    } else {
        BTreeMap::new()
    };
    let flush_snapshots = if with_values {
        flush_snapshots(conn, table.table_id)?
    } else {
        std::collections::BTreeSet::new()
    };
    let mut out = Vec::new();
    for (table_name, suffix) in inlined_tables(conn, table.table_id)? {
        let value_columns: Vec<String> = if with_values {
            table_columns(conn, &table_name)?
                .into_iter()
                .filter(|name| !name.starts_with("_ducklake_"))
                .collect()
        } else {
            Vec::new()
        };
        if with_values && value_columns.is_empty() {
            continue;
        }
        let mut sql = String::from(
            "SELECT _ducklake_row_id, _ducklake_begin_snapshot, _ducklake_end_snapshot",
        );
        for name in &value_columns {
            sql.push_str(", ");
            sql.push_str(&quote_ident(name));
        }
        sql.push_str(" FROM ");
        sql.push_str(&quote_ident(&table_name));
        sql.push_str(" WHERE _ducklake_begin_snapshot <= ?1");
        if live_only {
            // The commit-planning layout needs only the live row ids; the
            // history is the reader's (CDC/time-travel) concern.
            sql.push_str(" AND (_ducklake_end_snapshot IS NULL OR _ducklake_end_snapshot > ?1)");
        }
        let stmt = conn
            .prepare(&sql)
            .map_err(|e| sql_error("reading an inlined data table", e))?;
        let rows = stmt
            .query_map(params![sid], |row| {
                let row_id: i64 = row.get(0)?;
                let begin: i64 = row.get(1)?;
                let end: Option<i64> = row.get(2)?;
                let mut values = Vec::with_capacity(value_columns.len());
                for (index, name) in value_columns.iter().enumerate() {
                    let raw: SqlValue = row.get(3 + index)?;
                    values.push(sql_to_canonical(
                        types.get(&name.to_ascii_lowercase()),
                        &raw,
                    ));
                }
                Ok((row_id, begin, end, values))
            })
            .map_err(|e| sql_error("reading an inlined data table", e))?;
        let schema_version = schema_versions
            .get(&suffix)
            .copied()
            .unwrap_or(lake_schema_version);
        for row in rows {
            let (row_id, begin, end, values) =
                row.map_err(|e| sql_error("reading an inlined data table", e))?;
            let end_sid = end.filter(|end| *end <= sid);
            let flushed = end_sid.is_some_and(|end| flush_snapshots.contains(&end));
            let end = end_sid.map(pink_version);
            out.push(InlinedRow {
                row_id: i64_to_u64(row_id),
                begin: pink_version(begin),
                end,
                flushed,
                schema_version,
                columns: value_columns.clone(),
                values,
            });
        }
    }
    Ok(out)
}

/// `(table_name, schema_version/suffix)` for every typed inlined table of one
/// table, oldest schema first.
fn inlined_tables(conn: &dyn SqlConnection, table_id: i64) -> Result<Vec<(String, i64)>> {
    let stmt = conn
        .prepare(
            "SELECT table_name, schema_version FROM ducklake_inlined_data_tables
             WHERE table_id = ?1 ORDER BY schema_version, table_name",
        )
        .map_err(|e| sql_error("reading inlined table names", e))?;
    let rows = stmt
        .query_map(params![table_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(|e| sql_error("reading inlined table names", e))?;
    rows.into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| sql_error("reading inlined table names", e))
}

/// pinkdb's table-schema epoch for each DuckLake inlined-table suffix. DuckDB
/// only knows the snapshot schema version (the suffix); the epoch is what
/// pink's rename/default mapping needs.
fn inlined_schema_versions(conn: &dyn SqlConnection, table_id: i64) -> Result<BTreeMap<i64, u64>> {
    let stmt = conn
        .prepare(
            "SELECT suffix, table_schema_version FROM pinkdb_inlined_schema
             WHERE table_id = ?1",
        )
        .map_err(|e| sql_error("reading inlined schema versions", e))?;
    let rows = stmt
        .query_map(params![table_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(|e| sql_error("reading inlined schema versions", e))?;
    let mut out = BTreeMap::new();
    for row in rows {
        let (suffix, version) = row.map_err(|e| sql_error("reading inlined schema versions", e))?;
        out.insert(suffix, i64_to_u64(version));
    }
    Ok(out)
}

/// Snapshots that ended inlined rows by flushing them into data files.
fn flush_snapshots(
    conn: &dyn SqlConnection,
    table_id: i64,
) -> Result<std::collections::BTreeSet<i64>> {
    let stmt = conn
        .prepare("SELECT snapshot_id FROM pinkdb_table_change WHERE table_id = ?1 AND kind = 3")
        .map_err(|e| sql_error("reading flush snapshots", e))?;
    let rows = stmt
        .query_map(params![table_id], |row| row.get::<_, i64>(0))
        .map_err(|e| sql_error("reading flush snapshots", e))?;
    rows.into_iter()
        .collect::<std::result::Result<std::collections::BTreeSet<_>, _>>()
        .map_err(|e| sql_error("reading flush snapshots", e))
}

/// Count the inlined row versions this table begins and ends at `sid`, across
/// every typed inlined table.
fn inlined_change_counts(conn: &dyn SqlConnection, table_id: i64, sid: i64) -> Result<(u64, u64)> {
    let mut begin = 0u64;
    let mut end = 0u64;
    for (table_name, _) in inlined_tables(conn, table_id)? {
        let sql = format!(
            "SELECT CAST(COALESCE(SUM(CASE WHEN _ducklake_begin_snapshot = ?1 THEN 1 ELSE 0 END), 0) AS BIGINT),
                    CAST(COALESCE(SUM(CASE WHEN _ducklake_end_snapshot = ?1 THEN 1 ELSE 0 END), 0) AS BIGINT)
             FROM {}",
            quote_ident(&table_name)
        );
        let (b, e): (i64, i64) = query_one(conn, &sql, params![sid], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .map_err(|e| sql_error("counting a commit's inlined rows", e))?;
        begin = begin.saturating_add(i64_to_u64(b));
        end = end.saturating_add(i64_to_u64(e));
    }
    Ok((begin, end))
}

/// Live (not ended) inlined row versions at `sid`.
fn count_live_inlined(conn: &dyn SqlConnection, table_id: i64, sid: i64) -> Result<u64> {
    let mut live = 0u64;
    for (table_name, _) in inlined_tables(conn, table_id)? {
        let sql = format!(
            "SELECT COUNT(*) FROM {}
             WHERE _ducklake_begin_snapshot <= ?1
               AND (_ducklake_end_snapshot IS NULL OR _ducklake_end_snapshot > ?1)",
            quote_ident(&table_name)
        );
        let count: i64 = query_one(conn, &sql, params![sid], |row| row.get(0))
            .map_err(|e| sql_error("counting live inlined rows", e))?;
        live = live.saturating_add(i64_to_u64(count));
    }
    Ok(live)
}

/// The pink type of every column this table has ever had, latest definition
/// winning, keyed by lower-case name. Only used to decode/encode inlined
/// values (the typed inlined tables carry DuckDB's physical representation).
fn column_types(conn: &dyn SqlConnection, table_id: i64) -> Result<BTreeMap<String, DataType>> {
    let stmt = conn
        .prepare(
            "SELECT column_name, column_type FROM ducklake_column
             WHERE table_id = ?1
             ORDER BY begin_snapshot DESC, column_order",
        )
        .map_err(|e| sql_error("reading column types", e))?;
    let rows = stmt
        .query_map(params![table_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|e| sql_error("reading column types", e))?;
    let mut out = BTreeMap::new();
    for row in rows {
        let (name, column_type) = row.map_err(|e| sql_error("reading column types", e))?;
        out.entry(name.to_ascii_lowercase())
            .or_insert_with(|| pink_dtype(&column_type));
    }
    Ok(out)
}

fn newest_file_format(
    conn: &dyn SqlConnection,
    table: &TableRow,
    sid: i64,
) -> Result<Option<FileFormat>> {
    let format: Option<String> = conn
        .query_row(
            "SELECT file_format FROM ducklake_data_file
             WHERE table_id = ?1 AND begin_snapshot <= ?2
               AND (end_snapshot IS NULL OR end_snapshot > ?2)
             ORDER BY data_file_id DESC LIMIT 1",
            params![table.table_id, sid],
            |row| row.get(0),
        )
        .map_err(|e| sql_error("reading a data file's format", e))?;
    Ok(format.map(FileFormat::new))
}

// ================================ commit ===================================

fn commit_in_conn(
    conn: &dyn SqlConnection,
    base_version: u64,
    change: &CatalogChange,
    label: &Path,
) -> Result<u64> {
    let Some(target) = change.catalog.as_ref() else {
        return commit_delta_in_conn(conn, base_version, change, label);
    };
    let head = head_version_conn(conn)?;
    if head != Some(base_version) {
        return Err(LakeError::Conflict {
            path: label.to_path_buf(),
            message: format!(
                "the catalog head moved to {} while this commit was based on {base_version}; \
                 rebase and retry",
                head.map(|v| v.to_string())
                    .unwrap_or_else(|| "none".to_string())
            ),
        });
    }
    let Some(base) = load_conn(conn, Some(base_version))? else {
        return Err(LakeError::Conflict {
            path: label.to_path_buf(),
            message: format!("the base version {base_version} does not exist"),
        });
    };
    if target.version != base_version + 1 {
        return Err(LakeError::Internal {
            message: format!(
                "a catalog commit target must be version {}, got {}",
                base_version + 1,
                target.version
            ),
        });
    }
    let sid = snapshot_id(target.version)?;
    let head_sid = snapshot_id(base_version)?;
    let Some(state) = snapshot_state(conn, head_sid)? else {
        return Err(LakeError::Corrupt {
            path: PathBuf::from("ducklake_snapshot"),
            message: format!("the base snapshot {head_sid} is missing"),
        });
    };
    let mut next_catalog_id = state.next_catalog_id.max(1);
    let mut next_file_id = state.next_file_id.max(0);

    let base_rows = visible_tables(conn, head_sid)?;
    let base_by_key: std::collections::BTreeMap<String, TableRow> = base_rows
        .into_iter()
        .map(|row| (row.table_key.clone(), row))
        .collect();
    let mut live_by_key: std::collections::BTreeMap<String, TableRow> = base_by_key.clone();
    let mut changes: Vec<String> = Vec::new();
    let mut inlined_inserts = 0u64;
    let mut inlined_deletes = 0u64;

    // 1. End tables (and their rows) that disappeared.
    let mut tables_left: Vec<TableRow> = live_by_key
        .iter()
        .filter(|(key, _)| !target.tables.contains_key(*key))
        .map(|(_, row)| row.clone())
        .collect();
    tables_left.sort_by_key(|row| row.table_id);
    for row in &tables_left {
        end_table(conn, row, sid)?;
        changes.push(format!("dropped_table:{}", row.table_id));
        live_by_key.remove(&row.table_key);
    }

    // 2. Walk the target tables in key order (deterministic ids).
    let target_keys: Vec<String> = target.tables.keys().cloned().collect();
    for key in target_keys {
        let entry = &target.tables[&key];
        let (schema_name, table_name) = split_key(&key, label)?;
        let base_entry = base.tables.get(&key);
        let identity_changed = match (base_entry, entry.table_id.as_deref()) {
            (Some(old), Some(new)) => old.table_id.as_deref().is_some_and(|old| old != new),
            _ => false,
        };
        let mut row = live_by_key.get(&key).cloned();
        if row.is_none() || identity_changed {
            if let Some(old) = row.as_ref() {
                end_table(conn, old, sid)?;
                changes.push(format!("dropped_table:{}", old.table_id));
            }
            let schema_id = ensure_schema(conn, &schema_name, sid, &mut next_catalog_id)?;
            let table_id = next_catalog_id;
            next_catalog_id += 1;
            let table_uuid = entry
                .table_id
                .as_deref()
                .and_then(hex_to_uuid)
                .unwrap_or_else(crate::fresh_id);
            let table_path = format!("{}/", crate::encode_key(&key));
            conn.execute(
                "INSERT INTO ducklake_table
                 (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name,
                  path, path_is_relative)
                 VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6, 1)",
                params![table_id, table_uuid, sid, schema_id, table_name, table_path],
            )
            .map_err(|e| sql_error("creating a table row", e))?;
            let schema_path: String = query_one(
                conn,
                "SELECT path FROM ducklake_schema WHERE schema_id = ?1",
                params![schema_id],
                |row| row.get(0),
            )
            .map_err(|e| sql_error("reading a schema path", e))?;
            let new_row = TableRow {
                table_id,
                schema_path,
                table_path,
                table_uuid,
                table_key: key.clone(),
            };
            changes.push(format!(
                "created_table:\"{}\".\"{}\"",
                schema_name, table_name
            ));
            live_by_key.insert(key.clone(), new_row.clone());
            row = Some(new_row);
            // A new table has no base entry: write its columns and rows below.
        }
        let row = row.expect("a table row is always available after creation");
        let base_entry = if identity_changed { None } else { base_entry };
        sync_table(
            conn,
            &row,
            base_entry,
            entry,
            sid,
            &mut next_file_id,
            &mut changes,
        )?;
        upsert_table_side(conn, &row, &key, entry)?;
        inlined_inserts += write_inlined(
            conn,
            &row,
            base_entry,
            entry,
            sid,
            target.schema_version,
            &mut changes,
        )?;
        inlined_deletes += end_inlined(conn, &row, entry, sid, &mut changes)?;
        write_table_stats(conn, &row, entry)?;
    }

    // 3. The schema-version row: one per table whose columns changed.
    if target.schema_version != base.schema_version {
        for (key, entry) in &target.tables {
            let Some(row) = live_by_key.get(key) else {
                continue;
            };
            let columns_changed = base
                .tables
                .get(key)
                .map(|old| old.columns != entry.columns || old.renames != entry.renames)
                .unwrap_or(true);
            if columns_changed {
                conn.execute(
                    "INSERT INTO ducklake_schema_versions
                     (begin_snapshot, schema_version, table_id) VALUES (?1, ?2, ?3)",
                    params![sid, i64_from_u64(target.schema_version)?, row.table_id],
                )
                .map_err(|e| sql_error("recording a schema version", e))?;
            }
        }
    }

    // 4. The commit point: the snapshot row and its change row.
    conn.execute(
        "INSERT INTO ducklake_snapshot
         (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            sid,
            ducklake_time(&target.updated),
            i64_from_u64(target.schema_version)?,
            next_catalog_id,
            next_file_id
        ],
    )
    .map_err(|e| sql_error("inserting the snapshot", e))?;
    let changes_made = if changes.is_empty() {
        Some("nothing".to_string())
    } else {
        Some(changes.join(","))
    };
    conn.execute(
        "INSERT INTO ducklake_snapshot_changes
         (snapshot_id, changes_made, author, commit_message)
         VALUES (?1, ?2, ?3, ?4)",
        params![sid, changes_made, target.author, target.commit_message],
    )
    .map_err(|e| sql_error("inserting the snapshot change row", e))?;
    conn.execute(
        "INSERT INTO pinkdb_snapshot
         (snapshot_id, updated, format_version, lake_id)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            sid,
            target.updated,
            i64::from(target.format_version),
            target.lake_id,
        ],
    )
    .map_err(|e| sql_error("inserting the snapshot side row", e))?;
    if let Some(lake_id) = target.lake_id.as_deref() {
        crate::maybe_fail_publish_after_snapshot(lake_id)?;
    }
    // Wave 96b: the engine catalog image becomes visible with the version it
    // describes — a crash here leaves the previous image and the previous
    // version, never a half-published pair.
    if let Some(image) = change.engine_catalog.as_deref() {
        write_engine_catalog(conn, target.version, image)?;
    }
    let _ = (inlined_inserts, inlined_deletes);
    Ok(target.version)
}

fn commit_delta_in_conn(
    conn: &dyn SqlConnection,
    base_version: u64,
    change: &CatalogChange,
    label: &Path,
) -> Result<u64> {
    let target_version = base_version + 1;
    let head = head_version_conn(conn)?;
    if head != Some(base_version) {
        return Err(LakeError::Conflict {
            path: label.to_path_buf(),
            message: format!(
                "the catalog head moved to {} while this commit was based on {base_version}; \
                 rebase and retry",
                head.map(|v| v.to_string())
                    .unwrap_or_else(|| "none".to_string())
            ),
        });
    }
    if change.manifest.version != target_version {
        return Err(LakeError::Internal {
            message: format!(
                "a catalog commit target must be version {target_version}, got {}",
                change.manifest.version
            ),
        });
    }
    let base_sid = snapshot_id(base_version)?;
    let sid = snapshot_id(target_version)?;
    let Some(state) = snapshot_state(conn, base_sid)? else {
        return Err(LakeError::Corrupt {
            path: PathBuf::from("ducklake_snapshot"),
            message: format!("the base snapshot {base_sid} is missing"),
        });
    };
    let rows_before = conn.total_changes();
    let mut next_catalog_id = state.next_catalog_id.max(1);
    let mut next_file_id = state.next_file_id.max(0);
    let mut changes: Vec<String> = Vec::new();
    if let Some(wanted) = change.schemas.as_deref() {
        reconcile_schemas(conn, wanted, sid, &mut next_catalog_id, &mut changes)?;
    }

    // Tables whose columns began or ended at this snapshot: exactly these get
    // a `ducklake_schema_versions` row, as DuckLake writes.
    let mut altered_tables: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    for (key, delta) in &change.manifest.tables {
        let runtime = &delta.runtime;
        let (schema_name, table_name) = split_key(key, label)?;
        let mut row = find_table(conn, key, base_sid)?;
        if runtime.dropped {
            let Some(old) = row.take() else {
                return Err(LakeError::Corrupt {
                    path: PathBuf::from("ducklake_table"),
                    message: format!("the commit drops table \"{key}\", but it is not live"),
                });
            };
            end_table(conn, &old, sid)?;
            changes.push(format!("dropped_table:{}", old.table_id));
            conn.execute(
                "DELETE FROM ducklake_table_stats WHERE table_id = ?1",
                params![old.table_id],
            )
            .map_err(|e| sql_error("dropping table statistics", e))?;
            conn.execute(
                "DELETE FROM ducklake_table_column_stats WHERE table_id = ?1",
                params![old.table_id],
            )
            .map_err(|e| sql_error("dropping table column statistics", e))?;
            continue;
        }
        if row.is_none() {
            let schema_id = ensure_schema(conn, &schema_name, sid, &mut next_catalog_id)?;
            let table_id = next_catalog_id;
            next_catalog_id += 1;
            let table_uuid = runtime
                .meta
                .as_ref()
                .and_then(|meta| meta.table_id.as_deref())
                .and_then(hex_to_uuid)
                .unwrap_or_else(crate::fresh_id);
            let table_path = format!("{}/", crate::encode_key(key));
            conn.execute(
                "INSERT INTO ducklake_table
                 (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name,
                  path, path_is_relative)
                 VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6, 1)",
                params![table_id, table_uuid, sid, schema_id, table_name, table_path],
            )
            .map_err(|e| sql_error("creating a table row", e))?;
            let schema_path: String = query_one(
                conn,
                "SELECT path FROM ducklake_schema WHERE schema_id = ?1",
                params![schema_id],
                |row| row.get(0),
            )
            .map_err(|e| sql_error("reading a schema path", e))?;
            changes.push(format!(
                "created_table:\"{}\".\"{}\"",
                schema_name, table_name
            ));
            row = Some(TableRow {
                table_id,
                schema_path,
                table_path,
                table_uuid,
                table_key: key.clone(),
            });
        }
        let row = row.expect("a live table row exists after creation");
        let meta = runtime.meta.as_ref().ok_or_else(|| LakeError::Internal {
            message: format!("the delta for table \"{key}\" carried no target facts"),
        })?;
        let stub = entry_from_meta(meta);

        // Schema and partition records.
        // A column change on an existing table is DuckLake's `altered_table`
        // (a created table's columns are part of `created_table`).
        if sync_columns(conn, &row, None, &stub, sid)? {
            altered_tables.insert(row.table_id);
            if !runtime.created {
                changes.push(format!("altered_table:{}", row.table_id));
            }
        }
        if delta.partition_changed {
            sync_partition_spec(conn, &row, &stub, sid, &mut changes)?;
        }
        upsert_table_side(conn, &row, key, &stub)?;

        // The bytes the delta removes, measured before the end markers change.
        let ended_sizes = if delta.replaced {
            file_size_live(conn, row.table_id)?
        } else {
            let mut total = 0u64;
            for path in &delta.compacted_files {
                total += file_size_by_path(conn, row.table_id, path)?;
            }
            total
        };
        // End the files this delta removes, then append the new ones.
        if delta.replaced {
            conn.execute(
                "UPDATE ducklake_data_file SET end_snapshot = ?1
                 WHERE table_id = ?2 AND end_snapshot IS NULL",
                params![sid, row.table_id],
            )
            .map_err(|e| sql_error("ending a table's data files", e))?;
            conn.execute(
                "UPDATE ducklake_delete_file SET end_snapshot = ?1
                 WHERE table_id = ?2 AND end_snapshot IS NULL",
                params![sid, row.table_id],
            )
            .map_err(|e| sql_error("ending a table's delete files", e))?;
        } else {
            for path in &delta.compacted_files {
                conn.execute(
                    "UPDATE ducklake_data_file SET end_snapshot = ?1
                     WHERE table_id = ?2 AND path = ?3 AND end_snapshot IS NULL",
                    params![sid, row.table_id, path_basename(path)],
                )
                .map_err(|e| sql_error("ending a compacted data file", e))?;
            }
            for path in &delta.removed_delete_files {
                conn.execute(
                    "UPDATE ducklake_delete_file SET end_snapshot = ?1
                     WHERE table_id = ?2 AND path = ?3 AND end_snapshot IS NULL",
                    params![sid, row.table_id, path_basename(path)],
                )
                .map_err(|e| sql_error("ending a rewritten delete file", e))?;
            }
        }
        let mut added_sizes = 0u64;
        let mut added_records = 0u64;
        let format = meta.format.as_ref().map(|f| f.id().to_string());
        let partition_id = current_partition_id(conn, row.table_id, sid)?;
        for file in &delta.data_files {
            let file_id = next_file_id;
            next_file_id += 1;
            let basename = path_basename(file.path());
            let size = file_size(conn, file.path());
            added_sizes = added_sizes.saturating_add(size);
            added_records = added_records.saturating_add(file.rows().unwrap_or(0));
            conn.execute(
                "INSERT INTO ducklake_data_file
                 (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path,
                  path_is_relative, file_format, record_count, file_size_bytes, footer_size,
                  row_id_start, partition_id, encryption_key, mapping_id, partial_max,
                  row_group_count)
                 VALUES (?1, ?2, ?3, NULL, NULL, ?4, 1, ?5, ?6, ?7, ?8, ?9, ?10, ?11, NULL,
                         NULL, 1)",
                params![
                    file_id,
                    row.table_id,
                    sid,
                    basename,
                    format.as_deref(),
                    i64_from_u64(file.rows().unwrap_or(0))?,
                    i64_from_u64(size)?,
                    file.footer_location()
                        .map(|f| i64_from_u64(f.length))
                        .transpose()?,
                    file.row_id_start().map(i64_from_u64).transpose()?,
                    partition_id,
                    crate::storage::written_file_key(file.path()).map(|key| encode_hex(&key)),
                ],
            )
            .map_err(|e| sql_error("inserting a data file", e))?;
            let footer = file.footer_location();
            conn.execute(
                "INSERT INTO pinkdb_data_file
                 (data_file_id, schema_version, footer_offset, footer_len, footer_crc32)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    file_id,
                    file.schema_version().map(i64_from_u64).transpose()?,
                    footer.map(|f| i64_from_u64(f.offset)).transpose()?,
                    footer.map(|f| i64_from_u64(f.length)).transpose()?,
                    footer.map(|f| i64::from(f.crc32c)),
                ],
            )
            .map_err(|e| sql_error("inserting a data file's side row", e))?;
            write_file_stats(conn, &row, file, file_id)?;
            write_partition_values(conn, &row, file, file_id, sid)?;
        }
        if !delta.data_files.is_empty() {
            changes.push(format!("inserted_into_table:{}", row.table_id));
        }

        let mut added_delete_rows = 0u64;
        for delete in &delta.delete_files {
            let file_id = next_file_id;
            next_file_id += 1;
            let data_file_id: Option<i64> = conn
                .query_row(
                    "SELECT data_file_id FROM ducklake_data_file
                     WHERE table_id = ?1 AND path = ?2 AND end_snapshot IS NULL
                     ORDER BY data_file_id DESC LIMIT 1",
                    params![row.table_id, path_basename(&delete.file)],
                    |row| row.get(0),
                )
                .map_err(|e| sql_error("looking up a delete file's data file", e))?;
            let size = file_size(conn, &delete.path);
            let delete_format = if format.as_deref() == Some("parquet") {
                "parquet"
            } else {
                "pink"
            };
            conn.execute(
                "INSERT INTO ducklake_delete_file
                 (delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path,
                  path_is_relative, format, delete_count, file_size_bytes, footer_size,
                  encryption_key, partial_max, row_group_count)
                 VALUES (?1, ?2, ?3, NULL, ?4, ?5, 1, ?6, ?7, ?8, NULL, ?9, NULL, 1)",
                params![
                    file_id,
                    row.table_id,
                    sid,
                    data_file_id,
                    path_basename(&delete.path),
                    delete_format,
                    i64_from_u64(delete.count)?,
                    i64_from_u64(size)?,
                    crate::storage::written_file_key(&delete.path).map(|key| encode_hex(&key)),
                ],
            )
            .map_err(|e| sql_error("inserting a delete file", e))?;
            added_delete_rows = added_delete_rows.saturating_add(delete.count);
        }
        if !delta.delete_files.is_empty() {
            changes.push(format!("deleted_from_table:{}", row.table_id));
        }

        let mut inserted_inlined = 0u64;
        if !runtime.inlined_rows.is_empty() {
            let mut inlined_entry = entry_from_meta(meta);
            inlined_entry.inlined = runtime.inlined_rows.clone();
            inserted_inlined = write_inlined(
                conn,
                &row,
                None,
                &inlined_entry,
                sid,
                change.schema_version,
                &mut changes,
            )?;
        }
        let mut ended_inlined = 0u64;
        let mut flushed_inlined = 0u64;
        for old in &runtime.inlined_ends {
            end_inlined_row(conn, &row, old, sid)?;
            ended_inlined += 1;
            if old.flushed {
                flushed_inlined += 1;
            }
        }
        if ended_inlined > 0 {
            changes.push(format!("inlined_delete:{ended_inlined}"));
        }

        write_table_stats_delta(
            conn,
            &row,
            delta,
            added_records,
            added_sizes,
            ended_sizes,
            added_delete_rows,
            inserted_inlined,
            ended_inlined,
            flushed_inlined,
            meta.next_row_id,
        )?;
        let kind = match runtime.kind {
            DeltaKind::Commit => 0,
            DeltaKind::Save => 1,
            DeltaKind::Compaction => 2,
            DeltaKind::Flush => 3,
        };
        conn.execute(
            "INSERT INTO pinkdb_table_change
             (snapshot_id, table_id, kind, partition_changed, inlining_changed, inlined_limit)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                sid,
                row.table_id,
                kind,
                i64::from(delta.partition_changed),
                i64::from(delta.inlining_changed),
                delta.inlined_limit.map(i64_from_u64).transpose()?,
            ],
        )
        .map_err(|e| sql_error("recording a table change", e))?;
    }

    // One schema-version row per table whose columns or existence changed.
    if change.schema_version > state.schema_version {
        for (key, delta) in &change.manifest.tables {
            if delta.runtime.dropped {
                continue;
            }
            let Some(row) = find_table(conn, key, base_sid)? else {
                if let Some(row) = find_table(conn, key, sid)? {
                    conn.execute(
                        "INSERT INTO ducklake_schema_versions
                         (begin_snapshot, schema_version, table_id) VALUES (?1, ?2, ?3)",
                        params![sid, i64_from_u64(change.schema_version)?, row.table_id],
                    )
                    .map_err(|e| sql_error("recording a schema version", e))?;
                }
                continue;
            };
            if delta.runtime.created || altered_tables.contains(&row.table_id) {
                conn.execute(
                    "INSERT INTO ducklake_schema_versions
                     (begin_snapshot, schema_version, table_id) VALUES (?1, ?2, ?3)",
                    params![sid, i64_from_u64(change.schema_version)?, row.table_id],
                )
                .map_err(|e| sql_error("recording a schema version", e))?;
            }
        }
    }

    insert_snapshot(conn, sid, change, next_catalog_id, next_file_id, &changes)?;
    if let Some(image) = change.engine_catalog.as_deref() {
        write_engine_catalog(conn, pink_version(sid), image)?;
    }
    let touched = conn.total_changes().saturating_sub(rows_before);
    LAST_COMMIT_ROWS_TOUCHED.with(|rows| rows.set(touched));
    Ok(target_version)
}

fn entry_from_meta(meta: &TableMeta) -> TableEntry {
    TableEntry {
        files: Vec::new(),
        rows: 0,
        table_id: meta.table_id.clone(),
        format: meta.format.clone(),
        stats: None,
        deletes: Vec::new(),
        partition_by: meta.partition_by.clone(),
        schema_version: meta.schema_version,
        columns: meta.columns.clone(),
        renames: meta.renames.clone(),
        inlined: Vec::new(),
        inlined_limit: meta.inlined_limit,
        next_row_id: meta.next_row_id,
    }
}

fn end_inlined_row(
    conn: &dyn SqlConnection,
    row: &TableRow,
    old: &InlinedRow,
    sid: i64,
) -> Result<()> {
    let end_sid = snapshot_id(old.end.unwrap_or(pink_version(sid)))?;
    let begin_sid = snapshot_id(old.begin)?;
    // The typed table is named after the schema version of the snapshot that
    // wrote the row, so that version also names the table to end it in.
    if let Some(suffix) = snapshot_schema_version(conn, old.begin)? {
        let table_name = format!("ducklake_inlined_data_{}_{}", row.table_id, suffix);
        if table_exists(conn, &table_name)?
            && end_typed_inlined_row(conn, &table_name, old, end_sid, begin_sid)?
        {
            return Ok(());
        }
    }
    // A row carried over from a catalog whose suffix predates the current
    // snapshot history: find its table by identity.
    for (table_name, _) in inlined_tables(conn, row.table_id)? {
        if end_typed_inlined_row(conn, &table_name, old, end_sid, begin_sid)? {
            break;
        }
    }
    Ok(())
}

fn end_typed_inlined_row(
    conn: &dyn SqlConnection,
    table_name: &str,
    old: &InlinedRow,
    end_sid: i64,
    begin_sid: i64,
) -> Result<bool> {
    let sql = format!(
        "UPDATE {} SET _ducklake_end_snapshot = ?1
         WHERE _ducklake_row_id = ?2 AND _ducklake_begin_snapshot = ?3",
        quote_ident(table_name)
    );
    let updated = conn
        .execute(&sql, params![end_sid, i64_from_u64(old.row_id)?, begin_sid])
        .map_err(|e| sql_error("ending an inlined row", e))?;
    Ok(updated > 0)
}

#[allow(clippy::too_many_arguments)]
fn write_table_stats_delta(
    conn: &dyn SqlConnection,
    row: &TableRow,
    delta: &TableDelta,
    added_records: u64,
    added_bytes: u64,
    ended_bytes: u64,
    added_delete_rows: u64,
    inserted_inlined: u64,
    ended_inlined: u64,
    flushed_inlined: u64,
    next_row_id: u64,
) -> Result<()> {
    let current: (u64, u64, u64) = conn
        .query_row(
            "SELECT COALESCE(record_count, 0), COALESCE(next_row_id, 0),
                    COALESCE(file_size_bytes, 0)
             FROM ducklake_table_stats WHERE table_id = ?1",
            params![row.table_id],
            |row| {
                Ok((
                    i64_to_u64(row.get::<_, i64>(0)?),
                    i64_to_u64(row.get::<_, i64>(1)?),
                    i64_to_u64(row.get::<_, i64>(2)?),
                ))
            },
        )
        .map_err(|e| sql_error("reading table statistics", e))?
        .unwrap_or((0, 0, 0));
    let record_count = match delta.runtime.kind {
        DeltaKind::Save if delta.replaced => added_records.saturating_add(inserted_inlined),
        DeltaKind::Flush => current.0,
        DeltaKind::Compaction => current.0,
        DeltaKind::Commit | DeltaKind::Save => current
            .0
            .saturating_add(delta.rows_added)
            .saturating_sub(ended_inlined),
    };
    let _ = (added_delete_rows, flushed_inlined);
    let file_size = current
        .2
        .saturating_add(added_bytes)
        .saturating_sub(ended_bytes);
    let updated = conn
        .execute(
            "UPDATE ducklake_table_stats
             SET record_count = ?2, next_row_id = ?3, file_size_bytes = ?4
             WHERE table_id = ?1",
            params![
                row.table_id,
                i64_from_u64(record_count)?,
                i64_from_u64(current.1.max(next_row_id))?,
                i64_from_u64(file_size)?
            ],
        )
        .map_err(|e| sql_error("updating table statistics", e))?;
    if updated == 0 {
        conn.execute(
            "INSERT INTO ducklake_table_stats
             (table_id, record_count, next_row_id, file_size_bytes) VALUES (?1, ?2, ?3, ?4)",
            params![
                row.table_id,
                i64_from_u64(record_count)?,
                i64_from_u64(current.1.max(next_row_id))?,
                i64_from_u64(file_size)?
            ],
        )
        .map_err(|e| sql_error("inserting table statistics", e))?;
    }
    if delta.replaced {
        conn.execute(
            "DELETE FROM ducklake_table_column_stats WHERE table_id = ?1",
            params![row.table_id],
        )
        .map_err(|e| sql_error("replacing table column statistics", e))?;
    }
    update_table_column_stats(conn, row, delta)?;
    Ok(())
}

fn update_table_column_stats(
    conn: &dyn SqlConnection,
    row: &TableRow,
    delta: &TableDelta,
) -> Result<()> {
    let Some(meta) = delta.runtime.meta.as_ref() else {
        return Ok(());
    };
    let Some(columns) = meta.columns.as_deref() else {
        return Ok(());
    };
    let ids = column_ids(conn, row.table_id)?;
    for column in columns {
        let Some(dtype) = crate::catalog::parse_dtype_name(&column.dtype) else {
            continue;
        };
        let Some(column_id) = ids.get(&column.name).copied() else {
            continue;
        };
        let mut values: Vec<pinkdb::Value> = Vec::new();
        let mut contains_null = false;
        let mut contains_nan = false;
        if !delta.replaced
            && let Some(old) = conn
                .query_row(
                    "SELECT contains_null, contains_nan, min_value, max_value
                     FROM ducklake_table_column_stats
                     WHERE table_id = ?1 AND column_id = ?2",
                    params![row.table_id, column_id],
                    |row| {
                        Ok((
                            row.get::<_, Option<i64>>(0)?,
                            row.get::<_, Option<i64>>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    },
                )
                .map_err(|e| sql_error("reading table column statistics", e))?
        {
            contains_null |= old.0.unwrap_or(0) != 0;
            contains_nan |= old.1.unwrap_or(0) != 0;
            for value in [old.2, old.3].into_iter().flatten() {
                if let Some(value) = ducklake_to_canonical(dtype, &value)
                    && let Some(value) = stats::decode_value(dtype, &value)
                {
                    values.push(value);
                }
            }
        }
        for file in &delta.data_files {
            if let Some(block) = file.stats()
                && let Some(stat) = block.columns.iter().find(|c| c.name == column.name)
            {
                contains_null |= stat.nulls > 0;
                contains_nan |= stat.nan.unwrap_or(0) > 0;
                for value in [&stat.min, &stat.max].into_iter().flatten() {
                    if let Some(value) = stats::decode_value(dtype, value) {
                        values.push(value);
                    }
                }
            }
        }
        for inlined in &delta.runtime.inlined_rows {
            if let Some(value) = inlined
                .columns
                .iter()
                .position(|name| name == &column.name)
                .and_then(|index| inlined.values.get(index))
                .and_then(|value| value.as_deref())
            {
                if let Some(value) = stats::decode_value(dtype, value) {
                    values.push(value);
                }
            } else {
                contains_null = true;
            }
        }
        values.sort_by(compare_values);
        let min = values
            .first()
            .and_then(|value| stats::encode_value(dtype, value))
            .and_then(|value| canonical_to_ducklake(dtype, &value));
        let max = values
            .last()
            .and_then(|value| stats::encode_value(dtype, value))
            .and_then(|value| canonical_to_ducklake(dtype, &value));
        let nan = if dtype == pinkdb::DataType::Double {
            Some(i64::from(contains_nan))
        } else {
            None
        };
        let updated = conn
            .execute(
                "UPDATE ducklake_table_column_stats
                 SET contains_null = ?3, contains_nan = ?4, min_value = ?5, max_value = ?6,
                     extra_stats = NULL, min_is_exact = 1, max_is_exact = 1
                 WHERE table_id = ?1 AND column_id = ?2",
                params![row.table_id, column_id, contains_null, nan, min, max],
            )
            .map_err(|e| sql_error("updating table column statistics", e))?;
        if updated == 0 {
            conn.execute(
                "INSERT INTO ducklake_table_column_stats
                 (table_id, column_id, contains_null, contains_nan, min_value, max_value,
                  extra_stats, min_is_exact, max_is_exact)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, 1, 1)",
                params![row.table_id, column_id, contains_null, nan, min, max],
            )
            .map_err(|e| sql_error("inserting table column statistics", e))?;
        }
    }
    Ok(())
}

fn insert_snapshot(
    conn: &dyn SqlConnection,
    sid: i64,
    change: &CatalogChange,
    next_catalog_id: i64,
    next_file_id: i64,
    changes: &[String],
) -> Result<()> {
    conn.execute(
        "INSERT INTO ducklake_snapshot
         (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            sid,
            ducklake_time(&change.updated),
            i64_from_u64(change.schema_version)?,
            next_catalog_id,
            next_file_id
        ],
    )
    .map_err(|e| sql_error("inserting the snapshot", e))?;
    let changes_made = if changes.is_empty() {
        Some("nothing".to_string())
    } else {
        Some(changes.join(","))
    };
    conn.execute(
        "INSERT INTO ducklake_snapshot_changes
         (snapshot_id, changes_made, author, commit_message)
         VALUES (?1, ?2, ?3, ?4)",
        params![sid, changes_made, change.author, change.commit_message],
    )
    .map_err(|e| sql_error("inserting the snapshot change row", e))?;
    conn.execute(
        "INSERT INTO pinkdb_snapshot
         (snapshot_id, updated, format_version, lake_id)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            sid,
            change.updated,
            i64::from(change.format_version),
            change.lake_id,
        ],
    )
    .map_err(|e| sql_error("inserting the snapshot side row", e))?;
    if let Some(lake_id) = change.lake_id.as_deref() {
        crate::maybe_fail_publish_after_snapshot(lake_id)?;
    }
    Ok(())
}

fn file_size_live(conn: &dyn SqlConnection, table_id: i64) -> Result<u64> {
    let total: i64 = query_one(
        conn,
        "SELECT CAST(COALESCE(SUM(file_size_bytes), 0) AS BIGINT) FROM ducklake_data_file
         WHERE table_id = ?1 AND end_snapshot IS NULL",
        params![table_id],
        |row| row.get(0),
    )
    .map_err(|e| sql_error("summing live file sizes", e))?;
    Ok(i64_to_u64(total))
}

fn file_size_by_path(conn: &dyn SqlConnection, table_id: i64, path: &str) -> Result<u64> {
    let size: Option<i64> = conn
        .query_row(
            "SELECT file_size_bytes FROM ducklake_data_file
             WHERE table_id = ?1 AND path = ?2 AND end_snapshot IS NULL
             ORDER BY data_file_id DESC LIMIT 1",
            params![table_id, path_basename(path)],
            |row| row.get(0),
        )
        .map_err(|e| sql_error("reading a file's size", e))?;
    Ok(size.map(i64_to_u64).unwrap_or(0))
}

fn end_table(conn: &dyn SqlConnection, row: &TableRow, sid: i64) -> Result<()> {
    conn.execute(
        "UPDATE ducklake_table SET end_snapshot = ?1
         WHERE table_id = ?2 AND end_snapshot IS NULL",
        params![sid, row.table_id],
    )
    .map_err(|e| sql_error("ending a table", e))?;
    conn.execute(
        "UPDATE ducklake_column SET end_snapshot = ?1
         WHERE table_id = ?2 AND end_snapshot IS NULL",
        params![sid, row.table_id],
    )
    .map_err(|e| sql_error("ending a table's columns", e))?;
    conn.execute(
        "UPDATE ducklake_data_file SET end_snapshot = ?1
         WHERE table_id = ?2 AND end_snapshot IS NULL",
        params![sid, row.table_id],
    )
    .map_err(|e| sql_error("ending a table's data files", e))?;
    conn.execute(
        "UPDATE ducklake_delete_file SET end_snapshot = ?1
         WHERE table_id = ?2 AND end_snapshot IS NULL",
        params![sid, row.table_id],
    )
    .map_err(|e| sql_error("ending a table's delete files", e))?;
    Ok(())
}

fn ensure_schema(
    conn: &dyn SqlConnection,
    schema_name: &str,
    sid: i64,
    next_catalog_id: &mut i64,
) -> Result<i64> {
    if let Some(id) = conn
        .query_row(
            "SELECT schema_id FROM ducklake_schema
             WHERE schema_name = ?1 AND begin_snapshot <= ?2
               AND (end_snapshot IS NULL OR end_snapshot > ?2)
             ORDER BY begin_snapshot DESC LIMIT 1",
            params![schema_name, sid],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| sql_error("reading a schema", e))?
    {
        return Ok(id);
    }
    let schema_id = *next_catalog_id;
    *next_catalog_id += 1;
    conn.execute(
        "INSERT INTO ducklake_schema
         (schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path,
          path_is_relative, parent_schema_id)
         VALUES (?1, ?2, ?3, NULL, ?4, 'data/', 1, NULL)",
        params![schema_id, crate::fresh_id(), sid, schema_name],
    )
    .map_err(|e| sql_error("creating a schema", e))?;
    Ok(schema_id)
}

/// Make the catalog's live schemas match the engine's list: end the ones it no
/// longer has, create the ones it gained (a lone `CREATE SCHEMA` must show up
/// in DuckLake's catalog even while the schema is empty).
fn reconcile_schemas(
    conn: &dyn SqlConnection,
    wanted: &[String],
    sid: i64,
    next_catalog_id: &mut i64,
    changes: &mut Vec<String>,
) -> Result<()> {
    let live = schemas_conn(conn, sid)?;
    for name in &live {
        if wanted.iter().any(|w| w.eq_ignore_ascii_case(name)) {
            continue;
        }
        // DuckLake records a dropped schema by id (`dropped_schema:<id>`, see
        // ducklake_transaction_state.cpp) and a created one by quoted name.
        let Some(schema_id) = conn
            .query_row(
                "SELECT schema_id FROM ducklake_schema
                 WHERE schema_name = ?1 AND end_snapshot IS NULL AND begin_snapshot < ?2",
                params![name, sid],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|e| sql_error("reading a schema", e))?
        else {
            continue;
        };
        conn.execute(
            "UPDATE ducklake_schema SET end_snapshot = ?1 WHERE schema_id = ?2",
            params![sid, schema_id],
        )
        .map_err(|e| sql_error("ending a schema", e))?;
        changes.push(format!("dropped_schema:{schema_id}"));
    }
    for name in wanted {
        if live.iter().any(|l| l.eq_ignore_ascii_case(name)) {
            continue;
        }
        ensure_schema(conn, name, sid, next_catalog_id)?;
        changes.push(format!("created_schema:{}", quote_ident(name)));
    }
    Ok(())
}

/// Write a table's columns, files, deletes and partition values as one delta.
#[allow(clippy::too_many_arguments)]
fn sync_table(
    conn: &dyn SqlConnection,
    row: &TableRow,
    base: Option<&TableEntry>,
    target: &TableEntry,
    sid: i64,
    next_file_id: &mut i64,
    changes: &mut Vec<String>,
) -> Result<()> {
    sync_columns(conn, row, base, target, sid)?;
    if base.is_none_or(|b| b.partition_by != target.partition_by) {
        sync_partition_spec(conn, row, target, sid, changes)?;
    }
    let base_files: std::collections::BTreeMap<String, &FileRef> = base
        .map(|b| b.files.iter().map(|f| (f.path().to_string(), f)).collect())
        .unwrap_or_default();
    let target_paths: std::collections::BTreeSet<String> =
        target.files.iter().map(|f| f.path().to_string()).collect();
    for path in base_files.keys() {
        if !target_paths.contains(path) {
            conn.execute(
                "UPDATE ducklake_data_file SET end_snapshot = ?1
                 WHERE table_id = ?2 AND path = ?3 AND end_snapshot IS NULL",
                params![sid, row.table_id, path_basename(path)],
            )
            .map_err(|e| sql_error("ending a data file", e))?;
        }
    }
    let mut inserted_file = false;
    for file in &target.files {
        if base_files.contains_key(file.path()) {
            continue;
        }
        let file_id = *next_file_id;
        *next_file_id += 1;
        let basename = path_basename(file.path());
        let size = file_size(conn, file.path());
        let format = target.format().id().to_string();
        conn.execute(
            "INSERT INTO ducklake_data_file
             (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path,
              path_is_relative, file_format, record_count, file_size_bytes, footer_size,
              row_id_start, partition_id, encryption_key, mapping_id, partial_max,
              row_group_count)
             VALUES (?1, ?2, ?3, NULL, NULL, ?4, 1, ?5, ?6, ?7, ?8, ?9, ?10, ?11, NULL,
                     NULL, 1)",
            params![
                file_id,
                row.table_id,
                sid,
                basename,
                format,
                i64_from_u64(file.rows().unwrap_or(0))?,
                i64_from_u64(size)?,
                file.footer_location()
                    .map(|f| i64_from_u64(f.length))
                    .transpose()?,
                file.row_id_start().map(i64_from_u64).transpose()?,
                current_partition_id(conn, row.table_id, sid)?,
                crate::storage::written_file_key(file.path()).map(|key| encode_hex(&key)),
            ],
        )
        .map_err(|e| sql_error("inserting a data file", e))?;
        let footer = file.footer_location();
        conn.execute(
            "INSERT INTO pinkdb_data_file
             (data_file_id, schema_version, footer_offset, footer_len, footer_crc32)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                file_id,
                file.schema_version().map(i64_from_u64).transpose()?,
                footer.map(|f| i64_from_u64(f.offset)).transpose()?,
                footer.map(|f| i64_from_u64(f.length)).transpose()?,
                footer.map(|f| i64::from(f.crc32c)),
            ],
        )
        .map_err(|e| sql_error("inserting a data file's side row", e))?;
        write_file_stats(conn, row, file, file_id)?;
        write_partition_values(conn, row, file, file_id, sid)?;
        inserted_file = true;
    }
    if inserted_file {
        changes.push(format!("inserted_into_table:{}", row.table_id));
    }

    let base_deletes: std::collections::BTreeSet<String> = base
        .map(|b| b.deletes.iter().map(|d| d.path.clone()).collect())
        .unwrap_or_default();
    let target_delete_paths: std::collections::BTreeSet<String> =
        target.deletes.iter().map(|d| d.path.clone()).collect();
    for path in &base_deletes {
        if !target_delete_paths.contains(path) {
            conn.execute(
                "UPDATE ducklake_delete_file SET end_snapshot = ?1
                 WHERE table_id = ?2 AND path = ?3 AND end_snapshot IS NULL",
                params![sid, row.table_id, path_basename(path)],
            )
            .map_err(|e| sql_error("ending a delete file", e))?;
        }
    }
    let mut inserted_delete = false;
    for delete in &target.deletes {
        if base_deletes.contains(&delete.path) {
            continue;
        }
        let file_id = *next_file_id;
        *next_file_id += 1;
        let data_file_id: Option<i64> = conn
            .query_row(
                "SELECT data_file_id FROM ducklake_data_file
                 WHERE table_id = ?1 AND path = ?2 AND end_snapshot IS NULL
                 ORDER BY data_file_id DESC LIMIT 1",
                params![row.table_id, path_basename(&delete.file)],
                |row| row.get(0),
            )
            .map_err(|e| sql_error("looking up a delete file's data file", e))?;
        let size = file_size(conn, &delete.path);
        let delete_format = if target.format().id() == "parquet" {
            "parquet"
        } else {
            "pink"
        };
        conn.execute(
            "INSERT INTO ducklake_delete_file
             (delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path,
              path_is_relative, format, delete_count, file_size_bytes, footer_size,
              encryption_key, partial_max, row_group_count)
             VALUES (?1, ?2, ?3, NULL, ?4, ?5, 1, ?6, ?7, ?8, NULL, ?9, NULL, 1)",
            params![
                file_id,
                row.table_id,
                sid,
                data_file_id,
                path_basename(&delete.path),
                delete_format,
                i64_from_u64(delete.count)?,
                i64_from_u64(size)?,
                crate::storage::written_file_key(&delete.path).map(|key| encode_hex(&key)),
            ],
        )
        .map_err(|e| sql_error("inserting a delete file", e))?;
        inserted_delete = true;
    }
    if inserted_delete {
        changes.push(format!("deleted_from_table:{}", row.table_id));
    }
    Ok(())
}

fn sync_columns(
    conn: &dyn SqlConnection,
    row: &TableRow,
    base: Option<&TableEntry>,
    target: &TableEntry,
    sid: i64,
) -> Result<bool> {
    let target_columns = target.columns.as_deref().unwrap_or(&[]);
    if target_columns.is_empty() {
        return Ok(false);
    }
    // Whether any `ducklake_column` row began or ended at `sid` (DuckLake's
    // `altered_table` and a `ducklake_schema_versions` row follow from it).
    let mut altered = false;
    let mut live: Vec<(i64, i64, String, String)> = Vec::new(); // id, order, name, type
    {
        let stmt = conn
            .prepare(
                // Only top-level columns: an ARRAY's `element` child is
                // created by `insert_column` right after its parent, is not a
                // column of the table, and must never be matched by name or
                // ended by the "gone from the target" sweep below.
                "SELECT column_id, column_order, column_name, column_type
                 FROM ducklake_column
                 WHERE table_id = ?1 AND begin_snapshot <= ?2
                   AND (end_snapshot IS NULL OR end_snapshot > ?2)
                   AND parent_column IS NULL
                 ORDER BY column_order, column_id",
            )
            .map_err(|e| sql_error("reading live columns", e))?;
        let rows = stmt
            .query_map(params![row.table_id, sid], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(|e| sql_error("reading live columns", e))?;
        for r in rows {
            live.push(r.map_err(|e| sql_error("reading live columns", e))?);
        }
    }
    let mut max_column_id = conn
        .query_row(
            "SELECT MAX(column_id) FROM ducklake_column WHERE table_id = ?1",
            params![row.table_id],
            |row| row.get::<_, Option<i64>>(0),
        )
        .map_err(|e| sql_error("reading the column id sequence", e))?
        .flatten()
        .unwrap_or(0)
        .max(0);
    let mut max_order = live.iter().map(|c| c.1).max().unwrap_or(0);
    let live_by_name: std::collections::BTreeMap<String, &(i64, i64, String, String)> =
        live.iter().map(|c| (c.2.clone(), c)).collect();
    let target_names: std::collections::BTreeSet<&str> =
        target_columns.iter().map(|c| c.name.as_str()).collect();
    let renames = &target.renames;

    // Rename/end rows that the target no longer carries under their old name.
    let mut consumed: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for column in target_columns {
        if let Some(existing) = live_by_name.get(&column.name) {
            consumed.insert(column.name.clone());
            if existing.3 != ducklake_type(&column.dtype) {
                // A type change: end the old row and insert the new type
                // under the same column id.
                end_column(conn, row.table_id, existing.0, sid)?;
                altered = true;
                let _ = insert_column(
                    conn,
                    row,
                    existing.0,
                    existing.1,
                    &column.name,
                    &column.dtype,
                    column,
                    sid,
                )?;
            }
            continue;
        }
        // A rename: the target's rename log names the old spelling.
        if let Some(rename) = renames.iter().rev().find(|rename| rename.to == column.name)
            && let Some(existing) = live_by_name.get(&rename.from)
        {
            consumed.insert(rename.from.clone());
            end_column(conn, row.table_id, existing.0, sid)?;
            altered = true;
            // The renamed column keeps its epoch: it is the same column id,
            // so a file written before the rename still holds its values and
            // the rename map translates the physical name.
            let _ = insert_column(
                conn,
                row,
                existing.0,
                existing.1,
                &column.name,
                &column.dtype,
                column,
                sid,
            )?;
            let version = i64_from_u64(rename.version)?;
            insert_if_absent(
                conn,
                "writing a rename",
                "SELECT 1 FROM pinkdb_rename
                 WHERE table_id = ?1 AND version = ?2 AND from_name = ?3",
                params![row.table_id, version, rename.from],
                "INSERT INTO pinkdb_rename
                 (table_id, version, from_name, to_name) VALUES (?1, ?2, ?3, ?4)",
                params![row.table_id, version, rename.from, rename.to],
            )?;
            continue;
        }
        // An added column.
        max_column_id += 1;
        max_order += 1;
        if let Some((child_id, child_order)) = insert_column(
            conn,
            row,
            max_column_id,
            max_order,
            &column.name,
            &column.dtype,
            column,
            sid,
        )? {
            max_column_id = max_column_id.max(child_id);
            max_order = max_order.max(child_order);
        }
        altered = true;
        consumed.insert(column.name.clone());
    }
    for column in &live {
        if !consumed.contains(&column.2) && !target_names.contains(column.2.as_str()) {
            end_column(conn, row.table_id, column.0, sid)?;
            altered = true;
        }
    }
    // Persist the pink rename log even when the columns already match.
    for rename in renames {
        let version = i64_from_u64(rename.version)?;
        insert_if_absent(
            conn,
            "writing a rename",
            "SELECT 1 FROM pinkdb_rename
             WHERE table_id = ?1 AND version = ?2 AND from_name = ?3",
            params![row.table_id, version, rename.from],
            "INSERT INTO pinkdb_rename
             (table_id, version, from_name, to_name) VALUES (?1, ?2, ?3, ?4)",
            params![row.table_id, version, rename.from, rename.to],
        )?;
    }
    let _ = base;
    Ok(altered)
}

fn end_column(conn: &dyn SqlConnection, table_id: i64, column_id: i64, sid: i64) -> Result<()> {
    conn.execute(
        "UPDATE ducklake_column SET end_snapshot = ?1
         WHERE table_id = ?2 AND column_id = ?3 AND end_snapshot IS NULL",
        params![sid, table_id, column_id],
    )
    .map_err(|e| sql_error("ending a column", e))?;
    // A drop or type change takes the column's nested children with it.
    conn.execute(
        "UPDATE ducklake_column SET end_snapshot = ?1
         WHERE table_id = ?2 AND parent_column = ?3 AND end_snapshot IS NULL",
        params![sid, table_id, column_id],
    )
    .map_err(|e| sql_error("ending a column's children", e))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
/// Write one column's catalog rows and report the id and order its nested
/// `element` row took (an ARRAY is a DuckLake `list`), so the caller's id and
/// order sequences skip past it.
fn insert_column(
    conn: &dyn SqlConnection,
    row: &TableRow,
    column_id: i64,
    order: i64,
    name: &str,
    dtype: &str,
    column: &ColumnEntry,
    sid: i64,
) -> Result<Option<(i64, i64)>> {
    conn.execute(
        "INSERT INTO ducklake_column
         (column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name,
          column_type, initial_default, default_value, nulls_allowed, parent_column,
          default_value_type, default_value_dialect)
         VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, NULL, ?7, 1, NULL, 'literal', 'duckdb')",
        params![
            column_id,
            sid,
            row.table_id,
            order,
            name,
            ducklake_type(dtype),
            column.default
        ],
    )
    .map_err(|e| sql_error("inserting a column", e))?;
    conn.execute(
        "INSERT INTO pinkdb_column
         (table_id, column_id, begin_snapshot, dtype, default_value, added)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            row.table_id,
            column_id,
            sid,
            dtype,
            column.default,
            i64_from_u64(column.added)?
        ],
    )
    .map_err(|e| sql_error("inserting a column's side row", e))?;
    // DuckDB catalogs a nested type as child rows: `parent_column` points at
    // the parent's id, the child is named after the type's field (`element`),
    // and it takes the next column id and order slot. A DuckDB-written ARRAY
    // is a `list`, so that is what the parent row says above.
    let Some((elem, _)) = array_dtype_parts(dtype) else {
        return Ok(None);
    };
    let child_id: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(column_id), 0) + 1 FROM ducklake_column WHERE table_id = ?1",
            params![row.table_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| sql_error("allocating a nested column id", e))?
        .unwrap_or(1);
    let child_order = order + 1;
    conn.execute(
        "INSERT INTO ducklake_column
         (column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name,
          column_type, initial_default, default_value, nulls_allowed, parent_column,
          default_value_type, default_value_dialect)
         VALUES (?1, ?2, NULL, ?3, ?4, 'element', ?5, NULL, NULL, 1, ?6, 'literal', 'duckdb')",
        params![
            child_id,
            sid,
            row.table_id,
            child_order,
            ducklake_elem_type(elem),
            column_id
        ],
    )
    .map_err(|e| sql_error("inserting a nested column", e))?;
    Ok(Some((child_id, child_order)))
}

fn sync_partition_spec(
    conn: &dyn SqlConnection,
    row: &TableRow,
    target: &TableEntry,
    sid: i64,
    changes: &mut Vec<String>,
) -> Result<()> {
    let current: Option<i64> = conn
        .query_row(
            "SELECT partition_id FROM ducklake_partition_info
             WHERE table_id = ?1 AND begin_snapshot <= ?2
               AND (end_snapshot IS NULL OR end_snapshot > ?2)
             ORDER BY begin_snapshot DESC LIMIT 1",
            params![row.table_id, sid],
            |row| row.get(0),
        )
        .map_err(|e| sql_error("reading a partition specification", e))?;
    let Some(keys) = target.partition_by.as_ref().filter(|keys| !keys.is_empty()) else {
        if let Some(id) = current {
            conn.execute(
                "UPDATE ducklake_partition_info SET end_snapshot = ?1
                 WHERE table_id = ?2 AND partition_id = ?3 AND end_snapshot IS NULL",
                params![sid, row.table_id, id],
            )
            .map_err(|e| sql_error("ending a partition specification", e))?;
        }
        return Ok(());
    };
    if let Some(id) = current {
        conn.execute(
            "UPDATE ducklake_partition_info SET end_snapshot = ?1
             WHERE table_id = ?2 AND partition_id = ?3 AND end_snapshot IS NULL",
            params![sid, row.table_id, id],
        )
        .map_err(|e| sql_error("ending a partition specification", e))?;
    }
    let partition_id = query_one(
        conn,
        "SELECT COALESCE(MAX(partition_id), 0) + 1 FROM ducklake_partition_info",
        [],
        |row| row.get::<_, i64>(0),
    )
    .map_err(|e| sql_error("allocating a partition id", e))?;
    conn.execute(
        "INSERT INTO ducklake_partition_info
         (partition_id, table_id, begin_snapshot, end_snapshot) VALUES (?1, ?2, ?3, NULL)",
        params![partition_id, row.table_id, sid],
    )
    .map_err(|e| sql_error("inserting a partition specification", e))?;
    for (index, key) in keys.iter().enumerate() {
        let column_id: Option<i64> = conn
            .query_row(
                "SELECT column_id FROM ducklake_column
                 WHERE table_id = ?1 AND column_name = ?2
                   AND begin_snapshot <= ?3 AND (end_snapshot IS NULL OR end_snapshot > ?3)
                 ORDER BY begin_snapshot DESC LIMIT 1",
                params![row.table_id, key.column, sid],
                |row| row.get(0),
            )
            .map_err(|e| sql_error("looking up a partition column", e))?;
        conn.execute(
            "INSERT INTO ducklake_partition_column
             (partition_id, table_id, partition_key_index, column_id, \"transform\")
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                partition_id,
                row.table_id,
                index as i64,
                column_id,
                key.transform
            ],
        )
        .map_err(|e| sql_error("inserting a partition column", e))?;
    }
    changes.push(format!("altered_table:{}", row.table_id));
    Ok(())
}

fn current_partition_id(conn: &dyn SqlConnection, table_id: i64, sid: i64) -> Result<Option<i64>> {
    conn.query_row(
        "SELECT partition_id FROM ducklake_partition_info
         WHERE table_id = ?1 AND begin_snapshot <= ?2
           AND (end_snapshot IS NULL OR end_snapshot > ?2)
         ORDER BY begin_snapshot DESC LIMIT 1",
        params![table_id, sid],
        |row| row.get(0),
    )
    .map_err(|e| sql_error("reading the current partition id", e))
}

fn write_partition_values(
    conn: &dyn SqlConnection,
    row: &TableRow,
    file: &FileRef,
    file_id: i64,
    sid: i64,
) -> Result<()> {
    let Some(values) = file.partition() else {
        return Ok(());
    };
    let Some(keys) = load_partition_keys(conn, row.table_id, sid)? else {
        return Ok(());
    };
    for (index, key) in keys.iter().enumerate() {
        let value = values.get(&key.key_text()).cloned().flatten();
        conn.execute(
            "INSERT INTO ducklake_file_partition_value
             (data_file_id, table_id, partition_key_index, partition_value)
             VALUES (?1, ?2, ?3, ?4)",
            params![file_id, row.table_id, index as i64, value],
        )
        .map_err(|e| sql_error("inserting a file partition value", e))?;
    }
    Ok(())
}

fn write_file_stats(
    conn: &dyn SqlConnection,
    row: &TableRow,
    file: &FileRef,
    file_id: i64,
) -> Result<()> {
    let Some(block) = file.stats() else {
        return Ok(());
    };
    let rows = file.rows().unwrap_or(block.rows);
    let ids = column_ids(conn, row.table_id)?;
    for column in &block.columns {
        let Some(dtype) = stats::dtype_from_name(&column.dtype) else {
            continue;
        };
        let Some(column_id) = ids.get(&column.name).copied() else {
            continue;
        };
        let min = column
            .min
            .as_deref()
            .and_then(|v| canonical_to_ducklake(dtype, v));
        let max = column
            .max
            .as_deref()
            .and_then(|v| canonical_to_ducklake(dtype, v));
        let contains_nan =
            (dtype == pinkdb::DataType::Double).then_some(i64::from(column.nan.unwrap_or(0) > 0));
        conn.execute(
            "INSERT INTO ducklake_file_column_stats
             (data_file_id, table_id, column_id, column_size_bytes, value_count,
              null_count, min_value, max_value, contains_nan, extra_stats,
              min_is_exact, max_is_exact)
             VALUES (?1, ?2, ?3, 0, ?4, ?5, ?6, ?7, ?8, NULL, 1, 1)",
            params![
                file_id,
                row.table_id,
                column_id,
                i64_from_u64(rows.saturating_sub(column.nulls))?,
                i64_from_u64(column.nulls)?,
                min,
                max,
                contains_nan
            ],
        )
        .map_err(|e| sql_error("inserting file column statistics", e))?;
        for path in &column.paths {
            conn.execute(
                "INSERT INTO ducklake_file_variant_stats
                 (data_file_id, table_id, column_id, variant_path, shredded_type,
                  column_size_bytes, value_count, null_count, min_value, max_value,
                  contains_nan, extra_stats)
                 VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7, ?8, ?9, NULL, NULL)",
                params![
                    file_id,
                    row.table_id,
                    column_id,
                    path.path.clone(),
                    path.dtype.clone(),
                    i64_from_u64(rows.saturating_sub(path.nulls))?,
                    i64_from_u64(path.nulls)?,
                    path.min.clone(),
                    path.max.clone()
                ],
            )
            .map_err(|e| sql_error("inserting file variant statistics", e))?;
        }
    }
    Ok(())
}

fn upsert_table_side(
    conn: &dyn SqlConnection,
    row: &TableRow,
    key: &str,
    entry: &TableEntry,
) -> Result<()> {
    update_or_insert(
        conn,
        "writing a table's side row",
        "UPDATE pinkdb_table
         SET catalog_key = ?2, table_id_hex = ?3, format = ?4, inlined_limit = ?5,
             table_schema_version = ?6
         WHERE table_id = ?1",
        params![
            row.table_id,
            key,
            entry.table_id,
            entry.format().id(),
            entry.inlined_limit.map(i64_from_u64).transpose()?,
            i64_from_u64(entry.schema_version)?,
        ],
        "INSERT INTO pinkdb_table
         (table_id, catalog_key, table_id_hex, format, inlined_limit,
          table_schema_version)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            row.table_id,
            key,
            entry.table_id,
            entry.format().id(),
            entry.inlined_limit.map(i64_from_u64).transpose()?,
            i64_from_u64(entry.schema_version)?,
        ],
    )
}

fn write_table_stats(conn: &dyn SqlConnection, row: &TableRow, entry: &TableEntry) -> Result<()> {
    let deleted: u64 = entry.deletes.iter().map(|d| d.count).sum();
    let record_count = entry.rows.saturating_add(deleted);
    let file_size: u64 = entry
        .files
        .iter()
        .map(|f| file_size(conn, f.path()))
        .sum::<u64>();
    conn.execute(
        "DELETE FROM ducklake_table_stats WHERE table_id = ?1",
        params![row.table_id],
    )
    .map_err(|e| sql_error("replacing table statistics", e))?;
    conn.execute(
        "INSERT INTO ducklake_table_stats
         (table_id, record_count, next_row_id, file_size_bytes) VALUES (?1, ?2, ?3, ?4)",
        params![
            row.table_id,
            i64_from_u64(record_count)?,
            i64_from_u64(entry.next_row_id)?,
            i64_from_u64(file_size)?
        ],
    )
    .map_err(|e| sql_error("inserting table statistics", e))?;
    // Table-level column statistics: combine the live files' proven min/max
    // with the live inlined values. Exact enough for DuckDB's planner; the
    // engine's own answers come from the full `.pink` blocks on attach.
    conn.execute(
        "DELETE FROM ducklake_table_column_stats WHERE table_id = ?1",
        params![row.table_id],
    )
    .map_err(|e| sql_error("replacing table column statistics", e))?;
    let Some(columns) = entry.columns.as_deref() else {
        return Ok(());
    };
    let ids = column_ids(conn, row.table_id)?;
    for (index, column) in columns.iter().enumerate() {
        let Some(dtype) = crate::catalog::parse_dtype_name(&column.dtype) else {
            continue;
        };
        let Some(column_id) = ids.get(&column.name).copied() else {
            continue;
        };
        let mut values: Vec<String> = Vec::new();
        let mut contains_null = false;
        let mut contains_nan = false;
        for file in &entry.files {
            if let Some(block) = file.stats()
                && let Some(stat) = block.columns.get(index)
            {
                contains_null |= stat.nulls > 0;
                contains_nan |= stat.nan.unwrap_or(0) > 0;
                if let Some(min) = &stat.min {
                    values.push(min.clone());
                }
                if let Some(max) = &stat.max {
                    values.push(max.clone());
                }
            }
        }
        for inlined in entry.inlined.iter().filter(|r| r.live()) {
            if let Some(value) = inlined.values.get(index).and_then(|v| v.as_deref()) {
                values.push(value.to_string());
            } else {
                contains_null = true;
            }
        }
        let mut canonical: Vec<pinkdb::Value> = values
            .iter()
            .filter_map(|v| stats::decode_value(dtype, v))
            .collect();
        canonical.sort_by(compare_values);
        let min = canonical
            .first()
            .and_then(|v| stats::encode_value(dtype, v))
            .and_then(|v| canonical_to_ducklake(dtype, &v));
        let max = canonical
            .last()
            .and_then(|v| stats::encode_value(dtype, v))
            .and_then(|v| canonical_to_ducklake(dtype, &v));
        conn.execute(
            "INSERT INTO ducklake_table_column_stats
             (table_id, column_id, contains_null, contains_nan, min_value, max_value,
              extra_stats, min_is_exact, max_is_exact)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, 1, 1)",
            params![
                row.table_id,
                column_id,
                contains_null,
                if dtype == pinkdb::DataType::Double {
                    Some(i64::from(contains_nan))
                } else {
                    None
                },
                min,
                max
            ],
        )
        .map_err(|e| sql_error("inserting table column statistics", e))?;
    }
    Ok(())
}

fn write_inlined(
    conn: &dyn SqlConnection,
    row: &TableRow,
    base: Option<&TableEntry>,
    target: &TableEntry,
    sid: i64,
    lake_schema_version: u64,
    changes: &mut Vec<String>,
) -> Result<u64> {
    // New rows only: everything at or above the table's row-id high-water
    // mark, plus a row re-begun at this snapshot (an UPDATE that reuses an
    // existing row_id). This is O(delta), not O(inlined rows): the base list
    // is never diffed.
    let high_water = base.map(|b| b.next_row_id).unwrap_or(0);
    let current_version = pink_version(sid);
    let types = column_types(conn, row.table_id)?;
    let mut count = 0u64;
    for inlined in &target.inlined {
        let is_new =
            base.is_none() || inlined.row_id >= high_water || inlined.begin == current_version;
        if !is_new {
            continue;
        }
        let suffix = if inlined.begin == pink_version(sid) {
            lake_schema_version
        } else {
            snapshot_schema_version(conn, inlined.begin)?.unwrap_or(lake_schema_version)
        };
        let table_name = format!("ducklake_inlined_data_{}_{}", row.table_id, suffix);
        ensure_inlined_table(
            conn,
            row.table_id,
            &table_name,
            suffix,
            inlined,
            target,
            &types,
        )?;
        insert_inlined_row(conn, &table_name, inlined, &types, target)?;
        count += 1;
    }
    if count > 0 {
        changes.push(format!("inlined_insert:{count}"));
    }
    Ok(count)
}

/// End the inlined rows this commit's own target marks as ended at `sid`.
/// Reading the end snapshot from the delta (rather than diffing the base and
/// target lists) keeps the work O(ended rows), not O(inlined rows).
fn end_inlined(
    conn: &dyn SqlConnection,
    row: &TableRow,
    target: &TableEntry,
    sid: i64,
    changes: &mut Vec<String>,
) -> Result<u64> {
    let current_version = pink_version(sid);
    let mut count = 0u64;
    for ended in &target.inlined {
        if ended.end == Some(current_version) {
            end_inlined_row(conn, row, ended, sid)?;
            count += 1;
        }
    }
    if count > 0 {
        changes.push(format!("inlined_delete:{count}"));
    }
    Ok(count)
}

fn ensure_inlined_table(
    conn: &dyn SqlConnection,
    table_id: i64,
    table_name: &str,
    suffix: u64,
    inlined: &InlinedRow,
    target: &TableEntry,
    types: &BTreeMap<String, DataType>,
) -> Result<()> {
    if !table_exists(conn, table_name)? {
        // DuckDB stores an inlined column as BIGINT for the integer widths
        // (including BOOLEAN, as 0/1) and VARCHAR for everything else — the
        // declared type, not the value, decides. Create the table with the
        // whole schema, not just the columns this row happened to write.
        let mut columns: Vec<String> = Vec::new();
        let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        if let Some(schema) = &target.columns {
            for column in schema {
                if seen.insert(column.name.to_ascii_lowercase()) {
                    columns.push(column.name.clone());
                }
            }
        }
        for column in &inlined.columns {
            if seen.insert(column.to_ascii_lowercase()) {
                columns.push(column.clone());
            }
        }
        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (_ducklake_row_id BIGINT, \
             _ducklake_begin_snapshot BIGINT, _ducklake_end_snapshot BIGINT",
            quote_ident(table_name)
        );
        for column in &columns {
            sql.push_str(", ");
            sql.push_str(&quote_ident(column));
            sql.push(' ');
            sql.push_str(
                if column_is_integer_physical(conn, table_id, column, types)? {
                    "BIGINT"
                } else {
                    "VARCHAR"
                },
            );
        }
        sql.push(')');
        conn.execute_batch(&sql)
            .map_err(|e| sql_error("creating an inlined data table", e))?;
    }
    // SQLite gets the unique `(row_id, begin)` index; pinkdb's `CREATE INDEX`
    // grammar is single-column (and its declared constraints are metadata
    // only), so it gets the two access-path indexes the commit loop needs and
    // relies on the high-water/delta rule for uniqueness.
    let index_sql = match conn.dialect() {
        Dialect::Sqlite => format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}(_ducklake_row_id, _ducklake_begin_snapshot);
             CREATE INDEX IF NOT EXISTS {} ON {}(_ducklake_begin_snapshot);
             CREATE INDEX IF NOT EXISTS {} ON {}(_ducklake_end_snapshot);",
            quote_ident(&format!("pinkdb_inlined_row_{}_{}", table_id, suffix)),
            quote_ident(table_name),
            quote_ident(&format!("pinkdb_inlined_begin_{}_{}", table_id, suffix)),
            quote_ident(table_name),
            quote_ident(&format!("pinkdb_inlined_end_{}_{}", table_id, suffix)),
            quote_ident(table_name)
        ),
        Dialect::Postgres => format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}(_ducklake_row_id, _ducklake_begin_snapshot);
             CREATE INDEX IF NOT EXISTS {} ON {}(_ducklake_begin_snapshot);
             CREATE INDEX IF NOT EXISTS {} ON {}(_ducklake_end_snapshot);",
            quote_ident(&format!("pinkdb_inlined_row_{}_{}", table_id, suffix)),
            quote_ident(table_name),
            quote_ident(&format!("pinkdb_inlined_begin_{}_{}", table_id, suffix)),
            quote_ident(table_name),
            quote_ident(&format!("pinkdb_inlined_end_{}_{}", table_id, suffix)),
            quote_ident(table_name)
        ),
        Dialect::Pink => format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}(_ducklake_row_id);
             CREATE INDEX IF NOT EXISTS {} ON {}(_ducklake_begin_snapshot);
             CREATE INDEX IF NOT EXISTS {} ON {}(_ducklake_end_snapshot);",
            quote_ident(&format!("pinkdb_inlined_row_{}_{}", table_id, suffix)),
            quote_ident(table_name),
            quote_ident(&format!("pinkdb_inlined_begin_{}_{}", table_id, suffix)),
            quote_ident(table_name),
            quote_ident(&format!("pinkdb_inlined_end_{}_{}", table_id, suffix)),
            quote_ident(table_name)
        ),
    };
    conn.execute_batch(&index_sql)
        .map_err(|e| sql_error("indexing an inlined data table", e))?;
    let suffix_value = i64_from_u64(suffix)?;
    insert_if_absent(
        conn,
        "registering an inlined data table",
        "SELECT 1 FROM ducklake_inlined_data_tables
         WHERE table_id = ?1 AND schema_version = ?2",
        params![table_id, suffix_value],
        "INSERT INTO ducklake_inlined_data_tables (table_id, table_name, schema_version)
         VALUES (?1, ?2, ?3)",
        params![table_id, table_name, suffix_value],
    )?;
    let schema_version = i64_from_u64(inlined.schema_version)?;
    update_or_insert(
        conn,
        "recording an inlined table's schema version",
        "UPDATE pinkdb_inlined_schema SET table_schema_version = ?3
         WHERE table_id = ?1 AND suffix = ?2",
        params![table_id, suffix_value, schema_version],
        "INSERT INTO pinkdb_inlined_schema
         (table_id, suffix, table_schema_version) VALUES (?1, ?2, ?3)",
        params![table_id, suffix_value, schema_version],
    )?;
    Ok(())
}

/// DuckDB's physical inlined type: BIGINT for the integer logtypes it casts
/// to an integer, VARCHAR otherwise (including HUGEINT, UBIGINT, FLOAT,
/// DECIMAL, DATE, TIMESTAMP and INTERVAL).
fn column_is_integer_physical(
    conn: &dyn SqlConnection,
    table_id: i64,
    column: &str,
    types: &BTreeMap<String, DataType>,
) -> Result<bool> {
    let column_type: Option<String> = conn
        .query_row(
            "SELECT column_type FROM ducklake_column
             WHERE table_id = ?1 AND lower(column_name) = lower(?2)
             ORDER BY begin_snapshot DESC LIMIT 1",
            params![table_id, column],
            |row| row.get(0),
        )
        .map_err(|e| sql_error("reading an inlined column's type", e))?;
    if let Some(column_type) = column_type {
        return Ok(is_integer_ducklake_type(&column_type));
    }
    Ok(matches!(
        types.get(&column.to_ascii_lowercase()),
        Some(DataType::Boolean | DataType::Integer | DataType::Bigint)
    ))
}

fn is_integer_ducklake_type(column_type: &str) -> bool {
    matches!(
        column_type.to_ascii_lowercase().as_str(),
        "boolean" | "int8" | "int16" | "int32" | "int64" | "uint8" | "uint16" | "uint32"
    )
}

fn insert_inlined_row(
    conn: &dyn SqlConnection,
    table_name: &str,
    inlined: &InlinedRow,
    types: &BTreeMap<String, DataType>,
    target: &TableEntry,
) -> Result<()> {
    let mut columns = vec![
        "_ducklake_row_id".to_string(),
        "_ducklake_begin_snapshot".to_string(),
        "_ducklake_end_snapshot".to_string(),
    ];
    columns.extend(inlined.columns.iter().cloned());
    let mut sql = match conn.dialect() {
        Dialect::Sqlite => String::from("INSERT OR REPLACE INTO "),
        Dialect::Pink | Dialect::Postgres => String::from("INSERT INTO "),
    };
    sql.push_str(&quote_ident(table_name));
    sql.push_str(" (");
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&quote_ident(column));
    }
    sql.push_str(") VALUES (");
    for index in 1..=columns.len() {
        if index > 1 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("?{index}"));
    }
    sql.push(')');
    if conn.dialect() == Dialect::Postgres {
        // The typed table's `(row_id, begin_snapshot)` unique key is the
        // DuckLake identity of one inlined row version; a re-begun row
        // replaces its values exactly as SQLite's `INSERT OR REPLACE` does.
        // `(SELECT ...)` would not work here: the SET targets are columns.
        let mut first = true;
        for column in &columns[2..] {
            if first {
                sql.push_str(
                    " ON CONFLICT (_ducklake_row_id, _ducklake_begin_snapshot) DO UPDATE SET ",
                );
                first = false;
            } else {
                sql.push_str(", ");
            }
            sql.push_str(&quote_ident(column));
            sql.push_str(" = EXCLUDED.");
            sql.push_str(&quote_ident(column));
        }
    }
    let mut values: Vec<SqlValue> = Vec::with_capacity(columns.len());
    values.push(SqlValue::Integer(i64_from_u64(inlined.row_id)?));
    values.push(SqlValue::Integer(snapshot_id(inlined.begin)?));
    values.push(match inlined.end {
        Some(end) => SqlValue::Integer(snapshot_id(end)?),
        None => SqlValue::Null,
    });
    for (index, column) in inlined.columns.iter().enumerate() {
        let dtype = types
            .get(&column.to_ascii_lowercase())
            .copied()
            .or_else(|| {
                target.columns.as_ref().and_then(|columns| {
                    columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(column))
                        .and_then(|c| DataType::from_sql(&c.dtype))
                })
            });
        values.push(canonical_sql_value(dtype, inlined.values.get(index))?);
    }
    conn.execute(&sql, &values)
        .map_err(|e| sql_error("inserting an inlined row", e))?;
    Ok(())
}

fn canonical_sql_value(
    dtype: Option<DataType>,
    canonical: Option<&Option<String>>,
) -> Result<SqlValue> {
    use SqlValue;
    let Some(canonical) = canonical.and_then(|v| v.as_deref()) else {
        return Ok(SqlValue::Null);
    };
    let Some(dtype) = dtype else {
        // An unknown historical column: keep the canonical text. It is not
        // part of the table's current schema, so no reader decodes it.
        return Ok(SqlValue::Text(canonical.to_string()));
    };
    Ok(match dtype {
        DataType::Boolean => SqlValue::Integer(i64::from(matches!(canonical, "true" | "1"))),
        DataType::Integer => {
            SqlValue::Integer(canonical.parse::<i32>().map(i64::from).unwrap_or_default())
        }
        DataType::Bigint => SqlValue::Integer(canonical.parse::<i64>().unwrap_or_default()),
        DataType::Double
        | DataType::Date
        | DataType::Timestamp
        | DataType::Varchar
        | DataType::Decimal { .. }
        | DataType::Interval
        | DataType::Variant
        | DataType::Array { .. }
        | DataType::Uuid => match canonical_to_ducklake(dtype, canonical) {
            Some(text) => SqlValue::Text(text),
            None => SqlValue::Text(canonical.to_string()),
        },
    })
}

fn snapshot_schema_version(conn: &dyn SqlConnection, version: u64) -> Result<Option<u64>> {
    let sid = snapshot_id(version)?;
    conn.query_row(
        "SELECT schema_version FROM ducklake_snapshot WHERE snapshot_id = ?1",
        params![sid],
        |row| row.get::<_, i64>(0),
    )
    .map_err(|e| sql_error("reading a snapshot's schema version", e))
    .map(|v| v.map(i64_to_u64))
}

// ============================== small helpers ==============================

/// The `ducklake_column.column_id`s that writing a data file for `key` must
/// stamp as parquet `field_id`s, in `columns` order.
///
/// Data files are written before the catalog commit, so the ids cannot be
/// read back afterwards: they are planned here with exactly the rule
/// [`sync_columns`] applies. A live column of that name keeps its id (ids
/// never move, which is what makes them safe across `DROP`/`ADD COLUMN`); a
/// column that is not live yet takes the next id in column order, and an
/// ARRAY takes one more for its `element` child, because `insert_column`
/// allocates the child as `MAX(column_id) + 1` right after its parent.
fn planned_field_ids(
    conn: &dyn SqlConnection,
    key: &str,
    columns: &[(String, pinkdb::DataType)],
    label: &Path,
) -> Result<Vec<crate::format::FieldIds>> {
    use crate::format::FieldIds;
    let (schema_name, table_name) = split_key(key, label)?;
    let table_id: Option<i64> = conn
        .query_row(
            "SELECT t.table_id FROM ducklake_table t
             JOIN ducklake_schema s ON s.schema_id = t.schema_id
             WHERE s.schema_name = ?1 AND t.table_name = ?2
               AND t.begin_snapshot <= (SELECT MAX(snapshot_id) FROM ducklake_snapshot)
               AND (t.end_snapshot IS NULL
                    OR t.end_snapshot > (SELECT MAX(snapshot_id) FROM ducklake_snapshot))
             ORDER BY t.table_id DESC LIMIT 1",
            params![schema_name, table_name],
            |row| row.get::<_, i64>(0),
        )
        .ok()
        .flatten();
    let Some(table_id) = table_id else {
        // A table the catalog does not know yet: it is created by this very
        // commit, which numbers the columns from 1 in column order.
        return Ok(fresh_field_ids(columns, 0));
    };
    let mut live: std::collections::BTreeMap<String, (i64, Option<i64>)> =
        std::collections::BTreeMap::new();
    {
        let stmt = conn
            .prepare(
                "SELECT column_name, column_id, parent_column FROM ducklake_column
                 WHERE table_id = ?1 AND parent_column IS NULL
                 ORDER BY column_order, column_id",
            )
            .map_err(|e| sql_error("reading column ids", e))?;
        let rows = stmt
            .query_map(params![table_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            })
            .map_err(|e| sql_error("reading column ids", e))?;
        for row in rows {
            let (name, id, parent) = row.map_err(|e| sql_error("reading column ids", e))?;
            if parent.is_none() {
                live.insert(name, (id, None));
            }
        }
    }
    // An ARRAY's child id, from the live child row (`insert_column`
    // allocates it as `MAX(column_id) + 1`, so it is the parent's + 1, but
    // reading it is what keeps this planner independent of that).
    let parents: Vec<(String, i64)> = live.iter().map(|(n, (id, _))| (n.clone(), *id)).collect();
    for (name, parent_id) in parents {
        let child: Option<i64> = conn
            .query_row(
                "SELECT column_id FROM ducklake_column
                 WHERE table_id = ?1 AND parent_column = ?2
                 ORDER BY column_id LIMIT 1",
                params![table_id, parent_id],
                |row| row.get::<_, i64>(0),
            )
            .ok()
            .flatten();
        if let Some(child) = child
            && let Some(entry) = live.get_mut(&name)
        {
            entry.1 = Some(child);
        }
    }
    let max_id: i64 = conn
        .query_row(
            "SELECT MAX(column_id) FROM ducklake_column WHERE table_id = ?1",
            params![table_id],
            |row| row.get::<_, Option<i64>>(0),
        )
        .map_err(|e| sql_error("reading the column id sequence", e))?
        .flatten()
        .unwrap_or(0)
        .max(0);
    let mut next = max_id;
    let mut out = Vec::with_capacity(columns.len());
    for (name, _) in columns {
        if let Some((id, child)) = live.get(name) {
            out.push(FieldIds {
                column: *id,
                array_child: *child,
            });
            continue;
        }
        // The same running counter `sync_columns` keeps for added columns.
        next += 1;
        out.push(FieldIds {
            column: next,
            array_child: None,
        });
        if columns
            .iter()
            .any(|(n, dtype)| n == name && matches!(dtype, pinkdb::DataType::Array { .. }))
        {
            next += 1;
            out.last_mut().expect("just pushed").array_child = Some(next);
        }
    }
    Ok(out)
}

/// [`planned_field_ids`] for a table the catalog does not know yet: the
/// commit numbers its columns from 1, children included.
fn fresh_field_ids(
    columns: &[(String, pinkdb::DataType)],
    start: i64,
) -> Vec<crate::format::FieldIds> {
    use crate::format::FieldIds;
    let mut next = start;
    let mut out = Vec::with_capacity(columns.len());
    for (_, dtype) in columns {
        next += 1;
        let mut ids = FieldIds {
            column: next,
            array_child: None,
        };
        if matches!(dtype, pinkdb::DataType::Array { .. }) {
            next += 1;
            ids.array_child = Some(next);
        }
        out.push(ids);
    }
    out
}

fn split_key(key: &str, label: &Path) -> Result<(String, String)> {
    key.split_once('.')
        .map(|(db, table)| (db.to_string(), table.to_string()))
        .ok_or_else(|| LakeError::Corrupt {
            path: label.to_path_buf(),
            message: format!("catalog key \"{key}\" has no database part"),
        })
}

/// Compose a DuckLake schema/table/file into pinkdb's lake-relative path.
///
/// Pink's data files always live under `data/` (the engine's loader and the
/// maintenance scanner enforce that prefix). A pink-written catalog records
/// its schema path as `data/` directly; a DuckDB-written DuckLake records the
/// schema name (`main/`) and expects the data path to be the parent of that
/// directory. Prefix `data/` only when the schema path does not already carry
/// it, so both layouts resolve to `<data root>/data/...` when the caller opens
/// with the fixture's parent directory.
fn compose_path(schema_path: &str, table_path: &str, file_path: &str) -> String {
    let schema_path = if schema_path.is_empty() || schema_path.starts_with("data/") {
        schema_path.to_string()
    } else {
        format!("data/{}", schema_path.trim_start_matches('/'))
    };
    let mut out = String::new();
    for part in [schema_path.as_str(), table_path, file_path] {
        if part.is_empty() {
            continue;
        }
        if !out.is_empty() && !out.ends_with('/') {
            out.push('/');
        }
        out.push_str(part.trim_start_matches('/'));
    }
    out
}

fn path_basename(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

fn table_columns(conn: &dyn SqlConnection, table: &str) -> Result<Vec<String>> {
    // SQLite exposes the catalog through `PRAGMA table_info`, pinkdb through
    // its DuckDB-shaped `DESCRIBE`, and Postgres through
    // `information_schema.columns`. All three are metadata reads.
    match conn.dialect() {
        Dialect::Sqlite => {
            let sql = format!("PRAGMA table_info({})", quote_ident(table));
            let stmt = conn
                .prepare(&sql)
                .map_err(|e| sql_error("reading an inlined table's columns", e))?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(1))
                .map_err(|e| sql_error("reading an inlined table's columns", e))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(|e| sql_error("reading an inlined table's columns", e))?);
            }
            Ok(out)
        }
        Dialect::Pink => {
            let sql = format!("DESCRIBE {}", quote_ident(table));
            let stmt = conn
                .prepare(&sql)
                .map_err(|e| sql_error("reading an inlined table's columns", e))?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(|e| sql_error("reading an inlined table's columns", e))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(|e| sql_error("reading an inlined table's columns", e))?);
            }
            Ok(out)
        }
        Dialect::Postgres => {
            let stmt = conn
                .prepare(
                    "SELECT column_name FROM information_schema.columns
                     WHERE table_name = ?1 AND table_schema = current_schema()
                     ORDER BY ordinal_position",
                )
                .map_err(|e| sql_error("reading an inlined table's columns", e))?;
            let rows = stmt
                .query_map(params![table], |row| row.get::<_, String>(0))
                .map_err(|e| sql_error("reading an inlined table's columns", e))?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(|e| sql_error("reading an inlined table's columns", e))?);
            }
            Ok(out)
        }
    }
}

fn table_exists(conn: &dyn SqlConnection, name: &str) -> Result<bool> {
    match conn.dialect() {
        Dialect::Sqlite => {
            let found: Option<i64> = conn
                .query_row(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    params![name],
                    |row| row.get(0),
                )
                .map_err(|e| sql_error("checking a table", e))?;
            Ok(found.is_some())
        }
        // pinkdb has no `sqlite_master`; its catalog is the DuckDB-shaped
        // `SHOW TABLES` list (short, and only consulted when a typed inlined
        // table is created or ended).
        Dialect::Pink => {
            let rows = conn
                .query("SHOW TABLES", &[])
                .map_err(|e| sql_error("checking a table", e))?;
            for row in &rows {
                let listed: String = row.get(0)?;
                if listed.eq_ignore_ascii_case(name) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Dialect::Postgres => {
            let found: Option<String> = conn
                .query_row(
                    "SELECT table_name FROM information_schema.tables
                     WHERE table_name = ?1 AND table_schema = current_schema() LIMIT 1",
                    params![name],
                    |row| row.get(0),
                )
                .map_err(|e| sql_error("checking a table", e))?;
            Ok(found.is_some())
        }
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn with_trailing_slash(location: &str) -> String {
    if location.ends_with('/') {
        location.to_string()
    } else {
        format!("{location}/")
    }
}

/// The inverse of [`with_trailing_slash`]: the form pinkdb's `Location` uses.
fn trim_trailing_slash(location: &str) -> String {
    if location.len() > 1 && location.ends_with('/') {
        location.trim_end_matches('/').to_string()
    } else {
        location.to_string()
    }
}

fn ducklake_time(iso: &str) -> String {
    // DuckLake's `snapshot_time` is a timestamp string. Pink's manifests use
    // ISO-8601 UTC (`2026-10-23T11:09:19.127Z`); render the same instant in
    // DuckLake's `YYYY-MM-DD HH:MM:SS.mmm+00:00` shape.
    let trimmed = iso.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let (date, time) = trimmed
        .split_once('T')
        .or_else(|| trimmed.split_once(' '))
        .unwrap_or((trimmed, ""));
    let time = time.trim_end_matches('Z');
    if time.is_empty() {
        date.to_string()
    } else {
        format!("{date} {time}+00:00")
    }
}

fn ducklake_type(dtype: &str) -> String {
    let upper = dtype.to_ascii_uppercase();
    if upper.starts_with("DECIMAL(") {
        return upper.to_ascii_lowercase();
    }
    // DuckDB has no ARRAY column type in DuckLake: it refuses to create one.
    // What it can read is the LIST shape it writes itself, so a pinkdb ARRAY
    // is catalogued as `list` with an `element` child row (see
    // [`insert_column`]) and DuckDB reads it back as a variable-length list.
    if let Some((elem, _)) = array_dtype_parts(&upper) {
        let _ = elem;
        return "list".to_string();
    }
    match upper.as_str() {
        "BOOLEAN" => "boolean".to_string(),
        "INTEGER" => "int32".to_string(),
        "BIGINT" => "int64".to_string(),
        "DOUBLE" => "float64".to_string(),
        "VARCHAR" => "varchar".to_string(),
        "DATE" => "date".to_string(),
        "TIMESTAMP" => "timestamp".to_string(),
        "INTERVAL" => "interval".to_string(),
        other => other.to_ascii_lowercase(),
    }
}

fn pink_type_name(ducklake: &str) -> String {
    pink_dtype(ducklake).name()
}

/// `DOUBLE[3]` → `(ArrayElem::Double, 3)`, for the catalogued type text.
fn array_dtype_parts(dtype: &str) -> Option<(pinkdb::types::ArrayElem, u32)> {
    let upper = dtype.to_ascii_uppercase();
    match pinkdb::DataType::from_sql(&upper)? {
        pinkdb::DataType::Array { elem, len } => Some((elem, len)),
        _ => None,
    }
}

/// The DuckLake spelling of one ARRAY element type, as DuckDB writes the
/// `element` child row of a LIST.
fn ducklake_elem_type(elem: pinkdb::types::ArrayElem) -> &'static str {
    match elem {
        pinkdb::types::ArrayElem::Integer => "int32",
        pinkdb::types::ArrayElem::Bigint => "int64",
        pinkdb::types::ArrayElem::Float => "float32",
        pinkdb::types::ArrayElem::Double => "float64",
    }
}

fn pink_dtype(ducklake: &str) -> pinkdb::DataType {
    let lower = ducklake.to_ascii_lowercase();
    if let Some(rest) = lower.strip_prefix("decimal(") {
        if let Some((p, s)) = rest.trim_end_matches(')').split_once(',')
            && let (Ok(p), Ok(s)) = (p.trim().parse::<u8>(), s.trim().parse::<u8>())
            && let Some(dtype) = pinkdb::types::decimal_type(p, s)
        {
            return dtype;
        }
        return pinkdb::DataType::Varchar;
    }
    match lower.as_str() {
        "boolean" => pinkdb::DataType::Boolean,
        "int8" | "int16" | "int32" | "uint8" | "uint16" | "uint32" => pinkdb::DataType::Integer,
        "int64" | "uint64" => pinkdb::DataType::Bigint,
        "float32" | "float64" | "double" => pinkdb::DataType::Double,
        "date" => pinkdb::DataType::Date,
        "timestamp" | "timestamp_us" | "timestamp_s" | "timestamp_ms" | "timestamp_ns" => {
            pinkdb::DataType::Timestamp
        }
        "interval" => pinkdb::DataType::Interval,
        "variant" => pinkdb::DataType::Variant,
        "uuid" => pinkdb::DataType::Uuid,
        _ => pinkdb::DataType::Varchar,
    }
}

fn canonical_to_ducklake(dtype: pinkdb::DataType, canonical: &str) -> Option<String> {
    Some(match dtype {
        pinkdb::DataType::Boolean => canonical.to_string(),
        pinkdb::DataType::Integer | pinkdb::DataType::Bigint => canonical.to_string(),
        pinkdb::DataType::Date => {
            let days: i32 = canonical.parse().ok()?;
            format_ducklake_date(days)
        }
        pinkdb::DataType::Timestamp => {
            let micros: i64 = canonical.parse().ok()?;
            format_ducklake_timestamp(micros)
        }
        pinkdb::DataType::Double => {
            if canonical.len() == 16 && canonical.bytes().all(|b| b.is_ascii_hexdigit()) {
                let bits = u64::from_str_radix(canonical, 16).ok()?;
                format_ducklake_double(f64::from_bits(bits))
            } else {
                canonical.to_string()
            }
        }
        pinkdb::DataType::Varchar => canonical.to_string(),
        // The canonical statistic text is already the UUID text form.
        pinkdb::DataType::Uuid => canonical.to_string(),
        pinkdb::DataType::Decimal { .. }
        | pinkdb::DataType::Interval
        | pinkdb::DataType::Variant
        | pinkdb::DataType::Array { .. } => canonical.to_string(),
    })
}

fn ducklake_to_canonical(dtype: pinkdb::DataType, ducklake: &str) -> Option<String> {
    Some(match dtype {
        pinkdb::DataType::Boolean => match ducklake {
            "true" | "1" => "true".to_string(),
            "false" | "0" => "false".to_string(),
            _ => return None,
        },
        pinkdb::DataType::Integer => ducklake.parse::<i32>().ok()?.to_string(),
        pinkdb::DataType::Bigint => ducklake.parse::<i64>().ok()?.to_string(),
        pinkdb::DataType::Date => parse_ducklake_date(ducklake)?.to_string(),
        pinkdb::DataType::Timestamp => parse_ducklake_timestamp(ducklake)?.to_string(),
        pinkdb::DataType::Double => {
            let value: f64 = ducklake.parse().ok()?;
            format!("{:016x}", value.to_bits())
        }
        pinkdb::DataType::Varchar => ducklake.to_string(),
        pinkdb::DataType::Uuid => pinkdb::uuid::format(pinkdb::uuid::parse(ducklake)?),
        pinkdb::DataType::Decimal { precision, scale } => {
            let value = pinkdb::types::parse_decimal_str(ducklake, precision, scale).ok()?;
            pinkdb::types::format_decimal(value, scale)
        }
        pinkdb::DataType::Interval => {
            let value = crate::stats::decode_value(dtype, ducklake)?;
            crate::stats::encode_value(dtype, &value)?
        }
        // VARIANT has no canonical text of its own (no min/max statistics are
        // written for it); its JSON text passes through unchanged.
        pinkdb::DataType::Variant => ducklake.to_string(),
        // Likewise an ARRAY, which writes no statistics at all.
        pinkdb::DataType::Array { .. } => ducklake.to_string(),
    })
}

/// DuckDB's DOUBLE → VARCHAR shape. Rust's debug form is the shortest
/// round-tripping text (including `-0.0`); only NaN is spelled differently.
fn format_ducklake_double(value: f64) -> String {
    if value.is_nan() {
        "nan".to_string()
    } else {
        format!("{value:?}")
    }
}

/// DuckDB's DATE → VARCHAR shape: four-digit AD years, unpadded later years,
/// and `(BC)` on the far side of the epoch.
fn format_ducklake_date(days: i32) -> String {
    let (year, month, day) = pinkdb::types::civil_from_days(days);
    if year >= 1 {
        format!("{year:04}-{month:02}-{day:02}")
    } else {
        format!("{:04}-{month:02}-{day:02} (BC)", 1 - year)
    }
}

fn parse_ducklake_date(text: &str) -> Option<i32> {
    let text = text.trim();
    let (body, bc) = match text.strip_suffix(" (BC)") {
        Some(body) => (body, true),
        None => (text, false),
    };
    parse_ducklake_date_body(body, bc)
}

fn parse_ducklake_date_body(body: &str, bc: bool) -> Option<i32> {
    let (negative, body) = match body.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, body),
    };
    let mut parts = body.split('-');
    let year: i32 = parts.next()?.parse().ok()?;
    let month: u32 = parts.next()?.parse().ok()?;
    let day: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let year = if negative { -year } else { year };
    // DuckDB's `(BC)` year counts backwards from 1 BC = astronomical year 0.
    let year = if bc { 1 - year } else { year };
    Some(pinkdb::types::days_from_civil(year, month, day))
}

fn format_ducklake_timestamp(micros: i64) -> String {
    let (days, hour, minute, second, micro) = pinkdb::types::split_timestamp(micros);
    let date = format_ducklake_date(days);
    if micro == 0 {
        format!("{date} {hour:02}:{minute:02}:{second:02}")
    } else {
        format!("{date} {hour:02}:{minute:02}:{second:02}.{micro:06}")
    }
}

fn parse_ducklake_timestamp(text: &str) -> Option<i64> {
    let text = text.trim();
    let (date, rest, bc) = match text.find(" (BC)") {
        Some(index) => {
            let after = index + " (BC)".len();
            (&text[..index], &text[after..], true)
        }
        None => {
            let (date, time) = text.split_once(' ').or_else(|| text.split_once('T'))?;
            (date, time, false)
        }
    };
    let days = parse_ducklake_date_body(date, bc)? as i64;
    let time = rest.trim_start();
    let (hms, fraction) = match time.split_once('.') {
        Some((hms, fraction)) => (hms, Some(fraction)),
        None => (time, None),
    };
    if hms.len() != 8 || hms.as_bytes()[2] != b':' || hms.as_bytes()[5] != b':' {
        return None;
    }
    let hour: i64 = hms[0..2].parse().ok()?;
    let minute: i64 = hms[3..5].parse().ok()?;
    let second: i64 = hms[6..8].parse().ok()?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let micros = match fraction {
        None => 0,
        Some(fraction) => {
            if fraction.is_empty()
                || fraction.len() > 6
                || !fraction.bytes().all(|b| b.is_ascii_digit())
            {
                return None;
            }
            format!("{fraction:0<6}").parse::<i64>().ok()?
        }
    };
    let total = (days as i128) * 86_400_000_000
        + (hour * 3_600_000_000 + minute * 60_000_000 + second * 1_000_000 + micros) as i128;
    i64::try_from(total).ok()
}

fn sql_to_canonical(dtype: Option<&pinkdb::DataType>, value: &SqlValue) -> Option<String> {
    use SqlValue;
    let Some(dtype) = dtype else {
        return match value {
            SqlValue::Null | SqlValue::Blob(_) => None,
            SqlValue::Integer(v) => Some(v.to_string()),
            SqlValue::Real(v) => Some(v.to_string()),
            SqlValue::Text(v) => Some(v.clone()),
        };
    };
    match value {
        SqlValue::Null | SqlValue::Blob(_) => None,
        SqlValue::Integer(v) => Some(match dtype {
            pinkdb::DataType::Boolean => (if *v == 0 { "false" } else { "true" }).to_string(),
            pinkdb::DataType::Double => format!("{:016x}", (*v as f64).to_bits()),
            _ => v.to_string(),
        }),
        SqlValue::Real(v) => Some(match dtype {
            pinkdb::DataType::Double => format!("{:016x}", v.to_bits()),
            _ => v.to_string(),
        }),
        SqlValue::Text(v) => Some(ducklake_to_canonical(*dtype, v).unwrap_or_else(|| v.clone())),
    }
}

fn compare_values(a: &pinkdb::Value, b: &pinkdb::Value) -> std::cmp::Ordering {
    use pinkdb::Value;
    match (a, b) {
        (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
        (Value::Bigint(a), Value::Bigint(b)) => a.cmp(b),
        (Value::Double(a), Value::Double(b)) => a.total_cmp(b),
        (Value::Varchar(a), Value::Varchar(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
        (Value::Decimal { value: a, .. }, Value::Decimal { value: b, .. }) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}

fn file_size(conn: &dyn SqlConnection, path: &str) -> u64 {
    let data_path: Option<String> = conn
        .query_row(
            "SELECT value FROM ducklake_metadata WHERE key = 'data_path'",
            [],
            |row| row.get(0),
        )
        .ok()
        .flatten();
    let Some(data_path) = data_path else {
        return 0;
    };
    if data_path.starts_with("s3://") {
        return 0;
    }
    let full = Path::new(&data_path).join(path);
    std::fs::metadata(full).map(|m| m.len()).unwrap_or(0)
}

fn uuid_to_hex(uuid: &str) -> Option<String> {
    let hex: String = uuid.chars().filter(|c| *c != '-').collect();
    (hex.len() == 32 && hex.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| hex.to_ascii_lowercase())
}

fn hex_to_uuid(hex: &str) -> Option<String> {
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let lower = hex.to_ascii_lowercase();
    Some(format!(
        "{}-{}-{}-{}-{}",
        &lower[0..8],
        &lower[8..12],
        &lower[12..16],
        &lower[16..20],
        &lower[20..32]
    ))
}

/// DuckLake ids and counters are `BIGINT`. A `u64` above `i64::MAX` cannot be
/// represented; refuse it instead of clamping (a clamped id silently points at
/// the wrong row).
fn i64_from_u64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| LakeError::Corrupt {
        path: PathBuf::from("catalog"),
        message: format!("catalog value {value} does not fit DuckLake's BIGINT column"),
    })
}

fn i64_to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

/// Write the engine catalog image row (wave 96b), inserting it the first
/// time. Runs on the commit's connection so the image lands in the commit's
/// transaction.
fn write_engine_catalog(conn: &dyn SqlConnection, revision: u64, image: &[u8]) -> Result<()> {
    let revision = i64_from_u64(revision)?;
    update_or_insert(
        conn,
        "writing the engine catalog image",
        "UPDATE pinkdb_engine_catalog SET revision = ?1, encoded = ?2 WHERE id = 1",
        params![revision, image],
        "INSERT INTO pinkdb_engine_catalog (id, revision, encoded) VALUES (1, ?1, ?2)",
        params![revision, image],
    )
}

/// Update one row by key, inserting it when it is absent. Both backends
/// support `UPDATE` and `INSERT`; pinkdb's grammar has no `ON CONFLICT … DO
/// UPDATE`, so the catalog spells an upsert as this two-step in the same
/// transaction (the key lookup is a primary-key/index hit, never a scan).
fn update_or_insert(
    conn: &dyn SqlConnection,
    context: &str,
    update_sql: &str,
    update_params: &[SqlValue],
    insert_sql: &str,
    insert_params: &[SqlValue],
) -> Result<()> {
    let changed = conn
        .execute(update_sql, update_params)
        .map_err(|e| sql_error(context, e))?;
    if changed == 0 {
        conn.execute(insert_sql, insert_params)
            .map_err(|e| sql_error(context, e))?;
    }
    Ok(())
}

/// Insert a row unless its key already exists (`INSERT OR IGNORE`'s contract,
/// spelled with a keyed existence probe because pinkdb has no ignore clause).
fn insert_if_absent(
    conn: &dyn SqlConnection,
    context: &str,
    exists_sql: &str,
    exists_params: &[SqlValue],
    insert_sql: &str,
    insert_params: &[SqlValue],
) -> Result<bool> {
    let found: Option<i64> = conn
        .query_row(exists_sql, exists_params, |row| row.get(0))
        .map_err(|e| sql_error(context, e))?;
    if found.is_some() {
        return Ok(false);
    }
    conn.execute(insert_sql, insert_params)
        .map_err(|e| sql_error(context, e))?;
    Ok(true)
}

/// Add context to a backend error without ever masking a retryable conflict:
/// the commit path's `COMMIT` maps pinkdb's lease/write conflict to
/// [`LakeError::Conflict`], and the lake's rebase loop depends on it.
fn sql_error(context: &str, error: LakeError) -> LakeError {
    match error {
        conflict @ LakeError::Conflict { .. } => conflict,
        other => LakeError::Io {
            context: context.to_string(),
            source: std::io::Error::other(other.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::super::dialect::DUCKLAKE_TABLES;
    use super::*;

    #[test]
    fn the_ddl_lists_every_ducklake_table() {
        let sql = Dialect::Sqlite.ducklake_ddl();
        for table in DUCKLAKE_TABLES {
            let name = table.split('(').next().unwrap();
            assert!(
                sql.contains(&format!("CREATE TABLE IF NOT EXISTS {name}(")),
                "{name}"
            );
        }
    }

    #[test]
    fn path_composition_matches_the_reference() {
        assert_eq!(
            compose_path("main/", "t/", "x.parquet"),
            "data/main/t/x.parquet"
        );
        assert_eq!(
            compose_path("data/", "main.t/", "x.pink"),
            "data/main.t/x.pink"
        );
    }

    #[test]
    fn type_names_round_trip() {
        for (canonical, ducklake) in [
            ("INTEGER", "int32"),
            ("BIGINT", "int64"),
            ("DOUBLE", "float64"),
            ("VARCHAR", "varchar"),
            ("DATE", "date"),
            ("TIMESTAMP", "timestamp"),
            ("BOOLEAN", "boolean"),
            ("DECIMAL(10,2)", "decimal(10,2)"),
        ] {
            assert_eq!(ducklake_type(canonical), ducklake);
            assert_eq!(pink_type_name(ducklake), canonical);
        }
    }

    #[test]
    fn ducklake_values_round_trip() {
        use pinkdb::DataType;
        for (dtype, canonical) in [
            (DataType::Integer, "42".to_string()),
            (DataType::Bigint, "-9000".to_string()),
            (DataType::Double, format!("{:016x}", 1.5f64.to_bits())),
            (DataType::Double, format!("{:016x}", (-0.0f64).to_bits())),
            (DataType::Double, format!("{:016x}", f64::NAN.to_bits())),
            (
                DataType::Double,
                format!("{:016x}", f64::INFINITY.to_bits()),
            ),
            (
                DataType::Double,
                format!("{:016x}", f64::NEG_INFINITY.to_bits()),
            ),
            (DataType::Date, "0".to_string()),
            (DataType::Date, i32::MIN.to_string()),
            (DataType::Date, i32::MAX.to_string()),
            (DataType::Timestamp, "1000000".to_string()),
            (DataType::Timestamp, i64::MIN.to_string()),
            (DataType::Timestamp, i64::MAX.to_string()),
            (DataType::Varchar, "héllo 🌸".to_string()),
            (DataType::Boolean, "true".to_string()),
            (
                DataType::Decimal {
                    precision: 18,
                    scale: 3,
                },
                pinkdb::types::format_decimal(-999_999_999_999_999_999, 3),
            ),
            (
                DataType::Interval,
                pinkdb::types::format_interval(-13, -2, -3),
            ),
        ] {
            let text = canonical_to_ducklake(dtype, &canonical).unwrap();
            assert_eq!(
                ducklake_to_canonical(dtype, &text).as_deref(),
                Some(canonical.as_str()),
                "{dtype:?} {canonical}"
            );
        }
    }

    #[test]
    fn iso_timestamps_render_in_ducklake_shape() {
        assert_eq!(
            ducklake_time("2026-10-23T11:09:19.127Z"),
            "2026-10-23 11:09:19.127+00:00"
        );
    }
}
