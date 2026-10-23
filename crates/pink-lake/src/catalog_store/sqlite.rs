//! The SQLite catalog store (wave 96a).
//!
//! One lake per SQLite file: `pink_lake_meta` holds the lake's facts
//! (`schema_version`, `lake_id`, `data_location`, `created_at`) and
//! `pink_lake_objects` holds every metadata object as a row — path, kind,
//! version, the exact object bytes (a sealed `PNKLAKE1` blob for an encrypted
//! lake), a monotonically bumped tag and an update time. The primitive
//! semantics match the object store exactly: `put_new` is an `INSERT` whose
//! primary-key conflict is `Ok(false)`, `put_if_match` is an `UPDATE … WHERE
//! path=? AND tag=?` whose zero-row result is `Ok(false)`, and `read_tagged`
//! returns the row's tag.
//!
//! WAL mode plus a `busy_timeout` (5 s by default, `PINKDB_CATALOG_BUSY_TIMEOUT_MS`
//! overrides) let a second process block rather than fail. The publish scope
//! is one `BEGIN IMMEDIATE` transaction, so a publication's commit delta,
//! snapshot, catalog CAS and highest marker land atomically or not at all.
//!
//! The transaction lives on a thread-local connection that the publishing
//! thread's storage calls use; ordinary calls use the shared connection
//! behind a mutex. In-process writers on separate handles are separate
//! connections, serialized by SQLite's write lock exactly like separate
//! processes.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, ErrorCode, OptionalExtension, params};

use super::CatalogStore;
use crate::local::CATALOG_FILE;
use crate::storage::{Head, LocalCopy, ObjectInfo, PublishScope, Storage, VersionTag};
use crate::{HIGHEST_FILE, LAKE_FILE, LakeError, Result};

/// The catalog schema this build writes. A row with another value is a newer
/// (or unrelated) database: refused, never guessed at.
const SCHEMA_VERSION: i64 = 1;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pink_lake_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pink_lake_objects (
    path       TEXT PRIMARY KEY,
    kind       TEXT NOT NULL,
    version    BIGINT,
    bytes      BLOB NOT NULL,
    tag        BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS pink_lake_objects_kind_version
    ON pink_lake_objects (kind, version);
";

/// One live publish transaction on this thread. A thread runs at most one
/// store's publication at a time (the lake code is synchronous).
struct ActiveTx {
    store_id: u64,
    conn: Connection,
}

thread_local! {
    static ACTIVE_TX: RefCell<Option<ActiveTx>> = const { RefCell::new(None) };
}

/// Process-unique store ids, so the thread-local publish slot can tell two
/// handles on the same file apart.
static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct Inner {
    id: u64,
    url: String,
    path: PathBuf,
    conn: Mutex<Connection>,
}

/// The SQLite catalog backend. `Debug` is derived for the lake handle; the
/// connection lives in [`Inner`].
#[derive(Debug)]
pub(crate) struct SqliteCatalog {
    inner: Arc<Inner>,
}

impl SqliteCatalog {
    /// Open the one lake in `path`. With `create`, an absent file (and its
    /// parent directory, so `sqlite:/tmp/new-lake/catalog.db` works like a lake
    /// directory does) is created; without it, an absent file is `NotALake`
    /// and nothing is written — a mistyped open never leaves an empty catalog.
    pub(crate) fn open(url: &str, path: &Path, create: bool) -> Result<Self> {
        if !create && !path.exists() {
            return Err(LakeError::NotALake {
                path: PathBuf::from(url),
            });
        }
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| LakeError::Io {
                context: format!("creating the catalog directory {}", parent.display()),
                source: e,
            })?;
        }
        let conn =
            Connection::open(path).map_err(|e| sql_error("opening the SQLite catalog", e))?;
        configure(&conn)?;
        conn.execute_batch(SCHEMA)
            .map_err(|e| sql_error("creating the catalog schema", e))?;
        let store = Self {
            inner: Arc::new(Inner {
                id: NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed),
                url: url.to_string(),
                path: path.to_path_buf(),
                conn: Mutex::new(conn),
            }),
        };
        store.init_meta()?;
        Ok(store)
    }

    /// Seed `pink_lake_meta`'s fixed rows and refuse an unknown schema.
    fn init_meta(&self) -> Result<()> {
        let existing: Option<String> = self.query_meta("schema_version")?;
        match existing {
            Some(value) => {
                let found = value.parse::<u32>().unwrap_or(0);
                if value != SCHEMA_VERSION.to_string() {
                    return Err(LakeError::Unsupported {
                        path: self.inner.path.clone(),
                        found,
                    });
                }
            }
            None => self.put_meta("schema_version", &SCHEMA_VERSION.to_string())?,
        }
        if self.query_meta("created_at")?.is_none() {
            self.put_meta("created_at", &now_micros().to_string())?;
        }
        Ok(())
    }

    fn query_meta(&self, key: &str) -> Result<Option<String>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT value FROM pink_lake_meta WHERE key = ?1",
                params![key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| sql_error("reading pink_lake_meta", e))
        })
    }

    fn put_meta(&self, key: &str, value: &str) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO pink_lake_meta (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![key, value],
            )
            .map_err(|e| sql_error("writing pink_lake_meta", e))?;
            Ok(())
        })
    }

    /// Run `f` on the active publish transaction when this store owns one on
    /// this thread, else on the shared connection. Holding the mutex across
    /// `f` serializes ordinary calls; the publish scope's connection is
    /// exclusive to the publishing thread by construction.
    fn with_conn<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        ACTIVE_TX.with(|slot| {
            let borrowed = slot.borrow();
            if let Some(tx) = borrowed.as_ref()
                && tx.store_id == self.inner.id
            {
                return f(&tx.conn);
            }
            drop(borrowed);
            let conn = self
                .inner
                .conn
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            f(&conn)
        })
    }

    /// Open the transaction connection for a publication: a fresh connection
    /// (so the shared one stays usable) that waits for the write lock.
    fn open_transaction(&self) -> Result<Connection> {
        let conn = Connection::open(&self.inner.path)
            .map_err(|e| sql_error("opening a SQLite transaction", e))?;
        configure(&conn)?;
        conn.execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| sql_error("beginning the catalog transaction", e))?;
        Ok(conn)
    }

    fn path_of(&self, rel: &str) -> PathBuf {
        catalog_abs(&self.inner.url, rel)
    }
}

/// A catalog URL plus the object path, for error messages:
/// `sqlite:<path>#<rel>`. The SQLite URL has no credentials to redact.
fn catalog_abs(url: &str, rel: &str) -> PathBuf {
    if rel.is_empty() {
        PathBuf::from(url)
    } else {
        PathBuf::from(format!("{url}#{rel}"))
    }
}

impl Storage for SqliteCatalog {
    fn abs(&self, rel: &str) -> PathBuf {
        self.path_of(rel)
    }

    fn head(&self, rel: &str) -> Result<Head> {
        self.with_conn(|conn| {
            let exists = conn
                .query_row(
                    "SELECT 1 FROM pink_lake_objects WHERE path = ?1",
                    params![rel],
                    |_| Ok(()),
                )
                .optional()
                .map_err(|e| sql_error("checking a catalog object", e))?
                .is_some();
            Ok(if exists { Head::File } else { Head::Missing })
        })
    }

    fn stat(&self, rel: &str) -> Result<Option<ObjectInfo>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT length(bytes), updated_at FROM pink_lake_objects WHERE path = ?1",
                params![rel],
                |row| {
                    let size: i64 = row.get(0)?;
                    let updated: i64 = row.get(1)?;
                    Ok(ObjectInfo {
                        size: u64::try_from(size).unwrap_or(0),
                        modified_micros: Some(updated),
                    })
                },
            )
            .optional()
            .map_err(|e| sql_error("reading a catalog object's size", e))
        })
    }

    fn read(&self, rel: &str, context: &str) -> Result<Vec<u8>> {
        self.read_optional(rel, context)?
            .ok_or_else(|| missing(self.abs(rel)))
    }

    fn read_optional(&self, rel: &str, _context: &str) -> Result<Option<Vec<u8>>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT bytes FROM pink_lake_objects WHERE path = ?1",
                params![rel],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(|e| sql_error("reading a catalog object", e))
        })
    }

    fn read_range(&self, _rel: &str, _offset: u64, _len: u64, _context: &str) -> Result<Vec<u8>> {
        Err(LakeError::Internal {
            message: "ranged reads are a data-location operation; the catalog store is never \
                      range-read"
                .to_string(),
        })
    }

    fn read_tagged(&self, rel: &str, _context: &str) -> Result<(Vec<u8>, VersionTag)> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT bytes, tag FROM pink_lake_objects WHERE path = ?1",
                params![rel],
                |row| {
                    let bytes: Vec<u8> = row.get(0)?;
                    let tag: i64 = row.get(1)?;
                    Ok((bytes, VersionTag(tag.to_string())))
                },
            )
            .optional()
            .map_err(|e| sql_error("reading a catalog object's tag", e))?
            .ok_or_else(|| missing(self.abs(rel)))
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
        let (kind, version) = kind_version(rel);
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO pink_lake_objects (path, kind, version, bytes, tag, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 1, ?5)
                 ON CONFLICT(path) DO UPDATE SET
                     kind = excluded.kind,
                     version = excluded.version,
                     bytes = excluded.bytes,
                     tag = pink_lake_objects.tag + 1,
                     updated_at = excluded.updated_at",
                params![rel, kind, version, bytes, now_micros()],
            )
            .map_err(|e| sql_error("replacing a catalog object", e))?;
            Ok(())
        })
    }

    fn put_new(&self, rel: &str, bytes: &[u8]) -> Result<bool> {
        let (kind, version) = kind_version(rel);
        self.with_conn(|conn| {
            match conn.execute(
                "INSERT INTO pink_lake_objects (path, kind, version, bytes, tag, updated_at)
                 VALUES (?1, ?2, ?3, ?4, 1, ?5)",
                params![rel, kind, version, bytes, now_micros()],
            ) {
                Ok(_) => Ok(true),
                Err(rusqlite::Error::SqliteFailure(err, _))
                    if err.code == ErrorCode::ConstraintViolation =>
                {
                    // The object already exists: the create-only condition
                    // lost, exactly like a hard-link `EEXIST` or S3's 412.
                    Ok(false)
                }
                Err(e) => Err(sql_error("creating a catalog object", e)),
            }
        })
    }

    fn put_if_match(&self, rel: &str, bytes: &[u8], expected: &VersionTag) -> Result<bool> {
        let Ok(tag) = expected.0.parse::<i64>() else {
            // A tag that never came from this store cannot match any row.
            return Ok(false);
        };
        self.with_conn(|conn| {
            let changed = conn
                .execute(
                    "UPDATE pink_lake_objects
                     SET bytes = ?1, tag = tag + 1, updated_at = ?2
                     WHERE path = ?3 AND tag = ?4",
                    params![bytes, now_micros(), rel, tag],
                )
                .map_err(|e| sql_error("replacing a catalog object conditionally", e))?;
            Ok(changed == 1)
        })
    }

    fn list(&self, rel: &str) -> Result<Vec<String>> {
        self.list_prefix(rel, true)
    }

    fn list_recursive(&self, rel: &str) -> Result<Vec<String>> {
        self.list_prefix(rel, false)
    }

    fn delete(&self, rel: &str) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "DELETE FROM pink_lake_objects WHERE path = ?1",
                params![rel],
            )
            .map_err(|e| sql_error("deleting a catalog object", e))?;
            Ok(())
        })
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
        let already_active = ACTIVE_TX.with(|slot| match slot.borrow().as_ref() {
            Some(tx) if tx.store_id == self.inner.id => Ok(true),
            Some(_) => Err(LakeError::Internal {
                message: "another catalog store already has an open publish scope on this thread"
                    .to_string(),
            }),
            None => Ok(false),
        })?;
        if already_active {
            // A nested publication is part of the outer transaction; only the
            // outermost scope commits or rolls back.
            return Ok(PublishScope::noop());
        }
        let conn = self.open_transaction()?;
        let store_id = self.inner.id;
        ACTIVE_TX.with(|slot| {
            *slot.borrow_mut() = Some(ActiveTx { store_id, conn });
        });
        Ok(PublishScope::new(Box::new(move |commit| {
            finish_transaction(store_id, commit)
        })))
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
    /// `list`/`list_recursive` over the row paths. `direct` keeps only the
    /// remainder without a `/` (the manifest-facing direct-children listing).
    fn list_prefix(&self, rel: &str, direct: bool) -> Result<Vec<String>> {
        let prefix = if rel.is_empty() {
            String::new()
        } else {
            format!("{}/", rel.trim_end_matches('/'))
        };
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT path FROM pink_lake_objects
                     WHERE substr(path, 1, length(?1)) = ?1
                     ORDER BY path",
                )
                .map_err(|e| sql_error("listing catalog objects", e))?;
            let rows = stmt
                .query_map(params![prefix], |row| row.get::<_, String>(0))
                .map_err(|e| sql_error("listing catalog objects", e))?;
            let mut out = Vec::new();
            for row in rows {
                let path = row.map_err(|e| sql_error("listing catalog objects", e))?;
                let Some(tail) = path.strip_prefix(&prefix) else {
                    continue;
                };
                if direct && tail.contains('/') {
                    continue;
                }
                out.push(tail.to_string());
            }
            Ok(out)
        })
    }
}

impl CatalogStore for SqliteCatalog {
    fn data_location(&self) -> Result<Option<String>> {
        self.query_meta("data_location")
    }

    fn set_data_location(&self, location: &str) -> Result<()> {
        self.put_meta("data_location", location)
    }

    fn set_lake_id(&self, lake_id: &str) -> Result<()> {
        self.put_meta("lake_id", lake_id)
    }
}

/// Commit or roll back the thread's active transaction. Runs from
/// `PublishScope` (`commit()` or drop).
fn finish_transaction(store_id: u64, commit: bool) -> Result<()> {
    ACTIVE_TX.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(tx) = slot.take() else {
            return Ok(());
        };
        if tx.store_id != store_id {
            *slot = Some(tx);
            return Err(LakeError::Internal {
                message: "a publish scope finished another store's transaction".to_string(),
            });
        }
        if commit {
            tx.conn
                .execute_batch("COMMIT")
                .map_err(|e| sql_error("committing the catalog transaction", e))
        } else {
            let _ = tx.conn.execute_batch("ROLLBACK");
            Ok(())
        }
    })
}

/// Apply the per-connection settings: WAL is persistent in the file, but the
/// busy timeout is per connection and must be set on the transaction
/// connection too.
fn configure(conn: &Connection) -> Result<()> {
    conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")
        .map_err(|e| sql_error("configuring the SQLite catalog", e))?;
    let millis = std::env::var("PINKDB_CATALOG_BUSY_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5_000);
    conn.busy_timeout(Duration::from_millis(millis))
        .map_err(|e| sql_error("setting the SQLite busy timeout", e))
}

/// The catalog row's `kind`/`version` columns, derived from the path the way
/// the object layout encodes them. `version` is only meaningful for snapshot
/// and commit rows.
fn kind_version(rel: &str) -> (&'static str, Option<i64>) {
    if rel == LAKE_FILE {
        ("lake", None)
    } else if rel == CATALOG_FILE {
        ("catalog", None)
    } else if rel == HIGHEST_FILE {
        ("highest", None)
    } else if let Some(version) = snapshot_version(rel) {
        ("snapshot", Some(version as i64))
    } else if let Some(version) = commit_version(rel) {
        ("commit", Some(version as i64))
    } else if rel.starts_with("replication/") {
        ("replication", None)
    } else {
        ("metadata", None)
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

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
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

fn sql_error(context: &str, error: rusqlite::Error) -> LakeError {
    LakeError::Io {
        context: context.to_string(),
        source: std::io::Error::other(error),
    }
}
