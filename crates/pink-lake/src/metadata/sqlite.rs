//! The SQLite connection adapter (wave 103, `catalog-sqlite`).
//!
//! The SQL text, the DuckLake schema and the whole commit loop live in
//! [`super::store`]; this file is only the rusqlite half of
//! [`SqlConnection`]. A transaction is a second `Connection` to the same
//! file, started `BEGIN IMMEDIATE` so writers serialize on SQLite's own
//! write lock; the store's thread-local publish slot routes every statement
//! of the transaction to that handle.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rusqlite::{Connection, params_from_iter};

use super::conn::{SqlConnection, SqlRow, SqlValue};
use super::dialect::Dialect;
use super::store::SqlMetadataStore;
use crate::{LakeError, Result};

/// Open (or create) the one lake in `path`.
pub(crate) fn open(url: &str, path: &Path, create: bool) -> Result<SqlMetadataStore> {
    SqlMetadataStore::open(url, path, create, |_url, path, create| {
        if !create && !path.exists() {
            return Err(LakeError::NotALake {
                path: path.to_path_buf(),
            });
        }
        let conn =
            Connection::open(path).map_err(|e| sql_error("opening the SQLite catalog", e))?;
        configure(&conn)?;
        Ok(Box::new(SqliteConnection::new(conn, path.to_path_buf())))
    })
}

/// Configure a SQLite handle: WAL for concurrent readers, `NORMAL` sync
/// (the catalog is a cache of committed deltas; a lost last transaction is
/// the same retryable conflict the lake already handles), and the busy
/// timeout that turns a second writer into a wait rather than an error.
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

#[derive(Debug)]
struct SqliteConnection {
    conn: Mutex<Connection>,
    path: PathBuf,
    total: AtomicU64,
    in_transaction: bool,
}

impl SqliteConnection {
    fn new(conn: Connection, path: PathBuf) -> Self {
        Self {
            conn: Mutex::new(conn),
            path,
            total: AtomicU64::new(0),
            in_transaction: false,
        }
    }

    fn connection(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl SqlConnection for SqliteConnection {
    fn dialect(&self) -> Dialect {
        Dialect::Sqlite
    }

    fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<usize> {
        let conn = self.connection();
        let mut stmt = conn
            .prepare_cached(sql)
            .map_err(|e| sql_error("preparing a catalog statement", e))?;
        let changed = stmt
            .execute(params_from_iter(params.iter().map(to_rusqlite)))
            .map_err(|e| sql_error("executing a catalog statement", e))?;
        self.total.fetch_add(changed as u64, Ordering::Relaxed);
        Ok(changed)
    }

    fn query(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>> {
        let conn = self.connection();
        let mut stmt = conn
            .prepare_cached(sql)
            .map_err(|e| sql_error("preparing a catalog query", e))?;
        let columns = stmt.column_count();
        let mut rows = stmt
            .query(params_from_iter(params.iter().map(to_rusqlite)))
            .map_err(|e| sql_error("running a catalog query", e))?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .map_err(|e| sql_error("reading a catalog row", e))?
        {
            let mut values = Vec::with_capacity(columns);
            for index in 0..columns {
                let value: rusqlite::types::Value = row
                    .get(index)
                    .map_err(|e| sql_error("reading a catalog column", e))?;
                values.push(from_rusqlite(value));
            }
            out.push(SqlRow::new(values));
        }
        Ok(out)
    }

    fn execute_batch(&self, sql: &str) -> Result<()> {
        self.connection()
            .execute_batch(sql)
            .map_err(|e| sql_error("running catalog DDL", e))
    }

    fn begin(&self) -> Result<Box<dyn SqlConnection>> {
        if self.in_transaction {
            return Err(LakeError::Internal {
                message: "a SQLite catalog transaction cannot nest".to_string(),
            });
        }
        let conn = Connection::open(&self.path)
            .map_err(|e| sql_error("opening a SQLite transaction", e))?;
        configure(&conn)?;
        conn.execute_batch(Dialect::Sqlite.begin_write())
            .map_err(|e| sql_error("beginning the catalog transaction", e))?;
        Ok(Box::new(SqliteConnection {
            conn: Mutex::new(conn),
            path: self.path.clone(),
            total: AtomicU64::new(0),
            in_transaction: true,
        }))
    }

    fn commit(&self) -> Result<()> {
        if !self.in_transaction {
            return Err(LakeError::Internal {
                message: "commit on a non-transaction SQLite handle".to_string(),
            });
        }
        self.connection()
            .execute_batch("COMMIT")
            .map_err(|e| sql_error("committing the catalog transaction", e))
    }

    fn rollback(&self) -> Result<()> {
        if !self.in_transaction {
            return Ok(());
        }
        self.connection()
            .execute_batch("ROLLBACK")
            .map_err(|e| sql_error("rolling back the catalog transaction", e))
    }

    fn total_changes(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }
}

fn from_rusqlite(value: rusqlite::types::Value) -> SqlValue {
    match value {
        rusqlite::types::Value::Null => SqlValue::Null,
        rusqlite::types::Value::Integer(v) => SqlValue::Integer(v),
        rusqlite::types::Value::Real(v) => SqlValue::Real(v),
        rusqlite::types::Value::Text(v) => SqlValue::Text(v),
        rusqlite::types::Value::Blob(v) => SqlValue::Blob(v),
    }
}

fn to_rusqlite(value: &SqlValue) -> rusqlite::types::Value {
    match value {
        SqlValue::Null => rusqlite::types::Value::Null,
        SqlValue::Integer(v) => rusqlite::types::Value::Integer(*v),
        SqlValue::Real(v) => rusqlite::types::Value::Real(*v),
        SqlValue::Text(v) => rusqlite::types::Value::Text(v.clone()),
        SqlValue::Blob(v) => rusqlite::types::Value::Blob(v.clone()),
    }
}

fn sql_error(context: &str, error: rusqlite::Error) -> LakeError {
    LakeError::Io {
        context: context.to_string(),
        source: std::io::Error::other(error),
    }
}
