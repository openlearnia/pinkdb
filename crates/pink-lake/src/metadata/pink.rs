//! The pinkdb `.pink` connection adapter (wave 103).
//!
//! A `pink:<path>` catalog is a durable pinkdb database — the DuckDB-file
//! analogue for a lake catalog. The SQL text and the DuckLake commit loop are
//! the shared core in [`super::store`]; this file supplies the engine half of
//! [`SqlConnection`]: open durably, run one statement, materialize a query's
//! rows, and own one `BEGIN..COMMIT` transaction.
//!
//! # Transactions and the write lease
//!
//! One `PinkDb` carries one transaction state, so a transaction handle shares
//! the store behind the same mutex and owns a small gate: while a transaction
//! is open every statement of the owning handle runs inside it, and a
//! statement on another handle (another thread, or a read racing the publish)
//! waits for the transaction to finish. Across processes the engine's own
//! write lease serializes the commit; its `COMMIT` validates the transaction's
//! read/write set against frames other writers appended, and the resulting
//! `write conflict on …` (or a lease `… is busy:`) reaches the lake as
//! [`LakeError::Conflict`], exactly the retry/rebase path SQLite uses.
//!
//! # Placeholders
//!
//! The shared SQL uses numbered placeholders (`?1`, `?2`, …) because several
//! predicates legitimately repeat a value. Pinkdb prepared statements are
//! strictly positional `?`, so [`expand`] rewrites the text and duplicates
//! the bound value for each repeat.
//!
//! # Values
//!
//! Pinkdb has no BLOB type and no enforced constraints; the opaque side-object
//! bytes are stored as lower-case hex `VARCHAR` (the only binary column in the
//! catalog). DuckLake's snapshot insert is still the logical commit point: the
//! transaction reads `ducklake_snapshot` before writing it, so a concurrent
//! writer's snapshot row conflicts at `COMMIT` even though pinkdb's declared
//! PRIMARY KEY is metadata-only (see the report for wave 103 T1b).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pinkdb::{DurableOpenOptions, PinkDb, PreparedStatement, QueryResult, SyncPolicy, Value};

use super::conn::{SqlConnection, SqlRow, SqlValue, encode_hex};
use super::dialect::Dialect;
use super::store::SqlMetadataStore;
use crate::{LakeError, Result};

/// Open (or create) the one lake in `path`, durably: `SyncPolicy::Sync`
/// fsyncs a committed catalog transaction before the commit returns, so a
/// snapshot that the lake reported is on disk after a crash.
pub(crate) fn open(url: &str, path: &Path, create: bool) -> Result<SqlMetadataStore> {
    SqlMetadataStore::open(url, path, create, |url, path, create| {
        // A durable database may exist only as `<path>.pink.wal` until its
        // first checkpoint; the main file alone is not the existence test.
        let wal = PathBuf::from(format!("{}.wal", path.display()));
        if !create && !path.exists() && !wal.exists() {
            return Err(LakeError::NotALake {
                path: PathBuf::from(url),
            });
        }
        let options = DurableOpenOptions {
            policy: SyncPolicy::Sync,
            busy_timeout: busy_timeout(),
            ..DurableOpenOptions::default()
        };
        let (db, _) = PinkDb::open_durable_with_options(&path.to_string_lossy(), options)
            .map_err(|e| map_error(url, e))?;
        let connection = PinkConnection {
            db: Arc::new(Mutex::new(db)),
            url: url.to_string(),
            gate: Arc::new(Mutex::new(false)),
            prepared: Arc::new(Mutex::new(HashMap::new())),
            in_transaction: false,
            finished: AtomicBool::new(false),
            total: AtomicU64::new(0),
        };
        Ok(Box::new(connection))
    })
}

/// The catalog busy timeout: the same variable the SQLite adapter honours,
/// with the engine's `PINKDB_BUSY_TIMEOUT_MS` as the fallback so a test or a
/// host that already tunes the database tunes the catalog too.
fn busy_timeout() -> Duration {
    let millis = std::env::var("PINKDB_CATALOG_BUSY_TIMEOUT_MS")
        .ok()
        .or_else(|| std::env::var("PINKDB_BUSY_TIMEOUT_MS").ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5_000);
    Duration::from_millis(millis)
}

#[derive(Debug)]
struct PinkConnection {
    db: Arc<Mutex<PinkDb>>,
    url: String,
    /// `true` while a transaction handle owns the shared connection.
    gate: Arc<Mutex<bool>>,
    /// Parsed statements keyed by the rewritten (positional) SQL text. pinkdb
    /// has no prepared-statement cache of its own, and a catalog open issues
    /// the same few dozen statements every time; re-parsing them dominated
    /// small attaches.
    prepared: Arc<Mutex<HashMap<String, PreparedStatement>>>,
    in_transaction: bool,
    finished: AtomicBool,
    total: AtomicU64,
}

impl PinkConnection {
    /// Wait until no transaction handle owns the shared connection.
    fn wait_gate(&self) {
        loop {
            let held = *self.gate.lock().unwrap_or_else(|p| p.into_inner());
            if !held {
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn release_gate(&self) {
        *self.gate.lock().unwrap_or_else(|p| p.into_inner()) = false;
    }

    /// Run one single statement without the gate check (the owner path).
    ///
    /// The rewritten SQL is prepared once and cached: binding resolves types
    /// against the current catalog on every execute, so a schema change does
    /// not stale the cache. A statement the prepared path rejects (a
    /// non-preparable single statement) falls back to `execute`, which parses
    /// it exactly as before.
    fn run_unguarded(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<QueryResult>> {
        let (sql, values) = expand(sql, params)?;
        let mut db = self
            .db
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let prepared = {
            let mut cache = self
                .prepared
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match cache.get(&sql) {
                Some(prepared) => Some(prepared.clone()),
                None => match db.prepare(&sql) {
                    Ok(prepared) => {
                        cache.insert(sql.clone(), prepared.clone());
                        Some(prepared)
                    }
                    Err(_) => None,
                },
            }
        };
        match prepared {
            Some(prepared) => db
                .execute_prepared(&prepared, &values)
                .map(|result| vec![result])
                .map_err(|e| map_error(&self.url, e)),
            None => db.execute(&sql).map_err(|e| map_error(&self.url, e)),
        }
    }

    /// Run a semicolon-separated batch (schema creation) without the gate
    /// check. Batches are not preparable statement-by-statement, so this is
    /// the plain parse/execute path.
    fn run_batch_unguarded(&self, sql: &str) -> Result<Vec<QueryResult>> {
        let mut db = self
            .db
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        db.execute(sql).map_err(|e| map_error(&self.url, e))
    }
}

impl Drop for PinkConnection {
    fn drop(&mut self) {
        if self.in_transaction && !self.finished.load(Ordering::Relaxed) {
            let _ = self.run_batch_unguarded("ROLLBACK");
            self.release_gate();
        }
    }
}

impl SqlConnection for PinkConnection {
    fn dialect(&self) -> Dialect {
        Dialect::Pink
    }

    fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<usize> {
        if !self.in_transaction {
            self.wait_gate();
        }
        let results = self.run_unguarded(sql, params)?;
        let changed = changed_rows(&results);
        self.total.fetch_add(changed as u64, Ordering::Relaxed);
        Ok(changed)
    }

    fn query(&self, sql: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>> {
        if !self.in_transaction {
            self.wait_gate();
        }
        let results = self.run_unguarded(sql, params)?;
        for result in results {
            if let QueryResult::Selected { rows, .. } = result {
                return Ok(rows
                    .into_iter()
                    .map(|row| SqlRow::new(row.into_iter().map(from_pink).collect()))
                    .collect());
            }
        }
        Ok(Vec::new())
    }

    fn execute_batch(&self, sql: &str) -> Result<()> {
        if !self.in_transaction {
            self.wait_gate();
        }
        self.run_batch_unguarded(sql).map(|_| ())
    }

    fn begin(&self) -> Result<Box<dyn SqlConnection>> {
        if self.in_transaction {
            return Err(LakeError::Internal {
                message: "a pinkdb catalog transaction cannot nest".to_string(),
            });
        }
        // One transaction owner: another thread's publish waits here instead
        // of opening a second transaction on the same engine connection.
        loop {
            let mut held = self.gate.lock().unwrap_or_else(|p| p.into_inner());
            if !*held {
                *held = true;
                break;
            }
            drop(held);
            std::thread::sleep(Duration::from_millis(1));
        }
        let handle = PinkConnection {
            db: Arc::clone(&self.db),
            url: self.url.clone(),
            gate: Arc::clone(&self.gate),
            prepared: Arc::clone(&self.prepared),
            in_transaction: true,
            finished: AtomicBool::new(false),
            total: AtomicU64::new(0),
        };
        if let Err(error) = handle.run_batch_unguarded(Dialect::Pink.begin_write()) {
            handle.release_gate();
            return Err(error);
        }
        Ok(Box::new(handle))
    }

    fn commit(&self) -> Result<()> {
        if !self.in_transaction {
            return Err(LakeError::Internal {
                message: "commit on a non-transaction pinkdb handle".to_string(),
            });
        }
        let result = self.run_batch_unguarded("COMMIT");
        self.finished.store(true, Ordering::Relaxed);
        self.release_gate();
        result.map(|_| ())
    }

    fn rollback(&self) -> Result<()> {
        if !self.in_transaction {
            return Ok(());
        }
        let result = self.run_batch_unguarded("ROLLBACK");
        self.finished.store(true, Ordering::Relaxed);
        self.release_gate();
        result.map(|_| ())
    }

    fn total_changes(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }
}

/// Rewrite the core's numbered placeholders into pinkdb's positional `?`,
/// repeating a bound value when the same index appears more than once.
fn expand(sql: &str, params: &[SqlValue]) -> Result<(String, Vec<Value>)> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut values = Vec::new();
    let mut index = 0;
    let mut last = 0;
    while index < bytes.len() {
        if bytes[index] != b'?' {
            index += 1;
            continue;
        }
        let start = index + 1;
        let mut end = start;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        let slot: usize = sql[start..end].parse().map_err(|_| LakeError::Internal {
            message: format!("the catalog SQL has an unnumbered placeholder: {sql}"),
        })?;
        if slot == 0 || slot > params.len() {
            return Err(LakeError::Internal {
                message: format!(
                    "the catalog SQL binds placeholder ?{slot}, but {} value(s) were supplied",
                    params.len()
                ),
            });
        }
        out.push_str(&sql[last..index]);
        out.push('?');
        values.push(to_pink(&params[slot - 1]));
        last = end;
        index = end;
    }
    out.push_str(&sql[last..]);
    Ok((out, values))
}

fn to_pink(value: &SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(v) => Value::Bigint(*v),
        SqlValue::Real(v) => Value::Double(*v),
        SqlValue::Text(v) => Value::Varchar(v.clone()),
        // No BLOB type: the opaque side objects are JSON bytes, stored as
        // lower-case hex text and decoded by the value layer on read.
        SqlValue::Blob(v) => Value::Varchar(encode_hex(v)),
    }
}

fn from_pink(value: Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Boolean(v) => SqlValue::Integer(i64::from(v)),
        Value::Integer(v) => SqlValue::Integer(i64::from(v)),
        Value::Bigint(v) => SqlValue::Integer(v),
        Value::Double(v) => SqlValue::Real(v),
        Value::Varchar(v) => SqlValue::Text(v),
        // The catalog's DuckLake columns are BIGINT/VARCHAR; a DuckDB-created
        // inlined table can still declare DATE/TIMESTAMP, and the engine
        // represents those as text in DuckLake's physical encoding.
        other => SqlValue::Text(other.to_string()),
    }
}

fn changed_rows(results: &[QueryResult]) -> usize {
    results
        .iter()
        .map(|result| match result {
            QueryResult::Inserted { count }
            | QueryResult::Updated { count }
            | QueryResult::Deleted { count } => *count,
            _ => 0,
        })
        .sum()
}

/// A pinkdb error is a retryable catalog conflict when the engine's write
/// lease or its optimistic commit validator rejects this writer. The lake's
/// conflict loop treats both exactly like SQLite's `SQLITE_BUSY`.
fn map_error(url: &str, error: pinkdb::PinkDbError) -> LakeError {
    let text = error.to_string();
    if text.contains("write conflict on") || text.contains(" is busy:") {
        return LakeError::Conflict {
            path: PathBuf::from(url),
            message: text,
        };
    }
    LakeError::Io {
        context: format!("the pinkdb catalog {url}"),
        source: std::io::Error::other(text),
    }
}
