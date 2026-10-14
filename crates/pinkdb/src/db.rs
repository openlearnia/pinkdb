//! Multi-writer connections over a shared store.
//!
//! [`Database`] is a cheaply cloneable handle to one store shared between
//! threads (`Arc<RwLock<PinkDb>>`). Each thread works through its own
//! [`Connection`]:
//!
//! - Outside a transaction (autocommit): queries run under a shared read
//!   lock (readers never block each other); writes take the write lock for
//!   one statement.
//! - Inside `BEGIN..COMMIT`: the connection works on a private snapshot
//!   (repeatable reads, own writes visible). `COMMIT` merges table-by-table
//!   under the write lock after an optimistic version check — concurrent
//!   commits to disjoint tables/views both succeed; overlapping writes abort
//!   the second committer with a write-conflict error (no silent lost
//!   updates). `ROLLBACK` discards the snapshot.
//!
//! A transaction that writes is also validated on its read set: every
//! catalog table any of its statements read (subqueries and DML predicates
//! included) must be unchanged at COMMIT. That makes writing transactions
//! serializable at table granularity — and, crucially, keeps the WAL honest:
//! replay re-runs the committed SQL text after every earlier commit, so a
//! stale read would otherwise replay to different data than the session saw.
//! Read-only transactions log nothing and never conflict. Transactions hold
//! no locks while open (only validation+merge serializes at commit).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::crypto::EncryptionKey;
use crate::plan::{is_read_only, LogicalPlan};
use crate::storage::{PinkDb, ViewDef};
use crate::types::{PinkDbError, Result};
use crate::QueryResult;

/// One-time warning flag for poisoned-lock recovery (see [`read_lock`]).
static POISON_WARNED: AtomicBool = AtomicBool::new(false);

/// Loudly (once per process) note that a poisoned lock is being recovered.
fn warn_poisoned_once() {
    if !POISON_WARNED.swap(true, Ordering::Relaxed) {
        eprintln!(
            "pinkdb: WARNING: recovering from a poisoned lock after a panic; \
             the store may be inconsistent, but statements keep working"
        );
    }
}

/// Read-lock the shared store, recovering from poisoning.
///
/// A thread that panics while holding the write lock poisons it; refusing to
/// lock afterwards would let one panic permanently DoS the store. Recovery is
/// loud and the state may be inconsistent (the panicking statement is not
/// guaranteed to have applied atomically), but reads and writes keep working.
fn read_lock<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| {
        warn_poisoned_once();
        poisoned.into_inner()
    })
}

/// Write-lock the shared store, recovering from poisoning (see [`read_lock`]).
fn write_lock<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|poisoned| {
        warn_poisoned_once();
        poisoned.into_inner()
    })
}

/// A shared database handle. Clone it per thread; each thread then opens its
/// own [`Connection`].
#[derive(Clone, Debug, Default)]
pub struct Database {
    shared: Arc<RwLock<PinkDb>>,
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a durable database at `path`: load the `.pink` checkpoint (if
    /// present), replay `<path>.pink.wal` (stopping at a torn tail), and log
    /// every subsequent commit to that WAL.
    pub fn open(path: &str) -> Result<Database> {
        Ok(Database::from_store(PinkDb::open_durable(path)?))
    }

    /// Open a durable database at `path` with at-rest encryption: the
    /// checkpoint and the sidecar WAL must be encrypted and are decrypted
    /// with `key`; commits are written encrypted. Requires the `encryption`
    /// feature; a plaintext artifact is refused so the key cannot be silently
    /// ignored.
    pub fn open_with_key(path: &str, key: &EncryptionKey) -> Result<Database> {
        Ok(Database::from_store(PinkDb::open_durable_with_key(
            path, key,
        )?))
    }

    pub fn from_store(store: PinkDb) -> Self {
        Self {
            shared: Arc::new(RwLock::new(store)),
        }
    }

    pub fn connect(&self) -> Connection {
        // Start on the store's current database (fresh stores: `main`).
        let current_db = read_lock(&self.shared).current_db().to_string();
        Connection {
            db: self.clone(),
            txn: None,
            current_db,
        }
    }

    /// True when this database logs commits to a WAL.
    pub fn is_durable(&self) -> bool {
        read_lock(&self.shared).is_durable()
    }

    /// Checkpoint the durable store: atomically write the bound `.pink` and
    /// roll the WAL generation. Errors for in-memory databases.
    pub fn checkpoint(&self) -> Result<()> {
        let mut shared = write_lock(&self.shared);
        shared.checkpoint()
    }

    /// WAL size that triggers an automatic checkpoint after a committed
    /// write (`None` disables it; default
    /// [`crate::DEFAULT_AUTO_CHECKPOINT_BYTES`]). No-op when in-memory.
    pub fn set_auto_checkpoint_bytes(&self, bytes: Option<u64>) {
        read_lock(&self.shared).set_auto_checkpoint_bytes(bytes);
    }

    /// Allow or refuse the SQL `LOAD EXTENSION` statement for every
    /// connection of this database (default: allowed). Network servers turn
    /// it off unless the operator opts in.
    pub fn set_allow_sql_load_extension(&self, allow: bool) {
        write_lock(&self.shared).set_allow_sql_load_extension(allow);
    }

    /// fsync the WAL (meaningful with [`crate::SyncPolicy::Batch`]).
    pub fn flush_wal(&self) -> Result<()> {
        let mut shared = write_lock(&self.shared);
        shared.flush_wal()
    }
}

struct Txn {
    /// Private working copy (snapshot at BEGIN + own writes).
    work: PinkDb,
    /// `(database, table)` versions and view definitions frozen at BEGIN.
    base_tables: HashMap<(String, String), u64>,
    base_views: HashMap<(String, String), ViewDef>,
    /// Tables / views created, dropped, or written, keyed `(database, name)`.
    table_writes: HashSet<(String, String)>,
    /// Catalog tables read by any statement of the transaction (including
    /// subqueries and DML predicates). A transaction that writes must find
    /// them unchanged at COMMIT: its WAL record replays its SQL text *after*
    /// every earlier commit, so a read of state another commit changed would
    /// replay to different data than this session saw.
    table_reads: HashSet<(String, String)>,
    view_writes: HashSet<(String, String)>,
    /// SQL text of this transaction's mutating statements: one WAL record at
    /// COMMIT (empty for read-only transactions, which log nothing).
    wal_stmts: Vec<String>,
    /// Extension paths touched by those statements (dynamic functions they
    /// call), deduped, in first-use order; the record is
    /// `LOAD EXTENSION`-prefixed for every path the WAL generation has not
    /// loaded yet.
    wal_ext_paths: Vec<String>,
    /// Extension paths loaded by those statements' own `LOAD EXTENSION` text
    /// (empty in practice: the statement is refused inside a transaction).
    wal_ext_loaded: Vec<String>,
    /// Session database before the first buffered statement, and after the
    /// last (the record is `USE`-prefixed when replay's session differs).
    wal_db_before: Option<String>,
    wal_db_after: String,
    /// Every database the WAL record's replay may switch into: the session of
    /// each buffered statement (the implicit `USE` prefix) plus every `USE`
    /// target. A database dropped since BEGIN makes the record unreplayable,
    /// so COMMIT conflicts on it.
    wal_dbs: Vec<String>,
}

/// One connection: single-threaded statement stream, own transaction state,
/// and its own session database (`USE` never touches the shared store).
pub struct Connection {
    db: Database,
    txn: Option<Txn>,
    /// This connection's current database (lowercased).
    current_db: String,
}

impl Connection {
    /// Parse and run every statement, returning one result each.
    /// Multi-statement strings mix freely (each statement locks as needed).
    pub fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>> {
        read_lock(&self.db.shared).ensure_wal_healthy()?;
        let stmts = crate::sql::parse_statements(sql)?;
        // Only durable stores need the per-statement source text for the WAL.
        let texts = if self.db.is_durable() {
            let texts = crate::sql::statement_texts(sql)?;
            if texts.len() != stmts.len() {
                return Err(PinkDbError::Execution(
                    "internal error: statement source split disagrees with the parser".to_string(),
                ));
            }
            Some(texts)
        } else {
            None
        };
        let mut out = Vec::with_capacity(stmts.len());
        for (i, stmt) in stmts.iter().enumerate() {
            let text = texts.as_ref().map(|t| t[i].as_str());
            out.push(self.execute_one(stmt, text)?);
        }
        Ok(out)
    }

    /// The session's current database (lowercased).
    pub fn current_db(&self) -> &str {
        &self.current_db
    }

    fn execute_one(
        &mut self,
        stmt: &crate::sql::ast::Statement,
        sql_text: Option<&str>,
    ) -> Result<QueryResult> {
        // Transaction control never touches table data: handle first.
        match stmt {
            crate::sql::ast::Statement::Begin => return self.txn_begin(),
            crate::sql::ast::Statement::Commit => return self.txn_commit(),
            crate::sql::ast::Statement::Rollback => return self.txn_rollback(),
            _ => {}
        }
        match &mut self.txn {
            Some(txn) => {
                // Store/process-level DDL is refused inside a transaction: the
                // per-table snapshot merge cannot undo a whole database, and a
                // LOAD EXTENSION into the private snapshot would be discarded
                // at COMMIT/ROLLBACK.
                if let Some(name) = refused_in_txn(stmt) {
                    return Err(PinkDbError::Execution(format!(
                        "{name} is not allowed inside a transaction"
                    )));
                }
                let pre = self.current_db.clone();
                let (plan, dynamic_functions_used) =
                    crate::sql::bind_tracked(&txn.work, stmt, &pre)?;
                record_writes(&plan, &pre, &mut txn.table_writes, &mut txn.view_writes);
                crate::plan::collect_tables_read(&plan, &pre, &mut txn.table_reads);
                let result = crate::exec::run(&mut txn.work, &plan)?;
                let after = session_after(&result, &pre, &txn.work);
                // Buffer the committed statement for the transaction's single
                // WAL record (flushed by txn_commit; never by ROLLBACK).
                if let Some(text) = sql_text {
                    if crate::plan::writes_tables(&plan) {
                        if txn.wal_db_before.is_none() {
                            txn.wal_db_before = Some(pre.clone());
                        }
                        txn.wal_stmts.push(text.to_string());
                        txn.wal_db_after = after.clone();
                        // The record's replay starts in `pre` (possibly via a
                        // `USE` prefix) and may switch to `after` (a `USE`):
                        // both must still exist at COMMIT.
                        for db in [&pre, &after] {
                            if !txn.wal_dbs.iter().any(|d| d.eq_ignore_ascii_case(db)) {
                                txn.wal_dbs.push(db.clone());
                            }
                        }
                        // Extensions the statement calls: the record is
                        // prefixed with `LOAD EXTENSION` for every path the
                        // WAL generation has not loaded yet.
                        let touched = txn.work.dynamic_function_paths(&dynamic_functions_used);
                        for p in touched {
                            if !txn.wal_ext_paths.contains(&p) {
                                txn.wal_ext_paths.push(p);
                            }
                        }
                        // `LOAD EXTENSION` is refused inside a transaction, so
                        // this is unreachable today; keep the bookkeeping
                        // correct if that refusal is ever relaxed.
                        for p in crate::storage::loaded_extension_path(&plan) {
                            if !txn.wal_ext_loaded.contains(&p) {
                                txn.wal_ext_loaded.push(p);
                            }
                        }
                    }
                }
                self.current_db = after;
                Ok(result)
            }
            None => {
                let pre = self.current_db.clone();
                // Bind and run under one lock, so the plan always matches the
                // catalog it executes against (a concurrent DROP + CREATE can
                // never leave stale column indices). Reads bind and run under
                // a shared lock; a write rebinds under the exclusive lock.
                let shared = read_lock(&self.db.shared);
                let (plan, _) = crate::sql::bind_tracked(&shared, stmt, &pre)?;
                let result = if is_read_only(&plan) {
                    let result = crate::exec::run_read(&shared, &plan);
                    // Release before the `USE` logging below re-locks.
                    drop(shared);
                    result?
                } else {
                    drop(shared);
                    let mut shared = write_lock(&self.db.shared);
                    let (plan, dynamic_functions_used) =
                        crate::sql::bind_tracked(&shared, stmt, &pre)?;
                    let result = crate::exec::run(&mut shared, &plan)?;
                    // The write is published: log it while still holding the
                    // lock so WAL order matches commit order.
                    if let Some(text) = sql_text {
                        if crate::plan::writes_tables(&plan) {
                            let after = session_after(&result, &pre, &shared);
                            let touched = shared.dynamic_function_paths(&dynamic_functions_used);
                            let loaded = crate::storage::loaded_extension_path(&plan);
                            shared.wal_append_session(
                                &pre,
                                &after,
                                &[text.to_string()],
                                &touched,
                                &loaded,
                            )?;
                            shared.maybe_auto_checkpoint();
                        }
                    }
                    result
                };
                // `USE` is read-only for locking but still WAL-logged: replay
                // must switch sessions exactly like this connection did.
                // Re-validate the target under this read lock, held across the
                // append: the bind-time check released its lock, so another
                // connection could have dropped the database in between — a
                // logged `USE` for a dead database could never replay.
                if is_read_only(&plan) {
                    if let QueryResult::Used { .. } = &result {
                        let shared = read_lock(&self.db.shared);
                        let after = session_after(&result, &pre, &shared);
                        if !shared.database_exists(&after) {
                            return Err(PinkDbError::Catalog(format!(
                                "database \"{after}\" not found"
                            )));
                        }
                        if let Some(text) = sql_text {
                            shared.wal_append_session(
                                &pre,
                                &after,
                                &[text.to_string()],
                                &[],
                                &[],
                            )?;
                        }
                    }
                }
                // Apply the session transition.
                self.current_db = match &result {
                    QueryResult::Used { db } => db.clone(),
                    QueryResult::DroppedDatabase { name } if name.eq_ignore_ascii_case(&pre) => {
                        read_lock(&self.db.shared).fallback_db()
                    }
                    _ => pre,
                };
                Ok(result)
            }
        }
    }

    fn txn_begin(&mut self) -> Result<QueryResult> {
        if self.txn.is_some() {
            return Err(PinkDbError::Execution(
                "already in a transaction".to_string(),
            ));
        }
        let shared = read_lock(&self.db.shared);
        let mut base_tables = HashMap::new();
        for dbname in shared.list_databases() {
            for name in shared.list_tables_in(&dbname) {
                base_tables.insert(
                    (dbname.to_ascii_lowercase(), name.to_ascii_lowercase()),
                    shared.table_version_in(&dbname, &name)?,
                );
            }
        }
        let mut base_views = HashMap::new();
        for dbname in shared.list_databases() {
            for name in shared.list_views_in(&dbname) {
                base_views.insert(
                    (dbname.to_ascii_lowercase(), name.to_ascii_lowercase()),
                    shared.view_in(&dbname, &name)?.clone(),
                );
            }
        }
        let txn = Txn {
            work: shared.clone(),
            base_tables,
            base_views,
            table_writes: HashSet::new(),
            table_reads: HashSet::new(),
            view_writes: HashSet::new(),
            wal_stmts: Vec::new(),
            wal_ext_paths: Vec::new(),
            wal_ext_loaded: Vec::new(),
            wal_db_before: None,
            wal_db_after: self.current_db.clone(),
            wal_dbs: Vec::new(),
        };
        drop(shared);
        self.txn = Some(txn);
        Ok(QueryResult::Began)
    }

    fn txn_commit(&mut self) -> Result<QueryResult> {
        let mut txn = self
            .txn
            .take()
            .ok_or_else(|| PinkDbError::Execution("no transaction in progress".to_string()))?;
        let mut shared = write_lock(&self.db.shared);
        // A database dropped since BEGIN makes the transaction's WAL record
        // unreplayable (its statements or `USE` targets name it): conflict
        // instead of logging a dead record or silently no-op'ing the merge.
        for (db, name) in &txn.table_writes {
            if !shared.database_exists(db) {
                return Err(conflict(&format!("database \"{db}\"")));
            }
            // Validate tables: our writes must apply onto unchanged state.
            // (Versions equal base ⇒ nobody touched them; created tables must
            // still be absent; dropped ones still present-but-unchanged.)
            let base = txn.base_tables.get(&(db.clone(), name.clone())).copied();
            let now = shared.table_version_in(db, name).ok();
            if base != now {
                return Err(conflict(&format!("table \"{db}.{name}\"")));
            }
        }
        // Serializable reads: a writing transaction's reads must be unchanged
        // too (versions are store-unique, so a drop + recreate is a change).
        // Read-only transactions log nothing and never conflict.
        let writes = !txn.table_writes.is_empty() || !txn.view_writes.is_empty();
        if writes {
            for (db, name) in &txn.table_reads {
                if txn.table_writes.contains(&(db.clone(), name.clone())) {
                    continue;
                }
                let base = txn.base_tables.get(&(db.clone(), name.clone())).copied();
                let now = shared.table_version_in(db, name).ok();
                if base != now {
                    return Err(conflict(&format!("table \"{db}.{name}\" (read)")));
                }
            }
        }
        // Validate views by definition (no version counter on views).
        for (db, name) in &txn.view_writes {
            if !shared.database_exists(db) {
                return Err(conflict(&format!("database \"{db}\"")));
            }
            let base = txn.base_views.get(&(db.clone(), name.clone()));
            let now = shared.view_in(db, name).ok();
            if now != base {
                return Err(conflict(&format!("view \"{db}.{name}\"")));
            }
        }
        // Every session the record's replay switches into (implicit `USE`
        // prefixes and `USE` targets) must still exist.
        for db in &txn.wal_dbs {
            if !shared.database_exists(db) {
                return Err(conflict(&format!("database \"{db}\"")));
            }
        }
        // Validation passed: make the transaction durable first, then merge
        // (the merge itself cannot fail). A conflict abort returns above and
        // therefore never logs.
        if !txn.wal_stmts.is_empty() {
            let before = txn
                .wal_db_before
                .clone()
                .unwrap_or_else(|| txn.wal_db_after.clone());
            shared.wal_append_session(
                &before,
                &txn.wal_db_after,
                &txn.wal_stmts,
                &txn.wal_ext_paths,
                &txn.wal_ext_loaded,
            )?;
        }
        // Merge: swap written tables (carrying bumped versions) and views.
        // The catalog is proven present by the validation above, so a missing
        // catalog here is a hard error, never a silent no-op.
        for (db, name) in &txn.table_writes {
            match txn.work.tables_remove_in(db, name) {
                Some(t) => shared.tables_insert_in(db, name.clone(), t)?,
                None => {
                    shared.tables_remove_in(db, name);
                }
            }
        }
        for (db, name) in &txn.view_writes {
            match txn.work.views_remove_in(db, name) {
                Some(v) => shared.views_insert_in(db, name.clone(), v)?,
                None => {
                    shared.views_remove_in(db, name);
                }
            }
        }
        if !txn.wal_stmts.is_empty() {
            shared.maybe_auto_checkpoint();
        }
        Ok(QueryResult::Committed)
    }

    fn txn_rollback(&mut self) -> Result<QueryResult> {
        match self.txn.take() {
            Some(_) => Ok(QueryResult::RolledBack),
            None => Err(PinkDbError::Execution(
                "no transaction in progress".to_string(),
            )),
        }
    }
}

fn conflict(what: &str) -> PinkDbError {
    PinkDbError::Execution(format!(
        "write conflict on {what}: changed concurrently, retry the transaction"
    ))
}

/// Store/process-level statements refused inside a [`Connection`] transaction
/// (a snapshot commit cannot undo them and would silently discard them).
/// Returns the SQL statement name for the error message; `None` = allowed.
fn refused_in_txn(stmt: &crate::sql::ast::Statement) -> Option<&'static str> {
    use crate::sql::ast::Statement;
    Some(match stmt {
        Statement::CreateDatabase { .. } => "CREATE DATABASE",
        Statement::DropDatabase { .. } => "DROP DATABASE",
        Statement::LoadExtension { .. } => "LOAD EXTENSION",
        Statement::Checkpoint => "CHECKPOINT",
        _ => return None,
    })
}

/// The session database after `result` ran from `before`: `USE` switches to
/// its target; dropping the session's current database falls back to `main`
/// (or the first remaining database); everything else leaves it unchanged.
fn session_after(result: &QueryResult, before: &str, store: &PinkDb) -> String {
    match result {
        QueryResult::Used { db } => db.clone(),
        QueryResult::DroppedDatabase { name } if name.eq_ignore_ascii_case(before) => {
            store.fallback_db()
        }
        _ => before.to_string(),
    }
}

/// Record which `(database, table|view)` pairs a bound plan will mutate.
fn record_writes(
    plan: &LogicalPlan,
    default_db: &str,
    tables: &mut HashSet<(String, String)>,
    views: &mut HashSet<(String, String)>,
) {
    let key = |qdb: &Option<String>, name: &str| {
        (
            qdb.clone()
                .unwrap_or_else(|| default_db.to_string())
                .to_ascii_lowercase(),
            name.to_ascii_lowercase(),
        )
    };
    match plan {
        LogicalPlan::Insert { db, table, .. }
        | LogicalPlan::Update { db, table, .. }
        | LogicalPlan::Delete { db, table, .. } => {
            tables.insert(key(db, table));
        }
        LogicalPlan::CreateTable { db, name, .. } | LogicalPlan::DropTable { db, name } => {
            tables.insert(key(db, name));
        }
        LogicalPlan::CreateView { db, name, .. } | LogicalPlan::DropView { db, name } => {
            views.insert(key(db, name));
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    fn rows_of(mut results: Vec<QueryResult>) -> Vec<Vec<Value>> {
        match results.pop().unwrap() {
            QueryResult::Selected { rows, .. } => rows,
            r => panic!("expected rows, got {r:?}"),
        }
    }

    fn seeded() -> Database {
        let db = Database::new();
        let mut c = db.connect();
        c.execute("CREATE TABLE t (a INTEGER, b VARCHAR);").unwrap();
        c.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y');")
            .unwrap();
        c.execute("CREATE TABLE u (a INTEGER);").unwrap();
        c.execute("INSERT INTO u VALUES (10);").unwrap();
        db
    }

    #[test]
    fn concurrent_readers_agree() {
        let db = seeded();
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let db = db.clone();
                std::thread::spawn(move || {
                    let mut c = db.connect();
                    rows_of(c.execute("SELECT a, b FROM t ORDER BY a;").unwrap())
                })
            })
            .collect();
        let expected = vec![
            vec![Value::Integer(1), Value::Varchar("x".to_string())],
            vec![Value::Integer(2), Value::Varchar("y".to_string())],
        ];
        for h in handles {
            assert_eq!(h.join().unwrap(), expected);
        }
    }

    #[test]
    fn disjoint_writers_both_commit() {
        let db = seeded();
        let db2 = db.clone();
        let h = std::thread::spawn(move || {
            let mut c = db2.connect();
            c.execute("BEGIN;").unwrap();
            c.execute("INSERT INTO t VALUES (3, 'z');").unwrap();
            // Hold the txn briefly so the main thread overlaps.
            std::thread::sleep(std::time::Duration::from_millis(50));
            c.execute("COMMIT;").unwrap();
        });
        let mut c = db.connect();
        c.execute("BEGIN;").unwrap();
        c.execute("INSERT INTO u VALUES (20);").unwrap();
        h.join().unwrap();
        c.execute("COMMIT;").unwrap();
        // Both commits survived (disjoint tables merge cleanly).
        let mut c = db.connect();
        assert_eq!(
            rows_of(c.execute("SELECT COUNT(*) FROM t;").unwrap()),
            vec![vec![Value::Bigint(3)]]
        );
        assert_eq!(
            rows_of(c.execute("SELECT COUNT(*) FROM u;").unwrap()),
            vec![vec![Value::Bigint(2)]]
        );
    }

    #[test]
    fn conflicting_writers_abort_second_committer() {
        let db = seeded();
        // A writes t but does not commit yet.
        let mut a = db.connect();
        a.execute("BEGIN;").unwrap();
        a.execute("INSERT INTO t VALUES (3, 'z');").unwrap();
        // B snapshots the same state, writes t too, commits first (fine —
        // nothing changed underneath it).
        let mut b = db.connect();
        b.execute("BEGIN;").unwrap();
        b.execute("INSERT INTO t VALUES (4, 'w');").unwrap();
        b.execute("COMMIT;").unwrap();
        // A now conflicts (t's version moved without it).
        assert!(a.execute("COMMIT;").is_err());
        // B's row survived; A's did not.
        let mut c = db.connect();
        assert_eq!(
            rows_of(c.execute("SELECT a FROM t ORDER BY a;").unwrap()),
            vec![
                vec![Value::Integer(1)],
                vec![Value::Integer(2)],
                vec![Value::Integer(4)]
            ]
        );
    }

    #[test]
    fn txn_snapshot_isolation() {
        let db = seeded();
        let mut a = db.connect();
        a.execute("BEGIN;").unwrap();
        // B commits while A is open: A keeps seeing the old snapshot,
        // and B never sees A's uncommitted row.
        let mut b = db.connect();
        b.execute("INSERT INTO t VALUES (3, 'z');").unwrap();
        assert_eq!(
            rows_of(a.execute("SELECT COUNT(*) FROM t;").unwrap()),
            vec![vec![Value::Bigint(2)]]
        );
        a.execute("INSERT INTO t VALUES (4, 'w');").unwrap();
        assert_eq!(
            rows_of(b.execute("SELECT COUNT(*) FROM t;").unwrap()),
            vec![vec![Value::Bigint(3)]]
        );
        a.execute("ROLLBACK;").unwrap();
        assert_eq!(
            rows_of(b.execute("SELECT COUNT(*) FROM t;").unwrap()),
            vec![vec![Value::Bigint(3)]]
        );
    }

    #[test]
    fn recreated_table_never_reuses_a_version() {
        let db = seeded();
        let mut a = db.connect();
        a.execute("BEGIN;").unwrap();
        a.execute("INSERT INTO t VALUES (3, 'z');").unwrap();
        // Another session replaces t with a same-shaped table that has seen
        // the same number of mutation batches as the original.
        let mut b = db.connect();
        b.execute("DROP TABLE t;").unwrap();
        b.execute("CREATE TABLE t (a INTEGER, b VARCHAR);").unwrap();
        b.execute("INSERT INTO t VALUES (9, 'n');").unwrap();
        let err = a
            .execute("COMMIT;")
            .expect_err("recreated table must conflict");
        assert!(err.to_string().contains("write conflict"), "{err}");
        assert_eq!(
            rows_of(b.execute("SELECT a FROM t;").unwrap()),
            vec![vec![Value::Integer(9)]]
        );
    }

    #[test]
    fn writing_transaction_conflicts_when_its_reads_changed() {
        let db = seeded();
        let mut a = db.connect();
        a.execute("BEGIN;").unwrap();
        // A's write depends on t, which A does not write.
        a.execute("UPDATE u SET a = (SELECT MAX(a) FROM t);")
            .unwrap();
        let mut b = db.connect();
        b.execute("INSERT INTO t VALUES (50, 'late');").unwrap();
        // Committing would log SQL that replays against the new t (u = 50)
        // while this session computed u = 2: refuse instead.
        let err = a.execute("COMMIT;").expect_err("stale read must conflict");
        assert!(err.to_string().contains("(read)"), "{err}");
        assert_eq!(
            rows_of(b.execute("SELECT a FROM u;").unwrap()),
            vec![vec![Value::Integer(10)]]
        );
        // Read-only transactions never conflict on reads.
        a.execute("BEGIN;").unwrap();
        a.execute("SELECT COUNT(*) FROM t;").unwrap();
        b.execute("INSERT INTO t VALUES (51, 'later');").unwrap();
        a.execute("COMMIT;").unwrap();
        // An unrelated table changing does not abort a writer.
        a.execute("BEGIN;").unwrap();
        a.execute("UPDATE u SET a = (SELECT MAX(a) FROM t);")
            .unwrap();
        b.execute("CREATE TABLE other (x INTEGER);").unwrap();
        a.execute("COMMIT;").unwrap();
        assert_eq!(
            rows_of(b.execute("SELECT a FROM u;").unwrap()),
            vec![vec![Value::Integer(51)]]
        );
    }

    // ---------- multi-database ----------

    fn multi_db() -> Database {
        let db = Database::new();
        let mut c = db.connect();
        c.execute("CREATE DATABASE db1; CREATE DATABASE db2;")
            .unwrap();
        c.execute("CREATE TABLE main.t (a INTEGER, tag VARCHAR);")
            .unwrap();
        c.execute("INSERT INTO main.t VALUES (1, 'main');").unwrap();
        c.execute("CREATE TABLE db1.t (a INTEGER, tag VARCHAR);")
            .unwrap();
        c.execute("INSERT INTO db1.t VALUES (1, 'db1'), (2, 'db1');")
            .unwrap();
        c.execute("CREATE TABLE db2.u (a INTEGER, u VARCHAR);")
            .unwrap();
        c.execute("INSERT INTO db2.u VALUES (1, 'u1'), (1, 'u2');")
            .unwrap();
        db
    }

    #[test]
    fn use_is_per_connection_and_scopes_unqualified_names() {
        let db = multi_db();
        let mut a = db.connect();
        let mut b = db.connect();
        assert_eq!(a.current_db(), "main");
        assert_eq!(b.current_db(), "main");

        // A switches; B is untouched.
        let results = a.execute("USE db1;").unwrap();
        assert_eq!(
            results,
            vec![QueryResult::Used {
                db: "db1".to_string()
            }]
        );
        assert_eq!(a.current_db(), "db1");
        assert_eq!(b.current_db(), "main");
        assert_eq!(
            rows_of(a.execute("SELECT tag FROM t;").unwrap()),
            vec![
                vec![Value::Varchar("db1".to_string())],
                vec![Value::Varchar("db1".to_string())]
            ]
        );
        assert_eq!(
            rows_of(b.execute("SELECT tag FROM t;").unwrap()),
            vec![vec![Value::Varchar("main".to_string())]]
        );
        // The store's embedded session is not mutated by a connection's USE.
        assert_eq!(db.shared.read().unwrap().current_db(), "main");

        // USE to a missing database is a clear bind-time error.
        let err = b.execute("USE nope;").unwrap_err();
        assert!(
            err.to_string().contains("database \"nope\" not found"),
            "{err}"
        );

        // Qualified names resolve regardless of the session.
        assert_eq!(
            rows_of(a.execute("SELECT tag FROM main.t;").unwrap()),
            vec![vec![Value::Varchar("main".to_string())]]
        );
        assert_eq!(
            rows_of(b.execute("SELECT tag FROM db1.t;").unwrap()).len(),
            2
        );
    }

    #[test]
    fn same_table_name_in_two_databases_and_cross_db_join() {
        let db = multi_db();
        let mut c = db.connect();
        // Unqualified picks the session's table; qualified picks the database's.
        assert_eq!(
            rows_of(c.execute("SELECT tag FROM t;").unwrap()),
            vec![vec![Value::Varchar("main".to_string())]]
        );
        assert_eq!(
            rows_of(c.execute("SELECT tag FROM db1.t;").unwrap()).len(),
            2
        );
        // Cross-database join: each side resolves independently.
        let rows = rows_of(
            c.execute(
                "SELECT t.tag, u.u FROM db1.t JOIN db2.u ON t.a = u.a \
                 ORDER BY u.u;",
            )
            .unwrap(),
        );
        assert_eq!(
            rows,
            vec![
                vec![
                    Value::Varchar("db1".to_string()),
                    Value::Varchar("u1".to_string())
                ],
                vec![
                    Value::Varchar("db1".to_string()),
                    Value::Varchar("u2".to_string())
                ],
            ]
        );
        // Three-part column references stay a parse error.
        let err = c.execute("SELECT db1.t.a FROM db1.t;").unwrap_err();
        assert!(err.to_string().contains("three-part"), "{err}");
    }

    #[test]
    fn create_and_drop_database_rules() {
        let db = Database::new();
        let mut c = db.connect();
        // The last database cannot be dropped.
        let err = c.execute("DROP DATABASE main;").unwrap_err();
        assert!(err.to_string().contains("last database"), "{err}");

        assert_eq!(
            c.execute("CREATE DATABASE Analytics;").unwrap(),
            vec![QueryResult::CreatedDatabase {
                name: "Analytics".to_string()
            }]
        );
        let err = c.execute("CREATE DATABASE analytics;").unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");
        let err = c.execute("DROP DATABASE nope;").unwrap_err();
        assert!(err.to_string().contains("not found"), "{err}");

        // Dropping the session's current database falls back to main.
        c.execute("USE analytics;").unwrap();
        assert_eq!(c.current_db(), "analytics");
        assert_eq!(
            c.execute("DROP DATABASE analytics;").unwrap(),
            vec![QueryResult::DroppedDatabase {
                name: "analytics".to_string()
            }]
        );
        assert_eq!(c.current_db(), "main");
        assert_eq!(c.execute("SELECT 1;").unwrap().len(), 1);

        // Dropping main while another database exists is allowed and the
        // session moves to the first remaining database.
        c.execute("CREATE DATABASE db1; CREATE DATABASE db2;")
            .unwrap();
        assert_eq!(
            c.execute("DROP DATABASE main;").unwrap(),
            vec![QueryResult::DroppedDatabase {
                name: "main".to_string()
            }]
        );
        assert_eq!(
            c.current_db(),
            "db1",
            "fallback is the first sorted database"
        );
        // Now db1 is the current database and can only be dropped if another
        // remains; drop db2 first (not current), then db1 is the last.
        c.execute("DROP DATABASE db2;").unwrap();
        let err = c.execute("DROP DATABASE db1;").unwrap_err();
        assert!(err.to_string().contains("last database"), "{err}");
    }

    #[test]
    fn qualified_dml_ddl_and_per_database_views() {
        let db = multi_db();
        let mut c = db.connect();
        c.execute("INSERT INTO db1.t VALUES (3, 'new');").unwrap();
        c.execute("UPDATE db1.t SET tag = 'up' WHERE a = 3;")
            .unwrap();
        assert_eq!(
            rows_of(c.execute("SELECT tag FROM db1.t WHERE a = 3;").unwrap()),
            vec![vec![Value::Varchar("up".to_string())]]
        );
        c.execute("DELETE FROM db1.t WHERE a = 3;").unwrap();
        c.execute("CREATE TABLE db2.v (x BIGINT);").unwrap();
        c.execute("INSERT INTO db2.v VALUES (9);").unwrap();
        c.execute("DROP TABLE db2.v;").unwrap();
        assert!(c.execute("SELECT * FROM db2.v;").is_err());

        // Views are created in the session's database and resolved there.
        c.execute("USE db1;").unwrap();
        c.execute("CREATE VIEW tview AS SELECT tag FROM t;")
            .unwrap();
        assert_eq!(rows_of(c.execute("SELECT * FROM tview;").unwrap()).len(), 2);
        c.execute("USE main;").unwrap();
        assert!(
            c.execute("SELECT * FROM tview;").is_err(),
            "not visible from main"
        );
        // ... but reachable qualified from another session database.
        assert_eq!(
            rows_of(c.execute("SELECT * FROM db1.tview;").unwrap()).len(),
            2
        );
        // The view's body resolves `t` in db1 even though the session is main.
        c.execute("DROP VIEW db1.tview;").unwrap();
        assert!(c.execute("SELECT * FROM db1.tview;").is_err());
    }

    #[test]
    fn database_ddl_is_refused_inside_a_transaction() {
        let db = Database::new();
        let mut c = db.connect();
        c.execute("BEGIN;").unwrap();
        let err = c.execute("CREATE DATABASE db1;").unwrap_err();
        assert!(
            err.to_string().contains("not allowed inside a transaction"),
            "{err}"
        );
        let err = c.execute("DROP DATABASE main;").unwrap_err();
        assert!(
            err.to_string().contains("not allowed inside a transaction"),
            "{err}"
        );
        // LOAD EXTENSION would only register in the private snapshot and then
        // be discarded at COMMIT: refused like the database DDL above.
        let err = c
            .execute("LOAD EXTENSION '/does/not/exist.dylib';")
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "LOAD EXTENSION is not allowed inside a transaction"
        );
        c.execute("ROLLBACK;").unwrap();
        c.execute("CREATE DATABASE db1;").unwrap();
    }

    /// P1: a caller's CTE must not leak into a view body. The view's `FROM x`
    /// resolves the catalog table, not the caller's `WITH x` (DuckDB
    /// semantics: view bodies are self-contained).
    #[test]
    fn caller_ctes_do_not_leak_into_view_bodies() {
        let db = Database::new();
        let mut c = db.connect();
        c.execute("CREATE DATABASE db1;").unwrap();
        c.execute("CREATE TABLE db1.x (v INTEGER);").unwrap();
        c.execute("INSERT INTO db1.x VALUES (1);").unwrap();
        c.execute("CREATE VIEW db1.v AS SELECT * FROM x;").unwrap();
        // The caller's CTE is named like the view body's table.
        assert_eq!(
            rows_of(
                c.execute("WITH x AS (SELECT 99 AS v) SELECT * FROM db1.v;")
                    .unwrap()
            ),
            vec![vec![Value::Integer(1)]],
            "the view body must resolve the table, not the caller's CTE"
        );
        // A CTE the view does not reference is unaffected.
        assert_eq!(
            rows_of(
                c.execute("WITH y AS (SELECT 7 AS v) SELECT * FROM db1.v;")
                    .unwrap()
            ),
            vec![vec![Value::Integer(1)]]
        );
        // The view's own CTEs still bind and resolve.
        c.execute("CREATE VIEW db1.w AS WITH z AS (SELECT 5 AS v) SELECT * FROM z;")
            .unwrap();
        assert_eq!(
            rows_of(c.execute("SELECT * FROM db1.w;").unwrap()),
            vec![vec![Value::Integer(5)]]
        );
    }

    #[test]
    fn transactions_work_across_databases() {
        let db = multi_db();
        let mut a = db.connect();
        a.execute("BEGIN;").unwrap();
        a.execute("USE db1;").unwrap();
        a.execute("INSERT INTO t VALUES (10, 'txn');").unwrap();
        a.execute("USE main;").unwrap();
        a.execute("INSERT INTO t VALUES (20, 'txn');").unwrap();
        // Not visible outside the transaction yet.
        let mut b = db.connect();
        assert_eq!(
            rows_of(b.execute("SELECT COUNT(*) FROM db1.t;").unwrap()),
            vec![vec![Value::Bigint(2)]]
        );
        assert_eq!(
            rows_of(b.execute("SELECT COUNT(*) FROM main.t;").unwrap()),
            vec![vec![Value::Bigint(1)]]
        );
        a.execute("COMMIT;").unwrap();
        assert_eq!(
            rows_of(b.execute("SELECT COUNT(*) FROM db1.t;").unwrap()),
            vec![vec![Value::Bigint(3)]]
        );
        assert_eq!(
            rows_of(b.execute("SELECT COUNT(*) FROM main.t;").unwrap()),
            vec![vec![Value::Bigint(2)]]
        );
        // The transaction switched only its own session.
        assert_eq!(a.current_db(), "main");
        assert_eq!(b.current_db(), "main");
    }

    // ---------- robustness: depth guard + poisoned-lock recovery ----------

    /// `SELECT 1+1+...;` with `terms - 1` additions.
    fn chain_sql(terms: usize) -> String {
        let mut sql = String::from("SELECT 1");
        for _ in 1..terms {
            sql.push_str("+1");
        }
        sql.push(';');
        sql
    }

    /// The AST-depth guard at the engine level: a chain of
    /// `MAX_AST_DEPTH - 3` terms sits exactly at the cap (statement + Select +
    /// projection + one node per term), one more term is rejected cleanly,
    /// and the connection keeps working either way.
    #[test]
    fn ast_depth_cap_boundary_through_the_engine() {
        let db = Database::new();
        let mut c = db.connect();
        let at_cap = chain_sql(crate::sql::MAX_AST_DEPTH - 3);
        assert_eq!(
            rows_of(c.execute(&at_cap).unwrap()),
            vec![vec![Value::Integer((crate::sql::MAX_AST_DEPTH - 3) as i32)]]
        );
        let over_cap = chain_sql(crate::sql::MAX_AST_DEPTH - 2);
        let err = c.execute(&over_cap).unwrap_err();
        assert!(
            err.to_string().contains("expression too deep"),
            "unexpected error: {err}"
        );
        assert_eq!(
            rows_of(c.execute("SELECT 2;").unwrap()),
            vec![vec![Value::Integer(2)]]
        );
    }

    /// A ~10k-term chain (far past the recursive-drop crash point on this
    /// 2 MiB test thread) is a clean error, and the store stays usable.
    #[test]
    fn over_deep_chain_is_a_clean_error_and_leaves_the_store_usable() {
        let db = Database::new();
        let mut c = db.connect();
        c.execute("CREATE TABLE t (a INTEGER);").unwrap();
        let mut sql = String::from("INSERT INTO t VALUES (1");
        for _ in 0..10_000 {
            sql.push_str("+1");
        }
        sql.push_str(");");
        let err = c.execute(&sql).unwrap_err();
        assert!(err.to_string().contains("expression too deep"), "{err}");
        c.execute("INSERT INTO t VALUES (7);").unwrap();
        assert_eq!(
            rows_of(c.execute("SELECT a FROM t;").unwrap()),
            vec![vec![Value::Integer(7)]]
        );
    }

    /// A panic while holding the write lock poisons it; statements must keep
    /// working afterwards instead of failing forever.
    #[test]
    fn poisoned_lock_is_recovered_and_statements_still_work() {
        let db = seeded();
        let db2 = db.clone();
        let poisoned = std::thread::spawn(move || {
            let _guard = db2.shared.write().expect("unpoisoned write lock");
            panic!("poison the store lock on purpose");
        });
        assert!(poisoned.join().is_err(), "the poisoning thread must panic");
        assert!(db.shared.is_poisoned(), "the write lock must be poisoned");

        // Reads and writes recover with the one-time loud warning.
        let mut c = db.connect();
        assert_eq!(
            rows_of(c.execute("SELECT COUNT(*) FROM t;").unwrap()),
            vec![vec![Value::Bigint(2)]]
        );
        c.execute("INSERT INTO t VALUES (3, 'z');").unwrap();
        assert_eq!(
            rows_of(c.execute("SELECT COUNT(*) FROM t;").unwrap()),
            vec![vec![Value::Bigint(3)]]
        );
        // Transactions take the same path.
        c.execute("BEGIN; INSERT INTO t VALUES (4, 'w'); COMMIT;")
            .unwrap();
        assert_eq!(
            rows_of(c.execute("SELECT COUNT(*) FROM t;").unwrap()),
            vec![vec![Value::Bigint(4)]]
        );
    }
}
