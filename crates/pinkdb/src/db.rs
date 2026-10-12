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
//! This is snapshot isolation without predicate tracking: write-write
//! conflicts abort, but write-skew anomalies are possible. Transactions hold
//! no locks while open (only validation+merge serializes at commit).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::plan::{LogicalPlan, is_read_only};
use crate::storage::{PinkDb, ViewDef};
use crate::types::{PinkDbError, Result};
use crate::QueryResult;

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

    pub fn from_store(store: PinkDb) -> Self {
        Self { shared: Arc::new(RwLock::new(store)) }
    }

    pub fn connect(&self) -> Connection {
        Connection { db: self.clone(), txn: None }
    }

    /// True when this database logs commits to a WAL.
    pub fn is_durable(&self) -> bool {
        self.shared.read().map(|g| g.is_durable()).unwrap_or(false)
    }

    /// Checkpoint the durable store: atomically write the bound `.pink` and
    /// roll the WAL generation. Errors for in-memory databases.
    pub fn checkpoint(&self) -> Result<()> {
        let mut shared = self.shared.write().map_err(lock_err)?;
        shared.checkpoint()
    }

    /// fsync the WAL (meaningful with [`crate::SyncPolicy::Batch`]).
    pub fn flush_wal(&self) -> Result<()> {
        let mut shared = self.shared.write().map_err(lock_err)?;
        shared.flush_wal()
    }
}

struct Txn {
    /// Private working copy (snapshot at BEGIN + own writes).
    work: PinkDb,
    /// Table versions and view definitions frozen at BEGIN.
    base_tables: HashMap<String, u64>,
    base_views: HashMap<String, ViewDef>,
    /// Tables / views created, dropped, or written.
    table_writes: HashSet<String>,
    view_writes: HashSet<String>,
    /// SQL text of this transaction's mutating statements: one WAL record at
    /// COMMIT (empty for read-only transactions, which log nothing).
    wal_stmts: Vec<String>,
}

/// One connection: single-threaded statement stream, own transaction state.
pub struct Connection {
    db: Database,
    txn: Option<Txn>,
}

impl Connection {
    /// Parse and run every statement, returning one result each.
    /// Multi-statement strings mix freely (each statement locks as needed).
    pub fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>> {
        let stmts = crate::sql::parse_statements(sql)?;
        // Only durable stores need the per-statement source text for the WAL.
        let texts = if self.db.is_durable() {
            let texts = crate::sql::statement_texts(sql)?;
            if texts.len() != stmts.len() {
                return Err(PinkDbError::Execution(
                    "internal error: statement source split disagrees with the parser"
                        .to_string(),
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
                let plan = crate::sql::bind(&txn.work, stmt)?;
                record_writes(&plan, &mut txn.table_writes, &mut txn.view_writes);
                let result = crate::exec::run(&mut txn.work, &plan)?;
                // Buffer the committed statement for the transaction's single
                // WAL record (flushed by txn_commit; never by ROLLBACK).
                if let Some(text) = sql_text {
                    if crate::plan::writes_tables(&plan) {
                        txn.wal_stmts.push(text.to_string());
                    }
                }
                Ok(result)
            }
            None => {
                // Bind under a read lock; run reads shared, writes exclusive.
                // (A concurrent DROP between bind and run surfaces as a clean
                // "not found" error — never corruption.)
                let plan = {
                    let shared = self.db.shared.read().map_err(lock_err)?;
                    crate::sql::bind(&shared, stmt)?
                };
                if is_read_only(&plan) {
                    let shared = self.db.shared.read().map_err(lock_err)?;
                    crate::exec::run_read(&shared, &plan)
                } else {
                    let mut shared = self.db.shared.write().map_err(lock_err)?;
                    let result = crate::exec::run(&mut shared, &plan)?;
                    // The write is published: log it while still holding the
                    // lock so WAL order matches commit order.
                    if let Some(text) = sql_text {
                        if crate::plan::writes_tables(&plan) {
                            shared.wal_append(&[text.to_string()])?;
                        }
                    }
                    Ok(result)
                }
            }
        }
    }

    fn txn_begin(&mut self) -> Result<QueryResult> {
        if self.txn.is_some() {
            return Err(PinkDbError::Execution("already in a transaction".to_string()));
        }
        let shared = self.db.shared.read().map_err(lock_err)?;
        let mut base_tables = HashMap::new();
        for name in shared.list_tables() {
            base_tables.insert(name.clone(), shared.table_version(&name)?);
        }
        let txn = Txn {
            work: shared.clone(),
            base_tables,
            base_views: shared.clone_views(),
            table_writes: HashSet::new(),
            view_writes: HashSet::new(),
            wal_stmts: Vec::new(),
        };
        drop(shared);
        self.txn = Some(txn);
        Ok(QueryResult::Began)
    }

    fn txn_commit(&mut self) -> Result<QueryResult> {
        let mut txn = self.txn.take().ok_or_else(|| {
            PinkDbError::Execution("no transaction in progress".to_string())
        })?;
        let mut shared = self.db.shared.write().map_err(lock_err)?;
        // Validate tables: our writes must apply onto unchanged state.
        // (Versions equal base ⇒ nobody touched them; created tables must
        // still be absent; dropped ones still present-but-unchanged.)
        for name in &txn.table_writes {
            let base = txn.base_tables.get(name).copied();
            let now = shared.table_version(name).ok();
            if base != now {
                return Err(conflict(&format!("table \"{name}\"")));
            }
        }
        // Validate views by definition (no version counter on views).
        for name in &txn.view_writes {
            let key = name.to_ascii_lowercase();
            let base = txn.base_views.get(&key);
            let now = shared.view(&key).ok();
            if now != base {
                return Err(conflict(&format!("view \"{name}\"")));
            }
        }
        // Validation passed: make the transaction durable first, then merge
        // (the merge itself cannot fail). A conflict abort returns above and
        // therefore never logs.
        if !txn.wal_stmts.is_empty() {
            shared.wal_append(&txn.wal_stmts)?;
        }
        // Merge: swap written tables (carrying bumped versions) and views.
        for name in &txn.table_writes {
            let key = name.to_ascii_lowercase();
            match txn.work.tables_remove(&key) {
                Some(t) => shared.tables_insert(key, t),
                None => {
                    shared.tables_remove(&key);
                }
            }
        }
        for name in &txn.view_writes {
            let key = name.to_ascii_lowercase();
            match txn.work.views_remove(&key) {
                Some(v) => shared.views_insert(key, v),
                None => {
                    shared.views_remove(&key);
                }
            }
        }
        Ok(QueryResult::Committed)
    }

    fn txn_rollback(&mut self) -> Result<QueryResult> {
        match self.txn.take() {
            Some(_) => Ok(QueryResult::RolledBack),
            None => Err(PinkDbError::Execution("no transaction in progress".to_string())),
        }
    }
}

fn conflict(what: &str) -> PinkDbError {
    PinkDbError::Execution(format!(
        "write conflict on {what}: changed concurrently, retry the transaction"
    ))
}

fn lock_err<T>(_: T) -> PinkDbError {
    PinkDbError::Execution("internal lock error (poisoned)".to_string())
}
/// Record which tables/views a bound plan will mutate.
fn record_writes(
    plan: &LogicalPlan,
    tables: &mut HashSet<String>,
    views: &mut HashSet<String>,
) {
    match plan {
        LogicalPlan::Insert { table, .. }
        | LogicalPlan::Update { table, .. }
        | LogicalPlan::Delete { table, .. }
        | LogicalPlan::CreateTable { name: table, .. }
        | LogicalPlan::DropTable { name: table } => {
            tables.insert(table.clone());
        }
        LogicalPlan::CreateView { name, .. } | LogicalPlan::DropView { name } => {
            views.insert(name.clone());
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
        c.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y');").unwrap();
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
        assert_eq!(rows_of(c.execute("SELECT COUNT(*) FROM t;").unwrap()), vec![vec![Value::Bigint(3)]]);
        assert_eq!(rows_of(c.execute("SELECT COUNT(*) FROM u;").unwrap()), vec![vec![Value::Bigint(2)]]);
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
        // A now conflicts (t moved from v0 to v1 without it).
        assert!(a.execute("COMMIT;").is_err());
        // B's row survived; A's did not.
        let mut c = db.connect();
        assert_eq!(
            rows_of(c.execute("SELECT a FROM t ORDER BY a;").unwrap()),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(4)]]
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
}
