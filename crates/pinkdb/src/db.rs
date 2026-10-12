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
        // Start on the store's current database (fresh stores: `main`).
        let current_db = self
            .shared
            .read()
            .map(|g| g.current_db().to_string())
            .unwrap_or_else(|_| "main".to_string());
        Connection { db: self.clone(), txn: None, current_db }
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
    /// `(database, table)` versions and view definitions frozen at BEGIN.
    base_tables: HashMap<(String, String), u64>,
    base_views: HashMap<(String, String), ViewDef>,
    /// Tables / views created, dropped, or written, keyed `(database, name)`.
    table_writes: HashSet<(String, String)>,
    view_writes: HashSet<(String, String)>,
    /// SQL text of this transaction's mutating statements: one WAL record at
    /// COMMIT (empty for read-only transactions, which log nothing).
    wal_stmts: Vec<String>,
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
                // Store-level DDL is refused inside a transaction: the
                // per-table snapshot merge cannot undo a whole database.
                if matches!(
                    stmt,
                    crate::sql::ast::Statement::CreateDatabase { .. }
                        | crate::sql::ast::Statement::DropDatabase { .. }
                ) {
                    return Err(PinkDbError::Execution(format!(
                        "{} is not allowed inside a transaction",
                        if matches!(stmt, crate::sql::ast::Statement::CreateDatabase { .. }) {
                            "CREATE DATABASE"
                        } else {
                            "DROP DATABASE"
                        }
                    )));
                }
                let pre = self.current_db.clone();
                let plan = crate::sql::bind(&txn.work, stmt, &pre)?;
                record_writes(&plan, &pre, &mut txn.table_writes, &mut txn.view_writes);
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
                    }
                }
                self.current_db = after;
                Ok(result)
            }
            None => {
                let pre = self.current_db.clone();
                // Bind under a read lock; run reads shared, writes exclusive.
                // (A concurrent DROP between bind and run surfaces as a clean
                // "not found" error — never corruption.)
                let plan = {
                    let shared = self.db.shared.read().map_err(lock_err)?;
                    crate::sql::bind(&shared, stmt, &pre)?
                };
                let result = if is_read_only(&plan) {
                    let shared = self.db.shared.read().map_err(lock_err)?;
                    crate::exec::run_read(&shared, &plan)?
                } else {
                    let mut shared = self.db.shared.write().map_err(lock_err)?;
                    let result = crate::exec::run(&mut shared, &plan)?;
                    // The write is published: log it while still holding the
                    // lock so WAL order matches commit order.
                    if let Some(text) = sql_text {
                        if crate::plan::writes_tables(&plan) {
                            let after = session_after(&result, &pre, &shared);
                            shared.wal_append_session(&pre, &after, &[text.to_string()])?;
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
                        let shared = self.db.shared.read().map_err(lock_err)?;
                        let after = session_after(&result, &pre, &shared);
                        if !shared.database_exists(&after) {
                            return Err(PinkDbError::Catalog(format!(
                                "database \"{after}\" not found"
                            )));
                        }
                        if let Some(text) = sql_text {
                            shared.wal_append_session(&pre, &after, &[text.to_string()])?;
                        }
                    }
                }
                // Apply the session transition.
                self.current_db = match &result {
                    QueryResult::Used { db } => db.clone(),
                    QueryResult::DroppedDatabase { name }
                        if name.eq_ignore_ascii_case(&pre) =>
                    {
                        self.db.shared.read().map_err(lock_err)?.fallback_db()
                    }
                    _ => pre,
                };
                Ok(result)
            }
        }
    }

    fn txn_begin(&mut self) -> Result<QueryResult> {
        if self.txn.is_some() {
            return Err(PinkDbError::Execution("already in a transaction".to_string()));
        }
        let shared = self.db.shared.read().map_err(lock_err)?;
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
            view_writes: HashSet::new(),
            wal_stmts: Vec::new(),
            wal_db_before: None,
            wal_db_after: self.current_db.clone(),
            wal_dbs: Vec::new(),
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
            shared.wal_append_session(&before, &txn.wal_db_after, &txn.wal_stmts)?;
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

    // ---------- multi-database ----------

    fn multi_db() -> Database {
        let db = Database::new();
        let mut c = db.connect();
        c.execute("CREATE DATABASE db1; CREATE DATABASE db2;").unwrap();
        c.execute("CREATE TABLE main.t (a INTEGER, tag VARCHAR);").unwrap();
        c.execute("INSERT INTO main.t VALUES (1, 'main');").unwrap();
        c.execute("CREATE TABLE db1.t (a INTEGER, tag VARCHAR);").unwrap();
        c.execute("INSERT INTO db1.t VALUES (1, 'db1'), (2, 'db1');").unwrap();
        c.execute("CREATE TABLE db2.u (a INTEGER, u VARCHAR);").unwrap();
        c.execute("INSERT INTO db2.u VALUES (1, 'u1'), (1, 'u2');").unwrap();
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
        assert_eq!(results, vec![QueryResult::Used { db: "db1".to_string() }]);
        assert_eq!(a.current_db(), "db1");
        assert_eq!(b.current_db(), "main");
        assert_eq!(
            rows_of(a.execute("SELECT tag FROM t;").unwrap()),
            vec![vec![Value::Varchar("db1".to_string())], vec![Value::Varchar("db1".to_string())]]
        );
        assert_eq!(
            rows_of(b.execute("SELECT tag FROM t;").unwrap()),
            vec![vec![Value::Varchar("main".to_string())]]
        );
        // The store's embedded session is not mutated by a connection's USE.
        assert_eq!(db.shared.read().unwrap().current_db(), "main");

        // USE to a missing database is a clear bind-time error.
        let err = b.execute("USE nope;").unwrap_err();
        assert!(err.to_string().contains("database \"nope\" not found"), "{err}");

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
        assert_eq!(rows_of(c.execute("SELECT tag FROM db1.t;").unwrap()).len(), 2);
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
                vec![Value::Varchar("db1".to_string()), Value::Varchar("u1".to_string())],
                vec![Value::Varchar("db1".to_string()), Value::Varchar("u2".to_string())],
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
            vec![QueryResult::CreatedDatabase { name: "Analytics".to_string() }]
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
            vec![QueryResult::DroppedDatabase { name: "analytics".to_string() }]
        );
        assert_eq!(c.current_db(), "main");
        assert_eq!(c.execute("SELECT 1;").unwrap().len(), 1);

        // Dropping main while another database exists is allowed and the
        // session moves to the first remaining database.
        c.execute("CREATE DATABASE db1; CREATE DATABASE db2;").unwrap();
        assert_eq!(
            c.execute("DROP DATABASE main;").unwrap(),
            vec![QueryResult::DroppedDatabase { name: "main".to_string() }]
        );
        assert_eq!(c.current_db(), "db1", "fallback is the first sorted database");
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
        c.execute("UPDATE db1.t SET tag = 'up' WHERE a = 3;").unwrap();
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
        c.execute("CREATE VIEW tview AS SELECT tag FROM t;").unwrap();
        assert_eq!(rows_of(c.execute("SELECT * FROM tview;").unwrap()).len(), 2);
        c.execute("USE main;").unwrap();
        assert!(c.execute("SELECT * FROM tview;").is_err(), "not visible from main");
        // ... but reachable qualified from another session database.
        assert_eq!(rows_of(c.execute("SELECT * FROM db1.tview;").unwrap()).len(), 2);
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
        assert!(err.to_string().contains("not allowed inside a transaction"), "{err}");
        let err = c.execute("DROP DATABASE main;").unwrap_err();
        assert!(err.to_string().contains("not allowed inside a transaction"), "{err}");
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
            rows_of(c.execute("WITH x AS (SELECT 99 AS v) SELECT * FROM db1.v;").unwrap()),
            vec![vec![Value::Integer(1)]],
            "the view body must resolve the table, not the caller's CTE"
        );
        // A CTE the view does not reference is unaffected.
        assert_eq!(
            rows_of(c.execute("WITH y AS (SELECT 7 AS v) SELECT * FROM db1.v;").unwrap()),
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
        assert_eq!(rows_of(b.execute("SELECT COUNT(*) FROM db1.t;").unwrap()), vec![vec![Value::Bigint(2)]]);
        assert_eq!(rows_of(b.execute("SELECT COUNT(*) FROM main.t;").unwrap()), vec![vec![Value::Bigint(1)]]);
        a.execute("COMMIT;").unwrap();
        assert_eq!(rows_of(b.execute("SELECT COUNT(*) FROM db1.t;").unwrap()), vec![vec![Value::Bigint(3)]]);
        assert_eq!(rows_of(b.execute("SELECT COUNT(*) FROM main.t;").unwrap()), vec![vec![Value::Bigint(2)]]);
        // The transaction switched only its own session.
        assert_eq!(a.current_db(), "main");
        assert_eq!(b.current_db(), "main");
    }
}
