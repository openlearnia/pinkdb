//! Multi-writer connections over a shared store.
//!
//! [`Database`] is a cheaply cloneable handle to one store shared between
//! threads (`Arc<RwLock<PinkDb>>`). Each thread works through its own
//! [`Connection`]:
//!
//! - Outside a transaction (autocommit): queries run under a shared read
//!   lock (readers never block each other); writes execute under the write
//!   lock and commit one statement.
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
//! Read-only transactions log nothing and never conflict.
//!
//! Across processes (waves 45m/45o), the lease is held only for short spans,
//! never across statement/transaction execution. A durable autocommit write
//! executes without the lease, then commits under it: the lease acquisition
//! is followed by frame-based validation (each WAL frame records the set of
//! tables its statements read or wrote, payload v4) and, on a conflict, an
//! undo (reload the committed state) plus a pessimistic re-execution under
//! the held lease — exactly one effect reaches the WAL. A durable `BEGIN`
//! captures a base `(wal_id, generation, applied LSN, checkpoint identity)`
//! and holds no lease; `COMMIT` takes the lease, validates the transaction's
//! read∪write set against every frame committed after the base (a conflict
//! names the object and the frame's LSN, the cross-process "version"), and
//! applies the frames that arrived for other tables before appending. A
//! checkpoint, a recovered WAL, or a replaced `.pink` invalidates the base —
//! a retryable conflict, never a silent append onto another incarnation.
//! The write guard may be held while acquiring the lease (the optimistic
//! paths do); the in-process gate makes that safe (see the `Lease` docs).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use crate::QueryResult;
use crate::crypto::EncryptionKey;
use crate::plan::{LogicalPlan, is_read_only};
use crate::storage::{PinkDb, ViewDef};
use crate::types::{PinkDbError, Result};
use crate::wal::{BaseToken, DurableOpenOptions, Lease, LeaseGuard};

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
    /// The durable write lease, copied from the store at construction so the
    /// per-statement freshness precheck needs no store lock.
    lease: Option<Arc<Lease>>,
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a durable database at `path`: load the `.pink` checkpoint (if
    /// present), replay `<path>.pink.wal` (stopping at a torn tail), and log
    /// every subsequent commit to that WAL. Any number of local processes may
    /// open the same database; writers serialize per operation.
    pub fn open(path: &str) -> Result<Database> {
        Ok(Database::from_store(PinkDb::open_durable(path)?))
    }

    /// [`Database::open`] with the full durable-open options (sync policy,
    /// busy timeout, lock mode).
    pub fn open_with_options(path: &str, opts: DurableOpenOptions) -> Result<Database> {
        Ok(Database::from_store(
            PinkDb::open_durable_with_options(path, opts)?.0,
        ))
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

    /// [`Database::open_with_key`] with the full durable-open options.
    pub fn open_with_key_and_options(
        path: &str,
        key: &EncryptionKey,
        opts: DurableOpenOptions,
    ) -> Result<Database> {
        Ok(Database::from_store(
            PinkDb::open_durable_with_key_and_options(path, key, opts)?.0,
        ))
    }

    pub fn from_store(store: PinkDb) -> Self {
        let lease = store.lease_handle();
        Self {
            shared: Arc::new(RwLock::new(store)),
            lease,
        }
    }

    pub fn connect(&self) -> Connection {
        // Start on the store's current database (fresh stores: `main`).
        let current_db = read_lock(&self.shared).current_db().to_string();
        Connection {
            db: self.clone(),
            txn: None,
            current_db,
            // A plain connect is trusted (see `Connection::principal`).
            // Network front-ends authenticate with `Connection::login`; the
            // store's own policy switch is `Database::has_users`.
            principal: None,
        }
    }

    /// Is any user defined? The network server's authentication switch:
    /// while this is `false`, every connection is trusted (the documented
    /// empty-catalog policy).
    pub fn has_users(&self) -> bool {
        read_lock(&self.shared).has_users()
    }

    /// True when this database logs commits to a WAL.
    pub fn is_durable(&self) -> bool {
        read_lock(&self.shared).is_durable()
    }

    /// How long an operation waits for another process's write lease before
    /// reporting the database busy (default 5 s; no-op in memory).
    pub fn set_busy_timeout(&self, timeout: Duration) {
        if let Some(lease) = &self.lease {
            lease.set_busy_timeout(timeout);
        }
    }

    /// Run `f` against the shared store under a read lock (crate-internal:
    /// prepared-statement bind needs the catalog a statement will see).
    pub(crate) fn read_store<R>(&self, f: impl FnOnce(&PinkDb) -> R) -> R {
        f(&read_lock(&self.shared))
    }

    /// Acquire the write lease (`None` in memory). Invariant I6: callers
    /// take the lease *before* the store write lock.
    fn acquire_lease(&self) -> Result<Option<LeaseGuard>> {
        match &self.lease {
            Some(lease) => Ok(Some(lease.acquire()?)),
            None => Ok(None),
        }
    }

    /// Refresh the shared store if another process published something (or
    /// replaced a file). No-op when the epoch says nothing changed.
    fn refresh(&self) -> Result<()> {
        let Some(lease) = self.lease.clone() else {
            return Ok(());
        };
        if !lease.stale() {
            return Ok(());
        }
        let guard = lease.acquire()?;
        let mut shared = write_lock(&self.shared);
        shared.refresh_under_lease(&guard)?;
        Ok(())
    }

    /// The statement-boundary automatic materialized-view pass (wave 80) for
    /// connections: take the lease (durable only), refresh the store first (a
    /// WAL append below must land after every earlier commit), recompute every
    /// due view, and log the batch as one record. A statement with no due view
    /// pays one read-locked scan and nothing else.
    fn boundary_mv_refresh(&self) -> Result<()> {
        {
            let store = read_lock(&self.shared);
            if !store.has_materialized_views() {
                return Ok(());
            }
            if !store.mv_boundary_has_work(crate::mv::now_ms()) {
                // Nothing due: drop stale dirty marks so a store with views
                // nobody depends on does not accumulate them.
                drop(store);
                write_lock(&self.shared).mv_pending.clear();
                return Ok(());
            }
        }
        let lease = self.acquire_lease()?;
        let mut shared = write_lock(&self.shared);
        if let Some(g) = &lease {
            // Another process's commits may carry changed definitions.
            shared.refresh_under_lease(g)?;
        }
        let now = crate::mv::now_ms();
        if !shared.mv_boundary_has_work(now) {
            shared.mv_pending.clear();
        } else {
            let report = shared.refresh_due_auto_views(now);
            if !report.stmts.is_empty() {
                shared.log_auto_refresh(&report)?;
            }
        }
        shared.maybe_auto_checkpoint(lease.as_ref());
        drop(shared);
        drop(lease);
        Ok(())
    }

    /// Checkpoint the durable store: atomically write the bound `.pink` and
    /// roll the WAL generation. Errors for in-memory databases. Takes the
    /// lease before the store lock; the checkpoint refreshes first, so it can
    /// never omit another process's committed records.
    pub fn checkpoint(&self) -> Result<()> {
        let guard = self.acquire_lease()?;
        let mut shared = write_lock(&self.shared);
        shared.checkpoint_with_guard(guard.as_ref())
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

    /// fsync the WAL (meaningful with [`crate::SyncPolicy::Batch`]). Takes
    /// the lease first: a flush is the durability point other processes
    /// wait for.
    pub fn flush_wal(&self) -> Result<()> {
        let guard = self.acquire_lease()?;
        let mut shared = write_lock(&self.shared);
        shared.flush_wal_holding_lease()?;
        drop(shared);
        drop(guard);
        Ok(())
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
    /// The buffered (mutating) statements' touched-object set (wave 45o):
    /// the transaction's one WAL frame records exactly which tables/databases
    /// its replay depends on, so writers validate optimistic commits against
    /// it.
    wal_sets: crate::wal::FrameSets,
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
    /// The optimistic base captured at BEGIN (wave 45o): COMMIT validates
    /// every frame committed after it. No lease is held while the transaction
    /// is open — another process's writes run in parallel and are detected as
    /// conflicts at COMMIT instead of being blocked.
    base: Option<BaseToken>,
}

/// One connection: single-threaded statement stream, own transaction state,
/// and its own session database (`USE` never touches the shared store).
pub struct Connection {
    db: Database,
    txn: Option<Txn>,
    /// This connection's current database (lowercased).
    current_db: String,
    /// The authenticated principal of this session (wave 48), set by
    /// [`Connection::login`]: `Some(lowercased user name)` makes every
    /// statement of this connection run under that user's privileges.
    /// `None` — a plain [`Database::connect`], the FFI, wasm — is the
    /// documented **trusted** embedded session: no privilege check happens.
    principal: Option<String>,
}

impl Connection {
    /// Parse and run every statement, returning one result each.
    /// Multi-statement strings mix freely (each statement locks as needed).
    pub fn execute(&mut self, sql: &str) -> Result<Vec<QueryResult>> {
        read_lock(&self.db.shared).ensure_wal_healthy()?;
        let stmts = crate::sql::parse_statements(sql)?;
        // Per-statement source text: the WAL logs committed statements and a
        // materialized view persists its defining SQL (wave 80), so the split
        // runs for in-memory databases too.
        let texts = crate::sql::statement_texts(sql)?;
        if texts.len() != stmts.len() {
            return Err(PinkDbError::Execution(
                "internal error: statement source split disagrees with the parser".to_string(),
            ));
        }
        let texts = Some(texts);
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

    /// Verify `user`'s password against the store and adopt it as this
    /// session's principal (wave 48): every later statement runs under that
    /// user's privileges, exactly like a network client that logged in.
    ///
    /// Returns the user's display name. An unknown user and a wrong password
    /// produce the same `authentication failed` error, so a client cannot
    /// probe which names exist. Re-login is allowed (the new principal
    /// replaces the old one). A session that never logs in — the embedded
    /// API — stays trusted: the engine performs no privilege check for it.
    pub fn login(&mut self, user: &str, password: &str) -> Result<String> {
        let name = read_lock(&self.db.shared).authenticate(user, password)?;
        self.principal = Some(user.to_ascii_lowercase());
        Ok(name)
    }

    /// The authenticated principal of this session, if any (lowercased).
    pub fn principal(&self) -> Option<&str> {
        self.principal.as_deref()
    }

    /// Forget this session's principal: the connection becomes trusted
    /// again. Network front-ends do not expose this (a client cannot opt out
    /// of authentication mid-connection); it exists for embedders that
    /// logged a connection in for one statement.
    pub fn logout(&mut self) {
        self.principal = None;
    }

    /// Parse `sql` into a reusable prepared statement (exactly one statement,
    /// `?` placeholders allowed). Pure parsing: no catalog access; types are
    /// resolved by [`Connection::bind`] / [`Connection::execute_prepared`].
    pub fn prepare(&self, sql: &str) -> Result<crate::prepared::PreparedStatement> {
        crate::prepared::PreparedStatement::parse(sql)
    }

    /// Bind `params` to `stmt` against the catalog this connection binds
    /// against — the open transaction's snapshot when one is open, else the
    /// shared store. Returns the substituted SQL text; nothing executes.
    pub fn bind(
        &self,
        stmt: &crate::prepared::PreparedStatement,
        params: &[crate::types::Value],
    ) -> Result<crate::prepared::BoundStatement> {
        let default_db = self.current_db.clone();
        self.with_bind_store(|db| stmt.bind(db, &default_db, params))
    }

    /// Execute a prepared statement like any other statement of this
    /// connection: fresh binding/state per execute, inside the open
    /// transaction when one is open, logged to the WAL (as the substituted
    /// SQL) on a durable store.
    pub fn execute_prepared(
        &mut self,
        stmt: &crate::prepared::PreparedStatement,
        params: &[crate::types::Value],
    ) -> Result<QueryResult> {
        let bound = self.bind(stmt, params)?;
        self.execute_bound(&bound)
    }

    /// Execute a statement already bound to values (see [`Connection::bind`]).
    pub fn execute_bound(
        &mut self,
        bound: &crate::prepared::BoundStatement,
    ) -> Result<QueryResult> {
        let mut results = self.execute(bound.sql())?;
        debug_assert_eq!(results.len(), 1, "a bound statement is one statement");
        results.pop().ok_or_else(|| {
            PinkDbError::Execution("internal error: bound statement produced no result".to_string())
        })
    }

    /// Run `f` against the store a statement of this connection binds
    /// against: the open transaction's private snapshot when one is open
    /// (so a prepared statement sees uncommitted tables/rows), else the
    /// shared store under a read lock.
    fn with_bind_store<R>(&self, f: impl FnOnce(&PinkDb) -> R) -> R {
        match &self.txn {
            Some(txn) => f(&txn.work),
            None => self.db.read_store(f),
        }
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
        // Wave 80: automatic materialized-view refresh at the statement
        // boundary — never inside an open transaction (deferred to the first
        // boundary after COMMIT/ROLLBACK). Reads pay one shared-locked scan
        // unless something is due.
        if self.txn.is_none() {
            self.db.boundary_mv_refresh()?;
        }
        // Wave 44: the procedure frame is statement-scoped (see
        // `PinkDb::proc_frame`); the call's own execution (inside
        // `autocommit_write`/the transaction snapshot) reads it back.
        if let Some(txn) = &mut self.txn {
            txn.work.proc_frame_reset();
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
                let plan = crate::plan::with_mv_source(plan, sql_text);
                // Wave 48: an authenticated session is checked in every
                // transaction state too (the snapshot carries the RBAC
                // catalog as of BEGIN).
                enforce(self.principal.as_deref(), &txn.work, &plan, &pre)?;
                // A durable transaction logs its mutating statements: one
                // that reads an attached lake table could never replay (the
                // guard is a no-op for in-memory stores).
                if crate::plan::writes_tables(&plan) {
                    txn.work.refuse_logged_attached_reads(&plan)?;
                }
                record_writes(&plan, &pre, &mut txn.table_writes, &mut txn.view_writes);
                crate::plan::collect_tables_read(&plan, &pre, &mut txn.table_reads);
                // A materialized-view statement reads objects the plan cannot
                // name (its defining query's base tables). MV statements are
                // refused inside transactions today; keep the read set honest
                // if that refusal is ever relaxed.
                for (db, table) in txn.work.mv_reads_of(&plan, &pre) {
                    txn.table_reads.insert((db, table));
                }
                if crate::plan::writes_tables(&plan) {
                    // The frame's set: only statements that get buffered into
                    // the WAL record contribute (a read-only statement's text
                    // never replays).
                    let stmt_sets = txn.work.plan_sets_store(&plan, &pre);
                    txn.wal_sets.extend(&stmt_sets);
                }
                let result = crate::exec::run(&mut txn.work, &plan)?;
                // Wave 44: a `CALL`'s actual touched set joins this
                // transaction's read/write bookkeeping (the pre-execution
                // pass above saw only its argument subqueries).
                if matches!(plan, crate::plan::LogicalPlan::Call { .. }) {
                    txn.wal_sets.extend(&txn.work.proc_frame.touched());
                    for r in txn.work.proc_frame.reads() {
                        txn.table_reads.insert(r);
                    }
                    for w in txn.work.proc_frame.writes() {
                        txn.table_writes.insert(w);
                    }
                }
                let after = session_after(&result, &pre, &txn.work);
                // Buffer the committed statement for the transaction's single
                // WAL record (flushed by txn_commit; never by ROLLBACK).
                if let Some(text) = sql_text
                    && crate::plan::writes_tables(&plan)
                {
                    if txn.wal_db_before.is_none() {
                        txn.wal_db_before = Some(pre.clone());
                    }
                    txn.wal_stmts
                        .push(crate::rbac::wal_statement_text(&plan, text));
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
                self.current_db = after;
                Ok(result)
            }
            None => {
                let pre = self.current_db.clone();
                // Statement-boundary freshness: a stale view refreshes under
                // the write lease before binding (statement-start snapshot;
                // reads never hold a lease while executing).
                self.db.refresh()?;
                // Bind and run under one lock, so the plan always matches the
                // catalog it executes against (a concurrent DROP + CREATE can
                // never leave stale column indices). Reads bind and run under
                // a shared lock; a write takes the write lease, then rebinds
                // under the exclusive lock.
                let shared = read_lock(&self.db.shared);
                let (plan, _) = crate::sql::bind_tracked(&shared, stmt, &pre)?;
                // Wave 48: the privilege check runs on the bound plan under
                // the same lock the bind took, so no statement can slip
                // between the check and its execution.
                enforce(self.principal.as_deref(), &shared, &plan, &pre)?;
                let result = if is_read_only(&plan) {
                    let result = crate::exec::run_read(&shared, &plan);
                    // Release before the `USE` logging below re-locks.
                    drop(shared);
                    result?
                } else {
                    drop(shared);
                    self.autocommit_write(stmt, sql_text, &pre)?
                };
                // `USE` is read-only for locking but still WAL-logged: replay
                // must switch sessions exactly like this connection did.
                // Re-validate the target under the lease, held across the
                // append: the bind-time check released its lock, so another
                // connection could have dropped the database in between — a
                // logged `USE` for a dead database could never replay.
                if is_read_only(&plan)
                    && let QueryResult::Used { .. } = &result
                {
                    let lease = self.db.acquire_lease()?;
                    if let Some(g) = &lease {
                        // Refresh before logging: the `USE` record must
                        // land after every earlier commit.
                        let mut shared = write_lock(&self.db.shared);
                        shared.refresh_under_lease(g)?;
                    }
                    let shared = read_lock(&self.db.shared);
                    let after = session_after(&result, &pre, &shared);
                    if !shared.database_exists(&after) {
                        return Err(PinkDbError::Catalog(format!(
                            "database \"{after}\" not found"
                        )));
                    }
                    if let Some(text) = sql_text {
                        // `USE` touches no table: a known-empty set.
                        let sets = crate::wal::FrameSets::default();
                        shared.wal_append_session(
                            &pre,
                            &after,
                            &[text.to_string()],
                            &[],
                            &[],
                            Some(&sets),
                        )?;
                    }
                    drop(shared);
                    drop(lease);
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

    /// One autocommit write statement outside a transaction (wave 45o).
    ///
    /// Plain table DDL/DML takes the optimistic fast path: bind (and rebind
    /// under the exclusive store lock), capture a base, execute **without**
    /// the lease, then acquire the lease and validate the base from the
    /// frames other writers committed in the meantime. A clean check applies
    /// those frames and logs the statement; a conflict discards the in-memory
    /// effect (a rebuild from the checkpoint + live WAL) and re-executes
    /// under the held lease — the pessimistic fallback, so exactly one
    /// attempt ever reaches the WAL.
    ///
    /// Everything else that mutates (view DDL, `LOAD EXTENSION`,
    /// `CREATE/DROP DATABASE`, `CHECKPOINT`) stays on the serialized path:
    /// its effect cannot be discarded and restored by a rebuild.
    ///
    /// Lock order note (invariant I6): this path holds the store write guard
    /// while acquiring the lease. That is safe — the lease gate is an
    /// in-process refcount, and a lease holder blocked on the store write
    /// guard has not started its validation/append span; the flock itself is
    /// held by the process the whole time or is acquired here.
    fn autocommit_write(
        &self,
        stmt: &crate::sql::ast::Statement,
        sql_text: Option<&str>,
        pre: &str,
    ) -> Result<QueryResult> {
        let mut shared = write_lock(&self.db.shared);
        // Wave 44: the procedure frame is statement-scoped.
        shared.proc_frame_reset();
        // Rebind under the exclusive lock: a concurrent local commit could
        // have changed the catalog between the caller's read-guard bind and
        // this lock.
        let (plan, dynamic_functions_used) = crate::sql::bind_tracked(&shared, stmt, pre)?;
        let plan = crate::plan::with_mv_source(plan, sql_text);
        enforce(self.principal.as_deref(), &shared, &plan, pre)?;
        if crate::plan::writes_tables(&plan) {
            shared.refuse_logged_attached_reads(&plan)?;
        }
        if !self.db.lease.is_some() || !crate::plan::optimistic_eligible(&plan) {
            // Serialized path: lease first, then refresh, run, log.
            drop(shared);
            let lease = self.db.acquire_lease()?;
            let mut shared = write_lock(&self.db.shared);
            if let Some(g) = &lease {
                shared.refresh_under_lease(g)?;
            }
            let (plan, dynamic_functions_used) = crate::sql::bind_tracked(&shared, stmt, pre)?;
            let plan = crate::plan::with_mv_source(plan, sql_text);
            enforce(self.principal.as_deref(), &shared, &plan, pre)?;
            if crate::plan::writes_tables(&plan) {
                shared.refuse_logged_attached_reads(&plan)?;
            }
            let result = crate::exec::run(&mut shared, &plan)?;
            self.log_autocommit(
                &mut shared,
                &plan,
                &dynamic_functions_used,
                sql_text,
                pre,
                &result,
                lease.as_ref(),
                None,
            )?;
            drop(shared);
            drop(lease);
            return Ok(result);
        }
        // Optimistic fast path.
        let mut sets = crate::wal::FrameSets::default();
        crate::plan::plan_sets(&plan, pre, &mut sets);
        sets.canonicalize();
        let base = shared.capture_base()?;
        // Execute with no lease: this is the span other writers overlap.
        let result = crate::exec::run(&mut shared, &plan)?;
        // Past this point a failure must not leave the uncommitted effect
        // observable: without the lease it cannot be undone, so the handle
        // fail-stops (see `poison_uncommitted`).
        let lease = match self.db.acquire_lease() {
            Ok(l) => l,
            Err(e) => return Err(shared.poison_uncommitted(&e)),
        };
        let conflict = match (&base, &lease) {
            (Some(b), Some(g)) => {
                let position = b.position();
                match shared.optimistic_commit_check(g, &position, &sets) {
                    Ok(c) => c,
                    Err(e) => return Err(shared.poison_uncommitted(&e)),
                }
            }
            _ => None,
        };
        if conflict.is_none() {
            shared.note_optimistic_success_for_test();
            self.log_autocommit(
                &mut shared,
                &plan,
                &dynamic_functions_used,
                sql_text,
                pre,
                &result,
                lease.as_ref(),
                Some(&sets),
            )?;
            drop(shared);
            drop(lease);
            return Ok(result);
        }
        // Conflict: undo the never-logged effect, then re-execute under the
        // lease (which is held until this returns, so the retry cannot
        // conflict again).
        if let Some(g) = &lease {
            shared.rebuild_under_lease(g)?;
        }
        shared.note_optimistic_fallback_for_test();
        let (plan, dynamic_functions_used) = crate::sql::bind_tracked(&shared, stmt, pre)?;
        let plan = crate::plan::with_mv_source(plan, sql_text);
        enforce(self.principal.as_deref(), &shared, &plan, pre)?;
        if crate::plan::writes_tables(&plan) {
            shared.refuse_logged_attached_reads(&plan)?;
        }
        let result = crate::exec::run(&mut shared, &plan)?;
        self.log_autocommit(
            &mut shared,
            &plan,
            &dynamic_functions_used,
            sql_text,
            pre,
            &result,
            lease.as_ref(),
            None,
        )?;
        drop(shared);
        drop(lease);
        Ok(result)
    }

    /// Log one committed autocommit write: one WAL record (with a `USE`
    /// prefix and extension prefixes when needed) while the store lock is
    /// held, then the auto-checkpoint hook. `sets` is the statement's
    /// precomputed (canonical) touched set when the caller already has it.
    #[allow(clippy::too_many_arguments)]
    fn log_autocommit(
        &self,
        shared: &mut PinkDb,
        plan: &LogicalPlan,
        dynamic_functions_used: &[String],
        sql_text: Option<&str>,
        pre: &str,
        result: &QueryResult,
        lease: Option<&LeaseGuard>,
        sets: Option<&crate::wal::FrameSets>,
    ) -> Result<()> {
        if crate::plan::writes_tables(plan) {
            // Wave 80: the committed unit's writes feed the ON COMMIT trigger
            // for the next statement boundary (a no-op for MV-free stores).
            let writes = shared.plan_writes_of(plan, pre);
            shared.mark_mv_writes(&writes);
        }
        if let Some(text) = sql_text
            && crate::plan::writes_tables(plan)
        {
            let after = session_after(result, pre, shared);
            let touched = shared.dynamic_function_paths(dynamic_functions_used);
            let loaded = crate::storage::loaded_extension_path(plan);
            // The optimistic path already computed (and canonicalized)
            // the statement's set for validation; reuse it instead of
            // walking the plan again.
            let sets = match sets {
                Some(s) => s.clone(),
                None => shared.plan_sets_store(plan, pre),
            };
            shared.wal_append_session(
                pre,
                &after,
                &[text.to_string()],
                &touched,
                &loaded,
                Some(&sets),
            )?;
            shared.maybe_auto_checkpoint(lease);
        }
        Ok(())
    }

    fn txn_begin(&mut self) -> Result<QueryResult> {
        if self.txn.is_some() {
            return Err(PinkDbError::Execution(
                "already in a transaction".to_string(),
            ));
        }
        // Refresh (a short lease only when something was published), then
        // snapshot and capture the optimistic base. No lease is held while
        // the transaction runs (wave 45o): another process's writes never
        // wait for it; COMMIT validates against the frames they published.
        self.db.refresh()?;
        let shared = read_lock(&self.db.shared);
        // Base first (see `PinkDb::begin`): a frame committed between base and
        // snapshot is past the base and therefore validated.
        let base = shared.capture_base()?;
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
            wal_sets: crate::wal::FrameSets::default(),
            wal_stmts: Vec::new(),
            wal_ext_paths: Vec::new(),
            wal_ext_loaded: Vec::new(),
            wal_db_before: None,
            wal_db_after: self.current_db.clone(),
            wal_dbs: Vec::new(),
            base,
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
        // Take the lease for the commit span, then the store write guard.
        // (Optimistic paths acquire the lease while already holding the write
        // guard; the in-process gate makes that safe — see `Lease`.)
        // A lease failure (busy timeout, replaced lock file) keeps the
        // transaction open: nothing was validated or appended, and a retry or
        // ROLLBACK stays possible.
        let lease = match self.db.acquire_lease() {
            Ok(l) => l,
            Err(e) => {
                self.txn = Some(txn);
                return Err(e);
            }
        };
        let mut shared = write_lock(&self.db.shared);
        // Cross-process validation (wave 45o): conflict iff any frame
        // committed after the base touched a table this transaction read or
        // wrote. Read-only transactions log nothing and never conflict (there
        // is no record whose replay could disagree), so the check only runs
        // for transactions that will produce a WAL record. On a clean result
        // the frames that arrived for *other* tables are applied, so the
        // store is current before the merge below.
        if !txn.wal_stmts.is_empty()
            && let (Some(g), Some(base)) = (lease.as_ref(), txn.base.as_ref())
        {
            let mut ours = crate::wal::FrameSets::default();
            for (db, name) in txn.table_reads.iter().chain(txn.table_writes.iter()) {
                ours.add_table(db, name);
            }
            // Database-level conflicts come only from `CREATE`/`DROP
            // DATABASE` (tracked in `txn.wal_sets`), never from a
            // statement's *session* database: `conflict_with` treats our db
            // set as a wildcard over that database's tables, and the
            // documented contract is table-level granularity. (The session
            // dbs stay in `txn.wal_dbs` for the existence check below.)
            ours.extend_databases(&txn.wal_sets);
            // View DDL is session-local and never logged: no frame can
            // carry it, so `view_writes` contributes nothing here.
            ours.canonicalize();
            let position = base.position();
            if let Some(conflict) = shared.optimistic_commit_check(g, &position, &ours)? {
                return Err(conflict_error(&conflict, "transaction"));
            }
        }
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
            let mut sets = txn.wal_sets.clone();
            sets.canonicalize();
            shared.wal_append_session(
                &before,
                &txn.wal_db_after,
                &txn.wal_stmts,
                &txn.wal_ext_paths,
                &txn.wal_ext_loaded,
                Some(&sets),
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
            // Wave 48: grants follow their table through the same merge — a
            // transaction's `DROP TABLE`/`RENAME` also decided the table's
            // grant entry, which lives outside the table itself.
            let snapshot = txn.work.rbac();
            shared.rbac_mut().sync_table_grants(snapshot, db, name);
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
            shared.maybe_auto_checkpoint(lease.as_ref());
        }
        // Wave 80: the committed transaction's writes feed the ON COMMIT
        // trigger for the next statement boundary.
        shared.mark_mv_writes(&txn.table_writes);
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

/// Enforce one session principal's privileges on one bound plan (wave 48),
/// called at every point a statement is about to execute. A no-op for a
/// trusted session (`principal == None` — the embedded API) and for a store
/// with no RBAC data at all.
fn enforce(
    principal: Option<&str>,
    store: &PinkDb,
    plan: &LogicalPlan,
    default_db: &str,
) -> Result<()> {
    match principal {
        None => Ok(()),
        Some(principal) => crate::rbac::check_statement(store, principal, plan, default_db),
    }
}

fn conflict(what: &str) -> PinkDbError {
    PinkDbError::Execution(format!(
        "write conflict on {what}: changed concurrently, retry the transaction"
    ))
}

/// The cross-process conflict error (wave 45o): names the object and the
/// frame LSN the *other* writer committed (the WAL position is the
/// cross-process version: `DataTable::version` is process-local and never
/// persisted). The stable `write conflict on` prefix is what hosts match to
/// retry.
fn conflict_error(c: &crate::wal::CommitConflict, unit: &str) -> PinkDbError {
    PinkDbError::Execution(c.message(unit))
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
        // Wave 80: materialized-view definitions, schedules and counters are
        // store metadata the per-table transaction merge does not cover, and
        // auto-refresh defers to the first boundary after COMMIT anyway.
        Statement::CreateMaterializedView { .. } => "CREATE MATERIALIZED VIEW",
        Statement::RefreshMaterializedView { .. } => "REFRESH MATERIALIZED VIEW",
        Statement::RefreshMaterializedViews => "REFRESH MATERIALIZED VIEWS",
        Statement::DropMaterializedView { .. } => "DROP MATERIALIZED VIEW",
        // Wave 44: procedure definitions are catalog metadata the per-table
        // transaction merge does not carry. `CALL` itself is allowed inside a
        // transaction: its statements run there like hand-written ones.
        Statement::CreateProcedure { .. } => "CREATE PROCEDURE",
        Statement::DropProcedure { .. } => "DROP PROCEDURE",
        // Wave 77: macro definitions are catalog metadata too.
        Statement::CreateMacro { .. } => "CREATE MACRO",
        Statement::DropMacro { .. } => "DROP MACRO",
        // Wave 48: the RBAC catalog is store metadata the per-table
        // transaction merge does not carry (exactly like a procedure
        // definition), so these are refused inside a transaction.
        Statement::CreateUser { .. } => "CREATE USER",
        Statement::AlterUser { .. } => "ALTER USER",
        Statement::DropUser { .. } => "DROP USER",
        Statement::CreateRole { .. } => "CREATE ROLE",
        Statement::DropRole { .. } => "DROP ROLE",
        Statement::Grant { .. } => "GRANT",
        Statement::Revoke { .. } => "REVOKE",
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
pub(crate) fn record_writes(
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
        | LogicalPlan::InsertSelect { db, table, .. }
        | LogicalPlan::Update { db, table, .. }
        | LogicalPlan::Delete { db, table, .. } => {
            tables.insert(key(db, table));
        }
        LogicalPlan::CreateTable { db, name, .. }
        | LogicalPlan::CreateTableAs { db, name, .. }
        | LogicalPlan::DropTable { db, name, .. } => {
            tables.insert(key(db, name));
        }
        // A materialized-view statement writes its backing table; the
        // definition itself rides along in the same WAL record (wave 80).
        // `REFRESH MATERIALIZED VIEWS` names the whole database instead — the
        // store-side helper `PinkDb::plan_writes_of` resolves its views.
        LogicalPlan::CreateMaterializedView { db, name, .. }
        | LogicalPlan::RefreshMaterializedView { db, name }
        | LogicalPlan::DropMaterializedView { db, name, .. } => {
            tables.insert(key(db, name));
        }
        LogicalPlan::AlterTable { db, name, op } => {
            // A rename touches the old and the new key: the WAL frame's set
            // must cover both, so a concurrent writer of either name
            // conflicts instead of replaying onto the wrong table.
            tables.insert(key(db, name));
            if let crate::plan::AlterTablePlan::RenameTable { to } = op {
                tables.insert(key(db, to));
            }
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
    use crate::types::{DataType, Value};

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

    /// A constant predicate keeps its type through constant folding: a
    /// folded NULL is INTEGER-typed as a bare literal, which the executor's
    /// `WHERE` check used to reject (`SELECT * FROM t WHERE NULL` and
    /// `WHERE 1 = NULL` failed with "WHERE: expected BOOLEAN, got INTEGER" —
    /// and so did a procedure comparison of two NULL variables).
    #[test]
    fn folded_null_predicates_keep_their_boolean_type() {
        let db = Database::new();
        let mut c = db.connect();
        c.execute("CREATE TABLE t (a INTEGER);").unwrap();
        c.execute("INSERT INTO t VALUES (1);").unwrap();
        for (sql, expected) in [
            ("SELECT a FROM t WHERE 1 = NULL;", 0),
            ("SELECT a FROM t WHERE NULL IS NULL;", 1),
            // A comparison whose both sides fold to NULL is the shape a
            // procedure hits with two NULL variables.
            ("SELECT a FROM t WHERE 1 + 1 = NULL;", 0),
        ] {
            let rows = rows_of(c.execute(sql).unwrap());
            assert_eq!(rows.len(), expected, "{sql}");
        }
        // A bare `WHERE NULL` is (and stays) a bind-time type error: the
        // binder never types a NULL literal BOOLEAN.
        let err = c
            .execute("SELECT a FROM t WHERE NULL;")
            .unwrap_err()
            .to_string();
        assert!(err.contains("BOOLEAN"), "{err}");
        // A folded constant projection reports its own type, not INTEGER.
        match c.execute("SELECT 1 = NULL AS x;").unwrap().pop() {
            Some(QueryResult::Selected { types, rows, .. }) => {
                assert_eq!(types, vec![DataType::Boolean]);
                assert_eq!(rows, vec![vec![Value::Null]]);
            }
            other => panic!("{other:?}"),
        }
        match c.execute("SELECT NULL AS x;").unwrap().pop() {
            Some(QueryResult::Selected { types, .. }) => {
                assert_eq!(types, vec![DataType::Integer], "a bare NULL stays INTEGER");
            }
            other => panic!("{other:?}"),
        }
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
    fn a_session_database_is_not_a_database_level_conflict() {
        let db = seeded();
        // A transaction writes t.
        let mut a = db.connect();
        a.execute("BEGIN;").unwrap();
        a.execute("INSERT INTO t VALUES (3, 'z');").unwrap();
        // B autocommits a write to the *other* table in the same database.
        let mut b = db.connect();
        b.execute("INSERT INTO u VALUES (20);").unwrap();
        // A must still commit: its session database is not a database-level
        // (CREATE/DROP DATABASE) conflict, and t/u are disjoint.
        a.execute("COMMIT;").unwrap();
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
    fn show_tables_and_databases_follow_the_connection_session() {
        let db = multi_db();
        let mut c = db.connect();
        // SHOW TABLES names the session database's tables, sorted.
        assert_eq!(
            rows_of(c.execute("SHOW TABLES;").unwrap()),
            vec![vec![Value::Varchar("t".to_string())]]
        );
        c.execute("USE db2;").unwrap();
        assert_eq!(
            rows_of(c.execute("SHOW TABLES;").unwrap()),
            vec![vec![Value::Varchar("u".to_string())]]
        );
        // SHOW DATABASES lists the whole store, sorted by name.
        assert_eq!(
            rows_of(c.execute("SHOW DATABASES;").unwrap()),
            vec![
                vec![Value::Varchar("db1".to_string())],
                vec![Value::Varchar("db2".to_string())],
                vec![Value::Varchar("main".to_string())],
            ]
        );
        // EXPLAIN is read-only: it prints a plan and never runs the query.
        let rows = rows_of(c.execute("EXPLAIN SELECT * FROM u;").unwrap());
        assert!(!rows.is_empty());
        assert!(
            matches!(rows.last().unwrap().last().unwrap(), Value::Varchar(s) if s.contains("Scan"))
        );
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
