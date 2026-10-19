//! pinkdb — mini DuckDB: embedded analytical SQL in Rust.
//!
//! v1: in-memory columnar storage, vectorized execution (CHUNK_SIZE = 2048),
//! PEG SQL frontend, CSV import. Single connection, single thread, no unsafe,
//! no async.
//!
//! Loadable extensions add scalar functions at runtime through
//! [`PinkDb::load_extension`]: native targets load ABI v1 shared libraries
//! (`dlopen`), wasm targets resolve names against the linked registry
//! (`pinkdb_ext_loader::linked`) — the same engine registry either way. The
//! unsafe boundary (dlopen and callback invocation) lives entirely in the
//! `pinkdb-ext-loader` crate and is native-only, the engine stays
//! zero-unsafe, and the built-in binder/executor path is unchanged when no
//! extension is loaded. See `docs/wasm-extensions.md` for the wasm model.

pub mod attached;
pub mod crypto;
pub mod csv;
pub mod db;
pub mod exec;
mod ext;
/// Registered file formats (wave 71): the engine half of the first-party
/// `arrow` extension — see [`filefmt::FileFormat`].
pub mod filefmt;
/// The optional JavaScript procedure sandbox (cargo feature `js`): the bridge
/// to `pinkdb-js`, which refuses cleanly when no engine is bound.
#[cfg(feature = "js")]
pub(crate) mod js;
pub(crate) mod mv;
pub mod optimizer;
pub mod persist;
pub mod plan;
pub mod prepared;
pub mod procedure;
/// Role-based access control (wave 48): users, roles, grants, password
/// hashing, and the privilege check authenticated sessions run under.
pub mod rbac;
pub mod sql;
pub mod storage;
/// Host-registered table sources (wave 70): the engine-adjacent surface the
/// `pinkdb-parquet` crate (`read_parquet`, `FROM 'file.parquet'`) plugs into.
pub mod table_source;
pub mod types;
pub mod vector;
pub mod wal;

pub use attached::{
    AtSpec, AttachedLake, AttachedScan, AttachedTable, ColumnStats, PruneOp, PrunePredicate,
};
pub use crypto::EncryptionKey;
pub use db::{Connection, Database};
pub use exec::QueryResult;
pub use filefmt::FileFormat;
pub use prepared::{BoundStatement, PreparedStatement};
pub use procedure::{ProcLanguage, ProcedureDef};
pub use rbac::{
    GrantObject, GrantPrivs, GrantSpec, GrantTarget, PasswordSource, Privilege, UserDef,
    hash_password, validate_password_hash, verify_password,
};
pub use storage::{DataTable, PinkDb, ViewDef};
pub use table_source::{
    TableFunctionArg, TableFunctionCall, TableScanOutput, TableSource, TableSourceStats,
};
pub use types::{DataType, PinkDbError, Result, Value};
pub use vector::{CHUNK_SIZE, DataChunk, Vector, VectorData};
pub use wal::{
    DEFAULT_AUTO_CHECKPOINT_BYTES, DurableOpenOptions, LockMode, RecoveryInfo, SyncPolicy,
};

impl PinkDb {
    /// Parse and run every statement in `sql`, returning one result each.
    ///
    /// Durable databases (see [`PinkDb::open_durable`]) log each committed
    /// statement to the WAL: autocommit statements individually, transaction
    /// statements as one record at `COMMIT`. In-memory databases skip that
    /// entirely — behavior is unchanged.
    ///
    /// Multi-process (waves 45m/45o): at every statement boundary outside a
    /// transaction, a durable store refreshes its view from the WAL when the
    /// publication epoch says another process advanced it; reads are served
    /// from memory and pay one `pread` when nothing changed. Autocommit
    /// writes to plain tables run **optimistically**: the statement executes
    /// with no lease held, then the commit takes the lease, validates the
    /// statement's read/write set against the frames other writers committed
    /// since (payload v4 carries each frame's touched set), appends, and
    /// publishes. A conflict discards the never-logged effect (the store is
    /// rebuilt from the committed state) and the statement is re-executed
    /// under the held lease, so exactly one effect lands. Statements whose
    /// effect cannot be discarded (`LOAD EXTENSION`, view DDL, database DDL,
    /// `CHECKPOINT`) keep the serialized lease path.
    ///
    /// An explicit `BEGIN` takes no lease either: it refreshes (a short lease
    /// only when stale), snapshots, and captures a base position. The
    /// transaction's statements run on the private snapshot; `COMMIT` takes
    /// the lease and validates the transaction's read/write set against the
    /// frames committed after the base (table granularity, read sets
    /// included, so write skew is prevented), applying the frames that
    /// arrived for other tables. A conflict aborts the transaction with a
    /// retryable `write conflict` error; re-running it on the refreshed state
    /// is the intended retry (the CLI retries a buffered transaction up to
    /// `PINKDB_TXN_RETRIES` times, default 3).
    ///
    /// The embedded session's current database (`USE`) applies to every later
    /// statement in the same string; unqualified names resolve against it.
    pub fn execute(&mut self, sql: &str) -> types::Result<Vec<QueryResult>> {
        self.ensure_wal_healthy()?;
        let stmts = sql::parse_statements(sql)?;
        // Per-statement source text: the WAL logs committed statements, and a
        // materialized view persists its defining SQL (wave 80) — the split
        // runs for in-memory stores too.
        let texts = sql::statement_texts(sql)?;
        if texts.len() != stmts.len() {
            return Err(types::PinkDbError::Execution(
                "internal error: statement source split disagrees with the parser".to_string(),
            ));
        }
        let texts = Some(texts);
        let mut out = Vec::with_capacity(stmts.len());
        for (i, stmt) in stmts.iter().enumerate() {
            // Wave 44: the procedure frame is statement-scoped — whatever a
            // `CALL` recorded belongs to that statement and is read by the
            // logging/conflict paths below.
            self.proc_frame_reset();
            // Refresh at the statement boundary (never mid-statement, never
            // inside a transaction): reads served from memory only skip the
            // protocol when the epoch says the on-disk state did not change.
            if self.is_durable() && !self.in_transaction() {
                self.refresh_if_stale()?;
            }
            // Wave 80: automatic materialized-view refresh at the same
            // boundary — deferred inside an open transaction, and skipped
            // during WAL replay (the logged `REFRESH` statements reproduce
            // it exactly; see `crate::mv`).
            if !self.in_transaction() && !self.replaying {
                self.boundary_auto_refresh()?;
            }
            let session_db = self.current_db().to_string();
            let (plan, dynamic_functions_used) = sql::bind_tracked(self, stmt, &session_db)?;
            // The runner (not the binder) holds the per-statement source text;
            // a `CREATE MATERIALIZED VIEW` plan persists it (wave 80).
            let plan = plan::with_mv_source(plan, texts.as_ref().map(|t| t[i].as_str()));
            let plan = optimizer::optimize(self, plan);
            // Store/process-level statements cannot be undone by a snapshot
            // commit of the multi-writer path: refuse inside a txn.
            // (LOAD EXTENSION is process-level too: the private snapshot's
            // registry would be discarded at COMMIT/ROLLBACK.)
            if self.in_transaction()
                && let Some(stmt) = plan::refused_in_transaction(&plan)
            {
                return Err(types::PinkDbError::Execution(format!(
                    "{stmt} is not allowed inside a transaction"
                )));
            }
            // A durable store logs mutating statements: one that reads an
            // attached lake table could never replay (the table is
            // session-local), so refuse it before anything runs.
            if self.is_durable() && plan::writes_tables(&plan) {
                self.refuse_logged_attached_reads(&plan)?;
            }
            // Inside an open transaction, remember every object this
            // statement read or wrote: COMMIT validates the set against the
            // frames other writers committed (wave 45o). `USE` targets count
            // too: the transaction's record replays them.
            if self.in_transaction() {
                if self.is_durable() {
                    // Store-aware: a materialized-view statement depends on
                    // objects the plan alone cannot name (wave 80).
                    let sets = self.plan_sets_store(&plan, &session_db);
                    self.txn_touched.extend(&sets);
                    if let plan::LogicalPlan::Use { name } = &plan {
                        self.txn_touched.add_database(name);
                    }
                }
                if plan::writes_tables(&plan) && self.has_materialized_views() {
                    // The transaction's writes become the ON COMMIT trigger
                    // when it finally commits (never on ROLLBACK) — durable
                    // and in-memory stores alike.
                    let writes = self.plan_writes_of(&plan, &session_db);
                    self.txn_writes.extend(writes);
                }
            }
            // Autocommit writes to plain tables take the optimistic fast
            // path: execute with no lease, then validate + append under it
            // (with a pessimistic fallback when the commit conflicts). The
            // serialized path below covers everything else.
            let optimistic = self.is_durable()
                && !self.in_transaction()
                && !self.replaying
                && plan::optimistic_eligible(&plan);
            if optimistic {
                let mut sets = crate::wal::FrameSets::default();
                plan::plan_sets(&plan, &session_db, &mut sets);
                sets.canonicalize();
                let base = self.capture_base()?;
                // The execution span no other writer waits for.
                let first = exec::run(self, &plan)?;
                // Past this point a failure must not leave the uncommitted
                // effect observable: without the lease it cannot be undone,
                // so the handle fail-stops (see `poison_uncommitted`).
                let lease = match self.acquire_lease() {
                    Ok(l) => l,
                    Err(e) => return Err(self.poison_uncommitted(&e)),
                };
                let conflict = match (&base, &lease) {
                    (Some(b), Some(g)) => {
                        let position = b.position();
                        match self.optimistic_commit_check(g, &position, &sets) {
                            Ok(c) => c,
                            Err(e) => return Err(self.poison_uncommitted(&e)),
                        }
                    }
                    _ => None,
                };
                let (result, plan, dynamic_functions_used) = if conflict.is_none() {
                    self.note_optimistic_success_for_test();
                    (first, plan, dynamic_functions_used)
                } else {
                    // Conflict: discard the never-logged effect (reload the
                    // committed state) and re-execute under the held lease —
                    // exactly one attempt reaches the WAL.
                    if let Some(g) = &lease {
                        self.rebuild_under_lease(g)?;
                    }
                    self.note_optimistic_fallback_for_test();
                    let (p, d) = sql::bind_tracked(self, stmt, &session_db)?;
                    let p = plan::with_mv_source(p, texts.as_ref().map(|t| t[i].as_str()));
                    let p = optimizer::optimize(self, p);
                    if plan::writes_tables(&p) {
                        self.refuse_logged_attached_reads(&p)?;
                    }
                    (exec::run(self, &p)?, p, d)
                };
                // Session transitions are applied before WAL logging so the
                // log record carries the post-statement session database.
                match &result {
                    QueryResult::Used { db } => self.set_current_db(db),
                    QueryResult::DroppedDatabase { name }
                        if name.eq_ignore_ascii_case(self.current_db()) =>
                    {
                        let fallback = self.fallback_db();
                        self.set_current_db(&fallback);
                    }
                    _ => {}
                }
                self.mark_committed_mv_writes(&plan, &session_db);
                if let Some(texts) = &texts {
                    self.wal_log_statement(&plan, &texts[i], &dynamic_functions_used, Some(&sets))?;
                    self.maybe_auto_checkpoint(lease.as_ref());
                }
                drop(lease);
                out.push(result);
                continue;
            }
            // Transaction control manages its own lease (`begin` holds it
            // until `commit`/`rollback`). Other mutating statements (and
            // CHECKPOINT under these rules) take the lease around
            // `[refresh → execute → append → publish → auto-checkpoint]`;
            // reads never take it.
            let needs_lease = self.is_durable()
                && !self.in_transaction()
                && (plan::writes_tables(&plan) || matches!(plan, plan::LogicalPlan::Checkpoint));
            let lease = if needs_lease {
                self.acquire_lease()?
            } else {
                None
            };
            let (plan, dynamic_functions_used) = match &lease {
                Some(g) => {
                    if self.refresh_under_lease(g)? {
                        // The refresh may have changed the catalog (or the
                        // replayed frames may name objects bound above):
                        // rebind against the refreshed state.
                        let (p, d) = sql::bind_tracked(self, stmt, &session_db)?;
                        let p = plan::with_mv_source(p, texts.as_ref().map(|t| t[i].as_str()));
                        (optimizer::optimize(self, p), d)
                    } else {
                        (plan, dynamic_functions_used)
                    }
                }
                None => (plan, dynamic_functions_used),
            };
            let result = exec::run(self, &plan)?;
            // Wave 44: a `CALL`'s actual touched set joins the open
            // transaction's conflict bookkeeping (its pre-execution pass saw
            // an empty set).
            self.note_proc_frame_in_txn(&plan);
            // Session transitions are applied before WAL logging so the log
            // record carries the post-statement session database.
            match &result {
                QueryResult::Used { db } => self.set_current_db(db),
                QueryResult::DroppedDatabase { name }
                    if name.eq_ignore_ascii_case(self.current_db()) =>
                {
                    let fallback = self.fallback_db();
                    self.set_current_db(&fallback);
                }
                _ => {}
            }
            self.mark_committed_mv_writes(&plan, &session_db);
            if let Some(texts) = &texts {
                self.wal_log_statement(&plan, &texts[i], &dynamic_functions_used, None)?;
                self.maybe_auto_checkpoint(lease.as_ref());
            }
            drop(lease);
            out.push(result);
        }
        Ok(out)
    }
}
