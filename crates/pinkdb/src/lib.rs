//! pinkdb — mini DuckDB: embedded analytical SQL in Rust.
//!
//! v1: in-memory columnar storage, vectorized execution (CHUNK_SIZE = 2048),
//! PEG SQL frontend, CSV import. Single connection, single thread, no unsafe,
//! no async.
//!
//! Loadable extensions (ABI v1, `pinkdb-ext-loader`) add scalar functions at
//! runtime through [`PinkDb::load_extension`]; the unsafe boundary (dlopen and
//! callback invocation) lives entirely in that crate, the engine stays
//! zero-unsafe, and the built-in binder/executor path is unchanged when no
//! extension is loaded.

pub mod attached;
pub mod crypto;
pub mod csv;
pub mod db;
pub mod exec;
mod ext;
pub mod optimizer;
pub mod persist;
pub mod plan;
pub mod prepared;
pub mod sql;
pub mod storage;
pub mod types;
pub mod vector;
pub mod wal;

pub use attached::{AtSpec, AttachedLake, AttachedScan, AttachedTable, ColumnStats};
pub use crypto::EncryptionKey;
pub use db::{Connection, Database};
pub use exec::QueryResult;
pub use prepared::{BoundStatement, PreparedStatement};
pub use storage::{DataTable, PinkDb, ViewDef};
pub use types::{DataType, PinkDbError, Value};
pub use vector::{DataChunk, Vector, VectorData, CHUNK_SIZE};
pub use wal::{
    DurableOpenOptions, LockMode, RecoveryInfo, SyncPolicy, DEFAULT_AUTO_CHECKPOINT_BYTES,
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
        // Only durable databases need per-statement source text for the WAL.
        let texts = if self.is_durable() {
            let texts = sql::statement_texts(sql)?;
            if texts.len() != stmts.len() {
                return Err(types::PinkDbError::Execution(
                    "internal error: statement source split disagrees with the parser".to_string(),
                ));
            }
            Some(texts)
        } else {
            None
        };
        let mut out = Vec::with_capacity(stmts.len());
        for (i, stmt) in stmts.iter().enumerate() {
            // Refresh at the statement boundary (never mid-statement, never
            // inside a transaction): reads served from memory only skip the
            // protocol when the epoch says the on-disk state did not change.
            if self.is_durable() && !self.in_transaction() {
                self.refresh_if_stale()?;
            }
            let session_db = self.current_db().to_string();
            let (plan, dynamic_functions_used) = sql::bind_tracked(self, stmt, &session_db)?;
            let plan = optimizer::optimize(self, plan);
            // Store/process-level statements cannot be undone by a snapshot
            // commit of the multi-writer path: refuse inside a txn.
            // (LOAD EXTENSION is process-level too: the private snapshot's
            // registry would be discarded at COMMIT/ROLLBACK.)
            if self.in_transaction() {
                if let Some(stmt) = plan::refused_in_transaction(&plan) {
                    return Err(types::PinkDbError::Execution(format!(
                        "{stmt} is not allowed inside a transaction"
                    )));
                }
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
            if self.is_durable() && self.in_transaction() {
                plan::plan_sets(&plan, &session_db, &mut self.txn_touched);
                if let plan::LogicalPlan::Use { name } = &plan {
                    self.txn_touched.add_database(name);
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
                        (optimizer::optimize(self, p), d)
                    } else {
                        (plan, dynamic_functions_used)
                    }
                }
                None => (plan, dynamic_functions_used),
            };
            let result = exec::run(self, &plan)?;
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
