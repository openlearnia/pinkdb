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
pub mod sql;
pub mod storage;
pub mod types;
pub mod vector;
pub mod wal;

pub use attached::{AttachedLake, AttachedScan, AttachedTable, ColumnStats};
pub use crypto::EncryptionKey;
pub use db::{Connection, Database};
pub use exec::QueryResult;
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
    /// Multi-process (wave 45m): at every statement boundary outside a
    /// transaction, a durable store refreshes its view from the WAL when the
    /// publication epoch says another process advanced it; mutating
    /// statements take the write lease for `[refresh → execute → append →
    /// publish → auto-checkpoint]`. Reads are served from memory and pay one
    /// `pread` when nothing changed.
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
                self.wal_log_statement(&plan, &texts[i], &dynamic_functions_used)?;
                self.maybe_auto_checkpoint(lease.as_ref());
            }
            drop(lease);
            out.push(result);
        }
        Ok(out)
    }
}
