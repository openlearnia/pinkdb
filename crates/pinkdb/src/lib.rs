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

pub mod csv;
pub mod crypto;
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

pub use db::{Connection, Database};
pub use crypto::EncryptionKey;
pub use exec::QueryResult;
pub use storage::{DataTable, PinkDb, ViewDef};
pub use types::{DataType, PinkDbError, Value};
pub use vector::{DataChunk, Vector, VectorData, CHUNK_SIZE};
pub use wal::{RecoveryInfo, SyncPolicy, DEFAULT_AUTO_CHECKPOINT_BYTES};

impl PinkDb {
    /// Parse and run every statement in `sql`, returning one result each.
    ///
    /// Durable databases (see [`PinkDb::open_durable`]) log each committed
    /// statement to the WAL: autocommit statements individually, transaction
    /// statements as one record at `COMMIT`. In-memory databases skip that
    /// entirely — behavior is unchanged.
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
            let session_db = self.current_db().to_string();
            let (plan, dynamic_functions_used) =
                sql::bind_tracked(self, stmt, &session_db)?;
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
                self.maybe_auto_checkpoint();
            }
            out.push(result);
        }
        Ok(out)
    }
}
