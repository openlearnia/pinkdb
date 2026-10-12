//! pinkdb — mini DuckDB: embedded analytical SQL in Rust.
//!
//! v1: in-memory columnar storage, vectorized execution (CHUNK_SIZE = 2048),
//! hand-written SQL frontend, CSV import. Single connection, single thread,
//! no unsafe, no async.

pub mod csv;
pub mod crypto;
pub mod db;
pub mod exec;
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
pub use wal::{RecoveryInfo, SyncPolicy};

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
            let plan = optimizer::optimize(self, sql::bind(self, stmt, &session_db)?);
            // Database create/drop are store-level and cannot be undone by a
            // snapshot commit of the multi-writer path: refuse inside a txn.
            if self.in_transaction()
                && matches!(
                    plan,
                    plan::LogicalPlan::CreateDatabase { .. }
                        | plan::LogicalPlan::DropDatabase { .. }
                )
            {
                return Err(types::PinkDbError::Execution(format!(
                    "{} is not allowed inside a transaction",
                    if matches!(plan, plan::LogicalPlan::CreateDatabase { .. }) {
                        "CREATE DATABASE"
                    } else {
                        "DROP DATABASE"
                    }
                )));
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
                self.wal_log_statement(&plan, &texts[i])?;
            }
            out.push(result);
        }
        Ok(out)
    }
}
