//! pinkdb — mini DuckDB: embedded analytical SQL in Rust.
//!
//! v1: in-memory columnar storage, vectorized execution (CHUNK_SIZE = 2048),
//! hand-written SQL frontend, CSV import. Single connection, single thread,
//! no unsafe, no async.

pub mod csv;
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
            let plan = optimizer::optimize(self, sql::bind(self, stmt)?);
            let result = exec::run(self, &plan)?;
            if let Some(texts) = &texts {
                self.wal_log_statement(&plan, &texts[i])?;
            }
            out.push(result);
        }
        Ok(out)
    }
}
