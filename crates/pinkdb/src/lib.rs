//! pinkdb — mini DuckDB: embedded analytical SQL in Rust.
//!
//! v1: in-memory columnar storage, vectorized execution (CHUNK_SIZE = 2048),
//! hand-written SQL frontend, CSV import. Single connection, single thread,
//! no unsafe, no async.

pub mod csv;
pub mod exec;
pub mod optimizer;
pub mod persist;
pub mod plan;
pub mod sql;
pub mod storage;
pub mod types;
pub mod vector;

pub use exec::QueryResult;
pub use storage::{DataTable, PinkDb};
pub use types::{DataType, PinkDbError, Value};
pub use vector::{DataChunk, Vector, VectorData, CHUNK_SIZE};

impl PinkDb {
    /// Parse and run every statement in `sql`, returning one result each.
    pub fn execute(&mut self, sql: &str) -> types::Result<Vec<QueryResult>> {
        let stmts = sql::parse_statements(sql)?;
        let mut out = Vec::with_capacity(stmts.len());
        for stmt in &stmts {
            let plan = optimizer::optimize(self, sql::bind(self, stmt)?);
            out.push(exec::run(self, &plan)?);
        }
        Ok(out)
    }
}
