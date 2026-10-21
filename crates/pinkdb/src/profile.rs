//! Query execution profiles (wave 84).
//!
//! [`QueryProfile`] carries the timings and facts of one statement next to its
//! [`crate::QueryResult`]. It is produced by the opt-in
//! [`crate::PinkDb::execute_profiled`] and
//! [`crate::Connection::execute_profiled`] runners; plain `execute` stays
//! zero-cost and profiling never changes results or WAL bytes.

/// Timings and facts of one executed statement (wave 84).
#[derive(Debug, Clone, PartialEq)]
pub struct QueryProfile {
    /// Parsing the whole SQL text the statement came from (shared by every
    /// statement of one `execute_profiled` call — the text is parsed once).
    pub parse: std::time::Duration,
    /// Binding, which in pinkdb also builds the logical plan.
    pub bind: std::time::Duration,
    /// The optimizer.
    pub optimize: std::time::Duration,
    /// Execution, including any lease/refresh wait and conflict retry.
    pub execute: std::time::Duration,
    /// WAL logging (append + sync per policy) and auto-checkpoint; zero for
    /// reads and in-memory stores.
    pub log: std::time::Duration,
    /// Wall time of this statement from bind start to the end of logging,
    /// plus its share of `parse` (the first statement carries it; later
    /// statements of the same text report `parse` but do not add it again).
    pub total: std::time::Duration,
    /// Result rows (`Selected`), or the affected count (INSERT/UPDATE/DELETE),
    /// else 0.
    pub rows: u64,
    /// Result columns (`Selected`), else 0.
    pub columns: u32,
    /// The statement kind: the `QueryResult` variant name ("Selected",
    /// "Inserted", "Called", …).
    pub kind: String,
    /// True when the statement ran inside an open transaction.
    pub in_transaction: bool,
    /// `env!("CARGO_PKG_VERSION")` of the pinkdb crate.
    pub engine_version: &'static str,
}
