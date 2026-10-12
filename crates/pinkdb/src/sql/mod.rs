//! SQL frontend: lexer → parser → binder.
//!
//! Two parsers coexist: the hand-written lexer + recursive-descent parser
//! ([`parser`]) and the PEG grammar ([`peg_parser`]). `PINKDB_PARSER` selects
//! one per call: `legacy` → hand-written parser, anything else (default) → PEG.

pub mod ast;
pub mod binder;
pub mod lexer;
pub mod parser;
pub mod peg_parser;

pub use binder::{AggFunc, BoundAgg, BoundExpr, ScalarFunc, bind};
use crate::types::Result;

/// Parse all statements in `sql` with the parser selected by the
/// `PINKDB_PARSER` environment variable (read per call): `legacy` selects the
/// hand-written lexer + recursive-descent parser; the default is the PEG
/// grammar. Both produce the same AST for the same input (differential-tested
/// in [`peg_parser`]); PEG error messages are positional and worded
/// differently.
pub fn parse_statements(sql: &str) -> Result<Vec<ast::Statement>> {
    match std::env::var("PINKDB_PARSER").ok().as_deref() {
        Some("legacy") => parser::parse_statements(sql),
        _ => peg_parser::parse_statements(sql),
    }
}
