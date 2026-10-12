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

/// Split `sql` into one source text per statement, mirroring the parsers'
/// top-level `;` splitting (strings, quoted identifiers and comments are
/// lexed, so a `;` inside them never splits). Returns the raw source of every
/// non-empty statement, in order — used to write exact statement text to the
/// WAL. Lex errors surface exactly as [`parse_statements`] would report them.
/// Call only after [`parse_statements`] succeeded; the two lists are then
/// guaranteed to have the same length.
pub fn statement_texts(sql: &str) -> Result<Vec<String>> {
    let tokens = lexer::lex(sql)?;
    let mut out = Vec::new();
    let mut start: Option<usize> = None;
    for (tok, off) in &tokens {
        match tok {
            lexer::Token::Eof | lexer::Token::Semicolon => {
                if let Some(s) = start.take() {
                    out.push(sql[s..*off].trim_end().to_string());
                }
            }
            _ => {
                if start.is_none() {
                    start = Some(*off);
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_texts_split_on_top_level_semicolons() {
        assert_eq!(
            statement_texts("SELECT 1; INSERT INTO t VALUES (2);").unwrap(),
            vec!["SELECT 1", "INSERT INTO t VALUES (2)"]
        );
        // The final `;` is optional; empty statements are skipped.
        assert_eq!(statement_texts(";;SELECT 1").unwrap(), vec!["SELECT 1"]);
        assert_eq!(statement_texts("   ").unwrap(), Vec::<String>::new());
        // `;` inside strings, quoted identifiers and comments does not split.
        assert_eq!(
            statement_texts("SELECT ';', \"a;b\"; -- c;\n/* d; */ SELECT 2").unwrap(),
            vec!["SELECT ';', \"a;b\"", "SELECT 2"]
        );
        // Multi-line statement text is preserved; the terminating `;` is not.
        assert_eq!(
            statement_texts("INSERT INTO t\n  VALUES (1);\n").unwrap(),
            vec!["INSERT INTO t\n  VALUES (1)"]
        );
    }

    #[test]
    fn statement_texts_agree_with_parse_for_both_parsers() {
        let corpus = [
            "SELECT 1;",
            "CREATE TABLE t (a INTEGER, b VARCHAR); INSERT INTO t VALUES (1, 'x');",
            "BEGIN; UPDATE t SET a = a + 1 WHERE a > 0; COMMIT;",
            "DROP TABLE t;",
            "SELECT ';' FROM t WHERE b LIKE '%;%';",
        ];
        for sql in corpus {
            let stmts = parse_statements(sql).unwrap();
            let texts = statement_texts(sql).unwrap();
            assert_eq!(stmts.len(), texts.len(), "count mismatch for {sql:?}");
            // Every text re-parses to exactly one statement.
            for text in &texts {
                assert_eq!(parse_statements(text).unwrap().len(), 1, "text {text:?}");
            }
        }
    }
}
