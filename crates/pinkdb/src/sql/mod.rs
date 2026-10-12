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
use crate::types::{PinkDbError, Result, Value};

/// Maximum AST depth both parsers accept (see [`check_ast_depth`]).
///
/// Measured in nodes along the deepest root-to-leaf path: every statement,
/// `Select`, `Expr`, `FromClause`/`FromSource`, projection, CTE, join, order
/// item and window clause counts one. Left-associative operator chains
/// (`1+1+...`), `AND`/`OR` runs and `IN (a, b, ...)` desugaring grow this
/// depth without growing the parens/CASE/unary nesting estimate, and every
/// bind/execute/drop recursion follows it. Debug builds (what `cargo test`
/// uses, on 2 MiB threads) overflow the stack around depth 170 for chains and
/// around 130 for mixed subquery+chain shapes; the cap sits below half of the
/// smallest measured crash point.
pub(crate) const MAX_AST_DEPTH: usize = 64;

/// Parse all statements in `sql` with the parser selected by the
/// `PINKDB_PARSER` environment variable (read per call): `legacy` selects the
/// hand-written lexer + recursive-descent parser; the default is the PEG
/// grammar. Both produce the same AST for the same input (differential-tested
/// in [`peg_parser`]); PEG error messages are positional and worded
/// differently.
///
/// Both parsers apply the same two depth guards: the pre-parse nesting
/// estimate (parens/CASE/unary, protects parser recursion) and the post-parse
/// [`check_ast_depth`] (AST depth, protects bind/execute/drop recursion).
pub fn parse_statements(sql: &str) -> Result<Vec<ast::Statement>> {
    match std::env::var("PINKDB_PARSER").ok().as_deref() {
        Some("legacy") => parser::parse_statements(sql),
        _ => peg_parser::parse_statements(sql),
    }
}

/// Reject an AST whose depth exceeds [`MAX_AST_DEPTH`], iteratively.
///
/// Called by both parsers after building the AST, so they agree on the limit
/// and every entry point (including direct `parser`/`peg_parser` calls) is
/// guarded. The walk uses an explicit worklist — never recursion — because a
/// hostile input can be arbitrarily deep.
///
/// A rejected AST is dismantled with an explicit worklist before the error is
/// returned: dropping an over-deep tree would otherwise recurse through the
/// drop glue and overflow the very stack this guard exists to protect.
pub(crate) fn check_ast_depth(stmts: &mut Vec<ast::Statement>) -> Result<()> {
    let depth = max_ast_depth(stmts);
    if depth <= MAX_AST_DEPTH {
        return Ok(());
    }
    dismantle(stmts);
    Err(PinkDbError::Parse(format!(
        "expression too deep (limit {MAX_AST_DEPTH})"
    )))
}

/// A borrowed AST node in the depth walk.
#[derive(Clone, Copy)]
enum NodeRef<'a> {
    Statement(&'a ast::Statement),
    Select(&'a ast::Select),
    Expr(&'a ast::Expr),
    FromClause(&'a ast::FromClause),
    FromSource(&'a ast::FromSource),
    Projection(&'a ast::Projection),
    Cte(&'a ast::CteDef),
    Join(&'a ast::JoinClause),
    Order(&'a ast::OrderItem),
    Over(&'a ast::OverClause),
}

/// Maximum root-to-leaf node count, via an explicit worklist (no recursion).
fn max_ast_depth(stmts: &[ast::Statement]) -> usize {
    let mut work: Vec<(NodeRef, usize)> =
        stmts.iter().map(|s| (NodeRef::Statement(s), 1)).collect();
    let mut max = 0;
    while let Some((node, depth)) = work.pop() {
        max = max.max(depth);
        let child = depth + 1;
        match node {
            NodeRef::Statement(stmt) => match stmt {
                ast::Statement::Insert { rows, .. } => {
                    for row in rows {
                        for e in row {
                            work.push((NodeRef::Expr(e), child));
                        }
                    }
                }
                ast::Statement::Update { sets, where_clause, .. } => {
                    for (_, e) in sets {
                        work.push((NodeRef::Expr(e), child));
                    }
                    if let Some(e) = where_clause {
                        work.push((NodeRef::Expr(e), child));
                    }
                }
                ast::Statement::Delete { where_clause: Some(e), .. } => {
                    work.push((NodeRef::Expr(e), child));
                }
                ast::Statement::CreateView { query, .. } => {
                    work.push((NodeRef::Select(query), child));
                }
                ast::Statement::Select(select) => {
                    work.push((NodeRef::Select(select), child));
                }
                _ => {}
            },
            NodeRef::Select(select) => {
                for cte in &select.with {
                    work.push((NodeRef::Cte(cte), child));
                }
                for projection in &select.projections {
                    work.push((NodeRef::Projection(projection), child));
                }
                if let Some(from) = &select.from {
                    work.push((NodeRef::FromClause(from), child));
                }
                if let Some(e) = &select.where_clause {
                    work.push((NodeRef::Expr(e), child));
                }
                for e in &select.group_by {
                    work.push((NodeRef::Expr(e), child));
                }
                if let Some(e) = &select.having {
                    work.push((NodeRef::Expr(e), child));
                }
                for item in &select.order_by {
                    work.push((NodeRef::Order(item), child));
                }
                for (_, branch) in &select.unions {
                    work.push((NodeRef::Select(branch), child));
                }
            }
            NodeRef::Expr(expr) => match expr {
                ast::Expr::Binary { left, right, .. } => {
                    work.push((NodeRef::Expr(left), child));
                    work.push((NodeRef::Expr(right), child));
                }
                ast::Expr::Unary { expr, .. }
                | ast::Expr::IsNull { expr, .. }
                | ast::Expr::Cast { expr, .. } => {
                    work.push((NodeRef::Expr(expr), child));
                }
                ast::Expr::Between { expr, low, high, .. } => {
                    work.push((NodeRef::Expr(expr), child));
                    work.push((NodeRef::Expr(low), child));
                    work.push((NodeRef::Expr(high), child));
                }
                ast::Expr::Function { args, .. } => {
                    for arg in args {
                        work.push((NodeRef::Expr(arg), child));
                    }
                }
                ast::Expr::Window { args, over, .. } => {
                    for arg in args {
                        work.push((NodeRef::Expr(arg), child));
                    }
                    work.push((NodeRef::Over(over), child));
                }
                ast::Expr::ScalarSubquery(query) | ast::Expr::Exists { query, .. } => {
                    work.push((NodeRef::Select(query), child));
                }
                ast::Expr::InSubquery { expr, query, .. } => {
                    work.push((NodeRef::Expr(expr), child));
                    work.push((NodeRef::Select(query), child));
                }
                ast::Expr::Coalesce(args) => {
                    for arg in args {
                        work.push((NodeRef::Expr(arg), child));
                    }
                }
                ast::Expr::Case { branches, else_ } => {
                    for (cond, value) in branches {
                        work.push((NodeRef::Expr(cond), child));
                        work.push((NodeRef::Expr(value), child));
                    }
                    if let Some(e) = else_ {
                        work.push((NodeRef::Expr(e), child));
                    }
                }
                ast::Expr::Literal(_)
                | ast::Expr::Star
                | ast::Expr::Column { .. }
                | ast::Expr::QualifiedStar(_) => {}
            },
            NodeRef::FromClause(from) => {
                work.push((NodeRef::FromSource(&from.source), child));
                if let Some(join) = &from.join {
                    work.push((NodeRef::Join(join), child));
                }
            }
            NodeRef::FromSource(source) => {
                if let ast::FromSource::Derived(query) = source {
                    work.push((NodeRef::Select(query), child));
                }
            }
            NodeRef::Projection(projection) => {
                if let ast::Projection::Expr { expr, .. } = projection {
                    work.push((NodeRef::Expr(expr), child));
                }
            }
            NodeRef::Cte(cte) => {
                work.push((NodeRef::Select(&cte.query), child));
            }
            NodeRef::Join(join) => {
                work.push((NodeRef::FromSource(&join.source), child));
                work.push((NodeRef::Expr(&join.on), child));
            }
            NodeRef::Order(item) => {
                work.push((NodeRef::Expr(&item.expr), child));
            }
            NodeRef::Over(over) => {
                for e in &over.partition {
                    work.push((NodeRef::Expr(e), child));
                }
                for item in &over.order {
                    work.push((NodeRef::Order(item), child));
                }
            }
        }
    }
    max
}

/// Free an over-deep AST without recursing: move every child into an explicit
/// worklist and drop each node only after its children have been taken out of
/// it. Without this, the recursive drop glue would overflow the stack after
/// the guard rejected the tree.
fn dismantle(stmts: &mut Vec<ast::Statement>) {
    let mut exprs: Vec<ast::Expr> = Vec::new();
    let mut selects: Vec<ast::Select> = Vec::new();
    for stmt in stmts.iter_mut() {
        take_statement_children(stmt, &mut exprs, &mut selects);
    }
    stmts.clear();
    // Drain both worklists together: taking a `Select` apart can enqueue new
    // expressions (and vice versa), so neither may be considered finished
    // while the other still holds nodes.
    while !exprs.is_empty() || !selects.is_empty() {
        while let Some(mut expr) = exprs.pop() {
            take_expr_children(&mut expr, &mut exprs, &mut selects);
        }
        if let Some(mut select) = selects.pop() {
            take_select_children(&mut select, &mut exprs, &mut selects);
        }
    }
}

/// An empty `Select`, used as the replacement when a child is moved out.
fn empty_select() -> ast::Select {
    ast::Select {
        with: Vec::new(),
        distinct: false,
        projections: Vec::new(),
        from: None,
        where_clause: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        limit: None,
        unions: Vec::new(),
    }
}

/// A placeholder expression, used as the replacement when a child is moved out.
fn null_expr() -> ast::Expr {
    ast::Expr::Literal(Value::Null)
}

fn take_statement_children(
    stmt: &mut ast::Statement,
    exprs: &mut Vec<ast::Expr>,
    selects: &mut Vec<ast::Select>,
) {
    match stmt {
        ast::Statement::Insert { rows, .. } => {
            for row in rows.drain(..) {
                exprs.extend(row);
            }
        }
        ast::Statement::Update { sets, where_clause, .. } => {
            for (_, e) in sets.drain(..) {
                exprs.push(e);
            }
            if let Some(e) = where_clause.take() {
                exprs.push(e);
            }
        }
        ast::Statement::Delete { where_clause, .. } => {
            if let Some(e) = where_clause.take() {
                exprs.push(e);
            }
        }
        ast::Statement::CreateView { query, .. } => {
            selects.push(std::mem::replace(&mut **query, empty_select()));
        }
        ast::Statement::Select(select) => {
            selects.push(std::mem::replace(select, empty_select()));
        }
        _ => {}
    }
}

fn take_select_children(
    select: &mut ast::Select,
    exprs: &mut Vec<ast::Expr>,
    selects: &mut Vec<ast::Select>,
) {
    for cte in select.with.drain(..) {
        selects.push(*cte.query);
    }
    for projection in select.projections.drain(..) {
        if let ast::Projection::Expr { expr, .. } = projection {
            exprs.push(expr);
        }
    }
    if let Some(from) = select.from.take() {
        take_from_children(from, exprs, selects);
    }
    if let Some(e) = select.where_clause.take() {
        exprs.push(e);
    }
    exprs.append(&mut select.group_by);
    if let Some(e) = select.having.take() {
        exprs.push(e);
    }
    for item in select.order_by.drain(..) {
        exprs.push(item.expr);
    }
    for (_, branch) in select.unions.drain(..) {
        selects.push(branch);
    }
}

fn take_from_children(
    mut from: ast::FromClause,
    exprs: &mut Vec<ast::Expr>,
    selects: &mut Vec<ast::Select>,
) {
    take_source_children(&mut from.source, selects);
    if let Some(mut join) = from.join.take() {
        take_source_children(&mut join.source, selects);
        exprs.push(join.on);
    }
}

fn take_source_children(source: &mut ast::FromSource, selects: &mut Vec<ast::Select>) {
    if let ast::FromSource::Derived(query) = source {
        selects.push(std::mem::replace(&mut **query, empty_select()));
    }
}

fn take_expr_children(
    expr: &mut ast::Expr,
    exprs: &mut Vec<ast::Expr>,
    selects: &mut Vec<ast::Select>,
) {
    match expr {
        ast::Expr::Binary { left, right, .. } => {
            exprs.push(std::mem::replace(&mut **left, null_expr()));
            exprs.push(std::mem::replace(&mut **right, null_expr()));
        }
        ast::Expr::Unary { expr, .. }
        | ast::Expr::IsNull { expr, .. }
        | ast::Expr::Cast { expr, .. } => {
            exprs.push(std::mem::replace(&mut **expr, null_expr()));
        }
        ast::Expr::Between { expr, low, high, .. } => {
            exprs.push(std::mem::replace(&mut **expr, null_expr()));
            exprs.push(std::mem::replace(&mut **low, null_expr()));
            exprs.push(std::mem::replace(&mut **high, null_expr()));
        }
        ast::Expr::Function { args, .. } => exprs.append(args),
        ast::Expr::Window { args, over, .. } => {
            exprs.append(args);
            exprs.append(&mut over.partition);
            for item in over.order.drain(..) {
                exprs.push(item.expr);
            }
        }
        ast::Expr::ScalarSubquery(query) | ast::Expr::Exists { query, .. } => {
            selects.push(std::mem::replace(&mut **query, empty_select()));
        }
        ast::Expr::InSubquery { expr, query, .. } => {
            exprs.push(std::mem::replace(&mut **expr, null_expr()));
            selects.push(std::mem::replace(&mut **query, empty_select()));
        }
        ast::Expr::Coalesce(args) => exprs.append(args),
        ast::Expr::Case { branches, else_ } => {
            for (cond, value) in branches.drain(..) {
                exprs.push(cond);
                exprs.push(value);
            }
            if let Some(e) = else_.take() {
                exprs.push(*e);
            }
        }
        ast::Expr::Literal(_)
        | ast::Expr::Star
        | ast::Expr::Column { .. }
        | ast::Expr::QualifiedStar(_) => {}
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

    // ---------- AST-depth guard ----------

    /// `SELECT 1+1+...;` with `terms - 1` additions. The AST costs three
    /// nodes (statement, Select, projection) plus one per term.
    fn chain_sql(terms: usize) -> String {
        let mut sql = String::from("SELECT 1");
        for _ in 1..terms {
            sql.push_str("+1");
        }
        sql.push(';');
        sql
    }

    /// The cap and cap+1 chain are the same boundary for both parsers: at the
    /// cap the ASTs are equal, one term over both parsers reject with the
    /// shared message.
    #[test]
    fn ast_depth_guard_boundary_is_shared_by_both_parsers() {
        let at_cap = chain_sql(MAX_AST_DEPTH - 3);
        let over_cap = chain_sql(MAX_AST_DEPTH - 2);
        let l = parser::parse_statements(&at_cap).expect("legacy accepts the cap");
        let p = peg_parser::parse_statements(&at_cap).expect("PEG accepts the cap");
        assert_eq!(l, p);
        for sql in [&over_cap] {
            for (label, e) in [
                ("legacy", parser::parse_statements(sql)),
                ("peg", peg_parser::parse_statements(sql)),
            ] {
                let msg = e.expect_err(label).to_string();
                assert_eq!(
                    msg,
                    format!("expression too deep (limit {MAX_AST_DEPTH})"),
                    "{label}"
                );
            }
        }
        // The selected-parser entry point applies the same guard.
        assert!(parse_statements(&at_cap).is_ok());
        assert!(parse_statements(&over_cap).is_err());
    }

    /// Chains, `IN` lists and `AND` runs all grow the AST without growing the
    /// pre-parse nesting estimate; both parsers reject deep ones cleanly.
    #[test]
    fn deep_operator_chains_in_lists_and_and_runs_are_rejected() {
        let in_list = format!("SELECT 1 IN (1{});", ", 1".repeat(1000));
        let and_run = format!("SELECT true{};", " AND true".repeat(1000));
        for sql in [in_list, and_run] {
            for (label, e) in [
                ("legacy", parser::parse_statements(&sql)),
                ("peg", peg_parser::parse_statements(&sql)),
            ] {
                let msg = e.expect_err(label).to_string();
                assert!(msg.contains("expression too deep"), "{label}: {msg}");
            }
        }
    }

    /// Far beyond the recursive-drop crash point (~19k nodes on a 2 MiB
    /// debug stack): rejecting must dismantle the AST iteratively, so this
    /// test thread cannot abort.
    #[test]
    fn huge_chain_is_rejected_and_freed_without_recursion() {
        let sql = chain_sql(100_000);
        for (label, e) in [
            ("legacy", parser::parse_statements(&sql)),
            ("peg", peg_parser::parse_statements(&sql)),
        ] {
            let msg = e.expect_err(label).to_string();
            assert!(msg.contains("expression too deep"), "{label}: {msg}");
        }
    }
}
