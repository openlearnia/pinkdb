//! PEG-based SQL parser (`peg` crate) — the engine's only parser path
//! (see [`super::parse_statements`]).
//!
//! The grammar lexes and parses `&str` in one pass with the same rules the
//! SQL surface documents: case-insensitive keywords ([`super::keyword`]),
//! `''`/`""` escapes, `--`/`/* */` comments, leading-dot and exponent
//! floats, and i64-range integers. The corpus and seeded-fuzz tests at the
//! bottom of this file pin that valid input parses and invalid input is a
//! clean [`PinkDbError::Parse`].
//!
//! Grammar shape (low → high precedence): OR → AND → NOT → comparison
//! (`= <> != < <= > >=`, `IS [NOT] NULL`, `[NOT] BETWEEN`, `[NOT] IN
//! (list|subquery)`, `[NOT] LIKE`) → `||` concatenation → additive →
//! multiplicative → unary → primary. Every token rule consumes trailing
//! whitespace/comments, so composite rules read as bare token sequences.

use super::ast::*;
use super::keyword::Keyword;
use crate::types::{DataType, PinkDbError, Result, Value};

/// Parse all statements in `sql` with the PEG grammar. Statements may be
/// `;`-separated (the final `;` is optional); empty input yields no statements.
///
/// Errors are [`PinkDbError::Parse`]; messages are positional and worded as
/// PEG reports them (the furthest expected token).
pub fn parse_statements(sql: &str) -> Result<Vec<Statement>> {
    Ok(parse_prepared(sql)?.0)
}

/// [`parse_statements`] plus the `?` placeholder positions: every
/// [`Expr::Parameter`] is renumbered to its 0-based textual rank (the order
/// the placeholders appear in `sql`), and the returned vector holds the byte
/// offset of each `?` in that same order. Its length is the statement's
/// parameter count; the bind API takes one value per entry.
///
/// Placeholders in positions the engine cannot bind at execute time are
/// rejected here (see [`reject_parameter_positions`]).
pub(crate) fn parse_prepared(sql: &str) -> Result<(Vec<Statement>, Vec<usize>)> {
    super::check_nesting_depth(sql)?;
    reject_nul_in_quoted_identifier(sql)?;
    let mut out =
        peg_sql::statements(sql).map_err(|e| PinkDbError::Parse(format!("parse error: {e}")))?;
    // Post-parse depth guard (shared with the pre-parse nesting cap).
    super::check_ast_depth(&mut out)?;
    let offsets = renumber_parameters(&mut out)?;
    Ok((out, offsets))
}

/// Renumber every [`Expr::Parameter`] payload from a source byte offset to
/// its 0-based textual rank, and return the offsets in textual order.
///
/// Placeholders are rejected first ([`reject_parameter_positions`]) so the
/// error names the clause, not the statement's position.
fn renumber_parameters(stmts: &mut [Statement]) -> Result<Vec<usize>> {
    reject_parameter_positions(stmts)?;
    let mut offsets = Vec::new();
    for stmt in stmts.iter_mut() {
        collect_parameter_offsets(stmt, &mut offsets);
    }
    offsets.sort_unstable();
    offsets.dedup();
    for stmt in stmts.iter_mut() {
        apply_parameter_ranks(stmt, &offsets);
    }
    Ok(offsets)
}

// ---------- placeholder position checks ----------

/// Reject `?` placeholders in statement shapes whose substituted form would
/// not mean the same thing:
///
/// - `ORDER BY ?` / `GROUP BY ?`: the substituted literal is re-read as a
///   result-column ordinal (`ORDER BY 1`) or a constant key, neither of which
///   is the parameter the caller wrote. DuckDB refuses parameters in these
///   clauses; so does this engine.
/// - `CREATE VIEW ... SELECT ?`: the view stores its AST for per-use
///   rebinding, and a stored placeholder has no bind call to take a value
///   from.
///
/// Every other position is legal (comparisons, arithmetic, function
/// arguments, `CASE`, subqueries, `VALUES`, `SET`, `WHERE`, ...).
fn reject_parameter_positions(stmts: &[Statement]) -> Result<()> {
    for stmt in stmts {
        match stmt {
            Statement::CreateView { query, .. } => {
                if select_contains_parameter(query) {
                    return Err(PinkDbError::Parameter(
                        "parameters are not supported in CREATE VIEW".to_string(),
                    ));
                }
            }
            Statement::CreateTableAs { query, .. } => check_select_positions(query)?,
            Statement::Insert { source, .. } => match source {
                InsertSource::Values(rows) => {
                    for row in rows {
                        for e in row {
                            check_expr_positions(e)?;
                        }
                    }
                }
                InsertSource::Select(query) => check_select_positions(query)?,
            },
            Statement::Update {
                sets, where_clause, ..
            } => {
                for (_, e) in sets {
                    check_expr_positions(e)?;
                }
                if let Some(e) = where_clause {
                    check_expr_positions(e)?;
                }
            }
            Statement::Delete {
                where_clause: Some(e),
                ..
            } => check_expr_positions(e)?,
            Statement::Explain(query) => check_select_positions(query)?,
            Statement::Select(query) => check_select_positions(query)?,
            _ => {}
        }
    }
    Ok(())
}

/// Per-select clause check (nested selects recurse; each has its own clauses).
fn check_select_positions(sel: &Select) -> Result<()> {
    for cte in &sel.with {
        check_select_positions(&cte.query)?;
    }
    for p in &sel.projections {
        if let Projection::Expr { expr, .. } = p {
            check_expr_positions(expr)?;
        }
    }
    if let Some(from) = &sel.from {
        check_source_positions(&from.source)?;
        for join in &from.joins {
            check_source_positions(&join.source)?;
            if let Some(on) = &join.on {
                check_expr_positions(on)?;
            }
        }
    }
    if let Some(e) = &sel.where_clause {
        check_expr_positions(e)?;
    }
    for key in &sel.group_by {
        if expr_contains_parameter(key) {
            return Err(PinkDbError::Parameter(
                "parameters are not supported in GROUP BY".to_string(),
            ));
        }
    }
    if let Some(e) = &sel.having {
        check_expr_positions(e)?;
    }
    for item in &sel.order_by {
        if expr_contains_parameter(&item.expr) {
            return Err(PinkDbError::Parameter(
                "parameters are not supported in ORDER BY".to_string(),
            ));
        }
    }
    for (_, branch) in &sel.unions {
        check_select_positions(branch)?;
    }
    Ok(())
}

fn check_source_positions(source: &FromSource) -> Result<()> {
    match source {
        FromSource::Derived(query) => check_select_positions(query),
        FromSource::Table { .. } => Ok(()),
    }
}

/// Walk one expression, rejecting placeholders only in the clauses
/// [`check_select_positions`] owns; every nested subquery resets to its own
/// per-select rules.
fn check_expr_positions(expr: &Expr) -> Result<()> {
    match expr {
        Expr::Parameter(_) | Expr::Literal(_) | Expr::Star | Expr::QualifiedStar(_) => Ok(()),
        Expr::Column { .. } => Ok(()),
        Expr::Binary { left, right, .. } => {
            check_expr_positions(left)?;
            check_expr_positions(right)
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } | Expr::Cast { expr, .. } => {
            check_expr_positions(expr)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            check_expr_positions(expr)?;
            check_expr_positions(low)?;
            check_expr_positions(high)
        }
        Expr::Function { args, .. } | Expr::Coalesce(args) => {
            for a in args {
                check_expr_positions(a)?;
            }
            Ok(())
        }
        Expr::Window { args, over, .. } => {
            for a in args {
                check_expr_positions(a)?;
            }
            for e in &over.partition {
                check_expr_positions(e)?;
            }
            for item in &over.order {
                check_expr_positions(&item.expr)?;
            }
            Ok(())
        }
        Expr::ScalarSubquery(query) | Expr::Exists { query, .. } => check_select_positions(query),
        Expr::InSubquery { expr, query, .. } => {
            check_expr_positions(expr)?;
            check_select_positions(query)
        }
        Expr::Case { branches, else_ } => {
            for (cond, value) in branches {
                check_expr_positions(cond)?;
                check_expr_positions(value)?;
            }
            if let Some(e) = else_ {
                check_expr_positions(e)?;
            }
            Ok(())
        }
    }
}

/// Whether `expr` (or any nested subquery) holds a `?`.
fn expr_contains_parameter(expr: &Expr) -> bool {
    match expr {
        Expr::Parameter(_) => true,
        Expr::Literal(_) | Expr::Star | Expr::QualifiedStar(_) | Expr::Column { .. } => false,
        Expr::Binary { left, right, .. } => {
            expr_contains_parameter(left) || expr_contains_parameter(right)
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } | Expr::Cast { expr, .. } => {
            expr_contains_parameter(expr)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_parameter(expr)
                || expr_contains_parameter(low)
                || expr_contains_parameter(high)
        }
        Expr::Function { args, .. } | Expr::Coalesce(args) => {
            args.iter().any(expr_contains_parameter)
        }
        Expr::Window { args, over, .. } => {
            args.iter().any(expr_contains_parameter)
                || over.partition.iter().any(expr_contains_parameter)
                || over
                    .order
                    .iter()
                    .any(|item| expr_contains_parameter(&item.expr))
        }
        Expr::ScalarSubquery(query) | Expr::Exists { query, .. } => {
            select_contains_parameter(query)
        }
        Expr::InSubquery { expr, query, .. } => {
            expr_contains_parameter(expr) || select_contains_parameter(query)
        }
        Expr::Case { branches, else_ } => {
            branches
                .iter()
                .any(|(c, v)| expr_contains_parameter(c) || expr_contains_parameter(v))
                || else_.as_ref().is_some_and(|e| expr_contains_parameter(e))
        }
    }
}

fn select_contains_parameter(sel: &Select) -> bool {
    sel.with
        .iter()
        .any(|cte| select_contains_parameter(&cte.query))
        || sel.projections.iter().any(|p| match p {
            Projection::Expr { expr, .. } => expr_contains_parameter(expr),
            _ => false,
        })
        || sel.from.as_ref().is_some_and(from_contains_parameter)
        || sel
            .where_clause
            .as_ref()
            .is_some_and(expr_contains_parameter)
        || sel.group_by.iter().any(expr_contains_parameter)
        || sel.having.as_ref().is_some_and(expr_contains_parameter)
        || sel
            .order_by
            .iter()
            .any(|item| expr_contains_parameter(&item.expr))
        || sel.unions.iter().any(|(_, b)| select_contains_parameter(b))
}

fn from_contains_parameter(from: &FromClause) -> bool {
    let source = match &from.source {
        FromSource::Derived(query) => select_contains_parameter(query),
        FromSource::Table { .. } => false,
    };
    source
        || from.joins.iter().any(|join| {
            let s = match &join.source {
                FromSource::Derived(query) => select_contains_parameter(query),
                FromSource::Table { .. } => false,
            };
            s || join.on.as_ref().is_some_and(expr_contains_parameter)
        })
}

// ---------- placeholder collection / renumbering ----------

fn collect_parameter_offsets(stmt: &Statement, out: &mut Vec<usize>) {
    match stmt {
        Statement::CreateTableAs { query, .. }
        | Statement::CreateView { query, .. }
        | Statement::Explain(query) => collect_select_offsets(query, out),
        Statement::Insert { source, .. } => match source {
            InsertSource::Values(rows) => {
                for row in rows {
                    for e in row {
                        collect_expr_offsets(e, out);
                    }
                }
            }
            InsertSource::Select(query) => collect_select_offsets(query, out),
        },
        Statement::Update {
            sets, where_clause, ..
        } => {
            for (_, e) in sets {
                collect_expr_offsets(e, out);
            }
            if let Some(e) = where_clause {
                collect_expr_offsets(e, out);
            }
        }
        Statement::Delete {
            where_clause: Some(e),
            ..
        } => collect_expr_offsets(e, out),
        Statement::Select(sel) => collect_select_offsets(sel, out),
        _ => {}
    }
}

fn collect_select_offsets(sel: &Select, out: &mut Vec<usize>) {
    for cte in &sel.with {
        collect_select_offsets(&cte.query, out);
    }
    for p in &sel.projections {
        if let Projection::Expr { expr, .. } = p {
            collect_expr_offsets(expr, out);
        }
    }
    if let Some(from) = &sel.from {
        collect_source_offsets(&from.source, out);
        for join in &from.joins {
            collect_source_offsets(&join.source, out);
            if let Some(on) = &join.on {
                collect_expr_offsets(on, out);
            }
        }
    }
    if let Some(e) = &sel.where_clause {
        collect_expr_offsets(e, out);
    }
    for e in &sel.group_by {
        collect_expr_offsets(e, out);
    }
    if let Some(e) = &sel.having {
        collect_expr_offsets(e, out);
    }
    for item in &sel.order_by {
        collect_expr_offsets(&item.expr, out);
    }
    for (_, branch) in &sel.unions {
        collect_select_offsets(branch, out);
    }
}

fn collect_source_offsets(source: &FromSource, out: &mut Vec<usize>) {
    if let FromSource::Derived(query) = source {
        collect_select_offsets(query, out);
    }
}

fn collect_expr_offsets(expr: &Expr, out: &mut Vec<usize>) {
    match expr {
        Expr::Parameter(off) => out.push(*off),
        Expr::Literal(_) | Expr::Star | Expr::QualifiedStar(_) | Expr::Column { .. } => {}
        Expr::Binary { left, right, .. } => {
            collect_expr_offsets(left, out);
            collect_expr_offsets(right, out);
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } | Expr::Cast { expr, .. } => {
            collect_expr_offsets(expr, out)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_offsets(expr, out);
            collect_expr_offsets(low, out);
            collect_expr_offsets(high, out);
        }
        Expr::Function { args, .. } | Expr::Coalesce(args) => {
            for a in args {
                collect_expr_offsets(a, out);
            }
        }
        Expr::Window { args, over, .. } => {
            for a in args {
                collect_expr_offsets(a, out);
            }
            for e in &over.partition {
                collect_expr_offsets(e, out);
            }
            for item in &over.order {
                collect_expr_offsets(&item.expr, out);
            }
        }
        Expr::ScalarSubquery(query) | Expr::Exists { query, .. } => {
            collect_select_offsets(query, out)
        }
        Expr::InSubquery { expr, query, .. } => {
            collect_expr_offsets(expr, out);
            collect_select_offsets(query, out);
        }
        Expr::Case { branches, else_ } => {
            for (cond, value) in branches {
                collect_expr_offsets(cond, out);
                collect_expr_offsets(value, out);
            }
            if let Some(e) = else_ {
                collect_expr_offsets(e, out);
            }
        }
    }
}

fn apply_parameter_ranks(stmt: &mut Statement, offsets: &[usize]) {
    match stmt {
        Statement::CreateTableAs { query, .. }
        | Statement::CreateView { query, .. }
        | Statement::Explain(query) => apply_select_ranks(query, offsets),
        Statement::Insert { source, .. } => match source {
            InsertSource::Values(rows) => {
                for row in rows {
                    for e in row {
                        apply_expr_ranks(e, offsets);
                    }
                }
            }
            InsertSource::Select(query) => apply_select_ranks(query, offsets),
        },
        Statement::Update {
            sets, where_clause, ..
        } => {
            for (_, e) in sets {
                apply_expr_ranks(e, offsets);
            }
            if let Some(e) = where_clause {
                apply_expr_ranks(e, offsets);
            }
        }
        Statement::Delete {
            where_clause: Some(e),
            ..
        } => apply_expr_ranks(e, offsets),
        Statement::Select(sel) => apply_select_ranks(sel, offsets),
        _ => {}
    }
}

fn apply_select_ranks(sel: &mut Select, offsets: &[usize]) {
    for cte in sel.with.iter_mut() {
        apply_select_ranks(&mut cte.query, offsets);
    }
    for p in sel.projections.iter_mut() {
        if let Projection::Expr { expr, .. } = p {
            apply_expr_ranks(expr, offsets);
        }
    }
    if let Some(from) = sel.from.as_mut() {
        apply_source_ranks(&mut from.source, offsets);
        for join in from.joins.iter_mut() {
            apply_source_ranks(&mut join.source, offsets);
            if let Some(on) = join.on.as_mut() {
                apply_expr_ranks(on, offsets);
            }
        }
    }
    if let Some(e) = sel.where_clause.as_mut() {
        apply_expr_ranks(e, offsets);
    }
    for e in sel.group_by.iter_mut() {
        apply_expr_ranks(e, offsets);
    }
    if let Some(e) = sel.having.as_mut() {
        apply_expr_ranks(e, offsets);
    }
    for item in sel.order_by.iter_mut() {
        apply_expr_ranks(&mut item.expr, offsets);
    }
    for (_, branch) in sel.unions.iter_mut() {
        apply_select_ranks(branch, offsets);
    }
}

fn apply_source_ranks(source: &mut FromSource, offsets: &[usize]) {
    if let FromSource::Derived(query) = source {
        apply_select_ranks(query, offsets);
    }
}

fn apply_expr_ranks(expr: &mut Expr, offsets: &[usize]) {
    match expr {
        Expr::Parameter(off) => {
            debug_assert!(offsets.binary_search(off).is_ok());
            *off = offsets.binary_search(off).unwrap_or(0);
        }
        Expr::Literal(_) | Expr::Star | Expr::QualifiedStar(_) | Expr::Column { .. } => {}
        Expr::Binary { left, right, .. } => {
            apply_expr_ranks(left, offsets);
            apply_expr_ranks(right, offsets);
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } | Expr::Cast { expr, .. } => {
            apply_expr_ranks(expr, offsets)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            apply_expr_ranks(expr, offsets);
            apply_expr_ranks(low, offsets);
            apply_expr_ranks(high, offsets);
        }
        Expr::Function { args, .. } | Expr::Coalesce(args) => {
            for a in args {
                apply_expr_ranks(a, offsets);
            }
        }
        Expr::Window { args, over, .. } => {
            for a in args {
                apply_expr_ranks(a, offsets);
            }
            for e in over.partition.iter_mut() {
                apply_expr_ranks(e, offsets);
            }
            for item in over.order.iter_mut() {
                apply_expr_ranks(&mut item.expr, offsets);
            }
        }
        Expr::ScalarSubquery(query) | Expr::Exists { query, .. } => {
            apply_select_ranks(query, offsets)
        }
        Expr::InSubquery { expr, query, .. } => {
            apply_expr_ranks(expr, offsets);
            apply_select_ranks(query, offsets);
        }
        Expr::Case { branches, else_ } => {
            for (cond, value) in branches.iter_mut() {
                apply_expr_ranks(cond, offsets);
                apply_expr_ranks(value, offsets);
            }
            if let Some(e) = else_.as_mut() {
                apply_expr_ranks(e, offsets);
            }
        }
    }
}

/// Reject a NUL byte inside a quoted identifier with its exact position.
///
/// The PEG grammar also rejects it (see `quoted_ident`), but an action's `Err`
/// can only enter peg's expected-set, which renders as
/// `expected identifier contains a NUL byte` with the position of the
/// *closing* quote. Scanning the raw text up front — only when the input
/// contains a NUL at all — reports the opening quote instead, the positional
/// error the NUL-in-name contract (C bindings) is pinned to.
fn reject_nul_in_quoted_identifier(sql: &str) -> Result<()> {
    if !sql.as_bytes().contains(&0) {
        return Ok(());
    }
    match super::split::nul_in_quoted_identifier(sql) {
        Some(off) => Err(PinkDbError::Parse(format!(
            "parse error at offset {off}: identifier contains a NUL byte"
        ))),
        None => Ok(()),
    }
}

/// What may follow the left operand of a comparison-level expression.
/// `apply_cmp` turns `(left, tail)` into the AST node the tail describes.
enum CmpTail {
    IsNull {
        negated: bool,
    },
    Between {
        negated: bool,
        low: Expr,
        high: Expr,
    },
    InList {
        negated: bool,
        items: Vec<Expr>,
    },
    InSubquery {
        negated: bool,
        query: Box<Select>,
    },
    Like {
        negated: bool,
        right: Expr,
    },
    Binary {
        op: BinaryOp,
        right: Expr,
    },
}

fn apply_cmp(left: Expr, tail: CmpTail) -> Expr {
    match tail {
        CmpTail::IsNull { negated } => Expr::IsNull {
            expr: Box::new(left),
            negated,
        },
        CmpTail::Between { negated, low, high } => Expr::Between {
            expr: Box::new(left),
            low: Box::new(low),
            high: Box::new(high),
            negated,
        },
        CmpTail::InList { negated, items } => desugar_in_list(left, items, negated),
        CmpTail::InSubquery { negated, query } => Expr::InSubquery {
            expr: Box::new(left),
            query,
            negated,
        },
        CmpTail::Like { negated, right } => Expr::Binary {
            op: if negated {
                BinaryOp::NotLike
            } else {
                BinaryOp::Like
            },
            left: Box::new(left),
            right: Box::new(right),
        },
        CmpTail::Binary { op, right } => Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

/// `x IN (a, b)` → `(x = a) OR (x = b)`; `IN ()` → FALSE.
fn desugar_in_list(left: Expr, items: Vec<Expr>, negated: bool) -> Expr {
    let mut acc = Expr::Literal(Value::Boolean(false));
    for item in items {
        let eq = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(left.clone()),
            right: Box::new(item),
        };
        acc = Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(acc),
            right: Box::new(eq),
        };
    }
    if negated {
        Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(acc),
        }
    } else {
        acc
    }
}

/// Simple `CASE x WHEN c THEN v` desugars each `WHEN c` to `WHEN x = c`
/// (`x` is cloned per branch).
fn desugar_case(operand: Option<Expr>, branches: Vec<(Expr, Expr)>) -> Vec<(Expr, Expr)> {
    match operand {
        None => branches,
        Some(op) => branches
            .into_iter()
            .map(|(cond, val)| {
                let cond = Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(op.clone()),
                    right: Box::new(cond),
                };
                (cond, val)
            })
            .collect(),
    }
}

/// Integer literal → `Value` with DuckDB's magnitude rule (INTEGER when it
/// fits i32, else BIGINT); out-of-i64-range text is a parse error, exactly like
/// the lexer's `Token::Integer` overflow error.
fn int_value(text: &str) -> std::result::Result<Value, &'static str> {
    let n: i64 = text.parse().map_err(|_| "integer literal out of range")?;
    if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
        Ok(Value::Integer(n as i32))
    } else {
        Ok(Value::Bigint(n))
    }
}

peg::parser! {
    grammar peg_sql() for str {
        use super::{apply_cmp, desugar_case, int_value, CmpTail};

        // ---------- trivia (mirrors the lexer's skip rules) ----------
        // `ws` runs after every token, so the cursor is always at a non-trivia
        // character when a rule is entered (or at EOF).

        rule ws() = quiet!{ (space() / line_comment() / block_comment())* }
        rule space() = [' ' | '\t' | '\n' | '\x0C' | '\r']+
        rule line_comment() = "--" [^'\n']*
        rule block_comment() = "/*" (!"*/" [_])* "*/"

        // ---------- tokens ----------

        rule semi() = ";" ws()
        rule comma() = "," ws()
        // `.` is only a token when not the start of a leading-dot float.
        rule dot() = "." !['0'..='9'] ws()
        rule lparen() = "(" ws()
        rule rparen() = ")" ws()
        rule star() = "*" ws()
        rule plus() = "+" ws()
        // `-` never starts a token when it opens a `--` line comment.
        rule minus() = "-" !"-" ws()
        // `/` never starts a token when it opens a `/*` block comment.
        rule slash() = "/" !"*" ws()
        rule percent() = "%" ws()
        rule eq() = "=" ws()

        rule cmp_op() -> BinaryOp
            = "<=" ws() { BinaryOp::LtEq }
            / "<>" ws() { BinaryOp::NotEq }
            / "!=" ws() { BinaryOp::NotEq }
            / "<" ws() { BinaryOp::Lt }
            / ">=" ws() { BinaryOp::GtEq }
            / ">" ws() { BinaryOp::Gt }
            / "=" ws() { BinaryOp::Eq }

        rule add_op() -> BinaryOp
            = plus() { BinaryOp::Add }
            / minus() { BinaryOp::Subtract }

        rule mul_op() -> BinaryOp
            = star() { BinaryOp::Multiply }
            / slash() { BinaryOp::Divide }
            / percent() { BinaryOp::Modulo }

        // ---------- identifiers and keywords ----------

        rule ident_raw() -> &'input str
            = $(['a'..='z' | 'A'..='Z' | '_'] ['a'..='z' | 'A'..='Z' | '0'..='9' | '_']*)

        rule kw(k: Keyword)
            = s:ident_raw() ws() {? if Keyword::lookup(s) == Some(k) { Ok(()) } else { Err("keyword") } }

        /// `[keyword]` as a bool (capturing a no-value rule is not allowed).
        rule opt_kw(k: Keyword) -> bool
            = kw(k) { true }
            / { false }

        // `ALL` is not a reserved keyword (kept a plain identifier, like the
        // lexer): `UNION ALL` matches it case-insensitively.
        rule kw_all()
            = s:ident_raw() ws() {? if s.eq_ignore_ascii_case("all") { Ok(()) } else { Err("ALL") } }

        /// A plain identifier; reserved keywords must be double-quoted.
        rule ident() -> String
            = quoted_ident()
            / s:ident_raw() ws() {?
                  match Keyword::lookup(s) {
                      None => Ok(s.to_string()),
                      Some(_) => Err("identifier"),
                  }
              }

        rule quoted_ident() -> String
            = "\"" c:quoted_ident_char()* "\"" ws() {?
                  if c.contains(&'\0') {
                      Err("identifier contains a NUL byte")
                  } else {
                      Ok(c.into_iter().collect())
                  }
              }

        rule quoted_ident_char() -> char
            = "\"\"" { '"' }
            / c:[^'"'] { c }

        // ---------- literals ----------

        rule string_lit() -> String
            = "'" c:string_char()* "'" ws() { c.into_iter().collect() }

        rule string_char() -> char
            = "''" { '\'' }
            / c:[^'\''] { c }

        /// Float lexemes: fraction and/or exponent (mirrors `lex_number`).
        rule float_text() -> &'input str
            = $(['0'..='9']+ "." ['0'..='9']+ (['e' | 'E'] ['+' | '-']? ['0'..='9']+)?)
            / $(['0'..='9']+ ['e' | 'E'] ['+' | '-']? ['0'..='9']+)
            / $("." ['0'..='9']+ (['e' | 'E'] ['+' | '-']? ['0'..='9']+)?)

        /// Integer lexemes: digits that are *not* followed by a fraction or a
        /// valid exponent (otherwise the lexer would have produced a float).
        rule int_text() -> &'input str
            = n:$(['0'..='9']+) !("." ['0'..='9']) !(['e' | 'E'] ['+' | '-']? ['0'..='9']) { n }

        rule number() -> Value
            = n:float_text() ws() {? n.parse::<f64>().map(Value::Double).or(Err("double literal")) }
            / n:int_text() ws() {? int_value(n) }

        rule nonneg_integer() -> usize
            = n:int_text() ws() {? n.parse::<i64>().map(|v| v as usize).or(Err("integer")) }

        // ---------- statements ----------

        pub rule statements() -> Vec<Statement>
            = ws() semi()* first:statement()? rest:(semi()+ s:statement() { s })* semi()* {
                  let mut out = Vec::new();
                  if let Some(s) = first {
                      out.push(s);
                  }
                  out.extend(rest);
                  out
              }

        rule statement() -> Statement
            = create()
            / insert()
            / update()
            / delete()
            / txn()
            / use_stmt()
            / load_extension()
            / drop()
            / checkpoint()
            / show_stmt()
            / describe_stmt()
            / explain_stmt()
            / select_statement()

        rule select_statement() -> Statement = s:select() { Statement::Select(s) }

        /// A non-reserved word matched case-insensitively (used for syntax
        /// words that must stay usable as identifiers: SHOW, TABLES, IF,
        /// NULLS, ...). Consumes trailing trivia like every token.
        rule word(w: &'static str)
            = s:ident_raw() ws() {? if s.eq_ignore_ascii_case(w) { Ok(()) } else { Err(w) } }

        rule if_not_exists() -> bool
            = word("if") kw(Keyword::Not) kw(Keyword::Exists) { true }
            / { false }

        rule if_exists() -> bool
            = word("if") kw(Keyword::Exists) { true }
            / { false }

        rule use_stmt() -> Statement
            = kw(Keyword::Use) name:ident() { Statement::Use { name } }

        /// `SHOW TABLES` / `SHOW DATABASES` — matched as words, not reserved
        /// keywords, so `show` and `tables` stay usable as names.
        rule show_stmt() -> Statement
            = word("show") word("tables") { Statement::ShowTables }
            / word("show") word("databases") { Statement::ShowDatabases }

        /// `DESCRIBE <table>`: DuckDB-shaped column metadata.
        rule describe_stmt() -> Statement
            = word("describe") t:table_ref() { Statement::Describe { db: t.0, name: t.1 } }

        /// `EXPLAIN <select>` — the plain (optimized) plan text.
        rule explain_stmt() -> Statement
            = word("explain") q:select() { Statement::Explain(Box::new(q)) }

        /// `CHECKPOINT` — matched as a word, not a reserved keyword, so
        /// `checkpoint` stays usable as a table or column name.
        rule checkpoint() -> Statement
            = s:ident_raw() ws() &(";" / ![_]) {?
                  if s.eq_ignore_ascii_case("checkpoint") {
                      Ok(Statement::Checkpoint)
                  } else {
                      Err("CHECKPOINT")
                  }
              }

        /// `LOAD EXTENSION '<path>'` — the path must be a string literal.
        rule load_extension() -> Statement
            = kw(Keyword::Load) kw(Keyword::Extension) path:string_lit() {
                  Statement::LoadExtension { path }
              }

        /// `name` or `db.name` — a table reference. Three-part names are
        /// rejected as soon as the second dot appears, with a clear error
        /// (this alternative reaches furthest and wins, whatever follows the
        /// dot: `.col`, `.*`, or nothing at all).
        rule table_ref() -> (Option<String>, String)
            = _db:ident() dot() _t:ident() dot() {?
                  Err("three-part names (db.table.col) are not supported")
              }
            / db:ident() dot() name:ident() { (Some(db), name) }
            / name:ident() { (None, name) }

        rule create() -> Statement
            = kw(Keyword::Create) kw(Keyword::Database) ifne:if_not_exists() name:ident() {
                  Statement::CreateDatabase { name, if_not_exists: ifne }
              }
            / kw(Keyword::Create) kw(Keyword::View) t:table_ref() cols:col_name_list()? kw(Keyword::As) query:select() {
                  Statement::CreateView { db: t.0, name: t.1, cols, query: Box::new(query) }
              }
            / kw(Keyword::Create) kw(Keyword::Table) ifne:if_not_exists() t:table_ref() kw(Keyword::As) query:select() {
                  Statement::CreateTableAs { db: t.0, name: t.1, query: Box::new(query), if_not_exists: ifne }
              }
            / kw(Keyword::Create) kw(Keyword::Table) ifne:if_not_exists() t:table_ref() lparen() first:table_column()
              rest:(comma() c:table_column() { c })* rparen() {
                  let mut columns = vec![first];
                  columns.extend(rest);
                  Statement::CreateTable { db: t.0, name: t.1, columns, if_not_exists: ifne }
              }

        rule table_column() -> (String, DataType) = name:ident() ty:type_name() { (name, ty) }

        /// A type name: usually an identifier, but DATE/TIMESTAMP lex as keywords.
        rule type_name() -> DataType
            = kw(Keyword::Date) { DataType::Date }
            / kw(Keyword::Timestamp) { DataType::Timestamp }
            / s:ident() {? DataType::from_sql(&s).ok_or("known column type") }

        rule col_name_list() -> Vec<String>
            = lparen() first:ident() rest:(comma() c:ident() { c })* rparen() {
                  let mut cols = vec![first];
                  cols.extend(rest);
                  cols
              }

        rule insert() -> Statement
            = kw(Keyword::Insert) kw(Keyword::Into) t:table_ref() columns:col_name_list()? source:insert_source() {
                  Statement::Insert { db: t.0, table: t.1, columns, source }
              }

        rule insert_source() -> InsertSource
            = kw(Keyword::Values) rows:insert_rows() { InsertSource::Values(rows) }
            / q:select() { InsertSource::Select(Box::new(q)) }

        rule insert_rows() -> Vec<Vec<Expr>>
            = first:insert_row() rest:(comma() r:insert_row() { r })* {
                  let mut rows = vec![first];
                  rows.extend(rest);
                  rows
              }

        rule insert_row() -> Vec<Expr>
            = lparen() items:expr_list()? rparen() { items.unwrap_or_default() }

        rule update() -> Statement
            = kw(Keyword::Update) t:table_ref() kw(Keyword::Set) first:set_item() rest:(comma() s:set_item() { s })*
              where_:where_clause()? {
                  let mut sets = vec![first];
                  sets.extend(rest);
                  Statement::Update { db: t.0, table: t.1, sets, where_clause: where_ }
              }

        rule set_item() -> (String, Expr) = col:ident() eq() e:expr() { (col, e) }

        rule delete() -> Statement
            = kw(Keyword::Delete) kw(Keyword::From) t:table_ref() where_:where_clause()? {
                  Statement::Delete { db: t.0, table: t.1, where_clause: where_ }
              }

        rule drop() -> Statement
            = kw(Keyword::Drop) kw(Keyword::Database) ife:if_exists() name:ident() {
                  Statement::DropDatabase { name, if_exists: ife }
              }
            / kw(Keyword::Drop) kw(Keyword::Table) ife:if_exists() t:table_ref() {
                  Statement::DropTable { db: t.0, name: t.1, if_exists: ife }
              }
            / kw(Keyword::Drop) kw(Keyword::View) t:table_ref() { Statement::DropView { db: t.0, name: t.1 } }

        rule txn() -> Statement
            = kw(Keyword::Begin) kw(Keyword::Transaction)? { Statement::Begin }
            / kw(Keyword::Commit) kw(Keyword::Transaction)? { Statement::Commit }
            / kw(Keyword::Rollback) kw(Keyword::Transaction)? { Statement::Rollback }

        // ---------- SELECT ----------

        /// A full SELECT including trailing set-operation branches
        /// (`UNION`/`INTERSECT`/`EXCEPT`), in written order.
        rule select() -> Select
            = body:select_body() unions:(u:set_op_branch() { u })* {
                  let mut sel = body;
                  sel.unions = unions;
                  sel
              }

        rule set_op_branch() -> (SetOp, Select)
            = kw(Keyword::Union) all:set_quantifier() branch:select_body() {
                  (if all { SetOp::UnionAll } else { SetOp::Union }, branch)
              }
            / kw(Keyword::Intersect) all:set_quantifier() branch:select_body() {
                  (if all { SetOp::IntersectAll } else { SetOp::Intersect }, branch)
              }
            / kw(Keyword::Except) all:set_quantifier() branch:select_body() {
                  (if all { SetOp::ExceptAll } else { SetOp::Except }, branch)
              }

        /// `[ALL | DISTINCT]` after a set operator (default DISTINCT).
        rule set_quantifier() -> bool
            = kw(Keyword::Distinct) { false }
            / kw_all() { true }
            / { false }

        rule select_body() -> Select
            = with:with_clause()? kw(Keyword::Select) distinct:opt_kw(Keyword::Distinct) projections:projection_list()
              from:from_clause()? where_:where_clause()? group:group_by_clause()? having:having_clause()?
              order:order_by_clause()? limit:limit_clause()? offset:offset_clause()? {
                  Select {
                      with: with.unwrap_or_default(),
                      distinct,
                      projections,
                      from,
                      where_clause: where_,
                      group_by: group.unwrap_or_default(),
                      having,
                      order_by: order.unwrap_or_default(),
                      limit,
                      offset,
                      unions: Vec::new(),
                  }
              }

        rule with_clause() -> Vec<CteDef>
            = kw(Keyword::With) first:cte_def() rest:(comma() c:cte_def() { c })* {
                  let mut ctes = vec![first];
                  ctes.extend(rest);
                  ctes
              }

        rule cte_def() -> CteDef
            = name:ident() cols:col_name_list()? kw(Keyword::As) lparen() query:select() rparen() {
                  CteDef { name, cols, query: Box::new(query) }
              }

        /// `*` / `t.*` are whole-list-only in v1; a mid-list `*` is rejected by
        /// `projection`'s top-level check.
        rule projection_list() -> Vec<Projection>
            = star() !"," { vec![Projection::Star] }
            / t:ident() dot() star() !"," { vec![Projection::QualifiedStar(t)] }
            / first:projection() rest:(comma() p:projection() { p })* {
                  let mut out = vec![first];
                  out.extend(rest);
                  out
              }

        rule projection() -> Projection
            = e:expr() alias:alias()? {?
                  if matches!(&e, Expr::Star | Expr::QualifiedStar(_)) {
                      Err("`*` must be the whole projection list in v1")
                  } else {
                      Ok(Projection::Expr { expr: e, alias })
                  }
              }

        /// `[AS] alias` — a bare alias must be a non-keyword identifier.
        rule alias() -> String
            = kw(Keyword::As) name:ident() { name }
            / name:ident() { name }

        rule from_clause() -> FromClause
            = kw(Keyword::From) source:from_source() alias:alias()? joins:(j:join_clause() { j })* {?
                  if matches!(&source, FromSource::Derived(_)) && alias.is_none() {
                      Err("derived table needs an alias")
                  } else {
                      Ok(FromClause { source, alias, joins })
                  }
              }

        rule from_source() -> FromSource
            = lparen() q:select() rparen() { FromSource::Derived(Box::new(q)) }
            / t:table_ref() { FromSource::Table { db: t.0, name: t.1 } }

        /// One join step: `CROSS JOIN src` (no condition) or
        /// `[INNER|LEFT|RIGHT|FULL [OUTER]] JOIN src ON cond`. Each `ON`
        /// condition binds against every table to its left plus its own
        /// right-hand source (multi-way joins are left-deep).
        rule join_clause() -> JoinClause
            = kw(Keyword::Cross) kw(Keyword::Join) source:from_source() alias:alias()? {?
                  if matches!(&source, FromSource::Derived(_)) && alias.is_none() {
                      Err("derived table needs an alias")
                  } else {
                      Ok(JoinClause { kind: JoinKind::Cross, source, alias, on: None })
                  }
              }
            / kind:join_kind() source:from_source() alias:alias()? kw(Keyword::On) on:expr() {?
                  if matches!(&source, FromSource::Derived(_)) && alias.is_none() {
                      Err("derived table needs an alias")
                  } else {
                      Ok(JoinClause { kind, source, alias, on: Some(on) })
                  }
              }

        rule join_kind() -> JoinKind
            = kw(Keyword::Join) { JoinKind::Inner }
            / kw(Keyword::Inner) kw(Keyword::Join) { JoinKind::Inner }
            / kw(Keyword::Left) kw(Keyword::Outer)? kw(Keyword::Join) { JoinKind::Left }
            / kw(Keyword::Right) kw(Keyword::Outer)? kw(Keyword::Join) { JoinKind::Right }
            / kw(Keyword::Full) kw(Keyword::Outer)? kw(Keyword::Join) { JoinKind::Full }

        rule where_clause() -> Expr = kw(Keyword::Where) e:expr() { e }
        rule group_by_clause() -> Vec<Expr> = kw(Keyword::Group) kw(Keyword::By) l:expr_list() { l }
        rule having_clause() -> Expr = kw(Keyword::Having) e:expr() { e }
        rule order_by_clause() -> Vec<OrderItem> = kw(Keyword::Order) kw(Keyword::By) l:order_list() { l }
        rule limit_clause() -> usize = kw(Keyword::Limit) n:nonneg_integer() { n }
        rule offset_clause() -> usize = kw(Keyword::Offset) n:nonneg_integer() { n }

        rule expr_list() -> Vec<Expr>
            = first:expr() rest:(comma() e:expr() { e })* {
                  let mut out = vec![first];
                  out.extend(rest);
                  out
              }

        rule order_list() -> Vec<OrderItem>
            = first:order_item() rest:(comma() i:order_item() { i })* {
                  let mut out = vec![first];
                  out.extend(rest);
                  out
              }

        rule order_item() -> OrderItem
            = e:expr() desc:order_direction() nulls:nulls_order() {
                  OrderItem { expr: e, desc, nulls_first: nulls }
              }

        rule order_direction() -> bool
            = kw(Keyword::Desc) { true }
            / kw(Keyword::Asc) { false }
            / { false }

        /// `NULLS FIRST` / `NULLS LAST` (a non-reserved two-word suffix).
        rule nulls_order() -> Option<bool>
            = word("nulls") word("first") { Some(true) }
            / word("nulls") word("last") { Some(false) }
            / { None }

        // ---------- expressions ----------

        rule expr() -> Expr = or_expr()

        rule or_expr() -> Expr
            = first:and_expr() rest:(kw(Keyword::Or) r:and_expr() { r })* {
                  rest.into_iter().fold(first, |l, r| Expr::Binary {
                      op: BinaryOp::Or,
                      left: Box::new(l),
                      right: Box::new(r),
                  })
              }

        rule and_expr() -> Expr
            = first:not_expr() rest:(kw(Keyword::And) r:not_expr() { r })* {
                  rest.into_iter().fold(first, |l, r| Expr::Binary {
                      op: BinaryOp::And,
                      left: Box::new(l),
                      right: Box::new(r),
                  })
              }

        rule not_expr() -> Expr
            = kw(Keyword::Not) e:not_expr() { Expr::Unary { op: UnaryOp::Not, expr: Box::new(e) } }
            / comparison()

        rule comparison() -> Expr
            = l:concat_expr() tail:comparison_tail()? {
                  match tail {
                      None => l,
                      Some(t) => apply_cmp(l, t),
                  }
              }

        /// `a || b || c` — DuckDB/PG precedence: `||` binds tighter than
        /// comparisons but looser than `+`/`-` (`1 + 2 || '3'` ⇒ `'33'`).
        rule concat_expr() -> Expr
            = first:additive() rest:(concat_op() r:additive() { r })* {
                  rest.into_iter().fold(first, |l, r| Expr::Binary {
                      op: BinaryOp::Concat,
                      left: Box::new(l),
                      right: Box::new(r),
                  })
              }

        rule concat_op() = "||" ws()

        rule comparison_tail() -> CmpTail
            = kw(Keyword::Is) neg:opt_kw(Keyword::Not) kw(Keyword::Null) { CmpTail::IsNull { negated: neg } }
            / neg:not_flag() kw(Keyword::Between) low:additive() kw(Keyword::And) high:additive() {
                  CmpTail::Between { negated: neg, low, high }
              }
            / neg:not_flag() kw(Keyword::In) lparen() t:in_tail(neg) { t }
            / neg:not_flag() kw(Keyword::Like) right:additive() { CmpTail::Like { negated: neg, right } }
            / op:cmp_op() right:additive() { CmpTail::Binary { op, right } }

        rule in_tail(negated: bool) -> CmpTail
            = q:select() rparen() { CmpTail::InSubquery { negated, query: Box::new(q) } }
            / items:expr_list()? rparen() { CmpTail::InList { negated, items: items.unwrap_or_default() } }

        rule not_flag() -> bool
            = kw(Keyword::Not) { true }
            / { false }

        rule additive() -> Expr
            = first:multiplicative() rest:(op:add_op() r:multiplicative() { (op, r) })* {
                  rest.into_iter().fold(first, |l, (op, r)| Expr::Binary {
                      op,
                      left: Box::new(l),
                      right: Box::new(r),
                  })
              }

        rule multiplicative() -> Expr
            = first:unary() rest:(op:mul_op() r:unary() { (op, r) })* {
                  rest.into_iter().fold(first, |l, (op, r)| Expr::Binary {
                      op,
                      left: Box::new(l),
                      right: Box::new(r),
                  })
              }

        rule unary() -> Expr
            = minus() e:unary() { Expr::Unary { op: UnaryOp::Negate, expr: Box::new(e) } }
            / plus() e:unary() { Expr::Unary { op: UnaryOp::Plus, expr: Box::new(e) } }
            / primary()

        rule primary() -> Expr
            = v:number() { Expr::Literal(v) }
            / s:string_lit() { Expr::Literal(Value::Varchar(s)) }
            / kw(Keyword::True) { Expr::Literal(Value::Boolean(true)) }
            / kw(Keyword::False) { Expr::Literal(Value::Boolean(false)) }
            / kw(Keyword::Null) { Expr::Literal(Value::Null) }
            / date_literal()
            / timestamp_literal()
            / parameter()
            / cast_expr()
            / case_expr()
            / star() { Expr::Star }
            / exists_expr()
            / extract_call()
            / lparen() q:select() rparen() { Expr::ScalarSubquery(Box::new(q)) }
            / lparen() e:expr() rparen() { e }
            / ident_expr()

        /// `?` — a positional prepared-statement parameter. The payload is the
        /// placeholder's byte offset in the input (see [`Expr::Parameter`]);
        /// [`super::parse_prepared`] renumbers the offsets into 0-based ranks.
        ///
        /// `?` is not a token anywhere else in the grammar, so this rule
        /// cannot shadow existing syntax.
        rule parameter() -> Expr
            = p:position!() "?" ws() { Expr::Parameter(p) }

        /// `EXTRACT(field FROM date_or_timestamp)` — desugars to the
        /// `extract('field', value)` scalar (field may also be a string
        /// literal). Matched as a bare word so `extract` stays usable as a
        /// column/table name everywhere else.
        rule extract_call() -> Expr
            = extract_name() lparen() part:extract_part() kw(Keyword::From) e:expr() rparen() {
                  Expr::Function {
                      name: "extract".to_string(),
                      args: vec![Expr::Literal(Value::Varchar(part)), e],
                      distinct: false,
                  }
              }

        rule extract_name()
            = s:ident_raw() ws() {? if s.eq_ignore_ascii_case("extract") { Ok(()) } else { Err("EXTRACT") } }

        /// A field name (`year`, `dow`, ...) or a string literal.
        rule extract_part() -> String
            = s:string_lit() { s }
            / i:ident() { i }

        rule date_literal() -> Expr
            = kw(Keyword::Date) s:string_lit() {?
                  crate::types::parse_date(&s)
                      .map(|d| Expr::Literal(Value::Date(d)))
                      .ok_or("valid DATE literal")
              }

        rule timestamp_literal() -> Expr
            = kw(Keyword::Timestamp) s:string_lit() {?
                  crate::types::parse_timestamp(&s)
                      .map(|t| Expr::Literal(Value::Timestamp(t)))
                      .ok_or("valid TIMESTAMP literal")
              }

        rule cast_expr() -> Expr
            = kw(Keyword::Cast) lparen() e:expr() kw(Keyword::As) ty:type_name() rparen() {
                  Expr::Cast { expr: Box::new(e), dtype: ty }
              }

        rule case_expr() -> Expr
            = kw(Keyword::Case) operand:case_operand() branches:case_branches() else_:case_else()? kw(Keyword::End) {
                  Expr::Case { branches: desugar_case(operand, branches), else_ }
              }

        /// Optional simple-case operand: absent when `WHEN` follows directly.
        rule case_operand() -> Option<Expr>
            = !kw(Keyword::When) e:expr() { Some(e) }
            / { None }

        rule case_branches() -> Vec<(Expr, Expr)>
            = first:case_when() rest:(b:case_when() { b })* {
                  let mut out = vec![first];
                  out.extend(rest);
                  out
              }

        rule case_when() -> (Expr, Expr)
            = kw(Keyword::When) cond:expr() kw(Keyword::Then) val:expr() { (cond, val) }

        rule case_else() -> Box<Expr> = kw(Keyword::Else) e:expr() { Box::new(e) }

        rule exists_expr() -> Expr
            = kw(Keyword::Exists) lparen() q:select() rparen() {
                  Expr::Exists { query: Box::new(q), negated: false }
              }

        /// Function calls (`COALESCE` is n-ary sugar, not a ScalarFunc) and
        /// `t.col` / `t.*` / bare column references.
        ///
        /// The function name decides the form *before* the argument list is
        /// parsed, so no alternative parses `fn_args` twice (same-prefix
        /// alternatives that both parse args re-parse nested calls
        /// exponentially). COALESCE parses a plain expression list, so `*` is
        /// an ordinary expression there (`COALESCE(*, 1)` parses and the
        /// binder rejects it), while a generic call treats a leading `*` as
        /// the whole argument list (`f(*, 1)` is a parse error).
        rule ident_expr() -> Expr
            = coalesce_call()
            / fn_call()
            / three_part_column()
            / name:ident() dot() col:ident() { Expr::Column { table: Some(name), name: col } }
            / name:ident() dot() star() { Expr::QualifiedStar(Some(name)) }
            / name:ident() { Expr::Column { table: None, name } }

        /// `db.table.col` / `db.table.*` are not supported: fail with a clear
        /// message. This alternative consumes one token further than the
        /// valid two-part forms, so its error is the one that surfaces.
        rule three_part_column() -> Expr
            = _db:ident() dot() _t:ident() dot() _c:ident() {?
                  Err("three-part column references (db.table.col) are not supported")
              }
            / _db:ident() dot() _t:ident() dot() star() {?
                  Err("three-part references (db.table.*) are not supported")
              }

        rule coalesce_call() -> Expr
            = name:coalesce_name() lparen() args:coalesce_args() rparen() {?
                  if args.is_empty() {
                      Err("COALESCE needs at least one argument")
                  } else {
                      Ok(Expr::Coalesce(args))
                  }
              }

        /// The function name iff it is COALESCE (case-insensitive); failing
        /// here keeps `fn_call` from ever re-parsing COALESCE arguments.
        rule coalesce_name() -> String
            = s:ident() {? if s.eq_ignore_ascii_case("coalesce") { Ok(s) } else { Err("COALESCE") } }

        /// Any function name but COALESCE.
        rule non_coalesce_name() -> String
            = s:ident() {? if s.eq_ignore_ascii_case("coalesce") { Err("non-COALESCE function name") } else { Ok(s) } }

        rule fn_call() -> Expr
            = name:non_coalesce_name() lparen() distinct:distinct_flag() args:fn_args() rparen() over:over_clause()? {
                  match over {
                      Some(over) => Expr::Window { name, args, over, distinct },
                      None => Expr::Function { name, args, distinct },
                  }
              }

        /// `DISTINCT` before a function call's first argument.
        rule distinct_flag() -> bool
            = kw(Keyword::Distinct) { true }
            / { false }

        /// COALESCE arguments: a plain expression list, so `*` may appear as
        /// an expression anywhere in it (unlike `fn_args`).
        rule coalesce_args() -> Vec<Expr>
            = args:expr_list() { args }
            / { vec![] }

        /// `f(*)`, `f(a, b)` or `f()`; a leading `*` is the whole argument list.
        rule fn_args() -> Vec<Expr>
            = star() { vec![Expr::Star] }
            / args:expr_list() { args }
            / { vec![] }

        // ---------- window clauses ----------

        rule over_clause() -> OverClause
            = kw(Keyword::Over) lparen() partition:partition_clause()? order:order_by_clause()? frame:frame_clause()?
              rparen() {
                  OverClause {
                      partition: partition.unwrap_or_default(),
                      order: order.unwrap_or_default(),
                      frame,
                  }
              }

        rule partition_clause() -> Vec<Expr> = kw(Keyword::Partition) kw(Keyword::By) l:expr_list() { l }

        rule frame_clause() -> FrameSpec
            = kw(Keyword::Rows) s:frame_spec(FrameMode::Rows) { s }
            / kw(Keyword::Row) s:frame_spec(FrameMode::Rows) { s }
            / kw(Keyword::Range) s:frame_spec(FrameMode::Range) { s }

        rule frame_spec(mode: FrameMode) -> FrameSpec
            = kw(Keyword::Between) start:frame_bound() kw(Keyword::And) end:frame_bound() { FrameSpec { mode, start, end } }
            / kw(Keyword::Current) kw(Keyword::Row) {
                  FrameSpec { mode, start: FrameBound::CurrentRow, end: FrameBound::CurrentRow }
              }
            / kw(Keyword::Unbounded) kw(Keyword::Preceding) {
                  FrameSpec { mode, start: FrameBound::UnboundedPreceding, end: FrameBound::CurrentRow }
              }
            / kw(Keyword::Unbounded) kw(Keyword::Following) {
                  FrameSpec {
                      mode,
                      start: FrameBound::UnboundedFollowing,
                      end: FrameBound::UnboundedFollowing,
                  }
              }
            / n:nonneg_integer() kw(Keyword::Preceding) {
                  FrameSpec { mode, start: FrameBound::Preceding(n), end: FrameBound::CurrentRow }
              }
            / n:nonneg_integer() kw(Keyword::Following) {
                  FrameSpec { mode, start: FrameBound::CurrentRow, end: FrameBound::Following(n) }
              }

        rule frame_bound() -> FrameBound
            = kw(Keyword::Unbounded) kw(Keyword::Preceding) { FrameBound::UnboundedPreceding }
            / kw(Keyword::Unbounded) kw(Keyword::Following) { FrameBound::UnboundedFollowing }
            / kw(Keyword::Current) kw(Keyword::Row) { FrameBound::CurrentRow }
            / n:nonneg_integer() kw(Keyword::Preceding) { FrameBound::Preceding(n) }
            / n:nonneg_integer() kw(Keyword::Following) { FrameBound::Following(n) }
    }
}

#[cfg(test)]
mod tests {
    use super::super::ast::*;
    use super::super::MAX_NESTING_DEPTH;
    use super::parse_statements as parse;
    use crate::types::{PinkDbError, Value};

    fn parse_one(sql: &str) -> Statement {
        let stmts = parse(sql).unwrap_or_else(|e| panic!("rejected {sql:?}: {e}"));
        assert_eq!(stmts.len(), 1, "expected one statement in {sql:?}");
        stmts.into_iter().next().unwrap()
    }

    /// The first projection of a parsed `SELECT`, unwrapped to its `Expr`.
    fn projection_expr(stmt: Statement) -> Expr {
        let Statement::Select(sel) = stmt else {
            panic!("not a SELECT: {stmt:?}");
        };
        match sel.projections.into_iter().next().unwrap() {
            Projection::Expr { expr, .. } => expr,
            other => panic!("not an expression projection: {other:?}"),
        }
    }

    /// The first projection of a parsed `SELECT` that must be a literal.
    fn lit(sql: &str) -> Value {
        match projection_expr(parse_one(sql)) {
            Expr::Literal(v) => v,
            other => panic!("not a literal: {other:?}"),
        }
    }

    /// Corpus of valid SQL, curated from three sources:
    /// 1. every SQL string in the repo's tests, examples and benches
    ///    (`crates/**/*.rs`, `bench/duckdiff.py` queries);
    /// 2. hand-written edge cases covering lexical rules (keywords, quoting,
    ///    escapes, numbers, comments), expression precedence/associativity,
    ///    joins, unions, CTEs, derived tables, subqueries, windows and
    ///    CASE/CAST/COALESCE/LIKE/BETWEEN/IN/EXISTS;
    /// 3. multi-database and `LOAD EXTENSION` forms.
    ///
    /// 812 valid + 157 invalid entries.
    /// Every valid entry must parse (and split into the same statement count).
    const VALID: &[&str] = &[
        "SELECT COUNT(*) FROM sales;",
        "SELECT COUNT(*) FROM sales WHERE total > 500;",
        "SELECT COUNT(*) FROM sales WHERE region = 'west';",
        "SELECT COUNT(*) FROM (SELECT upper(region), length(region) FROM sales) AS s;",
        "SELECT region, COUNT(*), AVG(total), SUM(total), MIN(total), MAX(total) FROM sales GROUP BY region;",
        "SELECT COUNT(*) FROM (SELECT id FROM sales GROUP BY id) AS s;",
        "SELECT COUNT(*) FROM (SELECT id, total FROM sales ORDER BY total DESC LIMIT 10) AS s;",
        "SELECT COUNT(*) FROM (SELECT region FROM sales ORDER BY region) AS s;",
        "SELECT COUNT(*) FROM (SELECT DISTINCT region, active FROM sales) AS s;",
        "SELECT COUNT(*) FROM (SELECT DISTINCT id FROM sales) AS s;",
        "SELECT COUNT(*) FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) AS rn FROM sales) AS s;",
        "SELECT COUNT(*) FROM (SELECT id, COUNT(*) OVER (PARTITION BY region) AS c FROM sales) AS s;",
        "SELECT COUNT(*) FROM (SELECT a.region FROM sales a JOIN sales b ON a.id = b.id) AS s;",
        "SELECT COUNT(*) FROM (SELECT a.region FROM sales a JOIN sales b ON a.id = b.id GROUP BY a.region) AS s;",
        "SELECT COUNT(*) FROM (SELECT a.id, b.total FROM sales a JOIN sales b ON a.id = b.id WHERE a.total > b.total) AS s;",
        "SELECT COUNT(*) FROM (SELECT s.id FROM sales s JOIN (SELECT DISTINCT region FROM sales) r ON s.region = r.region) AS s;",
        "CREATE TABLE t (a INTEGER, b VARCHAR);",
        "INSERT INTO t VALUES (1, 'x'), (2, 'y');",
        "CREATE TABLE u (a INTEGER);",
        "INSERT INTO u VALUES (10);",
        "SELECT a, b FROM t ORDER BY a;",
        "BEGIN;",
        "INSERT INTO t VALUES (3, 'z');",
        "COMMIT;",
        "INSERT INTO u VALUES (20);",
        "SELECT COUNT(*) FROM t;",
        "SELECT COUNT(*) FROM u;",
        "INSERT INTO t VALUES (4, 'w');",
        "SELECT a FROM t ORDER BY a;",
        "ROLLBACK;",
        "SELECT id FROM sales WHERE total > 5 LIMIT 10;",
        "SELECT id FROM sales WHERE total > 0;",
        "SELECT id FROM sales WHERE total IS NULL;",
        "SELECT region, AVG(total) AS a FROM sales GROUP BY region HAVING COUNT(*) > 1 ORDER BY 2 DESC;",
        "CREATE TABLE g (k INTEGER, v INTEGER);",
        "SELECT k, COUNT(*) FROM g GROUP BY k;",
        "CREATE TABLE nn (k INTEGER);",
        "INSERT INTO nn VALUES (1), (NULL), (2), (NULL);",
        "SELECT k, COUNT(*) FROM nn GROUP BY k;",
        "SELECT COUNT(*) FROM sales WHERE false;",
        "SELECT DISTINCT region FROM sales ORDER BY region;",
        "SELECT total FROM sales ORDER BY total;",
        "SELECT total FROM sales ORDER BY total DESC;",
        "SELECT 1 / 2, upper(region), length(region) FROM sales LIMIT 1;",
        "CREATE TABLE t (a BIGINT, b VARCHAR);",
        "INSERT INTO t VALUES (1, 'x'), (2, 'yy');",
        "SELECT a, b FROM t ORDER BY a DESC;",
        "SELECT 1 + 2, upper('ab');",
        "SELECT id / 0 FROM sales;",
        "CREATE TABLE t (x INTEGER);",
        "INSERT INTO t VALUES (1), (2);",
        "SELECT SUM(x) + (1+1) FROM t;",
        "SELECT 1+1, SUM(x) FROM t;",
        "SELECT SUM(x) + abs(1) FROM t;",
        "SELECT SUM(x) FROM t HAVING SUM(x) > 1+1;",
        "INSERT INTO t VALUES (1);",
        "SELECT x AS x FROM t;",
        "CREATE TABLE u (a INTEGER, b INTEGER);",
        "INSERT INTO u VALUES (1, 100), (2, 0);",
        "SELECT a AS b FROM u WHERE b > 50;",
        "CREATE TABLE e (x INTEGER);",
        "INSERT INTO e VALUES (NULL), (NULL);",
        "SELECT SUM(x) FROM e;",
        "SELECT SUM(x) FROM e WHERE false;",
        "SELECT SUM(x + 1) FROM e;",
        "SELECT SUM(x) FROM e WHERE x IS NULL;",
        "SELECT AVG(x) FROM e;",
        "SELECT MIN(x) FROM e;",
        "SELECT MAX(x) FROM e;",
        "SELECT COUNT(x) FROM e;",
        "SELECT COUNT(*) FROM e;",
        "SELECT SUM(c) FROM (SELECT x, COUNT(*) AS c FROM e GROUP BY x) s;",
        "CREATE TABLE a (x INTEGER);",
        "CREATE TABLE b (x BIGINT);",
        "CREATE TABLE c (x DOUBLE);",
        "INSERT INTO a VALUES (1), (2);",
        "INSERT INTO b VALUES (1), (3);",
        "INSERT INTO c VALUES (1.0), (4.0);",
        "SELECT * FROM a JOIN b ON a.x = b.x;",
        "SELECT * FROM a JOIN c ON a.x = c.x;",
        "CREATE TABLE d (v DOUBLE);",
        "INSERT INTO d VALUES (-0.0), (0.0), (1.0);",
        "SELECT v, COUNT(*) FROM d GROUP BY v;",
        "SELECT DISTINCT v FROM d;",
        "SELECT COUNT(*) FROM d a JOIN d b ON a.v = b.v;",
        "SELECT COUNT(*) FROM d a JOIN d b ON a.v = b.v WHERE a.v = 0.0;",
        "CREATE TABLE n (v DOUBLE);",
        "INSERT INTO n VALUES (0.0 / 0.0), (-0.0 / 0.0);",
        "SELECT DISTINCT v FROM n;",
        "CREATE TABLE t1 (id INTEGER);",
        "CREATE TABLE t2 (id INTEGER, rn INTEGER);",
        "INSERT INTO t1 VALUES (1);",
        "SELECT rn FROM t1 JOIN t2 ON t1.id = t2.id;",
        "CREATE TABLE l (k INTEGER, lv VARCHAR);",
        "CREATE TABLE r (k INTEGER, rv VARCHAR);",
        "INSERT INTO l VALUES (1, 'a'), (2, 'b'), (1, 'c');",
        "INSERT INTO r VALUES (1, 'x'), (2, 'y'), (1, 'z');",
        "SELECT lv, rv FROM l JOIN r ON l.k = r.k;",
        "CREATE TABLE ig (k INTEGER, lv INTEGER);",
        "CREATE TABLE bg (k BIGINT);",
        "CREATE TABLE dg (k DOUBLE);",
        "CREATE TABLE sv (k VARCHAR, rn INTEGER);",
        "CREATE TABLE s (k VARCHAR);",
        "SELECT COUNT(*) FROM ig JOIN bg ON ig.k = bg.k;",
        "SELECT COUNT(*) FROM ig JOIN dg ON ig.k = dg.k;",
        "INSERT INTO sv VALUES ('x', 1), ('y', 2), (NULL, 3);",
        "INSERT INTO s VALUES ('x'), ('x'), ('y'), (NULL);",
        "SELECT rn FROM sv JOIN s ON sv.k = s.k;",
        "CREATE TABLE l (k INTEGER, lv INTEGER);",
        "CREATE TABLE r (k INTEGER, rv INTEGER);",
        "CREATE TABLE l (k BIGINT, lv INTEGER);",
        "CREATE TABLE r (k BIGINT, rv INTEGER);",
        "INSERT INTO l VALUES\n             (-9223372036854775807 - 1, 0), (-5, 1), (0, 2), (7, 3), (7, 4),\n             (9223372036854775807, 5), (NULL, 6);",
        "INSERT INTO r VALUES\n             (7, 10), (-5, 11), (NULL, 12), (9223372036854775807, 13),\n             (-9223372036854775807 - 1, 14), (7, 15);",
        "SELECT COUNT(*) FROM l JOIN r ON l.k = r.k;",
        "CREATE TABLE a (k INTEGER, lv INTEGER);",
        "CREATE TABLE b (k BIGINT, rv INTEGER);",
        "INSERT INTO a VALUES (1, 1), (-1, 2), (NULL, 3), (2147483647, 4);",
        "INSERT INTO b VALUES (1, 10), (-1, 11), (2147483648, 12),\n             (4294967297, 13), (2147483647, 14);",
        "SELECT lv, rv FROM a JOIN b ON a.k = b.k;",
        "CREATE TABLE l (ts TIMESTAMP, lv INTEGER);",
        "CREATE TABLE r (ts TIMESTAMP, rv INTEGER);",
        "INSERT INTO l VALUES\n             (TIMESTAMP '2024-01-15 10:00:00', 1),\n             (TIMESTAMP '2024-01-15 10:00:00.000001', 2),\n             (TIMESTAMP '1969-12-31 23:59:59.999999', 3),\n             (NULL, 4);",
        "INSERT INTO r VALUES\n             (TIMESTAMP '2024-01-15 10:00:00.000001', 10),\n             (TIMESTAMP '2024-01-15 10:00:00', 11),\n             (TIMESTAMP '1969-12-31 23:59:59.999999', 12),\n             (NULL, 13);",
        "SELECT lv, rv FROM l JOIN r ON l.ts = r.ts;",
        "CREATE TABLE l (k BIGINT);",
        "CREATE TABLE r (k BIGINT);",
        "INSERT INTO l VALUES (1), (NULL), (2);",
        "CREATE TABLE ls (k VARCHAR);",
        "CREATE TABLE rs (k VARCHAR);",
        "INSERT INTO ls VALUES ('a'), (NULL);",
        "SELECT COUNT(*) FROM ls JOIN rs ON ls.k = rs.k;",
        "CREATE TABLE r (k BIGINT, rn INTEGER);",
        "INSERT INTO l VALUES (1);",
        "SELECT rn FROM l JOIN r ON l.k = r.k;",
        "INSERT INTO l VALUES (1, 10), (2, 20), (NULL, 30), (4, 40);",
        "INSERT INTO r VALUES (1, 100), (2, NULL), (NULL, 300), (5, 500);",
        "SELECT l.k, lv, r.k, rv FROM l LEFT JOIN r ON l.k = r.k;",
        "SELECT l.k, lv, r.k, rv FROM l LEFT OUTER JOIN r ON l.k = r.k;",
        "SELECT * FROM l RIGHT OUTER JOIN r ON l.k = r.k;",
        "CREATE TABLE e (k INTEGER, ev INTEGER);",
        "INSERT INTO l VALUES (1, 10);",
        "INSERT INTO r VALUES (1, 20);",
        "SELECT lv, ev FROM l LEFT JOIN e ON l.k = e.k;",
        "SELECT COUNT(*) FROM l RIGHT JOIN e ON l.k = e.k;",
        "SELECT COUNT(*) FROM e LEFT JOIN r ON e.k = r.k;",
        "SELECT ev, rv FROM e RIGHT JOIN r ON e.k = r.k;",
        "SELECT COUNT(*) FROM e LEFT JOIN e e2 ON e.k = e2.k;",
        "SELECT COUNT(*) FROM e RIGHT JOIN e e2 ON e.k = e2.k;",
        "INSERT INTO l VALUES (1, 10), (2, 20);",
        "INSERT INTO r VALUES (3, 30), (4, 40);",
        "SELECT lv, rv FROM l LEFT JOIN r ON l.k = r.k;",
        "SELECT lv, rv FROM l RIGHT JOIN r ON l.k = r.k;",
        "SELECT COUNT(*), COUNT(r.rv) FROM l LEFT JOIN r ON l.k = r.k;",
        "SELECT l.k, r.k FROM l LEFT JOIN r ON l.k = r.k;",
        "SELECT COUNT(*), COUNT(l.lv) FROM l RIGHT JOIN r ON l.k = r.k;",
        "SELECT l.k, r.k FROM l RIGHT JOIN r ON l.k = r.k;",
        "SELECT rn FROM t1 LEFT JOIN t2 ON t1.id = t2.id;",
        "CREATE TABLE t3 (id INTEGER, rn INTEGER);",
        "CREATE TABLE t4 (id INTEGER);",
        "INSERT INTO t4 VALUES (1);",
        "SELECT t3.rn FROM t4 RIGHT JOIN t3 ON t4.id = t3.id;",
        "CREATE TABLE a (x INTEGER, av INTEGER);",
        "CREATE TABLE b (x BIGINT, bv INTEGER);",
        "INSERT INTO a VALUES (1, 10), (2, 20);",
        "INSERT INTO b VALUES (2, 200), (3, 300);",
        "SELECT * FROM a LEFT JOIN b ON a.x = b.x;",
        "SELECT * FROM a RIGHT JOIN b ON a.x = b.x;",
        "CREATE TABLE ls (k VARCHAR, lv INTEGER);",
        "CREATE TABLE rs (k VARCHAR, rv INTEGER);",
        "INSERT INTO ls VALUES ('a', 1), ('b', 2), (NULL, 3);",
        "INSERT INTO rs VALUES ('b', 20), ('c', 30), (NULL, 40);",
        "SELECT ls.k, lv, rs.k, rv FROM ls LEFT JOIN rs ON ls.k = rs.k;",
        "SELECT rv, lv FROM ls RIGHT JOIN rs ON ls.k = rs.k;",
        "SELECT lv, rv FROM l LEFT JOIN r ON l.k = r.k WHERE r.rv > 50;",
        "SELECT lv, rv FROM l LEFT JOIN r ON l.k = r.k WHERE r.rv IS NULL;",
        "SELECT lv, rv FROM l LEFT JOIN r ON l.k = r.k WHERE l.lv > 15;",
        "SELECT lv, rv FROM l RIGHT JOIN r ON l.k = r.k WHERE r.rv > 50;",
        "SELECT lv, rv FROM l RIGHT JOIN r ON l.k = r.k WHERE l.lv IS NULL;",
        "CREATE TABLE l (k INTEGER, b BOOLEAN, d DOUBLE, dt DATE, ts TIMESTAMP);",
        "INSERT INTO l VALUES\n             (1, true, 1.5, DATE '2024-01-15', TIMESTAMP '2024-01-15 10:00:00.000001');",
        "CREATE TABLE e (k INTEGER, b BOOLEAN, d DOUBLE, dt DATE, ts TIMESTAMP);",
        "SELECT * FROM l;",
        "SELECT * FROM l LEFT JOIN e ON l.k = e.k;",
        "SELECT COUNT(e.b), COUNT(e.d), COUNT(e.dt), COUNT(e.ts)\n                 FROM l LEFT JOIN e ON l.k = e.k;",
        "SELECT * FROM e RIGHT JOIN l ON e.k = l.k;",
        "CREATE TABLE lb (k BOOLEAN, lv INTEGER);",
        "CREATE TABLE rb (k BOOLEAN, rv INTEGER);",
        "INSERT INTO lb VALUES (true, 1), (false, 2), (NULL, 3);",
        "INSERT INTO rb VALUES (true, 10), (NULL, 11), (false, 12);",
        "SELECT lb.k, lv, rb.k, rv FROM lb LEFT JOIN rb ON lb.k = rb.k;",
        "SELECT lb.k, lv, rb.k, rv FROM lb RIGHT JOIN rb ON lb.k = rb.k;",
        "CREATE TABLE ld (k DATE, lv INTEGER);",
        "CREATE TABLE rd (k DATE, rv INTEGER);",
        "INSERT INTO ld VALUES (DATE '2024-01-15', 1), (DATE '2024-02-20', 2), (NULL, 3);",
        "INSERT INTO rd VALUES (DATE '2024-01-15', 10), (DATE '2024-03-01', 11), (NULL, 12);",
        "SELECT k FROM ld WHERE lv = 1;",
        "SELECT k FROM ld WHERE lv = 2;",
        "SELECT k FROM rd WHERE rv = 11;",
        "SELECT ld.k, lv, rd.k, rv FROM ld LEFT JOIN rd ON ld.k = rd.k;",
        "SELECT ld.k, lv, rd.k, rv FROM ld RIGHT JOIN rd ON ld.k = rd.k;",
        "CREATE TABLE lt (k TIMESTAMP, lv INTEGER);",
        "CREATE TABLE rt (k TIMESTAMP, rv INTEGER);",
        "INSERT INTO lt VALUES\n             (TIMESTAMP '2024-01-15 10:00:00.000001', 1),\n             (TIMESTAMP '2024-01-15 10:00:00', 2), (NULL, 3);",
        "INSERT INTO rt VALUES\n             (TIMESTAMP '2024-01-15 10:00:00', 10),\n             (TIMESTAMP '2024-01-15 10:00:00.000001', 11), (NULL, 12);",
        "SELECT k FROM lt WHERE lv = 1;",
        "SELECT k FROM lt WHERE lv = 2;",
        "SELECT lt.k, lv, rt.k, rv FROM lt LEFT JOIN rt ON lt.k = rt.k;",
        "SELECT lt.k, lv, rt.k, rv FROM lt RIGHT JOIN rt ON lt.k = rt.k;",
        "CREATE TABLE li (k INTEGER, lv INTEGER);",
        "CREATE TABLE rdd (k DOUBLE, rv INTEGER);",
        "INSERT INTO li VALUES (1, 10), (2, 20), (NULL, 30);",
        "INSERT INTO rdd VALUES (1.0, 100), (1.5, 150), (2.0, 200), (NULL, 300);",
        "SELECT li.k, lv, rdd.k, rv FROM li LEFT JOIN rdd ON li.k = rdd.k;",
        "SELECT li.k, lv, rdd.k, rv FROM li RIGHT JOIN rdd ON li.k = rdd.k;",
        "INSERT INTO r VALUES (1, 100), (8, 800), (1, 101), (9, 900);",
        "INSERT INTO l VALUES (1, 100), (8, 800), (1, 101), (9, 900);",
        "CREATE TABLE e (k INTEGER, ev VARCHAR);",
        "CREATE TABLE r (k INTEGER, rv DOUBLE);",
        "INSERT INTO r VALUES (1, 1.5), (2, 2.5);",
        "SELECT * FROM e RIGHT JOIN r ON e.k = r.k;",
        "CREATE TABLE \"left\" (\"right\" INTEGER, x VARCHAR);",
        "INSERT INTO \"left\" VALUES (1, 'a'), (2, 'b');",
        "SELECT \"right\" FROM \"left\" ORDER BY 1;",
        "SELECT x FROM \"left\" AS \"right\" ORDER BY x;",
        "SELECT 1 AS \"left\";",
        "INSERT INTO r VALUES (1, 100), (1, 25), (2, NULL), (NULL, 300), (5, 500);",
        "SELECT COUNT(*), COUNT(r.rv), SUM(r.rv), AVG(r.rv), MIN(r.rv), MAX(r.rv)\n                 FROM l LEFT JOIN r ON l.k = r.k;",
        "SELECT r.rv, COUNT(*), COUNT(l.lv)\n                 FROM l LEFT JOIN r ON l.k = r.k GROUP BY r.rv ORDER BY r.rv;",
        "SELECT COUNT(*), COUNT(r.rv), SUM(r.rv), MIN(r.rv), MAX(r.rv)\n                 FROM l LEFT JOIN r ON l.k = r.k WHERE false;",
        "SELECT COUNT(*), COUNT(r.rv), SUM(r.rv), MIN(r.rv), MAX(r.rv)\n                 FROM (SELECT * FROM l WHERE k > 100) x LEFT JOIN r ON x.k = r.k;",
        "SELECT COUNT(*) FROM fa LEFT JOIN fb ON fa.id = fb.id;",
        "SELECT fa.region, COUNT(*), SUM(fb.total) FROM fa LEFT JOIN fb ON fa.id = fb.id GROUP BY fa.region;",
        "SELECT COUNT(*) FROM fa RIGHT JOIN fb ON fa.id = fb.id;",
        "SELECT COUNT(fb.total), SUM(fb.total) FROM fa LEFT JOIN fb ON fa.id = fb.id;",
        "CREATE TABLE p (a INTEGER);",
        "INSERT INTO p VALUES (1);",
        "INSERT INTO p VALUES (2), ('bad');",
        "SELECT * FROM p;",
        "CREATE TABLE ev (id INTEGER, d DATE, ts TIMESTAMP);",
        "INSERT INTO ev VALUES\n             (1, DATE '2024-01-15', TIMESTAMP '2024-01-15 10:00:00'),\n             (2, DATE '2024-02-20', TIMESTAMP '2024-02-20 23:59:59.5'),\n             (3, NULL, NULL);",
        "SELECT d, ts FROM ev ORDER BY id;",
        "SELECT d + 10, 1 + d, d - 5 FROM ev WHERE id = 1;",
        "SELECT DATE '2024-02-20' - DATE '2024-01-15';",
        "SELECT d + 1 FROM ev WHERE id = 3;",
        "SELECT ts + 1 FROM ev;",
        "SELECT d + d FROM ev;",
        "SELECT * FROM ev WHERE d = ts;",
        "SELECT * FROM ev WHERE d = '2024-01-15';",
        "SELECT DATE '1970-01-01' + 2147483647;",
        "SELECT CAST(DATE '1970-01-01' + 2147483647 AS TIMESTAMP);",
        "SELECT year(d), month(d), day(d), hour(ts), minute(ts), second(ts) FROM ev WHERE id = 2;",
        "SELECT date_trunc('month', d), date_trunc('day', ts) FROM ev WHERE id = 1;",
        "SELECT date_trunc('year', TIMESTAMP '2024-05-06 07:08:09');",
        "SELECT hour(d) FROM ev;",
        "SELECT date_trunc('century', d) FROM ev;",
        "SELECT SUM(d) FROM ev;",
        "SELECT MIN(d), MAX(d), COUNT(d) FROM ev;",
        "SELECT date_trunc('month', d) AS m, COUNT(*) FROM ev GROUP BY m ORDER BY m;",
        "CREATE TABLE h (d DATE, note VARCHAR);",
        "INSERT INTO h VALUES (DATE '2024-01-15', 'x');",
        "SELECT note FROM ev JOIN h ON ev.d = h.d ORDER BY id;",
        "SELECT DISTINCT d FROM ev ORDER BY d;",
        "WITH e AS (SELECT * FROM emp WHERE id > 1) SELECT name FROM e ORDER BY name;",
        "WITH e AS (SELECT * FROM emp WHERE id > 1),\n                  n AS (SELECT name FROM e WHERE did IS NOT NULL)\n             SELECT * FROM n;",
        "WITH d(did, n) AS (SELECT did, COUNT(*) FROM emp GROUP BY did)\n             SELECT did, n FROM d ORDER BY did;",
        "WITH e AS (SELECT * FROM emp) SELECT dname FROM e JOIN dept ON e.did = dept.did ORDER BY 1;",
        "SELECT name FROM (SELECT * FROM emp WHERE id = 2) AS s;",
        "SELECT name FROM emp WHERE did = (SELECT did FROM dept WHERE dname = 'ops');",
        "SELECT name FROM emp WHERE did = (SELECT did FROM dept WHERE false);",
        "SELECT (SELECT id FROM emp);",
        "SELECT (SELECT COUNT(*) FROM emp WHERE false);",
        "SELECT name FROM emp WHERE did IN (SELECT did FROM dept) ORDER BY name;",
        "SELECT name FROM emp WHERE did NOT IN (SELECT did FROM dept) ORDER BY name;",
        "SELECT name FROM emp WHERE id IN (1, 3) ORDER BY name;",
        "SELECT COUNT(*) FROM emp WHERE id NOT IN (2);",
        "SELECT name FROM emp WHERE id IN (1, NULL);",
        "SELECT COUNT(*) FROM emp WHERE EXISTS (SELECT * FROM dept);",
        "SELECT COUNT(*) FROM emp WHERE NOT EXISTS (SELECT * FROM dept WHERE false);",
        "SELECT COUNT(*) FROM emp WHERE EXISTS (SELECT * FROM dept WHERE false);",
        "SELECT dname FROM dept WHERE EXISTS (SELECT * FROM emp WHERE emp.did = dept.did);",
        "SELECT name, dname FROM emp JOIN dept ON emp.did = dept.did ORDER BY name;",
        "SELECT COUNT(*) FROM emp JOIN dept ON emp.did = dept.did;",
        "SELECT dname, COUNT(*) FROM emp JOIN dept ON emp.did = dept.did \n             WHERE emp.id > 0 AND dept.did > 5 GROUP BY dname ORDER BY dname;",
        "SELECT region, amount,\n                    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount) AS rn,\n                    RANK() OVER (PARTITION BY region ORDER BY amount) AS r,\n                    DENSE_RANK() OVER (PARTITION BY region ORDER BY amount) AS d\n             FROM sales ORDER BY region, amount;",
        "SELECT amount, SUM(amount) OVER (ORDER BY amount) AS run FROM sales ORDER BY amount;",
        "SELECT COUNT(*) OVER () AS c FROM sales;",
        "SELECT amount, SUM(amount) OVER (ORDER BY amount ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS w\n             FROM sales ORDER BY amount;",
        "INSERT INTO t VALUES (1), (2), (3), (4), (5);",
        "SELECT COUNT(*) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) FROM t;",
        "SELECT COUNT(*) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM t;",
        "SELECT SUM(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) FROM t;",
        "SELECT COUNT(*) OVER (ORDER BY x ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM t;",
        "SELECT COUNT(*) OVER (ORDER BY x ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) FROM t;",
        "SELECT COUNT(*) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;",
        "CREATE TABLE f (d DOUBLE);",
        "INSERT INTO f VALUES (0.5), (1.5), (2.0);",
        "SELECT SUM(d) OVER (ORDER BY d ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),\n                    AVG(d) OVER (ORDER BY d ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM f;",
        "SELECT MIN(d) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),\n                    MAX(d) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM f;",
        "CREATE TABLE wt (id INTEGER, k INTEGER, x INTEGER);",
        "SELECT MIN(c), MAX(c) FROM (SELECT COUNT(*) OVER (PARTITION BY k) AS c FROM wt) AS s;",
        "SELECT SUM(x) OVER (PARTITION BY k ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s \n             FROM wt;",
        "SELECT region, SUM(amount) AS s, RANK() OVER (ORDER BY SUM(amount) DESC) AS r\n             FROM sales GROUP BY region ORDER BY r;",
        "SELECT * FROM sales WHERE RANK() OVER () > 1;",
        "SELECT * FROM sales GROUP BY RANK() OVER ();",
        "SELECT SUM(amount) FROM sales HAVING RANK() OVER () > 1;",
        "SELECT RANK() OVER (ORDER BY SUM(amount) OVER ()) FROM sales;",
        "SELECT MEDIAN(amount) OVER () FROM sales;",
        "INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, NULL);",
        "UPDATE t SET a = a + 10 WHERE a > 1;",
        "UPDATE t SET b = 'z';",
        "UPDATE t SET b = NULL WHERE a = 1;",
        "SELECT b FROM t WHERE a = 1;",
        "CREATE TABLE s (a INTEGER, b INTEGER);",
        "INSERT INTO s VALUES (1, 2);",
        "UPDATE s SET a = b, b = a;",
        "SELECT a, b FROM s;",
        "CREATE TABLE n (i INTEGER, g BIGINT, d DOUBLE);",
        "INSERT INTO n VALUES (1, 1, 1.0);",
        "UPDATE n SET g = i, d = g;",
        "UPDATE n SET i = g;",
        "UPDATE n SET i = 'x';",
        "UPDATE n SET missing = 1;",
        "UPDATE n SET i = 1, i = 2;",
        "UPDATE n SET i = SUM(i);",
        "SELECT i, g, d FROM n;",
        "DELETE FROM t WHERE a >= 2;",
        "SELECT a FROM t;",
        "DELETE FROM t;",
        "SELECT * FROM t;",
        "INSERT INTO t VALUES (9, 'q');",
        "DELETE FROM missing;",
        "DELETE FROM t WHERE a;",
        "UPDATE t SET b = 'top' WHERE a = (SELECT MAX(a) FROM t);",
        "SELECT b FROM t WHERE a = 3;",
        "DELETE FROM t WHERE a IN (SELECT a FROM t WHERE b IS NULL);",
        "UPDATE t SET a = 100 WHERE a = 1;",
        "DELETE FROM t WHERE a = 2;",
        "CREATE TABLE tmp (x INTEGER);",
        "SELECT a FROM t WHERE a = 1;",
        "BEGIN TRANSACTION;",
        "CREATE TABLE a (x INTEGER, s VARCHAR);",
        "CREATE TABLE b (y BIGINT, t VARCHAR);",
        "INSERT INTO a VALUES (1, 'one'), (2, 'two'), (NULL, 'nil');",
        "INSERT INTO b VALUES (2, 'TWO'), (3, 'three');",
        "SELECT x FROM a WHERE x IS NOT NULL UNION ALL SELECT y FROM b ORDER BY x;",
        "SELECT x AS n FROM a WHERE x = 1 UNION SELECT y AS m FROM b WHERE y = 3;",
        "SELECT x FROM a UNION SELECT y FROM b ORDER BY x;",
        "SELECT x FROM a UNION SELECT y, t FROM b;",
        "SELECT s FROM a UNION SELECT y FROM b;",
        "CREATE TABLE a1 (x INTEGER);",
        "INSERT INTO a1 VALUES (1), (3), (5);",
        "CREATE TABLE a2 (x INTEGER);",
        "INSERT INTO a2 VALUES (2), (4);",
        "SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY x DESC LIMIT 3;",
        "SELECT x FROM a1 UNION ALL SELECT x FROM a2 LIMIT 2;",
        "SELECT x FROM a1 UNION SELECT x FROM a2 ORDER BY x DESC LIMIT 3;",
        "SELECT x AS n FROM a1 UNION ALL SELECT x FROM a2 ORDER BY 1 DESC LIMIT 2;",
        "SELECT x AS n FROM a1 UNION ALL SELECT x FROM a2 ORDER BY n DESC LIMIT 2;",
        "SELECT x FROM a1 ORDER BY x LIMIT 1 UNION ALL SELECT x FROM a2;",
        "SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY x LIMIT 1 UNION ALL SELECT x FROM a1;",
        "SELECT x FROM (SELECT x FROM a1 UNION ALL SELECT x FROM a2) s ORDER BY x;",
        "SELECT x FROM (SELECT x FROM a1 UNION ALL SELECT x FROM a2) s ORDER BY x DESC LIMIT 3;",
        "SELECT (SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY x DESC LIMIT 1);",
        "SELECT x FROM a1 WHERE x IN (SELECT x FROM a1 UNION SELECT x FROM a2) ORDER BY x;",
        "SELECT x FROM a1 WHERE EXISTS (SELECT x FROM a2 WHERE x = 99 UNION ALL SELECT x FROM a2 WHERE x = 4) ORDER BY x;",
        "WITH c AS (SELECT x FROM a1 UNION ALL SELECT x FROM a2) SELECT x FROM c ORDER BY x DESC LIMIT 3;",
        "WITH c AS (SELECT 1 AS x) SELECT x FROM c UNION ALL SELECT x FROM c;",
        "CREATE VIEW uv AS SELECT x FROM a1 UNION ALL SELECT x FROM a2;",
        "SELECT x FROM uv ORDER BY x DESC LIMIT 3;",
        "CREATE VIEW uv2 AS SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY x DESC LIMIT 3;",
        "SELECT x FROM uv2;",
        "CREATE VIEW bad AS SELECT x FROM a1 ORDER BY x LIMIT 1 UNION ALL SELECT x FROM a2;",
        "CREATE TABLE a3 (y BIGINT);",
        "INSERT INTO a3 VALUES (2), (4);",
        "SELECT * FROM (SELECT x FROM a1 UNION ALL SELECT y FROM a3) s ORDER BY x;",
        "SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY -x LIMIT 2;",
        "SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY x + 1 DESC LIMIT 2;",
        "SELECT * FROM (SELECT x FROM a1 UNION ALL SELECT x FROM a2 ORDER BY -x) s;",
        "SELECT x AS n FROM a1 UNION ALL SELECT x AS m FROM a2 ORDER BY m;",
        "CREATE TABLE ti (x INTEGER);",
        "INSERT INTO ti VALUES (1), (3);",
        "CREATE TABLE td (x DOUBLE);",
        "INSERT INTO td VALUES (2.0), (3.0);",
        "SELECT * FROM (SELECT x FROM ti UNION ALL SELECT x FROM td) s WHERE x = 3;",
        "SELECT * FROM (SELECT x FROM ti UNION ALL SELECT x FROM td) s;",
        "SELECT x FROM a1 UNION ALL SELECT x FROM a2 UNION ALL SELECT x FROM a1 LIMIT 2;",
        "SELECT x FROM a1 UNION SELECT x FROM a2 UNION ALL SELECT x FROM a1 ORDER BY x DESC LIMIT 3;",
        "CREATE TABLE a3 (x INTEGER);",
        "INSERT INTO a3 VALUES (3), (5), (7);",
        "SELECT x FROM a1 UNION SELECT x FROM a3 ORDER BY x DESC LIMIT 2;",
        "WITH c(a) AS (SELECT x FROM a1 UNION ALL SELECT x FROM a2) SELECT a FROM c ORDER BY a DESC LIMIT 2;",
        "SELECT (SELECT x FROM a1 UNION ALL SELECT x, x FROM a2);",
        "WITH c AS (SELECT x FROM a1 UNION ALL SELECT x, x FROM a2) SELECT x FROM c;",
        "WITH o AS (WITH i AS (SELECT x FROM a1 UNION ALL SELECT x FROM a2) SELECT x FROM i) SELECT x FROM o ORDER BY x;",
        "WITH c AS (SELECT x FROM a1) SELECT x FROM a2 UNION ALL SELECT x FROM c UNION ALL SELECT x FROM a2 ORDER BY x;",
        "CREATE VIEW evens AS SELECT x FROM a WHERE x % 2 = 0;",
        "SELECT * FROM evens;",
        "WITH w AS (SELECT * FROM evens) SELECT x, y FROM w JOIN b ON w.x = b.y;",
        "CREATE VIEW named(n) AS SELECT x FROM a WHERE x = 1;",
        "SELECT n FROM named;",
        "CREATE VIEW a AS SELECT 1;",
        "CREATE TABLE evens (x INTEGER);",
        "CREATE VIEW evens AS SELECT 2;",
        "DROP TABLE b;",
        "CREATE VIEW vb AS SELECT y FROM b;",
        "DROP VIEW evens;",
        "DROP TABLE missing;",
        "SELECT CAST(x AS BIGINT), CAST(x AS DOUBLE) FROM a WHERE x = 1;",
        "SELECT CAST(s AS INTEGER) FROM a WHERE x = 1;",
        "SELECT CAST(x AS DATE) FROM a;",
        "SELECT COALESCE(x, 99), COALESCE(s, 'dflt') FROM a ORDER BY 1;",
        "SELECT s FROM a WHERE s LIKE 'o%' ORDER BY s;",
        "SELECT s FROM a WHERE s NOT LIKE '%o%' ORDER BY s;",
        "SELECT CAST(NULL AS VARCHAR) LIKE 'a';",
        "SELECT x LIKE '1' FROM a;",
        "CREATE TABLE t (id INTEGER, name VARCHAR);",
        "INSERT INTO t VALUES (1, 'ann'), (2, 'bob'), (3, NULL);",
        "SELECT id, CASE WHEN id = 1 THEN 'one' WHEN id = 2 THEN 'two' ELSE 'other' END AS label\n             FROM t ORDER BY id;",
        "SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM t ORDER BY id;",
        "SELECT CASE WHEN name IS NULL THEN 'anon' WHEN id > 100 THEN NULL ELSE name END FROM t ORDER BY id;",
        "SELECT CASE WHEN id = 1 THEN 1 WHEN id = 2 THEN 2.5 ELSE 0 END FROM t ORDER BY id;",
        "SELECT CASE WHEN id > 1 THEN CASE WHEN id > 2 THEN 'big' ELSE 'mid' END ELSE 'small' END FROM t ORDER BY id;",
        "SELECT CASE WHEN id THEN 1 END FROM t;",
        "SELECT CASE WHEN true THEN 1 ELSE 'x' END FROM t;",
        "SELECT COUNT(*) FROM t WHERE CASE WHEN id = 1 THEN true ELSE false END;",
        "SELECT CASE WHEN id <= 2 THEN 'low' ELSE 'high' END AS grp, COUNT(*) FROM t GROUP BY grp ORDER BY grp;",
        "SELECT SUM(CASE WHEN id = 3 THEN 10 ELSE 1 END) FROM t;",
        "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER, id INTEGER);",
        "SELECT a, b, c, id FROM t ORDER BY a, b, c LIMIT 2;",
        "SELECT a, b, c, id FROM t ORDER BY a, b, c;",
        "CREATE TABLE t (a INTEGER, b INTEGER, c VARCHAR, id INTEGER);",
        "SELECT a, b, c, id FROM t ORDER BY a ASC, b DESC, c ASC LIMIT 25;",
        "SELECT a, b, c, id FROM t ORDER BY a ASC, b DESC, c ASC;",
        "CREATE TABLE t (a INTEGER, id INTEGER);",
        "SELECT a, id FROM t ORDER BY a LIMIT 2;",
        "SELECT a, id FROM t ORDER BY a;",
        "CREATE TABLE s (k VARCHAR, id INTEGER);",
        "SELECT k, id FROM s ORDER BY k DESC LIMIT 2;",
        "SELECT k, id FROM s ORDER BY k DESC;",
        "SELECT k, id FROM s ORDER BY k DESC LIMIT 10000;",
        "CREATE TABLE f (d DOUBLE, id INTEGER);",
        "INSERT INTO f VALUES (3.0, 1), (1.0, 2), (NULL, 3), (2.0, 4), (1.5, 5), (0.0 / 0.0, 6);",
        "SELECT d, id FROM f ORDER BY d;",
        "SELECT d, id FROM f ORDER BY d LIMIT 2;",
        "CREATE TABLE m (i INTEGER, g BIGINT, d DOUBLE, s VARCHAR, b BOOLEAN, dt DATE, ts TIMESTAMP);",
        "CREATE TABLE t (v INTEGER, id INTEGER);",
        "CREATE TABLE fa (id INTEGER, region VARCHAR, x INTEGER);",
        "CREATE TABLE fb (id BIGINT, region VARCHAR, total DOUBLE);",
        "INSERT INTO fa VALUES\n             (1, 'west', 3), (2, 'east', 5), (1, 'west', 7), (NULL, 'west', 11), (2, 'east', NULL);",
        "INSERT INTO fb VALUES\n             (1, 'W1', 10.5), (2, 'E1', NULL), (1, 'W2', 20.0), (3, 'X', 1.0),\n             (NULL, 'N', 2.5), (2, 'E2', 4.25);",
        "SELECT COUNT(*) FROM fa JOIN fb ON fa.id = fb.id;",
        "SELECT COUNT(*) FROM (SELECT fa.region FROM fa JOIN fb ON fa.id = fb.id) s;",
        "CREATE TABLE fbe (id BIGINT, total DOUBLE);",
        "SELECT COUNT(*) FROM fa JOIN fbe ON fa.id = fbe.id;",
        "SELECT fa.region, COUNT(*), COUNT(fa.x), SUM(fa.x), AVG(fa.x), MIN(fa.x), MAX(fa.x)\n                   FROM fa JOIN fb ON fa.id = fb.id",
        "SELECT COUNT(*), SUM(fb.total * 2), SUM(fa.x * 3) FROM fa JOIN fb ON fa.id = fb.id",
        "SELECT region, COUNT(*), SUM(t)\n             FROM (SELECT fa.region AS region, fb.total AS t FROM fa JOIN fb ON fa.id = fb.id) s\n             GROUP BY region;",
        "SELECT region, COUNT(*), SUM(t)\n             FROM (SELECT fa.region AS region, fb.total AS t FROM fa JOIN fb ON fa.id = fb.id\n                   WHERE fa.id = fb.id) s\n             GROUP BY region;",
        "SELECT region, COUNT(*)\n                 FROM (SELECT fa.region AS region FROM fa JOIN fb ON fa.id = fb.id) s\n                 GROUP BY region;",
        "SELECT fb.region, COUNT(*), SUM(fa.x)\n                 FROM fa JOIN fb ON fa.id = fb.id GROUP BY fb.region;",
        "SELECT upper(fb.region), COUNT(*)\n                 FROM fa JOIN fb ON fa.id = fb.id GROUP BY upper(fb.region);",
        "CREATE TABLE fsa (k VARCHAR, v INTEGER);",
        "CREATE TABLE fsb (k VARCHAR, w INTEGER);",
        "INSERT INTO fsa VALUES ('a', 1), ('b', 2), ('a', 3), (NULL, 4);",
        "INSERT INTO fsb VALUES ('a', 10), ('b', 20), ('a', 30), (NULL, 40);",
        "SELECT COUNT(*) FROM fsa JOIN fsb ON fsa.k = fsb.k;",
        "SELECT fsa.k, COUNT(*), SUM(fsb.w) FROM fsa JOIN fsb ON fsa.k = fsb.k GROUP BY fsa.k;",
        "SELECT fsa.k, COUNT(*), SUM(fsb.w)\n             FROM fsa JOIN fsb ON fsa.k = fsb.k WHERE fsa.k = fsb.k GROUP BY fsa.k;",
        "CREATE TABLE tl (ts TIMESTAMP, lv INTEGER);",
        "CREATE TABLE tr (ts TIMESTAMP, rv INTEGER);",
        "INSERT INTO tl VALUES\n             (TIMESTAMP '2024-01-15 10:00:00', 1),\n             (TIMESTAMP '2024-01-15 10:00:00.000001', 2), (NULL, 3);",
        "INSERT INTO tr VALUES\n             (TIMESTAMP '2024-01-15 10:00:00.000001', 10),\n             (TIMESTAMP '2024-01-15 10:00:00', 11);",
        "SELECT COUNT(*) FROM tl JOIN tr ON tl.ts = tr.ts;",
        "SELECT tl.lv, COUNT(*), MIN(tr.rv)\n                 FROM tl JOIN tr ON tl.ts = tr.ts GROUP BY tl.lv;",
        "CREATE TABLE dl (k DOUBLE, lv INTEGER);",
        "CREATE TABLE dr (k DOUBLE, rv INTEGER);",
        "INSERT INTO dl VALUES (1.5, 1), (2.5, 2), (-0.0, 3), (NULL, 4);",
        "INSERT INTO dr VALUES (1.5, 10), (2.5, 20), (0.0, 30), (NULL, 40);",
        "SELECT COUNT(*) FROM dl JOIN dr ON dl.k = dr.k;",
        "SELECT dl.lv, COUNT(*) FROM dl JOIN dr ON dl.k = dr.k GROUP BY dl.lv;",
        "SELECT COUNT(*), COUNT(total), SUM(total), AVG(total), MIN(total), MAX(total)\n                 FROM fa JOIN fbe ON fa.id = fbe.id;",
        "SELECT fa.region, COUNT(*) FROM fa JOIN fbe ON fa.id = fbe.id GROUP BY fa.region;",
        "SELECT COUNT(*) FROM fa JOIN fb ON fa.id = fb.id WHERE false;",
        "SELECT fa.region, COUNT(*) FROM fa JOIN fb ON fa.id = fb.id\n                 WHERE false GROUP BY fa.region;",
        "SELECT fa.id + fb.id, COUNT(*)\n                 FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.id + fb.id;",
        "SELECT COUNT(*), SUM(c)\n                 FROM (SELECT fa.region, COUNT(*) AS c\n                       FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.region) s;",
        "WITH g AS (SELECT fa.region AS r, COUNT(*) AS c\n                            FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.region)\n                 SELECT r, c FROM g;",
        "CREATE TABLE ml (k INTEGER, v INTEGER);",
        "CREATE TABLE mr (k INTEGER, w INTEGER);",
        "SELECT ml.k, COUNT(*), SUM(ml.v), SUM(mr.w)\n             FROM ml JOIN mr ON ml.k = mr.k GROUP BY ml.k;",
        "SELECT ml.k, COUNT(*), SUM(ml.v), SUM(mr.w)\n             FROM ml JOIN mr ON ml.k = mr.k WHERE ml.k = mr.k GROUP BY ml.k;",
        "CREATE TABLE xl (k INTEGER, v INTEGER);",
        "CREATE TABLE xr (k INTEGER, w INTEGER);",
        "INSERT INTO xl VALUES (1, 5);",
        "SELECT xl.k, COUNT(*), SUM(xr.w) FROM xl JOIN xr ON xl.k = xr.k GROUP BY xl.k;",
        "SELECT xl.k, COUNT(*), SUM(xr.w)\n             FROM xl JOIN xr ON xl.k = xr.k WHERE xl.k = xr.k GROUP BY xl.k;",
        "SELECT fa.region, COUNT(*), SUM(fa.x), AVG(fb.total), MIN(fb.total), MAX(fb.total)\n             FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.region;",
        "SELECT fa.x + 1, COUNT(*) FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.x + 1;",
        "SELECT upper(fb.region), COUNT(*), SUM(fb.total)\n             FROM fa JOIN fb ON fa.id = fb.id GROUP BY upper(fb.region);",
        "SELECT region, COUNT(*), SUM(t)\n             FROM (SELECT fa.region AS region, fb.total AS t\n                   FROM fa JOIN fb ON fa.id = fb.id) s GROUP BY region;",
        "SELECT COUNT(*), SUM(fb.total * 2) FROM fa JOIN fb ON fa.id = fb.id;",
        "SELECT fa.region FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.region;",
        "SELECT COUNT(*), SUM(fb.total)\n             FROM fa JOIN fb ON fa.id = fb.id WHERE fb.total > 5;",
        "SELECT COUNT(*), SUM(fb.total)\n             FROM fa JOIN fb ON fa.id = fb.id WHERE fa.id = fb.id AND fb.total > 5;",
        "SELECT fa.id + fb.id, COUNT(*) FROM fa JOIN fb ON fa.id = fb.id GROUP BY fa.id + fb.id;",
        "SELECT SUM(fa.x + fb.total) FROM fa JOIN fb ON fa.id = fb.id;",
        "SELECT fa.region, COUNT(*) FROM fa JOIN fb ON fa.id = fb.id\n             WHERE fa.id = fb.id GROUP BY fa.region;",
        "SELECT COUNT(*) FROM (SELECT DISTINCT fa.region FROM fa JOIN fb ON fa.id = fb.id) s;",
        "CREATE TABLE t1 (k INTEGER, x INTEGER);",
        "CREATE TABLE t2 (k INTEGER, q INTEGER);",
        "INSERT INTO t1 VALUES (1, 10);",
        "INSERT INTO t2 VALUES (1, 2), (2, 0);",
        "SELECT SUM(100 / q) FROM t1 JOIN t2 ON t1.k = t2.k;",
        "CREATE TABLE va (k INTEGER, x INTEGER);",
        "CREATE TABLE vb (k INTEGER, qty INTEGER, s VARCHAR, a INTEGER, b INTEGER);",
        "INSERT INTO va VALUES (1, 10);",
        "INSERT INTO vb VALUES (1, 2, '3', 4, 2), (2, 0, 'nope', 5, 0);",
        "SELECT SUM(100 / vb.qty) FROM va JOIN vb ON va.k = vb.k;",
        "SELECT SUM(CAST(vb.s AS INTEGER)) FROM va JOIN vb ON va.k = vb.k;",
        "SELECT vb.a % vb.b, COUNT(*) FROM va JOIN vb ON va.k = vb.k GROUP BY vb.a % vb.b;",
        "SELECT SUM(fb.total * 2) FROM fa JOIN fb ON fa.id = fb.id;",
        "SELECT SUM(abs(fa.x)) FROM fa JOIN fb ON fa.id = fb.id;",
        "SELECT region, COUNT(*) FROM (SELECT upper(fa.region) AS region\n             FROM fa JOIN fb ON fa.id = fb.id) s GROUP BY region;",
        "SELECT region, COUNT(*) FROM (SELECT fa.region AS region,\n                    CAST(fb.region AS INTEGER) AS junk\n             FROM fa JOIN fb ON fa.id = fb.id) s GROUP BY region;",
        "SELECT region, COUNT(*) FROM (SELECT fa.region AS region, 1/0 AS junk\n             FROM fa JOIN fb ON fa.id = fb.id) s GROUP BY region;",
        "SELECT region, COUNT(*) FROM (SELECT fa.region AS region,\n                          upper(fb.region) AS junk\n                   FROM fa JOIN fb ON fa.id = fb.id) s GROUP BY region;",
        "CREATE TABLE fl (k INTEGER, d DOUBLE);",
        "CREATE TABLE fr (k INTEGER);",
        "SELECT SUM(fl.d), AVG(fl.d) FROM fl JOIN fr ON fl.k = fr.k;",
        "SELECT SUM(fl.d), AVG(fl.d) FROM fl JOIN fr ON fl.k = fr.k WHERE fl.k = fr.k;",
        "CREATE TABLE ija (k INTEGER, v INTEGER);",
        "CREATE TABLE ijb (k DOUBLE, w INTEGER);",
        "INSERT INTO ija VALUES (1, 10), (2, 20), (2, 21), (NULL, 30);",
        "INSERT INTO ijb VALUES (2.0, 1), (1.0, 2), (2.0, 3), (3.0, 4);",
        "SELECT COUNT(*) FROM ija JOIN ijb ON ija.k = ijb.k;",
        "SELECT ija.k, COUNT(*), SUM(ijb.w) FROM ija JOIN ijb ON ija.k = ijb.k GROUP BY ija.k;",
        "SELECT ija.k, COUNT(*), SUM(ijb.w)\n             FROM ija JOIN ijb ON ija.k = ijb.k WHERE ija.k = ijb.k GROUP BY ija.k;",
        "SELECT ijb.k, COUNT(*), SUM(ija.v) FROM ijb JOIN ija ON ijb.k = ija.k GROUP BY ijb.k;",
        "CREATE TABLE ela (k BIGINT, v INTEGER);",
        "CREATE TABLE elb (k BIGINT, w INTEGER);",
        "INSERT INTO ela VALUES\n             ((-9223372036854775807 - 1), 1), (9223372036854775807, 2), (0, 3), (NULL, 4);",
        "INSERT INTO elb VALUES\n             (9223372036854775807, 10), ((-9223372036854775807 - 1), 20),\n             (9223372036854775807, 30), (NULL, 40);",
        "SELECT COUNT(*) FROM ela JOIN elb ON ela.k = elb.k;",
        "SELECT ela.k, COUNT(*), SUM(elb.w) FROM ela JOIN elb ON ela.k = elb.k GROUP BY ela.k;",
        "SELECT ela.k, COUNT(*), SUM(elb.w)\n             FROM ela JOIN elb ON ela.k = elb.k WHERE ela.k = elb.k GROUP BY ela.k;",
        "CREATE TABLE dta (d DATE, v INTEGER);",
        "CREATE TABLE dtb (d DATE, w INTEGER);",
        "INSERT INTO dta VALUES\n             (DATE '2024-01-15', 1), (DATE '1969-12-31', 2), (DATE '2024-01-15', 3), (NULL, 4);",
        "INSERT INTO dtb VALUES\n             (DATE '2024-01-15', 10), (DATE '2000-01-01', 20), (DATE '2024-01-15', 30);",
        "SELECT dta.d, COUNT(*), SUM(dtb.w) FROM dta JOIN dtb ON dta.d = dtb.d GROUP BY dta.d;",
        "SELECT dta.d, COUNT(*), SUM(dtb.w) FROM dta JOIN dtb ON dta.d = dtb.d\n                 WHERE dta.d = dtb.d GROUP BY dta.d;",
        "CREATE TABLE gka (d DATE, ts TIMESTAMP, k INTEGER, v INTEGER);",
        "CREATE TABLE gkb (k INTEGER);",
        "INSERT INTO gka VALUES\n             (DATE '2024-01-15', TIMESTAMP '2024-01-15 10:00:00', 1, 1),\n             (DATE '2024-02-20', TIMESTAMP '2024-01-15 11:30:00', 1, 2),\n             (DATE '2023-12-31', TIMESTAMP '2024-01-16 10:00:00', 2, 3);",
        "INSERT INTO gkb VALUES (1), (1), (2);",
        "CREATE TABLE lea (id INTEGER, x INTEGER);",
        "CREATE TABLE leb (id BIGINT, total DOUBLE);",
        "SELECT COUNT(*), COUNT(lea.x), SUM(lea.x), AVG(lea.x),\n                        MIN(leb.total), MAX(leb.total)\n                 FROM lea JOIN leb ON lea.id = leb.id;",
        "SELECT lea.id, COUNT(*) FROM lea JOIN leb ON lea.id = leb.id GROUP BY lea.id;",
        "SELECT g, COUNT(*), SUM(t) FROM (SELECT lea.id AS g, leb.total AS t\n             FROM lea JOIN leb ON lea.id = leb.id) s GROUP BY g;",
        "SELECT g, COUNT(*), SUM(v) FROM (SELECT x.id AS g, x.x AS v\n                   FROM (SELECT fa.id AS id, fa.x AS x FROM fa JOIN fb ON fa.id = fb.id) x\n                   JOIN fb f2 ON x.id = f2.id) s GROUP BY g;",
        "SELECT g, COUNT(*), SUM(v) FROM (SELECT x.id AS g, x.x AS v\n                        FROM (SELECT fa.id AS id, fa.x AS x FROM fa JOIN fb ON fa.id = fb.id) x\n                        JOIN fb f2 ON x.id = f2.id WHERE x.id = f2.id) s GROUP BY g;",
        "SELECT COUNT(*), COUNT(*) FROM fa JOIN fb ON fa.id = fb.id;",
        "SELECT COUNT(*), COUNT(1), SUM(fa.x) FROM fa JOIN fb ON fa.id = fb.id;",
        "INSERT INTO s VALUES (3, 'z');",
        "SELECT a, b FROM s ORDER BY a;",
        "SELECT * FROM sales;",
        "SELECT nope FROM sales;",
        "SELECT * FROM missing;",
        "SELECT id FROM sales WHERE SUM(total) > 1;",
        "SELECT region, SUM(total) FROM sales GROUP BY region;",
        "SELECT id, SUM(total) FROM sales GROUP BY region;",
        "SELECT SUM(total) FROM sales;",
        "SELECT id FROM sales WHERE total BETWEEN 1 AND 5;",
        "SELECT * FROM sales JOIN sales ON sales.id = sales.id;",
        "SELECT * FROM sales s JOIN sales o ON s.id > o.id;",
        "SELECT * FROM sales s JOIN sales o ON s.id = o.id;",
        "SELECT id FROM sales UNION ALL SELECT id FROM sales;",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SELECT 1 + 2 * 3;",
        "SELECT a FROM t WHERE x BETWEEN 1 AND 5 AND y IS NOT NULL;",
        "SELECT * FROM a JOIN b ON a.id = b.id;",
        "SELECT * FROM a INNER JOIN b ON a.id = b.id;",
        "SELECT * FROM a LEFT JOIN b ON a.id = b.id;",
        "SELECT * FROM a left outer join b ON a.id = b.id;",
        "SELECT * FROM a RIGHT JOIN b ON a.id = b.id;",
        "SELECT * FROM a right OUTER join b ON a.id = b.id;",
        "SELECT DATE '2024-02-29', TIMESTAMP '2024-03-15 12:00:00.5';",
        "CREATE TABLE t (d DATE, ts TIMESTAMP);",
        "SELECT rank() OVER (PARTITION BY a ORDER BY b DESC) FROM t;",
        "SELECT sum(x) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t;",
        "SELECT CASE WHEN a > 1 THEN 'big' WHEN a = 1 THEN 'one' ELSE 'other' END FROM t;",
        "SELECT CASE a WHEN 1 THEN 'one' END FROM t;",
        "UPDATE t SET a = 1, b = a + 2 WHERE a > 0;",
        "ROLLBACK TRANSACTION;",
        "WITH a AS (SELECT 1 AS x), b(y) AS (SELECT x FROM a) SELECT * FROM b;",
        "SELECT * FROM (SELECT 1 AS x) AS s;",
        "SELECT a FROM t WHERE a IN (SELECT b FROM u) AND EXISTS (SELECT 1);",
        "SELECT * FROM t WHERE a IN (1, 2);",
        "SELECT (SELECT MAX(x) FROM u) + 1;",
        "\nCREATE TABLE emp (id INTEGER, name VARCHAR, did INTEGER, salary DOUBLE, hired DATE, active BOOLEAN);\nINSERT INTO emp VALUES\n (1, 'ann', 10, 150.5, DATE '2024-01-15', true),\n (2, 'bob', 20, 50.0, DATE '2024-02-20', false),\n (3, 'cat', NULL, 200.25, DATE '2023-12-01', true),\n (4, 'dan', 10, NULL, DATE '2024-01-15', NULL),\n (5, 'eve', 20, 75.75, NULL, false);\nCREATE TABLE dept (did INTEGER, dname VARCHAR);\nINSERT INTO dept VALUES (10, 'eng'), (20, 'ops'), (30, 'hr');\n",
        "SELECT id, name FROM emp WHERE salary > 100 ORDER BY id;",
        "SELECT id FROM emp WHERE did IS NULL;",
        "SELECT id FROM emp WHERE salary IS NOT NULL ORDER BY id;",
        "SELECT id FROM emp WHERE salary BETWEEN 50 AND 160 ORDER BY id;",
        "SELECT id FROM emp WHERE id NOT BETWEEN 2 AND 4 ORDER BY id;",
        "SELECT did, COUNT(*), COUNT(salary), SUM(salary), AVG(salary), MIN(salary), MAX(salary) FROM emp GROUP BY did ORDER BY did;",
        "SELECT did, AVG(salary) AS a FROM emp GROUP BY did HAVING COUNT(*) > 1 AND AVG(salary) > 10 ORDER BY did;",
        "SELECT DISTINCT did FROM emp ORDER BY did;",
        "SELECT upper(name), lower(name), length(name), substr(name, 1, 2) FROM emp ORDER BY id;",
        "SELECT id / 2, id % 2, -id, id + did FROM emp ORDER BY id;",
        "SELECT id FROM emp WHERE active AND salary > 10 ORDER BY id;",
        "SELECT id FROM emp WHERE NOT active ORDER BY id;",
        "SELECT id FROM emp WHERE active OR salary > 100 ORDER BY id;",
        "SELECT a.name FROM emp a JOIN emp b ON a.did = b.did WHERE a.id < b.id ORDER BY 1;",
        "SELECT emp.id, name, dname FROM emp LEFT JOIN dept ON emp.did = dept.did ORDER BY id;",
        "SELECT id, name, dept.did, dname FROM emp RIGHT JOIN dept ON emp.did = dept.did ORDER BY dname, id;",
        "SELECT emp.id, dname FROM emp LEFT JOIN dept ON emp.did = dept.did WHERE dept.did > 10 ORDER BY id;",
        "SELECT COUNT(*), COUNT(dept.did) FROM emp LEFT JOIN dept ON emp.did = dept.did;",
        "SELECT id FROM emp WHERE id <= 2 UNION ALL SELECT did FROM dept WHERE did >= 20;",
        "SELECT did FROM emp UNION SELECT did FROM dept;",
        "SELECT id FROM emp WHERE id <= 3 UNION ALL SELECT did FROM dept WHERE did >= 20 ORDER BY id DESC LIMIT 4;",
        "SELECT x FROM (SELECT id AS x FROM emp WHERE id <= 2 UNION ALL SELECT did AS x FROM dept WHERE did >= 20) s ORDER BY x;",
        "SELECT COUNT(*), SUM(salary), AVG(salary), MIN(salary) FROM emp WHERE false;",
        "SELECT name FROM emp WHERE id NOT IN (2, 3) ORDER BY name;",
        "SELECT name FROM emp WHERE id IN (1, 3, 5) ORDER BY name;",
        "SELECT COUNT(*) FROM emp WHERE EXISTS (SELECT * FROM dept WHERE did = 20);",
        "SELECT dname FROM dept WHERE NOT EXISTS (SELECT * FROM emp WHERE did = 999) ORDER BY 1;",
        "WITH e AS (SELECT * FROM emp WHERE id > 1), n AS (SELECT name FROM e WHERE did IS NOT NULL) SELECT * FROM n;",
        "SELECT name FROM (SELECT * FROM emp WHERE did = 20) AS s ORDER BY name;",
        "SELECT name, ROW_NUMBER() OVER (PARTITION BY did ORDER BY id) AS rn FROM emp ORDER BY name;",
        "SELECT RANK() OVER (ORDER BY salary) AS r, DENSE_RANK() OVER (ORDER BY salary) AS d FROM emp ORDER BY salary;",
        "SELECT id, SUM(salary) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run FROM emp ORDER BY id;",
        "SELECT did, SUM(salary) AS s, RANK() OVER (ORDER BY SUM(salary) DESC) AS r FROM emp GROUP BY did ORDER BY r;",
        "SELECT hired + 10, hired - DATE '2024-01-01' FROM emp ORDER BY id;",
        "SELECT year(hired), month(hired), day(hired) FROM emp ORDER BY id;",
        "SELECT date_trunc('month', hired) AS m, COUNT(*) FROM emp GROUP BY m ORDER BY m;",
        "SELECT MIN(hired), MAX(hired), COUNT(hired) FROM emp;",
        "SELECT name FROM emp WHERE name LIKE 'a%' OR name LIKE '%e%' ORDER BY name;",
        "SELECT name FROM emp WHERE name NOT LIKE '%a%' ORDER BY name;",
        "SELECT CAST(id AS VARCHAR), CAST(id AS DOUBLE), CAST(salary AS INTEGER) FROM emp WHERE id = 2;",
        "SELECT COALESCE(did, -1), COALESCE(salary, -1) FROM emp ORDER BY 1;",
        "SELECT 1 + 2 * 3, 7 / 2, 7 % 3;",
        "SELECT round(2.567, 1), abs(-3), abs(salary) FROM emp ORDER BY id;",
        "SELECT id FROM emp ORDER BY salary DESC;",
        "UPDATE emp SET salary = salary + 1 WHERE id = 1; SELECT salary FROM emp WHERE id = 1;",
        "DELETE FROM emp WHERE id = 5; SELECT COUNT(*) FROM emp;",
        "CREATE VIEW big AS SELECT * FROM emp WHERE salary > 100; SELECT name FROM big ORDER BY name;",
        "SELECT nope FROM emp;",
        "SELECT id, CASE WHEN id = 1 THEN 'one' WHEN id = 2 THEN 'two' ELSE 'other' END FROM emp ORDER BY id;",
        "SELECT CASE id WHEN 1 THEN 'one' ELSE 'many' END FROM emp ORDER BY id;",
        "SELECT CASE WHEN did IS NULL THEN 'none' WHEN salary > 100 THEN 'big' ELSE 'small' END FROM emp ORDER BY id;",
        "SELECT CASE WHEN id = 1 THEN 1 WHEN id = 2 THEN 2.5 ELSE 0 END FROM emp ORDER BY id;",
        "SELECT SUM(CASE WHEN did = 10 THEN salary ELSE 0 END) FROM emp;",
        "SELECT region, COUNT(*), AVG(total), SUM(total), MIN(total), MAX(total) FROM sales GROUP BY region ORDER BY region;",
        "SELECT region, AVG(total) FROM sales WHERE active GROUP BY region HAVING COUNT(*) > 100000 ORDER BY 2 DESC LIMIT 3;",
        "select 1;",
        "SeLeCt 1;",
        "SELECT selected, select_, s_elect FROM t;",
        "SELECT MyCol FROM MyTable WHERE OtherCol = 1;",
        "SELECT \"select\", \"my col\", \"we\"\"ird\" FROM \"tab le\";",
        "SELECT \"\" FROM t;",
        "SELECT ALL x FROM t;",
        "SELECT a AS \"select\" FROM t;",
        "SELECT 1e;",
        "SELECT 1ex;",
        "SELECT 0x10;",
        "SELECT 1_000;",
        "SELECT 3e;",
        "SELECT 'it''s', '', '''';",
        "SELECT 'a\\nb', 'tab\\there';",
        "SELECT 'héllo';",
        "SELECT 1 -- trailing comment",
        "SELECT 1 -- trailing comment\n;",
        "SELECT -- comment\n1;",
        "SELECT /* c */ 1;",
        "SELECT a/*x*/+1;",
        "SELECT a--x\nFROM t;",
        "SELECT 1, 2147483647, 2147483648, 9223372036854775807;",
        "SELECT 1.5, .5, 0.5E-3, 1e2, 1E+2, 1e-2, 3e2;",
        "SELECT .5e2;",
        "SELECT a<>b, a<=b, a>=b, a!=b, a=b, a<b, a>b FROM t;",
        "SELECT 1;",
        "SELECT 1 + 2 * 3 - 4 / 2 % 3;",
        "SELECT 2 * 3 + 1, (2 + 3) * 4;",
        "SELECT -1 - -2, +1, -(-1), -(1 + 2);",
        "SELECT 1 + 2 = 3, a = b AND c = d OR e;",
        "SELECT NOT a = b, NOT NOT a;",
        "SELECT a AND b OR c AND d;",
        "SELECT a IS NULL, a IS NOT NULL FROM t;",
        "SELECT a BETWEEN 1 AND 2, a NOT BETWEEN 1 + 1 AND 2 * 3 FROM t;",
        "SELECT a IN (1, 2), a NOT IN (1, 2), a IN () FROM t;",
        "SELECT a NOT IN () FROM t;",
        "SELECT a IN (SELECT b FROM t);",
        "SELECT a NOT IN (SELECT b FROM t);",
        "SELECT a IN (SELECT 1 UNION ALL SELECT 2);",
        "SELECT a IN ((SELECT 1));",
        "SELECT a IN (WITH x AS (SELECT 1) SELECT * FROM x);",
        "SELECT a LIKE 'x%', a NOT LIKE '%y_', a LIKE b FROM t;",
        "SELECT count(*), sum(x), f(), g(*), h((*));",
        "SELECT COALESCE(a), COALESCE(a, b, 1, 'x'), coalesce(did, -1);",
        "SELECT SUM(x) OVER ();",
        "SELECT CAST(x AS INTEGER), CAST(x AS INT), CAST(x AS BIGINT), CAST(x AS DOUBLE), CAST(x AS VARCHAR), CAST(x AS TEXT), CAST(x AS BOOLEAN), CAST(x AS DATE), CAST(x AS TIMESTAMP), CAST(x AS DATETIME);",
        "SELECT CAST(x AS \"INT\");",
        "SELECT CASE WHEN a THEN b END;",
        "SELECT CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END;",
        "SELECT CASE WHEN a THEN CASE WHEN b THEN 1 ELSE 2 END ELSE 3 END;",
        "SELECT timestamp '2024-03-15T12:00:00';",
        "SELECT TRUE, FALSE, NULL;",
        "SELECT (SELECT 1);",
        "SELECT (SELECT max(x) FROM u) + 1;",
        "SELECT (WITH x AS (SELECT 1) SELECT * FROM x);",
        "SELECT EXISTS (SELECT 1), NOT EXISTS (SELECT 1 FROM t);",
        "SELECT EXISTS (SELECT 1 FROM t WHERE a = 1) AND b;",
        "SELECT t.* FROM t;",
        "SELECT a AS x, b y, c AS \"z\" FROM t t1;",
        "SELECT DISTINCT a FROM t;",
        "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1 ORDER BY 2 DESC, a ASC LIMIT 10;",
        "SELECT a FROM t ORDER BY a DESC, b ASC;",
        "SELECT 1 LIMIT 0;",
        "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.x > 1 ORDER BY a.id LIMIT 3;",
        "SELECT * FROM (SELECT 1 AS x) s JOIN (SELECT 2 AS y) t ON s.x = t.y;",
        "SELECT * FROM (SELECT 1) AS s;",
        "WITH a AS (SELECT 1) SELECT * FROM a UNION ALL SELECT * FROM a;",
        "SELECT * FROM (WITH x AS (SELECT 1) SELECT * FROM x) s;",
        "SELECT 1 UNION SELECT 2;",
        "SELECT 1 UNION ALL SELECT 2;",
        "SELECT 1 UNION DISTINCT SELECT 2;",
        "SELECT 1 union all SELECT 2;",
        "SELECT 1 UNION ALL SELECT 2 UNION SELECT 3;",
        "SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) s;",
        "SELECT x AS n FROM a UNION ALL SELECT x AS m FROM b ORDER BY m;",
        "SELECT rank() OVER (), row_number() OVER (PARTITION BY a), sum(x) OVER (ORDER BY b DESC) FROM t;",
        "SELECT avg(x) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t;",
        "SELECT sum(x) OVER (ROWS UNBOUNDED PRECEDING) FROM t;",
        "SELECT sum(x) OVER (ROWS CURRENT ROW) FROM t;",
        "SELECT sum(x) OVER (ROWS 3 FOLLOWING) FROM t;",
        "SELECT sum(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;",
        "SELECT sum(x) OVER (ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM t;",
        "SELECT sum(x) OVER (ROW BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t;",
        "SELECT sum(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t;",
        "CREATE TABLE t (a INT, b TEXT, c FLOAT, d BOOL, e LONG, f STRING, g DATETIME, h DATE, i TIMESTAMP, j REAL);",
        "CREATE TABLE t (a INTEGER);",
        "CREATE TABLE \"select\" (\"from\" INT);",
        "INSERT INTO t VALUES ();",
        "INSERT INTO t VALUES (1, 'x'), ();",
        "INSERT INTO t (a, b) VALUES (1, 2), (3, NULL);",
        "UPDATE t SET a = 1;",
        "DELETE FROM t WHERE a IS NULL;",
        "COMMIT TRANSACTION;",
        "DROP TABLE t;",
        "DROP VIEW v;",
        "CREATE VIEW v AS SELECT 1;",
        "CREATE VIEW v (a, b) AS SELECT 1, 2;",
        "CREATE VIEW v AS SELECT 1 UNION ALL SELECT 2 ORDER BY 1 LIMIT 2;",
        "",
        ";",
        ";;;",
        ";;SELECT 1;;;",
        "SELECT 1; SELECT 2;",
        "SELECT 1; INSERT INTO t VALUES (1);",
        "SELECT 1;;SELECT 2;",
        "SELECT a FROM t WHERE a = 1 AND (b = 2 OR c = 3);",
        "SELECT a FROM t WHERE (a + b) * (c - d) / 2 % 3 = 0;",
        "SELECT upper(name), lower(name), length(name), substr(name, 1, 2) FROM t;",
        "SELECT date_trunc('month', d) AS m, COUNT(*) FROM t GROUP BY m ORDER BY m;",
        "SELECT year(d), month(d), day(d), hour(ts), minute(ts), second(ts) FROM t;",
        "SELECT a FROM t LIMIT 5;",
        // Operator/comment adjacency, keyword-boundary identifiers, number
        // edges, and COALESCE's `*` expression arguments (wave-21 review).
        "SELECT a- -b FROM t;",
        "SELECT a--b;",
        "SELECT a-/*x*/-b FROM t;",
        "SELECT 1/*x*/+2;",
        "SELECT ALLS, orderby FROM t;",
        "SELECT 007;",
        "SELECT COALESCE(*, 1);",
        "SELECT COALESCE(* + 1, 2);",
        "SELECT COALESCE(*/did, -1);",
        "SELECT coalesce(*, 1);",
        "SELECT CoAlEsCe(*);",
        // Multi-database: CREATE/DROP DATABASE, USE, qualified table refs.
        "CREATE DATABASE analytics;",
        "USE analytics;",
        "USE main;",
        "use MAIN;",
        "CREATE DATABASE \"My Db\";",
        "DROP DATABASE analytics;",
        "CREATE TABLE db1.t (a INTEGER, b VARCHAR);",
        "INSERT INTO db1.t VALUES (1, 'x'), (2, NULL);",
        "SELECT a FROM db1.t ORDER BY a;",
        "SELECT t.a FROM db1.t;",
        "SELECT * FROM db1.t AS x;",
        "SELECT COUNT(*) FROM db1.t;",
        "UPDATE db1.t SET a = a + 1 WHERE a > 0;",
        "DELETE FROM db1.t WHERE a = 1;",
        "DROP TABLE db1.t;",
        "CREATE VIEW db1.v AS SELECT * FROM db1.t;",
        "DROP VIEW db1.v;",
        "CREATE TABLE main.m (k BIGINT);",
        "INSERT INTO main.m VALUES (1);",
        "SELECT * FROM main.m;",
        "SELECT COUNT(*) FROM db1.t JOIN db2.u ON t.a = u.a;",
        "SELECT COUNT(*) FROM db1.t LEFT JOIN db2.u ON t.a = u.a;",
        "SELECT COUNT(*) FROM (SELECT a FROM db1.t) s JOIN db2.u ON s.a = u.a;",
        "WITH c AS (SELECT 1 AS x) SELECT * FROM c JOIN db1.t ON c.x = t.a;",
        "SELECT * FROM main.m AS m JOIN analytics.m AS a ON m.k = a.k;",
        // Loadable extensions: `LOAD EXTENSION '<path>'` (string-literal path,
        // keywords case-insensitive, path kept verbatim).
        "LOAD EXTENSION 'extensions/test_ext.dylib';",
        "LOAD EXTENSION '/tmp/a b/ext.so';",
        "load extension 'x';",
        "LOAD   EXTENSION   'x.dylib' ;",
        "SELECT 1; LOAD EXTENSION 'x'; SELECT 2;",
        // SQL completeness (joins, set ops, DML/DDL, windows, EXPLAIN/SHOW).
        "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id;",
        "SELECT * FROM a LEFT JOIN b ON a.id = b.id FULL OUTER JOIN c ON c.id = b.id;",
        "SELECT * FROM a CROSS JOIN b CROSS JOIN c;",
        "SELECT * FROM a JOIN b ON a.x = b.x AND a.y = b.y AND a.z > b.z;",
        "SELECT * FROM a JOIN b ON a.x < b.x;",
        "SELECT x FROM a INTERSECT SELECT x FROM b;",
        "SELECT x FROM a INTERSECT ALL SELECT x FROM b;",
        "SELECT x FROM a EXCEPT SELECT x FROM b ORDER BY x;",
        "SELECT x FROM a EXCEPT ALL SELECT x FROM b LIMIT 5;",
        "SELECT x FROM a UNION SELECT x FROM b INTERSECT SELECT x FROM c;",
        "INSERT INTO t (a, b) SELECT x, y FROM s;",
        "INSERT INTO t SELECT * FROM s;",
        "INSERT INTO t WITH c AS (SELECT 1 AS x) SELECT x FROM c;",
        "CREATE TABLE t AS SELECT a, b FROM s;",
        "CREATE TABLE IF NOT EXISTS t AS SELECT 1 AS x;",
        "CREATE TABLE IF NOT EXISTS t (a INTEGER);",
        "DROP TABLE IF EXISTS t;",
        "CREATE DATABASE IF NOT EXISTS db1;",
        "DROP DATABASE IF EXISTS db1;",
        "SELECT a FROM t ORDER BY a NULLS FIRST;",
        "SELECT a FROM t ORDER BY a DESC NULLS LAST, b ASC NULLS FIRST;",
        "SELECT a FROM t ORDER BY a OFFSET 3;",
        "SELECT a FROM t ORDER BY a LIMIT 2 OFFSET 3;",
        "SELECT COUNT(DISTINCT a) FROM t;",
        "SELECT COUNT(DISTINCT a + 1), COUNT(DISTINCT b) FROM t;",
        "SELECT SUM(x) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t;",
        "SELECT SUM(x) OVER (ORDER BY k RANGE UNBOUNDED PRECEDING) FROM t;",
        "SELECT SUM(x) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t;",
        "SELECT SUM(x) OVER (ORDER BY k DESC RANGE 2 PRECEDING) FROM t;",
        "EXPLAIN SELECT a FROM t WHERE a > 1 ORDER BY a LIMIT 1;",
        "EXPLAIN WITH c AS (SELECT 1 AS x) SELECT x FROM c UNION ALL SELECT 2;",
        "SHOW TABLES;",
        "SHOW DATABASES;",
        "show tables;",
        "DESCRIBE t;",
        "DESCRIBE db1.t;",
        "describe \"My Table\";",
    ];

    /// Must be rejected as [`PinkDbError::Parse`] (the invalid list below).
    const INVALID: &[&str] = &[
        "INSERT cell did not evaluate to one value",
        "INSERT INTO t2 VALUES",
        "CREATE TABLE t (left INTEGER);",
        "CREATE TABLE t (right INTEGER);",
        "CREATE TABLE left (x INTEGER);",
        "SELECT * FROM left;",
        "SELECT * FROM right;",
        "SELECT 1 AS left;",
        "SELECT 1 AS right;",
        "SELECT * FROM (SELECT * FROM emp);",
        "INSERT INTO wt VALUES ",
        "SELECT/HAVING expression must be an aggregate or appear in GROUP BY",
        "UPDATE needs at least one SET assignment",
        "SELECT needs at least one projection",
        "SELECT",
        "CREATE",
        "INSERT",
        "WITH",
        "UPDATE",
        "DELETE",
        "DROP",
        "select DISTINCT from t WHERE",
        "select `x`",
        "select 'abc",
        "INSERT needs at least one row",
        "SELECT * FROM a LEFT b ON a.id = b.id;",
        "SELECT * FROM a JOIN b USING (id);",
        "SELECT * FROM a NATURAL JOIN b;",
        "SELECT FROM;",
        "SELECT 1 SELECT 2;",
        "CREATE TABLE t (a NOPE);",
        "SELECT 1 LIMIT -1;",
        "SELECT DATE '2023-02-29';",
        "SELECT DATE 123;",
        "SELECT CASE WHEN 1 THEN 2;",
        "SELECT CASE END FROM t;",
        "UPDATE t SET;",
        "DELETE t;",
        "select",
        "with",
        "update-where",
        "delete-where",
        "SELECT left FROM t;",
        "SELECT a AS select FROM t;",
        "SELECT 1e+x;",
        "SELECT é FROM t;",
        "SELECT café FROM t;",
        "SELECT 'abc",
        "SELECT '' 'x';",
        "SELECT 1 /* unterminated",
        "SELECT a/*x",
        "SELECT 5.;",
        "SELECT 1.5.6;",
        "SELECT 1.e2;",
        "SELECT t.5;",
        "SELECT 5.e2;",
        "SELECT 99999999999999999999;",
        "SELECT -9223372036854775808;",
        "SELECT 1 LIMIT 1e2;",
        "SELECT `x`;",
        "SELECT 1)",
        "SELECT (1;",
        "SELECT ,1;",
        "SELECT 1,;",
        "SELECT a = b = c;",
        "SELECT a < b < c;",
        "SELECT a IS b FROM t;",
        "SELECT a BETWEEN 1 FROM t;",
        "SELECT a IN 1;",
        "SELECT a LIKE;",
        "SELECT a NOT b FROM t;",
        "SELECT f(*, 1);",
        "SELECT COALESCE();",
        "SELECT coalesce(a) OVER ();",
        "SELECT CAST(x AS nope);",
        "SELECT CAST(x);",
        "SELECT CASE END;",
        "SELECT CASE WHEN a THEN b ELSE c;",
        "SELECT TIMESTAMP 123;",
        "SELECT a, t.* FROM t;",
        "SELECT *, a FROM t;",
        "SELECT a, * FROM t;",
        "SELECT (*) FROM t;",
        "SELECT DISTINCT;",
        "SELECT a FROM t ORDER BY a ASC DESC;",
        "SELECT 1 LIMIT 1.5;",
        "SELECT 1 LIMIT x;",
        "SELECT a FROM;",
        "SELECT a FROM t WHERE;",
        "SELECT a FROM t GROUP BY;",
        "SELECT a FROM t ORDER BY;",
        "SELECT a FROM t HAVING;",
        "SELECT * FROM (SELECT 1);",
        "SELECT * FROM (1);",
        "WITH a AS SELECT 1 SELECT * FROM a;",
        "SELECT 1 UNION SELECT;",
        "SELECT rank() OVER;",
        "SELECT rank() OVER (PARTITION BY) FROM t;",
        "SELECT rank() OVER (ORDER BY) FROM t;",
        "CREATE TABLE t ();",
        "CREATE TABLE t (a INT,);",
        "CREATE TABLE t (a);",
        "CREATE TABLE select (a INT);",
        "INSERT INTO t (a,) VALUES (1);",
        "INSERT INTO t;",
        "INSERT INTO t VALUES;",
        "UPDATE t SET a;",
        "DELETE FROM;",
        "DROP t;",
        "DROP;",
        "CREATE VIEW v AS SELECT;",
        "FROM t SELECT 1;",
        "SELECT 1; SELECT;",
        "SELECT DISTINCT ON (a) b FROM t;",
        "SELECT 1e+;",
        "SELECT 1e-;",
        // Multi-database syntax errors.
        "CREATE DATABASE;",
        "DROP DATABASE;",
        "USE;",
        "USE 1;",
        "USE main extra;",
        "CREATE DATABASE a b;",
        "CREATE DATABASE db1.t;",
        "DROP DATABASE db1.t;",
        "CREATE TABLE db1.;",
        "INSERT INTO db1.;",
        "UPDATE db1.;",
        "DELETE FROM db1.;",
        "SELECT * FROM db1.;",
        "SELECT * FROM db1.t.col;",
        "SELECT * FROM db1.t.*;",
        "SELECT a FROM db1.t.;",
        "CREATE TABLE db1.t.col (a INTEGER);",
        "CREATE VIEW db1.t.col AS SELECT 1;",
        "SELECT db1.t.a FROM db1.t;",
        "SELECT db1.t.* FROM db1.t;",
        "SELECT a FROM db1.t WHERE db1.t.a = 1;",
        "DROP TABLE db1.;",
        "CREATE VIEW db1.;",
        "SELECT * FROM db1.t JOIN db2.u;",
        // LOAD EXTENSION syntax errors: missing or non-literal path.
        "LOAD EXTENSION;",
        "LOAD EXTENSION ;",
        "LOAD EXTENSION x;",
        "LOAD EXTENSION 1;",
        "LOAD EXTENSION 'x' 'y';",
        "LOAD EXTENSION \"x\";",
        "LOAD;",
        "LOAD 'x';",
        // NUL bytes are rejected inside quoted identifiers (the C bindings
        // would silently truncate them at the NUL); string literals stay free
        // to carry NUL bytes (see the parser-level tests).
        "SELECT \"a\0b\";",
        "CREATE TABLE \"a\0b\" (x INTEGER);",
        "SELECT 1 AS \"a\0b\";",
        // SQL completeness syntax errors: `EXPLAIN` takes a SELECT, SHOW only
        // knows TABLES/DATABASES, and RANGE offsets are integer literals.
        "EXPLAIN INSERT INTO t VALUES (1);",
        "EXPLAIN;",
        "SHOW;",
        "SHOW TABLESS;",
        "SHOW INDEXES;",
        "DESCRIBE;",
        "SELECT SUM(x) OVER (ORDER BY k RANGE BETWEEN 0.5 PRECEDING AND CURRENT ROW) FROM t;",
        "SELECT SUM(x) OVER (ORDER BY k RANGE BETWEEN -1 PRECEDING AND CURRENT ROW) FROM t;",
        "INSERT INTO t SELECT;",
        "CREATE TABLE t AS;",
        "CREATE TABLE IF t (a INTEGER);",
        "DROP TABLE IF t;",
    ];

    /// Every valid entry parses, and the raw statement split agrees with the
    /// parser (same count; every split text parses to exactly one statement).
    #[test]
    fn valid_corpus_parses() {
        for sql in VALID {
            let stmts = parse(sql).unwrap_or_else(|e| panic!("rejected {sql:?}: {e}"));
            let texts = super::super::statement_texts(sql)
                .unwrap_or_else(|e| panic!("split failed for {sql:?}: {e}"));
            assert_eq!(
                stmts.len(),
                texts.len(),
                "statement/split count mismatch for {sql:?}"
            );
            for text in &texts {
                assert_eq!(
                    parse(text)
                        .unwrap_or_else(|e| panic!("rejected split {text:?}: {e}"))
                        .len(),
                    1,
                    "split text {text:?} of {sql:?}"
                );
            }
        }
    }

    /// Every invalid entry is rejected, and always as [`PinkDbError::Parse`].
    #[test]
    fn invalid_corpus_is_rejected_with_parse_errors() {
        for sql in INVALID {
            match parse(sql) {
                Err(PinkDbError::Parse(_)) => {}
                other => panic!("expected a Parse error for {sql:?}, got {other:?}"),
            }
        }
    }

    // ---------- lexical rules ----------

    #[test]
    fn keywords_are_case_insensitive() {
        assert_eq!(parse_one("select 1;"), parse_one("SeLeCt 1;"));
        assert_eq!(
            parse_one("select distinct a from t where a = 1;"),
            parse_one("SELECT DISTINCT a FROM t WHERE a = 1;")
        );
    }

    #[test]
    fn identifiers_keep_keyword_prefixes_and_boundaries() {
        let Statement::Select(sel) =
            parse_one("SELECT selected, select_, s_elect, ALLS, orderby FROM t;")
        else {
            panic!()
        };
        let names: Vec<&str> = sel
            .projections
            .iter()
            .map(|p| match p {
                Projection::Expr {
                    expr: Expr::Column { name, .. },
                    ..
                } => name.as_str(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(names, ["selected", "select_", "s_elect", "ALLS", "orderby"]);
        // Reserved keywords are never identifiers, bare or aliased.
        for sql in [
            "SELECT left FROM t;",
            "SELECT a AS select FROM t;",
            "CREATE TABLE select (a INT);",
        ] {
            assert!(matches!(parse(sql), Err(PinkDbError::Parse(_))), "{sql}");
        }
    }

    #[test]
    fn number_formats_and_i64_overflow() {
        assert_eq!(lit("SELECT .5;"), Value::Double(0.5));
        assert_eq!(lit("SELECT 1e+2;"), Value::Double(100.0));
        assert_eq!(lit("SELECT 1E-2;"), Value::Double(0.01));
        assert_eq!(lit("SELECT 0.5E-3;"), Value::Double(0.0005));
        assert_eq!(lit("SELECT 3e2;"), Value::Double(300.0));
        // i32-fitting integers stay INTEGER, larger ones BIGINT.
        assert_eq!(lit("SELECT 2147483647;"), Value::Integer(i32::MAX));
        assert_eq!(lit("SELECT 2147483648;"), Value::Bigint(2147483648));
        assert_eq!(lit("SELECT 9223372036854775807;"), Value::Bigint(i64::MAX));
        // `1.` is not a float (the dot stays a token), a bare exponent marker
        // is not a number, and out-of-i64-range text is a parse error — so
        // i64::MIN cannot be spelled as a literal.
        for sql in [
            "SELECT 1.;",
            "SELECT 1.e2;",
            "SELECT 5.;",
            "SELECT 1e+;",
            "SELECT 9223372036854775808;",
            "SELECT 99999999999999999999;",
            "SELECT -9223372036854775808;",
        ] {
            assert!(matches!(parse(sql), Err(PinkDbError::Parse(_))), "{sql}");
        }
    }

    #[test]
    fn string_escaping() {
        assert_eq!(lit("SELECT 'it''s';"), Value::Varchar("it's".to_string()));
        assert_eq!(lit("SELECT '';"), Value::Varchar(String::new()));
        assert_eq!(lit("SELECT '''';"), Value::Varchar("'".to_string()));
        assert!(matches!(parse("SELECT 'abc"), Err(PinkDbError::Parse(_))));
    }

    #[test]
    fn comments_are_skipped_and_unterminated_blocks_error() {
        assert_eq!(lit("SELECT 1 -- hi\n;"), Value::Integer(1));
        assert_eq!(lit("SELECT -- comment\n1;"), Value::Integer(1));
        assert_eq!(lit("SELECT /* c */ 1;"), Value::Integer(1));
        // A line comment runs to end-of-input when there is no newline.
        assert!(parse("SELECT 1 -- trailing").is_ok());
        assert!(parse("SELECT a/*x*/+1;").is_ok());
        for sql in ["SELECT 1 /* unterminated", "SELECT a/*x"] {
            assert!(matches!(parse(sql), Err(PinkDbError::Parse(_))), "{sql}");
        }
    }

    #[test]
    fn semicolons_split_empty_and_trailing_statements() {
        assert!(parse("").unwrap().is_empty());
        assert!(parse(";").unwrap().is_empty());
        assert!(parse(";;;").unwrap().is_empty());
        assert_eq!(parse("SELECT 1; SELECT 2;").unwrap().len(), 2);
        assert_eq!(parse(";;SELECT 1;;;").unwrap().len(), 1);
        // The final `;` is optional.
        assert_eq!(parse("SELECT 1").unwrap().len(), 1);
        // `;` inside strings, quoted identifiers and comments never splits.
        let stmts = parse("SELECT ';', \"a;b\"; -- c;\n/* d; */ SELECT 2").unwrap();
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn bad_sql_errors_are_parse_kind() {
        for sql in [
            "select `x`",
            "select 'abc",
            "SELECT FROM;",
            "SELECT 1 SELECT 2;",
            "CREATE TABLE t (a NOPE);",
            "SELECT 1 LIMIT -1;",
            "SELECT 1)",
            "SELECT (1;",
        ] {
            match parse(sql) {
                Err(PinkDbError::Parse(_)) => {}
                other => panic!("expected a Parse error for {sql:?}, got {other:?}"),
            }
        }
    }

    // ---------- operator precedence and literals ----------

    #[test]
    fn comparison_operators_and_literals() {
        let Statement::Select(sel) =
            parse_one("SELECT 1 <> 2, 1 != 2, 2.5, 'x', 3e2, TRUE, FALSE, NULL;")
        else {
            panic!()
        };
        let exprs: Vec<&Expr> = sel
            .projections
            .iter()
            .map(|p| match p {
                Projection::Expr { expr, .. } => expr,
                other => panic!("{other:?}"),
            })
            .collect();
        assert!(matches!(
            exprs[0],
            Expr::Binary {
                op: BinaryOp::NotEq,
                ..
            }
        ));
        assert!(matches!(
            exprs[1],
            Expr::Binary {
                op: BinaryOp::NotEq,
                ..
            }
        ));
        assert!(matches!(exprs[2], Expr::Literal(Value::Double(v)) if *v == 2.5));
        assert!(matches!(exprs[3], Expr::Literal(Value::Varchar(v)) if v == "x"));
        assert!(matches!(exprs[4], Expr::Literal(Value::Double(v)) if *v == 300.0));
        assert!(matches!(exprs[5], Expr::Literal(Value::Boolean(true))));
        assert!(matches!(exprs[6], Expr::Literal(Value::Boolean(false))));
        assert!(matches!(exprs[7], Expr::Literal(Value::Null)));
    }

    #[test]
    fn precedence_mul_before_add() {
        let s = parse_one("SELECT 1 + 2 * 3;");
        let Statement::Select(sel) = s else { panic!() };
        assert_eq!(sel.projections.len(), 1);
        // 1 + (2 * 3)
        match &sel.projections[0] {
            Projection::Expr { expr, .. } => match expr {
                Expr::Binary {
                    op: BinaryOp::Add,
                    right,
                    ..
                } => {
                    assert!(matches!(
                        **right,
                        Expr::Binary {
                            op: BinaryOp::Multiply,
                            ..
                        }
                    ));
                }
                _ => panic!("wrong shape: {expr:?}"),
            },
            _ => panic!(),
        }
    }

    #[test]
    fn precedence_concat_between_comparison_and_additive() {
        // `'a' || 'b' = 'ab'` parses as `('a' || 'b') = 'ab'`.
        let Expr::Binary {
            op: BinaryOp::Eq,
            left,
            ..
        } = projection_expr(parse_one("SELECT 'a' || 'b' = 'ab';"))
        else {
            panic!()
        };
        assert!(matches!(
            *left,
            Expr::Binary {
                op: BinaryOp::Concat,
                ..
            }
        ));
        // `1 + 2 || '3'` parses as `(1 + 2) || '3'` (additive is tighter).
        let Expr::Binary {
            op: BinaryOp::Concat,
            left,
            ..
        } = projection_expr(parse_one("SELECT 1 + 2 || '3';"))
        else {
            panic!()
        };
        assert!(matches!(
            *left,
            Expr::Binary {
                op: BinaryOp::Add,
                ..
            }
        ));
        // Left-associative: `('a' || 'b') || 'c'`.
        let Expr::Binary { left, .. } = projection_expr(parse_one("SELECT 'a' || 'b' || 'c';"))
        else {
            panic!()
        };
        assert!(matches!(
            *left,
            Expr::Binary {
                op: BinaryOp::Concat,
                ..
            }
        ));
    }

    #[test]
    fn extract_call_desugars_to_function() {
        let Expr::Function { name, args, .. } =
            projection_expr(parse_one("SELECT extract(year FROM d);"))
        else {
            panic!()
        };
        assert_eq!(name, "extract");
        assert_eq!(args.len(), 2);
        assert!(matches!(&args[0], Expr::Literal(Value::Varchar(p)) if p == "year"));
        assert!(matches!(&args[1], Expr::Column { name, .. } if name == "d"));
        // A string part is accepted; `extract` stays a usable identifier.
        let Expr::Function { args, .. } =
            projection_expr(parse_one("SELECT extract('dow' FROM d);"))
        else {
            panic!()
        };
        assert!(matches!(&args[0], Expr::Literal(Value::Varchar(p)) if p == "dow"));
        let Expr::Column { name, .. } = projection_expr(parse_one("SELECT extract FROM t;")) else {
            panic!()
        };
        assert_eq!(name, "extract");
        // A plain two-argument call still parses (date_part alias).
        let Expr::Function { name, .. } = projection_expr(parse_one("SELECT extract('year', d);"))
        else {
            panic!()
        };
        assert_eq!(name, "extract");
    }

    #[test]
    fn precedence_comparison_below_additive() {
        // `1 + 2 = 3` parses as `(1 + 2) = 3`.
        let Expr::Binary {
            op: BinaryOp::Eq,
            left,
            ..
        } = projection_expr(parse_one("SELECT 1 + 2 = 3;"))
        else {
            panic!()
        };
        assert!(matches!(
            *left,
            Expr::Binary {
                op: BinaryOp::Add,
                ..
            }
        ));
        // `NOT a = b` parses as `NOT (a = b)` (NOT sits above comparison).
        let Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } = projection_expr(parse_one("SELECT NOT a = b;"))
        else {
            panic!()
        };
        assert!(matches!(
            *expr,
            Expr::Binary {
                op: BinaryOp::Eq,
                ..
            }
        ));
        // Subtraction is left-associative: `(10 - 3) - 2`.
        let Expr::Binary { left, .. } = projection_expr(parse_one("SELECT 10 - 3 - 2;")) else {
            panic!()
        };
        assert!(matches!(
            *left,
            Expr::Binary {
                op: BinaryOp::Subtract,
                ..
            }
        ));
    }

    #[test]
    fn and_binds_tighter_than_or() {
        let Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } = projection_expr(parse_one("SELECT a AND b OR c AND d;"))
        else {
            panic!()
        };
        assert!(matches!(
            *left,
            Expr::Binary {
                op: BinaryOp::And,
                ..
            }
        ));
        assert!(matches!(
            *right,
            Expr::Binary {
                op: BinaryOp::And,
                ..
            }
        ));
    }

    // ---------- statement shapes ----------

    #[test]
    fn where_between_and_is_null() {
        let s = parse_one("SELECT a FROM t WHERE x BETWEEN 1 AND 5 AND y IS NOT NULL;");
        let Statement::Select(sel) = s else { panic!() };
        assert!(sel.where_clause.is_some());
        assert!(sel.from.is_some());
    }

    #[test]
    fn insert_and_create_shapes() {
        assert!(matches!(
            parse_one("CREATE TABLE t (a BIGINT, b VARCHAR);"),
            Statement::CreateTable { .. }
        ));
        assert!(matches!(
            parse_one("INSERT INTO t VALUES (1, 'x'), (2, 'y');"),
            Statement::Insert { .. }
        ));
    }

    #[test]
    fn join_parses_for_wave3() {
        let s = parse_one("SELECT * FROM a JOIN b ON a.id = b.id;");
        let Statement::Select(sel) = s else { panic!() };
        let join = sel.from.unwrap().joins.into_iter().next().unwrap();
        assert_eq!(join.kind, JoinKind::Inner);
        let s = parse_one("SELECT * FROM a INNER JOIN b ON a.id = b.id;");
        let Statement::Select(sel) = s else { panic!() };
        assert_eq!(
            sel.from.unwrap().joins.into_iter().next().unwrap().kind,
            JoinKind::Inner
        );
    }

    #[test]
    fn join_kinds_parse_and_unsupported_joins_error() {
        // LEFT / LEFT OUTER / RIGHT / RIGHT OUTER / FULL / FULL OUTER,
        // case-insensitive.
        for (sql, want) in [
            (
                "SELECT * FROM a LEFT JOIN b ON a.id = b.id;",
                JoinKind::Left,
            ),
            (
                "SELECT * FROM a left outer join b ON a.id = b.id;",
                JoinKind::Left,
            ),
            (
                "SELECT * FROM a RIGHT JOIN b ON a.id = b.id;",
                JoinKind::Right,
            ),
            (
                "SELECT * FROM a right OUTER join b ON a.id = b.id;",
                JoinKind::Right,
            ),
            (
                "SELECT * FROM a FULL JOIN b ON a.id = b.id;",
                JoinKind::Full,
            ),
            (
                "SELECT * FROM a full outer join b ON a.id = b.id;",
                JoinKind::Full,
            ),
        ] {
            let Statement::Select(sel) = parse_one(sql) else {
                panic!()
            };
            assert_eq!(
                sel.from.unwrap().joins.into_iter().next().unwrap().kind,
                want,
                "{sql}"
            );
        }
        // CROSS JOIN parses with no ON condition.
        let Statement::Select(sel) = parse_one("SELECT * FROM a CROSS JOIN b;") else {
            panic!()
        };
        let join = sel.from.unwrap().joins.into_iter().next().unwrap();
        assert_eq!(join.kind, JoinKind::Cross);
        assert!(join.on.is_none());
        // Multiple joins in one FROM are a chain (left-deep).
        let Statement::Select(sel) =
            parse_one("SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON c.id = b.id;")
        else {
            panic!()
        };
        assert_eq!(sel.from.unwrap().joins.len(), 2);
        // LEFT/RIGHT without JOIN, CROSS ... ON, USING / NATURAL error.
        for sql in [
            "SELECT * FROM a LEFT b ON a.id = b.id;",
            "SELECT * FROM a CROSS JOIN b ON a.id = b.id;",
            "SELECT * FROM a JOIN b USING (id);",
            "SELECT * FROM a NATURAL JOIN b;",
        ] {
            assert!(matches!(parse(sql), Err(PinkDbError::Parse(_))), "{sql}");
        }
    }

    #[test]
    fn date_time_literals_and_types() {
        let s = parse_one("SELECT DATE '2024-02-29', TIMESTAMP '2024-03-15 12:00:00.5';");
        let Statement::Select(sel) = s else { panic!() };
        assert_eq!(sel.projections.len(), 2);
        assert!(matches!(
            parse("SELECT DATE '2023-02-29';"),
            Err(PinkDbError::Parse(_))
        ));
        assert!(matches!(
            parse("SELECT DATE 123;"),
            Err(PinkDbError::Parse(_))
        ));
        assert!(matches!(
            parse_one("CREATE TABLE t (d DATE, ts TIMESTAMP);"),
            Statement::CreateTable { .. }
        ));
    }

    #[test]
    fn window_shapes() {
        let s = parse_one("SELECT rank() OVER (PARTITION BY a ORDER BY b DESC) FROM t;");
        let Statement::Select(sel) = s else { panic!() };
        match &sel.projections[0] {
            Projection::Expr {
                expr: Expr::Window { name, over, .. },
                ..
            } => {
                assert_eq!(name, "rank");
                assert_eq!(over.partition.len(), 1);
                assert_eq!(over.order.len(), 1);
                assert!(over.order[0].desc);
                assert!(over.frame.is_none());
            }
            _ => panic!(),
        }
        let s = parse_one("SELECT sum(x) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t;");
        let Statement::Select(sel) = s else { panic!() };
        assert!(matches!(
            &sel.projections[0],
            Projection::Expr {
                expr: Expr::Window { .. },
                ..
            }
        ));
        // RANGE frames parse now (offsets on the single ORDER BY key).
        let s = parse_one(
            "SELECT sum(x) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t;",
        );
        let Statement::Select(sel) = s else { panic!() };
        match &sel.projections[0] {
            Projection::Expr {
                expr: Expr::Window { over, .. },
                ..
            } => {
                let frame = over.frame.as_ref().expect("frame parsed");
                assert_eq!(frame.mode, FrameMode::Range);
                assert_eq!(frame.start, FrameBound::Preceding(1));
                assert_eq!(frame.end, FrameBound::Following(1));
            }
            _ => panic!(),
        }
        // `ROWS` keeps the row-counting mode.
        let s = parse_one("SELECT sum(x) OVER (ORDER BY k ROWS 2 PRECEDING) FROM t;");
        let Statement::Select(sel) = s else { panic!() };
        match &sel.projections[0] {
            Projection::Expr {
                expr: Expr::Window { over, .. },
                ..
            } => {
                let frame = over.frame.as_ref().expect("frame parsed");
                assert_eq!(frame.mode, FrameMode::Rows);
                assert_eq!(frame.start, FrameBound::Preceding(2));
                assert_eq!(frame.end, FrameBound::CurrentRow);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn case_shapes() {
        // Searched form.
        let s = parse_one(
            "SELECT CASE WHEN a > 1 THEN 'big' WHEN a = 1 THEN 'one' ELSE 'other' END FROM t;",
        );
        let Statement::Select(sel) = s else { panic!() };
        assert!(matches!(
            &sel.projections[0],
            Projection::Expr { expr: Expr::Case { branches, else_ }, .. }
            if branches.len() == 2 && else_.is_some()
        ));
        // Simple form parses (desugars in the same node shape).
        let s = parse_one("SELECT CASE a WHEN 1 THEN 'one' END FROM t;");
        let Statement::Select(sel) = s else { panic!() };
        assert!(matches!(
            &sel.projections[0],
            Projection::Expr { expr: Expr::Case { branches, else_ }, .. }
            if branches.len() == 1 && else_.is_none()
        ));
        assert!(matches!(
            parse("SELECT CASE WHEN 1 THEN 2;"),
            Err(PinkDbError::Parse(_))
        ));
        assert!(matches!(
            parse("SELECT CASE END FROM t;"),
            Err(PinkDbError::Parse(_))
        ));
    }

    #[test]
    fn update_delete_shapes() {
        match parse_one("UPDATE t SET a = 1, b = a + 2 WHERE a > 0;") {
            Statement::Update {
                db,
                table,
                sets,
                where_clause,
            } => {
                assert_eq!(db, None);
                assert_eq!(table, "t");
                assert_eq!(sets.len(), 2);
                assert!(where_clause.is_some());
            }
            _ => panic!(),
        }
        match parse_one("UPDATE db1.t SET a = 1;") {
            Statement::Update { db, table, .. } => {
                assert_eq!(db, Some("db1".to_string()));
                assert_eq!(table, "t");
            }
            _ => panic!(),
        }
        match parse_one("DELETE FROM t;") {
            Statement::Delete {
                db,
                table,
                where_clause,
            } => {
                assert_eq!(db, None);
                assert_eq!(table, "t");
                assert!(where_clause.is_none());
            }
            _ => panic!(),
        }
        assert!(matches!(parse("UPDATE t SET;"), Err(PinkDbError::Parse(_))));
        assert!(matches!(parse("DELETE t;"), Err(PinkDbError::Parse(_))));
    }

    #[test]
    fn multi_database_statements() {
        assert!(matches!(
            parse_one("CREATE DATABASE analytics;"),
            Statement::CreateDatabase { name, .. } if name == "analytics"
        ));
        assert!(matches!(
            parse_one("DROP DATABASE analytics;"),
            Statement::DropDatabase { name, .. } if name == "analytics"
        ));
        assert!(matches!(
            parse_one("USE analytics;"),
            Statement::Use { name } if name == "analytics"
        ));
        // Qualified DDL/DML.
        match parse_one("CREATE TABLE db1.t (a INTEGER);") {
            Statement::CreateTable { db, name, .. } => {
                assert_eq!(db, Some("db1".to_string()));
                assert_eq!(name, "t");
            }
            _ => panic!(),
        }
        match parse_one("INSERT INTO db1.t VALUES (1);") {
            Statement::Insert { db, table, .. } => {
                assert_eq!(db, Some("db1".to_string()));
                assert_eq!(table, "t");
            }
            _ => panic!(),
        }
        match parse_one("CREATE VIEW db1.v AS SELECT 1;") {
            Statement::CreateView { db, name, .. } => {
                assert_eq!(db, Some("db1".to_string()));
                assert_eq!(name, "v");
            }
            _ => panic!(),
        }
        match parse_one("DROP TABLE db1.t;") {
            Statement::DropTable { db, name, .. } => {
                assert_eq!(db, Some("db1".to_string()));
                assert_eq!(name, "t");
            }
            _ => panic!(),
        }
        match parse_one("DROP VIEW db1.v;") {
            Statement::DropView { db, name } => {
                assert_eq!(db, Some("db1".to_string()));
                assert_eq!(name, "v");
            }
            _ => panic!(),
        }
        // Qualified FROM source.
        let Statement::Select(sel) = parse_one("SELECT a FROM db1.t;") else {
            panic!()
        };
        match sel.from.unwrap().source {
            FromSource::Table { db, name } => {
                assert_eq!(db, Some("db1".to_string()));
                assert_eq!(name, "t");
            }
            other => panic!("{other:?}"),
        }
        // Three-part references are rejected with a clear message.
        let e = parse("SELECT db1.t.a FROM db1.t;").unwrap_err();
        assert!(e.to_string().contains("three-part"), "{e}");
        let e = parse("SELECT db1.t.* FROM db1.t;").unwrap_err();
        assert!(e.to_string().contains("three-part"), "{e}");
    }

    #[test]
    fn txn_statements() {
        assert!(matches!(parse_one("BEGIN;"), Statement::Begin));
        assert!(matches!(parse_one("BEGIN TRANSACTION;"), Statement::Begin));
        assert!(matches!(parse_one("COMMIT;"), Statement::Commit));
        assert!(matches!(
            parse_one("ROLLBACK TRANSACTION;"),
            Statement::Rollback
        ));
    }

    #[test]
    fn with_in_exists_shapes() {
        let s = parse_one("WITH a AS (SELECT 1 AS x), b(y) AS (SELECT x FROM a) SELECT * FROM b;");
        let Statement::Select(sel) = s else { panic!() };
        assert_eq!(sel.with.len(), 2);
        assert_eq!(sel.with[1].cols, Some(vec!["y".to_string()]));

        let s = parse_one("SELECT * FROM (SELECT 1 AS x) AS s;");
        let Statement::Select(sel) = s else { panic!() };
        assert!(matches!(sel.from.unwrap().source, FromSource::Derived(_)));

        let s = parse_one("SELECT a FROM t WHERE a IN (SELECT b FROM u) AND EXISTS (SELECT 1);");
        let Statement::Select(sel) = s else { panic!() };
        assert!(sel.where_clause.is_some());

        // IN-list desugars to an OR chain.
        let s = parse_one("SELECT * FROM t WHERE a IN (1, 2);");
        let Statement::Select(sel) = s else { panic!() };
        assert!(matches!(
            sel.where_clause,
            Some(Expr::Binary {
                op: BinaryOp::Or,
                ..
            })
        ));

        // Scalar subquery in an expression.
        let s = parse_one("SELECT (SELECT MAX(x) FROM u) + 1;");
        let Statement::Select(sel) = s else { panic!() };
        assert_eq!(sel.projections.len(), 1);
    }

    /// `LOAD EXTENSION '<path>'` keeps the path exactly as written and
    /// keywords are case-insensitive; anything but a string-literal path is a
    /// parse error.
    #[test]
    fn load_extension_keeps_the_path_verbatim() {
        for (sql, path) in [
            ("LOAD EXTENSION '/a b/c.dylib';", "/a b/c.dylib"),
            ("load extension 'x.so';", "x.so"),
            ("LOAD EXTENSION 'it''s.dylib';", "it's.dylib"),
        ] {
            assert_eq!(
                parse(sql).unwrap(),
                vec![Statement::LoadExtension {
                    path: path.to_string()
                }],
                "{sql:?}"
            );
        }
        for sql in [
            "LOAD EXTENSION;",
            "LOAD EXTENSION ;",
            "LOAD EXTENSION ext;",
            "LOAD EXTENSION 1;",
            "LOAD EXTENSION 'x' 'y';",
            "LOAD EXTENSION \"x\";",
        ] {
            assert!(matches!(parse(sql), Err(PinkDbError::Parse(_))), "{sql}");
        }
    }

    /// NUL bytes inside quoted identifiers are rejected at parse time (the C
    /// bindings would silently truncate them at the NUL); valid quoted
    /// identifiers are unchanged and NULs stay legal in string literals, where
    /// they are value bytes rather than names.
    #[test]
    fn nul_in_quoted_identifier_is_rejected() {
        for sql in [
            "SELECT \"a\0b\";",
            "CREATE TABLE \"a\0b\" (x INTEGER);",
            "SELECT 1 AS \"a\0b\";",
            "SELECT * FROM \"a\0b\";",
            "USE \"a\0b\";",
        ] {
            let e = parse(sql).expect_err("must reject");
            assert!(matches!(e, PinkDbError::Parse(_)), "{sql:?}: {e:?}");
            assert!(
                e.to_string().contains("identifier contains a NUL byte"),
                "{sql:?}: {e}"
            );
        }
        // The error points at the *opening* quote of the identifier.
        let e = parse("SELECT \"a\0b\";").unwrap_err().to_string();
        assert_eq!(e, "parse error at offset 7: identifier contains a NUL byte");
        // Valid quoted identifiers keep their text (including `""` escapes).
        let Statement::Select(sel) =
            parse_one("SELECT \"select\", \"\", \"we\"\"ird\" FROM \"tab le\";")
        else {
            panic!()
        };
        let names: Vec<&str> = sel
            .projections
            .iter()
            .map(|p| match p {
                Projection::Expr {
                    expr: Expr::Column { name, .. },
                    ..
                } => name.as_str(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(names, ["select", "", "we\"ird"]);
        match sel.from.unwrap().source {
            FromSource::Table { db: None, name } => assert_eq!(name, "tab le"),
            other => panic!("{other:?}"),
        }
        // A NUL inside a string literal is value data, not a name: accepted.
        assert_eq!(lit("SELECT 'a\0b';"), Value::Varchar("a\0b".to_string()));
    }

    /// The four three-part shapes fail with a clear, specific message, not a
    /// generic PEG error.
    #[test]
    fn three_part_reference_errors_are_clear() {
        let cases = [
            "SELECT * FROM db1.t.*;",
            "SELECT a FROM db1.t.;",
            "CREATE TABLE db1.t.col (a INTEGER);",
            "CREATE VIEW db1.t.col AS SELECT 1;",
        ];
        for sql in cases {
            let e = parse(sql).unwrap_err().to_string();
            assert!(
                e.contains("three-part names (db.table.col) are not supported"),
                "{sql}: {e}"
            );
        }
    }

    // ---------- nesting-depth guard ----------

    fn nested_calls(name: &str, depth: usize) -> String {
        format!(
            "SELECT {}1{};",
            format!("{name}(").repeat(depth),
            ")".repeat(depth)
        )
    }

    fn nested_coalesce(depth: usize) -> String {
        format!(
            "SELECT {}1{};",
            "COALESCE(".repeat(depth),
            ", 2)".repeat(depth)
        )
    }

    fn nested_parens(depth: usize) -> String {
        format!("SELECT {}1{};", "(".repeat(depth), ")".repeat(depth))
    }

    fn nested_case(depth: usize) -> String {
        format!(
            "SELECT {}1{};",
            "CASE WHEN 1 THEN ".repeat(depth),
            " END".repeat(depth)
        )
    }

    fn nested_subqueries(depth: usize) -> String {
        format!("SELECT {}1{};", "(SELECT ".repeat(depth), ")".repeat(depth))
    }

    /// Regression for exponential backtracking: two `ident_expr` alternatives
    /// used to parse the same `fn_args` twice, doubling work per level — 24
    /// nested calls took ~2 minutes. Arguments are now parsed once, so this
    /// must stay well under a second in debug builds.
    #[test]
    fn nested_calls_parse_in_linear_time() {
        let inputs = [
            nested_calls("f", MAX_NESTING_DEPTH),
            nested_calls("upper", MAX_NESTING_DEPTH),
            nested_coalesce(MAX_NESTING_DEPTH),
        ];
        let start = std::time::Instant::now();
        for sql in &inputs {
            parse(sql).unwrap_or_else(|e| panic!("rejected {sql:?}: {e}"));
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "nested calls took {elapsed:?} — exponential backtracking regressed?"
        );
    }

    /// Inputs beyond the cap are rejected with a clean parse error instead of
    /// recursing until the stack overflows.
    #[test]
    fn nesting_above_cap_is_a_clean_error() {
        let too_deep = MAX_NESTING_DEPTH + 1;
        let inputs = [
            nested_parens(too_deep),
            nested_calls("f", too_deep),
            nested_coalesce(too_deep),
            nested_case(too_deep),
            nested_subqueries(too_deep),
            format!("SELECT {}1;", "NOT ".repeat(too_deep)),
            // Far above the cap: these would overflow the stack unguarded.
            nested_parens(500),
            nested_calls("f", 500),
            nested_coalesce(500),
            nested_subqueries(200),
        ];
        for sql in &inputs {
            match parse(sql) {
                Err(PinkDbError::Parse(msg)) => {
                    assert!(
                        msg.contains("nesting too deep"),
                        "unexpected error for too-deep input: {msg}"
                    );
                }
                other => panic!("too-deep input not rejected cleanly: {other:?}"),
            }
        }
    }

    /// Two levels below the text cap still parse. Nested subqueries are the
    /// exception: each level costs three AST nodes (Select, projection,
    /// scalar-subquery expression), so the AST-depth guard can reject the
    /// deepest nestings before the text cap does; both are clean `Parse`
    /// errors either way.
    #[test]
    fn nesting_just_below_cap_parses() {
        let depth = MAX_NESTING_DEPTH - 2;
        for sql in [
            nested_parens(depth),
            nested_calls("upper", depth),
            nested_coalesce(depth),
            nested_case(depth),
        ] {
            parse(&sql).unwrap_or_else(|e| panic!("rejected {sql:?}: {e}"));
        }
        for subquery_depth in [1, depth - 1, depth] {
            let sql = nested_subqueries(subquery_depth);
            match parse(&sql) {
                Ok(_) => {}
                Err(PinkDbError::Parse(msg)) => {
                    assert!(msg.contains("too deep"), "{sql:?}: {msg}");
                }
                other => panic!("too-deep input not rejected cleanly: {other:?}"),
            }
        }
    }

    /// The depth estimate ignores parens inside strings, quoted identifiers
    /// and comments, which the parser never recurses into.
    #[test]
    fn guard_ignores_nesting_inside_strings_and_comments() {
        let many = MAX_NESTING_DEPTH * 3;
        let sql = format!(
            "SELECT 'it''s {}', \"{}\" FROM t /* {} */ -- {}\n;",
            "(".repeat(many),
            "(".repeat(many),
            ")".repeat(many),
            ")".repeat(many),
        );
        parse(&sql).unwrap_or_else(|e| panic!("rejected: {e}"));
    }

    // ---------- COALESCE `*` arguments ----------

    /// COALESCE parses a plain expression list, so `*` is an ordinary
    /// expression in its arguments: the parser accepts these and the binder
    /// rejects them. A generic call keeps the leading-`*`-as-whole-argument-
    /// list rule (`f(*, 1)` is a parse error).
    #[test]
    fn coalesce_star_arguments_are_accepted() {
        for sql in [
            "SELECT COALESCE(*, 1);",
            "SELECT COALESCE(* + 1, 2);",
            "SELECT COALESCE(*/did, -1);",
            "SELECT coalesce(*, 1);",
            "SELECT CoAlEsCe(*/did, -1);",
            "SELECT COALESCE(*);",
        ] {
            parse(sql).unwrap_or_else(|e| panic!("rejected {sql:?}: {e}"));
        }
        for sql in ["SELECT f(*, 1);", "SELECT COALESCE();"] {
            assert!(matches!(parse(sql), Err(PinkDbError::Parse(_))), "{sql}");
        }
    }

    // ---------- seeded fuzzing ----------

    /// Deterministic splitmix64 PRNG for the seeded fuzz test (no new deps).
    struct SplitMix64(u64);

    impl SplitMix64 {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    /// Depth-bounded structured expression; includes comment/operator
    /// adjacency and COALESCE-with-`*` shapes on purpose.
    fn fuzz_expr(rng: &mut SplitMix64, depth: usize) -> String {
        if depth == 0 || rng.below(4) == 0 {
            let leaves = [
                "a", "b", "1", "007", "1.5", ".5e2", "'x'", "NULL", "TRUE", "\"a\"",
            ];
            return leaves[rng.below(leaves.len())].to_string();
        }
        let left = fuzz_expr(rng, depth - 1);
        let right = fuzz_expr(rng, depth - 1);
        match rng.below(14) {
            0 => format!("({left})"),
            1 => format!("NOT {left}"),
            2 => format!("-{left}"),
            3 => format!("{left} + {right}"),
            4 => format!("{left} AND {right}"),
            5 => format!("{left} = {right}"),
            6 => format!("upper({left})"),
            7 => format!("COALESCE({left}, {right})"),
            8 => format!("COALESCE(*, {left})"),
            9 => format!("CASE WHEN {left} THEN {right} ELSE {left} END"),
            10 => format!("{left} IN ({right})"),
            11 => format!("{left} -- c\n"),
            12 => format!("{left}/*x*/+{right}"),
            _ => format!("{left}- -{right}"),
        }
    }

    /// Structured random statement.
    fn fuzz_input(rng: &mut SplitMix64) -> String {
        match rng.below(8) {
            0 => format!("SELECT {};", fuzz_expr(rng, 3)),
            1 => format!(
                "SELECT {}, {} FROM t;",
                fuzz_expr(rng, 2),
                fuzz_expr(rng, 2)
            ),
            2 => format!("SELECT a FROM t WHERE {};", fuzz_expr(rng, 2)),
            3 => format!("SELECT {} FROM t ORDER BY a LIMIT 2;", fuzz_expr(rng, 2)),
            4 => format!(
                "INSERT INTO t VALUES ({}), ({});",
                fuzz_expr(rng, 1),
                fuzz_expr(rng, 1)
            ),
            5 => format!(
                "UPDATE t SET a = {} WHERE {};",
                fuzz_expr(rng, 1),
                fuzz_expr(rng, 1)
            ),
            6 => format!(
                "SELECT sum({}) OVER (PARTITION BY a ORDER BY b) FROM t;",
                fuzz_expr(rng, 1)
            ),
            _ => format!("SELECT * FROM (SELECT {}) s;", fuzz_expr(rng, 2)),
        }
    }

    /// Apply 1–3 random edits to a corpus entry.
    fn fuzz_mutate(rng: &mut SplitMix64, base: &str) -> String {
        let alphabet: Vec<char> = "abc019 ()*,;'\"-.+/*=<>eE\t\n".chars().collect();
        let fragments = [
            "--",
            "/*x*/",
            "COALESCE(*, 1)",
            "1e+",
            "007",
            "a- -b",
            "ALLS",
            "orderby",
            "((",
            "))",
            "\n",
        ];
        let mut chars: Vec<char> = base.chars().collect();
        for _ in 0..1 + rng.below(3) {
            if chars.is_empty() {
                chars.push('1');
            }
            match rng.below(4) {
                0 => {
                    let (at, ch) = (rng.below(chars.len()), rng.below(alphabet.len()));
                    chars[at] = alphabet[ch];
                }
                1 => {
                    let at = rng.below(chars.len());
                    chars.insert(at, alphabet[rng.below(alphabet.len())]);
                }
                2 => {
                    let at = rng.below(chars.len());
                    chars.remove(at);
                }
                _ => {
                    let at = rng.below(chars.len());
                    for (k, c) in fragments[rng.below(fragments.len())].chars().enumerate() {
                        chars.insert(at + k, c);
                    }
                }
            }
        }
        chars.into_iter().collect()
    }

    /// Seeded randomized fuzz: every generated or mutated input must parse
    /// deterministically (two runs produce equal results) and every error must
    /// be a [`PinkDbError::Parse`] — never a panic. Fixed seed, so a failure
    /// is reproducible in CI.
    #[test]
    fn seeded_fuzz_is_deterministic_and_flawless() {
        let mut rng = SplitMix64(0x5EED_2026_0000_0001);
        let (mut valid, mut invalid) = (0usize, 0usize);
        for i in 0..4_000 {
            let sql = if i % 3 == 0 {
                fuzz_input(&mut rng)
            } else {
                let base = if i % 2 == 0 {
                    VALID[rng.below(VALID.len())]
                } else {
                    INVALID[rng.below(INVALID.len())]
                };
                fuzz_mutate(&mut rng, base)
            };
            let first = parse(&sql);
            let second = parse(&sql);
            match (&first, &second) {
                (Ok(a), Ok(b)) => assert_eq!(a, b, "unstable AST for {sql:?}"),
                (Err(a), Err(b)) => {
                    assert_eq!(a.to_string(), b.to_string(), "unstable error for {sql:?}");
                }
                _ => panic!("unstable result for {sql:?}: {first:?} vs {second:?}"),
            }
            match first {
                Ok(_) => valid += 1,
                Err(e) => {
                    assert!(matches!(e, PinkDbError::Parse(_)), "{sql:?}: {e:?}");
                    invalid += 1;
                }
            }
        }
        assert!(
            valid > 0 && invalid > 0,
            "expected a valid/invalid mix: {valid} valid, {invalid} invalid"
        );
    }
}
