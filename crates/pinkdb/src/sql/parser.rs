//! Recursive-descent SQL parser → [`ast`](super::ast) types.
//!
//! Precedence (low → high): OR → AND → NOT → comparison
//! (`= <> != < <= > >=`, `IS [NOT] NULL`, `[NOT] BETWEEN`) → additive →
//! multiplicative → unary → primary.

use super::ast::*;
use super::lexer::{Keyword, Token, lex};
use crate::types::{DataType, PinkDbError, Result, Value};

/// Maximum nesting depth both parsers accept, measured in "nesting units":
/// one unit per open `(`, open `CASE` arm, or consecutive prefix unary
/// operator enclosing the current point. Deeper input is rejected with
/// [`PinkDbError::Parse`] before any recursion starts.
///
/// Every recursive cycle in both grammars passes through one of those
/// constructs, and debug builds (what `cargo test` uses, with its 2 MiB test
/// threads) overflow the stack at 36+ units in the tightest measured case
/// (nested subqueries), so the cap sits at two thirds of that.
pub(crate) const MAX_NESTING_DEPTH: usize = 24;

/// Reject `sql` when its estimated nesting depth exceeds
/// [`MAX_NESTING_DEPTH`]. Shared by both parsers so they agree on the limit.
pub(crate) fn check_nesting_depth(sql: &str) -> Result<()> {
    if nesting_estimate(sql) > MAX_NESTING_DEPTH {
        return Err(PinkDbError::Parse(format!(
            "expression nesting too deep (limit {MAX_NESTING_DEPTH})"
        )));
    }
    Ok(())
}

/// Upper bound on the parsers' recursion depth: the maximum over the input of
/// `open_parens + open_case_arms + consecutive_prefix_unary_ops`.
///
/// Every recursive cycle in both grammars goes through at least one of those
/// (`(` for parens, calls, `IN (...)`, `CAST`, `OVER (...)`, subqueries,
/// derived tables and CTEs; `CASE` until its `END`; a `NOT`/unary `+`/`-`
/// run), so this can never under-count. String literals, quoted identifiers
/// and comments are skipped, and keyword matches are boundary-aware.
fn nesting_estimate(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut parens = 0usize;
    let mut cases = 0usize;
    let mut unary = 0usize;
    let mut max = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            quote @ (b'\'' | b'"') => {
                // String literal / quoted identifier; `''` / `""` escapes.
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == quote {
                        i += 1;
                        if i < bytes.len() && bytes[i] == quote {
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                unary = 0;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < bytes.len() {
                    if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b'(' => {
                parens += 1;
                i += 1;
            }
            b')' => {
                parens = parens.saturating_sub(1);
                unary = 0;
                i += 1;
            }
            b'-' | b'+' => {
                unary += 1;
                i += 1;
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let word = &sql[start..i];
                if word.eq_ignore_ascii_case("case") {
                    cases += 1;
                } else if word.eq_ignore_ascii_case("end") {
                    cases = cases.saturating_sub(1);
                    unary = 0;
                } else if word.eq_ignore_ascii_case("not") {
                    unary += 1;
                } else {
                    unary = 0;
                }
            }
            _ => {
                // Whitespace is trivia (a `NOT` run spans it); anything else
                // (operand start, comma, operator, ...) ends a unary run.
                if !bytes[i].is_ascii_whitespace() {
                    unary = 0;
                }
                i += 1;
            }
        }
        max = max.max(parens + cases + unary);
    }
    max
}

/// Parse all statements in `sql`. Statements may be `;`-separated; the final
/// `;` is optional. Empty input yields no statements.
pub fn parse_statements(sql: &str) -> Result<Vec<Statement>> {
    check_nesting_depth(sql)?;
    let tokens = lex(sql)?;
    let mut p = Parser { tokens, pos: 0 };
    let mut out = Vec::new();
    loop {
        p.skip_semicolons();
        if p.peek().0 == Token::Eof {
            break;
        }
        out.push(p.parse_statement()?);
        // Each statement must end at `;` or EOF (catches `SELECT 1 SELECT 2`).
        match &p.peek().0 {
            Token::Semicolon => {
                p.next();
            }
            Token::Eof => break,
            tok => {
                return Err(p.error(format!("expected `;` after statement, found {tok:?}")));
            }
        }
    }
    // Post-parse depth guard (shared with the PEG parser): rejects over-deep
    // ASTs before bind/execute/drop can recurse through them.
    super::check_ast_depth(&mut out)?;
    Ok(out)
}

struct Parser {
    tokens: Vec<(Token, usize)>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &(Token, usize) {
        &self.tokens[self.pos]
    }

    fn next(&mut self) -> (Token, usize) {
        let t = self.tokens[self.pos].clone();
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        t
    }

    fn error(&self, msg: String) -> PinkDbError {
        let (_, off) = self.peek();
        PinkDbError::Parse(format!("parse error at offset {off}: {msg}"))
    }

    fn skip_semicolons(&mut self) {
        while self.peek().0 == Token::Semicolon {
            self.next();
        }
    }

    fn peek_keyword(&self) -> Option<Keyword> {
        match &self.peek().0 {
            Token::Keyword(k) => Some(*k),
            _ => None,
        }
    }

    fn eat_keyword(&mut self, kw: Keyword) -> bool {
        if self.peek_keyword() == Some(kw) {
            self.next();
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, kw: Keyword) -> Result<()> {
        if self.eat_keyword(kw) {
            Ok(())
        } else {
            Err(self.error(format!("expected keyword {kw:?}")))
        }
    }

    fn eat(&mut self, tok: &Token) -> bool {
        if &self.peek().0 == tok {
            self.next();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, tok: &Token) -> Result<()> {
        if self.eat(tok) {
            Ok(())
        } else {
            Err(self.error(format!("expected {tok:?}, found {:?}", self.peek().0)))
        }
    }

    /// Plain identifier (keywords rejected — use double quotes to quote them).
    fn parse_ident(&mut self) -> Result<String> {
        match self.next() {
            (Token::Ident(s), _) => Ok(s),
            (tok, _) => Err(self.error(format!("expected identifier, found {tok:?}"))),
        }
    }

    /// `name` or `db.name` (a table reference). Three-part names are not
    /// valid table references and get a clear error.
    fn parse_table_ref(&mut self) -> Result<(Option<String>, String)> {
        let first = self.parse_ident()?;
        if self.eat(&Token::Dot) {
            let second = self.parse_ident()?;
            if self.peek().0 == Token::Dot {
                return Err(self.error(
                    "three-part names (db.table.col) are not supported".to_string(),
                ));
            }
            return Ok((Some(first), second));
        }
        Ok((None, first))
    }

    // ---------- statements ----------

    fn parse_statement(&mut self) -> Result<Statement> {
        match self.peek_keyword() {
            Some(Keyword::Create) => self.parse_create(),
            Some(Keyword::Insert) => self.parse_insert(),
            Some(Keyword::Update) => self.parse_update(),
            Some(Keyword::Delete) => self.parse_delete(),
            Some(Keyword::Use) => {
                self.next();
                Ok(Statement::Use { name: self.parse_ident()? })
            }
            Some(Keyword::Begin) => {
                self.next();
                // `BEGIN` or `BEGIN TRANSACTION`.
                self.eat_keyword(Keyword::Transaction);
                Ok(Statement::Begin)
            }
            Some(Keyword::Commit) => {
                self.next();
                self.eat_keyword(Keyword::Transaction);
                Ok(Statement::Commit)
            }
            Some(Keyword::Rollback) => {
                self.next();
                self.eat_keyword(Keyword::Transaction);
                Ok(Statement::Rollback)
            }
            Some(Keyword::Drop) => self.parse_drop(),
            Some(Keyword::Select) | Some(Keyword::With) => {
                Ok(Statement::Select(self.parse_select()?))
            }
            _ => Err(self.error(format!(
                "expected a statement (CREATE, INSERT, UPDATE, DELETE, DROP, USE, BEGIN, COMMIT, ROLLBACK, SELECT), found {:?}",
                self.peek().0
            ))),
        }
    }

    fn parse_update(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Update)?;
        let (db, table) = self.parse_table_ref()?;
        self.expect_keyword(Keyword::Set)?;
        let mut sets = Vec::new();
        loop {
            let col = self.parse_ident()?;
            self.expect(&Token::Eq)?;
            let expr = self.parse_expr()?;
            sets.push((col, expr));
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        if sets.is_empty() {
            return Err(self.error("UPDATE needs at least one SET assignment".to_string()));
        }
        let where_clause = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Update { db, table, sets, where_clause })
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Delete)?;
        // `DELETE FROM t` (the FROM is required in v1).
        self.expect_keyword(Keyword::From)?;
        let (db, table) = self.parse_table_ref()?;
        let where_clause = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Delete { db, table, where_clause })
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Drop)?;
        if self.eat_keyword(Keyword::Database) {
            return Ok(Statement::DropDatabase { name: self.parse_ident()? });
        }
        if self.eat_keyword(Keyword::Table) {
            let (db, name) = self.parse_table_ref()?;
            return Ok(Statement::DropTable { db, name });
        }
        self.expect_keyword(Keyword::View)?;
        let (db, name) = self.parse_table_ref()?;
        Ok(Statement::DropView { db, name })
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Create)?;
        // `CREATE DATABASE name`.
        if self.eat_keyword(Keyword::Database) {
            return Ok(Statement::CreateDatabase { name: self.parse_ident()? });
        }
        // `CREATE VIEW name [(cols)] AS select`.
        if self.eat_keyword(Keyword::View) {
            let (db, name) = self.parse_table_ref()?;
            let cols = if self.peek().0 == Token::LParen {
                self.next();
                let mut c = vec![self.parse_ident()?];
                while self.eat(&Token::Comma) {
                    c.push(self.parse_ident()?);
                }
                self.expect(&Token::RParen)?;
                Some(c)
            } else {
                None
            };
            self.expect_keyword(Keyword::As)?;
            let query = self.parse_select()?;
            return Ok(Statement::CreateView { db, name, cols, query: Box::new(query) });
        }
        self.expect_keyword(Keyword::Table)?;
        let (db, name) = self.parse_table_ref()?;
        self.expect(&Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            let col = self.parse_ident()?;
            let dtype = self.parse_type_name()?;
            columns.push((col, dtype));
            if !self.eat(&Token::Comma) {
                break;
            }
            if self.peek().0 == Token::RParen {
                return Err(self.error("trailing comma in column list".to_string()));
            }
        }
        self.expect(&Token::RParen)?;
        if columns.is_empty() {
            return Err(self.error("table needs at least one column".to_string()));
        }
        Ok(Statement::CreateTable { db, name, columns })
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;
        let (db, table) = self.parse_table_ref()?;
        let columns = if self.peek().0 == Token::LParen {
            // Ambiguous with VALUES row parens only in theory: a column list
            // holds bare idents, and we require VALUES right after.
            self.next();
            let mut cols = Vec::new();
            loop {
                cols.push(self.parse_ident()?);
                if !self.eat(&Token::Comma) {
                    break;
                }
            }
            self.expect(&Token::RParen)?;
            Some(cols)
        } else {
            None
        };
        self.expect_keyword(Keyword::Values)?;
        let mut rows = Vec::new();
        loop {
            self.expect(&Token::LParen)?;
            let mut row = Vec::new();
            if self.peek().0 != Token::RParen {
                loop {
                    row.push(self.parse_expr()?);
                    if !self.eat(&Token::Comma) {
                        break;
                    }
                }
            }
            self.expect(&Token::RParen)?;
            rows.push(row);
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        if rows.is_empty() {
            return Err(self.error("INSERT needs at least one row".to_string()));
        }
        Ok(Statement::Insert { db, table, columns, rows })
    }

    /// A full SELECT including trailing UNION branches (left-associative).
    fn parse_select(&mut self) -> Result<Select> {
        let mut sel = self.parse_select_body()?;
        while self.eat_keyword(Keyword::Union) {
            // `UNION ALL` (bare ident, kept unreserved) or `UNION DISTINCT`.
            let op = if self.eat_keyword(Keyword::Distinct) {
                SetOp::Union
            } else if matches!(&self.peek().0, Token::Ident(s) if s.eq_ignore_ascii_case("all")) {
                self.next();
                SetOp::UnionAll
            } else {
                SetOp::Union
            };
            let branch = self.parse_select_body()?;
            sel.unions.push((op, branch));
        }
        Ok(sel)
    }

    /// One SELECT statement without UNION branches.
    fn parse_select_body(&mut self) -> Result<Select> {
        let mut with = Vec::new();
        if self.eat_keyword(Keyword::With) {
            loop {
                let name = self.parse_ident()?;
                let cols = if self.peek().0 == Token::LParen {
                    self.next();
                    let mut c = vec![self.parse_ident()?];
                    while self.eat(&Token::Comma) {
                        c.push(self.parse_ident()?);
                    }
                    self.expect(&Token::RParen)?;
                    Some(c)
                } else {
                    None
                };
                self.expect_keyword(Keyword::As)?;
                self.expect(&Token::LParen)?;
                let query = self.parse_select()?;
                self.expect(&Token::RParen)?;
                with.push(CteDef { name, cols, query: Box::new(query) });
                if !self.eat(&Token::Comma) {
                    break;
                }
            }
        }
        self.expect_keyword(Keyword::Select)?;
        let distinct = self.eat_keyword(Keyword::Distinct);
        let projections = self.parse_projection_list()?;
        let from = if self.eat_keyword(Keyword::From) {
            Some(self.parse_from()?)
        } else {
            None
        };
        let where_clause = if self.eat_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let group_by = if self.eat_keyword(Keyword::Group) {
            self.expect_keyword(Keyword::By)?;
            self.parse_expr_list()?
        } else {
            Vec::new()
        };
        let having = if self.eat_keyword(Keyword::Having) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let mut order_by = Vec::new();
        if self.eat_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            order_by = self.parse_order_list()?;
        }
        let limit = if self.eat_keyword(Keyword::Limit) {
            match self.next() {
                (Token::Integer(n), _) if n >= 0 => Some(n as usize),
                (tok, _) => {
                    return Err(self.error(format!(
                        "LIMIT needs a non-negative integer, found {tok:?}"
                    )));
                }
            }
        } else {
            None
        };
        Ok(Select {
            with,
            distinct,
            projections,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            unions: Vec::new(),
        })
    }

    /// A type name: usually an identifier, but some (DATE, TIMESTAMP) lex
    /// as keywords — accept those too.
    fn parse_type_name(&mut self) -> Result<DataType> {
        let (tok, _) = self.next();
        let name = match &tok {
            Token::Ident(s) => s.clone(),
            Token::Keyword(Keyword::Date) => "DATE".to_string(),
            Token::Keyword(Keyword::Timestamp) => "TIMESTAMP".to_string(),
            tok => {
                return Err(self.error(format!("expected column type, found {tok:?}")));
            }
        };
        DataType::from_sql(&name)
            .ok_or_else(|| self.error(format!("unknown data type \"{name}\"")))
    }

    fn parse_projection_list(&mut self) -> Result<Vec<Projection>> {
        // `SELECT *` / `SELECT t.*` — only as the whole list in v1.
        if self.peek().0 == Token::Star {
            self.next();
            if self.peek().0 == Token::Comma {
                return Err(self.error("`*` must be the whole projection list in v1".to_string()));
            }
            return Ok(vec![Projection::Star]);
        }
        if let (Token::Ident(_), _) = self.peek() {
            if let Some((Token::Dot, _)) = self.tokens.get(self.pos + 1) {
                if let Some((Token::Star, _)) = self.tokens.get(self.pos + 2) {
                    let (Token::Ident(t), _) = self.next() else { unreachable!() };
                    self.next(); // dot
                    self.next(); // star
                    if self.peek().0 == Token::Comma {
                        return Err(self.error(
                            "`t.*` must be the whole projection list in v1".to_string(),
                        ));
                    }
                    return Ok(vec![Projection::QualifiedStar(t)]);
                }
            }
        }
        let mut out = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            // `*` mid-list is invalid (`SELECT a, *`); reject with a clear error.
            if expr == Expr::Star || matches!(expr, Expr::QualifiedStar(_)) {
                return Err(self.error(
                    "`*` must be the whole projection list in v1".to_string(),
                ));
            }
            let alias = self.parse_optional_alias()?;
            out.push(Projection::Expr { expr, alias });
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        Ok(out)
    }

    /// `[AS] alias` — a bare alias must be a non-keyword identifier.
    fn parse_optional_alias(&mut self) -> Result<Option<String>> {
        if self.eat_keyword(Keyword::As) {
            return Ok(Some(self.parse_ident()?));
        }
        match &self.peek().0 {
            Token::Ident(s) => {
                let s = s.clone();
                self.next();
                Ok(Some(s))
            }
            _ => Ok(None),
        }
    }

    /// A FROM/JOIN source: `table` / `db.table` or `(SELECT ...)`.
    /// Derived tables require an alias (checked by the caller).
    fn parse_source(&mut self) -> Result<FromSource> {
        if self.peek().0 == Token::LParen
            && matches!(
                self.tokens.get(self.pos + 1).map(|(t, _)| t),
                Some(Token::Keyword(Keyword::Select)) | Some(Token::Keyword(Keyword::With))
            )
        {
            self.next();
            let q = self.parse_select()?;
            self.expect(&Token::RParen)?;
            Ok(FromSource::Derived(Box::new(q)))
        } else {
            let (db, name) = self.parse_table_ref()?;
            Ok(FromSource::Table { db, name })
        }
    }

    /// The join keyword sequence after a FROM source, if any:
    /// `JOIN` / `INNER JOIN` / `LEFT [OUTER] JOIN` / `RIGHT [OUTER] JOIN`.
    /// `FULL` and `CROSS` are rejected here (v1 has no such joins).
    fn parse_join_kind(&mut self) -> Result<Option<JoinKind>> {
        let kind = match self.peek_keyword() {
            Some(Keyword::Join) => {
                self.next();
                JoinKind::Inner
            }
            Some(Keyword::Inner) => {
                self.next();
                self.expect_keyword(Keyword::Join)?;
                JoinKind::Inner
            }
            Some(Keyword::Left) => {
                self.next();
                self.eat_keyword(Keyword::Outer);
                self.expect_keyword(Keyword::Join)?;
                JoinKind::Left
            }
            Some(Keyword::Right) => {
                self.next();
                self.eat_keyword(Keyword::Outer);
                self.expect_keyword(Keyword::Join)?;
                JoinKind::Right
            }
            Some(Keyword::Full) => {
                return Err(self.error("FULL [OUTER] JOIN is not supported in v1".to_string()))
            }
            Some(Keyword::Cross) => {
                return Err(self.error("CROSS JOIN is not supported in v1".to_string()))
            }
            _ => return Ok(None),
        };
        Ok(Some(kind))
    }

    fn parse_from(&mut self) -> Result<FromClause> {
        let source = self.parse_source()?;
        let alias = self.parse_optional_alias()?;
        if matches!(source, FromSource::Derived(_)) && alias.is_none() {
            return Err(self.error("derived table needs an alias".to_string()));
        }
        let join = match self.parse_join_kind()? {
            None => None,
            Some(kind) => {
                let source = self.parse_source()?;
                let alias = self.parse_optional_alias()?;
                if matches!(source, FromSource::Derived(_)) && alias.is_none() {
                    return Err(self.error("derived table needs an alias".to_string()));
                }
                self.expect_keyword(Keyword::On)?;
                let on = self.parse_expr()?;
                Some(JoinClause { kind, source, alias, on })
            }
        };
        Ok(FromClause { source, alias, join })
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>> {
        let mut out = vec![self.parse_expr()?];
        while self.eat(&Token::Comma) {
            out.push(self.parse_expr()?);
        }
        Ok(out)
    }

    /// `expr [ASC|DESC], ...` (the ORDER BY having been consumed).
    fn parse_order_list(&mut self) -> Result<Vec<OrderItem>> {
        let mut order_by = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let desc = if self.eat_keyword(Keyword::Desc) {
                true
            } else {
                self.eat_keyword(Keyword::Asc);
                false
            };
            order_by.push(OrderItem { expr, desc });
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        Ok(order_by)
    }

    /// `OVER ([PARTITION BY ...] [ORDER BY ...] [ROWS ...])`.
    fn parse_over(&mut self) -> Result<OverClause> {
        self.expect_keyword(Keyword::Over)?;
        self.expect(&Token::LParen)?;
        let mut partition = Vec::new();
        if self.eat_keyword(Keyword::Partition) {
            self.expect_keyword(Keyword::By)?;
            partition = self.parse_expr_list()?;
        }
        let mut order = Vec::new();
        if self.eat_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By)?;
            order = self.parse_order_list()?;
        }
        let mut frame = None;
        if self.eat_keyword(Keyword::Rows) || self.eat_keyword(Keyword::Row) {
            frame = Some(self.parse_rows_frame()?);
        } else if self.eat_keyword(Keyword::Range) {
            return Err(self.error(
                "RANGE frames are not supported in v1 (use ROWS)".to_string(),
            ));
        }
        self.expect(&Token::RParen)?;
        Ok(OverClause { partition, order, frame })
    }

    /// After `ROWS`: `BETWEEN a AND b | UNBOUNDED PRECEDING | CURRENT ROW |
    /// UNBOUNDED FOLLOWING | <n> PRECEDING | <n> FOLLOWING`.
    fn parse_rows_frame(&mut self) -> Result<FrameSpec> {
        if self.eat_keyword(Keyword::Between) {
            let start = self.parse_frame_bound()?;
            self.expect_keyword(Keyword::And)?;
            let end = self.parse_frame_bound()?;
            return Ok(FrameSpec { start, end });
        }
        if self.eat_keyword(Keyword::Current) {
            self.expect_keyword(Keyword::Row)?;
            let b = FrameBound::CurrentRow;
            return Ok(FrameSpec { start: b, end: b });
        }
        if self.eat_keyword(Keyword::Unbounded) {
            if self.eat_keyword(Keyword::Preceding) {
                return Ok(FrameSpec {
                    start: FrameBound::UnboundedPreceding,
                    end: FrameBound::CurrentRow,
                });
            }
            self.expect_keyword(Keyword::Following)?;
            return Ok(FrameSpec {
                start: FrameBound::UnboundedFollowing,
                end: FrameBound::UnboundedFollowing,
            });
        }
        match self.next() {
            (Token::Integer(n), _) if n >= 0 => {
                if self.eat_keyword(Keyword::Preceding) {
                    return Ok(FrameSpec {
                        start: FrameBound::Preceding(n as usize),
                        end: FrameBound::CurrentRow,
                    });
                }
                self.expect_keyword(Keyword::Following)?;
                Ok(FrameSpec {
                    start: FrameBound::CurrentRow,
                    end: FrameBound::Following(n as usize),
                })
            }
            (tok, _) => Err(self.error(format!("bad frame bound: {tok:?}"))),
        }
    }

    /// `CASE [op] WHEN c THEN v ... [ELSE e] END`. The simple form desugars
    /// `WHEN c` to `WHEN op = c` (NULL never matches, per SQL semantics).
    /// (The CASE keyword itself was consumed by the caller.)
    fn parse_case(&mut self) -> Result<Expr> {
        // Optional simple-case operand: present unless WHEN follows directly.
        let operand = if self.peek_keyword() == Some(Keyword::When) {
            None
        } else {
            Some(self.parse_expr()?)
        };
        let mut branches = Vec::new();
        loop {
            self.expect_keyword(Keyword::When)?;
            let cond = self.parse_expr()?;
            self.expect_keyword(Keyword::Then)?;
            let val = self.parse_expr()?;
            let cond = match &operand {
                Some(op) => Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(op.clone()),
                    right: Box::new(cond),
                },
                None => cond,
            };
            branches.push((cond, val));
            if self.peek_keyword() != Some(Keyword::When) {
                break;
            }
        }
        if branches.is_empty() {
            return Err(self.error("CASE needs at least one WHEN".to_string()));
        }
        let else_ = if self.eat_keyword(Keyword::Else) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_keyword(Keyword::End)?;
        Ok(Expr::Case { branches, else_ })
    }

    fn parse_frame_bound(&mut self) -> Result<FrameBound> {        if self.eat_keyword(Keyword::Unbounded) {
            if self.eat_keyword(Keyword::Preceding) {
                return Ok(FrameBound::UnboundedPreceding);
            }
            self.expect_keyword(Keyword::Following)?;
            return Ok(FrameBound::UnboundedFollowing);
        }
        if self.eat_keyword(Keyword::Current) {
            self.expect_keyword(Keyword::Row)?;
            return Ok(FrameBound::CurrentRow);
        }
        match self.next() {
            (Token::Integer(n), _) if n >= 0 => {
                if self.eat_keyword(Keyword::Preceding) {
                    return Ok(FrameBound::Preceding(n as usize));
                }
                self.expect_keyword(Keyword::Following)?;
                Ok(FrameBound::Following(n as usize))
            }
            (tok, _) => Err(self.error(format!("bad frame bound: {tok:?}"))),
        }
    }

    // ---------- expressions ----------

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.eat_keyword(Keyword::Or) {
            let right = self.parse_and()?;
            left = Expr::Binary {
                op: BinaryOp::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_not()?;
        while self.eat_keyword(Keyword::And) {
            let right = self.parse_not()?;
            left = Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr> {
        if self.eat_keyword(Keyword::Not) {
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(self.parse_not()?),
            });
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_additive()?;
        // `IS [NOT] NULL`
        if self.eat_keyword(Keyword::Is) {
            let negated = self.eat_keyword(Keyword::Not);
            self.expect_keyword(Keyword::Null)?;
            return Ok(Expr::IsNull {
                expr: Box::new(left),
                negated,
            });
        }
        // `[NOT] BETWEEN low AND high`
        let between_negated = if self.eat_keyword(Keyword::Between) {
            Some(false)
        } else if self.peek_keyword() == Some(Keyword::Not)
            && matches!(self.tokens.get(self.pos + 1).map(|(t, _)| t), Some(Token::Keyword(Keyword::Between)))
        {
            self.next();
            self.next();
            Some(true)
        } else {
            None
        };
        if let Some(negated) = between_negated {
            let low = self.parse_additive()?;
            self.expect_keyword(Keyword::And)?;
            let high = self.parse_additive()?;
            return Ok(Expr::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated,
            });
        }
        // `[NOT] IN (SELECT ...) | (a, b, ...)`
        let in_negated = if self.eat_keyword(Keyword::In) {
            Some(false)
        } else if self.peek_keyword() == Some(Keyword::Not)
            && matches!(self.tokens.get(self.pos + 1).map(|(t, _)| t), Some(Token::Keyword(Keyword::In)))
        {
            self.next();
            self.next();
            Some(true)
        } else {
            None
        };
        if let Some(negated) = in_negated {
            self.expect(&Token::LParen)?;
            // Subquery form.
            if matches!(
                self.peek().0,
                Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::With)
            ) {
                let query = self.parse_select()?;
                self.expect(&Token::RParen)?;
                return Ok(Expr::InSubquery {
                    expr: Box::new(left),
                    query: Box::new(query),
                    negated,
                });
            }
            // List form desugars to `=` chains (`IN ()` → FALSE).
            let mut items = Vec::new();
            if self.peek().0 != Token::RParen {
                items = self.parse_expr_list()?;
            }
            self.expect(&Token::RParen)?;
            return Ok(desugar_in_list(left, items, negated));
        }
        // `[NOT] LIKE pattern`
        let like_negated = if self.eat_keyword(Keyword::Like) {
            Some(false)
        } else if self.peek_keyword() == Some(Keyword::Not)
            && matches!(self.tokens.get(self.pos + 1).map(|(t, _)| t), Some(Token::Keyword(Keyword::Like)))
        {
            self.next();
            self.next();
            Some(true)
        } else {
            None
        };
        if let Some(negated) = like_negated {
            let right = self.parse_additive()?;
            return Ok(Expr::Binary {
                op: if negated { BinaryOp::NotLike } else { BinaryOp::Like },
                left: Box::new(left),
                right: Box::new(right),
            });
        }
        // Plain comparison operators.
        let op = match &self.peek().0 {
            Token::Eq => BinaryOp::Eq,
            Token::NotEq => BinaryOp::NotEq,
            Token::Lt => BinaryOp::Lt,
            Token::LtEq => BinaryOp::LtEq,
            Token::Gt => BinaryOp::Gt,
            Token::GtEq => BinaryOp::GtEq,
            _ => return Ok(left),
        };
        self.next();
        let right = self.parse_additive()?;
        Ok(Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

/// `x IN (a, b)` → `(x = a) OR (x = b)`; `IN ()` → FALSE.
/// Kleene NULL semantics are preserved by the OR chain.
    fn parse_additive(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match &self.peek().0 {
                Token::Plus => BinaryOp::Add,
                Token::Minus => BinaryOp::Subtract,
                _ => break,
            };
            self.next();
            let right = self.parse_multiplicative()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match &self.peek().0 {
                Token::Star => BinaryOp::Multiply,
                Token::Slash => BinaryOp::Divide,
                Token::Percent => BinaryOp::Modulo,
                _ => break,
            };
            self.next();
            let right = self.parse_unary()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        match &self.peek().0 {
            Token::Minus => {
                self.next();
                Ok(Expr::Unary {
                    op: UnaryOp::Negate,
                    expr: Box::new(self.parse_unary()?),
                })
            }
            Token::Plus => {
                self.next();
                Ok(Expr::Unary {
                    op: UnaryOp::Plus,
                    expr: Box::new(self.parse_unary()?),
                })
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match self.next() {
            (Token::Integer(n), _) => {
                // DuckDB rule: magnitude decides INTEGER vs BIGINT.
                if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                    Ok(Expr::Literal(Value::Integer(n as i32)))
                } else {
                    Ok(Expr::Literal(Value::Bigint(n)))
                }
            }
            (Token::Float(x), _) => Ok(Expr::Literal(Value::Double(x))),
            (Token::String(s), _) => Ok(Expr::Literal(Value::Varchar(s))),
            (Token::Keyword(Keyword::True), _) => Ok(Expr::Literal(Value::Boolean(true))),
            (Token::Keyword(Keyword::False), _) => Ok(Expr::Literal(Value::Boolean(false))),
            (Token::Keyword(Keyword::Null), _) => Ok(Expr::Literal(Value::Null)),
            (Token::Keyword(Keyword::Date), _) => match self.next() {
                (Token::String(s), _) => crate::types::parse_date(&s)
                    .map(Value::Date)
                    .map(Expr::Literal)
                    .ok_or_else(|| self.error(format!("bad DATE literal \"{s}\""))),
                (tok, _) => Err(self.error(format!("DATE needs a string literal, found {tok:?}"))),
            },
            (Token::Keyword(Keyword::Timestamp), _) => match self.next() {
                (Token::String(s), _) => crate::types::parse_timestamp(&s)
                    .map(Value::Timestamp)
                    .map(Expr::Literal)
                    .ok_or_else(|| self.error(format!("bad TIMESTAMP literal \"{s}\""))),
                (tok, _) => Err(self.error(format!(
                    "TIMESTAMP needs a string literal, found {tok:?}"
                ))),
            },
            (Token::Keyword(Keyword::Cast), _) => {
                self.expect(&Token::LParen)?;
                let expr = self.parse_expr()?;
                self.expect_keyword(Keyword::As)?;
                let dtype = self.parse_type_name()?;
                self.expect(&Token::RParen)?;
                Ok(Expr::Cast { expr: Box::new(expr), dtype })
            }
            (Token::Keyword(Keyword::Case), _) => self.parse_case(),
            (Token::Star, _) => Ok(Expr::Star),
            (Token::Keyword(Keyword::Exists), _) => {
                self.expect(&Token::LParen)?;
                let query = self.parse_select()?;
                self.expect(&Token::RParen)?;
                Ok(Expr::Exists { query: Box::new(query), negated: false })
            }
            (Token::LParen, _) => {
                // `(SELECT ...)` is a scalar subquery; otherwise a grouping.
                if matches!(
                    self.peek().0,
                    Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::With)
                ) {
                    let query = self.parse_select()?;
                    self.expect(&Token::RParen)?;
                    return Ok(Expr::ScalarSubquery(Box::new(query)));
                }
                let e = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(e)
            }
            (Token::Ident(name), _) => {
                // Function call?
                if self.peek().0 == Token::LParen {
                    self.next();
                    // COALESCE is n-ary sugar parsed here (not a ScalarFunc).
                    if name.eq_ignore_ascii_case("coalesce") {
                        let mut args = Vec::new();
                        if self.peek().0 != Token::RParen {
                            args = self.parse_expr_list()?;
                        }
                        self.expect(&Token::RParen)?;
                        if args.is_empty() {
                            return Err(self.error(
                                "COALESCE needs at least one argument".to_string(),
                            ));
                        }
                        return Ok(Expr::Coalesce(args));
                    }
                    let mut args = Vec::new();
                    if self.peek().0 == Token::Star {
                        self.next();
                        args.push(Expr::Star);
                    } else if self.peek().0 != Token::RParen {
                        args = self.parse_expr_list()?;
                    }
                    self.expect(&Token::RParen)?;
                    if self.peek_keyword() == Some(Keyword::Over) {
                        let over = self.parse_over()?;
                        return Ok(Expr::Window { name, args, over });
                    }
                    return Ok(Expr::Function { name, args });
                }
                // Qualified `table.column`, `table.*`, or bare column.
                if self.peek().0 == Token::Dot {
                    self.next();
                    match self.next() {
                        (Token::Ident(col), _) => {
                            if self.peek().0 == Token::Dot {
                                return Err(self.error(
                                    "three-part column references (db.table.col) are not supported"
                                        .to_string(),
                                ));
                            }
                            Ok(Expr::Column {
                                table: Some(name),
                                name: col,
                            })
                        }
                        (Token::Star, _) => Ok(Expr::QualifiedStar(Some(name))),
                        (tok, _) => Err(self.error(format!(
                            "expected column name or `*` after `.`, found {tok:?}"
                        ))),
                    }
                } else {
                    Ok(Expr::Column { table: None, name })
                }
            }
            (tok, _) => Err(self.error(format!("unexpected {tok:?} in expression"))),
        }
    }
}

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
        Expr::Unary { op: UnaryOp::Not, expr: Box::new(acc) }
    } else {
        acc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(sql: &str) -> Statement {
        let stmts = parse_statements(sql).unwrap();
        assert_eq!(stmts.len(), 1);
        stmts.into_iter().next().unwrap()
    }

    #[test]
    fn precedence_mul_before_add() {
        let s = parse_one("SELECT 1 + 2 * 3;");
        let Statement::Select(sel) = s else { panic!() };
        assert_eq!(sel.projections.len(), 1);
        // 1 + (2 * 3)
        match &sel.projections[0] {
            Projection::Expr { expr, .. } => match expr {
                Expr::Binary { op: BinaryOp::Add, right, .. } => {
                    assert!(matches!(**right, Expr::Binary { op: BinaryOp::Multiply, .. }));
                }
                _ => panic!("wrong shape: {expr:?}"),
            },
            _ => panic!(),
        }
    }

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
        let join = sel.from.unwrap().join.unwrap();
        assert_eq!(join.kind, JoinKind::Inner);
        let s = parse_one("SELECT * FROM a INNER JOIN b ON a.id = b.id;");
        let Statement::Select(sel) = s else { panic!() };
        assert_eq!(sel.from.unwrap().join.unwrap().kind, JoinKind::Inner);
    }

    #[test]
    fn outer_join_kinds_parse_and_full_cross_error() {
        // LEFT / LEFT OUTER / RIGHT / RIGHT OUTER, case-insensitive.
        for (sql, want) in [
            ("SELECT * FROM a LEFT JOIN b ON a.id = b.id;", JoinKind::Left),
            ("SELECT * FROM a left outer join b ON a.id = b.id;", JoinKind::Left),
            ("SELECT * FROM a RIGHT JOIN b ON a.id = b.id;", JoinKind::Right),
            ("SELECT * FROM a right OUTER join b ON a.id = b.id;", JoinKind::Right),
        ] {
            let Statement::Select(sel) = parse_one(sql) else { panic!() };
            assert_eq!(sel.from.unwrap().join.unwrap().kind, want, "{sql}");
        }
        // LEFT/RIGHT without JOIN, FULL/CROSS always.
        assert!(parse_statements("SELECT * FROM a LEFT b ON a.id = b.id;").is_err());
        assert!(parse_statements("SELECT * FROM a FULL JOIN b ON a.id = b.id;").is_err());
        assert!(parse_statements("SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id;").is_err());
        assert!(parse_statements("SELECT * FROM a CROSS JOIN b;").is_err());
        assert!(parse_statements("SELECT * FROM a CROSS JOIN b ON a.id = b.id;").is_err());
        // USING / NATURAL stay errors.
        assert!(parse_statements("SELECT * FROM a JOIN b USING (id);").is_err());
        assert!(parse_statements("SELECT * FROM a NATURAL JOIN b;").is_err());
    }

    #[test]
    fn bad_sql_errors() {
        assert!(parse_statements("SELECT FROM;").is_err());
        assert!(parse_statements("SELECT 1 SELECT 2;").is_err());
        assert!(parse_statements("CREATE TABLE t (a NOPE);").is_err());
        assert!(parse_statements("SELECT 1 LIMIT -1;").is_err());
    }

    #[test]
    fn date_time_literals_and_types() {
        let s = parse_one("SELECT DATE '2024-02-29', TIMESTAMP '2024-03-15 12:00:00.5';");
        let Statement::Select(sel) = s else { panic!() };
        assert_eq!(sel.projections.len(), 2);
        assert!(parse_statements("SELECT DATE '2023-02-29';").is_err());
        assert!(parse_statements("SELECT DATE 123;").is_err());
        assert!(matches!(
            parse_one("CREATE TABLE t (d DATE, ts TIMESTAMP);"),
            Statement::CreateTable { .. }
        ));
    }

    #[test]
    fn window_shapes() {        let s = parse_one("SELECT rank() OVER (PARTITION BY a ORDER BY b DESC) FROM t;");
        let Statement::Select(sel) = s else { panic!() };
        match &sel.projections[0] {
            Projection::Expr { expr: Expr::Window { name, over, .. }, .. } => {
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
            Projection::Expr { expr: Expr::Window { .. }, .. }
        ));
        assert!(parse_statements(
            "SELECT sum(x) OVER (RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t;"
        )
        .is_err());
    }

    #[test]
    fn case_shapes() {
        // Searched form.
        let s = parse_one("SELECT CASE WHEN a > 1 THEN 'big' WHEN a = 1 THEN 'one' ELSE 'other' END FROM t;");
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
        assert!(parse_statements("SELECT CASE WHEN 1 THEN 2;").is_err());
        assert!(parse_statements("SELECT CASE END FROM t;").is_err());
    }

    #[test]
    fn update_delete_shapes() {
        match parse_one("UPDATE t SET a = 1, b = a + 2 WHERE a > 0;") {
            Statement::Update { db, table, sets, where_clause } => {
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
            Statement::Delete { db, table, where_clause } => {
                assert_eq!(db, None);
                assert_eq!(table, "t");
                assert!(where_clause.is_none());
            }
            _ => panic!(),
        }
        assert!(parse_statements("UPDATE t SET;").is_err());
        assert!(parse_statements("DELETE t;").is_err());
    }

    #[test]
    fn multi_database_statements() {
        assert!(matches!(
            parse_one("CREATE DATABASE analytics;"),
            Statement::CreateDatabase { name } if name == "analytics"
        ));
        assert!(matches!(
            parse_one("DROP DATABASE analytics;"),
            Statement::DropDatabase { name } if name == "analytics"
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
            Statement::DropTable { db, name } => {
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
        let Statement::Select(sel) = parse_one("SELECT a FROM db1.t;") else { panic!() };
        match sel.from.unwrap().source {
            FromSource::Table { db, name } => {
                assert_eq!(db, Some("db1".to_string()));
                assert_eq!(name, "t");
            }
            other => panic!("{other:?}"),
        }
        // Three-part references are rejected with a clear message.
        let err = parse_statements("SELECT db1.t.a FROM db1.t;").unwrap_err();
        assert!(err.to_string().contains("three-part"), "{err}");
        let err = parse_statements("SELECT db1.t.* FROM db1.t;").unwrap_err();
        assert!(err.to_string().contains("three-part"), "{err}");
    }

    #[test]
    fn txn_statements() {
        assert!(matches!(parse_one("BEGIN;"), Statement::Begin));
        assert!(matches!(parse_one("BEGIN TRANSACTION;"), Statement::Begin));
        assert!(matches!(parse_one("COMMIT;"), Statement::Commit));
        assert!(matches!(parse_one("ROLLBACK TRANSACTION;"), Statement::Rollback));
    }

    #[test]
    fn with_in_exists_shapes() {        let s = parse_one("WITH a AS (SELECT 1 AS x), b(y) AS (SELECT x FROM a) SELECT * FROM b;");
        let Statement::Select(sel) = s else { panic!() };
        assert_eq!(sel.with.len(), 2);
        assert_eq!(sel.with[1].cols, Some(vec!["y".to_string()]));

        let s = parse_one("SELECT * FROM (SELECT 1 AS x) AS s;");
        let Statement::Select(sel) = s else { panic!() };
        assert!(matches!(
            sel.from.unwrap().source,
            crate::sql::ast::FromSource::Derived(_)
        ));

        let s = parse_one("SELECT a FROM t WHERE a IN (SELECT b FROM u) AND EXISTS (SELECT 1);");
        let Statement::Select(sel) = s else { panic!() };
        assert!(sel.where_clause.is_some());

        // IN-list desugars to an OR chain.
        let s = parse_one("SELECT * FROM t WHERE a IN (1, 2);");
        let Statement::Select(sel) = s else { panic!() };
        assert!(matches!(
            sel.where_clause,
            Some(Expr::Binary { op: BinaryOp::Or, .. })
        ));

        // Scalar subquery in an expression.
        let s = parse_one("SELECT (SELECT MAX(x) FROM u) + 1;");
        let Statement::Select(sel) = s else { panic!() };
        assert_eq!(sel.projections.len(), 1);
    }
}
