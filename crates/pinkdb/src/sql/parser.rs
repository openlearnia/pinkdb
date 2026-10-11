//! Recursive-descent SQL parser → [`ast`](super::ast) types.
//!
//! Precedence (low → high): OR → AND → NOT → comparison
//! (`= <> != < <= > >=`, `IS [NOT] NULL`, `[NOT] BETWEEN`) → additive →
//! multiplicative → unary → primary.

use super::ast::*;
use super::lexer::{Keyword, Token, lex};
use crate::types::{DataType, PinkDbError, Result, Value};

/// Parse all statements in `sql`. Statements may be `;`-separated; the final
/// `;` is optional. Empty input yields no statements.
pub fn parse_statements(sql: &str) -> Result<Vec<Statement>> {
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

    // ---------- statements ----------

    fn parse_statement(&mut self) -> Result<Statement> {
        match self.peek_keyword() {
            Some(Keyword::Create) => self.parse_create(),
            Some(Keyword::Insert) => self.parse_insert(),
            Some(Keyword::Select) | Some(Keyword::With) => {
                Ok(Statement::Select(self.parse_select()?))
            }
            _ => Err(self.error(format!(
                "expected CREATE, INSERT, or SELECT, found {:?}",
                self.peek().0
            ))),
        }
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Create)?;
        self.expect_keyword(Keyword::Table)?;
        let name = self.parse_ident()?;
        self.expect(&Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            let col = self.parse_ident()?;
            let (Token::Ident(type_name), _) = self.next() else {
                return Err(self.error("expected column type".to_string()));
            };
            let dtype = DataType::from_sql(&type_name).ok_or_else(|| {
                self.error(format!("unknown data type \"{type_name}\""))
            })?;
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
        Ok(Statement::CreateTable { name, columns })
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;
        let table = self.parse_ident()?;
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
        Ok(Statement::Insert { table, columns, rows })
    }

    fn parse_select(&mut self) -> Result<Select> {
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
        })
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

    /// A FROM/JOIN source: `table` or `(SELECT ...)`.
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
            Ok(FromSource::Table(self.parse_ident()?))
        }
    }

    fn parse_from(&mut self) -> Result<FromClause> {
        let source = self.parse_source()?;
        let alias = self.parse_optional_alias()?;
        if matches!(source, FromSource::Derived(_)) && alias.is_none() {
            return Err(self.error("derived table needs an alias".to_string()));
        }
        let join = if self.eat_keyword(Keyword::Join)
            || (self.eat_keyword(Keyword::Inner) && {
                self.expect_keyword(Keyword::Join)?;
                true
            })
        {
            let source = self.parse_source()?;
            let alias = self.parse_optional_alias()?;
            if matches!(source, FromSource::Derived(_)) && alias.is_none() {
                return Err(self.error("derived table needs an alias".to_string()));
            }
            self.expect_keyword(Keyword::On)?;
            let on = self.parse_expr()?;
            Some(JoinClause { source, alias, on })
        } else {
            None
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
                    let mut args = Vec::new();
                    if self.peek().0 == Token::Star {
                        self.next();
                        args.push(Expr::Star);
                    } else if self.peek().0 != Token::RParen {
                        args = self.parse_expr_list()?;
                    }
                    self.expect(&Token::RParen)?;
                    return Ok(Expr::Function { name, args });
                }
                // Qualified `table.column`, `table.*`, or bare column.
                if self.peek().0 == Token::Dot {
                    self.next();
                    match self.next() {
                        (Token::Ident(col), _) => Ok(Expr::Column {
                            table: Some(name),
                            name: col,
                        }),
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
        assert!(sel.from.unwrap().join.is_some());
    }

    #[test]
    fn bad_sql_errors() {
        assert!(parse_statements("SELECT FROM;").is_err());
        assert!(parse_statements("SELECT 1 SELECT 2;").is_err());
        assert!(parse_statements("CREATE TABLE t (a NOPE);").is_err());
        assert!(parse_statements("SELECT 1 LIMIT -1;").is_err());
    }

    #[test]
    fn with_in_exists_shapes() {
        let s = parse_one("WITH a AS (SELECT 1 AS x), b(y) AS (SELECT x FROM a) SELECT * FROM b;");
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
