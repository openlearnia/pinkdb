//! SQL AST for the v1 surface.
//!
//! Statements: `CREATE TABLE`, `INSERT`, `SELECT` (with an optional single
//! inner equi-`JOIN`).

use crate::types::Value;

#[derive(Debug, Clone, PartialEq)]
// v1: a handful of statements alive at once; enum size is irrelevant.
#[allow(clippy::large_enum_variant)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<(String, crate::types::DataType)>,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<Expr>>,
    },
    Update {
        table: String,
        /// `(column, expression)` pairs in written order.
        sets: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
    },
    Delete {
        table: String,
        where_clause: Option<Expr>,
    },
    Begin,
    Commit,
    Rollback,
    CreateView {
        name: String,
        cols: Option<Vec<String>>,
        query: Box<Select>,
    },
    DropTable {
        name: String,
    },
    DropView {
        name: String,
    },
    Select(Select),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub with: Vec<CteDef>,
    pub distinct: bool,
    pub projections: Vec<Projection>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<usize>,
    /// `UNION [ALL|DISTINCT] branch...` (left-associative).
    pub unions: Vec<(SetOp, Select)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Union,
    UnionAll,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// `*`
    Star,
    /// `alias.*`
    QualifiedStar(String),
    /// `expr [AS alias]`
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CteDef {
    pub name: String,
    /// `WITH w(a, b) AS ...` renames the output columns.
    pub cols: Option<Vec<String>>,
    pub query: Box<Select>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FromSource {
    Table(String),
    Derived(Box<Select>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub source: FromSource,
    pub alias: Option<String>,
    pub join: Option<JoinClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub source: FromSource,
    pub alias: Option<String>,
    /// `ON` must be a single equi-condition `a = b` (checked in the binder).
    pub on: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    pub desc: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OverClause {
    pub partition: Vec<Expr>,
    pub order: Vec<OrderItem>,
    /// `None` = default frame (whole partition, or cumulative with ORDER BY).
    pub frame: Option<FrameSpec>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FrameSpec {
    pub start: FrameBound,
    pub end: FrameBound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(usize),
    CurrentRow,
    Following(usize),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Value),
    /// Bare `*` (only valid as a function arg for `COUNT(*)`).
    Star,
    Column { table: Option<String>, name: String },
    QualifiedStar(Option<String>),
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Function {
        name: String,
        args: Vec<Expr>,
    },
    /// `f(...) OVER ([PARTITION BY ...] [ORDER BY ...] [ROWS ...])`.
    Window {
        name: String,
        args: Vec<Expr>,
        over: OverClause,
    },
    /// `(SELECT ...)` — uncorrelated scalar subquery (exactly 1 row × 1 col;
    /// 0 rows → NULL).
    ScalarSubquery(Box<Select>),
    /// `expr [NOT] IN (SELECT ...)` — uncorrelated.
    InSubquery {
        expr: Box<Expr>,
        query: Box<Select>,
        negated: bool,
    },
    /// `[NOT] EXISTS (SELECT ...)` — true iff the query returns any row.
    Exists {
        query: Box<Select>,
        negated: bool,
    },
    /// `COALESCE(a, b, ...)` — first non-NULL argument.
    Coalesce(Vec<Expr>),
    /// `CAST(x AS type)`.
    Cast {
        expr: Box<Expr>,
        dtype: crate::types::DataType,
    },
    /// `CASE [op] WHEN c THEN v ... [ELSE e] END` (simple form desugared
    /// to `op = c` in the parser; missing ELSE is NULL).
    Case {
        branches: Vec<(Expr, Expr)>,
        else_: Option<Box<Expr>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Like,
    NotLike,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Negate,
    Plus,
}
