//! SQL AST for the v1 surface.
//!
//! Statements: `CREATE TABLE`, `INSERT`, `SELECT` (with an optional single
//! equi-`JOIN` — INNER / LEFT [OUTER] / RIGHT [OUTER]).

use crate::types::Value;

#[derive(Debug, Clone, PartialEq)]
// v1: a handful of statements alive at once; enum size is irrelevant.
#[allow(clippy::large_enum_variant)]
pub enum Statement {
    CreateTable {
        /// Explicit `db.` qualifier, if written (resolved at bind).
        db: Option<String>,
        name: String,
        /// Columns and table-level constraints, in written order.
        items: Vec<TableItem>,
        /// `IF NOT EXISTS`: an existing table makes the statement a no-op.
        if_not_exists: bool,
    },
    /// `CREATE TABLE t AS SELECT ...` (schema inferred from the query).
    CreateTableAs {
        db: Option<String>,
        name: String,
        query: Box<Select>,
        if_not_exists: bool,
    },
    Insert {
        db: Option<String>,
        table: String,
        columns: Option<Vec<String>>,
        source: InsertSource,
    },
    Update {
        db: Option<String>,
        table: String,
        /// `(column, expression)` pairs in written order.
        sets: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
    },
    Delete {
        db: Option<String>,
        table: String,
        where_clause: Option<Expr>,
    },
    Begin,
    Commit,
    Rollback,
    CreateView {
        db: Option<String>,
        name: String,
        cols: Option<Vec<String>>,
        query: Box<Select>,
    },
    DropTable {
        db: Option<String>,
        name: String,
        /// `IF EXISTS`: a missing table makes the statement a no-op.
        if_exists: bool,
    },
    DropView {
        db: Option<String>,
        name: String,
    },
    /// `CREATE DATABASE <name>`.
    CreateDatabase {
        name: String,
        if_not_exists: bool,
    },
    /// `DROP DATABASE <name>`.
    DropDatabase {
        name: String,
        if_exists: bool,
    },
    /// `USE <name>`: switch the session's current database.
    Use {
        name: String,
    },
    /// `LOAD EXTENSION '<path>'`: load a native loadable extension (ABI v1)
    /// and register its scalar functions. The path is a string literal kept
    /// exactly as written (never canonicalized by the parser).
    LoadExtension {
        path: String,
    },
    /// `CHECKPOINT`: persist a durable database and roll its WAL (a no-op
    /// for in-memory databases). Not a reserved word: only the bare
    /// statement form is recognized.
    Checkpoint,
    /// `SHOW TABLES`: the current database's tables, sorted by name.
    ShowTables,
    /// `SHOW DATABASES`: every database in the store, sorted by name.
    ShowDatabases,
    /// `DESCRIBE <table>`: DuckDB-shaped column metadata.
    Describe {
        db: Option<String>,
        name: String,
    },
    /// `EXPLAIN <select>`: print the bound (optimized) plan.
    Explain(Box<Select>),
    Select(Select),
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    /// `VALUES (...), (...)`.
    Values(Vec<Vec<Expr>>),
    /// `SELECT ...` (or WITH ... SELECT).
    Select(Box<Select>),
}

/// One `CREATE TABLE` item list entry: a column definition or a table-level
/// constraint. Constraints are **declarative metadata**: they are recorded in
/// the catalog and used by the optimizer, never enforced on writes (like
/// DuckDB's informational foreign keys).
#[derive(Debug, Clone, PartialEq)]
pub enum TableItem {
    Column(ColumnDef),
    Constraint(TableConstraint),
}

/// A column definition with its inline constraints:
/// `a INTEGER PRIMARY KEY`, `b INTEGER REFERENCES u(x) NOT NULL`, ...
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub dtype: crate::types::DataType,
    pub constraints: Vec<ColumnConstraint>,
}

/// `CONSTRAINT name <constraint>` (the name is optional).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnConstraint {
    pub name: Option<String>,
    pub kind: ColumnConstraintKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraintKind {
    PrimaryKey,
    Unique,
    NotNull,
    /// `REFERENCES t [(cols)]` — the referenced column list defaults to the
    /// referenced table's primary key at bind time.
    References {
        ref_table: RefTable,
        ref_columns: Option<Vec<String>>,
    },
}

/// A table-level constraint (`PRIMARY KEY (a, b)`, `FOREIGN KEY (a)
/// REFERENCES u(x)`, `UNIQUE (a)`, ...). `RELY` / `[NOT] ENFORCED` suffixes
/// are accepted and dropped: every constraint here is declarative metadata.
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: RefTable,
        ref_columns: Option<Vec<String>>,
    },
}

/// A referenced table name: `u` or `db.u`.
#[derive(Debug, Clone, PartialEq)]
pub struct RefTable {
    pub db: Option<String>,
    pub name: String,
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
    /// `OFFSET <n>`: skip the first `n` rows (valid alone or with LIMIT).
    pub offset: Option<usize>,
    /// Set-operation branches, left-associative in written order:
    /// `UNION [ALL|DISTINCT]`, `INTERSECT [ALL|DISTINCT]`,
    /// `EXCEPT [ALL|DISTINCT]`. `INTERSECT` binds tighter than
    /// `UNION`/`EXCEPT` (DuckDB precedence).
    pub unions: Vec<(SetOp, Select)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
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
    /// A possibly-qualified table reference (`db.table`).
    Table {
        db: Option<String>,
        name: String,
    },
    Derived(Box<Select>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub source: FromSource,
    pub alias: Option<String>,
    /// Chained joins, left-deep, in written order: each `ON` condition sees
    /// every table to its left plus its own right-hand source.
    pub joins: Vec<JoinClause>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    /// `LEFT [OUTER] JOIN`: preserve every left row (NULL-extend unmatched).
    Left,
    /// `RIGHT [OUTER] JOIN`: preserve every right row (NULL-extend unmatched).
    Right,
    /// `FULL [OUTER] JOIN`: preserve rows from both sides.
    Full,
    /// `CROSS JOIN`: the cartesian product (never has an `ON`).
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub kind: JoinKind,
    pub source: FromSource,
    pub alias: Option<String>,
    /// `ON` condition; `None` for CROSS JOIN, which has no condition.
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub expr: Expr,
    /// `DESC` (default ASC).
    pub desc: bool,
    /// `NULLS FIRST` / `NULLS LAST`; `None` = the default (NULLS LAST, both
    /// directions, matching DuckDB's `default_null_order`).
    pub nulls_first: Option<bool>,
}

/// A bound order key: direction + NULL placement (the resolved `OrderItem`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortSpec {
    pub desc: bool,
    /// `false` = NULLS LAST (DuckDB's default for ASC and DESC).
    pub nulls_first: bool,
}

impl SortSpec {
    /// Resolve an AST order item's explicit/default NULL placement.
    pub fn of(item: &OrderItem) -> SortSpec {
        SortSpec {
            desc: item.desc,
            nulls_first: item.nulls_first.unwrap_or(false),
        }
    }
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
    pub mode: FrameMode,
    pub start: FrameBound,
    pub end: FrameBound,
}

/// How frame offsets count: `ROWS` counts rows; `RANGE` counts units of the
/// single ORDER BY expression's value (peers stay together).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameMode {
    Rows,
    Range,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameBound {
    UnboundedPreceding,
    /// `n PRECEDING` (ROWS: `n` rows; RANGE: value offset `n`).
    Preceding(usize),
    CurrentRow,
    /// `n FOLLOWING` (ROWS: `n` rows; RANGE: value offset `n`).
    Following(usize),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Value),
    /// `?` — a positional parameter placeholder in a prepared statement.
    ///
    /// The payload is the placeholder's byte offset in the statement source
    /// (the `?` character itself), as captured by the parser. Byte offsets
    /// are unique per placeholder, which makes them a stable identity through
    /// desugaring (a `BETWEEN`/`IN` desugar clones subtrees, so one
    /// placeholder can appear several times in the tree).
    ///
    /// [`crate::sql::parse_prepared`] renumbers every node in textual order
    /// (each payload becomes its 0-based rank), which is the index the bind
    /// API uses. Plain [`crate::sql::parse_statements`] does the same, so a
    /// `?` outside a prepared statement is a clear bind-time error, never a
    /// parse error.
    Parameter(usize),
    /// Bare `*` (only valid as a function arg for `COUNT(*)`).
    Star,
    Column {
        table: Option<String>,
        name: String,
    },
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
        /// `f(DISTINCT x)` — only `COUNT(DISTINCT x)` is supported (checked
        /// in the binder).
        distinct: bool,
    },
    /// `f(...) OVER ([PARTITION BY ...] [ORDER BY ...] [ROWS/RANGE ...])`.
    Window {
        name: String,
        args: Vec<Expr>,
        over: OverClause,
        /// Parsed for a clear binder error (DISTINCT windows are not
        /// supported, matching DuckDB).
        distinct: bool,
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
    /// `a || b` — strict string concatenation (NULL if either side is NULL).
    Concat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Negate,
    Plus,
}
