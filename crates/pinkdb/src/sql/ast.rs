//! SQL AST for the v1 surface.
//!
//! Statements: `CREATE TABLE`, `INSERT`, `SELECT` (with an optional single
//! equi-`JOIN` — INNER / LEFT [OUTER] / RIGHT [OUTER]).

use crate::types::{RefreshPolicy, Value};

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
    /// `COPY <table> TO '<path>' [(FORMAT <format>)]` (wave 71): write one
    /// catalog table to a registered file format. `format: None` infers the
    /// format from the target path's extension (DuckDB's rule). External
    /// side effect only: no store mutation, never WAL-logged.
    CopyTo {
        db: Option<String>,
        table: String,
        path: String,
        format: Option<String>,
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
    /// `ALTER TABLE` (wave 65): add a column (with an optional constant
    /// `DEFAULT`), drop a column, rename a column, or rename the table.
    AlterTable {
        /// Explicit `db.` qualifier, if written (resolved at bind).
        db: Option<String>,
        name: String,
        op: AlterTableOp,
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
    /// `CREATE MATERIALIZED VIEW [IF NOT EXISTS] name [REFRESH <policy>] AS
    /// SELECT ...`: the query is evaluated once at creation; the view is a
    /// real table until the next refresh.
    CreateMaterializedView {
        db: Option<String>,
        name: String,
        query: Box<Select>,
        if_not_exists: bool,
        /// The refresh policy (`REFRESH MANUAL | EVERY INTERVAL '…' | ON
        /// COMMIT`); defaults to `MANUAL`.
        policy: RefreshPolicy,
    },
    /// `REFRESH [MATERIALIZED VIEW] name`.
    RefreshMaterializedView {
        db: Option<String>,
        name: String,
    },
    /// `REFRESH MATERIALIZED VIEWS`: recompute every materialized view of the
    /// session database, all-or-nothing (every query is evaluated before any
    /// view is written).
    RefreshMaterializedViews,
    /// `DROP MATERIALIZED VIEW [IF EXISTS] name`.
    DropMaterializedView {
        db: Option<String>,
        name: String,
        if_exists: bool,
    },
    /// `SHOW MATERIALIZED VIEWS` (wave 80): name, schedule, last refresh,
    /// refresh count, staleness and the last refresh error, sorted by name.
    ShowMaterializedViews,
    /// `CREATE INDEX [IF NOT EXISTS] <name> ON <table>(<col>)`.
    CreateIndex {
        db: Option<String>,
        name: String,
        table: String,
        column: String,
        if_not_exists: bool,
    },
    /// `DROP INDEX [IF EXISTS] <name>`.
    DropIndex {
        db: Option<String>,
        name: String,
        if_exists: bool,
    },
    /// `DESCRIBE <table>`: DuckDB-shaped column metadata.
    Describe {
        db: Option<String>,
        name: String,
    },
    /// `EXPLAIN <select>`: print the bound (optimized) plan.
    Explain(Box<Select>),
    /// `CREATE [OR REPLACE] PROCEDURE name(params) [LANGUAGE lang] AS <body>`
    /// (wave 44). The body is the string literal's text (preferably
    /// dollar-quoted, `$$ BEGIN ... END $$`); the binder parses it
    /// ([`crate::procedure::parse_body`]) and stores the definition in the
    /// database's catalog.
    CreateProcedure {
        db: Option<String>,
        name: String,
        /// Typed parameters, in written order.
        params: Vec<(String, crate::types::DataType)>,
        /// `LANGUAGE SQL | JAVASCRIPT`; defaults to SQL.
        language: crate::procedure::ProcLanguage,
        /// The body text exactly as written inside the quotes.
        body: String,
        /// `OR REPLACE`: an existing procedure of that name is replaced.
        or_replace: bool,
    },
    /// `DROP PROCEDURE [IF EXISTS] name`.
    DropProcedure {
        db: Option<String>,
        name: String,
        if_exists: bool,
    },
    /// `CALL name(args...)`: run a stored procedure. The arguments are
    /// constant expressions (evaluated against an empty scope); `?`
    /// placeholders are allowed in a prepared statement.
    Call {
        db: Option<String>,
        name: String,
        args: Vec<Expr>,
    },
    /// `CREATE USER [IF NOT EXISTS] name PASSWORD '<plaintext>'` (wave 48).
    /// `PASSWORD_HASH '<phc>'` is the pre-hashed form the WAL replay uses.
    CreateUser {
        name: String,
        /// The as-written password: hashed on the first bind (memoized on
        /// this AST, so the multi-writer rebinds derive it once).
        password: crate::rbac::PasswordSource,
        if_not_exists: bool,
    },
    /// `ALTER USER name PASSWORD '<plaintext>'`: replace a user's password.
    AlterUser {
        name: String,
        password: crate::rbac::PasswordSource,
    },
    /// `DROP USER [IF EXISTS] name`.
    DropUser {
        name: String,
        if_exists: bool,
    },
    /// `CREATE ROLE [IF NOT EXISTS] name`.
    CreateRole {
        name: String,
        if_not_exists: bool,
    },
    /// `DROP ROLE [IF EXISTS] name`.
    DropRole {
        name: String,
        if_exists: bool,
    },
    /// `GRANT … TO …` (wave 48): a privilege grant or a role membership.
    Grant {
        target: crate::rbac::GrantTarget,
    },
    /// `REVOKE … FROM …`: the mirror of [`Statement::Grant`].
    Revoke {
        target: crate::rbac::GrantTarget,
    },
    Select(Select),
}

/// One `ALTER TABLE` operation (wave 65).
///
/// `ADD COLUMN` carries the column definition (type + inline constraints; the
/// binder accepts only `NOT NULL`, which needs a non-NULL `DEFAULT` on a
/// non-empty table — DuckDB's rule) plus the optional `DEFAULT` expression.
/// The default is bound as a constant expression and evaluated once when the
/// statement runs; every existing row (and every later `INSERT` that omits
/// the column) reads that value.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOp {
    AddColumn {
        column: ColumnDef,
        /// `DEFAULT <expr>`: bound in an empty scope (constants only).
        default: Option<Expr>,
        /// `IF NOT EXISTS`: an existing column makes the statement a no-op.
        if_not_exists: bool,
    },
    /// `DROP [COLUMN] <name> [IF EXISTS]`.
    DropColumn { name: String, if_exists: bool },
    /// `RENAME COLUMN <from> TO <to>`.
    RenameColumn { from: String, to: String },
    /// `RENAME TO <to>`: rename the whole table.
    RenameTable { to: String },
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
    /// A file source (wave 71): a string-literal replacement scan
    /// (`FROM 'f.arrow'`, `format: None`: resolved by file extension) or a
    /// `read_*` table-function call (`FROM read_arrow('f.arrow')`,
    /// `format: Some("read_arrow")`: resolved by the registered reader name).
    /// Both resolve through the store's registered file formats at bind time.
    File {
        path: String,
        /// The lowercased `read_*` function name, when written.
        function: Option<String>,
    },
    Derived(Box<Select>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub source: FromSource,
    pub alias: Option<String>,
    /// `AT (VERSION => n)` / `AT (TIMESTAMP => ts)`, written after the alias
    /// (`FROM t AS x AT (VERSION => 1)`), DuckLake's shape. Only table
    /// references may carry one.
    pub at: Option<crate::attached::AtSpec>,
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
    /// `AT (...)` after this join's table reference (see [`FromClause::at`]).
    pub at: Option<crate::attached::AtSpec>,
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
