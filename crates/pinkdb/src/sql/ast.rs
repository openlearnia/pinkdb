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
        /// `ON CONFLICT …` / `INSERT OR REPLACE|IGNORE` (wave 95b).
        on_conflict: Option<OnConflict>,
        returning: Option<Vec<Projection>>,
    },
    Update {
        db: Option<String>,
        table: String,
        /// `(target, expression)` pairs in written order. The target may be
        /// `column`, `table.column` or `db.table.column` (wave 94); a
        /// qualifier must name this statement's own target table.
        sets: Vec<(SetTarget, Expr)>,
        where_clause: Option<Expr>,
        /// `UPDATE … FROM <from>` (wave 95b): the source rows the SET
        /// expressions and WHERE may also read.
        from: Option<Box<FromClause>>,
        returning: Option<Vec<Projection>>,
    },
    Delete {
        db: Option<String>,
        table: String,
        where_clause: Option<Expr>,
        /// `DELETE … USING <from>` (wave 95b).
        using: Option<Box<FromClause>>,
        returning: Option<Vec<Projection>>,
    },
    /// `MERGE INTO <target> USING <source> ON <cond> <when clauses>`
    /// (wave 95b; DuckDB 2.0's shape).
    Merge {
        db: Option<String>,
        table: String,
        alias: Option<String>,
        using: FromClause,
        on: Expr,
        clauses: Vec<MergeClause>,
        returning: Option<Vec<Projection>>,
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
    /// `SET VARIABLE <name> = <expr>`: bind a session variable that
    /// `getvariable('name')` and `$name` read back.
    ///
    /// Variables are bind-time values (they are spliced as literals, never as
    /// SQL text) and live for the session, not the transaction.
    SetVariable {
        name: String,
        value: Expr,
    },
    /// `LOAD EXTENSION '<path>'`: load a native loadable extension (ABI v1)
    /// and register its scalar functions. The path is a string literal kept
    /// exactly as written (never canonicalized by the parser).
    LoadExtension {
        path: String,
    },
    /// `INSTALL <name> [FROM '<repository>']` / `FORCE INSTALL …` (wave 93):
    /// resolve, download, signature- and SHA-256-verify, cache, and load an
    /// extension from the extension repository (needs the `repo` feature).
    /// `from` overrides the configured repositories; `force` re-downloads an
    /// identical cached copy.
    InstallExtension {
        name: String,
        from: Option<String>,
        force: bool,
    },
    /// `LOAD <name>` (wave 93): load an already installed extension from the
    /// local cache (the name `INSTALL`/`.install` saved it under).
    LoadInstalledExtension {
        name: String,
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
    Explain(Box<Statement>),
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
    /// `CREATE [OR REPLACE] TRIGGER name {BEFORE|AFTER} <event>[ OR …] ON table
    /// [REFERENCING OLD TABLE AS o NEW TABLE AS n] [FOR EACH {ROW|STATEMENT}]
    /// [WHEN (cond)] EXECUTE {PROCEDURE p(args) | <statement>}` (wave 100).
    ///
    /// The action is an ordinary statement: `EXECUTE PROCEDURE p(args)` is a
    /// [`Statement::Call`], so there is no second action kind. The trigger
    /// lives in the table's database; `name` is unqualified.
    CreateTrigger {
        db: Option<String>,
        name: String,
        /// The table the trigger fires on.
        table: String,
        timing: crate::trigger::TriggerTiming,
        /// The events it fires for, in written order (`INSERT OR UPDATE`).
        events: Vec<crate::trigger::TriggerEvent>,
        /// The `OF` column list of an `UPDATE OF a, b` event.
        update_columns: Vec<String>,
        level: crate::trigger::TriggerLevel,
        /// `REFERENCING OLD TABLE AS <name>` — an AFTER STATEMENT relation.
        old_table: Option<String>,
        /// `REFERENCING NEW TABLE AS <name>` — an AFTER STATEMENT relation.
        new_table: Option<String>,
        when: Option<Expr>,
        action: Box<Statement>,
        or_replace: bool,
    },
    /// `DROP TRIGGER [IF EXISTS] name [ON table]` (wave 100).
    DropTrigger {
        db: Option<String>,
        name: String,
        table: Option<String>,
        if_exists: bool,
    },
    /// `SHOW TRIGGERS [ON table]` (wave 100).
    ShowTriggers {
        db: Option<String>,
        table: Option<String>,
    },
    /// `DO '<body>'` (wave 83): run an anonymous SQL procedural block
    /// (`BEGIN ... END`, the procedure-body language) once — atomic like a
    /// `CALL`, no parameters, no transaction control. It is also how a
    /// procedure's committed segments are written to the WAL.
    Do {
        body: String,
    },
    /// `CREATE MACRO name(params) AS <body>` (wave 77): a stored SQL macro.
    ///
    /// The body is a template, not a query: `AS expr` is a scalar macro and
    /// `AS TABLE SELECT ...` a table macro. Parameters are untyped (DuckDB
    /// macros are too) and may carry a constant `:=` default. The binder
    /// splices argument ASTs into the body at every use — no SQL text is ever
    /// generated from them, and nested macro calls resolve at the same bind.
    CreateMacro {
        db: Option<String>,
        name: String,
        params: Vec<MacroParam>,
        body: MacroBody,
        /// `OR REPLACE`: an existing macro of that name is replaced.
        or_replace: bool,
        /// `IF NOT EXISTS`: an existing macro makes this a no-op.
        if_not_exists: bool,
    },
    /// `DROP MACRO [IF EXISTS] name`.
    DropMacro {
        db: Option<String>,
        name: String,
        if_exists: bool,
    },
    /// `SHOW MACROS`: the session database's macros.
    ShowMacros,
    /// `DESCRIBE MACRO name`: one macro's DuckDB-shaped function metadata.
    DescribeMacro {
        db: Option<String>,
        name: String,
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

/// One macro parameter declaration (wave 77): `name [:= default]`.
///
/// A default is a constant expression (DuckDB's rule: no columns, aggregates
/// or subqueries). It is spliced into the body exactly like an argument when
/// the call omits the parameter, so it may refer to functions but never to the
/// caller's table scope.
#[derive(Debug, Clone, PartialEq)]
pub struct MacroParam {
    pub name: String,
    pub default: Option<Expr>,
}

/// A macro body template: `AS expr` (scalar) or `AS TABLE SELECT ...` (table).
#[derive(Debug, Clone, PartialEq)]
pub enum MacroBody {
    Scalar(Expr),
    Table(Box<Select>),
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
    /// `DISTINCT ON (a, b)` — keep the first row per key in ORDER BY order.
    /// `None` for a plain `DISTINCT` (`distinct` flag) or no dedupe at all.
    pub distinct_on: Option<Vec<Expr>>,
    pub projections: Vec<Projection>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    /// `ROLLUP(...)`, `CUBE(...)` and `GROUPING SETS (...)` items, in written
    /// order after the plain keys. Empty = one plain grouping set.
    pub group_sets: Vec<GroupSet>,
    pub having: Option<Expr>,
    /// `QUALIFY <cond>` — a filter over the window results, evaluated before
    /// the select list is projected, so it sees select aliases.
    pub qualify: Option<Expr>,
    /// `WINDOW w AS (...)` definitions, in written order; `OVER w` refers to
    /// one by name.
    pub windows: Vec<(String, OverClause)>,
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

/// One `GROUP BY` item that expands to several grouping sets.
///
/// The plain expressions of a `GROUP BY` list form the first item and the
/// sets of every such item are combined as a cross product, in written order
/// (`GROUP BY a, ROLLUP(b)` groups by `{a,b}` and `{a}`).
#[derive(Debug, Clone, PartialEq)]
pub enum GroupSet {
    /// `ROLLUP(a, b)` → `{a,b}`, `{a}`, `{}`.
    Rollup(Vec<Expr>),
    /// `CUBE(a, b)` → every subset of the keys.
    Cube(Vec<Expr>),
    /// `GROUPING SETS ((a, b), (a), ())` exactly as written.
    Sets(Vec<Vec<Expr>>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// `*`
    Star,
    /// `table.*` or `db.table.*` (wave 94). `db` is `None` for the two-part
    /// form; a database qualifier is never present without a table.
    QualifiedStar { db: Option<String>, table: String },
    /// `expr [AS alias]`
    Expr { expr: Expr, alias: Option<String> },
}

/// One `UPDATE ... SET` target: `column`, `table.column` or
/// `db.table.column` (wave 94). `db` implies `table`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetTarget {
    pub db: Option<String>,
    pub table: Option<String>,
    pub column: String,
}

/// `ON CONFLICT [(cols)] DO NOTHING | DO UPDATE SET … [WHERE …]` (wave 95b).
#[derive(Debug, Clone, PartialEq)]
pub struct OnConflict {
    /// The conflict target columns; `None` matches any `PRIMARY KEY`/`UNIQUE`
    /// constraint of the table.
    pub columns: Option<Vec<String>>,
    pub action: OnConflictAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictAction {
    DoNothing,
    DoUpdate {
        sets: Vec<(SetTarget, Expr)>,
        where_clause: Option<Expr>,
    },
    /// `INSERT OR REPLACE`: keep the row, assigning every written column from
    /// the rejected row (`excluded`). Expanded against the target schema by
    /// the binder, which is the only place the column list is known.
    Replace,
}

/// One `WHEN … THEN …` clause of a `MERGE`.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeClause {
    pub matched: MergeMatch,
    /// `WHEN MATCHED AND <cond>`: the clause applies only when it is TRUE.
    pub condition: Option<Expr>,
    pub action: MergeAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeMatch {
    /// `WHEN MATCHED`: a target row with at least one matching source row.
    Matched,
    /// `WHEN NOT MATCHED` / `WHEN NOT MATCHED BY TARGET`.
    NotMatched,
    /// `WHEN NOT MATCHED BY SOURCE`: a target row with no matching source.
    NotMatchedBySource,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MergeAction {
    Update {
        sets: Vec<(SetTarget, Expr)>,
    },
    Delete,
    Insert {
        /// `INSERT (a, b) VALUES (…, …)`; `None` fills every column.
        columns: Option<Vec<String>>,
        values: Vec<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CteDef {
    pub name: String,
    /// `WITH w(a, b) AS ...` renames the output columns.
    pub cols: Option<Vec<String>>,
    pub query: CteBody,
    /// `WITH RECURSIVE`: `query` must be `anchor UNION [ALL] step`, and the
    /// step may reference `name` (the previous iteration's rows).
    pub recursive: bool,
}

/// The body of a `WITH` item: a query, or a data-modifying statement
/// (wave 95b). SQL allows a data-modifying CTE only at the top level of a
/// statement's `WITH` — the binder enforces that.
#[derive(Debug, Clone, PartialEq)]
pub enum CteBody {
    Query(Box<Select>),
    Dml(Box<Statement>),
}

impl CteBody {
    /// The query body; `None` for a data-modifying body.
    pub fn query_select(&self) -> Option<&Select> {
        match self {
            CteBody::Query(q) => Some(q),
            CteBody::Dml(_) => None,
        }
    }

    pub fn query_select_mut(&mut self) -> Option<&mut Select> {
        match self {
            CteBody::Query(q) => Some(q),
            CteBody::Dml(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FromSource {
    /// A possibly-qualified table reference (`db.table`).
    Table {
        db: Option<String>,
        name: String,
    },
    Derived(Box<Select>),
    /// A table-function call in FROM position (wave 70): the binder offers
    /// it to every registered [`crate::table_source::TableSource`].
    Function {
        name: String,
        args: Vec<TableFunctionCallArg>,
    },
    /// A bare string literal in FROM position — DuckDB's *replacement scan*
    /// (`FROM 'file.parquet'`). The binder offers the path to every
    /// registered source's `bind_path` hook.
    Path(String),
    /// A `VALUES` list in FROM position: `(VALUES (1,'a'), (2,'b')) t(x,y)`.
    /// Rows may be empty (`VALUES ()` is a one-row, zero-column relation).
    Values(Vec<Vec<Expr>>),
}

/// One argument of a [`FromSource::Function`] call, as parsed: an expression
/// the binder must evaluate to a literal, optionally named with `:=` or `=>`.
#[derive(Debug, Clone, PartialEq)]
pub struct TableFunctionCallArg {
    pub name: Option<String>,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub source: FromSource,
    pub alias: Option<String>,
    /// `(VALUES ...) AS t(a, b)` / `FROM t AS x(a, b)`: renamed output
    /// columns, positionally.
    pub col_aliases: Option<Vec<String>>,
    /// `AT (VERSION => n)` / `AT (TIMESTAMP => ts)`, written after the alias
    /// (`FROM t AS x AT (VERSION => 1)`), DuckLake's shape. Only table
    /// references may carry one.
    pub at: Option<crate::attached::AtSpec>,
    /// `TABLESAMPLE …` / `USING SAMPLE …` on the head source.
    pub sample: Option<SampleSpec>,
    /// `FROM LATERAL (…)`: the source is bound after the items to its left, so
    /// it may name them — pinkdb's derived plans are uncorrelated, so that is
    /// refused with a named error (see `bind_source_raw`).
    pub lateral: bool,
    /// Chained joins, left-deep, in written order: each `ON` condition sees
    /// every table to its left plus its own right-hand source.
    pub joins: Vec<JoinClause>,
}

/// One `TABLESAMPLE` / `USING SAMPLE` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct SampleSpec {
    pub method: SampleMethod,
    pub size: SampleSize,
    /// `REPEATABLE (seed)`: the same seed samples the same rows.
    pub repeatable: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleMethod {
    /// Whole blocks (here: whole chunks) are kept or dropped together.
    System,
    /// Each row is kept independently.
    Bernoulli,
    /// Exactly `size` rows, drawn without replacement.
    Reservoir,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SampleSize {
    /// `n ROWS`.
    Rows(i64),
    /// `p PERCENT` (or `p%`).
    Percent(f64),
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
    /// `JOIN (VALUES …) v(x, y)`: positional column aliases for the source.
    pub col_aliases: Option<Vec<String>>,
    /// `AT (...)` after this join's table reference (see [`FromClause::at`]).
    pub at: Option<crate::attached::AtSpec>,
    /// `TABLESAMPLE …` / `USING SAMPLE …` on this join's right source.
    pub sample: Option<SampleSpec>,
    /// `JOIN LATERAL (…)` / `, LATERAL (…)`.
    pub lateral: bool,
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
    /// The window this specification builds on (`OVER (w ORDER BY ...)` and
    /// `WINDOW v AS (w ...)`); the base's clauses are inherited where this one
    /// leaves them empty.
    pub base: Option<String>,
    pub partition: Vec<Expr>,
    pub order: Vec<OrderItem>,
    /// `None` = default frame (whole partition, or cumulative with ORDER BY).
    pub frame: Option<FrameSpec>,
}

/// What `OVER` names: an inline specification (`OVER (PARTITION BY ...)`) or
/// a named window from the query's `WINDOW` clause (`OVER w`).
#[derive(Debug, Clone, PartialEq)]
pub enum OverSpec {
    Inline(OverClause),
    Named(String),
}

impl OverSpec {
    /// The inline spec, if this is not a reference to a named window.
    pub fn inline(&self) -> Option<&OverClause> {
        match self {
            OverSpec::Inline(over) => Some(over),
            OverSpec::Named(_) => None,
        }
    }

    /// The `PARTITION BY` expressions (empty for a named window, whose
    /// definition the binder resolves).
    pub fn partition(&self) -> &[Expr] {
        self.inline().map_or(&[], |over| &over.partition)
    }

    /// The `ORDER BY` items (empty for a named window).
    pub fn order(&self) -> &[OrderItem] {
        self.inline().map_or(&[], |over| &over.order)
    }

    pub fn partition_mut(&mut self) -> &mut [Expr] {
        match self {
            OverSpec::Inline(over) => &mut over.partition,
            OverSpec::Named(_) => &mut [],
        }
    }

    pub fn order_mut(&mut self) -> &mut [OrderItem] {
        match self {
            OverSpec::Inline(over) => &mut over.order,
            OverSpec::Named(_) => &mut [],
        }
    }
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
    /// `column`, `table.column` or `db.table.column` (wave 94). A database
    /// qualifier implies a table qualifier.
    Column {
        db: Option<String>,
        table: Option<String>,
        name: String,
    },
    /// `table.*` or `db.table.*` in expression position (rejected by the
    /// binder outside a projection; the projection parser owns the accepted
    /// shapes).
    QualifiedStar {
        db: Option<String>,
        table: String,
    },
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
        /// `f(...) FILTER (WHERE cond)` — aggregates only.
        filter: Option<Box<Expr>>,
    },
    /// `name := expr` (also `name => expr`) inside a function call's argument
    /// list (wave 77). Only macro calls consume these; the binder refuses one
    /// in a built-in, aggregate, window or extension call.
    NamedArg {
        name: String,
        expr: Box<Expr>,
    },
    /// `f(...) OVER ([PARTITION BY ...] [ORDER BY ...] [ROWS/RANGE ...])`.
    Window {
        name: String,
        args: Vec<Expr>,
        over: OverSpec,
        /// Parsed for a clear binder error (DISTINCT windows are not
        /// supported, matching DuckDB).
        distinct: bool,
        /// `f(...) FILTER (WHERE cond) OVER (...)` — aggregate windows only.
        filter: Option<Box<Expr>>,
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
    /// `TRY_CAST(x AS type)` — NULL when the cast would fail.
    TryCast {
        expr: Box<Expr>,
        dtype: crate::types::DataType,
    },
    /// `[a, b, c]` — a fixed-size ARRAY literal. The element type and length
    /// come from the position it lands in (a `CAST`, an INSERT, a comparison
    /// with an ARRAY column): a bare literal has no type of its own.
    ArrayLiteral(Vec<Expr>),
    /// `base[index]` — a VARIANT field/array element, a VARCHAR character or
    /// a list element (see the binder for the accepted base types).
    Subscript {
        base: Box<Expr>,
        index: Box<Expr>,
    },
    /// `base[from:to]` — a 1-based, inclusive VARCHAR slice.
    Slice {
        base: Box<Expr>,
        from: Option<Box<Expr>>,
        to: Option<Box<Expr>>,
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
    /// `a IS DISTINCT FROM b` — NULL-safe inequality (NULLs are equal to
    /// each other and distinct from every value).
    IsDistinctFrom,
    IsNotDistinctFrom,
    /// `a || b` — strict string concatenation (NULL if either side is NULL).
    Concat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Negate,
    Plus,
}
