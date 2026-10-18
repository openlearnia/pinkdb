//! Logical plan: Scan → Filter → Join → Aggregate → Window → Project →
//! Distinct → Sort → Limit (plus DDL/DML and set unions).

use std::collections::HashMap;

use crate::attached::AtSpec;
use crate::sql::ast::{JoinKind, Select, SetOp, SortSpec};
use crate::sql::binder::{BoundAgg, BoundExpr, BoundWindow};
use crate::types::{DataType, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    CreateTable {
        /// Target database (binder-resolved; `None` = session default).
        db: Option<String>,
        name: String,
        schema: Vec<(String, DataType)>,
        /// Declarative constraints (PRIMARY KEY / UNIQUE / FOREIGN KEY / NOT
        /// NULL): metadata only, never enforced. Already validated by the
        /// binder (foreign keys against the referenced table's keys).
        constraints: crate::storage::TableConstraints,
        /// `IF NOT EXISTS`: an existing table makes this a no-op.
        if_not_exists: bool,
    },
    /// `CREATE TABLE t AS SELECT ...`: schema from the query's output.
    CreateTableAs {
        db: Option<String>,
        name: String,
        query: Box<LogicalPlan>,
        if_not_exists: bool,
    },
    /// `ALTER TABLE` (wave 65): a bound add/drop/rename operation on one store
    /// table. The `ADD COLUMN` default is a bound constant expression the
    /// executor evaluates once (and stores as the column's default).
    AlterTable {
        /// Target database (binder-resolved; `None` = session default).
        db: Option<String>,
        name: String,
        op: AlterTablePlan,
    },
    Insert {
        db: Option<String>,
        table: String,
        /// One bound expr per table column per row (constants; missing
        /// columns are NULL). Evaluated against an empty single-row chunk.
        rows: Vec<Vec<BoundExpr>>,
    },
    /// `INSERT INTO t [(cols)] SELECT ...`.
    InsertSelect {
        db: Option<String>,
        table: String,
        /// Target column indices in written order (`None` = every column).
        columns: Option<Vec<usize>>,
        query: Box<LogicalPlan>,
    },
    Update {
        db: Option<String>,
        table: String,
        /// `(column index, value expression)` — expressions address table
        /// columns and evaluate simultaneously per row (pre-update values).
        sets: Vec<(usize, BoundExpr)>,
        predicate: Option<BoundExpr>,
    },
    Delete {
        db: Option<String>,
        table: String,
        predicate: Option<BoundExpr>,
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
    CreateMaterializedView {
        db: Option<String>,
        name: String,
        query: Box<LogicalPlan>,
        if_not_exists: bool,
        /// The declared refresh policy (wave 80).
        policy: crate::types::RefreshPolicy,
        /// The defining statement's source text (`CREATE MATERIALIZED VIEW
        /// ... AS SELECT ...`), attached by [`LogicalPlan::with_mv_source`]
        /// and persisted in `.pink`: a bound plan cannot be rendered back to
        /// SQL, so this is what a reopen rebinds.
        source: String,
    },
    RefreshMaterializedView {
        db: Option<String>,
        name: String,
    },
    /// `REFRESH MATERIALIZED VIEWS`: every view of `db`, all-or-nothing.
    RefreshMaterializedViews {
        db: Option<String>,
    },
    DropMaterializedView {
        db: Option<String>,
        name: String,
        if_exists: bool,
    },
    /// `SHOW MATERIALIZED VIEWS` of `db` (wave 80).
    ShowMaterializedViews {
        db: Option<String>,
    },
    DropTable {
        db: Option<String>,
        name: String,
        /// `IF EXISTS`: a missing table makes this a no-op.
        if_exists: bool,
    },
    CreateIndex {
        db: Option<String>,
        name: String,
        table: String,
        column: usize,
        if_not_exists: bool,
    },
    DropIndex {
        db: Option<String>,
        name: String,
        if_exists: bool,
    },
    IndexScan {
        db: Option<String>,
        table: String,
        index: String,
        value: BoundExpr,
    },
    DropView {
        db: Option<String>,
        name: String,
    },
    /// `CREATE DATABASE <name>` (store-level; never inside a transaction).
    CreateDatabase {
        name: String,
        if_not_exists: bool,
    },
    /// `DROP DATABASE <name>` (store-level; never inside a transaction).
    DropDatabase {
        name: String,
        if_exists: bool,
    },
    /// `USE <name>`: the session layer applies the returned database.
    Use {
        name: String,
    },
    /// `LOAD EXTENSION '<path>'`: load a native extension (ABI v1) and
    /// register its scalar functions. Process-level and not part of any
    /// snapshot — but WAL-logged, because a durable reopen must re-load it.
    LoadExtension {
        path: String,
    },
    /// `CHECKPOINT`: persist + roll the WAL of a durable store (no-op
    /// otherwise). Never WAL-logged and refused inside a transaction.
    Checkpoint,
    /// `SHOW TABLES`: the given database's tables, sorted by name.
    ShowTables {
        db: String,
    },
    /// `SHOW DATABASES`: every database, sorted by name.
    ShowDatabases,
    /// `DESCRIBE <table>`: the resolved schema, printed DuckDB-shaped.
    Describe {
        columns: Vec<DescribeColumn>,
    },
    /// `EXPLAIN <select>`: a printable textual plan (never executed).
    Explain {
        plan: Box<LogicalPlan>,
    },
    /// `CREATE [OR REPLACE] PROCEDURE ...` (wave 44): store the parsed
    /// definition in the target database's catalog. A name that already names
    /// a table, view or materialized view is refused at execution.
    CreateProcedure {
        db: Option<String>,
        name: String,
        def: crate::procedure::ProcedureDef,
        /// `OR REPLACE`: an existing procedure of that name is replaced.
        or_replace: bool,
    },
    /// `DROP PROCEDURE [IF EXISTS] name`.
    DropProcedure {
        db: Option<String>,
        name: String,
        if_exists: bool,
    },
    /// `CALL name(args...)` (wave 44): run the stored procedure. The
    /// arguments were bound as constant expressions; the definition is
    /// resolved against the catalog at execution (a rebind after a lease
    /// refresh always sees the current one).
    ///
    /// The call's *actual* touched set (which tables its statements read and
    /// wrote) is only known after execution — loop conditions are
    /// data-dependent — so the executor records it in
    /// [`crate::storage::PinkDb::proc_frame`] and the logging paths take the
    /// frame's payload from there.
    Call {
        db: Option<String>,
        name: String,
        args: Vec<BoundExpr>,
    },
    /// Set operation (`UNION`/`INTERSECT`/`EXCEPT`, each `[ALL]`): output
    /// names come from the left branch; every column is coerced to the
    /// pairwise common type at execution.
    SetOp {
        op: SetOp,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        types: Vec<DataType>,
    },
    /// Full scan of a table. `db: None` names a CTE (binder-resolved);
    /// `Some(db)` is a catalog table in that database. `at: Some(spec)` is a
    /// time-travel scan: exactly the attached lake table's contents as of
    /// that version/timestamp (`AT (VERSION => n)` / `AT (TIMESTAMP => ts)`).
    Scan {
        db: Option<String>,
        table: String,
        at: Option<AtSpec>,
    },
    /// One empty row — `SELECT` without `FROM`.
    SingleRow,
    Filter {
        input: Box<LogicalPlan>,
        predicate: BoundExpr,
    },
    /// Lexically scoped CTE: evaluate `query`, bind `body` with `name`
    /// visible (shadowing catalog tables), then forget it.
    Cte {
        name: String,
        query: Box<LogicalPlan>,
        body: Box<LogicalPlan>,
    },
    /// Window functions: output is input columns ++ one column per window.
    /// Partition/order/arg expressions address the input (scan-relative, or
    /// Aggregate-output after rebasing — same rules as select items).
    Window {
        input: Box<LogicalPlan>,
        windows: Vec<BoundWindow>,
        /// Input column count (window results start here).
        input_cols: usize,
    },
    /// Equi-join on one or more `left_key = right_key` pairs (all conjuncts
    /// must hold for a pair to match). Output columns are left ++ right. Keys
    /// are side-relative (evaluated against each input); every other
    /// expression above the join uses joined-space indices. NULL keys never
    /// match. `Inner` emits matched pairs only; `Left`/`Right`/`Full`
    /// preserve every row of their side (NULL-extending the other side's
    /// columns when unmatched).
    Join {
        kind: JoinKind,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        left_keys: Vec<BoundExpr>,
        right_keys: Vec<BoundExpr>,
        left_cols: usize,
    },
    /// Nested-loop join: `on` evaluates in joined space (left ++ right
    /// indices); `None` = CROSS JOIN (every pair). Used for non-equi and
    /// composite-residual conditions and for any outer join whose condition
    /// is not a pure conjunction of equi pairs. Output columns are
    /// left ++ right.
    NestedLoopJoin {
        kind: JoinKind,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on: Option<BoundExpr>,
        left_cols: usize,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        /// Group keys, evaluated against the input (scan-relative).
        keys: Vec<BoundExpr>,
        aggs: Vec<BoundAgg>,
    },
    Project {
        input: Box<LogicalPlan>,
        exprs: Vec<BoundExpr>,
        names: Vec<String>,
    },
    Distinct {
        input: Box<LogicalPlan>,
    },
    Sort {
        input: Box<LogicalPlan>,
        /// (key expr, direction + NULL placement) evaluated against the input.
        keys: Vec<(BoundExpr, SortSpec)>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
    },
    /// `OFFSET <n>`: drop the first `n` rows (may sit under `Limit`).
    Offset {
        input: Box<LogicalPlan>,
        offset: usize,
    },
}

/// One bound `ALTER TABLE` operation (wave 65). `ADD COLUMN`'s default is a
/// bound constant expression (evaluated once at execution).
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTablePlan {
    AddColumn {
        column: String,
        dtype: DataType,
        /// `DEFAULT <expr>`: a constant expression (empty scope). `None` =
        /// the DEFAULT of a nullable column (NULL).
        default: Option<BoundExpr>,
        /// `NOT NULL` was declared. A non-empty table needs a non-NULL
        /// default; the engine records the flag (constraints are declarative
        /// metadata, never enforced on writes — wave 41).
        not_null: bool,
        /// `IF NOT EXISTS`: an existing column makes the statement a no-op.
        if_not_exists: bool,
    },
    DropColumn {
        column: String,
        if_exists: bool,
    },
    RenameColumn {
        from: String,
        to: String,
    },
    RenameTable {
        to: String,
    },
}

/// One `DESCRIBE` output row: `(column_name, column_type, null, key)` plus the
/// always-NULL `default`/`extra` columns the executor appends.
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeColumn {
    pub name: String,
    pub dtype: DataType,
    /// `NO` when the column is declared NOT NULL or is part of the PRIMARY KEY
    /// (DuckDB's `null` column).
    pub not_null: bool,
    /// The `key` marker: `PRI` for primary-key columns, `UNI` for UNIQUE ones
    /// (PRI wins), `NULL` otherwise.
    pub key: Option<crate::storage::KeyKind>,
    /// The column's DEFAULT, rendered as a SQL literal (DuckDB's `default`
    /// column; wave 65). `None` = no default. The original expression text is
    /// not retained — the evaluated value is rendered instead.
    pub default: Option<String>,
}

/// True for row-producing queries (safe under a shared read lock).
/// Everything else mutates the store or the transaction state. `USE` is
/// read-only for locking: it only changes session state (the WAL record is
/// appended through the WAL's own lock).
pub fn is_read_only(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Scan { .. }
            | LogicalPlan::SingleRow
            | LogicalPlan::Filter { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::NestedLoopJoin { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Offset { .. }
            | LogicalPlan::Cte { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::SetOp { .. }
            | LogicalPlan::Use { .. }
            | LogicalPlan::ShowTables { .. }
            | LogicalPlan::ShowDatabases
            | LogicalPlan::ShowMaterializedViews { .. }
            | LogicalPlan::Describe { .. }
            | LogicalPlan::Explain { .. }
    )
}

/// Attach the statement's source text to a bound `CREATE MATERIALIZED VIEW`
/// plan (wave 80). The engine persists a view's defining SQL, but a bound plan
/// cannot be rendered back to text, and the statement runner — not the binder
/// — holds the per-statement source. Every other plan passes through
/// unchanged, so callers can wrap every bind result unconditionally.
pub(crate) fn with_mv_source(plan: LogicalPlan, source: Option<&str>) -> LogicalPlan {
    match plan {
        LogicalPlan::CreateMaterializedView {
            db,
            name,
            query,
            if_not_exists,
            policy,
            ..
        } => LogicalPlan::CreateMaterializedView {
            db,
            name,
            query,
            if_not_exists,
            policy,
            source: source.unwrap_or_default().to_string(),
        },
        other => other,
    }
}

/// True for plans whose committed SQL text has to be logged — exactly the
/// statements a durable commit must replay: table data/catalog mutations,
/// database create/drop, `LOAD EXTENSION`, and `USE` (replay starts at
/// `main` and the logged `USE` records switch the replay session). View DDL
/// is exempt: views are session-local and never persisted, so replaying them
/// would not match `save`/`open` semantics. Control plans
/// (`BEGIN`/`COMMIT`/`ROLLBACK`) are also false: transaction boundaries are
/// encoded in record shape.
///
/// Extensions are process-level and are never written into a checkpoint, so
/// a record that calls a dynamic function is additionally prefixed with
/// `LOAD EXTENSION '<path>'` for every extension the *current WAL generation*
/// has not loaded yet (touched-only prefixing; see [`crate::wal::Wal`]). The
/// logged `LOAD EXTENSION` statement itself is never prefixed with itself,
/// but marks its path as covered. A checkpoint resets that coverage: after
/// it, the next record touching an extension re-loads it by path — a missing
/// file at replay time is then a hard recovery error, never a silent drop.
pub fn writes_tables(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::CreateTable { .. }
            | LogicalPlan::CreateTableAs { .. }
            | LogicalPlan::Insert { .. }
            | LogicalPlan::InsertSelect { .. }
            | LogicalPlan::Update { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::AlterTable { .. }
            | LogicalPlan::CreateDatabase { .. }
            | LogicalPlan::DropDatabase { .. }
            | LogicalPlan::Use { .. }
            | LogicalPlan::LoadExtension { .. }
            // Wave 80: materialized-view statements mutate the store (the
            // backing table and the catalog definition), so they are logged:
            // a durable store replays `CREATE/DROP/REFRESH MATERIALIZED VIEW`
            // and lands on identical data. (Wave 43 shipped without this.)
            | LogicalPlan::CreateMaterializedView { .. }
            | LogicalPlan::RefreshMaterializedView { .. }
            | LogicalPlan::RefreshMaterializedViews { .. }
            | LogicalPlan::DropMaterializedView { .. }
            // Wave 44: procedure DDL mutates the catalog (and must replay), and
            // a CALL is logged as a whole: its statements' effects are the
            // call's effect, and replay re-executes the (deterministic) call.
            // A call that wrote nothing is still logged — harmless, and it
            // keeps the write/lease decisions independent of the body's
            // data-dependent control flow.
            | LogicalPlan::CreateProcedure { .. }
            | LogicalPlan::DropProcedure { .. }
            | LogicalPlan::Call { .. }
    )
}

/// Statements refused inside a transaction: store/process-level operations a
/// snapshot commit cannot undo. Returns the SQL statement name for the error
/// message (`None` = allowed).
pub(crate) fn refused_in_transaction(plan: &LogicalPlan) -> Option<&'static str> {
    Some(match plan {
        LogicalPlan::CreateDatabase { .. } => "CREATE DATABASE",
        LogicalPlan::DropDatabase { .. } => "DROP DATABASE",
        LogicalPlan::LoadExtension { .. } => "LOAD EXTENSION",
        LogicalPlan::Checkpoint => "CHECKPOINT",
        // Wave 80: materialized-view statements carry catalog metadata
        // (definition, schedule, refresh counters) that the per-table
        // transaction merge does not cover, and auto-refresh is deferred to
        // the first boundary after COMMIT anyway — keep them outside
        // transactions instead of half-applying them.
        LogicalPlan::CreateMaterializedView { .. } => "CREATE MATERIALIZED VIEW",
        LogicalPlan::RefreshMaterializedView { .. } => "REFRESH MATERIALIZED VIEW",
        LogicalPlan::RefreshMaterializedViews { .. } => "REFRESH MATERIALIZED VIEWS",
        LogicalPlan::DropMaterializedView { .. } => "DROP MATERIALIZED VIEW",
        // Wave 44: a procedure definition is catalog metadata the per-table
        // transaction merge does not carry, exactly like a materialized view's.
        // `CALL` itself is allowed inside a transaction: its statements run
        // there like hand-written statements.
        LogicalPlan::CreateProcedure { .. } => "CREATE PROCEDURE",
        LogicalPlan::DropProcedure { .. } => "DROP PROCEDURE",
        _ => return None,
    })
}

/// True for the statements the optimistic autocommit fast path may execute
/// before taking the lease (wave 45o): plain table DDL and DML, whose
/// in-memory effect can be discarded by a rebuild when the commit turns out
/// to conflict. Everything else that writes stays on the serialized path:
/// view DDL (session-local; a rebuild preserves rather than undoes it),
/// `LOAD EXTENSION` (process registry), `CREATE/DROP DATABASE` (catalog
/// lifetime spans the rebuild), and `CHECKPOINT`.
pub(crate) fn optimistic_eligible(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Insert { .. }
            | LogicalPlan::Update { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::CreateTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::AlterTable { .. }
    )
}

/// Collect every catalog table `plan` reads **or writes**, plus the
/// databases its `CREATE/DROP DATABASE` names, into `out` (wave 45o). This
/// is the set stored in a WAL frame and validated at an optimistic commit:
/// the union matters because a frame that merely *read* a table another
/// transaction wrote could not be replayed honestly on top of it. The
/// matches are exhaustive on purpose — a new plan variant must decide here
/// what it touches.
pub(crate) fn plan_sets(plan: &LogicalPlan, default_db: &str, out: &mut crate::wal::FrameSets) {
    match plan {
        LogicalPlan::CreateDatabase { name, .. } | LogicalPlan::DropDatabase { name, .. } => {
            out.add_database(name);
            return;
        }
        _ => {}
    }
    let mut reads = std::collections::HashSet::new();
    collect_tables_read(plan, default_db, &mut reads);
    for (db, table) in reads {
        out.add_table(&db, &table);
    }
    // Writes: the same `(database, table)` pairs `record_writes` records
    // (view DDL is session-local and never logged; a frame never contains
    // it).
    let mut writes = std::collections::HashSet::new();
    let mut views = std::collections::HashSet::new();
    crate::db::record_writes(plan, default_db, &mut writes, &mut views);
    for (db, table) in writes {
        out.add_table(&db, &table);
    }
}

/// Collect every catalog table `plan` reads, as lowercased
/// `(database, table)` keys, including tables scanned by subqueries in
/// expressions and by DML predicates. CTE scans (`db: None`) are skipped:
/// their bodies are walked through [`LogicalPlan::Cte`]. Used by COMMIT's
/// read-set validation. The matches are exhaustive on purpose: a new plan or
/// expression variant must decide here whether it reads tables.
pub(crate) fn collect_tables_read(
    plan: &LogicalPlan,
    default_db: &str,
    out: &mut std::collections::HashSet<(String, String)>,
) {
    let key = |db: &Option<String>, table: &str| {
        (
            db.as_deref().unwrap_or(default_db).to_ascii_lowercase(),
            table.to_ascii_lowercase(),
        )
    };
    match plan {
        LogicalPlan::Scan {
            db: Some(db),
            table,
            ..
        } => {
            out.insert((db.to_ascii_lowercase(), table.to_ascii_lowercase()));
        }
        LogicalPlan::Scan { db: None, .. }
        | LogicalPlan::SingleRow
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::CreateDatabase { .. }
        | LogicalPlan::DropDatabase { .. }
        | LogicalPlan::Use { .. }
        | LogicalPlan::LoadExtension { .. }
        | LogicalPlan::ShowTables { .. }
        | LogicalPlan::ShowDatabases
        | LogicalPlan::ShowMaterializedViews { .. }
        | LogicalPlan::Describe { .. }
        | LogicalPlan::Checkpoint
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::RefreshMaterializedView { .. }
        | LogicalPlan::RefreshMaterializedViews { .. }
        | LogicalPlan::DropMaterializedView { .. }
        | LogicalPlan::CreateProcedure { .. }
        | LogicalPlan::DropProcedure { .. } => {}
        // A materialized view's defining query is part of the statement: the
        // backing table is computed from exactly these reads.
        LogicalPlan::CreateMaterializedView { query, .. } => {
            collect_tables_read(query, default_db, out);
        }
        LogicalPlan::IndexScan { db, table, .. } => {
            out.insert(key(db, table));
        }
        // A call's own arguments may hold subqueries (they bind in an empty
        // scope); the tables its *body* touches are only known after
        // execution and are reported through `PinkDb::proc_frame`.
        LogicalPlan::Call { args, .. } => {
            for e in args {
                expr_tables_read(e, default_db, out);
            }
        }
        // `CREATE TABLE` reads the declared foreign-key targets: the binder
        // validated their keys against the catalog, so the statement depends
        // on them (a concurrent DROP/recreate that changes the key is a
        // conflict for the write-set validation).
        LogicalPlan::CreateTable { constraints, .. } => {
            for fk in &constraints.foreign_keys {
                out.insert((fk.ref_db.clone(), fk.ref_table.clone()));
            }
        }
        LogicalPlan::Explain { plan } => collect_tables_read(plan, default_db, out),
        LogicalPlan::CreateTableAs { query, .. } => collect_tables_read(query, default_db, out),
        LogicalPlan::InsertSelect { query, .. } => {
            collect_tables_read(query, default_db, out);
        }
        LogicalPlan::Insert { rows, .. } => {
            for e in rows.iter().flatten() {
                expr_tables_read(e, default_db, out);
            }
        }
        LogicalPlan::Update {
            db,
            table,
            sets,
            predicate,
        } => {
            out.insert(key(db, table));
            for (_, e) in sets {
                expr_tables_read(e, default_db, out);
            }
            if let Some(p) = predicate {
                expr_tables_read(p, default_db, out);
            }
        }
        LogicalPlan::Delete {
            db,
            table,
            predicate,
        } => {
            out.insert(key(db, table));
            if let Some(p) = predicate {
                expr_tables_read(p, default_db, out);
            }
        }
        LogicalPlan::SetOp { left, right, .. } => {
            collect_tables_read(left, default_db, out);
            collect_tables_read(right, default_db, out);
        }
        LogicalPlan::Filter { input, predicate } => {
            collect_tables_read(input, default_db, out);
            expr_tables_read(predicate, default_db, out);
        }
        LogicalPlan::Cte { query, body, .. } => {
            collect_tables_read(query, default_db, out);
            collect_tables_read(body, default_db, out);
        }
        LogicalPlan::Window { input, windows, .. } => {
            collect_tables_read(input, default_db, out);
            for w in windows {
                let exprs = w
                    .arg
                    .iter()
                    .chain(w.partition.iter())
                    .chain(w.order.iter().map(|(e, _)| e));
                for e in exprs {
                    expr_tables_read(e, default_db, out);
                }
            }
        }
        LogicalPlan::Join {
            left,
            right,
            left_keys,
            right_keys,
            ..
        } => {
            collect_tables_read(left, default_db, out);
            collect_tables_read(right, default_db, out);
            for k in left_keys {
                expr_tables_read(k, default_db, out);
            }
            for k in right_keys {
                expr_tables_read(k, default_db, out);
            }
        }
        LogicalPlan::NestedLoopJoin {
            left, right, on, ..
        } => {
            collect_tables_read(left, default_db, out);
            collect_tables_read(right, default_db, out);
            if let Some(on) = on {
                expr_tables_read(on, default_db, out);
            }
        }
        LogicalPlan::Aggregate { input, keys, aggs } => {
            collect_tables_read(input, default_db, out);
            for e in keys
                .iter()
                .chain(aggs.iter().filter_map(|a| a.arg.as_ref()))
            {
                expr_tables_read(e, default_db, out);
            }
        }
        LogicalPlan::Project { input, exprs, .. } => {
            collect_tables_read(input, default_db, out);
            for e in exprs {
                expr_tables_read(e, default_db, out);
            }
        }
        LogicalPlan::Sort { input, keys } => {
            collect_tables_read(input, default_db, out);
            for (e, _) in keys {
                expr_tables_read(e, default_db, out);
            }
        }
        LogicalPlan::Distinct { input } | LogicalPlan::Limit { input, .. } => {
            collect_tables_read(input, default_db, out);
        }
        LogicalPlan::Offset { input, .. } => {
            collect_tables_read(input, default_db, out);
        }
    }
}

fn expr_tables_read(
    expr: &BoundExpr,
    default_db: &str,
    out: &mut std::collections::HashSet<(String, String)>,
) {
    match expr {
        BoundExpr::Literal(_)
        | BoundExpr::Column { .. }
        | BoundExpr::AggRef { .. }
        | BoundExpr::WindowRef { .. } => {}
        BoundExpr::Binary { left, right, .. } => {
            expr_tables_read(left, default_db, out);
            expr_tables_read(right, default_db, out);
        }
        BoundExpr::Not(e)
        | BoundExpr::Negate { expr: e, .. }
        | BoundExpr::IsNull { expr: e, .. }
        | BoundExpr::Cast { expr: e, .. } => expr_tables_read(e, default_db, out),
        BoundExpr::Scalar { args, .. } | BoundExpr::Coalesce { args, .. } => {
            for a in args {
                expr_tables_read(a, default_db, out);
            }
        }
        BoundExpr::DynamicScalar { args, .. } => {
            for a in args.iter() {
                expr_tables_read(a, default_db, out);
            }
        }
        BoundExpr::Case {
            branches, else_, ..
        } => {
            for (c, v) in branches {
                expr_tables_read(c, default_db, out);
                expr_tables_read(v, default_db, out);
            }
            if let Some(e) = else_ {
                expr_tables_read(e, default_db, out);
            }
        }
        BoundExpr::ScalarSubquery { query, .. } | BoundExpr::Exists { query, .. } => {
            collect_tables_read(query, default_db, out);
        }
        BoundExpr::InSubquery { expr, query, .. } => {
            expr_tables_read(expr, default_db, out);
            collect_tables_read(query, default_db, out);
        }
    }
}

/// The catalog-table columns each `Scan` in a plan can be read through,
/// keyed by the scan node's address.
///
/// Two read paths use this to stop materializing unreferenced columns:
/// an attached lake's data files decode only the referenced columns (see
/// `pinkdb::attached`), and an in-memory table's chunks carry a shared
/// zero-length placeholder in place of every unreferenced column (see
/// `exec::project_scan_chunks`). Exactness matters in one direction only:
/// the set must contain *every* column some operator evaluates, because a
/// column left out is left undecoded — the executor reads such a column as
/// an internal error, never as NULLs (see `DataChunk::expect_column`). A
/// scan with no bound in this map is materialized in full, which is always
/// correct.
///
/// The rules mirror what the executor actually evaluates, not what the query
/// eventually displays: `Project` and `Window` evaluate every expression
/// they hold (`fused_join_aggregate` is the one exception, and joins are
/// unbounded here), `Filter`/`Sort` evaluate their predicates/keys, and the
/// operators that read whole rows (`Distinct`, `Union`, `Join`) or
/// materialize an output once and reuse it (`Cte`) are conservatively
/// unbounded. The matches are exhaustive on purpose, like
/// [`collect_tables_read`]: a new plan variant must decide here what its
/// inputs reference.
#[derive(Debug, Default)]
pub(crate) struct ScanProjections {
    /// Scan node address → referenced column indices, ascending. A `Scan`
    /// with no entry reads every column.
    map: HashMap<usize, Vec<usize>>,
}

impl ScanProjections {
    /// Analyze `plan` (the whole statement plan, subqueries included).
    pub(crate) fn of(plan: &LogicalPlan) -> Self {
        let mut projections = Self::default();
        walk_scan_projections(plan, &Needed::All, &mut projections.map);
        projections
    }

    /// The columns of `scan` (a [`LogicalPlan::Scan`]) this plan reads, or
    /// `None` when the analysis could not bound it (read every column).
    pub(crate) fn for_scan(&self, scan: &LogicalPlan) -> Option<&[usize]> {
        debug_assert!(matches!(scan, LogicalPlan::Scan { .. }));
        self.map.get(&node_addr(scan)).map(Vec::as_slice)
    }
}

/// A scan node's identity for [`ScanProjections`]: plans are immutable trees
/// during one statement, so the address of the node is stable across the
/// analysis walk and execution.
fn node_addr(plan: &LogicalPlan) -> usize {
    plan as *const LogicalPlan as usize
}

/// The simple pruning predicates each `Scan` in a plan can be pushed, keyed
/// by the scan node's address (wave 66, lake-scan stage 3): the conjuncts of
/// every `Filter` chain that sits **directly** over a qualified scan — only
/// `Filter`, `Limit` and `Offset` nodes pass predicates down (every other
/// node reorders, rebinds, or re-evaluates columns, so a predicate must not
/// cross it). A scan with no entry has no pushable predicate.
///
/// Extraction is deliberately narrow and always conservative: `column op
/// literal` (either operand order), `IS NULL`/`IS NOT NULL`, joined by `AND`.
/// An `OR` anywhere, a non-literal comparison, a mismatched literal type, or
/// any expression shape this analysis does not model simply yields no
/// predicate for that conjunct — the lake then decodes the file. A wrong
/// predicate could silently drop rows, so nothing is inferred beyond exact
/// binder-bound shapes.
#[derive(Debug, Default)]
pub(crate) struct ScanFilters {
    /// Scan node address → predicates (all conjunctive). A `Scan` with no
    /// entry has none.
    map: HashMap<usize, Vec<crate::attached::PrunePredicate>>,
}

impl ScanFilters {
    /// Analyze `plan` (the whole statement plan, subqueries included).
    pub(crate) fn of(plan: &LogicalPlan) -> Self {
        let mut filters = Self::default();
        walk_scan_filters(plan, &[], &mut filters.map);
        filters
    }

    /// The predicates `scan` (a [`LogicalPlan::Scan`]) may be pruned by.
    pub(crate) fn for_scan(&self, scan: &LogicalPlan) -> &[crate::attached::PrunePredicate] {
        debug_assert!(matches!(scan, LogicalPlan::Scan { .. }));
        self.map
            .get(&node_addr(scan))
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

/// Walk `plan` accumulating the conjuncts of every `Filter` chain that stays
/// directly over a scan. `inherited` holds the predicates gathered from the
/// chain above this node; only `Filter` (adds its conjuncts), `Limit` and
/// `Offset` (pass everything through) keep it alive.
fn walk_scan_filters(
    plan: &LogicalPlan,
    inherited: &[crate::attached::PrunePredicate],
    out: &mut HashMap<usize, Vec<crate::attached::PrunePredicate>>,
) {
    use crate::attached::PrunePredicate;
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let mut here: Vec<PrunePredicate> = inherited.to_vec();
            collect_prune_predicates(predicate, &mut here);
            walk_scan_filters(input, &here, out);
        }
        LogicalPlan::Limit { input, .. } | LogicalPlan::Offset { input, .. } => {
            walk_scan_filters(input, inherited, out);
        }
        LogicalPlan::Scan { db: Some(_), .. } => {
            if !inherited.is_empty() {
                out.entry(node_addr(plan))
                    // The same node reached through two parents may only use
                    // the predicates every path proves (the intersection);
                    // an empty intersection disables pruning for it.
                    .and_modify(|existing| existing.retain(|p| inherited.contains(p)))
                    .or_insert_with(|| inherited.to_vec());
            }
        }
        // Any other node breaks the chain: recurse into its children with no
        // inherited predicates (columns are rebound/reordered there). The
        // match mirrors `collect_tables_read`; a variant listed as a leaf
        // holds no child plan to analyze.
        LogicalPlan::Scan { db: None, .. }
        | LogicalPlan::SingleRow
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::CreateDatabase { .. }
        | LogicalPlan::DropDatabase { .. }
        | LogicalPlan::Use { .. }
        | LogicalPlan::LoadExtension { .. }
        | LogicalPlan::ShowTables { .. }
        | LogicalPlan::ShowDatabases
        | LogicalPlan::ShowMaterializedViews { .. }
        | LogicalPlan::Describe { .. }
        | LogicalPlan::Checkpoint
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::IndexScan { .. }
        | LogicalPlan::CreateMaterializedView { .. }
        | LogicalPlan::RefreshMaterializedView { .. }
        | LogicalPlan::RefreshMaterializedViews { .. }
        | LogicalPlan::DropMaterializedView { .. }
        | LogicalPlan::CreateProcedure { .. }
        | LogicalPlan::DropProcedure { .. } => {}
        LogicalPlan::Call { args, .. } => {
            for a in args {
                walk_expr_subquery_filters(a, out);
            }
        }
        LogicalPlan::Explain { plan } => walk_scan_filters(plan, &[], out),
        LogicalPlan::CreateTableAs { query, .. } | LogicalPlan::InsertSelect { query, .. } => {
            walk_scan_filters(query, &[], out)
        }
        LogicalPlan::Insert { rows, .. } => {
            for row in rows {
                for e in row {
                    walk_expr_subquery_filters(e, out);
                }
            }
        }
        LogicalPlan::Update {
            sets, predicate, ..
        } => {
            for (_, e) in sets {
                walk_expr_subquery_filters(e, out);
            }
            if let Some(p) = predicate {
                walk_expr_subquery_filters(p, out);
            }
        }
        LogicalPlan::Delete { predicate, .. } => {
            if let Some(p) = predicate {
                walk_expr_subquery_filters(p, out);
            }
        }
        LogicalPlan::SetOp { left, right, .. } => {
            walk_scan_filters(left, &[], out);
            walk_scan_filters(right, &[], out);
        }
        LogicalPlan::Cte { query, body, .. } => {
            walk_scan_filters(query, &[], out);
            walk_scan_filters(body, &[], out);
        }
        LogicalPlan::Window { input, windows, .. } => {
            walk_scan_filters(input, &[], out);
            for w in windows {
                for e in w
                    .arg
                    .iter()
                    .chain(w.partition.iter())
                    .chain(w.order.iter().map(|(e, _)| e))
                {
                    walk_expr_subquery_filters(e, out);
                }
            }
        }
        LogicalPlan::Join {
            left,
            right,
            left_keys,
            right_keys,
            ..
        } => {
            walk_scan_filters(left, &[], out);
            walk_scan_filters(right, &[], out);
            for k in left_keys.iter().chain(right_keys) {
                walk_expr_subquery_filters(k, out);
            }
        }
        LogicalPlan::NestedLoopJoin {
            left, right, on, ..
        } => {
            walk_scan_filters(left, &[], out);
            walk_scan_filters(right, &[], out);
            if let Some(on) = on {
                walk_expr_subquery_filters(on, out);
            }
        }
        LogicalPlan::Aggregate { input, keys, aggs } => {
            walk_scan_filters(input, &[], out);
            for e in keys
                .iter()
                .chain(aggs.iter().filter_map(|a| a.arg.as_ref()))
            {
                walk_expr_subquery_filters(e, out);
            }
        }
        LogicalPlan::Project { input, exprs, .. } => {
            walk_scan_filters(input, &[], out);
            for e in exprs {
                walk_expr_subquery_filters(e, out);
            }
        }
        LogicalPlan::Sort { input, keys } => {
            walk_scan_filters(input, &[], out);
            for (e, _) in keys {
                walk_expr_subquery_filters(e, out);
            }
        }
        LogicalPlan::Distinct { input } => walk_scan_filters(input, &[], out),
    }
}

/// Analyze the subquery plans an expression holds (their own filters apply to
/// their own scans). The expression's own shape is irrelevant here.
fn walk_expr_subquery_filters(
    expr: &crate::sql::binder::BoundExpr,
    out: &mut HashMap<usize, Vec<crate::attached::PrunePredicate>>,
) {
    use crate::sql::binder::BoundExpr;
    match expr {
        BoundExpr::Literal(_) | BoundExpr::Column { .. } => {}
        BoundExpr::Binary { left, right, .. } => {
            walk_expr_subquery_filters(left, out);
            walk_expr_subquery_filters(right, out);
        }
        BoundExpr::Not(e)
        | BoundExpr::Negate { expr: e, .. }
        | BoundExpr::IsNull { expr: e, .. }
        | BoundExpr::Cast { expr: e, .. } => walk_expr_subquery_filters(e, out),
        BoundExpr::Scalar { args, .. } | BoundExpr::Coalesce { args, .. } => {
            for a in args {
                walk_expr_subquery_filters(a, out);
            }
        }
        BoundExpr::Case {
            branches, else_, ..
        } => {
            for (c, v) in branches {
                walk_expr_subquery_filters(c, out);
                walk_expr_subquery_filters(v, out);
            }
            if let Some(e) = else_ {
                walk_expr_subquery_filters(e, out);
            }
        }
        BoundExpr::InSubquery { expr, query, .. } => {
            walk_expr_subquery_filters(expr, out);
            walk_scan_filters(query, &[], out);
        }
        BoundExpr::ScalarSubquery { query, .. } | BoundExpr::Exists { query, .. } => {
            walk_scan_filters(query, &[], out)
        }
        BoundExpr::AggRef { .. }
        | BoundExpr::WindowRef { .. }
        | BoundExpr::DynamicScalar { .. } => {}
    }
}

/// Extract the pushable predicates of one `Filter` predicate (its `AND`
/// conjuncts). `out` keeps the predicates of the enclosing chain.
fn collect_prune_predicates(
    expr: &crate::sql::binder::BoundExpr,
    out: &mut Vec<crate::attached::PrunePredicate>,
) {
    use crate::attached::PrunePredicate;
    use crate::sql::ast::BinaryOp;
    use crate::sql::binder::BoundExpr;
    match expr {
        BoundExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => {
            collect_prune_predicates(left, out);
            collect_prune_predicates(right, out);
        }
        BoundExpr::Binary {
            op, left, right, ..
        } => {
            // `column op literal`, or `literal op column` (flipped).
            let direct = match (&**left, &**right) {
                (BoundExpr::Column { index, dtype }, BoundExpr::Literal(v)) => {
                    Some((*index, *dtype, v, false))
                }
                (BoundExpr::Literal(v), BoundExpr::Column { index, dtype }) => {
                    Some((*index, *dtype, v, true))
                }
                _ => None,
            };
            let Some((column, column_dtype, literal, flipped)) = direct else {
                return;
            };
            if literal.is_null() {
                // A comparison with NULL is never true; the executor's
                // filter drops every row anyway. No predicate is pushed
                // (pruning everything is equivalent but noisy).
                return;
            }
            let Some(literal_dtype) = literal.data_type() else {
                return;
            };
            if !prunable_types(column_dtype, literal_dtype) {
                return;
            }
            let op = if flipped { flip(*op) } else { *op };
            let Some(prune_op) = prune_op(op) else {
                return;
            };
            out.push(PrunePredicate::compare(column, prune_op, literal.clone()));
        }
        BoundExpr::IsNull { expr, negated } => {
            if let BoundExpr::Column { index, .. } = &**expr {
                out.push(PrunePredicate::null_test(*index, *negated));
            }
        }
        _ => {}
    }
}

/// The [`PruneOp`] of a comparison operator; `None` for `AND`/`OR`/`LIKE`/
/// arithmetic (never pushed).
fn prune_op(op: crate::sql::ast::BinaryOp) -> Option<crate::attached::PruneOp> {
    use crate::attached::PruneOp;
    use crate::sql::ast::BinaryOp;
    Some(match op {
        BinaryOp::Eq => PruneOp::Eq,
        BinaryOp::NotEq => PruneOp::NotEq,
        BinaryOp::Lt => PruneOp::Lt,
        BinaryOp::LtEq => PruneOp::LtEq,
        BinaryOp::Gt => PruneOp::Gt,
        BinaryOp::GtEq => PruneOp::GtEq,
        _ => return None,
    })
}

/// The operator when the operands are swapped (`literal < column` becomes
/// `column > literal`).
fn flip(op: crate::sql::ast::BinaryOp) -> crate::sql::ast::BinaryOp {
    use crate::sql::ast::BinaryOp;
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}

/// Whether a literal of `literal_dtype` may be compared to statistics of
/// `column_dtype` under `compare_values`'s semantics. Exact types always may;
/// the numeric family (INTEGER/BIGINT/DOUBLE/DECIMAL) compares numerically
/// across widths. Anything else is not pushed — a fabricated ordering (the
/// generic fallback) must never prune a file.
fn prunable_types(column_dtype: DataType, literal_dtype: DataType) -> bool {
    if column_dtype == literal_dtype {
        return true;
    }
    let numeric = |t: DataType| {
        matches!(
            t,
            DataType::Integer | DataType::Bigint | DataType::Double | DataType::Decimal { .. }
        )
    };
    numeric(column_dtype) && numeric(literal_dtype)
}

/// True when `plan` (or any subquery inside one of its expressions) calls a
/// loadable-extension function. The procedure executor refuses such a
/// statement: a callback is opaque (it may be impure, nondeterministic or
/// fail), so a durable store could not replay a logged `CALL` that ran it.
///
/// Deeper than [`BoundExpr::contains_dynamic_scalar`], which stops at a
/// subquery boundary because its callers ask a folding question; here the
/// answer must cover the whole statement.
pub(crate) fn contains_dynamic_scalar(plan: &LogicalPlan) -> bool {
    let mut found = false;
    walk_dynamic_scalars(plan, &mut found);
    found
}

/// [`contains_dynamic_scalar`] for one bound expression (subqueries included).
pub(crate) fn bound_expr_has_dynamic_scalar(expr: &BoundExpr) -> bool {
    let mut found = false;
    expr_dynamic(expr, &mut found);
    found
}

fn walk_dynamic_scalars(plan: &LogicalPlan, found: &mut bool) {
    if *found {
        return;
    }
    let mut exprs: Vec<&BoundExpr> = Vec::new();
    let mut children: Vec<&LogicalPlan> = Vec::new();
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::SingleRow
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::CreateDatabase { .. }
        | LogicalPlan::DropDatabase { .. }
        | LogicalPlan::Use { .. }
        | LogicalPlan::LoadExtension { .. }
        | LogicalPlan::ShowTables { .. }
        | LogicalPlan::ShowDatabases
        | LogicalPlan::ShowMaterializedViews { .. }
        | LogicalPlan::Describe { .. }
        | LogicalPlan::Checkpoint
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreateProcedure { .. }
        | LogicalPlan::DropProcedure { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropMaterializedView { .. }
        | LogicalPlan::RefreshMaterializedViews { .. } => {}
        LogicalPlan::IndexScan { value, .. } => exprs.push(value),
        LogicalPlan::Call { args, .. } => exprs.extend(args.iter()),
        LogicalPlan::CreateTableAs { query, .. }
        | LogicalPlan::InsertSelect { query, .. }
        | LogicalPlan::CreateMaterializedView { query, .. } => children.push(query),
        LogicalPlan::Cte { query, body, .. } => {
            children.push(query);
            children.push(body);
        }
        LogicalPlan::AlterTable { op, .. } => {
            if let AlterTablePlan::AddColumn {
                default: Some(e), ..
            } = op
            {
                exprs.push(e);
            }
        }
        LogicalPlan::Insert { rows, .. } => {
            for row in rows {
                exprs.extend(row.iter());
            }
        }
        LogicalPlan::Update {
            sets, predicate, ..
        } => {
            exprs.extend(sets.iter().map(|(_, e)| e));
            if let Some(p) = predicate {
                exprs.push(p);
            }
        }
        LogicalPlan::Delete { predicate, .. } => {
            if let Some(p) = predicate {
                exprs.push(p);
            }
        }
        LogicalPlan::RefreshMaterializedView { .. } => {}
        LogicalPlan::Explain { plan } => children.push(plan),
        LogicalPlan::SetOp { left, right, .. } => {
            children.push(left);
            children.push(right);
        }
        LogicalPlan::Filter { input, predicate } => {
            children.push(input);
            exprs.push(predicate);
        }
        LogicalPlan::Window { input, windows, .. } => {
            children.push(input);
            for w in windows {
                exprs.extend(w.arg.iter());
                exprs.extend(w.partition.iter());
                exprs.extend(w.order.iter().map(|(e, _)| e));
            }
        }
        LogicalPlan::Join {
            left,
            right,
            left_keys,
            right_keys,
            ..
        } => {
            children.push(left);
            children.push(right);
            exprs.extend(left_keys.iter());
            exprs.extend(right_keys.iter());
        }
        LogicalPlan::NestedLoopJoin {
            left, right, on, ..
        } => {
            children.push(left);
            children.push(right);
            if let Some(on) = on {
                exprs.push(on);
            }
        }
        LogicalPlan::Aggregate { input, keys, aggs } => {
            children.push(input);
            exprs.extend(keys.iter());
            exprs.extend(aggs.iter().filter_map(|a| a.arg.as_ref()));
        }
        LogicalPlan::Project {
            input, exprs: es, ..
        } => {
            children.push(input);
            exprs.extend(es.iter());
        }
        LogicalPlan::Distinct { input } => children.push(input),
        LogicalPlan::Sort { input, keys } => {
            children.push(input);
            exprs.extend(keys.iter().map(|(e, _)| e));
        }
        LogicalPlan::Limit { input, .. } | LogicalPlan::Offset { input, .. } => {
            children.push(input)
        }
    }
    for e in exprs {
        expr_dynamic(e, found);
        if *found {
            return;
        }
    }
    for c in children {
        walk_dynamic_scalars(c, found);
        if *found {
            return;
        }
    }
}

/// One expression's dynamic-scalar check, descending into subquery plans (a
/// subquery's own WHERE/HAVING/key expressions bind in their own scope, so
/// they are checked through their plan).
fn expr_dynamic(expr: &BoundExpr, found: &mut bool) {
    use crate::sql::binder::BoundExpr as E;
    if *found {
        return;
    }
    match expr {
        E::DynamicScalar { .. } => *found = true,
        E::Literal(_) | E::Column { .. } | E::AggRef { .. } | E::WindowRef { .. } => {}
        E::Not(e)
        | E::Negate { expr: e, .. }
        | E::IsNull { expr: e, .. }
        | E::Cast { expr: e, .. } => expr_dynamic(e, found),
        E::Binary { left, right, .. } => {
            expr_dynamic(left, found);
            expr_dynamic(right, found);
        }
        E::Scalar { args, .. } | E::Coalesce { args, .. } => {
            for a in args {
                expr_dynamic(a, found);
            }
        }

        E::InSubquery { expr, query, .. } => {
            expr_dynamic(expr, found);
            walk_dynamic_scalars(query, found);
        }
        E::ScalarSubquery { query, .. } | E::Exists { query, .. } => {
            walk_dynamic_scalars(query, found)
        }
        E::Case {
            branches, else_, ..
        } => {
            for (c, v) in branches {
                expr_dynamic(c, found);
                expr_dynamic(v, found);
            }
            if let Some(e) = else_ {
                expr_dynamic(e, found);
            }
        }
    }
}

/// Which of a node's input columns a consumer reads: an explicit set, or
/// `All` when the shape cannot be bounded (a correct fallback: every column
/// is decoded).
#[derive(Clone, Debug)]
enum Needed {
    All,
    Cols(Vec<bool>),
}

impl Needed {
    fn add_col(&mut self, index: usize) {
        if let Needed::Cols(set) = self {
            if index >= set.len() {
                set.resize(index + 1, false);
            }
            set[index] = true;
        }
    }
}

/// Walk `plan`, telling every `Scan` which of its columns are referenced.
/// `needed` is the set of *this node's output* columns its consumer reads.
fn walk_scan_projections(
    plan: &LogicalPlan,
    needed: &Needed,
    out: &mut HashMap<usize, Vec<usize>>,
) {
    match plan {
        LogicalPlan::Scan { db: Some(_), .. } => {
            if let Needed::Cols(set) = needed {
                out.insert(
                    node_addr(plan),
                    set.iter()
                        .enumerate()
                        .filter(|(_, used)| **used)
                        .map(|(i, _)| i)
                        .collect(),
                );
            }
        }
        // Unqualified scans name CTE outputs (binder-resolved), not tables;
        // the CTE's own query is walked through `LogicalPlan::Cte`.
        LogicalPlan::Scan { db: None, .. } | LogicalPlan::SingleRow => {}
        LogicalPlan::Filter { input, predicate } => {
            let mut need = needed.clone();
            collect_expr_projections(predicate, &mut need, out);
            walk_scan_projections(input, &need, out);
        }
        // The executor evaluates every key and aggregate argument, whatever
        // the consumer of the aggregate needs, so the input bound is the
        // union of all of them.
        LogicalPlan::Aggregate { input, keys, aggs } => {
            let mut need = Needed::Cols(Vec::new());
            for k in keys {
                collect_expr_projections(k, &mut need, out);
            }
            for a in aggs {
                if let Some(e) = &a.arg {
                    collect_expr_projections(e, &mut need, out);
                }
            }
            walk_scan_projections(input, &need, out);
        }
        // The executor evaluates every window's partition/order/argument
        // expressions; the window's input columns themselves pass through,
        // so only the ones the consumer (or a window expression) reads are
        // needed.
        LogicalPlan::Window {
            input,
            windows,
            input_cols,
        } => {
            let mut need = match needed {
                Needed::All => Needed::All,
                Needed::Cols(set) => Needed::Cols(set[..set.len().min(*input_cols)].to_vec()),
            };
            for w in windows {
                for e in w
                    .arg
                    .iter()
                    .chain(w.partition.iter())
                    .chain(w.order.iter().map(|(e, _)| e))
                {
                    collect_expr_projections(e, &mut need, out);
                }
            }
            walk_scan_projections(input, &need, out);
        }
        // Every project expression is evaluated (only the fused
        // join→aggregate path skips unused ones, and joins are unbounded
        // here), so the bound is the union of all of them — plus each
        // expression's subquery plans, which are analyzed in full.
        LogicalPlan::Project { input, exprs, .. } => {
            let mut need = Needed::Cols(Vec::new());
            for e in exprs {
                collect_expr_projections(e, &mut need, out);
            }
            walk_scan_projections(input, &need, out);
        }
        LogicalPlan::Sort { input, keys } => {
            let mut need = needed.clone();
            for (e, _) in keys {
                collect_expr_projections(e, &mut need, out);
            }
            walk_scan_projections(input, &need, out);
        }
        LogicalPlan::Limit { input, .. } | LogicalPlan::Offset { input, .. } => {
            walk_scan_projections(input, needed, out)
        }
        // Unbounded shapes: `Distinct` hashes whole rows; `Union` coerces
        // every column of both sides; `Join` assembles all columns of both
        // sides; a `Cte` query's output is materialized once and reused by
        // the body. Nothing is lost by decoding in full.
        LogicalPlan::Distinct { input } => walk_scan_projections(input, &Needed::All, out),
        LogicalPlan::SetOp { left, right, .. } => {
            walk_scan_projections(left, &Needed::All, out);
            walk_scan_projections(right, &Needed::All, out);
        }
        LogicalPlan::Join {
            left,
            right,
            left_keys,
            right_keys,
            ..
        } => {
            let mut l = Needed::All;
            for k in left_keys {
                collect_expr_projections(k, &mut l, out);
            }
            let mut r = Needed::All;
            for k in right_keys {
                collect_expr_projections(k, &mut r, out);
            }
            walk_scan_projections(left, &l, out);
            walk_scan_projections(right, &r, out);
        }
        LogicalPlan::NestedLoopJoin {
            left, right, on, ..
        } => {
            if let Some(on) = on {
                collect_expr_projections(on, &mut Needed::All, out);
            }
            walk_scan_projections(left, &Needed::All, out);
            walk_scan_projections(right, &Needed::All, out);
        }
        LogicalPlan::Cte { query, body, .. } => {
            walk_scan_projections(query, &Needed::All, out);
            walk_scan_projections(body, needed, out);
        }
        // DML/DDL never runs through the projection-aware read path; their
        // expressions' subquery plans are still analyzed (in full).
        LogicalPlan::Insert { rows, .. } => {
            for row in rows {
                for e in row {
                    let mut need = Needed::All;
                    collect_expr_projections(e, &mut need, out);
                }
            }
        }
        LogicalPlan::InsertSelect { query, .. } => {
            walk_scan_projections(query, &Needed::All, out);
        }
        LogicalPlan::CreateTableAs { query, .. } => {
            walk_scan_projections(query, &Needed::All, out);
        }
        LogicalPlan::Explain { plan } => {
            walk_scan_projections(plan, &Needed::All, out);
        }
        LogicalPlan::Update {
            sets, predicate, ..
        } => {
            for (_, e) in sets {
                let mut need = Needed::All;
                collect_expr_projections(e, &mut need, out);
            }
            if let Some(p) = predicate {
                let mut need = Needed::All;
                collect_expr_projections(p, &mut need, out);
            }
        }
        LogicalPlan::Delete { predicate, .. } => {
            if let Some(p) = predicate {
                let mut need = Needed::All;
                collect_expr_projections(p, &mut need, out);
            }
        }
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::CreateDatabase { .. }
        | LogicalPlan::DropDatabase { .. }
        | LogicalPlan::Use { .. }
        | LogicalPlan::LoadExtension { .. }
        | LogicalPlan::ShowTables { .. }
        | LogicalPlan::ShowDatabases
        | LogicalPlan::ShowMaterializedViews { .. }
        | LogicalPlan::Describe { .. }
        | LogicalPlan::Checkpoint
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::IndexScan { .. }
        | LogicalPlan::CreateMaterializedView { .. }
        | LogicalPlan::RefreshMaterializedView { .. }
        | LogicalPlan::RefreshMaterializedViews { .. }
        | LogicalPlan::DropMaterializedView { .. }
        | LogicalPlan::CreateProcedure { .. }
        | LogicalPlan::DropProcedure { .. } => {}
        // A call's arguments are constant expressions; a subquery inside one
        // is analyzed in full.
        LogicalPlan::Call { args, .. } => {
            for a in args {
                let mut need = Needed::All;
                collect_expr_projections(a, &mut need, out);
            }
        }
    }
}

/// Union every table column `expr` reads into `need`, and analyze the plans
/// of any subqueries it holds (in full: a subquery's output is materialized
/// by the executor). Mirrors `expr_tables_read`'s walk.
fn collect_expr_projections(
    expr: &BoundExpr,
    need: &mut Needed,
    out: &mut HashMap<usize, Vec<usize>>,
) {
    match expr {
        BoundExpr::Literal(_) | BoundExpr::AggRef { .. } | BoundExpr::WindowRef { .. } => {}
        BoundExpr::Column { index, .. } => need.add_col(*index),
        BoundExpr::Binary { left, right, .. } => {
            collect_expr_projections(left, need, out);
            collect_expr_projections(right, need, out);
        }
        BoundExpr::Not(e)
        | BoundExpr::Negate { expr: e, .. }
        | BoundExpr::IsNull { expr: e, .. }
        | BoundExpr::Cast { expr: e, .. } => collect_expr_projections(e, need, out),
        BoundExpr::Scalar { args, .. } | BoundExpr::Coalesce { args, .. } => {
            for a in args {
                collect_expr_projections(a, need, out);
            }
        }
        BoundExpr::DynamicScalar { args, .. } => {
            for a in args.iter() {
                collect_expr_projections(a, need, out);
            }
        }
        BoundExpr::Case {
            branches, else_, ..
        } => {
            for (c, v) in branches {
                collect_expr_projections(c, need, out);
                collect_expr_projections(v, need, out);
            }
            if let Some(e) = else_ {
                collect_expr_projections(e, need, out);
            }
        }
        BoundExpr::ScalarSubquery { query, .. } | BoundExpr::Exists { query, .. } => {
            walk_scan_projections(query, &Needed::All, out);
        }
        BoundExpr::InSubquery { expr, query, .. } => {
            collect_expr_projections(expr, need, out);
            walk_scan_projections(query, &Needed::All, out);
        }
    }
}

/// Compare two non-null values. Cross-type numerics compare in i64 when both
/// are int-like, else f64. Used by comparisons, MIN/MAX, and Sort.
pub fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Integer(x), Value::Bigint(y)) => (*x as i64).cmp(y),
        (Value::Bigint(x), Value::Integer(y)) => x.cmp(&(*y as i64)),
        (Value::Bigint(x), Value::Bigint(y)) => x.cmp(y),
        (Value::Integer(x), Value::Double(y)) => num_cmp(*x as f64, *y),
        (Value::Bigint(x), Value::Double(y)) => num_cmp(*x as f64, *y),
        (Value::Double(x), Value::Integer(y)) => num_cmp(*x, *y as f64),
        (Value::Double(x), Value::Bigint(y)) => num_cmp(*x, *y as f64),
        (Value::Double(x), Value::Double(y)) => num_cmp(*x, *y),
        (Value::Varchar(x), Value::Varchar(y)) => x.cmp(y),
        (Value::Boolean(x), Value::Boolean(y)) => x.cmp(y),
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        // DECIMAL compares numerically across scales; INTERVAL normalizes
        // months to 30-day months (DuckDB rules).
        (Value::Decimal(x, sx), Value::Decimal(y, sy)) => {
            crate::types::decimal_cmp(*x, *sx, *y, *sy)
        }
        (Value::Decimal(x, sx), y) if y.data_type() == Some(DataType::Double) => {
            num_cmp(crate::types::decimal_to_f64(*x, *sx), as_double(y))
        }
        (x, Value::Decimal(y, sy)) if x.data_type() == Some(DataType::Double) => {
            num_cmp(as_double(x), crate::types::decimal_to_f64(*y, *sy))
        }
        (Value::Decimal(x, sx), Value::Integer(y)) => {
            crate::types::decimal_cmp(*x, *sx, *y as i128, 0)
        }
        (Value::Integer(x), Value::Decimal(y, sy)) => {
            crate::types::decimal_cmp(*x as i128, 0, *y, *sy)
        }
        (Value::Decimal(x, sx), Value::Bigint(y)) => {
            crate::types::decimal_cmp(*x, *sx, *y as i128, 0)
        }
        (Value::Bigint(x), Value::Decimal(y, sy)) => {
            crate::types::decimal_cmp(*x as i128, 0, *y, *sy)
        }
        (
            Value::Interval {
                months: am,
                days: ad,
                micros: au,
            },
            Value::Interval {
                months: bm,
                days: bd,
                micros: bu,
            },
        ) => crate::types::interval_cmp(
            crate::types::Interval {
                months: *am,
                days: *ad,
                micros: *au,
            },
            crate::types::Interval {
                months: *bm,
                days: *bd,
                micros: *bu,
            },
        ),
        // Binder rejects mixed-type comparisons; order deterministically anyway.
        _ => format!("{a:?}").cmp(&format!("{b:?}")),
    }
}

/// Output names/types appended by a Window node (one per window).
pub(crate) fn win_output_schema(
    windows: &[crate::sql::binder::BoundWindow],
) -> (Vec<String>, Vec<crate::types::DataType>) {
    let mut names = Vec::with_capacity(windows.len());
    let mut types = Vec::with_capacity(windows.len());
    for (i, w) in windows.iter().enumerate() {
        names.push(format!("__win{i}"));
        types.push(w.dtype);
    }
    (names, types)
}

/// Numeric ordering shared by [`compare_values`] and the aggregators' typed
/// MIN/MAX fast path.
pub(crate) fn num_cmp(x: f64, y: f64) -> std::cmp::Ordering {
    // NaN sorts equal to itself here (grouping/display edge, not ORDER truth).
    x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal)
}

/// A DOUBLE `Value` (the mixed decimal/double comparator's helper; callers
/// guard the type).
fn as_double(v: &Value) -> f64 {
    match v {
        Value::Double(x) => *x,
        _ => unreachable!("as_double on a non-DOUBLE value"),
    }
}

/// Subtract `by` from every column index in `expr` (used to rebase
/// joined-space subexpressions onto a single join side).
pub(crate) fn shift_columns(expr: BoundExpr, by: usize) -> BoundExpr {
    match expr {
        BoundExpr::Column { index, dtype } => {
            debug_assert!(
                index >= by,
                "shift_columns would underflow: column {index} < left_cols {by}"
            );
            BoundExpr::Column {
                index: index - by,
                dtype,
            }
        }
        BoundExpr::InSubquery {
            expr,
            query,
            negated,
        } => BoundExpr::InSubquery {
            expr: Box::new(shift_columns(*expr, by)),
            query,
            negated,
        },
        BoundExpr::Cast { expr, dtype } => BoundExpr::Cast {
            expr: Box::new(shift_columns(*expr, by)),
            dtype,
        },
        BoundExpr::Case {
            branches,
            else_,
            dtype,
        } => BoundExpr::Case {
            branches: branches
                .into_iter()
                .map(|(c, v)| (shift_columns(c, by), shift_columns(v, by)))
                .collect(),
            else_: else_.map(|e| Box::new(shift_columns(*e, by))),
            dtype,
        },
        BoundExpr::Coalesce { args, dtype } => BoundExpr::Coalesce {
            args: args.into_iter().map(|a| shift_columns(a, by)).collect(),
            dtype,
        },
        BoundExpr::Binary {
            op,
            left,
            right,
            dtype,
        } => BoundExpr::Binary {
            op,
            left: Box::new(shift_columns(*left, by)),
            right: Box::new(shift_columns(*right, by)),
            dtype,
        },
        BoundExpr::Not(e) => BoundExpr::Not(Box::new(shift_columns(*e, by))),
        BoundExpr::Negate { expr, dtype } => BoundExpr::Negate {
            expr: Box::new(shift_columns(*expr, by)),
            dtype,
        },
        BoundExpr::IsNull { expr, negated } => BoundExpr::IsNull {
            expr: Box::new(shift_columns(*expr, by)),
            negated,
        },
        BoundExpr::Scalar { func, args, dtype } => BoundExpr::Scalar {
            func,
            args: args.into_iter().map(|a| shift_columns(a, by)).collect(),
            dtype,
        },
        BoundExpr::DynamicScalar { name, args, dtype } => BoundExpr::DynamicScalar {
            name,
            args: args
                .into_vec()
                .into_iter()
                .map(|a| shift_columns(a, by))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            dtype,
        },
        leaf => leaf,
    }
}

/// Output names/types of an Aggregate node (keys then aggs). Shared by the
/// executor and the binder's schema inference.
pub(crate) fn agg_output_schema(
    keys: &[crate::sql::binder::BoundExpr],
    aggs: &[crate::sql::binder::BoundAgg],
) -> (Vec<String>, Vec<crate::types::DataType>) {
    let mut names = Vec::with_capacity(keys.len() + aggs.len());
    let mut types = Vec::with_capacity(keys.len() + aggs.len());
    for (i, k) in keys.iter().enumerate() {
        names.push(format!("__key{i}"));
        types.push(k.dtype());
    }
    for (i, a) in aggs.iter().enumerate() {
        names.push(format!("__agg{i}"));
        types.push(a.dtype);
    }
    (names, types)
}

#[cfg(test)]
mod scan_filter_tests {
    use super::*;
    use crate::attached::{PruneOp, PrunePredicate};
    use crate::sql::ast::BinaryOp;

    fn scan(table: &str) -> LogicalPlan {
        LogicalPlan::Scan {
            db: Some("main".to_string()),
            table: table.to_string(),
            at: None,
        }
    }

    fn filter(input: LogicalPlan, predicate: BoundExpr) -> LogicalPlan {
        LogicalPlan::Filter {
            input: Box::new(input),
            predicate,
        }
    }

    fn col(index: usize) -> BoundExpr {
        BoundExpr::Column {
            index,
            dtype: DataType::Integer,
        }
    }

    fn lit(i: i32) -> BoundExpr {
        BoundExpr::Literal(Value::Integer(i))
    }

    fn cmp(op: BinaryOp, left: BoundExpr, right: BoundExpr) -> BoundExpr {
        BoundExpr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
            dtype: DataType::Boolean,
        }
    }

    fn and(left: BoundExpr, right: BoundExpr) -> BoundExpr {
        BoundExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(left),
            right: Box::new(right),
            dtype: DataType::Boolean,
        }
    }

    /// A filter chain stays pushable through `Limit`/`Offset`; every other
    /// node (here `Project` and `Aggregate`) breaks it: the analysis must
    /// never hand a scan predicates that address pre-rebinding columns.
    #[test]
    fn scan_filters_stop_at_rebinding_nodes() {
        // Filter → Limit → Filter → Scan: both filters' conjuncts reach the
        // scan, in chain order.
        let plan = filter(
            LogicalPlan::Limit {
                input: Box::new(filter(scan("t"), cmp(BinaryOp::Lt, col(1), lit(5)))),
                limit: 10,
            },
            cmp(BinaryOp::Gt, col(0), lit(1)),
        );
        let LogicalPlan::Filter { input, .. } = &plan else {
            unreachable!()
        };
        let LogicalPlan::Limit { input, .. } = &**input else {
            unreachable!()
        };
        let LogicalPlan::Filter { input, .. } = &**input else {
            unreachable!()
        };
        let scan_node = &**input;
        let filters = ScanFilters::of(&plan);
        assert_eq!(
            filters.for_scan(scan_node),
            &[
                PrunePredicate::compare(0, PruneOp::Gt, Value::Integer(1)),
                PrunePredicate::compare(1, PruneOp::Lt, Value::Integer(5)),
            ]
        );

        // Filter → Project → Scan: nothing crosses the Project.
        let plan = filter(
            LogicalPlan::Project {
                input: Box::new(scan("t")),
                exprs: vec![col(0)],
                names: vec!["i".to_string()],
            },
            cmp(BinaryOp::Eq, col(0), lit(1)),
        );
        let LogicalPlan::Filter { input, .. } = &plan else {
            unreachable!()
        };
        let LogicalPlan::Project { input, .. } = &**input else {
            unreachable!()
        };
        assert!(ScanFilters::of(&plan).for_scan(input).is_empty());

        // Filter → Aggregate → Scan: nothing crosses the Aggregate.
        let plan = filter(
            LogicalPlan::Aggregate {
                input: Box::new(scan("t")),
                keys: vec![col(0)],
                aggs: Vec::new(),
            },
            cmp(BinaryOp::Eq, col(0), lit(1)),
        );
        let LogicalPlan::Filter { input, .. } = &plan else {
            unreachable!()
        };
        let LogicalPlan::Aggregate { input, .. } = &**input else {
            unreachable!()
        };
        assert!(ScanFilters::of(&plan).for_scan(input).is_empty());

        // An OR is not a conjunction: nothing is pushed.
        let plan = filter(
            scan("t"),
            cmp(
                BinaryOp::Or,
                cmp(BinaryOp::Lt, col(0), lit(1)),
                cmp(BinaryOp::Gt, col(0), lit(2)),
            ),
        );
        let LogicalPlan::Filter { input, .. } = &plan else {
            unreachable!()
        };
        assert!(ScanFilters::of(&plan).for_scan(input).is_empty());

        // An AND of exact conjuncts pushes both, left to right.
        let plan = filter(
            scan("t"),
            and(
                cmp(BinaryOp::Eq, col(0), lit(3)),
                BoundExpr::IsNull {
                    expr: Box::new(col(1)),
                    negated: true,
                },
            ),
        );
        let LogicalPlan::Filter { input, .. } = &plan else {
            unreachable!()
        };
        assert_eq!(
            ScanFilters::of(&plan).for_scan(input),
            &[
                PrunePredicate::compare(0, PruneOp::Eq, Value::Integer(3)),
                PrunePredicate::null_test(1, true),
            ]
        );

        // A comparison whose literal is a non-matching type (VARCHAR literal
        // against an INTEGER column) is never pushed: `compare_values` must
        // not order what the binder would have rejected.
        let plan = filter(
            scan("t"),
            cmp(
                BinaryOp::Eq,
                col(0),
                BoundExpr::Literal(Value::Varchar("x".to_string())),
            ),
        );
        let LogicalPlan::Filter { input, .. } = &plan else {
            unreachable!()
        };
        assert!(ScanFilters::of(&plan).for_scan(input).is_empty());
    }
}
