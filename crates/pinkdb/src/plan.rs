//! Logical plan: Scan → Filter → Join → Aggregate → Window → Project →
//! Distinct → Sort → Limit (plus DDL/DML and set unions).

use std::collections::HashMap;

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
    DropTable {
        db: Option<String>,
        name: String,
        /// `IF EXISTS`: a missing table makes this a no-op.
        if_exists: bool,
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
        schema: Vec<(String, DataType)>,
    },
    /// `EXPLAIN <select>`: a printable textual plan (never executed).
    Explain {
        plan: Box<LogicalPlan>,
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
    /// `Some(db)` is a catalog table in that database.
    Scan {
        db: Option<String>,
        table: String,
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
            | LogicalPlan::Describe { .. }
            | LogicalPlan::Explain { .. }
    )
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
            | LogicalPlan::CreateDatabase { .. }
            | LogicalPlan::DropDatabase { .. }
            | LogicalPlan::Use { .. }
            | LogicalPlan::LoadExtension { .. }
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
        } => {
            out.insert((db.to_ascii_lowercase(), table.to_ascii_lowercase()));
        }
        LogicalPlan::Scan { db: None, .. }
        | LogicalPlan::SingleRow
        | LogicalPlan::CreateTable { .. }
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
        | LogicalPlan::Describe { .. }
        | LogicalPlan::Checkpoint => {}
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
        | LogicalPlan::Describe { .. }
        | LogicalPlan::Checkpoint => {}
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
        BoundExpr::Column { index, dtype } => BoundExpr::Column {
            index: index - by,
            dtype,
        },
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
