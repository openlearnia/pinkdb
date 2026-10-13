//! Logical plan: Scan → Filter → Join → Aggregate → Window → Project →
//! Distinct → Sort → Limit (plus DDL/DML and set unions).

use crate::sql::ast::{JoinKind, Select};
use crate::sql::binder::{BoundAgg, BoundExpr, BoundWindow};
use crate::types::{DataType, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    CreateTable {
        /// Target database (binder-resolved; `None` = session default).
        db: Option<String>,
        name: String,
        schema: Vec<(String, DataType)>,
    },
    Insert {
        db: Option<String>,
        table: String,
        /// One bound expr per table column per row (constants; missing
        /// columns are NULL). Evaluated against an empty single-row chunk.
        rows: Vec<Vec<BoundExpr>>,
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
    },
    DropView {
        db: Option<String>,
        name: String,
    },
    /// `CREATE DATABASE <name>` (store-level; never inside a transaction).
    CreateDatabase {
        name: String,
    },
    /// `DROP DATABASE <name>` (store-level; never inside a transaction).
    DropDatabase {
        name: String,
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
    /// Set union: output names come from the left branch; every column is
    /// coerced to the pairwise common type at execution.
    Union {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        distinct: bool,
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
    /// Equi-join on a single `left_key = right_key` condition.
    /// Output columns are left ++ right. Keys are side-relative (evaluated
    /// against each input); every other expression above the join uses
    /// joined-space indices. NULL keys never match. `Inner` emits matched
    /// pairs only; `Left`/`Right` preserve every row of their side
    /// (NULL-extending the other side's columns when unmatched).
    Join {
        kind: JoinKind,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        left_key: BoundExpr,
        right_key: BoundExpr,
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
        /// (key expr, descending?) evaluated against the input.
        keys: Vec<(BoundExpr, bool)>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
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
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Project { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Cte { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Use { .. }
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
            | LogicalPlan::Insert { .. }
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
        _ => return None,
    })
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

fn num_cmp(x: f64, y: f64) -> std::cmp::Ordering {
    // NaN sorts equal to itself here (grouping/display edge, not ORDER truth).
    x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal)
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
