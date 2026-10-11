//! Logical plan: a tree of Scan → Filter → Join → Aggregate → Project →
//! Distinct → Sort → Limit.

use crate::sql::binder::{BoundAgg, BoundExpr, BoundWindow};
use crate::types::{DataType, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    CreateTable {
        name: String,
        schema: Vec<(String, DataType)>,
    },
    Insert {
        table: String,
        /// One bound expr per table column per row (constants; missing
        /// columns are NULL). Evaluated against an empty single-row chunk.
        rows: Vec<Vec<BoundExpr>>,
    },
    Update {
        table: String,
        /// `(column index, value expression)` — expressions address table
        /// columns and evaluate simultaneously per row (pre-update values).
        sets: Vec<(usize, BoundExpr)>,
        predicate: Option<BoundExpr>,
    },
    Delete {
        table: String,
        predicate: Option<BoundExpr>,
    },
    Begin,
    Commit,
    Rollback,
    /// Full scan of a table (column pruning is wave-3 optimizer work).
    Scan {
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
    /// Inner equi-join on a single `left_key = right_key` condition.
    /// Output columns are left ++ right. Keys are side-relative (evaluated
    /// against each input); every other expression above the join uses
    /// joined-space indices. NULL keys never match.
    Join {
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
