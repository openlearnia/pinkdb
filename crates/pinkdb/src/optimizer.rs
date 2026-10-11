//! v1 optimizer: constant folding + predicate pushdown.
//!
//! - Constant folding evaluates column-free expression trees at bind time
//!   (`1 + 2` → `3`, `true AND x` stays — only fully constant trees fold).
//! - Predicate pushdown moves `Filter` below `Project` (transparent columns),
//!   `Sort`, and `Distinct`, and splits conjuncts across `Join` inputs.
//!   Filters never move below `Limit` or `Aggregate` in v1.

use crate::exec::expr::eval_expr;
use crate::plan::LogicalPlan;
use crate::sql::ast::BinaryOp;
use crate::sql::binder::BoundExpr;
use crate::types::DataType;
use crate::vector::DataChunk;

/// Optimize a bound plan: constant folding + predicate pushdown.
/// Takes the database for the folding evaluation context (folds never touch
/// stored data — only column-free expressions are evaluated).
pub fn optimize(db: &crate::storage::PinkDb, plan: LogicalPlan) -> LogicalPlan {
    let plan = pushdown(fold_plan(db, plan));
    fold_plan(db, plan)
}

// ---------- constant folding ----------

fn const_chunk() -> DataChunk {
    DataChunk { columns: Vec::new(), num_rows: 1 }
}

fn fold_expr(cx: &crate::exec::Cx, expr: BoundExpr) -> BoundExpr {
    let rebuilt = match expr {
        BoundExpr::Binary { op, left, right, dtype } => BoundExpr::Binary {
            op,
            left: Box::new(fold_expr(cx, *left)),
            right: Box::new(fold_expr(cx, *right)),
            dtype,
        },
        BoundExpr::Not(e) => BoundExpr::Not(Box::new(fold_expr(cx, *e))),
        BoundExpr::Negate { expr, dtype } => {
            BoundExpr::Negate { expr: Box::new(fold_expr(cx, *expr)), dtype }
        }
        BoundExpr::IsNull { expr, negated } => {
            BoundExpr::IsNull { expr: Box::new(fold_expr(cx, *expr)), negated }
        }
        BoundExpr::Scalar { func, args, dtype } => BoundExpr::Scalar {
            func,
            args: args.into_iter().map(|a| fold_expr(cx, a)).collect(),
            dtype,
        },
        BoundExpr::Cast { expr, dtype } => BoundExpr::Cast {
            expr: Box::new(fold_expr(cx, *expr)),
            dtype,
        },
        BoundExpr::Coalesce { args, dtype } => BoundExpr::Coalesce {
            args: args.into_iter().map(|a| fold_expr(cx, a)).collect(),
            dtype,
        },
        leaf => return leaf,
    };
    // Only fully constant trees (no columns, no aggregate refs) fold.
    // Fold failures (e.g. `1/0`) keep the tree; execution reports them.
    // Subqueries need runtime evaluation — never fold them.
    if !rebuilt.has_column_ref() && rebuilt.is_aggregate_free() && !rebuilt.has_subquery() {
        if let Ok(v) = eval_expr(&rebuilt, &const_chunk(), cx) {
            if v.len() == 1 {
                return BoundExpr::Literal(v.get(0));
            }
        }
    }
    rebuilt
}

fn fold_aggs(cx: &crate::exec::Cx, aggs: Vec<crate::sql::binder::BoundAgg>) -> Vec<crate::sql::binder::BoundAgg> {
    aggs.into_iter()
        .map(|mut a| {
            a.arg = a.arg.map(|e| fold_expr(cx, e));
            a
        })
        .collect()
}

fn fold_plan(db: &crate::storage::PinkDb, plan: LogicalPlan) -> LogicalPlan {
    let cx = crate::exec::Cx::new(db);
    match plan {
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(fold_plan(db, *input)),
            predicate: fold_expr(&cx, predicate),
        },
        LogicalPlan::Join { left, right, left_key, right_key, left_cols } => {
            LogicalPlan::Join {
                left: Box::new(fold_plan(db, *left)),
                right: Box::new(fold_plan(db, *right)),
                left_key: fold_expr(&cx, left_key),
                right_key: fold_expr(&cx, right_key),
                left_cols,
            }
        }
        LogicalPlan::Aggregate { input, keys, aggs } => LogicalPlan::Aggregate {
            input: Box::new(fold_plan(db, *input)),
            keys: keys.into_iter().map(|k| fold_expr(&cx, k)).collect(),
            aggs: fold_aggs(&cx, aggs),
        },
        LogicalPlan::Window { input, windows, input_cols } => LogicalPlan::Window {
            input: Box::new(fold_plan(db, *input)),
            windows: windows
                .into_iter()
                .map(|mut w| {
                    w.arg = w.arg.map(|e| fold_expr(&cx, e));
                    w.partition =
                        w.partition.into_iter().map(|e| fold_expr(&cx, e)).collect();
                    w.order = w
                        .order
                        .into_iter()
                        .map(|(e, d)| (fold_expr(&cx, e), d))
                        .collect();
                    w
                })
                .collect(),
            input_cols,
        },
        LogicalPlan::Project { input, exprs, names } => LogicalPlan::Project {
            input: Box::new(fold_plan(db, *input)),
            exprs: exprs.into_iter().map(|e| fold_expr(&cx, e)).collect(),
            names,
        },
        LogicalPlan::Cte { name, query, body } => LogicalPlan::Cte {
            name,
            query: Box::new(fold_plan(db, *query)),
            body: Box::new(fold_plan(db, *body)),
        },
        LogicalPlan::Distinct { input } => {
            LogicalPlan::Distinct { input: Box::new(fold_plan(db, *input)) }
        }
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(fold_plan(db, *input)),
            keys: keys.into_iter().map(|(k, d)| (fold_expr(&cx, k), d)).collect(),
        },
        LogicalPlan::Limit { input, limit } => {
            LogicalPlan::Limit { input: Box::new(fold_plan(db, *input)), limit }
        }
        LogicalPlan::Insert { table, rows } => LogicalPlan::Insert {
            table,
            rows: rows
                .into_iter()
                .map(|r| r.into_iter().map(|e| fold_expr(&cx, e)).collect())
                .collect(),
        },
        LogicalPlan::Union { left, right, distinct, types } => LogicalPlan::Union {
            left: Box::new(fold_plan(db, *left)),
            right: Box::new(fold_plan(db, *right)),
            distinct,
            types,
        },
        leaf => leaf,
    }
}

// ---------- predicate pushdown ----------

/// Column indices referenced by `expr` (AggRefs count as unpushable).
/// Subquery bodies live in their own scope; only an IN-subquery's outer
/// expression can reference pushable columns.
fn columns_in(expr: &BoundExpr, out: &mut Vec<usize>) {
    match expr {
        BoundExpr::Column { index, .. } => out.push(*index),
        BoundExpr::Literal(_) | BoundExpr::ScalarSubquery { .. } | BoundExpr::Exists { .. } => {}
        BoundExpr::InSubquery { expr, .. } => columns_in(expr, out),
        BoundExpr::AggRef { .. } | BoundExpr::WindowRef { .. } => out.push(usize::MAX),
        BoundExpr::Not(e) | BoundExpr::Negate { expr: e, .. } | BoundExpr::IsNull { expr: e, .. } => {
            columns_in(e, out);
        }
        BoundExpr::Binary { left, right, .. } => {
            columns_in(left, out);
            columns_in(right, out);
        }
        BoundExpr::Scalar { args, .. } => {
            for a in args {
                columns_in(a, out);
            }
        }
        BoundExpr::Cast { expr, .. } => columns_in(expr, out),
        BoundExpr::Coalesce { args, .. } => {
            for a in args {
                columns_in(a, out);
            }
        }
    }
}

/// Split `a AND b AND ...` into conjuncts.
fn split_conjuncts(expr: BoundExpr, out: &mut Vec<BoundExpr>) {
    match expr {
        BoundExpr::Binary { op: BinaryOp::And, left, right, .. } => {
            split_conjuncts(*left, out);
            split_conjuncts(*right, out);
        }
        e => out.push(e),
    }
}

fn and_all(mut exprs: Vec<BoundExpr>) -> Option<BoundExpr> {
    if exprs.is_empty() {
        return None;
    }
    let mut acc = exprs.remove(0);
    for e in exprs {
        let dtype = DataType::Boolean;
        acc = BoundExpr::Binary { op: BinaryOp::And, left: Box::new(acc), right: Box::new(e), dtype };
    }
    Some(acc)
}

/// Remap a predicate through a transparent projection: every `Column{i}`
/// must address a bare `Column`/`AggRef` passthrough (same scope below).
fn rewrite_through_project(pred: &BoundExpr, exprs: &[BoundExpr]) -> Option<BoundExpr> {
    match pred {
        BoundExpr::Column { index, .. } => match &exprs[*index] {
            BoundExpr::Column { .. } | BoundExpr::AggRef { .. } => Some(exprs[*index].clone()),
            _ => None,
        },
        BoundExpr::Literal(_) | BoundExpr::AggRef { .. } | BoundExpr::WindowRef { .. } => {
            Some(pred.clone())
        }
        BoundExpr::Cast { expr, dtype } => Some(BoundExpr::Cast {
            expr: Box::new(rewrite_through_project(expr, exprs)?),
            dtype: *dtype,
        }),
        BoundExpr::Coalesce { args, dtype } => Some(BoundExpr::Coalesce {
            args: args.iter().map(|a| rewrite_through_project(a, exprs)).collect::<Option<Vec<_>>>()?,
            dtype: *dtype,
        }),
        // Subqueries are scope-independent: pass through (IN's outer expr
        // still rewrites below).
        BoundExpr::ScalarSubquery { .. } | BoundExpr::Exists { .. } => Some(pred.clone()),
        BoundExpr::InSubquery { expr, query, negated } => Some(BoundExpr::InSubquery {
            expr: Box::new(rewrite_through_project(expr, exprs)?),
            query: query.clone(),
            negated: *negated,
        }),
        BoundExpr::Not(e) => Some(BoundExpr::Not(Box::new(rewrite_through_project(e, exprs)?))),
        BoundExpr::Negate { expr, dtype } => Some(BoundExpr::Negate {
            expr: Box::new(rewrite_through_project(expr, exprs)?),
            dtype: *dtype,
        }),
        BoundExpr::IsNull { expr, negated } => Some(BoundExpr::IsNull {
            expr: Box::new(rewrite_through_project(expr, exprs)?),
            negated: *negated,
        }),
        BoundExpr::Binary { op, left, right, dtype } => Some(BoundExpr::Binary {
            op: *op,
            left: Box::new(rewrite_through_project(left, exprs)?),
            right: Box::new(rewrite_through_project(right, exprs)?),
            dtype: *dtype,
        }),
        BoundExpr::Scalar { func, args, dtype } => Some(BoundExpr::Scalar {
            func: *func,
            args: args.iter().map(|a| rewrite_through_project(a, exprs)).collect::<Option<Vec<_>>>()?,
            dtype: *dtype,
        }),
    }
}

/// Push a filter one level down. Returns `(plan, moved)`. When no push
/// applies, the filter stays above the input — never dropped.
fn push_one_level(input: LogicalPlan, pred: BoundExpr) -> (LogicalPlan, bool) {
    match input {
        LogicalPlan::Project { input: inner, exprs, names } => {
            match rewrite_through_project(&pred, &exprs) {
                Some(rewritten) => (
                    LogicalPlan::Project {
                        input: Box::new(LogicalPlan::Filter {
                            input: inner,
                            predicate: rewritten,
                        }),
                        exprs,
                        names,
                    },
                    true,
                ),
                None => {
                    let input = LogicalPlan::Project {
                        input: Box::new(pushdown(*inner)),
                        exprs,
                        names,
                    };
                    (LogicalPlan::Filter { input: Box::new(input), predicate: pred }, false)
                }
            }
        }
        LogicalPlan::Sort { input: inner, keys } => (
            LogicalPlan::Sort {
                input: Box::new(LogicalPlan::Filter { input: inner, predicate: pred }),
                keys,
            },
            true,
        ),
        LogicalPlan::Distinct { input: inner } => (
            LogicalPlan::Distinct {
                input: Box::new(LogicalPlan::Filter { input: inner, predicate: pred }),
            },
            true,
        ),
        LogicalPlan::Join { left, right, left_key, right_key, left_cols } => {
            let mut conjuncts = Vec::new();
            split_conjuncts(pred, &mut conjuncts);
            let mut keep = Vec::new();
            let mut left_push = Vec::new();
            let mut right_push = Vec::new();
            for c in conjuncts {
                let mut cols = Vec::new();
                columns_in(&c, &mut cols);
                if cols.iter().all(|i| *i < left_cols) {
                    left_push.push(c);
                } else if cols.iter().all(|i| *i >= left_cols) {
                    right_push.push(shift_columns(c, left_cols));
                } else {
                    keep.push(c);
                }
            }
            let pushed = !left_push.is_empty() || !right_push.is_empty();
            let left = match and_all(left_push) {
                Some(p) => Box::new(LogicalPlan::Filter { input: left, predicate: p }),
                None => left,
            };
            let right = match and_all(right_push) {
                Some(p) => Box::new(LogicalPlan::Filter { input: right, predicate: p }),
                None => right,
            };
            let join = LogicalPlan::Join { left, right, left_key, right_key, left_cols };
            match and_all(keep) {
                // Nothing moved below: report unmoved so pushdown terminates.
                Some(p) => (
                    LogicalPlan::Filter { input: Box::new(join), predicate: p },
                    pushed,
                ),
                // Everything pushed: filter node disappears (still progress).
                None => (join, true),
            }
        }
        // Never below Limit (changes semantics) or Aggregate (HAVING, v1).
        // Below Union: selection distributes over both branches (same
        // output width by construction; per-branch types were checked
        // compatible at bind time).
        LogicalPlan::Union { left, right, distinct, types } => (
            LogicalPlan::Union {
                left: Box::new(LogicalPlan::Filter {
                    input: left,
                    predicate: pred.clone(),
                }),
                right: Box::new(LogicalPlan::Filter {
                    input: right,
                    predicate: pred,
                }),
                distinct,
                types,
            },
            true,
        ),
        LogicalPlan::Window { input, windows, input_cols } => {
            let mut conjuncts = Vec::new();
            split_conjuncts(pred, &mut conjuncts);
            let mut keep = Vec::new();
            let mut push = Vec::new();
            for c in conjuncts {
                let mut cols = Vec::new();
                columns_in(&c, &mut cols);
                if !cols.is_empty() && cols.iter().all(|i| *i < input_cols) {
                    push.push(c);
                } else {
                    keep.push(c);
                }
            }
            let pushed = !push.is_empty();
            let input = match and_all(push) {
                Some(p) => Box::new(LogicalPlan::Filter { input, predicate: p }),
                None => input,
            };
            let window = LogicalPlan::Window { input, windows, input_cols };
            match and_all(keep) {
                // Nothing moved below: report unmoved so pushdown terminates.
                Some(p) => (
                    LogicalPlan::Filter { input: Box::new(window), predicate: p },
                    pushed,
                ),
                None => (window, true),
            }
        }
        // Anything else keeps the filter where it is.
        other => (
            LogicalPlan::Filter { input: Box::new(other), predicate: pred },
            false,
        ),
    }
}

/// Subtract `by` from every column index (right-side conjuncts of a join).
fn shift_columns(expr: BoundExpr, by: usize) -> BoundExpr {
    match expr {
        BoundExpr::Column { index, dtype } => BoundExpr::Column { index: index - by, dtype },
        BoundExpr::InSubquery { expr, query, negated } => BoundExpr::InSubquery {
            expr: Box::new(shift_columns(*expr, by)),
            query,
            negated,
        },
        BoundExpr::Cast { expr, dtype } => BoundExpr::Cast {
            expr: Box::new(shift_columns(*expr, by)),
            dtype,
        },
        BoundExpr::Coalesce { args, dtype } => BoundExpr::Coalesce {
            args: args.into_iter().map(|a| shift_columns(a, by)).collect(),
            dtype,
        },
        BoundExpr::Binary { op, left, right, dtype } => BoundExpr::Binary {
            op,
            left: Box::new(shift_columns(*left, by)),
            right: Box::new(shift_columns(*right, by)),
            dtype,
        },
        BoundExpr::Not(e) => BoundExpr::Not(Box::new(shift_columns(*e, by))),
        BoundExpr::Negate { expr, dtype } => {
            BoundExpr::Negate { expr: Box::new(shift_columns(*expr, by)), dtype }
        }
        BoundExpr::IsNull { expr, negated } => BoundExpr::IsNull {
            expr: Box::new(shift_columns(*expr, by)),
            negated,
        },
        BoundExpr::Scalar { func, args, dtype } => BoundExpr::Scalar {
            func,
            args: args.into_iter().map(|a| shift_columns(a, by)).collect(),
            dtype,
        },
        leaf => leaf,
    }
}

fn pushdown(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let input = pushdown(*input);
            // Literals: `WHERE false` still needs the row stream shaped
            // correctly — keep the Filter node (it yields zero rows).
            let (plan, moved) = push_one_level(input, predicate);
            if moved { pushdown(plan) } else { plan }
        }
        LogicalPlan::Project { input, exprs, names } => LogicalPlan::Project {
            input: Box::new(pushdown(*input)),
            exprs,
            names,
        },
        LogicalPlan::Join { left, right, left_key, right_key, left_cols } => {
            LogicalPlan::Join {
                left: Box::new(pushdown(*left)),
                right: Box::new(pushdown(*right)),
                left_key,
                right_key,
                left_cols,
            }
        }
        LogicalPlan::Aggregate { input, keys, aggs } => LogicalPlan::Aggregate {
            input: Box::new(pushdown(*input)),
            keys,
            aggs,
        },
        LogicalPlan::Window { input, windows, input_cols } => LogicalPlan::Window {
            input: Box::new(pushdown(*input)),
            windows,
            input_cols,
        },
        LogicalPlan::Union { left, right, distinct, types } => LogicalPlan::Union {
            left: Box::new(pushdown(*left)),
            right: Box::new(pushdown(*right)),
            distinct,
            types,
        },
        // Filters push within the body and within the query independently —
        // never across the CTE boundary (different scopes).
        LogicalPlan::Cte { name, query, body } => LogicalPlan::Cte {
            name,
            query: Box::new(pushdown(*query)),
            body: Box::new(pushdown(*body)),
        },
        LogicalPlan::Distinct { input } => {
            LogicalPlan::Distinct { input: Box::new(pushdown(*input)) }
        }
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(pushdown(*input)),
            keys,
        },
        LogicalPlan::Limit { input, limit } => {
            LogicalPlan::Limit { input: Box::new(pushdown(*input)), limit }
        }
        leaf => leaf,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::binder::BoundExpr as B;
    use crate::types::{DataType as T, Value};

    fn col(i: usize) -> BoundExpr {
        B::Column { index: i, dtype: T::Integer }
    }

    #[test]
    fn folds_constants() {
        let db = crate::storage::PinkDb::new();
        let cx = crate::exec::Cx::new(&db);
        let e = B::Binary {
            op: BinaryOp::Add,
            left: Box::new(B::Literal(Value::Integer(1))),
            right: Box::new(B::Literal(Value::Integer(2))),
            dtype: T::Integer,
        };
        assert_eq!(fold_expr(&cx, e), B::Literal(Value::Integer(3)));
        // Columns block folding.
        let e = B::Binary {
            op: BinaryOp::Add,
            left: Box::new(col(0)),
            right: Box::new(B::Literal(Value::Integer(2))),
            dtype: T::Integer,
        };
        assert!(matches!(fold_expr(&cx, e), B::Binary { .. }));
    }

    #[test]
    fn pushdown_splits_join_conjuncts() {
        // Filter(Join) with left-only, right-only, and mixed predicates.
        let left_only = B::Binary {
            op: BinaryOp::Gt,
            left: Box::new(col(0)),
            right: Box::new(B::Literal(Value::Integer(5))),
            dtype: T::Boolean,
        };
        let right_only = B::Binary {
            op: BinaryOp::Gt,
            left: Box::new(col(3)),
            right: Box::new(B::Literal(Value::Integer(1))),
            dtype: T::Boolean,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Join {
                left: Box::new(LogicalPlan::SingleRow),
                right: Box::new(LogicalPlan::SingleRow),
                left_key: col(0),
                right_key: col(0),
                left_cols: 2,
            }),
            predicate: B::Binary {
                op: BinaryOp::And,
                left: Box::new(left_only),
                right: Box::new(right_only),
                dtype: T::Boolean,
            },
        };
        let out = pushdown(plan);
        // Both conjuncts pushed; no Filter remains above the join.
        assert!(matches!(out, LogicalPlan::Join { .. }));
        if let LogicalPlan::Join { left, right, .. } = out {
            assert!(matches!(*left, LogicalPlan::Filter { .. }));
            assert!(matches!(*right, LogicalPlan::Filter { .. }));
        }
    }

    #[test]
    fn limit_blocks_pushdown() {
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Limit {
                input: Box::new(LogicalPlan::SingleRow),
                limit: 5,
            }),
            predicate: B::Literal(Value::Boolean(true)),
        };
        assert!(matches!(pushdown(plan), LogicalPlan::Filter { .. }));
    }

    #[test]
    fn partial_pushdown_terminates() {
        // A mixed join predicate (left-only AND cross-side) must terminate:
        // the cross-side conjunct stays above while the rest pushes down.
        let left_only = B::Binary {
            op: BinaryOp::Gt,
            left: Box::new(col(0)),
            right: Box::new(B::Literal(Value::Integer(5))),
            dtype: T::Boolean,
        };
        let mixed = B::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col(0)),
            right: Box::new(col(3)),
            dtype: T::Boolean,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Join {
                left: Box::new(LogicalPlan::SingleRow),
                right: Box::new(LogicalPlan::SingleRow),
                left_key: col(0),
                right_key: col(0),
                left_cols: 2,
            }),
            predicate: B::Binary {
                op: BinaryOp::And,
                left: Box::new(left_only),
                right: Box::new(mixed),
                dtype: T::Boolean,
            },
        };
        let out = pushdown(plan);
        // Left side got its filter; the mixed conjunct stays on top.
        match out {
            LogicalPlan::Filter { input, .. } => {
                assert!(matches!(*input, LogicalPlan::Join { .. }));
                if let LogicalPlan::Join { left, right, .. } = *input {
                    assert!(matches!(*left, LogicalPlan::Filter { .. }));
                    assert!(matches!(*right, LogicalPlan::SingleRow));
                }
            }
            _ => panic!("filter should stay above the join"),
        }
    }
}
