//! v1 optimizer: constant folding + predicate pushdown.
//!
//! - Constant folding evaluates column-free expression trees at bind time
//!   (`1 + 2` → `3`, `true AND x` stays — only fully constant trees fold).
//! - Predicate pushdown moves `Filter` below `Project` (transparent columns),
//!   `Sort`, and `Distinct`, and splits conjuncts across `Join` inputs.
//!   Conjuncts over just the GROUP BY keys also move below `Aggregate`
//!   (a HAVING on keys becomes a WHERE on the aggregate's input); filters
//!   never move below `Limit`.

use crate::exec::expr::eval_expr;
use crate::plan::LogicalPlan;
use crate::sql::ast::{BinaryOp, JoinKind};
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
    DataChunk {
        columns: Vec::new(),
        num_rows: 1,
    }
}

fn fold_expr(cx: &crate::exec::Cx, expr: BoundExpr) -> BoundExpr {
    let rebuilt = match expr {
        BoundExpr::Binary {
            op,
            left,
            right,
            dtype,
        } => BoundExpr::Binary {
            op,
            left: Box::new(fold_expr(cx, *left)),
            right: Box::new(fold_expr(cx, *right)),
            dtype,
        },
        BoundExpr::Not(e) => BoundExpr::Not(Box::new(fold_expr(cx, *e))),
        BoundExpr::Negate { expr, dtype } => BoundExpr::Negate {
            expr: Box::new(fold_expr(cx, *expr)),
            dtype,
        },
        BoundExpr::IsNull { expr, negated } => BoundExpr::IsNull {
            expr: Box::new(fold_expr(cx, *expr)),
            negated,
        },
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
        BoundExpr::Case {
            branches,
            else_,
            dtype,
        } => BoundExpr::Case {
            branches: branches
                .into_iter()
                .map(|(c, v)| (fold_expr(cx, c), fold_expr(cx, v)))
                .collect(),
            else_: else_.map(|e| Box::new(fold_expr(cx, *e))),
            dtype,
        },
        // Extension calls are opaque: fold their arguments (those are pure
        // expressions) but never the call itself. A callback may be impure,
        // non-deterministic, or fail at runtime; folding it would run it at
        // bind time — and once per execution instead of once per row.
        BoundExpr::DynamicScalar { name, args, dtype } => {
            return BoundExpr::DynamicScalar {
                name,
                args: args
                    .into_vec()
                    .into_iter()
                    .map(|a| fold_expr(cx, a))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                dtype,
            };
        }
        leaf => return leaf,
    };
    // Only fully constant trees (no columns, no aggregate refs) fold.
    // Fold failures (e.g. `1/0`) keep the tree; execution reports them.
    // Subqueries need runtime evaluation — never fold them, and never fold a
    // tree containing a dynamic call: the root-level guard above only covers
    // a bare call, but a call enclosed by an operator/COALESCE/CASE would
    // otherwise be evaluated here at bind time (and frozen into a literal).
    if !rebuilt.has_column_ref()
        && rebuilt.is_aggregate_free()
        && !rebuilt.has_subquery()
        && !rebuilt.contains_dynamic_scalar()
    {
        if let Ok(v) = eval_expr(&rebuilt, &const_chunk(), cx) {
            if v.len() == 1 {
                return BoundExpr::Literal(v.get(0));
            }
        }
    }
    rebuilt
}

fn fold_aggs(
    cx: &crate::exec::Cx,
    aggs: Vec<crate::sql::binder::BoundAgg>,
) -> Vec<crate::sql::binder::BoundAgg> {
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
        LogicalPlan::Join {
            kind,
            left,
            right,
            left_key,
            right_key,
            left_cols,
        } => LogicalPlan::Join {
            kind,
            left: Box::new(fold_plan(db, *left)),
            right: Box::new(fold_plan(db, *right)),
            left_key: fold_expr(&cx, left_key),
            right_key: fold_expr(&cx, right_key),
            left_cols,
        },
        LogicalPlan::Aggregate { input, keys, aggs } => LogicalPlan::Aggregate {
            input: Box::new(fold_plan(db, *input)),
            keys: keys.into_iter().map(|k| fold_expr(&cx, k)).collect(),
            aggs: fold_aggs(&cx, aggs),
        },
        LogicalPlan::Window {
            input,
            windows,
            input_cols,
        } => LogicalPlan::Window {
            input: Box::new(fold_plan(db, *input)),
            windows: windows
                .into_iter()
                .map(|mut w| {
                    w.arg = w.arg.map(|e| fold_expr(&cx, e));
                    w.partition = w.partition.into_iter().map(|e| fold_expr(&cx, e)).collect();
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
        LogicalPlan::Project {
            input,
            exprs,
            names,
        } => LogicalPlan::Project {
            input: Box::new(fold_plan(db, *input)),
            exprs: exprs.into_iter().map(|e| fold_expr(&cx, e)).collect(),
            names,
        },
        LogicalPlan::Cte { name, query, body } => LogicalPlan::Cte {
            name,
            query: Box::new(fold_plan(db, *query)),
            body: Box::new(fold_plan(db, *body)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(fold_plan(db, *input)),
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(fold_plan(db, *input)),
            keys: keys
                .into_iter()
                .map(|(k, d)| (fold_expr(&cx, k), d))
                .collect(),
        },
        LogicalPlan::Limit { input, limit } => LogicalPlan::Limit {
            input: Box::new(fold_plan(db, *input)),
            limit,
        },
        LogicalPlan::Insert { db, table, rows } => LogicalPlan::Insert {
            db,
            table,
            rows: rows
                .into_iter()
                .map(|r| r.into_iter().map(|e| fold_expr(&cx, e)).collect())
                .collect(),
        },
        LogicalPlan::Union {
            left,
            right,
            distinct,
            types,
        } => LogicalPlan::Union {
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
        BoundExpr::Not(e)
        | BoundExpr::Negate { expr: e, .. }
        | BoundExpr::IsNull { expr: e, .. } => {
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
        BoundExpr::DynamicScalar { args, .. } => {
            for a in args.iter() {
                columns_in(a, out);
            }
        }
        BoundExpr::Cast { expr, .. } => columns_in(expr, out),
        BoundExpr::Case {
            branches, else_, ..
        } => {
            for (c, v) in branches {
                columns_in(c, out);
                columns_in(v, out);
            }
            if let Some(e) = else_ {
                columns_in(e, out);
            }
        }
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
        BoundExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => {
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
        acc = BoundExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(acc),
            right: Box::new(e),
            dtype,
        };
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
        BoundExpr::Case {
            branches,
            else_,
            dtype,
        } => Some(BoundExpr::Case {
            branches: branches
                .iter()
                .map(|(c, v)| {
                    Some((
                        rewrite_through_project(c, exprs)?,
                        rewrite_through_project(v, exprs)?,
                    ))
                })
                .collect::<Option<Vec<_>>>()?,
            else_: match else_ {
                Some(e) => Some(Box::new(rewrite_through_project(e, exprs)?)),
                None => None,
            },
            dtype: *dtype,
        }),
        BoundExpr::Coalesce { args, dtype } => Some(BoundExpr::Coalesce {
            args: args
                .iter()
                .map(|a| rewrite_through_project(a, exprs))
                .collect::<Option<Vec<_>>>()?,
            dtype: *dtype,
        }),
        // Subqueries are scope-independent: pass through (IN's outer expr
        // still rewrites below).
        BoundExpr::ScalarSubquery { .. } | BoundExpr::Exists { .. } => Some(pred.clone()),
        BoundExpr::InSubquery {
            expr,
            query,
            negated,
        } => Some(BoundExpr::InSubquery {
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
        BoundExpr::Binary {
            op,
            left,
            right,
            dtype,
        } => Some(BoundExpr::Binary {
            op: *op,
            left: Box::new(rewrite_through_project(left, exprs)?),
            right: Box::new(rewrite_through_project(right, exprs)?),
            dtype: *dtype,
        }),
        BoundExpr::Scalar { func, args, dtype } => Some(BoundExpr::Scalar {
            func: *func,
            args: args
                .iter()
                .map(|a| rewrite_through_project(a, exprs))
                .collect::<Option<Vec<_>>>()?,
            dtype: *dtype,
        }),
        // A dynamic call is row-wise like any other function: pushing a
        // predicate through a projection is row-preserving (same rows, same
        // order), so it is allowed as long as every referenced column is a
        // passthrough (enforced per argument here). Pushing through a join,
        // union, or distinct is refused — those change the call count.
        BoundExpr::DynamicScalar { name, args, dtype } => Some(BoundExpr::DynamicScalar {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_through_project(a, exprs))
                .collect::<Option<Vec<_>>>()?
                .into_boxed_slice(),
            dtype: *dtype,
        }),
    }
}

/// Remap a predicate from aggregate-output space to input space: every
/// `Column{i}` addresses the aggregate's i-th group key, so it becomes that
/// key's own expression. `None` when an index escapes the key list (an
/// aggregate-output reference). Subqueries are scope-independent and pass
/// through; `AggRef`/`WindowRef` pass through too (the caller keeps any
/// conjunct holding one above the aggregate).
fn rewrite_through_keys(pred: &BoundExpr, keys: &[BoundExpr]) -> Option<BoundExpr> {
    match pred {
        BoundExpr::Column { index, .. } => keys.get(*index).cloned(),
        BoundExpr::Literal(_) | BoundExpr::AggRef { .. } | BoundExpr::WindowRef { .. } => {
            Some(pred.clone())
        }
        BoundExpr::ScalarSubquery { .. } | BoundExpr::Exists { .. } => Some(pred.clone()),
        BoundExpr::InSubquery {
            expr,
            query,
            negated,
        } => Some(BoundExpr::InSubquery {
            expr: Box::new(rewrite_through_keys(expr, keys)?),
            query: query.clone(),
            negated: *negated,
        }),
        BoundExpr::Binary {
            op,
            left,
            right,
            dtype,
        } => Some(BoundExpr::Binary {
            op: *op,
            left: Box::new(rewrite_through_keys(left, keys)?),
            right: Box::new(rewrite_through_keys(right, keys)?),
            dtype: *dtype,
        }),
        BoundExpr::Not(e) => Some(BoundExpr::Not(Box::new(rewrite_through_keys(e, keys)?))),
        BoundExpr::Negate { expr, dtype } => Some(BoundExpr::Negate {
            expr: Box::new(rewrite_through_keys(expr, keys)?),
            dtype: *dtype,
        }),
        BoundExpr::IsNull { expr, negated } => Some(BoundExpr::IsNull {
            expr: Box::new(rewrite_through_keys(expr, keys)?),
            negated: *negated,
        }),
        BoundExpr::Scalar { func, args, dtype } => Some(BoundExpr::Scalar {
            func: *func,
            args: args
                .iter()
                .map(|a| rewrite_through_keys(a, keys))
                .collect::<Option<Vec<_>>>()?,
            dtype: *dtype,
        }),
        BoundExpr::Cast { expr, dtype } => Some(BoundExpr::Cast {
            expr: Box::new(rewrite_through_keys(expr, keys)?),
            dtype: *dtype,
        }),
        BoundExpr::Coalesce { args, dtype } => Some(BoundExpr::Coalesce {
            args: args
                .iter()
                .map(|a| rewrite_through_keys(a, keys))
                .collect::<Option<Vec<_>>>()?,
            dtype: *dtype,
        }),
        BoundExpr::Case {
            branches,
            else_,
            dtype,
        } => Some(BoundExpr::Case {
            branches: branches
                .iter()
                .map(|(c, v)| {
                    Some((
                        rewrite_through_keys(c, keys)?,
                        rewrite_through_keys(v, keys)?,
                    ))
                })
                .collect::<Option<Vec<_>>>()?,
            else_: match else_ {
                Some(e) => Some(Box::new(rewrite_through_keys(e, keys)?)),
                None => None,
            },
            dtype: *dtype,
        }),
        BoundExpr::DynamicScalar { name, args, dtype } => Some(BoundExpr::DynamicScalar {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_through_keys(a, keys))
                .collect::<Option<Vec<_>>>()?
                .into_boxed_slice(),
            dtype: *dtype,
        }),
    }
}

/// Push a filter one level down. Returns `(plan, moved)`. When no push
/// applies, the filter stays above the input — never dropped.
fn push_one_level(input: LogicalPlan, pred: BoundExpr) -> (LogicalPlan, bool) {
    match input {
        LogicalPlan::Project {
            input: inner,
            exprs,
            names,
        } => match rewrite_through_project(&pred, &exprs) {
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
                (
                    LogicalPlan::Filter {
                        input: Box::new(input),
                        predicate: pred,
                    },
                    false,
                )
            }
        },
        LogicalPlan::Sort { input: inner, keys } => (
            LogicalPlan::Sort {
                input: Box::new(LogicalPlan::Filter {
                    input: inner,
                    predicate: pred,
                }),
                keys,
            },
            true,
        ),
        LogicalPlan::Distinct { input: inner } => {
            // Pushing the filter below a DISTINCT evaluates the predicate on
            // every input row instead of once per distinct row: an impure
            // dynamic call would change its call count, so it stays above.
            if pred.contains_dynamic_scalar() {
                return (
                    LogicalPlan::Filter {
                        input: Box::new(LogicalPlan::Distinct { input: inner }),
                        predicate: pred,
                    },
                    false,
                );
            }
            (
                LogicalPlan::Distinct {
                    input: Box::new(LogicalPlan::Filter {
                        input: inner,
                        predicate: pred,
                    }),
                },
                true,
            )
        }
        LogicalPlan::Join {
            kind,
            left,
            right,
            left_key,
            right_key,
            left_cols,
        } => {
            let mut conjuncts = Vec::new();
            split_conjuncts(pred, &mut conjuncts);
            let mut keep = Vec::new();
            let mut left_push = Vec::new();
            let mut right_push = Vec::new();
            for c in conjuncts {
                // A conjunct containing a dynamic call is opaque/impure:
                // pushing it into a join input changes how many times (and on
                // which rows) the callback runs, so it stays above the join.
                if c.contains_dynamic_scalar() {
                    keep.push(c);
                    continue;
                }
                let mut cols = Vec::new();
                columns_in(&c, &mut cols);
                let left_only = cols.iter().all(|i| *i < left_cols);
                let right_only = cols.iter().all(|i| *i >= left_cols);
                match kind {
                    // Inner: split freely (unchanged since wave 3).
                    JoinKind::Inner => {
                        if left_only {
                            left_push.push(c);
                        } else if right_only {
                            right_push.push(shift_columns(c, left_cols));
                        } else {
                            keep.push(c);
                        }
                    }
                    // Outer joins: only conjuncts over the preserved side's
                    // own columns may move into that input. Right-side and
                    // mixed conjuncts stay above the join (they must observe
                    // NULL-extended rows). Constant conjuncts stay too.
                    JoinKind::Left => {
                        if !cols.is_empty() && left_only {
                            left_push.push(c);
                        } else {
                            keep.push(c);
                        }
                    }
                    JoinKind::Right => {
                        if !cols.is_empty() && right_only {
                            right_push.push(shift_columns(c, left_cols));
                        } else {
                            keep.push(c);
                        }
                    }
                }
            }
            let pushed = !left_push.is_empty() || !right_push.is_empty();
            let left = match and_all(left_push) {
                Some(p) => Box::new(LogicalPlan::Filter {
                    input: left,
                    predicate: p,
                }),
                None => left,
            };
            let right = match and_all(right_push) {
                Some(p) => Box::new(LogicalPlan::Filter {
                    input: right,
                    predicate: p,
                }),
                None => right,
            };
            let join = LogicalPlan::Join {
                kind,
                left,
                right,
                left_key,
                right_key,
                left_cols,
            };
            match and_all(keep) {
                // Nothing moved below: report unmoved so pushdown terminates.
                Some(p) => (
                    LogicalPlan::Filter {
                        input: Box::new(join),
                        predicate: p,
                    },
                    pushed,
                ),
                // Everything pushed: filter node disappears (still progress).
                None => (join, true),
            }
        }
        // A conjunct over only GROUP BY keys moves below the aggregate: a
        // group's key holds one value for every one of its rows, so filtering
        // members by it drops exactly the groups the conjunct above would
        // drop — for any key expression, NULL keys included (NULLs group
        // together). The pushed predicate reads each row's key expression,
        // the same values the aggregate groups by, so it is the
        // SQL-standard equivalence of writing the predicate in WHERE.
        // Conjuncts touching aggregate outputs (`AggRef`) or constants stay
        // above: a keyless aggregate still emits its one row over an empty
        // input, so filtering below would change COUNT(*) / SUM() over an
        // empty relation.
        LogicalPlan::Aggregate { input, keys, aggs } => {
            let mut conjuncts = Vec::new();
            split_conjuncts(pred, &mut conjuncts);
            let mut keep = Vec::new();
            let mut push = Vec::new();
            for c in conjuncts {
                let mut cols = Vec::new();
                columns_in(&c, &mut cols);
                if cols.is_empty() || cols.iter().any(|i| *i >= keys.len()) {
                    keep.push(c);
                    continue;
                }
                // Rewrite the key positions into their own expressions
                // (input space). A pushed conjunct evaluates once per input
                // row instead of once per group, so a predicate holding a
                // dynamic call — reached through a key expression included —
                // stays above.
                match rewrite_through_keys(&c, &keys) {
                    Some(rewritten) if !rewritten.contains_dynamic_scalar() => push.push(rewritten),
                    _ => keep.push(c),
                }
            }
            let pushed = !push.is_empty();
            let input = match and_all(push) {
                Some(p) => Box::new(LogicalPlan::Filter {
                    input,
                    predicate: p,
                }),
                None => input,
            };
            let aggregate = LogicalPlan::Aggregate { input, keys, aggs };
            match and_all(keep) {
                // Nothing moved below: report unmoved so pushdown terminates.
                Some(p) => (
                    LogicalPlan::Filter {
                        input: Box::new(aggregate),
                        predicate: p,
                    },
                    pushed,
                ),
                None => (aggregate, true),
            }
        }
        // Never below Limit (changes semantics).
        // Below Union: selection distributes over both branches (same
        // output width by construction; per-branch types were checked
        // compatible at bind time) — but cloning the predicate into both
        // branches would run a dynamic call once per branch instead of once
        // per output row, so a predicate containing one stays above.
        LogicalPlan::Union {
            left,
            right,
            distinct,
            types,
        } => {
            if pred.contains_dynamic_scalar() {
                return (
                    LogicalPlan::Filter {
                        input: Box::new(LogicalPlan::Union {
                            left,
                            right,
                            distinct,
                            types,
                        }),
                        predicate: pred,
                    },
                    false,
                );
            }
            (
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
            )
        }
        LogicalPlan::Window {
            input,
            windows,
            input_cols,
        } => {
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
                Some(p) => Box::new(LogicalPlan::Filter {
                    input,
                    predicate: p,
                }),
                None => input,
            };
            let window = LogicalPlan::Window {
                input,
                windows,
                input_cols,
            };
            match and_all(keep) {
                // Nothing moved below: report unmoved so pushdown terminates.
                Some(p) => (
                    LogicalPlan::Filter {
                        input: Box::new(window),
                        predicate: p,
                    },
                    pushed,
                ),
                None => (window, true),
            }
        }
        // Anything else keeps the filter where it is.
        other => (
            LogicalPlan::Filter {
                input: Box::new(other),
                predicate: pred,
            },
            false,
        ),
    }
}

/// Subtract `by` from every column index (right-side conjuncts of a join).
fn shift_columns(expr: BoundExpr, by: usize) -> BoundExpr {
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

fn pushdown(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let input = pushdown(*input);
            // Literals: `WHERE false` still needs the row stream shaped
            // correctly — keep the Filter node (it yields zero rows).
            let (plan, moved) = push_one_level(input, predicate);
            if moved {
                pushdown(plan)
            } else {
                plan
            }
        }
        LogicalPlan::Project {
            input,
            exprs,
            names,
        } => LogicalPlan::Project {
            input: Box::new(pushdown(*input)),
            exprs,
            names,
        },
        LogicalPlan::Join {
            kind,
            left,
            right,
            left_key,
            right_key,
            left_cols,
        } => LogicalPlan::Join {
            kind,
            left: Box::new(pushdown(*left)),
            right: Box::new(pushdown(*right)),
            left_key,
            right_key,
            left_cols,
        },
        LogicalPlan::Aggregate { input, keys, aggs } => LogicalPlan::Aggregate {
            input: Box::new(pushdown(*input)),
            keys,
            aggs,
        },
        LogicalPlan::Window {
            input,
            windows,
            input_cols,
        } => LogicalPlan::Window {
            input: Box::new(pushdown(*input)),
            windows,
            input_cols,
        },
        LogicalPlan::Union {
            left,
            right,
            distinct,
            types,
        } => LogicalPlan::Union {
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
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(pushdown(*input)),
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(pushdown(*input)),
            keys,
        },
        LogicalPlan::Limit { input, limit } => LogicalPlan::Limit {
            input: Box::new(pushdown(*input)),
            limit,
        },
        leaf => leaf,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::binder::BoundExpr as B;
    use crate::types::{DataType as T, Value};

    fn col(i: usize) -> BoundExpr {
        B::Column {
            index: i,
            dtype: T::Integer,
        }
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
                kind: JoinKind::Inner,
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
                kind: JoinKind::Inner,
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

    fn bool_gt(c: BoundExpr, n: i32) -> BoundExpr {
        B::Binary {
            op: BinaryOp::Gt,
            left: Box::new(c),
            right: Box::new(B::Literal(Value::Integer(n))),
            dtype: T::Boolean,
        }
    }

    fn and(a: BoundExpr, b: BoundExpr) -> BoundExpr {
        B::Binary {
            op: BinaryOp::And,
            left: Box::new(a),
            right: Box::new(b),
            dtype: T::Boolean,
        }
    }

    fn outer_join(kind: JoinKind) -> LogicalPlan {
        LogicalPlan::Join {
            kind,
            left: Box::new(LogicalPlan::SingleRow),
            right: Box::new(LogicalPlan::SingleRow),
            left_key: col(0),
            right_key: col(0),
            left_cols: 2,
        }
    }

    /// LEFT JOIN: a left-side conjunct pushes into the preserved left input;
    /// a right-side conjunct stays above (it must see NULL-extended rows).
    #[test]
    fn left_join_pushdown_is_kind_aware() {
        let left_only = bool_gt(col(0), 5);
        let right_only = bool_gt(col(3), 1);
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_join(JoinKind::Left)),
            predicate: and(left_only, right_only),
        };
        let out = pushdown(plan);
        // The right-side conjunct stays above the join; the left-side one moved.
        match out {
            LogicalPlan::Filter { input, predicate } => {
                assert_eq!(predicate, bool_gt(col(3), 1));
                let LogicalPlan::Join {
                    left, right, kind, ..
                } = *input
                else {
                    panic!()
                };
                assert_eq!(kind, JoinKind::Left);
                assert_eq!(
                    *left,
                    LogicalPlan::Filter {
                        input: Box::new(LogicalPlan::SingleRow),
                        predicate: bool_gt(col(0), 5),
                    }
                );
                assert!(matches!(*right, LogicalPlan::SingleRow));
            }
            _ => panic!("right-side predicate must stay above the join"),
        }
    }

    /// RIGHT JOIN mirrors: only right-side-only conjuncts push into the
    /// right input; left-side ones stay above.
    #[test]
    fn right_join_pushdown_is_kind_aware() {
        let left_only = bool_gt(col(0), 5);
        let right_only = bool_gt(col(3), 1);
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_join(JoinKind::Right)),
            predicate: and(left_only, right_only),
        };
        let out = pushdown(plan);
        match out {
            LogicalPlan::Filter { input, predicate } => {
                assert_eq!(predicate, bool_gt(col(0), 5));
                let LogicalPlan::Join {
                    left, right, kind, ..
                } = *input
                else {
                    panic!()
                };
                assert_eq!(kind, JoinKind::Right);
                assert!(matches!(*left, LogicalPlan::SingleRow));
                // Shifted into right-input space: index 3 → 1.
                assert_eq!(
                    *right,
                    LogicalPlan::Filter {
                        input: Box::new(LogicalPlan::SingleRow),
                        predicate: bool_gt(col(1), 1),
                    }
                );
            }
            _ => panic!("left-side predicate must stay above the join"),
        }
    }

    /// A mixed-side conjunct never moves below an outer join; a constant
    /// conjunct stays above too (only column-restricted ones move).
    #[test]
    fn outer_join_keeps_mixed_and_constant_conjuncts() {
        let mixed = B::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col(0)),
            right: Box::new(col(3)),
            dtype: T::Boolean,
        };
        let constant = B::Literal(Value::Boolean(false));
        let plan = LogicalPlan::Filter {
            input: Box::new(outer_join(JoinKind::Left)),
            predicate: and(mixed.clone(), constant.clone()),
        };
        let out = pushdown(plan);
        match out {
            LogicalPlan::Filter { input, predicate } => {
                assert_eq!(predicate, and(mixed, constant));
                let LogicalPlan::Join { left, right, .. } = *input else {
                    panic!()
                };
                assert!(matches!(*left, LogicalPlan::SingleRow));
                assert!(matches!(*right, LogicalPlan::SingleRow));
            }
            _ => panic!("nothing should push below a LEFT join here"),
        }
    }

    fn count_star() -> crate::sql::binder::BoundAgg {
        crate::sql::binder::BoundAgg {
            func: crate::sql::binder::AggFunc::Count,
            arg: None,
            dtype: T::Bigint,
        }
    }

    fn aggref(i: usize) -> BoundExpr {
        B::AggRef {
            index: i,
            dtype: T::Bigint,
        }
    }

    fn aggregate(keys: Vec<BoundExpr>) -> LogicalPlan {
        LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::SingleRow),
            keys,
            aggs: vec![count_star()],
        }
    }

    /// A conjunct over only group keys pushes below the aggregate, rewritten
    /// into the key expression itself (input space).
    #[test]
    fn having_on_group_keys_pushes_below_aggregate() {
        let key = B::Binary {
            op: BinaryOp::Add,
            left: Box::new(col(0)),
            right: Box::new(B::Literal(Value::Integer(1))),
            dtype: T::Integer,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(aggregate(vec![key.clone()])),
            predicate: bool_gt(col(0), 5),
        };
        let out = pushdown(plan);
        let LogicalPlan::Aggregate { input, keys, aggs } = out else {
            panic!("the aggregate must remain the root: {out:?}");
        };
        assert_eq!(keys, vec![key.clone()]);
        assert_eq!(aggs, vec![count_star()]);
        assert_eq!(
            *input,
            LogicalPlan::Filter {
                input: Box::new(LogicalPlan::SingleRow),
                predicate: bool_gt(key, 5),
            }
        );
    }

    /// HAVING on an aggregate output (AggRef) or on a constant stays above;
    /// a mixed predicate splits — the key conjunct moves, the rest stays.
    #[test]
    fn having_on_aggregates_and_constants_stays_above() {
        // Key conjunct + aggregate conjunct + constant conjunct.
        let constant = B::Literal(Value::Boolean(false));
        let plan = LogicalPlan::Filter {
            input: Box::new(aggregate(vec![col(0)])),
            predicate: and(
                and(bool_gt(col(0), 5), bool_gt(aggref(0), 1)),
                constant.clone(),
            ),
        };
        let out = pushdown(plan);
        let LogicalPlan::Filter { input, predicate } = out else {
            panic!("filter must stay above the aggregate: {out:?}");
        };
        assert_eq!(predicate, and(bool_gt(aggref(0), 1), constant));
        let LogicalPlan::Aggregate { input, .. } = *input else {
            panic!()
        };
        assert_eq!(
            *input,
            LogicalPlan::Filter {
                input: Box::new(LogicalPlan::SingleRow),
                predicate: bool_gt(col(0), 5),
            }
        );

        // A keyless aggregate over an empty input still emits its row, so a
        // constant filter must never move below it.
        let plan = LogicalPlan::Filter {
            input: Box::new(aggregate(vec![])),
            predicate: B::Literal(Value::Boolean(false)),
        };
        assert!(matches!(pushdown(plan), LogicalPlan::Filter { .. }));
    }

    /// A key-only conjunct holding a dynamic (impure, possibly stateful)
    /// call stays above: pushing it would change how often the call runs.
    #[test]
    fn having_with_dynamic_call_stays_above() {
        let call = B::DynamicScalar {
            name: "f".into(),
            args: vec![col(0)].into_boxed_slice(),
            dtype: T::Boolean,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(aggregate(vec![col(0)])),
            predicate: call.clone(),
        };
        match pushdown(plan) {
            LogicalPlan::Filter { input, predicate } => {
                assert_eq!(predicate, call);
                assert!(matches!(*input, LogicalPlan::Aggregate { .. }));
            }
            other => panic!("dynamic call must stay above: {other:?}"),
        }
    }

    /// Optimization is deterministic: the same bound plan optimizes to the
    /// same plan (no hash-order or address-order leaks into the shape).
    #[test]
    fn optimization_is_deterministic() {
        let db = crate::storage::PinkDb::new();
        let key = B::Binary {
            op: BinaryOp::Add,
            left: Box::new(col(0)),
            right: Box::new(B::Literal(Value::Integer(1))),
            dtype: T::Integer,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Aggregate {
                input: Box::new(LogicalPlan::Filter {
                    input: Box::new(LogicalPlan::SingleRow),
                    predicate: B::Literal(Value::Boolean(true)),
                }),
                keys: vec![key],
                aggs: vec![count_star()],
            }),
            predicate: bool_gt(col(0), 5),
        };
        let a = optimize(&db, plan.clone());
        let b = optimize(&db, plan);
        assert_eq!(a, b);
    }
}
