//! v1 optimizer: constant folding + predicate pushdown + FK→PK join
//! elimination.
//!
//! - Constant folding evaluates column-free expression trees at bind time
//!   (`1 + 2` → `3`, `true AND x` stays — only fully constant trees fold).
//! - Predicate pushdown moves `Filter` below `Project` (transparent columns),
//!   `Sort`, and `Distinct`, and splits conjuncts across `Join` inputs.
//!   Conjuncts over just the GROUP BY keys also move below `Aggregate`
//!   (a HAVING on keys becomes a WHERE on the aggregate's input); conjuncts
//!   over bare partition-key columns move below `Window`, while any other
//!   input-column conjunct stays above so the window still sees every input
//!   row; filters never move below `Limit`.
//! - FK→PK join elimination drops a declared-referential `INNER JOIN` whose
//!   parent-side columns are unused: constraints are declarative metadata, so
//!   the optimizer *trusts* them — see [`eliminate_fk_joins`] for the exact
//!   rule and its caveat.

use crate::exec::expr::eval_expr;
use crate::plan::{shift_columns, LogicalPlan};
use crate::sql::ast::{BinaryOp, JoinKind};
use crate::sql::binder::BoundExpr;
use crate::types::DataType;
use crate::vector::DataChunk;

/// Optimize a bound plan: constant folding + predicate pushdown + FK→PK join
/// elimination.
/// Takes the database for the folding evaluation context (folds never touch
/// stored data — only column-free expressions are evaluated) and for the
/// declarative constraints the elimination rule reads.
pub fn optimize(db: &crate::storage::PinkDb, plan: LogicalPlan) -> LogicalPlan {
    let plan = pushdown(fold_plan(db, plan));
    let plan = eliminate_fk_joins(db, plan);
    let plan = drop_count_star_projections(plan);
    let plan = apply_indexes(db, plan);
    fold_plan(db, plan)
}

/// Rewrite `Filter { input: Scan, predicate: col = literal }` into an
/// `IndexScan` when a hash index exists on that column. Only simple equality
/// against a literal is supported; everything else keeps the scan.
fn apply_indexes(db: &crate::storage::PinkDb, plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let input = apply_indexes(db, *input);
            // Try to match input: Scan + predicate: col = literal.
            if let LogicalPlan::Scan { db: qdb, table, .. } = &input {
                if let BoundExpr::Binary {
                    op: crate::sql::ast::BinaryOp::Eq,
                    left,
                    right,
                    ..
                } = &predicate
                {
                    // col = literal or literal = col
                    let (col_idx, lit) = match (&**left, &**right) {
                        (BoundExpr::Column { index, .. }, BoundExpr::Literal(_)) => {
                            (*index, right.clone())
                        }
                        (BoundExpr::Literal(_), BoundExpr::Column { index, .. }) => {
                            (*index, left.clone())
                        }
                        _ => {
                            return LogicalPlan::Filter {
                                input: Box::new(input),
                                predicate,
                            }
                        }
                    };
                    let lookup = qdb.as_deref().unwrap_or_else(|| db.current_db());
                    if let Ok(t) = db.table_in(lookup, table) {
                        if let Some(idx) = t.indexes.iter().find(|i| i.column == col_idx) {
                            return LogicalPlan::IndexScan {
                                db: qdb.clone(),
                                table: table.clone(),
                                index: idx.name.clone(),
                                value: *lit,
                            };
                        }
                    }
                }
            }
            LogicalPlan::Filter {
                input: Box::new(input),
                predicate,
            }
        }
        LogicalPlan::Project {
            input,
            exprs,
            names,
        } => LogicalPlan::Project {
            input: Box::new(apply_indexes(db, *input)),
            exprs,
            names,
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(apply_indexes(db, *input)),
            keys,
        },
        LogicalPlan::Limit { input, limit } => LogicalPlan::Limit {
            input: Box::new(apply_indexes(db, *input)),
            limit,
        },
        LogicalPlan::Offset { input, offset } => LogicalPlan::Offset {
            input: Box::new(apply_indexes(db, *input)),
            offset,
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(apply_indexes(db, *input)),
        },
        LogicalPlan::Window {
            input,
            windows,
            input_cols,
        } => LogicalPlan::Window {
            input: Box::new(apply_indexes(db, *input)),
            windows,
            input_cols,
        },
        LogicalPlan::Aggregate { input, keys, aggs } => LogicalPlan::Aggregate {
            input: Box::new(apply_indexes(db, *input)),
            keys,
            aggs,
        },
        LogicalPlan::Join {
            kind,
            left,
            right,
            left_keys,
            right_keys,
            left_cols,
        } => LogicalPlan::Join {
            kind,
            left: Box::new(apply_indexes(db, *left)),
            right: Box::new(apply_indexes(db, *right)),
            left_keys,
            right_keys,
            left_cols,
        },
        LogicalPlan::NestedLoopJoin {
            kind,
            left,
            right,
            on,
            left_cols,
        } => LogicalPlan::NestedLoopJoin {
            kind,
            left: Box::new(apply_indexes(db, *left)),
            right: Box::new(apply_indexes(db, *right)),
            on,
            left_cols,
        },
        LogicalPlan::SetOp {
            op,
            left,
            right,
            types,
        } => LogicalPlan::SetOp {
            op,
            left: Box::new(apply_indexes(db, *left)),
            right: Box::new(apply_indexes(db, *right)),
            types,
        },
        LogicalPlan::Cte { name, query, body } => LogicalPlan::Cte {
            name,
            query: Box::new(apply_indexes(db, *query)),
            body: Box::new(apply_indexes(db, *body)),
        },
        LogicalPlan::Explain { plan } => LogicalPlan::Explain {
            plan: Box::new(apply_indexes(db, *plan)),
        },
        other => other,
    }
}

/// Drop a `Project` that feeds an aggregate that only counts rows
/// (`keys == []` and every aggregate is `COUNT(*)`): such an aggregate never
/// reads a column of its input, so evaluating the projection is pure waste
/// (e.g. `SELECT COUNT(*) FROM (SELECT upper(x) FROM t)`). Recurses so
/// nested shapes are covered.
fn drop_count_star_projections(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Aggregate { input, keys, aggs } => {
            let input = *input;
            let is_count_star_only = keys.is_empty()
                && !aggs.is_empty()
                && aggs.iter().all(|a| {
                    matches!(a.func, crate::sql::binder::AggFunc::Count)
                        && a.arg.is_none()
                        && !a.distinct
                });
            let new_input = if is_count_star_only {
                // COUNT(*) ignores its input's columns and row order, so any
                // chain of Project / Sort / Window feeding it is pure waste.
                // (Filter, Limit, Distinct, Join change the row set and must
                // stay.) Peel them all.
                let mut cur = input;
                while let LogicalPlan::Project { input: inner, .. }
                | LogicalPlan::Sort { input: inner, .. }
                | LogicalPlan::Window { input: inner, .. } = cur
                {
                    cur = *inner;
                }
                drop_count_star_projections(cur)
            } else {
                drop_count_star_projections(input)
            };
            LogicalPlan::Aggregate {
                input: Box::new(new_input),
                keys,
                aggs,
            }
        }
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(drop_count_star_projections(*input)),
            predicate,
        },
        LogicalPlan::Project {
            input,
            exprs,
            names,
        } => LogicalPlan::Project {
            input: Box::new(drop_count_star_projections(*input)),
            exprs,
            names,
        },
        LogicalPlan::Join {
            kind,
            left,
            right,
            left_keys,
            right_keys,
            left_cols,
        } => LogicalPlan::Join {
            kind,
            left: Box::new(drop_count_star_projections(*left)),
            right: Box::new(drop_count_star_projections(*right)),
            left_keys,
            right_keys,
            left_cols,
        },
        LogicalPlan::NestedLoopJoin {
            kind,
            left,
            right,
            on,
            left_cols,
        } => LogicalPlan::NestedLoopJoin {
            kind,
            left: Box::new(drop_count_star_projections(*left)),
            right: Box::new(drop_count_star_projections(*right)),
            on,
            left_cols,
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(drop_count_star_projections(*input)),
            keys,
        },
        LogicalPlan::Limit { input, limit } => LogicalPlan::Limit {
            input: Box::new(drop_count_star_projections(*input)),
            limit,
        },
        LogicalPlan::Offset { input, offset } => LogicalPlan::Offset {
            input: Box::new(drop_count_star_projections(*input)),
            offset,
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(drop_count_star_projections(*input)),
        },
        LogicalPlan::Window {
            input,
            windows,
            input_cols,
        } => LogicalPlan::Window {
            input: Box::new(drop_count_star_projections(*input)),
            windows,
            input_cols,
        },
        LogicalPlan::Cte { name, query, body } => LogicalPlan::Cte {
            name,
            query: Box::new(drop_count_star_projections(*query)),
            body: Box::new(drop_count_star_projections(*body)),
        },
        LogicalPlan::SetOp {
            op,
            left,
            right,
            types,
        } => LogicalPlan::SetOp {
            op,
            left: Box::new(drop_count_star_projections(*left)),
            right: Box::new(drop_count_star_projections(*right)),
            types,
        },
        LogicalPlan::Explain { plan } => LogicalPlan::Explain {
            plan: Box::new(drop_count_star_projections(*plan)),
        },
        other => other,
    }
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
        // A folded value cannot carry a parameterized type (a DECIMAL value
        // only knows its scale, not its precision) while downstream
        // output-schema/flush paths read the type off the expression, so
        // DECIMAL/INTERVAL casts stay unfolded. Evaluation is per-row either
        // way; this is a correctness guard, not a perf choice.
        && !contains_parameterized_cast(&rebuilt)
    {
        if let Ok(v) = eval_expr(&rebuilt, &const_chunk(), cx) {
            if v.len() == 1 {
                let value = v.get(0);
                // A folded NULL would lose the expression's type: a bare
                // `Literal(Null)` is INTEGER-typed (the `SELECT NULL`
                // convention), while the folded expression may be BOOLEAN (a
                // comparison), DATE, … — downstream operators check the type
                // (`WHERE` needs BOOLEAN), so pin the original type with a
                // CAST of NULL. Fixes `SELECT * FROM t WHERE NULL` and
                // `WHERE 1 = NULL` (plus procedure variables that are NULL).
                if value.is_null() {
                    let dtype = rebuilt.dtype();
                    if dtype != DataType::Integer {
                        return BoundExpr::Cast {
                            expr: Box::new(BoundExpr::Literal(crate::types::Value::Null)),
                            dtype,
                        };
                    }
                }
                return BoundExpr::Literal(value);
            }
        }
    }
    rebuilt
}

/// True when any node of `expr` is a cast whose target type carries
/// parameters a `Value` cannot round-trip (DECIMAL precision, INTERVAL).
/// Such a tree must not be constant-folded into a literal.
fn contains_parameterized_cast(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::Cast { expr, dtype } => {
            matches!(
                dtype,
                crate::types::DataType::Decimal { .. } | crate::types::DataType::Interval
            ) || contains_parameterized_cast(expr)
        }
        BoundExpr::Literal(_) | BoundExpr::Column { .. } | BoundExpr::AggRef { .. } => false,
        BoundExpr::Not(e)
        | BoundExpr::Negate { expr: e, .. }
        | BoundExpr::IsNull { expr: e, .. } => contains_parameterized_cast(e),
        BoundExpr::Binary { left, right, .. } => {
            contains_parameterized_cast(left) || contains_parameterized_cast(right)
        }
        BoundExpr::Scalar { args, .. } | BoundExpr::Coalesce { args, .. } => {
            args.iter().any(contains_parameterized_cast)
        }
        BoundExpr::Case {
            branches, else_, ..
        } => {
            branches
                .iter()
                .any(|(c, v)| contains_parameterized_cast(c) || contains_parameterized_cast(v))
                || else_
                    .as_ref()
                    .is_some_and(|e| contains_parameterized_cast(e))
        }
        // Subqueries/dynamic calls/window refs: not folded anyway.
        _ => false,
    }
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
            left_keys,
            right_keys,
            left_cols,
        } => LogicalPlan::Join {
            kind,
            left: Box::new(fold_plan(db, *left)),
            right: Box::new(fold_plan(db, *right)),
            left_keys: left_keys.into_iter().map(|k| fold_expr(&cx, k)).collect(),
            right_keys: right_keys.into_iter().map(|k| fold_expr(&cx, k)).collect(),
            left_cols,
        },
        LogicalPlan::NestedLoopJoin {
            kind,
            left,
            right,
            on,
            left_cols,
        } => LogicalPlan::NestedLoopJoin {
            kind,
            left: Box::new(fold_plan(db, *left)),
            right: Box::new(fold_plan(db, *right)),
            on: on.map(|e| fold_expr(&cx, e)),
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
        LogicalPlan::Offset { input, offset } => LogicalPlan::Offset {
            input: Box::new(fold_plan(db, *input)),
            offset,
        },
        LogicalPlan::Insert { db, table, rows } => LogicalPlan::Insert {
            db,
            table,
            rows: rows
                .into_iter()
                .map(|r| r.into_iter().map(|e| fold_expr(&cx, e)).collect())
                .collect(),
        },
        LogicalPlan::InsertSelect {
            db: qdb,
            table,
            columns,
            query,
        } => LogicalPlan::InsertSelect {
            db: qdb,
            table,
            columns,
            query: Box::new(fold_plan(db, *query)),
        },
        LogicalPlan::SetOp {
            op,
            left,
            right,
            types,
        } => LogicalPlan::SetOp {
            op,
            left: Box::new(fold_plan(db, *left)),
            right: Box::new(fold_plan(db, *right)),
            types,
        },
        LogicalPlan::Explain { plan } => LogicalPlan::Explain {
            plan: Box::new(fold_plan(db, *plan)),
        },
        LogicalPlan::CreateTableAs {
            db: qdb,
            name,
            query,
            if_not_exists,
        } => LogicalPlan::CreateTableAs {
            db: qdb,
            name,
            query: Box::new(fold_plan(db, *query)),
            if_not_exists,
        },
        leaf => leaf,
    }
}

// ---------- predicate pushdown ----------

/// Column indices referenced by `expr` (AggRefs/WindowRefs count as
/// unpushable, marked `usize::MAX`). Subquery bodies live in their own scope;
/// only an IN-subquery's outer expression can reference pushable columns.
pub(crate) fn columns_in(expr: &BoundExpr, out: &mut Vec<usize>) {
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
        BoundExpr::Column { index, .. } => match exprs.get(*index)? {
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
            left_keys,
            right_keys,
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
                    // Inner/Cross: split freely (unchanged since wave 3).
                    JoinKind::Inner | JoinKind::Cross => {
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
                    // FULL: both sides can be NULL-extended, so nothing moves
                    // (conservative; see the join pushdown tests).
                    JoinKind::Full => keep.push(c),
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
                left_keys,
                right_keys,
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
        // Below UNION/UNION ALL: selection distributes over both branches
        // (same output width by construction; per-branch types were checked
        // compatible at bind time) — but cloning the predicate into both
        // branches would run a dynamic call once per branch instead of once
        // per output row, so a predicate containing one stays above.
        // INTERSECT/EXCEPT never push: filtering one side changes the set
        // semantics (`(A EXCEPT B) WHERE p` ≠ `(A WHERE p) EXCEPT (B WHERE p)`).
        LogicalPlan::SetOp {
            op,
            left,
            right,
            types,
        } => {
            let pushable = matches!(
                op,
                crate::sql::ast::SetOp::Union | crate::sql::ast::SetOp::UnionAll
            ) && !pred.contains_dynamic_scalar();
            if !pushable {
                return (
                    LogicalPlan::Filter {
                        input: Box::new(LogicalPlan::SetOp {
                            op,
                            left,
                            right,
                            types,
                        }),
                        predicate: pred,
                    },
                    false,
                );
            }
            (
                LogicalPlan::SetOp {
                    op,
                    left: Box::new(LogicalPlan::Filter {
                        input: left,
                        predicate: pred.clone(),
                    }),
                    right: Box::new(LogicalPlan::Filter {
                        input: right,
                        predicate: pred,
                    }),
                    types,
                },
                true,
            )
        }
        // A nested-loop join's condition is not a hash key: pushing a WHERE
        // conjunct into one side would change outer-join and non-equi
        // semantics, so the filter always stays above (children still get
        // their own pushdown).
        LogicalPlan::NestedLoopJoin {
            kind,
            left,
            right,
            on,
            left_cols,
        } => (
            LogicalPlan::Filter {
                input: Box::new(LogicalPlan::NestedLoopJoin {
                    kind,
                    left: Box::new(pushdown(*left)),
                    right: Box::new(pushdown(*right)),
                    on,
                    left_cols,
                }),
                predicate: pred,
            },
            false,
        ),
        LogicalPlan::Window {
            input,
            windows,
            input_cols,
        } => {
            // A conjunct may move below the Window only when it is constant
            // within every window's partition, i.e. it filters solely on bare
            // partition-key columns. Pushing any other input-column conjunct
            // would recompute ROW_NUMBER/RANK/SUM/FIRST_VALUE/… over just the
            // surviving rows (a window must see every input row).
            let mut safe: Option<Vec<usize>> = None;
            for w in &windows {
                let cols: Vec<usize> = w
                    .partition
                    .iter()
                    .filter_map(|p| match p {
                        BoundExpr::Column { index, .. } => Some(*index),
                        _ => None,
                    })
                    .collect();
                safe = Some(match safe {
                    None => cols,
                    Some(prev) => prev.into_iter().filter(|c| cols.contains(c)).collect(),
                });
            }
            let safe = safe.unwrap_or_default();
            let mut conjuncts = Vec::new();
            split_conjuncts(pred, &mut conjuncts);
            let mut keep = Vec::new();
            let mut push = Vec::new();
            for c in conjuncts {
                let mut cols = Vec::new();
                columns_in(&c, &mut cols);
                if !cols.is_empty() && cols.iter().all(|i| *i < input_cols && safe.contains(i)) {
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
            left_keys,
            right_keys,
            left_cols,
        } => LogicalPlan::Join {
            kind,
            left: Box::new(pushdown(*left)),
            right: Box::new(pushdown(*right)),
            left_keys,
            right_keys,
            left_cols,
        },
        LogicalPlan::NestedLoopJoin {
            kind,
            left,
            right,
            on,
            left_cols,
        } => LogicalPlan::NestedLoopJoin {
            kind,
            left: Box::new(pushdown(*left)),
            right: Box::new(pushdown(*right)),
            on,
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
        LogicalPlan::SetOp {
            op,
            left,
            right,
            types,
        } => LogicalPlan::SetOp {
            op,
            left: Box::new(pushdown(*left)),
            right: Box::new(pushdown(*right)),
            types,
        },
        LogicalPlan::InsertSelect {
            db,
            table,
            columns,
            query,
        } => LogicalPlan::InsertSelect {
            db,
            table,
            columns,
            query: Box::new(pushdown(*query)),
        },
        LogicalPlan::CreateTableAs {
            db,
            name,
            query,
            if_not_exists,
        } => LogicalPlan::CreateTableAs {
            db,
            name,
            query: Box::new(pushdown(*query)),
            if_not_exists,
        },
        LogicalPlan::Explain { plan } => LogicalPlan::Explain {
            plan: Box::new(pushdown(*plan)),
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
        LogicalPlan::Offset { input, offset } => LogicalPlan::Offset {
            input: Box::new(pushdown(*input)),
            offset,
        },
        leaf => leaf,
    }
}

// ---------- FK→PK join elimination ----------

/// Column liveness for the top-down elimination pass: which of a node's
/// output columns are referenced anywhere above it.
#[derive(Debug, Clone)]
enum Live {
    /// Every output column is needed (the conservative default — also what
    /// nodes whose width cannot be derived pass down).
    All,
    /// Exactly the `true` positions.
    Cols(Vec<bool>),
}

impl Live {
    fn mark(&mut self, i: usize) {
        if let Live::Cols(m) = self {
            if let Some(b) = m.get_mut(i) {
                *b = true;
            }
        }
    }

    /// Mark every column an expression references (AggRef/WindowRef sentinels
    /// are not columns and are skipped).
    fn mark_expr(&mut self, e: &BoundExpr) {
        let mut cols = Vec::new();
        columns_in(e, &mut cols);
        for c in cols {
            if c != usize::MAX {
                self.mark(c);
            }
        }
    }

    /// True when every output column from `from` on is dead. `All` is never
    /// all-dead and a mask too short to judge is conservatively live.
    fn dead_from(&self, from: usize) -> bool {
        match self {
            Live::All => false,
            Live::Cols(m) => m.get(from..).is_some_and(|rest| rest.iter().all(|b| !b)),
        }
    }

    /// The first `n` positions of this liveness (the identity projection of a
    /// wider node, e.g. one `SetOp` branch).
    fn take(&self, n: usize) -> Live {
        match self {
            Live::All => Live::All,
            Live::Cols(m) => Live::Cols(m[..n.min(m.len())].to_vec()),
        }
    }

    /// All-dead mask of width `n` (for nodes that compute their own input
    /// liveness from scratch).
    fn none_of(n: usize) -> Live {
        Live::Cols(vec![false; n])
    }
}

/// Exact output width of `plan` when it can be derived (`None` = unknown; the
/// pass then stays conservative). Catalog scans resolve against the store;
/// a CTE scan (`db: None`) does not.
fn plan_width(db: &crate::storage::PinkDb, plan: &LogicalPlan) -> Option<usize> {
    match plan {
        LogicalPlan::Scan {
            db: Some(dbname),
            table,
            ..
        } => db.table_in(dbname, table).ok().map(|t| t.num_cols()),
        LogicalPlan::SingleRow => Some(0),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Offset { input, .. } => plan_width(db, input),
        LogicalPlan::Project { exprs, .. } => Some(exprs.len()),
        LogicalPlan::Aggregate { keys, aggs, .. } => Some(keys.len() + aggs.len()),
        LogicalPlan::Window { input, windows, .. } => Some(plan_width(db, input)? + windows.len()),
        LogicalPlan::Join { left, right, .. } | LogicalPlan::NestedLoopJoin { left, right, .. } => {
            Some(plan_width(db, left)? + plan_width(db, right)?)
        }
        LogicalPlan::SetOp { types, .. } => Some(types.len()),
        LogicalPlan::Cte { body, .. } => plan_width(db, body),
        _ => None,
    }
}

/// Where one output column of `plan` comes from: `(database, table, column
/// index, nullable-by-outer-join)`. Resolvable through transparent nodes
/// (filters, sorts, limits, distincts, passthrough projections) and joins —
/// with the outer-join flag set for columns that can be NULL-extended on the
/// way, so the elimination rule can reject them. `None` = opaque (CTEs, set
/// operations, computed projections, aggregates, ...): the pass then makes no
/// claim about the column.
#[allow(clippy::type_complexity)]
fn column_origin(
    db: &crate::storage::PinkDb,
    plan: &LogicalPlan,
    index: usize,
) -> Option<(String, String, usize, bool)> {
    match plan {
        LogicalPlan::Scan {
            db: Some(dbname),
            table,
            ..
        } => {
            let t = db.table_in(dbname, table).ok()?;
            (index < t.num_cols()).then(|| {
                (
                    dbname.to_ascii_lowercase(),
                    table.to_ascii_lowercase(),
                    index,
                    false,
                )
            })
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Offset { input, .. } => column_origin(db, input, index),
        LogicalPlan::Project { input, exprs, .. } => match exprs.get(index)? {
            BoundExpr::Column { index: i, .. } => column_origin(db, input, *i),
            _ => None,
        },
        LogicalPlan::Window {
            input, input_cols, ..
        } => (index < *input_cols)
            .then(|| column_origin(db, input, index))
            .flatten(),
        LogicalPlan::Join {
            kind,
            left,
            right,
            left_cols,
            ..
        }
        | LogicalPlan::NestedLoopJoin {
            kind,
            left,
            right,
            left_cols,
            ..
        } => {
            let on_left = index < *left_cols;
            let base = if on_left { 0 } else { *left_cols };
            let side = if on_left { left } else { right };
            let mut origin = column_origin(db, side, index - base)?;
            origin.3 |= match kind {
                JoinKind::Inner | JoinKind::Cross => false,
                // LEFT null-extends only its right side, RIGHT only its left.
                JoinKind::Left => !on_left,
                JoinKind::Right => on_left,
                JoinKind::Full => true,
            };
            Some(origin)
        }
        _ => None,
    }
}

/// True when `left INNER JOIN right ON left_keys = right_keys` is an identity
/// on the left side and can therefore be replaced by `left`.
///
/// The rule (DuckDB's declarative posture: the optimizer *trusts* the
/// declarations even though they are never enforced on writes):
/// - `right` is a bare catalog-table scan (a filter/projection on the parent
///   side would change which rows qualify);
/// - the join keys are exactly the column pairs of one declared
///   `FOREIGN KEY` on the left side's base table (resolved through inner
///   joins), with the child columns NOT NULL *by declaration* — a nullable
///   key would make the inner join drop NULL-key rows that the child side
///   keeps;
/// - the key columns do not flow through the null-extending side of an outer
///   join inside the left subtree;
/// - the parent table *currently* declares PRIMARY KEY/UNIQUE on exactly the
///   referenced columns (a re-created parent that lost its key makes the
///   stale foreign key unusable).
///
/// Caveat: unenforced declarations can be wrong (a dangling reference, a
/// duplicate parent key, or a NOT NULL column holding NULL) — then this
/// rewrite changes results. That is exactly the documented trade-off of using
/// informational constraints.
#[allow(clippy::too_many_arguments)]
fn fk_join_eliminable(
    db: &crate::storage::PinkDb,
    left: &LogicalPlan,
    right: &LogicalPlan,
    left_keys: &[BoundExpr],
    right_keys: &[BoundExpr],
) -> bool {
    let LogicalPlan::Scan {
        db: Some(ref_db),
        table: ref_table,
        ..
    } = right
    else {
        return false;
    };
    if left_keys.is_empty() || left_keys.len() != right_keys.len() {
        return false;
    }
    let Ok(ref_t) = db.table_in(ref_db, ref_table) else {
        return false;
    };
    // The left keys must be bare columns of one base table.
    let mut child_cols = Vec::with_capacity(left_keys.len());
    for e in left_keys {
        let BoundExpr::Column { index, .. } = e else {
            return false;
        };
        let Some((cdb, ctable, ccol, outer_nullable)) = column_origin(db, left, *index) else {
            return false;
        };
        if outer_nullable {
            return false;
        }
        child_cols.push((cdb, ctable, ccol));
    }
    let (child_db, child_table, _) = &child_cols[0];
    if child_cols
        .iter()
        .any(|(d, t, _)| d != child_db || t != child_table)
    {
        return false;
    }
    let Ok(child_t) = db.table_in(child_db, child_table) else {
        return false;
    };
    let cols: Vec<usize> = child_cols.iter().map(|(_, _, c)| *c).collect();
    // A FK on exactly these child columns (any declaration order). The
    // length guard keeps hand-built constraint sets (the public Rust API)
    // from indexing past `ref_columns`.
    let Some(fk) = child_t.constraints.foreign_keys.iter().find(|fk| {
        fk.columns.len() == cols.len()
            && fk.ref_columns.len() == fk.columns.len()
            && fk.columns.iter().all(|c| cols.contains(c))
            && cols.iter().all(|c| fk.columns.contains(c))
    }) else {
        return false;
    };
    if !fk.ref_db.eq_ignore_ascii_case(ref_db) || !fk.ref_table.eq_ignore_ascii_case(ref_table) {
        return false;
    }
    // Every FK column must be NOT NULL by declaration (see the rule above).
    if !fk
        .columns
        .iter()
        .all(|c| child_t.constraints.is_not_null(*c))
    {
        return false;
    }
    // Each join pair maps a child key column to its referenced column, and
    // every FK position is covered exactly once.
    let mut seen = vec![false; fk.columns.len()];
    for (i, e) in right_keys.iter().enumerate() {
        let BoundExpr::Column { index: rcol, .. } = e else {
            return false;
        };
        let Some(pos) = fk.columns.iter().position(|c| *c == cols[i]) else {
            return false;
        };
        if seen[pos] || fk.ref_columns[pos] != *rcol {
            return false;
        }
        seen[pos] = true;
    }
    // The parent still declares a key on exactly the referenced columns.
    ref_t.constraints.key_on(&fk.ref_columns).is_some()
}

/// Liveness of a transparent node's input: the same mask when the input width
/// is known, else `All` (conservative).
fn pass_live(db: &crate::storage::PinkDb, input: &LogicalPlan, live: &Live) -> Live {
    match plan_width(db, input) {
        Some(w) => live.take(w),
        None => Live::All,
    }
}

/// FK→PK join elimination: replace `child INNER JOIN parent ON child.fk =
/// parent.pk` by the child side whenever the parent's columns are unused
/// above the join and the join keys are exactly a declared foreign key
/// ([`fk_join_eliminable`]).
///
/// Column liveness flows top-down so "the parent's columns are unused" is
/// exact: a join whose right side still feeds a projection, filter, sort key,
/// aggregate argument, ... is never eliminated. Constraints are declarative
/// metadata (never enforced on writes), so the rewrite trusts them.
pub fn eliminate_fk_joins(db: &crate::storage::PinkDb, plan: LogicalPlan) -> LogicalPlan {
    let live = match plan_width(db, &plan) {
        Some(w) => Live::Cols(vec![true; w]),
        None => Live::All,
    };
    elim(db, plan, &live)
}

/// One recursive step of [`eliminate_fk_joins`]; `live` describes which of
/// `plan`'s output columns are used above.
fn elim(db: &crate::storage::PinkDb, plan: LogicalPlan, live: &Live) -> LogicalPlan {
    match plan {
        LogicalPlan::Join {
            kind,
            left,
            right,
            left_keys,
            right_keys,
            left_cols,
        } => {
            // Child liveness: what is live above, plus this join's own keys.
            let (mut left_live, mut right_live) =
                match (live, plan_width(db, &left), plan_width(db, &right)) {
                    (Live::Cols(m), Some(lw), Some(rw)) if m.len() >= lw + rw => (
                        Live::Cols(m[..lw].to_vec()),
                        Live::Cols(m[lw..lw + rw].to_vec()),
                    ),
                    _ => (Live::All, Live::All),
                };
            for e in &left_keys {
                left_live.mark_expr(e);
            }
            for e in &right_keys {
                right_live.mark_expr(e);
            }
            if kind == JoinKind::Inner
                && live.dead_from(left_cols)
                && fk_join_eliminable(db, &left, &right, &left_keys, &right_keys)
            {
                // The join is an identity on the child side; the child's own
                // key columns are no longer needed either.
                let child_live = live.take(left_cols);
                return elim(db, *left, &child_live);
            }
            LogicalPlan::Join {
                kind,
                left: Box::new(elim(db, *left, &left_live)),
                right: Box::new(elim(db, *right, &right_live)),
                left_keys,
                right_keys,
                left_cols,
            }
        }
        LogicalPlan::NestedLoopJoin {
            kind,
            left,
            right,
            on,
            left_cols,
        } => {
            let (mut left_live, mut right_live) =
                match (live, plan_width(db, &left), plan_width(db, &right)) {
                    (Live::Cols(m), Some(lw), Some(rw)) if m.len() >= lw + rw => (
                        Live::Cols(m[..lw].to_vec()),
                        Live::Cols(m[lw..lw + rw].to_vec()),
                    ),
                    _ => (Live::All, Live::All),
                };
            if let Some(on) = &on {
                left_live.mark_expr(on);
                right_live.mark_expr(on);
            }
            LogicalPlan::NestedLoopJoin {
                kind,
                left: Box::new(elim(db, *left, &left_live)),
                right: Box::new(elim(db, *right, &right_live)),
                on,
                left_cols,
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            let mut inner = pass_live(db, &input, live);
            inner.mark_expr(&predicate);
            LogicalPlan::Filter {
                input: Box::new(elim(db, *input, &inner)),
                predicate,
            }
        }
        LogicalPlan::Sort { input, keys } => {
            let mut inner = pass_live(db, &input, live);
            for (e, _) in &keys {
                inner.mark_expr(e);
            }
            LogicalPlan::Sort {
                input: Box::new(elim(db, *input, &inner)),
                keys,
            }
        }
        LogicalPlan::Distinct { input } => {
            let inner = pass_live(db, &input, live);
            LogicalPlan::Distinct {
                input: Box::new(elim(db, *input, &inner)),
            }
        }
        LogicalPlan::Limit { input, limit } => {
            let inner = pass_live(db, &input, live);
            LogicalPlan::Limit {
                input: Box::new(elim(db, *input, &inner)),
                limit,
            }
        }
        LogicalPlan::Offset { input, offset } => {
            let inner = pass_live(db, &input, live);
            LogicalPlan::Offset {
                input: Box::new(elim(db, *input, &inner)),
                offset,
            }
        }
        LogicalPlan::Project {
            input,
            exprs,
            names,
        } => {
            // Input liveness: the columns of the live output expressions.
            let inner = match plan_width(db, &input) {
                Some(w) => {
                    let mut m = Live::none_of(w);
                    match live {
                        Live::All => {
                            for e in &exprs {
                                m.mark_expr(e);
                            }
                        }
                        Live::Cols(out) => {
                            for (i, e) in exprs.iter().enumerate() {
                                if out.get(i).copied().unwrap_or(true) {
                                    m.mark_expr(e);
                                }
                            }
                        }
                    }
                    m
                }
                None => Live::All,
            };
            LogicalPlan::Project {
                input: Box::new(elim(db, *input, &inner)),
                exprs,
                names,
            }
        }
        LogicalPlan::Aggregate { input, keys, aggs } => {
            let inner = match plan_width(db, &input) {
                Some(w) => {
                    let mut m = Live::none_of(w);
                    for k in &keys {
                        m.mark_expr(k);
                    }
                    for a in &aggs {
                        if let Some(e) = &a.arg {
                            m.mark_expr(e);
                        }
                    }
                    m
                }
                None => Live::All,
            };
            LogicalPlan::Aggregate {
                input: Box::new(elim(db, *input, &inner)),
                keys,
                aggs,
            }
        }
        LogicalPlan::Window {
            input,
            windows,
            input_cols,
        } => {
            let inner = match plan_width(db, &input) {
                Some(w) => {
                    let mut m = Live::none_of(w);
                    match live {
                        Live::All => {
                            // Output = input ++ windows; every input column is
                            // live.
                            m = Live::Cols(vec![true; w]);
                        }
                        Live::Cols(out) => {
                            for i in 0..w {
                                if out.get(i).copied().unwrap_or(true) {
                                    m.mark(i);
                                }
                            }
                        }
                    }
                    for win in &windows {
                        if let Some(e) = &win.arg {
                            m.mark_expr(e);
                        }
                        for e in &win.partition {
                            m.mark_expr(e);
                        }
                        for (e, _) in &win.order {
                            m.mark_expr(e);
                        }
                    }
                    m
                }
                None => Live::All,
            };
            LogicalPlan::Window {
                input: Box::new(elim(db, *input, &inner)),
                windows,
                input_cols,
            }
        }
        LogicalPlan::SetOp {
            op,
            left,
            right,
            types,
        } => {
            // Both branches are positionally identical; each gets the same
            // liveness (truncated to its own width).
            let lw = plan_width(db, &left);
            let rw = plan_width(db, &right);
            let llive = match lw {
                Some(w) => live.take(w),
                None => Live::All,
            };
            let rlive = match rw {
                Some(w) => live.take(w),
                None => Live::All,
            };
            LogicalPlan::SetOp {
                op,
                left: Box::new(elim(db, *left, &llive)),
                right: Box::new(elim(db, *right, &rlive)),
                types,
            }
        }
        LogicalPlan::Cte { name, query, body } => LogicalPlan::Cte {
            name,
            query: Box::new(elim(db, *query, &Live::All)),
            body: Box::new(elim(db, *body, live)),
        },
        LogicalPlan::InsertSelect {
            db: qdb,
            table,
            columns,
            query,
        } => LogicalPlan::InsertSelect {
            db: qdb,
            table,
            columns,
            query: Box::new(elim(db, *query, &Live::All)),
        },
        LogicalPlan::CreateTableAs {
            db: qdb,
            name,
            query,
            if_not_exists,
        } => LogicalPlan::CreateTableAs {
            db: qdb,
            name,
            query: Box::new(elim(db, *query, &Live::All)),
            if_not_exists,
        },
        LogicalPlan::Explain { plan } => LogicalPlan::Explain {
            plan: Box::new(elim(db, *plan, &Live::All)),
        },
        // Leaves (scans, DML without a query child, DDL, ...) carry no joins.
        other => other,
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
                left_keys: vec![col(0)],
                right_keys: vec![col(0)],
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
                left_keys: vec![col(0)],
                right_keys: vec![col(0)],
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
            left_keys: vec![col(0)],
            right_keys: vec![col(0)],
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
            distinct: false,
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
