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
use crate::plan::{LogicalPlan, map_columns, shift_columns};
use crate::sql::ast::{BinaryOp, JoinKind};
use crate::sql::binder::BoundExpr;
use crate::types::{DataType, Value};
use crate::vector::DataChunk;

/// Optimize a bound plan: constant folding + predicate pushdown + FK→PK join
/// elimination.
/// Takes the database for the folding evaluation context (folds never touch
/// stored data — only column-free expressions are evaluated) and for the
/// declarative constraints the elimination rule reads.
pub fn optimize(db: &crate::storage::PinkDb, plan: LogicalPlan) -> LogicalPlan {
    let plan = order_chains(db, fold_plan(db, plan));
    let plan = pushdown_inner(plan, Some(db));
    let plan = apply_vector_topk(db, plan);
    let plan = eliminate_fk_joins(db, plan);
    let plan = drop_count_star_projections(plan);
    let plan = apply_indexes(db, plan);
    fold_plan(db, plan)
}

/// Propose a hash-index lookup for `Filter { input: Scan, predicate: col =
/// literal }` when an index exists on that column.
///
/// The `Filter` node is **kept above** the [`LogicalPlan::IndexScan`]: the
/// index only proposes candidate rows, and the original predicate is always
/// re-checked. A stale or approximate index can therefore cost performance,
/// never correctness (H-3/H-4 backstop).
///
/// The probe key is canonicalized to the *indexed column's* type before it is
/// stored in the plan (H-5): the binder keeps a numeric literal's own type
/// (`INTEGER` literal against a `BIGINT`/`DOUBLE`/`DECIMAL` column, or an
/// explicit `CAST`), while the index map is keyed by the column's stored
/// representation. If the coercion cannot be represented the rewrite is
/// refused and the plain scan stays.
///
/// A stale index (a mutation set the table's flag and the caller did not
/// rebuild it yet) is never trusted: the plan keeps the full scan, so
/// correctness never depends on lifecycle bookkeeping.
fn apply_indexes(db: &crate::storage::PinkDb, plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let input = apply_indexes(db, *input);
            // Try to match input: Scan + predicate: col = literal.
            if let LogicalPlan::Scan { db: qdb, table, .. } = &input
                && let BoundExpr::Binary {
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
                        };
                    }
                };
                let lookup = qdb.clone().unwrap_or_else(|| db.current_db().to_string());
                let candidate = match db.table_in(&lookup, table) {
                    Ok(t) if !t.indexes_stale => t
                        .indexes
                        .iter()
                        .find(|i| i.column == col_idx)
                        .map(|i| (i.name.clone(), t.columns.get(col_idx).map(|c| c.dtype))),
                    _ => None,
                };
                if let Some((index_name, Some(column_type))) = candidate {
                    let literal = match &*lit {
                        BoundExpr::Literal(v) => v.clone(),
                        _ => unreachable!("matched a literal above"),
                    };
                    // Canonicalize to the indexed column's type (H-5), or
                    // keep the full scan when no faithful key exists.
                    let Some(canonical) = canonical_probe_key(column_type, literal) else {
                        return LogicalPlan::Filter {
                            input: Box::new(input),
                            predicate,
                        };
                    };
                    return LogicalPlan::Filter {
                        input: Box::new(LogicalPlan::IndexScan {
                            db: qdb.clone(),
                            table: table.clone(),
                            index: index_name,
                            value: BoundExpr::Literal(canonical),
                        }),
                        predicate,
                    };
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
        LogicalPlan::Distinct { input, on } => LogicalPlan::Distinct {
            input: Box::new(apply_indexes(db, *input)),
            on,
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
            residual,
        } => LogicalPlan::Join {
            kind,
            left: Box::new(apply_indexes(db, *left)),
            right: Box::new(apply_indexes(db, *right)),
            left_keys,
            right_keys,
            left_cols,
            residual,
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
        LogicalPlan::Cte {
            name,
            query,
            body,
            recursive,
        } => LogicalPlan::Cte {
            name,
            query: Box::new(apply_indexes(db, *query)),
            body: Box::new(apply_indexes(db, *body)),
            recursive: recursive.map(|r| Box::new(r.map(&|p: LogicalPlan| apply_indexes(db, p)))),
        },
        LogicalPlan::Explain { plan } => LogicalPlan::Explain {
            plan: Box::new(apply_indexes(db, *plan)),
        },
        other => other,
    }
}

/// Canonicalize an equality probe literal to the indexed column's type, or
/// `None` when the comparison semantics cannot be represented by one key.
///
/// `cast_value` is the engine's CAST rule, so for exact pairs (integers ↔
/// integers, integer → DECIMAL, DOUBLE → wider numeric, DECIMAL scale
/// differences) the key matches the stored representation. Two mixed pairs
/// compare through `f64` in [`crate::plan::compare_values`] and collapse
/// distinct column values, so a single canonical key could *miss* rows that
/// compare equal (never merely add candidates — the `Filter` above removes
/// false positives):
///
/// - `BIGINT`/`INTEGER` column vs `DOUBLE` literal: above 2^53 several
///   integers share one `f64` value.
/// - `DECIMAL` column vs `DOUBLE` literal: several decimals share one `f64`.
///
/// Those keep the plain scan. `NULL` also keeps the scan: index maps never
/// store NULL keys and the predicate yields its normal empty result.
fn canonical_probe_key(
    column: crate::types::DataType,
    literal: crate::types::Value,
) -> Option<crate::types::Value> {
    use crate::types::{DataType, Value};
    if let (DataType::Integer | DataType::Bigint | DataType::Decimal { .. }, Value::Double(_)) =
        (column, &literal)
    {
        return None;
    }
    match crate::exec::expr::cast_value(literal, column) {
        Ok(value) if !value.is_null() => Some(value),
        _ => None,
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
                        // `COUNT(*) FILTER (WHERE c …)` reads `c`.
                        && a.filter.is_none()
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
            residual,
        } => LogicalPlan::Join {
            kind,
            left: Box::new(drop_count_star_projections(*left)),
            right: Box::new(drop_count_star_projections(*right)),
            left_keys,
            right_keys,
            left_cols,
            residual,
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
        LogicalPlan::Distinct { input, on } => LogicalPlan::Distinct {
            input: Box::new(drop_count_star_projections(*input)),
            on,
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
        LogicalPlan::Cte {
            name,
            query,
            body,
            recursive,
        } => LogicalPlan::Cte {
            name,
            query: Box::new(drop_count_star_projections(*query)),
            body: Box::new(drop_count_star_projections(*body)),
            recursive: recursive.map(|r| Box::new(r.map(&drop_count_star_projections))),
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
        } => {
            let (left, right) = (fold_expr(cx, *left), fold_expr(cx, *right));
            // `false OR x` and `true AND x` are `x` for every x, NULL
            // included (the binder seeds an `IN` list's `OR` chain with
            // `false`); dropping the constant lets an `IN` list compare equal
            // to the same list derived elsewhere.
            let neutral = match op {
                BinaryOp::Or => Some(false),
                BinaryOp::And => Some(true),
                _ => None,
            };
            if let Some(b) = neutral {
                if left == BoundExpr::Literal(Value::Boolean(b)) {
                    return right;
                }
                if right == BoundExpr::Literal(Value::Boolean(b)) {
                    return left;
                }
            }
            BoundExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
                dtype,
            }
        }
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
        BoundExpr::DynamicScalar {
            name,
            args,
            dtype,
            reads,
        } => {
            return BoundExpr::DynamicScalar {
                name,
                args: args
                    .into_vec()
                    .into_iter()
                    .map(|a| fold_expr(cx, a))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                dtype,
                reads,
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
        && let Ok(v) = eval_expr(&rebuilt, &const_chunk(), cx)
            && v.len() == 1
    {
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
            a.filter = a.filter.map(|e| fold_expr(cx, e));
            a
        })
        .collect()
}

fn fold_plan(db: &crate::storage::PinkDb, plan: LogicalPlan) -> LogicalPlan {
    let cx = crate::exec::Cx::new(db);
    match plan {
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(fold_plan(db, *input)),
            predicate: order_conjuncts(fold_expr(&cx, predicate)),
        },
        LogicalPlan::Join {
            kind,
            left,
            right,
            left_keys,
            right_keys,
            left_cols,
            residual,
        } => LogicalPlan::Join {
            kind,
            left: Box::new(fold_plan(db, *left)),
            right: Box::new(fold_plan(db, *right)),
            left_keys: left_keys.into_iter().map(|k| fold_expr(&cx, k)).collect(),
            right_keys: right_keys.into_iter().map(|k| fold_expr(&cx, k)).collect(),
            left_cols,
            residual: residual.map(|e| fold_expr(&cx, e)),
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
        LogicalPlan::Cte {
            name,
            query,
            body,
            recursive,
        } => LogicalPlan::Cte {
            name,
            query: Box::new(fold_plan(db, *query)),
            body: Box::new(fold_plan(db, *body)),
            recursive: recursive.map(|r| Box::new(r.map(&|p| fold_plan(db, p)))),
        },
        LogicalPlan::Distinct { input, on } => LogicalPlan::Distinct {
            input: Box::new(fold_plan(db, *input)),
            on,
        },
        LogicalPlan::Values {
            rows,
            names,
            types,
            sources,
        } => LogicalPlan::Values {
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(|e| fold_expr(&cx, e)).collect())
                .collect(),
            names,
            types,
            sources,
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
        LogicalPlan::Insert {
            db,
            table,
            rows,
            conflict,
            returning,
        } => LogicalPlan::Insert {
            db,
            table,
            rows: rows
                .into_iter()
                .map(|r| r.into_iter().map(|e| fold_expr(&cx, e)).collect())
                .collect(),
            conflict,
            returning,
        },
        LogicalPlan::InsertSelect {
            db: qdb,
            table,
            columns,
            query,
            conflict,
            returning,
        } => LogicalPlan::InsertSelect {
            db: qdb,
            table,
            columns,
            query: Box::new(fold_plan(db, *query)),
            conflict,
            returning,
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
            or_replace,
        } => LogicalPlan::CreateTableAs {
            db: qdb,
            name,
            query: Box::new(fold_plan(db, *query)),
            if_not_exists,
            or_replace,
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
        // An outer column is no column of this plan: it must not make a
        // predicate look side-specific.
        BoundExpr::Literal(_)
        | BoundExpr::OuterRef { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. } => {}
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

/// The value a conjunct pins column `c` to with `=`, if it does.
fn eq_const(atom: &BoundExpr, c: usize) -> Option<&Value> {
    let BoundExpr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = atom
    else {
        return None;
    };
    match (&**left, &**right) {
        (BoundExpr::Column { index, .. }, BoundExpr::Literal(v)) if *index == c => Some(v),
        (BoundExpr::Literal(v), BoundExpr::Column { index, .. }) if *index == c => Some(v),
        _ => None,
    }
}

/// The literal bound on column `c` for `>=` / `<=`, if the column is on the
/// left (the binder puts it there).
fn range_const(atom: &BoundExpr, c: usize, op: BinaryOp) -> Option<&Value> {
    let BoundExpr::Binary {
        op: got,
        left,
        right,
        ..
    } = atom
    else {
        return None;
    };
    if *got != op {
        return None;
    }
    match (&**left, &**right) {
        (BoundExpr::Column { index, .. }, BoundExpr::Literal(v)) if *index == c => Some(v),
        _ => None,
    }
}

fn value_num(v: &Value) -> Option<f64> {
    match v {
        Value::Integer(i) => Some(*i as f64),
        Value::Bigint(i) => Some(*i as f64),
        Value::Double(f) => Some(*f),
        Value::Decimal(i, s) => Some(*i as f64 / 10f64.powi(*s as i32)),
        Value::Date(d) => Some(*d as f64),
        Value::Timestamp(t) => Some(*t as f64),
        _ => None,
    }
}

/// Disjunctive normal form: each term is a conjunction of the atoms it kept.
/// Returns false once the term count would exceed `cap`, so a pathological
/// `OR` chain cannot blow up the plan.
fn dnf_terms<'a>(e: &'a BoundExpr, terms: &mut Vec<Vec<&'a BoundExpr>>, cap: usize) -> bool {
    match e {
        BoundExpr::Binary {
            op: BinaryOp::Or,
            left,
            right,
            ..
        } => {
            // Disjunction concatenates: each branch contributes its own terms.
            let mut l = Vec::new();
            let mut r = Vec::new();
            if !dnf_terms(left, &mut l, cap) || !dnf_terms(right, &mut r, cap) {
                return false;
            }
            if l.len() + r.len() > cap {
                return false;
            }
            l.extend(r);
            *terms = l;
            true
        }
        BoundExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => {
            let mut l = Vec::new();
            let mut r = Vec::new();
            if !dnf_terms(left, &mut l, cap) || !dnf_terms(right, &mut r, cap) {
                return false;
            }
            if l.len().saturating_mul(r.len()) > cap {
                return false;
            }
            let mut out = Vec::with_capacity(l.len() * r.len());
            for a in l {
                for b in &r {
                    let mut t = a.clone();
                    t.extend(b.iter().copied());
                    out.push(t);
                }
            }
            *terms = out;
            true
        }
        other => {
            // A `FALSE` conjunct makes its whole term unsatisfiable, so the
            // term drops out of the union. The binder emits exactly this for
            // empty `IN` lists (`false OR c = …`), and keeping those terms
            // would make every derived fact look unstated.
            if !matches!(other, BoundExpr::Literal(Value::Boolean(false))) {
                terms.push(vec![other]);
            }
            true
        }
    }
}

/// The values an `OR`-chain of `c = literal` atoms pins column `c` to, when
/// every leaf is such an equality. The binder expands `c IN (…)` to exactly
/// this shape, so one atom can contribute several values.
fn eq_set(atom: &BoundExpr, c: usize) -> Option<Vec<Value>> {
    if let Some(v) = eq_const(atom, c) {
        return Some(vec![v.clone()]);
    }
    let BoundExpr::Binary {
        op: BinaryOp::Or,
        left,
        right,
        ..
    } = atom
    else {
        return None;
    };
    let mut got = eq_set(left, c)?;
    got.extend(eq_set(right, c)?);
    Some(got)
}

/// Whether `a` and `b` both pin one column to the same set of values (`c IN
/// (…)` in any order, or `c = v`): a derived fact equal to a conjunct already
/// present adds nothing but a second evaluation.
fn same_value_set(a: &BoundExpr, b: &BoundExpr) -> bool {
    let column = |e: &BoundExpr| {
        let mut cols = Vec::new();
        columns_in(e, &mut cols);
        cols.sort_unstable();
        cols.dedup();
        match cols.as_slice() {
            [c] if *c != usize::MAX => Some(*c),
            _ => None,
        }
    };
    let (Some(c), Some(d)) = (column(a), column(b)) else {
        return false;
    };
    if c != d {
        return false;
    }
    match (eq_set(a, c), eq_set(b, c)) {
        (Some(x), Some(y)) => x.iter().all(|v| y.contains(v)) && y.iter().all(|v| x.contains(v)),
        _ => false,
    }
}

/// A filter's conjuncts in ascending estimated cost, so the cheap ones run
/// first and the costly ones (string compares, `LIKE`, long `IN` lists) only
/// see the rows that survive them — the selection narrows conjunct by
/// conjunct. A port of DuckDB's `ExpressionHeuristics`
/// (src/optimizer/expression_heuristics.cpp) and its cost table.
///
/// Stricter than DuckDB: conjuncts are reordered only when none of them can
/// raise an error, because a conjunct's error fires only for the rows that
/// reach it (wave 123a) and a reorder must not change which rows those are.
/// Identical conjuncts are evaluated once. A stable sort keeps equal-cost
/// conjuncts in written order.
fn order_conjuncts(pred: BoundExpr) -> BoundExpr {
    if !matches!(
        pred,
        BoundExpr::Binary {
            op: BinaryOp::And,
            ..
        }
    ) {
        return pred;
    }
    let mut parts = Vec::new();
    split_conjuncts(pred.clone(), &mut parts);
    if parts.len() < 2 || !parts.iter().all(infallible) {
        return pred;
    }
    let mut unique: Vec<BoundExpr> = Vec::with_capacity(parts.len());
    for p in parts {
        if !unique.contains(&p) {
            unique.push(p);
        }
    }
    unique.sort_by_key(expr_cost);
    and_all(unique).expect("at least one conjunct")
}

/// Whether evaluating `e` can never raise an error: comparisons, `LIKE`,
/// `AND`/`OR`/`NOT` and `IS [NOT] NULL` over columns and literals.
fn infallible(e: &BoundExpr) -> bool {
    match e {
        BoundExpr::Literal(_) | BoundExpr::Column { .. } => true,
        BoundExpr::Binary {
            op, left, right, ..
        } => {
            matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
                    | BinaryOp::Like
                    | BinaryOp::NotLike
                    | BinaryOp::And
                    | BinaryOp::Or
            ) && infallible(left)
                && infallible(right)
        }
        BoundExpr::Not(x) | BoundExpr::IsNull { expr: x, .. } => infallible(x),
        _ => false,
    }
}

/// DuckDB's per-type weight: strings cost most to compare, doubles more than
/// fixed-width integers.
fn type_cost(t: DataType) -> u64 {
    match t {
        DataType::Varchar => 5,
        DataType::Double => 2,
        _ => 1,
    }
}

/// DuckDB's expression cost (`ExpressionHeuristics::Cost`) for the shapes
/// [`infallible`] admits; anything else is 1000 there and here.
fn expr_cost(e: &BoundExpr) -> u64 {
    match e {
        BoundExpr::Column { dtype, .. } => type_cost(*dtype) * 8,
        BoundExpr::Literal(v) => v.data_type().map_or(1, type_cost),
        BoundExpr::Binary {
            op, left, right, ..
        } => {
            let children = expr_cost(left) + expr_cost(right);
            children
                + match op {
                    BinaryOp::Like | BinaryOp::NotLike => 200,
                    _ => 5,
                }
        }
        BoundExpr::Not(x) => expr_cost(x) + 10,
        BoundExpr::IsNull { expr, .. } => expr_cost(expr) + 5,
        _ => 1000,
    }
}

/// Facts every branch of an `OR` agrees on, as one conjunct per column:
/// `(a = 1 AND …) OR (a = 2 AND …)` implies `a IN (1, 2)`, and bounds imply
/// the union's bound — any row that satisfies one branch satisfies those. The
/// disjunction itself is kept (derived facts are weaker), so this only narrows
/// the rows the branches must inspect.
fn derive_common_filters(pred: &BoundExpr) -> Option<BoundExpr> {
    let mut terms = Vec::new();
    if !dnf_terms(pred, &mut terms, 256) || terms.len() < 2 {
        return None;
    }
    let mut cols = Vec::new();
    columns_in(pred, &mut cols);
    cols.sort_unstable();
    cols.dedup();
    let mut facts = Vec::new();
    for &c in &cols {
        if c == usize::MAX {
            continue;
        }
        let mut values = Vec::new();
        let mut every_eq = true;
        for term in &terms {
            let mut hit = Vec::new();
            for atom in term {
                if let Some(vs) = eq_set(atom, c) {
                    hit.extend(vs);
                }
            }
            if hit.is_empty() {
                every_eq = false;
                break;
            }
            values.extend(hit);
        }
        let dtype = values.first().and_then(|v| v.data_type());
        // Deduplicate while keeping a deterministic plan shape. The cap keeps
        // a pathological `OR` chain from making the pairwise check quadratic;
        // any list that large fails the `<= 64` test anyway.
        if values.len() > 1024 {
            continue;
        }
        let mut uniq: Vec<Value> = Vec::new();
        for v in values {
            if !uniq.contains(&v) {
                uniq.push(v);
            }
        }
        // Stable order keeps the derived conjunct identical when pushdown
        // revisits the node, so the "already present" check matches.
        uniq.sort_by_key(|v| format!("{v:?}"));
        if every_eq && !uniq.is_empty() && uniq.len() <= 64 {
            if uniq.len() == 1 {
                let v = uniq.pop().unwrap();
                facts.push(BoundExpr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(BoundExpr::Column {
                        index: c,
                        dtype: dtype.unwrap_or(DataType::Varchar),
                    }),
                    right: Box::new(BoundExpr::Literal(v)),
                    dtype: DataType::Boolean,
                });
            } else if let Some(dt) = dtype {
                let col = BoundExpr::Column {
                    index: c,
                    dtype: dt,
                };
                let mut acc: Option<BoundExpr> = None;
                for v in uniq {
                    let eq = BoundExpr::Binary {
                        op: BinaryOp::Eq,
                        left: Box::new(col.clone()),
                        right: Box::new(BoundExpr::Literal(v)),
                        dtype: DataType::Boolean,
                    };
                    acc = Some(match acc {
                        None => eq,
                        Some(prev) => BoundExpr::Binary {
                            op: BinaryOp::Or,
                            left: Box::new(prev),
                            right: Box::new(eq),
                            dtype: DataType::Boolean,
                        },
                    });
                }
                facts.push(acc.unwrap());
            }
            continue;
        }

        // Bounds: a bound every branch states holds for the union too, at the
        // weakest strength (smallest lower, largest upper). The tightest is
        // only implied by an intersection, not by an `OR`.
        for op in [BinaryOp::GtEq, BinaryOp::LtEq] {
            // `tighter(x, y)` says `x` is the tighter of the two bounds.
            let tighter = |x: &Value, y: &Value| match (value_num(x), value_num(y)) {
                (Some(a), Some(b)) => {
                    if op == BinaryOp::GtEq {
                        a > b
                    } else {
                        a < b
                    }
                }
                _ => false,
            };
            let mut best: Option<Value> = None;
            let mut every = true;
            for term in &terms {
                // Within a term the tightest bound stated is the one it implies.
                let mut bound: Option<&Value> = None;
                for atom in term {
                    if let Some(v) = range_const(atom, c, op) {
                        bound = Some(match bound {
                            Some(b) if tighter(v, b) => v,
                            Some(b) => b,
                            None => v,
                        });
                    }
                }
                let Some(v) = bound else {
                    every = false;
                    break;
                };
                // Across terms only the weakest of them is implied.
                best = Some(match &best {
                    Some(b) if tighter(v, b) => b.clone(),
                    Some(_) => v.clone(),
                    None => v.clone(),
                });
            }
            if !every {
                continue;
            }
            if let Some(v) = best {
                let dt = v.data_type().or(dtype);
                if let Some(dt) = dt {
                    facts.push(BoundExpr::Binary {
                        op,
                        left: Box::new(BoundExpr::Column {
                            index: c,
                            dtype: dt,
                        }),
                        right: Box::new(BoundExpr::Literal(v)),
                        dtype: DataType::Boolean,
                    });
                }
            }
        }
    }
    and_all(facts)
}

/// The Filter arm of [`pushdown_inner`].
///
/// `derive` is false for the one re-entry right after derived facts were
/// pushed: those facts now sit in the children, and the copy of a lone `OR`
/// that `push_one_level` leaves above the join would derive and push them
/// again on every visit (unbounded).
///
fn push_filter(
    input: LogicalPlan,
    predicate: BoundExpr,
    pdb: Option<&crate::storage::PinkDb>,
    derive: bool,
) -> LogicalPlan {
    // A left-deep comma chain joins in FROM order: tables that no equality
    // connects end up as a cross product before a later hash key can use
    // either side. Reorder the chain here, before the filter enters it, so the
    // predicate and the chain layout agree.

    let input = pushdown_inner(input, pdb);
    // Derived facts are plain conjuncts, so the usual pushdown moves them to
    // the scan that owns the column. Skip facts that are already conjuncts:
    // the derived set is a deterministic function of the conjuncts, so a
    // second visit adds nothing. A lone `OR` derives too — its facts are
    // strictly weaker — and a single-column `OR` yields the predicate itself,
    // which the equality check drops.
    let mut added = false;
    let predicate = match derive.then(|| derive_common_filters(&predicate)).flatten() {
        Some(extra) => {
            let mut have = Vec::new();
            split_conjuncts(predicate.clone(), &mut have);
            let mut add = Vec::new();
            split_conjuncts(extra, &mut add);
            add.retain(|e| {
                *e != predicate && !have.contains(e) && !have.iter().any(|h| same_value_set(h, e))
            });
            added = !add.is_empty();
            match and_all(add) {
                Some(extra) => and_all(vec![extra, predicate]).unwrap(),
                None => predicate,
            }
        }
        None => predicate,
    };
    // Literals: `WHERE false` still needs the row stream shaped correctly —
    // keep the Filter node (it yields zero rows).
    let (plan, moved) = push_one_level(input, predicate, pdb);
    if !moved {
        return plan;
    }
    match plan {
        LogicalPlan::Filter { input, predicate } => push_filter(*input, predicate, pdb, !added),
        other => pushdown_inner(other, pdb),
    }
}

/// A crossing equality usable as a hash key: one side addresses only the
/// accumulated left input, the other only the just-added right input.
fn equi_key(expr: &BoundExpr, left_cols: usize) -> Option<(BoundExpr, BoundExpr)> {
    let (l, r) = crate::sql::binder::equi_pair(expr, left_cols)?;
    // An impure expression must keep its per-row call count; a pair the
    // comparison rules reject cannot hash.
    (!expr.contains_dynamic_scalar()
        && crate::sql::binder::comparison_ok(BinaryOp::Eq, l.dtype(), r.dtype()).is_ok())
    .then_some((l, r))
}

/// Crossing equalities `expr` implies, and whether `expr` must still run above
/// the join. An `AND` implies every child's keys; an `OR` implies only the
/// keys every branch shares (the predicate guarantees them).
fn implied_equi(expr: &BoundExpr, left_cols: usize) -> (Vec<(BoundExpr, BoundExpr)>, bool) {
    match expr {
        BoundExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => {
            let (mut keys, left_keep) = implied_equi(left, left_cols);
            let (right_keys, right_keep) = implied_equi(right, left_cols);
            keys.extend(right_keys);
            (keys, left_keep && right_keep)
        }
        BoundExpr::Binary {
            op: BinaryOp::Or,
            left,
            right,
            ..
        } => {
            let (left_keys, _) = implied_equi(left, left_cols);
            let (right_keys, _) = implied_equi(right, left_cols);
            // The disjunction itself is never fully enforced by a key.
            (
                left_keys
                    .into_iter()
                    .filter(|pair| right_keys.contains(pair))
                    .collect(),
                true,
            )
        }
        _ => match equi_key(expr, left_cols) {
            Some(pair) => (vec![pair], false),
            None => (Vec::new(), true),
        },
    }
}

/// Split conjuncts into those a join may evaluate itself and those that must
/// stay in a `Filter` above it. A dynamic call is opaque and impure, and a
/// subquery is evaluated once per input row: below the join they would run a
/// different number of times (on candidate pairs rather than on rows), so both
/// kinds stay above.
fn split_residual(preds: Vec<BoundExpr>) -> (Vec<BoundExpr>, Vec<BoundExpr>) {
    preds
        .into_iter()
        .partition(|c| !c.contains_dynamic_scalar() && !c.has_subquery())
}

/// Split conjuncts into hash keys and the residual that must still run, in
/// the order the predicate listed them.
fn equi_keys(
    conjuncts: Vec<BoundExpr>,
    left_cols: usize,
) -> (Vec<BoundExpr>, Vec<BoundExpr>, Vec<BoundExpr>) {
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    let mut residual = Vec::new();
    for c in conjuncts {
        let (keys, keep) = implied_equi(&c, left_cols);
        for (l, r) in keys {
            left_keys.push(l);
            right_keys.push(r);
        }
        if keep {
            residual.push(c);
        }
    }
    (left_keys, right_keys, residual)
}

/// Flatten a condition-less comma-join chain into leaves in FROM order, with
/// each leaf's output width. Returns `None` for anything else (an `on`
/// condition or an outer join has semantics a plain reorder must not disturb).
fn comma_join_leaves(
    plan: &LogicalPlan,
    db: &crate::storage::PinkDb,
) -> Option<(Vec<LogicalPlan>, Vec<usize>)> {
    let mut chain = Vec::new();
    let mut cur = plan;
    while let LogicalPlan::NestedLoopJoin {
        kind: JoinKind::Inner | JoinKind::Cross,
        left,
        right,
        on: None,
        left_cols,
    } = cur
    {
        chain.push((right.as_ref().clone(), *left_cols));
        cur = left;
    }
    if chain.is_empty() {
        return None;
    }
    chain.reverse();
    let mut leaves = vec![cur.clone()];
    let mut widths = Vec::with_capacity(chain.len() + 1);
    let mut before = 0;
    for (right, left_cols) in chain {
        widths.push(left_cols - before);
        before = left_cols;
        leaves.push(right);
    }
    // A join records its left input's width, so the last leaf's width has to
    // come from its own schema.
    let last = crate::sql::binder::plan_schema(db, db.current_db(), &[], leaves.last()?)
        .ok()?
        .len();
    widths.push(last);
    Some((leaves, widths))
}

/// The leaf holding global column `g`, given each leaf's start and width.
fn leaf_of(g: usize, offsets: &[usize], widths: &[usize]) -> Option<usize> {
    offsets
        .iter()
        .zip(widths)
        .position(|(&off, &w)| g >= off && g < off + w)
}

/// Row-count fallbacks for the join-order cost model, used only where the
/// catalog statistics cannot answer: a literal equality keeps a fifth of the
/// rows and anything else keeps half, and an equality edge divides by the
/// larger distinct count of its two columns, which without a sketch is the
/// relation's row count (DuckDB's fallbacks — `DEFAULT_SELECTIVITY` in
/// `src/optimizer/relation_statistics/relation_statistics_helper.hpp`, and the
/// no-distinct-count path of
/// `src/optimizer/join_order/cardinality_estimator.cpp` in the DuckDB sources).
const EQ_SELECTIVITY: f64 = 0.2;
const OTHER_SELECTIVITY: f64 = 0.5;
/// A leaf whose row count cannot be read (an aggregate or a subquery) is
/// assumed small: too big to be joined first for free, far below a fact table.
const UNKNOWN_ROWS: f64 = 1_000.0;
/// Enumeration is exhaustive up to this many relations; beyond it the greedy
/// order is used instead. 2^12 subsets is a millisecond of planning.
const DP_LIMIT: usize = 12;
/// A reorder costs one projection that restores the outer column order, so it
/// is only taken for a clear win. It also makes the pass terminate: an order
/// that is already the cheapest maps to itself and is kept.
const REORDER_GAIN: f64 = 0.9;

/// Estimated rows of a chain leaf from its own table, before the filter's
/// conjuncts. Table row counts are exact; a shape that only narrows or
/// renames keeps its input's estimate.
fn leaf_base_rows(db: &crate::storage::PinkDb, leaf: &LogicalPlan) -> Option<f64> {
    match leaf {
        LogicalPlan::Scan {
            db: qdb,
            table,
            at: None,
        } => {
            let name = qdb.clone().unwrap_or_else(|| db.current_db().to_string());
            if let Some((attached, _)) = db.attached_lookup(&name, table) {
                return Some(attached.rows as f64);
            }
            db.table_in(&name, table).ok().map(|t| t.num_rows() as f64)
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => leaf_base_rows(db, input),
        _ => None,
    }
}

/// Catalog statistics of a leaf whose columns keep their base table's
/// numbering (a scan, possibly under a filter/limit/sort). A projection
/// renumbers, and an attached lake table has per-file statistics instead of a
/// catalog row, so both answer `None` and keep the fallbacks.
pub(crate) fn leaf_stats<'a>(
    db: &'a crate::storage::PinkDb,
    leaf: &LogicalPlan,
) -> Option<&'a crate::stats::TableStats> {
    match leaf {
        LogicalPlan::Scan {
            db: qdb,
            table,
            at: None,
        } => {
            let name = qdb.clone().unwrap_or_else(|| db.current_db().to_string());
            db.table_in(&name, table).ok().map(|t| t.stats())
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => leaf_stats(db, input),
        _ => None,
    }
}

/// The catalog's exact distinct estimate for one column of a leaf plan, if it
/// has one. Used to size a join's bloom filter (wave 122).
pub(crate) fn estimate_distinct(
    db: &crate::storage::PinkDb,
    plan: &LogicalPlan,
    col: usize,
) -> Option<u64> {
    leaf_stats(db, plan)?.distinct(col)
}

/// A value on the number line, for the uniform range assumption. `None` for
/// kinds without a meaningful distance (strings, intervals, variants).
fn numeric(v: &Value) -> Option<f64> {
    match v {
        Value::Integer(i) => Some(f64::from(*i)),
        Value::Bigint(i) => Some(*i as f64),
        Value::Double(d) if d.is_finite() => Some(*d),
        Value::Date(d) => Some(f64::from(*d)),
        Value::Timestamp(t) => Some(*t as f64),
        Value::Decimal(v, scale) => Some(*v as f64 / 10f64.powi(i32::from(*scale))),
        _ => None,
    }
}

/// The comparison a one-column conjunct states: the operator, oriented so the
/// column is on the left, and the literal.
fn column_comparison(atom: &BoundExpr, col: usize) -> Option<(BinaryOp, Value)> {
    let BoundExpr::Binary {
        op, left, right, ..
    } = atom
    else {
        return None;
    };
    let op = match op {
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => *op,
        _ => return None,
    };
    let is_col = |e: &BoundExpr| matches!(e, BoundExpr::Column { index, .. } if *index == col);
    let literal = |e: &BoundExpr| match e {
        BoundExpr::Literal(v) => Some(v.clone()),
        BoundExpr::Cast { expr, .. } => match &**expr {
            BoundExpr::Literal(v) => Some(v.clone()),
            _ => None,
        },
        _ => None,
    };
    match (literal(left), literal(right)) {
        (Some(v), None) if is_col(right) => {
            let flipped = match op {
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::LtEq => BinaryOp::GtEq,
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::GtEq => BinaryOp::LtEq,
                other => other,
            };
            Some((flipped, v))
        }
        (None, Some(v)) if is_col(left) => Some((op, v)),
        _ => None,
    }
}

/// Selectivity of one conjunct from catalog statistics: equality from the
/// distinct estimate, a range from the uniform spread between the column's
/// min and max (`src/optimizer/statistics_propagator.cpp` in the DuckDB
/// sources), and `IS [NOT] NULL` from the null count. `None` when the
/// statistics cannot answer, so the caller keeps its fallback.
fn stats_selectivity(
    stats: &crate::stats::TableStats,
    rows: f64,
    global_col: usize,
    local_col: usize,
    c: &BoundExpr,
) -> Option<f64> {
    let st = stats.col(local_col)?;
    if rows < 1.0 {
        return None;
    }
    if let BoundExpr::IsNull { expr, negated } = c
        && matches!(**expr, BoundExpr::Column { index, .. } if index == global_col)
    {
        let nulls = st.null_count as f64 / rows;
        return Some(if *negated { 1.0 - nulls } else { nulls });
    }
    if let Some(values) = eq_set(c, local_col)
        && !values.is_empty()
        && values.len() <= 64
    {
        let distinct = stats.distinct(local_col)? as f64;
        return Some(if distinct < 1.0 {
            0.0
        } else {
            (values.len() as f64 / distinct).min(1.0)
        });
    }
    let (op, literal) = column_comparison(c, global_col)?;
    if matches!(literal, Value::Null) {
        return Some(0.0);
    }
    if op == BinaryOp::NotEq {
        let distinct = stats.distinct(local_col)? as f64;
        return Some(if distinct < 1.0 {
            0.0
        } else {
            1.0 - (1.0 / distinct).min(1.0)
        });
    }
    let (lo, hi) = (numeric(st.min.as_ref()?)?, numeric(st.max.as_ref()?)?);
    let v = numeric(&literal)?;
    if hi <= lo {
        // One distinct value (or a constant column): the comparison is
        // either always true or always false.
        return Some(if v >= lo { 1.0 } else { 0.0 });
    }
    let sel = match op {
        BinaryOp::Lt | BinaryOp::LtEq => (v - lo) / (hi - lo),
        BinaryOp::Gt | BinaryOp::GtEq => (hi - v) / (hi - lo),
        _ => return None,
    };
    Some(sel.clamp(0.0, 1.0))
}

/// Selectivity of one conjunct: statistics when the conjunct pins exactly one
/// column of one leaf, else the fixed defaults. A literal equality keeps a
/// fifth, and an equality list (`IN`) keeps a fifth per member.
fn conjunct_selectivity(
    stats: Option<&crate::stats::TableStats>,
    rows: f64,
    local_col: Option<usize>,
    c: &BoundExpr,
) -> f64 {
    let mut cols = Vec::new();
    columns_in(c, &mut cols);
    cols.sort_unstable();
    cols.dedup();
    if cols.len() == 1 {
        if let (Some(stats), Some(local)) = (stats, local_col)
            && let Some(sel) = stats_selectivity(stats, rows, cols[0], local, c)
        {
            return sel;
        }
        if let Some(values) = eq_set(c, cols[0])
            && !values.is_empty()
        {
            return (EQ_SELECTIVITY * values.len() as f64).min(1.0);
        }
    }
    OTHER_SELECTIVITY
}

/// Selectivity of a filter predicate over `input`, from catalog statistics
/// (wave 122). `1.0` when the plan shape or the statistics cannot answer.
pub(crate) fn filter_selectivity(
    db: &crate::storage::PinkDb,
    input: &LogicalPlan,
    predicate: &BoundExpr,
) -> f64 {
    let Some(stats) = leaf_stats(db, input) else {
        return 1.0;
    };
    let rows = leaf_base_rows(db, input).unwrap_or(stats.rows as f64);
    fn walk(stats: &crate::stats::TableStats, rows: f64, e: &BoundExpr) -> f64 {
        if let BoundExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } = e
        {
            return walk(stats, rows, left) * walk(stats, rows, right);
        }
        let mut cols = Vec::new();
        columns_in(e, &mut cols);
        cols.sort_unstable();
        cols.dedup();
        match cols.as_slice() {
            [col] => stats_selectivity(stats, rows, *col, *col, e).unwrap_or(OTHER_SELECTIVITY),
            // A conjunct over no column at all (a constant, a scalar
            // subquery) keeps every row as far as the statistics know; only a
            // conjunct over *several* columns is genuinely unknown.
            [] => 1.0,
            _ => OTHER_SELECTIVITY,
        }
    }
    walk(stats, rows, predicate)
}

/// Exact distinct count of one column, for the equality-class domain. An
/// approximate sketch (a delete ran since the last `ANALYZE`) is left out:
/// the caller's row-count default is a safe upper bound, a dirty lower one is
/// not.
fn column_domain(
    db: &crate::storage::PinkDb,
    leaf: &LogicalPlan,
    local_col: usize,
    base_rows: f64,
) -> f64 {
    match leaf_stats(db, leaf).and_then(|s| s.distinct(local_col)) {
        Some(d) => (d as f64).min(base_rows),
        None => base_rows,
    }
}

/// Equality column pairs a conjunct implies: an `AND` contributes all of
/// them, an `OR` only the pairs every branch shares (the same rule pushdown
/// uses to pick hash keys).
fn implied_pairs(e: &BoundExpr, out: &mut Vec<(usize, usize)>) {
    match e {
        BoundExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => {
            implied_pairs(left, out);
            implied_pairs(right, out);
        }
        BoundExpr::Binary {
            op: BinaryOp::Or,
            left,
            right,
            ..
        } => {
            let mut l = Vec::new();
            let mut r = Vec::new();
            implied_pairs(left, &mut l);
            implied_pairs(right, &mut r);
            out.extend(l.into_iter().filter(|p| r.contains(p)));
        }
        BoundExpr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
            ..
        } => {
            let mut l = Vec::new();
            let mut r = Vec::new();
            columns_in(left, &mut l);
            columns_in(right, &mut r);
            l.sort_unstable();
            l.dedup();
            r.sort_unstable();
            r.dedup();
            if l.len() == 1 && r.len() == 1 && l[0] != r[0] {
                out.push((l[0], r[0]));
            }
        }
        _ => {}
    }
}

/// Cardinality inputs of a comma chain: each leaf's unfiltered row count, the
/// product of the conjuncts that filter only that leaf, and the join edges
/// with the size of the domain they join on.
struct ChainStats {
    bases: Vec<f64>,
    sels: Vec<f64>,
    denoms: Vec<((usize, usize), f64)>,
    edges: Vec<(usize, usize)>,
}

impl ChainStats {
    /// Whether leaf `i` has an equality to a leaf already in `mask`.
    fn links(&self, i: usize, mask: usize) -> bool {
        self.edges
            .iter()
            .any(|&(a, b)| (a == i && mask >> b & 1 == 1) || (b == i && mask >> a & 1 == 1))
    }

    /// Rows a single leaf yields after its own filters.
    fn rows(&self, i: usize) -> f64 {
        (self.bases[i] * self.sels[i]).max(1.0)
    }

    /// Estimated rows of joining the subset `mask` of the chain: the product
    /// of its inputs, divided by the domain size of every equality that holds
    /// inside it. Without column statistics the domain of an equality class is
    /// taken to be the smallest table stating it, which is the dimension side
    /// of a foreign key join; that keeps `lineitem ⋈ supplier` at the 600k rows
    /// it really has (a division by the fact side would fold it to 10k and make
    /// every order look equally cheap), while two dimensions joined to each
    /// other on a shared key still multiply as they should.
    fn card(&self, mask: usize) -> f64 {
        let mut card = 1.0;
        let mut members = 0;
        for i in 0..self.bases.len() {
            if mask & (1 << i) != 0 {
                card *= self.rows(i);
                members += 1;
            }
        }
        if members == 0 {
            return 0.0;
        }
        for &((a, b), denom) in &self.denoms {
            if mask & (1 << a) != 0 && mask & (1 << b) != 0 {
                card /= denom;
            }
        }
        card.max(1.0)
    }
}

/// Root of a column's equality class, compressing the path.
fn class_root(parent: &mut std::collections::HashMap<usize, usize>, x: usize) -> usize {
    let mut root = x;
    loop {
        match parent.get(&root) {
            Some(&p) if p != root => root = p,
            _ => break,
        }
    }
    let mut at = x;
    while at != root {
        let next = *parent.get(&at).expect("a walked column has a parent");
        parent.insert(at, root);
        at = next;
    }
    parent.insert(root, root);
    root
}

/// Estimated work of a join order: the sum of the intermediate cardinalities,
/// like DuckDB's `ComputeCost` (`src/optimizer/join_order/cost_model.cpp`).
fn order_cost(stats: &ChainStats, order: &[usize]) -> f64 {
    let mut mask = 0usize;
    let mut cost = 0.0;
    for &i in order {
        mask |= 1 << i;
        cost += stats.card(mask);
    }
    cost
}

/// Cheapest left-deep order by dynamic programming over relation subsets. A
/// chain is joined left to right, so a left-deep order is the whole space; a
/// disconnected subset is allowed and prices its cross product.
fn dp_order(stats: &ChainStats) -> Option<Vec<usize>> {
    let n = stats.bases.len();
    let full = (1usize << n) - 1;
    let mut cost = vec![f64::INFINITY; 1 << n];
    let mut last = vec![0usize; 1 << n];
    // Anchor the FROM clause's first relation. It is the one a query filters,
    // and without statistics a small dimension looks cheaper to start from even
    // when joining it first fans out through a many-to-many key and leaves the
    // fact table for last (TPC-H Q5 region-first: 315 ms against 49). With at
    // most three joins the estimate cannot compound far, so any leaf may start
    // there (TPC-H Q11: partsupp-first builds an 800k-row intermediate,
    // supplier-first 400).
    if n <= 4 {
        for i in 0..n {
            cost[1 << i] = stats.rows(i);
            last[1 << i] = i;
        }
    } else {
        cost[1] = stats.rows(0);
        last[1] = 0;
    }
    for mask in 1..=full {
        if mask.count_ones() < 2 {
            continue;
        }
        let card = stats.card(mask);
        for i in 0..n {
            if mask & (1 << i) == 0 {
                continue;
            }
            let rest = mask & !(1 << i);
            // A cross product is never the cheaper hash join: grow the plan
            // only along an equality, the way the from-clause author meant.
            if !stats.links(i, rest) {
                continue;
            }
            let c = cost[rest] + card;
            if c < cost[mask] {
                cost[mask] = c;
                last[mask] = i;
            }
        }
    }
    if !cost[full].is_finite() {
        // A table no equality reaches has no ordered place.
        return None;
    }
    let mut order = Vec::with_capacity(n);
    let mut mask = full;
    while mask != 0 {
        let i = last[mask];
        order.push(i);
        mask &= !(1 << i);
    }
    order.reverse();
    Some(order)
}

/// The same cost model greedily, for chains too wide to enumerate.
fn greedy_order(stats: &ChainStats) -> Option<Vec<usize>> {
    let n = stats.bases.len();
    let mut order = vec![0];
    let mut mask = 1usize;
    while order.len() < n {
        let mut pick = None;
        let mut pick_card = f64::INFINITY;
        for i in 0..n {
            if mask & (1 << i) != 0 || !stats.links(i, mask) {
                continue;
            }
            let card = stats.card(mask | (1 << i));
            if card < pick_card {
                pick_card = card;
                pick = Some(i);
            }
        }
        // A table no equality reaches has no ordered place: keep the FROM
        // order rather than moving a cross product.
        let pick = pick?;
        mask |= 1 << pick;
        order.push(pick);
    }
    Some(order)
}

/// Cost-based order for a comma chain, or `None` to keep the FROM order.
fn cost_join_order(
    db: &crate::storage::PinkDb,
    leaves: &[LogicalPlan],
    widths: &[usize],
    conjuncts: &[BoundExpr],
) -> Option<Vec<usize>> {
    let n = leaves.len();
    if n < 3 {
        return None;
    }
    let mut offsets = Vec::with_capacity(n);
    let mut acc = 0;
    for w in widths {
        offsets.push(acc);
        acc += w;
    }
    let mut bases = Vec::with_capacity(n);
    for leaf in leaves {
        bases.push(leaf_base_rows(db, leaf).unwrap_or(UNKNOWN_ROWS));
    }
    let mut sels = vec![1.0; n];
    let mut joins = Vec::new();
    let mut pairs = Vec::new();

    for c in conjuncts {
        let mut cols = Vec::new();
        columns_in(c, &mut cols);
        let mut touched = Vec::new();
        for &g in &cols {
            if let Some(i) = leaf_of(g, &offsets, widths)
                && !touched.contains(&i)
            {
                touched.push(i);
            }
        }
        if touched.len() == 1 {
            let leaf = touched[0];
            // A conjunct that pins several columns is not a single-column
            // restriction on this leaf (the fallbacks apply).
            let local = (cols.len() == 1)
                .then(|| cols[0].checked_sub(offsets[leaf]))
                .flatten();
            let stats = leaf_stats(db, &leaves[leaf]);
            sels[leaf] *= conjunct_selectivity(stats, bases[leaf], local, c);
        }
        pairs.clear();
        implied_pairs(c, &mut pairs);
        joins.append(&mut pairs);
    }
    // Every column an equality touches shares one domain. Its size is the
    // smallest table stating it: the dimension side of a foreign key join.
    let mut parent = std::collections::HashMap::new();
    for &(a, b) in &joins {
        let (ra, rb) = (class_root(&mut parent, a), class_root(&mut parent, b));
        if ra != rb {
            parent.insert(ra, rb);
        }
    }
    let mut ndv: std::collections::HashMap<usize, f64> = std::collections::HashMap::new();
    let columns: Vec<usize> = parent.keys().copied().collect();
    for col in columns {
        let root = class_root(&mut parent, col);
        let Some(leaf) = leaf_of(col, &offsets, widths) else {
            continue;
        };
        let local = col.checked_sub(offsets[leaf]);
        let domain = match local {
            Some(local) => column_domain(db, &leaves[leaf], local, bases[leaf]),
            None => bases[leaf],
        };
        let entry = ndv.entry(root).or_insert(f64::INFINITY);
        *entry = entry.min(domain);
    }
    // One denominator per pair of leaves, so a composite key join divides once.
    let mut denoms: std::collections::HashMap<(usize, usize), f64> =
        std::collections::HashMap::new();
    for &(a, b) in &joins {
        let (Some(x), Some(y)) = (leaf_of(a, &offsets, widths), leaf_of(b, &offsets, widths))
        else {
            continue;
        };
        if x == y {
            continue;
        }
        let root = class_root(&mut parent, a);
        let Some(&domain) = ndv.get(&root) else {
            continue;
        };
        let entry = denoms.entry((x.min(y), x.max(y))).or_insert(1.0);
        *entry = entry.max(domain);
    }
    let mut edges = Vec::new();
    for &(a, b) in &joins {
        if let (Some(x), Some(y)) = (leaf_of(a, &offsets, widths), leaf_of(b, &offsets, widths))
            && x != y
        {
            edges.push((x, y));
        }
    }
    let stats = ChainStats {
        bases,
        sels,
        denoms: denoms.into_iter().collect(),
        edges,
    };
    let order = if n <= DP_LIMIT {
        dp_order(&stats)?
    } else {
        greedy_order(&stats)?
    };
    let from: Vec<usize> = (0..n).collect();
    let chosen = order_cost(&stats, &order);
    (chosen < order_cost(&stats, &from) * REORDER_GAIN).then_some(order)
}

/// Decide the order of every comma-join chain while its from-tree is whole.
///
/// This runs before pushdown: `push_one_level` pushes side-only conjuncts onto
/// the chain that remains, so a filter pushdown revisits later covers a strict
/// sub-chain, and an order chosen there has no estimate for the leaves joined
/// above it. In TPC-H Q5 the domain of `nation` disappears that way, making
/// `customer ⋈ supplier` on `nationkey` — 600k rows — look cheaper than
/// `customer ⋈ orders`.
fn order_chains(db: &crate::storage::PinkDb, plan: LogicalPlan) -> LogicalPlan {
    use LogicalPlan as P;
    match plan {
        P::Filter { input, predicate } => {
            match reorder_comma_join(&predicate, &input, Some(db), true) {
                Some(reordered) => reordered,
                None => {
                    // Decorrelation puts the chain under a left join, out of
                    // reach of the filter above it: order the left spine.
                    let input = reorder_left_spine(db, *input, &predicate);
                    P::Filter {
                        input: Box::new(order_chains(db, input)),
                        predicate,
                    }
                }
            }
        }
        P::Project {
            input,
            exprs,
            names,
        } => P::Project {
            input: Box::new(order_chains(db, *input)),
            exprs,
            names,
        },
        P::Aggregate { input, keys, aggs } => P::Aggregate {
            input: Box::new(order_chains(db, *input)),
            keys,
            aggs,
        },
        P::Distinct { input, on } => P::Distinct {
            input: Box::new(order_chains(db, *input)),
            on,
        },
        P::Sort { input, keys } => P::Sort {
            input: Box::new(order_chains(db, *input)),
            keys,
        },
        P::Limit { input, limit } => P::Limit {
            input: Box::new(order_chains(db, *input)),
            limit,
        },
        P::Offset { input, offset } => P::Offset {
            input: Box::new(order_chains(db, *input)),
            offset,
        },
        P::Sample { input, sample } => P::Sample {
            input: Box::new(order_chains(db, *input)),
            sample,
        },
        P::Window {
            input,
            windows,
            input_cols,
        } => P::Window {
            input: Box::new(order_chains(db, *input)),
            windows,
            input_cols,
        },
        P::SetOp {
            op,
            left,
            right,
            types,
        } => P::SetOp {
            op,
            left: Box::new(order_chains(db, *left)),
            right: Box::new(order_chains(db, *right)),
            types,
        },
        P::Join {
            kind,
            left,
            right,
            left_keys,
            right_keys,
            left_cols,
            residual,
        } => P::Join {
            kind,
            left: Box::new(order_chains(db, *left)),
            right: Box::new(order_chains(db, *right)),
            left_keys,
            right_keys,
            left_cols,
            residual,
        },
        P::NestedLoopJoin {
            kind,
            left,
            right,
            on,
            left_cols,
        } => P::NestedLoopJoin {
            kind,
            left: Box::new(order_chains(db, *left)),
            right: Box::new(order_chains(db, *right)),
            on,
            left_cols,
        },
        // A CTE body and an EXPLAINed plan are optimized on their own.
        P::Cte {
            name,
            query,
            body,
            recursive,
        } => {
            let query = Box::new(order_chains(db, *query));
            let body = Box::new(order_chains(db, *body));
            let recursive = recursive.map(|s| Box::new(s.map(&|plan| order_chains(db, plan))));
            P::Cte {
                name,
                query,
                body,
                recursive,
            }
        }
        P::Explain { plan } => P::Explain {
            plan: Box::new(order_chains(db, *plan)),
        },
        other => other,
    }
}

/// Order the chain under a join's left input — at any depth of the left
/// spine: decorrelation puts the chain there, out of reach of the filter
/// above the join. Column indices of the
/// left input are unchanged and its projection restores the order the join's
/// keys and the parent predicate expect, so this only changes the join order.
fn reorder_left_spine(
    db: &crate::storage::PinkDb,
    plan: LogicalPlan,
    predicate: &BoundExpr,
) -> LogicalPlan {
    use LogicalPlan as P;
    match plan {
        P::Join {
            kind,
            left,
            right,
            left_keys,
            right_keys,
            left_cols,
            residual,
        } => {
            let left = match reorder_comma_join(predicate, &left, Some(db), false) {
                Some(reordered) => reordered,
                // Nested decorrelation (TPC-H Q21's `EXISTS` and `NOT
                // EXISTS` give an anti join over a semi join over the chain):
                // keep walking the left spine. Each level keeps its left
                // input's column indices, so the parent's keys still match.
                None => reorder_left_spine(db, *left, predicate),
            };
            P::Join {
                kind,
                left: Box::new(left),
                right,
                left_keys,
                right_keys,
                left_cols,
                residual,
            }
        }
        P::NestedLoopJoin {
            kind,
            left,
            right,
            on: Some(on),
            left_cols,
        } => {
            let left = match reorder_comma_join(predicate, &left, Some(db), false) {
                Some(reordered) => reordered,
                None => reorder_left_spine(db, *left, predicate),
            };
            P::NestedLoopJoin {
                kind,
                left: Box::new(left),
                right,
                on: Some(on),
                left_cols,
            }
        }
        other => other,
    }
}

/// Reorder a comma-join chain so equality-connected tables join first, and
/// rewrite the predicate's column indices to match. Only the shape changes:
/// every leaf and every conjunct is kept, so results are identical.
/// A predicate that reads a subquery or an outer column: never chain-local.
fn reads_subquery(e: &BoundExpr) -> bool {
    let mut found = false;
    crate::sql::binder::walk_bound_expr(e, &mut |e| {
        found |= matches!(
            e,
            BoundExpr::OuterRef { .. }
                | BoundExpr::ScalarSubquery { .. }
                | BoundExpr::Exists { .. }
                | BoundExpr::InSubquery { .. }
        );
    });
    found
}

fn has_outer_ref(e: &BoundExpr) -> bool {
    let mut found = false;
    crate::sql::binder::walk_bound_expr(e, &mut |e| {
        found |= matches!(e, BoundExpr::OuterRef { .. });
    });
    found
}

fn reorder_comma_join(
    pred: &BoundExpr,
    plan: &LogicalPlan,
    db: Option<&crate::storage::PinkDb>,
    keep_filter: bool,
) -> Option<LogicalPlan> {
    let db = db?;
    // A predicate holding an outer row column belongs to a subquery body: the
    // binder unnest the correlation by pattern-matching that body's from-tree,
    // and the projection this reorder puts above it hides the tree.
    if has_outer_ref(pred) {
        return None;
    }
    let (leaves, widths) = comma_join_leaves(plan, db)?;
    let mut conjuncts = Vec::new();
    split_conjuncts(pred.clone(), &mut conjuncts);
    let mut offsets = Vec::with_capacity(leaves.len());
    let mut acc = 0;
    for w in &widths {
        offsets.push(acc);
        acc += w;
    }
    // Column equalities are transitive: `p = l AND ps = l` also joins `p`
    // with `ps` (TPC-H Q9), so the order may join those two directly.
    let classes = equality_classes(&conjuncts, &offsets, &widths);
    let mut priced = conjuncts.clone();
    for class in &classes {
        for (k, a) in class.iter().enumerate() {
            for b in &class[k + 1..] {
                if leaf_of(a.0, &offsets, &widths) != leaf_of(b.0, &offsets, &widths) {
                    priced.push(column_eq(*a, *b));
                }
            }
        }
    }
    let order = cost_join_order(db, &leaves, &widths, &priced)?;
    // Each class then joins every member to the class's first column in the
    // new order: one key per join, whichever members it brings.
    if !classes.is_empty() {
        let position = |g: usize| {
            let leaf = leaf_of(g, &offsets, &widths).unwrap_or(usize::MAX);
            order.iter().position(|&l| l == leaf).unwrap_or(usize::MAX)
        };
        conjuncts.retain(|c| {
            simple_column_eq(c).is_none_or(|(a, b)| {
                !classes.iter().any(|class| {
                    class.iter().any(|m| m.0 == a.0) && class.iter().any(|m| m.0 == b.0)
                })
            })
        });
        for class in &classes {
            let mut members = class.clone();
            members.sort_by_key(|m| (position(m.0), m.0));
            for m in &members[1..] {
                conjuncts.push(column_eq(members[0], *m));
            }
        }
    }
    let mut new_offsets = vec![0; leaves.len()];
    let mut acc = 0;
    for (pos, &leaf) in order.iter().enumerate() {
        new_offsets[leaf] = acc;
        if pos + 1 < order.len() {
            acc += widths[leaf];
        }
    }
    let remap = |g: usize| match leaf_of(g, &offsets, &widths) {
        Some(i) => new_offsets[i] + (g - offsets[i]),
        None => g,
    };
    // A conjunct reading a column outside the chain cannot move under the
    // restore projection, whose input is the chain alone (a decorrelated
    // subquery's output column lives above it): keep such conjuncts above it,
    // where the original column order still holds.
    let (inside, outside): (Vec<_>, Vec<_>) = conjuncts.into_iter().partition(|c| {
        let mut cols = Vec::new();
        columns_in(c, &mut cols);
        cols.iter().all(|g| *g < acc) && !reads_subquery(c)
    });
    let above = and_all(outside);
    // The parent still addresses the chain's original column order, so put a
    // transparent projection on top. Its width is the chain's width.
    let schema = crate::sql::binder::plan_schema(db, db.current_db(), &[], plan).ok()?;
    let mut exprs = Vec::with_capacity(schema.len());
    let mut names = Vec::with_capacity(schema.len());
    for (g, (name, dtype)) in schema.iter().enumerate() {
        exprs.push(BoundExpr::Column {
            index: remap(g),
            dtype: *dtype,
        });
        names.push(name.clone());
    }
    let mut plan = leaves[order[0]].clone();
    let mut left_width = widths[order[0]];
    for &leaf in &order[1..] {
        plan = LogicalPlan::NestedLoopJoin {
            kind: JoinKind::Cross,
            left: Box::new(plan),
            right: Box::new(leaves[leaf].clone()),
            on: None,
            left_cols: left_width,
        };
        if widths[leaf] != usize::MAX {
            left_width += widths[leaf];
        }
    }
    let plan = match and_all(inside).map(|p| map_columns(p, &remap)) {
        Some(predicate) if keep_filter => LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        },
        _ => plan,
    };
    let plan = LogicalPlan::Project {
        input: Box::new(plan),
        exprs,
        names,
    };
    Some(match above {
        Some(predicate) if keep_filter => LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        },
        _ => plan,
    })
}

/// A conjunct `column = column` of one type, as `(index, dtype)` pairs.
fn simple_column_eq(c: &BoundExpr) -> Option<((usize, DataType), (usize, DataType))> {
    let BoundExpr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = c
    else {
        return None;
    };
    match (&**left, &**right) {
        (BoundExpr::Column { index: a, dtype: x }, BoundExpr::Column { index: b, dtype: y })
            if x == y && a != b =>
        {
            Some(((*a, *x), (*b, *y)))
        }
        _ => None,
    }
}

/// `a = b` over two columns of one type.
fn column_eq(a: (usize, DataType), b: (usize, DataType)) -> BoundExpr {
    BoundExpr::Binary {
        op: BinaryOp::Eq,
        left: Box::new(BoundExpr::Column {
            index: a.0,
            dtype: a.1,
        }),
        right: Box::new(BoundExpr::Column {
            index: b.0,
            dtype: b.1,
        }),
        dtype: DataType::Boolean,
    }
}

/// The equality classes of a chain's `column = column` conjuncts that span
/// at least three columns (two are already one direct equality), members
/// in column order. Only same-type equalities of chain columns join a
/// class, so every pair in it compares exactly as the written ones do; and
/// `a = b AND b = c` keeps the same rows as `a = b AND a = c` — a NULL
/// member fails some equality either way (DuckDB's equivalence sets in
/// `src/optimizer/join_order/query_graph_manager.cpp` infer these edges).
fn equality_classes(
    conjuncts: &[BoundExpr],
    offsets: &[usize],
    widths: &[usize],
) -> Vec<Vec<(usize, DataType)>> {
    let mut parent: std::collections::HashMap<usize, usize> = std::collections::HashMap::new();
    let mut types: std::collections::HashMap<usize, DataType> = std::collections::HashMap::new();
    for c in conjuncts {
        let Some((a, b)) = simple_column_eq(c) else {
            continue;
        };
        if leaf_of(a.0, offsets, widths).is_none() || leaf_of(b.0, offsets, widths).is_none() {
            continue;
        }
        types.insert(a.0, a.1);
        types.insert(b.0, b.1);
        let (ra, rb) = (class_root(&mut parent, a.0), class_root(&mut parent, b.0));
        if ra != rb {
            parent.insert(ra, rb);
        }
    }
    let mut groups: std::collections::BTreeMap<usize, Vec<(usize, DataType)>> =
        std::collections::BTreeMap::new();
    let mut columns: Vec<usize> = types.keys().copied().collect();
    columns.sort_unstable();
    for col in columns {
        let root = class_root(&mut parent, col);
        groups.entry(root).or_default().push((col, types[&col]));
    }
    groups
        .into_values()
        .filter(|members| members.len() >= 3 && members.iter().all(|m| m.1 == members[0].1))
        .collect()
}

/// Remap a predicate through a transparent projection: every `Column{i}`
/// must address a bare `Column`/`AggRef` passthrough (same scope below).
fn rewrite_through_project(pred: &BoundExpr, exprs: &[BoundExpr]) -> Option<BoundExpr> {
    match pred {
        BoundExpr::Column { index, .. } => match exprs.get(*index)? {
            BoundExpr::Column { .. } | BoundExpr::AggRef { .. } => Some(exprs[*index].clone()),
            _ => None,
        },
        // An outer ref means the same thing in any plan of this block, so it
        // survives the projection rewrite unchanged.
        BoundExpr::Literal(_)
        | BoundExpr::OuterRef { .. }
        | BoundExpr::AggRef { .. }
        | BoundExpr::WindowRef { .. } => Some(pred.clone()),
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
            outer,
            op,
        } => Some(BoundExpr::InSubquery {
            expr: Box::new(rewrite_through_project(expr, exprs)?),
            query: query.clone(),
            negated: *negated,
            outer: outer.clone(),
            op: *op,
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
        BoundExpr::DynamicScalar {
            name,
            args,
            dtype,
            reads,
        } => Some(BoundExpr::DynamicScalar {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_through_project(a, exprs))
                .collect::<Option<Vec<_>>>()?
                .into_boxed_slice(),
            dtype: *dtype,
            reads: reads.clone(),
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
        BoundExpr::Literal(_)
        | BoundExpr::OuterRef { .. }
        | BoundExpr::AggRef { .. }
        | BoundExpr::WindowRef { .. } => Some(pred.clone()),
        BoundExpr::ScalarSubquery { .. } | BoundExpr::Exists { .. } => Some(pred.clone()),
        BoundExpr::InSubquery {
            expr,
            query,
            negated,
            outer,
            op,
        } => Some(BoundExpr::InSubquery {
            expr: Box::new(rewrite_through_keys(expr, keys)?),
            query: query.clone(),
            negated: *negated,
            outer: outer.clone(),
            op: *op,
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
        BoundExpr::DynamicScalar {
            name,
            args,
            dtype,
            reads,
        } => Some(BoundExpr::DynamicScalar {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_through_keys(a, keys))
                .collect::<Option<Vec<_>>>()?
                .into_boxed_slice(),
            dtype: *dtype,
            reads: reads.clone(),
        }),
    }
}

/// Push a filter one level down. Returns `(plan, moved)`. When no push
/// applies, the filter stays above the input — never dropped.
fn push_one_level(
    input: LogicalPlan,
    pred: BoundExpr,
    pdb: Option<&crate::storage::PinkDb>,
) -> (LogicalPlan, bool) {
    // A correlated subquery reads the row of the block it was bound in, so a
    // conjunct holding one cannot cross a node that renumbers or narrows that
    // row.
    if crate::plan::has_correlated_subquery(&pred) {
        return (
            LogicalPlan::Filter {
                input: Box::new(input),
                predicate: pred,
            },
            false,
        );
    }
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
                    input: Box::new(pushdown_inner(*inner, pdb)),
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
        LogicalPlan::Distinct { input: inner, on } => {
            // Pushing the filter below a DISTINCT evaluates the predicate on
            // every input row instead of once per distinct row: an impure
            // dynamic call would change its call count, so it stays above.
            if pred.contains_dynamic_scalar() || on.is_some() {
                return (
                    LogicalPlan::Filter {
                        input: Box::new(LogicalPlan::Distinct { input: inner, on }),
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
                    on,
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
            residual,
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
                    // FULL: both sides can be NULL-extended, so nothing
                    // moves (conservative; see the join pushdown tests).
                    JoinKind::Full | JoinKind::Mark => keep.push(c),
                    // Semi/anti (wave 120) emit exactly their left columns, so
                    // a conjunct over the left side decides the same rows
                    // below the join. A mark join appends the marker column
                    // after them, so only semi/anti qualify.
                    JoinKind::Semi | JoinKind::Anti => {
                        if !cols.is_empty() && left_only {
                            left_push.push(c);
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
                left_keys,
                right_keys,
                left_cols,
                residual,
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
        // A comma join (`FROM a, b`) binds as a condition-less cross nested
        // loop, so its equality predicates arrive as a Filter above. Turn
        // crossing equalities into hash keys — the nested loop would build
        // the full |a| × |b| product, which is quadratic even for tiny
        // tables. Only a condition-less inner/cross join qualifies: with an
        // `on` or an outer kind, moving a conjunct changes semantics.
        LogicalPlan::NestedLoopJoin {
            kind,
            left,
            right,
            on,
            left_cols,
        } if on.is_none() && matches!(kind, JoinKind::Inner | JoinKind::Cross) => {
            let mut conjuncts = Vec::new();
            split_conjuncts(pred.clone(), &mut conjuncts);
            let (left_keys, right_keys, keep) = equi_keys(conjuncts, left_cols);
            // A conjunct that only reads one side still filters that side
            // first, so the cross product is over surviving rows only.
            let (mut left_only, mut right_only, mut above) = (Vec::new(), Vec::new(), Vec::new());
            for c in keep {
                let mut cols = Vec::new();
                columns_in(&c, &mut cols);
                if !cols.is_empty() && cols.iter().all(|i| *i < left_cols) {
                    left_only.push(c);
                } else if !cols.is_empty() && cols.iter().all(|i| *i >= left_cols) {
                    right_only.push(shift_columns(c, left_cols));
                } else {
                    above.push(c);
                }
            }
            let pushed = !left_keys.is_empty() || !left_only.is_empty() || !right_only.is_empty();
            let report = |plan: LogicalPlan, preds: Vec<BoundExpr>| match and_all(preds) {
                Some(predicate) => LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate,
                },
                None => plan,
            };
            let left = report(pushdown_inner(*left, pdb), left_only);
            let right = report(pushdown_inner(*right, pdb), right_only);
            if left_keys.is_empty() {
                let plan = LogicalPlan::NestedLoopJoin {
                    kind,
                    left: Box::new(left),
                    right: Box::new(right),
                    on,
                    left_cols,
                };
                (
                    match and_all(above) {
                        Some(predicate) => LogicalPlan::Filter {
                            input: Box::new(plan),
                            predicate,
                        },
                        None => plan,
                    },
                    pushed,
                )
            } else {
                let (residual, keep) = split_residual(above);
                let join = LogicalPlan::Join {
                    kind: JoinKind::Inner,
                    left: Box::new(left),
                    right: Box::new(right),
                    left_keys,
                    right_keys,
                    left_cols,
                    residual: and_all(residual),
                };
                (
                    match and_all(keep) {
                        Some(predicate) => LogicalPlan::Filter {
                            input: Box::new(join),
                            predicate,
                        },
                        None => join,
                    },
                    true,
                )
            }
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
                    left: Box::new(pushdown_inner(*left, pdb)),
                    right: Box::new(pushdown_inner(*right, pdb)),
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

fn pushdown_inner(plan: LogicalPlan, pdb: Option<&crate::storage::PinkDb>) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => push_filter(*input, predicate, pdb, true),
        LogicalPlan::Project {
            input,
            exprs,
            names,
        } => LogicalPlan::Project {
            input: Box::new(pushdown_inner(*input, pdb)),
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
            residual,
        } => LogicalPlan::Join {
            kind,
            left: Box::new(pushdown_inner(*left, pdb)),
            right: Box::new(pushdown_inner(*right, pdb)),
            left_keys,
            right_keys,
            left_cols,
            residual,
        },
        LogicalPlan::NestedLoopJoin {
            kind,
            left,
            right,
            on,
            left_cols,
        } => {
            let left = pushdown_inner(*left, pdb);
            let right = pushdown_inner(*right, pdb);
            // Same rewrite as above for a condition on the join itself: an
            // equality part hashes, and an outer join's residual may move into
            // the side it references (a filter above the join would drop the
            // NULL-extended rows the outer join must preserve).
            if let Some(on) = &on {
                let mut conjuncts = Vec::new();
                split_conjuncts(on.clone(), &mut conjuncts);
                let (left_keys, right_keys, residual) = equi_keys(conjuncts, left_cols);
                if !left_keys.is_empty() {
                    let residual_side = residual.iter().try_fold(
                        (Vec::new(), Vec::new()),
                        |(mut left_only, mut right_only), c| {
                            let mut cols = Vec::new();
                            columns_in(c, &mut cols);
                            // A constant predicate is not tied to either side;
                            // below an outer join it would change which rows
                            // are preserved, so it keeps the nested loop.
                            if cols.is_empty() || cols.contains(&usize::MAX) {
                                return None;
                            }
                            let all_left = cols.iter().all(|i| *i < left_cols);
                            let all_right = cols.iter().all(|i| *i >= left_cols);
                            // Only the *null-supplying* side's conditions may
                            // move into that side: a LEFT JOIN's right input, a
                            // RIGHT JOIN's left input. A condition on the
                            // preserved side decides which preserved rows find a
                            // match, not which rows survive, so filtering that
                            // input would drop rows the join must NULL-extend
                            // (`a LEFT JOIN b ON a.k = b.k AND a.x > 5` keeps
                            // every row of `a`). Those, mixed and FULL residuals
                            // keep the nested loop.
                            match kind {
                                JoinKind::Inner => {}
                                JoinKind::Left if all_right => {
                                    right_only.push(shift_columns(c.clone(), left_cols))
                                }
                                JoinKind::Right if all_left => left_only.push(c.clone()),
                                _ => return None,
                            }
                            Some((left_only, right_only))
                        },
                    );
                    if let Some((left_only, right_only)) = residual_side {
                        let with_filter =
                            |plan: LogicalPlan, preds: Vec<BoundExpr>| match and_all(preds) {
                                Some(predicate) => LogicalPlan::Filter {
                                    input: Box::new(plan),
                                    predicate,
                                },
                                None => plan,
                            };
                        let left = with_filter(left, left_only);
                        let right = with_filter(right, right_only);
                        // An inner join's remaining `ON` conjuncts decide
                        // whether a candidate pair is a match, so the join
                        // evaluates them itself. An outer join only gets here
                        // when every conjunct was side-only and is now a filter
                        // on the null-supplying side, so it carries none.
                        let residual = match kind {
                            JoinKind::Inner => {
                                let (residual, keep) = split_residual(residual);
                                return with_filter(
                                    LogicalPlan::Join {
                                        kind,
                                        left: Box::new(left),
                                        right: Box::new(right),
                                        left_keys,
                                        right_keys,
                                        left_cols,
                                        residual: and_all(residual),
                                    },
                                    keep,
                                );
                            }
                            _ => None,
                        };
                        return LogicalPlan::Join {
                            kind,
                            left: Box::new(left),
                            right: Box::new(right),
                            left_keys,
                            right_keys,
                            left_cols,
                            residual,
                        };
                    }
                }
            }
            LogicalPlan::NestedLoopJoin {
                kind,
                left: Box::new(left),
                right: Box::new(right),
                on,
                left_cols,
            }
        }
        LogicalPlan::Aggregate { input, keys, aggs } => LogicalPlan::Aggregate {
            input: Box::new(pushdown_inner(*input, pdb)),
            keys,
            aggs,
        },
        LogicalPlan::Window {
            input,
            windows,
            input_cols,
        } => LogicalPlan::Window {
            input: Box::new(pushdown_inner(*input, pdb)),
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
            left: Box::new(pushdown_inner(*left, pdb)),
            right: Box::new(pushdown_inner(*right, pdb)),
            types,
        },
        LogicalPlan::InsertSelect {
            db,
            table,
            columns,
            query,
            conflict,
            returning,
        } => LogicalPlan::InsertSelect {
            db,
            table,
            columns,
            query: Box::new(pushdown_inner(*query, pdb)),
            conflict,
            returning,
        },
        LogicalPlan::CreateTableAs {
            db,
            name,
            query,
            if_not_exists,
            or_replace,
        } => LogicalPlan::CreateTableAs {
            db,
            name,
            query: Box::new(pushdown_inner(*query, pdb)),
            if_not_exists,
            or_replace,
        },
        LogicalPlan::Explain { plan } => LogicalPlan::Explain {
            plan: Box::new(pushdown_inner(*plan, pdb)),
        },
        // Filters push within the body and within the query independently —
        // never across the CTE boundary (different scopes).
        LogicalPlan::Cte {
            name,
            query,
            body,
            recursive,
        } => LogicalPlan::Cte {
            name,
            query: Box::new(pushdown_inner(*query, pdb)),
            body: Box::new(pushdown_inner(*body, pdb)),
            recursive: recursive.map(|r| Box::new(r.map(&|p| pushdown_inner(p, pdb)))),
        },
        LogicalPlan::Distinct { input, on } => LogicalPlan::Distinct {
            input: Box::new(pushdown_inner(*input, pdb)),
            on,
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(pushdown_inner(*input, pdb)),
            keys,
        },
        LogicalPlan::Limit { input, limit } => LogicalPlan::Limit {
            input: Box::new(pushdown_inner(*input, pdb)),
            limit,
        },
        LogicalPlan::Offset { input, offset } => LogicalPlan::Offset {
            input: Box::new(pushdown_inner(*input, pdb)),
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
        if let Live::Cols(m) = self
            && let Some(b) = m.get_mut(i)
        {
            *b = true;
        }
    }

    /// Mark every column an expression references (AggRef/WindowRef sentinels
    /// are not columns and are skipped).
    fn mark_expr(&mut self, e: &BoundExpr) {
        let mut cols = Vec::new();
        columns_in(e, &mut cols);
        cols.extend(crate::plan::correlated_columns(e));
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
        | LogicalPlan::Distinct { input, .. }
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
        | LogicalPlan::Distinct { input, .. }
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
                // Semi/anti/mark never NULL-extend the left side, and it is
                // the only side they emit.
                JoinKind::Semi | JoinKind::Anti | JoinKind::Mark => false,
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
            residual,
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
            // A residual reads both sides in joined space; keeping both sides
            // fully live costs at most the columns this join would have
            // pruned.
            if residual.is_some() {
                left_live = Live::All;
                right_live = Live::All;
            }
            for e in &left_keys {
                left_live.mark_expr(e);
            }
            for e in &right_keys {
                right_live.mark_expr(e);
            }
            // A residual can drop pairs, so the join is not an identity on the
            // child side.
            if kind == JoinKind::Inner
                && residual.is_none()
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
                residual,
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
        LogicalPlan::Distinct { input, on } => {
            let inner = pass_live(db, &input, live);
            LogicalPlan::Distinct {
                input: Box::new(elim(db, *input, &inner)),
                on,
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
                        for e in a.arg.iter().chain(a.filter.iter()) {
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
        LogicalPlan::Cte {
            name,
            query,
            body,
            recursive,
        } => LogicalPlan::Cte {
            name,
            query: Box::new(elim(db, *query, &Live::All)),
            body: Box::new(elim(db, *body, live)),
            recursive: recursive.map(|r| Box::new(r.map(&|p| elim(db, p, &Live::All)))),
        },
        LogicalPlan::InsertSelect {
            db: qdb,
            table,
            columns,
            query,
            conflict,
            returning,
        } => LogicalPlan::InsertSelect {
            db: qdb,
            table,
            columns,
            query: Box::new(elim(db, *query, &Live::All)),
            conflict,
            returning,
        },
        LogicalPlan::CreateTableAs {
            db: qdb,
            name,
            query,
            if_not_exists,
            or_replace,
        } => LogicalPlan::CreateTableAs {
            db: qdb,
            name,
            query: Box::new(elim(db, *query, &Live::All)),
            if_not_exists,
            or_replace,
        },
        LogicalPlan::Explain { plan } => LogicalPlan::Explain {
            plan: Box::new(elim(db, *plan, &Live::All)),
        },
        // Leaves (scans, DML without a query child, DDL, ...) carry no joins.
        other => other,
    }
}

/// Rewrite `ORDER BY <metric-distance>(vec, <constant>) LIMIT k` onto a
/// vector index (wave 107b). The rewrite is deliberately narrow: a single
/// ascending sort key, a projection directly over a native table scan, a
/// folded literal query vector of the index's width, and the index's own
/// metric. A `WHERE` clause puts a `Filter` between the scan and the
/// projection, so filtered top-k falls back to the exact path rather than
/// silently returning fewer than `k` rows.
fn apply_vector_topk(db: &crate::storage::PinkDb, plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Explain { plan } => LogicalPlan::Explain {
            plan: Box::new(apply_vector_topk(db, *plan)),
        },
        LogicalPlan::Limit { input, limit } => {
            let Some(new_input) = rewrite_limit_input(db, (*input).clone(), limit) else {
                return LogicalPlan::Limit { input, limit };
            };
            LogicalPlan::Limit {
                input: Box::new(new_input),
                limit,
            }
        }
        other => other,
    }
}

/// `LIMIT` input is `Sort(Project(Scan))`, optionally under the binder's
/// identity projection that hides `ORDER BY` expressions.
fn rewrite_limit_input(
    db: &crate::storage::PinkDb,
    input: LogicalPlan,
    limit: usize,
) -> Option<LogicalPlan> {
    if limit == 0 {
        return None;
    }
    match input {
        LogicalPlan::Sort { keys, input } => rewrite_sort(db, keys, *input, limit),
        LogicalPlan::Project {
            input,
            exprs,
            names,
        } => match *input {
            LogicalPlan::Sort { keys, input: inner } => Some(LogicalPlan::Project {
                input: Box::new(rewrite_sort(db, keys, *inner, limit)?),
                exprs,
                names,
            }),
            other => {
                let _ = other;
                None
            }
        },
        _ => None,
    }
}

fn rewrite_sort(
    db: &crate::storage::PinkDb,
    keys: Vec<(BoundExpr, crate::sql::ast::SortSpec)>,
    input: LogicalPlan,
    limit: usize,
) -> Option<LogicalPlan> {
    // Exactly one ascending key: any other key changes the order the index
    // cannot produce.
    let [(key, spec)] = keys.as_slice() else {
        return None;
    };
    if spec.desc || spec.nulls_first {
        return None;
    }
    let LogicalPlan::Project {
        input: scan,
        exprs,
        names,
    } = input
    else {
        return None;
    };
    let LogicalPlan::Scan {
        db: qdb,
        table,
        at: None,
    } = *scan
    else {
        return None;
    };
    let expr = match key {
        BoundExpr::Column { index, .. } => exprs.get(*index)?,
        other => other,
    };
    let BoundExpr::Scalar { func, args, .. } = expr else {
        return None;
    };
    let want_metric = match func {
        crate::sql::binder::ScalarFunc::ArrayDistance => crate::distance::Metric::L2Sq,
        crate::sql::binder::ScalarFunc::ArrayCosineDistance => crate::distance::Metric::Cosine,
        crate::sql::binder::ScalarFunc::ArrayNegativeInnerProduct => {
            crate::distance::Metric::InnerProduct
        }
        _ => return None,
    };
    let [left, right] = args.as_slice() else {
        return None;
    };
    let (col_idx, query) = match (left, right) {
        (BoundExpr::Column { index, .. }, BoundExpr::Literal(v)) => (*index, v),
        (BoundExpr::Literal(v), BoundExpr::Column { index, .. }) => (*index, v),
        _ => return None,
    };
    let lookup = qdb.as_deref().unwrap_or_else(|| db.current_db());
    let t = db.table_in(lookup, &table).ok()?;
    let col = t.columns.get(col_idx)?;
    let dtype @ crate::types::DataType::Array { len, .. } = col.dtype else {
        return None;
    };
    if !matches!(
        dtype,
        crate::types::DataType::Array {
            elem: crate::types::ArrayElem::Float | crate::types::ArrayElem::Double,
            ..
        }
    ) {
        return None;
    }
    let def = t.vector_indexes.iter().find(|d| {
        d.column == col_idx
            && d.index.is_some()
            && d.options
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("metric"))
                .and_then(|(_, v)| crate::distance::Metric::parse(v))
                .unwrap_or(crate::distance::Metric::L2Sq)
                == want_metric
    })?;
    let crate::types::Value::Array(a) = query else {
        return None;
    };
    if a.len() != len as usize {
        return None;
    }
    if !matches!(
        a.values,
        crate::array::ArrayElems::Floats32(_) | crate::array::ArrayElems::Floats(_)
    ) {
        return None;
    }
    Some(LogicalPlan::Project {
        input: Box::new(LogicalPlan::VectorTopK {
            db: qdb,
            table: table.clone(),
            column: col_idx,
            column_name: col.name.clone(),
            index: def.name.clone(),
            query: query.clone(),
            metric: want_metric,
            dim: len as usize,
            k: limit,
        }),
        exprs,
        names,
    })
}
#[cfg(test)]
mod tests {
    fn pushdown(plan: LogicalPlan) -> LogicalPlan {
        pushdown_inner(plan, None)
    }

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

    fn lit(i: i32) -> BoundExpr {
        B::Literal(Value::Integer(i))
    }

    fn cmp(op: BinaryOp, left: BoundExpr, right: BoundExpr) -> BoundExpr {
        B::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
            dtype: T::Boolean,
        }
    }

    fn or(left: BoundExpr, right: BoundExpr) -> BoundExpr {
        cmp(BinaryOp::Or, left, right)
    }

    fn facts(pred: &BoundExpr) -> Vec<BoundExpr> {
        let mut out = Vec::new();
        split_conjuncts(derive_common_filters(pred).expect("a derivation"), &mut out);
        out
    }

    /// Across `OR` branches a bound is only implied by the *union*: `x >= 1 OR
    /// x >= 10` still allows 1. Keeping the tighter bound drops rows.
    #[test]
    fn or_derives_the_loosest_bound() {
        let lower = or(
            cmp(BinaryOp::GtEq, col(0), lit(1)),
            cmp(BinaryOp::GtEq, col(0), lit(10)),
        );
        assert_eq!(facts(&lower), vec![cmp(BinaryOp::GtEq, col(0), lit(1))]);
        let upper = or(
            cmp(BinaryOp::LtEq, col(0), lit(1)),
            cmp(BinaryOp::LtEq, col(0), lit(10)),
        );
        assert_eq!(facts(&upper), vec![cmp(BinaryOp::LtEq, col(0), lit(10))]);
    }

    /// The binder expands `IN` to an `OR` chain that starts with a false
    /// literal; the chain inside a branch still pins its column, so the
    /// values of every branch can be unioned.
    #[test]
    fn an_in_list_inside_an_or_branch_contributes_its_values() {
        let list = |c: usize, vs: [i32; 2]| {
            or(
                or(
                    B::Literal(Value::Boolean(false)),
                    cmp(BinaryOp::Eq, col(c), lit(vs[0])),
                ),
                cmp(BinaryOp::Eq, col(c), lit(vs[1])),
            )
        };
        let pred = or(
            and(cmp(BinaryOp::Eq, col(0), lit(1)), list(1, [1, 2])),
            and(cmp(BinaryOp::Eq, col(0), lit(2)), list(1, [2, 3])),
        );
        let got = facts(&pred);
        // `c` is pinned to 1 or 2, `d` to 1, 2 or 3.
        assert!(
            got.contains(&or(
                cmp(BinaryOp::Eq, col(0), lit(1)),
                cmp(BinaryOp::Eq, col(0), lit(2))
            )),
            "{got:?}"
        );
        assert!(
            got.contains(&or(
                or(
                    cmp(BinaryOp::Eq, col(1), lit(1)),
                    cmp(BinaryOp::Eq, col(1), lit(2))
                ),
                cmp(BinaryOp::Eq, col(1), lit(3)),
            )),
            "{got:?}"
        );
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
                residual: None,
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
    fn comma_join_equalities_become_hash_keys() {
        // Cross nested loop with an equality predicate above it: the pair
        // becomes hash keys instead of every left row meeting every right
        // row. `col(2)` is the right side's first column and must shift to 0.
        let eq = B::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col(0)),
            right: Box::new(col(2)),
            dtype: T::Boolean,
        };
        let cross = LogicalPlan::NestedLoopJoin {
            kind: JoinKind::Cross,
            left: Box::new(LogicalPlan::SingleRow),
            right: Box::new(LogicalPlan::SingleRow),
            on: None,
            left_cols: 2,
        };
        let out = pushdown(LogicalPlan::Filter {
            input: Box::new(cross.clone()),
            predicate: eq.clone(),
        });
        match out {
            LogicalPlan::Join {
                kind,
                left_keys,
                right_keys,
                ..
            } => {
                assert_eq!(kind, JoinKind::Inner);
                assert_eq!(left_keys, vec![col(0)]);
                assert_eq!(right_keys, vec![col(0)]);
            }
            other => panic!("comma join stayed nested: {other:?}"),
        }

        // A mixed-side residual cannot hash, but it is a predicate on a matched
        // pair: the join carries it and drops the pairs that fail it.
        let residual = B::Binary {
            op: BinaryOp::Gt,
            left: Box::new(col(1)),
            right: Box::new(col(3)),
            dtype: T::Boolean,
        };
        let pred = B::Binary {
            op: BinaryOp::And,
            left: Box::new(eq),
            right: Box::new(residual.clone()),
            dtype: T::Boolean,
        };
        let out = pushdown(LogicalPlan::Filter {
            input: Box::new(cross),
            predicate: pred,
        });
        match out {
            LogicalPlan::Join {
                kind,
                left_keys,
                right_keys,
                residual: carried,
                ..
            } => {
                assert_eq!(kind, JoinKind::Inner);
                assert_eq!(left_keys, vec![col(0)]);
                assert_eq!(right_keys, vec![col(0)]);
                assert_eq!(carried, Some(residual));
            }
            other => panic!("comma join residual did not ride the join: {other:?}"),
        }
    }

    #[test]
    fn or_of_conjunctions_derives_per_column_filters() {
        // (c0 = 1 AND c2 = 7) OR (c0 = 2 AND c2 = 8) implies c0 IN (1, 2)
        // and c2 IN (7, 8). The disjunction is kept (facts are weaker).
        let eq = |c: usize, v: i32| B::Binary {
            op: BinaryOp::Eq,
            left: Box::new(col(c)),
            right: Box::new(B::Literal(Value::Integer(v))),
            dtype: T::Boolean,
        };
        let and = |a: B, b: B| B::Binary {
            op: BinaryOp::And,
            left: Box::new(a),
            right: Box::new(b),
            dtype: T::Boolean,
        };
        let or = |a: B, b: B| B::Binary {
            op: BinaryOp::Or,
            left: Box::new(a),
            right: Box::new(b),
            dtype: T::Boolean,
        };
        let pred = or(and(eq(0, 1), eq(2, 7)), and(eq(0, 2), eq(2, 8)));
        let facts = derive_common_filters(&pred).expect("facts every branch agrees on");
        let mut conjuncts = Vec::new();
        split_conjuncts(facts, &mut conjuncts);
        assert_eq!(conjuncts.len(), 2, "{conjuncts:?}");
        let mut lits = Vec::new();
        for fact in &conjuncts {
            let mut pairs = Vec::new();
            collect_eq_literals(fact, &mut pairs);
            pairs.sort_unstable();
            lits.push(pairs);
        }
        assert_eq!(lits, vec![vec![(0, 1), (0, 2)], vec![(2, 7), (2, 8)]]);
    }

    /// Equality column/literal pairs under an OR chain, for the derivation test.
    fn collect_eq_literals(e: &B, out: &mut Vec<(usize, i32)>) {
        match e {
            B::Binary {
                op: BinaryOp::Or,
                left,
                right,
                ..
            } => {
                collect_eq_literals(left, out);
                collect_eq_literals(right, out);
            }
            B::Binary {
                op: BinaryOp::Eq,
                left,
                right,
                ..
            } => {
                if let (B::Column { index, .. }, B::Literal(Value::Integer(v))) =
                    (&**left, &**right)
                {
                    out.push((*index, *v));
                }
            }
            other => panic!("unexpected fact shape: {other:?}"),
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
                residual: None,
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
            residual: None,
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
            filter: None,
            direct: None,
            separator: None,
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
            reads: Box::default(),
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
