//! Vectorized expression evaluation: [`BoundExpr`] × [`DataChunk`] → [`Vector`].
//!
//! Semantics:
//! - NULL propagates through arithmetic, comparisons, NOT, and scalar
//!   functions (SQL three-valued logic).
//! - `AND`/`OR` use Kleene logic; `WHERE` keeps only TRUE rows.
//! - Numeric widening (INTEGER → BIGINT → DOUBLE); `int / int` → DOUBLE.
//! - Integer division/modulo by zero is an error; float follows IEEE 754.

use std::cmp::Ordering;

use pinkdb_ext_loader::ExtArg;

use super::aggregate::encode_row;
use super::{Cx, Memo, eval};
use crate::array::ArrayElems;
use crate::plan::{compare_values, num_cmp};
use crate::sql::ast::BinaryOp;
use crate::sql::binder::{BoundExpr, OuterBind, ScalarFunc, common_numeric};
use crate::types::FastSet;
use crate::types::{
    DataType, POW10_I128, PinkDbError, Result, Value, decimal_fits, format_decimal, rescale_decimal,
};
use crate::vector::{DataChunk, Vector, VectorData};

/// Evaluate `expr` over every row of `chunk`.
pub(crate) fn eval_expr(expr: &BoundExpr, chunk: &DataChunk, cx: &Cx) -> Result<Vector> {
    let n = chunk.num_rows();
    match expr {
        BoundExpr::Literal(v) => const_vector(v.clone(), n),
        // Column reads go through the presence check: an attached-lake scan
        // may not have decoded a column the projection analysis said nothing
        // reads, and that must fail loudly, never read as NULLs.
        BoundExpr::Column { index, .. } => Ok(chunk.expect_column(*index)?.as_ref().clone()),
        BoundExpr::AggRef { index, .. } => Ok(chunk.expect_column(*index)?.as_ref().clone()),
        BoundExpr::WindowRef { index, .. } => Ok(chunk.expect_column(*index)?.as_ref().clone()),
        BoundExpr::ScalarSubquery {
            query,
            dtype,
            outer,
        } => eval_scalar_subquery(query, *dtype, outer, chunk, cx),
        // A correlated body reads the enclosing block's row from the context
        // its own evaluation was given (see `eval_correlated`).
        BoundExpr::OuterRef {
            depth,
            index,
            dtype,
        } => {
            let level = cx.outer_rows.len().checked_sub(*depth + 1).ok_or_else(|| {
                PinkDbError::Execution(format!(
                    "correlated column reference at depth {depth} has no enclosing row"
                ))
            })?;
            match cx.outer_rows[level].get(*index) {
                Some(Value::Null) | None => Ok(null_vector(*dtype, n)),
                Some(v) => const_vector(v.clone(), n),
            }
        }
        BoundExpr::Cast { expr, dtype } => {
            let v = eval_expr(expr, chunk, cx)?;
            eval_cast(&v, *dtype)
        }
        BoundExpr::Coalesce { args, dtype } => eval_coalesce(args, *dtype, chunk, cx),
        BoundExpr::Case {
            branches,
            else_,
            dtype,
        } => eval_case(branches, else_, *dtype, chunk, cx),
        BoundExpr::InSubquery {
            expr,
            query,
            negated,
            outer,
            op,
        } => eval_in_subquery(expr, query, *negated, *op, outer, chunk, cx),
        BoundExpr::Exists {
            query,
            negated,
            outer,
        } => eval_exists(query, *negated, outer, chunk, cx),
        BoundExpr::Not(e) => {
            let v = eval_expr(e, chunk, cx)?;
            ensure_dtype(&v, DataType::Boolean, "NOT")?;
            let bits = match v.data() {
                VectorData::Boolean(b) => b,
                _ => unreachable!("BOOLEAN dtype implies BOOLEAN layout"),
            };
            let mut out = Vector::new_empty(DataType::Boolean);
            for (i, &b) in bits.iter().enumerate() {
                if v.is_null(i) {
                    out.push_null()?;
                } else {
                    out.push_bool(!b)?;
                }
            }
            Ok(out)
        }
        BoundExpr::Negate { expr, dtype } => {
            let v = eval_expr(expr, chunk, cx)?;
            let mut out = Vector::new_empty(*dtype);
            match (v.data(), *dtype) {
                // Typed checked loops; DATE/TIMESTAMP stay unsupported below.
                (VectorData::Int32(src), DataType::Integer) => {
                    for (i, &x) in src.iter().enumerate() {
                        if v.is_null(i) {
                            out.push_null()?;
                        } else {
                            out.push_int32(x.checked_neg().ok_or_else(|| {
                                overflow_err("negation", "INTEGER", format_args!("-({x})"))
                            })?)?;
                        }
                    }
                }
                (VectorData::Int64(src), DataType::Bigint) => {
                    for (i, &x) in src.iter().enumerate() {
                        if v.is_null(i) {
                            out.push_null()?;
                        } else {
                            out.push_int64(x.checked_neg().ok_or_else(|| {
                                overflow_err("negation", "BIGINT", format_args!("-({x})"))
                            })?)?;
                        }
                    }
                }
                (VectorData::Float64(src), DataType::Double) => {
                    for (i, &x) in src.iter().enumerate() {
                        if v.is_null(i) {
                            out.push_null()?;
                        } else {
                            out.push_float64(-x)?;
                        }
                    }
                }
                (VectorData::Decimal { values, scale, .. }, DataType::Decimal { .. }) => {
                    for (i, &x) in values.iter().enumerate() {
                        if v.is_null(i) {
                            out.push_null()?;
                        } else {
                            out.push_decimal(
                                x.checked_neg().ok_or_else(|| {
                                    overflow_err("negation", "DECIMAL", format_args!("-({x})"))
                                })?,
                                *scale,
                            )?;
                        }
                    }
                }
                (VectorData::Interval(src), DataType::Interval) => {
                    for (i, iv) in src.iter().enumerate() {
                        if v.is_null(i) {
                            out.push_null()?;
                        } else {
                            out.push_interval(crate::types::interval_negate(*iv))?;
                        }
                    }
                }
                _ => {
                    // Mixed dtype-vs-layout: keep the old per-row error path.
                    for i in 0..v.len() {
                        if v.is_null(i) {
                            out.push_null()?;
                        } else {
                            out.push_valid(negate_value(v.get(i), *dtype)?)?;
                        }
                    }
                }
            }
            Ok(out)
        }
        BoundExpr::IsNull { expr, negated } => {
            let v = eval_expr(expr, chunk, cx)?;
            let mut out = Vector::new_empty(DataType::Boolean);
            for i in 0..v.len() {
                // Pure validity-bit read: no `Value` materialization.
                out.push_bool(v.is_null(i) != *negated)?;
            }
            Ok(out)
        }
        BoundExpr::Binary {
            op,
            left,
            right,
            dtype,
        } => match op {
            BinaryOp::And | BinaryOp::Or => eval_logic(*op, left, right, chunk, cx),
            BinaryOp::Like | BinaryOp::NotLike => eval_like(*op, left, right, chunk, cx),
            BinaryOp::Concat => eval_concat(left, right, chunk, cx),
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq => eval_comparison(*op, left, right, chunk, cx),
            _ => eval_arith(*op, *dtype, left, right, chunk, cx),
        },
        BoundExpr::Scalar {
            func: ScalarFunc::Uuid,
            ..
        } => random_uuids(chunk.num_rows()),
        BoundExpr::Scalar { func, args, dtype } => {
            let mut vecs = Vec::with_capacity(args.len());
            for a in args {
                vecs.push(eval_expr(a, chunk, cx)?);
            }
            eval_scalar(*func, *dtype, &vecs)
        }
        BoundExpr::DynamicScalar {
            name, args, dtype, ..
        } => eval_dynamic_scalar(name, args, *dtype, chunk, cx),
    }
}

/// Row-at-a-time callback into a scalar function registered by a loaded
/// extension, whichever loading model registered it.
///
/// Arguments evaluate vector-at-a-time over the chunk (exactly like a static
/// scalar function); the callback then runs once per row with the ABI's
/// argument values. NULL cells are passed through as an [`ExtArg::Null`] of
/// the declared type — the extension decides what NULL means (documented in
/// `pinkdb_extension.h`). VARCHAR arguments borrow their bytes for the call;
/// a VARCHAR result is owned by the time it is converted back (the native
/// loader copied it out of extension memory and freed the buffer; a linked
/// callback returned `Vec<u8>`).
fn eval_dynamic_scalar(
    name: &str,
    args: &[BoundExpr],
    dtype: DataType,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    // A package's host scalar (wave 108a) reads the store per row; its
    // arguments are already resolved to the declared parameter order.
    if let Some(host) = cx.db.host_scalar(name) {
        let mut vecs = Vec::with_capacity(args.len());
        for a in args {
            vecs.push(eval_expr(a, chunk, cx)?);
        }
        let external = host.is_external_io();
        // A replay serves every `external_io` result from the WAL record's
        // memo: the provider is not reachable (and may have no key), and the
        // replayed vectors must equal the ones the original statement wrote.
        // A miss is a hard error, never a fresh call.
        if external && let Some(memo) = cx.db.ext_replay() {
            let mut out = Vector::new_empty(dtype);
            for row in 0..chunk.num_rows() {
                let row_values: Vec<crate::types::Value> =
                    vecs.iter().map(|v| v.get(row)).collect();
                match memo.get(name, &row_values) {
                    Some(result) => out.push_valid(result.clone())?,
                    None => {
                        return Err(PinkDbError::Execution(format!(
                            "WAL replay has no recorded result for {name}() (row {row}): the \
                             record's external-call memo does not cover this call"
                        )));
                    }
                }
            }
            return Ok(out);
        }
        // The authority for the session's external-services policy is the
        // call site, not the binder: a statement in a batch can bind only
        // after an earlier one created what it reads.
        if external && !cx.budget.external_services() {
            return Err(PinkDbError::Permission(
                crate::package::SERVICES_HINT.to_string(),
            ));
        }
        let collect = external && cx.budget.collects_external_calls();
        // A batched scalar gets the whole chunk, so one call can deduplicate
        // and batch its work (and an external round trip is not paid per
        // row). The contract is the declared type and one result per row.
        if let Some(batch) = host.batch() {
            let out = batch(cx.db, &vecs)?;
            if out.len() != chunk.num_rows() {
                return Err(PinkDbError::Execution(format!(
                    "host function {name}() returned {} rows for a chunk of {}",
                    out.len(),
                    chunk.num_rows()
                )));
            }
            if out.dtype() != dtype {
                return Err(PinkDbError::Execution(format!(
                    "host function {name}() returned {} but declares {dtype}",
                    out.dtype()
                )));
            }
            if collect {
                for row in 0..chunk.num_rows() {
                    let row_values: Vec<crate::types::Value> =
                        vecs.iter().map(|v| v.get(row)).collect();
                    cx.budget
                        .record_external_call(name, row_values, out.get(row));
                }
            }
            return Ok(out);
        }
        let mut out = Vector::new_empty(dtype);
        let mut row_values: Vec<crate::types::Value> = Vec::with_capacity(vecs.len());
        for row in 0..chunk.num_rows() {
            row_values.clear();
            for v in &vecs {
                row_values.push(v.get(row));
            }
            let value = host.call(cx.db, &row_values)?;
            if collect {
                cx.budget
                    .record_external_call(name, row_values.clone(), value.clone());
            }
            out.push_valid(value)?;
        }
        return Ok(out);
    }
    let function = cx.db.dynamic_scalar(name).ok_or_else(|| {
        PinkDbError::Catalog(format!(
            "function \"{name}\" is not registered; load its extension again"
        ))
    })?;
    let mut vecs = Vec::with_capacity(args.len());
    for a in args {
        vecs.push(eval_expr(a, chunk, cx)?);
    }
    let mut out = Vector::new_empty(dtype);
    // VARIANT arguments cross as encoded bytes, borrowed for the call, so the
    // row's payloads are encoded into an arena first: an `ExtArg` cannot own
    // them (it is `Copy` and points into the caller's borrow). The arena and
    // the argument list live for one row, because the list borrows the arena.
    let has_variant = vecs
        .iter()
        .any(|v| matches!(v.data(), crate::vector::VectorData::Variant(_)));
    for row in 0..chunk.num_rows() {
        let variant_args: Vec<Option<Vec<u8>>> = if has_variant {
            vecs.iter()
                .map(|v| crate::ext::variant_arg_bytes(v, row))
                .collect()
        } else {
            Vec::new()
        };
        let mut ext_args: Vec<ExtArg<'_>> = Vec::with_capacity(vecs.len());
        for (i, v) in vecs.iter().enumerate() {
            // Positional declared type; a variadic function repeats the last
            // one for arguments past its declared list. The binder checked the
            // arity, so a missing type here is an internal invariant failure,
            // never an out-of-bounds index (`None` is the 0-argument case).
            let Some(arg_type) = function.arg_type_at(i) else {
                return Err(PinkDbError::Execution(format!(
                    "function \"{name}\" declares no argument {i}, but the call supplied one"
                )));
            };
            let encoded = variant_args.get(i).and_then(|bytes| bytes.as_deref());
            crate::ext::push_ext_arg(&mut ext_args, v, row, arg_type, encoded)?;
        }
        let value = function
            .call(&ext_args)
            .map_err(|e| PinkDbError::Execution(e.to_string()))?;
        out.push_valid(crate::ext::ext_value_to_value(name, dtype, value)?)?;
    }
    Ok(out)
}

/// Evaluate every uncorrelated subquery of `exprs` once, on this thread.
///
/// A subquery body runs its own plan on the same worker pool. An operator must
/// therefore fill the memo for the subqueries its chunks share *before* it fans
/// them out: a worker that waits on a lock another worker holds, while that
/// worker waits for a pool job, deadlocks — the per-key flight lock wave 111ab
/// shipped did exactly that wherever the subquery was not in a filter. After
/// this call `Cx::memoized` only reads; its miss path computes without waiting,
/// so it is a fallback, never a rendezvous.
pub(crate) fn prime<'e>(cx: &Cx, exprs: impl IntoIterator<Item = &'e BoundExpr>) -> Result<()> {
    for expr in exprs {
        let mut out = Ok(());
        crate::sql::binder::walk_bound_expr(expr, &mut |e| {
            if out.is_err() {
                return;
            }
            out = prime_one(cx, e);
        });
        out?;
    }
    Ok(())
}

/// Fill the memo for one expression node, if it is an uncorrelated subquery.
/// A correlated body depends on the outer row, so it is never memoised and is
/// left to [`eval_correlated`].
fn prime_one(cx: &Cx, e: &BoundExpr) -> Result<()> {
    match e {
        BoundExpr::ScalarSubquery { query, outer, .. } if outer.refs.is_empty() => {
            prime_memo(cx, memo_key(query), || {
                Ok(Memo::Scalar(scalar_value(query, cx)?))
            })
        }
        BoundExpr::Exists { query, outer, .. } if outer.refs.is_empty() => {
            prime_memo(cx, memo_key(query), || {
                Ok(Memo::Flag(exists_body(query, cx)?))
            })
        }
        BoundExpr::InSubquery {
            expr,
            query,
            op,
            outer,
            ..
        } if outer.refs.is_empty() => {
            let key = memo_key(query);
            if *op == BinaryOp::Eq {
                prime_memo(cx, key, || {
                    let free = cx.order_free(query);
                    let out = eval(free.as_ref().unwrap_or(cx), query)?;
                    let (set, has_null, common) = in_set(&out, expr.dtype())?;
                    Ok(Memo::Set(std::sync::Arc::new(set), has_null, common))
                })
            } else {
                prime_memo(cx, key, || {
                    let out = eval(cx, query)?;
                    let (lo, hi, has_null) = extremes(&out)?;
                    Ok(Memo::Extremes(lo, hi, has_null))
                })
            }
        }
        _ => Ok(()),
    }
}

fn prime_memo(cx: &Cx, key: usize, f: impl FnOnce() -> Result<Memo>) -> Result<()> {
    if cx.memo.lock().expect("memo").contains_key(&key) {
        return Ok(());
    }
    let m = f()?;
    cx.memo.lock().expect("memo").entry(key).or_insert(m);
    Ok(())
}

fn memo_key(query: &crate::plan::LogicalPlan) -> usize {
    (query as *const crate::plan::LogicalPlan) as usize
}

/// Scalar subquery: 0 rows → NULL, 1 row → broadcast, else error.
/// Memoised per plan node: uncorrelated ⇒ stable within a statement.
fn eval_scalar_subquery(
    query: &crate::plan::LogicalPlan,
    dtype: DataType,
    outer: &OuterBind,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    if outer.refs.is_empty() {
        let key = memo_key(query);
        let v = cx.memoized(key, || Ok(Memo::Scalar(scalar_value(query, cx)?)))?;
        let Memo::Scalar(v) = v else {
            unreachable!("a memo key names one subquery kind")
        };
        return const_vector(v, chunk.num_rows());
    }
    eval_correlated(outer, dtype, chunk, cx, |cx, chunk| {
        let value = scalar_value(query, cx)?;
        if matches!(value, Value::Null) {
            // Zero rows: keep the column's type, so the enclosing expression's
            // arithmetic and comparison stay typed.
            Ok(null_vector(dtype, chunk.num_rows()))
        } else {
            const_vector(value, chunk.num_rows())
        }
    })
}

/// A scalar subquery's value: 0 rows → NULL, 1 row → the value, else error.
fn scalar_value(query: &crate::plan::LogicalPlan, cx: &Cx) -> Result<Value> {
    let out = eval(cx, query)?;
    let mut rows = 0usize;
    let mut value = Value::Null;
    for c in &out.chunks {
        let col = c.columns.first().ok_or_else(|| {
            PinkDbError::Execution("scalar subquery returned no columns".to_string())
        })?;
        for i in 0..c.num_rows() {
            rows += 1;
            if rows > 1 {
                return Err(PinkDbError::Execution(
                    "scalar subquery returned more than one row".to_string(),
                ));
            }
            value = col.get(i);
        }
    }
    Ok(value)
}

/// Evaluate a subquery expression once per chunk row, with that row's
/// enclosing-block values pushed on the outer stack (wave 120).
///
/// This is the correctness fallback for a correlated shape the planner cannot
/// decorrelate into a join (see `docs/wave-120-plan.md`): it executes the body
/// per row, so it is never memoised. An uncorrelated body takes the caller's
/// own path untouched.
fn eval_correlated(
    outer: &OuterBind,
    dtype: DataType,
    chunk: &DataChunk,
    cx: &Cx,
    f: impl Fn(&Cx, &DataChunk) -> Result<Vector>,
) -> Result<Vector> {
    if outer.refs.is_empty() {
        return f(cx, chunk);
    }
    let mut out: Option<Vector> = None;
    for row in 0..chunk.num_rows() {
        let sub = cx.with_outer_row(std::sync::Arc::from(outer.snapshot(chunk, row)));
        let v = f(&sub, &chunk.gather(&[row]))?;
        let acc = match &mut out {
            Some(acc) => acc,
            None => out.insert(Vector::with_capacity(v.dtype(), chunk.num_rows())),
        };
        acc.push_from(&v, 0)?;
    }
    Ok(out.unwrap_or_else(|| null_vector(dtype, 0)))
}

/// An all-NULL vector of `dtype` (the typed counterpart of
/// [`const_vector`]'s untyped NULL).
fn null_vector(dtype: DataType, n: usize) -> Vector {
    let mut v = Vector::with_capacity(dtype, n);
    for _ in 0..n {
        v.push_null().expect("a NULL fits any column");
    }
    v
}

/// `lhs [NOT] IN (query)` with SQL three-valued logic: a match is TRUE;
/// otherwise NULL if the LHS is NULL or the set holds a NULL, else FALSE.
fn eval_in_subquery(
    lhs: &BoundExpr,
    query: &crate::plan::LogicalPlan,
    negated: bool,
    op: BinaryOp,
    outer: &OuterBind,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    if op == BinaryOp::Eq {
        if outer.refs.is_empty() {
            let key = memo_key(query);
            let l = eval_expr(lhs, chunk, cx)?;
            let memo = cx.memoized(key, || {
                let free = cx.order_free(query);
                let out = eval(free.as_ref().unwrap_or(cx), query)?;
                let (set, has_null, common) = in_set(&out, lhs.dtype())?;
                Ok(Memo::Set(std::sync::Arc::new(set), has_null, common))
            })?;
            let Memo::Set(set, has_null, common) = memo else {
                unreachable!("a memo key names one subquery kind")
            };
            return probe_in(&l, &set, has_null, common, negated);
        }
        // A correlated set differs per row, so it can neither be memorised nor
        // shared: build it inside the row's own evaluation.
        return eval_correlated(outer, DataType::Boolean, chunk, cx, |cx, chunk| {
            let l = eval_expr(lhs, chunk, cx)?;
            let out = eval(cx, query)?;
            let (set, has_null, common) = in_set(&out, lhs.dtype())?;
            probe_in(&l, &set, has_null, common, negated)
        });
    }
    // An ordered ANY: `x < ANY (s)` holds iff `x < max(s)`, `x > ANY (s)` iff
    // `x > min(s)`, and `x <> ANY (s)` iff `x` differs from `min(s)` or
    // `max(s)`. So the whole set collapses to its extremes.
    if outer.refs.is_empty() {
        let key = memo_key(query);
        let memo = cx.memoized(key, || {
            let out = eval(cx, query)?;
            let (lo, hi, has_null) = extremes(&out)?;
            Ok(Memo::Extremes(lo, hi, has_null))
        })?;
        let Memo::Extremes(lo, hi, has_null) = memo else {
            unreachable!("a memo key names one subquery kind")
        };
        return probe_any(lhs, op, lo, hi, has_null, negated, chunk, cx);
    }
    eval_correlated(outer, DataType::Boolean, chunk, cx, |cx, chunk| {
        let out = eval(cx, query)?;
        let (lo, hi, has_null) = extremes(&out)?;
        probe_any(lhs, op, lo, hi, has_null, negated, chunk, cx)
    })
}

/// The smallest and largest non-NULL value of a one-column set, and whether it
/// held a NULL at all.
fn extremes(out: &crate::exec::Output) -> Result<(Option<Value>, Option<Value>, bool)> {
    let mut lo: Option<Value> = None;
    let mut hi: Option<Value> = None;
    let mut has_null = false;
    for c in &out.chunks {
        let Some(col) = c.columns.first() else {
            continue;
        };
        for i in 0..col.len() {
            if col.is_null(i) {
                has_null = true;
                continue;
            }
            let v = col.get(i);
            if lo
                .as_ref()
                .is_none_or(|cur| compare_values(&v, cur) == std::cmp::Ordering::Less)
            {
                lo = Some(v.clone());
            }
            if hi
                .as_ref()
                .is_none_or(|cur| compare_values(&v, cur) == std::cmp::Ordering::Greater)
            {
                hi = Some(v);
            }
        }
    }
    Ok((lo, hi, has_null))
}

/// `x op ANY (set)` from the set's extremes: OR over the rows, in three-valued
/// logic, so a set that matches nothing but holds a NULL gives NULL and an
/// empty set gives FALSE.
#[allow(clippy::too_many_arguments)]
fn probe_any(
    lhs: &BoundExpr,
    op: BinaryOp,
    lo: Option<Value>,
    hi: Option<Value>,
    has_null: bool,
    negated: bool,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    let n = chunk.num_rows();
    // `FALSE` is the identity of the OR over the set's rows.
    let mut acc: Vec<Option<bool>> = vec![Some(false); n];
    let extremum = match op {
        BinaryOp::Lt | BinaryOp::LtEq => hi.clone(),
        BinaryOp::Gt | BinaryOp::GtEq => lo.clone(),
        _ => None,
    };
    // `<> ANY` differs from every set member iff it differs from one extreme.
    let probes: Vec<BoundExpr> = match op {
        BinaryOp::NotEq => [lo.clone(), hi.clone()]
            .into_iter()
            .flatten()
            .map(BoundExpr::Literal)
            .collect(),
        _ => extremum.map(BoundExpr::Literal).into_iter().collect(),
    };
    if probes.is_empty() {
        // Nothing to match: FALSE, or NULL when the set held only NULLs.
        let v: Option<bool> = if has_null && lo.is_none() {
            None
        } else {
            Some(false)
        };
        acc = vec![v; n];
    }
    for probe in &probes {
        let cmp = eval_comparison(op, lhs, probe, chunk, cx)?;
        for (i, slot) in acc.iter_mut().enumerate() {
            let cur = match cmp.get(i) {
                Value::Boolean(b) => Some(b),
                _ => None,
            };
            *slot = match (*slot, cur) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            };
        }
    }
    if !probes.is_empty() {
        // A row that matched nothing sees the NULL rows of the set.
        for slot in acc.iter_mut() {
            if *slot == Some(false) && has_null {
                *slot = None;
            }
        }
    }
    let mut out = Vector::new_empty(DataType::Boolean);
    for slot in acc {
        let slot = if negated { slot.map(|b| !b) } else { slot };
        match slot {
            Some(b) => out.push_value(Value::Boolean(b))?,
            None => out.push_null()?,
        }
    }
    Ok(out)
}

/// The set an `IN` subquery builds, whether it held a NULL (such a row is
/// noted, never stored), and the type both sides compare at.
fn in_set(
    out: &crate::exec::Output,
    lhs: DataType,
) -> Result<(FastSet<Vec<u8>>, bool, Option<DataType>)> {
    let rhs_t = out.types.first().copied().unwrap_or(DataType::Integer);
    let common = common_numeric(lhs, rhs_t).ok();
    // Reserve the whole result up front: a large IN set (one entry per row)
    // otherwise rehashes ~log2(n) times while the filter scans the outer side.
    let rows: usize = out.chunks.iter().map(|c| c.num_rows()).sum();
    let mut set = FastSet::with_capacity_and_hasher(rows, Default::default());
    let mut has_null = false;
    let mut buf = Vec::new();
    for c in &out.chunks {
        let col = match c.columns.first() {
            Some(col) => col,
            None => continue,
        };
        let col = match common {
            Some(t) => coerce_to(col, t)?,
            None => (**col).clone(),
        };
        for i in 0..col.len() {
            if col.is_null(i) {
                has_null = true;
                continue;
            }
            buf.clear();
            encode_row(&mut buf, &[col.get(i)]);
            set.insert(buf.clone());
        }
    }
    Ok((set, has_null, common))
}

fn probe_in(
    l: &Vector,
    set: &FastSet<Vec<u8>>,
    has_null: bool,
    common: Option<DataType>,
    negated: bool,
) -> Result<Vector> {
    let l = match common {
        Some(t) => coerce_to(l, t)?,
        None => l.clone(),
    };
    let mut out = Vector::new_empty(DataType::Boolean);
    let mut buf = Vec::new();
    // "x IN (no rows)" is FALSE even when x is NULL, so an empty set decides
    // before the NULL rule: `NOT IN` over it is TRUE.
    let empty = set.is_empty() && !has_null;
    for i in 0..l.len() {
        let hit = if l.is_null(i) {
            false
        } else {
            buf.clear();
            encode_row(&mut buf, &[l.get(i)]);
            set.contains(&buf)
        };
        let v: Option<bool> = if empty {
            Some(negated)
        } else if hit {
            Some(!negated)
        } else if l.is_null(i) || has_null {
            None
        } else {
            Some(negated)
        };
        match v {
            Some(b) => out.push_valid(Value::Boolean(b))?,
            None => out.push_null()?,
        }
    }
    Ok(out)
}

/// `EXISTS` is true iff the query returns any row (memoised per node).
fn eval_exists(
    query: &crate::plan::LogicalPlan,
    negated: bool,
    outer: &OuterBind,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    if outer.refs.is_empty() {
        let key = memo_key(query);
        let memo = cx.memoized(key, || Ok(Memo::Flag(exists_body(query, cx)?)))?;
        let Memo::Flag(any) = memo else {
            unreachable!("a memo key names one subquery kind")
        };
        return const_vector(Value::Boolean(any != negated), chunk.num_rows());
    }
    eval_correlated(outer, DataType::Boolean, chunk, cx, |cx, chunk| {
        let any = exists_body(query, cx)?;
        const_vector(Value::Boolean(any != negated), chunk.num_rows())
    })
}

/// Whether the body produces any row under the current enclosing row.
fn exists_body(query: &crate::plan::LogicalPlan, cx: &Cx) -> Result<bool> {
    let out = eval(cx, query)?;
    Ok(out.chunks.iter().any(|c| c.num_rows() > 0))
}

/// Broadcast a scalar to `n` rows. `NULL` ⇒ INTEGER-typed all-NULL vector
/// (matches the old `push_value` loop); VARCHAR repeats one string view.
fn const_vector(value: Value, n: usize) -> Result<Vector> {
    Ok(Vector::new_const(&value, n))
}

fn ensure_dtype(v: &Vector, want: DataType, ctx: &str) -> Result<()> {
    if v.dtype() != want {
        return Err(PinkDbError::Execution(format!(
            "{ctx}: expected {want}, got {}",
            v.dtype()
        )));
    }
    Ok(())
}

fn as_bool(v: &Vector, i: usize) -> Result<bool> {
    match v.get(i) {
        Value::Boolean(b) => Ok(b),
        other => Err(PinkDbError::Execution(format!(
            "expected BOOLEAN, got {other}"
        ))),
    }
}

// ---------- Kleene AND/OR ----------

fn eval_logic(
    op: BinaryOp,
    left: &BoundExpr,
    right: &BoundExpr,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    let l = eval_expr(left, chunk, cx)?;
    let r = eval_expr(right, chunk, cx)?;
    ensure_dtype(&l, DataType::Boolean, "AND/OR")?;
    ensure_dtype(&r, DataType::Boolean, "AND/OR")?;
    let (lb, rb) = match (l.data(), r.data()) {
        (VectorData::Boolean(a), VectorData::Boolean(b)) => (a, b),
        _ => unreachable!("BOOLEAN dtype implies BOOLEAN layout"),
    };
    let mut out = Vector::new_empty(DataType::Boolean);
    for i in 0..l.len() {
        // Payloads + validity bits directly; Kleene logic unchanged.
        let a = if l.is_valid(i) { Some(lb[i]) } else { None };
        let b = if r.is_valid(i) { Some(rb[i]) } else { None };
        let res = match op {
            BinaryOp::And => match (a, b) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            BinaryOp::Or => match (a, b) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
            _ => unreachable!(),
        };
        match res {
            Some(v) => out.push_bool(v)?,
            None => out.push_null()?,
        }
    }
    Ok(out)
}

// ---------- comparisons ----------

/// Elementwise comparison. Same-dtype operands take a typed kernel over the
/// raw payloads (no `Value`/`String` materialization); mixed dtypes keep the
/// generic `compare_values` path. NULL on either side ⇒ NULL.
fn eval_comparison(
    op: BinaryOp,
    left: &BoundExpr,
    right: &BoundExpr,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    // Column-vs-literal fast path: evaluate only the column and compare each
    // row against the scalar. The general path builds the literal's constant
    // Vector per chunk (an `Arc` clone per element on VARCHAR, a `Value`
    // clone elsewhere), which for a 6M-row filter dwarfs the comparison
    // itself. Mixed numeric widths use the same kernel (a DECIMAL column
    // against a DOUBLE literal, a BIGINT column against an INTEGER literal,
    // ...) so no row is ever compared through a `Value`.
    let fast = match (left, right) {
        (BoundExpr::Literal(v), col) if !v.is_null() && !matches!(col, BoundExpr::Literal(_)) => {
            Some((v, col, true))
        }
        (col, BoundExpr::Literal(v)) if !v.is_null() && !matches!(col, BoundExpr::Literal(_)) => {
            Some((v, col, false))
        }
        _ => None,
    };
    if let Some((scalar, col_expr, scalar_left)) = fast {
        let col = eval_expr(col_expr, chunk, cx)?;
        if let Some(kernel) = ScalarCmp::new(&col, scalar, scalar_left) {
            let mut out = Vector::with_capacity(DataType::Boolean, col.len());
            kernel.push_bool(op, &mut out)?;
            return Ok(out);
        }
        // A pair with no typed form (a VARIANT or ARRAY column, an integer
        // column against a DECIMAL literal): the general path below keeps the
        // old behavior, re-evaluating the column (which has no side effects).
    }
    let l = eval_expr(left, chunk, cx)?;
    let r = eval_expr(right, chunk, cx)?;
    let mut out = Vector::new_empty(DataType::Boolean);
    if l.dtype() == r.dtype() {
        // Same dtype ⇒ same layout; each arm mirrors `compare_values` for
        // that type. DOUBLE uses `num_cmp` (DuckDB order: NaN == NaN, NaN
        // above every other double, -0.0 == 0.0).
        match (l.data(), r.data()) {
            (VectorData::Boolean(a), VectorData::Boolean(b)) => {
                cmp_loop(op, &l, &r, a, b, |x, y| x.cmp(y), &mut out)?;
            }
            (VectorData::Int32(a), VectorData::Int32(b)) => {
                cmp_loop(op, &l, &r, a, b, |x, y| x.cmp(y), &mut out)?;
            }
            (VectorData::Int64(a), VectorData::Int64(b)) => {
                cmp_loop(op, &l, &r, a, b, |x, y| x.cmp(y), &mut out)?;
            }
            (VectorData::Float64(a), VectorData::Float64(b)) => {
                cmp_loop(
                    op,
                    &l,
                    &r,
                    a,
                    b,
                    |x, y| crate::plan::num_cmp(*x, *y),
                    &mut out,
                )?;
            }
            (VectorData::VarChar(a), VectorData::VarChar(b)) => {
                // Borrowed `&str` compare — no per-row string clone.
                let (a, b): (Vec<&str>, Vec<&str>) = (a.iter().collect(), b.iter().collect());
                cmp_loop(op, &l, &r, &a, &b, |x, y| x.cmp(y), &mut out)?;
            }
            (VectorData::Date32(a), VectorData::Date32(b)) => {
                cmp_loop(op, &l, &r, a, b, |x, y| x.cmp(y), &mut out)?;
            }
            (VectorData::TimestampMicros(a), VectorData::TimestampMicros(b)) => {
                cmp_loop(op, &l, &r, a, b, |x, y| x.cmp(y), &mut out)?;
            }
            (
                VectorData::Decimal {
                    values: a,
                    scale: sa,
                    ..
                },
                VectorData::Decimal {
                    values: b,
                    scale: sb,
                    ..
                },
            ) => {
                cmp_loop(
                    op,
                    &l,
                    &r,
                    a,
                    b,
                    |x, y| crate::types::decimal_cmp(*x, *sa, *y, *sb),
                    &mut out,
                )?;
            }
            (VectorData::Interval(a), VectorData::Interval(b)) => {
                cmp_loop(
                    op,
                    &l,
                    &r,
                    a,
                    b,
                    |x, y| crate::types::interval_cmp(*x, *y),
                    &mut out,
                )?;
            }
            (VectorData::Variant(a), VectorData::Variant(b)) => {
                // `variant::cmp` is the same total order (and equality) that
                // `compare_values` uses, so `=` here agrees with GROUP BY.
                cmp_loop(op, &l, &r, a, b, |x, y| crate::variant::cmp(x, y), &mut out)?;
            }
            (
                VectorData::Array {
                    values: a,
                    valid: av,
                    len,
                    ..
                },
                VectorData::Array {
                    values: b,
                    valid: bv,
                    ..
                },
            ) => {
                // One dtype on both sides, so the slots line up: row `i` is
                // `len` consecutive elements at offset `i * len`.
                let n = *len as usize;
                for i in 0..l.len() {
                    if l.is_null(i) || r.is_null(i) {
                        out.push_null()?;
                        continue;
                    }
                    let ord =
                        crate::array::cmp_slot(a, av.as_deref(), i * n, b, bv.as_deref(), i * n, n);
                    out.push_bool(ord_matches(ord, op))?;
                }
            }
            _ => unreachable!("same dtype implies same vector layout"),
        }
        return Ok(out);
    }
    // Mixed dtypes: generic path, unchanged.
    for i in 0..l.len() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
        let ord = compare_values(&l.get(i), &r.get(i));
        out.push_bool(ord_matches(ord, op))?;
    }
    Ok(out)
}

/// Typed comparison loop: NULL when either side is invalid, else `cmp` under
/// the SQL operator. `bool::cmp` gives `false < true`; DATE/TIMESTAMP compare
/// their raw payloads.
fn cmp_loop<T>(
    op: BinaryOp,
    l: &Vector,
    r: &Vector,
    a: &[T],
    b: &[T],
    cmp: impl Fn(&T, &T) -> Ordering,
    out: &mut Vector,
) -> Result<()> {
    for (i, (x, y)) in a.iter().zip(b).enumerate() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
        } else {
            out.push_bool(ord_matches(cmp(x, y), op))?;
        }
    }
    Ok(())
}

/// A `column <op> literal` comparison compiled once per chunk into a typed
/// kernel over the column's payload slice.
///
/// One kernel, two consumers: [`ScalarCmp::push_bool`] builds the BOOLEAN
/// vector the materializing path returns, [`ScalarCmp::retain`] narrows a
/// selection in place (the select-style path), and because both walk the same
/// arms they cannot disagree. Every arm mirrors one arm of
/// `plan::compare_values` for its pair of types — `num_cmp` wherever a DOUBLE
/// is involved, `decimal_cmp` for DECIMAL payloads, plain `Ord` for the rest —
/// so no row is ever compared through a `Value`.
struct ScalarCmp<'a> {
    col: &'a Vector,
    /// The literal is the left operand: `literal op column`. Comparing the
    /// column to the literal and reversing the ordering is the same answer
    /// under the flipped operator, so the row loop stays monomorphic.
    reversed: bool,
    kind: CmpKind<'a>,
}

/// The typed payload/scalar pair of one [`ScalarCmp`]. Integral scalars widen
/// to `i128` and doubles stay `f64`, so every numeric mix has exactly one arm
/// and none of them loses a bit relative to `compare_values`.
enum CmpKind<'a> {
    Int32Int(&'a [i32], i128),
    Int32Float(&'a [i32], f64),
    Int64Int(&'a [i64], i128),
    Int64Float(&'a [i64], f64),
    FloatInt(&'a [f64], i128),
    FloatFloat(&'a [f64], f64),
    FloatDec(&'a [f64], i128, u8),
    DecInt(&'a [i128], u8, i128),
    DecFloat(&'a [i128], u8, f64),
    DecDec(&'a [i128], u8, i128, u8),
    Bool(&'a [bool], bool),
    VarChar(&'a crate::strings::StrVec, std::sync::Arc<str>),
    Date(&'a [i32], i32),
    Timestamp(&'a [i64], i64),
    Interval(&'a [crate::types::Interval], crate::types::Interval),
}

impl<'a> ScalarCmp<'a> {
    /// The kernel for `col` against `scalar`, or `None` when the pair has no
    /// typed form; the caller then falls back to `compare_values` row by row.
    fn new(col: &'a Vector, scalar: &Value, reversed: bool) -> Option<ScalarCmp<'a>> {
        use VectorData as V;
        let kind = match (col.data(), scalar) {
            (V::Int32(a), Value::Integer(s)) => CmpKind::Int32Int(a, *s as i128),
            (V::Int32(a), Value::Bigint(s)) => CmpKind::Int32Int(a, *s as i128),
            (V::Int32(a), Value::Double(s)) => CmpKind::Int32Float(a, *s),
            (V::Int64(a), Value::Integer(s)) => CmpKind::Int64Int(a, *s as i128),
            (V::Int64(a), Value::Bigint(s)) => CmpKind::Int64Int(a, *s as i128),
            (V::Int64(a), Value::Double(s)) => CmpKind::Int64Float(a, *s),
            (V::Float64(a), Value::Integer(s)) => CmpKind::FloatInt(a, *s as i128),
            (V::Float64(a), Value::Bigint(s)) => CmpKind::FloatInt(a, *s as i128),
            (V::Float64(a), Value::Double(s)) => CmpKind::FloatFloat(a, *s),
            (V::Float64(a), Value::Decimal(s, ss)) => CmpKind::FloatDec(a, *s, *ss),
            (V::Decimal { values, scale, .. }, Value::Integer(s)) => {
                CmpKind::DecInt(values, *scale, *s as i128)
            }
            (V::Decimal { values, scale, .. }, Value::Bigint(s)) => {
                CmpKind::DecInt(values, *scale, *s as i128)
            }
            (V::Decimal { values, scale, .. }, Value::Double(s)) => {
                CmpKind::DecFloat(values, *scale, *s)
            }
            (V::Decimal { values, scale, .. }, Value::Decimal(s, ss)) => {
                CmpKind::DecDec(values, *scale, *s, *ss)
            }
            (V::Boolean(a), Value::Boolean(s)) => CmpKind::Bool(a, *s),
            // One `Arc<str>` for the scalar (one allocation per chunk)
            // instead of one clone per element.
            (V::VarChar(a), Value::Varchar(s)) => {
                CmpKind::VarChar(a, std::sync::Arc::from(s.as_str()))
            }
            (V::Date32(a), Value::Date(s)) => CmpKind::Date(a, *s),
            (V::TimestampMicros(a), Value::Timestamp(s)) => CmpKind::Timestamp(a, *s),
            (
                V::Interval(a),
                Value::Interval {
                    months,
                    days,
                    micros,
                },
            ) => CmpKind::Interval(
                a,
                crate::types::Interval {
                    months: *months,
                    days: *days,
                    micros: *micros,
                },
            ),
            _ => return None,
        };
        Some(ScalarCmp {
            col,
            reversed,
            kind,
        })
    }

    /// `column <op> literal` for every row, NULL where the column is NULL.
    fn push_bool(&self, op: BinaryOp, out: &mut Vector) -> Result<()> {
        let (col, flipped) = (self.col, self.reversed);
        match &self.kind {
            CmpKind::Int32Int(a, s) => push_cmp(col, a, op, flipped, |x| (*x as i128).cmp(s), out),
            CmpKind::Int32Float(a, s) => {
                push_cmp(col, a, op, flipped, |x| num_cmp(*x as f64, *s), out)
            }
            CmpKind::Int64Int(a, s) => push_cmp(col, a, op, flipped, |x| (*x as i128).cmp(s), out),
            CmpKind::Int64Float(a, s) => {
                push_cmp(col, a, op, flipped, |x| num_cmp(*x as f64, *s), out)
            }
            CmpKind::FloatInt(a, s) => {
                push_cmp(col, a, op, flipped, |x| num_cmp(*x, *s as f64), out)
            }
            CmpKind::FloatFloat(a, s) => push_cmp(col, a, op, flipped, |x| num_cmp(*x, *s), out),
            CmpKind::FloatDec(a, s, ss) => push_cmp(
                col,
                a,
                op,
                flipped,
                |x| num_cmp(*x, crate::types::decimal_to_f64(*s, *ss)),
                out,
            ),
            CmpKind::DecInt(a, sa, s) => push_cmp(
                col,
                a,
                op,
                flipped,
                |x| crate::types::decimal_cmp(*x, *sa, *s, 0),
                out,
            ),
            CmpKind::DecFloat(a, sa, s) => push_cmp(
                col,
                a,
                op,
                flipped,
                |x| num_cmp(crate::types::decimal_to_f64(*x, *sa), *s),
                out,
            ),
            CmpKind::DecDec(a, sa, s, ss) => push_cmp(
                col,
                a,
                op,
                flipped,
                |x| crate::types::decimal_cmp(*x, *sa, *s, *ss),
                out,
            ),
            CmpKind::Bool(a, s) => push_cmp(col, a, op, flipped, |x| x.cmp(s), out),
            CmpKind::VarChar(a, s) => {
                let a: Vec<&str> = a.iter().collect();
                push_cmp(col, &a, op, flipped, |x| (**x).cmp(&**s), out)
            }
            CmpKind::Date(a, s) => push_cmp(col, a, op, flipped, |x| x.cmp(s), out),
            CmpKind::Timestamp(a, s) => push_cmp(col, a, op, flipped, |x| x.cmp(s), out),
            CmpKind::Interval(a, s) => push_cmp(
                col,
                a,
                op,
                flipped,
                |x| crate::types::interval_cmp(*x, *s),
                out,
            ),
        }
    }

    /// Keep only the rows of `sel` where `column <op> literal` is TRUE: the
    /// select-style form, which never builds the BOOLEAN vector.
    fn retain(&self, op: BinaryOp, sel: &mut Vec<u32>) {
        if self.retain_in_domain(op, sel) {
            return;
        }
        let (col, flipped) = (self.col, self.reversed);
        match &self.kind {
            CmpKind::Int32Int(a, s) => {
                retain_cmp(col, a, op, flipped, |x| (*x as i128).cmp(s), sel)
            }
            CmpKind::Int32Float(a, s) => {
                retain_cmp(col, a, op, flipped, |x| num_cmp(*x as f64, *s), sel)
            }
            CmpKind::Int64Int(a, s) => {
                retain_cmp(col, a, op, flipped, |x| (*x as i128).cmp(s), sel)
            }
            CmpKind::Int64Float(a, s) => {
                retain_cmp(col, a, op, flipped, |x| num_cmp(*x as f64, *s), sel)
            }
            CmpKind::FloatInt(a, s) => {
                retain_cmp(col, a, op, flipped, |x| num_cmp(*x, *s as f64), sel)
            }
            CmpKind::FloatFloat(a, s) => retain_cmp(col, a, op, flipped, |x| num_cmp(*x, *s), sel),
            CmpKind::FloatDec(a, s, ss) => retain_cmp(
                col,
                a,
                op,
                flipped,
                |x| num_cmp(*x, crate::types::decimal_to_f64(*s, *ss)),
                sel,
            ),
            CmpKind::DecInt(a, sa, s) => retain_cmp(
                col,
                a,
                op,
                flipped,
                |x| crate::types::decimal_cmp(*x, *sa, *s, 0),
                sel,
            ),
            CmpKind::DecFloat(a, sa, s) => retain_cmp(
                col,
                a,
                op,
                flipped,
                |x| num_cmp(crate::types::decimal_to_f64(*x, *sa), *s),
                sel,
            ),
            CmpKind::DecDec(a, sa, s, ss) => retain_cmp(
                col,
                a,
                op,
                flipped,
                |x| crate::types::decimal_cmp(*x, *sa, *s, *ss),
                sel,
            ),
            CmpKind::Bool(a, s) => retain_cmp(col, a, op, flipped, |x| x.cmp(s), sel),
            CmpKind::VarChar(a, s) => {
                let a: Vec<&str> = a.iter().collect();
                retain_cmp(col, &a, op, flipped, |x| (**x).cmp(&**s), sel)
            }
            CmpKind::Date(a, s) => retain_cmp(col, a, op, flipped, |x| x.cmp(s), sel),
            CmpKind::Timestamp(a, s) => retain_cmp(col, a, op, flipped, |x| x.cmp(s), sel),
            CmpKind::Interval(a, s) => retain_cmp(
                col,
                a,
                op,
                flipped,
                |x| crate::types::interval_cmp(*x, *s),
                sel,
            ),
        }
    }
}

impl ScalarCmp<'_> {
    /// [`ScalarCmp::retain`] with the literal moved into the column's own
    /// domain once per chunk, so each row is one integer comparison under an
    /// operator matched outside the loop (DuckDB's `SelectFlat` loops). A
    /// DECIMAL against a DOUBLE literal (pinkdb types `0.05` as DOUBLE) keeps
    /// its exact meaning: the row test `decimal_to_f64(x) <op> s` is monotone
    /// in `x`, so it equals an integer range whose bounds are found with that
    /// same conversion. `false` leaves the pair to the generic loop.
    fn retain_in_domain(&self, op: BinaryOp, sel: &mut Vec<u32>) -> bool {
        let op = if self.reversed { flip_ordering(op) } else { op };
        let col = self.col;
        match &self.kind {
            CmpKind::Date(a, s) => retain_ordered(col, a, *s, op, sel),
            CmpKind::Timestamp(a, s) => retain_ordered(col, a, *s, op, sel),
            CmpKind::Int64Int(a, s) => match i64::try_from(*s) {
                Ok(s) => retain_ordered(col, a, s, op, sel),
                Err(_) => return false,
            },
            CmpKind::Int32Int(a, s) => match i32::try_from(*s) {
                Ok(s) => retain_ordered(col, a, s, op, sel),
                Err(_) => return false,
            },
            CmpKind::DecDec(a, sa, s, ss) if sa >= ss => {
                match s.checked_mul(crate::types::POW10_I128[(sa - ss) as usize]) {
                    Some(s) => retain_ordered(col, a, s, op, sel),
                    None => return false,
                }
            }
            CmpKind::DecInt(a, sa, s) => {
                match s.checked_mul(crate::types::POW10_I128[*sa as usize]) {
                    Some(s) => retain_ordered(col, a, s, op, sel),
                    None => return false,
                }
            }
            CmpKind::VarChar(a, s) => retain_strings(
                col,
                a,
                |x| {
                    let ord = x.cmp(&**s);
                    match op {
                        BinaryOp::Eq => ord == Ordering::Equal,
                        BinaryOp::NotEq => ord != Ordering::Equal,
                        BinaryOp::Lt => ord == Ordering::Less,
                        BinaryOp::LtEq => ord != Ordering::Greater,
                        BinaryOp::Gt => ord == Ordering::Greater,
                        BinaryOp::GtEq => ord != Ordering::Less,
                        _ => unreachable!("retain_in_domain called with {op:?}"),
                    }
                },
                sel,
            ),
            CmpKind::DecFloat(a, sa, s) => {
                let at_least =
                    |strict| first_passing(*s, strict, |x| crate::types::decimal_to_f64(x, *sa));
                // `lo`: the first x with value >= s; `hi`: the first > s.
                let (Some(lo), Some(hi)) = (at_least(false), at_least(true)) else {
                    return false;
                };
                retain_int_range(col, a, op, lo, hi, sel)
            }
            _ => return false,
        }
        true
    }
}

/// Keep the rows of `sel` whose valid string passes `test`, deciding each
/// distinct string view once: a short string's view is the string, and the
/// rows of a dictionary-encoded column (the `.pink` store's low-cardinality
/// strings) share one location, so the answer is remembered by view for up
/// to [`STRING_MEMO`] views and the string itself is only read for a view not
/// seen yet (TPC-H Q10's `l_returnflag = 'R'` compared six million one-letter
/// strings).
fn retain_strings(
    col: &Vector,
    a: &crate::strings::StrVec,
    test: impl Fn(&str) -> bool,
    sel: &mut Vec<u32>,
) {
    let nulls = col.has_nulls();
    let mut memo: Vec<(u128, bool)> = Vec::new();
    // A view not seen once the memo is full means the strings are not
    // shared (or too many): test the rest directly.
    let mut direct = false;
    let mut w = 0;
    for r in 0..sel.len() {
        let row = sel[r];
        let i = row as usize;
        if nulls && !col.is_valid(i) {
            continue;
        }
        let keep = if direct {
            test(&a[i])
        } else {
            let p = a.view(i).key();
            match memo.iter().find(|(q, _)| *q == p) {
                Some(&(_, k)) => k,
                None => {
                    let k = test(&a[i]);
                    if memo.len() < STRING_MEMO {
                        memo.push((p, k));
                    } else {
                        direct = true;
                    }
                    k
                }
            }
        };
        sel[w] = row;
        w += usize::from(keep);
    }
    sel.truncate(w);
}

/// Distinct string views [`retain_strings`] remembers per chunk.
const STRING_MEMO: usize = 16;

/// The comparison `literal <op> column` read as `column <op'> literal`.
fn flip_ordering(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}

/// Keep the rows of `sel` whose cell is valid and passes `keep`: a
/// branch-free compaction (every row is written, the cursor only advances
/// for a kept one).
#[inline(always)]
fn retain_rows<T: Copy>(col: &Vector, a: &[T], sel: &mut Vec<u32>, keep: impl Fn(T) -> bool) {
    let nulls = col.has_nulls();
    let mut w = 0;
    for r in 0..sel.len() {
        let row = sel[r];
        let i = row as usize;
        let ok = keep(a[i]) & (!nulls || col.is_valid(i));
        sel[w] = row;
        w += usize::from(ok);
    }
    sel.truncate(w);
}

/// `column <op> s` over one ordered payload, the operator matched once.
fn retain_ordered<T: Copy + Ord>(col: &Vector, a: &[T], s: T, op: BinaryOp, sel: &mut Vec<u32>) {
    match op {
        BinaryOp::Eq => retain_rows(col, a, sel, |x| x == s),
        BinaryOp::NotEq => retain_rows(col, a, sel, |x| x != s),
        BinaryOp::Lt => retain_rows(col, a, sel, |x| x < s),
        BinaryOp::LtEq => retain_rows(col, a, sel, |x| x <= s),
        BinaryOp::Gt => retain_rows(col, a, sel, |x| x > s),
        BinaryOp::GtEq => retain_rows(col, a, sel, |x| x >= s),
        _ => unreachable!("retain_ordered called with {op:?}"),
    }
}

/// `column <op> s` for a monotone row test given as integer bounds: `lo` is
/// the first payload value comparing `>= s`, `hi` the first comparing `> s`.
fn retain_int_range(
    col: &Vector,
    a: &[i128],
    op: BinaryOp,
    lo: i128,
    hi: i128,
    sel: &mut Vec<u32>,
) {
    match op {
        BinaryOp::Eq => retain_rows(col, a, sel, |x| lo <= x && x < hi),
        BinaryOp::NotEq => retain_rows(col, a, sel, |x| x < lo || hi <= x),
        BinaryOp::Lt => retain_rows(col, a, sel, |x| x < lo),
        BinaryOp::LtEq => retain_rows(col, a, sel, |x| x < hi),
        BinaryOp::Gt => retain_rows(col, a, sel, |x| x >= hi),
        BinaryOp::GtEq => retain_rows(col, a, sel, |x| x >= lo),
        _ => unreachable!("retain_int_range called with {op:?}"),
    }
}

/// The smallest integer `x` whose `value(x)` compares `>= s` (or `> s` when
/// `strict`) under `num_cmp`, for a non-decreasing `value`: searched from
/// the rounded estimate with the exact conversion the row test uses. `None`
/// for a literal without such a bound nearby (NaN, infinities, huge values).
fn first_passing(s: f64, strict: bool, value: impl Fn(i128) -> f64) -> Option<i128> {
    if !s.is_finite() {
        return None;
    }
    let passes = |x: i128| {
        let ord = num_cmp(value(x), s);
        if strict {
            ord == Ordering::Greater
        } else {
            ord != Ordering::Less
        }
    };
    // Estimate in the payload's units: `value` divides by 10^scale, so the
    // probe at 1 tells the factor without knowing the scale.
    let unit = value(1);
    let guess = (s / unit).floor();
    if !guess.is_finite() || guess.abs() > 1e30 {
        return None;
    }
    let mut x = guess as i128;
    for _ in 0..64 {
        match (passes(x - 1), passes(x)) {
            (false, true) => return Some(x),
            (true, _) => x -= 1,
            (false, false) => x += 1,
        }
    }
    None
}

/// `column <op> literal` for every row of `a`, NULL-preserving.
fn push_cmp<T>(
    col: &Vector,
    a: &[T],
    op: BinaryOp,
    flipped: bool,
    ord: impl Fn(&T) -> Ordering,
    out: &mut Vector,
) -> Result<()> {
    for (i, x) in a.iter().enumerate() {
        if col.is_null(i) {
            out.push_null()?;
        } else {
            let ord = ord(x);
            out.push_bool(ord_matches(if flipped { ord.reverse() } else { ord }, op))?;
        }
    }
    Ok(())
}

/// Drop the rows of `sel` where `column <op> literal` is not TRUE (NULL is
/// not TRUE, exactly as in [`select_true`]).
fn retain_cmp<T>(
    col: &Vector,
    a: &[T],
    op: BinaryOp,
    flipped: bool,
    ord: impl Fn(&T) -> Ordering,
    sel: &mut Vec<u32>,
) {
    sel.retain(|&row| {
        let i = row as usize;
        if col.is_null(i) {
            return false;
        }
        let ord = ord(&a[i]);
        ord_matches(if flipped { ord.reverse() } else { ord }, op)
    });
}

/// Rescale integer payloads to `DECIMAL(precision, scale)` with the cast's
/// range check and error text.
fn decimal_from_int(
    v: &Vector,
    src: impl Iterator<Item = i128>,
    precision: u8,
    scale: u8,
) -> Result<Vector> {
    let mut values: Vec<i128> = Vec::with_capacity(v.len());
    for (i, x) in src.enumerate() {
        if v.is_null(i) {
            values.push(0);
            continue;
        }
        let y = crate::types::int_to_decimal(x, precision, scale).ok_or_else(|| {
            PinkDbError::Execution(format!(
                "cannot coerce {x} to DECIMAL({precision},{scale}): value out of range"
            ))
        })?;
        values.push(y);
    }
    Ok(Vector::from_parts(
        VectorData::Decimal {
            values,
            precision,
            scale,
        },
        v.validity().to_vec(),
        v.len(),
    ))
}

/// SQL operator truth for an ordering: `= <> < <= > >=`.
#[inline]
fn ord_matches(ord: Ordering, op: BinaryOp) -> bool {
    match op {
        BinaryOp::Eq => ord == Ordering::Equal,
        BinaryOp::NotEq => ord != Ordering::Equal,
        BinaryOp::Lt => ord == Ordering::Less,
        BinaryOp::LtEq => ord != Ordering::Greater,
        BinaryOp::Gt => ord == Ordering::Greater,
        BinaryOp::GtEq => ord != Ordering::Less,
        _ => unreachable!("ord_matches called with {op:?}"),
    }
}

// ---------- arithmetic ----------

pub(crate) fn coerce_to(v: &Vector, target: DataType) -> Result<Vector> {
    if v.dtype() == target {
        return Ok(v.clone());
    }
    // Typed slice fast paths: payloads + validity mask are copied directly,
    // with no per-row `Value` roundtrip (NULL rows keep NULL bits).
    match (v.data(), target) {
        (VectorData::Int32(src), DataType::Bigint) => {
            let data = VectorData::Int64(src.iter().map(|&x| x as i64).collect());
            Ok(Vector::from_parts(data, v.validity().to_vec(), v.len()))
        }
        (VectorData::Int32(src), DataType::Double) => {
            let data = VectorData::Float64(src.iter().map(|&x| x as f64).collect());
            Ok(Vector::from_parts(data, v.validity().to_vec(), v.len()))
        }
        (VectorData::Int64(src), DataType::Double) => {
            let data = VectorData::Float64(src.iter().map(|&x| x as f64).collect());
            Ok(Vector::from_parts(data, v.validity().to_vec(), v.len()))
        }
        // DECIMAL → DOUBLE: `/` widens both sides to DOUBLE (DuckDB rule);
        // explicit casts and comparisons use the exact rescale rules.
        (VectorData::Decimal { values, scale, .. }, DataType::Double) => {
            let data = VectorData::Float64(
                values
                    .iter()
                    .map(|&x| crate::types::decimal_to_f64(x, *scale))
                    .collect(),
            );
            Ok(Vector::from_parts(data, v.validity().to_vec(), v.len()))
        }
        // INTEGER/BIGINT → DECIMAL: the same `i * 10^scale` the per-row cast
        // does, without a `Value` per row. Mixed decimal arithmetic coerces
        // its integer operands here once per chunk (TPC-H Q1's `1 - l_discount`
        // and `1 + l_tax`), so the roundtrip is on a hot path.
        (VectorData::Int32(src), DataType::Decimal { precision, scale }) => {
            decimal_from_int(v, src.iter().map(|&x| x as i128), precision, scale)
        }
        (VectorData::Int64(src), DataType::Decimal { precision, scale }) => {
            decimal_from_int(v, src.iter().map(|&x| x as i128), precision, scale)
        }
        // DECIMAL → DECIMAL rescale: exact when widening the scale, rounding
        // half away from zero when narrowing (DuckDB's cast rule).
        (
            VectorData::Decimal { values, scale, .. },
            DataType::Decimal {
                precision,
                scale: want,
            },
        ) => {
            let mut out: Vec<i128> = Vec::with_capacity(values.len());
            for (i, &x) in values.iter().enumerate() {
                if v.is_null(i) {
                    out.push(0);
                    continue;
                }
                let y = rescale_decimal(x, *scale, want)
                    .filter(|y| decimal_fits(*y, precision))
                    .ok_or_else(|| {
                        PinkDbError::Execution(format!(
                            "cannot coerce {} to DECIMAL({precision},{want}): value out of range",
                            format_decimal(x, *scale)
                        ))
                    })?;
                out.push(y);
            }
            let data = VectorData::Decimal {
                values: out,
                precision,
                scale: want,
            };
            Ok(Vector::from_parts(data, v.validity().to_vec(), v.len()))
        }
        // Everything else: existing generic loop (errors unchanged).
        _ => generic_coerce(v, target),
    }
}

/// Per-row `coerce_value` loop (the fallback for shapes the typed fast paths
/// do not cover).
fn generic_coerce(v: &Vector, target: DataType) -> Result<Vector> {
    let mut out = Vector::new_empty(target);
    for i in 0..v.len() {
        if v.is_null(i) {
            out.push_null()?;
            continue;
        }
        out.push_valid(coerce_value(v.get(i), target)?)?;
    }
    Ok(out)
}

fn coerce_value(v: Value, target: DataType) -> Result<Value> {
    match (v, target) {
        (Value::Integer(i), DataType::Bigint) => Ok(Value::Bigint(i as i64)),
        (Value::Integer(i), DataType::Double) => Ok(Value::Double(i as f64)),
        (Value::Bigint(i), DataType::Double) => Ok(Value::Double(i as f64)),
        (Value::Decimal(x, from), DataType::Double) => {
            Ok(Value::Double(crate::types::decimal_to_f64(x, from)))
        }
        // INSERT/UPDATE coercion of a VARCHAR cell into the target column
        // type parses like a cast (DuckDB accepts string literals for typed
        // columns).
        (Value::Varchar(s), DataType::Decimal { precision, scale }) => {
            crate::types::parse_decimal_str(&s, precision, scale)
                .map(|x| Value::Decimal(x, scale))
                .map_err(PinkDbError::Type)
        }
        (Value::Varchar(s), DataType::Interval) => match crate::types::parse_interval(&s) {
            Some(iv) => Ok(Value::Interval {
                months: iv.months,
                days: iv.days,
                micros: iv.micros,
            }),
            None => Err(PinkDbError::Type(format!(
                "cannot parse \"{s}\" as INTERVAL"
            ))),
        },
        (Value::Decimal(x, from), DataType::Decimal { precision, scale }) => {
            let y = crate::types::rescale_decimal(x, from, scale)
                .filter(|y| crate::types::decimal_fits(*y, precision))
                .ok_or_else(|| {
                    PinkDbError::Execution(format!(
                        "cannot coerce {}: value out of range",
                        crate::types::format_decimal(x, from)
                    ))
                })?;
            Ok(Value::Decimal(y, scale))
        }
        (Value::Integer(i), DataType::Decimal { precision, scale }) => {
            crate::types::int_to_decimal(i as i128, precision, scale)
                .map(|x| Value::Decimal(x, scale))
                .ok_or_else(|| {
                    PinkDbError::Execution(format!(
                        "cannot coerce {i} to DECIMAL({precision},{scale}): value out of range"
                    ))
                })
        }
        (Value::Bigint(i), DataType::Decimal { precision, scale }) => {
            crate::types::int_to_decimal(i as i128, precision, scale)
                .map(|x| Value::Decimal(x, scale))
                .ok_or_else(|| {
                    PinkDbError::Execution(format!(
                        "cannot coerce {i} to DECIMAL({precision},{scale}): value out of range"
                    ))
                })
        }
        (v, _) => Err(PinkDbError::Execution(format!("cannot coerce {v}"))),
    }
}

fn negate_value(v: Value, dtype: DataType) -> Result<Value> {
    match (v, dtype) {
        (Value::Integer(i), DataType::Integer) => i
            .checked_neg()
            .map(Value::Integer)
            .ok_or_else(|| overflow_err("negation", "INTEGER", format_args!("-({i})"))),
        (Value::Bigint(i), DataType::Bigint) => i
            .checked_neg()
            .map(Value::Bigint)
            .ok_or_else(|| overflow_err("negation", "BIGINT", format_args!("-({i})"))),
        (Value::Double(x), DataType::Double) => Ok(Value::Double(-x)),
        (Value::Decimal(x, scale), DataType::Decimal { .. }) => x
            .checked_neg()
            .map(|y| Value::Decimal(y, scale))
            .ok_or_else(|| overflow_err("negation", "DECIMAL", format_args!("-({x})"))),
        (
            Value::Interval {
                months,
                days,
                micros,
            },
            DataType::Interval,
        ) => {
            let iv = crate::types::interval_negate(crate::types::Interval {
                months,
                days,
                micros,
            });
            Ok(Value::Interval {
                months: iv.months,
                days: iv.days,
                micros: iv.micros,
            })
        }
        (v, t) => Err(PinkDbError::Execution(format!("cannot negate {v} as {t}"))),
    }
}

fn eval_arith(
    op: BinaryOp,
    dtype: DataType,
    left: &BoundExpr,
    right: &BoundExpr,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    // One side a literal: a constant operand needs neither a chunk-wide
    // constant vector nor a coercion of every row, so it gets its own loop
    // (TPC-H Q1's `1 - l_discount`).
    if let Some(out) = eval_decimal_const(op, dtype, left, right, chunk, cx)? {
        return Ok(out);
    }
    let l = eval_expr(left, chunk, cx)?;
    let r = eval_expr(right, chunk, cx)?;
    // Temporal types skip numeric coercion (it rejects them): day/month
    // arithmetic below.
    let temporal =
        |d: DataType| matches!(d, DataType::Date | DataType::Timestamp | DataType::Interval);
    if temporal(l.dtype()) || temporal(r.dtype()) {
        return eval_temporal_arith(op, &l, &r, dtype);
    }
    // DECIMAL multiplication must run on the operands' own scales (the
    // result's scale is their sum), so it never goes through the target-type
    // coercion below.
    if op == BinaryOp::Multiply && matches!(dtype, DataType::Decimal { .. }) {
        return eval_decimal_mul(&l, &r, dtype);
    }
    // Integer division by zero errors even though `/` widens to DOUBLE
    // (DuckDB rule); 0.0/0.0 with a float operand stays IEEE Inf/NaN.
    let int_div =
        op == BinaryOp::Divide && l.dtype() != DataType::Double && r.dtype() != DataType::Double;
    // Move instead of deep-cloning when a side already has the target dtype.
    let l = if l.dtype() == dtype {
        l
    } else {
        coerce_to(&l, dtype)?
    };
    let r = if r.dtype() == dtype {
        r
    } else {
        coerce_to(&r, dtype)?
    };
    // Typed kernel when both sides now share one layout; otherwise the
    // generic per-row loop below (semantics unchanged).
    if let Some(out) = arith_typed(op, int_div, &l, &r)? {
        return Ok(out);
    }
    let mut out = Vector::new_empty(dtype);
    for i in 0..l.len() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
        if int_div && r.get(i) == Value::Double(0.0) {
            return Err(PinkDbError::Execution(
                "integer division by zero".to_string(),
            ));
        }
        out.push_valid(arith_values(op, l.get(i), r.get(i))?)?;
    }
    Ok(out)
}

/// DECIMAL `*`: the operands contribute their own scales, the result scale is
/// their sum (DuckDB rule). Operands are DECIMAL columns/literals or the
/// integer types the binder converts to DECIMAL(10,0)/DECIMAL(19,0).
fn eval_decimal_mul(l: &Vector, r: &Vector, dtype: DataType) -> Result<Vector> {
    let (lv, ls) = decimal_operand(l)?;
    let (rv, rs) = decimal_operand(r)?;
    let DataType::Decimal { precision, .. } = dtype else {
        unreachable!("eval_decimal_mul on non-decimal result type")
    };
    let len = l.len();
    // A row is NULL when either operand is, so the result mask is the AND of
    // the operand masks: the loop below then writes payload only, where a
    // `push_decimal` per row would re-test room and re-set a mask bit.
    let mut validity = l.validity().to_vec();
    validity.resize(len.div_ceil(64), 0);
    for (word, other) in validity.iter_mut().zip(r.validity()) {
        *word &= *other;
    }
    let all_valid = !l.has_nulls() && !r.has_nulls();
    // Chunk-bound fast path (wave 126): when the largest operands multiply
    // to less than 10^precision, no row can overflow `i128` or the result's
    // precision, so the checks are dropped; operands that fit in i64 also
    // make the product one widening multiply. Any other chunk takes the
    // checked loop below, which raises the error at the offending row.
    let (ma, mb) = (max_abs_i128(&lv), max_abs_i128(&rv));
    let limit = crate::types::POW10_I128[precision as usize] as u128;
    if ma <= i64::MAX as u128
        && mb <= i64::MAX as u128
        && ma.checked_mul(mb).is_some_and(|p| p < limit)
    {
        let mut values: Vec<i128> = lv
            .iter()
            .zip(rv.iter())
            .map(|(&a, &b)| (a as i64 as i128) * (b as i64 as i128))
            .collect();
        if !all_valid {
            zero_invalid(&mut values, &validity);
        }
        return Ok(Vector::from_parts(
            VectorData::Decimal {
                values,
                precision,
                scale: ls + rs,
            },
            validity,
            len,
        ));
    }
    let mut values: Vec<i128> = Vec::with_capacity(len);
    for i in 0..len {
        let (a, b) = (lv[i], rv[i]);
        if !all_valid && (validity[i / 64] >> (i % 64)) & 1 == 0 {
            values.push(0);
            continue;
        }
        let product = a
            .checked_mul(b)
            .filter(|p| crate::types::decimal_fits(*p, precision));
        let product = product.ok_or_else(|| {
            // DuckDB's text (DecimalMultiplyOverflowCheck): the physical width
            // and the unscaled operands.
            if precision <= 18 {
                PinkDbError::Execution(format!(
                    "Overflow in multiplication of DECIMAL(18) ({a} * {b}). You might want to \
                     add an explicit cast to a bigger decimal."
                ))
            } else {
                PinkDbError::Execution(format!(
                    "Overflow in multiplication of DECIMAL(38) ({a} * {b}). You might want to \
                     add an explicit cast to a decimal with a smaller scale."
                ))
            }
        })?;
        values.push(product);
    }
    Ok(Vector::from_parts(
        VectorData::Decimal {
            values,
            precision,
            scale: ls + rs,
        },
        validity,
        len,
    ))
}

/// The largest `|x|` in a decimal payload. NULL slots hold some valid value
/// (zero when this module wrote them), so the bound is conservative.
#[inline]
fn max_abs_i128(values: &[i128]) -> u128 {
    values.iter().map(|x| x.unsigned_abs()).max().unwrap_or(0)
}

/// Zero the payload of NULL rows, as the checked loops do, so a vector's
/// payload never depends on which path computed it.
fn zero_invalid(values: &mut [i128], validity: &[u64]) {
    for (i, v) in values.iter_mut().enumerate() {
        if (validity[i / 64] >> (i % 64)) & 1 == 0 {
            *v = 0;
        }
    }
}

/// A DECIMAL operand as unscaled i128 values plus their scale. Integer
/// operands are scale-0 values (the decimal *widths* only matter at bind
/// time, for the result precision). A DECIMAL payload is borrowed: the
/// caller only reads it, and a per-chunk copy of 2048 `i128`s is a visible
/// share of a multiply-heavy aggregate (TPC-H Q1).
fn decimal_operand(v: &Vector) -> Result<(std::borrow::Cow<'_, [i128]>, u8)> {
    Ok(match v.data() {
        VectorData::Decimal { values, scale, .. } => (std::borrow::Cow::Borrowed(values), *scale),
        VectorData::Int32(src) => (
            std::borrow::Cow::Owned(src.iter().map(|&x| x as i128).collect()),
            0,
        ),
        VectorData::Int64(src) => (
            std::borrow::Cow::Owned(src.iter().map(|&x| x as i128).collect()),
            0,
        ),
        other => {
            return Err(PinkDbError::Execution(format!(
                "decimal multiplication needs numeric operands, got {:?}",
                other.dtype()
            )));
        }
    })
}

/// Typed kernels for same-layout operands (both sides already coerced to
/// `dtype`). `Ok(None)` ⇒ combination not covered: caller runs the generic
/// per-row loop, which keeps the old behavior (including its errors).
fn arith_typed(op: BinaryOp, int_div: bool, l: &Vector, r: &Vector) -> Result<Option<Vector>> {
    match (l.data(), r.data()) {
        (VectorData::Int32(a), VectorData::Int32(b)) => match op {
            BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Modulo => {
                Ok(Some(arith_int32(op, l, r, a, b)?))
            }
            // `Divide` is never typed on integers by the binder.
            _ => Ok(None),
        },
        (VectorData::Int64(a), VectorData::Int64(b)) => match op {
            BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Modulo => {
                Ok(Some(arith_int64(op, l, r, a, b)?))
            }
            _ => Ok(None),
        },
        (VectorData::Float64(a), VectorData::Float64(b)) => match op {
            BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide => {
                Ok(Some(arith_f64(op, int_div, l, r, a, b)?))
            }
            _ => Ok(None),
        },
        // Both sides are already coerced to the result DECIMAL's scale, so
        // `+`, `-` and `%` are plain i128 operations with a precision check.
        (
            VectorData::Decimal {
                values: a,
                precision,
                scale,
            },
            VectorData::Decimal { values: b, .. },
        ) => match op {
            BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Modulo => {
                Ok(Some(arith_decimal(op, l, r, a, b, *precision, *scale)?))
            }
            // `*` never reaches here (eval_arith routes it earlier); `/` is
            // typed DOUBLE by the binder.
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

/// One DECIMAL `+`/`-`/`%` cell at a shared scale, with DuckDB's overflow and
/// division-by-zero errors (the two loops below share this, so the text cannot
/// drift between them).
#[inline]
fn decimal_arith_row(op: BinaryOp, x: i128, y: i128, precision: u8, scale: u8) -> Result<i128> {
    let detail = || {
        format!(
            "({} {} {})",
            crate::types::format_decimal(x, scale),
            match op {
                BinaryOp::Add => "+",
                BinaryOp::Subtract => "-",
                _ => "%",
            },
            crate::types::format_decimal(y, scale)
        )
    };
    match op {
        BinaryOp::Add => x
            .checked_add(y)
            .filter(|v| crate::types::decimal_fits(*v, precision)),
        BinaryOp::Subtract => x
            .checked_sub(y)
            .filter(|v| crate::types::decimal_fits(*v, precision)),
        BinaryOp::Modulo => {
            if y == 0 {
                return Err(PinkDbError::Execution(format!(
                    "Division by zero in expression {}",
                    detail()
                )));
            }
            Some(x.wrapping_rem(y))
        }
        _ => unreachable!("decimal_arith_row: {op:?}"),
    }
    .ok_or_else(|| {
        let what = match op {
            BinaryOp::Add => "addition",
            BinaryOp::Subtract => "subtraction",
            _ => "modulo",
        };
        PinkDbError::Execution(format!(
            "Overflow in {what} of DECIMAL({precision}) {}",
            detail()
        ))
    })
}

/// DECIMAL `+`/`-`/`%` at a shared scale, with DuckDB's overflow error.
fn arith_decimal(
    op: BinaryOp,
    l: &Vector,
    r: &Vector,
    a: &[i128],
    b: &[i128],
    precision: u8,
    scale: u8,
) -> Result<Vector> {
    let len = a.len();
    // Same shape as `eval_decimal_mul`: AND the operand masks, then write the
    // payload directly. `push_decimal` re-tests room and re-sets a mask bit per
    // row, and this loop is a visible share of a decimal-heavy aggregate.
    let mut validity = l.validity().to_vec();
    validity.resize(len.div_ceil(64), 0);
    for (word, other) in validity.iter_mut().zip(r.validity()) {
        *word &= *other;
    }
    // Chunk-bound fast path (wave 126): `|x ± y| <= max|x| + max|y|`, so
    // when that sum is below 10^precision no row can overflow and the
    // per-row checked op is dropped. Modulo keeps the checked loop (its
    // error is division by zero, not overflow).
    if matches!(op, BinaryOp::Add | BinaryOp::Subtract)
        && max_abs_i128(a)
            .checked_add(max_abs_i128(b))
            .is_some_and(|s| s < crate::types::POW10_I128[precision as usize] as u128)
    {
        let mut values: Vec<i128> = if op == BinaryOp::Add {
            a.iter().zip(b).map(|(&x, &y)| x + y).collect()
        } else {
            a.iter().zip(b).map(|(&x, &y)| x - y).collect()
        };
        zero_invalid(&mut values, &validity);
        return Ok(Vector::from_parts(
            VectorData::Decimal {
                values,
                precision,
                scale,
            },
            validity,
            len,
        ));
    }
    let mut values: Vec<i128> = Vec::with_capacity(len);
    for (i, (&x, &y)) in a.iter().zip(b).enumerate() {
        if (validity[i / 64] >> (i % 64)) & 1 == 0 {
            values.push(0);
            continue;
        }
        values.push(decimal_arith_row(op, x, y, precision, scale)?);
    }
    Ok(Vector::from_parts(
        VectorData::Decimal {
            values,
            precision,
            scale,
        },
        validity,
        len,
    ))
}

/// DECIMAL `+`/`-`/`%` with one constant operand: the same cells as
/// `arith_decimal`, read against a scalar instead of a second payload.
fn arith_decimal_const(
    op: BinaryOp,
    col: &Vector,
    a: &[i128],
    k: i128,
    precision: u8,
    scale: u8,
    const_left: bool,
) -> Result<Vector> {
    let len = a.len();
    // The constant is never NULL, so the column's mask is the result's.
    let mut validity = col.validity().to_vec();
    validity.resize(len.div_ceil(64), 0);
    // Chunk-bound fast path (wave 126), as in `arith_decimal`.
    if matches!(op, BinaryOp::Add | BinaryOp::Subtract)
        && max_abs_i128(a)
            .checked_add(k.unsigned_abs())
            .is_some_and(|s| s < crate::types::POW10_I128[precision as usize] as u128)
    {
        let mut values: Vec<i128> = match (op, const_left) {
            (BinaryOp::Add, _) => a.iter().map(|&x| x + k).collect(),
            (_, true) => a.iter().map(|&x| k - x).collect(),
            (_, false) => a.iter().map(|&x| x - k).collect(),
        };
        zero_invalid(&mut values, &validity);
        return Ok(Vector::from_parts(
            VectorData::Decimal {
                values,
                precision,
                scale,
            },
            validity,
            len,
        ));
    }
    let mut values: Vec<i128> = Vec::with_capacity(len);
    for (i, &x) in a.iter().enumerate() {
        if (validity[i / 64] >> (i % 64)) & 1 == 0 {
            values.push(0);
            continue;
        }
        values.push(if const_left {
            decimal_arith_row(op, k, x, precision, scale)?
        } else {
            decimal_arith_row(op, x, k, precision, scale)?
        });
    }
    Ok(Vector::from_parts(
        VectorData::Decimal {
            values,
            precision,
            scale,
        },
        validity,
        len,
    ))
}

/// `literal op column` / `column op literal` on DECIMAL: `Ok(None)` for every
/// other shape, and `eval_arith` then runs its own path.
fn eval_decimal_const(
    op: BinaryOp,
    dtype: DataType,
    left: &BoundExpr,
    right: &BoundExpr,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Option<Vector>> {
    let DataType::Decimal { precision, scale } = dtype else {
        return Ok(None);
    };
    if !matches!(op, BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Modulo) {
        return Ok(None);
    }
    let (lit, column, const_left) = match (left, right) {
        (BoundExpr::Literal(v), column) => (v, column, true),
        (column, BoundExpr::Literal(v)) => (v, column, false),
        _ => return Ok(None),
    };
    if !matches!(
        lit,
        Value::Integer(_) | Value::Bigint(_) | Value::Decimal(..)
    ) || matches!(column, BoundExpr::Literal(_))
    {
        return Ok(None);
    }
    // The column keeps its own layout: rescaling it would need a vector
    // anyway. A source precision that fits the result's means the coercion
    // the other path performs can never reject a value, so only the scale has
    // to agree.
    let v = eval_expr(column, chunk, cx)?;
    let VectorData::Decimal {
        values,
        precision: src,
        scale: col_scale,
    } = v.data()
    else {
        return Ok(None);
    };
    if *col_scale != scale || *src > precision {
        return Ok(None);
    }
    let Value::Decimal(k, _) = coerce_value(lit.clone(), dtype)? else {
        return Ok(None);
    };
    Ok(Some(arith_decimal_const(
        op, &v, values, k, precision, scale, const_left,
    )?))
}

/// DuckDB-style integer overflow error (`+`/`-`/`*`, negation, `abs`).
fn overflow_err(what: &str, ty: &str, detail: std::fmt::Arguments) -> PinkDbError {
    PinkDbError::Execution(format!("Overflow in {what} of {ty} ({detail})"))
}

/// Checked INTEGER/BIGINT binary arithmetic shared by the typed kernels and
/// the per-value path: overflow is an error (DuckDB semantics), never a
/// wrapped result. `%` keeps `wrapping_rem` (only `MIN % -1` wraps, and its
/// mathematical result 0 is exactly what wrapping returns).
macro_rules! checked_int_arith {
    ($op:expr_2021, $x:expr_2021, $y:expr_2021, $ty:literal) => {{
        let (x, y) = ($x, $y);
        match $op {
            BinaryOp::Add => x
                .checked_add(y)
                .ok_or_else(|| overflow_err("addition", $ty, format_args!("{x} + {y}")))?,
            BinaryOp::Subtract => x
                .checked_sub(y)
                .ok_or_else(|| overflow_err("subtraction", $ty, format_args!("{x} - {y}")))?,
            BinaryOp::Multiply => x
                .checked_mul(y)
                .ok_or_else(|| overflow_err("multiplication", $ty, format_args!("{x} * {y}")))?,
            BinaryOp::Modulo => {
                if y == 0 {
                    return Err(PinkDbError::Execution("integer modulo by zero".to_string()));
                }
                x.wrapping_rem(y)
            }
            op => unreachable!("binder types {op:?} on {}", $ty),
        }
    }};
}

/// INTEGER `+`/`-`/`*`/`%` with `arith_values`' checked semantics.
fn arith_int32(op: BinaryOp, l: &Vector, r: &Vector, a: &[i32], b: &[i32]) -> Result<Vector> {
    let mut out = Vector::with_capacity(DataType::Integer, a.len());
    for (i, (&x, &y)) in a.iter().zip(b).enumerate() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
        let v = checked_int_arith!(op, x, y, "INTEGER");
        out.push_int32(v)?;
    }
    Ok(out)
}

/// BIGINT `+`/`-`/`*`/`%` with `arith_values`' checked semantics.
fn arith_int64(op: BinaryOp, l: &Vector, r: &Vector, a: &[i64], b: &[i64]) -> Result<Vector> {
    let mut out = Vector::with_capacity(DataType::Bigint, a.len());
    for (i, (&x, &y)) in a.iter().zip(b).enumerate() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
        let v = checked_int_arith!(op, x, y, "BIGINT");
        out.push_int64(v)?;
    }
    Ok(out)
}

/// DOUBLE `+`/`-`/`*`/`/` with IEEE semantics. `int_div` keeps the old
/// int/int rule even after widening: a zero divisor errors (a float operand
/// would instead produce Inf/NaN).
fn arith_f64(
    op: BinaryOp,
    int_div: bool,
    l: &Vector,
    r: &Vector,
    a: &[f64],
    b: &[f64],
) -> Result<Vector> {
    let mut out = Vector::with_capacity(DataType::Double, a.len());
    for (i, (&x, &y)) in a.iter().zip(b).enumerate() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
        if int_div && y == 0.0 {
            return Err(PinkDbError::Execution(
                "integer division by zero".to_string(),
            ));
        }
        let v = match op {
            BinaryOp::Add => x + y,
            BinaryOp::Subtract => x - y,
            BinaryOp::Multiply => x * y,
            // Binder forces DOUBLE for `/`, so int/int arrives widened here.
            BinaryOp::Divide => x / y,
            _ => unreachable!("arith_f64: {op:?}"),
        };
        out.push_float64(v)?;
    }
    Ok(out)
}

/// DATE/TIMESTAMP/INTERVAL arithmetic: `date ± integer` days, `date - date`
/// days, `date/timestamp ± interval` → TIMESTAMP, `timestamp - timestamp`
/// → INTERVAL, `interval ± interval`, `interval *|/ number`. All month
/// arithmetic clamps at month end (DuckDB rule).
fn eval_temporal_arith(op: BinaryOp, l: &Vector, r: &Vector, dtype: DataType) -> Result<Vector> {
    let mut out = Vector::new_empty(dtype);
    for i in 0..l.len() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
        let v = temporal_arith(op, l.get(i), r.get(i))?;
        out.push_valid(v)?;
    }
    Ok(out)
}

fn as_interval(v: &Value) -> Result<crate::types::Interval> {
    match v {
        Value::Interval {
            months,
            days,
            micros,
        } => Ok(crate::types::Interval {
            months: *months,
            days: *days,
            micros: *micros,
        }),
        v => Err(PinkDbError::Execution(format!(
            "expected an INTERVAL, got {v}"
        ))),
    }
}

fn interval_out_of_range() -> PinkDbError {
    PinkDbError::Execution("interval out of range".to_string())
}

fn date_out_of_range(d: i32, sym: &str, n: i32) -> PinkDbError {
    PinkDbError::Execution(format!(
        "date out of range: {} {sym} {n} days",
        Value::Date(d)
    ))
}

fn as_interval_number(v: &Value) -> Result<f64> {
    match v {
        Value::Integer(i) => Ok(*i as f64),
        Value::Bigint(i) => Ok(*i as f64),
        Value::Double(x) => Ok(*x),
        Value::Decimal(x, scale) => Ok(crate::types::decimal_to_f64(*x, *scale)),
        v => Err(PinkDbError::Execution(format!(
            "interval arithmetic needs a number, got {v}"
        ))),
    }
}

fn timestamp_out_of_range() -> PinkDbError {
    PinkDbError::Execution("timestamp out of range".to_string())
}

fn timestamp_us_for_date(d: i32) -> Result<i64> {
    (d as i64)
        .checked_mul(86_400_000_000)
        .ok_or_else(timestamp_out_of_range)
}

fn temporal_arith(op: BinaryOp, a: Value, b: Value) -> Result<Value> {
    use crate::types::*;
    // Shorthand constructors.
    let mk_iv = |iv: Interval| Value::Interval {
        months: iv.months,
        days: iv.days,
        micros: iv.micros,
    };
    match (op, &a, &b) {
        (BinaryOp::Add, Value::Date(d), Value::Integer(n))
        | (BinaryOp::Add, Value::Integer(n), Value::Date(d)) => Ok(Value::Date(
            d.checked_add(*n)
                .ok_or_else(|| date_out_of_range(*d, "+", *n))?,
        )),
        (BinaryOp::Subtract, Value::Date(d), Value::Integer(n)) => Ok(Value::Date(
            d.checked_sub(*n)
                .ok_or_else(|| date_out_of_range(*d, "-", *n))?,
        )),
        (BinaryOp::Subtract, Value::Date(x), Value::Date(y)) => {
            Ok(Value::Bigint(*x as i64 - *y as i64))
        }
        (BinaryOp::Add, Value::Interval { .. }, Value::Interval { .. }) => {
            let (ai, bi) = (as_interval(&a)?, as_interval(&b)?);
            interval_add(ai, bi)
                .map(mk_iv)
                .ok_or_else(interval_out_of_range)
        }
        (BinaryOp::Subtract, Value::Interval { .. }, Value::Interval { .. }) => {
            let (ai, bi) = (as_interval(&a)?, as_interval(&b)?);
            interval_sub(ai, bi)
                .map(mk_iv)
                .ok_or_else(interval_out_of_range)
        }
        (BinaryOp::Add, Value::Date(d), Value::Interval { .. })
        | (BinaryOp::Add, Value::Interval { .. }, Value::Date(d)) => {
            let iv = as_interval(&b)?;
            date_add_interval(*d, iv)
                .map(Value::Timestamp)
                .ok_or_else(timestamp_out_of_range)
        }
        (BinaryOp::Subtract, Value::Date(d), Value::Interval { .. }) => {
            let iv = interval_negate(as_interval(&b)?);
            date_add_interval(*d, iv)
                .map(Value::Timestamp)
                .ok_or_else(timestamp_out_of_range)
        }
        (BinaryOp::Add, Value::Timestamp(us), Value::Interval { .. })
        | (BinaryOp::Add, Value::Interval { .. }, Value::Timestamp(us)) => {
            let iv = as_interval(&b)?;
            timestamp_add_interval(*us, iv)
                .map(Value::Timestamp)
                .ok_or_else(timestamp_out_of_range)
        }
        (BinaryOp::Subtract, Value::Timestamp(us), Value::Interval { .. }) => {
            let iv = interval_negate(as_interval(&b)?);
            timestamp_add_interval(*us, iv)
                .map(Value::Timestamp)
                .ok_or_else(timestamp_out_of_range)
        }
        (BinaryOp::Subtract, Value::Timestamp(x), Value::Timestamp(y)) => Ok(Value::Interval {
            months: 0,
            days: 0,
            micros: x.checked_sub(*y).ok_or_else(timestamp_out_of_range)?,
        }),
        (BinaryOp::Subtract, Value::Timestamp(x), Value::Date(d)) => {
            let base = timestamp_us_for_date(*d)?;
            Ok(Value::Interval {
                months: 0,
                days: 0,
                micros: x.checked_sub(base).ok_or_else(timestamp_out_of_range)?,
            })
        }
        (BinaryOp::Subtract, Value::Date(d), Value::Timestamp(x)) => {
            let base = timestamp_us_for_date(*d)?;
            Ok(Value::Interval {
                months: 0,
                days: 0,
                micros: base.checked_sub(*x).ok_or_else(timestamp_out_of_range)?,
            })
        }
        (BinaryOp::Multiply, Value::Interval { .. }, _) => {
            let iv = as_interval(&a)?;
            let f = as_interval_number(&b)?;
            interval_mul(iv, f)
                .map(mk_iv)
                .ok_or_else(interval_out_of_range)
        }
        (BinaryOp::Multiply, _, Value::Interval { .. }) => {
            let iv = as_interval(&b)?;
            let f = as_interval_number(&a)?;
            interval_mul(iv, f)
                .map(mk_iv)
                .ok_or_else(interval_out_of_range)
        }
        (BinaryOp::Divide, Value::Interval { .. }, _) => {
            let iv = as_interval(&a)?;
            let f = as_interval_number(&b)?;
            if f == 0.0 {
                return Err(PinkDbError::Execution("division by zero".to_string()));
            }
            interval_div(iv, f)
                .map(mk_iv)
                .ok_or_else(interval_out_of_range)
        }
        (op, a, b) => Err(PinkDbError::Execution(format!(
            "operator {op:?} does not support {a} and {b}"
        ))),
    }
}

fn arith_values(op: BinaryOp, l: Value, r: Value) -> Result<Value> {
    match (l, r) {
        (Value::Integer(a), Value::Integer(b)) => {
            Ok(Value::Integer(checked_int_arith!(op, a, b, "INTEGER")))
        }
        (Value::Bigint(a), Value::Bigint(b)) => {
            Ok(Value::Bigint(checked_int_arith!(op, a, b, "BIGINT")))
        }
        (Value::Double(a), Value::Double(b)) => {
            let v = match op {
                BinaryOp::Add => a + b,
                BinaryOp::Subtract => a - b,
                BinaryOp::Multiply => a * b,
                // Binder forces DOUBLE for `/`, so int/int arrives widened here.
                BinaryOp::Divide => a / b,
                _ => unreachable!("binder types {op:?} on DOUBLE"),
            };
            Ok(Value::Double(v))
        }
        // Decimal `+`/`-`/`%`: both values arrive at the result scale.
        (Value::Decimal(a, sa), Value::Decimal(b, sb)) => {
            debug_assert_eq!(sa, sb, "decimal values are coerced to one scale");
            let scale = sa.max(sb);
            let (a, b) = if sa < sb {
                (crate::types::rescale_decimal(a, sa, sb).unwrap_or(a), b)
            } else if sb < sa {
                (a, crate::types::rescale_decimal(b, sb, sa).unwrap_or(b))
            } else {
                (a, b)
            };
            let v = match op {
                BinaryOp::Add => a.checked_add(b),
                BinaryOp::Subtract => a.checked_sub(b),
                BinaryOp::Multiply => a.checked_mul(b),
                BinaryOp::Modulo => {
                    if b == 0 {
                        return Err(PinkDbError::Execution(
                            "Division by zero in expression".to_string(),
                        ));
                    }
                    Some(a.wrapping_rem(b))
                }
                _ => unreachable!("binder types {op:?} on DECIMAL"),
            };
            v.map(|v| Value::Decimal(v, scale))
                .ok_or_else(|| PinkDbError::Execution("Overflow in DECIMAL arithmetic".to_string()))
        }
        (l, r) => Err(PinkDbError::Execution(format!(
            "arithmetic on non-numeric values: {l}, {r}"
        ))),
    }
}

// ---------- scalar functions ----------

fn as_i64(v: &Value) -> Result<i64> {
    match v {
        Value::Integer(i) => Ok(*i as i64),
        Value::Bigint(i) => Ok(*i),
        Value::Double(x) => Ok(*x as i64),
        Value::Decimal(x, scale) => crate::types::rescale_decimal(*x, *scale, 0)
            .and_then(|v| i64::try_from(v).ok())
            .ok_or_else(|| PinkDbError::Execution(format!("expected numeric, got {v}"))),
        _ => Err(PinkDbError::Execution(format!("expected numeric, got {v}"))),
    }
}

fn as_f64(v: &Value) -> Result<f64> {
    match v {
        Value::Integer(i) => Ok(*i as f64),
        Value::Bigint(i) => Ok(*i as f64),
        Value::Double(x) => Ok(*x),
        Value::Decimal(x, scale) => Ok(crate::types::decimal_to_f64(*x, *scale)),
        _ => Err(PinkDbError::Execution(format!("expected numeric, got {v}"))),
    }
}

/// DECIMAL `abs`/`round`/`floor`/`ceil`/`sign` (DuckDB keeps the numeric
/// family on DECIMAL: ABS keeps the type, ROUND/FLOOR/CEIL return
/// DECIMAL(p, n)/DECIMAL(p, 0)).
fn decimal_scalar(func: ScalarFunc, dtype: DataType, v: &Value) -> Result<Option<Value>> {
    let Value::Decimal(x, scale) = v else {
        return Ok(None);
    };
    let DataType::Decimal { precision, .. } = dtype else {
        return Ok(None);
    };
    Ok(match func {
        ScalarFunc::Abs => Some(Value::Decimal(
            x.checked_abs()
                .ok_or_else(|| overflow_err("abs", "DECIMAL", format_args!("abs({x})")))?,
            *scale,
        )),
        ScalarFunc::Sign => Some(Value::Integer(x.signum() as i32)),
        // FLOOR rounds toward -inf, CEIL toward +inf (DuckDB: FLOOR(-1.5) =
        // -2, CEIL(-1.5) = -1); both return DECIMAL(p, 0).
        ScalarFunc::Floor => {
            let q = if *scale == 0 {
                *x
            } else {
                x.div_euclid(POW10_I128[*scale as usize])
            };
            let q = decimal_fits(q, precision)
                .then_some(q)
                .ok_or_else(|| overflow_err("floor", "DECIMAL", format_args!("floor of {v}")))?;
            Some(Value::Decimal(q, 0))
        }
        ScalarFunc::Ceil => {
            let q = if *scale == 0 {
                *x
            } else {
                -(-x).div_euclid(POW10_I128[*scale as usize])
            };
            let q = decimal_fits(q, precision)
                .then_some(q)
                .ok_or_else(|| overflow_err("ceil", "DECIMAL", format_args!("ceil of {v}")))?;
            Some(Value::Decimal(q, 0))
        }
        // The general ROUND path handles decimal digits below.
        _ => None,
    })
}

/// `n` random version-4 UUIDs in their canonical text form.
#[cfg(not(target_arch = "wasm32"))]
fn random_uuids(n: usize) -> Result<Vector> {
    let mut bytes = vec![0u8; n * 16];
    getrandom::getrandom(&mut bytes)
        .map_err(|e| PinkDbError::Execution(format!("cannot obtain randomness for uuid(): {e}")))?;
    let mut out = Vector::new_empty(DataType::Varchar);
    for raw in bytes.as_chunks_mut::<16>().0 {
        raw[6] = (raw[6] & 0x0f) | 0x40;
        raw[8] = (raw[8] & 0x3f) | 0x80;
        let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
        out.push_valid(Value::Varchar(format!(
            "{}-{}-{}-{}-{}",
            &hex[0..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        )))?;
    }
    Ok(out)
}

#[cfg(target_arch = "wasm32")]
fn random_uuids(_n: usize) -> Result<Vector> {
    Err(PinkDbError::Execution(
        "uuid() needs OS randomness, which the wasm build has not".to_string(),
    ))
}

fn eval_scalar(func: ScalarFunc, dtype: DataType, args: &[Vector]) -> Result<Vector> {
    let n = args.first().map(|v| v.len()).unwrap_or(0);
    for v in args {
        if v.len() != n {
            return Err(PinkDbError::Execution(
                "scalar arg length mismatch".to_string(),
            ));
        }
    }
    // Common single-arg functions read typed payloads directly (no per-row
    // `Vec<Value>`); rare shapes fall through to the generic row loop.
    if let Some(out) = eval_scalar_typed(func, args)? {
        return Ok(out);
    }
    if let Some(out) = eval_scalar_fast(func, dtype, args)? {
        return Ok(out);
    }
    // concat/nullif/greatest/least handle NULLs themselves.
    if !func.is_strict() {
        let mut out = Vector::new_empty(dtype);
        let mut row: Vec<Value> = Vec::with_capacity(args.len());
        for i in 0..n {
            row.clear();
            for v in args {
                row.push(v.get(i));
            }
            out.push_valid(apply_scalar_loose(func, dtype, &row)?)?;
        }
        return Ok(out);
    }
    // Any-NULL-in → NULL-out.
    let mut out = Vector::new_empty(dtype);
    for i in 0..n {
        let mut row: Vec<Value> = Vec::with_capacity(args.len());
        let mut is_null = false;
        for v in args {
            if v.is_null(i) {
                is_null = true;
                break;
            }
            row.push(v.get(i));
        }
        if is_null {
            out.push_null()?;
            continue;
        }
        out.push_valid(apply_scalar(func, dtype, &row)?)?;
    }
    Ok(out)
}

/// Scalar functions whose answer depends on the argument's *column type* (or
/// on the VARIANT kind inside it) rather than on converting the row to a
/// [`Value`]: a NULL cell still reports what it is, so these run before the
/// any-NULL-in → NULL-out loop.
fn eval_scalar_typed(func: ScalarFunc, args: &[Vector]) -> Result<Option<Vector>> {
    let Some(src) = args.first() else {
        return Ok(None);
    };
    let out = match func {
        ScalarFunc::TypeOf => Vector::new_const(&Value::Varchar(src.dtype().name()), src.len()),
        ScalarFunc::VariantType | ScalarFunc::VariantKeys => {
            let want_kind = func == ScalarFunc::VariantType;
            let mut out = Vector::new_empty(DataType::Varchar);
            for i in 0..src.len() {
                match src.get(i) {
                    Value::Null => out.push_null()?,
                    // DuckDB answers NULL for the variant-null: it has no type
                    // name and no keys (DuckDB 2.0.0).
                    Value::Variant(v) if matches!(&*v, crate::variant::Variant::Null) => {
                        out.push_null()?
                    }
                    Value::Variant(v) if want_kind => {
                        out.push_valid(Value::Varchar(v.type_name().to_string()))?
                    }
                    Value::Variant(v) => {
                        out.push_valid(Value::Varchar(crate::variant::keys_json(&v)))?
                    }
                    other => {
                        return Err(PinkDbError::Execution(format!(
                            "{func:?} needs a VARIANT argument, got {}",
                            other
                                .data_type()
                                .map_or_else(|| "NULL".to_string(), |t| t.name())
                        )));
                    }
                }
            }
            out
        }
        // `len(a)` / `array_length(a)`: one number for every row, so the
        // generic loop would rebuild a whole array `Value` per row.
        ScalarFunc::Length | ScalarFunc::ArrayLength
            if args.len() == 1 && matches!(src.data(), VectorData::Array { .. }) =>
        {
            let VectorData::Array { len, .. } = src.data() else {
                unreachable!("guarded above")
            };
            let len = *len as i32;
            let mut out = Vector::new_empty(DataType::Integer);
            for i in 0..src.len() {
                if src.is_null(i) {
                    out.push_null()?;
                } else {
                    out.push_int32(len)?;
                }
            }
            out
        }
        // `a[i]`: read the element out of the flat buffer. `INTEGER` indices
        // are the common case (`a[1]`, a vector column); other index layouts
        // take the generic path.
        ScalarFunc::ArrayDistance
        | ScalarFunc::ArrayCosineDistance
        | ScalarFunc::ArrayCosineSimilarity
        | ScalarFunc::ArrayInnerProduct
        | ScalarFunc::ArrayNegativeInnerProduct
            if args.len() == 2 =>
        {
            eval_distance(func, &args[0], &args[1])?
        }
        ScalarFunc::ArrayExtract if args.len() == 2 && src.dtype().is_array() => {
            let VectorData::Array {
                elem,
                len,
                values,
                valid: av,
            } = src.data()
            else {
                unreachable!("guarded above")
            };
            let (elem, len) = (*elem, *len as usize);
            let idx = &args[1];
            let mut out = Vector::new_empty(elem.dtype());
            match idx.data() {
                VectorData::Int32(ix) => {
                    for (i, index) in ix.iter().enumerate() {
                        if src.is_null(i) || idx.is_null(i) {
                            out.push_null()?;
                        } else {
                            out.push_valid(crate::array::ArrayValue::slot_elem(
                                elem,
                                values,
                                av.as_deref(),
                                i * len,
                                len,
                                i64::from(*index),
                            ))?;
                        }
                    }
                }
                _ => return Ok(None),
            }
            out
        }
        _ => return Ok(None),
    };
    Ok(Some(out))
}

/// `substr(s, offset)` without a length: DuckDB passes `u32::MAX`.
const SUBSTR_TO_END: i64 = u32::MAX as i64;

/// An INTEGER or BIGINT argument read per row without a `Value`.
enum IntArg<'a> {
    I32(&'a [i32]),
    I64(&'a [i64]),
}

impl<'a> IntArg<'a> {
    fn of(v: &'a Vector) -> Option<IntArg<'a>> {
        match v.data() {
            VectorData::Int32(x) => Some(IntArg::I32(x)),
            VectorData::Int64(x) => Some(IntArg::I64(x)),
            _ => None,
        }
    }

    fn get(&self, i: usize) -> i64 {
        match self {
            IntArg::I32(x) => i64::from(x[i]),
            IntArg::I64(x) => x[i],
        }
    }
}

/// `substr`'s result `[start, end)` in characters of a string `n` characters
/// long, or `None` for the empty string. A port of DuckDB's
/// `SubstringStartEnd` + `SubstringASCIIBounds`
/// (src/function/scalar/string/substring.cpp): a positive offset counts from
/// 1, a negative one from the end, offset 0 starts one character before the
/// first (so it eats one character of the length), and a negative length
/// reaches backwards from the start.
fn substr_range(n: i64, offset: i64, mut length: i64) -> Option<(usize, usize)> {
    if length == 0 {
        return None;
    }
    let mut start = match offset {
        o if o > 0 => o - 1,
        o if o < 0 => o,
        _ => {
            length -= 1;
            if length <= 0 {
                return None;
            }
            0
        }
    };
    let mut end = if length > 0 {
        start + length
    } else {
        let end = start;
        start += length;
        end
    };
    if offset < 0 {
        start += n;
        end += n;
    }
    let start = start.clamp(0, n);
    let end = end.clamp(0, n);
    (start < end).then_some((start as usize, end as usize))
}

/// `substr(s, offset, length)` with DuckDB's semantics and range errors,
/// borrowed from `s`. Characters are code points; ASCII strings slice bytes.
fn substr_slice(s: &str, offset: i64, length: i64) -> Result<&str> {
    const UPPER: i64 = u32::MAX as i64;
    const LOWER: i64 = -UPPER - 1;
    for (what, x) in [("offset", offset), ("length", length)] {
        if x > UPPER {
            return Err(PinkDbError::Execution(format!(
                "Substring {what} outside of supported range (> {UPPER})"
            )));
        }
        if x < LOWER {
            return Err(PinkDbError::Execution(format!(
                "Substring {what} outside of supported range (< {LOWER})"
            )));
        }
    }
    if s.is_ascii() {
        return Ok(match substr_range(s.len() as i64, offset, length) {
            Some((a, b)) => &s[a..b],
            None => "",
        });
    }
    let n = s.chars().count();
    let Some((a, b)) = substr_range(n as i64, offset, length) else {
        return Ok("");
    };
    let mut bytes = s
        .char_indices()
        .map(|(i, _)| i)
        .chain(std::iter::once(s.len()));
    let from = bytes.nth(a).unwrap_or(s.len());
    let to = if b == a {
        from
    } else {
        bytes.nth(b - a - 1).unwrap_or(s.len())
    };
    Ok(&s[from..to])
}

/// Typed fast paths for the common scalar functions. `Ok(None)` ⇒ arg
/// shape not covered: caller runs the generic `apply_scalar` loop, which
/// preserves the old errors for bad shapes/types.
fn eval_scalar_fast(func: ScalarFunc, dtype: DataType, args: &[Vector]) -> Result<Option<Vector>> {
    match func {
        ScalarFunc::Upper | ScalarFunc::Lower if args.len() == 1 && dtype == DataType::Varchar => {
            let v = &args[0];
            let src = match v.data() {
                VectorData::VarChar(s) => s,
                // Non-VARCHAR input hits the generic path for its error.
                _ => return Ok(None),
            };
            let upper = func == ScalarFunc::Upper;
            let mut out = Vector::new_empty(DataType::Varchar);
            for (i, s) in src.iter().enumerate() {
                if v.is_null(i) {
                    out.push_null()?;
                } else if upper {
                    out.push_str(&s.to_uppercase())?;
                } else {
                    out.push_str(&s.to_lowercase())?;
                }
            }
            Ok(Some(out))
        }
        ScalarFunc::Substr if dtype == DataType::Varchar && (2..=3).contains(&args.len()) => {
            let v = &args[0];
            let VectorData::VarChar(src) = v.data() else {
                // A NULL literal input (typed INTEGER) takes the generic path.
                return Ok(None);
            };
            let (Some(offsets), lengths) = (IntArg::of(&args[1]), args.get(2).map(IntArg::of))
            else {
                return Ok(None);
            };
            let lengths = match lengths {
                Some(Some(l)) => Some(l),
                Some(None) => return Ok(None),
                None => None,
            };
            let mut validity = v.validity().to_vec();
            for a in &args[1..] {
                for (w, m) in validity.iter_mut().zip(a.validity()) {
                    *w &= m;
                }
            }
            let mut out = crate::strings::StrVec::with_capacity(src.len());
            for (i, s) in src.iter().enumerate() {
                if validity[i / 64] >> (i % 64) & 1 == 0 {
                    out.push("");
                    continue;
                }
                let length = lengths.as_ref().map_or(SUBSTR_TO_END, |l| l.get(i));
                out.push(substr_slice(s, offsets.get(i), length)?);
            }
            Ok(Some(Vector::from_parts(
                VectorData::VarChar(out),
                validity,
                src.len(),
            )))
        }
        ScalarFunc::Length if args.len() == 1 && dtype == DataType::Integer => {
            let v = &args[0];
            let src = match v.data() {
                VectorData::VarChar(s) => s,
                _ => return Ok(None),
            };
            let mut out = Vector::new_empty(DataType::Integer);
            for (i, s) in src.iter().enumerate() {
                if v.is_null(i) {
                    out.push_null()?;
                } else {
                    // Char count without cloning the input string.
                    out.push_int32(s.chars().count() as i32)?;
                }
            }
            Ok(Some(out))
        }
        ScalarFunc::Abs if args.len() == 1 => {
            let v = &args[0];
            let mut out = Vector::new_empty(dtype);
            match (v.data(), dtype) {
                (VectorData::Int32(s), DataType::Integer) => {
                    for (i, &x) in s.iter().enumerate() {
                        if v.is_null(i) {
                            out.push_null()?;
                        } else {
                            out.push_int32(x.checked_abs().ok_or_else(|| {
                                overflow_err("abs", "INTEGER", format_args!("abs({x})"))
                            })?)?;
                        }
                    }
                    Ok(Some(out))
                }
                (VectorData::Int64(s), DataType::Bigint) => {
                    for (i, &x) in s.iter().enumerate() {
                        if v.is_null(i) {
                            out.push_null()?;
                        } else {
                            out.push_int64(x.checked_abs().ok_or_else(|| {
                                overflow_err("abs", "BIGINT", format_args!("abs({x})"))
                            })?)?;
                        }
                    }
                    Ok(Some(out))
                }
                (VectorData::Float64(s), DataType::Double) => {
                    for (i, &x) in s.iter().enumerate() {
                        if v.is_null(i) {
                            out.push_null()?;
                        } else {
                            out.push_float64(x.abs())?;
                        }
                    }
                    Ok(Some(out))
                }
                _ => Ok(None),
            }
        }
        ScalarFunc::Round if args.len() == 1 && dtype == DataType::Double => {
            let v = &args[0];
            let src = match v.data() {
                VectorData::Float64(s) => s,
                // Two-arg form or non-DOUBLE input uses the generic path.
                _ => return Ok(None),
            };
            let mut out = Vector::new_empty(DataType::Double);
            for (i, &x) in src.iter().enumerate() {
                if v.is_null(i) {
                    out.push_null()?;
                } else {
                    // One-arg ROUND: digits = 0, factor = 1 ⇒ plain rounding.
                    out.push_float64(x.round())?;
                }
            }
            Ok(Some(out))
        }
        _ => Ok(None),
    }
}

/// Non-strict scalar functions: NULL handling is part of the function
/// (`concat`/`greatest`/`least` skip NULLs; `nullif` only propagates its
/// first argument's NULL).
fn apply_scalar_loose(func: ScalarFunc, dtype: DataType, row: &[Value]) -> Result<Value> {
    match func {
        ScalarFunc::ArrayValue => {
            let DataType::Array { elem, len } = dtype else {
                unreachable!("the binder types an ARRAY constructor as ARRAY")
            };
            let mut values = crate::array::ArrayElems::with_capacity(elem, row.len());
            let mut valid = vec![0xFFu8; row.len().div_ceil(8)];
            let mut any_null = false;
            for (i, v) in row.iter().enumerate() {
                match v {
                    // A NULL argument is a NULL element, never a NULL array.
                    Value::Null => {
                        values.push_zero();
                        valid[i / 8] &= !(1u8 << (i % 8));
                        any_null = true;
                    }
                    v => values.push_elem(elem, Some(v))?,
                }
            }
            let value = crate::array::ArrayValue::new(
                elem,
                len,
                values,
                any_null.then(|| valid.into_boxed_slice()),
            )?;
            Ok(Value::Array(std::sync::Arc::new(value)))
        }
        ScalarFunc::Concat => {
            let mut out = String::new();
            for v in row {
                match v {
                    Value::Null => {}
                    Value::Varchar(s) => out.push_str(s),
                    // The binder casts every argument to VARCHAR; a bare NULL
                    // literal stays NULL and is skipped above.
                    other => {
                        return Err(PinkDbError::Execution(format!(
                            "concat of {other} (use CAST)"
                        )));
                    }
                }
            }
            Ok(Value::Varchar(out))
        }
        ScalarFunc::NullIf => {
            let a = &row[0];
            if a.is_null() {
                return Ok(Value::Null);
            }
            let b = &row[1];
            if b.is_null() {
                return Ok(a.clone());
            }
            Ok(if compare_values(a, b) == Ordering::Equal {
                Value::Null
            } else {
                a.clone()
            })
        }
        ScalarFunc::Greatest | ScalarFunc::Least => {
            let want_greatest = func == ScalarFunc::Greatest;
            let mut best: Option<&Value> = None;
            for v in row {
                if v.is_null() {
                    continue;
                }
                best = Some(match best {
                    None => v,
                    Some(b) => {
                        // DuckDB: NaN is greater than every other value
                        // (including Inf), and equal to NaN.
                        let v_nan = matches!(v, Value::Double(x) if x.is_nan());
                        let b_nan = matches!(b, Value::Double(x) if x.is_nan());
                        let take_v = if v_nan {
                            want_greatest
                        } else if b_nan {
                            !want_greatest
                        } else {
                            let ord = compare_values(b, v);
                            if want_greatest {
                                ord == Ordering::Less
                            } else {
                                ord == Ordering::Greater
                            }
                        };
                        if take_v { v } else { b }
                    }
                });
            }
            Ok(best.cloned().unwrap_or(Value::Null))
        }
        _ => unreachable!("strict scalar function reached the loose path"),
    }
}

/// DECIMAL `round(d[, n])` under DuckDB's result-type rules: `n >= scale`
/// keeps the scale, `0 <= n < scale` narrows to `n`, `n < 0` returns
/// `DECIMAL(p, 0)` rounded at `10^-n`.
fn decimal_round(x: i128, scale: u8, digits: i64, precision: u8) -> Result<Value> {
    let out_of_range = || {
        overflow_err(
            "round",
            "DECIMAL",
            format_args!("round of {}", format_decimal(x, scale)),
        )
    };
    if digits >= scale as i64 {
        return Ok(Value::Decimal(x, scale));
    }
    if digits >= 0 {
        let want = digits as u8;
        let y = rescale_decimal(x, scale, want).ok_or_else(out_of_range)?;
        return Ok(Value::Decimal(y, want));
    }
    let drop = digits.unsigned_abs();
    if drop > 38 {
        return Ok(Value::Decimal(0, 0));
    }
    let unit = POW10_I128[drop as usize];
    // Round x / unit half away from zero, then scale back up. `unit` is
    // 10^(scale + drop) in unscaled terms.
    let divisor = unit
        .checked_mul(POW10_I128[scale as usize])
        .ok_or_else(out_of_range)?;
    let q = div_round_half_away(x, divisor);
    let v = q
        .checked_mul(divisor)
        .filter(|v| decimal_fits(*v, precision));
    v.map(|v| Value::Decimal(v, 0)).ok_or_else(out_of_range)
}

/// Half-away-from-zero integer division (positive divisor).
fn div_round_half_away(v: i128, d: i128) -> i128 {
    debug_assert!(d > 0);
    if v >= 0 {
        let (q, r) = (v / d, v % d);
        if r * 2 >= d { q + 1 } else { q }
    } else {
        -div_round_half_away(-v, d)
    }
}

/// The subscript of `a[i]`, as a 1-based index. Only integral numbers index an
/// ARRAY — DuckDB refuses `a[1.5]` and so does this.
fn array_index(v: &Value) -> Result<Option<i64>> {
    match v {
        Value::Integer(i) => Ok(Some(*i as i64)),
        Value::Bigint(i) => Ok(Some(*i)),
        Value::Double(x) if x.fract() == 0.0 => Ok(Some(*x as i64)),
        Value::Null => Ok(None),
        v => Err(PinkDbError::Execution(format!(
            "an ARRAY subscript must be an integer, not {v}"
        ))),
    }
}

fn apply_scalar(func: ScalarFunc, dtype: DataType, row: &[Value]) -> Result<Value> {
    // DECIMAL keeps the numeric family (ABS/ROUND/FLOOR/CEIL/SIGN).
    if matches!(row[0], Value::Decimal(..)) {
        if let Some(v) = decimal_scalar(func, dtype, &row[0])? {
            return Ok(v);
        }
        if func == ScalarFunc::Round {
            let Value::Decimal(x, scale) = row[0] else {
                unreachable!("checked above")
            };
            let DataType::Decimal { precision, .. } = dtype else {
                unreachable!("decimal round has a decimal result type")
            };
            let digits = if row.len() > 1 { as_i64(&row[1])? } else { 0 };
            return decimal_round(x, scale, digits, precision);
        }
    }
    match func {
        // Not strict, so this only runs for a function reached through a path
        // that skipped `apply_scalar_loose`.
        ScalarFunc::ArrayValue => apply_scalar_loose(func, dtype, row),
        ScalarFunc::Abs => match &row[0] {
            Value::Integer(i) => i
                .checked_abs()
                .map(Value::Integer)
                .ok_or_else(|| overflow_err("abs", "INTEGER", format_args!("abs({i})"))),
            Value::Bigint(i) => i
                .checked_abs()
                .map(Value::Bigint)
                .ok_or_else(|| overflow_err("abs", "BIGINT", format_args!("abs({i})"))),
            Value::Double(x) => Ok(Value::Double(x.abs())),
            v => Err(PinkDbError::Execution(format!("abs of {v}"))),
        },
        ScalarFunc::Floor | ScalarFunc::Ceil => {
            // Non-decimal numerics keep the historical DOUBLE result.
            let x = as_f64(&row[0])?;
            Ok(Value::Double(if func == ScalarFunc::Floor {
                x.floor()
            } else {
                x.ceil()
            }))
        }
        ScalarFunc::Round => {
            let x = as_f64(&row[0])?;
            let digits = if row.len() > 1 { as_i64(&row[1])? } else { 0 };
            let factor = 10f64.powi(digits.clamp(-308, 308) as i32);
            Ok(Value::Double((x * factor).round() / factor))
        }
        ScalarFunc::Upper => match &row[0] {
            Value::Varchar(s) => Ok(Value::Varchar(s.to_uppercase())),
            v => Err(PinkDbError::Execution(format!("upper of {v}"))),
        },
        ScalarFunc::Lower => match &row[0] {
            Value::Varchar(s) => Ok(Value::Varchar(s.to_lowercase())),
            v => Err(PinkDbError::Execution(format!("lower of {v}"))),
        },
        ScalarFunc::Length => match &row[0] {
            Value::Varchar(s) => Ok(Value::Integer(s.chars().count() as i32)),
            Value::Array(a) => Ok(Value::Integer(a.len() as i32)),
            v => Err(PinkDbError::Execution(format!("length of {v}"))),
        },
        ScalarFunc::ArrayLength => match &row[0] {
            Value::Array(a) => Ok(Value::Integer(a.len() as i32)),
            v => Err(PinkDbError::Execution(format!("array_length of {v}"))),
        },
        ScalarFunc::ArrayExtract => match &row[0] {
            Value::Array(a) => Ok(match array_index(&row[1])? {
                Some(i) => a.elem_at(i),
                None => Value::Null,
            }),
            v => Err(PinkDbError::Execution(format!("array_extract of {v}"))),
        },
        ScalarFunc::ArrayDistance
        | ScalarFunc::ArrayCosineDistance
        | ScalarFunc::ArrayCosineSimilarity
        | ScalarFunc::ArrayInnerProduct
        | ScalarFunc::ArrayNegativeInnerProduct => match (&row[0], &row[1]) {
            (Value::Array(a), Value::Array(b)) => distance_value(func, a, b),
            (a, b) => Err(PinkDbError::Execution(format!(
                "{}: Arguments must be arrays of FLOAT or DOUBLE, got {a} and {b}",
                func.sql_name()
            ))),
        },
        ScalarFunc::Year | ScalarFunc::Month | ScalarFunc::Day => {
            let days = match &row[0] {
                Value::Date(d) => *d,
                Value::Timestamp(us) => crate::types::split_timestamp(*us).0,
                v => return Err(PinkDbError::Execution(format!("date part of {v}"))),
            };
            let (y, m, d) = crate::types::civil_from_days(days);
            Ok(Value::Integer(match func {
                ScalarFunc::Year => y,
                ScalarFunc::Month => m as i32,
                _ => d as i32,
            }))
        }
        ScalarFunc::Hour | ScalarFunc::Minute | ScalarFunc::Second => {
            let (_, h, mi, s, _) = match &row[0] {
                Value::Timestamp(us) => crate::types::split_timestamp(*us),
                v => return Err(PinkDbError::Execution(format!("time part of {v}"))),
            };
            Ok(Value::Integer(match func {
                ScalarFunc::Hour => h as i32,
                ScalarFunc::Minute => mi as i32,
                _ => s as i32,
            }))
        }
        ScalarFunc::DateTrunc => {
            let part = match &row[0] {
                Value::Varchar(s) => s.to_ascii_lowercase(),
                v => return Err(PinkDbError::Execution(format!("date_trunc part {v}"))),
            };
            match &row[1] {
                Value::Date(d) => {
                    let (y, m, _) = crate::types::civil_from_days(*d);
                    let trunc = match part.as_str() {
                        "year" => crate::types::days_from_civil(y, 1, 1),
                        "month" => crate::types::days_from_civil(y, m, 1),
                        "day" => *d,
                        _ => {
                            return Err(PinkDbError::Execution(format!(
                                "date_trunc: bad part \"{part}\" for DATE (want year|month|day)"
                            )));
                        }
                    };
                    Ok(Value::Date(trunc))
                }
                Value::Timestamp(us) => {
                    let (days, h, mi, s, _) = crate::types::split_timestamp(*us);
                    let day_us = days as i64 * 86_400_000_000;
                    let trunc = match part.as_str() {
                        "year" => {
                            let (y, _, _) = crate::types::civil_from_days(days);
                            crate::types::days_from_civil(y, 1, 1) as i64 * 86_400_000_000
                        }
                        "month" => {
                            let (y, m, _) = crate::types::civil_from_days(days);
                            crate::types::days_from_civil(y, m, 1) as i64 * 86_400_000_000
                        }
                        "day" => day_us,
                        "hour" => day_us + h as i64 * 3_600_000_000,
                        "minute" => day_us + h as i64 * 3_600_000_000 + mi as i64 * 60_000_000,
                        "second" => {
                            day_us
                                + h as i64 * 3_600_000_000
                                + mi as i64 * 60_000_000
                                + s as i64 * 1_000_000
                        }
                        _ => {
                            return Err(PinkDbError::Execution(format!(
                                "date_trunc: bad part \"{part}\" (want year|month|day|hour|minute|second)"
                            )));
                        }
                    };
                    Ok(Value::Timestamp(trunc))
                }
                v => Err(PinkDbError::Execution(format!("date_trunc of {v}"))),
            }
        }
        ScalarFunc::Substr => {
            let s = match &row[0] {
                Value::Varchar(s) => s.as_str(),
                v => return Err(PinkDbError::Execution(format!("substr of {v}"))),
            };
            let offset = as_i64(&row[1])?;
            let length = match row.get(2) {
                Some(l) => as_i64(l)?,
                None => SUBSTR_TO_END,
            };
            Ok(Value::Varchar(substr_slice(s, offset, length)?.to_string()))
        }
        ScalarFunc::Trim | ScalarFunc::LTrim | ScalarFunc::RTrim => {
            let s = match &row[0] {
                Value::Varchar(s) => s.as_str(),
                v => return Err(PinkDbError::Execution(format!("{func:?} of {v}"))),
            };
            let chars = match row.get(1) {
                None => None,
                Some(Value::Varchar(c)) => Some(c.as_str()),
                Some(v) => return Err(PinkDbError::Execution(format!("{func:?} chars {v}"))),
            };
            // DuckDB: the default trims ASCII spaces only; a second argument
            // is a set of characters (empty set ⇒ no-op).
            let pred = |c: char| match chars {
                None => c == ' ',
                Some(set) => set.contains(c),
            };
            let out = match func {
                ScalarFunc::LTrim => s.trim_start_matches(pred),
                ScalarFunc::RTrim => s.trim_end_matches(pred),
                _ => s.trim_matches(pred),
            };
            Ok(Value::Varchar(out.to_string()))
        }
        ScalarFunc::Replace => {
            let (s, from, to) = match (&row[0], &row[1], &row[2]) {
                (Value::Varchar(s), Value::Varchar(f), Value::Varchar(t)) => (s, f, t),
                (a, b, c) => {
                    return Err(PinkDbError::Execution(format!("replace of {a}, {b}, {c}")));
                }
            };
            // DuckDB: an empty search string is a no-op (Rust's `replace`
            // would insert the replacement between every character).
            if from.is_empty() {
                return Ok(Value::Varchar(s.clone()));
            }
            Ok(Value::Varchar(s.replace(from.as_str(), to)))
        }
        ScalarFunc::Extract => {
            let part = match &row[0] {
                Value::Varchar(s) => s.clone(),
                v => return Err(PinkDbError::Execution(format!("extract part {v}"))),
            };
            let field = crate::types::extract_field(&part).map_err(PinkDbError::Execution)?;
            let v = extract_value(field, &row[1])?;
            // A non-constant part hits DuckDB's VARCHAR overload, which
            // returns DOUBLE (the BIGINT kernel result widened).
            Ok(match (dtype, v) {
                (DataType::Double, Value::Bigint(x)) => Value::Double(x as f64),
                (_, v) => v,
            })
        }
        ScalarFunc::Sqrt => Ok(Value::Double(as_f64(&row[0])?.sqrt())),
        ScalarFunc::Sign => match &row[0] {
            Value::Integer(i) => Ok(Value::Integer(i.signum())),
            Value::Bigint(i) => Ok(Value::Integer(i.signum() as i32)),
            // DuckDB: sign(NaN) = 0, sign(±0.0) = 0.
            Value::Double(x) => Ok(Value::Integer(if x.is_nan() {
                0
            } else if *x > 0.0 {
                1
            } else if *x < 0.0 {
                -1
            } else {
                0
            })),
            v => Err(PinkDbError::Execution(format!("sign of {v}"))),
        },
        ScalarFunc::Power => Ok(Value::Double(as_f64(&row[0])?.powf(as_f64(&row[1])?))),
        // `v['a']`: the member with that name. A miss (absent key, or a base
        // that is not an object) is SQL NULL — DuckDB agrees.
        ScalarFunc::VariantField => match (&row[0], &row[1]) {
            (Value::Variant(v), Value::Varchar(key)) => Ok(match v.object_get(key) {
                Some(x) => Value::Variant(std::sync::Arc::new(x.clone())),
                None => Value::Null,
            }),
            (v, key) => Err(PinkDbError::Execution(format!(
                "a VARIANT field of {v} with key {key}"
            ))),
        },
        // `v[1]`: the element at a 1-based index (DuckDB's convention).
        ScalarFunc::VariantIndex => match (&row[0], &row[1]) {
            (Value::Variant(v), idx) => {
                let i = as_i64(idx)?;
                if i == 0 {
                    return Err(PinkDbError::Execution(
                        "Extracting index 0 from VARIANT(ARRAY) is invalid, indexes are 1-based"
                            .to_string(),
                    ));
                }
                if i < 0 {
                    return Err(PinkDbError::Execution(format!(
                        "a VARIANT index must be positive, got {i}"
                    )));
                }
                Ok(match v.array_get(i as usize - 1) {
                    Some(x) => Value::Variant(std::sync::Arc::new(x.clone())),
                    None => Value::Null,
                })
            }
            (v, idx) => Err(PinkDbError::Execution(format!(
                "a VARIANT index of {v} by {idx}"
            ))),
        },
        // `s[from:to]`: 1-based, inclusive, negative counts from the end.
        ScalarFunc::StringSlice => match &row[0] {
            Value::Varchar(text) => {
                let chars: Vec<char> = text.chars().collect();
                let from = as_i64(&row[1])?;
                let to = as_i64(&row[2])?;
                Ok(Value::Varchar(string_slice(&chars, from, to)))
            }
            v => Err(PinkDbError::Execution(format!("slicing {v}"))),
        },
        // `TRY_CAST(x AS t)`: a NULL instead of an error.
        ScalarFunc::TryCast => match cast_value(row[0].clone(), dtype) {
            Ok(v) => Ok(v),
            Err(_) => Ok(Value::Null),
        },
        ScalarFunc::LikeEscape => match (&row[0], &row[1], &row[2]) {
            (Value::Varchar(s), Value::Varchar(pat), Value::Varchar(esc)) => {
                let escape = escape_char(esc)?;
                Ok(Value::Boolean(like_match_escape(s, pat, escape)))
            }
            (s, pat, esc) => Err(PinkDbError::Execution(format!(
                "like_escape of {s}, {pat} and {esc}"
            ))),
        },
        ScalarFunc::Contains => match (&row[0], &row[1]) {
            (Value::Varchar(s), Value::Varchar(search)) => {
                Ok(Value::Boolean(s.contains(search.as_str())))
            }
            (s, search) => Err(PinkDbError::Execution(format!(
                "contains of {s} and {search}"
            ))),
        },
        ScalarFunc::Uuid => Err(PinkDbError::Execution(
            "uuid() is evaluated per chunk".to_string(),
        )),
        ScalarFunc::SimilarTo => match (&row[0], &row[1]) {
            (Value::Varchar(s), Value::Varchar(pat)) => {
                Ok(Value::Boolean(crate::regex::full_match(pat, s)?))
            }
            (s, pat) => Err(PinkDbError::Execution(format!(
                "SIMILAR TO of {s} and {pat}"
            ))),
        },
        ScalarFunc::VariantExtract => match (&row[0], &row[1]) {
            (Value::Variant(v), Value::Varchar(path)) => {
                let steps = crate::variant::parse_path(path)?;
                Ok(match crate::variant::walk(v, &steps) {
                    Some(x) => Value::Variant(std::sync::Arc::new(x.clone())),
                    // The path does not exist: SQL NULL, not a variant null
                    // (DuckDB agrees; `variant_type` of the result is NULL).
                    None => Value::Null,
                })
            }
            (v, path) => Err(PinkDbError::Execution(format!(
                "variant_extract of {v} and {path}"
            ))),
        },
        ScalarFunc::Mod => match (&row[0], &row[1]) {
            (Value::Double(a), Value::Double(b)) => {
                if *b == 0.0 {
                    return Err(PinkDbError::Execution("mod by zero".to_string()));
                }
                Ok(Value::Double(a % b))
            }
            (Value::Bigint(a), Value::Bigint(b)) => {
                if *b == 0 {
                    return Err(PinkDbError::Execution("mod by zero".to_string()));
                }
                Ok(Value::Bigint(a.wrapping_rem(*b)))
            }
            (Value::Integer(a), Value::Integer(b)) => {
                if *b == 0 {
                    return Err(PinkDbError::Execution("mod by zero".to_string()));
                }
                Ok(Value::Integer(a.wrapping_rem(*b)))
            }
            (a, b) => Err(PinkDbError::Execution(format!("mod of {a} and {b}"))),
        },
        ScalarFunc::JsonSet
        | ScalarFunc::JsonInsert
        | ScalarFunc::JsonReplace
        | ScalarFunc::JsonRemove => {
            let mode = match func {
                ScalarFunc::JsonInsert => crate::variant::SetMode::Insert,
                ScalarFunc::JsonReplace => crate::variant::SetMode::Replace,
                _ => crate::variant::SetMode::Set,
            };
            let as_variant = matches!(&row[0], Value::Variant(_));
            let doc = match &row[0] {
                Value::Variant(v) => (**v).clone(),
                Value::Varchar(text) => crate::variant::from_json_str(text)?,
                v => {
                    return Err(PinkDbError::Execution(format!(
                        "a JSON document must be VARCHAR or VARIANT, not {v}"
                    )));
                }
            };
            let Value::Varchar(path) = &row[1] else {
                return Err(PinkDbError::Execution(
                    "a JSON path must be VARCHAR".to_string(),
                ));
            };
            let steps = crate::variant::parse_path(path)?;
            let out = if func == ScalarFunc::JsonRemove {
                if steps.is_empty() {
                    // `$` removes the whole document, which DuckDB answers
                    // with SQL NULL rather than a variant null.
                    return Ok(Value::Null);
                }
                crate::variant::remove_path(&doc, &steps)?
            } else {
                let value = crate::variant::from_value(&row[2])?;
                crate::variant::set_path(&doc, &steps, &value, mode)?
            };
            Ok(if as_variant {
                Value::Variant(std::sync::Arc::new(out))
            } else {
                Value::Varchar(out.to_json())
            })
        }
        ScalarFunc::Concat | ScalarFunc::NullIf | ScalarFunc::Greatest | ScalarFunc::Least => {
            unreachable!("non-strict scalar reached the strict path")
        }
        ScalarFunc::TypeOf | ScalarFunc::VariantType | ScalarFunc::VariantKeys => {
            unreachable!("evaluated by eval_scalar_typed")
        }
    }
}

/// One `extract`/`date_part` cell: the field of a DATE (time = midnight) or
/// TIMESTAMP value. Only the fields in [`ExtractField`] are handled; DuckDB's
/// larger specifier set (julian, era, timezone, ...) is out of v1 scope.
fn extract_value(field: crate::types::ExtractField, v: &Value) -> Result<Value> {
    use crate::types::{ExtractField as F, civil_from_days, days_from_civil, split_timestamp};
    let (days, h, mi, s, micro) = match v {
        Value::Date(d) => (*d, 0u32, 0u32, 0u32, 0u32),
        Value::Timestamp(us) => split_timestamp(*us),
        v => return Err(PinkDbError::Execution(format!("extract of {v}"))),
    };
    let (y, m, d) = civil_from_days(days);
    let big = |x: i64| Ok(Value::Bigint(x));
    match field {
        F::Year => big(y as i64),
        F::Month => big(m as i64),
        F::Day => big(d as i64),
        F::Hour => big(h as i64),
        F::Minute => big(mi as i64),
        F::Second => big(s as i64),
        F::Millisecond => big(s as i64 * 1000 + micro as i64 / 1000),
        F::Microsecond => big(s as i64 * 1_000_000 + micro as i64),
        F::Epoch => match v {
            Value::Date(d) => Ok(Value::Double(*d as f64 * 86_400.0)),
            Value::Timestamp(us) => Ok(Value::Double(*us as f64 / 1_000_000.0)),
            _ => unreachable!("dates/timestamps only"),
        },
        // 1970-01-01 was a Thursday: 0 = Sunday .. 6 = Saturday.
        F::DayOfWeek => big((days as i64 + 4).rem_euclid(7)),
        // ISO: 1 = Monday .. 7 = Sunday.
        F::IsoDayOfWeek => big((days as i64 + 3).rem_euclid(7) + 1),
        F::DayOfYear => big((days - days_from_civil(y, 1, 1)) as i64 + 1),
        F::Week => big(iso_week(days).0),
        F::Quarter => big(((m - 1) / 3 + 1) as i64),
        F::IsoYear => big(iso_week(days).1),
        F::Century => big((y as i64 + 99).div_euclid(100)),
        F::Decade => big((y as i64).div_euclid(10)),
        F::Millennium => big((y as i64 + 999).div_euclid(1000)),
    }
}

/// ISO 8601 week and week-year for a civil date: week 1 is the week of
/// Jan 4 (the Monday-based week containing the year's first Thursday).
fn iso_week(days: i32) -> (i64, i64) {
    let monday_of_week_1 = |year: i32| {
        // Weekday with Monday = 0 .. Sunday = 6.
        let jan4 = crate::types::days_from_civil(year, 1, 4);
        jan4 - (jan4 + 3).rem_euclid(7)
    };
    let (mut year, _, _) = crate::types::civil_from_days(days);
    let mut w1 = monday_of_week_1(year);
    if days < w1 {
        year -= 1;
        w1 = monday_of_week_1(year);
    } else if days >= monday_of_week_1(year + 1) {
        year += 1;
        w1 = monday_of_week_1(year);
    }
    (((days - w1) / 7 + 1) as i64, year as i64)
}

/// `a || b`: strict concatenation (NULL if either side is NULL). The binder
/// casts both operands to VARCHAR, so payloads are VARCHAR (or a bare NULL).
fn eval_concat(left: &BoundExpr, right: &BoundExpr, chunk: &DataChunk, cx: &Cx) -> Result<Vector> {
    let l = eval_expr(left, chunk, cx)?;
    let r = eval_expr(right, chunk, cx)?;
    let mut out = Vector::new_empty(DataType::Varchar);
    for i in 0..chunk.num_rows() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
        match (l.get(i), r.get(i)) {
            (Value::Varchar(a), Value::Varchar(b)) => out.push_str(&format!("{a}{b}"))?,
            (a, b) => {
                return Err(PinkDbError::Execution(format!(
                    "cannot concatenate {a} and {b} (use CAST)"
                )));
            }
        }
    }
    Ok(out)
}

/// `LIKE` / `NOT LIKE` with three-valued logic (`%` = any run, `_` = one
/// char; no ESCAPE clause here — backslash is literal; `like_escape` takes
/// one). Reads the string payloads in place and compiles a constant pattern
/// once per chunk.
fn eval_like(
    op: BinaryOp,
    left: &BoundExpr,
    right: &BoundExpr,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    let l = eval_expr(left, chunk, cx)?;
    let r = eval_expr(right, chunk, cx)?;
    let negate = op == BinaryOp::NotLike;
    let (VectorData::VarChar(ls), VectorData::VarChar(ps)) = (l.data(), r.data()) else {
        // A NULL literal on either side (typed INTEGER) is NULL throughout;
        // anything else is a type error.
        let mut out = Vector::new_empty(DataType::Boolean);
        for i in 0..l.len() {
            if !(l.is_null(i) || r.is_null(i)) {
                return Err(PinkDbError::Execution(
                    "LIKE needs VARCHAR on both sides".to_string(),
                ));
            }
            out.push_null()?;
        }
        return Ok(out);
    };
    let mut validity = l.validity().to_vec();
    for (w, m) in validity.iter_mut().zip(r.validity()) {
        *w &= m;
    }
    let constant = match right {
        BoundExpr::Literal(Value::Varchar(p)) => Some(LikeMatcher::new(p, None)),
        _ => None,
    };
    let mut out = Vec::with_capacity(ls.len());
    for (i, s) in ls.iter().enumerate() {
        let hit = if validity[i / 64] >> (i % 64) & 1 == 0 {
            false
        } else {
            match &constant {
                Some(m) => m.matches(s),
                None => LikeMatcher::new(&ps[i], None).matches(s),
            }
        };
        out.push(hit != negate);
    }
    Ok(Vector::from_parts(
        VectorData::Boolean(out),
        validity,
        ls.len(),
    ))
}

/// One pattern atom: a literal char, "any char" (`_`), or "any run" (`%`).
#[derive(Clone, Copy, PartialEq)]
enum LikeAtom {
    Any,
    Lit(char),
    Many,
}

/// A compiled `LIKE` pattern.
enum LikeMatcher {
    /// Only `%` and literal text: DuckDB's `LikeMatcher`
    /// (src/function/scalar/string/like.cpp). The first segment is a prefix
    /// unless the pattern starts with `%`, the last a suffix unless it ends
    /// with `%`, and the segments between are found in order with a
    /// substring search — no per-character backtracking.
    Segments {
        segments: Vec<String>,
        start_any: bool,
        end_any: bool,
    },
    /// Anything else (`_`, an escape, an empty or all-`%` pattern): the
    /// wildcard matcher over atoms, walking the string in place.
    Atoms(Vec<LikeAtom>),
}

impl LikeMatcher {
    /// Compile `pat`; `escape` makes the next char literal (a trailing
    /// escape is a literal escape).
    fn new(pat: &str, escape: Option<char>) -> LikeMatcher {
        let simple = escape.is_none() && !pat.contains('_');
        let segments: Vec<String> = pat
            .split('%')
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if simple && !segments.is_empty() {
            return LikeMatcher::Segments {
                segments,
                start_any: pat.starts_with('%'),
                end_any: pat.ends_with('%'),
            };
        }
        let chars: Vec<char> = pat.chars().collect();
        let mut atoms = Vec::with_capacity(chars.len());
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            if Some(c) == escape {
                atoms.push(LikeAtom::Lit(chars.get(i + 1).copied().unwrap_or(c)));
                i += 2;
                continue;
            }
            atoms.push(match c {
                '_' => LikeAtom::Any,
                '%' => LikeAtom::Many,
                c => LikeAtom::Lit(c),
            });
            i += 1;
        }
        LikeMatcher::Atoms(atoms)
    }

    fn matches(&self, s: &str) -> bool {
        match self {
            LikeMatcher::Segments {
                segments,
                start_any,
                end_any,
            } => {
                let mut rest = s;
                let mut middle = &segments[..];
                if !start_any {
                    let Some(r) = rest.strip_prefix(segments[0].as_str()) else {
                        return false;
                    };
                    if segments.len() == 1 {
                        return *end_any || r.is_empty();
                    }
                    rest = r;
                    middle = &middle[1..];
                }
                let (last, middle) = middle.split_last().expect("at least one segment");
                for seg in middle {
                    match find_segment(rest, seg) {
                        Some(at) => rest = &rest[at + seg.len()..],
                        None => return false,
                    }
                }
                if *end_any {
                    find_segment(rest, last).is_some()
                } else {
                    rest.ends_with(last.as_str())
                }
            }
            LikeMatcher::Atoms(p) => {
                // Iterative backtracking: `star` is the last `%` seen and
                // `mark` the byte where its current run ends.
                let (mut si, mut pi) = (0usize, 0usize);
                let (mut star, mut mark) = (None, 0usize);
                while si < s.len() {
                    let c = s[si..].chars().next().expect("si is a char boundary");
                    match p.get(pi) {
                        Some(LikeAtom::Any) => {
                            si += c.len_utf8();
                            pi += 1;
                        }
                        Some(LikeAtom::Lit(x)) if *x == c => {
                            si += c.len_utf8();
                            pi += 1;
                        }
                        Some(LikeAtom::Many) => {
                            star = Some(pi);
                            mark = si;
                            pi += 1;
                        }
                        _ => match star {
                            Some(st) => {
                                pi = st + 1;
                                mark += s[mark..].chars().next().map_or(1, char::len_utf8);
                                si = mark;
                            }
                            None => return false,
                        },
                    }
                }
                while p.get(pi) == Some(&LikeAtom::Many) {
                    pi += 1;
                }
                pi == p.len()
            }
        }
    }
}

/// How common an ASCII byte is in text, lower = rarer (letters by English
/// frequency, then digits and punctuation as rare). memchr's `memmem` picks
/// the rarest needle byte to scan for the same way.
fn byte_rank(b: u8) -> u8 {
    const COMMON: &[u8] = b" etaoinshrdlcumwfgypbvkjxqz";
    let lower = b.to_ascii_lowercase();
    match COMMON.iter().position(|&c| c == lower) {
        Some(i) => 100 - i as u8,
        None => 0,
    }
}

/// Byte offset of `needle` in `hay`. `str::find(&str)` sets up a two-way
/// searcher on every call, which costs more than the search for the short
/// needles and strings `LIKE` sees row after row. For an ASCII needle, scan
/// for its rarest byte (`str::find(char)` uses the optimized byte search) and
/// compare the needle in place around each hit.
#[inline]
fn find_segment(hay: &str, needle: &str) -> Option<usize> {
    let nb = needle.as_bytes();
    if !needle.is_ascii() {
        return hay.find(needle);
    }
    let (k, &rare) = nb
        .iter()
        .enumerate()
        .min_by_key(|(_, b)| byte_rank(**b))
        .expect("segments are non-empty");
    let hb = hay.as_bytes();
    let mut from = k;
    while from < hb.len() {
        // `from` may fall inside a multi-byte char of a non-ASCII string;
        // the rare byte is ASCII, so it can only sit on a char boundary.
        while !hay.is_char_boundary(from) {
            from += 1;
        }
        let p = hay[from..].find(rare as char)?;
        let at = from + p - k;
        if hb[at..].starts_with(nb) {
            return Some(at);
        }
        from += p + 1;
    }
    None
}

/// `%`/`_` wildcard match with an optional escape char (`LIKE … ESCAPE '\'`).
/// The escape must be a single char or none; the binder checks that.
fn like_match_escape(s: &str, pat: &str, escape: Option<char>) -> bool {
    LikeMatcher::new(pat, escape).matches(s)
}

/// `s[from:to]` over chars: 1-based, inclusive, negative = from the end, and
/// clamped — an inverted or empty range is the empty string (DuckDB).
fn string_slice(chars: &[char], from: i64, to: i64) -> String {
    let n = chars.len() as i64;
    let resolve = |i: i64| if i < 0 { n + i + 1 } else { i };
    let start = resolve(from).max(1);
    let end = resolve(to).min(n);
    if end < start {
        return String::new();
    }
    chars[(start - 1) as usize..end as usize].iter().collect()
}

/// The `ESCAPE` char of `LIKE … ESCAPE`: none, or exactly one char.
fn escape_char(esc: &str) -> Result<Option<char>> {
    let mut chars = esc.chars();
    match (chars.next(), chars.next()) {
        (None, _) => Ok(None),
        (Some(c), None) => Ok(Some(c)),
        _ => Err(PinkDbError::Execution(
            "Invalid escape string. Escape string must be empty or one character.".to_string(),
        )),
    }
}

/// Elementwise `CAST` (NULL in → NULL out).
fn eval_cast(v: &Vector, target: DataType) -> Result<Vector> {
    if v.dtype() == target {
        return Ok(v.clone());
    }
    let mut out = Vector::new_empty(target);
    for i in 0..v.len() {
        if v.is_null(i) {
            out.push_null()?;
            continue;
        }
        out.push_valid(cast_value(v.get(i), target)?)?;
    }
    Ok(out)
}

/// Keep only rows where `pred` (Boolean) is TRUE. NULL/missing → dropped.
pub fn select_true(pred: &Vector) -> Result<Vec<usize>> {
    ensure_dtype(pred, DataType::Boolean, "WHERE")?;
    let bits = match pred.data() {
        VectorData::Boolean(b) => b,
        _ => unreachable!("BOOLEAN dtype implies BOOLEAN layout"),
    };
    let mut sel = Vec::new();
    for (i, &is_true) in bits.iter().enumerate() {
        // Validity bit + payload bit directly; no `Value` per row.
        if pred.is_valid(i) && is_true {
            sel.push(i);
        }
    }
    Ok(sel)
}

/// Number of rows where `pred` (Boolean) is TRUE. NULL/missing → not
/// counted. Same row set as [`select_true`] without building the vector.
pub fn count_true(pred: &Vector) -> Result<i64> {
    ensure_dtype(pred, DataType::Boolean, "WHERE")?;
    let bits = match pred.data() {
        VectorData::Boolean(b) => b,
        _ => unreachable!("BOOLEAN dtype implies BOOLEAN layout"),
    };
    let mut n = 0i64;
    for (i, &is_true) in bits.iter().enumerate() {
        // Validity bit + payload bit directly; no `Value` per row.
        if is_true && pred.is_valid(i) {
            n += 1;
        }
    }
    Ok(n)
}

/// Narrow `sel` (ascending rows of `chunk`) to the rows where `expr` is
/// TRUE, without materializing a filtered copy of the chunk.
///
/// The shapes with a typed kernel (DuckDB's `*Executor::Select`, see
/// `src/execution/expression_executor/execute_comparison.cpp`):
///
/// - `a AND b` narrows one selection in turn, so `b` — comparisons and all —
///   only looks at the rows `a` kept;
/// - `a OR b` evaluates both sides over the same rows and unions them;
/// - `column <op> literal` runs the monomorphic comparison kernel.
///
/// Anything else evaluates the expression over the whole chunk and walks its
/// BOOLEAN payload (`NULL` is not TRUE). Because conjuncts narrow, an error
/// inside a later conjunct (division by zero, DECIMAL overflow) is raised
/// only when a row that survived the earlier ones reaches it, and never for
/// an empty selection — the behaviour of DuckDB's conjunction path, which
/// also stops evaluating once nothing is selected.
pub(crate) fn select_predicate(
    expr: &BoundExpr,
    chunk: &DataChunk,
    cx: &Cx,
    sel: &mut super::selection::Selection,
) -> Result<()> {
    match expr {
        BoundExpr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => {
            select_predicate(left, chunk, cx, sel)?;
            if !sel.is_empty() {
                select_predicate(right, chunk, cx, sel)?;
            }
            Ok(())
        }
        BoundExpr::Binary {
            op: BinaryOp::Or,
            left,
            right,
            ..
        } => {
            // Only rows the left side did not settle as TRUE have to look at
            // the right one (DuckDB's `OrSelectOperation`): the result is the
            // same, and a right-side error stays limited to rows that reach
            // it, as a per-row evaluation would report.
            if let Some((operand, values)) = in_list_of(expr)
                && select_in_list(operand, &values, chunk, cx, sel)?
            {
                return Ok(());
            }
            let mut left_true = sel.clone();
            select_predicate(left, chunk, cx, &mut left_true)?;
            let rest = difference_selections(sel, &left_true);
            let mut right_true = rest;
            select_predicate(right, chunk, cx, &mut right_true)?;
            *sel = merge_selections(&left_true, &right_true);
            Ok(())
        }
        BoundExpr::Binary {
            op, left, right, ..
        } if is_ordering(*op) => {
            if let Some(kernel) = scalar_cmp_of(left, right, chunk) {
                kernel.retain(*op, sel);
                return Ok(());
            }
            if retain_column_pair(left, right, *op, chunk, sel) {
                return Ok(());
            }
            select_bool_vector(expr, chunk, cx, sel)
        }
        _ => select_bool_vector(expr, chunk, cx, sel),
    }
}

/// `column <op> literal` (either side, negated operator when the column is on
/// the right) when both sides have that shape.
fn scalar_cmp_of<'a>(
    left: &BoundExpr,
    right: &BoundExpr,
    chunk: &'a DataChunk,
) -> Option<ScalarCmp<'a>> {
    let (index, scalar, reversed) = match (left, right) {
        (BoundExpr::Literal(v), BoundExpr::Column { index, .. }) if !v.is_null() => {
            (index, v, true)
        }
        (BoundExpr::Column { index, .. }, BoundExpr::Literal(v)) if !v.is_null() => {
            (index, v, false)
        }
        _ => return None,
    };
    let col = match chunk.expect_column(*index) {
        Ok(col) => col,
        // A projection placeholder fails here and raises its own error when
        // the fallback evaluates the expression.
        Err(_) => return None,
    };
    ScalarCmp::new(col, scalar, reversed)
}

/// `e = v1 OR e = v2 OR …` with one shared non-literal operand `e` and
/// non-NULL literals of `e`'s own type — how the binder lowers
/// `e IN (v1, v2, …)` — as the operand and its values.
fn in_list_of(expr: &BoundExpr) -> Option<(&BoundExpr, Vec<&Value>)> {
    fn leaves<'a>(e: &'a BoundExpr, out: &mut Vec<&'a BoundExpr>) {
        match e {
            BoundExpr::Binary {
                op: BinaryOp::Or,
                left,
                right,
                ..
            } => {
                leaves(left, out);
                leaves(right, out);
            }
            other => out.push(other),
        }
    }
    let mut all = Vec::new();
    leaves(expr, &mut all);
    let mut operand: Option<&BoundExpr> = None;
    let mut values = Vec::with_capacity(all.len());
    for leaf in all {
        let BoundExpr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
            ..
        } = leaf
        else {
            return None;
        };
        let (e, v) = match (&**left, &**right) {
            (BoundExpr::Literal(_), BoundExpr::Literal(_)) => return None,
            (e, BoundExpr::Literal(v)) | (BoundExpr::Literal(v), e) => (e, v),
            _ => return None,
        };
        if v.is_null() || v.data_type() != Some(e.dtype()) {
            return None;
        }
        match operand {
            None => operand = Some(e),
            Some(o) if o == e => {}
            Some(_) => return None,
        }
        values.push(v);
    }
    (values.len() >= 2).then(|| (operand.expect("two leaves"), values))
}

/// Keep the rows of `sel` whose `operand` equals one of `values`: the
/// operand is evaluated once over the selected rows (an `IN` list lowered to
/// an `OR` chain otherwise evaluates it once per value — TPC-H Q22's
/// `substring(c_phone, 1, 2)` seven times) and each row is looked up in the
/// value list. `false` leaves the chain to the generic path (a payload with
/// no exact equality here).
fn select_in_list(
    operand: &BoundExpr,
    values: &[&Value],
    chunk: &DataChunk,
    cx: &Cx,
    sel: &mut Vec<u32>,
) -> Result<bool> {
    // A bare column is read in place, by chunk row; anything else is
    // evaluated once, over the whole chunk when nothing is filtered yet and
    // over the selected rows otherwise (row `k` = selection entry `k`).
    let column = match operand {
        BoundExpr::Column { index, .. } if chunk.is_present(*index) => {
            Some(chunk.expect_column(*index)?)
        }
        _ => None,
    };
    let whole = column.is_some() || sel.len() == chunk.num_rows();
    let owned;
    let v: &Vector = match column {
        Some(col) => col,
        None => {
            owned = if sel.len() == chunk.num_rows() {
                eval_expr(operand, chunk, cx)?
            } else {
                eval_expr(operand, &take_referenced(operand, chunk, sel), cx)?
            };
            &owned
        }
    };
    // Row `k` of `v` is selection entry `k` (or chunk row `k` when whole).
    fn keep(sel: &mut Vec<u32>, whole: bool, v: &Vector, test: impl Fn(usize) -> bool) {
        let mut w = 0;
        for k in 0..sel.len() {
            let row = sel[k];
            let i = if whole { row as usize } else { k };
            if v.is_valid(i) && test(i) {
                sel[w] = row;
                w += 1;
            }
        }
        sel.truncate(w);
    }
    match v.data() {
        VectorData::VarChar(d) => {
            let set: Vec<&str> = values
                .iter()
                .filter_map(|x| match x {
                    Value::Varchar(s) => Some(&**s),
                    _ => None,
                })
                .collect();
            keep(sel, whole, v, |i| set.contains(&&d[i]));
        }
        VectorData::Int32(d) | VectorData::Date32(d) => {
            let set: Vec<i32> = values
                .iter()
                .filter_map(|x| match x {
                    Value::Integer(n) | Value::Date(n) => Some(*n),
                    _ => None,
                })
                .collect();
            keep(sel, whole, v, |i| set.contains(&d[i]));
        }
        VectorData::Int64(d) | VectorData::TimestampMicros(d) => {
            let set: Vec<i64> = values
                .iter()
                .filter_map(|x| match x {
                    Value::Bigint(n) | Value::Timestamp(n) => Some(*n),
                    _ => None,
                })
                .collect();
            keep(sel, whole, v, |i| set.contains(&d[i]));
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// The rows `sel` of the columns `expr` reads, every other column an absent
/// placeholder: gathering the whole chunk to evaluate one expression over a
/// selection copies columns nothing reads.
fn take_referenced(expr: &BoundExpr, chunk: &DataChunk, sel: &[u32]) -> DataChunk {
    let mut cols = Vec::new();
    crate::optimizer::columns_in(expr, &mut cols);
    if cols.contains(&usize::MAX) {
        return chunk.take(sel);
    }
    let empty: std::sync::Arc<Vector> = std::sync::Arc::new(Vector::new_empty(DataType::Integer));
    let columns = chunk
        .columns
        .iter()
        .enumerate()
        .map(|(c, col)| {
            if cols.contains(&c) && chunk.is_present(c) {
                std::sync::Arc::new(col.take(sel))
            } else {
                std::sync::Arc::clone(&empty)
            }
        })
        .collect();
    DataChunk::projected(columns, sel.len())
}

/// `column <op> column` over two payloads of one ordered layout (TPC-H Q4's
/// `l_commitdate < l_receiptdate`): compared in place over the selection,
/// with the same order `eval_comparison` uses for the pair, instead of a
/// BOOLEAN vector over the rows the selection gathers. `false` for any
/// other shape.
fn retain_column_pair(
    left: &BoundExpr,
    right: &BoundExpr,
    op: BinaryOp,
    chunk: &DataChunk,
    sel: &mut Vec<u32>,
) -> bool {
    let (BoundExpr::Column { index: l, .. }, BoundExpr::Column { index: r, .. }) = (left, right)
    else {
        return false;
    };
    let (Ok(a), Ok(b)) = (chunk.expect_column(*l), chunk.expect_column(*r)) else {
        return false;
    };
    use VectorData as V;
    match (a.data(), b.data()) {
        (V::Int32(x), V::Int32(y)) | (V::Date32(x), V::Date32(y)) => {
            retain_pair(a, b, x, y, op, sel, |p, q| p.cmp(q))
        }
        (V::Int64(x), V::Int64(y)) | (V::TimestampMicros(x), V::TimestampMicros(y)) => {
            retain_pair(a, b, x, y, op, sel, |p, q| p.cmp(q))
        }
        (
            V::Decimal {
                values: x,
                scale: sx,
                ..
            },
            V::Decimal {
                values: y,
                scale: sy,
                ..
            },
        ) if sx == sy => retain_pair(a, b, x, y, op, sel, |p, q| p.cmp(q)),
        (V::VarChar(x), V::VarChar(y)) => {
            let (x, y): (Vec<&str>, Vec<&str>) = (x.iter().collect(), y.iter().collect());
            retain_pair(a, b, &x, &y, op, sel, |p, q| p.cmp(q))
        }
        _ => return false,
    }
    true
}

/// Keep the rows of `sel` where both cells are valid and `ord(x, y) <op>`
/// holds, the operator matched once.
fn retain_pair<T>(
    a: &Vector,
    b: &Vector,
    x: &[T],
    y: &[T],
    op: BinaryOp,
    sel: &mut Vec<u32>,
    ord: impl Fn(&T, &T) -> Ordering,
) {
    #[inline(always)]
    fn keep<T>(
        a: &Vector,
        b: &Vector,
        x: &[T],
        y: &[T],
        sel: &mut Vec<u32>,
        test: impl Fn(&T, &T) -> bool,
    ) {
        let nulls = a.has_nulls() || b.has_nulls();
        let mut w = 0;
        for r in 0..sel.len() {
            let row = sel[r];
            let i = row as usize;
            let ok = (!nulls || (a.is_valid(i) && b.is_valid(i))) && test(&x[i], &y[i]);
            sel[w] = row;
            w += usize::from(ok);
        }
        sel.truncate(w);
    }
    match op {
        BinaryOp::Eq => keep(a, b, x, y, sel, |p, q| ord(p, q) == Ordering::Equal),
        BinaryOp::NotEq => keep(a, b, x, y, sel, |p, q| ord(p, q) != Ordering::Equal),
        BinaryOp::Lt => keep(a, b, x, y, sel, |p, q| ord(p, q) == Ordering::Less),
        BinaryOp::LtEq => keep(a, b, x, y, sel, |p, q| ord(p, q) != Ordering::Greater),
        BinaryOp::Gt => keep(a, b, x, y, sel, |p, q| ord(p, q) == Ordering::Greater),
        BinaryOp::GtEq => keep(a, b, x, y, sel, |p, q| ord(p, q) != Ordering::Less),
        _ => unreachable!("retain_pair called with {op:?}"),
    }
}

/// The generic select: evaluate to BOOLEAN and intersect with its TRUE rows.
fn select_bool_vector(
    expr: &BoundExpr,
    chunk: &DataChunk,
    cx: &Cx,
    sel: &mut super::selection::Selection,
) -> Result<()> {
    let bits = bool_bits;
    // A conjunct this fallback covers evaluates over the rows still selected,
    // never the whole chunk: a row the narrowing already excluded must not pay
    // for the predicate, nor raise its error (DuckDB's `Select` sees the same
    // rows). An equal-length selection is the identity, so nothing to gather.
    if sel.len() == chunk.num_rows() {
        let pred = eval_expr(expr, chunk, cx)?;
        ensure_dtype(&pred, DataType::Boolean, "WHERE")?;
        let bits = bits(&pred);
        sel.retain(|&row| {
            let i = row as usize;
            // Validity bit + payload bit directly; no `Value` per row.
            pred.is_valid(i) && bits[i]
        });
        return Ok(());
    }
    let rows = std::mem::take(sel);
    let sub = chunk.take(&rows);
    let pred = eval_expr(expr, &sub, cx)?;
    ensure_dtype(&pred, DataType::Boolean, "WHERE")?;
    let bits = bits(&pred);
    let mut out = Vec::with_capacity(rows.len());
    for (k, &row) in rows.iter().enumerate() {
        if pred.is_valid(k) && bits[k] {
            out.push(row);
        }
    }
    *sel = out;
    Ok(())
}

/// The payload of a BOOLEAN vector.
fn bool_bits(v: &Vector) -> &[bool] {
    match v.data() {
        VectorData::Boolean(b) => b,
        _ => unreachable!("BOOLEAN dtype implies BOOLEAN layout"),
    }
}

/// `a` without the rows of `b`, both ascending selections of the same chunk.
fn difference_selections(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len().saturating_sub(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() {
        while j < b.len() && b[j] < a[i] {
            j += 1;
        }
        if j == b.len() || b[j] != a[i] {
            out.push(a[i]);
        }
        i += 1;
    }
    out
}

/// Union of two ascending selections of the same rows.
fn merge_selections(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

/// True for the six operators `ord_matches` answers.
fn is_ordering(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
    )
}

fn cast_out_of_range(v: &Value, target: DataType) -> PinkDbError {
    PinkDbError::Execution(format!("cannot CAST {v} AS {target}: value out of range"))
}

/// Convert one non-NULL value to `target` (CAST semantics; NULL is handled by
/// callers). Shared with the prepared-statement bind step, which converts a
/// bound parameter to its inferred slot type.
pub(crate) fn cast_value(v: Value, target: DataType) -> Result<Value> {
    match (v, target) {
        (v, t) if v.data_type() == Some(t) => Ok(v),
        // A NULL literal has no type of its own, so it casts to anything.
        (Value::Null, _) => Ok(Value::Null),
        // Every value becomes a VARIANT (VARCHAR parses as JSON text), and a
        // VARIANT converts to whatever kind its target can read — a kind
        // mismatch is an error here, which TRY_CAST turns into NULL.
        (v, DataType::Variant) => {
            crate::variant::from_value(&v).map(|x| Value::Variant(std::sync::Arc::new(x)))
        }
        (Value::Variant(x), t) => crate::variant::cast(&x, t),
        // Numeric tower (DuckDB rules): narrowing out of range is an error,
        // and float→int rounds half away from zero (NaN/±Inf error).
        (Value::Integer(i), DataType::Bigint) => Ok(Value::Bigint(i as i64)),
        (Value::Integer(i), DataType::Double) => Ok(Value::Double(i as f64)),
        (Value::Bigint(i), DataType::Double) => Ok(Value::Double(i as f64)),
        (Value::Decimal(x, from), DataType::Double) => {
            Ok(Value::Double(crate::types::decimal_to_f64(x, from)))
        }
        (Value::Bigint(i), DataType::Integer) => i32::try_from(i)
            .map(Value::Integer)
            .map_err(|_| cast_out_of_range(&Value::Bigint(i), DataType::Integer)),
        (Value::Double(x), DataType::Integer) => {
            let r = x.round();
            if (i32::MIN as f64..=i32::MAX as f64).contains(&r) {
                Ok(Value::Integer(r as i32))
            } else {
                Err(cast_out_of_range(&Value::Double(x), DataType::Integer))
            }
        }
        (Value::Double(x), DataType::Bigint) => {
            let r = x.round();
            // i64::MAX is not representable in f64; 2^63 is the first value
            // out of range.
            if (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&r) {
                Ok(Value::Bigint(r as i64))
            } else {
                Err(cast_out_of_range(&Value::Double(x), DataType::Bigint))
            }
        }
        (Value::Double(x), DataType::Double) => Ok(Value::Double(x)),
        // Boolean ↔ numeric.
        (Value::Boolean(b), DataType::Integer) => Ok(Value::Integer(b as i32)),
        (Value::Boolean(b), DataType::Bigint) => Ok(Value::Bigint(b as i64)),
        (Value::Boolean(b), DataType::Double) => Ok(Value::Double(if b { 1.0 } else { 0.0 })),
        (Value::Integer(i), DataType::Boolean) => Ok(Value::Boolean(i != 0)),
        (Value::Bigint(i), DataType::Boolean) => Ok(Value::Boolean(i != 0)),
        (Value::Double(x), DataType::Boolean) => Ok(Value::Boolean(x != 0.0)),
        // Everything formats to VARCHAR.
        (v, DataType::Varchar) => Ok(Value::Varchar(v.to_string())),
        // VARCHAR parses (strict; bad input errors per row).
        (Value::Varchar(s), DataType::Boolean) => match s.to_ascii_lowercase().as_str() {
            "true" => Ok(Value::Boolean(true)),
            "false" => Ok(Value::Boolean(false)),
            _ => Err(PinkDbError::Execution(format!(
                "cannot CAST \"{s}\" AS BOOLEAN"
            ))),
        },
        (Value::Varchar(s), DataType::Integer) => s
            .trim()
            .parse::<i32>()
            .map(Value::Integer)
            .map_err(|_| PinkDbError::Execution(format!("cannot CAST \"{s}\" AS INTEGER"))),
        (Value::Varchar(s), DataType::Bigint) => s
            .trim()
            .parse::<i64>()
            .map(Value::Bigint)
            .map_err(|_| PinkDbError::Execution(format!("cannot CAST \"{s}\" AS BIGINT"))),
        (Value::Varchar(s), DataType::Double) => s
            .trim()
            .parse::<f64>()
            .map(Value::Double)
            .map_err(|_| PinkDbError::Execution(format!("cannot CAST \"{s}\" AS DOUBLE"))),
        (Value::Varchar(s), DataType::Date) => crate::types::parse_date(s.trim())
            .map(Value::Date)
            .ok_or_else(|| PinkDbError::Execution(format!("cannot CAST \"{s}\" AS DATE"))),
        (Value::Varchar(s), DataType::Timestamp) => crate::types::parse_timestamp_or_date(s.trim())
            .map(Value::Timestamp)
            .ok_or_else(|| PinkDbError::Execution(format!("cannot CAST \"{s}\" AS TIMESTAMP"))),
        // Date ↔ timestamp. Microseconds since epoch overflow i64 for
        // extreme dates (the old `as i64 *` wrapped silently).
        (Value::Date(d), DataType::Timestamp) => (d as i64)
            .checked_mul(86_400_000_000)
            .map(Value::Timestamp)
            .ok_or_else(|| PinkDbError::Execution("timestamp out of range".to_string())),
        (Value::Timestamp(us), DataType::Date) => {
            Ok(Value::Date(us.div_euclid(86_400_000_000) as i32))
        }
        // DECIMAL ↔ numeric. DECIMAL → INTEGER/BIGINT rounds half away from
        // zero (DuckDB: CAST(DECIMAL '12.7' AS INTEGER) = 13), DECIMAL →
        // DOUBLE divides by the power of ten in f64.
        (Value::Decimal(x, scale), DataType::Integer) => {
            let d =
                crate::types::rescale_decimal(x, scale, 0).filter(|v| i32::try_from(*v).is_ok());
            d.map(|v| Value::Integer(v as i32))
                .ok_or_else(|| cast_out_of_range(&Value::Decimal(x, scale), DataType::Integer))
        }
        (Value::Decimal(x, scale), DataType::Bigint) => {
            let d =
                crate::types::rescale_decimal(x, scale, 0).filter(|v| i64::try_from(*v).is_ok());
            d.map(|v| Value::Bigint(v as i64))
                .ok_or_else(|| cast_out_of_range(&Value::Decimal(x, scale), DataType::Bigint))
        }
        // Anything numeric → DECIMAL(precision, scale): rounding half away
        // from zero, precision overflow is an error.
        (v, DataType::Decimal { precision, scale }) => {
            let out = match &v {
                Value::Integer(i) => crate::types::int_to_decimal(*i as i128, precision, scale),
                Value::Bigint(i) => crate::types::int_to_decimal(*i as i128, precision, scale),
                Value::Decimal(x, from) => crate::types::rescale_decimal(*x, *from, scale)
                    .filter(|y| crate::types::decimal_fits(*y, precision)),
                Value::Double(x) => crate::types::f64_to_decimal(*x, precision, scale),
                Value::Varchar(s) => crate::types::parse_decimal_str(s, precision, scale).ok(),
                _ => None,
            };
            out.map(|y| Value::Decimal(y, scale))
                .ok_or_else(|| cast_out_of_range(&v, DataType::Decimal { precision, scale }))
        }
        // VARCHAR → INTERVAL (INTERVAL → VARCHAR rides the generic
        // "everything formats to VARCHAR" arm above).
        (Value::Varchar(s), DataType::Interval) => crate::types::parse_interval(s.as_str())
            .map(|iv| Value::Interval {
                months: iv.months,
                days: iv.days,
                micros: iv.micros,
            })
            .ok_or_else(|| PinkDbError::Execution(format!("cannot CAST \"{s}\" AS INTERVAL"))),
        // ARRAY → ARRAY: the length is part of the type, so it must match;
        // the element tower is free, which is how an `INTEGER[3]` literal
        // lands in a `DOUBLE[3]` column.
        (Value::Array(a), DataType::Array { elem, len }) => {
            if a.len() != len as usize {
                return Err(PinkDbError::Execution(format!(
                    "cannot CAST {} AS {}: the array size is part of the type, so it must match",
                    a.dtype(),
                    DataType::Array { elem, len }
                )));
            }
            a.cast_to(elem)
                .map(|v| Value::Array(std::sync::Arc::new(v)))
        }
        // VARCHAR → ARRAY: the `[1.0, 2.0]` text form, exact length.
        (Value::Varchar(s), DataType::Array { elem, len }) => {
            crate::array::ArrayValue::parse(&s, elem, len)
                .map(|v| Value::Array(std::sync::Arc::new(v)))
        }
        (v, t) => Err(PinkDbError::Execution(format!("cannot CAST {v} AS {t}"))),
    }
}

/// `CASE WHEN c THEN v ... [ELSE e]`: first TRUE condition wins per row
/// (NULL conditions never match). Missing ELSE is NULL. Each condition and
/// value arm is evaluated only for the rows still undecided, so an unselected
/// arm's row-dependent error (e.g. `y / x` when `x = 0`) is never raised.
fn eval_case(
    branches: &[(BoundExpr, BoundExpr)],
    else_: &Option<Box<BoundExpr>>,
    dtype: DataType,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    if let Some(out) = eval_case_selected(branches, else_, dtype, chunk, cx)? {
        return Ok(out);
    }
    eval_case_rows(branches, else_, dtype, chunk, cx)
}

/// `CASE` a vector at a time (DuckDB's `ExecuteCase`): each condition is a
/// selection over the rows no earlier branch took (so a condition's error
/// is only raised for rows that reach it, as before), a literal result needs
/// no evaluation at all, and the output is one typed gather from the branch
/// results — instead of a `Value` per row. TPC-H Q12's
/// `SUM(CASE WHEN o_orderpriority = '1-URGENT' OR … THEN 1 ELSE 0 END)`.
/// `None` when a branch result is not the CASE's own type; the per-row path
/// then coerces as it always did.
fn eval_case_selected(
    branches: &[(BoundExpr, BoundExpr)],
    else_: &Option<Box<BoundExpr>>,
    dtype: DataType,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Option<Vector>> {
    let n = chunk.num_rows();
    let mut sources: Vec<Vector> = Vec::new();
    let mut picks = vec![(crate::vector::GATHER_NULL, 0u32); n];
    // The rows of `rows` take result `v`: a literal is one constant cell, an
    // expression is evaluated over just those rows.
    let mut take = |v: &BoundExpr, rows: &[u32], sources: &mut Vec<Vector>| -> Result<bool> {
        let (vals, constant) = match v {
            BoundExpr::Literal(Value::Null) => return Ok(true),
            BoundExpr::Literal(lit) => (Vector::new_const(lit, 1), true),
            _ if rows.len() == n => (eval_expr(v, chunk, cx)?, false),
            _ => (eval_expr(v, &chunk.take(rows), cx)?, false),
        };
        // A literal of another type (`ELSE 0` in a DECIMAL CASE) is one
        // cell to convert; a computed result of another type keeps the
        // per-row path and its coercion.
        let vals = if vals.dtype() == dtype {
            vals
        } else if constant {
            match coerce_to(&vals, dtype) {
                Ok(v) => v,
                Err(_) => return Ok(false),
            }
        } else {
            return Ok(false);
        };
        let src = sources.len() as u32;
        sources.push(vals);
        for (k, &row) in rows.iter().enumerate() {
            let at = if constant { 0 } else { k as u32 };
            picks[row as usize] = (src, at);
        }
        Ok(true)
    };
    let mut undecided: Vec<u32> = (0..n as u32).collect();
    for (c, v) in branches {
        if undecided.is_empty() {
            break;
        }
        let mut selected = undecided.clone();
        select_predicate(c, chunk, cx, &mut selected)?;
        if !selected.is_empty() && !take(v, &selected, &mut sources)? {
            return Ok(None);
        }
        undecided = difference_selections(&undecided, &selected);
    }
    if !undecided.is_empty()
        && let Some(e) = else_
        && !take(e, &undecided, &mut sources)?
    {
        return Ok(None);
    }
    if sources.is_empty() {
        let mut out = Vector::with_capacity(dtype, n);
        for _ in 0..n {
            out.push_null()?;
        }
        return Ok(Some(out));
    }
    let refs: Vec<&Vector> = sources.iter().collect();
    Ok(Some(Vector::gather_located(dtype, &refs, &picks)))
}

/// The row-at-a-time `CASE`, for branch results that need coercion.
fn eval_case_rows(
    branches: &[(BoundExpr, BoundExpr)],
    else_: &Option<Box<BoundExpr>>,
    dtype: DataType,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    let n = chunk.num_rows();
    let mut slots: Vec<Option<Value>> = vec![None; n];
    let mut undecided: Vec<usize> = (0..n).collect();
    for (c, v) in branches {
        if undecided.is_empty() {
            break;
        }
        // First branch (nothing decided yet) evaluates on the whole chunk.
        let cond = if undecided.len() == n {
            eval_expr(c, chunk, cx)?
        } else {
            eval_expr(c, &chunk.gather(&undecided), cx)?
        };
        let mut selected = Vec::new();
        let mut rest = Vec::with_capacity(undecided.len());
        for (j, &row) in undecided.iter().enumerate() {
            if !cond.is_null(j) && as_bool(&cond, j)? {
                selected.push(row);
            } else {
                rest.push(row);
            }
        }
        if !selected.is_empty() {
            let vals = if selected.len() == n {
                eval_expr(v, chunk, cx)?
            } else {
                eval_expr(v, &chunk.gather(&selected), cx)?
            };
            for (k, &row) in selected.iter().enumerate() {
                slots[row] = Some(vals.get(k));
            }
        }
        undecided = rest;
    }
    if !undecided.is_empty() {
        match else_ {
            Some(e) => {
                let vals = if undecided.len() == n {
                    eval_expr(e, chunk, cx)?
                } else {
                    eval_expr(e, &chunk.gather(&undecided), cx)?
                };
                for (k, &row) in undecided.iter().enumerate() {
                    slots[row] = Some(vals.get(k));
                }
            }
            None => {
                for &row in &undecided {
                    slots[row] = Some(Value::Null);
                }
            }
        }
    }
    let mut out = Vector::new_empty(dtype);
    for s in slots {
        match s {
            Some(Value::Null) | None => out.push_null()?,
            Some(v) => out.push_value(v)?,
        }
    }
    Ok(out)
}

/// `COALESCE`: first non-NULL argument per row (all args share the dtype).
/// Each argument is evaluated only for the rows still unresolved, so an
/// unselected argument's row-dependent error is never raised.
fn eval_coalesce(
    args: &[BoundExpr],
    dtype: DataType,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    let n = chunk.num_rows();
    let mut slots: Vec<Option<Value>> = vec![None; n];
    let mut undecided: Vec<usize> = (0..n).collect();
    for a in args {
        if undecided.is_empty() {
            break;
        }
        let vals = if undecided.len() == n {
            eval_expr(a, chunk, cx)?
        } else {
            eval_expr(a, &chunk.gather(&undecided), cx)?
        };
        let mut rest = Vec::with_capacity(undecided.len());
        for (j, &row) in undecided.iter().enumerate() {
            if vals.is_null(j) {
                rest.push(row);
            } else {
                slots[row] = Some(vals.get(j));
            }
        }
        undecided = rest;
    }
    let mut out = Vector::new_empty(dtype);
    for s in slots {
        match s {
            Some(Value::Null) | None => out.push_null()?,
            Some(v) => out.push_value(v)?,
        }
    }
    Ok(out)
}
/// One row of an ARRAY column, borrowed from the flat buffer.
enum ArrayCell<'a> {
    F32(&'a [f32]),
    F64(&'a [f64]),
}

fn array_cell<'a>(
    elem: crate::types::ArrayElem,
    values: &'a ArrayElems,
    start: usize,
    len: usize,
) -> Option<ArrayCell<'a>> {
    match (elem, values) {
        (crate::types::ArrayElem::Float, ArrayElems::Floats32(v)) => {
            Some(ArrayCell::F32(v.get(start..start + len)?))
        }
        (crate::types::ArrayElem::Double, ArrayElems::Floats(v)) => {
            Some(ArrayCell::F64(v.get(start..start + len)?))
        }
        _ => None,
    }
}

fn any_null_elem(valid: Option<&[u8]>, start: usize, len: usize) -> bool {
    valid.is_some_and(|m| {
        (0..len).any(|i| {
            let k = start + i;
            (m[k / 8] >> (k % 8)) & 1 == 0
        })
    })
}

fn distance_of_cells(func: ScalarFunc, a: &ArrayCell<'_>, b: &ArrayCell<'_>) -> f64 {
    use crate::distance;
    match (a, b) {
        (ArrayCell::F32(a), ArrayCell::F32(b)) => {
            let x = match func {
                ScalarFunc::ArrayDistance => distance::l2sq_f32(a, b).sqrt(),
                ScalarFunc::ArrayCosineDistance => distance::cosine_distance_f32(a, b),
                ScalarFunc::ArrayCosineSimilarity => distance::cosine_similarity_f32(a, b),
                ScalarFunc::ArrayInnerProduct => distance::dot_f32(a, b),
                ScalarFunc::ArrayNegativeInnerProduct => -distance::dot_f32(a, b),
                _ => unreachable!("guarded by the caller"),
            };
            f64::from(x)
        }
        (ArrayCell::F64(a), ArrayCell::F64(b)) => match func {
            ScalarFunc::ArrayDistance => distance::l2sq_f64(a, b).sqrt(),
            ScalarFunc::ArrayCosineDistance => distance::cosine_distance_f64(a, b),
            ScalarFunc::ArrayCosineSimilarity => distance::cosine_similarity_f64(a, b),
            ScalarFunc::ArrayInnerProduct => distance::dot_f64(a, b),
            ScalarFunc::ArrayNegativeInnerProduct => -distance::dot_f64(a, b),
            _ => unreachable!("guarded by the caller"),
        },
        // Mixed FLOAT/DOUBLE widens. Materialising one side per call is fine:
        // this is the uncommon overload.
        (a, b) => {
            let (a, b): (Vec<f64>, Vec<f64>) = match (a, b) {
                (ArrayCell::F32(a), b) => (a.iter().map(|x| f64::from(*x)).collect(), cell_f64(b)),
                (a, b) => (cell_f64(a), cell_f64(b)),
            };
            match func {
                ScalarFunc::ArrayDistance => distance::l2sq_f64(&a, &b).sqrt(),
                ScalarFunc::ArrayCosineDistance => distance::cosine_distance_f64(&a, &b),
                ScalarFunc::ArrayCosineSimilarity => distance::cosine_similarity_f64(&a, &b),
                ScalarFunc::ArrayInnerProduct => distance::dot_f64(&a, &b),
                ScalarFunc::ArrayNegativeInnerProduct => -distance::dot_f64(&a, &b),
                _ => unreachable!("guarded by the caller"),
            }
        }
    }
}

fn cell_f64(cell: &ArrayCell<'_>) -> Vec<f64> {
    match cell {
        ArrayCell::F32(v) => v.iter().map(|x| f64::from(*x)).collect(),
        ArrayCell::F64(v) => v.to_vec(),
    }
}

fn eval_distance(func: ScalarFunc, a: &Vector, b: &Vector) -> Result<Vector> {
    let VectorData::Array {
        elem: ea,
        len: la,
        values: va,
        valid: ba,
    } = a.data()
    else {
        return Err(PinkDbError::Type(format!(
            "{}: Arguments must be arrays of FLOAT or DOUBLE",
            func.sql_name()
        )));
    };
    let VectorData::Array {
        elem: eb,
        len: lb,
        values: vb,
        valid: bb,
    } = b.data()
    else {
        return Err(PinkDbError::Type(format!(
            "{}: Arguments must be arrays of FLOAT or DOUBLE",
            func.sql_name()
        )));
    };
    if la != lb {
        return Err(PinkDbError::Type(format!(
            "{}: Array arguments must be of the same size",
            func.sql_name()
        )));
    }
    let (ea, eb) = (*ea, *eb);
    if !matches!(
        ea,
        crate::types::ArrayElem::Float | crate::types::ArrayElem::Double
    ) || !matches!(
        eb,
        crate::types::ArrayElem::Float | crate::types::ArrayElem::Double
    ) {
        return Err(PinkDbError::Type(format!(
            "{}: Arguments must be arrays of FLOAT or DOUBLE",
            func.sql_name()
        )));
    }
    let n = *la as usize;
    let mut out = Vector::new_empty(DataType::Double);
    for i in 0..a.len() {
        if a.is_null(i) || b.is_null(i) {
            out.push_null()?;
            continue;
        }
        if any_null_elem(ba.as_deref(), i * n, n) {
            return Err(PinkDbError::Execution(format!(
                "{}: left argument can not contain NULL values",
                func.sql_name()
            )));
        }
        if any_null_elem(bb.as_deref(), i * n, n) {
            return Err(PinkDbError::Execution(format!(
                "{}: right argument can not contain NULL values",
                func.sql_name()
            )));
        }
        let ca = array_cell(ea, va, i * n, n).ok_or_else(|| {
            PinkDbError::Execution("corrupt file: ARRAY element kind mismatch".to_string())
        })?;
        let cb = array_cell(eb, vb, i * n, n).ok_or_else(|| {
            PinkDbError::Execution("corrupt file: ARRAY element kind mismatch".to_string())
        })?;
        out.push_valid(Value::Double(distance_of_cells(func, &ca, &cb)))?;
    }
    Ok(out)
}

fn distance_value(
    func: ScalarFunc,
    a: &crate::array::ArrayValue,
    b: &crate::array::ArrayValue,
) -> Result<Value> {
    if a.len() != b.len() {
        return Err(PinkDbError::Type(format!(
            "{}: Array arguments must be of the same size",
            func.sql_name()
        )));
    }
    if a.has_null_elems() {
        return Err(PinkDbError::Execution(format!(
            "{}: left argument can not contain NULL values",
            func.sql_name()
        )));
    }
    if b.has_null_elems() {
        return Err(PinkDbError::Execution(format!(
            "{}: right argument can not contain NULL values",
            func.sql_name()
        )));
    }
    let ca = array_cell(a.elem, &a.values, 0, a.len()).ok_or_else(|| {
        PinkDbError::Type(format!(
            "{}: Arguments must be arrays of FLOAT or DOUBLE",
            func.sql_name()
        ))
    })?;
    let cb = array_cell(b.elem, &b.values, 0, b.len()).ok_or_else(|| {
        PinkDbError::Type(format!(
            "{}: Arguments must be arrays of FLOAT or DOUBLE",
            func.sql_name()
        ))
    })?;
    Ok(Value::Double(distance_of_cells(func, &ca, &cb)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::Cx;
    use crate::sql::binder::BoundExpr as B;
    use crate::storage::PinkDb;
    use crate::types::Value as V;

    fn test_cx<'a>(db: &'a PinkDb) -> Cx<'a> {
        Cx::new(db)
    }

    #[test]
    fn decimal_round_handles_i64_min_digits() {
        // `-digits` would overflow for i64::MIN; the helper must not panic
        // (SQL caps the digit count at INTEGER, but the helper defends anyway).
        assert_eq!(decimal_round(15, 1, i64::MIN, 2).unwrap(), V::Decimal(0, 0));
    }

    fn chunk2() -> DataChunk {
        // a: INTEGER [1, NULL], b: DOUBLE [2.5, 4.0]
        let mut a = Vector::new_empty(DataType::Integer);
        a.push_valid(V::Integer(1)).unwrap();
        a.push_null().unwrap();
        let mut b = Vector::new_empty(DataType::Double);
        b.push_valid(V::Double(2.5)).unwrap();
        b.push_valid(V::Double(4.0)).unwrap();
        DataChunk::new(vec![a, b]).unwrap()
    }

    #[test]
    fn null_propagates_through_arith() {
        let e = B::Binary {
            op: BinaryOp::Add,
            left: Box::new(B::Column {
                index: 0,
                dtype: DataType::Integer,
            }),
            right: Box::new(B::Column {
                index: 1,
                dtype: DataType::Double,
            }),
            dtype: DataType::Double,
        };
        let db = PinkDb::new();
        let v = eval_expr(&e, &chunk2(), &test_cx(&db)).unwrap();
        assert_eq!(v.get(0), V::Double(3.5));
        assert_eq!(v.get(1), V::Null);
    }

    #[test]
    fn kleene_and_or() {
        // NULL AND FALSE = FALSE; NULL OR FALSE = NULL; NULL AND TRUE = NULL.
        let mut l = Vector::new_empty(DataType::Boolean);
        l.push_null().unwrap();
        l.push_null().unwrap();
        l.push_valid(V::Boolean(true)).unwrap();
        let mut r = Vector::new_empty(DataType::Boolean);
        r.push_valid(V::Boolean(false)).unwrap();
        r.push_valid(V::Boolean(true)).unwrap();
        r.push_valid(V::Boolean(true)).unwrap();
        let c = DataChunk::new(vec![l, r]).unwrap();
        let col = |i| B::Column {
            index: i,
            dtype: DataType::Boolean,
        };
        let and = B::Binary {
            op: BinaryOp::And,
            left: Box::new(col(0)),
            right: Box::new(col(1)),
            dtype: DataType::Boolean,
        };
        let or = B::Binary {
            op: BinaryOp::Or,
            left: Box::new(col(0)),
            right: Box::new(col(1)),
            dtype: DataType::Boolean,
        };
        let db = PinkDb::new();
        let cx = test_cx(&db);
        let a = eval_expr(&and, &c, &cx).unwrap();
        assert_eq!(
            (a.get(0), a.get(1), a.get(2)),
            (V::Boolean(false), V::Null, V::Boolean(true))
        );
        let o = eval_expr(&or, &c, &cx).unwrap();
        assert_eq!(
            (o.get(0), o.get(1), o.get(2)),
            (V::Null, V::Boolean(true), V::Boolean(true))
        );
    }

    #[test]
    fn comparison_nan_matches_num_cmp() {
        // plan::compare_values: DuckDB order — NaN == NaN, NaN above every
        // other double (+inf included), -0.0 == 0.0. Pin that reference
        // behavior first, then the typed DOUBLE kernels.
        let nan = f64::NAN;
        let other_nan = f64::from_bits(0xfff8_0000_0000_0042); // negative NaN payload
        for (a, b, want) in [
            (nan, 1.0, Ordering::Greater),
            (1.0, nan, Ordering::Less),
            (nan, f64::INFINITY, Ordering::Greater),
            (f64::NEG_INFINITY, nan, Ordering::Less),
            (nan, nan, Ordering::Equal),
            (nan, other_nan, Ordering::Equal),
            (-0.0, 0.0, Ordering::Equal),
            (1.0, 2.0, Ordering::Less),
        ] {
            assert_eq!(
                compare_values(&V::Double(a), &V::Double(b)),
                want,
                "{a} vs {b}"
            );
        }

        // Rows: (NaN, 1), (1, NaN), (NULL, 5), (2, NULL), (NaN, NaN'),
        // (-0.0, 0.0), (NaN, inf).
        let lhs = [
            Some(nan),
            Some(1.0),
            None,
            Some(2.0),
            Some(nan),
            Some(-0.0),
            Some(nan),
        ];
        let rhs = [
            Some(1.0),
            Some(nan),
            Some(5.0),
            None,
            Some(other_nan),
            Some(0.0),
            Some(f64::INFINITY),
        ];
        let mk = |vals: &[Option<f64>]| {
            let mut v = Vector::new_empty(DataType::Double);
            for x in vals {
                match x {
                    Some(x) => v.push_valid(V::Double(*x)).unwrap(),
                    None => v.push_null().unwrap(),
                }
            }
            v
        };
        let chunk = DataChunk::new(vec![mk(&lhs), mk(&rhs)]).unwrap();
        let db = PinkDb::new();
        let cx = test_cx(&db);
        let col = |index: usize| B::Column {
            index,
            dtype: DataType::Double,
        };
        let eval = |op: BinaryOp, left: B, right: B| {
            let e = B::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
                dtype: DataType::Boolean,
            };
            eval_expr(&e, &chunk, &cx).unwrap()
        };
        // One char per row: T(rue) / F(alse) / N(ULL).
        let tfn = |v: &Vector| {
            (0..v.len())
                .map(|i| match v.get(i) {
                    V::Boolean(true) => 'T',
                    V::Boolean(false) => 'F',
                    _ => 'N',
                })
                .collect::<String>()
        };
        // Vector-vs-vector kernel.
        for (op, want) in [
            (BinaryOp::Eq, "FFNNTTF"),
            (BinaryOp::NotEq, "TTNNFFT"),
            (BinaryOp::Lt, "FTNNFFF"),
            (BinaryOp::LtEq, "FTNNTTF"),
            (BinaryOp::Gt, "TFNNFFT"),
            (BinaryOp::GtEq, "TFNNTTT"),
        ] {
            assert_eq!(tfn(&eval(op, col(0), col(1))), want, "{op:?}");
        }

        // Column-vs-literal kernel, literal on either side: `d = NaN`
        // matches only the NaN rows; `inf < d` only NaN; `-0.0 = 0.0`.
        let lit = |x: f64| B::Literal(V::Double(x));
        assert_eq!(tfn(&eval(BinaryOp::Eq, col(0), lit(other_nan))), "TFNFTFT");
        assert_eq!(
            tfn(&eval(BinaryOp::Lt, lit(f64::INFINITY), col(0))),
            "TFNFTFT"
        );
        assert_eq!(tfn(&eval(BinaryOp::Eq, col(1), lit(-0.0))), "FFFNFTF");
    }

    #[test]
    fn comparison_varchar_nulls_and_empty_strings_past_word_boundary() {
        // 130 rows exercise the second validity word (bit 64+).
        let mut l = Vector::new_empty(DataType::Varchar);
        let mut r = Vector::new_empty(DataType::Varchar);
        for i in 0..130 {
            if matches!(i, 0 | 64 | 65 | 129) {
                l.push_null().unwrap();
            } else {
                l.push_str(match i % 4 {
                    0 => "",
                    2 => "banana",
                    1 => "apple",
                    _ => "",
                })
                .unwrap();
            }
            if matches!(i, 1 | 63 | 64) {
                r.push_null().unwrap();
            } else {
                r.push_str(match i % 3 {
                    0 => "",
                    1 => "apple",
                    _ => "banana",
                })
                .unwrap();
            }
        }
        let chunk = DataChunk::new(vec![l, r]).unwrap();
        let db = PinkDb::new();
        let cx = test_cx(&db);
        let cmp = |op: BinaryOp| {
            let e = B::Binary {
                op,
                left: Box::new(B::Column {
                    index: 0,
                    dtype: DataType::Varchar,
                }),
                right: Box::new(B::Column {
                    index: 1,
                    dtype: DataType::Varchar,
                }),
                dtype: DataType::Boolean,
            };
            eval_expr(&e, &chunk, &cx).unwrap()
        };

        let eq = cmp(BinaryOp::Eq);
        assert_eq!(eq.len(), 130);
        assert_eq!(eq.get(0), V::Null); // NULL vs ""
        assert_eq!(eq.get(1), V::Null); // "apple" vs NULL
        assert_eq!(eq.get(2), V::Boolean(true)); // "banana" = "banana"
        assert_eq!(eq.get(3), V::Boolean(true)); // "" = ""
        assert_eq!(eq.get(5), V::Boolean(false)); // "apple" vs "banana"
        assert_eq!(eq.get(63), V::Null); // "" vs NULL (word edge)
        assert_eq!(eq.get(64), V::Null); // NULL vs NULL (word edge)
        assert_eq!(eq.get(65), V::Null); // NULL vs "banana" (word edge)
        assert_eq!(eq.get(66), V::Boolean(false)); // "banana" vs ""
        assert_eq!(eq.get(129), V::Null); // NULL vs ""

        // Empty strings sort before non-empty; nulls stay NULL under `<`.
        let lt = cmp(BinaryOp::Lt);
        assert_eq!(lt.get(3), V::Boolean(false)); // "" < ""
        assert_eq!(lt.get(6), V::Boolean(false)); // "banana" < "" is false
        assert_eq!(lt.get(67), V::Boolean(true)); // "" < "apple"
        assert_eq!(lt.get(68), V::Boolean(true)); // "" < "banana"
        assert_eq!(lt.get(1), V::Null);
    }

    #[test]
    fn scalar_fast_path_matches_the_vector_path() {
        // The column-vs-literal fast path must agree with the general
        // (column-vs-constant-column) path for every supported type, on both
        // literal sides, NULLs included.
        let mut ints = Vector::new_empty(DataType::Integer);
        let mut bigs = Vector::new_empty(DataType::Bigint);
        let mut doubles = Vector::new_empty(DataType::Double);
        let mut strings = Vector::new_empty(DataType::Varchar);
        let mut dates = Vector::new_empty(DataType::Date);
        let mut stamps = Vector::new_empty(DataType::Timestamp);
        for i in 0..70 {
            if i % 11 == 0 {
                ints.push_null().unwrap();
                bigs.push_null().unwrap();
                doubles.push_null().unwrap();
                strings.push_null().unwrap();
                dates.push_null().unwrap();
                stamps.push_null().unwrap();
                continue;
            }
            ints.push_valid(V::Integer(i - 35)).unwrap();
            bigs.push_valid(V::Bigint(i as i64 * 1_000_000_007 - 3_000_000_000))
                .unwrap();
            doubles
                .push_valid(V::Double(i as f64 * 1.5 - 20.0))
                .unwrap();
            strings
                .push_str(match i % 4 {
                    0 | 3 => "apple",
                    1 => "",
                    _ => "banana",
                })
                .unwrap();
            dates.push_valid(V::Date(i - 35)).unwrap();
            stamps
                .push_valid(V::Timestamp(i as i64 * 86_400_000_000 - 1_000))
                .unwrap();
        }
        let db = PinkDb::new();
        let cx = test_cx(&db);

        let cases: Vec<(DataType, Vector, V)> = vec![
            (DataType::Integer, ints, V::Integer(4)),
            (DataType::Bigint, bigs, V::Bigint(1_000_000_007)),
            (DataType::Double, doubles, V::Double(10.5)),
            (DataType::Varchar, strings, V::Varchar("apple".to_string())),
            (DataType::Date, dates, V::Date(3)),
            (DataType::Timestamp, stamps, V::Timestamp(86_400_000_000)),
        ];
        for (dtype, col, lit) in cases {
            let mut lit_col = Vector::new_empty(dtype);
            for _ in 0..col.len() {
                lit_col.push_valid(lit.clone()).unwrap();
            }
            let chunk = DataChunk::new(vec![col.clone(), lit_col]).unwrap();
            let lit_expr = B::Literal(lit);
            let column = |index| Box::new(B::Column { index, dtype });
            for op in [
                BinaryOp::Eq,
                BinaryOp::NotEq,
                BinaryOp::Lt,
                BinaryOp::LtEq,
                BinaryOp::Gt,
                BinaryOp::GtEq,
            ] {
                for scalar_left in [false, true] {
                    // Reference: the same comparison as column-vs-column,
                    // with the columns chosen to match the scalar's side.
                    let (lidx, ridx) = if scalar_left { (1, 0) } else { (0, 1) };
                    let general = eval_expr(
                        &B::Binary {
                            op,
                            left: column(lidx),
                            right: column(ridx),
                            dtype: DataType::Boolean,
                        },
                        &chunk,
                        &cx,
                    )
                    .unwrap();
                    let (left, right) = if scalar_left {
                        (Box::new(lit_expr.clone()), column(0))
                    } else {
                        (column(0), Box::new(lit_expr.clone()))
                    };
                    let fast = eval_expr(
                        &B::Binary {
                            op,
                            left,
                            right,
                            dtype: DataType::Boolean,
                        },
                        &chunk,
                        &cx,
                    )
                    .unwrap();
                    assert_eq!(fast.len(), general.len());
                    for i in 0..general.len() {
                        assert_eq!(
                            fast.get(i),
                            general.get(i),
                            "{dtype:?} {op:?} scalar_left={scalar_left} row {i}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn arith_overflow_and_division_errors() {
        let mut a = Vector::new_empty(DataType::Integer);
        a.push_valid(V::Integer(i32::MAX)).unwrap();
        a.push_valid(V::Integer(7)).unwrap();
        let mut b = Vector::new_empty(DataType::Integer);
        b.push_valid(V::Integer(1)).unwrap();
        b.push_valid(V::Integer(0)).unwrap();
        let chunk = DataChunk::new(vec![a, b]).unwrap();
        let db = PinkDb::new();
        let cx = test_cx(&db);
        let bin = |op: BinaryOp, dtype: DataType| B::Binary {
            op,
            left: Box::new(B::Column {
                index: 0,
                dtype: DataType::Integer,
            }),
            right: Box::new(B::Column {
                index: 1,
                dtype: DataType::Integer,
            }),
            dtype,
        };

        // i32::MAX + 1 is an overflow error (DuckDB semantics), with the
        // same message as the per-value path.
        let err = eval_expr(&bin(BinaryOp::Add, DataType::Integer), &chunk, &cx).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Overflow in addition of INTEGER (2147483647 + 1)"
        );
        assert_eq!(
            arith_values(BinaryOp::Add, V::Integer(i32::MAX), V::Integer(1))
                .unwrap_err()
                .to_string(),
            err.to_string()
        );

        // 7 % 0 errors with the same message as the generic path.
        let err = eval_expr(&bin(BinaryOp::Modulo, DataType::Integer), &chunk, &cx).unwrap_err();
        assert_eq!(err.to_string(), "integer modulo by zero");

        // int / int widens to DOUBLE, but a zero divisor still errors.
        let err = eval_expr(&bin(BinaryOp::Divide, DataType::Double), &chunk, &cx).unwrap_err();
        assert_eq!(err.to_string(), "integer division by zero");

        // A DOUBLE operand keeps IEEE semantics instead: 7.0/0.0 = +inf.
        let mut f = Vector::new_empty(DataType::Double);
        f.push_valid(V::Double(7.0)).unwrap();
        f.push_valid(V::Double(0.0)).unwrap();
        let chunk_f = DataChunk::new(vec![f]).unwrap();
        let div_f = B::Binary {
            op: BinaryOp::Divide,
            left: Box::new(B::Column {
                index: 0,
                dtype: DataType::Double,
            }),
            right: Box::new(B::Literal(V::Double(0.0))),
            dtype: DataType::Double,
        };
        let out = eval_expr(&div_f, &chunk_f, &cx).unwrap();
        assert_eq!(out.get(0), V::Double(f64::INFINITY));
        match out.get(1) {
            V::Double(x) => assert!(x.is_nan(), "0.0/0.0 stays NaN"), // NaN != NaN
            other => panic!("expected NaN, got {other:?}"),
        }
    }

    #[test]
    fn coerce_to_typed_fast_paths_preserve_values_and_validity() {
        let mut v = Vector::new_empty(DataType::Integer);
        for i in 0..130 {
            if i % 5 == 0 {
                v.push_null().unwrap();
            } else {
                v.push_valid(V::Integer(i - 65)).unwrap();
            }
        }
        let big = coerce_to(&v, DataType::Bigint).unwrap();
        let dbl = coerce_to(&v, DataType::Double).unwrap();
        assert_eq!(big.dtype(), DataType::Bigint);
        assert_eq!(dbl.dtype(), DataType::Double);
        assert_eq!(big.len(), 130);
        for i in 0..130 {
            assert_eq!(big.is_null(i), v.is_null(i));
            assert_eq!(dbl.is_null(i), v.is_null(i));
            if !v.is_null(i) {
                let x = i as i32 - 65;
                assert_eq!(big.get(i), V::Bigint(x as i64));
                assert_eq!(dbl.get(i), V::Double(x as f64));
            }
        }
    }

    #[test]
    fn scalar_typed_fast_paths() {
        let mut s = Vector::new_empty(DataType::Varchar);
        s.push_str("MiXeD").unwrap();
        s.push_null().unwrap();
        s.push_str("").unwrap();
        let chunk = DataChunk::new(vec![s]).unwrap();
        let db = PinkDb::new();
        let cx = test_cx(&db);
        let call = |func: ScalarFunc, dtype: DataType| {
            let e = B::Scalar {
                func,
                args: vec![B::Column {
                    index: 0,
                    dtype: DataType::Varchar,
                }],
                dtype,
            };
            eval_expr(&e, &chunk, &cx).unwrap()
        };
        let up = call(ScalarFunc::Upper, DataType::Varchar);
        assert_eq!(up.get(0), V::Varchar("MIXED".to_string()));
        assert_eq!(up.get(1), V::Null);
        assert_eq!(up.get(2), V::Varchar(String::new()));
        let lo = call(ScalarFunc::Lower, DataType::Varchar);
        assert_eq!(lo.get(0), V::Varchar("mixed".to_string()));
        let len = call(ScalarFunc::Length, DataType::Integer);
        assert_eq!(len.get(0), V::Integer(5));
        assert_eq!(len.get(1), V::Null);
        assert_eq!(len.get(2), V::Integer(0));

        // ABS (checked) and one-arg ROUND on numeric columns.
        let mut d = Vector::new_empty(DataType::Integer);
        d.push_valid(V::Integer(i32::MIN)).unwrap();
        d.push_null().unwrap();
        let mut f = Vector::new_empty(DataType::Double);
        f.push_valid(V::Double(2.5)).unwrap();
        f.push_valid(V::Double(-2.5)).unwrap();
        let chunk2 = DataChunk::new(vec![d, f]).unwrap();
        let abs = B::Scalar {
            func: ScalarFunc::Abs,
            args: vec![B::Column {
                index: 0,
                dtype: DataType::Integer,
            }],
            dtype: DataType::Integer,
        };
        let err = eval_expr(&abs, &chunk2, &cx).unwrap_err();
        assert!(
            err.to_string().starts_with("Overflow in abs of INTEGER"),
            "{err}"
        );
        let mut ok = Vector::new_empty(DataType::Integer);
        ok.push_valid(V::Integer(-5)).unwrap();
        ok.push_null().unwrap();
        let mut f2 = Vector::new_empty(DataType::Double);
        f2.push_valid(V::Double(0.0)).unwrap();
        f2.push_valid(V::Double(0.0)).unwrap();
        let a = eval_expr(&abs, &DataChunk::new(vec![ok, f2]).unwrap(), &cx).unwrap();
        assert_eq!(a.get(0), V::Integer(5));
        assert_eq!(a.get(1), V::Null);
        let round = B::Scalar {
            func: ScalarFunc::Round,
            args: vec![B::Column {
                index: 1,
                dtype: DataType::Double,
            }],
            dtype: DataType::Double,
        };
        let out = eval_expr(&round, &chunk2, &cx).unwrap();
        assert_eq!((out.get(0), out.get(1)), (V::Double(3.0), V::Double(-3.0)));
    }

    // ---------- selection vectors vs a row-at-a-time reference ----------

    /// xorshift64: repeatable cases without a dependency.
    struct Rng(u64);

    impl Rng {
        fn new(seed: u64) -> Self {
            Rng(seed | 1)
        }
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: u64) -> usize {
            (self.next() % n) as usize
        }
        fn one_in(&mut self, n: u64) -> bool {
            self.below(n) == 0
        }
    }

    /// The fixed-width types the vector kernels cover, plus VARCHAR.
    const FUZZ_DTYPES: [DataType; 7] = [
        DataType::Integer,
        DataType::Bigint,
        DataType::Double,
        DataType::Date,
        DataType::Timestamp,
        DataType::Decimal {
            precision: 15,
            scale: 2,
        },
        DataType::Varchar,
    ];

    /// One random cell. The extremes (INTEGER/BIGINT limits, a full-width
    /// DECIMAL, NaN/±Inf/±0.0) are common enough that the overflow and
    /// comparison edge cases are reached inside a few hundred cases.
    fn fuzz_value(rng: &mut Rng, dtype: DataType) -> V {
        match dtype {
            DataType::Integer => V::Integer(match rng.below(4) {
                0 => 0,
                1 => i32::MAX,
                2 => i32::MIN,
                _ => rng.below(200) as i32 - 100,
            }),
            DataType::Bigint => V::Bigint(match rng.below(4) {
                0 => 0,
                1 => i64::MAX,
                2 => i64::MIN,
                _ => rng.below(2000) as i64 - 1000,
            }),
            DataType::Double => V::Double(match rng.below(7) {
                0 => 0.0,
                1 => -0.0,
                2 => f64::NAN,
                3 => f64::INFINITY,
                4 => f64::NEG_INFINITY,
                _ => (rng.below(2000) as f64 - 1000.0) / 8.0,
            }),
            DataType::Date => V::Date(rng.below(200) as i32 - 100),
            DataType::Timestamp => V::Timestamp(rng.below(200) as i64 * 86_400_000_000 - 1000),
            DataType::Decimal { precision, scale } => {
                let wide = 10i128.pow(precision as u32 - scale as u32) - 1;
                V::Decimal(
                    match rng.below(5) {
                        0 => 0,
                        1 => wide,
                        2 => -wide,
                        _ => rng.below(2000) as i128 - 1000,
                    },
                    scale,
                )
            }
            _ => V::Varchar(["", "apple", "banana", "apple pie"][rng.below(4)].to_string()),
        }
    }

    struct FuzzChunk {
        chunk: DataChunk,
        dtypes: [DataType; 7],
    }

    fn fuzz_chunk(rng: &mut Rng, rows: usize) -> FuzzChunk {
        let dtypes = FUZZ_DTYPES;
        let mut cols: Vec<Vector> = dtypes.iter().map(|&d| Vector::new_empty(d)).collect();
        for i in 0..rows {
            for (c, &dtype) in cols.iter_mut().zip(dtypes.iter()) {
                // Row 0 is all NULL and row 1 all valid, so every chunk has an
                // all-NULL row and a row the validity masking must keep.
                let null = i == 0 || (i != 1 && rng.one_in(5));
                if null {
                    c.push_null().unwrap();
                } else {
                    c.push_valid(fuzz_value(rng, dtype)).unwrap();
                }
            }
        }
        FuzzChunk {
            chunk: DataChunk::new(cols).unwrap(),
            dtypes,
        }
    }

    impl FuzzChunk {
        fn column(&self, index: usize) -> B {
            B::Column {
                index,
                dtype: self.dtypes[index],
            }
        }
        fn index(&self, dtype: DataType) -> usize {
            self.dtypes.iter().position(|&d| d == dtype).unwrap()
        }
    }

    /// A random expression of one SQL type and that type: a column, a
    /// literal, or same-type arithmetic (`+ - * %`), which is where the
    /// overflow, modulo-by-zero and DECIMAL rescale paths live.
    fn fuzz_scalar(rng: &mut Rng, fc: &FuzzChunk, dtype: DataType, depth: u32) -> (B, DataType) {
        // Arithmetic only on the numeric types: the binder rejects the rest,
        // so a shape it would not build is not worth fuzzing.
        let numeric = matches!(
            dtype,
            DataType::Integer | DataType::Bigint | DataType::Double | DataType::Decimal { .. }
        );
        if !numeric || depth == 0 || rng.one_in(2) {
            let leaf = if rng.one_in(3) {
                B::Literal(fuzz_value(rng, dtype))
            } else {
                fc.column(fc.index(dtype))
            };
            return (leaf, dtype);
        }
        // DOUBLE has no `%` (the binder never types one).
        let ops: &[BinaryOp] = if dtype == DataType::Double {
            &[BinaryOp::Add, BinaryOp::Subtract, BinaryOp::Multiply]
        } else {
            &[
                BinaryOp::Add,
                BinaryOp::Subtract,
                BinaryOp::Multiply,
                BinaryOp::Modulo,
            ]
        };
        let op = ops[rng.below(ops.len() as u64)];
        // The binder's decimal width rules, so the case shapes are ones a query
        // can actually produce.
        let out = match (op, dtype) {
            (BinaryOp::Multiply, DataType::Decimal { precision, scale }) => DataType::Decimal {
                precision: (precision * 2).min(38),
                scale: (scale * 2).min(38),
            },
            (BinaryOp::Add | BinaryOp::Subtract, DataType::Decimal { precision, scale }) => {
                DataType::Decimal {
                    precision: (precision + 1).min(38),
                    scale,
                }
            }
            _ => dtype,
        };
        let l = fuzz_scalar(rng, fc, dtype, depth - 1).0;
        let r = fuzz_scalar(rng, fc, dtype, depth - 1).0;
        (
            B::Binary {
                op,
                left: Box::new(l),
                right: Box::new(r),
                dtype: out,
            },
            out,
        )
    }

    /// One side of a comparison whose other side has type `dtype`: a numeric
    /// mix is allowed (the binder accepts any numeric pair, and the mixed-type
    /// path is what TPC-H Q6's DECIMAL predicates used to take), everything
    /// else stays on its own type.
    fn fuzz_operand(rng: &mut Rng, fc: &FuzzChunk, dtype: DataType, depth: u32) -> B {
        let numeric = matches!(
            dtype,
            DataType::Integer | DataType::Bigint | DataType::Double | DataType::Decimal { .. }
        );
        if numeric && rng.one_in(2) {
            let other = [
                DataType::Integer,
                DataType::Bigint,
                DataType::Double,
                DataType::Decimal {
                    precision: 15,
                    scale: 2,
                },
            ][rng.below(4)];
            return if rng.one_in(2) {
                B::Literal(fuzz_value(rng, other))
            } else {
                fc.column(fc.index(other))
            };
        }
        if rng.one_in(3) {
            B::Literal(fuzz_value(rng, dtype))
        } else {
            fuzz_scalar(rng, fc, dtype, depth.saturating_sub(1)).0
        }
    }

    fn fuzz_predicate(rng: &mut Rng, fc: &FuzzChunk, depth: u32) -> B {
        match rng.below(if depth < 3 { 8 } else { 3 }) {
            0 | 1 => {
                let dtype = fc.dtypes[rng.below(7)];
                let op = [
                    BinaryOp::Eq,
                    BinaryOp::NotEq,
                    BinaryOp::Lt,
                    BinaryOp::LtEq,
                    BinaryOp::Gt,
                    BinaryOp::GtEq,
                ][rng.below(6)];
                let left = fuzz_operand(rng, fc, dtype, depth);
                let right = fuzz_operand(rng, fc, dtype, depth);
                B::Binary {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                    dtype: DataType::Boolean,
                }
            }
            2 => {
                let dtype = fc.dtypes[rng.below(7)];
                let expr = fuzz_operand(rng, fc, dtype, depth);
                B::IsNull {
                    expr: Box::new(expr),
                    negated: rng.one_in(2),
                }
            }
            3 | 4 => B::Binary {
                op: BinaryOp::And,
                left: Box::new(fuzz_predicate(rng, fc, depth + 1)),
                right: Box::new(fuzz_predicate(rng, fc, depth + 1)),
                dtype: DataType::Boolean,
            },
            5 | 6 => B::Binary {
                op: BinaryOp::Or,
                left: Box::new(fuzz_predicate(rng, fc, depth + 1)),
                right: Box::new(fuzz_predicate(rng, fc, depth + 1)),
                dtype: DataType::Boolean,
            },
            _ => B::Not(Box::new(fuzz_predicate(rng, fc, depth + 1))),
        }
    }

    /// The reference: every row is evaluated on its own, so it shares nothing
    /// with the vector kernels. `AND`/`OR` narrow the way a per-row evaluation
    /// does — the right side only sees rows the left side left undecided.
    fn reference_rows(expr: &B, chunk: &DataChunk, cx: &Cx, sel: &[u32]) -> Result<Vec<u32>> {
        match expr {
            B::Binary {
                op: BinaryOp::And,
                left,
                right,
                ..
            } => {
                let kept = reference_rows(left, chunk, cx, sel)?;
                reference_rows(right, chunk, cx, &kept)
            }
            B::Binary {
                op: BinaryOp::Or,
                left,
                right,
                ..
            } => {
                let kept = reference_rows(left, chunk, cx, sel)?;
                let rest: Vec<u32> = sel.iter().copied().filter(|r| !kept.contains(r)).collect();
                let mut out = kept;
                out.extend(reference_rows(right, chunk, cx, &rest)?);
                out.sort_unstable();
                Ok(out)
            }
            _ => {
                let mut out = Vec::new();
                for &row in sel {
                    let one = chunk.take(&[row]);
                    let v = eval_expr(expr, &one, cx)?;
                    if v.is_valid(0) && v.get(0) == V::Boolean(true) {
                        out.push(row);
                    }
                }
                Ok(out)
            }
        }
    }

    fn selection_of(expr: &B, chunk: &DataChunk, cx: &Cx, sel: &[u32]) -> Result<Vec<u32>> {
        let mut out = sel.to_vec();
        select_predicate(expr, chunk, cx, &mut out)?;
        Ok(out)
    }

    #[test]
    fn select_predicate_matches_a_row_at_a_time_reference() {
        let db = PinkDb::new();
        let cx = test_cx(&db);
        let (mut checked, mut failures, mut narrowed_errors) = (0, 0, 0);
        for seed in 1..=24u64 {
            let mut rng = Rng::new(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
            // Row counts either side of a validity word and of a chunk.
            let rows = [0, 1, 2, 63, 64, 65, 130][(seed % 7) as usize];
            let fc = fuzz_chunk(&mut rng, rows);
            for _ in 0..10 {
                let expr = fuzz_predicate(&mut rng, &fc, 0);
                let all = crate::exec::selection::all_rows(rows);
                let want = reference_rows(&expr, &fc.chunk, &cx, &all);
                let got = selection_of(&expr, &fc.chunk, &cx, &all);
                match (&want, &got) {
                    (Ok(want), Ok(got)) => assert_eq!(got, want, "case {checked}: {expr:?}"),
                    (Err(_), Err(_)) => {
                        // Which of several erroring rows reports first depends
                        // on the evaluation order, so only the fact that both
                        // paths fail is comparable here; the exact text is
                        // pinned by `a_conjunct_error_fires_only_for_…`.
                        failures += 1;
                    }
                    _ => panic!(
                        "case {checked}: reference {want:?} vs selection {got:?} for {expr:?}"
                    ),
                }
                // The vector path must agree wherever the reference has a
                // value: its errors can land on rows the narrowing skipped, so
                // it is compared on success only.
                if let Ok(want) = &want {
                    match eval_expr(&expr, &fc.chunk, &cx) {
                        Ok(v) => {
                            let vector: Vec<u32> = select_true(&v)
                                .unwrap()
                                .into_iter()
                                .map(|i| i as u32)
                                .collect();
                            assert_eq!(&vector, want, "vector path, case {checked}: {expr:?}");
                        }
                        // The whole-chunk evaluation runs expressions on rows
                        // the narrowing excluded; the selection path must not
                        // report those rows (see `a_conjunct_error_fires_…`).
                        Err(_) => narrowed_errors += 1,
                    }
                }
                checked += 1;
            }
        }
        assert!(checked >= 200, "only {checked} cases");
        // The generator must reach the error paths too, not only clean rows.
        assert!(failures > 0, "no error cases generated");
        assert!(
            narrowed_errors > 0,
            "no rows excluded from an erroring conjunct"
        );
    }

    #[test]
    fn a_conjunct_error_fires_only_for_rows_that_reach_it() {
        // `q <= 24 AND 100 % (q - 24) > 3`: the rows above 24 never reach the
        // modulo, so a chunk of them is not an error even though the same
        // expression on its own divides by zero.
        let build = |q: &[i32]| {
            let mut col = Vector::new_empty(DataType::Integer);
            for &v in q {
                col.push_valid(V::Integer(v)).unwrap();
            }
            DataChunk::new(vec![col]).unwrap()
        };
        let column = B::Column {
            index: 0,
            dtype: DataType::Integer,
        };
        let minus = B::Binary {
            op: BinaryOp::Subtract,
            left: Box::new(column.clone()),
            right: Box::new(B::Literal(V::Integer(24))),
            dtype: DataType::Integer,
        };
        let modulo = B::Binary {
            op: BinaryOp::Modulo,
            left: Box::new(B::Literal(V::Integer(100))),
            right: Box::new(minus),
            dtype: DataType::Integer,
        };
        let guard = B::Binary {
            op: BinaryOp::LtEq,
            left: Box::new(column),
            right: Box::new(B::Literal(V::Integer(24))),
            dtype: DataType::Boolean,
        };
        let pred = B::Binary {
            op: BinaryOp::And,
            left: Box::new(guard),
            right: Box::new(B::Binary {
                op: BinaryOp::Gt,
                left: Box::new(modulo.clone()),
                right: Box::new(B::Literal(V::Integer(3))),
                dtype: DataType::Boolean,
            }),
            dtype: DataType::Boolean,
        };
        let db = PinkDb::new();
        let cx = test_cx(&db);

        let above = build(&[30, 40, 50]);
        let mut sel = crate::exec::selection::all_rows(3);
        select_predicate(&pred, &above, &cx, &mut sel).unwrap();
        assert!(sel.is_empty());

        // A row that does reach the modulo reports exactly the old error.
        let at = build(&[24]);
        let mut sel = crate::exec::selection::all_rows(1);
        let got = select_predicate(&pred, &at, &cx, &mut sel).unwrap_err();
        let old = eval_expr(&modulo, &at, &cx).unwrap_err();
        assert_eq!(format!("{got:?}"), format!("{old:?}"));
    }

    #[test]
    fn empty_and_all_null_chunks_select_nothing() {
        let db = PinkDb::new();
        let cx = test_cx(&db);
        let column = B::Column {
            index: 0,
            dtype: DataType::Integer,
        };
        let pred = B::Binary {
            op: BinaryOp::Lt,
            left: Box::new(column),
            right: Box::new(B::Literal(V::Integer(5))),
            dtype: DataType::Boolean,
        };
        // Zero rows.
        let empty = DataChunk::new(vec![Vector::new_empty(DataType::Integer)]).unwrap();
        let mut sel = crate::exec::selection::all_rows(0);
        select_predicate(&pred, &empty, &cx, &mut sel).unwrap();
        assert!(sel.is_empty());
        // Five NULL rows: no row is TRUE, and no value is ever boxed.
        let mut nulls = Vector::new_empty(DataType::Integer);
        for _ in 0..5 {
            nulls.push_null().unwrap();
        }
        let all_null = DataChunk::new(vec![nulls]).unwrap();
        let mut sel = crate::exec::selection::all_rows(5);
        select_predicate(&pred, &all_null, &cx, &mut sel).unwrap();
        assert!(sel.is_empty());
        // A literal TRUE predicate keeps every row of the same chunk.
        let mut sel = crate::exec::selection::all_rows(5);
        select_predicate(&B::Literal(V::Boolean(true)), &all_null, &cx, &mut sel).unwrap();
        assert_eq!(sel, crate::exec::selection::all_rows(5));
    }
}
