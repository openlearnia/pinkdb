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
use crate::plan::compare_values;
use crate::sql::ast::BinaryOp;
use crate::sql::binder::{BoundExpr, ScalarFunc, common_numeric};
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
        BoundExpr::ScalarSubquery { query, .. } => eval_scalar_subquery(query, chunk, cx),
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
        } => eval_in_subquery(expr, query, *negated, chunk, cx),
        BoundExpr::Exists { query, negated } => eval_exists(query, *negated, chunk, cx),
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
        BoundExpr::Scalar { func, args, dtype } => {
            let mut vecs = Vec::with_capacity(args.len());
            for a in args {
                vecs.push(eval_expr(a, chunk, cx)?);
            }
            eval_scalar(*func, *dtype, &vecs)
        }
        BoundExpr::DynamicScalar { name, args, dtype } => {
            eval_dynamic_scalar(name, args, *dtype, chunk, cx)
        }
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
    // Reused per row: no allocation once warm.
    let mut ext_args: Vec<ExtArg<'_>> = Vec::with_capacity(vecs.len());
    for row in 0..chunk.num_rows() {
        ext_args.clear();
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
            crate::ext::push_ext_arg(&mut ext_args, v, row, arg_type);
        }
        let value = function
            .call(&ext_args)
            .map_err(|e| PinkDbError::Execution(e.to_string()))?;
        out.push_valid(crate::ext::ext_value_to_value(name, dtype, value)?)?;
    }
    Ok(out)
}

fn memo_key(query: &crate::plan::LogicalPlan) -> usize {
    (query as *const crate::plan::LogicalPlan) as usize
}

/// Scalar subquery: 0 rows → NULL, 1 row → broadcast, else error.
/// Memoised per plan node: uncorrelated ⇒ stable within a statement.
fn eval_scalar_subquery(
    query: &crate::plan::LogicalPlan,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    let key = memo_key(query);
    if let Some(Memo::Scalar(v)) = cx.memo.lock().expect("memo").get(&key) {
        let v = v.clone();
        return const_vector(v, chunk.num_rows());
    }
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
    cx.memo
        .lock()
        .expect("memo")
        .insert(key, Memo::Scalar(value.clone()));
    const_vector(value, chunk.num_rows())
}

/// `lhs [NOT] IN (query)` with SQL three-valued logic: a match is TRUE;
/// otherwise NULL if the LHS is NULL or the set holds a NULL, else FALSE.
fn eval_in_subquery(
    lhs: &BoundExpr,
    query: &crate::plan::LogicalPlan,
    negated: bool,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    let key = memo_key(query);
    let l = eval_expr(lhs, chunk, cx)?;
    // Hit path borrows the set (no clone); the borrow ends at return.
    if let Some(Memo::Set(set, has_null, common)) = cx.memo.lock().expect("memo").get(&key) {
        return probe_in(&l, set, *has_null, *common, negated);
    }
    let out = eval(cx, query)?;
    let rhs_t = out.types.first().copied().unwrap_or(DataType::Integer);
    let common = common_numeric(l.dtype(), rhs_t).ok();
    let mut set = FastSet::default();
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
    let result = probe_in(&l, &set, has_null, common, negated)?;
    cx.memo
        .lock()
        .expect("memo")
        .insert(key, Memo::Set(set, has_null, common));
    Ok(result)
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
    for i in 0..l.len() {
        let hit = if l.is_null(i) {
            false
        } else {
            buf.clear();
            encode_row(&mut buf, &[l.get(i)]);
            set.contains(&buf)
        };
        let v: Option<bool> = if hit {
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
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    let key = memo_key(query);
    if let Some(Memo::Flag(b)) = cx.memo.lock().expect("memo").get(&key) {
        let b = *b;
        return const_vector(Value::Boolean(b != negated), chunk.num_rows());
    }
    let out = eval(cx, query)?;
    let any = out.chunks.iter().any(|c| c.num_rows() > 0);
    cx.memo.lock().expect("memo").insert(key, Memo::Flag(any));
    const_vector(Value::Boolean(any != negated), chunk.num_rows())
}

/// Broadcast a scalar to `n` rows. `NULL` ⇒ INTEGER-typed all-NULL vector
/// (matches the old `push_value` loop); VARCHAR shares one `Arc<str>`.
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
    // clone elsewhere), which for a 1M-row filter dwarfs the comparison
    // itself. Same-dtype only: mixed numeric widths keep the generic
    // value-by-value path below.
    if left.dtype() == right.dtype() {
        let fast = match (left, right) {
            (BoundExpr::Literal(v), col)
                if !v.is_null() && !matches!(col, BoundExpr::Literal(_)) =>
            {
                Some((v, col, true))
            }
            (col, BoundExpr::Literal(v))
                if !v.is_null() && !matches!(col, BoundExpr::Literal(_)) =>
            {
                Some((v, col, false))
            }
            _ => None,
        };
        if let Some((scalar, col_expr, scalar_left)) = fast {
            let col = eval_expr(col_expr, chunk, cx)?;
            let mut out = Vector::with_capacity(DataType::Boolean, col.len());
            let handled = match (col.data(), scalar) {
                (VectorData::Boolean(a), Value::Boolean(s)) => {
                    cmp_scalar(op, &col, a, s, scalar_left, |x, y| x.cmp(y), &mut out)?;
                    true
                }
                (VectorData::Int32(a), Value::Integer(s)) => {
                    cmp_scalar(op, &col, a, s, scalar_left, |x, y| x.cmp(y), &mut out)?;
                    true
                }
                (VectorData::Int64(a), Value::Bigint(s)) => {
                    cmp_scalar(op, &col, a, s, scalar_left, |x, y| x.cmp(y), &mut out)?;
                    true
                }
                (VectorData::Float64(a), Value::Double(s)) => {
                    cmp_scalar(
                        op,
                        &col,
                        a,
                        s,
                        scalar_left,
                        |x, y| x.partial_cmp(y).unwrap_or(Ordering::Equal),
                        &mut out,
                    )?;
                    true
                }
                (VectorData::VarChar(a), Value::Varchar(s)) => {
                    // One `Arc<str>` for the scalar (one allocation per chunk)
                    // instead of one clone per element.
                    let s: std::sync::Arc<str> = std::sync::Arc::from(s.as_str());
                    cmp_scalar(
                        op,
                        &col,
                        a,
                        &s,
                        scalar_left,
                        |x, y| (**x).cmp(&**y),
                        &mut out,
                    )?;
                    true
                }
                (VectorData::Date32(a), Value::Date(s)) => {
                    cmp_scalar(op, &col, a, s, scalar_left, |x, y| x.cmp(y), &mut out)?;
                    true
                }
                (VectorData::TimestampMicros(a), Value::Timestamp(s)) => {
                    cmp_scalar(op, &col, a, s, scalar_left, |x, y| x.cmp(y), &mut out)?;
                    true
                }
                // DECIMAL vs DECIMAL literal of the same scale: no rescaling.
                (VectorData::Decimal { values, scale, .. }, Value::Decimal(sv, ss))
                    if scale == ss =>
                {
                    cmp_scalar(
                        op,
                        &col,
                        values.as_slice(),
                        sv,
                        scalar_left,
                        |x, y| x.cmp(y),
                        &mut out,
                    )?;
                    true
                }
                (
                    VectorData::Interval(a),
                    Value::Interval {
                        months,
                        days,
                        micros,
                    },
                ) => {
                    let sv = crate::types::Interval {
                        months: *months,
                        days: *days,
                        micros: *micros,
                    };
                    cmp_scalar(
                        op,
                        &col,
                        a.as_slice(),
                        &sv,
                        scalar_left,
                        |x, y| crate::types::interval_cmp(*x, *y),
                        &mut out,
                    )?;
                    true
                }
                _ => false,
            };
            if handled {
                return Ok(out);
            }
            // A shape the binder cannot produce for this dtype: fall through
            // to the general path (re-evaluating the column has no side
            // effects).
        }
    }
    let l = eval_expr(left, chunk, cx)?;
    let r = eval_expr(right, chunk, cx)?;
    let mut out = Vector::new_empty(DataType::Boolean);
    if l.dtype() == r.dtype() {
        // Same dtype ⇒ same layout; each arm mirrors `compare_values` for
        // that type. DOUBLE keeps `num_cmp`: NaN compares Equal to
        // everything (`partial_cmp(..).unwrap_or(Equal)`, NOT `total_cmp`).
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
                    |x, y| x.partial_cmp(y).unwrap_or(Ordering::Equal),
                    &mut out,
                )?;
            }
            (VectorData::VarChar(a), VectorData::VarChar(b)) => {
                // Borrowed `&str` compare — no per-row string clone.
                cmp_loop(op, &l, &r, a, b, |x, y| (**x).cmp(&**y), &mut out)?;
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
/// Scalar companion of [`cmp_loop`] for the column-vs-literal fast path:
/// `col op scalar` (or `scalar op col` when `scalar_left`), NULL-preserving.
fn cmp_scalar<T>(
    op: BinaryOp,
    col: &Vector,
    a: &[T],
    s: &T,
    scalar_left: bool,
    cmp: impl Fn(&T, &T) -> Ordering,
    out: &mut Vector,
) -> Result<()> {
    for (i, x) in a.iter().enumerate() {
        if col.is_null(i) {
            out.push_null()?;
        } else {
            let ord = if scalar_left { cmp(s, x) } else { cmp(x, s) };
            out.push_bool(ord_matches(ord, op))?;
        }
    }
    Ok(())
}

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
        (VectorData::Int32(src), DataType::Decimal { precision, scale }) => {
            let mut out: Vec<i128> = Vec::with_capacity(src.len());
            for (i, &x) in src.iter().enumerate() {
                if v.is_null(i) {
                    out.push(0);
                    continue;
                }
                out.push(
                    crate::types::int_to_decimal(x as i128, precision, scale).ok_or_else(|| {
                        PinkDbError::Execution(format!(
                            "cannot coerce {x} to DECIMAL({precision},{scale}): value out of range"
                        ))
                    })?,
                );
            }
            let data = VectorData::Decimal {
                values: out,
                precision,
                scale,
            };
            Ok(Vector::from_parts(data, v.validity().to_vec(), v.len()))
        }
        (VectorData::Int64(src), DataType::Decimal { precision, scale }) => {
            let mut out: Vec<i128> = Vec::with_capacity(src.len());
            for (i, &x) in src.iter().enumerate() {
                if v.is_null(i) {
                    out.push(0);
                    continue;
                }
                out.push(
                    crate::types::int_to_decimal(x as i128, precision, scale).ok_or_else(|| {
                        PinkDbError::Execution(format!(
                            "cannot coerce {x} to DECIMAL({precision},{scale}): value out of range"
                        ))
                    })?,
                );
            }
            let data = VectorData::Decimal {
                values: out,
                precision,
                scale,
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
    let mut out = Vector::new_empty(dtype);
    for i in 0..l.len() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
        let (a, b) = (lv[i], rv[i]);
        let product = a
            .checked_mul(b)
            .filter(|p| crate::types::decimal_fits(*p, precision));
        let product = product.ok_or_else(|| {
            PinkDbError::Execution(format!(
                "Overflow in multiplication of DECIMAL({precision}) ({} * {})",
                crate::types::format_decimal(a, ls),
                crate::types::format_decimal(b, rs)
            ))
        })?;
        out.push_decimal(product, ls + rs)?;
    }
    Ok(out)
}

/// A DECIMAL operand as unscaled i128 values plus their scale. Integer
/// operands are scale-0 values (the decimal *widths* only matter at bind
/// time, for the result precision).
fn decimal_operand(v: &Vector) -> Result<(Vec<i128>, u8)> {
    Ok(match v.data() {
        VectorData::Decimal { values, scale, .. } => (values.clone(), *scale),
        VectorData::Int32(src) => (src.iter().map(|&x| x as i128).collect(), 0),
        VectorData::Int64(src) => (src.iter().map(|&x| x as i128).collect(), 0),
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
    let what = match op {
        BinaryOp::Add => "addition",
        BinaryOp::Subtract => "subtraction",
        BinaryOp::Modulo => "modulo",
        _ => unreachable!("arith_decimal: {op:?}"),
    };
    let mut out = Vector::new_empty(DataType::Decimal { precision, scale });
    for (i, (&x, &y)) in a.iter().zip(b).enumerate() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
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
        let v = match op {
            BinaryOp::Add => x
                .checked_add(y)
                .filter(|v| crate::types::decimal_fits(*v, precision)),
            BinaryOp::Subtract => x
                .checked_sub(y)
                .filter(|v| crate::types::decimal_fits(*v, precision)),
            _ => {
                if y == 0 {
                    return Err(PinkDbError::Execution(format!(
                        "Division by zero in expression {}",
                        detail()
                    )));
                }
                Some(x.wrapping_rem(y))
            }
        }
        .ok_or_else(|| {
            PinkDbError::Execution(format!(
                "Overflow in {what} of DECIMAL({precision}) {}",
                detail()
            ))
        })?;
        out.push_decimal(v, scale)?;
    }
    Ok(out)
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
    let mut out = Vector::new_empty(DataType::Integer);
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
    let mut out = Vector::new_empty(DataType::Bigint);
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
    let mut out = Vector::new_empty(DataType::Double);
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
            out.push_valid(apply_scalar_loose(func, &row)?)?;
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
fn apply_scalar_loose(func: ScalarFunc, row: &[Value]) -> Result<Value> {
    match func {
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
            v => Err(PinkDbError::Execution(format!("length of {v}"))),
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
                Value::Varchar(s) => s.clone(),
                v => return Err(PinkDbError::Execution(format!("substr of {v}"))),
            };
            // 1-indexed; start clamps to ≥1, negative/zero length → "".
            let start = as_i64(&row[1])?.max(1) as usize;
            let chars: Vec<char> = s.chars().collect();
            let from = (start - 1).min(chars.len());
            let take = if row.len() > 2 {
                as_i64(&row[2])?.max(0) as usize
            } else {
                chars.len()
            };
            Ok(Value::Varchar(chars[from..].iter().take(take).collect()))
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
        ScalarFunc::Concat | ScalarFunc::NullIf | ScalarFunc::Greatest | ScalarFunc::Least => {
            unreachable!("non-strict scalar reached the strict path")
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
/// char; no ESCAPE clause in v1 — backslash is literal).
fn eval_like(
    op: BinaryOp,
    left: &BoundExpr,
    right: &BoundExpr,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    let l = eval_expr(left, chunk, cx)?;
    let r = eval_expr(right, chunk, cx)?;
    let mut out = Vector::new_empty(DataType::Boolean);
    for i in 0..l.len() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
        let (s, p) = match (l.get(i), r.get(i)) {
            (Value::Varchar(s), Value::Varchar(p)) => (s, p),
            _ => {
                return Err(PinkDbError::Execution(
                    "LIKE needs VARCHAR on both sides".to_string(),
                ));
            }
        };
        let hit = like_match(&s, &p);
        out.push_valid(Value::Boolean(if op == BinaryOp::Like {
            hit
        } else {
            !hit
        }))?;
    }
    Ok(out)
}

/// Classic `%`/`_` wildcard match (iterative backtracking over chars).
fn like_match(s: &str, pat: &str) -> bool {
    let s: Vec<char> = s.chars().collect();
    let p: Vec<char> = pat.chars().collect();
    let (mut si, mut pi) = (0usize, 0usize);
    let (mut star, mut mark) = (None, 0usize);
    while si < s.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == s[si]) {
            si += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star = Some(pi);
            mark = si;
            pi += 1;
        } else if let Some(st) = star {
            pi = st + 1;
            mark += 1;
            si = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
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

fn cast_out_of_range(v: &Value, target: DataType) -> PinkDbError {
    PinkDbError::Execution(format!("cannot CAST {v} AS {target}: value out of range"))
}

/// Convert one non-NULL value to `target` (CAST semantics; NULL is handled by
/// callers). Shared with the prepared-statement bind step, which converts a
/// bound parameter to its inferred slot type.
pub(crate) fn cast_value(v: Value, target: DataType) -> Result<Value> {
    match (v, target) {
        (v, t) if v.data_type() == Some(t) => Ok(v),
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
        (Value::Varchar(s), DataType::Timestamp) => crate::types::parse_timestamp(s.trim())
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
        // plan::compare_values: NaN compares Equal to everything. Pin that
        // reference behavior first, then the typed DOUBLE kernel.
        assert_eq!(
            compare_values(&V::Double(f64::NAN), &V::Double(1.0)),
            Ordering::Equal
        );
        assert_eq!(
            compare_values(&V::Double(1.0), &V::Double(f64::NAN)),
            Ordering::Equal
        );

        let mut l = Vector::new_empty(DataType::Double);
        l.push_valid(V::Double(f64::NAN)).unwrap();
        l.push_valid(V::Double(1.0)).unwrap();
        l.push_null().unwrap();
        l.push_valid(V::Double(2.0)).unwrap();
        let mut r = Vector::new_empty(DataType::Double);
        r.push_valid(V::Double(1.0)).unwrap();
        r.push_valid(V::Double(f64::NAN)).unwrap();
        r.push_valid(V::Double(5.0)).unwrap();
        r.push_null().unwrap();
        let chunk = DataChunk::new(vec![l, r]).unwrap();
        let db = PinkDb::new();
        let cx = test_cx(&db);
        let cmp = |op: BinaryOp| {
            let e = B::Binary {
                op,
                left: Box::new(B::Column {
                    index: 0,
                    dtype: DataType::Double,
                }),
                right: Box::new(B::Column {
                    index: 1,
                    dtype: DataType::Double,
                }),
                dtype: DataType::Boolean,
            };
            eval_expr(&e, &chunk, &cx).unwrap()
        };

        // Row 0: NaN vs 1.0 ⇒ Equal; row 1: 1.0 vs NaN ⇒ Equal.
        let eq = cmp(BinaryOp::Eq);
        assert_eq!((eq.get(0), eq.get(1)), (V::Boolean(true), V::Boolean(true)));
        let ne = cmp(BinaryOp::NotEq);
        assert_eq!(
            (ne.get(0), ne.get(1)),
            (V::Boolean(false), V::Boolean(false))
        );
        let lt = cmp(BinaryOp::Lt);
        assert_eq!(
            (lt.get(0), lt.get(1)),
            (V::Boolean(false), V::Boolean(false))
        );
        let gt = cmp(BinaryOp::Gt);
        assert_eq!(
            (gt.get(0), gt.get(1)),
            (V::Boolean(false), V::Boolean(false))
        );
        let le = cmp(BinaryOp::LtEq);
        assert_eq!((le.get(0), le.get(1)), (V::Boolean(true), V::Boolean(true)));
        let ge = cmp(BinaryOp::GtEq);
        assert_eq!((ge.get(0), ge.get(1)), (V::Boolean(true), V::Boolean(true)));
        // NULL on either side ⇒ NULL.
        assert_eq!((eq.get(2), eq.get(3)), (V::Null, V::Null));
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
}
