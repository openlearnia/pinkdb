//! Vectorized expression evaluation: [`BoundExpr`] × [`DataChunk`] → [`Vector`].
//!
//! Semantics:
//! - NULL propagates through arithmetic, comparisons, NOT, and scalar
//!   functions (SQL three-valued logic).
//! - `AND`/`OR` use Kleene logic; `WHERE` keeps only TRUE rows.
//! - Numeric widening (INTEGER → BIGINT → DOUBLE); `int / int` → DOUBLE.
//! - Integer division/modulo by zero is an error; float follows IEEE 754.

use std::cmp::Ordering;

#[cfg(not(target_arch = "wasm32"))]
use pinkdb_ext_loader::ExtArg;

use super::aggregate::encode_row;
use super::{Cx, Memo, eval};
use crate::plan::compare_values;
use crate::sql::binder::{BoundExpr, ScalarFunc, common_numeric};
use crate::sql::ast::BinaryOp;
use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{DataChunk, Vector, VectorData};
use crate::types::FastSet;

/// Evaluate `expr` over every row of `chunk`.
pub(crate) fn eval_expr(expr: &BoundExpr, chunk: &DataChunk, cx: &Cx) -> Result<Vector> {
    let n = chunk.num_rows();
    match expr {
        BoundExpr::Literal(v) => const_vector(v.clone(), n),
        BoundExpr::Column { index, .. } => Ok(chunk.column(*index).clone()),
        BoundExpr::AggRef { index, .. } => Ok(chunk.column(*index).clone()),
        BoundExpr::WindowRef { index, .. } => Ok(chunk.column(*index).clone()),
        BoundExpr::ScalarSubquery { query, .. } => eval_scalar_subquery(query, chunk, cx),
        BoundExpr::Cast { expr, dtype } => {
            let v = eval_expr(expr, chunk, cx)?;
            eval_cast(&v, *dtype)
        }
        BoundExpr::Coalesce { args, dtype } => {
            let mut vecs = Vec::with_capacity(args.len());
            for a in args {
                vecs.push(eval_expr(a, chunk, cx)?);
            }
            eval_coalesce(*dtype, &vecs)
        }
        BoundExpr::Case { branches, else_, dtype } => {
            eval_case(branches, else_, *dtype, chunk, cx)
        }
        BoundExpr::InSubquery { expr, query, negated } => {
            eval_in_subquery(expr, query, *negated, chunk, cx)
        }
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
                // Typed wrapping loops; DATE/TIMESTAMP stay unsupported below.
                (VectorData::Int32(src), DataType::Integer) => {
                    for (i, &x) in src.iter().enumerate() {
                        if v.is_null(i) {
                            out.push_null()?;
                        } else {
                            out.push_int32(x.wrapping_neg())?;
                        }
                    }
                }
                (VectorData::Int64(src), DataType::Bigint) => {
                    for (i, &x) in src.iter().enumerate() {
                        if v.is_null(i) {
                            out.push_null()?;
                        } else {
                            out.push_int64(x.wrapping_neg())?;
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
        BoundExpr::Binary { op, left, right, dtype } => match op {
            BinaryOp::And | BinaryOp::Or => eval_logic(*op, left, right, chunk, cx),
            BinaryOp::Like | BinaryOp::NotLike => eval_like(*op, left, right, chunk, cx),
            BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt
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
            #[cfg(not(target_arch = "wasm32"))]
            {
                eval_dynamic_scalar(name, args, *dtype, chunk, cx)
            }
            // Unreachable in practice (a wasm binder can never build this
            // variant): mirrors the wasm `PinkDb::load_extension` stub.
            #[cfg(target_arch = "wasm32")]
            {
                let _ = (name, args, dtype, chunk, cx);
                Err(PinkDbError::Execution(
                    "extensions are not supported on wasm targets".to_string(),
                ))
            }
        }
    }
}

/// Row-at-a-time callback into a scalar function registered by a loaded
/// extension.
///
/// Arguments evaluate vector-at-a-time over the chunk (exactly like a static
/// scalar function); the callback then runs once per row with the ABI's
/// by-value argument structs. NULL cells are passed through with `is_null`
/// set — the extension decides what NULL means (documented in
/// `pinkdb_extension.h`). VARCHAR arguments borrow their bytes for the call;
/// VARCHAR results are copied out of extension memory by the loader, which
/// also frees the extension's buffer immediately.
#[cfg(not(target_arch = "wasm32"))]
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
    let declared = function.arg_types();
    for row in 0..chunk.num_rows() {
        ext_args.clear();
        for (i, v) in vecs.iter().enumerate() {
            crate::ext::push_ext_arg(&mut ext_args, v, row, declared[i]);
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
    cx.memo.lock().expect("memo").insert(key, Memo::Scalar(value.clone()));
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
    cx.memo.lock().expect("memo").insert(key, Memo::Set(set, has_null, common));
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
        other => Err(PinkDbError::Execution(format!("expected BOOLEAN, got {other}"))),
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
        // Everything else: existing generic loop (errors unchanged).
        _ => {
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
    }
}

fn coerce_value(v: Value, target: DataType) -> Result<Value> {
    match (v, target) {
        (Value::Integer(i), DataType::Bigint) => Ok(Value::Bigint(i as i64)),
        (Value::Integer(i), DataType::Double) => Ok(Value::Double(i as f64)),
        (Value::Bigint(i), DataType::Double) => Ok(Value::Double(i as f64)),
        (v, _) => Err(PinkDbError::Execution(format!("cannot coerce {v}"))),
    }
}

fn negate_value(v: Value, dtype: DataType) -> Result<Value> {
    match (v, dtype) {
        (Value::Integer(i), DataType::Integer) => Ok(Value::Integer(i.wrapping_neg())),
        (Value::Bigint(i), DataType::Bigint) => Ok(Value::Bigint(i.wrapping_neg())),
        (Value::Double(x), DataType::Double) => Ok(Value::Double(-x)),
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
    // Dates skip numeric coercion (it rejects them): day arithmetic below.
    if l.dtype() == DataType::Date || r.dtype() == DataType::Date {
        return eval_date_arith(op, &l, &r, dtype);
    }
    // Integer division by zero errors even though `/` widens to DOUBLE
    // (DuckDB rule); 0.0/0.0 with a float operand stays IEEE Inf/NaN.
    let int_div = op == BinaryOp::Divide && l.dtype() != DataType::Double && r.dtype() != DataType::Double;
    // Move instead of deep-cloning when a side already has the target dtype.
    let l = if l.dtype() == dtype { l } else { coerce_to(&l, dtype)? };
    let r = if r.dtype() == dtype { r } else { coerce_to(&r, dtype)? };
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
            return Err(PinkDbError::Execution("integer division by zero".to_string()));
        }
        out.push_valid(arith_values(op, l.get(i), r.get(i))?)?;
    }
    Ok(out)
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
        _ => Ok(None),
    }
}

/// INTEGER `+`/`-`/`*`/`%` with `arith_values`' wrapping semantics.
fn arith_int32(op: BinaryOp, l: &Vector, r: &Vector, a: &[i32], b: &[i32]) -> Result<Vector> {
    let mut out = Vector::new_empty(DataType::Integer);
    for (i, (&x, &y)) in a.iter().zip(b).enumerate() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
        let v = match op {
            BinaryOp::Add => x.wrapping_add(y),
            BinaryOp::Subtract => x.wrapping_sub(y),
            BinaryOp::Multiply => x.wrapping_mul(y),
            BinaryOp::Modulo => {
                if y == 0 {
                    return Err(PinkDbError::Execution("integer modulo by zero".to_string()));
                }
                x.wrapping_rem(y)
            }
            _ => unreachable!("arith_int32: {op:?}"),
        };
        out.push_int32(v)?;
    }
    Ok(out)
}

/// BIGINT `+`/`-`/`*`/`%` with `arith_values`' wrapping semantics.
fn arith_int64(op: BinaryOp, l: &Vector, r: &Vector, a: &[i64], b: &[i64]) -> Result<Vector> {
    let mut out = Vector::new_empty(DataType::Bigint);
    for (i, (&x, &y)) in a.iter().zip(b).enumerate() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
        let v = match op {
            BinaryOp::Add => x.wrapping_add(y),
            BinaryOp::Subtract => x.wrapping_sub(y),
            BinaryOp::Multiply => x.wrapping_mul(y),
            BinaryOp::Modulo => {
                if y == 0 {
                    return Err(PinkDbError::Execution("integer modulo by zero".to_string()));
                }
                x.wrapping_rem(y)
            }
            _ => unreachable!("arith_int64: {op:?}"),
        };
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
            return Err(PinkDbError::Execution("integer division by zero".to_string()));
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

fn eval_date_arith(op: BinaryOp, l: &Vector, r: &Vector, dtype: DataType) -> Result<Vector> {
    let mut out = Vector::new_empty(dtype);
    for i in 0..l.len() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
        let v = match (op, l.get(i), r.get(i)) {
            (BinaryOp::Add, Value::Date(d), Value::Integer(n)) => {
                Value::Date(d.wrapping_add(n))
            }
            (BinaryOp::Add, Value::Integer(n), Value::Date(d)) => {
                Value::Date(d.wrapping_add(n))
            }
            (BinaryOp::Subtract, Value::Date(d), Value::Integer(n)) => {
                Value::Date(d.wrapping_sub(n))
            }
            (BinaryOp::Subtract, Value::Date(a), Value::Date(b)) => {
                Value::Bigint(a as i64 - b as i64)
            }
            (op, a, b) => {
                return Err(PinkDbError::Execution(format!(
                    "operator {op:?} does not support {a} and {b}"
                )));
            }
        };
        out.push_valid(v)?;
    }
    Ok(out)
}

fn arith_values(op: BinaryOp, l: Value, r: Value) -> Result<Value> {
    match (l, r) {
        (Value::Integer(a), Value::Integer(b)) => {
            let v = match op {
                BinaryOp::Add => a.wrapping_add(b),
                BinaryOp::Subtract => a.wrapping_sub(b),
                BinaryOp::Multiply => a.wrapping_mul(b),
                BinaryOp::Modulo => {
                    if b == 0 {
                        return Err(PinkDbError::Execution("integer modulo by zero".to_string()));
                    }
                    a.wrapping_rem(b)
                }
                _ => unreachable!("binder types {op:?} on INTEGER"),
            };
            Ok(Value::Integer(v))
        }
        (Value::Bigint(a), Value::Bigint(b)) => {
            let v = match op {
                BinaryOp::Add => a.wrapping_add(b),
                BinaryOp::Subtract => a.wrapping_sub(b),
                BinaryOp::Multiply => a.wrapping_mul(b),
                BinaryOp::Modulo => {
                    if b == 0 {
                        return Err(PinkDbError::Execution("integer modulo by zero".to_string()));
                    }
                    a.wrapping_rem(b)
                }
                _ => unreachable!("binder types {op:?} on BIGINT"),
            };
            Ok(Value::Bigint(v))
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
        _ => Err(PinkDbError::Execution(format!("expected numeric, got {v}"))),
    }
}

fn as_f64(v: &Value) -> Result<f64> {
    match v {
        Value::Integer(i) => Ok(*i as f64),
        Value::Bigint(i) => Ok(*i as f64),
        Value::Double(x) => Ok(*x),
        _ => Err(PinkDbError::Execution(format!("expected numeric, got {v}"))),
    }
}

fn eval_scalar(func: ScalarFunc, dtype: DataType, args: &[Vector]) -> Result<Vector> {
    let n = args.first().map(|v| v.len()).unwrap_or(0);
    for v in args {
        if v.len() != n {
            return Err(PinkDbError::Execution("scalar arg length mismatch".to_string()));
        }
    }
    // Common single-arg functions read typed payloads directly (no per-row
    // `Vec<Value>`); rare shapes fall through to the generic row loop.
    if let Some(out) = eval_scalar_fast(func, dtype, args)? {
        return Ok(out);
    }
    // Any-NULL-in → NULL-out (all v1 scalar functions are strict).
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
        out.push_valid(apply_scalar(func, &row)?)?;
    }
    Ok(out)
}

/// Typed fast paths for the common scalar functions. `Ok(None)` ⇒ arg
/// shape not covered: caller runs the generic `apply_scalar` loop, which
/// preserves the old errors for bad shapes/types.
fn eval_scalar_fast(
    func: ScalarFunc,
    dtype: DataType,
    args: &[Vector],
) -> Result<Option<Vector>> {
    match func {
        ScalarFunc::Upper | ScalarFunc::Lower
            if args.len() == 1 && dtype == DataType::Varchar =>
        {
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
                            out.push_int32(x.wrapping_abs())?;
                        }
                    }
                    Ok(Some(out))
                }
                (VectorData::Int64(s), DataType::Bigint) => {
                    for (i, &x) in s.iter().enumerate() {
                        if v.is_null(i) {
                            out.push_null()?;
                        } else {
                            out.push_int64(x.wrapping_abs())?;
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

fn apply_scalar(func: ScalarFunc, row: &[Value]) -> Result<Value> {
    match func {
        ScalarFunc::Abs => match &row[0] {
            Value::Integer(i) => Ok(Value::Integer(i.wrapping_abs())),
            Value::Bigint(i) => Ok(Value::Bigint(i.wrapping_abs())),
            Value::Double(x) => Ok(Value::Double(x.abs())),
            v => Err(PinkDbError::Execution(format!("abs of {v}"))),
        },
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
                            day_us + h as i64 * 3_600_000_000
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
        }        ScalarFunc::Substr => {
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
    }
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
        out.push_valid(Value::Boolean(if op == BinaryOp::Like { hit } else { !hit }))?;
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

fn cast_value(v: Value, target: DataType) -> Result<Value> {
    match (v, target) {
        (v, t) if v.data_type() == Some(t) => Ok(v),
        // Numeric tower (float→int truncates toward zero, wrapping on overflow).
        (Value::Integer(i), DataType::Bigint) => Ok(Value::Bigint(i as i64)),
        (Value::Integer(i), DataType::Double) => Ok(Value::Double(i as f64)),
        (Value::Bigint(i), DataType::Double) => Ok(Value::Double(i as f64)),
        (Value::Bigint(i), DataType::Integer) => Ok(Value::Integer(i as i32)),
        (Value::Double(x), DataType::Integer) => Ok(Value::Integer(x as i32)),
        (Value::Double(x), DataType::Bigint) => Ok(Value::Bigint(x as i64)),
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
            _ => Err(PinkDbError::Execution(format!("cannot CAST \"{s}\" AS BOOLEAN"))),
        },
        (Value::Varchar(s), DataType::Integer) => s.trim().parse::<i32>().map(Value::Integer).map_err(|_| {
            PinkDbError::Execution(format!("cannot CAST \"{s}\" AS INTEGER"))
        }),
        (Value::Varchar(s), DataType::Bigint) => s.trim().parse::<i64>().map(Value::Bigint).map_err(|_| {
            PinkDbError::Execution(format!("cannot CAST \"{s}\" AS BIGINT"))
        }),
        (Value::Varchar(s), DataType::Double) => s.trim().parse::<f64>().map(Value::Double).map_err(|_| {
            PinkDbError::Execution(format!("cannot CAST \"{s}\" AS DOUBLE"))
        }),
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
        (v, t) => Err(PinkDbError::Execution(format!("cannot CAST {v} AS {t}"))),
    }
}

/// `CASE WHEN c THEN v ... [ELSE e]`: first TRUE condition wins per row
/// (NULL conditions never match). Missing ELSE is NULL.
fn eval_case(
    branches: &[(BoundExpr, BoundExpr)],
    else_: &Option<Box<BoundExpr>>,
    dtype: DataType,
    chunk: &DataChunk,
    cx: &Cx,
) -> Result<Vector> {
    let mut conds = Vec::with_capacity(branches.len());
    let mut vals = Vec::with_capacity(branches.len());
    for (c, v) in branches {
        conds.push(eval_expr(c, chunk, cx)?);
        vals.push(eval_expr(v, chunk, cx)?);
    }
    let els = match else_ {
        Some(e) => Some(eval_expr(e, chunk, cx)?),
        None => None,
    };
    let mut out = Vector::new_empty(dtype);
    for i in 0..chunk.num_rows() {
        let mut placed = false;
        for (c, v) in conds.iter().zip(vals.iter()) {
            if !c.is_null(i) && as_bool(c, i)? {
                out.push_value(v.get(i))?;
                placed = true;
                break;
            }
        }
        if !placed {
            match &els {
                Some(e) => out.push_value(e.get(i))?,
                None => out.push_null()?,
            }
        }
    }
    Ok(out)
}

/// `COALESCE`: first non-NULL argument per row (all args share the dtype).
fn eval_coalesce(dtype: DataType, args: &[Vector]) -> Result<Vector> {
    let n = args.first().map(|v| v.len()).unwrap_or(0);
    let mut out = Vector::new_empty(dtype);
    for i in 0..n {
        let mut placed = false;
        for v in args {
            if !v.is_null(i) {
                out.push_valid(v.get(i))?;
                placed = true;
                break;
            }
        }
        if !placed {
            out.push_null()?;
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
            left: Box::new(B::Column { index: 0, dtype: DataType::Integer }),
            right: Box::new(B::Column { index: 1, dtype: DataType::Double }),
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
        let col = |i| B::Column { index: i, dtype: DataType::Boolean };
        let and = B::Binary { op: BinaryOp::And, left: Box::new(col(0)), right: Box::new(col(1)), dtype: DataType::Boolean };
        let or = B::Binary { op: BinaryOp::Or, left: Box::new(col(0)), right: Box::new(col(1)), dtype: DataType::Boolean };
        let db = PinkDb::new();
        let cx = test_cx(&db);
        let a = eval_expr(&and, &c, &cx).unwrap();
        assert_eq!((a.get(0), a.get(1), a.get(2)), (V::Boolean(false), V::Null, V::Boolean(true)));
        let o = eval_expr(&or, &c, &cx).unwrap();
        assert_eq!((o.get(0), o.get(1), o.get(2)), (V::Null, V::Boolean(true), V::Boolean(true)));
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
                left: Box::new(B::Column { index: 0, dtype: DataType::Double }),
                right: Box::new(B::Column { index: 1, dtype: DataType::Double }),
                dtype: DataType::Boolean,
            };
            eval_expr(&e, &chunk, &cx).unwrap()
        };

        // Row 0: NaN vs 1.0 ⇒ Equal; row 1: 1.0 vs NaN ⇒ Equal.
        let eq = cmp(BinaryOp::Eq);
        assert_eq!((eq.get(0), eq.get(1)), (V::Boolean(true), V::Boolean(true)));
        let ne = cmp(BinaryOp::NotEq);
        assert_eq!((ne.get(0), ne.get(1)), (V::Boolean(false), V::Boolean(false)));
        let lt = cmp(BinaryOp::Lt);
        assert_eq!((lt.get(0), lt.get(1)), (V::Boolean(false), V::Boolean(false)));
        let gt = cmp(BinaryOp::Gt);
        assert_eq!((gt.get(0), gt.get(1)), (V::Boolean(false), V::Boolean(false)));
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
                left: Box::new(B::Column { index: 0, dtype: DataType::Varchar }),
                right: Box::new(B::Column { index: 1, dtype: DataType::Varchar }),
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
    fn arith_wrapping_and_division_errors() {
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
            left: Box::new(B::Column { index: 0, dtype: DataType::Integer }),
            right: Box::new(B::Column { index: 1, dtype: DataType::Integer }),
            dtype,
        };

        // i32::MAX + 1 wraps to i32::MIN (`arith_values` parity).
        let add = eval_expr(&bin(BinaryOp::Add, DataType::Integer), &chunk, &cx).unwrap();
        assert_eq!(add.get(0), V::Integer(i32::MIN));

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
            left: Box::new(B::Column { index: 0, dtype: DataType::Double }),
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
                args: vec![B::Column { index: 0, dtype: DataType::Varchar }],
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

        // ABS (wrapping) and one-arg ROUND on numeric columns.
        let mut d = Vector::new_empty(DataType::Integer);
        d.push_valid(V::Integer(i32::MIN)).unwrap();
        d.push_null().unwrap();
        let mut f = Vector::new_empty(DataType::Double);
        f.push_valid(V::Double(2.5)).unwrap();
        f.push_valid(V::Double(-2.5)).unwrap();
        let chunk2 = DataChunk::new(vec![d, f]).unwrap();
        let abs = B::Scalar {
            func: ScalarFunc::Abs,
            args: vec![B::Column { index: 0, dtype: DataType::Integer }],
            dtype: DataType::Integer,
        };
        let a = eval_expr(&abs, &chunk2, &cx).unwrap();
        assert_eq!(a.get(0), V::Integer(i32::MIN)); // wrapping_abs parity
        assert_eq!(a.get(1), V::Null);
        let round = B::Scalar {
            func: ScalarFunc::Round,
            args: vec![B::Column { index: 1, dtype: DataType::Double }],
            dtype: DataType::Double,
        };
        let out = eval_expr(&round, &chunk2, &cx).unwrap();
        assert_eq!((out.get(0), out.get(1)), (V::Double(3.0), V::Double(-3.0)));
    }
}
