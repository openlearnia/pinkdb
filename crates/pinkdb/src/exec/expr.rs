//! Vectorized expression evaluation: [`BoundExpr`] × [`DataChunk`] → [`Vector`].
//!
//! Semantics:
//! - NULL propagates through arithmetic, comparisons, NOT, and scalar
//!   functions (SQL three-valued logic).
//! - `AND`/`OR` use Kleene logic; `WHERE` keeps only TRUE rows.
//! - Numeric widening (INTEGER → BIGINT → DOUBLE); `int / int` → DOUBLE.
//! - Integer division/modulo by zero is an error; float follows IEEE 754.

use super::aggregate::encode_row;
use super::{Cx, Memo, eval};
use crate::plan::compare_values;
use crate::sql::binder::{BoundExpr, ScalarFunc, common_numeric};
use crate::sql::ast::BinaryOp;
use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{DataChunk, Vector};
use std::collections::HashSet;

/// Evaluate `expr` over every row of `chunk`.
pub(crate) fn eval_expr(expr: &BoundExpr, chunk: &DataChunk, cx: &mut Cx) -> Result<Vector> {
    let n = chunk.num_rows();
    match expr {
        BoundExpr::Literal(v) => const_vector(v.clone(), n),
        BoundExpr::Column { index, .. } => Ok(chunk.column(*index).clone()),
        BoundExpr::AggRef { index, .. } => Ok(chunk.column(*index).clone()),
        BoundExpr::ScalarSubquery { query, .. } => eval_scalar_subquery(query, chunk, cx),
        BoundExpr::InSubquery { expr, query, negated } => {
            eval_in_subquery(expr, query, *negated, chunk, cx)
        }
        BoundExpr::Exists { query, negated } => eval_exists(query, *negated, chunk, cx),
        BoundExpr::Not(e) => {
            let v = eval_expr(e, chunk, cx)?;
            ensure_dtype(&v, DataType::Boolean, "NOT")?;
            let mut out = Vector::new_empty(DataType::Boolean);
            for i in 0..v.len() {
                if v.is_null(i) {
                    out.push_null()?;
                } else {
                    out.push_valid(Value::Boolean(!as_bool(&v, i)?))?;
                }
            }
            Ok(out)
        }
        BoundExpr::Negate { expr, dtype } => {
            let v = eval_expr(expr, chunk, cx)?;
            let mut out = Vector::new_empty(*dtype);
            for i in 0..v.len() {
                if v.is_null(i) {
                    out.push_null()?;
                } else {
                    out.push_valid(negate_value(v.get(i), *dtype)?)?;
                }
            }
            Ok(out)
        }
        BoundExpr::IsNull { expr, negated } => {
            let v = eval_expr(expr, chunk, cx)?;
            let mut out = Vector::new_empty(DataType::Boolean);
            for i in 0..v.len() {
                out.push_valid(Value::Boolean(v.is_null(i) != *negated))?;
            }
            Ok(out)
        }
        BoundExpr::Binary { op, left, right, dtype } => match op {
            BinaryOp::And | BinaryOp::Or => eval_logic(*op, left, right, chunk, cx),
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
    }
}

fn memo_key(query: &crate::plan::LogicalPlan) -> usize {
    (query as *const crate::plan::LogicalPlan) as usize
}

/// Scalar subquery: 0 rows → NULL, 1 row → broadcast, else error.
/// Memoised per plan node: uncorrelated ⇒ stable within a statement.
fn eval_scalar_subquery(
    query: &crate::plan::LogicalPlan,
    chunk: &DataChunk,
    cx: &mut Cx,
) -> Result<Vector> {
    let key = memo_key(query);
    if let Some(Memo::Scalar(v)) = cx.memo.get(&key) {
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
    cx.memo.insert(key, Memo::Scalar(value.clone()));
    const_vector(value, chunk.num_rows())
}

/// `lhs [NOT] IN (query)` with SQL three-valued logic: a match is TRUE;
/// otherwise NULL if the LHS is NULL or the set holds a NULL, else FALSE.
fn eval_in_subquery(
    lhs: &BoundExpr,
    query: &crate::plan::LogicalPlan,
    negated: bool,
    chunk: &DataChunk,
    cx: &mut Cx,
) -> Result<Vector> {
    let key = memo_key(query);
    let l = eval_expr(lhs, chunk, cx)?;
    // Hit path borrows the set (no clone); the borrow ends at return.
    if let Some(Memo::Set(set, has_null, common)) = cx.memo.get(&key) {
        return probe_in(&l, set, *has_null, *common, negated);
    }
    let out = eval(cx, query)?;
    let rhs_t = out.types.first().copied().unwrap_or(DataType::Integer);
    let common = common_numeric(l.dtype(), rhs_t).ok();
    let mut set = HashSet::new();
    let mut has_null = false;
    let mut buf = Vec::new();
    for c in &out.chunks {
        let col = match c.columns.first() {
            Some(col) => col,
            None => continue,
        };
        let col = match common {
            Some(t) => coerce_to(col, t)?,
            None => col.clone(),
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
    cx.memo.insert(key, Memo::Set(set, has_null, common));
    Ok(result)
}

fn probe_in(
    l: &Vector,
    set: &HashSet<Vec<u8>>,
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
    cx: &mut Cx,
) -> Result<Vector> {
    let key = memo_key(query);
    if let Some(Memo::Flag(b)) = cx.memo.get(&key) {
        let b = *b;
        return const_vector(Value::Boolean(b != negated), chunk.num_rows());
    }
    let out = eval(cx, query)?;
    let any = out.chunks.iter().any(|c| c.num_rows() > 0);
    cx.memo.insert(key, Memo::Flag(any));
    const_vector(Value::Boolean(any != negated), chunk.num_rows())
}

fn const_vector(value: Value, n: usize) -> Result<Vector> {    let dtype = match &value {
        Value::Null => DataType::Integer,
        v => v.data_type().expect("non-null"),
    };
    let mut out = Vector::with_capacity(dtype, n.min(crate::vector::CHUNK_SIZE));
    for _ in 0..n {
        out.push_value(value.clone())?;
    }
    Ok(out)
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
    cx: &mut Cx,
) -> Result<Vector> {
    let l = eval_expr(left, chunk, cx)?;
    let r = eval_expr(right, chunk, cx)?;
    ensure_dtype(&l, DataType::Boolean, "AND/OR")?;
    ensure_dtype(&r, DataType::Boolean, "AND/OR")?;
    let mut out = Vector::new_empty(DataType::Boolean);
    for i in 0..l.len() {
        let a = if l.is_null(i) { None } else { Some(as_bool(&l, i)?) };
        let b = if r.is_null(i) { None } else { Some(as_bool(&r, i)?) };
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
            Some(v) => out.push_valid(Value::Boolean(v))?,
            None => out.push_null()?,
        }
    }
    Ok(out)
}

// ---------- comparisons ----------

fn eval_comparison(
    op: BinaryOp,
    left: &BoundExpr,
    right: &BoundExpr,
    chunk: &DataChunk,
    cx: &mut Cx,
) -> Result<Vector> {
    let l = eval_expr(left, chunk, cx)?;
    let r = eval_expr(right, chunk, cx)?;
    let mut out = Vector::new_empty(DataType::Boolean);
    for i in 0..l.len() {
        if l.is_null(i) || r.is_null(i) {
            out.push_null()?;
            continue;
        }
        let ord = compare_values(&l.get(i), &r.get(i));
        let hit = match op {
            BinaryOp::Eq => ord == std::cmp::Ordering::Equal,
            BinaryOp::NotEq => ord != std::cmp::Ordering::Equal,
            BinaryOp::Lt => ord == std::cmp::Ordering::Less,
            BinaryOp::LtEq => ord != std::cmp::Ordering::Greater,
            BinaryOp::Gt => ord == std::cmp::Ordering::Greater,
            BinaryOp::GtEq => ord != std::cmp::Ordering::Less,
            _ => unreachable!(),
        };
        out.push_valid(Value::Boolean(hit))?;
    }
    Ok(out)
}

// ---------- arithmetic ----------

pub(crate) fn coerce_to(v: &Vector, target: DataType) -> Result<Vector> {
    if v.dtype() == target {
        return Ok(v.clone());
    }
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
    cx: &mut Cx,
) -> Result<Vector> {
    let l = eval_expr(left, chunk, cx)?;
    let r = eval_expr(right, chunk, cx)?;
    // Integer division by zero errors even though `/` widens to DOUBLE
    // (DuckDB rule); 0.0/0.0 with a float operand stays IEEE Inf/NaN.
    let int_div = op == BinaryOp::Divide && l.dtype() != DataType::Double && r.dtype() != DataType::Double;
    let l = coerce_to(&l, dtype)?;
    let r = coerce_to(&r, dtype)?;
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
    }
}

/// Keep only rows where `pred` (Boolean) is TRUE. NULL/missing → dropped.
pub fn select_true(pred: &Vector) -> Result<Vec<usize>> {
    ensure_dtype(pred, DataType::Boolean, "WHERE")?;
    let mut sel = Vec::new();
    for i in 0..pred.len() {
        if !pred.is_null(i) && as_bool(pred, i)? {
            sel.push(i);
        }
    }
    Ok(sel)
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
        let v = eval_expr(&e, &chunk2(), &mut test_cx(&db)).unwrap();
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
        let mut cx = test_cx(&db);
        let a = eval_expr(&and, &c, &mut cx).unwrap();
        assert_eq!((a.get(0), a.get(1), a.get(2)), (V::Boolean(false), V::Null, V::Boolean(true)));
        let o = eval_expr(&or, &c, &mut cx).unwrap();
        assert_eq!((o.get(0), o.get(1), o.get(2)), (V::Null, V::Boolean(true), V::Boolean(true)));
    }
}
