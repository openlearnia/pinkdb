//! Binder: resolve AST against the catalog → typed [`LogicalPlan`](crate::plan).
//!
//! Rules (v1):
//! - Bare identifiers resolve to columns (case-insensitive); `t.c` to table `t`'s `c`.
//! - `*` / `t.*` expand to all (qualified) columns.
//! - `BETWEEN` desugars to `>= AND <=` (or `NOT (...)` when negated).
//! - Aggregates allowed in SELECT / HAVING only (also ORDER-hidden is
//!   aggregate-free in v1 — use an alias or position for aggregates).
//!   Never in WHERE or GROUP BY. With aggregation, every non-aggregate
//!   SELECT/HAVING fragment must equal a GROUP BY key.
//! - Numeric widening: INTEGER → BIGINT → DOUBLE. `int / int` → DOUBLE.
//! - Comparisons need both-numeric, both-VARCHAR, or both-BOOLEAN (equality).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use super::ast::*;
use crate::attached::AtSpec;
use crate::plan::{AlterTablePlan, LogicalPlan};
use crate::storage::{
    DataTable, ForeignKeyConstraint, KeyConstraint, KeyKind, PinkDb, TableConstraints,
};
use crate::table_source::{TableFunctionArg, TableFunctionCall};
use crate::types::{
    DECIMAL_MAX_PRECISION, DataType, ExtractField, PinkDbError, Result, Value, extract_field,
};

// ---------- bound expressions ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFunc {
    pub fn from_name(name: &str) -> Option<AggFunc> {
        Some(match name.to_ascii_lowercase().as_str() {
            "count" => AggFunc::Count,
            "sum" => AggFunc::Sum,
            "avg" => AggFunc::Avg,
            "min" => AggFunc::Min,
            "max" => AggFunc::Max,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunc {
    Abs,
    Round,
    Upper,
    Lower,
    Length,
    Substr,
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    DateTrunc,
    /// `concat(a, b, ...)` — NULL arguments are skipped (DuckDB).
    Concat,
    Trim,
    LTrim,
    RTrim,
    Replace,
    /// `nullif(a, b)` — NULL when `a = b` (else `a`); only `a`'s NULL is
    /// propagating, `b`'s NULL is not.
    NullIf,
    /// `greatest(...)` / `least(...)` — NULL arguments are skipped.
    Greatest,
    Least,
    /// `extract(field FROM x)` / `date_part('field', x)`.
    Extract,
    Sqrt,
    Floor,
    Ceil,
    Sign,
    Power,
    Mod,
}

impl ScalarFunc {
    pub fn from_name(name: &str) -> Option<ScalarFunc> {
        Some(match name.to_ascii_lowercase().as_str() {
            "abs" => ScalarFunc::Abs,
            "round" => ScalarFunc::Round,
            "upper" => ScalarFunc::Upper,
            "lower" => ScalarFunc::Lower,
            "length" => ScalarFunc::Length,
            "substr" => ScalarFunc::Substr,
            "year" => ScalarFunc::Year,
            "month" => ScalarFunc::Month,
            "day" => ScalarFunc::Day,
            "hour" => ScalarFunc::Hour,
            "minute" => ScalarFunc::Minute,
            "second" => ScalarFunc::Second,
            "date_trunc" => ScalarFunc::DateTrunc,
            "concat" => ScalarFunc::Concat,
            "trim" => ScalarFunc::Trim,
            "ltrim" => ScalarFunc::LTrim,
            "rtrim" => ScalarFunc::RTrim,
            "replace" => ScalarFunc::Replace,
            "nullif" => ScalarFunc::NullIf,
            "greatest" => ScalarFunc::Greatest,
            "least" => ScalarFunc::Least,
            "extract" | "date_part" => ScalarFunc::Extract,
            "sqrt" => ScalarFunc::Sqrt,
            "floor" => ScalarFunc::Floor,
            "ceil" | "ceiling" => ScalarFunc::Ceil,
            "sign" => ScalarFunc::Sign,
            "power" | "pow" => ScalarFunc::Power,
            "mod" => ScalarFunc::Mod,
            _ => return None,
        })
    }

    /// False for the functions that handle NULLs themselves (they are not
    /// `any-NULL-in → NULL-out`).
    pub(crate) fn is_strict(self) -> bool {
        !matches!(
            self,
            ScalarFunc::Concat | ScalarFunc::NullIf | ScalarFunc::Greatest | ScalarFunc::Least
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundAgg {
    pub func: AggFunc,
    /// COUNT(*) has no arg.
    pub arg: Option<BoundExpr>,
    pub dtype: DataType,
    /// `COUNT(DISTINCT x)` (the only DISTINCT aggregate supported).
    pub distinct: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    /// `lag(x [, offset [, default]])` — reads the row `offset` before the
    /// current one; the window frame is ignored.
    Lag,
    /// `lead(x [, offset [, default]])` — like `Lag`, in the other direction.
    Lead,
    /// `ntile(n)` — `n` (a positive constant) equally-sized buckets, the
    /// larger ones first.
    NTile,
    /// `first_value(x)` — `x` of the frame's first row (frame-respecting).
    FirstValue,
    /// `last_value(x)` — `x` of the frame's last row (frame-respecting).
    LastValue,
    Agg(AggFunc),
}

impl WindowFunc {
    pub(crate) fn ranking(name: &str) -> Option<WindowFunc> {
        Some(match name.to_ascii_lowercase().as_str() {
            "row_number" => WindowFunc::RowNumber,
            "rank" => WindowFunc::Rank,
            "dense_rank" => WindowFunc::DenseRank,
            _ => return None,
        })
    }

    /// Every built-in window function name (used for registry-conflict
    /// checks and window binding).
    pub(crate) fn from_name(name: &str) -> Option<WindowFunc> {
        if let Some(r) = Self::ranking(name) {
            return Some(r);
        }
        Some(match name.to_ascii_lowercase().as_str() {
            "lag" => WindowFunc::Lag,
            "lead" => WindowFunc::Lead,
            "ntile" => WindowFunc::NTile,
            "first_value" => WindowFunc::FirstValue,
            "last_value" => WindowFunc::LastValue,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(usize),
    CurrentRow,
    Following(usize),
    UnboundedFollowing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowKind {
    /// Ranking functions plus lag/lead/ntile/first_value/last_value.
    Simple(WindowFunc),
    Agg(AggFunc),
}

fn map_frame_bound(b: crate::sql::ast::FrameBound) -> Result<FrameBound> {
    Ok(match b {
        crate::sql::ast::FrameBound::UnboundedPreceding => FrameBound::UnboundedPreceding,
        crate::sql::ast::FrameBound::Preceding(n) => FrameBound::Preceding(n),
        crate::sql::ast::FrameBound::CurrentRow => FrameBound::CurrentRow,
        crate::sql::ast::FrameBound::Following(n) => FrameBound::Following(n),
        crate::sql::ast::FrameBound::UnboundedFollowing => FrameBound::UnboundedFollowing,
    })
}

/// `ROWS`/`RANGE` — same shape in the AST and the bound frame.
fn map_frame_mode(m: FrameMode) -> FrameMode {
    m
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundFrame {
    pub mode: FrameMode,
    pub start: FrameBound,
    pub end: FrameBound,
}

impl BoundFrame {
    /// Default frame. Without ORDER BY the whole partition; with ORDER BY
    /// DuckDB's `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — peers
    /// (equal ORDER BY keys) share the frame, so they get the same value.
    fn default_for(ordered: bool) -> Self {
        if ordered {
            Self {
                mode: FrameMode::Range,
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::CurrentRow,
            }
        } else {
            Self {
                mode: FrameMode::Rows,
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::UnboundedFollowing,
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundWindow {
    pub func: WindowFunc,
    /// COUNT(*) has no arg; lag/lead/ntile carry theirs here too.
    pub arg: Option<BoundExpr>,
    /// `lag`/`lead`: rows to move back/forward (may be negative — DuckDB
    /// treats a negative lag like a lead). Unused by other windows.
    pub offset: i64,
    /// `lag`/`lead`: value used when the target row is off the partition
    /// edge (coerced to the result type at bind time).
    pub default: Option<BoundExpr>,
    pub partition: Vec<BoundExpr>,
    pub order: Vec<(BoundExpr, SortSpec)>,
    pub frame: BoundFrame,
    pub dtype: DataType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpr {
    Literal(Value),
    /// Column of the current stage input (scan, agg output, or project output).
    Column {
        index: usize,
        dtype: DataType,
    },
    Binary {
        op: BinaryOp,
        left: Box<BoundExpr>,
        right: Box<BoundExpr>,
        dtype: DataType,
    },
    Not(Box<BoundExpr>),
    Negate {
        expr: Box<BoundExpr>,
        dtype: DataType,
    },
    IsNull {
        expr: Box<BoundExpr>,
        negated: bool,
    },
    Scalar {
        func: ScalarFunc,
        args: Vec<BoundExpr>,
        dtype: DataType,
    },
    /// Aggregate output column. Index is relative to the agg list until
    /// [`resolve_agg_refs`] rebases it past the group keys.
    AggRef {
        index: usize,
        dtype: DataType,
    },
    /// Window output column. Index is relative to the window list until
    /// rebased past the window input columns.
    WindowRef {
        index: usize,
        dtype: DataType,
    },
    /// `CAST(x AS type)`.
    Cast {
        expr: Box<BoundExpr>,
        dtype: DataType,
    },
    /// `COALESCE(a, b, ...)` — all args share `dtype` (narrower sides are
    /// wrapped in [`BoundExpr::Cast`] at bind time).
    Coalesce {
        args: Vec<BoundExpr>,
        dtype: DataType,
    },
    /// `CASE WHEN c THEN v ... [ELSE e]` — results share `dtype`
    /// (narrower sides wrapped in [`BoundExpr::Cast`]); missing ELSE is NULL.
    Case {
        branches: Vec<(BoundExpr, BoundExpr)>,
        else_: Option<Box<BoundExpr>>,
        dtype: DataType,
    },
    /// Uncorrelated scalar subquery (1 col by construction; rows at runtime).
    ScalarSubquery {
        query: Box<LogicalPlan>,
        dtype: DataType,
    },
    /// `expr [NOT] IN (query)` — `expr` may reference the outer scope.
    InSubquery {
        expr: Box<BoundExpr>,
        query: Box<LogicalPlan>,
        negated: bool,
    },
    /// `[NOT] EXISTS (query)` — scope-independent.
    Exists {
        query: Box<LogicalPlan>,
        negated: bool,
    },
    /// Call into a scalar function registered by a loaded extension — a
    /// native `dlopen` callback or a linked registry function
    /// (`PinkDb::load_extension` / `PinkDb::load_linked_extension`). `name` is
    /// the canonical lowercased registry key; args and result are typed as
    /// registered (ABI v1 inserts no implicit casts, except that a numeric
    /// argument widens into a `DOUBLE` parameter — see
    /// [`Binder::bind_dynamic_function`]). Never
    /// constant-folded (an extension callback is opaque: it may be impure and
    /// may fail).
    ///
    /// `Box<str>` + `Box<[BoundExpr]>` (not `String` + `Vec`) keep this
    /// variant no larger than the biggest pre-existing one, so
    /// `size_of::<BoundExpr>()` — and with it every expression tree and the
    /// hot match dispatch on the static path — is unchanged.
    DynamicScalar {
        name: Box<str>,
        args: Box<[BoundExpr]>,
        dtype: DataType,
    },
}

impl BoundExpr {
    pub fn dtype(&self) -> DataType {
        match self {
            BoundExpr::Literal(Value::Null) => DataType::Integer, // `SELECT NULL` → INTEGER
            BoundExpr::Literal(v) => v.data_type().expect("non-null literal"),
            BoundExpr::Column { dtype, .. } => *dtype,
            BoundExpr::Binary { dtype, .. } => *dtype,
            BoundExpr::Not(_) => DataType::Boolean,
            BoundExpr::Negate { dtype, .. } => *dtype,
            BoundExpr::IsNull { .. } => DataType::Boolean,
            BoundExpr::Scalar { dtype, .. } => *dtype,
            BoundExpr::AggRef { dtype, .. } => *dtype,
            BoundExpr::WindowRef { dtype, .. } => *dtype,
            BoundExpr::Cast { dtype, .. } => *dtype,
            BoundExpr::Coalesce { dtype, .. } => *dtype,
            BoundExpr::Case { dtype, .. } => *dtype,
            BoundExpr::ScalarSubquery { dtype, .. } => *dtype,
            BoundExpr::DynamicScalar { dtype, .. } => *dtype,
            BoundExpr::InSubquery { .. } | BoundExpr::Exists { .. } => DataType::Boolean,
        }
    }

    /// True if the tree contains no [`BoundExpr::AggRef`].
    /// Subquery bodies live in their own scope and never count here.
    pub fn is_aggregate_free(&self) -> bool {
        match self {
            BoundExpr::AggRef { .. } | BoundExpr::WindowRef { .. } => false,
            BoundExpr::Cast { expr, .. } => expr.is_aggregate_free(),
            BoundExpr::Coalesce { args, .. } => args.iter().all(|a| a.is_aggregate_free()),
            BoundExpr::Case {
                branches, else_, ..
            } => {
                branches
                    .iter()
                    .all(|(c, v)| c.is_aggregate_free() && v.is_aggregate_free())
                    && else_
                        .as_ref()
                        .map(|e| e.is_aggregate_free())
                        .unwrap_or(true)
            }
            BoundExpr::Literal(_)
            | BoundExpr::Column { .. }
            | BoundExpr::ScalarSubquery { .. }
            | BoundExpr::Exists { .. } => true,
            BoundExpr::InSubquery { expr, .. } => expr.is_aggregate_free(),
            BoundExpr::Not(e)
            | BoundExpr::Negate { expr: e, .. }
            | BoundExpr::IsNull { expr: e, .. } => e.is_aggregate_free(),
            BoundExpr::Binary { left, right, .. } => {
                left.is_aggregate_free() && right.is_aggregate_free()
            }
            BoundExpr::Scalar { args, .. } => args.iter().all(|a| a.is_aggregate_free()),
            BoundExpr::DynamicScalar { args, .. } => args.iter().all(|a| a.is_aggregate_free()),
        }
    }

    /// True if the tree references any [`BoundExpr::Column`].
    /// Subquery bodies bind in fresh scopes, so only [`BoundExpr::InSubquery`]'s
    /// outer expression can reference outer columns.
    pub fn has_column_ref(&self) -> bool {
        match self {
            BoundExpr::Column { .. } => true,
            BoundExpr::Literal(_)
            | BoundExpr::AggRef { .. }
            | BoundExpr::WindowRef { .. }
            | BoundExpr::ScalarSubquery { .. }
            | BoundExpr::Exists { .. } => false,
            BoundExpr::InSubquery { expr, .. } => expr.has_column_ref(),
            BoundExpr::Cast { expr, .. } => expr.has_column_ref(),
            BoundExpr::Coalesce { args, .. } => args.iter().any(|a| a.has_column_ref()),
            BoundExpr::Case {
                branches, else_, ..
            } => {
                branches
                    .iter()
                    .any(|(c, v)| c.has_column_ref() || v.has_column_ref())
                    || else_.as_ref().map(|e| e.has_column_ref()).unwrap_or(false)
            }
            BoundExpr::Not(e)
            | BoundExpr::Negate { expr: e, .. }
            | BoundExpr::IsNull { expr: e, .. } => e.has_column_ref(),
            BoundExpr::Binary { left, right, .. } => {
                left.has_column_ref() || right.has_column_ref()
            }
            BoundExpr::Scalar { args, .. } => args.iter().any(|a| a.has_column_ref()),
            BoundExpr::DynamicScalar { args, .. } => args.iter().any(|a| a.has_column_ref()),
        }
    }

    /// True if the tree holds a subquery (never constant-folded: needs runtime).
    pub fn has_subquery(&self) -> bool {
        match self {
            BoundExpr::ScalarSubquery { .. }
            | BoundExpr::InSubquery { .. }
            | BoundExpr::Exists { .. } => true,
            BoundExpr::Literal(_)
            | BoundExpr::Column { .. }
            | BoundExpr::AggRef { .. }
            | BoundExpr::WindowRef { .. } => false,
            BoundExpr::Not(e)
            | BoundExpr::Negate { expr: e, .. }
            | BoundExpr::IsNull { expr: e, .. }
            | BoundExpr::Cast { expr: e, .. } => e.has_subquery(),
            BoundExpr::Binary { left, right, .. } => left.has_subquery() || right.has_subquery(),
            BoundExpr::Scalar { args, .. } => args.iter().any(|a| a.has_subquery()),
            BoundExpr::DynamicScalar { args, .. } => args.iter().any(|a| a.has_subquery()),
            BoundExpr::Coalesce { args, .. } => args.iter().any(|a| a.has_subquery()),
            BoundExpr::Case {
                branches, else_, ..
            } => {
                branches
                    .iter()
                    .any(|(c, v)| c.has_subquery() || v.has_subquery())
                    || else_.as_ref().map(|e| e.has_subquery()).unwrap_or(false)
            }
        }
    }

    /// True if the tree contains a dynamic scalar call (anywhere in this
    /// scope, including inside enclosing operators/`COALESCE`/`CASE`).
    ///
    /// A callback is opaque: it may be impure, non-deterministic, or fail, so
    /// an expression tree containing one is never constant-folded and its
    /// predicate is never pushed through a row-count-changing operator (a
    /// join or a union would change how many times it runs). Subquery bodies
    /// bind in their own scope (like [`Self::has_column_ref`]); they are not
    /// descended into here — an `InSubquery`'s outer expression is.
    pub fn contains_dynamic_scalar(&self) -> bool {
        match self {
            BoundExpr::DynamicScalar { .. } => true,
            BoundExpr::Literal(_)
            | BoundExpr::Column { .. }
            | BoundExpr::AggRef { .. }
            | BoundExpr::WindowRef { .. }
            | BoundExpr::ScalarSubquery { .. }
            | BoundExpr::Exists { .. } => false,
            BoundExpr::InSubquery { expr, .. } => expr.contains_dynamic_scalar(),
            BoundExpr::Not(e)
            | BoundExpr::Negate { expr: e, .. }
            | BoundExpr::IsNull { expr: e, .. }
            | BoundExpr::Cast { expr: e, .. } => e.contains_dynamic_scalar(),
            BoundExpr::Binary { left, right, .. } => {
                left.contains_dynamic_scalar() || right.contains_dynamic_scalar()
            }
            BoundExpr::Scalar { args, .. } => args.iter().any(|a| a.contains_dynamic_scalar()),
            BoundExpr::Coalesce { args, .. } => args.iter().any(|a| a.contains_dynamic_scalar()),
            BoundExpr::Case {
                branches, else_, ..
            } => {
                branches
                    .iter()
                    .any(|(c, v)| c.contains_dynamic_scalar() || v.contains_dynamic_scalar())
                    || else_
                        .as_ref()
                        .map(|e| e.contains_dynamic_scalar())
                        .unwrap_or(false)
            }
        }
    }
}

// ---------- type rules ----------

fn is_numeric(t: DataType) -> bool {
    t.is_numeric()
}

/// The type a bare `?` operand takes from the other side of a binary
/// operator, when the operator itself fixes it. Most comparisons/arithmetic
/// take the other operand's type (handled by the caller); the operators with
/// a fixed parameter type decide it here.
fn fixed_operand_dtype(op: BinaryOp, other: DataType) -> Option<DataType> {
    Some(match op {
        // LIKE compares strings only; `||` formats either side to VARCHAR.
        BinaryOp::Like | BinaryOp::NotLike | BinaryOp::Concat => DataType::Varchar,
        // AND/OR need BOOLEAN on both sides.
        BinaryOp::And | BinaryOp::Or => DataType::Boolean,
        // `date ± ?` is day arithmetic: the parameter is the INTEGER count.
        BinaryOp::Add | BinaryOp::Subtract if other == DataType::Date => DataType::Integer,
        _ => return None,
    })
}

/// DECIMAL `(precision, scale)` of a value operand: DECIMAL keeps its own,
/// INTEGER/BIGINT widen like DuckDB (`INTEGER → DECIMAL(10,0)`,
/// `BIGINT → DECIMAL(19,0)`); anything else (DOUBLE) is not decimal-exact.
fn decimal_operand_width(t: DataType) -> Option<(u8, u8)> {
    match t {
        DataType::Decimal { precision, scale } => Some((precision, scale)),
        DataType::Integer => Some((10, 0)),
        DataType::Bigint => Some((19, 0)),
        _ => None,
    }
}

fn clamp_precision(precision: i32, scale: u8) -> u8 {
    precision.clamp(scale as i32, DECIMAL_MAX_PRECISION as i32) as u8
}

/// Common DECIMAL type for comparisons/UNION/CASE (DuckDB rule):
/// `scale = max(s1, s2)`, `precision = max(p1-s1, p2-s2) + scale`, capped
/// at 38.
fn decimal_common(a: (u8, u8), b: (u8, u8)) -> DataType {
    let scale = a.1.max(b.1);
    let int_digits = (a.0 - a.1).max(b.0 - b.1) as i32;
    DataType::Decimal {
        precision: clamp_precision(int_digits + scale as i32, scale),
        scale,
    }
}

/// INTEGER → BIGINT → DOUBLE, with DECIMAL as a first-class numeric type.
/// DECIMAL ⊕ DOUBLE is DOUBLE; DECIMAL ⊕ INTEGER widens the integer to its
/// decimal width (DuckDB rule).
pub(crate) fn common_numeric(a: DataType, b: DataType) -> Result<DataType> {
    if !is_numeric(a) || !is_numeric(b) {
        return Err(PinkDbError::Type(format!(
            "non-numeric operands for arithmetic: {a} and {b}"
        )));
    }
    Ok(match (a, b) {
        (DataType::Double, _) | (_, DataType::Double) => DataType::Double,
        (DataType::Decimal { .. }, _) | (_, DataType::Decimal { .. }) => {
            let wa = decimal_operand_width(a).expect("numeric without a decimal width");
            let wb = decimal_operand_width(b).expect("numeric without a decimal width");
            decimal_common(wa, wb)
        }
        (DataType::Bigint, _) | (_, DataType::Bigint) => DataType::Bigint,
        _ => DataType::Integer,
    })
}

fn arithmetic_dtype(op: BinaryOp, l: DataType, r: DataType) -> Result<DataType> {
    use BinaryOp::{Add, Divide, Multiply, Subtract};
    let temporal =
        |t: DataType| matches!(t, DataType::Date | DataType::Timestamp | DataType::Interval);
    // Dates: `date ± integer` is day arithmetic; `date - date` counts days
    // (BIGINT). INTERVAL adds month/day/time arithmetic (DuckDB signatures):
    // interval ± interval → INTERVAL, date/timestamp ± interval → TIMESTAMP,
    // timestamp - timestamp → INTERVAL, interval *|/ number → INTERVAL.
    match (op, l, r) {
        (Add, DataType::Date, DataType::Integer)
        | (Add, DataType::Integer, DataType::Date)
        | (Subtract, DataType::Date, DataType::Integer) => return Ok(DataType::Date),
        (Subtract, DataType::Date, DataType::Date) => return Ok(DataType::Bigint),
        (Add | Subtract, DataType::Interval, DataType::Interval) => return Ok(DataType::Interval),
        (Add, DataType::Date | DataType::Timestamp, DataType::Interval)
        | (Add, DataType::Interval, DataType::Date | DataType::Timestamp) => {
            return Ok(DataType::Timestamp);
        }
        (Subtract, DataType::Date | DataType::Timestamp, DataType::Interval) => {
            return Ok(DataType::Timestamp);
        }
        (Subtract, DataType::Timestamp, DataType::Timestamp)
        | (Subtract, DataType::Timestamp, DataType::Date)
        | (Subtract, DataType::Date, DataType::Timestamp) => return Ok(DataType::Interval),
        (Multiply | Divide, DataType::Interval, _) | (Multiply, _, DataType::Interval)
            if is_numeric(l) || is_numeric(r) =>
        {
            return Ok(DataType::Interval);
        }
        _ => {}
    }
    if temporal(l) || temporal(r) {
        return Err(PinkDbError::Type(format!(
            "operator {op:?} does not support {l} and {r} in v1"
        )));
    }
    match op {
        BinaryOp::Divide => {
            // DuckDB rule: int / int → DOUBLE; DECIMAL / DECIMAL → DOUBLE
            // (decimal division widens both sides to DOUBLE).
            if !is_numeric(l) || !is_numeric(r) {
                return Err(PinkDbError::Type(format!(
                    "non-numeric operands for `/`: {l} and {r}"
                )));
            }
            Ok(DataType::Double)
        }
        BinaryOp::Modulo => {
            if !is_numeric(l) || !is_numeric(r) {
                return Err(PinkDbError::Type(format!(
                    "non-numeric operands for `%`: {l} and {r}"
                )));
            }
            if l == DataType::Double || r == DataType::Double {
                return Err(PinkDbError::Type(
                    "`%` needs integer operands in v1".to_string(),
                ));
            }
            if matches!(l, DataType::Decimal { .. }) || matches!(r, DataType::Decimal { .. }) {
                let wa = decimal_operand_width(l).expect("numeric without a decimal width");
                let wb = decimal_operand_width(r).expect("numeric without a decimal width");
                // DuckDB: min integer digits + the wider scale.
                let scale = wa.1.max(wb.1);
                let int_digits = (wa.0 - wa.1).min(wb.0 - wb.1) as i32;
                return Ok(DataType::Decimal {
                    precision: clamp_precision(int_digits + scale as i32, scale),
                    scale,
                });
            }
            common_numeric(l, r)
        }
        BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply
            if matches!(l, DataType::Decimal { .. }) || matches!(r, DataType::Decimal { .. }) =>
        {
            if l == DataType::Double || r == DataType::Double {
                return Ok(DataType::Double);
            }
            let wa = decimal_operand_width(l).expect("numeric without a decimal width");
            let wb = decimal_operand_width(r).expect("numeric without a decimal width");
            let (p, s) = match op {
                // DuckDB: s = max(s1,s2), p = max(p1-s1, p2-s2) + s + 1.
                Add | Subtract => {
                    let scale = wa.1.max(wb.1);
                    let int_digits = (wa.0 - wa.1).max(wb.0 - wb.1) as i32;
                    (clamp_precision(int_digits + scale as i32 + 1, scale), scale)
                }
                // DuckDB: s = s1+s2 (an error past 38), p = p1+p2 capped.
                _ => {
                    let scale = wa.1 as u16 + wb.1 as u16;
                    if scale > DECIMAL_MAX_PRECISION as u16 {
                        return Err(PinkDbError::Type(format!(
                            "Needed scale {scale} to accurately represent the multiplication \
                             result, but this is out of range of the DECIMAL type. Max scale \
                             is {DECIMAL_MAX_PRECISION}."
                        )));
                    }
                    let scale = scale as u8;
                    let p = (wa.0 as u16 + wb.0 as u16).min(DECIMAL_MAX_PRECISION as u16) as u8;
                    (p.max(scale), scale)
                }
            };
            Ok(DataType::Decimal {
                precision: p,
                scale: s,
            })
        }
        _ => common_numeric(l, r),
    }
}

/// Fold one result arm into the common type, skipping NULL literals (a bare
/// NULL fits any result type instead of forcing INTEGER).
fn fold_result_common(current: Option<DataType>, expr: &BoundExpr) -> Result<Option<DataType>> {
    if matches!(expr, BoundExpr::Literal(Value::Null)) {
        return Ok(current);
    }
    Ok(Some(match current {
        None => expr.dtype(),
        Some(t) => coalesce_common(t, expr.dtype())?,
    }))
}

/// Common type for COALESCE/UNION arms: numeric tower, else identical.
fn coalesce_common(a: DataType, b: DataType) -> Result<DataType> {
    if a == b {
        return Ok(a);
    }
    common_numeric(a, b)
        .map_err(|_| PinkDbError::Type(format!("incompatible types {a} and {b} (use CAST)")))
}

/// Whether `CAST(from AS to)` is allowed in v1.
fn check_cast(from: DataType, to: DataType) -> Result<()> {
    if from == to {
        return Ok(());
    }
    // Numeric tower (both directions; float→int truncates, DECIMAL rounds
    // and range-checks).
    if from.is_numeric() && to.is_numeric() {
        return Ok(());
    }
    // Everything formats to VARCHAR; VARCHAR parses to everything
    // (including INTERVAL).
    if from == DataType::Varchar || to == DataType::Varchar {
        return Ok(());
    }
    // Boolean ↔ numeric.
    if (from == DataType::Boolean && to.is_numeric())
        || (from.is_numeric() && to == DataType::Boolean)
    {
        return Ok(());
    }
    // Date ↔ timestamp (midnight / truncate).
    if (from == DataType::Date && to == DataType::Timestamp)
        || (from == DataType::Timestamp && to == DataType::Date)
    {
        return Ok(());
    }
    Err(PinkDbError::Type(format!(
        "cannot CAST {from} AS {to} in v1"
    )))
}

fn comparison_ok(op: BinaryOp, l: DataType, r: DataType) -> Result<()> {
    if (is_numeric(l) && is_numeric(r))
        || (l == DataType::Varchar && r == DataType::Varchar)
        || (l == DataType::Date && r == DataType::Date)
        || (l == DataType::Timestamp && r == DataType::Timestamp)
        || (l == DataType::Interval && r == DataType::Interval)
    {
        return match op {
            // LIKE only compares strings.
            BinaryOp::Like | BinaryOp::NotLike
                if l != DataType::Varchar || r != DataType::Varchar =>
            {
                Err(PinkDbError::Type(
                    "LIKE needs VARCHAR on both sides".to_string(),
                ))
            }
            _ => Ok(()),
        };
    }
    if l == DataType::Boolean && r == DataType::Boolean {
        return match op {
            BinaryOp::Eq | BinaryOp::NotEq => Ok(()),
            _ => Err(PinkDbError::Type(
                "only `=` / `<>` compare BOOLEANs in v1".to_string(),
            )),
        };
    }
    Err(PinkDbError::Type(format!("cannot compare {l} with {r}")))
}

// ---------- parameter slots ----------

/// Shared per-bind state for a `?`-parameterized statement: one slot per
/// placeholder, in textual order.
///
/// Each slot starts with the value the caller bound and no type. A slot's
/// type is *inferred* while binding — a comparison takes the other operand's
/// type, an `INSERT`/`UPDATE` target takes the column's type, `CAST(? AS t)`
/// pins `t`, and a slot that is never pinned falls back to its value's own
/// type. The value is converted to the slot's type when the slot binds
/// (DuckDB's rule: the context wins, and an impossible conversion is a clear
/// bind-time error).
///
/// Cloning shares the state (subquery scopes bind through clones), and the
/// engine reads [`ParamSlots::resolved_values`] back after a successful bind
/// to substitute the converted values into the statement text.
#[derive(Clone)]
pub(crate) struct ParamSlots {
    inner: Rc<RefCell<ParamState>>,
    /// A procedure call's variables (parameters and `DECLARE`s), outermost
    /// first: a bare, unqualified name that matches one binds to its *value*
    /// as a literal — variables shadow columns (wave 44). Values are never
    /// spliced into SQL text, so a call is injection-safe by construction.
    variables: Vec<(String, Value)>,
    /// True while binding a statement that will be logged as part of a
    /// procedure call on a durable store: a read of a session-local view
    /// cannot replay, so it is refused here (see
    /// [`crate::procedure`]).
    logged_procedure: bool,
}

#[derive(Default)]
struct ParamState {
    /// The values passed to the bind call, one per slot.
    values: Vec<Value>,
    /// The slot's inferred type (pinned by the first context that needs one).
    types: Vec<Option<DataType>>,
    /// The value converted to the slot type, filled when the slot binds.
    resolved: Vec<Option<Value>>,
}

impl ParamSlots {
    /// One slot per bound value; every type starts uninferred.
    pub(crate) fn new(values: Vec<Value>) -> Self {
        let n = values.len();
        ParamSlots {
            inner: Rc::new(RefCell::new(ParamState {
                values,
                types: vec![None; n],
                resolved: vec![None; n],
            })),
            variables: Vec::new(),
            logged_procedure: false,
        }
    }

    /// Make a procedure call's variables visible to this bind (wave 44).
    pub(crate) fn with_variables(mut self, variables: Vec<(String, Value)>) -> Self {
        self.variables = variables;
        self
    }

    /// Mark this bind as a statement of a logged procedure call: a
    /// session-local view read is refused instead of silently binding.
    pub(crate) fn with_logged_procedure(mut self, logged: bool) -> Self {
        self.logged_procedure = logged;
        self
    }

    /// The value of the procedure variable `name` (innermost scope wins: the
    /// list is ordered outermost-first, so the last match is the innermost).
    pub(crate) fn variable(&self, name: &str) -> Option<Value> {
        self.variables
            .iter()
            .rev()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    }

    /// True when a view read would break the logged call's replay.
    pub(crate) fn rejects_view_reads(&self) -> bool {
        self.logged_procedure
    }

    /// The value bound to slot `i` (the raw API value), if any.
    pub(crate) fn value_type(&self, i: usize) -> Option<DataType> {
        self.inner
            .borrow()
            .values
            .get(i)
            .and_then(|v| v.data_type())
    }

    /// The type inferred for slot `i`, if any yet.
    pub(crate) fn slot_type(&self, i: usize) -> Option<DataType> {
        self.inner.borrow().types.get(i).copied().flatten()
    }

    /// The converted value for slot `i`, once the slot has bound.
    pub(crate) fn resolved_value(&self, i: usize) -> Option<Value> {
        self.inner.borrow().resolved.get(i).cloned().flatten()
    }

    /// Pin slot `i` to `dtype`. The first pin wins; a later use that needs a
    /// different type is a clear error (DuckDB's "parameter cannot have two
    /// types" case, e.g. `? BETWEEN 1 AND 2.5`).
    fn pin(&self, i: usize, dtype: DataType) -> Result<()> {
        let mut state = self.inner.borrow_mut();
        match state.types.get(i).copied().flatten() {
            Some(t) if t == dtype => Ok(()),
            Some(t) => Err(PinkDbError::Parameter(format!(
                "parameter {} has type {t} from an earlier use, but this use needs {dtype}",
                i + 1
            ))),
            None => {
                if let Some(slot) = state.types.get_mut(i) {
                    *slot = Some(dtype);
                    Ok(())
                } else {
                    Err(PinkDbError::Parameter(format!(
                        "internal error: no parameter slot {i}"
                    )))
                }
            }
        }
    }

    /// Bind slot `i`: infer the type (pinned, else the value's own), convert
    /// the value to it, and return the value as a bound literal.
    fn bind(&self, i: usize) -> Result<BoundExpr> {
        let mut state = self.inner.borrow_mut();
        let value = match state.values.get(i) {
            Some(v) => v.clone(),
            None => {
                return Err(PinkDbError::Parameter(format!(
                    "internal error: no parameter slot {i}"
                )));
            }
        };
        let dtype = match state.types.get(i).copied().flatten() {
            Some(t) => t,
            None => match value.data_type() {
                Some(t) => t,
                // A NULL value with no context has no type: DuckDB defaults
                // it to INTEGER; this engine asks for an explicit type
                // instead, so a mistyped statement fails at bind, not later.
                None => {
                    return Err(PinkDbError::Parameter(format!(
                        "could not determine data type of parameter {}: bind a non-NULL \
                         value or add an explicit CAST(? AS <type>)",
                        i + 1
                    )));
                }
            },
        };
        // NULL keeps its typeless form (`cast_value` assumes non-NULL input).
        let converted = if value.is_null() {
            Value::Null
        } else {
            crate::exec::expr::cast_value(value.clone(), dtype).map_err(|e| {
                PinkDbError::Parameter(format!("cannot bind parameter {} as {dtype}: {e}", i + 1))
            })?
        };
        if let Some(slot) = state.types.get_mut(i) {
            *slot = Some(dtype);
        }
        if let Some(slot) = state.resolved.get_mut(i) {
            *slot = Some(converted.clone());
        }
        Ok(BoundExpr::Literal(converted))
    }
}

// ---------- binder ----------

/// One queryable relation in scope.
#[derive(Clone)]
struct ScopeTable {
    /// Effective name (alias if present, else table name), lowercased.
    name: String,
    columns: Vec<(String, DataType)>,
}

struct Binder<'a> {
    db: &'a PinkDb,
    /// Session database for unqualified names (lowercased when used).
    default_db: String,
    tables: Vec<ScopeTable>,
    /// Select alias (lowercased) → AST, for cross-clause references.
    aliases: HashMap<String, Expr>,
    /// Aliases currently being resolved (cycle guard: `SELECT x AS x`).
    resolving: Vec<String>,
    /// Whether bare columns consult aliases (false for WHERE).
    allow_alias: bool,
    /// Whether window functions may appear (false for WHERE/GROUP BY/HAVING).
    allow_win: bool,
    /// True while binding a window's own arguments (rejects nesting).
    in_window: bool,
    /// Windows collected from the SELECT list (deduped, like aggregates).
    windows: Vec<BoundWindow>,
    /// Visible CTEs: (name, output schema). CTEs shadow catalog tables.
    ctes: Vec<(String, Vec<(String, DataType)>)>,
    /// Session-local views referenced while binding this statement. Shared
    /// with child scopes: a mutating statement that depends on one is
    /// refused on a durable database (the logged SQL could never replay).
    views_used: Rc<RefCell<Vec<String>>>,
    /// Dynamic-function names (lowercased registry keys) referenced while
    /// binding this statement, in first-use order. Shared with child scopes,
    /// so subquery bodies inside `ScalarSubquery`/`InSubquery`/`Exists` (and
    /// derived tables/views/CTE bodies) are covered. The WAL layer maps them
    /// back to their extension paths so a logged record can be prefixed with
    /// the `LOAD EXTENSION` statements its replay needs.
    dynamic_functions_used: Rc<RefCell<Vec<String>>>,
    /// `?` slots when binding a parameterized statement (shared with child
    /// scopes); `None` binds a plain statement, where a `?` is an error.
    params: Option<ParamSlots>,
}

impl<'a> Binder<'a> {
    fn new(db: &'a PinkDb, default_db: &str) -> Self {
        Self {
            db,
            default_db: default_db.to_string(),
            tables: Vec::new(),
            aliases: HashMap::new(),
            resolving: Vec::new(),
            allow_alias: true,
            allow_win: true,
            in_window: false,
            windows: Vec::new(),
            ctes: Vec::new(),
            views_used: Rc::new(RefCell::new(Vec::new())),
            dynamic_functions_used: Rc::new(RefCell::new(Vec::new())),
            params: None,
        }
    }

    /// This binder binds a parameterized statement: `?` slots resolve
    /// against `params` (see [`ParamSlots`]).
    fn with_params(mut self, params: Option<ParamSlots>) -> Self {
        self.params = params;
        self
    }

    /// A child scope for a subquery: no tables/aliases leak in, CTEs do.
    /// (Subqueries are uncorrelated in v1 — outer columns are invisible.)
    fn child(&self) -> Self {
        Self {
            db: self.db,
            default_db: self.default_db.clone(),
            tables: Vec::new(),
            aliases: HashMap::new(),
            resolving: Vec::new(),
            allow_alias: true,
            allow_win: true,
            in_window: false,
            windows: Vec::new(),
            ctes: self.ctes.clone(),
            views_used: Rc::clone(&self.views_used),
            dynamic_functions_used: Rc::clone(&self.dynamic_functions_used),
            params: self.params.clone(),
        }
    }

    /// Bind one `?` as a literal of the slot's inferred type; a plain
    /// (non-prepared) statement has no slots, so `?` is an error there.
    fn bind_parameter(&self, index: usize) -> Result<BoundExpr> {
        match &self.params {
            Some(slots) => slots.bind(index),
            None => Err(PinkDbError::Parameter(format!(
                "parameter {} (`?`) is only allowed in a prepared statement",
                index + 1
            ))),
        }
    }

    /// The slot of a top-level `?` operand whose type is not pinned yet, if
    /// any: the operand a parent expression can infer a type for.
    fn bare_unpinned_parameter(&self, expr: &Expr) -> Option<usize> {
        let Expr::Parameter(index) = expr else {
            return None;
        };
        let slots = self.params.as_ref()?;
        if slots.slot_type(*index).is_none() {
            Some(*index)
        } else {
            None
        }
    }

    /// Pin a slot from context (no-op when `expr` is not a bare parameter).
    fn pin_parameter(&self, expr: &Expr, dtype: DataType) -> Result<()> {
        if let Some(index) = self.bare_unpinned_parameter(expr)
            && let Some(slots) = &self.params
        {
            slots.pin(index, dtype)?;
        }
        Ok(())
    }

    /// Bind a binary operator's operands. When exactly one side is a bare,
    /// unpinned `?`, the other side binds first and pins the parameter's type
    /// (comparisons and arithmetic take the other operand's type; `LIKE`/
    /// `||` are VARCHAR, `AND`/`OR` BOOLEAN, date ± integer is INTEGER days).
    ///
    /// Both operands are bound exactly once — aggregates and alias lookups
    /// are not duplicated by the inference.
    fn bind_binary_operands(
        &mut self,
        op: BinaryOp,
        left: &Expr,
        right: &Expr,
        aggs: &mut Vec<BoundAgg>,
        allow_agg: bool,
    ) -> Result<(BoundExpr, BoundExpr)> {
        if let Some(index) = self.bare_unpinned_parameter(left)
            && !matches!(right, Expr::Parameter(_))
        {
            let r = self.bind_expr(right, aggs, allow_agg)?;
            let t = self.inferred_operand_type(op, r.dtype(), index);
            if let Some(slots) = &self.params {
                slots.pin(index, t)?;
            }
            let l = self.bind_expr(left, aggs, allow_agg)?;
            return Ok((l, r));
        }
        if let Some(index) = self.bare_unpinned_parameter(right) {
            let l = self.bind_expr(left, aggs, allow_agg)?;
            let t = self.inferred_operand_type(op, l.dtype(), index);
            if let Some(slots) = &self.params {
                slots.pin(index, t)?;
            }
            let r = self.bind_expr(right, aggs, allow_agg)?;
            return Ok((l, r));
        }
        let l = self.bind_expr(left, aggs, allow_agg)?;
        let r = self.bind_expr(right, aggs, allow_agg)?;
        Ok((l, r))
    }

    /// The type a bare `?` operand takes from the other side's type `ctx`:
    /// operator-fixed types first, else the wider of the numeric context and
    /// the bound value's own type (a BIGINT value beside an INTEGER literal
    /// stays BIGINT, like DuckDB), else the context type.
    fn inferred_operand_type(&self, op: BinaryOp, ctx: DataType, index: usize) -> DataType {
        if let Some(fixed) = fixed_operand_dtype(op, ctx) {
            return fixed;
        }
        let value_type = self.params.as_ref().and_then(|p| p.value_type(index));
        match value_type {
            Some(v) if is_numeric(v) && is_numeric(ctx) => common_numeric(v, ctx).unwrap_or(ctx),
            _ => ctx,
        }
    }

    /// Resolve a statement's table reference to a concrete database: an
    /// explicit qualifier must name an existing database; otherwise the
    /// session's default database is used.
    fn resolve_db(&self, db: &Option<String>) -> Result<String> {
        match db {
            Some(q) => {
                if !self.db.database_exists(q) {
                    return Err(PinkDbError::Catalog(format!("database \"{q}\" not found")));
                }
                Ok(q.to_ascii_lowercase())
            }
            None => Ok(self.default_db.to_ascii_lowercase()),
        }
    }

    /// Session-local views referenced while binding, in first-use order.
    fn views_used(&self) -> Vec<String> {
        self.views_used.borrow().clone()
    }

    /// Dynamic-function names (lowercased registry keys) referenced while
    /// binding, in first-use order.
    fn dynamic_functions_used(&self) -> Vec<String> {
        self.dynamic_functions_used.borrow().clone()
    }

    fn lookup_column(&self, table: Option<&str>, col: &str) -> Result<(usize, DataType)> {
        let mut found: Option<(usize, DataType)> = None;
        let mut base = 0;
        for t in &self.tables {
            if let Some(q) = table
                && !t.name.eq_ignore_ascii_case(q)
            {
                base += t.columns.len();
                continue;
            }
            for (i, (name, dtype)) in t.columns.iter().enumerate() {
                if name.eq_ignore_ascii_case(col) {
                    if table.is_none() && found.is_some() {
                        return Err(PinkDbError::Catalog(format!(
                            "column \"{col}\" is ambiguous"
                        )));
                    }
                    found = Some((base + i, *dtype));
                    if table.is_some() {
                        break;
                    }
                }
            }
            if table.is_some() && found.is_some() {
                break;
            }
            base += t.columns.len();
        }
        found.ok_or_else(|| {
            if let Some(q) = table {
                PinkDbError::Catalog(format!("no column \"{q}.{col}\""))
            } else {
                PinkDbError::Catalog(format!("no column \"{col}\""))
            }
        })
    }

    fn bind_expr(
        &mut self,
        expr: &Expr,
        aggs: &mut Vec<BoundAgg>,
        allow_agg: bool,
    ) -> Result<BoundExpr> {
        match expr {
            Expr::Literal(v) => Ok(BoundExpr::Literal(v.clone())),
            Expr::Parameter(index) => self.bind_parameter(*index),
            Expr::Star => Err(PinkDbError::Parse(
                "`*` is only valid in the projection list or COUNT(*)".to_string(),
            )),
            Expr::QualifiedStar(_) => Err(PinkDbError::Parse(
                "`t.*` is only valid in the projection list".to_string(),
            )),
            Expr::Column { table, name } => {
                // Wave 44: a procedure call's variables shadow columns. The
                // variable's *value* is bound as a literal (never SQL text).
                if table.is_none()
                    && let Some(value) = self.params.as_ref().and_then(|slots| slots.variable(name))
                {
                    return Ok(BoundExpr::Literal(value));
                }
                // Alias substitution (`... ORDER BY x`), with a cycle guard so
                // `SELECT x AS x` resolves to the column instead of recursing.
                if table.is_none() && self.allow_alias {
                    let key = name.to_ascii_lowercase();
                    if !self.resolving.contains(&key)
                        && let Some(aliased) = self.aliases.get(&key).cloned()
                    {
                        self.resolving.push(key);
                        let bound = self.bind_expr(&aliased, aggs, allow_agg);
                        self.resolving.pop();
                        return bound;
                    }
                }
                let (index, dtype) = self.lookup_column(table.as_deref(), name)?;
                Ok(BoundExpr::Column { index, dtype })
            }
            Expr::Binary { op, left, right } => {
                let (l, r) = self.bind_binary_operands(*op, left, right, aggs, allow_agg)?;
                let dtype = match op {
                    BinaryOp::And | BinaryOp::Or => {
                        if l.dtype() != DataType::Boolean || r.dtype() != DataType::Boolean {
                            return Err(PinkDbError::Type(
                                "AND/OR need BOOLEAN operands".to_string(),
                            ));
                        }
                        DataType::Boolean
                    }
                    BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
                    | BinaryOp::Like
                    | BinaryOp::NotLike => {
                        comparison_ok(*op, l.dtype(), r.dtype())?;
                        DataType::Boolean
                    }
                    // `a || b`: both sides format to VARCHAR (DuckDB casts
                    // implicitly); NULL on either side → NULL.
                    BinaryOp::Concat => {
                        let l2 = cast_to_varchar(l);
                        let r2 = cast_to_varchar(r);
                        return Ok(BoundExpr::Binary {
                            op: BinaryOp::Concat,
                            left: Box::new(l2),
                            right: Box::new(r2),
                            dtype: DataType::Varchar,
                        });
                    }
                    _ => arithmetic_dtype(*op, l.dtype(), r.dtype())?,
                };
                Ok(BoundExpr::Binary {
                    op: *op,
                    left: Box::new(l),
                    right: Box::new(r),
                    dtype,
                })
            }
            Expr::Unary { op, expr } => {
                let e = self.bind_expr(expr, aggs, allow_agg)?;
                match op {
                    UnaryOp::Not => {
                        if e.dtype() != DataType::Boolean {
                            return Err(PinkDbError::Type(
                                "NOT needs a BOOLEAN operand".to_string(),
                            ));
                        }
                        Ok(BoundExpr::Not(Box::new(e)))
                    }
                    UnaryOp::Negate => {
                        if !is_numeric(e.dtype()) {
                            return Err(PinkDbError::Type(
                                "unary `-` needs a numeric operand".to_string(),
                            ));
                        }
                        let dtype = e.dtype();
                        Ok(BoundExpr::Negate {
                            expr: Box::new(e),
                            dtype,
                        })
                    }
                    UnaryOp::Plus => Ok(e),
                }
            }
            Expr::IsNull { expr, negated } => {
                let e = self.bind_expr(expr, aggs, allow_agg)?;
                Ok(BoundExpr::IsNull {
                    expr: Box::new(e),
                    negated: *negated,
                })
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                // Desugar: `x BETWEEN l AND h` → `x >= l AND x <= h`.
                let desugared = Expr::Binary {
                    op: BinaryOp::And,
                    left: Box::new(Expr::Binary {
                        op: BinaryOp::GtEq,
                        left: expr.clone(),
                        right: low.clone(),
                    }),
                    right: Box::new(Expr::Binary {
                        op: BinaryOp::LtEq,
                        left: expr.clone(),
                        right: high.clone(),
                    }),
                };
                let bound = self.bind_expr(&desugared, aggs, allow_agg)?;
                if *negated {
                    Ok(BoundExpr::Not(Box::new(bound)))
                } else {
                    Ok(bound)
                }
            }
            Expr::Function {
                name,
                args,
                distinct,
            } => self.bind_function(name, args, *distinct, aggs, allow_agg),
            Expr::Window {
                name,
                args,
                over,
                distinct,
            } => self.bind_window(name, args, over, *distinct, aggs, allow_agg),
            Expr::Coalesce(args) => {
                let mut bound = Vec::with_capacity(args.len());
                for a in args {
                    bound.push(self.bind_expr(a, aggs, allow_agg)?);
                }
                // Common type across args (numeric tower, else identical);
                // bare NULLs fit any type. Narrower sides are Cast-wrapped.
                let mut common: Option<DataType> = None;
                for b in &bound {
                    common = fold_result_common(common, b)?;
                }
                let common = common.unwrap_or(DataType::Integer);
                let args = bound
                    .into_iter()
                    .map(|b| {
                        if b.dtype() == common || matches!(b, BoundExpr::Literal(Value::Null)) {
                            b
                        } else {
                            BoundExpr::Cast {
                                expr: Box::new(b),
                                dtype: common,
                            }
                        }
                    })
                    .collect();
                Ok(BoundExpr::Coalesce {
                    args,
                    dtype: common,
                })
            }
            Expr::Cast { expr, dtype } => {
                // `CAST(? AS t)` pins the parameter to `t`: the bound value is
                // converted to `t` at bind time (DuckDB reports the failed
                // conversion at bind too), and the cast itself is a no-op.
                self.pin_parameter(expr, *dtype)?;
                let bound = self.bind_expr(expr, aggs, allow_agg)?;
                check_cast(bound.dtype(), *dtype)?;
                Ok(BoundExpr::Cast {
                    expr: Box::new(bound),
                    dtype: *dtype,
                })
            }
            Expr::Case { branches, else_ } => {
                let mut bound_branches = Vec::with_capacity(branches.len());
                let mut common: Option<DataType> = None;
                for (cond, val) in branches {
                    let c = self.bind_expr(cond, aggs, allow_agg)?;
                    if c.dtype() != DataType::Boolean {
                        return Err(PinkDbError::Type(
                            "CASE WHEN needs a BOOLEAN condition".to_string(),
                        ));
                    }
                    let v = self.bind_expr(val, aggs, allow_agg)?;
                    common = fold_result_common(common, &v)?;
                    bound_branches.push((c, v));
                }
                let (else_bound, common) = match else_ {
                    Some(e) => {
                        let b = self.bind_expr(e, aggs, allow_agg)?;
                        let t = fold_result_common(common, &b)?.unwrap_or(b.dtype());
                        (Some(b), t)
                    }
                    // Missing ELSE is NULL: fits any result type. All-NULL
                    // results keep INTEGER (the NULL-literal convention).
                    None => (None, common.unwrap_or(DataType::Integer)),
                };
                // Widen narrower results to the common type.
                let branches = bound_branches
                    .into_iter()
                    .map(|(c, v)| {
                        let v = if v.dtype() == common {
                            v
                        } else {
                            BoundExpr::Cast {
                                expr: Box::new(v),
                                dtype: common,
                            }
                        };
                        (c, v)
                    })
                    .collect();
                let else_ = else_bound.map(|e| {
                    Box::new(if e.dtype() == common {
                        e
                    } else {
                        BoundExpr::Cast {
                            expr: Box::new(e),
                            dtype: common,
                        }
                    })
                });
                Ok(BoundExpr::Case {
                    branches,
                    else_,
                    dtype: common,
                })
            }
            Expr::ScalarSubquery(q) => {
                let mut child = self.child();
                let query = bind_select_in(child.db, &mut child, q)?;
                let schema = plan_schema(child.db, &child.default_db, &child.ctes, &query)?;
                if schema.len() != 1 {
                    return Err(PinkDbError::Execution(
                        "scalar subquery must return exactly one column".to_string(),
                    ));
                }
                let dtype = schema[0].1;
                Ok(BoundExpr::ScalarSubquery {
                    query: Box::new(query),
                    dtype,
                })
            }
            Expr::InSubquery {
                expr,
                query,
                negated,
            } => {
                let bound = self.bind_expr(expr, aggs, allow_agg)?;
                let mut child = self.child();
                let sub = bind_select_in(child.db, &mut child, query)?;
                let schema = plan_schema(child.db, &child.default_db, &child.ctes, &sub)?;
                if schema.len() != 1 {
                    return Err(PinkDbError::Execution(
                        "IN subquery must return exactly one column".to_string(),
                    ));
                }
                comparison_ok(BinaryOp::Eq, bound.dtype(), schema[0].1).map_err(|_| {
                    PinkDbError::Type(format!(
                        "cannot compare {} with IN-subquery {}",
                        bound.dtype(),
                        schema[0].1
                    ))
                })?;
                Ok(BoundExpr::InSubquery {
                    expr: Box::new(bound),
                    query: Box::new(sub),
                    negated: *negated,
                })
            }
            Expr::Exists { query, negated } => {
                let mut child = self.child();
                let sub = bind_select_in(child.db, &mut child, query)?;
                Ok(BoundExpr::Exists {
                    query: Box::new(sub),
                    negated: *negated,
                })
            }
        }
    }

    fn bind_window(
        &mut self,
        name: &str,
        args: &[Expr],
        over: &OverClause,
        distinct: bool,
        aggs: &mut Vec<BoundAgg>,
        allow_agg: bool,
    ) -> Result<BoundExpr> {
        if !self.allow_win {
            return Err(PinkDbError::Execution(
                "window functions are not allowed in WHERE, GROUP BY, or HAVING (use a subquery)"
                    .to_string(),
            ));
        }
        if distinct {
            return Err(PinkDbError::Execution(
                "DISTINCT is not supported in window functions".to_string(),
            ));
        }
        if self.in_window {
            return Err(PinkDbError::Execution(
                "nested window functions are not supported in v1".to_string(),
            ));
        }
        // Resolve the function first (arity checks mirror the agg path).
        let kind: WindowKind = if let Some(wf) = WindowFunc::from_name(name) {
            if args.iter().any(|a| a == &Expr::Star) {
                return Err(PinkDbError::Parse(
                    "`*` is only valid in COUNT(*)".to_string(),
                ));
            }
            match wf {
                WindowFunc::RowNumber | WindowFunc::Rank | WindowFunc::DenseRank => {
                    if !args.is_empty() {
                        return Err(PinkDbError::Type(format!("{name} takes no arguments")));
                    }
                }
                WindowFunc::Lag | WindowFunc::Lead => {
                    if args.is_empty() || args.len() > 3 {
                        return Err(PinkDbError::Type(format!(
                            "{name} takes 1 to 3 arguments (value, offset, default)"
                        )));
                    }
                }
                WindowFunc::NTile => {
                    if args.len() != 1 {
                        return Err(PinkDbError::Type(
                            "ntile takes exactly 1 argument".to_string(),
                        ));
                    }
                }
                WindowFunc::FirstValue | WindowFunc::LastValue => {
                    if args.len() != 1 {
                        return Err(PinkDbError::Type(format!(
                            "{name} takes exactly 1 argument"
                        )));
                    }
                }
                WindowFunc::Agg(_) => unreachable!("aggregate kinds resolve below"),
            }
            WindowKind::Simple(wf)
        } else if let Some(f) = AggFunc::from_name(name) {
            let is_star = args.len() == 1 && args[0] == Expr::Star;
            if args.iter().any(|a| a == &Expr::Star) && !is_star {
                return Err(PinkDbError::Parse(
                    "`*` is only valid in COUNT(*)".to_string(),
                ));
            }
            if f != AggFunc::Count && is_star {
                return Err(PinkDbError::Type(format!(
                    "only COUNT supports *, not {name}"
                )));
            }
            if !is_star && args.len() != 1 {
                return Err(PinkDbError::Type(format!(
                    "{name} takes exactly 1 argument in v1"
                )));
            }
            WindowKind::Agg(f)
        } else {
            return Err(PinkDbError::Catalog(format!("unknown function \"{name}\"")));
        };
        // Bind arg/partition/order in window scope (aggregates flow to the
        // shared agg list so `SUM(SUM(x)) OVER ()` works).
        self.in_window = true;
        let result = self.bind_window_inner(kind, args, over, aggs, allow_agg);
        self.in_window = false;
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn bind_window_inner(
        &mut self,
        kind: WindowKind,
        args: &[Expr],
        over: &OverClause,
        aggs: &mut Vec<BoundAgg>,
        allow_agg: bool,
    ) -> Result<BoundExpr> {
        let (func, dtype, arg, offset, default) = match kind {
            WindowKind::Simple(wf) => self.bind_simple_window(wf, args, aggs, allow_agg)?,
            WindowKind::Agg(f) => {
                let is_star = args.len() == 1 && args.first() == Some(&Expr::Star);
                let arg = if is_star || args.is_empty() {
                    None
                } else {
                    Some(self.bind_expr(&args[0], aggs, allow_agg)?)
                };
                let dtype = agg_dtype(f, arg.as_ref().map(|a| a.dtype()))?;
                (WindowFunc::Agg(f), dtype, arg, 1, None)
            }
        };
        let mut partition = Vec::with_capacity(over.partition.len());
        for p in &over.partition {
            partition.push(self.bind_expr(p, aggs, allow_agg)?);
        }
        let mut order = Vec::with_capacity(over.order.len());
        for o in &over.order {
            order.push((self.bind_expr(&o.expr, aggs, allow_agg)?, SortSpec::of(o)));
        }
        if let Some(f) = &over.frame
            && f.mode == FrameMode::Range
        {
            use crate::sql::ast::FrameBound as AstBound;
            let has_offset = matches!(f.start, AstBound::Preceding(_) | AstBound::Following(_))
                || matches!(f.end, AstBound::Preceding(_) | AstBound::Following(_));
            if order.len() > 1 {
                return Err(PinkDbError::Execution(
                    "RANGE window frames need at most one ORDER BY expression".to_string(),
                ));
            }
            if has_offset {
                let Some((key, _)) = order.first() else {
                    return Err(PinkDbError::Execution(
                        "RANGE frames with an offset need an ORDER BY expression".to_string(),
                    ));
                };
                match key.dtype() {
                    DataType::Integer | DataType::Bigint | DataType::Double | DataType::Date => {}
                    t => {
                        return Err(PinkDbError::Type(format!(
                            "RANGE frames with an offset need a numeric or DATE ORDER BY \
                                 expression, not {t}"
                        )));
                    }
                }
            }
        }
        let frame = match &over.frame {
            Some(f) => BoundFrame {
                mode: map_frame_mode(f.mode),
                start: map_frame_bound(f.start)?,
                end: map_frame_bound(f.end)?,
            },
            None => BoundFrame::default_for(!order.is_empty()),
        };
        let win = BoundWindow {
            func,
            arg,
            offset,
            default,
            partition,
            order,
            frame,
            dtype,
        };
        let index = match self.windows.iter().position(|w| w == &win) {
            Some(i) => i,
            None => {
                self.windows.push(win);
                self.windows.len() - 1
            }
        };
        Ok(BoundExpr::WindowRef { index, dtype })
    }

    /// Resolve a non-aggregate window function: its argument, result type,
    /// and (for lag/lead) the constant offset and default expression.
    #[allow(clippy::type_complexity)]
    fn bind_simple_window(
        &mut self,
        wf: WindowFunc,
        args: &[Expr],
        aggs: &mut Vec<BoundAgg>,
        allow_agg: bool,
    ) -> Result<(
        WindowFunc,
        DataType,
        Option<BoundExpr>,
        i64,
        Option<BoundExpr>,
    )> {
        match wf {
            WindowFunc::Lag | WindowFunc::Lead => {
                let name = if wf == WindowFunc::Lag { "lag" } else { "lead" };
                let arg = self.bind_expr(&args[0], aggs, allow_agg)?;
                // DuckDB accepts any integer constant (negative flips the
                // direction); a non-constant offset is not supported here.
                let offset = match args.get(1) {
                    None => 1,
                    Some(e) => const_i64(e).ok_or_else(|| {
                        PinkDbError::Type(format!("{name} offset must be an integer constant"))
                    })?,
                };
                let default = match args.get(2) {
                    Some(e) => Some(self.bind_expr(e, aggs, allow_agg)?),
                    None => None,
                };
                // Result type: the value argument's type; a bare NULL value
                // takes the default's type instead (DuckDB rule).
                let dtype = if matches!(arg, BoundExpr::Literal(Value::Null)) {
                    default
                        .as_ref()
                        .map(|d| d.dtype())
                        .unwrap_or(DataType::Integer)
                } else {
                    arg.dtype()
                };
                let arg = cast_window_arg(arg, dtype)?;
                let default = match default {
                    Some(d) => Some(cast_window_arg(d, dtype)?),
                    None => None,
                };
                Ok((wf, dtype, Some(arg), offset, default))
            }
            WindowFunc::NTile => {
                let n = const_i64(&args[0]).ok_or_else(|| {
                    PinkDbError::Type("ntile argument must be a constant integer".to_string())
                })?;
                if n <= 0 {
                    return Err(PinkDbError::Type(
                        "Argument for ntile must be greater than zero".to_string(),
                    ));
                }
                Ok((wf, DataType::Bigint, None, n, None))
            }
            WindowFunc::FirstValue | WindowFunc::LastValue => {
                let arg = self.bind_expr(&args[0], aggs, allow_agg)?;
                let dtype = arg.dtype();
                Ok((wf, dtype, Some(arg), 1, None))
            }
            WindowFunc::RowNumber | WindowFunc::Rank | WindowFunc::DenseRank => {
                // No argument; the rank itself is a BIGINT.
                Ok((wf, DataType::Bigint, None, 1, None))
            }
            WindowFunc::Agg(_) => unreachable!("{wf:?} does not bind through bind_simple_window"),
        }
    }

    fn bind_function(
        &mut self,
        name: &str,
        args: &[Expr],
        distinct: bool,
        aggs: &mut Vec<BoundAgg>,
        allow_agg: bool,
    ) -> Result<BoundExpr> {
        if let Some(func) = AggFunc::from_name(name) {
            if !allow_agg {
                return Err(PinkDbError::Execution(
                    "aggregate functions are not allowed in WHERE or GROUP BY".to_string(),
                ));
            }
            if distinct && (func != AggFunc::Count || args.first() == Some(&Expr::Star)) {
                return Err(PinkDbError::Execution(
                    "DISTINCT is only supported in COUNT(DISTINCT x) in v1".to_string(),
                ));
            }
            let is_star = args.len() == 1 && args[0] == Expr::Star;
            if args.iter().any(|a| a == &Expr::Star) && !is_star {
                return Err(PinkDbError::Parse(
                    "`*` is only valid in COUNT(*)".to_string(),
                ));
            }
            if func != AggFunc::Count && is_star {
                return Err(PinkDbError::Type(format!(
                    "only COUNT supports *, not {name}"
                )));
            }
            if !is_star && args.len() != 1 {
                return Err(PinkDbError::Type(format!(
                    "{name} takes exactly 1 argument in v1"
                )));
            }
            let arg = if is_star {
                None
            } else {
                let a = self.bind_expr(&args[0], aggs, allow_agg)?;
                if !a.is_aggregate_free() {
                    return Err(PinkDbError::Execution(
                        "nested aggregates are not supported in v1".to_string(),
                    ));
                }
                Some(a)
            };
            let dtype = agg_dtype(func, arg.as_ref().map(|a| a.dtype()))?;
            let agg = BoundAgg {
                func,
                arg,
                dtype,
                distinct,
            };
            // Dedupe identical aggregates so `SUM(x), SUM(x)` computes once.
            let index = match aggs.iter().position(|a| a == &agg) {
                Some(i) => i,
                None => {
                    aggs.push(agg);
                    aggs.len() - 1
                }
            };
            return Ok(BoundExpr::AggRef { index, dtype });
        }
        if distinct {
            return Err(PinkDbError::Execution(
                "DISTINCT is only supported in COUNT(DISTINCT x) in v1".to_string(),
            ));
        }
        if let Some(func) = ScalarFunc::from_name(name) {
            let mut bound = Vec::with_capacity(args.len());
            for a in args {
                if a == &Expr::Star {
                    return Err(PinkDbError::Parse(
                        "`*` is only valid in COUNT(*)".to_string(),
                    ));
                }
                bound.push(self.bind_expr(a, aggs, allow_agg)?);
            }
            let dtype = scalar_dtype(func, &bound)?;
            let bound = coerce_scalar_args(func, dtype, bound)?;
            return Ok(BoundExpr::Scalar {
                func,
                args: bound,
                dtype,
            });
        }
        // Not a built-in: a loaded extension may provide it (case-insensitive)
        // — a `dlopen` callback or a linked registry function.
        self.bind_dynamic_function(name, args, aggs, allow_agg)
    }

    /// Bind a call to a function registered by a loaded extension.
    ///
    /// ABI v1 is strictly typed: every argument's bound type must equal the
    /// registered argument type (a bare `NULL` literal is typeless and is
    /// passed as a NULL of the registered type), and the result type is the
    /// registered return type. The one implicit conversion is the numeric
    /// widening into a `DOUBLE` parameter (see below). Both loading models
    /// register through the same registry, so this is the one binder path for
    /// either. A linked function may declare a ranged or variadic arity
    /// (`new_optional` / `new_variadic`); `dlopen` functions and plain linked
    /// ones are fixed.
    fn bind_dynamic_function(
        &mut self,
        name: &str,
        args: &[Expr],
        aggs: &mut Vec<BoundAgg>,
        allow_agg: bool,
    ) -> Result<BoundExpr> {
        let Some(func) = self.db.dynamic_scalar(name) else {
            return Err(PinkDbError::Catalog(format!("unknown function \"{name}\"")));
        };
        // Copy the metadata out before binding arguments (which needs
        // `&mut self`): `func` borrows `self.db`.
        let func_name = func.name().to_string();
        // Record the canonical registry key for the WAL layer: a logged
        // statement that calls this function is prefixed with its extension's
        // `LOAD EXTENSION` when the current WAL generation has not loaded it.
        let key = func_name.to_ascii_lowercase();
        {
            let mut used = self.dynamic_functions_used.borrow_mut();
            if !used.contains(&key) {
                used.push(key.clone());
            }
        }
        let want_args: Vec<crate::ext::ExtType> = func.arg_types().to_vec();
        let min_args = func.min_args();
        let max_args = func.max_args();
        let dtype = crate::ext::dtype_of_ext(func.ret_type());
        if args.len() < min_args || args.len() > max_args {
            // Fixed arity keeps the historical wording; the ranged arities
            // (linked `new_optional` / `new_variadic`) say which bound was
            // missed.
            let message = if min_args == max_args {
                format!("{func_name} takes {min_args} argument(s)")
            } else if args.len() < min_args {
                if func.is_variadic() {
                    format!("{func_name} takes at least {min_args} argument(s)")
                } else {
                    format!("{func_name} takes between {min_args} and {max_args} argument(s)")
                }
            } else {
                format!("{func_name} takes at most {max_args} argument(s)")
            };
            return Err(PinkDbError::Type(message));
        }
        let mut bound = Vec::with_capacity(args.len());
        for (i, a) in args.iter().enumerate() {
            if a == &Expr::Star {
                return Err(PinkDbError::Parse(
                    "`*` is only valid in COUNT(*)".to_string(),
                ));
            }
            let b = self.bind_expr(a, aggs, allow_agg)?;
            // The declared type of position `i`: positional, or the repeated
            // last type of a variadic function.
            let want = match want_args.get(i) {
                Some(t) => *t,
                None => *want_args.last().expect("a variadic function has a type"),
            };
            // A bare NULL literal is typeless: the executor hands the
            // callback `is_null` with the registered type tag.
            //
            // Everything else must equal the registered argument type — ABI
            // v1 inserts no implicit casts — with one documented widening
            // rule: a **DOUBLE parameter accepts any numeric argument**
            // (INTEGER, BIGINT, DECIMAL) and the engine inserts the CAST to
            // DOUBLE that DuckDB applies implicitly, so `exp(1)`, `log(100)`
            // and an integer or DECIMAL column all work like DuckDB. Every
            // other declared type stays exact and keeps the CAST hint —
            // VARCHAR included (`json_array(1)` and `ext_repeat('x', 1)`
            // keep their documented errors): a string is not a number, and
            // parsing one is not this ABI's job.
            let mut b = b;
            if !matches!(b, BoundExpr::Literal(Value::Null))
                && crate::ext::ext_type_of(b.dtype()) != Some(want)
            {
                if want == crate::ext::ExtType::Double && b.dtype().is_numeric() {
                    b = BoundExpr::Cast {
                        expr: Box::new(b),
                        dtype: DataType::Double,
                    };
                } else {
                    return Err(PinkDbError::Type(format!(
                        "{func_name} argument {} must be {want}, got {} (use CAST)",
                        i + 1,
                        b.dtype()
                    )));
                }
            }
            bound.push(b);
        }
        Ok(BoundExpr::DynamicScalar {
            name: func_name.to_ascii_lowercase().into_boxed_str(),
            args: bound.into_boxed_slice(),
            dtype,
        })
    }
}

/// A VARCHAR slot for the string functions: a VARCHAR argument or a bare
/// NULL literal (which the executor turns into NULL).
fn varchar_arg(e: &BoundExpr) -> bool {
    e.dtype() == DataType::Varchar || matches!(e, BoundExpr::Literal(Value::Null))
}

/// Fold an integer constant expression (literals with unary `+`/`-`).
/// Materializing the value underflows to None (an overflowing literal is a
/// parse error long before this).
fn const_i64(e: &Expr) -> Option<i64> {
    match e {
        Expr::Literal(Value::Integer(n)) => Some(*n as i64),
        Expr::Literal(Value::Bigint(n)) => Some(*n),
        Expr::Unary {
            op: UnaryOp::Negate,
            expr,
        } => const_i64(expr).and_then(i64::checked_neg),
        Expr::Unary {
            op: UnaryOp::Plus,
            expr,
        } => const_i64(expr),
        _ => None,
    }
}

/// Implicitly cast one `||` operand to VARCHAR (everything formats to
/// VARCHAR; a bare NULL stays typeless so its NULL propagates).
fn cast_to_varchar(e: BoundExpr) -> BoundExpr {
    if e.dtype() == DataType::Varchar || matches!(e, BoundExpr::Literal(Value::Null)) {
        e
    } else {
        BoundExpr::Cast {
            expr: Box::new(e),
            dtype: DataType::Varchar,
        }
    }
}

/// Wrap scalar arguments in casts where a function's runtime kernel needs
/// one layout: `concat` reads VARCHARs (DuckDB casts implicitly), and
/// greatest/least compare in their common type.
fn coerce_scalar_args(
    func: ScalarFunc,
    dtype: DataType,
    args: Vec<BoundExpr>,
) -> Result<Vec<BoundExpr>> {
    let want = match func {
        ScalarFunc::Concat => DataType::Varchar,
        ScalarFunc::Greatest | ScalarFunc::Least | ScalarFunc::Mod => dtype,
        _ => return Ok(args),
    };
    args.into_iter()
        .map(|a| {
            if a.dtype() == want || matches!(a, BoundExpr::Literal(Value::Null)) {
                Ok(a)
            } else {
                check_cast(a.dtype(), want)?;
                Ok(BoundExpr::Cast {
                    expr: Box::new(a),
                    dtype: want,
                })
            }
        })
        .collect()
}

/// Wraps `e` in `CAST(e AS dtype)` when its type differs (bare NULL literals
/// are typeless already). Used for lag/lead value/default coercion.
fn cast_window_arg(e: BoundExpr, dtype: DataType) -> Result<BoundExpr> {
    if e.dtype() == dtype || matches!(e, BoundExpr::Literal(Value::Null)) {
        return Ok(e);
    }
    check_cast(e.dtype(), dtype)?;
    Ok(BoundExpr::Cast {
        expr: Box::new(e),
        dtype,
    })
}

fn agg_dtype(func: AggFunc, arg: Option<DataType>) -> Result<DataType> {
    match func {
        AggFunc::Count => Ok(DataType::Bigint),
        AggFunc::Avg => {
            let t = arg.expect("avg has an arg");
            if !is_numeric(t) {
                return Err(PinkDbError::Type(
                    "AVG needs a numeric argument".to_string(),
                ));
            }
            Ok(DataType::Double)
        }
        AggFunc::Sum => match arg.expect("sum has an arg") {
            DataType::Integer | DataType::Bigint => Ok(DataType::Bigint),
            DataType::Double => Ok(DataType::Double),
            // SUM(DECIMAL(p,s)) → DECIMAL(38,s) with a runtime overflow check
            // (DuckDB).
            DataType::Decimal { scale, .. } => Ok(DataType::Decimal {
                precision: DECIMAL_MAX_PRECISION,
                scale,
            }),
            _ => Err(PinkDbError::Type(
                "SUM needs a numeric argument".to_string(),
            )),
        },
        AggFunc::Min | AggFunc::Max => {
            let t = arg.expect("min/max has an arg");
            if !is_numeric(t)
                && t != DataType::Varchar
                && t != DataType::Boolean
                && t != DataType::Date
                && t != DataType::Timestamp
                && t != DataType::Interval
            {
                return Err(PinkDbError::Type(
                    "MIN/MAX need an orderable argument".to_string(),
                ));
            }
            Ok(t)
        }
    }
}

fn scalar_dtype(func: ScalarFunc, args: &[BoundExpr]) -> Result<DataType> {
    let arity = |allowed: &[usize]| {
        if allowed.contains(&args.len()) {
            Ok(())
        } else {
            let want = allowed
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(" or ");
            Err(PinkDbError::Type(format!(
                "{func:?} takes {want} argument(s) in v1"
            )))
        }
    };
    match func {
        ScalarFunc::Abs => {
            arity(&[1])?;
            let t = args[0].dtype();
            if !is_numeric(t) {
                return Err(PinkDbError::Type(
                    "abs needs a numeric argument".to_string(),
                ));
            }
            Ok(t)
        }
        ScalarFunc::Round => {
            arity(&[1, 2])?;
            if !is_numeric(args[0].dtype()) {
                return Err(PinkDbError::Type(
                    "round needs a numeric argument".to_string(),
                ));
            }
            if args.len() == 2 && !is_numeric(args[1].dtype()) {
                return Err(PinkDbError::Type(
                    "round digits must be numeric".to_string(),
                ));
            }
            // DECIMAL keeps its family: ROUND(d) → DECIMAL(p,0),
            // ROUND(d,n) → DECIMAL(p,n) for 0 ≤ n < s, DECIMAL(p,0) for
            // n < 0, and unchanged DECIMAL(p,s) for n ≥ s (DuckDB). The
            // digits must be a constant (the result type depends on it).
            if let DataType::Decimal { precision, scale } = args[0].dtype() {
                let digits = match args.get(1) {
                    None => 0i64,
                    Some(BoundExpr::Literal(Value::Integer(n))) => *n as i64,
                    Some(BoundExpr::Literal(Value::Bigint(n))) => *n,
                    Some(a) => {
                        return Err(PinkDbError::Type(format!(
                            "round digits must be a constant integer, got {}",
                            a.dtype()
                        )));
                    }
                };
                let out_scale = if digits >= scale as i64 {
                    scale
                } else {
                    digits.max(0) as u8
                };
                return Ok(DataType::Decimal {
                    precision: precision.max(out_scale).max(1),
                    scale: out_scale,
                });
            }
            Ok(DataType::Double)
        }
        ScalarFunc::Upper | ScalarFunc::Lower => {
            arity(&[1])?;
            if args[0].dtype() != DataType::Varchar {
                return Err(PinkDbError::Type(format!("{func:?} needs VARCHAR")));
            }
            Ok(DataType::Varchar)
        }
        ScalarFunc::Length => {
            arity(&[1])?;
            if args[0].dtype() != DataType::Varchar {
                return Err(PinkDbError::Type("length needs VARCHAR".to_string()));
            }
            Ok(DataType::Integer)
        }
        ScalarFunc::Substr => {
            arity(&[2, 3])?;
            if args[0].dtype() != DataType::Varchar {
                return Err(PinkDbError::Type("substr needs VARCHAR input".to_string()));
            }
            for a in &args[1..] {
                if !is_numeric(a.dtype()) {
                    return Err(PinkDbError::Type(
                        "substr start/length must be numeric".to_string(),
                    ));
                }
            }
            Ok(DataType::Varchar)
        }
        ScalarFunc::Year | ScalarFunc::Month | ScalarFunc::Day => {
            arity(&[1])?;
            match args[0].dtype() {
                DataType::Date | DataType::Timestamp => Ok(DataType::Integer),
                t => Err(PinkDbError::Type(format!(
                    "{func:?} needs DATE or TIMESTAMP, got {t}"
                ))),
            }
        }
        ScalarFunc::Hour | ScalarFunc::Minute | ScalarFunc::Second => {
            arity(&[1])?;
            match args[0].dtype() {
                DataType::Timestamp => Ok(DataType::Integer),
                t => Err(PinkDbError::Type(format!(
                    "{func:?} needs TIMESTAMP, got {t}"
                ))),
            }
        }
        ScalarFunc::DateTrunc => {
            arity(&[2])?;
            if args[0].dtype() != DataType::Varchar {
                return Err(PinkDbError::Type(
                    "date_trunc needs a VARCHAR part ('year', 'month', 'day', 'hour', 'minute', 'second')".to_string(),
                ));
            }
            match args[1].dtype() {
                DataType::Date | DataType::Timestamp => Ok(args[1].dtype()),
                t => Err(PinkDbError::Type(format!(
                    "date_trunc needs DATE or TIMESTAMP, got {t}"
                ))),
            }
        }
        ScalarFunc::Concat => {
            if args.is_empty() {
                return Err(PinkDbError::Type(
                    "concat needs at least 1 argument".to_string(),
                ));
            }
            // Every argument coerces to VARCHAR (DuckDB); NULL args are
            // skipped at evaluation, so a bare NULL literal stays bare.
            Ok(DataType::Varchar)
        }
        ScalarFunc::Trim | ScalarFunc::LTrim | ScalarFunc::RTrim => {
            arity(&[1, 2])?;
            // A bare NULL literal fits the VARCHAR slots (DuckDB types it
            // VARCHAR for these functions; the result is NULL anyway).
            if !varchar_arg(&args[0]) {
                return Err(PinkDbError::Type(format!("{func:?} needs VARCHAR")));
            }
            if args.len() == 2 && !varchar_arg(&args[1]) {
                return Err(PinkDbError::Type(format!(
                    "{func:?} characters must be VARCHAR"
                )));
            }
            Ok(DataType::Varchar)
        }
        ScalarFunc::Replace => {
            arity(&[3])?;
            for a in args {
                if !varchar_arg(a) {
                    return Err(PinkDbError::Type(
                        "replace needs VARCHAR arguments".to_string(),
                    ));
                }
            }
            Ok(DataType::Varchar)
        }
        ScalarFunc::NullIf => {
            arity(&[2])?;
            let (t0, t1) = (args[0].dtype(), args[1].dtype());
            // Compare types only when both sides are actually typed (a bare
            // NULL never needs a comparison, so `nullif(x, NULL)` is fine).
            let null0 = matches!(args[0], BoundExpr::Literal(Value::Null));
            let null1 = matches!(args[1], BoundExpr::Literal(Value::Null));
            if !null0 && !null1 {
                comparison_ok(BinaryOp::Eq, t0, t1)?;
            }
            // Result type: the first argument's (DuckDB); a bare NULL first
            // argument falls back to the second's type.
            Ok(if null0 {
                if null1 { DataType::Integer } else { t1 }
            } else {
                t0
            })
        }
        ScalarFunc::Greatest | ScalarFunc::Least => {
            if args.is_empty() {
                return Err(PinkDbError::Type(format!(
                    "{func:?} needs at least 1 argument"
                )));
            }
            // Common type across the typed arguments (NULLs are skipped).
            let mut common: Option<DataType> = None;
            for a in args {
                if matches!(a, BoundExpr::Literal(Value::Null)) {
                    continue;
                }
                common = Some(match common {
                    None => a.dtype(),
                    Some(t) => coalesce_common(t, a.dtype()).map_err(|_| {
                        PinkDbError::Type(format!(
                            "cannot combine {} with {} in {func:?}",
                            t,
                            a.dtype()
                        ))
                    })?,
                });
            }
            Ok(common.unwrap_or(DataType::Integer))
        }
        ScalarFunc::Extract => {
            arity(&[2])?;
            if args[0].dtype() != DataType::Varchar {
                return Err(PinkDbError::Type(
                    "extract needs a VARCHAR part as its first argument".to_string(),
                ));
            }
            match args[1].dtype() {
                DataType::Date | DataType::Timestamp => {}
                t => {
                    return Err(PinkDbError::Type(format!(
                        "extract needs DATE or TIMESTAMP, got {t}"
                    )));
                }
            }
            // Literal parts are validated now and yield BIGINT (the epoch is
            // DOUBLE). A non-constant VARCHAR part hits DuckDB's other
            // overload, which returns DOUBLE.
            if let BoundExpr::Literal(Value::Varchar(part)) = &args[0] {
                let field = extract_field(part).map_err(PinkDbError::Type)?;
                return Ok(match field {
                    ExtractField::Epoch => DataType::Double,
                    _ => DataType::Bigint,
                });
            }
            Ok(DataType::Double)
        }
        ScalarFunc::Sqrt | ScalarFunc::Floor | ScalarFunc::Ceil => {
            arity(&[1])?;
            if !is_numeric(args[0].dtype()) {
                return Err(PinkDbError::Type(format!(
                    "{func:?} needs a numeric argument"
                )));
            }
            // FLOOR/CEIL on DECIMAL return DECIMAL(p,0) (DuckDB).
            if func != ScalarFunc::Sqrt
                && let DataType::Decimal { precision, .. } = args[0].dtype()
            {
                return Ok(DataType::Decimal {
                    precision,
                    scale: 0,
                });
            }
            Ok(DataType::Double)
        }
        ScalarFunc::Sign => {
            arity(&[1])?;
            if !is_numeric(args[0].dtype()) {
                return Err(PinkDbError::Type(
                    "sign needs a numeric argument".to_string(),
                ));
            }
            Ok(DataType::Integer)
        }
        ScalarFunc::Power => {
            arity(&[2])?;
            if !is_numeric(args[0].dtype()) || !is_numeric(args[1].dtype()) {
                return Err(PinkDbError::Type(
                    "power needs numeric arguments".to_string(),
                ));
            }
            Ok(DataType::Double)
        }
        ScalarFunc::Mod => {
            arity(&[2])?;
            common_numeric(args[0].dtype(), args[1].dtype())
                .map_err(|_| PinkDbError::Type("mod needs numeric arguments".to_string()))
        }
    }
}

/// Default output name for a projection (DuckDB-style expression text).
fn default_name(expr: &Expr) -> String {
    match expr {
        Expr::Literal(v) => v.to_string(),
        // Only reachable when binding an unparameterized statement errors
        // first; the display name keeps `SELECT ?`-style output sensible.
        Expr::Parameter(i) => format!("?{}", i + 1),
        Expr::Star => "*".to_string(),
        Expr::Column { table, name } => match table {
            Some(t) => format!("{t}.{name}"),
            None => name.clone(),
        },
        Expr::QualifiedStar(t) => match t {
            Some(t) => format!("{t}.*"),
            None => "*".to_string(),
        },
        Expr::Binary { op, left, right } => {
            let sym = match op {
                BinaryOp::Add => "+",
                BinaryOp::Subtract => "-",
                BinaryOp::Multiply => "*",
                BinaryOp::Divide => "/",
                BinaryOp::Modulo => "%",
                BinaryOp::Eq => "=",
                BinaryOp::NotEq => "<>",
                BinaryOp::Lt => "<",
                BinaryOp::LtEq => "<=",
                BinaryOp::Gt => ">",
                BinaryOp::GtEq => ">=",
                BinaryOp::And => "AND",
                BinaryOp::Or => "OR",
                BinaryOp::Like => "LIKE",
                BinaryOp::NotLike => "NOT LIKE",
                BinaryOp::Concat => "||",
            };
            format!("({} {sym} {})", default_name(left), default_name(right))
        }
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => format!("(NOT {})", default_name(expr)),
            UnaryOp::Negate => format!("(-{})", default_name(expr)),
            UnaryOp::Plus => default_name(expr),
        },
        Expr::IsNull { expr, negated } => {
            let not = if *negated { " NOT" } else { "" };
            format!("({}{not} IS NULL)", default_name(expr))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let not = if *negated { "NOT " } else { "" };
            format!(
                "({} {not}BETWEEN {} AND {})",
                default_name(expr),
                default_name(low),
                default_name(high)
            )
        }
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            let inner: Vec<String> = args
                .iter()
                .map(|a| {
                    if a == &Expr::Star {
                        "*".to_string()
                    } else {
                        default_name(a)
                    }
                })
                .collect();
            let distinct = if *distinct { "DISTINCT " } else { "" };
            format!(
                "{}({distinct}{})",
                name.to_ascii_lowercase(),
                inner.join(", ")
            )
        }
        Expr::ScalarSubquery(_) => "(subquery)".to_string(),
        Expr::InSubquery { negated, .. } => {
            format!("{}in (subquery)", if *negated { "not " } else { "" })
        }
        Expr::Exists { negated, .. } => {
            format!("{}exists(subquery)", if *negated { "not " } else { "" })
        }
        Expr::Window { name, args, .. } => {
            let inner: Vec<String> = args
                .iter()
                .map(|a| {
                    if a == &Expr::Star {
                        "*".to_string()
                    } else {
                        default_name(a)
                    }
                })
                .collect();
            format!("{}({}) OVER", name.to_ascii_lowercase(), inner.join(", "))
        }
        Expr::Coalesce(args) => {
            let inner: Vec<String> = args.iter().map(default_name).collect();
            format!("coalesce({})", inner.join(", "))
        }
        Expr::Cast { expr, dtype } => format!("cast({} as {dtype})", default_name(expr)),
        Expr::Case {
            branches, else_, ..
        } => {
            let mut s = String::from("CASE");
            for (c, v) in branches {
                s.push_str(&format!(
                    " WHEN {} THEN {}",
                    default_name(c),
                    default_name(v)
                ));
            }
            if let Some(e) = else_ {
                s.push_str(&format!(" ELSE {}", default_name(e)));
            }
            s.push_str(" END");
            s
        }
    }
}

/// Every aggregate-free fragment of `expr` that references a column must
/// equal a GROUP BY key. Pure constants (no columns) and aggregate results
/// are always fine.
fn validate_grouped(expr: &BoundExpr, keys: &[BoundExpr]) -> Result<()> {
    if expr.is_aggregate_free() {
        if expr.has_column_ref() && !keys.contains(expr) {
            return Err(PinkDbError::Execution(
                "SELECT/HAVING expression must be an aggregate or appear in GROUP BY".to_string(),
            ));
        }
        return Ok(());
    }
    match expr {
        BoundExpr::AggRef { .. }
        | BoundExpr::WindowRef { .. }
        | BoundExpr::Literal(_)
        | BoundExpr::Column { .. }
        | BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. } => Ok(()),
        BoundExpr::Not(e)
        | BoundExpr::Negate { expr: e, .. }
        | BoundExpr::IsNull { expr: e, .. } => validate_grouped(e, keys),
        BoundExpr::Binary { left, right, .. } => {
            validate_grouped(left, keys)?;
            validate_grouped(right, keys)
        }
        BoundExpr::Scalar { args, .. } => {
            for a in args {
                validate_grouped(a, keys)?;
            }
            Ok(())
        }
        BoundExpr::DynamicScalar { args, .. } => {
            for a in args.iter() {
                validate_grouped(a, keys)?;
            }
            Ok(())
        }
        BoundExpr::Cast { expr, .. } => validate_grouped(expr, keys),
        BoundExpr::Coalesce { args, .. } => {
            for a in args {
                validate_grouped(a, keys)?;
            }
            Ok(())
        }
        BoundExpr::Case {
            branches, else_, ..
        } => {
            for (c, v) in branches {
                validate_grouped(c, keys)?;
                validate_grouped(v, keys)?;
            }
            if let Some(e) = else_ {
                validate_grouped(e, keys)?;
            }
            Ok(())
        }
        BoundExpr::InSubquery { expr, .. } => validate_grouped(expr, keys),
    }
}

/// Rebase a post-aggregation expression onto Aggregate-output scope:
/// maximal aggregate-free fragments become `Column{key position}`, AggRefs
/// and literals pass through. Returns `None` if a fragment matches no key.
fn rebase_to_keys(expr: &BoundExpr, keys: &[BoundExpr]) -> Option<BoundExpr> {
    if expr.is_aggregate_free() {
        // Constants (no columns) are scope-free: the same value in every group.
        if !expr.has_column_ref() {
            return Some(expr.clone());
        }
        return keys
            .iter()
            .position(|k| k == expr)
            .map(|pos| BoundExpr::Column {
                index: pos,
                dtype: expr.dtype(),
            });
    }
    Some(match expr {
        BoundExpr::AggRef { .. } => expr.clone(),
        BoundExpr::Not(e) => BoundExpr::Not(Box::new(rebase_to_keys(e, keys)?)),
        BoundExpr::Negate { expr: e, dtype } => BoundExpr::Negate {
            expr: Box::new(rebase_to_keys(e, keys)?),
            dtype: *dtype,
        },
        BoundExpr::IsNull { expr: e, negated } => BoundExpr::IsNull {
            expr: Box::new(rebase_to_keys(e, keys)?),
            negated: *negated,
        },
        BoundExpr::Binary {
            op,
            left,
            right,
            dtype,
        } => BoundExpr::Binary {
            op: *op,
            left: Box::new(rebase_to_keys(left, keys)?),
            right: Box::new(rebase_to_keys(right, keys)?),
            dtype: *dtype,
        },
        BoundExpr::Scalar { func, args, dtype } => BoundExpr::Scalar {
            func: *func,
            args: args
                .iter()
                .map(|a| rebase_to_keys(a, keys))
                .collect::<Option<Vec<_>>>()?,
            dtype: *dtype,
        },
        BoundExpr::DynamicScalar { name, args, dtype } => BoundExpr::DynamicScalar {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rebase_to_keys(a, keys))
                .collect::<Option<Vec<_>>>()?
                .into_boxed_slice(),
            dtype: *dtype,
        },
        BoundExpr::InSubquery {
            expr,
            query,
            negated,
        } => BoundExpr::InSubquery {
            expr: Box::new(rebase_to_keys(expr, keys)?),
            query: query.clone(),
            negated: *negated,
        },
        BoundExpr::Cast { expr, dtype } => BoundExpr::Cast {
            expr: Box::new(rebase_to_keys(expr, keys)?),
            dtype: *dtype,
        },
        BoundExpr::Coalesce { args, dtype } => BoundExpr::Coalesce {
            args: args
                .iter()
                .map(|a| rebase_to_keys(a, keys))
                .collect::<Option<Vec<_>>>()?,
            dtype: *dtype,
        },
        BoundExpr::Case {
            branches,
            else_,
            dtype,
        } => BoundExpr::Case {
            branches: branches
                .iter()
                .map(|(c, v)| Some((rebase_to_keys(c, keys)?, rebase_to_keys(v, keys)?)))
                .collect::<Option<Vec<_>>>()?,
            else_: match else_ {
                Some(e) => Some(Box::new(rebase_to_keys(e, keys)?)),
                None => None,
            },
            dtype: *dtype,
        },
        // ScalarSubquery/Exists bodies live in their own scope; WindowRef
        // already addresses window output.
        BoundExpr::ScalarSubquery { .. }
        | BoundExpr::Exists { .. }
        | BoundExpr::WindowRef { .. } => expr.clone(),
        _ => return None,
    })
}

/// Rebase relative `WindowRef{index}` past the window input columns.
fn resolve_window_refs(expr: BoundExpr, input_width: usize) -> BoundExpr {
    match expr {
        BoundExpr::WindowRef { index, dtype } => BoundExpr::WindowRef {
            index: input_width + index,
            dtype,
        },
        BoundExpr::Binary {
            op,
            left,
            right,
            dtype,
        } => BoundExpr::Binary {
            op,
            left: Box::new(resolve_window_refs(*left, input_width)),
            right: Box::new(resolve_window_refs(*right, input_width)),
            dtype,
        },
        BoundExpr::Not(e) => BoundExpr::Not(Box::new(resolve_window_refs(*e, input_width))),
        BoundExpr::Negate { expr, dtype } => BoundExpr::Negate {
            expr: Box::new(resolve_window_refs(*expr, input_width)),
            dtype,
        },
        BoundExpr::IsNull { expr, negated } => BoundExpr::IsNull {
            expr: Box::new(resolve_window_refs(*expr, input_width)),
            negated,
        },
        BoundExpr::Scalar { func, args, dtype } => BoundExpr::Scalar {
            func,
            args: args
                .into_iter()
                .map(|a| resolve_window_refs(a, input_width))
                .collect(),
            dtype,
        },
        BoundExpr::DynamicScalar { name, args, dtype } => BoundExpr::DynamicScalar {
            name,
            args: args
                .into_vec()
                .into_iter()
                .map(|a| resolve_window_refs(a, input_width))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            dtype,
        },
        BoundExpr::InSubquery {
            expr,
            query,
            negated,
        } => BoundExpr::InSubquery {
            expr: Box::new(resolve_window_refs(*expr, input_width)),
            query,
            negated,
        },
        BoundExpr::Cast { expr, dtype } => BoundExpr::Cast {
            expr: Box::new(resolve_window_refs(*expr, input_width)),
            dtype,
        },
        BoundExpr::Coalesce { args, dtype } => BoundExpr::Coalesce {
            args: args
                .into_iter()
                .map(|a| resolve_window_refs(a, input_width))
                .collect(),
            dtype,
        },
        BoundExpr::Case {
            branches,
            else_,
            dtype,
        } => BoundExpr::Case {
            branches: branches
                .into_iter()
                .map(|(c, v)| {
                    (
                        resolve_window_refs(c, input_width),
                        resolve_window_refs(v, input_width),
                    )
                })
                .collect(),
            else_: else_.map(|e| Box::new(resolve_window_refs(*e, input_width))),
            dtype,
        },
        other => other,
    }
}

/// Rebase relative `AggRef{index}` past the group keys.
fn resolve_agg_refs(expr: BoundExpr, num_keys: usize) -> BoundExpr {
    match expr {
        BoundExpr::AggRef { index, dtype } => BoundExpr::AggRef {
            index: num_keys + index,
            dtype,
        },
        BoundExpr::Binary {
            op,
            left,
            right,
            dtype,
        } => BoundExpr::Binary {
            op,
            left: Box::new(resolve_agg_refs(*left, num_keys)),
            right: Box::new(resolve_agg_refs(*right, num_keys)),
            dtype,
        },
        BoundExpr::Not(e) => BoundExpr::Not(Box::new(resolve_agg_refs(*e, num_keys))),
        BoundExpr::Negate { expr, dtype } => BoundExpr::Negate {
            expr: Box::new(resolve_agg_refs(*expr, num_keys)),
            dtype,
        },
        BoundExpr::IsNull { expr, negated } => BoundExpr::IsNull {
            expr: Box::new(resolve_agg_refs(*expr, num_keys)),
            negated,
        },
        BoundExpr::Scalar { func, args, dtype } => BoundExpr::Scalar {
            func,
            args: args
                .into_iter()
                .map(|a| resolve_agg_refs(a, num_keys))
                .collect(),
            dtype,
        },
        BoundExpr::DynamicScalar { name, args, dtype } => BoundExpr::DynamicScalar {
            name,
            args: args
                .into_vec()
                .into_iter()
                .map(|a| resolve_agg_refs(a, num_keys))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            dtype,
        },
        BoundExpr::InSubquery {
            expr,
            query,
            negated,
        } => BoundExpr::InSubquery {
            expr: Box::new(resolve_agg_refs(*expr, num_keys)),
            query,
            negated,
        },
        BoundExpr::Cast { expr, dtype } => BoundExpr::Cast {
            expr: Box::new(resolve_agg_refs(*expr, num_keys)),
            dtype,
        },
        BoundExpr::Coalesce { args, dtype } => BoundExpr::Coalesce {
            args: args
                .into_iter()
                .map(|a| resolve_agg_refs(a, num_keys))
                .collect(),
            dtype,
        },
        BoundExpr::Case {
            branches,
            else_,
            dtype,
        } => BoundExpr::Case {
            branches: branches
                .into_iter()
                .map(|(c, v)| (resolve_agg_refs(c, num_keys), resolve_agg_refs(v, num_keys)))
                .collect(),
            else_: else_.map(|e| Box::new(resolve_agg_refs(*e, num_keys))),
            dtype,
        },
        BoundExpr::WindowRef { .. } => expr,
        // ScalarSubquery/Exists bodies bind in their own scope.
        other => other,
    }
}

// ---------- statement binding ----------

/// How one join step will execute.
enum BoundJoin {
    /// Hash join on extracted equi pairs (side-relative keys), plus an
    /// optional residual predicate over the joined output (INNER only).
    Hash {
        left_keys: Vec<BoundExpr>,
        right_keys: Vec<BoundExpr>,
        residual: Option<BoundExpr>,
    },
    /// Nested-loop join with the full condition in joined space (`None` =
    /// CROSS JOIN).
    Nested { on: Option<BoundExpr> },
}

/// Split `a AND b AND ...` into conjuncts (mirrors the optimizer's splitter;
/// kept local so the binder need not depend on the optimizer).
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

/// Rebuild `a AND b AND ...` from conjuncts (None when empty).
fn and_all(mut exprs: Vec<BoundExpr>) -> Option<BoundExpr> {
    if exprs.is_empty() {
        return None;
    }
    let mut acc = exprs.remove(0);
    for e in exprs {
        acc = BoundExpr::Binary {
            op: BinaryOp::And,
            left: Box::new(acc),
            right: Box::new(e),
            dtype: DataType::Boolean,
        };
    }
    Some(acc)
}

/// Try to read a conjunct as a crossing equality: one side references only
/// the accumulated left (indices < `left_cols`), the other only the just-added
/// right (indices >= `left_cols`). Returns the pair as side-relative keys.
fn equi_pair(conjunct: &BoundExpr, left_cols: usize) -> Option<(BoundExpr, BoundExpr)> {
    let BoundExpr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
        ..
    } = conjunct
    else {
        return None;
    };
    // Classify an operand as referencing only the accumulated left (indices
    // < `left_cols`), only the just-added right (indices >= `left_cols`), or
    // neither. A MIXED operand is not an equi key: treating it as a right-side
    // key would shift its left-side column indices below zero.
    let side = |e: &BoundExpr| -> Option<bool> {
        let mut cols = Vec::new();
        crate::optimizer::columns_in(e, &mut cols);
        // No columns, or an AggRef/WindowRef (usize::MAX), is not a side key.
        if cols.is_empty() || cols.contains(&usize::MAX) {
            return None;
        }
        let all_left = cols.iter().all(|i| *i < left_cols);
        let all_right = cols.iter().all(|i| *i >= left_cols);
        if all_left {
            Some(true)
        } else if all_right {
            Some(false)
        } else {
            None
        }
    };
    match (side(left), side(right)) {
        (Some(true), Some(false)) => Some((
            (**left).clone(),
            crate::plan::shift_columns((**right).clone(), left_cols),
        )),
        (Some(false), Some(true)) => Some((
            (**right).clone(),
            crate::plan::shift_columns((**left).clone(), left_cols),
        )),
        _ => None,
    }
}

/// Bind a `JOIN ... ON` step: the ON condition binds over the accumulated
/// scope (already pushed into `binder.tables`, right table last) and the
/// result decides hash vs. nested-loop execution. `kind: Cross` never has an
/// `ON` (the parser rejects it) and becomes a condition-less nested loop.
fn bind_join_shape(
    binder: &mut Binder,
    kind: JoinKind,
    left_cols: usize,
    on: Option<&Expr>,
) -> Result<BoundJoin> {
    let Some(on) = on else {
        return Ok(BoundJoin::Nested { on: None });
    };
    // ON binds with table columns only (no aliases, no windows, no
    // aggregates), exactly like WHERE.
    let saved_alias = binder.allow_alias;
    let saved_win = binder.allow_win;
    binder.allow_alias = false;
    binder.allow_win = false;
    let mut aggs = Vec::new();
    let bound = binder.bind_expr(on, &mut aggs, false);
    binder.allow_alias = saved_alias;
    binder.allow_win = saved_win;
    let bound = bound?;
    if !aggs.is_empty() {
        return Err(PinkDbError::Execution(
            "aggregates are not allowed in JOIN ON".to_string(),
        ));
    }
    if bound.dtype() != DataType::Boolean {
        return Err(PinkDbError::Type(
            "JOIN ON needs a BOOLEAN expression".to_string(),
        ));
    }
    if kind == JoinKind::Cross {
        // `CROSS JOIN ... ON` is a syntax error in DuckDB; the grammar can
        // never produce it, but keep the plan honest if it ever does.
        return Err(PinkDbError::Parse(
            "CROSS JOIN does not take an ON condition".to_string(),
        ));
    }

    let mut conjuncts = Vec::new();
    split_conjuncts(bound.clone(), &mut conjuncts);
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    let mut residuals = Vec::new();
    for c in &conjuncts {
        match equi_pair(c, left_cols) {
            Some((l, r)) => {
                comparison_ok(BinaryOp::Eq, l.dtype(), r.dtype()).map_err(|_| {
                    PinkDbError::Type(format!(
                        "JOIN keys have incompatible types: {} and {}",
                        l.dtype(),
                        r.dtype()
                    ))
                })?;
                left_keys.push(l);
                right_keys.push(r);
            }
            None => residuals.push(c.clone()),
        }
    }

    // Pure equi condition (any kind): hash join.
    if residuals.is_empty() && !left_keys.is_empty() {
        return Ok(BoundJoin::Hash {
            left_keys,
            right_keys,
            residual: None,
        });
    }
    // INNER with some equi keys: hash + a residual filter above the join.
    if kind == JoinKind::Inner && !left_keys.is_empty() {
        return Ok(BoundJoin::Hash {
            left_keys,
            right_keys,
            residual: and_all(residuals),
        });
    }
    // Everything else (outer joins with residual conditions, non-equi
    // conditions, constant conditions): one nested loop over the full ON.
    Ok(BoundJoin::Nested { on: Some(bound) })
}
/// Total width of the scan scope (0 with no FROM).
fn scan_width(binder: &Binder<'_>) -> usize {
    binder.tables.iter().map(|t| t.columns.len()).sum()
}

/// Bind one statement against `db`, with `default_db` as the session's
/// current database for unqualified names. The returned plan borrows nothing
/// from `db`.
pub fn bind(db: &PinkDb, stmt: &Statement, default_db: &str) -> Result<LogicalPlan> {
    Ok(bind_tracked(db, stmt, default_db)?.0)
}

/// [`bind`] plus the dynamic-function names (lowercased registry keys) the
/// statement's expressions reference, in first-use order — collected by the
/// binder (subquery bodies included) so the WAL layer can map them to
/// extension paths and prefix a logged record with the `LOAD EXTENSION`
/// statements its replay needs.
pub(crate) fn bind_tracked(
    db: &PinkDb,
    stmt: &Statement,
    default_db: &str,
) -> Result<(LogicalPlan, Vec<String>)> {
    bind_tracked_with(db, stmt, default_db, None)
}

/// [`bind_tracked`] for a `?`-parameterized statement: `params` carries one
/// bound value per placeholder, and each `?` binds (and type-infers) against
/// its slot. A plain statement passes `None`, where any `?` is a clear error.
///
/// On top of [`bind_tracked_plan`] this applies the WAL-divergence guard: a
/// statement whose SQL text the engine logs (see
/// [`crate::plan::writes_tables`] — INSERT/UPDATE/DELETE, DDL, `CALL`, ...)
/// may not call a volatile (nondeterministic) extension function. Replay
/// re-executes the logged text, and a volatile callback could produce a
/// different value there; refusing the statement before it runs is what makes
/// divergence impossible rather than merely unlikely. Read-only statements
/// never reach the WAL and are unaffected, and the guard is applied uniformly
/// whether or not this store currently has a WAL attached, so the same SQL
/// behaves identically in tests and on a durable store.
pub(crate) fn bind_tracked_with(
    db: &PinkDb,
    stmt: &Statement,
    default_db: &str,
    params: Option<ParamSlots>,
) -> Result<(LogicalPlan, Vec<String>)> {
    let (plan, dynamic) = bind_tracked_plan(db, stmt, default_db, params)?;
    if crate::plan::writes_tables(&plan)
        && let Some(function) = db.first_volatile_dynamic_function(&dynamic)
    {
        return Err(PinkDbError::Execution(format!(
            "extension function {function}() is volatile (nondeterministic) and cannot be called \
             in a statement the engine WAL-logs: replaying it could produce a different result. \
             Use it in a read-only statement, or declare the function deterministic"
        )));
    }
    Ok((plan, dynamic))
}

/// [`bind_tracked_with`]'s plan construction, without the volatility guard.
fn bind_tracked_plan(
    db: &PinkDb,
    stmt: &Statement,
    default_db: &str,
    params: Option<ParamSlots>,
) -> Result<(LogicalPlan, Vec<String>)> {
    let empty = |plan| Ok((plan, Vec::new()));
    match stmt {
        Statement::CreateTable {
            db: qdb,
            name,
            items,
            if_not_exists,
        } => bind_create_table(db, default_db, qdb.as_deref(), name, items, *if_not_exists),
        Statement::CreateTableAs {
            db: qdb,
            name,
            query,
            if_not_exists,
        } => bind_create_table_as(
            db,
            default_db,
            qdb,
            name,
            query,
            *if_not_exists,
            params.clone(),
        ),
        Statement::Insert {
            db: qdb,
            table,
            columns,
            source,
        } => match source {
            InsertSource::Values(rows) => {
                bind_insert(db, default_db, qdb, table, columns, rows, params.clone())
            }
            InsertSource::Select(query) => {
                bind_insert_select(db, default_db, qdb, table, columns, query, params.clone())
            }
        },
        Statement::Update {
            db: qdb,
            table,
            sets,
            where_clause,
        } => bind_update(
            db,
            default_db,
            qdb,
            table,
            sets,
            where_clause,
            params.clone(),
        ),
        Statement::Delete {
            db: qdb,
            table,
            where_clause,
        } => bind_delete(db, default_db, qdb, table, where_clause, params.clone()),
        Statement::CopyTo {
            db: qdb,
            table,
            path,
            format,
        } => bind_copy_to(db, default_db, qdb, table, path, format.as_deref()),
        Statement::Begin => empty(LogicalPlan::Begin),
        Statement::Commit => empty(LogicalPlan::Commit),
        Statement::Rollback => empty(LogicalPlan::Rollback),
        Statement::CreateView {
            db: qdb,
            name,
            cols,
            query,
        } => bind_create_view(db, default_db, qdb, name, cols, query),
        Statement::CreateMaterializedView {
            db: qdb,
            name,
            query,
            if_not_exists,
            policy,
        } => {
            let target = Binder::new(db, default_db).resolve_db(qdb)?;
            // Bind the inner query.
            let mut binder = Binder::new(db, default_db).with_params(params.clone());
            let plan = bind_select_in(db, &mut binder, query)?;
            let dynamic = binder.dynamic_functions_used();
            Ok((
                LogicalPlan::CreateMaterializedView {
                    db: Some(target),
                    name: name.clone(),
                    query: Box::new(plan),
                    if_not_exists: *if_not_exists,
                    policy: *policy,
                    // The statement's source text is attached by the runner
                    // (`plan::with_mv_source`): the binder never sees it.
                    source: String::new(),
                },
                dynamic,
            ))
        }
        Statement::RefreshMaterializedView { db: qdb, name } => {
            empty(LogicalPlan::RefreshMaterializedView {
                db: Some(Binder::new(db, default_db).resolve_db(qdb)?),
                name: name.clone(),
            })
        }
        Statement::RefreshMaterializedViews => empty(LogicalPlan::RefreshMaterializedViews {
            db: Some(Binder::new(db, default_db).resolve_db(&None)?),
        }),
        Statement::ShowMaterializedViews => empty(LogicalPlan::ShowMaterializedViews {
            db: Some(default_db.to_ascii_lowercase()),
        }),
        Statement::DropMaterializedView {
            db: qdb,
            name,
            if_exists,
        } => empty(LogicalPlan::DropMaterializedView {
            db: Some(Binder::new(db, default_db).resolve_db(qdb)?),
            name: name.clone(),
            if_exists: *if_exists,
        }),
        Statement::DropTable {
            db: qdb,
            name,
            if_exists,
        } => empty(LogicalPlan::DropTable {
            db: Some(Binder::new(db, default_db).resolve_db(qdb)?),
            name: name.clone(),
            if_exists: *if_exists,
        }),
        Statement::CreateIndex {
            db: qdb,
            name,
            table,
            column,
            if_not_exists,
        } => {
            // Validate table + column exist.
            let target_db = Binder::new(db, default_db).resolve_db(qdb)?;
            let col_idx = if let Ok(t) = db.table_in(&target_db, table) {
                t.columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(column))
                    .ok_or_else(|| {
                        PinkDbError::Catalog(format!(
                            "column \"{column}\" not found in table \"{table}\""
                        ))
                    })?
            } else {
                if !*if_not_exists {
                    return Err(PinkDbError::Catalog(format!("table \"{table}\" not found")));
                }
                0
            };
            empty(LogicalPlan::CreateIndex {
                db: Some(target_db),
                name: name.clone(),
                table: table.clone(),
                column: col_idx,
                if_not_exists: *if_not_exists,
            })
        }
        Statement::DropIndex {
            db: qdb,
            name,
            if_exists,
        } => empty(LogicalPlan::DropIndex {
            db: Some(Binder::new(db, default_db).resolve_db(qdb)?),
            name: name.clone(),
            if_exists: *if_exists,
        }),
        Statement::AlterTable { db: qdb, name, op } => {
            bind_alter_table(db, default_db, qdb.as_deref(), name, op, params.clone())
        }
        Statement::DropView { db: qdb, name } => empty(LogicalPlan::DropView {
            db: Some(Binder::new(db, default_db).resolve_db(qdb)?),
            name: name.clone(),
        }),
        Statement::CreateDatabase {
            name,
            if_not_exists,
        } => empty(LogicalPlan::CreateDatabase {
            name: name.clone(),
            if_not_exists: *if_not_exists,
        }),
        Statement::DropDatabase { name, if_exists } => empty(LogicalPlan::DropDatabase {
            name: name.clone(),
            if_exists: *if_exists,
        }),
        Statement::ShowTables => empty(LogicalPlan::ShowTables {
            db: default_db.to_ascii_lowercase(),
        }),
        Statement::ShowDatabases => empty(LogicalPlan::ShowDatabases),
        Statement::Describe { db: qdb, name } => {
            bind_describe(db, default_db, qdb.as_deref(), name)
        }
        Statement::Explain(query) => {
            // Bind the inner query fully (errors surface here) and wrap it —
            // EXPLAIN never executes it.
            let mut binder = Binder::new(db, default_db).with_params(params.clone());
            let plan = bind_select_in(db, &mut binder, query)?;
            let dynamic = binder.dynamic_functions_used();
            Ok((
                LogicalPlan::Explain {
                    plan: Box::new(plan),
                },
                dynamic,
            ))
        }
        Statement::Use { name } => {
            // Validate at bind: USE must fail clearly before touching the
            // session (and before any later statement in the same string).
            if !db.database_exists(name) {
                return Err(PinkDbError::Catalog(format!(
                    "database \"{name}\" not found"
                )));
            }
            empty(LogicalPlan::Use { name: name.clone() })
        }
        Statement::LoadExtension { path } => {
            // Existence/success is checked at execution (loading has side
            // effects); bind only carries the literal path through.
            empty(LogicalPlan::LoadExtension { path: path.clone() })
        }
        Statement::Checkpoint => empty(LogicalPlan::Checkpoint),
        Statement::CreateProcedure {
            db: qdb,
            name,
            params: sig,
            language,
            body,
            or_replace,
        } => bind_create_procedure(
            db,
            default_db,
            qdb.as_deref(),
            CreateProcedureArgs {
                name,
                sig,
                language: *language,
                body,
                or_replace: *or_replace,
            },
        ),
        Statement::DropProcedure {
            db: qdb,
            name,
            if_exists,
        } => empty(LogicalPlan::DropProcedure {
            db: Some(Binder::new(db, default_db).resolve_db(qdb)?),
            name: name.clone(),
            if_exists: *if_exists,
        }),
        Statement::Call {
            db: qdb,
            name,
            args,
        } => bind_call(db, default_db, qdb.as_deref(), name, args, params.clone()),
        Statement::CreateUser {
            name,
            password,
            if_not_exists,
        } => bind_create_user(db, name, password, *if_not_exists),
        Statement::AlterUser { name, password } => bind_alter_user(db, name, password),
        Statement::DropUser { name, if_exists } => bind_drop_user(db, name, *if_exists),
        Statement::CreateRole {
            name,
            if_not_exists,
        } => bind_create_role(db, name, *if_not_exists),
        Statement::DropRole { name, if_exists } => bind_drop_role(db, name, *if_exists),
        Statement::Grant { target } => bind_grant(db, default_db, target, false),
        Statement::Revoke { target } => bind_grant(db, default_db, target, true),
        Statement::Select(sel) => bind_select(db, default_db, sel, params.clone()),
    }
}

/// The parsed `CREATE [OR REPLACE] PROCEDURE` header the binder validates (a
/// struct so the helper stays under the argument-count lint).
struct CreateProcedureArgs<'a> {
    name: &'a str,
    sig: &'a [(String, DataType)],
    language: crate::procedure::ProcLanguage,
    body: &'a str,
    or_replace: bool,
}

/// Bind `CREATE [OR REPLACE] PROCEDURE` (wave 44): validate the signature and
/// parse the body into the catalog definition the executor stores.
///
/// The body's statements are **not bound here**: each rebinds against the
/// current catalog when the call runs (so a procedure may create a table and
/// then insert into it), and its variables have no values yet. What is
/// checked here is everything a definition must satisfy forever: the
/// language, a duplicate-free signature, and the body's *shape* (the
/// procedural parser's terminators, declared `SET` targets and the
/// store-level statements a body may not contain).
fn bind_create_procedure(
    db: &PinkDb,
    default_db: &str,
    qdb: Option<&str>,
    args: CreateProcedureArgs<'_>,
) -> Result<(LogicalPlan, Vec<String>)> {
    let CreateProcedureArgs {
        name,
        sig,
        language,
        body,
        or_replace,
    } = args;
    let target = Binder::new(db, default_db).resolve_db(&qdb.map(str::to_string))?;
    crate::procedure::check_language_available(language)?;
    // A duplicate parameter name would make the scope ambiguous.
    for (i, (n, _)) in sig.iter().enumerate() {
        if sig[..i].iter().any(|(m, _)| m.eq_ignore_ascii_case(n)) {
            return Err(PinkDbError::Catalog(format!(
                "procedure \"{name}\": parameter \"{n}\" is declared twice"
            )));
        }
    }
    let param_names: Vec<String> = sig.iter().map(|(n, _)| n.clone()).collect();
    let parsed = match language {
        crate::procedure::ProcLanguage::Sql => {
            Some(crate::procedure::parse_body(name, &param_names, body)?)
        }
        // A JavaScript body is opaque text for the sandbox to evaluate.
        crate::procedure::ProcLanguage::JavaScript => None,
    };
    Ok((
        LogicalPlan::CreateProcedure {
            db: Some(target),
            name: name.to_string(),
            def: crate::procedure::ProcedureDef {
                name: name.to_string(),
                params: sig.to_vec(),
                language,
                source: body.to_string(),
                body: parsed,
            },
            or_replace,
        },
        Vec::new(),
    ))
}

/// Bind `CALL name(args...)`: resolve the procedure, check arity, and bind
/// every argument as a constant expression (no table scope). Arguments of a
/// call that a durable store will log must be deterministic: a
/// loadable-extension callback is refused here, exactly like one inside a
/// body.
fn bind_call(
    db: &PinkDb,
    default_db: &str,
    qdb: Option<&str>,
    name: &str,
    args: &[Expr],
    params: Option<ParamSlots>,
) -> Result<(LogicalPlan, Vec<String>)> {
    let target = Binder::new(db, default_db).resolve_db(&qdb.map(str::to_string))?;
    let def = db.procedure_in(&target, name)?;
    if args.len() != def.params.len() {
        return Err(PinkDbError::Catalog(format!(
            "procedure \"{}\" takes {} argument(s), but {} were given",
            def.name,
            def.params.len(),
            args.len()
        )));
    }
    let mut bound = Vec::with_capacity(args.len());
    for (i, arg) in args.iter().enumerate() {
        let value = bind_constant_expr(db, &target, arg, params.clone())?;
        if crate::plan::bound_expr_has_dynamic_scalar(&value) {
            return Err(PinkDbError::Execution(format!(
                "CALL argument {} calls a loadable-extension function; a durable store replays \
                 a committed CALL statement, and an extension callback is opaque (impure or \
                 nondeterministic) — evaluate the value into a table or a literal first",
                i + 1
            )));
        }
        bound.push(value);
    }
    Ok((
        LogicalPlan::Call {
            db: Some(target),
            name: def.name.clone(),
            args: bound,
        },
        Vec::new(),
    ))
}

/// A plan that references no dynamic (extension) function: the shape every
/// RBAC catalog statement returns (its definitions are plain SQL text).
fn no_dynamic(plan: LogicalPlan) -> Result<(LogicalPlan, Vec<String>)> {
    Ok((plan, Vec::new()))
}

/// Bind `CREATE USER [IF NOT EXISTS] name PASSWORD …` (wave 48): refuse a
/// name already taken by a user or role, hash the plaintext (or validate a
/// supplied PHC hash), and carry the hash into the plan — the plaintext never
/// goes further than this call.
fn bind_create_user(
    db: &PinkDb,
    name: &str,
    password: &crate::rbac::PasswordSource,
    if_not_exists: bool,
) -> Result<(LogicalPlan, Vec<String>)> {
    let key = name.to_ascii_lowercase();
    if db.rbac().roles.contains_key(&key) {
        return Err(PinkDbError::Catalog(format!(
            "a role named \"{name}\" already exists (pick another name)"
        )));
    }
    if db.rbac().users.contains_key(&key) && !if_not_exists {
        return Err(PinkDbError::Catalog(format!(
            "user \"{name}\" already exists (use ALTER USER … PASSWORD to change its password)"
        )));
    }
    let password_hash = checked_password_hash(password)?;
    no_dynamic(LogicalPlan::CreateUser {
        name: name.to_string(),
        password_hash,
        if_not_exists,
    })
}

/// The PHC hash of a `PASSWORD`/`PASSWORD_HASH` clause. A supplied hash must
/// be a valid Argon2id PHC string; a plaintext is refused when empty (the
/// hash of the empty string is valid, so the check happens before hashing).
fn checked_password_hash(password: &crate::rbac::PasswordSource) -> Result<String> {
    if let Some(hash) = password.prehashed_hash() {
        crate::rbac::validate_password_hash(hash)?;
        return Ok(hash.to_string());
    }
    if password.is_empty_plaintext() {
        return Err(PinkDbError::Catalog(
            "a password must not be empty".to_string(),
        ));
    }
    Ok(password.hashed()?.to_string())
}

/// Bind `ALTER USER name PASSWORD …`: the user must exist.
fn bind_alter_user(
    db: &PinkDb,
    name: &str,
    password: &crate::rbac::PasswordSource,
) -> Result<(LogicalPlan, Vec<String>)> {
    let key = name.to_ascii_lowercase();
    if db.rbac().roles.contains_key(&key) {
        return Err(PinkDbError::Catalog(format!(
            "\"{name}\" is a role, not a user (roles have no password)"
        )));
    }
    if !db.rbac().users.contains_key(&key) {
        return Err(PinkDbError::Catalog(format!("user \"{name}\" not found")));
    }
    let password_hash = checked_password_hash(password)?;
    no_dynamic(LogicalPlan::AlterUser {
        name: name.to_string(),
        password_hash,
    })
}

/// Bind `DROP USER [IF EXISTS] name`.
fn bind_drop_user(db: &PinkDb, name: &str, if_exists: bool) -> Result<(LogicalPlan, Vec<String>)> {
    let key = name.to_ascii_lowercase();
    if db.rbac().roles.contains_key(&key) {
        return Err(PinkDbError::Catalog(format!(
            "\"{name}\" is a role, not a user (use DROP ROLE)"
        )));
    }
    if !db.rbac().users.contains_key(&key) && !if_exists {
        return Err(PinkDbError::Catalog(format!("user \"{name}\" not found")));
    }
    no_dynamic(LogicalPlan::DropUser {
        name: name.to_string(),
        if_exists,
    })
}

/// Bind `CREATE ROLE [IF NOT EXISTS] name`.
fn bind_create_role(
    db: &PinkDb,
    name: &str,
    if_not_exists: bool,
) -> Result<(LogicalPlan, Vec<String>)> {
    let key = name.to_ascii_lowercase();
    if db.rbac().users.contains_key(&key) {
        return Err(PinkDbError::Catalog(format!(
            "a user named \"{name}\" already exists (pick another name)"
        )));
    }
    if db.rbac().roles.contains_key(&key) && !if_not_exists {
        return Err(PinkDbError::Catalog(format!(
            "role \"{name}\" already exists"
        )));
    }
    no_dynamic(LogicalPlan::CreateRole {
        name: name.to_string(),
        if_not_exists,
    })
}

/// Bind `DROP ROLE [IF EXISTS] name`.
fn bind_drop_role(db: &PinkDb, name: &str, if_exists: bool) -> Result<(LogicalPlan, Vec<String>)> {
    let key = name.to_ascii_lowercase();
    if db.rbac().users.contains_key(&key) {
        return Err(PinkDbError::Catalog(format!(
            "\"{name}\" is a user, not a role (use DROP USER)"
        )));
    }
    if !db.rbac().roles.contains_key(&key) && !if_exists {
        return Err(PinkDbError::Catalog(format!("role \"{name}\" not found")));
    }
    no_dynamic(LogicalPlan::DropRole {
        name: name.to_string(),
        if_exists,
    })
}

/// Bind `GRANT`/`REVOKE` (wave 48): resolve and validate the principal, the
/// object, the privilege/scope combination and membership cycles, and build
/// the display form the result reports. The executor re-validates existence
/// under the store lock: bind and execution are separate lock acquisitions on
/// the multi-writer paths.
fn bind_grant(
    db: &PinkDb,
    default_db: &str,
    target: &crate::rbac::GrantTarget,
    revoke: bool,
) -> Result<(LogicalPlan, Vec<String>)> {
    use crate::rbac::{GrantObject, GrantSpec, GrantTarget};
    let rbac = db.rbac();
    let resolve = |name: &str| -> Result<String> {
        let key = name.to_ascii_lowercase();
        if !rbac.principal_exists(&key) {
            return Err(PinkDbError::Catalog(format!(
                "user or role \"{name}\" not found (create it first)"
            )));
        }
        Ok(key)
    };
    let spec = match target {
        GrantTarget::Admin { principal } => {
            let key = resolve(principal)?;
            GrantSpec {
                privs: Vec::new(),
                object: GrantObject::Admin,
                description: format!("ADMIN TO {}", rbac.display_of(&key)),
                principal: key,
            }
        }
        GrantTarget::Role { role, principal } => {
            let role_key = role.to_ascii_lowercase();
            if rbac.users.contains_key(&role_key) {
                return Err(PinkDbError::Catalog(format!(
                    "\"{role}\" is a user, not a role"
                )));
            }
            if !rbac.roles.contains_key(&role_key) {
                return Err(PinkDbError::Catalog(format!("role \"{role}\" not found")));
            }
            let key = resolve(principal)?;
            if key == role_key {
                return Err(PinkDbError::Catalog(format!(
                    "a principal cannot be a member of itself (\"{role}\")"
                )));
            }
            if rbac.roles_of(&role_key).contains(&key) {
                return Err(PinkDbError::Catalog(format!(
                    "granting role \"{}\" to \"{}\" would create a membership cycle",
                    rbac.display_of(&role_key),
                    rbac.display_of(&key)
                )));
            }
            GrantSpec {
                privs: Vec::new(),
                object: GrantObject::Role {
                    role: role_key.clone(),
                },
                description: format!(
                    "ROLE {} TO {}",
                    rbac.display_of(&role_key),
                    rbac.display_of(&key)
                ),
                principal: key,
            }
        }
        GrantTarget::Table {
            privs,
            db: qdb,
            table,
            principal,
        } => {
            let target_db = Binder::new(db, default_db).resolve_db(qdb)?;
            let table_key = table.to_ascii_lowercase();
            let table_display = db
                .table_in(&target_db, table)
                .map(|t| t.name.clone())
                .map_err(|_| {
                    PinkDbError::Catalog(format!(
                        "table \"{table}\" not found in database \"{target_db}\""
                    ))
                })?;
            let expanded = expand_table_privs(privs)?;
            let principal = resolve(principal)?;
            let db_display = db.database_display_name(&target_db);
            GrantSpec {
                privs: expanded.clone(),
                object: GrantObject::Table {
                    db: target_db,
                    table: table_key,
                },
                description: format!(
                    "{} ON TABLE {db_display}.{table_display} TO {}",
                    crate::rbac::Privilege::list(&expanded),
                    rbac.display_of(&principal)
                ),
                principal,
            }
        }
        GrantTarget::Database {
            privs,
            db: qdb,
            principal,
        } => {
            let target_db = Binder::new(db, default_db).resolve_db(&Some(qdb.clone()))?;
            let expanded = expand_database_privs(privs)?;
            let principal = resolve(principal)?;
            let db_display = db.database_display_name(&target_db);
            GrantSpec {
                privs: expanded.clone(),
                object: GrantObject::Database {
                    db: target_db.clone(),
                },
                description: format!(
                    "{} ON DATABASE {db_display} TO {}",
                    crate::rbac::Privilege::list(&expanded),
                    rbac.display_of(&principal)
                ),
                principal,
            }
        }
    };
    let plan = if revoke {
        LogicalPlan::Revoke { spec }
    } else {
        LogicalPlan::Grant { spec }
    };
    no_dynamic(plan)
}

/// Expand and validate a `GRANT … ON TABLE` privilege list: `ALL` means the
/// four table privileges, and a database privilege (`CREATE`) is refused with
/// the statement form that does accept it.
fn expand_table_privs(privs: &crate::rbac::GrantPrivs) -> Result<Vec<crate::rbac::Privilege>> {
    use crate::rbac::Privilege;
    let expanded = privs.expand_table();
    if let Some(p) = expanded.iter().find(|p| !Privilege::TABLE.contains(p)) {
        return Err(PinkDbError::Catalog(format!(
            "{} is a database privilege: GRANT {} ON DATABASE <db> TO <principal>",
            p.keyword(),
            p.keyword()
        )));
    }
    let mut expanded = expanded;
    expanded.sort();
    expanded.dedup();
    Ok(expanded)
}

/// Expand and validate a `GRANT … ON DATABASE` privilege list: `ALL` means
/// `CREATE`, and a table privilege is refused with the statement form that
/// accepts it.
fn expand_database_privs(privs: &crate::rbac::GrantPrivs) -> Result<Vec<crate::rbac::Privilege>> {
    use crate::rbac::Privilege;
    let expanded = privs.expand_database();
    if let Some(p) = expanded.iter().find(|p| !Privilege::DATABASE.contains(p)) {
        return Err(PinkDbError::Catalog(format!(
            "{} is a table privilege: GRANT {} ON TABLE <table> TO <principal>",
            p.keyword(),
            p.keyword()
        )));
    }
    let mut expanded = expanded;
    expanded.sort();
    expanded.dedup();
    Ok(expanded)
}

/// Bind one expression as a constant (an empty table scope) with optional
/// `?` slots / procedure variables visible — the shared path for `CALL`
/// arguments, `ALTER TABLE … DEFAULT`, and every expression a procedure body
/// evaluates outside a statement (conditions, assignments, `RETURN`,
/// `RAISE`, `DECLARE … DEFAULT`).
pub(crate) fn bind_constant_expr(
    db: &PinkDb,
    default_db: &str,
    expr: &Expr,
    params: Option<ParamSlots>,
) -> Result<BoundExpr> {
    Ok(bind_constant_expr_tracked(db, default_db, expr, params)?.0)
}

/// [`bind_constant_expr`] plus the dynamic-function names (lowercased
/// registry keys) the expression references, in first-use order — the same
/// list the statement-level binders return.
///
/// A caller that logs its whole unit anyway (the procedure executor: a `CALL`
/// is WAL-logged as one record, and its replay re-evaluates this expression)
/// applies the volatility guard to expressions bound outside any statement.
pub(crate) fn bind_constant_expr_tracked(
    db: &PinkDb,
    default_db: &str,
    expr: &Expr,
    params: Option<ParamSlots>,
) -> Result<(BoundExpr, Vec<String>)> {
    let mut binder = Binder::new(db, default_db).with_params(params);
    binder.allow_win = false;
    binder.allow_alias = false;
    let mut aggs = Vec::new();
    let bound = binder.bind_expr(expr, &mut aggs, false)?;
    if !aggs.is_empty() {
        return Err(PinkDbError::Execution(
            "an aggregate is not allowed here (no table scope)".to_string(),
        ));
    }
    Ok((bound, binder.dynamic_functions_used()))
}

/// Bind CREATE VIEW: validate the query now (errors surface at creation),
/// store the AST for per-use rebinding. The view lives in the qualified
/// database (or the session's), and its body resolves unqualified names there.
fn bind_create_view(
    db: &PinkDb,
    default_db: &str,
    qdb: &Option<String>,
    name: &str,
    cols: &Option<Vec<String>>,
    query: &Select,
) -> Result<(LogicalPlan, Vec<String>)> {
    let view_db = Binder::new(db, default_db).resolve_db(qdb)?;
    let mut binder = Binder::new(db, &view_db);
    let bound = bind_select_in(db, &mut binder, query)?;
    let schema = plan_schema(db, &view_db, &binder.ctes, &bound)?;
    if let Some(cols) = cols
        && cols.len() != schema.len()
    {
        return Err(PinkDbError::Execution(format!(
            "view \"{name}\" has {} columns but {} aliases",
            schema.len(),
            cols.len()
        )));
    }
    let dynamic_functions = binder.dynamic_functions_used();
    Ok((
        LogicalPlan::CreateView {
            db: Some(view_db),
            name: name.to_string(),
            cols: cols.clone(),
            query: Box::new(query.clone()),
        },
        dynamic_functions,
    ))
}

/// Bind UPDATE: resolve target columns, bind SET expressions in the table
/// scope (no aggregates, windows, or aliases), check Boolean WHERE. A bare
/// `?` SET value takes the target column's type.
fn bind_update(
    db: &PinkDb,
    default_db: &str,
    qdb: &Option<String>,
    table: &str,
    sets: &[(String, Expr)],
    where_clause: &Option<Expr>,
    params: Option<ParamSlots>,
) -> Result<(LogicalPlan, Vec<String>)> {
    let table_db = Binder::new(db, default_db).resolve_db(qdb)?;
    if let Some(err) = db.attached_read_only(&table_db, table) {
        return Err(err);
    }
    let t = db.table_in(&table_db, table)?;
    if sets.is_empty() {
        return Err(PinkDbError::Parse(
            "UPDATE needs at least one SET assignment".to_string(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for (col, _) in sets {
        if !seen.insert(col.to_ascii_lowercase()) {
            return Err(PinkDbError::Catalog(format!(
                "duplicate SET column \"{col}\""
            )));
        }
    }
    let mut binder = Binder::new(db, &table_db).with_params(params);
    binder.allow_win = false;
    binder.tables.push(ScopeTable {
        name: t.name.to_ascii_lowercase(),
        columns: t.schema(),
    });
    let mut bound_sets = Vec::with_capacity(sets.len());
    for (col, expr) in sets {
        let idx = t
            .column_index(col)
            .map_err(|_| PinkDbError::Catalog(format!("no column \"{col}\"")))?;
        let (_, target) = t.schema()[idx].clone();
        // `UPDATE t SET col = ?`: the parameter takes the column's type.
        binder.pin_parameter(expr, target)?;
        let mut aggs = Vec::new();
        let bound = binder.bind_expr(expr, &mut aggs, false)?;
        if !aggs.is_empty() {
            return Err(PinkDbError::Execution(
                "aggregates are not allowed in UPDATE SET".to_string(),
            ));
        }
        // Subqueries are fine (uncorrelated; evaluated pre-mutation).
        if !DataTable::can_coerce(bound.dtype(), target)
            && !matches!(bound, BoundExpr::Literal(Value::Null))
        {
            return Err(PinkDbError::Type(format!(
                "cannot assign {} to {target} column \"{col}\"",
                bound.dtype()
            )));
        }
        bound_sets.push((idx, bound));
    }
    binder.allow_alias = false;
    let mut no_aggs = Vec::new();
    let predicate = where_clause
        .as_ref()
        .map(|e| binder.bind_expr(e, &mut no_aggs, false))
        .transpose()?;
    if let Some(p) = &predicate
        && p.dtype() != DataType::Boolean
    {
        return Err(PinkDbError::Type(
            "WHERE needs a BOOLEAN expression".to_string(),
        ));
    }
    reject_view_dependency(db, &binder.views_used())?;
    let dynamic_functions = binder.dynamic_functions_used();
    Ok((
        LogicalPlan::Update {
            db: Some(table_db),
            table: t.name.clone(),
            sets: bound_sets,
            predicate,
        },
        dynamic_functions,
    ))
}

/// Bind DELETE: Boolean WHERE over the table scope (no aggregates/windows).
fn bind_delete(
    db: &PinkDb,
    default_db: &str,
    qdb: &Option<String>,
    table: &str,
    where_clause: &Option<Expr>,
    params: Option<ParamSlots>,
) -> Result<(LogicalPlan, Vec<String>)> {
    let table_db = Binder::new(db, default_db).resolve_db(qdb)?;
    if let Some(err) = db.attached_read_only(&table_db, table) {
        return Err(err);
    }
    let t = db.table_in(&table_db, table)?;
    let mut binder = Binder::new(db, &table_db).with_params(params);
    binder.allow_win = false;
    binder.allow_alias = false;
    binder.tables.push(ScopeTable {
        name: t.name.to_ascii_lowercase(),
        columns: t.schema(),
    });
    let mut no_aggs = Vec::new();
    let predicate = where_clause
        .as_ref()
        .map(|e| binder.bind_expr(e, &mut no_aggs, false))
        .transpose()?;
    if let Some(p) = &predicate
        && p.dtype() != DataType::Boolean
    {
        return Err(PinkDbError::Type(
            "WHERE needs a BOOLEAN expression".to_string(),
        ));
    }
    reject_view_dependency(db, &binder.views_used())?;
    let dynamic_functions = binder.dynamic_functions_used();
    Ok((
        LogicalPlan::Delete {
            db: Some(table_db),
            table: t.name.clone(),
            predicate,
        },
        dynamic_functions,
    ))
}

/// Bind `COPY <table> TO '<path>' [(FORMAT <name>)]` (wave 71): resolve the
/// source table and the target file format at bind time, so a missing table
/// or an unregistered format is a clean plan-time error (never a partial
/// file written by a half-bound statement).
///
/// The source must be a catalog table of the store: views are not tables and
/// attached lake tables live outside the store, so both are refused with a
/// message saying what to do instead.
fn bind_copy_to(
    db: &PinkDb,
    default_db: &str,
    qdb: &Option<String>,
    table: &str,
    path: &str,
    format: Option<&str>,
) -> Result<(LogicalPlan, Vec<String>)> {
    let table_db = Binder::new(db, default_db).resolve_db(qdb)?;
    let t = db.table_in(&table_db, table).map_err(|_| {
        PinkDbError::Catalog(format!(
            "COPY source table \"{table}\" does not exist in database \"{table_db}\" \
             (COPY writes plain catalog tables; views and attached lake tables are \
             not supported — materialize the query into a table first)"
        ))
    })?;
    let fmt = match format {
        Some(name) => db.file_format(name).ok_or_else(|| {
            PinkDbError::Catalog(format!(
                "no file format \"{name}\" is registered (registered formats: {}); \
                 a build with the matching first-party extension (e.g. `arrow`) \
                 registers it",
                db.file_format_names()
            ))
        })?,
        None => db.file_format_for_path(path)?,
    };
    Ok((
        LogicalPlan::CopyTo {
            db: Some(table_db),
            table: t.name.clone(),
            format: fmt.name().to_string(),
            path: path.to_string(),
        },
        Vec::new(),
    ))
}

fn bind_insert(
    db: &PinkDb,
    default_db: &str,
    qdb: &Option<String>,
    table: &str,
    columns: &Option<Vec<String>>,
    rows: &[Vec<Expr>],
    params: Option<ParamSlots>,
) -> Result<(LogicalPlan, Vec<String>)> {
    let table_db = Binder::new(db, default_db).resolve_db(qdb)?;
    if let Some(err) = db.attached_read_only(&table_db, table) {
        return Err(err);
    }
    let t = db.table_in(&table_db, table)?;
    let schema = t.schema();
    let order: Vec<usize> = match columns {
        Some(cols) => cols
            .iter()
            .map(|c| {
                t.column_index(c)
                    .map_err(|_| PinkDbError::Catalog(format!("no column \"{c}\"")))
            })
            .collect::<Result<_>>()?,
        None => (0..schema.len()).collect(),
    };
    if let Some(cols) = columns {
        let mut seen = std::collections::HashSet::new();
        for c in cols {
            if !seen.insert(c.to_ascii_lowercase()) {
                return Err(PinkDbError::Catalog(format!(
                    "duplicate INSERT column \"{c}\""
                )));
            }
        }
    }
    // Cells bind in an empty scope: only constants (literals, constant scalar
    // expressions) are accepted; column refs and aggregates fail here.
    let mut bound_rows = Vec::with_capacity(rows.len());
    let mut views_used: Vec<String> = Vec::new();
    let mut dynamic_functions: Vec<String> = Vec::new();
    for row in rows {
        if row.len() != order.len() {
            return Err(PinkDbError::Execution(format!(
                "INSERT has {} values for {} columns",
                row.len(),
                order.len()
            )));
        }
        let mut bound = vec![BoundExpr::Literal(Value::Null); schema.len()];
        // Fill unspecified columns with NULL — or the column's DEFAULT when it
        // has one (`ALTER TABLE … ADD COLUMN … DEFAULT`, wave 65); only when a
        // column list is given.
        for (i, cell) in bound.iter_mut().enumerate() {
            if !order.contains(&i)
                && let Some(d) = &t.columns[i].default
            {
                *cell = BoundExpr::Literal(d.clone());
            }
        }
        for (pos, cell) in row.iter().enumerate() {
            let mut binder = Binder::new(db, &table_db).with_params(params.clone());
            binder.allow_win = false;
            // `INSERT INTO t [(cols)] VALUES (?...)`: a bare `?` cell takes the
            // target column's type; the value converts to it below.
            binder.pin_parameter(cell, schema[order[pos]].1)?;
            let mut aggs = Vec::new();
            let e = binder.bind_expr(cell, &mut aggs, false)?;
            for v in binder.views_used() {
                if !views_used.iter().any(|n| n.eq_ignore_ascii_case(&v)) {
                    views_used.push(v);
                }
            }
            for f in binder.dynamic_functions_used() {
                if !dynamic_functions.iter().any(|n| n == &f) {
                    dynamic_functions.push(f);
                }
            }
            bound[order[pos]] = e;
        }
        bound_rows.push(bound);
    }
    reject_view_dependency(db, &views_used)?;
    Ok((
        LogicalPlan::Insert {
            db: Some(table_db),
            table: t.name.clone(),
            rows: bound_rows,
        },
        dynamic_functions,
    ))
}

/// Bind `INSERT INTO t [(cols)] SELECT ...`: resolve the target column
/// indices, bind the query (its own scope), and check the arity and
/// coercibility of every projected column against the target schema.
fn bind_insert_select(
    db: &PinkDb,
    default_db: &str,
    qdb: &Option<String>,
    table: &str,
    columns: &Option<Vec<String>>,
    query: &Select,
    params: Option<ParamSlots>,
) -> Result<(LogicalPlan, Vec<String>)> {
    let table_db = Binder::new(db, default_db).resolve_db(qdb)?;
    if let Some(err) = db.attached_read_only(&table_db, table) {
        return Err(err);
    }
    let t = db.table_in(&table_db, table)?;
    let schema = t.schema();
    let order: Vec<usize> = match columns {
        Some(cols) => cols
            .iter()
            .map(|c| {
                t.column_index(c)
                    .map_err(|_| PinkDbError::Catalog(format!("no column \"{c}\"")))
            })
            .collect::<Result<_>>()?,
        None => (0..schema.len()).collect(),
    };
    if let Some(cols) = columns {
        let mut seen = std::collections::HashSet::new();
        for c in cols {
            if !seen.insert(c.to_ascii_lowercase()) {
                return Err(PinkDbError::Catalog(format!(
                    "duplicate INSERT column \"{c}\""
                )));
            }
        }
    }
    let mut binder = Binder::new(db, default_db).with_params(params);
    let query_plan = bind_select_in(db, &mut binder, query)?;
    let source = plan_schema(db, &binder.default_db, &binder.ctes, &query_plan)?;
    if source.len() != order.len() {
        return Err(PinkDbError::Execution(format!(
            "INSERT has {} values for {} columns",
            source.len(),
            order.len()
        )));
    }
    for ((_, src), &target) in source.iter().zip(order.iter()) {
        if !DataTable::can_coerce(*src, schema[target].1) {
            return Err(PinkDbError::Type(format!(
                "cannot insert {src} into {} column \"{}\"",
                schema[target].1, schema[target].0
            )));
        }
    }
    reject_view_dependency(db, &binder.views_used())?;
    let dynamic = binder.dynamic_functions_used();
    Ok((
        LogicalPlan::InsertSelect {
            db: Some(table_db),
            table: t.name.clone(),
            columns: Some(order),
            query: Box::new(query_plan),
        },
        dynamic,
    ))
}

/// Bind `CREATE TABLE t (items...)`: build the schema and resolve the
/// declarative constraints.
///
/// Constraints are **metadata**: foreign keys are validated here (the
/// referenced table must exist and the referenced columns must be exactly one
/// of its `PRIMARY KEY`/`UNIQUE` constraints, with compatible types — DuckDB's
/// DDL-time rule) but nothing is ever enforced on INSERT/UPDATE. `RELY` /
/// `[NOT] ENFORCED` were already accepted (and dropped) by the parser. A
/// dangling reference is therefore a `CREATE TABLE` error, exactly like
/// DuckDB's; unenforced *data* is not.
fn bind_create_table(
    db: &PinkDb,
    default_db: &str,
    qdb: Option<&str>,
    name: &str,
    items: &[TableItem],
    if_not_exists: bool,
) -> Result<(LogicalPlan, Vec<String>)> {
    let target = Binder::new(db, default_db).resolve_db(&qdb.map(str::to_string))?;
    let mut schema: Vec<(String, DataType)> = Vec::new();
    let mut constraints = TableConstraints::default();
    // Named constraints are echoes of the DDL; only the declaration matters.
    for item in items {
        match item {
            TableItem::Column(col) => {
                let idx = schema.len();
                if schema
                    .iter()
                    .any(|(n, _)| n.eq_ignore_ascii_case(&col.name))
                {
                    return Err(PinkDbError::Catalog(format!(
                        "duplicate column name \"{}\"",
                        col.name
                    )));
                }
                schema.push((col.name.clone(), col.dtype));
                for c in &col.constraints {
                    bind_column_constraint(db, &target, name, &schema, idx, c, &mut constraints)?;
                }
            }
            TableItem::Constraint(tc) => {
                bind_table_constraint(db, &target, name, &schema, tc, &mut constraints)?;
            }
        }
    }
    if schema.is_empty() {
        return Err(PinkDbError::Catalog(
            "cannot create table with no columns".to_string(),
        ));
    }
    Ok((
        LogicalPlan::CreateTable {
            db: Some(target),
            name: name.to_string(),
            schema,
            constraints,
            if_not_exists,
        },
        Vec::new(),
    ))
}

/// Bind `ALTER TABLE` (wave 65): validate the target table/columns against the
/// catalog, convert the operation into an [`AlterTablePlan`], and bind the
/// optional `ADD COLUMN` default as a constant expression (an empty scope,
/// like an `INSERT` cell: literals and constant scalar functions only).
///
/// Error wording follows DuckDB where it is pinned by parity tests
/// (`Column with name … already exists!`, `Table "t" does not have a column
/// with name …`, `Could not rename …`).
fn bind_alter_table(
    db: &PinkDb,
    default_db: &str,
    qdb: Option<&str>,
    name: &str,
    op: &AlterTableOp,
    params: Option<ParamSlots>,
) -> Result<(LogicalPlan, Vec<String>)> {
    let target = Binder::new(db, default_db).resolve_db(&qdb.map(str::to_string))?;
    if let Some(err) = db.attached_read_only(&target, name) {
        return Err(err);
    }
    let t = db.table_in(&target, name)?;
    let schema: Vec<(String, DataType)> = t.schema();
    let table_display = t.name.clone();
    let plan_op = match op {
        AlterTableOp::AddColumn {
            column,
            default,
            if_not_exists,
        } => {
            let exists = schema
                .iter()
                .any(|(n, _)| n.eq_ignore_ascii_case(&column.name));
            if exists && !*if_not_exists {
                return Err(PinkDbError::Catalog(format!(
                    "Column with name \"{}\" already exists!",
                    column.name
                )));
            }
            let mut not_null = false;
            for c in &column.constraints {
                match c.kind {
                    ColumnConstraintKind::NotNull => not_null = true,
                    // The other constraint kinds exist on CREATE TABLE only in
                    // this wave; a clean refusal instead of a half-recording.
                    ColumnConstraintKind::PrimaryKey
                    | ColumnConstraintKind::Unique
                    | ColumnConstraintKind::References { .. } => {
                        return Err(PinkDbError::Catalog(format!(
                            "ALTER TABLE ADD COLUMN supports only the NOT NULL constraint \
                             (a {} constraint on an added column is not supported by this \
                             wave); add the column first and re-create the table with the \
                             constraint",
                            match c.kind {
                                ColumnConstraintKind::PrimaryKey => "PRIMARY KEY",
                                ColumnConstraintKind::Unique => "UNIQUE",
                                _ => "REFERENCES",
                            }
                        )));
                    }
                }
            }
            // A default expression binds in an empty scope (constants only).
            let bound_default = match default {
                None => None,
                Some(expr) => {
                    let mut binder = Binder::new(db, &target).with_params(params.clone());
                    binder.allow_win = false;
                    binder.pin_parameter(expr, column.dtype)?;
                    let mut aggs = Vec::new();
                    Some(binder.bind_expr(expr, &mut aggs, false)?)
                }
            };
            AlterTablePlan::AddColumn {
                column: column.name.clone(),
                dtype: column.dtype,
                default: bound_default,
                not_null,
                if_not_exists: *if_not_exists,
            }
        }
        AlterTableOp::DropColumn {
            name: col,
            if_exists,
        } => {
            let Some(idx) = schema.iter().position(|(n, _)| n.eq_ignore_ascii_case(col)) else {
                if *if_exists {
                    // A no-op plan the executor recognizes by index `None`.
                    return Ok((
                        LogicalPlan::AlterTable {
                            db: Some(target),
                            name: table_display,
                            op: AlterTablePlan::DropColumn {
                                column: col.clone(),
                                if_exists: true,
                            },
                        },
                        Vec::new(),
                    ));
                }
                return Err(PinkDbError::Catalog(format!(
                    "Table \"{name}\" does not have a column with name \"{col}\""
                )));
            };
            // A column a declarative key depends on must not disappear: the
            // stored constraint indices would silently address other columns.
            // (NOT NULL is fine: the executor reindexes it.)
            for k in &t.constraints.keys {
                if k.columns.contains(&idx) {
                    return Err(PinkDbError::Catalog(format!(
                        "Cannot drop column \"{col}\" because there is a {} constraint that \
                         depends on it",
                        match k.kind {
                            KeyKind::Primary => "PRIMARY KEY",
                            KeyKind::Unique => "UNIQUE",
                        }
                    )));
                }
            }
            for fk in &t.constraints.foreign_keys {
                if fk.columns.contains(&idx) {
                    return Err(PinkDbError::Catalog(format!(
                        "Cannot drop column \"{col}\" because there is a FOREIGN KEY constraint \
                         that depends on it"
                    )));
                }
            }
            AlterTablePlan::DropColumn {
                column: col.clone(),
                if_exists: *if_exists,
            }
        }
        AlterTableOp::RenameColumn { from, to } => {
            let Some(from_idx) = schema
                .iter()
                .position(|(n, _)| n.eq_ignore_ascii_case(from))
            else {
                return Err(PinkDbError::Catalog(format!(
                    "Table \"{name}\" does not have a column with name \"{from}\""
                )));
            };
            if let Some(to_idx) = schema.iter().position(|(n, _)| n.eq_ignore_ascii_case(to))
                && to_idx != from_idx
            {
                return Err(PinkDbError::Catalog(format!(
                    "Column with name \"{to}\" already exists!"
                )));
            }
            AlterTablePlan::RenameColumn {
                from: schema[from_idx].0.clone(),
                to: to.clone(),
            }
        }
        AlterTableOp::RenameTable { to } => {
            if to.eq_ignore_ascii_case(&table_display) {
                return Ok((
                    LogicalPlan::AlterTable {
                        db: Some(target),
                        name: table_display,
                        op: AlterTablePlan::RenameTable { to: to.clone() },
                    },
                    Vec::new(),
                ));
            }
            let taken = db.table_in(&target, to).is_ok() || db.view_in(&target, to).is_ok();
            if taken {
                return Err(PinkDbError::Catalog(format!(
                    "Could not rename \"{}\" to \"{to}\": another entry with this name already \
                     exists!",
                    table_display
                )));
            }
            if let Some(err) = db.attached_read_only(&target, to) {
                return Err(err);
            }
            AlterTablePlan::RenameTable { to: to.clone() }
        }
    };
    Ok((
        LogicalPlan::AlterTable {
            db: Some(target),
            name: table_display,
            op: plan_op,
        },
        Vec::new(),
    ))
}

/// Resolve a key/foreign-key column-name list to schema indices.
fn resolve_constraint_columns(
    schema: &[(String, DataType)],
    table: &str,
    what: &str,
    columns: &[String],
) -> Result<Vec<usize>> {
    if columns.is_empty() {
        return Err(PinkDbError::Catalog(format!(
            "{what} needs at least one column"
        )));
    }
    let mut out = Vec::with_capacity(columns.len());
    for c in columns {
        let idx = schema
            .iter()
            .position(|(n, _)| n.eq_ignore_ascii_case(c))
            .ok_or_else(|| {
                PinkDbError::Catalog(format!("table \"{table}\" has no column \"{c}\""))
            })?;
        if out.contains(&idx) {
            return Err(PinkDbError::Catalog(format!(
                "column \"{c}\" appears more than once in the {what}"
            )));
        }
        out.push(idx);
    }
    Ok(out)
}

fn bind_column_constraint(
    db: &PinkDb,
    target: &str,
    table: &str,
    schema: &[(String, DataType)],
    idx: usize,
    c: &ColumnConstraint,
    out: &mut TableConstraints,
) -> Result<()> {
    match &c.kind {
        ColumnConstraintKind::PrimaryKey => {
            push_key(out, table, c.name.clone(), KeyKind::Primary, vec![idx])
        }
        ColumnConstraintKind::Unique => {
            push_key(out, table, c.name.clone(), KeyKind::Unique, vec![idx])
        }
        ColumnConstraintKind::NotNull => {
            out.not_null.push(idx);
            Ok(())
        }
        ColumnConstraintKind::References {
            ref_table,
            ref_columns,
        } => bind_foreign_key(
            db,
            target,
            schema,
            c.name.clone(),
            &[idx],
            ref_table,
            ref_columns.as_deref(),
            out,
        ),
    }
}

fn bind_table_constraint(
    db: &PinkDb,
    target: &str,
    table: &str,
    schema: &[(String, DataType)],
    tc: &TableConstraint,
    out: &mut TableConstraints,
) -> Result<()> {
    match tc {
        TableConstraint::PrimaryKey { name, columns } => {
            let cols = resolve_constraint_columns(schema, table, "PRIMARY KEY", columns)?;
            push_key(out, table, name.clone(), KeyKind::Primary, cols)
        }
        TableConstraint::Unique { name, columns } => {
            let cols = resolve_constraint_columns(schema, table, "UNIQUE", columns)?;
            push_key(out, table, name.clone(), KeyKind::Unique, cols)
        }
        TableConstraint::ForeignKey {
            name,
            columns,
            ref_table,
            ref_columns,
        } => {
            let cols = resolve_constraint_columns(schema, table, "FOREIGN KEY", columns)?;
            bind_foreign_key(
                db,
                target,
                schema,
                name.clone(),
                &cols,
                ref_table,
                ref_columns.as_deref(),
                out,
            )
        }
    }
}

fn push_key(
    out: &mut TableConstraints,
    table: &str,
    name: Option<String>,
    kind: KeyKind,
    columns: Vec<usize>,
) -> Result<()> {
    if kind == KeyKind::Primary && out.primary_key().is_some() {
        return Err(PinkDbError::Catalog(format!(
            "table \"{table}\" has more than one primary key"
        )));
    }
    out.keys.push(KeyConstraint {
        name,
        kind,
        columns,
    });
    Ok(())
}

/// Resolve and validate one `REFERENCES`/`FOREIGN KEY` declaration against the
/// *current* catalog (DuckDB's DDL-time rule): the referenced table must
/// exist, the referenced columns must name exactly one of its PRIMARY
/// KEY/UNIQUE constraints (order included), and every pair must have
/// compatible types. Nothing is enforced on writes afterwards.
#[allow(clippy::too_many_arguments)]
fn bind_foreign_key(
    db: &PinkDb,
    target: &str,
    schema: &[(String, DataType)],
    name: Option<String>,
    columns: &[usize],
    ref_table: &RefTable,
    ref_columns: Option<&[String]>,
    out: &mut TableConstraints,
) -> Result<()> {
    let ref_db = match &ref_table.db {
        Some(q) => {
            if !db.database_exists(q) {
                return Err(PinkDbError::Catalog(format!("database \"{q}\" not found")));
            }
            q.to_ascii_lowercase()
        }
        None => target.to_string(),
    };
    let ref_t = db.table_in(&ref_db, &ref_table.name).map_err(|_| {
        PinkDbError::Catalog(format!(
            "failed to create foreign key: table \"{}\" does not exist",
            ref_table.name
        ))
    })?;
    let ref_constraints = ref_t.constraints.clone();
    let ref_names: Vec<String> = ref_t.columns.iter().map(|c| c.name.clone()).collect();
    let ref_types: Vec<DataType> = ref_t.columns.iter().map(|c| c.dtype).collect();
    let ref_cols: Vec<usize> = match ref_columns {
        Some(cols) => cols
            .iter()
            .map(|c| {
                ref_names
                    .iter()
                    .position(|n| n.eq_ignore_ascii_case(c))
                    .ok_or_else(|| {
                        PinkDbError::Catalog(format!(
                            "table \"{}\" has no column \"{c}\"",
                            ref_table.name
                        ))
                    })
            })
            .collect::<Result<Vec<usize>>>()?,
        // A bare `REFERENCES u` means u's primary key (DuckDB).
        None => match ref_constraints.primary_key() {
            Some(k) => k.columns.clone(),
            None => {
                return Err(PinkDbError::Catalog(format!(
                    "failed to create foreign key: there is no primary key or unique \
                     constraint for referenced table \"{}\"",
                    ref_table.name
                )));
            }
        },
    };
    if ref_cols.len() != columns.len() {
        return Err(PinkDbError::Catalog(format!(
            "the number of referencing and referenced columns for foreign keys must be \
             the same ({} vs {})",
            columns.len(),
            ref_cols.len()
        )));
    }
    if ref_constraints.key_on(&ref_cols).is_none() {
        let cols = ref_cols
            .iter()
            .map(|&i| ref_names[i].as_str())
            .collect::<Vec<_>>()
            .join(",");
        return Err(PinkDbError::Catalog(format!(
            "failed to create foreign key: referenced table \"{}\" does not have a \
             primary key or unique constraint on the columns {cols}",
            ref_table.name
        )));
    }
    // Type compatibility (DuckDB's rule): equal types or the INTEGER →
    // BIGINT → DOUBLE widening used by INSERT coercion.
    for (child, &parent) in columns.iter().zip(ref_cols.iter()) {
        let (child_name, child_type) = &schema[*child];
        let parent_type = ref_types[parent];
        if !DataTable::can_coerce(*child_type, parent_type)
            && !DataTable::can_coerce(parent_type, *child_type)
        {
            return Err(PinkDbError::Catalog(format!(
                "failed to create foreign key: incompatible types between column \"{}\" \
                 (\"{}\") and column \"{}\" (\"{}\")",
                ref_names[parent], parent_type, child_name, child_type
            )));
        }
    }
    out.foreign_keys.push(ForeignKeyConstraint {
        name,
        columns: columns.to_vec(),
        ref_db,
        ref_table: ref_table.name.to_ascii_lowercase(),
        ref_columns: ref_cols,
    });
    Ok(())
}

/// Bind `CREATE TABLE t AS SELECT ...`: infer the new table's schema from the
/// query's output (names + types).
fn bind_create_table_as(
    db: &PinkDb,
    default_db: &str,
    qdb: &Option<String>,
    name: &str,
    query: &Select,
    if_not_exists: bool,
    params: Option<ParamSlots>,
) -> Result<(LogicalPlan, Vec<String>)> {
    let table_db = Binder::new(db, default_db).resolve_db(qdb)?;
    let mut binder = Binder::new(db, default_db).with_params(params);
    let query_plan = bind_select_in(db, &mut binder, query)?;
    // The schema is taken from the query's evaluated output at execution
    // (names from the final Project, types from its expressions) — binding
    // here already validated the query in full.
    reject_view_dependency(db, &binder.views_used())?;
    let dynamic = binder.dynamic_functions_used();
    Ok((
        LogicalPlan::CreateTableAs {
            db: Some(table_db),
            name: name.to_string(),
            query: Box::new(query_plan),
            if_not_exists,
        },
        dynamic,
    ))
}

/// Bind `DESCRIBE <table>`: resolve the name like a FROM source (CTEs and
/// views cannot be described — only catalog/attached tables) and carry its
/// schema into the plan; the executor formats the DuckDB-shaped rows.
fn bind_describe(
    db: &PinkDb,
    default_db: &str,
    qdb: Option<&str>,
    name: &str,
) -> Result<(LogicalPlan, Vec<String>)> {
    let lookup = match qdb {
        Some(q) => {
            if !db.database_exists(q) {
                return Err(PinkDbError::Catalog(format!("database \"{q}\" not found")));
            }
            q.to_ascii_lowercase()
        }
        None => default_db.to_ascii_lowercase(),
    };
    if let Ok(t) = db.table_in(&lookup, name) {
        let mut columns = Vec::with_capacity(t.columns.len());
        for (i, c) in t.columns.iter().enumerate() {
            columns.push(crate::plan::DescribeColumn {
                name: c.name.clone(),
                dtype: c.dtype,
                not_null: t.constraints.is_not_null(i),
                key: t.constraints.column_key_kind(i),
                default: c
                    .default
                    .as_ref()
                    .map(crate::prepared::value_literal)
                    .transpose()?,
            });
        }
        return Ok((LogicalPlan::Describe { columns }, Vec::new()));
    }
    if let Ok(view) = db.view_in(&lookup, name) {
        let view = view.clone();
        let mut child = Binder::new(db, &lookup);
        // The view body is self-contained (same rule as FROM resolution).
        child.ctes.clear();
        let sub = bind_select_in(db, &mut child, &view.query)?;
        let mut schema = plan_schema(db, &child.default_db, &child.ctes, &sub)?;
        if let Some(cols) = &view.cols {
            for (s, c) in schema.iter_mut().zip(cols.iter()) {
                s.0 = c.clone();
            }
        }
        let columns = schema
            .into_iter()
            .map(|(name, dtype)| crate::plan::DescribeColumn {
                name,
                dtype,
                not_null: false,
                key: None,
                default: None,
            })
            .collect();
        return Ok((LogicalPlan::Describe { columns }, Vec::new()));
    }
    // Attached lake tables are visible to queries (they are not in the store).
    let schema = db.visible_table_schema(&lookup, name)?;
    let columns = schema
        .into_iter()
        .map(|(name, dtype)| crate::plan::DescribeColumn {
            name,
            dtype,
            not_null: false,
            key: None,
            default: None,
        })
        .collect();
    Ok((LogicalPlan::Describe { columns }, Vec::new()))
}

/// Durable databases replay committed SQL text, and views are never
/// checkpointed — so a mutating statement that reads a session-local view
/// could never be rebuilt during replay (recovery would hard-error
/// "table not found"). Refuse it at bind time, before any change is made.
fn reject_view_dependency(db: &PinkDb, views: &[String]) -> Result<()> {
    if views.is_empty() || !db.is_durable() {
        return Ok(());
    }
    Err(PinkDbError::Execution(format!(
        "statement reads session-local view \"{}\", which is never persisted; a durable \
         database cannot replay it — materialize the view into a table first",
        views[0]
    )))
}

/// Bind a top-level SELECT with a fresh binder.
fn bind_select(
    db: &PinkDb,
    default_db: &str,
    sel: &Select,
    params: Option<ParamSlots>,
) -> Result<(LogicalPlan, Vec<String>)> {
    let mut binder = Binder::new(db, default_db).with_params(params);
    let plan = bind_select_in(db, &mut binder, sel)?;
    Ok((plan, binder.dynamic_functions_used()))
}

/// ORDER BY/LIMIT written before a UNION (on the head or a middle branch):
/// the current parser attaches such clauses to the branch they follow, which
/// would silently change semantics. DuckDB rejects them too.
fn union_clause_error() -> PinkDbError {
    PinkDbError::Parse(
        "ORDER BY/LIMIT/OFFSET before UNION is not supported; place it after the last branch"
            .to_string(),
    )
}

/// True when a set-op branch carries a trailing clause the parser attached
/// to it (which means the clause was written *before* the next set operator).
fn branch_has_trailing_clause(sel: &Select) -> bool {
    !sel.order_by.is_empty() || sel.limit.is_some() || sel.offset.is_some()
}

/// Bind a complete SELECT in an existing scope: WITH/CTE definitions, the
/// body, and — when present — the whole set-operation chain. Chain-level
/// ORDER BY/LIMIT/OFFSET (written after the last branch) apply to the whole
/// set operation.
///
/// Every query context (statement, derived table, CTE body, view, subquery)
/// must enter here: `bind_select_with` binds a single branch and ignores
/// `unions`.
fn bind_select_in(db: &PinkDb, binder: &mut Binder, sel: &Select) -> Result<LogicalPlan> {
    if sel.unions.is_empty() {
        return bind_select_with(db, binder, sel);
    }
    // The parser attaches trailing clauses to the last branch; a clause on the
    // head or on a middle branch was therefore written *before* a set operator.
    if branch_has_trailing_clause(sel) {
        return Err(union_clause_error());
    }
    for (_, branch) in &sel.unions[..sel.unions.len() - 1] {
        if branch_has_trailing_clause(branch) {
            return Err(union_clause_error());
        }
    }
    // Hoist the last branch's trailing clauses to the chain level.
    let mut head = sel.clone();
    let (_, last) = head.unions.last_mut().expect("unions checked non-empty");
    let order_by = std::mem::take(&mut last.order_by);
    let limit = last.limit.take();
    let offset = last.offset.take();

    // WITH belongs to the whole chain: bind it once, then wrap the final plan.
    // Every branch (and the ORDER BY/LIMIT wrapper below) sees the CTEs.
    let cte_base = binder.ctes.len();
    let cte_plans = bind_ctes(db, binder, sel)?;
    head.with.clear();

    let plan = if order_by.is_empty() && limit.is_none() && offset.is_none() {
        bind_union_chain(db, binder, &head)?
    } else {
        // Apply the chain clauses over the whole set operation by wrapping it
        // in a derived table: the plain-SELECT path then binds ORDER BY/LIMIT
        // with exactly the same positional/alias/expression rules.
        let wrapper = Select {
            with: Vec::new(),
            distinct: false,
            projections: vec![Projection::Star],
            from: Some(FromClause {
                source: FromSource::Derived(Box::new(head)),
                alias: Some("__setop".to_string()),
                at: None,
                joins: Vec::new(),
            }),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
            order_by,
            limit,
            offset,
            unions: Vec::new(),
        };
        bind_select_in(db, binder, &wrapper)?
    };
    Ok(wrap_ctes(binder, cte_base, cte_plans, plan))
}

/// Bind the head and every set-operation branch into the left-associative set
/// operation, honoring `INTERSECT`'s higher precedence over `UNION`/`EXCEPT`
/// (DuckDB's rule): consecutive `INTERSECT` runs fold into their left
/// neighbor first, then `UNION`/`EXCEPT` apply left-associatively over the
/// resulting units. Branch-level ORDER BY/LIMIT are hoisted (or rejected) by
/// [`bind_select_in`] before this point, so each branch binds bare; pairwise
/// type unification (numeric tower) is recorded on every `SetOp` node.
fn bind_union_chain(db: &PinkDb, binder: &mut Binder, sel: &Select) -> Result<LogicalPlan> {
    debug_assert!(!sel.unions.is_empty());
    debug_assert!(sel.order_by.is_empty() && sel.limit.is_none() && sel.offset.is_none());
    let mut head = sel.clone();
    head.unions.clear();
    let head_plan = bind_select_with(db, binder, &head)?;
    // Right branches: each sees the outer CTEs (including the head's WITH
    // CTEs, bound by the caller).
    let mut units: Vec<(Option<SetOp>, LogicalPlan)> = vec![(None, head_plan)];
    for (op, branch) in &sel.unions {
        debug_assert!(!branch_has_trailing_clause(branch));
        let mut child = binder.child();
        let right = bind_select_with(db, &mut child, branch)?;
        if is_intersect(*op) {
            // Fold into the previous unit (INTERSECT binds tighter).
            let (prev_op, prev) = units.pop().expect("at least the head");
            let combined = bind_pair_types(db, &binder.default_db, &binder.ctes, *op, prev, right)?;
            units.push((prev_op, combined));
        } else {
            units.push((Some(*op), right));
        }
    }
    // Left-associative fold of the folded units.
    let mut iter = units.into_iter();
    let (_, mut plan) = iter.next().expect("head unit");
    for (op, right) in iter {
        let op = op.expect("non-head units carry their operator");
        plan = bind_pair_types(db, &binder.default_db, &binder.ctes, op, plan, right)?;
    }
    Ok(plan)
}

fn is_intersect(op: SetOp) -> bool {
    matches!(op, SetOp::Intersect | SetOp::IntersectAll)
}

/// Combine two already-bound set-operation operands with pairwise common
/// types (numeric tower; else identical).
fn bind_pair_types(
    db: &PinkDb,
    default_db: &str,
    ctes: &[(String, Vec<(String, DataType)>)],
    op: SetOp,
    left: LogicalPlan,
    right: LogicalPlan,
) -> Result<LogicalPlan> {
    let left_schema = plan_schema(db, default_db, ctes, &left)?;
    let right_schema = plan_schema(db, default_db, ctes, &right)?;
    if left_schema.len() != right_schema.len() {
        return Err(PinkDbError::Execution(format!(
            "set-operation branches have {} and {} columns",
            left_schema.len(),
            right_schema.len()
        )));
    }
    let mut types = Vec::with_capacity(left_schema.len());
    // A branch column that is a bare NULL literal adopts the other branch's
    // type (like COALESCE/CASE) rather than failing numeric-tower unification,
    // and is cast so the branch's vector matches the set-operation's type.
    let left_null = null_literal_columns(&left, left_schema.len());
    let right_null = null_literal_columns(&right, right_schema.len());
    for (i, ((_, l), (_, r))) in left_schema.iter().zip(right_schema.iter()).enumerate() {
        let t = if left_null[i] && !right_null[i] {
            *r
        } else if right_null[i] && !left_null[i] {
            *l
        } else {
            coalesce_common(*l, *r)?
        };
        types.push(t);
    }
    let left = cast_null_columns(left, &left_null, &left_schema, &types);
    let right = cast_null_columns(right, &right_null, &right_schema, &types);
    Ok(LogicalPlan::SetOp {
        op,
        left: Box::new(left),
        right: Box::new(right),
        types,
    })
}

/// Per output column: true when a top-level `Project` emits a bare NULL there.
fn null_literal_columns(plan: &LogicalPlan, len: usize) -> Vec<bool> {
    let LogicalPlan::Project { exprs, .. } = plan else {
        return vec![false; len];
    };
    (0..len)
        .map(|i| matches!(exprs.get(i), Some(BoundExpr::Literal(Value::Null))))
        .collect()
}

/// Cast a `Project`'s bare-NULL output columns to the set-operation's type
/// when they differ from the literal's own (default) type.
fn cast_null_columns(
    plan: LogicalPlan,
    is_null: &[bool],
    schema: &[(String, DataType)],
    types: &[DataType],
) -> LogicalPlan {
    match plan {
        LogicalPlan::Project {
            input,
            mut exprs,
            names,
        } => {
            for (i, e) in exprs.iter_mut().enumerate() {
                if is_null.get(i).copied().unwrap_or(false) && schema[i].1 != types[i] {
                    let dtype = types[i];
                    *e = BoundExpr::Cast {
                        expr: Box::new(e.clone()),
                        dtype,
                    };
                }
            }
            LogicalPlan::Project {
                input,
                exprs,
                names,
            }
        }
        other => other,
    }
}

/// Bind a materialized view's defining query without evaluating it: the
/// view's own database resolves unqualified names (wave 80). Used by `CREATE
/// MATERIALIZED VIEW` (which also carries where-clauses/params via the normal
/// statement path) and by the `.pink` loader, which rebinds a persisted
/// definition.
pub(crate) fn bind_mv_query(
    db: &PinkDb,
    default_db: &str,
    query: &crate::sql::ast::Select,
) -> Result<LogicalPlan> {
    let mut binder = Binder::new(db, default_db);
    bind_select_in(db, &mut binder, query)
}

/// Output schema of a bound plan: (column name, type) per output column.
/// Used for CTE/derived-table scoping and subquery arity checks. `ctes`
/// carries the CTEs visible at this point (CTEs shadow catalog tables);
/// `default_db` resolves a `Scan` that names a CTE only indirectly. The
/// executor uses it too (LIMIT 0 returns an empty result whose header must
/// still match, and the LIMIT stream needs a scan's shape without decoding).
pub(crate) fn plan_schema(
    db: &PinkDb,
    default_db: &str,
    ctes: &[(String, Vec<(String, DataType)>)],
    plan: &LogicalPlan,
) -> Result<Vec<(String, DataType)>> {
    match plan {
        LogicalPlan::Scan { db: qdb, table, at } => {
            if qdb.is_none()
                && let Some((_, schema)) = ctes
                    .iter()
                    .rev()
                    .find(|(n, _)| n.eq_ignore_ascii_case(table))
                {
                    return Ok(schema.clone());
                }
            let lookup = qdb.as_deref().unwrap_or(default_db);
            // Attached lake tables are visible to queries (they are not in
            // the store themselves); an `AT` target resolves the schema from
            // the target version's own descriptor.
            match at {
                None => db.visible_table_schema(lookup, table),
                Some(at) => db.visible_table_schema_at(lookup, table, *at),
            }
        }
        LogicalPlan::SingleRow => Ok(Vec::new()),
        // A file source's schema is the schema the binder read from the
        // file at bind time — carried in the plan, so no second read.
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Offset { input, .. } => plan_schema(db, default_db, ctes, input),
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::CreateTableAs { .. }
        | LogicalPlan::Insert { .. }
        | LogicalPlan::InsertSelect { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. }
        // COPY's result shape is built by the executor (a one-row Count),
        // like DESCRIBE/EXPLAIN's.
        | LogicalPlan::CopyTo { .. }
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::AlterTable { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::CreateDatabase { .. }
        | LogicalPlan::DropDatabase { .. }
        | LogicalPlan::LoadExtension { .. }
        | LogicalPlan::ShowTables { .. }
        | LogicalPlan::ShowDatabases
        | LogicalPlan::ShowMaterializedViews { .. }
        | LogicalPlan::Checkpoint
        | LogicalPlan::Use { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::CreateMaterializedView { .. }
        | LogicalPlan::RefreshMaterializedView { .. }
        | LogicalPlan::RefreshMaterializedViews { .. }
        | LogicalPlan::DropMaterializedView { .. }
        // Wave 44: procedure DDL and `CALL` never appear as a query scope.
        | LogicalPlan::CreateProcedure { .. }
        | LogicalPlan::DropProcedure { .. }
        | LogicalPlan::Call { .. }
        // Wave 48: neither do RBAC statements.
        | LogicalPlan::CreateUser { .. }
        | LogicalPlan::AlterUser { .. }
        | LogicalPlan::DropUser { .. }
        | LogicalPlan::CreateRole { .. }
        | LogicalPlan::DropRole { .. }
        | LogicalPlan::Grant { .. }
        | LogicalPlan::Revoke { .. } => Ok(Vec::new()),
        LogicalPlan::IndexScan { db: qdb, table, .. } => {
            let lookup = qdb.as_deref().unwrap_or(default_db);
            db.visible_table_schema(lookup, table)
        }
        // DESCRIBE/EXPLAIN produce fixed shapes (their schema is built by the
        // executor); they never appear as a query scope.
        LogicalPlan::Describe { .. } | LogicalPlan::Explain { .. } => Ok(Vec::new()),
        LogicalPlan::TableFunction { schema, .. } => Ok(schema.clone()),
        LogicalPlan::Project { exprs, names, .. } => Ok(names
            .iter()
            .cloned()
            .zip(exprs.iter().map(|e| e.dtype()))
            .collect()),
        LogicalPlan::Aggregate { keys, aggs, .. } => {
            let (names, types) = crate::plan::agg_output_schema(keys, aggs);
            Ok(names.into_iter().zip(types).collect())
        }
        LogicalPlan::Join { left, right, .. } | LogicalPlan::NestedLoopJoin { left, right, .. } => {
            let mut out = plan_schema(db, default_db, ctes, left)?;
            out.extend(plan_schema(db, default_db, ctes, right)?);
            Ok(out)
        }
        LogicalPlan::SetOp { left, types, .. } => {
            // Output names come from the left branch.
            let left_schema = plan_schema(db, default_db, ctes, left)?;
            Ok(left_schema
                .into_iter()
                .zip(types.iter().copied())
                .map(|((n, _), t)| (n, t))
                .collect())
        }
        LogicalPlan::Cte { name, query, body } => {
            let mut inner: Vec<(String, Vec<(String, DataType)>)> = ctes.to_vec();
            inner.push((name.clone(), plan_schema(db, default_db, ctes, query)?));
            plan_schema(db, default_db, &inner, body)
        }
        LogicalPlan::Window { input, windows, .. } => {
            let mut out = plan_schema(db, default_db, ctes, input)?;
            let (names, types) = crate::plan::win_output_schema(windows);
            out.extend(names.into_iter().zip(types));
            Ok(out)
        }
    }
}

/// Evaluate a table-function call's parsed arguments to literals.
///
/// Only literals bind: the call is resolved against the filesystem at bind
/// time (the schema comes from the files), so an argument that is not a
/// constant (`?`, a column, an expression) has no defined bind-time value.
/// A single unary minus over a numeric literal folds (`-3`), which is the one
/// expression shape worth special-casing.
fn bind_table_function_args(
    args: &[crate::sql::ast::TableFunctionCallArg],
) -> Result<Vec<TableFunctionArg>> {
    let mut out = Vec::with_capacity(args.len());
    for arg in args {
        let value = literal_arg_value(&arg.expr).ok_or_else(|| {
            PinkDbError::Type(
                "a table-function argument must be a literal (got an expression); \
                 write the value directly, e.g. read_parquet('file.parquet', \
                 union_by_name := TRUE)"
                    .to_string(),
            )
        })?;
        out.push(TableFunctionArg {
            name: arg.name.clone(),
            value,
        });
    }
    Ok(out)
}

/// The literal value of a table-function argument expression, if it is one.
fn literal_arg_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Literal(v) => Some(v.clone()),
        Expr::Unary {
            op: crate::sql::ast::UnaryOp::Negate,
            expr,
        } => match literal_arg_value(expr)? {
            Value::Integer(n) => Some(Value::Integer(-n)),
            Value::Bigint(n) => Some(Value::Bigint(-n)),
            Value::Double(d) => Some(Value::Double(-d)),
            Value::Decimal(v, s) => Some(Value::Decimal(-v, s)),
            _ => None,
        },
        _ => None,
    }
}

/// Bind a table-function FROM source against the registered table sources.
fn bind_table_function(
    db: &PinkDb,
    name: &str,
    args: &[TableFunctionArg],
    alias: &Option<String>,
) -> Result<(ScopeTable, LogicalPlan)> {
    let call = TableFunctionCall::Function {
        name: name.to_string(),
        args: args.to_vec(),
    };
    for source in db.table_sources() {
        if let Some(schema) = source.bind(&call)? {
            let eff = alias.clone().unwrap_or_else(|| name.to_string());
            return Ok((
                ScopeTable {
                    name: eff.to_ascii_lowercase(),
                    columns: schema.clone(),
                },
                LogicalPlan::TableFunction {
                    source: source.name().to_ascii_lowercase(),
                    call,
                    schema,
                },
            ));
        }
    }
    Err(PinkDbError::Catalog(format!(
        "unknown table function \"{name}\": {}",
        no_file_source_hint(db)
    )))
}

/// The hint appended to "unknown table function" / "cannot read by path"
/// errors: whether any source is registered at all changes what the user
/// should do about it.
fn no_file_source_hint(db: &PinkDb) -> String {
    if db.table_sources().is_empty() {
        "no table source is registered in this session (a build with the matching \
         first-party extension — e.g. `arrow` or `parquet` — registers it, or \
         register one with PinkDb::register_table_source)"
            .to_string()
    } else {
        "the registered table sources do not serve it".to_string()
    }
}

/// The scope name of a replacement scan: the file name's stem
/// (`FROM 'dir/u.parquet'` binds as `u`, DuckDB's rule), lowercased. A path
/// whose last component has no usable stem (a glob like `data/*.parquet`)
/// keeps the raw stem text; qualified references to it are unusual, and
/// unqualified column references are unaffected.
fn replacement_scan_name(path: &str) -> String {
    let last = path.rsplit(['/', '\\']).next().unwrap_or(path);
    let stem = last.split('.').next().unwrap_or(last);
    if stem.is_empty() {
        last.to_string()
    } else {
        stem.to_string()
    }
}

/// Bind one FROM/JOIN source: returns its scope entry + executable plan.
/// Unqualified tables resolve CTEs first (shadowing), then views, then the
/// catalog — all in the session's default database. Qualified `db.table`
/// never resolves a CTE and looks only in `db`. Derived tables bind their
/// inner query in a child scope (same CTEs, no outer tables — subqueries are
/// uncorrelated in v1).
fn bind_source(
    db: &PinkDb,
    binder: &mut Binder,
    source: &FromSource,
    alias: &Option<String>,
    at: Option<AtSpec>,
) -> Result<(ScopeTable, LogicalPlan)> {
    match source {
        FromSource::Table { db: qdb, name } => {
            // CTEs shadow everything — but only unqualified names.
            if qdb.is_none()
                && let Some((_, schema)) = binder
                    .ctes
                    .iter()
                    .rev()
                    .find(|(n, _)| n.eq_ignore_ascii_case(name))
            {
                if let Some(at) = at {
                    // DuckLake silently ignores the clause here; a
                    // silently ignored time-travel request is a
                    // wrong-answer trap, so this engine refuses instead.
                    return Err(PinkDbError::Catalog(format!(
                        "time travel is not available on the CTE \"{name}\" (AT ({at}) \
                             works on lake tables)"
                    )));
                }
                let eff = alias.clone().unwrap_or_else(|| name.clone());
                return Ok((
                    ScopeTable {
                        name: eff.to_ascii_lowercase(),
                        columns: schema.clone(),
                    },
                    LogicalPlan::Scan {
                        db: None,
                        table: name.clone(),
                        at: None,
                    },
                ));
            }
            let lookup_db = binder.resolve_db(qdb)?;
            if let Ok(view) = db.view_in(&lookup_db, name) {
                if binder
                    .params
                    .as_ref()
                    .is_some_and(|slots| slots.rejects_view_reads())
                {
                    return Err(PinkDbError::Execution(format!(
                        "a durable procedure cannot read the session-local view \"{name}\": \
                         views are never persisted, so the logged CALL could not replay — \
                         materialize the view into a table first"
                    )));
                }
                if let Some(at) = at {
                    return Err(PinkDbError::Catalog(format!(
                        "time travel is not available on the view \"{name}\" (AT ({at}) \
                         works on lake tables)"
                    )));
                }
                // Views rebind per use (like derived tables): the scope always
                // matches the freshly bound plan, so catalog drift surfaces
                // as errors, never stale indices. The body resolves
                // unqualified names in the view's own database.
                {
                    let mut used = binder.views_used.borrow_mut();
                    if !used.iter().any(|v| v.eq_ignore_ascii_case(name)) {
                        used.push(name.clone());
                    }
                }
                let view = view.clone();
                let mut child = binder.child();
                child.default_db = lookup_db.clone();
                // A view body is self-contained: the caller's CTEs must not
                // leak in (a caller CTE named like a table in the body would
                // silently shadow it; DuckDB resolves the table). The body's
                // own CTEs are bound below and scoped to it.
                child.ctes.clear();
                let sub = bind_select_in(child.db, &mut child, &view.query)?;
                let mut schema = plan_schema(child.db, &child.default_db, &child.ctes, &sub)?;
                if let Some(cols) = &view.cols {
                    for (s, c) in schema.iter_mut().zip(cols.iter()) {
                        s.0 = c.clone();
                    }
                }
                let eff = alias.clone().unwrap_or_else(|| name.clone());
                return Ok((
                    ScopeTable {
                        name: eff.to_ascii_lowercase(),
                        columns: schema,
                    },
                    sub,
                ));
            }
            // With an `AT` target the scope comes from the *version's*
            // descriptor (an attached lake reads that version's footer /
            // statistics block); without one, the ordinary visible schema.
            let columns = match at {
                None => db.visible_table_schema(&lookup_db, name)?,
                Some(at) => db.visible_table_schema_at(&lookup_db, name, at)?,
            };
            let eff = alias.clone().unwrap_or_else(|| name.clone());
            Ok((
                ScopeTable {
                    name: eff.to_ascii_lowercase(),
                    columns,
                },
                LogicalPlan::Scan {
                    db: Some(lookup_db),
                    table: name.clone(),
                    at,
                },
            ))
        }
        FromSource::Function { name, args } => {
            if let Some(at) = at {
                // The parser refuses `AT` after a function source; a
                // hand-built AST gets the same clean error.
                let _ = at;
                return Err(PinkDbError::Parse(
                    "AT is only valid after a table name".to_string(),
                ));
            }
            let args = bind_table_function_args(args)?;
            bind_table_function(db, name, &args, alias)
        }
        FromSource::Path(path) => {
            if let Some(at) = at {
                let _ = at;
                return Err(PinkDbError::Parse(
                    "AT is only valid after a table name".to_string(),
                ));
            }
            // DuckDB's replacement scan: the first registered source that
            // claims the path owns it.
            let call = TableFunctionCall::Path(path.clone());
            for source in db.table_sources() {
                if let Some(schema) = source.bind(&call)? {
                    let eff = alias.clone().unwrap_or_else(|| replacement_scan_name(path));
                    return Ok((
                        ScopeTable {
                            name: eff.to_ascii_lowercase(),
                            columns: schema.clone(),
                        },
                        LogicalPlan::TableFunction {
                            source: source.name().to_ascii_lowercase(),
                            call,
                            schema,
                        },
                    ));
                }
            }
            Err(PinkDbError::Catalog(format!(
                "cannot read \"{path}\" by path: {}",
                no_file_source_hint(db)
            )))
        }
        FromSource::Derived(q) => {
            if let Some(at) = at {
                // The parser refuses `(SELECT …) alias AT (…)`, so this is a
                // defensive path for a hand-built AST: a clean error, never a
                // silently ignored time-travel target.
                let _ = at;
                return Err(PinkDbError::Parse(
                    "AT is only valid after a table name".to_string(),
                ));
            }
            let alias = alias
                .clone()
                .ok_or_else(|| PinkDbError::Parse("derived table needs an alias".to_string()))?;
            let mut child = binder.child();
            let sub = bind_select_in(db, &mut child, q)?;
            let schema = plan_schema(db, &child.default_db, &binder.ctes, &sub)?;
            Ok((
                ScopeTable {
                    name: alias.to_ascii_lowercase(),
                    columns: schema,
                },
                sub,
            ))
        }
    }
}

/// Bind one SELECT branch (no UNION handling — callers use [`bind_select_in`]
/// for full SELECTs): WITH/CTE definitions + body, wrapped in Cte nodes.
fn bind_select_with(db: &PinkDb, binder: &mut Binder, sel: &Select) -> Result<LogicalPlan> {
    // Every full SELECT must route through `bind_select_in`; a direct call
    // here with UNION branches would silently drop all but the head branch.
    if !sel.unions.is_empty() {
        return Err(PinkDbError::Execution(
            "internal: UNION must be bound via bind_select_in".to_string(),
        ));
    }
    let cte_base = binder.ctes.len();
    let cte_plans = bind_ctes(db, binder, sel)?;
    let plan = bind_select_body(db, binder, sel)?;
    Ok(wrap_ctes(binder, cte_base, cte_plans, plan))
}

/// Bind the WITH clause of `sel`: each CTE in order (seeing previous +
/// ancestor CTEs), pushed onto `binder.ctes` so the caller's body — including
/// every UNION branch — can reference them. Pair with [`wrap_ctes`].
fn bind_ctes(db: &PinkDb, binder: &mut Binder, sel: &Select) -> Result<Vec<(String, LogicalPlan)>> {
    let mut cte_plans: Vec<(String, LogicalPlan)> = Vec::new();
    for cte in &sel.with {
        if binder
            .ctes
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case(&cte.name))
        {
            return Err(PinkDbError::Catalog(format!(
                "duplicate CTE name \"{}\"",
                cte.name
            )));
        }
        let mut child = binder.child();
        let query = bind_select_in(db, &mut child, &cte.query)?;
        let mut schema = plan_schema(db, &child.default_db, &binder.ctes, &query)?;
        if let Some(cols) = &cte.cols {
            if cols.len() != schema.len() {
                return Err(PinkDbError::Execution(format!(
                    "CTE \"{}\" has {} columns but {} aliases",
                    cte.name,
                    schema.len(),
                    cols.len()
                )));
            }
            for (s, c) in schema.iter_mut().zip(cols.iter()) {
                s.0 = c.clone();
            }
        }
        binder.ctes.push((cte.name.to_ascii_lowercase(), schema));
        cte_plans.push((cte.name.to_ascii_lowercase(), query));
    }
    Ok(cte_plans)
}

/// Wrap `plan` in the Cte nodes from [`bind_ctes`] (reverse order) and pop
/// the CTE scope back down to `cte_base`.
fn wrap_ctes(
    binder: &mut Binder,
    cte_base: usize,
    cte_plans: Vec<(String, LogicalPlan)>,
    plan: LogicalPlan,
) -> LogicalPlan {
    let mut plan = plan;
    for (name, query) in cte_plans.into_iter().rev() {
        plan = LogicalPlan::Cte {
            name,
            query: Box::new(query),
            body: Box::new(plan),
        };
    }
    binder.ctes.truncate(cte_base);
    plan
}

fn bind_select_body(db: &PinkDb, binder: &mut Binder, sel: &Select) -> Result<LogicalPlan> {
    // FROM (+ left-deep join chain). Returns the base plan and left width.
    let mut base: LogicalPlan;
    if let Some(from) = &sel.from {
        let (left, left_plan) = bind_source(db, binder, &from.source, &from.alias, from.at)?;
        let mut left_cols = left.columns.len();
        base = left_plan;
        binder.tables.push(left);
        for join in &from.joins {
            let (right, right_plan) = bind_source(db, binder, &join.source, &join.alias, join.at)?;
            if binder.tables.iter().any(|t| t.name == right.name) {
                return Err(PinkDbError::Catalog(format!(
                    "table name \"{}\" specified more than once (use an alias)",
                    right.name
                )));
            }
            // The right scope joins the accumulated scope before the ON
            // condition binds, so `ON` sees the left tables ++ right table
            // (joined-space column indices).
            binder.tables.push(right);
            let shape = bind_join_shape(binder, join.kind, left_cols, join.on.as_ref())?;
            base = match shape {
                BoundJoin::Hash {
                    left_keys,
                    right_keys,
                    residual,
                } => {
                    let mut join = LogicalPlan::Join {
                        kind: join.kind,
                        left: Box::new(base),
                        right: Box::new(right_plan),
                        left_keys,
                        right_keys,
                        left_cols,
                    };
                    if let Some(predicate) = residual {
                        join = LogicalPlan::Filter {
                            input: Box::new(join),
                            predicate,
                        };
                    }
                    join
                }
                BoundJoin::Nested { on } => LogicalPlan::NestedLoopJoin {
                    kind: join.kind,
                    left: Box::new(base),
                    right: Box::new(right_plan),
                    on,
                    left_cols,
                },
            };
            left_cols += binder.tables.last().expect("pushed above").columns.len();
        }
    } else {
        base = LogicalPlan::SingleRow;
    }

    for proj in &sel.projections {
        if let Projection::Expr {
            expr,
            alias: Some(a),
        } = proj
        {
            binder.aliases.insert(a.to_ascii_lowercase(), expr.clone());
        }
    }

    // WHERE (aggregates forbidden; aliases NOT visible here — standard SQL
    // resolves WHERE against table columns so aliases never shadow them).
    let mut where_aggs = Vec::new();
    binder.allow_alias = false;
    binder.allow_win = false;
    let filter = sel
        .where_clause
        .as_ref()
        .map(|e| binder.bind_expr(e, &mut where_aggs, false))
        .transpose()?;
    binder.allow_alias = true;
    binder.allow_win = true;
    debug_assert!(where_aggs.is_empty());
    if let Some(f) = &filter
        && f.dtype() != DataType::Boolean
    {
        return Err(PinkDbError::Type(
            "WHERE needs a BOOLEAN expression".to_string(),
        ));
    }

    // Expand `*` / `t.*`, then bind select items (aggregates flow into `aggs`).
    let mut pending: Vec<(Expr, String)> = Vec::new();
    for proj in &sel.projections {
        match proj {
            Projection::Star => {
                for t in &binder.tables {
                    // Qualify: with two tables in scope, bare names are ambiguous.
                    let q = t.name.clone();
                    for (name, _) in &t.columns.clone() {
                        pending.push((
                            Expr::Column {
                                table: Some(q.clone()),
                                name: name.clone(),
                            },
                            name.clone(),
                        ));
                    }
                }
            }
            Projection::QualifiedStar(q) => {
                let cols = binder
                    .tables
                    .iter()
                    .find(|t| t.name.eq_ignore_ascii_case(q))
                    .map(|t| t.columns.clone())
                    .ok_or_else(|| PinkDbError::Catalog(format!("no table \"{q}\" for `{q}.*`")))?;
                for (name, _) in &cols {
                    pending.push((
                        Expr::Column {
                            table: Some(q.clone()),
                            name: name.clone(),
                        },
                        name.clone(),
                    ));
                }
            }
            Projection::Expr { expr, alias } => {
                pending.push((
                    expr.clone(),
                    alias.clone().unwrap_or_else(|| default_name(expr)),
                ));
            }
        }
    }
    if pending.is_empty() {
        return Err(PinkDbError::Parse(
            "SELECT needs at least one projection".to_string(),
        ));
    }

    let mut aggs: Vec<BoundAgg> = Vec::new();
    let mut select: Vec<(BoundExpr, String)> = Vec::with_capacity(pending.len());
    // Aliases from this SELECT list are not visible to the other items in the
    // same list (standard SQL exposes them only to ORDER BY / GROUP BY /
    // HAVING), so `SELECT id*2 AS id, id` resolves the second `id` to the
    // input column, not the alias.
    let saved_alias = binder.allow_alias;
    binder.allow_alias = false;
    for (e, name) in &pending {
        select.push((binder.bind_expr(e, &mut aggs, true)?, name.clone()));
    }
    binder.allow_alias = saved_alias;

    // GROUP BY keys (pre-agg scope, no aggregates, no windows).
    let mut keys: Vec<BoundExpr> = Vec::new();
    binder.allow_win = false;
    for g in &sel.group_by {
        if let Expr::Literal(Value::Integer(n)) = g {
            let idx = (*n as usize).checked_sub(1).ok_or_else(|| {
                PinkDbError::Execution("GROUP BY position must be >= 1".to_string())
            })?;
            let (ast_expr, _) = pending.get(idx).ok_or_else(|| {
                PinkDbError::Execution(format!("GROUP BY position {n} out of range"))
            })?;
            let mut no_aggs = Vec::new();
            let pre = binder.bind_expr(ast_expr, &mut no_aggs, false)?;
            if !no_aggs.is_empty() {
                return Err(PinkDbError::Execution(
                    "GROUP BY position refers to an aggregate".to_string(),
                ));
            }
            keys.push(pre);
            continue;
        }
        let mut no_aggs = Vec::new();
        keys.push(binder.bind_expr(g, &mut no_aggs, false)?);
    }
    binder.allow_win = true;

    // HAVING forces aggregation (single-group when no GROUP BY).
    let is_agg = !aggs.is_empty() || !keys.is_empty() || sel.having.is_some();
    if is_agg {
        for (e, _) in &select {
            validate_grouped(e, &keys)?;
        }
    }
    let having = sel
        .having
        .as_ref()
        .map(|e| {
            binder.allow_win = false;
            let h = binder.bind_expr(e, &mut aggs, true);
            binder.allow_win = true;
            h
        })
        .transpose()?;
    if let Some(h) = &having {
        if h.dtype() != DataType::Boolean {
            return Err(PinkDbError::Type(
                "HAVING needs a BOOLEAN expression".to_string(),
            ));
        }
        // HAVING without aggregation (`SELECT a ... HAVING ...`) is rejected:
        // without GROUP BY, fragments can't validate.
        validate_grouped(h, &keys)?;
    }

    // Windows collected from the SELECT list. Their args/partition/order
    // live in the same scope as select items: validate + rebase them alike.
    let mut windows = std::mem::take(&mut binder.windows);
    if is_agg {
        for w in &windows {
            if let Some(a) = &w.arg {
                validate_grouped(a, &keys)?;
            }
            for p in &w.partition {
                validate_grouped(p, &keys)?;
            }
            for (o, _) in &w.order {
                validate_grouped(o, &keys)?;
            }
        }
    }

    // Rebase relative AggRefs past the group keys (dedupe only appends,
    // so bind-time indices stay valid; HAVING reuses SELECT's agg slots).
    let num_keys = keys.len();
    let mut select: Vec<(BoundExpr, String)> = select
        .into_iter()
        .map(|(e, n)| (resolve_agg_refs(e, num_keys), n))
        .collect();
    let mut having = having.map(|h| resolve_agg_refs(h, num_keys));
    for w in windows.iter_mut() {
        w.arg = w.arg.take().map(|a| resolve_agg_refs(a, num_keys));
        w.partition = std::mem::take(&mut w.partition)
            .into_iter()
            .map(|p| resolve_agg_refs(p, num_keys))
            .collect();
        w.order = std::mem::take(&mut w.order)
            .into_iter()
            .map(|(o, d)| (resolve_agg_refs(o, num_keys), d))
            .collect();
    }
    // Agg args stay scan-relative (Aggregate evaluates them pre-aggregation).
    // Select/having address Aggregate output [keys..., aggs...]: rebase their
    // scan-relative Columns onto key positions. Validated above, so this is
    // infallible.
    if is_agg {
        select = select
            .into_iter()
            .map(|(e, n)| (rebase_to_keys(&e, &keys).expect("validated"), n))
            .collect();
        having = having.map(|h| rebase_to_keys(&h, &keys).expect("validated"));
        for w in windows.iter_mut() {
            // Window fields address the Aggregate output too.
            if let Some(a) = w.arg.take() {
                w.arg = Some(rebase_to_keys(&a, &keys).expect("validated"));
            }
            w.partition = std::mem::take(&mut w.partition)
                .into_iter()
                .map(|p| rebase_to_keys(&p, &keys).expect("validated"))
                .collect();
            w.order = std::mem::take(&mut w.order)
                .into_iter()
                .map(|(o, d)| (rebase_to_keys(&o, &keys).expect("validated"), d))
                .collect();
        }
    }
    let keys_for_order = keys.clone();

    // Scan / join → filter → aggregate → having-filter → window.
    let mut plan = base;
    if let Some(pred) = filter {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: pred,
        };
    }
    if is_agg {
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            keys,
            aggs,
        };
        if let Some(h) = having {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: h,
            };
        }
    }
    // Windows see the aggregate output (or the scan). Their results append
    // after `input_cols` columns; select items rebase onto those positions.
    // (HAVING sits below, so it never holds WindowRefs.)
    let input_cols = match &plan {
        LogicalPlan::Aggregate { keys, aggs, .. } => keys.len() + aggs.len(),
        LogicalPlan::Filter { input, .. } => match &**input {
            LogicalPlan::Aggregate { keys, aggs, .. } => keys.len() + aggs.len(),
            _ => scan_width(binder),
        },
        _ => scan_width(binder),
    };
    if !windows.is_empty() {
        plan = LogicalPlan::Window {
            input: Box::new(plan),
            windows,
            input_cols,
        };
    }
    // NOTE: select items are rebound below (resolve_window_refs) once the
    // projection is built — see the projection section.

    // Projection, plus hidden ORDER BY columns pruned after the sort.
    let visible = select.len();
    let visible_dtypes: Vec<DataType> = select.iter().map(|(e, _)| e.dtype()).collect();
    let visible_names: Vec<String> = select.iter().map(|(_, n)| n.clone()).collect();
    let mut proj_exprs: Vec<BoundExpr> = select
        .into_iter()
        .map(|(e, _)| resolve_window_refs(e, input_cols))
        .collect();
    let mut proj_names = visible_names.clone();
    let mut hidden = 0usize;
    let mut sort_keys: Vec<(BoundExpr, SortSpec)> = Vec::new();
    for item in &sel.order_by {
        // Positional `ORDER BY 2` → Nth visible output.
        if let Expr::Literal(Value::Integer(n)) = &item.expr {
            let idx = (*n as usize).checked_sub(1).ok_or_else(|| {
                PinkDbError::Execution("ORDER BY position must be >= 1".to_string())
            })?;
            if idx >= visible {
                return Err(PinkDbError::Execution(format!(
                    "ORDER BY position {n} out of range (only {visible} select items)"
                )));
            }
            sort_keys.push((
                BoundExpr::Column {
                    index: idx,
                    dtype: visible_dtypes[idx],
                },
                SortSpec::of(item),
            ));
            continue;
        }
        // Select alias → reuse that output (no hidden column).
        if let Expr::Column { table: None, name } = &item.expr
            && let Some(pos) = proj_names[..visible]
                .iter()
                .position(|n| n.eq_ignore_ascii_case(name))
        {
            sort_keys.push((
                BoundExpr::Column {
                    index: pos,
                    dtype: visible_dtypes[pos],
                },
                SortSpec::of(item),
            ));
            continue;
        }
        // General expression → hidden sort column. It binds in scan scope;
        // in agg queries it is rebased onto Aggregate-output scope (key
        // positions). Aggregate-free in v1: aggregates ride via alias/position.
        let mut no_aggs = Vec::new();
        binder.allow_win = false;
        let bound = binder.bind_expr(&item.expr, &mut no_aggs, false)?;
        binder.allow_win = true;
        if !no_aggs.is_empty() {
            return Err(PinkDbError::Execution(
                "aggregates in ORDER BY must use a select alias or position in v1".to_string(),
            ));
        }
        let scoped = if is_agg {
            rebase_to_keys(&bound, &keys_for_order).ok_or_else(|| {
                PinkDbError::Execution(
                    "ORDER BY expression must be an alias, position, or GROUP BY key in v1"
                        .to_string(),
                )
            })?
        } else {
            bound
        };
        // Reuse an identical projected expression when possible.
        match proj_exprs.iter().position(|p| p == &scoped) {
            Some(idx) => {
                let dtype = proj_exprs[idx].dtype();
                sort_keys.push((BoundExpr::Column { index: idx, dtype }, SortSpec::of(item)));
            }
            None => {
                let dtype = scoped.dtype();
                proj_exprs.push(scoped);
                proj_names.push(format!("__order{hidden}"));
                hidden += 1;
                sort_keys.push((
                    BoundExpr::Column {
                        index: proj_exprs.len() - 1,
                        dtype,
                    },
                    SortSpec::of(item),
                ));
            }
        }
    }

    plan = LogicalPlan::Project {
        input: Box::new(plan),
        exprs: proj_exprs,
        names: proj_names,
    };
    if sel.distinct {
        if hidden > 0 {
            return Err(PinkDbError::Execution(
                "ORDER BY expression must appear in the select list with DISTINCT".to_string(),
            ));
        }
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
        };
    }
    if !sort_keys.is_empty() {
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            keys: sort_keys,
        };
        if hidden > 0 {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                exprs: (0..visible)
                    .map(|i| BoundExpr::Column {
                        index: i,
                        dtype: visible_dtypes[i],
                    })
                    .collect(),
                names: visible_names,
            };
        }
    }
    if let Some(n) = sel.offset {
        plan = LogicalPlan::Offset {
            input: Box::new(plan),
            offset: n,
        };
    }
    if let Some(n) = sel.limit {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            limit: n,
        };
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::super::parse_statements;
    use super::*;

    fn test_db() -> PinkDb {
        let mut db = PinkDb::new();
        db.create_table(
            "sales",
            vec![
                ("id".to_string(), DataType::Integer),
                ("region".to_string(), DataType::Varchar),
                ("total".to_string(), DataType::Double),
            ],
        )
        .unwrap();
        db
    }

    fn bind_one(db: &PinkDb, sql: &str) -> Result<LogicalPlan> {
        let stmts = parse_statements(sql).unwrap();
        bind(db, &stmts[0], "main")
    }

    fn bind_in(db: &PinkDb, sql: &str, default_db: &str) -> Result<LogicalPlan> {
        let stmts = parse_statements(sql).unwrap();
        bind(db, &stmts[0], default_db)
    }

    /// The database of the first `Scan` under the top projection.
    fn scan_db(plan: &LogicalPlan) -> Option<String> {
        match plan {
            LogicalPlan::Project { input, .. } => scan_db(input),
            LogicalPlan::Cte { body, .. } => scan_db(body),
            LogicalPlan::Scan { db, .. } => db.clone(),
            other => panic!("no scan in plan: {other:?}"),
        }
    }

    #[test]
    fn star_expands() {
        let db = test_db();
        let plan = bind_one(&db, "SELECT * FROM sales;").unwrap();
        match plan {
            LogicalPlan::Project { names, .. } => {
                assert_eq!(names, vec!["id", "region", "total"]);
            }
            _ => panic!("{plan:?}"),
        }
    }

    #[test]
    fn unknown_column_errors() {
        let db = test_db();
        assert!(bind_one(&db, "SELECT nope FROM sales;").is_err());
        assert!(bind_one(&db, "SELECT * FROM missing;").is_err());
    }

    #[test]
    fn agg_in_where_rejected() {
        let db = test_db();
        assert!(bind_one(&db, "SELECT id FROM sales WHERE SUM(total) > 1;").is_err());
    }

    #[test]
    fn group_validation() {
        let db = test_db();
        assert!(bind_one(&db, "SELECT region, SUM(total) FROM sales GROUP BY region;").is_ok());
        assert!(bind_one(&db, "SELECT id, SUM(total) FROM sales GROUP BY region;").is_err());
        assert!(bind_one(&db, "SELECT SUM(total) FROM sales;").is_ok());
    }

    #[test]
    fn between_desugars() {
        let db = test_db();
        let plan = bind_one(&db, "SELECT id FROM sales WHERE total BETWEEN 1 AND 5;").unwrap();
        assert!(matches!(plan, LogicalPlan::Project { .. }));
    }

    #[test]
    fn join_binding() {
        let db = test_db();
        // Self-join without aliases is rejected (name used twice).
        let err = bind_one(
            &db,
            "SELECT * FROM sales JOIN sales ON sales.id = sales.id;",
        )
        .unwrap_err();
        assert!(matches!(err, PinkDbError::Catalog(_)));
        // Non-equi ON binds to a nested-loop join.
        let plan = bind_one(&db, "SELECT * FROM sales s JOIN sales o ON s.id > o.id;").unwrap();
        assert!(
            has_nested_loop_join(&plan),
            "expected a nested-loop join: {plan:?}"
        );
        // Composite equi ON binds to a multi-key hash join.
        let plan = bind_one(
            &db,
            "SELECT * FROM sales s JOIN sales o ON s.id = o.id AND s.region = o.region;",
        )
        .unwrap();
        assert!(has_multi_key_join(&plan), "expected a 2-key hash join");
        // Aliased equi-join binds.
        let plan = bind_one(&db, "SELECT * FROM sales s JOIN sales o ON s.id = o.id;").unwrap();
        assert!(matches!(plan, LogicalPlan::Project { .. }));
    }

    /// True when `plan` (recursively, through the projection) holds a
    /// nested-loop join.
    fn has_nested_loop_join(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::NestedLoopJoin { .. } => true,
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Limit { input, .. } => has_nested_loop_join(input),
            _ => false,
        }
    }

    fn has_multi_key_join(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Join { left_keys, .. } => left_keys.len() == 2,
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Limit { input, .. } => has_multi_key_join(input),
            _ => false,
        }
    }

    /// A misrouted call with UNION branches must fail loudly instead of
    /// silently dropping every branch after the head.
    #[test]
    fn union_branches_must_bind_via_bind_select_in() {
        let db = test_db();
        let stmts =
            parse_statements("SELECT id FROM sales UNION ALL SELECT id FROM sales;").unwrap();
        let Statement::Select(sel) = &stmts[0] else {
            panic!("expected SELECT")
        };
        let mut binder = Binder::new(&db, "main");
        let err = bind_select_with(&db, &mut binder, sel).unwrap_err();
        assert!(matches!(err, PinkDbError::Execution(_)), "{err:?}");
        assert!(err.to_string().contains("bind_select_in"), "{err}");
    }

    /// Two databases with a same-named table of different shapes.
    fn multi_db() -> PinkDb {
        let mut db = PinkDb::new();
        db.create_database("db1").unwrap();
        db.create_table("t", vec![("a".to_string(), DataType::Integer)])
            .unwrap();
        db.create_table_in(
            "db1",
            "t",
            vec![
                ("a".to_string(), DataType::Integer),
                ("b".to_string(), DataType::Varchar),
            ],
        )
        .unwrap();
        db
    }

    #[test]
    fn unqualified_names_resolve_against_the_session_database() {
        let db = multi_db();
        // `main.t` has one column; `db1.t` has two.
        let main_plan = bind_in(&db, "SELECT * FROM t;", "main").unwrap();
        assert_eq!(scan_db(&main_plan).as_deref(), Some("main"));
        assert_eq!(plan_schema(&db, "main", &[], &main_plan).unwrap().len(), 1);
        let db1_plan = bind_in(&db, "SELECT * FROM t;", "db1").unwrap();
        assert_eq!(scan_db(&db1_plan).as_deref(), Some("db1"));
        assert_eq!(plan_schema(&db, "db1", &[], &db1_plan).unwrap().len(), 2);
        // A column that only exists in db1.t fails when bound on main.
        assert!(bind_in(&db, "SELECT b FROM t;", "main").is_err());
        assert!(bind_in(&db, "SELECT b FROM t;", "db1").is_ok());
    }

    #[test]
    fn qualified_names_resolve_independently_of_the_session() {
        let db = multi_db();
        for session in ["main", "db1"] {
            let plan = bind_in(&db, "SELECT a FROM db1.t;", session).unwrap();
            assert_eq!(scan_db(&plan).as_deref(), Some("db1"));
        }
        let plan = bind_in(&db, "SELECT a FROM main.t;", "db1").unwrap();
        assert_eq!(scan_db(&plan).as_deref(), Some("main"));
        // Unknown database / table errors are clear.
        let err = bind_in(&db, "SELECT * FROM nope.t;", "main").unwrap_err();
        assert!(
            err.to_string().contains("database \"nope\" not found"),
            "{err}"
        );
        let err = bind_in(&db, "SELECT * FROM db1.missing;", "main").unwrap_err();
        assert!(
            err.to_string().contains("table \"missing\" not found"),
            "{err}"
        );
    }

    #[test]
    fn ctes_shadow_unqualified_names_only() {
        let db = multi_db();
        // The CTE shadows db1.t (the session's table).
        let plan = bind_in(&db, "WITH t AS (SELECT 1 AS x) SELECT x FROM t;", "db1").unwrap();
        match &plan {
            LogicalPlan::Cte { query, body, .. } => {
                assert!(matches!(**query, LogicalPlan::Project { .. }));
                assert_eq!(scan_db(body), None, "body scans the CTE");
            }
            other => panic!("{other:?}"),
        }
        // A qualified name never resolves to a CTE.
        let plan = bind_in(
            &db,
            "WITH t AS (SELECT 1 AS x) SELECT a FROM db1.t;",
            "main",
        )
        .unwrap();
        assert_eq!(scan_db(&plan).as_deref(), Some("db1"));
    }

    #[test]
    fn use_and_database_ddl_bind() {
        let db = multi_db();
        assert!(matches!(
            bind_one(&db, "USE db1;").unwrap(),
            LogicalPlan::Use { name } if name == "db1"
        ));
        assert!(matches!(
            bind_one(&db, "USE MAIN;").unwrap(),
            LogicalPlan::Use { .. }
        ));
        let err = bind_one(&db, "USE nope;").unwrap_err();
        assert!(
            err.to_string().contains("database \"nope\" not found"),
            "{err}"
        );
        assert!(matches!(
            bind_one(&db, "CREATE DATABASE db2;").unwrap(),
            LogicalPlan::CreateDatabase { name, .. } if name == "db2"
        ));
        assert!(matches!(
            bind_one(&db, "DROP DATABASE db1;").unwrap(),
            LogicalPlan::DropDatabase { name, .. } if name == "db1"
        ));
    }

    #[test]
    fn qualified_ddl_and_dml_bind_to_the_right_database() {
        let db = multi_db();
        assert!(matches!(
            bind_one(&db, "CREATE TABLE db1.u (x BIGINT);").unwrap(),
            LogicalPlan::CreateTable { db: Some(d), name, .. } if d == "db1" && name == "u"
        ));
        assert!(matches!(
            bind_one(&db, "INSERT INTO db1.t VALUES (1, 'x');").unwrap(),
            LogicalPlan::Insert { db: Some(d), .. } if d == "db1"
        ));
        assert!(matches!(
            bind_one(&db, "UPDATE db1.t SET a = 2;").unwrap(),
            LogicalPlan::Update { db: Some(d), .. } if d == "db1"
        ));
        assert!(matches!(
            bind_one(&db, "DELETE FROM db1.t;").unwrap(),
            LogicalPlan::Delete { db: Some(d), .. } if d == "db1"
        ));
        assert!(matches!(
            bind_one(&db, "DROP TABLE db1.t;").unwrap(),
            LogicalPlan::DropTable { db: Some(d), .. } if d == "db1"
        ));
        // Unqualified DDL/DML resolves to the session database.
        assert!(matches!(
            bind_in(&db, "INSERT INTO t VALUES (1, 'x');", "db1").unwrap(),
            LogicalPlan::Insert { db: Some(d), .. } if d == "db1"
        ));
        // A DML statement against a missing database is a clean error.
        let err = bind_one(&db, "INSERT INTO nope.t VALUES (1);").unwrap_err();
        assert!(err.to_string().contains("database \"nope\""), "{err}");
    }

    #[test]
    fn views_are_per_database_and_resolve_their_body_there() {
        let mut db = multi_db();
        // A view in db1 over db1.t; db1.t and main.t differ in shape.
        db.create_table_in("db1", "src", vec![("v".to_string(), DataType::Bigint)])
            .unwrap();
        db.create_view_in(
            "db1",
            "v",
            {
                let stmts = parse_statements("SELECT v FROM src;").unwrap();
                let Statement::Select(sel) = &stmts[0] else {
                    panic!()
                };
                sel.clone()
            },
            None,
        )
        .unwrap();
        // Found in db1: body resolves `src` in db1.
        assert!(bind_in(&db, "SELECT * FROM v;", "db1").is_ok());
        assert!(bind_in(&db, "SELECT * FROM db1.v;", "main").is_ok());
        // `v` is not visible from main, and `src` does not exist there.
        let err = bind_in(&db, "SELECT * FROM v;", "main").unwrap_err();
        assert!(err.to_string().contains("table \"v\" not found"), "{err}");
    }
}
