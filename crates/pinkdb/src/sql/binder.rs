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
use crate::plan::LogicalPlan;
use crate::storage::{DataTable, PinkDb};
use crate::types::{DataType, PinkDbError, Result, Value};

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
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundAgg {
    pub func: AggFunc,
    /// COUNT(*) has no arg.
    pub arg: Option<BoundExpr>,
    pub dtype: DataType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
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
    Rank(WindowFunc),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundFrame {
    pub start: FrameBound,
    pub end: FrameBound,
}

impl BoundFrame {
    /// Default frame: whole partition, or cumulative when ordered.
    fn default_for(ordered: bool) -> Self {
        if ordered {
            Self {
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::CurrentRow,
            }
        } else {
            Self {
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::UnboundedFollowing,
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundWindow {
    pub func: WindowFunc,
    /// COUNT(*) has no arg.
    pub arg: Option<BoundExpr>,
    pub partition: Vec<BoundExpr>,
    pub order: Vec<(BoundExpr, bool)>,
    pub frame: BoundFrame,
    pub dtype: DataType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpr {
    Literal(Value),
    /// Column of the current stage input (scan, agg output, or project output).
    Column { index: usize, dtype: DataType },
    Binary {
        op: BinaryOp,
        left: Box<BoundExpr>,
        right: Box<BoundExpr>,
        dtype: DataType,
    },
    Not(Box<BoundExpr>),
    Negate { expr: Box<BoundExpr>, dtype: DataType },
    IsNull { expr: Box<BoundExpr>, negated: bool },
    Scalar { func: ScalarFunc, args: Vec<BoundExpr>, dtype: DataType },
    /// Aggregate output column. Index is relative to the agg list until
    /// [`resolve_agg_refs`] rebases it past the group keys.
    AggRef { index: usize, dtype: DataType },
    /// Window output column. Index is relative to the window list until
    /// rebased past the window input columns.
    WindowRef { index: usize, dtype: DataType },
    /// `CAST(x AS type)`.
    Cast { expr: Box<BoundExpr>, dtype: DataType },
    /// `COALESCE(a, b, ...)` — all args share `dtype` (narrower sides are
    /// wrapped in [`BoundExpr::Cast`] at bind time).
    Coalesce { args: Vec<BoundExpr>, dtype: DataType },
    /// `CASE WHEN c THEN v ... [ELSE e]` — results share `dtype`
    /// (narrower sides wrapped in [`BoundExpr::Cast`]); missing ELSE is NULL.
    Case {
        branches: Vec<(BoundExpr, BoundExpr)>,
        else_: Option<Box<BoundExpr>>,
        dtype: DataType,
    },
    /// Uncorrelated scalar subquery (1 col by construction; rows at runtime).
    ScalarSubquery { query: Box<LogicalPlan>, dtype: DataType },
    /// `expr [NOT] IN (query)` — `expr` may reference the outer scope.
    InSubquery { expr: Box<BoundExpr>, query: Box<LogicalPlan>, negated: bool },
    /// `[NOT] EXISTS (query)` — scope-independent.
    Exists { query: Box<LogicalPlan>, negated: bool },
    /// Call into a scalar function registered by a loaded extension
    /// (`PinkDb::load_extension`). `name` is the canonical lowercased
    /// registry key; args and result are typed exactly as registered (no
    /// implicit casts in ABI v1). Never constant-folded (an extension
    /// callback is opaque: it may be impure and may fail).
    ///
    /// `Box<str>` + `Box<[BoundExpr]>` (not `String` + `Vec`) keep this
    /// variant no larger than the biggest pre-existing one, so
    /// `size_of::<BoundExpr>()` — and with it every expression tree and the
    /// hot match dispatch on the static path — is unchanged.
    DynamicScalar { name: Box<str>, args: Box<[BoundExpr]>, dtype: DataType },
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
            BoundExpr::Case { branches, else_, .. } => {
                branches.iter().all(|(c, v)| c.is_aggregate_free() && v.is_aggregate_free())
                    && else_.as_ref().map(|e| e.is_aggregate_free()).unwrap_or(true)
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
            BoundExpr::DynamicScalar { args, .. } => {
                args.iter().all(|a| a.is_aggregate_free())
            }
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
            BoundExpr::Case { branches, else_, .. } => {
                branches.iter().any(|(c, v)| c.has_column_ref() || v.has_column_ref())
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
            BoundExpr::Binary { left, right, .. } => {
                left.has_subquery() || right.has_subquery()
            }
            BoundExpr::Scalar { args, .. } => args.iter().any(|a| a.has_subquery()),
            BoundExpr::DynamicScalar { args, .. } => args.iter().any(|a| a.has_subquery()),
            BoundExpr::Coalesce { args, .. } => args.iter().any(|a| a.has_subquery()),
            BoundExpr::Case { branches, else_, .. } => {
                branches.iter().any(|(c, v)| c.has_subquery() || v.has_subquery())
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
            BoundExpr::Scalar { args, .. } => {
                args.iter().any(|a| a.contains_dynamic_scalar())
            }
            BoundExpr::Coalesce { args, .. } => {
                args.iter().any(|a| a.contains_dynamic_scalar())
            }
            BoundExpr::Case { branches, else_, .. } => {
                branches.iter().any(|(c, v)| {
                    c.contains_dynamic_scalar() || v.contains_dynamic_scalar()
                }) || else_
                    .as_ref()
                    .map(|e| e.contains_dynamic_scalar())
                    .unwrap_or(false)
            }
        }
    }
}

// ---------- type rules ----------

fn is_numeric(t: DataType) -> bool {
    matches!(t, DataType::Integer | DataType::Bigint | DataType::Double)
}

/// INTEGER → BIGINT → DOUBLE.
pub(crate) fn common_numeric(a: DataType, b: DataType) -> Result<DataType> {
    if !is_numeric(a) || !is_numeric(b) {
        return Err(PinkDbError::Type(format!(
            "non-numeric operands for arithmetic: {a} and {b}"
        )));
    }
    Ok(match (a, b) {
        (DataType::Double, _) | (_, DataType::Double) => DataType::Double,
        (DataType::Bigint, _) | (_, DataType::Bigint) => DataType::Bigint,
        _ => DataType::Integer,
    })
}

fn arithmetic_dtype(op: BinaryOp, l: DataType, r: DataType) -> Result<DataType> {
    // Dates: `date ± integer` is day arithmetic; `date - date` counts days
    // (BIGINT). Timestamps never mix with integers in v1 (that needs
    // INTERVAL types).
    match (op, l, r) {
        (BinaryOp::Add, DataType::Date, DataType::Integer)
        | (BinaryOp::Add, DataType::Integer, DataType::Date)
        | (BinaryOp::Subtract, DataType::Date, DataType::Integer) => {
            return Ok(DataType::Date)
        }
        (BinaryOp::Subtract, DataType::Date, DataType::Date) => {
            return Ok(DataType::Bigint)
        }
        _ => {}
    }
    if l == DataType::Date
        || r == DataType::Date
        || l == DataType::Timestamp
        || r == DataType::Timestamp
    {
        return Err(PinkDbError::Type(format!(
            "operator {op:?} does not support {l} and {r} in v1"
        )));
    }
    match op {
        BinaryOp::Divide => {
            // DuckDB rule: int / int → DOUBLE.
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
                return Err(PinkDbError::Type("`%` needs integer operands in v1".to_string()));
            }
            common_numeric(l, r)
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
    common_numeric(a, b).map_err(|_| {
        PinkDbError::Type(format!("incompatible types {a} and {b} (use CAST)"))
    })
}

/// Whether `CAST(from AS to)` is allowed in v1.
fn check_cast(from: DataType, to: DataType) -> Result<()> {
    if from == to {
        return Ok(());
    }
    // Numeric tower (both directions; float→int truncates).
    if from.is_numeric() && to.is_numeric() {
        return Ok(());
    }
    // Everything formats to VARCHAR; VARCHAR parses to everything.
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
    Err(PinkDbError::Type(format!("cannot CAST {from} AS {to} in v1")))
}

fn comparison_ok(op: BinaryOp, l: DataType, r: DataType) -> Result<()> {    if (is_numeric(l) && is_numeric(r))
        || (l == DataType::Varchar && r == DataType::Varchar)
        || (l == DataType::Date && r == DataType::Date)
        || (l == DataType::Timestamp && r == DataType::Timestamp)
    {
        return match op {
            // LIKE only compares strings.
            BinaryOp::Like | BinaryOp::NotLike
                if l != DataType::Varchar || r != DataType::Varchar =>
            {
                Err(PinkDbError::Type("LIKE needs VARCHAR on both sides".to_string()))
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
        }
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
        }
    }

    /// Resolve a statement's table reference to a concrete database: an
    /// explicit qualifier must name an existing database; otherwise the
    /// session's default database is used.
    fn resolve_db(&self, db: &Option<String>) -> Result<String> {
        match db {
            Some(q) => {
                if !self.db.database_exists(q) {
                    return Err(PinkDbError::Catalog(format!(
                        "database \"{q}\" not found"
                    )));
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
            if let Some(q) = table {
                if !t.name.eq_ignore_ascii_case(q) {
                    base += t.columns.len();
                    continue;
                }
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
            Expr::Star => Err(PinkDbError::Parse(
                "`*` is only valid in the projection list or COUNT(*)".to_string(),
            )),
            Expr::QualifiedStar(_) => Err(PinkDbError::Parse(
                "`t.*` is only valid in the projection list".to_string(),
            )),
            Expr::Column { table, name } => {
                // Alias substitution (`... ORDER BY x`), with a cycle guard so
                // `SELECT x AS x` resolves to the column instead of recursing.
                if table.is_none() && self.allow_alias {
                    let key = name.to_ascii_lowercase();
                    if !self.resolving.contains(&key) {
                        if let Some(aliased) = self.aliases.get(&key).cloned() {
                            self.resolving.push(key);
                            let bound = self.bind_expr(&aliased, aggs, allow_agg);
                            self.resolving.pop();
                            return bound;
                        }
                    }
                }
                let (index, dtype) = self.lookup_column(table.as_deref(), name)?;
                Ok(BoundExpr::Column { index, dtype })
            }
            Expr::Binary { op, left, right } => {
                let l = self.bind_expr(left, aggs, allow_agg)?;
                let r = self.bind_expr(right, aggs, allow_agg)?;
                let dtype = match op {
                    BinaryOp::And | BinaryOp::Or => {
                        if l.dtype() != DataType::Boolean || r.dtype() != DataType::Boolean {
                            return Err(PinkDbError::Type(
                                "AND/OR need BOOLEAN operands".to_string(),
                            ));
                        }
                        DataType::Boolean
                    }
                    BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq
                    | BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Like | BinaryOp::NotLike => {
                        comparison_ok(*op, l.dtype(), r.dtype())?;
                        DataType::Boolean
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
                        Ok(BoundExpr::Negate { expr: Box::new(e), dtype })
                    }
                    UnaryOp::Plus => Ok(e),
                }
            }
            Expr::IsNull { expr, negated } => {
                let e = self.bind_expr(expr, aggs, allow_agg)?;
                Ok(BoundExpr::IsNull { expr: Box::new(e), negated: *negated })
            }
            Expr::Between { expr, low, high, negated } => {
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
            Expr::Function { name, args } => self.bind_function(name, args, aggs, allow_agg),
            Expr::Window { name, args, over } => self.bind_window(name, args, over, aggs, allow_agg),
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
                        if b.dtype() == common
                            || matches!(b, BoundExpr::Literal(Value::Null))
                        {
                            b
                        } else {
                            BoundExpr::Cast { expr: Box::new(b), dtype: common }
                        }
                    })
                    .collect();
                Ok(BoundExpr::Coalesce { args, dtype: common })
            }
            Expr::Cast { expr, dtype } => {
                let bound = self.bind_expr(expr, aggs, allow_agg)?;
                check_cast(bound.dtype(), *dtype)?;
                Ok(BoundExpr::Cast { expr: Box::new(bound), dtype: *dtype })
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
                            BoundExpr::Cast { expr: Box::new(v), dtype: common }
                        };
                        (c, v)
                    })
                    .collect();
                let else_ = else_bound.map(|e| {
                    Box::new(if e.dtype() == common {
                        e
                    } else {
                        BoundExpr::Cast { expr: Box::new(e), dtype: common }
                    })
                });
                Ok(BoundExpr::Case { branches, else_, dtype: common })
            }
            Expr::ScalarSubquery(q) => {
                let mut child = self.child();
                let query = bind_select_in(child.db, &mut child, q)?;
                let schema =
                    plan_schema(child.db, &child.default_db, &child.ctes, &query)?;
                if schema.len() != 1 {
                    return Err(PinkDbError::Execution(
                        "scalar subquery must return exactly one column".to_string(),
                    ));
                }
                let dtype = schema[0].1;
                Ok(BoundExpr::ScalarSubquery { query: Box::new(query), dtype })
            }
            Expr::InSubquery { expr, query, negated } => {
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
                Ok(BoundExpr::Exists { query: Box::new(sub), negated: *negated })
            }
        }
    }

    fn bind_window(
        &mut self,
        name: &str,
        args: &[Expr],
        over: &OverClause,
        aggs: &mut Vec<BoundAgg>,
        allow_agg: bool,
    ) -> Result<BoundExpr> {
        if !self.allow_win {
            return Err(PinkDbError::Execution(
                "window functions are not allowed in WHERE, GROUP BY, or HAVING (use a subquery)".to_string(),
            ));
        }
        if self.in_window {
            return Err(PinkDbError::Execution(
                "nested window functions are not supported in v1".to_string(),
            ));
        }
        // Resolve the function first (arity checks mirror the agg path).
        let kind: WindowKind = if let Some(r) = WindowFunc::ranking(name) {
            if !args.is_empty() {
                return Err(PinkDbError::Type(format!("{name} takes no arguments")));
            }
            WindowKind::Rank(r)
        } else if let Some(f) = AggFunc::from_name(name) {
            let is_star = args.len() == 1 && args[0] == Expr::Star;
            if args.iter().any(|a| a == &Expr::Star) && !is_star {
                return Err(PinkDbError::Parse("`*` is only valid in COUNT(*)".to_string()));
            }
            if f != AggFunc::Count && is_star {
                return Err(PinkDbError::Type(format!("only COUNT supports *, not {name}")));
            }
            if !is_star && args.len() != 1 {
                return Err(PinkDbError::Type(format!("{name} takes exactly 1 argument in v1")));
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
        let is_star = args.len() == 1 && args.first() == Some(&Expr::Star);
        let arg = if is_star || args.is_empty() {
            None
        } else {
            Some(self.bind_expr(&args[0], aggs, allow_agg)?)
        };
        let mut partition = Vec::with_capacity(over.partition.len());
        for p in &over.partition {
            partition.push(self.bind_expr(p, aggs, allow_agg)?);
        }
        let mut order = Vec::with_capacity(over.order.len());
        for o in &over.order {
            order.push((self.bind_expr(&o.expr, aggs, allow_agg)?, o.desc));
        }
        let frame = match &over.frame {
            Some(f) => BoundFrame {
                start: map_frame_bound(f.start)?,
                end: map_frame_bound(f.end)?,
            },
            None => BoundFrame::default_for(!order.is_empty()),
        };
        let (func, dtype) = match kind {
            WindowKind::Rank(r) => (r, DataType::Bigint),
            WindowKind::Agg(f) => {
                let dtype = agg_dtype(f, arg.as_ref().map(|a| a.dtype()))?;
                (WindowFunc::Agg(f), dtype)
            }
        };
        let win = BoundWindow { func, arg, partition, order, frame, dtype };
        let index = match self.windows.iter().position(|w| w == &win) {
            Some(i) => i,
            None => {
                self.windows.push(win);
                self.windows.len() - 1
            }
        };
        Ok(BoundExpr::WindowRef { index, dtype })
    }

    fn bind_function(
        &mut self,
        name: &str,
        args: &[Expr],
        aggs: &mut Vec<BoundAgg>,
        allow_agg: bool,
    ) -> Result<BoundExpr> {
        if let Some(func) = AggFunc::from_name(name) {
            if !allow_agg {
                return Err(PinkDbError::Execution(
                    "aggregate functions are not allowed in WHERE or GROUP BY".to_string(),
                ));
            }
            let is_star = args.len() == 1 && args[0] == Expr::Star;
            if args.iter().any(|a| a == &Expr::Star) && !is_star {
                return Err(PinkDbError::Parse("`*` is only valid in COUNT(*)".to_string()));
            }
            if func != AggFunc::Count && is_star {
                return Err(PinkDbError::Type(format!("only COUNT supports *, not {name}")));
            }
            if !is_star && args.len() != 1 {
                return Err(PinkDbError::Type(format!("{name} takes exactly 1 argument in v1")));
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
            let agg = BoundAgg { func, arg, dtype };
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
        if let Some(func) = ScalarFunc::from_name(name) {
            let mut bound = Vec::with_capacity(args.len());
            for a in args {
                if a == &Expr::Star {
                    return Err(PinkDbError::Parse("`*` is only valid in COUNT(*)".to_string()));
                }
                bound.push(self.bind_expr(a, aggs, allow_agg)?);
            }
            let dtype = scalar_dtype(func, &bound)?;
            return Ok(BoundExpr::Scalar { func, args: bound, dtype });
        }
        // Not a built-in: a loaded extension may provide it (case-insensitive).
        // (On wasm32 the extension loader is not linked: no dynamic functions
        // can be registered, so the unknown-function error is the whole story.)
        #[cfg(not(target_arch = "wasm32"))]
        return self.bind_dynamic_function(name, args, aggs, allow_agg);
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (args, aggs, allow_agg);
            Err(PinkDbError::Catalog(format!("unknown function \"{name}\"")))
        }
    }

    /// Bind a call to a function registered by a loaded extension.
    ///
    /// ABI v1 is strictly typed: every argument's bound type must equal the
    /// registered argument type (a bare `NULL` literal is typeless and is
    /// passed as a NULL of the registered type), and the result type is the
    /// registered return type.
    ///
    /// Native-only: the dynamic registry (and the loader that feeds it) does
    /// not exist on wasm32.
    #[cfg(not(target_arch = "wasm32"))]
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
        let dtype = crate::ext::dtype_of_ext(func.ret_type());
        if args.len() != want_args.len() {
            return Err(PinkDbError::Type(format!(
                "{func_name} takes {} argument(s)",
                want_args.len()
            )));
        }
        let mut bound = Vec::with_capacity(args.len());
        for (i, a) in args.iter().enumerate() {
            if a == &Expr::Star {
                return Err(PinkDbError::Parse("`*` is only valid in COUNT(*)".to_string()));
            }
            let b = self.bind_expr(a, aggs, allow_agg)?;
            let want = want_args[i];
            // A bare NULL literal is typeless: the executor hands the
            // callback `is_null` with the registered type tag. Everything
            // else must match the registered argument type exactly — ABI v1
            // inserts no implicit casts.
            if !matches!(b, BoundExpr::Literal(Value::Null))
                && crate::ext::ext_type_of(b.dtype()) != want
            {
                return Err(PinkDbError::Type(format!(
                    "{func_name} argument {} must be {want}, got {} (use CAST)",
                    i + 1,
                    b.dtype()
                )));
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

fn agg_dtype(func: AggFunc, arg: Option<DataType>) -> Result<DataType> {
    match func {
        AggFunc::Count => Ok(DataType::Bigint),
        AggFunc::Avg => {
            let t = arg.expect("avg has an arg");
            if !is_numeric(t) {
                return Err(PinkDbError::Type("AVG needs a numeric argument".to_string()));
            }
            Ok(DataType::Double)
        }
        AggFunc::Sum => match arg.expect("sum has an arg") {
            DataType::Integer | DataType::Bigint => Ok(DataType::Bigint),
            DataType::Double => Ok(DataType::Double),
            _ => Err(PinkDbError::Type("SUM needs a numeric argument".to_string())),
        },
        AggFunc::Min | AggFunc::Max => {
            let t = arg.expect("min/max has an arg");
            if !is_numeric(t)
                && t != DataType::Varchar
                && t != DataType::Boolean
                && t != DataType::Date
                && t != DataType::Timestamp
            {
                return Err(PinkDbError::Type("MIN/MAX need an orderable argument".to_string()));
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
            let want = allowed.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(" or ");
            Err(PinkDbError::Type(format!("{func:?} takes {want} argument(s) in v1")))
        }
    };
    match func {
        ScalarFunc::Abs => {
            arity(&[1])?;
            let t = args[0].dtype();
            if !is_numeric(t) {
                return Err(PinkDbError::Type("abs needs a numeric argument".to_string()));
            }
            Ok(t)
        }
        ScalarFunc::Round => {
            arity(&[1, 2])?;
            if !is_numeric(args[0].dtype()) {
                return Err(PinkDbError::Type("round needs a numeric argument".to_string()));
            }
            if args.len() == 2 && !is_numeric(args[1].dtype()) {
                return Err(PinkDbError::Type("round digits must be numeric".to_string()));
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
                t => Err(PinkDbError::Type(format!("{func:?} needs DATE or TIMESTAMP, got {t}"))),
            }
        }
        ScalarFunc::Hour | ScalarFunc::Minute | ScalarFunc::Second => {
            arity(&[1])?;
            match args[0].dtype() {
                DataType::Timestamp => Ok(DataType::Integer),
                t => Err(PinkDbError::Type(format!("{func:?} needs TIMESTAMP, got {t}"))),
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
                t => Err(PinkDbError::Type(format!("date_trunc needs DATE or TIMESTAMP, got {t}"))),
            }
        }
    }
}

/// Default output name for a projection (DuckDB-style expression text).
fn default_name(expr: &Expr) -> String {
    match expr {
        Expr::Literal(v) => v.to_string(),
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
        Expr::Between { expr, low, high, negated } => {
            let not = if *negated { "NOT " } else { "" };
            format!(
                "({} {not}BETWEEN {} AND {})",
                default_name(expr),
                default_name(low),
                default_name(high)
            )
        }
        Expr::Function { name, args } => {
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
            format!("{}({})", name.to_ascii_lowercase(), inner.join(", "))
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
        Expr::Case { branches, else_, .. } => {
            let mut s = String::from("CASE");
            for (c, v) in branches {
                s.push_str(&format!(" WHEN {} THEN {}", default_name(c), default_name(v)));
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
        BoundExpr::Not(e) | BoundExpr::Negate { expr: e, .. } | BoundExpr::IsNull { expr: e, .. } => {
            validate_grouped(e, keys)
        }
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
        BoundExpr::Case { branches, else_, .. } => {
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
        return keys.iter().position(|k| k == expr).map(|pos| BoundExpr::Column {
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
        BoundExpr::Binary { op, left, right, dtype } => BoundExpr::Binary {
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
        BoundExpr::InSubquery { expr, query, negated } => BoundExpr::InSubquery {
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
        BoundExpr::Case { branches, else_, dtype } => BoundExpr::Case {
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
        BoundExpr::Binary { op, left, right, dtype } => BoundExpr::Binary {
            op,
            left: Box::new(resolve_window_refs(*left, input_width)),
            right: Box::new(resolve_window_refs(*right, input_width)),
            dtype,
        },
        BoundExpr::Not(e) => BoundExpr::Not(Box::new(resolve_window_refs(*e, input_width))),
        BoundExpr::Negate { expr, dtype } => {
            BoundExpr::Negate { expr: Box::new(resolve_window_refs(*expr, input_width)), dtype }
        }
        BoundExpr::IsNull { expr, negated } => BoundExpr::IsNull {
            expr: Box::new(resolve_window_refs(*expr, input_width)),
            negated,
        },
        BoundExpr::Scalar { func, args, dtype } => BoundExpr::Scalar {
            func,
            args: args.into_iter().map(|a| resolve_window_refs(a, input_width)).collect(),
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
        BoundExpr::InSubquery { expr, query, negated } => BoundExpr::InSubquery {
            expr: Box::new(resolve_window_refs(*expr, input_width)),
            query,
            negated,
        },
        BoundExpr::Cast { expr, dtype } => BoundExpr::Cast {
            expr: Box::new(resolve_window_refs(*expr, input_width)),
            dtype,
        },
        BoundExpr::Coalesce { args, dtype } => BoundExpr::Coalesce {
            args: args.into_iter().map(|a| resolve_window_refs(a, input_width)).collect(),
            dtype,
        },
        BoundExpr::Case { branches, else_, dtype } => BoundExpr::Case {
            branches: branches
                .into_iter()
                .map(|(c, v)| {
                    (resolve_window_refs(c, input_width), resolve_window_refs(v, input_width))
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
        BoundExpr::Binary { op, left, right, dtype } => BoundExpr::Binary {
            op,
            left: Box::new(resolve_agg_refs(*left, num_keys)),
            right: Box::new(resolve_agg_refs(*right, num_keys)),
            dtype,
        },
        BoundExpr::Not(e) => BoundExpr::Not(Box::new(resolve_agg_refs(*e, num_keys))),
        BoundExpr::Negate { expr, dtype } => {
            BoundExpr::Negate { expr: Box::new(resolve_agg_refs(*expr, num_keys)), dtype }
        }
        BoundExpr::IsNull { expr, negated } => BoundExpr::IsNull {
            expr: Box::new(resolve_agg_refs(*expr, num_keys)),
            negated,
        },
        BoundExpr::Scalar { func, args, dtype } => BoundExpr::Scalar {
            func,
            args: args.into_iter().map(|a| resolve_agg_refs(a, num_keys)).collect(),
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
        BoundExpr::InSubquery { expr, query, negated } => BoundExpr::InSubquery {
            expr: Box::new(resolve_agg_refs(*expr, num_keys)),
            query,
            negated,
        },
        BoundExpr::Cast { expr, dtype } => BoundExpr::Cast {
            expr: Box::new(resolve_agg_refs(*expr, num_keys)),
            dtype,
        },
        BoundExpr::Coalesce { args, dtype } => BoundExpr::Coalesce {
            args: args.into_iter().map(|a| resolve_agg_refs(a, num_keys)).collect(),
            dtype,
        },
        BoundExpr::Case { branches, else_, dtype } => BoundExpr::Case {
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

/// Bind a JOIN `ON` condition. v1 accepts a single `left_expr = right_expr`
/// equality (either orientation); each side binds in its own table's scope,
/// so keys are side-relative. Aggregates are forbidden in ON.
fn bind_join_keys(
    db: &PinkDb,
    default_db: &str,
    left: &ScopeTable,
    right: &ScopeTable,
    on: &Expr,
) -> Result<(BoundExpr, BoundExpr)> {
    let (l, r) = match on {
        Expr::Binary { op: BinaryOp::Eq, left, right } => (left.as_ref(), right.as_ref()),
        _ => {
            return Err(PinkDbError::Execution(
                "JOIN ON must be a single equality `left_col = right_col` in v1".to_string(),
            ));
        }
    };
    fn bind_side(
        db: &PinkDb,
        default_db: &str,
        scope: &ScopeTable,
        expr: &Expr,
    ) -> Result<BoundExpr> {
        let mut b = Binder::new(db, default_db);
        b.tables.push(scope.clone());
        b.allow_alias = false;
        b.allow_win = false;
        let mut aggs = Vec::new();
        let bound = b.bind_expr(expr, &mut aggs, false)?;
        if !aggs.is_empty() {
            return Err(PinkDbError::Execution(
                "aggregates are not allowed in JOIN ON".to_string(),
            ));
        }
        Ok(bound)
    }
    // Try `l=left, r=right`, then the swapped orientation.
    if let (Ok(lk), Ok(rk)) = (
        bind_side(db, default_db, left, l),
        bind_side(db, default_db, right, r),
    ) {
        comparison_ok(BinaryOp::Eq, lk.dtype(), rk.dtype()).map_err(|_| {
            PinkDbError::Type(format!(
                "JOIN keys have incompatible types: {} and {}",
                lk.dtype(),
                rk.dtype()
            ))
        })?;
        return Ok((lk, rk));
    }
    match (
        bind_side(db, default_db, right, l),
        bind_side(db, default_db, left, r),
    ) {
        (Ok(lk), Ok(rk)) => {
            comparison_ok(BinaryOp::Eq, rk.dtype(), lk.dtype()).map_err(|_| {
                PinkDbError::Type(format!(
                    "JOIN keys have incompatible types: {} and {}",
                    rk.dtype(),
                    lk.dtype()
                ))
            })?;
            Ok((rk, lk))
        }
        _ => Err(PinkDbError::Catalog(
            "JOIN ON must reference one column from each side".to_string(),
        )),
    }
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
    let empty = |plan| Ok((plan, Vec::new()));
    match stmt {
        Statement::CreateTable { db: qdb, name, columns } => empty(LogicalPlan::CreateTable {
            db: Some(Binder::new(db, default_db).resolve_db(qdb)?),
            name: name.clone(),
            schema: columns.clone(),
        }),
        Statement::Insert { db: qdb, table, columns, rows } => {
            bind_insert(db, default_db, qdb, table, columns, rows)
        }
        Statement::Update { db: qdb, table, sets, where_clause } => {
            bind_update(db, default_db, qdb, table, sets, where_clause)
        }
        Statement::Delete { db: qdb, table, where_clause } => {
            bind_delete(db, default_db, qdb, table, where_clause)
        }
        Statement::Begin => empty(LogicalPlan::Begin),
        Statement::Commit => empty(LogicalPlan::Commit),
        Statement::Rollback => empty(LogicalPlan::Rollback),
        Statement::CreateView { db: qdb, name, cols, query } => {
            bind_create_view(db, default_db, qdb, name, cols, query)
        }
        Statement::DropTable { db: qdb, name } => empty(LogicalPlan::DropTable {
            db: Some(Binder::new(db, default_db).resolve_db(qdb)?),
            name: name.clone(),
        }),
        Statement::DropView { db: qdb, name } => empty(LogicalPlan::DropView {
            db: Some(Binder::new(db, default_db).resolve_db(qdb)?),
            name: name.clone(),
        }),
        Statement::CreateDatabase { name } => {
            empty(LogicalPlan::CreateDatabase { name: name.clone() })
        }
        Statement::DropDatabase { name } => {
            empty(LogicalPlan::DropDatabase { name: name.clone() })
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
        Statement::Select(sel) => bind_select(db, default_db, sel),
    }
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
    if let Some(cols) = cols {
        if cols.len() != schema.len() {
            return Err(PinkDbError::Execution(format!(
                "view \"{name}\" has {} columns but {} aliases",
                schema.len(),
                cols.len()
            )));
        }
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
/// scope (no aggregates, windows, or aliases), check Boolean WHERE.
fn bind_update(
    db: &PinkDb,
    default_db: &str,
    qdb: &Option<String>,
    table: &str,
    sets: &[(String, Expr)],
    where_clause: &Option<Expr>,
) -> Result<(LogicalPlan, Vec<String>)> {
    let table_db = Binder::new(db, default_db).resolve_db(qdb)?;
    let t = db.table_in(&table_db, table)?;
    if sets.is_empty() {
        return Err(PinkDbError::Parse("UPDATE needs at least one SET assignment".to_string()));
    }
    let mut seen = std::collections::HashSet::new();
    for (col, _) in sets {
        if !seen.insert(col.to_ascii_lowercase()) {
            return Err(PinkDbError::Catalog(format!(
                "duplicate SET column \"{col}\""
            )));
        }
    }
    let mut binder = Binder::new(db, &table_db);
    binder.allow_win = false;
    binder.tables.push(ScopeTable {
        name: t.name.to_ascii_lowercase(),
        columns: t.schema(),
    });
    let mut bound_sets = Vec::with_capacity(sets.len());
    for (col, expr) in sets {
        let idx = t.column_index(col).map_err(|_| {
            PinkDbError::Catalog(format!("no column \"{col}\""))
        })?;
        let (_, target) = t.schema()[idx].clone();
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
    if let Some(p) = &predicate {
        if p.dtype() != DataType::Boolean {
            return Err(PinkDbError::Type("WHERE needs a BOOLEAN expression".to_string()));
        }
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
) -> Result<(LogicalPlan, Vec<String>)> {
    let table_db = Binder::new(db, default_db).resolve_db(qdb)?;
    let t = db.table_in(&table_db, table)?;
    let mut binder = Binder::new(db, &table_db);
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
    if let Some(p) = &predicate {
        if p.dtype() != DataType::Boolean {
            return Err(PinkDbError::Type("WHERE needs a BOOLEAN expression".to_string()));
        }
    }
    reject_view_dependency(db, &binder.views_used())?;
    let dynamic_functions = binder.dynamic_functions_used();
    Ok((
        LogicalPlan::Delete { db: Some(table_db), table: t.name.clone(), predicate },
        dynamic_functions,
    ))
}

fn bind_insert(
    db: &PinkDb,
    default_db: &str,
    qdb: &Option<String>,
    table: &str,
    columns: &Option<Vec<String>>,
    rows: &[Vec<Expr>],
) -> Result<(LogicalPlan, Vec<String>)> {
    let table_db = Binder::new(db, default_db).resolve_db(qdb)?;
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
        // Fill unspecified columns with NULL (only when a column list is given).
        for (pos, cell) in row.iter().enumerate() {
            let mut binder = Binder::new(db, &table_db);
            binder.allow_win = false;
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
) -> Result<(LogicalPlan, Vec<String>)> {
    let mut binder = Binder::new(db, default_db);
    let plan = bind_select_in(db, &mut binder, sel)?;
    Ok((plan, binder.dynamic_functions_used()))
}

/// ORDER BY/LIMIT written before a UNION (on the head or a middle branch):
/// the current parser attaches such clauses to the branch they follow, which
/// would silently change semantics. DuckDB rejects them too.
fn union_clause_error() -> PinkDbError {
    PinkDbError::Parse(
        "ORDER BY/LIMIT before UNION is not supported; place it after the last branch"
            .to_string(),
    )
}

/// Bind a complete SELECT in an existing scope: WITH/CTE definitions, the
/// body, and — when present — the whole UNION chain. Chain-level ORDER
/// BY/LIMIT (written after the last branch) apply to the whole set operation.
///
/// Every query context (statement, derived table, CTE body, view, subquery)
/// must enter here: `bind_select_with` binds a single branch and ignores
/// `unions`.
fn bind_select_in(db: &PinkDb, binder: &mut Binder, sel: &Select) -> Result<LogicalPlan> {
    if sel.unions.is_empty() {
        return bind_select_with(db, binder, sel);
    }
    // The parser attaches trailing clauses to the last branch; a clause on the
    // head or on a middle branch was therefore written *before* a UNION.
    if !sel.order_by.is_empty() || sel.limit.is_some() {
        return Err(union_clause_error());
    }
    for (_, branch) in &sel.unions[..sel.unions.len() - 1] {
        if !branch.order_by.is_empty() || branch.limit.is_some() {
            return Err(union_clause_error());
        }
    }
    // Hoist the last branch's trailing clauses to the chain level.
    let mut head = sel.clone();
    let (_, last) = head.unions.last_mut().expect("unions checked non-empty");
    let order_by = std::mem::take(&mut last.order_by);
    let limit = last.limit.take();

    // WITH belongs to the whole chain: bind it once, then wrap the final plan.
    // Every branch (and the ORDER BY/LIMIT wrapper below) sees the CTEs.
    let cte_base = binder.ctes.len();
    let cte_plans = bind_ctes(db, binder, sel)?;
    head.with.clear();

    let plan = if order_by.is_empty() && limit.is_none() {
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
                join: None,
            }),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
            order_by,
            limit,
            unions: Vec::new(),
        };
        bind_select_in(db, binder, &wrapper)?
    };
    Ok(wrap_ctes(binder, cte_base, cte_plans, plan))
}

/// Bind the head and every UNION branch into the left-associative set
/// operation. Branch-level ORDER BY/LIMIT are hoisted (or rejected) by
/// [`bind_select_in`] before this point, so each branch binds bare; pairwise
/// type unification (numeric tower) is recorded on every `Union` node.
fn bind_union_chain(db: &PinkDb, binder: &mut Binder, sel: &Select) -> Result<LogicalPlan> {
    debug_assert!(!sel.unions.is_empty());
    debug_assert!(sel.order_by.is_empty() && sel.limit.is_none());
    let mut head = sel.clone();
    head.unions.clear();
    let mut plan = bind_select_with(db, binder, &head)?;
    // UNION branches (left-associative): each sees the outer CTEs (including
    // the head's WITH CTEs, bound by the caller).
    for (op, branch) in &sel.unions {
        debug_assert!(branch.order_by.is_empty() && branch.limit.is_none());
        let mut child = binder.child();
        let right = bind_select_with(db, &mut child, branch)?;
        let left_schema = plan_schema(db, &binder.default_db, &binder.ctes, &plan)?;
        let right_schema = plan_schema(db, &child.default_db, &child.ctes, &right)?;
        if left_schema.len() != right_schema.len() {
            return Err(PinkDbError::Execution(format!(
                "UNION branches have {} and {} columns",
                left_schema.len(),
                right_schema.len()
            )));
        }
        let mut types = Vec::with_capacity(left_schema.len());
        for ((_, l), (_, r)) in left_schema.iter().zip(right_schema.iter()) {
            types.push(coalesce_common(*l, *r)?);
        }
        plan = LogicalPlan::Union {
            left: Box::new(plan),
            right: Box::new(right),
            distinct: *op == SetOp::Union,
            types,
        };
    }
    Ok(plan)
}

/// Output schema of a bound plan: (column name, type) per output column.
/// Used for CTE/derived-table scoping and subquery arity checks. `ctes`
/// carries the CTEs visible at this point (CTEs shadow catalog tables);
/// `default_db` resolves a `Scan` that names a CTE only indirectly.
fn plan_schema(
    db: &PinkDb,
    default_db: &str,
    ctes: &[(String, Vec<(String, DataType)>)],
    plan: &LogicalPlan,
) -> Result<Vec<(String, DataType)>> {
    match plan {
        LogicalPlan::Scan { db: qdb, table } => {
            if qdb.is_none() {
                if let Some((_, schema)) =
                    ctes.iter().rev().find(|(n, _)| n.eq_ignore_ascii_case(table))
                {
                    return Ok(schema.clone());
                }
            }
            let lookup = qdb.as_deref().unwrap_or(default_db);
            Ok(db.table_in(lookup, table)?.schema())
        }
        LogicalPlan::SingleRow => Ok(Vec::new()),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => plan_schema(db, default_db, ctes, input),
        LogicalPlan::CreateTable { .. }
        | LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. }
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback
        | LogicalPlan::CreateView { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::CreateDatabase { .. }
        | LogicalPlan::DropDatabase { .. }
        | LogicalPlan::LoadExtension { .. }
        | LogicalPlan::Use { .. } => Ok(Vec::new()),
        LogicalPlan::Project { exprs, names, .. } => Ok(names
            .iter()
            .cloned()
            .zip(exprs.iter().map(|e| e.dtype()))
            .collect()),
        LogicalPlan::Aggregate { keys, aggs, .. } => {
            let (names, types) = crate::plan::agg_output_schema(keys, aggs);
            Ok(names.into_iter().zip(types).collect())
        }
        LogicalPlan::Join { left, right, .. } => {
            let mut out = plan_schema(db, default_db, ctes, left)?;
            out.extend(plan_schema(db, default_db, ctes, right)?);
            Ok(out)
        }
        LogicalPlan::Union { left, types, .. } => {
            // Output names come from the left branch.
            let left_schema = plan_schema(db, default_db, ctes, left)?;
            Ok(left_schema.into_iter().zip(types.iter().copied()).map(|((n, _), t)| (n, t)).collect())
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
) -> Result<(ScopeTable, LogicalPlan)> {
    match source {
        FromSource::Table { db: qdb, name } => {
            // CTEs shadow everything — but only unqualified names.
            if qdb.is_none() {
                if let Some((_, schema)) = binder
                    .ctes
                    .iter()
                    .rev()
                    .find(|(n, _)| n.eq_ignore_ascii_case(name))
                {
                    let eff = alias.clone().unwrap_or_else(|| name.clone());
                    return Ok((
                        ScopeTable { name: eff.to_ascii_lowercase(), columns: schema.clone() },
                        LogicalPlan::Scan { db: None, table: name.clone() },
                    ));
                }
            }
            let lookup_db = binder.resolve_db(qdb)?;
            if let Ok(view) = db.view_in(&lookup_db, name) {
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
                    ScopeTable { name: eff.to_ascii_lowercase(), columns: schema },
                    sub,
                ));
            }
            let columns = db.table_in(&lookup_db, name)?.schema();
            let eff = alias.clone().unwrap_or_else(|| name.clone());
            Ok((
                ScopeTable { name: eff.to_ascii_lowercase(), columns },
                LogicalPlan::Scan { db: Some(lookup_db), table: name.clone() },
            ))
        }
        FromSource::Derived(q) => {
            let alias = alias.clone().ok_or_else(|| {
                PinkDbError::Parse("derived table needs an alias".to_string())
            })?;
            let mut child = binder.child();
            let sub = bind_select_in(db, &mut child, q)?;
            let schema = plan_schema(db, &child.default_db, &binder.ctes, &sub)?;
            Ok((
                ScopeTable { name: alias.to_ascii_lowercase(), columns: schema },
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
fn bind_ctes(
    db: &PinkDb,
    binder: &mut Binder,
    sel: &Select,
) -> Result<Vec<(String, LogicalPlan)>> {
    let mut cte_plans: Vec<(String, LogicalPlan)> = Vec::new();
    for cte in &sel.with {
        if binder.ctes.iter().any(|(n, _)| n.eq_ignore_ascii_case(&cte.name)) {
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
        plan = LogicalPlan::Cte { name, query: Box::new(query), body: Box::new(plan) };
    }
    binder.ctes.truncate(cte_base);
    plan
}

fn bind_select_body(db: &PinkDb, binder: &mut Binder, sel: &Select) -> Result<LogicalPlan> {
    // FROM (+ optional single JOIN). Returns the base plan and left width.
    let mut base: LogicalPlan;
    if let Some(from) = &sel.from {
        let (left, left_plan) = bind_source(db, binder, &from.source, &from.alias)?;
        let left_cols = left.columns.len();
        base = left_plan;
        binder.tables.push(left);
        if let Some(join) = &from.join {
            let (right, right_plan) = bind_source(db, binder, &join.source, &join.alias)?;
            if binder.tables[0].name == right.name {
                return Err(PinkDbError::Catalog(format!(
                    "table name \"{}\" specified more than once (use an alias)",
                    right.name
                )));
            }
            let (left_key, right_key) =
                bind_join_keys(db, &binder.default_db, &binder.tables[0], &right, &join.on)?;
            binder.tables.push(right);
            base = LogicalPlan::Join {
                kind: join.kind,
                left: Box::new(base),
                right: Box::new(right_plan),
                left_key,
                right_key,
                left_cols,
            };
        }
    } else {
        base = LogicalPlan::SingleRow;
    }

    for proj in &sel.projections {
        if let Projection::Expr { expr, alias: Some(a) } = proj {
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
    if let Some(f) = &filter {
        if f.dtype() != DataType::Boolean {
            return Err(PinkDbError::Type("WHERE needs a BOOLEAN expression".to_string()));
        }
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
                            Expr::Column { table: Some(q.clone()), name: name.clone() },
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
                        Expr::Column { table: Some(q.clone()), name: name.clone() },
                        name.clone(),
                    ));
                }
            }
            Projection::Expr { expr, alias } => {
                pending.push((expr.clone(), alias.clone().unwrap_or_else(|| default_name(expr))));
            }
        }
    }
    if pending.is_empty() {
        return Err(PinkDbError::Parse("SELECT needs at least one projection".to_string()));
    }

    let mut aggs: Vec<BoundAgg> = Vec::new();
    let mut select: Vec<(BoundExpr, String)> = Vec::with_capacity(pending.len());
    for (e, name) in &pending {
        select.push((binder.bind_expr(e, &mut aggs, true)?, name.clone()));
    }

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
            return Err(PinkDbError::Type("HAVING needs a BOOLEAN expression".to_string()));
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
        plan = LogicalPlan::Filter { input: Box::new(plan), predicate: pred };
    }
    if is_agg {
        plan = LogicalPlan::Aggregate { input: Box::new(plan), keys, aggs };
        if let Some(h) = having {
            plan = LogicalPlan::Filter { input: Box::new(plan), predicate: h };
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
        plan = LogicalPlan::Window { input: Box::new(plan), windows, input_cols };
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
    let mut sort_keys: Vec<(BoundExpr, bool)> = Vec::new();
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
                BoundExpr::Column { index: idx, dtype: visible_dtypes[idx] },
                item.desc,
            ));
            continue;
        }
        // Select alias → reuse that output (no hidden column).
        if let Expr::Column { table: None, name } = &item.expr {
            if let Some(pos) = proj_names[..visible].iter().position(|n| n.eq_ignore_ascii_case(name)) {
                sort_keys.push((
                    BoundExpr::Column { index: pos, dtype: visible_dtypes[pos] },
                    item.desc,
                ));
                continue;
            }
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
                sort_keys.push((BoundExpr::Column { index: idx, dtype }, item.desc));
            }
            None => {
                let dtype = scoped.dtype();
                proj_exprs.push(scoped);
                proj_names.push(format!("__order{hidden}"));
                hidden += 1;
                sort_keys.push((
                    BoundExpr::Column { index: proj_exprs.len() - 1, dtype },
                    item.desc,
                ));
            }
        }
    }

    plan = LogicalPlan::Project { input: Box::new(plan), exprs: proj_exprs, names: proj_names };
    if sel.distinct {
        if hidden > 0 {
            return Err(PinkDbError::Execution(
                "ORDER BY expression must appear in the select list with DISTINCT".to_string(),
            ));
        }
        plan = LogicalPlan::Distinct { input: Box::new(plan) };
    }
    if !sort_keys.is_empty() {
        plan = LogicalPlan::Sort { input: Box::new(plan), keys: sort_keys };
        if hidden > 0 {
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                exprs: (0..visible)
                    .map(|i| BoundExpr::Column { index: i, dtype: visible_dtypes[i] })
                    .collect(),
                names: visible_names,
            };
        }
    }
    if let Some(n) = sel.limit {
        plan = LogicalPlan::Limit { input: Box::new(plan), limit: n };
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
        let err = bind_one(&db, "SELECT * FROM sales JOIN sales ON sales.id = sales.id;")
            .unwrap_err();
        assert!(matches!(err, PinkDbError::Catalog(_)));
        // Non-equi ON is rejected.
        assert!(
            bind_one(&db, "SELECT * FROM sales s JOIN sales o ON s.id > o.id;").is_err()
        );
        // Aliased equi-join binds.
        let plan = bind_one(&db, "SELECT * FROM sales s JOIN sales o ON s.id = o.id;").unwrap();
        assert!(matches!(plan, LogicalPlan::Project { .. }));
    }

    /// A misrouted call with UNION branches must fail loudly instead of
    /// silently dropping every branch after the head.
    #[test]
    fn union_branches_must_bind_via_bind_select_in() {
        let db = test_db();
        let stmts = parse_statements("SELECT id FROM sales UNION ALL SELECT id FROM sales;").unwrap();
        let Statement::Select(sel) = &stmts[0] else { panic!("expected SELECT") };
        let mut binder = Binder::new(&db, "main");
        let err = bind_select_with(&db, &mut binder, sel).unwrap_err();
        assert!(matches!(err, PinkDbError::Execution(_)), "{err:?}");
        assert!(err.to_string().contains("bind_select_in"), "{err}");
    }

    /// Two databases with a same-named table of different shapes.
    fn multi_db() -> PinkDb {
        let mut db = PinkDb::new();
        db.create_database("db1").unwrap();
        db.create_table("t", vec![("a".to_string(), DataType::Integer)]).unwrap();
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
        assert_eq!(
            plan_schema(&db, "main", &[], &main_plan).unwrap().len(),
            1
        );
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
        assert!(err.to_string().contains("database \"nope\" not found"), "{err}");
        let err = bind_in(&db, "SELECT * FROM db1.missing;", "main").unwrap_err();
        assert!(err.to_string().contains("table \"missing\" not found"), "{err}");
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
        assert!(err.to_string().contains("database \"nope\" not found"), "{err}");
        assert!(matches!(
            bind_one(&db, "CREATE DATABASE db2;").unwrap(),
            LogicalPlan::CreateDatabase { name } if name == "db2"
        ));
        assert!(matches!(
            bind_one(&db, "DROP DATABASE db1;").unwrap(),
            LogicalPlan::DropDatabase { name } if name == "db1"
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
        db.create_table_in(
            "db1",
            "src",
            vec![("v".to_string(), DataType::Bigint)],
        )
        .unwrap();
        db.create_view_in(
            "db1",
            "v",
            {
                let stmts = parse_statements("SELECT v FROM src;").unwrap();
                let Statement::Select(sel) = &stmts[0] else { panic!() };
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
