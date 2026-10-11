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

use std::collections::HashMap;

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
    fn ranking(name: &str) -> Option<WindowFunc> {
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
    /// Uncorrelated scalar subquery (1 col by construction; rows at runtime).
    ScalarSubquery { query: Box<LogicalPlan>, dtype: DataType },
    /// `expr [NOT] IN (query)` — `expr` may reference the outer scope.
    InSubquery { expr: Box<BoundExpr>, query: Box<LogicalPlan>, negated: bool },
    /// `[NOT] EXISTS (query)` — scope-independent.
    Exists { query: Box<LogicalPlan>, negated: bool },
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
            BoundExpr::ScalarSubquery { dtype, .. } => *dtype,
            BoundExpr::InSubquery { .. } | BoundExpr::Exists { .. } => DataType::Boolean,
        }
    }

    /// True if the tree contains no [`BoundExpr::AggRef`].
    /// Subquery bodies live in their own scope and never count here.
    pub fn is_aggregate_free(&self) -> bool {
        match self {
            BoundExpr::AggRef { .. } | BoundExpr::WindowRef { .. } => false,
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
            BoundExpr::Not(e)
            | BoundExpr::Negate { expr: e, .. }
            | BoundExpr::IsNull { expr: e, .. } => e.has_column_ref(),
            BoundExpr::Binary { left, right, .. } => {
                left.has_column_ref() || right.has_column_ref()
            }
            BoundExpr::Scalar { args, .. } => args.iter().any(|a| a.has_column_ref()),
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
            | BoundExpr::IsNull { expr: e, .. } => e.has_subquery(),
            BoundExpr::Binary { left, right, .. } => {
                left.has_subquery() || right.has_subquery()
            }
            BoundExpr::Scalar { args, .. } => args.iter().any(|a| a.has_subquery()),
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

fn comparison_ok(op: BinaryOp, l: DataType, r: DataType) -> Result<()> {
    if (is_numeric(l) && is_numeric(r))
        || (l == DataType::Varchar && r == DataType::Varchar)
        || (l == DataType::Date && r == DataType::Date)
        || (l == DataType::Timestamp && r == DataType::Timestamp)
    {
        return Ok(());
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
}

impl<'a> Binder<'a> {
    fn new(db: &'a PinkDb) -> Self {
        Self {
            db,
            tables: Vec::new(),
            aliases: HashMap::new(),
            resolving: Vec::new(),
            allow_alias: true,
            allow_win: true,
            in_window: false,
            windows: Vec::new(),
            ctes: Vec::new(),
        }
    }

    /// A child scope for a subquery: no tables/aliases leak in, CTEs do.
    /// (Subqueries are uncorrelated in v1 — outer columns are invisible.)
    fn child(&self) -> Self {
        Self {
            db: self.db,
            tables: Vec::new(),
            aliases: HashMap::new(),
            resolving: Vec::new(),
            allow_alias: true,
            allow_win: true,
            in_window: false,
            windows: Vec::new(),
            ctes: self.ctes.clone(),
        }
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
                    | BinaryOp::Gt | BinaryOp::GtEq => {
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
            Expr::ScalarSubquery(q) => {
                let mut child = self.child();
                let query = bind_select_with(child.db, &mut child, q)?;
                let schema = plan_schema(child.db, &child.ctes, &query)?;
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
                let sub = bind_select_with(child.db, &mut child, query)?;
                let schema = plan_schema(child.db, &child.ctes, &sub)?;
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
                let sub = bind_select_with(child.db, &mut child, query)?;
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
        Err(PinkDbError::Catalog(format!("unknown function \"{name}\"")))
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
        BoundExpr::InSubquery { expr, query, negated } => BoundExpr::InSubquery {
            expr: Box::new(rebase_to_keys(expr, keys)?),
            query: query.clone(),
            negated: *negated,
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
        BoundExpr::InSubquery { expr, query, negated } => BoundExpr::InSubquery {
            expr: Box::new(resolve_window_refs(*expr, input_width)),
            query,
            negated,
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
        BoundExpr::InSubquery { expr, query, negated } => BoundExpr::InSubquery {
            expr: Box::new(resolve_agg_refs(*expr, num_keys)),
            query,
            negated,
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
fn bind_join_keys(db: &PinkDb, left: &ScopeTable, right: &ScopeTable, on: &Expr) -> Result<(BoundExpr, BoundExpr)> {
    let (l, r) = match on {
        Expr::Binary { op: BinaryOp::Eq, left, right } => (left.as_ref(), right.as_ref()),
        _ => {
            return Err(PinkDbError::Execution(
                "JOIN ON must be a single equality `left_col = right_col` in v1".to_string(),
            ));
        }
    };
    fn bind_side(db: &PinkDb, scope: &ScopeTable, expr: &Expr) -> Result<BoundExpr> {
        let mut b = Binder::new(db);
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
    if let (Ok(lk), Ok(rk)) = (bind_side(db, left, l), bind_side(db, right, r)) {
        comparison_ok(BinaryOp::Eq, lk.dtype(), rk.dtype()).map_err(|_| {
            PinkDbError::Type(format!(
                "JOIN keys have incompatible types: {} and {}",
                lk.dtype(),
                rk.dtype()
            ))
        })?;
        return Ok((lk, rk));
    }
    match (bind_side(db, right, l), bind_side(db, left, r)) {
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

/// Bind one statement. The returned plan borrows nothing from `db`.
pub fn bind(db: &PinkDb, stmt: &Statement) -> Result<LogicalPlan> {
    match stmt {
        Statement::CreateTable { name, columns } => Ok(LogicalPlan::CreateTable {
            name: name.clone(),
            schema: columns.clone(),
        }),
        Statement::Insert { table, columns, rows } => bind_insert(db, table, columns, rows),
        Statement::Update { table, sets, where_clause } => {
            bind_update(db, table, sets, where_clause)
        }
        Statement::Delete { table, where_clause } => bind_delete(db, table, where_clause),
        Statement::Begin => Ok(LogicalPlan::Begin),
        Statement::Commit => Ok(LogicalPlan::Commit),
        Statement::Rollback => Ok(LogicalPlan::Rollback),
        Statement::Select(sel) => bind_select(db, sel),
    }
}

/// Bind UPDATE: resolve target columns, bind SET expressions in the table
/// scope (no aggregates, windows, or aliases), check Boolean WHERE.
fn bind_update(
    db: &PinkDb,
    table: &str,
    sets: &[(String, Expr)],
    where_clause: &Option<Expr>,
) -> Result<LogicalPlan> {
    let t = db.table(table)?;
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
    let mut binder = Binder::new(db);
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
    Ok(LogicalPlan::Update {
        table: t.name.clone(),
        sets: bound_sets,
        predicate,
    })
}

/// Bind DELETE: Boolean WHERE over the table scope (no aggregates/windows).
fn bind_delete(db: &PinkDb, table: &str, where_clause: &Option<Expr>) -> Result<LogicalPlan> {
    let t = db.table(table)?;
    let mut binder = Binder::new(db);
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
    Ok(LogicalPlan::Delete { table: t.name.clone(), predicate })
}

fn bind_insert(
    db: &PinkDb,
    table: &str,
    columns: &Option<Vec<String>>,
    rows: &[Vec<Expr>],
) -> Result<LogicalPlan> {
    let t = db.table(table)?;
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
            let mut binder = Binder::new(db);
            binder.allow_win = false;
            let mut aggs = Vec::new();
            let e = binder.bind_expr(cell, &mut aggs, false)?;
            bound[order[pos]] = e;
        }
        bound_rows.push(bound);
    }
    Ok(LogicalPlan::Insert { table: t.name.clone(), rows: bound_rows })
}

fn bind_select(db: &PinkDb, sel: &Select) -> Result<LogicalPlan> {
    let mut binder = Binder::new(db);
    bind_select_with(db, &mut binder, sel)
}

/// Output schema of a bound plan: (column name, type) per output column.
/// Used for CTE/derived-table scoping and subquery arity checks. `ctes`
/// carries the CTEs visible at this point (CTEs shadow catalog tables).
fn plan_schema(
    db: &PinkDb,
    ctes: &[(String, Vec<(String, DataType)>)],
    plan: &LogicalPlan,
) -> Result<Vec<(String, DataType)>> {
    match plan {
        LogicalPlan::Scan { table } => {
            if let Some((_, schema)) =
                ctes.iter().rev().find(|(n, _)| n.eq_ignore_ascii_case(table))
            {
                return Ok(schema.clone());
            }
            Ok(db.table(table)?.schema())
        }
        LogicalPlan::SingleRow => Ok(Vec::new()),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => plan_schema(db, ctes, input),
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
            let mut out = plan_schema(db, ctes, left)?;
            out.extend(plan_schema(db, ctes, right)?);
            Ok(out)
        }
        LogicalPlan::Cte { name, query, body } => {
            let mut inner: Vec<(String, Vec<(String, DataType)>)> = ctes.to_vec();
            inner.push((name.clone(), plan_schema(db, ctes, query)?));
            plan_schema(db, &inner, body)
        }
        LogicalPlan::Window { input, windows, .. } => {
            let mut out = plan_schema(db, ctes, input)?;
            let (names, types) = crate::plan::win_output_schema(windows);
            out.extend(names.into_iter().zip(types));
            Ok(out)
        }
        LogicalPlan::CreateTable { .. } | LogicalPlan::Insert { .. } => Ok(Vec::new()),
        LogicalPlan::Update { .. } | LogicalPlan::Delete { .. } => Ok(Vec::new()),
        LogicalPlan::Begin | LogicalPlan::Commit | LogicalPlan::Rollback => Ok(Vec::new()),
    }
}

/// Bind one FROM/JOIN source: returns its scope entry + executable plan.
/// Tables resolve CTEs first (shadowing), then the catalog; derived tables
/// bind their inner query in a child scope (same CTEs, no outer tables —
/// subqueries are uncorrelated in v1).
fn bind_source(
    db: &PinkDb,
    binder: &mut Binder,
    source: &FromSource,
    alias: &Option<String>,
) -> Result<(ScopeTable, LogicalPlan)> {
    match source {
        FromSource::Table(name) => {
            let columns = match binder
                .ctes
                .iter()
                .rev()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
            {
                Some((_, schema)) => schema.clone(),
                None => db.table(name)?.schema(),
            };
            let eff = alias.clone().unwrap_or_else(|| name.clone());
            Ok((
                ScopeTable { name: eff.to_ascii_lowercase(), columns },
                LogicalPlan::Scan { table: name.clone() },
            ))
        }
        FromSource::Derived(q) => {
            let alias = alias.clone().ok_or_else(|| {
                PinkDbError::Parse("derived table needs an alias".to_string())
            })?;
            let mut child = binder.child();
            let sub = bind_select_with(db, &mut child, q)?;
            let schema = plan_schema(db, &binder.ctes, &sub)?;
            Ok((
                ScopeTable { name: alias.to_ascii_lowercase(), columns: schema },
                sub,
            ))
        }
    }
}

fn bind_select_with(db: &PinkDb, binder: &mut Binder, sel: &Select) -> Result<LogicalPlan> {
    // WITH: bind each CTE in order (seeing previous + ancestor CTEs), then
    // wrap the body in Cte nodes. Scopes pop on exit via truncation.
    let cte_base = binder.ctes.len();
    let mut cte_plans: Vec<(String, LogicalPlan)> = Vec::new();
    for cte in &sel.with {
        if binder.ctes.iter().any(|(n, _)| n.eq_ignore_ascii_case(&cte.name)) {
            return Err(PinkDbError::Catalog(format!(
                "duplicate CTE name \"{}\"",
                cte.name
            )));
        }
        let mut child = binder.child();
        let query = bind_select_with(db, &mut child, &cte.query)?;
        let mut schema = plan_schema(db, &binder.ctes, &query)?;
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

    let mut plan = bind_select_body(db, binder, sel)?;

    for (name, query) in cte_plans.into_iter().rev() {
        plan = LogicalPlan::Cte { name, query: Box::new(query), body: Box::new(plan) };
    }
    binder.ctes.truncate(cte_base);
    Ok(plan)
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
                bind_join_keys(db, &binder.tables[0], &right, &join.on)?;
            binder.tables.push(right);
            base = LogicalPlan::Join {
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
    use super::super::parser::parse_statements;
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
        bind(db, &stmts[0])
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
}
