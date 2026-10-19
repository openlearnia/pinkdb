//! SQL macros (wave 77): scalar and table templates, expanded at bind time.
//!
//! ```sql
//! CREATE MACRO add(a, b := 1) AS a + b;
//! CREATE MACRO high(s) AS TABLE SELECT * FROM emp WHERE salary > s;
//! SELECT add(2), add(2, 10);
//! SELECT * FROM high(100);
//! DROP MACRO high;
//! ```
//!
//! A macro is a catalog object of one database, stored next to its tables,
//! materialized views and procedures. `CREATE MACRO` and `DROP MACRO` are
//! WAL-logged exactly like the other catalog DDL, and a checkpoint writes the
//! definitions into the `.pink` file (flags bit 6).
//!
//! # The substitution model
//!
//! Parameters are **bind-time placeholders**. A call's argument expressions
//! are spliced into a clone of the body AST (scalar macros into an expression
//! position, table macros into a `FROM` position) and the expanded template is
//! then bound by the ordinary binder. No SQL text is ever generated from an
//! argument, so a macro is injection-safe by construction and `?` parameters
//! in an argument keep their prepared-statement slots.
//!
//! The consequences, all pinned by tests:
//! - types are inferred at the call site (macros are untyped, like DuckDB's);
//! - nested macro calls expand recursively during the same bind;
//! - a macro used by a view / materialized-view body expands when that body
//!   rebinds (views are session-local and rebind per use; a materialized view
//!   rebinds its persisted SQL), so redefining a macro changes the next bind;
//! - an argument expression is duplicated wherever its parameter appears
//!   (`twice(x) AS x + x` evaluates the argument twice, DuckDB's rule).
//!
//! # Recursion
//!
//! Definitions are checked for direct and mutual recursion at `CREATE` time:
//! the new body's macro call graph is walked through the already-stored
//! definitions, and the reverse direction (an existing macro that already
//! names this one) is checked too, so a forward reference cannot sneak a
//! cycle in when its target is defined later. `CREATE OR REPLACE MACRO a(n)
//! AS a(n)` is therefore refused as well — DuckDB accepts the definition
//! when an older `a` exists and reports a bind-depth error on the next call;
//! this engine refuses the cycle earlier and never risks a binder stack
//! overflow (a deliberate, documented divergence). Forward references that
//! stay acyclic are allowed: the body binds at its first use, by which time
//! the named macro must exist.
//!
//! Cycles that run through a view (`v` uses macro `f`, `f`'s body subquery
//! reads `v`) cannot be seen from one definition: they are caught at bind by
//! the expansion-depth and expansion-size limits below, with a clear error.
//!
//! # Privileges
//!
//! A macro expands **before** the privilege check, and has invoker rights:
//! the bound plan names the base tables, so an authenticated caller still
//! needs `SELECT` on every table the body reads. There is no macro-level
//! `EXECUTE` privilege and no definer semantics. Creating or dropping a macro
//! needs the database `CREATE` privilege (or `ADMIN`), exactly like a
//! procedure.

use std::collections::{HashMap, HashSet};

use crate::sql::ast::{Expr, FromClause, FromSource, MacroBody, Projection, Select};
use crate::types::{PinkDbError, Result};

/// Maximum nested macro expansion depth during one bind. Recursion that
/// crosses views is caught here (DuckDB reports its own expression-depth
/// limit; this engine's parser/binder depth budget is much smaller). The cap
/// is deliberately low: a debug-build binder frame is large, and a cycle that
/// alternates macro → view → macro must fail before a 2 MiB test thread's
/// stack is exhausted.
pub(crate) const MAX_MACRO_EXPANSION_DEPTH: usize = 8;

/// Maximum substitution work (AST nodes created) for one statement's bind.
/// A macro body can duplicate its arguments, so depth alone does not bound
/// the expanded tree (`f(x) AS g(x) + g(x)` with a 30-deep chain would be
/// exponential). The budget turns that into a clean error.
pub(crate) const MAX_MACRO_EXPANSION_NODES: usize = 65_536;

/// One stored macro parameter: its untyped name and optional constant default.
#[derive(Debug, Clone, PartialEq)]
pub struct MacroParamDef {
    pub name: String,
    pub default: Option<Expr>,
}

/// One stored macro definition.
#[derive(Debug, Clone, PartialEq)]
pub struct MacroDef {
    /// Display name as created (catalog keys are lowercased).
    pub name: String,
    /// Parameters in written order (defaults are constant expressions).
    pub params: Vec<MacroParamDef>,
    /// The parsed body template.
    pub body: MacroBody,
    /// The full `CREATE MACRO ...` statement text. Persisted in `.pink` so a
    /// reopen re-parses the exact definition; also shown by `DESCRIBE MACRO`.
    pub source: String,
}

impl MacroDef {
    /// Is this a table macro (`AS TABLE SELECT ...`)?
    pub fn is_table(&self) -> bool {
        matches!(self.body, MacroBody::Table(_))
    }

    /// The macro's kind as shown by `SHOW MACROS` / `DESCRIBE MACRO`.
    pub fn kind(&self) -> &'static str {
        if self.is_table() { "table" } else { "scalar" }
    }

    /// `name(a, b)` — parameters with defaults marked, for error messages and
    /// `SHOW MACROS`' `parameters` column.
    pub fn signature(&self) -> String {
        let params: Vec<String> = self
            .params
            .iter()
            .map(|p| {
                if p.default.is_some() {
                    format!("{} := …", p.name)
                } else {
                    p.name.clone()
                }
            })
            .collect();
        format!("{}({})", self.name, params.join(", "))
    }

    /// The comma-joined parameter names (DuckDB's `parameters` column shape).
    pub fn parameter_names(&self) -> String {
        self.params
            .iter()
            .map(|p| p.name.clone())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Resolve one call's arguments against a macro's parameters.
///
/// Positional arguments fill parameters left to right; named arguments (the
/// `name := expr` form, `=>` accepted too) target a parameter by name. Missing
/// trailing parameters take their declared default. Everything else — an
/// unknown name, a duplicate, an extra argument, or a missing parameter
/// without a default — is a clean arity error naming the candidate signature.
pub(crate) fn bind_arguments(
    def: &MacroDef,
    args: &[(Option<String>, Expr)],
) -> Result<HashMap<String, Expr>> {
    let mut supplied: Vec<Option<Expr>> = vec![None; def.params.len()];
    let mut positional = 0usize;
    for (name, expr) in args {
        match name {
            None => {
                if positional >= def.params.len() {
                    return Err(arity_error(def, args.len()));
                }
                if supplied[positional].is_some() {
                    return Err(arity_error(def, args.len()));
                }
                supplied[positional] = Some(expr.clone());
                positional += 1;
            }
            Some(name) => {
                let Some(index) = def
                    .params
                    .iter()
                    .position(|p| p.name.eq_ignore_ascii_case(name))
                else {
                    return Err(PinkDbError::Catalog(format!(
                        "macro {} has no parameter \"{name}\"",
                        def.signature()
                    )));
                };
                if supplied[index].replace(expr.clone()).is_some() {
                    return Err(PinkDbError::Catalog(format!(
                        "macro {}: parameter \"{name}\" is supplied twice",
                        def.signature()
                    )));
                }
            }
        }
    }
    let mut map = HashMap::with_capacity(def.params.len());
    for (i, param) in def.params.iter().enumerate() {
        let value = match supplied[i].take() {
            Some(e) => e,
            None => match &param.default {
                Some(e) => e.clone(),
                None => return Err(arity_error(def, args.len())),
            },
        };
        map.insert(param.name.to_ascii_lowercase(), value);
    }
    Ok(map)
}

fn arity_error(def: &MacroDef, supplied: usize) -> PinkDbError {
    PinkDbError::Type(format!(
        "macro {} does not support {supplied} argument(s) (candidate: {})",
        def.name,
        def.signature()
    ))
}

/// Count one substitution step against the statement's expansion budget.
fn tick(used: &std::cell::Cell<usize>, n: usize) -> Result<()> {
    let total = used.get().saturating_add(n);
    used.set(total);
    if total > MAX_MACRO_EXPANSION_NODES {
        return Err(PinkDbError::Execution(format!(
            "macro expansion exceeds the {MAX_MACRO_EXPANSION_NODES}-node limit: a macro body \
             duplicates its arguments too many times"
        )));
    }
    Ok(())
}

/// Clone `expr`, replacing every bare parameter reference with its argument.
pub(crate) fn substitute_expr(
    expr: &Expr,
    args: &HashMap<String, Expr>,
    used: &std::cell::Cell<usize>,
) -> Result<Expr> {
    tick(used, 1)?;
    Ok(match expr {
        Expr::Column { table: None, name } => {
            if let Some(replacement) = args.get(&name.to_ascii_lowercase()) {
                count_expr(replacement, used)?;
                replacement.clone()
            } else {
                expr.clone()
            }
        }
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(substitute_expr(left, args, used)?),
            right: Box::new(substitute_expr(right, args, used)?),
        },
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: Box::new(substitute_expr(expr, args, used)?),
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(substitute_expr(expr, args, used)?),
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(substitute_expr(expr, args, used)?),
            low: Box::new(substitute_expr(low, args, used)?),
            high: Box::new(substitute_expr(high, args, used)?),
            negated: *negated,
        },
        Expr::Function {
            name,
            args: call_args,
            distinct,
        } => Expr::Function {
            name: name.clone(),
            args: call_args
                .iter()
                .map(|a| substitute_expr(a, args, used))
                .collect::<Result<_>>()?,
            distinct: *distinct,
        },
        Expr::Window {
            name,
            args: call_args,
            over,
            distinct,
        } => Expr::Window {
            name: name.clone(),
            args: call_args
                .iter()
                .map(|a| substitute_expr(a, args, used))
                .collect::<Result<_>>()?,
            over: crate::sql::ast::OverClause {
                partition: over
                    .partition
                    .iter()
                    .map(|e| substitute_expr(e, args, used))
                    .collect::<Result<_>>()?,
                order: over
                    .order
                    .iter()
                    .map(|item| {
                        Ok(crate::sql::ast::OrderItem {
                            expr: substitute_expr(&item.expr, args, used)?,
                            desc: item.desc,
                            nulls_first: item.nulls_first,
                        })
                    })
                    .collect::<Result<_>>()?,
                frame: over.frame.clone(),
            },
            distinct: *distinct,
        },
        Expr::Coalesce(items) => Expr::Coalesce(
            items
                .iter()
                .map(|e| substitute_expr(e, args, used))
                .collect::<Result<_>>()?,
        ),
        Expr::Cast { expr, dtype } => Expr::Cast {
            expr: Box::new(substitute_expr(expr, args, used)?),
            dtype: *dtype,
        },
        Expr::Case { branches, else_ } => Expr::Case {
            branches: branches
                .iter()
                .map(|(c, v)| {
                    Ok((
                        substitute_expr(c, args, used)?,
                        substitute_expr(v, args, used)?,
                    ))
                })
                .collect::<Result<_>>()?,
            else_: match else_ {
                Some(e) => Some(Box::new(substitute_expr(e, args, used)?)),
                None => None,
            },
        },
        Expr::ScalarSubquery(query) => {
            Expr::ScalarSubquery(Box::new(substitute_select(query, args, used)?))
        }
        Expr::InSubquery {
            expr,
            query,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(substitute_expr(expr, args, used)?),
            query: Box::new(substitute_select(query, args, used)?),
            negated: *negated,
        },
        Expr::Exists { query, negated } => Expr::Exists {
            query: Box::new(substitute_select(query, args, used)?),
            negated: *negated,
        },
        Expr::NamedArg { name, expr } => Expr::NamedArg {
            name: name.clone(),
            expr: Box::new(substitute_expr(expr, args, used)?),
        },
        Expr::Literal(_)
        | Expr::Parameter(_)
        | Expr::Star
        | Expr::QualifiedStar(_)
        | Expr::Column { table: Some(_), .. } => expr.clone(),
    })
}

/// Clone a SELECT template, substituting parameters everywhere an expression
/// can appear (projections, FROM arguments/joins, WHERE, GROUP BY, HAVING,
/// ORDER BY, CTEs, set-operation branches and nested subqueries).
pub(crate) fn substitute_select(
    select: &Select,
    args: &HashMap<String, Expr>,
    used: &std::cell::Cell<usize>,
) -> Result<Select> {
    tick(used, 1)?;
    Ok(Select {
        with: select
            .with
            .iter()
            .map(|cte| {
                Ok(crate::sql::ast::CteDef {
                    name: cte.name.clone(),
                    cols: cte.cols.clone(),
                    query: Box::new(substitute_select(&cte.query, args, used)?),
                })
            })
            .collect::<Result<_>>()?,
        distinct: select.distinct,
        projections: select
            .projections
            .iter()
            .map(|p| match p {
                Projection::Expr { expr, alias } => Ok(Projection::Expr {
                    expr: substitute_expr(expr, args, used)?,
                    alias: alias.clone(),
                }),
                other => Ok(other.clone()),
            })
            .collect::<Result<_>>()?,
        from: match &select.from {
            Some(from) => Some(substitute_from(from, args, used)?),
            None => None,
        },
        where_clause: match &select.where_clause {
            Some(e) => Some(substitute_expr(e, args, used)?),
            None => None,
        },
        group_by: select
            .group_by
            .iter()
            .map(|e| substitute_expr(e, args, used))
            .collect::<Result<_>>()?,
        having: match &select.having {
            Some(e) => Some(substitute_expr(e, args, used)?),
            None => None,
        },
        order_by: select
            .order_by
            .iter()
            .map(|item| {
                Ok(crate::sql::ast::OrderItem {
                    expr: substitute_expr(&item.expr, args, used)?,
                    desc: item.desc,
                    nulls_first: item.nulls_first,
                })
            })
            .collect::<Result<_>>()?,
        limit: select.limit,
        offset: select.offset,
        unions: select
            .unions
            .iter()
            .map(|(op, branch)| Ok((*op, substitute_select(branch, args, used)?)))
            .collect::<Result<_>>()?,
    })
}

fn substitute_from(
    from: &FromClause,
    args: &HashMap<String, Expr>,
    used: &std::cell::Cell<usize>,
) -> Result<FromClause> {
    tick(used, 1)?;
    Ok(FromClause {
        source: substitute_source(&from.source, args, used)?,
        alias: from.alias.clone(),
        at: from.at,
        joins: from
            .joins
            .iter()
            .map(|join| {
                Ok(crate::sql::ast::JoinClause {
                    kind: join.kind,
                    source: substitute_source(&join.source, args, used)?,
                    alias: join.alias.clone(),
                    at: join.at,
                    on: match &join.on {
                        Some(e) => Some(substitute_expr(e, args, used)?),
                        None => None,
                    },
                })
            })
            .collect::<Result<_>>()?,
    })
}

fn substitute_source(
    source: &FromSource,
    args: &HashMap<String, Expr>,
    used: &std::cell::Cell<usize>,
) -> Result<FromSource> {
    tick(used, 1)?;
    match source {
        FromSource::Derived(query) => Ok(FromSource::Derived(Box::new(substitute_select(
            query, args, used,
        )?))),
        FromSource::Function {
            name,
            args: call_args,
        } => Ok(FromSource::Function {
            name: name.clone(),
            args: call_args
                .iter()
                .map(|a| {
                    Ok(crate::sql::ast::TableFunctionCallArg {
                        name: a.name.clone(),
                        expr: substitute_expr(&a.expr, args, used)?,
                    })
                })
                .collect::<Result<_>>()?,
        }),
        FromSource::Table { .. } | FromSource::Path(_) => Ok(source.clone()),
    }
}

/// Count cloned argument nodes (they count against the same budget as the
/// template nodes being walked).
fn count_expr(expr: &Expr, used: &std::cell::Cell<usize>) -> Result<()> {
    tick(used, 1)?;
    match expr {
        Expr::Binary { left, right, .. } => {
            count_expr(left, used)?;
            count_expr(right, used)
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::NamedArg { expr, .. } => count_expr(expr, used),
        Expr::Between {
            expr, low, high, ..
        } => {
            count_expr(expr, used)?;
            count_expr(low, used)?;
            count_expr(high, used)
        }
        Expr::Function { args, .. } | Expr::Coalesce(args) => {
            for a in args {
                count_expr(a, used)?;
            }
            Ok(())
        }
        Expr::Window { args, over, .. } => {
            for a in args {
                count_expr(a, used)?;
            }
            for e in &over.partition {
                count_expr(e, used)?;
            }
            for item in &over.order {
                count_expr(&item.expr, used)?;
            }
            Ok(())
        }
        Expr::ScalarSubquery(query) | Expr::Exists { query, .. } => count_select(query, used),
        Expr::InSubquery { expr, query, .. } => {
            count_expr(expr, used)?;
            count_select(query, used)
        }
        Expr::Case { branches, else_ } => {
            for (c, v) in branches {
                count_expr(c, used)?;
                count_expr(v, used)?;
            }
            if let Some(e) = else_ {
                count_expr(e, used)?;
            }
            Ok(())
        }
        Expr::Literal(_)
        | Expr::Parameter(_)
        | Expr::Star
        | Expr::QualifiedStar(_)
        | Expr::Column { .. } => Ok(()),
    }
}

fn count_select(select: &Select, used: &std::cell::Cell<usize>) -> Result<()> {
    tick(used, 1)?;
    for cte in &select.with {
        count_select(&cte.query, used)?;
    }
    for p in &select.projections {
        if let Projection::Expr { expr, .. } = p {
            count_expr(expr, used)?;
        }
    }
    if let Some(from) = &select.from {
        count_source(&from.source, used)?;
        for join in &from.joins {
            count_source(&join.source, used)?;
            if let Some(on) = &join.on {
                count_expr(on, used)?;
            }
        }
    }
    if let Some(e) = &select.where_clause {
        count_expr(e, used)?;
    }
    for e in &select.group_by {
        count_expr(e, used)?;
    }
    if let Some(e) = &select.having {
        count_expr(e, used)?;
    }
    for item in &select.order_by {
        count_expr(&item.expr, used)?;
    }
    for (_, branch) in &select.unions {
        count_select(branch, used)?;
    }
    Ok(())
}

fn count_source(source: &FromSource, used: &std::cell::Cell<usize>) -> Result<()> {
    tick(used, 1)?;
    match source {
        FromSource::Derived(query) => count_select(query, used),
        FromSource::Function { args, .. } => {
            for a in args {
                count_expr(&a.expr, used)?;
            }
            Ok(())
        }
        FromSource::Table { .. } | FromSource::Path(_) => Ok(()),
    }
}

/// Every macro name a body calls (scalar `f(...)` and table `FROM f(...)`),
/// lowercased and deduped. Used for the `CREATE`-time recursion check.
pub(crate) fn body_calls(body: &MacroBody) -> Vec<String> {
    let mut out = HashSet::new();
    match body {
        MacroBody::Scalar(expr) => collect_expr_calls(expr, &mut out),
        MacroBody::Table(select) => collect_select_calls(select, &mut out),
    }
    let mut out: Vec<String> = out.into_iter().collect();
    out.sort();
    out
}

fn collect_expr_calls(expr: &Expr, out: &mut HashSet<String>) {
    match expr {
        Expr::Function { name, args, .. } => {
            out.insert(name.to_ascii_lowercase());
            for a in args {
                collect_expr_calls(a, out);
            }
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_calls(left, out);
            collect_expr_calls(right, out);
        }
        Expr::Unary { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::NamedArg { expr, .. } => collect_expr_calls(expr, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_calls(expr, out);
            collect_expr_calls(low, out);
            collect_expr_calls(high, out);
        }
        Expr::Window { args, over, .. } => {
            for a in args {
                collect_expr_calls(a, out);
            }
            for e in &over.partition {
                collect_expr_calls(e, out);
            }
            for item in &over.order {
                collect_expr_calls(&item.expr, out);
            }
        }
        Expr::Coalesce(args) => {
            for a in args {
                collect_expr_calls(a, out);
            }
        }
        Expr::ScalarSubquery(query) | Expr::Exists { query, .. } => {
            collect_select_calls(query, out)
        }
        Expr::InSubquery { expr, query, .. } => {
            collect_expr_calls(expr, out);
            collect_select_calls(query, out);
        }
        Expr::Case { branches, else_ } => {
            for (c, v) in branches {
                collect_expr_calls(c, out);
                collect_expr_calls(v, out);
            }
            if let Some(e) = else_ {
                collect_expr_calls(e, out);
            }
        }
        Expr::Literal(_)
        | Expr::Parameter(_)
        | Expr::Star
        | Expr::QualifiedStar(_)
        | Expr::Column { .. } => {}
    }
}

fn collect_select_calls(select: &Select, out: &mut HashSet<String>) {
    for cte in &select.with {
        collect_select_calls(&cte.query, out);
    }
    for p in &select.projections {
        if let Projection::Expr { expr, .. } = p {
            collect_expr_calls(expr, out);
        }
    }
    if let Some(from) = &select.from {
        collect_source_calls(&from.source, out);
        for join in &from.joins {
            collect_source_calls(&join.source, out);
            if let Some(on) = &join.on {
                collect_expr_calls(on, out);
            }
        }
    }
    if let Some(e) = &select.where_clause {
        collect_expr_calls(e, out);
    }
    for e in &select.group_by {
        collect_expr_calls(e, out);
    }
    if let Some(e) = &select.having {
        collect_expr_calls(e, out);
    }
    for item in &select.order_by {
        collect_expr_calls(&item.expr, out);
    }
    for (_, branch) in &select.unions {
        collect_select_calls(branch, out);
    }
}

fn collect_source_calls(source: &FromSource, out: &mut HashSet<String>) {
    match source {
        FromSource::Derived(query) => collect_select_calls(query, out),
        FromSource::Function { name, args } => {
            out.insert(name.to_ascii_lowercase());
            for a in args {
                collect_expr_calls(&a.expr, out);
            }
        }
        FromSource::Table { .. } | FromSource::Path(_) => {}
    }
}

/// Does this (not yet installed) body call `target`, directly or through the
/// stored macro definitions?
pub(crate) fn body_reaches(
    db: &crate::storage::PinkDb,
    db_name: &str,
    body: &MacroBody,
    target: &str,
) -> bool {
    let target = target.to_ascii_lowercase();
    let mut seen = HashSet::new();
    body_calls(body)
        .iter()
        .any(|call| reaches(db, db_name, call, &target, &mut seen))
}

/// Does the definition of `start` (transitively) call `target`?
pub(crate) fn reaches(
    db: &crate::storage::PinkDb,
    db_name: &str,
    start: &str,
    target: &str,
    seen: &mut HashSet<String>,
) -> bool {
    let key = start.to_ascii_lowercase();
    if key == target {
        return true;
    }
    if !seen.insert(key.clone()) {
        return false;
    }
    let Ok(def) = db.macro_in(db_name, &key) else {
        return false;
    };
    body_calls(&def.body)
        .iter()
        .any(|call| reaches(db, db_name, call, target, seen))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::Statement;
    use crate::types::Value;

    fn lit(i: i32) -> Expr {
        Expr::Literal(Value::Integer(i))
    }

    fn def(params: Vec<MacroParamDef>, body: Expr) -> MacroDef {
        MacroDef {
            name: "m".to_string(),
            params,
            body: MacroBody::Scalar(body),
            source: String::new(),
        }
    }

    #[test]
    fn arguments_fill_positional_then_named_then_defaults() {
        let d = def(
            vec![
                MacroParamDef {
                    name: "a".to_string(),
                    default: None,
                },
                MacroParamDef {
                    name: "b".to_string(),
                    default: Some(lit(10)),
                },
            ],
            lit(0),
        );
        let map = bind_arguments(&d, &[(None, lit(1))]).unwrap();
        assert_eq!(map["a"], lit(1));
        assert_eq!(map["b"], lit(10));
        let map = bind_arguments(&d, &[(None, lit(1)), (Some("b".to_string()), lit(2))]).unwrap();
        assert_eq!(map["b"], lit(2));
        let map = bind_arguments(
            &d,
            &[
                (Some("b".to_string()), lit(2)),
                (Some("a".to_string()), lit(3)),
            ],
        )
        .unwrap();
        assert_eq!(map["a"], lit(3));
        // Missing a required parameter, an unknown name, a duplicate and an
        // extra argument are all clean errors.
        assert!(bind_arguments(&d, &[]).is_err());
        assert!(bind_arguments(&d, &[(Some("z".into()), lit(1))]).is_err());
        assert!(bind_arguments(&d, &[(None, lit(1)), (Some("a".into()), lit(2))]).is_err());
        assert!(bind_arguments(&d, &[(None, lit(1)), (None, lit(2)), (None, lit(3))]).is_err());
    }

    #[test]
    fn substitution_splices_parameters_and_leaves_qualified_columns() {
        let body = crate::sql::parse_statements("SELECT a + t.a FROM t WHERE b = a;")
            .unwrap()
            .pop()
            .unwrap();
        let crate::sql::ast::Statement::Select(select) = body else {
            panic!()
        };
        let used = std::cell::Cell::new(0);
        let mut args = HashMap::new();
        args.insert("a".to_string(), lit(7));
        let out = substitute_select(&select, &args, &used).unwrap();
        // The bare `a` in the projection and the WHERE is spliced; the
        // qualified `t.a` is a column and stays.
        assert!(matches!(
            &out.projections[0],
            Projection::Expr {
                expr: Expr::Binary { left, .. },
                ..
            } if **left == lit(7)
        ));
        assert_eq!(
            out.where_clause,
            Some(Expr::Binary {
                op: crate::sql::ast::BinaryOp::Eq,
                left: Box::new(Expr::Column {
                    table: None,
                    name: "b".to_string()
                }),
                right: Box::new(lit(7)),
            })
        );
        assert!(used.get() > 0);
    }

    #[test]
    fn body_calls_sees_scalar_and_table_macro_calls() {
        let Statement::CreateMacro { body, .. } =
            crate::sql::parse_statements("CREATE MACRO m(x) AS TABLE SELECT f(x) FROM g(x);")
                .unwrap()
                .pop()
                .unwrap()
        else {
            panic!()
        };
        assert_eq!(body_calls(&body), vec!["f".to_string(), "g".to_string()]);
    }

    #[test]
    fn expansion_budget_stops_runaway_duplication() {
        let used = std::cell::Cell::new(MAX_MACRO_EXPANSION_NODES);
        let mut args = HashMap::new();
        args.insert("x".to_string(), lit(1));
        let err = substitute_expr(&lit(1), &args, &used).unwrap_err();
        assert!(err.to_string().contains("node limit"), "{err}");
    }
}
