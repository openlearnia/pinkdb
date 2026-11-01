//! Triggers (wave 100): stored `CREATE TRIGGER` definitions, and the code
//! that fires them from a writing statement.
//!
//! # What a trigger is
//!
//! A trigger is a named catalog object owned by a table's database:
//!
//! ```sql
//! CREATE TRIGGER audit AFTER INSERT OR UPDATE OF amount ON orders
//!   FOR EACH ROW WHEN (NEW.amount > 100)
//!   EXECUTE PROCEDURE log_change(NEW.id, 'amount');
//! ```
//!
//! The action is an ordinary statement — `EXECUTE PROCEDURE p(args)` is a
//! [`Statement::Call`], everything else (`INSERT …`, `UPDATE …`, `DELETE …`,
//! `DO …`, …) runs as written. A trigger body therefore needs no separate
//! language: it is bound and run by the same machinery a `CALL` body uses
//! ([`crate::procedure::run_call_statement`]), with `OLD`/`NEW` visible as
//! qualified row variables, and it is logged *nowhere*: the firing statement
//! is what the WAL holds, so replay re-fires the triggers deterministically
//! (never twice).
//!
//! # Semantics
//!
//! * **Order.** Triggers of one event fire in creation order. All `BEFORE`
//!   triggers run before the row change, all `AFTER` triggers after it; within
//!   a timing, `ROW` and `STATEMENT` triggers interleave in creation order.
//! * **Row triggers.** `OLD` and `NEW` are the firing row's images. A `BEFORE
//!   ROW` trigger may assign `NEW.<col>` in its body (a procedure body's `SET
//!   NEW.col = …`), and the change reaches the stored row; `OLD` is read-only.
//!   A `BEFORE ROW` trigger that returns no value is *not* a skip: its only
//!   effect is whatever it changed (DuckDB has no skip either).
//! * **Statement triggers.** `REFERENCING OLD TABLE AS o NEW TABLE AS n` hands
//!   an `AFTER STATEMENT` trigger the statement's changed rows as read-only
//!   relations (`FROM o`, `FROM n`). A `BEFORE STATEMENT` trigger gets neither
//!   images nor tables: it runs before the statement touched anything.
//! * **Atomicity.** Triggers run inside the firing statement's transaction. An
//!   error in a trigger aborts and rolls back the whole statement, autocommit
//!   included — the outermost firing statement takes the store snapshot and
//!   restores it ([`crate::storage::PinkDb::savepoint_restore`]).
//! * **Recursion.** A trigger body's DML fires triggers too, bounded by
//!   [`MAX_TRIGGER_DEPTH`]; the limit is reported, never an endless loop.
//! * **Privileges.** Creating or dropping a trigger needs the `CREATE`
//!   privilege on the database (the same requirement `ALTER TABLE` and index
//!   DDL have). A body runs with **invoker** rights: every statement it binds
//!   is checked against the user whose statement fired it, so a trigger can
//!   never write a table that user could not write.
//! * **Visibility inside a body.** A body is its own statement: it does not
//!   see the firing statement's other rows, except that (row triggers on a
//!   multi-row statement) it runs per row in row order, so a body that reads
//!   the target table sees the rows already written by this statement.
//!
//! # Determinism and replay
//!
//! The WAL records the *firing* statement, never a trigger body, and replay
//! re-executes the firing statement, which re-fires its triggers — exactly the
//! same effects, once. That holds only if the trigger's result depends solely
//! on the firing row and the catalog, so a body (and a `WHEN` clause) may not
//! call a volatile extension function or any other nondeterministic source
//! (`now()`, `random()` — the engine has no builtin clock or RNG; a loadable
//! extension's volatile function is refused at bind time by
//! [`crate::procedure::run_call_statement`], exactly as for a `CALL`).

use crate::plan::LogicalPlan;
use crate::sql::ast::{Expr, Statement};
use crate::storage::{DataTable, PinkDb};
use crate::types::{DataType, Value};
use crate::{PinkDbError, QueryResult, Result};

/// How deeply triggers may nest before the engine refuses: a trigger body's
/// own DML may fire triggers, so a recursive trigger would otherwise loop
/// forever. 16 matches [`crate::procedure::MAX_CALL_DEPTH`]'s spirit (deep
/// enough for layered audit chains, shallow enough to fail fast).
pub(crate) const MAX_TRIGGER_DEPTH: usize = 16;

/// `BEFORE` or `AFTER` the row/statement change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerTiming {
    Before,
    After,
}

impl TriggerTiming {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            TriggerTiming::Before => "BEFORE",
            TriggerTiming::After => "AFTER",
        }
    }
}

/// The statement kinds a trigger fires for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

impl TriggerEvent {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            TriggerEvent::Insert => "INSERT",
            TriggerEvent::Update => "UPDATE",
            TriggerEvent::Delete => "DELETE",
        }
    }
}

/// `FOR EACH ROW` / `FOR EACH STATEMENT`. The default is `ROW` (DuckDB's
/// triggers are always row-level; `STATEMENT` is the extension that makes
/// `REFERENCING … TABLE` useful).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerLevel {
    Row,
    Statement,
}

impl TriggerLevel {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            TriggerLevel::Row => "ROW",
            TriggerLevel::Statement => "STATEMENT",
        }
    }
}

/// One stored trigger, persisted as its `CREATE TRIGGER …` source text.
#[derive(Debug, Clone, PartialEq)]
pub struct TriggerDef {
    pub name: String,
    /// The database the trigger — and its table — belongs to.
    pub db: String,
    /// The schema the trigger's table and body resolve in.
    pub schema: String,
    pub table: String,
    pub timing: TriggerTiming,
    /// The events it fires for, in written order.
    pub events: Vec<TriggerEvent>,
    /// `UPDATE OF a, b` narrows an `UPDATE` event to those columns; empty
    /// means every column.
    pub columns: Vec<String>,
    pub level: TriggerLevel,
    pub old_table: Option<String>,
    pub new_table: Option<String>,
    pub when: Option<Expr>,
    pub action: Statement,
    /// The `CREATE TRIGGER …` text the runner attached; `.pink` stores the
    /// definition as this text and re-parses it on load (like a macro).
    pub source: String,
}

impl TriggerDef {
    /// Does this definition fire for `event`, and (for `UPDATE OF`) does the
    /// statement write one of the named columns? `changed` is `None` when the
    /// caller cannot narrow (a delete, an insert, or an update whose SET list
    /// is unknown).
    fn fires_on(
        &self,
        event: TriggerEvent,
        changed: Option<&std::collections::HashSet<String>>,
    ) -> bool {
        if !self.events.contains(&event) {
            return false;
        }
        if event != TriggerEvent::Update || self.columns.is_empty() {
            return true;
        }
        let Some(changed) = changed else {
            return true;
        };
        self.columns
            .iter()
            .any(|c| changed.contains(&c.to_ascii_lowercase()))
    }
}

/// The `NEW`/`OLD` row a row trigger's body sees, and the transition tables a
/// statement trigger's body sees, while a body runs. Live on the store
/// (`PinkDb::trigger_scope`) so a nested statement's bind picks it up from
/// wherever it runs (the action, or any statement of a procedure it calls).
/// One row image: `(alias, [(column, type, value)])` — `old` and/or `new`.
pub(crate) type TriggerRow = (String, Vec<(String, DataType, Value)>);

#[derive(Debug, Clone, Default)]
pub(crate) struct TriggerScope {
    /// `(alias, [(column, type, value)])` — `old` and/or `new`.
    pub(crate) rows: Vec<TriggerRow>,
    /// `(alias, output)` for `REFERENCING … TABLE`.
    pub(crate) tables: Vec<(String, crate::exec::Output)>,
}

/// Build a definition from the parsed `CREATE TRIGGER` statement. Shared by
/// the binder (first creation) and the `.pink` reader (reopen), so both agree
/// on exactly what a definition means.
pub(crate) fn def_from_statement(
    ns: &crate::storage::SchemaPath,
    stmt: &Statement,
) -> Result<TriggerDef> {
    let Statement::CreateTrigger {
        name,
        table,
        timing,
        events,
        update_columns,
        level,
        old_table,
        new_table,
        when,
        action,
        ..
    } = stmt
    else {
        return Err(PinkDbError::Execution(
            "internal error: not a CREATE TRIGGER statement".to_string(),
        ));
    };
    Ok(TriggerDef {
        name: name.clone(),
        db: ns.db().to_string(),
        schema: ns.schema().to_string(),
        table: table.clone(),
        timing: *timing,
        events: events.clone(),
        // `UPDATE OF a, b` narrows every UPDATE event of the definition.
        columns: update_columns.clone(),
        level: *level,
        old_table: old_table.clone(),
        new_table: new_table.clone(),
        when: when.clone(),
        action: (**action).clone(),
        source: String::new(),
    })
}

/// The `(database, table, event)` a plan writes, if it is a DML statement — the
/// cheap pre-check that keeps trigger-free tables on the plain path.
pub(crate) fn plan_dml(
    plan: &LogicalPlan,
    default_ns: &crate::storage::SchemaPath,
) -> Option<(crate::storage::SchemaPath, String, TriggerEvent)> {
    let (qns, table, event) = match plan {
        LogicalPlan::Insert { ns, table, .. } | LogicalPlan::InsertSelect { ns, table, .. } => {
            (ns, table, TriggerEvent::Insert)
        }
        LogicalPlan::Update { ns, table, .. } => (ns, table, TriggerEvent::Update),
        LogicalPlan::Delete { ns, table, .. } => (ns, table, TriggerEvent::Delete),
        // MERGE fires INSERT/UPDATE/DELETE row triggers through the write
        // funnels; its one statement-level event is UPDATE.
        LogicalPlan::Merge { ns, table, .. } => (ns, table, TriggerEvent::Update),
        _ => return None,
    };
    let ns = qns.clone().unwrap_or_else(|| default_ns.clone());
    Some((ns, table.clone(), event))
}

/// The columns an `UPDATE` statement writes, by name — the input to
/// `UPDATE OF col` narrowing.
fn updated_columns(
    db: &PinkDb,
    plan: &LogicalPlan,
    target_ns: &crate::storage::SchemaPath,
    table: &str,
) -> Option<std::collections::HashSet<String>> {
    let LogicalPlan::Update { sets, .. } = plan else {
        return None;
    };
    let schema = db.table_in(target_ns, table).ok()?.schema();
    Some(
        sets.iter()
            .filter_map(|(col, _)| schema.get(*col).map(|(n, _)| n.to_ascii_lowercase()))
            .collect(),
    )
}

/// Every trigger of `event` on the table, in creation order.
fn triggers_for(
    db: &PinkDb,
    target_ns: &crate::storage::SchemaPath,
    table: &str,
    event: TriggerEvent,
    changed: Option<&std::collections::HashSet<String>>,
) -> Vec<TriggerDef> {
    db.triggers_in(target_ns)
        .iter()
        .filter(|t| t.table.eq_ignore_ascii_case(table) && t.fires_on(event, changed))
        .cloned()
        .collect()
}

/// The row-level triggers of `event` on the table, in creation order. Empty
/// (and heap-free) on a table with no triggers: the write path's only cost.
pub(crate) fn row_defs(
    db: &PinkDb,
    target_ns: &crate::storage::SchemaPath,
    table: &str,
    event: TriggerEvent,
    changed: Option<&std::collections::HashSet<String>>,
) -> Vec<TriggerDef> {
    let mut out: Vec<TriggerDef> = Vec::new();
    for def in db.triggers_in(target_ns) {
        if def.level == TriggerLevel::Row
            && def.table.eq_ignore_ascii_case(table)
            && def.fires_on(event, changed)
        {
            out.push(def.clone());
        }
    }
    out
}

/// True when the table has any trigger at all: the only lookup a writing
/// statement pays when nothing is defined.
pub(crate) fn has_triggers(
    db: &PinkDb,
    target_ns: &crate::storage::SchemaPath,
    table: &str,
) -> bool {
    db.triggers_in(target_ns)
        .iter()
        .any(|t| t.table.eq_ignore_ascii_case(table))
}

/// Run a DML statement that has at least one trigger, wrapping the plain
/// execution with the statement-level triggers and the atomicity/depth guard.
pub(crate) fn run_with_triggers(
    db: &mut PinkDb,
    plan: &LogicalPlan,
    budget: &std::sync::Arc<crate::exec::ExecBudget>,
    target_ns: &crate::storage::SchemaPath,
    table: &str,
    event: TriggerEvent,
) -> Result<QueryResult> {
    let changed = updated_columns(db, plan, target_ns, table);
    let defs = triggers_for(db, target_ns, table, event, changed.as_ref());
    guarded(db, |db| {
        for def in defs
            .iter()
            .filter(|d| d.timing == TriggerTiming::Before && d.level == TriggerLevel::Statement)
        {
            run_trigger(db, def, TriggerScope::default(), budget)?;
        }
        let after: Vec<&TriggerDef> = defs
            .iter()
            .filter(|d| d.timing == TriggerTiming::After && d.level == TriggerLevel::Statement)
            .collect();
        if !after.is_empty() {
            // Only an AFTER STATEMENT trigger needs the changed rows; the DML
            // arms append to this. Nested statements save/restore it (see
            // `run_trigger`), so a body's own rows never leak into it.
            db.trigger_capture = Some(Vec::new());
        }
        let result = crate::exec::run_plain(db, plan, budget.clone());
        let capture = db.trigger_capture.take().unwrap_or_default();
        let result = result?;
        if !after.is_empty() {
            let schema: Vec<(String, DataType)> = db
                .table_in(target_ns, table)?
                .schema()
                .into_iter()
                .map(|(n, t)| (n.to_ascii_lowercase(), t))
                .collect();
            for def in after {
                let mut scope = TriggerScope::default();
                if let Some(alias) = &def.old_table {
                    let rows: Vec<Vec<Value>> = capture
                        .iter()
                        .filter(|(old, _)| !old.is_empty())
                        .map(|(old, _)| old.clone())
                        .collect();
                    scope.tables.push((
                        alias.clone(),
                        crate::exec::output_from_rows(&schema, &rows)?,
                    ));
                }
                if let Some(alias) = &def.new_table {
                    let rows: Vec<Vec<Value>> = capture
                        .iter()
                        .filter(|(_, new)| !new.is_empty())
                        .map(|(_, new)| new.clone())
                        .collect();
                    scope.tables.push((
                        alias.clone(),
                        crate::exec::output_from_rows(&schema, &rows)?,
                    ));
                }
                run_trigger(db, def, scope, budget)?;
            }
        }
        Ok(result)
    })
}

/// Fire the `BEFORE`/`AFTER` `ROW` triggers of one timing for one row, and
/// return the row's (possibly modified) `NEW` image. Called from the DML arms
/// with the statement's matching defs, so the lookup happens once.
pub(crate) fn fire_row_triggers(
    db: &mut PinkDb,
    defs: &[TriggerDef],
    timing: TriggerTiming,
    old: Option<&[Value]>,
    new: Option<&[Value]>,
    budget: &std::sync::Arc<crate::exec::ExecBudget>,
) -> Result<Option<Vec<Value>>> {
    let mut image = new.map(|n| n.to_vec());
    for def in defs.iter().filter(|d| d.timing == timing) {
        let scope = row_scope(db, def, old, image.as_deref())?;
        let scope = run_trigger(db, def, scope, budget)?;
        if let Some(rows) = scope.rows.iter().find(|(a, _)| a == "new").map(|(_, c)| c) {
            image = Some(rows.iter().map(|(_, _, v)| v.clone()).collect());
        }
    }
    Ok(image)
}

/// Where the next appended row lands: the tail chunk when it has room, a new
/// chunk otherwise (the rule [`DataTable::append_row`] follows).
fn next_row_id(t: &DataTable) -> (usize, usize) {
    let chunks = t.num_chunks();
    if chunks == 0 {
        return (0, 0);
    }
    // Appends only ever extend the tail chunk.
    let tail = t.columns[0].chunks[chunks - 1].len();
    if tail >= crate::vector::CHUNK_SIZE {
        (chunks, 0)
    } else {
        (chunks - 1, tail)
    }
}

/// Append staged rows, firing the table's `INSERT` row triggers per row.
///
/// The id row `offset` rows past the tail will get once appended.
fn predicted_id(t: &DataTable, offset: usize) -> crate::keys::RowId {
    let global = t.num_rows() + offset;
    crate::keys::RowId::new(
        global / crate::vector::CHUNK_SIZE,
        global % crate::vector::CHUNK_SIZE,
    )
}

/// A table with no triggers and no waiting `AFTER STATEMENT` transition table
/// runs the unchanged batch path — the trigger machinery costs it nothing.
pub(crate) fn insert_rows(
    db: &mut PinkDb,
    target_ns: &crate::storage::SchemaPath,
    table: &str,
    rows: Vec<Vec<Value>>,
    budget: &std::sync::Arc<crate::exec::ExecBudget>,
) -> Result<()> {
    let defs = row_defs(db, target_ns, table, TriggerEvent::Insert, None);
    // Primary/unique keys are enforced here, the funnel every SQL INSERT
    // (VALUES, SELECT, CTAS row source, procedures, WAL replay) uses. The
    // occupancy map is cached on the table, so a statement pays O(rows
    // written) instead of an O(table) scan.
    let mut keys = crate::keys::take_or_build(db, target_ns, table)?;
    if defs.is_empty() {
        // No row triggers: validate every image before the first append, so a
        // duplicate key leaves the table untouched (a statement is atomic).
        if let Some(keys) = &mut keys {
            let t = db.table_in(target_ns, table)?;
            // Every row is reserved before the first append, so the ids are
            // predicted from the row count (`next_row_id` would return the
            // same position for all of them).
            for (i, values) in rows.iter().enumerate() {
                keys.check(values, t, None)?;
                keys.reserve(values, predicted_id(t, i));
            }
        }
        if db.trigger_capture.is_none() {
            let t = db.table_mut_in(target_ns, table)?;
            for values in rows {
                t.append_row(values)?;
            }
            crate::keys::store(db, target_ns, table, keys);
            return Ok(());
        }
        let mut appended = Vec::with_capacity(rows.len());
        {
            let t = db.table_mut_in(target_ns, table)?;
            for values in rows {
                t.append_row(values.clone())?;
                appended.push(values);
            }
        }
        let capture = db
            .trigger_capture
            .as_mut()
            .expect("checked above that a capture is installed");
        capture.extend(appended.into_iter().map(|v| (Vec::new(), v)));
        crate::keys::store(db, target_ns, table, keys);
        return Ok(());
    }
    let mut capture = db.trigger_capture.take();
    let out = (|| -> Result<()> {
        for values in rows {
            budget.check_deadline()?;
            // A BEFORE trigger may assign NEW.col; the modified image is what
            // the table stores and what AFTER triggers and RETURNING see.
            let new = if defs.is_empty() {
                values
            } else {
                fire_row_triggers(
                    db,
                    &defs,
                    TriggerTiming::Before,
                    None,
                    Some(&values),
                    budget,
                )?
                .unwrap_or(values)
            };
            if let Some(keys) = &mut keys {
                let t = db.table_in(target_ns, table)?;
                keys.check(&new, t, None)?;
                let (chunk, row) = next_row_id(t);
                keys.reserve(&new, crate::keys::RowId::new(chunk, row));
            }
            db.table_mut_in(target_ns, table)?.append_row(new.clone())?;
            if let Some(c) = &mut capture {
                c.push((Vec::new(), new.clone()));
            }
            if !defs.is_empty() {
                fire_row_triggers(db, &defs, TriggerTiming::After, None, Some(&new), budget)?;
            }
        }
        Ok(())
    })();
    db.trigger_capture = capture;
    if out.is_ok() {
        crate::keys::store(db, target_ns, table, keys);
    }
    out
}

/// Apply evaluated `UPDATE` writes, firing the table's `UPDATE` row triggers
/// per affected row. `per_chunk` is the executor's phase-1 result: the column
/// writes for each chunk, evaluated against the pre-mutation rows.
pub(crate) fn update_rows(
    db: &mut PinkDb,
    target_ns: &crate::storage::SchemaPath,
    table: &str,
    per_chunk: Vec<Vec<(usize, usize, Value)>>,
    budget: &std::sync::Arc<crate::exec::ExecBudget>,
) -> Result<()> {
    // Keys are enforced below, on the row image the write produces (a BEFORE
    // trigger's NEW included), so the plain fast path is only safe for a
    // keyless table.
    // Only a SET that names a key column can break a key; a bulk UPDATE of
    // other columns keeps the plain path. (A BEFORE trigger can still rewrite
    // a key, which is why the fast path also requires that there is none.)
    let keyed = {
        let t = db.table_in(target_ns, table)?;
        let key_cols: Vec<usize> = t
            .constraints
            .keys
            .iter()
            .flat_map(|k| k.columns.iter().copied())
            .collect();
        per_chunk
            .iter()
            .flatten()
            .any(|(col, _, _)| key_cols.contains(col))
    };
    if !keyed && db.trigger_capture.is_none() && !has_triggers(db, target_ns, table) {
        let t = db.table_mut_in(target_ns, table)?;
        for (ci, writes) in per_chunk.iter().enumerate() {
            t.update_rows(ci, writes)?;
        }
        return Ok(());
    }
    let schema = db.table_in(target_ns, table)?.schema();
    // `UPDATE OF col` narrowing sees the written columns, not the predicate.
    // Deduplicate by index: a bulk UPDATE carries one entry per row.
    let mut seen = vec![false; schema.len()];
    let mut changed: std::collections::HashSet<String> = std::collections::HashSet::new();
    for writes in &per_chunk {
        for (col, _, _) in writes {
            if let Some(flag) = seen.get_mut(*col)
                && !*flag
            {
                *flag = true;
                if let Some((name, _)) = schema.get(*col) {
                    changed.insert(name.to_ascii_lowercase());
                }
            }
        }
    }
    let defs = row_defs(db, target_ns, table, TriggerEvent::Update, Some(&changed));
    // A BEFORE trigger can rewrite a key column, so a keyed table always needs
    // the occupancy map here (cheap: it is cached on the table).
    let mut keys = crate::keys::take_or_build(db, target_ns, table)?;
    let mut capture = db.trigger_capture.take();
    let out = (|| -> Result<()> {
        for (ci, writes) in per_chunk.iter().enumerate() {
            budget.check_deadline()?;
            if writes.is_empty() {
                continue;
            }
            // Group this chunk's writes by row, then take the chunk's rows as
            // full OLD images before the first write: OLD must be the row as
            // it was when the statement started.
            let mut by_row: std::collections::BTreeMap<usize, Vec<(usize, Value)>> =
                std::collections::BTreeMap::new();
            for (col, row, value) in writes {
                by_row.entry(*row).or_default().push((*col, value.clone()));
            }
            let images: Vec<Vec<Value>> = {
                let chunk = db
                    .table_in(target_ns, table)?
                    .scan_chunk(ci)
                    .ok_or_else(|| {
                        PinkDbError::Execution("storage changed during UPDATE".to_string())
                    })?;
                (0..chunk.num_rows())
                    .map(|r| chunk.columns.iter().map(|c| c.get(r)).collect())
                    .collect()
            };
            for (row, sets) in by_row {
                budget.check_deadline()?;
                let Some(old) = images.get(row) else { continue };
                let old = old.clone();
                let mut new = old.clone();
                for (col, value) in sets {
                    new[col] = value;
                }
                if !defs.is_empty()
                    && let Some(modified) = fire_row_triggers(
                        db,
                        &defs,
                        TriggerTiming::Before,
                        Some(&old),
                        Some(&new),
                        budget,
                    )?
                {
                    new = modified;
                }
                // Write every column the row actually changed: the SET list
                // plus whatever a BEFORE trigger assigned to NEW.
                let writes: Vec<(usize, usize, Value)> = old
                    .iter()
                    .zip(new.iter())
                    .enumerate()
                    .filter(|(_, (o, n))| o != n)
                    .map(|(col, (_, n))| (col, row, n.clone()))
                    .collect();
                if let Some(keys) = &mut keys {
                    let t = db.table_in(target_ns, table)?;
                    let id = crate::keys::RowId::new(ci, row);
                    keys.release(&old);
                    if let Err(e) = keys.check(&new, t, Some(id)) {
                        // Restore the map so the error leaves no trace.
                        keys.reserve(&old, id);
                        return Err(e);
                    }
                    keys.reserve(&new, id);
                }
                if !writes.is_empty() {
                    db.table_mut_in(target_ns, table)?
                        .update_rows(ci, &writes)?;
                }
                if let Some(c) = &mut capture {
                    c.push((old.clone(), new.clone()));
                }
                if !defs.is_empty() {
                    fire_row_triggers(
                        db,
                        &defs,
                        TriggerTiming::After,
                        Some(&old),
                        Some(&new),
                        budget,
                    )?;
                }
            }
        }
        Ok(())
    })();
    db.trigger_capture = capture;
    if out.is_ok() {
        crate::keys::store(db, target_ns, table, keys);
    }
    out
}

/// Apply per-chunk drop lists, firing the table's `DELETE` row triggers per
/// row. Rows are removed highest-index first so the remaining indices stay
/// valid, as the plain path does per chunk.
pub(crate) fn delete_rows(
    db: &mut PinkDb,
    target_ns: &crate::storage::SchemaPath,
    table: &str,
    drops: Vec<Vec<usize>>,
    budget: &std::sync::Arc<crate::exec::ExecBudget>,
) -> Result<usize> {
    let defs = row_defs(db, target_ns, table, TriggerEvent::Delete, None);
    if defs.is_empty() && db.trigger_capture.is_none() {
        let mut count = 0;
        let t = db.table_mut_in(target_ns, table)?;
        for ci in (0..drops.len()).rev() {
            count += t.delete_from_chunk(ci, &drops[ci])?;
        }
        // Deletes shift row indices, so the cached key map cannot be patched.
        crate::keys::invalidate(db, target_ns, table);
        return Ok(count);
    }
    let mut count = 0;
    let mut capture = db.trigger_capture.take();
    let out = (|| -> Result<usize> {
        for ci in (0..drops.len()).rev() {
            budget.check_deadline()?;
            let mut rows = drops[ci].clone();
            rows.sort_unstable();
            rows.reverse();
            for row in rows {
                budget.check_deadline()?;
                let old: Vec<Value> = {
                    let chunk = db
                        .table_in(target_ns, table)?
                        .scan_chunk(ci)
                        .ok_or_else(|| {
                            PinkDbError::Execution("storage changed during DELETE".to_string())
                        })?;
                    (0..chunk.columns.len())
                        .map(|c| chunk.columns[c].get(row))
                        .collect()
                };
                if !defs.is_empty() {
                    fire_row_triggers(db, &defs, TriggerTiming::Before, Some(&old), None, budget)?;
                }
                count += db
                    .table_mut_in(target_ns, table)?
                    .delete_from_chunk(ci, &[row])?;
                if let Some(c) = &mut capture {
                    c.push((old.clone(), Vec::new()));
                }
                if !defs.is_empty() {
                    fire_row_triggers(db, &defs, TriggerTiming::After, Some(&old), None, budget)?;
                }
            }
        }
        Ok(count)
    })();
    db.trigger_capture = capture;
    if out.is_ok() {
        crate::keys::invalidate(db, target_ns, table);
    }
    out
}

/// The row-image scope of a trigger: `NEW.col` / `OLD.col` as typed values.
fn row_scope(
    db: &PinkDb,
    def: &TriggerDef,
    old: Option<&[Value]>,
    new: Option<&[Value]>,
) -> Result<TriggerScope> {
    let schema = db.table_in(def_ns(def), &def.table)?.schema();
    let mut scope = TriggerScope::default();
    for (alias, image) in [("old", old), ("new", new)] {
        let Some(image) = image else { continue };
        let cols = schema
            .iter()
            .zip(image.iter())
            .map(|((name, dtype), value)| (name.to_ascii_lowercase(), *dtype, value.clone()))
            .collect();
        scope.rows.push((alias.to_string(), cols));
    }
    Ok(scope)
}

/// Run one trigger body: bind and execute its action (and its `WHEN` clause)
/// with the row/transition scope installed, and hand the scope back so a
/// `BEFORE ROW` trigger's `NEW` assignments can be read off it.
fn run_trigger(
    db: &mut PinkDb,
    def: &TriggerDef,
    scope: TriggerScope,
    budget: &std::sync::Arc<crate::exec::ExecBudget>,
) -> Result<TriggerScope> {
    let previous_scope = db.trigger_scope.replace(scope);
    // A body's own DML must not append to the firing statement's transition
    // tables (its rows are the outer statement's, not this body's).
    let previous_capture = db.trigger_capture.take();
    // Invoker rights: the body's statements are checked against the user who
    // fired the statement, never a definer. Nested calls inherit it.
    let previous_invoker = db
        .trigger_invoker
        .replace(db.session_principal.clone().unwrap_or_default());
    let result = (|| -> Result<()> {
        // A plain function, not a closure: binding borrows `db` immutably and
        // the body then records its dynamic functions on the store.
        fn slots(db: &PinkDb) -> crate::sql::binder::ParamSlots {
            crate::sql::binder::ParamSlots::new(Vec::new()).with_variables(trigger_variables(db))
        }
        if let Some(when) = &def.when {
            let (bound, dynamic) = crate::sql::binder::bind_constant_expr_tracked(
                db,
                &def_ns(def),
                when,
                Some(slots(db)),
            )
            .map_err(|e| err(def, format!("WHEN: {e}")))?;
            // A `now()` in the WHEN clause is part of the record too.
            db.proc_frame.add_memo_deps(&dynamic);
            if let Some(function) = db.first_volatile_dynamic_function(&dynamic) {
                return Err(err(
                    def,
                    format!(
                        "the WHEN clause calls extension function {function}(), which is volatile \
                         (nondeterministic); a trigger is replayed from the WAL, so it must \
                         depend only on the firing row and the catalog"
                    ),
                ));
            }
            let single = crate::vector::DataChunk {
                columns: Vec::new(),
                num_rows: 1,
                ordinal: 0,
            };
            let cx = crate::exec::Cx::with_budget(db, budget.clone());
            let v = crate::exec::expr::eval_expr(&bound, &single, &cx)?;
            if v.len() != 1 {
                return Err(err(def, "the WHEN clause did not evaluate to one value"));
            }
            match v.get(0) {
                Value::Boolean(true) => {}
                // NULL is not TRUE: the trigger does not fire (SQL's rule).
                Value::Null | Value::Boolean(false) => return Ok(()),
                other => {
                    return Err(err(
                        def,
                        format!(
                            "the WHEN clause must be BOOLEAN, got {}",
                            other
                                .data_type()
                                .map(|t| t.to_string())
                                .unwrap_or_else(|| "NULL".to_string())
                        ),
                    ));
                }
            }
        }
        crate::procedure::run_call_statement(
            db,
            &def_ns(def),
            slots(db),
            &def.action,
            Some(budget),
            |msg| err(def, msg.to_string()),
        )?;
        Ok(())
    })();
    let scope = db.trigger_scope.take().unwrap_or_default();
    db.trigger_scope = previous_scope;
    db.trigger_capture = previous_capture;
    db.trigger_invoker = previous_invoker;
    result.map(|()| scope)
}

/// The `OLD`/`NEW` variables a body's statements bind: `new.id`, `old.id`, …
pub(crate) fn trigger_variables(db: &PinkDb) -> Vec<(String, Value)> {
    let Some(scope) = &db.trigger_scope else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (alias, cols) in &scope.rows {
        for (name, _, value) in cols {
            out.push((format!("{alias}.{name}"), value.clone()));
        }
    }
    out
}

/// `SET NEW.<col> = <value>` in a trigger body: write the pending row image.
/// `Ok(false)` means `target` is not a row variable at all (the caller then
/// reports the usual "not in scope" error).
pub(crate) fn assign_row_column(
    db: &mut PinkDb,
    target: &str,
    value: Value,
) -> std::result::Result<bool, String> {
    let Some((alias, column)) = target.split_once('.') else {
        return Ok(false);
    };
    let Some(scope) = &mut db.trigger_scope else {
        return Ok(false);
    };
    let Some((_, cols)) = scope
        .rows
        .iter_mut()
        .find(|(a, _)| a.eq_ignore_ascii_case(alias))
    else {
        return Ok(false);
    };
    if alias.eq_ignore_ascii_case("old") {
        return Err(format!(
            "SET {target}: OLD is read-only in a trigger body; only NEW may be assigned"
        ));
    }
    let Some((_, dtype, slot)) = cols
        .iter_mut()
        .find(|(n, _, _)| n.eq_ignore_ascii_case(column))
    else {
        return Err(format!("SET {target}: the row has no column \"{column}\""));
    };
    *slot = DataTable::coerce(value, *dtype)
        .map_err(|e| format!("SET {target}: cannot store the value as {dtype}: {e}"))?;
    Ok(true)
}

/// The depth bound plus statement atomicity: the outermost firing statement
/// takes the store snapshot and restores it when anything fails, so a trigger
/// error rolls back the whole statement (an open transaction stays open, its
/// earlier statements untouched — the same rule a `CALL` has).
fn guarded<R>(db: &mut PinkDb, f: impl FnOnce(&mut PinkDb) -> Result<R>) -> Result<R> {
    if db.trigger_depth >= MAX_TRIGGER_DEPTH {
        return Err(PinkDbError::Execution(format!(
            "trigger recursion depth exceeded (limit {MAX_TRIGGER_DEPTH}): a trigger body may fire \
             triggers up to {MAX_TRIGGER_DEPTH} levels deep — check the triggers for a cycle"
        )));
    }
    let snapshot = if db.trigger_depth == 0 {
        Some(crate::procedure::boxed_snapshot(db))
    } else {
        None
    };
    db.trigger_depth += 1;
    let out = f(db);
    db.trigger_depth = db.trigger_depth.saturating_sub(1);
    if out.is_err()
        && let Some(snap) = snapshot
    {
        db.savepoint_restore(snap);
    }
    out
}

/// The schema a trigger definition lives in (wave 131b).
fn def_ns(def: &TriggerDef) -> crate::storage::SchemaPath {
    crate::storage::SchemaPath::new(&def.db, &def.schema)
}

/// A trigger's error, tagged with the definition it comes from.
fn err(def: &TriggerDef, msg: impl std::fmt::Display) -> PinkDbError {
    PinkDbError::Execution(format!(
        "trigger \"{}\" on \"{}.{}\": {msg}",
        def.name, def.db, def.table
    ))
}

/// A placeholder value of `dtype`, used to bind a body once at creation time
/// so a misspelled `NEW.<col>` fails at `CREATE TRIGGER` instead of on the
/// first write.
fn placeholder(dtype: DataType) -> Value {
    match dtype {
        DataType::Boolean => Value::Boolean(false),
        DataType::Integer => Value::Integer(0),
        DataType::Bigint => Value::Bigint(0),
        DataType::Double => Value::Double(0.0),
        DataType::Varchar => Value::Varchar(String::new()),
        DataType::Date => Value::Date(0),
        DataType::Timestamp => Value::Timestamp(0),
        DataType::Decimal { scale, .. } => Value::Decimal(0, scale),
        // Interval and anything newer: a NULL is enough to validate names.
        _ => Value::Null,
    }
}

/// Validate a definition against the catalog at creation time: the table must
/// exist and be writable (never a view or an attached lake), the transition
/// tables are `AFTER STATEMENT` only, and the `WHEN` clause and the action bind
/// with the row/transition names in scope.
pub(crate) fn validate(db: &PinkDb, def: &TriggerDef) -> Result<()> {
    if def.old_table.is_some() || def.new_table.is_some() {
        if def.level != TriggerLevel::Statement {
            return Err(err(
                def,
                "REFERENCING OLD/NEW TABLE is only available in a FOR EACH STATEMENT trigger",
            ));
        }
        if def.timing != TriggerTiming::After {
            return Err(err(
                def,
                "REFERENCING OLD/NEW TABLE is only available in an AFTER trigger (a BEFORE \
                 trigger runs before the statement changed anything)",
            ));
        }
    }
    if def.events.is_empty() {
        return Err(err(def, "a trigger needs at least one event"));
    }
    // A trigger definition names its table by database; the schema
    // qualifier arrives with the grammar (wave 131b), so it means `main`.
    if let Some(e) = db.attached_read_only(&def_ns(def), &def.table) {
        return Err(err(
            def,
            format!("triggers are not supported on attached lake tables: {e}"),
        ));
    }
    if db.view_in(def_ns(def), &def.table).is_ok() {
        return Err(err(
            def,
            "triggers are not supported on views; name a table",
        ));
    }
    let schema = db.table_in(def_ns(def), &def.table)?.schema();
    for column in &def.columns {
        if !schema.iter().any(|(n, _)| n.eq_ignore_ascii_case(column)) {
            return Err(err(
                def,
                format!(
                    "table \"{}.{}\" has no column \"{column}\"",
                    def.db, def.table
                ),
            ));
        }
    }
    let mut scope = TriggerScope::default();
    if def.level == TriggerLevel::Row {
        for alias in ["old", "new"] {
            scope.rows.push((
                alias.to_string(),
                schema
                    .iter()
                    .map(|(n, t)| (n.to_ascii_lowercase(), *t, placeholder(*t)))
                    .collect(),
            ));
        }
    } else {
        // A transition table is empty at creation time; its *shape* is what
        // the body binds against.
        let columns: Vec<(String, DataType)> = schema
            .iter()
            .map(|(n, t)| (n.to_ascii_lowercase(), *t))
            .collect();
        for alias in [def.old_table.as_ref(), def.new_table.as_ref()]
            .into_iter()
            .flatten()
        {
            scope
                .tables
                .push((alias.clone(), crate::exec::output_from_rows(&columns, &[])?));
        }
    }
    // Bind through the same path a body takes at firing time, so the check and
    // the real run agree on what resolves. A clone carries the probe scope;
    // `validate` never mutates the catalog.
    let mut probe = db.clone();
    probe.trigger_scope = Some(scope);
    let slots =
        crate::sql::binder::ParamSlots::new(Vec::new()).with_variables(trigger_variables(&probe));
    if let Some(when) = &def.when {
        crate::sql::binder::bind_constant_expr_tracked(
            &probe,
            &def_ns(def),
            when,
            Some(slots.clone()),
        )
        .map_err(|e| err(def, format!("WHEN: {e}")))?;
    }
    crate::sql::binder::bind_tracked_with(
        &probe,
        &def.action,
        &def_ns(def),
        Some(slots),
        &probe.session,
    )
    .map_err(|e| err(def, format!("EXECUTE: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_dml_only_matches_writes() {
        let plan = LogicalPlan::Delete {
            ns: Some(crate::storage::SchemaPath::new("db", "main")),
            table: "t".to_string(),
            predicate: None,
            using: None,
            returning: None,
        };
        assert_eq!(
            plan_dml(&plan, &crate::storage::SchemaPath::main("cur")),
            Some((
                crate::storage::SchemaPath::new("db", "main"),
                "t".to_string(),
                TriggerEvent::Delete
            ))
        );
        let plan = LogicalPlan::Update {
            ns: None,
            table: "T".to_string(),
            sets: Vec::new(),
            predicate: None,
            from: None,
            returning: None,
        };
        assert_eq!(
            plan_dml(&plan, &crate::storage::SchemaPath::main("cur")),
            Some((
                crate::storage::SchemaPath::main("cur"),
                "T".to_string(),
                TriggerEvent::Update
            ))
        );
        assert_eq!(
            plan_dml(
                &LogicalPlan::SingleRow,
                &crate::storage::SchemaPath::main("cur")
            ),
            None
        );
    }
}
