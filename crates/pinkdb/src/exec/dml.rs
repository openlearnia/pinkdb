//! Wave 95b's data-modifying forms: `INSERT … ON CONFLICT`,
//! `UPDATE … FROM`, `DELETE … USING`, `MERGE`, and `RETURNING`.
//!
//! Two invariants shape the module:
//!
//! * **A statement is atomic.** Rows are resolved against an in-memory
//!   snapshot of the target table before the first write, so a rejected row
//!   (a constraint, a `MERGE` target matched twice) leaves the table untouched
//!   and no trigger fires for a statement that then fails.
//! * **`RETURNING` reports final rows.** The images come from the same capture
//!   the `AFTER … REFERENCING` transition tables use
//!   ([`PinkDb::trigger_capture`]), so a `BEFORE` trigger's rewrite of `NEW` is
//!   what `RETURNING` sees.
//!
//! The joins here are nested loops: `UPDATE … FROM` and `DELETE … USING`
//! evaluate their predicate once per (target row, source row) pair and `MERGE`
//! once per pair for its `ON`. That is O(n·m); a hash join is the upgrade path
//! when these forms see large inputs. Plain DML never enters this module.

use std::collections::HashMap;
use std::sync::Arc;

use super::{Cx, ExecBudget, Output, QueryResult, eval, eval_expr};
use crate::keys::{KeySpace, RowId};
use crate::plan::{
    ConflictActionPlan, ConflictPlan, LogicalPlan, MergeActionPlan, MergeClausePlan, Returning,
};
use crate::sql::ast::MergeMatch;
use crate::sql::binder::BoundExpr;
use crate::storage::{DataTable, PinkDb};
use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{CHUNK_SIZE, DataChunk, Vector};

type RowImages = Vec<(Vec<Value>, Vec<Value>)>;

/// Run `write` with a row capture installed, returning its result and the
/// `(old, new)` image of every row it wrote.
///
/// An `AFTER STATEMENT … REFERENCING` trigger installs a capture too; the rows
/// are forwarded to it so the two features compose.
pub(crate) fn captured<T>(
    db: &mut PinkDb,
    write: impl FnOnce(&mut PinkDb) -> Result<T>,
) -> Result<(T, RowImages)> {
    let outer = db.trigger_capture.take();
    db.trigger_capture = Some(Vec::new());
    let result = write(db);
    let images = db.trigger_capture.take().unwrap_or_default();
    db.trigger_capture = match outer {
        Some(mut outer) => {
            outer.extend(images.iter().cloned());
            Some(outer)
        }
        None => None,
    };
    Ok((result?, images))
}

/// The `RETURNING` result: its expressions over the final row images.
pub(crate) fn returning_result(
    db: &PinkDb,
    r: &Returning,
    schema: &[(String, DataType)],
    rows: Vec<Vec<Value>>,
) -> Result<QueryResult> {
    let types: Vec<DataType> = r.exprs.iter().map(|e| e.dtype()).collect();
    if rows.is_empty() {
        return Ok(QueryResult::Selected {
            names: r.names.clone(),
            types,
            rows: Vec::new(),
        });
    }
    let width = rows[0].len();
    let mut columns = Vec::with_capacity(width);
    for i in 0..width {
        let mut col = Vector::with_capacity(schema[i].1, rows.len());
        for row in &rows {
            col.push_value(row[i].clone())?;
        }
        columns.push(col);
    }
    let chunk = DataChunk::new(columns)?;
    let cx = Cx::with_budget(db, Arc::new(ExecBudget::unbounded()));
    let mut out = Vec::with_capacity(rows.len());
    for i in 0..chunk.num_rows() {
        out.push(
            r.exprs
                .iter()
                .map(|e| eval_expr(e, &chunk, &cx).map(|v| v.get(i)))
                .collect::<Result<Vec<_>>>()?,
        );
    }
    Ok(QueryResult::Selected {
        names: r.names.clone(),
        types,
        rows: out,
    })
}

/// `INSERT … [ON CONFLICT …]`: resolve every row against the target's declared
/// keys, then apply. Returns the inserted count (rows the conflict action
/// updated are not counted, matching DuckDB's `INSERT` count).
pub(crate) fn insert(
    db: &mut PinkDb,
    target_db: &str,
    table: &str,
    staged: Vec<Vec<Value>>,
    conflict: Option<&ConflictPlan>,
    budget: &Arc<ExecBudget>,
) -> Result<usize> {
    let keyed = !db.table_in(target_db, table)?.constraints.keys.is_empty();
    if conflict.is_none() {
        // No conflict clause: nothing to resolve, the trigger funnel checks the
        // rows against the table's cached key map.
        let count = staged.len();
        crate::trigger::insert_rows(db, target_db, table, staged, budget)?;
        return Ok(count);
    }
    if !keyed {
        return Err(PinkDbError::Execution(format!(
            "ON CONFLICT needs a PRIMARY KEY or UNIQUE constraint on table \"{table}\""
        )));
    }
    // `space` is the table's occupancy, read-only here; rows this statement
    // stages live in `side`, so applying the ops cannot collide with itself.
    // `shortcut:` a staged update is not visible to a later source row of the
    // same statement (DuckDB resolves upserts in source order), which only
    // matters when an ON CONFLICT action rewrites a key column.
    let space = crate::keys::take_or_build(db, target_db, table)?
        .expect("checked that the table declares a key");
    let mut side = space.empty_like();
    // Images of the rows staged for insertion, so a second conflict against
    // one of them can run its action without touching the table.
    let mut pending_images: HashMap<RowId, Vec<Value>> = HashMap::new();
    let mut pending_at: HashMap<RowId, usize> = HashMap::new();
    let mut inserts: Vec<Vec<Value>> = Vec::new();
    let mut updates: Vec<(RowId, Vec<Value>)> = Vec::new();
    {
        let t = db.table_in(target_db, table)?;
        let target = conflict.and_then(|cp| cp.target.as_deref());
        for values in staged {
            budget.check_deadline()?;
            let hit = space
                .find(&values, target)
                .or_else(|| side.find(&values, target));
            let Some(id) = hit else {
                // A brand-new row must not break any declared key.
                space.check(&values, t, None)?;
                side.check(&values, t, None)?;
                let id = RowId::new(usize::MAX, inserts.len());
                side.reserve(&values, id);
                pending_at.insert(id, inserts.len());
                pending_images.insert(id, values.clone());
                inserts.push(values);
                continue;
            };
            let cp = conflict.expect("a hit needs a conflict clause");
            let old = match pending_images.get(&id) {
                Some(image) => image.clone(),
                None => {
                    let chunk = t.scan_chunk(id.chunk).ok_or_else(|| {
                        PinkDbError::Execution("storage changed during INSERT".to_string())
                    })?;
                    row_values(&chunk, id.row)
                }
            };
            let new = match &cp.action {
                ConflictActionPlan::Nothing => continue,
                ConflictActionPlan::Update { sets, predicate } => {
                    let joined = pair_chunk(&old, &values, &schema_types(t), &t.schema())?;
                    let cx = Cx::with_budget(db, budget.clone());
                    if let Some(p) = predicate
                        && !is_true(&eval_expr(p, &joined, &cx)?, 0)?
                    {
                        continue;
                    }
                    eval_sets(&cx, sets, &joined, 0)?
                }
            };
            if let Some(idx) = pending_at.get(&id).copied() {
                // A row this statement already staged: the action rewrites the
                // pending image in place.
                side.release(&old);
                side.check(&new, t, Some(id))?;
                side.reserve(&new, id);
                pending_images.insert(id, new.clone());
                inserts[idx] = new;
                continue;
            }
            // An existing row: validate now (the write funnels maintain the
            // map themselves), then write it in `apply`.
            space.check(&new, t, Some(id))?;
            updates.push((id, new));
        }
    }
    crate::keys::store(db, target_db, table, Some(space));
    let inserted = inserts.len();
    let mut ops: Vec<Op> = Vec::with_capacity(updates.len() + inserts.len());
    ops.extend(updates.into_iter().map(|(id, new)| Op::Update(id, new)));
    ops.extend((0..inserts.len()).map(Op::Insert));
    apply(db, target_db, table, ops, &inserts, budget)?;
    Ok(inserted)
}

pub(crate) fn update_from(
    db: &mut PinkDb,
    target_db: &str,
    table: &str,
    source: &LogicalPlan,
    sets: &[(usize, BoundExpr)],
    predicate: Option<&BoundExpr>,
    budget: &Arc<ExecBudget>,
) -> Result<usize> {
    let (src_chunks, src_types) = {
        let cx = Cx::with_budget(db, budget.clone());
        let out = eval(&cx, source)?;
        (out.chunks, out.types)
    };
    let t = db.table_in(target_db, table)?;
    let schema = t.schema();
    let mut per_chunk: Vec<Vec<(usize, usize, Value)>> = vec![Vec::new(); t.num_chunks()];
    let mut count = 0usize;
    {
        let cx = Cx::with_budget(db, budget.clone());
        for (ci, writes) in per_chunk.iter_mut().enumerate() {
            cx.check_deadline()?;
            let chunk = t.scan_chunk(ci).ok_or_else(|| {
                PinkDbError::Execution("storage changed during UPDATE".to_string())
            })?;
            for row in 0..chunk.num_rows() {
                let mut chosen: Option<DataChunk> = None;
                for sc in &src_chunks {
                    for srow in 0..sc.num_rows() {
                        budget.check_deadline()?;
                        let joined = join_chunk(sc, srow, &chunk, row, &src_types, &schema)?;
                        let keep = match predicate {
                            Some(p) => is_true(&eval_expr(p, &joined, &cx)?, 0)?,
                            None => true,
                        };
                        if keep {
                            chosen = Some(joined);
                        }
                    }
                }
                let Some(joined) = chosen else { continue };
                count += 1;
                // The last matching source row wins (DuckDB's answer); only
                // the surviving image's SET expressions are evaluated.
                let mut staged: Vec<(usize, usize, Value)> = Vec::with_capacity(sets.len());
                for (col, expr) in sets {
                    staged.push((*col, row, eval_expr(expr, &joined, &cx)?.get(0)));
                }
                writes.extend(staged);
            }
        }
    }
    crate::trigger::update_rows(db, target_db, table, per_chunk, budget)?;
    Ok(count)
}

/// `DELETE FROM t USING …`: a target row goes when the predicate holds for any
/// source row. Duplicate matches are irrelevant — the row is deleted once.
pub(crate) fn delete_using(
    db: &mut PinkDb,
    target_db: &str,
    table: &str,
    source: &LogicalPlan,
    predicate: Option<&BoundExpr>,
    budget: &Arc<ExecBudget>,
) -> Result<usize> {
    let (src_chunks, src_types) = {
        let cx = Cx::with_budget(db, budget.clone());
        let out = eval(&cx, source)?;
        (out.chunks, out.types)
    };
    let t = db.table_in(target_db, table)?;
    let schema = t.schema();
    let mut drops: Vec<Vec<usize>> = Vec::with_capacity(t.num_chunks());
    {
        let cx = Cx::with_budget(db, budget.clone());
        for ci in 0..t.num_chunks() {
            cx.check_deadline()?;
            let chunk = t.scan_chunk(ci).ok_or_else(|| {
                PinkDbError::Execution("storage changed during DELETE".to_string())
            })?;
            let mut dropped = Vec::new();
            'rows: for row in 0..chunk.num_rows() {
                for sc in &src_chunks {
                    for srow in 0..sc.num_rows() {
                        budget.check_deadline()?;
                        let joined = join_chunk(sc, srow, &chunk, row, &src_types, &schema)?;
                        let hit = match predicate {
                            Some(p) => is_true(&eval_expr(p, &joined, &cx)?, 0)?,
                            None => true,
                        };
                        if hit {
                            dropped.push(row);
                            continue 'rows;
                        }
                    }
                }
            }
            drops.push(dropped);
        }
    }
    crate::trigger::delete_rows(db, target_db, table, drops, budget)
}

/// `MERGE INTO … USING … ON … WHEN …`: the source is materialized, then every
/// target row is matched against it.
///
/// A target row may be affected by at most one source row; more is an error
/// (DuckDB's rule, which never silently picks one). Returns
/// `(inserted, updated, deleted)`.
pub(crate) fn merge(
    db: &mut PinkDb,
    target_db: &str,
    table: &str,
    source: &LogicalPlan,
    on: &BoundExpr,
    clauses: &[MergeClausePlan],
    budget: &Arc<ExecBudget>,
) -> Result<(usize, usize, usize)> {
    let (src_chunks, src_types) = {
        let cx = Cx::with_budget(db, budget.clone());
        let out = eval(&cx, source)?;
        (out.chunks, out.types)
    };
    let src_rows: Vec<(usize, usize)> = src_chunks
        .iter()
        .enumerate()
        .flat_map(|(c, ch)| (0..ch.num_rows()).map(move |r| (c, r)))
        .collect();
    let t = db.table_in(target_db, table)?;
    let schema = t.schema();
    let defaults: Vec<Value> = t
        .columns
        .iter()
        .map(|c| c.default.clone().unwrap_or(Value::Null))
        .collect();
    let mut images = snapshot(t);
    let mut keys = KeySpace::build(t);
    let mut updates: Vec<(RowId, Vec<Value>)> = Vec::new();
    let mut drops: Vec<Vec<usize>> = vec![Vec::new(); t.num_chunks()];
    let mut inserts: Vec<Vec<Value>> = Vec::new();
    let mut source_matched = vec![false; src_rows.len()];
    {
        let cx = Cx::with_budget(db, budget.clone());
        for ci in 0..t.num_chunks() {
            cx.check_deadline()?;
            let chunk = t.scan_chunk(ci).ok_or_else(|| {
                PinkDbError::Execution("storage changed during MERGE".to_string())
            })?;
            for row in 0..chunk.num_rows() {
                budget.check_deadline()?;
                // Every source row whose ON holds for this target row.
                let mut hits: Vec<usize> = Vec::new();
                for (si, &(sc, sr)) in src_rows.iter().enumerate() {
                    let joined = join_chunk(&src_chunks[sc], sr, &chunk, row, &src_types, &schema)?;
                    if is_true(&eval_expr(on, &joined, &cx)?, 0)? {
                        hits.push(si);
                        source_matched[si] = true;
                    }
                }
                if hits.is_empty() {
                    let joined = join_chunk_null_source(&chunk, row, &src_types, &schema)?;
                    let Some(clause) =
                        first_clause(clauses, MergeMatch::NotMatchedBySource, &cx, &joined)?
                    else {
                        continue;
                    };
                    match &clause.action {
                        MergeActionPlan::Delete => drops[ci].push(row),
                        MergeActionPlan::Update { sets } => {
                            let old = images[ci][row].clone();
                            let new = eval_sets(&cx, sets, &joined, src_types.len())?;
                            stage_update(
                                t,
                                &mut keys,
                                &mut images,
                                &mut updates,
                                ci,
                                row,
                                old,
                                new,
                            )?;
                        }
                        MergeActionPlan::Insert { .. } => {
                            return Err(PinkDbError::Execution(
                                "MERGE: WHEN NOT MATCHED BY SOURCE cannot INSERT".to_string(),
                            ));
                        }
                    }
                    continue;
                }
                // First clause (in written order) that any matching source row
                // satisfies; two source rows for the same target row is an
                // error, as in DuckDB.
                let mut applied = None;
                for clause in clauses.iter().filter(|c| c.matched == MergeMatch::Matched) {
                    for &si in &hits {
                        let (sc, sr) = src_rows[si];
                        let joined =
                            join_chunk(&src_chunks[sc], sr, &chunk, row, &src_types, &schema)?;
                        if clause_holds(clause, &cx, &joined)? {
                            if applied.is_some() {
                                return Err(duplicate_match());
                            }
                            applied = Some((clause, joined));
                        }
                    }
                    if applied.is_some() {
                        break;
                    }
                }
                let Some((clause, joined)) = applied else {
                    continue;
                };
                match &clause.action {
                    MergeActionPlan::Delete => drops[ci].push(row),
                    MergeActionPlan::Update { sets } => {
                        let old = images[ci][row].clone();
                        let new = eval_sets(&cx, sets, &joined, src_types.len())?;
                        stage_update(t, &mut keys, &mut images, &mut updates, ci, row, old, new)?;
                    }
                    MergeActionPlan::Insert { .. } => {
                        return Err(PinkDbError::Execution(
                            "MERGE: WHEN MATCHED cannot INSERT".to_string(),
                        ));
                    }
                }
            }
        }
        // Source rows no target row matched: WHEN NOT MATCHED inserts them.
        let null_target = vec![Value::Null; schema.len()];
        for (si, &(sc, sr)) in src_rows.iter().enumerate() {
            if source_matched[si] {
                continue;
            }
            let src_row = row_values(&src_chunks[sc], sr);
            let joined = pair_chunk(&src_row, &null_target, &src_types, &schema)?;
            let Some(clause) = first_clause(clauses, MergeMatch::NotMatched, &cx, &joined)? else {
                continue;
            };
            let MergeActionPlan::Insert { columns, values } = &clause.action else {
                return Err(PinkDbError::Execution(
                    "MERGE: WHEN NOT MATCHED must INSERT".to_string(),
                ));
            };
            let mut image = defaults.clone();
            for (col, value) in columns.iter().zip(values.iter()) {
                image[*col] =
                    DataTable::coerce(eval_expr(value, &joined, &cx)?.get(0), schema[*col].1)?;
            }
            keys.check(&image, t, None)?;
            let id = append_id(&images);
            keys.reserve(&image, id);
            push_image(&mut images, image.clone());
            inserts.push(image);
        }
    }
    // Apply updates in place, then deletes (highest row first within each
    // chunk, so the staged ids stay valid), then appends.
    let updated = updates.len();
    let inserted = inserts.len();
    let mut ops: Vec<Op> = updates
        .into_iter()
        .map(|(id, new)| Op::Update(id, new))
        .collect();
    ops.extend((0..inserts.len()).map(Op::Insert));
    apply(db, target_db, table, ops, &inserts, budget)?;
    let deleted = if drops.iter().all(|d| d.is_empty()) {
        0
    } else {
        crate::trigger::delete_rows(db, target_db, table, drops, budget)?
    };
    Ok((inserted, updated, deleted))
}

fn duplicate_match() -> PinkDbError {
    PinkDbError::Execution(
        "MERGE INTO command cannot affect the same target row more than once. A target row \
         matched more than one source row; ensure the source rows are deduplicated or the ON \
         condition is unique."
            .to_string(),
    )
}

/// One staged write, applied in statement order.
enum Op {
    Insert(usize),
    Update(RowId, Vec<Value>),
}

fn apply(
    db: &mut PinkDb,
    target_db: &str,
    table: &str,
    ops: Vec<Op>,
    inserts: &[Vec<Value>],
    budget: &Arc<ExecBudget>,
) -> Result<()> {
    for op in ops {
        match op {
            Op::Insert(i) => {
                crate::trigger::insert_rows(
                    db,
                    target_db,
                    table,
                    vec![inserts[i].clone()],
                    budget,
                )?;
            }
            Op::Update(id, new) => {
                let chunk = db
                    .table_in(target_db, table)?
                    .scan_chunk(id.chunk)
                    .ok_or_else(|| {
                        PinkDbError::Execution("storage changed during UPDATE".to_string())
                    })?;
                let old: Vec<Value> = chunk.columns.iter().map(|c| c.get(id.row)).collect();
                let mut writes: Vec<(usize, usize, Value)> = old
                    .iter()
                    .zip(new.iter())
                    .enumerate()
                    .filter(|(_, (o, n))| o != n)
                    .map(|(col, (_, n))| (col, id.row, n.clone()))
                    .collect();
                if writes.is_empty() {
                    // An `ON CONFLICT` action that stores the values already
                    // there still counts as affecting the row (DuckDB returns
                    // it from RETURNING and fires its triggers), so stage the
                    // first column as a same-value write.
                    writes.push((0, id.row, new[0].clone()));
                }
                let mut per_chunk = vec![Vec::new(); db.table_in(target_db, table)?.num_chunks()];
                per_chunk[id.chunk] = writes;
                crate::trigger::update_rows(db, target_db, table, per_chunk, budget)?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn stage_update(
    t: &DataTable,
    keys: &mut KeySpace,
    images: &mut [Vec<Vec<Value>>],
    updates: &mut Vec<(RowId, Vec<Value>)>,
    chunk: usize,
    row: usize,
    old: Vec<Value>,
    new: Vec<Value>,
) -> Result<()> {
    let id = RowId::new(chunk, row);
    keys.release(&old);
    keys.check(&new, t, Some(id))?;
    keys.reserve(&new, id);
    images[chunk][row] = new.clone();
    updates.push((id, new));
    Ok(())
}

/// The first clause of `kind` whose `AND` condition holds for `joined`.
fn first_clause<'a>(
    clauses: &'a [MergeClausePlan],
    kind: MergeMatch,
    cx: &Cx,
    joined: &DataChunk,
) -> Result<Option<&'a MergeClausePlan>> {
    for clause in clauses.iter().filter(|c| c.matched == kind) {
        if clause_holds(clause, cx, joined)? {
            return Ok(Some(clause));
        }
    }
    Ok(None)
}

fn clause_holds(clause: &MergeClausePlan, cx: &Cx, joined: &DataChunk) -> Result<bool> {
    match &clause.condition {
        Some(c) => is_true(&eval_expr(c, joined, cx)?, 0),
        None => Ok(true),
    }
}

/// Evaluate `SET` expressions over `joined` and return the new target image,
/// which starts at column `base` (`0` for ON CONFLICT's
/// `[existing, excluded]`, the source width for MERGE's `[source, target]`).
fn eval_sets(
    cx: &Cx,
    sets: &[(usize, BoundExpr)],
    joined: &DataChunk,
    base: usize,
) -> Result<Vec<Value>> {
    let width = joined.num_cols() - base;
    let mut row: Vec<Value> = (0..width)
        .map(|i| joined.columns[base + i].get(0))
        .collect();
    for (col, expr) in sets {
        row[*col] = eval_expr(expr, joined, cx)?.get(0);
    }
    Ok(row)
}

fn schema_types(t: &DataTable) -> Vec<DataType> {
    t.schema().into_iter().map(|(_, ty)| ty).collect()
}

fn snapshot(t: &DataTable) -> Vec<Vec<Vec<Value>>> {
    let mut out = Vec::with_capacity(t.num_chunks());
    for ci in 0..t.num_chunks() {
        let Some(chunk) = t.scan_chunk(ci) else {
            continue;
        };
        out.push(
            (0..chunk.num_rows())
                .map(|r| chunk.columns.iter().map(|c| c.get(r)).collect())
                .collect(),
        );
    }
    out
}

fn append_id(images: &[Vec<Vec<Value>>]) -> RowId {
    let Some(last) = images.last() else {
        return RowId::new(0, 0);
    };
    if last.len() >= CHUNK_SIZE {
        RowId::new(images.len(), 0)
    } else {
        RowId::new(images.len() - 1, last.len())
    }
}

fn push_image(images: &mut Vec<Vec<Vec<Value>>>, values: Vec<Value>) {
    match images.last_mut() {
        Some(last) if last.len() < CHUNK_SIZE => last.push(values),
        _ => images.push(vec![values]),
    }
}

fn row_values(chunk: &DataChunk, row: usize) -> Vec<Value> {
    chunk.columns.iter().map(|c| c.get(row)).collect()
}

fn is_true(v: &Vector, row: usize) -> Result<bool> {
    if v.dtype() != DataType::Boolean {
        return Err(PinkDbError::Type(
            "DML condition needs a BOOLEAN expression".to_string(),
        ));
    }
    Ok(v.is_valid(row) && matches!(v.get(row), Value::Boolean(true)))
}

/// A one-row chunk of `[source…, target…]`.
fn join_chunk(
    src: &DataChunk,
    src_row: usize,
    target: &DataChunk,
    target_row: usize,
    src_types: &[DataType],
    target_types: &[(String, DataType)],
) -> Result<DataChunk> {
    let mut cols = Vec::with_capacity(src.columns.len() + target.columns.len());
    for (v, t) in src.columns.iter().zip(src_types.iter()) {
        cols.push(single(*t, v.get(src_row))?);
    }
    for (v, (_, t)) in target.columns.iter().zip(target_types.iter()) {
        cols.push(single(*t, v.get(target_row))?);
    }
    DataChunk::new(cols)
}

fn join_chunk_null_source(
    target: &DataChunk,
    target_row: usize,
    src_types: &[DataType],
    target_types: &[(String, DataType)],
) -> Result<DataChunk> {
    let mut cols = Vec::with_capacity(src_types.len() + target_types.len());
    for t in src_types {
        let mut col = Vector::with_capacity(*t, 1);
        col.push_null()?;
        cols.push(col);
    }
    for (v, (_, t)) in target.columns.iter().zip(target_types.iter()) {
        cols.push(single(*t, v.get(target_row))?);
    }
    DataChunk::new(cols)
}

fn pair_chunk(
    left: &[Value],
    right: &[Value],
    left_types: &[DataType],
    right_types: &[(String, DataType)],
) -> Result<DataChunk> {
    let mut cols = Vec::with_capacity(left.len() + right.len());
    for (v, t) in left.iter().zip(left_types.iter()) {
        cols.push(single(*t, v.clone())?);
    }
    for (v, (_, t)) in right.iter().zip(right_types.iter()) {
        cols.push(single(*t, v.clone())?);
    }
    DataChunk::new(cols)
}

fn single(t: DataType, v: Value) -> Result<Vector> {
    let mut col = Vector::with_capacity(t, 1);
    col.push_value(v)?;
    Ok(col)
}

/// Run the statement's data-modifying CTEs before its body.
///
/// `eval` has no mutable store handle, so the writes cannot happen where the
/// `Cte` node is evaluated: the statement entry point runs them here, once per
/// CTE, and leaves the result sets for the `Cte` nodes to hand to their bodies.
/// Binding already refuses a data-modifying CTE anywhere but the top level, so
/// the nodes form one chain.
pub(crate) fn dml_cte_outputs(
    db: &mut PinkDb,
    plan: &LogicalPlan,
    budget: &std::sync::Arc<ExecBudget>,
) -> Result<Option<std::sync::Arc<HashMap<String, Output>>>> {
    let mut map: HashMap<String, Output> = HashMap::new();
    let mut node = plan;
    while let LogicalPlan::Cte {
        name,
        query,
        body,
        recursive,
    } = node
    {
        if recursive.is_none() && crate::plan::writes_tables(query) {
            let out = match super::run_plain(db, query, budget.clone())? {
                QueryResult::Selected { names, types, rows } => rows_output(names, types, rows)?,
                // No `RETURNING`: the CTE has zero columns and no rows.
                _ => Output {
                    chunks: Vec::new(),
                    names: Vec::new(),
                    types: Vec::new(),
                },
            };
            map.insert(name.clone(), out);
        }
        node = body;
    }
    Ok(if map.is_empty() {
        None
    } else {
        Some(std::sync::Arc::new(map))
    })
}

fn rows_output(names: Vec<String>, types: Vec<DataType>, rows: Vec<Vec<Value>>) -> Result<Output> {
    let mut columns: Vec<Vector> = types
        .iter()
        .map(|t| Vector::with_capacity(*t, rows.len()))
        .collect();
    for row in rows {
        for (i, value) in row.into_iter().enumerate() {
            columns[i].push_value(value)?;
        }
    }
    Ok(Output {
        chunks: vec![DataChunk::new(columns)?],
        names,
        types,
    })
}
