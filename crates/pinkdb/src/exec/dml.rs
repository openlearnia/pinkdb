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
//! The joins here are hash probes where the predicate equates a source
//! expression with a target one ([`DmlIndex`]), so a target chunk's keys are
//! evaluated once instead of once per row. `MERGE` gathers its candidate pairs
//! into `[source…, target…]` batches and evaluates `ON`, the clause conditions
//! and its `SET`s a vector at a time. A predicate with no such equality falls
//! back to pairs. Plain DML never enters this module.

use std::collections::HashMap;
use std::sync::Arc;

use super::selection::all_rows;
use super::{Cx, DmlIndex, ExecBudget, Output, QueryResult, eval, eval_expr};
use crate::attached::PruneOp;
use crate::keys::RowId;
use crate::plan::{
    ConflictActionPlan, ConflictPlan, LogicalPlan, MergeActionPlan, MergeClausePlan, Returning,
};
use crate::sql::ast::MergeMatch;
use crate::sql::binder::BoundExpr;
use crate::storage::{DataTable, PinkDb};
use crate::types::{DataType, PinkDbError, Result, Value};
use crate::vector::{CHUNK_SIZE, DataChunk, GATHER_NULL, Vector};

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
    let cx = Cx::with_budget(db, Arc::new(ExecBudget::unbounded()));
    let mut out = Vec::with_capacity(rows.len());
    // Expressions run over a chunk at a time; one chunk per `CHUNK_SIZE` rows
    // because a statement can return more rows than a chunk holds.
    for window in rows.chunks(CHUNK_SIZE) {
        let mut columns = Vec::with_capacity(width);
        for i in 0..width {
            let mut col = Vector::with_capacity(schema[i].1, window.len());
            for row in window {
                col.push_value(row[i].clone())?;
            }
            columns.push(col);
        }
        let chunk = DataChunk::new(columns)?;
        let values = r
            .exprs
            .iter()
            .map(|e| eval_expr(e, &chunk, &cx))
            .collect::<Result<Vec<_>>>()?;
        for i in 0..chunk.num_rows() {
            out.push(values.iter().map(|v| v.get(i)).collect());
        }
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
        let index = match predicate {
            Some(p) => DmlIndex::build(&cx, &src_chunks, src_types.len(), p)?,
            None => None,
        };
        let mut probe_keys = None;
        for (ci, writes) in per_chunk.iter_mut().enumerate() {
            cx.check_deadline()?;
            let chunk = t.scan_chunk(ci).ok_or_else(|| {
                PinkDbError::Execution("storage changed during UPDATE".to_string())
            })?;
            if let Some(index) = &index {
                probe_keys = Some(index.probe_keys(&cx, &chunk)?);
            }
            for row in 0..chunk.num_rows() {
                let mut chosen: Option<DataChunk> = None;
                // One candidate pair: the predicate decides whether it wins.
                let visit =
                    |sc: usize, srow: usize, chosen: &mut Option<DataChunk>| -> Result<()> {
                        budget.check_deadline()?;
                        let joined =
                            join_chunk(&src_chunks[sc], srow, &chunk, row, &src_types, &schema)?;
                        let keep = match predicate {
                            Some(p) => is_true(&eval_expr(p, &joined, &cx)?, 0)?,
                            None => true,
                        };
                        if keep {
                            *chosen = Some(joined);
                        }
                        Ok(())
                    };
                match (&index, &probe_keys) {
                    (Some(index), Some(keys)) => {
                        for (sc, srow) in index.matches(keys, row) {
                            visit(sc as usize, srow as usize, &mut chosen)?;
                        }
                    }
                    _ => {
                        for (sc, source_chunk) in src_chunks.iter().enumerate() {
                            for srow in 0..source_chunk.num_rows() {
                                visit(sc, srow, &mut chosen)?;
                            }
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
        let index = match predicate {
            Some(p) => DmlIndex::build(&cx, &src_chunks, src_types.len(), p)?,
            None => None,
        };
        let mut probe_keys = None;
        for ci in 0..t.num_chunks() {
            cx.check_deadline()?;
            let chunk = t.scan_chunk(ci).ok_or_else(|| {
                PinkDbError::Execution("storage changed during DELETE".to_string())
            })?;
            if let Some(index) = &index {
                probe_keys = Some(index.probe_keys(&cx, &chunk)?);
            }
            let mut dropped = Vec::new();
            for row in 0..chunk.num_rows() {
                let hit = |sc: usize, srow: usize| -> Result<bool> {
                    budget.check_deadline()?;
                    let joined =
                        join_chunk(&src_chunks[sc], srow, &chunk, row, &src_types, &schema)?;
                    Ok(match predicate {
                        Some(p) => is_true(&eval_expr(p, &joined, &cx)?, 0)?,
                        None => true,
                    })
                };
                let matched = match (&index, &probe_keys) {
                    (Some(index), Some(keys)) => {
                        let mut found = false;
                        for (sc, srow) in index.matches(keys, row) {
                            if hit(sc as usize, srow as usize)? {
                                found = true;
                                break;
                            }
                        }
                        found
                    }
                    _ => {
                        let mut found = false;
                        'candidates: for (sc, source_chunk) in src_chunks.iter().enumerate() {
                            for srow in 0..source_chunk.num_rows() {
                                if hit(sc, srow)? {
                                    found = true;
                                    break 'candidates;
                                }
                            }
                        }
                        found
                    }
                };
                if matched {
                    dropped.push(row);
                }
            }
            drops.push(dropped);
        }
    }
    crate::trigger::delete_rows(db, target_db, table, drops, budget)
}

/// `MERGE INTO … USING … ON … WHEN …`: the source is materialized, then every
/// target row is matched against it by a hash probe over the `ON`'s equality
/// conjuncts ([`DmlIndex`]).
///
/// The candidates of one target chunk are gathered into `[source…, target…]`
/// batches of at most [`CHUNK_SIZE`] rows, so the `ON`, the clause conditions
/// and the `SET`/`VALUES` expressions each evaluate a vector at a time, and the
/// writes of one statement go out as one call per kind. DuckDB's `MERGE` has
/// the same shape — a join of source and target whose joined chunks are routed
/// to per-action sinks (`duckdb/src/planner/binder/statement/bind_merge_into.cpp`,
/// `duckdb/src/execution/operator/persistent/physical_merge_into.cpp`).
///
/// A target row may be affected by at most one source row; more is an error
/// (DuckDB's rule, which never silently picks one). Every row is resolved
/// before the first write, so an error leaves the table untouched. Returns
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
    // Global index of each source chunk's first row: a probe yields
    // chunk-local coordinates, `source_matched` is indexed globally.
    let mut src_starts = Vec::with_capacity(src_chunks.len());
    let mut acc = 0usize;
    for source_chunk in &src_chunks {
        src_starts.push(acc);
        acc += source_chunk.num_rows();
    }
    // The source column by column, chunk by chunk: the batched gathers.
    let src_cols: Vec<Vec<&Vector>> = (0..src_types.len())
        .map(|i| src_chunks.iter().map(|c| c.column(i)).collect())
        .collect();
    let mut source_matched = vec![false; src_rows.len()];
    // Constraints are checked against a copy of the table's cached key map: the
    // write funnels take and maintain the canonical one themselves, so it goes
    // back pristine. A keyless table stages nothing.
    let mut keys = match crate::keys::take_or_build(db, target_db, table)? {
        Some(space) => {
            let staged = space.clone();
            crate::keys::store(db, target_db, table, Some(space));
            Some(staged)
        }
        None => None,
    };
    let matched: Vec<usize> = clauses
        .iter()
        .enumerate()
        .filter(|(_, c)| c.matched == MergeMatch::Matched)
        .map(|(i, _)| i)
        .collect();
    // `WHEN NOT MATCHED BY SOURCE` sees every target row, even one no source
    // row can reach: no chunk may be skipped for it.
    let by_source = clauses
        .iter()
        .any(|c| c.matched == MergeMatch::NotMatchedBySource);
    let (per_chunk, drops, inserts, updated) = {
        let cx = Cx::with_budget(db, budget.clone());
        let t = db.table_in(target_db, table)?;
        let schema = t.schema();
        let defaults: Vec<Value> = t
            .columns
            .iter()
            .map(|c| c.default.clone().unwrap_or(Value::Null))
            .collect();
        let num_chunks = t.num_chunks();
        let mut per_chunk: Vec<Vec<(usize, usize, Value)>> = vec![Vec::new(); num_chunks];
        let mut drops: Vec<Vec<usize>> = vec![Vec::new(); num_chunks];
        let mut inserts: Vec<Vec<Value>> = Vec::new();
        // `append_row` fills the tail chunk before opening a new one, and a
        // delete can leave a chunk short, so the next row's id is
        // `(tail chunk, its length)`, not `num_rows() / CHUNK_SIZE`.
        let mut next_id = append_id(t);
        let mut updated = 0usize;
        let index = DmlIndex::build(&cx, &src_chunks, src_types.len(), on)?;
        // The source side's reach, per plain-column key: a target chunk whose
        // zone cannot overlap it has no match (a hash join skips its probe
        // chunks the same way, through `JoinFilter`).
        let zones = match &index {
            Some(index) if !by_source => {
                index.target_zones(&cx, &src_chunks, src_types.len(), &schema)?
            }
            _ => Vec::new(),
        };
        let ctx = MergeCtx {
            cx: &cx,
            on,
            clauses,
            matched: &matched,
            src_cols: &src_cols,
            src_types: &src_types,
            schema: &schema,
        };
        let mut hits: Vec<RowHit> = Vec::new();
        for ci in 0..num_chunks {
            cx.check_deadline()?;
            let chunk = t.scan_chunk(ci).ok_or_else(|| {
                PinkDbError::Execution("storage changed during MERGE".to_string())
            })?;
            let rows = chunk.num_rows();
            let reachable = zones.is_empty()
                || zones.iter().all(|(col, zone)| {
                    let own = t.zone(*col, ci);
                    own.may_match(rows as u64, PruneOp::GtEq, zone.min.as_ref())
                        && own.may_match(rows as u64, PruneOp::LtEq, zone.max.as_ref())
                });
            if !reachable {
                continue;
            }
            hits.clear();
            hits.extend((0..rows).map(|_| RowHit::default()));
            let probe_keys = match &index {
                Some(index) => Some(index.probe_keys(&cx, &chunk)?),
                None => None,
            };
            let mut batch: Vec<(u32, u32, u32)> = Vec::with_capacity(CHUNK_SIZE);
            // A batch is one joined chunk, so it is full at `CHUNK_SIZE`: a
            // target row with more candidates than that splits across batches,
            // its `hits` carrying the earlier batches' matches.
            let flush = |batch: &mut Vec<(u32, u32, u32)>,
                         hits: &mut [RowHit],
                         source_matched: &mut [bool]|
             -> Result<()> {
                budget.check_deadline()?;
                ctx.match_batch(&chunk, batch, hits, source_matched, &src_starts)?;
                batch.clear();
                Ok(())
            };
            for row in 0..rows {
                budget.check_deadline()?;
                match (&index, &probe_keys) {
                    (Some(index), Some(probe_keys)) => {
                        for (sc, sr) in index.matches(probe_keys, row) {
                            batch.push((row as u32, sc, sr));
                            if batch.len() == CHUNK_SIZE {
                                flush(&mut batch, &mut hits, &mut source_matched)?;
                            }
                        }
                    }
                    // No equality between the source and the target: every pair
                    // is a candidate.
                    _ => {
                        for &(sc, sr) in &src_rows {
                            batch.push((row as u32, sc as u32, sr as u32));
                            if batch.len() == CHUNK_SIZE {
                                flush(&mut batch, &mut hits, &mut source_matched)?;
                            }
                        }
                    }
                }
            }
            if !batch.is_empty() {
                flush(&mut batch, &mut hits, &mut source_matched)?;
            }
            // Every row of the chunk resolved before any of it is staged.
            for action in ctx.resolve_chunk(&chunk, &hits)? {
                let row = action.row;
                if matches!(clauses[action.clause].action, MergeActionPlan::Delete) {
                    drops[ci].push(row);
                    continue;
                }
                // An update: its new key must not collide with another row, and
                // only the columns whose value changed are written (a same-value
                // first column when none did, so the row still counts and fires
                // triggers, as `apply` stages it).
                let old = row_values(&chunk, row);
                let new = action.image.expect("a resolved update carries its image");
                if let Some(keys) = &mut keys {
                    let id = RowId::new(ci, row);
                    keys.release(&old);
                    keys.check(&new, t, Some(id))?;
                    keys.reserve(&new, id);
                }
                let mut writes: Vec<(usize, usize, Value)> = old
                    .iter()
                    .zip(new.iter())
                    .enumerate()
                    .filter(|(_, (o, n))| o != n)
                    .map(|(col, (_, n))| (col, row, n.clone()))
                    .collect();
                if writes.is_empty() {
                    writes.push((0, row, new[0].clone()));
                }
                per_chunk[ci].extend(writes);
                updated += 1;
            }
        }
        // Source rows no target row matched: `WHEN NOT MATCHED` inserts them,
        // their values evaluated a batch at a time against NULL target columns.
        let mut open: Vec<u32> = (0..src_rows.len() as u32)
            .filter(|i| !source_matched[*i as usize])
            .collect();
        for clause in clauses
            .iter()
            .filter(|c| c.matched == MergeMatch::NotMatched)
        {
            if open.is_empty() {
                break;
            }
            let mut kept: Vec<u32> = Vec::new();
            for batch in open.chunks(CHUNK_SIZE) {
                cx.check_deadline()?;
                let picks: Vec<(u32, u32)> = batch
                    .iter()
                    .map(|&i| {
                        let (sc, sr) = src_rows[i as usize];
                        (sc as u32, sr as u32)
                    })
                    .collect();
                let joined = ctx.joined(&picks, None)?;
                let mut sel = all_rows(batch.len());
                if let Some(condition) = &clause.condition {
                    select_rows(condition, &joined, &cx, &mut sel)?;
                }
                if !sel.is_empty() {
                    let MergeActionPlan::Insert { columns, values } = &clause.action else {
                        return Err(PinkDbError::Execution(
                            "MERGE: WHEN NOT MATCHED must INSERT".to_string(),
                        ));
                    };
                    // One evaluation per value expression for the batch.
                    let taken = joined.take(&sel);
                    let mut evaluated: Vec<Vector> = Vec::with_capacity(values.len());
                    for value in values {
                        evaluated.push(eval_expr(value, &taken, &cx)?);
                    }
                    for row in 0..taken.num_rows() {
                        let mut image = defaults.clone();
                        for (col, value) in columns.iter().zip(&evaluated) {
                            image[*col] = DataTable::coerce(value.get(row), schema[*col].1)?;
                        }
                        if let Some(keys) = &mut keys {
                            keys.check(&image, t, None)?;
                            keys.reserve(&image, next_id);
                            advance_id(&mut next_id);
                        }
                        inserts.push(image);
                    }
                }
                if sel.len() < batch.len() {
                    let mut taken = vec![false; batch.len()];
                    for &k in &sel {
                        taken[k as usize] = true;
                    }
                    kept.extend(
                        batch
                            .iter()
                            .zip(taken)
                            .filter(|(_, taken)| !taken)
                            .map(|(&i, _)| i),
                    );
                }
            }
            open = kept;
        }
        (per_chunk, drops, inserts, updated)
    };
    // Updates in place, then appends, then deletes (highest row first within
    // each chunk, so the staged ids stay valid).
    let inserted = inserts.len();
    if per_chunk.iter().any(|writes| !writes.is_empty()) {
        crate::trigger::update_rows(db, target_db, table, per_chunk, budget)?;
    }
    if !inserts.is_empty() {
        crate::trigger::insert_rows(db, target_db, table, inserts, budget)?;
    }
    let deleted = if drops.iter().all(|d| d.is_empty()) {
        0
    } else {
        crate::trigger::delete_rows(db, target_db, table, drops, budget)?
    };
    Ok((inserted, updated, deleted))
}

/// What every batch of one `MERGE` is evaluated against.
struct MergeCtx<'a, 'db> {
    cx: &'a Cx<'db>,
    on: &'a BoundExpr,
    clauses: &'a [MergeClausePlan],
    /// Indices of the `WHEN MATCHED` clauses, in written order.
    matched: &'a [usize],
    /// Source columns by column, then by source chunk, for the gathers.
    src_cols: &'a [Vec<&'a Vector>],
    src_types: &'a [DataType],
    schema: &'a [(String, DataType)],
}

impl MergeCtx<'_, '_> {
    /// `[source…, target…]` over `picks` (`(source chunk, source row)` pairs)
    /// and `target`'s rows (`(chunk, rows)`); `None` leaves the target side all
    /// NULL, as `WHEN NOT MATCHED` sees it. A [`GATHER_NULL`] pick is a NULL
    /// source row, as `WHEN NOT MATCHED BY SOURCE` sees it.
    fn joined(
        &self,
        picks: &[(u32, u32)],
        target: Option<(&DataChunk, &[u32])>,
    ) -> Result<DataChunk> {
        let rows = picks.len();
        let mut cols: Vec<Vector> = self
            .src_cols
            .iter()
            .zip(self.src_types)
            .map(|(sources, dtype)| Vector::gather_located(*dtype, sources, picks))
            .collect();
        match target {
            Some((chunk, target_rows)) => {
                for col in 0..self.schema.len() {
                    cols.push(chunk.column(col).take(target_rows));
                }
            }
            None => {
                for (_, dtype) in self.schema {
                    let mut col = Vector::with_capacity(*dtype, rows);
                    for _ in 0..rows {
                        col.push_null()?;
                    }
                    cols.push(col);
                }
            }
        }
        DataChunk::new(cols)
    }

    /// Match one batch of `(target row, source chunk, source row)` candidates:
    /// the `ON` over the whole batch, then the `WHEN MATCHED` conditions over
    /// the rows still without a clause. Each target row keeps the earliest
    /// clause (written order) one of its candidates satisfied.
    fn match_batch(
        &self,
        chunk: &DataChunk,
        batch: &[(u32, u32, u32)],
        hits: &mut [RowHit],
        source_matched: &mut [bool],
        src_starts: &[usize],
    ) -> Result<()> {
        let picks: Vec<(u32, u32)> = batch.iter().map(|&(_, sc, sr)| (sc, sr)).collect();
        let target_rows: Vec<u32> = batch.iter().map(|&(row, _, _)| row).collect();
        let joined = self.joined(&picks, Some((chunk, &target_rows)))?;
        let mut sel = all_rows(batch.len());
        select_rows(self.on, &joined, self.cx, &mut sel)?;
        for &k in &sel {
            let (row, sc, sr) = batch[k as usize];
            hits[row as usize].on = true;
            source_matched[src_starts[sc as usize] + sr as usize] = true;
        }
        // A row an earlier clause resolved does not look at a later one: its
        // condition stays unevaluated for that row, as in a per-row pass.
        let mut resolved = vec![false; batch.len()];
        for &clause in self.matched {
            let mut taken: Vec<u32> = sel
                .iter()
                .copied()
                .filter(|k| !resolved[*k as usize])
                .collect();
            if taken.is_empty() {
                break;
            }
            if let Some(condition) = &self.clauses[clause].condition {
                select_rows(condition, &joined, self.cx, &mut taken)?;
            }
            for &k in &taken {
                resolved[k as usize] = true;
                let (row, sc, sr) = batch[k as usize];
                let hit = &mut hits[row as usize];
                match hit.clause {
                    Some(earlier) if earlier < clause => {}
                    Some(earlier) if earlier == clause => hit.count += 1,
                    _ => {
                        hit.clause = Some(clause);
                        hit.count = 1;
                        hit.cand = (sc, sr);
                    }
                }
            }
        }
        Ok(())
    }

    /// Resolve one target chunk's rows into the actions to stage: the duplicate
    /// match, the earliest satisfied clause per row, and every `SET` image,
    /// evaluated a clause at a time over batched joined chunks.
    fn resolve_chunk(&self, chunk: &DataChunk, hits: &[RowHit]) -> Result<Vec<Action>> {
        let rows = chunk.num_rows();
        for hit in hits {
            if hit.count > 1 {
                return Err(duplicate_match());
            }
            if let Some(clause) = hit.clause
                && matches!(self.clauses[clause].action, MergeActionPlan::Insert { .. })
            {
                return Err(PinkDbError::Execution(
                    "MERGE: WHEN MATCHED cannot INSERT".to_string(),
                ));
            }
        }
        let mut images: Vec<Option<Vec<Value>>> = vec![None; rows];
        for &clause in self.matched {
            let MergeActionPlan::Update { sets } = &self.clauses[clause].action else {
                continue;
            };
            let rows_of: Vec<u32> = (0..rows)
                .filter(|row| hits[*row].clause == Some(clause))
                .map(|row| row as u32)
                .collect();
            for batch in rows_of.chunks(CHUNK_SIZE) {
                let picks: Vec<(u32, u32)> = batch.iter().map(|&r| hits[r as usize].cand).collect();
                let joined = self.joined(&picks, Some((chunk, batch)))?;
                let new = eval_sets_batch(self.cx, sets, &joined, self.src_types.len())?;
                for (&row, image) in batch.iter().zip(new) {
                    images[row as usize] = Some(image);
                }
            }
        }
        let mut actions: Vec<Action> = (0..rows)
            .filter_map(|row| {
                hits[row].clause.map(|clause| Action {
                    row,
                    clause,
                    image: images[row].take(),
                })
            })
            .collect();
        // `WHEN NOT MATCHED BY SOURCE`: the rows no `ON` matched, clause by
        // clause, over NULL source columns.
        let by_source: Vec<usize> = self
            .clauses
            .iter()
            .enumerate()
            .filter(|(_, c)| c.matched == MergeMatch::NotMatchedBySource)
            .map(|(i, _)| i)
            .collect();
        let mut open: Vec<u32> = (0..rows as u32)
            .filter(|row| !hits[*row as usize].on)
            .collect();
        for &clause in &by_source {
            if open.is_empty() {
                break;
            }
            let mut kept: Vec<u32> = Vec::new();
            for batch in open.chunks(CHUNK_SIZE) {
                let null_picks = vec![(GATHER_NULL, 0u32); batch.len()];
                let joined = self.joined(&null_picks, Some((chunk, batch)))?;
                let mut sel = all_rows(batch.len());
                if let Some(condition) = &self.clauses[clause].condition {
                    select_rows(condition, &joined, self.cx, &mut sel)?;
                }
                match &self.clauses[clause].action {
                    MergeActionPlan::Delete => {
                        for &k in &sel {
                            let row = batch[k as usize] as usize;
                            actions.push(Action {
                                row,
                                clause,
                                image: None,
                            });
                        }
                    }
                    MergeActionPlan::Update { sets } => {
                        if !sel.is_empty() {
                            let picked = joined.take(&sel);
                            let new =
                                eval_sets_batch(self.cx, sets, &picked, self.src_types.len())?;
                            for (&k, image) in sel.iter().zip(new) {
                                actions.push(Action {
                                    row: batch[k as usize] as usize,
                                    clause,
                                    image: Some(image),
                                });
                            }
                        }
                    }
                    MergeActionPlan::Insert { .. } => {
                        if !sel.is_empty() {
                            return Err(PinkDbError::Execution(
                                "MERGE: WHEN NOT MATCHED BY SOURCE cannot INSERT".to_string(),
                            ));
                        }
                    }
                }
                if sel.len() < batch.len() {
                    let mut taken = vec![false; batch.len()];
                    for &k in &sel {
                        taken[k as usize] = true;
                    }
                    kept.extend(
                        batch
                            .iter()
                            .zip(taken)
                            .filter(|(_, taken)| !taken)
                            .map(|(&row, _)| row),
                    );
                }
            }
            open = kept;
        }
        // Row order, so staging follows the same order a per-row pass did.
        actions.sort_by_key(|action| action.row);
        Ok(actions)
    }
}

/// One target row's resolved `MERGE` action.
struct Action {
    row: usize,
    clause: usize,
    /// The staged image of an update: the row with its `SET` values applied.
    image: Option<Vec<Value>>,
}

/// One target row's match state while its chunk's candidates are probed.
#[derive(Default)]
struct RowHit {
    /// Some candidate's `ON` held: the row is not a `WHEN NOT MATCHED BY
    /// SOURCE` candidate.
    on: bool,
    /// The earliest clause (written order) one of its candidates satisfied.
    clause: Option<usize>,
    /// How many candidates satisfied `clause`: two is the duplicate match.
    count: usize,
    /// `(source chunk, source row)` of the first such candidate.
    cand: (u32, u32),
}

/// Narrow `sel` to the rows of `chunk` where `expr` is TRUE, keeping the DML
/// wording for a condition that is not `BOOLEAN`.
fn select_rows(expr: &BoundExpr, chunk: &DataChunk, cx: &Cx, sel: &mut Vec<u32>) -> Result<()> {
    if expr.dtype() != DataType::Boolean {
        return Err(PinkDbError::Type(
            "DML condition needs a BOOLEAN expression".to_string(),
        ));
    }
    super::expr::select_predicate(expr, chunk, cx, sel)
}

/// Evaluate `SET` expressions over every row of `joined` and return the new
/// target images, which start at column `base` (the source width for `MERGE`).
fn eval_sets_batch(
    cx: &Cx,
    sets: &[(usize, BoundExpr)],
    joined: &DataChunk,
    base: usize,
) -> Result<Vec<Vec<Value>>> {
    let width = joined.num_cols() - base;
    let mut out: Vec<Vec<Value>> = (0..joined.num_rows())
        .map(|row| {
            (0..width)
                .map(|i| joined.columns[base + i].get(row))
                .collect()
        })
        .collect();
    for (col, expr) in sets {
        let values = eval_expr(expr, joined, cx)?;
        for (row, image) in out.iter_mut().enumerate() {
            image[*col] = values.get(row);
        }
    }
    Ok(out)
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

/// The id `append_row` gives the next row: the last chunk while it has room,
/// otherwise a new one.
fn append_id(t: &DataTable) -> RowId {
    let chunks = t.num_chunks();
    if chunks == 0 {
        return RowId::new(0, 0);
    }
    let tail = chunks - 1;
    let len = t.scan_chunk(tail).map(|c| c.num_rows()).unwrap_or(0);
    if len >= CHUNK_SIZE {
        RowId::new(tail + 1, 0)
    } else {
        RowId::new(tail, len)
    }
}

fn advance_id(id: &mut RowId) {
    id.row += 1;
    if id.row == CHUNK_SIZE {
        id.row = 0;
        id.chunk += 1;
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
    let mut chunks = Vec::with_capacity(rows.len().div_ceil(CHUNK_SIZE));
    for window in rows.chunks(CHUNK_SIZE) {
        let mut columns: Vec<Vector> = types
            .iter()
            .map(|t| Vector::with_capacity(*t, window.len()))
            .collect();
        for row in window {
            for (i, value) in row.iter().enumerate() {
                columns[i].push_value(value.clone())?;
            }
        }
        chunks.push(DataChunk::new(columns)?);
    }
    Ok(Output {
        chunks,
        names,
        types,
    })
}
