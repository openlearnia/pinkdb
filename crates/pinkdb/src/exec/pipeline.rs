//! The pipeline executor: one source of morsels, streaming operators, one sink.
//!
//! An executor that materializes every plan node pays a barrier at each
//! operator boundary: the pool waits while one operator runs, then starts the
//! next. Here a *morsel* — one chunk of the source — flows through the filter,
//! projection and hash-join probes above it into the sink, and no intermediate
//! result is ever collected. The sink keeps one partial per chunk it folded;
//! the partials merge once, in morsel order. That is what keeps the answer the
//! materialized path's answer: the same rows in the same order, hence the same
//! first-seen group order and the same floating-point accumulation order.
//!
//! The shape follows DuckDB's push-based pipeline (`src/parallel/pipeline.cpp`,
//! `src/parallel/pipeline_executor.cpp`, `src/parallel/meta_pipeline.cpp`, and
//! the source/operator/sink split of
//! `src/include/duckdb/execution/physical_operator.hpp`), in pinkdb's
//! chunk-at-a-time terms. Morsels go out to the pool, and everything between a
//! chunk and its partial is one worker's serial job; the order-preserving
//! collect is what pins row order.
//!
//! # What is covered
//!
//! [`aggregate`] compiles `Aggregate` over a chain of `Filter`, `Project` and
//! join nodes down to a source. Each join's build side is evaluated (by `eval`)
//! before the probe pipeline runs, exactly as an operator would have, and the
//! pipeline flows through the *probe* (preserved) side. A chain of joins
//! therefore never materializes a result between two probes. Inner, left and
//! right joins are probes; semi, anti and mark joins reuse the materialized
//! path's match table and NULL bookkeeping ([`MarkJoin`]) and stream their
//! preserved side.
//!
//! [`collect`] runs the same compilation for a chain the materialized path
//! would have evaluated node by node and hands its parent the chunks it would
//! have produced, in the same order, so a `Sort`, top-N, `Distinct`, `Limit`,
//! set operation, CTE materialization or the statement root reads a pipeline's
//! result without noticing it.
//!
//! Every shape this module does not compile — a full join, a semi/anti/mark
//! join the materialized path decides to build the preserved side for, an
//! unsupported input, an aggregate in an outer context — is declined with
//! `Ok(None)` and the caller runs the materialized path unchanged. `SET
//! pipeline_execution = false` forces that path, which is how the two are
//! compared.

use std::sync::Arc;

use rayon::prelude::*;

use super::aggregate::{self, Partial};
use super::group_table::{FlatPartial, FlatSink};
use super::{
    Cx, MARK_NAME, MarkJoin, Output, ProbePlan, build_on_left, chunk_approx_bytes,
    derive_join_filters, estimate_rows, eval, expr, filter_pays, mark_probe_chunk, project_chunk,
    project_mask, scan_targets, selection, swap_join_space,
};
use crate::plan::LogicalPlan;
use crate::sql::ast::JoinKind;
use crate::sql::binder::{BoundAgg, BoundExpr};
use crate::types::{DataType, PinkDbError, Result};
use crate::vector::DataChunk;

/// One chunk on its way down the pipeline, with the rows a filter kept of it
/// (`None` — every row). It is compacted only where a consumer needs a
/// compacted chunk, so a selection costs nothing until then.
struct Morsel {
    chunk: DataChunk,
    sel: Option<Vec<u32>>,
}

/// What a sink does with one chunk: fold it into a partial of its own, or
/// decline (`Ok(None)`) when the shape is beyond it.
pub(crate) trait Sink {
    type Partial;
    fn fold_into(
        &self,
        cx: &Cx,
        chunk: &DataChunk,
        sel: Option<&[u32]>,
    ) -> Result<Option<Self::Partial>>;
    fn finish(self, items: Vec<Self::Partial>, cx: &Cx) -> Result<Vec<DataChunk>>;
}

/// Groups by encoded key with the general accumulators: the fallback for the
/// group shapes the flat table declines, and the only path for aggregates with
/// `FILTER (WHERE …)`, `DISTINCT` and the rest.
pub(crate) struct GeneralSink<'a> {
    keys: &'a [BoundExpr],
    aggs: &'a [BoundAgg],
}

impl<'a> GeneralSink<'a> {
    pub(crate) fn new(keys: &'a [BoundExpr], aggs: &'a [BoundAgg]) -> GeneralSink<'a> {
        GeneralSink { keys, aggs }
    }
}

impl Sink for GeneralSink<'_> {
    type Partial = Partial;

    fn fold_into(
        &self,
        cx: &Cx,
        chunk: &DataChunk,
        sel: Option<&[u32]>,
    ) -> Result<Option<Partial>> {
        let rows = sel.map_or(chunk.num_rows(), <[u32]>::len);
        let mut partial = Partial::for_rows(rows, cx.budget.memory());
        match aggregate::fold_chunk(cx, &mut partial, chunk, sel, self.keys, self.aggs) {
            Ok(()) => Ok(Some(partial)),
            // A row the selection dropped must not raise: retry on the kept
            // rows alone, the rule `aggregate::fold_kept_rows` keeps.
            Err(err) => match sel {
                None => Err(err),
                Some(sel) => {
                    let mut partial = Partial::for_rows(rows, cx.budget.memory());
                    match aggregate::fold_chunk(
                        cx,
                        &mut partial,
                        &chunk.take(sel),
                        None,
                        self.keys,
                        self.aggs,
                    ) {
                        Ok(()) => Ok(Some(partial)),
                        Err(_) => Err(err),
                    }
                }
            },
        }
    }

    fn finish(self, items: Vec<Partial>, cx: &Cx) -> Result<Vec<DataChunk>> {
        let key_types: Vec<DataType> = self.keys.iter().map(BoundExpr::dtype).collect();
        aggregate::merge_and_emit(items, &key_types, self.aggs, cx.budget.memory())
    }
}

impl Sink for FlatSink<'_> {
    type Partial = FlatPartial;

    fn fold_into(
        &self,
        cx: &Cx,
        chunk: &DataChunk,
        sel: Option<&[u32]>,
    ) -> Result<Option<FlatPartial>> {
        FlatSink::fold(self, cx, chunk, sel)
    }

    fn finish(self, items: Vec<FlatPartial>, cx: &Cx) -> Result<Vec<DataChunk>> {
        FlatSink::merge(self, items, !cx.unordered, cx.budget.memory())
    }
}

/// One draining probe: the completed build side is only known at run time, so
/// the plan keeps what its compilation settled — the keys, the filters derived
/// from the build rows, the residual, and the layout.
struct ProbeOp<'a> {
    build: usize,
    preserved_keys: &'a [BoundExpr],
    build_keys: &'a [BoundExpr],
    residual: Option<BoundExpr>,
    filters: Option<Vec<Arc<crate::stats::JoinFilter>>>,
    preserved_is_right: bool,
    /// Emit the preserved rows no build row matched, NULL-extended: a left or
    /// right join.
    extend_unmatched: bool,
    /// The probe's input dtypes, for [`ProbePlan`].
    probe_types: Vec<DataType>,
    /// Which of the join's output columns anything above reads (wave 127).
    present: Option<Vec<bool>>,
}

impl<'a> ProbeOp<'a> {
    /// Build the match table once per statement: every morsel probes the same
    /// completed build side, so hashing its keys again per morsel would cost
    /// one build per chunk.
    fn prepare(&'a self, cx: &Cx, builds: &'a [Output]) -> Result<ProbePlan<'a>> {
        let build = &builds[self.build];
        ProbePlan::new(
            cx,
            &self.probe_types,
            &build.chunks,
            &build.types,
            self.preserved_keys,
            self.build_keys,
            self.preserved_is_right,
            self.extend_unmatched,
            self.filters.as_deref(),
            self.residual.as_ref(),
            self.present.as_deref(),
        )
    }
}

/// One draining semi/anti/mark probe: the materialized path's match table and
/// NULL bookkeeping, built once from the completed build side, then probed
/// chunk for chunk with the same assembly the materialized join uses.
struct MarkOp<'a> {
    build: usize,
    kind: JoinKind,
    preserved_keys: &'a [BoundExpr],
    build_keys: &'a [BoundExpr],
    residual: Option<BoundExpr>,
    filters: Option<Vec<Arc<crate::stats::JoinFilter>>>,
    /// The preserved side's dtypes.
    probe_types: Vec<DataType>,
}

impl<'a> MarkOp<'a> {
    fn prepare(&'a self, cx: &Cx, builds: &'a [Output]) -> Result<MarkProbe<'a>> {
        let build = &builds[self.build];
        Ok(MarkProbe {
            how: MarkJoin::new(
                cx,
                self.kind,
                self.preserved_keys,
                &self.probe_types,
                &build.chunks,
                self.build_keys,
                self.filters.as_deref(),
                self.residual.as_ref(),
            )?,
            preserved_keys: self.preserved_keys,
        })
    }
}

/// A prepared [`MarkOp`]: the settled match state. Boxed by [`Step`], like a
/// probe's match table.
struct MarkProbe<'a> {
    how: MarkJoin<'a>,
    preserved_keys: &'a [BoundExpr],
}

impl MarkProbe<'_> {
    /// One output chunk, or `None` when every preserved row was dropped: a
    /// preserved chunk holds at most `CHUNK_SIZE` rows, so one goes out.
    fn probe(&self, cx: &Cx, chunk: &DataChunk, sel: Option<&[u32]>) -> Result<Option<DataChunk>> {
        // A residual tests gathered build pairs, so it needs a compacted chunk
        // (the materialized join compacts a filtered side too).
        let compacted;
        let (chunk, sel) = match (sel, self.how.residual.is_some()) {
            (Some(sel), true) => {
                compacted = chunk.take(sel);
                (&compacted, None)
            }
            (sel, _) => (chunk, sel),
        };
        let mut chunks = mark_probe_chunk(cx, &self.how, self.preserved_keys, chunk, sel)?;
        Ok(chunks.pop())
    }
}

/// One streaming operator above the source.
enum RunOp<'a> {
    /// Keep the rows a predicate accepts. The selection only narrows here: a
    /// row a filter drops costs nothing above it, and the chunk is compacted
    /// only where reading through the indices would.
    Filter {
        predicate: &'a BoundExpr,
    },
    /// Evaluate a projection over the kept rows, compacted first — the order
    /// the materialized filter → project path evaluates them in.
    Project {
        exprs: &'a [BoundExpr],
        read: Option<Vec<bool>>,
    },
    Probe(ProbeOp<'a>),
    Mark(MarkOp<'a>),
}

impl<'a> Step<'a> {
    fn apply(&self, cx: &Cx, mut morsel: Morsel, out: &mut Vec<Morsel>) -> Result<()> {
        match self {
            Step::Filter { predicate } => {
                let rows = morsel.chunk.num_rows();
                let mut sel = match morsel.sel.take() {
                    Some(sel) => sel,
                    None => selection::all_rows(rows),
                };
                expr::select_predicate(predicate, &morsel.chunk, cx, &mut sel)?;
                if sel.is_empty() {
                    return Ok(());
                }
                if sel.len() == rows {
                    // Nothing was dropped: no selection to carry.
                } else if selection::worth_materializing(sel.len(), rows) {
                    morsel.chunk = morsel.chunk.take(&sel);
                } else {
                    morsel.sel = Some(sel);
                }
                out.push(morsel);
                Ok(())
            }
            Step::Project { exprs, read } => {
                if let Some(sel) = morsel.sel.take() {
                    morsel.chunk = morsel.chunk.take(&sel);
                }
                morsel.chunk = project_chunk(cx, exprs, &morsel.chunk, *read)?;
                out.push(morsel);
                Ok(())
            }
            Step::Probe(plan) => {
                // A probe can emit more rows than a chunk holds: each output
                // chunk is its own morsel, folded as the materialized path's
                // join output chunks are.
                for chunk in plan.probe(cx, &morsel.chunk, morsel.sel.as_deref(), None)? {
                    out.push(Morsel { chunk, sel: None });
                }
                Ok(())
            }
            Step::Mark(plan) => {
                if let Some(chunk) = plan.probe(cx, &morsel.chunk, morsel.sel.as_deref())? {
                    out.push(Morsel { chunk, sel: None });
                }
                Ok(())
            }
        }
    }
}

/// One operator a morsel passes through, with its probe already prepared.
enum Step<'a> {
    Filter {
        predicate: &'a BoundExpr,
    },
    Project {
        exprs: &'a [BoundExpr],
        read: Option<&'a [bool]>,
    },
    /// Boxed: the match table dwarfs the other variants.
    Probe(Box<ProbePlan<'a>>),
    Mark(Box<MarkProbe<'a>>),
}

/// Prepare the compiled operators once, so a statement builds each probe's
/// match table once rather than once per morsel.
fn steps<'a>(cx: &Cx, builds: &'a [Output], ops: &'a [RunOp<'a>]) -> Result<Vec<Step<'a>>> {
    // Fill the subquery memo before the morsels fan out, as every materialized
    // operator does (see `expr::prime`): a worker that finds the memo empty
    // computes the body without waiting, so a filter over a subquery would run
    // that subquery once per morsel on every worker instead of once.
    for op in ops {
        match op {
            RunOp::Filter { predicate } => expr::prime(cx, [*predicate])?,
            RunOp::Project { exprs, .. } => expr::prime(cx, *exprs)?,
            RunOp::Probe(probe) => {
                expr::prime(cx, probe.preserved_keys)?;
                expr::prime(cx, probe.residual.as_ref())?;
            }
            RunOp::Mark(probe) => {
                expr::prime(cx, probe.preserved_keys)?;
                expr::prime(cx, probe.residual.as_ref())?;
            }
        }
    }
    ops.iter()
        .map(|op| {
            Ok(match op {
                RunOp::Filter { predicate } => Step::Filter { predicate },
                RunOp::Project { exprs, read } => Step::Project {
                    exprs,
                    read: read.as_deref(),
                },
                RunOp::Probe(probe) => Step::Probe(Box::new(probe.prepare(cx, builds)?)),
                RunOp::Mark(probe) => Step::Mark(Box::new(probe.prepare(cx, builds)?)),
            })
        })
        .collect()
}

/// The plan node the compile walked through, top-down.
enum Node<'a> {
    Filter {
        predicate: &'a BoundExpr,
    },
    Project {
        plan: &'a LogicalPlan,
        exprs: &'a [BoundExpr],
        names: &'a [String],
    },
    Join {
        plan: &'a LogicalPlan,
        kind: JoinKind,
        build: usize,
        preserved_keys: &'a [BoundExpr],
        build_keys: &'a [BoundExpr],
        residual: Option<BoundExpr>,
        filters: Option<Vec<Arc<crate::stats::JoinFilter>>>,
        build_left: bool,
    },
}

/// Fold `Aggregate`'s input through a pipeline, or `Ok(None)` when the shape is
/// not one this module compiles.
pub(crate) fn aggregate(
    cx: &Cx,
    input: &LogicalPlan,
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
) -> Result<Option<Output>> {
    if !cx.pipeline_execution() || aggs.is_empty() {
        return Ok(None);
    }
    let Some(compiled) = compile(cx, input)? else {
        return Ok(None);
    };
    let chunks = match FlatSink::new(keys, aggs, compiled.rows, compiled.types.len()) {
        Some(flat) => match run(cx, &compiled, &flat)? {
            Some(items) => flat.finish(items, cx)?,
            // A key column whose payload is not the one its type promises:
            // the flat table declines, as it does for the materialized path.
            None => general(cx, &compiled, keys, aggs)?,
        },
        None => general(cx, &compiled, keys, aggs)?,
    };
    let (names, types) = crate::plan::agg_output_schema(keys, aggs);
    Ok(Some(Output {
        chunks,
        names,
        types,
    }))
}

/// Fold a chain of `Filter`, `Project` and join nodes into the materialized
/// `Output` the node above it consumes, or `Ok(None)` when the shape is not one
/// this module compiles. The chunks are the materialized path's: same chunks,
/// same order, so whatever reads them sees no difference.
pub(crate) fn collect(cx: &Cx, plan: &LogicalPlan) -> Result<Option<Output>> {
    if !cx.pipeline_execution() {
        return Ok(None);
    }
    let Some(compiled) = compile(cx, plan)? else {
        return Ok(None);
    };
    let Some(chunks) = run(cx, &compiled, &CollectSink::default())? else {
        return Err(PinkDbError::Execution(
            "internal: the pipeline's collect sink declined a chunk".to_string(),
        ));
    };
    Ok(Some(Output {
        chunks,
        names: compiled.names,
        types: compiled.types,
    }))
}

/// The collect sink: the morsels' chunks themselves, in morsel order — what the
/// materialized path's intermediate result holds, chunk for chunk. The chunks
/// accumulate, so they are charged to the statement's pool as they arrive
/// (wave 136a) and released when the sink finishes.
#[derive(Default)]
struct CollectSink {
    held: std::sync::Mutex<crate::memory::Reservation>,
}

impl Default for crate::memory::Reservation {
    fn default() -> crate::memory::Reservation {
        crate::memory::Reservation::empty()
    }
}

impl Sink for CollectSink {
    type Partial = DataChunk;

    fn fold_into(
        &self,
        cx: &Cx,
        chunk: &DataChunk,
        sel: Option<&[u32]>,
    ) -> Result<Option<DataChunk>> {
        let out = match sel {
            None => chunk.clone(),
            Some(sel) => chunk.take(sel),
        };
        let mut held = self
            .held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if held.bytes() == 0 {
            *held = cx.budget.memory().empty_reservation();
        }
        held.grow(chunk_approx_bytes(&out))?;
        Ok(Some(out))
    }

    fn finish(self, items: Vec<DataChunk>, _cx: &Cx) -> Result<Vec<DataChunk>> {
        Ok(items)
    }
}

/// A compiled pipeline: the materialized source, every join's completed build
/// side, and the operators a morsel passes through, bottom-up.
struct Compiled<'a> {
    source: Output,
    builds: Vec<Output>,
    ops: Vec<RunOp<'a>>,
    /// The dtypes a morsel carries above the last operator: what the sink folds.
    types: Vec<DataType>,
    /// Their names, for [`collect`]' parent.
    names: Vec<String>,
    /// Source rows, the flat table's bucket-count hint (never a result).
    rows: usize,
}

/// The materialized shape's own fallback for a shape the flat table declined
/// mid-stream. Re-running costs one extra pass over the source, which only
/// happens for a group key whose encoded layout differs from its bound type.
fn general(
    cx: &Cx,
    compiled: &Compiled<'_>,
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
) -> Result<Vec<DataChunk>> {
    let sink = GeneralSink::new(keys, aggs);
    match run(cx, compiled, &sink)? {
        Some(items) => sink.finish(items, cx),
        None => Err(PinkDbError::Execution(
            "internal: the pipeline's general aggregate sink declined a chunk".to_string(),
        )),
    }
}

/// Walk `input` top-down, compiling its operators and evaluating each join's
/// build side. Returns the source plan the operators sit on, or `None` when
/// nothing above the source compiles.
///
/// The join filters pushed for a probe side are left in `frames` for the caller
/// to pop: the source under them is evaluated by the caller, and it must see
/// them. Handing them back instead of popping here is what keeps every error
/// path in this walk from leaving a filter bound for the scans a later
/// statement evaluates.
fn descend<'a>(
    cx: &Cx,
    input: &'a LogicalPlan,
    nodes: &mut Vec<Node<'a>>,
    builds: &mut Vec<Output>,
    frames: &mut Vec<usize>,
) -> Result<Option<&'a LogicalPlan>> {
    let mut cursor = input;
    loop {
        // A subtree the statement uses more than once is answered from
        // `eval_shared_join`'s cache: flowing through it here would evaluate
        // it a second time and leave the cache cold, so it is the source.
        if cx.subplans.has_candidates() && cx.subplans.slot(cursor).is_some() {
            break;
        }
        match cursor {
            LogicalPlan::Filter {
                input: inner,
                predicate,
            } => {
                nodes.push(Node::Filter { predicate });
                cursor = inner;
            }
            LogicalPlan::Project {
                input: inner,
                exprs,
                names,
            } => {
                nodes.push(Node::Project {
                    plan: cursor,
                    exprs,
                    names,
                });
                cursor = inner;
            }
            LogicalPlan::Join {
                kind,
                left,
                right,
                left_keys,
                right_keys,
                residual,
                ..
            } => {
                // Which side is built and which streams through: the side the
                // materialized `hash_join` builds today — `build_on_left` for an
                // inner join, the unpreserved side for every other kind.
                let (build_plan, build_keys, probe_plan, probe_keys, build_left) = match kind {
                    JoinKind::Inner => {
                        if build_on_left(cx, *kind, left, right) {
                            (&**left, left_keys, &**right, right_keys, true)
                        } else {
                            (&**right, right_keys, &**left, left_keys, false)
                        }
                    }
                    JoinKind::Left => (&**right, right_keys, &**left, left_keys, false),
                    JoinKind::Right => (&**left, left_keys, &**right, right_keys, true),
                    JoinKind::Mark if residual.is_some() => {
                        // A mark join with a residual is the materialized
                        // path's error to raise.
                        break;
                    }
                    // The materialized path marks or probes the left instead
                    // when it is provably smaller (wave 125a). That needs the
                    // left's exact row count, so an estimate that says so leaves
                    // the shape to it rather than ruling differently.
                    JoinKind::Semi | JoinKind::Anti | JoinKind::Mark
                        if matches!(
                            (estimate_rows(cx, left), estimate_rows(cx, right)),
                            (Some(l), Some(r)) if l < r
                        ) =>
                    {
                        break;
                    }
                    JoinKind::Semi | JoinKind::Anti | JoinKind::Mark => {
                        (&**right, right_keys, &**left, left_keys, false)
                    }
                    _ => break,
                };
                // Same reason as the loop-top break: a probe side the statement
                // shares is computed once, by its cache slot, so the join over
                // it is the source rather than one more operator streaming it.
                if cx.subplans.has_candidates() && cx.subplans.slot(probe_plan).is_some() {
                    break;
                }
                let build = eval(cx, build_plan)?;
                let mut filters = None;
                let mut frame = None;
                // Only an inner or semi probe may drop rows through a bloom
                // filter: a left, right, anti or mark probe keeps or counts
                // every preserved row.
                if cx.join_filter_pushdown() && matches!(kind, JoinKind::Inner | JoinKind::Semi) {
                    // Only a probe key that reaches a scan is worth a filter,
                    // and only a small build pays for one (wave 128).
                    let reachable = probe_keys.iter().any(|key| match key {
                        BoundExpr::Column { index, .. } => {
                            let mut targets = Vec::new();
                            scan_targets(probe_plan, *index, &mut targets);
                            !targets.is_empty()
                        }
                        _ => false,
                    });
                    let rows: u64 = build.chunks.iter().map(|c| c.num_rows() as u64).sum();
                    if reachable && filter_pays(rows as f64) {
                        filters = Some(derive_join_filters(
                            cx, &build, build_plan, probe_keys, build_keys,
                        )?);
                    }
                    frame = Some(cx.dynamic.push_plan(
                        probe_plan,
                        probe_keys,
                        filters.as_deref().unwrap_or(&[]),
                    ));
                }
                nodes.push(Node::Join {
                    plan: cursor,
                    kind: *kind,
                    build: builds.len(),
                    preserved_keys: probe_keys,
                    build_keys,
                    residual: residual.clone(),
                    filters,
                    build_left,
                });
                builds.push(build);
                if let Some(frame) = frame {
                    frames.push(frame);
                }
                cursor = probe_plan;
            }
            // Anything else is the source: `eval` feeds it from the chunks of
            // a scan or of a subtree the pipeline does not compile.
            _ => break,
        }
    }
    Ok(if nodes.is_empty() { None } else { Some(cursor) })
}

/// The materialized source, every join's completed build side, and the
/// operators above it, in the order a morsel passes through them.
fn compile<'a>(cx: &Cx, input: &'a LogicalPlan) -> Result<Option<Compiled<'a>>> {
    let mut nodes: Vec<Node<'a>> = Vec::new();
    let mut builds: Vec<Output> = Vec::new();
    let mut frames: Vec<usize> = Vec::new();
    let source = descend(cx, input, &mut nodes, &mut builds, &mut frames).and_then(|source| {
        // The source sits inside the join-filter frames, so it is evaluated
        // before they are popped.
        match source {
            Some(plan) => eval(cx, plan).map(Some),
            None => Ok(None),
        }
    });
    for frame in frames.into_iter().rev() {
        cx.dynamic.pop(frame);
    }
    let Some(source) = source? else {
        return Ok(None);
    };
    // Bottom-up: each node's output dtypes are its input's, transformed.
    let mut types: Vec<DataType> = source.types.clone();
    let mut names: Vec<String> = source.names.clone();
    let mut ops: Vec<RunOp<'a>> = Vec::with_capacity(nodes.len());
    for node in nodes.into_iter().rev() {
        match node {
            Node::Filter { predicate } => ops.push(RunOp::Filter { predicate }),
            Node::Project {
                plan,
                exprs,
                names: out_names,
            } => {
                let read = project_mask(cx, plan, exprs.len());
                types = exprs.iter().map(|e| e.dtype()).collect();
                names = out_names.to_vec();
                ops.push(RunOp::Project { exprs, read });
            }
            Node::Join {
                plan,
                kind,
                build,
                preserved_keys,
                build_keys,
                residual,
                filters,
                build_left,
            } => {
                let build_types = &builds[build].types;
                let build_names = &builds[build].names;
                let present = cx.projections.for_join(plan).map(|cols| {
                    let mut mask = vec![false; build_types.len() + types.len()];
                    for &c in cols {
                        if let Some(slot) = mask.get_mut(c) {
                            *slot = true;
                        }
                    }
                    mask
                });
                let (left_types, right_types) = if build_left {
                    (build_types, &types)
                } else {
                    (&types, build_types)
                };
                let (left_names, right_names) = if build_left {
                    (build_names, &names)
                } else {
                    (&names, build_names)
                };
                let mut out = left_types.clone();
                let mut out_names = left_names.clone();
                match kind {
                    // A semi/anti join emits the preserved row, a mark join the
                    // same row plus the marker (wave 120).
                    JoinKind::Semi | JoinKind::Anti => {}
                    JoinKind::Mark => {
                        out.push(DataType::Boolean);
                        out_names.push(MARK_NAME.to_string());
                    }
                    _ => {
                        out.extend(right_types.iter().copied());
                        out_names.extend(right_names.iter().cloned());
                    }
                }
                // The residual reads `left ++ right`; a probe that assembles
                // the preserved (right) side first is swapped to match.
                let residual = match residual {
                    Some(expr) if build_left => {
                        Some(swap_join_space(&expr, build_types.len(), types.len()))
                    }
                    residual => residual,
                };
                let probe_types = std::mem::replace(&mut types, out);
                match kind {
                    JoinKind::Semi | JoinKind::Anti | JoinKind::Mark => {
                        ops.push(RunOp::Mark(MarkOp {
                            build,
                            kind,
                            preserved_keys,
                            build_keys,
                            residual,
                            filters,
                            probe_types,
                        }));
                    }
                    _ => ops.push(RunOp::Probe(ProbeOp {
                        build,
                        preserved_keys,
                        build_keys,
                        residual,
                        filters,
                        preserved_is_right: build_left,
                        extend_unmatched: kind != JoinKind::Inner,
                        probe_types,
                        present,
                    })),
                }
                names = out_names;
            }
        }
    }
    let rows = source.chunks.iter().map(DataChunk::num_rows).sum();
    Ok(Some(Compiled {
        source,
        builds,
        ops,
        types,
        names,
        rows,
    }))
}

/// One worker per morsel, order preserved: each morsel's partials come back in
/// the order its chunks were folded, and the morsels in source order.
fn run<S: Sink + Sync>(
    cx: &Cx,
    compiled: &Compiled<'_>,
    sink: &S,
) -> Result<Option<Vec<S::Partial>>>
where
    S::Partial: Send,
{
    let steps = steps(cx, &compiled.builds, &compiled.ops)?;
    let folded: Vec<Result<Option<Vec<S::Partial>>>> = compiled
        .source
        .chunks
        .par_iter()
        .map(|chunk| flow(cx, &steps, sink, chunk))
        .collect();
    let mut items = Vec::new();
    for partials in folded {
        match partials? {
            Some(mut partials) => items.append(&mut partials),
            None => return Ok(None),
        }
    }
    Ok(Some(items))
}

fn flow<S: Sink + Sync>(
    cx: &Cx,
    steps: &[Step<'_>],
    sink: &S,
    chunk: &DataChunk,
) -> Result<Option<Vec<S::Partial>>>
where
    S::Partial: Send,
{
    let mut morsels = vec![Morsel {
        chunk: chunk.clone(),
        sel: None,
    }];
    // A builder's output is a list because a probe can emit more rows than a
    // chunk holds; each of them is folded as its own chunk, as the materialized
    // path's join output chunks are.
    for step in steps {
        let mut next = Vec::new();
        for morsel in std::mem::take(&mut morsels) {
            step.apply(cx, morsel, &mut next)?;
        }
        morsels = next;
        if morsels.is_empty() {
            break;
        }
    }
    // One source chunk can fan out into every output chunk of a wide probe, so
    // the fold spreads over the pool too. `collect` keeps the order the chunks
    // reach the sink, which is what fixes first-seen group order and float
    // accumulation order.
    let folded: Vec<Result<Option<S::Partial>>> = morsels
        .par_iter()
        .map(|morsel| sink.fold_into(cx, &morsel.chunk, morsel.sel.as_deref()))
        .collect();
    let mut partials = Vec::with_capacity(folded.len());
    for partial in folded {
        match partial? {
            Some(partial) => partials.push(partial),
            None => return Ok(None),
        }
    }
    Ok(Some(partials))
}
