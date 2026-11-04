//! The pipeline executor: one source of morsels, streaming operators, one sink.
//!
//! An executor that materializes every plan node pays a barrier at each
//! operator boundary: the pool waits while one operator runs, then starts the
//! next. Here a *morsel* — one independently decodable piece of the source —
//! flows through the filter, projection and hash-join probes above it into the
//! sink, and no intermediate result is ever collected. The sink keeps one
//! partial per chunk it folded; the partials merge once, in morsel order. That
//! is what keeps the answer the materialized path's answer: the same rows in
//! the same order, hence the same first-seen group order and the same
//! floating-point accumulation order.
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
//! # The source
//!
//! A scan is a [`MorselSource`]: the workers pull the next morsel and decode
//! it themselves — a run of up to [`CHUNKS_PER_TASK`] store chunks, a Parquet
//! row group or page range — push it through the same steps and fold it, so
//! the scan's memory is the morsels in flight (one per worker), not the table.
//! Partials are kept by morsel index, so a streamed scan merges in exactly the
//! order the materialized one would have. Every other source is a subtree
//! `eval` materializes first, and its chunks are the morsels.
//!
//! Every shape this module does not compile — a full join, a semi/anti/mark
//! join the materialized path decides to build the preserved side for, an
//! unsupported input, an aggregate in an outer context — is declined with
//! `Ok(None)` and the caller runs the materialized path unchanged. `SET
//! pipeline_execution = false` forces that path, which is how the two are
//! compared.

/// How many morsels pass between deadline checks.
const CHECK_DEADLINE_EVERY: usize = 64;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use rayon::prelude::*;

use super::aggregate::{self, Partial};
use super::group_table::{FlatPartial, FlatSink};
use super::{
    CHUNKS_PER_TASK, Cx, MARK_NAME, MarkJoin, Output, ProbePlan, build_on_left, chunk_approx_bytes,
    derive_join_filters, estimate_rows, eval, expr, filter_pays, mark_probe_chunk, project_chunk,
    project_mask, scan_targets, selection, swap_join_space,
};
use crate::morsel::{MorselCursor, MorselSource, ScanIo};
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
    /// The bytes of statement memory a partial holds, for [`dense`]'s sample.
    fn state(part: &Self::Partial) -> usize;
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

    fn state(part: &Partial) -> usize {
        part.bytes()
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

    fn state(part: &FlatPartial) -> usize {
        part.bytes()
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
    let streamed = compiled.source.streamed();
    let chunks = match FlatSink::new(keys, aggs, compiled.rows, compiled.types.len()) {
        Some(flat) => match sample(cx, &compiled, &flat, keys, aggs)? {
            // One entry per group instead of one per group per chunk, for the
            // group shapes whose state grows with the rows (wave 136c).
            Some(sample) if sample.dense => {
                let first = if sample.pulled {
                    sample.chunks
                } else {
                    Vec::new()
                };
                general_serial(cx, &compiled, keys, aggs, first)?
            }
            other => {
                // A source that gave the sampled morsel up must not read it
                // again: its partials go in front of the ones `run` returns.
                let seed = match other {
                    Some(sample) if sample.pulled => sample.partials,
                    _ => Vec::new(),
                };
                match run(cx, &compiled, &flat)? {
                    Some(mut items) => {
                        items.splice(0..0, seed);
                        flat.finish(items, cx)?
                    }
                    // A key column whose payload is not the one its type
                    // promises: the flat table declines, as it does for the
                    // materialized path. A streamed source has already been
                    // read, so the caller's materialized path re-reads it once.
                    None if streamed => return Ok(None),
                    None => general(cx, &compiled, keys, aggs)?,
                }
            }
        },
        // An aggregate the flat table cannot hold runs the general sink over
        // the one source read: a streamed source cannot be re-read.
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
    let Some(mut compiled) = compile(cx, plan)? else {
        return Ok(None);
    };
    let Some(chunks) = run(cx, &compiled, &CollectSink::new())? else {
        return Err(PinkDbError::Execution(
            "internal: the pipeline's collect sink declined a chunk".to_string(),
        ));
    };
    Ok(Some(Output {
        chunks,
        names: std::mem::take(&mut compiled.names),
        types: std::mem::take(&mut compiled.types),
    }))
}

/// The collect sink: the morsels' chunks themselves, in morsel order — what the
/// materialized path's intermediate result holds, chunk for chunk. The chunks
/// accumulate, so they are charged to the statement's pool as they arrive
/// (wave 136a) and released when the sink finishes. Each worker charges its own
/// slot: chunks from the pool's threads never contend on one shared lock.
struct CollectSink {
    held: Vec<std::sync::Mutex<crate::memory::Reservation>>,
}

impl CollectSink {
    fn new() -> Self {
        Self {
            held: (0..rayon::current_num_threads().max(1))
                .map(|_| std::sync::Mutex::new(crate::memory::Reservation::empty()))
                .collect(),
        }
    }
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
        let bytes = chunk_approx_bytes(&out);
        let slot = rayon::current_thread_index().unwrap_or(0) % self.held.len();
        let mut held = self.held[slot]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if held.bytes() == 0 {
            *held = cx.budget.memory().empty_reservation();
        }
        held.grow(bytes)?;
        Ok(Some(out))
    }

    fn finish(self, items: Vec<DataChunk>, _cx: &Cx) -> Result<Vec<DataChunk>> {
        Ok(items)
    }

    fn state(part: &DataChunk) -> usize {
        chunk_approx_bytes(part)
    }
}

/// Where a pipeline's morsels come from: a [`MorselSource`] pulled on demand
/// (a store scan, an attached-lake scan), or an `Output` a subtree `eval`
/// materialized first.
enum Source<'a> {
    Chunks(Output),
    Streamed(Box<dyn MorselSource + 'a>),
}

impl Source<'_> {
    fn types(&self) -> &[DataType] {
        match self {
            Source::Chunks(output) => &output.types,
            Source::Streamed(source) => source.types(),
        }
    }

    fn names(&self) -> &[String] {
        match self {
            Source::Chunks(output) => &output.names,
            Source::Streamed(source) => source.names(),
        }
    }

    fn rows(&self) -> usize {
        match self {
            Source::Chunks(output) => output.chunks.iter().map(DataChunk::num_rows).sum(),
            // A source that knows its rows without reading (a store scan's
            // footer) offers them; a lake scan has to run to count.
            Source::Streamed(source) => source.rows().unwrap_or(0),
        }
    }

    fn streamed(&self) -> bool {
        matches!(self, Source::Streamed(_))
    }
}

/// A compiled pipeline: the source, every join's completed build side, and the
/// operators a morsel passes through, bottom-up.
struct Compiled<'a> {
    source: Source<'a>,
    builds: Vec<Output>,
    ops: Vec<RunOp<'a>>,
    /// The dtypes a morsel carries above the last operator: what the sink folds.
    types: Vec<DataType>,
    /// Their names, for [`collect`]' parent.
    names: Vec<String>,
    /// Source rows, the flat table's bucket-count hint (never a result).
    rows: usize,
}

/// The build sides are freed by every worker, not one (wave 141b). A join's
/// build outputs can be hundreds of MB of chunks; freeing them on the thread
/// that returns the result was 8–12 % of TPC-H Q7 and Q10 at SF 10, with
/// every worker idle meanwhile. The chunks are dropped in parallel and before
/// the statement returns: a background free would only move that work into
/// the next statement (measured: Q22 slower).
impl Drop for Compiled<'_> {
    fn drop(&mut self) {
        use rayon::prelude::*;
        let chunks: Vec<DataChunk> = std::mem::take(&mut self.builds)
            .into_iter()
            .flat_map(|build| build.chunks)
            .collect();
        if chunks.len() > 1 {
            chunks.into_par_iter().for_each(drop);
        }
    }
}

/// Partial state a folded row may leave before the aggregate is treated as
/// holding state per row rather than per group. `od_pairs_rollup` keeps one
/// group per input row and 421 bytes of key strings per group, so a chunk's
/// partial costs ~300 bytes a row; the widest TPC-H aggregate holds 50, and a
/// VARCHAR key of small domain under 15. Only a string-valued group shape is
/// sampled at all, so nothing else pays for the check.
const DENSE_BYTES_PER_ROW: usize = 64;

/// The dense lane is off (2026-11-03). Its per-chunk `Partial` builds a full
/// group (owned key values, accumulators) for every input row and then merges
/// it, which made `od_pairs_rollup` 5–15× slower than the per-chunk path it
/// replaced (170–850 ms against 34 ms, interleaved A/B). Bounding wide-key
/// GROUP BY state comes back with a radix-partitioned aggregate table
/// (ROADMAP row 124, step 136c-0b), which replaces this lane.
const DENSE_LANE: bool = false;

/// Inputs smaller than this are never sampled: their partial state is bounded
/// by the input itself, and the serial sample would cost more than it saves.
/// `od_pairs_rollup` (245,570 rows) stays above it.
const DENSE_MIN_ROWS: usize = 131_072;

/// What the source's first morsel said: the chunks it decoded to, the flat
/// sink's partials for them, and whether the aggregate's state grows with the
/// rows. `pulled` marks a source that has given the morsel up — a lake scan
/// cannot re-read it — so the chunks and partials are used, not read again.
struct Sample<P> {
    chunks: Vec<DataChunk>,
    partials: Vec<P>,
    dense: bool,
    pulled: bool,
}

/// Read one morsel and measure what folding it left behind per row. A string
/// group key is what makes a partial grow with the rows: an integer key's
/// partial is a fixed number of bytes per distinct value, and TPC-H's widest
/// stays far below [`DENSE_BYTES_PER_ROW`], so those shapes are never sampled.
fn sample<S: Sink + Sync>(
    cx: &Cx,
    compiled: &Compiled<'_>,
    sink: &S,
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
) -> Result<Option<Sample<S::Partial>>>
where
    S::Partial: Send,
{
    if !DENSE_LANE {
        return Ok(None);
    }
    let strings = keys.iter().any(|key| key.dtype() == DataType::Varchar)
        || aggs.iter().any(|agg| {
            agg.distinct
                && agg
                    .arg
                    .as_ref()
                    .is_some_and(|arg| arg.dtype() == DataType::Varchar)
        });
    if !strings {
        return Ok(None);
    }
    // The sample folds a whole morsel on one thread before the parallel run
    // starts. On a small input that serial prefix is a large share of the
    // work (a 60k-row DISTINCT under a correlated NOT IN paid ~1 ms in the CI
    // guard), and the per-row state it guards against is bounded anyway: at
    // DENSE_BYTES_PER_ROW × DENSE_MIN_ROWS it cannot pass ~40 MB.
    let known_rows = match &compiled.source {
        Source::Chunks(source) => Some(source.chunks.iter().map(DataChunk::num_rows).sum()),
        Source::Streamed(source) => source.rows(),
    };
    if known_rows.is_some_and(|rows| rows < DENSE_MIN_ROWS) {
        return Ok(None);
    }
    let (chunks, pulled) = match &compiled.source {
        Source::Chunks(source) => match source.chunks.first() {
            Some(chunk) => (vec![chunk.clone()], false),
            None => return Ok(None),
        },
        Source::Streamed(source) => {
            let Some(morsel) = source.next_morsel()? else {
                return Ok(None);
            };
            let mut chunks = Vec::new();
            source.decode(morsel, &mut |chunk| {
                chunks.push(chunk);
                Ok(())
            })?;
            if chunks.is_empty() {
                return Ok(None);
            }
            (chunks, true)
        }
    };
    let rows: usize = chunks.iter().map(DataChunk::num_rows).sum();
    let steps = steps(cx, &compiled.builds, &compiled.ops)?;
    let mut partials = Vec::new();
    for chunk in &chunks {
        // A shape the sink declines is not one this lane takes over: the
        // caller falls back exactly as it does for the pipeline's own run.
        let Some(mut folded) = flow(cx, &steps, sink, chunk)? else {
            return Ok(None);
        };
        partials.append(&mut folded);
    }
    let state: usize = partials.iter().map(S::state).sum();
    Ok(Some(Sample {
        chunks,
        partials,
        dense: rows > 0 && state / rows >= DENSE_BYTES_PER_ROW,
        pulled,
    }))
}

/// Fold the source into one `Partial` per aggregate, serially: the state is one
/// entry per group instead of one per group per chunk. The shapes [`sample`]
/// selects are the ones whose per-chunk partials hold a group per row, so the
/// walk loses nothing they were not already paying for. `first` holds the
/// morsel the sample pulled from a streamed source.
fn general_serial(
    cx: &Cx,
    compiled: &Compiled<'_>,
    keys: &[BoundExpr],
    aggs: &[BoundAgg],
    first: Vec<DataChunk>,
) -> Result<Vec<DataChunk>> {
    let steps = steps(cx, &compiled.builds, &compiled.ops)?;
    let mut table: Option<Partial> = None;
    let fold = |table: &mut Option<Partial>, chunk: &DataChunk| -> Result<()> {
        for morsel in morsels(cx, &steps, chunk)? {
            // A selection is compacted first: a row it dropped must not raise,
            // the rule `GeneralSink::fold_into` keeps.
            let mut part = Partial::for_rows(super::CHUNK_SIZE, cx.budget.memory());
            match morsel.sel {
                None => aggregate::fold_chunk(cx, &mut part, &morsel.chunk, None, keys, aggs)?,
                Some(sel) => aggregate::fold_chunk(
                    cx,
                    &mut part,
                    &morsel.chunk.take(&sel),
                    None,
                    keys,
                    aggs,
                )?,
            }
            match table {
                Some(table) => table.absorb(part)?,
                None => *table = Some(part),
            }
        }
        Ok(())
    };
    for chunk in &first {
        fold(&mut table, chunk)?;
    }
    match &compiled.source {
        Source::Chunks(source) => {
            for chunk in &source.chunks {
                fold(&mut table, chunk)?;
            }
        }
        Source::Streamed(source) => {
            // Pull the rest through the same interface the parallel run uses,
            // one morsel at a time: a lake provider charges its morsels to the
            // pool as they are decoded, so the lane keeps that accounting.
            while let Some(morsel) = source.next_morsel()? {
                if morsel % CHECK_DEADLINE_EVERY as u64 == 0 {
                    cx.check_deadline()?;
                }
                source.decode(morsel, &mut |chunk| fold(&mut table, &chunk))?;
            }
        }
    }
    let key_types: Vec<DataType> = keys.iter().map(BoundExpr::dtype).collect();
    // One partial is already one entry per group: merging it again would
    // reserve its bytes a second time, which is the peak this lane exists to
    // avoid.
    let groups = aggregate::merge_partials(table.into_iter().collect(), !keys.is_empty(), aggs);
    aggregate::emit_groups(groups, &key_types, aggs)
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
    let sample = sample(cx, compiled, &sink, keys, aggs)?;
    // One entry per group instead of one per group per chunk, for the group
    // shapes whose state grows with the rows (wave 136c).
    if sample.as_ref().is_some_and(|sample| sample.dense) {
        let first = sample
            .map(|sample| {
                if sample.pulled {
                    sample.chunks
                } else {
                    Vec::new()
                }
            })
            .unwrap_or_default();
        return general_serial(cx, compiled, keys, aggs, first);
    }
    // A source that gave the sampled morsel up keeps its partials: they go in
    // front of the ones the run returns.
    let seed = match sample {
        Some(sample) if sample.pulled => sample.partials,
        _ => Vec::new(),
    };
    match run(cx, compiled, &sink)? {
        Some(mut items) => {
            items.splice(0..0, seed);
            sink.finish(items, cx)
        }
        None => Err(PinkDbError::Execution(
            "internal: the pipeline's general aggregate sink declined a chunk".to_string(),
        )),
    }
}

/// Walk `input` top-down, compiling its operators and evaluating each join's
/// build side. Returns the source plan the operators sit on; the caller
/// decides whether the walk found anything worth pipelining.
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
    // Set when the walk stops at something the pipeline cannot stream: the
    // caller must then `eval` the plan, which would come back here.
    let mut declined = false;
    loop {
        // A subtree the statement uses more than once is answered from
        // `eval_shared_join`'s cache: flowing through it here would evaluate
        // it a second time and leave the cache cold, so it is the source.
        if cx.subplans.has_candidates() && cx.subplans.slot(cursor).is_some() {
            declined = true;
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
                        declined = true;
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
                        declined = true;
                        break;
                    }
                    JoinKind::Semi | JoinKind::Anti | JoinKind::Mark => {
                        (&**right, right_keys, &**left, left_keys, false)
                    }
                    _ => {
                        declined = true;
                        break;
                    }
                };
                // Same reason as the loop-top break: a probe side the statement
                // shares is computed once, by its cache slot, so the join over
                // it is the source rather than one more operator streaming it.
                if cx.subplans.has_candidates() && cx.subplans.slot(probe_plan).is_some() {
                    declined = true;
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
            // A scan is the source the pipeline streams from. Anything else
            // is a subtree `eval` materializes first, and answering it by
            // `eval` here would enter this same walk again.
            LogicalPlan::Scan { .. } => break,
            _ => {
                declined = true;
                break;
            }
        }
    }
    Ok(if declined && nodes.is_empty() {
        None
    } else {
        Some(cursor)
    })
}

/// The materialized source, every join's completed build side, and the
/// operators above it, in the order a morsel passes through them.
fn compile<'a>(cx: &'a Cx, input: &'a LogicalPlan) -> Result<Option<Compiled<'a>>> {
    let mut nodes: Vec<Node<'a>> = Vec::new();
    let mut builds: Vec<Output> = Vec::new();
    let mut frames: Vec<usize> = Vec::new();
    let source = descend(cx, input, &mut nodes, &mut builds, &mut frames).and_then(|source| {
        // The source sits inside the join-filter frames, so it is evaluated
        // before they are popped.
        match source {
            Some(plan) => source_of(cx, plan, !nodes.is_empty()),
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
    let mut types: Vec<DataType> = source.types().to_vec();
    let mut names: Vec<String> = source.names().to_vec();
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
    let rows = source.rows();
    Ok(Some(Compiled {
        source,
        builds,
        ops,
        types,
        names,
        rows,
    }))
}

/// The pipeline's source: a store scan and an attached-lake scan are both
/// pulled morsel by morsel; everything else `eval` materializes (and must
/// never come back here for the same plan).
fn source_of<'a>(cx: &'a Cx, plan: &'a LogicalPlan, fused: bool) -> Result<Option<Source<'a>>> {
    if let Some(scan) = super::store_scan(cx, plan)? {
        return Ok(Some(Source::Streamed(Box::new(StoreSource::new(scan)))));
    }
    if let Some(source) = super::lake_source(cx, plan)? {
        return Ok(Some(Source::Streamed(source)));
    }
    // Nothing above the source is fused, so a materialized source is handed
    // back to the caller's own path rather than read here and read again
    // there.
    if !fused {
        return Ok(None);
    }
    eval(cx, plan).map(|out| Some(Source::Chunks(out)))
}

/// A store scan as a [`MorselSource`]: the worker that claims a run of chunks
/// decodes them itself, so no pool thread waits for another thread's chunks.
/// A run is a morsel for the same reason [`StoreScan::par_chunks`] folds runs:
/// a task per chunk pays dispatch on every chunk of a long scan.
#[derive(Debug)]
struct StoreSource<'a> {
    scan: super::StoreScan<'a>,
    names: Vec<String>,
    types: Vec<DataType>,
    cursor: MorselCursor,
}

impl<'a> StoreSource<'a> {
    fn new(scan: super::StoreScan<'a>) -> Self {
        let (names, types) = scan.schema();
        let cursor = MorselCursor::new(scan.len().div_ceil(CHUNKS_PER_TASK) as u64);
        StoreSource {
            scan,
            names,
            types,
            cursor,
        }
    }
}

impl MorselSource for StoreSource<'_> {
    fn names(&self) -> &[String] {
        &self.names
    }

    fn types(&self) -> &[DataType] {
        &self.types
    }

    fn label(&self) -> &str {
        "store"
    }

    fn rows(&self) -> Option<usize> {
        Some(self.scan.rows())
    }

    fn next_morsel(&self) -> Result<Option<u64>> {
        Ok(self.cursor.claim())
    }

    fn decode(&self, morsel: u64, sink: &mut dyn FnMut(DataChunk) -> Result<()>) -> Result<()> {
        let first = morsel as usize * CHUNKS_PER_TASK;
        let last = (first + CHUNKS_PER_TASK).min(self.scan.len());
        for index in first..last {
            if let Some(chunk) = self.scan.chunk(index)? {
                sink(chunk)?;
            }
        }
        Ok(())
    }

    fn io(&self) -> ScanIo {
        ScanIo::default()
    }

    fn reports_io(&self) -> bool {
        false
    }
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
    match &compiled.source {
        Source::Chunks(output) => run_chunks(cx, &steps, sink, &output.chunks),
        Source::Streamed(source) => run_streamed(cx, &steps, sink, source.as_ref()),
    }
}

fn run_chunks<S: Sink + Sync>(
    cx: &Cx,
    steps: &[Step<'_>],
    sink: &S,
    chunks: &[DataChunk],
) -> Result<Option<Vec<S::Partial>>>
where
    S::Partial: Send,
{
    let folded: Vec<Result<Option<Vec<S::Partial>>>> = chunks
        .par_iter()
        .map(|chunk| flow(cx, steps, sink, chunk))
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

/// Pull morsels from a shared source state until it is empty: one worker per
/// pool thread claims the next morsel and decodes it itself, so at most one
/// morsel per worker is in flight and nothing waits on another thread for
/// rows. Partials are sorted by morsel index before they merge, which is what
/// keeps first-seen group order, float accumulation order and collected row
/// order identical to the materialized path's.
fn run_streamed<S: Sink + Sync>(
    cx: &Cx,
    steps: &[Step<'_>],
    sink: &S,
    source: &dyn MorselSource,
) -> Result<Option<Vec<S::Partial>>>
where
    S::Partial: Send,
{
    let workers = rayon::current_num_threads().max(1);
    // A failed morsel stops every worker: the statement fails anyway, and a
    // scan must not keep decoding for an answer nobody will read.
    let stop = AtomicBool::new(false);
    let in_flight = AtomicU64::new(0);
    let peak = AtomicU64::new(0);
    // A morsel's partials, or `None` when the sink declined the chunk.
    type Folded<P> = Result<Option<Vec<P>>>;
    let folded: Mutex<Vec<(u64, Folded<S::Partial>)>> = Mutex::new(Vec::new());
    (0..workers).into_par_iter().for_each(|_| {
        while !stop.load(Ordering::Relaxed) {
            let morsel = match source.next_morsel() {
                Ok(Some(morsel)) => morsel,
                Ok(None) => return,
                Err(error) => {
                    stop.store(true, Ordering::Relaxed);
                    // `u64::MAX` sorts after every morsel, so a source error
                    // surfaces only when no morsel failed first.
                    folded
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push((u64::MAX, Err(error)));
                    return;
                }
            };
            // The deadline is a runaway guard, not a per-row check: a batch
            // of morsels between checks keeps a long scan abortable without
            // an `Instant::now()` in every fold.
            if morsel % CHECK_DEADLINE_EVERY as u64 == 0
                && let Err(error) = cx.check_deadline()
            {
                stop.store(true, Ordering::Relaxed);
                folded
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push((u64::MAX, Err(error)));
                return;
            }
            let now = in_flight.fetch_add(1, Ordering::Relaxed) + 1;
            peak.fetch_max(now, Ordering::Relaxed);
            // The claiming worker decodes its own morsel. It is the only
            // thread that ever waits for morsels, so a decode is free to use
            // the pool it runs on (a provider's own parallel reads, the fold
            // below): no other thread waits on this one.
            let mut partials = Vec::new();
            let mut declined = false;
            let decoded = source.decode(morsel, &mut |chunk| {
                if declined {
                    return Ok(());
                }
                match flow(cx, steps, sink, &chunk) {
                    Ok(Some(mut folded)) => {
                        partials.append(&mut folded);
                        Ok(())
                    }
                    // The sink does not know this shape: stop, and let the
                    // caller run a plan it does know.
                    Ok(None) => {
                        declined = true;
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            });
            in_flight.fetch_sub(1, Ordering::Relaxed);
            let folded_morsel = decoded.map(|()| (!declined).then_some(partials));
            if declined || folded_morsel.is_err() {
                stop.store(true, Ordering::Relaxed);
            }
            folded
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((morsel, folded_morsel));
        }
    });
    if source.reports_io() {
        let mut io = source.io();
        io.in_flight_peak = peak.load(Ordering::Relaxed);
        cx.db.record_scan(source.label(), io);
    }
    let mut folded = folded.into_inner().unwrap_or_else(|e| e.into_inner());
    folded.sort_unstable_by_key(|(index, _)| *index);
    let mut items = Vec::new();
    for (_, partials) in folded {
        match partials? {
            Some(mut partials) => items.append(&mut partials),
            None => return Ok(None),
        }
    }
    Ok(Some(items))
}

/// Run `plan`'s pipeline and hand every output chunk to `consume` in the
/// order [`collect`] would return them, as soon as the chunks before it are
/// done: a bulk load encodes and writes while the scan still runs (the
/// streaming load). `Ok(None)` when the plan is not a pipeline this module
/// compiles (neither callback ran); otherwise the output's names and types,
/// which `begin` also receives before the first chunk.
///
/// `consume` runs on the calling thread. At most a few morsels per worker
/// wait for it: a worker ahead of the consumer yields to the pool instead of
/// claiming another morsel, so the consumer's own parallel work (the row
/// group encode) runs on the threads the scan would otherwise hold.
pub(crate) fn stream_collect(
    cx: &Cx,
    plan: &LogicalPlan,
    begin: &mut dyn FnMut(&[String], &[DataType]) -> Result<()>,
    consume: &mut dyn FnMut(DataChunk) -> Result<()>,
) -> Result<Option<(Vec<String>, Vec<DataType>)>> {
    if !cx.pipeline_execution() {
        return Ok(None);
    }
    let Some(compiled) = compile(cx, plan)? else {
        return Ok(None);
    };
    // The output's shape is known before the first chunk.
    begin(&compiled.names, &compiled.types)?;
    let steps = steps(cx, &compiled.builds, &compiled.ops)?;
    let sink = PassSink;
    let declined = || {
        PinkDbError::Execution("internal: the pipeline's collect sink declined a chunk".to_string())
    };
    match &compiled.source {
        Source::Chunks(output) => {
            let Some(chunks) = run_chunks(cx, &steps, &sink, &output.chunks)? else {
                return Err(declined());
            };
            for chunk in chunks {
                consume(chunk)?;
            }
        }
        Source::Streamed(source) => {
            if !run_streamed_ordered(cx, &steps, &sink, source.as_ref(), consume)? {
                return Err(declined());
            }
        }
    }
    Ok(Some((compiled.names.clone(), compiled.types.clone())))
}

/// [`CollectSink`] without the statement charge: a streamed load hands each
/// chunk on as soon as it is in order, so the chunks never accumulate here.
struct PassSink;

impl Sink for PassSink {
    type Partial = DataChunk;

    fn fold_into(
        &self,
        _cx: &Cx,
        chunk: &DataChunk,
        sel: Option<&[u32]>,
    ) -> Result<Option<DataChunk>> {
        Ok(Some(match sel {
            None => chunk.clone(),
            Some(sel) => chunk.take(sel),
        }))
    }

    fn finish(self, items: Vec<DataChunk>, _cx: &Cx) -> Result<Vec<DataChunk>> {
        Ok(items)
    }

    fn state(part: &DataChunk) -> usize {
        chunk_approx_bytes(part)
    }
}

/// Morsels a streamed load may hold ahead of its consumer, per worker.
const ORDERED_AHEAD_PER_WORKER: u64 = 2;

/// [`run_streamed`] for [`stream_collect`]: the same workers and morsel order,
/// but each morsel's chunks go to `consume` once every earlier morsel's have.
/// `Ok(false)` when the sink declined a chunk.
fn run_streamed_ordered<S: Sink<Partial = DataChunk> + Sync>(
    cx: &Cx,
    steps: &[Step<'_>],
    sink: &S,
    source: &dyn MorselSource,
    consume: &mut dyn FnMut(DataChunk) -> Result<()>,
) -> Result<bool> {
    type Folded = Result<Option<Vec<DataChunk>>>;
    struct Reorder {
        next: u64,
        ready: std::collections::BTreeMap<u64, Folded>,
    }
    let workers = rayon::current_num_threads().max(1);
    let ahead_cap = ORDERED_AHEAD_PER_WORKER * workers as u64;
    let stop = AtomicBool::new(false);
    let in_flight = AtomicU64::new(0);
    let peak = AtomicU64::new(0);
    // Morsels claimed but not yet taken by the consumer.
    let ahead = AtomicU64::new(0);
    let reorder = Mutex::new(Reorder {
        next: 0,
        ready: std::collections::BTreeMap::new(),
    });
    let (tx, rx) = std::sync::mpsc::channel::<Folded>();
    let (stop, ahead, in_flight, peak, reorder) = (&stop, &ahead, &in_flight, &peak, &reorder);
    let outcome = std::thread::scope(|scope| {
        // The producer owns the sender: the channel closes when it is done.
        let producer = scope.spawn(move || {
            (0..workers).into_par_iter().for_each(|_| {
                let tx = tx.clone();
                while !stop.load(Ordering::Relaxed) {
                    // Backpressure without blocking a pool thread: run other
                    // pool work (the consumer's encode) until there is room.
                    if ahead.load(Ordering::Acquire) >= ahead_cap {
                        if rayon::yield_now() != Some(rayon::Yield::Executed) {
                            std::thread::sleep(std::time::Duration::from_micros(50));
                        }
                        continue;
                    }
                    let morsel = match source.next_morsel() {
                        Ok(Some(morsel)) => morsel,
                        Ok(None) => return,
                        Err(error) => {
                            stop.store(true, Ordering::Relaxed);
                            let _ = tx.send(Err(error));
                            return;
                        }
                    };
                    ahead.fetch_add(1, Ordering::AcqRel);
                    if morsel % CHECK_DEADLINE_EVERY as u64 == 0
                        && let Err(error) = cx.check_deadline()
                    {
                        stop.store(true, Ordering::Relaxed);
                        let _ = tx.send(Err(error));
                        return;
                    }
                    let now = in_flight.fetch_add(1, Ordering::Relaxed) + 1;
                    peak.fetch_max(now, Ordering::Relaxed);
                    let mut partials = Vec::new();
                    let mut declined = false;
                    let decoded = source.decode(morsel, &mut |chunk| {
                        if declined {
                            return Ok(());
                        }
                        match flow(cx, steps, sink, &chunk) {
                            Ok(Some(mut folded)) => {
                                partials.append(&mut folded);
                                Ok(())
                            }
                            Ok(None) => {
                                declined = true;
                                Ok(())
                            }
                            Err(error) => Err(error),
                        }
                    });
                    in_flight.fetch_sub(1, Ordering::Relaxed);
                    let folded = decoded.map(|()| (!declined).then_some(partials));
                    if declined || folded.is_err() {
                        stop.store(true, Ordering::Relaxed);
                    }
                    // Hand over every morsel that is now next in order.
                    let mut order = reorder.lock().unwrap_or_else(|e| e.into_inner());
                    order.ready.insert(morsel, folded);
                    loop {
                        let next = order.next;
                        let Some(item) = order.ready.remove(&next) else {
                            break;
                        };
                        order.next += 1;
                        if tx.send(item).is_err() {
                            stop.store(true, Ordering::Relaxed);
                            break;
                        }
                    }
                    // Give the consumer's pool work a turn between morsels.
                    let _ = rayon::yield_now();
                }
            });
        });
        let mut result: Result<bool> = Ok(true);
        for item in rx.iter() {
            ahead.fetch_sub(1, Ordering::AcqRel);
            match item {
                Ok(Some(chunks)) => {
                    for chunk in chunks {
                        if let Err(error) = consume(chunk) {
                            result = Err(error);
                            break;
                        }
                    }
                }
                Ok(None) => result = Ok(false),
                Err(error) => result = Err(error),
            }
            if !matches!(result, Ok(true)) {
                stop.store(true, Ordering::Relaxed);
                break;
            }
        }
        // Workers still running stop at their next claim; dropping the
        // receiver makes their sends fail instead of queueing.
        drop(rx);
        producer
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        result
    });
    if source.reports_io() {
        let mut io = source.io();
        io.in_flight_peak = peak.load(Ordering::Relaxed);
        cx.db.record_scan(source.label(), io);
    }
    outcome
}

/// The morsels one source chunk becomes after the operators above it: the chunk
/// itself plus whatever a probe adds.
fn morsels(cx: &Cx, steps: &[Step<'_>], chunk: &DataChunk) -> Result<Vec<Morsel>> {
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
    Ok(morsels)
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
    let morsels = morsels(cx, steps, chunk)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::ExecBudget;

    /// The collect sink must return every chunk exactly once, in source order,
    /// no matter which worker folded it or which reservation slot it took.
    #[test]
    fn collect_sink_keeps_every_chunk_in_order_across_workers() {
        let db = crate::storage::PinkDb::new();
        let cx = Cx::with_budget(&db, std::sync::Arc::new(ExecBudget::unbounded()));
        let chunks: Vec<DataChunk> = (0..4096)
            .map(|i| {
                let mut v = crate::vector::Vector::new_empty(crate::types::DataType::Integer);
                v.push_int32(i).expect("push");
                DataChunk::new(vec![v]).expect("chunk")
            })
            .collect();
        let steps = steps(&cx, &[], &[]).expect("steps");
        let got = run_chunks(&cx, &steps, &CollectSink::new(), &chunks)
            .expect("run")
            .expect("sink collected");
        assert_eq!(got.len(), chunks.len());
        for (i, chunk) in got.iter().enumerate() {
            assert_eq!(chunk.num_rows(), 1);
            assert_eq!(
                chunk.columns[0].get(0),
                crate::types::Value::Integer(i as i32)
            );
        }
    }
}
