# Wave 135 — Push-based pipelines (ROADMAP 123b + 122b)

Planned 2026-11-01, stream C of the QuackLab track. ROADMAP 123b
(push-based pipelines with morsel parallelism) and 122b (late
materialization of join payload columns), then dictionary vectors for
low-cardinality strings. It is the biggest structural change in the
executor, so it lands as a series of reviewable steps, each keeping all 22
TPC-H queries identical to DuckDB at SF 0.01 and SF 1.

## What exists

- **A materializing interpreter.** `exec/mod.rs` `eval` → `eval_node`
  evaluates each plan node to a complete `Output { chunks }` before its
  parent starts. Parallelism is rayon `par_iter` over the chunks *inside*
  one operator (filter, project, probe, fold), so every operator boundary is
  a barrier: the workers finish, results are collected into a `Vec`, and the
  next operator fans out again.
- **Hand-fused shortcuts** cover the hottest shapes, each with its own
  fusibility rules: `selected_chunks` (an aggregate folds through a filter's
  selection), `probe_through_selection` (an inner/semi probe reads through a
  filter), `fused_join_aggregate` (an aggregate fed straight from one
  join's matches, general `Partial`s only), `fused_scan_aggregate` /
  `stream_leaf_chunks` (streaming table-function and lake scans into a
  fold), join-filter pushdown into scans (122), shared repeated joins (125d).
- **Where the time goes** (baseline 236c4f2 / wave 140, `sample`, load ~5):
  - TPC-H Q9: 30 % of samples are idle workers (`__psynch_cvwait`), 25 %
    the probe, ~15 % materializing join outputs (`Vector::take`,
    `gather_located`, `malloc`/`free`). Q21: 25 % idle, 11 %
    `Vector::take`. Q3: 46 % idle. Q1: 24 % idle.
  - QuackLab mart refresh (55 views, 9.1 s against DuckDB's 2.5 s after
    wave 140): every view is `INSERT INTO mart SELECT … GROUP BY` over the
    1.92M-row fact joined to small dimensions. The slowest single
    statements run 4.5–23× DuckDB. `PEAK_HOUR_DEMAND.BY_HOUR_BOROUGH`
    (fact ⋈ 24 hours ⋈ 265 zones, GROUP BY five dimension columns) takes
    362 ms against 12 ms: **81 % of samples are idle workers**; the main
    thread runs `fused_join_aggregate` with byte-encoded VARCHAR group keys
    (`encode_into`), and its CTE (18 `COALESCE` columns over the fact) is
    materialized whole although it is read once.
- **Numbers:** TPC-H SF 1 geometric mean 1.90× DuckDB (2026-10-30, quiet);
  QuackLab chart p50 already below DuckDB's at one client.

## Decisions

1. **Pipelines, in pinkdb's terms** (DuckDB: `src/parallel/pipeline.cpp`,
   `pipeline_executor.cpp`, `meta_pipeline.cpp`; the source/operator/sink
   interface in `src/execution/physical_operator.cpp`). A new
   `exec/pipeline.rs` compiles a plan subtree into
   `Pipeline { source, operators, sink }`:
   - **sources:** a store table scan (zone pruning, projections, dynamic
     join filters, as today), a streaming table function or lake scan, or
     any subtree the compiler does not handle, evaluated by `eval` (the
     fallback that keeps every existing path working);
   - **streaming operators:** filter (yields a selection and never compacts
     on its own), projection, hash-join probe against a completed build
     (inner first; semi, anti, mark and left after);
   - **sinks (pipeline breakers):** aggregate (the existing `FlatPartial`
     and general `Partial` folds, merged by `merge_and_emit`), collect (in
     morsel order), hash build, sort/top-N, distinct.
   A morsel is one source chunk. Workers take morsels in parallel, push each
   through every operator while it is hot, and fold into a sink state owned
   by that morsel; states merge in morsel order. Row order, first-seen group
   order and accumulation order are therefore exactly today's, and so are
   float sums.
2. **Breakers schedule pipelines, not operators.** A join's build side is
   its own pipeline that completes (and hands its join filters to the probe
   side's scans) before the probe pipeline starts, as today. A probe
   pipeline may pass through several probes (Q5, Q7–Q9, every QuackLab mart
   view) before its sink, so a chain of joins never materializes an
   intermediate join result. The build orientation (`build_on_left`) and the
   probe-row-major output order stay the materialized path's.
3. **The shortcuts fold in.** `selected_chunks`, `probe_through_selection`
   and `fused_join_aggregate` become special cases of a pipeline and are
   deleted once the pipeline covers their shapes with equal or better
   numbers; `fused_scan_aggregate`'s streaming leaves become pipeline
   sources. Nothing is kept twice.
4. **Single-reference CTEs are inlined** (DuckDB
   `src/optimizer/cte_inlining.cpp`): a non-recursive CTE read once becomes
   the subquery it names, so projection pruning and the pipeline reach
   through it. A CTE read several times, or one with side effects, stays
   materialized.
5. **Late materialization (122b).** A probe operator emits probe-side
   selections plus build row ids instead of gathered payload columns; a
   column is gathered only when an operator or the sink reads it (DuckDB's
   `LATE_MATERIALIZATION` optimizer and the join's row-matcher read path).
6. **Dictionary vectors.** A `VectorData` dictionary form (codes plus
   shared values) produced by the `.pink` reader for dictionary-encoded
   VARCHAR chunks and kept through filters, projections and the group-by key
   path (DuckDB `VectorType::DICTIONARY_VECTOR`, `UnifiedVectorFormat`). It
   changes a public type other crates match on, so it takes the full gate.
7. **Kept working:** `ExecLimits`, cancellation and deadlines,
   `QueryProfile`, zone maps and join filters, the correlated per-row paths,
   `RETURNING`, wasm32 (single-threaded: morsels run in order on one
   thread).

## Steps (one pi brief each; TPC-H 22/22 and duckdiff at every commit)

| step | content | measure |
|---|---|---|
| 135a | `exec/pipeline.rs`: scan → filter → project → inner-probe chain → aggregate sink (flat and general partials); replaces `fused_join_aggregate` and `selected_chunks` for the shapes it covers | QuackLab refresh statements, TPC-H Q1, Q3, Q5, Q7–Q10, Q12, Q14, Q18, Q19 |
| 135b | collect sink and non-aggregate roots (sort, top-N, projection over joins); semi/anti/mark/left probes; build sides as pipelines with a parallel hash-build sink; `probe_through_selection` folds in | Q2, Q4, Q11, Q13, Q16, Q20–Q22, QuackLab charts |
| 135c | single-reference CTE inlining | QuackLab refresh |
| 135d | late materialization of join payload columns (122b) | Q9, Q10, Q18, peak RSS |
| 135e | dictionary vectors for low-cardinality strings | Q1, Q12, Q16, QuackLab charts |

## Measurements

Per step, before and after: TPC-H SF 1 as an interleaved A/B against the
previous commit (`pinkdb-wt/tools/tpch-ab.py`, 7 rounds) and against
DuckDB; QuackLab (`bench/quacklab/run.py --size small --storage local`):
mart refresh per scenario, the five slowest refresh statements, chart p50,
peak RSS.

## Tests

Each step: TPC-H checksums at SF 0.01 and SF 1, duckdiff, the correlated
and join fuzzers, the existing operator tests; new tests for determinism
(results and first-seen group order identical with 1 and 8 threads and
across morsel sizes), cancellation and a deadline inside a probe chain, an
error raised in a late morsel (no partial state), the wasm32 build.
Targeted gate for 135a–d; full gate for 135e.
