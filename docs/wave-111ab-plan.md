# Wave 111ab — join ordering, subquery paths, execution throughput

Status: shipped. This file is the design record and the working log. It was
written before the first code change and is kept current as each item landed;
numbers in "Baseline" and in each item's "Result" come from runs on this
machine, never from memory. The review found one blocker — the first subquery
memo deadlocked under parallel evaluation — and "The deadlock" below records
the fix and its measurements.

## Goal

Two ROADMAP rows, one wave:

- **111a** — cost-based join ordering and hash build side.
- **111b** — execution throughput (scan/filter/aggregate), the Q18/Q11
  subquery paths, Q19's nested `OR`s, and ingest.

No new SQL features. Correctness must not change: every step is checked with
the TPC-H answer checks (row count + normalized checksum), the pinned tests in
`crates/pinkdb/tests/`, and `bench/duckdiff.py`.

## Baseline (before)

HEAD `38e2b08` (wave 120), release build of the suite's feature set, 8 worker
threads. The machine was otherwise quiet (`pgrep -x cargo` empty; suite's own
`loads` records start 2.1 / 2.3 for SF 1 / SF 0.1 and the per-query values in
the JSON).

### SF 0.1 — all 22 queries

| query | pinkdb ms | DuckDB ms | ratio | rows |
|---|---:|---:|---:|---:|
| 1 | 16 | 3 | 5.33× | 4 |
| 2 | 20 | 3 | 6.67× | 44 |
| 3 | 18 | 2 | 9.00× | 10 |
| 4 | 16 | 2 | 8.00× | 5 |
| 5 | 31 | 3 | 10.33× | 5 |
| 6 | 8 | 1 | 8.00× | 1 |
| 7 | 111 | 3 | 37.00× | 4 |
| 8 | 22 | 3 | 7.33× | 2 |
| 9 | 57 | 6 | 9.50× | 175 |
| 10 | 18 | 4 | 4.50× | 20 |
| 11 | 17 | 2 | 8.50× | 2541 |
| 12 | 13 | 2 | 6.50× | 2 |
| 13 | 40 | 11 | 3.64× | 37 |
| 14 | 7 | 2 | 3.50× | 1 |
| 15 | 3 | 1 | 3.00× | 1 |
| 16 | 8 | 3 | 2.67× | 2762 |
| 17 | 7 | 2 | 3.50× | 1 |
| 18 | 74 | 4 | 18.50× | 5 |
| 19 | 231 | 3 | 77.00× | 1 |
| 20 | 51 | 3 | 17.00× | 9 |
| 21 | 425 | 4 | 106.25× | 47 |
| 22 | 73 | 3 | 24.33× | 7 |

Geometric mean over 22 queries: **9.47×**; total 1266 ms vs 70 ms.

### SF 1 — all 22 queries

| query | pinkdb ms | DuckDB ms | ratio | rows |
|---|---:|---:|---:|---:|
| 1 | 170 | 14 | 12.14× | 4 |
| 2 | 197 | 4 | 49.25× | 100 |
| 3 | 192 | 8 | 24.00× | 10 |
| 4 | 167 | 8 | 20.88× | 5 |
| 5 | 336 | 9 | 37.33× | 5 |
| 6 | 85 | 3 | 28.33× | 1 |
| 7 | 1253 | 9 | 139.22× | 4 |
| 8 | 270 | 9 | 30.00× | 2 |
| 9 | 715 | 28 | 25.54× | 175 |
| 10 | 231 | 17 | 13.59× | 20 |
| 11 | 510 | 3 | 170.00× | 1048 |
| 12 | 147 | 9 | 16.33× | 2 |
| 13 | 450 | 40 | 11.25× | 42 |
| 14 | 71 | 8 | 8.88× | 1 |
| 15 | 36 | 5 | 7.20× | 1 |
| 16 | 70 | 10 | 7.00× | 18314 |
| 17 | 92 | 8 | 11.50× | 1 |
| 18 | 5515 | 23 | 239.78× | 57 |
| 19 | 3855 | 11 | 350.45× | 1 |
| 20 | 4855 | 11 | 441.36× | 186 |
| 21 | 10391 | 17 | 611.24× | 100 |
| 22 | 680 | 15 | 45.33× | 7 |

Geometric mean: **38.5×**; total 30,288 ms vs 269 ms. Q18+Q19+Q20+Q21 are 82 %
of pinkdb's total.

The committed `docs/benchmarks.md` SF 1 table is from a quieter wave-120 run
(Q19 2415 ms, Q20 1933 ms, Q21 7222 ms). Both runs are the same binary; the
difference is machine noise on the two longest queries. This wave compares
against the numbers above because they were taken in one session.

Non-TPC-H families, same run: ingest lineitem CSV 3464 ms / Parquet 6188 ms
(DuckDB 3620 / 1705), orders Parquet 786 ms (DuckDB 512); durable Sync insert
8116 ms, Batch 60 ms; lake `pink:` scan 123 ms, `sqlite:` 123 ms, mock-S3
194 ms; FTS build 834 ms, HNSW 1-thread build 111.8 s.

## Where the time goes

Measured on the SF 1 store with the release CLI:

| shape | ms | note |
|---|---:|---|
| `GROUP BY l_orderkey` over lineitem (1.5M groups) | 1370 | 6M rows → 4.4M rows/s |
| the same with `HAVING sum(l_quantity) > 300` | 1356 | filter is not the cost |
| `GROUP BY l_returnflag, l_linestatus` (4 groups) | 19 | low-cardinality is fine |
| `count(*) FROM orders WHERE o_orderkey IN (subquery)` | 5509 | 1360 for the subquery, 4149 for the filter |
| the same with `IN (1,2,3)` | 18 | probe itself is fast |
| Q19 (lineitem ⋈ part, one 3-branch `OR`) | 3855 | no per-column filters reach either scan |
| Q21 | 10391 | four tables + semi/anti over lineitem |

The 4149 ms filter overhead was a `MutexGuard` held across the whole `probe_in`
(see 111b.2). The 1370 ms aggregate is a per-row key encode + hash insert with
no partitioning; DuckDB radix-partitions and does the same shape in ~100 ms.

## Plan, in measured-payoff order

1. **Uncorrelated subquery memo (111b.2).** Release the memo lock before
   probing an `IN` set, and add a per-plan-node once-cell so parallel chunks
   cannot each evaluate the same subquery. Measured: Q18 filter 4149 → ~0.
2. **Q19's nested `OR`s (111b.3).** Extend `derive_common_filters`: read an
   `OR`-of-`Eq` atom as a value set, union value sets across DNF terms, take
   the *union* of ranges (min lower / max upper — the current code takes the
   intersection, which is only correct for an intersection), and run it for a
   lone `OR` predicate. Measured target: Q19 3855 → under 200.
3. **Cost-based join order (111a).** Replace `comma_join_order`'s greedy walk
   with a Selinger-style DP over connected subsets (≤ 12 relations, greedy
   above), cardinality = base rows × filter selectivity / product of join
   denominators, cost = sum of intermediate cardinalities (DuckDB's
   `cost_model.cpp`). Smaller side builds at runtime; the restore projection
   keeps the output order. References: `src/optimizer/join_order/
   {query_graph_manager,cardinality_estimator,cost_model,plan_enumerator,
   build_probe_side_optimizer}.cpp` (MIT, read only).
4. **Delim join (wave-120 follow-up).** Share the `Distinct(Project(outer))`
   key side of an `IN`/`EXISTS` decorrelation across the outer plan, after the
   shape of `physical_delim_join.cpp` / `physical_left_delim_join.cpp` /
   `optimizer/deliminator.cpp`.
5. **Q21 join residual.** Put `l2.l_suppkey <> l1.l_suppkey` in the semi/anti
   join as a residual instead of a `Filter` above the join.
6. **Aggregate throughput (111b.1).** Profile the 1.5M-group hash aggregate;
   candidates: partition by key hash across threads, avoid the per-row key
   `Vec<u8>` allocation, perfect hash for ≤ N groups.
7. **Ingest (111b.4).** Parquet load is 3.6× DuckDB's; profile the decoder.

Each item: before/after from the suite, a pinned-result test (plus DuckDB
cross-check where the CLI is present), `bench/duckdiff.py` and the 22-query
correctness check green.

## What the cost model assumes (recorded here)

No histograms. Per-column distinct counts are the relation's row count, which
is DuckDB's no-statistics fallback (`UpdateTotalDomains` in
`cardinality_estimator.cpp`):

- base cardinality: the scan's row count (`db.table_in` / attached / values),
  seen through Filter/Project/Distinct/Sort/Limit;
- filter selectivity: equality `0.2` (`relation_statistics_helper.hpp:32`
  `DEFAULT_SELECTIVITY`), an `IN` list `0.2 × values` capped at 1, anything
  else (ranges included) `0.5`. `AND` multiplies; an `OR` is not modelled —
  the derived per-column facts are what reach the scan and what the estimate
  sees;
- equality classes: columns joined by equalities form one class (union-find
  over the predicate's column ids). The class domain is the **smallest base
  row count** among the class's relations: that is the dimension side of a
  foreign-key join, and taking it makes `customer ⋈ orders` (150k) cost less
  than `customer ⋈ supplier` on `nationkey` (600k), which is the ordering
  mistake that cost 315 ms on Q5;
- cardinality of a connected subset = ∏ filtered rows / ∏ class domains of
  the edges inside the subset; a disconnected subset is a real cross product
  and is never chosen (`ChainStats::links`);
- cost of a join = its output cardinality + the costs of its inputs
  (`cost_model.cpp` `ComputeCost`), and the DP is anchored at the FROM
  clause's first relation (the one a query filters; starting from a small
  dimension fans out through a many-to-many key and leaves the fact table
  last — Q5 region-first 315 ms against 49);
- a reorder is applied only when the estimate improves by 10 %
  (`REORDER_GAIN`), which also makes the pass terminate: a reordered chain is
  already at the optimum and does not reorder again.

Build side is not chosen from these estimates. The executor's `build_on_left`
already compares the two inputs' exact row counts for inner joins, which is
strictly better information than the plan-time estimate; what it lacks is an
estimate for a filtered or joined input, recorded as a follow-up.

## The deadlock in the first memo (found in review, fixed)

The first memo was single-flight: a per-key `Mutex` was held **across** `f()`,
the evaluation of the subquery body. The body runs its own plan on the same
rayon pool, so a worker that waited on that mutex could be holding the pool job
the evaluator was waiting for — `sample` showed nine workers in
`WorkerThread::wait_until_cold` and one in `__psynch_mutexwait` inside
`eval_in_subquery`. Making a filter predicate's chunks serial hid it only for
filters; a subquery in a select list, `CASE`, aggregate argument or join
condition still hung (about one run in six on a 3 M-row table).

The fix removes the wait instead of narrowing it. Every operator that owns an
expression with a subquery calls `expr::prime` **before** its chunks fan out:
`Filter`, `Project`, `Join` keys, `NestedLoopJoin`'s `on`, `Aggregate` keys and
every argument/`FILTER`, `Sort` keys and `Window` arguments. `prime` walks the
expression, and for a `ScalarSubquery`/`Exists`/`InSubquery` whose `outer` bind
is empty (a correlated body depends on the current row and is never memoised)
evaluates the body once and inserts the result under the body plan's key;
`in_set` takes the left operand's *type* rather than a `Vector` so priming can
build the set without a chunk. `Cx::memoized` then only reads; its miss path
computes and inserts without waiting, so a duplicate evaluation is possible in
principle but a wait is not. The memo never holds its map's lock across an
evaluation, and the serial-chunk workaround is gone.

Measured: 30 runs of the reviewer's five-shape repro over 3 M rows, 0 hangs,
0.24 s per run (the same file took 0.4 s and hung about one run in six before);
the values match DuckDB (428572, 428572, 429 per group, 5999996, 214287).
`crates/pinkdb/tests/subquery_parallel.rs` repeats the five shapes 30 times
each over 1 M rows with a 20 s no-progress watchdog.

## Results

Before: commit 38e2b08, this machine, 8 threads. After: the same stores and
binary with this wave's changes. Both runs are `bench/suite/run.py --only tpch
--skip-build`.

| | SF 0.1 | SF 1 |
|---|---:|---:|
| geometric mean, before | 9.5× | 38.5× |
| geometric mean, after | **6.5×** | **21.8×** |
| pinkdb total, before | 1,266 ms | 30,288 ms |
| pinkdb total, after | **681 ms** | **7,983 ms** |
| worst query, before | Q21 425 ms | Q21 10,391 ms |
| worst query, after | Q21 232 ms | Q21 2,252 ms |
| binary, default CLI | 5,570,944 → 5,608,752 B | |

Per-query tables are in [docs/benchmarks.md](benchmarks.md).

| change | measured |
|---|---|
| subquery memo: lock released before probing, then primed before the chunks fan out | Q18 SF 1 5,515 → 1,449 ms; Q20 4,855 → 606 ms; Q22 680 → 166 ms; deadlock gone |
| `OR`-derived filters (`eq_set`, `false` term dropped, union bounds, lone `OR`) | Q19 SF 1 3,855 → 112 ms; SF 0.1 231 → 11 ms |
| cost-based comma-chain order (DP ≤ 12, greedy above, anchored, 10 % gate) | Q7 SF 1 1,253 → 246 ms; SF 0.1 111 → 32 ms |
| subquery bodies left un-reordered (`has_outer_ref`) | Q2 stayed 21 ms instead of a 2·10¹³-row cross |
| decorrelation left join: reorder the left input (`reorder_left_spine`) | Q2 52 → 23 ms |
| suite: decode a timeout payload before concatenating it | suite no longer crashes on a timeout |

Regressions, kept and recorded: Q5 SF 0.1 31 → 35 ms, Q9 SF 0.1 57 → 61 ms
(Q9's chosen order joins the two-key `partsupp` last; the best hand order was
13 % faster). Q1/Q6 are unchanged — the aggregate and scan work (111b.1) was
not attempted.

## Log

- Baseline captured at SF 0.1 and SF 1 (tables above); the SF 1 baseline ran
  at load 2.1–11 and its JSON is `/tmp/w111ab/base-sf1.json`.
- 111b.2: memo lock diagnosed with `sample` (110/122 samples in
  `merge_partials`, all workers waiting on the memo `MutexGuard` held across
  `probe_in`). Memo now stores `Arc<FastSet>`; `Cx::memoized` is single-flight
  with a per-key flight lock. That exposed a second hang — a worker taking the
  flight lock then running the body's own parallel plan starved the pool
  (Q20 hung 2 of 5 runs) — fixed by processing a chunk serially when it
  evaluates a subquery. Q18 SF 1 5,515 → 1,517 ms.
- 111b.3: `derive_common_filters` rewritten. First version overflowed the
  stack: the derived facts were pushed into the join's children while the lone
  `OR` stayed above it, so re-entry derived them again. `push_filter`'s
  `derive` flag handles that. Range union: within a branch keep the tighter
  bound, across branches the looser one (the old always-tighter rule dropped
  rows; verified against DuckDB).
- 111a: first DP without anchoring put `nation` first on Q9 (531 ms) and
  `region` first on Q5; anchoring to the FROM clause's first leaf and the
  FK-aware class domains brought Q9 to 61 ms and Q5 to 35 ms. A first attempt
  also inserted the restore projection into a subquery body, which the binder
  decorrelates by pattern-matching the body's from-tree — Q2 planned a cross
  product. `has_outer_ref` leaves those bodies alone.
- Tests: `comma_join` 5 (2 new), optimizer units 15 (2 new), workspace gates
  green; all 22 TPC-H queries match DuckDB's rows at SF 0.01 and SF 0.1.
- The first hang fix (serial chunks under a subquery) was replaced in review:
  priming the memo before the chunks fan out removes the wait entirely, so the
  chunks stay parallel (see "The deadlock"). The CI guard's `corr_not_in`
  shape went back to 1 ms.
- Not attempted, recorded as ROADMAP rows 111b1, 111c, 111b4: parallel
  aggregate merge (Q18's remaining 1.4 s), delim join and the Q21 residual,
  selection-vector join output, perfect-hash aggregation, Parquet decode.
- Review round: the single-flight memo deadlocked when the subquery was not in
  a filter (a worker holding the flight lock ran the body's parallel plan and
  starved the pool). Replaced the wait with priming (`expr::prime` at each
  operator) and removed the serial-chunk workaround; `sample` and the 30-run
  repro are in "The deadlock" above. SF 1 after: Q18 1,449 ms, Q20 606 ms,
  Q22 166 ms, Q11 120 ms; TPC-H geomean 21.8× (from 22.9× before the fix, so
  priming costs nothing measurable).
- Post-fix measurements (final): SF 1 `--only tpch` at load 3.44/3.34/2.92
  (log `/tmp/w111ab/fix-sf1b.log`), SF 0.1 at load 3.02/3.49/3.05
  (`/tmp/w111ab/fix-sf0.1.log`); the tables in "Results" are those runs. Q11
  re-checked with the CLI: 112–114 ms over five runs (the suite's 120 ms is its
  own timing harness), i.e. unchanged from the pre-fix 111 ms.
- Gates after the fix: `cargo fmt --all -- --check` clean; `cargo clippy
  --all-targets -- -D warnings` clean; `cargo test -q --workspace
  --no-fail-fast` 2,257 passed / 0 failed / 14 ignored (one extra test, the new
  `subquery_parallel`); `python3 bench/duckdiff.py` 432 passed / 0 failed with
  the `arrow,parquet,httpfs` build (404 with the suite build, 2 batteries
  skipped); TPC-H correctness 22/22 status ok at SF 0.01; the repro loop 30
  runs / 0 hangs; and the CI family's four query medians within tolerance
  (q1 1.0, q6 1.0, corr_scalar 4.0, corr_exists 2.0, corr_not_in 4.0 ms). The
  wasm32 build is green. `bench/suite/baseline-ci.json` was left alone: its
  `binary_bytes` was recorded by CI's own build, so a local size comparison is
  not meaningful, and no query moved.
