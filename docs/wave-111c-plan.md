# Wave 111c — the join- and subquery-bound TPC-H outliers

Rows 111c (delim join and join residuals) and 111b1 part (1) (a parallel
`merge_partials`). **Performance only: no query result may change.**

Targets at SF 1 after wave 123a (DuckDB in brackets): Q21 2,371 ms (18),
Q18 1,500 ms (22), Q20 609 ms (10), Q2 173 ms (43×) and Q11 128 ms (43×).
Q20 also scales superlinearly with SF (38.7× for 10× the rows, wave 120).

Measured baseline for this wave (`bench/suite --only tpch --scale 1 --runs 3`,
10-core machine, quiet): q18 1437 ms, q20 619, q21 2255, q2 183, q11 129.
Statement splits (`pinkdb -no-agent`, `.timer on`) put essentially all of the
time in execution; parse + bind + optimize are 0.3–0.5 ms together.

## What the baseline plans do (measured, not guessed)

### Q21 — two duplicated outer planes, one residual filter

`EXPLAIN` (SF 1) shows the 4-way outer chain — supplier ⋈ lineitem l1 ⋈
orders ⋈ nation, 36 columns — cloned **three times**, once per subquery, each
under a `Distinct(Project[__key0,__key1])` over the full 36-column projection.
The EXISTS body is a `Filter (#2 <> #17)` above `Join inner on [#0=#0]`
(body lineitem ⋈ key side on orderkey only); the semi join on top then probes
both key columns.

Attribution, running the pieces separately (`/tmp/q21parts.sql`):

| plan piece | execute |
| --- | ---: |
| outer chain alone (`count(*)`) | 222.6 ms |
| + EXISTS | 589.8 ms |
| + NOT EXISTS (the whole query) | 2170.3 ms |

The asymmetry is the nesting: the anti join's key side is a clone of the
*semi* subtree, so `NOT EXISTS` recomputes the outer chain *and* the EXISTS
semi join, plus another 36-column distinct.

Cause: `Binder::dependent_join` only takes the cheap *plain* shape (body keys
probed directly, no key side) when `residual.is_empty()`. A non-equality
correlation (`l2.l_suppkey <> l1.l_suppkey`) is not a key, so every such
subquery falls back to `body ⨯ Distinct(Project(outer))`, `Filter` above the
inner join. The join has no place to put the residual, so it is evaluated on
fully materialized 36-column rows.

### Q18 — the serial merge in `merge_partials`

The whole query cost is the `IN` subquery aggregate
`SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300`:
1350 mg. `GROUP BY l_returnflag` (3 groups) over the same 6M rows is 10.7 ms,
so accumulation is not the cost — group count is. Temporary timing in
`aggregate()` (`PINKDB_AGG_PROFILE`, removed before finishing):

```
agg: chunks=2931 phase1=20.6ms merge=1283.8ms groups=1500000
```

Phase 1 (per-chunk partials, already parallel) is 20 ms; the serial merge of
~6M partial entries is 1.28 s, i.e. 95 % of Q18. Each 2048-row chunk holds
~2048 distinct `l_orderkey` (an order's 4 lines scatter across chunks), so
the merge probes a `FastMap<Vec<u8>, usize>` reserved to 6M slots (~144 MB)
6M times — every probe a cache miss — and each new group allocates its
`Vec<u8>` key, `Vec<Value>` keys and `Vec<Acc>` accumulators.

### Q20 — the key side is built from an unfiltered scan

```
Join left on [#0=#0, #1=#1]                 -- partsupp (800k) ⋈ agg
  Filter (#0 IN (forest parts))             -- 800k → 11k rows, 13.9 ms
  Aggregate keys=[#16, #17] aggs=[sum(#4)]
    Join inner on [#1=#0, #2=#1]            -- 1.2M date-filtered lineitem rows
      Distinct(Project[__key0,__key1](Scan partsupp))   -- 800k keys
```

Subquery alone 682.7 ms. The correlation key side is `Distinct(Project)` over
the *whole* `partsupp` scan, because `dependent_join` drops enclosing
conjuncts that contain a subquery (`carried`: "it would run twice"), so the
enclosing `ps_partkey IN (…)` never reaches the key side. The 1.2M-row probe
side then walks an 800k-entry match table instead of an 11k-entry one.

### Q2 and Q11

Q2 folds five outer joins into `Filter (#19 = #29)` above
`Join left on [#0=#0]` (33 columns), plus a second clone of that chain under
`Distinct(Project[__key0])` feeding `min(ps_supplycost)`. Q11 is a plain
`partsupp ⋈ supplier ⋈ nation` aggregate with an uncorrelated HAVING
subquery — no delim shape, no residual. Both are investigated for cheap wins
after the three above; the rest is written up as follow-ups.

## Design

### 1. Join residuals (`LogicalPlan::Join.residual`)

`LogicalPlan::Join` gains `residual: Option<BoundExpr>`, evaluated in joined
space (`left ++ right`) on candidate key matches only. The executor tests it
per candidate pair, before materializing anything: a pair failing the residual
is not a match. Semantics per kind:

- `Inner`: the pair is dropped;
- `Semi`: the left row is dropped unless some candidate pair passes;
- `Anti`: the left row is kept unless some candidate pair passes;
- outer joins do not take one, and neither does `Mark`: its NULL bookkeeping
  counts the matching rows' key values per right row, which a residual would
  have to narrow too, and the binder never gives it one (an `IN` subquery keeps
  extra conjuncts in the keyed shape). Both are rejected at execution with an
  internal error rather than silently answered wrong.

DuckDB's `physical_hash_join.cpp` (`InitializeResidualPredicate`,
`MapResidualBuildColumns`) gathers only the residual's columns for the
candidate pairs into a dense chunk and evaluates the predicate vectorize.
We do the same: probe pass collects `(probe row, build row)` candidate pairs,
then one gather of the residual's referenced columns from both sides into a
small chunk (columns renumbered to dense slots), one `eval_expr`, and the
emit pass walks the surviving pairs. The no-residual path is unchanged, so
nothing pays for the feature.

Binder: `dependent_join` returns the *plain* shape even with a residual, with
the residual rewritten into joined space (outer columns keep their enclosing
row index, body columns shift by the left width). `decorrelate_predicate`
puts it in the semi/anti join. Q21 then reads: `Anti(residual
l3.l_suppkey <> l1.l_suppkey, keys orderkey)` over `Semi(residual
l2.l_suppkey <> l1.l_suppkey, keys orderkey)` over the outer chain **once**.

This turned out to remove Q21's delim shape rather than share it: with the
residual on the join, an `EXISTS` keeps the plain shape, so the outer chain
appears once and nothing needs a duplicate-eliminated key side. **Follow-up
(delim join proper)**: the keyed shape is still one `Distinct(Project(outer))`
per correlated *scalar* or `IN` subquery that needs one, and two such
subqueries over the same outer key still build it twice. A shared materialized
key side (DuckDB's `PhysicalDelimJoin` + `CLogicalDelimGet` per statement)
is the fix; no TPC-H query at SF 1 needs it after this wave, so it stays a
follow-up with its design recorded here and in ROADMAP 111c.

### 2. Parallel `merge_partials` (111b1 part 1)

Wave 123a's fix is incomplete: phase 1 is parallel, the merge is not. Radix
partition the partial entries by key hash (DuckDB's
`radix_partitioned_hashtable.cpp`), merge partitions in parallel, then restore
the deterministic first-seen group order:

1. `buckets = (entries / 4096).next_power_of_two().clamp(1, 64)`.
2. Scatter `(partial, local)` index pairs per bucket, in parallel per partial
   and concatenated in partial order, so each bucket stays in first-seen order.
3. Merge each bucket in parallel into its own `FastMap<Vec<u8>, usize>`; the
   partition's table is small enough to stay in cache. The first visit of a key
   in a bucket is its global first visit, so the bucket records the entry's
   `(partial << 32) | local` order key with each merged group.
4. Sort the merged groups' order keys (one `u32` permutation sort) and apply
   the permutation to `Vec<Group>`; result order is identical to the serial
   merge.
5. `buckets == 1` (small aggregates, under 16k groups) keeps today's serial
   loop verbatim.

The bucket function is the one place this can silently fail: masking the *low*
bits of an FNV hash put essentially every key in one partition, and the whole
merge stayed at 1.46 s flat across 1/4/16/64 buckets. FNV propagates upward
only, so the low bits barely mix; a Fibonacci multiply followed by a shift of
the *top* bits partitions evenly (merge 56 ms at 64 buckets).

### 3. Q20: carry the enclosing filter into the key side

Allow a *correlation-free-subquery* conjunct into `carried`, so the
`Distinct(Project(partsupp))` keys come from the 11k filtered rows instead of
800k. Only an impure conjunct (`contains_dynamic_scalar`) is dropped now; an
uncorrelated subquery is worth the second evaluation because it only filters
rows the key side reads anyway, and it is safe because every `decorrelate_clause`
caller is a WHERE/HAVING/QUALIFY filter above the plan, so every output row
already satisfies the conjunct. Measured: the second evaluation costs 13.9 ms
and the whole query 604 → 100 ms at SF 1.

## Tests

- `crates/pinkdb/tests/correlated_fuzz.rs` (wave 120, vs DuckDB) stays green
  and its grammar gains non-equality correlation inside `EXISTS`/`NOT EXISTS`
  and several subqueries sharing one outer key.
- A new differential test for residual joins: random inner/semi/anti/mark
  joins with a residual, NULLs and duplicates, against a nested-loop
  reference and DuckDB.
- `merge_partials`: results (and row order) unchanged with many duplicate keys
  across chunks, NULL keys, every aggregate kind, `DISTINCT` aggregates and
  `FILTER (WHERE …)`, compared with the serial path.
- `bench/duckdiff.py`, the TPC-H 22-query correctness check and the 122/123a
  suites stay green.

## Measurement

`bench/suite --only tpch --runs 3 --skip-build` at SF 0.1 and SF 1, before and
after, on the same stores back to back. The "before" binary is `HEAD` built
into a scratch target dir (`/tmp/w111c-base-tree`, 15,009,008 bytes — the same
size the wave-123a record has), swapped into `target/release/pinkdb` for the
baseline runs and restored afterwards, so both sides ran the same runner, the
same data and the same machine. Load average 2.3–3.0 both ways. `sample`
profiles of Q21 and Q18 before and after. The full tables are in
`docs/benchmarks.md#wave-111c--join-residuals-and-a-parallel-aggregate-merge`.

| change | SF 1 | SF 0.1 |
| --- | --- | --- |
| join residual (Q21) | 2,333 → 1,352 ms | 232 → 114 ms |
| parallel merge (Q18) | 1,390 → 251 ms | 25 → 20 ms |
| key-side filter carry (Q20) | 604 → 100 ms | 40 → 10 ms |
| four-leaf join order (Q11) | 129 → 26 ms | 13 → 5 ms |
| **all 22, geometric mean** | **166.5 → 128.1 ms (1.30×)** | **14.6 → 12.4 ms (1.18×)** |

## What shipped, and what did not

Shipped: the residual on the join (binder + executor), the plain `EXISTS`
shape for a residual correlation, the parallel/radix `merge_partials`, the
key-side filter carry, and the four-leaf join-order start.

Not shipped, recorded as follow-ups:

- **A shared key side across subqueries** (real delim join). Q21 no longer has
  a delim shape at all and Q20's is now linear, so nothing in TPC-H needs it.
- **Q2 (44×)**: the subquery body is ordered before decorrelation (`order_chains`
  refuses a chain whose predicate reads an outer column) and its projection
  copies 19 columns to use one. Reordering after decorrelation needs a pass
  over `Join` trees or a statement-level recursion into `BoundExpr` subquery
  plans; `fold_expr` has no subquery arms, so this is a real (if small) piece
  of work, not a switch.
- **Scan-level bloom pushdown** for a join key: wave 122's dynamic filter only
  skips whole chunks by zone bounds, so Q21's 6 M-row `lineitem` scans pay the
  probe without pruning. Two experiments that pushed a build-side bloom at the
  *join* (filtering either the built or the probed rows) were measured 10–20 %
  **slower** on Q21 and reverted: at 6 M rows the per-row bloom test costs what
  the match-table insert costs.
- **Perf work for phase 1 of aggregation** (one partial table per thread
  instead of per chunk), and the rest of 111b1 (2)/(3).
