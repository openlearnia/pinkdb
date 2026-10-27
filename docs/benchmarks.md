# Benchmarks

pinkdb is measured against DuckDB v2.0.0 on the same data, on one quiet
machine, with warm caches and the same worker count. The suite that produces
these tables is committed: `bench/suite/run.py` records the machine, the
commit, the load average and every raw timing, and it refuses to compare two
engines on different data. This page is the reading of the latest run; the
numbers under `bench/suite/results/` are the record.

The suite compares answers as well as times: row counts must match and a
checksum of the normalized rows must match, so a fast wrong answer cannot make
the table.

## The machine

| | |
|---|---|
| CPU | Apple M4, 10 physical / 10 logical cores |
| RAM | 17.18 GB |
| OS | macOS 27.0.1 arm64 |
| rustc | 1.97.1 |
| DuckDB | v2.0.0 `d54cbbe3df` |
| pinkdb | commit `38e2b08` (wave 120) plus the uncommitted wave-111ab tree |
| threads | 8 for both engines (`RAYON_NUM_THREADS=8`, `SET threads=8`) |
| runs | 1 warm-up + 5 timed, median reported |
| load average | 2.5–2.9, recorded per stage in the JSON |

TPC-H data is generated once by DuckDB's built-in `dbgen`, written to Parquet,
and the *same* Parquet files are loaded into a `.pink` store and a DuckDB
database. DuckDB's timer has 1 ms resolution, so its smallest figures are upper
bounds.

## TPC-H

### SF 0.1 (default)

All 22 queries, before and after wave 111ab. "before" is commit 38e2b08
(wave 120) on the same machine, same store, same eight threads; the baseline
run is in `docs/wave-111ab-plan.md` and `/tmp/w111ab/base-sf0.1.log`.

| query | before ms | after ms | DuckDB ms | ratio | rows |
|---|---:|---:|---:|---:|---:|
| 1 | 16 | 16 | 3 | 5.3× | 4 |
| 2 | 20 | 20 | 3 | 6.7× | 44 |
| 3 | 18 | 18 | 2 | 9.0× | 10 |
| 4 | 16 | 16 | 2 | 8.0× | 5 |
| 5 | 31 | 31 | 3 | 10.3× | 5 |
| 6 | 8 | 8 | 1 | 8.0× | 1 |
| 7 | 111 | **31** | 3 | 10.3× | 4 |
| 8 | 22 | **19** | 3 | 6.3× | 2 |
| 9 | 57 | 60 | 6 | 10.0× | 175 |
| 10 | 18 | **16** | 4 | 4.0× | 20 |
| 11 | 17 | **13** | 2 | 6.5× | 2541 |
| 12 | 13 | 16 | 2 | 8.0× | 2 |
| 13 | 40 | 40 | 11 | 3.6× | 37 |
| 14 | 7 | 7 | 2 | 3.5× | 1 |
| 15 | 3 | 3 | 1 | 3.0× | 1 |
| 16 | 8 | **7** | 3 | 2.3× | 2762 |
| 17 | 7 | 8 | 2 | 4.0× | 1 |
| 18 | 74 | **25** | 4 | 6.2× | 5 |
| 19 | 231 | **11** | 3 | 3.7× | 1 |
| 20 | 51 | **42** | 3 | 14.0× | 9 |
| 21 | 425 | **232** | 5 | 46.4× | 47 |
| 22 | 73 | **17** | 3 | 5.7× | 7 |

Geometric mean over all 22 queries: **9.5× before → 6.5× after**. Total
pinkdb time fell 1,266 ms → 681 ms. Three sub-10 ms queries (Q15, Q6, Q14)
divide by a 1–2 ms DuckDB timer and move the mean by several tenths; the
per-query column is the honest comparison.

### SF 1

| query | before ms | after ms | DuckDB ms | ratio | rows |
|---|---:|---:|---:|---:|---:|
| 1 | 170 | **156** | 13 | 12.0× | 4 |
| 2 | 197 | **185** | 3 | 61.7× | 100 |
| 3 | 192 | **181** | 8 | 22.6× | 10 |
| 4 | 167 | **152** | 7 | 21.7× | 5 |
| 5 | 336 | 342 | 9 | 38.0× | 5 |
| 6 | 85 | **78** | 3 | 26.0× | 1 |
| 7 | 1253 | **246** | 9 | 27.3× | 4 |
| 8 | 270 | **216** | 8 | 27.0× | 2 |
| 9 | 715 | **675** | 26 | 26.0× | 175 |
| 10 | 231 | **168** | 16 | 10.5× | 20 |
| 11 | 510 | **120** | 3 | 40.0× | 1048 |
| 12 | 147 | 172 | 9 | 19.1× | 2 |
| 13 | 450 | 453 | 37 | 12.2× | 42 |
| 14 | 71 | 71 | 7 | 10.1× | 1 |
| 15 | 36 | **33** | 5 | 6.6× | 1 |
| 16 | 70 | **58** | 10 | 5.8× | 18314 |
| 17 | 92 | **90** | 7 | 12.9× | 1 |
| 18 | 5515 | **1449** | 22 | 65.9× | 57 |
| 19 | 3855 | **114** | 10 | 11.4× | 1 |
| 20 | 4855 | **606** | 10 | 60.6× | 186 |
| 21 | 10391 | **2252** | 18 | 125.1× | 100 |
| 22 | 680 | **166** | 11 | 15.1× | 7 |

Geometric mean: **38.5× before → 21.8× after** over all 22 queries; total
pinkdb time 30,288 ms → 7,983 ms (3.8×). The four queries that were 82 % of
the old total (Q18–Q21) are now 55 % of a much smaller one. Load average was
2.1 when the baseline run started and 3.4–4.7 for the after run, both recorded
in the JSON.

## Ingest (SF 0.1)

| table | format | engine | ms | M rows/s | MB/s |
|---|---|---|---:|---:|---:|
| lineitem | CSV | pinkdb | 263 | 2.28 | 288 |
| lineitem | CSV | DuckDB | 366 | 1.64 | 207 |
| lineitem | Parquet | pinkdb | 488 | 1.23 | 39 |
| lineitem | Parquet | DuckDB | 104 | 5.77 | 181 |
| orders | CSV | pinkdb | 94 | 1.60 | 182 |
| orders | CSV | DuckDB | 95 | 1.58 | 180 |
| orders | Parquet | pinkdb | 95 | 1.58 | 56 |
| orders | Parquet | DuckDB | 63 | 2.38 | 84 |

pinkdb's CSV reader beats DuckDB's on the 16-column lineitem file; its Parquet
reader is about 4.5× slower because DuckDB decodes larger column chunks in
parallel.

## Durable writes (pinkdb only)

| work | time |
|---|---:|
| 2,000 single-row autocommit inserts, `Sync` | 7.78 s |
| 2,000 inserts, batched (`SyncPolicy::Batch`) | 63 ms |
| 200 selects after each policy | 34–63 ms |
| 3 checkpoints | 40–50 ms |
| CLI single-row autocommit, warm | 119k rows/s |
| CLI batched `VALUES`, warm | 380k rows/s |
| two concurrent writers, optimistic transactions | 0.96× speedup |

There is no DuckDB comparison here: DuckDB has no per-statement WAL with fsync
policies. The single-process `Sync` cost is dominated by fsync, and the
two-writer number is honest about contention: two processes do not go faster
than one.

## Lake

SF 0.1 lineitem (600,572 rows), 8-thread attach, warm object cache:

| catalog | attach ms | attach + scan `sum(l_extendedprice)` ms |
|---|---:|---:|
| `pink:` | 21.5 | 13 |
| `sqlite:` | 2.1 | 14 |
| `pink+s3://` (mock S3, 61 objects) | — | 83 |

The S3 number is a local, in-process mock endpoint with no network latency; it
measures request count and object layout, not the internet. Commit latency is
not separately timed here — wave 110 measured one commit as 3 S3 requests and
2,954 bytes, independent of lake size.

## Search

| workload | result |
|---|---|
| FTS index build, 100k documents × 2 fields | 822 ms |
| FTS top-10 query, same corpus | <1 ms (below the CLI's 1 ms timer resolution; wave 108a's 103 ms was cold) |
| HNSW build, 100k × 768, 1 thread | 126.5 s |
| HNSW build, 100k × 768, 8 threads | 40.5 s |
| HNSW index size | 318,915,668 B |
| recall@10, `ef_search` 16 / 64 / 128 / 256 | 0.535 / 0.878 / 0.961 / 0.991 |
| p50/p95 latency (ms), 8 threads, same ef | 0.57/0.65, 0.89/1.03, 1.20/1.35, 1.57/1.79 |

DuckDB's `fts` and `vss` extensions need `INSTALL`, which this project does not
run without an explicit go-ahead, so there is no cross-engine search comparison
yet. It is a follow-up, not an omission by accident.

## Footprint

| build | bytes |
|---|---:|
| CLI, default features | 5,608,752 (+37,808 since 38e2b08) |
| CLI, `--no-default-features` | 5,450,480 (+37,744 since 38e2b08) |
| CLI, all measured features | 14,842,064 (+37,744 since 38e2b08) |
| server, default features | 5,203,312 |
| CLI + `fts` | 5,612,736 (+175,568) |
| CLI + `vss` | 5,570,128 (+132,960) |
| CLI + `parquet` | 7,459,728 (+2,022,560) |
| CLI + `pinklake` | 8,265,536 (+2,828,368) |
| `libpinkdb_agent` | 2,548,752 |
| `libpinkdb_mcp` | 609,200 |
| `libpinkdb_memory` | 2,746,160 |
| `libstring_ext` | 16,720 |

CLI start-up (open an empty store, quit): 5.2 ms. Peak RSS at SF 0.1: Q1
313 MB, Q9 674 MB, Q10 247 MB — Q9 materializes the largest join output.

## Where pinkdb is slower, and why

**Q21 (125×).** The two `EXISTS`/`NOT EXISTS` subqueries each build the same
duplicate-eliminated key side: a `Distinct(Project(outer copy))` over the whole
enclosing block, computed twice. DuckDB's delim join materializes those keys
once and shares them. The non-equality condition (`l3.l_suppkey <>
l1.l_suppkey`) also still runs as a filter above the join rather than as a
join residual. Both are follow-ups below; wave 111ab cut Q21 from 10.4 s to
2.25 s at SF 1 without addressing either.

**Q18 (66×).** The `IN` subquery's aggregate — `GROUP BY l_orderkey` over 6 M
rows — is the hot path, and its merge phase is serial: each of the eight
per-chunk partial hash tables is merged into one table on one thread. DuckDB's
partitioned aggregate merges in parallel. The subquery memo cut Q18 from 5.5 s
to 1.4 s; what is left is the aggregate.

**Q2/Q5/Q11 (38–62×).** These are scans and joins that are simply 10–40× the
DuckDB time per row: the hash join copies every surviving row into fresh
vectors at each probe instead of writing a selection vector and materializing
once, and the aggregate emits group by group with a value copy per cell.

**Q1/Q6 (12–26×).** Both are scan-bound. pinkdb's filter and aggregate paths
are vectorized, but they lack DuckDB's perfect-hash aggregate and
selection-vector shortcuts; the cost is flat per row and does not change with
scale.

**Q9 (26×).** The cost model picks
`part ⋈ lineitem ⋈ supplier ⋈ nation ⋈ orders ⋈ partsupp`, which joins the
two-key `partsupp` last. The best order measured by hand was 13 % faster, so
the join order is not the whole gap; the row-copying join and the aggregate
are. The wave left Q9 715 → 675 ms at SF 1 and 57 → 60 ms at SF 0.1 (within the
run-to-run spread); Q5's 31 → 31 ms is unchanged.

**Parquet ingest.** DuckDB reads and decodes in larger parallel batches; pinkdb
decodes column chunk by column chunk.

## Correlated subqueries

Wave 120 rewrites a correlated subquery into a join (see
[docs/wave-120-plan.md](wave-120-plan.md)): `EXISTS`/`NOT EXISTS` become a
semi/anti hash join on the correlation keys, `IN`/`NOT IN`/`ANY`/`ALL` a mark
join that computes SQL's three-valued answer, and a correlated scalar
aggregate groups by the correlation keys and left-joins the block, with
`count` coalesced to 0. Q2, Q4, Q17, Q20, Q21 and Q22 therefore answer at all,
and they answer in the same time class as the rest.

Two shape choices were measured rather than assumed, at SF 0.1 on the same
store, one release binary and the same suite:

| shape | Q4 | Q17 | Q20 | Q21 | Q22 |
|---|---:|---:|---:|---:|---:|
| duplicate-eliminated key side for every form | 16 | 7 | 50 | 407 | 57 |
| body's own key columns for `EXISTS`/`NOT EXISTS` | 16 | 39 | 30 | 427 | 66 |

So `EXISTS`/anti joins use the body's own keys (no second scan of the outer
block, which is the asymptotically important half) and scalar aggregates keep
the key side (the aggregate then groups rows the join has already narrowed,
which is what makes Q17 cheap). Both are visible in `EXPLAIN`; nothing is
hidden.

The remaining gap for this family is the duplicate-eliminated key side: it is
a `Distinct(Project(…))` over a copy of the enclosing block, computed once per
subquery, where DuckDB's delim join materialises the outer keys once and shares
them across a query. Q20 shows the cost scaling: 50 ms at SF 0.1 → 1933 ms at
SF 1 for 10× the rows is 38×, while Q4 (16 → 163 ms) and Q17 (7 → 94 ms) are
linear. Q21 also keeps its non-equality condition (`l3_suppkey <> l1_suppkey`)
as a filter above the join instead of a join residual, which is why 407 ms → 7.2 s
is 17.7×. Both are recorded as follow-ups in the plan, not as done work.

For reference, the pre-existing gap is unchanged and unrelated: Q18 (214×) and
Q19 (241×) have no correlated subquery at all — Q18 is the aggregate inside a
plain `IN`, Q19 the nested `OR` lists that defeat the derived filters.

## What changed in wave 111ab

Two roadmap rows in one wave: cost-based join ordering with a build-side
choice (111a) and the execution/subquery paths (111b). No SQL feature was
added; every answer is checked against DuckDB's rows.

| change | measurement |
|---|---|
| cost-based comma-chain order (query graph, DP up to 12 relations, greedy above, FK-aware cardinalities, anchored at the FROM clause's first relation) | Q7 SF 1 1.25 s → 246 ms; Q7 SF 0.1 111 → 32 ms; Q5 SF 0.1 31 → 35 ms is the one regression |
| per-column filters derived through a lone `OR`, with `IN` lists inside branches and union (not intersection) bounds | Q19 SF 1 3.86 s → 112 ms, SF 0.1 231 → 11 ms |
| subquery memo filled by the operator before its chunks fan out, so no lock is ever taken across an evaluation | Q18 SF 1 5.5 s → 1.45 s, Q20 4.9 s → 0.61 s, Q22 680 → 166 ms; the deadlock is gone (30 repro runs, 0 hangs) |
| subquery bodies are left un-reordered (a restore projection hides the from-tree the binder decorrelates by pattern) | Q2 stayed at 21 ms instead of planning a 2·10¹³-row cross product |
| scanner/suite: timeout payloads are decoded before concatenation | the suite no longer crashes when a query times out |

SF 1 geometric mean **38.5× → 21.8×**, total time 30.3 s → 8.0 s. SF 0.1
geometric mean **9.5× → 6.5×**, total 1,266 → 681 ms. Binary size +37.8 kB
(+0.68 %) for the default CLI, no new dependencies.

## What changed in wave 111

| change | measurement |
|---|---|
| comma joins become hash joins (Filter over a condition-less cross join) | Q9 SF 1 100.9 s → 0.73 s; Q8 11.7 s → 0.33 s |
| join reordering for disconnected comma chains (greedy equality graph, restore projection) | Q9 0.73 s → 0.66 s; Q8 0.33 s → 0.29 s |
| per-table filters derived from an `OR` of conjunctions | Q7 SF 0.1 219 → 114 ms; SF 1 2.35 → 1.20 s |
| `IN` set capacity reserved from the subquery row count | 8.8 s → 5.8 s on the probe query |
| subquery memoization (single-flight) | Q18 SF 0.1 36 ms, but deadlocked once at SF 1; **reverted**, follow-up |
| pink-lake corruption test cleans its scratch dirs | no benchmark; removes ~1,500 leaked `$TMPDIR` directories |

SF 1 geometric mean over the same 15 queries: **33.1× before → 32.0×
after** (29.0× over all 16). At SF 0.1 the geomean moved 7.4× → 8.6×: Q7
halved and Q9 fell 451 → 60 ms, but reverting the memo left Q18 at 76 ms
(36 with it), and the sub-5 ms queries swing a ratio by a whole unit.

## Reproduce

```sh
# default: TPC-H SF 0.1, every family, about ten minutes
bench/suite/run.sh

# one family at SF 1 (about 1 GB per engine; run it on a quiet machine)
bench/suite/run.sh --only tpch --scale 1

# the CI guard: 60k rows, no DuckDB, compares against bench/suite/baseline-ci.json
python3 bench/suite/run.py --only ci
```

`--only` takes a comma-separated subset of
`tpch,ingest,durable,lake,search,footprint,ci`; `--work-dir` keeps the
generated stores between runs and `--skip-build` reuses the release binary.
Nothing in the suite downloads data or installs a DuckDB extension.

## Gaps and follow-ups

- **Delim join for decorrelated subqueries** (Q21's shared key side) and the
  Q21 non-equality condition as a join residual. Q21 is the largest single
  query left: 2.25 s at SF 1, 125× DuckDB.
- **Parallel aggregate merge.** `merge_partials` merges every chunk's partial
  table on one thread; Q18's remaining 1.4 s is mostly this, and Q1/Q9/Q13 pay
  a share of it.
- **Scatter-then-gather join output** (selection vectors instead of copying
  rows) for Q2/Q5/Q11 and the join-heavy scans.
- **Perfect-hash aggregation and selection-vector filters** for Q1/Q6.
- **Q9's join order**: the estimator joins the two-key `partsupp` last. A
  finer model (or a rule that joins the table with the most equality edges
  first) would recover the 13 % measured by hand.
- **Ingest**: Parquet decode in larger parallel batches.
- A DuckDB `fts`/`vss` comparison, once `INSTALL` is allowed.
- The CI guard is non-blocking until it has a stable history on GitHub's
  shared runners.
