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
| pinkdb | commit `2c7ee92` plus the uncommitted wave-111 tree |
| threads | 8 for both engines (`RAYON_NUM_THREADS=8`, `SET threads=8`) |
| runs | 1 warm-up + 5 timed, median reported |
| load average | 2.5–2.9, recorded per stage in the JSON |

TPC-H data is generated once by DuckDB's built-in `dbgen`, written to Parquet,
and the *same* Parquet files are loaded into a `.pink` store and a DuckDB
database. DuckDB's timer has 1 ms resolution, so its smallest figures are upper
bounds.

## TPC-H

### SF 0.1 (default)

| query | pinkdb ms | DuckDB ms | ratio | rows |
|---|---:|---:|---:|---:|
| 1 | 18 | 3 | 6.0× | 4 |
| 3 | 19 | 2 | 9.5× | 10 |
| 5 | 31 | 3 | 10.3× | 5 |
| 6 | 8 | 1 | 8.0× | 1 |
| 7 | 114 | 3 | 38.0× | 4 |
| 8 | 24 | 3 | 8.0× | 2 |
| 9 | 60 | 6 | 10.0× | 175 |
| 10 | 18 | 4 | 4.5× | 20 |
| 11 | 19 | 2 | 9.5× | 2541 |
| 12 | 14 | 2 | 7.0× | 2 |
| 13 | 42 | 11 | 3.8× | 37 |
| 14 | 7 | 2 | 3.5× | 1 |
| 15 | 3 | 1 | 3.0× | 1 |
| 16 | 8 | 3 | 2.7× | 2762 |
| 18 | 76 | 5 | 15.2× | 5 |
| 19 | 236 | 2 | 118.0× | 1 |

Geometric mean over the 16 supported queries: **8.6×** slower than DuckDB.

### SF 1

| query | pinkdb ms | DuckDB ms | ratio |
|---|---:|---:|---:|
| 1 | 156 | 13 | 12.0× |
| 3 | 181 | 8 | 22.6× |
| 5 | 321 | 8 | 40.1× |
| 6 | 79 | 3 | 26.3× |
| 7 | 1199 | 9 | 133.2× |
| 8 | 290 | 9 | 32.2× |
| 9 | 659 | 26 | 25.3× |
| 10 | 198 | 16 | 12.4× |
| 11 | 486 | 3 | 162.0× |
| 12 | 141 | 9 | 15.7× |
| 13 | 439 | 38 | 11.6× |
| 14 | 69 | 7 | 9.9× |
| 15 | 34 | 5 | 6.8× |
| 16 | 66 | 10 | 6.6× |
| 18 | 4960 | 25 | 198.4× |
| 19 | 2457 | 10 | 245.7× |

Geometric mean: **32.0×** slower over the 15 queries measured before tuning
(33.1× before), or **29.0×** over all 16 — Q16 was not comparable in the
earlier run because the CLI's 10,000-row display cap cut its output, which the
suite's correctness check now bypasses. The same run's stores are 321 MB
(pinkdb) and 298 MB (DuckDB), built in 9.3 s and 1.3 s respectively.

TPC-H queries 2, 4, 17, 20, 21 and 22 are absent: they reference an outer
column from a subquery, and pinkdb's binder refuses correlated subqueries with
a clear error ("subqueries are uncorrelated in v1"). That is a missing feature
scheduled for wave 120, not a slow query hidden here.

## Ingest (SF 0.1)

| table | format | engine | ms | M rows/s | MB/s |
|---|---|---|---:|---:|---:|
| lineitem | CSV | pinkdb | 290 | 2.07 | 261 |
| lineitem | CSV | DuckDB | 362 | 1.66 | 209 |
| lineitem | Parquet | pinkdb | 499 | 1.20 | 38 |
| lineitem | Parquet | DuckDB | 111 | 5.41 | 170 |
| orders | CSV | pinkdb | 118 | 1.27 | 145 |
| orders | CSV | DuckDB | 96 | 1.56 | 178 |
| orders | Parquet | pinkdb | 99 | 1.52 | 54 |
| orders | Parquet | DuckDB | 63 | 2.38 | 84 |

pinkdb's CSV reader beats DuckDB's on the 16-column lineitem file; its Parquet
reader is about 4.5× slower because DuckDB decodes larger column chunks in
parallel.

## Durable writes (pinkdb only)

| work | time |
|---|---:|
| 2,000 single-row autocommit inserts, `Sync` | 8.32 s |
| 2,000 inserts, batched (`SyncPolicy::Batch`) | 63 ms |
| 200 selects after each policy | 33–63 ms |
| 3 checkpoints | 51 ms |
| CLI single-row autocommit, warm | 113k rows/s |
| CLI batched `VALUES`, warm | 363k rows/s |
| two concurrent writers, optimistic transactions | 0.79× speedup |

There is no DuckDB comparison here: DuckDB has no per-statement WAL with fsync
policies. The single-process `Sync` cost is dominated by fsync, and the
two-writer number is honest about contention: two processes do not go faster
than one.

## Lake

SF 0.1 lineitem (600,572 rows), 8-thread attach, warm object cache:

| catalog | attach ms | attach + scan `sum(l_extendedprice)` ms |
|---|---:|---:|
| `pink:` | 20.1 | 14 |
| `sqlite:` | 2.2 | 14 |
| `pink+s3://` (mock S3, 61 objects) | — | 85 |

The S3 number is a local, in-process mock endpoint with no network latency; it
measures request count and object layout, not the internet. Commit latency is
not separately timed here — wave 110 measured one commit as 3 S3 requests and
2,954 bytes, independent of lake size.

## Search

| workload | result |
|---|---|
| FTS index build, 100k documents × 2 fields | 824 ms |
| FTS top-10 query, same corpus | <1 ms (below the CLI's 1 ms timer resolution; wave 108a's 103 ms was cold) |
| HNSW build, 100k × 768, 1 thread | 117.3 s |
| HNSW build, 100k × 768, 8 threads | 24.0 s |
| HNSW index size | 318,915,668 B |
| recall@10, `ef_search` 16 / 64 / 128 / 256 | 0.535 / 0.878 / 0.961 / 0.991 |
| p50/p95 latency (ms), 8 threads, same ef | 0.57/0.65, 0.89/1.03, 1.20/1.35, 1.57/1.79 |

DuckDB's `fts` and `vss` extensions need `INSTALL`, which this project does not
run without an explicit go-ahead, so there is no cross-engine search comparison
yet. It is a follow-up, not an omission by accident.

## Footprint

| build | bytes |
|---|---:|
| CLI, default features | 5,437,168 |
| CLI, `--no-default-features` | 5,278,896 |
| CLI, all measured features | 14,668,944 |
| server, default features | 5,032,960 |
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

**TPC-H joins.** The hash join copies surviving rows into fresh vectors at
every probe; DuckDB writes a selection vector and materializes once. pinkdb's
join also builds its hash table on whichever input the plan puts on the right,
which for several TPC-H queries is the large side. There are no table
statistics or cost model, so the planner cannot reorder a six-table join the
way DuckDB's query graph does. Wave 111 fixed the largest part — comma joins
were planned as nested-loop crosses, and `OR`-of-conjunctions predicates hid
per-table filters — but Q7 and Q19 remain the visible cost of the missing
build-side choice and join enumeration.

**Q18/IN.** An `IN` subquery materializes 1.5 million group keys and builds a
hash set from them; the aggregate inside the subquery is the hot path, not the
probe. A single-flight memo attempt halved Q18 but deadlocked once at SF 1 and
was reverted rather than shipped with a possible hang. The capacity fix for
the set is kept (8.8 s → 5.8 s on a focused probe).

**Q1/Q6.** Both are scan-bound. pinkdb's filter and aggregate paths are
vectorized, but they lack DuckDB's perfect-hash aggregate and selection-vector
shortcuts; that is most of the 8–26× gap.

**Parquet ingest.** DuckDB reads and decodes in larger parallel batches; pinkdb
decodes column chunk by column chunk.

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

- A join-order enumerator with cardinality estimates (DuckDB's query graph and
  DPccp shape) and a build-side choice driven by those estimates. Wave 111's
  greedy reorder only fixes disconnected comma chains.
- A cheaper `IN`-subquery path, or a safe single-flight memo.
- Perfect-hash aggregation and selection-vector filters for Q1/Q6.
- A DuckDB `fts`/`vss` comparison, once `INSTALL` is allowed.
- The CI guard is non-blocking until it has a stable history on GitHub's
  shared runners.
