# Performance architecture: pinkdb vs DuckDB (review, 2026-10-28)

A source-level comparison of pinkdb's query execution, planning and
optimization with DuckDB 2.0 (`~/Desktop/projects/ducklake-vortex/ducklake/duckdb/`)
and DuckLake (`~/Desktop/projects/ducklake-vortex/ducklake/src/`), written to
drive the next performance waves. Numbers are TPC-H on an Apple M4 (10 cores,
8 threads), from `bench/suite`.

## Status, 2026-10-30

After the waves of 2026-10-28/29 (each landed with full gates and every
TPC-H query checked against DuckDB at SF 0.01 and SF 1), SF 1 on the same
M4, quiet machine, 7 rounds:

| | pinkdb | DuckDB 2.0 |
|---|---|---|
| all 22, sum of medians | 508 ms | 264 ms |
| geometric mean of the ratios | **1.90×** | 1.00× |
| fastest / slowest ratio | Q22 0.38× / Q3 3.0× | |

What closed most of the gap, by component below:

- **Hash join (§2):** flat bucket table, vector-at-a-time assembly (125),
  batched probes (125b), packed two-column integer keys (125c), right
  semi/anti joins with residuals (125a), repeated joins evaluated once
  (125d), vectorized join filters (122d), join order through equality
  classes and a join row estimate for the build side (111e).
- **Aggregation (§3):** flat per-chunk tables for fixed-width and text keys,
  partitioned by hash during the fold (123d, 123e), pointer/view-first small
  domains (123f), shared argument arithmetic (123g), order-free `IN`
  subqueries (123h).
- **Physical types (§4):** VARCHAR as 16-byte string views over shared
  buffers (128a); filters in the column's own domain (127d), column pairs
  (127e), IN lists (127f), CASE (127g), text memo (127h), filter output
  pruning (127i).

What remains is mostly §1 (pipelines) and §5 (dictionary/constant
vectors): the profiles now spend their time in materialization between
operators (`take`/`gather` of join outputs), Q1's per-row accumulator work,
and decimal arithmetic on i128 vectors. Next structural steps, in payoff
order: late materialization of join payload columns, dictionary vectors for
low-cardinality strings (Q1's grouping), then push-based pipelines (123b).

## Status, 2026-11-04

After the waves since then (136, 141 and 142), the parts of this review marked
"today" have moved:

- **§6's "decodes every column chunk into memory at open" no longer holds.**
  Since wave 136c-1, opening a store reads the two header slots and the
  footer, not the data: a chunk's bytes are read and CRC-checked on first use,
  and the decoded cache those reads populate is charged to `memory_limit`
  (136c-2). The dated comparison in §6 is kept as the pre-136c baseline.
- **`memory_limit` is enforced** process-wide since 136a (the default is 80%
  of the memory the process may use, physical RAM or a tighter cgroup limit;
  an operator past it fails with DuckDB's out-of-memory wording). Operators
  still do not spill (row 124), so a statement over the limit fails rather
  than degrading.
- **Joins and decimals:** 141a/141b improved join output assembly and 142 the
  decimal arithmetic, the two costs the 2026-10-30 profile named.
- **Top-N (T1)** now looks through column-only projections for
  `ORDER BY … LIMIT`, instead of materializing the projected columns first.

## Is C++ faster than Rust?

Not in any way that matters here. Both compile through LLVM to native code
with the same optimizer, and DuckDB's own release builds are portable (no
`-march=native`), as are pinkdb's. The language difference is worth a few
percent at most: bounds checks (pinkdb's executor has no `unsafe`, so every
hot loop pays them unless the compiler proves them away) and the allocator
(DuckDB links jemalloc on Linux; pinkdb uses the system allocator).

The gap is architecture. DuckDB is the product of about a decade of work on
vectorized execution; at SF 1 pinkdb is still ~17× slower than DuckDB on the
TPC-H geometric mean even after six tuning waves, and almost all of that comes
from the design choices below, every one of which Rust can express.

## Where pinkdb stands (SF 1, after waves 123a and 111c)

| | pinkdb | DuckDB | ratio |
|---|---|---|---|
| Q1 (scan + group by, 6M rows) | 64–70 ms | 13 ms | ~5× |
| Q6 (scan + filter + sum) | 13 ms | 3–4 ms | ~3.5× |
| Q9 (6-way join) | ~760 ms | 26 ms | ~29× |
| Q21 (anti/semi joins) | ~1,350 ms | 18 ms | ~75× |
| Q13 (outer join + group by) | ~440 ms | 38 ms | ~11× |
| all 22, total | ~5.0 s | ~0.25 s | ~20× |

Single-table scans are now within 3.5–5×. Joins and large aggregations are
10–75× behind: that's where the architecture shows.

## The gaps, by component

### 1. Execution model: materializing interpreter vs push-based pipelines (largest)

- **pinkdb:** the plan evaluator in `exec/mod.rs` (the recursive function that
  returns `Output { chunks: Vec<DataChunk> }`) evaluates each plan node to a
  **fully materialized** result before its parent starts. Parallelism is rayon
  `par_iter` over the chunks *inside* an operator; there are a few hand-fused
  paths (`fused_scan_aggregate`, `fused_join_aggregate`, `stream_leaf_chunks`).
- **DuckDB:** `src/parallel/` (pipeline.cpp, pipeline_executor.cpp, executor.cpp,
  task_scheduler.cpp, meta_pipeline.cpp). The physical plan is cut at pipeline
  breakers (hash build, aggregate, sort) into pipelines: source → operators →
  sink. Each worker thread claims a morsel (a range of a row group), pushes one
  2,048-row vector through every streaming operator while it's hot in cache,
  and sinks into a thread-local state merged at the end. Nothing is
  materialized between streaming operators.
- **Cost to pinkdb:** every join output, projection and filter result is
  written to memory and read back. For Q9 that is hundreds of MB of
  intermediate vectors (peak RSS 676 MB at SF 1) and cache misses on every
  operator boundary. This is ROADMAP **123b**.

### 2. Hash join: single-threaded row-at-a-time build vs parallel partitioned vectorized build

- **pinkdb:** `MatchTable` (exec/mod.rs) is a `std::collections::HashMap`
  (`FastMap`, FNV hasher) from key to a match chain, built **on one thread, one
  row at a time**. Integer keys are widened to `i64`; any other key (VARCHAR,
  composite, mixed) is serialized into a `Vec<u8>` per row, and every distinct
  key allocates a copy. Probing is per row.
- **DuckDB:** `execution/join_hashtable.cpp`. Rows are appended in parallel to a
  radix-partitioned `TupleDataCollection` (row layout, `[ROW][NEXT]`); hashes
  are computed for a whole vector at once; a separate pointer table is filled
  with compare-and-swap inserts, and each pointer carries **salt bits** from the
  hash so most mismatches are rejected without touching the row. Probing is
  vectorized: hash a vector, look up a vector of pointers, compare keys
  column-at-a-time with selection vectors (`RowMatcher`). Large builds spill by
  partition. Perfect-hash joins exist for dense integer keys.
- **Cost:** Q9/Q21 build tables of millions of rows; a single-threaded
  `HashMap` with per-key heap allocations and per-row probing is the main
  reason joins are 25–75× behind.

### 3. Aggregation: per-chunk `HashMap<Vec<u8>>` + enum accumulators vs radix-partitioned row-layout tables

- **pinkdb:** `exec/aggregate.rs`. Each chunk builds a `Partial { groups:
  Vec<(Vec<u8>, Group)>, index: FastMap<Vec<u8>, usize> }` (single-int keys get
  a fast `FastMap<i64, usize>` path); per-group state is a `Vec<Acc>` where
  `Acc` is a Rust enum matched **per row, per aggregate**; `min`/`max` hold a
  dynamic `Option<Value>`. Partials merge at the end (now radix-parallel, 111c).
- **DuckDB:** `radix_partitioned_hashtable.cpp`, `aggregate_hashtable.cpp`,
  `perfect_aggregate_hashtable.cpp`. Thread-local tables in row layout with
  salted pointers; aggregate states are fixed-size byte regions updated by
  type-specialized function pointers over whole vectors (one call per vector
  per aggregate, not per row); radix partitioning from the start so the merge is
  embarrassingly parallel; a perfect-hash table when the group keys' ranges are
  small (Q1's 4 groups), which turns grouping into array indexing.
- **Cost:** Q1 is ~5× (its 8 aggregates per row through the enum), Q13/Q18
  more. This is ROADMAP **123c**.

### 4. Physical types: i128 decimals and heap-allocated strings

- **Decimals:** pinkdb stores every `DECIMAL` as `i128`
  (`VectorData::Decimal { values: Vec<i128>, .. }`). DuckDB picks the
  **smallest** physical type for the precision
  (`include/duckdb/common/types/decimal.hpp`: `MAX_WIDTH_INT16/32/64`, then
  `hugeint`). TPC-H's `DECIMAL(15,2)` is `int64` in DuckDB: half the memory
  bandwidth and native 64-bit arithmetic. After 123a, Q1's profile is
  `eval_arith` 28% and `add_decimal` 20%, mostly i128 work.
- **Strings:** pinkdb stores `Vec<Arc<str>>`: one heap allocation plus a
  refcount per string, and comparing or hashing chases a pointer. DuckDB's
  `string_t` (`include/duckdb/common/types/string_type.hpp`) is 16 bytes: strings
  of 12 bytes or less are **inline**, longer ones keep a 4-byte **prefix**
  inline plus a pointer into a per-vector string heap. Most comparisons finish
  on the prefix without a pointer chase, and no per-string allocation happens.
  Every string-keyed join, group-by and sort pays for this today.

### 5. Vector representations: flat only vs constant/dictionary/sequence/FSST

- **pinkdb:** a `Vector` is always flat (`VectorData` + validity), except a
  constant shortcut in a few places.
- **DuckDB:** `enum class VectorType { FLAT, FSST, CONSTANT, DICTIONARY, SEQUENCE, SHREDDED }`
  (`common/enums/vector_type.hpp`), with `UnifiedVectorFormat` so kernels read
  any of them without flattening. A dictionary-compressed column is scanned
  **as a dictionary vector**: a filter or group-by on it touches the
  dictionary once, not every row; a constant vector costs one value.

### 6. Storage: decode-everything-on-open vs buffer-managed compressed row groups

- **pinkdb (before 136c):** the `.pink` format compresses well (bitpack, RLE,
  delta, ALP, dictionary, FSST, a second LZ4 stage; see the persist.rs header),
  but `load_table_data` **decoded every column chunk into memory at open**, and
  execution saw only decompressed flat vectors. Memory equalled the decoded
  dataset; opening a large store cost a full decode. Since wave 136c-1 open
  reads the header slots and footer and a chunk is decoded on first use (see
  the 2026-11-04 status above).
- **DuckDB:** blocks are loaded on demand by the buffer manager
  (`storage/buffer_manager.cpp`), row groups are 122,880 rows
  (`DEFAULT_ROW_GROUP_SIZE`), segments stay compressed until scanned and are
  scanned straight into (often dictionary/constant) vectors; zone maps skip
  segments; memory is bounded and spills to temp files (`memory_limit`).
  ROADMAP **124** covers spilling; lazy loading is not yet a row.

### 7. Optimizer coverage: 7 passes vs ~45

pinkdb's `optimizer::optimize` runs: constant folding, comma-chain ordering,
filter pushdown (with OR-derived filters), vector top-k, FK join elimination,
count-star projection pruning, index selection. DuckDB's
`optimizer/optimizer.cpp` runs ~45 including, most relevant here:

- **UNUSED_COLUMNS / COLUMN_LIFETIME:** drop columns nobody reads, at every
  operator (111c measured Q2 copying 19 columns to use 1);
- **COMMON_SUBEXPRESSIONS, PROJECTION_PULLUP, PROJECTION_PLACEMENT;**
- **STATISTICS_PROPAGATION:** infer value ranges through operators and shrink
  types ("compressed materialization"), prove filters always true/false;
- **LIMIT_PUSHDOWN, TOP_N, LATE_MATERIALIZATION, EMPTY_RESULT_PULLUP;**
- **PARTIAL_AGGREGATE_PUSHDOWN, AGGREGATE_REUSE, COMMON_SUBPLAN;**
- **REORDER_FILTER** with adaptive filter ordering at run time
  (`execution/adaptive_filter.cpp`);
- **JOIN_ORDER** with a full DPhyp enumerator over a query graph plus the
  cardinality estimator (pinkdb has a simpler DP over comma chains, 111ab/122);
- **IN_CLAUSE, REGEX_RANGE, DELIMINATOR, OUTER_JOIN_SIMPLIFICATION.**

### 8. Lake scans: materializing load vs streamed multi-file reads with catalog-side pruning

- **pinkdb:** `lake.load_filtered(...)` decodes the matching files into chunks
  (zone/file-stat pruning since 122), then the query runs over the result.
- **DuckLake:** `storage/ducklake_multi_file_list.cpp` and
  `ducklake_metadata_manager.cpp` (`GenerateFilterFromExpression`) translate the
  query's filters into **SQL against `ducklake_file_column_stats` in the
  catalog database**, so SQLite/Postgres return only files that can match;
  DuckDB's multi-file Parquet reader then streams those files in parallel
  with row-group pruning, projection pushdown and late decode, inside the
  pipeline (no full materialization).

## Plan (in payoff order; each a measured wave, results pinned against DuckDB)

| step | what | why first | ROADMAP |
|---|---|---|---|
| 1 | **Push-based pipelines with morsel parallelism**: physical plan with source/operator/sink, thread-local sink states, no materialization between streaming operators | removes the biggest structural cost; everything below plugs into it | 123b |
| 2 | **Hash join rewrite**: parallel partitioned build into a row-layout collection, salted pointer table, vectorized hash/probe/compare, perfect-hash for dense int keys | joins are 25–75× | new 125 |
| 3 | **Aggregation rewrite**: row-layout states updated per vector, radix-partitioned thread-local tables, perfect-hash for small key domains | Q1/Q13/Q18 | 123c |
| 4 | **Physical types**: decimal width by precision (i16/i32/i64/i128), `string_t`-style 16-byte strings with inline/prefix + per-vector heap | halves decimal bandwidth; string joins/groups/sorts | new 126 |
| 5 | **Column pruning and expression passes**: unused columns/column lifetime, common subexpressions, statistics propagation | cheap, broad wins | new 127 |
| 6 | **Vector types**: constant, dictionary, sequence through the kernels (`UnifiedVectorFormat`-style) and scanning dictionary-encoded columns as dictionary vectors | filters/group-bys on low-cardinality strings | new 128 |
| 7 | **Buffer-managed lazy storage + spill**: load segments on demand, keep them compressed until scanned, bounded memory, spill joins/aggregates/sorts | memory and open time; SF 10+ | 124 + new 129 |
| 8 | **Lake streaming scans**: catalog-side file pruning as SQL over file stats, parallel streamed Parquet reads inside the pipeline | lake queries | new 130 |
| 9 | **Allocator and bounds checks**: an optional `mimalloc`/jemalloc global allocator (measure; feature-gated to keep the default light), targeted `unsafe` slice access in the hottest kernels behind `debug_assert`s | a few % to 15% on allocation-heavy paths | fold into 125/126 |

Rules carried by every step: no result change (bench/duckdiff.py, the 22-query
TPC-H checksums, the correlated and join fuzzers), measure on a quiet machine
before and after, DuckDB's source as the design reference (port, credit,
never paste), and keep the default binary small.
