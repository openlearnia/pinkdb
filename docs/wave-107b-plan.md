# Wave 107b — vector similarity search

ROADMAP row 107 is vector storage in three parts. 107a (the fixed-size
`ARRAY` type) is in the base. This is **107b**: the distance functions and an
HNSW index behind a `vss` extension. 107c (`embed`) stays queued.

Everything measured below comes from `/tmp/duckdb-2.0.0/duckdb`
(2.0.0 "Cyanoptera", commit `d54cbbe3df`). The `vss` extension is **not
installed** in that binary and the task forbids downloading it, so every
statement about vss internals is documented behaviour, not a measurement;
those points are marked *(documented, not verified)*.

## Reference behaviour (DuckDB 2.0.0, measured)

The five functions are **core** in DuckDB, not part of vss:

```
array_distance([1,2,3],[4,5,6])                 -> 5.196152      FLOAT
array_distance([1.0,2.0]::DOUBLE[2], …)         -> 2.828…        DOUBLE
array_cosine_distance([1,2,3],[4,5,6])          -> 0.025368094   FLOAT
array_cosine_similarity([1,2,3],[4,5,6])        -> 0.9746319     FLOAT
array_inner_product([1,2,3],[4,5,6])            -> 32.0          FLOAT
array_negative_inner_product(…)                 -> -32.0         FLOAT
```

- `array_distance` is the **Euclidean** distance (square root), not squared.
- `FLOAT[N]` arguments produce a `FLOAT` result, `DOUBLE[N]` a `DOUBLE`;
  one of each widens to `DOUBLE`.
- `INTEGER[N]`/`BIGINT[N]` are refused at bind:
  `array_distance: Arguments must be arrays of FLOAT or DOUBLE`.
- A `NULL` array is `NULL`. A `NULL` *element* is a runtime error:
  `array_distance: left argument can not contain NULL values` (likewise
  `right`).
- Different lengths are a bind error:
  `array_distance: Array arguments must be of the same size`.
- A zero vector gives `nan` for both cosine functions.
- `array_l2_distance` does **not** exist in DuckDB 2.0.0. ROADMAP row 107
  names it, so pinkdb accepts it as an alias of `array_distance`.

pinkdb has no `FLOAT` scalar type (107a widens an element read to `DOUBLE`),
so the FLOAT overloads return `DOUBLE`; the arithmetic is still performed in
`f32` so a result matches DuckDB's `FLOAT` value widened to `DOUBLE`.

## Where the code lives

**Distance functions: engine core** (`crates/pinkdb/src/exec/expr.rs`,
`ScalarFunc`). They are five small, typed kernels, they need no catalog, and
DuckDB puts them in core for the same reason. A loadable extension cannot be
used inside the optimizer or the index build without materialising values.

**HNSW: a linked extension, `extensions/vss` (`pinkdb-vss`)**, behind a
CLI/server cargo feature `vss` (off by default). It is a native extension
package like `pinkdb-fts`: no cdylib, no new dependency.

**The engine hook is generic, not HNSW-shaped.** `crates/pinkdb/src/`
`vector_index.rs` defines:

```rust
pub trait VectorIndex: Send + Sync + Debug {
    fn clone_box(&self) -> Box<dyn VectorIndex>;
    fn dim(&self) -> usize;
    fn insert(&mut self, row: u64, vector: &[f32]) -> Result<()>;
    fn delete(&mut self, row: u64);
    fn remap_after_delete(&mut self, shift: &DeleteShift);
    fn search(&self, query: &[f32], k: usize, ef_search: Option<usize>)
        -> Result<Vec<(f32, u64)>>;
    fn compact(&mut self);
    fn serialize(&self) -> Result<Vec<u8>>;
    fn stats(&self) -> VectorIndexStats;
}

pub trait VectorIndexProvider: Send + Sync + Debug {
    fn name(&self) -> &'static str;              // "hnsw"
    fn create(&self, dim: usize, options: &[(String, String)])
        -> Result<Box<dyn VectorIndex>>;
    fn deserialize(&self, bytes: &[u8])
        -> Result<Box<dyn VectorIndex>>;
}
```

`ExtensionPackage` gets `.with_vector_index_provider(Arc<dyn …>)`; loading
the package installs it into `PinkDb.vector_providers` (exactly the host
scalar/statement pattern from 108a). A table carries at most one vector
index per column; the catalog entry is `VectorIndexDef { name, column,
provider, options, index: Arc<dyn VectorIndex> }` on `DataTable`.

### Why not reuse the hash `Index` machinery?

The hash `Index` is a static `HashMap<Vec<u8>, Vec<usize>>` rebuilt from the
table; HNSW is mutable state that must survive a checkpoint and apply WAL
replay. Keeping it in a separate list leaves the hash index path (and its
`indexes_stale` rebuild) untouched; `CREATE INDEX … USING HNSW` and
`CREATE INDEX` stay distinct statements with distinct plan variants.

## SQL surface

```sql
CREATE INDEX idx ON docs USING HNSW (embedding)
  WITH (metric = 'cosine', M = 16, ef_construction = 128, ef_search = 64);
DROP INDEX idx;
CALL hnsw_compact_index('docs', 'idx');  -- tombstones are rebuilt away
```

`USING` may come before or after the column list. pinkdb has no `PRAGMA`
and no `ALTER INDEX`, so compaction is a host statement from the `vss`
package; it returns one `Called` result row.

Metrics are DuckDB's spellings: `l2sq`, `cosine`, `ip`. Defaults are
DuckDB's: `M = 16`, `ef_construction = 128`, `ef_search = 64`, `M0 = 2*M`.
`M0` is accepted and defaults to `2*M`. `metric` defaults to `l2sq`.

The index applies to `FLOAT[N]` and `DOUBLE[N]` columns. A `DOUBLE` vector is
narrowed to `f32` for the graph (vss itself only indexes `FLOAT[N]`, so this
is a pinkdb extension); exact mode keeps `f64`.

### The rewrite

`optimizer::apply_vector_topk` recognises exactly:

```
Limit(k)
  [Project]                              -- optional hidden-column prune
    Sort[(distance_expr, ASC)]            -- exactly one key, ASC
      Project                            -- optional
        Scan(table)                      -- native base table
```

where `distance_expr` is one of `array_distance` / `array_l2_distance`,
`array_cosine_distance`, or `array_negative_inner_product`, with a column of
the scanned table as one argument and a **constant** `ARRAY` literal of the
same N as the other, and that column is the indexed column of a live HNSW
index with the matching metric (`l2sq`/`cosine`/`ip`). The literal is folded
by `fold_plan` before this pass, so it is a `Value::Array`. A parameter is
bound as a literal too (`$x`, prepared values), so it qualifies; the plan
node carries the folded `Value`.

`DESC`/`NULLS FIRST`, a second sort key, a `WHERE` (Filter) between the
limit and the scan, a non-native table, a mismatched metric, a mismatched N,
a different function, or an expression that is not exactly one column and
one constant all **fall back to the existing scan + sort + top-N heap**. It
is not an error: the answer stays exact. This is also what makes a
`WHERE`-filtered top-k return the full `k` result rows instead of DuckDB's
documented "may return fewer" warning *(documented, not verified)*.

The rewrite produces `LogicalPlan::VectorTopK { db, table, column,
column_name, index, query: Value, metric, dim, k }`. At execution, when
`SET VARIABLE hnsw_exact = true`, the node brute-forces with a bounded
max-heap instead of calling the graph (so the exact path shares the
scan/index-column plumbing). `EXPLAIN` prints

```
VectorTopK {db}.{table} using {index} ({metric}, k = {k})
```

and a `Limit` above it is a no-op (the node already returns at most `k`).

### Session switches

pinkdb has no `SET`/`PRAGMA`; session settings are `SET VARIABLE`. They are
per-connection and are not persisted or WAL-logged.

```sql
SET VARIABLE hnsw_exact = true;     -- force brute force
SET VARIABLE hnsw_ef_search = 128;  -- per-session search width
```

`hnsw_ef_search` overrides the index's `ef_search` for the plan.

## HNSW

Hand-written, no dependency:

- **Levels** come from SplitMix64 seeded with `0x5EED_107B` (per index), so a
  build is deterministic for a given insert order and the RNG state is
  serialized, so inserts after a reopen continue the same stream. Level =
  `floor(-ln(u) / ln(M))`, capped at 32.
- **Insert** is the standard HNSW: greedy descent to the node's level, then
  `ef_construction` search per layer with a visited bitset, neighbour
  selection by the paper's heuristic (not plain nearest-M), and pruning of
  an over-full neighbour list with the same heuristic.
- **Search** is greedy descent to layer 0 then an `ef_search` best-first
  search; results sorted by `(distance, row)` and truncated to `k`.
  Tombstones are skipped, so a search can return fewer than `k`.
- **Kernels** are chunked `f32` loops with four accumulators over 8-element
  blocks, written so LLVM auto-vectorises; no `unsafe` SIMD.
- **Deletes** set a tombstone and drop the row→node map entry; the
  tombstoned vector stays in the graph for connectivity until `COMPACT`.
  `COMPACT` builds a fresh graph from the live nodes in node order with a
  fresh seed, so the result is deterministic and replayable.
- **An `UPDATE` of the vector** is delete + insert (the engine calls
  `delete(row)` before writing and `insert(row, new)` after).
- **Row-id shifts**: `delete_from_chunk` physically removes rows. The engine
  passes `delete_from_chunk`'s chunk start, old length, removed positions and
  whether the chunk was dropped; the index tombstones the removed rows and
  rewrites `row → row'` for every later row. DELETE processes chunks in
  reverse, so this is the only remap needed per chunk.

## Persistence

The graph is stored with the store, not rebuilt on open:

- A new `.pink` footer flag `0x100`. Per table, after the hash-index
  definitions, write one `VectorIndexDef` record: name, column, provider
  name, options as `(key, value)` strings, and the serialized graph bytes.
  Unknown flags are already refused, so an older build refusing a newer file
  is the intended no-legacy behaviour. `FOOTER_VERSION` stays 1.
- The blob starts `PHNSW01\0`, then dim, metric, M/M0/ef parameters,
  RNG state, entry point, max level, and nodes (level, row, tombstone flag,
  f32 vector, per-level neighbour lists). Read-side bounds are checked; a
  truncated blob is a corrupt-file error.
- `load_bytes` keeps the blob opaque if the provider is not registered yet.
  `load_extension_package("vss")` hydrates every pending entry for that
  provider, after the rows are decoded (row ids match). A store opened
  without the extension still opens, saves and serves exact queries; it
  cannot use, create or compact the index. The CLI/server load built-ins
  right after opening, so the common path hydrates immediately.
- **WAL replay** rebuilds the index by replaying the insert/delete/update
  statements through the ordinary mutation funnels (the WAL stores SQL
  text). `LOAD EXTENSION 'vss'` in the WAL resolves the process-registered
  package. No experimental flag: unlike DuckDB, the graph is an ordinary
  checksummed footer section written by an atomic save; a crash leaves either
  the old or the new file, and the WAL is replayed on top. (DuckDB requires
  `hnsw_enable_experimental_persistence`; we do not.)
- A checkpoint serializes each graph once; `to_pink_bytes` refuses while a
  graph is still pending (the store cannot be written without its provider).

## Refusals and permissions

- `CREATE INDEX … USING HNSW` on a lake/attached table, a view, a non-`ARRAY`
  column, or an integer array is refused by name before any graph is built.
  (The native-table check is the binder's ordinary table lookup; the attached
  check is `attached_read_only` in `create_vector_index_in`.)
- An unknown provider (`USING FOO`) is refused when the statement runs, naming
  it; an unknown option is refused by the provider.
- RBAC rides on `CREATE INDEX`: the same plan variant is in the schema-DDL
  set, so it needs `CREATE` on the database (ADMIN passes). `DROP INDEX` and
  `CALL hnsw_compact_index` follow `DropIndex`/`Call`. The read-only MCP
  policy uses `plan::is_read_only`, which includes `VectorTopK` and excludes
  the DDL, so top-k is allowed and index changes are refused.
- Lake/MCP/agent: the index only ever applies to a native table; a query
  over a lake table never matches the rewrite.

## Distance functions

Add `ScalarFunc::ArrayDistance`, `ArrayCosineDistance`,
`ArrayCosineSimilarity`, `ArrayInnerProduct`,
`ArrayNegativeInnerProduct`; `array_l2_distance` maps to the first. Binder:
both args must be `ARRAY` of `Float`/`Double`, same `len`; result `DOUBLE`
(pinkdb has no scalar `FLOAT`) and `f64` arithmetic for DOUBLE/mixed, `f32`
arithmetic widened for FLOAT/FLOAT. Typed evaluation in `eval_scalar_typed`
reads `VectorData::Array` slices directly. A constant query is folded at bind
time and broadcast per chunk by `Vector::new_const`; it is not cast per row.

Brute force without an index is a bounded top-k heap: `VectorTopK`'s exact
mode, or the existing `Limit`-over-`Sort` `top_n` for a query that does not
match the rewrite.

## Benchmarks

Method: `cargo run --release -p pinkdb-vss --example bench_vector` on an
Apple-silicon laptop; clustered data (32 centres for 100k×768, 64 for
1M×128; each row is a centre plus a small Gaussian offset); 200 held-out
queries from the same centres; `recall@10` against the exact scan;
p50/p95 over the 200 queries. Stores live in `/tmp` and were deleted after
measuring. `bench_vector` is the checked-in reproduction.

| set | build | graph bytes | ef | recall@10 | index p50/p95 | brute p50/p95 |
|---|---|---|---|---|---|---|
| 100k × 768 FLOAT | 94.5 s | 317,651,764 | 16 | 0.522 | 0.53 / 0.58 ms | |
| | | | 64 | 0.874 | 0.86 / 0.92 ms | 17.99 / 18.40 ms |
| | | | 128 | 0.963 | 1.13 / 1.22 ms | |
| | | | 256 | 0.992 | 1.43 / 1.55 ms | |
| 1M × 128 FLOAT | 867.2 s | 616,545,888 | 16 | 0.414 | 0.26 / 0.43 ms | |
| | | | 64 | 0.740 | 0.49 / 0.56 ms | 69.59 / 94.29 ms |
| | | | 128 | 0.862 | 0.76 / 0.87 ms | |
| | | | 256 | 0.926 | 1.22 / 1.36 ms | |

The brute column is the plain `ORDER BY … LIMIT` path with no index (the
existing bounded `top_n` heap). The "exact switch" (`hnsw_exact = true`,
`VectorTopK`'s own heap) measured 15.16/15.24 ms and 29.36/30.35 ms on the
two sets, so the heap is not slower than the sort path. Loading the CSV-free
synthetic rows took 34.2 s and 63.8 s respectively and is not part of build.

Recall at the DuckDB default `ef_search = 64` is 0.874 on the 768-dimension
set and 0.740 on 1M×128. The engine test that gates recall (100k × 8, 16
clusters) measures **≥ 0.95 at 64**; the high-dimensional sets need
`ef_search` 128–256 for ≥ 0.95. Increasing the default would diverge from
DuckDB's documented default, so the default stays 64 and the table reports
the trade-off.

### Non-vector A/B and binary sizes

Release CLI built from `git archive HEAD` (the base commit) in
`/tmp/pinkdb-ab-target` (deleted afterwards) against this tree, interleaved
on the same TPC-H SF 0.1 `lineitem` store, 12 timed runs per process after
warm-up, medians of two 8/10-round series:

| query | base | this tree | ratio |
|---|---|---|---|
| Q1 | 9.11 ms | 9.13 ms | 1.004 |
| Q6 | 4.88 ms | 4.94 ms | 1.017 |

Q1 is within noise; Q6 is ~1.5 % slower. The change touches no scan,
aggregate or join path; the only hot-path edit is the added `ScalarFunc`
match arm for the distance functions, so this is code-layout noise (the
same binary was used for all four columns). No vector index exists in
either run.

CLI binary (release, unstripped): default **5,304,688 B**; with
`--features vss` **5,390,816 B** (+86,128 B, 1.6 %). The default build links
neither SQLite nor Postgres and does not contain `pinkdb-vss`.

## Tests

- distance values against the real DuckDB binary on the same inputs, for
  every function, FLOAT/DOUBLE/mixed, zero vectors, NULL array, NULL element
  error, length-mismatch bind error, integer-array refusal;
- deterministic build/save/open/query, plus crash-after-inserts replay gives
  the same graph bytes;
- inserts, deletes and vector updates after `CREATE INDEX` are visible;
- `CALL hnsw_compact_index` removes tombstones and keeps the same answers;
- `EXPLAIN` shows `VectorTopK … using …`;
- `SET VARIABLE hnsw_exact = true` returns brute-force order;
- recall ≥ 0.95 at `ef_search = 64` on the clustered 100k set;
- refusals: lake table, non-array column, metric mismatch falls back exact
  and is still correct;
- MCP read-only refuses `CREATE INDEX`/`CALL hnsw_compact_index` and allows
  top-k.

## What could not be verified

- vss's actual defaults, its planner rule and its persistence internals
  *(documented, not verified)*: the DuckDB docs name
  `M = 16`, `ef_construction = 128`, `ef_search = 64`, `M0 = 2*M`, the
  experimental-persistence setting, `PRAGMA hnsw_compact_index`, and that a
  filtered top-k may return fewer rows. We implement that surface with
  `CALL hnsw_compact_index` (pinkdb has no `PRAGMA`/`ALTER INDEX`) and
  always-on persistence. vss's exact parameter defaults are therefore also
  taken from the docs, not from a local `duckdb_extensions()` row.
- DuckDB's exact tie-breaking and its float accumulation order.
  Tests compare with a tolerance; `f32` kernels reproduce DuckDB's
  `FLOAT` results to the last bit when widened.

## Risks

- The graph is copied on the first write of a transaction (`Arc` is shared
  with the pre-transaction snapshot), so a large index plus a small write
  pays a deep clone. Autocommit writes mutate in place.
- `DELETE` moves row ids; `remap_after_delete` walks every node, so a delete
  is O(nodes) per affected chunk. Bulk deletes of large tables are therefore
  linear, not per-row; compaction is the expensive repair.
- A store opened without the `vss` package keeps its graphs opaque. Queries
  fall back to exact (not an error); mutations and saves refuse so a graph
  can never be lost silently.
- A 1M × 128 index is ~0.5 GiB of vectors plus links; the benchmark measures
  the real size and the build time before it is called acceptable.
