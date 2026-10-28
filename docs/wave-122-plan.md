# Wave 122 — column statistics and join-filter pushdown

Row 122 of `ROADMAP.md`: keep per-column statistics in the catalog, feed them
to wave 111ab's cardinality model, and push a hash join's build-side keys into
the probe side's scans (row-group skipping plus a membership filter). The gate
is performance only: no query may return a different result, with statistics
enabled or disabled, filtered or not.

Reference design (read-only, MIT): DuckDB
`src/storage/statistics/{base_statistics,numeric_stats,string_stats,distinct_statistics}.cpp`,
`src/optimizer/{join_filter_pushdown_optimizer,statistics_propagator}.cpp`,
`src/optimizer/join_order/cardinality_estimator.cpp`,
`src/execution/operator/join/physical_hash_join.cpp` (dynamic filters),
`src/planner/filter/`, `src/optimizer/late_materialization.cpp`. DuckLake's
`ducklake_table_column_stats` / `ducklake_file_column_stats` are the per-file
min/max the lake path already prunes with. Nothing is transliterated; the
sketches and the pushdown shape are re-derived for this engine.

## Design

### 1. Statistics (new `crates/pinkdb/src/stats.rs`)

* `Hll` — a fixed-precision HyperLogLog with `p = 12` (4096 one-byte
  registers, ~1.6 % standard error), hand-written, no crate. The register hash
  is FNV-1a plus a final avalanche over the same canonical value the
  equality encodings produce (`stats::key_hash` has a typed path per vector
  kind and falls back to `value_hash` for VARIANT/ARRAY; the two agree cell for
  cell), so `1` and `1::BIGINT` hash alike — exactly like `=`. It has `insert`,
  `merge`, `estimate`, `to_bytes` / `from_bytes`; the wire form is sparse (a
  block directory plus the occupied 32-register blocks), so a column with few
  distinct values costs tens of bytes, not 4 KiB.
* `ColStats` — `null_count: u64`, `min: Option<Value>`, `max: Option<Value>`,
  `hll: Option<Arc<Hll>>`, `dirty: bool`. `dirty` means the distinct estimate
  is an upper bound left behind by deletes/updates (HLL cannot subtract);
  `ANALYZE` clears it. NaN never takes part in min/max and counts in no NULL
  count; `-0.0`/NaN normalize exactly as `encode_value` does.
* `TableStats` — `rows: u64` and one `ColStats` per column.
* `Zone` — `{ null_count: u64, min: Option<Value>, max: Option<Value>,
  nan_count: u64 }`, one per column chunk (2048 rows). Zones are the zone
  maps: exact when the chunk was written/rewritten, and only ever *widened* by
  a delete, so pruning is always conservative.
* `Bloom` — a blocked Bloom filter (8 × u64 words per 512-row block, two hash
  probes into the block, block chosen by the high bits), sized from the build
  side's catalog distinct estimate with DuckDB's allocation formula
  (`-n·ln(fpr)/ln(2)²`, 1 % target). It hashes a key cell with the same
  `stats::key_hash` the sketch uses, so the probe scan rebuilds exactly the
  build side's hash from its own chunk.

Maintenance lives on `DataTable` (every write in the codebase goes through
`append_row`/`append_chunk`/`append_shared_chunk`, `update_rows` and
`delete_from_chunk`):

| write | rows/NULL/min/max | distinct |
| --- | --- | --- |
| append | exact (merge the new chunk's zone, insert each value into the HLL) | exact HLL insert |
| update | exact in the written chunk; the table bound is re-merged from zones only when the touched chunk held it | HLL insert the new values (old ones stay; `dirty = true`) |
| delete | the chunk is re-encoded, so its zone is recomputed exactly; the table bound re-merges only when the chunk held it | unchanged (upper bound; `dirty = true`) |
| `ANALYZE` | recomputed from the chunks | recomputed from the chunks, `dirty = false` |

Deletes therefore never rescan the table to fix a bound (DuckDB's rule), but
they also never leave a *wrong* bound. `min`/`max` on a delete may shrink —
that is stricter than DuckDB and still O(chunk).

**Persistence.** `.pink` footer version 1 → 2: each table entry gains one
statistics block after its row groups — per column NULL count, min, max, dirty
flag and the HLL bytes (length 0 = unknown), then the per-chunk zone maps. The
reader installs both without rescanning the data (`append_loaded_chunk` skips
statistics maintenance, `set_loaded_stats` restores the block), so a reopened
store has exactly the statistics it was saved with. There is no legacy format
to read; an older file fails with a clear "written by an older pinkdb" error.
The rows-free metadata image carries `TableStats::unknown` (no bounds, no
sketch): that image describes tables whose rows live elsewhere.

**SQL surface.**

* `ANALYZE [<table>]` — parser → `Statement::Analyze` → `LogicalPlan::Analyze`
  (a statement-only node, never WAL-logged: statistics are advisory and
  `ANALYZE` can always be re-run; a checkpoint persists them). Without a table
  it analyzes every table of the session database. It is refused inside a
  transaction (a rollback would leave statistics for rows that never existed)
  and it is ADMIN-only, like `CHECKPOINT`: it rewrites what every session's
  planner reads.
* `pinkdb_column_stats([<table>])` — a read-only table function shaped like
  `duckdb_columns`, bound at `FROM` time to a `Values` plan from the catalog:
  `database, table_name, column_name, data_type, row_count, null_count,
  distinct_count, min, max, stats_approximate`.
* `SET VARIABLE join_filter_pushdown = false` (also `SET join_filter_pushdown =
  false`) — the session switch kept for debugging and the differential tests.
  Default on.

### 2. The cardinality estimator uses them

`crates/pinkdb/src/optimizer.rs`:

* `conjunct_selectivity` becomes `conjunct_selectivity(db, leaf, c)`: for a
  single-column comparison against a literal, equality is `1/distinct`, a
  range is the uniform fraction between the stored `min`/`max` (numeric, date,
  timestamp, decimal, double), `IN` is `k/distinct`, `<>` is `1 - equality`.
  No stats, or a shape it cannot interpolate (strings, `LIKE`, functions), keep
  today's `EQUALITY 0.2` / `OTHER 0.5` fallbacks.
* Equality-class domains (join cardinality) use the *distinct* estimates of
  the class's key columns instead of "smallest base row count", which is
  today's NDV stand-in. Fallback unchanged.
* `estimate_rows` (exec/mod.rs), which chooses the hash join's build side,
  applies the same filter selectivity over the leaf base rows, and estimates
  an inner join as `|L|·|R| / max(ndv_L, ndv_R)` per key pair from the two
  catalog distinct counts. When either key has no catalog distinct count (a
  derived input) the join stays unestimated: a guessed `min`/`max` fallback
  misplaces the build side more often than no estimate. With no statistics at
  all it degrades exactly to today's choice.
* `estimate_distinct(db, plan, column)` is the shared helper (optimizer.rs):
  the leaf table's exact distinct estimate, dirtied by deletes, else `None`.

Plans are cached per table version, and stats change without a version bump by
`ANALYZE`; the optimizer reads the live stats when it plans (it already runs
per statement), so an `ANALYZE` affects the next statement.

### 3. Join-filter pushdown

`scan_output` already receives static predicates through
`plan::ScanFilters`; the dynamic filters are a second registry on `Cx`
(`DynamicFilters`), keyed by plan-node address and column, consulted by the
local and lake scans. A filter is only derived when the build side is small
(≤ 10k rows) or at least 8× smaller than the probe estimate — hashing a large
build side to filter a scan that is not much larger is a net loss, which the
measurements showed for most TPC-H joins.

1. In `exec::eval` for `Inner` and `Semi` joins, evaluate the **build** side
   first (inner: `build_on_left`'s choice; semi: the right side) and build the
   `MatchTable` while collecting, from the first key column only,
   `lo`/`hi` (non-NULL min/max) and a `Bloom` over the key cells.
2. Walk the **probe** plan to the scans that produce each direct-column key
   (`Scan`, `Filter`, `Limit`, `Project` of a bare column, and through a
   `Join`'s left/right halves). For every such scan, register the range and
   the Bloom on that scan's column and plan-node address.
3. Evaluate the probe side; `scan_output` then
    * skips a whole chunk when the chunk zone cannot overlap the range (or a
      static predicate),
    * builds a selection mask for the Bloom/range and emits only surviving
      rows for the projected columns,
    * hands the same predicates to a lake scan's existing `load_filtered` path.
4. Drop the registrations after the probe evaluation (a correlated
   re-evaluation must not see the previous build's filters).

Semantics: only `Inner` and `Semi`, because their probe rows with no build
match can never be emitted. `Left`/`Right`/`Full` preserve probe rows and
`Anti`/`Mark` need the *absence* of a match, so filtering the probe would
change their results; none of them get a dynamic filter (they still get static
zone-map skipping). The Bloom is only a pre-filter: the hash join still
decides every row, so even a false positive cannot change a result; a false
negative is impossible by construction (the filter is built from the build
keys with the same encoding the probe scan uses).

The join shapes that do not evaluate a general hash join (`fused_join_aggregate`
for `JOIN … GROUP BY` on an inner join) take no dynamic filters; TPC-H has no
such shape on the star queries. That, and applying filters to a build side that
is itself a scan, are follow-up rows.

### 4. Zone maps in `.pink` scans

Static predicates (`WHERE col = 5`, ranges, `IS NULL`) reach the scan through
`ScanFilters`; for a local table the scan now maps them to the zone maps and
skips non-overlapping chunks before anything reads them. The lake path already
does this from Parquet row-group statistics; the dynamic join filters are now
appended to the same predicate list. A NULL-only chunk never satisfies a
comparison and satisfies `IS NOT NULL` never; `IS NULL` is answered from the
zone's NULL count, so an all-NULL chunk is skipped for a range filter.

### 5. Late materialization

Measured first, as the brief allows: a local scan already copies only the
columns the projection needs (`project_scan_chunks`), and the TPC-H SF 1
profile shows no query whose time is dominated by materializing columns the
filter does not need — Q21 (the only query the join filters move) holds the
same chunk shapes with and without them. Deferred to a follow-up row, to be
landed only with a measurement that shows headroom.

## Order of work

1. `stats.rs` + `DataTable` maintenance + footer v2 + `ANALYZE` + the table
   function + unit/integration tests (exactness after seeded I/U/D, HLL bound,
   round-trip, `ANALYZE`).
2. Cost model: optimizer selectivities and build-side estimates + optimizer
   unit tests (no result change: plans only).
3. In-memory zone maps + local scan skipping, differential tests.
4. Dynamic filters: build-first join evaluation, registry, scan filtering,
   session switch; seeded differential fuzz (NULL keys, duplicates, empty
   build, all-match, no-match, chained joins) filters on vs off.
5. Measure `bench/suite` TPC-H SF 0.1 and SF 1 (star queries Q3/Q5/Q7/Q8/Q9/
   Q10/Q21), the durable-insert and ingest families (≤ 5 % regression),
   `bench/duckdiff.py`, the 22-query correctness check at SF 0.01/0.1, the CI
   bench, the wasm build, binary size.
6. Docs: README, `docs/benchmarks.md`, website benchmarks page, CHANGELOG,
   ROADMAP row 122 (plus follow-up rows for late materialization and anything
   measured as still open).

## Measurements

Two release binaries, same generated Parquet, each loading its own `.pink`
store: the wave-111ab tree (`e4a8677`) and the wave-122 tree. Queries alternate
between the two, 1 warm-up + 6 timed runs per process, and the reported number
is the better of two process medians. Load average was 2.5–2.7 for SF 1 and
5.4 for SF 0.1 (two users on the machine); `pgrep -x cargo` was empty.

SF 1, pinkdb milliseconds (before → after):

| query | before | after | Δ | query | before | after | Δ |
|---|---|---|---|---|---|---|---|
| Q1 | 146 | 150 | +2.7 % | Q12 | 194 | 183 | −5.4 % |
| Q2 | 192 | 194 | +1.0 % | Q13 | 468 | 484 | +3.5 % |
| Q3 | 200 | 192 | −4.0 % | Q14 | 83 | 82 | −1.8 % |
| Q4 | 169 | 174 | +3.0 % | Q15 | 40 | 40 | +1.3 % |
| Q5 | 354 | 369 | +4.1 % | Q16 | 68 | 68 | 0.0 % |
| Q6 | 75 | 75 | 0.0 % | Q17 | 94 | 89 | −4.8 % |
| Q7 | 255 | 272 | +6.9 % | Q18 | 1,520 | 1,514 | −0.5 % |
| Q8 | 220 | 234 | +6.4 % | Q19 | 102 | 105 | +2.4 % |
| Q9 | 763 | 778 | +2.0 % | Q20 | 620 | 624 | +0.6 % |
| Q10 | 180 | 172 | −4.2 % | Q21 | 3,831 | 3,196 | −16.6 % |
| Q11 | 130 | 146 | +11.9 % | **total** | **9,704** | **9,141** | **−5.8 %** |

SF 0.1 total 728 → 726 ms; every query within 0–2 ms except Q9 65 → 69 and
Q21 262 → 274 (load 5.4, so these are inside the noise).

The dynamic filters are visible on their own by flipping the session switch in
one binary (SF 1): Q21 **2,092 (on) vs 2,402 ms (off)**, −12.9 %; Q3 +1.6 %,
Q5 +5.0 %, Q7 +2.3 %, Q8 0.0 %, Q9 −1.7 %, Q18 +0.2 %, Q20 −0.8 %.

Write path (same two binaries, 10 runs, medians): `lineitem.csv`
**227 → 260 ms (+14.5 %)**, `orders.csv` **91 → 102 ms (+12.1 %)**; the
Parquet CTAS path and the durable family (single-row Sync inserts 7.78 → 7.76 s,
batched 63 → 60 ms, checkpoints 47–53 ms) are unchanged. The CSV cost is the
exact per-chunk zone and sketch fold over 16 columns (~5 ns/cell); folding
serially instead is worse (+36 %), and a sketch-only switch saved just 8 ms.
Keeping exact statistics and recording the miss is preferable to weakening
them, so the 5 % bound is met on every write path except CSV ingest, and
follow-up row 122a is opened for it.

Binary: default CLI 5,608,752 → 5,690,064 B (+1.4 %), all-features CLI
14,842,064 → 14,942,048 B (+0.7 %). No new dependencies.

Decisions taken from measurements:

- **Filters are priced, not unconditional.** Every TPC-H join with a build
  above 10k rows was slower with a filter (deriving it hashes the build, and
  the probe side usually cannot skip enough), so `filter_pays` only accepts a
  build of ≤ 10,000 rows. That is why Q21 is the only TPC-H winner; the others
  were left unfiltered by design rather than by accident.
- **The join-cardinality estimate was reverted.** Feeding
  `rows = l·r / max(ndv)` into `estimate_rows` made Q8 218 → 116 ms but Q7
  +41 %, Q21 +22.7 %, Q5 +16.7 %: a wrong build side costs more than an
  unestimated one, and the estimate also opened the filter gate for large
  builds. `estimate_rows` reports a join's rows only from shapes it knows.
- **Late materialization stays a follow-up (122b).** `project_scan_chunks`
  already copies only the projected columns, and the TPC-H projections are
  narrow, so a first implementation would not have paid for itself.

## Fix round (review)

The review found one leak: `pinkdb_column_stats()` binds to a constant
`LogicalPlan::Values`, and a plan with no scan names no table, so every read
policy in the engine — RBAC's read pass, `ServePolicy::deny_read_databases` and
`ServePolicy::read_databases` — could not see what the function had read. A
principal without `SELECT` on `secret` got its bounds from
`pinkdb_column_stats('secret')`, and an MCP session could read the agent's
memory bounds.

The fix makes the read set explicit instead of adding a special case:

- `LogicalPlan::Values` gained `sources: Vec<(String, String)>`, the catalog
  tables the rows were read from. A literal `VALUES` list has none; the catalog
  function fills it at bind time, and `collect_tables_read` inserts them, so
  RBAC, `deny_read_databases`, `read_databases`, the COMMIT read set and any
  future read check all see them with no further code. The price is that
  listing statistics inside a transaction counts as reading the listed tables
  for conflict validation — correct (the statement read their statistics) and
  the only observable behaviour change beyond the refusals.
- The bind-time filter uses `Session::principal()`: the named form refuses with
  the same `check_table` error a `SELECT` gets (`rbac::check_select`), and the
  all-tables form lists only the tables the principal may read. The trusted
  embedded session (no principal) is unchanged.
- Serve policies work on the bound plan, so the all-tables form is refused
  when it reaches a masked or non-allowed database rather than filtered: the
  policy is not visible at bind time, and refusing is the safe branch. The
  named form of a table inside the allowed database still works.
- Two policy checks tested the *syntax* before the plan (`ast_scans_files`:
  "any `FROM` function is a file source"). That is no longer true now that a
  catalog function is a `FROM` source, so `read_reason` and `external_reason`
  bind first and fall back to the syntax only when binding fails — which is
  exactly the case the syntax check existed for (a file source whose function
  is not installed). An installed file source is still refused by the plan's
  table-function check.
- Checked for other catalog functions: `pinkdb_column_stats` is the only one
  the binder intercepts. `SHOW TABLES`/`SHOW DATABASES`/`SHOW COLUMNS`/
  `SHOW MATERIALIZED VIEWS`/`SHOW MACROS`/`SHOW TRIGGERS` are their own plan
  nodes and return names only (no table contents or statistics), so they stay
  as they are.
- Tests: `column_stats_respect_select_privilege` (the reviewer's, plus a
  `GRANT` bringing the table back), `column_stats_respect_the_serve_read_policies`
  (masked database, both forms, plus an unmasked table still answering) in
  `crates/pinkdb/tests/stats.rs`, and
  `the_channel_reads_only_its_own_statistics` (the internal channel's
  allow-list, both forms, its own table still readable) in
  `crates/pinkdb/tests/internal_channel.rs`.

## Gates

The commands the task lists, run once at the end with `--no-fail-fast`:
`cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`,
`cargo test -q --workspace --no-fail-fast`, `cargo test -q -p pink-lake
--features catalog-sqlite,s3,encryption --no-fail-fast`, `python3
bench/duckdiff.py`, the TPC-H 22-query correctness check at SF 0.01/0.1, the
CI regression bench, and the wasm32 build.

## Status

- [x] plan written
- [x] stats core + persistence + ANALYZE + table function
- [x] cost model
- [x] zone maps + local scan skipping
- [x] dynamic join filters + session switch
- [x] measurements, docs, ROADMAP/CHANGELOG, website
- [ ] late materialization — deferred to row 122b after measurement
