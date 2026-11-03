# Wave 133 — streaming lake scans

Planned 2026-10-31, stream B of the QuackLab track, after wave 131 (schemas) lands on `main`.
QuackLab serves dashboards from a DuckLake on memory-capped Cloud Run instances. A pinkdb lake
database must therefore answer a query by reading only the files it needs, in parallel, holding
a bounded amount of data, never the whole table. Step 8 of `docs/performance-architecture.md`.
Wave 134 does the same for writes; the two share this wave's table model.

## What exists

- **Writable lake databases are materialized** (wave 124, `crates/pinkdb/src/lake_db.rs`,
  `pink-lake/src/database.rs`): opening one decodes the head version into store tables; every
  committed unit republishes through `LakeDatabase::publish` → `Lake::commit_from_tables`, a
  whole-table diff. The wave-130 baseline measures it: lake cold start 4.5 s vs DuckDB 0.12 s,
  refresh peak RSS 5.5 GB vs 1.1 GB, chart server RSS 2.2 GB vs 0.47 GB.
- **Read-only lakes are lazy** (`pink-lake/src/attach.rs`, `AttachedLake`, waves 76/98c):
  projected and zone-map-pruned loads, exact `COUNT`/`MIN`/`MAX` from statistics, and
  `scan_streamed`, but only for tables above `MATERIALIZE_MAX_ROWS` (64 chunks) inside an
  aggregate fusion; below that, and for every other plan shape, `load_filtered` materializes the
  table and caches it for the session. Files are decoded one at a time (row groups in parallel
  inside a file); pruning walks every file's stats in memory.
- **Stream C's 135** turns the executor into push-based pipelines with morsel parallelism; the
  lake scan should be a morsel source there. Order with 135 to be agreed through the parent.

## Decisions

1. **One table model for lake databases**, read-only and writable: a `LakeTable` catalog entry
   (schema, snapshot handle: data files with row counts, `row_id_start`, column stats, delete
   files, inlined rows) instead of rows in the store. Wave 134 adds the write half; until it lands,
   writable databases keep wave 124's materialization and only read-only ones switch.
2. **Every scan of a lake table streams**: the executor's `Scan` of a lake table is a source of
   chunks, whatever the plan above it (no 64-chunk threshold, no fusion requirement). The
   session-wide materialized cache goes; a small bounded cache of decoded column chunks (size from
   `SET lake_cache_size`, default 0 on the server) may keep hot dimension tables.
3. **Pruning in three steps**, ported from DuckLake's `ducklake_multi_file_list` and
   `ducklake_scan`: (a) files by `ducklake_file_column_stats` min/max/null counts — in memory for
   catalogs under a threshold, else a SQL query against the catalog (`WHERE table_id = ? AND
   column_id = ? AND max_value >= ?`); (b) Parquet row groups by their footer statistics; (c)
   partition values. The scan reports files and row groups read vs pruned in `EXPLAIN ANALYZE`.
4. **Parallel, bounded reads**: surviving files are split into row-group morsels and decoded by
   the worker pool (one in-flight row group per worker, prefetch of the next file's footer); chunk
   order is file order only when the plan needs it (an ordered scan or `LIMIT` without
   `ORDER BY` keeps order; aggregates and joins do not). Remote reads coalesce byte ranges like
   the httpfs Parquet reader.
5. **Row identity**: scans can project DuckLake's `rowid` (`row_id_start` + position; inlined
   rows carry their own) as a virtual column, which 134's DELETE/UPDATE/MERGE consume.
6. **Statistics answers stay**: `COUNT(*)`, `MIN`/`MAX` from exact stats, and file counts in
   `ducklake_table_info`.

## Briefs (pi)

- **133a** (pink-lake): a scan API over a snapshot — morsels (file, row-group range), catalog-side
  file pruning, row-group pruning, `rowid`, deletes applied per morsel. Tests against DuckDB-written
  lakes (132a fixtures) and pinkdb-written ones; memory bound test (scan a lake 20× larger than a
  cap with bounded RSS).
- **133b** (engine): `LakeTable` catalog entries and the streaming `Scan` source (morsel-driven,
  ordered when required) for read-only lake databases; the materializing `load*` paths deleted.
- **133c**: benchmark rerun (charts and cold start on the lake path); TPC-H SF 1 from a lake vs
  DuckDB attaching the same DuckLake.

## Added 2026-11-01 (top priority, from the parent session)

- **No whole-table decode on first touch**: `LakeAttachment::load` /
  `load_filtered` (`pink-lake/src/attach.rs`) and the session table cache go;
  every read is a streamed, projected, pruned scan.
- **Pruning down to pages**: catalog file statistics, then row-group footer
  statistics, then the **Parquet page index** (column index + offset index),
  which pinkdb's Parquet reader does not read yet — port DuckDB's page
  filtering (`parquet_reader.cpp`, `column_reader.cpp`), credited.
- **Projected columns only, ranged and coalesced reads** on R2/S3: the footer
  once per file version, then only the projected chunks' or pages' byte
  ranges, merged across small gaps; never a whole-object GET for a scan.
- **Parallel, streamed, bounded**: a fixed number of in-flight row-group or
  page-range morsels.
- **Metrics in `bench/quacklab/run.py` (lake mode), before and after**: lake
  attach time, first-query latency, bytes and requests read from object
  storage per query (pink-lake's `StorageStats`, surfaced in `EXPLAIN ANALYZE`
  and a function the bench reads), peak RSS. Baseline: the 132 emitted-lake
  numbers in `docs/wave-132-plan.md`.

## Gate

Full gate (lake/catalog code). Plus: TPC-H 22/22 equal to DuckDB from a pinkdb lake at SF 0.01 and
SF 1; the QuackLab bench's lake path agreeing with DuckDB.

## Measured

Interleaved A/B, TPC-H SF 1, 133b (`dd69c2f` on `841e3f8`) against `main` `841e3f8`, release CLIs
with the gate's features (`~/Desktop/projects/pinkdb-wt/ab-133b/`):

| storage | rounds | 133b / main (geomean) |
|---|---|---|
| Parquet pinkdb lake (`ATTACH 'lake:…' (READ_ONLY)`) | 5, under load | **0.372** (2.7× faster; Q14 4.3 → 0.55 s, Q10 4.5 → 0.82 s; only Q4 slower, 1.21) |
| store tables | 7, under load | 1.031 |
| store tables | 9, swapped, clean store | 0.924 (0.977 without the warm-up queries Q1–Q3) |
| store tables, both passes | — | 0.976; no query slower than 1.05 in both |

Still far from DuckDB on the lake: a single cold SF 1 pass reads the Parquet lake at 3–77× DuckDB's
time (its own store), against 2–7× from pinkdb store tables. Every lineitem scan decodes Parquet again
by design (no table cache); the next lake-read work is profiling where that time goes.

Before (wave 130, workaround lake path): cold start 4.51 s (DuckDB 0.118), chart server RSS at 16
clients 2.18 GB (0.47), refresh peak 5.5 GB (1.11). The QuackLab lake smoke after 133b: attach 1.31 s,
first query 1.29 s (DuckDB 0.10 / 0.12); refresh still 10–16× (the write side, wave 134).

Gates: full gate `tmerge133b` on `841e3f8` (86/90; the 4 lake failures were disk-full and contention,
rerun clean: `logs/rerun133b-lake-*.log`, EXIT=0), targeted gate `t133b` on `9529227` (18/19; the
Postgres interop failure was a time-zone flake in a wave-132 test, fixed in `ca2edae` and rerun
inside the failing window: `logs/t133b-pgfix.log`, EXIT=0).

## 133c — the lake read hot path (added 2026-11-03)

A `sample` profile of TPC-H SF 1 on a Parquet lake (133b binary) showed that most of a lake scan's
time was not Parquet decoding:

1. **`push_physical` (pink-lake) ≈ 85% of lake-scan CPU.** For every decoded chunk it filled each
   undecoded column with NULLs one `push_null` at a time, built a `DataTable` through `from_columns`
   (an exact HLL + zone-map pass over every column), had `project_table` build a second one (another
   stats pass), then re-split it with `scan_chunk`. Parquet decoding itself was under 10%.
2. **`exec::drain_source` decoded serially.** A plan node evaluated node by node (a join's build side
   under `probe_through_selection`, a sort's input) drained the lake on the calling thread: Q4 and
   Q17 spent ~92% of the query there while the pool idled.

Fixes (pi session `w133c`, brief `briefs/w133c.md`; review: `crates/pink-lake` only plus two generic
core pieces):
- `push_physical` returns the logical-width chunk with `DataChunk::projected`: decoded columns are
  shared, unrequested ones are zero-length placeholders, a column the file predates is its DEFAULT
  through one `Vector::constant` (a DEFAULT typed narrower than its column, `DECIMAL(10,2) DEFAULT
  1.5`, is coerced once, as the row-by-row fill did — a regression caught in review). No `DataTable`,
  statistics or re-split on the scan path; an entry without a schema record maps the file's columns
  one to one, as before.
- `drain_source` claims morsels on the rayon pool; each worker decodes into its own list, the lists
  merge by morsel index (the serial order, and the serial first error). No channel, no lock across a
  decode, nobody waits on another worker (133b's deadlock), correct from the main thread and from a
  worker.
- Tests: a core unit test (64 morsels, two call sites, a timeout), `crates/pink-lake-parquet/tests/
  projected_scan.rs` (projected reads across rename/add-column with DEFAULTs on a > 1-row-group table;
  a join over a drained lake scan equal to the same query on a store copy), `Vector::constant`
  coercion.

## Measured (133c)

Interleaved A/B, TPC-H SF 1, release CLIs with the gate's features
(`~/Desktop/projects/pinkdb-wt/ab-133c/`): A = `main` + clippy fix `3832368`, B = 133c.

| storage | rounds | 133c / main (geomean) |
|---|---|---|
| Parquet pinkdb lake (`ATTACH 'lake:…' (READ_ONLY)`) | 7, under load | **0.239** (4.2× faster; sum 10.7 → 2.1 s; every query faster: Q4 1.48 s → 55 ms, Q21 1.89 s → 122 ms, Q17 1.06 s → 78 ms; least gain Q13/Q16 ≈ 0.66) |
| store tables | 5, under load | 1.066 (Q17 2.07, Q19 1.61, Q13 1.33 — rerun with 11 rounds: 0.88, 1.00, 1.11, geomean 0.989; 133c touches no store path) |

Against DuckDB v2.0.0 (its own store; bench suite, one cold run each, SF 1): the Parquet lake is now
**2.2–7.5×** DuckDB's time (Q13 2.16 … Q2 7.5), down from 3–77× after 133b. 22/22 results equal.

Gates: full gate `t133c` on `7ef8edf` (on main `32c9a9d` + clippy fix): 89 steps pass, including every
DuckDB v2.0.0 interop step and the stricter VARIANT/ARRAY step. The one failure was the gate's own
Postgres sharing port 55433 with the interop tests' servers; the step is split and both halves pass
(`logs/t133c-pgfix.log`, 49 tests, no skips).

## 133d — dictionary pages, per-entry filters, open files (added 2026-11-03)

Profile basis: `~/Desktop/projects/pinkdb-wt/briefs/w133d-proposal.md`. Against DuckDB v2.0.0 reading the
**same** Parquet files (the fair baseline, `prof133d/vs_duck_parquet.py`), the 133c lake was 2.40×
(geomean); dictionary-encoded VARCHAR decoding was 22–45% of busy CPU (a refcounted `ByteArray` and a
`from_utf8` per value), reopening the data file for every range read 5–7%.

Changes (pi sessions `w133d` and, after the app crash, `w133d-c1`; brief `briefs/w133d.md`):
- `extensions/parquet/src/dictionary.rs`: a dictionary-encoded VARCHAR column chunk decodes its
  dictionary page once (UTF-8 checked once per entry), reads data-page ids with its own RLE/bit-pack
  decoder, and gathers ordinary pinkdb vectors (no dictionary vector in the engine, per the parent's
  condition). A chunk whose footer page-encoding mask shows PLAIN data pages (a dictionary that outgrew
  its page: high-cardinality text such as `l_comment`) keeps the old path. Without that check every such
  chunk was read twice (review caught it: plain string scan 1.44× slower; fixed, 0.98×).
- Pushed predicates on such a column are evaluated once per dictionary entry; failing rows are dropped
  from every column before materialization (after DuckDB's `DictionaryDecoder::InitializeDictionary`
  filter). The lake's row accounting subtracts dropped rows (pi found `+` there, which broke every
  filtered lake scan in the first draft).
- Generic `IN` pushdown: `PrunePredicate::in_list`, rebuilt from the parser's `OR` chain; store zone
  pruning, lake zones, the Parquet and Iceberg scans honour it.
- `LocalStorage` keeps ≤ 64 data files open and reads with `read_at`.

## Measured (133d)

Release CLIs with the gate's features (`~/Desktop/projects/pinkdb-wt/ab-133d/`), TPC-H SF 1, A = main
`0493148`, B = 133d. Data rebuilt by A (the `bench-ab` tree had been deleted by a cleanup).

| | result | target |
|---|---|---|
| lake A/B, 7 rounds interleaved | **0.888** geomean, no query slower (Q19 0.49, Q12 0.78, Q2 0.79) | — |
| store A/B, 7 rounds interleaved | **0.996** (max Q11 1.12 at 8 → 9 ms) | ≤ 1.03 ✓ |
| vs DuckDB on the same Parquet, all 22 | **1.71×** (was 2.40×); worst Q18 2.68, Q12 2.34, Q20 2.30 | ≤ 1.8 ✓ |
| Q12 vs DuckDB on the same Parquet | **2.34×** (was 6.31×) | ≤ 2.5 ✓ |
| dictionary-filter probe (`l_shipmode IN (…)`), 9 rounds interleaved with DuckDB | **1.40×** (14 vs 10 ms; main 29 ms) | ≤ 1.3 ✗ (by ~4 ms) |

The probe target was set when DuckDB took 23–29 ms on it; at 10–14 ms the per-query fixed cost
dominates. The remaining lake gap is the engine paths handed to stream C (ungrouped aggregate through
`GeneralSink`, serial join build, VARCHAR GROUP BY fold: numeric probe 2.1×, dictionary group 2.6×).

Gates: full gate `t133d` (90/91 steps; the workspace step's 236 "never executed" errors were test
binaries deleted mid-run by the build lock's cleanup after a concurrent baseline build launched from
this worktree, rerun alone: 2,878 passed, 0 failed, `logs/t133d-workspace-rerun.log`).
