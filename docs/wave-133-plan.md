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

## Gate

Full gate (lake/catalog code). Plus: TPC-H 22/22 equal to DuckDB from a pinkdb lake at SF 0.01 and
SF 1; the QuackLab bench's lake path agreeing with DuckDB.

## Measured

Before (wave 130, workaround lake path): cold start 4.51 s (DuckDB 0.118), chart server RSS at 16
clients 2.18 GB (0.47), refresh peak 5.5 GB (1.11). After: to fill in.
