# Wave 137 — incremental DuckLake materialized views and JS procedures in the server

Planned 2026-10-31, last wave of stream B, after 134 (MVs refresh by writing lake files).
QuackLab's marts are DuckLake materialized views refreshed after every ETL batch through JS stored
procedures (`staging.grain_refresh_mart`, `grain_refresh_stale_marts`, `grain_reset_staging*`).
The wave-130 benchmark refreshes all 55 views in full: 9 s locally and 14–71 s on the lake for
pinkdb, 1.4 s and 3–5 s for DuckDB. QuackLab's DuckDB refreshes most of them incrementally.

## What exists

- **The reference**: QuackLab's DuckLake port, `~/Desktop/projects/ducklake-vortex/ducklake`
  (`src/functions/ducklake_materialized_view.cpp`, 2,460 lines;
  `storage/ducklake_metadata_manager.cpp` for the `ducklake_materialized_view`,
  `_dependency`, `_refresh_history` and `ducklake_procedure`, `_parameters` tables, versions
  1.1/1.2/1.3), and its user guide `~/Desktop/projects/grain/grain/DUCKDB_2_MV_SP_GUIDE.md`
  (modes, staleness, history columns, the JS procedure contract).
- **QuackLab's callers**: `grain/go/internal/deploy/procedure_ddl.go` (the procedures it deploys),
  `grain/vps/internal/vpsetl/mart_refresh.go`, and the compiler's `refreshMartSetSql` pack in
  `bench/quacklab/plan/nyc_taxi_essential1.json`.
- **pinkdb**: `LANGUAGE JAVASCRIPT` procedures run in QuickJS (`crates/pinkdb-js`, waves 44/81)
  with a synchronous host API (`db.exec`, `db.query` returning rows as arrays, `db.value`),
  behind the `js` feature, which the server does not enable. No materialized views.

## Decisions

1. **Materialized views as DuckLake objects** in lake databases: `CREATE MATERIALIZED VIEW
   db.s.v AS …` and `ducklake_create_materialized_view(db, schema_name :=, view_name :=,
   query :=)`, stored in the port's tables (`ducklake_materialized_view`,
   `_dependency`) with a backing lake table, so QuackLab's DuckDB and pinkdb read each
   other's MVs. Catalogs move to the port's version (1.3) when an MV or procedure is created.
2. **Refresh modes as the port defines them** — `skipped` (no source change since
   `last_refreshed_snapshot`), `delta` (SUM/COUNT/AVG-derivable aggregates, applied from the
   sources' insertions and deletions between snapshots, as a `MERGE` into the backing table),
   `incremental` (changed group keys recomputed, MIN/MAX invalidations), `join_incremental`
   (fact-side changes of an eligible inner join), `full` — chosen by analysing the bound plan;
   the change feed is 132's `ducklake_table_insertions`/`deletions`. Ported, credited, never
   pasted.
3. **Functions**: `ducklake_refresh_materialized_view(db, schema_name :=, view_name :=,
   if_stale :=)` returning `(schema_name, view_name, refresh_mode, rows_refreshed)`,
   `ducklake_materialized_views(db)` (with `is_stale`), `ducklake_drop_materialized_view`,
   `DROP MATERIALIZED VIEW`, `ducklake_materialized_view_refresh_history(db)` with the guide's
   columns.
4. **QuackLab's JS procedure contract** on top of wave 81's engine: `CREATE [OR REPLACE] PROCEDURE
   s.p(args) RETURNS type LANGUAGE JAVASCRIPT AS $$…$$`, the body an async function with
   `duckdb.execute`, `duckdb.query` (rows as objects keyed by column name),
   `duckdb.transaction(callback)`, Promise-only, a private connection per call, the guide's value
   conversions, 64 MiB heap / 8 MiB stack, 16 nesting levels. Procedures in lake databases
   persist in `ducklake_procedure`. pinkdb's existing `db.*` API stays for local databases.
   The server build enables `js`.
5. **The benchmark's refresh** switches from the full `DELETE`/`INSERT` refresh to QuackLab's own
   `refreshMartSetSql` pack (`ducklake_refresh_materialized_view` per view) on both engines once
   DuckDB's side has an equivalent (stock DuckDB has no MVs: keep DuckDB on the full refresh and
   record that pinkdb's number is incremental).

## Briefs (pi)

- **137a** MV catalog objects, create/drop, full refresh, history, `ducklake_materialized_views`.
- **137b** refresh-mode analysis and the `delta` / `incremental` / `join_incremental` paths, with a
  differential test: every mode's result equals a full recomputation, over random batches.
- **137c** QuackLab JS procedure contract and the server `js` build; QuackLab's deployed
  procedures (`procedure_ddl.go`) run unchanged.
- **137d** benchmark rerun; interop: an MV QuackLab's DuckDB build created reads in pinkdb.

## Measured

Before (wave 130): refresh after the 50k append — local 9.2 s (DuckDB full 1.35 s), lake 62.8 s
(4.72 s); after the one-row correction — local 9.3 s, lake 71.0 s. After: to fill in.
