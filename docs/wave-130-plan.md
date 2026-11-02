# Wave 130 — the QuackLab benchmark

Planned and started 2026-10-31, the first wave of the QuackLab track's
stream B (130 → 132 → 133 → 134 → 137). pinkdb is to replace the custom
DuckDB 2.0 build under QuackLab, so every later wave in the track is measured
on QuackLab's own workload rather than on TPC-H alone. This wave builds that
measurement; it changes no engine code.

## What exists

- **QuackLab's harness measures nothing locally.**
  `grain/scripts/benchmarks/duckdb-platform/` (`workload.json`,
  `nyc_taxi_essential1_platform_v0`) names nine scenarios (initial ingest,
  refresh unchanged / append / update, filtered dashboard aggregates,
  multi-widget fan-out, worksheet join, bounded result, bulk export), but its
  `run.sh` only emits a capability matrix and its hosted runner times
  `SELECT 1`.
- **The workload is fully generated.** QuackLab compiles a model
  (`fixtures/shared/models/model_nyc_taxi_essential1.json`) into table specs,
  a runtime SQL catalog (stage loads from `read_csv` on R2, `MERGE` from
  staging to ingest, validation queries) and an execution plan; master
  tables and marts are DuckLake materialized views whose `selectSql` the
  compiler emits; dashboard widgets are semantic queries compiled to SQL on
  every request (`internal/compiler/semantics.CompileQuery`).
- **Pre-built batches exist.** `grain/dummy-data/batch-trips-2024-*` hold the
  NYC-taxi aggregates as `csv.gz` plus `manifest.csv` /
  `manifest_incremental.csv`, in QuackLab's upload contract: smoke (48k fact
  rows), 2024-07 (1.87M), q1 (5.5M), q2 (6.2M), plus a 50k append (08-12)
  and a one-row correction batch.
- **pinkdb runs the shapes with workarounds.** Wave 129 made all 114 golden
  statements run once schemas become databases and the `r2_cat.` prefix is
  stripped. Schemas are wave 131 (stream A); `ATTACH 'ducklake:…'` is wave
  132. A wave-124 lake database lives entirely in memory.
- **Precedent.** `bench/suite/run.py` (stdlib Python, machine info, medians,
  DuckDB as the reference) and `crates/pinkdb-server/examples/
  remote_client_bench.rs` (client timings against pinkdb-server, written to
  compare with DuckDB's quack server).

## Decisions

1. **The workload comes from QuackLab's compiler, not from hand-written
   SQL.** `bench/quacklab/compile/main.go` calls QuackLab's
   `physical.BuildTableSpecs`, `BuildRuntimeSqlCatalog`,
   `BuildExecutionPlan`, `semantics.EmitCatalog` and `CompileQuery`. It runs
   through `go run -overlay`, so QuackLab's checkout is never written. The
   output, `bench/quacklab/plan/nyc_taxi_essential1.json`, is committed with
   the QuackLab commit it came from; the runner needs neither Go nor the
   QuackLab tree except for the batch data.
2. **Twelve dashboard widgets** (`bench/quacklab/charts.json`): KPI totals,
   time series with date ranges, month and week buckets, top-N, filters
   (equals, `IN`, `startsWith` → `ILIKE`), a rollup over the widest mart.
   They are semantic queries; the SQL is whatever QuackLab's compiler emits.
3. **Scenarios, in order:** `initial` (full batch: 2024-07 by default), then
   `append` (08-12, incremental manifest), then `correction` (one updated
   fact row). Each runs QuackLab's ETL plan, then the mart refresh. Sizes:
   `--size smoke|small|large` (smoke batch / 2024-07 / q1 then q2); `small`
   is the default and the recorded size, chosen for the disk.
4. **Mart refresh is a full refresh, the same on both engines.** Stock
   DuckDB has no `ducklake_refresh_materialized_view` (QuackLab's build adds
   it; wave 137 brings it to pinkdb). Every materialized view (master dims and
   facts, mart dims, marts) is a table refreshed in dependency order with
   `DELETE FROM t; INSERT INTO t BY NAME <selectSql>`, the shape QuackLab's
   older native plans emitted.
5. **Two storages.** `local`: DuckDB on one `.duckdb` file, pinkdb on one
   `.pink` store. `lake`: QuackLab's production shape, a Postgres catalog on
   port 55433 (throwaway `initdb` under `$TMPDIR`) and S3 data. The S3 server
   is **RustFS** in Docker (the image already on this machine and in
   `crates/pink-lake/tests/s3_docker.sh`; it verifies SigV4): MinIO is not
   installed and pulling it would be a download. Raw batch files are
   uploaded to a `raw` bucket and read with `read_csv('s3://…')` like
   QuackLab's ETL reads R2.
6. **pinkdb runs as emitted first, then with the recorded workaround.**
   For each storage the runner first tries QuackLab's statements unchanged
   (`CREATE SCHEMA`, `r2_cat."staging"."T"`, `ATTACH 'ducklake:…'`) and
   records the first refusal as `blocked by 131` / `blocked by 132` with
   pinkdb's message. It then runs the wave-129 workaround (one database per
   schema, `r2_cat.` stripped; in `lake`, one wave-124 lake database per
   schema with its own Postgres catalog) and labels those numbers as such.
   When 131 and 132 land, the emitted path starts running by itself.
7. **Chart concurrency is a server with N client sessions,** the way
   QuackLab serves dashboards (one engine process, a connection per
   request): pinkdb-server with a Rust load client
   (`crates/pinkdb-server/examples/chart_load.rs`, one `Client` per thread),
   and DuckDB's own server, `quack_serve`, with N DuckDB CLI clients calling
   `quack_query`. Both report client-observed latency per request; the
   DuckDB number includes its client CLI, which favors pinkdb slightly and
   is noted in the results. 1, 4 and 16 clients; every client runs the
   twelve widgets in a seeded shuffled order for a fixed number of rounds
   after one warm-up round; p50 / p95 / p99 and throughput are recorded.
8. **Results must agree.** Mart row counts and every widget's result
   (rounded doubles, order-insensitive unless the widget orders) are
   compared between the engines; a mismatch fails the run.
9. **Cold start and memory.** Cold start: a fresh process opening the
   storage (attaching the lake) and answering the KPI widget. Peak memory:
   maximum RSS of each batch process (`/usr/bin/time -l`) and of each server
   during its chart run (sampled).
10. **Machine info** is recorded with every result (reusing
    `bench/suite/run.py`'s probe), with the load average before and after.

## Measured

Baseline, 2026-10-31, `--size small` (2024-07: 1.87M fact rows, then the 08-12
append and the one-row correction), pinkdb at f4bf44b (wave 129) against DuckDB
v2.0.0-alpha46057, Apple M4 / 16 GB. The run started on a quiet machine (load
2.4) and ended at load 9 (another stream began compiling); pi's earlier run
under load 6–8 agreed within noise. Full tables:
`bench/quacklab/results/2026-10-31-f4bf44b-small.md`. pinkdb ran the workaround
path on both storages (emitted path: blocked by 131 locally, 132 on the lake).

| step | local pinkdb / DuckDB | lake pinkdb / DuckDB |
|---|---|---|
| ETL, initial 1.87M rows | 37.6 s / 1.87 s (20×) | 44.6 s / 2.97 s (15×) |
| ETL, 50k append | 12.5 s / 0.27 s (47×) | 35.2 s / 1.78 s (20×) |
| ETL, one-row correction | 10.5 s / 0.05 s (199×) | 19.0 s / 0.58 s (33×) |
| mart refresh (55 views), initial | 9.2 s / 1.49 s (6.1×) | 14.5 s / 3.27 s (4.4×) |
| mart refresh, after append | 9.2 s / 1.35 s (6.8×) | 62.8 s / 4.72 s (13×) |
| mart refresh, after correction | 9.3 s / 1.42 s (6.6×) | 71.0 s / 4.09 s (17×) |
| charts, 1 client p50 / p95 ms | 0.19 / 7.4 vs 1 / 6 | 0.23 / 8.6 vs 3 / 9 |
| charts, 16 clients p50 / p95 ms | 2.3 / 79 vs 5 / 26 | 3.0 / 96 vs 12 / 117 |
| charts, 16 clients req/s | 877 vs 743 | 641 vs 393 |
| cold start (`kpi_totals`) | 0.081 s vs 0.019 s | 4.51 s vs 0.118 s |
| peak RSS, ETL / refresh | 4.6 / 5.4 GB vs 0.76 / 0.85 GB | 3.7 / 5.5 GB vs 1.27 / 1.11 GB |
| chart server RSS, 16 clients | 1.41 GB vs 0.29 GB | 2.18 GB vs 0.47 GB |

Counts of all 67 master/mart tables and the rows of every widget both engines
ran agree. Chart latencies below a millisecond compare the clients, not the
servers (DuckDB's go through its CLI at 1 ms resolution); throughput at 16
clients is the server comparison.

What the numbers say, for the waves after this one:
- **Writes are the gap.** A one-row MERGE batch costs 10.5 s locally: every
  statement in a phase process opens the whole store into memory (2.4 GB RSS
  for that batch) and pinkdb's MERGE/DELETE over the 1.87M-row ingest table is
  slow. On the lake, every committed statement republishes the wave-124
  in-memory lake (134 replaces this).
- **Lake opens materialize everything**: 4.5 s cold start and 5.5 GB refresh
  RSS (133: streamed, pruned scans).
- **Refresh after small changes costs a full refresh** on both engines here;
  137's incremental views are measured against these rows.
- **Dialect gaps** (parser/binder; not stream B's to fix): `INSERT … BY NAME`,
  `MERGE … INSERT BY NAME`, struct literals (`read_csv(types={…})`),
  `INTERVAL 1 DAY`, `ILIKE … ESCAPE`, and `GROUP BY date_trunc(…, "DAY_ID")`
  when the select alias is also `DAY_ID`. The last three keep 5 of the 12
  widgets off pinkdb. The runner probes each one and stops shimming it once
  pinkdb accepts it.
- The DuckDB quack server ignores SIGINT; the runner ends it by closing its
  stdin.

## Follow-ups

- Rerun after 131 (schemas: the emitted local path), 132 (DuckLake attach:
  the emitted lake path, same catalog as DuckDB), 133, 134 and 137
  (incremental refresh against the full refresh measured here).
- QuackLab's remaining scenarios (`worksheet_join_sort`, `bulk_export`,
  `alongside_etl`) once the core numbers are stable.
