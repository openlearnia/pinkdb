# QuackLab benchmark (NYC taxi)

Measures QuackLab's compiled workload (`plan/nyc_taxi_essential1.json`) on
pinkdb and DuckDB, on this machine, against the same files, one phase per CLI
process so the wall times are process wall times.

Per scenario (a batch from `~/Desktop/projects/grain/grain/dummy-data`):

| phase | what runs |
|---|---|
| setup | every schema and `CREATE TABLE` from `tableSpecs` (materialized views are tables here too) |
| ETL | `executionPlan` steps in order, `$inject` expanded from `runtimeSqlCatalog`, restricted to the tables in that batch's manifest, `$batchId` / `$batchType` / `s3://$rawBucket/$sourcePath` substituted |
| refresh | every `materialized_view` spec in `dependsOn` order: `DELETE` then `INSERT INTO … <selectSql>` |

Validation SELECTs run with the ETL; rows containing `fail` are recorded (not
fatal). After every refresh, `count(*)` of the 67 master/mart tables is compared
between engines. After the last scenario every widget the two engines can both
run is compared row by row (doubles to 6 significant digits; `ORDER BY … LIMIT`
widgets compare as returned, and an order difference that is only tie-breaking
at the limit is recorded as `tie_order`, not a mismatch).

Then charts, per storage: pinkdb through `pinkdb-server` + `examples/chart_load`
(client-side `Instant` timings) and DuckDB through one `quack_serve` process
with N `duckdb` clients (timings parsed from `Run Time` lines, so they include
the CLI and are quantised to 1 ms). One warm-up round, then `--rounds` of all
widgets in a seeded shuffle per client. Records per-request latency, p50/p95/p99,
throughput, server peak RSS (sampled every 100 ms) and per-widget p50 at one
client. Finally a cold start: a fresh CLI process opens the store / attaches the
lake databases and runs `kpi_totals`, median of 5.

## Prerequisites

- Python 3 (stdlib only), no pip packages.
- Docker running. The runner starts RustFS
  (`rustfs/rustfs:latest@sha256:9ca9559533b537d10482fe923288b676ad08f92a7069d9a17327fbae3cab688b`,
  bound to `127.0.0.1`) and removes it at exit. The first lake run pulls the image.
- PostgreSQL server binaries on `PATH`: `initdb`, `pg_ctl`, `createdb`, `psql`
  (tested with Homebrew PostgreSQL 18.4). The runner initdb's a throwaway
  cluster under the work dir, on `127.0.0.1:55433` with trust auth, and deletes
  it at exit. Nothing else on 55433 may be running.
- DuckDB CLI (tested: v2.0.0-alpha46057) with the `ducklake`,
  `postgres_scanner`, `quack` and `httpfs` extensions installed:
  `duckdb -c "INSTALL ducklake; INSTALL postgres_scanner; INSTALL quack; INSTALL httpfs;"`
- The data checkout at `~/Desktop/projects/grain/grain/dummy-data`
  (read only; override the checkout with `GRAIN` or `--grain <path>`), and at
  least 8 GB free disk. A small-sized run wrote ~3 GB of store and lake data on
  top of that.
- Release builds (from the repo root):

```sh
cargo build --release -p pinkdb-cli --features pinklake,s3,catalog-postgres,httpfs
cargo build --release -p pinkdb-server --features catalog-postgres,lake-s3 --example chart_load
```

  - CLI: `pinklake` = lake databases, `s3` = stage files from `s3://`,
    `catalog-postgres` = lake catalogs in Postgres, `httpfs` = `read_csv` over
    `s3://` (and its SigV4 transport, shared with `pink-lake/s3`).
  - Server: `catalog-postgres,lake-s3` opens the same wave-124 lake databases;
    local charts need no extra features.

## Run

```sh
python3 bench/quacklab/run.py [--size smoke|small|large] [--storage local,lake] \
    [--engines pinkdb,duckdb] [--clients 1,4,16] [--rounds 10] \
    [--work-dir DIR] [--keep] [--grain PATH] [--pink-cli PATH] \
    [--pink-server PATH] [--chart-load PATH] [--duckdb PATH]
```

Sizes are the batch sequences: `smoke` = smoke → 08-12 → correction (seconds),
`small` (default) = 2024-07 → 08-12 → correction (~2M trips), `large` = q1 → q2.
Storages: `local` (one `.pink` store / one `.duckdb` file) and `lake` (wave-124
lake database per schema on Postgres with data in S3, for pinkdb; a DuckLake
Postgres catalog with `s3://lake/duck/` data, for DuckDB). `--engines` must
include `duckdb` because the agreement checks compare against it.

Results go to `bench/quacklab/results/<date>-<shortsha>-<size>.json` and `.md`
next to it. The JSON has every phase's wall time, RSS, validation failures,
blocked/unsupported records and chart percentiles/throughput — under 100 KB, so
per-statement timings stay in `<work-dir>/logs/` and the raw per-request records
go to `<work-dir>/raw-latencies.json` (both deleted with the work dir unless
`--keep`). The `.md` is the summary
table view (ETL / refresh wall, chart p50/p95 per client count, cold start, peak
RSS, pinkdb vs DuckDB ratios, blocked table, dialect gaps).

The work dir defaults to `~/Desktop/projects/pinkdb-wt/bench/quacklab-work` and
is deleted at exit (also on Ctrl-C and exceptions) unless `--keep`. `--keep`
leaves the work dir, Postgres and the RustFS container running for inspection;
stop Postgres before the next run (the runner refuses to start when 55433 is
busy and keeps the work dir).

## Blocked and shimmed steps

pinkdb first gets every statement exactly as QuackLab emits it. On the first
error the engine is recorded as `{"status": "blocked", "by": "131"|"132",
"error": …}`: wave 131 for schema / three-part names (`CREATE SCHEMA`,
`r2_cat."s"."t"`), wave 132 for the DuckLake `ATTACH`. The recorded run then
uses the workaround:

- `local`: one `CREATE DATABASE` per schema, `r2_cat.` stripped.
- `lake`: one wave-124 lake database per schema
  (`CREATE DATABASE "staging" (TYPE LAKE, CATALOG 'postgres://…/pink_staging',
  DATA 's3://lake/pink/staging/')`), `r2_cat.` stripped.

If a workaround step fails, that phase is recorded as
`{"status": "failed", "error": …}` and the next engine/storage continues. Nothing
is reported as a number unless the phase ran.

Two dialect workarounds, both recorded in the JSON and the MD. Before the
measured phases the runner probes each emitted form once on a throwaway
workaround store, so a shim is applied only while pinkdb rejects that form; the
MD's "pinkdb dialect gaps" section lists the emitted statement and pinkdb's
error for each:

1. pinkdb does not parse `read_csv(..., types={...})`, `INSERT … BY NAME`, or
   `MERGE … INSERT BY NAME`. The stage loads drop the `types` map and cast every
   projected column to its `tableSpecs` type; the loads name their columns
   explicitly (from the emitted projection), the merge's `INSERT BY NAME` is
   expanded to explicit columns/values, and the mart refresh is `DELETE` +
   `INSERT INTO … (<DuckDB DESCRIBE output columns>) <selectSql>`.
2. DuckDB `quack_query` sessions start in `memory`, so the server's `USE r2_cat`
   does not apply. Chart SQL sent through `quack` has its two-part table refs
   qualified with `r2_cat.` (the same text runs unchanged on pinkdb).

The 12 widgets from `chartSql` are not all portable: on this engine the ones
pinkdb cannot parse or bind are listed per storage/engine as `unsupported`,
with their exact errors in the MD (measured: 7 of 12). Only widgets both engines
executed are timed, so the chart numbers compare the same work. Widgets with
`ORDER BY … LIMIT` can tie at the limit; the runner notes `tie_order` when the
returned order differs but the row multiset is identical.

The first batch's ETL reports ten `fail` validation rows on both engines: the
plan validates the master dimensions before `build_marts_phase` refreshes them,
so the empty-table check fails on the first batch and passes afterwards.
