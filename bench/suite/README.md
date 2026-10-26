# Benchmark suite

One runner for the benchmark families, so a number from one wave can be
compared with the next: `run.py` (Python standard library only) plus the
`run.sh` wrapper.

```sh
bench/suite/run.sh                          # all families, TPC-H SF 0.1
bench/suite/run.sh --only tpch --scale 1    # the expensive query set
bench/suite/run.sh --only ci                # the small regression guard
```

DuckDB is the reference engine. The runner never installs a DuckDB
extension and never downloads anything: TPC-H data comes from DuckDB's
built-in `dbgen`, and both engines load the same Parquet files. Every
query answer is compared (row count and a checksum of the row multiset)
before it is timed.

## What it writes

- `results/<YYYY-MM-DD>-<shortsha>.json` — machine, commit, thread counts,
  load average, and every sample/median. This is the history.
- `results/<YYYY-MM-DD>-<shortsha>.md` — the same tables for review.

Stores and generated data stay under `--work-dir` (default
`$TMPDIR/pinkdb_bench_suite`), so re-runs are warm; delete that directory
to regenerate from scratch. TPC-H SF 1 needs about 1 GB for the two stores
and one GB for the Parquet files.

## Families

| `--only` | Measures |
|---|---|
| `tpch` | all 22 queries against DuckDB, per-query medians and ratios |
| `ingest` | CSV and Parquet load throughput (rows/s, MB/s) |
| `durable` | autocommit and batched inserts, WAL fsync policies, multi-process writers |
| `lake` | pink-lake attach/open/scan/commit for `pink:`, `sqlite:`, `pink+s3://`, time travel |
| `search` | FTS build/top-10 on 100k docs; HNSW build, recall@10, p50/p95 on 100k×768 |
| `footprint` | release binary sizes, CLI start-up, peak RSS |
| `ci` | TPC-H SF 0.01 queries, an insert micro-benchmark, binary size |

The `ci` family compares against the committed `baseline-ci.json` and fails
on a 2× query regression (or +5 ms), a 1.1× size increase, or a wrong
answer. Refresh it deliberately with `--only ci --update-baseline`.

## Flags

`--scale`, `--runs` (default 5), `--warmup` (default 1), `--threads`
(default: physical cores, capped at 8; applied to both engines),
`--skip-build`, `--regen` (new data), `--rebuild` (new stores),
`--durable-rows`, `--work-dir`.

`bench/suite/results/` is committed; `$TMPDIR/pinkdb_bench_suite` is not.
