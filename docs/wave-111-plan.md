# Wave 111 — benchmarking and performance tuning (plan)

Started 2026-10-26. Every fifth wave is a performance wave; the last was
wave 55 (2026-10-16). Since then the engine gained a database-backed lake
catalog (103, S3 catalog in 110), `VARIANT` (101), triggers (100), PK/UNIQUE
(95b), the dialect batches (95), fixed `ARRAY` (107a), FTS/BM25 (108a), HNSW
(107b/d), embeddings (107c), a new WAL format (107c), the MCP server and the
agent (105/106/106b), and the agent-memory extension (109). None of that has
been measured as a whole.

This wave produces (1) a committed, reproducible benchmark suite,
(2) profile-driven tuning with before/after numbers, and (3) published
results plus a CI regression guard. **No new features.** Where a benchmark
exposes a missing capability, it is recorded as a follow-up, not built.

## Why a suite, not one more script

The earlier rounds each carried their own script (`wave95*_perf`,
`lake_attach_bench.py`, `mp_*`) and their numbers live in ROADMAP prose.
That makes regressions invisible and comparisons across waves impossible.
One runner with one result format fixes that: the JSON is the history, the
Markdown summary is the review artefact, and the CI job reads the same
format at a smaller scale.

## Decisions

1. **Runner**: `bench/suite/run.py` (Python 3 standard library only) with a
   thin `bench/suite/run.sh`. Python keeps the harness independent of the
   engine it measures and matches the existing `bench/duckdiff.py`.
   Selection: `--only tpch,ingest,…` (repeatable/comma list), `--scale`,
   `--runs`, `--warmup`, `--threads`, `--duckdb PATH`, `--pinkdb PATH`,
   `--work-dir`, `--skip-build`, `--regen`, `--rebuild`,
   `--update-baseline`. `--only ci` is the small guard (below).
2. **Oracle**: the local DuckDB CLI (`/tmp/duckdb-2.0.0/duckdb`, v2.0.0).
   No extension is installed and nothing is downloaded. Anything that needs
   `INSTALL` (DuckDB `fts`, `vss`) is out of scope and listed as a
   follow-up pending the user's approval.
3. **Data**: TPC-H is generated once with DuckDB's built-in `dbgen`
   (`CALL dbgen(sf=…)`; no `INSTALL`). Every table is written to Parquet
   with explicit types, and *both* engines load that same Parquet into a
   native table. No CSV round-trip for the query benchmark, so type
   inference cannot differ between engines. The Parquet files are also the
   ingest source, so ingest and query see identical bytes.
4. **Stores**: the suite builds `tpch_sf<scale>.pink` and
   `tpch_sf<scale>.duckdb` once per (scale, commit) under `--work-dir`
   (default `$TMPDIR/pinkdb_bench_suite/tpch_sf<scale>/`), then reuses
   them. Build time is
   reported as ingest, never folded into query time.
5. **Timing**: for each query, one process per engine loads the store,
   runs the query `warmup` times untimed, then `runs` times with the CLI's
   `.timer on`. `Run Time (s): real X` is parsed; the reported value is the
   median. Process start and store open are outside the timed region. This
   is warm-cache (the store is in the OS page cache and, for pinkdb, opened
   once) and the same for both engines. Cold-open numbers are measured
   separately in the footprint/lake families.
6. **Threads**: both engines get the same count via `--threads`
   (default: physical cores, capped at 8). pinkdb's pool is the rayon pool
   (`RAYON_NUM_THREADS`); DuckDB gets `SET threads=<n>`. The JSON records
   both settings and the environment override that won.
7. **Honesty**: before a timed family the runner samples the load average,
   records it per run, and refuses (unless `--force`) when a `cargo`/`rustc`
   build or another `pi` benchmark process is running. Every result records
   the machine, git commit, whether the tree was dirty, rustc version,
   DuckDB version, and the scale.
8. **Correctness**: every TPC-H answer is checked, not just timed. The
   suite runs each query once per engine with full output (CLI row cap
   lifted), compares row counts exactly and a canonical checksum of the
   row multiset (strings/dates exact, numbers parsed and rounded to 6
   significant digits). A mismatch marks the query `mismatch` and fails the
   family; it is never silently timed.
9. **Results**: `bench/suite/results/<YYYY-MM-DD>-<shortsha>.json` holds
   everything machine-readable; the runner also writes the sibling `.md`
   summary from the same data. Committed results are the baseline history.
10. **Mock S3**: a stdlib Python server (`MockS3` in `run.py`) implements
    only the operations pinkdb's S3 client uses: `ListObjectsV2` (prefix,
    delimiter, paging), `GET` with `Range`, `HEAD`, `PUT` with
    `If-None-Match`/`If-Match`, `DELETE`. It ignores SigV4 (local throwaway
    only) and never talks to a network. Used for the `pink+s3://` catalog
    numbers. No real credentials.
11. **Profiling**: use `samply`/`cargo flamegraph` only if already on PATH;
    otherwise macOS `sample <pid>` / `xcrun xctrace`. No tool is installed.
12. **CI**: `bench/suite/run.py --only ci` is the small guard: TPC-H SF 0.01
    (a few queries), micro-benchmarks, and the footprint sizes, compared
    against a committed baseline with a 2× threshold. It is a separate,
    non-blocking workflow job; it flips to required only after it has been
    stable.

## Families and what each answers

| Family | Default | What it measures |
|---|---|---|
| `tpch` | SF 0.1 (SF 1 via `--scale 1`) | 22 queries vs DuckDB, warm cache, medians, correctness |
| `ingest` | SF 0.1 tables | CSV and Parquet load rows/s and MB/s, both engines |
| `durable` | 2k inserts, 3 checkpoints | autocommit inserts/s, batched inserts, Sync/Batch/Off WAL policies, multi-process writers |
| `lake` | SF 0.1 lineitem | pink-lake Parquet scan, commit latency, open time for `pink:`, `sqlite:`, `pink+s3://`, time travel |
| `search` | 100k docs / 100k×768 | FTS build + top-10; HNSW build 1 vs all threads, recall@10, p50/p95 |
| `footprint` | release builds | binary sizes (default/per-feature/server/extensions), CLI start-up, peak RSS for Q1/Q9/Q10 (Q18 is unsupported) |
| `ci` | SF 0.01 | the small regression guard |

## Tasks

Part 1 — suite

- [x] `bench/suite/run.py`, `run.sh`, `README.md` (the mock S3 is in
      `run.py`).
- [x] TPC-H data generation + dual load + 22-query run + correctness check.
- [x] Ingest, durable, lake, search, footprint families.
- [x] JSON + Markdown output; full runs at SF 0.1 and SF 1.

Part 2 — tuning

- [x] Baseline full run; compare with the ROADMAP's earlier numbers.
- [x] Profile the worst gaps (macOS `sample`; no profiler installed).
- [x] Fix in payoff order; each with before/after, an unchanged-results
      test, and a ROADMAP note: comma-join hash conversion + join
      reordering, OR-of-conjunctions filter derivation, IN-subquery set
      capacity, single-flight subquery memoisation.
- [x] Fix the `pink_lake_corrupt_*` scratch-dir leak (Drop guard; clean up
      on panic) and delete the leaked dirs.

Part 3 — publish and guard

- [x] `docs/benchmarks.md` (method, machine, tables, honest discussion).
- [x] Website `benchmarks` page + nav link + regenerated site.
- [x] CI regression job and committed baseline.
- [x] ROADMAP: 111 done, headline numbers, every-fifth-wave cadence restated.

## Tuning candidates (measured before touching)

From the existing notes and the feature work, the likely gaps are:

1. **TPC-H aggregate/scan path** — Q1/Q6 at SF 1 are the simplest scan
   shapes; 107a/107b left A/B numbers (10M-row lineitem: Q1 ≈ 0.123 s,
   Q6 ≈ 0.012 s) that the suite can put next to DuckDB.
2. **Join-heavy queries** — Q9/Q18/Q21 exercise the hash join, the
   optimizer's build/probe choice, and spilling.
3. **CSV/Parquet ingest** — parallel decode and per-batch overhead.
4. **WAL fsync** — the durable bench's Sync policy is fsync-per-commit by
   design; the suite should show the cost curve rather than "fix" it.
5. **Lake open** — the catalog is a database now (103/110): count the
   queries/GETs an open and a one-row commit cost.
6. **FTS build** (12.3 s / 100k docs in 108a) and **HNSW build**
   (30.4 s / 100k×768 all threads) — check for regressions since.
7. **Footprint** — fts added +3.1%, vss +86 KB (109); re-measure after
    all the waves.

Each candidate is only touched if the suite shows it is both large and
fixable without new features. No query-text special cases, no benchmark-only
code paths.

## Non-goals

- No DuckDB `fts`/`vss` comparison (needs `INSTALL`).
- No network, cloud provider, dataset download, or real credentials.
- No new dependencies; no new SQL surface.
- No change to the on-disk format or defaults unless a measurement shows a
  clear regression with a safe fix.

## Risks

- The machine is shared with other agents; a timed run needs a quiet
  window. Load average is recorded with every result and noisy runs are
  discarded, not averaged.
- SF 1 needs ~1 GB per engine plus the release build; disk is checked
  before each family and generated data is deleted afterwards.
- Absolute numbers are machine-specific; ratios and the JSON history are
  the durable output, the Markdown tables name the machine.

## Results

Measured on the SF 0.1 run of 2026-10-26 (`bench/suite/results/2026-10-26-2c7ee92.json`)
and the SF 1 run beside it (`…-sf1.json`). Eight threads for both engines,
warm cache, median of five.

- TPC-H SF 1: geometric mean **32.0×** DuckDB's time over the same 15 queries
  measured before tuning (33.1×), or **29.0×** over all 16 supported queries
  (Q16 was cut by the old CLI display cap and is now compared in full). Q9
  100.9 s → 0.66 s, Q8 11.7 s → 0.29 s, Q7 2.35 s → 1.20 s. Q1/Q6/Q13 stay
  12–26×, Q18 198× and Q19 246×.
- Peak RSS at SF 0.1: Q1 313 MB, Q9 674 MB, Q10 247 MB (Q9 materializes the
  largest join output); CLI start-up 5.2 ms.
- TPC-H SF 0.1: Q7 219 → 114 ms and Q9 451 → 60 ms, but the geometric mean
  moved 7.4× → 8.6×: reverting the memo put Q18 back at 76 ms (36 with it),
  and Q15/Q19 sit near the 1 ms timer where a millisecond swings a ratio.
- Ingest: CSV 2.07 M rows/s vs DuckDB 1.66 (faster), Parquet 1.20 vs 5.41
  (4.5× slower).
- Durable: 113k rows/s autocommit, 363k batched, `Sync` 2000 rows 8.32 s vs
  `Batch` 63 ms, two writers 0.79×.
- Lake: attach 20.1 ms (`pink:`), 2.2 ms (`sqlite:`), 85 ms (`pink+s3://`, mock);
  scan 14 ms after attach.
- Search: FTS build 824 ms / query <1 ms on 100k docs; HNSW 100k×768 117.3 s
  on one thread, 24.0 s on eight, recall@10 0.961 at ef 128.
- Footprint: default CLI 5,437,168 B (wave 109: 5,382,768 B, +1.0%); the
  measured feature set 14,668,944 B.

What the tuning changed:

1. Comma joins were planned as `NestedLoopJoin cross` with a filter above, so
   every TPC-H query that used `FROM a, b WHERE a.k = b.k` built the cross
   product. The optimizer now extracts equi-conjuncts into a hash join and
   keeps non-equi residuals above it (`optimizer.rs`, `tests/comma_join.rs`,
   unit test in `optimizer::tests`).
2. Disconnected comma chains (part × supplier before lineitem) are reordered
   by an equality graph, with a transparent restore projection so ancestors
   keep their column positions; only chains whose original order has a cross
   are touched.
3. A filter that is an `OR` of conjunctions now also derives per-column
   filters (Q7's `n1 = 'FRANCE' AND n2 = 'GERMANY' OR …` implies
   `n1 IN (…)` and `n2 IN (…)`), pushed to the scans, with the original OR
   kept above (`optimizer::tests::or_of_conjunctions_derives_per_column_filters`).
4. The `IN` subquery set reserves its capacity from the row count
   (8.8 s → 5.8 s on a focused probe).
5. A single-flight subquery memo was tried and **reverted**: it halved Q18 at
   SF 0.1 but deadlocked once at SF 1, and a possible hang is worse than the
   milliseconds it saved. Follow-up rather than shipped.
6. The pink-lake corruption test now owns its four scratch directories through
   a `Drop` guard, so a panic cleans up too; the ~1,500 already-leaked
   `$TMPDIR/pink_lake_corrupt_*` directories were deleted.

Not done: a cost-based join enumerator and build-side selection (the next
large win), safe subquery single-flight, DuckDB `fts`/`vss` comparison (needs
`INSTALL`), and scan/aggregate micro-optimizations for Q1/Q6.
