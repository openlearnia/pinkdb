# Wave 98b — fast lake reads on object storage (plan)

Started 2026-10-22; **done** the same day. Pulled ahead by the user right after 98a ("we need to
make pinkdb/pinklakes faster"). Goal: pinkdb lakes on S3/R2 read like
DuckLake does — only the bytes a query needs, with concurrent range
requests — instead of downloading whole data files one at a time.

## Baseline (R2 `pinkdb-bench/lakes`, TPC-H SF1, median of 3, after 98a)

| | DuckLake (DuckDB 2.0.0) | pinkdb `.pink` lake | pinkdb Parquet lake | pinkdb Vortex lake (DOUBLE) |
|---|---|---|---|---|
| attach + `count(*)` | 0.07 s | 2.75 s | 3.25 s | 15.5 s |
| attach + Q6 | 3.9 s | 31.7 s | 22.9 s | 31.4 s |
| attach + Q1 | 3.4 s | 31.9 s | 24.3 s | 33.2 s |

DuckLake's catalog here is a local DuckDB file; pinkdb's catalog objects
live on R2 next to the data, so some catalog round trips are inherent. The
remote-Parquet path (98a) is already at parity with DuckDB (SF1 Q6 4.3 s
vs 3.9 s), so the gap is the lake read path.

## Where the time goes (from the code)

- `format::DataFile::read()` downloads the **whole object**
  (`Storage::read`); `LakeFormat::read(&DataFile)` decodes a whole file into
  a `PinkDb`. The `Storage` trait has no ranged read for object storage
  (`local_path` is `None` there: "no ranged reads in this build").
- **Parquet lake files**: whole download, no projection
  (`supports_projection() == false`).
- **Vortex**: decodes projections, but over S3 still downloads the whole
  object first; attach reads **footers one file after another**.
- **`.pink` data files**: no projected reader (a projection decodes the whole
  table).
- **Catalog open**: several sequential GETs (`lake.json`, `highest.json`,
  `catalog.json`, the snapshot …) — each a full round trip.
- **Encrypted lakes** seal each object as one AEAD container (`PNKLAKE1`), so
  ranged reads of plaintext are impossible for them; they keep whole-object
  reads (but can still use the parallel whole-object download below).

## Goals

1. **Measure first.** Transport counters on the lake's S3 storage (requests,
   bytes, network wait — reuse the `pinkdb-httpfs` counters 98a added if
   possible) and a diagnostic exposure (`AttachmentReport`, a debug/`EXPLAIN`
   line, or `.lake attached`) so request patterns are visible. Add mock-S3
   tests (`crates/pink-lake/src/mock_s3.rs`, as `tests/s3.rs` uses) that pin
   request counts for attach + `count(*)` and attach + a 2-column query.
2. **Ranged reads in `Storage`.** `len`/`stat` + `read_range(rel, offset,
   len)` for the S3 backend (`S3Client` range GET, retries as today) and the
   local backend (positioned read); the crypto wrapper refuses ranged reads
   of sealed objects (callers fall back to whole reads). Expose a ranged
   reader on `format::DataFile` for providers.
3. **Parquet lake files: projected, ranged, concurrent.** The Parquet
   provider (`crates/pink-lake-parquet`) decodes only the projected columns,
   fetching their column-chunk ranges with 98a's machinery (coalescing,
   bounded concurrent read-ahead, ordered decode) through an adapter from the
   lake's ranged `DataFile` to whatever `pinkdb-parquet`'s remote scan
   consumes (factor that scan's core so a lake data file can drive it; do
   not duplicate it). `supports_projection()` becomes true; row-group pruning
   from the scan's filters where the provider receives them.
4. **Vortex lake files: ranged.** Footers and projected segments read through
   ranged reads instead of whole downloads (Vortex reads through a
   positioned-read abstraction; implement it over the lake storage). Attach
   reads the footers of all files **concurrently** (bounded by the same
   parallelism setting).
5. **Whole-object reads in parallel.** Where a whole object is still needed
   (`.pink` data files, encrypted containers, small files), download it as
   several concurrent range requests (e.g. 8–16 MiB parts, bounded by
   `PINKDB_HTTPFS_PARALLELISM`) and reassemble — so even the `.pink` lake
   uses more of the link than one request stream does.
6. **Concurrency across files.** A table with several data files (and a query
   touching several tables) fetches its files concurrently, bounded, in
   deterministic order for results.
7. **Fewer catalog round trips on open/attach.** Issue the independent
   catalog-object GETs concurrently (or skip ones that are not needed for a
   read-only attach of the latest version), and reuse fetched objects within
   one open/attach. Never weaken the existing identity/consistency checks
   (`lake.json` / `highest.json` / snapshot agreement, encryption checks).
8. **Unchanged semantics:** every result identical (values and order) to
   today, all existing lake tests pass unchanged, errors keep their shapes,
   local lakes do not regress.

## Targets (R2, SF1, same machine)

Parquet lake attach + Q6 ≤ 6 s; attach + `count(*)` ≤ 1.5 s; Vortex lake
attach + `count(*)` ≤ 3 s; `.pink` lake attach + Q6 clearly below 31.7 s
(bandwidth-bound: the whole file is still read).

## Tests

Mock-S3 request-count tests per goal (ranged Parquet projection, concurrent
Vortex footers, parallel whole-object download, concurrent catalog reads),
equality tests (ranged vs whole reads, every type, multi-file tables,
deletes/inlined rows, partitions, encrypted lakes falling back), the existing
`pink-lake`, `pink-lake-parquet` and CLI lake suites, the format contract
test (wave 88) for every provider.

## Benchmark (reviewer, not the agent)

`/tmp/r2bench/lakes.py` before/after against DuckLake; numbers into the
ROADMAP.

## Gates

`cargo test --workspace`; pink-lake `s3` / `s3,vortexlake` / `s3,encryption`
/ `s3,vortexlake,encryption` / `vortexlake`; pink-lake-parquet; CLI
`vortexlake,s3` / `vortexlake,s3,encryption` / `pinklake` / `pinklake,s3` /
`parquetlake`; clippy on both toolchains; fmt; wasm32 core build; duckdiff.

## Rules for delegated agents

Do not commit, push or create branches. Touch only the files the task
names. Use `CARGO_INCREMENTAL=0`. Do not use any cloud credentials (mock S3
and local files only). Report what changed, the request counts before/after
from the mock-S3 tests, and every check.

## How it was built and what it measured

Delegated to OpenCode (DeepSeek V4.1 Flash). The first run lost its API
connection mid-way; its CLI exited but the session **kept running on the
`opencode serve` backend**, so a continuation run worked alongside it for a
few minutes until the continuation was stopped. The original session then
finished the wave; reviewed here (encryption fallback, ordered concurrent
decode, catalog checks unchanged).

What landed: `Storage::read_range` / `ranged_reads` (local positioned reads,
S3 range GETs; keyed sessions refuse ranged reads of sealed objects so
encrypted lakes fall back to whole-object reads), transport counters
(`Lake::transport_stats`, `AttachmentReport`, a CLI `Transport:` line),
`DataFile::read_range` / `range_source` for providers, the 98a scan core
factored into `extensions/parquet/src/ranged.rs` (transport-agnostic
`RangeSource`), the Parquet lake provider on it (`supports_projection`,
projected column-chunk ranges, pruning), Vortex footers and segments read by
range with attach prefetching all footers concurrently, `map_bounded`
(bounded, order-preserving parallel map) for multi-file tables, parallel
part downloads for whole objects, fewer sequential catalog reads on open.
`crates/pinkdb-cli/tests/lake_s3.rs` changed one assertion (the CLI's
"no ranged reads" note is now a transport line).

Mock-S3 request counts (small lake, 2-column query): Parquet 1 request /
569 KB → 7 requests / 223 KB; Vortex 2 / 722 KB → 5 / 315 KB; Vortex attach
363 KB → 68 KB — more, smaller requests, much less data.

**R2, TPC-H SF1 lakes, median of 3:**

| | DuckLake | `.pink` lake | Parquet lake | Vortex lake (DOUBLE) |
|---|---|---|---|---|
| attach + `count(*)` | 0.07 s | 2.75 → **1.84 s** | 3.25 → **1.87 s** | 15.5 → **2.22 s** |
| attach + Q6 | 1.9 s (3.9 s in the earlier run) | 31.7 → **20.4 s** | 22.9 → **6.2 s** | 31.4 → **4.5 s** |
| attach + Q1 | 2.05 s (3.4 s earlier) | 31.9 → **19.5 s** | 24.3 → **6.2 s** | 33.2 → **4.5 s** |

The `.pink` lake stays whole-file by layout (wave 102 replaces it). The
remaining `count(*)` gap is largely the catalog living on R2 versus
DuckLake's local catalog file.
