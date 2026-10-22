# Wave 98a — fast remote Parquet reads (plan)

Started 2026-10-22; **done** the same day. Pulled ahead of the queue by the user ("these are
priority") after the first R2 baseline. The rest of wave 98 (CSV and lake
data files through the read-ahead pool, the remote-file cache, scan
streaming) stays queued as 98b.

## Baseline that triggered it (R2 bucket `pinkdb-bench`, same machine and network)

| query (SF1 `lineitem.parquet`, 196 MB, 49 row groups) | DuckDB 2.0.0 | pinkdb |
|---|---|---|
| TPC-H Q6 | 3.4–4.8 s | **406 s** (CPU 3.2 s: I/O-latency bound) |
| `SELECT count(*)` | 0.41 s | **29.5 s** |

DuckDB answers `count(*)` from footer row counts and reads Q6's four
columns with large, concurrent range requests. pinkdb's remote Parquet
reader (`extensions/parquet/src/remote.rs`) streams each column chunk
through `RangedRead`, one **serial 128 KiB** `Range` request at a time
(`READ_BLOCK`), so a query pays one round trip per 128 KiB of every column
chunk it decodes. A single 128 KiB range GET to this bucket takes roughly
0.1–0.15 s from this machine (edge RTT ~7 ms).

## Goals

1. **Measure first.** Add per-scan transport counters (requests, bytes,
   time waiting on the network; at least for the Parquet source, through
   its existing `.parquet` counters / `TableScanOutput` counters) and use
   them to confirm the request pattern before and after. Confirm the scan
   receives the projection for Q6's shape (Filter + Aggregate over
   `read_parquet`) — `ScanProjections` should bound it to 4 columns — and
   make `EXPLAIN` show the projected width (e.g. `(4 of 16 columns)`)
   instead of only the schema width.
2. **`count(*)` from metadata.** A `count(*)` (and `count(*)` with only
   prunable filters that eliminate whole row groups — optional) over
   `read_parquet` with no other column referenced is answered from the
   footer's row-group row counts: zero data-page requests. Same for local
   files.
3. **One request per needed byte range, not per 128 KiB.** For each row
   group that survives pruning, compute the byte ranges of the projected
   columns' chunks from the footer metadata and fetch each chunk (or each
   run of adjacent/near-adjacent chunks, coalescing gaps up to a small
   threshold, e.g. 1 MiB) with **one** range request, then decode from
   memory. Bound the size of a single coalesced request (e.g. 64 MiB) and the
   bytes buffered at once.
4. **Concurrency and read-ahead.** A bounded pool issues the range requests
   of the next row groups while earlier ones decode: default in-flight
   request limit 8–16 (setting, e.g. `PINKDB_HTTPFS_PARALLELISM` or a
   pinkdb setting — pick one consistent with existing knobs, `0`/`1` =
   today's serial behavior), bounded in-flight bytes, results delivered in
   row-group order (output order must not change). Row groups decode in
   parallel where the engine allows (rayon is already a dependency), still
   assembled in order.
5. **Connection reuse.** One transport client (one `ureq::Agent` pool) per
   scan shared by every request and thread; the pool must allow at least the
   in-flight limit of idle connections per host. Verify no per-request
   client/TLS setup.
6. **Errors and retries unchanged:** a failed range request fails the scan
   with the same error shapes; the existing retry policy still applies per
   request; an object changing size/ETag mid-scan is detected if the
   transport already detects it (keep that behavior).

## Non-goals (98b)

Remote CSV/Arrow read-ahead, lake data files on S3 through the pool, the
on-disk remote-file cache, streaming scans into the executor (bounded
memory for SF100), Vortex DECIMAL.

## Tests

Use the existing local HTTP test server (`crates/pinkdb-httpfs/src/test_http.rs`
and the parquet `remote` tests) — no network in tests:

- request-count bounds: a multi-row-group file read with a 2-of-N-column
  projection issues at most `row_groups × 2` data requests (fewer with
  coalescing) plus the footer requests; `count(*)` issues only footer
  requests;
- results identical (values and row order) to the local-file read for every
  type, with and without pruning, for parallelism 1 and the default;
- in-flight limit respected (the test server counts concurrent requests);
- a failing range request (500 then success → retried; persistent 500 →
  scan error) and a short read are clean errors;
- `EXPLAIN` shows the projected width.

## Benchmark (run by the reviewer, not the agent)

R2 `pinkdb-bench`, DuckDB 2.0.0 vs pinkdb before/after: SF1 + SF10
`count(*)`, Q6, Q1; numbers recorded in the ROADMAP.

## Gates

`cargo test --workspace`, the CLI/parquet feature combinations
(`parquet`, `parquet,httpfs`, `arrow,parquet,httpfs`), clippy on both
toolchains, fmt, wasm32 core build, duckdiff (full battery).

## Rules for delegated agents

Do not commit, push or create branches. Touch only the files the task
names. Use `CARGO_INCREMENTAL=0`. Do not use any cloud credentials.
Report what changed, the measured request counts before/after on the local
test server, and every check.

## How it was built and what it measured

Delegated to OpenCode (DeepSeek V4.1 Flash; CommandCode kept failing to
reach its API), reviewed here. `extensions/parquet/src/{remote,scan,lib}.rs`:
per-scan transport counters (`ParquetSource::remote_stats()` — requests,
bytes, network wait), metadata-only scans (`count(*)` and virtual-column-only
projections read only footers), per-row-group byte spans of the projected
column chunks coalesced across gaps up to 1 MiB (single request at most
64 MiB, larger chunks stay lazy), a bounded read-ahead window (128 MiB) with
concurrent fetches and ordered, parallel row-group decode, short-read
detection; `pinkdb-httpfs`: `PINKDB_HTTPFS_PARALLELISM` (default 8, max 16,
`0`/`1` = the old serial path) also sizing each client's idle-connection
pool; `EXPLAIN` renders `(N of M columns)` from `ScanProjections`.

Local test server, 3 row groups x 7 columns, a 2-column projection with one
row group pruned: **20 → 2** data range requests (4.45 MB → 3.25 MB, network
wait 54 → 15 ms); `count(*)`: 0 data requests.

**R2, same machine and network** (median of 3; DuckDB 2.0.0 official):

| query | DuckDB 2.0.0 | pinkdb before | pinkdb after |
|---|---|---|---|
| SF1 `count(*)` | 0.41 s | 29.5 s | **0.62 s** |
| SF1 Q6 | 3.87 s | 406 s | **4.26 s** |
| SF1 Q1 | 5.41 s | — | **4.15 s** |
| SF10 `count(*)` | 0.65 s | 339 s | **0.63 s** |
| SF10 Q6 | 25.9 s | (~1 h est.) | **28.9 s** |
| SF10 Q1 | 33.1 s | — | **35.7 s** |
| SF1 CSV `count(*)` (773 MB) | 42.5 s | 44.1 s | 44–53 s |

Remote Parquet is now at parity; both engines are bandwidth-bound on this
link at SF10. Lakes are not (they download whole data files) — wave 98b:

| SF1 lake on R2 | DuckLake | pinkdb `.pink` | pinkdb Parquet | pinkdb Vortex (DOUBLE) |
|---|---|---|---|---|
| attach + `count(*)` | 0.07 s | 2.75 s | 3.25 s | 15.5 s |
| attach + Q6 | 3.9 s | 31.7 s | 22.9 s | 31.4 s |
| attach + Q1 | 3.4 s | 31.9 s | 24.3 s | 33.2 s |

(DuckLake's catalog is a local DuckDB file; pinkdb's catalog lives on R2.)
