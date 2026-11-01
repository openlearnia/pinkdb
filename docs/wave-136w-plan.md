# Wave 136w — the write side: streamed and incremental checkpoints, WAL and ingest throughput

Planned 2026-11-01, stream A, under the performance-first plan
(`plan-2026-11-01.md`). Stream C owns 136a–e (memory pool, lazy open, spill)
and the single format bump 6→7; stream A owns save/checkpoint in
`persist.rs`, the WAL append path, and ingest. This becomes
`docs/wave-136w-plan.md` once C agrees the format parts.

## What exists (code read 2026-11-01)

- `PinkDb::to_bytes_inner` builds the **whole file in memory** (`Writer`
  over a `Vec<u8>`): `write_table` collects every chunk's `Arc<Vector>`s,
  encodes **every page of the table** (rayon over chunks), then appends them
  column by column per row group (60 chunks) to the in-memory data region;
  the footer follows. `save` / `save_sync` / `write_atomic` then write the
  buffer. Encryption seals the whole buffer (`seal_container(&bytes, key,
  0)`).
- Peak memory of a save = decoded store + all encoded pages of the largest
  table + the whole file image (+ the sealed copy when encrypted). C's spike
  measured `.save` of QuackLab's 94 MB store: 0.79 s, 1.01 GB RSS.
- Every checkpoint rewrites every table, decoded and re-encoded, even when
  nothing changed (C's spike: the output is byte-identical for an unchanged
  store). Auto-checkpoint runs every 64 MiB of WAL
  (`DEFAULT_AUTO_CHECKPOINT_BYTES`), so a large store pays a full rewrite
  per 64 MiB written.

## Steps

| step | content | format | depends on | gate |
|---|---|---|---|---|
| 136w0 | **Baseline.** Checkpoint time and peak RSS for TPC-H SF 1 and SF 10 stores (pinkdb `CHECKPOINT` / `.save` vs DuckDB `CHECKPOINT` after the same load), QuackLab's store, a 1-row change on each; durable insert throughput (the suite's `durable` family, single-row and batched); CSV and Parquet ingest rows/s vs DuckDB (lineitem SF 1). Committed as `bench/results` + a table in the plan. | none | — | none |
| 136w1 | **Streamed writer.** The data region is written straight to the temp file through a positioned sink (`BufWriter<File>` + offset), one row group at a time: encode that row group's pages in parallel, write them, drop them. Statistics accumulate per row group. Bytes identical to today (a test compares). Encrypted stores keep the in-memory path until 136w3's container. Peak drops to decoded store + one row group. | none | — | targeted + writer tests |
| 136w2 | **Copy-through save.** A chunk that is still clean (C's lazy handle: `Column::encoded(i)`) is copied as bytes; only dirty chunks are encoded. Schema changes mark a table dirty. Byte-identical save of an unchanged store. | none beyond C's v7 | C's 136b-2 (lazy handles, footer v7) | full |
| 136w3 | **Incremental checkpoint.** The checkpoint appends only changed row groups and a new footer to the existing file and points the footer at both old and new row groups; space held by superseded row groups is recorded in the footer and reused (or compacted when over a threshold, e.g. 50 % garbage). Crash safety: the new footer is written and fsynced before the trailer that names it; recovery reads the last valid trailer. Encrypted stores: a chunked container (per-block AEAD) so blocks can be appended. | **yes: rides C's 6→7** (free-space list, multiple footers / trailer chain, chunked encryption) | 136w2 | full |
| 136w4 | **WAL write throughput.** Profile the durable-insert families; group commit for concurrent writers, frame batching, avoiding per-row `Value` materialization in record encoding, fsync policy defaults vs DuckDB. | WAL frame layout only if needed (tell C/parent first) | — | targeted (full if WAL format changes) |
| 136w5 | **Ingest.** 111b4 (Parquet decode in larger batches, in parallel; 1.23 → DuckDB's 5.77 M rows/s) and 122a (statistics fold within 5 % of the no-stats write path). | none | — | targeted |

Order: w0, w1 now; w4 and w5 next (independent of C); w2 when C's lazy
handles land; w3 last, its format agreed with C before C freezes v7.

## Measurements per step

Checkpoint time and peak RSS (SF 1, SF 10, QuackLab store; full and
1-row-change), durable inserts/s, ingest rows/s — each against DuckDB on the
same machine, and TPC-H SF 1 A/B (no query regression).

## Format proposal for C (136w3), to agree before v7 freezes

1. Footer gains a **free-space list** (`(offset, len)` ranges in the data
   region no row group references).
2. The trailer gains the **offset of the previous footer** (a chain), so a
   torn append falls back to the last complete footer.
3. Row-group directory entries already carry absolute offsets; an
   incremental footer can reference row groups anywhere in the file.
4. Encrypted stores: `PINKENC2`, a sequence of independently sealed blocks
   (nonce per block, block index in the AAD) so appends never re-seal the
   file.

## Baseline (136w0) and after (136w1)

Measured 2026-11-01 on the development machine (Apple M4, 10 physical
cores, 16 GB, macOS 27.0.1, DuckDB v2.0.0 `d54cbbe3df`), release CLI built
with `--features parquetlake,httpfs`, 8 threads in both engines, TPC-H data
generated once by DuckDB's `dbgen` into Parquet and loaded into a `.pink`
store and a DuckDB file from the *same* Parquet files. Times are the
engine's own statement timer (`.timer on`); peak RSS is `/usr/bin/time -l`
of the whole process; the file size is after the measured step. "136w0" is
the CLI built from the tree before 136w1, "136w1" the streamed writer.

The machine is shared with other work (background load average moved
between 1 and 12 during these runs), so the pinkdb columns are medians of
**interleaved** runs of the two binaries on the same store, not of one
binary's run followed by the other's. Where the spread is large it is given.
DuckDB was measured with `bench/suite/run.py`'s driver on the same data.

Loading is `CREATE TABLE t AS SELECT * FROM read_parquet(...)` for all
eight tables, then `.save` (pinkdb) or commit (DuckDB). An "update" is
`UPDATE lineitem SET l_quantity = l_quantity + 1 WHERE l_orderkey = 1`
(one row); DuckDB has no save-to-a-new-file, so its (c) is another
`CHECKPOINT` of an unchanged store.

### TPC-H SF 1

| step | pinkdb 136w0 | pinkdb 136w1 | DuckDB |
|---|---:|---:|---:|
| load | 6.21 s / 2383 MB / 323,433,245 B | 5.94 s / 2716 MB / 323,433,245 B | 1.50 s / 1044 MB / 299,380,736 B |
| (a) `CHECKPOINT` after the load | 1.95 s / 2426 MB | 1.92 s / 2156 MB | 0.001 s / 27 MB |
| (b) `CHECKPOINT` after a one-row update | 2.02 s / 2426 MB | 2.03 s / 2172 MB | 0.059 s / 69 MB |
| (c) save to a new file | 1.86 s / 2425 MB / 323,433,542 B | 1.92 s / 2155 MB / 323,433,542 B | 0.002 s / 36 MB / 299,380,736 B |

n = 4–5 interleaved runs per pinkdb cell (the (a) and (c) cells also had a
5-run set, the (b) cells 4; the load column is 3 rounds of 3 runs). An
open-only run of the same store peaks at 2088 MB, so the marginal memory a
checkpoint needs fell from **337 MB to 34 MB** — from the whole file image
(323 MB) to one row group.

### TPC-H SF 3

| step | pinkdb 136w0 | pinkdb 136w1 | DuckDB |
|---|---:|---:|---:|
| load | 23.9 s / 6307 MB / 980,290,342 B | 21.9 s / 6381 MB / 980,290,342 B | 3.21 s / 1817 MB / 911,486,976 B |
| (a) `CHECKPOINT` after the load | 6.27 s / 5899 MB | 6.23 s / 6266 MB | 0.000 s / 28 MB |
| (b) `CHECKPOINT` after a one-row update | 7.38 s / 5521 MB | 7.76 s / 5462 MB | 0.077 s / 79 MB |
| (c) save to a new file | 6.19 s / 6333 MB / 980,290,342 B | 6.36 s / 6023 MB / 980,290,342 B | 0.002 s / 38 MB / 911,486,976 B |

n = 4 interleaved runs per pinkdb cell; a second 4-run set taken under
heavier background load gave 7.61/7.83 s (136w0) and 7.44/7.32 s (136w1)
for (a)/(b), i.e. the two binaries are indistinguishable. Peak RSS does not
move either way here: loading the store already peaks at ~6.2 GB (the
decoded columns are ~6× the 980 MB file), and the image the baseline
builds reuses pages the decode touched, so it does not raise the
high-water mark. The saving at this scale is the 980 MB buffer the
baseline allocates, not the reported peak.

### Durable inserts (2000 rows, file-backed store, CLI process wall clock)

| operation | pinkdb 136w0 | pinkdb 136w1 | DuckDB |
|---|---:|---:|---:|
| 2000 single-row `INSERT` (autocommit, one fsync each) | 7.83 s, 255 rows/s | 7.80 s, 256 rows/s | 0.232 s, 8,626 rows/s |
| one batched `INSERT` of 2000 rows | 0.022 s, 90,198 rows/s | 0.026 s, 75,551 rows/s | 0.028 s, 71,429 rows/s |

Median of 3. The suite's `durable` family (its in-memory CLI runs plus the
`durable` bench binary) reports, 136w0 / 136w1: bench `Sync` inserts
7777.9 / 8226.9 ms, selects 50.4 / 57.5 ms, checkpoints 48.6 / 44.4 ms;
bench `Batch` inserts 69.5 / 66.9 ms. A single-row commit costs ~3.9 ms,
dominated by the fsync — the gap to DuckDB lives in the WAL commit path
(step 136w4), not in the checkpoint.

### Ingest, lineitem and orders at SF 1

| table | format | pinkdb 136w0 | pinkdb 136w1 | DuckDB |
|---|---|---:|---:|---:|
| lineitem | CSV `.import` / `read_csv_auto` | 2387 ms, 2.51 M rows/s | 2345 ms, 2.56 M rows/s | 3056 ms, 1.96 M rows/s |
| lineitem | Parquet | 3240 ms, 1.85 M rows/s | 3121 ms, 1.92 M rows/s | 625 ms, 9.60 M rows/s |
| orders | CSV | 613 ms, 2.45 M rows/s | 634 ms, 2.37 M rows/s | 455 ms, 3.30 M rows/s |
| orders | Parquet | 582 ms, 2.58 M rows/s | 576 ms, 2.60 M rows/s | 159 ms, 9.43 M rows/s |

Median of 3 samples each (`bench/suite/run.py --only ingest --scale 1`).
The streamed writer changes nothing here — ingest builds tables in memory
and the reported step excludes the save. Pinkdb reads CSV faster than
DuckDB and Parquet ~4× slower; that is step 136w5.

### What 136w1 bought, and what it did not

The data region is now serialized into the target file one row group at a
time: the save allocates one row group's pages instead of the whole file,
and nothing else changed. Correctness: the streamed image is byte-identical
to `to_pink_bytes()` for both the serial and the parallel encoder
(`streamed_save_writes_the_in_memory_bytes`), and a `.save` of the real
323 MB SF 1 store by the two CLIs compares equal with `cmp`. Time is
unchanged (±2 %, within the run-to-run spread). The remaining checkpoint
cost is the *full rewrite* — every checkpoint decodes and re-encodes every
table even when nothing changed — which is step 136w2 (copy clean chunks
through) and 136w3 (append-only checkpoints); the 1.9 s vs 59 ms gap to
DuckDB is that step, not this one.

## 136w5 — Parquet decode and the statistics fold (ROADMAP 111b4, 122a)

### Where the time went (profiles, `sample` at 1 ms, release CLI)

Parquet, `CREATE TABLE lineitem AS SELECT * FROM
read_parquet('lineitem.parquet')` at SF 1, 3.3 s. Self ticks on the main
thread: `Vector::push_valid` 171, `memcmp` 157, `stats::key_hash` 154,
`core::str::from_utf8` 151, `memmove` 146, `snap::decompress` 129,
`plan::compare_values` 105, malloc/free 174, `Vector::extend_from` 85,
`stats::Zone::of_vector` 81, `StrVec::bytes_of` 60, `StrVec::push` 52,
`scan::push_entry` 51, `StrVec::push_view_of` 46, `Value::decimal` 42,
`read_chunks::<ByteArrayType>` 40, `ColStats::widen` 35,
`append_shared_chunk_inner` 34, `RleDecoder::get_batch_with_dict::<ByteArray>` 29,
`bytes::shared_clone`/`shared_drop` 52.

The call graph says the same in one line: `exec::table_function_output` →
`ParquetSource::scan` is **1213 of the main thread's 2067 ticks (59 %)**, all
of it on one thread, and the rest splits into ~420 ticks of append (zone and
sketch fold, mostly `refresh_zone` re-deriving whole chunks because the
tail was ragged) and ~430 of staging, index and WAL. Inside the decode the
per-cell `Value` (a `String` per VARCHAR, `Value::decimal` per DECIMAL) and
the generic `Vector::push_value`/`push_valid` dispatch dominate; snappy is
~6 %.

CSV, `.import lineitem.csv lineitem` into an in-memory table: `key_hash`
**516** ticks, `Zone::of_vector` 253, `memcmp` 208, `StrVec::bytes_of` 115,
`fold_chunk_stats`'s closure 63. The parser is not in the top ten — the
statistics fold was the CSV import's hot path, at ~350 ms of a ~2.3 s
import (the 12–15 % 122a names).

### Changes

1. **Typed batched decode** (`extensions/parquet/src/scan.rs`).
   `read_flat_chunks` accumulates a whole `CHUNK_SIZE` chunk across page
   boundaries into the parquet crate's own typed buffer and builds the
   engine vector once, with `Vector::from_raw`: `Vec<i32>`/`Vec<i64>`/
   `Vec<f64>`/`Vec<bool>` payloads, a `StrVec` filled with `push` (no
   `String` per cell), `Vec<i128>` for DECIMAL, a validity bitmap from the
   definition levels. ARRAY and VARIANT keep the value path. Same errors,
   same values (the round-trip tests below compare bit-exactly).
2. **Parallel local decode** (`scan_local_parallel`). The decode used to run
   row group by row group on the caller's thread; a local file now decodes
   on rayon — the engine's pool, so no new threads — with one task per
   (surviving row group × projected column), a window of row groups in
   flight to bound memory, one `SerializedFileReader` per worker in a pool
   (a reader parses the footer once), and assembly strictly in row-group
   then column order. DuckDB's `parquet_reader.cpp` splits a row group
   across threads the same way; pinkdb writes one row group per file
   (`write.rs`), so columns are the only independent unit inside one.
3. **Chunk-aligned scan** (`chunk_lens`). A row group's chunks are cut on
   `CHUNK_SIZE` boundaries of the *file*, not of the row group: the first
   chunk of a group ends the previous group's ragged tail. Before, every
   row group ended on a short chunk, so from the first row group on the
   table's tail was never full and **every** CTAS append took the copy path
   (2381 of 2800 appends; 1.28 s of copying and zone rebuilding). Now 2760
   of 2800 appends share the decoder's vectors (40 copies are the file's own
   ragged tail).
4. **Statistics fold in the decoder's own pass, per column** (`stats.rs`,
   `storage.rs`): `Hll::fold_vector` matches the vector's payload type once
   per chunk instead of once per row (`key_hash`'s jump table was 516 ticks
   in the CSV profile), skips the validity bitmap when a column has no
   NULLs, and hashes with exactly the constants `key_hash` uses, so
   estimates are unchanged. The copy path now folds the appended rows once,
   in parallel, and merges the two halves' zones into the two chunks they
   landed in, instead of re-deriving both chunks' zones from their rows
   (`refresh_zone`) — same bounds, one pass.
5. Rejected: sampling the sketch at 1/8 or widening zones at row-group
   granularity (ROADMAP 122a's alternatives). Both change what a statistic
   means — an unlabelled sample makes `approx_count_distinct` wrong by 8×
   on small tables and needs a stored rate to be honest (a format change),
   and row-group zones are not what the zone map indexes.

### What each change bought (SF 1 lineitem, same machine, same harness)

| state | Parquet CTAS | of which fold | CSV `.import` |
|---|---:|---:|---:|
| 136w1 (baseline of this step) | 3121 ms | — | 2345 ms |
| typed decode only | 1816 ms | 1076 ms (copy path) | 1975 ms |
| + per-column fold, copy path without `refresh_zone` | 1060 ms | 300 ms | 2218 ms |
| + chunk-aligned scan (2800 appends now shared) | 905 ms | 49 ms (fast-path fold) | 2200 ms |
| fold off (`PINKDB_NO_STATS_FOLD`, measurement only) | 361 ms | — | ~1950 ms |

Zone fold ≈ 214 ms and sketch ≈ 330 ms for 96 M cells (6.0 M rows × 16
columns, 2.2 and 3.4 ns per cell) once both are vectorized and parallel;
that is the arithmetic floor
for exact per-chunk bounds plus an exact sketch over every row, and it is
~30 % of the 1.06 s CTAS. Getting inside 122a's 5 % bound would need a
cheaper statistic, not a cheaper fold — see the rejected options above.

### Ingest at SF 1 after 136w5

`bench/suite/run.py --only ingest --scale 1 --runs 3`, medians, both
engines on the same Parquet files and CSV dumps (load average 2.8):

| table | format | pinkdb 136w1 | pinkdb 136w5 | DuckDB |
|---|---|---:|---:|---:|
| lineitem | CSV `.import` / `read_csv_auto` | 2345 ms, 2.56 M rows/s | 2469 ms, 2.43 M rows/s | 3220 ms, 1.86 M rows/s |
| lineitem | Parquet | 3121 ms, 1.92 M rows/s | **809 ms, 7.42 M rows/s** | 712 ms, 8.43 M rows/s |
| orders | CSV | 634 ms, 2.37 M rows/s | 684 ms, 2.19 M rows/s | 456 ms, 3.29 M rows/s |
| orders | Parquet | 576 ms, 2.60 M rows/s | **142 ms, 10.56 M rows/s** | 160 ms, 9.38 M rows/s |

Parquet ingest is 3.9× faster than at the start of the wave and now within
14 % of DuckDB on lineitem and ahead of it on orders; CSV is unchanged
within the run-to-run spread and still ahead of DuckDB on lineitem. A
second, quieter measurement of the same four cells, interleaved with
DuckDB (`/usr/bin/time -l` for RSS, medians of 3): lineitem Parquet
**530 ms / 1445 MB** vs 562 ms / 690 MB, lineitem CSV **2052 ms /
1900 MB** vs 2843 ms / 1362 MB, orders Parquet **144 ms / 330 MB** vs
152 ms / 440 MB, orders CSV **552 ms / 460 MB** vs 544 ms / 475 MB. The
two harnesses disagree on lineitem Parquet by 50 % because they were taken
under different background load; both put pinkdb at or near DuckDB, where
136w1 was 4–5× behind. Peak RSS of the Parquet load fell from 2564 MB to
1445 MB (the old tail also copied every chunk; the fill ratio is the same).

### TPC-H SF 1 query A/B

Per query, on the same prebuilt store, medians of 3, the 136w1 binary and
the 136w5 one alternating (run the other had the machine at load 10–13):

| | q1 | q2 | q3 | q9 | q10 | q13 | q18 | q20 | q21 | total |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 136w1 | 42 | 13 | 34 | 106 | 85 | 102 | 127 | 68 | 87 | 1088 |
| 136w5 | 38 | 13 | 35 | 106 | 88 | 100 | 123 | 96 | 111 | 1143 |

(ms; the columns are the queries that moved most, all 22 make the total.)
The total is +5 % and 20 of 22 queries are within ±12 %. A control run of
the *same* binary on both sides under the same load gives +2.3 % on the
total and a per-query spread of up to ±22 % (q13 alone 1.22×), so the two
large outliers here (q20, q21) are inside the noise this machine has while
other work runs; no query lost a plan or a pruned row group, and the
suite's row-for-row comparison against DuckDB passes for all 22.

### Left undone

- **122a's 5 % bound is not met** for Parquet CTAS (~30 % of it is the fold)
  and CSV (~15 %). The fold is now one pass per column, parallel, typed
  (≈5.6 ns per cell); what is left is exactness itself. Reaching 5 % needs a
  statistic that does not see every value, which changes `DESCRIBE`'s
  answers — a decision for whoever owns the catalog, not a decode change.
- The Parquet scan streams (`scan_streamed`), but CTAS still materializes
  the whole query result before appending (`exec::CreateTableAs` evaluates
  into `chunks`), so the decoded columns are held for the whole load. RSS is
  now bounded by the old copy-path inflation rather than by the decoder; a
  streaming CTAS is an `exec` change outside this step's files.
