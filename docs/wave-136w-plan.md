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
