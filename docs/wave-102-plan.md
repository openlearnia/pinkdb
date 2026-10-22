# Wave 102 — one fast `.pink` format: ranged, projectable, no legacy (plan)

Started 2026-10-22, queued right after 98b by the user ("we need to make
pinkdb/pinklakes faster … if we need to steal stuff from vortex or parquet
so be it"), with all legacy compatibility dropped ("legacy doesn't make
sense for us … we are in early development").

## Why

`.pink` v5 (`crates/pinkdb/src/persist.rs`) is written sequentially — per
database, per table, per column, its chunks inline — with **no offset
directory** and **one CRC-32 over the whole image**. A reader cannot locate
one column without reading everything before it, cannot verify part of a
file, and every lake read downloads and decodes the whole file. Measured on
R2 (TPC-H SF1): `.pink` lake attach + Q6 31.7 s vs DuckLake 3.9 s, and the
`.pink` lake is ~2.2× DuckLake's Parquet (680 MiB vs 308 MiB). The chunk
encodings themselves (DELTA_BITPACK, DICT_BITPACK, ALP, LZ frames) are good
and stay.

## Decisions

### One format, no legacy

- **`PINKDB06`** replaces every earlier layout, for engine database files
  **and** lake data files (a lake data file is a one-table `.pink` file).
  Readers for `PINKDB01`–`PINKDB05` are **deleted** with their fixtures and
  compatibility tests; such files are refused with
  `this .pink file was written by an older pinkdb (PINKDB0N); this build
  reads PINKDB06 only`.
- **Lake catalog:** one current `format_version` (bump to **7**: the new
  per-file footer location below); `format_version` 1–6 readers, the
  v1–v5 fixtures (`crates/pink-lake/tests/fixtures/lake_v1_v4` and the
  rest) and every migration test built on them are deleted; anything older
  is refused with "written by an older pinkdb". The **wave 88 hybrid
  migration rule** goes: a table keeps its format on every save/append and
  changes only through `set_table_format` / `.lake format … for …` /
  `.lake rewrite … format …`; the `format_pinned` flag is removed.
- The encrypted containers (`PINKENC1` for databases, `PNKLAKE1` for lake
  objects) keep wrapping whole files: encrypted lakes stay whole-object
  reads (98b already falls back). Per-chunk encryption is a follow-up.

### Layout (from Parquet)

```text
"PINKDB06"
data region: column chunks, each = a page index + pages
footer (deterministic binary, versioned): store metadata (databases, tables,
  schemas, defaults, constraints, index definitions + stats, MVs, procedures,
  macros, RBAC — today's v5 sections, moved here), and per table:
  row_count, row groups [ {rows, per column: offset, length, page count,
  encoding summary, crc32c, stats {min, max, null_count, nan_count}} ]
footer_crc32c u32 | footer_len u64 | "PINKDB06"   (fixed 20-byte trailer)
```

- **Row groups** of 122,880 rows (60 engine chunks of 2,048); a **page** is
  one engine chunk encoded with the existing per-chunk encoder, so
  decode stays chunk-native.
- **Per-chunk CRC32C** (and a footer CRC): any column chunk verifies on its
  own; no whole-file CRC.
- **Exact per-chunk statistics** feed zone-map pruning and the lake's
  `COUNT`/`MIN`/`MAX` (the catalog keeps publishing its stats block from
  them).
- A reader needs the trailer (20 bytes) + footer: two small range requests,
  or **zero** when the lake catalog records the file's footer offset/length
  (`format_version` 7 field), which attach uses directly.

### Encodings and decode (from Vortex)

- Keep today's encodings and the per-chunk sampling choice.
- **FastLanes-style bit-packing** (1,024-value blocks, transposed lanes) for
  integer/date/timestamp/dictionary codes, so decode is branch-free and
  SIMD-friendly (portable Rust, autovectorized; no `unsafe` needed).
- **FSST** for strings (tag 9 is reserved and its design recorded).
- A general-purpose second stage chosen by measurement on the TPC-H set:
  the existing hand-rolled LZ vs a pure-Rust LZ4 (`lz4_flex`) vs pure-Rust
  zstd decode if an encoder without C exists. **Default builds stay free of
  C dependencies.**
- Parallel decode: row groups decode on threads, assembled in order.

### Readers

- **Lake `pink` provider:** `supports_projection() == true`; reads only the
  projected column chunks of row groups that survive pruning, via 98b's
  ranged `DataFile` / coalescing / bounded concurrent read-ahead; footer from
  the catalog location when present.
- **Engine database files:** open reads the trailer + footer, then loads
  columns in parallel (eagerly, as today, but chunk-parallel). Lazy
  column loading for very large local stores is a recorded follow-up unless
  it falls out naturally.
- WAL, checkpoints, durable replay, `PinkDb::from_pink_bytes` (wasm
  playground import/export) all move to `PINKDB06` unchanged in behavior.

## Targets (R2, TPC-H, same machine)

- `.pink` lake file size within ~20% of DuckDB's Parquet (SF1 ≤ 370 MiB).
- `.pink` lake attach + Q6 / Q1 at parity with the Parquet lake after 98b
  and close to DuckLake (3.4–3.9 s at SF1).
- Local save/open of a `.pink` database no slower than v5.

## Tasks (delegated in order; CommandCode, MiniMax M3.1 Flash)

- **T1 — engine format.** `persist.rs`: `PINKDB06` writer/reader (layout,
  footer, CRCs, stats, parallel decode), FastLanes bit-packing, FSST, the
  second-stage choice by measurement; delete v1–v5 readers, fixtures and
  compatibility tests; the older-file refusal; every engine user of
  `.pink` (save/open, WAL checkpoints, encryption container, wasm import)
  on the new format. Tests: round-trip every type and edge value, corruption
  per chunk and in the footer, refusal of a `PINKDB05` file, size and
  save/open timings on a TPC-H SF1 store before/after.
- **T2 — lake.** The `pink` provider on `PINKDB06` with projected ranged
  reads; catalog `format_version` 7 with the footer location; delete catalog
  v1–v6 readers, legacy fixtures, migration tests and the hybrid rule /
  `format_pinned`; update the wave 88 contract test and mock-S3 request-count
  tests (98b's `fast_reads`) for the `pink` format.

## Gates

`cargo test --workspace`; the full pink-lake / pink-lake-parquet / CLI
feature matrix; clippy on both toolchains; fmt; wasm32 core build + the
playground smoke (`website/tools/wasm-smoke.mjs`); duckdiff; the R2 lake
benchmark vs DuckLake.

## Rules for delegated agents

Do not commit, push or create branches. Touch only the files the task
names. Use `CARGO_INCREMENTAL=0`. No cloud credentials (mock S3, local
files only). Report what changed, measurements, and every check.

## T1 — how it was built (2026-10-22)

CommandCode on MiniMax M3.1 Flash produced no edits in 10 minutes and was
cancelled; the task ran on **OpenCode with `opencode-go/minimax-m3.1-flash-free`**
and completed. Reviewed here.

Landed: `PINKDB06` in `crates/pinkdb/src/persist.rs` (row groups of 122,880
rows, column chunks = page index + pages, CRC32C per chunk and for the footer,
exact per-chunk stats, footer with all store metadata, 20-byte trailer),
FastLanes bit-packing (tag 10) for integers/dates/timestamps/dictionary codes,
FSST (tag 9) for strings, `lz4_flex` (pure Rust) as the second stage — chosen
by measurement (hand-rolled LZ 321.1 MB in 1.54 s vs lz4 318.7 MB in 0.61 s
compress+decompress on 570 MB of TPC-H VARCHAR); `PINKDB_COMPRESS=lz|lz4`
for measurement. v1–v5 readers, fixtures and compatibility tests deleted;
older files refused with "written by an older pinkdb (PINKDB0N)". WAL,
checkpoints, the `PINKENC1` container and the wasm playground moved to the
new format (module rebuilt, smoke 10/10).

**Review fix — DECIMAL.** The agent's size measurement used a CSV import
(money columns inferred as DOUBLE) and a 7-table set, and reported 271.6 MB,
"3.4% below Parquet". Measured through the CLI from the real Parquet
(DECIMAL(15,2) money columns, 8 tables), PINKDB06 was **706 MB** — DECIMAL
chunks were stored as 16 raw bytes per value (`lineitem` alone: 546.7 MB vs
191.3 MB with DOUBLE). DECIMAL with precision <= 18 now uses the BIGINT
encodings on exact i64 unscaled values (an engine invariant —
`types::decimal_fits` on every write path — is asserted rather than
silently clamped); precision > 18 keeps the 16-byte path.
`crates/pinkdb/tests/decimal_storage.rs` pins the i64 boundary
(±(10^18−1)), NULLs, DECIMAL(38,6), and the compressed size.

| TPC-H SF1, 8 tables, from Parquet via the CLI | size |
|---|---|
| `.pink` v5 | 713 MB |
| PINKDB06 before the DECIMAL fix | 706 MB |
| **PINKDB06** | **320.8 MB** |
| DuckDB Parquet (same tables) | 309 MiB ≈ 324 MB |

The agent's own engine-level timing on its data set: save 2.50 s → 1.22 s,
open 2.57 s → 0.17 s.

Other review fixes: `crc32c` uses `as_chunks::<8>()` (stable clippy 1.99),
two vortexlake-only CLI tests that read old `.pink` fixtures deleted (the
remaining legacy lake fixtures go in T2).
