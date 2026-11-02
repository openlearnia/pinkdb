# Wave 136b — lazy storage design

Status: design spike only. No engine code was changed and no gate was run in this worktree
(a full gate was running while these measurements were taken). All numbers below come from
the prebuilt 135d binary, `~/Desktop/projects/pinkdb-wt/bench-c/bin-135d/pinkdb`, on copies
of `~/Desktop/projects/pinkdb-wt/bench-c/scratch/ql.pink` under `/tmp/w136b`.

Recommendation: **design A, chunk-granularity lazy decode**, with private `Column` accessors,
a retained encoded image, pool-charged decoded chunks, strong-count pinning, deferred hash-index
maps, and a `.save` path that copies clean chunk bytes. It is the only option that keeps
zone-map pruning meaningful and bounds decoded bytes per scanned table. It needs one small
footer addition — per-chunk row counts — and therefore footer version 4; the data chunks
(and their CRCs) are unchanged.

---

## 1. What is actually in the QuackLab store

`ql.pink` is 93,983,593 bytes: 91,975,331 bytes of data chunks + a 2,008,242-byte footer
(92 tables). Chunk bytes dominate; the footer holds every table's schema, constraints, index
definitions, row-group directory, column statistics and zone maps.

| table | rows | cols | encoded bytes |
|---|---:|---:|---:|
| master.TRIPS_BY_OD_HOUR_PAYMENT | 1,920,390 | 18 | 35,154,708 |
| ingest.TRIPS_BY_OD_HOUR_PAYMENT | 1,920,390 | 18 | 35,154,708 |
| DZP.BY_OD_DAY | 245,570 | 18 | 12,891,639 |
| DZP.BY_OD_DISTANCE | 245,570 | 10 | 3,230,198 |
| PHD.BY_HOUR_ZONE_DAY | 90,383 | 12 | 1,785,775 |
| master.FACT_TRIP_DISTANCE | 245,570 | 4 | 1,502,064 |
| ingest.FACT_TRIP_DISTANCE | 245,570 | 4 | 1,502,064 |
| DZP.BY_ZONE_DAY | 7,427 | 22 | 526,352 |
| PHD.BY_HOUR_DAY | 768 | 16 | 48,626 |
| FARE.BY_PAYMENT_DAY | 162 | 17 | 14,390 |
| FARE.BY_RATE_CODE_DAY | 193 | 14 | 10,795 |
| FARE.BY_VENDOR_DAY | 64 | 14 | 3,869 |
| every `DIM_ZONE_*` dim | 265 | 5 | 6,502 |
| `metadata.BATCH_STATUS` | 3 | — | 329 |

The two fact copies plus the master mart are ~82.5 MB of the 92 MB data region; all marts are
~18.5 MB; every dim is under 7 KB. `master.TRIPS_BY_OD_HOUR_PAYMENT` has 16 row groups.

### Measured eager behaviour (135d)

| action | wall | max RSS |
|---|---:|---:|
| empty store, open + `SELECT 1` | 0.01 s | 11,059,200 B |
| `.open ql.pink` + `SELECT 1` | 0.28 s | 893,386,752 B |
| 12 chart statements in one process (7 runnable, 5 parse/bind errors) | 0.36 s | 952,401,920 B |
| one mart refresh (`r50sel.sql`) | 1.37 s | 2,307,031,040 B |
| `.save` the whole store | 0.79 s (0.646 s reported by engine) | 1,013,940,224 B |

The saved file is byte-identical (`sha256 ad58a260…` both), which matters for design A's
copy-through save: an unchanged store can be produced without decoding anything.

Single-table stores, decoded RSS = max RSS − 11.06 MB baseline:

| table | decoded RSS | file size |
|---|---:|---:|
| master.TRIPS_BY_OD_HOUR_PAYMENT | 369.6 MB | 35,747,179 B |
| DZP.BY_OD_DAY | 60.6 MB | 13,027,969 B |
| DZP.BY_OD_DISTANCE | 35.4 MB | 3,299,857 B |
| PHD.BY_HOUR_ZONE_DAY | 11.9 MB | 1,828,839 B |
| DZP.BY_ZONE_DAY | ~2 MB (12.9 MB raw; baseline noise ±1 MB) | 592,371 B |
| every other widget table | < 0.4 MB each (10.0–10.4 MB raw) | 11–96 KB |

Sum of the two facts + the four marts above ≈ 850 MB, matching the measured 893 MB open:
**the eager open pays for the whole store, not for what a query reads.**

### What the chart widgets actually read

The 12 `chartSql` statements in stream B's
`bench/quacklab/plan/nyc_taxi_essential1.json` each read one mart and project only the columns
they name. Seven of the twelve run on 135d; the other five fail in parse/bind (not storage):
`od_distance_filtered`, `payment_mix`, `revenue_by_month`, `trips_by_day`, `vendor_daily`.
The 10-table footprint is still known from the SQL.

| widget | table | rows | projected columns | decoded bytes |
|---|---|---:|---|---:|
| borough_weekend_filter | DZP.BY_BOROUGH_DAY | 255 | BOROUGH_NAME, TRIPS, TIP_RATE, IS_WEEKEND | 8 KB |
| hour_zone_heatmap | PHD.BY_HOUR_ZONE_DAY | 90,383 | HOUR_OF_DAY, ZONE_NAME, TRIPS, SERVICE_ZONE | 5.25 MB |
| hourly_profile | PHD.BY_HOUR_DAY | 768 | HOUR_OF_DAY, DAYPART, TRIPS, AVG_FARE | 25 KB |
| kpi_totals | DZP.BY_DAY | 32 | TRIPS, FARE_AMOUNT, TIP_AMOUNT, TOTAL_AMOUNT | 1 KB |
| od_distance_filtered | DZP.BY_OD_DISTANCE | 245,570 | SERVICE_ZONE, TRIP_DISTANCE, ZONE_NAME | 11.08 MB |
| od_pairs_rollup | DZP.BY_OD_DAY | 245,570 | ZONE_NAME, DO_ZONE_NAME, TRIPS, AVG_FARE | 13.24 MB |
| payment_mix | FARE.BY_PAYMENT_DAY | 162 | PAYMENT_TYPE_NAME, TRIPS, FARE_AMOUNT, TIP_RATE, DAY_ID | 7 KB |
| rate_code_table | FARE.BY_RATE_CODE_DAY | 193 | RATE_CODE_NAME, WEEKDAY_NAME, TRIPS, FARE_AMOUNT, TOTAL_AMOUNT | 11 KB |
| revenue_by_month | DZP.BY_ZONE_DAY | 7,427 | DAY_ID, TOTAL_AMOUNT, AVG_FARE | (union below) |
| top_zones | DZP.BY_ZONE_DAY | 7,427 | ZONE_NAME, BOROUGH_NAME, TRIPS, REVENUE_PER_TRIP | (union below) |
| trips_by_day | DZP.BY_DAY | 32 | DAY_ID, TRIPS, TOTAL_AMOUNT | 1 KB |
| vendor_daily | FARE.BY_VENDOR_DAY | 64 | VENDOR_NAME, DAY_ID, TRIPS, AIRPORT_FEE | 3 KB |

Union for all 10 tables ≈ **31.3 MB** decoded; the seven runnable widgets touch ≈ **20.2 MB**.
Strings are charged at 16 B/row for the view plus the measured long-string bytes; fixed types
at 4/8/1 B/row plus one validity bit. This is the decoded cost only, not the executor's state.

One mart refresh (`r50sel.sql`) reads **four tables and 37 columns**: the 18-column fact plus
`DZP.DIM_ZONE_ZONE_ID` (5), `DZP.DIM_DO_ZONE_DO_ZONE_ID` (5) and `DZP.DIM_DATE_DAY_ID` (9).
Its fact scan projects all 18 columns, so a lazy store must decode the full ~370 MB fact. No
`WHERE` prunes the fact; the early aggregation is what produces 245,570 rows. The refresh's
2.31 GB peak is therefore ~0.9 GB decoded store + ~1.4 GB join/aggregate state; lazy storage
can only remove the first part (~370 MB of it), not the state (that is 136c/d territory).

---

## 2. What the format must add: per-chunk row counts

Each data chunk is self-contained and already carries almost everything lazy storage needs:

- page count, page index (offsets relative to the chunk start), pages, per-chunk CRC32C;
- `ColumnChunkDir { offset, length, page_count, encodings, crc32c, null_count, nan_count, min, max }`;
- table statistics and zone maps in the footer, independent of decoded data;
- hash-index definitions (not maps), vector-index serialized graphs, constraints and defaults.

What it does not carry is the row count of each chunk. A chunk is one encoded page, and after
`DELETE` a chunk can be short in the middle of a row group (`delete_from_chunk` gathers rows
and leaves a short chunk), so `min(2048, group_rows - 2048 * index_in_group)` is not exact.
Exact row counts are needed for `chunk_start`, `DataTable::get`, row-group accounting and for
`zone_may_match(rows, …)`; learning them by decoding is the whole cost we are trying to avoid
(a compressed page must be decompressed to see its row count).

The smallest complete change is to add one `Vec<u32>` of chunk rows to `RowGroupDir` (the
values are shared by every column in the group; a chunk has at most 2048 rows). QuackLab has
2,474 chunks (126 row groups), so the footer grows by 9,896 bytes; the data region does not change. This bumps
`FOOTER_VERSION` from 3 to 4. Version 3 files then fail with the existing clear message
("this .pink file was written by an older pinkdb … this build reads footer version 4 only");
there is no conversion path, consistent with the pre-1.0 rule. `PINKDB06` stays.

A clean chunk can be copied byte-for-byte into a new file: its internal offsets are relative,
its CRC stays valid, and only its outer `offset` in the footer changes. The new `chunk_rows`
entry is metadata, so it is copied from the handle as well. That is the property that makes
both lazy open and copy-through save possible.

`load_bytes` / `load_table_data` (`crates/pinkdb/src/persist.rs:4864`) currently decode every
chunk through `decode_column_chunk` (`:4914`). `write_table` (`:3881`) re-encodes every page by
calling `scan_chunk`, so today every `.save` also pays full decode + encode. The footer writer
is `to_bytes_inner` (`:3319`). The public `to_pink_bytes` / `from_pink_bytes` pair is used by
pink-lake data files and must keep working.

---

## 3. The three designs

### A. Chunk granularity (recommended)

`Column` keeps one entry per engine chunk, but the entry is a handle:

```text
ChunkHandle {
    rows: usize,                       // from footer; no decode
    encoded: Option<Encoded>,          // offset/len/page_count/encodings/crc32c into the image
    state: Mutex<Encoded | Decoded(Arc<Vector>)>,
    charge: Reservation,               // 136a pool, resized on decode/evict
    dirty: AtomicBool,
}
```

`Column.chunks` becomes private; accessors are the only way in:

- `Column::chunk(i) -> Result<Arc<Vector>>` — decode on first touch, verify CRC, clone the Arc
  (this clone is the pin), leave the decoded entry in the handle.
- `Column::chunk_rows(i)`, `Column::len()`, `Column::num_chunks()` — metadata only.
- `Column::chunk_mut(i) -> Result<&mut Vector>` — decode, mark dirty, `Arc::make_mut` as today.
- `Column::encoded(i) -> Option<&Encoded>` for the save copier.
- `Column::push_chunk(Arc<Vector>)` / `append_loaded_chunk` for writes and bulk load.

`DataTable::scan_chunk(idx)` becomes `Result<Option<DataChunk>>` and decodes every column
(today's semantics). A second accessor, `scan_chunk_projected(idx, present)`, decodes only the
columns the plan needs and inserts `Vector::new_empty(dtype)` placeholders for the rest; that is
exactly what `project_scan_chunks_of` (`crates/pinkdb/src/exec/mod.rs:3150`) needs, and it
avoids decoding 18 columns to read 4.

Two existing call sites deserve special mention because they currently force a full decode
even when nothing is projected:

- `scan_candidates` (`crates/pinkdb/src/exec/mod.rs:2765`) calls `t.scan_chunk(chunk)` only to
  read `num_rows()` and then applies zone maps. It must switch to `t.chunk_rows(chunk)`; the
  zone maps are already resident and pruning then costs no decode.
- `project_scan_chunks_of` currently calls `t.scan_chunk(i)` and replaces absent columns with
  empty vectors afterwards. It must call the projected accessor, or lazy storage silently
  decodes every column of every scanned chunk (a 60 MB BY_OD_DAY scan instead of 13 MB).

Failure handling changes: decode can now fail after open (corrupt page, OOM), so `scan_chunk`
and `DataTable::get` must return `Result`. That is the bulk of the mechanical port: 45
`scan_chunk` call sites, 9 `make_mut` sites, and ~116 `Column.chunks` expressions (203 `.chunks`
occurrences total, the rest are `DataChunk.chunks`) across 20 files. Doing this in step 1 while
still decoding eagerly keeps behaviour identical and isolates the compile fallout.

Expected numbers (derived from the measurements, not a lazy run):

- open: ~105–115 MB (11 MB binary + 94 MB image + 2 MB footer + handles), ~0.05–0.08 s
  (the 94 MB read alone measured 0.04–0.07 s);
- chart workload: ~130–145 MB, ~20–31 MB of decoded marts, first query ~10–30 ms;
- refresh: ~1.87 GB (94 MB image + 370 MB fact + the same ~1.4 GB state) instead of 2.31 GB;
- `.save`: copy 92 MB of clean chunks, ~0.1 s, no decode, byte-identical output.

Risk: the result-error plumbing is wide but compile-checked; the deferrable hash-index maps
must not become an open-time decode; a scan that materialises every chunk (`scan_output`
returns `Vec<DataChunk>`) pins its chunks until the pipeline ends, so lazy storage does not
bound a single large scan's peak (streaming local scans are a later wave).

### B. Table granularity

`DataTable` gains `encoded: Option<EncodedTable>` and `ensure_loaded()`; `Column` is unchanged.
The first access to any column decodes the whole table. Zone maps and statistics stay resident,
but a scan that survives pruning still decodes every column of every chunk: a chart scan of
BY_OD_DAY costs 60.6 MB instead of 13.2 MB, and the chart workload (10 tables) costs ~110 MB.
A full fact scan stays 370 MB. `.save` can copy a whole untouched table as one byte range;
one dirty row forces the whole table through decode/encode. Writing one row also loads the
whole table, so copy-on-write at table level is coarse.

Expected: open ~105–115 MB; chart ~220 MB; refresh ~1.87 GB; first query 20–60 ms. Code surface
is smaller (`Column` unchanged, no per-chunk handles), but every read path must call
`ensure_loaded()`; a missed call returns empty columns (silent wrong answer) instead of a
compile error. It also gives up the main benefit of lazy storage: projection and pruning no
longer reduce decode. Save copy is easy.

### C. Column granularity

`Column` holds an `EncodedColumn`; the first access decodes all chunks of that column. This
matches projection (scans project columns) and is simpler than A, but:

- zone-map pruning no longer helps: decoding a column decodes every chunk, including chunks a
  predicate excluded;
- eviction is per column, so a scan of one row pins a whole column;
- a one-row UPDATE decodes and later re-encodes an entire column (18 MB for a fact column);
- `.save` copies clean columns, but any dirty chunk re-encodes the whole column.

Expected: chart ≈ A (~31 MB, 4 projected columns each fully decoded); refresh ≈ 370 MB;
open ~105–115 MB. It is A with a coarser entry and no pruning benefit; strictly worse for the
workloads that motivated 136b.

### Rejected variants

- **Page granularity**: pages are already the unit inside a chunk; scans read whole chunks and
  zone maps are per chunk. More entries, no benefit.
- **Decode-on-every-scan, no cache**: lowest RSS but repeats decode for joins, index lookups and
  multi-statement workloads; no pool accounting.
- **mmap instead of a retained image**: saves 94 MB on native but is unsafe/portability-heavy and
  meaningless on wasm; `from_pink_bytes` already has the bytes in memory.

### Comparison

| | A chunk | B table | C column |
|---|---|---|---|
| open RSS / time | ~110 MB / ~0.07 s | ~110 MB / ~0.07 s | ~110 MB / ~0.07 s |
| chart RSS (7 widgets) | ~135 MB | ~220 MB | ~135 MB |
| refresh RSS | ~1.87 GB | ~1.87 GB | ~1.87 GB |
| first query | 10–30 ms | 20–60 ms | 10–30 ms |
| zone-map pruning | yes, no decode for pruned chunks | only before load; load decodes all | no (whole column decodes) |
| projection | decodes projected columns only | no | decodes projected columns |
| write / COW | one chunk; snapshots pin that chunk | whole table; whole-table copy | whole column; snapshots pin column |
| save untouched | copy each clean chunk | copy each clean table | copy each clean column |
| eviction safety | `Arc<Vector>` strong count under handle lock | table load state + refs | column load state + refs |
| code surface | private `Column` + ~250 mechanical sites | `Column` unchanged, every read path must load | private `Column`, fewer entries |
| main risk | Result plumbing; pinned large scans | silent empty columns; no projection/pruning benefit | coarse decode/evict; writes decode columns |

---

## 4. Recommended design in detail

### 4.1 Lifetime of the encoded image

`PinkDb` keeps one `Arc<[u8]>` of the file image (the plaintext image for an encrypted store).
Handles hold `(offset, length)` into it. Evicting a chunk drops its decoded `Arc<Vector>` and
the charge but keeps the descriptor; the image stays until the last handle that references it
is gone. A `.save` to the same path writes a temp file and renames, so the old image remains
valid for the running process; copy-through reads the old image, not the new file. This also
makes `.save` safe while a statement holds decoded vectors.

Keeping the image costs 94 MB of RSS. That is the price of ~0.07 s open and no I/O on first
query. A native-only `pread` variant could drop it to ~15 MB, but it does not work for
`from_pink_bytes`, encrypted stores, or wasm, and it turns first-query cost into disk I/O.
Recommendation: keep the image in 136b; revisit only if server RSS targets require it.

### 4.2 Decode, charge, pin, evict

- The expected charge is known before decode: `rows * fixed_width(dtype) + rows/8` for fixed
  types, `rows * 16 + encoded_len + rows/8` for VARCHAR (the decoded string buffer is at most
  the encoded payload; `StrVec::over_buffer` allocates it). Reserve the upper bound, decode,
  then `Reservation::resize` down to the actual `Vector` size.
- Add `Vector::memory_size()` (and a `StrVec::memory_size()`) that sums views capacity, owned
  buffer, shared buffers, validity and fixed payload. This is also needed by 136a's accounting
  tests; the pool must charge what the allocator actually holds, not a row-count guess.
- If `reserve` fails, evict clean unpinned chunks (global LRU of `Weak<ChunkHandle>`) and retry.
  Do not evict dirty chunks (they would need re-encoding) and do not evict a chunk whose
  `Arc<Vector>` has another owner. 136a's `memory::reserve` has no callback today; the smallest
  change is a `memory::set_evictor(fn(usize) -> usize)` hook registered by the storage cache, so
  operator reservations can also trigger chunk eviction before spilling. Coordinate this with
  136a (its file is still uncommitted).
- Eviction locks the handle, checks `Arc::strong_count(&vector) == 1` (the handle's own state is
  the only owner), replaces it with `Encoded`, and drops the reservation. Because accessors
  clone under the same lock, a scan that has an Arc can never race an eviction. This is the
  direct analogue of DuckDB's `BlockMemory::CanUnload()` returning false while
  `GetReaders() > 0` (`block_handle.cpp:109`) and `BufferPool::EvictBlocksInternal`
  (`buffer_pool.cpp:397`).
- Cache policy: evict on demand first. Optionally cap retained decoded bytes at
  `min(pool_limit / 2, 512 MB)` so a long-lived server does not hold every table it ever read;
  the exact fraction is an open question for the reviewer. `scan_output` currently returns a
  `Vec<DataChunk>` of the whole table, so all chunks of one scan are pinned simultaneously;
  eviction helps across statements and for small scans, not within a single full-table scan.

### 4.3 `.save` (checkpoint)

`write_table` gets a clean path: for each chunk, if the handle has `encoded` and is not dirty,
copy `image[offset..offset + length]` into the output; set the new `ColumnChunkDir.offset` to
the output position and keep `length/page_count/encodings/crc32c/null_count/nan_count/min/max`.
If the chunk is dirty, decode and run the existing encoder (page index offsets are rewritten by
that encoder). Schema changes (`ALTER TABLE`) mark every chunk dirty. The footer is rebuilt as
today, so statistics, zones, constraints and index definitions are unchanged.

This makes `.save` of an untouched store a ~92 MB memcpy (expect ~0.1 s, byte-identical output,
no 0.9 GB decode and no 1 GB peak). It also makes the CLI's "open → query → save" path cheap.
No data-byte change: clean chunks are self-describing and CRC-stable. The writer emits footer
version 4 with `RowGroupDir::chunk_rows` for both copied and re-encoded chunks.

### 4.4 Indexes, constraints, key maps, vector indexes

- Hash indexes: `apply_indexes` (`persist.rs:3260`) currently builds every map at load by
  scanning every decoded chunk (`index_map_for`, `storage.rs:227`). With lazy storage, keep the
  `IndexDef` and an `Index { map: Option<HashMap<..>> }`; `map == None` means "not loaded".
  `rebuild_stale_indexes` (`storage.rs:3562`) skips unloaded maps; the optimizer's
  `index_map_for` / `rebuild_indexes` path decodes only the indexed column when it actually
  chooses an `IndexScan`. A mutation sets the map to `None` (not a stale map), so the next
  indexed lookup rebuilds from live data. Results are unchanged; a plan may differ only in
  that an index is built on first use instead of at open.
- Key maps: `key_index` is already built on the first keyed write, never at open. It scans the
  key columns through the same accessors, so a lazy open pays for those chunks only when a
  keyed write occurs.
- Constraints: catalog metadata only, never enforced; no decode.
- Vector indexes: `load_bytes` keeps the serialized `blob` and `index: None`; hydration is the
  provider's job and needs no decoded rows (mutating an unhydrated graph is already refused).
  No change.
- Statistics and zone maps: already parsed from the footer; all pruning and `pinkdb_column_stats`
  paths keep working without decode. The zone/stat shapes are unchanged, so
  `set_loaded_stats` still validates alignment.

### 4.5 Writes and copy-on-write

Any mutation that touches a chunk goes through `chunk_mut`, which decodes, marks dirty and
leaves the handle decoded. Appends create new decoded dirty chunks. A snapshot that already
holds `Arc<Vector>` pins those chunks, so the cache cannot evict them; `Arc::make_mut` clones
only when the snapshot actually shares the chunk, exactly as today. Dirty chunks are never
evicted, so the COW path cannot be surprised by a handle that reverted to encoded. Bulk load
(`append_loaded_chunk`) and WAL replay use the same handle constructors; a WAL record only
decodes the chunks it modifies, so opening a store with a small WAL stays cheap.

### 4.6 wasm32, encryption, corruption

- wasm32: `from_pink_bytes` already receives the store as bytes; the image is `Arc<[u8]>`, no
  mmap, no temp files, and the decode path is the existing one. `cargo build -p pinkdb
  --target wasm32-unknown-unknown` is a CI gate (`.github/workflows/ci.yml:297`). The pool's
  `available_memory()` returns `None` on wasm, so the embedder must call `set_memory_limit`;
  otherwise the cache is unbounded (document this).
- Encrypted stores: `open_with_key` already decrypts the whole container to a `Vec<u8>`; that
  becomes the retained image. Behaviour is unchanged, but the plaintext now lives for the whole
  session instead of being dropped after decode. This is a real exposure increase; the image
  should be zeroized on drop when the last handle goes away (open question: use the existing
  crypto dependency or a small `Drop`/`zeroize` helper; do not add a dependency just for this).
- Corruption: today every chunk is CRC-checked during open. Lazy open would defer that to first
  access, so a corrupt untouched chunk would make `open` succeed and a later query fail. Two
  options: (a) accept the timing change and document it; (b) add a cheap CRC-only sweep at open
  (read 92 MB, verify each `ColumnChunkDir.crc32c`, no allocation) — this keeps open-time
  detection and costs ~0.01–0.03 s. Recommendation: (b), because "open succeeds then a random
  query fails" is a worse failure mode; a decode failure still returns an error (CRC-valid but
  malformed data is a trust-boundary case, never a panic).

---

## 5. Impact on stream B's lake work

The design does not require any lake change. `decode_projected_columns`
(`crates/pinkdb/src/persist.rs:4415`) and the lake data-file reader are independent. The only
compile-time fallout is the mechanical `Column.chunks` accessor swap, confined to two files:

- `crates/pink-lake/src/attach.rs`: 6 lines (1308, 1311, 2420, 2421, 2462, 2463) — 9 expressions.
- `crates/pink-lake/src/incremental.rs`: 10 lines (514, 583, 617, 621, 629, 630, 656, 658, 712, 729).

No `Column.chunks` sites exist in `crates/pink-lake/src/database.rs` or
`crates/pinkdb/src/lake_db.rs`. `scan_chunk` calls in `pink-lake/src/{lib,format}.rs` keep their
current signature if `scan_chunk` still decodes all columns (it does). These 16 lines can be
swapped by the 136b author in step 1; stream B's own edits to `attach.rs`/`incremental.rs` are
in different functions (lake I/O), so the conflict surface is small but should be sequenced by
the reviewer.

---

## 6. Implementation steps (one pi brief each)

Each step leaves TPC-H and the gates green; each is independently reviewable.

1. **136b-1 — accessor layer, eager behaviour.** Add `ChunkHandle` (encoded descriptor, rows,
   state, charge, dirty), make `Column.chunks` private, add the accessor set and
   `scan_chunk_projected`/`chunk_rows`, port the ~250 sites (203 `.chunks`, 45 `scan_chunk`,
   9 `make_mut`, lake's 16 lines), and keep decoding every chunk at open exactly as today.
   `DataTable::get`/`scan_chunk` return `Result` so step 2 has an error path. No format change.
   Gate: full (public type), TPC-H, wasm check.
2. **136b-2 — lazy open + projected scans + footer v4.** Add `RowGroupDir::chunk_rows`, bump
   `FOOTER_VERSION` to 4, and make `load_table_data` create handles from the footer instead of
   decoding; `chunk()` decodes on demand; `scan_candidates` uses `chunk_rows`;
   `project_scan_chunks_of` uses the projected accessor; add the open-time CRC sweep decision.
   Old v3 files get the existing clear version error. Gate: full (format + open semantics),
   results-equality vs eager, v3-rejection test, corrupt-store tests, wasm.
3. **136b-3 — pool charge + eviction.** `Vector::memory_size`, per-handle `Reservation`,
   global LRU, evict-on-reserve-failure, dirty/pinned rules, the 136a evictor hook.
   Gate: full, 136a low-limit tests, OOM/eviction tests.
4. **136b-4 — save copy-through.** Clean-chunk byte copy, dirty re-encode, schema-change
   invalidation, encrypted save, byte-identical `.save` test. Gate: full, writer tests.
5. **136b-5 — deferred hash-index maps.** `apply_indexes`/`index_map_for`/`rebuild_indexes` and
   the optimizer path; no decode at open for indexed tables. Gate: full (planner semantics),
   TPC-H.
6. **136b-6 — WAL/encrypted/wasm and benchmark record.** WAL replay dirty marking, encrypted
   store test, wasm smoke, QuackLab measurements with the lazy binary, docs update. Gate: full
   including wasm.

If steps must be merged, merge 5 into 2 and 6 into 4; do not merge 1 with 2, because step 1 is
the compile-driven port and step 2 is the behaviour flip.

### Full-gate triggers

- Step 1 changes a public type (`Column.chunks` is `pub`; `DataTable.columns` stays `pub`) and
  the `scan_chunk`/`get` signatures → full gate.
- Step 2 changes the footer version (3 → 4, no legacy reader) and open/load semantics (and
  the CRC policy) → full gate.
- Step 3 changes memory accounting/eviction → full gate.
- Step 4 changes the writer → full gate; it writes the v4 `chunk_rows` field introduced in
  step 2. The data bytes and `PINKDB06` are unchanged, and there is no legacy read path.
- Step 5 changes optimizer plan choice → full gate.
- wasm32 check is required for every step (the crate's core must stay wasm-compatible).

---

## 7. Open questions for the reviewer

1. **Image retention**: keep the 94 MB `Arc<[u8]>` (simple, wasm/encrypted-safe) or add a
   native-only paged reader to get server RSS to ~40 MB? Recommendation: keep the image in
   136b; revisit if the 70–90 MB server target is hard.
2. **CRC timing**: open-time CRC-only sweep (recommended, ~0.01–0.03 s) or lazy first-access
   failure with documentation? Either way decode errors must propagate, never panic.
3. **Eviction hook**: extending 136a with `memory::set_evictor` couples the two waves; the
   alternative is a storage-local budget that operators cannot borrow from. Which does the
   reviewer prefer? The plan's "over the limit, first evict, then spill" needs the hook.
4. **Cache cap**: is `min(pool/2, 512 MB)` the right retained-decoded limit, or should the cache
   only evict on pressure (retain everything otherwise)?
5. **`scan_output` materialisation**: it returns all chunks of a table, so a full fact scan
   pins ~370 MB regardless of eviction. Do we accept that in 136b (peak r50 ~1.87 GB) and defer
   streaming local scans to 136c, or does 136b need a streaming scan to meet the RSS goal?
6. **Deferred indexes**: fully lazy (plans may choose an index only on second use) vs eagerly
   building small indexes (< 1 MB decoded) at open. Recommendation: fully lazy; results are
   identical, and QuackLab has no indexes.
7. **Encrypted plaintext lifetime**: zeroize the retained plaintext image on last drop, and with
   which existing dependency? This is a security regression relative to today's open (which
   drops the plaintext after decode).
8. **wasm limit**: the embedder must set a limit because `available_memory()` is `None`; should
   `pinkdb-wasm` set a default derived from the host, or document it as required?
9. **Small-table eager exception**: decode tables under, say, 256 KB at open to avoid per-chunk
   overhead on the 80 tiny tables? Measurements say the chart workload's projected decode is
   only ~20–31 MB, so the exception is not needed; confirm with a lazy benchmark before adding
   a threshold.

---

## 8. Test plan for the implementation waves

- Results equality: TPC-H SF1 and the QuackLab SQL (all statements that 135d supports) must
  return byte-identical results to the eager build; `.save` of an unchanged store must be
  byte-identical to the original file.
- Lazy correctness: open with a low pool limit, scan a table, evict, scan again; a scan holding
  `Arc<Vector>` must prevent eviction and still see its data.
- Corruption: flip a byte in a chunk; open must fail with the current "corrupt file" error if
  the CRC sweep is adopted; otherwise the first access must return that error.
- Writes: INSERT/UPDATE/DELETE/MERGE, WAL replay, `ALTER TABLE`, keyed writes, index rebuilds,
  vector-index mutation refusal, encrypted open/save, wasm smoke. A footer-v3 file must fail
  with the clear older-version error (no conversion).
- Measurements: open RSS/time, chart workload RSS/time, one mart refresh RSS/time, `.save`
  time/RSS, and the number of decoded chunks per query (add a test-only counter or read
  `memory::peak()` around each statement).
