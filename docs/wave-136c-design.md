# Wave 136c — compressed-resident storage: design

136b made decode lazy but left the *compressed* bytes eager: a lazily opened store still reads the
whole file into one heap buffer. This wave moves the compressed bytes out of the process. The file
becomes the byte source, chunks are read with positioned reads on demand, a scan decodes a chunk,
uses it and drops it, and the decoded copy survives only in a bounded cache of its own — small,
separate from `memory_limit`, and only for the paths that revisit a chunk.

Design only: no code changes. The steps are in §8; the decisions the reviewer and stream A owe are
in §9.

---

## 1. Where the bytes are today

`storage::StoreImage` (`crates/pinkdb/src/storage.rs:73`) is `Box<[u8]>` plus a `secret` flag, and
every `EncodedChunk` (`storage.rs:121`) holds `Arc<StoreImage>` + `(offset, length, rows)`.

- `PinkDb::open` (`persist.rs:4497`) does `std::fs::read(path)`; `from_pink_bytes`/`load_bytes`
  (`persist.rs:4568`) copy the caller's slice; `open_with_key` (`persist.rs:4510`) reads the whole
  `PINKENC1` container and decrypts it whole.
- `load_table_data` (`persist.rs:6421`) then `par_iter`s every column chunk through
  `verify_column_chunk` (`persist.rs:6483`): a CRC32C over each chunk's bytes, the page index
  checked against the footer, and the page ranges that the handles carry.
- The image is dropped only when the last handle holding it dies. `Column::clone`
  (`storage.rs:574`) duplicates handles, each keeping `encoded` — so a snapshot pins the image too.

Measured (136b-4): QuackLab's store is 93,983,593 B (2,008,242 B of it footer) and opens at 118 MB
RSS; TPC-H SF1 is 323,433,245 B and opens at 350 MB. On the chart load the server sits at 412 MB,
"mostly the 90 MB image plus allocator churn". One decoded copy of lineitem is ~620 MB of the
973 MB Q1 peak, because every decoded chunk is cached until the existing LRU evicts it.

DuckDB does not do this. Its `BlockManager` reads blocks with positioned reads on demand
(`src/storage/block_manager.cpp`), its buffer manager keeps the *compressed* blocks under
`memory_limit` with pin counts and eviction (`src/storage/buffer_manager.cpp`), and a
`ColumnSegment` decodes a vector at a time into the scan state
(`src/storage/compression/column_segment.cpp`, `src/storage/table/table_scan_state.cpp`). We port
the shape, not the code: the block reader and the fetch-on-demand scan state. We do **not** port the
compressed-block cache — see §2.2.

---

## 2. The byte source

### 2.1 `StoreSource` replaces `StoreImage`

`storage.rs:73` becomes:

```rust
/// The store's bytes: the file itself, or memory.
pub(crate) struct StoreSource {
    data: SourceData,
    /// Plaintext from an encrypted container: wiped on drop. Only the memory
    /// form is ever secret; the file form holds ciphertext.
    secret: bool,
}

enum SourceData {
    Memory(Box<[u8]>),
    File { file: Arc<std::fs::File>, len: u64 },
}

impl StoreSource {
    fn memory(bytes: Vec<u8>, secret: bool) -> Arc<Self>;
    fn file(file: Arc<std::fs::File>, len: u64) -> Arc<Self>;
    fn len(&self) -> u64;
    /// Fill `buf` from `offset`. The one place bytes enter the process.
    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<()>;
    fn footer(&self) -> Result<(u64, Slot, StoreFooter)>;
}
```

`read` on `Memory` is a range check plus `copy_from_slice`, with `EncodedChunk::decode`'s error text
for a range outside the file ("corrupt file: a chunk is outside the file"). On `File` it calls
`pread_exact` (`persist.rs:5363`), which already has the platform split — `FileExt::read_at` on
unix, `FileExt::seek_read` on Windows, and the `Unsupported` error elsewhere. Move it to
`storage.rs` next to `StoreSource` and leave `pwrite_all` where it is.

`footer()` is the open path: two slots (8,192 B) and the footer, then the same
`valid_slot`/`parse_store_footer` validation `read_checkpoint_file` (`persist.rs:5340`) already
does for a checkpoint. For `Memory` it slices; for `File` it preads. `PinkDb::load_image`
(`persist.rs:4573`) becomes `load_source(Arc<StoreSource>)`, which calls `source.footer()` and then
does exactly what it does today. `open` = `File::open`, an 8-byte magic read to keep the
`PINKENC1` refusal, `footer()`, `load_source`. `read_store_footer` keeps serving `to_bytes` callers
that hold a slice.

The `PINKDB07` layout means open reads **8,192 B + the footer** (2.0 MB for QuackLab, 2.1 MB for
SF1's footer) instead of the whole file. Nothing in the data region is touched.

**Memory stays the source for:**

- **wasm** — `pinkdb-wasm` loads through `from_pink_bytes`; `std::fs::File` reads exist but nothing
  opens one, and `read` on a file source would be the cfg-else `Unsupported` error anyway.
- **`from_pink_bytes` / `load_bytes`** — an in-memory store has no path to read from.
- **encrypted stores** — `PINKENC1` is one sealed blob; `open_with_key` decrypts it whole and keeps
  the plaintext as `StoreSource::memory(plain, true)` so `Drop` can wipe it (`storage.rs:96`). The
  container becomes block-sealed only in a later wave (136w3's `PINKENC2`), and only then can an
  encrypted store read its bytes on demand. Say so in the module docs; do not pretend otherwise.
- **`to_bytes` / `VecSink` saves** — the bytes leave the process (§5).

### 2.2 No mmap

The page cache already holds the compressed bytes: a `pread` hits the same cache an `mmap` fault
would, without a user-space block cache and without a new dependency. mmap would also mean
`SIGBUS`/`EXCEPTION_IN_PAGE_ERROR` on a truncated file or a full disk (a process abort, not a
`Result`), a second copy of the format's range checks in the kernel, and no `wasm` story. Positioned
reads keep every failure an error with the text it has today.

### 2.3 Lifetime, sharing, and the per-handle descriptor

`PinkDb` keeps `source: Arc<StoreSource>` (one descriptor per open store) even though today the
image survives only through handles. Handles clone the `Arc`, so a store whose chunks are all
decoded still holds its file open — cheap, and it means a later read never has to reopen and re-check
the header. Reopening a file (`PinkDb::open` again) is a new `StoreSource`.

A column chunk's bytes are described once, not once per page:

```rust
pub(crate) struct EncodedColumnChunk {
    source: Arc<StoreSource>,
    offset: u64,      // absolute file offset of the page index
    length: u64,      // index + pages, the CRC's granule
    crc32c: u32,
    page_count: u32,
    state: Mutex<BytesState>,
}

struct BytesState {
    pages: Option<Arc<[(u32, u32)]>>,  // (rel, len), parsed once (§3)
    verified: bool,                     // the CRC has been checked (§3)
}

pub(crate) struct EncodedChunk {
    chunk: Arc<EncodedColumnChunk>,
    page: u32,
    rows: usize,
}
```

The up-to-60 handles of one column chunk share one `Arc<EncodedColumnChunk>`. A handle shrinks from
today's 32 B (`Arc<StoreImage>` + `offset` + `length` + `rows`) to 24 B (`Arc`, `page`, `rows`); the
shared descriptor holds what used to be per-handle (the source, the offset, the length) plus the CRC,
the page count and the verified/page-index state. Built at open from the footer with **no reads** —
the constructor is `EncodedColumnChunk::from_dir(source, &ColumnDir)`.

A snapshot's `Column::clone` shares the same `Arc<EncodedColumnChunk>` instead of a fresh
`Arc<StoreImage>`; that sharing is what §4's rule tests.

### 2.4 Reading a page

Decoding a page needs the page's `(rel, len)` and the CRC-verified chunk:

```rust
impl EncodedColumnChunk {
    /// The whole chunk verified once, and the page index parsed once.
    fn ensure_pages(&self, scratch: &mut Vec<u8>) -> Result<Arc<[(u32, u32)]>>;
    fn ensure_verified(&self, scratch: &mut Vec<u8>) -> Result<()>;
    /// Verify if needed, then read this page alone and decode it.
    fn decode_page(&self, page: u32, dtype: DataType, scratch: &mut Vec<u8>) -> Result<Vector>;
}
```

`scratch` is a thread-local `Vec<u8>` (`fn with_scratch<R>(f: impl FnOnce(&mut Vec<u8>) -> R) -> R`
in `storage.rs`). The CRC is streamed over the chunk in 128 KB blocks — never the whole chunk in one
allocation — and the page read reuses the same buffer. Per worker that is 128 KB plus one page,
bounded and uncharged: it is a scratch buffer, not cached state. Streaming needs one small split in
`persist.rs`, where `crc32c` (`persist.rs:679`) is a table loop over one slice:
`fn crc32c_update(crc: u32, bytes: &[u8]) -> u32` with `pub fn crc32c(bytes) = !crc32c_update(!0,
bytes)`. The byte-wise remainder loop is order-preserving, so per-block updates compose to the same
value, and the existing `crc32c_matches_the_standard_check_value` test (`persist.rs:7696`) pins it.

`ChunkHandle::get` (`storage.rs:273`) clones the `EncodedChunk` under the handle lock (as today) and
then reads without it. That is what makes §3 and §4 sound: the cloned `Arc<EncodedColumnChunk>` keeps
the range *named*, and a named range is never overwritten.

---

## 3. The CRC check moves to first read

Today `load_table_data` verifies every column chunk at open. After this wave:

**At open (footer-only, no reads of the data region).** For every column chunk keep the cheap
static checks: `offset >= DATA_START`, `offset + length <= slot.file_len`, `length >= 4 +
8 * page_count`, `page_count > 0`, no overlap with another row group's ranges and none with a
`free_ranges` hole (`check_free_ranges`, `persist.rs:6388`), and `total_rows == tdir.row_count`
("corrupt file: table \"{}\" claims {} rows but its row groups hold {total_rows}"). These catch a
malicious or truncated footer before any read, with the same texts as today.

**At first read of a column chunk.** `ensure_verified` reads the chunk's `length` bytes in 128 KB
blocks, keeps a running `crc32c`, and on mismatch returns today's text verbatim:

    corrupt file: a column chunk fails its checksum (the file is damaged)

`ensure_pages` then reads the page index and validates it exactly as `verify_column_chunk` does today
(count against the footer: "corrupt file: the column chunk's page count disagrees with the footer";
each `(rel, len)` contiguous, in range, and filling the chunk exactly). Both results are cached in
`BytesState` under the descriptor's mutex: a column chunk costs one CRC pass and one index parse over
its life, and each page after that one cheap look at the two cached fields.

**Why the column chunk is the granule.** The format stores one CRC per column chunk; a per-page CRC
would be a footer format bump bought only to avoid reading bytes the scan is about to read anyway:

| store | column chunks | average chunk | largest (fact / lineitem) |
|---|---|---|---|
| QuackLab `ql.pink` | 2,474 | 37 KB | ~122 KB |
| TPC-H SF1 lineitem | 784 (16 columns × 49 row groups) | ~290 KB (derived: file share ÷ column chunks) | — |

A scan walks pages 0..59 of the same column chunk consecutively (`StoreScan::chunk` indexes
`group = i / 60, page = i % 60`), so the verification read is the read the scan needs, one granule
early, and its bytes stay in the page cache for the pages that follow. A point lookup pays one
extra chunk read (37 KB typical, 290 KB worst) once per column chunk — against today's 94–323 MB at
open, and again nothing after the first read.

**Consequence to document:** a corrupt file now opens and fails the query that reads the damaged
chunk. The `persist.rs` module docs say so, and the corruption tests move from "open fails" to "the
reading statement fails with this text". A damaged *footer* still fails at open.

---

## 4. Checkpoint holes and live handles

136w3's incremental checkpoint overwrites free ranges in the same file
(`checkpoint_in_place`, `persist.rs`), and `reusable_group` keeps a row group where it is when its
bytes are unchanged. A group that is re-encoded leaves its old ranges in the new footer's
`free_ranges`, and the next checkpoint may allocate them (`first_fit`, `alloc_footer`).

**The invariant.** A handle that is still encoded can name a range only through
`Arc<EncodedColumnChunk>`, and that `Arc` is cloned under `ChunkHandle`'s lock before any read
(§2.4). So:

> A range named by a live `Arc<EncodedColumnChunk>` is never overwritten.

Today's per-column-chunk CRC means a violation is a spurious "corrupt file", never silent
corruption — but a live snapshot must not see it at all.

**Which holder is at risk.** Not a running query: `StoreScan` borrows `&DataTable` and calls
`chunk()` on the live handle, and a checkpoint's repoint takes that handle's lock, so a scan sees
either the old descriptor (whose bytes are still allocated in the *current* footer, because a
checkpoint never reuses a range it frees itself) or the new one. The risk is a **snapshot**:
`Column::clone` (`storage.rs:574`) duplicates handles, and each duplicate keeps `encoded` — a
snapshot taken before checkpoint N still names ranges that checkpoint N freed, and checkpoint N+1
may hand them to another group.

**Chosen rule: one generation of grace, enforced by weak references.** A hole is offered to
`first_fit` only when the `Arc<EncodedColumnChunk>` that used to occupy it is dead.

- `PinkDb` gains `superseded: Mutex<Vec<SupersededRange>>` where
  `struct SupersededRange { offset: u64, len: u64, chunk: std::sync::Weak<EncodedColumnChunk> }`.
- When a checkpoint supersedes a group, it records the old `ColumnDir`'s ranges with the `Weak` of
  the descriptor it repointed away from. Ranges that no handle ever named get a `Weak` that dies as
  soon as the checkpoint drops its own `CopyThrough` reference, so they are free at the next
  checkpoint without ever having been withheld for long.
- At the start of the next checkpoint, `sweep_superseded` drops entries whose `Weak` no longer
  upgrades (their ranges are ordinary holes) and keeps the rest. `CheckpointWriter` filters
  `first_fit` and `alloc_footer` through the kept list, so a range a snapshot still names is
  skipped even though the footer's `free_ranges` contains it.
- The footer's free list stays **complete**. A withheld range is still listed as free; the filter is
  in memory only. That is what makes the rule leak-free: if the snapshot dies, the range is
  allocated by the next checkpoint without any footer bookkeeping; if the process dies, the wait
  list dies with it and a reopened store may allocate every hole, which is correct because no
  handle survives a reopen.

**Cost.** One `Weak` per superseded column chunk (a few dozen bytes), one sweep per checkpoint, a
`HashSet` lookup per free entry. A hole becomes allocatable at the second checkpoint after it is
freed, so the transient peak of the file is one checkpoint's superseded bytes higher than today.
A full rewrite allocates no holes at all and clears the wait list.

**Why not the alternatives.** A per-range refcount is the same information with a counter to keep
consistent on every clone (including `duplicate`, `evict`, and `Column::drop`'s parallel path), and
it cannot be stored in the footer either — so it buys nothing over a `Weak`. An epoch on the file
(handles record the generation they were opened at, holes carry the generation they were freed at)
is coarser in the wrong direction: reused groups keep handling their old generation's ranges, so the
store's own handles would have to be exempted from the test, which is exactly the distinction the
`Weak` makes directly. Reading the superseded ranges into memory before reuse is the third option
and the one to reject on cost: it is what today's retained image does implicitly, so every
in-place checkpoint would allocate the superseded bytes (the groups that changed — tens of MB for a
mart refresh, up to the whole file for a schema change) and hold them until it could prove no
snapshot remained, which it cannot, so the peak returns every checkpoint. That is the memory this
wave exists to remove.

**Ordering constraint for stream A.** The repoint of live handles must complete before the checkpoint
lock is released, and checkpoints must stay serialized. Between "the new footer's slot is durable"
and "the repoint has run", the live tree still names the freed ranges; a concurrent checkpoint in
that window would be the only way to overwrite them.

### 4.1 Full rewrite to a new file

`save_sync(tmp)` writes a new inode that the WAL then publishes over the target. Every chunk is
re-encoded or copied through into that file, so **every** handle must be repointed, not only the
ones `write_table` reports (§5). The repoint uses absolute offsets, which do not change across the
rename, so only the `Arc<File>` changes.

- Unix: the old inode stays readable through the handles that still hold it, and is unlinked when
  the last one closes. A snapshot mid-rename keeps reading a consistent old image.
- Windows: `std::fs::File::open` and `crate::fsutil::create_new_private` (`fsutil.rs:87`, a plain
  `OpenOptions`) share `FILE_SHARE_DELETE`, so `rename` over an open target succeeds; the old file
  becomes delete-pending and its bytes stay readable through the open handles until the last close.
  There is no "file in use" failure to handle.
- Cross-process: only one process owns a store file (WAL + checkpoint), so a repoint never has to
  reach another process's handles. If a second process opened the target earlier it keeps the old
  inode — correct, and out of scope.

---

## 5. Copy-through and save

`write_image_hooked` (`persist.rs:3703`) currently accumulates the whole data region in
`carry: Vec<u8>` (88 MB for QuackLab, 323 MB for SF1) and `repoint_carried` (`persist.rs:3821`)
builds an in-memory `StoreImage` from it just to point the saved handles somewhere. After this wave
the saved chunks point at the file they were written to.

- `CopyThrough { source: Arc<StoreSource>, start, end, pages }` (`persist.rs:5419`);
  `copy_through` (`persist.rs:5429`) validates the stored page index against the footer as today, then
  streams the range through a `Sink` (`fn copy_from(&mut self, source: &StoreSource, start, end)` in
  the `Sink` trait, `persist.rs:749`, with a 64 KB buffer). No slice of a whole image is taken, and
  `stored_group` (`storage.rs:754`) returns `(Arc<StoreSource>, Vec<EncodedChunk>)`.
- `Sink::source(&self) -> Option<Arc<StoreSource>>`: `FileSink` (`persist.rs:798`) holds
  `shared: Arc<File>` (a `try_clone` taken in `FileSink::new`) and returns
  `Some(StoreSource::file(shared, pos))`; `VecSink` returns `None`.
- `write_image_hooked` appends to `carry` only when `data.source()` is `None`. For a file sink the
  re-encoded chunk's carry entry is its `(absolute offset, length, rows)` and the repoint is
  `column.repoint_chunk(i, &source, offset, length, rows)` — no `DATA_START` shift, because file
  offsets are absolute.
- `repoint_carried(carry, repoints, encrypted)` becomes
  `repoint_written(target: Option<Arc<StoreSource>>, carry: Vec<u8>, repoints: Vec<(&DataTable,
  TableCarry)>)`: a `Some` target repoints in place; `None` keeps today's prefix-and-carry path,
  which is what `to_bytes`/`to_bytes_inner`/`save_with_key` and encrypted stores need.
- A full rewrite repoints **all** chunks at the tmp file's source (the file it just wrote), and the
  repoints `write_table` reported become redundant — `repoint_written` can ignore them when the target
  is the file every chunk now lives in. Simplest correct form for step (3): one
  `repoint_all(&source)` plus the per-table repoints for the in-place case.

This removes the carry from the file path entirely. Peak memory for `.save` drops by the data
region's size (88 MB / 323 MB), which is also most of the 412 MB the server holds on the chart load.

---

## 6. The scan path

The streaming scan is already shaped for this: `StoreSource` (`exec/pipeline.rs:1115`) hands a
morsel to each rayon worker, and `decode` iterates the morsel's chunks, calling `self.scan.chunk()`
and `sink(chunk)?` with the chunk dropped at the end of its `flow` (`run_streamed`,
`pipeline.rs:1225`). What still retains bytes today is `ChunkHandle::get`, which stores the decoded
vector in the handle and registers it in the LRU.

**Step (1) changes no scan behaviour** — the bytes just come from the file.

**Step (2) makes the streaming scan decode uncached.** `Column` gains

```rust
pub(crate) fn chunk_uncached(&self, i: usize) -> Result<Arc<Vector>>;
```

which resolves the handle's `EncodedChunk` (clone under the lock), decodes with the thread-local
scratch, and returns the vector **without** touching `ChunkState::payload` and without registering
in `DECODED`. `DataTable::scan_chunk_uncached` / `scan_chunk_projected_uncached` do the same per
column, so `StoreScan` (exec/mod.rs:3061) gains a `cache: bool` field and `StoreScan::new` a matching
argument: `false` from `store_scan` (the streaming path: `pipeline.rs:1093`), `true` from the
materialized paths (`scan_output`, `materialize`, the lake provider) that hold chunks past the call.

Per-worker cost is one chunk's decoded columns — `Σ decoded_bound(dtype, rows, len)` over the
projected columns, ~0.5 MB for a 16-column lineitem chunk — dropped when the chunk's `flow`
returns. Ten workers: ~5 MB against Q1's present 620 MB of decoded lineitem.

- **Zone pruning** happens before any chunk is read: `scan_candidates` reads the footer's zone maps,
  which `load_table_data` already materializes from the footer (`set_loaded_stats`). No change.
- **Projections** are already per-column: `scan_chunk_projected` decodes only the marked columns, and
  the placeholders stay checked (`DataTable::expect_column`). Unchanged.
- `decode_all` (`storage.rs:2386`) is the lake provider's "materialize the table" call. Decoding
  everything and caching it is now the wrong shape; step (2) replaces it with
  `verify_all(&self)` (stream the CRC of every column chunk, decode nothing, charge nothing) and the
  lake path calls that after ingest. A lake reader then decodes through the cache as it reads.

**Callers that need a decoded chunk to persist**, and how they get it:

| caller | why it persists | path |
|---|---|---|
| `Column::chunk_mut` / `set_chunk` / DML, `INSERT`, `UPDATE`, `DELETE`, `MERGE` | it is the truth until saved | `ChunkHandle::get(dirty=true)`: charged to `memory_limit`, never cached, `evict` returns `Never` while dirty |
| `DataTable::get`, keyed lookups, index probes | revisited by the `WHERE` that chose them | `chunk()` — cached, in the decoded budget (§7) |
| index builds (`apply_indexes`, `index_map_for`, `rebuild_indexes`) and HNSW inserts | the map holds row ids and keys, the vector index holds vectors — never chunks | decode through `chunk()` (cached); the map is charged to the operator pool, not the decoded cache |
| a snapshot (`Column::clone`) | MVCC readers share payloads | shares the existing `Arc<Vector>`; never decodes on its own |
| `scan_output` / `materialize` | the rows outlive the scan | `StoreScan { cache: true }` |

---

## 7. The decoded-cache budget

**Name and default.** `decoded_cache_limit`, a pinkdb setting (`PINKDB_SETTINGS`,
`settings.rs:248`), settable as `SET decoded_cache_limit = '64MB'`, `0` = no cache, `-1`/`NULL` = the
whole free room, `RESET` = default. There is no separate share: like DuckDB's buffer pool inside
`max_memory`, clean decoded chunks take whatever `memory_limit` leaves free
(`USED + CACHE_USED ≤ limit`), an operator reservation that does not fit evicts them in one batch
(`evict_request` asks for the shortfall, not `2 × bytes`), and an explicit value caps them below
that. The ladder measured for a fixed cap (§10) showed why a *small* fixed share is wrong: 256 MiB
thrashed at 2.68, 512 MiB lost 17%, 768 MiB first held parity (0.989) and 1 GiB gave 0.988, so a
share would have to track the pool; the free-room rule needs no such constant.

**Accounting** (`crates/pinkdb/src/memory.rs`). The pool keeps two counters:

```rust
static USED: AtomicUsize;        // operators, and dirty decoded chunks
static CACHE_USED: AtomicUsize;  // clean decoded chunks
static CACHE_LIMIT: AtomicUsize; // 0 = follow limit() / 4
```

`Reservation` gains `Account::Decoded`; `reserve` stays the operator path, `reserve_decoded` the
cache path. They are separate on purpose: `memory_limit` remains the cap for operators, and a scan
that needs memory can no longer evict a hash join's build side to make room for a cache entry (the
136a evictor hook stays wired for the operator pool only).

**Admission.** `ChunkHandle::get(dirty=false)` calls `reserve_decoded(decoded_bound(...))`. On
failure it runs `evict_decoded(want)` (the existing LRU and `install_evictor`, `storage.rs`), retries
once, and on a second failure **decodes without caching**: the vector is returned to the caller and
dropped when the caller is done, and `ChunkState::payload` stays `None`. That keeps the budget hard
without failing a query on cache pressure; the write path (`dirty=true`) keeps today's behaviour of
charging the operator pool and failing with `out_of_memory` when there is truly no room.

**Eviction.** Unchanged in kind: the same `DECODED` LRU of `Weak<ChunkHandle>`, the same
`ChunkHandle::evict` rules (`Busy` when a snapshot/scan holds the payload, `Never` when dirty or
already encoded, `Freed(bytes)` otherwise), now released into `CACHE_USED`. `evict_decoded` stops
once it has freed the requested bytes.

**Interaction with `memory_limit`.** `SET memory_limit` recomputes the cache default while the
setting is not explicit, and clamps a `CACHE_USED` overage down on the next eviction pass;
`RESET`/`RESET ALL` restores both defaults. `used()` (operator usage, the value spill and `temp`
decisions read) is unchanged, so no planner or spill path starts counting cache entries as
"pressure". Total resident decoded state is `USED + CACHE_USED`, which is what the RSS targets in §8
measure.

---

## 8. Measurement plan and steps

Baselines to reproduce before touching anything (they are 136b-4 numbers): open RSS/time for
`ql.pink` and TPC-H SF1, the QuackLab chart load (16 clients × 10 rounds) RSS/time on
`pinkdb-server`, TPC-H SF1 warm A/B, Q1 peak RSS, `.save` time/RSS. Same machine, same store copies,
`/usr/bin/time -l` for the peak, the `bench-c` binaries for before/after.

Targets after all three steps: chart load ≤ 51 MB RSS (DuckDB's number), SF1 warm A/B ≤ 1.03, Q1
peak far below 973 MB, and open RSS/time down by the file size (open should be ~footer-sized).

### Step 136c-1 — the byte source and the lazy CRC

`StoreImage` → `StoreSource`, `EncodedColumnChunk`, `pread_exact` in `storage.rs`, `open` through
`StoreSource::footer`, `load_source`, CRC and page-index validation on first read, thread-local
scratch. No scan or save change. **Gate: full** (open semantics and the corruption contract change).
Tests: open a store and assert the source is a file and nothing in the data region was read (a
`StoreSource` test hook counting `read` bytes); a `.save`/`open` round trip is byte-identical; flipping
a chunk byte still opens, and the reading statement fails with "a column chunk fails its checksum";
a flipped *footer* still fails at open; a truncated file fails at open with today's text; tagged
pages beyond `page_count`, an index that does not fill the chunk, and a chunk outside the file each
keep their current message at first read; wasm smoke (`from_pink_bytes`, scan, save); encrypted
open/save unchanged. Measurements: open RSS/time on both stores.

### Step 136c-2 — decode-and-drop scans and the budget

`chunk_uncached`, `scan_chunk_*_uncached`, `StoreScan::cache`, `verify_all`, `CACHE_USED`/
`CACHE_LIMIT`/`reserve_decoded`/`Account::Decoded`, the `decoded_cache_limit` setting and both `SET`
sites (`exec/mod.rs:1139`, `set_setting` at `exec/mod.rs:1206`), settings tests. **Gate: full**, plus
TPC-H and the QuackLab measurements. Tests: results equality for TPC-H SF1 and the QuackLab SQL
against the previous commit; a streamed scan decodes each chunk once and leaves no payload registered
(a test hook on `DECODED`'s length); a low `decoded_cache_limit` still returns correct results and
keeps `CACHE_USED` under the limit after eviction; a pinned payload is not evicted; `SET`/`RESET`
of both limits and the derived default; dirty chunks are never evicted and never counted in
`CACHE_USED`; the 136a low-limit tests still pass. Measurements: chart load, Q1 peak, SF1 warm A/B —
and the default for `decoded_cache_limit` chosen from them.

### Step 136c-3 — save, checkpoint and repoint

`Sink::source`/`copy_from`, `FileSink`'s shared `Arc<File>`, `repoint_written`,
`repoint_all`, the `superseded` wait list and `sweep_superseded` in the checkpoint writer. **Gate:
full** (the writer and checkpoint safety change), plus the checkpoint fault tests. Tests:
`open_save_is_byte_identity` and `streamed_save_writes_the_in_memory_bytes` unchanged; `.save` peak
memory shows no carry buffer (a test hook asserting `carry.is_empty()` on the file path); after a
save, a re-encoded chunk's descriptor names the saved file and a re-read decodes the saved bytes;
incremental checkpoint with a snapshot holding an old descriptor: the snapshot keeps reading its
bytes after two further checkpoints, and the hole is reused only once the snapshot drops; a full
rewrite while a snapshot holds the old file keeps the snapshot correct; Windows rename-over-open is
covered by the existing `save` tests (no new platform-specific test); encrypted save still goes
through memory.

If steps must be merged, merge (3) into (2) only if the checkpoint wait list is deferred — do not
merge (1) with (2), because (1) is the byte-source swap and (2) is the behaviour flip.

---

## 9. Open questions for the reviewer and stream A

1. **First-read CRC** (§3): column-chunk granule on first page read. The alternative — per-page CRCs
   in a footer v8 — buys a cheaper point lookup at the cost of a format bump and a writer change.
   Recommendation: the column chunk; confirm that a corrupt file failing mid-query (instead of at
   open) is acceptable for the server, where a damaged store would now report the error on the
   statement rather than at boot.
2. **`decode_all` → `verify_all`** (§6): is any lake caller relying on `decode_all` leaving the table
   decoded and cached? If so it must ask for `StoreScan { cache: true }` explicitly.
3. **The cache default** (§7): `min(limit/4, 256 MB)`, floor 16 MB, until step (2) measures it. A
   small server (chart load) will need `SET decoded_cache_limit = '32MB'` or a config default;
   whose job is that — the server's config, or a smaller engine default?
4. **Stream A (checkpoints)**: the one-generation grace rule in §4, its in-memory-only wait list, and
   the ordering constraint (repoint before the checkpoint lock is released). Also: should the
   checkpoint *refuse* to allocate holes while any snapshot is older than one checkpoint, as a
   belt-and-braces policy — or is the `Weak` test enough?
5. **Stream A (save)**: `save_with_key` and encrypted stores keep the carry path (§5). Confirm that is
   acceptable until 136w's block-sealed container lands.
6. **Windows** (§4.1): we rely on `FILE_SHARE_DELETE` being set by `std::fs::File::open` and by
   `crate::fsutil::create_new_private`. Worth a one-line comment at the rename site, and a CI run on
   Windows would be the only way to prove it — is that in the gate?

---

## 10. Review decisions (2026-11-03)

- **Sequencing fix:** step (1) already reads clean chunks from the file, so §4's hazard is live as
  soon as (1) lands. The `superseded` wait list, `sweep_superseded` and the snapshot-across-two-
  checkpoints regression test move into **step (1)**. The test is written first, against the
  in-memory image, where it passes by construction. Step (3) keeps the save, carry and repoint work.
- §9.1: the column-chunk granule is accepted. A damaged chunk fails the reading statement, as in
  DuckDB, where a block checksum is checked at read.
- §9.3: the engine default stays `min(memory_limit/4, 256 MB)` until step (2) measures it. The server
  gets no separate config.
- **Performance risk in step (2):** uncached streaming scans re-decode on every warm query. If the
  TPC-H SF1 warm A/B exceeds 1.03, streaming scans admit into the decoded cache under its budget
  instead of decoding uncached. Measurement decides, and the result is recorded here.
- §9.4, §9.5: stream A to answer.

### §10 result: measured 2026-11-03, step 136c-2 (fix pass 1)

The uncached streaming variant (`Column::chunk_uncached`, `StoreScan { cache: false }`) was built
first: TPC-H SF1 warm A/B, 5 interleaved rounds, `bin-fix1` (136c-1) as A — sum 552 → 807 ms,
geomean **1.623**, every query 1.18–3.17× (`/tmp/w136c2-ab.json`). That is above 1.03, so the rule
above applies: a streaming scan admits into the decoded cache under its own budget and is the only
policy the engine uses; the uncached path stays available to the A/B.

Same harness, the admission variant (`bin-c2b`), one cache budget per run:

| `decoded_cache_limit` | geomean B/A | sum A → B (ms) | q1 | q6 | q9 | q18 | q21 |
|---|---|---|---|---|---|---|---|
| 256 MiB (old default) | 2.677 | 641 → 2133 | 3.82 | 12.57 | 3.28 | 1.00 | 0.98 |
| 512 MiB | 1.175 | 607 → 889 | 5.06 | 0.83 | 0.94 | 1.02 | 0.74 |
| 768 MiB | 0.989 | 738 → 746 | 1.05 | 0.83 | 1.01 | 1.03 | 1.00 |
| **1 GiB (default)** | **0.988** | 696 → 693 | 0.96 | 1.00 | 1.04 | 1.08 | 0.97 |
| unlimited | 0.996 | 648 → 640 | 1.02 | 1.00 | 1.04 | 1.03 | 1.02 |

(`/tmp/w136c2-ab-{256mb,512MB,768MB,1gb,1}.json`; CLI load at 1 GiB is 9.8/8.5/7.3.) Below ~640 MiB
the budget cannot hold one query's working set, so every morsel evicts the chunk another morsel just
decoded: the profile shows `register`/`evict_decoded` mutex contention instead of decode. The
default is therefore **a quarter of `memory_limit`, capped at 1 GiB, floor 16 MiB** — 1 GiB on the
default pool, passing with margin; the smallest measured passing budget is 768 MiB. `SET
decoded_cache_limit` overrides it (`-1` is unlimited); a smaller `memory_limit` yields a
proportionally smaller cache and gives the ratio back — the trade-off the setting exists for. Q1 peak
RSS on the shipped default: **456 MB** (four-column Q1, `/usr/bin/time -l`), against 973 MB at
136b-4 and 48–51 MB for the rejected uncached variant.

### §10 decode profile (step 136c-2, uncached)

`sample` of a Q1 loop with `decoded_cache_limit='1KB'` (a true uncached scan) plus single-column
scans over TPC-H SF1's 6M lineitem rows (min of 3, uncached, ms):

| column | ms | hot leaf |
|---|---|---|
| `l_orderkey` | 11.2 | bitpack (`unpack_bits`, 707 samples) |
| `l_partkey` | 16.1 | bitpack / FastLanes (`fastlanes_unpack`, 234) |
| `l_quantity` | 22.9 | bitpack |
| `l_extendedprice` | 27.8 | bitpack + `Vector::push_valid` |
| `l_discount` | 25.3 | bitpack |
| `l_tax` | 19.0 | bitpack |
| `l_shipdate` | 15.2 | delta / bitpack |
| `l_returnflag` | 12.3 | dictionary (`dict_rows`, 143) |
| `l_shipmode` | 20.7 | dictionary |
| `l_comment` | **89.9** | FSST expansion |
| `count(*)` | 0.1 | zone map, no decode |

Integer decode is bitpack/FastLanes at 2–5 ns per row; the one FSST-compressed varchar column costs
about a third of Q1. `pread` is ~5% of samples and the first-read CRC never rose above the sampling
threshold (one pass over the chunk, fused with the read). The cost above decode is per-value
materialization (`Vector::push_valid`/`push_bool`, `cast_value`, `decode_payload` building an
intermediate `Vec`), which is what DuckDB v2.0.0 avoids: its `ColumnSegment::Scan` decompresses
straight into the target `Vector`, bitpack/FLS write into the result buffer, and FSST expands eight
bytes at a time from a 2 KB symbol table into the vector's string heap
(`~/Desktop/projects/duckdb-v2.0.0/src/storage/compression/`, read only). A plausible follow-up:
decode into the vector's typed buffers instead of an intermediate `Vec` plus per-value pushes, and an
8-byte-symbol FSST fast path. No decoder changed in this pass.

**136c-2 result, reviewer's measurement** (2026-11-03; release CLI built with `-p pinkdb-cli --bin pinkdb`;
interleaved against 136c-1 on the same v7 SF1 store; load 9–12):
- Default limit (the cache takes the free room): warm geomean **1.001** over 7 rounds. A 5-round
  run under heavier load gave 1.048, and direct repeats showed the outliers were noise. Q1 peak
  RSS is 656 MB on both builds.
- `memory_limit = '256MB'`: geomean 0.999 against 136c-1, but the run sum is 2.3× the default's
  (1.71 s against 0.74 s), because scans decode again. Q1 peak RSS is 300 MB on both.
- The QuackLab 16×10 server is unchanged: 476–525 MB peak with 17.4 MB live. The peak is
  per-statement working memory the allocator keeps.
- Pi's earlier capped-budget ladder (256 MiB 2.68, 768 MiB 0.989) was measured on a stale CLI
  binary whose default was the 256 MiB cap. Its relative order holds, but the default row is
  replaced by the numbers above.
