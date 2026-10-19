# pinkdb-arrow — core **arrow** extension (wave 71)

Arrow IPC (**Feather v2 file format**, the `ARROW1` magic + footer) file
read/write for pinkdb, over the arrow-rs crates (`arrow-array`,
`arrow-buffer`, `arrow-schema`, `arrow-ipc` — the 59.x line the workspace lock
already carries for vortex).

## SQL surface

```sql
-- Replacement scan: the format is chosen by the file extension
-- (.arrow / .ipc / .feather / .arrows).
SELECT * FROM 'data.arrow';
SELECT f.a FROM 'data.arrow' AS f WHERE f.a > 10;

-- Table function spelling (aliases: read_ipc, read_feather).
SELECT COUNT(*) FROM read_arrow('data.arrow');

-- Write one table (uncompressed; FORMAT may be omitted and inferred from
-- the target's extension).
COPY t TO 'out.arrow' (FORMAT arrow);
```

The result of `COPY` is DuckDB's `Count` shape (one row, one `BIGINT`
column).

## Integration model

The scalar extension ABI cannot express a replacement scan, a table function
or a COPY format, so this crate uses the engine's second, purpose-built
extension surface: [`pinkdb::filefmt::FileFormat`]. The engine owns the trait
and the per-store registry, this crate implements it, and the **host**
registers it:

```rust
// A host that opens durable stores must register process-wide *before*
// creating them (recovery replays inside `open_durable`):
pinkdb_arrow::ensure_registered();

// An embedder may instead register per store:
let mut db = pinkdb::PinkDb::new();
pinkdb_arrow::register(&mut db);
```

The CLI wires exactly that behind the opt-in `arrow` cargo feature
(`cargo build -p pinkdb-cli --features arrow`); the default binary stays lean
and refuses every file surface with a clear "no file format is registered"
error. This is the same *engine-adjacent crate* shape as `pink-lake`
(which implements `AttachedLake`).

## Semantics

| | |
|---|---|
| Format | Arrow IPC **file** format. The IPC *stream* format is detected by its magic and refused with a message naming the difference. |
| Compression | Written uncompressed (pyarrow's and DuckDB's default); reading accepts uncompressed, LZ4- and ZSTD-compressed files. |
| Layout | Our writer emits one record batch per engine chunk (≤ 2048 rows); a reader accepts any batch sizes and re-chunks to the engine's chunk size. |
| Scans | Whole-file materialization: no projection/predicate pushdown, no streaming, one decode per scan. |
| Durability | A WAL-logged statement that reads a file replays by re-reading it (DuckDB's rule). `COPY … TO` publishes no store change and is never logged. |
| Types | See `src/types.rs` for the full mapping; anything without an **exact** engine counterpart is refused at bind time, naming the column and type. |

Not in this wave: BLOB/TIME/LIST/STRUCT/MAP/UNION/DICTIONARY/`Decimal256`
(clean refusal), `UInt64` (signed-engine refusal), IPC stream files,
projection pushdown, streaming reads, multi-file/glob sources,
`COPY (SELECT …) TO`, and the C Data Interface (below).

## Follow-up pin: the Arrow **C Data Interface** in the FFI

The wave-71 row also names Arrow's C Data Interface for `pinkdb-ffi`
(query-result export + Arrow-array scan input, zero-copy where the layout
allows). It is deliberately **not half-implemented** here; this is the precise
pin for the follow-up wave:

1. **Structures**: add `ArrowSchema` and `ArrowArray` (`#[repr(C)]`, exactly
   the Arrow columnar-format layout: `format`, `name`, `metadata`, `flags`,
   `n_children`, `children`, `dictionary`, `release`, `private_data` for the
   schema; `length`, `null_count`, `offset`, `n_buffers`, `n_children`,
   `buffers`, `children`, `dictionary`, `release`, `private_data` for the
   array) plus `ArrowArrayStream`, in `crates/pinkdb-ffi/src/arrow.rs`
   (a new module; the crate already owns the audited `unsafe` carve-out).
2. **Export**: `pinkdb_arrow_export_result(handle, statement, ArrowArrayStream* out)`
   producing per-chunk batches from a `QueryResult::Selected` — one
   `ArrowArray` set per `DataChunk`, with an owned `release` callback that
   drops the boxed chunk. Zero-copy per column where the engine's layout is
   Arrow-compatible (`INTEGER`/`BIGINT`/`DOUBLE`/`DATE`/`TIMESTAMP` are
   contiguous engine buffers — reuse them via
   `arrow_buffer::Buffer::from_owner`); `VARCHAR` and `DECIMAL`/`INTERVAL`
   copy into owned buffers in v1 (offsets/`i128`/month-day-nano) with the
   copy documented in the header. `BOOLEAN` needs bit-unpacking.
3. **Import**: `pinkdb_scan_arrow(handle, const ArrowArrayStream* stream, const char* table)`
   materializing a registered stream as an engine table (the mirror of
   `pinkdb_execute`), validating the Arrow format strings against the same
   `engine_type` mapping this crate uses — factor that mapping into a small
   shared crate (or re-export it from `pinkdb`) so the C path cannot drift.
4. **Hosts**: the tests must cover pyarrow (`pyarrow.array(..., from_arrow)`)
   and Polars through Python, and an ADBC-style consumer, using a C test like
   `crates/pinkdb-ffi/tests/ffi.rs`; `pinkdb.h` gains the declarations and the
   header-sync test (`tests/header_sync.rs`) is extended.
5. **wasm**: not applicable to the FFI crate; the *file* half (this crate)
   stays wasm-compatible (no OS-specific APIs beyond `std::fs`).

## Tests

- `tests/roundtrip.rs` — our writer → our reader round trips for every
  engine type (NULLs, multiple chunks, empty tables, NaN/±inf, unicode,
  batch boundaries), pyarrow golden fixtures (`tests/fixtures/`, regenerated
  by `tests/fixtures/generate_fixtures.py`), compressed files, stream-file
  refusal, unsupported-type refusal, error cases.
- `tests/engine.rs` — the SQL surfaces end to end plus the durable-replay
  rule, against the real engine.
- `crates/pinkdb/src/filefmt.rs` unit tests — the same surfaces driven by a
  fake in-engine format (no arrow dependency), including the durability
  error paths.
- `crates/pinkdb-cli/tests/arrow.rs` (feature on) and `arrow_disabled.rs`
  (feature off) — the CLI's registration step and refusal message.
- `bench/duckdiff.py` — value cross-checks against DuckDB for the
  round-trip and the foreign pyarrow fixture (DuckDB α ships no arrow
  support at all, so the comparison is value-level; see the battery's
  comments).
