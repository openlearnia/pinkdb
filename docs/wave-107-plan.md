# Wave 107a — the fixed-size `ARRAY` type

ROADMAP row 107 is *vector storage*: an engine type for embeddings, and two
extensions on top. It is split three ways:

- **107a (this document):** the type — `FLOAT[N]` / `DOUBLE[N]` /
  `INTEGER[N]` / `BIGINT[N]` as a columnar type, literals, casts, element
  access, `len`, persistence, and the wire/FFI/ABI encodings.
- **107b (queued):** the `vss` extension: `array_cosine_distance` and
  friends, plus an HNSW index the optimizer uses for
  `ORDER BY distance LIMIT k`. It consumes `ExtType::ArrayDouble` (ABI minor
  5, added here).
- **107c (queued):** the `embeddings` extension: `embed(text[, model])`
  against an OpenAI-compatible endpoint.
- **107a2 (queued, split off here):** the general variable-length `LIST`
  (`T[]`) and `unnest`. See "Scope" below.

Everything marked *measured* comes from `/tmp/duckdb-2.0.0/duckdb`
(2.0.0 "Cyanoptera", commit `d54cbbe3df`) run while writing this wave.

## Reference behaviour (DuckDB 2.0.0, measured)

```
typeof([1,2,3]::INTEGER[3])                     -> INTEGER[3]
typeof([1.0,2.0]::DOUBLE[2])                    -> DOUBLE[2]      -- FLOAT[N] is f32 in DuckDB
typeof([1,2,3])                                 -> INTEGER[]      -- a bare literal is a LIST
array_value(1.0,2.0,3.0)                        -> DECIMAL(2,1)[3] -- element type from the args
CAST([1.0,2.0,3.0]::FLOAT[3] AS VARCHAR)        -> [1.0, 2.0, 3.0]
([1.0,2.0,3.0]::FLOAT[3])[1]                    -> 1.0             -- 1-based
([1.0,2.0,3.0]::FLOAT[3])[0] / [4] / [-1] / [NULL] -> NULL         -- out of range is NULL, not an error
len(x) / array_length(x)                        -> 3
[1.0,NULL,3.0]::FLOAT[3] = [1.0,NULL,3.0]::FLOAT[3] -> true         -- NULL elements are structural
[1.0,NULL,3.0]::FLOAT[3] = [1.0,2.0,3.0]::FLOAT[3]  -> false
[1.0,2.0,3.0]::FLOAT[3] = [1.0,2.0,3.0]::DOUBLE[3]  -> true
CAST('[1,2,3]' AS FLOAT[3])                     -> [1.0, 2.0, 3.0]
CAST('[1,2]'   AS FLOAT[3])                     -> error "…the size of the array must match the destination type"
CAST([1,2,3]::INTEGER[3] AS DOUBLE[4])          -> error "Cannot cast array of size 3 to array of size 4"
CAST([1,2,3]::INTEGER[3] AS JSON)               -> [1,2,3]          (no spaces)
CAST([1,2,3]::INTEGER[3] AS VARIANT)            -> [1, 2, 3]        (variant display)
INSERT INTO t(v FLOAT[3]) VALUES ([1.0,2.0,3.0]) -> implicit INTEGER[3]/DECIMAL[3] -> FLOAT[3]
```

Ordering and grouping, on `t(v FLOAT[3])` holding `[1,NULL,3]` twice,
`[1,2,3]`, `NULL` and `[0.5,2,3]`:

```
GROUP BY v   -> [0.5,2,3]|1  [1,2,3]|1  [1,NULL,3]|2  NULL|1
count(DISTINCT v) -> 3                    -- NULL arrays skipped, like any NULL
ORDER BY v   -> [0.5,2,3]  [1,2,3]  [1,NULL,3]  [1,NULL,3]  NULL
min / max    -> [0.5,2,3] / [1,NULL,3]    -- a NULL element sorts after any value
```

So the element order is `-inf … +inf < NaN < NULL`, elements are compared
left to right, and a fixed-size array is never shorter than another of its
own type. Two arrays group together exactly when their elements compare
equal, so a NULL element is equal to a NULL element at the same position.

DuckDB in Parquet writes an `ARRAY` column exactly like a `LIST` — a
three-level `group`, `list`, `element` — and **loses the length**; reading
it back gives `v FLOAT[]`. It does write element-level row-group statistics
(`path_in_schema "v, list, element"`). In DuckLake, DuckDB **refuses** an
`ARRAY` column ("Failed to convert DuckDB type to DuckLake - unsupported
type FLOAT[3]"); a `LIST` becomes two `ducklake_column` rows, a `list`
parent and a child literally named `element` with `parent_column` set, and
the statistics land on the child.

## Scope

`ARRAY` only. It is what vector storage needs, and it is the only shape the
rest of the engine can type statically (a fixed length is part of
`DataType`, so a column's layout is known before a byte is read).

Measured reason for splitting `LIST` off: a `LIST` is only expressible as
`DataType::List(Box<DataType>)`, which breaks `Copy` on `DataType` — the
single most load-bearing property in the engine (`VectorData::dtype`,
`BoundExpr::dtype`, every copied schema) — and a variable-length cell
reintroduces per-row offsets in the vector, the `.pink` page, the wire
format and the arrow-free Parquet reader. That is structural work well past
"a third more", so it is **107a2**, together with `unnest` (a table
function that would need its own operator plumbing).

Not done in 107a, tracked:

- **107a2:** `LIST`/`T[]`, `unnest`, `list_*` functions, `ARRAY` ↔ `LIST`
  casts.
- `FLOAT` *scalar*: pinkdb still has none; `FLOAT` is 32-bit only inside an
  array (see "Element kinds").
- element-level Parquet statistics for an `ARRAY` column (we write none,
  like `VARIANT`).
- `array_slice`, `array_to_string`, `list_sort`, … — `array_extract`,
  `array_length`/`len`, `array_value` and `typeof` are the 107a set.

## Value model

```rust
pub enum ArrayElem { Integer, Bigint, Float, Double }  // Copy, Hash
DataType::Array { elem: ArrayElem, len: u32 }        // stays Copy
Value::Array(Arc<ArrayValue>)                        // one allocation per cell

pub struct ArrayValue {
    pub elem: ArrayElem,
    pub len: u32,
    /// `rows * len` elements of `elem`, one flat buffer.
    pub values: ArrayElems,  // Ints(Vec<i64>) | Floats32(Vec<f32>) | Floats(Vec<f64>)
    /// Bit `row * len + i` set ⇒ element `i` of `row` is not NULL.
    /// `None` when the whole column has no NULL elements.
    pub valid: Option<Box<[u8]>>,
}
```

### Element kinds

`INTEGER[N]`, `BIGINT[N]`, `FLOAT[N]` and `DOUBLE[N]`. `FLOAT` is 32-bit
(`Float`) — the embedding width: providers return f32, DuckDB's vss works on
`FLOAT[N]`, and a 1M × 768 table costs 3.1 GB instead of 6.1 GB. pinkdb has
no `FLOAT` *scalar*, so the kind exists only inside an array: `typeof` says
`FLOAT[N]`, the stored bytes are 4 per element, and `a[i]` widens to DOUBLE
(the one deliberate difference from DuckDB, which returns a `FLOAT`).
`REAL[N]` is the same kind. The casts follow DuckDB 2.0 (measured): a finite
`DOUBLE` that rounds to infinity errors ("out of range for FLOAT"), underflow
is silent, `inf`/`nan` pass through, and the VARCHAR path parses straight to
f32, so `'1e40'` becomes `inf`.

`DataType` stays `Copy`: the element is a small enum, not a `Box`. The
columnar form is one flat child buffer of `rows × N` elements plus the
row validity bitmap the vector already has, and one extra element bitmap
that is absent (`None`) for the common no-NULL-element case — so a 768-d
`FLOAT` column at 100k rows is 307 MB of contiguous `f32` (614 MB as
`DOUBLE`) and no per-row heap allocation at all. `Vec<Arc<ArrayValue>>` per vector row is only the
`Vector::get` boundary (scalar `Value`), not the storage.

NULL elements are allowed, like DuckDB. They cost one bit per element in a
bitmap that is only allocated when some element is NULL; `CAST(x AS
T[N])` never produces them, only a literal or text/Parquet input can.

## `.pink` encoding

- **dtype tag 10:** `u8 elem` (0 = INTEGER, 1 = BIGINT, 2 = DOUBLE, 3 =
  FLOAT — appended, never renumbered, so an old reader sees an unknown kind),
  `u32 len`. Written and read next to the DECIMAL payload, same place as
  every other dtype.
- **default-value tag 10 (`VD_ARRAY`):** the column default is literal text
  (`[1.0, 2.0]`), so the writer emits `u32 len | u32 bytes` and the reader
  parses it with the same text parser as `CAST(VARCHAR AS …)`.
- **page encoding tag 11 (`ENC_ARRAY`):** one frame,
  `u32 rows | u8 layout | u8 elem tag | u32 bitmap bytes | element bitmap |
  values`, where `layout` 0 is row-major (`row * len + i`) and 1 is
  byte-planed (byte `j` of every element together, over the element's own
  width: 4-byte lanes for `FLOAT`, 8 for `DOUBLE`). The writer emits
  whichever of the two is smaller and then wraps that in the existing
  `LZ_FRAME` when compression wins, so one tag covers four combinations
  (row/planed × raw/LZ) and the decoder picks them apart from the layout
  byte. The element bitmap is the vector's own: `u32` length 0 when no row
  has a NULL element. NULL *rows* are written as zero elements, which keeps
  the payload deterministic and lets the reader skip them by the frame's
  row bitmap.
- **statistic tags:** unchanged; an `ARRAY` writes `SV_NONE`.
- **statistics:** `SV_NONE`, like `VARIANT` — an array has no meaningful
  min/max of its own, and DuckDB's are element-level anyway. The lake's
  `StatsColumn` writes no bounds for it.
- **catalog image (96b):** no change — the image carries the schema through
  the PINKDB06 footer, so the new dtype tag is the only addition.
- **WAL:** the WAL stores SQL text, so an array parameter is inlined as
  `CAST('[1.0, 2.0]' AS DOUBLE[2])` by `value_literal`; replay re-parses it.

## Semantics

- **Equality / ordering:** element-wise left to right, using the existing
  total order for each element kind (the DOUBLE order already folds `-0.0`
  and all NaNs). A NULL element compares greater than every value (so
  `[1,NULL] > [1,2]`, matching DuckDB's ORDER BY and MIN/MAX) and equal to
  another NULL element, which is what makes `[1,NULL]=[1,NULL]` true.
  Arrays of different `len` never compare (the binder rejects the
  comparison); `compare_values` still defines an order (shorter first) for
  the `Value`-level fallback.
- **Hashing / GROUP BY / DISTINCT / joins:** `Vector::encode_into` tag 11 =
  `elem byte | len | element-validity flag+bits | normalized values`. Equal
  arrays encode byte-identically, including NULL elements.
- **Sort keys:** an `ARRAY` column bails out of `encode_sort_keys` (like
  `VARIANT`), so the generic `compare_values` comparator runs.
- **NULL array vs NULL element:** a NULL array is SQL NULL (skipped by
  `count(x)`, grouped together, last in `ORDER BY`); NULL elements are
  inside the value.
- **Casts:** `ARRAY→ARRAY` when `len` matches and `elem→elem` is a numeric
  cast (a mismatch is "cannot CAST DOUBLE[3] AS DOUBLE[4]: array length
  mismatch"); `VARCHAR↔ARRAY` via the `[a, b]` text form (exact length
  required); `ARRAY↔VARIANT` (an `Array` kind in the variant model) and
  `ARRAY↔JSON` through `VARIANT`; `ARRAY→BOOLEAN`/date/timestamp/interval
  are refused.
- **Element access** `a[i]` and `array_extract(a, i)`: 1-based, out of range
  or a NULL index yields NULL (DuckDB's ARRAY behaviour).
- `len(a)` / `array_length(a)` return the static `N` as `INTEGER`.

## Encodings elsewhere

- **Wire protocol (v13):** value tag 11 = `u32 byte length | the `.pink`
  value encoding` (element kind, length, element bitmap, elements), so one
  decoder serves the wire, the FFI and the WAL; dtype tag 10 =
  `u8 elem | u32 len` with elem 0/1/2/3 = INTEGER/BIGINT/DOUBLE/FLOAT. `PROTOCOL_VERSION` bumps to 13 with a
  ledger entry; the server's `row_encoded_len` gains the same shape.
- **FFI:** `PINKDB_TYPE_ARRAY = 11` (`N * 8` bytes of little-endian `f64`)
  and `PINKDB_TYPE_ARRAY_FLOAT = 12` (`N * 4` bytes of `f32`), each with the
  element count packed into the type tag (`tag | len << 8`) because the tag
  is what carries a *declaration* too. The cell's `text_ptr`/`text_len` point
  at the raw elements and `int64` holds `len`, so a 768-d vector crosses as
  one pointer and no per-element boxing. `PINKDB_ABI_VERSION` stays 1
  (additive tags). Only `FLOAT[N]`/`DOUBLE[N]` cross: an
  `INTEGER`/`BIGINT` array, and an array holding a `NULL` element, are
  refused with a message that says so (there is no per-element NULL channel
  in the cell layout), and a `CAST` to `DOUBLE[N]`/`VARCHAR` makes either
  crossable.
- **Bindings:** Node `Float32Array`/`Float64Array`, Python `list[float]`,
  Go `[]float32`/`[]float64`, keyed off the tag byte — the same
  `FLOAT`/`DOUBLE[N]`-only rule as the ABI, inherited from it.
- **Extension ABI, minor 5:** `ExtType::ArrayFloat` (tag 12) and
  `ExtType::ArrayDouble` (tag 11) — argument and return are a borrowed
  `(const T *ptr, size_t len)` view with the element count in `int64` and
  `text_len` = `len * sizeof(T)`; no per-element boxing and no copy in the
  argument direction. `Float` is the one 107b/107c need (an f32 embedding
  costs half the memory a distance kernel reads); `Integer`/`Bigint` arrays
  are not in the ABI (a registered function asking for one is rejected with
  a clear error). An array that contains a NULL element is rejected at the
  call boundary, since a pointer+length view cannot express it. The five
  header copies and the Rust SDK (`ArrayFloat<N>`/`ArrayDouble<N>`) carry
  the new types; the C++/Go SDK wrappers only need a `set_array` convenience
  (noted, not written here) because they are thin sugar over the same
  struct.

## Lake and Parquet

- **`.pink` lake data file:** nothing new — the file is the same
  `persist.rs` format, so a new dtype tag round-trips it. Statistics carry
  no bounds, like `VARIANT`.
- **Parquet write:** the same three-level `LIST` group DuckDB writes
  (`v` → `list` → `element`), because that is what a reader expects; the
  length is not in the file (parquet-rs 59.3 has no `Array` logical type),
  so it is written as a `pinkdb.array.<column>` key-value entry and is
  otherwise recoverable from the row count. Exactly `N` level entries per
  row are written, `0` = NULL array, `2` = NULL element, `3` = value, which
  is what makes `num_values / num_rows` exact even for NULL rows.
- **Parquet read:** a three-level group whose element leaf is a numeric
  type becomes `DataType::Array` with the length taken from the key-value
  entry when present, else inferred from `num_values / num_rows` of every
  row group (all must agree). A row whose entry count differs from `N` is a
  clear "variable-length LIST is 107a2" error, never a silent truncation.
- **DuckLake:** write the two rows DuckDB writes for a nested type — parent
  `column_type='list'` with `parent_column` NULL, child `element` with
  `parent_column` = the parent's id and the next `column_id`/`column_order`
  — and keep the real type (`DOUBLE[3]`) in the `pinkdb_column` side row,
  which is ours anyway; every catalog reader prefers that row over the
  coarse DuckLake name. A catalog DuckDB wrote has no side row and only a
  length-less `list`, so reading it is **refused** with a message naming the
  column and 107a2 rather than decoding it as a `VARCHAR`.
- **Field ids:** DuckDB's lake data files carry
  `field_id = ducklake_column.column_id` on every top-level column and on an
  ARRAY's `element` leaf, and its DuckLake reader finds nested children by
  that id (a parquet list has no name of its own). A pinkdb data file now
  carries the same ids, which is what makes DuckDB read exact values through
  our lake. The ids cannot be read back after the fact — data files are
  written before the commit that creates their columns — so
  `metadata::store::planned_field_ids` predicts them with exactly the rule
  `sync_columns` applies: a live column keeps its id (ids never move, which
  is what makes them safe across `DROP`/`ADD COLUMN`), a new column takes
  `MAX(column_id) + 1` in column order, and an ARRAY takes one more for its
  child. Standalone `COPY … TO`/`write_parquet` files carry none, as DuckDB
  writes none there; pinkdb's own lake reader ignores them.
- One bug this exposed: `sync_columns`'s live-column query included the
  `element` child rows, so a second save ended every existing ARRAY child
  (its name is not in the target). It now filters `parent_column IS NULL`.
- **Column statistics:** none for an `ARRAY` (`SV_NONE`, no
  `ducklake_file_column_stats` bounds), matching `VARIANT`.

## Differences from DuckDB, recorded

1. `a[i]` of a `FLOAT[N]` widens to `DOUBLE` (an exact conversion): pinkdb
   has no `FLOAT` scalar, while DuckDB returns a `FLOAT` element. `typeof`
   and the stored bytes are `FLOAT[N]` in both.
2. A bare list literal is an `ARRAY`, not a `LIST`: `[1,2,3]` is
   `INTEGER[3]`, and `array_value(1.0,2.0)` is `DOUBLE[2]` rather than
   `DECIMAL(2,1)[2]` (pinkdb's literal inference is the standard numeric
   tower, not DECIMAL).
3. `array_value(…)` and `[a, b]` build the array per row from any
   expressions (a `ColumnarValue` constructor), so they are not restricted to
   constants like a folded literal would be.
4. `CAST(v AS ARRAY)` from `VARIANT`/JSON requires a JSON array of the exact
   length (DuckDB also allows a `LIST` of the right length).
5. Out-of-range element access yields NULL in both; the element-NULL ordering
   above was matched to DuckDB's ORDER BY and MIN/MAX rather than to its
   three-valued `=` (which it does not use for arrays).

## Measured (release build, this change)

Vectors are the workload, so the sizes below are the point of the design.

| measurement | value |
| --- | --- |
| 100k × 768 `DOUBLE`, insert | 307 ms |
| 100k × 768 `DOUBLE`, insert + full scan | 308 ms |
| 100k × 768 `DOUBLE`, `.pink` store | 585 MiB = 6144 bytes/row = 8 bytes/element |
| the same data with `sin(base + k)` values | 12.0 MiB → 0.1 MiB (108×, byte-plane + LZ) |
| a constant column | 253× |
| pseudo-random (incompressible) values | 1.0× (12 583 170 bytes for 12 582 912) |

So an embedding column costs exactly its raw bytes; the encoder never
inflates it and never silently drops precision (the 1.0× row is the check).

`FLOAT[768]` is the embedding type (providers return f32, and DuckDB's vss
works on `FLOAT[N]`), and it is a real 4-byte element kind:

| measurement | value |
| --- | --- |
| 100k × 768 `FLOAT`, `.pink` store | 292 MiB = 4.00 bytes/element |
| 100k × 768 `FLOAT`, insert (debug) | 10.5 s |

The page codec byte-planes over 4-byte lanes, so an incompressible
`FLOAT[768]` column stores at exactly 4 bytes per element too.

Interleaved A/B on TPC-H Q1/Q6 over a 10M-row `lineitem` store (50 MB,
`bench/wave95_perf/{q1,q6}.sql`), alternating the order every round on a share
of the machine that was drifting about 2× during the run (13 rounds each):

| query | best base | best new | paired median (new − base) |
| --- | --- | --- | --- |
| Q1 | 0.121 s | 0.121 s | −8 ms |
| Q6 | 0.063 s | 0.062 s | −2 ms |

No query that does not touch an `ARRAY` changes path: the new dtype has its
own tag in every match, and the only shared code it touches is
`encode_chunk_typed`'s dispatch (one extra arm) and `encode_sort_keys`'s bail
(one extra `matches!` alternative).

Binary size, `--release`, before (main `71ba9de`, wave 106) and after:

| binary | before | after | delta |
| --- | --- | --- | --- |
| `pinkdb` | 5 073 744 | 5 111 056 | +37 312 (+0.74%) |
| `pinkdb-server` | 4 686 112 | 4 743 824 | +57 712 (+1.23%) |
