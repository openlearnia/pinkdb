# LIST and STRUCT — scoping plan (not scheduled)

Written 2026-11-01 for wave 139 (QuackLab track, stream A), which was asked to
add LIST/STRUCT "or scope them first". This is the scoping; no code.

## Is it on QuackLab's critical path? No.

- QuackLab's SQL generators and runtime (`grain/go/internal/{compiler,
  physical,deploy,httpapi}`, `grain/vps`) emit no `list()`, `array_agg`,
  `unnest`, `struct_pack`, `[…]` list literals, `STRUCT(…)` or `T[]` types
  (searched 2026-11-01). The compatibility audit's `list` entry has no call
  site in the code.
- QuackLab moved *away* from lists: its JS procedures take JSON `VARCHAR`
  instead of `VARCHAR[]` (`go/internal/deploy/procedure_ddl.go`: DuckDB's
  `CALL` with a `VARCHAR[]` argument hit a `ListBoundCast` internal error).
- The 114 golden statements use none.

Where lists do matter for the track:
- **The quack protocol** (audit "second seam", only if QuackLab readers
  attach `quack:`): DuckDB clients send `duckdb_*()` queries whose results
  carry `tags MAP(VARCHAR, VARCHAR)` and `SHOW ALL TABLES` lists; wave 131
  left `tags`/`options` out and prints the lists as VARCHAR.
- **DuckLake interop**: DuckDB-written lakes can hold LIST/STRUCT columns;
  pink-lake must at least refuse them clearly (stream B, wave 132).
- **General DuckDB parity** (`list()`, `unnest`, `string_split`, JSON →
  LIST), which users of pinkdb outside QuackLab will expect.

## What exists

- `ARRAY` (fixed size, numeric elements, wave 107a): flat element buffers,
  a row bitmap, `.pink` tag 10, Parquet three-level LIST both ways, wire v13,
  FFI/bindings. Row 107a2 already queues "variable-length LIST, `unnest`,
  slicing".
- `VARIANT` (semi-structured, wave 84+) covers JSON-shaped data and is what
  pinkdb uses where DuckDB would use STRUCT/MAP for dynamic data.

## The design, when it is scheduled (port DuckDB, credit it)

DuckDB's layout (`src/common/types/vector.cpp`, `list_vector.cpp`,
`struct_vector.cpp`): a LIST vector is `(offset, length)` entries over one
child vector; a STRUCT vector is one child vector per field plus the
parent's validity. Both nest. pinkdb's `VectorData` gains `List { entries:
Vec<(u32 offset, u32 len)>, child: Box<Vector> }` and `Struct { fields:
Vec<(String, Vector)> }`; `DataType` gains recursive `List(Box<DataType>)`
and `Struct(Vec<(String, DataType)>)` — which makes `DataType` non-`Copy`,
the single largest cost (it is `Copy` and passed by value across the
engine; measure the churn first).

## Briefs (each its own pi task, in order)

| Brief | Scope |
|---|---|
| N1 type | `DataType` recursion (and the `Copy` removal), `VectorData::{List,Struct}`, `Value::{List,Struct}`, casts to/from VARCHAR in DuckDB's text form, equality/ordering/hashing (DuckDB's rules), `typeof`. |
| N2 SQL | `[a, b]` literals (today ARRAY), `{'k': v}` struct literals, `T[]` / `STRUCT(k T, …)` column types, `list_value`, `struct_pack`, subscripts `l[i]`/`l[a:b]`/`s.k`/`s['k']`, `len`, `list_contains`, `list_extract`, `string_split`, `unnest` (table function and in the SELECT list), `list()`/`array_agg` aggregates, `list_distinct`/`list_sort`. |
| N3 storage | `.pink` encodings (child column chunks after the parent's offsets), WAL value encoding, wire protocol, FFI/bindings, the console grid. |
| N4 interop | Parquet LIST/STRUCT both ways (extend 107a's LIST path), DuckLake catalog `list`/`struct` columns, `duckdb_*()` `tags` MAP — or MAP as LIST(STRUCT(key, value)) as DuckDB defines it. |

Gate per brief: duckdiff probes against DuckDB for every function and
literal, the TPC-H checksums (no regression from the `DataType` change), a
before/after timing of SF 1, and the full gate for N1 and N3.

## Recommendation

Do not put this on the QuackLab track. Schedule it as its own wave after the
track (or when the quack protocol or a DuckLake interop need forces N4),
starting with a one-day measurement of the `DataType: !Copy` churn.
