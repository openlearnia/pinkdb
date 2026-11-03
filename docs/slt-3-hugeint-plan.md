# slt-3 — HUGEINT and UHUGEINT: design

Design spike only. Nothing here is implemented; the deliverables are the
decisions below and the step split in §8.

**Recommendation.** Add `DataType::Hugeint` / `Value::Hugeint(i128)` +
`VectorData::Int128` as a first-class type, then `UHUGEINT` as a second,
mechanical pass over the same arms. The expensive-looking part — `SUM` — is
free: pinkdb's existing `SumInt` accumulator already carries the exact 128-bit
result and only throws it away in `finalize_ref`. No accumulator changes, no
per-row cost, and TPC-H is unaffected. `UHUGEINT` is far cheaper than DuckDB's
matrix suggests because outside `UHUGEINT` every arithmetic promotion is
`DOUBLE`.

## 1. Measured corpus impact

Source: `bench/sqllogictest/baseline.json` (4,233 files; 444 pass, 3,468 fail,
2 timeout, 319 skip) and `~/Desktop/projects/ducklake-vortex/ducklake/duckdb/test/sql`.

- **42** failing files name `HUGEINT`/`UHUGEINT` in their first failure:
  - **38** stop at `CREATE TABLE … (col HUGEINT|UHUGEINT)` — `parse error:
    … expected … known column type`;
  - **4** stop at `(col HUGEINT[])` — a *variable-length LIST*, which is a
    different missing feature (out of scope; see §9).
- By area: `types/hugeint` 10, `types/uhugeint` 10, `copy/parquet/writer` 3,
  `types/numeric` 3, `function/list` 2, `storage/types` 2, `storage/wal` 2,
  `transactions/types` 2, `types/bignum` 2, and singletons in
  `aggregate/aggregates`, `json/issues`, `show_select`, `table_function`,
  `topn`, `types/uuid`.
- **Blockers besides the type**, from a keyword scan of those 42 files: 22 need
  nothing but the type and core SQL; 4 also need `TRY_CAST`; 4 need
  `FIRST`/`LAST` (pinkdb's `AggFunc` has no such variant —
  `crates/pinkdb/src/sql/binder.rs:36`); 2 need LIST; 2 need `BIGNUM`; 2 need
  `<<`/`>>` (`BinaryOp` has no shift — `crates/pinkdb/src/sql/ast.rs:1205`);
  2 need `BIGNUM` + `TRY_CAST`; 1 each need `top_n`, `SUMMARIZE`,
  `struct_pack`, `<<` + `BIGNUM`, UUID + `TRY_CAST`.
  **Expected: ~22 files (up to 26 with `TRY_CAST`) — a 10 % file-pass gain.**
- **Exactly one** file's first failure is the `SUM` *value*:
  `aggregate/aggregates/test_sum.test` — `Overflow in SUM: 4611686018427388403500
  is out of range for BIGINT`. No file fails today on a `SUM` type mismatch,
  because sqllogictest compares values, not types.
- QuackLab's chart corpus (`bench/quacklab/corpus/REPORT.md`, `ROADMAP.md:204`):
  121 cases, "59 identical + 61 differing only in the HUGEINT type, 0 pinkdb
  errors"; the difference is the type row (`int128` vs `bigint`) on
  `SUM(INTEGER)`.

The brief's "~102 files" is not reproducible from the baseline; 38 is the
measured count of first failures on the type, 42 counting LIST-shaped uses.

## 2. DuckDB's behaviour

Measured on `~/.local/bin/duckdb` (v2.0.0-alpha46057). Where the installed
binary and the corpus disagree, both are recorded.

### 2.1 Integer literals

DuckDB's magnitude cascade, measured with `typeof()`: fits `i32` → `INTEGER`;
fits `i64` → `BIGINT`; fits `i128` → `HUGEINT` (`9223372036854775808`,
`10000000000000000000`, `-9223372036854775809`); fits `u128` →
`UHUGEINT` (`170141183460469231731687303715884105728`); beyond → `BIGNUM`.

pinkdb: `crates/pinkdb/src/sql/peg_parser.rs:1363 fn parse_int(text) ->
Option<i64>` and `:1453 fn int_value` (INTEGER if it fits `i32`, else BIGINT;
out of `i64` is `"integer literal out of range"`). One function to widen.

### 2.2 Aggregates

| aggregate | result type |
|---|---|
| `SUM(TINYINT/SMALLINT/INTEGER/BIGINT/HUGEINT)` | `HUGEINT` |
| `SUM(UTINYINT/USMALLINT/UINTEGER/UBIGINT)` | `HUGEINT` |
| `SUM(UHUGEINT)` | **`DOUBLE`** (`…211455` → `3.402823669209385e+38`) |
| `AVG(any integer)` | `DOUBLE` |
| `MIN/MAX(HUGEINT)` / `(UHUGEINT)` | `HUGEINT` / `UHUGEINT` |
| `SUM(DISTINCT h)`, `SUM(h) FILTER (…)`, `SUM(h) OVER ()` | `HUGEINT` |

`SUM` over a `HUGEINT` *column* raises a different message from scalar
arithmetic: `Out of Range Error: Overflow in HUGEINT addition:
170141183460469231731687303715884105727 + 1`.

### 2.3 Promotion (`common_numeric` targets)

| left | right | result |
|---|---|---|
| `HUGEINT` | `TINYINT`…`UBIGINT` (any narrower) | `HUGEINT` |
| `UHUGEINT` | `UTINYINT`…`UBIGINT` (unsigned) | `UHUGEINT` |
| `UHUGEINT` | `TINYINT`/`INTEGER`/`BIGINT`/`HUGEINT` | **`DOUBLE`** |
| `HUGEINT`/`UHUGEINT` | `DECIMAL(p,s)` | `DECIMAL(38,s)` |
| `HUGEINT` | `DOUBLE` | `DOUBLE` |
| `HUGEINT` | `HUGEINT` (÷) | `DOUBLE` |
| `HUGEINT` | `HUGEINT` (`%`) | `HUGEINT` |

So `UHUGEINT` needs no 128-bit arithmetic at all except `%` and same-type
`+`/`-`/`*`: everything mixed with a signed type is `DOUBLE` (DuckDB's
`MaxLogicalType` cannot keep an unsigned 128-bit value exact next to a signed
one).

### 2.4 Arithmetic and overflow

Errors are exact strings and the sqllogictest runner requires the expected
block to be a **substring** of the message
(`tools/sqllogictest/src/runner.rs:1086 fn error_matches`), so the 128-bit
types must reproduce DuckDB's wording verbatim:

```
Out of Range Error: Overflow in addition of INT128 (a + b)!
Out of Range Error: Overflow in subtraction of INT128 (a - b)!
Out of Range Error: Overflow in multiplication of INT128 (a * b)!
Out of Range Error: Overflow in left shift (1 << 127)
```

Unsigned overflow spells **`UINT128`**. `-(min HUGEINT)` and `abs(min
HUGEINT)` fail with `Conversion Error: Type UINT128 with value … can't be cast
because the value is out of range for the destination type INT128`. `%` and
`//` by zero: `Invalid Input Error: Division by zero in expression (1 % 0).
Use TRY(...) …`; `/` by zero is `inf` (it is `DOUBLE` division).

pinkdb today: `crates/pinkdb/src/exec/expr.rs:2275 fn overflow_err` →
`"Overflow in {what} of {ty} ({detail})"` with `ty = "INTEGER" | "BIGINT"`,
e.g. `Overflow in addition of INTEGER (2147483647 + 1)`. Only 6 corpus files
expect `Out of Range Error: Overflow in` and only 2 are integer-related, so the
plan uses DuckDB's spelling **for the two 128-bit types only**; unifying the
older `INTEGER`/`BIGINT` messages is a separate cheap follow-up that this wave
does not take.

### 2.5 Casts

- `DOUBLE → HUGEINT` rounds half away from zero (`1.5→2`, `-1.5→-2`, `1.9→2`);
  `1e300` → `Conversion Error: Type DOUBLE with value 1e+300 can't be cast
  because the value is out of range for the destination type INT128`.
- text → 128-bit accepts an exponent and surrounding spaces (`'3.6e2'`→360,
  `'  12  '`→12, `'1.5'`→2) and rejects garbage with `Conversion Error: Could
  not convert string 'abc' to INT128`.
- `-1::BIGINT::UHUGEINT` → `Conversion Error: Type INT64 with value -1 can't be
  cast … destination type UINT128`.
- `HUGEINT → DECIMAL(38,0)` is exact; `HUGEINT → DECIMAL(38,5)` overflows with
  `Conversion Error: Could not cast value … to DECIMAL(38,5)`.
  `DECIMAL(10,3) → HUGEINT` truncates (`12.345`→12). `→ BOOLEAN`, `→ DOUBLE`,
  `→ VARCHAR` all exist (`1::HUGEINT::VARCHAR` is `1`, no padding).

### 2.6 Comparison, hash, ordering

Ordinary signed/unsigned order; `hash(1::HUGEINT) = hash(1::BIGINT)` and
`hash(1::UHUGEINT) = hash(1::HUGEINT)` are both `true` (DuckDB folds
cross-width numeric equality into one hash), `hash(170141183460469231731687303715884105727)
= 17464040461585920464`. pinkdb does not have to match DuckDB's hash *values*
anywhere (no protocol carries them), only the equality relation it implies:
`GROUP BY`/`DISTINCT` on a `HUGEINT` column must hash equal values equally, and
`INTEGER 1`/`HUGEINT 1` must group together — which `compare_values` already
decides.

### 2.7 Functions

`bit_count(HUGEINT) → TINYINT`; `<<`/`>>` keep `HUGEINT` (arithmetic shift, so
`-1 >> 1 = -1`); `greatest`/`least`/`coalesce` keep `HUGEINT`; `^`, `power`,
`sqrt` → `DOUBLE`; `//` keeps the input type; `round(HUGEINT) → HUGEINT`.
pinkdb has none of `<<`, `>>`, `bit_count`, `//` for any type, so these are not
part of this wave (§9).

### 2.8 UHUGEINT

`UHUGEINT` columns order/compare unsigned, print unsigned
(`340282366920938463463374607431768211455`), and appear in
`describe`/`information_schema` as `UHUGEINT`. Its only arithmetic in
`UHUGEINT` space is same-type `+`, `-`, `*`, `%`; mixing with a signed type is
`DOUBLE` (§2.3).

## 3. Engine representation

Two type variants, two value variants, two vector payloads. `DECIMAL` cannot be
reused: `DECIMAL(38,0)` tops out at 10³⁸−1 while `HUGEINT` reaches 1.70×10³⁸,
and every `Decimal` arm carries a `scale`.

| layer | addition |
|---|---|
| `crates/pinkdb/src/types.rs:124 DataType` | `Hugeint`, `Uhugeint`; `name()` `"HUGEINT"`/`"UHUGEINT"`; `from_sql`/`from_sql_params` accept the spellings (DuckDB's aliases `INT128`/`UINT128` too); `is_numeric` true for both |
| `crates/pinkdb/src/types.rs:398 Value` | `Hugeint(i128)`, `Uhugeint(u128)`; `PartialEq` (hand-written, `:441`), `data_type()` `:501`, `Display` `:519` |
| `crates/pinkdb/src/vector.rs:18 VectorData` | `Int128(Vec<i128>)`, `UInt128(Vec<u128>)`; `dtype()` `:118` |
| `crates/pinkdb/src/plan.rs:3029 compare_values` | the pairwise match is ~120 lines of cross arms; add the two variants **through a normalizing helper** (`fn wide(v) -> Option<(bool /*unsigned*/, i128-or-u128)>`) rather than by hand, so the arm count does not grow by 4×. Rule: `HUGEINT` compares with every signed integer/`DOUBLE`/`DECIMAL`; `UHUGEINT` compares with unsigned integers, `HUGEINT` (unsigned promotion — DuckDB orders `-1::HUGEINT < 0::UHUGEINT`), `DOUBLE` and `DECIMAL` |
| `crates/pinkdb/src/exec/mod.rs:7986 encode_key_into` | `Int128` → `push_be_i128` (`:8058`, sign-flipped BE); `UInt128` → plain unsigned BE, the `Uuid` arm's shape |
| `crates/pinkdb/src/stats.rs:62 key_hash`, `:94 value_hash`, `:429` zone map | mix 128 bits like `Decimal` does (hi/lo), never truncate |
| `crates/pinkdb/src/sql/binder.rs:837 fixed_operand_dtype`, `:857 decimal_operand_width`, `:884 common_numeric`, `:898 arithmetic_dtype` | the promotion table of §2.3; `Divide → Double` already handles `/` |
| `crates/pinkdb/src/exec/expr.rs` arithmetic | `arith_typed` `:2009` already has a `Decimal` row path; add the 128-bit rows via `checked_add/sub/mul`. `ScalarCmp::new` `:1093`: a 128-bit column against a same-width literal is the existing generic `retain_cmp<T>` `:1540` path with `T = i128`/`u128` (no new `CmpKind` variant); against an `INTEGER`/`BIGINT`/`DOUBLE`/`DECIMAL` literal it needs its own `CmpKind` arm, the way `Int64Float` and `DecDec` do |
| `crates/pinkdb/src/optimizer.rs:402 fold_expr` | **no change**: folding reuses the exec kernels, so it gets the type for free |

## 4. Every boundary that must learn the type

| boundary | change | evidence |
|---|---|---|
| `.pink` dtype tag | `dtype_tag` `:400` / `dtype_from_tag` `:425` gain 12 = HUGEINT, 13 = UHUGEINT; parameterless, so no footer extra bytes (the `Decimal`/`Array` extra-byte path is not used). Encoders: PLAIN (`u32 rows | 16-byte LE | nulls zeroed`, the UUID shape at `:1868`), plus RLE and bit-packing over the two 64-bit halves (bit-packing a 128-bit value needs two packed streams or a per-block "too wide to bit-pack" fallback — see §8 step 5) | `crates/pinkdb/src/persist.rs` |
| zone maps / statistics | `SV_*` value tags `:209` gain 11 = HUGEINT, 12 = UHUGEINT; `write_stat_value` ~`:3040`, `read_stat_value` `:3068`; min/max via the same signed/unsigned order | `crates/pinkdb/src/persist.rs` |
| WAL | value tags gain 13/14 (`VALUE_UUID = 12` at `:2615`, comment there: changing a tag means the payload version changes with it); `encode_value` `:2644`, decode `:2818` | `crates/pinkdb/src/wal.rs` |
| prepared replay | `crates/pinkdb/src/prepared.rs:296` renders `Value::Uuid` as `CAST('…' AS UUID)` and `:331` `Decimal` as `CAST('…' AS DECIMAL(38,s))`; HUGEINT gets `CAST('{x}' AS HUGEINT)` |  |
| wire protocol | `dtype_tag`/`value_tag` gain the two tags; decoder errors already name the tag (`"unknown value tag {other}"` `:1178`, `"unknown data type tag {other}"` `:1209`); bump `PROTOCOL_VERSION` 15 → 16 (§7) | `crates/pinkdb-server/src/protocol.rs:90` |
| CLI renderer | **no per-type code**: `pinkdb-cli` prints `dtype.name()` (`:901`, `:1577`), so a HUGEINT column renders `HUGEINT`. DuckDB's shell prints the *physical* name (`int32`, `int64`, `int128`, `uint128`, `decimal(10,2)`), so pinkdb already differs from DuckDB's type row for every type; matching `int128` is a separate CLI-wide change, not a HUGEINT one (§9) | `pinkdb-cli/src/main.rs` |
| C ABI | new tags 13 `PINKDB_TYPE_HUGEINT`, 14 `PINKDB_TYPE_UHUGEINT`, cell = canonical text (the `Uuid` precedent at `crates/pinkdb-ffi/src/lib.rs:462 fn dtype_tag`). `PINKDB_ABI_VERSION` stays 1: DECIMAL/INTERVAL/VARIANT/ARRAY were all added additively inside ABI v1 (`include/pinkdb.h:93-124`) |  |
| bindings | each binding already funnels every text tag through one arm (`bindings/node/native/src/lib.rs:152 PINKDB_TYPE_VARCHAR | PINKDB_TYPE_DECIMAL | PINKDB_TYPE_INTERVAL => string`); add the two tags to it → `string`. No new bind path: like DECIMAL, a 128-bit value is bound as text plus `CAST(? AS HUGEINT)` inside SQL |  |
| bindings conformance corpus | `bindings/conformance/corpus.json` + `README.md` table: two type names, canonical decimal text, `CAST(? AS HUGEINT)` bind case | `bindings/conformance/README.md` |
| Parquet | §5 |  |
| CSV | **no change**: the sniffer's cascade is Integer→Bigint→Double→Date→Timestamp→Varchar (`crates/pinkdb/src/csv.rs:902 fn dtype`), which is exactly what DuckDB infers (`read_csv_auto` gives BIGINT for `1` and BIGNUM for `3402823…11455`); a HUGEINT column exported to CSV reads back as BIGINT there too. `field_value` (`:1199`) and `parse_type_name` (`:368`) still accept `'HUGEINT'`/`'UHUGEINT'` as declared types |  |
| DuckLake | list only, no change this wave: `crates/pink-lake/src/stats.rs:425 fn dtype_name` needs `"hugeint"`/`"uhugeint"` plus its inverse parser and the value encode/decode at `:491`/`:534`, and `crates/pink-lake/src/zone.rs:387 fn comparable`'s numeric set needs both — otherwise a lake with a HUGEINT column fails the stats write. Treat as a hard prerequisite *only if* the lake gate runs a HUGEINT file (§8 step 9) |  |
| JSON / VARIANT | **no change**: `crates/pinkdb/src/variant.rs:1437 Value::Decimal → Variant::Decimal(x, scale)`, so `Hugeint(x) → Variant::Decimal(x, 0)` prints bare digits exactly like DuckDB (`to_json(1::HUGEINT << 100)` → `1267650600228229401496703205376`) and reuses the existing exact 128-bit path |  |
| Arrow | `extensions/arrow/src/types.rs`: read side has no `Int128` arm (`:66-81`); write side maps `Decimal → Decimal128(p,s)` (`:149`), so a HUGEINT column must cross as `Decimal128(38,0)` and comes back as `DECIMAL(38,0)` — the type is lost, matching DuckDB's ArrowConverter. State the loss in the doc and test the value, not the type |  |
| extension ABI | `crates/pinkdb/src/ext.rs:86 ext_type_of` returns `None` for `Decimal`/`Uuid` ("no ABI type … refused at bind") and `:620` panics on a `Uuid` payload; HUGEINT does the same — `None`, refused at bind, no `ExtType` variant |  |

## 5. Parquet

Two DuckDBs disagree here, and both matter.

- **The installed binary** (`v2.0.0-alpha46057`) writes `HUGEINT` and
  `UHUGEINT` columns as Parquet **`DOUBLE`**, with no metadata: measured with
  `parquet_schema()` and `parquet_kv_metadata()`, and
  `typeof(col)` reads back `DOUBLE`. Lossy.
- **The corpus** (`copy/parquet/writer/parquet_write_hugeint.test`,
  `parquet_write_int128_exact.test`, from the `ducklake-vortex` checkout)
  requires an exact round trip, `typeof` back to `HUGEINT`/`UHUGEINT`, and a
  `Parquet metadata keys starting with duckdb.int128. are reserved` error. That
  is DuckDB's newer layout, readable in
  `~/Desktop/projects/ducklake-vortex/ducklake/duckdb/extension/parquet/`:

  - physical `FIXED_LEN_BYTE_ARRAY`, `type_length = 16`, **no logical type and
    no converted type** (`parquet_writer.cpp:338` and the reader's validation
    at `parquet_reader.cpp:385-425`);
  - file-level key/value metadata `duckdb.int128.<schema_index>` =
    `"HUGEINT" | "UHUGEINT"`, where the index is the position in the flattened
    schema (`include/parquet_int128.hpp:23`, `AddInt128Metadata`,
    `parquet_reader.cpp:401`);
  - bytes = plain 128-bit big-endian two's complement, high 8 bytes then low 8
    (`ParquetInt128::Encode`), i.e. exactly `i128::to_be_bytes()` /
    `u128::to_be_bytes()`;
  - **no statistics**: `NullStatsUnifier` for both types
    (`parquet_writer.cpp:1136`), so min/max must not be written for these
    columns.

  pinkdb's extension already has the plumbing: `KeyValue` on the write side
  (`extensions/parquet/src/write.rs:51`), `key_value_metadata()` on the read
  side (`extensions/parquet/src/file_meta.rs:160`,
  `extensions/parquet/src/schema_map.rs:350`).

**Decision:** write the newer exact layout (FLBA(16) + the reserved key + no
statistics, and refuse user-supplied `duckdb.int128.*` keys with DuckDB's
message), and read both — a file with the marker is `HUGEINT`/`UHUGEINT`, a
bare `DOUBLE` column stays `DOUBLE`. Writing `DOUBLE` would pass interop with
the installed binary but silently lose data and never satisfy the corpus test;
that is the wrong trade for a type whose entire point is exactness.

## 6. CSV, JSON, lake — summary

CSV: nothing. JSON/VARIANT: nothing (`Variant::Decimal(x, 0)`). Lake: two
`pink-lake` matches if a HUGEINT column ever reaches a lake (§4), deferred to
the step that needs it. Arrow: crosses as `DECIMAL(38,0)`, type lost, matching
DuckDB.

## 7. Format and protocol versions

- **`.pink` format: no bump.** The container (`PINKDB06` magic, 20-byte
  trailer, `crates/pinkdb/src/persist.rs:124`) is unchanged; a dtype tag is an
  additive value inside it. pinkdb keeps no legacy readers, so the only
  consequence of a new tag is that an *older* binary reading a *newer* file
  hits `"corrupt file: bad dtype tag {b}"` (`:425`) — which should be
  reworded to say "written by a newer pinkdb" (§8 step 5) rather than bumped.
  Riding **stream C's pending 6 → 7** would add a version number without adding
  a capability and would collide with stream C's work for no benefit. If stream
  C lands first, HUGEINT simply uses tags 12/13 inside `PINKDB07`.
- **Wire protocol: bump, 15 → 16.** A value tag is part of the result stream;
  DECIMAL (v6), VARIANT (v11) and ARRAY (v13) each bumped. An old client
  decoding tag 13 would fail mid-result with `unknown value tag 13` — a clean
  error, but the version is the mechanism that says so up front. If another
  stream also needs a protocol change, the first to land takes 16 and the
  second takes 17; the constant is one line.
- **C ABI: no bump** (`PINKDB_ABI_VERSION` stays 1), following
  DECIMAL/INTERVAL/VARIANT/ARRAY.

## 8. Steps

Each step ends green on the gates it names. "Full gate" means: `cargo fmt
--all`, clippy (core + CLI), `cargo test -p pinkdb` (+ `prepared`),
encryption, `pinkdb-server` (protocol + server), FFI, CLI lake+s3, `pink-lake`,
`bench/duckdiff.py` (709 today), `pinkdb-slt check` against
`bench/sqllogictest/baseline.json`, the QuackLab goldens (117), workspace
green.

| # | Step | Tests | Gate |
|---|---|---|---|
| 1 | **Type model.** `DataType::{Hugeint,Uhugeint}`, `Value::{Hugeint,Uhugeint}`, `VectorData::{Int128,UInt128}`, `name`/`from_sql`/`is_numeric`, `data_type`, `Display`, `compare_values` via the normalizing helper, `key_hash`/`value_hash`, `encode_key_into`. No SQL surface yet. | `types.rs`/`vector.rs` unit tests: name round trip, `Display`, `PartialEq`, ordering of the extremes, group-key bytes monotone in `compare_values`, hash equal for equal values | `cargo test -p pinkdb --lib` |
| 2 | **Literals, casts, `typeof`, INSERT coercion.** `parse_int` cascade (§2.1); `CAST` text↔128 (exponent, spaces, garbage message), `DOUBLE↔128` (round-half-away, out-of-range message), `DECIMAL↔128` (truncate, overflow message), `128→VARCHAR/BOOLEAN`; `typeof`; the declared-type parser and `CREATE TABLE … HUGEINT`; `INSERT` coercion of `DOUBLE`/VARCHAR into a HUGEINT column (needed by `test_hugeint_aggregates.test`). | new `crates/pinkdb/tests/hugeint.rs`: the corpus's literal and cast cases verbatim; `duckdiff` gains a `dialect-hugeint` battery (literals, casts, `typeof`, `describe`) | pinkdb tests + duckdiff + slt `types/hugeint` subset |
| 3 | **Arithmetic, comparison kernels, `MIN`/`MAX`.** `common_numeric`/`arithmetic_dtype` per §2.3; `arith_typed` rows with checked add/sub/mul and DuckDB's exact overflow text; `ScalarCmp` for a 128-bit column against `INTEGER`/`BIGINT`/`DOUBLE`/`DECIMAL` literals; `MIN`/`MAX` accumulators accept the type. | `tests/hugeint.rs` arithmetic/overflow/comparison; duckdiff `dialect-hugeint-arithmetic`; perf micro-check that a filter on a HUGEINT column uses the same `retain_cmp` path as BIGINT | pinkdb tests + duckdiff + slt `types/hugeint` subset |
| 4 | **`SUM` result types.** `binder.rs:4872 agg_dtype`: `Sum` over `Integer`/`Bigint`/`Hugeint` → `Hugeint` (decimal and double unchanged); `aggregate.rs:1070 finalize_ref`'s `SumInt` arm returns `Value::Hugeint(exact_sum(..))` instead of `i64::try_from(..)` + the overflow error; new `Acc::SumHugeint{sum:i128, overflow:bool, non_null:bool}` for a HUGEINT input column (checked add, §2.2's message). `finalize` must emit an `Int128` vector — check every `Finalize` site. | `aggregate` unit tests per input type; `tests/aggregate.rs` for `SUM(INTEGER)` text and `typeof`; the previously-overflowing `test_sum.test` value; TPC-H Q1/Q6 SF 1 wall time before/after | **full gate** (result type) |
| 5 | **Persistence + WAL.** dtype tags 12/13, PLAIN/RLE/bit-packing, `SV_*` 11/12 and zone maps, WAL tags 13/14, `prepared` replay, the "newer pinkdb" reword of the unknown-tag error, and the format-contract test vectors. | `persist` round trip of the extremes + NULLs for every encoding (PLAIN, RLE, bit-packed, FSST/LZ refuse), zone-map pruning on a HUGEINT column, a WAL replay of `INSERT`/`UPDATE`/`DELETE`, a checkpoint that reopens, `tests/prepared.rs` replay of a `CAST('…' AS HUGEINT)` parameter | **full gate** (format) |
| 6 | **Wire, CLI, C ABI, bindings.** protocol tags + `PROTOCOL_VERSION` 16; FFI tags 13/14 with text cells; Node/Python/Go value arms; `bindings/conformance/corpus.json` cases. | `pinkdb-server` protocol round trip (dtype + value + NULL + a result column), FFI unit test, the three binding conformance runs, a `TYPE HUGEINT` parameter via `CAST(? AS HUGEINT)` in each binding | **full gate** (protocol/ABI) |
| 7 | **Parquet.** Write FLBA(16) + `duckdb.int128.<i>` + no statistics + reject reserved keys; read the marker; keep a bare DOUBLE read as DOUBLE. | round trip exact for the four extreme values (min, max, a > 2^64, NULL), `duckdb.int128.` duplicate/invalid-metadata errors, marker round trip through a real DuckDB-written file if the corpus's DuckDB supports it (else skip with a note, like VARIANT's DuckDB-interop tests), `COPY`-to-parquet of a HUGEINT column and `read_parquet` back | **full gate** (file format) |
| 8 | **UHUGEINT.** The same arms for `Uhugeint`/`UInt128`: literal cascade above `i128::MAX`, unsigned ordering, `UHUGEINT`-only arithmetic and `%`, everything mixed with a signed type → `DOUBLE`, `SUM(UHUGEINT) → DOUBLE`, casts with the `UINT128` messages, storage tags 13/14, protocol 13/14, ABI 14, parquet marker `"UHUGEINT"`. | the uhugeint half of every test above; `types/uhugeint` + `types/uuid/test_uuid_uhugeint_cast` | **full gate** |
| 9 | **Corpus re-baseline, lake, docs.** Re-run the corpus, update `bench/sqllogictest/baseline.json` + `docs/sqllogictest-baseline.md`, state the file delta; add `pink-lake`'s `dtype_name`/`comparable`/stats arms if a lake test now writes HUGEINT; `ROADMAP.md` and `docs/sql-conformance.md`. | the corpus `check` shows no regression and the expected gain; goldens 117/117; a lake round trip if in scope | **full gate** |

Steps 1–3 change no file, protocol or ABI and can land as one PR if the review
prefers; step 4 is the one that must not be bundled with anything else,
because it moves a result type. Steps 5–7 each need the full gate for their own
reason and should be separate PRs.

## 9. Out of scope

Named so review does not read their absence as an oversight:

- **LIST / `HUGEINT[]`** — 4 of the 42 files, and a type-system feature of its
  own (`function/list/aggregates/*`).
- **`BIGNUM`** — 2 files directly, 2 more with `TRY_CAST`; a decimal-like
  arbitrary-precision type, much larger.
- **`<<`, `>>`, `bit_count`, `//`** — absent for *every* type today
  (`crates/pinkdb/src/sql/ast.rs:1205`), so 3 files stay blocked.
- **`FIRST`/`LAST`** — 4 files; `AggFunc` has no such variant.
- **`TRY_CAST`** — 4+ files; orthogonal, and cheap enough to consider as a
  separate wave.
- **`top_n`, `SUMMARIZE`, `struct_pack`** — 3 files, each its own feature.
- **CLI type-row spelling** (`int128` vs `HUGEINT`) — a CLI-wide change that
  affects every type, and the QuackLab comparator's business, not the engine's.
- **Unifying `INTEGER`/`BIGINT` overflow messages** with DuckDB's `Out of Range
  Error: … INT32/INT64 …` — 2 corpus files, one function, separate PR.
- **`BIGNUM`-style `10^38 < x ≤ u128::MAX` literal → `UHUGEINT`** is in scope
  (step 8); beyond `u128` stays a parse error, which is where DuckDB says
  BIGNUM.

## 10. Open questions for review

1. **Parquet**: write the newer exact FLBA(16) layout (recommended, needed by
   the corpus) even though the installed DuckDB binary reads such a column as
   `DOUBLE`? The alternative — match the binary, write `DOUBLE` — is lossy and
   can never pass `parquet_write_hugeint.test`.
2. **`SUM` and speed**: the change is finalize-only, so the expectation is
   *zero* TPC-H delta. Step 4 measures Q1/Q6 at SF 1 to confirm rather than
   assume; if the `Finalize` output vector allocation shows up, the fix is to
   keep `SumInt`'s `Acc` and only widen the output chunk, never to add a second
   per-row accumulator.
3. **Grouping across widths**: DuckDB hashes `HUGEINT 1` and `BIGINT 1` alike,
   so a `GROUP BY` mixing the two groups them together. pinkdb's `key_hash`
   derives from the value, so this falls out of `compare_values` — confirm with
   a test in step 1 rather than a special case.

## 11. Review decisions (2026-11-02)

1. Parquet: write DuckDB's exact layout (FLBA(16), `duckdb.int128.<i>`, no
   statistics) and read both; the installed alpha binary lags the corpus.
2. Agreed: `SUM` changes in finalize only; step 4 measures TPC-H Q1/Q6 at
   SF 1 and must show no slowdown.
3. Agreed: cross-width grouping is tested in step 1.
4. Steps 1–3 run as one brief (no file, protocol or ABI change); step 4 on
   its own; 5–7 each separately; 8 after 7; 9 last.
5. Parquet, corrected against the v2.0.0 release (2026-11-02): the release
   writes HUGEINT and UHUGEINT columns as a plain `DOUBLE` with no metadata,
   and reads them back as `DOUBLE` — its own
   `parquet_write_hugeint.test` / `parquet_write_uhugeint.test` expect
   `typeof(h) = DOUBLE`. pinkdb matches the release (a cast to `DOUBLE`, so
   values above 2^53 lose the same low bits), replacing the exact FLBA(16)
   layout this plan originally chose. Arrow export stays `Decimal128(38,0)`,
   as DuckDB's ArrowConverter writes both widths (UHUGEINT keeps its raw 128
   bits, so values above `i128::MAX` read as a negative decimal).
