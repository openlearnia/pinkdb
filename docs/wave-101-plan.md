# Wave 101 — the `VARIANT` type

ROADMAP row 101: a first-class semi-structured type stored columnar, casts
from/to JSON text and from any type, `variant_type` / `variant_keys` / path
extraction, `.pink` persistence, lake data files, ordering/equality/grouping
rules, and protocol/FFI/binding encodings. The row is marked *large — plan
first*, so it is split:

- **101a (done, this document):** the type itself — value model, columnar
  storage, casts, functions, semantics, persistence, and the wire/FFI
  encodings.
- **101b (queued):** shredding, Parquet `VARIANT` read/write, lake data files
  and statistics on shredded paths. Scope at the end of this document.

Everything marked *measured* comes from `/tmp/duckdb-2.0.0/duckdb`
(2.0.0 "Cyanoptera", commit `d54cbbe3df`) run while writing this wave.

## Reference behaviour (DuckDB 2.0.0, measured)

```
typeof(CAST('{"a":1}' AS VARIANT))        -> 'VARIANT'
CAST('{"a":1}' AS VARIANT)                -> the *string* {"a":1}   -- VARCHAR -> VARIANT is identity
json('{"a":1}')::VARIANT                  -> {'a': 1}               -- the JSON extension produces the object
variant_type(json('{"a":1}')::VARIANT)    -> OBJECT
variant_type(...)                          NULL / BOOL_TRUE / BOOL_FALSE / INT32 / INT64 / UINT64 /
                                           DOUBLE / FLOAT / DECIMAL / INT128 / VARCHAR / DATE /
                                           TIMESTAMP_MICROS / INTERVAL / ARRAY / OBJECT
variant_keys({'a':1,'b':2}::VARIANT)       -> [a, b]
variant_keys(non-object)                   -> []          (empty LIST, not NULL)
variant_keys(json('null')::VARIANT)        -> NULL
({'a':[1,2,3]}::VARIANT)['a']              -> [1, 2, 3]   (subscript by field name / index)
(json('[10,20,30]')::VARIANT)[1]           -> 10          (subscripts are 1-based)
({'a':1}::VARIANT)['a']                    -> 1
(json('1')::VARIANT)['a']                  -> NULL        (a field of a scalar is a miss)
variant_extract(v, 'a')                    -> field 'a'   (second arg is a *field name*)
variant_extract(v, '$.a[0]')               -> NULL        (JSON paths are not wired up in 2.0)
variant_array_length([1,2,3]::VARIANT)     -> 3
```

Extracting element 0 of an array errors (`Extracting index 0 from
VARIANT(ARRAY) is invalid, indexes are 1-based`), which is the clearest sign
that DuckDB's subscript is 1-based while its JSONPath extraction is not
implemented at all in 2.0.

Ordering across kinds, from `ORDER BY v` over one row of each kind:

```
BOOL < NUMBER < VARCHAR < DATE < TIMESTAMP < ARRAY < OBJECT < NULL
```

Numbers compare *by value* across kinds: `1::VARIANT = 1.0::VARIANT` is true
and `GROUP BY` / `COUNT(DISTINCT)` fold `1`, `1.0` and `json('1')` into one
group (measured: `count(DISTINCT)` = 1). `'1'::VARIANT` (a string) is a
different group. A JSON null and SQL NULL are folded together for grouping but
`json('null')::VARIANT IS NULL` is **false**; that split is one of the pieces
of DuckDB behaviour this wave deliberately does not copy (see *Differences*).

Cast behaviour (measured): `json('1.9')::VARIANT::INTEGER` = 2 (`-1.9` → -2,
half away from zero), `json('"5"')::VARIANT::INTEGER` = 5, `'"1e3"'::VARIANT::DOUBLE`
= 1000.0, `json('1.005')::VARIANT::DECIMAL(10,2)` = 1.00,
`json('9223372036854775808')::VARIANT::BIGINT` errors
`Conversion Error: Can't convert VARIANT(UINT64) value '9223372036854775808'
to 'BIGINT'`, `json('null')::VARIANT::VARCHAR IS NULL` = true, and
`(json('{"a":1}')::VARIANT)::VARCHAR` = `{'a': 1}`.

## Value model

A VARIANT is one of ten kinds. The in-memory model is `crate::variant::Variant`:

```rust
pub enum Variant {
    Null,                                  // a JSON null — a value, not SQL NULL
    Bool(bool),
    Int(i64),
    Double(f64),
    Decimal(i128, u8),                     // unscaled i128 + scale, like Value::Decimal
    Str(Arc<str>),
    Date(i32),                             // days since 1970-01-01
    Timestamp(i64),                        // microseconds since 1970-01-01
    Array(Arc<[Variant]>),
    Object(Arc<[(Arc<str>, Variant)]>),    // keys sorted by UTF-8 bytes at construction
}
```

`Value::Variant(Arc<Variant>)` carries it, `VectorData::Variant(Vec<Arc<Variant>>)`
holds a column. Objects are kept key-sorted at construction so the binary
encoding is canonical: two structurally equal objects have byte-identical
encodings regardless of input key order. Duplicate keys are impossible
(construction keeps the last value, like JSON parsers do). Nesting is capped at
`MAX_DEPTH = 128` levels, enforced wherever a value enters the engine (the JSON
parser and the binary decoder), so encoding and comparison can recurse without
a depth check.

Deliberate simplifications against DuckDB's physical kinds: one integer width
(`i64`, reported as `BIGINT`) instead of INT32/INT64/UINT64/INT128, and no
FLOAT, BLOB or UUID kinds. INTERVAL has no VARIANT kind either: it becomes its
canonical text (a `Str`), which is the same spelling the engine already prints
for intervals.

## `.pink` encoding

Binary, little-endian, self-describing per value — the kind is a tag byte, not
text. This is the same shape the Parquet `VARIANT` logical type uses (a type
dictionary plus a value buffer), with the dictionary inlined per value; 101b
hoists it to one dictionary per column.

```
tag  0x00 NULL        (no payload)
     0x01 BOOL        u8 (0/1)
     0x02 INT         i64
     0x03 DOUBLE      f64 bits
     0x04 DECIMAL     u8 digits, u8 scale, i128 unscaled   (digits is informational)
     0x05 STRING      u32 byte length + UTF-8
     0x06 DATE        i32
     0x07 TIMESTAMP   i64
     0x08 OBJECT      u32 field count + count * (u32 key length + key + value)
     0x09 ARRAY       u32 element count + count * value
```

A page payload is `u32 value count | u32 offset table (count + 1 entries) |
concatenated values`, mirroring how VARCHAR pages are already written, so the
existing page framing, validity bitmap, compression and CRC machinery are
reused unchanged. A NULL bitmap bit means SQL NULL and contributes zero payload
bytes; a `Variant::Null` row contributes one byte (`0x00`). The kind tags are
*not* the storage encoding tags (`ENC_*`): VARIANT pages are always
`ENC_PLAIN`.

New format tags (all appended after the highest tag in use, so no existing
byte changes):

| place | constant | value |
|---|---|---|
| `persist.rs` column dtype tag | `dtype_tag(Variant)` | `9` |
| `persist.rs` default-value tag | `VD_VARIANT` (`u32 length` + encoded value) | `9` |
| `pinkdb-server` wire dtype tag | `dtype_tag::VARIANT` | `9` |
| `pinkdb-server` wire value tag | `value_tag::VARIANT` (`u32 length` + encoded value) | `10` |
| `pinkdb-ffi` type tag | `PINKDB_TYPE_VARIANT` (payload = JSON text) | `10` |

An older file simply has no VARIANT column, so `dtype_from_tag` never sees tag
9 there; a pinkdb older than this wave that meets tag 9 reports
`corrupt file: bad dtype tag 9`, which is the no-legacy failure this repo asks
for (no reader shim, no conversion).

VARIANT columns carry **no min/max statistics** (written as `SV_NONE`, so an
old reader needs no new tag), and therefore no zone map: the total order below
is defined, but a truthful min/max over a semi-structured column is not worth
the bytes until 101b shreds typed paths.

## Semantics

**Equality** is structural: same kind and same value, except that numbers
compare by *value* across kinds — `1` equals `1.0` equals a `DECIMAL(3,1)`
holding `1.0`. A DOUBLE is compared through its shortest round-trip decimal
form, so `0.1::VARIANT = 0.1::DECIMAL(2,1)::VARIANT` is true and a decimal that
no f64 round-trips (for example `DECIMAL(19,19)`'s `1.0000000000000000001`)
does not equal `1`. A number never equals a string that spells it (`'1'` ≠
`1`); a `DATE` never equals a `TIMESTAMP` or a string. SQL NULL equals
nothing, not even a JSON null.

**Total order** (used by `ORDER BY`, `MIN`/`MAX`, index range scans):

```
SQL NULL  (last in both directions, like every other pinkdb column)
  < Variant::Null
  < Bool           false < true
  < number         by value only (1 == 1.0 == 1.00, no kind tie-break)
  < Str            byte order
  < Date           by day count
  < Timestamp      by microsecond count
  < Array          lexicographic, then shorter first
  < Object         lexicographic over (key, value) pairs, then fewer fields first
```

Within a kind, arrays and objects order lexicographically; because objects keep
their keys sorted, that is a total order. `compare_values` puts SQL NULL last
for `ORDER BY`; VARIANT does not change that.

ORDER BY does not use the order-preserving sort arena for VARIANT columns
(`encode_sort_keys` bails and the generic `Value` comparator runs), so the
comparison operators, `ORDER BY`, `MIN`/`MAX` and the hash all agree by
construction rather than by keeping two encodings in step. Sorting cost is the
same as any VARCHAR column; queries with no VARIANT column take no new path.

**Grouping / hashing / DISTINCT** use one canonical byte string per value
(`variant::hash_key`): a class byte, then numbers normalized to a single
decimal form (`-0.0` folds into `0.0`, all NaNs into one pattern, integral
decimals drop their trailing zeros), object keys already sorted. That is
exactly what `=` reports, so `GROUP BY`, `DISTINCT`, `JOIN` keys and `MIN`/`MAX`
agree with the comparison operators. SQL NULL and `Variant::Null` are different
groups (deliberate; see below), so `SELECT DISTINCT v` over
`{1, 1.0, '1', true, JSON null, NULL}` returns five rows while
`count(DISTINCT v)` returns four (it skips SQL NULL).

**NULL vs JSON null:** `NULL` (SQL) means "no value" and is the NULL bitmap
bit; `Variant::Null` is the JSON `null` *value*. `variant_type(NULL)` is SQL
NULL; `variant_type(CAST('null' AS VARIANT))` is also SQL NULL (matching
DuckDB), but the JSON null is still a value: `... IS NULL` is false and it is
its own group. A missing path or a field of the wrong kind yields SQL NULL,
never a `Variant::Null`.

**Casts** (`CAST(v AS T)`; pinkdb has no `TRY_CAST` at all, so a failed cast is
an error — the brief's "NULL under TRY_CAST" is the one item not delivered, see
*Not done*).

| from | to | rule |
|---|---|---|
| any scalar | VARIANT | widen: BOOLEAN→Bool, INTEGER/BIGINT→Int, DOUBLE→Double, DECIMAL→Decimal, VARCHAR→*JSON text parsed*, DATE→Date, TIMESTAMP→Timestamp, INTERVAL→Str, NULL→SQL NULL |
| VARIANT | VARIANT | identity |
| VARIANT | VARCHAR | the value's JSON text (`"hi"` keeps its quotes, a JSON null is the text `null`) |
| any VARIANT | BOOLEAN | Bool; a Str `"true"`/`"false"` (case-insensitive); anything else errors |
| any VARIANT | INTEGER/BIGINT | Int; a Decimal of scale 0 in range; a DOUBLE (rounds half away from zero, out of range errors); a Str of digits |
| any VARIANT | DOUBLE | Int, Double, Decimal, a Str that parses |
| any VARIANT | DECIMAL(p,s) | any number rescaled through the existing decimal conversion, or a Str; out of range errors |
| any VARIANT | DATE / TIMESTAMP | the matching kind; DATE→TIMESTAMP widens to midnight; a Str that parses |
| `Variant::Null` | every type except VARCHAR | SQL NULL |
| anything else | VARIANT / VARIANT → kind | `cannot CAST …` error |

Error text names the kind and the value, in the shape DuckDB uses:
`cannot CAST VARIANT(VARCHAR) value "hi" to INTEGER`,
`cannot CAST VARIANT(DECIMAL) value 9223372036854775808 (out of range) to BIGINT`.

`VARCHAR → VARIANT` *parses JSON*, unlike DuckDB (where the cast is the
identity and the JSON extension is needed first). This is the deliberate
difference that buys the most: it gives VARIANT a SQL literal form
(`'{"a":1}'::VARIANT`) that the parser, the WAL (which replays statement text)
and `value_literal` (used to inline bound parameters) all already carry.

**JSON round-trip.** `VARIANT → VARCHAR → VARIANT` is exact for JSON's value
model: object keys come out sorted, unicode stays raw UTF-8 (`\uXXXX` only for
control characters), duplicate keys keep the last value, and numbers keep their
kind — an integral literal that fits `i64` becomes `Int`, a fractional literal
that fits `DECIMAL(38, s)` stays a `Decimal` (so `1.0` and
`-1234567890123.456789` round-trip digit for digit), anything wider becomes
`Double`. `NaN`, `Infinity` and `-Infinity` have no JSON form but survive as
those three spellings. `DATE` and `TIMESTAMP` print as quoted ISO strings and
read back as `Str` — documented, and the reason `.pink`/WAL use the binary
encoding rather than JSON text (`Variant::is_json_exact` reports this).

## Functions

New core scalar functions:

| function | returns | notes |
|---|---|---|
| `typeof(x)` | VARCHAR | `'VARIANT'` for a VARIANT column, the existing name for every other type |
| `variant_type(v)` | VARCHAR | `NULL`, `BOOL`, `BIGINT`, `DOUBLE`, `DECIMAL`, `VARCHAR`, `DATE`, `TIMESTAMP`, `ARRAY`, `OBJECT`; SQL NULL for SQL NULL *and* for the JSON null (DuckDB parity) |
| `variant_keys(v)` | VARCHAR | JSON array of the object's keys (`'["a","b"]'`), `'[]'` for a non-object, SQL NULL for SQL NULL and the JSON null. pinkdb has no LIST type, so this is text, not `VARCHAR[]` |
| `variant_extract(v, path)` | VARIANT | path is a field name (`'a'`), a JSONPath (`'$.a[0]'`) or a dotted/indexed path (`'a.b[1]'`); **0-based** indices; missing path or a scalar's field → SQL NULL; malformed path → `invalid VARIANT path "a["` |

All four are handled by the typed per-column evaluator (a new
`eval_scalar_typed` step that runs before the existing fast paths), so a query
that does not mention VARIANT takes no new path.

pinkdb has no subscript operator in its grammar (`v['a']`, `v.a` do not parse
for any type), so extraction is spelled `variant_extract(v, 'a.b[1]')`. That is
DuckDB's own third spelling; the subscript one is left to a later dialect wave
and recorded in `docs/GAPS.md`.

The JSON extension's functions (`json_extract`, `json_array_length`,
`json_valid`, …) accept a VARIANT too: the binder inserts the one documented
widening `VARIANT → VARCHAR` for an extension argument declared `VARCHAR` (the
other widening, `numeric → DOUBLE`, already existed). No `json_*` function is
re-plumbed through the extension ABI; see below.

## Encodings elsewhere

- **Wire protocol (`pinkdb-server`, v11):** dtype tag `9`, value tag `10` with
  a `u32` length and the encoded value bytes, so a client reads a VARIANT as an
  opaque self-describing blob (and an old client reports an unknown tag rather
  than mis-reading it). `row_encoded_len` computes the exact frame length.
- **FFI (`pinkdb-ffi`):** `PINKDB_TYPE_VARIANT = 10`, and the cell's `text`
  field carries the JSON text (the only exact spelling the C ABI has). A
  VARIANT holding DATE/TIMESTAMP comes back as a VARCHAR-like string, which is
  documented in the header.
- **WASM / JS bindings:** `Value::Variant` renders as its JSON text, so it
  crosses as a native JSON value.
- **Ext ABI (`pinkdb-ext-loader`): refused.** `ext_type_of(Variant)` returns
  `None` exactly like DECIMAL: extensions v1 have no byte-string payload kind,
  so a VARIANT argument to an extension is a bind-time error rather than a
  silently lossy encoding. Tracked follow-up: an `ExtType::Variant` whose
  payload is the binary encoding, when a second extension needs it.
- **Arrow extension:** refused with a clear `Err` (arrow has no VARIANT type
  pinkdb can write exactly).

## Lake and Parquet

- `.pink` lake data files: free — the lake writes through the same persist
  encoder, so a VARIANT column round-trips (covered by the shared
  `format_contract` test, which now carries a `VARIANT` column with nested,
  unicode and NULL rows). The catalog type spelling gains `variant`; the stats
  block emits no min/max for it.
- Parquet lake: a VARIANT column is **refused** with a message naming 101b
  (`VARIANT needs parquet's shredded representation (wave 101b)`), the same way
  INTERVAL is refused today, rather than written as text or a blob.

## Not done in 101a

- **`TRY_CAST`** does not exist anywhere in pinkdb (no parser, binder or plan
  support). Adding it is a general language feature that touches `ast.rs`,
  `binder.rs` and `plan.rs` for every type, not a VARIANT concern, so a failing
  VARIANT cast is an error and the "NULL under TRY_CAST" half of the brief is
  left undone with that reason.
- **`variant_extract_string`, `variant_array_length`, `variant_exists`,
  `variant_normalize`, `variant_comparator`** (DuckDB 2.0 functions) are not
  implemented: `variant_extract` plus `CAST(... AS VARCHAR)` covers the first,
  and the rest are cheap follow-ups once something asks for them.
- **Subscripts** `v['a']` / `v.a` and the path argument of `variant_type` /
  `variant_keys` (`variant_type(v, 'a')`).

## 101b (queued) — from this plan's scope

1. **Shredding.** Split a VARIANT column into typed sub-columns (a
   `typed_value` column per kind present, plus a leftover `unshredded` blob
   column), with the "is the shredding valid" invariants Parquet's `variant_shredding`
   defines; a reader reconciles the two.
2. **Parquet VARIANT.** Write the `Variant` logical type (metadata dictionary +
   shredded value buffers + the `unshredded` column) and read it back,
   replacing the refusal with a real path. This is why the `.pink` encoding
   above separates kinds with a tag byte, and why objects are key-sorted.
3. **Lake data files and statistics.** Per-path statistics and zone maps on
   the shredded columns (`stats.rs` already keys statistics by column; 101b
   adds path-qualified keys), pruning on them, and the `.pink` lake stats block
   growing path-qualified entries.
4. **Pressure from 101a that 101b should revisit:** a shared per-column type
   dictionary (this wave writes one per value), a LIST type so
   `variant_keys` can return `VARCHAR[]` like DuckDB, and an
   `ExtType::Variant` payload for extensions.

## Differences from DuckDB, recorded

| # | DuckDB 2.0 | pinkdb 101a | why |
|---|---|---|---|
| 1 | `VARCHAR → VARIANT` is identity; `json()` builds the value | the cast parses JSON | gives a literal form the parser/WAL already carry, without the JSON extension |
| 2 | INT32/INT64/UINT64/INT128/FLOAT kinds | one `Int(i64)`, `Double`, `Decimal` | pinkdb has no unsigned/half-precision types; one width keeps `variant_type` honest |
| 3 | `variant_type` = `BOOL_TRUE`/`BOOL_FALSE`, `TIMESTAMP_MICROS` | `BOOL`, `TIMESTAMP` | pinkdb's own type names |
| 4 | a fractional JSON number is DOUBLE (so `1.0` is DOUBLE) | kept exact as DECIMAL when it fits | the text round trip stays digit-exact |
| 5 | JSON null == SQL NULL for `DISTINCT`, but `IS NULL` false | strictly distinct everywhere | consistency: `IS NULL`, grouping and `DISTINCT` must agree |
| 6 | `MIN`/`MAX`/`ORDER BY` put both nulls last | variant-null orders just after SQL NULL | keeps one total order for all values |
| 7 | `variant_keys` → `VARCHAR[]` | JSON array text | no LIST type yet |
| 8 | `variant_type(v,'$.a')` → NULL (paths unwired) | JSONPath accepted, incl. `$.a[0]`, 0-based | cheap, and the brief asks for path extraction |
| 9 | subscripts are 1-based (`v[1]` is the first element) | `variant_extract(v,'[0]')` is the first element | JSONPath convention, and there is no subscript operator to be 1-based about |
| 10 | `VARIANT → VARCHAR` prints `{'a': 1}` (struct text), a JSON null is NULL | prints JSON `{"a":1}`, a JSON null is `null` | round-trips through cast #1 |
| 11 | `INTERVAL` is a VARIANT kind | interval becomes its canonical text | pinkdb's interval is an engine-only type; the text is lossless for display |
| 12 | `variant_extract_string`, `variant_array_length`, `variant_exists`, `variant_normalize`, `variant_comparator` | absent (see *Not done*) | scope: 101a is the type, not the whole function family |
