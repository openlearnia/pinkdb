# pinkdb binding conformance corpus

One language-neutral corpus that every pinkdb binding runs: the same SQL, the same
bound parameters and the same expected rows, types, statement kinds, affected-row
counts and error classes. `corpus.json` is the only source of truth; the runners live
next to the bindings:

| Binding | Runner | Command |
| ------- | ------ | ------- |
| Python  | `bindings/python/tests/test_conformance.py` | `python3 -m unittest discover -s bindings/python/tests -v` |
| Node.js | `bindings/node/test/conformance.test.js` | `node --test bindings/node/test` |
| Go      | `bindings/go/conformance_test.go` | `go test ./...` in `bindings/go` |

A binding passes when it runs every case and every step and every expectation holds.
Runners must not weaken an expectation to make it pass; a mismatch means the binding
(or, rarely, the corpus) is wrong.

## Value encoding

`null` is SQL NULL. Every non-NULL value is a two-element array `[type, value]`.
The `type` string is exactly the pinkdb type name that the C ABI reports in
`pinkdb_column_type` (`boolean`, `integer`, `bigint`, `double`, `varchar`, `date`,
`timestamp`, `decimal`, `interval`).

| type | JSON value | notes |
| ---- | ---------- | ----- |
| `boolean` | `true` / `false` | |
| `integer` | number | 32-bit signed range |
| `bigint` | decimal string | outside the JSON safe-integer range on purpose |
| `double` | string | parsed as a float; `NaN`, `Infinity`, `-Infinity` are valid; compared numerically, so `1e300` and `1E+300` both match |
| `varchar` | string | UTF-8, may contain newlines, tabs, quotes and any non-NUL code point |
| `date` | `YYYY-MM-DD` | days since the Unix epoch on the wire |
| `timestamp` | `YYYY-MM-DD HH:MM:SS.ffffff` | always exactly six fractional digits; µs since the epoch on the wire, naive UTC |
| `decimal` | canonical decimal string | scale-preserving text from the engine (`12.350`, `-0.01`) |
| `interval` | canonical interval string | text from the engine (`1 day 02:00:00`, `-3 months`) |
| `hugeint` | exact decimal string | the engine's text, e.g. `170141183460469231731687303715884105727`; never a JSON number (it would lose digits) |
| `uhugeint` | exact decimal string | as `hugeint`, unsigned |

The 128-bit integers cross the ABI as text cells (tags 13 and 14), the same
channel as `decimal` and `interval`: no binding has a lossless numeric type on
every path (a JS number is 53-bit, a Go `database/sql` value is 64-bit), so all
three surface them as strings. A test converts with `int(...)` / `BigInt(...)`
when it wants arithmetic.

`decimal` and `interval` values are never bound as parameters in ABI v1 — the corpus
binds their canonical text and `CAST(? AS DECIMAL(p,s))` / `CAST(? AS INTERVAL)`s it
inside SQL, which every binding supports.

## Schema

```json
{
  "corpus_version": 1,
  "abi_version": 65539,
  "cases": [
    {
      "name": "types_roundtrip",
      "description": "…",
      "setup": ["CREATE TABLE …", "INSERT INTO …"],
      "steps": [ … ]
    }
  ]
}
```

`abi_version` is the C ABI version (`major << 16 | minor`) the expectations were produced with; a binding
whose library has another major version, or an older minor, fails the whole corpus instead of guessing.

`setup` statements run once per case, in order, on a fresh database (in-memory where
the binding offers it). Each step is one of the ops below.

### `query`

```json
{"op": "query", "sql": "SELECT …", "params": [ … ], "expect": {"columns": […], "types": […], "rows": [[cells…]…]}}
```

`params` is optional. When present the query must go through a prepared statement
(positional `?` placeholders); without it a plain query is fine. `types` are the
reported column type names, in column order.

### `prepared_query`

Same expectation shape as `query`, but the binding must always prepare and execute
the statement, even when it has no placeholders.

### `exec`

```json
{"op": "exec", "sql": "CREATE TABLE …; INSERT …; SELECT …", "expect": [{"kind": "created"}, {"kind": "inserted", "affected": 2}, {"kind": "selected", "columns": […], "types": […], "rows": […]}]}
```

Run the whole batch through the binding's multi-statement API. `expect` has one entry
per statement. `kind` is the pinkdb statement kind name (`created`, `inserted`,
`updated`, `deleted`, `selected`, `began`, `committed`, `rolledback`, …), `affected`
is the affected-row count (0 when absent), and statements that produce rows carry the
same `columns` / `types` / `rows` fields as `query`.

### `exec_prepared`

```json
{"op": "exec_prepared", "sql": "INSERT INTO t VALUES (?, ?)", "params": […], "expect": [{"kind": "inserted", "affected": 1}]}
```

Prepare one statement, execute it once with `params`, check the kind and affected
count. This is the only op whose expectation is a list for a single statement, so a
runner can share the `exec` checker.

### `error`

```json
{"op": "error", "sql": "SELECT 1 / 0", "class": "execution", "message_contains": "division by zero"}
```

The step must fail with the stable C ABI error class `class` (see
`pinkdb_last_error_class` in `crates/pinkdb-ffi/include/pinkdb.h`) and a message
containing `message_contains`. When `params` is present the statement is prepared
first — `"params": []` therefore means "prepare, then execute with no values", which
is how a parameter-count error is provoked. The class names are part of the C ABI
contract: `parse`, `catalog`, `type`, `parameter`, `execution`, `constraint`, `io`,
`csv`, `invalid_argument`, `internal`.

### `stream`

```json
{"op": "stream", "sql": "SELECT …", "expect": {"columns": ["i"], "types": ["bigint"], "row_count": 100000, "first_rows": [[…], […]], "last_rows": [[…], […]], "checksum": 4999950000}}
```

Run the query and consume the rows without materializing an ordinary result set when
the binding can stream. Check the column metadata, the total row count, the first and
last two rows, and `checksum`: the sum of the first column over every row (the column
is `integer`/`bigint`; the sum stays inside 2^53).

## Adding a case

Produce the expected values with the reference implementation, not from the binding
you are about to test:

```sh
cargo run --release -p pinkdb-cli -c "SELECT …"
```

then transcribe them into the encoding above, update the case's `description`, and
run all three runners. Keep cases independent: each starts from an empty database and
only relies on its own `setup`. Do not add a case for behaviour that differs per
binding (transaction defaults, API naming) — that belongs in the binding's own suite.
