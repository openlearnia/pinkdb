# PinkDB for Node.js

A native N-API addon over the [pinkdb C ABI](../../crates/pinkdb-ffi/include/pinkdb.h).
It embeds the engine in the Node.js process: no server, no helper binary.

```js
const { open } = require('@pinkdb/pinkdb');

const db = open('app.pinkdb'); // omit the path for an in-memory database
db.run('CREATE TABLE users (id INTEGER, name VARCHAR, score DOUBLE)');
db.run('INSERT INTO users VALUES (?, ?, ?)', [1, 'Ada', 9.5]);

const rows = db.query('SELECT id, name, score FROM users ORDER BY id');
// [ { id: 1, name: 'Ada', score: 9.5 } ]
db.close();
```

## Building

The addon links `libpinkdb_ffi` statically, so the addon is the only artifact
you need:

```sh
npm install          # no runtime dependencies; nothing is fetched from npm
npm run build        # cargo build --release + copy to build/pinkdb.node
npm test             # node --test test
```

`npm run build` needs a Rust toolchain and the pinkdb source checkout (the
addon builds the engine from `crates/pinkdb`). It writes `build/pinkdb.node`.

## API

### `open(path?)` / `openMemory()`

Opens a database file, or an in-memory database when `path` is omitted. Returns
a `Database`.

### `Database`

| Method | Result |
| --- | --- |
| `run(sql, params?)` | one result per `;`-separated statement (see below) |
| `query(sql, params?)` | the rows of a single statement, as objects keyed by column name |
| `prepare(sql)` | a `Prepared` |
| `begin()` / `commit()` / `rollback()` | run `BEGIN` / `COMMIT` / `ROLLBACK` |
| `close()` | close the database; later use throws a `PinkdbError` |

With `params` the SQL must be one statement with `?` placeholders; without
them `run` may execute several statements and returns a result for each.

A statement result is:

```js
{
  kind: 'selected',            // 'created' | 'inserted' | 'selected' | 'began' | ...
  name: null,                  // table / view / database name, when the kind has one
  columns: ['id', 'name'],     // SELECT only, otherwise []
  columnTypes: ['integer', 'varchar'],
  rowCount: 2,
  affected: 0,                 // rows changed by INSERT / UPDATE / DELETE
  rows: [{ id: 1, name: 'Ada' }], // SELECT only
}
```

### `Prepared`

| Method | Result |
| --- | --- |
| `parameterCount` | number of `?` placeholders |
| `run(params?)` | statement metadata, no rows |
| `all(params?)` | every row |
| `iterate(params?)` | a synchronous iterator that decodes one row per `next()` |
| `close()` | free the prepared statement |

`iterate` keeps the native result alive and streams it: the 100k-row conformance
case never holds the whole result in JavaScript.

### Transactions

The engine is in autocommit mode until `BEGIN`. `begin()` / `commit()` /
`rollback()` are explicit, so a script that never calls them never holds a
transaction open. `commit()` and `rollback()` without an open transaction throw
an `execution` `PinkdbError`.

### Errors

Every failure throws `PinkdbError` with a stable `class` property:

| Class | Meaning |
| --- | --- |
| `parse` | SQL could not be parsed |
| `catalog` | unknown table, view, column or catalog object |
| `type` | type mismatch or unsupported cast |
| `parameter` | wrong number of parameters, or a parameter whose type cannot be inferred |
| `execution` | runtime failure (division by zero, `COMMIT` without a transaction, …) |
| `constraint` | a `NOT NULL` constraint rejected the row |
| `io` | file or network I/O failure |
| `csv` | CSV import failure |
| `invalid_argument` | bad argument from the host language (closed database, unsupported parameter type) |
| `internal` | an engine invariant was violated; report it as a bug |

```js
const { PinkdbError } = require('@pinkdb/pinkdb');
try {
  db.run('SELEC 1');
} catch (error) {
  error instanceof PinkdbError; // true
  error.class; // 'parse'
}
```

### Value mapping

| pinkdb type | JavaScript | Binding a parameter |
| --- | --- | --- |
| `NULL` | `null` | `null` / `undefined` |
| `BOOLEAN` | `boolean` | `boolean` |
| `INTEGER` | `number` | integral `number` within 32 bits |
| `BIGINT` | `BigInt` | `BigInt`, or an integral `number` outside 32 bits |
| `DOUBLE` | `number` (`NaN`, `Infinity` preserved) | non-integral `number` |
| `VARCHAR` | `string` | `string` |
| `DATE` | `string` `YYYY-MM-DD` | `string`, coerced to DATE by the target column or `CAST` |
| `TIMESTAMP` | `string` `YYYY-MM-DD HH:MM:SS.ffffff` (UTC) | `string`, likewise |
| `DECIMAL` | `string` with the column scale | not bindable in ABI v1: bind text and `CAST(? AS DECIMAL(p,s))` |
| `INTERVAL` | `string`, e.g. `1 day 02:00:00` | not bindable in ABI v1: bind text and `CAST(? AS INTERVAL)` |

`BIGINT` is never narrowed to a `number`: values that do not fit in a JS
`number` keep their exact value as a `BigInt`.

### ABI version

The package targets C ABI v1. `require('@pinkdb/pinkdb')` checks
`pinkdb_abi_version()` at load time and throws if the addon speaks a different
ABI; `abiVersion()` exposes the value for diagnostics.

## Conformance

`npm test` runs the shared corpus in [`bindings/conformance`](../conformance)
(the same cases the Python and Go bindings run) plus binding-level tests for
file databases, transactions, error classes and close semantics. The Python
binding's runner is the reference for the value encoding; `bindings/conformance/README.md`
documents the corpus format and how to regenerate expected values with the CLI.
