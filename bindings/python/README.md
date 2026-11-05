# pinkdb Python binding

A dependency-free Python wrapper over the pinkdb C ABI
(`crates/pinkdb-ffi/include/pinkdb.h`, ABI 1.4), implemented with the standard
library `ctypes` module only. It has two layers:

- a **PEP 249 (DB-API 2.0)** module: `connect()`, `Connection`, `Cursor`,
  `paramstyle = "qmark"`, the exception hierarchy and the type objects;
- the **native wrapper** below it: `Database.execute`, `prepare()`/`Statement`
  and materialized `Result`s, still exported unchanged.

A wheel bundles `libpinkdb_ffi` inside the package, so `pip install` needs no
Rust toolchain and no separately built library.

## Build the C ABI first

```sh
cargo build --release -p pinkdb-ffi
```

The package looks for the shared library in this order:

1. `PINKDB_FFI_LIB` — full path to `libpinkdb_ffi.dylib` /
   `libpinkdb_ffi.so`, or `libpinkdb_ffi.dll` on Windows;
2. next to the package itself — where a wheel puts its bundled copy;
3. `<repo>/target/release/libpinkdb_ffi.dylib` (or `.so` / `.dll`),
   computed relative to this package's location.

If neither exists, importing `pinkdb` raises a `pinkdb.Error` telling you to
run the build above (or to set `PINKDB_FFI_LIB`). Importing also fails with
`pinkdb.Error` when the file it loads is not a pinkdb-ffi library (it lacks
the `pinkdb_*` symbols) or reports an ABI other than major 1 at minor 4 or
later. `pinkdb.abi_version()` returns the encoded value (`major << 16 |
minor`) and `pinkdb.abi_version_info()` the `(major, minor)` tuple.

## Build a wheel

```sh
cd bindings/python
python3 -m pip wheel . --no-deps -w dist
python3 -m pip install dist/pinkdb-*.whl
```

`setup.py` runs `cargo build --release -p pinkdb-ffi` and copies the shared
library into the `pinkdb` package, so the wheel is self-contained and
platform-specific (not a pure-Python wheel). Set `PINKDB_FFI_LIB` to an
existing library to skip the cargo step. Build wheels on the platform and
Python version you target; there is no cross-compilation.

## DB-API 2.0 usage

```python
import pinkdb

connection = pinkdb.connect("data.pink")     # or pinkdb.connect(":memory:")
try:
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE t (x INTEGER, s VARCHAR)")
    cursor.executemany("INSERT INTO t VALUES (?, ?)", [(1, "héllo"), (2, None)])
    cursor.execute("SELECT x, s FROM t ORDER BY x")
    cursor.description   # [('x', 2, None, None, None, None, None), ...]
    cursor.fetchone()    # (1, 'héllo')
    cursor.fetchall()    # [(2, None)]
    connection.commit()
finally:
    connection.close()
```

`pinkdb.apilevel == "2.0"`, `pinkdb.paramstyle == "qmark"`,
`pinkdb.threadsafety == 2` (a module and a connection may be shared between
threads, a cursor may not).

`Cursor` supports `execute`, `executemany`, `fetchone`, `fetchmany`,
`fetchall`, iteration, `description`, `rowcount`, `arraysize`, `close`, and
the no-op `setinputsizes`/`setoutputsizes`. `execute` prepares the statement
first, so a multi-statement string is refused before anything runs. Rows are
tuples in `description` order, fetched one at a time from the native result:
the 100k-row conformance case streams without materializing the rows in
Python. While a cursor is still fetching a streaming result, `rowcount` is
`-1` and the connection stays busy: another `execute` raises until the cursor
is exhausted or closed.

**Autocommit deviation.** The engine is in autocommit mode until `BEGIN`
(and refuses some catalog DDL inside a transaction), so `execute()` does not
open an implicit transaction the way PEP 249 expects. `commit()` and
`rollback()` are explicit and are no-ops when no transaction is open. A
script that never calls `BEGIN` never holds a transaction.

### Exceptions

All DB-API exceptions are also `pinkdb.Error`, and each carries
`pinkdb_class` — the stable class from the C ABI:

| Exception | `pinkdb_class` | When |
|---|---|---|
| `InterfaceError` | `invalid_argument` | closed connection or cursor, unsupported parameter |
| `ProgrammingError` | `parse`, `catalog`, `type`, `parameter` | bad SQL, unknown object, wrong parameter count/type |
| `IntegrityError` | `constraint` | a `NOT NULL` constraint rejected the row |
| `OperationalError` | `execution`, `io` | runtime or I/O failure (`COMMIT` with no transaction, division by zero) |
| `DataError` | `csv` | CSV import failure |
| `InternalError` | `internal` | engine invariant violated; report it as a bug |
| `NotSupportedError` | — | `callproc()`: pinkdb has no stored procedures |

### Type objects

`description` carries the integer C ABI type tag, comparable to the standard
type objects: `STRING` (`VARCHAR`), `NUMBER` (`INTEGER`, `BIGINT`, `DOUBLE`,
`DECIMAL`), `DATETIME` (`DATE`, `TIMESTAMP`), `ROWID` and `BINARY` (both
empty: ABI v1 has no row ids or BLOBs). `INTERVAL` has a tag but no PEP 249
group.

## Native usage

```python
import pinkdb

with pinkdb.connect_memory() as db:            # or pinkdb.connect("data.pink")
    db.execute("CREATE TABLE t (x INTEGER, s VARCHAR);")
    db.execute("INSERT INTO t VALUES (1, 'héllo'), (NULL, NULL);")

    result = db.execute("SELECT x, s FROM t ORDER BY x;")[0]
    print(result.kind)          # 'selected'
    print(result.columns)       # ['x', 's']
    print(result.column_types)  # ['integer', 'varchar']
    print(result.rows)          # [[1, 'héllo'], [None, None]]
```

`Database.execute(sql)` runs one or more `;`-separated statements and returns
a list of `Result` objects, one per statement (this is the materializing
layer; `Connection` in the DB-API section above is the same class plus
`cursor()`/`commit()`/`rollback()`):

> **Security.** The SQL statement `LOAD EXTENSION '<path>'` makes the host
> process load and run a native library from a path the SQL text names. It is
> **disabled by default**: call `Database.allow_load_extension(True)` to
> enable it for a handle whose SQL comes from a trusted source. Never enable
> it for a database whose SQL is influenced by untrusted input.

| Attribute | Contents |
|---|---|
| `kind` | `selected`, `created`, `inserted`, `updated`, `deleted`, `began`, `committed`, `rolledback`, `created_view`, `dropped_table`, `dropped_view`, `created_database`, `dropped_database`, `used`, `loaded_extension`, `checkpointed`, `altered_table`, `created_user`, `altered_user`, `dropped_user`, `created_role`, `dropped_role`, `granted`, `revoked`, `set_variable`, `created_schema`, `dropped_schema` |
| `columns` / `column_types` | SELECT column names and types (`boolean`, `integer`, `bigint`, `double`, `varchar`, `date`, `timestamp`, `decimal`, `interval`) |
| `rows` | SELECT rows as Python values (see mapping below) |
| `affected` | row count for INSERT/UPDATE/DELETE, else 0 |
| `name` | object/session/extension name for view/table/database/`USE`/`LOAD EXTENSION` results, else `None` |

Value mapping: `NULL` → `None`, `BOOLEAN` → `bool`, `INTEGER`/`BIGINT` →
`int`, `DOUBLE` → `float`, `VARCHAR` → `str`, `DATE` → `datetime.date`,
`TIMESTAMP` → naive UTC `datetime.datetime` (microseconds since the Unix
epoch). SQL literals are always passed as strings.

## Prepared statements

`Database.prepare(sql)` parses exactly one SQL statement with `?`
placeholders into a reusable `Statement` (multi-statement strings are
refused; a statement with no placeholders is allowed). Types are resolved at
bind time against the current catalog, so one statement can be executed
repeatedly with different values — and sees catalog changes between
executions:

```python
with pinkdb.connect_memory() as db:
    db.execute("CREATE TABLE t (i INTEGER, s VARCHAR);")
    insert = db.prepare("INSERT INTO t VALUES (?, ?);")
    insert.execute(1, "héllo")
    insert.execute(2, None)          # NULL binds fine in a typed slot
    select = db.prepare("SELECT s FROM t WHERE i = ?;")
    print(select.execute(1).rows)    # [['héllo']]
    print(select.execute(2).rows)    # [[None]]
    insert.close()                   # idempotent; also a context manager
    select.close()
```

`Statement.execute(*params)` binds one value per `?` in textual order and
returns a single `Result` (a prepared statement is exactly one statement).
Parameter types map as follows:

| Python value | pinkdb type |
|---|---|
| `None` | `NULL` |
| `bool` | `BOOLEAN` |
| `int` (fits int32) | `INTEGER` |
| `int` (fits int64) | `BIGINT` |
| `float` | `DOUBLE` |
| `str` | `VARCHAR` (UTF-8; embedded NULs preserved) |
| `datetime.date` | `DATE` (days since the Unix epoch) |
| `datetime.datetime` | `TIMESTAMP` (microseconds since the Unix epoch; naive values are UTC, aware values are converted to UTC) |

`bytes`/`bytearray`/`memoryview` are refused (`TypeError`): ABI v1 has no
BLOB type. `decimal.Decimal` is refused too: ABI v1 cannot bind
DECIMAL/INTERVAL parameters — pass the canonical text and `CAST` it in SQL
(that path works, because the bind converts `VARCHAR`). An integer outside
int64 and a `str` with a lone surrogate raise `pinkdb.Error`.

A wrong parameter count, an unconvertible value (e.g. `"abc"` into
`CAST(? AS INTEGER)`), and an untypable `NULL` (no column/operator context
and no `CAST`) raise `pinkdb.Error` with the native message, and leave both
the statement and the database usable. `Statement.parameter_count` reports
the number of `?` placeholders and `Statement.sql` the original text;
`Statement.close()` frees the native statement (idempotent, safe after
`Database.close`), and executing after either close raises `pinkdb.Error`.
A `Statement` cannot be copied (`copy.copy` / `copy.deepcopy` raise
`TypeError`).

Errors are raised as `pinkdb.Error` carrying the message from
`pinkdb_last_error`; a failed statement leaves the handle usable. Values
outside Python's `datetime` range (e.g. `DATE '0000-01-01'`, or a date
expression whose day count exceeds `datetime.date`'s range) also raise
`pinkdb.Error`, naming the raw native value. Strings that are not valid
UTF-8 (lone surrogates in SQL or paths) are rejected as `pinkdb.Error`
before the native call, and `connect()` rejects an empty path or one
containing a NUL byte with `ValueError` (a NUL would otherwise silently
truncate the path at the C boundary). Results and handles are freed on the
Rust side (`pinkdb_result_free` / `pinkdb_close`) as soon as they are no
longer needed, so loops do not leak. `Database.close()` is idempotent,
`__del__` is safe, and the class works as a context manager. Handles cannot
be copied: `copy.copy` / `copy.deepcopy` raise `TypeError`, because both
objects would own the same native handle (pickling is not supported and
fails safely).

Like the C ABI, handles are **not thread-safe**: use one `Database` per
thread.

## Tests

```sh
python3 -m unittest discover -s bindings/python/tests -v

# and prove the env override:
PINKDB_FFI_LIB="$PWD/target/release/libpinkdb_ffi.dylib" \
    python3 -m unittest discover -s bindings/python/tests -v
```

`tests/test_conformance.py` runs the shared corpus in
[`bindings/conformance`](../conformance) — the same cases the Node and Go
bindings run — through the DB-API layer. To check an installed wheel, run it
from outside the checkout with the wheel's interpreter:

```sh
cd /tmp && python -m unittest discover -s <repo>/bindings/python/tests \
    -p 'test_conformance.py' -v
```

No pip installs are required for the source tests: they only need the build
above and the Python standard library. The suite covers the prepared-statement wrapper
too: per-type parameter round trips, repeated execution with different
values, NULLs, wrong arity/type errors with the exact native messages,
prepare-time errors, close/use-after-close and copy semantics, a prepared
statement per RBAC result kind, durable WAL replay and cross-handle use,
and an instrumentation check that every native prepared statement is freed
exactly once.
