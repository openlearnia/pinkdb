# pinkdb Python binding

A thin, dependency-free Python wrapper over the pinkdb C ABI
(`crates/pinkdb-ffi/include/pinkdb.h`, ABI v1), implemented with the standard
library `ctypes` module only. The roadmap originally called for `cffi`; this
package deliberately uses `ctypes` instead so it installs with zero
dependencies and no compiler/build step (same thin-wrapper model — every
exported C function is bound directly).

## Build the C ABI first

```sh
cargo build --release -p pinkdb-ffi
```

The package looks for the shared library in this order:

1. `PINKDB_FFI_LIB` — full path to `libpinkdb_ffi.dylib` / `libpinkdb_ffi.so`;
2. `<repo>/target/release/libpinkdb_ffi.dylib` (or `.so`), computed relative
   to this package's location.

If neither exists, importing `pinkdb` raises a `pinkdb.Error` telling you to
run the build above (or to set `PINKDB_FFI_LIB`).

## Usage

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
a list of `Result` objects, one per statement:

| Attribute | Contents |
|---|---|
| `kind` | `selected`, `created`, `inserted`, `updated`, `deleted`, `began`, `committed`, `rolledback`, `created_view`, `dropped_table`, `dropped_view`, `created_database`, `dropped_database`, `used` |
| `columns` / `column_types` | SELECT column names and types (`boolean`, `integer`, `bigint`, `double`, `varchar`, `date`, `timestamp`) |
| `rows` | SELECT rows as Python values (see mapping below) |
| `affected` | row count for INSERT/UPDATE/DELETE, else 0 |
| `name` | object/session name for view/table/database/`USE` results, else `None` |

Value mapping: `NULL` → `None`, `BOOLEAN` → `bool`, `INTEGER`/`BIGINT` →
`int`, `DOUBLE` → `float`, `VARCHAR` → `str`, `DATE` → `datetime.date`,
`TIMESTAMP` → naive UTC `datetime.datetime` (microseconds since the Unix
epoch). SQL literals are always passed as strings.

Errors are raised as `pinkdb.Error` carrying the message from
`pinkdb_last_error`; a failed statement leaves the handle usable. Results and
handles are freed on the Rust side (`pinkdb_result_free` / `pinkdb_close`) as
soon as they are no longer needed, so loops do not leak. `Database.close()` is
idempotent, `__del__` is safe, and the class works as a context manager.

Like the C ABI, handles are **not thread-safe**: use one `Database` per
thread.

## Tests

```sh
python3 -m unittest discover -s bindings/python/tests -v

# and prove the env override:
PINKDB_FFI_LIB="$PWD/target/release/libpinkdb_ffi.dylib" \
    python3 -m unittest discover -s bindings/python/tests -v
```

No pip installs are required: the tests only need the build above and the
Python standard library.
