# pinkdb Go binding (`bindings/go`)

A thin [cgo](https://pkg.go.dev/cmd/cgo) wrapper over pinkdb's C ABI v1
(`crates/pinkdb-ffi`, header `crates/pinkdb-ffi/include/pinkdb.h`). No
third-party dependencies — just the Go standard library.

## Prerequisite

Build the native library once from the repo root:

```sh
cargo build --release -p pinkdb-ffi
```

This writes `target/release/libpinkdb_ffi.dylib` (or `.so` on Linux) plus the
static archive.

The module is self-contained: the C ABI declarations it needs are carried in
`pinkdb.go` (not included from `crates/pinkdb-ffi/include/pinkdb.h`), so
`go get github.com/openlearnia/pinkdb/bindings/go` works, and `abi_test.go`
checks those declarations against the real header when a checkout is
present. Only the library itself has to be found at link time:

```sh
# inside a pinkdb checkout: the default cgo flag points at ../../target/release
cargo build --release -p pinkdb-ffi && go test ./...

# anywhere else: point cgo at the directory holding libpinkdb_ffi
export PINKDB_LIB_DIR=/opt/pinkdb/lib
export CGO_LDFLAGS="-L$PINKDB_LIB_DIR -Wl,-rpath,$PINKDB_LIB_DIR"
```

The rpath keeps the runtime lookup independent of `LD_LIBRARY_PATH` /
`DYLD_LIBRARY_PATH`. A static link is possible by pointing `CGO_LDFLAGS` at
`libpinkdb_ffi.a`, but the binding is tested against the shared library.

## Quick start

```go
package main

import (
	"fmt"

	pinkdb "github.com/openlearnia/pinkdb/bindings/go"
)

func main() {
	db, err := pinkdb.OpenMemory() // or pinkdb.Open("data.pink")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	results, err := db.Execute(
		"CREATE TABLE t (x INTEGER, s VARCHAR);" +
			"INSERT INTO t VALUES (1, 'héllo'), (NULL, NULL);" +
			"SELECT x, s FROM t ORDER BY x;")
	if err != nil {
		panic(err)
	}
	fmt.Println(results[2].Kind, results[2].Rows) // selected [[1 héllo] [<nil> <nil>]]
}
```

## API

| Function | Description |
|---|---|
| `Open(path string) (*DB, error)` | Open a durable database (checkpoint + WAL) at `path`. |
| `OpenMemory() (*DB, error)` | Open a fresh, empty in-memory database. |
| `(*DB).Execute(sql string) ([]Result, error)` | Run one or more `;`-separated statements; one `Result` per statement. |
| `(*DB).SetAllowLoadExtension(allow bool) error` | Allow or refuse the SQL `LOAD EXTENSION '<path>'` statement (off by default). |
| `(*DB).Prepare(sql string) (*Stmt, error)` | Parse exactly one statement with `?` placeholders into a reusable `Stmt`. |
| `(*Stmt).Execute(args ...any) (Result, error)` | Bind one value per `?` and run the statement once. |
| `(*Stmt).ParameterCount() int` | Number of `?` placeholders. |
| `(*Stmt).SQL() string` | The original statement text. |
| `(*Stmt).Close()` | Free the native prepared statement. Idempotent. |
| `(*DB).Close()` | Free the native handle. Idempotent; a second call is a no-op. |
| `(*DB).ABI() uint32` | ABI version reported by the linked library, `major<<16 \| minor` (split it with `ABIMajorMinor`); stays callable after `Close`. |

Both open functions check `pinkdb_abi_version()` first: a library whose ABI is not
major 1 at minor 4 or later fails with an error naming both versions. `Open` rejects an empty path
or one containing a NUL byte before any native call; a path with invalid
UTF-8 fails at the native boundary with `path is not valid UTF-8`. SQL that
is not valid UTF-8 fails with `SQL is not valid UTF-8` and leaves the handle
usable.

> **Security.** The SQL statement `LOAD EXTENSION '<path>'` makes the host
> process load and run a native library from a path the SQL text names. It is
> **disabled by default**: call `(*DB).SetAllowLoadExtension(true)` to enable
> it for a handle whose SQL comes from a trusted source. Never enable it for
> a database whose SQL is influenced by untrusted input.

`Result` mirrors one statement's outcome:

- `Kind`: `selected`, `created`, `inserted`, `updated`, `deleted`, `began`,
  `committed`, `rolledback`, `created_view`, `dropped_table`, `dropped_view`,
  `created_database`, `dropped_database`, `used`, `loaded_extension`,
  `checkpointed`, `altered_table`, `created_user`, `altered_user`,
  `set_variable`, `created_schema`, `dropped_schema`,
  `dropped_user`, `created_role`, `dropped_role`, `granted`, `revoked`;
- `Columns` / `ColumnTypes` (SELECT only; types `boolean`, `integer`,
  `bigint`, `double`, `varchar`, `date`, `timestamp`, `decimal`, `interval`),
  `Rows`;
- `Affected` (INSERT/UPDATE/DELETE row count), `Name` (view/table/database/`USE`
  name, the extension name for `LOAD EXTENSION`, or the user/role name /
  grant description for RBAC statements; empty string when absent).

## Prepared statements

`DB.Prepare` parses exactly one SQL statement with `?` placeholders into a
reusable `Stmt` (a multi-statement string is refused; zero placeholders is
allowed). Types are resolved by the native bind step against the current
catalog on every execution, so one statement rebinds values and even sees
catalog changes between runs:

```go
db, _ := pinkdb.OpenMemory()
defer db.Close()
db.Execute("CREATE TABLE t (i INTEGER, s VARCHAR);")

insert, err := db.Prepare("INSERT INTO t VALUES (?, ?)")
if err != nil {
	panic(err)
}
defer insert.Close()
insert.Execute(1, "héllo")
insert.Execute(2, nil) // nil binds as NULL in a typed slot

selectByI, _ := db.Prepare("SELECT s FROM t WHERE i = ?")
defer selectByI.Close()
result, err := selectByI.Execute(2)
if err != nil {
	panic(err)
}
fmt.Println(result.Rows) // [[<nil>]]
```

Parameters bind by Go type:

| Go value | pinkdb type |
|---|---|
| `nil` | `NULL` |
| `bool` | `BOOLEAN` |
| any integer kind that fits int32 | `INTEGER` |
| any integer kind within int64 | `BIGINT` |
| `float32` / `float64` | `DOUBLE` |
| `string` | `VARCHAR` (UTF-8; embedded NULs preserved) |
| `time.Time` | `TIMESTAMP` (UTC, microsecond precision) |
| `pinkdb.Date` | `DATE` (the instant's UTC calendar day) |

`[]byte` is refused: ABI v1 has no BLOB type. A `uint64` above `MaxInt64`,
an unsupported Go type, and a `Date` outside int32 days are errors.
`pinkdb.Date` is `type Date time.Time`; a plain `time.Time` binds as
`TIMESTAMP`, and the engine's `TIMESTAMP -> DATE` cast covers `DATE` columns,
so use `Date` for the rare prepared `DATE` value with no column context.

A wrong argument count, an unconvertible value (e.g. `"abc"` for
`CAST(? AS INTEGER)`), and an untypable `NULL` (no context, no `CAST`)
return the native error string and leave both the statement and the database
usable. After `Stmt.Close`, `Execute` returns `ErrStmtClosed`; after
`DB.Close`, `Prepare` and `Stmt.Execute` return `ErrClosed`. `Stmt.Close`
remains safe after the DB is closed (it frees the prepared statement, not the
handle). A prepared statement resolves the catalog snapshot of its handle at
bind time: on a shared durable file, run a plain statement first to refresh a
handle before preparing against another handle's new DDL.

## Value mapping

| pinkdb type | Go value | Notes |
|---|---|---|
| `NULL` | `nil` | For any column type. |
| `BOOLEAN` | `bool` | |
| `INTEGER` | `int64` | 32-bit range, widened. |
| `BIGINT` | `int64` | |
| `DOUBLE` | `float64` | Bit-exact: `-0.0`, `NaN` and infinities survive. |
| `VARCHAR` | `string` | Decoded from `text_ptr` + `text_len`, so embedded NUL bytes and empty strings are preserved. |
| `DATE` | `time.Time` | UTC midnight; days since the Unix epoch. |
| `TIMESTAMP` | `time.Time` | UTC; microseconds since the Unix epoch; pre-epoch instants exact. |

The `ErrOutOfRange` guard on the DATE and TIMESTAMP conversions is
**defensive-only**: no current producer path can emit a payload outside
`time.Time`'s range, so no SQL statement can currently trigger it. Should a
DATE day count or TIMESTAMP microsecond count that overflows `time.Time`'s
internal `int64` seconds representation ever reach the binding, the
conversion returns `ErrOutOfRange` instead of a wrong calendar value. Every
`int64` microsecond timestamp produced by the engine converts exactly.
### Errors

Native failures come back as `*pinkdb.Error`, whose `Error()` is the message
from `pinkdb_last_error` and whose `Class` is the stable class from
`pinkdb_last_error_class`:

| `Class` | Meaning | Typical Go check |
|---|---|---|
| `parse` | SQL does not parse | `ProgrammingError`-style: fix the SQL |
| `catalog` | unknown table/column/object | |
| `type` | type mismatch in an expression | |
| `parameter` | wrong count, or a `NULL` with no inferable type | |
| `execution` | runtime failure (division by zero, `COMMIT` with no transaction) | |
| `constraint` | a `NOT NULL` constraint rejected a row | |
| `io` / `csv` | storage or import failure | |
| `invalid_argument` | the binding or caller passed something the ABI rejects | |
| `internal` | engine invariant violated; report it as a bug | |

```go
var pinkdbErr *pinkdb.Error
if errors.As(err, &pinkdbErr) && pinkdbErr.Class == "parse" {
	// ...
}
```

`ErrClosed`, `ErrStmtClosed` and `ErrOutOfRange` are separate sentinel errors
for the wrapper's own state; every error from the native library is a
`*pinkdb.Error`. A failed statement leaves the handle usable, and the message
is copied before the next native call on the thread replaces it.

## Limitations

Results are materialized: `Execute` and `Stmt.Execute` copy every row into
Go slices before returning, so a very large SELECT costs memory proportional
to its result. There is no row iterator yet (the Node and Python bindings
have one); use `LIMIT` for large tables until that follow-up lands.

ABI v1 returns column names and object/session names (`Columns`, `Name`) as
NUL-terminated C strings, so an **identifier containing a NUL byte is
truncated at the first NUL** in those fields. The engine keeps the full
identifier internally — subsequent statements using the full name still
match the object — but the name reported back to Go is cut short. An
engine-level fix is planned; until then, avoid NUL bytes in identifiers.

## Thread safety

The C ABI's handles are **single-threaded**. `DB` serializes every native call
behind a `sync.Mutex`, so one `*DB` may be shared between goroutines and each
`Execute` is atomic with respect to the others. A `*Stmt` is goroutine-safe
too: its native calls are serialized by the same per-`DB` mutex. Native
errors are thread-local, so the wrapper pins only the native call itself and
the `pinkdb_last_error` read to one OS thread (`runtime.LockOSThread`);
result materialization happens after the thread is released.

`Close` frees the native handle and is idempotent: a second `Close` is a
no-op (there is no handle left to free), and `ABI()` stays callable because
it is library-wide. `Execute`, `Prepare` and `Stmt.Execute` report a closed
database: they fail fast with `ErrClosed` instead of touching freed memory,
and `Stmt.Execute` fails with `ErrStmtClosed` after `Stmt.Close`.

## Testing

```sh
cd bindings/go
gofmt -l .        # must print nothing
go vet ./...
go test ./...
go test -race ./...
```

`conformance_test.go` runs the shared corpus in
[`bindings/conformance`](../conformance) — the same cases the Python and Node
bindings run, including typed parameter round trips, stable error classes,
transactions, multiple result sets and the 100k-row case. It reads
`../conformance/corpus.json` and skips (rather than fails) when the module is
built outside a checkout; set `PINKDB_CONFORMANCE` to run it from a copy.
`abi_test.go` checks the package's ABI declarations against the real
`pinkdb.h` and skips the same way.

The remaining tests cover all value types (NULLs, empty/multibyte/embedded-NUL strings,
`-0.0`, `NaN`/`±Inf`, i32/i64 extremes), DATE/TIMESTAMP boundaries including
pre-epoch values and the out-of-range mapping, all statement kinds with
`Affected`/`Name`, exact native error messages plus a still-usable handle,
invalid UTF-8 SQL and paths, empty/comment-only input, a durable round-trip
through `t.TempDir()`, idempotent close plus use-after-close errors, 8
goroutines hammering one `DB`, a 1000-iteration `Execute` loop, an
`Execute`-vs-`Close` race hammer (also under `-race`), per-goroutine
thread-local error isolation, and scale/leak pins: a 20k-row SELECT, a 20k
statement `Execute`, a multi-MB VARCHAR round-trip, a bounded peak-RSS and
Go-heap check over 20k success+error loops, and goroutine-count stability
over 8k concurrent executes.

The prepared-statement wrapper has its own coverage: prepare metadata and
prepare-time errors, per-type parameter round trips (including integer
widths, `-0.0`, embedded NULs, pre-epoch timestamps and `pinkdb.Date`), NULL
handling, repeated execution with different values, exact arity/type error
messages, unsupported-type rejection, close/use-after-close semantics,
concurrent reuse of one `Stmt` from 8 goroutines, prepared writes through
the WAL across reopen, RBAC result kinds (17-23), an injection-safety check,
and a bounded-RSS pin for the prepared success+error loop.

## Linking notes

- The cgo directives are:

  ```
  #cgo CFLAGS: -I${SRCDIR}/../../crates/pinkdb-ffi/include
  #cgo LDFLAGS: -L${SRCDIR}/../../target/release -Wl,-rpath,${SRCDIR}/../../target/release -lpinkdb_ffi
  ```

  `${SRCDIR}` is this directory, so both flags point at the repo checkout. The
  rpath lets test/demo binaries find the release dylib without
  `DYLD_LIBRARY_PATH`/`LD_LIBRARY_PATH`.
- Rebuild `cargo build --release -p pinkdb-ffi` after changing the Rust side;
  Go does not track the dylib as a build input.
