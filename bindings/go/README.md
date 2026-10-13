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
static archive. The `#cgo` directives in `pinkdb.go` resolve the ABI header
and the workspace `target/release` directory relative to this package, so a
repo checkout needs no environment variables at build or run time.

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
| `(*DB).Close()` | Free the native handle. Idempotent. |
| `(*DB).ABI() uint32` | ABI version reported by the linked library (1). |

Both open functions check `pinkdb_abi_version()` first: a library that is not
ABI 1 fails with an error naming both versions. `Open` rejects an empty path
or one containing a NUL byte before any native call.

`Result` mirrors one statement's outcome:

- `Kind`: `selected`, `created`, `inserted`, `updated`, `deleted`, `began`,
  `committed`, `rolledback`, `created_view`, `dropped_table`, `dropped_view`,
  `created_database`, `dropped_database`, `used`;
- `Columns` / `ColumnTypes` (SELECT only; types `boolean`, `integer`,
  `bigint`, `double`, `varchar`, `date`, `timestamp`), `Rows`;
- `Affected` (INSERT/UPDATE/DELETE row count), `Name` (view/table/database/`USE`
  name; empty string when absent).

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

DATE day counts that would overflow `time.Time`'s internal `int64` seconds
representation wrap `ErrOutOfRange` instead of returning a wrong calendar
value (the C ABI can carry them via `int64`; SQL expressions cannot reach that
range). Every `int64` microsecond timestamp is representable in `time.Time`
and converts exactly. Errors from the native library are returned verbatim
(the message from `pinkdb_last_error`) and leave the handle usable.

## Thread safety

The C ABI's handles are **single-threaded**. `DB` serializes every native call
behind a `sync.Mutex`, so one `*DB` may be shared between goroutines and each
`Execute` is atomic with respect to the others. Native errors are
thread-local, so the wrapper pins each operation to one OS thread
(`runtime.LockOSThread`) while it runs the call and reads
`pinkdb_last_error`.

`Close` is idempotent; after it, `Execute` fails fast with `ErrClosed` instead
of touching freed memory (`ABI()` stays callable because it is library-wide).

## Testing

```sh
cd bindings/go
gofmt -l .        # must print nothing
go vet ./...
go test ./...
```

The tests cover all value types (NULLs, empty/multibyte/embedded-NUL strings,
`-0.0`, `NaN`/`±Inf`, i32/i64 extremes), DATE/TIMESTAMP boundaries including
pre-epoch values and the out-of-range mapping, all statement kinds with
`Affected`/`Name`, errors with a still-usable handle, a durable round-trip
through `t.TempDir()`, idempotent close plus use-after-close errors, 8
goroutines hammering one `DB`, and a 1000-iteration `Execute` loop.

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
