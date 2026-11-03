# pinkdb Go extension SDK

Everything needed to build a loadable extension for pinkdb in Go against the
extension ABI v1 (minor 4). An extension is a shared library built with cgo
(`go build -buildmode=c-shared`) that the host `dlopen`s and drives through the
C ABI in [`include/pinkdb_extension.h`](include/pinkdb_extension.h). No pinkdb
libraries are linked: the host provides its services through the
`PinkdbHostApi` struct at load time.

```
ext-sdk/go/
├── include/pinkdb_extension.h   # vendored ABI header (byte-identical to the loader crate's copy)
├── pinkdb/                      # the SDK package: negotiation, registration, values, ownership
├── template/                    # copy this directory to start an extension (self-contained)
│   ├── main.go                  # skeleton: exports, one VARCHAR scalar, one volatile scalar
│   ├── go.mod                   # standalone module
│   └── Makefile                 # standalone build
├── examples/
│   └── string_ext.go            # ext_go_repeat(VARCHAR, INTEGER), ext_go_len(VARCHAR)
├── conformance/
│   └── main.go                  # the ABI conformance fixture (ext-sdk/conformance/README.md)
├── go.mod                       # module for the example, fixture and package
├── Makefile                     # builds the example and the fixture into build/
├── smoke.sh                     # end-to-end test against the release CLI
└── README.md
```

The header is the complete contract; this README is the guided tour, with the
Go-specific patterns and sharp edges the header cannot know about.

## Quick start

```sh
make -C ext-sdk/go                 # -> build/string_ext.dylib, build/conformance_ext.dylib
./ext-sdk/go/smoke.sh              # build + load + query, end to end

# or, manually, in a pinkdb REPL (release build):
cargo build --release -p pinkdb-cli
./target/release/pinkdb --allow-load-extension
```

```sql
LOAD EXTENSION '/abs/path/to/ext-sdk/go/build/string_ext.dylib';
-- Extension string_ext loaded.
SELECT ext_go_repeat('ab', 3), ext_go_len('héllo');
-- ababab | 6
```

`LOAD EXTENSION '<path>'` takes a string literal path (absolute paths are
strongly recommended; see [Security posture](#security-posture)). A successful
load prints `Extension <name> loaded.`; the declared name is pinned, so loading
the same extension twice is an error. Loaded functions resolve
case-insensitively like built-ins and cannot shadow one.

## Writing an extension

There are two starting points and they are not exclusive:

- `ext-sdk/go/pinkdb` — the SDK package. It owns the negotiation, registration,
  value, alloc/free and panic plumbing, so an extension is callbacks plus a
  registration table. Start here.
- `template/` — a single self-contained file with its own C shims. Copy the
  directory when you want zero imports beyond the standard library (or want to
  see what the package does for you).

### The package

An extension built on `github.com/openlearnia/pinkdb/ext-sdk/go/pinkdb` keeps
the same three exports and adds one package-level `*API`:

```go
var api *pinkdb.API   // written in init, read-only afterwards

//export ext_go_len
func ext_go_len(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
    return C.int32_t(pinkdb.Guard(func() int32 {
        argv := pinkdb.Args(args, int(nArgs))
        if len(argv) < 1 {
            return 1
        }
        if argv[0].IsNull != 0 {
            return 0                       // `out` already is NULL
        }
        *pinkdb.Out(out) = pinkdb.Bigint(int64(argv[0].TextLen))
        return 0
    }))
}

//export pinkdb_extension_init
func pinkdb_extension_init(hostAPI, hostCtx unsafe.Pointer) C.int32_t {
    return C.int32_t(pinkdb.Guard(func() int32 {
        a, code := pinkdb.Init(hostAPI, hostCtx)   // 1: wrong ABI, 2: host too old
        if code != 0 {
            return code
        }
        if c := a.Register("ext_go_len", []int32{pinkdb.TypeVarchar},
            pinkdb.TypeBigint, C.ext_go_len_fn(), nil); c != 0 {
            return 10                              // the host's rejection code
        }
        api = a
        return 0
    }))
}
```

Signatures use `unsafe.Pointer` where the ABI has a struct or a callback: two
cgo packages never share a generated C type, so the host's
`PinkdbHostApi *`, `pinkdb_ext_value *` and `PinkdbExtScalarFn` cross into the
package as opaque pointers. Inside, `pinkdb.Args`/`pinkdb.Out` turn them into
`[]pinkdb.Value`/`*pinkdb.Value` (a Go mirror of the C struct, size-checked at
compile time), and a static accessor in the extension hands each `//export`ed
callback over for registration:

```c
extern int32_t ext_go_len(void *user_data, void *args, size_t n_args, void *out);
static inline void *ext_go_len_fn(void) { return (void *)ext_go_len; }
```

What the package guarantees, so the extension does not have to:

- `Init` copies the host's `PinkdbHostApi` (the host documents it as valid for
  the init call only), records what the host negotiated, and refuses a host it
  cannot trust — `pinkdb.ErrABIVersion` (1) for a different ABI major version,
  `pinkdb.ErrStructSize` (2) for a host older than `PINKDB_EXT_API_V1_SIZE`.
- `Register` checks nothing the host can check better and returns the host's
  own rejection code; `RegisterVolatile` refuses (`ErrNoFlags`, 4) when the host
  has no `register_scalar_flags` and registers nothing.
- `SetText` is the only way to publish a VARCHAR result: it allocates through
  the host's `alloc`, never `C.malloc`, and an empty result becomes a
  zero-length non-NULL string with no allocation.
- `Guard` turns a panic into `ErrPanic` (3) so it cannot abort the host, and
  prints the panic value on stderr because the error code cannot carry it.
- `ExecuteStatement` is the serve-path call into the host (see
  [Serving](#serving-execute_statement-and-pinkdb_ext_serve-minor-2)).

### The template

`template/` is a standalone module with no imports of the SDK package: copy it
out of the tree and it still builds as long as `../include` (the ABI header) is
reachable. It writes the C shims itself — that is the price of
self-containment, and the file explains each one. It registers the same kind of
function as the package example plus one `PINKDB_EXT_FLAG_VOLATILE` function,
so it also demonstrates the negotiation that flag needs.

### The three exports

```go
//export pinkdb_extension_abi_version
func pinkdb_extension_abi_version() C.uint32_t  // must return PINKDB_EXT_ABI_VERSION

//export pinkdb_extension_name
func pinkdb_extension_name() *C.char            // NUL-terminated UTF-8, non-empty, ≤256 bytes

//export pinkdb_extension_init
func pinkdb_extension_init(api *C.PinkdbHostApi, hostCtx unsafe.Pointer) C.int32_t
```

cgo turns each `//export`ed function into a C-ABI symbol with exactly that
name, which is what the loader looks up. Two rules about `//export`:

- the comment must sit immediately above the function, with no blank line;
- a file that uses `//export` must keep its cgo preamble free of **non-static
  definitions** (declarations and `static` functions only) — the preamble is
  copied into two generated C files, and a non-static definition becomes a
  `duplicate symbol` link error.

A c-shared library is still `package main` with a `func main() {}`; the host
never calls it, but the toolchain requires it.

### Negotiation: `struct_size`, never `sizeof`

`PinkdbHostApi` has grown additively and will grow again, so an extension must
compare against the header's landmarks instead of its own idea of the struct
size:

| question | test |
|---|---|
| can I init at all? | `api->struct_size >= PINKDB_EXT_API_V1_SIZE` |
| is `register_scalar_flags` there? | `PINKDB_EXT_HOST_HAS_FLAGS(api)` |
| is `execute_statement` there? | `PINKDB_EXT_HOST_HAS_EXECUTE(api)` |

`sizeof(PinkdbHostApi)` built from a newer header is a *ceiling*, not a
requirement: rejecting a host that reports less fails every older-but-usable
host, and copying `sizeof` bytes out of it reads past the end of the host's
struct. Copy what the host reports (the package does exactly that) or copy
nothing and keep only the fields you negotiated.

## Values and type tags

`pinkdb.Value` mirrors `pinkdb_ext_value` field for field. The payload field
depends on the tag:

| tag | Go constant | payload |
|---|---|---|
| 0 NULL | `pinkdb.TypeNull` | none — only valid as a result, never an argument tag |
| 1 BOOLEAN | `TypeBoolean` | `Int64` 0/1 |
| 2 INTEGER | `TypeInteger` | `Int64` (i32 range) |
| 3 BIGINT | `TypeBigint` | `Int64` |
| 4 DOUBLE | `TypeDouble` | `Double` |
| 5 VARCHAR | `TypeVarchar` | `TextPtr` + `TextLen` (bytes, not a C string) |
| 6 DATE | `TypeDate` | `Int64`: days since 1970-01-01 |
| 7 TIMESTAMP | `TypeTimestamp` | `Int64`: microseconds since the epoch (UTC) |
| 9 INTERVAL | `TypeInterval` | `Int64`: micros, `Double`: packed months/days |

Tag 8 is the pinkdb C ABI's DECIMAL and is deliberately not part of extension
ABI v1. Builders and readers exist for every tag in both directions —
`pinkdb.Bool/Int/Bigint/Float/Date/Timestamp/Interval/Bytes` and
`Value.Bool/Int/Bigint/Float/Date/Timestamp/Interval/Bytes`.

INTERVAL is the one tag that does not fit the struct: ABI v1 cannot grow a
field, so a calendar interval's three DuckDB fields ride in the two scalars that
exist — micros in `Int64`, and `(uint32(months) << 32) | uint32(days)` in the
*bit pattern* of `Double`. The package's `Interval(months, days, micros)` and
`Value.Interval()` are the whole story: never read `Double` as a number for an
INTERVAL (a months/days pattern is usually a denormal, sometimes a NaN, and
always meaningless as a float). The C-side rule is the same: `memcpy` to
`uint64_t` / `std::bit_cast` / `f64::to_bits`, never the value.

## Registration: deterministic vs volatile

`Register` (and C's `register_scalar`) declares a **deterministic** function: a
pure function of its arguments. That is not a hint. The engine may WAL-log a
statement that calls it and re-execute that statement on recovery, so returning
different values for the same arguments on a durable store is a correctness bug
in the extension.

`RegisterVolatile` (C's `register_scalar_flags` with `PINKDB_EXT_FLAG_VOLATILE`)
declares the exception for a function that reads a clock, a random source or
other ambient state. The engine then *refuses to execute* that call in any
statement it WAL-logs (`INSERT`, `UPDATE`, `DELETE`, DDL, `CALL`, ...) with an
error naming the function, before the statement runs — replaying it later could
produce a different value than this session saw. Plain `SELECT`s may call it.
Unknown flag bits are rejected at registration: silently ignoring one would turn
a declared volatile function back into a deterministic one.

A host older than that addition has no `register_scalar_flags` field. An
extension that needs it must **fail init** there — `RegisterVolatile` returns
`pinkdb.ErrNoFlags` for exactly this reason — and never fall back to
`register_scalar`. The template shows the `PINKDB_EXT_HOST_HAS_FLAGS` check that
does the same thing by hand.

## Callbacks, NULLs, and panic containment

The callback receives `args` (exactly the registered arity, read-only) and a
writable `out` pre-filled with NULL. A NULL argument still arrives as a struct
with `is_null` set — the callback decides what NULL means, and the host always
calls the callback:

```go
if argv[0].IsNull != 0 {
    return 0            // out is already NULL; this is not an error
}
```

Return 0 for success; a non-zero return aborts the enclosing SQL statement with
`extension function <name>() failed with code <n>`. Never use a non-zero code
for an ordinary NULL result.

A Go panic must never cross the ABI boundary: it unwinds into the host's C
frames, which is undefined behavior, and in practice aborts the process. Both
the package (`pinkdb.Guard`) and the template (`defer recover()`) contain it and
return a code, printing the panic value to stderr. Contained is not repaired:
that call fails, the statement fails, the process lives.

`pinkdb_extension_init` and `pinkdb_ext_serve` are on the same boundary and get
the same treatment. Callbacks must also never call back into the engine — no
SQL from inside a callback — and must not be called by the host before init
returns (they are not).

## VARCHAR results: alloc/free ownership

A non-NULL VARCHAR result **must** be allocated with the host's `alloc` — never
`C.malloc`, never a Go buffer, never a stack or static buffer. The host copies
exactly `text_len` bytes and calls the host's `free` immediately after the
callback returns:

- a non-NULL `text_ptr` is **always** freed by the host — even when `is_null`
  is set or the callback returns non-zero, so never hand it Go memory or an
  interior pointer;
- the host never frees a NULL `text_ptr`; a zero-length result is
  `text_ptr == NULL, text_len == 0` (`SetText` produces it without allocating);
- VARCHAR **inputs** (`args[i].text_ptr`) are host-owned: never free them, and
  treat them as valid only for the duration of the call (copy to keep);
- a result over 64 MiB is rejected without reading or freeing the buffer.

`SetText` is the SDK's whole surface here: build the bytes in Go, hand them
over, and the host owns the copy. `FreeText` is for the opposite direction —
text the *host* allocated for you (`ExecuteStatement`) — because the host's
alloc/free are a pair; freeing anything else corrupts the host heap.

## Thread safety

Callbacks are invoked **concurrently** from the host's rayon worker threads: a
registered function may run on any thread, several at once, on threads the Go
runtime did not create. cgo attaches such foreign threads on entry, but nothing
else is guaranteed:

- do not assume init and callbacks run on the same thread;
- do not key state off goroutine or thread identity;
- guard every piece of shared mutable state with Go's own synchronization
  (`sync`, `sync/atomic`) — and see `conformance/main.go`, which counts
  callbacks that arrive on a foreign thread;
- the `*pinkdb.API` copy is the exception: it is written once during init,
  before any callback can run, and read-only afterwards.

`alloc`/`free` are thread-safe; `register_scalar` is init-only. The smoke
test's 5,000-row queries go through the executor's parallel filter/projection
paths, so the callbacks run on several rayon workers at once.

## Serving: `execute_statement` and `pinkdb_ext_serve` (minor 2)

ABI minor 2 added an optional export and one host field. Both are additive: an
extension that exports only the three required symbols is a functions-only
extension and works exactly as before.

```c
int32_t pinkdb_ext_serve(const PinkdbHostApi *api, void *host_ctx,
                         const char *transport, const char *options);
```

If present, the host calls it instead of treating init's registrations as the
whole lifetime, and **blocks** until the extension returns. The extension is the
server loop: `transport` is `"stdio"` or `"http:HOST:PORT"`, `options` is a
host-defined string (`""` when none), both NUL-terminated UTF-8 valid for the
call only. Return 0 when the client disconnected or the host asked you to stop
(SIGINT closes stdin, the HTTP loop sees its listener closed), non-zero on
failure. While a serve call runs the library is never unloaded and cannot be
loaded a second time; the host enforces both.

The API struct handed to serve is freshly built for that call and its
`execute_statement` field is wired to the live session:

```go
//export pinkdb_ext_serve
func pinkdb_ext_serve(hostAPI, hostCtx unsafe.Pointer, transport, options *C.char) C.int32_t {
    return C.int32_t(pinkdb.Guard(func() int32 {
        api, code := pinkdb.Init(hostAPI, hostCtx)   // serve hands a fresh struct
        if code != 0 {
            return code
        }
        code, text := api.ExecuteStatement([]byte("SELECT 1"))
        if code == 1 {
            // The statement was refused or failed; `text` is a JSON error object.
            fmt.Fprintf(os.Stderr, "statement failed: %s\n", text)
        }
        return 0                                     // the client is gone: clean exit
    }))
}
```

`ExecuteStatement` returns the host's code (0 = success with Markdown text,
1 = a JSON error object, 2 = no live session, i.e. the API came from init) and
a *Go copy* of the text; it frees the host buffer through the host's `free`
internally, so the caller owns an ordinary `[]byte`. Statement policy —
read-only enforcement, limits, role checks — is the host's, not the
extension's: asking is allowed, doing is decided there. Implementing a whole
client is out of scope for this SDK; the fixture pattern above is enough to
prove the surface works.

## The conformance fixture

`conformance/main.go` is the Go implementation of the shared ABI fixture
described in [`ext-sdk/conformance/README.md`](../conformance/README.md):
`conformance_ext` registers every value tag in both directions (`cf_echo`,
`cf_bool`, `cf_int`, `cf_bigint`, `cf_double`, `cf_date`, `cf_timestamp`,
`cf_interval`), the canonical `cf_all` summary, zero-length and host-allocated
results (`cf_empty`, `cf_fill`), the error path (`cf_fail` allocates its text
before returning code 1), a real panic (`cf_unwind`, contained by `Guard`), the
volatile flag (`cf_volatile`), and thread tracking (`cf_thread` plus the
`pinkdb_conformance_*` introspection symbols, which are not part of the ABI).

The loader's conformance tests build this library and assert the same contract
against the C, C++ and Rust fixtures, so it is the file to run when a change
touches the ABI: `make -C ext-sdk/go build/conformance_ext.dylib`, then load it
with `--allow-load-extension`.

## Security posture

Loading an extension means `dlopen`ing and running native code in the host
process, with the host's privileges. This SDK does not change that; the host's
rules do:

- **Opt-in, always.** `LOAD EXTENSION` is refused unless the CLI was started
  with `--allow-load-extension` (or `PINKDB_ALLOW_LOAD_EXTENSION=1`).
  `pinkdb-server` and embedded users make the same explicit choice; there is no
  "extensions are just on" mode.
- **Confinement.** `--extension-root DIR` restricts every load to `DIR`. A path
  outside it, a bare library name, a `..` component and a symlinked library are
  always refused — the path must name a regular file under the root.
- **Stable absolute paths.** The WAL replays the statements that need an
  extension, prefixed with `LOAD EXTENSION '<path>'`, so recovery re-loads by
  path. Keep extension files at stable absolute paths; a missing file at replay
  time is a hard recovery error naming the path (a checkpoint resets that
  coverage).
- **`--extension-pin PATH=SHA256` pins the bytes.** The flag (on the CLI and the
  server, repeatable) installs a SHA-256 allowlist: once at least one pin is
  given, a library that is not pinned is refused (`is not pinned`), a library
  whose hash differs is refused (`does not match its pinned SHA-256`), and so is
  one that changes between the hash and the load (`changed on disk while it was
  being checked`). It is the only content check a local load has, and
  `dlopen` takes a path rather than a descriptor, so the directory holding the
  library must still be one only the operator can write.
- **Local libraries are unsigned.** Ed25519 signing covers the published
  catalog and the artifacts `INSTALL`/`.install` fetch through it — it does not
  cover a library a user points `LOAD EXTENSION` at. Treat an extension path as
  code you are choosing to run.
- **Server privileges.** `pinkdb-server` loads extensions into the server
  process, so anyone who can send SQL can load a shared library with the
  server's privileges: only expose the server to principals you would give a
  shell to.
- **Process-level, not transactional.** Extensions are not stored in `.pink`
  snapshots and are not part of a transaction; `LOAD EXTENSION` is refused
  inside `BEGIN..COMMIT`.

### What this ABI cannot do

Extension ABI v1 is a **scalar-function** ABI: scalar callbacks, (minor 2)
the statement bridge above, and (minor 4, not wrapped by this package yet) the
`pinkdb_ext_command` entry point with its text sink and the `execute_internal`
JSON-row host field. Table functions, scans, copy sinks and any other
physical operator are the *linked* extension model — the `pinkdb-ext-loader`
registry whose implementations are compiled into the host. A loadable extension
cannot register them, and neither this package nor the template pretends
otherwise; a Go extension that needs one needs a host that links it.

## Sharp edges

The ABI header documents the contract; these are the Go-specific consequences,
verified against the real host (macOS, Go 1.26, cgo):

- **The Go runtime is linked into the host process.** `-buildmode=c-shared`
  embeds a complete Go runtime (GC, scheduler, `sysmon` threads, signal
  handling) in the `.dylib`. `dlopen` starts it; it never shuts down (the host
  keeps extensions mapped for the process lifetime). A trivial extension is
  ~1.5 MB, and the first `LOAD EXTENSION` of a Go-built library costs the
  runtime's startup (roughly half a second here) — C extensions are ~17–34 KB
  and load in milliseconds. Loading two Go-built extensions into one host also
  works (each carries its own runtime), but it doubles that footprint and the
  runtimes share the process's signals; prefer one Go extension per process
  when you can.
- **Callbacks come from foreign threads.** Rayon workers are not Go-created
  threads. cgo handles the entry, but thread-local assumptions break: keep
  state in package-level variables guarded by mutexes/atomics, never in
  per-goroutine or per-thread state. There is per-call cgo overhead (stack
  switch + runtime check); this is not a hot-loop primitive.
- **Go memory must never cross back to the host.** cgo's pointer rules forbid
  storing Go pointers in C memory, and the host frees your result buffer with
  its own `free`. Build the bytes in Go, let `SetText` copy them into a
  host-`alloc` buffer. Input bytes are borrowed for the call only.
- **No panics across the boundary.** A panic in an exported function is not an
  error return — it aborts the whole host process (`panic: ...`, exit 134 in
  our test). Recover in every `//export`ed function (`init` and `serve`
  included) or route through `pinkdb.Guard`.
- **`-buildmode=c-shared` symbol rules.** Requires `package main` + a `main`
  function (may be empty), `CGO_ENABLED=1`, and a C compiler. Only `//export`ed
  functions (plus Go runtime internals) get C symbols. `-o build/name.dylib`
  also writes a generated cgo header `build/name.h` next to the library; it is
  not needed to load the extension. `go.mod` must exist in the directory you
  build from.
- **The `//export` preamble restriction.** With `//export` present the
  preamble is emitted into two C translation units: only declarations and
  `static` definitions are allowed (a plain `int helper(void) {...}` fails the
  link with `duplicate symbol`). The shims are `static inline` for this reason
  (and to stay warning-free when unused).
- **cgo does not emit `const`.** The `extern` declaration of an exported
  callback must match cgo's generated declaration exactly (non-const pointer
  types) or compilation fails with `conflicting types`; the accessor's cast
  back to `PinkdbExtScalarFn` only adjusts `const`.
- **Two cgo packages do not share a C type.** That is why the SDK package takes
  `unsafe.Pointer` for the host API, the value structs and the callbacks, and
  why the extension's own preamble is where the `//export`ed symbols are
  declared.

## Building

The only include path needed is this SDK's `include/` (wired up through
`#cgo CFLAGS: -I${SRCDIR}/../include`). From a template copy:

```sh
make                                    # -> build/my_extension.dylib (.so on Linux)
CGO_ENABLED=1 go build -buildmode=c-shared -o build/my_extension.dylib .
```

The shared-library suffix is `.dylib` on macOS, `.so` on Linux (Windows would
use `.dll`). `template/` is a standalone module (`go.mod`), so it can be copied
anywhere; keep `include/` reachable or adjust the `#cgo` include path.

## Testing your extension

There is no test harness to install — load the library into the CLI and call
the functions:

```sql
LOAD EXTENSION '/abs/path/my_extension.dylib';
SELECT my_ext_greet('world');        -- Hello, world!
SELECT my_ext_greet(NULL) IS NULL;   -- true   (NULL path)
SELECT my_ext_greet(1);              -- Error: my_ext_greet argument 1 must be VARCHAR, got INTEGER (use CAST)
SELECT my_ext_now();                 -- clock reading (volatile: fine in a SELECT)
```

Tips: test NULL inputs explicitly (`... IS NULL`), exercise the VARCHAR result
through a built-in (`upper(...)`, `length(...)`) to prove the bytes survive,
check a multibyte round trip, and trigger your error codes on purpose. A
non-NULL argument needs an explicit `CAST` to the declared type — `INTEGER`
literals do not widen into `BIGINT` parameters. The documented exception is a
HUGEINT/UHUGEINT argument bound to a BIGINT parameter: the host checks that it
fits (an out-of-range value is an error naming the function and the parameter,
never a wrap), which keeps `my_ext(SUM(int_col))` working; an exact HUGEINT
parameter receives the value unchanged. Go has no native 128-bit integer:
`pinkdb.Hugeint(lo, hi)` / `pinkdb.Uhugeint(lo, hi)` and the matching
`HugeintBits`/`UhugeintBits` accessors carry the two halves. `./smoke.sh` is the full worked
example: it builds the release CLI and the example, runs a deterministic
session in a self-cleaning temp directory, and greps 17 checks — VARCHAR
results, NULLs, a multibyte round trip, composition with built-ins, a 5,000-row
parallel query, and both error paths.

## The vendored header

`include/pinkdb_extension.h` is vendored byte-for-byte from
`crates/pinkdb-ext-loader/include/pinkdb_extension.h` (the single source of
truth). The `header_sync` test in the loader crate fails `cargo test` if either
SDK's copy drifts; re-vendor with:

```sh
cp crates/pinkdb-ext-loader/include/pinkdb_extension.h ext-sdk/go/include/pinkdb_extension.h
```
