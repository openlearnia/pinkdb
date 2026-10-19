# pinkdb Go extension SDK

Everything needed to build a loadable extension for pinkdb in Go against the
extension ABI v1. An extension is a shared library built with cgo
(`go build -buildmode=c-shared`) that the host `dlopen`s and drives through the
C ABI in [`include/pinkdb_extension.h`](include/pinkdb_extension.h). No pinkdb
libraries are linked: the host provides its services through the
`PinkdbHostApi` struct at load time.

```
ext-sdk/go/
├── include/pinkdb_extension.h   # vendored ABI header (byte-identical to the loader crate's copy)
├── template/                    # copy this directory to start an extension
│   ├── main.go                  # commented skeleton: exports + one VARCHAR scalar
│   ├── go.mod                   # standalone module
│   └── Makefile                 # standalone build
├── examples/
│   └── string_ext.go            # ext_go_repeat(VARCHAR, INTEGER), ext_go_len(VARCHAR)
├── go.mod                       # module for the worked example (extensions are not imported)
├── Makefile                     # builds the example into build/
├── smoke.sh                     # end-to-end test against the release CLI
└── README.md
```

The header is the complete contract; this README is the guided tour, with the
Go-specific patterns and sharp edges the header cannot know about.

## Quick start

```sh
make -C ext-sdk/go                 # -> build/string_ext.dylib (.so on Linux)
./ext-sdk/go/smoke.sh              # build + load + query, end to end

# or, manually, in a pinkdb REPL (release build):
cargo build --release -p pinkdb-cli
./target/release/pinkdb
```

```sql
LOAD EXTENSION '/abs/path/to/ext-sdk/go/build/string_ext.dylib';
-- Extension string_ext loaded.
SELECT ext_go_repeat('ab', 3), ext_go_len('héllo');
-- ababab | 6
```

`LOAD EXTENSION '<path>'` takes a string literal path (relative paths resolve
against the process's working directory; absolute paths are recommended). A
successful load prints `Extension <name> loaded.`; the declared name is pinned,
so loading the same extension twice is an error. Loaded functions resolve
case-insensitively like built-ins and cannot shadow one.

## Writing an extension

Start from `template/` and keep the three exports:

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
  `duplicate symbol` link error. All the shims below are `static inline`.

A c-shared library is still `package main` with a `func main() {}`; the host
never calls it, but the toolchain requires it.

### Calling the host: the C shims

Go cannot call C function pointers, so `api->register_scalar`, `api->alloc`
and `api->free` are each wrapped in a one-line static C shim in the preamble:

```c
static inline int32_t pinkdb_go_register_scalar(const PinkdbHostApi *api, void *ctx,
                                                const char *name, uint32_t n_args,
                                                const int32_t *arg_types, int32_t ret_type,
                                                PinkdbExtScalarFn fn, void *user_data) {
    return api->register_scalar(ctx, name, n_args, arg_types, ret_type, fn, user_data);
}
static inline void *pinkdb_go_alloc(const PinkdbHostApi *api, size_t size) { return api->alloc(size); }
static inline void pinkdb_go_free(const PinkdbHostApi *api, void *ptr) { api->free(ptr); }
```

The `PinkdbHostApi` struct is valid **only during init**; copy it into Go
memory once (it is small POD) and pass the copy to the shims later:

```go
var hostAPI C.PinkdbHostApi   // written in init, read-only afterwards

func pinkdb_extension_init(api *C.PinkdbHostApi, hostCtx unsafe.Pointer) C.int32_t {
    // ...version checks...
    hostAPI = *api
}
```

Registering the callback needs the same trick in reverse: Go cannot take the
address of a C symbol either. The preamble declares the `//export`ed callback
*exactly as cgo declares it* (cgo does not emit `const` on pointer parameters,
so the spelling must match) and a static accessor hands it to Go:

```c
extern int32_t my_ext_greet(void *user_data, pinkdb_ext_value *args,
                            size_t n_args, pinkdb_ext_value *out);
static inline PinkdbExtScalarFn my_ext_greet_fn(void) {
    return (PinkdbExtScalarFn)my_ext_greet;   /* the cast only drops const */
}
```

Registration itself then stays in Go:

```go
var argTypes = [1]C.int32_t{C.PINKDB_EXT_TYPE_VARCHAR}
name := C.CString("my_ext_greet")
defer C.free(unsafe.Pointer(name))
if C.pinkdb_go_register_scalar(api, hostCtx, name, 1, &argTypes[0],
    C.PINKDB_EXT_TYPE_VARCHAR, C.my_ext_greet_fn(), nil) != 0 {
    return 10   // rejected: a colliding name, an invalid tag, a NULL callback...
}
```

`register_scalar` may only be called from inside `pinkdb_extension_init`, and
`host_ctx` must be passed back unchanged. A rejected registration fails the
whole load even when init returns 0, so check every return value.

### Callbacks, NULLs, and panic recovery

The callback receives `args` (exactly the registered arity, read-only) and a
writable `out` pre-filled with NULL. A NULL argument still arrives as a struct
with `is_null` set — the callback decides what NULL means:

```go
//export my_ext_greet
func my_ext_greet(userData unsafe.Pointer, args *C.pinkdb_ext_value, nArgs C.size_t,
    out *C.pinkdb_ext_value) (code C.int32_t) {
    defer func() { if recover() != nil { code = 1 } }()   // see "no panics" below
    if args == nil || nArgs < 1 { return 1 }
    arg := unsafe.Slice((*C.pinkdb_ext_value)(unsafe.Pointer(args)), int(nArgs))[0]
    if arg.is_null != 0 { out.is_null = 1; return 0 }
    // ...read arg.text_ptr/text_len, fill out...
    return 0
}
```

Return 0 for success; a non-zero return aborts the enclosing SQL statement
with `extension function <name>() failed with code <n>`. Never use a non-zero
code for an ordinary NULL result.

### VARCHAR results: alloc/free ownership

A non-NULL VARCHAR result **must** be allocated with the host's `alloc`
(through the shim) — never `C.malloc`, never a Go buffer. The host copies
exactly `text_len` bytes and calls the host's `free` immediately after the
callback returns:

- a non-NULL `text_ptr` is **always** freed by the host — even when `is_null`
  is set or the callback returns non-zero, so never return a stack pointer, a
  static buffer, or Go memory there;
- the host never frees a NULL `text_ptr`; a zero-length result may use either
  a 1-byte `alloc` buffer with `text_len = 0` or `text_ptr == NULL`;
- VARCHAR **inputs** (`args[i].text_ptr`) are host-owned: never free them, and
  treat them as valid only for the duration of the call (copy to keep).

`string_ext.go` and the template demonstrate the pattern:

```go
buf := C.pinkdb_go_alloc(&hostAPI, C.size_t(len(msg)+1))
if buf == nil { return 1 }
dst := unsafe.Slice((*byte)(buf), len(msg)+1)
copy(dst, msg)
out.type_tag = C.PINKDB_EXT_TYPE_VARCHAR
out.is_null = 0
out.text_ptr = (*C.char)(buf)
out.text_len = C.size_t(len(msg))
```

### Thread safety

Callbacks are invoked **concurrently** from the host's rayon worker threads:
a registered function may run on any thread, several at once, on threads the
Go runtime did not create. cgo attaches such foreign threads on entry, but
nothing else is guaranteed: do not assume init and callbacks run on the same
thread, do not key state off goroutine/thread identity, and guard every piece
of shared mutable state with Go's own synchronization (`sync`, atomics).
`alloc`/`free` are thread-safe; `register_scalar` is init-only.

The smoke test's 5,000-row queries go through the executor's parallel
filter/projection paths; the callbacks run on several rayon workers at once.

## Sharp edges

The ABI header documents the contract; these are the Go-specific consequences,
verified against the real host (macOS, Go 1.26, cgo):

- **The Go runtime is linked into the host process.** `-buildmode=c-shared`
  embeds a complete Go runtime (GC, scheduler, `sysmon` threads, signal
  handling) in the `.dylib`. `dlopen` starts it; it never shuts down (the host
  keeps extensions mapped for the process lifetime). A trivial extension is
  ~1.3 MB, and the first `LOAD EXTENSION` of a Go-built library costs the
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
- **VARCHAR results must be host-`alloc` memory.** Go memory must never cross
  back to the host: cgo's pointer rules forbid storing Go pointers in C
  memory, and the host frees your buffer with its own `free`. Build the bytes
  in Go, copy them into a `pinkdb_go_alloc` buffer, set `text_ptr`/`text_len`.
  Input bytes are borrowed for the call only.
- **No panics across the boundary.** A panic in an exported function is not an
  error return — it aborts the whole host process (`panic: ...`, exit 134 in
  our test). Recover in every `//export`ed function (`init` included) and
  return a non-zero code instead. `init` and callbacks must also never call
  back into the engine.
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
- **The host API struct is init-only.** Copy it (Go struct copy) and pass the
  copy to the shims; dereferencing the host's original later is undefined.
  `register_scalar` is only valid inside init.
- **cgo does not emit `const`.** The `extern` declaration of an exported
  callback must match cgo's generated declaration exactly (non-const pointer
  types) or compilation fails with `conflicting types`; the accessor's cast
  back to `PinkdbExtScalarFn` only adjusts `const`.

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
```

Tips: test NULL inputs explicitly (`... IS NULL`), exercise the VARCHAR result
through a built-in (`upper(...)`, `length(...)`) to prove the bytes survive,
check a multibyte round trip, and trigger your error codes on purpose.
`./smoke.sh` is the full worked example: it builds the release CLI and the
example, runs a deterministic session in a self-cleaning temp directory, and
greps 15 checks — VARCHAR results, NULLs, a multibyte round trip, composition
with built-ins, a 5,000-row parallel query, and both error paths.

## Deterministic vs volatile functions

A registration is **deterministic by default** (`register_scalar`, and any
`register_scalar_flags` call with `flags == 0`): the callback must be a pure
function of its arguments, because the engine may WAL-log a statement that
calls it and re-execute that statement on recovery. A callback that reads a
clock, a random source, or other ambient state is **not** deterministic:
register it through the additive `api->register_scalar_flags` entry point with
`PINKDB_EXT_FLAG_VOLATILE`, and the engine then *refuses to execute* it in any
statement it WAL-logs (`INSERT`/`UPDATE`/`DELETE`/DDL/`CALL`) with a clear
error naming the function — before the statement runs, because replaying it
later could produce a different value than the original session saw. Read-only
statements may call volatile functions. Unknown flag bits are rejected at
registration, and a host older than the addition (smaller `struct_size`) does
not have the field: an extension that needs a volatile registration must fail
init there rather than silently registering as deterministic. `volatile_ext.c`
in the loader's test suite is the worked example. (The Rust SDK —
`ext-sdk/rust/` — expresses the same thing with its `volatile = [...]` list.)

## Semantics and persistence

Extensions are **process-level**: they are not stored in `.pink` snapshots and
are not part of a transaction. `LOAD EXTENSION` is refused inside
`BEGIN..COMMIT`. For a durable database the WAL replays the statements that
need an extension, prefixed with `LOAD EXTENSION '<path>'` (a checkpoint
resets that coverage), so keep extension files at stable absolute paths. A
missing file at replay time is a hard recovery error naming the path.
`pinkdb-server` loads extensions into the *server* process; anyone who can
send SQL can load a shared library with the server's privileges.

## The vendored header

`include/pinkdb_extension.h` is vendored byte-for-byte from
`crates/pinkdb-ext-loader/include/pinkdb_extension.h` (the single source of
truth). The `header_sync` test in the loader crate fails `cargo test` if either
SDK's copy drifts; re-vendor with:

```sh
cp crates/pinkdb-ext-loader/include/pinkdb_extension.h ext-sdk/go/include/pinkdb_extension.h
```
