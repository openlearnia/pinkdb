# pinkdb C/C++ extension SDK

Everything needed to build a loadable extension for pinkdb against the
extension ABI v1. No pinkdb libraries are linked: an extension is a shared
library that the host `dlopen`s and drives through the C ABI in
[`include/pinkdb_extension.h`](include/pinkdb_extension.h).

```
ext-sdk/c/
├── include/pinkdb_extension.h   # vendored ABI header (byte-identical to the loader crate's copy)
├── template/                    # copy this directory to start an extension
│   ├── my_extension.c           # commented skeleton: exports + one VARCHAR scalar
│   ├── Makefile                 # standalone build
│   └── CMakeLists.txt           # alternative build
├── examples/
│   ├── math_ext.c               # ext_clamp(BIGINT×3), ext_deg2rad(DOUBLE)
│   ├── string_ext.c             # ext_repeat(VARCHAR, INTEGER), ext_title(VARCHAR)
│   └── cpp_ext.cpp              # ext_add_checked(BIGINT×2) — proves the header is C++-safe
├── Makefile                     # builds all examples into build/
├── smoke.sh                     # end-to-end test against the release CLI
└── README.md
```

The header is the complete contract; this README is the guided tour.

## Quick start

```sh
make -C ext-sdk/c                 # -> build/math_ext.dylib, string_ext.dylib, cpp_ext.dylib (.so on Linux)
./ext-sdk/c/smoke.sh              # build + load + query all three, end to end

# or, manually, in a pinkdb REPL (release build):
cargo build --release -p pinkdb-cli
./target/release/pinkdb
```

```sql
LOAD EXTENSION '/abs/path/to/ext-sdk/c/build/math_ext.dylib';
-- Extension math_ext loaded.
SELECT ext_clamp(CAST(5 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT));
-- 5
SELECT ext_deg2rad(CAST(180 AS DOUBLE));
-- 3.141592653589793
```

`LOAD EXTENSION '<path>'` takes a string literal path (relative paths resolve
against the process's working directory; absolute paths are recommended). A
successful load prints `Extension <name> loaded.`; the declared name is pinned,
so loading the same extension twice is an error (`extension "x" is already
loaded`). Loaded functions resolve case-insensitively like built-ins and cannot
shadow one.

## Writing an extension

Start from `template/` and keep the three exports:

```c
uint32_t    pinkdb_extension_abi_version(void);   /* must return PINKDB_EXT_ABI_VERSION */
const char *pinkdb_extension_name(void);          /* NUL-terminated UTF-8, non-empty, ≤256 bytes */
int32_t     pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx);
```

`pinkdb_extension_init` runs exactly once, on the loading thread, after the
version and name checks. Register scalar functions through `api->register_scalar`
(pass `host_ctx` back unchanged). Return 0 on success; any non-zero code aborts
the load and is reported. A rejected registration also fails the load even when
init returns 0, so check every `register_scalar` result.

### The host API struct

```c
typedef struct PinkdbHostApi {
    uint32_t abi_version;    /* == PINKDB_EXT_ABI_VERSION */
    uint32_t struct_size;    /* host's sizeof(PinkdbHostApi); larger = newer host */
    int32_t (*register_scalar)(void *ctx, const char *name, uint32_t n_args,
                               const int32_t *arg_types, int32_t ret_type,
                               PinkdbExtScalarFn fn, void *user_data);
    void *(*alloc)(size_t size);
    void (*free)(void *ptr);
} PinkdbHostApi;
```

The struct itself is valid **only during init**. Callbacks run later on other
threads; copy it into a static (it is a small POD) if they need `alloc`/`free`,
as the template does. `abi_version`/`struct_size` let you refuse a mismatched
host explicitly. `register_scalar` may only be called from init.

### Values and NULL semantics

One tagged scalar, `pinkdb_ext_value`, crosses the ABI by value:

| field | meaning |
|---|---|
| `type_tag` | `PINKDB_EXT_TYPE_*` — the *declared* type (the binder inserts no implicit casts; callers write `CAST(...)`) |
| `is_null` | non-zero ⇒ NULL; only `type_tag` is then meaningful |
| `int64` | BOOLEAN (0/1), INTEGER, BIGINT, DATE (days since epoch), TIMESTAMP (µs since epoch UTC), INTERVAL (µs part) |
| `double_` | DOUBLE (same layout as the Rust field named `double`); for INTERVAL it carries the `(months << 32) \| days` bit pattern |
| `text_ptr`/`text_len` | VARCHAR bytes; embedded NULs allowed, no trailing NUL required |

`PINKDB_EXT_TYPE_INTERVAL` (tag 9) is DuckDB's three-field calendar
interval. The by-value struct cannot grow a field without breaking ABI v1,
so `months` and `days` ride in the `double_` bit pattern: memcpy the double
to a `uint64_t` and read `months = (uint32_t)(bits >> 32)`,
`days = (uint32_t)bits`; `int64` carries `micros`. All three fields
round-trip exactly; a NULL interval ignores the payload. The header
documents the same layout.

- The host always calls the callback, even for NULL arguments, with `args[i].is_null` set — the extension decides what NULL means (DuckDB calls this "special NULL handling"). A zero-argument callback receives `args == NULL`.
- `out` points at host-owned writable storage pre-filled with NULL. Set `out->type_tag` (and text fields for VARCHAR), then return 0. Leaving `is_null` set yields NULL.
- Return non-zero only for errors: the statement aborts with `extension function <name>() failed with code <n>`. Never use it for an ordinary NULL result.

### VARCHAR results: alloc/free ownership

A non-NULL VARCHAR result **must** be allocated with `api->alloc(size)` — never
`malloc`, `new`, a stack buffer, or a static/interior pointer. The host copies
exactly `text_len` bytes and calls `api->free(text_ptr)` immediately after the
callback returns. Rules:

- a non-NULL `text_ptr` is **always** freed by the host — even when `is_null`
  is set or the callback returns a non-zero code;
- the host never frees a NULL `text_ptr`;
- VARCHAR **inputs** (`args[i].text_ptr`) are host-owned: never free them, and
  treat them as valid only for the duration of the call (copy to keep).

`string_ext.c` and the template demonstrate the pattern. For a zero-length
result the host also accepts `text_ptr == NULL` (empty string, nothing to
free), but allocating a 1-byte buffer is the uniform pattern — the rule that
matters is: a non-NULL `text_ptr` must always be `api->alloc` memory.

### Thread safety

Callbacks are invoked **concurrently** from the host's rayon worker threads: a
registered function may run on any thread, several at once. Everything a
callback touches (globals, statics, `user_data`) must be thread-safe —
atomics, locks, or thread-local storage. Do not assume init and callbacks run
on the same thread. `alloc`/`free` are thread-safe.

### No unwinding across the boundary

`pinkdb_extension_init` and every callback are called through an
`extern "C"` edge where unwinding is undefined behavior. In C there is no
exception story; in C++ catch every exception inside the callback (or avoid
throwing) and return a non-zero code instead. A callback must also not call
back into the engine.

## Building

The only include path needed is this SDK's `include/`. Examples for the system
toolchain:

```sh
# C99
cc -std=c99 -O2 -fPIC -Wall -Wextra -I ext-sdk/c/include \
   -dynamiclib -o my_ext.dylib my_ext.c          # -shared on Linux

# C++17 (the header has extern "C" guards; export your entry points with C linkage)
c++ -std=c++17 -O2 -fPIC -Wall -Wextra -I ext-sdk/c/include \
   -dynamiclib -o my_ext.dylib my_ext.cpp
```

`make -C ext-sdk/c` builds all three examples into `ext-sdk/c/build/` and is
warning-free with `-Wall -Wextra`; `template/Makefile` and
`template/CMakeLists.txt` are self-contained starting points. The shared-library
suffix is `.dylib` on macOS and `.so` on Linux (Windows would use `.dll`).

## Testing your extension

There is no test harness to install — load the library into the CLI and call
the functions:

```sql
LOAD EXTENSION '/abs/path/my_ext.dylib';
SELECT my_ext_greet('world');        -- Hello, world!
SELECT my_ext_greet(NULL) IS NULL;   -- true   (NULL path)
SELECT my_ext_greet(1);              -- Error: ... argument 1 must be VARCHAR, got INTEGER (use CAST)
```

Tips: test NULL inputs explicitly (`... IS NULL`), exercise the VARCHAR result
through a built-in (`upper(...)`, `length(...)`) to prove the bytes survive,
and trigger your error codes on purpose. `./smoke.sh` is the full worked
example: it builds the examples and runs a deterministic session covering
numeric results, NULLs, a VARCHAR result, and both error paths.

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
in the loader's test suite is the worked example.

## Semantics and persistence

- **Extensions are process-level.** They are not stored in `.pink` snapshots
  and are not part of a transaction. `LOAD EXTENSION` is refused inside
  `BEGIN..COMMIT`.
- Every fresh session/process starts with no extensions loaded. For a durable
  database the WAL is replayed on open and **re-loads exactly the extensions
  the replayed records need** (records that call an extension are prefixed with
  `LOAD EXTENSION '<path>'`; a checkpoint resets that coverage). Keep the
  extension files at stable paths, preferably absolute: a missing file at
  replay time is a hard recovery error naming the path, never a silent drop.
- `pinkdb-server` loads extensions into the *server* process; anyone who can
  send SQL can load a shared library with the server's privileges.

## The vendored header

`include/pinkdb_extension.h` is vendored byte-for-byte from
`crates/pinkdb-ext-loader/include/pinkdb_extension.h` (the single source of
truth). The `header_sync` test in the loader crate fails `cargo test` if the
two copies drift; re-vendor with:

```sh
cp crates/pinkdb-ext-loader/include/pinkdb_extension.h ext-sdk/c/include/pinkdb_extension.h
```
