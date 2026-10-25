# pinkdb C extension SDK

Everything needed to build a loadable extension for pinkdb against extension
ABI 1.2. No pinkdb libraries are linked: an extension is a shared library that
the host `dlopen`s and drives through the C ABI in
[`include/pinkdb_extension.h`](include/pinkdb_extension.h).

```
ext-sdk/c/
├── include/pinkdb_extension.h   # vendored ABI header (byte-identical to the loader crate's copy)
├── template/                    # copy this directory to start an extension
│   ├── my_extension.c           # commented skeleton: exports, a VARCHAR scalar, a volatile scalar, serve
│   ├── Makefile                 # standalone build
│   └── CMakeLists.txt           # alternative build
├── examples/
│   ├── math_ext.c               # ext_clamp(BIGINT×3), ext_deg2rad(DOUBLE), ext_now_micros() [volatile]
│   └── string_ext.c             # ext_repeat(VARCHAR, INTEGER), ext_title(VARCHAR)
├── conformance/
│   └── conformance_ext.c        # the shared ABI conformance fixture (see ext-sdk/conformance/)
├── Makefile                     # builds the examples + fixture into build/
├── smoke.sh                     # end-to-end test against the release CLI
└── README.md
```

The C++ SDK lives in [`ext-sdk/cpp/`](../cpp/README.md): a header-only RAII
wrapper over this same header. The header itself is the complete contract;
this README is the guided tour.

## Quick start

Loading a native library is **opt-in** (security review C-2) — a database a
user merely opens must never `dlopen` arbitrary code:

```sh
export PINKDB_ALLOW_LOAD_EXTENSION=1     # or start the CLI with --allow-load-extension
make -C ext-sdk/c                        # -> build/math_ext.dylib, string_ext.dylib, … (.so on Linux)
./ext-sdk/c/smoke.sh                     # build + load + query, end to end

# or, manually, in a pinkdb REPL (release build):
cargo build --release -p pinkdb-cli
./target/release/pinkdb --allow-load-extension
```

```sql
LOAD EXTENSION '/abs/path/to/ext-sdk/c/build/math_ext.dylib';
-- Extension math_ext loaded.
SELECT ext_clamp(CAST(5 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT));
-- 5
SELECT ext_deg2rad(CAST(180 AS DOUBLE));
-- 3.141592653589793
```

`LOAD EXTENSION '<path>'` takes a string literal path. Absolute paths are
strongly recommended (see [Security posture](#security-posture)). A successful
load prints `Extension <name> loaded.`; the declared name is pinned, so loading
the same extension twice is an error (`extension "x" is already loaded`).
Loaded functions resolve case-insensitively like built-ins and cannot shadow
one.

## Writing an extension

Start from `template/` and keep the three exports:

```c
uint32_t    pinkdb_extension_abi_version(void);   /* must return PINKDB_EXT_ABI_VERSION */
const char *pinkdb_extension_name(void);          /* NUL-terminated UTF-8, non-empty, ≤256 bytes */
int32_t     pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx);
```

`pinkdb_extension_init` runs exactly once, on the loading thread, after the
version and name checks. Register scalar functions through `api->register_scalar`
or `api->register_scalar_flags` (pass `host_ctx` back unchanged). Return 0 on
success; any non-zero code aborts the load and is reported. A rejected
registration also fails the load even when init returns 0, so check every
registration result.

There is also one **optional** entry point, added in ABI minor 2:

```c
int32_t pinkdb_ext_serve(const PinkdbHostApi *api, void *host_ctx,
                         const char *transport, const char *options);
```

A host that runs the extension as an MCP server looks it up after init and
calls it instead of returning to SQL; it blocks until the client disconnects
and returns 0. `template/my_extension.c` implements a `"stdio"` transport with
one statement per line; see [Serving a session](#serving-a-session-minor-2).

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
    /* appended in later minor versions — only present when struct_size says so */
    int32_t (*register_scalar_flags)(void *ctx, const char *name, uint32_t n_args,
                                     const int32_t *arg_types, int32_t ret_type,
                                     uint32_t flags, PinkdbExtScalarFn fn,
                                     void *user_data);
    PinkdbExtExecuteStatementFn execute_statement;
} PinkdbHostApi;
```

`abi_version` is the major version: a mismatch aborts the load with an error
naming both versions. Minor additions are **additive and appended**, so an
extension built against 1.0 keeps loading into a 1.2 host, and a 1.2 SDK must
work out what the host actually has. That is what `struct_size` is for — never
read a field past it. The header provides the comparison macros:

```c
if (api->struct_size < PINKDB_EXT_API_V1_SIZE) { return 2; }   /* too old */
if (PINKDB_EXT_HOST_HAS_FLAGS(api)) { /* api->register_scalar_flags is readable */ }
if (PINKDB_EXT_HOST_HAS_EXECUTE(api)) { /* api->execute_statement is readable */ }
```

`PINKDB_EXT_API_V1_SIZE` is the offset of the first appended field, i.e. the
size of the 1.0 struct — the oldest layout an SDK that only needs the original
five fields can accept. `PINKDB_EXT_API_FLAGS_SIZE` and
`PINKDB_EXT_API_MINOR2_SIZE` are the offsets of `register_scalar_flags` and
`execute_statement`.

The struct itself is valid **only during init**. Callbacks run later on other
threads; copy it into a static (it is a small POD) if they need `alloc`/`free`,
as the template does. `register_scalar`/`register_scalar_flags` may only be
called from init.

### Values and NULL semantics

One tagged scalar, `pinkdb_ext_value`, crosses the ABI by value:

| tag | value in `int64` | value in `double_` |
|---|---|---|
| `PINKDB_EXT_TYPE_NULL` (0) | — | — |
| `PINKDB_EXT_TYPE_BOOLEAN` (1) | 0 / 1 | — |
| `PINKDB_EXT_TYPE_INTEGER` (2) | the i32 value | — |
| `PINKDB_EXT_TYPE_BIGINT` (3) | the i64 value | — |
| `PINKDB_EXT_TYPE_DOUBLE` (4) | — | the f64 value |
| `PINKDB_EXT_TYPE_VARCHAR` (5) | — | — (`text_ptr`/`text_len`) |
| `PINKDB_EXT_TYPE_DATE` (6) | days since 1970-01-01 | — |
| `PINKDB_EXT_TYPE_TIMESTAMP` (7) | µs since the epoch, UTC | — |
| `PINKDB_EXT_TYPE_INTERVAL` (9) | `micros` | `((uint32)months << 32) \| (uint32)days` bits |

Tag 8 (`DECIMAL`) is deliberately not part of the ABI. `is_null` non-zero
means NULL, and only `type_tag` is then meaningful. `text_ptr`/`text_len` are
the VARCHAR bytes; embedded NULs are allowed and no trailing NUL is required.
A `VARCHAR` argument holds UTF-8 text, but the ABI is bytes: validate/reject
anything you cannot handle instead of assuming ASCII.

`PINKDB_EXT_TYPE_INTERVAL` (tag 9) is DuckDB's three-field calendar interval.
The by-value struct cannot grow a field without breaking ABI v1, so `months`
and `days` ride in the `double_` bit pattern: memcpy the double to a
`uint64_t` and read `months = (uint32_t)(bits >> 32)`,
`days = (uint32_t)bits`; `int64` carries `micros`. All three fields round-trip
exactly; a NULL interval ignores the payload. The header documents the same
layout.

- The host always calls the callback, even for NULL arguments, with `args[i].is_null` set — the extension decides what NULL means. A zero-argument callback receives `args == NULL` (`n_args == 0`).
- `out` points at host-owned writable storage pre-filled with NULL. Set `out->type_tag` (and text fields for VARCHAR), then return 0. Leaving `is_null` set yields NULL.
- Return non-zero only for errors: the statement aborts with `extension function <name>() failed with code <n>`. Never use it for an ordinary NULL result.
- Argument and return tags are checked at call time: returning a different tag than you registered, or an unknown tag, is an error.

### VARCHAR results: alloc/free ownership

A non-NULL VARCHAR result **must** be allocated with `api->alloc(size)` — never
`malloc`, `new`, a stack buffer, or a static/interior pointer. The host copies
exactly `text_len` bytes and calls `api->free(text_ptr)` immediately after the
callback returns. Rules:

- a non-NULL `text_ptr` is **always** freed by the host — even when `is_null`
  is set or the callback returns a non-zero code;
- the host never frees a NULL `text_ptr`;
- a VARCHAR result over 64 MiB is rejected by the host without reading or
  freeing the buffer (a corrupt `text_len` must not become an unbounded read);
- VARCHAR **inputs** (`args[i].text_ptr`) are host-owned: never free them, and
  treat them as valid only for the duration of the call (copy to keep).

`string_ext.c`, the template and `conformance_ext.c` demonstrate the pattern.
For a zero-length result the host also accepts `text_ptr == NULL`, but
allocating a 1-byte buffer is the uniform pattern — the rule that matters is:
a non-NULL `text_ptr` must always be `api->alloc` memory.

### Thread safety

Callbacks are invoked **concurrently** from the host's rayon worker threads: a
registered function may run on any thread, several at once. Everything a
callback touches (globals, statics, `user_data`) must be thread-safe —
atomics, locks, or thread-local storage. Do not assume init and callbacks run
on the same thread (the conformance fixture counts callbacks that don't).
`alloc`/`free` are thread-safe.

### No unwinding across the boundary

`pinkdb_extension_init` and every callback are called through an
`extern "C"` edge where unwinding is undefined behavior. In C there is no
exception story; in C++ catch every exception inside the callback (or avoid
throwing) and return a non-zero code instead. A callback must also not call
back into the engine (except through the `execute_statement` service, which is
only valid inside `pinkdb_ext_serve`).

## Building

The only include path needed is this SDK's `include/`:

```sh
# C99
cc -std=c99 -O2 -fPIC -Wall -Wextra -I ext-sdk/c/include \
   -dynamiclib -o my_ext.dylib my_ext.c          # -shared on Linux
```

`make -C ext-sdk/c` builds the examples and the conformance fixture into
`ext-sdk/c/build/` and is warning-free with `-Wall -Wextra`;
`template/Makefile` and `template/CMakeLists.txt` are self-contained starting
points. The shared-library suffix is `.dylib` on macOS and `.so` on Linux.

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
numeric results, NULLs, a VARCHAR result, the volatile flag and an error path.

### The conformance fixture

`conformance/conformance_ext.c` is this SDK's half of the shared
[ABI conformance fixture](../conformance/README.md): every type tag in both
directions, NULL and zero-length results, host-allocated text, error codes, the
volatile flag, foreign-thread callbacks and `struct_size` negotiation. The
loader's `tests/conformance.rs` builds this file and the other three SDKs'
fixtures and runs the same assertions against each, so a gap in this SDK fails
`cargo test -p pinkdb-ext-loader`. Port it (don't extend an example) when you
add an SDK.

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
registration, and a host older than the addition (`PINKDB_EXT_HOST_HAS_FLAGS`
false) does not have the field: an extension that needs a volatile registration
must fail init there rather than silently registering as deterministic.
`examples/math_ext.c` (`ext_now_micros`) is the worked example.

## Serving a session (minor 2)

In the plain-scalar ABI an extension never sees the engine's session: it gets
argument structs and returns one value. The minor-2 `execute_statement`
callback exists for extensions that are *hosts* of their own protocol (the
in-repo MCP extension is the reference):

```c
int32_t (*execute_statement)(void *ctx, const char *sql, size_t sql_len,
                             char **out_text, size_t *out_len);
```

- the SQL need not be NUL-terminated (`sql_len` bytes);
- return 0 with the statement's Markdown result in `*out_text`/`*out_len`
  (either pointer may be NULL; a NULL `*out_text` with code 0 means "no
  output"), 1 when the statement failed (the text is a JSON error object), or
  2 when there is no live session;
- the returned buffer is `malloc`'d by the host: copy it and then `free` it
  (with libc `free`, not `api->free`); it is not NUL-terminated;
- read-only refusals arrive as code 1 plus a JSON error, not as 2.

The callback is only meaningful inside `pinkdb_ext_serve`, which receives the
same `host_ctx`. In the init-scoped struct the pointer is a stub that always
answers 2, so an extension can call it defensively from init without effect.

## Security posture

Local libraries are **not** signed. The Ed25519 signing and hash pinning of
`.install` cover extension *packages* fetched from a repository (a source-built
CLI with the `repo` feature); a `LOAD EXTENSION '/path/lib.so'` is exactly what
it looks like — running local native code:

- **Loading is opt-in.** Without `--allow-load-extension` (CLI/server) or
  `PINKDB_ALLOW_LOAD_EXTENSION=1`, `LOAD EXTENSION` is refused. A `.pink` file
  cannot enable it.
- **`--extension-root <dir>`** confines loads to one directory tree; a path
  outside it is refused, as is a bare filename (the dynamic loader's search
  path must never decide which library runs), any `..` component, and a
  symlink (load the real file instead). The root is canonicalized once at
  startup, so the check does not depend on the working directory.
- **`--extension-pin PATH=SHA256` pins the bytes.** The flag (on the CLI and the
  server, repeatable) installs a SHA-256 allowlist: once at least one pin is
  given, a library that is not pinned is refused (`is not pinned`), a library
  whose hash differs is refused (`does not match its pinned SHA-256`), and so is
  one that changes between the hash and the load (`changed on disk while it was
  being checked`). It is the only content check a local load has, and
  `dlopen` takes a path rather than a descriptor, so the directory holding the
  library must still be one only the operator can write.
- **Use stable absolute paths.** A WAL re-loads by path on replay; a relative
  path resolves against whatever directory the process happens to start in,
  and a missing file is a hard recovery error.
- **A server loads with server privileges.** `pinkdb-server` `dlopen`s into its
  own process: whoever can send `LOAD EXTENSION` can run code as the server
  user. Combined with the opt-in flag and the root confinement, treat
  `LOAD EXTENSION` as equivalent to shell access on that host.

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
- Extensions are never unloaded while the process lives, so function pointers
  stay valid for the whole session (a loaded library also cannot be re-loaded a
  second time while it serves).

## What is linked-only

Table functions, table scans and copy sinks are **not** part of this ABI: there is no registration entry point for them, so a
dynamically loaded extension cannot provide them. Those extensions are built
into the binary (the in-repo `extensions/*` crates) or shipped as SDK-level
plugins in a host that links them. A future ABI minor may add registration;
until then, do not plan around it.

## The vendored header

`include/pinkdb_extension.h` is vendored byte-for-byte from
`crates/pinkdb-ext-loader/include/pinkdb_extension.h` (the single source of
truth). The `header_sync` test in the loader crate fails `cargo test` if any
copy drifts; re-vendor all SDKs with:

```sh
for sdk in c cpp go rust; do
    cp crates/pinkdb-ext-loader/include/pinkdb_extension.h ext-sdk/$sdk/include/pinkdb_extension.h
done
```
