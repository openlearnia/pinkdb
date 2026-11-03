# pinkdb C++ extension SDK

Everything needed to build a loadable extension for pinkdb in **C++17**, as a
thin header-only wrapper over the extension ABI v1 (minor 4). No pinkdb
libraries are linked and no third-party library is used: an extension is a
shared library that the host `dlopen`s and drives through the C ABI in
[`include/pinkdb_extension.h`](include/pinkdb_extension.h). The wrapper in
[`include/pinkdb_ext.hpp`](include/pinkdb_ext.hpp) adds RAII, typed accessors
and exception containment on top of it; the header stays the contract.

```
ext-sdk/cpp/
├── include/pinkdb_extension.h      # vendored ABI header (byte-identical to the loader crate's copy)
├── include/pinkdb_ext.hpp          # header-only C++17 wrapper (this SDK)
├── examples/cpp_ext.cpp            # ext_add_checked, ext_reverse (VARCHAR), ext_now (volatile)
├── conformance/conformance_ext.cpp # the ABI conformance fixture (see ext-sdk/conformance/README.md)
├── Makefile                        # builds both libraries into build/
├── smoke.sh                        # end-to-end test against the release CLI
└── README.md
```

## Quick start

```sh
make -C ext-sdk/cpp                # -> build/cpp_ext.dylib, conformance_ext.dylib (.so on Linux)
./ext-sdk/cpp/smoke.sh             # build + load + query, end to end

# or, manually, in a pinkdb REPL (release build):
cargo build --release -p pinkdb-cli
./target/release/pinkdb
```

```sql
LOAD EXTENSION '/abs/path/to/ext-sdk/cpp/build/cpp_ext.dylib';
-- Extension cpp_ext loaded.
SELECT ext_add_checked(CAST(2 AS BIGINT), CAST(3 AS BIGINT));
-- 5
SELECT ext_reverse('héllo');
-- olléh
SELECT ext_now();
-- 2024-05-01 12:00:00.123456   (volatile: a read, so it is allowed)
INSERT INTO t VALUES (ext_now());
-- Error: extension function ext_now() is volatile (nondeterministic) and
-- cannot be called in a statement the engine WAL-logs: ...
```

`LOAD EXTENSION '<path>'` needs the opt-in described under
[Security posture](#security-posture). A successful load prints
`Extension <name> loaded.`; the declared name is pinned, so loading the same
extension twice is an error. Loaded functions resolve case-insensitively like
built-ins and cannot shadow one.

## Writing an extension

An extension is a set of exported C functions plus one `init`. With the wrapper
you write the callbacks and one registration block; the wrapper supplies the
ABI plumbing. The smallest complete extension:

```cpp
#include "pinkdb_ext.hpp"

namespace ext = pinkdb::ext;

// The host api copy taken during init. Read-only afterwards; callbacks run
// concurrently, so anything mutable must be thread-safe.
ext::Host g_host;

int32_t greet(void *user_data, const ext::Args &args, ext::Writer &out) {
    (void)user_data;
    if (args[0].is_null()) {
        out.set_null();                  // NULL in -> NULL out
        return 0;
    }
    out.set_text(std::string("Hello, ") + std::string(args[0].varchar()) + "!");
    return 0;                            // 0 = success, non-zero = statement error
}

PINKDB_EXT_EXPORTS("my_ext")             // abi_version + name, nothing else

extern "C" int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    return ext::guard("pinkdb_extension_init", [&]() -> int32_t {
        g_host = ext::Host::adopt(api, host_ctx);   // throws on an unusable host
        if (g_host.register_function("my_greet", {ext::kVarchar}, ext::kVarchar, &greet) != 0) {
            return 10;                                // rejected: fail the load
        }
        return 0;
    });
}
```

`examples/cpp_ext.cpp` is the worked version of this, with three functions:

| function | signature | shows |
|---|---|---|
| `ext_add_checked` | `(BIGINT, BIGINT) -> BIGINT` | NULL in → NULL out, `i64` overflow as `Error` code 1 |
| `ext_reverse` | `(VARCHAR) -> VARCHAR` | the RAII text path, multibyte input, empty result |
| `ext_now` | `() -> TIMESTAMP` | a **volatile** registration (reads the clock) |

### The wrapper API

| name | what it is |
|---|---|
| `ext::Host` | Copy of `PinkdbHostApi` + `host_ctx`. `Host::adopt(api, ctx)` refuses a NULL api, a mismatched `abi_version`, a `struct_size` below `PINKDB_EXT_API_V1_SIZE`, or missing `register_scalar`/`alloc`/`free`; it throws `ext::Error`. `has_flags()` / `has_execute()` report the two append-only additions. |
| `Host::register_function(name, {arg tags}, ret_tag, fn, user_data = nullptr)` | Registers a deterministic scalar. Returns the host's code: `0` registered, non-zero rejected. |
| `Host::register_volatile(...)` | Same, with `PINKDB_EXT_FLAG_VOLATILE`. **Throws** `ext::Error` when the host has no `register_scalar_flags` — it never falls back to a deterministic registration. |
| `Host::execute_statement(sql, &out)` | ABI minor 2: run SQL in the host. Returns the host's code (`0` ok, `1` statement failed, `2` no live session / older host). Only meaningful inside `pinkdb_ext_serve`. |
| `Host::alloc(size)` / `Host::free(ptr)` / `Host::allocator()` | The host's allocator pair. Prefer `Text`/`Writer::set_text`, which use it for you. |
| `ext::Value` | Read-only view of one argument: `type()`, `is_null()`, `boolean()`, `integer()`, `bigint()`, `double_()`, `varchar()` → `std::string_view`, `date()` (days), `timestamp()` (µs), `interval()` → `ext::Interval{months, days, micros}`, `raw()`. |
| `ext::Args` | The argument list: `size()`, `empty()`, bounds-checked `operator[]`. |
| `ext::Text` | Move-only owning host buffer. `Text::copy(allocator, bytes)`, `view()`, `size()`; the destructor frees through the host's `free`, never `std::free`. |
| `ext::Writer` | Writes the single result into the host's slot: `set_null`, `set_bool`, `set_int`, `set_bigint`, `set_double`, `set_date`, `set_timestamp`, `set_interval`, `set_text(std::string_view)` (allocates through `host.alloc`), `set_text(Text&&)` (transfers ownership to the host), `make_text(bytes)` (build without publishing). |
| `ext::guard(what, fn)` | `noexcept` exception boundary: returns `0`, the `Error`'s own code, `kOutOfMemory` for `std::bad_alloc`, `kUnhandledException` for anything else, and prints one diagnostic to stderr. Wrap every `extern "C"` entry point in it. |
| `PINKDB_EXT_EXPORTS("name")` | Emits `pinkdb_extension_abi_version` and `pinkdb_extension_name`. `pinkdb_extension_init` (and the optional `pinkdb_ext_serve`) stay hand-written, as above. |
| `ext::Error(code, message)` | Throw it from a callback to fail the statement with a specific code: the host reports `extension function <name>() failed with code <n>`. |

Requirements: C++17, standard library only, `-Iinclude`. Builds claimed by the
Makefile are warning-clean under `-Wall -Wextra -Werror` (and under
`-pedantic`).

## ABI 1.2 coverage

- **Host copy is init-scoped.** The `PinkdbHostApi` the loader passes is valid
  only during `pinkdb_extension_init`. `Host::adopt` copies it (and the wrapper
  hands each registration its own copy), so callbacks can use `alloc`/`free`/
  flags later. Never keep the original pointer.
- **`struct_size` negotiation.** `has_flags()` and `has_execute()` are true only
  when the host's `struct_size` covers the field *and* the function pointer is
  non-NULL, so a wrapper built against this header also runs on an older host.
  An extension that requires a newer field fails init cleanly
  (`register_volatile` throws) instead of misbehaving.
- **Every tag round-trips.** `NULL`, `BOOLEAN`, `INTEGER`, `BIGINT`, `DOUBLE`,
  `VARCHAR`, `DATE`, `TIMESTAMP`, `INTERVAL`. `DOUBLE` is copied bit-for-bit
  (`std::memcpy`), so NaN payloads and ±inf survive. `INTERVAL` is DuckDB's
  three-field calendar interval: `int64` carries `micros`, the `double_` bit
  pattern carries `months << 32 | days`; `Value::interval()` /
  `Writer::set_interval()` do the `std::memcpy` conversion, never pointer
  punning. `DECIMAL` (tag 8) is unsupported in ABI v1.
- **Deterministic vs volatile.** Registrations are deterministic by default;
  a callback that reads a clock or randomness must be registered volatile, and
  the engine then refuses it in any statement it WAL-logs
  (`INSERT`/`UPDATE`/`DELETE`/DDL/`CALL`) before the statement runs, because
  replay could produce a different value. Read-only statements may call
  volatile functions freely. Unknown flag bits are rejected by the host.
- **Text ownership.** A non-NULL `VARCHAR` result must be `api->alloc` memory.
  The host copies exactly `text_len` bytes, then calls `api->free` immediately —
  even when `is_null` is set or the callback returns an error code. `Text`
  guarantees both halves; an empty string is a zero-length result
  (`text_ptr == NULL`, `text_len == 0`) and needs no allocation. VARCHAR
  **inputs** are host-owned and valid only during the call: copy them (into a
  `std::string`) to keep them. Results above 64 MiB are rejected.
- **`execute_statement` (minor 2)** is reachable through
  `Host::execute_statement` from inside the optional
  `pinkdb_ext_serve(api, host_ctx, transport, options)` entry point, where the
  host wires the api to a live session. It is not usable from a scalar
  callback, and a scalar callback must never call back into the engine.
- **Commands (minor 4).** The header copy carries the optional
  `pinkdb_ext_command(api, host_ctx, args, sink, sink_ctx)` entry point, the
  `PinkdbExtSinkFn` stream sink and the `execute_internal` host field (JSON
  rows, confined to the host's database for the extension). This wrapper does
  not expose them yet: a C++ command extension needs a plain C shim over them
  (see the Rust SDK's `CommandSession` for the shape).
- **No unwinding.** `guard` catches everything at the `extern "C"` edge;
  anything it catches becomes a documented non-zero code plus a stderr
  diagnostic, because an exception crossing into C is undefined behavior.
- **Thread safety.** Callbacks run **concurrently** on the host's rayon worker
  threads, never necessarily on init's thread. Everything a callback touches —
  globals, statics, `user_data` — must be thread-safe (atomics, locks, or
  thread-local storage). `alloc`/`free` are thread-safe. The counters in
  `conformance/conformance_ext.cpp` are the worked example.
- **Linked-only kinds.** The ABI has **no** registration entry point for table
  functions, scans, or copy sinks. Those extension kinds are *linked* into the
  engine (or a linked extension), not loaded as shared libraries; nothing in
  this SDK can register one.

## The conformance fixture

`conformance/conformance_ext.cpp` implements
[`ext-sdk/conformance/README.md`](../conformance/README.md): the extension is
named `conformance_ext` and registers the `cf_*` functions every SDK
implements, so the loader tests measure the four SDKs against one contract
rather than against their own examples. It also exports
`pinkdb_conformance_calls`, `pinkdb_conformance_foreign_threads` (callbacks
that ran on a different thread than init) and `pinkdb_conformance_caps`
(`3`: `cf_fail` really allocates its 4 bytes through the host and then returns
code 1; `cf_unwind` really throws inside the callback).

```sh
make -C ext-sdk/cpp build/conformance_ext.dylib   # or: make -C ext-sdk/cpp
```

## Building

The only include path needed is this SDK's `include/`:

```sh
c++ -std=c++17 -O2 -fPIC -Wall -Wextra -Werror -I ext-sdk/cpp/include \
    -dynamiclib -o my_ext.dylib my_ext.cpp      # -shared on Linux
```

`make -C ext-sdk/cpp` builds both libraries into `ext-sdk/cpp/build/` and is
warning-free. `make TARGET_ARCH=x86_64` cross-builds on macOS. The
shared-library suffix is `.dylib` on macOS and `.so` on Linux.

## Testing your extension

There is no harness to install — load the library into the CLI and call the
functions:

```sql
LOAD EXTENSION '/abs/path/my_ext.dylib';
SELECT my_greet('world');            -- Hello, world!
SELECT my_greet(NULL) IS NULL;       -- true  (NULL path)
SELECT my_greet(1);                  -- Error: ... argument 1 must be VARCHAR, got INTEGER
```

Tips: test NULL inputs explicitly (`... IS NULL`), exercise a VARCHAR result
through a built-in (`upper(...)`, `length(...)`) to prove the bytes survive,
and trigger your error codes on purpose. Arguments are strictly typed, with
one documented exception: a HUGEINT/UHUGEINT argument binds to a declared
BIGINT parameter through a **checked** narrowing — an out-of-range value is an
error naming the function and the parameter, never a wrap. That keeps
`my_ext(SUM(int_col))` working; an exact HUGEINT parameter receives the value
unchanged (`Value::hugeint()`/`uhugeint()`, `Writer::set_hugeint`/
`set_uhugeint`). `./smoke.sh` is the full worked
example: it builds both libraries and runs one session covering numeric
results, NULLs, multibyte and empty VARCHAR results, the volatile path in a
read-only statement and its refusal in a WAL-logged `INSERT`, both error paths,
and the security posture checks below.

## Security posture

Loading an extension executes native code in the host process. That is why it
is opt-in and confined:

- **Off by default.** `LOAD EXTENSION` is refused
  (`LOAD EXTENSION is disabled`) unless the operator passes
  `--allow-load-extension` or sets `PINKDB_ALLOW_LOAD_EXTENSION=1`.
- **Confinement.** `--extension-root PATH` confines every load to one
  directory; a library outside it is refused (`outside the extension root`).
- **Path rules.** A bare name (`libfoo.so`) is refused — the dynamic loader
  would search its own path — as is any path containing a `..` component, a
  symlink, and anything that is not a regular file. Use the real, absolute
  path.
- **Stable absolute paths.** For a durable database the WAL is replayed on open
  and re-loads exactly the extensions the replayed records need, by the path
  recorded in the log. Keep extension files at stable absolute paths: a missing
  file at replay time is a hard recovery error naming the path, never a silent
  drop.
- **`--extension-pin PATH=SHA256` pins the bytes.** The flag (on the CLI and the
  server, repeatable) installs a SHA-256 allowlist: once at least one pin is
  given, a library that is not pinned is refused (`is not pinned`), a library
  whose hash differs is refused (`does not match its pinned SHA-256`), and so is
  one that changes between the hash and the load (`changed on disk while it was
  being checked`). It is the only content check a local load has, and
  `dlopen` takes a path rather than a descriptor, so the directory holding the
  library must still be one only the operator can write.
- **Unsigned local libraries.** A library loaded with `LOAD EXTENSION` is not
  verified: it runs as-is. The Ed25519 signature checks cover only `INSTALL`
  and `.install` packages, not ad-hoc loads.
- **Same privileges as the host.** `pinkdb-server` loads extensions into the
  *server* process, so anyone who can send SQL can run native code with the
  server's privileges.

## The vendored header

`include/pinkdb_extension.h` is vendored byte-for-byte from
`crates/pinkdb-ext-loader/include/pinkdb_extension.h` (the single source of
truth); the loader crate's `header_sync` test fails if the two copies drift.
Do not edit it — the wrapper is in `include/pinkdb_ext.hpp`.
