# pinkdb Rust extension SDK

Everything needed to build a pinkdb extension in **safe Rust** against the
extension ABI v1. One `pinkdb_extension!` declaration defines the extension —
its name and its functions — and the SDK generates the ABI glue: value
marshalling, VARCHAR ownership, NULL handling, the three `pinkdb_extension_*`
exports a native cdylib needs, and the wave-49 **linked** registration that
covers every target *including wasm*. Extension authors write no `unsafe` and
no C.

```
ext-sdk/rust/
├── include/pinkdb_extension.h   # vendored ABI header (byte-identical to the loader crate's copy)
├── src/                         # the SDK crate (`pinkdb-ext-sdk`)
│   ├── lib.rs                   # crate docs: types, volatility, errors, the macros
│   ├── macros.rs                # `pinkdb_extension!`
│   ├── spec.rs                  # ScalarSpec: the one description both loading models consume
│   ├── value.rs                 # the typed value model (ExtArgType / FromExtArg / IntoExtValue)
│   └── native.rs                # the ABI v1 exports and the single extern "C" trampoline
├── example/string_ext/          # worked example (a workspace member): one declaration, four functions
├── template/                    # copy this directory to start an extension (standalone crate)
├── tests/                       # native (rustc-built cdylib) and linked round trips + a durable replay
├── Makefile                     # builds the example / the template
├── smoke.sh                     # end-to-end test against the release CLI
└── README.md
```

The header is the complete ABI contract; this README is the guided tour, with
the Rust-specific patterns and sharp edges the header cannot know about.

## Quick start

```sh
cargo build --release -p pinkdb-ext-sdk-example   # or: make -C ext-sdk/rust
./ext-sdk/rust/smoke.sh                           # build + load + query, end to end
```

Load the extension from a pinkdb REPL (release CLI):

```sql
LOAD EXTENSION '/abs/path/to/target/release/librust_string_ext.dylib';
-- Extension rust_string_ext loaded.
SELECT ext_rs_repeat('ab', CAST(3 AS BIGINT)), ext_rs_len('héllo');
-- ababab | 6
SELECT ext_rs_next_day(DATE '2024-02-28');
-- 2024-02-29
SELECT ext_rs_tick();
-- 0     (volatile: a read, so it is allowed)
INSERT INTO t VALUES (ext_rs_tick());
-- Error: extension function ext_rs_tick() is volatile (nondeterministic) and
-- cannot be called in a statement the engine WAL-logs: ...
```

`LOAD EXTENSION '<path>'` takes a string literal path (absolute paths are
recommended). A successful load prints `Extension <name> loaded.`; the declared
name is pinned, so loading the same extension twice is an error. Loaded
functions resolve case-insensitively like built-ins and cannot shadow one.

## Writing an extension

A whole extension is one macro invocation. Start from `template/` and write
plain Rust functions:

```rust
use pinkdb_ext_sdk::pinkdb_extension;

pinkdb_extension! {
    name = "my_extension",
    functions = [
        fn my_ext_greet(name: Option<&str>) -> Result<Option<String>, String> {
            match name {
                Some(name) => Ok(Some(format!("Hello, {name}!"))),
                None => Ok(None),
            }
        },
    ],
}
```

The macro emits:

- `pinkdb_extension_abi_version`, `pinkdb_extension_name`,
  `pinkdb_extension_init` — the ABI v1 exports `LOAD EXTENSION '<path>'`
  drives (native targets only: wasm has no dynamic linker);
- `pinkdb_scalar_specs()` — the extension's functions as a `&'static
  [ScalarSpec]`, the one description both loading models consume;
- `pinkdb_linked_extension()` / `pinkdb_register_linked()` — the wave-49
  linked registration (see [One source, two loading
  models](#one-source-two-loading-models)).

A declaration may appear **once per crate**: it defines the extension's
exported symbols and a few plain functions. Use one module per extension, as
the template does.

### Types

Parameters and return values are the ABI v1 types, in the Rust spelling the
author prefers:

| SQL type | parameter types | return types |
|---|---|---|
| `BOOLEAN` | `bool` | `bool` |
| `INTEGER` | `i32` | `i32` |
| `BIGINT` | `i64` | `i64` |
| `DOUBLE` | `f64` | `f64` |
| `VARCHAR` | `&str` (borrowed for the call) or `String` (copied) | `String`, `&'static str` |
| `DATE` | `Date` (days since 1970-01-01) | `Date` |
| `TIMESTAMP` | `Timestamp` (microseconds since 1970-01-01 UTC) | `Timestamp` |
| `INTERVAL` | `Interval` (`months`/`days`/`micros`) | `Interval` |

`Option<T>` around any of them makes NULL a normal value; `Result<T, String>`
around a return type makes errors returnable. All combinations are accepted:
`T`, `Option<T>`, `Result<T, String>`, `Result<Option<T>, String>`. The
registered argument/return types are *derived from the Rust types you write* —
there is no separate metadata table to keep in sync.

ABI v1 is strictly typed: the engine inserts **no implicit casts** for
extension arguments, with one documented exception — a **numeric** argument
(INTEGER, BIGINT, DECIMAL) widens into a `DOUBLE` parameter, the implicit
cast DuckDB applies to its math functions (`exp(1)`, `sqrt(4)`).
`ext_rs_repeat('ab', 3)` is an error (`argument 2 must be BIGINT, got
INTEGER (use CAST)`) because the literal is INTEGER and the parameter is
BIGINT — write `CAST(3 AS BIGINT)`, or declare the parameter `i32`. A
VARCHAR stays strict even for a `DOUBLE` parameter (`exp('1')` is an error;
DuckDB parses the string — a documented divergence).

### NULL handling is explicit

Every registered argument type accepts a bare `NULL` at bind time; the
*function* decides what NULL means, through its signature:

- `Option<T>`: NULL arrives as `None`; returning `None` produces SQL NULL;
- plain `T`: a NULL is an error — the statement fails rather than the value
  being silently reinterpreted. The error message names the fix
  (`declare the parameter Option<i64> ...`); on native targets the C ABI
  carries only a code, so the statement reports `failed with code 1` — the
  linked model carries the message.

`NULL` literals are typeless and pass through for any declared type; a
*typed* NULL column of the wrong type is a bind-time error like any other
mismatch.

### Ranged arity: `optional(min)` and `variadic(min)`

A function is fixed-arity unless it says otherwise. Two leading modifiers
declare the arity ranges the wave-52 linked model supports:

```rust
pinkdb_extension! {
    name = "my_extension",
    functions = [
        /// 1 or 2 arguments: `my_log(100)` or `my_log(2, 8)`.
        optional(1) fn my_log(b: Option<f64>, x: Optional<f64>) -> Option<f64> {
            let b = b?;
            Some(match x {
                Optional::Absent => b.log10(),
                Optional::Null => return None,
                Optional::Value(x) => x.log10() / b.log10(),
            })
        },
        /// Any number of NULL-able arguments, at least one.
        variadic(1) fn my_sum(values: Vec<Option<i64>>) -> Option<i64> {
            let mut total = 0i64;
            for value in values.into_iter().flatten() {
                total = total.wrapping_add(value);
            }
            Some(total)
        },
    ]
}
```

- `optional(min)`: the declared parameters, but a call may stop after `min`
  of them. Positions from `min` on must be written [`Optional<T>`], whose
  three states are `Absent` (not passed), `Null` and `Value(T)` — a bare
  `Option<T>` conflates the first two, and `my_log(10)` (base-10) really is
  not `my_log(2, NULL)` (SQL NULL). Positions below `min` are always
  `Value(_)`; write a plain type there to say so in the signature.
- `variadic(min)`: exactly one parameter, `Vec<T>`, and a call passes `min`
  to 64 arguments of the declared `T` type. `T` is usually `Option<_>` so
  NULL stays representable; `decode_arguments` (what the generated glue
  calls) copies each argument, borrowing VARCHAR bytes for the call.

Both forms register through the linked model
(`LinkedFunction::new_optional` / `new_variadic`). **The native ABI v1 has no
ranged-arity descriptor**: `pinkdb_extension_init` refuses a declaration that
uses either modifier (a diagnostic on stderr names the function) instead of
silently registering it as fixed-arity at its maximum — which would refuse
the shorter calls the declaration promises. Publish such an extension with
`pinkdb_register_linked()` (wasm's only model, and the native fallback):

```rust
my_extension::pinkdb_register_linked()?;
// then, from SQL: LOAD EXTENSION 'my_extension'; SELECT my_log(100);
```

Fixed-arity declarations are untouched by all of this: they never take this
path, and their generated code is the same as before wave 54.

### Deterministic by default — `volatile` for everything else

**Functions under `functions` are deterministic**: the same arguments always
produce the same result. That is what lets the engine WAL-log a statement
that calls them and re-execute that statement on recovery (replay), and it is
the default for every extension, including all pre-existing ABI v1 ones.

A function that reads a clock, a random source, or other ambient state is
*not* deterministic. Declare it under the separate `volatile` list:

```rust
pinkdb_extension! {
    name = "my_extension",
    functions = [ /* deterministic */ ],
    volatile = [
        fn my_ext_now_micros() -> Result<i64, String> { /* read a clock */ },
    ],
}
```

The engine then **refuses to execute a volatile call in any statement it
WAL-logs** — `INSERT`, `UPDATE`, `DELETE`, DDL, `CALL`, ... — with a clear
error, *before the statement runs*:

```
Error: extension function my_ext_now_micros() is volatile (nondeterministic) and
cannot be called in a statement the engine WAL-logs: replaying it could produce
a different result. Use it in a read-only statement, or declare the function
deterministic
```

Read-only statements (a plain `SELECT`, including one inside a view body) never
reach the WAL and may call volatile functions freely. The refusal is applied by
the *statement class*, on every store — also on an in-memory one, so the same
SQL behaves identically in tests and on a durable database.

The declaration is the author's contract: the host cannot verify purity.
Forgetting to declare a nondeterministic function is a bug that can make a
durable store replay divergent data; declaring a pure function volatile only
costs it the right to run in a WAL-logged statement. (For the native ABI this
is the additive `PINKDB_EXT_FLAG_VOLATILE` registration flag — see the
header's "Deterministic vs volatile functions" section — and the linked model
carries it as `LinkedFunction::volatile()`; the SDK sets it for you.)

### Errors

`Err(message)` fails the enclosing SQL statement. On native targets the C ABI
carries an error **code**, not text (ABI v1's contract), so the statement
reports:

```
Error: extension function my_ext_fail() failed with code 1
```

Code 1 means "the function returned an error, or was handed an argument its
declared signature does not accept". It is deliberately not printed anywhere:
a failing function is a normal SQL error, not a diagnostic. Code 2 means an
SDK/ABI contract failure (a panic in the function, an allocation failure, a
host that violates the ABI) and prints a one-line diagnostic to stderr.
Under the **linked** model the message itself reaches SQL:
`extension function <name>() failed: <message>`.

### Threading

Callbacks are invoked **concurrently** from the host's rayon worker threads: a
registered function may run on any thread, several at once, and init does not
run on the same thread as the callbacks. Guard shared mutable state with
atomics or locks (the example's volatile counter uses an `AtomicI64`). The SDK
catches panics at the ABI boundary — a panic fails the statement and prints a
diagnostic instead of unwinding into the host — but panics are not a control
flow mechanism; return `Err`.

## One source, two loading models

The same declaration feeds both ways an extension can be loaded:

- **native `dlopen`** (this is what a cdylib built from the declaration is):
  `LOAD EXTENSION '/path/to/libmy_extension.dylib'` runs
  `pinkdb_extension_init`, which registers every function through the host
  API. The SDK's single `extern "C"` trampoline serves them all.
- **linked registration** (wave 49; the only model on wasm, where there is no
  dynamic linker): the embedder calls the generated
  `pinkdb_register_linked()` at host bootstrap, then `LOAD EXTENSION
  'my_extension'` resolves the *name* against the process-global registry. The
  same functions, declared volatile ones included, behave identically.

```rust
// In the embedder (natively, or a wasm-bindgen start function):
my_extension::pinkdb_register_linked()?;
// then, from SQL: LOAD EXTENSION 'my_extension'; SELECT my_ext_greet('world');
```

Because this is a compile-time registration, an rlib of the extension can be
linked into the host binary — that is exactly how a wasm build ships. The SDK
itself is never a dependency of pinkdb: the release binary does not grow with
an extension SDK.

## Sharp edges

The ABI header documents the contract; these are the Rust-specific
consequences, verified against the real host:

- **A cdylib extension must not be loaded into a process that never unloads
  it and then rebuilt in place.** Like every native extension, the library
  stays mapped for the life of the host process; replacing the file on disk
  does not change the loaded code. Restart to pick up a new build.
- **`&str` parameters borrow the host's bytes for the duration of the call.**
  They are valid only inside the function body — copy to `String` to keep them.
  `String` parameters copy per row; prefer `&str` in hot paths.
- **A VARCHAR result must be UTF-8.** The engine rejects a non-UTF-8 VARCHAR
  result with `returned invalid UTF-8`; the SDK produces `String`/`&'static
  str` only, which is why raw byte results have no representation here.
- **Returning `&'static str` requires `'static`.** A borrowed result from the
  function's own data works (`'static` string literals, leaked data); anything
  shorter must be returned as `String`.
- **The extension name is a static C string.** It is the literal you passed to
  `name =` (NUL-terminated by the macro), matched case-insensitively, and
  pinned: loading the same extension twice fails.
- **Errors are codes on native targets.** There is no message channel in ABI
  v1 (see [Errors](#errors)); if you need messages in SQL errors, the linked
  model carries them — or wrap the failure in a deterministic function that
  can only fail for argument-visible reasons and document them.
- **`cargo build` of a cdylib on macOS needs no extra link flags.** The
  extension links only the SDK (and thereby the loader crate for the shared
  value model); the host provides everything else at load time.

## Building

The example is a workspace member, so `cargo build --release -p
pinkdb-ext-sdk-example` (or `make -C ext-sdk/rust`) puts the library at
`target/release/librust_string_ext.dylib` (`.so` on Linux; a `.dll` on
Windows). A copied-out `template/` builds on its own: it is a standalone crate
(`cargo build --release` inside it) with a path dependency on this SDK —
`make -C ext-sdk/rust template` builds it in place to check the skeleton.

## Testing your extension

There is no harness to install — load the library into the CLI and call the
functions. The SDK's own tests are the worked patterns:

- `tests/linked_round_trip.rs` — one declaration registered as a linked
  extension and queried through a real engine (every value kind, NULLs,
  errors, the volatile refusal);
- `tests/native_round_trip.rs` — the example compiled with `rustc` into a
  cdylib, loaded through `LOAD EXTENSION '<path>'`, queried, plus a durable
  store whose deterministic extension statement WAL-logs and replays
  identically;
- `./smoke.sh` — the CLI-visible end-to-end session (15 greps), including the
  volatile refusal and the error paths.

Tips: test NULL inputs explicitly (`... IS NULL`), exercise VARCHAR results
through a built-in (`upper(...)`, `length(...)`) to prove the bytes survive,
check a multibyte round trip, trigger your error paths on purpose, and put
every nondeterministic helper in the `volatile` list *before* a durable store
depends on it.

## Semantics and persistence

Extensions are **process-level**: they are not stored in `.pink` snapshots and
are not part of a transaction. `LOAD EXTENSION` is refused inside
`BEGIN..COMMIT`. For a durable database the WAL replays the statements that
need an extension, prefixed with `LOAD EXTENSION '<path>'` (a checkpoint
resets that coverage), so keep extension files at stable absolute paths. A
missing file at replay time is a hard recovery error naming the path. A
*volatile* call is refused in any WAL-logged statement before it runs, so
replay never has to reproduce a nondeterministic value. `pinkdb-server` loads
extensions into the *server* process; anyone who can send SQL can load a
shared library with the server's privileges.

## The vendored header

`include/pinkdb_extension.h` is vendored byte-for-byte from
`crates/pinkdb-ext-loader/include/pinkdb_extension.h` (the single source of
truth; the Rust SDK implements the extension side of that header, and the
loader crate's `header_sync` test fails `cargo test` if the copies drift):

```sh
for sdk in c go rust; do
    cp crates/pinkdb-ext-loader/include/pinkdb_extension.h \
       ext-sdk/$sdk/include/pinkdb_extension.h
done
```
