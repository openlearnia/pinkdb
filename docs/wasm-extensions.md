# Extensions on wasm — the linked registry (wave 49)

Native extensions are shared libraries: `LOAD EXTENSION '<path>'` runs
`dlopen`, the library exports ABI v1 symbols, and its callbacks are
`extern "C"` functions over by-value `pinkdb_ext_value` structs with host
`alloc`/`free` ownership. A wasm sandbox has no dynamic linker, no `dlsym`
and no filesystem, so that model cannot exist there. Wave 49 adds a second,
wasm-compatible model — **linked registration** — that feeds the same
engine registry, binder and executor.

Nothing about ABI v1 changed: the C header, the loader's `dlopen` path, the
C/Go SDKs and the extension repository are exactly as before. `libloading`
stays a native-only edge in the dependency graph; `wasm32` builds link zero
new dependencies.

## The two models

| | `dlopen` (ABI v1, native) | linked registry (wasm & hosts) |
|---|---|---|
| how the code arrives | shared library on disk | linked into the host binary (rlib / embedder code) |
| how it is addressed | filesystem path | registry name |
| how functions register | `pinkdb_extension_init` calls `api->register_scalar` | `LinkedExtension::new` at host bootstrap |
| callback | `extern "C"` over `pinkdb_ext_value` structs | `LinkedScalar`: safe Rust over `ExtArg` / `ExtValue` |
| text ownership | extension `alloc`s, host copies + `free`s | values by value, no ownership transfer |
| errors | `i32` return code | `Result<_, String>` |
| panics | forbidden (undefined behavior) | caught at the boundary, reported as a statement error |
| thread safety | promised by the extension | required by the type system (`Send + Sync`) |
| version check | `PINKDB_EXT_ABI_VERSION` compared at load | none needed — the Rust type system checks the ABI at compile time |
| lifecycle | `dlopen`; image stays mapped | process-lifetime registration; no unload |

Both models share `ExtType`, `ExtArg`, `ExtValue` and the 64-argument limit,
and both end up as `pinkdb_ext_loader::ExtFunction` entries in the store's
registry, keyed by lowercased name — the binder resolves names
case-insensitively, the executor marshals one row at a time, and the
optimizer never constant-folds a call to either (a callback is opaque in
both models).

## Loading rules

* **wasm targets** have no dynamic loader: `LOAD EXTENSION '<name>'` and
  `PinkDb::load_extension` resolve the name against the process-global linked
  registry. An unregistered name is a clear error
  (`extension "x" is not registered (register it at host bootstrap with
  pinkdb_ext_loader::linked::register; wasm targets have no dynamic loader)`).
* **native targets** keep the filesystem model: `LOAD EXTENSION '<path>'`
  `dlopen`s the library. Only when nothing is loadable at that exact string
  *and* a linked extension is registered under it does the call fall back to
  the registry — that is what lets a replayed WAL record
  (`LOAD EXTENSION '<name>'`) resolve natively, and it never hides the
  `dlopen` error otherwise.
* Registration is one-shot and host-side: publishing a name twice is an
  error, an extension stays registered for the life of the process, and SQL
  can only *activate* what the host published — it can never inject code.

## Publishing an extension (Rust)

```rust
use pinkdb_ext_loader::linked::{self, LinkedExtension, LinkedFunction};
use pinkdb_ext_loader::{ExtArg, ExtType, ExtValue};

// Host bootstrap, once per process. On wasm this runs from the embedder's
// start function; natively it can run before/after opening a store.
let ext = LinkedExtension::new(
    "windowed_math",                       // the registry key LOAD EXTENSION takes
    vec![
        LinkedFunction::new(
            "wma_halve",
            vec![ExtType::Double],
            ExtType::Double,
            |args: &[ExtArg<'_>]| match args[0] {      // closures annotate the parameter type
                ExtArg::Double(x) => Ok(ExtValue::Double(x / 2.0)),
                ExtArg::Null(_) => Ok(ExtValue::Null),
                other => Err(format!("unexpected argument {other:?}")),
            },
        )?,
    ],
)?;
linked::register(ext)?;

// Then, in SQL (or `PinkDb::load_linked_extension("windowed_math")`):
//   LOAD EXTENSION 'windowed_math';
//   SELECT wma_halve(5.0);   -- 2.5
```

`LinkedScalar` is a plain trait; any `Fn(&[ExtArg<'_>]) -> Result<ExtValue, String>`
that is `Send + Sync` implements it (closures must annotate the parameter
type, see the trait docs), and a struct with state implements it directly.
Metadata mistakes (empty names, duplicated function names, more than 64
arguments) are rejected when the record is built, so a handle in hand is
always well-formed; the engine then rejects collisions with built-ins or
already-registered functions atomically (nothing half-registers).

## Engine integration

| what | where |
|---|---|
| the linked model (registry, metadata, callbacks, JS contract) | `crates/pinkdb-ext-loader/src/linked.rs` |
| the unified function/handle types | `crates/pinkdb-ext-loader/src/lib.rs` (`ExtFunction`, `ExtHandle`) |
| the registry, both load entry points, WAL bookkeeping | `crates/pinkdb/src/ext.rs` |
| binder resolution (`BoundExpr::DynamicScalar`) | `crates/pinkdb/src/sql/binder.rs` |
| per-row marshalling and the call | `crates/pinkdb/src/exec/expr.rs` |

Extensions are process-level, so the WAL rules are unchanged: a durable
record whose statements call a linked function is prefixed with
`LOAD EXTENSION '<name>'` at append time (a checkpoint rolls the prefix
away), and a reopen replays the prefix through the registry. A linked
extension must therefore be registered **before** the store is opened when a
replayed record needs it — the wasm analogue of "the library file must still
be on disk".

## The JavaScript host surface (contract)

A wasm-bindgen embedder (the follow-up `pinkdb-wasm` package, recorded as the
next step) exposes the registry to JavaScript as exactly two functions —
`pinkdb_ext_loader::linked::JS_HOST_SURFACE`:

```js
pinkdb.registerExtension({
  name: "js_math",
  functions: [
    { name: "js_sqrt", args: ["DOUBLE"], ret: "DOUBLE",
      fn: (x) => x === null ? null : Math.sqrt(x) },
    { name: "js_upper", args: ["VARCHAR"], ret: "VARCHAR",
      fn: (s) => s === null ? null : s.toUpperCase() },
  ],
});

// LOAD EXTENSION 'js_math';
// SELECT js_sqrt(9.0), js_upper('hey');   -- 3.0, 'HEY'
```

`args` / `ret` are the ABI type names; `fn` is called once per row with
exactly that row's argument values and returns the row's result. The shim
registers the extension through `linked::register`, so from the engine's
side a JavaScript function is an ordinary linked function — same binder, same
executor, same WAL prefixing.

Value mapping (`linked::JS_TYPE_MAPPINGS`, `null` for SQL NULL on both
sides):

| SQL type | JavaScript | notes |
|---|---|---|
| `BOOLEAN` | `boolean` | true / false |
| `INTEGER` | `number` | integral, 32-bit range |
| `BIGINT` | `number` | integral, exactly representable (`\|n\| ≤ 2^53`); wider values need a host-side cast |
| `DOUBLE` | `number` | any finite value; NaN / ±Infinity cross as SQL NULL |
| `VARCHAR` | `string` | UTF-8 |
| `DATE` | `number` | days since 1970-01-01 (the ABI's `i32`) |
| `TIMESTAMP` | `number` | microseconds since 1970-01-01 00:00:00 (the ABI's `i64`) |
| `NULL` | `null` | valid for any declared type, on both sides |

Failures: a `throw` fails the enclosing statement
(`extension function <name>() failed: <message>`); a return value that does
not match the declared type fails it too (`… returned VARCHAR, registered as
INTEGER`); an arity/type mismatch in SQL is the ordinary binder error.

Rules the shim and its scripts must satisfy
(`linked::JS_SANDBOX_RULES`, requirements rather than enforcement — unlike
`pinkdb-js`, which *removes* the intrinsics that break determinism, the shim
cannot sandbox the embedder's JavaScript engine):

* the callback sees exactly its declared arguments — no database handle, no
  clock, no I/O, no ambient state;
* a callback must be a pure function of its arguments: a durable store
  replays committed statements, so `Date.now()` / `Math.random()` inside a
  script makes replay diverge, and that is the embedder's bug;
* a `throw` must not leave host state half-updated if the statement is
  retried;
* the registry is the only extension source on wasm — `LOAD EXTENSION` can
  never inject code.

## Limits and follow-ups

* **No wasm side-module dynamic linking.** The ROADMAP's alternative shape —
  pre-built WASI side modules with a per-runtime `dlopen` — needs a wasm
  runtime (wasmtime/wasmer) in the host. That is deliberately *not* in the
  default graph and is not shipped: the linked registration model is this
  wave's deliverable. A future wave can add a runtime behind a non-default
  feature and implement `LinkedScalar` over side-module exports; the value
  model and the registry are already the right seam.
* **The JavaScript binding itself is a follow-up.** The contract (surface,
  value mapping, failure mapping, determinism rules) is shipped and pinned by
  tests; no `wasm-bindgen` dependency was added.
* **Single-threaded on wasm.** `LinkedScalar` still requires `Send + Sync` so
  one extension is portable between targets, but on wasm (no threads) calls
  are serial.
* **One registry per process.** Registration is global, like the filesystem
  is for native extensions; there is no per-store registry and no unload.
* **`LOAD EXTENSION` takes a name on wasm**, not a path. A native embedder
  with a linked extension uses `PinkDb::load_linked_extension` (or the same
  statement, via the fallback).

## Tests and gates

* `crates/pinkdb-ext-loader/tests/linked_extensions.rs` — end-to-end through
  SQL on a native build (the models share every line except `dlopen`):
  registration → loading by name → binder → optimizer → executor, every ABI
  value kind, NULLs, columns, WHERE/GROUP BY/aggregation, failure paths
  (callback error, panic, wrong result type, arity/type mismatch),
  registration conflicts (built-in collision, duplicate function, duplicate
  extension, metadata) and a durable WAL round-trip with a checkpoint.
* `linked.rs` unit tests — metadata validation, call checks, panic catching,
  the registry, and the pinned JavaScript contract.
* `cargo build -p pinkdb --target wasm32-unknown-unknown` stays green (the CI
  wasm check) with no new dependencies on that target.
