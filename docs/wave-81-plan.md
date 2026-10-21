# Wave 81 — JavaScript procedure engine binding (kickoff plan)

Prepared 2026-10-21. Status: **in progress** (started 2026-10-21). The roadmap's
deferral condition ("until the skipped block 46–56 is finished") is met:
every wave 46–56 is `done`.

## Goal

Make `CREATE PROCEDURE … LANGUAGE JAVASCRIPT` and `CALL` work in builds with
the `js` feature, by filling the one stub the wave-44 contract left:
`pinkdb_js::run_procedure` (today it returns `JsError::EngineUnavailable`).
The default build stays engine-free and byte-identical.

## What already exists (wave 44)

- `crates/pinkdb-js/src/lib.rs` — the engine-agnostic contract, no
  dependencies: `HOST_FUNCTIONS` (`print`, `db.exec`, `db.query`,
  `db.value`), `DENIED_INTRINSICS`, `SandboxConfig { seed, statement_limit,
  step_limit }`, `JsValue`, `HostRequest`/`HostReply`, the `Host` trait,
  `JsError`, and the stubbed `engine_available()` / `run_procedure()`.
- `crates/pinkdb/src/js.rs` — two-line bridge (`engine_available`).
- `crates/pinkdb/src/procedure.rs` — `ProcLanguage::JavaScript` is parsed,
  persisted (`.pink` flags bit 4, language tag 1) and refused in two places:
  `check_language_available` (at `CREATE`) and `unsupported_language` (at
  `CALL`, for definitions loaded from a file). `execute_call_inner` holds the
  atomicity snapshot, argument coercion and `MAX_CALL_DEPTH`; `exec_sql` is
  the per-statement bind → optimize → replay-safety checks → touched-set →
  execute path that `db.exec`/`db.query` must reuse.
- Features: `pinkdb/js = ["dep:pinkdb-js"]`, `pinkdb-cli/js = ["pinkdb/js"]`.

## Engine facts (checked 2026-10-21, `rquickjs` 0.14.0, MSRV 1.87)

- Vendors quickjs-ng C sources and compiles them with `cc`. Prebuilt
  bindings ship for every wave-90 release target (linux x86_64/aarch64
  gnu+musl, darwin arm64/x86_64, windows) **and `wasm32-unknown-unknown`**
  (vendored wasi-libc + `wasm-shim/shim.c`, which supplies a clock — moot if
  `Date` is not installed). `bindgen` is not needed.
- Intrinsics are selected per context (`Context::custom::<(…)>`):
  `BaseObjects`, `Date`, `Eval`, `RegExp(Compiler)`, `Json`, `Proxy`,
  `MapSet`, `TypedArrays`, `Promise`, `Performance`, `WeakRef`,
  `DOMException`. Omitting `Date`/`Performance`/`WeakRef`/`Promise`/
  `TypedArrays` covers most of `DENIED_INTRINSICS`. **`Math.random` lives in
  `BaseObjects`** and must be deleted after context creation (then checked by
  a test). quickjs-ng has no `Intl`, `fetch`, timers or module loader unless
  added (`loader` feature stays off).
- Limits: `Runtime::set_interrupt_handler`, `set_memory_limit`,
  `set_max_stack_size`, `set_gc_threshold`. The interrupt handler fires on
  an internal op counter, so counting its calls gives a step bound that is
  deterministic for a given script and build.
- The public API is safe Rust: `pinkdb-js` itself may need **no `unsafe`**;
  the "contained-FFI carve-out" is the dependency, not our code.

## Implementation steps

1. **Dependency + feature.** `pinkdb-js` gains `rquickjs = { version =
   "0.14", default-features = false, features = ["std"] }` behind a new
   `engine` feature; `pinkdb/js` enables it. Default `cargo build` links
   nothing new (verify with the CLI size check).
2. **`run_procedure`.** One `Runtime` + `Context` per call (no state
   survives between calls): intrinsics = `BaseObjects`, `Eval`? (decide;
   likely off), `RegExp`, `RegExpCompiler`, `Json`, `MapSet`; delete
   `Math.random`; install `print` and a frozen `db` object with `exec`,
   `query`, `value`, `random` (seeded xorshift from `SandboxConfig::seed`).
   Wrap the body as a function so `return` works, pass arguments by
   parameter name, convert the return value through `JsValue`.
3. **Limits.** Interrupt handler counts toward `step_limit`; a host-call
   counter enforces `statement_limit`; set a memory limit and max stack
   size (new `SandboxConfig` fields, with defaults). Every limit hit is a
   `JsError` naming the limit.
4. **Host in `pinkdb`.** A `Host` impl in `crates/pinkdb/src/js.rs` that
   parses the SQL, binds `params` as bound values (the same
   `ParamSlots` path the SQL language uses — never string splicing), and
   runs it through the `exec_sql` checks (dynamic-scalar refusal, attached
   reads, touched/written sets into `proc_frame`). Refuse the statements
   `refused_in_body` refuses (transaction control, `USE`, DDL on
   databases/procedures/MVs, `LOAD EXTENSION`, `CHECKPOINT`).
5. **Wire the call path.** `check_language_available` / `unsupported_language`
   return `Ok`/`None` when `engine_available()` is `Ok`; `execute_call_inner`
   dispatches JavaScript bodies to `pinkdb_js::run_procedure` inside the
   same snapshot/restore and depth accounting; the result becomes
   `QueryResult::Called` (a returned value → one-row `Selected` named
   `return`, like SQL `RETURN expr`). Seed = hash of the stored definition,
   so a WAL replay of `CALL` draws the same `db.random()` stream.
6. **Surfaces.** CLI/server/FFI already render `Called`; check `print`
   output reaches the result (contract: "appends one line to the call's
   output") and decide its rendering.
7. **wasm.** Try `cargo build -p pinkdb --features js --target
   wasm32-unknown-unknown` (needs clang with the wasm target). Ship it if
   it builds cleanly; otherwise document the refusal and keep the wasm gate
   on the default feature set.
8. **Release/CI.** Add a `js` build/test/clippy row to CI, decide whether a
   `js` CLI variant joins the wave-90 matrix, and record the measured
   engine size (roadmap says ~1.1 MB — verify in a release build).

## Tests to write

- Contract: every `DENIED_INTRINSICS` name is undefined (or not callable)
  in the sandbox; `HOST_FUNCTIONS` is exactly what a script can see on
  `db`.
- Determinism: the same `CALL` on a durable store, reopened and replayed,
  yields identical data, including `db.random()` streams; a CALL that hits
  a limit fails identically on replay.
- Injection safety: params with quotes/semicolons stay values.
- Atomicity: a throwing script after `db.exec` leaves no trace; inside an
  open transaction the transaction survives (mirror the SQL-language tests).
- Refusals: transaction control, attached-lake reads, extension functions,
  session views on a durable store, nested `CALL` depth.
- Limits: infinite loop → step-limit error; host-call flood →
  statement-limit error; allocation bomb → memory-limit error; deep
  recursion → stack error. None may crash or hang the engine.
- Conversions: every `JsValue` direction, integral vs fractional numbers,
  `i64` edge cases, `undefined`/`null`, unsupported types (objects,
  functions, BigInt) as `JsError::Value` naming the argument.
- Persistence: a JavaScript definition round-trips through `.pink` and the
  WAL and runs after reopen; a build without `js` still refuses it cleanly.

## Decisions (recommended defaults, accepted 2026-10-21)

1. **`print` output** — a new `output: Vec<String>` field on
   `QueryResult::Called` (one entry per `print`, args joined by a space). The
   CLI prints the lines before the call's result; FFI and the server protocol
   **do not carry it yet** (no protocol bump this wave — a recorded
   follow-up; the client side reports an empty list).
2. **`eval` / `Function` constructor** — **off**: the `Eval` intrinsic is not
   installed, so scripts cannot compile code at run time. Smaller surface;
   procedure bodies are static text anyway.
3. **Step-limit units** — keep the `step_limit` name and document the unit
   honestly: quickjs-ng **interrupt-handler ticks** (one per internal op
   budget), deterministic for a given script and build, not a portable
   instruction count.
4. **wasm** — try `--features js` on wasm32-unknown-unknown; ship it if it
   builds and the sandbox tests pass, otherwise document `js` as native-only
   and keep the wasm gate on default features.

## Gates

`cargo build` (default unchanged) + `--features js`; `cargo test --workspace`
and `-p pinkdb --features js`; clippy (both toolchains; `-p pinkdb
--features js` added); wasm (default, plus `js` if step 7 ships); duckdiff;
release CLI size default vs `js`.
