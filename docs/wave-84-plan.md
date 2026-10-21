# Wave 84 — query execution profile attached to results (plan)

Started 2026-10-21; **done** the same day. ROADMAP row 84: every statement can return its timings
and facts next to its rows, opt-in; plain `execute` stays zero-cost;
profiling never changes results and timings are never WAL-logged.

## The profile

A new public module `pinkdb::profile` with:

```rust
/// Timings and facts of one executed statement (wave 84).
#[derive(Debug, Clone, PartialEq)]
pub struct QueryProfile {
    /// Parsing the whole SQL text the statement came from (shared by every
    /// statement of one `execute_profiled` call — the text is parsed once).
    pub parse: std::time::Duration,
    /// Binding, which in pinkdb also builds the logical plan.
    pub bind: std::time::Duration,
    /// The optimizer.
    pub optimize: std::time::Duration,
    /// Execution, including any lease/refresh wait and conflict retry.
    pub execute: std::time::Duration,
    /// WAL logging (append + sync per policy) and auto-checkpoint; zero for
    /// reads and in-memory stores.
    pub log: std::time::Duration,
    /// Wall time of this statement from bind start to the end of logging,
    /// plus its share of `parse` (the first statement carries it; later
    /// statements of the same text report `parse` but do not add it again).
    pub total: std::time::Duration,
    /// Result rows (`Selected`), or the affected count (INSERT/UPDATE/DELETE),
    /// else 0.
    pub rows: u64,
    /// Result columns (`Selected`), else 0.
    pub columns: u32,
    /// The statement kind: the `QueryResult` variant name ("Selected",
    /// "Inserted", "Called", …).
    pub kind: String,
    /// True when the statement ran inside an open transaction.
    pub in_transaction: bool,
    /// `env!("CARGO_PKG_VERSION")` of the pinkdb crate.
    pub engine_version: &'static str,
}
```

## Engine API (task T1)

- `PinkDb::execute_profiled(&mut self, sql: &str) -> Result<Vec<(QueryResult, QueryProfile)>>`
- `Connection::execute_profiled(&mut self, sql: &str) -> Result<Vec<(QueryResult, QueryProfile)>>`
- Same semantics, results and errors as `execute` (the first failing
  statement returns its error; earlier statements' effects stay, exactly as
  with `execute`).
- **Zero cost when off:** `execute` and `execute_profiled` share one
  runner that takes an optional profile sink; with `None` it never calls
  `Instant::now()` and allocates nothing for profiling.
- **Never changes results or the WAL:** profiling only reads clocks; WAL
  records are byte-identical with and without it.

## Server protocol v10 (task T3)

- Bump `PROTOCOL_VERSION` 9 → 10 (client and server; the handshake already
  refuses a mismatch).
- New request `SetProfiling { enabled: bool }` (a per-connection session
  setting, default off) answered by a new `Response::ProfilingSet
  { enabled: bool }`.
- While enabled, the server sends one `Response::Profile` frame **after each
  statement's `Done`** (before the next result group or `BatchDone`) with
  every `QueryProfile` field: durations as `u64` nanoseconds, `rows` u64,
  `columns` u32, `kind` string, `in_transaction` u8, `engine_version`
  string. Decoding validates lengths like every other frame.
- Client: `Client::set_profiling(bool)` and
  `Client::execute_profiled(sql) -> Result<Vec<(QueryResult, Option<QueryProfile>)>>`
  (a profile is `None` when profiling is off).

## FFI (task T4)

- After executing a statement through the C ABI, a caller can read the
  profile of the last executed statement: add `pinkdb_set_profiling(db, int)`
  and accessors returning each duration in nanoseconds, rows, columns and
  the kind string (same ownership rules as the existing string accessors).
  Off by default; when off the accessors report "no profile".

## CLI (task T2)

- `.timer on` prints, after each statement: `Run Time: 1.234 ms (parse
  0.010, bind 0.020, optimize 0.005, execute 1.100, log 0.090)` from the
  local engine's profile; in remote (client) mode it enables profiling on
  the connection and prints the server's profile.

## Tests and checks

- Engine: results identical with and without profiling (multi-statement
  strings, DML, transactions, errors); `kind`/`rows`/`columns` correct;
  durations non-negative and `total >= bind + optimize + execute + log`;
  WAL bytes identical for the same workload with and without profiling.
- Overhead: `bench/profile_overhead.py` (or a Rust bench test, `#[ignore]`)
  reports `execute` vs `execute_profiled` on many tiny statements.
- Protocol: round-trip encode/decode of the new frames, a malformed
  `Profile` frame is rejected, v9 clients are refused cleanly.
- Gates: `cargo test --workspace`, `cargo clippy --all-targets -- -D
  warnings`, fmt, wasm32 core build, duckdiff.

## Rules for delegated agents

Do not commit, push or create branches. Touch only the files your task
names. Run `cargo fmt --all`, then the task's tests and `cargo clippy
--all-targets -p <crate> -- -D warnings` before finishing, and report what
you changed and the test results.

## How it was built

Implemented by delegated agents on DeepSeek V4.1 Flash, each limited to its
task's files and reviewed before acceptance: T1 engine (OpenCode), T3
protocol v10 (CommandCode), T4 FFI (OpenCode), T2 CLI (CommandCode).
Review fixes: the CLI summed the shared `parse` time once per statement
(now counted once), and the `Connection` docs claimed bind+optimize were
measured together although that path runs no optimizer (`optimize` is 0).
Measured: plain `execute` 21,155 vs 21,185 ns/stmt before the change (noise);
`execute_profiled` +2.1% on tiny statements.
