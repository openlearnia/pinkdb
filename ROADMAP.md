# pinkdb Roadmap

Single source of truth for wave status; update after every wave.

## Packaging principles

- New capabilities ship as separate workspace crates and/or opt-in cargo features; the default
  `cargo build` stays lean (~2 MB) and links no crypto/TLS/format code.
- Every wave reports per-artifact release binary size; CI gates grow a build matrix (default,
  each feature, each crate) plus a wasm check — `pinkdb` and `pinkdb-parquet` must stay
  wasm32-compatible, and the server is target-gated out of wasm builds.
- No hand-rolled crypto; audited RustCrypto crates only.
- Zero-`unsafe` holds for the core; contained `unsafe` is confined to `pinkdb-ffi` (wave 26)
  and `pinkdb-ext-loader` (wave 29). Compile-time engine components ship as separate
  crates/features (waves 22–28), distinct from waves 29+ loadable extensions (versioned C ABI
  + runtime loader).

Status: `done` / `in progress` / `queued`. A wave is `done` only when `cargo build` +
`cargo test` + `cargo clippy` are green (plus duckdiff/bench where applicable).

## Waves

| Wave | Scope | Status | Evidence |
|---|---|---|---|
| 0 | Repo baseline: initial commit (pre-wave-1) | done | e0785a3 (initial commit); details per PLAN.md |
| 1 | M0–M1: scaffold, types/vector/storage/CSV, skeleton CLI | done | per PLAN.md: 18 tests |
| 2 | M2–M3: SQL frontend + single-table executor + aggregates/sort/DISTINCT + CLI output | done | per PLAN.md: 45 tests |
| 3 | M4–M5: hash join, optimizer, README, benchmark, polish | done | per PLAN.md: 51 tests, 1M-row bench |
| 4 | Independent code review of full codebase; fix findings | done | per PLAN.md: 4 critical + 3 major fixed, 58 tests |
| 5 | Persistence: on-disk columnar format (RLE/dictionary/bitpack), `.save`/`.open` | done | per PLAN.md: 64 tests, 1M-row save/load bench |
| 6 | Subqueries + CTEs: derived tables, scalar/IN/EXISTS (uncorrelated), WITH | done | per PLAN.md: 72 tests |
| 7 | DATE/TIMESTAMP types: storage, literals, arithmetic, date functions, CSV + persist | done | per PLAN.md: 81 tests |
| 8 | Window functions: ranking + aggregates OVER, PARTITION/ORDER/ROWS frames | done | per PLAN.md: 86 tests |
| 9 | UPDATE/DELETE: physical DML, simultaneous SET, statement-atomic | done | per PLAN.md: 92 tests |
| 10 | Transactions: BEGIN/COMMIT/ROLLBACK via whole-store snapshots | done | per PLAN.md: 95 tests |
| 11 | SQL completeness: UNION, views, CAST, COALESCE, LIKE, DROP | done | per PLAN.md: 98 tests |
| 12 | Parallel execution (rayon) + multi-writer Database/Connection | done | per PLAN.md: 103 tests, 1M-row bench |
| 13 | Differential testing vs DuckDB (bench/duckdiff.py) | done | per PLAN.md: 53 queries match |
| 14 | Speed round: Arc storage, borrowed codecs, FNV hashing, COUNT(*) path | done | per PLAN.md: duckdiff 53/53, 1M-row bench |
| 15 | Import speed round: streaming inference, buffer-reused records, zero-alloc bool checks | done | per PLAN.md: duckdiff 53/53, 1M-row import 0.42→0.03 s |
| 16 | CASE expressions (searched + simple, NULL semantics, type unification) | done | per PLAN.md: 119 tests, duckdiff 54/54 |
| 17 | Performance tuning: O(n²) window-aggregate fix, typed expression kernels, sort key arena + top-N, join/group hash paths, REPL display cap, thin LTO | done | e96f973 — 148 tests, duckdiff 54/54, 1M-row query bench |
| 18 | Binder fix: UNION inside derived tables/CTEs/views/subqueries; ORDER BY/LIMIT apply to the whole set operation | done | 47c428f — 162 lib + 3 CLI tests, duckdiff 56/56 |
| 19 | Join fast path: integer-key specialization + fused join→aggregate (no row materialization) | done | 773bf10 — 193 lib + 3 CLI tests, duckdiff 56/56, join_self 64→~9 ms |
| 20 | Outer joins: LEFT/RIGHT equi-joins (NULL-extended unmatched rows), join-kind-aware pushdown | done | 4637827 — 208 lib + 3 CLI tests, duckdiff 60/60, bench total 493 ms |
| 21 | PEG parser (`peg` crate) in parallel with recursive descent; differential-tested; default flipped | done | f4ce500 — 223 lib + 3 CLI tests both parser modes, duckdiff 60/60 both modes; docs follow-ups 709ede4, 6dfb8c4 |
| 22 | WAL / crash recovery (core) | done | 253 lib + 3 CLI unit + 9 CLI integration tests; duckdiff 60/60; kill loop 100/100 random + 40/40 targeted clean (dev runs: 600 + 190); bench ≈498 ms |
| 23 | Multi-database: `USE <db>`, qualified `db.table`, per-database persistence (core) | done | 288 lib + 3 CLI unit + 9 crash + 8 multi-db integration tests (both parser modes); duckdiff 60/60; kill loop 30/30 + 30/30 multi-db clean; bench ≈497 ms |
| 24 | Network server: `pinkdb-server` binary + CLI `--host` client mode (separate crate) | done | pinkdb-server 1.84 MB; 366 tests both parser modes; duckdiff 60/60; live E2E (10k-term chain → clean error, server stays up); kill loop 30/30 clean; bench ≈488 ms |
| 25 | At-rest encryption: encrypted `.pink` + WAL (AEAD + Argon2id, key via option/env) (core `encryption` feature) | done | 376 default / 412 with `--features encryption` tests (both parser modes); clippy clean all feature sets; duckdiff 60/60; kill loops 30/30 plain + 30/30 encrypted + 15/15 encrypted multi-db; boundary-truncation nonce-reuse fix (fresh prefix per keyed open) proven by regression; sizes 2.21 MB default / 2.28 MB encrypted / `pinkdb-server` 1.84 MB |
| 26 | C ABI: `pinkdb-ffi` (staticlib + cdylib, C headers, example) (separate crate; contained `unsafe`) | done | 7 FFI unit + 3 integration tests, C example runs; workspace suites both parser modes; duckdiff 60/60; sizes: dylib 1.70 MB, staticlib 15.53 MB, `pinkdb` 2.21 MB unchanged |
| 27 | Python binding (cffi) (`bindings/python`) | done | stdlib ctypes binding (14 unittest tests, both default discovery and PINKDB_FFI_LIB override); all value types + statement kinds; 20k-iteration RSS flat; sizes unchanged (dylib 1.70 MB) |
| 28 | Go binding (cgo) (`bindings/go`) | done | cgo binding over pinkdb-ffi: 15 tests pass + race clean, gofmt/vet clean; durable round-trip; goroutine-safe wrapper; sizes unchanged |
| 29 | Extensions support: loadable extensions (`LOAD`/`INSTALL`), versioned C ABI v1, manifest/signature checks (separate `pinkdb-ext-loader` crate; contained `unsafe`) | done | loader crate + C ABI v1 + SQL LOAD EXTENSION (both parsers) + result-kind ripple (CLI/server v3/C ABI/Python/Go); 432 tests / 25 suites green both parser modes; duckdiff 60/60; kill loop 30/30; wasm core build green; sizes pinkdb 2.25 MB (+1.0%) |
| 30 | C/C++ extension SDK: ABI headers, templates, examples (in-repo `ext-sdk/c`) | done | ext-sdk/c/ (vendored header + byte-identity sync test, template, 3 examples incl. C++, Makefile/CMake); smoke.sh 14/14 checks green through the real CLI; 434 tests; clippy clean |
| 31 | Go extension SDK: build extensions as c-shared libraries (cgo) exporting the ABI (in-repo `ext-sdk/go`) | done | ext-sdk/go/ (cgo c-shared template + example, vendored header pinned by the sync test, README with sharp edges); smoke 15/15 incl. a 5,000-row multi-threaded callback query (3 rayon threads); gofmt/vet clean; 434 tests; clippy clean |
| 32 | Extensions repository: hosted catalog of built extension artifacts (metadata + binaries) (infra service) | done | live at https://pinkdb-ext-repo.defaultcms.workers.dev (Workers + R2 bucket pinkdb-extensions); index.json + immutable artifact streaming; first entry math_ext 0.1.0 darwin-arm64 (sha256 38abc8b3…, 17088 B) verified by verify.sh |
| 33 | Custom repositories: user-configurable extension repository URLs and config (core config + repo client) | done | CLI `repo` feature: config `~/.pinkdb/repositories.json`, catalog client (platform/ABI resolve, atomic download, SHA-256 verify), `.install`/`.repositories`/`.installed`; 7 hermetic fixture-server tests + 24 unit tests + live `math_ext` install verified; default CLI 2.27 MB vs 4.33 MB with the feature |
| 34 | Lake architecture: DuckLake-style decoupled metadata + data storage (catalog, snapshots, data files) (separate crates, storage layer) | done | crates/pink-lake + CLI lake feature: catalog.json + immutable snapshots/<v>.json + per-table data/<table>/<uuid>.pink (single-table engine bytes); snapshot/version/restore API; crash-atomic publication; 465 tests both parser modes, 35 CLI lake tests, clippy clean incl. lake,encryption; process-race harness 8×4/6×4/6×6 consistent; sizes default 2.29 MB / +lake 2.47 MB |
| 35 | Vortex file format as the default lake data format (format crate / extension) | done | vortex 0.88 direct dep in pink-lake (blocking, direct arrays, no arrow on our side); catalog v2 with per-table format; v1 `.pink` lakes readable (checked-in fixture) and migrate on save; bit-exact round-trips incl. NaN payloads/-0.0; temporals stored as raw ints + metadata (native vortex timestamp ext panics on i64 extremes); 1M-row data 3.1 MB vs 14.0 MB `.pink` (4.5× smaller), save 0.20 s vs 0.086 s, load 0.067 s vs 0.108 s; +218 crates, lake binary 44.6 MB (default unchanged 2.29 MB) |
| 35b | Parser cleanup: delete the hand-written recursive-descent parser + lexer and the `PINKDB_PARSER` switch (PEG-only parsing); retain/re-home the shared pieces the PEG grammar and WAL statement-splitting still use; port lexer/parser unit tests to PEG tests; keep the differential corpus as a PEG-only corpus; update README/docs (including the docs website's CLI page) | done | hand-written parser + lexer deleted (2,001 lines; server cross-parser test replaced); Keyword/statement-splitting/nesting-guard re-homed; `PINKDB_PARSER` switch removed; lexer/parser tests + corpus + seeded fuzz ported to PEG-only; 472 tests, clippy clean, duckdiff 60/60, docs CLI page redeployed |
| 36 | S3 bucket support for lake storage and file reading (optional `s3` feature, storage provider) — HOLD: the user requested that this wave not be started automatically after 35b; wait for an explicit go. | held (do not start automatically — user gate) | — |
| 37 | Documentation website (separate site/track) | done | website/ generator (stdlib Python, layout+fragments), 10 pages + CSS, 86 KiB self-contained, extensionless canonical links, wrangler dry-run validated |
| 38 | Host the website and extension repository on Cloudflare Workers + R2 (infra; Workers half done, R2 deferred to the extensions-repository wave 32/39) | done | live at https://pinkdb-docs.defaultcms.workers.dev (Workers static assets; .html links 307→canonical extensionless; 404-page; assets 200) |
| 39 | CI/CD publishing of extensions to the repository (build, test, sign, publish) (infra/CI workflows) | queued | — |
| 40 | Performance tuning (recurring every 5th wave): bench-driven speed work, no new features (core) | queued | — |
| 41 | Primary/foreign keys, Snowflake-style: declarative `PRIMARY KEY` / `FOREIGN KEY` / `UNIQUE` / `NOT NULL` constraints (metadata, not enforced), `RELY` / `NOT ENFORCED` accepted, persisted in the catalog, used by the optimizer (e.g. FK→PK join elimination) | queued | — |
| 42 | Index support: `CREATE INDEX` / `DROP INDEX`, hash indexes for equality lookups + automatic per-chunk zone maps, optimizer index-vs-scan choice, per-database, persisted/WAL-logged | queued | — |
| 43 | Materialized views: `CREATE MATERIALIZED VIEW`, `REFRESH [MATERIALIZED VIEW]`, on-demand full refresh, per-database, persisted + WAL-logged (incremental/auto-refresh deferred) | queued | — |
| 44 | Stored procedures: SQL procedural language (parameters, variables, control flow) + JavaScript procedures (optional embedded engine; WAL-replay determinism must be solved) | queued | — |
| 45 | Performance tuning (recurring every 5th wave): bench-driven speed work against the committed query bench; profile-first, small measured wins (core) | queued | — |
| 46 | Network server TLS: rustls + certificate config, encrypted transport (optional pinkdb-server feature) | queued | — |
| 47 | Async I/O at the I/O boundary: optional async storage layer (object stores / S3 for the lake) and/or an async server variant, feature-gated; requires a documented carve-out to the zero-async design rule for the I/O boundary only (core executor stays sync) | queued | — |
| 48 | RBAC: users + roles with Argon2id password hashes (reusing the encryption crate), `CREATE USER`/`CREATE ROLE`/`GRANT`/`REVOKE` on databases/tables, session login on the network server, per-connection enforcement (embedded API stays trusted), grants persisted in the catalog + WAL | queued | — |
| 49 | Extensions on wasm: support loadable/pluggable extensions for wasm targets — either wasm dynamic-linking (pre-built WASI side modules, per-runtime `dlopen` support) or a host-function registration path (JS/wasm-bindgen scripting surface); native `dlopen` extensions are impossible in the wasm sandbox, so a distinct ABI/loading model is required | queued | — |
| 50 | Performance tuning (recurring every 5th wave): bench-driven speed work (core) | queued | — |
| 51 | Rust extension SDK: `ext-sdk/rust` safe wrapper + registration macros for first-party extensions; ABI v1 additive: a deterministic/volatile function flag (the engine refuses to WAL-log volatile calls, preventing replay divergence) | queued | in-repo `ext-sdk/rust` |
| 52 | Core extension: **json** — `json_extract`, `json_value`, `json_valid`, `json_array_length`, `json_object`, `json_array` (JSON as VARCHAR in v1) | queued | `extensions/json` |
| 53 | Core extension: **string** — `split_part`, trim variants, `replace`, `reverse`, `left`/`right`, `strpos`, `starts_with`/`ends_with`, `regexp_match`/`regexp_extract` | queued | `extensions/string` |
| 54 | Core extension: **math** — `sqrt`, `pow`, `log`/`ln`/`exp`, trig, `floor`/`ceil`, `sign`, `greatest`/`least`, `random` (volatile-flagged) | queued | `extensions/math` |
| 55 | Performance tuning (recurring every 5th wave): bench-driven speed work (core) | queued | core |
| 56 | Core extension: **datetime** — `strftime`, `strptime`, `epoch`/`epoch_ms`, `dayofweek`/`dayofyear`/`week`, `last_day`, `age`, `make_date` (naive timestamps only) | queued | `extensions/datetime` |

Cadence: every fifth wave (40, 45, 50, ...) is a performance-tuning wave — no new features, bench-driven speed work against the committed query bench.

Deferred (not currently scheduled): Parquet read (`pinkdb-parquet`); JS/wasm, Node-native, and
Java bindings.

## Progress log

- 2026-10-11 — Wave 17 (e96f973): performance tuning — O(n²) window-aggregate fix, typed
  expression kernels, sort key arena + top-N, join/group hash paths, REPL display cap, thin
  LTO. 148 tests, duckdiff 54/54, 1M-row query bench.
- 2026-10-11 — Wave 18 (47c428f): UNION binder fix for derived tables/CTEs/views/subqueries;
  set-operation ORDER BY/LIMIT apply to the whole set. 162 lib + 3 CLI tests, duckdiff 56/56.
- 2026-10-12 — Wave 19 (773bf10): join fast path — integer-key specialization + fused
  join→aggregate. 193 lib + 3 CLI tests, duckdiff 56/56, join_self 64→~9 ms.
- 2026-10-12 — Wave 20 (4637827): outer joins (LEFT/RIGHT equi-joins, NULL-extended unmatched
  rows, join-kind-aware pushdown). 208 lib + 3 CLI tests, duckdiff 60/60, bench total 493 ms.
- 2026-10-12 — Wave 21 (f4ce500): PEG parser default (legacy via `PINKDB_PARSER=legacy`),
  differential-tested. 223 lib + 3 CLI tests both modes, duckdiff 60/60 both modes; docs
  follow-ups 709ede4 and 6dfb8c4.
- 2026-10-12 — Wave 22: WAL / crash recovery — WAL v2 generation scheme (sidecar `.pink.wal`,
  roll files `<wal>.<gen>.roll`, recovery publishes before deleting, crash-idempotent); durable
  `.import` checkpoints; `.save` path identity; view-dependent DML refused on durable DBs;
  crash tests + kill-loop harness `bench/kill_loop.py`. 253 lib + 3 CLI unit + 9 CLI
  integration tests, duckdiff 60/60, kill loop 100/100 random + 40/40 targeted clean,
  bench ≈498 ms.
- 2026-10-12 — Wave 23: multi-database — `CREATE DATABASE`/`DROP DATABASE`/`USE` + qualified
  `db.table` in both parsers; per-connection session scoping; `.pink` v2 (v1 back-compat
  fixture); WAL session-prefixed records with the dropped-db replay hole fixed;
  `.databases`/scoped `.tables`/`.schema`/quoted `.import`; view-body CTE isolation;
  multi-db kill-loop mode in `bench/kill_loop.py`. 288 lib + 3 CLI unit + 9 crash + 8
  multi-db integration tests (both parser modes), duckdiff 60/60, kill loop 30/30 + 30/30
  multi-db clean, bench ≈497 ms.
- 2026-10-12 — Wave 24: network server — new `crates/pinkdb-server` (protocol v2 framed
  binary, explicit `BatchDone` terminator, typed values); thread-per-connection zero-async
  server (16 MiB stacks, 128-connection cap, accept-error backoff); panic + poisoned-lock
  isolation; AST-depth guard (limit 64) with iterative free in both parsers; remote CLI via
  optional `client` feature; durable `--file` mode with WAL replay. 366 tests (incl.
  server/durable/remote) both parser modes, duckdiff 60/60, kill loop 30/30 clean, bench
  ≈488 ms; release sizes `pinkdb` 2.19 MB and `pinkdb-server` 1.84 MB.
- 2026-10-12 — Wave 25: at-rest encryption — `PINKENC1` container (AES-256-GCM + Argon2id,
  feature-gated, zero crypto in default builds); encrypted `.pink` + WAL preserving wave-22
  recovery invariants; fresh-nonce-prefix-per-open eliminates the boundary-truncation nonce
  reuse; checkpoint refuses mode/key changes on live stores; `PINKDB_KEY` env (non-UTF8 is a
  hard error); Argon2 work bounds; terminal checkpoint segment. 376 default / 412 with
  `--features encryption` tests (both parser modes); clippy clean all feature sets;
  duckdiff 60/60; kill loops 30/30 plain + 30/30 encrypted + 15/15 encrypted multi-db;
  boundary-truncation nonce-reuse fix proven by regression; sizes 2.21 MB default /
  2.28 MB encrypted / `pinkdb-server` 1.84 MB.
- 2026-10-13 — Wave 26: C ABI — new `pinkdb-ffi` crate (rlib + cdylib + staticlib;
  hand-written `include/pinkdb.h`, ABI v1); opaque handles, `pinkdb_execute` (UTF-8 ptr+len),
  full typed result surface (`pink_value` returned by value, thread-local
  `pinkdb_last_error`); contained `unsafe` (single documented allow), core stays zero-unsafe;
  FFI tests + a runnable C example. 7 FFI unit + 3 integration tests, C example runs;
  workspace suites both parser modes; duckdiff 60/60; sizes: dylib 1.70 MB, staticlib
  15.53 MB, `pinkdb` 2.21 MB unchanged. Built inline by the orchestrator because subagent
  dispatch kept timing out (a recovery, not a design change).
- 2026-10-13 — Wave 27: Python binding — new `bindings/python` package (ctypes over
  `libpinkdb_ffi`; documented deviation from "cffi": zero dependencies, same thin-wrapper
  model); `connect`/`connect_memory`/`Database.execute -> [Result]` with kinds/columns/
  types/rows/affected/name; Python-native values incl. `datetime.date`/`datetime.datetime`,
  `None` NULLs, `-0.0` bit-exact, embedded NULs; `Error` from `pinkdb_last_error`;
  owner-free discipline with leak-checked loops; library discovery via `PINKDB_FFI_LIB` or
  the repo-relative dylib. 14 unittest tests (default discovery and `PINKDB_FFI_LIB`
  override); all value types + statement kinds; 20k-iteration RSS flat; sizes unchanged.
- 2026-10-13 — Binary size: unchanged — `pinkdb` 2.21 MB + `pinkdb-server` 1.84 MB +
  `libpinkdb_ffi.dylib` 1.70 MB (release, after wave 27; pre-wave-17 was 2.26 MB).
- 2026-10-13 — Waves 37–38: documentation website + hosting — new `website/` track: stdlib-only
  Python generator (`build.py`, layout + fragments) rendering 10 pages + CSS, dark/pink
  self-contained output (~86 KiB), canonical extensionless links, generated 404 page; the
  Python binding section is auto-included once `bindings/python` exists. Deployed to
  Cloudflare Workers as `pinkdb-docs` (11 files, ~1.99 s upload; `.html` links 307→canonical
  extensionless); live at https://pinkdb-docs.defaultcms.workers.dev. R2 still queued for the
  extension repository (wave 32/39).
- 2026-10-13 — Wave 28: Go binding — new `bindings/go` package (module
  `github.com/openlearnia/pinkdb/bindings/go`); cgo over `libpinkdb_ffi` with
  ${SRCDIR}-relative link/rpath flags; `Open`/`OpenMemory`/`Close`/`Execute -> []Result`
  (14 kind strings, columns/types/rows/affected/name); native value mapping incl. `time.Time`
  for DATE/TIMESTAMP, bit-exact `-0.0`/NaN/±Inf, embedded-NUL strings; ABI-version guard at
  Open; path validation; `sync.Mutex` + closed-flag making one DB safe to share across
  goroutines; 15 tests incl. pre-epoch dates, errors, durable round-trip, 8×400 concurrent
  executes, 1000-iteration loop; also green under `go test -race`. 15 tests pass + race clean,
  gofmt/vet clean; durable round-trip; goroutine-safe wrapper; sizes unchanged.
- 2026-10-13 — Binary size: unchanged — `pinkdb` 2.21 MB + `pinkdb-server` 1.84 MB +
  `libpinkdb_ffi.dylib` 1.70 MB (release, after wave 28; pre-wave-17 was 2.26 MB).
- 2026-10-13 — Wave 29: extensions support — new `crates/pinkdb-ext-loader` (dlopen via
  libloading; versioned `PinkdbHostApi`; `include/pinkdb_extension.h` value ABI with
  alloc/free ownership; thread-safety contract) + dynamic scalar functions in
  binder/executor/optimizer; SQL `LOAD EXTENSION '<path>'` in both parsers;
  `QueryResult::LoadedExtension` rippled through CLI/server protocol v3/C ABI (kind 14)/
  Python/Go; parse-time NUL-identifier rejection; wasm target-gating with a clean stub.
  Post-review fixes: C1 checkpoint+extension-use brick fixed by touched-only
  `LOAD EXTENSION` prefixing in the WAL (bind-time tracking, tracked-set reset at
  checkpoint; verified end-to-end: checkpoint then ext-using INSERT reopens and replays),
  H1 enclosed dynamic calls no longer constant-folded (`contains_dynamic_scalar` guard;
  per-row semantics, 0 calls on empty input, 1 invocation for erroring calls), H2 NULL
  callbacks rejected at registration instead of SIGSEGV, impure-predicate pushdown refused,
  PEG/legacy NUL error parity, header/offset/ownership-edge tests. 432 tests / 25 suites
  green both parser modes; duckdiff 60/60; kill loop 30/30; wasm core build green; sizes
  `pinkdb` 2.25 MB (+1.0%).
- 2026-10-13 — Binary size: `pinkdb` 2.25 MB + `pinkdb-server` 1.90 MB +
  `libpinkdb_ffi.dylib` 1.76 MB (release, after wave 29; pre-wave-17 was 2.26 MB).
- 2026-10-14 — Wave 30: C/C++ extension SDK — new `ext-sdk/c/` with a vendored
  `pinkdb_extension.h` (byte-identity test in
  `crates/pinkdb-ext-loader/tests/header_sync.rs`, verified to bite), a commented template
  (Makefile + CMake), three warning-free examples (`math_ext.c`, `string_ext.c` with VARCHAR
  alloc ownership, `cpp_ext.cpp` with layout static_asserts), an SDK README documenting the
  ABI contract/ownership/thread-safety/LOAD usage, and `smoke.sh` (builds, loads all three
  via `LOAD EXTENSION`, 14/14 checks incl. NULLs, multibyte VARCHAR, overflow error paths).
  434 tests; clippy clean.
- 2026-10-14 — Binary size: unchanged — `pinkdb` 2.25 MB + `pinkdb-server` 1.90 MB +
  `libpinkdb_ffi.dylib` 1.76 MB (release, after wave 30; pre-wave-17 was 2.26 MB).
- 2026-10-14 — Wave 31: Go extension SDK — new `ext-sdk/go/` with a nested-module template
  and a worked example (`ext_go_repeat`, `ext_go_len`), cgo patterns (exact-name `//export`ed
  ABI symbols, `static inline` function-pointer shims, host-`alloc` VARCHAR results,
  recover-in-every-export), a vendored header pinned by the loader's `header_sync` test, and
  sharp edges documented (Go runtime in the host process, foreign-thread callbacks, no
  panics, `-buildmode=c-shared` rules). `smoke.sh` 15/15 incl. NULLs, multibyte VARCHAR,
  error paths, and a 5,000-row parallel query. Also fixed a flaky test
  (`wal_replay_reloads_the_extension`): its "deleted file at replay" simulation used
  `fs::copy`, whose inherited macOS install name made dyld alias the already-loaded original
  under parallel tests; the copy is now a fresh compile with its own install name (15/15
  clean after the fix). Numbers: smoke 15/15 incl. a 5,000-row multi-threaded callback query
  (3 rayon threads); gofmt/vet clean; 434 tests; clippy clean.
- 2026-10-14 — Binary size: unchanged — `pinkdb` 2.25 MB + `pinkdb-server` 1.90 MB +
  `libpinkdb_ffi.dylib` 1.76 MB (release, after wave 31; pre-wave-17 was 2.26 MB).
- 2026-10-14 — Wave 32: extensions repository — new `ext-repo/` track: format spec v1
  (`index.json` with name/version/abi/platform/file/sha256; immutable coordinates; reserved
  platform tags), dependency-free ES-module Worker bound to R2 (info page, cached
  `GET /index.json` with ETag/304, artifact streaming with immutable cache, 404s, no write
  API), stdlib `publish.py`/`publish.sh` (sha256, coordinate immutability, artifact-then-index
  uploads via `wrangler r2 object put --remote`), `verify.sh` (download + hash check).
  Deployed for real: bucket `pinkdb-extensions`, worker `pinkdb-ext-repo`, first artifact =
  the wave-30 `math_ext` C example; live curl + verify.sh confirmed. Live at
  https://pinkdb-ext-repo.defaultcms.workers.dev; index.json + immutable artifact streaming;
  first entry `math_ext` 0.1.0 darwin-arm64 (sha256 38abc8b3…, 17088 B) verified by verify.sh.
- 2026-10-14 — Binary size: unchanged — `pinkdb` 2.25 MB + `pinkdb-server` 1.90 MB +
  `libpinkdb_ffi.dylib` 1.76 MB (release, after wave 32; pre-wave-17 was 2.26 MB).
- 2026-10-14 — Wave 33: custom repositories + client-side INSTALL — `crates/pinkdb-cli` `repo`
  feature (ureq/rustls + serde_json + sha2, feature-gated so the default binary stays lean,
  +1.96 MiB with it); `~/.pinkdb/repositories.json` ordered list with atomic writes and URL
  validation; client fetches the catalog with format-version/ABI/platform checks, resolves
  `name[@version]` (first repo wins, highest version), downloads atomically and verifies
  SHA-256, reuses verified cache, replaces corrupt copies; `.install`,
  `.repositories list|add|remove`, `.installed` dot-commands (feature-aware help; clear errors
  without the feature); durable-WAL replay picks up the stable cache path via the existing
  touched-only `LOAD` prefixing. 7 hermetic fixture-server tests + 24 unit tests; live install
  of the official `math_ext` verified; default CLI 2.27 MB vs 4.33 MB with the feature.
- 2026-10-14 — Binary size: `pinkdb` 2.27 MB (4.33 MB with `--features repo`) + `pinkdb-server`
  1.90 MB + `libpinkdb_ffi.dylib` 1.76 MB (release, after wave 33; pre-wave-17 was 2.26 MB).
- 2026-10-14 — Wave 34: pink-lake minimal lake layer — new `crates/pink-lake` + CLI `lake`
  feature, non-default: option 1 — own catalog + own columnar data files (`serde_json`
  crate-local; no engine changes); layout `catalog.json` + immutable `snapshots/<v>.json`
  + per-table `data/<table>/<uuid>.pink` (single-table engine bytes, bit-exact); flat
  `db.table` keys; atomic publish order (data → snapshot → catalog); full-snapshot
  versioning; snapshot/version/restore API; `.lake save/open/versions/tables/info`.
  Post-review fixes: M1 version-overflow (validated 1..=u64::MAX-1, `checked_add`, clean
  errors; release-mode proof), M2 concurrent saves (unique `create_new` tmp files, hard-link
  snapshot publication with no replace, catalog re-read conflict detection → `write
  conflict … reopen the lake`; process-race harness green), M3 `.lake open` retains the
  session `PINKDB_KEY` (encrypted `.save` after lake open) with a plaintext-lake note,
  M4 durability wording precise + parent-dir fsyncs (process-crash safe; APFS power-loss
  not guaranteed by fsync alone), plus minors (no snapshot replacement, required `tables`,
  canonical `versions()`, validate-before-create, missing-file warnings, real error paths,
  symlink-resistant tmp). 465 tests both parser modes, 35 CLI lake tests, clippy clean
  incl. lake,encryption; process-race harness 8×4/6×4/6×6 consistent; sizes default
  2.29 MB / +lake 2.47 MB.
- 2026-10-14 — Binary size: `pinkdb` 2.29 MB (2.47 MB with `--features lake`) +
  `pinkdb-server` 1.90 MB + `libpinkdb_ffi.dylib` 1.76 MB (release, after wave 34;
  pre-wave-17 was 2.26 MB).
- 2026-10-14 — Waves 35 + 35b: Vortex default lake format + parser cleanup — `pink-lake`
  now takes `vortex` 0.88 as a direct dependency (blocking API, direct arrays, no arrow
  dependency on our side); catalog v2 records the per-table format; v1 `.pink` lakes stay
  readable (checked-in fixture) and migrate on save; round-trips are bit-exact incl. NaN
  payloads and `-0.0`; temporals are stored as raw ints + metadata (the native vortex
  timestamp extension panics on i64 extremes); 1M-row data 3.1 MB vs 14.0 MB `.pink`
  (4.5× smaller), save 0.20 s vs 0.086 s, load 0.067 s vs 0.108 s; +218 crates, lake binary
  44.6 MB (default unchanged 2.29 MB). Parser cleanup: the hand-written parser + lexer are
  deleted (2,001 lines; the server cross-parser test replaced); Keyword/statement-splitting/
  nesting-guard re-homed; the `PINKDB_PARSER` switch is removed; lexer/parser tests + corpus
  + seeded fuzz ported to PEG-only; 472 tests, clippy clean, duckdiff 60/60, docs CLI page
  redeployed. The env switch and the "both parser modes" gate convention are gone — PEG +
  corpus + fuzz are the only parser path.
- 2026-10-14 — Binary size: `pinkdb` 2.29 MB (44.6 MB with `--features lake` after the Vortex
  dep; default unchanged) + `pinkdb-server` 1.90 MB + `libpinkdb_ffi.dylib` 1.76 MB (release,
  after waves 35/35b; pre-wave-17 was 2.26 MB).
