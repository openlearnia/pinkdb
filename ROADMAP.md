# pinkdb Roadmap

Single source of truth for wave status; update after every wave. Known gaps in done waves: [docs/GAPS.md](docs/GAPS.md).

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
| 29 | Extensions support: loadable extensions (`LOAD`/`INSTALL`), versioned C ABI v1, manifest/signature checks (separate `pinkdb-ext-loader` crate; contained `unsafe`) — *as built: `LOAD EXTENSION` + ABI-version/symbol checks only; SQL `INSTALL` and signatures moved to wave 40i (docs/GAPS.md)* | done | loader crate + C ABI v1 + SQL LOAD EXTENSION (both parsers) + result-kind ripple (CLI/server v3/C ABI/Python/Go); 432 tests / 25 suites green both parser modes; duckdiff 60/60; kill loop 30/30; wasm core build green; sizes pinkdb 2.25 MB (+1.0%) |
| 30 | C/C++ extension SDK: ABI headers, templates, examples (in-repo `ext-sdk/c`) | done | ext-sdk/c/ (vendored header + byte-identity sync test, template, 3 examples incl. C++, Makefile/CMake); smoke.sh 14/14 checks green through the real CLI; 434 tests; clippy clean |
| 31 | Go extension SDK: build extensions as c-shared libraries (cgo) exporting the ABI (in-repo `ext-sdk/go`) | done | ext-sdk/go/ (cgo c-shared template + example, vendored header pinned by the sync test, README with sharp edges); smoke 15/15 incl. a 5,000-row multi-threaded callback query (3 rayon threads); gofmt/vet clean; 434 tests; clippy clean |
| 32 | Extensions repository: hosted catalog of built extension artifacts (metadata + binaries) (infra service) | done | live at https://pinkdb-ext-repo.defaultcms.workers.dev (Workers + R2 bucket pinkdb-extensions); index.json + immutable artifact streaming; first entry math_ext 0.1.0 darwin-arm64 (sha256 38abc8b3…, 17088 B) verified by verify.sh |
| 33 | Custom repositories: user-configurable extension repository URLs and config (core config + repo client) | done | CLI `repo` feature: config `~/.pinkdb/repositories.json`, catalog client (platform/ABI resolve, atomic download, SHA-256 verify), `.install`/`.repositories`/`.installed`; 7 hermetic fixture-server tests + 24 unit tests + live `math_ext` install verified; default CLI 2.27 MB vs 4.33 MB with the feature |
| 34 | Lake architecture: DuckLake-style decoupled metadata + data storage (catalog, snapshots, data files) (separate crates, storage layer) | done | crates/pink-lake + CLI lake feature: catalog.json + immutable snapshots/<v>.json + per-table data/<table>/<uuid>.pink (single-table engine bytes); snapshot/version/restore API; crash-atomic publication; 465 tests both parser modes, 35 CLI lake tests, clippy clean incl. lake,encryption; process-race harness 8×4/6×4/6×6 consistent; sizes default 2.29 MB / +lake 2.47 MB |
| 35 | Vortex file format as the default lake data format (format crate / extension) | done | vortex 0.88 direct dep in pink-lake (blocking, direct arrays, no arrow on our side); catalog v2 with per-table format; v1 `.pink` lakes readable (checked-in fixture) and migrate on save; bit-exact round-trips incl. NaN payloads/-0.0; temporals stored as raw ints + metadata (native vortex timestamp ext panics on i64 extremes); 1M-row data 3.1 MB vs 14.0 MB `.pink` (4.5× smaller), save 0.20 s vs 0.086 s, load 0.067 s vs 0.108 s; +218 crates, lake binary 44.6 MB (default unchanged 2.29 MB) ; post-review C1: tampered .vortex files could panic inside the vortex decoder (~0.5% of single-byte flips) — the full decode boundary is now catch_unwind-guarded and surfaces Corrupt errors naming the file, with a deterministic bit-flip sweep in debug and release plus a CLI tamper test; corrupt-input gate + crafted pinkdb.table cases added (472 → 487 tests); docs corrected (decoder panics caught; workspace builds compile vortex while the feature-less CLI binary stays vortex-free) |
| 35b | Parser cleanup: delete the hand-written recursive-descent parser + lexer and the `PINKDB_PARSER` switch (PEG-only parsing); retain/re-home the shared pieces the PEG grammar and WAL statement-splitting still use; port lexer/parser unit tests to PEG tests; keep the differential corpus as a PEG-only corpus; update README/docs (including the docs website's CLI page) | done | hand-written parser + lexer deleted (2,001 lines; server cross-parser test replaced); Keyword/statement-splitting/nesting-guard re-homed; `PINKDB_PARSER` switch removed; lexer/parser tests + corpus + seeded fuzz ported to PEG-only; 472 tests, clippy clean, duckdiff 60/60, docs CLI page redeployed |
| 36 | S3 bucket support for lake storage and file reading (optional `s3` feature, storage provider) | done | pink-lake `Storage` trait + `s3` feature (hand-rolled SigV4 over ureq/hmac/sha2, AWS + R2 path/virtual-hosted, conditional writes: create-only snapshots + `If-Match` catalog CAS with conflict errors, documented `501`→check-then-put degradation); mock-S3 test server + AWS SigV4 vectors + 3×3 race; CLI `s3://` URLs; default CLI byte-identical, +1.96 MB with `lake,s3`; 490 default tests, 79 pink-lake `s3` tests, clippy clean ; multi-server real-S3 harness (SERVER=s3mock|seaweedfs|rustfs): SeaweedFS and RustFS are strict witnesses (verify SigV4; conditional PUTs atomic under 2-writer races — 0/20 double-200s) so one-winner-per-version and catalog==snapshot bytes are HARD-enforced there; s3mock stays lenient (documented non-atomic conditionals); SeaweedFS's default config is anonymous (probed+reported; the harness starts an identity-configured container for the enforced run); RustFS needs a 4M-row slow-vs-quick window; MinIO images currently unpullable; recipes + capability matrix in README/s3_docker_notes.md |
| 37 | Documentation website (separate site/track) | done | website/ generator (stdlib Python, layout+fragments), 10 pages + CSS, 86 KiB self-contained, extensionless canonical links, wrangler dry-run validated |
| 38 | Host the website and extension repository on Cloudflare Workers + R2 (infra; Workers half done, R2 deferred to the extensions-repository wave 32/39) | done | live at https://pinkdb-docs.defaultcms.workers.dev (Workers static assets; .html links 307→canonical extensionless; 404-page; assets 200) |
| 39 | CI/CD publishing of extensions to the repository (build, test, sign, publish) (infra/CI workflows) — *as built: signing is a documented hook only (SHA-256 checksums); signatures moved to wave 40i* | done | three GitHub Actions workflows (.github/workflows/{ci,publish-extensions,deploy-docs}.yml): CI build/test/clippy + feature matrices + wasm + duckdiff + Python/Go bindings + C/Go SDK smokes + Docker S3 job; manual-dispatch extension publishing to the live R2 repo (dup refusal, checksums today, signing hook documented); docs auto-deploy; actionlint-clean, YAML-parsed, steps executed locally where possible; secrets documented (CLOUDFLARE_API_TOKEN scopes + account id) |
| 40 | Performance tuning (recurring every 5th wave): bench-driven speed work, no new features (core) | done | profile-driven: parallel vortex writes (2-2.7×, byte-identical), typed aggregate kernels + count-filter fast path (filter 50→24 ms @10M), window key rework (95→61 ms @1M); standing battery 493→447 ms; 494 tests, duckdiff 60/60; group-by @10M target missed (74 vs ≤60, parallel-ceiling analysis + breadcrumb); next-round breadcrumbs recorded |
| 40a | Hardening — safety (docs/GAPS.md P0 #1–#10): server-side `LOAD EXTENSION` opt-in, publish catalog-wipe guard, store-wide table versions, read-set COMMIT validation, bounded `.pink` decoder + v3 CRC trailer, DuckDB-style overflow errors, CLI lake handle baselines, https-only `.install`, redacted S3 secrets, bounded server frames + idle timeout | done | 502 workspace tests (+8 regression tests, each shown to fail without its fix where the platform allows); feature matrix green (pink-lake s3 106, cli lake,s3 44, cli encryption 27, core encryption 352, cli client 27, cli repo 54); clippy -D warnings clean all feature sets; duckdiff 60/60; kill loops 30/30 plain + 30/30 multi-db + 20/20 targeted + 30/30 encrypted; bench parity vs main interleaved (418–422 vs 419–425 ms); sizes `pinkdb` 2.14 MB (unchanged) / `pinkdb-server` 1.80 MB (+0.02) |
| 40b | Hardening — durability/operability: auto-checkpoint + `CHECKPOINT`, `.pink` file lock, fail-stop after a WAL write failure, single-lock bind+run, Python handle lock, CLI exit codes/stderr/`-c`/`.read`/EOF/`;` scanning, S3 retries, honest `versions()`, `.lake` transaction/durable guards | done | 512 workspace tests; feature matrix green (pink-lake s3 108, cli lake,s3 51, cli encryption 33, core encryption 356, cli client 33, cli repo 60); Python 37 + Go (-race) green; clippy -D warnings clean all feature sets; duckdiff 60/60; kill loops 30/30 + 30/30 multi-db + 20/20 targeted + 30/30 encrypted; C/Go SDK smokes 14/14 + 15/15; bench parity (421–424 vs 418–424 ms); sizes `pinkdb` 2.15 / `pinkdb-server` 1.83 / dylib 1.68 MB |
| 40c | Hardening — CI + docs truthfulness: macOS CI + fmt/audit/feature-test gaps, website refresh, committed-binary/account-id cleanup, wave 29/39 claims corrected | done | CI: fmt gate (after a one-time mechanical `cargo fmt`), feature tests for encryption/client/repo, clippy per feature set, kill loops, macOS job, MSRV job (rust-version 1.89 core / 1.95 pink-lake), `cargo audit`, docs build + roadmap-sync check, Go vet/-race, C example; all actions pinned to commit SHAs, wrangler pinned (4.151.0); `pinkdb.h` ↔ Rust header-sync test (shown to fail on a planted drift); website refreshed (parser, protocol v4, TLS/RBAC wave numbers, scripting, locking) with the roadmap page generated from ROADMAP.md; committed `hello` binary removed + ignored; account id removed from README; 514 tests; actionlint clean. Only GitHub can verify: the new jobs' runner execution (macOS, MSRV 1.89, cargo audit) |
| 40d | SQL completeness: multi-way/composite/non-equi/CROSS/FULL joins, INSERT…SELECT, CTAS, IF [NOT] EXISTS, OFFSET, NULLS FIRST/LAST, INTERSECT/EXCEPT, COUNT(DISTINCT), EXPLAIN, SHOW/DESCRIBE, RANGE frames + frame validation | queued | — |
| 40e | Prepared statements + parameters: placeholders, bind API, protocol v4, FFI prepare/bind, Python DB-API, Go database/sql | queued | — |
| 40f | Functions + types: lag/lead/ntile/first/last_value, core scalar gaps, DECIMAL, INTERVAL | queued | — |
| 40g | Optimizer: projection pushdown, LIMIT short-circuit, join build-side choice, filter through aggregate | queued | — |
| 40h | Lake: incremental saves, `.lake gc`, `lake_scan` SQL access + time travel, bulk typed decode, encode panic guard | queued | — |
| 40i | Extension trust + distribution: Ed25519 artifact signatures, SQL `INSTALL`, Linux artifacts, Python wheel, standalone Go module | queued | — |
| 40j | Test depth: window/plan unit tests, WAL fuzz, persist property tests, duckdiff gated in CI | queued | — |
| 41 | Primary/foreign keys, Snowflake-style: declarative `PRIMARY KEY` / `FOREIGN KEY` / `UNIQUE` / `NOT NULL` constraints (metadata, not enforced), `RELY` / `NOT ENFORCED` accepted, persisted in the catalog, used by the optimizer (e.g. FK→PK join elimination) | queued | — |
| 42 | Index support: `CREATE INDEX` / `DROP INDEX`, hash indexes for equality lookups + automatic per-chunk zone maps, optimizer index-vs-scan choice, per-database, persisted/WAL-logged | queued | — |
| 43 | Materialized views: `CREATE MATERIALIZED VIEW`, `REFRESH [MATERIALIZED VIEW]`, on-demand full refresh, per-database, persisted + WAL-logged (incremental/auto-refresh deferred) | queued | — |
| 44 | Stored procedures: SQL procedural language (parameters, variables, control flow) + JavaScript procedures (optional embedded engine; WAL-replay determinism must be solved) | queued | — |
| 45 | Performance tuning (recurring every 5th wave): bench-driven speed work against the committed query bench; profile-first, small measured wins (core) — *pulled forward after 40c; byte-neutral part done, layout-changing save work (bigger write units, stats trim) pending a format decision* | in progress | 4b53844, 70ab514, 0deea0e — @10M interleaved vs pre-wave: import 274–282 → 173–177 ms, `.lake open` 618–620 → 147–157 ms (≈4×), store teardown 93 → 16–24 ms, group_region 68–90 → 33–52 ms (≤60 target met; DuckLake 37–55), save flat cross-binary (−11% in-process A/B; the rest is Vortex's own writer pipeline); lake files byte-identical (10M sha256 unchanged); 1M battery 428 → 415–421 ms; 515 tests, duckdiff 60/60 + 64/64 big |
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
  corpus + fuzz are the only parser path. Review fix (35): the .vortex decode path catches
  decoder panics → Corrupt; release-mode corrupt sweep green.
- 2026-10-14 — Binary size: `pinkdb` 2.29 MB (44.6 MB with `--features lake` after the Vortex
  dep; default unchanged) + `pinkdb-server` 1.90 MB + `libpinkdb_ffi.dylib` 1.76 MB (release,
  after waves 35/35b; pre-wave-17 was 2.26 MB).
- 2026-10-14 — Wave 36: S3 bucket support for lake storage + file reading — `pink-lake` gains
  a `Storage` trait with an optional `s3` feature: hand-rolled SigV4 signing over
  `ureq`/`hmac`/`sha2` (no aws-sdk, no tokio — stays blocking/sync), AWS + R2 with path-style
  and virtual-hosted addressing, and conditional writes (create-only snapshots, `If-Match`
  catalog CAS surfacing conflict errors, documented `501`→check-then-put degradation); a
  mock-S3 test server + AWS SigV4 test vectors + a 3×3 race; the CLI accepts `s3://` lake
  URLs; an R2 cookbook in the README; documented limits (no multipart uploads, no object GC);
  the live S3 test is gated by `PINKDB_S3_LIVE`. Default CLI byte-identical (+1.96 MB with
  `lake,s3`); 490 default tests, 79 pink-lake `s3` tests, clippy clean. ; post-review
  M2: the committed mock now verifies SigV4 for real (recomputes the canonical request + HMAC
  chain from received bytes, body-hash + clock checks; hand-rolled std-only SHA-256/HMAC with
  FIPS/RFC vectors) so a signing regression cannot hide; M1: endpoints that reject conditional
  writes now REFUSE writes by default (UnsupportedEndpoint; opt-in PINKDB_S3_ALLOW_DEGRADED=1 keeps
  a single-writer fallback with read-back verification + catalog/snapshot reconciliation, race
  window documented); minors: client-side >5 GiB PUT refusal, timeout/no-retry docs, NoSuchBucket
  mapping, session-token scoping, sticky-flag scoping, symlink-safe temp copies, http:// opt-in
  (PINKDB_S3_ALLOW_HTTP), paging cap, bucket-rule alignment; 493 default / 104 pink-lake s3 tests ;
  Docker-based real-S3 test harness (`crates/pink-lake/tests/s3_docker.sh`, s3mock): full
  `s3://` roundtrip through the CLI (typed CSV, fresh-process open, byte-exact values),
  concurrency races with catalog==snapshot byte checks, refusals, container lifecycle/KEEP;
  discovered+documented that s3mock's conditional PUTs are NOT atomic under concurrency
  (check-then-act; 60/60 duplicate 200s) — the harness reports that as a server limitation
  and enforces one-winner only where the server can (plus a deterministic same-version
  conflict test); AWS/R2 remain the atomic check via `PINKDB_S3_LIVE=1`; MinIO images now
  unpullable without auth (documented)
- 2026-10-14 — Binary size: `pinkdb` 2.22 MB default; 44.6 MB with `--features lake`; 46.5 MB
  with `lake,s3` (release, after wave 36; pre-wave-17 was 2.26 MB).
- 2026-10-14 — Wave 39: CI/CD publishing of extensions — three GitHub Actions workflows
  (`.github/workflows/ci.yml`, `publish-extensions.yml`, `deploy-docs.yml`): CI build/test/
  clippy + feature matrices + wasm + duckdiff + Python/Go bindings + C/Go SDK smokes + Docker S3
  job; manual-dispatch extension publishing to the live R2 repo (duplicate refusal, checksums
  today, signing hook documented); docs auto-deploy. actionlint-clean, YAML-parsed, steps
  executed locally where possible; secrets documented (`CLOUDFLARE_API_TOKEN` scopes +
  account id). Can only be verified on GitHub: real runner action execution
  (checkout/setup/matrix scheduling), real R2 uploads, and `docker pull` in CI.
- 2026-10-14 — Binary size: unchanged — no crate changes this wave (`pinkdb` 2.22 MB default /
  44.6 MB with `--features lake` / 46.5 MB with `lake,s3` + `pinkdb-server` 1.90 MB +
  `libpinkdb_ffi.dylib` 1.76 MB, release, after wave 39; pre-wave-17 was 2.26 MB).
- 2026-10-14 — Wave 40 (de1dcc7): performance tuning — profile-driven round, no new features.
  Standing 1M battery (`cargo run --release -p pinkdb --example query_bench`, best-of-5;
  measured run: TOTAL 447 ms paired / 421 ms quiet): filter_num 5.5→2.4,
  window_row_number 95→61, window_count_star 80→65, group_region 9.2→7.0, order_num_topn
  3.4→2.8, order_str 42→39; join entries flat (10.3/17.3/28.1/25.8); TOTAL 493→447.
  `.lake save` writes Vortex byte-identically (sha256 unchanged) but now through a driven
  worker pool: 1M 0.180→0.088 s, 10M 1.562→0.76–1.22 s (2.0–2.7×) — the vortex
  compression cascade ran inline on the main thread because `CurrentThreadRuntime` does not
  drive `spawn_cpu`; fixed by a pool that does. 10M in-memory queries (interleaved):
  filter 50→23–25 ms (target ≤30 ✓), order-by 36→24–32 ms, group-by 100→68–74 ms (target
  ≤60 ✗ — 444 ms single-thread at the measured 6.0× ceiling of a 4P+6E M4; merge is 3 ms).
  vs DuckLake at 10M: save 0.76 vs 0.37 s, filter ~equal, group-by still ~1.7×. Semantics
  pinned: 494 tests (one new byte-stability test for parallel writes), duckdiff 60/60,
  clippy clean; default binary +1.0% (2.24 MB); typed kernels preserve wrapping sums / NULL
  skips / NaN rules exactly. Breadcrumbs for the next perf round: SoA accumulators for the
  parallel group-by ceiling; bulk typed lake decode (no per-row push/`from_utf8`) for the
  ~0.63 s 10M `.lake open`.
- 2026-10-14 — Wave 40a: safety hardening from the waves 0–40 gap audit (docs/GAPS.md P0).
  Server: SQL `LOAD EXTENSION` refused for remote clients unless `--allow-load-extension`
  (library `Server::allow_load_extension`), idle socket timeout (default 300 s,
  `--idle-timeout`), `--max-connections`, frame bodies buffered as they arrive (never
  pre-sized from the prefix). Publishing: the workflow fails on any catalog fetch error
  except 404, and `publish.py` refuses to upload an index missing live entries. Engine:
  table versions drawn from a store-wide counter (DROP+CREATE can no longer alias a
  version); writing transactions validate their read set at COMMIT so WAL replay always
  matches the session; `.pink` v3 = v2 + CRC-32 trailer (v1/v2 still load) and chunk-sized
  counts capped at CHUNK_SIZE before allocating; integer `+ - *`, negation, `abs`,
  `date ± n` and narrowing casts error on overflow like DuckDB (DOUBLE→int casts round
  half away from zero), `SUM` is exact via an i64 + wrap-counter accumulator and errors
  only when the final value leaves BIGINT; the fused join→aggregate totality check now
  treats integer arithmetic as fallible. CLI: per-lake session baselines turn a stale
  `.lake save` into a conflict (`force` overrides); repositories accept plain `http://`
  only on loopback (or `PINKDB_REPO_ALLOW_HTTP=1`), `https://` agents are https-only
  through redirects, plain-http agents follow none. Lake: `S3Config`/`Credentials`
  `Debug` redact secrets. 502 tests, feature matrix + clippy green, duckdiff 60/60, kill
  loops clean, bench at parity (the first i128 SUM accumulator cost ~7% on group_id via
  16-byte alignment; the wrap-counter design recovers it).
- 2026-10-14 — Wave 40b: durability and operability (docs/GAPS.md P1). Engine: SQL
  `CHECKPOINT` (protocol v4 `Checkpointed`, FFI kind 15, Python/Go `checkpointed`) and an
  automatic checkpoint once the WAL passes 64 MiB (`set_auto_checkpoint_bytes`); an
  exclusive advisory lock on `<file>.pink.lock` gives a durable database one live handle
  (OS-released on exit/crash; the CLI releases its own before re-opening the bound path);
  a failed WAL append is fail-stop — every later statement and save is refused until
  reopen, so the non-durable change is never observed; autocommit statements bind and run
  under one lock (writes rebind under the write lock). Found and fixed during the wave: a
  first fail-stop check inside `to_bytes` re-locked the WAL mutex a checkpoint already
  held (deadlock, caught by the new CHECKPOINT test before commit). Bindings: the Python
  `Database` serializes calls with a lock (ctypes drops the GIL); FFI/Python docs now say
  to share one handle. CLI: sqlite3/duckdb scripting conventions — errors on stderr, exit
  1 after any failure in non-interactive runs, banner/timing only on a TTY (`.timer`),
  `-c`, `.read`, unterminated final statements run, `;` detection that understands
  strings and comments; test helpers merge stdout+stderr in order; duckdiff/kill_loop/
  lake_bench/SDK smokes updated. Lake: S3 retries with jittered backoff (429/503 always;
  500/502/504/transport only for idempotent requests — never an ambiguous conditional
  PUT; `PINKDB_S3_RETRIES`), `versions()` reports unreadable snapshots instead of hiding
  them, `.lake save`/`.lake open` refused inside a transaction and `.lake open` refuses to
  detach a durable session; repo downloads use `create_new` temp files. 512 tests, gates
  green, bench at parity.
- 2026-10-14 — Wave 40c: CI + docs truthfulness. A mechanical `cargo fmt --all` commit
  enables a `cargo fmt --check` gate; CI gains the missing feature tests (core
  `encryption`, CLI `client`/`repo`), clippy per feature set, the kill loops, a macOS
  job, an MSRV job (crates now declare `rust-version`: 1.89 core for std file locking,
  1.95 for pink-lake because vortex requires it), `cargo audit`, a docs job (site build +
  roadmap-page sync check), `gofmt`/`go vet`/`go test -race`, and the C ABI example;
  every action is pinned to a commit SHA and wrangler to an exact version. New
  `pinkdb-ffi` header-sync test keeps `pinkdb.h` enumerators and declarations equal to
  the Rust exports. Website: stale wave/parser/protocol/TLS-RBAC claims fixed, CLI
  scripting and durable-locking docs added, and `website/tools/sync_roadmap.py`
  regenerates the roadmap tables from ROADMAP.md. Hygiene: the committed
  `crates/pinkdb-ffi/hello` Mach-O binary is removed and ignored; the Cloudflare account
  id is gone from the README (it remains in git history — it is an identifier, not a
  credential); waves 29/39 rows now say what was actually built. 514 tests; actionlint
  clean; GitHub-only verification pending for the new runner jobs.
- 2026-10-15 — Wave 45 (pulled forward), byte-neutral part: profile-driven (macOS `sample`).
  Ingest: `append_chunk` moves a full batch in behind a full tail instead of copying it
  row by row (the serial "store append" of import and lake load); lake save streams
  parallel-converted windows into Vortex's push writer and straight into the temp file
  (word-wise validity), byte-identical output; lake open drains the scan then decodes
  chunks in parallel with bulk column builds (`Vector::from_raw`). The save profile shows
  the remaining time is Vortex's sequential layout pipeline + a serial file-stats pass on
  the writer thread — only layout-changing options (bigger row blocks + loader splitting,
  stats trim) move it further. Query side: two-pass hash aggregation (group ids first,
  then one typed loop per aggregate; typed single-column keys) — group_region @10M
  68–70 → 33–35 ms isolated. Teardown: big columns drop on the rayon pool (93 → 16 ms
  @10M). Measured and discarded: batch-level UTF-8/typed-push CSV parsing (no change —
  parsing is already parallel). Also fixed a flaky test: a busy durable lock is retried
  for 200 ms (a forking sibling thread briefly holds a duplicate descriptor).
