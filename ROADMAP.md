# pinkdb Roadmap

Single source of truth for wave status; update after every wave. Known gaps in done waves: [docs/GAPS.md](docs/GAPS.md). User-facing summary: [CHANGELOG.md](CHANGELOG.md) — appended at every integration; the website docs are refreshed in the docs catch-up wave (73).

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
| 34 | Lake architecture: DuckLake-style decoupled metadata + data storage (catalog, snapshots, data files) (separate crates, storage layer) | done | crates/pink-lake + CLI lake feature: `catalog.json` + immutable `snapshots/<v>.json` + per-table `data/<table>/<uuid>.pink` (single-table engine bytes); snapshot/version/restore API; crash-atomic publication; 465 tests both parser modes, 35 CLI lake tests, clippy clean incl. lake,encryption; process-race harness 8×4/6×4/6×6 consistent; sizes default 2.29 MB / +lake 2.47 MB |
| 35 | Vortex file format as the default lake data format (format crate / extension) | done | vortex 0.88 direct dep in pink-lake (blocking, direct arrays, no arrow on our side); catalog v2 with per-table format; v1 `.pink` lakes readable (checked-in fixture) and migrate on save; bit-exact round-trips incl. NaN payloads/-0.0; temporals stored as raw ints + metadata (native vortex timestamp ext panics on i64 extremes); 1M-row data 3.1 MB vs 14.0 MB `.pink` (4.5× smaller), save 0.20 s vs 0.086 s, load 0.067 s vs 0.108 s; +218 crates, lake binary 44.6 MB (default unchanged 2.29 MB) ; post-review C1: tampered .vortex files could panic inside the vortex decoder (~0.5% of single-byte flips) — the full decode boundary is now catch_unwind-guarded and surfaces Corrupt errors naming the file, with a deterministic bit-flip sweep in debug and release plus a CLI tamper test; corrupt-input gate + crafted pinkdb.table cases added (472 → 487 tests); docs corrected (decoder panics caught; workspace builds compile vortex while the feature-less CLI binary stays vortex-free) |
| 35b | Parser cleanup: delete the hand-written recursive-descent parser + lexer and the `PINKDB_PARSER` switch (PEG-only parsing); retain/re-home the shared pieces the PEG grammar and WAL statement-splitting still use; port lexer/parser unit tests to PEG tests; keep the differential corpus as a PEG-only corpus; update README/docs (including the docs website's CLI page) | done | hand-written parser + lexer deleted (2,001 lines; server cross-parser test replaced); Keyword/statement-splitting/nesting-guard re-homed; `PINKDB_PARSER` switch removed; lexer/parser tests + corpus + seeded fuzz ported to PEG-only; 472 tests, clippy clean, duckdiff 60/60, docs CLI page redeployed |
| 36 | S3 bucket support for lake storage and file reading (optional `s3` feature, storage provider) | done | pink-lake `Storage` trait + `s3` feature (hand-rolled SigV4 over ureq/hmac/sha2, AWS + R2 path/virtual-hosted, conditional writes: create-only snapshots + `If-Match` catalog CAS with conflict errors, documented `501`→check-then-put degradation); mock-S3 test server + AWS SigV4 vectors + 3×3 race; CLI `s3://` URLs; default CLI byte-identical, +1.96 MB with `lake,s3`; 490 default tests, 79 pink-lake `s3` tests, clippy clean ; multi-server real-S3 harness (SERVER=s3mock\|seaweedfs\|rustfs): SeaweedFS and RustFS are strict witnesses (verify SigV4; conditional PUTs atomic under 2-writer races — 0/20 double-200s) so one-winner-per-version and catalog==snapshot bytes are HARD-enforced there; s3mock stays lenient (documented non-atomic conditionals); SeaweedFS's default config is anonymous (probed+reported; the harness starts an identity-configured container for the enforced run); RustFS needs a 4M-row slow-vs-quick window; MinIO images currently unpullable; recipes + capability matrix in README/s3_docker_notes.md |
| 37 | Documentation website (separate site/track) | done | website/ generator (stdlib Python, layout+fragments), 10 pages + CSS, 86 KiB self-contained, extensionless canonical links, wrangler dry-run validated |
| 38 | Host the website and extension repository on Cloudflare Workers + R2 (infra; Workers half done, R2 deferred to the extensions-repository wave 32/39) | done | live at https://pinkdb-docs.defaultcms.workers.dev (Workers static assets; .html links 307→canonical extensionless; 404-page; assets 200) |
| 39 | CI/CD publishing of extensions to the repository (build, test, sign, publish) (infra/CI workflows) — *as built: signing is a documented hook only (SHA-256 checksums); signatures moved to wave 40i* | done | three GitHub Actions workflows (.github/workflows/{ci,publish-extensions,deploy-docs}.yml): CI build/test/clippy + feature matrices + wasm + duckdiff + Python/Go bindings + C/Go SDK smokes + Docker S3 job; manual-dispatch extension publishing to the live R2 repo (dup refusal, checksums today, signing hook documented); docs auto-deploy; actionlint-clean, YAML-parsed, steps executed locally where possible; secrets documented (CLOUDFLARE_API_TOKEN scopes + account id) |
| 40 | Performance tuning (recurring every 5th wave): bench-driven speed work, no new features (core) | done | profile-driven: parallel vortex writes (2-2.7×, byte-identical), typed aggregate kernels + count-filter fast path (filter 50→24 ms @10M), window key rework (95→61 ms @1M); standing battery 493→447 ms; 494 tests, duckdiff 60/60; group-by @10M target missed (74 vs ≤60, parallel-ceiling analysis + breadcrumb); next-round breadcrumbs recorded |
| 40a | Hardening — safety (docs/GAPS.md P0 #1–#10): server-side `LOAD EXTENSION` opt-in, publish catalog-wipe guard, store-wide table versions, read-set COMMIT validation, bounded `.pink` decoder + v3 CRC trailer, DuckDB-style overflow errors, CLI lake handle baselines, https-only `.install`, redacted S3 secrets, bounded server frames + idle timeout | done | 502 workspace tests (+8 regression tests, each shown to fail without its fix where the platform allows); feature matrix green (pink-lake s3 106, cli lake,s3 44, cli encryption 27, core encryption 352, cli client 27, cli repo 54); clippy -D warnings clean all feature sets; duckdiff 60/60; kill loops 30/30 plain + 30/30 multi-db + 20/20 targeted + 30/30 encrypted; bench parity vs main interleaved (418–422 vs 419–425 ms); sizes `pinkdb` 2.14 MB (unchanged) / `pinkdb-server` 1.80 MB (+0.02) |
| 40b | Hardening — durability/operability: auto-checkpoint + `CHECKPOINT`, `.pink` file lock, fail-stop after a WAL write failure, single-lock bind+run, Python handle lock, CLI exit codes/stderr/`-c`/`.read`/EOF/`;` scanning, S3 retries, honest `versions()`, `.lake` transaction/durable guards | done | 512 workspace tests; feature matrix green (pink-lake s3 108, cli lake,s3 51, cli encryption 33, core encryption 356, cli client 33, cli repo 60); Python 37 + Go (-race) green; clippy -D warnings clean all feature sets; duckdiff 60/60; kill loops 30/30 + 30/30 multi-db + 20/20 targeted + 30/30 encrypted; C/Go SDK smokes 14/14 + 15/15; bench parity (421–424 vs 418–424 ms); sizes `pinkdb` 2.15 / `pinkdb-server` 1.83 / dylib 1.68 MB |
| 40c | Hardening — CI + docs truthfulness: macOS CI + fmt/audit/feature-test gaps, website refresh, committed-binary/account-id cleanup, wave 29/39 claims corrected | done | CI: fmt gate (after a one-time mechanical `cargo fmt`), feature tests for encryption/client/repo, clippy per feature set, kill loops, macOS job, MSRV job (rust-version 1.89 core / 1.95 pink-lake), `cargo audit`, docs build + roadmap-sync check, Go vet/-race, C example; all actions pinned to commit SHAs, wrangler pinned (4.151.0); `pinkdb.h` ↔ Rust header-sync test (shown to fail on a planted drift); website refreshed (parser, protocol v4, TLS/RBAC wave numbers, scripting, locking) with the roadmap page generated from ROADMAP.md; committed `hello` binary removed + ignored; account id removed from README; 514 tests; actionlint clean. Only GitHub can verify: the new jobs' runner execution (macOS, MSRV 1.89, cargo audit) |
| 40d | SQL completeness: multi-way/composite/non-equi/CROSS/FULL joins, INSERT…SELECT, CTAS, IF [NOT] EXISTS, OFFSET, NULLS FIRST/LAST, INTERSECT/EXCEPT, COUNT(DISTINCT), EXPLAIN, SHOW/DESCRIBE, RANGE frames + frame validation | done | absorbed by wave 58 |
| 40e | Prepared statements + parameters: placeholders, bind API, protocol v4, FFI prepare/bind, Python DB-API, Go database/sql | done | absorbed by wave 59 (binding wrappers = wave 69) |
| 40f | Functions + types: lag/lead/ntile/first/last_value, core scalar gaps, DECIMAL, INTERVAL | done | absorbed by waves 60 + 62 |
| 40g | Optimizer: projection pushdown, LIMIT short-circuit, join build-side choice, filter through aggregate | done | absorbed by wave 61 |
| 40h | Lake: incremental saves, `.lake gc`, `lake_scan` SQL access + time travel, bulk typed decode, encode panic guard | done | audited by 97: incremental saves wave 63 (`Lake::commit` / `commit_from_tables`, `tests/incremental.rs` `commit_appends_without_rewriting_and_the_manifest_records_the_ranges`), `.lake gc` shipped as `.lake cleanup` + `.lake orphans` wave 67 (`maintenance.rs` `cleanup_old_files`), `lake_scan` obsolete — never existed, time travel shipped as the `AT (VERSION => n)` clause wave 64 (`attached.rs` `AtSpec`), bulk typed decode wave 45h/102 (`persist.rs` `decode_chunk_typed`), encode panic guard obsolete — atomic publication (`persist.rs` `write_atomic`) closes the failure mode it guarded; see `docs/wave-97-audit.md` |
| 40i | Extension trust + distribution: Ed25519 artifact signatures, SQL `INSTALL`, Linux artifacts, Python wheel, standalone Go module | done | audited by 97: signatures shipped with the release (security pass 3 / M-10 — `repo/signature.rs` pinned key, `tests/repo.rs` `unsigned_catalog_is_refused_before_any_artifact_request`), Linux artifacts wave 90 (`scripts/release/matrix.py` `PLATFORMS`); open items moved out — SQL `INSTALL` → 93, Python wheel + standalone Go module → 92; see `docs/wave-97-audit.md` |
| 40j | Test depth: window/plan unit tests, WAL fuzz, persist property tests, duckdiff gated in CI | done | audited by 97: window and plan-tree tests (`exec/mod.rs` `window_*`, `optimizer.rs` `mod tests`), WAL fuzz wave 45n (`wal.rs` `mod wave45n` — `fuzz_wal_truncation_at_every_offset`), persist property tests (`persist.rs` `roundtrip_corpora_are_bit_exact`, `seeded_encode_decode_loop_is_bit_exact`), `duckdiff` a required CI job since wave 39; see `docs/wave-97-audit.md` |
| 41 | Primary/foreign keys, Snowflake-style: declarative `PRIMARY KEY` / `FOREIGN KEY` / `UNIQUE` / `NOT NULL` constraints (metadata, not enforced), `RELY` / `NOT ENFORCED` accepted, persisted in the catalog, used by the optimizer (e.g. FK→PK join elimination) | done | landed; DuckDB-pinned DDL validation, DESCRIBE `PRI`/`UNI` parity, conservative FK→PK join elimination; `.pink` v5 constraint section (flags bit 0) composed with wave 62's dtype tags; duckdiff 165 → 171 |
| 42 | Index support: `CREATE INDEX` / `DROP INDEX`, hash indexes for equality lookups + automatic per-chunk zone maps, optimizer index-vs-scan choice, per-database, persisted/WAL-logged | done | landed (user, `a79cd41`): single-column **hash indexes** (`CREATE INDEX [IF NOT EXISTS]` / `DROP INDEX [IF EXISTS]`), `Value → row indices`, per-database scoping, optimizer rewrites `col = literal` → `IndexScan`, CLI/server/FFI result kinds; **`.pink` persistence deferred** (queued as wave 79) |
| 43 | Materialized views: `CREATE MATERIALIZED VIEW`, `REFRESH [MATERIALIZED VIEW]`, on-demand full refresh, per-database, persisted + WAL-logged (incremental/auto-refresh deferred) | done | landed (user, `2be5d3b`): stored as a queryable table + refresh query in the catalog, `REFRESH` fully recomputes, `IF [NOT] EXISTS`, per-database, WAL-logged, persisted; manual + scheduled refresh policies follow in wave 80 |
| 44 | Stored procedures: SQL procedural language (parameters, variables, control flow) + **JavaScript procedures** (optional embedded engine; WAL-replay determinism must be solved — volatile calls refused or pinned at record time) | done | SQL language landed (bodies in `$$…$$`, DECLARE/SET/IF/WHILE/nested blocks/RETURN/RAISE, values-never-text binding, atomic `CALL`, WAL replay re-executes deterministically, `.pink` flags bit 4, protocol v8); JS: the `pinkdb-js` sandbox contract + clean refusal landed — **engine binding deferred to wave 81 (quickjs-ng)**; also fixed a pre-existing folded-NULL typing bug |
| 45 | Performance tuning (recurring every 5th wave): bench-driven speed work against the committed query bench; profile-first, small measured wins (core) — *pulled forward after 40c* | done | 4b53844…(part 6) — 1M battery 416 → 245–250 ms (−41%): group_region 6.7 → 3.2, distinct_lowcard 19.4 → 3.8, windows 63 → 13.5, order_str 36 → 29, group_id/distinct_id 62/69 → 50/50, join_probe 21 → 5.3; @10M group_region 68–70 → 33–35 ms (≤60 target met; DuckLake 37–55), import 274 → 173 ms, `.lake save` 632–758 → 296–340 ms (lake format_version 3, 8,192-row blocks, agreed layout change), `.lake open` 618 → 100 ms, data 29.7 → 25 MB, teardown 93 → 16 ms; 516 tests, duckdiff 60/60 + 64/64 big, kill loops clean, clippy/fmt clean |
| 46 | Network server TLS: rustls + certificate config, encrypted transport (optional pinkdb-server feature) | done | landed; `tls` feature = rustls transport (`Server::bind_tls*`, `--tls-cert/--tls-key`, CLI `--tls/--tls-ca-cert/--tls-insecure`), same protocol v8, cert validation before bind, handshake failures isolated under idle timeout, cap applies; no auth/authorization unchanged (documented); 22 tests (incl. runtime-generated certs); default graph byte-identical, server +1.18 MB only with the feature |
| 47 | Async I/O at the I/O boundary: optional async storage layer (object stores / S3 for the lake) and/or an async server variant, feature-gated; requires a documented carve-out to the zero-async design rule for the I/O boundary only (core executor stays sync) | done | landed; `async-server` feature = tokio listener with one task per connection, engine calls on the blocking pool (`spawn_blocking`, 16 MiB stacks), shared protocol/result codecs via a `FrameSink` trait → **byte-identical frames** to the sync server (test), cap/idle-timeout/panic-isolation/EMFILE/shutdown tests, `--transport sync\|async`; documented carve-out; default graph tokio-free; server +0.38 MB with the feature |
| 48 | RBAC: users + roles with Argon2id password hashes (reusing the encryption crate), `CREATE USER`/`CREATE ROLE`/`GRANT`/`REVOKE` on databases/tables, session login on the network server, per-connection enforcement (embedded API stays trusted), grants persisted in the catalog + WAL | done | landed; users/roles hashed with Argon2id (OWASP params reused from `crypto.rs`; PHC at rest; one derivation per statement via an AST memo so plan and WAL share it — **no plaintext ever reaches disk**, proven by a bytes-scanning test); SQL: `CREATE/ALTER/DROP USER [IF NOT EXISTS]`, `CREATE/DROP ROLE`, role membership, `GRANT/REVOKE` table `SELECT/INSERT/UPDATE/DELETE` + database `CREATE` + global `ADMIN`, `PASSWORD_HASH` (validated argon2id) as the replay form; enforcement fail-closed per statement (prepared statements covered; grants follow renames, cascade on drops incl. in-transaction merges; `CALL`, `LOAD EXTENSION`, `CHECKPOINT`, database + user/role DDL need `ADMIN`); `.pink` v5 flags **bit 5** RBAC section (canonical sorted; byte-identical when absent; older builds refuse via the unknown-flags check), WAL-exact; server **protocol v9** (`Login` tag 6 / `LoggedIn` tag 18, DoneKinds 19–25) shared by both transports; `Client::{login,connect_with_login}`, CLI `--user/--password` + `PINKDB_PASSWORD`; empty catalog = trusted with a loud startup warning, embedded API (Rust/FFI/wasm) stays trusted; tests pinkdb 564 → **597**, server 59 → **66** (**94** with `async-server,tls`), CLI 68 → **72** (**77** tls), duckdiff **171/171**; release sizes +242 KB CLI / +262 KB server (Argon2id linkage; **zero new crates**). Refused cleanly: `SHOW USERS/GRANTS`, `WITH GRANT OPTION`, `PUBLIC`, object owners; wasm refuses plaintext `PASSWORD` (no OS randomness) while `PASSWORD_HASH` verifies |
| 49 | Extensions on wasm: support loadable/pluggable extensions for wasm targets — either wasm dynamic-linking (pre-built WASI side modules, per-runtime `dlopen` support) or a host-function registration path (JS/wasm-bindgen scripting surface); native `dlopen` extensions are impossible in the wasm sandbox, so a distinct ABI/loading model is required | done | landed; distinct model = **linked registration** — new `pinkdb-ext-loader::linked` (dependency-free, all targets): `LinkedExtension`/`LinkedFunction` metadata, safe-Rust `LinkedScalar` callbacks (`Send + Sync`, panics caught at the boundary, ≤64 args), one-shot process-global registry, `LinkedExtHost::load`; **unified engine registry** (`ExtFunction`/`ExtHandle`: native `Dyn` + `Linked`) drives binder/executor/optimizer/WAL prefixing for both models on every target (wasm stubs gone); on wasm `LOAD EXTENSION '<name>'` resolves registry **names**, native `dlopen` stays first with registry fallback only when nothing is loadable *and* the name is registered (keeps WAL replay of linked `LOAD EXTENSION` working, no path shadowing); pinned **JS/wasm-bindgen contract** (`pinkdb.registerExtension`/`listExtensions`, SQL↔JS value table, sandbox rules) — contract only, no wasm-bindgen dependency; side-module runtime deferred (documented follow-up); **zero new deps** (`libloading` target-gated, wasm tree childless); tests: ext-loader 43 → **59**, pinkdb 564 → **565**, workspace + wasm (1.97.1 & stable) + duckdiff **171/171**; native CLI +3.7 KB, wasm debug cdylib +0.96 MB |
| 50 | Performance tuning (recurring every 5th wave): bench-driven speed work (core) | done | targeted-profile wave: `fast_distinct_int` rewritten to typed scan + typed chunked output (`Vector::from_raw`, one NULL slot, adaptive one-shot reserve capped at 1<<20) — **group_id 15.76 → 4.37 ms**, **distinct_id 15.63 → 4.63 ms** (both ahead of DuckDB α46057's 7 ms); `FnvHash::write` mixes 8-byte words + folded tail (order-independence audited) — distinct_lowcard 3.84 → 3.07, join_probe 5.30 → 4.60; battery **101.5 → 75.7 ms (−25 %)** (main re-run 79.4–79.8); dropped with numbers: join-cell assembly micro-opt (real query already ≈2.6 ns/cell), fused join (9.3 vs 26.2 ms materialized — feature-sized), unconditional reserve (replaced by adaptive); general 1M-group path measured 180 ms (SoA accumulators = wave-40 breadcrumb); DuckDB α re-measured today: total 85 ms, remaining gaps join_mat 25.0/20, join_group 14.7/12, join_probe 4.6/3, order_num_topn 2.87/2; gates pinkdb **597**, duckdiff **171/171** + `--big` **175/175**, fmt/clippy, wasm green |
| 51 | Rust extension SDK: `ext-sdk/rust` safe wrapper + registration macros for first-party extensions; ABI v1 additive: a deterministic/volatile function flag (the engine refuses to WAL-log volatile calls, preventing replay divergence) | done | landed; `ext-sdk/rust` = **`pinkdb-ext-sdk`** (workspace member): one `pinkdb_extension!` declaration (name, functions, optional `volatile` list) generates the three ABI v1 exports, a `&'static [ScalarSpec]` description **both** loading models consume (native cdylib + wave-49 linked/wasm registrars), and linked registration helpers; safe-Rust typed args (`bool/i32/i64/f64/&str/String/Date/Timestamp`, `Option<T>` NULLs) + returns (`T`/`Option<T>`/`Result<…>`), all marshalling/ownership/panic-containment in the SDK; worked example + copy-me template + Makefile + 15-check smoke (`sdk-smokes` CI now C/C++/Go/**Rust**); **additive ABI v1 volatile flag** (`PINKDB_EXT_FLAG_VOLATILE` + appended `register_scalar_flags`; `register_scalar` ≡ flags 0 so pre-flag descriptors behave byte-identically — pinned by a legacy-view fixture; unknown bits rejected; pre-flag hosts fail init instead of silently registering as deterministic); engine refuses volatile calls in any statement `plan::writes_tables` logs (incl. in-memory stores; reads/view bodies stay callable; refused statements leave no WAL bytes) and in procedure **expressions** via the new `bind_constant_expr_tracked` (a real gap found by the audit); docs across Rust/C/Go SDKs + wasm JS contract (`volatile: true`); tests ext-loader 59 → **72**, ext-sdk **19**, workspace **957**, pinkdb **597**, duckdiff **171/171**; CLI +17,184 B (+0.035 %); **no new external deps** |
| 52 | Core extension: **json** — `json_extract`, `json_value`, `json_valid`, `json_array_length`, `json_object`, `json_array` (JSON as VARCHAR in v1) landed; new `extensions/json` = **`pinkdb-json`** (workspace member, **zero new deps** — hand-rolled parser/path engine/serializer, measured cheaper than `serde_json` for the pinned semantics): `json_extract`, `json_value`, `json_valid`, `json_array_length` (1–2 args), `json_object`/`json_array` (variadic) — JSON as VARCHAR v1; semantics pinned case-by-case against DuckDB α46057 (trailing commas, `NaN`/`inf`, duplicate-key first-match + order preservation, canonical numbers incl. verbatim `1e400`, `\u00XX` escapes, lone surrogates rejected, compact re-serialization; `json_value` returns quoted JSON text like DuckDB α — a documented divergence from the brief's wording); loader gained **optional/variadic arity** (`new_optional`/`new_variadic`, min/max + per-position types; additive, dlopen unchanged); CLI gains a **default `json` feature** (publishes the registration at startup; `--no-default-features` = lean; an explicit `LOAD EXTENSION 'json'` reports the documented duplicate); WAL replay resolves via the registry (`LOAD EXTENSION 'json'` prefix); tests pinkdb-json **32**, ext-loader 59 → **60**, workspace **958**, duckdiff 171 → **196** (+25 json probes; an 809-case scratch fuzz showed 0 mismatches); CLI **+76 KB** default (3,636,960 B), +75,584 B lake; wasm green | done | details in scope |
| 53 | Core extension: **string** — `split_part`, trim variants, `replace`, `reverse`, `left`/`right`, `strpos`, `starts_with`/`ends_with`, `regexp_match`/`regexp_extract` landed; new `extensions/string` = **`pinkdb-ext-string`** (workspace member; **no new crates** — `regex` was already locked) authored with the wave-51 SDK: `split_part`, `reverse`, `left`, `right`, `strpos`, `starts_with`, `ends_with` (exact DuckDB α semantics incl. negative/1-based indexes, Unicode scalars, NULL propagation) + the regexp surface: `regexp_extract` (4-arg full shape), **`regexp_matches`** (3-arg BOOLEAN — DuckDB's real function; the brief's `regexp_match` does not exist in DuckDB — verified — pinned as pinkdb's first-match VARCHAR, documented loudly), option surface `i/c/l/m/n/p/s` with DuckDB's left-to-right precedence (`g` refused exactly like DuckDB); `trim`/`replace` families deliberately NOT re-registered (core built-ins; extensions cannot shadow — proven by a test that pins the refusal); one minimal engine grammar relaxation so unquoted `left(`/`right(` parse (LEFT/RIGHT are join keywords; `LEFT OUTER JOIN`, bare keywords, `CAST(x)` unchanged); tests ext-string **19**, pinkdb 597 → **598**, workspace **1010**, duckdiff 196 → **223** (+27 probes), smoke **15/15**, wasm green; CLI default **+0 B** (runtime-loaded dylib: 1.70 MB unstripped / 1.43 MB stripped, regex chain ≈1.22 MiB of it) | done | details in scope |
| 54 | Core extension: **math** — `sqrt`, `pow`, `log`/`ln`/`exp`, trig, `floor`/`ceil`, `sign`, `greatest`/`least`, `random` (volatile-flagged) landed; new `extensions/math` = **`pinkdb-math`** (workspace member, **zero new deps** — hand-rolled SplitMix64 `random`) with the SDK: **17 functions** — `exp`, `ln`, `log` (1-arg = log₁₀, 2-arg = `log(b, x)` composed as DuckDB's log10 ratio), `log2`, `log10`, the trig family (`sin/cos/tan/cot/asin/acos/atan/atan2`), `degrees`/`radians`/`pi`, **volatile `random()`** — while `sqrt`/`pow`/`floor`/`ceil`/`sign`/`greatest`/`least` stay engine built-ins (extensions cannot shadow; probes pin them end-to-end anyway); SDK gained **ranged arity** (`optional(min)` + three-state `Optional<T>`; `variadic(min)` over `Vec<T>`) and is **linked-only by construction** for ranged declarations — the native ABI has no ranged descriptor, so the SDK refuses a cdylib load with code 5 (tested); **numeric widening**: DOUBLE-declared dynamic args accept INTEGER/BIGINT/DECIMAL via an inserted `CAST(… AS DOUBLE)` (DuckDB's implicit cast; VARCHAR stays strict, documented); volatile enforcement refuses `random()` in every WAL-logged statement (reads/views work; refused statements leave no WAL bytes); `math` joins the CLI **default features** (json+datetime+math); tests math **21**, ext-sdk 19 → **28**, cli 67 → **70**, workspace **1080**, duckdiff 248 → **272** (+24 probes), smokes green, wasm green; wave sizes **+25,520 B** default / **+41,888 B** lake over json-only; final four-pack CLI default **3,719,664 B** (see README table); the `publish-extensions` discovery fix (skip artifact-less `extensions/*/` dirs instead of failing) rides here too | done | details in scope |
| 55 | Performance tuning (recurring every 5th wave): bench-driven speed work (core) — done: column-vs-literal comparison fast path (`filter_str` 5.39 → 1.60 ms, battery 235.5 ms best-ever); audit deltas attributed (per-chunk literal Vectors + codegen/thermal noise), fused-path choice validated | done | core |
| 56 | Core extension: **datetime** — `strftime`, `strptime`, `epoch`/`epoch_ms`, `dayofweek`/`dayofyear`/`week`, `last_day`, `age`, `make_date` (naive timestamps only) landed; new `extensions/datetime` = **`pinkdb-datetime`** (workspace member, **zero new deps** — hand-rolled civil-date + format engine) with the wave-51 SDK: `strftime`, `strptime`, `epoch`, `epoch_ms`, `dayofweek`, `dayofyear`, `week`, `last_day`, `age`, `make_date` over **naive** DATE/TIMESTAMP; **additive ABI v1 INTERVAL value type** (tag 9, matching `pinkdb.h`; months/days ride the bits of `double`, µs in `int64`; loader + engine + SDK + all four header copies via `header_sync`) so `age` returns a real INTERVAL; semantics pinned against α46057 (epoch is fractional **DOUBLE** — the brief's BIGINT is the 1.x shape; `epoch_ms` ties-away rounding; ISO-8601 `week`; `age` decomposition borrows from the start month; `make_date` errors like DuckDB; strftime/strptime specifier set = α's minus deliberate refusals `%z %Z %n %N %g` (+strptime `%U %W %V %G`) — full matrix + divergences in the crate docs); `datetime` joins `json` as a CLI **default feature** (auto-loaded; `--no-default-features` lean); tests datetime **35**, ext-loader 73 → **74** (+C-ABI interval round-trip), pinkdb 598 → **599**, workspace **1047**, duckdiff 223 → **248** (+25 probes), smokes green, wasm green; sizes **+40,304 B** default / **+56,832 B** lake (exact) | done | details in scope |
| 57 | Pink-lake data encryption/decryption at rest (feature-gated): one Argon2id KDF per lake session (salt recorded in the catalog), AEAD containers for the catalog + snapshots (the `catalog == snapshot` byte invariant preserved) and the data files — stage 1 `.pink` v1/v4, stage 2 Vortex v2/v3 (whole-file container first; attach keeps working via the encrypted manifest statistics, footer-only/segment-level as the follow-up); `PINKDB_KEY` UX consistent with the engine; plaintext lakes keep working; a missing/wrong key fails closed with an authenticated error; S3 ciphertext CAS unchanged; encrypted soak + strict S3 harness coverage | done | landed; `PNKLAKE1` AES-256-GCM containers over every object via a `Storage` decorator (manifests sealed once → invariant holds; commits/deletes/time-travel inherited), stats-fast attach preserved, wrong/missing key fails closed with feature-off rebuild hints, encrypted CI matrix + Docker harness section; Vortex stage-1 whole-file reads documented |
| 58 | SQL completeness (worktree `wt-sql-completeness`): multi-way/composite/non-equi joins, CROSS/FULL joins, INTERSECT/EXCEPT, INSERT…SELECT, CTAS, IF [NOT] EXISTS, OFFSET, NULLS FIRST/LAST, COUNT(DISTINCT), RANGE window frames + the DuckDB RANGE-default peer fix, EXPLAIN, SHOW/DESCRIBE | done | landed; duckdiff 88 → 155 probes |
| 59 | Prepared statements and parameters: placeholders, a bind API, and protocol/FFI/Python/Go front-end support | done | landed (engine + protocol v5 + FFI + client); Python/Go wrappers follow in wave 69 |
| 60 | Functions (worktree `wt-functions`): window `lag`/`lead`/`ntile`/`first_value`/`last_value` + remaining scalar gaps, DuckDB-pinned semantics | done | landed; duckdiff 60 → 88 probes |
| 61 | Optimizer work (worktree `wt-optimizer`): projection pushdown, LIMIT short-circuiting, join build-side choice, filter-through-aggregate | done | landed; battery 267 → 249 ms, duckdiff 88/88 |
| 62 | Types: DECIMAL and INTERVAL (storage, arithmetic, casts, CSV/persist encodings, DuckDB semantics) | done | landed; `.pink` v5, protocol v6, FFI text cells; duckdiff 155 → 165 |
| 63 | Lake writes, DuckLake parity: incremental lake commits — append new immutable data files with per-file row ranges instead of whole-table rewrites, positional delete files for DELETE/UPDATE (merge-on-read; deletion-vector option as a follow-up), writable attached lakes, per-commit manifests, concurrent-writer retry/backoff (`max_retry_count`/`retry_backoff`/`retry_wait_ms` parity), WAL-logged for durable sessions | done | landed; catalog v4 (per-file rows + `pinkdel` delete files + `commits/<v>.json` deltas), `Lake::commit`/`commit_from_tables`, writable attach + retry parity (10/1.5/100ms probed), 1M-row demo: 7.6 KB / 0.54 s per commit, 0 bytes rewritten |
| 64 | Lake time travel & snapshot metadata, DuckLake parity: `AT (VERSION => n)` / `AT (TIMESTAMP => ts)` per query on attached tables, per-version records (time, author, commit message, change summary), `.lake snapshots` listing, commit messages on save, `default_version` setting (optional) | done | landed; DuckLake-pinned AT semantics (literals only, capped at the attached version), per-version stats fast paths, manifest records + `.lake snapshots` + commit messages; `default_version` probed to be a catalog-format selector, not a read default — skipped |
| 65 | Lake schema evolution, DuckLake parity: ALTER TABLE on lake tables (ADD COLUMN with DEFAULT, RENAME, DROP), catalog schema versions, old data files read through the schema mapping | done | landed; catalog **format 5** schema records (per-column defaults/added-epochs/renames), metadata-only commits, per-version schemas under time travel, engine-wide ALTER + `.pink` FLAG_DEFAULTS; reconciled with 66 by hand (21 conflicts — `record` field split, zone-map vs merge gating, format-5 pins) |
| 66 | Lake partitioning & pruning, DuckLake parity: `PARTITIONED BY` file splitting, per-file column statistics/zone maps (absorbs the queued lake-scan stage 3), filter pruning on attached scans | done | landed; identity + `bucket(N)` specs recorded in the catalog (additive to v4), one data file per partition value per commit, per-file zone maps (Vortex via `persist::table_stats`), planner-extracted conjunct pruning with decode counters; bucket labels are pinkdb's own FNV-1a (no bucket-value pruning; documented); duckdiff 171/171 |
| 67 | Lake maintenance, DuckLake parity: `merge_adjacent_files`, `rewrite_data_files`, `expire_snapshots`, `cleanup_old_files`, `delete_orphaned_files`, `list_files`, `table_info`/`options`/`settings` introspection as `.lake` commands | done | landed; compaction merged-away sources never touched (bytes+mtimes pinned), groups by partition+epoch with ≥2 delete-free files; rewrite materializes deletes; expire (versions/older-than/keep, never current); cleanup/orphans share the unreferenced scan (no scheduled-deletion queue — documented) with dry-run + age gate; S3 `stat`/`list_recursive`; duckdiff 171/171 |
| 68 | Lake inlined small writes & CDC, DuckLake parity: inline tiny commits into the metadata (`data_inlining_row_limit`-style, flush to files later), change summaries rendered as CDC queries (`insertions`/`deletions`/`changes`), `add_data_files` (register existing files); depends on 63 | done | landed; inlined rows in the catalog (default limit 10, α-pinned; `.lake flush` materializes, history kept), CDC via `table_insertions`/`deletions`/`changes` + `.lake changes` (updates = delete+insert, documented), `add_data_files` with exact-schema checks (+ encrypted path binding); duckdiff 171/171 |
| 69 | Prepared statements in the Python and Go bindings: wrapper ergonomics + tests over the FFI entry points landed in 59 landed; idiomatic prepared-statement wrappers over the wave-59 FFI (`pinkdb_prepare` → typed bind → `pinkdb_execute_prepared`) with **no C-ABI/engine changes**: Python `Database.prepare(sql)` → `Statement` (`execute(*params)` → one `Result`, `parameter_count`, `sql`, idempotent `close()`, context manager + `__del__`, copy/pickle refusal); Go `(*DB).Prepare` → `*Stmt` (`Execute(args ...any)`, `ParameterCount`, `SQL`, idempotent `Close`, `ErrStmtClosed`, goroutine-safe reuse, `pinkdb.Date` param type); value-based int widths (INTEGER if it fits i32, else BIGINT; >int64 refused); native error strings mapped verbatim; string payloads copied into C memory (embedded NULs preserved); both bindings **caught up to wave-48 result kinds 17–23**; tests Python 37 → **56**, Go 25 → **38** (incl. `-race`, an 8-goroutine `Stmt` reuse test, a 10k-execution bounded-RSS pin, durable WAL replay), FFI **16**, fmt/clippy clean on both toolchains; limits: no BLOB/DECIMAL/INTERVAL/HUGEINT parameters (ABI v1, workaround documented), Go requires explicit `Close` (no finalizer), prepared binds resolve the handle's current catalog snapshot (wave-59 engine behavior, tested) | done | details in scope |
| 70 | Core extension: **parquet** — read: `read_parquet` / `parquet_scan` (globs, multi-file, `union_by_name`, hive-partition + `filename` columns, column subset), projection + filter pushdown via row-group statistics (min/max/null counts), introspection (`parquet_schema`, `parquet_metadata`, `parquet_file_metadata`, `parquet_kv_metadata`), direct `FROM 'file.parquet'` replacement scan; write: `COPY … (FORMAT parquet)` (snappy default; zstd/gzip/uncompressed; `row_group_size`; per-column stats) with the α-probed physical mapping (DECIMAL→INT32 or FIXED_LEN_BYTE_ARRAY, TIMESTAMP→INT64 micros, DATE→INT32 days, INTERVAL→FIXED_LEN_BYTE_ARRAY) landed (unified with wave 71); new `extensions/parquet` = **`pinkdb-parquet`** (engine-adjacent; `parquet` 59.3.1 with its `arrow` feature **off**, so no duplicate arrow-rs units with vortex's 59.3.0; snappy+gzip built in, zstd/brotli/lz4 opt-in): `read_parquet`/`parquet_scan` (globs `*?[]**`, directories, multi-file, `union_by_name`, hive keys, `filename`, `file_row_number`, `allow_empty`, `hive_types_autocast`, `binary_as_string`), four introspection functions (`parquet_schema` / `_metadata` / `_file_metadata` / `_kv_metadata`), replacement scan (`FROM 'x.parquet'`, claiming `.parquet/.parq/.pq` plus extension-less existing directories), **projection pushdown** and **row-group-statistics filter pruning** (conservative: unsigned/absent stats prune nothing; floats never for `<>`), `EXPLAIN filters=[…]`, `.parquet [stats|reset]` counters; type mapping widens (TINYINT→INTEGER etc.) and refuses by column name (TIME, TIMESTAMP(nanos), UUID, FLOAT16, BSON, VARIANT, nested); **read-only** (COPY parquet = follow-up); **unification**: one read surface (`TableSource`, this wave's) — wave 71's `FromSource::File`/`FileScan`/read half of `FileFormat` deleted, arrow ported to `ArrowSource` with its `ArrowSink` kept for COPY; one grammar (`Function` + `Path`); **logging policy = re-read for all file sources** (this wave's refusal dropped; replay re-reads, a deleted file is a hard error; tests flipped, not dropped); crate relocated `crates/` → `extensions/parquet`; duckdiff batteries merged, CI artifact builds `--features arrow,parquet`; tests pinkdb 604 → **612**, parquet+arrow **54**, cli 4-combo **72/74/75/77**, workspace **1149**, duckdiff **315/315**; sizes default **3,759,968 B**, parquet **4,889,968 B**, arrow 6,948,272 B, arrow+parquet 8,059,168 B | |
| 71 | Core extension: **arrow** — Arrow IPC (Feather v2) file read/write (`read_arrow` + `FROM 'file.arrow'`, `COPY … (FORMAT arrow)`) over the arrow-rs crates; Arrow **C Data Interface** in the FFI (query-result export + Arrow-array scan input, zero-copy where the layout allows) for pyarrow/Polars/DuckDB/ADBC hosts; DuckDB α ships no loadable `arrow` extension on this platform (probed: HTTP 404), so parity is pinned at the file level (arrow-rs/pyarrow round-trips; pyarrow pip-installable) and at the C ABI level, not via SQL landed; new `extensions/arrow` = **`pinkdb-arrow`** (engine-adjacent crate over arrow-rs 59.3 — `arrow-ipc` + flatbuffers/LZ4/ZSTD are the only new lock entries) behind the opt-in CLI **`arrow`** feature; **new engine extension surface** `pinkdb::filefmt::FileFormat` (trait + per-store registry seeded from a process-global registration at host bootstrap — the timing that makes durable replay work): `FROM 'x.arrow'` replacement scans, `read_arrow`/`read_ipc`/`read_feather`, `COPY t TO 'x.arrow' [(FORMAT arrow)]`, plans/EXPLAIN/DESCRIBE, fail-closed RBAC; IPC **file** format only (stream refused precisely), reads accept uncompressed/LZ4/ZSTD, writes uncompressed, one batch per chunk ≤2048 rows, full-file materialization (no pushdown yet); DuckDB α46057 has **no arrow support at all** (verified) so parity is pinned by pyarrow round-trips both directions + committed golden fixtures + 15 duckdiff value probes; **C Data Interface deferred with a 5-item pinned follow-up** (crate README); sizes: engine seam **+21,840 B** on every build, `--features arrow` **+3,169,520 B**; tests pinkdb 599 → **604**, arrow **29**, cli+arrow **73**, workspace **1115**, duckdiff 272 → **287**; CI duckdiff artifact now builds `--features arrow`; recorded reconciliation for wave 70: **one** `CopyTo` + `FromSource::File` with a `ParquetFormat` impl against the same registry | done | details in scope |
| 72 | Core extension: **iceberg** — Iceberg v2 reads (newest snapshot + the α-probed introspection set) through the **shared httpfs transport**, so tables work on local paths, `https://` and `s3://` alike; **wave 75 is the prerequisite** — sequence after it (the original local-filesystem-only scope was cancelled: Iceberg tables live on object stores) ; **landed**: new `extensions/iceberg` = **`pinkdb-iceberg`** (engine-adjacent, opt-in `iceberg` CLI feature that turns `parquet` on) — `iceberg_scan` / `iceberg_snapshots` / `iceberg_metadata`, current or named snapshot (`snapshot_from_id`, `snapshot_from_timestamp`), positional deletes, schema evolution by field id, partition-summary + bound pruning, `count(*)` from manifest record counts; a hand-written Avro decoder in the extension (null/deflate, records/unions/arrays/maps/enums/fixed) instead of apache-avro; equality deletes refused by name; local, `https://` and `s3://` through the wave-75 transport; stdlib-Python fixture generator (metadata + Avro + DuckDB-written Parquet) and DuckDB-α oracle tests | done | details in scope |
| 73 | Docs catch-up & changelog wiring: bring the website pages current through the landed waves (58–64 now; extend as 65–68 land) — SQL completeness, prepared statements, DECIMAL/INTERVAL, PK/FK, lake time travel, incremental commits — fold in the wave-41 constraint docs excluded at integration, link CHANGELOG.md from the site + README, and add a docs-freshness check to CI | done | landed; site current through 64 (prepared/DECIMAL/keys/lake pages), generated changelog page + nav/footer links, CI fails on stale site output; the redesign WIP landed first (`544cc3f`) |
| 74 | Lake naming sweep: rename the lake builds — `lake-lite` → **pinklake** (the `.pink`-file lake) and `lake` → **vortexlake** (the Vortex-file lake) — across cargo features (`pinklake`/`vortexlake`; pink-lake's crate feature `vortex-format` → `vortexlake`; **`s3` is a pure storage mechanism and implies neither lake build** — it only forwards `pink-lake/s3`, so combinations are explicit: `pinklake,s3` / `vortexlake,s3`, and `--features s3` alone no longer provides `.lake` commands), CLI help/error text, test cfgs and feature matrices, CI commands, README, the website and the ROADMAP/CHANGELOG wording ("lean lake"/"full lake" → pinklake/vortexlake). No aliases (pre-release interface) | done | landed; no old names survive outside history (`grep`-verified), `s3`-alone pinned by a test + CI matrix, test files renamed (`pinklake.rs`), CI extended with the decoupling cases; release sizes drift noted as a follow-up (measured 49.0 MB vs docs ≈47.3 MB, predates the wave) |
| 75 | landed; `crates/pinkdb-httpfs` = the **format-agnostic remote transport**: SigV4 S3 client extracted from pink-lake (config/addressing/signing/retries/conditional PUTs/listings unchanged; pink-lake's `S3Storage` is a thin adapter — its 134 tests + the 26-test S3 suite stay green), https GET/HEAD + **byte-range GET** on both schemes, `Fetcher` (`size`/`fetch`/`fetch_range`); `read_csv` table source (local/`https://`/`s3://`, replacement scan, `.import <url>` via a process-global engine URL hook — no dep cycle); **parquet remote** via a real `ChunkReader` over `fetch_range` (footer + row-group/column pages ranged; whole-object only when HEAD reports unknown size; tests pin the exact 8-byte trailer range and assert no whole GET) incl. remote metadata functions; **arrow remote** whole-object (documented — IPC has no ranged footer protocol); claims by **extension regardless of scheme**, `http://` transport-gated; CLI composition: `httpfs` weakly enables the format remotes so `--features parquet,arrow,httpfs` reads all three formats remotely while default binaries stay transport-free; DuckDB α `httpfs` cross-checks remote parquet/CSV in duckdiff (+7 CSV, +6 remote probes); tests: httpfs **49**(s3), parquet+remote **36**, arrow+remote **37**, CLI combo **80**, pinkdb **641**, workspace **1198**; duckdiff **339/339**; sizes: `httpfs` ≈+1.95 MiB, `parquet,httpfs` ≈+3.09, `arrow,httpfs` ≈+5.01, default +~800 B (engine CSV helpers); limits: single object (no globs/dirs/compression, no `COPY TO s3://`), 128 KiB read-ahead granularity, no HTTP auth/custom CA, S3 URLs follow no redirects / no `versionId` | done | — |
| 76 | landed; CSV import options shared by `.import` and wave-75 `read_csv` through **one `CsvOptions`** in `pinkdb::csv`: `delimiter`/`sep`/`delim`/`separator` (any single byte, `\t` spelling), `quote`, `escape` (off by default), `encoding` (`utf-8` default, `latin-1`, `windows-1252`, `utf-16le`/`utf-16be`, and **BOM autodetect** — a documented superset of α, which refuses UTF-16 BOMs), `errors=error\|warn` (strict default; `warn` skips bad rows and prints a rejects report with line/column/reason, ten shown plus the total; a rejects table stays the recorded follow-up), `header=auto\|true\|false` with **DuckDB's auto rule** (types over all rows vs rows[1..]; every probe matched), `names=`, `nullstr=` (+ DuckDB's `null` alias), `allow_quoted_nulls` and `force_not_null`; **column matching into an existing table by header name** (case-insensitive; missing → NULL, extra → the error policy); the empty-field rule **as α actually behaves** (unquoted *and* quoted empty are NULL by default; `allow_quoted_nulls=false` is what keeps a quoted empty — the brief's wording described that option, not the default); a hand-rolled streaming decoder feeding a streaming record reader, so wave-98c read-ahead and ranged remote reads are unchanged, and the parallel `parse_clean` batch parser now understands RFC 4180 quoting (so quoted files stay parallel instead of falling back), the type sniff samples 20 480 rows instead of walking the whole file, and wave-75 `read_csv` scans run the same batch parser window by window (so a scan no longer pays the reader) — `.import` of TPC-H `lineitem` (773 MB, 6 001 215 rows) 2.54 s → **0.87 s** and `orders` (174 MB) 0.42 s → **0.21 s** against pre-76 (reviewer harness, interleaved, median of 3), while `errors=warn` (12.2 s) and `encoding=latin-1`/`utf-16` (14.2 s / 13.3 s) still run through the record reader — the recorded next target. Exposed as `.import <path> [table] [key=value …]` and `read_csv('<path>', key := value, …)` (DuckDB parameter names), identical for local, `https://` and `s3://` files | done | `pinkdb` CSV module 19 → **36** tests (incl. a DuckDB-oracle test for the empty-field rule and header auto, skipped without the α binary, and streaming-vs-reader parity across a window boundary), `pinkdb-httpfs` **63** (local-vs-remote parity, `warn` in a scan, one `#[ignore]`d speed aid), 4 new `.import` CLI tests; workspace **1865** / 161 suites; every gate green; no new dependency (latin-1/windows-1252/UTF-16 are ~120 lines of hand-rolled decoding), and wave 75's unused `csv` crate left the tree (the engine's own `PinkDbError::Csv` now carries the file line) |
| 77 | landed; scalar `CREATE [OR REPLACE] MACRO [IF NOT EXISTS] name(params) AS <expr>` + table macros `AS TABLE SELECT …`, `DROP MACRO [IF EXISTS]`; untyped params with constant `:=`/`=>` defaults and positional/named call args; **bind-time AST substitution** (args cloned into the template, bound in the caller's scope — no SQL text generation, `?` slots survive, depth cap 8 + 65,536-node budget fail cleanly); works in expressions, FROM/joins, aggregates, windows, CTEs, prepared statements, INSERT/UPDATE/DELETE, views and other macros; recursion refused at CREATE for direct/mutual/forward cycles (two-direction call-graph check), view-crossing cycles at bind; `SHOW MACROS` + `DESCRIBE MACRO`; persistence is `.pink` flags **bit 6 (0x40)** with the macro section written **before** MVs (MV rebinds see macros) and full DDL text persisted; WAL-logged DDL replayed in order; RBAC: create/drop needs database `CREATE` (or `ADMIN`), invocation is invoker-rights (expansion happens before `check_statement`, so table grants stay enforced); macro DDL refused inside explicit transactions (catalog-metadata policy); α divergences documented (no typed params / `query_table` / lateral args; create-time body validation deferred to first use; OR REPLACE cycles refused stricter; DDL-in-transaction refused); tests pinkdb 612 → **639**, workspace **1176**, duckdiff **326/326** (+11 probes); default binary **+97,728 B** | done | engine wave; persistence gains a catalog object — same version policy (clean newer-format refusal, no compat gates) |
| done | landed 2026-10-21: new `crates/pinkdb-server-ui` crate behind the **off-by-default `ui` feature** (+128 B on the default server binary; +1.15 MiB and 19 new lock entries with the feature on). **axum 0.8 + tokio 1.53 + maud 0.27** — the repo's **second documented async carve-out** (after wave 47): the console owns its own tokio runtime on its own thread, so it works with the sync and `async-server` transports; every engine read goes through `spawn_blocking` on an explicitly sized blocking pool (4 workers / 8 blocking threads), the accept loop is semaphore-bounded (over-cap → 503), and hyper/axum limits are explicit (64 KiB body, 10 s header deadline, 30 s request deadline). Read-only routes: `GET /` (server-rendered shell), `/preview` (no-JS paged preview), embedded `/assets/*` (`include_str!`, no CDN), and JSON `/api/databases|schemas|tables|rows|stats` with bounded paging (≤1000 rows, 16 MiB) and exact totals. Exposure fails closed: `--enable-ui` opt-in, **127.0.0.1 default**, any non-loopback bind **refused** (the console has no TLS of its own), and a store with users refuses to start unless `--ui-auth basic` (engine login, per-user grants, pass-5-style throttling) or `--ui-token` (constant-time bearer) is configured; a user-less store warns loudly. Previews mirror the CLI box-renderer rules (NULL, DOUBLE, control chars, 512-char cap) and maud escapes all cell content; the script uses `textContent`, never `innerHTML`. Tests: 28 hermetic integration tests on a real ephemeral port (self-hosted shell, every endpoint, paging/totals, 404/405/HEAD, header/body limits, slow client, traversal refusal, non-loopback refusal, Basic/token gates, read-only guarantee, `<script>` escaping, engine on the blocking pool, multi-database) + 12 unit tests; the `ui` doctest was fixed. CI now runs `--features ui` and `--features ui,tls`; the default graph stays tokio-free. `docs/server-ui.md` documents the carve-out and policy. Residual: loopback-only (console TLS is a follow-up), a user-less store is served unauthenticated, token mode is operator-level (Basic is the per-user mode), sizes are estimates, deep paging scans, and the query console is deliberately a follow-up. |
| 79 | Index persistence & hardening (follow-up to the user-landed wave 42): store index definitions in `.pink` (format bump with the clean newer-format refusal), WAL-log `CREATE`/`DROP INDEX` replay exactly, multi-process touched-set coverage, and the optimizer's index-vs-scan choice for range/zone-map skips beyond equality | partial | **WAL durability landed 2026-10-20** (index DDL logged as canonical fully-qualified SQL, survives reopen without a checkpoint). **`.pink` index section landed 2026-10-21**: v5 flags **bit 3** (`FLAG_INDEXES`) carries every index's definition plus save-time stats (distinct keys, indexed rows), ~30 B per index; maps are rebuilt from the rows at load (persisted posting lists were built, measured — verify-at-load ≈ rebuild cost, +2–4 MB per 1M-row index — and dropped by decision); checkpoints publish indexes atomically with the data, so the post-roll WAL re-log is gone. Remaining: the range/zone-map index-vs-scan choice |
| 80 | Materialized-view refresh policies (extends the user-landed wave 43): **manual refresh completion** — `REFRESH MATERIALIZED VIEW(S)`, `last_refreshed` bookkeeping and a stale flag in introspection (`SHOW MATERIALIZED VIEWS`-style, or extending `SHOW TABLES`) — plus **scheduled refresh**: a declared policy (`REFRESH EVERY INTERVAL '…'`, `REFRESH ON COMMIT`, `MANUAL` default) persisted in the catalog, enforced daemonless at statement boundaries (works embedded and in `pinkdb-server` alike) with deterministic WAL logging of automatic refreshes; no α reference exists (DuckDB has no MVs) — semantics documented | done | landed; `REFRESH name`/`… VIEWS` (all-or-nothing), `SHOW MATERIALIZED VIEWS`, `REFRESH MANUAL\|EVERY INTERVAL\|ON COMMIT` enforced at statement boundaries (never mid-statement/transaction; fixpoint for view chains), automatic refreshes WAL-logged + replay-exact; fixed a wave-43 lineage bug (MV DDL was unlogged/unpersisted); `.pink` v5 flags bit 2, bit 3 reserved for wave 79 |
| 81 | JavaScript procedure engine binding (the recorded follow-up to wave 44): bind **quickjs-ng** via the vendored `rquickjs` crate inside `pinkdb-js` — **~1.1 MB engine** (vs Boa's ~30 MB; small binaries are a requirement), enforce the sandbox's `DENIED_INTRINSICS` (deterministic subset: no ambient clock/randomness — rquickjs intrinsic selection), wire `Host` to the executor's bound-parameter `db.exec`/`db.query`, contained-FFI carve-out in `pinkdb-js` (same policy as `pinkdb-ffi`/`pinkdb-ext-loader`), feature-gated `js`; the wasm story documented | done | landed 2026-10-21: quickjs-ng via `rquickjs` 0.14 behind `pinkdb-js/engine` (enabled by `pinkdb/js`); fresh runtime per call, intrinsics BaseObjects+Eval+RegExp+Json+MapSet+Promise with a pre-run lock-down (no `eval`/`Function`/`queueMicrotask`, `Math.random` deleted), host = `db.exec/query/value/random` + `print` over the SQL body's statement path (bound `?` params, same refusals/replay checks/touched sets); step/host-call/memory/stack limits, JS nesting ≤ 4; `Called.output` carries `print` lines (CLI renders, sanitized; protocol/FFI follow-up); one contained `unsafe` (guarded host pointer); release CLI **+1,110,208 B** with `js`, default +160 B; wasm32 compiles but is unverified → native-only; plan + deviations: [docs/wave-81-plan.md](docs/wave-81-plan.md) |
| 82 | Rust toolchain upgrade: bump MSRV `rust-version` (core 1.89 → **1.97**, pink-lake 1.95 → 1.97 — the current stable, matching the dev machine) and migrate every crate **edition 2021 → 2024** (`cargo fix --edition` + rustfmt churn + the edition-specific rules: `unsafe extern`, RPIT capture, tail-expression temporaries, the `gen` keyword); update the CI MSRV job (`MSRV (1.89 core)` → 1.97) and any version notes in README/docs; adopt new-stable ergonomics where clean (e.g. **let-chains**, stable since 1.88) | done | landed; all 7 crates at **MSRV 1.97 / edition 2024** (dependency floor check: highest dep is `home` at 1.88), CI `MSRV (1.97)` job runs `cargo check --workspace --all-targets`, wasm32 check now passes under rustup, ~60 let-chain collapses via `clippy --fix`, edition fallout hand-fixed (`gen` → `generation` ×93, `unsafe(...)` attributes, test-only `set_var` unsafe, macro `expr_2021`); 1895 test passes, duckdiff 171/171 |
| 83 | Procedure transaction control, **Postgres-style**: bodies may `COMMIT`/`ROLLBACK` **only when the `CALL` has no enclosing transaction** (a standalone call is the procedure's own transaction manager); inside an explicit transaction, transaction control is refused with a clear "started at a different scope"-style error. Compare-and-document vs Snowflake (probed, Oct 18): Snowflake allows scoped/autonomous transactions in procedures but is **non-atomic** when called outside one — the trap pinkdb already avoids by making a standalone `CALL` atomic; Snowflake-style scoped-autonomy (inner commit survives an outer rollback) stays a documented follow-up unless requested. Also includes block-scoped `EXCEPTION` handlers (savepoint-under-the-hood, plpgsql-style) | done | landed 2026-10-21: body `COMMIT`/`ROLLBACK` only in the outermost procedure of a standalone `CALL` (inside `BEGIN` → "started at a different scope"; nested/`DO`/JavaScript/guarded blocks refused); committed segments logged as self-contained `DO` blocks with every variable pinned to its exact literal (replay never re-runs the procedure); `BEGIN … EXCEPTION WHEN OTHERS THEN … END` with a whole-store savepoint and `SQLERRM`; new public `DO` statement (ADMIN like `CALL`); two quoted-identifier parser bugs fixed; default CLI +35,888 B; plan: [docs/wave-83-plan.md](docs/wave-83-plan.md) |
| 84 | **Query execution profile attached to results**: every statement can return its timings and facts alongside the rows — **parse**, **bind**, **plan**, **optimize**, **execute** (plus total), row/column counts and related properties (statement kind, transaction state, engine version, per-statement decoded-file/column counters where the lake path applies). Delivered as an opt-in engine API (`execute_profiled` returning `(result, QueryProfile)`; plain `execute` stays zero-cost — pinned by a no-overhead benchmark), a **protocol** profile payload on the statement result (protocol bump; server session setting to enable, default off), a `.timer` CLI display broken down by phase, and an FFI accessor; **profiling never affects results, and timings are never WAL-logged** (replay stays phase-blind). `EXPLAIN ANALYZE`-style per-operator timing noted as a possible follow-up | done | landed 2026-10-21: `PinkDb`/`Connection::execute_profiled` → `QueryProfile` (parse/bind/optimize/execute/log/total, rows, columns, kind, in_transaction, engine_version); plain `execute` measured unchanged (21,155 vs 21,185 ns/stmt), profiling +2.1%; protocol **v10** (`SetProfiling`/`ProfilingSet`/`Profile` after each `Done`, sync + async servers, client API); CLI `.timer` phase breakdown local + remote; FFI `pinkdb_set_profiling` + `pinkdb_result_profile_*`; built by delegated DeepSeek V4.1 Flash agents (OpenCode/CommandCode) and reviewed; plan: [docs/wave-84-plan.md](docs/wave-84-plan.md) |
| 85 | **Browser playground + storage**: run pinkdb in the browser on the docs website via wasm — a `pinkdb-wasm` glue crate (wasm-bindgen or JS exports) over the wasm32-unknown-unknown core + the wave-49 linked-registration host surface; an interactive SQL editor + result grid served as self-hosted assets (no CDNs, per site policy). **Storage backends: memory (default, session-only), IndexedDB and OPFS** behind one JS adapter over the engine's `to_bytes`/`load_bytes` snapshot API — the engine stays storage-agnostic and the wasm glue only marshals bytes. Run the engine in a **Web Worker** (OPFS `createSyncAccessHandle` is worker-only; the UI stays responsive), autosave at statement/commit boundaries with debounce + explicit Save/Export/Import, quota/persistence UX (`storage.estimate()` / `storage.persist()`), **Web Locks** for multi-tab safety, atomic saves (IndexedDB transaction; OPFS temp+rename or versioned file + pointer), format-version/corruption refusal with a clear error, and **no COOP/COEP/SharedArrayBuffer requirement** for v1. WAL-grade sync-file semantics over OPFS/IndexedDB (a VFS in the engine, crash-safe append/rename) is a **follow-up**, not v1 — v1 persists whole snapshots. No lake/server; wasm-artifact size + per-query cost budgets. | done | landed 2026-10-21: `crates/pinkdb-wasm` (raw JS ABI, JSON results, no wasm-bindgen; 2,359,016 B) + public `PinkDb::from_pink_bytes`; docs-site **Playground** page (module worker; Memory / IndexedDB / OPFS snapshot adapters with atomic saves, 500 ms autosave, Export/Import, refused snapshots never overwritten, Web Locks read-only second tab, quota/persist UX, no COOP/COEP, no third-party requests); verified in a real browser; CI builds the module and runs a Node smoke test; WAL-grade OPFS VFS remains a follow-up; plan: [docs/wave-85-plan.md](docs/wave-85-plan.md) |
| 86 | **Extension capability: table functions + copy sinks**: grow the extension model so extensions can expose scans/table functions (`read_*`, replacement scans) and COPY sinks — linked model first (registration + chunk callbacks into the engine), the native C ABI for scans as a later, separate step; the enabler that makes Parquet/Arrow/Iceberg installable extensions while `.pink` stays the native default | done | landed 2026-10-21 (linked model): engine-level **extension packages** (`pinkdb::ExtensionPackage` = linked scalars + `TableSource`s + `FileFormat` sinks, registered process-wide) activated per store by `LOAD EXTENSION '<name>'` (atomic collision refusal, same opt-in gate, durable replay, WAL `LOAD` prefixes for package sources/sinks, wasm-compatible); `pinkdb_arrow::package()` / `register_package()`; native C ABI for scans stays the later step; plan: [docs/wave-86-plan.md](docs/wave-86-plan.md) |
| 87 | **Lake format providers**: make the lake's data-file format a runtime extension seam — `.pink` (pinklake) stays built-in; extensions register additional formats (writer/reader/statistics contract), so the lake catalog/commit/attach/S3/encryption machinery works over extension-supplied formats (catalog pins the format id; unknown formats refuse cleanly); Parquet is the first provider and Iceberg reads through the same seam | done | landed 2026-10-21: `pink_lake::format::LakeFormat` provider seam + process-global registry (built-in `pink`, `vortex` with `vortexlake`); catalog `format` ids resolved through it (byte-identical catalogs; unknown ids refused cleanly); `Lake::set_write_format` / CLI `.lake format`; **Parquet** provider (`pinkdb-parquet` writer + `crates/pink-lake-parquet`, CLI `parquetlake`; INTERVAL refused, no lossy write); Iceberg via the same seam stays a follow-up; plan: [docs/wave-87-plan.md](docs/wave-87-plan.md) |
| 88 | **Per-table file formats (mixed lakes)**: a single lake catalog may declare the data-file format **per table** — `.pink` stays the built-in default; Parquet/Vortex are choices resolved through the wave-87 provider seam; the statistics contract must hold for every provider (attach/COUNT/MIN/MAX fast paths render per format); an unknown/unregistered format refuses cleanly at open/attach; changing a table's format is an explicit rewrite/compaction operation (in scope only if small — otherwise a recorded follow-up) | done | landed 2026-10-22: per-table `Lake::set_table_format` / `.lake format <id> for <db.table>`; **hybrid pinning** — explicit choices (per table, `set_write_format`, `rewrite_table_format`) record `"format_pinned": true`, default-written tables keep migrating to the saving build's format (wave-45 promise kept; the five migration tests pass unchanged); `Lake::rewrite_table_format` / `.lake rewrite <dir> <table> format <id>`; **per-table refusal** of unavailable formats (`Lake::unavailable_tables`; open skips + warns, attach lists + reads fail, saves carry the entry verbatim, targeted writes/maintenance refuse, lake-wide maintenance skips); one shared **format contract** test for pink/vortex/parquet, which found and fixed a Vortex DECIMAL/INTERVAL panic and lossy extreme-INTERVAL statistics; plan: [docs/wave-88-plan.md](docs/wave-88-plan.md) |
| 89 | **Lake replication**: keep a read-only replica lake in sync with a primary without manual copying — for lake→lake the delta is **version-based** (primary manifests + only the data files the new versions reference; no row-level CDC needed), resumable by version, hash-verified; **copy** mode (true DR: bytes into the replica's storage) and **share** mode (register source files in the replica catalog, zero copy — only when both sides reach the same store/reachable URIs; retention + GC guards so replica cleanup never deletes source bytes); replica marked read-only (`replica of <primary> @ <version>`, writes refused; copy-mode failover = stop replication, attach read-write); manual **catchup** (the testable core) + an optional **in-process supervisor** (interval/backoff/pause/resume, process-scoped, off by default); state + `status` (lag, snapshot, `last_error`) in the replica catalog; safety requirements borrowed from the ducklake-vortex catalog-replication implementation: per-table error isolation, ownership guards (never clobber unmanaged dest tables), state written only after commit, overlapping-cycle guard, expired-version → re-seed, source-table-drop reconciliation, identifier/RBAC tests; waves 87/88 make it format-agnostic (mixed-format lakes replicate) | done | landed 2026-10-22, **reshaped to ducklake-vortex's job model** (whole-lake byte mirror and share mode deferred): `pink_lake::replication` — jobs + per-table state in the destination (`replication/state.json`, conditional writes, sealed when keyed), lake or session-store sources, strategies watermark / snapshot (CDC replay + verify, reseed on full-save versions, expired history, identity change or mismatch) / merge (PRIMARY KEY) / full, include/exclude globs, stale-table drops, ownership claims, per-table failure isolation, schema drift (same / additive / recreate), catalog `table_id`, cycle guard; `Supervisor` (thread per destination, 250 ms minimum interval, backoff to 60 s, abort at table boundaries; store-source jobs not supervised); CLI `.lake replicate create\|catchup\|start\|stop\|pause\|resume\|resume-all\|status\|tables\|drop`; bit-exact value matching; plan: [docs/wave-89-plan.md](docs/wave-89-plan.md) |
| 90 | **Auto-published builds + extensions & install script**: tag-driven release pipeline that builds and publishes the platform matrix (linux x86_64/aarch64, macOS arm64/x86_64; Windows best-effort and documented if unsupported) for the CLI feature variants (default, `pinklake`, `pinklake,s3`, `vortexlake`, `vortexlake,s3`, lean) and the server variants (default, `async-server`, `tls`), smoke-testing every artifact before it is published, with a `SHA256SUMS` manifest + immutable name+version+platform policy (the existing extension-repo rule); extensions auto-build per platform from their Makefiles and auto-publish on the same tag (replacing today's manual-only dispatch); a portable `install.sh` (OS/arch + variant detection, latest or pinned version, checksum verification, installs to `~/.local/bin` with PATH guidance; `install.ps1` if Windows ships) and a website install section fed from the release metadata; signing (Ed25519) stays a follow-up | done | landed 2026-10-20: `.github/workflows/release.yml` (`v*` tag + manual version dispatch; 36 rows = 24 CLI × 12 server over linux x86_64/aarch64 and darwin arm64/x86_64; every artifact built → smoke-tested → tarballed → checksummed before upload; one `contents: write` publish job downloads all, validates `SHA256SUMS`, refuses an existing release, creates a draft, uploads and publishes). `scripts/release/{matrix.py,package.sh,smoke.sh,manifest.sh,build-extensions.sh,extensions.py}` are the single matrix/ownership definition and are exercised locally. `publish-extensions.yml` now also triggers on `v*` and builds per-platform (linux-x64/arm64, darwin-arm64/x64) with a fan-in single signed catalog publish + one Worker deploy; existing `name+version+platform` coordinates are skipped and independently refused by `publish.py`. New `install.sh`: latest or pinned, variant mapping, checksum fail-closed, archive path checks, atomic install to `${PREFIX:-$HOME/.local}/bin`, PATH guidance, `--dry-run`/`--force`, no sudo, temp cleanup; test-only `PINKDB_API_BASE`/`PINKDB_RELEASES_BASE`/`PINKDB_OS`/`PINKDB_ARCH` overrides. `docs/releasing.md`, static site install page + nav/home links, CLI `-V/--version`, Windows documented unsupported (no `install.ps1`). Tests: installer **23/23**, release tooling **12/12**, actionlint/YAML/`bash -n`/`sh -n`/shellcheck/py_compile clean, website 24 pages + 12 assets; a real local artifact install ran `pinkdb 0.1.0` and `json_valid('{}') = true`; packaging is reproducible (identical hashes); darwin-x64 cross build smoked under Rosetta; extension staging arch-checked for darwin-arm64/x64. Gates: CLI 77 → **78**, workspace 1282 → **1283**, fmt/clippy both, duckdiff skipped (engine untouched). Local artifacts: default 1,694,159 B, lean 1,632,628 B, server default 1,508,280 B, pinklake 2,283,259 B. Residual: runner availability (`ubuntu-24.04-arm`, Rosetta), Windows unsupported, Cargo-linked core extensions/SDK examples are not catalog entries, unauthenticated GitHub API rate limit for `latest`, **final gate passed 2026-10-22: tag `v0.1.0` (b0be682) ran Release on real runners — all 36 artifacts built + smoke-tested, `SHA256SUMS` validated, immutable release published (73 assets); a downloaded darwin-aarch64 artifact verified against `SHA256SUMS` and ran, and `install.sh` installed pinned `0.1.0 pinklake-s3` and resolved `latest` against the live release**; Publish extensions built all four platforms but its publish step got HTTP 403 (Cloudflare rejects Python's default `Python-urllib` user agent when `publish.py` reads the live catalog) — fixed in d20d7a4, re-published by a `platform=all` dispatch, and a failed upload leaves a hidden draft that blocks tag reuse until cleaned up. |
| 91 | **Docker image (public, GHCR)**: containerized `pinkdb-server` (optional CLI entry in the same image) via a committed multi-stage Dockerfile (static/musl or slim-glibc runtime — pick by measured size), `linux/amd64` + `linux/arm64` buildx manifests, non-root user, `/data` volume, healthcheck (protocol `Ping`), entrypoint flags/env for host/port/file/transport and the TLS/RBAC options; published publicly as `ghcr.io/openlearnia/pinkdb:<version>` (+ `latest`, + `-slim`/lake tags aligned with the wave-90 matrix) on release tags using `GITHUB_TOKEN` (`packages: write`), with provenance/SBOM; website gains a `docker run` quickstart; image signing/attestation beyond provenance is a follow-up | done | landed 2026-10-22: multi-stage `Dockerfile` (static musl on Alpine, measured vs slim glibc: 36.6 vs 170 MB; `lake` variant dynamic musl, 101 MB, because of a `bindgen`/libclang build dependency), non-root UID 10001, `/data` volume, port 5433, `docker/entrypoint.sh` (CLI pass-through, `PINKDB_*` env), `pinkdb-server --health-check` (protocol `Ping`; TCP accept when TLS is configured), `scripts/docker/smoke.sh`, `.github/workflows/docker.yml` (native amd64 build + smoke on relevant pushes/PRs; tag publish of linux/amd64+arm64 to `ghcr.io/openlearnia/pinkdb:<v>`/`latest`/`<v>-lake`/`lake` with provenance + SBOM), website Docker page; first images publish on the next `v*` tag; plan: [docs/wave-91-plan.md](docs/wave-91-plan.md) |
| 92 | **Other languages & engine bindings**: thin bindings over the stable C ABI (single source of truth, `pinkdb_abi_version` gated) for **Node.js** (N-API addon), **Java** (JNI binding + **JDBC** driver), **C#/.NET** (P/Invoke package), plus a **DB-API 2.0** polish of the Python binding (`sqlalchemy` dialect) and a standalone Go module release; **engine surfaces**: an **ADBC** driver (in-process via the deferred Arrow C Data Interface from wave 71, or remote over the v9 protocol — decide and document; JDBC/ADBC share the conformance corpus), with ODBC and ecosystem connectors (dbt/Spark/DataFusion) recorded only as demand-driven follow-ups; packaging per ecosystem (npm, Maven/GitHub Packages, NuGet, PyPI wheel, Go module) wired into the wave-90 release train; one shared cross-binding conformance suite (types, NULLs, prepared statements, errors, transactions) so every binding is verified against the same corpus | done (first pass) | landed 2026-10-24 (worktree `wave/92-bindings`): **shared corpus** `bindings/conformance/corpus.json` — language-neutral JSON (typed cells: bigint/date/timestamp/decimal/interval as canonical text) with 8 cases run by every binding: all nine SQL types round-tripping (NULLs, i32/i64 extremes, unicode, NaN/±Infinity, DECIMAL scale, DATE/TIMESTAMP extremes), positional parameters through `prepare`, stable error classes, BEGIN/COMMIT/ROLLBACK plus autocommit, multiple and empty result sets, and a 100k-row case read row-at-a-time; **Node.js** (`bindings/node`, napi-rs addon that statically links `pinkdb-ffi`): `open`/`close`, `run` (one result per statement), `query` (row objects), `prepare` with `run`/`all`/`iterate` (lazy row stream over the native result), explicit transactions, typed mapping (BIGINT→BigInt, DECIMAL/INTERVAL→canonical string, DATE/TIMESTAMP→string), `PinkdbError.class`, load-time ABI gate, `npm pack` only (package marked private); **Python** (`bindings/python`): PEP 249 DB-API 2.0 layer over the existing `ctypes` wrapper — `connect`/`Connection`/`Cursor`, `execute`/`executemany`/`fetchone`/`fetchmany`/`fetchall`, `description`, `rowcount`, iteration, `paramstyle="qmark"`, the exception hierarchy mapped from the FFI error class, the type objects, and a documented autocommit deviation — plus a **real wheel** (`setup.py` runs cargo and bundles `libpinkdb_ffi`; platform tag, installed into a throwaway venv and exercised from outside the checkout); **Go** (`bindings/go`): the module now carries the ABI subset in its cgo preamble instead of including a header outside the module, so `go get` works, with `PINKDB_LIB_DIR`/`CGO_LDFLAGS` linking outside a checkout and an `abi_test.go` header-sync check; **FFI**: `pinkdb_last_error_class()` (additive to ABI v1; ten stable classes) so bindings report errors without parsing messages; CI: Node + Go + Python in `bindings`, wheel build/install/corpus in a new `python-wheel` job. **Remaining**: JNI+JDBC, C#/.NET, ADBC (in-process vs remote undecided), ODBC, npm/PyPI/Maven/NuGet publishing on the wave-90 release train, SQLAlchemy dialect, Go result streaming and a released Go module |
| 93 | **Extension SDK refresh: current C ABI + boundary & trust hardening** (added 2026-10-20, user; split into **93a** SDK/ABI/integrity work — done — and **93b** SQL `INSTALL` + a `repo`-feature release variant): bring the **C, C++ (today only an example), Go and Rust** SDKs to the complete current native ABI after wave **86** — `struct_size` negotiation before optional fields, `register_scalar_flags` / `PINKDB_EXT_FLAG_VOLATILE`, the full type-tag set, init-only host-API copy, `abi_version`/name limits, host `alloc`/`free` ownership, and whatever table-function/scan/copy-sink registration 86 lands (if scans stay linked-only, the SDKs say so and the harness covers the ABI that exists). Align every template, example, README and smoke with the post-C-2 security posture: loading is opt-in (`--allow-load-extension` / `PINKDB_ALLOW_LOAD_EXTENSION=1`), `--extension-root` confinement, no bare/`..`/symlinked paths, stable absolute paths for WAL replay, and the trust model that locally loaded libraries are **unsigned** (Ed25519 catalog signing covers `.install` only; server loads run with server privileges). Add a **single ABI-conformance fixture** that all four SDKs build and the loader suite drives, with shared boundary tests: NULL/zero-length/oversized values, invalid tags/flags, `n_args`/`text_len`/name bounds, panic/unwind containment (Go/Rust/C++), foreign-thread callbacks, error-path frees, and old-host/new-host `struct_size` behavior; optionally factor the copy-paste Go shims into a reusable package and add a header-only C++ RAII wrapper. Decide and implement **local-extension integrity** (path+SHA-256 allowlist for CLI/server, or close the `dlopen` TOCTOU with descriptor-based loading) so H-7's residual risk is mitigated or explicitly documented. Gate: header sync, all SDK smokes in CI, the conformance matrix, and allowlist refusal/pin tests. | done | **93a done 2026-10-25**: `ext-sdk/{c,cpp,go,rust}` all at ABI 1.2 — `struct_size` negotiation (`PINKDB_EXT_API_V1_SIZE`/`_FLAGS_SIZE`/`_MINOR2_SIZE`, `PINKDB_EXT_HOST_HAS_FLAGS`/`_EXECUTE`, `pinkdb_ext_copy_api`), `register_scalar_flags` + `PINKDB_EXT_FLAG_VOLATILE`, the full tag set incl. INTERVAL bit-packing, init-only host copy, name/arity/tag bounds, host `alloc`/`free` ownership, `execute_statement` + the optional `pinkdb_ext_serve`; a header-only C++ RAII SDK (`ext-sdk/cpp`); a Go package instead of per-example shims; **one conformance fixture** (`ext-sdk/conformance/README.md`) built by all four SDKs and driven by `crates/pinkdb-ext-loader/tests/conformance.rs` (tag round-trips, NULL/zero-length/64 MiB+1 values, unwind containment, foreign threads, error-path frees, old/new `struct_size` in a child process) plus `tests/conformance_ownership.rs` (allocation counters); **local-extension integrity** as a SHA-256 path allowlist in the loader (`set_pins`/`--extension-pin PATH=SHA256`, unpinned/tampered/mutated-on-disk refusals, identity re-check just before `dlopen`, residual TOCTOU documented); `ext-sdk/posture.sh` (13 refusals: opt-in, root confinement, bare/`..`/symlink, pins) wired into CI with the four smokes, the C++ build and `PINKDB_CONFORMANCE_STRICT=1`. **93b done 2026-10-25**: SQL `INSTALL name [FROM '<repo-or-url>']`, `FORCE INSTALL …` and `LOAD <name>` — the parser uses non-reserved `word("install")`/`word("force")` (no keyword-table change) so both stay usable as identifiers, and `.install` is now a thin alias of the same engine path (`crates/pinkdb/src/install.rs`; the repository client moved from `crates/pinkdb-cli/src/repo/` to `crates/pinkdb/src/repo/` behind the new engine feature `repo = [ureq, serde_json, sha2, ed25519-dalek, base64]`, with the CLI's `repo` forwarding to it and its own deps dropped). Signature verification/catalog/cache rules unchanged; the deleted `stage_for_loading` temp-copy is replaced by a re-hash of the cache file immediately before `load_extension` (the cache path is what the WAL records, so replay can reload it; `LOAD <name>` trusts the per-user cache — content-addressed entries/descriptor loads remain recorded follow-ups). Gating: a no-repo build fails with `this build has no extension repository support (rebuild with --features repo, or use the pinkdb-repo release variant)`; both statements take the `LOAD EXTENSION` connection opt-in (`sql_load_gate`, message names `--allow-load-extension`, so a server's flag covers remote clients), are refused inside a transaction, are ADMIN-only in RBAC (`read_checked` + `check_statement`), and are refused by the MCP read-only policy (unit + integration tests). **WAL**: `INSTALL` is not logged (`writes_tables` false; a download is not replay-deterministic and replay never touches the network), `LOAD <name>` is logged like `LOAD EXTENSION` and `resolve_replay_extension` re-resolves it; pinned by `exec::tests::install_is_not_wal_logged_but_load_by_name_is`. New CLI release variant **`repo`** (features `repo`, native only, asset `pinkdb-<ver>-<os>-<arch>-repo.tar.gz`) in `matrix.py` (CLI 16 → 20 rows), `package.sh`, `smoke.sh` (`.help` advertises the SQL surface, `.install` usage runs), `install.sh`, `tests/release/test_package.sh`, `release.yml` and `docs/releasing.md`; measured darwin-aarch64 release binary **7,040,240 B** vs default **4,920,832 B** (tarball 3,161,246 vs 2,151,249 B). Tests: SQL end-to-end against the loopback fixture repository (install, `FROM`-only with no `repositories.json`, `LOAD` in a fresh process, `FORCE` re-download, tampered signature refused with zero artifact requests, gate fires before the network), RBAC/MCP refusals, no-repo messages in `repo_disabled.rs`, `install.rs` cache-resolution units. Gates: workspace **1,914** tests, `pinkdb-cli --features repo` **123**, `pinkdb --features repo` **912**, release tooling **12/12**, wasm32 build, fmt + both clippy runs |
| 94 | **Three-part column references (`db.table.column`)**: support a database qualifier on column references wherever a column may appear — SELECT lists, WHERE/HAVING/GROUP BY/ORDER BY, join conditions, subqueries/CTEs, UPDATE `SET` targets, and `db.table.*` — resolving `db.table` through the same catalog lookup as the FROM clause. Define the alias interaction explicitly (an explicit table alias hides the original table name, consistent with the existing resolution rules; decide and document whether the unaliased form is also accepted), and make unknown-database/unknown-table/ambiguous-column errors name the full reference. The current workaround (`FROM db.table AS x; SELECT x.col`) must keep working unchanged. Parser (PEG grammar) + AST column-ref qualifier + binder/expression resolution + EXPLAIN/DESCRIBE rendering; tests for every clause position, joins, aliases, CTEs/subqueries, case-insensitivity, quoted/multi-word identifiers, the error cases, and the alias workaround. No storage or plan-format change. | done | landed 2026-10-22: `db.table.column` in every clause + `db.table.*` in projections, qualified `UPDATE … SET` targets (`t.c` / `db.t.c`, must name the target), resolution through the FROM lookup with the current database for unqualified entries, aliases hide the original name (error names the alias), cross-database same-name joins admitted with two-part `t.c`/`t.*` ambiguous (old binder bound the first entry that had the column), four-part references refused naming the reference; durable MV/macro round-trips; duckdiff battery (7 probes); correlated subqueries remain a v1 limit; plan: [docs/wave-94-plan.md](docs/wave-94-plan.md) |
| 95 | **SQL dialect completeness ("all query types")**: make the supported dialect explicit and close the remaining query-form gaps — publish a **conformance matrix** (supported / divergent / refused with a precise error; DuckDB parity where applicable), then implement in batches: `VALUES`/`TABLE` query forms, `WITH RECURSIVE`, `GROUPING SETS`/`ROLLUP`/`CUBE`, `QUALIFY`, `DISTINCT ON`, `LATERAL`, named `WINDOW` clauses + `FILTER (WHERE …)`, ordered-set aggregates (`WITHIN GROUP`), `INSERT … ON CONFLICT`/`REPLACE`, `UPDATE … FROM`, `DELETE … USING`, `RETURNING`, `MERGE` (if in scope), `TABLESAMPLE`, `FOR UPDATE`/`SHARE`, `FETCH FIRST`, set-operation `ALL`/`CORRESPONDING`, row constructors, `IS DISTINCT FROM`, `SIMILAR TO`, `LIKE … ESCAPE`, `AT TIME ZONE`, plus the expression/type forms they need; **added 2026-10-22 (DuckDB 2.0 parity):** data-modifying CTEs (`INSERT`/`UPDATE`/`DELETE`/`COPY` as CTE steps), JSON mutation functions (`json_set`, `json_insert`, `json_replace`, `json_remove`), `$name` variable shorthand (with `SET VARIABLE`/`getvariable` if missing), and well-defined `MERGE`/`UPDATE … FROM` semantics when several source rows match. Extend the PEG grammar, AST, binder, planner and executor together, with EXPLAIN/error coverage; coordinate with wave 94 (`db.table.col`). Every unsupported form must fail with a clear diagnostic, never silently misparse; add a conformance corpus (SQL logic tests + duckdiff probes) and gated tests per batch. | done | parser + binder + planner + executor; large; inventory first, then batches; after 94. **95a done 2026-10-25**: the conformance matrix is [docs/sql-conformance.md](docs/sql-conformance.md) (every row-95 form: supported / divergent / refused with the exact error, DuckDB 2.0.0 as the reference, each assigned to 95a/95b/95c), and batch 1 landed: `QUALIFY`, `TRY_CAST`, VARIANT/string subscripts (`v['a']`, `v[0]`, `v.a`, `'hello'[2:4]`), `IS [NOT] DISTINCT FROM`, `VALUES` as a query and in `FROM` with column aliases, `TABLE t`, `FETCH FIRST n ROWS ONLY`/`OFFSET … ROWS`, `LIKE … ESCAPE`, `SIMILAR TO` (a hand-written anchored-regex engine, no new dependency), `DISTINCT ON`, aggregate `FILTER (WHERE …)`, named `WINDOW` clauses, `GROUPING SETS`/`ROLLUP`/`CUBE` with `GROUPING()`, `WITH RECURSIVE` (semi-naive iteration; 100 000-iteration and 10 000 000-row bounds, both errors), and `UNION`/`INTERSECT`/`EXCEPT` `ALL`; every EXPLAIN shows the new operator (`Values`, `Recursive step (UNION ALL)`); `crates/pinkdb/tests/sql_conformance.rs` pins 24 shared-corpus cases against DuckDB (re-measured when the reference binary is present); no query regression (release: Q1 0.012→0.012 s, Q6 0.007→0.007 s, group/sort 0.131→0.132 s, medians of six interleaved runs). **95b done 2026-10-25:** the DML forms — `INSERT … ON CONFLICT [(cols)] DO NOTHING | DO UPDATE SET … [WHERE …]` with `excluded.*`, `INSERT OR REPLACE`/`INSERT OR IGNORE`, `UPDATE … SET … FROM`, `DELETE … USING`, `RETURNING` on every DML form (the post-BEFORE-trigger row), `MERGE INTO … USING … ON … WHEN MATCHED [AND] THEN UPDATE/DELETE | WHEN NOT MATCHED [BY TARGET|BY SOURCE] [AND] THEN INSERT`, and data-modifying CTEs (`WITH x AS (INSERT/UPDATE/DELETE … RETURNING …) SELECT … FROM x`, top-level only; COPY-as-CTE refused); semantics probed against DuckDB 2.0.0 and pinned in `crates/pinkdb/tests/sql_conformance.rs` (25 DML cases, re-measured on DuckDB) + `bench/duckdiff.py` (`dml-*` battery): UPDATE…FROM takes the last matching source row (DuckDB's rule, no error), MERGE refuses a target row affected twice with DuckDB's message, ON CONFLICT dedupes a statement's own duplicate keys to the first row; **PRIMARY KEY/UNIQUE are now enforced** (duplicate key, NOT NULL on a PK, atomic rollback; UNIQUE accepts many NULLs, composite keys work; FKs stay metadata) because ON CONFLICT over a non-enforced key is incoherent — cost 0.095→0.381 s median on a 1 M-row INSERT…SELECT into a PK table (≈3.2 M keys/s), bulk UPDATE/DELETE unchanged (0.025/0.019 s); RETURNING/CTE images ride the trigger capture, so WAL replay stays statement-text (crash/replay test in `crates/pinkdb/tests/dml_replay.rs`); RBAC checks every table of a MERGE/CTE (per-action privilege on the target, SELECT on sources), the MCP read-only allowlist refuses all new kinds and a writing `WITH`. **95c done 2026-10-25 (row 95 done):** the rest of the matrix — `LATERAL` (uncorrelated only: a derived plan has no outer scope, so a correlated subquery is refused by name) and comma-`FROM` as a left-deep `CROSS JOIN` chain; ordered-set aggregates (`percentile_cont`/`percentile_disc`/`quantile_*`/`mode()` `WITHIN GROUP`, one `ORDER BY` key, constant fraction, `FILTER`, ties to the smallest mode); `TABLESAMPLE`/`USING SAMPLE` (`bernoulli`, `system`, `reservoir`; `ROWS`/`PERCENT`/`%`) with a deterministic `REPEATABLE (seed)` (own generator, so the picked rows are pinkdb's own); `FILTER` on a window aggregate; windows built on another window (inherit `PARTITION BY`/`ORDER BY`, DuckDB's three error texts); row constructors in comparisons and `IN` lists (field-wise, NULL-as-a-value, DuckDB's exact answers); `AT TIME ZONE`/`timezone()` as a UTC-family identity on the naive `TIMESTAMP` with any other zone refused for the missing time zone database; the JSON mutators in the engine (`json_set`/`json_insert`/`json_replace`/`json_remove` on JSON text and on `VARIANT`); `SET VARIABLE`/`getvariable`/`$name` (session value spliced at bind time, `getvariable` of an unset name is `NULL` while `$x` errors); `FOR UPDATE`/`FOR SHARE`, list literals (needs a `LIST` type), a `VARIANT` slice and other subscripts stay refused with a named sentence. Every EXPLAIN shows the new operator (`Sample reservoir(1 ROWS) REPEATABLE(7)`); `crates/pinkdb/tests/sql_conformance.rs` grew to 26 batch-3 cases re-measured on DuckDB, and `bench/duckdiff.py` to 376 passing probes. `MERGE` and `UPDATE … FROM` now hash-join their equality keys instead of nest-looping: 1 M × 100 k `MERGE` 4.9 s and `UPDATE … FROM` 0.28 s (the old code is ~9 h at that size, extrapolated), 100 k × 10 k 340.2 s → 0.47 s and 318.2 s → 0.027 s; no query regression (Q1 0.012→0.012 s, Q6 0.007→0.007 s, group/sort 0.1285→0.1295 s, medians of 12 interleaved runs, distributions overlapping). |
| 96 | **Stateless pinkdb: engine metadata in the lake catalog** (added 2026-10-22, user; re-scoped by wave 97's audit). The 96a half — pluggable SQLite/Postgres lake catalogs — shipped in wave 103, which put every catalog on the DuckLake v1.1 schema (`pink:` / `sqlite:` / `postgres://`, `ducklake_*` tables). **96b phase 1 done (wave 96b):** `pinkdb-server --catalog <pink:|sqlite:|postgres://…> --data <dir|s3://…>` serves a store with no local state — the engine catalog (databases, tables with constraints/defaults/indexes, views, macros, materialized views, procedures, triggers, RBAC, extension sources) is one `pinkdb_engine_catalog` row beside the `ducklake_*` tables, rows are lake data files, and one catalog transaction publishes data + metadata. Design, the WAL's narrowed role and the crash story: `docs/wave-96b-plan.md`. **Phase 2 (scoped, open):** several servers on one catalog — fencing tokens/epochs, cache invalidation (version or `LISTEN`/`NOTIFY`), an append-only engine image, partial-image publishes. | 96b phase 1 done | `crates/pinkdb-server/src/catalog.rs`, `crates/pinkdb/src/catalog_image.rs`, `crates/pink-lake/src/engine.rs`, `docs/wave-96b-plan.md` |
| 97 | **Stale-row audit: 40h / 40i / 40j / 96** (added 2026-10-22): re-checked each early queued row against what later waves shipped — 40h, 40i and 40j closed (every item shipped, obsolete, or merged into 92 / 93) and 96 re-scoped to 96b, the half wave 103 did not cover. No new implementation was needed. | done | `docs/wave-97-audit.md` (item, verdict, evidence) |
| 98 | **Async remote I/O + streaming parallel scans** (added 2026-10-22, user; the DuckDB 2.0 gap — their TPC-H Q6 on a 22 GB S3 Parquet file went 8.23 s → 2.84 s with a read-ahead I/O pool). First verify and fix that remote scans **stream** row groups/chunks into the executor instead of materializing whole files (an in-memory engine must still scan SF100 `lineitem`, 600M rows, within bounded memory). Then a dedicated **read-ahead I/O pool** in `pinkdb-httpfs` issuing byte-range requests ahead of decode with a bound on in-flight bytes/requests, adjacent-range coalescing and retries, decode overlapping download, and parallel row-group decode across workers; applied to Parquet first, then CSV (`read_csv` over `s3://`/`https://`) and lake data files on S3 (`.pink`, Vortex, Parquet providers; attach/projection/pruning paths). A setting like DuckDB's `read_ahead_depth` (auto / 0 = old behavior) and an optional **remote-file cache** (on-disk, size-capped, keyed by URL + ETag, off by default). Benchmarks: TPC-H Q6 SF10 (+ SF100 where hardware allows) on S3/R2 Parquet and CSV vs synchronous pinkdb and DuckDB 2.0, local and same-region numbers recorded honestly; no regression for local files. **Benchmark data is staged (2026-10-22)** in R2 bucket `pinkdb-bench` (layout in `tpch/README.txt` there): TPC-H SF1 + SF10 Parquet (DuckDB writer, Snappy, 122,880-row groups; SF10 `lineitem` 2.1 GiB), SF1 CSV, SF1 Arrow IPC and `tpch.pink` written by pinkdb, and three SF1 lakes saved straight to R2 by pinkdb (`.pink` 680 MiB, Parquet 642 MiB, Vortex 248 MiB with DECIMAL cast to DOUBLE). CI reads it through the `R2_*` secrets/variables. Findings from staging it, to fold into this or follow-up waves: **Vortex lakes cannot store DECIMAL** (all TPC-H money columns), **pinkdb's Parquet lake writer is ~2× DuckDB's size** (642 MiB lake vs ~310 MiB of DuckDB Parquet for the same data: no compression/dictionary yet), and **footer-only attach of the Vortex lake over R2 took 7.6–16 s** vs 1.3–1.5 s for the statistics-only `.pink`/Parquet lakes (serial footer reads — a read-ahead target) | done | **98a done 2026-10-22** (remote Parquet: projected column-chunk ranges coalesced, bounded concurrent read-ahead, footer-only `count(*)`, `(N of M columns)` in EXPLAIN — R2 SF1 Q6 406 s → 4.3 s vs DuckDB 2.0.0 3.9 s, SF10 at parity; plan + numbers: [docs/wave-98a-plan.md](docs/wave-98a-plan.md)); **98b done 2026-10-22** (lake data files: ranged/projected/concurrent reads, concurrent Vortex footers, parallel whole-object parts — R2 SF1 lake Q6: Parquet 22.9 → 6.2 s, Vortex 31.4 → 4.5 s, `.pink` 31.7 → 20.4 s vs DuckLake 1.9–3.9 s; plan + numbers: [docs/wave-98b-plan.md](docs/wave-98b-plan.md)); **98c done 2026-10-24** (streaming scans: an aggregate over `read_parquet('<url>')` or an attached lake table folds each decoded row group in parallel workers instead of collecting the scan — with the fold pinned at two workers, executor-held rows stayed within `2n+1` chunks (2 000 rows on a 200-row-group/80 000-row mock-S3 file, 10 240 on a 60-file/180 000-row lake), a bound that does not follow the file size; a lake streams one data file at a time (`.pink` files chunk by chunk), so its bound is the largest data file; no regression, interleaved A/B against HEAD (5 alternating runs, medians, local SF1): lake Q6 0.340 → 0.337 s, lake Q1 0.390 → 0.396 s, `read_parquet` Q6 0.212 → 0.210 s, HTTP-served `lineitem` Q6 1.317 → 1.315 s; Parquet writer Snappy + dictionary with a per-chunk plain fallback: SF1 lineitem 388 467 625 → 238 298 565 B (DuckDB 205 471 342 B), write 3.4 → 3.7 s; CSV `s3://`/`https://` read-ahead: 1 MiB ranged GETs 8 MiB ahead, one in flight, retried mid-file, parsing overlapping the download (mock test: 12 MB/800 000 rows, spans tile the file); optional on-disk remote-range cache, off by default — URL+ETag keyed, LRU-capped, 0600/0700, corrupt entry a miss — `PINKDB_EXTERNAL_FILE_CACHE=1` (+ `_DIR`, `_BYTES`, default 1 GiB) or `--file-cache DIR`); pinkdb-httpfs + parquet ext + engine scan path + pink-lake readers; after 96a |
| 99 | **Agent-friendly CLI mode** (added 2026-10-22, user; DuckDB 2.0 "agent mode" parity): detect an agent-driven session (non-TTY plus known agent environment markers) or `-agent` / `-no-agent` flags; compact Markdown table output instead of boxed tables, explicit truncation notices (rows/columns cut and totals), errors as one-line JSON (`{"error": …, "kind": …, "position": …}`), runaway-query guard (row/time budget with a clear message), and a pre-execution cost hint for expensive statements (from the optimizer's row estimates / `EXPLAIN`); measure output volume on a TPC-H-style script before/after | done | landed 2026-10-24: the reusable formatter/error/estimate module is `pinkdb::agent` (`crates/pinkdb/src/agent.rs`), so the server and extensions share it; the CLI only wires detection, settings and rendering (`crates/pinkdb-cli/src/main.rs`), and the guard uses new `PinkDb::execute_with_limits` / `execute_profiled_with_limits` (`ExecLimits` threaded to every `exec::run` site; the default path stays unbounded). Markers: `AI_AGENT`, `CLAUDECODE`, `CLAUDE_CODE`, `PI_AGENT`, `OPENCODE`, `CODEX_SANDBOX`, `CURSOR_AGENT`, `GEMINI_CLI`; `-agent`/`-no-agent` and `PINKDB_AGENT_MODE=1|0` override; one stderr announcement; never auto on a terminal. Defaults: 100 rows / 32 columns / 500 chars per cell, 1,000,000-row and 30,000 ms budget, 100,000-row cost-hint threshold; `.agent rows|cols|cell|budget|timeout|hint` and `PINKDB_AGENT_*` env vars. Measured on a TPC-H-style script (small locally generated dataset with 10,000-row tables, 10 queries including a 10,000-row result and a missing-table error): normal **485,403 B / 10,114 lines** → agent **4,162 B / 180 lines** (bytes −99.1%, 116.6× fewer; lines −98.2%), same exit code 1 and same error classification (`{"kind":"catalog"}` JSON in agent mode) | pinkdb-cli + new `pinkdb::agent`; small |
| 100 | **Triggers** (added 2026-10-22, user; DuckDB 2.0 parity): `CREATE [OR REPLACE] TRIGGER … {BEFORE\|AFTER} {INSERT\|UPDATE\|DELETE} ON t [REFERENCING OLD TABLE AS o NEW TABLE AS n] [FOR EACH {ROW\|STATEMENT}] [WHEN (cond)] EXECUTE {PROCEDURE p(args) \| <statement>}`, `DROP TRIGGER [IF EXISTS]`, `SHOW TRIGGERS`; multiple triggers per event in a defined order; transition tables; `RETURNING` on triggered tables; recursion/cascade depth bound; triggers run inside the firing statement's transaction (atomic with it); persistence in `.pink` (new flags bit) + WAL replay semantics decided (replay re-fires vs logs effects — pick one consistent with procedures); RBAC (create needs table owner-equivalent privilege; trigger bodies run with definer or invoker rights — decide); interaction with materialized views, lakes (`.lake commit` of triggered changes) and the MVCC touched sets; document unsupported combinations like DuckDB does | done | landed 2026-10-24: `CREATE [OR REPLACE] TRIGGER … {BEFORE\|AFTER} {INSERT\|UPDATE [OF cols]\|DELETE} [OR …] ON t [REFERENCING OLD TABLE AS o NEW TABLE AS n] [FOR EACH {ROW\|STATEMENT}] [WHEN (…)] EXECUTE {PROCEDURE p(args) \| <statement>}`, `DROP TRIGGER [IF EXISTS] name [ON t]`, `SHOW TRIGGERS [ON t]` — executed through the stored-procedure machinery (`ParamSlots` + `run_call_statement`), so bodies reuse the binder/optimizer/executor path and `OLD`/`NEW` are bound as literals (`NEW.col` assignable in BEFORE ROW, `OLD` read-only); triggers fire in creation order per event, REFERENCING tables are AFTER-STATEMENT-only read-only relations (bound as CTEs), recursion is bounded at 16 levels, and a trigger error rolls the whole statement back in autocommit too (`savepoint_restore`). Definitions persist in a new `.pink` footer section under flags bit `0x80` as their `CREATE TRIGGER` text in creation order; the WAL still logs only the firing statement, so replay re-fires deterministically and never double-counts. Invoker rights: creating/dropping needs `CREATE` on the database and every body statement is re-checked against the firing user. Refused: views, attached lakes, `RETURNING` (absent from pinkdb), row-level skips. Tables without triggers keep the untouched batch path (1,048,576-row `INSERT … SELECT` 34.4–34.8 ms vs 34.1–34.3 ms at HEAD, bulk `UPDATE` 19.9–20.4 vs 20.3–20.5 ms, both within run-to-run noise). Tests: 39 trigger integration tests (+1 ignored crash child), CLI lake test; workspace 1,796 tests / 156 suites |
| 101 | **VARIANT type** (added 2026-10-22, user; DuckDB 2.0 / DuckLake parity): a first-class semi-structured type stored columnar (not text) — each row may differ in shape; casts from/to JSON text and from any type; `variant_type()`, `variant_keys()`, path extraction and the JSON functions over VARIANT; `.pink` persistence (new dtype tag), lake data files (pink round-trips, Parquet VARIANT refused naming 101b), ordering/equality/grouping rules defined and documented, FFI/protocol/bindings encodings. **101a done 2026-10-24:** the type + value model (`crate::variant::Variant`, ten kinds, key-sorted objects, depth 128), columnar storage (`VectorData::Variant`), the binary per-value encoding and `dtype_tag` 9 / `VD_VARIANT` 9, casts in every direction (VARCHAR parses JSON; errors name the kind and value), `typeof` / `variant_type` / `variant_keys` / `variant_extract` (JSONPath, 0-based), structural equality with cross-kind numeric folding, a documented total order, grouping/hashing/DISTINCT via one canonical key, no min/max stats, WAL/checkpoint replay, wire protocol v11 (dtype 9, value 10), FFI tag 10 (JSON text), wasm JSON, ext ABI + arrow + parquet refused, `.pink` lake round-trip via the shared format contract; plan in `docs/wave-101-plan.md`. **101b done 2026-10-25:** shredding into typed sub-columns (per-column policy over all of the column's rows; metadata dictionary shared by the row group; residual `value` + `typed_value`; exact round trip), Parquet `Variant` logical type written and read (DuckDB 2.0 interop both ways, test skipped without the binary), `COPY … TO '….parquet'` / `read_parquet` support, path-qualified statistics and zone-map pruning on shredded paths in `.pink` and Parquet lakes (`v['a']` / `variant_extract(v,'$.a')`), carried in DuckLake's own `ducklake_file_variant_stats` table with DuckDB's path spelling (interop tested both ways), scalar-to-VARIANT comparison coercion, extension ABI minor 3 `ExtType::Variant`; measured 1M-row lake sizes, pruning speedups and a TPC-H Q1/Q6 A/B in CHANGELOG + README | done | `crates/pinkdb` (types, vector, variant, persist, binder, expr, plan, storage, csv, prepared, ext), `crates/pinkdb-server` (protocol v11), `crates/pinkdb-ffi`, `crates/pinkdb-wasm`, `crates/pink-lake`, `crates/pink-lake-parquet`, `extensions/{parquet,arrow,json}`; tests: pinkdb `tests/variant.rs` (14) + 7 unit tests, protocol 29, ffI, lake contract (VARIANT round trip + parquet refusal), json extension; plan `docs/wave-101-plan.md` |
| 102 | **Fast `.pink` lake data files: a ranged, projectable layout** (added 2026-10-22, user: "if we need to steal stuff from vortex or parquet so be it"). Today a `.pink` data file (engine persistence v5) is written sequentially — per table, per column, its chunks inline — with **no offset directory** and **one CRC-32 over the whole image**, so a reader cannot locate one column without reading everything before it, cannot verify part of a file, and every lake read downloads and decodes the whole file (R2 SF1 `.pink` lake attach + Q6 31.7 s vs DuckLake 3.9 s; the file is also ~2.2× DuckLake's Parquet). Design a new `.pink` **columnar data-file layout** for lakes, taking what works from both: **from Parquet** — row groups (~122,880 rows) × column chunks, a **footer** at the end (schema, row-group directory with per-chunk byte offset/length/row count, per-chunk statistics: min/max/NULL count/NaN count) with a fixed-size trailer (footer length + magic) so a reader needs one or two small range requests, and **per-chunk CRCs** so any chunk is verifiable on its own; **from Vortex** — lightweight cascading encodings chosen per chunk by sampling (keep today's DELTA_BITPACK/DICT_BITPACK/ALP/LZ, add FSST for strings — tag 9 is reserved with a recorded design — and an ALP-RD/real-double path), FastLanes-style bit-packing that decodes with SIMD-friendly fixed lanes, lazy/zero-copy decode, and statistics rich enough for pruning and exact `COUNT`/`MIN`/`MAX`. Readers: projected + ranged + concurrent through the 98a/98b machinery (only the projected chunks of row groups that survive pruning, coalesced, bounded read-ahead), parallel decode per row group in order, optional catalog-published footer location so attach needs no footer request. Compression target: within ~10–20% of DuckDB's Parquet for the TPC-H set (decide in the plan whether a pure-Rust zstd/LZ4 path is worth it versus the existing hand-rolled LZ; the no-C-dependency rule for default builds stays). **No backward compatibility (user, 2026-10-22: "legacy doesn't make sense for us … we are in early development")**: the new layout **replaces** today's `.pink` file format outright — one current format, no version detection, no conversion/migration path, and the old readers are deleted rather than kept (the current `.pink` v5 lake data-file reader and the engine's v1–v4 `.pink` loaders with their fixtures and compatibility tests); files written by earlier builds simply stop opening, with a clear "written by an older pinkdb" error. **The same rule for the lake catalog (user, 2026-10-22):** drop the catalog `format_version` 1–5 readers (one current version; anything else is refused with the "written by an older pinkdb" error), the legacy fixtures (`crates/pink-lake/tests/fixtures/lake_v1_v4` and the other v1–v5 fixtures) and the migration tests built on them, and the wave 88 **hybrid migration rule** — a table keeps its pinned format on every save and append; changing it is always explicit (`set_table_format` / `.lake format … for …` / `.lake rewrite … format …`), so the `format_pinned` flag and the "default-written tables migrate to the saving build's format" path go away (decide in the plan whether the per-entry `format` field alone suffices, and what each build writes by default for new tables now that `.pink` is fast). The engine's own `.pink` database files move to the same layout (lazy column loading for large local stores) unless the plan finds a concrete reason not to — either way there is one `.pink` format. Tests: round-trips for every type and edge value (the wave 88 contract test), ranged/projected/pruned reads against mock S3 with request-count bounds, corruption per chunk, and a refusal test for a file from the old layout. Benchmark: R2 TPC-H SF1/SF10 `.pink` lake vs the Parquet lake vs DuckLake — target parity with DuckLake on Q1/Q6 and file size near Parquet | done | landed 2026-10-22 — **T1**: `PINKDB06` engine format (row groups, per-chunk CRC32C + stats, footer + trailer, FastLanes, FSST, lz4_flex), v1–v5 readers deleted; TPC-H SF1 713 → 320.8 MB (DuckDB Parquet ≈ 324 MB) after a review fix storing DECIMAL(p ≤ 18) as i64; plan: [docs/wave-102-plan.md](docs/wave-102-plan.md); **T2**: catalog `format_version` 7 only (footer location per `.pink` file; v1–v6 readers, legacy fixtures and migration tests deleted; wave 88 hybrid rule and `format_pinned` removed), the `pink` provider reads only projected chunks of surviving row groups by range — R2 SF1 `.pink` lake attach + Q6 20.4 → **4.56 s** (DuckLake 2.20 s), Q1 19.5 → 4.25 s, lake 680 → 321 MB; persist/encodings + pink-lake `pink` provider; **after 98b** (needs its ranged `Storage`/`DataFile`); plan first |
| 103 | **Lake catalog = a database with DuckLake's exact schema** (added 2026-10-23, user: "we can follow like duckdb"). The catalog is always a database — a pinkdb `.pink` database file (default; the DuckDB-file analogue), SQLite or Postgres — holding DuckLake v1.1's `ducklake_*` tables with `begin_snapshot`/`end_snapshot` per row: a commit inserts its delta (snapshot row = commit point, retry/rebase on conflict) instead of rewriting a JSON catalog (~1.8 KB per data file, written twice per commit, read in full on every open). DuckDB's `ducklake` extension can attach a pinkdb lake with a SQLite/Postgres catalog and Parquet data, and pinkdb opens DuckDB-written DuckLakes. `count(*)` = `SUM(record_count)`. Encryption like DuckLake (plaintext metadata rows, per-file data keys). The JSON layout (`catalog.json`, `snapshots/`, `commits/`, `highest.json`, `lake.json`, sealed manifests, `migrate-catalog`) is deleted, no converter (no-legacy rule). Plan: `docs/wave-103-plan.md` | done | DuckLake tables on `pink:`/`sqlite:`/`postgres://`; JSON layout deleted; catalog transaction flat from 10 → 10,000 files (7.1–9.8 ms, 3.43 MB pink catalog), attach 184 ms; plan: `docs/wave-103-plan.md` |
| 103c | **Errors name the real lake catalog; the last JSON-catalog traces removed** (added 2026-10-26): every error that reports a catalog problem names the catalog as given — `pink:/…`, `sqlite:/…` or the Postgres URL with the password redacted — through one `Lake::catalog_label()` computed at open, so a wrong or unreadable catalog no longer reports a stale JSON file name or a data-root path. `CATALOG_FILE` and every root-joined catalog path are gone; doc comments, README and website pages no longer describe the JSON layout as current; S3 tests use a neutral object key. One test per backend (pink, SQLite, PostgreSQL) removes the catalog's DuckLake metadata and asserts the error names the location (Postgres: password redacted). No behaviour change beyond messages, no new dependencies. Plan: `docs/wave-103c-plan.md` | done | pink-lake 323 tests, pink-lake-parquet 29, pinkdb-cli 189 (1 ignored), workspace 2,118 passed / 12 ignored / 195 binaries; fmt and every clippy set (`-p pink-lake` with `s3`, `s3,encryption`, `catalog-sqlite,s3,encryption`, `catalog-postgres`) green; `grep -rn 'catalog\.json\|CATALOG_FILE\|highest\.json' crates/ README.md website/src` prints nothing |
| 104 | **Remove the Vortex data format** (user, 2026-10-24): pinkdb keeps `.pink` (`PINKDB06`) plus Parquet for DuckLake interop. Delete `vortex_file.rs`, the `vortexlake` cargo feature and the `vortex`/`rayon` dependencies, the Vortex helpers in format/attach/zone/stats/incremental, and the leftover wave-103 crypto (PNKLAKE1 v1 reading, Argon2id KDF; per-file keys are raw). A catalog row naming the removed format fails with a clear "re-save the lake with an older build" error — no reader, no converter. CI/Docker/install release variants drop the feature; tests that used Vortex for general subjects run on `.pink`/Parquet. | done | release `pinkdb-cli --features pinklake,s3,encryption` **8,780,560 B** (HEAD `pinklake` 8,822,192 B; the removed `vortexlake,s3,encryption` set was **51,407,280 B**); dependency tree 422 → **109** lines (HEAD `pinklake` 110); pink-lake 51,714 → **47,865** `.rs` lines (−7.4%; `src/` 26,598 → **23,183**, −12.8%); clean debug build 12.95 → 13.69 s (noise: the removed feature was already off in the `pinklake` set); every gate green |
| 105 | **pinkdb agent bridge: MCP server (`mcp` extension)** (added 2026-10-24, user; **runs right after wave 99**, which it builds on): agentic use of pinkdb **with no LLM code in pinkdb itself**. An agent (pi, Claude Code, any MCP client) supplies the reasoning; pinkdb exposes tools. (1) An **`mcp` extension for both the CLI and the server** (user: "of course this would be an extension for cli and server"): a native pinkdb extension loaded with `LOAD EXTENSION 'mcp'` in `pinkdb-cli` (e.g. `.mcp serve` on stdio) and in `pinkdb-server` (serving the store it already holds), so neither default binary grows. If the current extension ABI cannot host a long-running serve loop with SQL access, extending the ABI (and the wave-93 SDKs) is part of this wave. Transports: stdio and localhost-only HTTP. Tools: `query` (SQL with a row/time budget and wave-99 compact Markdown results plus truncation notices), `describe`, `tables` / `databases`, `explain` (with the cost hint), and lake tools `lake_attach` / `lake_versions` / `lake_tables` / `lake_changes`. **Read-only by default**: writes (DML/DDL, lake commit and maintenance) only with an explicit `--allow-writes`, and a `SELECT` may not reach external files or URLs (`read_csv`, `read_parquet`, bare paths) without an explicit `--allow-files`, and always under the engine's RBAC grants and the pass-5 limits when the store has users. (2) No pi package (user, 2026-10-24: not needed): pi and other MCP clients connect with `mcp add`. Tests: an MCP conformance check, a read-only refusal for every write path, budget/truncation behaviour, and a scripted pi run that answers a TPC-H question end to end. Docs: README + website page. | done | landed 2026-10-24 — `pinkdb --mcp`, `.mcp serve` and `pinkdb-server --mcp-extension` over a new `pinkdb-mcp` cdylib; ABI minor 2 (`execute_statement` + `pinkdb_ext_serve`); 8 schema'd tools; read-only in the host, no external files or URLs without `allow-files`; `pinkdb` 4,221,296 → 4,260,048 B, `pinkdb-server` 3,802,464 → 3,927,120 B, bridge 565,824 B; workspace 1,753 tests and every gate green |
| 107 | **Vector storage and embeddings (extension-first)** (added 2026-10-25, user: "vector storage and embeddings using extensions"). (1) **Core prerequisite: a fixed-size `ARRAY` type** — **107a done 2026-10-25**: `FLOAT[N]`/`REAL[N]` (a real 4-byte element kind, so a 100k×768 embedding table is 292 MiB = 4.00 bytes/element; an element read widens to `DOUBLE`, the one difference from DuckDB), `DOUBLE[N]`/`INTEGER[N]`/`BIGINT[N]`, columnar flat element buffers with a row bitmap and an element bitmap only when a NULL element exists, `[a,b,c]`/`array_value(…)` per-row constructors with the numeric tower, `a[i]`/`array_extract`/`len`/`array_length`, casts `VARCHAR↔ARRAY`/`ARRAY↔ARRAY` (same length)/`ARRAY↔VARIANT`, structural equality and element-wise lexicographic ordering for `ORDER BY`/`MIN`/`MAX`/`GROUP BY`/`DISTINCT`/joins, `.pink` dtype tag 10 + default tag 10 + page encoding tag 11 (row-major or byte-planed, LZ behind it; 100k×768 random `DOUBLE` = 585 MiB = 8 bytes/element, insert/scan 0.31 s each), wire v13 (value tag 11, dtype tag 10, element kind 0–3), C ABI `PINKDB_TYPE_ARRAY`/`PINKDB_TYPE_ARRAY_FLOAT` + Node `Float32Array`/`Float64Array`/Python `list[float]`/Go `[]float32`/`[]float64`, extension ABI **minor 5** `ExtType::ArrayFloat`/`ExtType::ArrayDouble` (pointer+length views, no per-element boxing), Parquet three-level LIST both ways with DuckDB 2.0 (an `f32` element leaf for `FLOAT[N]`), `.pink` lake round trip and DuckLake `list`/`element` catalog rows, and lake data files stamping `field_id = ducklake_column.column_id` (planned before the commit that creates the columns) so DuckDB's DuckLake reader reads exact ARRAY values, including after `ADD`/`DROP COLUMN`; plan: [docs/wave-107-plan.md](docs/wave-107-plan.md); **107a2 queued** (variable-length `LIST`, `unnest`, slicing, `T[]` is a parse error naming it). (2) **A `vss` extension** (DuckDB-vss parity): `array_cosine_distance` / `array_l2_distance` / `array_inner_product` / `array_negative_inner_product`, an **HNSW index** (`CREATE INDEX … USING HNSW (col) WITH (metric = 'cosine')`) used by the optimizer for `ORDER BY distance LIMIT k` top-k, persisted with the store, rebuilt or incrementally maintained on writes, recall measured against brute force. (3) **An `embeddings` extension**: `embed(text [, model])` calling an **OpenAI-compatible embeddings endpoint** (base URL, model and key from env/settings; z.ai / DeepSeek / OpenAI / local servers), batched, cached by (model, text hash), opt-in and network-gated like httpfs, never in the default binary; a recorded mock provider in tests. Benchmarks: top-10 recall and latency for 1M × 768-d vectors, HNSW vs brute force. | 107a/107b/107c done, 107b queued | **107a** (the type, end to end) landed 2026-10-25 — see the (1) note above and [docs/wave-107-plan.md](docs/wave-107-plan.md). TPC-H Q1/Q6 on a 10M-row `lineitem` are unchanged (best of 13 interleaved rounds against a `main` build: 0.1228 s vs 0.1225 s and 0.0634 s vs 0.0633 s), and the release binaries grew 1.10% (`pinkdb`, 5 073 744 → 5 129 344 B) and 1.61% (`pinkdb-server`, 4 686 080 → 4 761 344 B). **107b** (distance functions + HNSW) landed 2026-10-26 — plan: [docs/wave-107b-plan.md](docs/wave-107b-plan.md). Core SQL has `array_distance`/`array_l2_distance`, `array_cosine_distance`/`array_cosine_similarity`, `array_inner_product` and `array_negative_inner_product` over `FLOAT[N]`/`DOUBLE[N]` (DuckDB 2.0 values; `FLOAT` inputs compute in `f32` and return `DOUBLE`); `extensions/vss` (`pinkdb-vss`, CLI/server feature `vss`, off by default) adds a deterministic HNSW index (`M=16`, `ef_construction=128`, `ef_search=64`, `M0=2*M`, metric `l2sq`/`cosine`/`ip`) with a persisted graph (`.pink` footer flag `0x100`), WAL replay, tombstones + `CALL hnsw_compact_index`, session switches `hnsw_exact`/`hnsw_ef_search`, and an optimizer `VectorTopK` scan for `ORDER BY distance(col, constant) ASC LIMIT k` (other shapes stay exact). 100k×768: build 94.5 s, graph 318 MB, recall@10 0.874 at ef 64, p50 0.86 ms vs 18.0 ms brute force; 1M×128: build 867 s, graph 617 MB, recall 0.740 at ef 64, p50 0.49 ms vs 69.6 ms. Non-vector TPC-H Q1/Q6 unchanged within noise; `--features vss` adds 86 KB (1.6%) to the CLI. **107c done 2026-10-25: `extensions/embeddings` = `pinkdb-embeddings`** — `embed(text)` → `FLOAT[N]` from an OpenAI-compatible endpoint (CLI/server feature `embeddings`, off by default); the width is `PINKDB_EMBEDDINGS_DIMENSIONS`, read when the package is published and sent as `dimensions`; there is no default provider (`PINKDB_EMBEDDINGS_BASE_URL` and `PINKDB_EMBEDDINGS_MODEL` are required; z.ai's global API serves no embedding model) and the key is only `PINKDB_EMBEDDINGS_API_KEY` (environment-only, never another provider's variable, redacted from `Debug`, errors and the store, sentinel-tested); a chunk-at-a-time host scalar (`HostScalar::batched`) with in-chunk dedupe sends 128 inputs per request (1000 rows / 300 texts = 3 requests); a 64 MiB LRU plus an optional content-addressed `PINKDB_EMBEDDINGS_CACHE_DIR` answers repeats and survives a restart; `HostScalar::external_io()` makes the read-only MCP gate refuse the call (the new hint points at `allow-files`), and the refusal is enforced at the call site too, so a batch whose `SELECT` cannot bind at check time, a macro, a view, a trigger action or a `CALL`/`DO` body cannot smuggle a paid request past it; deliberately not WAL-volatile, because that guard would refuse `UPDATE … SET v = embed(body)` — instead a logged statement's record carries a **replay memo** of each `external_io` result keyed by (function, arguments) (WAL payload format v5, so older builds refuse the file with the usual version error): a crash replay answers from the memo with no key and no provider and cannot diverge (1,000 rows at 1024 dimensions add 4 133 005 bytes, 4.0 KiB per distinct text); the `DynamicScalar` path is opaque to the planner anyway (no folding, no bind-time call, `WHERE false` makes none). Plan: [docs/wave-107c-plan.md](docs/wave-107c-plan.md). Release `pinkdb` 5 208 656 → 7 252 992 B with the feature (+39.2%), no new crates (ureq 3.4/rustls and sha2 were already locked). **107b** (distance functions, HNSW, optimizer rule) is the remaining extension; it takes and returns a `FLOAT[N]`/`DOUBLE[N]` through minor-5 `ExtType::ArrayFloat`/`ArrayDouble`, and its HNSW index needs an index-side API of its own. **107a2** (variable-length `LIST`, `unnest`, slicing, `array_agg`-style constructors) is queued for when 107b/107c or wave 108 need it. **before 106** |
| 108 | **RAG mechanism** (added 2026-10-25, user), on top of 107: (1) **full-text search** (a BM25 index extension, DuckDB-fts parity: `PRAGMA create_fts_index` / `match_bm25`, stemming and stopwords for English first); (2) **ingestion**: text splitters (`split_text(text, size, overlap)`, sentence/markdown-aware), document loaders over existing readers (files, `read_csv`, httpfs, lakes), and an ingest pipeline into a chunk table (chunk text, source, offsets, embedding); (3) **retrieval**: `rag_search(query, k)` table function doing **hybrid BM25 + vector retrieval fused with RRF**, metadata filters, a character/token budget; (4) exposed as MCP tools through the row-105 `mcp` extension and used by `pinkdb-agent` (106). Evaluation: a small labelled QA corpus with recall@k for BM25, vector and hybrid. | part done | **108a done (2026-10-25): `extensions/fts` = `pinkdb-fts`** — `CALL create_fts_index(...)` / `CALL drop_fts_index(...)` (DuckDB's argument names and defaults), `match_bm25(index, id, query, fields := 'a,b', k := 1.2, b := 0.75, conjunctive := 0)`, `stem(word)` and `rrf(rank, k := 60)`; index stored as a database `fts_main_<table>` (meta/docs/dict/terms/term_stats/field_stats), static like DuckDB (re-create with `overwrite := 1`); DuckDB BM25 verified against a stdlib-Python reference to 1e-9; Snowball `porter` hand-written (2994-word fixture, 0 mismatches); CLI/server feature `fts` (off by default, CLI +3.1%, server +3.4%); `match_bm25` is checked as a `SELECT` on the base table (RBAC) and must name its index as a constant; DuckDB's SMART 571-word stopword list; the default tokenizer is regex-free (`fts-regex` enables a custom `ignore`); read-only MCP ranks with `match_bm25` and refuses index creation; release 100k docs × 2 fields: build 12.3 s, top-10 query 103 ms mean. **Still queued: (2) ingestion, (3) `rag_search`, (4) MCP tools.** | new `rag` extension; after 107 |
| 109 | **Agent memory extension: a Rust clone of TencentDB-Agent-Memory** (added 2026-10-25, user; reference: github.com/TencentCloud/TencentDB-Agent-Memory, MIT). A native pinkdb extension, a reimplementation in Rust (no code copied) whose storage is pinkdb tables: (1) **layered chat memory** L0 conversation → L1 atoms (facts) → L2 scenarios (project-level knowledge) → L3 persona (long-term profile), with asynchronous distillation via an **OpenAI-compatible chat client** (shared with 106) and a recorded mock in tests; (2) **visibility** `private` / `team` / `restricted` (user/role/agent ACL) / `agent`, mapped onto pinkdb RBAC; (3) **layered retrieval**: L2/L3 for fast context bootstrap, falling back to L1/L0 with **BM25 + vector + RRF** (108), capped by item count, character budget and timeout; (4) operations as SQL functions and as **MCP tools** via the row-105 extension (memory add/search/update/forget, distill, persona); (5) later phases recorded as follow-ups: Skill assets, LLM-Wiki with a link graph, Code-Graph. Tests: an end-to-end conversation → atoms → persona flow on the mock provider, ACL isolation between users and teams, and retrieval budgets. | done 2026-10-27 | new `extensions/memory` (`pinkdb-memory`, cdylib + rlib) and `extensions/mcp-protocol` (the MCP wire `pinkdb-mcp` and memory now share); `crates/pinkdb` seam: `current_user()`/`current_roles()` bound to a literal at bind time (no ABI bump), `ServePolicy::{internal, deny_read_databases}`, `Session::run_internal`; `pinkdb-cli` `.memory` + the agent seam. Release `pinkdb` 5,382,768 B, `pinkdb-server` 4,977,888 B, `libpinkdb_memory.dylib` 2,746,160 B, zero new third-party crates. Gates: memory 13 (+1 ignored live), agent 13, mcp 11, cli `agent_run` 26 (+2 ignored), `memory_run` 4, ext-loader 122, `current_user` 6, workspace 2,229 passed / 14 ignored / 209 binaries, wasm green, posture 13 + four smokes. Plan: `docs/wave-109-plan.md`. Follow-ups below (109b/109c/109d) plus the agent's own `memory_search` tool and the server's background distillation worker |
| 109b | **Agent memory: Skill assets** (added 2026-10-26, follow-up of row 109): a skill is a named, versioned asset (prompt text, tool list, worked example) that a run attaches — stored in `pinkdb_memory`, distilled the same way an atom is, visible under the same four classes. Needs the memory extension's `memory_skill` operations and an MCP tool pair. | queued | after 109; small |
| 109c | **Agent memory: LLM-Wiki with a link graph** (added 2026-10-26, follow-up of row 109): scenarios gain typed links (`depends_on`, `contradicts`, `supersedes`, `part_of`), pages assembled from a scenario plus its neighbours, and a graph walk in `memory_search` (one hop, budgeted) so a question about one project can reach the fact filed under another. | queued | after 109; medium; needs 109d's storage ideas |
| 109d | **Agent memory: Code-Graph** (added 2026-10-26, follow-up of row 109): index a repository (files, symbols, call edges) into `pinkdb_memory` as its own layer with a scenario-like summary, so `memory_search` can answer "where is this parsed" from stored structure rather than a re-scan. | queued | after 109; large |
| 110 | **S3-backed lake catalog, `catalog-s3`** (added 2026-10-26, user): the catalog database itself on object storage at `pink+s3://bucket/prefix` — no database server and nothing durable on local disk, for lakes shared across machines or run from CI; data defaults to `s3://bucket/prefix/data/`. The durable catalog is the existing pinkdb `.pink` database split across immutable objects: `prefix/catalog/log/<seq>` holds one committed WAL record per object (20-digit sequence, `PINKREC1` framing: magic, version, length, crc32), `prefix/catalog/checkpoint/<seq>.pink` is a full image every 100 records (`PINKDB_CATALOG_S3_CHECKPOINT_INTERVAL`), and `prefix/catalog/head` names the newest checkpoint for operators — readers list and never read it, so a stale, missing, or malformed hint cannot break a reopen. A commit uploads only its own delta, never the catalog: writers race on a conditional create (`If-None-Match: *`) of the next log object, exactly one wins, and the loser replays the records it missed and retries through the existing touched-set validation (non-overlapping work retries, overlapping work returns the usual conflict); `Put::ConditionalUnsupported` is a hard write refusal with no check-then-put fallback, leaving the lake read-only on such an endpoint. `expire`/`vacuum` (never a commit) checkpoints and prunes folded logs and superseded checkpoints, a newer checkpoint fences an in-flight writer, and checkpoint images are cached under a private temp dir by endpoint/bucket/prefix/sequence (encrypted bytes only for a keyed lake). A keyed lake seals checkpoints and log records in `PINKENC1` with the lake key. Off-by-default `catalog-s3` feature, forwarded by the CLI and server; a build without it reports `FeatureMissing` naming `catalog-s3`. DuckDB's `ducklake` extension cannot attach this catalog (DuckDB/SQLite/Postgres catalogs only); `pink+s3://` migrates in both directions with `pink:`, `sqlite:` and postgres:// catalogs. Plan: `docs/wave-110-plan.md` | done | `pink+s3://` create/commit/reopen/scan/time-travel/expire/migrate/encrypt against the in-process mock endpoint, with the narrow engine seam `PinkDb::drain_wal_records` / `replay_wal_records` / `seal_wal_record` / `open_wal_record` and `to_pink_bytes_with_key` / `from_pink_bytes_with_key` (`crates/pinkdb/src/wal.rs`, `persist.rs`), store in `crates/pink-lake/src/metadata/s3.rs` + `pink.rs` wiring. Measured (mock endpoint, 1-row append, `catalog-s3,encryption`): one commit = 3 S3 requests (2 LIST, 1 conditional PUT) and 2,954 bytes uploaded, independent of lake size — a 1,000-data-file lake's one-file commit stays within 128 bytes of a 10-file lake's; cold reopen from checkpoint 300 replays only the logs written since, in 5 S3 requests (2 LIST, 1 checkpoint GET, 2 log GET) where the per-statement refresh first cost 77; a 252-version lake (250 commits, 302 log objects, 3 checkpoint images of 32,475/35,537/38,617 bytes) is 865,344 catalog bytes and 311 data bytes. Tests: 11 in `crates/pink-lake/tests/catalog_s3.rs` (250-commit replay with GET counts, deterministic lost race with and without retries, conditional-write refusal, delta size at 1 vs 1,000 files, both crash windows, time travel + expire pruning, pink/sqlite migration round trips, keyed object hiding) and `lake_catalog_s3_save_open_and_versions` in `crates/pinkdb-cli/tests/lake_s3.rs`; pink-lake `--features catalog-s3,encryption` 283 tests, CLI 9 lake tests. Plan: `docs/wave-110-plan.md` |
| 111 | **Benchmarking and performance tuning** (added 2026-10-26, user: "after adding all these features we havent done any proper benchmarks. we should do a performance tuning and benchmarking after all the waves have completed"). The suite `bench/suite/run.py` (Python stdlib, `run.sh` wrapper) has seven selectable families — TPC-H against DuckDB, CSV/Parquet ingest, durable writes, lake open/scan/time travel (mock S3), FTS/HNSW search, binary/RSS footprint, and a DuckDB-free CI guard — with `--only`, `--scale`, `--runs` and `--threads`; it records CPU/RAM/OS/rustc/commit/load, discards one warm-up, reports medians of five, and compares row counts and checksums before publishing `results/<date>-<commit>.json` + Markdown. Published as `docs/benchmarks.md` and the website's Benchmarks page. Tuning, each with a before/after and a test: comma joins were planned as `NestedLoopJoin cross` and became hash joins (equi-conjuncts extracted, residuals kept above, outer joins limited to what is provably safe); disconnected comma chains are reordered by an equality graph with a transparent restore projection; a filter that is an `OR` of conjunctions derives per-column filters pushed to the scans; an `IN` subquery's set reserves its capacity from the row count. TPC-H SF 1 geometric mean 33.1× → 32.0× over the same 15 queries (29.0× over all 16, Q16 now compared in full; Q9 100.9 s → 0.66 s, Q8 11.7 s → 0.29 s, Q7 2.35 s → 1.20 s); SF 0.1's geomean moved 7.4× → 8.6× even though Q7 halved (219 → 114 ms) and Q9 fell 451 → 60 ms: the reverted memo left Q18 at 76 ms (was 36), and the 1–2 ms queries move a ratio sharply. Default CLI 5,382,768 → 5,437,168 B (+1.0%). A single-flight subquery memo was tried and reverted — it halved Q18 but deadlocked once at SF 1, so the safe version is a follow-up, as are a cost-based join enumerator/build-side choice, Q1/Q6 aggregate work, and a DuckDB `fts`/`vss` comparison (needs `INSTALL`). The pink-lake corruption test now owns its four scratch directories through a `Drop` guard (1,500 already-leaked `$TMPDIR` dirs deleted). Tests: `comma_join` 2, `subquery_chunks` 2, optimizer units 2, pink-lake 26. Plan: [docs/wave-111-plan.md](docs/wave-111-plan.md) | done | `crates/pinkdb/src/{optimizer,plan,sql/binder}.rs`, `crates/pinkdb/src/exec/expr.rs`, `crates/pinkdb/tests/{comma_join,subquery_chunks}.rs`, `crates/pink-lake/tests/lake.rs`, `bench/suite/`, `docs/benchmarks.md`, `docs/wave-111-plan.md`, website page+nav, `.github/workflows/ci.yml`; plan `docs/wave-111-plan.md` |
| 111a | **Cost-based join ordering and build-side choice** (added 2026-10-26, follow-up of 111; wave 111 shipped an equality-graph reorder for disconnected comma chains but no cardinality model): build a query graph over inner/comma joins, estimate cardinalities from base row counts plus filter selectivities (no histograms yet — record the defaults assumed), enumerate with DPccp/DPhyp up to ~12 relations and a greedy fallback above that, then put the smaller estimated input on the hash build side and restore the output column order with a projection. Reference DuckDB's `src/optimizer/join_order/{query_graph_manager,cardinality_estimator,cost_model,plan_enumerator,build_probe_side_optimizer}.cpp`. Gate: TPC-H SF 1 Q7/Q19 and the SF 0.1 suite must improve without a checksum change, and `bench/duckdiff.py` stays green. | done | `crates/pinkdb/src/optimizer.rs` (`order_chains` pre-pass in `optimize`, `cost_join_order` with `ChainStats`, `dp_order`/`greedy_order`, `reorder_comma_join(pred, plan, db, keep_filter)`, `reorder_left_spine`, `has_outer_ref`). A query graph over the comma chain's equality edges; selectivities from base row counts and the predicate (`0.2` per equality or `IN` value capped at 1, `0.5` per range/other — recorded in `docs/wave-111ab-plan.md`); equivalence-class cardinalities whose denominator is the smallest base of the class (FK-aware), cost = sum of intermediate cardinalities; `dp_order` over subsets up to 12 leaves anchored at the FROM clause's first relation (it is the one a query filters, and a small dimension looks cheap to start from even when starting there fans out through a many-to-many key), greedy above; reorders only when the estimated cost drops 10 %, which also makes it terminate. The restore projection keeps the output column order and outside-chain conjuncts go above it. Subquery bodies and the chain under a decorrelation left join are handled separately (`has_outer_ref` leaves the body alone because the binder decorrelates by pattern-matching its from-tree; `reorder_left_spine` reorders the left input of the left join). Build side stays the executor's exact `build_on_left` (inner joins pick the smaller input at run time), so no plan-time estimate is needed there. Measured: Q7 SF 1 1253 → 246 ms, SF 0.1 111 → 32 ms; Q19 3855 → 112 ms and 231 → 11 ms (with 111b's filters); Q5 SF 0.1 31 → 35 ms and Q9 57 → 61 ms are the regressions, recorded below. Tests: `comma_join` 5 (two new: a reordered chain equals the explicit order with no `cross` left; an `OR` over join conditions plans once), optimizer units 15 (two new for the OR-bound union and `IN` lists inside branches). |
| 111b | **Execution throughput and subquery paths** (added 2026-10-26, from wave 111's numbers; DuckDB source as reference): close the remaining non-join gaps the suite measured. (1) **Scan/filter/aggregate throughput**: TPC-H Q1 12× and Q6 26× slower than DuckDB at SF 1. Profile first (wave 111 saw per-value `Vector::push_from` copies in projection): selection-vector filters, perfect-hash and partitioned hash aggregation (`physical_perfecthash_aggregate.cpp`, `physical_hash_aggregate.cpp`), fewer per-row materializations. (2) **`IN`/`HAVING` subqueries**: Q18 198× and Q11 162×; a safe single-flight memo for uncorrelated subqueries (wave 111's first attempt deadlocked and was reverted), or the `IN` set built straight from the aggregate. (3) **Q19's nested `OR`s**: `IN` lists expanded inside OR branches defeat the OR-derived filters (Q19 246×). (4) **Ingest**: CSV/Parquet load at ~0.5× DuckDB's rows/s. Every change measured with `bench/suite` and pinned by result tests. | 111b.2 and 111b.3 done; 111b.1/111b.4 queued as 111b1/111b4 | **(2) Subquery paths: done** (review round included). `Memo::Set` holds an `Arc<FastSet<Vec<u8>>>`; every operator primes the memo (`expr::prime`) for the uncorrelated subqueries of an expression *before* its chunks fan out, so no thread ever waits on a lock while a parallel evaluation is pending — the first single-flight version held a per-key lock across the body's evaluation and deadlocked on a subquery in a select list/CASE/aggregate/join condition (a worker ran the body's parallel plan and starved the pool; 30-run repro, 0 hangs after). `Cx::memoized` then only reads and computes without waiting, and `in_set` takes the operand's type so priming needs no chunk. Q18 SF 1 5515 → 1449 ms, Q20 4855 → 606, Q21 10391 → 2252, Q22 680 → 166. **(3) Q19: done.** `derive_common_filters` now (a) unions `OR`-of-equalities (`eq_set`, i.e. an `IN` list the binder expanded) per column and per branch, (b) drops the `false` literal the binder puts at the head of an `IN` expansion, (c) takes the *loosest* bound across `OR` branches (the old code kept the tightest, which is only valid for an intersection), and (d) runs on a lone `OR` too; the `push_filter(…, derive)` flag stops the lone `OR` left above the join from re-deriving the same facts forever (a stack overflow the first version produced). Q19 SF 1 3855 → 112 ms, SF 0.1 231 → 11 ms. **(1) and (4) not done** — see 111b1/111b4. |
| 111b1 | **Aggregate and join output throughput** (added 2026-10-27, follow-up of 111b.1; measured in wave 111ab): (1) `merge_partials` merges every chunk's partial hash table on one thread after a parallel build — Q18's remaining 1.4 s at SF 1 is mostly this, and Q1/Q9/Q13 pay a share; make the merge partitioned/parallel (DuckDB `physical_hash_aggregate.cpp`, `physical_perfecthash_aggregate.cpp`). (2) The hash join copies every surviving row into fresh vectors at each probe; write selection vectors and materialize once. (3) Perfect-hash aggregation for low-cardinality keys and selection-vector filters for Q1/Q6. Each change measured with `bench/suite`, pinned by result tests. | (1) done (wave 111c); (2) and (3) open | **(1) Parallel `merge_partials`: done** in wave 111c. Keys are hashed into up to 64 partitions (`buckets = (groups / 4096).next_power_of_two().clamp(1, 64)`, skipped below 16k groups), each partition merges in parallel into its own `FastMap`, and the first-seen `(partial, position)` of every merged group is sorted once to reproduce the serial output order exactly — so answers and row order are unchanged (`tests/aggregate_merge.rs` runs each shape twice and against DuckDB). Q18 SF 1 merge 1,284 → 56 ms, query 1,390 → 251 ms. The first attempt did not scale at all (1.46 s, flat across 1/4/16/64 buckets): it masked the *low* bits of an FNV hash, which barely mix, so nearly every key landed in one partition — a Fibonacci multiply over the top bits fixed it. (2) The probe still copies every surviving row per pair; the wave-111c residual path already gathers only what a predicate reads, and the general case wants selection-vector output from `hash_join_probe`/`hash_mark_join`. (3) Q1/Q6 were fixed by wave 123a's selection vectors instead; perfect hash is still open for 3-group aggregates. |
| 111c | **Delim join for decorrelated subqueries** (added 2026-10-27, follow-up of 120/111ab): the duplicate-eliminated key side is a `Distinct(Project(outer copy))` recomputed once per subquery; share it across a query the way DuckDB's delim join does (`physical_delim_join.cpp`, `physical_left_delim_join.cpp`, `src/optimizer/deliminator.cpp`). Also give the Q21-style non-equality correlation (`l3.l_suppkey <> l1.l_suppkey`) a join-residual slot instead of a filter above the join. Q21 is 2.6 s at SF 1 (150× DuckDB) and the largest single query left. | done (residual + carry); shared key side still open | **Join residuals: done**, and they removed the delim shape from Q21 instead of sharing it. `LogicalPlan::Join` carries `residual: Option<BoundExpr>` — a predicate on a *candidate key match*, evaluated in joined space on that pair before materialization; a failing pair is not a match for `Inner` (dropped pair), `Semi`/`Anti` (so a preserved row can match zero times); outer and mark joins reject one because their unmatched/NULL bookkeeping has no pair to test, and the binder produces none. The probe gathers only the residual's columns into a dense chunk and evaluates it once (DuckDB `physical_hash_join.cpp`'s `InitializeResidualPredicate`/`MapResidualBuildColumns`). `dependent_join` now keeps the *plain* (no key side) shape for an `EXISTS` with a residual, so Q21 is one outer chain instead of three plus two 36-column distincts: SF 1 2,333 → 1,352 ms, SF 0.1 232 → 114 ms; `tests/residual_joins.rs` fuzzes inner/semi/anti with five residual forms against a nested-loop reference and DuckDB, and `tests/correlated_fuzz.rs` grew the Q21 shape. **The key side also carries the enclosing filters now** (`Q20`: `Distinct(partsupp)` → `Distinct(Filter(ps_partkey IN (…))(partsupp))`, 604 → 100 ms, and its SF 0.1 → SF 1 scaling 15.1× → 10.0×). **Still open: sharing one duplicate-eliminated key side across several subqueries of a statement** (a real delim join / materialized CTE-like node). No TPC-H query needs it after this wave — Q20 and Q21 are the only delim shapes and both are now linear — so it stays queued with the design in `docs/wave-111c-plan.md`. Also open: Q2 stays 44× DuckDB because a correlated subquery body is optimized at *bind* time, before decorrelation removes the outer refs, so `order_chains` refuses its chain (partsupp-first: 115 ms against 9 ms for the same four-leaf chain ordered region-first) and its projection copies 19 columns to use one. |
| 111b4 | **Parquet ingest throughput** (added 2026-10-27, follow-up of 111b.4): pinkdb loads lineitem SF 0.1 Parquet at 1.23 M rows/s against DuckDB's 5.77, decoding column chunk by column chunk; decode larger batches in parallel (CSV is already faster: 2.28 vs 1.64 M rows/s). | queued | after 111ab; small |
| 106 | **`pinkdb-agent`: a pinkdb-native data agent harness, as a pinkdb extension** (added 2026-10-24, user: "building a pure pink harness"; **queued last: after every other queued wave** (user, 2026-10-24) **and before the warehouse WebUI**, which it powers): an agent loop whose state lives in pinkdb. (1) **A pinkdb extension** (user, 2026-10-25: "we are building our own harness as an extension for the pinkdb"), loaded like the `mcp` extension and never linked into the default CLI or server, so the engine stays LLM-free. **API key handling:** read from the environment at load time: `Z_API_KEY` (the user's z.ai key, kept in the repo's git-ignored 0600 `.env`; `PINKDB_AGENT_API_KEY` as the provider-neutral name), with `PINKDB_AGENT_BASE_URL` and `PINKDB_AGENT_MODEL` defaulting to z.ai's OpenAI-compatible endpoint and a GLM model. The extension never auto-loads a `.env` file from the working directory (a security footgun); callers export the variables (`set -a; . ./.env`); never stored in a table, never WAL-logged, never printed, and no `SET` for it. (2) **Sessions in a `.pink` database:** sessions, messages, tool calls/results, checkpoints and token/cost usage are tables. Every step commits a checkpoint, so a killed run resumes where it stopped (interrupted model streams are re-requested; only tools marked replay-safe re-run). Agent history is plain SQL; time travel and forks branch a session at any point, CDC lets a client (the WebUI) watch a run live, and the multi-writer store lets several clients steer one run. (3) **A data agent, not a coding agent:** the tools are row 105's warehouse tools (`query`, `describe`, `tables`, `explain`, lake tools, plus worksheet create/update), shared code with the `mcp` extension and the wave-99 compact output, read-only unless `--allow-writes`. No shell, file-edit or web tools. (4) **One provider protocol:** an OpenAI-compatible chat-completions client (streaming, tool calls) configured by base URL, model and key-from-env, covering z.ai's GLM coding plan, OpenCode and DeepSeek. Live provider tests run on the user's z.ai subscription, **opt-in and env-keyed, never committed**. CI runs only a **recorded mock provider** (canned responses exercising the tool loop, multi-step plans, an interrupted stream and a crash/resume), so tests are deterministic and free. Gates: resume-after-kill, fork, a read-only refusal for every write tool, and a TPC-H question answered end to end against the mock. Measure binary size and the dependency count; the HTTP/TLS client is reused from `pinkdb-httpfs`. | done | landed 2026-10-25 — a native extension, not engine code: `extensions/agent` (`pinkdb-agent`, cdylib) plus the shared tools moved to `extensions/warehouse-tools` (the same eight schemas and dispatch, so `pinkdb-mcp` behaviour is unchanged). ABI minor 4: optional `pinkdb_ext_command(api, host_ctx, args, sink, sink_ctx)` (detected by symbol presence; a sink callback streams the answer or a diagnostic) and `PinkdbHostApi::execute_internal` (JSON `[{"columns":…,"rows":…}]` for the extension's own reads), with `PINKDB_EXT_API_MINOR2_SIZE` moved to `offsetof(execute_internal)` so the C and Go SDKs stay correct and every v1/minor-2/minor-3 extension and all four SDK smokes are unchanged. Sessions live in the `pinkdb_agent` database (`sessions`, `messages`, `tool_calls`, `checkpoints`, `usage`; ids like `a1f2…`, unix-microsecond timestamps); a step is one model turn plus its tool runs and commits one checkpoint. Two policies: the model's calls go through the host under the row-105 read-only policy (plus `deny_write_databases = ["pinkdb_agent"]`, so a prompt injection cannot rewrite the transcript even with `allow-writes`), while the extension's own writes use a separate `write_database`-confined channel over `execute_internal`. Resume reuses `state='recorded'` tool results, re-runs only replay-safe (read-only) tools and re-requests an interrupted stream; fork copies rows up to step N into a new session and records `origin`/`origin_step`. Budgets: 20 steps by default, optional token budget, wave-99 `ExecLimits`, all per run and written into the transcript with the stop reason. Provider: one hand-parsed SSE `/chat/completions` client on blocking `ureq` 3.4 with rustls (no async runtime, no client crate, **zero new third-party crates in `Cargo.lock`**), 3 attempts with backoff on 429/5xx, and a 401 that never echoes the key; key from `PINKDB_AGENT_API_KEY` then `Z_API_KEY`, defaults `https://api.z.ai/api/paas/v4/chat/completions` and `glm-4.6`, no `.env` autoload. CLI: `.agent ask\|resume\|sessions\|show\|fork`, `--agent FILE "question"`, `--agent-allow-writes`/`--agent-allow-files`; wave-99 `.agent rows\|cols\|…` still applies when no run verb is given; the server and WebUI stay for the WebUI wave (worksheets were deferred there too). Tests: `pinkdb-agent` 26 (unit + recorded mock provider over checked-in SSE fixtures), CLI `tests/agent_run.rs` 13 + 1 ignored live test (single answer, tool loop, split arguments, interrupted stream, 429, 401, key sentinel over tables/.pink/WAL/stderr, kill-and-resume, fork, seven write refusals under the default policy, allow-writes except the agent's tables, budget stop, TPC-H top-3), loader `tests/command.rs` 4 + C command fixture, conformance minor-2 and v1 hosts; workspace 2,040 tests / 179 suites. Release: `pinkdb` 5,035,312 → 5,073,504 B (+0.76%), `pinkdb-server` 4,668,016 → 4,669,264 B (+0.03%), `libpinkdb_agent.dylib` 2,471,632 B, `libpinkdb_mcp.dylib` 588,640 B; wasm32 build green. Plan: `docs/wave-106-plan.md` | new `extensions/agent`, `extensions/warehouse-tools`; `crates/pinkdb-ext-loader`, `ext-sdk/rust`, `crates/pinkdb/src/serve.rs`, `crates/pinkdb-cli`; medium |
| 106b | **Several model providers for `pinkdb-agent`** — **done 2026-10-26**. (1) A new rlib `extensions/llm` (`pinkdb-llm`): the transcript types, SSE parsing and one blocking `ureq` client moved out of the agent, so the next harness reuses them; zero new third-party crates. (2) **Two wire protocols**: OpenAI chat completions (`Authorization: Bearer`, `reasoning_content` dropped) and Anthropic Messages (`x-api-key` + `anthropic-version: 2023-06-01`, `system` hoisted, `tool_use`/`tool_result` blocks with same-role merging, `max_tokens` 8192 or `PINKDB_AGENT_MAX_TOKENS`, `stop_reason` normalised to the OpenAI names); retries on 429/5xx/529, a 401/403 stops at once naming the provider's key variables. (3) **A provider registry with no default provider**: `zai`, `anthropic`, `openai`, `deepseek`, `openrouter`, `gemini`, `ollama`, `custom` (`PINKDB_AGENT_BASE_URL` + `PINKDB_AGENT_PROTOCOL`); each built-in root is overridden only by its own `PINKDB_AGENT_<NAME>_BASE_URL` (the variable is a column of the table), and `PINKDB_AGENT_BASE_URL` is `custom`'s alone, so a root exported for a custom endpoint never receives a built-in provider's key; a run names one with `provider <name>` / `--agent-provider`, or `PINKDB_AGENT_PROVIDER`. **Key isolation**: a key variable is only ever sent to its own provider, and no key or part of one is printed or stored. (4) Precedence: ask = run argument > environment; resume and fork = run argument > the session's record > environment, and a recorded model is dropped when the provider switches. (5) Sessions record `provider`; `usage` rows record `provider` and `model` per step, so a fork from zai to anthropic keeps an honest bill. (6) `.agent providers` lists the table, each override variable, the root in effect and whether each key is set, never a key. Tests: `pinkdb-llm` 44 unit + 17 mock-provider (both protocols, headers, 429, 529, 401, 403, empty key), `pinkdb-agent` 13, CLI `tests/agent_run.rs` 25 + 2 ignored live tests (each provider receives only its own key, no provider is an error even with a key exported, unknown provider, missing model, run argument beats environment, CLI flags, Anthropic tool loop over real queries, Anthropic 529 retry, resume keeps the session's provider, fork zai → anthropic, providers never print a key, the custom root never receives the anthropic key); workspace 2,192 tests / 201 suites. Release: `libpinkdb_agent.dylib` 2,471,968 → 2,548,240 B (+76,272 B, +3.09%); wasm32 build green. Plan: `docs/wave-106b-plan.md` | new `extensions/llm`; `extensions/agent`, `crates/pinkdb-cli`; medium |
| 120 | **Correlated subqueries: decorrelation with DuckDB parity** (added 2026-10-26, user; found by wave 111: subqueries are uncorrelated-only in v1, so TPC-H Q2/Q4/Q17/Q20/Q21/Q22 fail with `no column`): a subquery may reference outer columns in `WHERE`, `SELECT`, `HAVING` and `ON`, at any nesting depth. Supported forms: `EXISTS` / `NOT EXISTS`, `IN` / `NOT IN` (with SQL's NULL semantics), `ANY`/`ALL` comparisons, and correlated scalar subqueries (including aggregates, e.g. Q17's `avg` per part, and the zero-rows-means-NULL and more-than-one-row-is-an-error rules). Planned by **decorrelation into joins** (semi/anti/mark joins plus aggregation over the outer keys, after Neumann & Kemper's "Unnesting Arbitrary Queries", which DuckDB implements), not per-row re-execution; a per-row fallback only where unnesting is not yet implemented, chosen in the plan and visible in `EXPLAIN`. Correctness pinned against DuckDB 2.0 on a probe corpus (NULLs, duplicates, empty inputs, nested correlation, correlation inside `LATERAL`-free derived tables) plus all 22 TPC-H queries; then **re-run wave 111's TPC-H suite for all 22 queries** and update `docs/benchmarks.md`. | done | `crates/pinkdb/src/sql/{ast,peg_parser,binder}.rs`, `crates/pinkdb/src/{plan,optimizer}.rs`, `crates/pinkdb/src/exec/{mod,expr}.rs`, `crates/pinkdb/tests/{correlated_subqueries,three_part_columns}.rs`, `bench/suite/{run.py,baseline-ci.json}`, `docs/benchmarks.md`; plan `docs/wave-120-plan.md`. **Binding** is a scope chain (`Binder.outer`, `OuterScope`, `subquery_scope()`); a miss in the block's own FROM yields `BoundExpr::OuterRef{depth,index,dtype}`, which every column-rewriting pass treats as opaque and which `collect_expr_projections` keeps alive. Each subquery expression carries `OuterBind{refs, deep, keys}` collected by the binder itself (no plan walker): `refs` is the correlation inside the body, `keys` maps a FROM-space column to the aggregate output when the block is grouped. **Decorrelation** (`decorrelate_clause` for WHERE/HAVING/QUALIFY, `decorrelate_project` for the select list, in `sql/binder.rs`) follows Neumann & Kemper (BTW 2015) as DuckDB implements it: `EXISTS`/`NOT EXISTS` → semi/anti hash join, `IN`/`NOT IN`/`ANY`/`ALL` → mark join with three-valued truth, a correlated scalar aggregate → `Aggregate(keys = correlation keys)` plus a left join, `count` wrapped in `COALESCE(x,0)` (the COUNT bug; `flatten_dependent_join.cpp`'s `PushDownAggregate` + `count.cpp`'s `SPECIAL_HANDLING` are the reference). Two measured shapes: `EXISTS`/anti use the body's own key columns (no second scan of the outer block), scalars use a duplicate-eliminated key side. `JoinKind` gained `Semi`/`Anti`/`Mark`, implemented in the hash join (`hash_mark_join`: full-key hit → TRUE, NULL value in the key's group → NULL, no group → FALSE). `ANY`/`ALL` were added to the parser (`ALL` = `NOT(inv(op) ANY …)`); ordered quantifiers collapse the set to its min/max extremes at run time. **Fallback** (correct, per distinct key, visible in `EXPLAIN`) covers two-level correlation, a body with `LIMIT`/`DISTINCT`/`GROUP BY`/a set operation, a grouped scalar (its >1-row error must survive), pure range correlation, scalar-under-`NOT`, non-`=` `ANY`, and the `ON`/`ORDER BY`/`GROUP BY`/window-argument sites. Tests: `correlated_subqueries` 5 (68 cases pinned **and** re-run against DuckDB 2.0, plus the >1-row error, the `EXPLAIN` shapes, the grouped-block rule, and correlated `UPDATE`/`DELETE` replaying identically from the WAL) and `correlated_fuzz` (seeded generator, 500 cases per run against DuckDB, thousands in a soak), which found the empty-group, grouped-block and derived-table census bugs of the review round, two pre-existing refusal tests ported to assert the new behaviour (`three_part_columns`, `exec::correlated_exists_reads_the_outer_row`); the CLI-free `ci` family gained `corr_scalar`/`corr_exists`/`corr_not_in`. TPC-H: all 22 queries match DuckDB at SF 0.01, SF 0.1 and SF 1. Measured SF 0.1 (M4, 8 threads, release): geomean **9.3×** over all 22 (8.0× over the 16 wave 111 could run, vs 8.6× then); Q4 16 ms (8.0×), Q17 7 ms (3.5×), Q20 50 ms (16.7×), Q21 407 ms (81.4×), Q22 57 ms (19.0×). SF 1: geomean 35.5× over 22 (28.4× over the 16). Scaling SF 0.1 → SF 1 is ~10× for Q4/Q17/Q22/Q21 and 38× for Q20 — its duplicate-eliminated key side is recomputed per subquery, so the recorded follow-ups are DuckDB's delim join and a join-residual slot for Q21's `<>` condition (`docs/wave-120-plan.md`) |
| 111d | **Join order through nested decorrelated joins** (added 2026-10-28, from Q21's plan: `nation` joined last; done by Claude directly) — **done 2026-10-28**. `reorder_left_spine` ordered a comma chain only when it was the direct left input of the join under the filter; Q21's chain is under an anti join over a semi join, so it kept FROM order (`supplier ⋈ lineitem ⋈ orders ⋈ nation`, carrying ~1.9M rows until the one-row `nation` filter). It now walks the whole left spine (each level keeps its left input's column indices, so the parents' keys still match). The new plan exposed a 111c bug at SF 1: `residual_selection` gathered all of a probe chunk's candidate pairs into one vector and failed past 2,048 ("vector overflow"); fixed separately (batches of `CHUNK_SIZE`, typed gathers) with a regression test in `tests/residual_joins.rs`. The first measurement (−59 %) was of the wrong answer — the SF 1 checksum check caught Q21 returning 0 rows. Measured with correct results, TPC-H SF 1 A/B against 127c (8 rounds): Q21 286 → 201 ms (−30 %), Q4/Q20/Q22 flat; 22/22 match DuckDB at SF 0.01 and SF 1. Test: `comma_join::a_chain_under_nested_decorrelated_joins_starts_from_its_filtered_table` (plan order + DuckDB-pinned result; fails without the change). | done | `crates/pinkdb/src/optimizer.rs`, `crates/pinkdb/tests/comma_join.rs`; small |
| 122 | **Statistics and join-filter pushdown** (added 2026-10-27, user: queued right after 111ab, before the WebUI; layer 2 of the performance plan). (1) **Column statistics** kept in the catalog: per-column distinct-count estimates (HyperLogLog), min/max, NULL fraction and row counts, maintained on write/checkpoint (no full rescans) and on lake commits (DuckLake's `ducklake_table_column_stats` already carries min/max), exposed through `ANALYZE` and `duckdb_columns`-style views; the 111a cardinality estimator uses them instead of fixed selectivity defaults. (2) **Join-filter pushdown**: after a hash join's build side is complete, push a min/max range and a compact membership filter (Bloom-style) of its keys into the probe side's scan, so rows that cannot match are skipped before they're materialized (after DuckDB's `join_filter_pushdown_optimizer.cpp` and the dynamic filters in `physical_hash_join.cpp`). (3) **Zone-map skipping** in `.pink` and lake Parquet scans driven by those filters and the stored min/max. (4) Late materialization of wide columns where it pays (`late_materialization.cpp`). Gate: TPC-H SF 1 star-shaped queries (Q3/Q5/Q7/Q8/Q9/Q10) improve with identical checksums; `bench/duckdiff.py` and the 22-query correctness check green. Reference: `~/Desktop/projects/ducklake-vortex/ducklake/duckdb/`. | done | `crates/pinkdb/src/stats.rs` (new), `crates/pinkdb/src/{storage,persist,optimizer,plan}.rs`, `crates/pinkdb/src/sql/{ast,peg_parser,binder}.rs`, `crates/pinkdb/src/exec/mod.rs`, `crates/pinkdb/src/{rbac,serve}.rs`, `crates/pinkdb/tests/stats.rs` (new); plan `docs/wave-122-plan.md`. **Statistics** (new `stats.rs`, no dependency): a hand-written HyperLogLog (p=12, 4,096 registers, FNV-1a over the same canonical encoding the hash join uses, sparse 32-register block encoding on disk, error 1.6 % at 100k), exact `null_count`/`min`/`max`/`nan_count` per column, and per-chunk `Zone`s (2,048 rows, the plan's dynamic-filter target). Maintained in `DataTable` on every path: `append_row`/`append_chunk` observe the values and the new zone, `update_rows` folds the new value into the sketch and refreshes the touched chunk's zone, `delete_from_chunk` recomputes the zone and rebuilds the column's bounds only when the deleted rows held a bound (duplicates), leaving the sketch an upper bound and the column `stats_approximate`; INSERT/UPDATE/DELETE/COPY/import/lake inlining all funnel through those four methods. Persisted in the `.pink` footer (`FOOTER_VERSION` 1 → 2: per-column nulls/bounds/dirty/sketch plus the per-chunk zones, so `.open` restores statistics instead of rehashing every value; the rows-free metadata image carries `TableStats::unknown`). `ANALYZE [TABLE] t` / `ANALYZE` recomputes exactly (refused inside a transaction, ADMIN-only, deliberately not WAL-logged — advisory data the next checkpoint persists). `pinkdb_column_stats(['db.]table')` is a catalog table function bound to a `Values` plan (no table source, no scan): database, table, column, data type, row count, NULL count, distinct, min, max, `stats_approximate`. Lake tables keep DuckLake's per-file stats. **Cost model**: `optimizer.rs` now asks the catalog first — equality/`IN` from `1/distinct`, `<>` from `1 - 1/distinct`, ranges by uniform spread between min and max (numeric/date/timestamp/decimal/double), `IS [NOT] NULL` from the NULL count — and keeps `0.2`/`0.5` as the fallback where a column has no statistics; equality-class domains take the smallest exact distinct estimate; `exec::estimate_rows` applies the same selectivity to filters, so the inner-join build side is chosen with statistics. **Zone maps**: `.pink` scans skip a chunk when the predicate cannot touch its `[min,max]` (strict at the boundaries: `Lt`/`LtEq` versus `Gt`/`GtEq` differ by the edge row, NULL-only chunks answer only `IS NULL`); the same predicates were already pushed to lake Parquet row groups. **Join-filter pushdown**: an inner or semi hash join evaluates its build side first, derives per-key `[min,max]` and a blocked Bloom filter (2 probes, 1 % false positives, sized from the build key's distinct estimate via DuckDB's `-n·ln(fpr)/ln(2)²`), and registers them on the probe subtree before it runs; a local scan then skips non-overlapping chunks and tests each row against the Bloom before its other columns are read, and a lake scan gets the same bounds as row-group predicates. Left/right/full joins (they owe every preserved row an output) and anti/mark joins (a false positive would drop a surviving row) are excluded; `SET join_filter_pushdown = false` (also `SET VARIABLE …`) turns it off for debugging. The filters are priced: only a build of ≤ 10,000 rows earns one, because every larger TPC-H build measured slower with a filter (hashing the build is the cost). **Measured** (A/B two release binaries, alternating, same generated Parquet, load 2.5–2.7, 1 warm-up + 6 timed, min of two process medians): TPC-H SF 1 total **9.70 s → 9.14 s (−5.8 %)**, Q21 **3,831 → 3,196 ms (−16.6 %)** — the only query where the build side is small enough to filter (10k rows against a 3M-row probe); Q3/Q5/Q7/Q8/Q9/Q10 flat (within ±7 %, i.e. noise); the switch alone shows Q21 **−12.9 %** (2,092 vs 2,402 ms). SF 0.1 flat (728 → 726 ms). Write path: CSV ingest **+14.5 %** (lineitem 227 → 260 ms, orders 91 → 102 ms — the exact zone + sketch fold over 16 columns), Parquet CTAS and durable inserts unchanged; the brief's 5 % bound is missed on the CSV import path only (follow-up 122a). Binary +1.4 % (5,608,752 → 5,690,064 B default CLI), no new dependencies. Tests: new `tests/stats.rs` 7 (seeded random insert/update/delete against exact SQL recomputation, sketch within its error bound, persist/reload, `ANALYZE`, 13 zone-map predicate boundaries, 30 × 11 differential join cases filters on/off, 7 joins against DuckDB, the catalog function across two databases) plus a boundary/NULL-only zone unit test in `exec/mod.rs`; `cargo test -q -p pinkdb --lib` 671, workspace and lake suitemates green, `bench/duckdiff.py` and the 22-query TPC-H check at SF 0.01/0.1 green, wasm32 green. |
| 122a | **Cheap bulk statistics maintenance** (added 2026-10-28, follow-up of 122): CSV `.import` and CTAS pay 12–15 % for the exact per-chunk zone and sketch fold (lineitem SF 0.1 227 → 260 ms), above the wave's 5 % write-path bound. Fold in the parser's own pass, or at chunk close, or batch it; or sample the sketch at 1/8 and widen zones at row-group granularity while keeping per-chunk zones for the scan. Gate: the ingest family back within 5 % of the wave-111ab numbers with statistics still exact enough for the tests (distinct within the HLL bound, bounds exact). | queued | after 122; small |
| 122b | **Late materialization for scans feeding a selective join or `ORDER BY … LIMIT`** (added 2026-10-28, phase 5 of wave 122, measured and deferred): the scan already copies only the projected columns, but a wide column is still read for every row that a filter or Bloom test later drops. Read the narrow predicate/key columns first, then fetch the remaining columns for surviving rows (after DuckDB's `late_materialization.cpp`). Measure first: the TPC-H projections are narrow, so the win may be small; prove it on a wide-table probe (e.g. 40 columns) before building. | queued | after 122; measure first |
| 122c | **Filters on the other join shapes** (added 2026-10-28, follow-up of 122): (1) the fused join → aggregate path (`exec::fused_join_aggregate`) applies no filters today (`SELECT count(*) FROM a JOIN b ON …` never gets one); (2) a large build could filter the *probe* side's scan by the probe's own key range derived from the build's `[min,max]` before hashing the build, and the build side itself could skip chunks by the probe's range (reverse pushdown); (3) anti/mark joins could take a Bloom filter for the *positive* side only (probe rows that pass cannot match, so the absence is proven) — needs the three-valued NULL rule spelled out. Gate: differential filters-on/off over the shapes plus TPC-H Q3/Q5/Q7/Q8/Q9/Q10 unchanged or better. | queued | after 122; medium |
| 123a | **Selection vectors and fixed-width expression kernels** (added 2026-10-27, user; layer 3, first part of 123, the step behind most of the remaining scan and aggregate gap: TPC-H Q1 was 12.6×, Q6 26.7× at SF 1). (1) A filter now evaluates its predicate to a **selection** — the row indices that survive — instead of copying survivors out: comparisons and `AND`/`OR` build it directly (`expr::select_predicate`), `AND` narrows progressively so later conjuncts only look at survivors, `OR` narrows the same way over the rows its left side did not take (DuckDB's `Select`/`OrSelectOperation`), a conjunct's error fires only for the rows that reach it, and every other predicate falls back to `Vector` + `select_true_in`. (2) `Aggregate` over a `Filter`, and `COUNT(*)` over one, fold **through** the selection — `group_ids`/`accumulate` index the chunk rather than a compacted copy — and only compact when the selection is sparser than 1/8 (measured: always compact makes Q1 58→138 ms, never compact makes Q6 11.9→16.7 ms). (3) **Fixed-width kernels**: one `ScalarCmp` kernel over 15 typed layouts serves both `eval_comparison`'s column-vs-literal path and the selection path, so `DECIMAL` against `DOUBLE`/`INTEGER`/`BIGINT`/any-scale `DECIMAL` no longer drops to a per-row `Value` (Q6's `l_discount BETWEEN` 38→20 ms); `DECIMAL` arithmetic borrows its operands' payloads, reuses the column validity mask and writes a pre-sized payload instead of `push_decimal` per row; column-vs-constant `DECIMAL` arithmetic skips building the constant vector at all. (4) `Vector::push_from`/`push_decimal` are gone from Q1's profile (`push_from` was 11 % of it in wave 111). Measured, SF 1 median of 3, same machine back to back: Q1 164→**64** ms (DuckDB 13), Q6 80→**13** (3), Q19 115→**50** (10), Q16 60→45, Q14 80→73, whole-suite geomean **21.56×→17.30×**; SF 0.1 geomean 6.35×→5.04× (Q1 17→7, Q6 8→1, Q19 11→5); ingest and durable families unchanged (CSV 2.10→2.11 M rows/s, Parquet 0.80, durable inserts 7745→7747 ms). Correctness: a seeded differential fuzz (24 seeds × 10 cases, type extremes, NULLs and mixed numeric types) comparing the selection path, the mass-evaluated predicate and a row-at-a-time reference; an aggregate read-through test with `FILTER (WHERE …)`; `bench/duckdiff.py` 404; the 22-query TPC-H check at SF 0.01, 0.1 and 1; wave 120 and 122 suites; wasm32 green. No new dependencies, binary +50 KB (+0.34 %). Typed `CASE` scatter measured at 6 % of Q14 and deferred to 123b. Plan: `docs/wave-123a-plan.md`. | done | `crates/pinkdb/src/exec/selection.rs` (new), `crates/pinkdb/src/exec/{expr,aggregate,mod}.rs`, `crates/pinkdb/src/vector.rs`; `cargo test -q --workspace --no-fail-fast` 2281 passed, clippy, `duckdiff.py`, SF 0.01/0.1/1 correctness, `--only ci` and wasm32 green. |
| 123b | **Push-based pipelines with morsel-driven parallelism** (added 2026-10-28, split out of 123): replace the `Output { chunks }` recursion with DuckDB's execution model (`src/parallel/executor.cpp`, `pipeline.cpp`, `task_scheduler.cpp`, the `physical_operator` source/sink interface) — split the plan at pipeline breakers (hash build, aggregate, sort, materialization), let each pipeline pull from a source through streaming operators into a sink over morsels that worker threads claim and finish independently, and materialize nothing between streaming operators. Scope: (1) make the projector and filter **streaming** operators that pass a chunk plus a `Selection` on instead of compacting at every boundary — the pieces exist (`Selection`, `DataChunk::take`, `select_predicate`) but every parent that is not an aggregate still calls `take`; (2) the deferred typed `CASE`/`COALESCE` scatter (`eval_case`'s `Vec<Option<Value>>` + per-branch `gather`, measured 7 ms of Q14's 113); (3) `fused_scan_aggregate` for local scans, which today deliberately does not fire because that fold is already parallel; (4) keep `ExecLimits`, cancellation, `QueryProfile` (wave 84), zone maps and join filters (122), the correlated paths (120) and the single-threaded wasm32 path working. Gate: the join-shaped queries 123a cannot touch (Q12 172, Q14 73, Q18 1500, Q21 2371 ms) improve, nothing in `bench/suite` regresses, identical results. | queued | after 123a; large |
| 123c | **Perfect-hash and radix-partitioned aggregation** (added 2026-10-28, split out of 123): Q1's remaining cost is the grouping (19.6 ms of it; `Vector::encode_into` builds a byte-encoded key per row for the two-key `GROUP BY`) plus the decimal aggregates (~32 ms). Scope: (1) perfect-hash aggregation for small integer/dictionary key domains (`physical_perfecthash_aggregate.cpp`, `perfect_hash_table.hpp`) — Q1's `l_returnflag`/`l_linestatus` are two three-value strings, so the domain is six groups after a dictionary encode; (2) radix-partitioned hash aggregation for large domains (`radix_partitioned_hashtable.cpp`); (3) fused argument evaluation: `SUM(a * b)` evaluates its argument into a temporary `Vector` and re-reads it, so let the accumulator own the arithmetic (it cannot be decomposed into per-operand accumulators without changing results); (4) keep `COUNT(DISTINCT …)` and `FILTER (WHERE …)` exact. Gate: Q1 within 3× of DuckDB at SF 1 (13 ms), Q3/Q9/Q13/Q18 not worse, `bench/suite` unchanged elsewhere, identical results. **First part done 2026-10-28** (by Claude directly, ahead of 123b because profiles put it first): Q18's samples showed every thread idle half the time, and on the main thread `emit_groups` (serial `push_value` per group, ~23 % of Q18) and the end of `merge_by_bucket` (a serial sort of 1.5M first-seen positions plus moving every group into one `Vec`). Now `merge_and_emit` replaces the `merge_partials` + `emit_groups` pair at all four call sites: a large keyed merge sorts the order with `par_sort_unstable_by_key` (positions are unique) and builds every output chunk in parallel from the partitions by reference (`Acc::finalize_ref`, identical to `finalize`); small merges keep the serial path, and `emit_groups` itself builds chunks in parallel and reports the first error in group order. The scatter in `merge_by_bucket` grew 64 lists per partial (~190k small `Vec`s for Q18; 5.5k samples summed over threads): each partial is now stably sorted by partition (`ScatteredGroup`) and every partition merges its contiguous run of each partial through `split_at_mut` borrows, and groups sort by their exact first-seen index in the partial (the old key was the index inside the bucket list, which ties across buckets). TPC-H SF 1 A/B against 126d (6 rounds, load ~10): Q18 451 → 320 ms (−29 %), Q13 −9 %, Q3 −8 %, Q16 −6 %, none slower; the scatter then took Q18 another −13 % and Q3 −5 % (10 rounds); 22/22 match DuckDB at SF 0.01 and SF 1. Then for Q1 (profile: `fold_chunk` 4.6k, `Acc::add_decimal` 4.2k, `encode_into` 2.6k samples): `small_domain_gids` groups a 2–3 column key whose columns each hold ≤ `SMALL_KEY_SET` values in the chunk by coding each column (`small_column_codes`, one first-seen counter so NULL and values share a dense code space) and indexing a dense slot table — a per-chunk perfect hash, after DuckDB's `PerfectHashAggregate`; and `accumulate` sums a DECIMAL(≤18) column per group per chunk into local `i128`s and calls `Acc::add_decimal_bulk` once per group (only when every accumulator `takes_decimal_bulk`: fresh or same-scale SUM, non-float AVG — so the overflow rules see the same totals). A new test pinned to DuckDB (`tests/aggregate_small_domain.rs`) caught a 126 rounding difference: DuckDB's optimizer rewrites `avg(x)` to `CAST(sum(x) AS DOUBLE) / CAST(count(x) AS DOUBLE)`, so `AvgState::finalize` now divides the scaled sum by the count instead of dividing by `count × 10^scale`. Q1 −11 %, Q18 a further −12 % (8 rounds, load ~9). **Second part:** Q18's profile after the first part had 6.8k samples (summed over threads) of self time in the per-partition merge's `FastMap<Vec<u8>, usize>` inserts, ~400 ns each. `FnvHash` mixes one 8-byte word per step, so a result's low bits depend only on the low bits of each word; an encoded group key starts with a constant type-tag byte, so every encoded integer key had the same low 8 bits, which hashbrown uses for the bucket index. `FnvHash::finish` now applies MurmurHash3's `fmix64`. Q18 −20 % (10 rounds), the other 21 flat; every `FastMap`/`FastSet` gains. Per-group allocations and typed integer-key tables continue in 123d. | in progress | after 125; medium |
| 123d | **Flat GROUP BY tables for fixed-width keys** (added 2026-10-29, from Q18's profile after 123c: `merge_by_bucket` ~4× the fold, `malloc`/`free` and the stable sort of `ScatteredGroup`s on top; done by Claude directly) — **done 2026-10-29**. Isolated, `GROUP BY l_orderkey` over SF 1 `lineitem` took 155 ms (DuckDB 25 ms), `GROUP BY l_partkey` 380 ms (DuckDB 55 ms): every chunk's group allocated an encoded key, a `Vec<Value>` of keys and a `Vec<Acc>`, and a random key domain (partkey: ~2,000 groups per chunk, 6M local groups) moved all of them through the merge. New `exec/group_table.rs`, after DuckDB's `GroupedAggregateHashTable` / `RadixPartitionedHashTable`: when every key is INTEGER/BIGINT/DATE/TIMESTAMP/BOOLEAN/DECIMAL(≤18) and every aggregate is a non-DISTINCT COUNT/SUM/AVG/MIN/MAX, a chunk folds into a `FlatPartial` — keys as `u64` words plus a NULL-bit word, the hash, accumulators in one `Vec<Acc>` (stride = aggregates), an open-addressing `u32` slot table — then reorders its groups by merge partition (count-sorted on the folding thread; partitions sized from the input rows), so each partition's merge reads one contiguous run per partial. Partitions merge in parallel in chunk order with `Acc::merge_ref` (in-place merge of the fixed-size states, same arithmetic) and emit by first-seen position, so groups and every accumulator's merge order match the general path exactly (float sums included). `accumulate` is now generic over `AccSlots` (the partial's group list or the flat array), so both paths share the typed update loops; a key payload that is not its type's layout falls back to the general path, and the selection path keeps its kept-rows retry. Isolated: orderkey 155 → 48 ms, partkey 380 → 90 ms, `(l_orderkey, l_linenumber)` (6M groups) 840 → 225 ms. TPC-H SF 1 A/B against 123c part 2 (5 rounds): Q18 198 → 83 ms (−58 %), Q15 −38 %, Q11 −20 %, Q13 −12 %, the rest flat; 22/22 match DuckDB at SF 0.01 and SF 1. Tests: new `tests/aggregate_flat_keys.rs` 4 (first-seen order over 60k rows / ~20k groups, every key type with NULLs, a filter's selection plus `FILTER (WHERE …)`, an error only a filtered-out row raises); breaking the order sort fails 3 of them. Not yet covered: string keys, the fused join→aggregate (`fused_join_aggregate` still builds per-group `Group`s row by row) and the streaming fold. | done | `crates/pinkdb/src/exec/group_table.rs`, `crates/pinkdb/src/exec/aggregate.rs`, `crates/pinkdb/tests/aggregate_flat_keys.rs`; medium |
| 125 | **Hash join rewrite, first part: flat hash table and vector-at-a-time assembly** (added 2026-10-28, step 2 of docs/performance-architecture.md; done by Claude directly) — **done 2026-10-28**. Profiles of Q9/Q18/Q21 after 126c: threads mostly idle, `Vector::push_from` + `ProbeSink::push` (per-row output assembly), `Vector::get` (from `chunk_approx_bytes`, `Vector::take` and `JoinFilter::may_match`) and a single-threaded `MatchTable::build` over a `FastMap` with a heap key per distinct `Vec<u8>`. Now: (1) `MatchTable` follows DuckDB's `JoinHashTable` (src/execution/join_hashtable.cpp): keys (`BuildKeys::Ints` i64 or `Bytes` with offsets) and 64-bit hashes (`mix64` = MurmurHash3 `fmix64` for integers, an 8-byte-word hash for encodings) computed in parallel per build chunk, then linked into a power-of-two bucket array (`2 × rows`, DuckDB's `PointerTableCapacity`) with a per-row `next`; rows linked in reverse input order so each chain, and each key's matches, come out in build input order; the `Matches` iterator compares the full hash before the key. All five users (probe, residual candidates, semi/anti/mark, fused join→aggregate, the DML join index) walk it through `matches_int`/`matches_bytes` + `loc`. (2) `JoinAssembler` replaces `ProbeSink`: pairs `(probe row, build location)` are collected per `CHUNK_SIZE` and each output column is one typed gather — `Vector::take` for the preserved side, the new `Vector::gather_located` across build chunks for the build side (`GATHER_NULL` NULL-extends an outer join's unmatched row); placeholders (wave 127) are never gathered. (3) `Vector::take`/`gather` became a typed `select_rows` (ARRAY keeps the per-row path); `chunk_approx_bytes` reads string lengths off the payload with identical totals; `JoinFilter` precomputes an integer range and compares strings borrowed. Measured, TPC-H SF 1 A/B against 126c (alternating, median of 5, suspects rechecked at 10 rounds, load 7–11 from a parallel gate): all 22 3,013 → 1,892 ms (−37 %), Q9 554 → 164, Q17 116 → 43, Q21 544 → 268, Q12 −47 %, Q2/Q11 −46 %, Q7 −41 %; none slower (Q14's +97 % in one run rechecked at −20 %). Results match DuckDB at SF 0.01 and SF 1; all pinkdb tests pass. Next (rest of 125): partitioned parallel linking for large builds, vectorized probe hashing, salt bits, perfect hashing for dense integer keys; and the fused/mark paths onto the assembler. | done | `crates/pinkdb/src/exec/mod.rs`, `crates/pinkdb/src/vector.rs`, `crates/pinkdb/src/stats.rs`; medium |
| 127 | **Unused-column pruning through joins** (added 2026-10-28, from docs/performance-architecture.md step 5; done by Claude directly) — **done 2026-10-28**. The scan-projection analysis (`plan::ScanProjections`) now runs through hash joins: each `Join` records its read output columns (`for_join`), each side gets that bound plus its keys and residual, and `ProbeSink`/`hash_mark_join` assemble only read columns, emitting shared placeholders for the rest; `AggRef`/`WindowRef` count as column reads. TPC-H SF 1: total 7.2 → 3.7 s, geomean 14.1× → 8.0× DuckDB (Q21 2,407 → 393 ms, Q5 412 → 108, Q2 185 → 53). Follow-ups: the reorder's restore projection (Q7/Q9) references every column; nested-loop joins stay unbounded; common subexpressions and statistics propagation (the rest of step 5). | done | `crates/pinkdb/src/plan.rs`, `exec/mod.rs`; small |
| 127c | **Filter conjuncts ordered by cost; duplicate derived conjuncts dropped** (added 2026-10-28, from Q12's profile — `retain_cmp` on strings and `memcmp` on top; done by Claude directly) — **done 2026-10-28**. Q12's filter was `((mode='MAIL' OR mode='SHIP') AND ((false OR mode='MAIL') OR mode='SHIP')) AND <dates>`: the binder seeds an `IN` list's `OR` chain with `false`, constant folding only folds fully constant trees, so 111ab's `derive_common_filters` fact never matched the written list and was added again, ahead of the cheap date bounds. Now (1) `fold_expr` drops a neutral boolean operand (`false OR x`, `true AND x` → `x`); (2) `push_filter` drops a derived fact when an existing conjunct pins the same column to the same value set (`same_value_set`, order-insensitive); (3) `fold_plan`'s Filter arm calls `order_conjuncts`: split, drop exact duplicates, stable-sort by a port of DuckDB's `ExpressionHeuristics` cost (column = type weight × 8 with VARCHAR 5 / DOUBLE 2 / other 1, literal = type weight, comparison +5, `LIKE` +200, `NOT` +10, `IS NULL` +5, anything else 1000) — but only when every conjunct is `infallible` (comparisons, `LIKE`, `AND`/`OR`/`NOT`/`IS NULL` over columns and literals), so a guarded `CAST` never moves ahead of its guard (stricter than DuckDB, which reorders regardless). Tests: new `tests/filter_order.rs` 2 (plan order + single `IN` + results; a fallible conjunct keeps the written order). TPC-H SF 1 A/B against 126d (6 rounds, rechecks at 10): Q12 103 → 69 ms (−34 %), the rest flat; 22/22 match DuckDB at SF 0.01 and SF 1. | done | `crates/pinkdb/src/optimizer.rs`, `crates/pinkdb/tests/filter_order.rs`; small |
| 127d | **Filter comparisons in the column's domain** (added 2026-10-29, from Q6's profile: `retain_cmp` and `select_predicate` ~85 % of busy samples, `__floattidf` on top; done by Claude directly) — **done 2026-10-29**. A selection kernel compared every row through a closure returning `Ordering` plus `ord_matches(op)` per row, and since pinkdb types `0.05` as DOUBLE (126a parked), Q6's `l_discount BETWEEN 0.05 AND 0.07` converted every DECIMAL cell to f64 (an i128 → f64 conversion and a division per row); `l_quantity < 24` rescaled the literal per row. `ScalarCmp::retain_in_domain` now moves the literal into the column's domain once per chunk — DATE/TIMESTAMP as is, INTEGER/BIGINT when the literal fits, DECIMAL vs DECIMAL/INTEGER literal rescaled exactly — and runs one branch-free compaction loop per operator (DuckDB's `SelectFlat`). DECIMAL vs DOUBLE keeps the exact per-row meaning: `num_cmp(decimal_to_f64(x), s)` is monotone in `x`, so it is an integer range whose bounds (`first_passing`) are searched from the rounded estimate with that same conversion; NaN/infinite/huge literals and any other pair keep the generic loop. TPC-H SF 1 A/B against 123d (7 rounds): Q6 12 → 7 ms (−42 %), Q19 −24 %, Q14 −20 %, Q20 −14 %, Q12 −7 %, the rest flat; 22/22 match DuckDB at SF 0.01 and SF 1. Tests: new `tests/filter_compare_domain.rs` 3 (every operator, both operand orders, NULLs, on/between/off-grid and out-of-range literals: the filter keeps exactly what the same comparison marks TRUE in a projection); off-by-one mutations of a bound or an operator fail them. | done | `crates/pinkdb/src/exec/expr.rs`, `crates/pinkdb/tests/filter_compare_domain.rs`; small |
| 126 | **Decimal arithmetic fast paths, DuckDB's 18-digit multiply typing and exact AVG** (added 2026-10-28, from docs/performance-architecture.md step 2; done by Claude directly) — **done 2026-10-28**. (1) `eval_decimal_mul`, `arith_decimal` and `arith_decimal_const` bound each chunk by its operands' largest magnitudes (`max_abs_i128`, NULL rows zeroed first) and run unchecked when the result provably fits the target precision; products of operands within `i64` use an `i64 × i64 → i128` multiply; anything else takes the checked loop, whose overflow error is now DuckDB's text (`DECIMAL(18)` and `DECIMAL(38)` variants, unscaled operands). (2) The binder types a product of two inputs of ≤ 18 digits with scale < 18 as `DECIMAL(18, s1+s2)` (DuckDB's `BindDecimalMultiply`); wider inputs still widen, capped at 38. TPC-H's `l_extendedprice * (1 - l_discount)` stays `DECIMAL(18,4)`, as in DuckDB. (3) `Acc::Avg` became `AvgState`: an exact `i128` sum for integer and decimal inputs (rescaled to the widest scale seen), divided once in `finalize` as DuckDB does (`sum / (count · 10^scale)`), and an `f64` sum for floating inputs. (4) Fix of a 123a gap: `aggregate_selected` evaluated keys and arguments over the whole chunk, so a filtered-out row could raise an error; on an error it now folds the compacted kept rows (`fold_kept_rows`, `#[cold]` and out of line: inlined, the second `fold_chunk` slowed Q22-shaped folds by ~20 %) and the error stands only if a kept row raises it. Measured, TPC-H SF 1, A/B of two release binaries alternating (median of 5 rounds, 10 for rechecks): **Q1 65 → 48 ms (−26 %)**, all 22 2,065 → 2,047 ms, every other query within noise except Q22 +4.7 % (substr-bound, see 126b). Profile before: `add_decimal` 22 %, `eval_decimal_mul` 19 %, `decimal_arith_row` 10 % of Q1. Tests: new `tests/decimal_arithmetic.rs` 4 (typing, DuckDB's overflow text, a 3,001-row chunk whose one large row errors unfiltered and not when filtered out, exact AVG), all values read from DuckDB 2.0. | done | `crates/pinkdb/src/exec/{expr,aggregate}.rs`, `crates/pinkdb/src/sql/binder.rs`, `crates/pinkdb/tests/decimal_arithmetic.rs`; small |
| 126a | **Decimal literals typed like DuckDB** (added 2026-10-28, found by wave 126): pinkdb types `1.5` as `DOUBLE`; DuckDB types it `DECIMAL(2,1)`, so `x - 1.5` over a `DECIMAL(15,2)` column is `DOUBLE` here and `DECIMAL(16,2)` there, and sums print without their scale. Type a decimal literal as the narrowest `DECIMAL(p,s)` that holds it (falling back to `DOUBLE` past 38 digits, as DuckDB does), keep exponent literals (`1e3`) `DOUBLE`, and fix the comparisons and arithmetic that relied on a `DOUBLE` literal. Gate: `bench/duckdiff.py`, the 22-query TPC-H check, and a probe corpus of literal arithmetic whose types and values match DuckDB. **Started 2026-10-28 and parked** (behind the performance plan; patch kept at `/tmp/w126/126a-wip.patch`). What it takes: (1) `peg_parser::number()` types a point literal without an exponent as `DECIMAL(width, scale)` per DuckDB's `NumberToValue` (src/parser/literal.cpp; `DOUBLE` past 38 digits or with an exponent); (2) `Value::data_type()` and `Vector::new_const` derive a bare decimal's precision as `max(digits, scale + 1)`, so `0.06` is `DECIMAL(3,2)` like DuckDB (only the bare `.5` form then differs: DuckDB `DECIMAL(1,1)`); (3) the constant folder must not fold when the folded value's type differs from the expression's (`3 * 0.5` is `DECIMAL(12,1)`); (4) `storage::coerce` takes `DECIMAL` into `DOUBLE` columns. With those, every literal-typing probe matched DuckDB, but 38 tests fail: array literals (`[1.5, 2.5]` becomes a `DECIMAL` array, so `FLOAT`/`DOUBLE` array casts, `vector_distance`, array storage round-trips), two index-scan tests built on `DOUBLE` literals (NaN/±0, collapsing numeric literals), `VARIANT` typing, and unit tests that expect `Double` literals. Array literal element typing and `DECIMAL` → `FLOAT`/`DOUBLE` array casts need doing first. | queued | after the performance plan; medium |
| 126b | **`substr` with DuckDB's semantics, vectorized** (added 2026-10-28, found by wave 126; done by Claude directly) — **done 2026-10-28**. Problem: (1) parity — pinkdb clamps the start to 1 and treats a negative length as empty, so `substr('hello',0,2)`, `substr('hello',-2)`, `substr('hello',-3,2)` and `substr('hello',2,-1)` give `he`, `hello`, `he`, `` where DuckDB gives `h`, `lo`, `ll`, `h`; (2) the SQL-standard `substring(x FROM a [FOR b])` form does not parse (TPC-H Q22 uses it; `bench/suite` rewrites it to `substr`); (3) speed — `ScalarFunc::Substr` runs per row through `Value`, clones the string and collects a `Vec<char>`, which dominates Q22 (the allocator shows up as `mach_absolute_time` in profiles) and makes it sensitive to unrelated code layout. Add a column-with-constant-arguments kernel over the string payload that slices by char boundaries (ASCII fast path) without per-row allocation beyond the result. Gate: an edge-case corpus (zero, negative, past-the-end, multi-byte UTF-8, NULLs) matching DuckDB, Q22 faster at SF 1, `bench/duckdiff.py` and the TPC-H check green. **Done:** `substr_range` ports DuckDB's `SubstringStartEnd` + `SubstringASCIIBounds` over the code-point count (ASCII strings slice bytes); `substr_slice` adds DuckDB's ±2³² range errors; the row path borrows instead of cloning; `eval_scalar_fast` gained a `VarChar` kernel with `IntArg` INTEGER/BIGINT arguments that ANDs the argument validity masks and reuses the input `Arc<str>` when the slice is the whole string. The binder accepts `substring` and requires integer start/length (NULL literals allowed); the parser takes `SUBSTRING(s FROM a [FOR b])` and `SUBSTRING(s FOR b)` as bare words, so `substring` stays a usable column name. Tests: new `tests/substring.rs` 3 (a 4-string × 9-offset × 8-length grid through the column kernel and through literals, pinned to DuckDB 2.0, the keyword forms, the range and type errors). TPC-H SF 1 Q22 182 → 70 ms (−62 %, A/B against 126, 8 rounds). Not done: `substring_grapheme`. | done | `crates/pinkdb/src/exec/expr.rs`, `crates/pinkdb/src/sql/{binder,peg_parser}.rs`, `crates/pinkdb/tests/substring.rs`; small |
| 126c | **`LIKE` compiled once, without per-row allocation** (added 2026-10-28, found profiling Q13 after 126b; done by Claude directly) — **done 2026-10-28**. `eval_like` read each row through `Vector::get` (two owned `String`s) and `like_match` collected both into `Vec<char>`: four allocations per row, the top of Q13's profile. Now `eval_like` reads the `VarChar` payloads in place, ANDs the validity masks, and compiles a literal pattern once per chunk into a `LikeMatcher`: `Segments` for `%`-and-text patterns (a port of DuckDB's `LikeMatcher` in src/function/scalar/string/like.cpp: anchored prefix/suffix, in-order `str::find` for the middle) or `Atoms` (the iterative backtracking matcher, now over byte positions of the original string). `like_escape` shares it. Also: a bare `NULL` in a comparison or `LIKE` binds as the other side's type (`null_of`), so `NULL LIKE 'a%'` is NULL like DuckDB instead of a type error. Tests: new `tests/like.rs` 2 (15 strings × 28 patterns pinned to DuckDB 2.0 through a pattern column, a constant pattern and `NOT LIKE`; NULLs and `like_escape`). TPC-H SF 1, A/B against 126b (8 rounds): Q13 250 → 108 ms (−57 %), Q20 101 → 66 (−34 %), Q14 −15 %, Q16 −11 %, Q9 −8 %. | done | `crates/pinkdb/src/exec/expr.rs`, `crates/pinkdb/src/sql/binder.rs`, `crates/pinkdb/tests/like.rs`; small |
| 126d | **`LIKE` segment search without per-call setup** (added 2026-10-28, from Q13's profile after 125, where `StrSearcher::new` was the second-largest frame; done by Claude directly) — **done 2026-10-28**. `find_segment` scans for the needle's rarest byte (a static English byte-frequency rank, the heuristic memchr's `memmem` uses) with `str::find(char)` and compares the needle in place; non-ASCII needles keep `str::find`. Scanning for the first byte instead measured Q13 +14 % (`s` is common in comments), so the rarest byte is the point. A bug the grid caught while building it: the scan offset can fall inside a multi-byte char of a non-ASCII string, so it advances to the next char boundary (pinned by `ascii_segments_in_non_ascii_strings`). TPC-H SF 1, A/B against 125 (8 rounds): Q20 49 → 43 ms, Q13 99 → 92, Q16 45 → 43. Also measured and dropped: packing the join table's per-row link, hash salt, integer key and location into one 24-byte `ChainEntry` made Q5 and Q13 25 % slower (10 rounds) — the serial per-row build loop costs more than the probe saves; patch kept for the partitioned-build step of 125. | done | `crates/pinkdb/src/exec/expr.rs`, `crates/pinkdb/tests/like.rs`; small |
| 124 | **Spill to disk for joins, aggregates and sorts** (added 2026-10-27, user; layer 3; a correctness requirement at SF 10+, not only speed: wave 111 measured 676 MB peak RSS for Q9 at SF 1). A memory budget (`SET memory_limit`, default a fraction of RAM; reported in profiles) enforced by a buffer manager; hash-join builds, hash aggregates and sorts partition (radix) and spill partitions to temporary files when over budget, then process them partition by partition; temporary files are encrypted when the store is (wave 46 keys), cleaned up on success, failure and crash (at open). After DuckDB's `buffer_manager`, `temporary_file_manager`, `radix_partitioned_hashtable.cpp` and the external join/sort. Gate: TPC-H SF 10 runs to completion under a 2 GB limit with checksums matching DuckDB; a test forces spilling at tiny budgets for every operator; no slowdown when nothing spills. | queued | after 123 |
| 112 | **First-class C API** (added 2026-10-26, user: "C"; foundation for 113–118): promote `crates/pinkdb-ffi` (`include/pinkdb.h`, ABI v1, 33 exports today) to a supported public C library. A versioned, documented header (every function's ownership, threading and error contract; the ten `pinkdb_last_error_class` values); shared **and** static libraries with an install target and a `pinkdb.pc` pkg-config file; an ABI stability policy (additive minors, `pinkdb_abi_version` gating, a header/symbol diff check in CI); the deferred **Arrow C Data Interface** export (`pinkdb_result_to_arrow`, from wave 71) so every binding below can hand results to Arrow-native tools without copying; examples, an API reference page, and tests under ASan/UBSan plus a leak check. No engine changes beyond the export surface. | queued | `crates/pinkdb-ffi`, website docs; before 113–118 |
| 113 | **Full Python support** (added 2026-10-26, user: "full python support"; today `bindings/python` is a `ctypes` DB-API 2.0 layer plus a local wheel, wave 92): a native module (PyO3 or the C ABI, decide and measure) with DuckDB-style ergonomics: `pinkdb.connect()`, `sql()`/`execute()` returning a relation with `.fetchall()`, `.df()` (pandas), `.pl()` (polars), `.arrow()` (zero-copy through 112's Arrow export); querying a pandas/polars/Arrow object by name (`register` and replacement scans); writing a DataFrame into a table; Python scalar UDFs; type stubs (`.pyi`) and full typing; the SQLAlchemy dialect; Jupyter display; the agent, memory and lake surfaces reachable from Python; wheels for macOS (arm64/x86_64), manylinux and Windows built in CI and published to PyPI on the release train. Gate: the shared conformance corpus, plus a pandas/polars/Arrow round trip of every type. | queued | `bindings/python`; after 112 |
| 114 | **R support** (added 2026-10-26, user): an R package over the C ABI (C glue via `.Call`, no Rcpp unless it earns its weight) implementing **DBI** (`dbConnect(pinkdb::pinkdb(), ...)`, `dbGetQuery`, `dbExecute`, `dbWriteTable`, `dbReadTable`, parameterized queries, transactions), data.frame and Arrow (`nanoarrow`, via 112) results, **dbplyr** backend support, and the shared conformance corpus as testthat tests; passes `R CMD check --as-cran` on macOS and Linux; CRAN/r-universe submission recorded as a release step. | queued | new `bindings/r`; after 112 |
| 115 | **C++ support** (added 2026-10-26, user): a header-only C++17 client library over the C ABI (distinct from `ext-sdk/cpp`, which builds extensions): RAII `Database`/`Connection`/`PreparedStatement`/`Result`, exceptions carrying the stable error class, `std::optional`/`std::string_view`/`std::chrono` mappings, a row iterator, an Arrow export (via 112); a CMake package (`find_package(pinkdb)`) plus pkg-config, examples, and the conformance corpus as tests under ASan. | queued | new `bindings/cpp`; after 112 |
| 116 | **Swift support** (added 2026-10-26, user): a SwiftPM package (a system-library target with a module map over `pinkdb.h`, plus a Swift API layer): `Database`/`Connection`/`Statement`, `throws` errors with the stable class, `Decodable` row mapping, async/await over a background executor; macOS and Linux first, an iOS `xcframework` measured and decided (binary size, sandboxed paths); the conformance corpus as XCTest; DocC docs. | queued | new `bindings/swift`; after 112 |
| 117 | **C# / .NET support** (added 2026-10-26, user; wave 92 listed P/Invoke as remaining): a NuGet package with native runtimes (`runtimes/{osx-arm64,osx-x64,linux-x64,linux-arm64,win-x64}/native`), a `LibraryImport` (source-generated P/Invoke) layer, and an **ADO.NET provider** (`PinkDbConnection`/`PinkDbCommand`/`PinkDbDataReader`/`PinkDbParameter`, transactions) so Dapper and EF-style tooling work; Arrow interop via 112; the conformance corpus as xUnit tests on .NET 8+. | queued | new `bindings/dotnet`; after 112 |
| 118 | **Zig support** (added 2026-10-26, user): a Zig package (`build.zig` / `build.zig.zon`) wrapping the C ABI through `@cImport`, with an idiomatic API: error unions mapped from the stable error classes, explicit allocators for result buffers, comptime-typed row decoding, and `defer`-friendly handles; links `libpinkdb` statically or dynamically (via 112's pkg-config); the conformance corpus as `zig build test`. Pin a Zig version and record the policy for following Zig releases. | queued | new `bindings/zig`; after 112 |
| 119 | **GPU acceleration** (added 2026-10-26, user: "also add GPU support"): an opt-in, feature-gated acceleration path that never changes results and never grows the default binary. **Phase 1: measure before building.** Prototype the workloads most likely to win: brute-force and HNSW-build vector distance (107b/107d), large hash aggregations and joins, and filter/projection over wide scans. Measure host↔device transfer cost against the CPU path on the machines that matter (Apple Metal first, since that's the dev machine; then CUDA or Vulkan on Linux). **Phase 2 only where Phase 1 shows a clear, repeatable win** (e.g. ≥ 3× end to end, transfers included): a backend chosen in the plan (Metal via `objc2`/`metal` on macOS, `cudarc`/CUDA on Linux, or a portable `wgpu` compute path; the dependency weight is part of the decision, under the project's "stay light" rule), shipped as a linked extension (`gpu` feature, off by default) behind an operator hook the optimizer picks by size thresholds, with automatic CPU fallback, bit-for-bit result tests against the CPU path (or documented float tolerances), and a `SET gpu = off` escape hatch. Uses the 111 benchmark suite as its yardstick. | queued | new `extensions/gpu`; after 111 |

**▶ Queue order after wave 89 (user, Oct 22):** ~~**90**~~ (gate passed: `v0.1.0` released 2026-10-22) → ~~**94**~~ (done 2026-10-22) → ~~**91**~~ (done 2026-10-22) → ~~**96a**~~ (done 2026-10-23: SQLite/Postgres lake catalogs) → **98** (async remote I/O + streaming parallel scans; **98a + 98b done**; CSV read-ahead, the remote-file cache and streaming scans remain) → ~~**102**~~ (done 2026-10-22) → **95** (dialect completeness, inventory first, then batches led by `RETURNING` / `ON CONFLICT` / `UPDATE … FROM` / `DELETE … USING` / `MERGE`) → ~~**93**~~ (done 2026-10-25: extension SDK refresh, SQL `INSTALL`/`LOAD`, `repo` release variant) ~~**76**~~ (done 2026-10-24: CSV import options) → **72** (Iceberg) → ~~**96b**~~ (phase 1 done 2026-10-25: stateless `--catalog`/`--data` server; phase 2 = shared catalogs) → ~~**92**~~ (first pass done 2026-10-24: Node.js + Python DB-API 2.0 + Go module + shared conformance corpus; JDBC/.NET/ADBC and publishing remain) → **97** (stale-row audit) → **99** (agent CLI mode) → ~~**100**~~ (done 2026-10-24: triggers) → **101** (VARIANT, plan first). Waves still open outside this list (79's range/zone-map index-vs-scan choice, the GAPS debug-stack item, recorded follow-ups) are picked up when they block something or as small fillers.

Cadence: every fifth wave (40, 45, 50, 55, 111 …) is a performance-tuning wave — no new features, bench-driven speed work against the committed suite in `bench/suite`.

Docs discipline: every integrated wave appends a [CHANGELOG.md](CHANGELOG.md) entry (the integrator does it together with the ROADMAP progress entry) and keeps `README.md` current for user-visible surfaces; the website pages ship with the wave too (wave 73 brought them current through 64; the CI docs job fails on stale generated output). Loadable-extension waves also update their in-repo docs.

Parallel feature work (Oct 2026): the first batch — waves 58 (SQL completeness),
60 (functions) and 61 (optimizer work) — landed one at a time (60 → 61 → 58)
with full gates plus CI. The second batch (57 lake encryption, 59 prepared
statements, 62 DECIMAL/INTERVAL, 41 PK/FK constraints) finished as: **59, 62
and 41 landed** (the two `.pink` v5 proposals were reconciled into one v5 —
constraint section gated by flags bit 0 plus DECIMAL/INTERVAL dtype tags; wave
41 also shipped conservative FK→PK join elimination). **57 was re-dispatched**
on the current base (its old partial kept as the design reference). **Third
batch** (63 + 64, in `wt-lake-incr` / `wt-lake-versions` off 8b401cd): both
**landed** — 64 first, then 63 integrated on top with the flagged
`pink-lake/src/{lib,catalog,attach}.rs` overlaps resolved by hand (14
conflicts). **In flight (fourth batch):** 57 (lake encryption, reusing the
`wt-lake-encryption` worktree), 65 (schema evolution, `wt-lake-schema`) and 66
(partitioning, `wt-lake-partition`). **Queue discipline (Oct 17):** the
lake-parity block (63–68) finishes first, in order — **65 → 66 → 67 → 68**
(user-flagged: 67 starts after 66; 65/66/57 continue from their secured
`partial-stuck.diff` partials after the second disk-full incident). Then
dispatches return to the numeric list — **74 (naming sweep) + 44 (stored
procedures with JavaScript, the user-flagged next item)**, then 42 (indexes),
43 (matviews), 46… — with new items always taking the next free number (77
macros, 78 server UI queued); the file-format block (70–72) plus 75 (httpfs)
and 76 (import options) runs after the rename. Integrations are one patch at a
time with full gates + CI (max two worktrees per the disk-space policy; every
brief now carries the disk guard). **▶ Resumed (Oct 18, user):** waves **42 (indexes)** and **43
(materialized views)** landed in the user's own session (`a79cd41`,
`2be5d3b`); 44 (procedures), 68 (inlining/CDC), 74 (rename) and 80 (MV
refresh) landed with full gates. **Corrected order (user, Oct 18): finish the
SKIPPED block first** — **46 (TLS), 47 (async I/O), 48 (RBAC), 49 (wasm
extensions), 50 (perf), 51 (Rust extension SDK), 52 (json), 53 (string), 54
(math), 56 (datetime)** in numeric order — then **69** (prepared binding
wrappers), then the file-access block (**70–72, 75, 76**), and only then the
new/follow-up items (**77 macros, 78 server UI, 79 index persistence, 81
quickjs-ng, 83 procedure transactions**). Wave **82 (Rust upgrade) is in
flight solo**; nothing touches the crates in parallel with an edition
migration.

Lake-scan plan (lake workstream, sub-waves 45b–45o) and the multi-process
durability workstream (45m–45n): stage 1 shipped as 45d — footer-only
`.lake attach` + exact `COUNT`/`MIN`/`MAX` from stored statistics + lazy materialize; stage 2
shipped as 45e — projection pushdown (an attached scan decodes only the columns a query
references); stage 3 (per-chunk statistics + filter/zone-map pruning) is the remaining
follow-up. The lean `lake-lite` build shipped as 45i (2.69 MB vs 45.3 MB; `.pink` v4 data
files) and `.pink` LZ frames (tag 8) shipped as 45j (text corpora 3–5× smaller). 45k shipped
exact attach fast paths for lean `.pink` lakes — a CRC-protected per-column statistics block
published in the manifest on every lean save, so `COUNT`/`MIN`/`MAX` are answered with zero
data-file reads under the same exactness gates as the Vortex attach. Known follow-ups (not
committed): lean projection pushdown (`.pink` has no projected reader — a fallback decodes
the whole table), per-chunk statistics + filter/zone pruning (stage 3), and the parked cold
order-by/group-by items. Durability: 45m made durable `.pink` databases multi-process
(per-operation write lease, lock-free epoch refresh, WAL v3, encrypted container v2); 45n
shipped the stability-hardening round — chaos-soak exactly-once evidence under SIGKILL,
lock-file identity that fails closed on replacement, WAL fuzzing and gap closures (details
in the progress log below). CI is fully green on **493ea50** (run 38931704447, all nine
jobs; the duckdiff job pins the DuckDB 2.0 alpha as the parity reference).
Stage 5 — optimistic disjoint-table commits —
**shipped as 45o**: WAL v4 touched sets, lease-free transactions with commit-time
read/write-set validation (write skew prevented), typed conflicts and bounded CLI
retry; the contention bench measures 2.31–2.49× on overlapping transactions with
disjoint writes, with an honest no-win on replay-bound write-heavy workloads.
**Wave 57 (queued): pink-lake data encryption/decryption
at rest** — one Argon2id KDF per lake session (salt in the catalog), AEAD containers for
the catalog/snapshots and the data files, `PINKDB_KEY` UX consistent with the engine,
plaintext lakes unaffected.

DuckLake parity program (Oct 17; queued as waves 63–68, to dispatch in a later
worktree batch — the four in-flight worktrees are the concurrency cap):
incremental lake writes/mutations (63), time travel + snapshot metadata (64),
schema evolution (65), partitioning + zone-map pruning (66), maintenance/
compaction (67), inlined small writes + CDC (68). The probed feature delta is
recorded in the progress log below.

Deferred (not currently scheduled): JS/wasm, Node-native, and Java bindings.

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
- 2026-10-15 — Wave 45 completed: after the byte-neutral part, the agreed lake layout change
  (format_version 3: 8,192-row / ~1 MiB Vortex blocks, loader slices back into engine
  chunks; stats kept for future pruning; an immutable v2 fixture pins old-lake reads) took
  save @10M to ~0.3 s and open to ~0.1 s. Query side: DISTINCT now runs through the
  parallel aggregation; window functions parallelize partitioning, sorting and output
  assembly (output chunks share input columns); sorts compare 8-byte key prefixes first and
  rebuild output chunks in parallel. 1M battery 416 → 245–250 ms. Measured and dropped: a
  radix-parallel merge for 1M-group aggregations (−2–3% only — the cost is per-group heap
  churn). Next round's breadcrumb: struct-of-arrays group storage (typed key columns +
  accumulator arrays, no per-group allocations) for high-cardinality GROUP BY / DISTINCT
  (~50 ms @1M); `proj_func` (per-row string allocation in `upper`) is the other one.
- 2026-10-15 — Wave 45b (89d0a2f): writer block-size A/B — negative result, kept the 1 MiB
  write-block target. The 8/16 MiB coalescing from the DuckDB/Vortex prior art does not
  transfer: bytes move by only −0.15%, save is not faster (10M median +9–11%), and
  `.lake open` is ~2× slower (13→24 ms @1M, 119→241 ms @10M). Root cause measured: the
  reader sub-splits stored chunks at ~100k rows, so larger targets leave ragged tails that
  are not multiples of the engine's 2,048-row chunks and the append takes `append_chunk`'s
  element-wise merge path (append phase 0.7 → 110 ms @10M). The scheme-exclusion half of
  the A/B (RunEnd/IntRLE/ALPRD/FloatRLE) is not expressible through the vortex facade
  without a new dependency and is byte-neutral on this corpus anyway (no such encodings
  appear in the written files). Comment-only change; format_version stays 3. 517 tests.
- 2026-10-15 — Wave 45c (ca5eb37): lake open 3.7× — the loader moves the `fs::read` buffer
  into Vortex (`ByteBuffer::from`) instead of copying it again; `DataTable::append_shared_chunk`
  shares `Arc<Vector>` columns, so the decoded store no longer deep-clones ~250 MB @10M; the
  varchar decode interns per chunk keyed by the exact 16-byte Vortex view. `.lake open` @10M
  110–126 → 30 ms printed (open CPU 540 → 190 ms, peak RSS 996 → 352 MB); @1M 13 → 4 ms.
  Official cold-process @10M: count 38.8 vs DuckLake 35.2 ms, group-by 72.2 vs 64.3, filter
  62.2 vs 52.2, order 77.2 vs 55.4 (was 2.8–4.2× behind; now 1.1–1.4×). Ingest wall 0.482 s /
  save 0.275 s; storage 25.1 MB vs 92.6 MB. Measured and rejected: a file-backed reader and
  rayon min-length batching (no gain). 518 tests.
- 2026-10-15 — Wave 45d (5a024ea): `.lake attach` — footer-only `.lake attach <dir|s3>
  [version]` (+`.lake detach`, `.lake attached`); exact `COUNT(*)`/`COUNT(col)`/`MIN`/`MAX`
  answered from stored statistics under strict gates (exact NullCount; `Precision::Exact`
  min/max; `NaNCount == 0`; no ±0.0 DOUBLE stat; multi-agg all-or-nothing; anything unproven
  materializes lazily and caches). Cold @10M: attach+`COUNT(*)` **4.94 ms** vs DuckLake 34.92
  vs `.lake open` 34.76; attach+`MIN`/`MAX` 4.90 ms; attach overhead ~1.2–1.7 ms, constant in
  file size; fallback queries within noise of open. @1M attach+`COUNT(*)` 4.62 ms. Also closed
  a real hazard: durable stores now refuse mutating SQL that reads an attached table (the
  logged text could not replay → unopenable WAL). Lake-scan plan stage 1. Gates: 542 workspace /
  CLI lake+s3 59 / pink-lake s3 123 / duckdiff 60/60.
- 2026-10-15 — Wave 45e (64a29e1): `.lake attach` projection pushdown (lake-scan plan stage 2).
  New address-keyed, exhaustive referenced-columns analysis (`plan.rs::ScanProjections`;
  Distinct/Union/Join/Cte stay conservative = all columns) drives the attached-lake scan, which
  decodes only referenced columns into a per-column lazy cache (positioned local reads). Safety
  net: unreferenced columns become zero-length placeholders and a missed reference is a hard
  internal error — never silent NULLs. Cold-process @10M: filter 60.6 → 41.9 ms (1.45×,
  1 column; DuckLake 51.9), top-10 74.0 → 60.5 ms (1.22×, 2 columns; 55.4), group-by
  72.8 → 68.9 ms (1.06×, 2 columns; 63.8), `COUNT(*)` unchanged (~5 ms, 0 decodes). @1M:
  filter 10.7 → 8.9, top-10 12.5 → 11.1. Cold scorecard vs DuckLake: count 6.8× faster,
  filter 1.24× faster, order/group ~1.1–1.2× behind. Gates: 546 workspace / CLI lake+s3 60 /
  pink-lake 125, duckdiff 60/60.
- 2026-10-15 — Wave 45h (76a787e): `.pink` format v4 — typed encode/decode paths (no per-row
  `Value`; chunk-parallel order-preserving save; serial bytes == parallel bytes), new
  DELTA_BITPACK / DICT_BITPACK / ALP encodings (bit-exact exceptions) and an all-valid validity
  flag; magic `PINKDB04`, CRC kept, v1–v3 readable (new v2/v3 fixtures), `PINKDB05`+ clean
  "newer format" error; the `PINKENC1` encryption container is untouched; also fixed a
  pre-existing `unpack_bits` width-64 bug. 1M sales `.pink` 14,042,575 → 2,679,387 B (1.015×
  the Vortex bytes), save 0.165 → 0.029 s, open 0.188 → 0.026 s; 10M sales 140,424,925 →
  26,792,121 B (26.8 MB, 1.067× Vortex), save 1.682 → 0.286 s, open 1.897 → 0.256 s. No size
  regressions on adversarial corpora (worst +4 B of flags on a 755 B file); high-entropy
  strings/floats stay >1.3× Vortex (no FSST/LZ yet — recorded as stretch). Gates: 566
  workspace / 392 encryption / CLI lake+s3 60, duckdiff 60/60.
- 2026-10-15 — Wave 45i (0394795): lean lake build `lake-lite` — `vortex` + `rayon` in
  pink-lake became optional behind `vortex-format`; the CLI's full `lake` feature enables it,
  and the new `lake-lite` feature is the lean lake over `.pink` v4 data files (catalog
  `format_version` 1). `s3` now implies `lake-lite` (README says so explicitly), so the full
  lake over S3/R2 is `lake,s3`. Sizes (release): default 2,436,320 B; `lake-lite` 2,692,464 B
  (+250 KiB over default, −40.7 MiB vs full); `lake-lite,s3` 4,701,120 B; `lake` (full)
  45,333,984 B (the Vortex dependency measures 42.6 MB of it). Lean trade-offs: no `.lake
  attach` fast paths yet (45k, in progress), eager open 0.023 s @1M / 0.238 s @10M printed vs
  full 0.004 / 0.031 s, saves ≈ equal (0.054 / 0.321 s), data files 1.6–1.8% larger; lean
  errors for v2/v3 lakes and `.lake attach` are explicit rebuild-with-`lake` messages.
  Cross-build verified: a lake written by the lean build opens bit-exactly in the full build
  (which migrates to v3, leaving the v1 files untouched; checked-in fixture), and a v3 lake
  in a lean build refuses cleanly. Gates: workspace 513 default (the lean set; full coverage
  via feature runs — CLI 61 `lake,s3` / 35 lean; pink-lake 18 lean / 72 `vortex-format` /
  126 `s3,vortex-format`), duckdiff 60/60; CI runs both feature sets.
- 2026-10-15 — Wave 45j (ff1ea61): `.pink` LZ frame (encoding tag 8, reserved since v4) —
  measurement first: the v4 losses vs Vortex are compressible TEXT, not random data (random
  f64 1.00×, random strings 1.07–1.10×; JSON-like 5.71×, logs 3.48×, URLs 3.15× behind). A
  hand-rolled greedy LZ77 (64 KiB window, 4-byte hash, min match 4, max 130; no deps, no
  unsafe) frames a chunk as `u32 raw_len | u8 base_tag | u32 token_len | tokens` (base tags
  0..=6), emitted only when strictly smaller; candidate gating needs the best existing
  candidate ≥1024 B and a 4 KiB prefix probe ≥10% saved (sales VARCHAR chunks win with a
  bitpacked dict and skip the scan). Results (1M rows, before → after, vs Vortex): JSON
  70.4 → 15.7 MB (5.71× → 1.27×), logs 118.7 → 44.5 MB (3.48× → 1.30×), URLs 103.5 → 41.4 MB
  (3.15× → 1.26×); random corpora and both sales files byte-identical (sales save +0–1%);
  LZ also speeds save/open on text. FSST (tag 9) deliberately NOT implemented, with the
  measured ceiling (post-LZ gaps 9.8% / 6.5%; FSST floor estimate 3.2% / 4.3%) — the tag
  stays reserved with a design. Invariants: bit-exact round trips (forced tag-8 frames),
  determinism, old binaries reject tag-8 files cleanly (`unsupported v4 encoding tag 8;
  upgrade pinkdb`), malformed-frame tests. Gates: workspace 517/0, encryption 396/0, CLI
  61/35, pink-lake 126/0, duckdiff 60/60. Live: 5.88 MB JSON CSV → 699 KB `.pink` with an
  exact round trip.
- 2026-10-15 — Wave 45k (bede9fc): exact attach fast paths for lean `.pink` lakes. Every lean
  save now publishes a per-column **statistics block** in the manifest (`catalog.json` ==
  `snapshots/<v>.json`, published atomically with the data-file reference): rows, exact NULL
  counts, bit-exact min/max as canonical strings per type, and NaN counts, CRC-32-protected
  over the block including its catalog key, data-file path and row count. `.lake attach`
  verifies the CRC, cross-checks table/file/rows, the block version and every value's
  decodability, and fails closed on corruption ("the statistics block failed its checksum
  (corrupted or edited metadata) — use .lake open, or re-save the store to publish fresh
  statistics"); attach reads NO data file. The engine collects the facts inside the existing
  parallel chunk-encode pass (`PinkDb::to_pink_bytes_with_stats`; `.pink` bytes unchanged;
  save cost +1 ms @1M / +17 ms @10M). One shared exactness gate for both formats
  (`stats::serve_column_stats`; the Vortex footer path delegates) — the same gates as the
  Vortex attach. `.lake attach` is now available in the lean `lake-lite` build
  ("statistics-only"); a lake saved before 45k (no block) refuses with a clear re-save
  message. Cold results: lean `.lake attach` + `COUNT`/`MIN`/`MAX` 2.64 ms @1M / 2.31 ms @10M
  (overhead ~0.1–0.2 ms, constant in file size, 0 decodes) vs lean eager open 29.9 / 269.5 ms;
  a full (Vortex) build attaches `.pink` lakes from the same block (3.44 / 3.45 ms), and full
  Vortex attach is 4.07 / 4.13 ms. Also fixed a pre-existing `.pink` v4 correctness bug:
  `pack_bits`/`unpack_bits` u64 accumulators silently dropped high bits for widths > 56
  (64-bit BITPACK and wide DELTA_BITPACK decoded wrong, e.g. i64 extremes) — switched to u128
  (bytes unchanged for previously-correct widths; regression test added). Known follow-ups
  (not committed): lean projection pushdown (`.pink` has no projected reader — a fallback
  decodes the whole table), per-chunk statistics + filter/zone pruning (stage 3), and the
  parked cold order-by/group-by items. Gates: workspace 530/0, encryption 398/0, CLI
  `lake,s3` 62/0 + `lake-lite` 38/0, pink-lake `s3,vortex-format` 132/0, duckdiff 60/60.
- 2026-10-16 — Wave 45m (b026765): multi-process durable `.pink` writers (first slice).
  Durable databases are no longer single-process: any local process may open one read-write,
  and writers serialize on a short-held per-operation **write lease** (an exclusive `flock` on
  `<db>.pink.lock` plus an in-process refcount gate) instead of the wave-40b lifetime-exclusive
  lock. Readers never take the lease: the lock file's first 48 bytes carry a dual-slot/CRC
  **epoch region**, and a reader that detects a change with one `pread` replays the WAL frames
  past its applied position (`wal_id`, generation, file identity, byte/LSN offsets, checkpoint
  identity) under the lease — refresh at autocommit boundaries and before `BEGIN`, never
  mid-statement; replay runs with the WAL detached (never re-logging), restores the lineage
  session around it and the local session after it, bypasses and restores the extension gate,
  and poisons the store on a replay error; a replaced WAL or checkpoint triggers a full
  rebuild that preserves views and attachments. Busy timeout default 5 s (CLI
  `PINKDB_BUSY_TIMEOUT_MS`; library `DurableOpenOptions`/`set_busy_timeout`); an open on a
  filesystem that reports locking unsupported fails closed (`LockMode::Required`, the default)
  with `LockMode::UnsafeSingleProcess` as the explicit opt-out. WAL v3: a 96-byte header with a
  16-byte `wal_id` (the ABA guard across checkpoints) and dual 24-byte commit slots; every
  append publishes `(seq + 1, last_lsn)` into the alternate slot and bumps the lock epoch;
  v2 files upgrade in place under the lease, and older builds refuse v3 cleanly through the
  shared unsupported-version path. Encrypted WALs use `PINKENC1` container v2 (a fixed 88-byte
  plaintext publish block — `wal_id`, CRC, the dual slots — before the AEAD records; container
  v1 stays readable and is rebuilt as v2 on the next keyed open). Checkpoints refresh first
  under the lease, before writing `.tmp` — without that a checkpoint could silently drop
  another process's commits (the L0 hazard). Transactions: `BEGIN` holds the lease to
  `COMMIT`/`ROLLBACK`; readers never block and never see uncommitted rows. Evidence: new
  `crates/pinkdb-cli/tests/multiprocess.rs` (13 spawned-process scenarios incl. the L0 window,
  kill9 mid-insert, kill9 in the checkpoint window, generation reload, stale writer, busy
  timeout, long-transaction serialization, a 4×200 stress, the v2 upgrade and a mixed
  old-build busy) + 2 new crash windows (the W1–W3 / C1–C6 / R1–R3 matrix is covered) +
  `benches/durable.rs`; single-process perf shows no regression — inserts 465.7 → 413.5 s
  across runs, selects −10%, checkpoints −8%, within the ≤3% gate. Gates: workspace 559/0,
  encryption 413/0, CLI 77/53, pink-lake 132/0, duckdiff 60/60, kill sweeps clean; live demo:
  two CLI processes inserted into one `db.pink`, a third read n=2 s=3. Honest gaps: the
  keyed-WAL rebuild was kept for nonce safety instead of the design's narrowing; the
  in-process gate is a lease refcount; no network-FS detection without libc; the busy report
  is an `Execution` message (no new variant); stage 5 (optimistic disjoint-table commits) is
  deferred; an old *binary* was not executed against a v3 WAL (refusal by construction, the
  version path is tested). A follow-up stability-hardening wave (chaos soak, fuzzing, gap
  closures) is in progress.
- 2026-10-16 — CI unblocked (dd53f41…a837896): first fully green CI run (38875669660; all
  nine jobs, incl. the kill loops, wasm check, release build and the DuckDB differential
  job). The saga: lake fixture data files had been silently excluded by `.gitignore`
  (`*.pink`) since the wave-45i fixtures — 18 fixture files committed and the ignore
  negated; the Python bindings test still asserted the pre-45m single-live-handle lock —
  rewritten for multi-process semantics; `cargo audit`: rustls 0.23.45 advisories →
  0.23.46; deploy docs: the workflow read `CLOUDFLARE_*` secrets but the org provides
  `CF_API_TOKEN` / `CF_ACCOUNT_ID` — both names accepted now (Deploy docs green, site
  deployed); the S3 docker harness: four stale pre-wave-40b exit-code expectations
  (race losers and an intentional refusal are supposed to exit 1) were replaced with
  outcome-based checks (per-outcome exit codes + documented-conflict message matching);
  Linux `pwrite` on `O_APPEND`: WAL commit-slot hint writes had been appending to EOF on
  Linux (macOS honours the offset), failing five WAL tests only on CI — `publish()` now
  uses a positioned handle (reproduced and verified in a Linux Docker container); the new
  stable-clippy lint `chunks_exact_to_as_chunks` (×5 workspace-wide) → `as_chunks`; kill
  loops: a step-level `PINKDB_KEY` made the plain runs write encrypted checkpoints the
  non-crypto binary could not reopen — the key is scoped to the encrypted invocation and
  `bench/kill_loop.py` now strips it from plain children (its `--encrypted` env assignment
  had been a no-op local).
- 2026-10-16 — Wave 45n (6d227ec): multi-process stability hardening. Chaos soak
  `bench/mp_soak.py` (ack-authoritative writers — a unit is acknowledged only when its own
  success output appears with no Error line; kill-stuck detection, fd/RSS sampling,
  fresh-process exactly-once verification): 300 s default (272 workers, 180 kills, 31,836
  acked units) and 300 s encrypted (194 workers, 184 kills, 3,739 units) both verified
  exact; 15 s CI modes exact; no leaks. Lock-file identity robustness (dev/ino checks at
  every acquisition/wait/mutation boundary; fails closed; residual TOCTOU documented). WAL
  fuzz (every-offset truncation + byte flips) found and fixed a real bug: a single-bit
  v3→v2 version-word flip took the legacy-upgrade path and silently dropped all records —
  now refused via the corrected-header CRC (regression test). Gap closures: real-extension
  replay, attached lakes across rebuild, save_snapshot vs a real process, epoch fast-path
  counters. Contention: 20 processes, busy 0, perfect fairness, no livelock. Perf within
  noise (≤3% budget). Gates: 574 workspace / 424 encryption / 82+57 CLI / 132 pink-lake /
  duckdiff 60/60 / multiprocess 17/17 + Linux Docker (lib 386, encryption 403,
  multiprocess 17/17).
- 2026-10-16 — Wave 45o (optimistic multi-writer commits, stage 5): WAL **v4** — every
  record carries its touched set (tables its statements read or wrote + `CREATE`/`DROP
  DATABASE` names; `u32::MAX` = unknown for records adapted from v2/v3); v2/v3 upgrade
  in place, encrypted WAL containers are v3, encrypted checkpoints stay container-v2,
  plaintext `.pink` bytes untouched, older builds refuse v4 cleanly (`BadVersion`).
  `BEGIN` snapshots `(wal_id, generation, applied LSN, checkpoint identity)` and holds
  no lease; `COMMIT` leases only for refresh → validate → append → publish. Validation
  is table-granularity against the frame journal: conflict iff a frame committed in
  `(base, now]` touched a table the unit read or wrote — read-set validation also
  prevents write skew across processes (both directions tested). Conflicts are typed
  (`write conflict on table "db.name" (… lsn N …); retry`); the engine aborts, the CLI
  retries (`PINKDB_TXN_RETRIES`, default 3) including a one-call `BEGIN; …; COMMIT;`
  with the pre-BEGIN session restored. Autocommit runs optimistically too: on conflict
  the never-logged effect is undone and the statement re-executes under the lease
  (exactly-once, no user-visible error; a lease that cannot be acquired after an
  optimistic execution fail-stops the handle — reopen recovers). Checkpoints and
  `.save` snapshots invalidate open bases (retryable). Matrix: 26 multiprocess
  scenarios (11 new, incl. kill -9 mid-optimistic-transaction, write skew, the
  one-call CLI retry, autocommit hammering 4×100 and 3×60 exactly-once) + 10 new wal
  unit tests. `bench/mp_optimistic.py`: 4 overlapping transactions with disjoint
  writes **2.31–2.49×** vs the serialized binary (old binary 0.98–0.99×); honest
  no-win on write-heavy replay-bound workloads (full-replica refresh materializes
  peers' frames). Budget: `Sync` inserts within noise (≤3% holds); `Batch` ~9% faster
  after the commit-check fast path. Verified on main with a clean workspace rebuild:
  fmt, clippy (pinkdb, CLI lake+s3), pinkdb 396/396 + encryption 413/413, multiprocess
  26/26, duckdiff 60/60, workspace all green, `mp_soak.py --ci` PASS (1,135 acked
  units, 18 kills, exactly-once across 23 writers); Linux Docker 396/413/26 all green.
- 2026-10-16 — Wave 60 (functions; worktree fan-out): window `lag`/`lead` (offset,
  default; negative offset flips direction like DuckDB; frame-ignoring), `ntile`
  (larger buckets first; `n ≤ 0` errors), `first_value`/`last_value` (frame-respecting;
  empty frame → NULL); scalar `||` (new `BinaryOp::Concat`, strict NULL, precedence
  pinned), `concat` (skips NULLs), `trim`/`ltrim`/`rtrim` (charset forms), `replace`,
  `nullif`, `greatest`/`least` (NULL-skipping, NaN-greatest, common-type rules),
  `extract`/`date_part` (ISO week and isoyear, millisecond/microsecond/epoch,
  DATE-as-midnight), `sqrt`, `floor`/`ceil`/`ceiling`, `sign`, `power`/`pow`, `mod`
  (int + double; zero-divisor errors). Semantics pinned with probes against DuckDB,
  not assumptions; `bench/duckdiff.py` grew 60 → **88 probes, all passing**. Deferred
  with reasons in the worktree report: `IGNORE NULLS`, non-constant offsets and
  `ntile`, SQL-standard `trim(BOTH … FROM …)` syntax, `%` for doubles. Gates on main
  after 45o: fmt, clippy `-D warnings`, pinkdb 403/403, CLI lake+s3 green
  (multiprocess 27/27 including the lake-gated test), duckdiff 88/88.
- 2026-10-16 — Wave 61 (optimizer work; worktree fan-out): projection pushdown for
  in-memory scans (reuses the `ScanProjections` bound; unreferenced columns are shared
  zero-length placeholders — the attached-lake precedent), LIMIT short-circuiting
  (`LIMIT 0` returns the schema without evaluating; a chunk-at-a-time `Stream` cursor
  over Scan/SingleRow/Filter/Project/UNION ALL/Limit stops pulling once N rows exist;
  blocking chains still materialize; LIMIT-over-Sort keeps top-N), join build-side
  choice for INNER joins (shape-based row estimates with exact table counts; unknown
  or tie keeps build-on-right; outer joins untouched so the preserved side's row
  order is unchanged), filter-through-aggregate (key-only HAVING conjuncts rewritten
  into the group keys; constants, `AggRef` and dynamic calls stay above). In-worktree
  battery: TOTAL 267.4 → 250.2 ms, wins on `filter_str` 5.8 → 3.4, `distinct_lowcard`
  6.0 → 3.9, `join_probe` 6.7 → 5.4, windows ~10%; micro-benchmarks: `WHERE total >
  400 LIMIT 100` 6–7 ms → `<1` ms, `dim(4) JOIN sales(1M)` 26–28 → 6–7 ms. Gates on
  main after 45o/60: fmt, clippy, pinkdb 414/415 (1 ignored timing probe), CLI lake+s3
  and multiprocess green, duckdiff 88/88, standing battery TOTAL 248.7 ms (best of 5).
  Honest limits: outer joins cannot swap sides, INNER swaps change row order to
  probe-side order (one test expectation updated), projection stays conservative
  through Join/Distinct/Union/CTE, the estimator has no statistics.
- 2026-10-16 — Wave 58 (SQL completeness; worktree fan-out, integrated after 60/61):
  joins — multi-way `FROM` (left-deep), composite `ON` (multi-key hash), non-equi
  `ON` (`> < >= <= !=`, vectorized nested loop), `CROSS JOIN` (`CROSS JOIN ... ON`
  is a parse error like DuckDB), `FULL [OUTER] JOIN` (unmatched rows emitted from
  both sides, NULL-extended; NULL keys never match but are preserved); set ops —
  `INTERSECT [ALL]`, `EXCEPT [ALL]`, precedence pinned by probe (`INTERSECT` binds
  tighter; `UNION`/`EXCEPT` left-assoc; trailing ORDER BY/LIMIT/OFFSET apply to the
  whole set), pairwise common-type coercion; DML/DDL — `INSERT ... SELECT` (with
  column lists), `CREATE TABLE AS SELECT`, `IF NOT EXISTS`/`IF EXISTS` on
  CREATE/DROP TABLE and DATABASE, `OFFSET` (alone and with LIMIT), `ORDER BY ...
  NULLS FIRST/LAST` — the DuckDB default was pinned by probe: NULLS LAST for both
  ASC and DESC (the Postgres rule is different); aggregates — `COUNT(DISTINCT
  expr)` (others reject clearly); windows — the default frame with ORDER BY is now
  `RANGE UNBOUNDED PRECEDING AND CURRENT ROW` (peers share the value — the parity
  fix), RANGE frames with UNBOUNDED/CURRENT ROW and integer value offsets
  (direction-aware; DATE/DOUBLE/DESC covered); utilities — `EXPLAIN <select>`
  (stable indented tree, never executes), `SHOW TABLES`, `SHOW DATABASES`,
  `DESCRIBE` in DuckDB's exact shape. Durability: CTAS/INSERT…SELECT are logged as
  statements and replay; the touched-set walkers were audited for all new plan
  variants (`collect_tables_read`/`record_writes`/`writes_tables` cover them), so
  wave-45o optimistic validation stays honest. Integration: hand-resolved merge
  conflicts against 45o (WAL v4), 60 and 61 — join build-side choice generalized
  to multi-key slices, fused-aggregate single-key/DISTINCT guards merged,
  `scan_output` kept, `Union`→`SetOp` renames, window `frame_range` call site,
  `Expr::Function { distinct }`, the `Concat` symbol. Test infra: the multiprocess
  harness now bounds its waits (`f92c2a6`) after a rare 30+-minute silent stall
  surfaced here (engine scenario passes 5/5 solo and 27/27; the stall was
  `wait_for` with no deadline — a recurrence now fails with the collected CLI
  output). Gates on main: fmt, clippy (pinkdb + CLI lake,s3 all-targets), pinkdb
  424/424 (+ encryption 441/441 and 21/21 integration), CLI suites green
  (multiprocess 26 + 27), `bench/duckdiff.py` **155/155**, workspace all green,
  `mp_soak.py --ci` PASS (991 acked units, 18 kills, exactly-once across 24
  writers).
- 2026-10-16 — Integration close-out (45o → 60 → 61 → 58 → CI green). CI caught
  two real issues that local gates could not: (1) `fc4b8eb` shipped the pre-fix
  versions of `window.rs`/`plan.rs`/`peg_parser.rs` — the wave-58 merge fixes
  were made in the working tree but never re-staged before the full-index
  commit, so the pushed tree did not match the verified one; fixed in `0a8d111`
  (the commit now exactly matches the locally verified tree: `git diff HEAD --
  crates` is empty). (2) The duckdiff CI job downloaded DuckDB `latest` stable
  while the 155 probes are pinned against the 2.0 alpha: stable accepts
  `lag(x, 1.5)` (the alpha raises a Binder error on the non-BIGINT offset) and
  returns rows for `mod(x, 0)` (the alpha raises "Division by zero in
  expression mod(1, 0)") — CI now installs the alpha through the official
  installer, pins it into PATH, and fails loudly if the version is not v2
  (`493ea50`); `bench/duckdiff.py` prints its reference version in the header.
  A rare multiprocess-harness stall surfaced during these gates and is closed
  by bounded waits (`f92c2a6`; a recurrence now fails with the collected CLI
  output instead of hanging the suite). Fully green on 493ea50 (run
  38931704447, all nine jobs).
- 2026-10-16 — Post-merge benchmark audit (HEAD 3e71930, idle machine). Standing
  battery ~253 ms (61-tree 248.7; ≈ +1.8%). `mp_optimistic.py` txn-read reproduced
  **2.34×** vs serialized (baseline binary 1.01× — the 45o contention result holds
  on the merged tree). Durable: `Sync` inserts 42.2 s (fsync-bound ≈4.2 ms/insert),
  `Batch` inserts **231.4 ms** (identical to 45o — the commit-check fast path
  holds), selects 548/519 ms, checkpoints 59–60 ms. Back-to-back A/B against a
  61-tree build (same machine, best-of-7): `join_group` 15.90 → 18.14 (**+14%**),
  `filter_str` 4.50 → 5.37 (**+19%**), `join_probe` 5.83 → 6.22 (+7%); `join_self`,
  `join_mat`, windows and `distinct_lowcard` within noise. Function-level diffs
  (`fused_join_aggregate`, `MatchTable::build`, `walk_scan_projections`,
  `ScanProjections`) show no algorithmic change — flagged for the wave-55 perf
  round (profile the fused path's `MatchTable` build and the VARCHAR filter path;
  the int filter and projection pushdown are unaffected). Note: same-binary
  session-to-session variance on this machine reaches 9–15% on some queries, so
  the back-to-back A/B pairs — not absolute totals — are the signal.
- 2026-10-16 — Wave 55 (performance round; pulled forward after the post-merge
  audit). Investigation: none of the audit deltas were algorithmic — `filter_chunk`
  is byte-identical between trees, the projection walker and `ScanProjections` are
  unchanged, and the fused join→aggregate hot loop only gained two once-per-query
  guards; a materialized-path experiment proved fusion is the right choice
  (materialized is 2–5× slower: join_self 25.6 vs 9.3, join_probe 32.6 vs 6.2).
  Profiling (samply + manual RVA symbolization against `nm`) located the filter
  cost: the literal's per-chunk constant Vector — on VARCHAR a full-width `Arc`
  clone per element (~1M clones for a 1M-row filter) — plus `eval_comparison`,
  ~25% of `filter_str`. Shipped: a same-dtype column-vs-literal fast path in
  `eval_comparison` (BOOLEAN/INTEGER/BIGINT/DOUBLE/VARCHAR/DATE/TIMESTAMP, both
  literal sides, NULL-preserving, one `Arc<str>` scalar per chunk; mixed numeric
  widths keep the generic value-by-value path). A unit test proves
  fast ≡ general across the type × op × side × NULL matrix. Result: `filter_str`
  **5.39 → 1.60 ms** (3.4×) and the standing battery's best-ever total,
  **235.5 ms** (best of 5; previous best 248.7 — joins and windows at their best
  readings too). Gates: fmt, clippy `-D warnings`, pinkdb 425/425 (+1 ignored),
  CLI lake+s3 green (multiprocess 27/27), `bench/duckdiff.py` 155/155, full
  workspace green. Follow-up candidates (not scheduled): extend the fast path to
  mixed numeric widths (`filter_num` unchanged at 2.31 ms), LIKE constant
  folding, and a cool-machine re-audit of the join queries.
- 2026-10-17 — Fresh-day benchmark record (all harnesses; DuckDB still
  v2.0.0-alpha46057 — `lake_bench.py` prints its normalized "v2.0.0" — with
  duckdiff **155/155** against it; the final 2.0 is expected Oct 21). Standing
  battery 1M best-of-7: **TOTAL 234.0 ms** (best yet; 235.5 on Oct 16, 248.7
  pre-wave-55): filter_str 1.67, filter_num 2.30, group_id 46.8, distinct_id
  47.5, windows 13.5/13.6, joins 9.4/15.6/24.1/5.2, order_str 29.2.
  Multi-writer txn-read, 4 procs: **2.55×** (0.363→0.143 s; the serialized
  baseline binary 1.00×); honest no-win cases measured: txn-write 0.77×,
  autocommit 0.80×. Durable 10k inserts / 3 checkpoints: `Sync` inserts 39.4 s
  (the fsync floor), `Batch` inserts **225.0 ms** (best yet), selects 459–488
  ms, checkpoints 52–54 ms. Lake vs DuckLake: @1M stored **2.5 MB vs 12.5 MB**
  (metadata 643 B vs 3.8 MB), cold e2e open 0.009 vs 0.059 s, cold per-query
  0.14–0.19× (pink faster); @10M stored **25.1 MB vs 91.4 MB** (identical to
  the previous record — byte-stable), cold open 0.037 vs 0.059 s, cold
  per-query 0.63–0.93× (pink's eager Vortex open dominates its cold cost at
  10M, while DuckLake's lazy attach stays ~60 ms flat), in-session in-memory
  queries <1/32/21/22 ms (count/groupby/filter/orderby).
- 2026-10-17 — DuckLake feature-gap audit (probed live against the ducklake
  extension on DuckDB v2.0.0-alpha46057; 26 `ducklake_*` functions, settings
  and SQL surfaces exercised): confirmed absent from pink-lake — mutable tables
  (INSERT/UPDATE/DELETE; snapshots show `inlined_insert`, deletes via positional
  files, `ducklake_write_deletion_vectors` option), per-query time travel
  (`AT (VERSION => n)` and `AT (TIMESTAMP => ts)` both resolved live,
  `ducklake_default_version`), snapshot metadata (`snapshots()`: 0-based ids,
  snapshot_time, schema_version, change-summary map, author, commit_message;
  `ducklake_set_commit_message`), CDC macros
  (`table_insertions/deletions/changes(table, start, end)`), schema evolution
  (ALTER ADD COLUMN DEFAULT reads back on old rows), `PARTITIONED BY` file
  splitting, data inlining (`ducklake_default_data_inlining_row_limit=10`,
  `flush_inlined_data`), maintenance functions (`merge_adjacent_files` ran;
  `rewrite_data_files`, `expire_snapshots`, `cleanup_old_files`,
  `delete_orphaned_files`, `list_files`, `scan`, `table_info`), optimistic
  writer retry controls (`max_retry_count`, `retry_backoff`, `retry_wait_ms`),
  `add_data_files`, and info/introspection functions. Queued as waves 63–68;
  note that pink-lake's `open + mutate + save` already provides whole-store
  edit-then-publish semantics — the gap is incremental commits, delete files,
  per-query time travel, evolution, partitioning and maintenance. All six stay
  queued for the next worktree batch after the in-flight four land
  (rate-limit cap: max four concurrent agents).
- 2026-10-17 — Wave 62 (DECIMAL and INTERVAL; salvaged from the disk-full
  incident's worktree and finished by hand): `Value::Decimal(i128, scale)` +
  `DataType::Decimal{p,s}` and `Value::Interval{months, days, micros}` +
  `DataType::Interval` end to end — literals (`CAST`, `::`), canonical display
  (`format_decimal` trailing-zero rules, `format_interval`), arithmetic with
  DuckDB's promotion rules (`+ - *` keep scale/width with the 38-digit caps,
  `/` widens both sides to DOUBLE, `%` uses min integer digits), comparisons,
  `SUM`/`AVG`, INSERT/CSV coercion from text and numerics, date ± interval,
  month-end clamping, interval `*`/`/`, MIN/MAX — every rule pinned with
  probes. Storage: `.pink` **v5** (dtype tags 7/8; DECIMAL's precision/scale
  ride after the tag; PLAIN i128 and months/days/micros payloads; exact
  min/max stats; v1–v4 stay readable, older builds refuse v5 via the
  newer-magic path). Lake: lean `.pink` v1 lakes store both types (stats
  encoding extended); the **Vortex writer refuses them with a clear
  "not supported yet" error** — the Vortex mapping is a follow-up. Front ends:
  protocol **v6** value/dtype tags with round-trip tests; FFI additive
  `PINKDB_TYPE_DECIMAL/INTERVAL` cells marshaled as canonical text (+ header
  enumerators); wave-59's `value_literal` renderer gained exact
  `CAST('…' AS DECIMAL(38,s))` / `INTERVAL '…'` arms so bound DML replays.
  Integration notes: conflicts with wave 59 resolved in `protocol.rs` (docs
  merged → v6), `binder.rs` (placeholder helper ∪ decimal width/scale helpers)
  and `peg_parser.rs` (kept both `?` and `INTERVAL` alternatives); a
  `.pink.lock`/`.wal` test artifact was excluded from the patch and removed.
  Gates on main: fmt, clippy (core + CLI), pinkdb 438/438 (+prepared 22),
  encryption 455/455 (+21), server (protocol 22, server 25), FFI, CLI lake+s3
  (multiprocess 27/27), pink-lake default + vortex-format, `bench/duckdiff.py`
  **165/165**, workspace green. Follow-ups (not scheduled): DECIMAL/INTERVAL
  in Vortex lakes; SQL numeric literals auto-typing as DECIMAL; a
  prepared-parameter durable replay test for the new types.
- 2026-10-17 — Wave 59 (prepared statements): `?` placeholders in the parser;
  inference + binding in the new `prepared` module (`PreparedStatement::parse`,
  parameter types resolved against the catalog at bind time, `CAST(? AS t)` to
  pin a type); an engine API on `Connection` (`prepare` → `bind` →
  `execute_bound`/`execute_prepared`; binding uses the open transaction's
  snapshot when one is open, execution goes through the normal statement path
  so durable stores log the substituted SQL — replay-exact). Protocol **v5**:
  `Prepare` / `ExecutePrepared` requests + the `Prepared` response
  (connection-local handles, capped at 1024 per connection; bind/type errors
  and unknown handles are normal errors, the connection stays usable; the
  server's hook sees the substituted SQL). FFI: `pinkdb.h` + `pinkdb-ffi`
  gained prepare/bind/execute entry points with tests. Client:
  `Client::prepare` / `execute_prepared`, sharing the batch reader with
  `execute`. Tests: the engine suite (`tests/prepared.rs`), a server round-trip
  (binding, reuse, unknown handle, count/type errors, prepare-time parse
  errors), and extended protocol round-trips. Gates on main: fmt, clippy
  (pinkdb/server/ffi all-targets `-D warnings`), engine 431/431, server 25/25,
  FFI, CLI lake+s3 (multiprocess 27/27), duckdiff **155/155**, workspace green.
  The Python/Go binding wrappers are queued as **wave 69** (the FFI they wrap
  is in place). Note: finished directly after the worktree agent died in the
  Oct-17 disk-full incident (see next entry).
- 2026-10-17 — Disk-full incident and recovery: the four-worktree batch's
  private target dirs plus main's caches filled the volume (100%, 897 MiB
  free); builds in every worktree began failing and the failure cascades
  killed the agents with model-input-limit errors (57, 59 and 62 confirmed;
  41's agent survived the incident (actively working; its freed target just
  forces a rebuild). Recovery:
  every worktree's source was secured as a patch artifact
  (`wt-lake-encryption` 3,759 / `wt-pkfk` 2,843 / `wt-types` 2,993 /
  `wt-prepared` 3,698 lines), then all four private targets, main's
  incremental cache and the lake-bench temp dirs were removed — 29 GiB freed.
  Wave 59 was finished by hand from its partial state and landed same-day;
  57, 62 and 41 stay parked with their patches secured. Lesson: at most two
  concurrent worktrees with private targets on this machine; write patch
  artifacts at milestones; check free space before dispatching.
- 2026-10-17 — Wave 41 (primary/foreign keys): declarative `PRIMARY KEY` /
  `UNIQUE` / `NOT NULL` / `REFERENCES` in column and table form (`CONSTRAINT
  name` prefix; `RELY` / `NORELY` / `[NOT] ENFORCED` accepted and dropped; all
  keywords non-reserved, so `CREATE TABLE t (key INTEGER, unique INTEGER)`
  still parses). Catalog: `TableConstraints` on `DataTable`; DDL-time
  validation pinned against DuckDB alpha (referenced table must exist; the
  referenced column list must be exactly one of its PK/UNIQUE constraints,
  count and types checked, widening-only INTEGER→BIGINT→DOUBLE; a bare
  `REFERENCES u` uses u's PK; cross-database references resolve in the
  created table's database). Constraints are **metadata only** —
  INSERT/UPDATE never enforce them (dangling FKs, duplicate keys, NULLs in
  PK/NOT NULL columns all accepted; explicit tests) — and `DESCRIBE` reports
  `PRI`/`UNI` keys plus `NO` nullability for PK/NOT NULL. Optimizer:
  conservative FK→PK join elimination (top-down column liveness; fires only
  for INNER joins whose key pairs are exactly one declared FK on the left
  table, every FK column NOT NULL by declaration, and the parent *currently*
  declaring PK/UNIQUE on the referenced columns — a stale FK after DROP/
  recreate never fires; documented caveat: declarations are trusted, not
  verified, so a wrong declaration changes results). Persistence: the two v5
  proposals are reconciled into **one v5** — flags bit 0 gates the per-table
  constraint section (kind/name/column indexes; FK refs carry
  database+table+columns) and wave 62's DECIMAL/INTERVAL dtype tags 7/8 live
  in the schema block; a constraint-free, tag-free v5 body stays
  byte-identical to v4; corrupt sections are clean errors; WAL replay
  re-derives constraints from DDL text (checkpoint and replay-only paths
  tested). The golden pin now CRCs the body without the trailer (the old
  whole-file CRC was the data+trailer identity constant and could not notice
  drift). DuckDB α divergence (documented, intentional): α *enforces*
  constraints and rejects `RELY`/`NOT ENFORCED`, and does no FK→PK
  elimination — differential probes use conforming data only. Integration:
  the 17-file worktree patch applied `--3way` minus `website/` (12 files; the
  encryption.rs hunk was a no-op after wave 62's v5 asserts); conflicts
  resolved by hand in `persist.rs` (9 hunks — flags docs/constants merged,
  the `with_constraints` probe kept, reader `(v2, flags, v5)` tuple, body-CRC
  golden), `binder.rs` (import union), `duckdiff.py` (seed union: decimal +
  constraint tables), `lake_lite.rs` (v5 message). Website edits from the
  patch were **excluded** (main carries uncommitted site WIP on the same
  pages) — constraint docs for sql.html/persistence.html are a follow-up.
  Gates on main: fmt, clippy (pinkdb all-targets, CLI lake,s3, pink-lake),
  pinkdb **452/452** (+prepared 22; encryption 469 + 21), CLI lake+s3 **91**
  (multiprocess 27/27), pink-lake 29 + vortex 38, `bench/duckdiff.py`
  **171/171**, workspace green. Worktree `wt-pkfk` removed after landing.
- 2026-10-17 — Multiprocess harness determinism fix (`one_call_transaction_conflict_is_retried_by_the_cli`):
  the committer fed a fixed 500-insert burst before P2 spawned, so on the
  macOS CI runner the burst could finish before P2's transaction window even
  opened — the expected conflict never happened and the test hit the 120 s
  bound (wave-41 CI red on `accf8cb`, macOS only; every other job green). The
  committer is now fed continuously from a background thread
  (`Cli::feed_until_stopped`, 5 ms cadence) and stopped + killed immediately
  after the conflict line, so the retry sees a quiescent store and commits
  within the bounded budget. 6/6 local repeats in ~2.3 s each; suite **26/26**
  (default) and **27/27** (`lake,s3`), clippy clean.
- 2026-10-17 — Queue: waves 70–72 added (parquet, arrow, iceberg as core
  extensions at request; parquet leaves the Deferred list). Grounded on a live
  probe of the α reference: parquet is built-in there (`read_parquet` /
  `parquet_scan` + the metadata functions + `COPY … (FORMAT parquet)`; the
  physical mapping probed for DECIMAL/DATE/TIMESTAMP/INTERVAL); iceberg
  installs with a 19-function distinct surface (reads/introspection plus
  rewrite/rollback/property mutations/`iceberg_to_ducklake`); `arrow` has **no
  loadable extension** on this α (HTTP 404, candidates listed) — Arrow parity
  is pinned at the file (IPC/Feather) + C-Data-Interface level instead. Also:
  stale queue rows 40d–40g marked absorbed by waves 58–62; the wave-41 macOS
  flake fix verified green in CI (`8b401cd`).
- 2026-10-17 — Wave 64 (lake time travel & snapshot metadata): `FROM t AT
  (VERSION => n)` / `AT (TIMESTAMP => ts)` per query on attached lake tables
  (first source or any `JOIN` source, optional alias *before* the clause — the
  exact shape DuckLake accepts; literals only). The clause binds at bind time
  against the target version's own footer/statistics descriptor (the version's
  schema comes from itself); unknown versions, timestamps before the first
  snapshot and tables that did not exist at the target version are clean
  errors. Timestamp = newest snapshot with `time <= ts` (inclusive), pinned by
  live DuckLake probes; the reachable version set is capped at the attached
  version. Per-version stats fast paths: `COUNT`/`MIN`/`MAX` decode no data
  file, projected scans decode only referenced columns. Manifests gained
  optional per-version records (`author`, `commit_message`, computed change
  summary; a manifest without them is byte-identical to the pre-64 shape);
  `.lake snapshots` lists time/author/message/changes with the current version
  marked; `.lake save … author/message` + `Lake::set_commit_message` +
  `Lake::snapshots()` add the API. Probed divergences documented: DuckDB
  *silently ignores* `AT` on CTEs (pinkdb refuses instead — wrong-answer
  trap), DuckDB accepts constant expressions in the clause (pinkdb takes
  literals), `ducklake_default_version` probed to be a new-catalog storage
  selector, not a read default → skipped. Library surface: `AtSpec` +
  `AttachedLake::{table_at, load_at, load_projected_at}` (defaults refuse, so
  implementors compile unchanged). Gates on main: fmt, clippy (pinkdb ±
  encryption, pink-lake ± vortex-format, CLI lake,s3), pinkdb **458/458**
  (+prepared 22; encryption 475 + 21), CLI lake,s3 **93** (lake 24,
  multiprocess 27/27), pink-lake 21 + 10 (+1 ignored) and vortex lake **46**,
  `bench/duckdiff.py` **171/171**, workspace green. Integration: patch applied
  clean (`--3way`, 20 files, +2,507/−117); wave-63 overlaps flagged in
  `pink-lake/src/{lib,catalog,attach}.rs` + the lake test files — reconcile
  when 63 lands. Worktree `wt-lake-versions` removed after landing.
- 2026-10-17 — Wave 63 (incremental lake writes): catalog **v4** — every data
  file is listed with its exact row count, appends publish one new immutable
  file per table and never rewrite a published file (byte- and mtime-verified
  by tests), scans union the file list in manifest order. Positional **delete
  files** (`<uuid>.pinkdel`; `PINKDL1` + sorted unique u64 positions, bound
  per data file in the manifest) implement merge-on-read DELETE/UPDATE;
  corruption/truncation/misorder/out-of-bounds are clean errors. Per-commit
  **delta manifests** (`commits/<v>.json`) list each commit's files and row
  movement; `Lake::commits()` + `.lake info` show them. `Lake::commit` +
  `commit_from_tables` (exact row-level diff — bit-exact for DOUBLE NaN/−0.0,
  duplicate- and order-safe; chunk-addressed so a 1M-row diff stays fast) +
  `plan_tables_commit`; **writable attached lakes** (`.lake attach <dir>
  write`, `.lake commit`, guarded `.lake detach`; adoption recovery, new
  tables documented outside scope, schema changes refuse → wave 65).
  **Retry/backoff parity**: `CommitSettings { max_retry_count: 10,
  retry_backoff: 1.5, retry_wait_ms: 100 }` (probed DuckLake defaults), wait
  = `retry_wait_ms·backoff^(attempt−1)`, rebase on conflict, bounded, with
  `PINKDB_LAKE_*` overrides. WAL: attach checkpoints the materialized tables,
  commit flushes the WAL first; crash-window CLI tests show idempotent
  re-commits (a committed change publishes nothing, an uncommitted one
  exactly once). Demo (1M-row CSV): commit v3 wrote **7,567 B** (1 data + 1
  delete file) in **0.536 s** vs a 2.6 MB / 0.089 s full save — **0 bytes
  rewritten** (commit/base = 0.0029). Gates: pink-lake default 24 + 12 + 10
  (vortex: 44 + 12 + lake **46**), CLI lake,s3 **27** lake tests +
  multiprocess 27/27, pinkdb **458/458** (+prepared 22; encryption 475 + 21),
  duckdiff **171/171**, workspace green; fmt/clippy clean (pink-lake ±
  vortex-format, CLI lake,s3). Integration on top of the landed wave 64:
  **14 conflicts resolved by hand** — catalog docs union (one v4 shape + wave
  64's optional snapshot record), `.lake` help/usage unions, CLI test unions,
  both README sections; `build` split into the wave-64 storage-rooted
  `build_from` (needed by time-travel views) with `entry_layout` extracted to
  a storage-parameterized free function shared by both paths; commit publishes
  now record author/message + a diffed change summary like saves
  (`pending_commit` cleared on success). Known limits: deletion vectors and
  data inlining not implemented (inlining = wave 68); attached writes are
  content-sync (a concurrent same-table append makes the next diff refuse
  rather than silently drop rows); diffs are O(table rows). Worktree
  `wt-lake-incr` removed after landing.
- 2026-10-17 — Wave-63 CI follow-up (`e155568` was red in three jobs): the
  feature-less build's `lake_disabled` test still expected the pre-63 "lean:
  v1 `.pink` data files" rebuild hint, and the S3 Docker script pinned
  `format version:  3` — both updated to the merged messages ("lean: `.pink`
  data files", `format version:  4`; the S3 conflict-message asserts were
  checked and are unchanged). Local gates now include the **feature-less CLI
  suite** plus the whole CLI feature matrix — none / `lake-lite` /
  `lake-lite,s3` / `lake` / `lake,s3` — all green (the gap: every earlier run
  had a lake feature on, so `lake_disabled` never compiled). Second pass: the
  S3 Docker script's object-count expectations now include the per-version
  `commits/<v>.json` sidecars (`itest/` 4 → **5** with an explicit key check;
  `conc/` gains `(FINAL − 1)` commit files, positively asserted for every
  version). `bash -n` clean; the Docker job re-verifies on CI.
- 2026-10-17 — Wave 73 (docs catch-up & changelog wiring): the user's
  uncommitted site redesign landed first (`544cc3f` — soft data-zine, day/night
  editions, docs furniture, regenerated `public/`), then every stale page was
  brought current through waves 58–64: `sql.html` (prepared statements with the
  engine `prepare` → `bind` → `execute_bound` flow plus protocol/FFI notes;
  DECIMAL(38,s)/INTERVAL with the probed rules; declarative keys + FK→PK join
  elimination; full/multi-way/cross joins; RANGE frames; the wave-60
  scalar/window set; EXPLAIN/SHOW/DESCRIBE; set operations; a corrected
  "not supported" list), `persistence.html` (**v5** — constraint section + type
  tags, `PINKDB05`; WAL **v4** touched sets; optimistic commits +
  `PINKDB_TXN_RETRIES`), `lake.html` (incremental commits with the 1M-row
  7.6 KB demo + DuckLake retry knobs, writable-attach lifecycle,
  `AT (VERSION/TIMESTAMP)` time travel, `.lake snapshots` records, catalog
  `format_version` **4**, lean data files = `.pink` **v5**), `cli.html`
  (`.lake commit/snapshots/attach write/detach force`, author/message, the new
  env vars), `server.html` (protocol **v6**, `Prepare`/`ExecutePrepared`/
  `Prepared` frames, DECIMAL/INTERVAL value tags),
  `index.html`/`roadmap.html`/`api-rust.html`/`getting-started.html`/
  `guide-durable-multiprocess.html` refresh. New generated
  `docs/changelog.html` (`website/tools/make-changelog.py`, markdown subset)
  linked from the sidebar ("Project" group), the footer and the roadmap page.
  CI's docs job now runs the changelog generator + `build.py` and fails on a
  stale `git diff` — regenerate-and-commit is enforced; `website/README.md`
  documents the tooling.
- 2026-10-17 — Second disk-full incident (evening): the three lake-cluster
  agents (57, 65, 66) ran full gate batteries (including release builds) in
  private targets that grew to 6.0 / 6.8 / 7.5 GiB; with main's target at
  20 GiB the volume hit 98% (5.1 GiB free) and builds began failing — WT-65
  died with an explicit `database or disk is full` error, 66/57 hung at the
  session level (no processes, no writes). All three partials were secured
  before cleanup: `wt-lake-encryption/partial-stuck.diff` (3,802 lines / 12
  files), `wt-lake-schema/partial-stuck.diff` (5,272 / 29) and
  `wt-lake-partition/partial-stuck.diff` (4,796 / 16). Recovery: the three
  worktree targets (20.3 GiB) and ~7 GiB of old bench scratch under the
  session temp dir were removed — **30 GiB free**. Plan: cancel the stuck
  66/57 sessions, then re-dispatch continuations from the partials (max two
  at a time, 65 first). Lesson: every agent brief carries the disk guard
  (check `df` before/after heavy builds; prefer debug gates; keep release
  builds out of worktrees unless the wave needs duckdiff).
- 2026-10-17 — Wave 66 (lake partitioning & pruning): identity and
  `bucket(N)`-style partition specs recorded in catalog v4 entries
  (`TableEntry.partition_by` + per-file `partition` values — additive, an older
  v4 reader ignores them); commits split appended rows into one data file per
  distinct partition value set, and a full `.lake save` preserves the spec and
  re-splits; `Commit::set_partition`/`Lake::set_partition_spec` + `.lake
  partition` publish spec changes without rewriting old files. Every v4 data
  file now publishes a CRC-protected per-file **zone map** in the catalog —
  Vortex files newly, computed at write time and cross-checked against their
  footer at attach (tampered or misplaced values are refused, never silently
  pruned). The planner extracts exact conjuncts (comparisons in either operand
  order, `IS [NOT] NULL`, through `Limit`/`Offset` only) and skips whole files
  whose zone map or identity partition value proves no match, decoding
  survivors with the existing projection pushdown; `decode_stats()` is now
  `(loads, columns, files)` and `.lake attached` prints files. Pruning is
  version-correct across time travel, stays valid with positional deletes
  (deletes only shrink a file's value set) and the `COUNT`/`MIN`/`MAX`
  honesty fallback once deletes exist is preserved. DuckLake-pinned: spec DDL
  errors (unsupported transforms named, duplicates, missing columns), metadata
  shape (transform strings, varchar partition values, NULLs), `EXPLAIN`'s
  per-file read counts, one file per value per commit. Honest limits: bucket
  labels use pinkdb's own stable FNV-1a (no cross-engine numbers, no
  bucket-value pruning); `year/month/day/hour/epoch_*` transforms refused by
  name; pre-spec files are unprunable but decoded correctly. Gates on main:
  fmt, clippy (pink-lake ± vortex-format, pinkdb, CLI lake,s3), pink-lake
  28 + 12 + 10 + **12** (vortex: 48 + 12 + 46 + **13**), pinkdb **460/460**
  (+prepared 22; encryption 477 + 21), CLI lake,s3 **29** (multiprocess 27/27)
  + the full CLI feature matrix, duckdiff **171/171**, workspace green.
  Recovered from the Oct-17 disk-full stall (partial secured, continued in
  place). Worktree `wt-lake-partition` removed after landing.
- 2026-10-17 — Wave 65 (lake schema evolution): `ALTER TABLE` on
  writable-attached lake tables — `ADD COLUMN … DEFAULT`, `RENAME COLUMN`,
  `DROP COLUMN` (plus engine-wide ALTER: parser/binder/exec, constants
  materialized for later inserts, `NOT NULL`-needs-a-default on non-empty
  tables, `.pink` `FLAG_DEFAULTS` section, `DESCRIBE`'s `default` column, WAL
  touched-sets, durable replay) — published as metadata-only catalog **format
  5** schema records (`schema_version` epochs, per-column defaults and
  `added` markers, ordered rename history, per-file epochs, lake-wide
  counter); old files are never rewritten and project through renames /
  defaults / drops with positions intact (delete files stay valid). Time
  travel reads each version with that version's schema and names; legacy v4
  entries upgrade on first schema change; unknown future formats refuse
  cleanly. DuckLake-pinned (live): `snapshot.schema_version` bumps on
  create/ALTER only, ADD COLUMN DEFAULT is metadata-only (`initial_default`),
  DROP closes the column row, RENAME keeps identity, RENAME TABLE is
  metadata-only (refused here at commit level — documented, `.lake save`
  hint). Gates on the merged tree: pink-lake default 28 + 12 + 10 + 12
  (vortex 48 + 12 + 46 + 13; s3,vortex 77 + …), pinkdb **461/461** + alter 12,
  server/FFI green, CLI lake,s3 **31** (multiprocess 27/27) + the feature
  matrix, duckdiff **171/171**, fmt/clippy/workspace clean. **Integration on
  top of 66: 21 conflicts resolved by hand** — `AttachedEntry.schema`
  collision split (66's Vec stays `schema`, 65's record renamed `record`),
  the deletes-honesty regimes reconciled (per-file **zone maps stay ungated**
  for pruning; table-level merges keep 65's per-physical-column gating with
  default constants exact; legacy entries fall back on deletes), 66's
  filter-walk gained the `AlterTable` arm, `write_data_files` files are
  epoch-stamped via `with_schema_version` (partition values carried through),
  and every format-5 pin was swept (s3 tests, docker script, README/site).
  Worktree `wt-lake-schema` removed after landing.
- 2026-10-17 — Wave 57 (lake encryption at rest): a `Storage`-level
  **`CryptoStorage` decorator** wraps every session (keyed and keyless) with
  `PNKLAKE1` **AES-256-GCM** containers — 1 MiB frames, four AAD-bound domains
  (manifest / data file / commit / delete file, path-bound where applicable),
  a fresh nonce prefix per container, and one Argon2id derivation per session
  (each container's plaintext header carries the salt and parameters). Every
  lake object is covered — `catalog.json`, `snapshots/<v>.json`,
  `commits/<v>.json`, `.pink` and Vortex data files, `.pinkdel` delete files —
  so commits, deletes, time travel and retry/rebase inherit encryption with no
  per-call-site code. The **catalog == snapshot byte invariant survives**: the
  manifest is sealed once per version (`seal_manifest`) and the same ciphertext
  is published to both objects (including create-adoption and the degraded-S3
  reconciliation path). `PINKDB_KEY` UX is engine-consistent: keyed
  save/open/attach/commit; keyless reads of keyed lakes fail closed
  ("the lake is encrypted" / rebuild hint without the feature), wrong keys fail
  the authenticated key-check, and keyed sessions refuse plaintext lakes —
  never a silent rewrite. S3 ciphertext CAS is unchanged (conditional writes
  evaluate on sealed bytes; verified over the mock and the Docker harness).
  Stats-fast attach on encrypted lean lakes still decodes **zero** data files
  (stats ride inside the sealed manifest); encrypted Vortex attach reads whole
  objects for footers (stage 1; segment-level reads are the documented
  follow-up). CI gained the encrypted feature combos (pink-lake
  `s3,encryption` / `s3,vortex-format,encryption`, CLI `lake,s3,encryption`,
  and the s3-docker job now builds an encrypting binary). Gates on the merged
  tree: every pink-lake feature combo compiles clean (12/12), suites green —
  default 76, vortex 122 incl. encryption 96/…, s3,vortex 187, encryption 0
  failures in all configs — CLI `lake,s3,encryption` green (one stale
  `format version: 4` pin fixed to 5), fmt/clippy clean across the encrypted
  combos, duckdiff **171/171**. Integration on top of 65+66 was **one
  import-line conflict** — the decorator sits entirely at the `Storage` choke
  point, which the schema/partition restructures never touched. Worktree
  `wt-lake-encryption` removed after landing.
- 2026-10-17 — Wave 67 (lake maintenance, **the last wave before the dispatch
  freeze**): a `maintenance` module publishes compaction as one new version —
  `Lake::merge_adjacent_files` consolidates groups of **delete-free** live
  files that share **(partition values, schema epoch)** with ≥2 members and
  pass the byte-size filters (exactly DuckLake's `DuckLakeCompactionGroup`
  rules, pinned by live probes + source): merged files re-encode under the
  group's epoch, carry its partition values, and get exact rows/zone maps from
  the normal writer; sources are never touched (bytes **and mtimes** pinned by
  tests) and row-order stays a set-exact scan. `Lake::rewrite_data_files`
  materializes deletes above `delete_threshold` (default 0.95) and drops the
  delete files. `expire_snapshots` deletes old `snapshots/<v>.json` +
  `commits/<v>.json` by explicit versions / `older_than` / `retain_last`
  (never the current version; combined criteria refused with DuckLake's
  message; dry-run reports victims; expired versions fail time travel with the
  clean `no snapshot version` error). `cleanup_old_files` and
  `delete_orphaned_files` run the same unreferenced-object scan (pinkdb has no
  scheduled-deletion queue — documented): a default 2-day age gate plus
  explicit dry-run/confirm (a would-delete without either is refused),
  idempotent, `data/`-scoped, never touching `catalog`/`snapshots`/`commits`;
  encrypted lakes delete ciphertext path-wise. Introspection:
  `Lake::{list_files, table_info, options, settings}` + `.lake
  files/table_info/options/settings`. Storage gained `stat` (size +
  Last-Modified; S3 HEAD header plumbing) and `list_recursive` (local walk +
  ListObjectsV2). CLI: `.lake merge/rewrite/expire/cleanup/orphans/files/
  table_info/options/settings` with writable-attach guards. Gates on main:
  fmt, clippy (pink-lake ± vortex/encryption/s3, CLI ± encryption), pink-lake
  default **94**, vortex **151**, s3+vortex **207**, encryption combos green,
  CLI lake,s3 **32** + full feature matrix, workspace clean, duckdiff
  **171/171**. Clean apply, **zero conflicts**. Worktree `wt-lake-maint`
  removed after landing. **Dispatch freeze in effect: no new waves until the
  user says otherwise.**
- 2026-10-18 — User-landed batch (their session; commits `fdee077`,
  `bc2fd2d`, `a350efc`, `eba967e`, `1fd0531`, `a79cd41`, `2be5d3b`): a
  bug-review remediation close-out (server ETag validation; both FFI
  result-building paths guarded; a `.pink` VARCHAR chunk over the u32
  string-offset limit refused cleanly; attach re-checks its declared row count
  against the deduped layout), engine performance (COUNT(*) dead-work
  elimination + fast DISTINCT for int/date/timestamp keys), **wave 42** —
  single-column hash indexes: `CREATE INDEX [IF NOT EXISTS] … ON t(col)` /
  `DROP INDEX [IF EXISTS]`, per-database scoping, `Value → row indices`, and
  the optimizer's `Filter(col = literal) → IndexScan` rewrite (CLI/server/FFI
  result kinds included; **`.pink` persistence explicitly deferred** → queued
  as wave 79) — and **wave 43** — materialized views:
  `CREATE MATERIALIZED VIEW … AS SELECT`, `REFRESH [MATERIALIZED VIEW]` (full
  recompute), `DROP MATERIALIZED VIEW [IF EXISTS]`, stored as a queryable
  table plus its refresh query in the catalog, per-database, WAL-logged,
  persisted. Tracker synced (42/43 → done; 79 queued); the wave-42 dispatch
  agent was cancelled (redundant) with nothing written.
- 2026-10-18 — Wave 68 (inlined small writes + CDC; **closes the lake-parity
  block**): commits appending ≤ `data_inlining_row_limit` rows (default **10**,
  α-pinned; 0 disables; per-table persisted limits + `PINKDB_LAKE_INLINING_ROW_LIMIT`)
  store them **in the catalog** as versioned rows (`row_id`/`begin`/`end`,
  schema-mapped values via renames/defaults; additive fields — pre-68 manifests
  unchanged); scans read data files in manifest order then live inlined rows;
  stats fold exact inlined facts with the shared DOUBLE gate; deletes/updates
  of inlined rows are version-ends (no row-id pairing — documented).
  `flush_inlined_data` + `.lake flush` materialize rows into partition-split
  files and keep the history (`flushed: true`). CDC reads:
  `Lake::{table_insertions, table_deletions, table_changes}` + `.lake changes`
  derive rows from commit deltas + inlined records with
  `snapshot_id`/`rowid`/`change_type` (inclusive ranges α-pinned; a full-save
  version in range is refused by name; updates surface as delete+insert).
  `add_data_files` + `.lake add_data_files` register lake-relative files with
  exact-schema/row-count/statistics checks (encrypted lakes verify the
  container's path binding). Maintenance interplay handled (merge carries
  untouched inlined rows; `table_info` reports them). Also fixed two
  pre-existing base issues: the s3 error-taxonomy test now targets a
  data-file PUT (matching the bug-review no-retry semantics) and rustfmt
  drift in five `pinkdb` files (wave 42/43 leftovers) — fmt gate green again.
  Gates on main: fmt/clippy (all lake combos), pink-lake default **117**,
  vortex **174**, +encryption **195**, s3+vortex+encryption **258**, CLI
  matrix incl. `lake,s3` **103** (multiprocess 27/27), workspace clean,
  `bench/duckdiff.py` **171/171**. Clean apply; worktree removed.
- 2026-10-18 — Wave 80 (MV refresh policies): manual completion —
  `REFRESH [MATERIALIZED VIEW] name` (shorthand `REFRESH name`), `REFRESH
  MATERIALIZED VIEWS` (every view of the session database, all-or-nothing:
  all defining queries evaluate before any table is written), per-view
  `last_refreshed`/`refresh_count`/`last_error`, and `SHOW MATERIALIZED VIEWS`
  (schedule, stamps, staleness). Scheduled — `CREATE MATERIALIZED VIEW v
  REFRESH MANUAL | EVERY INTERVAL '…' | ON COMMIT AS …` (policy before `AS`,
  Postgres storage-option position), persisted in `.pink` v5 **flags bit 2**
  (`0x4`; bit 3 reserved for wave 79) with every MV statement now
  WAL-logged. Enforcement is **daemonless**: one statement-boundary hook in
  the embedded loop and every `Connection` (so `pinkdb-server` gets it free)
  plus commit-time dirty marking; `EVERY` is due once per interval since the
  last attempt, `ON COMMIT` fires the first boundary after a dirty commit,
  view chains reach a fixpoint in one pass, and neither fires inside a
  transaction. Automatic refreshes are explicit WAL records, one per boundary
  pass; replay suppresses the pass (`PinkDb::replaying`) and reproduces data +
  counters exactly (timestamps are wall-clock, deliberately not payload).
  Auto failures never wedge (data stays at the last success, `last_error`
  recorded, retry backed off), evaluation now happens **before** the backing
  table is touched, and dropping/recreating a base table heals the view.
  **Lineage fix found while doing this**: wave 43 shipped MV DDL *unlogged*
  and *unpersisted* — a durable reopen silently lost the definition; MV
  statements are now `writes_tables` → logged/replayed exactly (pinned).
  Gates on main: fmt clean (the agent's toolchain-skew note did not reproduce
  here), clippy all touched configurations, pinkdb **532** (495 lib + 14 alter
  + 22 prepared + 1 doc), pinkdb-server **57**, pinkdb-ffi **16**, pink-lake
  vortex green, CLI matrix incl. `lake,s3` **103** (multiprocess 27/27),
  workspace clean, `bench/duckdiff.py` **171/171**. Two conflicts resolved
  (rustfmt-style vs 80's new plan variants; took both). Worktree removed.
- 2026-10-18 — Wave 74 (lake naming sweep): CLI features `lake-lite` →
  **`pinklake`**, `lake` → **`vortexlake`**; pink-lake's `vortex-format` →
  **`vortexlake`** (default = the `.pink`-file lake). **`s3` decoupled to pure
  storage**: `s3 = ["pink-lake/s3"]` — implies neither build; `--features s3`
  alone compiles the client, exposes no `.lake` commands, and the new
  feature-hint names both builds plus the `,s3` combine rule; combinations are
  explicit (`pinklake,s3` / `vortexlake,s3`). Decoupling proven at the
  dependency-graph level (`cargo tree`), pinned by a new
  `s3_alone_does_not_provide_lake_commands` test and CI matrix cases. Full
  sweep, grep-verified zero survivors outside history: cfgs, messages, CLI
  help, CI workflow (actionlint clean), README, bench preflights, the website
  (+ regenerated `public/`), and both test files renamed
  (`lake_lite.rs` → `pinklake.rs`). Gates on main: fmt/workspace clean,
  clippy 0 across the touched sets, CLI {none, `pinklake,s3`, `vortexlake,s3`}
  and pink-lake {default, `vortexlake`} suites green, release
  `vortexlake,s3` + `bench/duckdiff.py` **171/171**. Note: measured release
  size 49.0 MB vs the docs' ≈47.3 MB (predates this wave; sizes are the prior
  measurements — re-measure when convenient). Worktree removed.
- 2026-10-18 — Wave 44 (stored procedures): the **SQL procedural language**
  ships complete — `CREATE [OR REPLACE] PROCEDURE p(a INTEGER, …) AS $$ … $$`
  (dollar-quoted bodies), `DROP PROCEDURE [IF EXISTS]`, `CALL p(args)`;
  bodies support typed `DECLARE`/`SET`, `IF/ELSIF/ELSE`, `WHILE`, nested
  blocks, `RETURN` (last result or a value), `RAISE`, recursion (bounded at
  32), and any ordinary SQL statement; transaction/store statements refused at
  `CREATE`. **Values, never text**: at each statement's bind a variable or
  parameter becomes a bound literal (`BoundExpr::Literal`) — injection-safe by
  construction; each body statement rebinds per call, so a body can create a
  table and then insert into it. **WAL/replay pinned**: the committed `CALL`
  is logged as one record and replay re-executes it — sound because the engine
  has no nondeterministic built-ins and opaque/attached/session-local reads
  are refused per statement; multi-writer touched sets come from the actual
  executed reads/writes (`PinkDb::proc_frame`, `wal::frames_carry_exact_touched_sets`
  extended). `CALL` is statement-atomic everywhere (the outermost call
  snapshots and restores without disturbing an open transaction). Catalog-
  resident per database, persisted via `.pink` v5 flags **bit 4**
  (`FLAG_PROCEDURES`; bit 3 still reserved for 79), protocol **v8**
  (`DoneKind::Called = 18`), CLI/server/FFI rendering incl. a C-extension
  refusal inside bodies. **JavaScript**: the `pinkdb-js` sandbox crate ships
  as the *contract* — host API, `DENIED_INTRINSICS`, conversions,
  `engine_available()` clean refusal at CREATE *and* CALL (inert JS bodies
  load from files); the engine binding (quickjs-ng via `rquickjs`) is wave
  **81**. Pre-existing bug fixed on the way: constant-folded NULLs lost their
  type (`WHERE 1 = NULL`); `fold_expr` now CAST-pins them. Gates on main:
  fmt/clippy 0 across all sets (incl. `js`), pinkdb **564** (527 + 14 alter +
  22 prepared + 1 doc; 545 with `encryption,js`), CLI `vortexlake,s3` **108** +
  matrix (none/`pinklake`/`vortexlake`/`pinklake,s3`/`s3`/`js`), server/FFI
  green, pink-lake green, workspace clean, `bench/duckdiff.py` **171/171**.
  Sizes: default CLI 3 130 560 → **3 298 480 B** (+164 KiB), server +148 KiB.
  Wasm check not runnable locally (toolchain shim) — CI covers it. Worktree
  removed.
- 2026-10-17 — Post-freeze benchmark session (final tree `b3154e7`, Apple
  Silicon, best-of-N, DuckDB α46057 reference): standing battery **250.8 ms**
  (per-query: count 0.03, filter_num 2.25, filter_str 1.60, proj_func 18.2,
  group_region 3.39, group_id 50.3, order_num_topn 2.64, order_str 29.8,
  distinct_lowcard 3.9, distinct_id 49.6, window_row_number 15.7,
  window_count_star 16.0, join_self 9.0, join_group 15.5, join_mat 26.0,
  join_probe 6.8; vs the fresh-day 234.0 ms — variance/thermal, same codegen
  class). Differential: **duckdiff 175/175** including the 1M big battery;
  DuckDB on the same corpus: COUNT 2 ms, GROUP BY 16 ms (pinkdb 2.2 / 3.4 ms
  warm). Lake vs DuckLake: 1M — **2.5 MB vs 12.5 MB** stored, ingest
  0.114 s vs 0.137 s, cold e2e 0.009 vs 0.058 s, cold per-query **0.11–0.20×**;
  10M — **25.1 MB vs 91.4 MB**, ingest 0.63 vs 0.31 s, cold open 0.032 s,
  cold per-query **0.64–0.96×** (count 38 vs 60, groupby 73 vs 83, filter 62
  vs 74, orderby 75 vs 78 ms). Attach fast paths (1M, cold process): COUNT
  4.7 ms, filter 9.0, orderby 11.2, groupby 12.6; statistics probe
  COUNT+MIN+MAX = **0 decodes**. Multi-writer (4 processes, txn-read):
  serial 0.344 s vs concurrent 0.143 s → **2.41×**, verify ok (previous
  record 2.55×).
- 2026-10-17 — **Bug-review remediation** (branch `hardening-review-fixes`;
  source [docs/bug-review-2026-10-17-1827.md](docs/bug-review-2026-10-17-1827.md),
  status [docs/bug-review-remediation-2026-10-17.md](docs/bug-review-remediation-2026-10-17.md)):
  all **12 P1** findings fixed with regression tests verified to fail on the
  pre-fix code — Window filter pushdown, mixed-operand JOIN keys, `ALTER …
  DEFAULT` guards, JOIN/UNION plan depth, RANGE-frame NULL keys, plaintext WAL
  mid-file CRC, atomic/guarded `save`, legacy-head lake commits, delete-file
  count overflow, private staging copies, server `ExecutePrepared`/`Rows` caps
  — plus the P2 engine/lake/S3/CLI/server/bindings edges (concat operand,
  alias shadowing, set-op NULL, checked overflow, INTERVAL hashing, lazy
  CASE/COALESCE, WAL fsync/LSN/decoders, conflict set, container versions,
  schema-counter monotonicity, conditional retry, SigV4, CLI guards, bindings
  DECIMAL/INTERVAL/`ALTERED_TABLE`, client caps/timeouts, FFI unwind, docs).
  No deferred items: a `.pink` VARCHAR chunk over the `u32` string-offset
  limit is refused at serialization, and attach recomputes its row count from
  the deduped delete layout. Gates: workspace test/clippy green, duckdiff
  171/171 (175/175 big), feature matrices (encryption, lake s3/vortex, CLI
  lake,s3/client/repo, Python 37, Go) green.

- 2026-10-18 — Wave 82 (Rust toolchain upgrade): all seven workspace crates now
  declare **MSRV 1.97** and **edition 2024** (the local toolchain is 1.97.1, so
  the whole gate matrix doubles as the floor proof; the highest dependency
  floor in the resolved graph is 1.88). CI's `MSRV (1.89 core)` job is now
  **`MSRV (1.97)`** running `cargo check --workspace --all-targets`; all other
  jobs already float on stable. Edition fallout fixed by hand: `gen` reserved
  keyword → `generation` (93 sites, WAL internals), `#[unsafe(no_mangle)]`
  ×20 + `unsafe extern` (ffi/ext-loader), test-only `std::env::set_var` unsafe
  blocks (13 sites, 2 files), macro fragment `expr` → `expr_2021`, the
  tail-expression drop-order lint reviewed at all 8 sites (every temporary
  provably inert), and rustfmt style-2024 churn. Ergonomics: ~60 nested `if`s
  collapsed to **let-chains** via `clippy --fix` + 3 hand-written wins. Gates:
  fmt/clippy 10/10 feature sets clean, pinkdb 564 (603 with `encryption,js`),
  pinkdb-js 3, server+ffi 73, ext-loader 43, CLI matrix 66–108 across configs,
  pink-lake 117, **wasm32 build works** (via rustup shims), workspace check
  clean, `bench/duckdiff.py` **171/171**; 1895 test passes total. No dependency
  or format-surface changes; `Cargo.lock` byte-identical. Worktree removed.
- 2026-10-18 — Benchmark re-run (edition-2024 tree + the user's perf commit
  `bc2fd2d`): standing battery **106.8 ms** (Oct 17: 250.8) — `proj_func`
  18.2 → 0.035, `order_str` 29.8 → 0.036, `window_row_number`/`count_star`
  15.7/16.0 → 0.035, `distinct_id` 49.6 → 16.3, `group_id` 50.3 → 17.7; filter,
  group_region and the join paths unchanged (dead-work elimination + fast
  DISTINCT, not edition noise). vs DuckDB α on the same corpus: import 17.3 vs
  73 ms, COUNT 0.03 vs 3, filter 2.5 vs 2, GROUP BY 3.5 vs 22, top-n 3.7 vs 1,
  self-join 9.1 vs 22, materialized join 26.3 vs 22. Measured at load ~2.2
  (waves 46/47 compiling); two runs agreed (105.3/106.8). Lake/attach/multi-
  writer figures unchanged since Oct 17/18.

- 2026-10-18 — Wave 47 (async I/O boundary): the async slice chosen is an
  **async server variant** (`pinkdb-server/async-server`): a tokio listener
  runs one task per connection for frame I/O and idle timeouts and executes
  every engine call on the blocking pool (`spawn_blocking`, same 16 MiB
  stacks); the wire protocol and result-streaming codecs are shared through a
  new `FrameSink` trait, and a parity harness proves the frames are
  **byte-identical** to the blocking server (13-request script incl. errors,
  prepared statements, duplicate Hello). `--transport sync|async` on the
  binary (clean exit-2 refusal without the feature); the async runtime is
  feature-gated and the default dependency graph stays tokio-free (CI guard).
  The carve-out is documented in `lib.rs` + `async_server.rs`: async lives
  only at the server's I/O boundary; the executor, planner, storage and WAL
  stay synchronous — no `async fn` engine API, no async storage bridging.
  Tests (16): all value types + NULLs, batches, sessions/`USE`, Ping, prepared
  handles, handshake rules, malformed frames, connection cap, pipelined
  concurrency, idle timeout, panic isolation, oversized values, EMFILE
  survival, graceful shutdown, byte-parity. Gates: fmt/docs/workspace clean,
  clippy (server ± feature, CLI client), server 57 default / **74** with the
  feature, pinkdb 564, duckdiff **171/171**. Sizes: server 2,980,912 B default
  (+16 B), 3,374,352 B with `async-server`. **A/B benchmark** (in-process
  example `server_bench`, same `Client`, `SELECT COUNT(*)` round trips):
  1 client 14.7k vs **20.2k** QPS; 8 clients 41.8k vs **64.6k** (1.55×);
  32 clients 56.0k vs **84.8k** (1.51×, p95 0.69 → 0.50 ms). The async
  transport wins throughput under concurrency and the tail latency, with
  equal per-request latency at low load. Overlap flagged with wave 46
  (TLS edits the same server files) — reconcile at 46's integration. Worktree
  removed.
- 2026-10-18 — Wave 46 (server TLS): opt-in rustls transport — `Server::bind_tls`/
  `bind_tls_with_max_connections` (same framed protocol v8), binary flags
  `--tls-cert/--tls-key`, blocking-client `connect_tls(addr, server_name,
  trust)` and CLI `--tls` / `--tls-ca-cert` / `--tls-insecure` behind a `tls`
  feature (`= ["client", "pinkdb-server/tls"]`). Certificates load and validate
  **before binding** (missing/non-PEM/empty/mismatch each refuse startup with
  the file and reason); the handshake runs on the connection's own thread under
  the socket idle timeout, so failures and stalls never touch the accept loop;
  the connection cap applies (over-cap TLS connections are dropped pre-
  handshake); plaintext↔TLS mismatches fail cleanly. **No authentication or
  authorization was added** — TLS encrypts the transport and lets a client
  verify *which* server it reached; the docs say so loudly. Trust anchors are
  explicit PEM (system roots are the recorded next increment). Tests (22):
  round-trip + prepared + multi-frame results over TLS, wrong anchor, expired
  and not-yet-valid certs, both mismatch directions, 16-connection junk/stall
  isolation, cap/idle under TLS, 8 startup refusals, CLI flag matrix, disabled-
  build refusals — certificates are rcgen-generated per test (nothing checked
  in). Gates on main: fmt/clippy (server ± tls ± async-server, CLI client ±
  tls), server 59 default / 70 with `tls` / async suites green, CLI 68 client /
  73 client,tls, pinkdb 564, workspace clean, duckdiff **171/171**. Sizes:
  server 2,979,952 B default (unchanged graph), 4,217,408 B with `tls`.
  Integration on top of wave 47: **6 hand-resolved conflicts** — the TLS bind
  moved inside the async transport's `Sync` arm, `announce` gained a transport
  label (TLS-aware security warning), feature/dep tables and the lock unified.
  Worktree removed.
- 2026-10-18 — Wave 49 (extensions on wasm; worktree `wt-wasm`): the distinct
  ABI/loading model is **linked registration** — extensions register through a
  safe-Rust API (`pinkdb-ext-loader::linked`: `LinkedExtension` metadata,
  `LinkedScalar` callbacks with `Send + Sync` and panics caught, one-shot
  process-global registry) and feed the *same* engine registry as native ABI v1
  (`ExtFunction` gained a `Linked` variant; binder/executor/optimizer/WAL prefix
  handle both on every target, and the wasm "extension unsupported" stubs are
  gone). Resolution: on wasm, `LOAD EXTENSION '<name>'` looks the name up;
  natively `dlopen` stays first and the registry is consulted only when nothing
  is loadable at that exact string *and* the name is registered — which is what
  makes replayed `LOAD EXTENSION '<name>'` WAL records resolve. The
  **JS/wasm-bindgen contract** (`pinkdb.registerExtension`, SQL↔JS value table
  incl. DATE/TIMESTAMP reps, sandbox/determinism rules) is pinned as docs
  (`docs/wasm-extensions.md`) with no wasm-bindgen dependency; a side-module
  runtime stays a recorded follow-up. Simple scalar extensions cannot add
  aggregates/table functions yet (recorded). Tests: 8 real-engine linked tests
  (incl. durable WAL round-trip with checkpoint), registry/WAL failure paths,
  atomic-registration conflicts; ext-loader 43 → **59**, pinkdb 564 → **565**,
  duckdiff **171/171**, wasm build green (1.97.1 & stable), fmt/clippy clean.
  Practical note: local wasm builds must pin the toolchain rustc (`RUSTC=$(rustup
  which --toolchain 1.97.1 rustc)`), because PATH resolves Homebrew's rustc,
  whose sysroot lacks wasm std. Integration on main: patch applied cleanly.
  Worktree removed.

- 2026-10-18 — Wave 48 (RBAC; worktree `wt-rbac`): users/roles/grants are
  **database state** — `rbac.rs` keeps them in the engine catalog (`.pink` v5
  flag bit 5, canonical sorted sections, byte-identical when absent, refused by
  older builds through the shared unknown-flags check) and every RBAC statement
  WAL-logs; `CREATE/ALTER USER` is rewritten to `PASSWORD_HASH '<phc>'` so the
  plaintext never reaches the WAL (test scans `.pink` + WAL bytes). Argon2id
  reuses `crypto.rs`'s OWASP parameters (19 MiB / t=2 / p=1), hashed once per
  statement via an AST memo so the executing plan and the WAL share the
  derivation; `getrandom` is a native-only salt source (wasm refuses plaintext
  `PASSWORD`, `PASSWORD_HASH` still verifies). Enforcement is fail-closed per
  statement for authenticated sessions — grants are keyed by object identity
  (survive renames; cascade on drops incl. in-transaction merges), prepared
  statements are covered, and `CALL` / `LOAD EXTENSION` / `CHECKPOINT` / database
  and user/role DDL require `ADMIN` (procedure bodies are not privilege-checked,
  hence admin-only `CALL` — a documented refusal, not a silent pass). Empty
  catalog ⇒ trusted connections with a per-startup warning; once any user exists,
  statement requests require login (evaluated per statement, so `CREATE USER`
  takes effect immediately for that connection); the embedded API (Rust/FFI/wasm)
  stays trusted by design. Server: protocol **v9** (`Login`/`LoggedIn`, DoneKinds
  19–25) shared by the blocking and async transports; CLI `--user`/`--password`
  with `PINKDB_PASSWORD` fallback. Tests: pinkdb 564 → **597**, server 59 → **66**
  (**94** with `async-server,tls`), CLI 68 → **72** (**77** tls), duckdiff
  **171/171**, wasm build green, fmt/clippy clean. Release sizes: CLI +242 KB
  (+7.3 %), server +262 KB (+8.8 %); zero new crates (`argon2` was already in the
  lock; made unconditional). Integration on main: one hand-resolved conflict in
  `crates/pinkdb/Cargo.toml` (wave 49's all-targets loader dep merged with wave
  48's native-only `getrandom` section). Worktree removed.

- 2026-10-19 — Wave 50 (perf tuning; worktree `wt-perf50`): profile-first pass.
  In-situ instrumentation split the 16.7 ms `group_id` into scan+insert 12.9 ms
  + emit 3.8 ms; `sample` attributed the scan to rehash growth (22 %), per-row
  `Value` pushes (20 %) and vector `get` (12 %). The integer-DISTINCT fast path
  now reads typed payload slices per chunk, appends into a raw buffer flushed as
  `CHUNK_SIZE` vectors (`Vector::from_raw`, one NULL slot), and jumps the set to
  input size in one rehash only when high cardinality proves out (cap 1<<20 —
  low-cardinality DISTINCT over huge tables never allocates a rows-sized set).
  `group_id` **15.76 → 4.37 ms**, `distinct_id` **15.63 → 4.63 ms**. A `sample`
  on join probes (hash+probe ≈ 73 % of leaf time) motivated word-wise FNV
  byte-slice mixing (temp micro-bench: 6.4 → 2.7 ns per 13-byte key), after
  auditing every FastMap/FastSet use for order-independent lookup;
  `distinct_lowcard` 3.84 → 3.07, `join_probe` 5.30 → 4.60. Battery **101.5 →
  75.7 ms (−25 %)** (official runs 103.6/102.8 → 77.4/78.4; main re-run
  79.4–79.8). Dropped, with evidence: join-cell typed-push micro-opt (real query
  already ≈2.6 ns/cell), fused join (measured 9.3 ms vs 26.2 materialized —
  needs a per-pair predicate evaluator: feature-sized), unconditional set
  reserve. DuckDB α46057 re-measured on the same corpus: total 85 ms; remaining
  gaps are join_mat (25.0 vs 20), join_group (14.7 vs 12), join_probe (4.6 vs 3),
  order_num_topn (2.87 vs 2). The general 1M-group aggregate (180 ms) is
  recorded as the wave-40 SoA-accumulator breadcrumb. Gates: pinkdb **597**,
  duckdiff **171/171**, `--big` **175/175**, fmt/clippy clean, wasm green.
  Worktree removed.

- 2026-10-19 — Wave 51 (Rust extension SDK; worktree `wt-ext-sdk`): `ext-sdk/rust`
  (`pinkdb-ext-sdk`, workspace member) declares an extension once —
  `pinkdb_extension! { name, functions = [...], volatile = [...] }` — and
  generates the ABI v1 exports plus a `&'static [ScalarSpec]` **both** loading
  models consume (native cdylib and the wave-49 linked/wasm registry). Authors
  write safe Rust with typed args/returns (`Option<T>` NULLs, `Result` errors);
  all ABI glue lives in the SDK, and the specs must be `static` because the
  native `user_data` points into them (a temporary-`Vec` draft segfaulted —
  caught by the native round-trip test). The **ABI v1 volatile flag** is
  additive: `PINKDB_EXT_FLAG_VOLATILE`, an appended `register_scalar_flags`
  entry (`register_scalar` ≡ flags 0, so pre-flag descriptors/binaries are
  byte-identically compatible — proved by a legacy-view fixture), and unknown
  bits are rejected (silently dropping one is exactly how a volatile function
  would sneak past replay). The engine refuses a volatile call in any statement
  class `plan::writes_tables` logs — including on in-memory stores so tests
  mirror durable behavior — with an explicit error; reads stay callable,
  prepared statements decide at bind, and the refusal leaves no WAL bytes. The
  audit also found a real gap: procedure-body **expressions** bind outside
  statements, so they now refuse volatile through `bind_constant_expr_tracked`
  (deterministic expressions unchanged). Docs: Rust/C/Go SDK READMEs, root
  README, the wasm JS contract (`volatile: true`), CI `sdk-smokes` runs the Rust
  suite (15/15). Gates: ext-loader 59 → **72**, ext-sdk **19**, pinkdb **597**,
  workspace **957**, duckdiff **171/171**, fmt/clippy clean, wasm green (core +
  SDK linked path), C/Go smokes green. Size: CLI +17,184 B (+0.035 %), no new
  external deps. Integration on main: clean apply (40 files). Worktree removed.

- 2026-10-19 — Wave 52 (json core extension; worktree `wt-json`): `extensions/json`
  (`pinkdb-json`) implements the six functions over JSON text in VARCHAR with a
  hand-rolled parser/path engine/serializer — **zero new deps**, and cheaper
  than linking `serde_json` for what the pinned semantics need (a toy binary
  grew +50,576 B just from serde_json, which still couldn't model duplicate
  keys or DuckDB's number rendering). The differential drove the details:
  trailing commas, `NaN`/`inf`/`1e400`, duplicate-key first-match with order
  preservation, canonical number rendering, `\u00XX` escapes, lone surrogates
  rejected, compact re-serialization — pinned against the local DuckDB α46057
  with a 809-case scratch fuzz (0 mismatches) plus 25 committed `duckdiff`
  probes (**171 → 196**). The loader gained additive **optional/variadic
  arity** (`new_optional`/`new_variadic`, min/max args, per-position types;
  `dlopen` functions stay fixed-arity) because the name-keyed registry cannot
  express overloads. Activation: `pinkdb_json::ensure_registered()` +
  `load_linked_extension("json")` for embedders; the CLI's new **default
  `json` feature** auto-publishes and loads it so `json_valid('{}')` works out
  of the box (CI's duckdiff artifact builds with defaults), with
  `--no-default-features` for the lean binary; durable replay resolves calls
  through the registry (`LOAD EXTENSION 'json'` prefix). Gates: pinkdb-json
  **32**, ext-loader 59 → **60**, workspace **958**, pinkdb **597**, duckdiff
  **196/196**, fmt/clippy clean, wasm green (crate + core). Sizes: CLI
  **+75,904 B** default (+74.1 KiB), +75,584 B with `vortexlake,s3`. Honest
  limits recorded: no JSON type/operators/`->>`, VARCHAR-only args (explicit
  CAST; builders embed nested calls as strings), wildcards `[*]`/`$..a`
  refused, depth cap 1000, parse-per-row. This wave predates the wave-51 SDK
  (dispatched before it landed) and its arity needs exceed the SDK's current
  fixed-arity macros — **follow-up recorded: extend the SDK with
  optional/variadic arity, then port json** (as-is it uses the raw linked
  API). Integration on main: one conflict (workspace members union). Worktree
  removed.

- 2026-10-19 — Wave 53 (string core extension; worktree `wt-string`): the first
  pack authored with the wave-51 SDK (`pinkdb_extension!`), 10 deterministic
  functions. DuckDB pinning found two corrections to the brief: DuckDB has no
  `regexp_match` (Catalog Error — verified), so it ships as a loudly documented
  pinkdb pin (first-match VARCHAR, `''` when none) **plus** DuckDB's real
  `regexp_matches` (BOOLEAN); and `trim`/`ltrim`/`rtrim`/`replace` are engine
  built-ins that extensions cannot shadow (a test constructs the registration
  and pins the refusal), so they are deliberately omitted. Regexp option
  surface `i/c/l/m/n/p/s` matches DuckDB's left-to-right precedence, `g` is
  refused exactly like DuckDB; the `regex` crate (already in the lock; RE2-style
  linear time) differs only in Unicode-aware `\d`/`\w`/`\s` and message text.
  One minimal grammar relaxation lets unquoted `left(`/`right(` parse —
  `LEFT/RIGHT` are join keywords — keyed on a following `(`, so `LEFT OUTER
  JOIN` and bare keyword uses are unchanged (parser test pins it). Gates:
  ext-string **19**, pinkdb **598**, workspace **1010**, duckdiff **223/223**
  (+27 probes), smoke **15/15**, fmt/clippy clean, wasm green. CLI size **+0 B**
  (runtime `LOAD EXTENSION '<path>'`; dylib 1.70 MB unstripped / 1.43 MB
  stripped). Also landed here: the wave-50 FNV loop rewritten with
  `as_chunks::<8>` (CI's stable clippy `chunks_exact_to_as_chunks` — local 1.97
  clippy predates it; **both toolchains now gate clippy**) and the server
  `--help` duplicate `--allow-load-extension` line removed (46↔47 merge
  leftover). Integration on main: one conflict (workspace members union).
  Worktree removed.

- 2026-10-19 — Wave 56 (datetime core extension; worktree `wt-datetime`): ten
  DuckDB-pinned functions over naive DATE/TIMESTAMP, authored with the SDK. The
  headline ABI change is **additive INTERVAL** (tag 9): `age` needs a real
  interval result and the ABI could not express one, so months/days ride the
  bits of `double` (lossless, zero-allocation, documented) and µs in `int64`;
  the C dlopen path round-trips it in `extensions.rs` and `header_sync` keeps
  all four vendored headers byte-identical. Semantics were pinned live: α's
  `epoch` is fractional **DOUBLE** (the brief's "BIGINT seconds" is the 1.x
  shape — divergence documented), `epoch_ms` rounds ties away from zero,
  `week` is ISO-8601, `age` decomposes month→days with the start month's
  length and swap-negates, `make_date` errors on invalid fields, and the
  strftime/strptime set is α's minus deliberate refusals (`%z %Z %n %N %g`; plus
  `%U %W %V %G` for strptime). `datetime` joins `json` in the CLI's default
  features (auto-loaded; `--no-default-features` stays lean). Gates: datetime
  **35**, ext-loader 73 → **74**, pinkdb 598 → **599**, workspace **1047**,
  duckdiff 223 → **248** (+25 probes), fmt/clippy on both toolchains, wasm
  green, C/Go/Rust smokes green. Sizes (exact): **+40,304 B** on the default
  CLI (3,637,696 → 3,678,000), **+56,832 B** with vortexlake,s3 (49,533,392 →
  49,590,224). Integration on main: two conflicts resolved (workspace members
  union; the README core-extensions paragraph merged with wave 53's wording).
  Worktree removed.

- 2026-10-19 — Wave 69 (prepared statements in the Python and Go bindings;
  worktree `wt-bindings`): both bindings gained idiomatic wrappers over the
  wave-59 FFI prepared flow with **no ABI/engine changes** — Python's
  `db.prepare(sql)` returns a `Statement` with `execute(*params)` (one `Result`
  per execution; the FFI runs exactly one statement), metadata, idempotent
  `close()` plus context-manager/`__del__` cleanup; Go's `db.Prepare` returns a
  `*Stmt` with `Execute(args ...any)`, `ErrStmtClosed`, explicit `Close` (no
  finalizer, matching `database/sql`) and goroutine-safe reuse. Parameters map
  by value: INTEGER when an int fits i32 else BIGINT (> int64 refused — no
  HUGEINT in ABI v1), `None`/`nil` → NULL, strings copied into C memory so
  embedded NULs survive. The wave also found and fixed a bindings lag: both
  result-kind maps now know wave-48's kinds 17–23 (RBAC results previously read
  as "unknown kind"). Gates: Python **56/56** (was 37), Go **38/38** with
  `-race` (was 25; includes 8-goroutine reuse of one `Stmt` and a
  10k-execution bounded-RSS pin), `pinkdb-ffi` **16/16**, fmt/clippy clean on
  both toolchains. Honest limits: no BLOB/DECIMAL/INTERVAL params (documented
  workaround: canonical text + `CAST(? AS …)`), Go DATE params use
  `pinkdb.Date`, prepared binds resolve the handle's current snapshot (engine
  behavior, covered by a test). Integration on main: clean apply; the bindings
  suites need a freshly built `libpinkdb_ffi` first (noted for the checklist).
  Worktree removed.

- 2026-10-19 — Wave 54 (math core extension; worktree `wt-math`): the last pack,
  authored with the SDK. Interpretation pinned up front: the listed
  `sqrt`/`pow`/`floor`/`ceil`/`sign`/`greatest`/`least` are already engine
  built-ins that extensions cannot shadow, so the extension ships the
  complement (`exp/ln/log/log2/log10`, trig + inverses, `cot`, `degrees`/
  `radians`, `pi`, volatile `random()`) and the probes/tests pin the built-ins
  too. Two SDK/engine design points landed: **ranged arity** —
  `optional(min)` with a three-state `Optional<T>` (`Absent` ≠ `NULL`) and
  `variadic(min)` over a `Vec<T>` tail — is **linked-only by construction**
  (ABI v1 native descriptors cannot express ranges; the SDK refuses a cdylib
  load with code 5 rather than silently registering the maximum arity), and
  **numeric widening**: a DOUBLE-declared dynamic argument accepts
  INTEGER/BIGINT/DECIMAL through an inserted cast (DuckDB's implicit numeric
  coercion; VARCHAR stays strict by design). `log(b, x)` is composed as
  DuckDB's `log10(x)/log10(b)` (1-ulp-sensitive, probed). `random()` is the
  first live volatile function: reads and view bodies work, every WAL-logged
  statement is refused, refused statements leave no rows/WAL bytes, and
  deterministic math replays through the registry prefix. `math` joins
  json+datetime in the CLI defaults. Gates: math **21**, ext-sdk 19 → **28**,
  cli 67 → **70**, pinkdb **599**, workspace **1080**, duckdiff 248 → **272**
  (+24), fmt/clippy both toolchains, wasm green, C/Rust smokes green.
  Integration on main: five conflicts (members union, CLI features/deps
  merged to `default = ["json", "datetime", "math"]`, the CLI's extension
  loader refactored into a shared `load_builtin_extension` helper, SDK
  re-export union, both probe blocks kept) plus a missing-brace fix in the
  CLI help merge; the flagged `publish-extensions.yml` discovery bug
  (any `extensions/*/` without a built library made the manual publish run
  fail) is fixed by skipping artifact-less dirs. The final four-pack size
  pass re-measured all six CLI builds (default **3,719,664 B**). Worktree
  removed.

- 2026-10-19 — Wave 71 (arrow file-format extension; worktree `wt-arrow`):
  the scalar ABI can't express scans/COPY, so the engine gained a second,
  deliberately tiny extension surface — `pinkdb::filefmt::FileFormat` (trait
  + per-store registry seeded from a process-global registration at host
  bootstrap, which is what makes durable replay of file reads work) — and
  `pinkdb-arrow` implements it over arrow-rs 59.3 (versions aligned with the
  vortex family already in the lock). Landed fully: replacement scans,
  `read_arrow`/aliases, `COPY … (FORMAT arrow)`, plans/EXPLAIN/DESCRIBE,
  bind-time refusals naming the format, replay re-reading files with a
  vanished-file hard error, RBAC fail-closed. IPC **file** format only (the
  stream magic is refused by name); reads accept uncompressed/LZ4/ZSTD,
  writes uncompressed, one batch per chunk; scans materialize (pushdown is a
  recorded follow-up). DuckDB α has no Arrow support to diff against, so
  parity was pinned with pyarrow round-trips (both directions), golden
  fixtures and 15 value probes — the negative control (default-feature CLI)
  fails loudly with the build hint. The **C Data Interface is deferred with a
  5-item pin** (crate README §follow-up). Sizes: engine seam +21,840 B on
  every build; `--features arrow` +3,169,520 B. Gates: pinkdb **604**,
  arrow **29**, cli+arrow **73**, workspace **1115**, duckdiff **287/287**,
  fmt/clippy both toolchains, wasm green (core + crate). CI duckdiff artifact
  now builds `--features arrow`. Integration on main: clean apply (33 files);
  conflicts with wave 70 anticipated and the reconciliation recipe recorded
  (one `CopyTo`/`FromSource::File`, parquet as another `FileFormat` impl).
  Worktree removed.

- 2026-10-19 — Wave 70 (parquet read extension; worktrees `wt-parquet` →
  `wt-unify`): read-side Parquet with the DuckDB surface (globs/directories,
  multi-file, `union_by_name`, hive keys, `filename`, `file_row_number`, the
  four metadata functions, replacement scan) plus **projection pushdown** and
  **row-group-statistics pruning** with visible counters; the reader decodes
  straight into engine vectors (no arrow-array bridge, no duplicate arrow-rs
  units — `parquet`'s arrow feature stays off). It was built in parallel with
  wave 71 and they collided on the FROM/scan layer (two AST variants for
  `FROM 'x'`, two plan leaves, opposite logging policies), so the landing is a
  **unification**, not a union: one read surface (`TableSource` — bind-time
  schema, chunk scans, projection + `PrunePredicate` pushdown, global +
  per-store registry seeded at construction for durable replay), arrow ported
  onto it (`ArrowSource`; its `ArrowSink` kept, and `filefmt::FileFormat`
  scoped to COPY-only write sinks), exactly one grammar alternative for a bare
  path and one for table functions, and **one file-logging policy: re-read**
  (this wave's refusal reverted; replay re-reads, a deleted file is a hard
  recovery error, a changed file replays its new content — DuckDB's rule).
  The parquet source claims only `.parquet/.parq/.pq` (+ extension-less
  existing directories) so sources can coexist. duckdiff batteries merged
  (28 parquet + 15 arrow = **315/315**) and CI now builds the differential
  artifact with `--features arrow,parquet`. Honest limits: read-only (COPY
  parquet is a follow-up), flat schemas only, no TIME/TIMESTAMP(nanos)/UUID/
  FLOAT16/BSON/VARIANT/nested, unannotated BINARY reads as VARCHAR, no
  bloom/page-index pruning, no remote paths (wave 75). Integration on main:
  clean apply (47 files, 12 binary fixtures). Worktrees removed.

- 2026-10-19 — Wave 77 (SQL macros; worktree `wt-macros`): scalar and table
  macros with bind-time **AST substitution** — arguments are cloned and bound
  in the caller's scope (no SQL text, no dynamic SQL), so call-site typing,
  invoker rights and injection safety all hold, and `?` slots survive; a
  depth cap (8) plus a node budget make recursive/duplicating macros fail
  cleanly. Recursion is refused at CREATE for direct/mutual/forward-reference
  cycles via a two-direction call-graph check (stricter than α for OR REPLACE
  cycles), with view-crossing cycles caught at bind. Macros persist as a new
  `.pink` section (flags **bit 6**), deliberately ordered before materialized
  views so MV rebinds on load see them, and DDL is WAL-logged and replayed in
  order. RBAC is deliberately security-tested: create/drop needs database
  `CREATE`/`ADMIN`, invocation is invoker-rights, and expansion happens before
  `check_statement` — a test proves a macro cannot bypass table grants.
  Documented α divergences: no typed params/`query_table`/lateral arguments,
  body validation deferred to first use, macro DDL refused inside explicit
  transactions, MV macro expansion frozen until the stored definition rebinds.
  Gates: pinkdb **639**, workspace **1176**, duckdiff **326/326** (+11 macro
  probes), fmt/clippy both toolchains, wasm green. Size: default binary
  **+97,728 B** (full size-table refresh deferred to the next landing, where
  httpfs also shifts the rows). Integration on main: clean apply (19 files).
  Worktree removed.

- 2026-10-19 — Fix (wave-51 SDK follow-up; worktree `wt-sdkfix`): CI's
  `cargo build` went red on Linux with duplicate `pinkdb_extension_init` /
  `_name` symbols — `pinkdb_extension!` emitted the three ABI v1 exports
  unconditionally, so **linked-only** extensions (`pinkdb-math`,
  `pinkdb-datetime`) collided once wave 77's codegen shifted which archive
  members got pulled; macOS's linker tolerates duplicate symbols, which is why
  every local gate was green. Fix: `pinkdb_extension!` now generates only the
  declaration (`pinkdb_scalar_specs`/`pinkdb_linked_extension`/
  `pinkdb_register_linked` + hidden name constants) and a new explicit
  `pinkdb_abi_exports!()` emits the three `#[no_mangle]` symbols —
  **cdylib-only by construction**; the dual-artifact `extensions/string`
  gates it behind an `abi-exports` feature enabled by its Makefile, while
  math/datetime emit nothing and the SDK example/template call the macro
  unconditionally. Regression coverage that doesn't need rust-lld:
  `ext-sdk/rust/check-native-exports.sh` (`nm` proof that linked-only rlibs
  carry no exports, with a positive control on the cdylib; wired into the
  smoke and the `sdk-smokes` CI job) plus `extensions/math/tests/coexist.rs`,
  which links math + datetime + a local declaration into one binary and
  queries all three (on Linux, a regression is a link error there). Docs:
  SDK README "Native cdylib: opting in to the ABI exports", crate/manifest
  docs, bench builds string with the feature. Gates: `cargo build` green
  (the failing step), SDK/loader/math/datetime/string suites green, pinkdb
  **639**, workspace **1177**, fmt/clippy both toolchains, wasm green,
  duckdiff **326/326**. Size: **0 B** (the linker already GC'd the wrappers).

- 2026-10-19 — Wave 75 (httpfs transport + remote file formats; worktree
  `wt-httpfs`, second pass `wt-httpfs` resume): the SigV4 S3 client moved out
  of pink-lake into **`pinkdb-httpfs`** with behavior-identical lake semantics
  (config envs, retries, conditional writes, listings; `S3Storage` is now a
  thin adapter and the lake's 134 + 26 S3-suite tests stay green), plus https
  GET/HEAD and **byte ranges** on both schemes behind a small `Fetcher`
  contract. CSV reads remote via the engine's URL hook; **parquet reads
  remote through a real `ChunkReader`** — footer and row-group/column pages
  are ranged requests (tests pin the 8-byte trailer range and prove no
  whole-object GET when HEAD reports size; whole-object only on unknown
  size), including the remote metadata functions; **arrow is whole-object by
  design** (IPC has no ranged footer protocol — documented, not faked).
  Claims are extension-based regardless of scheme, so `FROM 's3://…x.parquet'`
  and `read_arrow('https://…')` route to the right source and `http://` is
  transport-gated. Feature composition is explicit: `httpfs` alone is the
  transport + CSV, and it weakly enables the format crates' `remote`
  features, so `--features parquet,arrow,httpfs` gets everything while
  default builds link no transport. DuckDB α's httpfs is installed here and
  now cross-checks remote parquet/CSV in the differential battery (Arrow has
  no α reader — pinned in Rust). Gates: pinkdb **641**, workspace **1198**,
  httpfs **49** (s3), parquet+remote **36**, arrow+remote **37**, CLI combo
  **80**, pink-lake **134**, duckdiff **339/339**, fmt/clippy both toolchains,
  wasm green. Sizes: opt-in `httpfs` +1.95 MiB, `parquet,httpfs` +3.09,
  `arrow,httpfs` +5.01, default +~800 B. Honest limits: single-object reads
  (no remote globs/dirs, no compression, no `COPY TO s3://`), 128 KiB
  read-ahead granularity, no HTTP auth/custom CA, S3 URLs don't follow
  redirects. This closes the file-access block's transport; **iceberg (72)
  remains queued** as the transport's next consumer. Worktrees left in place
  (no-cleanup policy).

- 2026-10-20 — Security hardening, low-severity batch (from
  `docs/security-review-2026-10-19.md`; worktree `wt-seclow`): **L-2** the
  server protocol's `Request` Debug now redacts the plaintext login password;
  **L-3** engine key-material comparison uses `subtle::ConstantTimeEq` (the
  `encryption`-feature edge only; `subtle` was already locked); **L-11** the
  SigV4 `Signature` Debug redacts `canonical_request`/`signed_headers` so a
  live `x-amz-security-token` cannot leak through logs (the code moved to
  `pinkdb-httpfs` in wave 75); **L-12** every lake commit-delta path
  (`data_files`, `delete_files`, `compacted_files`, `removed_delete_files`)
  is validated at manifest-read time, and `LocalStorage` gained
  `checked_path()` (absolute/`..`/prefix paths → `LakeError::Corrupt`, with
  `abs()` made escape-proof at I/O); the dead `pink-lake/src/s3/sigv4.rs`
  copy left by the wave-75 extraction is deleted; **L-13** all five `.pink`
  RLE decoders (and two LZ guards) use `checked_add` instead of `out.len() +
  len`; **L-14** the Cloudflare token moved from the job-level env to the
  `wrangler deploy` step only; the four `.lake` subcommands that bypassed
  `PINKDB_KEY` now open through `open_lake()` (keyed, fail-closed); the S3
  test harness pins all three images by multi-arch digest. Gates: pinkdb
  **642**, server **67**, CLI **73**, pink-lake **121**, workspace **1204**,
  duckdiff **339/339**, fmt/clippy both toolchains, wasm green, YAML/shell
  validated. No format change; no new crates. Pass 1 (extension RCE)
  remains separate.

- 2026-10-20 — Security hardening, pass 1: extension-loading RCE (review
  C-1/C-2/H-7; worktree `wt-sec1`). `LOAD EXTENSION` is now **off by default**
  everywhere (engine store default false; C ABI
  `pinkdb_set_allow_sql_load_extension`; CLI `--allow-load-extension` /
  `PINKDB_ALLOW_LOAD_EXTENSION`; Python/Go binding opt-ins; the C header now
  documents that `pinkdb_execute` can load native code). **WAL replay never
  auto-loads code from a file**: the replay force-allow is deleted,
  `DurableOpenOptions` gained `allow_extension_replay` (default false) and
  `extension_root` (default None), and a recorded load resolves in this order —
  already loaded in this process by **canonical path** (the mapped image is
  reused: no `dlopen`, no constructor, no re-init) → opt-in with a path
  **inside the root** → **hard error at open** naming both options (never a
  silent skip). The WAL now records the canonical path. Loader: bare names
  (dynamic-loader search path), `..`, symlinked libraries and non-regular
  files are refused; paths are canonicalized; `load_in_root` /
  `resolve_path_in_root` confine to a root; server and CLI gained
  `--extension-root`. The review's A.1 attack is a **passing regression test**
  (a fixture extension whose constructor writes a marker: the marker must stay
  absent after reopen, and the open fails unless the opt-in + matching root
  allow it), plus default-off, C-ABI switch, confinement, server/CLI flag and
  binding tests. Documented behavior change: a durable store that used an
  extension refuses to open unless the extension is already mapped in-process
  or the replay opt-in + root are supplied. Gates: pinkdb **644**, ext-loader
  72 → **81**, server **70/87/81**, CLI **77/82/87**, FFI 16 → **17**, Python
  **60**, Go **40**, workspace 1198 → **1217**, duckdiff **339/339**, fmt and
  clippy clean on both toolchains, wasm green. Size: default CLI **+37,056 B**
  (the gate cannot be feature-gated). Residual risk recorded: already-mapped
  libraries can be re-enabled by a crafted file (no new code runs), a TOCTOU
  window remains between validation and `dlopen`, a root-contained library's
  `DT_NEEDED` dependencies are not confined, and `PinkDb::load_extension`
  stays host-trusted.

- 2026-10-20 — Security hardening, pass 2: authentication gate + escalation
  (H-1/M-1 + role-ADMIN + banner; worktree `wt-sec2`). The server-armed gate
  now runs **inside `Connection` before every statement** (`require_login`),
  so a multi-statement batch is checked between statements against the store's
  current `has_users()`: the unauthenticated `CREATE USER evil …; GRANT ADMIN
  TO evil` batch creates the user but **refuses the grant** (A.2 pinned as a
  test on both transports), and prepared execution is covered through the same
  path while the early request-level refusal stays for clean errors. The
  empty-catalog shortcut moved out of `check_statement`: only
  `principal == None` (embedded/trusted) skips checks, so an authenticated
  session whose catalog is emptied is denied (A.7 on both transports). The
  engine refreshes a durable store's catalog before the gate, so cross-process
  user commits are observed (a residual TOCTOU window is documented).
  Role-derived ADMIN is now honored consistently by enforcement and
  `PinkDb::principal_is_admin` (`is_admin_resolved`), and the server's
  usage/announce text no longer claims "no authentication". Gates: pinkdb
  645 → **650**, server **78** (async **95**, tls **89**), CLI 77, workspace
  1223 → **1235**, duckdiff **339/339**, fmt/clippy both toolchains, wasm
  green. Size: server +336 B; CLI unchanged. Tests added: 12 (A.2 both
  transports, batch-refused-before-first-statement, A.7 both transports,
  role-granted ADMIN over the wire and engine, prepared gating, banner text,
  empty-catalog engine unit).

- 2026-10-20 — Security hardening: extension repository signing (M-10;
  worktree `wt-signing`). `publish.py` signs the **exact `index.json` bytes**
  with Ed25519 (`openssl pkeyutl -sign -rawin`) and emits/uploads
  `index.json.sig` (`PINKDB-SIG1 <keyid> <base64>`, key id = first 16 hex of
  SHA-256(SPKI DER); accepts a PEM path/content/base64; no key = unsigned
  publish with a loud warning). `verify.sh` verifies the catalog and every
  artifact. `.install` fetches catalog + signature, verifies against the key
  **pinned in the binary** (embedded SPKI PEM; current key id
  `c84e54bfea6b2d09`) **before any artifact request**, then verifies the
  SHA-256, stages a copy in a `0700` directory with `create_new`, re-hashes it
  from disk, and only then loads it. A 404 signature means "unsigned" and is
  refused unless `PINKDB_EXT_ALLOW_UNSIGNED=1` (loud dev warning);
  `PINKDB_EXT_REPO_PUBKEY` replaces the pin for tests/rotation. The Worker
  serves `/index.json.sig`; the publish workflow scopes
  `PINKDB_EXT_SIGNING_KEY` to the publish step and adds a post-sign
  verification gate against the live catalog + every artifact. Deps:
  `ed25519-dalek` + `base64` behind `repo` only (default binary unchanged;
  `repo` build +327 KB). Tests: 10 signature unit, 2 staging, 6 CLI
  integration (unsigned/tampered/unknown-key/malformed/refused before any
  artifact fetch), 14 Python (publish/verify modes); real-key end-to-end
  install verified locally. Gates: CLI 77 / repo **122**, workspace ~1230,
  duckdiff **339/339**, fmt/clippy both toolchains, wasm green.
  Residual: the `create_new`-then-`dlopen` TOCTOU window (open-fd loading is
  the recorded follow-up), the env overrides are insecure if the environment
  is hostile, a single pinned key with no revocation/freshness (an old
  catalog can be replayed), and the **live catalog remains unsigned** until
  the operator runs a signed publish and deploys the Worker route.

- 2026-10-20 — Security hardening, pass 3: async transport TLS downgrade (H-2;
  worktree `wt-sec3`). `--transport async` now **refuses** `--tls-cert` /
  `--tls-key` before binding: exit code 2 with an actionable message, checked
  before the sync both-or-neither pairing rule so either flag alone gets the
  transport-specific error (independent of the `tls` cargo feature). The
  help/usage text, crate docs and README state that TLS is sync-only and that
  the async transport refuses the flags — no security flag is accepted and
  ignored. Tests (gated on `async-server`): a valid rcgen pair + async → exit 2,
  exact message, no readiness line, port not listening; each flag alone → same;
  async without TLS still starts and serves `SELECT 1`; `--help` documents the
  limitation. The sync TLS round-trip/startup-refusal suites stay green.
  Implementing native async TLS (`tokio-rustls`, handshake timeout and
  isolation, connection-cap/stall tests, then removing the refusal) is recorded
  as the follow-up. Gates: server **79/99/90/110** (default/async/tls/both),
  workspace **1236**, CLI 77, fmt/clippy both toolchains, wasm green; size
  **0 B**. `duckdiff` skipped (server-only change).

- 2026-10-20 — Security hardening, pass 4: index lifecycle (H-3/H-4/H-5/L-1;
  worktree `wt-sec4`). Every mutation (INSERT/UPDATE/DELETE/COPY/bulk load,
  materialized-view clears) now marks a table's indexes stale via
  `indexes_stale`; `PinkDb::rebuild_stale_indexes` rebuilds them from the live
  chunks at the next statement boundary (refreshes and WAL reloads trigger it
  too). The optimizer always emits `Filter` **above** `IndexScan` and refuses
  an index whose stale flag is set, so an index can only propose candidates —
  a stale or corrupt one costs performance, never correctness. The index gather
  bounds-checks every row id against the live row set **and** each column's
  chunk length, making the stale-index panic (A.5) impossible; it also fixes a
  latent global-row mapping bug (real per-chunk lengths instead of
  `ri / CHUNK_SIZE`). Probe keys are canonicalized to the indexed column's type
  with the engine's `cast_value` (BIGINT/DOUBLE/DECIMAL columns + integer
  literals now find their rows, A.6), and the rewrite is refused when no
  faithful key exists (NULL, unrepresentable casts, f64-collapse pairs).
  `DROP COLUMN` drops indexes on the dropped column and shifts the remaining
  ordinals (L-1), so a later `ADD COLUMN` cannot make an index point at the
  wrong column. **Indexes are process-lifetime only**: the hash maps are not in
  `.pink` (v5 flags bit 3 is reserved for wave 79) and `CREATE`/`DROP INDEX`
  are **not** replayed from the WAL today, so a reopen loses the index
  definition and falls back to scans — results stay correct, but the missing
  durability is wave 79's job (the earlier assumption that replay rebuilt the
  index is wrong; measured with the durable CLI store). Tests: new
  `crates/pinkdb/tests/indexes.rs` (15: EXPLAIN `IndexScan` + identical
  results, A.4/A.5/A.6, INSERT/UPDATE/DELETE/bulk freshness, duplicates/NULL/
  empty tables, multi-chunk delete mapping, out-of-range entries skipped,
  corrupt-index candidate rejection, DROP+ADD COLUMN). Gates: pinkdb **665**,
  workspace **1251**, duckdiff **339/339**, fmt/clippy both toolchains, wasm
  green; CLI size **+17,296 B**.

- 2026-10-20 — Index DDL durability (minimal half of wave 79): `CREATE INDEX` /
  `DROP INDEX` are now WAL-durable **without a `.pink` format change**. Index
  DDL is classified as a catalog write and buffered as canonical,
  fully-qualified statement text (`CREATE INDEX [IF NOT EXISTS] "idx" ON
  "db"."table"("col")`, `DROP INDEX [IF EXISTS] "db"."idx"`) with the target
  table in the touched set. `CREATE INDEX` plans retain both the resolved
  ordinal and the column's catalog spelling; `DROP INDEX` retains its owning
  table — so quoted/spaced/unicode/keyword identifiers round-trip. Because a
  checkpoint folds `.pink` (still no index section; bit 3 reserved) and rolls
  the WAL, a successful checkpoint **re-logs every live index definition into
  the fresh WAL generation** (rendered before the checkpoint, appended after
  the roll, exactly once per generation; dropped indexes stay dropped). A
  crash in that post-roll window loses only index definitions — full atomicity
  needs the `.pink` section — and startup rebuilds each map from live data
  (O(rows) per index). `.pink` bytes/version/flags are unchanged: bit 3 stays
  clear, no-index snapshots are byte-identical to the base build, and the base
  build replays the canonical records. Tests: 13 + 1 encrypted — reopen
  without checkpoint, checkpoint + repeated checkpoints, checkpoint-then-drop,
  drop-table, quoted/unicode/recreated names, second-handle refresh,
  transaction order, byte/flag identity, canonical text + touched set,
  torn-tail (no fabricated index), intact-record-before-torn-tail, and
  connection-level reopen; the pre-fix test failed with "index lost on reopen
  without a checkpoint". Gates: pinkdb 665 → **678**, workspace 1251 →
  **1264**, `encryption` **719**, duckdiff **339/339**, fmt/clippy both
  toolchains, wasm green; default CLI **+21,680 B**. Remaining wave-79 scope:
  the `.pink` index section + bit 3, persisted maps/statistics, and the
  range/zone-map index-vs-scan choice.

- 2026-10-20 — Security hardening, pass 5: authentication DoS, timing, hash
  policy, and statement resource bounds (H-6/M-2/M-8/M-6/M-11; worktree
  `wt-sec5`). **H-6**: `Connection::login` refreshes, copies the display name
  + PHC hash out under the read lock, **drops the guard**, verifies Argon2id
  lock-free, then re-refreshes/re-checks the record before adopting the
  principal (a rotation/drop during derivation cannot be bypassed). The server
  adds a per-connection failed-login limit (5; delays 25→400 ms, then close), a
  bounded per-IP limiter (20 failures/60 s → 60 s block, 4096 entries, lazy
  expiry + oldest-observed eviction) and a non-queueing global verification
  gate (`LoginGate`, default 4), so 128 clients can no longer allocate ~2.4 GiB
  of transient Argon2 memory or saturate the read lock. **M-2**: unknown users
  run a fixed dummy Argon2id verification, so unknown-user and wrong-password
  paths both pay one derivation and keep identical text (deterministic counter
  + coarse median timing test). **M-8**: `validate_password_hash` parses the
  PHC version and m/t/p and enforces floor m≥19456, t≥2, p≥1; ceiling m≤77824,
  t≤8, p≤4, m·t≤311296; version 19; salt and hash present — applied to
  CREATE/ALTER USER, persistence loading and WAL replay. Weak hashes fail
  closed; oversized ones cannot cause multi-GiB logins. **M-6**:
  `ExecLimits`/`ExecBudget` deadline checks at every plan node and
  scan/filter/project/sort/aggregate/update/delete/join chunk boundaries, with
  materialization caps on join/`CROSS JOIN`/hash-join flushes and final
  results; server defaults 30 s / 10M rows / 256 MiB behind
  `--statement-timeout-ms`, `--max-result-rows`, `--max-result-bytes`,
  `--max-concurrent-logins` (invalid/0 rejected); embedded defaults stay
  unlimited unless explicitly set; timeout errors drop the write lock and
  UPDATE/DELETE check before write-back. **M-11**: no per-refusal thread —
  refusals are written on the accept loop with a 100 ms timeout and then
  closed; accepted-spawn failures are logged and the connection closed; async
  refusals remain tokio tasks. Tests: 20 new (lock-not-held, rate-limit
  close/block/expiry/eviction, gate cap, dummy counter + timing, PHC
  floor/ceiling/malformed, timeout + row/byte caps including CROSS JOIN,
  exact-cap acceptance, both transports, connection reuse after timeout,
  non-reading-refusal responsiveness, flag validation). Gates: pinkdb 678 →
  **686**, server **89** (async **109**, tls **100**, both **120**), CLI 77,
  workspace 1264 → **1282**, duckdiff **339/339**, fmt/clippy both toolchains,
  wasm green. Sizes: server **+36,624 B** (~1.05%), CLI **+17,184 B** (~0.44%).
  Residual: timeout granularity is chunk-boundary (a single in-progress chunk,
  sort, CREATE INDEX, CHECKPOINT, procedure loop or COPY is not interrupted;
  write-back is never partial), per-IP dilution via rotating addresses and NAT
  sharing, embedded unbounded defaults, result caps are estimates and
  LIMIT-over-join may still materialize the join, legacy weak/oversized hashes
  now fail closed on load/replay, async refusals remain one tokio task each.

- 2026-10-20 — Wave 90: tag-driven release train + `install.sh` + same-tag extension
  publishing (worktree `wt-w90`). `release.yml` builds 36 artifacts (24 CLI × 12
  server) for linux x86_64/aarch64 and darwin arm64/x86_64 (darwin-x86_64 cross-built
  on `macos-latest`), smoke-tests every one, writes `SHA256SUMS`, and has a single
  `contents: write` publish job that refuses an existing release, creates a draft,
  uploads, then publishes — immutability enforced in code, never by overwriting.
  `publish-extensions.yml` now also fires on `v*`: per-platform builds fan in to one
  signed catalog publish (existing coordinates skipped; `publish.py` refuses too) and
  one Worker deploy, under the existing concurrency group. `install.sh` resolves
  latest (GitHub API) or a pinned version, maps variants (`pinklake,s3` →
  `pinklake-s3`), verifies `SHA256SUMS` fail-closed, checks archive paths, extracts
  to a temp dir and atomically installs to `${PREFIX:-$HOME/.local}/bin`, with PATH
  guidance, `--dry-run`/`--force`, no sudo, and test-only
  `PINKDB_API_BASE`/`PINKDB_RELEASES_BASE`/`PINKDB_OS`/`PINKDB_ARCH` overrides. The
  static site gained an install page + nav/home links; CLI gained `-V/--version`;
  Windows is documented unsupported (no `install.ps1`). Verification: installer
  **23/23**, release tooling **12/12**, actionlint + YAML + `bash -n`/`sh -n` +
  shellcheck + py_compile clean, site 24 pages + 12 assets, a real local install ran
  `pinkdb 0.1.0` with `json_valid('{}') = true`, packaging hashes reproducible,
  darwin-x64 cross smoke under Rosetta, extension staging arch-checked. Gates: CLI
  77 → **78**, workspace 1282 → **1283**, fmt/clippy both, duckdiff skipped (engine
  untouched). Residual: runner availability, no Windows, linked core extensions/SDK
  examples not yet catalog entries, unauthenticated `latest` API rate limits, and the
  workflows have not run on real GitHub runners — the first `v*` tag is the final gate.

- 2026-10-21 — Wave 78: `pinkdb-server` warehouse browsing UI (worktree `wt-w78`).
  A new `crates/pinkdb-server-ui` crate behind the off-by-default `ui` feature
  serves a read-only console next to the binary protocol: **axum 0.8 + tokio +
  maud**, with the tokio runtime owned by the console crate on its own thread
  (the repo's **second documented async carve-out**, after wave 47). Every
  engine read runs on `spawn_blocking` with explicit worker/blocking-pool
  sizes; the accept loop is semaphore-bounded; hyper/axum limits are explicit
  (64 KiB body, 10 s header, 30 s request). Routes: `GET /`, `/preview`
  (no-JS), embedded `/assets/*`, and JSON databases/schemas/tables/rows/stats
  with bounded paging and exact totals — **no mutating route exists**. The
  exposure policy fails closed: opt-in, loopback default, non-loopback bind
  refused (no console TLS), and a store with RBAC users refuses to start
  without `--ui-auth basic` (engine login → per-user grants + pass-5-style
  throttling) or `--ui-token` (constant-time bearer); user-less stores warn
  loudly. Previews mirror the CLI box-renderer rules and maud escapes every
  cell; the script never uses `innerHTML`. Tests: 28 hermetic integration tests
  on a real port (endpoints, paging/totals, 404/405/HEAD, header/body limits,
  slow client, traversal, loopback refusal, Basic/token, read-only guarantee,
  `<script>` escaping, engine off the reactor, multi-database) + 12 unit tests;
  the `ui` doctest was fixed and CI now runs `--features ui` and `ui,tls`.
  Sizes: default server **+128 B**; UI feature **+1.15 MiB** (19 new lock
  entries). Residual: loopback-only until console TLS, user-less stores are
  unauthenticated, token mode is operator-level, sizes are estimates, deep
  paging scans, and the query console is a follow-up.

- 2026-10-21 — Security hardening, pass 6: filesystem privileges, durable file
  modes, and lake integrity binding (M-3/M-4/M-5/M-7/M-12; worktree `wt-sec6`).
  **M-3**: `COPY … TO` now requires **ADMIN** (SELECT never authorizes a
  filesystem write) and the server refuses it by default; `Server::allow_copy_to`
  + `copy_root` (and `--allow-copy-to`/`--copy-root`; a root is mandatory) install
  a confinement policy — destinations are canonicalized under the root, `..`,
  absolute escapes, symlinked parents and symlinked targets are refused, and
  every write goes through `pinkdb::write_atomic_private` (unique `create_new`
  temp sibling, 0600, flush/fsync, atomic rename, parent fsync) in the engine,
  the test sink and the Arrow writer. **M-4**: table-function plans are detected
  by a full statement walk (CTEs, set-operation arms, expression subqueries,
  materialized-view queries, `INSERT … SELECT`, `CREATE TABLE AS`, `EXPLAIN`)
  and denied unless the principal is ADMIN; the embedded/trusted path is
  unchanged and table-source path policy stays source-owned (documented).
  **M-5**: WAL, recovery temps, checkpoint `.pink`/`.tmp`, lock files, engine
  saves and every lake artifact (catalog/snapshots/commits/data/delete/lake.json/
  highest.json) are created **0600**, newly created lake directories **0700**,
  and existing checkpoint/WAL/lock/lake files are tightened best-effort on open
  (shared user-owned parents untouched; non-Unix no-op). The A.9 reproduction is
  a regression test. **M-7**: lake catalog format **v6** with a random
  `lake_id`, manifest containers AEAD-bound to `lake:<lake_id>:<version>`,
  `lake.json` identity + `highest.json` rollback marker, catalog↔snapshot
  identity/version cross-checks and rollback refusal; keyed pre-v6 lakes are
  cleanly refused (empty-binding containers cannot be upgraded safely) and
  plaintext pre-v6 lakes upgrade on the next save. **M-12**: local
  `read_tagged` opens once, `fstat`s and reads the **same fd**, and the tag now
  includes `(dev, ino)` in addition to length/mtime — a same-length/same-second
  replacement is distinguished and `put_if_match` rejects the stale tag.
  Tests: engine file modes (fresh 0600, existing 0644 tightened, A.9), server
  COPY refusal/opt-in/root escapes/symlinks/atomic replace/both transports,
  table-function denial in every nesting shape, lake cross-lake/rollback/
  version-mismatch refusal (local + S3), keyed-legacy refusal, and the M-12
  inode/race cases. Gates: pinkdb **689**, server **94** (async **114**, tls
  **105**, both **125**), CLI **78**, workspace **1293**, duckdiff **339/339**,
  encryption **730**, lake **251** (all features), arrow **30**, fmt/clippy both
  toolchains, wasm green. Sizes vs pass-5: `pinkdb` **+17,424 B**,
  `pinkdb-server` **+19,344 B**. Residual: full-storage-control rollback still
  needs an external witness/object lock; legacy keyed pre-v6 lakes need a
  pre-M-7 build to migrate; coarse-timestamp filesystems now fall back to inode
  identity (network FS inode reuse remains); COPY atomically replaces (never
  appends) and a symlink-parent swap between validation and temp creation is a
  narrow TOCTOU mitigated by root confinement; tightening scans `data/**` on
  lake open; table-function paths are ADMIN-gated rather than root-confined.

- 2026-10-21 — Security hardening, pass 7 (**final**): terminal escaping, crypto,
  WAL, loader/FFI and operational hardening (M-9, L-4…L-10 + the informational
  items; worktree `wt-sec7`). **M-9**: one public `pinkdb::sanitize_terminal`
  now guards every rendered identifier/name — all `QueryResult` variants,
  `Selected` column names (before width calculation), `cmd_databases`/
  `cmd_tables`/`cmd_schema`, `.installed`, CLI errors, and lake names/keys/
  changes/paths; ANSI/CSI/OSC/DCS are escaped, legitimate UTF-8 is preserved,
  and VARCHAR cells keep the existing behavior. **L-4**: engine containers v4
  and lake containers v2 use a fresh 128-bit random `nonce_base`; the 96-bit
  GCM nonce is `truncate96(SHA-256("pinkdb-nonce-v4" || domain || base ||
  counter))`, dropping the key-check birthday bound from n²/2⁶⁵ to n²/2⁹⁷;
  read-old/write-new (engine v1–v3 and lake v1 open; keyed WALs rebuild at open,
  checkpoints/lake artifacts rewrite on save). **L-5**: WAL v4 authenticates a
  144-byte publish block (static identity + meta AEAD tag + two individually
  authenticated slots with fresh 96-bit nonces); tampered identity is a hard
  recovery error and a torn slot is a hint, frames remain authoritative; v2/v3
  blocks are read only for upgrade. **L-6**: `arg_type_at` returns
  `Option<ExtType>`, so zero-argument functions cannot underflow. **L-7**: the
  FFI validates `value_count` (hard cap 65,536 + exact parameter-count match)
  before allocating. **L-8**: the `Collector` is `Arc`-owned by the loaded
  library and carries an `init_complete` flag; post-init registration errors
  deterministically. **L-9**: VARCHAR results decode the type first, ignore and
  never free non-VARCHAR pointers, and cap copies at 64 MiB. **L-10**:
  `offset_of!`/`size_of` const assertions pin `PinkValue` for 64/32-bit layouts
  and the header-sync test now parses and lays out the C struct. Informational:
  per-frame idle deadline on the sync transport (whole prefix/type/body, reset
  between frames), `PINKDB_PASSWORD` only with `--host`, keyed `.lake
  partition/inlining/flush/add_data_files`, a pinned DuckDB alpha artifact with
  SHA-256 verification in CI (no piped remote installer), digest-pinned Docker
  images, and `--tls-insecure` documented as a residual footgun. Tests: sanitizer
  coverage for every result variant + alignment + hostile-server output, v4/v2
  nonce and legacy-format tests, WAL publish-block tampering, zero-arg loader,
  absurd FFI counts, late-registration, bogus VARCHAR length/pointer, FFI
  layout, slow-drip frame deadline, ambient-password, keyed lake maintenance,
  and the S3 v6 identity/count expectations. Gates: pinkdb **693**, server
  **95** (async **115**, tls **106**, both **126**, ui **96**, ui,tls **107**),
  CLI **81** (**88** with client; lake combos 124/89/93/82), workspace **1346**,
  encryption **737**, lake **124/253**, arrow **30**, FFI **19**, ext-loader
  **83**, Python **60**, Go ok, duckdiff **339/339**, fmt/clippy both toolchains,
  wasm green, actionlint/YAML/shell clean. Sizes: engine rlib **+38,480 B**,
  server **+1,456 B**, CLI **+1,168 B**, FFI dylib **+2,224 B**. The closing
  C/H/M/L + informational matrix and the residual-risk register live in
  `docs/security-remediation-pass7.md`; **no findings remain beyond the
  documented residuals** (dlopen TOCTOU, single pinned signing key/no
  revocation, env overrides, no native async TLS, unsigned local extensions,
  index-rebuild cost, full-storage-control lake rollback, table-function/COPY
  path policy, renderer-side terminal protection, legacy encrypted bytes
  retaining the old birthday bound until rewritten, slot-hint suppression, and
  `--tls-insecure`).

- 2026-10-21 — Index persistence in `.pink` (wave 79, part 2): v5 flags
  **bit 3** (`FLAG_INDEXES`, the bit reserved since wave 80) adds a per-table
  index section after the column chunks: per index (sorted by name) the name,
  column ordinal and save-time statistics — `distinct_keys` and
  `indexed_rows` (the column's non-NULL count). Maps are **rebuilt from the
  decoded rows at load**, never read from the file, so an index cannot
  disagree with its rows whatever a (CRC-valid, crafted) file claims; the
  loader bounds the statistics (`indexed_rows ≤ rows`, `distinct ≤ indexed`)
  and refuses empty, duplicate or out-of-order names and out-of-range
  columns as `corrupt file`. Stores without indexes keep their exact bytes
  (bit 3 clear); older builds refuse bit-3 files through the unknown-flags
  check. Checkpoints now publish indexes **atomically with the data**, so the
  part-1 post-roll WAL re-log (and its crash window) is removed; WAL-only
  `CREATE`/`DROP INDEX` durability between checkpoints is unchanged, and WALs
  written by the part-1 build still replay. **Persisted maps were built and
  dropped on measurement** (1M rows, release, best of 3): a posting-list
  section (LEB128 row deltas, keys re-derived from each group's first row,
  fully verified at load so the never-a-wrong-answer invariant held) opened
  in 28 / 165 / 60 ms vs a 23 / 149 / 67 ms rebuild (INTEGER 1k distinct /
  BIGINT unique / VARCHAR 5k distinct) while adding 2.0 / 4.0 / 2.0 MB to a
  10.1 MB file; skipping per-row verification would have traded that
  invariant for speed. The shipped section adds ~30 B per index. Found in
  passing: the scan comparison kernel treated DOUBLE `NaN` as equal to
  every value (`partial_cmp(..).unwrap_or(Equal)` in `exec/expr.rs`), so
  `d = 0.0` on a column holding NaN counted the NaN rows on a scan but not
  through an index — fixed in the next entry; and (not changed) a
  save→load→save of a table after `DELETE` can differ in validity-buffer
  length (not canonical bytes). Tests: the section layout
  pinned byte for byte, rows-not-statistics map authority, 7 corrupt-section
  refusals, round trips over three chunks with NULLs/duplicates/`-0.0`/NaN/
  VARCHAR keys and an emptied range, bit-3 clear without indexes (and after
  the last `DROP INDEX`), stale-at-save, damaged in-memory map, checkpoint
  publication; the part-1 re-log tests now assert an empty post-checkpoint
  WAL. Gates: pinkdb **700**, workspace **1353**, `encryption` **744**,
  duckdiff **283/283** (default-feature CLI), fmt/clippy both toolchains
  (Homebrew 1.97.1, rustup stable 1.99), wasm green. Remaining wave-79
  scope: the range/zone-map index-vs-scan choice.

- 2026-10-21 — Fix: DOUBLE NaN comparison semantics (found in passing by wave
  79 part 2's index-vs-scan test). The scan compared DOUBLE with
  `partial_cmp(..).unwrap_or(Equal)`, so NaN compared **Equal to every value**:
  `WHERE d = 0.0` over `{-0.0, 0.0, NaN, 1.5}` returned 3 rows while the hash
  `IndexScan` (whose `encode_value` keys NaN separately) returned 2, breaking
  the index-never-changes-the-result contract. `plan::num_cmp` is now DuckDB's
  total order (probed against the local `duckdb`): `NaN = NaN` for every
  payload, NaN equals no other value and sorts above every other double
  (`+inf` included), `-0.0 = 0.0`. It backs `compare_values` (ORDER BY
  fallback, MIN/MAX, nested-loop/range joins, windows, GREATEST/LEAST), both
  typed DOUBLE comparison kernels in `exec/expr.rs` (vector-vs-vector and
  column-vs-literal) and the aggregators' typed MIN/MAX path. The byte-arena
  sort key now encodes NaN as all-ones (above `+inf`), so NaN keys no longer
  force the legacy `Value` sort or disable top-N (`encode_key_into` cannot
  fail any more). Integration audit: hash GROUP BY/DISTINCT/JOIN already
  folded NaN/-0.0 (unchanged); `.pink`/Vortex/inlined lake statistics already
  exclude NaN from min/max and gate DOUBLE extremes on `nan_count == 0`, so
  lake zone maps stay sound; **Parquet row-group pruning** had no NaN count and
  became unsound for `>`/`>=`/`= NaN` on DOUBLE under the new order (a group
  with max 3.0 and a NaN row was pruned for `d > 5.0`) — it now keeps DOUBLE
  groups for those predicates (`<`/`<=`/`=` non-NaN still prune). Display of a
  stored `-0.0` is unchanged (DuckDB's ORDER BY output prints such rows as
  `0.0`; pinkdb keeps the value's sign — a display-only difference, not
  probed). Tests: `indexes.rs` IndexScan-vs-scan over NaN payloads/±0/±inf for
  nine literals (fails before the fix), the comparison-kernel truth table +
  literal-on-either-side cases, NaN sort/top-N encoding (ASC/DESC/LIMIT vs the
  legacy comparator), the Parquet NaN row-group test, and a 21-probe duckdiff
  NaN battery (comparisons, BETWEEN/IN, ORDER BY ASC/DESC/LIMIT, MIN/MAX,
  GROUP BY, DISTINCT, `=`/`<`/`>=` joins, `rank()`); the stale "NaN = NaN out
  of scope" note in `math-nan-predicates` is gone. Gates: pinkdb **694**,
  workspace **1348**, parquet ext **26**, duckdiff **304/304** (default CLI
  features), fmt/clippy both toolchains, wasm green.

- 2026-10-21 — Wave 81: JavaScript procedure engine binding. `pinkdb-js`
  gains an `engine` feature (enabled by `pinkdb/js`) binding **quickjs-ng**
  through `rquickjs` 0.14 (vendored C, no bindgen). Every `CALL` gets a fresh
  runtime with step (interrupt ticks of 10,000 ops), host-call, memory
  (64 MiB) and stack (256 KiB) limits; JavaScript calls nest at most 4 deep.
  Intrinsics are BaseObjects + Eval + RegExp + Json + MapSet + Promise, and a
  lock-down script runs after the body compiles and before it is called:
  `eval`, `Function` and every function-kind `.constructor` throw,
  `queueMicrotask` and `Math.random` are gone. Two plan deviations, both
  forced by quickjs-ng: `Eval` must be installed for the host to compile
  anything, and `Promise` must be installed because creating an `async`
  function without it leaks and aborts at runtime teardown (jobs are drained
  inside the call, under the limits). The host (`crates/pinkdb/src/js.rs`)
  parses each `db.exec`/`db.query`/`db.value` as exactly one statement, binds
  its `?` placeholders as values (prepared-statement arity rules), refuses
  what a SQL body refuses (reworded for JavaScript), refuses non-query
  statements in `db.query` *before* running them, and runs them through the
  SQL body's statement path, now shared as `procedure::run_call_statement`
  (same replay checks and touched sets). `execute_call_inner` now shares
  argument coercion and the atomicity snapshot between languages. Bodies
  compile at `CREATE` time (syntax errors and non-identifier parameter names
  are refused then). `db.random()` is seeded by an FNV-1a hash of the
  definition, so WAL replay draws the same stream (tested end to end through
  reopen and checkpoint). Values: integers within ±2^53 are numbers, larger
  ones `BigInt` both ways (exact; `BigInt::to_i64` wraps, so the conversion
  goes through decimal text); DATE/TIMESTAMP/DECIMAL/INTERVAL cross as text.
  `QueryResult::Called` gains `output` (`print` lines, nested calls in
  order); the CLI renders them first, through the terminal sanitizer; the
  server protocol and FFI do not carry them yet (follow-up, would need a
  protocol bump). One contained `unsafe` block in `pinkdb-js`: a
  lifetime-erased host pointer guarded for the duration of the call, because
  `rquickjs` only accepts `'static` closures. wasm: `--features js` compiles
  for wasm32-unknown-unknown but no wasm test runner is installed, so `js` is
  documented native-only. Sizes: release CLI **+1,110,208 B** with `js`
  (matches the ~1.1 MB estimate), default **+160 B** (the `output` field);
  `cargo tree` confirms the default CLI does not link `rquickjs` (now a CI
  check). CI: a `js` feature-test entry and three clippy rows. Tests:
  `pinkdb-js` engine suite (10: conversions, every denied intrinsic and
  compile route, host calls, catchable host errors, CREATE-time checks,
  seeded random, step/statement/memory/stack limits, async jobs, no shared
  state), `crates/pinkdb/tests/js_procedures.rs` (9: write/read/print/
  return, injection-safe params, atomicity inside and outside a
  transaction, host-call refusals, CREATE-time checks, exact types, runaway
  scripts, nesting + nested output, durable replay through reopen and
  checkpoint), a CLI rendering test, and the two wave-44 refusal tests now
  prove both builds. Gates: pinkdb **701** (default) / **710** (`js`),
  `pinkdb-js` **3** / **12** (`engine`), CLI `js` **82**, workspace **1355**,
  `encryption` **745**, duckdiff **304/304**, fmt/clippy both toolchains
  (incl. the new `js` rows), wasm default + `js` compile. Follow-ups:
  `print` output over the protocol/FFI, wasm verification, a `js` CLI
  variant in the release matrix.

- 2026-10-21 — Wave 83: procedure transaction control + `EXCEPTION`
  handlers + `DO` blocks. **Transaction control:** a SQL body may `COMMIT`
  / `ROLLBACK` only in the outermost procedure of a `CALL` that runs outside
  any transaction (the runners set `standalone_call` around that one
  statement); inside `BEGIN` the error says the transaction "was started at
  a different scope", and nested procedures, `DO` blocks, JavaScript bodies
  and blocks with an `EXCEPTION` handler (checked at `CREATE`) are refused.
  `COMMIT` moves the call's restore point; `ROLLBACK` returns to it; a later
  failure keeps the committed work. **WAL:** a standalone `CALL` already
  runs under the lease (never optimistic). With transaction control, each
  `COMMIT` appends the pending **segment** — every writing statement since
  the last commit, rendered as `DO 'BEGIN DECLARE <var> <type> DEFAULT
  <exact literal>; …; <statement source>; END'` (innermost binding per
  name, `value_literal` from prepared statements) — and the runner logs the
  final segment instead of `CALL p(args)` (`ProcFrame::segment_log`, both
  the embedded runner and `Connection`). Replay never re-runs the
  procedure, so it is exact even when the procedure is later redefined or
  dropped (tested) and robust to interleaved frames. **`EXCEPTION WHEN
  OTHERS`:** a whole-store savepoint at block entry; on error the block's
  data/catalog effects and pending-segment entries are undone, the
  handler runs with `SQLERRM`, variables keep their values (plpgsql); a WAL
  fail-stop is never caught; the touched set keeps the failed statements'
  reads. **`DO`** (public, Postgres syntax) binds to `LogicalPlan::Call`
  with an `inline` definition, so RBAC (ADMIN), atomicity and logging match
  `CALL`; refused inside procedure bodies. Body statements now keep their
  full `source` text. Fixed two pre-existing bugs in the body's
  identifier reader (byte-wise decoding of quoted names; a trimmed
  remainder that made `DECLARE "x" …` always fail). **Stack:** snapshots are
  built in `#[inline(never)]` helpers so the recursive `exec_stmts` frame
  stays small; debug builds still lose ~1 level of recursion headroom on a
  2 MiB thread, and the pre-existing debug overflow below `MAX_CALL_DEPTH`
  is recorded in `docs/GAPS.md` (release: 32 guarded levels fit in 512
  KiB). Tests: `crates/pinkdb/tests/procedure_txn.rs` (10: handler undo +
  SQLERRM + variables kept, nested/rethrow/RETURN, CREATE-time handler
  checks, guarded recursion, commit/rollback/failure, different-scope /
  nested / DO refusals, DO atomicity and `do` as a name, segment replay
  after the procedure is dropped and redefined + checkpoint, exact replay
  of NaN/-0.0/DECIMAL/DATE/INTERVAL/i64::MIN/quoted unicode names with a
  rolled-back guarded block, the `Connection` path). Gates: pinkdb **711** /
  **720** (`js`), workspace **1365**, `encryption` **755**, CLI `js`
  **82**, duckdiff **304/304**, fmt/clippy both toolchains (incl. `js`
  rows), wasm green; default CLI **3,972,864 B (+35,888)**. Follow-ups:
  named exception conditions/SQLSTATE, `SAVEPOINT` SQL, `COMMIT AND CHAIN`,
  transaction control in JavaScript bodies and `DO`, stack-measured
  recursion bounds.

- 2026-10-21 — Wave 84: query execution profiles. New `pinkdb::profile::
  QueryProfile` and `execute_profiled` on `PinkDb` and `Connection`: one
  shared runner per front end takes an optional profile sink, so `execute`
  reads no clock and allocates nothing (measured 21,155 vs 21,185 ns/stmt
  against the pre-wave runner on 40,000 tiny statements; the profiled run
  costs +2.1%). Phases: `parse` (once per SQL string, reported on every
  statement and added to the first one's `total`), `bind` (includes logical
  planning), `optimize` (always 0 on the `Connection` path, which has no
  optimizer pass), `execute` (lease wait, refresh/rebind and the optimistic
  conflict retry included), `log` (WAL append + auto-checkpoint), `total`;
  facts: rows/affected count, columns, `QueryResult` kind, transaction state,
  engine version. WAL files are byte-identical with and without profiling
  (header incarnation id/CRC normalized in the test). Protocol **v10**:
  `SetProfiling { enabled }` → `ProfilingSet`, then one `Profile` frame after
  each statement's `Done` (also for `ExecutePrepared`), in the sync and async
  servers through one shared encoder; the client gains `set_profiling` /
  `execute_profiled`; v9 `Hello`s are refused. CLI: `.timer on` keeps the
  historical `Run Time (s): real X.XXX` prefix and appends `(parse …, bind …,
  optimize …, execute …, log … ms)`, locally or from the server's profiles.
  FFI: `pinkdb_set_profiling` and per-statement `pinkdb_result_profile_*`
  accessors (ABI stays 1, additive). **Delegated build**: per the user's
  workflow, T1 engine and T4 FFI ran on OpenCode, T3 protocol and T2 CLI on
  CommandCode (DeepSeek V4.1 Flash, each confined to its crate); review fixes
  here: the CLI counted the shared `parse` once per statement (now once),
  and the `Connection` docs wrongly said bind+optimize were measured
  together. Tests: `crates/pinkdb/tests/profile.rs` (6 + an ignored overhead
  report), server protocol/server tests (round trips, malformed `Profile`
  rejection, wire order, v9 refusal), FFI tests (off by default, batches,
  prepared, NULL/out-of-range), CLI `tests/timer.rs` (3) + a remote timer
  test. Gates: workspace **1384**, pinkdb **717**, `encryption` **761**,
  `js` **726**, server `tls` **112** / `async-server` **121** / `ui`
  **102**, CLI `client` **92** / `client,tls` **97**, duckdiff **304/304**,
  fmt/clippy both toolchains, wasm, FFI C example; default CLI **3,976,256
  B (+3,392)**.

- 2026-10-21 — Wave 85: browser playground + storage. **Engine:** public
  `PinkDb::from_pink_bytes` (wrapper over the private loader; round trip and
  refusals tested). **`crates/pinkdb-wasm`:** a `cdylib` with a raw ABI
  (`pw_alloc/free/open/close/exec/snapshot/restore/version`, length-prefixed
  buffers, hand-escaped JSON with BIGINT beyond 2^53 and non-finite DOUBLEs
  as strings, `catch_unwind` on every export, `unsafe` confined to
  pointer/length marshaling) — no wasm-bindgen (none installed; the roadmap
  allows plain exports). No core change was needed to run on wasm (rayon
  1.13 falls back to one thread; plain `execute` reads no clock); one
  crate-level `cfg_attr(wasm32, allow(clippy::drop_non_drop))` because
  `File` is an inert stub there. Module **2,359,016 B**.
  `website/tools/build-wasm.sh` + `wasm-smoke.mjs` (10 checks under Node).
  **Site:** `playground.html` (top-nav entry) with `pinkdb.js` (loader),
  `worker.js` (engine + storage in a module worker, debounced autosave after
  non-SELECT results, export as a transferable, Web Locks ownership with a
  read-only memory copy in other tabs, refused snapshots kept untouched),
  `storage.js` (memory; IndexedDB in one readwrite transaction; OPFS via
  `createSyncAccessHandle`, temp file + `move()`, else versioned files + a
  pointer written last), `playground.js/.css` (examples, result grid via
  `textContent`, status with `storage.estimate()`, Keep data →
  `storage.persist()`). `deploy.sh` builds the module first. **Delegated:**
  T1 hung for 55 min on OpenCode (no output/changes) and was re-run on
  CommandCode; T2 on CommandCode. **Verified in the desktop app's
  Chromium** against `wrangler dev`: four examples; Memory/IndexedDB/OPFS
  persistence across reloads (OPFS left only `db.pink`); corrupt import
  refused ("not a .pink file (bad magic)") with the saved snapshot
  byte-identical; valid import + Export byte-identical (`PINKDB05`);
  second tab read-only; no console errors; no overflow at 375 px. Review
  fix: Import's tooltip repeated Save's. CI's wasm job builds the module
  fresh and runs the smoke test. Gates: workspace **1392**, `pinkdb-wasm`
  **7**, `encryption` **762**, duckdiff **304/304**, fmt/clippy both
  toolchains + clippy for wasm32 (`pinkdb-wasm`), wasm core build; default
  CLI unchanged (**3,976,256 B**). Follow-ups: WAL-grade OPFS VFS,
  wasm-bindgen/size optimization (wasm-opt), `print`/profile output in the
  playground.

- 2026-10-21 — Wave 86: extension packages (table functions + COPY sinks,
  linked model). New `crates/pinkdb/src/package.rs`: `ExtensionPackage`
  (`with_scalars` / `with_table_source` / `with_file_format`) and a
  process-global, case-insensitive registry. `LOAD EXTENSION '<name>'`
  resolves dlopen → package → scalar-only linked registry (wasm: package →
  linked); activation is atomic (scalar, source and format names checked
  against built-ins, global registrations and earlier loads before anything
  is installed) and **per store** (store-local package sources/formats are
  consulted alongside the global ones); a second load is refused. The
  `LOAD EXTENSION` opt-in gate is unchanged. Replay resolves registered
  package names; the binder records package-provided table functions,
  replacement scans and COPY sinks in its "extensions used" list, so the
  WAL's existing `LOAD EXTENSION` prefixing covers them (a checkpointed
  generation re-loads the package before a new record that reads through
  it). `pinkdb_arrow::package()` / `register_package()` offer Arrow as a
  package (third model beside `register(db)` / `ensure_registered()`; a
  process picks one, since a global registration makes the package's LOAD
  collide). Extensions guide + README updated. **Delegated:** T1 on
  CommandCode; T2's SDK target was wrong (the SDK depends only on the
  loader) and moved to the arrow crate; OpenCode hung again (20 min,
  `--standalone`), CommandCode finished it. **CI follow-ups found when the
  user's push of waves 79–85 was re-run by hand** (GitHub's degraded
  webhooks had skipped the push): the docs job failed because CHANGELOG
  edits had not been regenerated into the site, and a `vortexlake`-only
  test still expected `MAX(d) = inf` for a NaN-holding DOUBLE column (now
  `NaN`, DuckDB order, after the NaN fix). Fixed both and prevented the
  first class: `website/build.py` now regenerates the changelog page
  itself, `scripts/check-website.sh` is the one check CI and the new
  `scripts/git-hooks/pre-commit` hook both run (also catching untracked
  generated files). Gates: workspace **1403**, pinkdb **727**,
  `encryption` **771**, `js` **736**, parquet ext **26**, and CI's full
  feature matrix run locally — CLI `vortexlake,s3` **127**,
  `vortexlake,s3,encryption` **128**, `pinklake` **92**, `pinklake,s3`
  **96**, `s3` **85**, `encryption` **84**, `client` **92**, `client,tls`
  **97**, `repo` **129**; pink-lake `s3` **141**, `s3,vortexlake` **222**,
  `s3,encryption` **172**, `s3,vortexlake,encryption` **253** — duckdiff
  **304/304**, fmt/clippy both toolchains + wasm32, playground module
  rebuilt (2,376,804 B) and smoke-tested; default CLI **3,996,512 B**.

- 2026-10-21 — Wave 87: lake format providers + Parquet. **Seam (T1):**
  `crates/pink-lake/src/format.rs` — public `LakeFormat` (`id`,
  `extension`, `write` → bytes + exact `SavedTableStats`, `write_streamed`
  (Vortex streams), `read`, optional `footer_stats`, capabilities
  `has_footer_stats` / `supports_projection`), `EncodedFile`, `DataFile`,
  `FileFooter`; a process-global registry lazily seeded with `pink` and
  (`vortexlake`) `vortex`; `register_lake_format` refuses duplicates and
  malformed ids. `catalog::FileFormat` became a `#[serde(transparent)]`
  newtype over the id, so existing catalogs are byte-identical (v1 entries
  without `format` still mean `pink`). Every former enum match site in
  attach / lib / maintenance / incremental / vortex_file dispatches through
  the provider or a capability; an unknown id fails open/load/attach/
  maintenance with an error naming it (keeping the established
  "corrupt lake metadata" prefix). `Lake::set_write_format(id)`. Test
  provider `pinkwrap` exercised end to end. No existing test changed.
  **Parquet (T2):** `extensions/parquet/src/write.rs` — flat OPTIONAL
  schema, definition-level NULLs, 64K-row row groups, chunk statistics;
  BOOLEAN/INT32/INT64/DOUBLE (bit-exact)/UTF8/DATE/TIMESTAMP micros/
  DECIMAL (INT32/INT64/FIXED16); INTERVAL refused (Parquet INTERVAL is
  millisecond-precision); table identity in footer key-value metadata.
  `crates/pink-lake-parquet` (`ParquetLakeFormat`, id/extension
  `parquet`, statistics from the engine's exact `table_stats`, manifest
  statistics at attach). CLI `parquetlake` feature + `.lake format [<id>]`
  (lists registered ids; applies to later saves/commits/flushes and
  writable attachments). Tests: write round trips through the existing
  reader for every type and edge value (i32/i64 extremes, NaN, -0.0, ±inf,
  unicode, all-NULL, empty, 70K-row multi-row-group), Parquet lakes (multi
  db/table save → open equality, attach COUNT/MIN/MAX = open, append,
  merge, INTERVAL refusal publishing nothing), CLI `.lake format` tests.
  Both tasks delegated to CommandCode (DeepSeek V4.1 Flash), reviewed here.
  Gates (CARGO_INCREMENTAL=0, after a disk-space cleanup): workspace
  **1418**, `encryption` **771**, parquet ext **31**, pink-lake `s3`
  **147** / `s3,vortexlake` **228** / `s3,encryption` **178** /
  `s3,vortexlake,encryption` **259**, CLI `parquetlake` **98**,
  `vortexlake,s3` **127**, `vortexlake,s3,encryption` **128**, `pinklake`
  **92**, `pinklake,s3` **96**, `s3` **85**, `encryption` **84**, `client`
  **92**, `client,tls` **97**, `repo` **129**, `js` **85**; duckdiff
  **304/304**; fmt/clippy both toolchains; wasm + playground smoke
  (module unchanged); default CLI **3,996,512 B** (unchanged).

- 2026-10-22 — Wave 88: mixed-format lakes. **T1 (pinning + per-table
  refusal):** catalog entries gain `format_pinned` (serde-default, omitted
  when false — default-written catalogs stay byte-identical); full-save
  resolution = per-table choice (pinned) > pinned or non-builtin entry >
  handle write format (pinned iff `set_write_format` was called); appends
  keep format + pin; `rewrite_table_format` / `CompactionKind::Convert` pin.
  v4–v6 entries with an unregistered id or `vortex` without `vortexlake` no
  longer refuse the manifest: `Lake::unavailable_tables[_at]`, `load` skips,
  attach lists them with `UNAVAILABLE_ROWS` (u64::MAX, which the engine's
  stats fast paths already refuse) and every read errors naming table + id,
  full saves carry them verbatim (refused if the store has that key),
  commits/targeted maintenance refuse, lake-wide maintenance skips
  (`CompactionReport::skipped`); v2/v3 keep the whole-lake refusal. CLI
  warning lines on `.lake open`/`attach`. **T2 (contract):**
  `tests/common/format_contract.rs` — every type with NULL/NaN/±0.0/±inf/
  extremes, empty/all-NULL/70K-row tables, refusal publishes nothing, attach
  COUNT/MIN/MAX = open, append keeps format, rewrite round trip, pin flag;
  run by pink-lake (pink, vortex) and pink-lake-parquet (all three). It
  found two bugs, fixed here: Vortex `encode_table_into` panicked on
  DECIMAL/INTERVAL (metadata built before the refusal), and the stats codec
  decoded INTERVAL text through the f64 SQL parser (`i64::MIN` micros
  undecodable → attach refused; >2^53 rounded) — an exact integer decoder
  now runs first, text unchanged. Both tasks delegated to CommandCode
  (DeepSeek V4.1 Flash), reviewed here. Gates (CARGO_INCREMENTAL=0):
  workspace **1487**, pink-lake `vortexlake` **193** / `s3` **151** /
  `s3,vortexlake` **231** / `s3,encryption` **182** /
  `s3,vortexlake,encryption` **262**, pink-lake-parquet **10**, CLI
  `vortexlake,s3` **128**, `vortexlake,s3,encryption` **129**, `pinklake`
  **92**, `pinklake,s3` **96**, `s3` **85**, `encryption` **84**, `client`
  **92**, `client,tls` **97**, `repo` **129**, `js` **85**, `parquetlake`
  **99**; fmt + clippy on both toolchains. Engine crates untouched (no
  wasm/duckdiff change). Three CLI tests were killed mid-gate once (a
  relinked binary on macOS) and passed on every rerun.

- 2026-10-22 — Wave 89: lake replication jobs. Re-planned from a whole-lake
  byte mirror to ducklake-vortex's job model after reviewing
  `ducklake_replication.cpp` (mirror + share mode deferred). **T1:** catalog
  `table_id` (assigned on create, preserved per key, minted on a published
  drop + recreate, back-filled on any write; serde-optional), the
  `replication` module — `replication/state.json` in the destination
  (conditional `put_new`/`put_if_match` with bounded retry, sealed when
  keyed), `create`/`drop`/`jobs`/`tables`/`pause`/`resume`/`resume_all`/
  `versions_behind`/`catch_up`, strategies watermark (boundary replay, NULL
  refusal, backwards → reseed), snapshot (`table_changes` per version, insert
  de-dup + delete match, multiset verification, reseed on full-save versions /
  expired history / identity change / mismatch), merge (PRIMARY KEY diff,
  unchanged rows keep position), full; every write one
  `commit_from_tables` for that table. **T2:** `Supervisor`, the `.lake
  replicate` CLI (10 commands), README section, and a process-wide lock
  serializing in-process state writers (a local conditional write's
  tag-check → rename window let a cycle's status write overwrite a `pause`).
  **Review fixes:** removed a `replica` byte-mirror module added by an
  orphaned agent process from the abandoned first plan; replica value
  matching made bit-exact (SQL equality would keep `0.0` for a source
  `-0.0`); snapshot apply made linear (row-position index instead of a scan
  per change). Both tasks delegated to CommandCode (DeepSeek V4.1 Flash).
  Gates (CARGO_INCREMENTAL=0): workspace **1509**, pink-lake `s3` **177** /
  `s3,vortexlake` **257** / `s3,encryption` **209** /
  `s3,vortexlake,encryption` **289** / `vortexlake` **215**,
  pink-lake-parquet **10**, CLI `vortexlake,s3` **132**,
  `vortexlake,s3,encryption` **133**, `pinklake` **92**, `pinklake,s3`
  **96**, `s3` **85**, `encryption` **84**, `client` **92**, `client,tls`
  **97**, `repo` **129**, `js` **85**, `parquetlake` **99**; fmt + clippy on
  both toolchains. Engine crates untouched (no wasm/duckdiff change). Also
  this session: R2 buckets `pinkdb-bench` / `pinkdb-ci` (CI expiry rule),
  GitHub `R2_*` secrets/variables, `scripts/r2-credentials.sh`, and the
  TPC-H benchmark data staged in R2 (see wave 98).

- 2026-10-22 — Wave 90 final gate: first real release. main pushed
  (b0be682, CI + docs green), annotated tag `v0.1.0` pushed. **Release**
  (run 39726636556, 17m52s): 36 build/smoke jobs + the immutable publish all
  green on real runners — GitHub release `v0.1.0`, 36 tarballs + 36
  `.sha256` + `SHA256SUMS`. Verified locally: the darwin-aarch64 default
  tarball matches `SHA256SUMS`, `pinkdb --version` = 0.1.0, queries and the
  json extension run; `install.sh --version 0.1.0 --variant pinklake-s3`
  installs into a temporary prefix with checksum verification and PATH
  guidance, and the unpinned install resolves the live release. **Publish
  extensions** (run 39726636522): all four platform builds green, the publish
  step failed — `publish.py` read the live catalog with urllib's default
  `Python-urllib` user agent, which Cloudflare answers with 403 (curl, `ureq`
  — the CLI's `INSTALL` client — and custom agents get 200). Fixed in
  d20d7a4 (explicit `User-Agent`; `platform=all` manual dispatch so a failed
  tag publish can be redone from main). The `platform=all` dispatch
  (run 39728577930) then published and signed the catalog — 16 entries,
  `cpp_ext`/`math_ext`/`string`/`string_ext` × darwin-arm64/darwin-x64/
  linux-arm64/linux-x64 — but its verify step got 404 for `/index.json.sig`:
  the job deployed the Worker only after verifying through it, and the live
  Worker predated the signature route. 118a58c deploys first; the re-dispatch
  (run 39729164421) deployed the current Worker. Verified from here: live
  `index.json` + `index.json.sig` both 200, `verify.sh` signature OK (key
  `c84e54bfea6b2d09`), and all 16 artifacts' bytes match the signed catalog's
  SHA-256.

- 2026-10-22 — Wave 94: three-part column references. `Expr::Column` gained
  `db`, the star forms carry `{db, table}`, `ast::SetTarget` for qualified
  `UPDATE … SET` targets; the binder's `ScopeTable` records the original
  table, its database and whether its name is an alias, and resolves
  `db.t.c` against unaliased base tables/views of that database (unqualified
  FROM entries belong to the current database). Old behaviors found and
  changed: `FROM db1.t JOIN db2.t` was refused by the duplicate-name guard
  and a two-part `t.c` bound whichever same-named entry had the column — now
  admitted, with `t.c`/`t.*` ambiguous and `SELECT *` expanding through
  three-part names; qualified SET targets were parse errors. Four-part
  references are refused by a parser marker turned into an error by the AST
  walk (review fix: the quoted reference kept trailing whitespace). CommandCode
  failed three times at startup ("Unable to connect to the API") while short
  runs worked, so the wave ran on OpenCode (DeepSeek V4.1 Flash), reviewed
  here. Gates (CARGO_INCREMENTAL=0): workspace **1526** (pinkdb lib 577 +
  `three_part_columns` 17), CLI matrix `vortexlake,s3` **132** /
  `vortexlake,s3,encryption` **133** / `pinklake` **92** / `pinklake,s3`
  **96** / `s3` **85** / `encryption` **84** / `client` **92** /
  `client,tls` **97** / `repo` **129** / `js` **85** / `parquetlake` **99**,
  pink-lake `s3` **177** / `s3,vortexlake` **257** / `s3,encryption` **209**
  / `s3,vortexlake,encryption` **289** / `vortexlake` **215**,
  pink-lake-parquet **10**; fmt + clippy on both toolchains, wasm32, duckdiff
  **367/367** (reference still the 2.0 alpha).

- 2026-10-22 — Wave 98a: fast remote Parquet (pulled ahead by the user after
  the first R2 baseline: SF1 Q6 406 s vs DuckDB 2.0.0's 3.9 s, `count(*)`
  29.5 s vs 0.41 s). Cause: the remote `ChunkReader` streamed every column
  chunk as serial 128 KiB range requests (projection pushdown itself was
  fine; EXPLAIN only printed the schema width). Now: footer-only `count(*)`,
  per-row-group projected byte spans coalesced (1 MiB gap, 64 MiB cap), a
  128 MiB concurrent read-ahead window with ordered parallel decode,
  `PINKDB_HTTPFS_PARALLELISM` (default 8), per-client idle pools, transport
  counters, `(N of M columns)` in EXPLAIN. Local test server: 20 → 2 data
  requests. R2 (median of 3): SF1 `count(*)` 0.62 s (DuckDB 0.41), Q6
  4.26 s (3.87), Q1 4.15 s (5.41); SF10 `count(*)` 0.63 s (0.65), Q6 28.9 s
  (25.9), Q1 35.7 s (33.1) — bandwidth-bound at SF10 on this link. Lakes vs
  DuckLake (SF1, R2): attach + Q6 22.9–31.7 s vs 3.9 s (our lakes download
  whole data files), attach + `count(*)` 2.75 s vs 0.07 s (catalog on R2 vs a
  local DuckLake catalog) → wave 98b. A Vortex-lake reading of 53/98 s was
  transient (A/B old vs new binary: 15–17 s both). Delegated to OpenCode
  (DeepSeek V4.1 Flash), reviewed here. Gates (CARGO_INCREMENTAL=0):
  workspace **1526**, pinkdb-httpfs (all features) **50**, pinkdb-parquet
  (all features) **50**, CLI `parquet,httpfs` **89** / `arrow,parquet,httpfs`
  **91** and the full CLI/lake matrix unchanged, fmt + clippy on both
  toolchains, wasm32, duckdiff **367/367**. Benchmark data and DuckDB 2.0.0
  (official, digest-verified) are described under wave 98; the server-vs-Quack
  comparison is deferred until the lake work lands
  (`crates/pinkdb-server/examples/remote_client_bench.rs` is its client).

- 2026-10-22 — Wave 98b: fast lake reads on object storage. `Storage`
  gained ranged reads (local positioned, S3 range GET; keyed sessions refuse
  ranged reads of sealed objects and fall back to whole reads), transport
  counters (`Lake::transport_stats`, `AttachmentReport`, CLI `Transport:`),
  `DataFile::read_range`; the 98a remote-Parquet scan core moved into
  `extensions/parquet/src/ranged.rs` behind a transport-agnostic
  `RangeSource`, which the Parquet lake provider now uses (projection +
  pruning); Vortex footers/segments are ranged and attach prefetches all
  footers concurrently; multi-file tables decode through `map_bounded`
  (bounded, order-preserving); whole objects download in parallel parts.
  R2 SF1 (median of 3) vs DuckLake: attach + Q6 Parquet 22.9 → 6.2 s, Vortex
  31.4 → 4.5 s, `.pink` 31.7 → 20.4 s (DuckLake 1.9 s this run, 3.9 s
  earlier); attach + `count(*)` 1.8–2.2 s (Vortex was 15.5 s) vs 0.07 s.
  Agent run: OpenCode (DeepSeek V4.1 Flash); the first run's CLI dropped but
  its session continued server-side next to a continuation run (stopped),
  then completed the wave. Gates: workspace **1613**, pink-lake `s3` **179**
  / `s3,vortexlake` **263** / `s3,encryption` **211** /
  `s3,vortexlake,encryption` **295** / `vortexlake` **216**,
  pink-lake-parquet **14**, pinkdb-parquet **50**, pinkdb-httpfs **50**, CLI
  matrix unchanged (`parquet,httpfs` 89, `arrow,parquet,httpfs` 91), fmt +
  clippy on both toolchains, wasm32, duckdiff **367/367**. Also fixed:
  `remote_client_bench.rs` (committed in e9d5275) was not rustfmt-clean.

- 2026-10-22 — Wave 102 T1: one `.pink` format, `PINKDB06`. Layout: row
  groups of 122,880 rows, column chunks (page index + pages encoded with the
  per-chunk encoders), CRC32C per chunk and for the footer, exact per-chunk
  stats, footer carrying all store metadata, 20-byte trailer; FastLanes
  bit-packing (tag 10), FSST (tag 9), `lz4_flex` second stage by measurement;
  v1–v5 readers/fixtures/compat tests deleted with an "older pinkdb" refusal;
  WAL/checkpoint/`PINKENC1`/wasm playground on the new format. Delegated to
  OpenCode `opencode-go/minimax-m3.1-flash-free` (CommandCode on MiniMax M3.1
  Flash made no edits in 10 min and was cancelled). Review fix: DECIMAL chunks
  were 16 raw bytes/value — the agent's 271.6 MB came from a CSV-typed,
  7-table set; through the CLI from Parquet the file was 706 MB. DECIMAL
  (p ≤ 18) now uses the BIGINT encodings → **320.8 MB** for SF1 (v5 713 MB,
  DuckDB Parquet ≈ 324 MB); `tests/decimal_storage.rs`. Also `crc32c` →
  `as_chunks` (stable clippy 1.99) and two legacy-fixture CLI tests deleted.
  Gates: workspace **1615**, CLI `vortexlake,s3` **130** / `pinklake` **91**
  and the rest of the matrix green, pink-lake matrix green, fmt + clippy on
  both toolchains, wasm32 + playground smoke 10/10, duckdiff **367/367**.

- 2026-10-22 — Wave 102 T2: lakes on `PINKDB06`. Catalog `format_version` 7
  only (`MIN = MAX = 7`, older catalogs refused with "written by an older
  pinkdb"); `.pink` file entries carry `footer_offset` / `footer_len` /
  `footer_crc32c`; the `pink` provider reads the footer at that location,
  prunes row groups by exact chunk stats, and fetches only the projected
  chunks via coalesced, bounded concurrent ranges, decoding in parallel in
  order; the wave 88 hybrid rule and `format_pinned` removed; legacy fixtures
  (`lake_v1`, `lake_v1_v4`, `lake_v1_v4_stats`, `lake_v2`) and every test on
  them deleted. Agent: OpenCode `minimax-m3.1-flash-free` stopped early twice
  (the model then left the free tier) — finished here. Review fixes: readers
  never received a file's positional deletes, so row-group pruning could have
  dropped the wrong rows (attach now passes deletes + footer; no pruning with
  deletes); a v7 manifest would have skipped `validate_rel_path` (the version
  match covered `4..=6`); the pruned-read row check relaxed to `<=` only for
  pruning providers with filters and no deletes. New
  `tests/pink_ranged_reads.rs` (mock S3): `COUNT(*)` no data request, a
  2-of-4-column query ranged only (~2.3 of 3.65 MB), pruning reads less.
  R2 SF1 vs DuckLake (median of 3): `.pink` lake attach + Q6 20.4 → **4.56 s**
  (DuckLake 2.20 s), Q1 19.5 → 4.25 s (2.36 s), `count(*)` 2.05 s (0.08 s,
  catalog round trips), lake 680 → 321 MB, save 49.7 → 29.2 s; lakes
  re-saved under `lakes/v7/`. Gates: workspace **1613**, CLI matrix green
  (`vortexlake,s3` 129, `vortexlake,s3,encryption` 130, `pinklake` 90,
  `pinklake,s3` 94, `parquetlake` 97, the rest unchanged), pink-lake `s3` 177 /
  `s3,vortexlake` 258 / `s3,encryption` 209 / `s3,vortexlake,encryption` 290 /
  `vortexlake` 208 (and the new test), pink-lake-parquet 13, fmt + clippy on
  both toolchains, wasm32, duckdiff **367/367**. Also measured today (agent
  models, 250-word prompt): Beam free 7–22 s, DeepSeek V4.1 Flash 19–26 s
  (paid = free), Z.AI GLM-5.3 / 5.3-flash 46–82 s; GLM-5.3-highspeed not in
  the plan.

- 2026-10-22 — Wave 91: public Docker image. Delegated to OpenCode
  `opencode/deepseek-v4.1-flash-free` (the user's model choice; it completed
  without the paid fallback). `Dockerfile` + `.dockerignore`,
  `docker/entrypoint.sh`, `pinkdb-server --health-check` +
  `Client::connect_timeout`, `scripts/docker/smoke.sh`,
  `.github/workflows/docker.yml`, website Docker page, README +
  `docs/releasing.md`. Runtime measured: static musl 36.6 MB vs glibc 170 MB;
  `lake` 101 MB (dynamic musl: `bindgen` needs libclang). Review fixes: CI
  built arm64 under QEMU on every push/PR (hours) → native amd64 build + smoke
  on relevant changes only, emulated arm64 only at publish; the health check
  spoke plaintext even when the container serves TLS → TCP-accept probe with
  TLS configured (+ test). Verified here: both images built (arm64) and
  smoke-tested (healthy, UID 10001, health-check codes, remote CLI, restart
  persistence, lake round trip); `pinkdb-server` 108 tests, clippy on both
  toolchains, fmt, actionlint, website check.

- 2026-10-24 — Wave 105: agents and MCP, no model code in pinkdb. Extension
  ABI minor 2 keeps major 1: `PinkdbHostApi::execute_statement` (SQL text in,
  agent Markdown or a JSON error out; limits, grants and the read-only policy
  stay in the host) and the optional `pinkdb_ext_serve` export (transport
  descriptor `stdio` or `http:HOST:PORT` plus an options string). Both are
  detected by symbol presence and `struct_size`, so every v1 extension loads
  unchanged; the loader refuses path escapes as before and now refuses to load
  or serve a library twice at once. New `extensions/mcp` (`pinkdb-mcp`, cdylib
  over `serde_json`): JSON-RPC 2.0 `initialize`/`ping`/`tools/list`/
  `tools/call`, stdio and loopback-only streamable HTTP, eight schema'd tools
  (`query`, `describe`, `tables`, `databases`, `explain`, `lake_tables`,
  `lake_versions`, `lake_changes`), answers rendered by `pinkdb::agent` under
  `PinkDb::execute_with_limits`; lake reads are `.lake` text routed back
  through the host hook, so the extension never links pink-lake. Hosts:
  `pinkdb --mcp FILE` (installed bridge, stdio), `.mcp serve [stdio|http
  PORT] [allow-writes]` after `LOAD EXTENSION`, and `pinkdb-server
  --mcp-extension PATH [--mcp-port 7801] [--mcp-user NAME]` under the
  server's logins/RBAC. Read-only default refuses DML/DDL, `COPY … TO`,
  `LOAD EXTENSION`, write-mode `ATTACH`, lake maintenance and security `SET`
  in the host callback; `allow-writes` lifts the policy, never the budgets.
  No pi package (user: not needed): pi, Claude Code and any MCP client
  connect with `<client> mcp add pinkdb -- pinkdb --mcp store.pink`. Tests: loader C serve extension 6, engine
  `tests/serve.rs` 8, `extensions/mcp` protocol/transport 11, CLI end-to-end
  `tests/mcp.rs` 5 (TPC-H top-3, every refusal, truncation, HTTP grammar);
  workspace 1,749 tests / 154 suites, 0 failed. Release: `pinkdb` 4,221,296 →
  4,260,048 B (+0.9%), `pinkdb-server` 3,802,464 → 3,927,120 B (+3.3%),
  `libpinkdb_mcp.dylib` 565,824 B; wasm32 build green.
- 2026-10-24 — Wave 100: triggers. Syntax `CREATE [OR REPLACE] TRIGGER name
  {BEFORE|AFTER} {INSERT|UPDATE [OF cols]|DELETE} [OR …] ON t [REFERENCING
  OLD TABLE AS o NEW TABLE AS n] [FOR EACH {ROW|STATEMENT}] [WHEN (cond)]
  EXECUTE {PROCEDURE p(args) | <statement>}`, `DROP TRIGGER [IF EXISTS] name
  [ON t]`, `SHOW TRIGGERS [ON t]`; AST statements + a `TriggerDef` catalog
  entry (`crates/pinkdb/src/trigger.rs`) and three `LogicalPlan` variants
  (`CreateTrigger` boxes the definition — a whole `Statement` payload pushed
  the enum over clippy's `large_enum_variant`). Bodies are not a parallel
  interpreter: `trigger::run_trigger` installs the row images on the store,
  seeds `db.trigger_scope.tables` for transition tables, and drives the
  statement through `procedure::run_call_statement` (bind → optimize →
  `exec::run`), so `OLD.col`/`NEW.col` are `ParamSlots` variables resolved by
  the ordinary column arm of the binder and `REFERENCING` aliases are ordinary
  CTEs (`Binder::ctes` + `Cx.ctes` seeding — a transition table is just a
  materialized relation, no new plan node). `SET NEW.col := …` parses as a
  dotted target in the procedure-body parser and writes back through
  `trigger::assign_row_column` so the coerced value is what the DML applies.
  Order: creation order per event, all BEFORE then the change then all AFTER.
  Recursion `MAX_TRIGGER_DEPTH = 16` with an explicit cycle hint; statement
  atomicity via `procedure::boxed_snapshot` at depth 0 and
  `PinkDb::savepoint_restore` on error, so a failing trigger undoes the DML
  even in autocommit. Trigger-free DML is untouched: `exec::insert_rows` /
  `update_rows` / `delete_rows` return the original batch code when the table
  has no triggers and no transition capture; measured 1 M-row `INSERT …
  SELECT` 34.4–34.8 ms (HEAD 34.1–34.3 ms) and bulk `UPDATE` 19.9–20.4 ms
  (HEAD 20.3–20.5 ms). Persistence: `.pink` footer flags bit `0x80` with a
  per-database section after procedures, storing each definition's `CREATE
  TRIGGER` text in creation order (re-parsed on load, `corrupt file: trigger
  "…" does not parse` on damage). WAL: rules unchanged — the firing statement
  is logged, triggers never are, so replay re-fires exactly once (tested with
  a crash child that commits and `exit(0)`s, plus a replay that observes a
  dropped trigger); bodies are already covered by the logged-statement
  volatile-function refusal. RBAC: `CREATE`/`DROP TRIGGER` use the same
  database-`CREATE` check as `ALTER TABLE`/index DDL, and bodies run with
  invoker rights — `run_call_statement` re-checks every body plan against
  `db.trigger_invoker` (the firing statement's principal, set on the write
  paths), verified by a user who may write `t` but not `hidden`. Interactions:
  trigger-written tables reach the MVCC touched set because `mv::
  plan_writes_of`/`plan_sets_store` now always merge `proc_frame` writes (not
  just for `Call`), MVs refresh for triggered changes, `.lake commit`
  publishes trigger effects, and lake/`view` targets are refused. Refused and
  documented: views, attached lakes, `REFERENCING` on ROW/BEFORE triggers,
  unknown `UPDATE OF` columns, `RETURNING` (not in pinkdb), trigger DDL inside
  a transaction. Tests: `crates/pinkdb/tests/triggers.rs` 39 + 1 ignored crash
  child, CLI `tests/lake.rs` trigger attach/commit, `tests/macros.rs` unknown
  flag bit moved to bit 8. Gates green: fmt, clippy `-D warnings`, workspace
  1,796 tests / 156 suites, `pinkdb` 820, `pinkdb-cli` 100, `pinkdb-cli
  --features pinklake,s3,parquetlake` 168, wasm32 build.
- 2026-10-25 — Wave 101b: `VARIANT` in Parquet, shredding, path statistics.
  `crate::variant_shred` holds the spec codec (metadata dictionary with
  version/sorted-string/offset-size header, the value encoding's primitive,
  short-string, object and array layouts) and the shredding policy:
  `infer_policy` over all of the column's rows, a key shredded when it appears
  in ≥32 rows, in ≥50 % of them, with one non-NULL `ShredKind` per
  occurrence, at most 8 keys (most frequent first, ties by name). The writer
  keeps one rule of its own — a present-but-null or other-kind key stays in the
  residual `value` — so `{}` and `{"a": null}` survive a round trip, and the
  reader reconciles the buffers (`unshred_row`, the spec's "appears in both"
  error). Parquet: the extension writes the `Variant` logical type
  (specification version 1) with `metadata`/`value`/`typed_value`, one shared
  dictionary per row group, and reads it back including per-row dictionaries
  and partially shredded files; DuckDB 2.0 reads pinkdb files and pinkdb reads
  DuckDB files (`extensions/parquet/tests/variant_parquet.rs`, 4 tests,
  skipped without `/tmp/duckdb-2.0.0/duckdb`). Path statistics:
  `PathStats`/`PathStatsAcc` in `variant_shred`, `SavedPathStats` in the
  `.pink` stats block, path-qualified `PrunePredicate`s from
  `collect_prune_predicates` (`v['a']`, `v.a`, `variant_extract(v,'$.a')`),
  zone-map pruning in `pink-lake` and row-group pruning in the Parquet
  extension; a lake catalog stores one `ducklake_file_variant_stats` row per
  path per data file (DuckLake v1.1's own table), written and deleted with the
  file's other statistics on `pink:`, `sqlite:` and Postgres catalogs. Comparisons
  coerce the scalar side (`variant_coerce` in the binder). Extension ABI minor
  3 with `ExtType::Variant` (tag 10) as the opaque binary encoding, header +
  Rust SDK + `cf_variant` in all four SDKs. Measured: ten 100k-row Parquet
  files 11.9 MB shredded / 16.4 MB unshredded / 16.3 MB JSON text; a
  selective path filter 0.038 s vs 0.301 s (Parquet) and 0.026 s vs 0.250 s
  (`.pink`); full scan of a shredded column ≈1.2x the unshredded layout in
  Parquet (no projection pushdown); TPC-H Q1/Q6 10M rows 0.168/0.065 s vs
  0.166/0.066 s before the wave. Gates: fmt, clippy `-D warnings`, workspace
  1,941 tests / 171 suites, `pinkdb` 894, `pink-lake --features
  s3,encryption` 270, `pink-lake-parquet --features catalog-sqlite` 22,
  `pinkdb-ext-loader` 113, four SDK smokes (13/15/15/15) + 13 posture checks,
  wasm32 build.
- 2026-10-25 — Wave 101b review round: path statistics moved out of the
  VARIANT column's `min` slot (an overload the reviewer rejected) into
  `ducklake_file_variant_stats` — DuckLake v1.1 already defines it and
  `metadata/dialect.rs` created it unused. `variant_path` uses DuckDB's
  spelling (every key double-quoted, joined by `.`, embedded quotes doubled),
  `shredded_type`/bounds plain text (`int64`, `varchar`, `date`, …). DuckDB 2.0
  writes rows only for `varchar`/`boolean`/`float64` paths (it type-shreds
  integers but publishes no row) and validates nothing, so pinkdb publishes
  `int64`/`date`/`timestamp` too and both directions are tested
  (`variant_statistics_match_duckdbs_own_rows`,
  `pinkdb_prunes_files_duckdb_wrote`). Delete rides expire (its DELETEs already
  covered the table); tests assert merge → expire → 2 rows on sqlite and
  Postgres (`crates/pink-lake-parquet/tests/variant_paths.rs`, 5 tests;
  `crates/pink-lake/tests/variant_paths.rs`, 4 tests). Postgres covers
  `initdb` on port 55437 behind `PINKDB_TEST_POSTGRES_URL`. Reassembly: a
  leaf's entries are stored flat with one offset per row and its byte values in
  one pool per leaf, so a full-value scan of a shredded Parquet column over 1M
  rows went 0.695 s → 0.525 s and the ratio against unshredded 1.40x → 1.18x
  (lake-level full scan at parity). Docs re-pointed at the new transport;
  website regenerated. Gates: fmt, clippy (default, and `pink-lake` with
  `catalog-postgres,catalog-sqlite,s3,encryption`), workspace 2,000 tests / 172
  suites, `pink-lake --features s3,encryption` 271, `catalog-sqlite,s3,encryption`
  321, `catalog-postgres` 222 (server on 55437), `pink-lake-parquet --features
  catalog-sqlite` 25, `pinkdb-parquet` 41, wasm32 build.
- 2026-10-24 — Wave 101a: the `VARIANT` type core. Value model
  `crate::variant::Variant` (Null / Bool / Int(i64) / Double / Decimal(i128,u8) /
  Str / Date / Timestamp / Array / Object; objects key-sorted at construction,
  depth 128), `Value::Variant(Arc<..>)` and `VectorData::Variant` for the
  column, and a binary per-value encoding (kind tag 0x00–0x09; OBJECT/ARRAY
  self-describing) inside a page payload laid out like VARCHAR's
  (`u32 count | u32 offsets | values`). `.pink` gains `dtype_tag` 9 and
  `VD_VARIANT` 9; VARIANT chunks write no min/max (`SV_NONE`), so an older
  build meeting tag 9 reports `corrupt file: bad dtype tag 9`. Casts: any type
  → VARIANT (VARCHAR *parses JSON*, unlike DuckDB, which gives the type a
  literal form the parser/WAL/prepared statements already carry; INTERVAL
  becomes its canonical text), VARIANT → every scalar (errors name the kind and
  value, e.g. `cannot CAST VARIANT(VARCHAR) value "hi" to INTEGER`), a JSON
  null reads as SQL NULL for every target except VARCHAR, which keeps the text
  `null` so the text round trip stays lossless for JSON's model. Functions:
  `typeof`, `variant_type`, `variant_keys` (JSON array text — no LIST type),
  `variant_extract(v, path)` with 0-based JSONPath. Semantics: structural
  equality with cross-kind numeric folding (`1` = `1.0` = `1.00`), one
  documented total order (Bool < number < Str < Date < Timestamp < Array <
  Object < JSON null, with SQL NULL last), grouping/DISTINCT through the same
  canonical key as `=`/`cmp`, JSON null strictly distinct from SQL NULL.
  Persistence: save/open, WAL replay, checkpoint, ALTER ADD COLUMN DEFAULT.
  Encodings: wire protocol v11 (dtype 9, value `u32 len | bytes`), FFI
  `PINKDB_TYPE_VARIANT` (JSON text), wasm JSON, ext ABI and arrow refused like
  DECIMAL, parquet refused naming wave 101b. Lake: a `.pink` lake data file
  round-trips a VARIANT column through the shared format contract; the Parquet
  provider refuses it. JSON extension functions accept VARIANT through the one
  documented `VARIANT → VARCHAR` widening. Plan (value model, encoding, tag
  table, semantics, 101b scope, 12 recorded DuckDB differences) in
  `docs/wave-101-plan.md`.
- 2026-10-24 — Wave 92, first pass: one conformance corpus for every binding.
  `bindings/conformance/corpus.json` is the contract: cases name a `setup`
  and ordered `steps`; ops are `query` (plain when no `params`, prepared when
  present), `prepared_query`, `exec` (multi-statement batch, one expected
  result per statement), `exec_prepared`, `error` (stable `class` +
  `message_contains`) and `stream` (row count, head/tail rows, first-column
  checksum). Values are `null` or a `[type, value]` pair; doubles, bigints,
  dates, timestamps, DECIMAL and INTERVAL stay canonical text so a JS
  `BigInt`/`Number`, a Python `int`/`Decimal` and a Go `int64`/`time.Time`
  compare the same. DECIMAL/INTERVAL are never bound as parameters (ABI v1
  cannot); the cases bind text and `CAST` it. The 100k-row case is a five-way
  `CROSS JOIN` over a ten-row seed (no `range()`/`generate_series`), ordered,
  and both the Node `iterate` and Python `Cursor` paths read it one row at a
  time (Python: 0.33 s, 1,280 B `tracemalloc` peak — a materialized fetchall
  would be ~10 MB, pinned by the test). FFI addition: a thread-local
  `pinkdb_last_error_class()` next to `pinkdb_last_error()` returning one of
  `parse`, `catalog`, `type`, `parameter`, `execution`, `constraint`, `io`,
  `csv`, `invalid_argument`, `internal` (the engine's `Execution` message
  `NOT NULL constraint failed …` is mapped to `constraint`), so no binding
  parses messages. Node: `bindings/node/native` is a separate cargo
  workspace (the root manifest is out of scope for a binding) with napi-rs;
  `DbState` is an `Arc<Mutex<*mut pinkdb_handle>>` shared by the database and
  its prepared statements so closing the database under a live statement is
  an `invalid_argument` error, not a use-after-free; parameter text buffers
  must outlive the FFI call (a `Vec` dropped at function end produced a
  "VARCHAR payload is not valid UTF-8" bug that only the corpus caught).
  Python: `_candidate_library_paths` prefers the package directory, so an
  installed wheel loads its own bundled dylib; `setup.py` builds
  `pinkdb-ffi` and copies the library into `build_lib/pinkdb`, and
  `PlatformDistribution.has_ext_modules()` gives the platform wheel tag
  (`pinkdb-0.1.0-cp314-cp314-macosx_26_0_arm64.whl`, 1,533,602 B). Go: the
  preamble declares the used ABI subset and `pinkdb_value`'s layout; a
  copy of the module outside the checkout builds and runs with only
  `CGO_LDFLAGS` set, and the corpus/header tests skip there. Counts: Node
  `node --test test` 16/16 (7 API + 9 corpus), Python `unittest` 69/69 (60
  existing + 9 corpus; 9/9 also from the installed wheel), Go 42 top-level
  tests + 23 subtests including the 8 corpus cases, FFI 12 + header-sync 3,
  workspace 1,797 tests / 156 suites, 0 failed, 8 ignored. Gates: fmt,
  clippy `-D warnings`, `cargo build --release -p pinkdb-ffi`, `go vet`,
  `go test -race`. One pre-existing flake seen once, passing on rerun and in
  the final full run: `pinkdb-cli --test mcp`
  `read_only_refuses_writes_and_errors_are_json` ("server closed its stdout").
  Follow-ups stay on row 92 (JDBC, .NET, ADBC/ODBC, publishing, SQLAlchemy,
  Go streaming).
- 2026-10-24 — Wave 76: CSV import options, one `CsvOptions` for `.import` and `read_csv`
  (DuckDB parameter names): delimiter/quote/escape, `encoding` (utf-8 default; latin-1,
  windows-1252, utf-16 LE/BE, BOM autodetect — hand-written streaming decoders, no new
  dependency), `errors=error|warn` with a capped rejects report (line/column/reason, ten
  shown + total), `header=auto|true|false` using DuckDB's probe-pinned rule, `names`, and
  `nullstr`/`allow_quoted_nulls`/`force_not_null`. Probe finding worth keeping: α treats a
  *quoted* empty field as NULL by default — `allow_quoted_nulls=false` is the option the
  brief described, and the docs now say so. Importing into an existing table matches
  columns by header name (case-insensitive), missing → NULL, extra → the error policy.
  The fast comma/quote-free `parse_clean` path still applies to the default dialect, and
  the httpfs source sniffs only the 8 MiB window and streams the rest, so local and remote
  results are identical. Counts: pinkdb CSV module 19 → 31 tests (incl. a DuckDB-oracle
  test skipped without the α binary), httpfs 62, 4 new `.import` CLI tests, workspace
  1,860 tests / 161 suites, 0 failed, 8 ignored (pre-existing). Gates: fmt, clippy
  `-D warnings`, workspace tests, `pinkdb-httpfs --all-features`, `pinkdb-cli`, wasm32
  `pinkdb` build. Follow-up stays on the row: storing rejects in a table.

### 2026-10-25 — wave 76 follow-up: import speed restored (regression measured, fixed)
- The reviewer measured `.import` 7× slower than pre-76 (lineitem 773 MB: 2.54 s → 18.2 s),
  with identical row counts. `sample <pid>` on a release import put essentially all self time
  in `csv::Records::next`/`at`/`from_utf8` plus `_platform_memmove`, i.e. the type sniff
  walking the whole 773 MB through the byte-at-a-time record reader; after capping the sniff at
  `INFER_SAMPLE_ROWS` the import was 6.7 s, still 2.5× over pre-76.
- Second cause, from the re-profile: `parse_clean` bailed on *any* `"` and lineitem has 1.14 M
  quote bytes (568 431 quoted comment fields, no `\r`), so every row went through the record
  reader (~116 MB/s) where pre-76 fell back to the memchr-based `csv` crate (~292 MB/s).
  Rejected fixes: SWAR-optimising the reader (lands ~2.8 s, outside the 5 % budget) and
  stripping quotes from a test file (invalid — the comments hold commas).
- Fix in `crates/pinkdb/src/csv.rs`: `record_ends` proves the dialect in two parallel passes —
  newline and quote offsets per 1 MiB block, then each block's opening quote state from the
  parity of the quotes before it (`""` counts twice, so an escaped quote keeps it) — and keeps
  only the newlines outside quotes, validating RFC 4180 (a quote opens a field, closes before a
  delimiter or line end, never balanced-open at EOF). `parse_batch` toggles quote state to find
  record ends, and the new `Fields` splitter unescapes `""` and keeps a quoted cell's whitespace;
  quoted empties are NULL on that path because `fast_eligible` keeps `allow_quoted_nulls := false`
  off it. A file the scan cannot prove still goes to the record reader — the guard changed from
  "no quotes" to "quoting the reader would read the same way", and the `fast_path_matches_exact_two_pass`
  / `quoted_fields_survive_chunking` / `quoted_cells_keep_their_whitespace` tests cross-check the
  two paths. One trap: a block legitimately ends inside a quoted field (odd quote count), so a
  per-block "ends in quotes" bail was wrong — only an unbalanced file is (found via a temporary
  probe test, removed).
- Numbers (reviewer harness, interleaved, median of 3, out-of-tree release build): import lineitem
  2.54 s → **0.87 s** (−66 %), import orders 0.42 s → **0.21 s** (−50 %), both rows 6 001 215 /
  1 500 000. Cross-check of the two paths on the real file: `count(*)`, `sum(length(l_comment))`,
  `sum(l_orderkey)`, `min(l_shipdate)`, `max(l_commitdate)`, `sum(l_quantity)` identical.
  Transcoding/warn still use the reader: `errors=warn` 12.2 s, `encoding=latin-1` 14.2 s,
  utf-16 LE with BOM (1.5 GB) 13.3 s — measured, then deleted. `read_csv` count of lineitem
  (CLI built `--features httpfs`, which the harness's binaries are not): 5.03 s pre-76 → 7.43 s,
  a real regression on the streaming scan path that this pass did not touch — the next target.
- Then closed it: `parse_csv_chunks`/`stream_csv_chunks` now run the same batch parser. A whole
  local file is parsed at once; a remote file (or a local file past the 8 MiB sniff window) is
  read in 8 MiB windows of the wave-98c prefetch, each cut at its last provable record end (a
  record may straddle two windows — `record_ends(.., partial)` tolerates the open tail and the
  remainder is re-read with the next window), bounded by the window plus one record
  (`STREAM_CARRY_MAX` = 64 MiB) before the reader takes over. The reader finishes any window the
  scan cannot prove, continuing mid-stream with the carry's bytes, the correct `line_base` for
  error lines and the header still to skip when the batch pass bailed on the first window.
  Measured (`/tmp/w96a/pinkdb-pre76-httpfs` from `git worktree add --detach /tmp/w76pre 7d995ba`
  vs `pinkdb-post76-httpfs`, both `--features httpfs`, median of 3): `SELECT count(*) FROM
  read_csv('lineitem.csv')` 5.33 s → **1.36 s**; through the local test HTTP server on a 33.9 MB
  file, remote 486 ms → **120 ms**, local 522 ms → **77 ms** (ignored aid test `csv_scan_speed`).
  `read_csv` values match `.import` on lineitem (same six aggregates).
- Counts after both changes: `pinkdb` CSV module 36 tests, `pinkdb-httpfs` 63 (one `#[ignore]`d
  speed aid), workspace 1,865 / 161 suites.

- 2026-10-25 — Wave 95b: SQL dialect completeness, batch 2 — the DML forms.
  Parser/AST/binder/planner/executor/EXPLAIN for `INSERT … ON CONFLICT
  [(cols)] DO NOTHING | DO UPDATE SET … [WHERE …]` (`excluded.*`), `INSERT OR
  REPLACE`/`OR IGNORE`, `UPDATE … FROM`, `DELETE … USING`, `RETURNING` on all
  three (and on MERGE), `MERGE INTO … USING … ON … WHEN MATCHED [AND] THEN
  UPDATE/DELETE | WHEN NOT MATCHED [BY TARGET|BY SOURCE] [AND] THEN INSERT`,
  and top-level data-modifying CTEs (`WITH x AS (INSERT … RETURNING …) SELECT
  … FROM x`). Semantics were probed row by row on DuckDB 2.0.0 and pinned:
  UPDATE…FROM with several matching source rows takes the last one (DuckDB
  agrees; Postgres would error), DELETE…USING deletes once, MERGE runs clauses
  in order with first-match-wins and refuses a target row hit by two source
  rows with DuckDB's own message, ON CONFLICT keeps the first of a statement's
  own duplicate keys. `PRIMARY KEY`/`UNIQUE` are now **enforced** (new
  `crates/pinkdb/src/keys.rs`: a per-statement key map built from a table scan,
  checked in `trigger::insert_rows`/`update_rows`, patched for BEFORE-trigger
  rewrites and rolled back on a refusal) with DuckDB's wording; foreign keys
  stay metadata. `RETURNING` reuses `trigger_capture`, so it reports the row
  after BEFORE triggers. WAL: every form is logged as its source text and
  replays deterministically (new `crates/pinkdb/tests/dml_replay.rs` drops the
  store and reopens it). RBAC (`check_writes` recurses into CTEs, MERGE needs
  each action's privilege) and the MCP read-only allowlist (refuses all new
  kinds plus a writing `WITH`) are covered. Tests: sql_conformance 7→8
  (25-case DuckDB-measured corpus, error/trigger/keys cases), duckdiff
  311→336 (25 `dml-*` probes), dml_replay 1, rbac +1, serve +2; workspace and
  all gates green. No regression: 1 M-row INSERT…SELECT 0.095→0.381 s (the key
  check), UPDATE 0.025→0.025 s, DELETE 0.019→0.019 s (medians of five
  interleaved runs, `bench/wave95b_perf/`).
- 2026-10-25 — Wave 93a: one ABI (1.2) in four SDKs, one conformance fixture, pinned
  local libraries. The ABI header gained the size macros an extension negotiates with
  (`PINKDB_EXT_API_V1_SIZE`, `_FLAGS_SIZE`, `_MINOR2_SIZE`, `PINKDB_EXT_HOST_HAS_FLAGS`,
  `PINKDB_EXT_HOST_HAS_EXECUTE`) and `pinkdb_ext_copy_api` for the init-scoped host copy;
  re-vendored byte-identically into `ext-sdk/{c,cpp,go,rust}` (`tests/header_sync.rs`
  checks all four). C/C++/Go/Rust now all cover `register_scalar_flags` +
  `PINKDB_EXT_FLAG_VOLATILE`, the full tag set (INTERVAL as packed months/days + micros),
  the optional `pinkdb_ext_serve`, and `execute_statement`; the C++ SDK is new
  (header-only `pinkdb::ext` RAII wrapper), the Go shims became one package
  (`ext-sdk/go/pinkdb`). Every SDK builds `conformance/conformance_ext` from the single
  contract in `ext-sdk/conformance/README.md`, and the loader drives all four through
  `tests/conformance.rs` (11 tests: 15-function contract, per-tag round trips incl. NaN/±0/
  extremes, `cf_all` canonical row, VARCHAR edges to 64 MiB + 1, error codes, panic
  containment (C 2, C++ 3, Go 3, Rust 2), 100 foreign-thread callbacks) and
  `tests/conformance_ownership.rs` (host alloc/free balance on success, empty, NULL and
  error paths). Two harness lessons: a fixture is initialised once per process, so
  `struct_size` negotiation runs in a child process, and a `libloading::Library` must be
  forgotten — dropping it `dlclose`s a Go c-shared library and hangs. Local-extension
  integrity is a process-wide **SHA-256 path allowlist** in the loader
  (`--extension-pin PATH=SHA256`, `PINKDB_EXT_*`-free): unpinned, tampered and
  changed-on-disk libraries are refused, the hash is taken from an open file and its
  `(dev, ino, len, mtime)` re-checked immediately before `dlopen`, and the residual
  check-then-map window (no `fdlopen` on Linux/macOS) is documented in the module. That
  makes the H-7 residual risk explicit rather than mitigated: locally loaded libraries
  remain unsigned, and pins are what a deployment with no writable directory next to its
  extensions can rely on. `ext-sdk/posture.sh` pins the host-side refusals (opt-in,
  `--extension-root`, bare/`..`/symlink, pins) against the release CLI; CI runs the four
  smokes, the C++ build and the conformance matrix under `PINKDB_CONFORMANCE_STRICT=1`.
  Gates: fmt, clippy, `cargo test --workspace -q` (1,906 passed, 9 ignored, 166 test
  binaries), `cargo test -p pinkdb-ext-loader` (112 tests, 14 targets), four SDK smokes
  (C 13, C++ 15, Go 15, Rust 15 checks), `ext-sdk/posture.sh` 13 checks, header sync,
  wasm32 build.
- 2026-10-25 — Wave 96b, phase 1: a stateless `pinkdb-server`. `--catalog <pink:|sqlite:|postgres://…> --data <dir|s3://…>`
  serves a store with no local state: the engine catalog travels in the lake catalog and the rows
  in lake data files. Design first, as the row asked (`docs/wave-96b-plan.md`): the `ducklake_*`
  schema is DuckDB's, so the engine catalog is one row of `pinkdb_engine_catalog` beside it
  (`id`, `revision`, `encoded`), an opaque image of the engine catalog with its row pages omitted —
  a decomposition into `pinkdb_engine_*` tables per object kind would duplicate the `.pink` footer
  codec with no consumer that wants the parts. The image (`PINKCAT1`, `crates/pinkdb/src/catalog_image.rs`)
  is `magic | format | len | .pink metadata image | extensions | views | crc32c`: it carries the
  databases, tables with constraints/defaults/indexes, macros, materialized views, procedures,
  triggers and RBAC the footer already carries, plus the two things it does not (views as their
  `CREATE VIEW` source text, and loaded-extension sources replayed under the server's loader
  policy), and a table's statistics are stripped so the lake's files stay authoritative for the
  numbers. Engine-core edits stay behind two seams: `ViewDef` gained `sql` (a bound `Select` cannot
  be rendered back to SQL, the same reason `MaterializedViewDef` stores `query_sql`) and `PinkDb`
  gained `to_catalog_image`/`load_catalog_image` plus a commit hook (`Database::set_commit_hook`,
  called after a committed statement with the WAL append; a publish failure poisons the store so
  nothing acknowledged stays unpublishable). The publisher (`crates/pinkdb-server/src/catalog.rs`)
  is the CLI's own `WritableAttachment` loop: the image rides the same `CatalogChange` as the lake
  delta, so data files, delta and metadata commit in **one catalog transaction**; a metadata-only
  statement publishes the image through `Lake::publish_engine_catalog` without minting a lake
  version; a crash before the commit publishes nothing (`try_publish_commit` clears the pending
  image only after the commit). WAL role, narrowed to `--file`: the catalog transaction is the
  durable record, so a catalog-mode store is in memory — no local WAL, checkpoint or replay — and
  `--file` is byte-for-byte unchanged (views still not persisted there). One server per catalog
  (phase 1): an `engine/lease` line in `pinkdb_object` (`crates/pink-lake/src/engine.rs`), 2 s
  heartbeat, 6 s staleness, re-read before every publish, deleted on clean shutdown; a second
  server fails with `the catalog is already served by …` and a crashed one is taken over after the
  timeout. Tests (`crates/pinkdb-server/tests/catalog.rs`): the full round trip on `pink:` and
  `sqlite:` (databases, a table with a PK/default/index, view, macro, materialized view, procedure
  and trigger that run after the restart, RBAC login + role grant + grant refusal, a role created
  and a column added after the restart to prove the image, not a cached snapshot, is loaded) with
  the data directory deleted between runs and nothing but the catalog left in the scratch dir;
  the same on a Postgres catalog and on mock-S3 data (30 rows above the inlining limit, so the
  rows really come from the bucket, and no `catalog.json` there); a killed server's catalog
  reopening complete after the lease goes stale; the lease refusal; and Argon2id-only secrets.
  Two fixes the suite forced: the image must zero an index's `distinct_keys`/`indexed_rows`
  statistics (the row pages are gone, and the loader refuses statistics above the row count), and
  the RBAC bootstrap over the wire is a pre-existing gap — the engine's per-statement login gate
  refuses `GRANT ADMIN` right after the `CREATE USER` that armed it, in every mode, although the
  README documents that flow — recorded here rather than fixed in this wave (engine/exec surface).
  Gates: fmt, clippy (also with `catalog-sqlite,catalog-postgres,lake-s3`), `cargo test --workspace -q`
  (1,968 passed, 9 ignored, 168 test binaries), `cargo test -p pinkdb-server` (121), the same with
  `catalog-sqlite,catalog-postgres,lake-s3` and `PINKDB_TEST_POSTGRES_URL` set to a throwaway local
  server (126), `cargo test -p pink-lake --features catalog-sqlite,s3,encryption` (317), wasm32 build.
  Docs: README server section, website server page, `docs/wave-96b-plan.md`.
- 2026-10-25 — Wave 106: `pinkdb-agent`, the loop inside the store. The harness
  is a native extension (`extensions/agent`, `pinkdb-agent`, a cdylib), so the
  engine, CLI and server stay free of model and HTTP code; the only host change
  is additive ABI. Minor 4 adds the optional `pinkdb_ext_command(api, host_ctx,
  args, sink, sink_ctx)` export — detected by symbol presence like
  `pinkdb_ext_serve`, with a sink callback that streams the answer (channel 0)
  or a diagnostic (channel 1) — and a last `PinkdbHostApi` field,
  `execute_internal`, which runs a statement and answers machine-readable JSON
  rows instead of agent Markdown. `PINKDB_EXT_API_MINOR2_SIZE` became
  `offsetof(execute_internal)` (still 48 on 64-bit), so `PINKDB_EXT_HOST_HAS_EXECUTE`
  keeps working in the C and Go SDKs, and `PINKDB_EXT_API_MINOR4_SIZE` plus
  `PINKDB_EXT_HOST_HAS_INTERNAL` mark the new end; the header ships in all five
  copies, the Rust SDK grew `pinkdb_abi_exports_with_command!` and
  `CommandSession`, and the C/C++/Go SDKs need the same two additions at their
  next refresh (their smokes pass unchanged). The eight warehouse tools moved
  to `extensions/warehouse-tools` (`ToolReply`/`ToolHost`/`run`), which both
  `pinkdb-mcp` (behaviour identical, 12 tests) and the agent use. Sessions are
  tables in a dedicated `pinkdb_agent` database — `sessions`, `messages`,
  `tool_calls`, `checkpoints`, `usage`, ids like `a1f2…`, microsecond
  timestamps, no primary keys because the engine's MVCC already orders writes
  — and a step (one model turn, its tool results, the checkpoint) commits as
  one internal batch, so `kill -9` costs at most the step in flight. Two
  policies keep the transcript honest: the model's statements go through the
  host callback under the row-105 read-only allowlist plus
  `deny_write_databases: ["pinkdb_agent"]`, and the extension's own writes go
  through `write_database: Some("pinkdb_agent")` on the internal channel, so
  neither side can write the other's data (both directions tested, including
  `.lake bulk_commit` as a dot command). Resume reuses recorded tool results,
  re-runs only replay-safe (read-only) tools, and re-requests an interrupted
  stream; fork copies the rows up to step N into a new session recording
  `origin`/`origin_step` and leaves the original alone. The provider is one
  blocking `ureq` 3.4 (rustls) client with a hand-written SSE reader: split
  tool-call arguments reassemble by index, usage and `finish_reason` ride the
  same stream, 429/5xx retry three times (250/500 ms) and a 401 stops with a
  message that names the variable, not the key. The key comes from
  `PINKDB_AGENT_API_KEY` then `Z_API_KEY` at run start, never from a `.env`
  file, and a sentinel test greps every agent table, the `.pink` and WAL bytes
  and stderr. Defaults: `https://api.z.ai/api/paas/v4/chat/completions` and
  `glm-4.6`, 20 steps, no token budget. CLI: `.agent ask|resume|sessions|show|
  fork`, `--agent FILE "question"` (loading the library from the install
  directory like `--mcp`) with `--agent-allow-writes`/`--agent-allow-files`;
  wave-99's `.agent rows|cols|cell|budget|timeout|hint` still owns the name
  when no run verb follows it. Tests: `pinkdb-agent` 26 (4 chat, 4 SSE, 2
  provider, 6 command, 3 run, plus 7 recorded-mock-provider fixtures) and 13 +
  1 ignored live run in `crates/pinkdb-cli/tests/agent_run.rs` against a
  localhost mock (single answer, tool loop, split arguments, interrupted
  stream, 429, 401, kill/resume, fork, seven refusals, allow-writes minus the
  transcript, budget stop, TPC-H top-3); loader `tests/command.rs` 4 with a C
  fixture, conformance minor-2/v1 hosts. Gates: fmt, clippy (also
  `-p pinkdb-cli --features pinklake,s3,parquetlake`), `cargo test --workspace
  -q` (2,040 passed, 10 ignored, 179 binaries), ext-loader 117, pinkdb-cli 121
  (and 189 with the lake features), pinkdb-mcp 12, release agent build +
  tests, `ext-sdk/posture.sh` and all four smokes, wasm32 build. Sizes:
  `pinkdb` 5,035,312 → 5,073,504 B (+0.76%), `pinkdb-server` 4,668,016 →
  4,669,264 B (+0.03%), `libpinkdb_agent.dylib` 2,471,632 B,
  `libpinkdb_mcp.dylib` 588,640 B, zero new third-party crates; plan in
  `docs/wave-106-plan.md`.

- 2026-10-26 — Wave 103c (acf7b84): the last JSON-catalog traces are gone
  from pink-lake. Errors carry the catalog location as given — `pink:`, `sqlite:` or a
  redacted Postgres URL — from one `Lake::catalog_label()`/metadata-store label computed at
  open, replacing `CATALOG_FILE` and the root-joined catalog paths; `NotALake`/
  `AlreadyALake` lose the "directory" wording; SQLite I/O errors name the `.db`; the crypto
  `Domain` enum keeps only `DataFile`/`DeleteFile` (ids kept); the dead `STATE_PATH` and the
  never-written `lake.json` / `replication/state.json` lines left the docs tree; docs,
  README, website and five historical CHANGELOG phrases were reworded (website/src must
  stay literal-free) and the SigV4/S3 tests use `object.bin` (vectors recomputed). Tests:
  one per backend removes the catalog's metadata and asserts the error names the location,
  carries no stale file name and no Postgres password. Gates: fmt; clippy (workspace, and
  `-p pink-lake` with `s3` / `s3,encryption` / `catalog-sqlite,s3,encryption` /
  `catalog-postgres`); pink-lake 323, pink-lake-parquet 29, pinkdb-cli 189 (1 ignored),
  workspace 2,118 passed / 12 ignored / 195 binaries.
- 2026-10-26 — Wave 107d (row 107 follow-up): **parallel HNSW build**,
  deterministic at any thread count. Batched insertion in
  `extensions/vss`: the first 1,024 vectors build sequentially as in
  107b, then batches of 256 run a parallel read-only search against the
  frozen graph plus an exact intra-batch pass, with neighbour selection
  in the same parallel phase (peers and frozen candidates selected
  separately, then merged — a joint selection let exact duplicates
  island the batch), a sequential in-order commit of own links and the
  entry point, and reverse-link pruning applied in parallel one
  neighbour list at a time in insert order. Constants are fixed, never
  derived from the pool size, so 1/2/4/8 threads produce byte-identical
  graph bytes (unit test) and a WAL replay rebuilds the same graph
  (engine test). `VectorIndex::insert_batch` is a new defaulted trait
  method; the engine calls it for `CREATE INDEX` and for appends of
  at least 32 rows, single-row inserts keep the old path. Measured on
  an Apple M4 (4P+6E): 100k×768 build 111.5 s sequential → 32.1 s at 4
  threads (3.5×) and 30.4 s on all 10 (3.7×), recall@10 0.878/0.961 at
  ef 64/128 (sequential 0.874/0.963); 1M×128 867 s → 139.1 s (6.2×),
  recall 0.751/0.883 (sequential 0.740/0.862 — never worse). Zero new
  crates: `pinkdb-vss` depends on the already-locked rayon only on
  non-wasm targets, and wasm32 runs the same phases sequentially.
  Plan: [docs/wave-107d-plan.md](docs/wave-107d-plan.md).
- 2026-10-27 — Wave 109 (row 109): **agent memory**, a Rust reimplementation of
  TencentDB-Agent-Memory's design (no code copied) as a native extension.
  Four layers in one database, `pinkdb_memory`: `l0_turns` (raw turns),
  `atoms` (standalone facts with confidence, `superseded_by` and a status of
  `active | superseded | forgotten`), `scenarios` (titled summaries naming
  their atom ids), `personas` (latest row wins), plus `distill_queue`, `audit`
  and `runs` (one row per model call). Every row carries `owner`, `team`,
  `visibility`, three ACL columns and `agent`; nothing is ever hard-deleted —
  `forget` sets `forgotten` and appends an audit row, `update` writes a new
  atom that supersedes the old one. **Identity** is the wave's engine seam:
  SQL builtins `current_user()` and `current_roles()` bound to a
  `BoundExpr::Literal` at bind time from `storage::Session`'s new principal
  field (a network login sets it, `logout` clears it, an embedded store answers
  the implicit `pinkdb`). They are not `ScalarFunc` variants because
  `eval_scalar` sizes its chunk from the first argument, and the dependency
  token reuses the existing WAL guard, so a logged write cannot replay under
  another user. No ABI change: the four SDKs and every loaded extension are
  untouched. **Serving**: `ServePolicy` gained `internal:
  Option<Box<ServePolicy>>` (the channel an extension's own bookkeeping runs
  under — `Session::run_internal`, wired to ABI minor 4's `execute_internal`
  slot, so an older extension just sees "no channel") and `deny_read_databases`
  (a served statement that reads `pinkdb_memory` is refused with a hint naming
  the tools; enforced by binding the statement and collecting the tables it
  reads, so a CTE or macro cannot slip past). **Retrieval**: persona, then the
  best-matching scenarios, then atoms and turns ranked by BM25 (108a's
  constants, computed in the extension because 108a's index is static and its
  `CALL` is a global write) and, in hybrid mode, by `embed()` +
  `array_distance` over a lazily created `atom_vectors`, fused with
  `rrf = 1/(60+rank)`; three budgets (items, characters, timeout) each stop it
  and the result names which one did. **Distillation**: queued, never on the
  add path; one model call per step (`turn` → atoms, `scenario`, `persona`),
  each answer validated against a strict JSON contract before a single write, a
  malformed answer recorded on the queue row and retried later, `attempts`
  capped at 3, and an operator's drain resets failed jobs.
  `PINKDB_MEMORY_DISTILL_EVERY=N` queues every N turns. **Surfaces**: `.memory
  add|search|update|forget|distill|persona|show|serve` and six MCP tools
  (`memory_add|search|update|forget|distill|persona`) on the memory extension's
  own serve loop, which shares the extracted `pinkdb-mcp-protocol` crate with
  `pinkdb-mcp` and the unchanged warehouse tools; the write tools need
  `allow-memory` and the host's policies confine them to `pinkdb_memory`.
  **Agent**: with the memory extension loaded, an `.agent` run records its
  question and answer as two L0 turns under the caller's identity — the two
  extensions cannot call each other, so the CLI host that owns both is the
  seam. **Confined on both sides** (reviewer round): the internal channel is a
  read allow-list (`ServePolicy::read_databases`, checked from the bound plan)
  and external services have their own `allow_external_services` switch, no
  longer riding on `allow-files`; that work found a latent hole —
  `Connection::hint_store()` is `None`, so no catalog-based policy check ran
  for a served session over a connection — closed by `Session::check_store`
  (`crates/pinkdb/tests/internal_channel.rs`, 5 tests). The reported
  `LOAD EXTENSION` order anomaly did not reproduce; the only mechanism in the
  tree that matches the symptom is an unterminated `LOAD` line swallowing the
  next one, and `loading_the_agent_before_memory_still_runs_every_statement`
  now pins two loads plus two statements in one script. Tests: 13 in
  `pinkdb-memory` (end-to-end mock distillation, all four
  visibility classes against real users and roles, a direct `SELECT` refused,
  supersession and forget with audit rows, the three budgets, hybrid RRF
  against hand-computed scores, malformed JSON, 429-then-success,
  `attempts`-capped failure, no provider, the write switch, the command
  surface, and a sentinel grep of the durable store's bytes) plus one
  `#[ignore]`d live test; 6 `current_user` engine tests; 4 CLI `memory_run`
  tests and 26 `agent_run` (2 ignored), including a run whose turns a later
  run searches. Gates (after the reviewer round): fmt, clippy -D warnings,
  memory 13 (+1 ignored), agent 13, mcp 11, embeddings 20 (+1 ignored),
  ext-loader 122, `current_user` 6, `internal_channel` 5, CLI `agent_run` +
  `memory_run` + `mcp` 39 (+2 ignored) and `--features embeddings` 4,
  workspace 2,235 passed / 14 ignored / 210 binaries, wasm32 green,
  `ext-sdk/posture.sh` 13 and all four smokes. Sizes (release, default features): `pinkdb` 5,382,768 B,
  `pinkdb-server` 4,977,888 B, `libpinkdb_memory.dylib` 2,746,160 B — the
  extension is a cdylib and neither binary links it; zero new third-party
  crates. Plan: [docs/wave-109-plan.md](docs/wave-109-plan.md).

- 2026-10-26 — Wave 111 (benchmarks + tuning): `bench/suite` (seven families, TPC-H vs DuckDB, ingest, durable, lake, search, footprint, CI guard), `docs/benchmarks.md` and the website page; comma joins hash instead of crossing, disconnected chains reordered, `OR`-derived filters, `IN` capacity. SF 1 geomean 33.1× → 32.0× over the same 15 (29.0× over 16), Q9 100.9 s → 0.66 s, Q8 11.7 s → 0.29 s, Q7 2.35 s → 1.20 s; SF 0.1 Q7 219 → 114 ms (geomean 7.4× → 8.6× after the memo revert); default CLI +1.0% (5,437,168 B). Leak fix for the pink-lake corruption test.

- 2026-10-27 — Wave 120 (row 120): **correlated subqueries**. Binding is a scope
  chain (`Binder.outer`, `OuterScope`, `subquery_scope()`): a miss in the block's
  own FROM resolves to `BoundExpr::OuterRef{depth,index,dtype}` through any number
  of enclosing blocks, and `OuterBind{refs,deep,keys}` travels with each subquery
  expression. The planner rewrites them into joins after Neumann & Kemper (BTW
  2015) as DuckDB does: `EXISTS`/`NOT EXISTS` → semi/anti hash join, `IN`/`NOT
  IN`/`ANY`/`ALL` → mark join with three-valued truth, a correlated scalar
  aggregate → `Aggregate(keys = correlation keys)` + left join with `count`
  coalesced to 0 (the COUNT bug). `JoinKind` gained `Semi`/`Anti`/`Mark` in the
  hash join; `ANY`/`ALL` were added to the parser. Correct but slow shapes (two
  levels deep, `LIMIT`/`DISTINCT`/`GROUP BY` bodies, pure range correlation,
  scalar under `NOT`) stay on a per-distinct-key path. 54 probe cases are pinned
  and re-checked against DuckDB 2.0, correlated `UPDATE`/`DELETE` replay from the
  WAL, and all 22 TPC-H queries match DuckDB at SF 0.01, 0.1 and 1. SF 0.1
  geomean 9.3× over all 22 (8.0× over the 16 wave 111 could run, vs 8.6× then).
  Plan: [docs/wave-120-plan.md](docs/wave-120-plan.md).
- 2026-10-27 — Wave 111ab (38e2b08 + this wave): cost-based join ordering (111a) and
  the execution/subquery paths (111b.2/.3). A query graph over comma chains with a
  lightweight cardinality model (FK-aware equivalence classes, no histograms) is
  enumerated by a subset DP up to 12 relations, greedy above, anchored at the FROM
  clause's first relation, and only applied when the estimate improves 10 %; the
  restore projection preserves the output order and outside-chain predicates.
  Filters now derive per-column facts through a lone `OR`, including `IN` lists
  inside branches, with union (not intersection) bounds. The subquery memo is
  primed by each operator before its chunks fan out, so no lock is held across an
  evaluation and no worker waits on one: the review round's deadlock (a subquery
  outside a filter) and Q20's earlier hang are both gone (30 repro runs, 0 hangs).
  TPC-H SF 1 geomean 38.5× → 21.8× (30.3 s → 8.0 s total), SF 0.1 9.5× → 6.5×;
  Q7 1.25 s → 246 ms, Q18 5.5 s → 1.45 s, Q19 3.86 s → 114 ms, Q20 4.9 s → 606 ms,
  Q21 10.4 s → 2.25 s, Q22 680 → 166 ms. Q9 at SF 0.1 57 → 60 ms and Q5 31 → 31 ms:
  recorded, with the remaining aggregate, delim-join and ingest work as rows 111b1,
  111c and 111b4. Default CLI +37.8 kB (+0.68 %), no new dependencies. Plan:
  [docs/wave-111ab-plan.md](docs/wave-111ab-plan.md).
