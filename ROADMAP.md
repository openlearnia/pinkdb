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
| 34 | Lake architecture: DuckLake-style decoupled metadata + data storage (catalog, snapshots, data files) (separate crates, storage layer) | done | crates/pink-lake + CLI lake feature: catalog.json + immutable snapshots/<v>.json + per-table data/<table>/<uuid>.pink (single-table engine bytes); snapshot/version/restore API; crash-atomic publication; 465 tests both parser modes, 35 CLI lake tests, clippy clean incl. lake,encryption; process-race harness 8×4/6×4/6×6 consistent; sizes default 2.29 MB / +lake 2.47 MB |
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
| 40h | Lake: incremental saves, `.lake gc`, `lake_scan` SQL access + time travel, bulk typed decode, encode panic guard | queued | — |
| 40i | Extension trust + distribution: Ed25519 artifact signatures, SQL `INSTALL`, Linux artifacts, Python wheel, standalone Go module | queued | — |
| 40j | Test depth: window/plan unit tests, WAL fuzz, persist property tests, duckdiff gated in CI | queued | — |
| 41 | Primary/foreign keys, Snowflake-style: declarative `PRIMARY KEY` / `FOREIGN KEY` / `UNIQUE` / `NOT NULL` constraints (metadata, not enforced), `RELY` / `NOT ENFORCED` accepted, persisted in the catalog, used by the optimizer (e.g. FK→PK join elimination) | done | landed; DuckDB-pinned DDL validation, DESCRIBE `PRI`/`UNI` parity, conservative FK→PK join elimination; `.pink` v5 constraint section (flags bit 0) composed with wave 62's dtype tags; duckdiff 165 → 171 |
| 42 | Index support: `CREATE INDEX` / `DROP INDEX`, hash indexes for equality lookups + automatic per-chunk zone maps, optimizer index-vs-scan choice, per-database, persisted/WAL-logged | queued | — |
| 43 | Materialized views: `CREATE MATERIALIZED VIEW`, `REFRESH [MATERIALIZED VIEW]`, on-demand full refresh, per-database, persisted + WAL-logged (incremental/auto-refresh deferred) | queued | — |
| 44 | Stored procedures: SQL procedural language (parameters, variables, control flow) + **JavaScript procedures** (optional embedded engine; WAL-replay determinism must be solved — volatile calls refused or pinned at record time) | queued | **user-flagged next-list item** — dispatch right after the naming sweep (74) once the lake cluster lands |
| 45 | Performance tuning (recurring every 5th wave): bench-driven speed work against the committed query bench; profile-first, small measured wins (core) — *pulled forward after 40c* | done | 4b53844…(part 6) — 1M battery 416 → 245–250 ms (−41%): group_region 6.7 → 3.2, distinct_lowcard 19.4 → 3.8, windows 63 → 13.5, order_str 36 → 29, group_id/distinct_id 62/69 → 50/50, join_probe 21 → 5.3; @10M group_region 68–70 → 33–35 ms (≤60 target met; DuckLake 37–55), import 274 → 173 ms, `.lake save` 632–758 → 296–340 ms (lake format_version 3, 8,192-row blocks, agreed layout change), `.lake open` 618 → 100 ms, data 29.7 → 25 MB, teardown 93 → 16 ms; 516 tests, duckdiff 60/60 + 64/64 big, kill loops clean, clippy/fmt clean |
| 46 | Network server TLS: rustls + certificate config, encrypted transport (optional pinkdb-server feature) | queued | — |
| 47 | Async I/O at the I/O boundary: optional async storage layer (object stores / S3 for the lake) and/or an async server variant, feature-gated; requires a documented carve-out to the zero-async design rule for the I/O boundary only (core executor stays sync) | queued | — |
| 48 | RBAC: users + roles with Argon2id password hashes (reusing the encryption crate), `CREATE USER`/`CREATE ROLE`/`GRANT`/`REVOKE` on databases/tables, session login on the network server, per-connection enforcement (embedded API stays trusted), grants persisted in the catalog + WAL | queued | — |
| 49 | Extensions on wasm: support loadable/pluggable extensions for wasm targets — either wasm dynamic-linking (pre-built WASI side modules, per-runtime `dlopen` support) or a host-function registration path (JS/wasm-bindgen scripting surface); native `dlopen` extensions are impossible in the wasm sandbox, so a distinct ABI/loading model is required | queued | — |
| 50 | Performance tuning (recurring every 5th wave): bench-driven speed work (core) | queued | — |
| 51 | Rust extension SDK: `ext-sdk/rust` safe wrapper + registration macros for first-party extensions; ABI v1 additive: a deterministic/volatile function flag (the engine refuses to WAL-log volatile calls, preventing replay divergence) | queued | in-repo `ext-sdk/rust` |
| 52 | Core extension: **json** — `json_extract`, `json_value`, `json_valid`, `json_array_length`, `json_object`, `json_array` (JSON as VARCHAR in v1) | queued | `extensions/json` |
| 53 | Core extension: **string** — `split_part`, trim variants, `replace`, `reverse`, `left`/`right`, `strpos`, `starts_with`/`ends_with`, `regexp_match`/`regexp_extract` | queued | `extensions/string` |
| 54 | Core extension: **math** — `sqrt`, `pow`, `log`/`ln`/`exp`, trig, `floor`/`ceil`, `sign`, `greatest`/`least`, `random` (volatile-flagged) | queued | `extensions/math` |
| 55 | Performance tuning (recurring every 5th wave): bench-driven speed work (core) — done: column-vs-literal comparison fast path (`filter_str` 5.39 → 1.60 ms, battery 235.5 ms best-ever); audit deltas attributed (per-chunk literal Vectors + codegen/thermal noise), fused-path choice validated | done | core |
| 56 | Core extension: **datetime** — `strftime`, `strptime`, `epoch`/`epoch_ms`, `dayofweek`/`dayofyear`/`week`, `last_day`, `age`, `make_date` (naive timestamps only) | queued | `extensions/datetime` |
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
| 68 | Lake inlined small writes & CDC, DuckLake parity: inline tiny commits into the metadata (`data_inlining_row_limit`-style, flush to files later), change summaries rendered as CDC queries (`insertions`/`deletions`/`changes`), `add_data_files` (register existing files); depends on 63 | queued | DuckLake-parity program |
| 69 | Prepared statements in the Python and Go bindings: wrapper ergonomics + tests over the FFI entry points landed in 59 | queued | follows 59 |
| 70 | Core extension: **parquet** — read: `read_parquet` / `parquet_scan` (globs, multi-file, `union_by_name`, hive-partition + `filename` columns, column subset), projection + filter pushdown via row-group statistics (min/max/null counts), introspection (`parquet_schema`, `parquet_metadata`, `parquet_file_metadata`, `parquet_kv_metadata`), direct `FROM 'file.parquet'` replacement scan; write: `COPY … (FORMAT parquet)` (snappy default; zstd/gzip/uncompressed; `row_group_size`; per-column stats) with the α-probed physical mapping (DECIMAL→INT32 or FIXED_LEN_BYTE_ARRAY, TIMESTAMP→INT64 micros, DATE→INT32 days, INTERVAL→FIXED_LEN_BYTE_ARRAY) | queued | crate `pinkdb-parquet` (wasm-compatible); nested types + parquet encryption deferred |
| 71 | Core extension: **arrow** — Arrow IPC (Feather v2) file read/write (`read_arrow` + `FROM 'file.arrow'`, `COPY … (FORMAT arrow)`) over the arrow-rs crates; Arrow **C Data Interface** in the FFI (query-result export + Arrow-array scan input, zero-copy where the layout allows) for pyarrow/Polars/DuckDB/ADBC hosts; DuckDB α ships no loadable `arrow` extension on this platform (probed: HTTP 404), so parity is pinned at the file level (arrow-rs/pyarrow round-trips; pyarrow pip-installable) and at the C ABI level, not via SQL | queued | crate `pinkdb-arrow` (wasm-compatible); Python/Go wrapper ergonomics follow-up |
| 72 | Core extension: **iceberg** — Iceberg v2 reads from the local filesystem: `iceberg_scan` (newest snapshot) plus the α-probed introspection set (`iceberg_metadata`, `iceberg_snapshots`, `iceberg_scan_plan`, `iceberg_scan_tasks`, `iceberg_column_stats`, `iceberg_partition_stats`, schema/table properties), time travel, `ATTACH … (TYPE iceberg)` catalogs (local first; REST follow-up), over parquet data files (depends on wave 70) | queued | crate `pinkdb-iceberg`; writes/maintenance (`iceberg_rewrite_data_files`, `iceberg_rollback_to_snapshot`, truncate, property mutations, `iceberg_to_ducklake`) deferred until the α surface is probed |
| 73 | Docs catch-up & changelog wiring: bring the website pages current through the landed waves (58–64 now; extend as 65–68 land) — SQL completeness, prepared statements, DECIMAL/INTERVAL, PK/FK, lake time travel, incremental commits — fold in the wave-41 constraint docs excluded at integration, link CHANGELOG.md from the site + README, and add a docs-freshness check to CI | done | landed; site current through 64 (prepared/DECIMAL/keys/lake pages), generated changelog page + nav/footer links, CI fails on stale site output; the redesign WIP landed first (`544cc3f`) |
| 74 | Lake naming sweep: rename the lake builds — `lake-lite` → **pinklake** (the `.pink`-file lake) and `lake` → **vortexlake** (the Vortex-file lake) — across cargo features (`pinklake`/`vortexlake`; pink-lake's crate feature `vortex-format` → `vortexlake`; **`s3` is a pure storage mechanism and implies neither lake build** — it only forwards `pink-lake/s3`, so combinations are explicit: `pinklake,s3` / `vortexlake,s3`, and `--features s3` alone no longer provides `.lake` commands), CLI help/error text, test cfgs and feature matrices, CI commands, README, the website and the ROADMAP/CHANGELOG wording ("lean lake"/"full lake" → pinklake/vortexlake). No aliases (pre-release interface) | queued | schedule right after the lake cluster (57/65/66) lands so no in-flight patch conflicts; then 42, 43, 44, 46… in list order |
| 75 | Core extension: **httpfs** — direct remote-file access *without* the lake: a shared transport crate (`pinkdb-httpfs`) with the SigV4 S3 client extracted from pink-lake (https GET + `s3://` objects, range reads; the `AWS_*`/`PINKDB_S3_*` config and retry semantics unchanged — pink-lake's `s3` refactors onto it with no behavior change), feeding URL file sources: `.import https://…/x.csv` / `.import s3://bucket/x.csv` and the engine's `import_csv` accepting URLs, plus a minimal `read_csv('url')` SQL table function (`SELECT … FROM read_csv('s3://…/x.csv')`) as the seed of the table-function path waves 70–71 extend to parquet/arrow. Single-object first; globs, gzip/compression and `COPY TO s3://` exports as follow-ups | queued | schedule adjacent to the file-format block (70–72) and after 74 (both touch pink-lake features); https-only by default, same credential envs as the lake |
| 76 | File-import options (CSV, shared by `.import` and wave-75 `read_csv`): `delimiter` (+ `quote`/`escape`), `encoding` (utf-8 default; latin-1/windows-1252; utf-16 LE/BE with BOM autodetect), `errors=error\|warn` (strict stays the default; `warn` skips bad rows and prints a rejects report with line/column/reason — store-rejects-table as follow-up), `header=auto\|true\|false` and column matching into existing tables **by header name** (case-insensitive; missing columns import as NULL, extra columns follow the error policy; `names=` override), NULL handling: `nullstr=` custom tokens plus the DuckDB empty-field rule (unquoted empty = NULL, quoted empty = empty string — probe α) with an option to keep empties as empty strings. Exposed as `.import <path> [table] [key=value …]` and a `CsvOptions` API struct | queued | after 75 (shares the surface) or alongside — engine/CSV work, disjoint from the lake cluster |
| 77 | **SQL macros**: `CREATE MACRO name(params) AS expr` (scalar) and `CREATE MACRO name(params) AS TABLE SELECT …` (table macros), `DROP MACRO [IF EXISTS]`, bind-time parameter substitution (arguments spliced at bind, no dynamic SQL), visibility in `SHOW`/`DESCRIBE`-style introspection, use inside views and other macros (recursion rules probed against α), persisted + WAL-logged like other DDL | queued | engine wave; persistence gains a catalog object — same version policy (clean newer-format refusal, no compat gates) |
| 78 | **`pinkdb-server` warehouse browsing UI**: feature-gated web console served by the server — static self-hosted assets (no CDNs) + JSON endpoints (databases, schemas, tables, paged row previews, row counts/sizes), read-only browsing first; query console as a follow-up; blocking HTTP/1.1, no async runtime (consistent with the server); binds localhost by default with a loud no-auth warning — **TLS (46) and RBAC (48) are prerequisites for any non-localhost exposure** | queued | new crate `pinkdb-server-ui` (feature of or alongside `pinkdb-server`); reuses the cookieless box-renderer semantics for previews |

Cadence: every fifth wave (40, 45, 50, ...) is a performance-tuning wave — no new features, bench-driven speed work against the committed query bench.

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
brief now carries the disk guard). **⏸ Dispatch freeze (Oct 17, user): after
wave 67 lands and is integrated, no new waves start — everything still queued
(68, 74, 44, 42, 43, 46–56, 70–72, 75, 76) stays frozen until the user says
otherwise. Landing/integrating the in-flight 67 is the last scheduled work.**

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
