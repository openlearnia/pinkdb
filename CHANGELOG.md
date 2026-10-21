# Changelog

pinkdb is developed in numbered waves: each wave ships one focused capability
(or a performance pass) with its own tests and CI gates. This file is the
user-facing summary — one section per wave, newest first — while the
[ROADMAP progress log](ROADMAP.md#progress-log) carries the full engineering
detail (probes, gate counts, integration notes). Entries are appended at
integration time starting with wave 63; earlier waves are backfilled as
one-liners below.

## 2026-10-21
### Security hardening — pass 6: filesystem privileges, file modes, lake binding ✅
- `COPY … TO` now needs `ADMIN` and is **off by default on the server**; with
  the opt-in it is confined to a configured root, refuses traversal/symlinks,
  and writes atomically as a `0600` temp file. Table-function scans likewise
  require `ADMIN` in authenticated sessions, including nested/CTE shapes.
- WALs, checkpoints, lock files and lake artifacts are created `0600` (lake
  directories `0700`), and existing engine/lake files are tightened on open.
- Lake catalogs are now **v6**: manifests are bound to `(lake_id, version)`,
  a `highest.json` marker detects rollbacks, and cross-lake/foreign-manifest
  substitution is refused. Local tags now include `(dev, ino)` so a
  same-size/same-second replacement can no longer slip past the CAS.


### Wave 78 — server warehouse browsing UI ✅
- A feature-gated (`ui`, default off) read-only console served by
  `pinkdb-server`: embedded self-hosted assets, paged row previews, catalog and
  row-count/size JSON endpoints. Built on **axum + tokio + maud** (a documented
  async carve-out; the console owns its runtime and every engine read runs on a
  bounded blocking pool).
- Fails closed: opt-in, loopback-only, non-loopback binds refused, and a store
  with RBAC users refuses to start without Basic auth (per-user grants) or a
  bearer token. The default server binary grows by only ~128 B.

## 2026-10-20

### Wave 90 — tag-driven release train + `install.sh` ✅
- A `v*` tag now builds and publishes 36 smoke-tested artifacts (24 CLI variants ×
  12 server variants) for Linux x86_64/aarch64 and macOS arm64/x86_64, with an
  immutable `SHA256SUMS` manifest and a single publish job that refuses to overwrite
  an existing release.
- Extensions build per platform on the same tag and fan in to one signed catalog
  publish (plus one Worker deploy), so releases and the extension repository stay in
  lockstep.
- `install.sh` installs latest or a pinned version with checksum verification,
  variant selection, atomic install to `~/.local/bin`, and no sudo; the docs site has
  an install page, and the CLI reports `--version`.

### Security hardening — pass 5: auth DoS/timing/hash policy + resource bounds ✅
- Logins no longer run Argon2id while holding the store lock; failed attempts
  are rate-limited per connection and per IP, and concurrent verifications are
  capped — credential-free clients can no longer starve writers or allocate
  GiBs of transient memory.
- Unknown users pay the same dummy Argon2id cost as wrong passwords (no timing
  enumeration), and `PASSWORD_HASH` values are checked against an Argon2id
  work-factor floor **and** ceiling.
- The server enforces a 30 s statement timeout, 10M-row and 256 MiB total
  result caps (configurable), and refused connections no longer spawn a thread.

### Index DDL durability (minimal half of wave 79) ✅
- `CREATE INDEX` / `DROP INDEX` now survive reopen **and** checkpoints: the DDL
  is WAL-logged as canonical SQL (quoting preserved), and each checkpoint
  re-logs the live definitions into the fresh WAL generation. No `.pink`
  format change — indexes are still rebuilt from data at open, and flags bit 3
  remains reserved for the full persistence work.

### Security hardening — pass 4: index lifecycle ✅
- Indexed lookups are now **always re-checked**: the optimizer keeps `Filter`
  above `IndexScan`, and any INSERT/UPDATE/DELETE/COPY marks indexes stale
  (rebuilt lazily at the next statement boundary) — a stale index can no longer
  return wrong rows.
- Out-of-range row ids in an index are skipped instead of panicking (closing a
  remote DoS), probe keys are canonicalized to the column's type (BIGINT/
  DOUBLE/DECIMAL + integer literals now find rows), and `DROP COLUMN` reindexes
  correctly.
- Indexes remain **session-lifetime**: the hash maps are not stored in `.pink`
  and the DDL is not replayed from the WAL yet, so reopening a store currently
  loses its indexes (queries fall back to correct but slower scans). Index
  persistence is wave 79.

### Security hardening — pass 3: async TLS downgrade ✅
- `--transport async` now **refuses** `--tls-cert`/`--tls-key` at startup with a
  clear error instead of silently serving plaintext. TLS remains available on
  the default sync transport (or via a TLS terminator in front); native async
  TLS is recorded as a follow-up.

### Security hardening — extension repository signing ✅
- The extension catalog is now **Ed25519-signed** (`index.json.sig`), and
  `.install` verifies it against a key **pinned in the binary before it
  fetches or loads any artifact** — a compromised repository origin can no
  longer serve a matching hash for its own payload. Unsigned catalogs are
  refused unless an explicit dev override is set.
- `publish.py` and `verify.sh` sign/verify; the publish workflow signs with the
  `PINKDB_EXT_SIGNING_KEY` secret and adds a verification gate. No new default
  dependencies (+~319 KiB only in `repo` builds).

### Security hardening — pass 2: authentication gate ✅
- The login gate is now evaluated **per statement inside a batch** (both
  transports, prepared statements included): an unauthenticated `CREATE USER;
  GRANT ADMIN` batch can no longer promote itself — the created user exists
  without admin rights.
- "Empty catalog ⇒ trusted" now applies only to embedded/trusted sessions; an
  authenticated session whose catalog is emptied is denied, not promoted.
  Role-granted ADMIN is honored consistently, and the server's startup text no
  longer claims there is no authentication.

### Security hardening — pass 1: extension loading ✅
- **`LOAD EXTENSION` is off by default** everywhere (engine, CLI, C ABI,
  bindings); enable it explicitly with `--allow-load-extension` /
  `PINKDB_ALLOW_LOAD_EXTENSION`, the new C-ABI switch, or the binding opt-ins.
- **Opening a database file can no longer execute native code recorded in its
  WAL**: replay refuses a recorded `LOAD EXTENSION` unless the library is
  already mapped in the process or the new replay opt-in + extension root
  allow it — otherwise it is a hard, actionable error (the audit's reproduced
  attack is now a regression test, marker absent).
- Extensions are path-confined: no bare loader-path names, no `..`, no
  symlinked libraries, plus an optional `--extension-root` on server and CLI.

### Security hardening — low-severity batch ✅
- Redacted the login password from the server protocol's `Debug` output (L-2)
  and the SigV4 canonical request / session token from `Signature`'s (L-11).
- Constant-time key-material comparison in the encryption layer (L-3).
- Every lake commit-delta path is validated, and local storage refuses
  absolute/`..` paths (L-12); the stale extracted SigV4 copy is deleted.
- Checked arithmetic in all `.pink` RLE decoders (32-bit overflow, L-13).
- The Cloudflare API token is scoped to the deploy step only (L-14).
- The four `.lake` subcommands that bypassed `PINKDB_KEY` now use the keyed
  constructor; the S3 test harness pins image digests.

## 2026-10-19

### Wave 75 — core `httpfs` extension (remote files for every format) ✅
- New shared transport `pinkdb-httpfs`: the lake's SigV4 S3 client extracted
  behavior-identically, plus https GET and **byte-range reads**, with
  `Fetcher` as the contract.
- Every registered format reads remote paths: CSV (whole-object), **Parquet
  via a ranged `ChunkReader`** (footer/row-group pages — no whole-file
  downloads when the size is known), Arrow (whole-object, documented).
  Claims are by extension regardless of scheme; `--features
  parquet,arrow,httpfs` composes, and default binaries stay transport-free.

### Wave 77 — SQL macros ✅
- `CREATE [OR REPLACE] MACRO … AS expr` and `AS TABLE SELECT …`, with
  `DROP MACRO [IF EXISTS]`, parameter defaults and named arguments, expanded
  by **bind-time AST substitution** (no dynamic SQL — call-site typing,
  invoker rights and injection safety hold). Usable in expressions, FROM,
  aggregates, windows, CTEs, prepared statements, views and other macros;
  recursion is refused cleanly.
- `SHOW MACROS` / `DESCRIBE MACRO`; macros persist in `.pink` (flag bit 6,
  loaded before MVs) and are WAL-logged DDL; invocation keeps table grants
  enforced. +98 KB on the default binary.

### Wave 70 — core `parquet` extension (read-side) ✅
- Parquet reads with the DuckDB surface: `read_parquet`/`parquet_scan`
  (globs, directories, multi-file, `union_by_name`, hive partitions,
  `filename`/`file_row_number`), four metadata functions, a replacement scan
  (`FROM 'x.parquet'`), projection pushdown and row-group-statistics filter
  pruning with visible counters.
- Landed with the **unified file-format surface**: one read path
  (`TableSource`) shared with arrow, one grammar (`Function` + `Path`), the
  COPY sink kept separate, and logged file reads replay by re-reading
  (DuckDB's rule). `parquet` is opt-in (+1.1 MiB); the default binary pays
  ~18 KB for the surface.

### Wave 71 — core `arrow` extension (Arrow IPC) ✅
- New engine **file-format extension surface** (`pinkdb::filefmt`): arrow
  files work through `FROM 'x.arrow'`, `read_arrow`/`read_ipc`/`read_feather`
  and `COPY … (FORMAT arrow)`, with durable replay re-reading files and lean
  builds refusing everything with a precise error.
- `arrow` is an **opt-in CLI feature** (+≈3.0 MiB; the default binary grows
  only 22 KB for the seam). Round-tripped against pyarrow in both directions;
  DuckDB has no Arrow support to diff against (documented). The Arrow C Data
  Interface is pinned as a follow-up. `.pink` remains the native format.

### Wave 54 — core `math` extension ✅
- Seventeen functions via the Rust SDK: `exp`/`ln`/`log`/`log2`/`log10`, the
  trigonometric family, `degrees`/`radians`/`pi`, and the first **volatile**
  function — `random()` refuses to run in any WAL-logged statement (replay
  could diverge), while reads work normally. `sqrt`/`pow`/`floor`/`ceil`/
  `sign`/`greatest`/`least` remain core built-ins.
- SDK gained ranged arity (optional/variadic — linked-only by construction),
  and integer/DECIMAL arguments now widen into DOUBLE parameters like DuckDB.
  `math` joins json+datetime as a **default feature**; +25 KB default.

### Wave 69 — prepared statements in the Python and Go bindings ✅
- Both bindings gained idiomatic prepared-statement APIs over the FFI: Python
  `db.prepare(sql)` → `Statement.execute(*params)`, Go `db.Prepare(sql)` →
  `Stmt.Execute(args...)` — typed parameters, NULLs, reuse and clean close
  semantics, with native error strings passed through unchanged.
- No C-ABI changes; the bindings also caught up on the RBAC result kinds
  (17–23). Python 56 tests, Go 38 (with `-race`).

### Wave 56 — core `datetime` extension ✅
- Ten DuckDB-pinned datetime functions over naive `DATE`/`TIMESTAMP`:
  `strftime`, `strptime`, `epoch`/`epoch_ms`, `dayofweek`/`dayofyear`/`week`,
  `last_day`, `age`, `make_date` — hand-rolled civil-date and format engines,
  zero new dependencies.
- The extension ABI gained an additive **INTERVAL** value type so `age` returns
  a real interval; `datetime` joins `json` as a **default CLI feature**
  (`--no-default-features` for lean). +40 KB on the default binary.

### Wave 53 — core `string` extension ✅
- Ten DuckDB-pinned string functions built with the Rust SDK: `split_part`,
  `reverse`, `left`/`right` (now parse unquoted), `strpos`,
  `starts_with`/`ends_with`, `regexp_extract`, `regexp_matches`, plus pinkdb's
  `regexp_match` (a pinned divergence — DuckDB has no such function) with
  DuckDB's full option surface.
- `trim`/`ltrim`/`rtrim`/`replace` stay core built-ins (extensions can't shadow
  them). Loaded at runtime; the default CLI binary grows **0 bytes**.

### Wave 52 — core `json` extension ✅
- Six DuckDB-pinned JSON functions over `VARCHAR` — `json_extract`,
  `json_value`, `json_valid`, `json_array_length`, `json_object`,
  `json_array` — with a hand-rolled parser (zero new dependencies) that matches
  DuckDB on the messy corners: trailing commas, `NaN`/`Infinity`, duplicate
  keys, canonical number rendering, escapes.
- `json` is a **default CLI feature** (`--no-default-features` for the lean
  binary); the functions register through the linked registry, so durable
  replay resolves them automatically.

### Wave 51 — Rust extension SDK + volatile flag ✅
- New `ext-sdk/rust`: declare an extension once (`pinkdb_extension!`) and get
  both the native ABI v1 cdylib exports and the wasm/all-target linked
  registration from safe Rust — typed args, `Option` NULLs, `Result` errors,
  no `unsafe` in author code.
- Additive **ABI v1 volatile flag**: functions can declare themselves
  nondeterministic, and the engine refuses to call them in any statement it
  WAL-logs (replay could diverge) with an explicit error; deterministic
  extensions are unchanged.

- **Fixed:** the SDK's ABI exports (`pinkdb_extension_abi_version` /
  `_name` / `_init`) are now **opt-in** — `pinkdb_abi_exports!()` (or the
  `abi-exports` feature for dual-artifact crates, as `extensions/string`
  does) — because emitting them from linked-only extensions caused duplicate
  symbols on Linux linkers. Cdylib authors add the macro; linked-only crates
  need nothing.

### Wave 50 — performance pass ✅
- High-cardinality `GROUP BY` / `COUNT(DISTINCT)` got a typed fast path (no
  per-row values, chunked output, adaptive one-shot reserve): `group_id`
  15.8 → 4.4 ms and `distinct_id` 15.6 → 4.6 ms — both now ahead of the DuckDB
  reference on the same corpus.
- Byte-slice hashing went word-wise: `distinct_lowcard` 3.8 → 3.1 ms and
  `join_probe` 5.3 → 4.6 ms.
- Standing battery total **101.5 → 75.7 ms (−25%)**; all correctness gates
  unchanged.

## 2026-10-18

### Wave 48 — RBAC: users, roles and enforced grants ✅
- New **users and roles** hashed with Argon2id: `CREATE USER`/`CREATE ROLE`,
  `GRANT`/`REVOKE` (table `SELECT`/`INSERT`/`UPDATE`/`DELETE`, database `CREATE`,
  global `ADMIN`), persisted in the catalog + WAL — plaintext passwords never
  hit disk.
- The network server gains **session login** (protocol v9) and **per-statement
  enforcement**: an empty catalog trusts connections with a loud startup warning;
  once a user exists every statement is checked against their grants. Embedded,
  FFI and wasm use stays trusted by design.
- Remote CLI gains `--user`/`--password` (or `PINKDB_PASSWORD`); without the
  `tls` feature, login passwords cross the wire in cleartext — documented.

### Wave 49 — extensions on wasm ✅
- Extensions now work on **every target** through a new **linked registration**
  model: extensions register through a safe-Rust API and feed the same engine
  registry as native ABI v1 extensions. On wasm (where `dlopen` cannot work),
  `LOAD EXTENSION '<name>'` resolves a registered name; unknown names fail with
  an actionable error.
- The **JS / wasm-bindgen host contract** is pinned — surface, SQL↔JS value
  mappings and sandbox rules, documented in `docs/wasm-extensions.md` — with no
  new dependencies added.
- Native CLI grows ~3.7 KB; the wasm dependency tree gains nothing.

### Wave 46 — network server TLS ✅
- Opt-in `tls` feature: the server serves the same protocol over
  **rustls**-encrypted TCP (`--tls-cert/--tls-key`), and the CLI's remote mode
  gained `--tls`, `--tls-ca-cert` and `--tls-insecure`. Certificate problems
  refuse startup with precise messages; handshake failures never disturb the
  accept loop; the default build and dependency graph are unchanged.
- Still **no authentication** — TLS encrypts the transport only, which the docs
  and startup warning state explicitly.

### Wave 47 — async I/O at the boundary ✅
- `pinkdb-server` gains an opt-in **`async-server`** transport: a tokio
  listener with one task per connection, engine calls on the blocking pool,
  and **byte-identical protocol frames** to the synchronous server (same
  client works unchanged). `--transport sync|async`.
- The "zero async" rule now has its documented carve-out: async lives only at
  the server I/O boundary — the executor, storage and WAL stay synchronous —
  and the default build links no runtime (+0.38 MB only with the feature).

### Wave 82 — Rust toolchain upgrade ✅
- All crates now require **Rust 1.97** and compile as **edition 2024**; CI's
  MSRV job pins 1.97 (was 1.89 core / 1.95 pink-lake), and the wasm check
  passes again.
- Modernized the code where the new edition/compiler asks for it — let-chains
  (~60 collapsed conditionals), `unsafe(...)` attribute syntax, reserved
  `gen` renames — with semantics unchanged (duckdiff still 171/171).

### Wave 44 — stored procedures ✅
- **SQL procedures**: `CREATE [OR REPLACE] PROCEDURE p(a INTEGER, …) AS $$ … $$`
  (typed `DECLARE`/`SET`, `IF/ELSIF/ELSE`, `WHILE`, nested blocks, `RETURN`,
  `RAISE`), `DROP PROCEDURE [IF EXISTS]`, `CALL p(args)` — parameters and
  variables are bound as **values, never spliced SQL**, so injection-shaped
  input is inert by construction. `CALL` is statement-atomic, works in
  transactions, is persisted per database and replays exactly from the WAL.
- **JavaScript procedures**: the sandbox **contract** ships (`pinkdb-js`:
  host API, denied-intrinsics determinism policy, conversions) with a clean
  refusal — the engine binding (quickjs-ng, ~1.1 MB) is the queued follow-up
  (wave 81).

### Wave 74 — lake naming sweep ✅
- The lake builds are now **`pinklake`** (the `.pink`-file lake; formerly
  `lake-lite`) and **`vortexlake`** (the Vortex-file lake; formerly `lake`),
  in cargo features, help text, tests, CI, README and the docs site.
- **`s3` is pure storage**: it implies neither build — combine explicitly,
  `pinklake,s3` or `vortexlake,s3` (`--features s3` alone has no `.lake`
  commands and says so). No old aliases survive.

### Wave 80 — materialized-view refresh policies ✅
- **Manual**: `REFRESH MATERIALIZED VIEW name` (and `REFRESH name`, plus
  `REFRESH MATERIALIZED VIEWS` refreshing every view of the database
  all-or-nothing); `SHOW MATERIALIZED VIEWS` lists schedule, last refresh
  time/count and staleness.
- **Scheduled**: `CREATE MATERIALIZED VIEW v REFRESH MANUAL | EVERY INTERVAL
  '5 minutes' | ON COMMIT AS SELECT …` — enforced **daemonless** at statement
  boundaries (embedded and `pinkdb-server` alike; never mid-transaction), with
  automatic refreshes WAL-logged and replay-exact.
- Fix: MV definitions are now WAL-logged and persisted — previously a durable
  reopen silently lost them.

### Wave 68 — inlined small writes & CDC ✅
- Tiny commits (≤ 10 rows by default; per-table limits, `PINKDB_LAKE_INLINING_ROW_LIMIT`)
  live **in the catalog metadata** instead of new data files — scans, stats,
  time travel, partitions and encryption all see them correctly. `.lake flush`
  materializes them into real files and keeps the history.
- **CDC reads**: `.lake changes` (and the library's
  `table_insertions`/`table_deletions`/`table_changes`) replay each version's
  deltas as `snapshot_id`/`rowid`/`change_type` rows. Updates read as
  delete + insert (documented).
- `.lake add_data_files` registers existing lake files into a version with
  exact schema and statistics checks — encrypted lakes included.

### Wave 42 — hash indexes ✅
- `CREATE INDEX [IF NOT EXISTS] name ON t (col)` and
  `DROP INDEX [IF EXISTS] name` (per-database): a single-column **hash index**
  turns `col = literal` lookups into an `IndexScan` instead of a full scan —
  results are identical either way. `.pink` persistence is queued as the
  follow-up (wave 79).

### Wave 43 — materialized views ✅
- `CREATE MATERIALIZED VIEW … AS SELECT`, `REFRESH [MATERIALIZED VIEW]` (full
  recompute) and `DROP MATERIALIZED VIEW [IF EXISTS]`: stored as a queryable
  table plus its refresh query, per-database, WAL-logged and persisted.

### Engine fixes and performance
- Bug-review remediation: server ETag validation, both FFI result-building
  paths guarded, VARCHAR string-offset-limit refusal, attach row-count
  re-checks. Performance: COUNT(*) dead-work elimination and fast DISTINCT for
  integer/date/timestamp keys.

## 2026-10-17

### Bug-review remediation — P1 + P2 ✅
- **P1 (12)** — Window filter pushdown (wrong ranks), mixed-operand JOIN keys
  (panic), `ALTER … DEFAULT` depth/parameter guards, JOIN/UNION plan depth,
  RANGE-frame NULL keys, plaintext WAL mid-file corruption (silent data loss),
  atomic + guarded `save`, legacy-head lake commits (unreadable catalog),
  delete-file count overflow, plaintext staging copies, and the server's
  `ExecutePrepared` / `Rows` allocation caps.
- **P2 (engine)** — `||` on the right of `=`/`LIKE`/`BETWEEN`, SELECT-alias
  shadowing within one list, set-op `NULL` arms, checked decimal/temporal
  overflow, an optimizer index guard, INTERVAL hashing, lazy CASE/COALESCE,
  WAL directory fsync + checked LSN/decoders, the COMMIT conflict set, and
  unsupported WAL container versions.
- **P2 (lake/S3/CLI/server/bindings)** — monotonic lake `schema_version`,
  partition error names, container-version classification, conditional-retry
  guard, SigV4 whitespace, CLI fail-closed key/backoff/partition guards,
  loopback parsing, atomic + Windows-safe repo files, terminal-escape output
  escaping, checked prepared handles, an FFI unwind guard, DECIMAL/INTERVAL/
  `ALTERED_TABLE` in the Python and Go bindings, a client batch cap + socket
  timeouts, off-thread over-limit rejection, and documentation drift.
- No deferred items: the last two (a `.pink` VARCHAR chunk over the `u32`
  string-offset limit is refused at serialization; attach recomputes the row
  count from the deduped delete layout) are fixed too.

### Wave 67 — lake maintenance ✅
- **Compaction**: `merge_adjacent_files` publishes one new version referencing
  consolidated files — groups of delete-free files sharing partition values and
  schema epoch (≥2 members) merge into one; source files are **never touched**
  (bytes and mtimes pinned by tests) and row counts/zone maps stay exact.
  `rewrite_data_files` materializes deletes above a threshold.
- **Retention**: `expire_snapshots` removes old snapshots + commit deltas
  (explicit versions, `older_than`, or `retain_last` — never the current
  version); `cleanup_old_files` / `delete_orphaned_files` delete only
  unreferenced `data/` objects, behind a 2-day age gate and an explicit
  dry-run/confirm; encrypted lakes included.
- **Introspection**: `.lake files / table_info / options / settings` (+
  `merge`, `rewrite`, `expire`, `cleanup`, `orphans`).

### Wave 57 — lake encryption at rest ✅
- With the `encryption` feature, a keyed session (`PINKDB_KEY`, consistent with
  the engine) seals **every lake object** — catalogs, snapshots, commit deltas,
  data files, delete files — as `PNKLAKE1` **AES-256-GCM** containers under one
  Argon2id derivation per session. A `Storage`-level decorator means commits,
  deletes, time travel and retry paths are covered with no per-call-site code.
- The `catalog.json == snapshots/<v>.json` byte invariant survives (seal once,
  publish the same ciphertext); **S3 conditional-write CAS is unchanged**;
  statistics-fast attach on encrypted lean lakes still decodes zero data files.
- Wrong/missing keys and tampered containers fail closed with authenticated
  errors (never “corrupt file”); plaintext lakes stay byte-identical and are
  refused by keyed sessions. Encrypted Vortex attach reads whole objects in
  stage 1 (segment-level reads documented as the follow-up).

### Wave 65 — lake schema evolution ✅
- `ALTER TABLE` on writable-attached lake tables — `ADD COLUMN … DEFAULT`,
  `RENAME COLUMN`, `DROP COLUMN` (plus the engine-wide `ALTER TABLE` support
  behind it: defaults, `NOT NULL` rules, `DESCRIBE` defaults, WAL replay) —
  published as **metadata-only** catalog schema versions (format now **v5**):
  old data files are never rewritten; they project through rename chains,
  missing columns materialize their defaults, and dropped columns keep their
  bytes (row positions unchanged — delete files stay valid).
- **Time travel reads each version with that version's schema and names**;
  writes across versions keep working through the projection.
- Limits (documented): `RENAME TABLE` through a lake commit is refused with a
  `.lake save` hint; no `ALTER COLUMN TYPE` / `SET|DROP DEFAULT` /
  `ADD CONSTRAINT`; read-only attachments refuse `ALTER`.

### Wave 66 — lake partitioning & pruning ✅
- Lake tables can be **partitioned**: identity values or `bucket(N)` labels are
  recorded per file, commits write **one data file per partition value set**,
  and spec changes (`.lake partition … by (…) | clear`) never rewrite old
  files.
- Every v4 data file publishes a CRC-protected **zone map** in the catalog
  (Vortex files newly); the planner prunes whole files for exact predicates —
  `WHERE region = 'x'` reads only that partition's file, and disjoint ranges
  read zero.
- `COUNT`/`MIN`/`MAX` honesty rules are preserved (fall back once positional
  deletes exist); pruning stays correct with deletes and across time travel.
- Limits (documented): bucket labels are pinkdb's own hash (no cross-engine
  numbers, no bucket-value pruning); `year/month/…` transforms are refused by
  name.

### Wave 73 — docs catch-up, changelog & site wiring
- Follow-up: a **SQL dialect reference** page — statements / queries / types
  tables plus the known-divergences list (correlated subqueries, USING/NATURAL
  joins, three-part references, CTE `AT` handling) — added under Reference.
- Website docs brought current through waves 58–64: **SQL** (prepared statements
  with the `prepare` → `bind` → `execute_bound` flow, DECIMAL/INTERVAL with the
  DuckDB promotion rules, declarative keys + FK→PK elimination, full/multi-way
  joins, the new function set, `EXPLAIN`/`SHOW`/`DESCRIBE`, set operations),
  **persistence** (`.pink` v5 with the constraint section and type tags; WAL v4
  touched sets; optimistic commits and `PINKDB_TXN_RETRIES`), **lake**
  (incremental commits with the 1M-row numbers and DuckLake retry knobs, writable
  attach, `AT (VERSION/TIMESTAMP)` time travel, per-version records, catalog
  `format_version` 4), **CLI** (new `.lake` commands, author/message, retry env
  vars), **server** (protocol v6, `Prepare`/`ExecutePrepared`/`Prepared` frames),
  plus landing/roadmap/API/guide updates.
- New **Changelog** docs page generated from this file
  (`website/tools/make-changelog.py`), linked from the sidebar, footer and
  roadmap page.
- CI's docs job now regenerates the changelog fragment + the site and **fails on a
  stale diff** — regenerate-and-commit is enforced; `website/README.md` documents
  the tooling.

### Wave 63 — incremental lake writes ✅
- Lake commits now **append**: each commit writes one new immutable data file
  per table (with its exact row count in the catalog) and never rewrites a
  published file — a 1M-row lake committed 4 rows in **7.6 KB / 0.54 s with 0
  bytes rewritten** (vs a 2.6 MB full save).
- **DELETE / UPDATE** write positional delete files applied at read time
  (merge-on-read); every published version also writes a small
  `commits/<v>.json` delta manifest.
- **Writable attached lakes**: `.lake attach <dir> write`, mutate with normal
  SQL, `.lake commit` publishes the diff; guarded `.lake detach`; durable
  sessions replay commits idempotently after a crash.
- **Concurrent-writer retry** with DuckLake's settings and defaults
  (`max_retry_count` 10, `retry_backoff` 1.5, `retry_wait_ms` 100 — rebase on
  conflict, bounded, clean failure).
- Fixes after landing: the feature-less build message and the S3 Docker
  expectations updated for the new catalog format (`format_version` 4).

### Wave 64 — lake time travel & snapshot metadata ✅
- Query any published lake version: `SELECT … FROM t AT (VERSION => n)` /
  `AT (TIMESTAMP => ts)` on attached lake tables, with DuckLake-pinned
  resolution (timestamp = newest snapshot at or before it, inclusive).
- **Per-version records**: `.lake snapshots` lists time, author, message,
  `(tables, rows)` and a change summary; `.lake save … author '…' message '…'`
  attaches commit metadata.
- Statistics fast paths work per version: `COUNT`/`MIN`/`MAX` over an older
  version decode no data files.
- `ducklake_default_version` deliberately not mirrored (probed: it selects a
  new-catalog storage format, not a read default).

### Wave 41 — primary / foreign keys ✅
- Declarative `PRIMARY KEY` / `UNIQUE` / `NOT NULL` / `REFERENCES` in column
  and table form (`CONSTRAINT` names; `RELY` / `[NOT] ENFORCED` accepted and
  dropped), validated at DDL time like DuckDB.
- Constraints are **metadata-only** — writes never enforce them; `DESCRIBE`
  reports `PRI`/`UNI` markers and nullability.
- Optimizer: conservative **FK→PK join elimination** (declarations are
  trusted, not verified — documented).

### Wave 62 — DECIMAL and INTERVAL types ✅
- Full DECIMAL(38) and INTERVAL support: literals, arithmetic with DuckDB's
  promotion rules, comparisons, sums, CSV import/export, date ± interval.
- `.pink` v5 (new type tags), server protocol v6, FFI text cells for both.

### Wave 59 — prepared statements ✅
- `?` placeholders with bind-time type inference; engine `prepare` → `bind` →
  `execute`; server protocol `Prepare`/`ExecutePrepared`; FFI
  prepare/bind/execute. (Python/Go wrappers: wave 69, queued.)

### Process notes (no user-visible change)
- DuckLake feature-gap audit (live probes) queued as waves 63–68; extension
  waves 70–72 queued (parquet, arrow, iceberg).
- Disk-full incident and recovery: the four-worktree batch was salvaged and
  landed wave by wave.
- Multiprocess test-harness determinism fix (macOS CI flake).
- Fresh-day benchmark record: query battery 234 ms; multi-writer 2.55×; lake
  25.1 MB vs DuckLake 91.4 MB at 10M rows.

## 2026-10-16
- **Wave 55** — performance round (column-vs-literal comparison fast path;
  battery best 235.5 ms).
- **Wave 58** — SQL completeness (multi-way/composite/non-equi/CROSS/FULL
  joins, INSERT…SELECT, CTAS, IF [NOT] EXISTS, OFFSET, NULLS FIRST/LAST,
  INTERSECT/EXCEPT, COUNT(DISTINCT), EXPLAIN, SHOW/DESCRIBE, window frames).
- **Wave 60** — functions (`lag`/`lead`/`ntile`/`first_value`/`last_value` +
  scalar gaps).
- **Wave 61** — optimizer (projection pushdown, LIMIT short-circuit, join
  build-side choice, filter-through-aggregate).
- **Wave 45o** — optimistic multi-writer commits (stage 5): concurrent local
  writers on one durable store with WAL touched sets and automatic retry.
- **Wave 45n** — multi-process stability hardening (chaos soak harness).
- **Wave 45m** — multi-process durable `.pink` writers.
- Benchmark audit (battery ≈253 ms on an idle machine) and integration
  close-out (45o → 60 → 61 → 58 → CI green); first fully green nine-job CI
  pipeline.

## 2026-10-15
- **Wave 45k** — exact attach fast paths for lean lakes (published per-column
  statistics blocks).
- **Wave 45j** — `.pink` LZ frames (text corpora 3–5× smaller).
- **Wave 45i** — lean lake build (`lake-lite`, 2.69 MB).
- **Wave 45h** — `.pink` format v4 (flags word, typed codecs).
- **Wave 45e** — `.lake attach` projection pushdown.
- **Wave 45d** — `.lake attach` (footer-only, statistics-driven).
- **Wave 45c** — lake open 3.7× faster.
- **Wave 45b** — writer block-size A/B.
- **Wave 45** — recurring performance wave, expanded into the lake-scan
  program (sub-waves 45b–45o).

## 2026-10-14
- **Wave 40c** — CI + docs truthfulness (fmt gate, macOS/MSRV/audit jobs,
  docs sync).
- **Wave 40b** — durability and operability hardening.
- **Wave 40a** — safety hardening from the waves 0–40 gap audit.
- **Wave 40** — performance tuning.
- **Wave 39** — CI/CD publishing of extensions.
- **Wave 36** — S3 bucket support for lake storage + file reading.
- **Waves 35 + 35b** — Vortex default lake format + parser cleanup.
- **Wave 34** — pink-lake minimal lake layer.
- **Wave 33** — custom repositories + client-side INSTALL.
- **Wave 32** — extensions repository.
- **Wave 31** — Go extension SDK.
- **Wave 30** — C/C++ extension SDK.

## 2026-10-13
- **Wave 29** — extensions support (native ABI v1 + runtime loader).
- **Wave 28** — Go binding.
- **Waves 37–38** — documentation website + hosting.
- **Wave 27** — Python binding.
- **Wave 26** — C ABI (`pinkdb-ffi`).

## 2026-10-12
- **Wave 25** — at-rest encryption (`PINKENC1`: AES-256-GCM + Argon2id).
- **Wave 24** — network server (`pinkdb-server`, protocol v2).
- **Wave 23** — multi-database (`CREATE/DROP DATABASE`, `USE`, `db.table`).
- **Wave 22** — WAL / crash recovery.
- **Wave 21** — PEG parser default.
- **Wave 20** — outer joins.
- **Wave 19** — join fast path.

## 2026-10-11
- **Wave 18** — UNION binder fix for derived tables/CTEs/views/subqueries.
- **Wave 17** — performance tuning.

Waves 0–16 (through 2026-10-10) predate this changelog — core engine, SQL,
CSV, CLI, persistence and the first extension groundwork; see the git history
and the ROADMAP wave table.
