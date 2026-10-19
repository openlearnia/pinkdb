# Changelog

pinkdb is developed in numbered waves: each wave ships one focused capability
(or a performance pass) with its own tests and CI gates. This file is the
user-facing summary — one section per wave, newest first — while the
[ROADMAP progress log](ROADMAP.md#progress-log) carries the full engineering
detail (probes, gate counts, integration notes). Entries are appended at
integration time starting with wave 63; earlier waves are backfilled as
one-liners below.

## 2026-10-19

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
