# pinkdb — v1 Plan

A mini DuckDB: an embedded **analytical** SQL database in Rust — in-memory columnar
storage, vectorized execution, SQL with aggregates and joins, CSV import.
Ships as both a **library crate** (`pinkdb`) and a **CLI REPL** (`pinkdb-cli`).

## Decisions (locked with user, Oct 2026)

| Decision | Choice |
|---|---|
| Language | Rust (edition 2021, zero async, no unsafe) |
| Interface | Library + REPL (CLI is a thin consumer of the library) |
| v1 scope | In-memory storage + CSV import (no persistence, no UPDATE/DELETE) |
| Deps | `csv` (lib), `rustyline` (cli) only — hand-written SQL lexer/parser |

## Architecture

```
SQL text
  → Lexer + recursive-descent parser → AST
  → Binder (resolve tables/columns, assign types)
  → Logical plan (scan → filter → join → aggregate → project → sort → limit)
  → Optimizer (predicate pushdown, constant folding)
  → Vectorized executor (pull/Volcano; operators exchange DataChunks of ≤2048 rows)
  → In-memory columnar storage
```

### Workspace layout

```
pinkdb/
├── Cargo.toml               # workspace: crates/pinkdb, crates/pinkdb-cli
├── crates/
│   ├── pinkdb/              # the library (engine)
│   │   └── src/
│   │       ├── lib.rs       # pub mods; re-export PinkDb
│   │       ├── types.rs     # DataType, Value, PinkDbError
│   │       ├── vector.rs    # Vector, Validity bitmask, DataChunk
│   │       ├── storage.rs   # Column, ColumnMeta, DataTable, PinkDb (catalog)
│   │       ├── csv.rs       # CSV reader + schema inference
│   │       ├── sql/         # lexer, ast, parser, binder
│   │       ├── plan.rs      # logical plan + optimizer
│   │       └── exec/        # operators, expression eval, aggregates
│   └── pinkdb-cli/
│       └── src/main.rs      # rustyline REPL, dot-commands, output rendering
```

### Core design decisions

1. **Vectors, not rows.** `Vector` = typed column chunk (`enum VectorData`:
   `Boolean(Vec<bool>)`, `Int32(Vec<i32>)`, `Int64(Vec<i64>)`, `Float64(Vec<f64>)`,
   `VarChar(Vec<String>)`) + a **validity bitmask** (`Vec<u64>`, bit set = valid),
   DuckDB-style. Every operator processes a whole `DataChunk` per call.
2. **Selection vectors.** `WHERE` produces a selection vector (no row copying);
   `Vector::gather` applies it downstream.
3. **Types for v1:** BOOLEAN, INTEGER (i32), BIGINT (i64), DOUBLE (f64), VARCHAR.
4. **Storage:** `DataTable` = per-column `Vec<Vector>` chunks of CHUNK_SIZE=2048;
   `PinkDb` catalog maps table names → tables. Single connection, single thread.
5. **CSV inference:** per column over all non-empty cells: i64-fits-i32 → INTEGER,
   i64 → BIGINT, f64 → DOUBLE, true/false (ci) → BOOLEAN, else VARCHAR.
   Empty cell → NULL.
6. **Executor:** pull-based operators — Scan, Filter, Project, HashAggregate,
   HashJoin (inner, equi), Sort, Limit. Expression evaluation vector-at-a-time.
7. **Optimizer (v1):** predicate pushdown + constant folding. (Pruning later.)

### SQL surface for v1

```sql
CREATE TABLE t (id BIGINT, name VARCHAR, price DOUBLE);
INSERT INTO t VALUES (...);
SELECT [DISTINCT] expr [AS alias], ...
FROM t [JOIN u ON t.x = u.y]     -- inner hash join
WHERE expr                        -- incl. IS [NOT] NULL, BETWEEN
GROUP BY ... HAVING ...
ORDER BY expr [ASC|DESC], ...     -- NULLS LAST; positional ORDER BY 2
LIMIT n;
```

- Aggregates: `COUNT(*)`, `COUNT(x)`, `SUM`, `AVG`, `MIN`, `MAX`
- Scalar functions: `abs`, `round`, `upper`, `lower`, `length`, `substr`
- Numeric coercion: INTEGER + DOUBLE → DOUBLE; int `/` int → DOUBLE (DuckDB rule)
- SQL three-valued logic: NULL fails WHERE; HAVING filters after aggregation

### CLI

- REPL prompt `pinkdb> `, rustyline line editing, multi-line until `;`.
- Dot-commands: `.help`, `.quit`/`.exit`, `.tables`, `.schema <t>`,
  `.import <file.csv> [table]`.
- Box-drawn result tables with a type row (DuckDB-style) + `Run Time (s): real X.XXX`.

## Milestones

| # | Deliverable | Proof |
|---|---|---|
| M0 | Workspace scaffold, REPL loop | `cargo run` opens the shell |
| M1 | Vectors, tables, catalog, CSV import w/ inference | `.import sales.csv sales` + `.tables` |
| M2 | Lexer/parser/binder + Scan/Filter/Project/Limit | `SELECT name, price FROM sales WHERE price > 100 LIMIT 10;` |
| M3 | Expressions, functions, GROUP BY/HAVING, ORDER BY, DISTINCT | `SELECT region, AVG(total) FROM sales GROUP BY region ORDER BY 2 DESC;` |
| M4 | Hash join + optimizer (pushdown, constant folding) | Two-table join queries |
| M5 | Polish: box output, timing, README, 1M-row benchmark | Pretty output + timings |

## Implementation waves (subagent dispatches)

| Wave | Scope | Status |
|---|---|---|
| 1 | M0–M1: scaffold, types/vector/storage/csv, skeleton CLI | done (build+test+clippy green, 18 tests) |
| 2 | M2–M3: SQL frontend + single-table executor + aggregates/sort/DISTINCT + CLI output | done (build+test+clippy green, 45 tests) |
| 3 | M4–M5: hash join, optimizer, README, benchmark, polish | done (build+test+clippy green, 51 tests, 1M-row bench) |
| 4 | Independent code review of full codebase; fix findings | done (4 critical + 3 major fixed, 58 tests green) |
| 5 | Persistence: own on-disk columnar format (RLE/dictionary/bitpack), `.save`/`.open` | done (build+test+clippy green, 64 tests, 1M-row save/load bench) |
| 6 | Subqueries + CTEs: derived tables, scalar/IN/EXISTS (uncorrelated), WITH | done (build+test+clippy green, 72 tests) |
| 7 | DATE/TIMESTAMP types: storage, literals, arithmetic, date functions, CSV + persist | done (build+test+clippy green, 81 tests) |
| 8 | Window functions: ranking + aggregates OVER, PARTITION/ORDER/ROWS frames | done (build+test+clippy green, 86 tests) |
| 9 | UPDATE/DELETE: physical DML, simultaneous SET, statement-atomic | done (build+test+clippy green, 92 tests) |
| 10 | Transactions: BEGIN/COMMIT/ROLLBACK via whole-store snapshots | done (build+test+clippy green, 95 tests) |
| 11 | SQL completeness: UNION, views, CAST, COALESCE, LIKE, DROP | done (build+test+clippy green, 98 tests) |
| 12 | Parallel execution (rayon) + multi-writer Database/Connection | done (build+test+clippy green, 103 tests, 1M-row bench) |
| 13 | Differential testing vs DuckDB (53 queries match) | done (bench/duckdiff.py) |
| 14 | Speed round: Arc storage, borrowed codecs, FNV hashing, COUNT(*) path | done (build+test+clippy green, duckdiff 53/53, 1M-row bench) |
| 15 | Import speed round: streaming inference, buffer-reused records, zero-alloc bool checks | done (build+test+clippy green, duckdiff 53/53, 1M-row import 0.42→0.03 s) |
| 16 | CASE expressions (searched + simple, NULL semantics, type unification) | done (build+test+clippy green, 119 tests, duckdiff 54/54) |
| 17 | Performance tuning: O(n²) window-aggregate fix, typed expression kernels, sort key arena + top-N, join/group hash paths, REPL display cap, thin LTO | done (build+test+clippy green, 148 tests, duckdiff 54/54, 1M-row query bench) |
| 18 | Binder fix: UNION inside derived tables/CTEs/views/subqueries; ORDER BY/LIMIT apply to the whole set operation | done (build+test+clippy green, 162 lib + 3 CLI tests, duckdiff 56/56) |
| 19 | Join fast path: integer-key specialization + fused join→aggregate (no row materialization) | done (build+test+clippy green, 193 lib + 3 CLI tests, duckdiff 56/56, join_self 64→~9 ms) |
| 20 | Outer joins: LEFT/RIGHT equi-joins (NULL-extended unmatched rows), join-kind-aware pushdown | done (build+test+clippy green, 208 lib + 3 CLI tests, duckdiff 60/60, bench total 493 ms, join entries unregressed) |
| 21 | PEG parser (`peg` crate) in parallel with recursive descent; differential-tested; default flipped | done (build+test+clippy green under PEG default and `PINKDB_PARSER=legacy`, 223 lib + 3 CLI tests both ways, deterministic differential corpus + seeded fuzz differential test, duckdiff 60/60 both modes, bench total unchanged vs legacy; post-review: linear-time function calls, shared 24-level nesting cap, COALESCE `*` parity) |
| 22 | WAL / crash recovery: sidecar `.pink.wal` (format v2: per-generation names + generation in the header), one record per committed transaction unit (autocommit vs `BEGIN..COMMIT`), Sync/Batch policy, logical replay with torn-tail truncation + hard error on CRC-valid bad SQL, checkpoint recovery state machine (publish `.tmp` before removing any roll, generation-checked, idempotent), filesystem-identity `.save` target, durable `.import` checkpoint, view-dependent DML refusal | done (build+test+clippy green, 253 lib + 3 CLI unit + 9 CLI integration tests, duckdiff 60/60, bench total 494–501 ms; post-review fixes: C1 interrupted checkpoint lost on the next open, C2 stale-roll duplicate replay, `.save ./x.pink`/absolute/case-only double-apply, `.import` replay brick, non-monotonic LSN reuse, u32 payload guard, torn-header recreate, dir fsync after the roll; 500 random + 160 targeted SIGKILL iterations zero loss/dup vs 10/150 random + 40/40 targeted on the pre-fix binary) |
| 23 | Multi-database: `CREATE/DROP DATABASE` + `USE` (per-session), qualified `db.table` in FROM/JOIN/INSERT/UPDATE/DELETE/DDL, cross-db joins, per-database views, `.pink` format v2 (v1 loads as `main`), WAL logs `USE`/database DDL with self-contained session-prefixed records, CLI `.databases` + db-scoped `.tables`/`.schema`/`.import` | done (build+test+clippy green, 288 lib + 3 CLI unit + 17 CLI integration tests under PEG default and `PINKDB_PARSER=legacy`, differential corpus 807 valid + 146 invalid, duckdiff 60/60, WAL temp-file leak fixed via self-cleaning test scratch dirs; post-review fixes: C1 a dropped database could leave an unreplayable `USE`-prefixed record — appends now drop the prefix for a vanished session database and `COMMIT` conflicts when a write key or `USE` target vanished (merge fails loudly instead of silently no-op'ing), `.import` treats `x.y` as `db.table` only when `x` is an existing database (dotted stems/quoted names work), `.databases` counts views separately, `.schema` shows views and honors quoting, `.save` view note checks all databases, PEG three-part errors match legacy, caller CTEs no longer leak into view bodies) |
| 24 | Network server: separate `pinkdb-server` crate (lib + `pinkdb-server` binary; `pinkdb` path dep only) — framed binary protocol v2 (u32 LE length + type byte, 64 MiB hard max, every inner length validated against the frame; one `BatchDone { statements }` terminator per `Execute`, so the client never parses SQL to count statements), thread-per-connection with a per-connection `Connection` (wave-23 `USE` isolation per TCP client), `Server::spawn()`/`local_addr()` ephemeral-port helper, durable `--file` mode (wave-22 open + WAL replay on restart), blocking `Client` reconstructing public `QueryResult`/`Value`/`DataType`, CLI `--host` remote mode behind the optional `client` feature (file/catalog dot-commands rejected; no auth/TLS, localhost default, README security posture) | done (build+test+clippy green under PEG default and `PINKDB_PARSER=legacy`: 294 lib + 20 server-crate + 4 durable + 1 cross-parser + 20 protocol + 23 CLI + 2 server doc-tests; 365 total under both modes, 3 remote CLI tests with `--features client`; duckdiff 60/60; query bench total 487–492 ms; kill loop 30/30 clean; release sizes `pinkdb` 2.19 MB, `pinkdb-server` 1.84 MB; post-review fixes: C1 remote stack-overflow abort (`SELECT 1+1+…`, ~200 terms in debug / ~1800 release, aborted the whole server) — a shared post-parse AST-depth guard (cap 64 nodes, iterative walk, and an iterative free of the rejected AST so the drop glue cannot overflow either) now applies to both parsers, and connection threads get 16 MiB stacks: a 10k/1M-term chain returns a clean `Parse` error and the server stays healthy (measured debug crash points in a 2 MiB thread: chains 172, `IN` lists 170, `AND` runs 172, nested-CASE/subquery mixes 129–174; cap 64 keeps ≥2× margin); M2 accept errors are transient (log + 10 ms→1 s backoff; `ulimit -n 48` repro with 60 held connections keeps serving) with a 128-concurrent-connection cap (`bind_with_max_connections`); M3 panics are caught per statement (Error frame + stderr log, connection and server survive; hidden test hook proves it) and poisoned store locks recover loudly via `PoisonError::into_inner()` instead of failing forever; broken-flag client, zero-statement/oversized-value/malformed-corpus/cap/panic tests, concurrent-clients-across-SIGKILL exact-once durable test, remote-vs-local CLI rendering parity, cross-parser `BatchDone` end-to-end; earlier post-review fix: accepted sockets inherit the listener's nonblocking flag on BSD/macOS — connection threads now force blocking IO) |
| 25 | At-rest encryption (core `encryption` feature): `PINKENC1` container (plaintext header with cipher/KDF params/salt/nonce prefix/key-check; AES-256-GCM payloads: 1 MiB checkpoint segments, one AEAD unit per WAL record with the LSN in the AAD; Argon2id for passphrases; fresh nonce prefix per container and per WAL rewrite; best-effort zeroize), `save_with_key`/`open_with_key`, keyed durable opens (`open_durable_with_key`, `Database::open_with_key`) with encrypted checkpoints/rolls/snapshots, `PINKDB_KEY` CLI passphrase, feature-gated `aes-gcm`+`argon2` optional deps, mode-mismatch and wrong-key errors (`wrong key` vs corruption), wave-22 recovery invariants on encrypted WALs (torn tail truncates, mid-file auth failure is a hard error, atomic nonce-fresh rewrite) | done (build+test+clippy green under PEG default and `PINKDB_PARSER=legacy`, both feature sets: 376 tests without the feature / 412 with it, per 16 binaries; 317 lib + 21 encryption integration tests with the feature; duckdiff 60/60; query bench 502–507 ms default under heavy host load (StorageManagementService + WindowServer ~100% CPU), indistinguishable from an interleaved HEAD baseline (704–790 ms in the same window); kill loop 30/30 clean plus encrypted 30/30 random + 15/15 multi-db; release sizes `pinkdb` 2.21 MB default and 2.28 MB with the feature (crypto-free and ~18 KB over the pre-wave 2.19 MB, mostly segment page rounding); no plaintext leakage greps; encrypted binary refuses wrong keys before replay; post-review fixes: C1 a shorter copy of an encrypted WAL ending exactly at a record boundary reused the discarded record's (key, nonce, AAD) — a keyed open now rebuilds the WAL with a fresh random prefix on *every* open (atomic temp+rename, records re-sealed; regression test pins the keystream-XOR relation and was verified to fail against the old behavior), normal reopen/append/checkpoint cycles and counter overflow are pinned; M1 mode/key changes on a live store are refused at checkpoint (`cannot change encryption mode on a live store; reopen it instead`; same mode but a different key is refused too); M2 a set-but-non-UTF-8 `PINKDB_KEY` is a hard CLI error (exit 2), never a silent plaintext save; M3 `save_snapshot` probes the target `.pink`'s mode as well and the README documents the single-writer nonce-reuse consequence (no lockfile by design); m1 the four `needless_return` clippy warnings are gone under both feature sets; m2 Argon2id header parameters are capped at sane multiples of the defaults (m ≤ 76 MiB, t ≤ 4, p ≤ 4, `m·t` ≤ 8× default work; worst accepted ≈ 0.11 s release here vs ~6.6 s for the old caps) with a crafted-header rejection test; m3 `save_snapshot` refuses a target `.pink` in the other mode; m4 a nonzero `crc` on an authenticating encrypted record is a hard error (tamper test pins mid-file and tail); m5 feature-off snapshot-only encrypted `.pink` errors use the standard "file is encrypted; build with the encryption feature" wording; m6 remote mode warns that `PINKDB_KEY` is inert (`pinkdb-server` has no key option; documented); m7 a crafted roll name at `u64::MAX` is a clean generation-overflow error, not a panic; checkpoint containers end with a terminal zero-length segment so a container cut at a segment boundary is rejected instead of loading a silently shorter database; `bench/kill_loop.py` reads the passphrase from the environment, never the command line; fixed-path unit tests now use pid+counter temp files) |

Each wave must end with `cargo build` + `cargo test` + `cargo clippy` green.

## Roadmap (post-wave 21)

| Wave | Scope | Packaging |
|---|---|---|
| 22 | WAL / crash recovery | core |
| 23 | Multi-database: `USE <db>`, qualified `db.table`, per-database persistence | core |
| 24 | Network server: `pinkdb-server` binary + CLI `--host` client mode | separate crate; TLS optional feature |
| 25 | At-rest encryption: encrypted `.pink` + WAL (AEAD + Argon2id, key via option/env) | core `encryption` feature |
| 26 | C ABI: `pinkdb-ffi` (staticlib + cdylib, C headers, example) | separate crate; contained `unsafe` |
| 27 | Python binding (cffi) | `bindings/python` |
| 28 | Go binding (cgo) | `bindings/go` |
| 29 | Extensions support: loadable extensions (`LOAD`/`INSTALL`), versioned C ABI v1, manifest/signature checks | separate `pinkdb-ext-loader` crate; contained `unsafe` |
| 30 | C/C++ extension SDK: ABI headers, templates, examples | in-repo `ext-sdk/c` |
| 31 | Go extension SDK: build extensions as c-shared libraries (cgo) exporting the ABI | in-repo `ext-sdk/go` |
| 32 | Extensions repository: hosted catalog of built extension artifacts (metadata + binaries) | infra service |
| 33 | Custom repositories: user-configurable extension repository URLs and config | core config + repo client |
| 34 | Lake architecture: DuckLake-style decoupled metadata + data storage (catalog, snapshots, data files) | separate crates (storage layer) |
| 35 | Vortex file format as the default lake data format | format crate / extension |
| 36 | S3 bucket support for lake storage and file reading | optional `s3` feature (storage provider) |
| 37 | Documentation website | separate site/track |
| 38 | Host the website and extension repository on Cloudflare Workers + R2 | infra |
| 39 | CI/CD publishing of extensions to the repository (build, test, sign, publish) | infra/CI workflows |
| 40 | Performance improvement: ongoing bench-driven speed rounds | core |
| deferred | Parquet read (`pinkdb-parquet`); JS/wasm, Node-native, Java bindings | separate crates |

Packaging principles:
- New capabilities ship as separate workspace crates and/or opt-in cargo features; the default
  `cargo build` stays lean (~2 MB) and links no crypto/TLS/format code.
- Every wave reports per-artifact release binary size; gates grow a build matrix (default,
  each feature, each crate) plus a wasm check for the core (`pinkdb` and `pinkdb-parquet`
  must stay wasm32-compatible; the server is target-gated out of wasm builds).
- No hand-rolled crypto; audited RustCrypto crates only.
- Zero-unsafe holds for the core; the C ABI crate is the first, contained exception.
- Compile-time engine components ship as separate crates/features (waves 22-28) — distinct from
  waves 29+ loadable extensions, which use a versioned C ABI and a runtime loader (the loader
  crate is the second contained-`unsafe` location after `pinkdb-ffi`).

## Known sharp edges (planned handling)

- GROUP BY keys incl. strings: encode keys as `Vec<u8>` → `HashMap<Vec<u8>, AggState>`.
- NULL ordering: NULLS LAST in sorts; NULL keys group together.
- Type coercion matrix across arithmetic/comparison operators.
- INTEGER overflow in SUM: accumulate integers in i64; document overflow policy.

Post-v1 roadmap: see "Roadmap (post-wave 21)" above.
