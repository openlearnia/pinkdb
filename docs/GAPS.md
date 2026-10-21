# Gap audit — waves 0–40 (2026-10-14)

An audit of everything ROADMAP.md marks done through wave 40: all six crates, the bindings,
`ext-sdk`, `ext-repo`, the CI workflows and the website. Paths are repo-relative and line
numbers refer to commit `cf25d73`. Findings marked ✔ were reproduced or checked against the
code by hand.

The remediation waves are **40a–40j** in ROADMAP.md, all scheduled ahead of wave 41. Each
item below names the wave that closes it.

## P0 — correctness, data loss, security (wave 40a — closed)

All ten items are fixed in wave 40a, each with a regression test (see the ROADMAP progress log).

| # | Gap | Evidence |
|---|---|---|
| 1 ✔ | Any network client can load an arbitrary native library with `LOAD EXTENSION`. The server has no authentication, and the statement is WAL-logged, so it runs again on every restart. | `crates/pinkdb/src/exec/mod.rs:254`, `crates/pinkdb-server/src/server.rs:478` |
| 2 ✔ | The publish workflow falls back to an empty `index.json` whenever `curl` fails (including a transient 5xx) and then uploads it, wiping the live catalog. | `.github/workflows/publish-extensions.yml:136-141`, `ext-repo/publish.py` |
| 3 ✔ | A table recreated with DROP + CREATE starts again at `version: 0`. A transaction based on the old table can then COMMIT over the new one without a conflict. | `crates/pinkdb/src/storage.rs:76`, `crates/pinkdb/src/db.rs:385-397` |
| 4 | COMMIT validates only the tables a transaction wrote. WAL replay re-runs the SQL text in commit order, so a transaction that read a concurrently committed table can come back with different data after a restart. | `crates/pinkdb/src/db.rs:378-397`, `crates/pinkdb/src/wal.rs:11-17` |
| 5 ✔ | Corrupt `.pink` input can abort the process: the decoder calls `Vec::with_capacity(n)` with an untrusted u32, and out-of-memory aborts can't be caught. The format has no checksums, although `persist.rs:34` claims the reader "never panics". | `crates/pinkdb/src/persist.rs:560,568,596` |
| 6 ✔ | Integer `+ - *`, negation, `abs`, BIGINT→INTEGER casts, date ± n and `SUM` wrap silently. DuckDB raises an error in all these cases. | `crates/pinkdb/src/exec/expr.rs:78,549,632-664,1181`, `exec/aggregate.rs:160` |
| 7 ✔ | `.lake save` opens a fresh lake handle each time, so two sessions that opened the same version can overwrite each other with no conflict error. The module docs promise conflict detection. | `crates/pinkdb-cli/src/lake.rs:16-19,74` |
| 8 ✔ | `.install` accepts `http://` for any host. The SHA-256 comes from the same origin as the artifact, and the library is loaded straight away. | `crates/pinkdb-cli/src/repo/config.rs:153-168`, `crates/pinkdb-cli/src/repo/client.rs` |
| 9 ✔ | S3 `Credentials` and `S3Config` derive `Debug`, so `{:?}` on a `Lake` prints the secret key. | `crates/pink-lake/src/s3/sigv4.rs:26`, `crates/pink-lake/src/s3/mod.rs:124` |
| 10 ✔ | The server allocates the full declared frame (up to 64 MiB) before any body bytes arrive, and sets no read timeout. 128 idle sockets block every other client; 128 bare headers reserve about 8 GiB. | `crates/pinkdb-server/src/protocol.rs:470`, `crates/pinkdb-server/src/server.rs:27` |

## P1 — promised but missing, or operationally broken (waves 40b — closed, 40c)

The 40b items below (WAL growth, write-before-log, bind/execute race, file locking, Python thread safety, CLI scripting, lake retries/versions/guards) are fixed in wave 40b, and the CI and wave-claim items in wave 40c (darwin-only extension publishing moves to 40i); the window-frame items remain for 40d.

- **Wave 29 and 39 claims vs code (40c, 40i).**
  - Wave 29's title promises "manifest/signature checks" and SQL `INSTALL`. Neither exists: the loader only checks the ABI version and symbols (`crates/pinkdb-ext-loader/src/lib.rs:56-75`), and the grammar has only `LOAD EXTENSION`.
  - Wave 39 says "sign", but the signing step is an `echo` (`publish-extensions.yml:286-299`).
- **WAL growth (40b).** Nothing checkpoints automatically, and the server and FFI cannot checkpoint at all, so the WAL and restart-replay time grow without bound.
- **Write-before-log (40b).** A write is applied in memory before its WAL append (`crates/pinkdb/src/db.rs:276-292`). If the append fails, readers keep seeing data that a restart will lose.
- **Bind/execute race (40b).** Binding and execution take separate locks (`db.rs:267-276`), so a concurrent DROP + CREATE with a different schema can leave a stale plan.
- **No file locking (40b).** Combined with the "one handle per thread" advice in the FFI and Python docs (`crates/pinkdb-ffi/include/pinkdb.h:18`), this leads to several durable handles on one file.
- **Python thread safety (40b).** `Database.execute` has no lock and ctypes releases the GIL (`bindings/python/pinkdb/__init__.py:407-428`).
- **CLI scripting (40b).**
  - Errors go to stdout and the exit status stays 0 (`crates/pinkdb-cli/src/main.rs:700`).
  - The banner and `Run Time` always print.
  - A statement left unfinished at EOF is silently dropped.
  - `SELECT 1; -- c` never terminates because the check is `ends_with(';')`.
- **Lake (40b).**
  - No retries on S3 5xx/429/transport errors (`crates/pink-lake/src/s3/mod.rs:45-51`).
  - `versions()` silently skips snapshots it fails to read (`crates/pink-lake/src/lib.rs:556`).
  - `.lake save` inside an open transaction may snapshot uncommitted rows.
  - `.lake open` silently drops the durable WAL binding.
- **Window frames (40d).**
  - With ORDER BY, the default frame is ROWS, not RANGE, so tied keys get different values (`crates/pinkdb/src/sql/binder.rs:144-157`).
  - Invalid frames such as `ROWS UNBOUNDED FOLLOWING` or inverted bounds are accepted and silently return empty results.
- **CI (40c).**
  - Ubuntu only. Not covered: macOS, `cargo fmt --check`, `cargo audit`, `go vet`/`-race`, core `--features encryption`, `-p pinkdb-cli --features client|repo`.
  - Actions are pinned by tag, not by SHA.
  - Only darwin-arm64 extensions are published.

## P2 — feature gaps (waves 40d–40j)

- **SQL (40d).**
  - Joins: one JOIN per FROM with a single-equality ON. No multi-way, composite-key, non-equi, FULL or CROSS joins.
  - Statements: no INSERT…SELECT, CTAS, `IF [NOT] EXISTS`, OFFSET, NULLS FIRST/LAST, INTERSECT/EXCEPT, COUNT(DISTINCT), EXPLAIN, or SHOW/DESCRIBE (`crates/pinkdb/src/sql/peg_parser.rs`).
- **Parameters (40e).** There are no prepared statements or parameter binding in the server protocol, the FFI, Python or Go, so every binding builds SQL strings and carries an injection risk.
- **Functions and types (40f).**
  - Functions: 5 aggregates, 13 scalars, 3 window functions.
  - Types: 7 data types; no DECIMAL or INTERVAL (`crates/pinkdb/src/types.rs:62-71`).
- **Optimizer (40g).** Constant folding and filter pushdown only.
  - No projection pushdown.
  - `LIMIT` without ORDER BY computes the whole input first.
  - The hash join always builds on the right side.
- **Lake (40h).**
  - Every save rewrites every table, and there is no garbage collection.
  - SQL can't query the lake directly; there is no time travel and no column projection on read.
  - Decode is per-row.
  - The encode path is not panic-guarded.
- **Extensions trust and packaging (40i).**
  - No artifact signatures and no SQL `INSTALL`.
  - The Python binding has no wheel; the Go module only works inside the repository layout.
- **Tests (40j).**
  - No unit tests for `exec/window.rs` or `plan.rs`.
  - No WAL decoder fuzz and no property tests.
  - duckdiff runs outside the gated jobs.

## Open items found after wave 80

- **Debug-build stack headroom for deep procedure recursion** (found in
  wave 83). A recursive SQL procedure with one nested block per level
  overflows a 2 MiB thread stack in **debug** builds at depth ~29 (28 with
  wave 83's restructuring, 27 through a guarded block) — below
  `MAX_CALL_DEPTH` (32) — and a stack overflow aborts the process instead of
  failing the call. Release builds run the full 32 levels through guarded
  blocks in 512 KiB. A proper fix bounds the executor by measured stack use
  (or grows the stack), rather than by call depth alone.

## Docs drift and hygiene (wave 40c — closed)

- `website/` still describes the deleted hand-written parser (`architecture.html`), protocol v2 (`server.html`), "wave 26 shipped" (`index.html`), stale test counts (`roadmap.html`), and TLS/RBAC as waves 45/47.
- `crates/pinkdb-ffi/hello` is a committed Mach-O binary with an absolute rpath to the author's machine.
- The README CI/CD section prints the Cloudflare account ID it tells readers to keep secret.
- README "Known v1 limits" lists none of the lake, S3 or repo limits.
