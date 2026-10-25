# Changelog

pinkdb is developed in numbered waves: each wave ships one focused capability
(or a performance pass) with its own tests and CI gates. This file is the
user-facing summary — one section per wave, newest first — while the
[ROADMAP progress log](ROADMAP.md#progress-log) carries the full engineering
detail (probes, gate counts, integration notes). Entries are appended at
integration time starting with wave 63; earlier waves are backfilled as
one-liners below.

## 2026-10-25
### Wave 95c — SQL dialect completeness, batch 3 (query forms) ✅
- **LATERAL and comma-separated `FROM`.** `FROM a, b` is a cross join in the
  same left-deep list as `JOIN`, so `FROM a, b JOIN c ON a.x = c.z` binds like
  DuckDB. `LATERAL (SELECT …)` may sit in the head position, after a comma, or
  after any `JOIN` (including `CROSS JOIN LATERAL` and `LEFT JOIN LATERAL … ON
  true`). pinkdb binds derived plans without an outer scope, so a lateral
  subquery that references a column of an earlier `FROM` item is refused by
  name instead of silently returning the uncorrelated answer.
- **Ordered-set aggregates.** `percentile_cont`/`quantile_cont`,
  `percentile_disc`/`quantile_disc` and `mode` with `WITHIN GROUP (ORDER BY …)`,
  `FILTER` before or after the `WITHIN GROUP` clause, and the DuckDB result
  types (a `DECIMAL` percentile stays `DECIMAL`, an integer one becomes
  `DOUBLE`, `percentile_disc`/`mode` keep the input type, empty input is
  `NULL`, ties in `mode` pick the smallest value). A non-constant fraction, a
  missing `WITHIN GROUP`, or `OVER ()` on an ordered-set aggregate are named
  errors.
- **`TABLESAMPLE` / `USING SAMPLE`** with DuckDB's methods — `bernoulli`,
  `system`, `reservoir` — for `n ROWS` and `n PERCENT`, plus `REPEATABLE
  (seed)`. Sampling is deterministic under a seed (pinkdb's own SplitMix64
  stream, so the same seed picks different rows than DuckDB but the same rows
  every run); a discrete count with `bernoulli`/`system` is refused with
  DuckDB's message. `USING SAMPLE 2 ROWS REPEATABLE(7)` (no method) is accepted
  as a superset of DuckDB, which needs the parenthesised method form.
- **Window `FILTER` and windows built on windows.** `agg(x) FILTER (WHERE …)
  OVER (…)` skips non-matching rows frame-relatively (DuckDB's answer, `NULL`
  where nothing matches); a non-aggregate window function with `FILTER` is
  refused by name. `WINDOW w AS (…)` definitions can now build on an earlier
  window (`OVER (w ORDER BY …)`, `WINDOW v AS (w ROWS 1 PRECEDING)`), with
  DuckDB's merge rules: the base's `PARTITION BY`/`ORDER BY` are inherited, a
  derived clause may fill a gap but not override one, and copying a base that
  has a frame clause is an error.
- **Row constructors.** `ROW(a, b)` and `(a, b)` compare field-wise in `=`,
  `<>`, `<`, `<=`, `>`, `>=` and work as the left side of `IN (…)` with a
  value list. DuckDB's semantics are reproduced exactly: row comparison treats
  `NULL` as an ordinary value (`(1,NULL) = (1,NULL)` is true, `NULL` sorts
  greater), while `IN (SELECT …)` keeps three-valued logic. A standalone row
  and `(a, b) IN (SELECT …)` are refused by name (pinkdb has no `STRUCT`
  type and its `IN` probe is single-column).
- **`AT TIME ZONE` and `timezone(zone, ts)`.** Folded into one function. pinkdb
  timestamps are naive and it has no time-zone database, so a UTC-family zone
  (`UTC`, `GMT`, `Z`, `Etc/UTC`, `+00:00`) is the identity on a `TIMESTAMP`
  (a `DATE`/`VARCHAR` is cast first) and any other zone is refused with a
  message naming the missing time-zone database.
- **JSON mutation functions.** `json_set`, `json_insert`, `json_replace` and
  `json_remove` are engine functions with DuckDB's path semantics (missing
  parents are created, `json_insert` never overwrites, `json_replace` never
  creates, an array index past the end is ignored, `$` is the whole document,
  `$."quoted key"` works, any `NULL` argument gives `NULL`). They accept JSON
  text or a `VARIANT` and return the same kind, and a `VARIANT` value argument
  needs no `::JSON` cast — a documented superset of DuckDB, whose signatures
  require `JSON`.
- **Session variables.** `SET VARIABLE name = <expr>`, `getvariable('name')` and
  the `$name` shorthand. The value is spliced at bind time, so a later statement
  sees it (a variable set inside a multi-statement string is visible to the
  statements after it). `getvariable` of an unset name is `NULL` like DuckDB;
  `$name` errors, also like DuckDB. The state is **per connection**, like
  DuckDB's session: the embedded handle is one session, every network
  `Connection` and every served/MCP session its own, so one client's variable is
  invisible to — and cannot be overwritten by — another's. Variables are never
  persisted or WAL-logged, and a logged statement may not read one (replay
  would bind its text against a fresh session), which the binder refuses by
  name.
- **`FOR UPDATE` / `FOR SHARE`** stay refused, with a message naming the reason:
  pinkdb has no row locks and its MVCC already serializes writers. DuckDB 2.0
  rejects the clause too.
- **`MERGE` and `UPDATE … FROM` now hash-join their equality keys.** The ON /
  WHERE equality conjuncts build a hash table over the source once and probe it
  per target row (residual predicates still filter), keeping the old nested loop
  for anything without an equality key. Measured on 10 000 × 1 000 rows,
  interleaved: `MERGE` 3.18 s → 0.05 s (~65×), `UPDATE … FROM` 3.09 s → 0.006 s
  (~500×); 100 000 × 10 000: 340 s → 0.47 s and 318 s → 0.03 s; 1 000 000 ×
  100 000: `MERGE` 4.9 s, `UPDATE … FROM` 0.28 s (the nested-loop baseline at
  that size is an extrapolation of ~9 h, not a measurement). TPC-H Q1/Q6 and a
  group-by/sort query show no change (interleaved A/B, medians equal).
- **Docs, tests, wire protocol.** `docs/sql-conformance.md` gains the full
  batch-3 matrix (no form left unassigned) and `bench/duckdiff.py` 40 new cases
  (376 pass). The conformance suite adds 26 DuckDB-checked cases plus
  pinkdb-only cases for the forms whose spelling differs, seeded-sampling
  determinism and session variables. `SET VARIABLE` reports
  `PINKDB_STMT_SET_VARIABLE` in the C ABI and `DoneKind::SetVariable` (tag 26)
  on the server wire, which bumps the protocol to v12.

### Wave 93b — SQL `INSTALL`/`LOAD`, and a released `repo` variant ✅
- **`INSTALL` is SQL now.** `INSTALL name [FROM '<repo-or-url>']`,
  `FORCE INSTALL …` (re-download an identical cached copy) and `LOAD name`
  (resolve an installed extension by name, no-op if it is already loaded) run
  the same code as `.install` — one implementation in the engine
  (`crates/pinkdb/src/install.rs` plus the repository client moved from the CLI
  to `crates/pinkdb/src/repo/`), with `.install` reduced to a console alias.
  Signature verification, SHA-256 checking, the catalog format and the cache
  layout are unchanged.
- **The trust boundary is explicit.** Both statements need the `repo` feature;
  without it the error is `this build has no extension repository support
  (rebuild with --features repo, or use the pinkdb-repo release variant)`.
  Installing loads native code, so they take the `LOAD EXTENSION` opt-in
  (`--allow-load-extension` / `PINKDB_ALLOW_LOAD_EXTENSION=1`), need `ADMIN`,
  are refused inside a transaction, and a server refuses them for remote
  clients unless started with `--allow-load-extension`; the MCP read-only
  policy refuses them like every other write.
- **WAL decision.** `INSTALL` is deliberately **not** logged: downloading is
  not replay-deterministic and replay must never reach the network. `LOAD
  <name>` is logged like `LOAD EXTENSION`, and replay re-resolves it from the
  cache. Loading now happens from the verified cache path instead of a deleted
  private staging copy — that is what keeps the recorded path replayable — with
  a re-hash immediately before the load (a file that changed in between is
  refused). `LOAD <name>` trusts the per-user cache.
- **Release variant `repo`.** `install.sh --variant repo` (asset
  `pinkdb-<ver>-<os>-<arch>-repo.tar.gz`, features `repo`) is now part of the
  release matrix, smoke test and docs, so `.install`/`INSTALL` and the Ed25519
  check are no longer source-build-only. Measured on darwin-aarch64: release
  binary **7,040,240 B** with `repo` vs **4,920,832 B** default (+43.1%;
  tarball 3,161,246 vs 2,151,249 B).
- **Tests.** SQL-level end-to-end against the existing loopback fixture
  repository: install, `FROM`-only (no configured repositories), `LOAD` by name
  in a fresh process (no download), `FORCE` re-download, a tampered catalog
  signature refused before any artifact request, the connection gate firing
  before the network; non-admin RBAC refusals, MCP policy refusals and the
  no-repo messages; `writes_tables` pins the WAL decision. Workspace **1,914**
  tests, CLI with `repo` **123**, engine with `repo` **912**, release tooling
  **12/12**, wasm32 build green.

### Wave 95b — SQL dialect completeness, batch 2 (DML) ✅
- **Upserts.** `INSERT … ON CONFLICT [(cols)] DO NOTHING | DO UPDATE SET … [WHERE …]`
  with `excluded.col` (the proposed row) and the target table readable in the
  `SET`/`WHERE`; `INSERT OR REPLACE` (= `DO UPDATE` over the columns the
  statement writes, so unlisted columns keep their stored values) and
  `INSERT OR IGNORE` (= `DO NOTHING` for every key). A conflict target must be
  a declared `PRIMARY KEY`/`UNIQUE` constraint — DuckDB's refusal, verbatim —
  and a duplicated key inside one statement keeps the first input row.
  `ON DUPLICATE KEY UPDATE` and `REPLACE INTO` stay refused by name (DuckDB
  has no `REPLACE INTO` either).
- **Keys are enforced now.** Wave 103 left `PRIMARY KEY`/`UNIQUE` as metadata;
  `ON CONFLICT` without enforcement is incoherent, so `INSERT`/`UPDATE`/`MERGE`
  check every declared key per statement (`crates/pinkdb/src/keys.rs`: one hash
  map per statement, built from the table and updated as rows are staged). A
  duplicate — or a `NULL` in a primary key, or a multi-row insert whose rows
  collide with each other — raises DuckDB's message and the statement leaves
  the store unchanged. `UNIQUE` accepts any number of `NULL`s; `REFERENCES`
  stays a declaration (`DESCRIBE`/FK→PK join elimination only). Cost, measured:
  1 M-row `INSERT … SELECT` into a PK table 0.095 → 0.381 s (median of five,
  ~3.5 M keys/s); a bulk `UPDATE` that does not write a key column and a
  `DELETE` are unchanged.
- **`UPDATE … FROM` / `DELETE … USING`.** The source is bound before the
  target, so `FROM`/`JOIN` columns come first in expressions. Several source
  rows matching one target row: `UPDATE … FROM` keeps the last one in source
  order (DuckDB's answer — no error, unlike Postgres), `DELETE … USING`
  deletes once.
- **`RETURNING`** on `INSERT`/`UPDATE`/`DELETE`, on `ON CONFLICT` (scalar
  subqueries in the projection included) and on `MERGE`. The rows come from the
  write funnels, so they are the final image: a `BEFORE` row trigger's rewrite
  of `NEW` is what `RETURNING` reports, and a `DELETE` reports its rows in scan
  order. An `ON CONFLICT` action that stores the values already there still
  counts as affected, like DuckDB.
- **`MERGE INTO … USING … ON …`** with `WHEN MATCHED [AND …] THEN UPDATE SET …
  | DELETE`, `WHEN NOT MATCHED [AND …] THEN INSERT [(cols)] VALUES (…)`,
  `WHEN NOT MATCHED BY SOURCE [AND …]`, first matching clause wins, a target
  row affected by more than one source row refused with DuckDB's "cannot affect
  the same target row more than once" and the statement atomic. The source is
  materialized once; matching is a nested loop (a `shortcut:` comment documents
  a hash join as the upgrade path).
- **Data-modifying CTEs.** `WITH x AS (INSERT/UPDATE/DELETE … [RETURNING …])
  SELECT … FROM x`: the body runs once, at the top level (a nested one is
  DuckDB's "must be at the top level" error), and its rows are served from an
  in-memory hand-off (`db.dml_cte_outputs`) because the evaluator holds the
  store immutably. `COPY` as a CTE step stays refused (it is ADMIN-gated as a
  statement; a CTE step would bypass that gate).
- **AST, parser, binder, plan, executor, EXPLAIN.** `Statement`/`LogicalPlan`
  carry `conflict`/`returning`/`from`/`using`, plus `LogicalPlan::Merge` and a
  `CteBody::Dml` CTE body; the AST walkers (depth guard, parameter renumbering)
  recurse into all of them; `EXPLAIN` prints the conflict action and the MERGE
  clauses.
- **Durability.** The WAL logs each statement's SQL text, so every new form
  replays by re-execution. `crates/pinkdb/tests/dml_replay.rs` writes upserts,
  `MERGE`, `UPDATE … FROM`, `DELETE … USING` and a DML CTE, drops the process
  without a checkpoint, reopens (replay) and compares; then checkpoints and
  reopens again.
- **Privileges and the MCP.** The per-statement gate sees every table: `MERGE`
  needs the privilege of each action its clauses spell out plus `SELECT` on its
  `USING` source, `UPDATE … FROM`/`DELETE … USING` need `SELECT` on the source,
  a DML CTE's target needs its write privilege, and the read pass walks the CTE
  body. The MCP read-only policy refuses every new statement kind and a `WITH`
  whose body writes (tests in `serve.rs`).
- **Tests.** `sql_conformance.rs` gained 25 DML cases pinned to DuckDB and
  re-measured on DuckDB 2.0.0 (the same 25 run in `bench/duckdiff.py`, 336/336
  passing), the error cases, key enforcement, `RETURNING` after triggers, the
  DML-CTE `EXPLAIN`, and MERGE's multiple-match; `rbac.rs`, `dml_replay.rs` and
  `serve.rs` cover the gates. Gates: clippy clean, workspace 1856 passed /
  0 failed (8 ignored), `-p pinkdb` 855 (6 ignored), `-p pinkdb-cli --features
  pinklake,s3,parquetlake` 168, wasm32 build, duckdiff 336/336.
- **No regression** for existing DML: interleaved A/B against HEAD `6531691`
  (own worktree + target dir), five rounds, medians —
  `INSERT INTO t2 SELECT * FROM t` 1 M rows 0.095 → 0.381 s (the key check),
  `UPDATE t SET v = v + 1` 1 M rows 0.025 → 0.025 s, `DELETE FROM t WHERE id %
  2 = 0` 500 k rows 0.019 → 0.019 s (`bench/wave95b_perf/`).

### Wave 93a — one ABI in four SDKs, one conformance fixture, pinned libraries ✅
- **ABI 1.2, negotiated.** The extension header gained the size constants and macros a
  library uses to ask what the host actually publishes — `PINKDB_EXT_API_V1_SIZE`,
  `PINKDB_EXT_API_FLAGS_SIZE`, `PINKDB_EXT_API_MINOR2_SIZE`,
  `PINKDB_EXT_HOST_HAS_FLAGS()` and `PINKDB_EXT_HOST_HAS_EXECUTE()` — plus
  `pinkdb_ext_copy_api()`, which copies only the bytes `struct_size` promises (the old
  `g_api = *api` read past a smaller host struct). The four vendored copies
  (`ext-sdk/{c,cpp,go,rust}/include/pinkdb_extension.h`) are byte-identical to the loader's
  and `tests/header_sync.rs` fails `cargo test` if they drift.
- **C, C++, Go and Rust at the same ABI.** All four cover `register_scalar_flags` and
  `PINKDB_EXT_FLAG_VOLATILE` (refusing to register a volatile function on a host too old
  for the flag, instead of silently calling it deterministic), the full type-tag set
  including INTERVAL's packed months/days with micros, the init-only host-API copy, the
  name/arity/tag bounds, host `alloc`/`free` text ownership, and `execute_statement`. The
  C++ SDK is new (`ext-sdk/cpp`, header-only `pinkdb::ext` with a move-only owned `Text`, a
  `Writer` for results and an exception guard), and the copy-pasted Go shims became one
  package (`ext-sdk/go/pinkdb`). Table functions, scans and copy sinks are still
  linked-only, and every README says so.
- **One fixture, four SDKs.** `ext-sdk/conformance/README.md` defines
  `conformance_ext`: 15 functions, a canonical `cf_all` row that renders every tag
  bit-exactly, `cf_fail` (publishes text, then fails), `cf_unwind` (panics/thows) and
  introspection counters. Each SDK builds it, and the loader drives all four
  (`crates/pinkdb-ext-loader/tests/conformance.rs`, 11 tests): tag round-trips with NULL,
  embedded NUL, empty strings and 64 MiB + 1 results refused, error codes, panic
  containment (a statement fails and the extension still answers afterwards), 100 callbacks
  from four foreign threads, and `struct_size`/`abi_version` negotiation in a child process
  (old host → refused non-zero with nothing registered as deterministic; full host → all 15
  registered). `tests/conformance_ownership.rs` checks the host allocates and frees exactly
  once per VARCHAR result and leaks nothing on empty, NULL or error paths.
- **Local libraries can be pinned.** `--extension-pin PATH=SHA256` (CLI and server,
  repeatable) installs a SHA-256 allowlist in the loader: once it is set, every load must
  match a pin — an unpinned library, a hash mismatch, or a file that changed between the
  hash and the `dlopen` is refused with the reason. The hash is read from an open file and
  the file's `(dev, ino, len, mtime)` re-checked immediately before mapping; the residual
  check-then-map window (there is no `fdlopen` on Linux or macOS) is documented. Local
  libraries are still unsigned: Ed25519 signatures cover `.install` packages only.
- **Posture, stated and tested.** `ext-sdk/posture.sh` drives the release CLI through the
  refusals every README promises: `LOAD EXTENSION` off unless
  `--allow-load-extension`/`PINKDB_ALLOW_LOAD_EXTENSION=1`, `--extension-root`
  confinement, bare names and `..` and symlinks refused, pins enforced. CI runs it after
  the four smokes and the C++ build, with the conformance matrix under
  `PINKDB_CONFORMANCE_STRICT=1` so a missing toolchain fails instead of skipping.
- **93b** shipped separately (see below): SQL `INSTALL`/`LOAD <name>` and a
  released CLI variant with the `repo` feature.

## 2026-10-25
### Wave 95a — SQL dialect completeness, batch 1 ✅
- **The matrix first.** `docs/sql-conformance.md`
  lists every query form row 95 scopes, each checked by running it against
  DuckDB 2.0.0: supported (same answer), divergent (and how), or refused with
  the exact error — and each assigned to batch 95a, 95b or 95c. Row 95 is
  large, so 95a is the query forms, 95b the DML forms, 95c the rest.
- **New query forms.** `QUALIFY`; `TRY_CAST(expr AS type)` (NULL on failure,
  every target type); subscripts `v['a']`, `v[0]`, `v.a` and string
  `'hello'[2]` / `'hello'[2:4]` (1-based, negative from the end, like DuckDB);
  `IS [NOT] DISTINCT FROM`; `VALUES` as a query, in `FROM` with column aliases
  and as the column-name source (`col0`, `col1`, …), plus `TABLE t`;
  `FETCH FIRST n ROWS ONLY` and `OFFSET … ROWS`; `LIKE … ESCAPE`; `SIMILAR TO`
  as the fully anchored regex DuckDB actually implements (a hand-written NFA in
  `crates/pinkdb/src/regex.rs` — no new dependency); `DISTINCT ON (…)`
  (first row per key in `ORDER BY` order); aggregate `FILTER (WHERE …)`, also
  with `DISTINCT`; named `WINDOW w AS (…)` clauses; `GROUPING SETS`, `ROLLUP`,
  `CUBE` with `GROUPING()`/`GROUPING(a, b)` (big-endian mask, DuckDB's set
  order); `WITH RECURSIVE` (semi-naive iteration, `UNION` de-duplicating,
  `UNION ALL` not; bounded at 100 000 iterations and 10 000 000 rows — both
  are errors, so a runaway recursion cannot hang the engine); and
  `UNION`/`INTERSECT`/`EXCEPT` `ALL` are covered by the corpus now.
- **Everything else fails by name.** Every form 95c will add is recognised
  and refused with a `wave 95c` sentence (`LATERAL`, `WITHIN GROUP`, `TABLESAMPLE`, `FOR UPDATE`/`SHARE`,
  `AT TIME ZONE`, row constructors, list literals, `FILTER` on a window, a
  window built on a window, VARIANT slicing, `SET VARIABLE`, `$name`), and the
  refusal is reported on its own instead of inside peg's expected-token list.
- **Plan and EXPLAIN.** `LogicalPlan` gained `Values`, a `recursive` step on
  `Cte` and an `on` column subset on `Distinct`; `EXPLAIN` prints `Values (n
  rows)` and `Recursive step (UNION ALL)`, and every new form shows its
  operator (`Window` for `QUALIFY`, `Distinct` for `DISTINCT ON`, …).
- **Tests.** `crates/pinkdb/tests/sql_conformance.rs`: 25 shared-corpus cases
  pinned to DuckDB's answers and re-measured on DuckDB when
  `/tmp/duckdb-2.0.0/duckdb` exists, the EXPLAIN shapes, the 95c refusals,
  and the pinkdb-only shapes (VARIANT subscripts, grouping masks, recursion
  bounds). A `SELECT 1.e2` regression from the new `.field` postfix is a parse
  error again.
- **No regression.** Interleaved A/B against HEAD `1d199fb` on a
  1 000 000-row TPC-H-shaped store (release: TPC-H Q1 0.012 → 0.012 s, Q6
  0.007 → 0.007 s, group/sort 0.131 → 0.132 s; medians of six runs,
  `bench/wave95_perf/`).

## 2026-10-24
### Wave 76 — CSV import options ✅
- **One option set, two front ends.** `.import` and `read_csv` now share
  `pinkdb::csv::CsvOptions`, so `.import f.csv t sep=';' header=true` and
  `read_csv('f.csv', sep := ';', header := true)` read a file identically —
  locally, over `https://` and from `s3://`. Named parameters use DuckDB's
  names (`delimiter`/`sep`/`delim`/`separator`, `quote`, `escape`, `encoding`,
  `header`, `nullstr`/`null`, `names`/`column_names`,
  `ignore_errors`/`errors`, `allow_quoted_nulls`, `force_not_null`); an unknown
  or unsupported name is an error naming it, never silently ignored.
- **Dialect.** `delimiter` takes any single byte (a literal tab or the `\t`
  spelling), `quote` any byte, `escape` a byte or off (the default). A
  multi-byte delimiter is refused instead of truncated.
- **Encodings.** `latin-1`, `windows-1252`, `utf-16le`, `utf-16be`, and the
  default `utf-8`; the default (`auto`) also strips a UTF-8 BOM and switches to
  UTF-16 on a LE/BE BOM. The decoders are hand-written and stream — no new
  dependency — so the wave-98c read-ahead and ranged remote reads keep working
  and the sniff window never needs the whole file.
- **Header and column matching.** `header=auto` applies DuckDB's rule: infer
  the types over every row and again over the rows below the first; row 1 is a
  header when the two disagree or when the data is all text. Importing into an
  existing table matches columns **by header name**, case-insensitively:
  missing columns become `NULL`, and file columns the table has no home for
  follow the error policy. `names=` overrides the header, `header=false` reads
  the first line as data (`column0`, `column1`, …).
- **NULLs.** `nullstr='NA,N/A'` adds tokens, `allow_quoted_nulls=false` keeps a
  quoted empty field as `''`, `force_not_null='a'` keeps a column's empties as
  `''` (which widens the column to `VARCHAR`). The default is DuckDB's measured
  behaviour, not the folklore: an empty field is `NULL` whether quoted or not.
  A quoted cell keeps its own whitespace (only unquoted cells are trimmed), so
  `"  x  "` stays `"  x  "` while `" 1 "` still imports as a number.
- **`errors=warn`** (DuckDB's `ignore_errors := true`, accepted by that name
  too) skips bad rows — a value that does not parse, or a row with more fields
  than the header — and `.import` prints a rejects report (line, column,
  reason; the first ten plus a total). A `read_csv` scan has nowhere to print
  that report, so a scan skips silently: `warn` means skip, never "count the
  bad rows". `error` stays the default: the first bad row fails the import and
  leaves no table behind, and the error names the line. Storing rejects in a
  table remains the recorded follow-up.
- **The `csv` crate is gone.** Wave 75 pulled it in for the record reader; the
  engine's own streaming reader replaced it and the dependency was left behind,
  unused, with only `PinkDbError::Csv(csv::Error)` still naming it. That variant
  is now pinkdb's own `PinkDbError::Csv { message, line, column }` (the FFI,
  agent-JSON and Python bindings keep their stable `"csv"` error kind, and a
  structural error now names its line), and `csv` left `Cargo.toml` and the
  lockfile (22 lines). Size of the default release `pinkdb` binary:
  **4,524,032 → 4,536,208 bytes** (+12,176, +0.27 %), which is the new error
  payload and formatting, not the dependency — re-adding the unused `csv` crate
  to the same tree builds a binary 16 bytes *smaller* (build metadata), so it
  contributed no code.
- **No import-speed regression.** The options layer initially cost 7× on
  `.import` (18.2 s for TPC-H `lineitem`, 773 MB / 6 001 215 rows, against
  2.54 s before the wave). Two causes: the type sniff walked the *whole* file
  through the byte-at-a-time record reader, and the parallel batch parser
  refused any file containing a `"`, so any quoted column fell back to it. The
  sniff is now sample-limited (20 480 rows, DuckDB's `sample_size`; a column
  with no value in the sample still triggers the exact pass), and the batch
  parser understands RFC 4180 quoting — fields are split with the quoting
  rules, `""` unescapes, a quoted cell keeps its whitespace — so a file is
  parsed in parallel exactly as the record reader would read it — a file the
  scan cannot prove falls back to the reader rather than being read wrong.
  Measured with the reviewer's harness, interleaved, median of 3: `lineitem`
  **2.54 s → 0.87 s**, `orders` (174 MB) **0.42 s → 0.21 s** — twice as fast
  as before the wave, not just within budget. Slower by design, and measured
  on the same 773 MB file: `errors=warn` 12.2 s (the rejects report needs the
  sequential reader), `encoding=latin-1` 14.2 s, `encoding=utf-16` 13.3 s
  (1.5 GB, BOM autodetected) — transcoding and warn still run through the
  record reader, which is the recorded next target.
- **`read_csv` scans share it.** A table-function scan parsed with the record
  reader too (7.4 s for `SELECT count(*) FROM read_csv('lineitem.csv')`
  against 5.03 s before the wave). It now runs the same batch parser: a file
  under the 8 MiB sniff window at once, anything larger — a remote object, or
  a local file past that window — in 8 MiB windows of the wave-98c prefetch,
  each window cut at its last provable record end so a record can straddle two
  windows, and a window the scan cannot prove is finished by the reader. 5.03 s → **1.36 s**, and
  the same 33.9 MB file through the test HTTP server 486 ms → **120 ms**
  (local 522 → 77 ms). Values, not just counts, match `.import` on lineitem
  (`sum(length(l_comment))`, `sum(l_orderkey)`, date bounds, `sum(l_quantity)`).
- **Unchanged:** a sampled column that a later row contradicts still falls
  back to whole-file inference, and a durable import still checkpoints before
  it returns. An import that appends to an existing table is atomic too: a
  strict failure restores the table instead of leaving half the file in it.
- Tests: `pinkdb` CSV module 34 (from 19), including a DuckDB-oracle test for
  the empty-field rule and header auto-detection that skips when the α binary
  is absent; `pinkdb-httpfs` 62 (local-vs-remote option parity, `errors=warn`
  in a scan); 4 new `.import` CLI tests; workspace **1865** tests / 161 suites,
  9 ignored (one of them the scan speed aid).

### Wave 101a — the `VARIANT` type core ✅
- **The type.** `VARIANT` is now a column type: one column, any shape per row —
  a JSON null, boolean, integer, double, decimal, string, date/timestamp, array
  or object. Values live in a binary, self-describing encoding (a kind tag per
  value plus the payload), not in JSON text, and objects keep their keys sorted
  so equal objects encode identically. `typeof(v)` reports `'VARIANT'`.
- **Casts.** Any type → `VARIANT`; `VARIANT` → every scalar, with an error that
  names the kind and the value (`cannot CAST VARIANT(VARCHAR) value "hi" to
  INTEGER`) instead of a wrong answer. `'{"a":1}'::VARIANT` parses JSON — the
  literal form the parser, the WAL and prepared statements already carry (this
  is where we differ from DuckDB, where `VARCHAR → VARIANT` is the identity
  and the JSON extension is required first). A JSON null reads as SQL `NULL`
  for every target except `VARCHAR`, which keeps the text `null`, so
  `VARIANT → VARCHAR → VARIANT` is lossless for JSON's value model.
- **Functions.** `variant_type(v)`, `variant_keys(v)` (a JSON array of the
  keys; pinkdb has no list type yet) and `variant_extract(v, path)` with
  0-based JSONPath (`'a.b[1]'`, `'$.a[0]'`) — a missing path is SQL `NULL`.
  The JSON extension's functions (`json_extract`, `json_array_length`, …)
  accept a `VARIANT` and see its JSON text.
- **Semantics, defined and tested.** Equality is structural, with numbers
  comparing by value across kinds (`1` = `1.0` = `1.00`, `'1'` ≠ `1`). One
  documented total order serves `ORDER BY`, `MIN`/`MAX` and range comparisons:
  bool < number < string < date < timestamp < array < object < JSON null, with
  SQL `NULL` last. `GROUP BY`, `DISTINCT` and joins hash a canonical key that
  agrees with `=` by construction. A JSON null is its own group and is never
  equal to SQL `NULL`.
- **Persistence.** New `.pink` dtype tag (9) and default-value tag; a
  `VARIANT` column survives save/open, WAL replay, checkpoint and
  `ALTER TABLE … ADD COLUMN … DEFAULT`. VARIANT chunks carry no min/max
  statistics. An older build that meets the new tag reports a clear
  `corrupt file: bad dtype tag 9` — no legacy reader.
- **Encodings elsewhere.** Wire protocol v11 (new dtype and value tags; a
  `VARIANT` rides as a length-prefixed blob so an old client fails cleanly);
  FFI `PINKDB_TYPE_VARIANT` (the JSON text); the JS/WASM binding hands it over
  as a native JSON value. Refused with clear errors: the extension C ABI
  (like `DECIMAL`), the Arrow extension, and Parquet write.
- **Lake.** A `VARIANT` column round-trips in a `.pink` lake data file; a
  Parquet-format lake refuses it with a message naming wave 101b rather than
  writing it as text.
- **Not in this wave.** `TRY_CAST` (does not exist anywhere in pinkdb),
  DuckDB's `variant_extract_string`/`variant_array_length`/`variant_exists`/
  `variant_normalize`/`variant_comparator`, array/field subscript syntax
  (`v['a']`), and Parquet `VARIANT` — that is 101b, together with shredding
  into typed sub-columns and statistics on shredded paths. The plan for it,
  the full tag table, the semantics and every deliberate difference from
  DuckDB are written in `docs/wave-101-plan.md`.
### Wave 72 — Core extension: iceberg ✅
- **`iceberg_scan`, `iceberg_snapshots`, `iceberg_metadata`** in a new
  engine-adjacent crate, `extensions/iceberg` (`pinkdb-iceberg`), behind the
  opt-in `iceberg` CLI feature (which turns `parquet` on — the data files are
  read by that reader). A table location is the table directory, its
  `metadata/` directory or one `…/metadata/vN.metadata.json`; a directory is
  resolved through `metadata/version-hint.text`, else the newest
  `vN.metadata.json`. Paths inside the metadata resolve against the table root,
  so a copied or mocked table works unchanged — what DuckDB calls
  `allow_moved_paths`.
- **Iceberg v2 semantics.** The current snapshot, or one named by
  `snapshot_from_id` / `snapshot_from_timestamp` (DuckDB's names); the manifest
  list, then the Avro manifests, then the live Parquet data files; **positional
  delete files** applied; **equality deletes refused by name** (a documented
  follow-up — DuckDB applies them, and the test pins that difference); schema
  evolution by **field id**, so a renamed column projects under its new name
  and an added one reads NULL in files written before it; **partition-summary
  pruning** (identity transforms) and **lower/upper-bound + NULL-count pruning**
  from the manifest entries; `count(*)` answered from the manifest record counts
  without opening a data file.
- **A hand-written Avro decoder**, not apache-avro: container header, sync
  markers, `null`/`deflate` block codecs, and the records, unions, arrays,
  maps, enums and fixed types the Iceberg manifest schemas use (600 lines
  against a dependency tree). `serde_json` parses the metadata JSON (already in
  the lock file).
- **Remote tables through the shared transport.** The `remote` feature (enabled
  by the CLI's `httpfs` feature) reads `https://`/`s3://` tables through
  `pinkdb-httpfs` — the same path `read_parquet` uses — with windowed 256 KiB
  read-ahead for the metadata JSON and the Avro files, then Parquet's ranged
  reader for the data. A remote table needs a `version-hint.text`; without one,
  pass the metadata file.
- **Fixtures without downloads.** `extensions/iceberg/tests/fixtures/gen.py`
  writes Iceberg v2 tables with the standard library alone — metadata JSON, an
  Avro container writer and manifests by hand, Parquet through the DuckDB 2.0
  binary — for six tables (two snapshots, positional deletes, equality
  deletes, a delete file older than the data file it names, schema evolution,
  partitioned), 232 KB committed. The **DuckDB 2.0 α oracle** compares every
  fixture's results (13 query/table pairs) and runs whenever the binary is
  present; a mock S3 and the local HTTP test server prove the remote path with
  ranged request counts.
- **Cost:** the crate's release rlib is 1,165,120 B; the CLI binary grows
  254,560 B over `parquet` (6,475,744 B with `iceberg`, 8,626,896 B with
  `iceberg,httpfs`) and the default binary is untouched — the crate is not in
  its dependency graph.

## 2026-10-24
### Wave 92 — Language bindings: Node.js, Python DB-API 2.0, Go ✅
- **One conformance corpus.** `bindings/conformance/corpus.json` is the
  language-neutral contract every binding is tested against: every SQL type's
  round trip (including NULLs, unicode, bigint extremes, NaN/±Infinity,
  DECIMAL, DATE and TIMESTAMP), positional parameters, a stable error class
  for parse/catalog/type/parameter/execution/constraint failures, explicit
  transactions and autocommit, multi-statement batches with several result
  sets, empty results, and a 100k-row stream that must not be materialized.
- **Node.js** (`bindings/node`): a napi-rs addon that links `pinkdb-ffi`
  statically, so the `.node` file has no runtime library dependency.
  `open(path?)` / `close()`, `run(sql, params?)` for batches, `query(sql,
  params?)` for row objects, `prepare()` with `run`/`all`/`iterate`, explicit
  `begin`/`commit`/`rollback`, `PinkdbError` carrying a `.class`, and the
  `pinkdb_abi_version` gate at load. BIGINT maps to `BigInt`, DECIMAL and
  INTERVAL to their canonical strings, DATE/TIMESTAMP to strings. Builds with
  plain `cargo` through a small copy step; `npm pack` works, nothing is
  published.
- **Python** (`bindings/python`): the `ctypes` binding is now also a PEP 249
  DB-API 2.0 driver — `connect()`, `Connection`, `Cursor` with
  `execute`/`executemany`/`fetchone`/`fetchmany`/`fetchall`, `description`,
  `rowcount`, `qmark` parameters, the standard exception hierarchy mapped from
  the C ABI's error class, and the type objects. Cursors fetch one row at a
  time: the 100k-row case peaks at about a kilobyte of Python allocations.
  Transactions are explicit, matching DuckDB: DDL is refused inside them, so
  there is no implicit `BEGIN`.
- **A real wheel.** `pip wheel . --no-deps -w dist` compiles `libpinkdb_ffi`
  and bundles it in `pinkdb/`, producing a platform wheel that installs and
  runs without a local build (`PINKDB_FFI_LIB` skips the compile).
- **Go** (`bindings/go`): the module is usable outside the checkout — the cgo
  preamble carries the ABI declarations instead of pointing at the header, and
  the library location comes from `PINKDB_LIB_DIR`/`CGO_LDFLAGS` (with the
  in-repo `target/release` as the convenience default). Errors now keep their
  class in `*pinkdb.Error`, and an ABI test checks the carried declarations
  against `pinkdb.h` whenever the checkout is present.
- **C ABI.** `pinkdb_last_error_class()` returns the stable error class next
  to the message (additive to ABI v1), so bindings no longer parse text to
  tell a parse error from a constraint violation. The engine and default CLI
  binaries are unchanged.
- **CI.** The bindings job now builds and tests the Node addon, the corpus and
  `npm pack`; Go keeps `gofmt`/`vet`/`-race`; a new `python-wheel` job builds
  the wheel, installs it into a throwaway venv, and runs the corpus from the
  installed package.

## 2026-10-24
### Wave 100 — Triggers ✅
- **Triggers.** `CREATE [OR REPLACE] TRIGGER name {BEFORE|AFTER}
  {INSERT|UPDATE [OF col, …]|DELETE} [OR …] ON table [REFERENCING OLD TABLE
  AS o NEW TABLE AS n] [FOR EACH {ROW|STATEMENT}] [WHEN (cond)] EXECUTE
  {PROCEDURE p(args) | <statement>}`, `DROP TRIGGER [IF EXISTS] name [ON
  table]` and `SHOW TRIGGERS [ON table]`. Bodies run through the stored
  procedure machinery (`ParamSlots` + `run_call_statement`), so each body
  statement is bound, optimized and executed like any other statement.
- **Semantics.** Triggers of one event fire in creation order; every BEFORE
  trigger runs before the row change and every AFTER trigger after it.
  `OLD.col` / `NEW.col` are available in `WHEN` and in the body; a BEFORE ROW
  trigger may assign `NEW.col` (the assignment is coerced to the column type
  and the written value is what lands in the table), `OLD` is read-only, and
  there is no row-skipping signal in v1 — returning from a BEFORE ROW trigger
  always lets the change through. `REFERENCING OLD TABLE / NEW TABLE` are
  readable relations (ordinary CTEs to the body, so any query shape works) and
  are accepted on AFTER STATEMENT triggers only. `UPDATE OF` narrows to the
  columns the statement assigns. Recursion is bounded at 16 levels with a
  clear error; a trigger error rolls back the whole firing statement,
  including in autocommit.
- **Replay.** The WAL still logs the firing statement and nothing else, so a
  replay re-executes it and the triggers fire again exactly once; a trigger
  is never logged separately, which is what keeps replay from double-counting.
  Volatile extension functions are already refused in logged statements, so
  bodies are deterministic; `now()`/`random()` are not builtins in pinkdb.
- **Persistence.** Definitions live in a new `.pink` footer section under
  flags bit `0x80`, written as their `CREATE TRIGGER` text in creation order
  so a reopen reproduces the firing order. A file without the bit has no
  triggers; no old format is read or converted.
- **RBAC.** Creating or dropping a trigger needs `CREATE` on the database —
  the same requirement as `ALTER TABLE` and the index DDL. Bodies run with
  **invoker** rights: every body statement is re-checked against the firing
  user, so a low-privilege user cannot use a trigger to write a table they
  cannot write. `SHOW TRIGGERS` is a metadata read available to any user.
- **Interactions.** Trigger-written tables join the statement's MVCC touched
  set (a concurrent writer conflicts instead of silently losing), materialized
  views refresh for triggered changes like any other change, and `.lake commit`
  of a writable attachment publishes trigger effects with the rest of the
  session's writes. Trigger definitions are not carried into a lake (it stores
  data).
- **Refused, clearly.** Triggers on views, on attached lake tables, on a
  missing table, `REFERENCING` on a ROW trigger or a BEFORE trigger, unknown
  `UPDATE OF` columns, and `RETURNING` on a triggered table (pinkdb has no
  `RETURNING`). `CREATE`/`DROP TRIGGER` inside a transaction is refused like
  the other catalog DDL.
- **No cost without triggers.** The DML fast path is untouched: the trigger
  check is one empty-slice lookup and the batch insert/update/delete code is
  byte-identical when a table has no triggers. 1,048,576-row `INSERT …
  SELECT` 34.4–34.8 ms (HEAD 34.1–34.3 ms) and bulk `UPDATE` 19.9–20.4 ms
  (HEAD 20.3–20.5 ms) — both within run-to-run noise.
- 39 trigger integration tests plus an ignored crash/replay child, a CLI lake
  test, and the full gate set green (workspace 1,796 tests / 156 suites).

### Wave 105 — agent bridge: MCP server, no model code ✅
- **A store is an MCP server.** `pinkdb --mcp <file.pink>` loads the
  `pinkdb-mcp` extension from the install directory and serves one MCP
  client over stdio (JSON-RPC 2.0); `.mcp serve [stdio|http <port>]
  [allow-writes]` does the same after `LOAD EXTENSION`, and
  `pinkdb-server --mcp-extension <path>` bridges the server's own store over
  loopback HTTP (`--mcp-port`, default 7801) under its logins
  (`--mcp-user` + `PINKDB_MCP_PASSWORD` on a store with users, RBAC applied).
- Eight tools, each with a JSON Schema and a model-facing description:
  `query(sql)`, `describe(table)`, `tables(database?)`, `databases()`,
  `explain(sql)` (with wave 99's scan-size hint) and the lake readers
  `lake_tables(catalog)`, `lake_versions(catalog)`,
  `lake_changes(catalog, table?, from?, to?)`. Answers are the wave-99
  Markdown tables with truncation notices and one-line JSON errors, under the
  same 1,000,000-row / 30 s budget.
- **Read-only by default, enforced in the host.** The statement callback
  parses every statement and, without `allow-writes`, refuses everything but
  `SELECT`/`EXPLAIN`/`DESCRIBE`/`SHOW …`/`USE`: DML, DDL, `COPY … TO`,
  `LOAD EXTENSION`, `CHECKPOINT`, role changes and lake maintenance answer
  `{"error":…,"kind":"permission"}`. Lake maintenance is never served. The
  HTTP transport binds loopback only and refuses any other address.
- **No external files or URLs by default.** A query alone can leak data, so
  the host also binds every statement and refuses it when the plan or the
  `FROM` clause reaches a table function or a bare path
  (`read_csv('/home/u/.ssh/id_rsa')`, `read_parquet('https://…')`,
  `FROM 'x.parquet'`), including through CTEs, subqueries, set-operation arms,
  joins and `EXPLAIN`; `allow-files` (`--mcp-allow-files`,
  `.mcp serve … allow-files`) lifts it and is independent of `allow-writes`.
  The `pinkdb-server` bridge keeps its login's grants instead, where file
  scans are administrator-only.
- **Extension ABI minor 2, additive.** `PinkdbHostApi::execute_statement`
  runs SQL text in the host and returns formatted text (no engine pointer
  crosses the boundary), and the optional `pinkdb_ext_serve` export hands the
  extension a transport descriptor (`stdio` or `http:HOST:PORT`) plus an
  options string. Detection is by symbol presence and `struct_size`, so every
  ABI 1 extension loads unchanged; the loader refuses to load or serve a
  library twice at once. The C header documents both, vendored into the three
  SDKs; the C/C++/Go wrappers get the surface in the wave-93 refresh.
- The extension is `extensions/mcp` (`pinkdb-mcp`, a cdylib using
  `serde_json` only); the host's policy, budgets and formatting live in
  `pinkdb::serve`, and the CLI routes `.lake` reads back through the same
  callback, so the extension never links pink-lake. Neither default binary
  grows beyond the host-side field.
- Any MCP client connects with no adapter: `pi mcp add pinkdb -- pinkdb --mcp
  store.pink` (pi) or `claude mcp add pinkdb -- pinkdb --mcp store.pink`
  (Claude Code).
- Tests: the loader's C serve extension (6), engine serve policy/rendering
  (8), MCP protocol conformance and transport rules (11), a CLI end-to-end
  set that drives `pinkdb --mcp` over stdio through a TPC-H style
  top-3-customers question, every write refusal, a truncation case and the
  `allow-writes` counterparts over HTTP (6, plus the lake catalog reader
  with `pinklake`, 7), and the server bridge under logins (3). Workspace:
  1,753 tests in 155 suites, 0 failed, 7 ignored (pre-existing).
- Release sizes, unchanged feature sets: `pinkdb` 4,221,296 → **4,260,048 B**
  (+0.9%), `pinkdb-server` 3,802,464 → **3,927,120 B** (+3.3%); the bridge
  itself is a 565,824 B cdylib that no binary links. `pinkdb` still builds
  for wasm32.

- **Agent mode** turns on with `-agent`, or automatically when stdout is not
  a terminal and one of `AI_AGENT`, `CLAUDECODE`, `CLAUDE_CODE`, `PI_AGENT`,
  `OPENCODE`, `CODEX_SANDBOX`, `CURSOR_AGENT` or `GEMINI_CLI` is set. A flag
  wins over the environment; `-no-agent` and `PINKDB_AGENT_MODE=0` force it
  off, and an interactive terminal never enables it by itself. One line on
  stderr announces the mode.
- Results are compact Markdown tables with types in the header, `NULL` for
  nulls, control characters escaped, and explicit truncation notices such as
  `… 1,234 more rows (1,284 total); 3 columns hidden: a, b, c`. Defaults are
  100 rows, 32 columns and 500 characters per cell.
- Errors are one line of JSON on stderr (`{"error": …,"kind": "parse",
  "position": 6}`), with byte offsets for parse errors. Exit codes are
  unchanged.
- A runaway-query guard (default 1,000,000 rows and 30,000 ms per statement)
  cancels the statement cleanly and leaves the session usable. Before a scan
  the engine estimates as expensive (default 100,000 rows) the CLI prints one
  cost hint on stderr naming the estimate and whether a remote scan is
  involved.
- The formatter, error encoder and scan estimate live in the reusable
  `pinkdb::agent` engine module (`crates/pinkdb/src/agent.rs`); the guard is
  exposed as `PinkDb::execute_with_limits`. `.help agent` documents flags,
  caps, settings (`PINKDB_AGENT_DISPLAY_ROWS`, `_COLUMNS`, `_CELL_CHARS`,
  `PINKDB_AGENT_SCAN_ROWS`, `_TIMEOUT_MS`, `_HINT_ROWS`) and `.agent`.
- Measured on a locally generated TPC-H-style dataset (10,000-row tables,
  10 queries, one 10,000-row result and one error): normal output 485,403 B /
  10,114 lines, agent output 4,162 B / 180 lines — 99.1% fewer bytes and
  98.2% fewer lines for the same answers and exit code.

## 2026-10-24
### Wave 104 — the legacy lake data format removed ✅
- **Breaking:** a lake whose catalog names the removed columnar data format
  no longer opens, with a clear message asking for a re-save with an older
  build. There is no reader and no converter. `.pink` (`PINKDB06`) and
  Parquet (the `parquetlake` provider, for DuckDB interop) remain.
- The old full-lake cargo feature and its large dependency tree are gone, so
  `pinklake` is the lake build. The release, install and Docker variants drop
  the old names. Tests that used the removed format for general subjects
  (format contract, mixed lakes, ranged/projected reads, partition pruning,
  schema evolution, fast reads) now run on `.pink` or Parquet.
- Leftover wave-103 crypto removed: PNKLAKE1 version-1 reading and the
  Argon2id KDF path. Per-file keys are raw, and `argon2` is no longer a
  pink-lake dependency.
- Size (release, Apple Silicon): `pinkdb-cli --features
  pinklake,s3,encryption` is **8,780,560 B**, down 41,632 B from the same
  build at HEAD; the removed full-lake build was **51,407,280 B**. The
  dependency tree is 109 lines (was 422 with the old format, 110 for HEAD
  `pinklake`), pink-lake is 51,714 → 47,865 `.rs` lines across the crate
  (−7.4%; `src/` 26,598 → 23,183, −12.8%), and a clean debug build takes
  12.95 → 13.69 s (noise: the removed feature was already off in the
  `pinklake` set).

## 2026-10-24
### Wave 103 — Lake catalogs are databases (DuckLake v1.1 tables) ✅
- **The catalog is always a database.** A lake is a `pink:<path>` durable
  pinkdb catalog (default), `sqlite:<path>` (`catalog-sqlite`) or
  `postgres://…` (`catalog-postgres`), all using DuckLake v1.1's
  `ducklake_*` tables with `begin_snapshot`/`end_snapshot`. Commits insert a
  typed delta and one snapshot row instead of rewriting
  `catalog.json`/`snapshots/`; `count(*)` is `SUM(record_count)`, and
  time travel, expire, cleanup, CDC, replication state and statistics read
  the same rows. DuckDB's `ducklake` extension can attach the SQLite/Postgres
  catalogs, and pinkdb opens DuckDB-written DuckLakes.
- **Locations.** A bare path means `pink:<path>` with data in
  `<path>.files/`. A catalog records its data location, so
  `.lake open <catalog>` is enough. `s3://…` alone is no longer a lake: pass
  a catalog and `data s3://…` (`.lake save pink:lake.pink data
  s3://bucket/prefix`), or use a `postgres://` catalog for multi-machine
  lakes. `.lake migrate-catalog` is removed.
- **Per-file encryption** stays: each data/delete file is a PNKLAKE1
  raw-key container with its own random 256-bit key recorded in the
  catalog; a `pink:` catalog may itself be engine-encrypted. Manifest
  sealing, the passphrase-derived lake key, `highest.json`, `lake.json`,
  `moved.json` and the degraded/conditional metadata-write paths are gone.
- **Breaking:** lakes saved by an older build in the JSON layout do not open.
  There is no converter; re-save them with an older build (for example
  `.lake open <old>` then `.lake save pink:new.pink`) before opening with this
  build.
- **Fix after review:** file statistics were resolved against the column's
  *current* name, so a file written before a RENAME lost its old physical
  column when attach rebuilt the zone map. Statistics now resolve through the
  `ducklake_column` definition visible at the file's `begin_snapshot`; a v1
  file projects through the rename at head, and time travel to v1 sees its
  original names and rows. Positional deletes keep addressing the same rows
  across ADD/DROP COLUMN. The schema, time-travel, replication, S3,
  encryption, corruption and attach-error tests deleted in the first pass were
  restored (and the schema/ replication ones run on both `pink:` and
  `sqlite:`).
- Result at 10,000 files: catalog transaction stays flat (pink 7.1–9.8 ms,
  sqlite 0.7 ms), pink catalog 3.43 MB with statistics, attach + `count(*)`
  184 ms. Data formats (`.pink`, Parquet) and the S3 data path are
  unchanged.

## 2026-10-24
### Wave 98c — streaming remote scans, a smaller Parquet writer, CSV read-ahead, an optional file cache ✅
- `COPY … TO '….parquet'` and Parquet lake files are now Snappy-compressed
  and dictionary-encoded like DuckDB's. TPC-H SF1 `lineitem` is 238 MB
  instead of 388 MB (DuckDB writes 205 MB) and takes 3.7 s instead of 3.4 s.
  A column whose dictionary grows past its chunk budget falls back to plain
  pages for that chunk. DuckDB reads the files (an interop test checks it).
- A query that aggregates over a remote file or a lake table no longer
  loads the table first: each decoded chunk goes straight into the
  aggregate, folded by the same worker pool the materialized path uses, so
  peak memory is one read-ahead window plus about two chunks per fold
  worker, however large the file is — and the fusion costs no speed.
  Measured with the fold pinned at two workers: the executor never held more
  than `2n+1` chunks, 2 000 rows on a mock-S3 file of 200 row groups (80 000
  rows) and 10 240 rows on a 60-file/180 000-row lake. A lake streams one
  data file at a time — `.pink` files chunk by chunk — so its bound is the
  largest data file, not the largest table.
- No regression, measured as an interleaved A/B against HEAD (5 alternating
  runs each, medians, local SF1): lake Q6 0.340 → 0.337 s, lake Q1
  0.390 → 0.396 s, local `read_parquet` Q6 0.212 → 0.210 s, an HTTP-served
  `lineitem.parquet` Q6 1.317 → 1.315 s (44.27 → 44.64 s with ranged GETs
  forced serial). Every aggregate the binder builds is order-insensitive, so
  chunks may be folded by any worker.
- `read_csv('s3://…')` and `read_csv('https://…')` download 1 MiB ranges a
  window of 8 MiB ahead of the parser, with one request in flight, so
  parsing overlaps the download and memory stays bounded; a transient
  mid-file error is retried.
- An optional on-disk cache keeps remote byte ranges between queries (**off
  by default**): `PINKDB_EXTERNAL_FILE_CACHE=1` (with
  `PINKDB_EXTERNAL_FILE_CACHE_DIR` and `_BYTES`, default 1 GiB) or
  `--file-cache DIR`. Ranges are keyed by URL and ETag, so a changed object
  is never served stale; the directory is size-capped with LRU eviction,
  entries are `0600` in a `0700` tree, and a corrupt entry is a miss.
- Release size: the default `pinkdb-cli` is 4,183,936 B, 63,552 B larger
  than HEAD (the streaming fold is in the engine); the rest is behind the
  `httpfs`/`parquet` features.

## 2026-10-23
### Wave 96a (part 2) — Postgres catalogs and catalog migration ✅
- A lake's metadata can now live in **Postgres**, not just SQLite or the
  object-store layout: `.lake save postgres://user:password@host/db?schema=lake
  data <dir|s3://…>` keeps the same lake, with the two catalog tables in a
  schema you choose and the data files where they were. Publication is one
  database transaction with a per-schema advisory lock, so concurrent writers
  keep the conflict/rebase rules.
- TLS verifies remote servers by default (`verify-full`, with
  `sslrootcert=<pem file>` for a private CA); `sslmode=require` opts into
  encrypt-only, and `disable` is allowed only for loopback hosts. Passwords are never printed —
  every error and command output shows `***`.
- `.lake migrate-catalog <from> to <to> [data <location>]` copies metadata
  byte-for-byte between the object-store, SQLite and Postgres backends in one
  publish scope, verifies it, and retires the source; opening the old location
  says where it moved. Encrypted lakes migrate without their key (sealed blobs
  are copied verbatim), and a non-empty target is refused.
- The Postgres catalog runs through the same shared test suite as SQLite in CI
  against a throwaway `postgres:17` service, and locally when
  `PINKDB_TEST_POSTGRES_URL` is set. The default CLI still links neither
  `rusqlite` nor the Postgres client.

### Wave 96a (part 1) — SQL catalogs: SQLite and split data locations ✅
- A lake's metadata may live in a **SQLite** file (`sqlite:<path>`) while its
  data files stay on local disk or S3/R2: `.lake save <catalog> data
  <dir|s3://…>` records the data location once, and later `.lake
  open`/`attach`/`commit` commands need only the catalog URL. An explicit
  `data <location>` overrides a moved data root for one session.
- Catalogs are a `CatalogStore` seam inside `pink-lake`: the object-store
  layout remains the default, and the SQL backends store the same
  byte-identical manifests, snapshots and commit deltas. Version uniqueness,
  crash safety, time travel, CDC, attach statistics, replication state and
  encryption keep their existing rules.
- The `catalog-sqlite` and `catalog-postgres` features are opt-in
  (`pinklake` combines with them explicitly), and the default binary stays
  lean.

## 2026-10-22
### Wave 91 — Docker image ✅
- pinkdb ships as a container: `docker run -p 5433:5433 -v pinkdb-data:/data
  ghcr.io/openlearnia/pinkdb` runs the server with a durable database on the
  volume; `docker run --rm -it ghcr.io/openlearnia/pinkdb pinkdb` gives the
  CLI. A `lake` tag adds the Parquet lake feature.
- Small (about 37 MB), runs as a non-root user, configurable with
  `PINKDB_*` environment variables, and reports its health to Docker.
- Images are built and published automatically with each release (the
  first images appear with the next release tag).

### Wave 102 (part 2) — `.pink` lakes read only what a query needs ✅
- Querying a `.pink` lake on S3/R2 now reads just the needed columns of the
  row groups that can match the query, with several range requests in
  flight; `COUNT(*)` and similar answers still come from the catalog alone.
- Lakes use one catalog version (7), which records where each `.pink` file's
  directory is, so no extra request is needed to find it.
- A table now always keeps its file format until you change it explicitly.
- Lakes written by earlier pinkdb versions are no longer readable (early
  development: one current format); they are refused with a clear message.

### Wave 102 (part 1) — a new, faster `.pink` file format
- `.pink` files use a new layout (`PINKDB06`): column data in row groups
  with a directory at the end, a checksum per column chunk, and new
  encodings (FastLanes bit-packing, FSST for text, LZ4).
- TPC-H SF1 saved as `.pink` is now 321 MB instead of 713 MB — slightly
  smaller than the same data as DuckDB Parquet — and opens far faster.
- Files from earlier pinkdb versions are no longer readable (pinkdb is in
  early development; there is one current format). They are refused with a
  clear message.

### Wave 98b — fast lakes on S3/R2 ✅
- Lakes on object storage read only what a query needs: lake files fetch just
  the queried columns with concurrent range requests, and whole files
  download in parallel parts. On R2, TPC-H Q6 over a Parquet lake went from
  23 s to 6 s.
- Encrypted lakes keep reading whole files (each file is one sealed
  container).
- `.lake attach` reports the requests and bytes it transferred.

### Wave 98a — fast remote Parquet ✅
- Reading Parquet from S3/R2/HTTPS is dramatically faster: TPC-H Q6 on a
  196 MB file went from 6 min 46 s to about 4 s, on par with DuckDB 2.0, and
  `count(*)` is answered from the file footer (29 s → 0.6 s).
- pinkdb now fetches only the columns a query needs, in one request per
  column chunk instead of many small ones, with several requests in flight
  while earlier data is decoded. `PINKDB_HTTPFS_PARALLELISM` sets how many
  (default 8; 1 restores the old serial reads).
- `EXPLAIN` shows how many columns a file scan reads (`4 of 16 columns`).

### Wave 94 — `database.table.column` references ✅
- Columns can be qualified with their database: `SELECT db1.t.a FROM db1.t`,
  and `db1.t.*` in a SELECT list. It works in every clause, in `UPDATE … SET`
  targets, and through the current database (`SELECT main.t.a FROM t`).
- Joining two tables with the same name from different databases now works;
  a plain `t.a` there is reported as ambiguous instead of silently picking
  one of them.
- An alias hides the table's own name, as in Postgres and DuckDB: with
  `FROM db1.t AS x`, write `x.a` (the error says so).

### Wave 89 — lake replication jobs ✅
- A lake can now keep copies of another lake's tables, or of a database in
  your session, in sync: `.lake replicate create <dest> from <source>`, then
  `.lake replicate catchup` or a background `.lake replicate start`.
- Each table syncs the cheapest safe way: by a timestamp/ID column you name,
  by replaying the source lake's changes, by primary key, or by a full copy —
  and falls back to a full copy whenever it can't be sure.
- One failing table doesn't stop the others; a job never overwrites a table
  it doesn't manage; a table dropped or excluded on the source is dropped from
  the copy; a recreated source table is re-copied from scratch.
- `.lake replicate status` and `tables` show how far behind each job and
  table is and the last error. Copied tables are ordinary lake tables (time
  travel, attach and changes all work).

### Wave 88 — mixed-format lakes ✅
- One lake can hold tables in different formats: `.lake format parquet for
  main.events` picks a table's format, and `.lake rewrite <dir> <table>
  format <id>` converts an existing table in one step.
- A format you choose explicitly is remembered in the catalog ("pinned"), so
  later saves and appends keep it. Tables written with the build's default
  still migrate to the saving build's format, as before.
- A table in a format this build can't read no longer blocks the whole lake:
  the other tables open, the CLI warns about the skipped one, reading it
  gives a clear error, and saving keeps it unchanged.
- Every format (`.pink`, Parquet) now passes the same contract test suite.
  It caught two bugs, now fixed: saving a DECIMAL or INTERVAL table crashed
  instead of giving an error, and very large INTERVAL values made
  `.lake attach` refuse the lake.

## 2026-10-21
### Wave 87 — lake data formats are pluggable; Parquet lakes ✅
- A lake's data files are now written by a registered **format provider**:
  `.pink` is built in, and the new `parquetlake` CLI feature adds
  **Parquet**.
- `.lake format parquet` (or `Lake::set_write_format`) picks the format for
  the next saves; attach, commits, S3, encryption and maintenance work the
  same for every format. INTERVAL columns are refused in Parquet (it cannot
  store microseconds exactly).
- A lake that names a format this build doesn't have is refused with a clear
  error; existing lakes are unchanged.

### Wave 86 — extensions can provide table functions and COPY formats ✅
- New **extension packages**: a host registers a named bundle of scalar
  functions, table functions/file readers and `COPY` formats; `LOAD
  EXTENSION '<name>'` turns it on for one database, atomically (a name
  clash refuses the whole package).
- Durable stores replay package usage after a reopen, and new log records
  that read through a package re-load it automatically.
- The Arrow extension can now be offered this way
  (`pinkdb_arrow::register_package()`, then `LOAD EXTENSION 'arrow'`), in
  addition to its existing always-on registration.

### Wave 85 — pinkdb in the browser (Playground) ✅
- The docs site has a **Playground**: the real engine compiled to wasm,
  running in your tab with no server, with example queries and a result
  grid.
- Keep your database in Memory, IndexedDB or OPFS; writes autosave, and you
  can Export/Import `.pink` files (the same format the CLI uses). Damaged
  files are refused without touching your saved data, and a second tab
  opens a read-only copy.
- New public `PinkDb::from_pink_bytes` loads a database from snapshot bytes.

### Wave 84 — query execution profiles ✅
- `execute_profiled` (embedded `PinkDb` and `Connection`) returns each
  statement's timings — parse, bind, optimize, execute, WAL log, total —
  with its rows, columns, kind and transaction state. Results and WAL bytes
  are unchanged, and plain `execute` costs exactly what it did.
- Server protocol **v10**: a client can turn profiling on per connection and
  receives one profile after each statement (`Client::set_profiling`,
  `Client::execute_profiled`). v9 clients are refused at the handshake.
- The CLI's `.timer on` line now shows the phase breakdown, locally and
  against a server.
- C API: `pinkdb_set_profiling` and `pinkdb_result_profile_*` accessors.

### Wave 83 — procedures can COMMIT, catch errors, and `DO` blocks ✅
- A procedure called **outside** a transaction can now `COMMIT` and
  `ROLLBACK` in its body (Postgres-style): long batch jobs keep the progress
  they committed if they later fail. Inside `BEGIN … COMMIT` this is refused
  ("started at a different scope"); a plain `CALL` stays all-or-nothing.
- `BEGIN … EXCEPTION WHEN OTHERS THEN … END` blocks catch errors: the
  block's work is undone and the handler runs with `SQLERRM`.
- New `DO $$ BEGIN … END $$` statement runs an anonymous block once.
- Durable stores log a committing procedure's work as self-contained
  segments, so replay is exact even if the procedure is later changed or
  dropped.
- Fixed: quoted variable names (`DECLARE "my var" …`) work, including
  non-ASCII names.

### Wave 81 — JavaScript procedures run (quickjs-ng) ✅
- With the `js` feature (+1.1 MB), `CREATE PROCEDURE … LANGUAGE JAVASCRIPT`
  now works: the body runs in quickjs-ng with `print`, `db.exec`,
  `db.query`, `db.value` and a seeded `db.random()`; `?` parameters are
  bound as values.
- Same guarantees as SQL procedures: a `CALL` is all-or-nothing, transaction
  control is refused, and durable stores replay calls exactly (no clock, no
  `Math.random`, no I/O, no `eval`).
- Runaway scripts stop cleanly at step, host-call, memory and stack limits;
  syntax errors are reported at `CREATE` time.
- `print` lines come back with the call and the CLI shows them (not yet over
  the server protocol or FFI). Native targets only for now.

### Fix — DOUBLE NaN comparison semantics now match DuckDB ✅
- `NaN = NaN` is true, NaN equals no other value and sorts above every other
  double (`+inf` included), and `-0.0 = 0.0` — in filters, ORDER BY (NaN last
  ascending, first descending), MIN/MAX (`max` over a column with a NaN is
  NaN), range joins and window ordering. Scans used to treat NaN as equal to
  every value (`WHERE d = 0.0` matched NaN rows), disagreeing with the hash
  index; an index once again changes only the plan, never the result.
- Parquet row-group pruning keeps a DOUBLE row group for `>`/`>=`/`= NaN`
  (its statistics never cover NaN); ORDER BY over NaN keys now takes the fast
  byte-key sort instead of falling back.

### Index persistence in `.pink` (wave 79, part 2) ✅
- Index definitions are now stored **in the `.pink` file** (v5 flags bit 3,
  about 30 bytes per index), so a checkpoint publishes indexes atomically with
  the data and no longer re-logs them into the fresh WAL.
- The hash maps are rebuilt from the table's rows when the file is opened, so
  an index can never disagree with the data it indexes. (Storing the maps
  themselves was measured: no faster to open once verified, and 2–4 MB larger
  per million-row index.)
- Stores without indexes are byte-identical to before; files with indexes
  need this build or newer (older builds refuse them with "upgrade pinkdb").
- Still open in wave 79: using indexes/zone maps for range predicates.

### Security hardening — pass 7 (final): terminal escaping, crypto/WAL/loader/FFI hardening ✅
- A single terminal sanitizer now covers **every rendered identifier** (all
  result variants, column names, catalog/lake output, errors), closing ANSI/OSC
  injection while preserving legitimate UTF-8.
- Engine containers v4 and lake containers v2 use a 128-bit nonce base (96-bit
  derived GCM nonces); WAL v4 authenticates its publish block; tampering is
  rejected. Legacy formats still open, and are rewritten on the next save.
- The extension loader validates zero-arg types, owns its collector, refuses
  post-init registration, bounds VARCHAR copies and ignores non-VARCHAR frees;
  the FFI bounds `value_count` and pins `PinkValue`'s layout. The sync transport
  enforces a per-frame deadline; the CI installer and Docker images are pinned.

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
  `lake-lite`) and `lake` (the full lake; later removed),
  in cargo features, help text, tests, CI, README and the docs site.
- **`s3` is pure storage**: it implies no lake build — combine it
  explicitly (`pinklake,s3`; `--features s3` alone has no `.lake` commands
  and says so). No old aliases survive.

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
  refused by keyed sessions. Encrypted lake attach reads whole objects in
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
- Every v4 data file publishes a CRC-protected **zone map** in the catalog;
  the planner prunes whole files for exact predicates —
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
- **Waves 35 + 35b** — lake data format + parser cleanup.
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
