# pinkdb — a mini DuckDB in Rust

An embedded **analytical** SQL database: in-memory columnar storage, vectorized
execution, SQL with aggregates and joins, CSV import. Ships as a library crate
(`pinkdb`), a CLI REPL (`pinkdb-cli`, a thin consumer of the library), and a
network server (`pinkdb-server`, a separate crate so the core stays lean).

```sql
.import sales.csv sales
SELECT region, AVG(total) FROM sales GROUP BY region ORDER BY 2 DESC;
```

```
┌─────────┬────────────┐
│ region  │ avg(total) │
│ VARCHAR │ DOUBLE     │
├─────────┼────────────┤
│ west    │ 175.0      │
│ east    │ 50.5       │
└─────────┴────────────┘
2 rows
Run Time (s): real 0.000
```

## Quick start

```sh
cargo run -p pinkdb-cli          # debug REPL
cargo build --release -p pinkdb-cli
```

### Install a release binary

Linux (x86_64/aarch64) and macOS (arm64/x86_64) release builds are published
from `v*` tags. Download the installer, inspect it, and run it — the script
fetches `SHA256SUMS`, verifies the artifact (fail closed), and installs
atomically to `${PREFIX:-$HOME/.local}/bin`:

```sh
curl -fsSLO https://raw.githubusercontent.com/openlearnia/pinkdb/main/install.sh
sh install.sh
```

Pin a version or choose a feature variant:

```sh
sh install.sh --version 0.1.0 --variant default
sh install.sh --variant pinklake-s3     # .lake over .pink files + s3://
sh install.sh --variant lean            # no linked json/datetime/math
```

`--dry-run` prints the resolved version, asset, URL and destination without
downloading; `--force` replaces an existing install. Supported variants are
`default`, `lean`, `pinklake`, `pinklake-s3`.
Server binaries (`pinkdb-server-<version>-<os>-<arch>-{default,async-server,tls}.tar.gz`)
and the `SHA256SUMS` manifest are attached to the same release. **Windows is
not supported** — there is no Windows binary or `install.ps1`. See
[`docs/releasing.md`](docs/releasing.md) for the full matrix, checksum
verification, and immutable-release policy.

### Docker image

The same release publishes a public multi-arch image (linux/amd64 and
linux/arm64) to GHCR. It contains `pinkdb-server` (TLS + async transport) and
the `pinkdb` CLI, runs as a fixed non-root user (UID/GID 10001), keeps the
database on the `/data` volume, and has a protocol-level `HEALTHCHECK`:

```sh
docker run -p 5433:5433 -v pinkdb-data:/data ghcr.io/openlearnia/pinkdb
```

The server listens on `0.0.0.0:5433` and opens
`/data/pinkdb.pink` (durable mode: checkpoint + WAL). Use the image's own CLI
against it, locally or in a second container:

```sh
docker run --rm -it ghcr.io/openlearnia/pinkdb pinkdb --host host.docker.internal:5433
# or, from inside the same container network namespace:
docker run --rm --network container:<name> ghcr.io/openlearnia/pinkdb \
  pinkdb --host 127.0.0.1:5433
```

Two CLI variants are tagged: `:<version>` / `:latest` (default features plus
`pinklake,s3` — `.lake` over `.pink` data files and `s3://` storage) and
`:<version>-lake` / `:lake` (adds `pinklake,parquetlake,s3` — Parquet data
files; a larger image). Both share the same server binary.

Every `pinkdb-server` flag still works after the image name; explicit flags
win over the environment:

```sh
docker run -p 6000:6000 -v pinkdb-data:/data ghcr.io/openlearnia/pinkdb \
  --port 6000 --idle-timeout 60
```

The entrypoint maps the common settings from the environment:
`PINKDB_HOST` (default `0.0.0.0`), `PINKDB_PORT` (`5433`), `PINKDB_FILE`
(`/data/pinkdb.pink`), `PINKDB_TRANSPORT` (`sync`; the image has both
transports), `PINKDB_TLS_CERT`/`PINKDB_TLS_KEY`, the limit flags
(`PINKDB_MAX_CONNECTIONS`, `PINKDB_IDLE_TIMEOUT`,
`PINKDB_STATEMENT_TIMEOUT_MS`, `PINKDB_MAX_RESULT_ROWS`,
`PINKDB_MAX_RESULT_BYTES`, `PINKDB_MAX_CONCURRENT_LOGINS`), and the
extension/COPY opt-ins (`PINKDB_ALLOW_LOAD_EXTENSION`,
`PINKDB_EXTENSION_ROOT`, `PINKDB_ALLOW_COPY_TO`, `PINKDB_COPY_ROOT`).
Environment beats the image defaults and explicit arguments beat both.
`PINKDB_HEALTHCHECK_ADDR` and `PINKDB_HEALTHCHECK_TIMEOUT_MS` tune the
`HEALTHCHECK`; `docker exec <name> pinkdb-server --health-check` is the same
probe from the shell.

**RBAC is per store, not an image setting.** Authentication switches on when
the served database has users: create the first one over the wire
(`CREATE USER … PASSWORD …`, then `GRANT ADMIN …`) and the catalog becomes the
source of truth. Until then every connection is trusted, so do not publish
port 5433 to an untrusted network before the store has users and, for
encrypted transport, TLS certificates. The image's `pinkdb-server` is built
with the `tls` and `async-server` features; TLS is still `sync`-only.

The image is built from source by the root `Dockerfile`
(`--build-arg VARIANT=default|lake` selects the CLI features) and published by
`.github/workflows/docker.yml` on `v*` tags with provenance and an SBOM. See
[`docs/releasing.md`](docs/releasing.md) and
[`scripts/docker/smoke.sh`](scripts/docker/smoke.sh).

Library use:

```rust
use pinkdb::PinkDb;

let mut db = PinkDb::new();
pinkdb::csv::import_csv_string(&mut db, "t", "a,b\n1,x\n2,y\n")?;
for result in db.execute("SELECT a, upper(b) FROM t WHERE a > 1;")? {
    println!("{result:?}");
}
```

## Architecture

```
SQL text
  → PEG parser → AST
  → Binder (resolve tables/columns, assign types)
  → Logical plan (scan → filter → join → aggregate → project → sort → limit)
  → Optimizer (predicate pushdown, constant folding)
  → Vectorized executor (operators exchange DataChunks of ≤2048 rows)
  → In-memory columnar storage
```

| Crate / module | Role |
|---|---|
| `pinkdb::types` | `DataType`, `Value`, `PinkDbError` |
| `pinkdb::vector` | `Vector` (typed data + validity bitmask, bit = valid) + `gather`, `DataChunk` |
| `pinkdb::storage` | `DataTable` (per-column `Vector` chunks), `PinkDb` catalog (databases → tables/views) |
| `pinkdb::csv` | CSV import + type inference (rayon-parallel parse) |
| `pinkdb::sql` | PEG grammar, AST, binder |
| `pinkdb::plan` | Logical plan tree |
| `pinkdb::optimizer` | Constant folding, predicate pushdown (incl. across joins) |
| `pinkdb::exec` | Vectorized expression eval, hash aggregate, hash join, operators (rayon-parallel) |
| `pinkdb::persist` | `.pink` single-file columnar persistence, `PINKDB06` (row groups + a footer with a per-table row-group directory, per-chunk CRC32C and exact per-chunk statistics; typed paths + delta/dict-bitpack/ALP/FastLanes/FSST encodings + a compression frame; one format, no legacy) |
| `pinkdb::crypto` | optional at-rest encryption (`PINKENC1` container: AES-256-GCM + Argon2id) behind the `encryption` feature; a parse/identity stub otherwise |
| `pinkdb::db` | Multi-writer `Database`/`Connection` over `Arc<RwLock<PinkDb>>` |
| `pinkdb::procedure` | Stored procedures: the SQL procedural language (parser + `CALL` executor), catalog definitions, `.pink` section |
| `pinkdb-wasm` | The browser playground's glue: a `cdylib` with a raw JS-callable ABI (`pw_open`/`pw_exec`/`pw_snapshot`/`pw_restore`, JSON results) over the wasm32 core |
| `pinkdb-js` | Optional (`js` feature) JavaScript procedure sandbox: quickjs-ng via `rquickjs` (~1.1 MB), the deterministic host contract, limits and value conversions |
| `pinkdb-cli` | rustyline REPL, dot-commands, box-drawn output |
| `pinkdb-server` | TCP server + blocking client (framed binary protocol, thread-per-connection); no external deps |
| `pinkdb-arrow` | opt-in (`arrow` feature) Arrow IPC (Feather v2) file extension, engine-adjacent: a **table source** for `FROM 'f.arrow'` / `read_arrow` and a **COPY sink** for `COPY … (FORMAT arrow)`; reads materialize the file, writes are uncompressed; its `remote` feature (enabled by the CLI `httpfs` feature) reads `https://`/`s3://` objects whole |
| `pinkdb-parquet` | opt-in (`parquet` feature) read-side Parquet extension, engine-adjacent: `read_parquet`/`parquet_scan`, the four `parquet_*` metadata functions and DuckDB's replacement scan (`FROM 'file.parquet'`), with projection + row-group-statistics pushdown; flat schemas, TIMESTAMP(nanos)/TIME/UUID/FLOAT16/nested types refused by name; its `remote` feature (enabled by the CLI `httpfs` feature) reads `https://`/`s3://` objects through a ranged `ChunkReader` |
| `pinkdb-httpfs` | opt-in (`httpfs` feature) remote-file transport + `read_csv` table source: `https://` GET/HEAD/byte-range and the SigV4 `s3://` object client **shared with pink-lake and the parquet/arrow `remote` features**; `read_csv('url')`, `.import <url>`, `FROM 'https://…/x.csv'`; single objects only (no globs/compression/COPY TO yet) |
| `pink-lake` | lake layer: a catalog database (`pink:` `.pink` file, `sqlite:`, or `postgres://`) on DuckLake v1.1's tables plus immutable data files — `.pink` (`PINKDB06`) always, Parquet with `parquetlake`; data on local disk or S3/R2 (opt-in `s3`); opt-in `encryption` seals each data/delete file with its own key in a `PNKLAKE1` container; `catalog-sqlite` / `catalog-postgres` add the SQL backends |

Design rules: Rust edition 2024 (MSRV 1.97), zero async (the server's opt-in
`async-server` transport, wave 47, is the documented I/O-boundary carve-out);
the core is
zero-unsafe, with
`unsafe` contained to exactly two crates (the C ABI `pinkdb-ffi` and the
extension loader `pinkdb-ext-loader`). Dependencies are `csv` + `rustyline`,
plus `rayon` for parallel execution and `peg` for the SQL parser. The server
crate adds no external dependencies; the extension loader adds `libloading`
(dlopen) and the core links it on native targets only — wasm builds use the
linked registry model, which adds no dependency at all (see Extensions
below).
With the `encryption` feature the audited RustCrypto crates `aes-gcm` +
`argon2` are linked (optional dependencies: the default build stays
crypto-free).

SQL is parsed by a `peg`-generated grammar — the only parser path. A committed
corpus plus a seeded randomized fuzz test (`cargo test`) check that valid input
parses and invalid input returns a clean `Parse` error; parse-error messages are
positional.

## SQL surface (v1)

```sql
CREATE TABLE t (id BIGINT, name VARCHAR, price DOUBLE);
CREATE TABLE db1.t (...);            -- qualified: any db.table target works
INSERT INTO t VALUES (1, 'x', 9.5), (2, NULL, 3.0);
UPDATE t SET price = price * 1.1 WHERE id > 1;   -- SET exprs see pre-update values
DELETE FROM t WHERE price IS NULL;               -- NULL never matches; both are statement-atomic
BEGIN; UPDATE t SET price = 0; ROLLBACK;         -- flat snapshot transactions (in-memory only)
CREATE DATABASE db1;                 -- new, empty database
DROP DATABASE db1;                   -- last database cannot be dropped; dropping the
                                     -- current one switches the session to main
USE db1;                             -- per-session current database (default: main)
LOAD EXTENSION 'extensions/hello.dylib';   -- load a native extension (ABI v1)
LOAD EXTENSION 'windowed_math';            -- on wasm: load a linked registry extension
CREATE PROCEDURE p(a INTEGER, b VARCHAR) AS $$
BEGIN INSERT INTO t VALUES (a, b); END $$;     -- stored procedure (see below)
CALL p(1, 'x');                              -- run it; DROP PROCEDURE p removes it
CREATE USER alice PASSWORD 's3cret';         -- Argon2id hash at DDL time (wave 48)
ALTER USER alice PASSWORD 'rotated';         -- replace a user's password
DROP USER [IF EXISTS] alice;
CREATE ROLE analyst;  DROP ROLE [IF EXISTS] analyst;
GRANT analyst TO alice;                      -- role membership (transitive; cycles refused)
GRANT SELECT, INSERT ON TABLE t TO analyst;  -- table privileges (or ALL)
GRANT CREATE ON DATABASE main TO analyst;    -- database DDL privilege (or ALL)
GRANT ADMIN TO alice;                        -- global admin: every check passes
REVOKE SELECT ON TABLE t FROM analyst;       -- GRANT/REVOKE are idempotent
SELECT [DISTINCT] expr [AS alias], ...
FROM t [<join> u ON <cond>] ...    -- multi-way; INNER / LEFT [OUTER] / RIGHT [OUTER] /
                                   -- FULL [OUTER] / CROSS JOIN; composite ON
                                   -- (several equality pairs), non-equi predicates
                                   -- (>, <, >=, <=, !=) run over a nested loop
  | FROM db1.t                    -- qualified tables resolve in that database
  | FROM (SELECT ...) AS s         -- derived tables (alias required)
  | WITH w AS (SELECT ...) ...     -- CTEs, incl. WITH w(a, b) AS column aliases
WHERE expr                        -- incl. IS [NOT] NULL, BETWEEN,
                                  -- IN (subquery|list), EXISTS (subquery)
GROUP BY ... HAVING ...
ORDER BY expr [ASC|DESC] [NULLS FIRST|LAST], ...  -- default NULLS LAST both ways;
                                                  -- ORDER BY 2 is positional
LIMIT n [OFFSET m] | OFFSET m;
<query> UNION|INTERSECT|EXCEPT [ALL] <query>      -- INTERSECT binds tighter
-- Scalar subqueries `(SELECT ...)` anywhere an expression goes
-- (uncorrelated only: exactly 1 row × 1 col, 0 rows → NULL).
```

### Browser playground

The docs site's **Playground** page (wave 85) runs the real engine compiled
to wasm in a Web Worker — no server. Storage is Memory (session-only),
IndexedDB or OPFS: each keeps a whole `.pink` snapshot (the same format the
CLI writes), autosaved 500 ms after a write, atomically (one IndexedDB
transaction; OPFS writes a temp file and renames it). Export/Import move
`.pink` files in and out; a snapshot the engine refuses (corrupt, newer
format) is reported and never overwritten. A second tab on the same
persistent store gets a read-only in-memory copy (Web Locks). No COOP/COEP
headers and no third-party requests. `website/tools/build-wasm.sh` builds
the module (`crates/pinkdb-wasm`, ~2.25 MiB) and
`website/tools/wasm-smoke.mjs` tests it under Node. A WAL-grade
virtual file system for crash-safe incremental writes is a follow-up.

### Query profiles

`PinkDb::execute_profiled` and `Connection::execute_profiled` (wave 84) run
SQL exactly like `execute` and also return one `QueryProfile` per statement:
`parse` (shared by the statements of one call), `bind` (which also builds
the plan), `optimize`, `execute` (including lease waits and conflict
retries), `log` (WAL append + auto-checkpoint), `total`, plus `rows`,
`columns`, the result `kind`, `in_transaction` and `engine_version`.
Profiling only reads clocks: results and WAL bytes are identical, and plain
`execute` is unchanged (measured equal before/after; profiling itself costs
~2% on tiny statements). The `Connection` path has no optimizer step, so its
`optimize` is always 0. Remote clients enable it per connection
(`Client::set_profiling`, protocol v10); C callers use
`pinkdb_set_profiling` and the `pinkdb_result_profile_*` accessors.

### Stored procedures

```sql
CREATE [OR REPLACE] PROCEDURE name(param TYPE, ...) AS $$ ... $$;
DROP PROCEDURE [IF EXISTS] name;
CALL name(args, ...);
DO $$ BEGIN ... END $$;   -- an anonymous block, run once (wave 83)
```

The body is one `BEGIN ... END` block written as a string literal —
preferably dollar-quoted (`$$ ... $$`), so inner quotes and semicolons need no
escaping. Inside it:

* `DECLARE name TYPE [DEFAULT expr]` — a typed local variable (NULL without
  `DEFAULT`), visible from its declaration onward in its block and in nested
  blocks;
* `SET name = expr` (also `SET name := expr`, or a bare `name := expr`) — the
  value is coerced to the declared type with the engine's INSERT coercion;
* `IF ... THEN ... [ELSIF ... THEN ...]* [ELSE ...] END IF`, `WHILE ... DO ...
  END WHILE`, nested `BEGIN ... END` blocks — a condition must be BOOLEAN,
  NULL counts as false;
* `RETURN [expr]` — end the call; `RETURN expr` becomes the call's result (a
  one-row result set named `return`), `RETURN` alone keeps the last
  statement's result;
* `RAISE expr` — abort the call with that (VARCHAR) message;
* `BEGIN ... EXCEPTION WHEN OTHERS THEN ... END` — a block with an error
  handler (wave 83): an error inside the block undoes the block's work (a
  savepoint) and runs the handler, where `SQLERRM` holds the message;
  variables keep their values, and an error in the handler propagates;
* `COMMIT` / `ROLLBACK` — transaction control in a standalone `CALL` (wave
  83, see below);
* any ordinary SQL statement (DML, table DDL, `SELECT`, `SHOW`, `DESCRIBE`,
  `EXPLAIN`) and nested `CALL`s (recursion is bounded at 32 levels).

`CALL` returns the call's own outcome: the procedure's name, how many body
statements ran, and the result of the last statement it executed (`SELECT`
rows, an `Inserted` count, …). Store/process-level statements are refused in a
body at `CREATE` time (`USE`, `BEGIN` as a transaction start, `CHECKPOINT`,
`LOAD EXTENSION`, `CREATE`/`DROP DATABASE`, materialized-view DDL, nested
procedure DDL and `DO`).

**Values, never text.** Parameters and variables are handed to the binder as
bound values at each statement's bind, so no input can change the shape of an
executed statement (injection-safe by construction), and every body statement
rebinds against the current catalog per call. Variables shadow table columns
of the same name; qualify the column (`t.x`) to reach it.

**Transactions and atomicity.** A top-level `CALL` is statement-atomic: the
store is snapshotted first and restored if the body fails, so a failing call
leaves no trace — not even a table it created. Inside an open transaction the
same rule applies to the call itself (its own partial effects are undone),
while the surrounding transaction stays open with its earlier statements and
its place in the commit order — a statement-level rule, not a savepoint.
`CREATE`/`DROP PROCEDURE` are refused inside a transaction: the definition is
catalog metadata, like a materialized view's.

**A procedure that manages its own transaction** (wave 83, Postgres-style).
When a `CALL` runs **outside** any transaction, the outermost procedure body
may `COMMIT` (make everything so far durable and start a new transaction)
and `ROLLBACK` (undo back to the last `COMMIT` and continue). A later failure
then undoes only the work since the last `COMMIT`. Inside `BEGIN … COMMIT`
the caller owns the transaction, so a body `COMMIT`/`ROLLBACK` fails with a
"started at a different scope" error; nested procedures, `DO` blocks,
JavaScript bodies and blocks with an `EXCEPTION` handler cannot use them
either. Use this for long batch jobs that should keep the progress they made
(commit every N rows); keep the default — an atomic `CALL` — whenever the
work must be all-or-nothing. Compared with Snowflake: a Snowflake procedure
called outside a transaction is *not* atomic (a failure leaves earlier
statements committed); pinkdb keeps a standalone `CALL` atomic unless the
body commits explicitly. Snowflake-style autonomous inner transactions (an
inner commit surviving an outer rollback) are not supported.

**`DO` blocks** run an anonymous body once — the same language, atomic like
a `CALL`, no parameters and no transaction control. Like `CALL`, running one
requires `ADMIN` in an authenticated session.

**Durability.** Procedures live per database in the catalog, are written to
`.pink` (flags bit 4) and replay through the ordinary WAL path: a durable
store logs the committed `CALL` statement text and replays it by re-executing
the call (a call that used `COMMIT`/`ROLLBACK` logs each committed segment
instead: the statements that ran, as self-contained `DO` blocks with every
variable pinned to its exact value, so replay never re-runs the procedure).
That is sound because the engine has no nondeterministic built-in
function (no clock, no randomness) and the constructs that could break replay
are refused while the call runs — a body statement that reads an attached
lake table, reads a session-local view (durable stores only), or calls a
loadable-extension function (an opaque callback) fails the call with a clear
error, and `CALL` arguments must be constant deterministic expressions. A
call's actual read/write set (it depends on the data: loop conditions, `IF`
branches) is recorded by the executor into the WAL frame and the transaction's
conflict set, so multi-writer conflict detection still holds.

**JavaScript procedures** (`LANGUAGE JAVASCRIPT`, wave 81) are an optional
build (`js` cargo feature, +1.1 MB): the body runs in **quickjs-ng** as a
strict-mode function of the declared parameters.

```sql
CREATE PROCEDURE add_many(n INTEGER, prefix VARCHAR) LANGUAGE JAVASCRIPT AS $$
  for (let i = 1; i <= n; i++) {
    db.exec('INSERT INTO t VALUES (?, ?)', [i, prefix + i]);
  }
  print('inserted', n);
  return db.value('SELECT count(*) FROM t');
$$;
CALL add_many(3, 'r');
```

A script sees only `print(...)`, `db.exec(sql, params?)` (affected rows),
`db.query(sql, params?)` (rows as arrays), `db.value(sql, params?)` (first
column of the first row) and `db.random()`; `?` placeholders are bound as
values, never spliced into SQL. The same rules as SQL bodies apply: a `CALL`
is statement-atomic, transaction control and store-level statements are
refused, and a durable store logs the `CALL` and replays it. Replay is exact
because the sandbox has no clock (`Date`, `performance`), no `Math.random`
(`db.random()` is seeded from the definition), no I/O, timers or modules, and
no run-time code compilation (`eval`, `Function`). Each call gets a fresh
engine with step, host-call, memory and stack limits, and JavaScript calls
nest at most 4 deep. Values: integers within ±2^53 are numbers, larger
BIGINTs are `BigInt`; DATE/TIMESTAMP/DECIMAL/INTERVAL arrive as their text.
`print` lines are returned with the call (`QueryResult::Called::output`) and
shown by the CLI; the server protocol and FFI do not carry them yet. The
body is compiled at `CREATE` time, so a syntax error is reported then.
Without the feature, `CREATE PROCEDURE … LANGUAGE JAVASCRIPT` names the
rebuild (`--features js`). Native targets only for now: the engine compiles
for wasm32 but is not yet verified there. The SQL procedural language needs
no feature and is always available.

### Databases

Databases are named, case-insensitive namespaces (`main` by default). A
session (the embedded `PinkDb`, or each `Connection`) has a current database;
unqualified table names resolve there. `USE db` switches it and never affects
other sessions — `Database`/`Connection` carry their own current database, the
shared store is untouched. `db.table` qualifies any table reference in
`FROM`/`JOIN`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `DROP TABLE`,
`CREATE VIEW` and `DROP VIEW`; cross-database joins (`db1.t JOIN db2.u`) work.
A column may carry the same database qualifier: `db.table.column` anywhere a
column expression is allowed (SELECT list, WHERE, HAVING, GROUP BY, ORDER BY,
join `ON`, window `PARTITION BY`/`ORDER BY`, function arguments, subqueries,
CTE bodies, `UPDATE … SET` right-hand sides/`WHERE`, `DELETE … WHERE`), and
`db.table.*` in a projection (alone or beside other columns). `db.table` is
resolved exactly like the FROM clause (case-insensitive; the database must
exist) and must name an **unaliased** base table or view of that database —
`SELECT db.t.a FROM db.t AS x` fails and tells you to write `x.a`, while the
workaround `FROM db.t AS x … x.col` is unchanged. An unqualified `FROM t`
belongs to the session's current database, so `SELECT main.t.a FROM t` works
while `main` is current. Derived tables, CTEs, table functions and file scans
have no database and never match. Two unaliased tables with the same name can
only come from different databases (`FROM db1.t JOIN db2.t`): a two-part
`t.c` there is an ambiguous-reference error naming both, so use `db1.t.c`,
`db2.t.c` or aliases. Qualifiers in `UPDATE … SET` must name the statement's
own target table (and database); errors name the full reference. CTEs shadow
unqualified names only; a qualified name never resolves to
a CTE. Views are session-local *and* per-database: created in the current
database and resolved there, with their body resolving unqualified names in
that same database. View bodies are self-contained: the caller's CTEs never
leak in, so a CTE named like a table in the body does not shadow that table.
`DROP DATABASE` refuses the last database and switches a
session that was using the dropped database to `main` (or the first remaining
database). `CREATE`/`DROP DATABASE` are refused inside a transaction.

- Aggregates: `COUNT(*)` / `COUNT(x)` / `COUNT(DISTINCT x)`, `SUM`, `AVG`, `MIN`, `MAX`
- Window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `lag`/`lead` (offset,
  default), `ntile`, `first_value`/`last_value`, and aggregates `OVER`
  (`PARTITION BY` / `ORDER BY` / `ROWS` and `RANGE` frames — with ORDER BY the
  default frame is `RANGE UNBOUNDED PRECEDING AND CURRENT ROW`, so peers share
  the value; no nesting)
- Scalar functions: `abs`, `round` (1–2 args), `upper`, `lower`, `length`,
  `substr`, `concat` / `||`, `trim`/`ltrim`/`rtrim`, `replace`, `nullif`,
  `greatest`/`least`, `extract`/`date_part`, `sqrt`, `floor`/`ceil`, `sign`,
  `power`/`pow`, `mod`
- `CAST(x AS type)` (numeric tower both ways, bool↔numeric, varchar parse/format,
  date↔timestamp; bad input and out-of-range narrowing error; DOUBLE → integer
  rounds half away from zero, like DuckDB), `COALESCE(a, b, ...)` (common type),
  `LIKE`/`NOT LIKE` (`%`/`_` patterns)
- `CASE WHEN c THEN v ... [ELSE e] END` + simple `CASE x WHEN v` form
  (NULL conditions never match; result types unify like COALESCE)
- Set ops: `UNION [ALL|DISTINCT]`, `INTERSECT [ALL]`, `EXCEPT [ALL]`
  (`INTERSECT` binds tighter than `UNION`/`EXCEPT`; numeric types unify;
  ORDER BY/LIMIT/OFFSET after the set operation apply to the whole result —
  branch-level clauses need parentheses, unsupported in v1); `CREATE VIEW v
  [(cols)] AS ...` (session-local, rebound per use), `DROP TABLE` / `DROP VIEW`
- DDL/DML: `CREATE TABLE ...` (with `IF NOT EXISTS`), `CREATE TABLE t AS
  SELECT ...`, `DROP TABLE [IF EXISTS]`, `INSERT INTO t [(cols)] SELECT ...`,
  `CREATE/DROP DATABASE [IF [NOT] EXISTS]`
- Utilities: `EXPLAIN <select>` (stable plan tree), `SHOW TABLES`,
  `SHOW DATABASES`, `DESCRIBE t` (DuckDB's column shape)
- Types: `BOOLEAN`, `INTEGER` (i32), `BIGINT` (i64), `DOUBLE` (f64), `VARCHAR`,
  `DATE` (days since epoch), `TIMESTAMP` (microseconds, naive, no time zone)
- Literals: `DATE '2024-02-29'`, `TIMESTAMP '2024-03-15 12:30:45.5'`
- Date ops: `date ± integer` (days), `date - date` → BIGINT days;
  `year/month/day/hour/minute/second(x)`, `date_trunc(part, x)`
- CSV auto-detects `YYYY-MM-DD` → DATE and `YYYY-MM-DD HH:MM:SS[.f]` → TIMESTAMP
- Coercion: INTEGER → BIGINT → DOUBLE; `int / int` → DOUBLE (DuckDB rule)
- Three-valued logic: NULL fails `WHERE`; `COUNT(x)`/`SUM`/`AVG` skip NULLs;
  NULL group keys group together; NULL join keys never match; LEFT/RIGHT/FULL
  joins NULL-extend the unmatched side's columns (FULL emits unmatched rows
  from both sides)
- Integer division/modulo by zero is an error; `0.0` float division is IEEE
- Integer overflow is an error, like DuckDB: `+`/`-`/`*`, negation, `abs`,
  `date ± n` and narrowing casts report `Overflow in …` instead of wrapping;
  `SUM` over integers accumulates in 128 bits and errors only when the final
  result does not fit BIGINT

Scripting follows sqlite3/duckdb conventions: errors go to **stderr**; when
stdin is not a terminal (or with `-c COMMAND`, repeatable) there is no banner
and no per-statement timing (`.timer on` turns it back on; the line reads
`Run Time (s): real 0.012 (parse 0.010, bind 0.020, optimize 0.005, execute
11.900, log 0.090 ms)`, from the local engine or, remote, from the server's
profiles), and the process
**exits 1** if any statement or dot-command failed. A statement still open at
end of input runs as if terminated, and `;` is recognized outside strings and
comments (`SELECT 1; -- note` completes; `SELECT ';'` does not).
`.read <file.sql>` runs a script in the current session.

REPL dot-commands: `.help`, `.quit` / `.exit`, `.read`, `.timer`, `.databases`, `.tables`,
`.schema <t|v>` (accepts `db.table`; views show their output columns),
`.import <file.csv> [table]` (table defaults to the file stem; `db.table`
targets another database; with `--features httpfs`, an `https://`/`s3://` URL),
`.save <file.pink>`, `.open <file.pink>`.
`.databases` marks the current one and counts tables and views separately
(`main (current) (2 tables, 1 view)`). `.tables` and `.schema` scope to the
current database; `USE <db>` switches it. Double-quoted identifiers with
spaces/quotes work in dot-command arguments (`"My Db".t`). For `.import`, a
dotted target is `db.table` only when the left side names an existing
database — otherwise the whole string is the table name, so a file stem like
`data.2024.csv` imports as `data.2024`.

CSV inference (per column, over all non-empty cells): all i32 → INTEGER, all
i64 → BIGINT, all f64 → DOUBLE, all true/false (case-insensitive) → BOOLEAN,
else VARCHAR. Empty cell → NULL.

Types are inferred from the first 20 480 rows (DuckDB's sample size); if a
later row disagrees with the sampled schema, the import transparently reruns
with whole-file inference, so the final schema always matches the rule above.
Files containing no `"` and no `\r` bytes are parsed in parallel across the
rayon pool; anything else goes through the `csv` crate sequentially.

## Persistence

`db.save("x.pink")` writes every database and table in a single-file
columnar format; `PinkDb::open("x.pink")` loads it back (`.save` / `.open`
in the REPL). The format is **`PINKDB06`** — one format, no legacy: the
readers of `PINKDB01`–`PINKDB05` are gone with their fixtures and their
compatibility tests, and a file written by an older pinkdb is refused with
`this .pink file was written by an older pinkdb (PINKDB0N); this build
reads PINKDB06 only`.

### The layout (Parquet-shaped)

```text
data region: for every table (databases and tables in sorted name order)
  for every row group (122,880 rows = 60 engine chunks of 2,048)
    for every column (schema order)
      column chunk = page index + pages

footer: store metadata (databases, tables, schemas, column defaults,
  constraints, index definitions + statistics, materialized views,
  procedures, macros, RBAC) and, per table, the row count and the
  row-group directory:
    row groups [ { rows, per column: offset, length, page count,
                             encoding summary, crc32c,
                             stats { min, max, null_count, nan_count } } ]

trailer (20 bytes): footer_crc32c u32 | footer_len u64 | "PINKDB06"
```

A **page** is one engine chunk (2,048 rows) encoded with the per-chunk
encoder, so decoding stays chunk-native; a **column chunk** is the 60 pages
of one (row group, column) pair. Every column chunk carries its own
**CRC32C** and the footer carries its own — there is **no whole-file CRC**,
so any chunk verifies on its own and a damaged chunk never invalidates the
rest of the file. The magic lives in the **trailer**, at the end of the
file: a reader needs the 20-byte trailer plus the footer (two small range
requests), and the lake's `pink` provider reads exactly those plus the
projected column chunks of the row groups that survive pruning.

The footer records the **exact** statistics of every column chunk — its
min and max (first-seen wins ties, exactly like the engine's MIN/MAX
accumulators), its NULL count and its NaN count — which feed zone-map
pruning and the lake's exact `COUNT`/`MIN`/`MAX`.

The encoder and the decoder run on typed per-column paths (no per-row
`Value` materialization); chunks save in parallel and load in parallel
(columns × row groups, assembled in order), and **serial and parallel
saves produce identical bytes**. Views are session-local and never
persisted. Round-trips are bit-exact (including `-0.0` and NaN payloads);
truncated or corrupt files return errors, never panics — the footer
checksum is verified before a single byte of it is decoded, and no
allocation is sized from an unvalidated length.

### The encodings

Per-chunk encoding is picked by size (smallest wins; ties keep the
earlier candidate — the candidate order is part of the format contract):

- Integers (INTEGER/BIGINT/DATE/TIMESTAMP): Plain (raw LE) vs RLE (low
  run count) vs Bitpack (small range, min + width + packed bits) vs delta
  bitpack (first value + min delta + width + packed deltas) vs
  **FastLanes bit-packing** (1,024-value blocks, transposed lanes, a frame
  of reference per block).
- Doubles: Plain (raw bits) vs RLE vs ALP (decimal-scaled integer codes
  plus a bit-exact exception list; non-finite, `-0.0`, subnormals and
  out-of-range values ride the exceptions).
- Booleans: Plain (bitpacked) vs RLE.
- Varchars: Plain (offsets + data) vs Dictionary (first-occurrence codes)
  vs RLE vs dictionary-bitpack (the dictionary with minimal-width codes,
  packed in the FastLanes transposed layout) vs a **compression frame**
  (tag 8) around the plain layout vs **FSST** (tag 9: a symbol table that
  rewrites the string bytes, then the second compression stage).

The **second compression stage** — the general-purpose compressor a chunk
frame rides on — is a **measurement**, not a guess: wave 102 measured the
hand-rolled LZ77 of wave 45j against the pure-Rust `lz4_flex` on the TPC-H
SF1 set (18,569 VARCHAR chunks, 569.6 MB of plain payloads):

| second stage | size | vs the plain | compress | compress + decompress |
|---|---|---|---|---|
| hand-rolled LZ77 | 321,112,231 B | 0.564× | 1.172 s | 1.559 s |
| `lz4_flex` (pure Rust) | **318,692,386 B** | **0.560×** | **0.507 s** | **0.614 s** |

`lz4_flex` wins on both size and speed, so it is the default. Both are
pure Rust: **a default build links no C code**. `PINKDB_COMPRESS=lz|lz4`
overrides the choice for a measurement run.

Measured on the same TPC-H SF1 store (7 tables, 7.86M rows), the format
change against the previous v5 container:

| | size | save (encode) | open (decode) |
|---|---|---|---|
| v5 (`PINKDB05`, one CRC over the whole image) | 276,513,123 B | 2.502 s | 2.569 s |
| `PINKDB06` (per-chunk CRC32C, footer, row groups) | **271,584,979 B** | **1.223 s** | **0.167 s** |

(`PINKDB06` is 1.8% smaller, 2.0× faster to save and **15× faster to open**
(the decode now runs over the columns *and* the row groups in parallel);
DuckDB's own Parquet for the same tables is 281 MB.) The compression
measurements: `PINKDB_COMPRESS=lz` gives 277,023,016 B / 1.317 s / 0.203 s.

Durable opens add a sidecar write-ahead log: `PinkDb::open_durable("x.pink")`
(the REPL's `.open`) loads the checkpoint, recovers an interrupted checkpoint,
replays `x.pink.wal`, then logs each committed transaction unit — an
autocommit statement individually, `BEGIN..COMMIT` as one record at `COMMIT`
(a `ROLLBACK` or conflict abort logs nothing). `USE`, `CREATE DATABASE` and
`DROP DATABASE` are logged like any other mutating statement: replay starts in
the checkpoint's session and the logged `USE` records switch the replay
session. Because the WAL is shared by every `Connection` while `USE` is
per-session, a record is made self-contained — when the replay session differs
from the statement's session (after a checkpoint, or with multiple connections
on different databases) a `USE <db>` statement is prepended to the record, so
replay always reproduces each statement in the database it ran in. A `USE`
prefix is never emitted for a database that no longer exists (the `DROP` is
already earlier in the WAL): the record is appended without it, and a
transaction whose write target or `USE` target was dropped since `BEGIN`
fails `COMMIT` with a write conflict instead of logging an unreplayable
record. `SyncPolicy::Sync` fsyncs every record; `Batch` fsyncs on
`flush_wal`/checkpoint.

The WAL format is `PINKWAL1` + version `4`: a generation number, a 16-byte
`wal_id` (a fresh incarnation id per file creation, so a replaced file is
never mistaken for the old one), a header CRC, two 24-byte commit slots and,
per record, the payload's **touched set** — the tables its statements read or
wrote, plus `CREATE`/`DROP DATABASE` names — the substrate cross-process
commit validation compares (`sets_len = u32::MAX` marks a record whose set is
unknown, adopted from an older WAL). Rolled generations are named
`<wal>.<gen>.roll`. Every append publishes
`(seq + 1, last LSN)` into the alternate commit slot — the dual slots plus a
CRC make a torn or lost update harmless — and bumps the epoch in
`<file>.pink.lock`, so another process can detect that something was
published with one `pread` of the lock file's first 48 bytes (two epoch
slots + CRC; wave 45m). Version-2 and version-3 WALs are accepted: a durable open upgrades
them in place under the write lease (records are re-encoded with an unknown
touched set), and older builds refuse a v4 WAL cleanly through the shared
unsupported-version path. Encrypted WALs are `PINKENC1` container version 3
(the record payload gained the touched set, so older builds must refuse the
file; the publish block's CRC domain binds the version) — a fixed 88-byte
plaintext publish block (`wal_id`, CRC, the dual slots) before the
AEAD-sealed records; encrypted checkpoints stay container version 2, so
`.pink` bytes are unchanged; version-1 files stay readable, and a keyed open
rebuilds a WAL at the current version. A checkpoint
writes `.pink.tmp` (fsynced, directory fsynced), renames the live WAL to
`.wal.<gen>.roll` and starts a fresh WAL of generation `gen + 1`, publishes
`.pink.tmp` with an atomic rename, then removes the roll. Recovery never
removes anything before the recovered state is durable:

- `.tmp` present and the live WAL still has records: the live WAL is
  authoritative, `.tmp` (possibly a partial write) is discarded.
- `.tmp` present and the live WAL is empty/absent/torn: the checkpoint
  reached its WAL roll, which proves `.tmp` was fully written, so `.tmp` is
  *published* first and the rolls are removed afterwards.
- no `.tmp`: every roll is already folded into `.pink`; its generation (and
  the live WAL generation) must prove that, otherwise recovery refuses rather
  than delete uncertain records.

Recovery is idempotent, a crash inside recovery re-runs from the same files,
and a torn WAL header (a file no longer than one header) is recreated while
invalid magic/version on a file that carries records is a hard error. A
mid-file framing corruption (short record, bad CRC, or a non-continuous LSN)
stops replay at the first bad record; later committed records are dropped and
the discarded byte count is reported (`RecoveryInfo::truncated_bytes`). A
CRC-valid record that fails to parse, bind, or execute is a hard error —
committed data is never silently dropped.

Durability notes: `.save <bound path>` checkpoints; the target is compared by
filesystem identity, so `./x.pink`, absolute paths and case-only differences
compare equal. `.save <other>` is a plain snapshot and is refused when the
target has a sidecar WAL with committed records. `.import` is made durable by
a checkpoint before it returns (and is refused inside a transaction), so an
import survives a crash immediately after it acks. A mutating statement that
reads a session-local view — or an attached lake table — is refused on durable
databases: neither is persisted, so replaying its logged SQL could not
rebuild it.

Durable databases are **multi-process** (waves 45m/45o): any number of local
processes may open the same database read-write, and writers coordinate on a
short-held per-operation **write lease** — an exclusive `flock` on
`<file>.pink.lock`, held for `[refresh → validate → append → fsync →
publish]` only (explicit transactions no longer hold it for their lifetime;
see the optimistic commits below) — instead of a
lifetime-exclusive lock (the OS releases it on exit or crash, so there is no
stale-lock cleanup). Readers never take the lease: each process keeps a full
in-memory store, checks the lock file's epoch region with one `pread`, and
only when the epoch changed replays the WAL frames it has not applied under
the lease. Refresh runs at statement boundaries — autocommit statements and
before `BEGIN` — never mid-statement; a replaced WAL or checkpoint (changed
`wal_id`, generation or inode) triggers a full rebuild that preserves views
and attachments, and replay runs with the WAL detached so a refresh never
re-logs. Inside one process, handles share the lease through a refcount gate
(`flock` locks are per open file description), and the store lock and WAL
mutex serialize threads exactly as before. `BEGIN` snapshots the committed
state and holds no lease; `COMMIT` takes it only for
`[validate → append → publish]`, while readers never block and never see
uncommitted rows.

While another process holds the lease, an operation waits up to the **busy
timeout** — 5 s by default; the CLI reads `PINKDB_BUSY_TIMEOUT_MS`, the
library exposes `DurableOpenOptions.busy_timeout` /
`Database::set_busy_timeout` — then fails with a clear `database … is busy`
error. An open on a filesystem that reports locking unsupported fails closed
(`LockMode::Required`, the default), with `LockMode::UnsafeSingleProcess` as
the explicit opt-out. This is not a network-filesystem guarantee: there is
no NFS/SMB detection (no libc), so a filesystem that accepts `flock` but
does not honor it across hosts is not caught, and Windows is untested. A
checkpoint refreshes first under the lease — before writing its `.tmp` — so
it can never publish a state that omits another process's committed records.
The lease keeps two writers off one WAL at any instant (two writers would
corrupt its framing and, encrypted, could reuse a `(key, nonce)`): per
operation rather than per open, but still impossible rather than a
convention.

Wave 45n verified these guarantees under chaos: a SIGKILL soak (300 s runs,
hundreds of worker processes) commits exactly-once — every acknowledged unit
survives exactly once — and a deleted or replaced lock file now fails closed
at every lease acquisition, wait and mutation boundary, with the residual
check-then-write window documented in the code.

Wave 45o made cross-process writers **optimistic** (stage 5 of the 45m
design): `BEGIN` captures the current position (WAL `wal_id`, generation,
applied LSN, checkpoint identity) and takes no lease; statements run on that
private snapshot, and `COMMIT` takes the lease only for `[refresh → validate
→ append → publish]`. Validation is table-granularity against the touched
sets every WAL v4 record carries: a frame committed after the base that
touched a table the unit **read or wrote** is a typed `write conflict on
table "db.name" (… lsn N …)` naming the object and the frame's LSN —
read-set validation is also what prevents write skew across processes. The
CLI retries a conflicted transaction up to `PINKDB_TXN_RETRIES` (default 3;
`0` surfaces the first conflict), including a whole one-call
`BEGIN; …; COMMIT;` with the pre-`BEGIN` session restored; the embedded API
returns the conflict. Autocommit writes run optimistically too and fall back
to re-execution under the lease on conflict — exactly-once, with no
user-visible error. A concurrent checkpoint or `.save` snapshot invalidates
an open base, and the transaction fails retryably. If the lease cannot be
acquired after an optimistic execution (busy timeout or a replaced lock
file), the handle fail-stops — the uncommitted effect cannot be undone
without the lease — and a reopen recovers the durable prefix; reads never do
this. `bench/mp_optimistic.py` measures **2.31–2.49×** for overlapping
transactions with disjoint writes (the serialized binary: 0.98–0.99×, no
gain); write-heavy replay-bound workloads stay serial-equivalent because
refresh replays every peer's frames.

The WAL is bounded: `CHECKPOINT` (SQL, any front-end — server, FFI,
bindings; a no-op in memory, refused inside a transaction) persists the store
and rolls the WAL, and a committed write checkpoints automatically once the
WAL passes 64 MiB (`set_auto_checkpoint_bytes`, `None` disables).

A failed WAL append is **fail-stop**: the statement already changed memory
but is not durable, so every later statement — reads included — and every
save is refused until the database is reopened, which recovers exactly the
durable prefix. Nobody can observe or persist the non-durable state.

### Encryption (feature `encryption`)

At-rest encryption is opt-in: `cargo build --features encryption` links the
audited RustCrypto crates `aes-gcm` (AES-256-GCM) and `argon2` (Argon2id); the
default build stays crypto-free and unencrypted files are byte-identical.
`PinkDb::save_with_key` / `open_with_key` write and read `PINKENC1`
containers; `open_durable_with_key` (and the multi-writer
`Database::open_with_key`) apply the same key to the sidecar WAL, and a keyed
store's checkpoints and snapshots encrypt as well.

The container is a plaintext header (magic `PINKENC1`, container version 2,
cipher id, KDF id + parameters + salt, a random per-container nonce prefix,
and an AEAD key-check block) followed by AEAD-sealed payloads: 1 MiB
segments for a checkpoint, one unit per WAL record (with the LSN bound into
the AAD). The container is version 2 since wave 45m (version-1 files stay
readable: checkpoints as-is, WALs rebuilt as v2 on the next keyed open);
WAL containers additionally carry a fixed 88-byte plaintext publish block
(`wal_id`, CRC and the dual commit slots the multi-process epoch refresh
reads) before the records. The
nonce is `prefix || counter`, counters are unique per container. A keyed open
rebuilds the WAL with a fresh random prefix on **every** open (re-sealing the
intact records atomically), not only after a torn-tail truncation: a file cut
exactly at a record boundary — a `Batch`-mode tail lost at a boundary, or a
restored older backup — is byte-identical to a cleanly shorter WAL, so only a
fresh prefix guarantees the discarded record's `(key, nonce, AAD)` is never
handed out again. Checkpoint containers end with a terminal zero-length
segment, so a container cut at a segment boundary is rejected instead of
loading a silently shorter database. Encrypted WAL records always frame
`crc = 0` (GCM authenticates the payload); a nonzero crc on a record whose
ciphertext authenticates is refused as framing tampering. Keys are zeroed on
drop on a best-effort basis (safe writes only, no unsafe).

The key is a raw 32-byte key or a passphrase stretched with Argon2id (defaults
m=19 MiB, t=2, p=1; the salt and parameters live in the header, so raising the
defaults later keeps old files readable). Header parameters are validated on
read against a small multiple of the defaults (m ≤ 76 MiB, t ≤ 4, p ≤ 4, and
`m·t` ≤ 8× the default work), so a crafted header cannot make an open burn
minutes deriving (the worst accepted combination costs ≈ 0.11 s in release
here):

```rust
use pinkdb::{EncryptionKey, PinkDb};

let key = EncryptionKey::from_passphrase("correct horse battery staple");
db.save_with_key("x.pink", &key)?;                       // encrypted snapshot
let db = PinkDb::open_with_key("x.pink", &key)?;         // decrypts
let db = PinkDb::open_durable_with_key("x.pink", &key)?; // encrypted WAL
```

The REPL reads `PINKDB_KEY` from the environment: `.save` writes encrypted and
`.open` decrypts (the durable WAL uses the same key); unset, behavior is
byte-identical to an unkeyed build. When a lake feature is built too (and the
CLI's `encryption` feature is on), the same variable is the lake's at-rest
key: `.lake save`/`open`/`attach`/`commit` seal and unseal `PNKLAKE1` lake
containers (see "Lakes" below). The value must be valid UTF-8: a
set-but-non-UTF-8 variable is a hard error (exit 2) instead of a silent
fallback to plaintext, and remote mode (`--host`) warns that the variable is
inert (the server's own configuration applies; `pinkdb-server` has no key
option, so its `--file` must be a plaintext database). A wrong key (or a
tampered header) fails the key-check block with a clear `wrong key` error,
never "corrupt file". A tampered payload byte fails AEAD authentication: for a
`.pink` checkpoint that is a hard error (loads are all-or-nothing), and for the
WAL a complete record that fails authentication with more records after it is
a hard error too — at the very tail it is indistinguishable from a torn write,
so the wave-22 torn-tail policy applies and the tail is truncated. Encrypted
and plaintext artifacts never mix: a keyed open refuses a plaintext
checkpoint/WAL and a plaintext open refuses an encrypted one, so a key can
never be silently ignored; without the feature, opening an encrypted file
reports `file is encrypted; build with the encryption feature`. A live store's
mode and key are fixed at open: changing `set_encryption_key` afterwards is
refused at the next `checkpoint()` (`cannot change encryption mode on a live
store; reopen it instead`), and `save_snapshot` refuses a target `.pink` or
sidecar WAL in the other mode or under a different key. Nothing but the
header is readable in the clear — payloads (rows, SQL text) are always
ciphertext, covered by tests that grep the files for a known marker string.
Release `pinkdb` binary: 2.21 MB default, 2.28 MB with `--features encryption`
(Apple Silicon, thin LTO; the default build grows ~18 KB over the
pre-encryption 2.19 MB — most of it segment page rounding, ~4.6 KB of code).

Each ≤2048-row column chunk picks its smallest encoding: integers choose
between raw, RLE, bitpacking (min + width + packed bits) and DELTA_BITPACK;
doubles raw, RLE or ALP (bit-exact exceptions); booleans bitpacked or RLE;
strings raw, dictionary (u32 codes), RLE or DICT_BITPACK. NULLs ride in a
separate validity bitmap, and an all-valid chunk sets a flag instead of
writing one. 1M-row sales table: 24.5 MB CSV → **2.68 MB** `.pink`, saved in
0.03 s, reopened in 0.03 s; the same
table at 10M rows is 26.8 MB, saved in 0.29 s and reopened in 0.26 s. 1M-row
text corpora (before → after the compression frame): JSON-like 70.4 → 15.7 MB,
logs 118.7 → 44.5 MB, URLs 103.5 → 41.4 MB. The same parallel encode pass
also collects the exact facts a
lake save publishes (row count, per-column NULL/NaN counts, bit-exact
min/max — wave 45k); the `.pink` bytes are unchanged by it.

## Concurrency

```rust
use pinkdb::{Database, PinkDb};

// One shared store, one connection per thread.
let db = Database::new();
let mut c = db.connect();
c.execute("CREATE TABLE t (a INTEGER);")?;
```

Queries run under a shared read lock (parallel readers, parallel chunks via
rayon inside each query). Writes take the write lock per statement. On a
durable `.pink` database the same statement stream extends across processes
(wave 45m): writers additionally take the file's write lease for the
statement (a transaction holds it from `BEGIN` to `COMMIT`/`ROLLBACK`, so
cross-process transactions serialize on it) and statements refresh from the
WAL at their boundaries — see Persistence.
`BEGIN..COMMIT` works on a private snapshot: commits to disjoint tables all
succeed; overlapping writes abort the second committer with a write-conflict
error (retry the transaction) — never silent lost updates. A transaction that
writes also has its *reads* validated at COMMIT (every table any of its
statements read, subqueries included, must be unchanged), so the WAL — which
replays committed SQL in commit order — always reproduces exactly what the
session computed; read-only transactions never conflict. Table versions are
store-unique, so a table dropped and recreated underneath a transaction is
always a conflict. The plain `PinkDb` API remains for single-connection
embedding.

## Lake layer (features `pinklake` / `parquetlake`)

**A lake is a catalog database plus data files.** The catalog is always one of
three databases:

- `pink:<path>` — a durable pinkdb database file (the default), the analogue
  of DuckDB's `.duckdb` catalog;
- `sqlite:<path>` — a SQLite file (feature `catalog-sqlite`);
- `postgres://…` — a Postgres schema (feature `catalog-postgres`), for lakes
  shared by several machines.

All three store the same DuckLake v1.1-style `ducklake_*` tables. A commit
inserts its delta and one snapshot row (the commit point); readers materialize
the head by joining the snapshot rows. `count(*)` is `SUM(record_count)`, and
`begin_snapshot`/`end_snapshot` give time travel and maintenance without
rewriting a catalog object.

A bare path means `pink:<path>`. Its default data location is
`<path>.files/`; a catalog records its data location, so later opens need only
the catalog. `s3://…` on its own is **not** a lake — pass a catalog and put the
data there:

```text
.lake save pink:lake.pink data s3://bucket/prefix
.lake open pink:lake.pink
.lake save postgres://user@host/db?schema=lake data s3://bucket/prefix
```

**Data-file formats are providers** (wave 87). `.pink` (`PINKDB06`) is always
available; Parquet comes with `parquetlake`. A table
pins its provider, and appends keep it. Append-only commits add immutable data
files (and DuckLake Parquet delete files when rows are removed); merge,
rewrite, expire and cleanup are explicit maintenance commands.

**Statistics live in the catalog.** Attach reads the typed metadata and answers
`COUNT`/`MIN`/`MAX` without opening data; projections and partition predicates
read only the required files/columns.

**Encryption is per file.** A keyed lake seals each data/delete file in a
`PNKLAKE1` container with a random 256-bit key recorded in the catalog, so two
files never share a key and range reads remain possible. A `pink:` catalog can
itself be an engine-encrypted pinkdb database (`PINKDB_KEY`), while SQLite and
Postgres catalogs are protected by their server/filesystem. Metadata rows are
plaintext, like DuckLake's.

**CDC and replication** read the same typed snapshots
(`.lake changes`, `.lake replicate`); replication state and per-table cursors
are typed side tables, not JSON objects.

**No legacy layout.** JSON-layout lakes (`catalog.json`, `snapshots/`,
`commits/`, `highest.json`, `lake.json`) do not open in this build. There is no
converter: re-save such a lake with an older pinkdb build (for example
`.lake open <old>; .lake save pink:new.pink`), then open it with this one.

## Network server

`pinkdb-server` is a separate workspace crate (packaging: the core stays
lean): a TCP server plus a blocking client for a hand-rolled binary protocol.
The default build has no external dependencies, no unsafe, and no async (one
std thread per connection). The optional `async-server` feature adds a
tokio transport (`--transport async`: async accept + frame I/O, engine calls
on the blocking pool) that speaks the identical protocol — it is wave 47's
documented carve-out to the zero-async rule, scoped to the I/O boundary only
(the executor stays synchronous; see the crate docs). Release binaries
(wave 48; Apple Silicon, `--release`, thin LTO, decimal MB): `pinkdb`
3.54 MB (3.60 MB with `--features encryption`), `pinkdb-server` 3.24 MB
(3.67 MB with `--features async-server`). Wave 48 links Argon2id into the
default build for RBAC password hashing — about +0.23 MB per binary (the
`encryption` variants grow less: the same crate already backed the
passphrase KDF there).

```sh
# in-memory server on 127.0.0.1:5433
cargo run --release -p pinkdb-server

# durable server: same checkpoint + WAL guarantees as the embedded path
cargo run --release -p pinkdb-server -- --file sales.pink

# remote REPL: build the CLI with the optional `client` feature, then
cargo build --release -p pinkdb-cli --features client
./target/release/pinkdb --host 127.0.0.1:5433
```

`pinkdb-server --help` lists the flags: `--host` (default `127.0.0.1`),
`--port` (default `5433`; `0` picks an ephemeral port), `--file` (durable
`.pink`: the checkpoint loads and the sidecar WAL replays on restart;
in-memory when omitted), `--max-connections N` (default 128),
`--idle-timeout SECS` (default 300; `0` = never),
`--statement-timeout-ms N` (per-statement engine deadline, default 30000;
checked at chunk boundaries), `--max-result-rows N` (materialized rows per
statement, join output included, default 10 000 000), `--max-result-bytes N`
(estimated materialized bytes per statement, default 268 435 456 = 256 MiB),
`--max-concurrent-logins N` (in-flight Argon2id verifications, default 4),
`--transport sync|async`
(default `sync`; `async` is the tokio I/O transport and needs a build with
the `async-server` feature), `--allow-load-extension` (off by default; see
Security posture), `--allow-copy-to` and `--copy-root PATH`. The last two are
the `COPY … TO` opt-in: off by default (the server refuses it even for
`ADMIN`), and when enabled every destination is confined to `--copy-root`,
written atomically with mode 0600. `--allow-copy-to` without `--copy-root` is
a startup error (exit 2). The four limit flags reject `0`/invalid values at startup
(exit 2); the embedded engine's defaults stay unlimited unless a limit is set
explicitly. TLS is available only with the default `sync` transport:
`--transport async` combined with
`--tls-cert`/`--tls-key` is refused at startup (exit code 2) rather than
silently serving plaintext. The server
prints its listening address on startup and runs until terminated.
`pinkdb-server --health-check [HOST:PORT]` is a one-shot liveness probe:
connect, send the protocol `Ping`, and exit 0 (healthy) or 1 (unhealthy)
within `PINKDB_HEALTHCHECK_TIMEOUT_MS` (default 2000 ms); without an address it
reads `PINKDB_HEALTHCHECK_ADDR`, then `PINKDB_HOST`/`PINKDB_PORT`, then
`127.0.0.1:5433`. It is what the Docker image's `HEALTHCHECK` uses.

Server robustness, in one place:

- **16 MiB stack per connection thread.** The default 2 MiB thread stack sits
  too close to the engine's worst-case recursion; connection threads ask for
  16 MiB and the engine's depth guards (below) reject hostile statements
  before they can recurse.
- **Transient accept errors never stop the server.** `EMFILE`/`ENFILE`/`ECONNABORTED`/
  `ENOBUFS`/... are logged and retried with a 10 ms→1 s backoff, so an fd
  spike no longer kills the process (`ulimit -n` pressure, many short-lived
  clients). Only the nonblocking poll and `EINTR` follow the fast path.
- **128 concurrent connections** (`DEFAULT_MAX_CONNECTIONS`). Over the cap the
  server sends a best-effort `Error` frame and closes, bounding fds/threads.
  The refusal is written **on the accept loop** with a 100 ms write timeout:
  a client that will not read is dropped, no thread is spawned per refusal,
  and a failed connection-thread spawn is logged and the connection closed.
- **Per-statement deadline and result caps.** The server always sets a 30 s
  engine deadline, a 10 000 000-row and a 256 MiB materialized-result cap per
  statement (all configurable; see the flags above). The deadline is checked
  at chunk boundaries inside scans, joins and row loops — including the
  nested-loop join that implements `CROSS JOIN` — so an unbounded Cartesian
  product or a huge `UPDATE` returns a clear error instead of pinning a
  connection (and, for a write, the store's write lock) forever. A timed-out
  `UPDATE`/`DELETE` fails during predicate evaluation, before any write-back,
  so no partial state is committed. The socket idle timeout is a separate
  bound on I/O, not on engine work; the deadline is not fully preemptive and
  cannot interrupt a single in-progress chunk or the final write-back pass.
- **Login resource bounds.** Password verification runs **outside** the store
  lock (the catalog record is copied out first), a fixed dummy Argon2id
  verification runs for unknown users so login timing does not enumerate
  names, and at most `--max-concurrent-logins` (default 4) verifications run
  at once — excess requests get `too many concurrent logins` immediately.
  A connection closes after 5 failed logins with an increasing delay, and a
  bounded per-IP map (4096 entries, 20 failures/60 s → 60 s block, expiry on
  access plus oldest-entry eviction) slows reconnects.
- **Panics are isolated.** A panic while executing one statement is caught,
  logged, and reported as an `Error` frame; the connection and every other
  client stay alive. A panic while the store's write lock is held poisons it;
  the engine logs once and recovers via `PoisonError::into_inner()`, so one
  bad statement cannot permanently DoS the database (the state may be
  inconsistent — recovery is loud).
- **Idle timeout (default 300 s).** A client that sends nothing between
  requests, or stops reading a result, for longer than `--idle-timeout` is
  disconnected, so stalled peers cannot pin the connection cap. Frame bodies
  are buffered as bytes arrive (64 KiB steps), never pre-sized from the
  length prefix, so announcing a 64 MiB frame and stalling costs nothing.
- **Depth guards.** Statements nested deeper than 24 units (parens, `CASE`,
  prefix unary) or with an AST deeper than 64 nodes (left-associative
  operator chains, `IN` lists, `AND`/`OR` runs, nested subqueries) are
  rejected with a clean `Parse` error before bind/execute/drop can recurse.

### Protocol

One TCP connection is one session. Every message is a frame: `u32` little-
endian length prefix + `u8` message type + body. Frames larger than 64 MiB
are rejected before allocation, and every inner length (strings, columns,
rows, values) is validated against the bytes remaining in the frame — a
malformed or truncated peer can never make the decoder read out of bounds or
over-allocate.

Requests: `Hello { protocol_version }` (required first; a mismatched version
is rejected with a clear error), `Execute { sql }`, `Ping`,
`Prepare { sql }` / `ExecutePrepared { handle, values }` (connection-local
prepared statements) and `Login { user, password }` (wave 48). Responses:
`HelloOk { server_version, protocol_version }`, `Pong`, and per statement one
result group — `ResultSet { names, types }`, then chunked `Rows { count,
columns, values... }` frames (typed value encoding: a tag byte + payload for
NULL/BOOL/I32/I64/F64/VARCHAR/DATE/TIMESTAMP; `DataType` tag byte), then
`Done { kind }`. A successful `Execute` batch ends with exactly one
`BatchDone { statements }` frame and the client reads result groups until it,
`Prepared { handle, parameter_count }` answers `Prepare` and `LoggedIn
{ user }` answers `Login`; so neither side parses SQL just to count
statements. A failing statement ends the batch with a single
`Error { message }` instead; SQL errors arrive as `Error` frames and the
connection stays open. The protocol version is **10** (v2 added the
`BatchDone` terminator, v3 `LoadedExtension`, v4 `Checkpointed`, v5
prepared statements, v6 DECIMAL/INTERVAL, v7 `AlteredTable`, v8 `Called`,
v9 the `Login`/`LoggedIn` frames and the RBAC `Done` kinds, v10 query
profiles: `SetProfiling { enabled }` → `ProfilingSet`, after which every
statement's `Done` is followed by one `Profile` frame).

The client (`pinkdb_server::Client`) reconstructs pinkdb's public
`QueryResult`/`Value`/`DataType`, so the CLI renders remote results with the
exact same renderer as local ones. Each connection gets its own
`Connection`, so wave-23 sessions (`USE`, transactions) are isolated per TCP
client. After a decode or transport error the client marks itself broken and
fails fast on later calls instead of reading a desynchronized stream.

### Remote CLI mode

`pinkdb --host HOST:PORT` sends every SQL statement to the server and renders
results with the same box-drawn output and timing line as the embedded REPL.
SQL (including `USE`, `CREATE DATABASE`, `BEGIN`/`COMMIT`) works unchanged.
`--user NAME --password PASS` (or `PINKDB_PASSWORD` in the environment, which
keeps the password out of the process list) logs in first; a server whose
store has users refuses statements until the login succeeded, and a failed
login is fatal rather than a silent fallback to the anonymous client.
The file and catalog dot-commands (`.import`, `.save`, `.open`, `.databases`,
`.tables`, `.schema`) are not available in remote mode and report
`Error: <cmd> is not available in remote mode`. The feature is opt-in: the
default build rejects `--host` with "built without the client feature".
`PINKDB_KEY` is inert in remote mode (the server's own at-rest configuration
applies, and `pinkdb-server` currently has no key option), so a set value is
reported as a warning instead of being silently ignored.

### Security posture

**Users, roles and grants decide what a remote connection may do (wave 48);
the store's catalog is the switch.** Two states, chosen by whether any user
is defined:

- **No users yet** — every connection is trusted and gets the full SQL
  surface of the served database, exactly like the embedded API. The server
  prints a loud startup warning in this state. The bootstrap is a normal
  wire session: `CREATE USER alice PASSWORD '…'` then
  `GRANT ADMIN TO alice`.
- **At least one user** — every statement request needs a successful
  `Login` first (`authentication required: …` otherwise; `Ping` is exempt),
  and each statement then runs under that user's privileges: table
  `SELECT`/`INSERT`/`UPDATE`/`DELETE`, database `CREATE` for schema DDL,
  and the global `ADMIN` (user/role management, `CREATE/DROP DATABASE`,
  `CHECKPOINT`, `LOAD EXTENSION`, `CALL`, and every other check). Roles are
  transitive (`GRANT role TO principal`); `SHOW …`/`DESCRIBE` need no
  privilege. The gate is evaluated per statement, so the connection that
  creates the first user must log in before its next statement.

Passwords are hashed with Argon2id (19 MiB, t=2, p=1) when the DDL runs; the
`.pink` catalog and the WAL only ever hold the PHC hash — the plaintext never
reaches disk. **The embedded API stays trusted**: `Database`/`Connection`
handles, the C ABI and the wasm bindings carry no principal and run every
statement unchecked. `pinkdb-server --transport async` carries the same
posture through the same login path.

Transport encryption is separate and still opt-in: `--tls-cert`/`--tls-key`
(crate feature `tls`) encrypt the connection and let the client verify the
server, but they identify the *server*, not the client. TLS is supported by
the default `sync` transport only; the async transport has no TLS path and
refuses those flags at startup (exit code 2) rather than serving plaintext.
Without a trust anchor, anyone who can open a TCP connection to the port
still reaches the login prompt, so bind `127.0.0.1` (the default) or a
trusted network, and do not expose the port to the internet.

`LOAD EXTENSION` is additionally refused for remote clients unless the
server runs with `--allow-load-extension` (library:
`Server::allow_load_extension(true)`): the statement makes the server process
load and run a native library from its own filesystem, and even with RBAC
that is a host-level capability (an authenticated session also needs
`ADMIN`). WAL replay at startup still re-loads extensions a durable database
already recorded.

`COPY … TO` is treated the same way and more strictly: an authenticated
session needs `ADMIN` (a `SELECT` grant never implies a filesystem write),
and the server refuses it by default (library:
`Server::allow_copy_to(true).copy_root(Some(dir))`). With the opt-in, a
destination is canonicalized under the root, `..`/symlinked
parents/symlinked targets are refused, and the write goes to a `create_new`
temp file (0600) renamed atomically over the target. File/table-function
scans (`read_parquet`, `read_arrow`, `FROM 'file.parquet'`) likewise require
`ADMIN` in an authenticated session; the embedded API stays trusted. See
[`docs/security-remediation-pass6.md`](docs/security-remediation-pass6.md).

### Warehouse browsing UI (wave 78)

`pinkdb-server` can serve a **read-only web console** next to its binary
protocol: a browser UI with the store's databases, relations, paged row
previews and statistics. It is **off by default** and lives behind the
`ui` cargo feature, so the default server binary is unchanged (the console
crate is not even compiled without the feature):

```sh
cargo build --release -p pinkdb-server --features ui
./target/release/pinkdb-server --enable-ui     # console on 127.0.0.1:8080
```

The console is **read-only by construction**: its store access can only run
`SHOW DATABASES`, `SHOW TABLES`, `DESCRIBE`, `SELECT COUNT(*)` and
`SELECT … LIMIT … OFFSET …`. There is no DML and no DDL endpoint, and the
router has no `POST`/`PUT`/`DELETE` route at all. Every read runs under the
engine's own pass-5 limits (a 30 s statement deadline, a 1 000-row
materialized-result cap, a 16 MiB byte cap) and releases the store lock
before any HTTP work happens.

**Exposure policy (fail closed).** The console binds `127.0.0.1` by default
and a **non-loopback bind is refused** (exit 2): the console is a browser UI
and cannot use the server's TLS transport, so a non-localhost exposure needs
console TLS (wave 46) *and* RBAC (wave 48) first. A store that **has users**
is never served unauthenticated: the server refuses to start unless
`--ui-auth basic` (HTTP Basic, verified through the engine's own Argon2id
path — the console's statements then run under that user's grants) or
`--ui-token <secret>` (a constant-time bearer token) is given. Serving a
store *without* users prints a loud warning: every request can read the
whole store.

**Endpoints** (all `GET`/`HEAD`; anything else is `405`, an unknown path is
`404`): `/` (the console shell), `/preview?db=&table=&page=` (a
server-rendered row preview — the no-JavaScript fallback), `/assets/console.css`
and `/assets/console.js` (embedded in the binary — no CDN, no external font
or script), and the JSON API `/api/databases`, `/api/schemas?db=`,
`/api/tables?db=&schema=`, `/api/rows?db=&table=&page=&page_size=` and
`/api/stats?db=` (per-table row counts and size estimates). Paging is
bounded (`--ui-max-page-size`, default 500) and `total_rows` is exact.

The preview text **mirrors the CLI's box renderer** (`NULL` renders as
`NULL`, a `DOUBLE` prints `1.0`, a `VARCHAR` never carries a raw control
character) and untrusted cell content is never rendered as markup: the HTML
is generated by `maud` (which escapes every interpolated value) and the
script inserts cells with `textContent`.

The console is the workspace's **second async carve-out** (after wave 47's
`async-server` transport), scoped to the I/O boundary: it owns a tokio
runtime on its own thread (so it works with either transport), and every
synchronous engine call runs through `spawn_blocking` on a **bounded blocking
pool** — the async runtime never executes engine code. See
[`docs/server-ui.md`](docs/server-ui.md) for the full policy, the endpoint
list and the known limits (no query console yet).

## C ABI (`pinkdb-ffi`)

The `pinkdb-ffi` crate exposes the engine over a small C ABI as
`libpinkdb_ffi.dylib` / `libpinkdb_ffi.a` for foreign bindings (Python `cffi`,
Go `cgo`, ...). It is the only crate in the repo that contains `unsafe`; the
core stays zero-unsafe.

```sh
cargo build --release -p pinkdb-ffi
make -C crates/pinkdb-ffi hello   # compiles and runs the C example
```

- Header: `crates/pinkdb-ffi/include/pinkdb.h` (ABI v1; `pinkdb_abi_version()`).
- Handles (`pinkdb_open` for durable files, `pinkdb_open_memory`) are not
  thread-safe — use one per thread. Errors are thread-local
  (`pinkdb_last_error`).
- `pinkdb_execute(handle, sql, len)` takes UTF-8 bytes (embedded NULs allowed)
  and returns a result that round-trips every statement kind and value type:
  typed `pink_value` by value, with VARCHAR text borrowed from the result.
- Ownership: free results with `pinkdb_result_free`, handles with
  `pinkdb_close`; the full contract (pointers, threading, text lifetimes) is in
  the header.

Artifacts: `libpinkdb_ffi.dylib` ≈ 1.70 MB, `libpinkdb_ffi.a` ≈ 15.5 MB (build
archive); nothing is added to the `pinkdb` binary.

## Extensions (loadable)

`pinkdb` can load shared libraries that register scalar SQL functions at
runtime — no recompilation of the engine. This is the repo's second versioned
C ABI: it is **separate** from the `pinkdb-ffi` embedding ABI above (which is
host→foreign-binding); an extension is a foreign library→host callback ABI.

- Header: `crates/pinkdb-ext-loader/include/pinkdb_extension.h` (ABI v1;
  plain C99, no pinkdb libraries needed to build an extension).
- Loader crate: `pinkdb-ext-loader` (owns every `unsafe` here: `dlopen`,
  symbol lookup, callback invocation, result-text ownership).
- Engine API: `PinkDb::load_extension(path) -> name`; the SQL statement is
  `LOAD EXTENSION '<path>'` (string-literal path). Loaded functions resolve
  case-insensitively like built-ins and cannot shadow one.

First-party **core extensions** register through the same registry (linked
registration, no `dlopen`). `json` (`json_extract`, `json_value`, `json_valid`,
`json_array_length`, `json_object`, `json_array` over JSON text in `VARCHAR`),
`datetime` (`strftime`, `strptime`, `epoch`, `epoch_ms`, `dayofweek`,
`dayofyear`, `week`, `last_day`, `age`, `make_date` over naive
`DATE`/`TIMESTAMP`, pinned to DuckDB 2.0 α semantics — `extensions/datetime`
carries the full matrix and divergences) and `math` (`exp`/`ln`/`log`/`log2`/
`log10`, the trigonometric family, `degrees`/`radians`, `pi` and the volatile
`random()`) are CLI **default features**; `--no-default-features` keeps the
binary lean. `string` ships as an in-repo loadable extension
(`extensions/string`: `make -C extensions/string`, then
`LOAD EXTENSION '<path>'`).

Two core extensions are **file formats** rather than scalar functions: the
scalar ABI (one callback per row) cannot express a replacement scan, a table
function, or a COPY format. The engine therefore owns one **table-source**
read surface (`pinkdb::table_source::TableSource`: a bind-time schema, chunk
scans that receive a projection and pushdown predicates, and the
process-global registration a durable replay needs) and one **COPY sink**
surface (`pinkdb::filefmt::FileFormat`). Both extensions are engine-adjacent
crates implementing those traits over the arrow-rs crates — the same shape as
`pink-lake` implementing `AttachedLake`:

`pinkdb-arrow` also publishes that surface as an **extension package** (wave
86): `pinkdb_arrow::register_package()` registers the package process-wide,
and a store gets the Arrow reader/writer only after
`LOAD EXTENSION 'arrow'` (or `PinkDb::load_extension_package`). That is the
third registration model, next to `register(&mut db)` (one store) and
`ensure_registered()` (every store, from startup); pick one per process,
because a package whose source or format name is already registered globally
is refused at load, atomically, leaving the store unchanged. Register the
package before `open_durable` so durable replay can resolve it, activation is
per store, and packages are plain Rust, so they work on wasm32 too.

| Extension | Crate | Read | Write |
|---|---|---|---|
| `arrow` | `extensions/arrow` (`pinkdb-arrow`) | `FROM 'f.arrow'` / `read_arrow('f.arrow')` (`read_ipc`/`read_feather`; `.arrow`/`.ipc`/`.feather`/`.arrows`) | `COPY t TO 'f.arrow' (FORMAT arrow)` |
| `parquet` | `extensions/parquet` (`pinkdb-parquet`) | `read_parquet('glob')` / `parquet_scan`, `FROM 'f.parquet'`, `parquet_schema`/`parquet_metadata`/`parquet_file_metadata`/`parquet_kv_metadata`; globs + `union_by_name`, hive partitions, `filename`, projection + row-group-statistics pushdown | — (follow-up) |

```sql
SELECT * FROM 'data.arrow';                        -- replacement scan (.arrow/.ipc/.feather)
SELECT * FROM read_arrow('data.arrow');            -- table function (read_ipc / read_feather)
COPY t TO 'out.arrow' (FORMAT arrow);              -- write (FORMAT inferred from the extension)
SELECT * FROM read_parquet('data/*.parquet') WHERE id > 10;
SELECT * FROM 'data/people.parquet';
SELECT * FROM parquet_metadata('data/people.parquet');
```

Arrow: IPC **file** format (Feather v2) only — the IPC *stream* format is
detected by its magic and refused with a message naming the difference; reads
accept uncompressed/LZ4/ZSTD files, writes are uncompressed; one record batch
per engine chunk (≤2048 rows) on write, any batch layout on read; a scan
materializes the whole file (no projection/streaming yet); every type without
an exact engine counterpart (BLOB, TIME, nested types, `UInt64`,
`Decimal256`, …) is refused at bind time naming the column. The Arrow **C
Data Interface** in `pinkdb-ffi` is a documented follow-up with a precise pin
in `extensions/arrow/README.md` (not half-implemented).

Parquet: read-only for now (`COPY … (FORMAT parquet)` is a follow-up);
DuckDB-α-pinned semantics for globs and directories, multi-file reads,
`union_by_name`, hive-partition columns, `filename`, `file_row_number`,
`allow_empty` and `hive_types_autocast`; min/max/NULL-count row-group
statistics prune scans (visible in `EXPLAIN`'s `filters=[…]` and
`.parquet`'s counters); flat schemas only, and TIMESTAMP(nanos)/TIME/UUID/
FLOAT16/BSON/VARIANT/geo/nested columns are refused by name. Add
`,parquet-zstd` for zstd-compressed files (compiles the C zstd library).

Both are **opt-in** CLI features (`cargo build -p pinkdb-cli --features
arrow,parquet`; sizes in the builds table above): the default binary stays
lean and refuses every file surface with a clear error. A durable store
replays a logged statement that reads a file by **re-reading** the file
(DuckDB's rule; the host registers the sources process-wide before any store
is opened, which is what makes that replay resolvable). A file deleted
before replay is a hard recovery error, never a silent drop; a file
*changed* before replay replays its new content — the documented caveat of
the re-read policy, matching DuckDB.

### Remote files without a lake (feature `httpfs`, wave 75)

`httpfs` is the **shared remote transport**; each registered file format
claims its own extensions and reads through it:

```sql
SELECT * FROM read_csv('https://example.com/sales.csv');
SELECT * FROM read_csv('s3://bucket/data/sales.csv');
SELECT * FROM 'https://example.com/sales.csv';   -- replacement scan (.csv)

SELECT * FROM read_parquet('https://example.com/data.parquet');
SELECT * FROM 's3://bucket/data.parquet';        -- ranged footer/row groups

SELECT * FROM read_arrow('https://example.com/data.arrow');
SELECT * FROM 's3://bucket/data.arrow';          -- whole object (IPC has no ranges)

SELECT * FROM 'data/sales.csv';                  -- local .csv, same source
```

```text
.import https://example.com/sales.csv
.import s3://bucket/data/sales.csv import_name
```

- **Claim rules**: claims are by **extension regardless of scheme** — `.csv`
  goes to the CSV source, `.parquet`/`.parq`/`.pq` to parquet,
  `.arrow`/`.ipc`/`.feather`/`.arrows` to arrow. The CSV source still claims
  only `.csv` and never another format's path; parquet and arrow claim
  `http://` too (the transport refuses plain http unless
  `PINKDB_HTTPFS_ALLOW_HTTP=1`). A remote path's query/fragment is ignored
  for the extension decision and kept for the transport.
- **Parser**: exactly the engine's `.import` parser — comma-separated,
  `"`-quoting with `""` escapes, one header row, UTF-8, empty fields as
  NULL, and the same whole-file type inference (INTEGER → BIGINT → DOUBLE →
  BOOLEAN → DATE → TIMESTAMP → VARCHAR). `read_csv` and `.import` of the same
  bytes produce the same schema and rows. The wave-76 option surface
  (`delimiter`, `quote`, `encoding`, `header`, …) is parsed-but-refused today
  with a precise message, never ignored.
- **Transport**: one shared crate, `pinkdb-httpfs`, serves both this feature
  and the lake's `s3` feature. It supports whole-object GET and **byte-range
  GET** (`Range: bytes=…`) on `https://` and `s3://`, plus HEAD size, through
  a small `Fetcher` trait (`size`/`fetch`/`fetch_range`). Parquet implements
  its `ChunkReader` over `fetch_range`, so **footers and row groups are
  ranged reads** (a whole-object GET happens only when the backend reports no
  size); Arrow IPC has no ranged protocol, so it fetches the whole object.
  The S3 half is the extracted SigV4 client: the same `AWS_*` /
  `PINKDB_S3_*` environment variables, endpoint addressing, conditional
  writes, retry/backoff and timeouts as before — pink-lake's behavior did not
  change, its tests and the S3 CI job still cover it.
- **Remote reads (wave 98a)**: a remote Parquet scan computes the projected
  column chunks' byte ranges from the footer, coalesces adjacent chunks (and
  gaps up to 1 MiB, capped at a 64 MiB request) into one range GET, and
  fetches the next row groups with a bounded pool while earlier ones decode.
  Results are assembled in row-group order, so values and row order are
  identical to a local read. `PINKDB_HTTPFS_PARALLELISM` sets the in-flight
  limit (default 8, clamped to 16); `0`/`1` restores the legacy **serial**
  path (one lazy 128 KiB range at a time). One read-ahead window buffers at
  most 128 MiB. A scan whose projection needs no file column — `COUNT(*)` —
  is answered from the footer's row counts with **zero data-page requests**,
  for local files too. Every transport call a scan makes is counted
  (requests, bytes, wall time); the parquet source exposes the session
  totals through `ParquetSource::remote_stats()`. `EXPLAIN` shows the pushed
  width (`(4 of 16 columns)`). The transport keeps at least the in-flight
  limit of idle connections per host, so requests reuse one `ureq::Agent`
  pool instead of setting up TLS per range.
- **Credentials**: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
  `AWS_SESSION_TOKEN`; `AWS_REGION` or `AWS_DEFAULT_REGION`; a custom
  endpoint via `AWS_ENDPOINT_URL_S3` or `AWS_ENDPOINT_URL` (R2 uses
  `region=auto`); `AWS_S3_FORCE_PATH_STYLE`; `PINKDB_S3_RETRIES`;
  `PINKDB_S3_ALLOW_HTTP` for a local test endpoint. `https://` is the default;
  a plain `http://` URL needs `PINKDB_HTTPFS_ALLOW_HTTP=1` (tests only).
- **Limits**: one object per read — no remote globs/directories, no gzip,
  and no `COPY TO s3://` (documented follow-ups). A remote read is re-read by
  the next statement (the re-read policy); a durable store replays a logged
  URL read by re-fetching it, and a missing object is a hard recovery error.
- **Size**: opt-in `httpfs` is 5,811,760 B (+1.95 MiB over the 3,762,736 B
  default); the default binary stays transport-free. The `pinklake,s3` build
  shares the same client and re-measures at 7,116,144 B; `parquet,httpfs` is
  6,998,912 B, `arrow,httpfs` 9,018,112 B, and `arrow,parquet,httpfs`
  10,187,840 B.
- **Cross-check**: DuckDB 2.0 α's `httpfs` extension is installed here, so
  `bench/duckdiff.py` compares remote **parquet** and **CSV** over the same
  local HTTP server (6 probes). DuckDB has no Arrow file reader, so remote
  Arrow is value-pinned in `extensions/arrow/tests/remote.rs`. duckdiff total:
  **328/328** (322 before the remote battery).

#### Remote format matrix

| Format | Schemes | Read strategy | Limits |
|---|---|---|---|
| CSV (`httpfs`) | `https://`, `s3://` (claim); `http://` explicit-only | whole-object `fetch()`; parser reuses `.import` | single object; default dialect only |
| Parquet (`parquet/remote`) | `https://`, `s3://`, `http://` (claim; transport gate) | **ranged** footer + projected column-chunk ranges (one request per coalesced run, bounded read-ahead; serial 128 KiB blocks when `PINKDB_HTTPFS_PARALLELISM=0/1`); whole-object only when HEAD reports no size; `COUNT(*)` from footer row counts | single object; no remote globs/dirs |
| Arrow IPC (`arrow/remote`) | `https://`, `s3://`, `http://` (claim; transport gate) | **whole-object** `fetch()` (IPC has no ranged footer protocol); one fetch per bind→scan | single object; no remote globs/dirs |

A minimal extension registering one `hello_add(BIGINT, BIGINT) -> BIGINT`:

```c
/* hello_ext.c */
#include "pinkdb_extension.h"

static int32_t hello_add(void *user_data, const pinkdb_ext_value *args,
                         size_t n_args, pinkdb_ext_value *out) {
    (void)user_data; (void)n_args;
    if (args[0].is_null || args[1].is_null) {   /* NULL in → NULL out */
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_BIGINT;
    out->is_null = 0;
    out->int64 = args[0].int64 + args[1].int64;
    return 0;
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    static const int32_t arg_types[2] = {
        PINKDB_EXT_TYPE_BIGINT, PINKDB_EXT_TYPE_BIGINT
    };
    return api->register_scalar(host_ctx, "hello_add", 2, arg_types,
                                PINKDB_EXT_TYPE_BIGINT, hello_add, NULL);
}

uint32_t    pinkdb_extension_abi_version(void) { return PINKDB_EXT_ABI_VERSION; }
const char *pinkdb_extension_name(void)        { return "hello_ext"; }
```

```sh
cc -std=c99 -O2 -fPIC -shared -I crates/pinkdb-ext-loader/include \
   -o hello_ext.dylib hello_ext.c          # .so on Linux, .dll with -shared on Windows
./target/release/pinkdb
```

```sql
LOAD EXTENSION 'hello_ext.dylib';
-- Extension hello_ext loaded.
SELECT hello_add(40, 2);                   -- 42
```

Contract highlights (the header is complete):

- **Thread safety:** callbacks run concurrently on the executor's rayon
  workers — a registered function may be invoked on several threads at once,
  so its state must be guarded (atomics/locks/TLS). `alloc`/`free` are
  thread-safe; `register_scalar` is only valid inside `pinkdb_extension_init`.
- **Typing:** ABI v1 is strictly typed. Arguments must match the registered
  types exactly (`NULL` literals are typeless and pass through as NULL; write
  `CAST(...)` for a different numeric type) and the result must carry the
  registered type tag.
- **Result text:** a non-NULL VARCHAR result must be allocated with
  `api->alloc(size)`; the host copies `text_len` bytes and frees it with
  `api->free` immediately after the call — on every path, including error
  returns and NULL results. A rejected registration (including a NULL `fn`)
  fails the whole load atomically — nothing half-registers.
- **Errors:** returning non-zero aborts the enclosing statement with an error
  naming the function and code; `init` returning non-zero (or a rejected
  registration) fails the load naming the code/reason. `init` and callbacks
  must not panic/unwind across the ABI boundary.
- **Determinism (wave 51):** a function is deterministic (a pure function of
  its arguments) unless it was registered with the additive
  `PINKDB_EXT_FLAG_VOLATILE` bit through `api->register_scalar_flags`.
  Deterministic functions may be called in any statement — the engine WAL-logs
  the SQL and replays it after recovery. A *volatile* call is refused in any
  statement the engine WAL-logs (`INSERT`/`UPDATE`/`DELETE`/DDL/`CALL`), with a
  clear error before it runs, because replaying logged SQL could otherwise
  produce a different value; pure reads may still call it. `register_scalar`
  is exactly flags 0, so every pre-wave-51 extension keeps today's semantics.

### Extensions on wasm (linked registry)

A wasm sandbox has no dynamic linker and no filesystem, so ABI v1 cannot be
loaded there. Wave 49 adds a second, wasm-compatible model: the extension is
**linked into the host** and published in a process-global registry through a
safe Rust API, and `LOAD EXTENSION '<name>'` resolves that name instead of a
path (native targets keep the `dlopen` model; the registry is consulted there
only when nothing is loadable at the given string — which is how a replayed
WAL record naming a linked extension resolves).

```rust
use pinkdb_ext_loader::linked::{self, LinkedExtension, LinkedFunction};
use pinkdb_ext_loader::{ExtArg, ExtType, ExtValue};

let ext = LinkedExtension::new("windowed_math", vec![
    LinkedFunction::new("wma_halve", vec![ExtType::Double], ExtType::Double,
        |args: &[ExtArg<'_>]| match args[0] {
            ExtArg::Double(x) => Ok(ExtValue::Double(x / 2.0)),
            ExtArg::Null(_) => Ok(ExtValue::Null),
            other => Err(format!("unexpected argument {other:?}")),
        })?,
])?;
linked::register(ext)?;                 // host bootstrap (wasm-bindgen start fn)
// LOAD EXTENSION 'windowed_math'; SELECT wma_halve(5.0);   -- 2.5
```

The same value model, arity limit, binder, executor and WAL rules as the C
ABI (see `crates/pinkdb-ext-loader/src/linked.rs`); callbacks are plain Rust
(`Send + Sync`, panics caught at the boundary), metadata is validated once,
and registration is one-shot — SQL can activate what the host published but
can never inject code. No `dlopen`-less dependency was added to the wasm
graph: `libloading` stays target-gated in the loader crate. The JavaScript
(wasm-bindgen) surface — `pinkdb.registerExtension` / `pinkdb.listExtensions`
with its value mapping and determinism rules — is shipped as a pinned
contract; the binding itself is the recorded follow-up. Full design, limits
and test matrix: `docs/wasm-extensions.md`.

### Writing extensions (C/C++)

The in-repo C/C++ SDK (`ext-sdk/c/`) bundles the vendored ABI header, a
commented skeleton (`ext-sdk/c/template/`), three worked examples (C numeric,
C string, C++), a Makefile, and an end-to-end smoke test:

```sh
make -C ext-sdk/c        # builds the examples into ext-sdk/c/build/
./ext-sdk/c/smoke.sh     # builds them, loads each via the release CLI, greps the results
```

`ext-sdk/c/README.md` walks through the contract (exports, the host API
struct, value tags, VARCHAR `alloc`/`free` ownership, NULL semantics,
thread-safety, no unwinding) and the `LOAD EXTENSION '<path>'` workflow. The
SDK's header is kept byte-identical to the loader crate's copy by a
`cargo test` check.

A Go SDK (`ext-sdk/go/`) builds the same ABI v1 with cgo
(`go build -buildmode=c-shared`): the three exports come from `//export`
directives, and small static C shims stand in for the host's function
pointers (`register_scalar`/`alloc`/`free`), which Go cannot call directly.
`ext-sdk/go/README.md` documents the cgo patterns and the sharp edges — the
Go runtime is linked into the host process, callbacks are invoked from
foreign rayon worker threads, VARCHAR results must be copied into
host-`alloc` memory, panics must be recovered before they cross the ABI
boundary, and `-buildmode=c-shared` symbol-export rules — and
`./ext-sdk/go/smoke.sh` builds and exercises the example end to end. Its
header copy is pinned by the same `cargo test` check.

### Writing extensions (Rust)

Wave 51 adds a Rust SDK (`ext-sdk/rust/`, crate `pinkdb-ext-sdk`): one
`pinkdb_extension!` declaration defines the extension's name and its
functions, and the SDK generates the ABI glue — value marshalling, VARCHAR
ownership, NULL handling — from typed Rust bodies. Extension authors write no
`unsafe` and no C:

```rust
use pinkdb_ext_sdk::pinkdb_extension;

pinkdb_extension! {
    name = "my_extension",
    functions = [
        fn my_ext_greet(name: Option<&str>) -> Result<Option<String>, String> {
            Ok(name.map(|n| format!("Hello, {n}!")))
        },
    ],
    volatile = [
        fn my_ext_now_micros() -> Result<i64, String> { /* read a clock */ },
    ],
}

// Only a cdylib (a shared library loaded with LOAD EXTENSION '<path>') emits
// the three ABI v1 symbols; a linked-only extension must not (see below).
pinkdb_ext_sdk::pinkdb_abi_exports!();
```

Parameters and returns are the ABI v1 types (`bool`, `i32`, `i64`, `f64`,
`&str`/`String`, `Date`, `Timestamp`), `Option<T>` makes NULL explicit, and
`Result<_, String>` returns errors. The same declaration drives both loading
models: wave 49's linked registration (`pinkdb_register_linked()`, the only
model on wasm, and how the CLI's built-in `json`/`datetime`/`math` extensions
reach the binary) and — for a cdylib — the native ABI v1 exports, emitted by
the **separate, explicit** `pinkdb_abi_exports!()` call. The split is
deliberate: a linked-only extension is an rlib the host links in, and two
rlibs carrying `#[no_mangle]` `pinkdb_extension_*` symbols are a
`duplicate symbol: pinkdb_extension_init` link error on Linux. A crate that
builds both an rlib and a cdylib (like `extensions/string`) gates the call
behind an `abi-exports` Cargo feature that only the shared-library build
enables; `./ext-sdk/rust/check-native-exports.sh` asserts the linked-only
rlibs carry none. The SDK depends on the loader crate, never the other way
around: no pinkdb binary grows because of it.

The `volatile` list is the additive ABI v1 deterministic/volatile flag
surfaced safely: **functions are deterministic by default** — the engine may
WAL-log statements that call them and replay them — and a function declared
volatile (a clock, a random source, ambient state) is **refused in any
statement the engine WAL-logs** (`INSERT`/`UPDATE`/`DELETE`/DDL/`CALL`), with
a clear error before it runs, because replaying logged SQL could otherwise
produce a different value. Pure reads may call volatile functions freely.
Native C extensions get the same rule through the additive
`api->register_scalar_flags` entry point and `PINKDB_EXT_FLAG_VOLATILE` (the
pre-flag `register_scalar` is exactly flags 0, so existing extensions keep
today's semantics byte-for-byte); the linked model carries it as
`LinkedFunction::volatile()`.

```sh
make -C ext-sdk/rust        # builds the example into target/release/
./ext-sdk/rust/smoke.sh     # builds it, loads it via the release CLI, greps 15 checks
./ext-sdk/rust/check-native-exports.sh   # linked-only rlibs carry no ABI symbols
```

`ext-sdk/rust/README.md` walks through the macros, the type/`Option`/`Result`
rules, the volatility contract, the error-code mapping (the C ABI carries no
error text) and the crate's own round-trip tests (native `LOAD EXTENSION`,
linked registration, durable replay); its header copy is pinned by the same
`cargo test` check.

### Extension repository

Built extensions are published to a hosted catalog (Cloudflare Worker + R2,
format v1) at <https://pinkdb-ext-repo.defaultcms.workers.dev>: `GET
/index.json` is the catalog, `GET /index.json.sig` its detached Ed25519
signature, and `GET /artifacts/…` serves the bytes. The
`ext-repo/` directory holds the format spec (`ext-repo/FORMAT.md`), the
publisher (`publish.sh`/`publish.py`), the verifier (`verify.sh`), and the
worker source. Catalog authenticity is an Ed25519 detached signature over the
exact `index.json` bytes (`index.json.sig`), verified by `.install` against a
key pinned in the binary (see `docs/extension-signing.md`). The first published
artifact is the C SDK example `math_ext` 0.1.0 (`darwin-arm64`).

#### Installing extensions (`.install`)

The CLI can install extensions from repositories. The HTTP/TLS/JSON/SHA-256
client is an opt-in feature so the default binary stays lean:

```sh
cargo build -p pinkdb-cli --features repo     # the `repo` feature
```

Then, inside the REPL:

```text
pinkdb> .install math_ext
Installed math_ext 0.1.0 (darwin-arm64)
  repository: https://pinkdb-ext-repo.defaultcms.workers.dev
  path:       /Users/you/.pinkdb/extensions/math_ext/0.1.0/darwin-arm64/math_ext.dylib
  sha256:     38abc8b345466e0710bf9c2497a99b0b5c0a3e906874cae1c3323ee3e754b9b2
  signature:  verified (key c84e54bfea6b2d09)
Extension math_ext loaded.
pinkdb> SELECT ext_clamp(CAST(42 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT));
```

- **Config:** `$PINKDB_HOME/repositories.json` (default
  `~/.pinkdb/repositories.json`) is a JSON object with an ordered list,
  `{"repositories": ["https://…"]}`. When the file is absent the official
  repository above is the default; the first mutation materializes it.
  `.repositories list` / `.repositories add <url>` /
  `.repositories remove <url>` manage it: URLs are validated and
  canonicalized (trailing slashes stripped; plain `http://` only for loopback
  hosts unless `PINKDB_REPO_ALLOW_HTTP=1`; `https://` repositories never follow
  a redirect to `http://`, and `http://` ones follow no redirects),
  duplicates are refused, order is preserved, and an empty list is allowed
  (`.install` then says what to add). Writes are atomic (temp + rename).
- **Resolution:** repositories are tried in config order and the first one
  with a match wins. `.install <name>` picks the highest semver-ish version
  for this platform — derived at runtime from the OS and architecture, e.g.
  `darwin-arm64` — with ABI v1; `.install <name>@<version>` is exact.
- **Cache + integrity:** artifacts land under
  `$PINKDB_HOME/extensions/<name>/<version>/<platform>/<file>`, are written
  atomically (temp file + rename), and are verified against the catalog's
  SHA-256 before the rename; a mismatch is refused and leaves nothing behind.
  Installing the same coordinates again reuses the verified copy (no
  re-download). `.installed` lists the cache.
- **Catalog authenticity:** before any artifact is requested, `.install` fetches
  `index.json.sig` and verifies the Ed25519 signature over the exact catalog
  bytes against the key embedded in the binary
  (`ext-repo/keys/extension-signing.pub.pem`). Missing/malformed/unknown-key/
  invalid signatures are fatal; there is no unsigned fallback. The dev-only
  overrides `PINKDB_EXT_REPO_PUBKEY` (replace the pin) and
  `PINKDB_EXT_ALLOW_UNSIGNED=1` (skip verification) print loud warnings and are
  **insecure**. See `docs/extension-signing.md`.
- **Load:** `.install` finishes by staging the verified file in a private
  `0700` temp directory with `create_new`, re-hashing the staged copy, then
  `LOAD EXTENSION`-ing that path into the current session; the extension then
  behaves exactly as described below (process-level, WAL replay re-loads it by
  path).

Semantics and persistence:

- Extensions are **process-level**: they are not stored in `.pink` snapshots
  and not part of a transaction. `LOAD EXTENSION` is refused inside
  `BEGIN..COMMIT` (`LOAD EXTENSION is not allowed inside a transaction`), like
  `CREATE DATABASE`. An accepted extension stays loaded (its library stays
  mapped) for the lifetime of the store — even one that registers no functions
  — and its declared name is pinned: loading it again is a clear
  `extension "x" is already loaded` error (a failed load registers nothing).
- For a **durable** database the statement *is* WAL-logged, and every logged
  record that *calls* an extension function is prefixed with
  `LOAD EXTENSION '<path>'` unless the current WAL generation already loaded
  that path (touched-only prefixing; the logged `LOAD EXTENSION` itself is
  never prefixed with itself). Replay therefore re-loads exactly the
  extensions the replayed records need. A missing/unloadable file at replay
  time is a hard recovery error naming the path (never a silent drop); prefer
  absolute paths, since the recorded path is re-resolved against the
  process's working directory at reopen.
- A **checkpoint** rolls the WAL and never writes an extension into the
  `.pink` (extensions are process-level). An open after a checkpoint starts
  with no extension loaded; the first record that touches one re-loads it by
  path, so its file must still exist. An extension loaded before the last
  checkpoint is gone after a reopen until it is loaded again.
- **Remote** use (`pinkdb-server`): `LOAD EXTENSION` loads native code into
  the *server* process, not the client. The server trusts its clients, so
  treat remote access as a trusted-clients posture — anyone who can send SQL
  can load a shared library with the server's privileges.
- **ABI policy:** the extension ABI version bumps only for breaking changes.
  New result/statement kinds (e.g. `LOAD EXTENSION` itself, which added
  `PINKDB_STMT_LOADED_EXTENSION = 14` to the `pinkdb-ffi` embedding ABI and a
  `loaded_extension` kind to the Python/Go bindings) are additive and keep
  ABI v1 — existing extensions and embedders keep working unchanged.
- wasm32 builds do not link the loader: `LOAD EXTENSION` fails cleanly with
  `extensions are not supported on wasm targets`.

```sh
cargo test -p pinkdb-ext-loader   # compiles the C test extensions with cc
```

## Python binding (`bindings/python`)

A thin, dependency-free wrapper over the C ABI, implemented with the standard
library `ctypes` module (no `cffi`, no pip installs, no build step for the
package itself — the roadmap's `cffi` wording is a deliberate deviation here;
the binding model is the same). Build the shared library first:

```sh
cargo build --release -p pinkdb-ffi
```

```python
import pinkdb

with pinkdb.connect_memory() as db:            # or pinkdb.connect("data.pink")
    db.execute("CREATE TABLE t (x INTEGER, s VARCHAR);")
    db.execute("INSERT INTO t VALUES (1, 'héllo'), (NULL, NULL);")
    result = db.execute("SELECT x, s FROM t ORDER BY x;")[0]
    print(result.kind, result.rows)  # selected [[1, 'héllo'], [None, None]]
```

`Database.execute(sql)` returns one `Result` per `;`-separated statement:

- `kind`: `selected`, `created`, `inserted`, `updated`, `deleted`, `began`,
  `committed`, `rolledback`, `created_view`, `dropped_table`, `dropped_view`,
  `created_database`, `dropped_database`, `used`, `loaded_extension`;
- `columns` / `column_types` (SELECT only; types `boolean`, `integer`,
  `bigint`, `double`, `varchar`, `date`, `timestamp`), `rows`;
- `affected` (INSERT/UPDATE/DELETE) and `name` (view/table/database/`USE`/
  extension).

Values map to `None`/`bool`/`int`/`float`/`str`/`datetime.date`/naive-UTC
`datetime.datetime`; errors raise `pinkdb.Error` with the message from
`pinkdb_last_error` and leave the handle usable. The library is located via
`PINKDB_FFI_LIB` (full dylib/so path) or, by default, the repo-relative
`target/release/libpinkdb_ffi.{dylib,so}`.

```sh
python3 -m unittest discover -s bindings/python/tests -v
PINKDB_FFI_LIB="$PWD/target/release/libpinkdb_ffi.dylib" \
    python3 -m unittest discover -s bindings/python/tests -v   # env override
```

## Go binding (`bindings/go`)

A cgo wrapper over the same C ABI v1 (module
`github.com/openlearnia/pinkdb/bindings/go`). Build the shared library first:

```sh
cargo build --release -p pinkdb-ffi
```

```go
import pinkdb "github.com/openlearnia/pinkdb/bindings/go"

db, err := pinkdb.OpenMemory() // or pinkdb.Open("data.pink")
if err != nil {
	panic(err)
}
defer db.Close()
results, err := db.Execute(
	"CREATE TABLE t (x INTEGER, s VARCHAR);" +
		"INSERT INTO t VALUES (1, 'héllo'), (NULL, NULL);" +
		"SELECT x, s FROM t ORDER BY x;")
if err != nil {
	panic(err)
}
fmt.Println(results[2].Kind, results[2].Rows) // selected [[1 héllo] [<nil> <nil>]]
```

`Execute` returns materialized `[]Result`s (`Kind`, `Columns`, `ColumnTypes`,
`Rows`, `Affected`, `Name`); values map to `nil`/`bool`/`int64`/`float64`/
`string`/UTC `time.Time`, and errors carry the native `pinkdb_last_error`
message. All native calls are serialized behind a per-`DB` mutex (the C
handle itself is single-threaded), `Close` is idempotent, and use after close
returns `ErrClosed`. See `bindings/go/README.md` for the value-mapping table
and linking notes.

```sh
cd bindings/go && go test ./...
```

## Benchmark (1M rows, release, Apple Silicon)

Generated with `bench/gen_sales.py` (deterministic seed 42), measured with `cargo run --release -p pinkdb --example query_bench` (best of 5):

For the join rows, "before" is the pre-wave-19 baseline (join_self 66, join_group 68, join_probe 64); the materializing mixed-predicate join is new, so it has no comparable baseline ("—").

| Query | Now (ms) | Before (ms) |
|---|---|---|
| `.import` 1M rows (24 MB CSV) | 25 | 28 |
| `SELECT COUNT(*) FROM sales` | 0.01 | 0.02 |
| `SELECT COUNT(*) ... WHERE total > 500` | 5.1 | 5.8 |
| `SELECT COUNT(*) ... WHERE region = 'west'` | 4.1 | 27 |
| `SELECT upper(region), length(region)` (1M rows) | 15 | 35 |
| GROUP BY region + 5 aggregates | 7.7 | 9 |
| GROUP BY id (1M groups) | 66 | 141 |
| ORDER BY total DESC LIMIT 10 | 3.0 | 100 |
| ORDER BY region (full sort) | 40 | 104 |
| DISTINCT region, active | 22 | 22 |
| DISTINCT id (1M values) | 64 | 75 |
| ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) | 93 | 165 |
| COUNT(*) OVER (PARTITION BY region) | 79 | minutes (O(n²)) |
| 1M-row self hash join + COUNT (fused) | 9.0 | 66 |
| 1M-row self join + GROUP BY region (fused) | 16 | 68 |
| 1M-row self join, mixed predicate (materializing) | 26 | — |
| 1M probes vs 4-row string-key side (fused) | 24 | 64 |

For reference, DuckDB 1.5.6 imports the same file in 0.06 s on this machine
(`read_csv_auto` → table), single connection.

### Wave 40 refresh (Oct 2026)

Wave 40 was a performance round (no new features). Same machine, same battery,
best-of-5; "before" = the same battery immediately before the wave:

| Query | Pre-wave-40 (ms) | Wave 40 (ms) |
|---|---|---|
| `SELECT COUNT(*) ... WHERE total > 500` | 5.5 | 2.4 |
| GROUP BY region + 5 aggregates | 9.2 | 7.0 |
| ORDER BY total DESC LIMIT 10 | 3.4 | 2.8 |
| ORDER BY region (full sort) | 42 | 39 |
| ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) | 95 | 61 |
| COUNT(*) OVER (PARTITION BY region) | 80 | 65 |
| 1M-row self joins (fused / +GROUP BY / mixed / probe) | 10.3 / 17.3 / 28.1 / 25.8 | flat (unchanged) |
| **battery total** | **493** | **447** (421 on a quiet host) |

The wins are profile-driven: typed aggregate kernels + a count-filter fast path
(`WHERE total > 500`), a window-key rework (the `OVER` rows) and parallel lake
data-file writes. The typed kernels keep semantics pinned exactly
(wrapping integer `SUM`, NULL skips, NaN rules), and no join row regressed.

### Wave 45 refresh (Oct 2026)

A second profile-driven round (pre-wave binary vs this one, interleaved
best-of-5 on the same host; full numbers in ROADMAP.md):

| Query (1M) | Pre-wave-45 (ms) | Wave 45 (ms) |
|---|---|---|
| GROUP BY region + 5 aggregates | 6.5–6.9 | 3.2 |
| `SELECT DISTINCT region, active` | 19.4 | 3.8 |
| ROW_NUMBER() / COUNT(*) OVER (PARTITION BY region …) | 63 / 63 | 13.7 / 13.4 |
| ORDER BY region (full sort) | 36 | 29 |
| GROUP BY id / DISTINCT id (1M groups) | 62 / 69 | 50 / 50 |
| join with a DISTINCT subquery | 21 | 5.3 |
| **battery total** | **416** | **245–250** |

At 10M: GROUP BY region 68–70 → 33–35 ms (DuckLake 37–55), import 274 → 173
ms, `.lake save` 632–758 → 296–340 ms, `.lake open` 618 → 100 ms, data
29.7 → 25 MB, store teardown 93 → 16
ms. The techniques: two-pass hash aggregation (group ids, then one typed loop
per aggregate) with typed single-column keys; DISTINCT through that
aggregation; parallel window partitioning/sorting/assembly; prefix-keyed
sorting; moving (not copying) bulk-loaded chunks; streamed + parallel data-file
encode and parallel bulk decode; parallel teardown. Every result is pinned
to the old semantics (duckdiff 60/60 + the 1M battery 64/64).

## Known v1 limits

- No nested transactions or savepoints; a durable `.pink` path is
  multi-process (wave 45m) — writers serialize on a per-operation write
  lease, a busy database waits up to the 5 s busy timeout (then errors), and
  `flock` is assumed to be honored: unsupported locking fails closed unless
  the `LockMode::UnsafeSingleProcess` opt-out is used, network filesystems
  are not detected, and Windows is untested; `.open` replaces the whole
  store; `.lake save`/`.lake open`/`.lake attach` are refused inside a
  transaction and `.lake open` refuses to detach a durable session
- `CREATE DATABASE` / `DROP DATABASE` / `LOAD EXTENSION` are refused inside a
  transaction; the last database cannot be dropped; `DROP DATABASE` does not
  remove views from other databases (views are per-database and session-local)
- RBAC (wave 48): `CREATE/ALTER/DROP USER`, `CREATE/DROP ROLE`, `GRANT` and
  `REVOKE` are refused inside a transaction (the per-table commit merge does
  not carry store metadata). There are no object owners — `CREATE` on a
  database lets a session change its schema but grants no table access, so
  the creator must be granted `SELECT`/… explicitly. `CALL` needs `ADMIN`
  (a procedure body binds only at call time, so its statements cannot be
  privilege-checked per statement). `DROP TABLE`/`DROP ROLE`/`DROP USER`/a
  `RENAME` cascade or move grants; concurrent user DDL from two processes is
  not conflict-detected by the WAL frame sets (the same shape procedure DDL
  has). wasm32 builds refuse `CREATE USER … PASSWORD '<plaintext>'` (no OS
  randomness there) while `PASSWORD_HASH` and verification work
- Loadable extensions are native-only (wasm builds refuse `LOAD EXTENSION`),
  process-level, and not part of `.pink` snapshots. A durable database
  WAL-logs `LOAD EXTENSION`, and records that call an extension are prefixed
  with the `LOAD EXTENSION` statements replay needs (touched-only; a
  checkpoint resets that coverage). A missing file at replay time is a hard
  error, and loading the same extension twice reports
  `extension "x" is already loaded`
- `USE`, `DATABASE`, `LOAD`, `EXTENSION` and
  `LEFT`/`RIGHT`/`OUTER`/`FULL`/`CROSS` are reserved
  words; quote identifiers to use them as names (`"use"` works)
- Durable commits refuse statements that read a session-local view (views are
  never persisted); `.import` checkpoints, so it needs a settled transaction
- Correlated subqueries are rejected (uncorrelated only); `NATURAL` and `USING`
  joins are rejected (write an explicit `ON` condition instead)
- Window functions are SELECT/ORDER-BY only (not WHERE/GROUP BY/HAVING)
- Aggregates in ORDER BY need a select alias or position
- General ORDER BY expressions must be aliases, positions, or GROUP BY keys
- `%` is integer-only
- Mixed DATE/TIMESTAMP comparison is rejected; timestamps never mix with
  integers; no time zones (naive UTC)
- Expression nesting is capped at 24 levels (parentheses, `CASE` arms and
  consecutive unary operators counted together); deeper input returns a clean
  `Parse` error (`expression nesting too deep`) instead of
  overflowing the parser stack. Separately, the parser-to-AST depth is capped
  at 64 nodes: left-associative operator chains (`1+1+...`), long `IN` lists,
  `AND`/`OR` runs and deeply nested subqueries return `expression too deep
  (limit 64)` — the AST is freed iteratively, so even a multi-megabyte chain
  cannot overflow the stack during rejection

- Lake (`pinklake`/`s3` features): every `.lake save` writes a full snapshot of
  every table; old versions and orphaned data files are never garbage
  collected; the lake is reached through `.lake open` (whole store into memory)
  or `.lake attach` (read-only, one version pinned, statistics-only: exact
  COUNT/MIN/MAX from the catalog's blocks, anything else lazily decodes the
  one table it touches — no time travel within a session; projections decode
  only referenced columns, and row-group statistics prune chunks inside a
  file); attached tables are session-local, not saved, and mutations over
  them are refused (mutating SQL over them on a durable store is refused too,
  since the log could not replay them); lake data files are plaintext unless
  the lake is keyed; one S3 object is capped at 5 GiB (no multipart uploads)
  and credentials come only from the environment or explicit config (no
  profiles/IMDS/SSO); the lake writes and reads `.pink` (`PINKDB06`) data
  files and supports `.lake attach … write` + `.lake commit`
- Extensions repository: artifacts are verified by SHA-256 from the catalog
  (no signatures yet), and only `darwin-arm64` builds are published
- Stored procedures (wave 44): a body's expressions bind at `CALL` time (the
  catalog may change between calls), so a mistyped condition is a call-time
  error, not a create-time one; a body cannot `USE`, `CHECKPOINT`,
  `LOAD EXTENSION`, touch databases or materialized views, or create/drop
  procedures; `COMMIT`/`ROLLBACK` only in the outermost procedure of a
  standalone `CALL`; recursion is capped at 32 levels; a durable store
  refuses a procedure that reads a session-local view or calls a
  loadable-extension function (their replay could not be honest); there is no
  `SHOW PROCEDURES` (a missing call reports the missing name), no OUT/INOUT
  parameters, and exception handlers catch only `WHEN OTHERS` (no SQLSTATE
  condition names, no `SAVEPOINT` statement)
- JavaScript procedures need the `js` feature (native targets only); without
  it a JavaScript definition loaded from a `.pink` file stays inert and
  `CALL` names the rebuild. `print` output is not carried by the server
  protocol or the FFI yet
- Known gaps and their scheduled waves are tracked in `docs/GAPS.md`

Wave 102 replaced the `.pink` container with **`PINKDB06`**: a Parquet-shaped
layout (row groups of 122,880 rows, a footer carrying the store metadata and
the per-table row-group directory, a per-column-chunk CRC32C and exact
per-chunk statistics, a 20-byte trailer) with FastLanes bit-packing, FSST and
a measured second compression stage (pure-Rust `lz4_flex`). One format, no
legacy: the `PINKDB01`–`PINKDB05` readers, their fixtures and their
compatibility tests are gone.

Post-v1 roadmap: C/Python/Go language bindings, Parquet reader (see
PLAN.md). At-rest encryption shipped in wave 25 behind the `encryption`
feature; the network server (`pinkdb-server`) shipped in wave 24; the lake
layer (`pink-lake`, CLI `pinklake` feature) shipped in wave 34 and gained S3/R2
object storage in wave 36 (`s3` feature; `s3://` locations, conditional-write
conflict detection), read-only `.lake attach` (statistics-only; exact
`COUNT`/`MIN`/`MAX` from stored statistics; lazy materialize) in wave 45d and
projection pushdown for attached scans in wave 45e; `.pink` v4 (wave 45h)
brought single-file saves to roughly the size of a Parquet lake, and wave 45j
added LZ frames (tag 8) that shrink text corpora 3–5×. Wave 45k gave attach
exact fast paths (a CRC-32-protected per-column statistics block published in
the manifest; zero data-file reads for `COUNT`/`MIN`/`MAX`) together with a
`.pink` v4 bit-pack correctness fix (u64 accumulators dropped high bits for
widths > 56). Durable `.pink` databases became multi-process in wave 45m — a
per-operation write lease, lock-free epoch refresh, WAL v3 and encrypted
container v2 — with stage 5 (optimistic disjoint-table commits) deferred and
the stability-hardening round shipped as wave 45n (chaos soak, lock-file
identity, WAL fuzz). Authentication and TLS are deferred.

## Development

```sh
cargo build            # workspace
cargo test             # 100+ unit + end-to-end tests
cargo test -p pinkdb-server                          # protocol + server + durable tests
cargo test -p pinkdb-cli --features client           # remote-mode CLI tests
cargo test -p pinkdb-cli --features pinklake         # `.pink` lake round-trips + CLI tests
cargo test -p pinkdb-cli --features pinklake,s3      # + S3/R2 over a hermetic mock S3
cargo test -p pinkdb-cli --features s3               # storage client alone: no .lake commands
crates/pink-lake/tests/s3_docker.sh                  # real S3 round-trip/race in Docker: SERVER=s3mock (default)|seaweedfs|rustfs (not cargo test)
cargo build -p pinkdb-cli --features pinklake        # REPL with .lake commands
cargo build -p pinkdb-cli --features pinklake,s3     # REPL with .lake s3:// URLs
PINKDB_CLI_BIN=./target/release/pinkdb cargo test -p pinkdb-cli --features pinklake  # release CLI (overflow checks off)
cargo test -p pinkdb-ext-loader                      # loadable extensions (compiles C test extensions with cc)
cargo test --features encryption                     # encrypted round-trips + WAL recovery
cargo build -p pinkdb-cli --features encryption      # REPL with PINKDB_KEY support
cargo clippy --all-targets   # must stay clean
python3 bench/duckdiff.py [--big sales.csv]  # differential test vs DuckDB
PINKDB_KEY=pass python3 bench/kill_loop.py 30 --encrypted  # SIGKILL stress, encrypted WAL
cargo run --release -p pinkdb --example import_bench  # CSV import timing
cargo run --release -p pinkdb --example query_bench  # full query battery (ms)
```

## CI/CD

Four GitHub Actions workflows in `.github/workflows/`, GitHub-hosted runners
only:

| Workflow | Trigger | Jobs / steps |
|---|---|---|
| `ci.yml` | push to `main`, every pull request | `build-test` — `cargo fmt --check`, `cargo build`, `cargo test -q`, `cargo clippy --all-targets -- -D warnings`, the feature tests (`pink-lake --features s3` / `s3,encryption`, `pinkdb-cli --features pinklake` / `pinklake,s3` / `s3` / `encryption` / `client` / `repo`, `pinkdb --features encryption`), clippy on every feature set, the SIGKILL kill loops (plain, multi-db, targeted, encrypted), the wasm check, then the release CLI uploaded as an artifact; `duckdiff` — downloads that CLI, installs the DuckDB CLI and runs `python3 bench/duckdiff.py`; `bindings` — release `pinkdb-ffi`, the C example (`make -C crates/pinkdb-ffi hello`), the Python `unittest` suite, and `gofmt` + `go vet` + `go test -race`; `sdk-smokes` — `make -C ext-sdk/c` + the C/C++, Go and Rust SDK smoke scripts; `s3-docker` — `crates/pink-lake/tests/s3_docker.sh` (s3mock in Docker; a hard gate); `macos` — workspace, lake and encryption tests plus the Python binding on macOS; `msrv` — `cargo check` of the whole workspace on Rust 1.97 (the edition-2024 MSRV); `audit` — `cargo audit`; `docs` — `website/build.py` and the roadmap-page sync check. Actions are pinned to commit SHAs |
| `release.yml` | push of a `v*` tag, or manual dispatch with a version | `prepare` (tag/version/Cargo.toml match + matrix), one `build` matrix job per CLI/server variant × platform (build → `scripts/release/smoke.sh` → tarball + `.sha256`, no secrets), then one `publish` job (`contents: write`) that downloads all 28 artifacts, regenerates and validates `SHA256SUMS`, refuses an existing release, and uploads the GitHub release |
| `deploy-docs.yml` | push to `main` touching `website/**` (or the workflow itself), manual dispatch | `python3 website/build.py`, then `wrangler deploy` from `website/` (the `pinkdb-docs` Worker) |
| `publish-extensions.yml` | push of a `v*` tag, or manual dispatch | builds the extension catalog in a per-platform matrix (C/C++, Go and Rust SDKs plus `extensions/*/Makefile`s; `file` architecture check; native SDK smokes), fans in to one publish job that fetches the live catalog, skips already-published coordinates, publishes with `ext-repo/publish.sh` (wrangler → R2), signs the catalog, deploys the Worker exactly once, then verifies the local/live signature and artifacts through it |

### Required repository secrets

Set these under *Settings → Secrets and variables → Actions*. Two names are
accepted for each — `CF_API_TOKEN` / `CF_ACCOUNT_ID` (tried first; used by this
repository's organization) or `CLOUDFLARE_API_TOKEN` / `CLOUDFLARE_ACCOUNT_ID`
(the fallback):

| Secret | Value | Used by |
|---|---|---|
| `CF_API_TOKEN` (or `CLOUDFLARE_API_TOKEN`) | a Cloudflare API token scoped to **Workers Scripts: Edit** (docs Worker) and **R2: Edit** (extension artifact + catalog uploads) | `deploy-docs.yml`, `publish-extensions.yml` |
| `CF_ACCOUNT_ID` (or `CLOUDFLARE_ACCOUNT_ID`) | the account that owns the `pinkdb-docs` / `pinkdb-ext-repo` Workers and the `pinkdb-extensions` R2 bucket (find it in the Cloudflare dashboard); store it as a secret, never in the repository | same |
| `PINKDB_EXT_SIGNING_KEY` | the Ed25519 catalog-signing private key (PEM content, base64 of the PEM, or a path). Never commit or log it; `docs/extension-signing.md` is the contract | `publish-extensions.yml` publish step only |

The release workflow needs no configured secret: its single `publish` job uses
the automatic `GITHUB_TOKEN` with `contents: write`; build jobs have no secrets
and only `contents: read`.

### Publishing extensions by hand

Actions → **Publish extensions** → *Run workflow*. Every input is optional:

- `name` — publish a single discovered extension, e.g. `cpp_ext`;
- `version` — artifact version (default: the version in `crates/pinkdb/Cargo.toml`);
- `platform` — one catalog platform tag (`darwin-arm64`, `darwin-x64`,
  `linux-x64`, `linux-arm64`); the workflow selects the matching runner
  (default: `darwin-arm64`, the historical manual runner).

With no inputs the workflow publishes every discovered artifact for that one
platform; a tag push builds all four platforms. Publishing is
immutable: coordinates already in the live catalog are skipped (the publisher,
`ext-repo/publish.py`, refuses them on its own too), and every catalog entry
carries the artifact's SHA-256 and size. The catalog is signed with Ed25519
(`index.json.sig`) using the `PINKDB_EXT_SIGNING_KEY` secret, passed only to the
publish step; a post-sign gate verifies the local and live signatures and every
artifact published by the run. `.install` pins the public key and refuses a
missing or invalid signature before downloading anything. See
`docs/extension-signing.md`.

### Verifying the workflows

Release and installer logic is also exercised locally without GitHub:
`tests/install/run.sh` serves a fake release tree to `install.sh` (latest,
pinned, variants, tampered/missing checksums, `--force`, prefixes, cleanup),
and `tests/release/test_package.sh` checks asset naming, tar contents,
`SHA256SUMS`, the matrix and extension discovery. `docs/releasing.md` is the
release contract.

Workflows can only be *truly* verified on GitHub — no local runner is used
here. Locally we lint every workflow with `actionlint`, parse them as YAML, and
run the individual commands they execute (the docs build, the SDK smokes, the
wasm build, `bench/duckdiff.py`, the binding suites, and the publish workflow's
discovery/report steps against the live catalog) by hand; the first GitHub run
of a workflow is the real gate. That gate is now passed: run
[38875669660](https://github.com/openlearnia/pinkdb/actions/runs/38875669660)
(2026-10-16) is the first fully green CI run — all nine jobs, including the
SIGKILL kill loops, the wasm check, the release build and the DuckDB
differential job — and the separate Deploy docs workflow is green too (the
site is deployed).
