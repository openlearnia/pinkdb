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
| `pinkdb::persist` | `.pink` single-file columnar persistence, v4 (typed paths + delta/dict-bitpack/ALP encodings + LZ frames; v1–v3 files load) |
| `pinkdb::crypto` | optional at-rest encryption (`PINKENC1` container: AES-256-GCM + Argon2id) behind the `encryption` feature; a parse/identity stub otherwise |
| `pinkdb::db` | Multi-writer `Database`/`Connection` over `Arc<RwLock<PinkDb>>` |
| `pinkdb::procedure` | Stored procedures: the SQL procedural language (parser + `CALL` executor), catalog definitions, `.pink` section |
| `pinkdb-js` | Optional (`js` feature) JavaScript procedure sandbox: the deterministic host contract; the engine binding is the recorded follow-up |
| `pinkdb-cli` | rustyline REPL, dot-commands, box-drawn output |
| `pinkdb-server` | TCP server + blocking client (framed binary protocol, thread-per-connection); no external deps |
| `pink-lake` | minimal lake layer: JSON catalog + snapshots + immutable per-table data files — Vortex with the CLI `vortexlake` feature, `.pink` v4 with the `pinklake` feature; v1 `.pink` data files still readable; local directories or S3/R2 via `s3://` URLs (opt-in `s3` feature — pure storage, it combines explicitly as `pinklake,s3` / `vortexlake,s3`); opt-in `encryption` feature (`PNKLAKE1` containers for every lake artifact, one Argon2id key per session) |

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

### Stored procedures

```sql
CREATE [OR REPLACE] PROCEDURE name(param TYPE, ...) AS $$ ... $$;
DROP PROCEDURE [IF EXISTS] name;
CALL name(args, ...);
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
* any ordinary SQL statement (DML, table DDL, `SELECT`, `SHOW`, `DESCRIBE`,
  `EXPLAIN`) and nested `CALL`s (recursion is bounded at 32 levels).

`CALL` returns the call's own outcome: the procedure's name, how many body
statements ran, and the result of the last statement it executed (`SELECT`
rows, an `Inserted` count, …). Store/process-level statements are refused in a
body at `CREATE` time (`USE`, transaction control, `CHECKPOINT`,
`LOAD EXTENSION`, `CREATE`/`DROP DATABASE`, materialized-view DDL and nested
procedure DDL).

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

**Durability.** Procedures live per database in the catalog, are written to
`.pink` (flags bit 4) and replay through the ordinary WAL path: a durable
store logs the committed `CALL` statement text and replays it by re-executing
the call. That is sound because the engine has no nondeterministic built-in
function (no clock, no randomness) and the constructs that could break replay
are refused while the call runs — a body statement that reads an attached
lake table, reads a session-local view (durable stores only), or calls a
loadable-extension function (an opaque callback) fails the call with a clear
error, and `CALL` arguments must be constant deterministic expressions. A
call's actual read/write set (it depends on the data: loop conditions, `IF`
branches) is recorded by the executor into the WAL frame and the transaction's
conflict set, so multi-writer conflict detection still holds.

**JavaScript procedures** (`LANGUAGE JAVASCRIPT`, syntax
`CREATE PROCEDURE p(a BIGINT) LANGUAGE JAVASCRIPT AS $$ ... $$`) are an
optional build (`js` cargo feature) and **refuse cleanly in this release**:
the `pinkdb-js` crate ships the sandbox *contract* — the deterministic host
API (`print`, `db.exec`, `db.query`, `db.value`), the denied-intrinsics policy
(no clock, no randomness, no ambient I/O) and the value conversions — while
the engine binding is the recorded follow-up. `CREATE PROCEDURE … LANGUAGE
JAVASCRIPT` without the feature names the rebuild
(`--features js`); with it, `pinkdb-js` reports the missing engine. The SQL
procedural language needs no feature and is always available.

### Databases

Databases are named, case-insensitive namespaces (`main` by default). A
session (the embedded `PinkDb`, or each `Connection`) has a current database;
unqualified table names resolve there. `USE db` switches it and never affects
other sessions — `Database`/`Connection` carry their own current database, the
shared store is untouched. `db.table` qualifies any table reference in
`FROM`/`JOIN`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `DROP TABLE`,
`CREATE VIEW` and `DROP VIEW`; cross-database joins (`db1.t JOIN db2.u`) work.
Three-part column references (`db.table.col`) are not supported (clear parse
error). CTEs shadow unqualified names only; a qualified name never resolves to
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
and no per-statement timing (`.timer on` turns it back on), and the process
**exits 1** if any statement or dot-command failed. A statement still open at
end of input runs as if terminated, and `;` is recognized outside strings and
comments (`SELECT 1; -- note` completes; `SELECT ';'` does not).
`.read <file.sql>` runs a script in the current session.

REPL dot-commands: `.help`, `.quit` / `.exit`, `.read`, `.timer`, `.databases`, `.tables`,
`.schema <t|v>` (accepts `db.table`; views show their output columns),
`.import <file.csv> [table]` (table defaults to the file stem; `db.table`
targets another database), `.save <file.pink>`, `.open <file.pink>`.
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
columnar format (v4: `PINKDB04`, ending in a CRC-32 of the whole image);
`PinkDb::open("x.pink")` loads it back (`.save` / `.open` in the REPL). The
v4 encoder and decoder run on typed per-column paths (no per-row `Value`
materialization; chunks save in parallel, and serial and parallel saves
produce identical bytes) and add DELTA_BITPACK, DICT_BITPACK and ALP
encodings (bit-exact exceptions) plus an "all valid" validity flag to the
raw/RLE/bitpack/dictionary set. v1 files (`PINKDB01`, a flat table list), v2
(`PINKDB02`, the databases layout) and v3 (`PINKDB03`, v2 + the checksum)
still load; v1 tables become the single `main` database, and a format newer
than this build (`PINKDB05`+) is refused with a clean "newer format" error,
never guessed at. The encrypted `PINKENC1` container wraps v4 transparently
(the container itself is version 2 since wave 45m — see Encryption). Views are
session-local and never persisted. Round-trips are bit-exact (including `-0.0`
and NaN payloads); truncated or corrupt files return errors, never panics — a
damaged file fails its CRC-32 before any decoding, and no allocation is sized
from an unvalidated length.

v4 also carries an optional **LZ frame** (encoding tag 8, reserved since v4,
implemented in wave 45j): a chunk whose best existing encoding is at least
1024 B is probed on its first 4 KiB and, when the probe saves ≥10%, stored as
a hand-rolled greedy LZ77 stream (64 KiB window, 4-byte hash, min match 4; no
dependencies, no `unsafe`) framing the chosen encoding — `u32 raw_len | u8
base_tag | u32 token_len | tokens`, where `base_tag` names the encoding the
tokens rebuild — and only when strictly smaller. Measured on 1M-row text
corpora (before → after, ×Vortex): JSON-like 70.4 → 15.7 MB (5.71× → 1.27×),
logs 118.7 → 44.5 MB (3.48× → 1.30×), URLs 103.5 → 41.4 MB (3.15× → 1.26×).
Random corpora and both sales tables stay byte-identical (a ~300 B
bitpacked-dictionary candidate skips the scan; save +0–1%), and LZ also speeds
save/open on text. Round trips stay bit-exact and old binaries reject a tag-8
file cleanly (`unsupported v4 encoding tag 8; upgrade pinkdb`). A further
string encoding, **FSST** (tag 9), is measured but deliberately not
implemented: after LZ the text gaps to Vortex are 9.8% / 6.5%, and a
255-symbol FSST floor estimate reaches only 3.2% / 4.3% (the remainder is the
offset encoding); tag 9 stays reserved with a recorded design.

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
writing one. 1M-row sales table: 24.5 MB CSV → **2.68 MB** `.pink` (1.015×
the equivalent Vortex bytes), saved in 0.03 s, reopened in 0.03 s; the same
table at 10M rows is 26.8 MB, saved in 0.29 s and reopened in 0.26 s. 1M-row
text corpora (before → after the LZ frame): JSON-like 70.4 → 15.7 MB, logs
118.7 → 44.5 MB, URLs 103.5 → 41.4 MB — now 1.26–1.30× Vortex (was
3.15–5.71×). The same parallel encode pass also collects the exact facts a
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

## Lake layer (features `pinklake` / `vortexlake`)

`pink-lake` is the minimal lake: **metadata and data storage decoupled** — a
JSON catalog plus immutable per-table data files, on local disk or (with the
`s3` feature) in an S3 bucket/prefix. Wave 35 makes **Vortex the default data
format**; wave 36 adds S3/R2 object storage; wave 45d adds read-only
**`.lake attach`** — a footer-only open with exact `COUNT`/`MIN`/`MAX` from
stored statistics and lazy materialization — and wave 45e adds **projection
pushdown** (an attached scan decodes only the columns a query references).
Wave 45i adds the `pinklake` build: the same lake over `.pink` v4
data files with no Vortex dependency, and wave 45k brings attach to it — a
pinklake save publishes a CRC-protected per-column statistics block in the
manifest (`statistics-only` attach), so exact `COUNT`/`MIN`/`MAX` read no data
file in either build (see "The `pinklake` build" below). Wave 63
makes every lake **incremental** (`format_version` 4): appends add new
immutable data files and never rewrite published ones, `DELETE`/`UPDATE`
become positional delete files applied merge-on-read, each commit writes an
append-only delta manifest (`commits/<version>.json`), `.lake attach … write`
+ `.lake commit` publish through the attached-database route, and a
conflicting commit retries with DuckLake's settings (see "Incremental
commits" below). Wave 66 mirrors DuckLake's **partitioning and pruning**:
`.lake partition <dir> <table> by (<key>, …)` (identity columns and
`bucket(N, col)`; DuckLake's `ALTER TABLE … SET PARTITIONED BY`) records a
per-table spec in the catalog, commits **split appended rows into one data
file per partition value set** and record each file's values
(`ducklake_file_partition_value`'s twin), every data file publishes a
per-file zone map (the lean `.pink` block, or the Vortex catalog block
cross-checked against the file footer), and an attached scan whose `WHERE`
carries simple comparisons (`=`, ranges, `IS [NOT] NULL`) **skips whole data
files without decoding them** — pruning a partitioned table by its partition
column reads only the matching files (see "Partitioning and pruning"
below). Wave 67 adds **maintenance**: `.lake merge` / `.lake rewrite`
consolidate data files into one new version (published files are never
mutated), `.lake expire` removes old snapshots + commit manifests, `.lake
cleanup` / `.lake orphans` delete `data/` objects no retained version
references — both behind a two-day age gate and an explicit
`dry-run`/`confirm` — and `.lake files` / `.lake table_info` / `.lake
options` / `.lake settings` are DuckLake's list/info introspection (see
"Maintenance" below).

A lake is a directory (or, with the `s3` feature, an `s3://bucket/prefix`
key prefix with exactly this layout):

```text
mylake/
├── catalog.json               current state (authoritative "latest")
├── snapshots/<version>.json   immutable manifest per published version
├── commits/<version>.json     immutable per-commit delta manifest (audit)
├── data/<table>/<uuid>.vortex immutable per-table Vortex files (v4, default)
├── data/<table>/<uuid>.pink   `.pink` data files (v1/v4 lean builds)
└── data/<table>/<uuid>.pinkdel positional delete files (v4; merge-on-read)
```

The manifests (`catalog.json` and every snapshot) share one shape:

```json
{
  "format_version": 5,
  "updated": "2026-10-14T02:11:50.760Z",
  "version": 2,
  "tables": {
    "analytics.u": {
      "files": [{ "path": "data/analytics.u/e9a4….vortex", "rows": 1 }],
      "rows": 1,
      "format": "vortex"
    },
    "main.t": {
      "files": [
        { "path": "data/main.t/831a….vortex", "rows": 2 },
        { "path": "data/main.t/c709….vortex", "rows": 1 }
      ],
      "deletes": [
        { "path": "data/main.t/2ab1….pinkdel", "file": "data/main.t/831a….vortex", "count": 1 }
      ],
      "rows": 2,
      "format": "vortex"
    }
  }
}
```

Wave 64 adds the optional **snapshot record** to that shape — `"author"`,
`"commit_message"` and `"changes"` (see "Snapshot records and time travel"
below). All three are additive and absent when a save records nothing, so an
older reader ignores them and a manifest written today without a message is
byte-identical to the pre-wave-64 shape.

**Format history and defaults:**

| catalog `format_version` | data file | written by | read by this build |
|---|---|---|---|
| 1 | `data/<table>/<uuid>.pink` — the engine's single-table persistence bytes; a `pinklake` save also publishes a CRC-protected per-column statistics block in the entry (wave 45k) | wave 34 | yes (entries have no `format` field; `.pink` opens standalone with `PinkDb::open`); `.lake attach` reads the statistics block, opening no data file |
| 2 | `data/<table>/<uuid>.vortex` — one Vortex file per table (one `files` entry), `"format": "vortex"` on every entry; one row block per 2,048-row engine chunk | waves 35–40 | yes |
| 3 | as 2, but row blocks of 8,192 rows / ~1 MiB (fewer blocks → less per-block stats/compression work on save) | waves 45 | yes |
| 4 | **incremental**: every data file is listed as `{path, rows}`, a table's file list grows by appended files, positional delete files (`<uuid>.pinkdel`) drop rows merge-on-read, `rows` must equal `sum(files.rows) − sum(deletes.count)`, and `format` is `"vortex"` (vortexlake build) or `"pink"` (pinklake build; each `.pink` file carries its own CRC-protected statistics block). Wave 66 adds (additively): a table's `partition_by` spec, a data file's `partition` values, and a catalog-published zone map (`stats`) on Vortex files too — an older v4 reader ignores the fields and reads the same files | this build (default) | yes |

**Migration:** open an old lake and `.lake save` again — the next snapshot is a
full rewrite in this build's v4 format; old snapshots and their data files stay
untouched, so every published version still loads. A `pinklake` build
refuses a v4 (or v2/v3) **Vortex** lake with the rebuild-with-`vortexlake`
error (a pinklake-written lake opens bit-exactly in a vortexlake build, which
migrates it on the next save), and a v1/v2/v3 lake stays readable in both
builds. Any unknown
`format_version` (a lake written by a newer pinkdb) refuses with "the lake was
written by a newer pinkdb — upgrade to open it", and a wrong-shape entry (a
v2/v3 entry without `format`, a v4 file object without its row count, a v4
manifest whose rows do not add up) is a clear `Corrupt` error, never a guess.

- **Schema and typing live in the data files.** A data file is a plain
  Vortex file: one struct array, one field per column, row order preserved;
  the loader slices stored row blocks back into `CHUNK_SIZE`-row engine
  chunks, so a table loads densely chunked (ragged chunks left by deletes come
  back compacted — values and order unchanged). DOUBLES keep
  raw IEEE-754 bits (NaN payloads, `-0.0`), `i32`/`i64` cover their full
  ranges, VARCHAR is `utf8`. DATE/TIMESTAMP are stored as their raw `i32`/`i64`
  storage integers plus the exact pinkdb schema in a `pinkdb.table` metadata
  segment (with the table's `db`/`table` identity, which is what rejects a
  data file swapped in from a *differently named* table): the native
  `vortex.timestamp` extension converts values
  through a calendar type limited to about ±20 000 years and **panics** on
  extreme timestamps (±6.31e17 µs) during statistics computation, so no bit
  pattern is left to chance — see below.
- **Every save is a full snapshot; commits are incremental.** `write_from_store`
  writes every table of every database to a fresh data file, publishes
  `snapshots/<version>.json` (`version = previous + 1`, via `checked_add` and
  capped at `MAX_VERSION`, so a tampered `version: u64::MAX` or a lake at the
  cap is a clear error, never a wrap), then replaces `catalog.json` with the
  same bytes. Old versions and their data files are never modified — and a
  snapshot file that already exists is never replaced: `save` that would land
  on a published version (e.g. after restoring an older `catalog.json`) fails
  with a conflict instead of clobbering it. `load(Some(v))` restores any
  published version, `load(None)` the latest. A **commit**
  (`Lake::commit`/`commit_from_tables`, `.lake commit`) publishes a new
  version made of the previous files **plus** one new data file per appended
  row batch and one positional delete file per (table, data file) —
  `UPDATE` is a delete of the old position plus an append — so a commit that
  adds a few rows to a 1M-row table writes only those rows. A table's data
  files are never rewritten, replaced or reused for new data. There is no
  compaction or GC yet (maintenance is a later wave), so old versions' files
  stay.
- **Atomicity order: data files → delta manifest → snapshot → catalog.** Locally every write
  goes to a *unique* temp sibling (`<name>.<uuid>.tmp`, created with
  `create_new(true)` so a planted symlink or an older writer's temp file is
  never followed or truncated), is fsynced, then renamed into place (catalog,
  data files) or **hard-linked** into place (snapshots: an existing version
  can never be replaced). Over S3 each write is a single atomic `PutObject`
  (a reader sees the old or the new bytes, never a mix — no temp objects are
  needed), the snapshot is `If-None-Match: *` and the catalog `If-Match`;
  see "Object storage (S3/R2)". The parent directory is fsynced afterwards,
  best-effort. Locally this is
  **process-crash safety**: after a `kill -9` the previous version always
  loads, and stray data files, `.tmp` files and snapshots above the catalog's
  version are ignored (`versions` lists only manifests that parse and declare
  their own canonical version; `commits()` lists the delta manifests). A
  stray delta manifest at the next version (never with its snapshot) is
  self-healing: the retry that reaches that version overwrites it, because
  the snapshot is the authoritative manifest. It is *not* a power-loss
  guarantee on APFS:
  `fsync(2)`/`File::sync_all` can return before the drive flushes its cache;
  only `F_FULLFSYNC` would, and this crate deliberately adds no
  unsafe/libc dependency for it (a future opt-in mode could). One leftover is
  visible: a stray manifest at exactly the *next* version can't be told apart
  from a published one, so a save landing on it refuses with a clear conflict
  ("if catalog.json still points at the previous version it was never
  published — remove the stray snapshot and retry"). Corrupt metadata is a
  clear error; a truncated/corrupt Vortex data file, or one whose
  `pinkdb.table` metadata does not parse, is the same `Corrupt` error. Panics
  inside Vortex's decoder for malformed layouts (there are several, including
  `debug_assert!` sites) are caught — the whole decode boundary (`open_buffer`,
  metadata/schema checks, scan, per-column decode) runs under
  `catch_unwind` — and surfaced as a `Corrupt` error naming the data file, so
  a decoder panic is an error, not a process death. The default panic hook may
  still print one line to stderr before the caught error; the library does not
  swap the global hook (that would race other threads).
- **Flat `db.table` names**, restored by splitting on the first dot; a table
  name may contain dots, a database name containing `.` is refused at save
  time. Views are session-local and are not saved. A lake records tables,
  so a database with no tables is not preserved and a dropped `main` comes
  back empty (loading always recreates it). Data files are plaintext by
  default. With the non-default `encryption` feature (the same CLI
  `--features` flag that guards `PINKDB_KEY` for `.save`/`.open`) a lake
  opened or created with a key seals **every** artifact — catalog, snapshots,
  commit deltas, data files, and positional delete files — into a `PNKLAKE1`
  container (AES-256-GCM; one Argon2id-derived key per session, whose salt is
  recorded in every container header, catalog included); a keyed open of a
  plaintext lake and a keyless open of an encrypted one are both refused
  (`wrong key` is distinguished from tampering), and plaintext lakes stay
  readable everywhere, byte-identically. Plaintext data files carry no
  checksums: mutating one in place is undetected by design. The checks that
  do run on load are structural — a v2 file's `pinkdb.table` metadata must
  name the catalog's table, agree with the Vortex field names/types, and the
  file's row count must match the catalog — so a file swapped in from a
  *differently named* table is rejected. Swapping two *plaintext* files of the
  **same** table (same name, same schema, same row count) cannot be detected
  (an encrypted file is authenticated and bound to its catalog path, so the
  swap fails closed), and a tampered plaintext file may still decode to
  different values or fail with a clean `Corrupt` error (caught decoder
  panics included).
  `save_store`/`.lake save` pre-validate the store (dotted database names,
  over-long encoded table names) *before* creating a lake, so a rejected save
  leaves nothing behind.
- **Read-only attach (wave 45d): `.lake attach <dir|s3> [version]`** reads the
  catalog, the chosen manifest and every data file's footer, metadata and
  statistics — no column data is decoded (over object storage the whole objects
  are read, still without decoding). A `.pink` lake attaches from the
  manifest's **statistics blocks** instead (wave 45k; v4 stores one per file,
  v1 one per entry): not even the data file is opened, and a block that fails
  its CRC-32 checksum, disagrees with the entry (table/file/rows), declares an
  unreadable version or carries an undecodable value refuses with a clear
  "use .lake open" / re-save message, never a guess; a lake saved before
  45k (no block) refuses the same way. A v4 table with several files merges
  the per-file statistics (NULL counts add up, min/max fold) as long as every
  file proves them; once the table has **delete files** the per-column facts
  are no longer provable without reading data, so they fall back to
  materialization while the row count stays exact (`COUNT(*)` keeps its fast
  path). Attached tables are session-local and
  read-only: they resolve under their `db.table` names, `.tables`/`.schema` mark
  them (`t (3 rows, attached)`), and every mutation over one is refused
  (`attached lake tables are read-only`). Exact `COUNT(*)`/`COUNT(col)`/`MIN`/
  `MAX` answers come from the stored statistics under strict gates (exact null
  counts; `Precision::Exact` min/max; `NaNCount == 0`; no ambiguous `±0.0`
  DOUBLE stat; a multi-aggregate query is all-or-nothing); anything unproven
  materializes the one table the query touches through the same decode
  `.lake open` performs, cached for the session. `.lake detach <dir>` drops one
  attachment and `.lake attached` lists them; `.lake save` skips attached tables
  with a note; `.lake open` drops them all; and a durable database refuses
  mutating SQL that reads an attached table (its logged text could not replay).
- **Incremental commits, DuckLake parity (wave 63).** `Lake::commit` takes
  appended rows (`Commit::append`, encoded into one new immutable data file
  per table) and positional deletes (`Commit::delete_positions`); readers
  union the data files in listed order and drop each delete file's positions
  from its file. `Lake::commit_from_tables(base_version, base, current)`
  computes the exact row-level diff (bit-exact for `DOUBLE` — NaN payloads
  and `-0.0` are themselves; duplicate rows and order handled) and publishes
  it; `WritableAttachment` is the `.lake attach … write` route on top: it
  materializes the pinned version, the caller mutates with ordinary DML, and
  `.lake commit` publishes the difference (durable sessions flush the WAL
  first and re-attach by adoption after recovery, so commits are idempotent).
  A conflicting publish retries with DuckLake's writer controls —
  `max_retry_count` (default 10), `retry_backoff` (1.5) and `retry_wait_ms`
  (100, the wait doubles per retry by the backoff factor) — rebasing the
  already-written immutable files onto the new head; a delta whose base file
  the head no longer contains, or positions another commit already deleted,
  is refused, never re-applied. The CLI reads the overrides from
  `PINKDB_LAKE_MAX_RETRY_COUNT`, `PINKDB_LAKE_RETRY_BACKOFF` and
  `PINKDB_LAKE_RETRY_WAIT_MS`. Every published version also writes
  `commits/<version>.json`, an append-only delta manifest listing the data
  and delete files that commit added and its row movement (audit; the
  snapshot stays authoritative for loading).
- **Snapshot records and time travel (wave 64).** Every save records the
  version's publish time in the manifest's `updated` field (millisecond
  ISO-8601 UTC — that field has been there since wave 34) and can record an
  author, a commit message and a **change summary**: `.lake save <dir>
  [force] [author '<name>'] [message '<text>']` carries DuckLake's
  `ducklake_set_commit_message` metadata, and `Lake::set_commit_message`
  attaches it to the next published save (kept across a failed save, cleared
  by a successful one). The summary is computed at save time from the
  previous manifest (`tables_created`/`tables_dropped`, `tables_rewritten` —
  every surviving table, since a whole-store save writes fresh files — and
  `rows_added`/`rows_removed` for tables present in both); it is absent for
  the create-time empty version and when the previous manifest is
  unavailable. `.lake snapshots <dir>` lists every version's record: time,
  author, message, `(tables, rows)` and the summary.
  **`FROM t AT (VERSION => n)` / `AT (TIMESTAMP => ts)`** queries an attached
  table as of a published version: `n` is a pinkdb version (1-based;
  DuckLake's snapshot ids count from 0), a timestamp resolves to the newest
  snapshot published at or before it (inclusive, DuckLake's `snapshot_time <=
  ts`), and both accept the exact strings `.lake snapshots` prints
  (`TIMESTAMP '2026-10-17 12:30:36.029'` or the manifest's
  `'2026-10-17T12:30:36.029Z'`). Time travel is an **attach-only**
  capability: it reaches versions `1..=the attached version` of that
  attachment (a pinned `.lake attach dir 3` never sees a later version), a
  store table refuses with "has no snapshot history" (DuckLake: "Catalog type
  does not support time travel"), CTEs and views refuse, an unknown version
  or a timestamp before the first snapshot is a clean "no snapshot found"
  error, and a table that did not exist at the target version names it. The
  statistics fast paths work per version, so `COUNT(*)`/`MIN`/`MAX` over
  `AT (VERSION => n)` decode no data file. DuckLake's
  `ducklake_default_version` setting has no pinkdb counterpart on purpose: it
  is not a per-query default — it selects the *storage format version for new
  catalogs*, and the probe build rejects every explicit value ("Unsupported
  ducklake_version") — so pinkdb skips the setting rather than inventing
  different semantics under the same name.
- **Partitioning and pruning, DuckLake parity (wave 66).**
  `.lake partition <dir> <table> by (<key>, …)` (or `… clear`) sets or
  clears a table's partition specification as one catalog version — the
  pinkdb surface for DuckLake's `ALTER TABLE … SET PARTITIONED BY`. A key is
  an identity column (`region`) or `bucket(N, column)`; DuckLake's other
  transforms (`year`, `month`, `day`, `hour`, `epoch_*`, `truncate`) are
  refused with its own supported set named in the message, duplicate keys
  are refused like DuckLake's "matches an earlier partition key", and every
  key column must exist in the table's schema. A commit (or save) that
  writes rows to a partitioned table **splits them into one data file per
  partition value set**, and the catalog records each file's values —
  `ducklake_file_partition_value`'s twin (identity values exact, bucket
  labels from pinkdb's own stable FNV hash, SQL NULL as JSON `null`).
  Existing files are never rewritten: a spec change governs new writes
  only, exactly like DuckLake's versioned partition records. Every data
  file also publishes a **zone map** — the exact per-column min/max/NULL
  facts — for `.pink` files in the existing CRC-protected per-file block
  and for Vortex files in a catalog block cross-checked against the file's
  own footer at attach. An attached scan whose `WHERE` carries simple
  conjuncts (`col = literal`, ranges, `IS [NOT] NULL`, `AND`-combined;
  extracted by the planner, never inferred through `OR`/expressions)
  **skips whole files the zone maps prove cannot match — no decode**, then
  decodes the survivors with the ordinary projection pushdown. A file with
  positional deletes is still prunable (deleting rows only shrinks its
  value set), while the table-level `COUNT`/`MIN`/`MAX` fast paths keep
  their wave-63 fallback once any delete file exists — both rules stay
  exact. `.lake attached` reports `(loads, columns, files)` decoded, and
  time travel prunes from **that version's** file list and zone maps.
  Limits: bucket values are pinkdb's own (DuckLake records DuckDB's hash;
  the numbers are not comparable across engines), no `year`/`month`/… time
  transforms yet (the full set is probed and refused by name), no bucket
  pruning (a `bucket` file still prunes through its column zone maps), and
  old files written before a spec carry no values until rewritten
  (`.lake save` re-splits a table under its preserved spec).
- **Maintenance, DuckLake parity (wave 67).** `Lake::merge_adjacent_files`
  (`.lake merge <dir> [table] [min_file_size=N] [max_file_size=N]
  [max_compacted_files=N]`) is `ducklake_merge_adjacent_files`: it publishes
  one new version whose table entries reference consolidated data files —
  never a rewrite of a published file (tests pin bytes *and* mtimes). A file
  is only a candidate when it carries no positional deletes (DuckLake skips
  delete-bearing files; `rewrite` is the tool for them), and files group by
  **partition values + schema epoch**, with at least two per group, exactly
  DuckLake's `DuckLakeCompactionGroup` rules probed live (the merge of 3 of
  4 files, the 2-file floor, and the `>=`/`<` size filters). The merged
  file's rows and zone map are exact (same writer path as any data file),
  so stats-fast `COUNT`/`MIN`/`MAX` and filter pruning work unchanged.
  `Lake::rewrite_data_files` (`.lake rewrite`) materializes the deletes of
  the files whose delete ratio reaches `delete_threshold` (DuckLake's
  default 0.95) and drops their delete files from the new manifest.
  `Lake::expire_snapshots` (`.lake expire [versions=a,b] [older_than=T]
  [keep=N] [dry-run]`) removes old `snapshots/<v>.json` + `commits/<v>.json`
  — never the current version, `versions` and `older_than` mutually
  exclusive — and time travel to an expired version is the ordinary clean
  `no snapshot version` error. `Lake::cleanup_old_files` /
  `Lake::delete_orphaned_files` (`.lake cleanup` / `.lake orphans
  [older_than=T] [all] [dry-run] [confirm]`) delete `data/` objects **no
  retained version references** (merged-away files after expiration, crashed
  writers' `.tmp` leftovers), behind the probed DuckLake default age gate
  (`delete_older_than` = 2 days; `all` = `cleanup_all`) and an explicit
  `dry-run`/`confirm` — a call that would delete without either is refused.
  Honest limits: pinkdb stores no `ducklake_files_scheduled_for_deletion`
  queue, so `cleanup` and `delete_orphaned_files` run the same object scan
  (documented, not faked); compaction never merges files of different
  schema epochs (DuckLake v1.1 can fold older schemas into the latest); and
  an object whose age cannot be read (no `Last-Modified` over S3) is never
  deleted without `all`. Introspection mirrors DuckLake too: `.lake files`
  (`ducklake_list_files`; rows, size, partition values, schema epoch,
  delete pairing, and `all` marks objects no retained version references),
  `.lake table_info` (`ducklake_table_info`), `.lake options`
  (`ducklake_options` — read-only catalog facts) and `.lake settings`
  (`ducklake_settings`).
- **One live writer handle per lake** (no file locking; concurrent readers
  of published versions are fine). A handle that saves after another
  handle/process moved the catalog fails with `LakeError::Conflict`
  ("another writer published version N; reopen the lake") *before* replacing
  the catalog, and two writers racing for one version cannot both win: the
  snapshot publish is create-only (`If-None-Match: *` over S3, a hard link
  locally), so exactly one publishes it, and `catalog.json` is byte-identical
  to the snapshot of its version. Over S3 the catalog replace itself is a
  conditional `If-Match: <etag>` write, so the check is atomic with the write
  — see "Object storage (S3/R2)" below. All I/O goes through a private
  `Storage` trait: `LocalStorage` (the original directory implementation,
  same files/atomicity/errors) or, with the non-default `s3` feature,
  `S3Storage` (a blocking SigV4 REST client over `ureq`/rustls).
- **Measured on this machine (2026-10, release, Apple Silicon, 1M-row
  `bench/sales_1m.csv`: id/region/total/active):** one Vortex data file is
  3,117,738 B vs 14,042,574 B for the equivalent `.pink` bytes (0.22×, ≈4.5×
  smaller); a lake save takes 0.20 s vs 0.09 s (Vortex compresses on write)
  and a load 0.07 s vs 0.11 s (wave 45h: `.pink` v4 shrank the same table's
  `.pink` bytes to 2,679,387 B — 1.015× the Vortex bytes — save 0.029 s, open
  0.026 s). Reproduce with
  `cargo test --release -p pink-lake --features vortexlake --test bench_1m
  -- --ignored --nocapture`.
  **Dependency/build note:** `crates/pink-lake` depends on the `vortex`
  crate directly (`default-features = false, features = ["files"]`, opt-in
  via `vortexlake` since wave 45i); that
  pulls in ~218 additional crates (vortex-array/file/layout + encodings,
  arrow, prost/flatbuffers, smol/async, jiff) and adds ≈42 MB to the release
  `pinkdb` binary built with `--features vortexlake`. A cold build is
  machine-dependent (minutes for the debug and release trees on Apple
  Silicon; the exact numbers were not reproduced precisely). Note that
  `pink-lake` is a workspace member, but since wave 45i its Vortex format is
  opt-in: root workspace commands (`cargo build`/`cargo test` at the repo
  root) compile the `.pink` set, and the Vortex tree compiles only for
  `vortexlake` builds (the CLI `vortexlake` feature, or `-p pink-lake
  --features vortexlake`). The default `pinkdb-cli` and the `pinklake` builds
  are Vortex-free; see the build table above for current sizes.

Library:

```rust
use pink_lake::{Lake, save_store};
use pinkdb::PinkDb;

let mut db = PinkDb::new();
db.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2);")?;

let version = save_store(&db, "mylake")?;  // creates the lake, publishes v2
let mut lake = Lake::open("mylake")?;      // v1 is the create-time empty catalog
lake.set_commit_message(Some("alice".into()), Some("two rows".into()));
let v3 = lake.write_from_store(&db)?;      // full snapshot in Vortex, version 3

// Incremental: append rows as one new immutable data file (nothing else is
// rewritten) and/or record positional deletes.
let mut commit = pink_lake::Commit::new();
commit.append("main.t", std::sync::Arc::new(appended_rows));  // same schema
commit.delete_positions("main.t", "data/main.t/<uuid>.vortex", &[0]);
let report = lake.commit(&commit)?;        // v4: files + rows reported

let tables = lake.tables()?;               // [("main.t", 3)]
let versions = lake.versions()?;           // [1, 2, 3, 4]
let records = lake.snapshots()?;           // per-version time/author/message/changes
let latest = lake.load(None)?;             // rebuild the latest
let old = lake.load(Some(2))?;             // rebuild a specific snapshot

// Attach the lake read-only and query a published version by name:
// let report = lake.attach(None)?;
// db.attach_lake(report.lake.clone())?;
// db.execute("SELECT * FROM t AT (VERSION => 2);")?;          // exactly v2's rows
// db.execute("SELECT * FROM t AT (TIMESTAMP => TIMESTAMP '2026-10-17 12:30:36');")?;

// With the `s3` feature, every location may be an object-storage URL:
// let mut lake = Lake::open_url("s3://bucket/prefix")?;  // env credentials
// let version = save_store_url(&db, "s3://bucket/prefix")?;
```

REPL builds (`.lake` commands are opt-in; `vortexlake` is the Vortex build,
`pinklake` the `.pink`-data-file build, and **`s3` is pure storage — it
implies neither lake build**; combine them explicitly, `pinklake,s3` or
`vortexlake,s3`):

| Build | CLI features | Lake | Data files | Release size |
|---|---|---|---|---|
| default | — | no `.lake` commands | — | 3,637,664 B |
| pinklake | `pinklake` | all `.lake` commands incl. read-only `.lake attach`, `.lake attach … write` + `.lake commit` (statistics-only attach: exact `COUNT`/`MIN`/`MAX` from the manifest's stats blocks, 0 data-file reads; anything else lazily decodes) | `.pink` (v4 lakes) | 4,950,400 B (+1.25 MiB over default, −40.6 MiB vs vortexlake) |
| pinklake + S3 | `pinklake,s3` | as above, `s3://bucket/prefix` locations | `.pink` | 6,970,192 B |
| vortexlake | `vortexlake` | all `.lake` commands incl. read-only `.lake attach`, `.lake attach … write` + `.lake commit` (footer-only statistics fast paths) | Vortex (v4 lakes) | 47,553,136 B |
| vortexlake + S3 | `vortexlake,s3` | as above, `s3://` locations | Vortex (v4 lakes) | ≈49.5 MB (vortexlake + the same ≈2 MB S3 client) |
| s3 only | `s3` | no `.lake` commands (the client only takes effect with a lake feature) | — | same bytes as default |

```sh
cargo build -p pinkdb-cli --features vortexlake        # Vortex data files, local directories
cargo build -p pinkdb-cli --features vortexlake,s3     # Vortex data files + S3/R2 object storage
cargo build -p pinkdb-cli --features pinklake          # lean `.pink` data files (no Vortex), local
cargo build -p pinkdb-cli --features pinklake,s3       # lean `.pink` data files + S3/R2
cargo build -p pinkdb-cli --features s3                # storage client only (no .lake commands)
```

```text
.lake save <dir> [force] [author '<name>'] [message '<text>']
                          snapshot the store (version, tables, rows, bytes);
                          author/message record the version's commit metadata
.lake open <dir> [ver]    replace the in-memory store with the lake
                          (session database resets to main; PINKDB_KEY is kept)
.lake attach <dir> [ver] [read-only|write]
                          read-only (default): attach from statistics only
                          write: materialize the lake's tables into the store
                            (a same-named, same-shape store table is adopted),
                            mutate them with DML, publish with .lake commit
.lake commit <dir>        publish the writable attach's change as one
                          incremental version (appended data files + positional
                          delete files; nothing published is rewritten)
.lake detach <dir> [force] detach one attached lake (refuses with uncommitted
                          writable changes; force keeps the tables as ordinary
                          store tables)
.lake attached            list this session's attached lakes and their tables
.lake versions <dir>      published versions (current marked)
.lake snapshots <dir>     per-version records: time, author, message, counts,
                          change summary
.lake tables <dir>        current version's db.table names + row counts
.lake info <dir>          format version, current version, snapshots, commits,
                          tables

SELECT * FROM t AT (VERSION => 2);                -- attached tables only
SELECT * FROM t AT (TIMESTAMP => TIMESTAMP '2026-10-17 12:30:36.029');
```

Incremental writes in the REPL:

```text
.lake save mylake                        # v2: full snapshot
.lake attach mylake write                # materialize v2 writable
INSERT INTO t VALUES (...);
UPDATE t SET ...; DELETE FROM t WHERE ...;
.lake commit mylake                      # v3: appends + deletes only
.lake partition mylake t by (region)     # v4: spec; later writes split per value
.lake partition mylake t clear           # new writes unpartitioned again
```

With the `encryption` feature and a non-empty `PINKDB_KEY`, the same commands
read and write an **encrypted lake**: every artifact — catalog, snapshots,
commit deltas, data files, positional delete files — is a `PNKLAKE1` container
(AES-256-GCM under a key derived once per session with Argon2id; the salt
rides in every container header, the catalog included). The
`catalog.json == snapshots/<version>.json` byte-identity invariant survives:
the manifest is sealed once per version and the same ciphertext bytes are
published to both. A keyed open of a plaintext lake, a keyless open of an
encrypted lake, and a wrong key are each refused with a distinct,
authenticated error; a plaintext lake is never silently rewritten as
encrypted. Without the variable the lake stays plaintext, byte-identically.
A keyed attach reads whole ciphertext objects for the Vortex footers (stage
1: footer-range reads over unsealed segments are a follow-up) while still
decoding nothing for `COUNT`/`MIN`/`MAX`; lean `.pink` lakes attach from the
encrypted manifests' statistics blocks without opening any data file.

With the `s3` feature every `<dir>` may be an `s3://bucket/prefix` URL — see
the next subsection. In a `pinklake` build, `.lake attach` works
statistics-only, from the stats blocks a pinklake save publishes (a lake saved
before 45k refuses with a re-save message); Vortex lakes are refused with the
rebuild-with-`vortexlake` error. See "The `pinklake` build" below.

`.lake tables`/`.lake info` also warn about tables whose referenced data file
is missing, instead of advertising data a load cannot read. Without any lake
feature, `.lake` reports `this pinkdb binary was built without a lake
feature`; without `s3`, an `s3://` location is `this pink-lake build does not
include the `s3` feature` with the rebuild command. Release `pinkdb` on
Apple Silicon (2026-10, after the S3 review fixes; pre-45i — the build table
above has the current sizes): 2,217,792 B default
(unchanged, **byte-identical** to the pre-S3 build) vs 44,587,232 B with
`--features vortexlake` (38,121,880 B stripped) and 46,565,744 B with
`--features vortexlake,s3` (39,792,376 B stripped) — the Vortex array encodings
dominate; S3 adds ≈1.98 MB raw (ureq/rustls + hmac/sha2). “Stripped” =
copying the linked binary and running the system `strip` with no flags
(`strip <file>`, macOS LLVM strip), then `stat -f%z`; `strip -x` gives the
same size. See the dependency/build note above.

### The `pinklake` build

The same lake layer without the Vortex dependency: a `pinklake` build writes
and reads **`.pink` v4 data files** (`data/<table>/<uuid>.pink`, the engine's
single-table persistence bytes) under `format_version` 4 catalogs (each file
object carries its own statistics block) — no `vortex` crate and none of its
~218 transitive crates, a 4.95 MB binary instead of 47.55 MB. It enables every
`.lake` command (save/open/attach/commit/versions/tables/info) and S3/R2
(add `,s3`; the storage client alone provides no commands), with the same
layout, incremental commits, atomic
publish order and conflict semantics. `.lake attach` is **statistics-only**:
each lean save/commit publishes a CRC-32-protected per-column statistics block
per data file (rows, exact NULL counts, bit-exact min/max as canonical strings
per type, NaN counts), collected inside the same parallel chunk-encode pass as
the data file — save cost +1 ms @1M / +17 ms @10M, the `.pink` bytes unchanged
— so attach answers exact `COUNT`/`COUNT(col)`/`MIN`/`MAX` with zero
data-file reads, and a `vortexlake` build attaches `.pink` lakes from the same
blocks (a table with positional deletes falls back for the per-column facts,
row counts stay exact). The same blocks are the pinklake build's **zone maps**:
`.lake partition` works identically (one `.pink` data file per partition
value set, values in the catalog) and a pruned scan skips a `.pink` file
entirely without opening it. A lake saved without a block refuses attach with
a re-save-the-store message; a Vortex lake is refused with the clear
rebuild-with-`vortexlake` error instead of being guessed at.

Measured trade-offs (release, Apple Silicon, sales corpus): eager `.lake open`
is slower than the `vortexlake` build — 0.023 s @1M / 0.238 s @10M printed vs
0.004 / 0.031 s; attach is the fast start instead: cold `.lake attach` +
`COUNT`/`MIN`/`MAX` takes **2.64 ms @1M / 2.31 ms @10M** (overhead ~0.1–0.2 ms,
constant in file size, 0 data-file reads) while eager open costs 29.9 /
269.5 ms. Saves are ≈ equal (0.054 s @1M / 0.321 s @10M); data files are
1.6–1.8% larger, because Vortex compresses the numeric columns better than
`.pink` v4. In-session query results are identical, and both builds answer the
statistics fast paths identically.

Cross-build compatibility is verified both ways: a lake written by the
pinklake build opens **bit-exactly** in a `vortexlake` build (the next save
migrates it to `format_version` 3 and leaves the v1 files untouched), and a
pinklake build refuses a v3 lake cleanly; a checked-in fixture pins the
pinklake-written format.

### Object storage (S3/R2)

The non-default `s3` feature makes any lake location an
`s3://bucket/prefix` URL — AWS S3, Cloudflare R2, MinIO, any S3-compatible
endpoint. It is a minimal **blocking** S3 REST client (`ureq` over rustls +
`hmac` + `sha2` for AWS SigV4; no aws-sdk, no async runtime) with path-style
and virtual-hosted addressing; the whole S3 code is compiled only with the
feature. **`s3` is pure storage: it implies neither lake build** — pair it
with the build you want, `pinklake,s3` or `vortexlake,s3`:

```sh
cargo build -p pinkdb-cli --features vortexlake,s3   # Vortex data files + S3/R2
cargo build -p pinkdb-cli --features pinklake,s3     # lean `.pink` data files + S3/R2
cargo build -p pinkdb-cli --features s3              # storage client only (no .lake commands)
```

Library: `Lake::open_url("s3://bucket/prefix")`,
`Lake::open_or_create_url`, `Lake::create_url`, `save_store_url` — local
paths keep working exactly as before. For explicit configuration (no
environment), `S3Config::new(bucket).with_prefix(..).with_region(..)
.with_endpoint(..).with_credentials(..)` feeds `Lake::{open_s3,
open_or_create_s3, create_s3}` and `save_store_s3`.

Environment (the `s3://` URL form):

| variable | meaning | R2 example |
|---|---|---|
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | credentials (required) | R2 API token key id/secret |
| `AWS_SESSION_TOKEN` | optional temporary-credential token (signed); used only with the environment's credential pair, ignored with explicit credentials | — |
| `AWS_ENDPOINT_URL` (or `AWS_ENDPOINT_URL_S3`) | non-AWS endpoint | `https://<account>.r2.cloudflarestorage.com` |
| `AWS_REGION` (or `AWS_DEFAULT_REGION`) | SigV4 region (`auto` requires a custom endpoint) | `auto` |
| `AWS_S3_FORCE_PATH_STYLE` | force path-style (`1`/`true`/`yes`/`on`) | optional; path-style is already the default with a custom endpoint |
| `PINKDB_S3_ALLOW_HTTP` | allow a plain `http://` endpoint (local tests only — credentials travel unencrypted) | — |
| `PINKDB_S3_ALLOW_DEGRADED` | accept the single-writer check-then-put fallback on endpoints that reject conditional writes (see below) | — |
| `PINKDB_S3_RETRIES` | retries after a retryable failure (default 3; `0` disables) | — |

R2 cookbook:

```sh
export AWS_ACCESS_KEY_ID=<r2-token-key-id>
export AWS_SECRET_ACCESS_KEY=<r2-token-secret>
export AWS_ENDPOINT_URL=https://<account>.r2.cloudflarestorage.com
export AWS_REGION=auto
cargo build -p pinkdb-cli --features vortexlake,s3
# in the REPL:
#   .lake save s3://my-bucket/lakes/demo
#   .lake open s3://my-bucket/lakes/demo
#   .lake versions s3://my-bucket/lakes/demo
#   .lake tables   s3://my-bucket/lakes/demo
#   .lake info     s3://my-bucket/lakes/demo
```

- **Conditional writes are the conflict machinery.** A snapshot is published
  with `If-None-Match: *` (a published version is never replaced; a 412 is
  the familiar `snapshot N already exists … refusing to replace` conflict),
  and `catalog.json` is replaced with `If-Match: <etag>` from the read made
  moments earlier (a 412 is `catalog.json changed while this save was in
  flight … reopen the lake`). AWS S3 and Cloudflare R2 evaluate those
  headers **atomically with the write commit** (R2 documents `If-Match` /
  `If-None-Match` on `PutObject` and the atomicity of the conditional
  evaluation), so concurrent single-writer saves cannot fork the version
  chain: `create`/`save` are class-CRUD-safe for one live writer, with the
  same conflict error text as the local hard-link/rename path. The lake
  additionally re-reads and re-validates the catalog right before the
  replace, and on an endpoint that cannot enforce conditionals it refuses
  the write outright (see the next two bullets) instead of guessing.
- **Endpoints without conditional writes are refused by default.** If a
  conditional request is rejected with HTTP 501 or a 400
  `NotImplemented`/`InvalidRequest` (an old S3-compatible server), writes are
  refused with `this endpoint does not support conditional writes;
  concurrent-safe saves are unavailable. Set PINKDB_S3_ALLOW_DEGRADED=1 (or
  S3Config::allow_degraded(true)) to accept a SINGLE-WRITER guarantee with a
  small race window`. Reads keep working. The refusal is sticky: once an
  endpoint has proven it rejects conditionals, later writes fail immediately
  without touching the bucket (a save that hits it mid-flight may have
  uploaded already-written data files — unreferenced leftovers, harmless like
  a crash; no snapshot or catalog is ever replaced).
- **The degraded opt-in is single-writer only.** With
  `S3Config::allow_degraded(true)` / `PINKDB_S3_ALLOW_DEGRADED=1` the client
  falls back to check-then-put for `If-None-Match`/`If-Match`: it HEADs or
  GETs the current ETag, compares, writes, then **reads the object back and
  byte-compares** it (a mismatch is a conflict, never a silent success). The
  lake additionally re-checks the catalog before the replace and verifies it
  after, restoring `snapshots/<v>.json` from the authoritative
  `catalog.json` bytes if a racing writer transiently replaced it. That keeps
  the published pair consistent in the interleavings the fallback can reach,
  but it cannot make a save atomic: **concurrent writers are unsupported with
  the opt-in** — one save can lose with a `Conflict`, and a prior publish can
  be replaced by a later one (never left as a catalog/snapshot mismatch). Use
  it only for a strictly single-writer lake.
- **Errors are clear:** HTTP 403 → `object storage access error at <lake>`
  (credentials/permissions, including the missing-credentials case before any
  request); 404 on the catalog → `not a pink-lake directory`, while a 404 on
  a bucket the client has not yet seen answer successfully (the body says
  `NoSuchBucket`, or a bodiless HEAD is followed by a bucket probe) → `the
  bucket does not exist (or is not visible to these credentials)`; a missing
  snapshot → `has no snapshot version N`; a deleted data object is reported
  by `.lake tables`/`.lake info` and by `load` with the local
  `loading data file "…"` context; 412 → `write conflict`; `409 Conflict` on
  a conditional PUT is retried up to 3 times (a concurrent delete raced the
  write), everything else is an I/O error naming the S3 code. The host clock
  must be within S3's allowed skew (~15 min) for SigV4. Every request has one
  **120 s whole-request timeout** (connect + transfer). **Retries** use
  exponential backoff with jitter (default 3, `PINKDB_S3_RETRIES`):
  `429`/`503` (`SlowDown`/`ServiceUnavailable`, returned before S3 applies a
  request) are retried for every request; `500`/`502`/`504` and transport
  errors only for requests that are safe to repeat (reads, deletes, the
  unconditional data-file PUTs). A conditional PUT that failed ambiguously is
  never retried — it may have landed, and a retry would report its own write
  as a conflict — and a redirect (301 `PermanentRedirect`) is an error (fix
  the endpoint/region). After a failure, reopen the lake before retrying the
  save.
- **After a mid-save I/O error, reopen and inspect before retrying.** A
  failed or interrupted save can leave three safe-but-visible states: the
  data files are unreferenced (ignored); `snapshots/<v>.json` exists while
  `catalog.json` still points at the previous version (an unpublished stray —
  `.lake versions` omits it, and the next save refuses with `snapshot N
  already exists … remove the stray snapshot and retry`); or the catalog PUT
  landed and its response was lost, in which case the new version is
  published and `catalog.json` is byte-identical to its snapshot. So: reopen
  the lake and run `.lake versions`, compare it with what the error said, and
  only then retry — never blindly re-run a save on the old handle (it will
  fail with the version conflict, which is the safe outcome).
- **What is not implemented:** multipart uploads — one `PutObject` per
  object, so a single table's data file must be ≤ 5 GiB (the S3 single-PUT
  limit; the client refuses larger bodies with a clear error before sending
  them and caps reads at the same size); no bucket creation, no lifecycle
  rules, no object versioning, no server-side copy, no lake-side GC (old
  versions/data objects stay, exactly like local), and lake data is plaintext
  unless the `encryption` feature and `PINKDB_KEY` are used (then each object
  is one encrypted container, written with the same single `PutObject`, and
  the conditional `If-None-Match`/`If-Match` CAS evaluates on the ciphertext
  bytes exactly as on plaintext). A dotted bucket name needs **path-style**
  addressing (virtual-hosted TLS certificates do not cover
  `dots.in.bucket.<endpoint>`;
  a custom endpoint already defaults to path-style and
  `AWS_S3_FORCE_PATH_STYLE=1` forces it for AWS). v1 `.pink` data files
  stored in an S3 lake still load: the engine's loader needs a path, so the
  object is downloaded to a unique temp file (created with `create_new`, so a
  planted symlink is never followed; partial files are removed on error) and
  removed when done.
- **Live test (opt-in):** `PINKDB_S3_LIVE=1 PINKDB_S3_BUCKET=<bucket>
  PINKDB_S3_PREFIX=<prefix> AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=…
  AWS_ENDPOINT_URL=… AWS_REGION=auto cargo test -p pink-lake --features s3
  --test s3 live_s3_roundtrip -- --nocapture` runs create/save/open/conflict
  against the real service (it is skipped by default and leaves objects under
  the prefix — there is no GC).

### Testing against a real S3 server (Docker)

The in-repo mock (`mock_s3.rs`) is the hermetic protocol suite — it verifies
SigV4 and the conditional-write machinery in-process. For an independent
end-to-end check over a real socket, `crates/pink-lake/tests/s3_docker.sh`
manages its own S3 server container, drives the release CLI through it, and
removes the container again (`KEEP=1` leaves it for the next run to reuse).
`SERVER=` picks the server (default `s3mock`):

```sh
crates/pink-lake/tests/s3_docker.sh                    # adobe/s3mock (lenient)
SERVER=seaweedfs crates/pink-lake/tests/s3_docker.sh   # chrislusf/seaweedfs (strict)
SERVER=rustfs    crates/pink-lake/tests/s3_docker.sh   # rustfs/rustfs (strict)
```

Equivalent manual container recipes (SeaweedFS needs an identities file —
`crates/pink-lake/tests/s3_docker_notes.md` has its content; the script writes
one into the run's temp dir):

```sh
# s3mock — no SigV4 verification, non-atomic conditionals
docker run -d --name pinkdb-s3mock-itest -p 127.0.0.1:9090:9090 \
  -e COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS=pinklake \
  adobe/s3mock:latest
# seaweedfs — with -s3.config identities it verifies SigV4 (unsigned PUT -> 403)
docker run -d --name pinkdb-seaweed-itest -p 127.0.0.1:8333:8333 \
  -v /tmp/seaweedfs-s3.json:/etc/seaweedfs/s3.json:ro \
  chrislusf/seaweedfs:latest server -s3 -dir=/data -s3.config=/etc/seaweedfs/s3.json
# rustfs — verifies SigV4 (unsigned PUT -> 403)
docker run -d --name pk-rustfs -p 127.0.0.1:9100:9000 \
  -e RUSTFS_ACCESS_KEY=rustfsadmin -e RUSTFS_SECRET_KEY=rustfsadmin \
  rustfs/rustfs:latest
```

Capability matrix (probed by the script before any lake write; measured on
this machine 2026-10-14):

| server | verifies SigV4? | atomic conditional PUTs? | one-winner race assertion |
|---|---|---|---|
| s3mock | no (unsigned accepted; documented) | no (check-then-act) | reported |
| seaweedfs + `-s3.config` | **yes** (unsigned/wrong-key → 403) | **yes** | **enforced** |
| seaweedfs default config | no (anonymous; the harness measures and reports it) | yes | enforced conditionals, SigV4 reported |
| rustfs | **yes** (unsigned/wrong-key → 403) | **yes** | **enforced** |

The CLI environment the script pins:

```sh
AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=…   # s3mock: any dummy; seaweedfs anykey/anysecret; rustfs rustfsadmin/rustfsadmin
AWS_ENDPOINT_URL=http://127.0.0.1:<port> AWS_REGION=us-east-1
PINKDB_S3_ALLOW_HTTP=1     # plain http:// endpoint: local test servers only
```

`PINKDB_S3_ALLOW_HTTP=1` is required for an `http://` endpoint (credentials
travel unencrypted); `PINKDB_S3_ALLOW_DEGRADED` must stay **unset** — all
three servers enforce the conditional headers, so the safe path applies, and
the script proves that before any lake write (`If-None-Match: *` on an
existing key → 412; a wrong `If-Match` → 412). The script then runs a
typed-CSV round-trip with a fresh-process `.lake open` + exact value
comparison (NULL, `-0.0`, BIGINT, DATE/TIMESTAMP, multibyte) and a REST
listing of the published objects; a 3-writer × 3-round race plus a
deterministic slow-vs-quick save racing for the *same* version
(win-xor-conflict per attempt, gapless version chain, exactly one winner for
the controlled version); and the snapshot-exists refusal (a planted stray
snapshot, then a clean save after removing it). Ports/names/row counts are
environment knobs in the script header (`SERVER`, `PORT`, `CONTAINER`,
`KEEP`, `PINKDB_BIN`, `SKIP_BUILD`, `WROWS`, `SLOWROWS`).

**Bucket note:** the CLI never creates buckets, so the harness creates
`BUCKET` (default `pinklake`) with a `CreateBucket` up front — signed for
seaweedfs/rustfs, unsigned for s3mock — tolerating "already exists". Point
`BUCKET=` at an existing bucket to skip that; bucket creation from the CLI is
future `INSTALL` work, out of scope here.

**Measured s3mock caveat:** s3mock checks a conditional PutObject's
precondition *before* taking the store's per-object lock
(`ObjectController.putObject` → `verifyObjectMatching` → `putObject`), so two
truly concurrent conditional PUTs can both be answered 200 — unlike AWS S3 /
R2 / SeaweedFS / RustFS, which evaluate the condition atomically with the
write commit. The script probes this (racing `If-None-Match: *` and
`If-Match` PUTs) and, on a server that cannot uphold it, reports the race's
duplicate publications as the measured server limitation instead of failing;
on `SERVER=seaweedfs|rustfs` (atomic + SigV4-verifying) the one-winner race
assertion, the raced `catalog.json == snapshots/<v>.json` byte check, and the
unsigned-must-403 signature check are **hard failures**. Raw evidence, the
source reference, and the full assert/caveat matrix:
`crates/pink-lake/tests/s3_docker_notes.md`.

**MinIO note:** MinIO images cannot be pulled anonymously right now —
`docker pull minio/minio` fails with `pull access denied … may require
'docker login'` and `quay.io/minio/minio` answers `401 UNAUTHORIZED` — which
is why s3mock is the default recipe and SeaweedFS/RustFS are the strict
witnesses. Any S3-compatible endpoint works the same way (`CONTAINER=`,
`PORT=`); for a real AWS/R2 bucket use the opt-in `PINKDB_S3_LIVE=1` test
above, which is also where the atomic-conditional one-winner guarantee is
checked against a real service.

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
`--idle-timeout SECS` (default 300; `0` = never), `--transport sync|async`
(default `sync`; `async` is the tokio I/O transport and needs a build with
the `async-server` feature) and `--allow-load-extension` (off by default; see
Security posture). The server
prints its listening address on startup and runs until terminated.

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
connection stays open. The protocol version is **9** (v2 added the
`BatchDone` terminator, v3 `LoadedExtension`, v4 `Checkpointed`, v5
prepared statements, v6 DECIMAL/INTERVAL, v7 `AlteredTable`, v8 `Called`,
v9 the `Login`/`LoggedIn` frames and the RBAC `Done` kinds).

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
server, but they identify the *server*, not the client. Without a trust
anchor, anyone who can open a TCP connection to the port still reaches the
login prompt, so bind `127.0.0.1` (the default) or a trusted network, and do
not expose the port to the internet.

`LOAD EXTENSION` is additionally refused for remote clients unless the
server runs with `--allow-load-extension` (library:
`Server::allow_load_extension(true)`): the statement makes the server process
load and run a native library from its own filesystem, and even with RBAC
that is a host-level capability (an authenticated session also needs
`ADMIN`). WAL replay at startup still re-loads extensions a durable database
already recorded.

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
`json_array_length`, `json_object`, `json_array` over JSON text in `VARCHAR`)
and `datetime` (`strftime`, `strptime`, `epoch`, `epoch_ms`, `dayofweek`,
`dayofyear`, `week`, `last_day`, `age`, `make_date` over naive
`DATE`/`TIMESTAMP`, pinned to DuckDB 2.0 α semantics — `extensions/datetime`
carries the full matrix and divergences) are CLI **default features**;
`--no-default-features` keeps the binary lean. `string` ships as an in-repo
loadable extension (`extensions/string`: `make -C extensions/string`, then
`LOAD EXTENSION '<path>'`), with `math` landing in the same series.

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
ownership, NULL handling, the three `pinkdb_extension_*` exports — from
typed Rust bodies. Extension authors write no `unsafe` and no C:

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
```

Parameters and returns are the ABI v1 types (`bool`, `i32`, `i64`, `f64`,
`&str`/`String`, `Date`, `Timestamp`), `Option<T>` makes NULL explicit, and
`Result<_, String>` returns errors. The same declaration drives both loading
models: a native cdylib (`LOAD EXTENSION '<path>'`) and wave 49's linked
registration (`pinkdb_register_linked()`, the only model on wasm), so one
source covers every target. The SDK depends on the loader crate, never the
other way around: no pinkdb binary grows because of it.

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
```

`ext-sdk/rust/README.md` walks through the macro, the type/`Option`/`Result`
rules, the volatility contract, the error-code mapping (the C ABI carries no
error text) and the crate's own round-trip tests (native `LOAD EXTENSION`,
linked registration, durable replay); its header copy is pinned by the same
`cargo test` check.

### Extension repository

Built extensions are published to a hosted catalog (Cloudflare Worker + R2,
format v1) at <https://pinkdb-ext-repo.defaultcms.workers.dev>: `GET
/index.json` is the catalog and `GET /artifacts/…` serves the bytes. The
`ext-repo/` directory holds the format spec (`ext-repo/FORMAT.md`), the
publisher (`publish.sh`/`publish.py`), the verifier (`verify.sh`), and the
worker source. The first published artifact is the C SDK example `math_ext`
0.1.0 (`darwin-arm64`).

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
- **Load:** `.install` finishes by `LOAD EXTENSION`-ing the artifact into the
  current session; the extension then behaves exactly as described below
  (process-level, WAL replay re-loads it by path).

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
(`WHERE total > 500`), a window-key rework (the `OVER` rows) and parallel Vortex
lake writes (see below). The typed kernels keep semantics pinned exactly
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
ms, `.lake save` 632–758 → 296–340 ms (format_version 3: 8,192-row Vortex
blocks), `.lake open` 618 → 100 ms, data 29.7 → 25 MB, store teardown 93 → 16
ms. The techniques: two-pass hash aggregation (group ids, then one typed loop
per aggregate) with typed single-column keys; DISTINCT through that
aggregation; parallel window partitioning/sorting/assembly; prefix-keyed
sorting; moving (not copying) bulk-loaded chunks; streamed + parallel Vortex
encode and parallel bulk decode; parallel teardown. Every result is pinned
to the old semantics (duckdiff 60/60 + the 1M battery 64/64).

### Lake format comparison (Vortex vs DuckLake)

`bench/lake_bench.py` (stdlib-only Python, one file) runs the same 1M-row CSV
through both lake formats and the same four queries, and prints the raw
numbers as JSON:

```sh
cargo build --release -p pinkdb-cli --features vortexlake
python3 bench/lake_bench.py --rows 1000000 --runs 5
```

Both arms store the table as `lake.sales`, so the SQL text is identical
(pink-lake is told to import into a database named `lake`; DuckLake attaches
a catalog named `lake`). Measured 2026-10-14 on this machine (macOS 26.6.2,
Apple Silicon, 10 CPUs, load average ≈3 during the run; `pinkdb`
44,567,680 B with `--features vortexlake`; `duckdb` v1.5.5 with the `ducklake`
extension already cached, `INSTALL` ≈1 ms):

| Metric (1M rows) | pink-lake (Vortex) | DuckLake (DuckDB) |
|---|---|---|
| ingest wall (s) | 0.243 (import 0.031 + lake save 0.174) | 0.125 (CTAS 0.091) |
| stored total | 3.12 MB (4 files) | 13.26 MB (2 files) |
|  data | 3,117,735 B Vortex (1 file) | 9,313,736 B Parquet (1 file) |
|  metadata | 643 B (catalog 272 B + 2 snapshots) | 3,944,448 B SQLite catalog |
| cold open printed (s) | 0.061 (`.lake open`) | n/a (ATTACH ≈3 ms, lazy) |
| cold end-to-end wall (s) | 0.077 (start→open→first query→exit) | 0.036 (start→attach→query→exit) |
| `COUNT(*)` best-of-5 (ms) | <1 | 7 (printed; 36 as fresh-process wall) |
| GROUP BY region + 5 aggs (ms) | 10 | 12 (printed; 41 as fresh-process wall) |
| `WHERE total > 500` (ms) | 5 | 11 (printed; 39 as fresh-process wall) |
| ORDER BY total DESC LIMIT 10 (ms) | 3 | 12 (printed; 41 as fresh-process wall) |

Both CLIs print `real X.XXX` (1 ms resolution; 0.000 means <1 ms), both
numbers above are best-of-5, and the run-to-run spread is ±1 ms at this
resolution. A 10M-row run (`--rows 10000000`, generated
deterministically, 243 MB CSV) shows DuckLake winning the larger scans:
ingest 0.368 s vs 1.943 s, stored 97.1 MB (93.1 MB Parquet + 3.9 MB catalog)
vs 31.1 MB Vortex, cold open 0.036 s vs 0.724 s, and query best-of-5
group-by 37 ms vs 93 ms, filter 24 ms vs 48 ms, order-by 28 ms vs 30 ms —
pink-lake keeps only `COUNT(*)` (<1 ms vs 8 ms). No cherry-picking: at 10M
DuckLake wins ingest, cold start and three of the four queries.

**Wave 40 refresh (Oct 2026):** `.lake save` now runs the Vortex compression
cascade on a driven worker pool instead of inline on the main thread, and the
written files are **byte-identical** (sha256 unchanged): 1M save 0.180 s →
**0.088 s**, 10M 1.562 s → **0.76–1.22 s** (2.0–2.7×). At 10M, pink-lake's
in-memory queries moved too: filter 50 → **23–25 ms** (now ~equal to
DuckLake's 24 ms), order-by 36 → 24–32 ms, group-by 100 → **68–74 ms** (still
~1.7× DuckLake's 37 ms). Targets were ≤30 ms filter (met) and ≤60 ms
group-by (missed; 444 ms single-thread at the measured 6.0× parallel ceiling
of this 4P+6E M4, merge 3 ms) — SoA accumulators are the next round's
breadcrumb. At 10M, lake save is still behind DuckLake's ingest (0.76 s vs
0.37 s), and the `.lake open` decode (~0.63 s) is the last obvious lake gap
(bulk typed decode is the other breadcrumb).

**Wave 45 refresh (Oct 2026, sub-waves 45c/45d):** `.lake open` at 10M went
110–126 → **30 ms** printed (open CPU 540 → 190 ms, peak RSS 996 → 352 MB;
13 → 4 ms at 1M) via a zero-copy file read, shared decoded chunks and
per-chunk varchar interning; 10M ingest wall 0.482 s (save 0.275 s), storage
25.1 MB vs DuckLake's 92.6 MB. `.lake attach` (wave 45d) is the lazy
counterpart: footer-only (no column decode), exact `COUNT`/`MIN`/`MAX` from
the stored statistics, and lazy per-table materialization for anything else.
Cold-process runs (fresh process → open/attach → one query → exit; best-of-5,
2026-10-15, same host — the `COUNT(*)`/attach rows are from 45d's
side-by-side run, the scan-query rows from 45c's official run):

| cold process (ms) | pink `.lake attach` | pink `.lake open` | DuckLake |
|---|---|---|---|
| @10M `COUNT(*)` | **4.94** | 34.76 | 34.92 |
| @10M `MIN(total)` / `MAX(total)` | **4.90** | — | — |
| @10M group-by / filter / order-by | fallback = decode, within noise of open | 72.2 / 62.2 / 77.2 | 64.3 / 52.2 / 55.4 |
| @1M `COUNT(*)` | **4.62** | — | — |

Attach overhead (catalog + manifest + footers) is ~1.2–1.7 ms, constant in
file size. `.lake open` is now within 1.1–1.4× of DuckLake on the three scan
queries (was 2.8–4.2× behind); the remaining gap is the scan path itself —
stage 2 (projection pushdown) shipped as 45e right below, and stage 3
(filter/zone-map pruning) is the follow-up.

**Wave 45e refresh (Oct 2026):** projection pushdown for `.lake attach` — an
attached scan decodes only the columns the query references, into a per-column
lazy cache (a missed reference is a hard internal error, never a silent NULL).
Cold-process @10M, fresh process → attach → one query → exit, best-of-5,
2026-10-15, DuckLake measured alongside:

| cold process, @10M (ms) | pink pre-45e | pink 45e | DuckLake |
|---|---|---|---|
| `COUNT(*)` — 0 columns decoded | ~5 | ~5 | 34.9 |
| `WHERE total > 500` — 1 column | 60.6 | **41.9** (1.45×) | 51.9 |
| `ORDER BY total DESC LIMIT 10` — 2 columns | 74.0 | **60.5** (1.22×) | 55.4 |
| group-by — 2 columns | 72.8 | **68.9** (1.06×) | 63.8 |

@1M: filter 10.7 → 8.9 ms, top-10 12.5 → 11.1 ms. The cold scorecard vs
DuckLake is now: `COUNT(*)` 6.8× faster, filter 1.24× faster, order-by and
group-by ~1.1–1.2× behind.

**Fairness caveat — this is "same data, same queries, different
architectures", not an apples-to-apples product comparison.** `.lake open`
eagerly loads the whole table from Vortex back into pinkdb's in-memory
engine, so pink-lake's per-query lines are in-memory scans and the load cost
is the separate open row; DuckLake is lazy — ATTACH only opens the SQL
catalog and every query scans Parquet at query time, so its printed query
times include the scan and its fresh-process wall adds ~30 ms of process
start + LOAD + ATTACH at 1M on top of the query. `.lake attach` is pink-lake's
lazy mode (footer-only; statistics-only answers), but a query the statistics
cannot answer still decodes the whole table (projection pushdown shipped in
45e, so only the columns it references decode; filter/zone-map pruning is not
implemented yet). DuckLake is a full
lakehouse format (transactional SQL
catalog, MVCC-ish snapshots, time travel) and its catalog DB grows with
every write/snapshot (this run is a single ingest), while pink-lake
publishes a full snapshot per save and keeps old versions' files. No cache
was dropped on either side, and both engines returned matching results
(float sums differ in the last few digits from different summation orders;
ties in ORDER BY have no stable order on either engine).

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
- Three-part column references (`db.table.col`) are unsupported; use an alias
  (`SELECT x.col FROM db.table AS x`)
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

- Lake (`vortexlake`/`pinklake`/`s3` features): every `.lake save` writes a full snapshot of
  every table; old versions and orphaned data files are never garbage
  collected; the lake is reached through `.lake open` (whole store into memory)
  or `.lake attach` (read-only, one version pinned, footer-only: exact
  COUNT/MIN/MAX from stored statistics, anything else lazily decodes the one
  table it touches — no time travel within a session; projection pushdown
  shipped in 45e, so only referenced columns decode, but there is no
  filter/zone-map pruning yet); attached tables are session-local, not saved,
  and mutations over them are refused (mutating SQL over them on a durable
  store is refused too, since the log could not replay them); lake files are
  plaintext; one S3 object is capped at 5 GiB (no multipart uploads) and
  credentials come only from the environment or explicit config (no
  profiles/IMDS/SSO); the `pinklake` build writes and reads only
  `.pink` data files (v4 catalogs), attaches them
  statistics-only (exact `COUNT`/`MIN`/`MAX` from the manifest's stats blocks;
  `.pink` has no projected reader, so a fallback decodes the whole table),
  supports `.lake attach … write` + `.lake commit`, and refuses a Vortex lake
  with the rebuild-with-`vortexlake` error
- Extensions repository: artifacts are verified by SHA-256 from the catalog
  (no signatures yet), and only `darwin-arm64` builds are published
- Stored procedures (wave 44): a body's expressions bind at `CALL` time (the
  catalog may change between calls), so a mistyped condition is a call-time
  error, not a create-time one; a body cannot `USE`, manage transactions,
  `CHECKPOINT`, `LOAD EXTENSION`, touch databases or materialized views, or
  create/drop procedures; recursion is capped at 32 levels; a durable store
  refuses a procedure that reads a session-local view or calls a
  loadable-extension function (their replay could not be honest); there is no
  `SHOW PROCEDURES` (a missing call reports the missing name), no OUT/INOUT
  parameters and no exception handlers yet
- JavaScript procedures: the `js` feature links the `pinkdb-js` sandbox
  *contract* only — `CREATE PROCEDURE … LANGUAGE JAVASCRIPT` refuses in this
  release (the engine binding is the recorded follow-up); a JavaScript
  definition loaded from a `.pink` file stays inert and `CALL` reports the
  same refusal
- Known gaps and their scheduled waves are tracked in `docs/GAPS.md`

Post-v1 roadmap: C/Python/Go language bindings, Parquet reader (see
PLAN.md). At-rest encryption shipped in wave 25 behind the `encryption`
feature; the network server (`pinkdb-server`) shipped in wave 24; the lake
layer (`pink-lake`, CLI `vortexlake` feature) shipped in wave 34, became
Vortex-first in wave 35 (v1 `.pink` lakes still load; open + save migrates)
and gained S3/R2 object storage in wave 36 (`s3` feature; `s3://` locations,
conditional-write conflict detection), read-only `.lake attach` (footer-only;
exact `COUNT`/`MIN`/`MAX` from stored statistics; lazy materialize) in wave 45d
and projection pushdown for attached scans in wave 45e; `.pink` v4 (wave 45h)
brought single-file saves to roughly the Vortex size (1.015× at 1M, 1.067× at
10M), and wave 45j added LZ frames (tag 8) that shrink text corpora 3–5×. The
`pinklake` build (`.pink` v4 lake data files, no `vortex` dependency,
4.95 MB vs 47.55 MB CLI) shipped in wave 45i, and wave 45k gave it exact
attach fast paths (a CRC-32-protected per-column statistics block published in
the manifest; zero data-file reads for `COUNT`/`MIN`/`MAX`) together with a
`.pink` v4 bit-pack correctness fix (u64 accumulators dropped high bits for
widths > 56). Durable `.pink` databases became multi-process in wave 45m — a
per-operation write lease, lock-free epoch refresh, WAL v3 and encrypted
container v2 — with stage 5 (optimistic disjoint-table commits) deferred and
the stability-hardening round shipped as wave 45n (chaos soak, lock-file
identity, WAL fuzz). Remaining lake follow-ups (not
committed): projection pushdown for `.pink`, per-chunk statistics +
filter/zone pruning (stage 3), and the parked cold order-by/group-by items.
Authentication and TLS are deferred.

## Development

```sh
cargo build            # workspace
cargo test             # 100+ unit + end-to-end tests
cargo test -p pinkdb-server                          # protocol + server + durable tests
cargo test -p pinkdb-cli --features client           # remote-mode CLI tests
cargo test -p pinkdb-cli --features vortexlake       # Vortex lake round-trip + CLI tests
cargo test -p pinkdb-cli --features vortexlake,s3    # + S3/R2 over a hermetic mock S3
cargo test -p pinkdb-cli --features pinklake         # lean `.pink` lake round-trips (no Vortex)
cargo test -p pinkdb-cli --features pinklake,s3      # + S3/R2 over a hermetic mock S3
cargo test -p pinkdb-cli --features s3               # storage client alone: no .lake commands
crates/pink-lake/tests/s3_docker.sh                  # real S3 round-trip/race in Docker: SERVER=s3mock (default)|seaweedfs|rustfs (not cargo test)
cargo build -p pinkdb-cli --features vortexlake      # REPL with .lake commands
cargo build -p pinkdb-cli --features vortexlake,s3   # REPL with .lake s3:// URLs
cargo build -p pinkdb-cli --features pinklake        # REPL with the lean `.pink` lake
PINKDB_CLI_BIN=./target/release/pinkdb cargo test -p pinkdb-cli --features vortexlake  # release CLI (overflow checks off)
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

Three GitHub Actions workflows in `.github/workflows/`, GitHub-hosted runners
only:

| Workflow | Trigger | Jobs / steps |
|---|---|---|
| `ci.yml` | push to `main`, every pull request | `build-test` — `cargo fmt --check`, `cargo build`, `cargo test -q`, `cargo clippy --all-targets -- -D warnings`, the feature tests (`pink-lake --features s3` / `s3,vortexlake`, `pinkdb-cli --features vortexlake,s3` / `pinklake` / `pinklake,s3` / `s3` / `encryption` / `client` / `repo`, `pinkdb --features encryption`), clippy on every feature set, the SIGKILL kill loops (plain, multi-db, targeted, encrypted), the wasm check, then the release CLI uploaded as an artifact; `duckdiff` — downloads that CLI, installs the DuckDB CLI and runs `python3 bench/duckdiff.py`; `bindings` — release `pinkdb-ffi`, the C example (`make -C crates/pinkdb-ffi hello`), the Python `unittest` suite, and `gofmt` + `go vet` + `go test -race`; `sdk-smokes` — `make -C ext-sdk/c` + the C/C++, Go and Rust SDK smoke scripts; `s3-docker` — `crates/pink-lake/tests/s3_docker.sh` (s3mock in Docker; a hard gate); `macos` — workspace, lake and encryption tests plus the Python binding on macOS; `msrv` — `cargo check` of the whole workspace on Rust 1.97 (the edition-2024 MSRV); `audit` — `cargo audit`; `docs` — `website/build.py` and the roadmap-page sync check. Actions are pinned to commit SHAs |
| `deploy-docs.yml` | push to `main` touching `website/**` (or the workflow itself), manual dispatch | `python3 website/build.py`, then `wrangler deploy` from `website/` (the `pinkdb-docs` Worker) |
| `publish-extensions.yml` | manual dispatch only | builds the C/C++ SDK examples (and any `extensions/*/` built by their own Makefile), runs both SDK smokes, fetches the live catalog, skips already-published coordinates, publishes new artifacts with `ext-repo/publish.sh` (wrangler → R2) and prints the artifact/catalog URLs |

### Required repository secrets

Set both under *Settings → Secrets and variables → Actions*. Two names are
accepted for each — `CF_API_TOKEN` / `CF_ACCOUNT_ID` (tried first; used by this
repository's organization) or `CLOUDFLARE_API_TOKEN` / `CLOUDFLARE_ACCOUNT_ID`
(the fallback):

| Secret | Value | Used by |
|---|---|---|
| `CF_API_TOKEN` (or `CLOUDFLARE_API_TOKEN`) | a Cloudflare API token scoped to **Workers Scripts: Edit** (docs Worker) and **R2: Edit** (extension artifact + catalog uploads) | `deploy-docs.yml`, `publish-extensions.yml` |
| `CF_ACCOUNT_ID` (or `CLOUDFLARE_ACCOUNT_ID`) | the account that owns the `pinkdb-docs` / `pinkdb-ext-repo` Workers and the `pinkdb-extensions` R2 bucket (find it in the Cloudflare dashboard); store it as a secret, never in the repository | same |

### Publishing extensions by hand

Actions → **Publish extensions** → *Run workflow*. Every input is optional:

- `name` — publish a single discovered extension, e.g. `cpp_ext`;
- `version` — artifact version (default: the version in `crates/pinkdb/Cargo.toml`);
- `platform` — platform tag; it must match the runner (default derived from it:
  `darwin-arm64` on `macos-latest`) — publishing for another platform means
  changing `runs-on`, not forcing a tag.

With no inputs the workflow publishes every discovered artifact. Publishing is
immutable: coordinates already in the live catalog are skipped (the publisher,
`ext-repo/publish.py`, refuses them on its own too), and every catalog entry
carries the artifact's SHA-256 and size. There is no cryptographic signing yet —
checksums are the integrity mechanism (`ext-repo/verify.sh` checks a download
against the catalog); the workflow has a placeholder step documenting the
planned signing shape rather than faking one.

### Verifying the workflows

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
