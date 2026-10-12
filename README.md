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
  → PEG parser (or hand-written lexer + recursive descent) → AST
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
| `pinkdb::sql` | PEG grammar + hand-written lexer/recursive-descent parser, AST, binder |
| `pinkdb::plan` | Logical plan tree |
| `pinkdb::optimizer` | Constant folding, predicate pushdown (incl. across joins) |
| `pinkdb::exec` | Vectorized expression eval, hash aggregate, hash join, operators (rayon-parallel) |
| `pinkdb::persist` | `.pink` single-file columnar persistence, v2 (databases + RLE/dictionary/bitpack; v1 files load) |
| `pinkdb::db` | Multi-writer `Database`/`Connection` over `Arc<RwLock<PinkDb>>` |
| `pinkdb-cli` | rustyline REPL, dot-commands, box-drawn output |
| `pinkdb-server` | TCP server + blocking client (framed binary protocol, thread-per-connection); no external deps |

Design rules: Rust edition 2021, zero async, no unsafe. Dependencies are
`csv` + `rustyline`, plus `rayon` for parallel execution and `peg` for the
SQL parser. The server crate adds no external dependencies.

SQL is parsed by a `peg`-generated grammar; the hand-written lexer +
recursive-descent parser stays selectable via `PINKDB_PARSER=legacy`. A
committed differential corpus plus a seeded randomized differential test
(`cargo test`) check that both parsers accept the same inputs with equal ASTs
and reject the same inputs; PEG error messages are positional and worded
differently.

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
SELECT [DISTINCT] expr [AS alias], ...
FROM t [JOIN u ON t.x = u.y]      -- INNER / LEFT [OUTER] / RIGHT [OUTER]
                                  -- equi-join (single equality)
  | FROM db1.t                    -- qualified tables resolve in that database
  | FROM (SELECT ...) AS s         -- derived tables (alias required)
  | WITH w AS (SELECT ...) ...     -- CTEs, incl. WITH w(a, b) AS column aliases
WHERE expr                        -- incl. IS [NOT] NULL, BETWEEN,
                                  -- IN (subquery|list), EXISTS (subquery)
GROUP BY ... HAVING ...
ORDER BY expr [ASC|DESC], ...     -- NULLS LAST both ways; ORDER BY 2 positional
LIMIT n;
-- Scalar subqueries `(SELECT ...)` anywhere an expression goes
-- (uncorrelated only: exactly 1 row × 1 col, 0 rows → NULL).
```

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

- Aggregates: `COUNT(*)` / `COUNT(x)`, `SUM`, `AVG`, `MIN`, `MAX`
- Window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, aggregates `OVER`
  (`PARTITION BY` / `ORDER BY` / `ROWS` frames; default frame is cumulative
  with ORDER BY, whole partition without; no `RANGE` frames, no nesting)
- Scalar functions: `abs`, `round` (1–2 args), `upper`, `lower`, `length`, `substr`
- `CAST(x AS type)` (numeric tower both ways, bool↔numeric, varchar parse/format,
  date↔timestamp; bad input errors), `COALESCE(a, b, ...)` (common type),
  `LIKE`/`NOT LIKE` (`%`/`_` patterns)
- `CASE WHEN c THEN v ... [ELSE e] END` + simple `CASE x WHEN v` form
  (NULL conditions never match; result types unify like COALESCE)
- Set ops: `UNION [ALL|DISTINCT]` (numeric types unify; ORDER BY/LIMIT after
  the set operation apply to the whole result — branch-level clauses need
  parentheses, unsupported in v1); `CREATE VIEW v [(cols)] AS ...` (session-local,
  rebound per use), `DROP TABLE` / `DROP VIEW`
- Types: `BOOLEAN`, `INTEGER` (i32), `BIGINT` (i64), `DOUBLE` (f64), `VARCHAR`,
  `DATE` (days since epoch), `TIMESTAMP` (microseconds, naive, no time zone)
- Literals: `DATE '2024-02-29'`, `TIMESTAMP '2024-03-15 12:30:45.5'`
- Date ops: `date ± integer` (days), `date - date` → BIGINT days;
  `year/month/day/hour/minute/second(x)`, `date_trunc(part, x)`
- CSV auto-detects `YYYY-MM-DD` → DATE and `YYYY-MM-DD HH:MM:SS[.f]` → TIMESTAMP
- Coercion: INTEGER → BIGINT → DOUBLE; `int / int` → DOUBLE (DuckDB rule)
- Three-valued logic: NULL fails `WHERE`; `COUNT(x)`/`SUM`/`AVG` skip NULLs;
  NULL group keys group together; NULL join keys never match; LEFT/RIGHT
  joins NULL-extend the unmatched side's columns
- Integer division/modulo by zero is an error; `0.0` float division is IEEE
- `SUM` over integers accumulates in i64 (wraps on overflow)

REPL dot-commands: `.help`, `.quit` / `.exit`, `.databases`, `.tables`,
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
columnar format (v2: `PINKDB02`); `PinkDb::open("x.pink")` loads it back
(`.save` / `.open` in the REPL). v1 files (`PINKDB01`, a flat table list)
still load: their tables become the single `main` database. Views are
session-local and never persisted. Round-trips are bit-exact (including
`-0.0` and NaN payloads); truncated or corrupt files return errors, never
panics.

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

The WAL format is `PINKWAL1` + version `2` + a generation number, and rolled
generations are named `<wal>.<gen>.roll`. A checkpoint writes `.pink.tmp`
(fsynced, directory fsynced), renames the live WAL to
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
reads a session-local view is refused on durable databases: views are never
persisted, so replaying its logged SQL could not rebuild it. A database file
is single-writer (no file locking); run one process per `.pink` path.

Each ≤2048-row column chunk picks its smallest encoding: integers choose
between raw, RLE, and bitpacking (min + width + packed bits); doubles raw or
RLE; booleans bitpacked or RLE; strings raw, dictionary (u32 codes), or RLE.
NULLs ride in a separate validity bitmap. 1M-row sales table: 24.5 MB CSV →
14.0 MB `.pink`, saved in 0.16 s, reopened and queried in 0.25 s.

## Concurrency

```rust
use pinkdb::{Database, PinkDb};

// One shared store, one connection per thread.
let db = Database::new();
let mut c = db.connect();
c.execute("CREATE TABLE t (a INTEGER);")?;
```

Queries run under a shared read lock (parallel readers, parallel chunks via
rayon inside each query). Writes take the write lock per statement.
`BEGIN..COMMIT` works on a private snapshot: commits to disjoint tables all
succeed; overlapping writes abort the second committer with a write-conflict
error (retry the transaction) — never silent lost updates. This is snapshot
isolation without predicate tracking (write-skew is possible). The plain
`PinkDb` API remains for single-connection embedding.

## Network server

`pinkdb-server` is a separate workspace crate (packaging: the core stays
lean): a TCP server plus a blocking client for a hand-rolled binary protocol.
No external dependencies, zero async (one std thread per connection), no
unsafe. Release binaries: `pinkdb` 2.19 MB, `pinkdb-server` 1.84 MB (Apple
Silicon, `--release`, thin LTO).

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
`--port` (default `5433`; `0` picks an ephemeral port), and `--file` (durable
`.pink`: the checkpoint loads and the sidecar WAL replays on restart;
in-memory when omitted). The server prints its listening address on startup
and runs until terminated.

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
- **No read/write timeouts.** A slow or stalled client holds one thread and
  one fd until it disconnects; a client that stops reading a large result
  blocks only its own connection thread (the accept loop is never blocked).
  The connection cap bounds that pressure.
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
is rejected with a clear error), `Execute { sql }`, `Ping`. Responses:
`HelloOk { server_version, protocol_version }`, `Pong`, and per statement one
result group — `ResultSet { names, types }`, then chunked `Rows { count,
columns, values... }` frames (typed value encoding: a tag byte + payload for
NULL/BOOL/I32/I64/F64/VARCHAR/DATE/TIMESTAMP; `DataType` tag byte), then
`Done { kind }`. A successful `Execute` batch ends with exactly one
`BatchDone { statements }` frame and the client reads result groups until it,
so neither side parses SQL just to count statements. A failing statement ends
the batch with a single `Error { message }` instead; SQL errors arrive as
`Error` frames and the connection stays open. The protocol version is 2.

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
The file and catalog dot-commands (`.import`, `.save`, `.open`, `.databases`,
`.tables`, `.schema`) are not available in remote mode and report
`Error: <cmd> is not available in remote mode`. The feature is opt-in: the
default build rejects `--host` with "built without the client feature".

### Security posture

**No authentication, no TLS, no authorization in this wave.** Anyone who can
open a TCP connection to the port gets the full SQL surface of the served
database (queries, writes, DDL) on the one shared store. The server binds
`127.0.0.1` by default and must stay on localhost or a trusted network: do
not expose it to the internet or to untrusted clients. Auth and TLS are
planned; they are explicitly out of scope for this wave.

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

## Known v1 limits

- No nested transactions or savepoints; one process per `.pink` path
  (single-writer: no file locking is taken); `.open` replaces the whole store
- `CREATE DATABASE` / `DROP DATABASE` are refused inside a transaction; the
  last database cannot be dropped; `DROP DATABASE` does not remove views from
  other databases (views are per-database and session-local)
- Three-part column references (`db.table.col`) are unsupported; use an alias
  (`SELECT x.col FROM db.table AS x`)
- `USE`, `DATABASE` and `LEFT`/`RIGHT`/`OUTER`/`FULL`/`CROSS` are reserved
  words; quote identifiers to use them as names (`"use"` works)
- Durable commits refuse statements that read a session-local view (views are
  never persisted); `.import` checkpoints, so it needs a settled transaction
- Correlated subqueries are rejected (uncorrelated only); no FULL joins and
  CROSS joins are rejected
- Window functions are SELECT/ORDER-BY only (not WHERE/GROUP BY/HAVING)
- Aggregates in ORDER BY need a select alias or position
- General ORDER BY expressions must be aliases, positions, or GROUP BY keys
- `%` is integer-only; `SUM` overflow wraps
- Mixed DATE/TIMESTAMP comparison is rejected; timestamps never mix with
  integers (no INTERVAL type in v1); no time zones (naive UTC)
- Expression nesting is capped at 24 levels (parentheses, `CASE` arms and
  consecutive unary operators counted together); deeper input returns a clean
  `Parse` error (`expression nesting too deep`) in both parsers instead of
  overflowing the parser stack. Separately, the parser-to-AST depth is capped
  at 64 nodes: left-associative operator chains (`1+1+...`), long `IN` lists,
  `AND`/`OR` runs and deeply nested subqueries return `expression too deep
  (limit 64)` — the AST is freed iteratively, so even a multi-megabyte chain
  cannot overflow the stack during rejection

Post-v1 roadmap: at-rest encryption, C/Python/Go language bindings, Parquet
reader (see PLAN.md). The network server (`pinkdb-server`) shipped in wave 24;
authentication and TLS are deferred.

## Development

```sh
cargo build            # workspace
cargo test             # 100+ unit + end-to-end tests
cargo test -p pinkdb-server                          # protocol + server + durable tests
cargo test -p pinkdb-cli --features client           # remote-mode CLI tests
cargo clippy --all-targets   # must stay clean
python3 bench/duckdiff.py [--big sales.csv]  # differential test vs DuckDB
cargo run --release -p pinkdb --example import_bench  # CSV import timing
cargo run --release -p pinkdb --example query_bench  # full query battery (ms)
```
