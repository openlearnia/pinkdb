# pinkdb — a mini DuckDB in Rust

An embedded **analytical** SQL database: in-memory columnar storage, vectorized
execution, SQL with aggregates and joins, CSV import. Ships as a library crate
(`pinkdb`) and a CLI REPL (`pinkdb-cli`, a thin consumer of the library).

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
  → Lexer + recursive-descent parser → AST
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
| `pinkdb::storage` | `DataTable` (per-column `Vector` chunks), `PinkDb` catalog |
| `pinkdb::csv` | CSV import + whole-file type inference |
| `pinkdb::sql` | Hand-written lexer, AST, recursive-descent parser, binder |
| `pinkdb::plan` | Logical plan tree |
| `pinkdb::optimizer` | Constant folding, predicate pushdown (incl. across joins) |
| `pinkdb::exec` | Vectorized expression eval, hash aggregate, hash join, operators (rayon-parallel) |
| `pinkdb::persist` | `.pink` single-file columnar persistence (RLE/dictionary/bitpack) |
| `pinkdb::db` | Multi-writer `Database`/`Connection` over `Arc<RwLock<PinkDb>>` |
| `pinkdb-cli` | rustyline REPL, dot-commands, box-drawn output |

Design rules: Rust edition 2021, zero async, no unsafe. Dependencies are
`csv` + `rustyline`, plus `rayon` for parallel execution.

## SQL surface (v1)

```sql
CREATE TABLE t (id BIGINT, name VARCHAR, price DOUBLE);
INSERT INTO t VALUES (1, 'x', 9.5), (2, NULL, 3.0);
UPDATE t SET price = price * 1.1 WHERE id > 1;   -- SET exprs see pre-update values
DELETE FROM t WHERE price IS NULL;               -- NULL never matches; both are statement-atomic
BEGIN; UPDATE t SET price = 0; ROLLBACK;         -- flat snapshot transactions (in-memory only)
SELECT [DISTINCT] expr [AS alias], ...
FROM t [JOIN u ON t.x = u.y]      -- inner equi-join, single equality
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

- Aggregates: `COUNT(*)` / `COUNT(x)`, `SUM`, `AVG`, `MIN`, `MAX`
- Window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, aggregates `OVER`
  (`PARTITION BY` / `ORDER BY` / `ROWS` frames; default frame is cumulative
  with ORDER BY, whole partition without; no `RANGE` frames, no nesting)
- Scalar functions: `abs`, `round` (1–2 args), `upper`, `lower`, `length`, `substr`
- `CAST(x AS type)` (numeric tower both ways, bool↔numeric, varchar parse/format,
  date↔timestamp; bad input errors), `COALESCE(a, b, ...)` (common type),
  `LIKE`/`NOT LIKE` (`%`/`_` patterns)
- Set ops: `UNION [ALL|DISTINCT]` (numeric types unify; ORDER BY/LIMIT attach
  to their own branch in v1); `CREATE VIEW v [(cols)] AS ...` (session-local,
  rebound per use), `DROP TABLE` / `DROP VIEW`
- Types: `BOOLEAN`, `INTEGER` (i32), `BIGINT` (i64), `DOUBLE` (f64), `VARCHAR`,
  `DATE` (days since epoch), `TIMESTAMP` (microseconds, naive, no time zone)
- Literals: `DATE '2024-02-29'`, `TIMESTAMP '2024-03-15 12:30:45.5'`
- Date ops: `date ± integer` (days), `date - date` → BIGINT days;
  `year/month/day/hour/minute/second(x)`, `date_trunc(part, x)`
- CSV auto-detects `YYYY-MM-DD` → DATE and `YYYY-MM-DD HH:MM:SS[.f]` → TIMESTAMP
- Coercion: INTEGER → BIGINT → DOUBLE; `int / int` → DOUBLE (DuckDB rule)
- Three-valued logic: NULL fails `WHERE`; `COUNT(x)`/`SUM`/`AVG` skip NULLs;
  NULL group keys group together; NULL join keys never match
- Integer division/modulo by zero is an error; `0.0` float division is IEEE
- `SUM` over integers accumulates in i64 (wraps on overflow)

REPL dot-commands: `.help`, `.quit` / `.exit`, `.tables`, `.schema <t>`,
`.import <file.csv> [table]` (table defaults to the file stem),
`.save <file.pink>`, `.open <file.pink>`.

CSV inference (per column, over all non-empty cells): all i32 → INTEGER, all
i64 → BIGINT, all f64 → DOUBLE, all true/false (case-insensitive) → BOOLEAN,
else VARCHAR. Empty cell → NULL.

## Persistence

`db.save("x.pink")` writes every table in a single-file columnar format;
`PinkDb::open("x.pink")` loads it back (`.save` / `.open` in the REPL).
Round-trips are bit-exact (including `-0.0` and NaN payloads); truncated or
corrupt files return errors, never panics.

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

## Benchmark (1M rows, release, Apple Silicon)

Generated with `bench/gen_sales.py` (deterministic seed 42):

| Step | Time (rayon) | Before |
|---|---|---|
| `.import` 1M rows (24 MB CSV) | 0.43 s | 0.42 s |
| `SELECT COUNT(*) FROM sales` | 0.03 s | 0.04 s |
| GROUP BY region + 5 aggregates | 0.04 s | 0.11 s |
| `SELECT COUNT(*) ... WHERE total > 500` | 0.03 s | 0.07 s |
| WHERE + GROUP BY + HAVING + ORDER + LIMIT | 0.04 s | 0.10 s |
| 1M-row inner hash join + GROUP BY | 0.13 s | 0.27 s |

## Known v1 limits

- No nested transactions or savepoints; snapshots live in memory only
  (crash recovery is future work); `.open` replaces the whole store
- Correlated subqueries are rejected (uncorrelated only); no outer joins
- Window functions are SELECT/ORDER-BY only (not WHERE/GROUP BY/HAVING)
- Aggregates in ORDER BY need a select alias or position
- General ORDER BY expressions must be aliases, positions, or GROUP BY keys
- `%` is integer-only; `SUM` overflow wraps
- Mixed DATE/TIMESTAMP comparison is rejected; timestamps never mix with
  integers (no INTERVAL type in v1); no time zones (naive UTC)
- Very deeply nested expressions (tens of thousands of levels) can overflow
  the parser stack instead of erroring gracefully

Post-v1 ideas: on-disk columnar format (RLE + dictionary + bitpacking),
Parquet reader, parallel execution, DATE/TIMESTAMP, subqueries/CTEs.

## Development

```sh
cargo build            # workspace
cargo test             # 100+ unit + end-to-end tests
cargo clippy --all-targets   # must stay clean
python3 bench/duckdiff.py [--big sales.csv]  # differential test vs DuckDB
```
