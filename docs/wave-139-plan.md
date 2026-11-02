# Wave 139 — QuackLab leftovers: per-row defaults, volatile writes, TEMP tables

Planned 2026-10-31 (QuackLab track, stream A), after wave 131 (schemas).
Wave 129 left four gaps the QuackLab goldens and runtime SQL hit: column
defaults are constants evaluated once at `CREATE TABLE`, `uuid()` (and every
volatile function) is refused in any statement that writes, `CREATE TEMP
TABLE` does not parse, and LIST/STRUCT do not exist. DuckDB 2.0
(`~/.local/bin/duckdb`) is the behaviour reference; its source the design
reference (port and credit, never paste).

## What exists

- **Defaults.** `Column.default` holds a `Value` computed once at `CREATE
  TABLE`/`ADD COLUMN`; a default reading the clock is refused ("not
  supported yet"); the footer's default section stores values.
- **Volatile functions.** The binder refuses `uuid()`, `gen_random_uuid()`
  and volatile extension functions in any statement that writes, in memory
  too, because the WAL replays SQL text. `now()` is allowed: the statement's
  first clock reading rides in the WAL record's memo (`clock_memo`) and
  replay binds it.
- **No `UUID` type**: `uuid()` returns VARCHAR (DuckDB: `UUID`).
- **No temporary catalog**: `CREATE TEMP` is a parse error with a hint.
- **No nested types**: `[1,2]`, `{'a': 1}`, `list()` are parse errors.

## Decisions

1. **Defaults are expressions, evaluated per row.** `Column.default` becomes
   a bound expression plus its SQL text (DuckDB keeps the parsed expression,
   `ColumnDefinition::DefaultValue`). `INSERT` (VALUES, SELECT, `DEFAULT`
   keyword, `DEFAULT VALUES`), `MERGE … INSERT` and `UPDATE … SET c =
   DEFAULT` evaluate it per inserted row; `ALTER TABLE ADD COLUMN … DEFAULT
   e` evaluates it per existing row (DuckDB does). A default is bound at
   `CREATE` (types checked, no column references, no subqueries, no
   aggregates) but a failing cast happens at insert, like DuckDB's `DEFAULT
   'now()'` on a `TIMESTAMP`: `CREATE` succeeds, an insert that uses it
   fails with the conversion error. The footer stores the default's SQL text
   (format bump, older refused) and rebinds it at load; `duckdb_columns().
   column_default` and `DESCRIBE` print it.
2. **Volatile functions in writes, replayed exactly.** A write that calls
   `uuid()`, `gen_random_uuid()` or `random()` gets a per-statement 128-bit
   seed, recorded in the WAL memo next to the clock reading. Each call
   derives its value from `(seed, call site, chunk index, row index)` with a
   small in-tree mixer (SplitMix64 rounds; DuckDB's own `uuid()` draws from a
   PCG generator, not a CSPRNG, and pinkdb's default build has no PRF crate),
   so values are random to the user, independent of thread scheduling, and
   identical on replay. The seed comes from `getrandom`, already a
   dependency. Generated UUIDs are version 4.
   Volatile *extension* functions stay refused in logged writes (their
   values cannot be derived). Writes that are not WAL-logged (in-memory
   stores, lake-only units) need no seed but use the same path.
3. **A `UUID` type** (16 bytes, DuckDB's text form, casts to and from
   VARCHAR, comparison and hashing), so `uuid()` returns `UUID` and
   `CREATE TABLE t (id UUID DEFAULT uuid())` works. Persisted in the footer,
   the WAL value encoding and the lake writers that already handle fixed
   binary (or written as VARCHAR to Parquet if the lake path lacks it: say
   which).
4. **`TEMP` tables.** `CREATE [OR REPLACE] TEMP|TEMPORARY TABLE [IF NOT
   EXISTS] t (…)` and `… AS SELECT` create the table in the session's own
   `temp` database (schema `main`), as DuckDB does: never checkpointed, never
   in the WAL, dropped when the session ends, invisible to other sessions.
   Name resolution: an unqualified name is looked up in `temp.main` first,
   then in the current schema — DuckDB's search path, so a temp table
   shadows a persistent one of the same name (checked live); `temp.t` and
   `temp.main.t` name it explicitly; `DROP TABLE t` drops the temp one
   first. `duckdb_tables()` lists it under database `temp` with `temporary
   = true`, and `information_schema.tables` as `LOCAL TEMPORARY`. A logged write
   (local durable store) that **reads** a temp table is refused, like a
   session-local view: replay could not rebuild it. A write into a lake
   database or an in-memory store may read temp tables — QuackLab's ETL
   pattern `CREATE TEMP TABLE stage AS …; INSERT INTO r2_cat.master.x
   SELECT … FROM stage` commits to the lake, never the local WAL.
   Temporary views, macros and sequences stay out of scope.
5. **LIST and STRUCT get their own wave.** QuackLab's SQL generators and
   runtime emit no list or struct syntax (searched 2026-11-01), and its JS
   procedures deliberately take JSON `VARCHAR` instead of `VARCHAR[]`, so
   they are off the QuackLab critical path. They touch the vector layout,
   every operator, the `.pink` encoder, Parquet/lake I/O, the wire protocol,
   FFI and bindings. This wave writes their scoping plan
   (`docs/nested-types-plan.md`): where lists matter for the track (the
   quack protocol, DuckLake interop), the physical layout (DuckDB's offset/length list
   vectors and child vectors), and a brief split. Nothing is implemented.

## Briefs

| Brief | Scope |
|---|---|
| 139a defaults | Decision 1, footer default section as SQL text. |
| 139b volatile writes + UUID | Decisions 2 and 3. |
| 139c temp tables | Decision 4. |
| 139d nested-types plan | Decision 5 (doc only, written by the orchestrator; done with this plan). |

## Tests

Defaults: per-row `now()`, `uuid()`, `nextval`-free arithmetic defaults;
`DEFAULT 'now()'` on TIMESTAMP creates and fails at insert with DuckDB's
message; `DEFAULT` keyword in VALUES and UPDATE; ADD COLUMN per row;
checkpoint and WAL round trip; `information_schema.columns.column_default`
text equal to DuckDB's. Volatile writes: `INSERT … SELECT uuid() FROM
range(100000)` distinct, durable crash-replay byte-identical, parallel and
serial execution agree after replay. TEMP: session isolation over two
connections, end-of-session drop, search order, durable refusal, lake ETL
pattern allowed. Goldens with `PINK_DEFAULTS=1` pass every statement DuckDB
passes. Full gate (format and type change).
