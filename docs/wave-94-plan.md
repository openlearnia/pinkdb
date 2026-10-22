# Wave 94 — three-part column references (`db.table.column`) (plan)

Started 2026-10-22; **done** the same day. ROADMAP row 94: a database qualifier on column
references wherever a column may appear, resolved through the same catalog
lookup as the FROM clause; the alias interaction defined; errors name the
full reference; the existing alias workaround unchanged. Parser, AST, binder
and rendering only — no storage, plan-format or protocol change.

## Where things stand

- `crates/pinkdb/src/sql/peg_parser.rs`: `three_part_column()` and the
  three-part alternative of `table_ref()` reject `db.t.col`, `db.t.*` (and
  `CREATE TABLE db.t.col`) with a clear "not supported" error; tests pin
  those messages (`three_part_reference_errors_are_clear`).
- `crates/pinkdb/src/sql/ast.rs`: `Expr::Column { table: Option<String>, name }`,
  `Expr::QualifiedStar(Option<String>)`, `Projection::QualifiedStar(String)`.
- `crates/pinkdb/src/sql/binder.rs`: `ScopeTable { name, columns }` (name =
  alias if present else table name, lowercased); `lookup_column(table, col)`.
- README "Known v1 limits" lists three-part references as unsupported.

## Semantics (decided)

1. **Forms.** `db.table.column` anywhere a column expression is allowed
   (SELECT list, WHERE, HAVING, GROUP BY, ORDER BY, join `ON`, window
   `PARTITION BY`/`ORDER BY`, function arguments, subqueries — correlated
   ones included —, CTE bodies, `UPDATE … SET` right-hand sides and `WHERE`,
   `DELETE … WHERE`) and `db.table.*` in a projection. Identifiers are
   case-insensitive unless quoted, exactly like today; quoted and
   multi-word identifiers work in every part (`"My Db"."t 1"."a b"`).
   Four or more parts are refused with a clear error naming the reference.
2. **Resolution.** `db.t.c` matches a FROM-scope entry that is a **base
   table or view of database `db`** (resolved like the FROM clause:
   case-insensitive, the database must exist — else `database "db" not
   found`) **whose effective name is its own table name, i.e. it is not
   aliased**. An unqualified FROM entry (`FROM t`) belongs to the session's
   current database, so `SELECT main.t.a FROM t` works while `main` is
   current. Derived tables, CTEs, table functions and file scans have no
   database and never match a three-part reference.
3. **Aliases hide the original name** (the Postgres/DuckDB rule, and the
   existing two-part rule): `SELECT db.t.a FROM db.t AS x` is an error that
   names the alias — `table "db.t" is aliased as "x" in this query; refer to
   its columns as x.a`. The workaround `FROM db.t AS x … x.col` keeps
   working unchanged.
4. **Ambiguity.** If the same table appears twice unaliased (only possible
   across databases, e.g. `FROM db1.t JOIN db2.t`), a two-part `t.c` is
   **ambiguous** and must say so, naming both `db1.t` and `db2.t` and
   suggesting the three-part form or aliases; `db1.t.c` resolves uniquely.
   Check what today's binder does for `t.c` there: if it silently picks the
   first entry, that is a correctness bug fixed in this wave. A bare `c`
   keeps today's ambiguity rule.
5. **Errors name the full reference**: unknown database, unknown/not-in-scope
   table (`no table "db.t" in this query's FROM clause`), unknown column
   (`no column "db.t.c"`), ambiguity, aliased table.
6. **`UPDATE … SET` targets.** A target may be `c`, `t.c` or `db.t.c`; a
   qualifier must name the UPDATE's own target table (and database), else an
   error naming both. (Check what qualified targets do today and keep any
   existing accepted form working.)
7. **Grouping/ordering equivalence** is decided on bound columns, not AST
   text: `SELECT db.t.a, COUNT(*) FROM db.t GROUP BY t.a` (and `GROUP BY a`,
   `ORDER BY db.t.a`) must bind like the two-part forms do today. Check how
   the binder matches GROUP BY / ORDER BY / select-alias references and make
   three-part references participate.
8. **Procedure variables** shadow only unqualified names (unchanged);
   **macro parameters** are only unqualified names (a three-part reference in
   a macro body is never a parameter).
9. **RBAC** is unchanged: privileges are per table, and a three-part column
   reference can only resolve to a table already in the statement's FROM
   scope, whose access is checked as today.
10. **Rendering.** Every place the AST is turned back into text (Display /
    SQL rendering used by EXPLAIN, DESCRIBE, error messages, view/macro/MV
    definitions, prepared-statement or WAL paths if any) prints
    `db.table.column` and `db.table.*` faithfully, so persisted definitions
    and replay round-trip.
11. `table_ref()` keeps refusing three-part **table** names (`CREATE TABLE
    db.t.col`, `FROM a.b.c`) — pinkdb has no schemas — but with a message
    that no longer claims column references are unsupported (e.g. "a table
    reference has at most two parts (database.table)").

## AST shape (suggested)

`Expr::Column { db: Option<String>, table: Option<String>, name: String }`
(`db.is_some()` implies `table.is_some()`); `Expr::QualifiedStar` and
`Projection::QualifiedStar` carry `{ db: Option<String>, table: String }`.
`ScopeTable` gains the entry's database (`None` for derived/CTE/function
scopes) and whether its name is an alias. Update every match site
mechanically (`Expr::Column` has ~78 uses across `sql/`, `plan.rs`,
`optimizer.rs`, `macros.rs`, `exec/`).

## Tests

New `crates/pinkdb/tests/three_part_columns.rs`: every clause position in
rule 1; joins across two databases with the same table name (three-part
disambiguates, two-part is ambiguous); self-join with aliases; the alias rule
and its error; unqualified FROM + three-part via the current database, and
after `USE` of another database; CTEs/derived tables never match; correlated
subqueries referencing the outer `db.t.c`; `db.t.*` alone and next to other
columns; GROUP BY/ORDER BY/HAVING equivalence between forms; `UPDATE … SET
db.t.c = …` and a mismatched qualifier; DELETE; case-insensitivity and
quoted/multi-word identifiers; four-part refusal; every error message; views
and macros whose bodies use three-part references (definition persisted in a
durable store and replayed after reopen); prepared statements; EXPLAIN text.
Update the existing parser tests whose messages change. **duckdiff**: add
probes to `bench/duckdiff.py` where DuckDB's `catalog.table.column`
resolution matches (DuckDB side: `ATTACH ':memory:' AS db1; CREATE TABLE
db1.t …`; pinkdb side: `CREATE DATABASE db1; CREATE TABLE db1.t …` — use the
existing per-engine setup mechanism, e.g. `run_pair`), covering selection,
filtering, joins across databases, grouping and `db.t.*`.

## Gates

`cargo test --workspace`, clippy (`-D warnings`) on both toolchains, fmt,
wasm32 core build, duckdiff (full battery, all queries match), the CLI
feature matrix spot-check (`default`, `pinklake`, `vortexlake,s3`).

## Rules for delegated agents

Do not commit, push or create branches. Touch only the files the task
names. Use `CARGO_INCREMENTAL=0`. Report what changed and every check.

## How it was built

Delegated with the brief above. CommandCode failed three times at startup
with "Unable to connect to the API" (short CommandCode runs succeeded in
between), so the wave ran on **OpenCode** (DeepSeek V4.1 Flash), which
completed it this time; reviewed here.

What it found and decided beyond the plan:

- **Old behavior, fixed:** `FROM db1.t JOIN db2.t` was refused by the
  duplicate-effective-name guard, and where two same-named tables were in
  scope a two-part `t.c` bound whichever matching entry had the column. The
  guard now admits unaliased base tables from different databases; `t.c` and
  `t.*` are then ambiguous (naming both), `SELECT *` expands through
  three-part names, and any other duplicate keeps the old error.
- **Qualified `UPDATE … SET` targets** were parse errors; `t.c` and `db.t.c`
  are now accepted when they name the statement's own table (new
  `ast::SetTarget`).
- **Four-part references:** the PEG error type is a static string, so the
  parser marks the node (`FOUR_PART_MARKER`, `db: Some(marker)` with
  `table: None` — a shape a real reference never has) and the AST depth walk
  turns it into an error quoting the reference. Review fix: the quoted text
  kept the identifier's trailing whitespace (`"a.b.c.d "`); it is trimmed.
- **Correlated subqueries** remain unsupported engine-wide (a v1 limit, not
  binder work), so an outer `db.t.c` inside a subquery gets the existing clean
  refusal, naming the full reference.
- **Durable round-trip** is covered with a materialized view and a macro
  (plain views are session-local and are not persisted).
- **duckdiff:** a new battery (7 probes, per-engine multi-database setup);
  DuckDB rejects a database qualifier before `*`, so the star probes spell the
  columns on the DuckDB side.
