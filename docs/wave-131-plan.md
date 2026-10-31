# Wave 131 — schemas and three-part names

Planned 2026-10-31 (QuackLab track, stream A). pinkdb is becoming the
engine under QuackLab, whose compiler writes `r2_cat."master"."FACT"`
everywhere and creates the schemas `master`, `ingest`, `staging`,
`metadata` and one per mart. pinkdb has one fixed schema per database and
table names of at most two parts, so wave 129 reached 114 of 114 golden
statements only by turning every schema into a database and stripping the
`r2_cat.` prefix. This wave adds the schema level for real. The catalog
design follows DuckDB's (`src/catalog/`, `planner/binder/statement/
bind_create.cpp`): ported and credited, not pasted.

## What exists

- **Store → database → objects.** `PinkDb.databases` maps a database key to
  a `DbCatalog` (`storage.rs`) holding tables, views, materialized views,
  procedures, macros and triggers directly. Every database has the implicit
  schema `main`; a lake database exposes its one lake schema under the
  database's name and wave 124 refuses lakes with more (`one_schema`,
  `pink-lake/src/database.rs`).
- **Names.** The AST carries `db: Option<String>` beside every object name
  (about 40 statement variants); the parser accepts `name` and `db.name`.
  The binder's `resolve_db` turns the qualifier into a database; the plan
  carries `db: Option<String>`; the executor's `resolve_db` fills in the
  session default. Column references take `col`, `t.col` and `db.t.col`.
- **Store API.** `*_in(db: &str, …)` (`table_in`, `create_table_in`,
  `list_tables_in`, `macro_in`, …). pink-lake, the CLI and many tests call
  them with plain database names.
- **Persistence.** The `.pink` footer (version 3) lists databases, each
  with its storage and then its tables, macros, materialized views,
  procedures and triggers. Views are session-local and never persisted
  (`reject_view_dependency`). Lake databases keep their engine metadata in
  a per-database image (`catalog_image.rs`).
- **WAL.** Records are SQL text replayed in a session database: the record
  carries the session and a `USE db` prefix moves replay there. A frame's
  touched set (`FrameSets`, `wal.rs`) is `(database, table)` pairs plus
  created or dropped databases; optimistic commits validate against it.
- **RBAC.** `GrantObject::{Admin, Role, Table { db, table }, Database
  { db }}`; `CREATE` is a database privilege.
- **Introspection.** `SHOW TABLES`, `SHOW DATABASES`, `DESCRIBE` and
  `pinkdb_catalog()`; no `information_schema` and no `duckdb_*()`
  functions. The console's `CatalogObject.schema` is always `main`.

## Decisions

1. **Store → database → schema → objects.** `DbCatalog` keeps its name,
   storage and lake version and gains `schemas: BTreeMap<String,
   SchemaCatalog>` (key lowercased). `SchemaCatalog` holds the display name
   plus everything `DbCatalog` holds today: tables, views, materialized
   views, procedures, macros and triggers (creation order kept per
   schema). Every database always has schema `main`; it cannot be dropped
   (DuckDB: "internal system entry"). Indexes stay inside their table.
2. **One resolved path type.** `SchemaPath { db, schema }` (lowercased
   keys) names a schema. The store API keeps its `*_in` names but takes
   `ns: impl Into<SchemaPath>`: `&str`/`&String` convert to that
   database's `main` schema, so a database name alone still means its
   default schema (DuckDB's rule for a catalog without a schema) and
   pink-lake, the CLI and the tests keep compiling untouched. New calls
   pass a `&SchemaPath`. New store calls: `create_schema`, `drop_schema
   (cascade)`, `schema_exists`, `list_schemas`.
3. **Names in the AST and the plan.** The parser reads up to three parts
   for every object name (tables, views, MVs, indexes, macros, procedures,
   triggers, `COPY`, `DESCRIBE`, `ANALYZE`, `GRANT`) into a `Qualifier
   { catalog: Option<String>, schema: Option<String> }` beside the name,
   replacing `db: Option<String>`. `x.t` is parsed as `schema: Some(x)`:
   it is ambiguous until bind. The plan carries the resolved
   `ns: Option<SchemaPath>` where it carried `db` (`None` still means a CTE
   for `Scan`).
4. **Resolution order** (binder, ported from DuckDB's
   `BindSchemaOrCatalog` and `SearchSchema`):
   - `c.s.t`: database `c`, schema `s`; both must exist.
   - `x.t`: if a database `x` exists and the session database has no
     schema `x`, it is `x.main`; if neither, it is schema `x` of the
     session database (missing schema → `schema "x" not found`). If both
     exist and they name different schemas, the statement is refused:
     `ambiguous reference to database or schema "x": use x.main.t or
     <db>.x.t`. `main.t` while the session is in database `main` is not
     ambiguous (both readings are `main.main`).
   - `t`: a CTE, then the session's current schema. (Wave 139 adds the
     `temp` database after it.)
   - Column references take one to four parts (`col`, `t.col`, `s.t.col`,
     `c.s.t.col`); a qualifier matches a FROM item whose own path ends
     with it.
   - A view, materialized view, procedure or trigger binds its body with
     **its own schema** as the default (DuckDB binds view bodies against
     the view's schema). A trigger and its table share a schema. A macro is
     spliced into the calling query, so its body resolves against the
     **caller's** schema, as DuckDB's macros do. Macros are called
     unqualified (current schema) or as `s.m()` / `c.s.m()`.
   - Foreign keys must stay in one schema (DuckDB: "Creating foreign keys
     across different schemas or catalogs is not supported").
5. **Session position.** A session (embedded handle, `Connection`) holds
   `current_db` and `current_schema`. `USE x`: database `x` (schema
   `main`) if it exists, else schema `x` of the current database. `USE
   x.y`: database `x`, schema `y`. `current_database()`,
   `current_schema()` and `current_schemas(bool)` report it. Dropping the
   current schema falls back to `main`, the way dropping the current
   database already falls back.
6. **DDL.** `CREATE SCHEMA [IF NOT EXISTS] [db.]s`; `DROP SCHEMA [IF
   EXISTS] [db.]s [CASCADE | RESTRICT]` (RESTRICT, the default, refuses a
   non-empty schema with DuckDB's message). Both are WAL-logged catalog
   mutations, refused for a read-only lake database and inside a
   transaction only if `CREATE DATABASE` is. New result kinds
   `CreatedSchema { name }` / `DroppedSchema { name }` through the wire
   protocol, FFI, wasm and the CLI. `Used { name }` reports `db.schema`.
7. **Checkpoint footer version 4.** Per database: name, storage, then the
   schema count and, per schema, its name and the sections version 3 kept
   per database (tables, macros, materialized views, procedures,
   triggers), in sorted schema order. Data-region order becomes database,
   schema, table. Version 3 files are refused as written by an older
   pinkdb; their reader goes. The lake per-database image carries schemas
   the same way.
8. **WAL.** The replay session is a schema path: the `USE` prefix becomes
   `USE "db"."schema"` (quoted, so any name round-trips), and
   `set_session_db` becomes a path. `FrameSets` records `(db, schema,
   table)` triples and created or dropped `(db, schema)` pairs besides
   databases; `CREATE/DROP SCHEMA` conflict with any frame touching that
   schema. Older WAL payloads are refused, like older footers.
9. **RBAC per schema.** `GrantObject::Table { db, schema, table }` and a
   new `GrantObject::Schema { db, schema }`. `GRANT SELECT|INSERT|UPDATE|
   DELETE|ALL ON SCHEMA s` covers every table and view of `s`, now and
   later; `GRANT CREATE ON SCHEMA s` lets a principal create objects in
   `s`; `CREATE ON DATABASE` covers `CREATE SCHEMA` and objects in every
   schema of it. A table check walks table → schema → database grants.
   `pinkdb_grants()` gains a `schema` column. Persisted in the RBAC
   footer section.
10. **Catalog functions** (DuckDB's column names, order and types, from
    `DESCRIBE` on DuckDB 2.0; columns of types pinkdb lacks — `tags` and
    `options` maps — are left out): `duckdb_databases()`,
    `duckdb_schemas()`, `duckdb_tables()`, `duckdb_views()`,
    `duckdb_columns()`, each listing the user's objects (`internal` false;
    the `information_schema` and `pg_catalog` schemas are listed as
    internal). `information_schema.{schemata, tables, columns, views}` are
    built-in views whose SQL is DuckDB's definition over those functions
    (`default_views.cpp`), bound like any view; they resolve from any
    database (`db.information_schema.tables` too) and list every database.
    OIDs are assigned in sorted enumeration order per statement, so the
    functions join with each other; they are not stable across DDL (nor
    are DuckDB's across restarts). `sql` columns carry the stored
    `CREATE` text where the engine keeps it, else NULL. Every function
    honours RBAC like `pinkdb_catalog()` does.
11. **Other introspection.** `SHOW TABLES` lists the current schema;
    `SHOW ALL TABLES` gives DuckDB's `database, schema, name,
    column_names, column_types, temporary` (the two lists as VARCHAR until
    LIST exists); `pinkdb_catalog()` gains a `schema` column and one
    `SCHEMA` row per schema; `DESCRIBE` takes three-part names.
12. **Lake databases with several schemas.** Each DuckLake schema of a lake
    is a schema of its database, read-only and writable alike;
    `one_schema` and `multi_schema_error` go. `LakeContent.tables`,
    `publish` and `tables_at` key tables by `(schema, table)`, and
    `publish` also receives the schema list so `CREATE SCHEMA` of an empty
    schema and `DROP SCHEMA` reach the lake's `ducklake_schema` rows. Only
    schema mapping changes; lake I/O stays with stream B.
13. **Console.** `/api/catalog` fills `CatalogObject.schema`; the catalog
    tree shows database → schema → objects, and the SQL the explorer
    generates uses three-part quoted names.
14. **Golden harness.** `quacklab-goldens.py` runs pinkdb with the same
    names DuckDB gets: `CREATE SCHEMA` for each schema, `r2_cat` created
    as a database and selected with `USE r2_cat`, no prefix stripping;
    `PINK_DEFAULTS=1` keeps the column defaults pinkdb supports.

## Out of scope (recorded)

- Persisting views in local stores (they stay session-local, as today).
- `SET search_path` and `ALTER … SET SCHEMA`.
- `ATTACH ':memory:'` and attaching `.pink` files.
- Column defaults evaluated per row, `TEMP` tables, LIST/STRUCT and
  `uuid()` in writes: wave 139.

## Briefs (pi-sized, in order; each one gated and committed)

| Brief | Scope |
|---|---|
| 131a catalog and persistence | Decisions 1, 2, 7, 8 and the RBAC model of 9, with no SQL change: every object lives in `main`. All existing tests pass unchanged; new API-level tests round-trip a non-`main` schema through checkpoint and the lake image. |
| 131b grammar and binder | Decisions 3–6, the SQL of 9, result kinds across protocol/FFI/wasm/CLI, WAL logging of schema DDL. |
| 131c catalog functions | Decisions 10 and 11, with a DuckDB differential test. |
| 131d lake schemas | Decision 12 (pink-lake `database.rs`, `lake_db.rs`, `attached.rs` aliasing). Coordinated with stream B. |
| 131e console | Decision 13, `build-console.sh`, a live check in the preview browser. |

## Tests

Engine: name resolution (three-part, schema-or-database, the ambiguity
error, `main.t` in `main`), `CREATE/DROP SCHEMA` with `IF [NOT] EXISTS`,
`CASCADE`, `RESTRICT` and `main`; `USE db.schema` and `USE schema`; view,
macro, MV, procedure and trigger bodies resolving in their own schema; a
cross-schema foreign key refused; checkpoint and WAL replay round trips
(including a `USE` prefix into a non-`main` schema and a dropped schema);
footer version 3 refused; optimistic-commit conflicts on schema DDL;
grants on a schema (inherited by later tables) and on a three-part table;
`information_schema` and `duckdb_*()` diffed against DuckDB on one fixed
catalog. Lake: a two-schema lake read-only and writable, `CREATE SCHEMA`
and `DROP SCHEMA` published, reopened. Server: `/api/catalog` schemas.
Golden harness: every statement passes with no workaround. Full gate
(format, API, lake and console change), plus the DuckDB differential suite
and the TPC-H checksums.
