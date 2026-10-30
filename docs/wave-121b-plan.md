# Wave 121b — the console: access control, imports, storage

Follow-up of wave 121 (2026-10-30, user: "implement more features in the
UI. test RBAC, storage integrations, importing data from remote files,
different database storage integrations"). Defaults taken while the user
is away.

## Access (RBAC) panel

- Engine: `pinkdb_users()` (`name`, `kind` USER/ROLE, `is_admin`, `roles`)
  and `pinkdb_grants()` (`principal`, `privilege`, `object_type`,
  `database`, `name`). An administrator (or a trusted session) sees every
  row; anyone else sees only rows about themselves and the roles they hold
  — the catalog does not leak other principals.
- Console: an **Access** tab lists users, roles and grants. Administrators
  get forms: create user (a dedicated `POST /api/access/user`, so the
  password never reaches the run history), change a password, create/drop
  role, drop user, grant/revoke a privilege or a role. Every action runs as
  the session user, so RBAC decides (a non-admin gets the engine's
  refusal). `GET /api/session` reports `admin`.
- History: a run containing `CREATE USER`/`ALTER USER` is recorded with its
  SQL replaced by a placeholder (passwords stay out of `pinkdb_worksheets`).

## Imports

- **From a URL or a server path** (`https://`, `s3://`, a local path; CSV
  or Parquet): on a durable store the dialog follows the load with
  `CHECKPOINT`, so the table stops depending on the source at a replay. The
  Import dialog previews `SELECT * FROM read_csv|read_parquet(…)
  LIMIT 50` and loads with `CREATE TABLE … AS SELECT …` (create), `DROP TABLE
  IF EXISTS` + create (replace) or `INSERT INTO … SELECT …` (append) — plain
  SQL as the session user, so the engine's rule that file and URL reads are
  administrator-only (wave 48 M-4) holds: a non-admin cannot make the server
  fetch a URL or read its files.
- **Upload** (`POST /api/upload`, the file as the body, up to
  `--ui-max-upload-mb`, default 64): the server parses the bytes itself —
  CSV with the engine's own parser, Parquet through a scratch in-memory
  store and the process's Parquet reader — then writes the table as the
  session user. **Changed while building:** pinkdb's RBAC grants a table's
  creator nothing on it, so create-then-insert would need an `INSERT` grant
  that cannot exist yet; a new or replaced table is one `CREATE TABLE … AS
  SELECT … FROM (VALUES …)` with bound, typed placeholders (CTAS semantics:
  `CREATE` only), an append batched `INSERT`s. That found an engine bug: a
  `VALUES` row source of more than 2,048 rows failed ("vector overflow");
  fixed in the executor. The served store never
  references a temp file, so a durable store's WAL replays the rows, not a
  file read; RBAC applies through `CREATE`/`INSERT`, so an upload does not
  need `ADMIN`.
- Server features `httpfs` (CSV over https/s3/local) and `parquet`
  (Parquet, remote with `httpfs`), off by default, registered process-wide
  before the store opens (the CLI's order).

## Storage panel

`GET /api/storage`: the mode (in-memory, durable `.pink` file, lake
catalog), its location, and details — for a lake the catalog backend
(`.pink`, SQLite, Postgres), the data location (local or `s3://`) and the
published lake version (live). Secrets are redacted (a Postgres URL's
password, any `user:pass@`). A **Checkpoint** button runs `CHECKPOINT` as
the user (an administrator action in an authenticated console).

## Tests

- Engine: the two relations' rows and visibility.
- `pinkdb-server-ui` `tests/access_import.rs`: admin vs user views, user
  creation and login, grants taking effect, non-admin refusals, password
  redaction in history, URL import from a local HTTP server, non-admin URL
  import refused, CSV and Parquet uploads (non-admin with grants), upload to
  a durable store surviving a reopen, the upload size cap, the storage
  endpoint for in-memory and durable stores.
- `pinkdb-server-ui` `tests/import_s3.rs`: an `s3://` import from the mock
  S3 (own binary: it sets `AWS_*`).
- `pinkdb-server` `tests/console_storage.rs` (features `ui` + catalogs): the
  console over a lake catalog — `.pink`, SQLite, Postgres (throwaway local
  instance when `PINKDB_TEST_POSTGRES_URL` is set), S3 data on the mock —
  worksheet writes publish to the catalog and a reopen sees them; the
  storage endpoint reports the lake without secrets.
