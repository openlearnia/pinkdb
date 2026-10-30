# Wave 121 — the warehouse WebUI with SQL worksheets

Goal (the user's, since 2026-10-23): "build a better WebUI for the warehouse,
with SQL worksheets". The wave-78 console (`crates/pinkdb-server-ui`) is a
read-only catalog browser; this wave makes it a worksheet console that runs
SQL as the logged-in user, hosts the data agent, and keeps wave 78's exposure
policy.

## What stays

- The `ui` feature of `pinkdb-server` stays **off by default**; the default
  server binary does not grow (sizes in the ROADMAP row).
- The console owns its tokio runtime; the engine runs on the bounded blocking
  pool; the store lock is never held across HTTP work.
- Exposure: loopback-only without console TLS; a store with users is never
  served unauthenticated; an unauthenticated console warns loudly.
- The wave-78 GET API (`/api/databases|schemas|tables|rows|stats`, `/preview`)
  keeps working, with `Authorization` headers or a session cookie.

## Engine changes (small, in `pinkdb`)

1. **Cancellation.** `pinkdb::exec::CancelFlag` (an `Arc<AtomicBool>`), set on
   a connection with `Connection::set_cancel_flag`. `ExecBudget` carries it
   and `check_deadline` — already called at chunk boundaries in scans, joins,
   aggregates and row loops — answers `statement cancelled` once it is set.
   Same guarantee as the timeout: the statement fails before any write-back.
2. **`Connection::impersonate(user)`.** Adopt an existing user as the session
   principal without a password. For a front-end that authenticated the user
   itself (the console's session cookie). Strictly narrower than the trusted
   embedded session every embedder already has, so it widens nothing. The
   console verifies the password once at login and never keeps it.
3. **`pinkdb_catalog()`.** A constant relation like `pinkdb_column_stats()`:
   `database, name, kind, table_name, detail` for tables, views, materialized
   views, hash and vector (HNSW) indexes, macros, triggers and attached lake
   tables. Rows of a table the caller cannot `SELECT` are left out (and so
   are its indexes).
4. **`Connection::run_command`.** Run a command extension's `pinkdb_ext_command`
   on a *connection* instead of `&mut PinkDb`: the store lock is taken per
   statement (the CLI path holds `&mut PinkDb` for the whole run, which would
   block every other console session for the length of a model call). The
   caller gets the answer/diagnostic text and every public-channel statement
   with its reply (`CommandEvents`), and can stop the run (`cancelled()`).
5. **Policy constructors in `pinkdb::serve`.** `agent_policies` and
   `memory_policies` (tool + internal policies). The CLI had them as private
   functions; the server needs the same ones. The CLI now calls them.
6. **`.agent providers json`.** Machine-readable provider list: name, protocol,
   default model, and whether a key / base URL is set — never the key or URL.

## The console

### Sessions and CSRF

- `POST /api/login` — `{"user","password"}` (Basic mode, verified through
  `Connection::login`), `{"token"}` (token mode), `{}` (no-auth mode). It
  answers `{user, csrf, read_only, agent}` and sets
  `pinkdb_session=<256-bit random>; HttpOnly; SameSite=Strict; Path=/`
  (`; Secure` when console TLS is configured).
- Sessions live server-side: a map keyed by the SHA-free random token
  (compared through the map lookup; tokens are 43 base64url chars from
  `getrandom`). Idle timeout (30 min), absolute lifetime (12 h), a cap on live
  sessions (oldest evicted). `POST /api/logout` drops it.
- Every POST needs the cookie **and** `X-CSRF-Token` equal to the session's
  token (constant time), and an `Origin`, when present, equal to the request's
  own origin. Every request's `Host` must be a loopback name or the bind
  address (DNS-rebinding defense: a rebinding page's requests carry its own
  host name).
- Who runs the SQL: Basic mode → the user (`impersonate` on each pooled
  connection, so RBAC applies statement by statement); token and no-auth mode
  → the trusted operator session (as in wave 78). `--ui-read-only` refuses
  every statement outside `serve::statement_allowed(…, false)` for everyone.

### Headers

Every response: `Content-Security-Policy: default-src 'none'; script-src
'self'; style-src 'self'; img-src 'self' data:; connect-src 'self';
base-uri 'none'; form-action 'self'; frame-ancestors 'none'`, plus
`X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`,
`X-Frame-Options: DENY`, `Cache-Control: no-store` on API responses.

### Running SQL

- `POST /api/query {sql, db?, run_id, worksheet_id?}` — parse the whole text
  first (a parse error runs nothing), then run each statement in order on one
  connection (so `USE`/`SET VARIABLE`/transactions carry over), stopping at the
  first error. Each result: kind, columns (name, type), rows (JSON: numbers
  that fit a double exactly as numbers, everything else as text, NULL as
  null), total row count, `truncated`, the wave-84 `QueryProfile`, elapsed.
  Shown rows are capped (`--ui-max-result-rows`, default 10,000); the
  engine's byte cap still bounds what a statement may materialize.
- `POST /api/cancel {run_id}` — sets the run's `CancelFlag`; only the session
  that started a run can cancel it. At most `--ui-session-queries` (default 2)
  runs per session at once (`429` over it).
- `POST /api/export {sql, db?, format: csv|json}` — re-runs **one read-only**
  statement with the download cap (`--ui-max-export-rows`, default 1,000,000)
  and answers it as an attachment. Used when the grid holds a truncated result.
- Each run is added to the history (`pinkdb_worksheets.history`).

### Worksheets

`pinkdb_worksheets` database, created on first use by the console's own
bookkeeping connection (trusted; parameters bound with prepared statements):

```sql
worksheets(id VARCHAR, owner VARCHAR, title VARCHAR, sql VARCHAR,
           created_at TIMESTAMP, updated_at TIMESTAMP, shared_with VARCHAR)
history(id VARCHAR, owner VARCHAR, worksheet_id VARCHAR, sql VARCHAR,
        started_at TIMESTAMP, elapsed_ms BIGINT, status VARCHAR,
        row_count BIGINT, error VARCHAR)
```

`owner` is `current_user()` of the session. A worksheet is visible to its
owner and to the users named in `shared_with` (`*` = everyone); only the
owner may change or delete it. Routes: `GET /api/worksheets`,
`POST /api/worksheets` (create/update), `POST /api/worksheets/delete`,
`GET /api/history`. A user's own SQL reaches these tables only through RBAC
(administrators); the console's filtering is what serves everyone else.

### Catalog

`GET /api/catalog?db=` — `pinkdb_catalog()` plus columns (`DESCRIBE`) plus
`pinkdb_column_stats()`, run as the session user. The completion list (the
editor's) is built from it plus a static keyword and function list.

### The ask box (agent)

- The server loads the agent extension with `--agent-extension PATH` (and
  memory with `--memory-extension PATH`); without it the UI shows "the agent
  is not loaded". `GET /api/agent` lists providers from `.agent providers
  json` — name, default model, configured or not; no key, no URL. No provider
  configured → the UI shows how to configure one (`PINKDB_AGENT_PROVIDER` and
  the key variable, on the server).
- `POST /api/ask {question, provider?, model?}` answers `text/event-stream`
  (read by `fetch` + a stream reader, so CSRF applies): `progress`,
  `tool` (the SQL, the reply code, the reply preview), `answer`, `done`,
  `error`. The run is `Connection::run_command("agent", "ask …")` on a
  connection logged in as the session user, with `serve::agent_policies`
  (read-only unless `--agent-allow-writes` and not `--ui-read-only`). Sessions
  go to `pinkdb_agent` as before. A closed stream cancels the run: the
  connection's cancel flag stops the running statement and every later one.
- One agent run per library at a time (the loader's pin): a second ask gets
  `409`.
- After a run, when memory is loaded, the question and answer are added as
  L0 turns under the caller's identity (the CLI's seam).

### Memory distillation worker

`--memory-distill-every SECS` (off by default; needs `--memory-extension`):
a server thread that, every interval, finds the users with pending memory
jobs and runs `.memory distill` for each, impersonated, under the memory
policies. Lives in `pinkdb-server` (no async, no UI dependency).

### Frontend

**Changed 2026-10-30 (the user's choice): Rust, not hand-written JS.** The
browser app is `crates/pinkdb-server-ui-web`, a Leptos (CSR) app compiled to
wasm32 with trunk, outside the workspace (own lockfile), committed gzipped
to `crates/pinkdb-server-ui/web-dist/` by `scripts/build-console.sh` and
embedded; a source-hash test keeps it fresh. The wire types and the SQL
tokenizer are the shared crate `crates/pinkdb-server-ui-api`. The CSP gains
`'wasm-unsafe-eval'` (compiling the page's own module; no `eval`). The
120 KB budget below assumed JS; the app is 255 KB gzipped. The design
below otherwise holds (the editor, grid, charts and panels are Leptos
components):

- **Editor:** `<textarea>` over a `<pre>` highlighting layer (same font
  metrics; the textarea's text is transparent, caret visible). A small SQL
  tokenizer (`sql.js` section) drives highlighting, statement-at-cursor and
  completion. Ctrl/Cmd+Enter runs the selection, else the statement at the
  cursor; Shift+Ctrl/Cmd+Enter runs everything. Completion popup on
  Ctrl+Space or after `.`: keywords, functions, databases, tables, columns.
- **Tabs:** one per worksheet; autosave (debounced) to the server.
- **Results:** one tab per statement result; a virtual-scrolled grid (fixed
  row height, only visible rows in the DOM), NULL styled, numbers right
  aligned, JSON/STRUCT/LIST/VARIANT pretty-printed in a cell viewer; row
  count, timing, profile; `EXPLAIN` output as an indented tree; CSV/JSON
  download (client-side, or server export when truncated).
- **Charts:** bar / line / scatter of chosen x and y columns, as SVG built
  with `createElementNS` (no `innerHTML` anywhere with data).
- **Catalog sidebar**, **history panel**, **ask panel** (live tool calls,
  "copy SQL into worksheet").
- All text from data goes through `textContent`; no `innerHTML` with data.
- Budget (for the original JS plan): JS + CSS under 120 KB uncompressed.

## Tests

`crates/pinkdb-server-ui/tests/worksheets.rs` (new) plus updates to
`console.rs`: login per mode, cookie flags, CSRF refusal (missing/wrong
token, foreign Origin, foreign Host), CSP and security headers on every
route, XSS in values and identifiers (JSON stays data; the shell never
interpolates), RBAC refusal through a worksheet, `--ui-read-only`, multi-
statement results and stop-at-error, cancel stops a long query, per-session
query limit, worksheet CRUD and per-user isolation (`shared_with`), history,
export, catalog (views/indexes/macros), agent ask via the localhost mock
provider (SSE events, tool SQL, key never in any response body), no provider
configured. Engine tests: cancel flag, impersonate, `pinkdb_catalog()` with
RBAC filtering. A the tokenizer's tests are Rust (`pinkdb-server-ui-api`).

## Demo

```bash
cargo build --release -p pinkdb-server --features ui -p pinkdb-agent
target/release/pinkdb-server --file ~/Desktop/projects/pinkdb-wt/bench/tpch_sf0.01/store.pink \
  --enable-ui --ui-port 8080
# then open http://127.0.0.1:8080/
```

## Gaps (stated up front)

- Lake attachments are session-local and attached from the CLI (`.lake
  attach`); the server has none to show unless it runs with `--catalog`. The
  catalog lists attached lake tables when present; version lists and the
  change feed stay SQL (`AT (VERSION => n)`, `lake_changes`).
- One agent run at a time per server (the extension loader's pin).
