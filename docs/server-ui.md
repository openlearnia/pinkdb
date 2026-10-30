# The warehouse console (waves 78 and 121)

`pinkdb-server` can serve a **web console** next to its binary protocol:
SQL worksheets, a catalog browser, the run history, and an ask box that runs
the data agent. It is **off by default** and lives behind the `ui` cargo
feature, so the default server binary is unchanged (the console crate is not
even compiled without the feature).

```sh
# build the server with the console (the `ui` feature is off by default);
# add `parquet,httpfs` for imports from paths, https:// and s3://
cargo build --release -p pinkdb-server --features ui,parquet,httpfs

# serve the console next to the protocol, on 127.0.0.1:8080
./target/release/pinkdb-server --file warehouse.pink --enable-ui
# pinkdb-server: warehouse console on http://127.0.0.1:8080 (worksheets)
```

Open the address in a browser and sign in with a user of the store. A store
without users starts in **first-run setup**: the server prints a setup code,
and the account you create with it becomes the administrator.

```
pinkdb-server-ui: first-run setup: the store has no users. Open http://127.0.0.1:8080 and create the administrator with the setup code OMRD-MSNQ-QH2F
```

Wave 78 shipped a read-only catalog browser. Wave 121 turned it into a
worksheet console: the browser app is now Rust (Leptos) compiled to
WebAssembly, the console runs the logged-in user's SQL, and it hosts the
agent.

## What the console does

- **Worksheets.** Tabs of SQL, saved as you type in the console's state
  store (below), private to their owner unless shared with
  named users (`share with: bob, carol`, or `*` for everyone; a shared
  worksheet is read-only for the others, who can duplicate it).
- **The editor.** Highlighting, completion of keywords, functions,
  databases, tables and columns from the catalog (`Ctrl+Space`, or as you
  type; `alias.` completes that table's columns), `Ctrl/Cmd+Enter` runs the
  statement at the cursor or the selection, `Shift+Ctrl/Cmd+Enter` runs
  everything, `Esc` cancels a running query, `Ctrl/Cmd+S` saves.
- **Results.** Every statement of a run gets its own result tab; a run stops
  at the first failing statement and shows it. The grid is virtual-scrolled
  (only the visible rows are in the page), NULLs are marked, numbers are
  right-aligned, and clicking a cell opens it whole — JSON, `STRUCT`, lists
  and `VARIANT` values pretty-printed. Each result shows its row count and
  timing; **Profile** shows wave 84's per-phase profile; an `EXPLAIN` result
  shows as a plan tree; **Chart** draws a bar, line or scatter chart of any
  x and numeric y column (SVG); **CSV** and **JSON** download the result.
- **Cancel.** A running query stops at its next chunk boundary (the same
  checkpoints as the statement timeout), so a cancelled statement fails
  before any write-back.
- **Catalog.** Databases with their tables, views, materialized views,
  indexes (hash and HNSW), triggers, macros and attached lake tables, each
  table's columns with types and statistics (NULLs, distinct estimate,
  min … max). Click a name to insert it; ▶ previews 100 rows; ⏱ on a lake
  table opens time travel (`AT (VERSION => n)`) and `lake_changes(…)`.
- **History.** Every run, newest first (status, timing, row count); click
  one to reopen its SQL.
- **Ask.** A question to the data agent, answered live: each statement the
  model runs appears with its reply and a "Copy SQL into worksheet" button,
  then the answer. See [the ask box](#the-ask-box).
- **Access** (wave 121b). Users, roles and grants; administrators create
  users and roles, change passwords, grant and revoke. See
  [access control](#access-control).
- **Import** (wave 121b). A CSV or Parquet file from a server path,
  `https://` or `s3://` (preview, then create, replace or append), or
  uploaded from the browser. See [imports](#imports).
- **Storage** (wave 121b). What the server stores into — memory, a durable
  `.pink` file, or a lake catalog (`.pink`, SQLite or Postgres) with local or
  S3 data — and the lake's published version; a Checkpoint button.

## Who runs the SQL

| Console mode | Statements run as |
|---|---|
| sign-in (the default) | the signed-in user — in the browser, or a script with that user's access token: every statement is checked against that user's grants (wave 48), exactly like a protocol client |
| `--ui-auth none` (a store without users, on loopback) | the implicit session of a store without users |

The password is verified once, at login, through the engine's own Argon2id
path; the console keeps a server-side session and never the password. Later
statements adopt the user with `Connection::impersonate`, so a dropped user's
session ends with them.

Every worksheet statement runs under the console's limits: a 5-minute
statement deadline, a 512 MiB materialization cap, at most 10,000 rows sent
to the browser per result (the full count is still reported; a CSV/JSON
export re-runs a single read-only statement with a 1,000,000-row cap), and
at most two queries running per session at once.

`--ui-read-only` limits **everyone** to queries (`SELECT`, `EXPLAIN`,
`DESCRIBE`, `SHOW`, `USE`), whatever their grants; a run with any other
statement is refused before its first statement runs.

## Signing in (wave 121d)

- **First-run setup.** A store without users shows *Create the
  administrator*: the setup code from the server's log, a name and a
  password. The account gets `GRANT ADMIN` and is signed in. The code is
  60 random bits, compared in constant time, throttled like a sign-in, and
  void once the store has a user (from the console or any client).
- **Passwords** are the store's own (Argon2id, verified by the engine). A
  password set in the console needs at least 10 characters. A user changes
  their own on the **Account** page (the current password first); every
  other session of theirs ends. An administrator's reset ends the user's
  sessions too.
- **Two-factor sign-in (TOTP).** On the Account page: scan the QR code with
  an authenticator app, confirm a code, and keep the ten recovery codes
  (shown once, stored hashed). Signing in then takes the password and a
  6-digit code (30-second steps, one step of drift, a code is never
  accepted twice) or a recovery code (once each). The challenge between the
  two steps lives 5 minutes, allows 5 tries and only from the same address.
  Turning it off needs the password; an administrator can reset it for a
  user who lost their phone (their sessions end).
- **Personal access tokens** are how scripts call the API: created on the
  Account page (a name; 7, 30, 90, 365 days or no expiry), shown once,
  stored as their SHA-256. `Authorization: Bearer pdbt_…` works on every
  `/api` route as the token's owner, with their grants and without a CSRF
  token (there is no cookie to ride on). A token cannot change a password,
  the second factor or tokens. HTTP Basic is not accepted: a password in a
  header would skip the second factor.
- **Sessions** are listed on the Account page (device, address, start, last
  use); a user signs out one of them or every other one, and an
  administrator signs a user out everywhere from the Access page, which
  also shows who uses two-factor.
- **Where the credentials live.** Token hashes, TOTP secrets and recovery
  code hashes are in the state store (below), written on trusted connections
  with bound parameters.
  A dropped user's rows go at the next sweep (at start, every minute, and
  whenever an administrator opens Access), so a later user of the same name
  starts clean.
- **HTTPS.** `--ui-tls-cert` and `--ui-tls-key` (PEM; rustls with ring)
  make the console serve HTTPS: its cookie is `Secure`, every response
  carries `Strict-Transport-Security`, and it may bind a non-loopback
  address. Plain HTTP on that port gets no answer.

## Security

The console runs SQL, so a page on another site must not be able to drive
it through your browser, and stored data must never become markup.

- **Sessions.** `POST /api/login` sets `pinkdb_session` — 256 random bits,
  `HttpOnly`, `SameSite=Strict`, `Path=/`, and `Secure` when the console is
  served over TLS. Sessions end after 30 minutes idle, 12 hours in any case,
  on logout, or when the oldest is evicted to make room (256 at most).
  Failed logins are throttled as in wave 78 (four verifications at once; 20
  failures in 60 s block the address for 60 s).
- **CSRF.** Every state-changing request is a `POST` that needs the cookie,
  the session's CSRF token in `X-CSRF-Token` (a separate 256-bit value,
  compared in constant time), and an `Origin`, when the browser sends one,
  equal to the console's own.
- **DNS rebinding.** Every request's `Host` must be a loopback name, the
  bind address, or a name given with `--ui-allowed-host`; anything else is
  `421`.
- **Output.** Every response carries `Content-Security-Policy: default-src
  'none'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self'; img-src
  'self' data:; connect-src 'self'; base-uri 'none'; form-action 'self';
  frame-ancestors 'none'` (no inline script, no `eval`; `wasm-unsafe-eval`
  only lets the page compile its own WebAssembly), plus `nosniff`,
  `X-Frame-Options: DENY` and `Referrer-Policy: no-referrer`. The API
  answers JSON only, and the app renders every value and identifier as a
  text node.
- **Requests.** Header count and size caps, a 10 s header and body read
  timeout, a 1 MiB body cap (a larger declared body is refused before it is
  read), a connection cap, and a deadline per request (30 s; 15 minutes for
  worksheet SQL, exports and agent runs).
- **Secrets.** The agent's provider key never reaches the browser, and the
  agent's and memory's own databases follow their wave 106b/109 policies
  (below). Worksheets, history and credentials live in the state store,
  which no statement reaches — not even an administrator's — and the console
  serves each user only their own worksheets and those shared with them.

## The state store (wave 121e)

The console's worksheets, run history and credentials, the agent's
transcript (`pinkdb_agent`) and memory's layers (`pinkdb_memory`) live in a
**separate store**, never in the warehouse: `SHOW DATABASES`, the catalog and
completion do not list them, and no statement on the warehouse can read,
write or drop them. The extensions are unchanged: their own (internal)
channel runs on the state store, as the caller — `current_user()` and
`current_roles()` there answer the warehouse's user, so memory stays per
user — while the model's statements run on the warehouse.

| Served store | State store |
|---|---|
| `--state PATH` | that file |
| `--file warehouse.pink` | `warehouse.state.pink` next to it |
| a local lake catalog (`pink:`, `sqlite:`, a directory) | next to the catalog (`cat.pink` → `cat.state.pink`) |
| a remote lake catalog (Postgres, S3) | **`--state` is required** (in memory, second factors and tokens would be lost on restart) |
| in memory | in memory |

A server opens it only when it runs the console, the agent or memory; the
CLI opens it on the first `.agent` or `.memory`, encrypted under the store's
key when the store is encrypted. Back it up with the warehouse. Existing
`pinkdb_worksheets`, `pinkdb_agent` and `pinkdb_memory` databases inside a
warehouse are not migrated: they become ordinary databases an administrator
may drop.

## Access control

The Access tab reads the engine's `pinkdb_users()` (name, kind, admin,
roles) and `pinkdb_grants()` (principal, privilege, object) **as the session
user**: an administrator — or the operator of a token or no-auth console —
sees every principal and grant, anyone else only themselves and the roles
they hold. Every change is a statement run as the session user (`CREATE ROLE`,
`GRANT … TO`, `REVOKE … FROM`, `DROP USER`), so the engine's RBAC decides
and a non-administrator gets its refusal. Creating a user or changing a
password goes through `POST /api/access/user`, never a worksheet, so the
password is not in the run history; a worksheet run whose SQL mentions a
password is recorded as `-- (a statement with a password: not recorded)`.

On a console started with `--ui-auth none`, creating the first user ends
that console: the store now has users, so unauthenticated sessions stop and
the server must be restarted without that flag. The form says so.

## Imports

- **From a path or URL.** The dialog previews `SELECT * FROM read_csv(…)` /
  `read_parquet(…)` and loads with `CREATE TABLE … AS SELECT …`, `DROP TABLE
  IF EXISTS` + create, or `INSERT INTO … SELECT …` — SQL run as the session
  user, so the engine's rule that file and URL reads are administrator-only
  holds (a non-administrator cannot make the server fetch a URL or read its
  files). On a durable store the dialog follows the import with `CHECKPOINT`,
  so the table no longer depends on the source being readable at a replay.
  Needs a server built with `httpfs` (CSV over a path, `https://`, `s3://`;
  remote Parquet) and/or `parquet`; `s3://` reads take the server's
  `AWS_*` environment.
- **Upload.** `POST /api/upload?table=&db=&format=&mode=` with the file as the
  body (up to `--ui-max-upload-mb`, default 64). The server parses it itself
  — CSV with the engine's own parser, Parquet through a scratch in-memory
  store — and writes the rows as the session user, in one transaction, with
  bound values: a new or replaced table is one `CREATE TABLE … AS SELECT …
  FROM (VALUES …)` (it needs `CREATE` on the database, exactly like a
  `CREATE TABLE AS`; pinkdb's RBAC grants a table's creator nothing on it,
  so a create-then-insert would need an `INSERT` grant that cannot exist
  yet), an append is batched `INSERT`s (it needs `INSERT`). No server file is
  read on the user's behalf, so an upload needs no `ADMIN`, and a durable
  store's WAL holds the rows, not a reference to a temporary file.

## Storage

`GET /api/storage` reports the mode (`in-memory`, `durable`, `lake`), the
location, and facts: for a lake the catalog backend (`.pink` file, `.pink` on
S3, SQLite, Postgres) and the data location, plus the lake's published
version, read per request. Credentials never appear: a URL's `user:password@`
is shown as `user:***@` and `password=`/`secret=`/`token=` parameters are
masked; S3 keys come from the environment and are never shown. The panel's
Checkpoint button runs `CHECKPOINT` as the user (an administrator action in a
console with logins).

The console works the same over every backend — the tests run worksheets and
uploads over a `.pink` catalog, a SQLite catalog, a Postgres catalog and S3
data, then reopen the lake under a new server and read the rows back.

## The exposure policy (fail closed)

| Situation | What happens |
|---|---|
| The console was not asked for | It is not started at all (the default). |
| `--ui-host` names a non-loopback address without `--ui-tls-cert` | **Refused** (exit 2): passwords and cookies would cross the network in clear text. |
| A non-loopback address with `--ui-auth none` | **Refused** (exit 2): a console on the network must ask for sign-in. |
| The store has no users (sign-in, the default) | The console starts in first-run setup and prints the setup code. |
| `--ui-auth none` and the store has users | **Refused** (exit 2). |
| `--ui-auth none` and the store has no users | The console starts and prints a **loud warning**: every local process can run any SQL. |
| The store gains a user while an `--ui-auth none` console runs | Its unauthenticated sessions end and new logins are refused. |
| `--ui-token` | **Refused** (exit 2): it was replaced by personal access tokens. |

## The ask box

Start the server with the agent extension:

```sh
cargo build --release -p pinkdb-agent
PINKDB_AGENT_PROVIDER=zai ZAI_API_KEY=… \
  ./target/release/pinkdb-server --file warehouse.pink --enable-ui \
  --agent-extension ./target/release/libpinkdb_agent.dylib
```

- **Providers come from the server's environment**, exactly as for the CLI
  (wave 106b). There is **no default provider**: unless the server sets
  `PINKDB_AGENT_PROVIDER`, the user picks one; with none configured the
  panel says which variables to set. The browser sees provider names and
  whether each is configured — never a key, a key prefix or a base URL.
- **The run is the user's.** The console runs `.agent ask` through the
  extension's command entry point on a connection logged in as the session
  user (`Connection::run_command`), so each statement the model runs is
  checked by the user's grants **and** by the agent policy: read-only unless
  the server was started with `--agent-allow-writes` (and never with
  `--ui-read-only`), never a write to the transcript database
  (`pinkdb_agent`), never a file or an external service. The store lock is
  taken per statement, so a model call between statements blocks nobody.
- **Live.** The run streams as server-sent events over the `POST` response
  (`progress`, `tool`, `answer`, `done`, `error`). Stopping the run (or
  closing the page) cancels it: the next statement is refused and a running
  one stops at its next chunk.
- **Memory.** With `--memory-extension libpinkdb_memory…` the question and
  the answer are added to the user's agent memory (wave 109), as the CLI
  does. `--memory-distill-every SECS` starts a worker that drains the
  distillation queue on that interval, running `.memory distill` as each
  owner with pending turns (off by default; it needs a provider in the
  environment and reports a missing one once).
- One agent run at a time per server (the extension loader runs one
  host-driven call per library); a second ask gets an error to retry.
- On macOS 27 a release build of the agent or memory library can fail to
  load with "mis-aligned LINKEDIT string pool" (the linker writes a string
  table the new loader rejects); `python3 scripts/macos-align-linkedit.py
  <library>` fixes the file.

## The endpoints

| Method and path | What |
|---|---|
| `GET /` | the shell: the stylesheet and the app's boot module, no data |
| `GET /query[/{worksheet}]`, `/catalog[/{database}/{object}]`, `/history`, `/access`, `/storage` | the same shell: the app's own URLs, so a reload, Back and Forward and a shared link keep the place (wave 121c) |
| `GET /assets/console.css`, `/assets/boot.js` | the stylesheet and the boot module |
| `GET /assets/pinkdb-console.js`, `/assets/pinkdb-console_bg.wasm` | the app, served gzip-encoded (`406` to a client that refuses gzip) |
| `GET /api/session` | the session (user, CSRF token, mode), or `401` with the login mode |
| `POST /api/setup` | the first account of a store without users (the setup code) |
| `POST /api/login`, `/api/login/verify`, `/api/logout` | the password (a session, or a two-factor challenge), the second factor, the end of a session |
| `GET /api/account` | the user's sessions, tokens and two-factor state |
| `POST /api/account/password`, `/api/account/sessions/end` | change the password; end one or every other session |
| `POST /api/account/tokens`, `/api/account/tokens/revoke` | create (the secret, once) or revoke a personal access token |
| `POST /api/account/totp/start`, `/confirm`, `/disable`, `POST /api/account/recovery` | enrol, turn on, turn off two-factor; new recovery codes |
| `POST /api/access/sessions/end`, `/api/access/totp/reset` | an administrator signs a user out everywhere, or resets their second factor |
| `POST /api/query` | run `{sql, db?, run_id, worksheet_id?}`; every statement in order until one fails |
| `POST /api/cancel` | cancel `{run_id}` (the session's own runs only) |
| `POST /api/export` | re-run `{sql, db?, format: csv\|json}` (one read-only statement) as a download |
| `GET /api/worksheets`, `POST /api/worksheets`, `POST /api/worksheets/delete` | list, create/update, delete worksheets |
| `GET /api/history` | the user's latest 200 runs |
| `GET /api/catalog` | every object the user may read, with columns and statistics (`pinkdb_catalog()` + `pinkdb_column_stats()`) |
| `GET /api/agent`, `POST /api/ask` | the agent's provider table; an agent run as an event stream |
| `GET /api/access`, `POST /api/access/user` | users, roles and grants as the session may see them; create a user or set a password |
| `POST /api/upload` | a CSV or Parquet file into a table (the body is the file) |
| `GET /api/storage` | the store's mode, location and facts (secrets masked) |
| `GET /api/databases`, `/api/schemas`, `/api/tables`, `/api/rows`, `/api/stats`, `GET /preview` | wave 78's browse API and no-JavaScript preview page: a session cookie or a personal access token |

The request and response types are one Rust crate,
`crates/pinkdb-server-ui-api`, shared by the server and the browser app.

## The browser app

`crates/pinkdb-server-ui-web` is a Leptos (client-side rendering) app
compiled to wasm32: the editor (a `<textarea>` over a highlighted layer, so
the browser's own editing, selection and undo work), the virtual grid, the
SVG charts and the ask panel. Its look follows the console design system
(`docs/console-design/README.md`, wave 121c): a section rail (Query,
Catalog, History, Access, Storage), an explorer, the workspace, an
inspector for Ask, and a status bar; tokens for color, type and spacing in
light and dark; one icon set; no inline styles (the Content-Security-Policy
forbids them). The SQL tokenizer, statement splitter and
completion context are plain Rust in the shared API crate, unit-tested
natively. Nothing is fetched from a CDN: the app, its stylesheet (system
fonts only) and its boot module are embedded in the server binary.

The app is **not** a workspace member (the workspace never compiles
Leptos). `scripts/build-console.sh` builds it with
[trunk](https://trunkrs.dev) and commits its gzipped output to
`crates/pinkdb-server-ui/web-dist/` with a hash of its sources; a test in
`pinkdb-server-ui` fails when the sources changed and the build was not
refreshed. So building the server needs no wasm toolchain.

| Asset | Size | Gzipped (served) |
|---|---|---|
| `pinkdb-console_bg.wasm` | 972 KB | 383 KB |
| `pinkdb-console.js` (wasm-bindgen glue) | 52 KB | 9 KB |
| `console.css` + `boot.js` | 55 KB | — |

## The worker model and the async carve-out

The console owns a **tokio runtime on its own thread** (it works with either
server transport), and every engine call runs through `spawn_blocking` on a
**bounded blocking pool** (`--ui-workers` runtime threads, default 4; a
blocking pool of 8): the async runtime never executes engine code. The
workspace's rule is zero async; the console is the documented carve-out
scoped to the I/O boundary, and the default server build does not compile
it (`cargo tree -p pinkdb-server` shows no tokio without `ui`).

## The preview renderer

Wave 78's `/preview` page and the result cells mirror the CLI's box
renderer: `NULL` is `NULL`, a `DOUBLE` prints `1.0`, and the preview page's
HTML comes from `maud`, which escapes every interpolated value.

## Known limits

- **Lake attachments are session-local** and made from the CLI (`.lake
  attach`); a server shows attached lake tables when its store has them,
  and time travel and the change feed are SQL (`AT (VERSION => n)`,
  `lake_changes`), not a version browser.
- **One agent run per server at a time** (above).
- **The browser holds at most 10,000 rows of a result**; the CSV/JSON
  buttons export the rest by re-running the statement, so they refuse
  anything but a single query.
- **A transaction does not span runs.** Each run has its own connection: a
  `BEGIN` left open at the end of a run is rolled back.
- **A user dropped and recreated through SQL within the same minute** keeps
  the old user's tokens and second factor (the sweep had no chance to run);
  an administrator can reset both.
- **Sessions live in memory**: a server restart logs everyone out
  (worksheets and history are in the store and survive).
- **Deep paging in `/preview` costs a scan** (`OFFSET` is applied after
  the scan).
- **Imports read CSV and Parquet** (the readers this server can register);
  JSON and Arrow files are not offered.
- **An upload is held in memory** while it is parsed (up to the upload cap).

## The flags

```
--state PATH                the state store (default: next to --file; see above)
--enable-ui                 serve the console (needs a build with `ui`)
--ui-host HOST              the console's bind address (default 127.0.0.1)
--ui-port PORT              the console's TCP port (default 8080; 0 = ephemeral)
--ui-auth password|none     sign in with the store's users (the default), or no
                            sign-in for a store without users on loopback
--ui-tls-cert PATH          serve HTTPS with this PEM certificate chain
--ui-tls-key PATH           the certificate's PEM private key
--ui-read-only              worksheets run only queries, for everyone
--ui-allowed-host NAME      another Host name to answer to (repeatable)
--ui-workers N              the runtime's worker threads (default 4)
--ui-max-connections N      the concurrent connections (default 64)
--ui-max-page-size N        the /preview and /api/rows page bound (default 500)
--ui-max-upload-mb N        the largest file an upload may send (default 64)
--agent-extension PATH      load the data agent for the ask box
--agent-allow-writes        let the agent write (under the user's grants)
--memory-extension PATH     load the agent memory
--memory-distill-every SECS drain the memory queue on this interval
```

## A demo

```sh
# a TPC-H SF 0.01 store (bench/suite generates one; any .pink file works)
python3 bench/suite/run.py --only tpch --scale 0.01 --runs 1 --warmup 0
cargo build --release -p pinkdb-server --features ui -p pinkdb-agent
./target/release/pinkdb-server --file <work-dir>/tpch_sf0.01/store.pink \
  --enable-ui --agent-extension ./target/release/libpinkdb_agent.dylib
# open http://127.0.0.1:8080/
```
