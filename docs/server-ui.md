# The warehouse browsing UI (wave 78)

`pinkdb-server` can serve a **read-only web console** next to its binary
protocol: a small browser UI that shows the store's databases, relations,
row previews and statistics.

The console is **opt-in**, **feature-gated** and **local-only by default**.
It is a browsing tool, not a query console: there is no endpoint that runs
a statement the operator did not write, and no endpoint that mutates
anything.

```sh
# build the server with the console (the `ui` feature is off by default)
cargo build --release -p pinkdb-server --features ui

# serve the console next to the protocol, on 127.0.0.1:8080
./target/release/pinkdb-server --enable-ui
# pinkdb-server: warehouse console on http://127.0.0.1:8080 (read-only)
```

Open <http://127.0.0.1:8080> in a browser.

## The exposure policy (fail closed)

The console refuses to run in a configuration it cannot protect:

| Situation | What happens |
|---|---|
| The console was not asked for | It is not started at all (the default). |
| `--ui-host` names a non-loopback address | **Refused** (exit 2): the console is a browser UI and cannot use the server's TLS transport, so a non-localhost exposure needs console TLS (wave 46) *and* RBAC (wave 48) first. |
| The store has users (the wave-48 RBAC catalog) and no credential was configured | **Refused** (exit 2): start it with `--ui-auth basic` or `--ui-token <secret>`. |
| `--ui-auth basic` and the store has no users | **Refused** (exit 2): there is nothing to authenticate against. |
| The store has no users and no credential was configured | The console starts and prints a **loud warning**: every request can read the whole store. |
| The store gains a user while the console runs | The console refuses to serve it (the empty-catalog policy is re-checked per request). |

`--ui-auth basic` verifies the password through the **engine's own path**
(`pinkdb::Connection::login`, the same Argon2id verification the wire
protocol uses) and then runs the console's statements **under that user's
grants**: RBAC applies to the console exactly as it applies to a wire
client (a user without the `SELECT` privilege can list the catalog but gets
`permission denied` on a row preview).

`--ui-token TOKEN` is an operator-level credential: every API request must
carry `Authorization: Bearer <token>` (compared in constant time). The token
is independent of the store's catalog, so the console's statements run with
the engine's trusted-session semantics — a token holder sees everything the
store holds. The two modes are mutually exclusive.

The console's login throttling mirrors the pass-5 policy of the wire
server: at most four password verifications run at once (a fifth gets
`too many concurrent logins`), and a client address that accumulates 20
failures inside 60 seconds is blocked for 60 seconds — **before** any
hashing happens.

## The endpoints

Every route is `GET` (and, through the HTTP layer, `HEAD`). Anything else is
`405` with an `Allow: GET, HEAD` header; an unknown path is `404`.

| Route | What it returns |
|---|---|
| `GET /` | The console shell (server-rendered HTML; the assets are self-hosted). |
| `GET /preview?db=&table=&page=&page_size=` | A server-rendered row preview (the no-JavaScript fallback). |
| `GET /assets/console.css` | The stylesheet (embedded in the binary). |
| `GET /assets/console.js` | The script (embedded in the binary). |
| `GET /api/databases` | `{"current": "main", "databases": [{"name": "main", "current": true}]}` |
| `GET /api/schemas?db=` | `{"db": "main", "schemas": [{"name": "main", "relations": 3}]}` — pinkdb has no schema layer, so every database holds its relations in the implicit schema `main`. |
| `GET /api/tables?db=&schema=` | `{"db": …, "schema": "main", "tables": [{"name": …, "columns": [{"name": …, "type": "INTEGER", "nullable": true}]}]}` |
| `GET /api/rows?db=&table=&page=&page_size=` | `{"db": …, "table": …, "page": 1, "page_size": 50, "total_rows": 1234, "total_pages": 25, "columns": [...], "rows": [[{"text": "1", "null": false, "truncated": false}]]}` |
| `GET /api/stats?db=` | `{"db": …, "tables": [{"name": …, "rows": 1234, "estimated_bytes": 24680, "columns": 3}]}` |

Paging is bounded: `page` is 1-based (`page=0` is a `400`) and `page_size`
is clamped to the console's maximum (`--ui-max-page-size`, default 500).
`total_rows` is the **exact** row count (`SELECT COUNT(*)`), and it is the
same on every page.

## Read-only by construction

The console's store access runs exactly these statements:

```sql
SHOW DATABASES
SHOW TABLES
DESCRIBE "db"."table"
SELECT COUNT(*) FROM "db"."table"
SELECT * FROM "db"."table" LIMIT n OFFSET m
```

There is no DML and no DDL, and the router has no `POST`/`PUT`/`DELETE`
route at all. The one session statement the console uses is `USE` (to
browse a database other than the session's current one): it changes no
table, no row and no catalog entry — on a durable store the engine appends
a *session record* to the WAL so a replay reproduces the same session. The
console pools its sessions, so the switch happens once per pooled session
and database, not once per request.

Every store read runs under the engine's own limits (the pass-5 ones): a
30 s statement deadline, a 1 000-row materialized-result cap and a 16 MiB
materialized-byte cap. The store lock is **released** before any HTTP work
happens: the engine call returns materialized rows, and the JSON/HTML
rendering runs afterwards, outside the lock.

## The preview renderer

The console **mirrors** the repository's box renderer (the CLI's
`format_select`/`display_value`/`sanitize_cell`):

- a `NULL` cell renders as the text `NULL`;
- a `DOUBLE` renders DuckDB-style (`1.0`, not `1`);
- a `VARCHAR` cell never carries a raw control character (a newline becomes
  a space, every other control character is shown as `\u{..}`).

The renderer lives in the CLI's *binary* crate, so wave 78 mirrors the
rules and pins them with tests instead of moving the CLI's renderer into a
shared crate. The one addition: a per-cell character cap (512), because the
terminal box sizes a column to its widest cell and a browser table cannot.
A truncated cell is marked (`"truncated": true`).

Untrusted cell content is **never** rendered as markup: the HTML is
generated by `maud`, which escapes every interpolated value, and the
script inserts cell values with `textContent`, never `innerHTML`. A cell
containing `<script>` is shown as text.

## The worker model and the limits

The console owns a **tokio runtime on its own thread**: it does not care
whether the server next to it runs the blocking (`sync`) transport or the
tokio transport (`async-server`, wave 47).

- The runtime's worker threads and its **blocking-pool size are set
  explicitly** (`--ui-workers`, default 4; the blocking pool, default 8).
  The engine is synchronous, so every store read runs through
  `spawn_blocking` on that bounded pool: the async runtime never executes
  engine code, and a slow scan cannot stall the reactor.
- The accept loop caps the concurrent connections (`--ui-max-connections`,
  default 64): over the cap the connection is refused with a `503`.
- hyper caps the request headers (64 headers, 16 KiB each) and enforces
  the header read timeout (10 s), so a slow client cannot hold a
  connection open.
- Every request has a deadline (30 s) that covers the head, the body and
  the response.
- A request that declares a body over the limit (64 KiB) is refused with a
  `413` at the edge — the console never reads a body at all.

## The async carve-out

The workspace's design rule is **zero async**: the engine, the planner, the
storage and the default server are synchronous. The console is the second
documented carve-out (after wave 47's `async-server` transport), scoped to
the I/O boundary only:

1. The console owns its own runtime, on its own thread. The default build
   of `pinkdb-server` does not even compile the console crate, so the
   lean server binary is unchanged (`cargo tree -p pinkdb-server` shows no
   tokio without the `ui` feature).
2. The engine stays synchronous: every store read is a `spawn_blocking`
   call on the bounded blocking pool.
3. No protocol change, no engine change, no async storage: the console is
   an HTTP surface around the existing synchronous engine.

## Known limits (this wave)

- **No query console.** The console browses; it cannot run SQL. That is
  the explicit follow-up of this wave, and it needs its own authorization
  story (a query console is an arbitrary-statement surface, not a
  read-only one).
- **The sizes are estimates.** The engine keeps no byte-size accounting (a
  table knows its row count and its chunks, not its bytes), so
  `estimated_bytes` is the row count times the sum of the columns' type
  widths (an average inline string is counted as 16 bytes).
- **Views and tables are listed together.** pinkdb's `SHOW TABLES` merges
  them and the console uses that statement, so a view appears in the
  relation list like a table (its rows are previewed through the view).
- **A token cannot open the server-rendered preview page.** A browser
  cannot add a bearer header to a top-level navigation; the interactive
  console asks for the token once and sends it with every API call, and
  `curl -H "Authorization: Bearer …" /preview?…` works for scripts.
- **Deep paging costs a scan.** The engine applies `OFFSET` after the scan,
  so a far page of a large table reads the table's chunks.

## The flags

```
--enable-ui                 serve the console (needs a build with `ui`)
--ui-host HOST              the console's bind address (default 127.0.0.1)
--ui-port PORT              the console's TCP port (default 8080; 0 = ephemeral)
--ui-auth basic             HTTP Basic against the store's users
--ui-token TOKEN            a bearer token for the API
--ui-workers N              the runtime's worker threads (default 4)
--ui-max-connections N      the concurrent connections (default 64)
--ui-max-page-size N        the row-preview page-size bound (default 500)
```
