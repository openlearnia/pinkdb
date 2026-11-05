# Wave 138 — QuackLab's `/sql` adapter

Planned 2026-11-01 (QuackLab track, stream A). QuackLab's API, ETL and
deployer never link DuckDB: they talk JSON over HTTP to one process,
`quacklab-vps-duckdb` (Go, `grain/vps/cmd/quacklab-vps-duckdb`), through
`grain/vps/internal/vpsduck/client.go`. Swapping the engine therefore means
serving that exact HTTP contract from pinkdb — the "first seam" of
`docs/quacklab-compat-audit.md`. Nothing on QuackLab's side changes except the
container it deploys.

The contract below was read from the Go source and **checked against the
running Go service** (built from the grain repo, file mode, loopback):
response bodies quoted here are what it actually returns.

## The contract

### Endpoints

| Method, path | Auth | Purpose |
|---|---|---|
| `GET /health`, `GET /healthz` | none | Cloud Run probe (`/healthz` is eaten by Google's front end on `run.app`, so `/health` is the real one). `200 {"ok":true}`; `{"ok":true,"writerBusy":true}` when a writer holds the engine (never block the probe); `{"ok":true,"writeLeased":true}` while a write lease is held; `503 not_ready` when a `SELECT 1` fails. |
| `POST /sql` `{"sql": "..."}` | SQL token | Run a statement or script; the **last** statement's result is the answer. |
| `POST /recover` | SQL token | Close and reopen the warehouse (ends any write lease). `{"ok":true,"reopenMs":n}`. |
| `POST /write-lease/acquire` `{"owner","ttlSeconds"}` | loopback + lease token | Stop admitting `/sql`, drain in-flight requests, close the warehouse so an out-of-process writer owns the lake. `{"leaseId","owner","ttlSeconds","expiresAt","drainMs","acquireMs"}`; `409 warehouse_write_leased` if held; `503 lease_drain_failed` past `GRAIN_WRITE_LEASE_DRAIN_TIMEOUT` (default 5 min). |
| `POST /write-lease/renew` `{"leaseId","ttlSeconds"}` | same | Re-arm the TTL (clamped 30 s–6 h, default 60 min); `404 unknown_lease`. |
| `POST /write-lease/release` `{"leaseId"}` | same | Reopen the warehouse and admit writers. `{"ok":true,"heldMs","reopenMs"}`. |
| `POST /write-lease/sql` `{"leaseId","sql"}` | same | The holder's own SQL (DuckLake flush, mart validation) while everyone else gets 423. |

Write-lease routes exist only on the read-write service (`DUCKDB_READ_ONLY`
unset). A TTL watchdog ends a lease whose holder died. Lease bodies are capped
at 4 KiB.

### Auth

- `DUCKDB_SQL_TOKEN` set → `Authorization: Bearer <token>` required
  (constant-time compare) on `/sql` and `/recover`; `401 unauthorized`
  otherwise. Unset → open (local development only).
- Lease routes: loopback peers only (`403 loopback_only`), and the token is
  mandatory (`503 lease_auth_unavailable` when unset).
- `GRAIN_CR_SERVERLESS=1`: Cloud Run IAM has already authenticated the caller
  (the client sends a Google ID token in `Authorization` and
  `X-Serverless-Authorization`); the service skips its own bearer checks.

### `/sql` success body

```json
{"columns":["i"],"rows":[[0],[1]],"rowCount":7000,"truncated":true,
 "rowsAffected":0,"waitMs":0,"gateWaitMs":0,"poolWaitMs":0,"setupMs":0,
 "tookMs":3,"execMs":1,"rowScanMs":2,"serializationMs":0}
```

- At most `GRAIN_SQL_RESULT_ROW_LIMIT` rows (default 5,000, invalid → default,
  ceiling 100,000); the rest are counted, so `rowCount` is exact and
  `truncated` says whether rows were dropped.
- Statement results, as DuckDB returns them (checked):
  `CREATE TABLE/SCHEMA` → `columns ["Count"]`, no rows;
  `INSERT/UPDATE/DELETE/MERGE/COPY` → `["Count"]`, one row `[[n]]`, and
  `rowsAffected = n` (lifted from a lone `Count` column);
  `DROP`, `SET`, `BEGIN`/`COMMIT` scripts → `["Success"]`, no rows;
  `EXPLAIN` → `["explain_key","explain_value"]`.
- Cell encoding is the Go driver's `json.Marshal` of each scanned value:
  BIGINT/INTEGER/HUGEINT as JSON numbers, DOUBLE shortest form, VARCHAR and
  BLOB as strings, `null`; **DECIMAL as `{"Width":2,"Scale":1,"Value":15}`**;
  DATE as `"2026-01-02T00:00:00Z"`; TIMESTAMP as RFC 3339 nano in UTC
  (`"2026-01-02T03:04:05.123456Z"`); TIME as `"0001-01-01T01:02:03Z"`;
  INTERVAL as `{"days":3,"months":0,"micros":0}`; LIST/STRUCT as arrays and
  objects. A UUID comes out as its 16 raw bytes forced into a JSON string —
  mangled, unusable, so pinkdb sends the canonical text instead (the one
  deliberate difference; QuackLab casts UUIDs to VARCHAR where it reads them).

### Errors

`{"error":{"code","message"}}` with: `400 bad_json`, `400 empty_sql`,
`400 sql_error` (DuckDB's text, e.g. `Catalog Error: Table with name nope does
not exist!`), `423 warehouse_write_leased`, `401 unauthorized`,
`503 conn_error | recover_failed | not_ready`, `500 encode_error`. QuackLab
branches on parts of the message: `catalog error`, `does not exist`,
`not found`, `already exists`, `referenced column`, `unique` /
`duplicate key` (`vpsapi/refresh_history.go`, `ddl_mv_recovery.go`,
`deploy/ddl_sql.go`, `vpsdb/semantic_apps.go`). The client retries 429/503 on
a 4-minute budget and calls `/health` + `/recover` after a transport error.

### Session and concurrency

Every request starts in the warehouse database (`USE warehouse`; alias
`DUCKLAKE_ALIAS`), with any transaction a previous request left open rolled
back. The Go service serializes writers against readers itself (`duckGate`)
because its DuckDB build crashes on a concurrent read and write; pinkdb's
store already gives readers and one writer, so the adapter only needs the
admission count the lease drains on.

### Configuration (same environment names)

`DUCKDB_BIND` (default `127.0.0.1:1294`; on Cloud Run `0.0.0.0:$PORT`),
`DUCKDB_SQL_TOKEN`, `GRAIN_CR_SERVERLESS`, `DUCKDB_READ_ONLY`,
`GRAIN_SQL_RESULT_ROW_LIMIT`, `GRAIN_WRITE_LEASE_DRAIN_TIMEOUT`,
`GRAIN_DUCKLAKE`, `DUCKLAKE_ALIAS`, `DUCKLAKE_PG_URI`, `DUCKLAKE_DATA_PATH`,
`DUCKLAKE_ENCRYPTED`, `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` /
`R2_ENDPOINT` (and the AWS / Bunny variants), `DUCKDB_PATH` (file mode),
`DUCKDB_MEMORY_LIMIT`, `DUCKDB_THREADS`, `GRAIN_DUCKDB_SANDBOX`,
`GRAIN_DUCKDB_ALLOWED_PATHS`. DuckDB-only knobs (extension repository and
directory, `allow_unsigned_extensions`, `AUTOMATIC_MIGRATION`, the
pre-migration `pg_dump`) are accepted and ignored, logged once.

## Decisions

1. **Placement: a small crate, linked into pinkdb-server behind a feature.**
   `crates/pinkdb-quacklab` holds the router, handlers, lease runtime and
   result encoder over a `pinkdb::Database`; pinkdb-server gains feature
   `quacklab` and a `--quacklab-bind ADDR` flag (env `DUCKDB_BIND` honoured)
   that serves it next to (or instead of) the wire protocol, reusing the
   server's data directory, lake opener, extension set and graceful
   shutdown — the same shape as the console (`pinkdb-server-ui` behind `ui`).
   HTTP is axum + tokio + hyper, already in the tree for the console: no new
   dependency. Not in the default build.
2. **Warehouse lifecycle** (138c, re-scoped 2026-11-05: since 11-03 pinkdb
   has no DuckLake compatibility goal, and the test drive starts from a fresh
   lake pinkdb writes). Without `GRAIN_DUCKLAKE` the warehouse is a local
   database named `DUCKLAKE_ALIAS` in the server's store (`--data-dir`),
   created if missing. With `GRAIN_DUCKLAKE=1` (Go's truthy rule: `1`,
   `true`, `yes`; `PINKDB_QUACKLAB_LAKE=1` is the same switch) it is a **pinkdb
   lake database** (wave 124), built by `crates/pinkdb-quacklab/src/lake.rs`:
   - the object-store secret from the R2 / AWS / Bunny variables exactly as
     `r2S3SecretSQL` selects them, only for a remote `DUCKLAKE_DATA_PATH`, as a
     `PERSISTENT` secret (pinkdb's temporary secrets are per session and every
     request has its own session);
   - the catalog from `DUCKLAKE_PG_URI` (libpq conninfo or URL; its password
     becomes a `postgres` secret scoped to the catalog), optionally in its own
     Postgres schema (`PINKDB_LAKE_CATALOG_SCHEMA`), or any pinkdb catalog
     from `PINKDB_LAKE_CATALOG`;
   - `ATTACH 'lake:<catalog>' AS <alias> (DATA '<path>/', CREATE_IF_NOT_EXISTS)`
     (what `CREATE DATABASE … (TYPE LAKE, CATALOG, DATA)` makes on the first
     boot, the existing lake after), `READ_ONLY` instead on the read-only
     service. Each statement goes through `execBootstrap`'s retry (8 attempts,
     200 ms linear) on serialization conflicts and on the lake's writer lease;
     errors name the step, never the SQL.
   - Skipped, with no counterpart: `SET home_directory` / `extension_directory`,
     `INSTALL`/`LOAD`, `SET httpfs_connection_caching`, the five
     `CALL <alias>.set_option(…)` writes (Parquet compression/version describe
     files a pinkdb lake does not write; snapshot expiry has its own pinkdb
     maintenance). `DUCKLAKE_AUTOMATIC_MIGRATION` and the extension knobs are
     logged once as ignored. `DUCKLAKE_ENCRYPTED=1` **fails the boot** (pinkdb
     has no per-lake encryption; never silently unencrypted).
   - A write lease **detaches** the lake (releasing pinkdb's writer lease on the
     catalog so the external writer owns it); the holder's SQL and `release`
     re-attach it at the new head; `/recover` drains, detaches and attaches.
     A lake another writer still serves at boot (a revision rollout) leaves the
     warehouse closed instead of failing: requests answer `503 conn_error`
     (`/health` `503 not_ready`) until the first one after that writer lets go
     attaches it — Go's `admit` self-heal.
   - In the image, lake mode runs the server's own store `--in-memory`.
3. **Results through the same code path as the CLI's**: run the script on a
   per-request `Connection`, keep the last statement's result, map
   `QueryResult` kinds to DuckDB's `Count` / `Success` shapes, encode cells as
   the Go driver does (the table above), cap rows and count the rest.
4. **Errors.** pinkdb's error kind maps to DuckDB's prefix (`Catalog Error:`,
   `Parser Error:`, `Binder Error:`, `Conversion Error:`, `Constraint
   Error:`, `Invalid Input Error:`, `TransactionContext Error:`), so the
   substrings QuackLab branches on hold; wording inside the message stays
   pinkdb's unless a consumer needs a phrase (listed above — each gets a
   test).
5. **Sandbox.** `GRAIN_DUCKDB_SANDBOX=1` maps to pinkdb-server's existing
   policies: no `LOAD`/`INSTALL` from SQL, file reads and `COPY … TO` only
   under the warehouse's data, temp and landing paths plus
   `GRAIN_DUCKDB_ALLOWED_PATHS` (and the R2 buckets), settings locked.
6. **Container.** A `quacklab` variant of the repo Dockerfile: pinkdb-server
   with `quacklab,pinklake,s3,catalog-postgres,parquet,httpfs`, listening on
   `0.0.0.0:$PORT`, health on `/health`, SIGTERM → drain + checkpoint (Cloud
   Run gives 10 s). Measured: image size, cold start to first `/health`, idle
   RSS.
7. **Shadow run.** `bench/quacklab/shadow.py` replays one request log against
   both services — the Go service over DuckDB and pinkdb's adapter — and
   diffs the bodies (timings dropped; error messages compared by code and
   DuckDB prefix only). Corpus: the 117 golden statements in order with their
   setup, a typed-cell fixture, the write-lease choreography, and later a log
   captured from a QuackLab test environment (a `vpsduck` client option to
   mirror requests is a QuackLab change, proposed, not made here).

## Briefs

| Brief | Scope |
|---|---|
| 138a adapter | Decisions 1, 3, 4: crate, router, auth, `/sql`, `/health`, `/recover`, result encoding, limits, errors, against a local warehouse. |
| 138b lease and sandbox | The write-lease runtime (acquire/drain/renew/release/expiry/holder SQL), decision 5. |
| 138c lake warehouse | Decision 2, re-scoped 2026-11-05 to a pinkdb lake database (no DuckLake attach). |
| 138d image and shadow | Decisions 6 and 7. |

## Tests

Contract tests in `crates/pinkdb-quacklab/tests/` that spawn the router on an
ephemeral port: every endpoint, status and error code above; the row cap and
exact count; `Count`/`Success` shapes per statement kind; the cell encoding
per type; token and IAM modes; loopback-only lease routes; lease drain while
a slow query runs, 423 for other callers, holder SQL, renew, release, TTL
expiry, `/recover` ending a lease. The shadow harness against the real Go
service is the end-to-end check (the Go binary builds from the grain repo with
`go build ./cmd/quacklab-vps-duckdb`).
