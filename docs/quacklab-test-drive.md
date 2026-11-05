# QuackLab test drive: pinkdb behind `/sql`

How to run one QuackLab test environment's warehouse on pinkdb instead of
DuckDB, and how to go back. Written for wave 138 (2026-11-05). Nothing here has
been deployed: the user decides on the test drive, and QuackLab's own repo
(grain) is not changed by any of it.

## What gets swapped

A QuackLab Cloud Run environment (`go/internal/deploy/cr_serverless.go`) has
four pieces, all from one Grain image:

| piece | Cloud Run | args | talks to |
|---|---|---|---|
| `quack` | service, max 1 instance | `duckdb` | the DuckLake (Neon catalog + R2 data) |
| `quack-ro` | service, `DUCKDB_READ_ONLY=1` | `duckdb` | the same lake, read-only |
| API | service, max 2 instances | `api` | `DUCKDB_HTTP_URL` (quack), `DUCKDB_HTTP_URL_RO` (quack-ro) |
| ETL | job | `etl` | the API's env, so `DUCKDB_HTTP_URL` too |

The API and the ETL job only ever speak JSON over HTTP to `quack` and
`quack-ro` (`vps/internal/vpsduck`). The pinkdb image serves that same contract
(`docs/wave-138-plan.md`), so the test drive replaces **only the two quack
services**, as new services next to the existing ones, and points the API and
the ETL job at them. The DuckDB services, the DuckLake and its catalog stay
untouched, which is also the rollback.

pinkdb's warehouse is a **fresh pinkdb lake**: its own catalog tables (in their
own Postgres schema of the same Neon database, or another database) and its own
data prefix in the same R2 bucket. pinkdb does not open the existing DuckLake.
The environment's marts and tables are rebuilt by QuackLab's own deploy and ETL
runs against the new warehouse.

## Build the image

From a pinkdb checkout that has wave 138:

```bash
docker build --build-arg VARIANT=quacklab -t pinkdb:quacklab .
```

```bash
scripts/docker/smoke.sh --image pinkdb:quacklab --variant quacklab
```

The variant builds `pinkdb-server` with
`quacklab,parquet,httpfs,lake-s3,catalog-postgres` (the HTTP adapter, the
Parquet and CSV readers ETL loads with, S3 lake data and the Postgres lake
catalog) and the CLI. The entrypoint (`docker/entrypoint-quacklab.sh`) accepts
QuackLab's `duckdb` argument, serves `0.0.0.0:$PORT`, keeps the wire protocol on
loopback, and runs the server's own store `--in-memory` when the warehouse is a
lake (durable state is the lake, like the Go service's in-memory DuckDB).

Push it to the environment's Artifact Registry under a tag of its own (for
example `…/quacklab/pinkdb-quacklab:w138-<commit>`); the Grain image tag is not
reused.

Measured on this Mac (Docker Desktop, arm64, 2026-11-05), see
`~/Desktop/projects/pinkdb-wt/logs/w138-image.log`:

| | pinkdb:quacklab |
|---|---|
| image size | IMAGE_SIZE |
| time to first `200` on `/health` (median of 5 `docker run`s) | HEALTH_MS |
| idle RSS after boot | IDLE_RSS |

## Environment

The pinkdb service reads the Go service's variables. Copy the existing `quack`
service's environment (`gcloud run services describe <prefix>-quack --format
export`) and change only what this table says.

| variable | quack (Go) | pinkdb | notes |
|---|---|---|---|
| `GRAIN_DUCKLAKE` | `1` | `1` (keep) | turns on the lake warehouse; `PINKDB_QUACKLAB_LAKE=1` is the same switch |
| `GRAIN_CR_SERVERLESS` | `1` | `1` (keep) | Cloud Run IAM authenticates callers; the adapter skips its bearer check |
| `DUCKLAKE_ALIAS` | `warehouse` | keep | the database requests run in |
| `DUCKLAKE_PG_URI` | Neon conninfo | keep | the password is moved into a secret, never into a statement or log |
| `PINKDB_LAKE_CATALOG_SCHEMA` | — | **`pinkdb_lake`** | new: the pinkdb lake's catalog tables live in this Postgres schema, apart from DuckLake's |
| `DUCKLAKE_DATA_PATH` | `s3://<lake-bucket>/…` | **`s3://<lake-bucket>/pinkdb/`** | a prefix of its own |
| `DUCKLAKE_ENCRYPTED` | `1` | **`0`** | pinkdb has no per-lake encryption; the adapter refuses to boot with `1` rather than write unencrypted files silently (R2 still encrypts at rest) |
| `R2_ENDPOINT`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | set | keep | become `CREATE PERSISTENT SECRET r2_lake (TYPE s3, …, URL_STYLE 'path', REGION 'auto')`, as Go's `r2S3SecretSQL`; the AWS and Bunny variants (`GRAIN_OBJECT_STORE_PROVIDER`) work the same way |
| `DUCKDB_READ_ONLY` | `1` on quack-ro | keep | attaches `READ_ONLY` (see the gaps: it does not follow the writer) |
| `DUCKDB_SQL_TOKEN`, `GRAIN_SQL_RESULT_ROW_LIMIT`, `GRAIN_WRITE_LEASE_DRAIN_TIMEOUT`, `GRAIN_DUCKDB_SANDBOX` | as set | keep | same meaning |
| `DUCKLAKE_AUTOMATIC_MIGRATION`, `DUCKLAKE_PRE_MIGRATE_BACKUP_DIR`, `DUCKDB_MEMORY_LIMIT`, `DUCKDB_MAX_CONNS`, `DUCKDB_THREADS`, extension knobs | set | ignored | logged once at start |
| `PINKDB_LAKE_CATALOG` | — | optional | any pinkdb catalog location instead of `DUCKLAKE_PG_URI` (`postgres://…?schema=…`, `sqlite:…` for a single node) |

The boot log names the mode, never a credential:
`pinkdb-server: QuackLab adapter on http://0.0.0.0:8080 (read-write, mode=lake alias=warehouse data=s3://…/pinkdb/)`.

## Deploy (one test environment)

`<prefix>` is the environment's resource prefix (`crserverless.ResourceNamesFor`),
`<region>` and `<project>` its Cloud Run region and project. These are the
commands an operator would run; this wave ran none of them.

1. **Writer.** Deploy `quack` again under a new name with the pinkdb image and
   the environment above (same CPU, memory, service account and flags the
   deployer uses: `--execution-environment=gen2 --cpu-boost
   --no-allow-unauthenticated --min-instances=0 --max-instances=1
   --concurrency=80 --timeout=3600s`):

   ```bash
   gcloud run deploy <prefix>-quack-pinkdb --image <registry>/pinkdb-quacklab:w138-<commit> \
     --args=duckdb --region <region> --project <project> \
     --execution-environment=gen2 --cpu-boost --no-allow-unauthenticated \
     --min-instances=0 --max-instances=1 --concurrency=80 --timeout=3600s \
     --cpu=<quack cpu> --memory=<quack memory> --service-account <runtime SA> \
     --env-vars-file quack-pinkdb.env.yaml
   ```

   Keep `--max-instances=1`: a pinkdb lake has one writer at a time (below).
   Grant the API's service account `roles/run.invoker` on the new service, as
   the deployer does for `quack`.

2. **Reader.** Either deploy `<prefix>-quack-ro-pinkdb` the same way with
   `DUCKDB_READ_ONLY=1`, or — recommended for the first test drive, because a
   read-only pinkdb attachment does not see the writer's later commits — send
   the API's read traffic to the writer too (step 3).

3. **Point the API and the ETL job at it.** On the API service and the ETL
   job, set `DUCKDB_HTTP_URL` and `GRAIN_DUCKDB_ID_TOKEN_AUDIENCE` to the new
   writer's URL, and `DUCKDB_HTTP_URL_RO` to the same URL (the API then uses
   one client, `vps/cmd/quacklab-vps-api/main.go`):

   ```bash
   gcloud run services update <prefix>-api --region <region> --project <project> \
     --update-env-vars DUCKDB_HTTP_URL=<pinkdb url>,DUCKDB_HTTP_URL_RO=<pinkdb url>,GRAIN_DUCKDB_ID_TOKEN_AUDIENCE=<pinkdb url>
   ```

   ```bash
   gcloud run jobs update <prefix>-etl --region <region> --project <project> \
     --update-env-vars DUCKDB_HTTP_URL=<pinkdb url>,DUCKDB_HTTP_URL_RO=<pinkdb url>,GRAIN_DUCKDB_ID_TOKEN_AUDIENCE=<pinkdb url>
   ```

4. **Fill the warehouse.** Run the environment's deploy DDL and one ETL run
   from QuackLab as usual; they now land in the pinkdb lake. A QuackLab
   redeploy of the environment rewrites the API's and the job's environment
   back to the DuckDB services: repeat step 3 after one, or don't redeploy
   during the test drive.

5. **Check.** `GET <pinkdb url>/health` with an ID token answers
   `{"ok":true}`; the API's logs show `duck=<pinkdb url>`; dashboards load.

## Rollback

Nothing on the DuckDB side changed, so rolling back is re-pointing:

```bash
gcloud run services update <prefix>-api --region <region> --project <project> \
  --update-env-vars DUCKDB_HTTP_URL=<quack url>,DUCKDB_HTTP_URL_RO=<quack-ro url>,GRAIN_DUCKDB_ID_TOKEN_AUDIENCE=<quack url>
```

```bash
gcloud run jobs update <prefix>-etl --region <region> --project <project> \
  --update-env-vars DUCKDB_HTTP_URL=<quack url>,DUCKDB_HTTP_URL_RO=<quack-ro url>,GRAIN_DUCKDB_ID_TOKEN_AUDIENCE=<quack url>
```

(or redeploy the environment from QuackLab, which writes those values itself).
Then delete the pinkdb services. Writes made during the test drive exist only
in the pinkdb lake: the DuckDB warehouse is as it was before the test drive,
and the next ETL run brings it up to date. To remove the pinkdb lake, drop the
`pinkdb_lake` schema in the catalog database and delete the
`s3://<lake-bucket>/pinkdb/` prefix.

## How it behaves

- **Boot.** Secrets first, then `ATTACH 'lake:<catalog>' AS warehouse (DATA
  '…/', CREATE_IF_NOT_EXISTS)`: the first boot creates the lake, later boots
  open it. Each statement goes through Go's `execBootstrap` retry (8 attempts,
  200 ms linear backoff) on Postgres serialization conflicts and on the lake's
  writer lease. A failure exits the process naming the step, never the SQL.
- **One writer.** A writable pinkdb lake holds a writer lease in its catalog
  (heartbeat 1 s, stale after 6 s). During a Cloud Run revision rollout the new
  instance starts while the old one still serves: it boots without the lake,
  answers `/sql` with `503 conn_error` and `/health` with `503 not_ready`
  (QuackLab's client retries 503s for four minutes), and attaches on the first
  request after the old instance's SIGTERM releases the lease. Cloud Run's
  default startup probe is the TCP port, which is open, so the rollout
  proceeds.
- **Write leases.** `acquire` drains, then **detaches** the lake (its writer
  lease is released, so an outside writer can take it); the holder's
  `/write-lease/sql` re-attaches it lazily; `release` attaches it at the new
  head. `/recover` drains, detaches and attaches. The lease routes stay
  loopback-only, as in Go.
- **Writes** go to the lake on every commit (one lake version per committed
  statement or transaction).

## Known not to work yet

In order of how much they block a real test drive; each has a one-line repro
in the wave 138 hand-off (`~/Desktop/projects/pinkdb-wt/logs/w138-gaps.md`).

GAPS_LIST
