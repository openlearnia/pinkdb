# Real-S3 integration harness — notes (`s3_docker.sh`)

Companion notes for `crates/pink-lake/tests/s3_docker.sh`. The server
measurements here (SigV4 enforcement, conditional-PUT atomicity, save timings)
were taken on this machine (macOS, Docker Desktop, 2026-10-14); treat the
numbers as evidence, not as constants.

## What the harness is

The in-repo `mock_s3.rs` (used by `cargo test -p pink-lake --features s3` and
the CLI's `lake_s3` tests) is the hermetic protocol suite — it even verifies
SigV4 on every request — but it is a home-grown server inside the test
process. `s3_docker.sh` is the independent check: a real, separate HTTP
server in a container, real sockets, driven through the release CLI
(`cargo build --release -p pinkdb-cli --features pinklake,s3`) exactly as a user
would. `SERVER=` picks which real server:

```sh
crates/pink-lake/tests/s3_docker.sh                    # adobe/s3mock (default)
SERVER=seaweedfs crates/pink-lake/tests/s3_docker.sh   # chrislusf/seaweedfs
SERVER=rustfs    crates/pink-lake/tests/s3_docker.sh   # rustfs/rustfs
KEEP=1 crates/pink-lake/tests/s3_docker.sh             # leave a container this run started; the next run reuses it
```

Knobs (see the script header): `SERVER`, `PORT` (per-server default 9090 /
8333 / 9100, auto-picks a free port when busy), `CONTAINER`, `BUCKET`,
`KEEP`, `PINKDB_BIN`, `SKIP_BUILD`, `ACCESS_KEY`/`SECRET_KEY`,
`WROWS` (race table size), `SLOWROWS`/`QUICKROWS` (deterministic conflict).

The script is **not wired into `cargo test`** (it needs Docker), so the
default suite is unchanged.

## What a lake is since wave 103 (what the assertions below rely on)

A lake has two halves, and only one of them is on S3:

* **The catalog** is a database on local disk, named by the `.lake`
  location: `pink:<path>` (a single file), a bare path (a `pink:` catalog at
  that path, with the default data location `<path>.files/`), `sqlite:<path>`,
  or a `postgres://` URL (the multi-machine form). Versions, snapshots,
  commit manifests and the head pointer all live in that database.
* **The data location** holds immutable `data/<table>/<uuid>.pink` data files
  and `.pinkdel` delete files. It may be a directory or an `s3://bucket/prefix`
  URL; the split form names both: `.lake save <catalog> data s3://bucket/prefix`.

An `s3://…` URL alone is **not** a lake: `.lake open`/`.lake save` refuse it
with `is a data location, not a lake catalog` and point at the split form, e.g.
`.lake save pink:lake.pink data s3://bucket/prefix` (or a `postgres://`
catalog for multi-machine lakes).

Nothing named `catalog.json`, `snapshots/`, `commits/`, `highest.json`,
`lake.json` or `moved.json` is written to S3 any more; the only non-data
object under a split lake's prefix is the `lake-data.json` ownership marker
(lake id) that cleanup verifies before deleting anything.

The harness reads that state with `pinkdb-cli`'s own commands (all of which
accept the catalog and print catalog state, never S3 metadata):

| command | what the harness asserts from it |
|---|---|
| `.lake versions <catalog>` | one line per version, `N` or `N (current)`; exactly `1..FINAL`, no gaps |
| `.lake info <catalog>` | `Lake <data location>`, `current version:`, `snapshots:       N (1, …, N)`, `tables (N):` and `    <table> (<rows> row[s])` |
| `.lake tables <catalog>` | `main.<table> (<rows> rows)` per table |
| `.lake files <catalog> [all]` | `data/…: table=… rows=… size=… schema_version=…` lines and (with `all`) a `N files (R referenced, U unreferenced)` summary; `all` adds every `data/` object no retained version references |
| `.lake orphans <catalog> [all] [dry-run\|confirm]` | `would delete <path>` / `deleted <path>` lines, `Deleted N object(s) from … (B bytes freed; still-referenced versions are untouched)`, or `Nothing to orphans in …` |

`referenced` in `.lake files all` means the file is in the listed (head)
manifest; `unreferenced` means **no retained version** references the object —
exactly the set `.lake orphans` deletes. The harness checks the head's one file
against the REST listing and compares the `unreferenced` count with the orphan
dry-run's `would delete` lines.

## The three servers: capability matrix and enforcement policy

The harness probes each endpoint **before any lake write** — an unsigned PUT
(SigV4 enforcement) — and sets its policy from the probe results, not from
the server name. Measured on this machine:

| server (container) | SigV4 rejected when unsigned? | harness policy |
|---|---|---|
| `s3mock` (`pinkdb-s3mock-itest`, default config) | **no** — unsigned PUT -> 200 (documented; the in-repo mock checks signatures separately) | SigV4 reported, not asserted |
| `seaweedfs` + `-s3.config` identities (`pinkdb-seaweed-itest`, started by the harness) | **yes** — unsigned -> 403, wrong-key signed -> 403 | SigV4 **asserted** (strict) |
| `seaweedfs` default config (a plain `pk-seaweed`, no identities file) | **no** — unsigned PUT -> 200, garbage signed -> 200 | SigV4 reported |
| `rustfs` (`pk-rustfs`) | **yes** — unsigned -> 403, wrong-key signed -> 403 | SigV4 **asserted** (strict) |

`STRICT` is now only about signatures: an unsigned PUT must be answered 403.
Every catalog and race assertion (one winner per version, gapless chain, the
head's table and row counts, the S3 object set, orphan cleanup) is a hard
failure on **every** endpoint, because it is decided by a local database
transaction rather than by an S3 conditional PUT. See the history section for
the pre-wave-103 measurements that used to justify a per-server policy here.

### Container recipes

The harness starts (or reuses) exactly one container per run. These are the
equivalent manual recipes:

```sh
# s3mock (no SigV4 check)
docker run -d --name pinkdb-s3mock-itest -p 127.0.0.1:9090:9090 \
  -e COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS=pinklake \
  adobe/s3mock:latest

# seaweedfs (strict; the file below is written into the run's temp dir)
cat > /tmp/seaweedfs-s3.json <<'JSON'
{ "identities": [ { "name": "pinkdb-itest",
  "credentials": [ { "accessKey": "anykey", "secretKey": "anysecret" } ],
  "actions": ["Admin", "Read", "Write", "List"] } ] }
JSON
docker run -d --name pinkdb-seaweed-itest -p 127.0.0.1:8333:8333 \
  -v /tmp/seaweedfs-s3.json:/etc/seaweedfs/s3.json:ro \
  chrislusf/seaweedfs:latest server -s3 -dir=/data \
  -s3.config=/etc/seaweedfs/s3.json

# rustfs (strict)
docker run -d --name pk-rustfs -p 127.0.0.1:9100:9000 \
  -e RUSTFS_ACCESS_KEY=rustfsadmin -e RUSTFS_SECRET_KEY=rustfsadmin \
  rustfs/rustfs:latest
```

Container lifecycle:

* **s3mock**: `pinkdb-s3mock-itest`, initial bucket via the env var; reuse of
  an existing container of that name works as before.
* **rustfs**: prefers the running `pk-rustfs` (the recipe above); starts it on
  `:9100 -> :9000` when absent, and reuses it as-is when it exists.
* **seaweedfs**: prefers a running `pk-seaweed` **only when it actually
  rejects unsigned writes with 403** (probed). A default-config `pk-seaweed`
  does not (see below), so the harness reports that, leaves it untouched, and
  starts its own identity-configured `pinkdb-seaweed-itest` (free host port
  when 8333 is taken). `CONTAINER=...` overrides the choice and then only
  *reports* whether that container verifies SigV4.
* Cleanup: a container **this run started** is removed on exit unless
  `KEEP=1`; a container that already existed is reused and left running (it
  was not started by this run). The script never touches another server's
  container. `KEEP=1` also keeps the run's temp dir (raw CLI logs + CSVs).

### Bucket-preexists caveat

The client **never creates buckets** (S3 bucket creation from the CLI is
future `INSTALL` work, out of scope here), and neither server auto-creates
one. The harness creates `BUCKET` (default `pinklake`) before the first lake
write — with an unsigned CreateBucket for s3mock, a SigV4-signed one for
seaweedfs/rustfs — tolerating `200`/`409` (already exists). Set `BUCKET=` to
a bucket that already exists on the server if you do not want the harness to
create one; a missing bucket otherwise fails the run with the server's HTTP
code.

## SeaweedFS's default config does not verify SigV4 (measured)

SeaweedFS's S3 API is anonymous-allowed unless it is started with identities
(`-s3.config=...`). Measured against the plain
`server -s3 -dir=/data` recipe (a running `pk-seaweed`):

```
$ curl -X PUT --data-binary x http://127.0.0.1:8333/pinklake/anon-probe
200
$ curl -X PUT --aws-sigv4 aws:amz:us-east-1:s3 --user wrongkey:wrongsecret ...
200
$ curl -X PUT -H 'Authorization: AWS4-HMAC-SHA256 Credential=garbage/...' ...
200
```

So an unsigned-PUT-must-403 assertion cannot hold on that container — but it
does hold on the same image started with a one-identity `-s3.config`:

```
unsigned PUT                       -> 403
signed, correct access/secret key  -> 200
signed, wrong key                  -> 403
```

That is why `SERVER=seaweedfs` starts its own identity container instead of
adopting an anonymous one, and why the strict run asserts the 403. A
pre-existing `pk-seaweed` that *does* enforce SigV4 is reused normally.
SigV4 with **any** configured key pair works against SeaweedFS — the key just
has to match its config; RustFS verifies the pair it was started with
(`RUSTFS_ACCESS_KEY`/`RUSTFS_SECRET_KEY`).

## History: S3 conditional writes are no longer in the write path

Before wave 103 the catalog was JSON on S3 and the one-winner rule was built on
`If-None-Match: *` / `If-Match` PUTs, so each server's conditional-write
behaviour mattered. That is gone: the catalog is a database transaction
locally, data files are written with unconditional PUTs, and the harness no
longer probes or asserts conditional semantics. The measurements below are
kept because they explain the old per-server policy and remain true of the
servers themselves.

`adobe/s3mock:latest` (5.x) supports the conditional headers — added for
PutObject in 4.3.0
([adobe/S3Mock#2371](https://github.com/adobe/S3Mock/issues/2371),
[PR #2382](https://github.com/adobe/S3Mock/pull/2382)) — and honoured them
**sequentially**:

| request | s3mock answer |
|---|---|
| `PUT` a new key with `If-None-Match: *` | 200, object created |
| `PUT` an existing key with `If-None-Match: *` | 412 |
| `PUT` with the current `If-Match: <etag>` | 200, replaced |
| `PUT` with a wrong `If-Match` | 412 |

But under **true concurrency** s3mock did not evaluate the precondition
atomically with the write:

```
# 60/60 trials (two parallel unsigned curls, fresh key each trial):
#   -H 'If-None-Match: *'  -> BOTH requests answered 200
$ for t in $(seq 1 60); do curl -X PUT -H 'If-None-Match: *' .../condrace/t$t & \
                             curl -X PUT -H 'If-None-Match: *' .../condrace/t$t & wait; done
both-succeeded: 60 ; exactly-one: 0 ; neither: 0
```

The same held for `If-Match` (4/4 duplicate wins for each header in the
regression run). Both strict servers answered 0/4 duplicate wins in the same
probe — and 0/20 in the raw two-writer race that preceded this work (RustFS
and SeaweedFS both evaluate the precondition with the write).

**Mechanism (from the S3Mock sources):** in
`server/src/main/kotlin/com/adobe/testing/s3mock/s3/controller/ObjectController.kt`,
`putObject` first calls
`objectService.verifyObjectMatching(bucketName, key, match, noneMatch)` — a
`getObject` + header comparison — and only then `objectService.putObject(...)`.
The store does have per-object locks (`ObjectStore.kt` wraps its operations in
`synchronized(lockFor(id))`), but the precondition check is outside that lock:
check-then-act. Two requests can both pass the check before either write
lands. AWS S3 / Cloudflare R2 / SeaweedFS / RustFS evaluate the condition as
part of the write commit.

## What the race asserts now

The race sections are all catalog-database checks; none of them reads a
metadata object from S3:

1. **Per attempt, win-xor-conflict.** All 9 outputs are parsed together: each
   writer either printed exactly one `Saved lake version N` and exited 0, or
   printed exactly one `Error: write conflict at <catalog>: …` whose text is
   one of the documented forms — the handle check
   (`another writer published version N … reopen the lake`), the store's
   commit (`the catalog head moved to N while this commit was based on M;
   rebase and retry`), or the engine's write lease (`… is busy`). Two writers
   claiming one version is a hard failure on every server.
2. **Gapless chain.** `.lake versions` lists exactly `1..FINAL`, with `FINAL`
   marked current; `FINAL` is the last version the race claimed.
3. **The head is the final claimant's save.** `.lake info` and `.lake tables`
   report `current version: FINAL`, one table (`main.t<writer>`), and that
   writer's row count — each save writes the whole lake from its own session
   store, so the winning version holds exactly its writer's one table.
4. **A settle save** reopens the catalog and publishes one more version
   alone (the documented "reopen the lake to continue"), which also pins the
   full-snapshot semantics: the settle save drops the race's table and
   publishes `main.settle`.
5. **The S3 side is data only.** Under `conc/` the harness asserts there is
   no `catalog.json`, `snapshots/`, `commits/`, `highest.json` or `lake.json`,
   that `lake-data.json` is present, that the data objects match
   `data/main.<table>/<uuid>.pink`, and that `.lake files` names exactly the
   head's one data file, which is in the listing.
6. **Leftovers are cleaned exactly.** `.lake files all` must agree with the
   bucket: exactly one `referenced` file, and its `unreferenced` objects are
   exactly the lines `.lake orphans all dry-run` would delete (the bucket holds
   `FINAL - 1` files of retained versions plus those leftovers). `all confirm`
   then reports exactly that many deletions and a second dry-run says
   `Nothing to orphans`. After that `conc/data/` holds exactly `FINAL - 1`
   objects — one per version above the empty v1 — `.lake files all` reports
   `1` file (`1 referenced, 0 unreferenced`), and `.lake versions` is
   unchanged.
7. **The slow-vs-quick pair** (below) still asserts exactly one winner of v3
   and the loser's documented conflict; both writers' data objects may remain
   (the loser's upload is a documented orphan) and the catalog must show
   version 3 with the winner's single table.
8. **Refusal.** `.lake open s3://…` and `.lake save s3://…` fail with
   `is a data location, not a lake catalog` and the split-form hint; a second
   `.lake save` in one session publishes the next version; the split form
   (`pink:` catalog + `data s3://…`) publishes normally, and its prefix holds
   data plus `lake-data.json` only.

### The slow-vs-quick pair needed a wider window on RustFS

The deterministic conflict (point 7) is a *timing* construction: the quick
writer must read the catalog (v2) while the slow writer's save is still in
flight. With the original `SLOWROWS=400000` the window is too narrow on a
fast local server — measured on this machine against RustFS:

```
slow: Imported 400000 rows in 0.008s
slow: Saved lake version 3 in 0.085s (1 table, 400000 rows, 2221716 bytes written)   # too fast
quick (started 0.1s later): Saved lake version 4 ...                                 # read v3, never raced
```

The slow writer's *whole* process finished in ~125 ms, so the quick writer's
catalog read landed after v3 was published: two clean saves of *different*
versions, no conflict. The harness now uses `SLOWROWS=4000000` and a 0.3 s
head start; on RustFS that is a ~1.5 s save window
(`Imported 4000000 rows in 0.186s`, `Saved lake version 3 in 1.501s (1 table,
4000000 rows, 22277590 bytes written)`), and the pair behaved deterministically
(2/2 manual trials + both full strict runs: the quick save wins v3, the slow
save gets the documented conflict). If the window is ever missed, the failure
message says so explicitly (both writers published different versions)
instead of blaming the client.

## MinIO images are not pullable right now

Measured on this machine (2026-10-14):

```
$ docker pull minio/minio:latest
Error response from daemon: pull access denied for minio/minio, repository
does not exist or may require 'docker login'
$ docker pull quay.io/minio/minio:latest
Error response from daemon: unknown: failed to resolve reference
"quay.io/minio/minio:latest": unexpected status from HEAD request to
https://quay.io/v2/minio/manifests/latest: 401 UNAUTHORIZED
```

So s3mock remains the default recipe, and SeaweedFS/RustFS are the
SigV4-verifying witnesses (`SERVER=seaweedfs|rustfs`). Any S3-compatible
endpoint works with the same script (`CONTAINER=`/`PORT=`, or point the CLI's
`AWS_ENDPOINT_URL` at your own server); for a real AWS/R2 bucket use the
opt-in live test:

```sh
PINKDB_S3_LIVE=1 PINKDB_S3_BUCKET=<bucket> PINKDB_S3_PREFIX=<prefix> \
AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_ENDPOINT_URL=… AWS_REGION=auto \
cargo test -p pink-lake --features s3 --test s3 live_s3_roundtrip -- --nocapture
```

The in-repo CLI test `concurrent_lake_saves_over_s3_stay_consistent`
(`crates/pinkdb-cli/tests/lake_s3.rs:190`, run with
`cargo test -p pinkdb-cli --features pinklake,s3`) is the hermetic version of
the same race: the same `pink:` catalog + `data s3://…` split form, one
winner per version, against the in-process mock. It does not depend on the
mock's conditional-PUT support.

## What is asserted where

| check | s3mock run | seaweedfs/rustfs run |
|---|---|---|
| unsigned PUT → 403 (SigV4 enforced) | reported (accepted, documented) | **enforced** |
| typed CSV round-trip, exact values in a fresh process | enforced | enforced |
| S3 holds data objects + `lake-data.json` only (no catalog/snapshots/commits) | enforced | enforced |
| local `pink:` catalog exists and `.lake info` reports the data location | enforced | enforced |
| `.lake versions` == `1..N`, `N` current, no gaps | enforced | enforced |
| race: win-xor-conflict per attempt, documented conflict forms | enforced | enforced |
| race: exactly one winner per version (catalog transaction) | enforced | enforced |
| race: head = final claimant's single table, its row count | enforced | enforced |
| `.lake files` head file ∈ REST listing; `.lake files all` unreferenced == orphan dry-run | enforced | enforced |
| leftover objects: dry-run count == confirmed deletes, second dry-run empty | enforced | enforced |
| after cleanup: `FINAL - 1` data objects; `.lake files all` = 1 referenced, 0 unreferenced | enforced | enforced |
| slow-vs-quick: exactly one winner of v3 + loser conflict | enforced | enforced |
| settle save: new version, single claimant, `main.settle` in the head | enforced | enforced |
| refusal: `s3://` alone is not a catalog; split form publishes | enforced | enforced |
| encrypted: sealed catalog on disk, containers on S3, no/wrong key refused | enforced (feature builds) | enforced (feature builds) |

Also asserted on every run: the bucket exists before the first write, and
every encrypted `enc/data/` object is a `PNKLAKE1` container.

## Other observations

* The CLI opens a **fresh lake handle per `.lake save`**, so a stale-handle
  conflict ("another writer published version N") is not reachable from a
  REPL session by running `.lake save` twice; the second save publishes a new
  version. The harness pins that explicitly, and covers the cross-process
  conflict with the slow-vs-quick pair.
* A conflicted writer may leave its already-uploaded data object behind
  (unreferenced, no retained version uses it). The concurrency section
  cleans those with `.lake orphans` and asserts the exact count; the
  slow-vs-quick prefix keeps them, documented.
* The first save into a fresh catalog publishes **version 2**: the CLI's
  `open_or_create` publishes the empty version 1 first.
* s3mock does not verify SigV4; the harness's s3mock REST calls are unsigned
  on purpose. Signature verification lives in the in-repo mock
  (`cargo test -p pink-lake --features s3`) and, for real sockets, in the
  `SERVER=seaweedfs`/`SERVER=rustfs` runs above (the harness's own probes and
  the CLI both sign there — `curl --aws-sigv4`
  `aws:amz:us-east-1:s3 --user <key>:<secret>` for the harness).
* Ports: the harness binds host ports it owns (fresh containers get
  `PORT` or a free one); a reused container's mapped port is discovered with
  `docker port`. A `KEEP=1` container bound elsewhere is reused, never
  rebound.
