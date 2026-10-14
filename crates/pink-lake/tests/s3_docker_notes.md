# Real-S3 integration harness — notes (`s3_docker.sh`)

Companion notes for `crates/pink-lake/tests/s3_docker.sh`. Everything here was
measured on this machine (macOS, Docker Desktop 29.8.2, `adobe/s3mock:latest`,
2026-10-14); treat the numbers as evidence, not as constants.

## What the harness is

The in-repo `mock_s3.rs` (used by `cargo test -p pink-lake --features s3` and
the CLI's `lake_s3` tests) is the hermetic protocol suite — it even verifies
SigV4 on every request — but it is a home-grown server inside the test
process. `s3_docker.sh` is the independent check: a real, separate HTTP
server in a container, real sockets, driven through the release CLI
(`cargo build --release -p pinkdb-cli --features lake,s3`) exactly as a user
would.

```sh
crates/pink-lake/tests/s3_docker.sh          # fresh container, removed at exit
KEEP=1 crates/pink-lake/tests/s3_docker.sh   # leave the container; the next run reuses it
```

Knobs (see the script header): `PORT` (default 9090, auto-picks a free port
when busy), `CONTAINER`, `BUCKET`, `KEEP`, `PINKDB_BIN`, `SKIP_BUILD`,
`WROWS` (race table size), `SLOWROWS`/`QUICKROWS` (deterministic conflict).

The script is **not wired into `cargo test`** (it needs Docker), so the
default suite is unchanged.

## Container lifecycle

* Name: `pinkdb-s3mock-itest` (label `pinkdb-s3-itest=1`), image
  `adobe/s3mock:latest`, initial bucket `pinklake`
  (`COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS`).
* Host port: `PORT` if set (must be free, else a clear failure); otherwise
  9090, else a free port is picked and logged. It never touches a container
  it did not create (the manual `pinkdb-s3mock` can keep its 9090).
* Reuse: an existing container of that name is started (if stopped) or used
  as-is (if running), and its mapped port is discovered with `docker port`. A
  container under that name with a different image is a clear failure.
* Cleanup: `trap ... EXIT INT TERM` removes the container unless `KEEP=1`.
  A `KEEP=1` leftover belongs to the harness and is removed by the next
  non-KEEP run. `KEEP=1` also keeps the run's temp directory (the raw CLI
  logs and generated CSVs) for inspection; without it the directory is
  removed on exit (a failure has already printed the relevant log).
* Readiness: retry a CreateBucket + unsigned PUT/GET probe (up to 60 s);
  s3mock takes ~1.5 s to start under Docker.

## s3mock conditional writes: sequential yes, atomic no

`adobe/s3mock:latest` (5.x) supports the conditional headers the single-writer
contract uses — added for PutObject in 4.3.0
([adobe/S3Mock#2371](https://github.com/adobe/S3Mock/issues/2371),
[PR #2382](https://github.com/adobe/S3Mock/pull/2382)) — and the harness
verifies the *sequential* semantics before any lake write:

| request | s3mock answer |
|---|---|
| `PUT` a new key with `If-None-Match: *` | 200, object created |
| `PUT` an existing key with `If-None-Match: *` | 412 |
| `PUT` with the current `If-Match: <etag>` | 200, replaced |
| `PUT` with a wrong `If-Match` | 412 |

But under **true concurrency** s3mock does not evaluate the precondition
atomically with the write:

```
# 60/60 trials (two parallel unsigned curls, fresh key each trial):
#   -H 'If-None-Match: *'  -> BOTH requests answered 200
$ for t in $(seq 1 60); do curl -X PUT -H 'If-None-Match: *' .../condrace/t$t & \
                             curl -X PUT -H 'If-None-Match: *' .../condrace/t$t & wait; done
both-succeeded: 60 ; exactly-one: 0 ; neither: 0
```

The same holds for `If-Match` (the harness's atomicity probe races both
headers, 4 trials each, and observed 3–4/4 duplicate wins per header in every
run).

**Mechanism (from the S3Mock sources):** in
`server/src/main/kotlin/com/adobe/testing/s3mock/s3/controller/ObjectController.kt`,
`putObject` first calls
`objectService.verifyObjectMatching(bucketName, key, match, noneMatch)` — a
`getObject` + header comparison — and only then `objectService.putObject(...)`.
The store does have per-object locks (`ObjectStore.kt` wraps its operations in
`synchronized(lockFor(id))`), but the precondition check is outside that lock:
check-then-act. Two requests can both pass the check before either write
lands. AWS S3 / Cloudflare R2 evaluate the condition as part of the write
commit (that is exactly what the README's "Conditional writes are the
conflict machinery" bullet relies on).

### What the harness does about it

1. It probes atomicity up front and **says so** (the probe result is printed;
   no silent assumption).
2. The 3×3 race still asserts everything that does not need server atomicity:
   every attempt is win-xor-conflict, every loser's message is one of the
   documented conflict forms, and the published version chain is gapless.
   On a server that upholds atomic conditionals, duplicate publications are a
   hard failure (they would be a real bug). On a non-atomic server the
   duplicates are reported as the measured server limitation, with the
   `PINKDB_S3_LIVE=1` / in-repo-mock pointer for the enforced check.
3. A **deterministic** same-version conflict is asserted regardless of
   atomicity: a slow save (400 000 rows) opens the lake at v2, a quick save
   publishes v3 while the slow writer is still encoding, and the slow
   writer's own snapshot PUT then correctly gets 412 (it is a plain
   "does this key exist" case by the time it runs — no overlap window). The
   harness asserts exactly one winner of v3, the loser's conflict, and
   `catalog.json == snapshots/3.json` bytes.
4. A **settle save** after the race reopens the lake and publishes one more
   version alone (the documented "reopen the lake to continue"), so the
   final version of the race prefix has exactly one claimant and the
   final-version `catalog.json == snapshots/<v>.json` byte check is enforced
   on any server.

Observed effect on the race (four full runs of the script): 7 claims/2
conflicts/2 duplicate versions, 5/4/2, then 3/6/**0** (the strict
one-winner assertion held and the run reported it), then 4/5/1. The duplicate
count depends on how much the three writers' conditional PUTs overlap, which
is exactly the server's window — the controlled same-version conflict and the
settle save assertions passed in all four runs.

## MinIO images are not pullable right now

Measured on this machine (2026-10-14):

```
$ docker pull minio/minio:latest
Error response from daemon: pull access denied for minio/minio, repository
does not exist or may require 'docker login'
$ docker pull quay.io/minio/minio:latest
Error response from daemon: unknown: failed to resolve reference
"quay.io/minio/minio:latest": unexpected status from HEAD request to
https://quay.io/v2/minio/minio/manifests/latest: 401 UNAUTHORIZED
```

So s3mock is the documented recipe. Any S3-compatible endpoint works with
the same script (set `PORT`/`CONTAINER` or point `AWS_ENDPOINT_URL` at your
own server); for a real AWS/R2 bucket use the opt-in live test:

```sh
PINKDB_S3_LIVE=1 PINKDB_S3_BUCKET=<bucket> PINKDB_S3_PREFIX=<prefix> \
AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_ENDPOINT_URL=… AWS_REGION=auto \
cargo test -p pink-lake --features s3 --test s3 live_s3_roundtrip -- --nocapture
```

The in-repo CLI test `concurrent_lake_saves_over_s3_stay_consistent`
(`cargo test -p pinkdb-cli --features lake,s3`) is the hermetic version of the
one-winner assertion, against the mock's conditional writes.

## What is asserted where

| check | server atomicity needed | s3mock run |
|---|---|---|
| typed CSV round-trip, exact values in a fresh process | no | enforced |
| published object set via unsigned `?list-type=2` | no | enforced |
| `catalog.json == snapshots/2.json` (round-trip lake) | no | enforced |
| race: win-xor-conflict per attempt, documented conflict forms | no | enforced |
| race: version chain gapless, `.lake versions` == 1..V | no | enforced |
| race: exactly one winner per version | **yes** | reported (duplicates = measured server quirk) |
| slow-vs-quick: exactly one winner of one version + loser conflict | no (see above) | enforced |
| slow-vs-quick: `catalog.json == snapshots/3.json` | no | enforced |
| settle save: final version single-claimant, bytes equal | no | enforced |
| second `.lake save` → new version (same process) | no | enforced |
| planted stray snapshot → `snapshot N already exists …` refusal, then a clean save after removing it | no | enforced |

## Other observations

* The CLI opens a **fresh lake handle per `.lake save`**, so a stale-handle
  conflict ("another writer published version N") is not reachable from a
  REPL session by running `.lake save` twice; the second save publishes a new
  version. The harness pins that explicitly, and covers the cross-process
  conflict with the slow-vs-quick pair.
* A conflicted writer may leave its already-uploaded data object behind
  (unreferenced; documented, no GC). The harness bounds and reports that
  count instead of asserting an exact one.
* The first save into a fresh prefix publishes **version 2**: the CLI's
  `open_or_create` creates the empty version 1 first (also over S3).
* s3mock does not verify SigV4; the harness's readiness/probe/REST calls are
  unsigned on purpose. Signature verification lives in the in-repo mock
  (`cargo test -p pink-lake --features s3`).
