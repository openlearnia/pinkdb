# Real-S3 integration harness — notes (`s3_docker.sh`)

Companion notes for `crates/pink-lake/tests/s3_docker.sh`. Everything here was
measured on this machine (macOS, Docker Desktop, 2026-10-14); treat the
numbers as evidence, not as constants.

## What the harness is

The in-repo `mock_s3.rs` (used by `cargo test -p pink-lake --features s3` and
the CLI's `lake_s3` tests) is the hermetic protocol suite — it even verifies
SigV4 on every request — but it is a home-grown server inside the test
process. `s3_docker.sh` is the independent check: a real, separate HTTP
server in a container, real sockets, driven through the release CLI
(`cargo build --release -p pinkdb-cli --features lake,s3`) exactly as a user
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

## The three servers: capability matrix and enforcement policy

The harness probes each endpoint **before any lake write** — an unsigned PUT
(SigV4 enforcement), the sequential conditional semantics, and a bounded
two-writer atomicity race (4× `If-None-Match: *` + 4× `If-Match`, fresh key
per trial) — and sets its policy from the probe results, not from the server
name. Measured on this machine:

| server (container) | SigV4 rejected when unsigned? | conditionals atomic under true concurrency? | one-winner race assertion |
|---|---|---|---|
| `s3mock` (`pinkdb-s3mock-itest`, default config) | **no** — unsigned PUT -> 200 (documented; the in-repo mock checks signatures separately) | **no** — 4/4 racing trials answered both writers 200 | reported |
| `seaweedfs` + `-s3.config` identities (`pinkdb-seaweed-itest`, started by the harness) | **yes** — unsigned -> 403, wrong-key signed -> 403 | **yes** — 0/4 both-200 trials | **enforced** (hard failure) |
| `seaweedfs` default config (a plain `pk-seaweed`, no identities file) | **no** — unsigned PUT -> 200, garbage signed -> 200 | **yes** | enforced conditionals; SigV4 reported |
| `rustfs` (`pk-rustfs`) | **yes** — unsigned -> 403, wrong-key signed -> 403 | **yes** — 0/4 both-200 trials | **enforced** (hard failure) |

"Enforced" means: the run hard-fails if an unsigned PUT is not 403 (on the
SigV4-verifying containers), if a racing conditional PUT trial has two
winners, if two writers claim the same race version, or if
`catalog.json` is not byte-identical to the snapshot it names. On s3mock the
one-winner-per-version outcome stays **reported** (the previous policy), with
the raw evidence below.

### Container recipes

The harness starts (or reuses) exactly one container per run. These are the
equivalent manual recipes:

```sh
# s3mock (lenient: no SigV4 check, non-atomic conditionals)
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
headers, 4 trials each; s3mock answered 4/4 duplicate wins for each header in
the regression run below). Both strict servers answered 0/4 duplicate wins
in the same probe — and 0/20 in the raw two-writer race that preceded this
work (RustFS and SeaweedFS both evaluate the precondition with the write).

**Mechanism (from the S3Mock sources):** in
`server/src/main/kotlin/com/adobe/testing/s3mock/s3/controller/ObjectController.kt`,
`putObject` first calls
`objectService.verifyObjectMatching(bucketName, key, match, noneMatch)` — a
`getObject` + header comparison — and only then `objectService.putObject(...)`.
The store does have per-object locks (`ObjectStore.kt` wraps its operations in
`synchronized(lockFor(id))`), but the precondition check is outside that lock:
check-then-act. Two requests can both pass the check before either write
lands. AWS S3 / Cloudflare R2 / SeaweedFS / RustFS evaluate the condition as
part of the write commit (that is exactly what the README's "Conditional
writes are the conflict machinery" bullet relies on).

### What the harness does about it

1. It probes capabilities up front and **says so** (the probe results are
   printed; no silent assumption), then picks the policy: strict on
   seaweedfs/rustfs, lenient on s3mock.
2. The 3×3 race still asserts everything that does not need server atomicity:
   every attempt is win-xor-conflict, every loser's message is one of the
   documented conflict forms, and the published version chain is gapless.
   On a strict server duplicate publications are a hard failure; on s3mock
   they are reported as the measured server limitation, with the
   `PINKDB_S3_LIVE=1` / in-repo-mock pointer for the enforced check.
3. A **deterministic** same-version conflict is asserted regardless of
   atomicity: a slow save opens the lake at v2, a quick save publishes v3
   while the slow writer is still encoding, and the slow writer's own
   snapshot PUT then correctly gets 412 (it is a plain "does this key exist"
   case by the time it runs — no overlap window). The harness asserts exactly
   one winner of v3, the loser's conflict, and `catalog.json ==
   snapshots/3.json` bytes.
4. A **settle save** after the race reopens the lake and publishes one more
   version alone (the documented "reopen the lake to continue"), so the
   final version of the race prefix has exactly one claimant and the
   final-version `catalog.json == snapshots/<v>.json` byte check is enforced
   on any server.
5. On the strict servers the raced prefix gets one more hard check *before*
   the settle save: `catalog.json` must be byte-identical to the snapshot it
   names. Atomic conditionals make that an invariant (the one-winner snapshot
   PUT is immutable and the catalog's `If-Match` replace publishes those same
   bytes); with s3mock's check-then-act a late racer can overwrite snapshot
   bytes, so there the check is reported.

Observed effect on the race (four earlier s3mock runs of the script): 7
claims/2 conflicts/2 duplicate versions, 5/4/2, then 3/6/**0** (the strict
one-winner assertion held and the run reported it), then 4/5/1. The duplicate
count depends on how much the three writers' conditional PUTs overlap, which
is exactly the server's window — the controlled same-version conflict and the
settle save assertions passed in all four runs.

## Strict runs measured on this machine (2026-10-14)

`SERVER=seaweedfs` (identity container, free port, `SKIP_BUILD=1`) — key
output:

```
   pk-seaweed is running but accepts unsigned writes (SeaweedFS default anonymous config)
   => starting the SigV4-enforcing pinkdb-seaweed-itest instead; pk-seaweed is left untouched
  ok: seaweedfs answered the readiness GET (7 retries at http://127.0.0.1:64969)
  ok: bucket pinklake ready (CreateBucket -> 200; the CLI never creates buckets)
  ok: unsigned PUT -> 403: the server verifies SigV4 (enforced for this run)
  ok: server enforces If-None-Match: * (412) and If-Match (200 / 412) — checked sequentially
  ok: conditional PUTs are atomic under concurrency (4+4 racing trials, exactly one winner each)
   policy: STRICT — one winner per version and catalog==snapshot bytes are hard assertions
  ok: catalog.json == snapshots/2.json (bytes; enforced)          # round-trip
  9 save attempts -> 3 claim(s) of 3 version(s), 6 conflict(s)
  ok: exactly one winner per version, no gaps (versions 3..5)
  ok: every loser got the documented write conflict (6 conflict(s))
  ok: catalog.json == snapshots/5.json (bytes)                    # the raced catalog
  ok: reopened after the race; the settle save published version 6 alone
  ok: catalog.json == snapshots/6.json (bytes, version field 6, single claimant; enforced)
  ok: exactly one winner of version 3: the quick save
  ok: the slow save got the documented conflict: Error: write conflict at
      s3://pinklake/conflict/snapshots/3.json: snapshot 3 already exists; refusing to replace a published version
  ok: catalog.json == snapshots/3.json (bytes; single claimant; enforced)
  server:        seaweedfs (container pinkdb-seaweed-itest at http://127.0.0.1:64969)
  signatures:    SigV4 enforced — unsigned PUT -> 403 (asserted)
  enforcement:   STRICT — one winner per version and catalog == snapshot bytes are hard assertions
ALL CHECKS PASSED
```

`SERVER=rustfs` (reusing the running `pk-rustfs`, `SKIP_BUILD=1`):

```
   reusing running container pk-rustfs
  ok: reuse: container pk-rustfs is running on 127.0.0.1:9100
  ok: rustfs answered the readiness GET (1 retries at http://127.0.0.1:9100)
  ok: bucket pinklake ready (CreateBucket -> 200; the CLI never creates buckets)
  ok: unsigned PUT -> 403: the server verifies SigV4 (enforced for this run)
  ok: server enforces If-None-Match: * (412) and If-Match (200 / 412) — checked sequentially
  ok: conditional PUTs are atomic under concurrency (4+4 racing trials, exactly one winner each)
   policy: STRICT — one winner per version and catalog==snapshot bytes are hard assertions
  ok: catalog.json == snapshots/2.json (bytes; enforced)
  9 save attempts -> 3 claim(s) of 3 version(s), 6 conflict(s)
  ok: exactly one winner per version, no gaps (versions 3..5)
  ok: every loser got the documented write conflict (6 conflict(s))
  ok: catalog.json == snapshots/5.json (bytes)
  ok: reopened after the race; the settle save published version 6 alone
  ok: catalog.json == snapshots/6.json (bytes, version field 6, single claimant; enforced)
  ok: exactly one winner of version 3: the quick save
  ok: the slow save got the documented conflict: Error: write conflict at
      s3://pinklake/conflict/snapshots/3.json: snapshot 3 already exists; refusing to replace a published version
  ok: catalog.json == snapshots/3.json (bytes; single claimant; enforced)
  server:        rustfs (container pk-rustfs at http://127.0.0.1:9100)
  signatures:    SigV4 enforced — unsigned PUT -> 403 (asserted)
  enforcement:   STRICT — one winner per version and catalog == snapshot bytes are hard assertions
ALL CHECKS PASSED
```

`s3mock` regression run (same script, `SKIP_BUILD=1`): readiness/roundtrip/
races/refusal all pass; the capability probe reports
`WARNING: conditional PUTs are NOT atomic under concurrency on this server`
(4/4 both-200 per header), the policy stays
`lenient — duplicates are reported as the measured server limitation`, and
the run ends `ALL CHECKS PASSED`.

### The slow-vs-quick pair needed a wider window on RustFS

The deterministic conflict (point 3 above) is a *timing* construction: the
quick writer must read the catalog (v2) while the slow writer's save is still
in flight. With the original `SLOWROWS=400000` the window is too narrow on a
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
4000000 rows, 22277590 bytes written)`), and the pair behaves deterministically
(2/2 manual trials + both full strict runs: the quick save wins v3, the slow
save gets the documented conflict). If the window is ever missed, the failure
message now says so explicitly (both writers published different versions)
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
SigV4-verifying, atomic-conditional witnesses (`SERVER=seaweedfs|rustfs`).
Any S3-compatible endpoint works with the same script (`CONTAINER=`/`PORT=`,
or point the CLI's `AWS_ENDPOINT_URL` at your own server); for a real AWS/R2
bucket use the opt-in live test:

```sh
PINKDB_S3_LIVE=1 PINKDB_S3_BUCKET=<bucket> PINKDB_S3_PREFIX=<prefix> \
AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_ENDPOINT_URL=… AWS_REGION=auto \
cargo test -p pink-lake --features s3 --test s3 live_s3_roundtrip -- --nocapture
```

The in-repo CLI test `concurrent_lake_saves_over_s3_stay_consistent`
(`cargo test -p pinkdb-cli --features lake,s3`) is the hermetic version of the
one-winner assertion, against the mock's conditional writes.

## What is asserted where

| check | server atomicity needed | s3mock run | seaweedfs/rustfs run |
|---|---|---|---|
| unsigned PUT → 403 (SigV4 enforced) | no (but fails there) | reported (accepted, documented) | **enforced** |
| typed CSV round-trip, exact values in a fresh process | no | enforced | enforced |
| published object set (REST `?list-type=2`) | no | enforced | enforced |
| `catalog.json == snapshots/2.json` (round-trip lake) | no | enforced | enforced |
| race: win-xor-conflict per attempt, documented conflict forms | no | enforced | enforced |
| race: version chain gapless, `.lake versions` == 1..V | no | enforced | enforced |
| race: exactly one winner per version | **yes** | reported (duplicates = measured server quirk) | **enforced** |
| race: `catalog.json == snapshots/<catalog version>.json` (pre-settle) | **yes** | reported | **enforced** |
| slow-vs-quick: exactly one winner of one version + loser conflict | no (see above) | enforced | enforced |
| slow-vs-quick: `catalog.json == snapshots/3.json` | no | enforced | enforced |
| settle save: final version single-claimant, bytes equal | no | enforced | enforced |
| second `.lake save` → new version (same process) | no | enforced | enforced |
| planted stray snapshot → `snapshot N already exists …` refusal, then a clean save after removing it | no | enforced | enforced |

Also asserted on every run: the sequential conditional semantics
(`If-None-Match: *` on an existing key → 412, `If-Match` → 200/412) and the
bucket existing before the first write.

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
