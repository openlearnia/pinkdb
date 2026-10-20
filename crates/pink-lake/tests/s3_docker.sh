#!/usr/bin/env bash
# ============================================================================
# pink-lake real-S3 integration test: a real S3 server in Docker, release CLI.
# ============================================================================
#
# The in-repo `mock_s3.rs` (used by
# `cargo test -p pink-lake --features s3,vortexlake` and the CLI's
# `lake_s3` tests) is the hermetic protocol suite — it even
# verifies SigV4 on every request — but it is a home-grown server inside the
# test process. This script is the independent check: a real, separate HTTP
# server in a container, speaking real S3 semantics over a socket, driven
# through the release CLI just like a user would.
#
# It needs Docker, python3 and curl 7.75+ (for `--aws-sigv4`, the harness's
# own signed probes) plus cargo for the default CLI build:
#   * SERVER=s3mock     adobe/s3mock:latest — accepts unsigned requests (no
#                       SigV4 verification) and evaluates conditional PUTs
#                       non-atomically under true concurrency. The one-winner
#                       race assertion is REPORTED there, as before (the
#                       in-repo atomic mock and the strict servers below pin
#                       the client logic).
#   * SERVER=seaweedfs  chrislusf/seaweedfs:latest — a container started with
#                       `-s3.config` identities verifies SigV4 (unsigned PUT
#                       -> 403) and evaluates conditionals atomically: STRICT.
#                       A pre-existing `pk-seaweed` running SeaweedFS's
#                       DEFAULT config is measured to accept anonymous writes
#                       and is therefore NOT reused: the harness leaves it
#                       untouched and starts its own identity-configured
#                       `pinkdb-seaweed-itest`. (A `pk-seaweed` that verifies
#                       SigV4 is reused.)
#   * SERVER=rustfs     rustfs/rustfs:latest — verifies SigV4 (unsigned and
#                       wrong-key puts -> 403) and evaluates conditionals
#                       atomically: STRICT.
#   Strict means: an unsigned PUT must be answered 403 (asserted), the
#   two-writer atomicity probe must show one winner per trial (asserted), and
#   the race's exactly-one-winner-per-version + catalog==snapshot byte checks
#   are hard failures, not reports.
#
# Container recipes (equivalent manual `docker run` commands; the client
# never creates buckets, so the harness creates $BUCKET itself):
#   s3mock:
#     docker run -d --name pinkdb-s3mock-itest -p 127.0.0.1:9090:9090 \
#       -e COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS=pinklake \
#       adobe/s3mock:latest
#   seaweedfs (strict; the identity file is written into the run's temp dir):
#     docker run -d --name pinkdb-seaweed-itest -p 127.0.0.1:8333:8333 \
#       -v <tmp>/seaweedfs-s3.json:/etc/seaweedfs/s3.json:ro \
#       chrislusf/seaweedfs:latest server -s3 -dir=/data \
#         -s3.config=/etc/seaweedfs/s3.json
#   rustfs:
#     docker run -d --name pk-rustfs -p 127.0.0.1:9100:9000 \
#       -e RUSTFS_ACCESS_KEY=rustfsadmin -e RUSTFS_SECRET_KEY=rustfsadmin \
#       rustfs/rustfs:latest
#   The script itself pins every image by digest (`repo:tag@sha256:…`), so the
#   tag in these recipes is only for readability; see the pin table below.
#
# On this machine (2026-10-14) seaweedfs and rustfs both answered the
# two-writer conditional race with exactly one 200 in every trial, while
# s3mock let both racers win (its precondition check is check-then-act);
# see `s3_docker_notes.md` for the evidence and the capability matrix.
#
# Per-server container lifecycle:
#   * s3mock uses `pinkdb-s3mock-itest` (initial bucket via env); reuse of an
#     existing container of that name works like before.
#   * rustfs prefers `pk-rustfs` (the documented manual container); reuse when
#     present, otherwise start it on :9100 -> :9000.
#   * seaweedfs prefers `pk-seaweed` ONLY when it verifies SigV4; otherwise
#     it starts `pinkdb-seaweed-itest` (identity config) and leaves any other
#     container untouched. Host ports default to 9090 / 8333 / 9100 and fall
#     back to a free port when busy.
#   * Cleanup: a container THIS RUN STARTED is removed unless KEEP=1; a
#     container that already existed is reused and left running on exit (it
#     was not started by this run). The script never touches another
#     server's container.
#
# Usage:
#   crates/pink-lake/tests/s3_docker.sh                  # s3mock (default)
#   SERVER=seaweedfs crates/pink-lake/tests/s3_docker.sh # strict
#   SERVER=rustfs    crates/pink-lake/tests/s3_docker.sh # strict
#
# Environment knobs:
#   SERVER=s3mock|seaweedfs|rustfs   server under test (default s3mock)
#   PORT=9090            host port; the per-server default, or a free port
#                        auto-picked when the default is busy
#   CONTAINER=...        container name (defaults per server; setting it
#                        explicitly reuses that container and only REPORTS
#                        whether it verifies SigV4)
#   BUCKET=pinklake      bucket the harness creates if missing (the CLI never
#                        creates buckets — see s3_docker_notes.md)
#   KEEP=1               leave a container this run started running on exit
#                        (reused next run) and keep the run's temp dir
#   PINKDB_BIN=...       use this CLI binary (default: build lake,s3 release)
#   SKIP_BUILD=1         do not run cargo build (use target/release/pinkdb)
#   ACCESS_KEY/SECRET_KEY  per-server credentials (s3mock: any dummy pair —
#                        but defaults are pinned; seaweedfs/rustfs: must match
#                        the container)
#   WROWS=20000          rows per racing writer's table
#   SLOWROWS=4000000     rows of the slow save in the deterministic conflict
#                        (must encode for long enough that the quick writer
#                        reads the catalog while it is still in flight; a
#                        local RustFS encodes 400k rows in ~0.1s, which is
#                        too narrow a window)
#   QUICKROWS=1000       rows of the quick save that wins that version
#
# Every failure prints the failing step, the raw output and exits non-zero.
# The script is not wired into the test suite (it needs Docker).

set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

SERVER="${SERVER:-s3mock}"
BUCKET="${BUCKET:-pinklake}"
KEEP="${KEEP:-0}"
WROWS="${WROWS:-20000}"
SLOWROWS="${SLOWROWS:-4000000}"
QUICKROWS="${QUICKROWS:-1000}"
PORT="${PORT:-}"
REGION="us-east-1"

# The servers are pinned by digest, not by mutable `:latest` tag: a re-tag
# between runs must not silently change the code CI executes. Update a pin
# when intentionally bumping the server, with:
#   docker buildx imagetools inspect <repo>:<tag> | sed -n 's/^Digest: *//p'
# (or `docker manifest inspect -v <image>` and use the top-level descriptor's
# digest). The multi-arch index digest is used; Docker picks this host's arch.
case "$SERVER" in
s3mock)
    IMAGE_TAG="adobe/s3mock:latest"
    IMAGE_DIGEST="sha256:ab01a6946750f451ca215a47e91030695b260e4003b8a5a6201d25029b8fca92"
    IMAGE="$IMAGE_TAG@$IMAGE_DIGEST"
    CONTAINER_DEFAULT="pinkdb-s3mock-itest"
    DEFAULT_PORT=9090
    CONTAINER_PORT=9090
    STRICT=0
    ACCESS_KEY="${ACCESS_KEY:-pinkdb-itest}"
    SECRET_KEY="${SECRET_KEY:-pinkdb-itest-secret}"
    ;;
seaweedfs)
    IMAGE_TAG="chrislusf/seaweedfs:latest"
    IMAGE_DIGEST="sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d"
    IMAGE="$IMAGE_TAG@$IMAGE_DIGEST"
    CONTAINER_DEFAULT="pk-seaweed"
    CONTAINER_FALLBACK="pinkdb-seaweed-itest"
    DEFAULT_PORT=8333
    CONTAINER_PORT=8333
    STRICT=1
    ACCESS_KEY="${ACCESS_KEY:-anykey}"
    SECRET_KEY="${SECRET_KEY:-anysecret}"
    ;;
rustfs)
    IMAGE_TAG="rustfs/rustfs:latest"
    IMAGE_DIGEST="sha256:9ca9559533b537d10482fe923288b676ad08f92a7069d9a17327fbae3cab688b"
    IMAGE="$IMAGE_TAG@$IMAGE_DIGEST"
    CONTAINER_DEFAULT="pk-rustfs"
    DEFAULT_PORT=9100
    CONTAINER_PORT=9000
    STRICT=1
    ACCESS_KEY="${ACCESS_KEY:-rustfsadmin}"
    SECRET_KEY="${SECRET_KEY:-rustfsadmin}"
    ;;
*)
    printf 'unknown SERVER=%s (expected: s3mock, seaweedfs or rustfs)\n' "$SERVER" >&2
    exit 2
    ;;
esac
CONTAINER="${CONTAINER:-}"

# `s3curl` auth: empty for s3mock (its REST probes are unsigned on purpose —
# s3mock does not verify SigV4), SigV4 for the strict servers (which reject
# unsigned writes). A plain space-separated option string, expanded unquoted
# inside s3curl() below (no value contains a space).
AUTH_ARGS=""
case "$SERVER" in
seaweedfs|rustfs) AUTH_ARGS="--aws-sigv4 aws:amz:us-east-1:s3 --user $ACCESS_KEY:$SECRET_KEY" ;;
esac

TMP="$(mktemp -d "${TMPDIR:-/tmp}/pinkdb_s3_docker.XXXXXX")" || exit 1
CURRENT_LOG=""
STEP=""
ENDPOINT=""
OWNED=0          # 1 when this run created the container
SIG_EXPECTED=0   # 1 when the run requires a SigV4-verifying endpoint
SIG_VERIFIED=0   # probe result: unsigned PUT answered 403
RACE_ATOMIC=""   # probe result: one winner per racing conditional PUT
ENFORCE_ATOMIC="$STRICT"
BUCKET_READY=0
BUCKET_CODE=""

# ---------------------------------------------------------------------------
# output helpers
# ---------------------------------------------------------------------------

step() {
    STEP="$*"
    printf '\n== %s ==\n' "$*"
}

note() { printf '   %s\n' "$*"; }
ok() { printf '  ok: %s\n' "$*"; }
warn() { printf '  WARNING: %s\n' "$*"; }

fail() {
    printf '\nFAILED: %s\n' "$1" >&2
    [ -n "$STEP" ] && printf '  step: %s\n' "$STEP" >&2
    if [ -n "$CURRENT_LOG" ] && [ -f "$CURRENT_LOG" ]; then
        printf -- '--- raw output: %s ---\n' "$CURRENT_LOG" >&2
        cat "$CURRENT_LOG" >&2
        printf -- '--- (end raw output) ---\n' >&2
    fi
    if [ -f "$TMP/conc.state" ]; then
        printf -- '--- race summary ---\n' >&2
        cat "$TMP/conc.state" >&2
    fi
    if [ -n "${CONTAINER:-}" ] && docker inspect "$CONTAINER" >/dev/null 2>&1; then
        printf -- '--- last container log lines ---\n' >&2
        docker logs --tail 20 "$CONTAINER" >&2 2>&1 || true
        printf -- '--- (end container log) ---\n' >&2
    fi
    printf '  hint: raw CLI logs and probe evidence: %s\n' "$TMP" >&2
    exit 1
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    if [ -n "${CONTAINER:-}" ] && docker inspect "$CONTAINER" >/dev/null 2>&1; then
        if [ "$OWNED" = "1" ]; then
            if [ "$KEEP" = "1" ]; then
                printf '\nkept container %s (KEEP=1) on port %s' "$CONTAINER" "${PORT:-?}"
                printf ' — remove it with: docker rm -f %s\n' "$CONTAINER"
                printf 'kept run directory %s (raw CLI logs + generated CSVs, KEEP=1)\n' "$TMP"
            else
                docker rm -f "$CONTAINER" >/dev/null 2>&1 \
                    && printf '\nremoved container %s (started by this run)\n' "$CONTAINER"
            fi
        else
            printf '\nleft the reused container %s running (it was not started by this run)\n' "$CONTAINER"
        fi
    fi
    if [ "$KEEP" != "1" ]; then
        rm -rf "$TMP"
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# CLI driving
# ---------------------------------------------------------------------------

# Run the CLI with exactly the pinned S3 environment (a developer's real AWS
# credentials can never leak into the test server), stdin from the caller,
# combined output into <log>. Returns the CLI's exit status.
cli_run() {
    local log="$1"
    CURRENT_LOG="$log"
    env \
        -u AWS_ENDPOINT_URL_S3 \
        -u AWS_DEFAULT_REGION \
        -u AWS_SESSION_TOKEN \
        -u AWS_S3_FORCE_PATH_STYLE \
        -u PINKDB_S3_ALLOW_DEGRADED \
        -u PINKDB_KEY \
        AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
        AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
        AWS_REGION="$REGION" \
        AWS_ENDPOINT_URL="$ENDPOINT" \
        PINKDB_S3_ALLOW_HTTP=1 \
        "$BIN" >"$log" 2>&1
}

# cli_run with PINKDB_KEY set: the lake commands seal/unseal `PNKLAKE1`
# containers; the rest of the environment is pinned exactly like cli_run.
cli_run_keyed() { # <log> <passphrase>
    local log="$1" key="$2"
    CURRENT_LOG="$log"
    env \
        -u AWS_ENDPOINT_URL_S3 \
        -u AWS_DEFAULT_REGION \
        -u AWS_SESSION_TOKEN \
        -u AWS_S3_FORCE_PATH_STYLE \
        -u PINKDB_S3_ALLOW_DEGRADED \
        AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
        AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
        AWS_REGION="$REGION" \
        AWS_ENDPOINT_URL="$ENDPOINT" \
        PINKDB_S3_ALLOW_HTTP=1 \
        PINKDB_KEY="$key" \
        "$BIN" >"$log" 2>&1
}

assert_contains() { # <log> <literal>
    grep -qF -- "$2" "$1" || fail "expected the literal \"$2\" in $(basename "$1")"
    ok "$2"
}

assert_re() { # <log> <ERE>
    grep -qE -- "$2" "$1" || fail "expected a line matching /$2/ in $(basename "$1")"
    ok "/$2/"
}

assert_no_match() { # <log> <ERE>
    if grep -qE -- "$2" "$1"; then
        fail "expected no line matching /$2/ in $(basename "$1")"
    fi
    ok "no line matching /$2/"
}

assert_status_zero() { # <log> <status>
    [ "$2" = "0" ] || fail "$(basename "$1"): CLI exited with status $2"
    ok "CLI exited 0 ($(basename "$1"))"
}

# ---------------------------------------------------------------------------
# REST helpers
# ---------------------------------------------------------------------------

# curl with this server's request auth: unsigned for s3mock, SigV4-signed for
# seaweedfs/rustfs (their capability probes and this harness's REST calls all
# go through here).
s3curl() {
    # shellcheck disable=SC2086  # $AUTH_ARGS is a deliberate option list
    curl $AUTH_ARGS "$@"
}

# `?list-type=2` GET, one key per line (≤1000 keys — plenty here; a full
# prefix is refused below rather than silently truncated).
list_keys() {
    local enc body
    enc="$(python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.argv[1]))' "$1")" || return 1
    body="$(s3curl -fsS --max-time 30 "$ENDPOINT/$BUCKET/?list-type=2&prefix=$enc&max-keys=1000")" || return 1
    printf '%s' "$body" | python3 -c '
import html, re, sys
body = sys.stdin.read()
keys = [html.unescape(k) for k in re.findall(r"<Key>(.*?)</Key>", body)]
print("\n".join(keys))' || return 1
}

assert_listing_count() { # <keys> <prefix> <expected>
    local count
    count="$(printf '%s\n' "$1" | grep -c .)"
    [ "$count" != 1000 ] || fail "prefix $2 has ≥1000 objects; use a fresh bucket/prefix or remove the container"
    [ "$count" = "$3" ] || fail "prefix $2: expected $3 object(s), found $count"
    ok "$2 has exactly $3 object(s)"
}

wipe_prefix() { # <prefix>
    local keys n k
    keys="$(list_keys "$1")" || fail "listing $1 failed"
    n=0
    for k in $keys; do
        s3curl -fsS --max-time 30 -X DELETE "$ENDPOINT/$BUCKET/$k" -o /dev/null \
            || fail "DELETE of $k failed"
        n=$((n + 1))
    done
    note "wiped $n object(s) under $1"
}

# ---------------------------------------------------------------------------
# Docker management
# ---------------------------------------------------------------------------

port_free() { # <port> — true when nothing is listening on 127.0.0.1:<port>
    python3 - "$1" <<'PY'
import socket, sys
s = socket.socket()
s.settimeout(0.5)
try:
    s.connect(("127.0.0.1", int(sys.argv[1])))
except OSError:
    sys.exit(0)  # free
sys.exit(1)      # something is listening
PY
}

free_port() {
    python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

# A container created by this script records the pinned `repo:tag@digest`;
# an older one (or a hand-made documented recipe) records just the tag, which
# stays accepted so reuse behavior is unchanged.
same_image() { # <observed image>
    local observed="$1"
    [ "$observed" = "$IMAGE" ] \
        || [ "$observed" = "$IMAGE_TAG" ] \
        || [ "$observed" = "${IMAGE_TAG%:latest}" ]
}

mapped_port() { # <container> <container-port> — host port of a TCP mapping
    docker port "$1" "$2/tcp" 2>/dev/null | sed -n 's/.*:\([0-9][0-9]*\)$/\1/p' | head -1
}

unsigned_put_403() { # <endpoint> — true when an unsigned PUT is rejected 403
    local code
    code="$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' \
        -X PUT --data-binary 'unsigned' "$1/$BUCKET/itest-unsigned-probe-retry" 2>/dev/null)" \
        || return 1
    if [ "$code" = "403" ]; then
        return 0
    fi
    curl -sS --max-time 5 -X DELETE "$1/$BUCKET/itest-unsigned-probe-retry" -o /dev/null 2>/dev/null || true
    return 1
}

pick_port() {
    if [ -n "$PORT" ]; then
        port_free "$PORT" || fail "PORT=$PORT is already in use on 127.0.0.1; choose another port or unset PORT (nothing was started)"
    elif port_free "$DEFAULT_PORT"; then
        PORT="$DEFAULT_PORT"
    else
        PORT="$(free_port)"
        note "port $DEFAULT_PORT is busy (another container?); using free port $PORT instead"
    fi
}

# SeaweedFS verifies SigV4 only when it is started with identities; the
# default `server -s3` config serves anonymous requests (measured — unsigned
# and even garbage-signed PUTs get 200). The harness's own container uses
# this file so the strict SigV4 assertion has teeth.
write_seaweedfs_config() {
    SEAWEEDFS_CONFIG="$TMP/seaweedfs-s3.json"
    cat >"$SEAWEEDFS_CONFIG" <<JSON
{
  "identities": [
    {
      "name": "pinkdb-itest",
      "credentials": [
        { "accessKey": "$ACCESS_KEY", "secretKey": "$SECRET_KEY" }
      ],
      "actions": ["Admin", "Read", "Write", "List"]
    }
  ]
}
JSON
}

docker_run_container() {
    case "$SERVER" in
    s3mock)
        docker run -d --name "$CONTAINER" \
            --label pinkdb-s3-itest=1 \
            -p "127.0.0.1:${PORT}:${CONTAINER_PORT}" \
            -e COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS="$BUCKET" \
            "$IMAGE" >/dev/null || fail "docker run $IMAGE failed"
        ;;
    seaweedfs)
        write_seaweedfs_config
        docker run -d --name "$CONTAINER" \
            --label pinkdb-s3-itest=1 \
            -p "127.0.0.1:${PORT}:${CONTAINER_PORT}" \
            -v "$SEAWEEDFS_CONFIG:/etc/seaweedfs/s3.json:ro" \
            "$IMAGE" server -s3 -dir=/data -s3.config=/etc/seaweedfs/s3.json \
            >/dev/null || fail "docker run $IMAGE failed"
        ;;
    rustfs)
        docker run -d --name "$CONTAINER" \
            --label pinkdb-s3-itest=1 \
            -p "127.0.0.1:${PORT}:${CONTAINER_PORT}" \
            -e RUSTFS_ACCESS_KEY="$ACCESS_KEY" \
            -e RUSTFS_SECRET_KEY="$SECRET_KEY" \
            "$IMAGE" >/dev/null || fail "docker run $IMAGE failed"
        ;;
    esac
    ok "container $CONTAINER started"
}

# Use <container>: start it when stopped, reuse it as-is when running, create
# it when absent. $2=1 means the run requires an endpoint that verifies SigV4
# (asserted by check_signature_enforcement); $2=0 only reports it.
use_container() { # <container> <sig-expected>
    local name="$1" sig="$2" image state mapped
    CONTAINER="$name"
    SIG_EXPECTED="$sig"
    if docker inspect "$CONTAINER" >/dev/null 2>&1; then
        image="$(docker inspect -f '{{.Config.Image}}' "$CONTAINER" 2>/dev/null || true)"
        same_image "$image" \
            || fail "container $CONTAINER already exists but runs \"$image\", not $IMAGE; remove it first (docker rm -f $CONTAINER)"
        state="$(docker inspect -f '{{.State.Status}}' "$CONTAINER" 2>/dev/null || true)"
        if [ "$state" != "running" ]; then
            note "container $CONTAINER exists but is $state; starting it (reuse)"
            docker start "$CONTAINER" >/dev/null || fail "docker start $CONTAINER failed"
        else
            note "reusing running container $CONTAINER"
        fi
        mapped="$(mapped_port "$CONTAINER" "$CONTAINER_PORT")"
        [ -n "$mapped" ] || fail "container $CONTAINER has no host mapping for $CONTAINER_PORT/tcp"
        if [ -n "$PORT" ] && [ "$PORT" != "$mapped" ]; then
            fail "PORT=$PORT was requested but the running container is mapped to $mapped; remove it (docker rm -f $CONTAINER) or unset PORT"
        fi
        PORT="$mapped"
        ok "reuse: container $CONTAINER is running on 127.0.0.1:$PORT"
        return 0
    fi

    pick_port
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        note "image $IMAGE is not local; pulling it (MinIO images are not anonymously pullable right now — see s3_docker_notes.md)"
        docker pull "$IMAGE" >/dev/null || fail "docker pull $IMAGE failed"
        ok "pulled $IMAGE"
    fi

    note "creating container $CONTAINER on 127.0.0.1:$PORT (bucket $BUCKET)"
    docker_run_container
    OWNED=1
}

select_container() {
    if [ -n "$CONTAINER" ]; then
        # An explicit CONTAINER=... is the caller's choice: reuse it and only
        # report whether that container verifies SigV4.
        use_container "$CONTAINER" 0
        return
    fi
    case "$SERVER" in
    s3mock)
        use_container "$CONTAINER_DEFAULT" 0
        ;;
    rustfs)
        use_container "$CONTAINER_DEFAULT" 1
        ;;
    seaweedfs)
        # Prefer a running `pk-seaweed` only when it actually verifies SigV4
        # (probed, not assumed); default-config SeaweedFS accepts anonymous
        # writes, so such a container is reported and left untouched while
        # the harness starts its own identity-configured container.
        if docker inspect "$CONTAINER_DEFAULT" >/dev/null 2>&1 \
            && [ "$(docker inspect -f '{{.State.Status}}' "$CONTAINER_DEFAULT" 2>/dev/null || true)" = "running" ]; then
            local probe_port
            probe_port="$(mapped_port "$CONTAINER_DEFAULT" "$CONTAINER_PORT")"
            if [ -n "$probe_port" ] && unsigned_put_403 "http://127.0.0.1:$probe_port"; then
                use_container "$CONTAINER_DEFAULT" 1
                return
            fi
            note "$CONTAINER_DEFAULT is running but accepts unsigned writes (SeaweedFS default anonymous config)"
            note "=> starting the SigV4-enforcing $CONTAINER_FALLBACK instead; $CONTAINER_DEFAULT is left untouched"
        fi
        use_container "$CONTAINER_FALLBACK" 1
        ;;
    esac
}

wait_ready() {
    local tries=0 probe body code
    probe="itest-readiness-probe"
    while [ "$tries" -lt 120 ]; do
        # CreateBucket when the container did not come with the bucket;
        # 200/201/204/405/409 all mean "the endpoint answered".
        if [ "$BUCKET_READY" != "1" ]; then
            code="$(s3curl -sS --max-time 5 -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/$BUCKET" 2>/dev/null)" || true
            case "$code" in
            200|201|204|405|409)
                BUCKET_READY=1
                BUCKET_CODE="$code"
                ;;
            esac
        fi
        if body="$(s3curl -fsS --max-time 5 "$ENDPOINT/$BUCKET/$probe" 2>/dev/null)"; then
            if [ "$body" = "ready" ]; then
                s3curl -sS --max-time 5 -X DELETE "$ENDPOINT/$BUCKET/$probe" -o /dev/null 2>/dev/null || true
                ok "$SERVER answered the readiness GET ($tries retries at $ENDPOINT)"
                return 0
            fi
        fi
        printf 'ready' | s3curl -fsS --max-time 5 -X PUT --data-binary @- "$ENDPOINT/$BUCKET/$probe" -o /dev/null 2>/dev/null || true
        tries=$((tries + 1))
        sleep 0.5
    done
    fail "$SERVER did not become ready at $ENDPOINT within 60s"
}

# The client never creates buckets; the harness creates $BUCKET up front
# (200/201/204 created, 405/409 already exists). Point BUCKET= at an existing
# bucket if you do not want this.
ensure_bucket() {
    local code
    if [ "$BUCKET_READY" = "1" ]; then
        ok "bucket $BUCKET ready (CreateBucket -> $BUCKET_CODE; the CLI never creates buckets)"
        return 0
    fi
    code="$(s3curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/$BUCKET" 2>/dev/null)" \
        || fail "CreateBucket for $BUCKET failed"
    case "$code" in
    200|201|204|405|409)
        ok "bucket $BUCKET ready (CreateBucket -> $code; the CLI never creates buckets)"
        ;;
    *)
        fail "could not create bucket $BUCKET (HTTP $code); the CLI never creates buckets, create it yourself and retry"
        ;;
    esac
}

# ---------------------------------------------------------------------------
# capability probes -> enforcement policy
# ---------------------------------------------------------------------------

# A real signature check: a SigV4-enforcing endpoint rejects an unsigned PUT
# with 403. s3mock accepts unsigned requests (documented; the in-repo mock
# verifies SigV4 separately), and a default-config SeaweedFS does too — which
# is exactly why the harness's own seaweedfs container uses -s3.config.
check_signature_enforcement() {
    local key="itest-unsigned-probe" code
    code="$(curl -sS --max-time 30 -o /dev/null -w '%{http_code}' \
        -X PUT --data-binary 'unsigned' "$ENDPOINT/$BUCKET/$key")" \
        || fail "the unsigned signature probe PUT failed"
    if [ "$code" = "403" ]; then
        SIG_VERIFIED=1
        ok "unsigned PUT -> 403: the server verifies SigV4 (enforced for this run)"
        return 0
    fi
    SIG_VERIFIED=0
    if [ "$SIG_EXPECTED" = "1" ]; then
        fail "unsigned PUT -> $code, expected 403: this run requires a SigV4-verifying endpoint (SERVER=$SERVER)"
    fi
    case "$SERVER" in
    s3mock)
        ok "unsigned PUT -> $code: s3mock does not verify SigV4 (documented; the in-repo mock does)"
        ;;
    *)
        warn "unsigned PUT -> $code: this endpoint does NOT verify SigV4"
        warn "  (a plain SeaweedFS 'server -s3' without -s3.config serves anonymous requests;"
        warn "   SERVER=seaweedfs starts its own identity-configured container — see s3_docker_notes.md)"
        ;;
    esac
    case "$code" in
    200|201|204) s3curl -sS --max-time 30 -X DELETE "$ENDPOINT/$BUCKET/$key" -o /dev/null 2>/dev/null || true ;;
    esac
    ok "the unsigned-403 assertion is reported, not enforced, on this endpoint"
}

# The single-writer contract needs conditional writes (the client refuses a
# degraded endpoint by default; this proves the safe path is the one tested).
check_conditional_writes() {
    local key="itest-conditional-probe" code etag headers
    headers="$(s3curl -sS --max-time 30 -D - -o /dev/null -X PUT --data-binary 'first' "$ENDPOINT/$BUCKET/$key")" \
        || fail "conditional probe PUT failed"
    etag="$(printf '%s' "$headers" | tr -d '\r' | awk 'tolower($1) == "etag:" { print $2; exit }')"
    [ -n "$etag" ] || fail "conditional probe: the server returned no ETag"

    code="$(s3curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H 'If-None-Match: *' --data-binary 'second' "$ENDPOINT/$BUCKET/$key")"
    [ "$code" = "412" ] || fail "If-None-Match: * on an existing key returned $code, expected 412 (conditional writes not enforced?)"

    code="$(s3curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H "If-Match: $etag" --data-binary 'third' "$ENDPOINT/$BUCKET/$key")"
    [ "$code" = "200" ] || fail "If-Match with the current ETag returned $code, expected 200"

    code="$(s3curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H 'If-Match: "deadbeefdeadbeef"' --data-binary 'fourth' "$ENDPOINT/$BUCKET/$key")"
    [ "$code" = "412" ] || fail "If-Match with a wrong ETag returned $code, expected 412"

    s3curl -fsS --max-time 30 -X DELETE "$ENDPOINT/$BUCKET/$key" -o /dev/null || fail "conditional probe DELETE failed"
    ok "server enforces If-None-Match: * (412) and If-Match (200 / 412) — checked sequentially"
}

# Is the server's conditional evaluation atomic with the write? Two racing
# conditional PUTs to one fresh key must leave exactly one 200 for a
# concurrent-safe endpoint (AWS S3 / Cloudflare R2 / SeaweedFS / RustFS
# evaluate the precondition as part of the write; measured here on this
# machine). s3mock's PutObject checks the precondition outside its
# per-object lock (check-then-act), so both racers can be told 200 — on the
# strict servers that would be a hard failure of the run's contract.
probe_conditional_atomicity() {
    local trials=4 t a b both=0 create_dup=0 replace_dup=0 key
    for t in 1 2 3 4; do
        key="itest-atomic-probe-$t"
        (
            s3curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H 'If-None-Match: *' \
                --data-binary "racer-a-$t" "$ENDPOINT/$BUCKET/$key" >"$TMP/atomic_a.$t"
        ) &
        (
            s3curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H 'If-None-Match: *' \
                --data-binary "racer-b-$t" "$ENDPOINT/$BUCKET/$key" >"$TMP/atomic_b.$t"
        ) &
        wait
        a="$(cat "$TMP/atomic_a.$t")"
        b="$(cat "$TMP/atomic_b.$t")"
        [ "$a" = "200" ] || [ "$b" = "200" ] || fail "If-None-Match race trial $t: neither racer won ($a/$b)"
        if [ "$a" = "200" ] && [ "$b" = "200" ]; then
            both=$((both + 1))
        fi
        # If-Match race: two concurrent replaces of one key with the same ETag.
        key="itest-atomic-probe-replace-$t"
        s3curl -fsS --max-time 30 -o /dev/null -X PUT --data-binary 'base' "$ENDPOINT/$BUCKET/$key" || fail "atomic probe PUT failed"
        a="$(s3curl -sS --max-time 30 -D - -o /dev/null -X PUT --data-binary 'base2' "$ENDPOINT/$BUCKET/$key" | tr -d '\r' | awk 'tolower($1) == "etag:" { print $2; exit }')"
        [ -n "$a" ] || fail "atomic probe: no ETag"
        (
            s3curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H "If-Match: $a" \
                --data-binary "repl-a-$t" "$ENDPOINT/$BUCKET/$key" >"$TMP/atomic_c.$t"
        ) &
        (
            s3curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H "If-Match: $a" \
                --data-binary "repl-b-$t" "$ENDPOINT/$BUCKET/$key" >"$TMP/atomic_d.$t"
        ) &
        wait
        a="$(cat "$TMP/atomic_c.$t")"
        b="$(cat "$TMP/atomic_d.$t")"
        [ "$a" = "200" ] || [ "$b" = "200" ] || fail "If-Match race trial $t: neither racer won ($a/$b)"
        if [ "$a" = "200" ] && [ "$b" = "200" ]; then
            replace_dup=$((replace_dup + 1))
        fi
    done
    for key in $(list_keys "itest-atomic-probe"); do
        s3curl -sS --max-time 30 -X DELETE "$ENDPOINT/$BUCKET/$key" -o /dev/null || true
    done

    if [ "$both" = "0" ] && [ "$replace_dup" = "0" ]; then
        RACE_ATOMIC=1
        ENFORCE_ATOMIC=1
        ok "conditional PUTs are atomic under concurrency (4+4 racing trials, exactly one winner each)"
    else
        RACE_ATOMIC=0
        if [ "$STRICT" = "1" ]; then
            fail "conditional PUTs are NOT atomic on $SERVER (If-None-Match both-200: $both/4, If-Match both-200: $replace_dup/4); this run requires the atomic-conditional contract"
        fi
        ENFORCE_ATOMIC=0
        warn "conditional PUTs are NOT atomic under concurrency on this server"
        warn "  If-None-Match races with both PUTs answered 200: $both/4"
        warn "  If-Match races with both PUTs answered 200:      $replace_dup/4"
        warn "  (s3mock checks the precondition outside the store lock: adobe/S3Mock"
        warn "   ObjectController.putObject -> verifyObjectMatching, then putObject)"
        warn "  the race's one-winner-per-version assertion is REPORTED here, not enforced;"
        warn "  use SERVER=seaweedfs, SERVER=rustfs or AWS S3/R2 (PINKDB_S3_LIVE=1) for the enforced check"
    fi
}

# ===========================================================================
# main
# ===========================================================================

printf 'pink-lake real-S3 integration test (SERVER=%s)\n' "$SERVER"
printf 'repo: %s\n' "$ROOT"

# --- CLI binary ------------------------------------------------------------
if [ -n "${PINKDB_BIN:-}" ]; then
    BIN="$PINKDB_BIN"
    [ -x "$BIN" ] || fail "PINKDB_BIN=$BIN is not executable"
    if ! printf '.help\n.quit\n' | "$BIN" 2>/dev/null | grep -qF 'lake save <dir|s3://bucket/prefix>'; then
        fail "PINKDB_BIN=$BIN lacks the lake+s3 features (build with --features vortexlake,s3)"
    fi
    note "using PINKDB_BIN=$BIN"
elif [ "${SKIP_BUILD:-0}" = "1" ]; then
    BIN="$ROOT/target/release/pinkdb"
    [ -x "$BIN" ] || fail "SKIP_BUILD=1 but $BIN does not exist"
    if ! printf '.help\n.quit\n' | "$BIN" 2>/dev/null | grep -qF 'lake save <dir|s3://bucket/prefix>'; then
        fail "SKIP_BUILD=1 but $BIN lacks the lake+s3 features (another build may have overwritten it; rebuild with: cargo build --release -p pinkdb-cli --features vortexlake,s3)"
    fi
    note "SKIP_BUILD=1: using $BIN"
else
    step "build the CLI (cargo build --release -p pinkdb-cli --features vortexlake,s3)"
    (cd "$ROOT" && cargo build --release -p pinkdb-cli --features vortexlake,s3) \
        || fail "cargo build --release -p pinkdb-cli --features vortexlake,s3 failed"
    BIN="$ROOT/target/release/pinkdb"
    ok "built $BIN"
fi

# --- container + capability probes -----------------------------------------
step "container ($SERVER)"
select_container
ENDPOINT="http://127.0.0.1:$PORT"
note "endpoint: $ENDPOINT (bucket $BUCKET)"
wait_ready
ensure_bucket
check_signature_enforcement
check_conditional_writes
probe_conditional_atomicity
if [ "$STRICT" = "1" ]; then
    note "policy: STRICT — one winner per version and catalog==snapshot bytes are hard assertions"
else
    note "policy: lenient — duplicates are reported as the measured server limitation"
fi

# --- clean slate for the run's prefixes ------------------------------------
step "clean slate (list + delete of the run's prefixes)"
for p in "itest/" "enc/" "enc-plain/" "conc/" "conflict/" "refuse/"; do
    wipe_prefix "$p"
done

ITEST="s3://$BUCKET/itest"
ENC="s3://$BUCKET/enc"
ENC_PLAIN="s3://$BUCKET/enc-plain"
ENC_KEY="wave-57-s3-passphrase"
CONC="s3://$BUCKET/conc"
CONFLICT="s3://$BUCKET/conflict"
REFUSE="s3://$BUCKET/refuse"

# --- roundtrip -------------------------------------------------------------
step "roundtrip: typed CSV -> .lake save $ITEST -> fresh process .lake open + SELECT"
CSV="$TMP/roundtrip.csv"
cat >"$CSV" <<'CSV'
b,i,g,d,v,dt,ts
true,1,9999999999,1.5,alice,2026-01-02,2026-01-02 03:04:05
false,-5,-9223372036854775808,-0.0,,,
true,2147483647,0,0.25,héllo ✓ 🌍,1970-01-01,1969-12-31 23:59:59.999999
CSV
note "csv: $CSV (BOOLEAN, INTEGER, BIGINT, DOUBLE, VARCHAR, DATE, TIMESTAMP; NULL and multibyte)"

{
    echo ".import $CSV t"
    echo ".lake save $ITEST"
    echo ".lake versions $ITEST"
    echo ".lake tables $ITEST"
    echo ".lake info $ITEST"
    echo ".quit"
} | cli_run "$TMP/roundtrip_save.log"
assert_status_zero "$TMP/roundtrip_save.log" "$?"
assert_contains "$TMP/roundtrip_save.log" "Imported 3 rows"
# A fresh location: open_or_create publishes the empty version 1, the save
# publishes version 2 (documented full-snapshot semantics).
assert_re "$TMP/roundtrip_save.log" '^Saved lake version 2 in .* \(1 table, 3 rows, [0-9]+ bytes written\)$'
assert_contains "$TMP/roundtrip_save.log" "lake: $ITEST"
assert_re "$TMP/roundtrip_save.log" '^1$'
assert_re "$TMP/roundtrip_save.log" '^2 \(current\)$'
assert_contains "$TMP/roundtrip_save.log" "main.t (3 rows)"
assert_contains "$TMP/roundtrip_save.log" "format version:  5"
assert_contains "$TMP/roundtrip_save.log" "current version: 2"
assert_contains "$TMP/roundtrip_save.log" "snapshots:       2 (1, 2)"
assert_contains "$TMP/roundtrip_save.log" "tables (1):"

{
    echo ".lake open $ITEST"
    echo "SELECT * FROM t ORDER BY i;"
    echo ".quit"
} | cli_run "$TMP/roundtrip_open.log"
assert_status_zero "$TMP/roundtrip_open.log" "$?"
assert_re "$TMP/roundtrip_open.log" "^Opened lake version 2 in .* \(1 table\) from s3://$BUCKET/itest$"

python3 - "$TMP/roundtrip_open.log" <<'PY' || fail "roundtrip values differ (see the diff above)"
import sys

text = open(sys.argv[1], encoding="utf-8").read()
expected_header = ["b", "i", "g", "d", "v", "dt", "ts"]
expected_types = ["BOOLEAN", "INTEGER", "BIGINT", "DOUBLE", "VARCHAR", "DATE", "TIMESTAMP"]
expected_rows = [
    ["false", "-5", "-9223372036854775808", "-0.0", "NULL", "NULL", "NULL"],
    ["true", "1", "9999999999", "1.5", "alice", "2026-01-02", "2026-01-02 03:04:05"],
    ["true", "2147483647", "0", "0.25", "héllo ✓ 🌍", "1970-01-01", "1969-12-31 23:59:59.999999"],
]
rows = [
    [cell.strip() for cell in line.strip("│").split("│")]
    for line in text.splitlines()
    if line.startswith("│")
]
if len(rows) < 2:
    print("no box-table rows found in the CLI output", file=sys.stderr)
    sys.exit(1)
header, types, data = rows[0], rows[1], rows[2:]
problems = []
if header != expected_header:
    problems.append(f"header:  {header} != {expected_header}")
if types != expected_types:
    problems.append(f"types:   {types} != {expected_types}")
if data != expected_rows:
    for i, (got, want) in enumerate(zip(data, expected_rows)):
        if got != want:
            problems.append(f"row {i}: {got} != {want}")
    if len(data) != len(expected_rows):
        problems.append(f"row count: {len(data)} != {len(expected_rows)}")
if "3 rows" not in text:
    problems.append("missing the '3 rows' summary line")
if problems:
    print("roundtrip value mismatch:", file=sys.stderr)
    for p in problems:
        print("  " + p, file=sys.stderr)
    sys.exit(1)
print("  ok: values and types match exactly (3 rows, incl. NULL row, -0.0, multibyte)")
print("  ok: types row: " + ", ".join(types))
PY

# Published objects, listed over the REST API (signed for the strict servers).
KEYS="$(list_keys "itest/")" || fail "listing itest/ failed"
printf '%s\n' "$KEYS" | grep -qxF "itest/catalog.json" || fail "itest/catalog.json is missing"
printf '%s\n' "$KEYS" | grep -qxF "itest/snapshots/1.json" || fail "itest/snapshots/1.json is missing"
printf '%s\n' "$KEYS" | grep -qxF "itest/snapshots/2.json" || fail "itest/snapshots/2.json is missing"
printf '%s\n' "$KEYS" | grep -qE '^itest/data/main\.t/[0-9a-f-]+\.vortex$' \
    || fail "the roundtrip's Vortex data object is missing"
printf '%s\n' "$KEYS" | grep -qxF "itest/commits/2.json" \
    || fail "itest/commits/2.json (the v2 delta manifest) is missing"
assert_listing_count "$KEYS" "itest/" 5
note "keys: $(printf '%s ' $KEYS)"

# catalog.json == snapshots/<version>.json (bytes), fetched + cmp.
s3curl -fsS --max-time 30 -o "$TMP/rt_catalog.json" "$ENDPOINT/$BUCKET/itest/catalog.json" || fail "fetching catalog.json failed"
s3curl -fsS --max-time 30 -o "$TMP/rt_snapshot.json" "$ENDPOINT/$BUCKET/itest/snapshots/2.json" || fail "fetching snapshots/2.json failed"
cmp -s "$TMP/rt_catalog.json" "$TMP/rt_snapshot.json" \
    || fail "catalog.json is not byte-identical to snapshots/2.json"
ok "catalog.json == snapshots/2.json (bytes; enforced)"

# --- encrypted roundtrip (PINKDB_KEY) --------------------------------------
step "encrypted roundtrip: .lake save/open/attach with PINKDB_KEY on $ENC"
note "a keyed save writes PNKLAKE1 containers (catalog + snapshots + commit"
note "deltas + data files + delete files); the harness fetches the objects over"
note "REST and checks they are ciphertext-only, byte-identical as a pair, and"
note "openable only with the right key. Requires a CLI built with the"
note 'the `encryption` feature (the CI build is --features vortexlake,s3,encryption);'
note 'a build without it is reported and skipped.'

ENC_SUPPORTED=0
if printf '.lake save %s\n.quit\n' "$TMP/enc-probe" | cli_run_keyed "$TMP/enc_probe.log" "$ENC_KEY"; then
    ENC_SUPPORTED=1
    rm -rf "$TMP/enc-probe"
else
    if grep -qF 'does not include the `encryption` feature' "$TMP/enc_probe.log"; then
        warn 'this CLI build has no `encryption` feature: skipping the encrypted roundtrip'
        warn "  (build with: cargo build --release -p pinkdb-cli --features vortexlake,s3,encryption)"
    else
        fail "the encryption feature probe failed unexpectedly (see $(basename "$TMP/enc_probe.log"))"
    fi
fi

if [ "$ENC_SUPPORTED" = "1" ]; then

{
    echo ".import $CSV t"
    echo ".lake save $ENC"
    echo ".quit"
} | cli_run_keyed "$TMP/enc_save.log" "$ENC_KEY"
assert_status_zero "$TMP/enc_save.log" "$?"
assert_re "$TMP/enc_save.log" '^Saved lake version 2 in .* \(1 table, 3 rows, [0-9]+ bytes written\)$'
assert_contains "$TMP/enc_save.log" "the lake is encrypted at rest with PINKDB_KEY"
ok "keyed save reported the encrypted lake"

# The catalog object is a container, not JSON, and equals its snapshot.
s3curl -fsS --max-time 30 -o "$TMP/enc_catalog.bin" "$ENDPOINT/$BUCKET/enc/catalog.json" \
    || fail "fetching enc/catalog.json failed"
s3curl -fsS --max-time 30 -o "$TMP/enc_snapshot.bin" "$ENDPOINT/$BUCKET/enc/snapshots/2.json" \
    || fail "fetching enc/snapshots/2.json failed"
[ "$(head -c 8 "$TMP/enc_catalog.bin")" = "PNKLAKE1" ] \
    || fail "enc/catalog.json is not a PNKLAKE1 container"
cmp -s "$TMP/enc_catalog.bin" "$TMP/enc_snapshot.bin" \
    || fail "encrypted catalog.json is not byte-identical to snapshots/2.json"
python3 - "$TMP/enc_catalog.bin" <<'PY' || fail "the encrypted catalog parses as JSON (it must not)"
import json, sys
try:
    json.load(open(sys.argv[1]))
except Exception:
    print("  ok: the ciphertext does not parse as JSON")
    sys.exit(0)
sys.exit(1)
PY
ENC_KEYS="$(list_keys "enc/")" || fail "listing enc/ failed"
printf '%s\n' "$ENC_KEYS" | grep -qE '^enc/data/main\.t/[0-9a-f-]+\.vortex$' \
    || fail "the encrypted roundtrip's data object is missing"
printf '%s\n' "$ENC_KEYS" | grep -qxF "enc/commits/2.json" \
    || fail "enc/commits/2.json (the v2 delta manifest) is missing"
for k in $(printf '%s\n' "$ENC_KEYS" | grep -E '^enc/data/'); do
    s3curl -fsS --max-time 30 -o "$TMP/enc_data.bin" "$ENDPOINT/$BUCKET/$k" \
        || fail "fetching $k failed"
    [ "$(head -c 8 "$TMP/enc_data.bin")" = "PNKLAKE1" ] \
        || fail "$k is not a PNKLAKE1 container"
done
assert_listing_count "$ENC_KEYS" "enc/" 5
ok "every enc/ object is ciphertext (catalog, snapshots, commit delta, data)"

# A fresh process with the key opens and queries the encrypted lake.
{
    echo ".lake open $ENC"
    echo "SELECT i, v FROM t ORDER BY i;"
    echo ".lake tables $ENC"
    echo ".quit"
} | cli_run_keyed "$TMP/enc_open.log" "$ENC_KEY"
assert_status_zero "$TMP/enc_open.log" "$?"
assert_contains "$TMP/enc_open.log" "Opened lake version 2"
assert_contains "$TMP/enc_open.log" "the lake is encrypted at rest with PINKDB_KEY"
assert_contains "$TMP/enc_open.log" "main.t (3 rows)"

# Attach unseals the footer without a decode.
{
    echo ".lake attach $ENC"
    echo "SELECT COUNT(*) FROM t;"
    echo ".quit"
} | cli_run_keyed "$TMP/enc_attach.log" "$ENC_KEY"
assert_status_zero "$TMP/enc_attach.log" "$?"
assert_contains "$TMP/enc_attach.log" "the lake is encrypted: attach read the whole ciphertext"

# The conditional ciphertext CAS is unchanged: a keyed incremental commit on
# the encrypted prefix publishes a delete file + new data file as containers.
{
    echo ".lake attach $ENC write"
    echo "DELETE FROM t WHERE i = 1;"
    echo "INSERT INTO t VALUES (true, 4, 40, 0.5, 'delta', NULL, NULL);"
    echo ".lake commit $ENC"
    echo ".quit"
} | cli_run_keyed "$TMP/enc_commit.log" "$ENC_KEY"
assert_status_zero "$TMP/enc_commit.log" "$?"
assert_contains "$TMP/enc_commit.log" "Committed lake version 3"
ENC_KEYS="$(list_keys "enc/")" || fail "listing enc/ failed"
printf '%s\n' "$ENC_KEYS" | grep -qE '^enc/data/main\.t/[0-9a-f-]+\.pinkdel$' \
    || fail "the encrypted commit's delete file is missing"
for k in $(printf '%s\n' "$ENC_KEYS" | grep -E '^enc/data/'); do
    s3curl -fsS --max-time 30 -o "$TMP/enc_data.bin" "$ENDPOINT/$BUCKET/$k" \
        || fail "fetching $k failed"
    [ "$(head -c 8 "$TMP/enc_data.bin")" = "PNKLAKE1" ] \
        || fail "$k is not a PNKLAKE1 container"
done
s3curl -fsS --max-time 30 -o "$TMP/enc_catalog3.bin" "$ENDPOINT/$BUCKET/enc/catalog.json" \
    || fail "fetching enc/catalog.json failed"
s3curl -fsS --max-time 30 -o "$TMP/enc_snapshot3.bin" "$ENDPOINT/$BUCKET/enc/snapshots/3.json" \
    || fail "fetching enc/snapshots/3.json failed"
cmp -s "$TMP/enc_catalog3.bin" "$TMP/enc_snapshot3.bin" \
    || fail "encrypted catalog.json is not byte-identical to snapshots/3.json after the commit"
ok "the encrypted commit's catalog == snapshot bytes and data files are ciphertext"

# No key: refused (fail closed, never a plaintext guess).
{
    echo ".lake open $ENC"
    echo ".quit"
} | cli_run "$TMP/enc_nokey.log"
[ "$?" != "0" ] || fail "opening an encrypted lake without a key must fail"
assert_contains "$TMP/enc_nokey.log" "the lake is encrypted"

# Wrong key: refused, distinguished from corruption.
{
    echo ".lake open $ENC"
    echo ".quit"
} | cli_run_keyed "$TMP/enc_wrongkey.log" "$ENC_KEY-wrong"
[ "$?" != "0" ] || fail "opening an encrypted lake with a wrong key must fail"
assert_contains "$TMP/enc_wrongkey.log" "wrong key at"

# A keyed save onto a plaintext lake is refused, never silently rewritten.
{
    echo ".import $CSV t"
    echo ".lake save $ENC_PLAIN"
    echo ".quit"
} | cli_run "$TMP/enc_plain_save.log"
assert_status_zero "$TMP/enc_plain_save.log" "$?"
{
    echo ".lake save $ENC_PLAIN"
    echo ".quit"
} | cli_run_keyed "$TMP/enc_plain_keyed.log" "$ENC_KEY"
[ "$?" != "0" ] || fail "a keyed save onto a plaintext lake must fail"
assert_contains "$TMP/enc_plain_keyed.log" "not an encrypted pink-lake container"
s3curl -fsS --max-time 30 -o "$TMP/enc_plain_catalog.json" "$ENDPOINT/$BUCKET/enc-plain/catalog.json" \
    || fail "fetching enc-plain/catalog.json failed"
python3 - "$TMP/enc_plain_catalog.json" <<'PY' || fail "the plaintext lake's catalog is not JSON (it was rewritten?)"
import json, sys
json.load(open(sys.argv[1]))
print("  ok: the plaintext lake's catalog is still plain JSON")
PY
ok "keyed open of a plaintext lake refused, and the plaintext lake is untouched"

fi # ENC_SUPPORTED

# --- concurrency (a): 3 writers x 3 rounds ---------------------------------
step "concurrency (a): 3 writers x 3 rounds on $CONC (one distinct table each)"

{ echo "CREATE TABLE warm (x INTEGER);"; echo "INSERT INTO warm VALUES (1);"; echo ".lake save $CONC"; echo ".quit"; } \
    | cli_run "$TMP/conc_seed.log"
assert_status_zero "$TMP/conc_seed.log" "$?"
assert_contains "$TMP/conc_seed.log" "Saved lake version 2"

for i in 1 2 3; do
    python3 - "$TMP/writer$i.csv" "$i" "$WROWS" <<'PY'
import sys
path, writer, rows = sys.argv[1], sys.argv[2], int(sys.argv[3])
with open(path, "w", encoding="utf-8") as f:
    f.write("id,v\n")
    for k in range(rows):
        f.write(f"{k},w{writer}-{k}\n")
PY
done
note "writer CSVs: $WROWS rows each (id INTEGER, v VARCHAR)"

GO="$TMP/go"
for r in 1 2 3; do
    rm -f "$GO"
    for i in 1 2 3; do
        (
            # Race barrier: every writer blocks until all three are started.
            while [ ! -e "$GO" ]; do sleep 0.01; done
            { echo ".import $TMP/writer$i.csv t$i"; echo ".lake save $CONC"; echo ".quit"; } \
                | cli_run "$TMP/conc_r${r}_w${i}.log"
            echo "$?" >"$TMP/conc_r${r}_w${i}.exit"
        ) &
    done
    sleep 0.5
    : >"$GO"
    wait
    rm -f "$GO"
    note "round $r launched (3 simultaneous writers)"
done

# The save outcome (a version claim, or the documented write conflict) is
# classified below together with the exit code that belongs to it: since
# wave 40b the CLI exits 1 after a failed statement in a piped script, so a
# losing racer is expected to exit nonzero — and it must say why.
for f in "$TMP"/conc_r*_w*.log; do
    assert_contains "$f" "Imported $WROWS rows"
done

python3 - "$TMP" "$ENFORCE_ATOMIC" <<'PY' || fail "the race did not behave as documented (see above)"
import glob, os, re, sys

tmp, atomic = sys.argv[1], sys.argv[2] == "1"
logs = sorted(glob.glob(os.path.join(tmp, "conc_r*_w*.log")))
if len(logs) != 9:
    print(f"expected 9 writer logs, found {len(logs)}", file=sys.stderr)
    sys.exit(1)

claims = {}          # version -> [log names that were told they published it]
conflicts = []
problems = []
conflict_forms = ("another writer published version", "already exists", "changed while this save was in flight")
for log in logs:
    name = os.path.basename(log)
    text = open(log, encoding="utf-8", errors="replace").read()
    saved = [int(v) for v in re.findall(r"^Saved lake version (\d+)\b", text, re.M)]
    errors = [l for l in text.splitlines() if l.startswith("Error:")]
    try:
        code = int(open(log[:-3] + "exit", encoding="utf-8").read().strip())
    except (OSError, ValueError):
        problems.append(f"{name}: missing or unreadable exit status")
        continue
    if len(saved) == 1 and not errors:
        if code != 0:
            problems.append(f"{name}: published a version but exited {code}")
            continue
        claims.setdefault(saved[0], []).append(name)
    elif not saved and len(errors) == 1:
        message = errors[0]
        if "write conflict at" not in message or not any(p in message for p in conflict_forms):
            problems.append(f"{name}: save failed without the documented conflict: {message}")
        else:
            if code == 0:
                problems.append(f"{name}: documented conflict but exited 0")
            conflicts.append((name, message))
    else:
        problems.append(f"{name}: expected exactly one save outcome, got saved={saved} errors={errors}")

if problems:
    print("race problems:", file=sys.stderr)
    for p in problems:
        print("  " + p, file=sys.stderr)
    sys.exit(1)

versions = sorted(claims)
if not versions:
    print("no writer published anything", file=sys.stderr)
    sys.exit(1)
if versions != list(range(versions[0], versions[0] + len(versions))):
    print(f"the version chain has a gap: {versions}", file=sys.stderr)
    sys.exit(1)
duplicates = {v: names for v, names in sorted(claims.items()) if len(names) > 1}
final = versions[-1]
total_claims = sum(len(names) for names in claims.values())
with open(os.path.join(tmp, "conc.state"), "w", encoding="utf-8") as f:
    f.write(f"final={final}\nclaims={total_claims}\nunique_versions={len(versions)}\n")
    f.write(f"conflicts={len(conflicts)}\nduplicates={len(duplicates)}\n")
    f.write(f"final_claimants={len(claims[final])}\n")

print(f"  9 save attempts -> {total_claims} claim(s) of {len(versions)} version(s), {len(conflicts)} conflict(s)")
for v, names in sorted(claims.items()):
    print(f"    version {v}: claimed by {', '.join(names)}")
for name, message in conflicts:
    print(f"    conflict ({name}): {message[:110]}")
for v, names in duplicates.items():
    target = sys.stderr if atomic else sys.stdout
    heading = "DUPLICATE PUBLISH (wrong on an atomic server):" if atomic else "duplicate claim (server quirk):"
    print(f"    {heading} version {v} claimed by {', '.join(names)}", file=target)
if duplicates and atomic:
    print("the server claimed atomic conditionals but let two writers publish one version", file=sys.stderr)
    sys.exit(1)
if duplicates:
    print("  NOTE: the exactly-one-winner-per-version assertion is NOT enforced on this server")
    print("        (non-atomic conditionals; see s3_docker_notes.md) — the catalog chain is gapless")
else:
    print(f"  ok: exactly one winner per version, no gaps (versions {versions[0]}..{final})")
print(f"  ok: every loser got the documented write conflict ({len(conflicts)} conflict(s))")
PY

FINAL="$(sed -n 's/^final=//p' "$TMP/conc.state")"
CLAIMS="$(sed -n 's/^claims=//p' "$TMP/conc.state")"
UNIQUE_VERSIONS="$(sed -n 's/^unique_versions=//p' "$TMP/conc.state")"
RACE_CONFLICTS="$(sed -n 's/^conflicts=//p' "$TMP/conc.state")"
DUPES="$(sed -n 's/^duplicates=//p' "$TMP/conc.state")"
RACE_FINAL_CLAIMANTS="$(sed -n 's/^final_claimants=//p' "$TMP/conc.state")"
note "final race version: $FINAL ($CLAIMS claims, $RACE_CONFLICTS conflicts, $DUPES duplicate claim(s), $RACE_FINAL_CLAIMANTS claimant(s))"

# The raced catalog must name a snapshot that exists and is byte-identical:
# on an atomic-conditional server that is a hard invariant (the one-winner
# snapshot PUT is what the catalog's If-Match replace published, and neither
# can be overwritten); on s3mock it is reported as the measured limitation.
step "race byte check: catalog.json vs the snapshot it names"
RACE_CATALOG="$TMP/conc_race_catalog.json"
RACE_CATALOG_V=""
if s3curl -fsS --max-time 30 -o "$RACE_CATALOG" "$ENDPOINT/$BUCKET/conc/catalog.json"; then
    RACE_CATALOG_V="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["version"])' "$RACE_CATALOG" 2>/dev/null)" || RACE_CATALOG_V=""
fi
if [ -n "$RACE_CATALOG_V" ] \
    && s3curl -fsS --max-time 30 -o "$TMP/conc_race_snapshot.json" "$ENDPOINT/$BUCKET/conc/snapshots/$RACE_CATALOG_V.json" \
    && cmp -s "$RACE_CATALOG" "$TMP/conc_race_snapshot.json"; then
    ok "catalog.json == snapshots/$RACE_CATALOG_V.json (bytes)"
else
    if [ "$STRICT" = "1" ]; then
        fail "race: catalog.json is not byte-identical to the snapshot it names (catalog version ${RACE_CATALOG_V:-unreadable})"
    fi
    warn "race: catalog.json vs snapshots/${RACE_CATALOG_V:-?}.json mismatch (non-atomic server; reported, not enforced — see s3_docker_notes.md)"
fi

# Reopen and continue (the documented recovery after a conflict): one fresh
# save settles the chain. Its version has exactly one claimant by
# construction, so the catalog/snapshot byte-equality below is enforced even
# on a server with non-atomic conditionals.
step "settle save after the race (reopen and continue)"
{ echo ".lake info $CONC"; echo "CREATE TABLE settle (x INTEGER);"; echo "INSERT INTO settle VALUES (7);"; echo ".lake save $CONC"; echo ".quit"; } \
    | cli_run "$TMP/conc_settle.log"
assert_status_zero "$TMP/conc_settle.log" "$?"
CATALOG_V="$(sed -n 's/.*current version: \([0-9][0-9]*\).*/\1/p' "$TMP/conc_settle.log" | head -1)"
SAVED_V="$(sed -n 's/^Saved lake version \([0-9][0-9]*\) in.*/\1/p' "$TMP/conc_settle.log" | head -1)"
[ -n "$CATALOG_V" ] && [ -n "$SAVED_V" ] \
    || fail "the settle session did not report both the catalog version and the saved version"
[ "$CATALOG_V" -ge "$FINAL" ] \
    || fail "the catalog ($CATALOG_V) regressed below the race's last claim ($FINAL)"
[ "$SAVED_V" = "$((CATALOG_V + 1))" ] \
    || fail "the settle save published $SAVED_V from catalog version $CATALOG_V (expected +1)"
assert_re "$TMP/conc_settle.log" '^Saved lake version [0-9]+ in .* \(1 table, 1 row, [0-9]+ bytes written\)$'
ok "reopened after the race; the settle save published version $SAVED_V alone"
FINAL="$SAVED_V"

{ echo ".lake versions $CONC"; echo ".lake info $CONC"; echo ".lake tables $CONC"; echo ".quit"; } \
    | cli_run "$TMP/conc_metadata.log"
assert_status_zero "$TMP/conc_metadata.log" "$?"
assert_contains "$TMP/conc_metadata.log" "current version: $FINAL"
python3 - "$TMP/conc_metadata.log" "$FINAL" <<'PY' || fail "the published version chain is not 1..final"
import re, sys
text, final = open(sys.argv[1], encoding="utf-8").read(), int(sys.argv[2])
versions = []
for line in text.splitlines():
    m = re.match(r"^(\d+)( \(current\))?$", line.strip())
    if m:
        versions.append(int(m.group(1)))
if not versions:
    print("no version lines found", file=sys.stderr)
    sys.exit(1)
if versions != list(range(1, final + 1)):
    print(f".lake versions listed {versions}, expected 1..{final}", file=sys.stderr)
    sys.exit(1)
if f"{final} (current)" not in text:
    print(f".lake versions did not mark {final} as current", file=sys.stderr)
    sys.exit(1)
print(f"  ok: .lake versions lists 1..{final} with {final} marked current")
PY
assert_contains "$TMP/conc_metadata.log" "main.settle (1 row)"

# catalog.json == snapshots/<final>.json (bytes) for the final version.
s3curl -fsS --max-time 30 -o "$TMP/conc_catalog.json" "$ENDPOINT/$BUCKET/conc/catalog.json" || fail "fetching conc/catalog.json failed"
s3curl -fsS --max-time 30 -o "$TMP/conc_snapshot.json" "$ENDPOINT/$BUCKET/conc/snapshots/$FINAL.json" \
    || fail "fetching conc/snapshots/$FINAL.json failed"
V_JSON="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["version"])' "$TMP/conc_catalog.json")" \
    || fail "conc/catalog.json does not parse as JSON"
[ "$V_JSON" = "$FINAL" ] || fail "catalog.json declares version $V_JSON, expected $FINAL"
cmp -s "$TMP/conc_catalog.json" "$TMP/conc_snapshot.json" \
    || fail "conc: catalog.json is not byte-identical to snapshots/$FINAL.json"
ok "catalog.json == snapshots/$FINAL.json (bytes, version field $V_JSON, single claimant; enforced)"

CONC_KEYS="$(list_keys "conc/")" || fail "listing conc/ failed"
for v in $(seq 1 "$FINAL"); do
    printf '%s\n' "$CONC_KEYS" | grep -qxF "conc/snapshots/$v.json" \
        || fail "conc/snapshots/$v.json is missing"
done
ok "conc/snapshots/1..$FINAL all exist"
DATA_COUNT="$(printf '%s\n' "$CONC_KEYS" | grep -cE '^conc/data/')"
printf '%s\n' "$CONC_KEYS" | grep -qE '^conc/data/main\.[A-Za-z0-9_-]+/[0-9a-f-]+\.vortex$' \
    || fail "conc data objects are missing"
for v in $(seq 2 "$FINAL"); do
    printf '%s\n' "$CONC_KEYS" | grep -qxF "conc/commits/$v.json" \
        || fail "conc/commits/$v.json (the v$v delta manifest) is missing"
done
ok "conc/commits/2..$FINAL all exist"
# Every published version above the empty v1 has exactly one referenced data
# object; a writer that lost after uploading, and an extra claimant of a
# version on a non-atomic server, may leave unreferenced leftovers
# (documented, harmless, no GC).
if [ "$DATA_COUNT" -lt "$((FINAL - 1))" ]; then
    fail "fewer data objects ($DATA_COUNT) than published versions above v1 ($((FINAL - 1)))"
fi
LEFTOVER_MAX=$((FINAL - 1 + RACE_CONFLICTS + CLAIMS - UNIQUE_VERSIONS))
if [ "$DATA_COUNT" -gt "$LEFTOVER_MAX" ]; then
    fail "more data objects ($DATA_COUNT) than referenced + possible leftovers ($LEFTOVER_MAX)"
fi
assert_listing_count "$CONC_KEYS" "conc/" "$((1 + FINAL + (FINAL - 1) + DATA_COUNT))"
ok "conc/data: $DATA_COUNT object(s), $((DATA_COUNT - (FINAL - 1))) unreferenced leftover(s) from conflicted writers"

# --- concurrency (b): a slow save deterministically loses one version ------
step "concurrency (b): slow save vs quick save for the same next version on $CONFLICT"
note "the slow writer opens the lake at v2 and encodes $SLOWROWS rows; the quick"
note "writer publishes v3 while it is busy. Exactly one of them may win v3; the"
note "loser must get a documented write conflict (this is deterministic by"
note "construction: the winner's snapshot is already published when the slow"
note "writer gets to its own snapshot PUT — no atomicity needed for that PUT)."
note "SLOWROWS is sized so the slow save is still in flight when the quick"
note "writer reads the catalog even on a fast local server (measured: a local"
note "RustFS saves 400k rows in ~0.1s, which was too short)."

python3 - "$TMP/slow.csv" "$SLOWROWS" <<'PY'
import sys
with open(sys.argv[1], "w", encoding="utf-8") as f:
    f.write("id,v\n")
    for k in range(int(sys.argv[2])):
        f.write(f"{k},slow-{k}\n")
PY
python3 - "$TMP/quick.csv" "$QUICKROWS" <<'PY'
import sys
with open(sys.argv[1], "w", encoding="utf-8") as f:
    f.write("id,v\n")
    for k in range(int(sys.argv[2])):
        f.write(f"{k},quick-{k}\n")
PY

{ echo "CREATE TABLE warm (x INTEGER);"; echo "INSERT INTO warm VALUES (1);"; echo ".lake save $CONFLICT"; echo ".quit"; } \
    | cli_run "$TMP/conflict_seed.log"
assert_status_zero "$TMP/conflict_seed.log" "$?"
assert_contains "$TMP/conflict_seed.log" "Saved lake version 2"

(
    { echo ".import $TMP/slow.csv tslow"; echo ".lake save $CONFLICT"; echo ".quit"; } \
        | cli_run "$TMP/conflict_slow.log"
    echo "$?" >"$TMP/conflict_slow.exit"
) &
SLOW_PID=$!
# Head start: long enough that the slow writer has read the catalog (v2) and
# is inside its encode/upload when the quick writer reads it too, short
# enough that the slow writer has not published v3 yet. With SLOWROWS sized
# to ~1s of encoding the window is wide on any local server.
sleep 0.3
{ echo ".import $TMP/quick.csv tquick"; echo ".lake save $CONFLICT"; echo ".quit"; } \
    | cli_run "$TMP/conflict_quick.log"
assert_status_zero "$TMP/conflict_quick.log" "$?"
wait "$SLOW_PID"
# The slow writer is the expected loser. Since wave 40b the CLI exits 1 after
# a failed statement in a piped script, so a nonzero exit is correct here; the
# python block below classifies its log (exactly one error: the documented
# write conflict). The exit file is kept for diagnostics only.

python3 - "$TMP" <<'PY' || fail "the slow-save conflict did not behave as documented (see above)"
import os, re, sys

tmp = sys.argv[1]
logs = {name: open(os.path.join(tmp, f"conflict_{name}.log"), encoding="utf-8", errors="replace").read()
        for name in ("slow", "quick")}
saved = {name: [int(v) for v in re.findall(r"^Saved lake version (\d+)\b", text, re.M)]
         for name, text in logs.items()}
errors = {name: [l for l in text.splitlines() if l.startswith("Error:")]
          for name, text in logs.items()}
winners = [name for name, v in saved.items() if v]
losers = [name for name in logs if name not in winners]
if winners == [] or losers == []:
    for name in logs:
        print(f"{name}: saved={saved[name]} errors={errors[name]}", file=sys.stderr)
    print("expected exactly one winner and one loser", file=sys.stderr)
    sys.exit(1)
if len(winners) != 1:
    if len(winners) == 2 and saved["slow"] != saved["quick"]:
        print(f"both writers published, but different versions (slow {saved['slow']}, "
              f"quick {saved['quick']}): the quick writer read the catalog after the slow "
              "writer had already published, so the two never raced for the same version — "
              "the harness's timing window was missed. Raise SLOWROWS (and/or lower the "
              "quick writer's head start) until the slow save is still in flight when the "
              "quick writer reads the catalog.", file=sys.stderr)
    else:
        print(f"expected exactly one winner of one version, got: {saved}", file=sys.stderr)
    sys.exit(1)
winner, loser = winners[0], losers[0]
if saved[winner] != [3]:
    print(f"{winner} published {saved[winner]}, expected [3]", file=sys.stderr)
    sys.exit(1)
if len(errors[loser]) != 1:
    print(f"{loser}: expected one error line, got {errors[loser]}", file=sys.stderr)
    sys.exit(1)
message = errors[loser][0]
if "write conflict at" not in message or not any(
    p in message for p in ("snapshot 3 already exists", "another writer published version", "changed while this save was in flight")
):
    print(f"{loser}: unexpected conflict message: {message}", file=sys.stderr)
    sys.exit(1)
with open(os.path.join(tmp, "conflict.state"), "w", encoding="utf-8") as f:
    f.write(f"winner={winner}\nloser={loser}\n")
print(f"  ok: exactly one winner of version 3: the {winner} save")
print(f"  ok: the {loser} save got the documented conflict: {message[:110]}")
PY
WINNER="$(sed -n 's/^winner=//p' "$TMP/conflict.state")"

s3curl -fsS --max-time 30 -o "$TMP/conflict_catalog.json" "$ENDPOINT/$BUCKET/conflict/catalog.json" || fail "fetching conflict/catalog.json failed"
s3curl -fsS --max-time 30 -o "$TMP/conflict_snapshot.json" "$ENDPOINT/$BUCKET/conflict/snapshots/3.json" \
    || fail "fetching conflict/snapshots/3.json failed"
cmp -s "$TMP/conflict_catalog.json" "$TMP/conflict_snapshot.json" \
    || fail "conflict: catalog.json is not byte-identical to snapshots/3.json"
ok "catalog.json == snapshots/3.json (bytes; single claimant; enforced)"

{ echo ".lake versions $CONFLICT"; echo ".lake tables $CONFLICT"; echo ".lake open $CONFLICT"; echo ".quit"; } \
    | cli_run "$TMP/conflict_state.log"
assert_status_zero "$TMP/conflict_state.log" "$?"
assert_re "$TMP/conflict_state.log" '^3 \(current\)$'
assert_no_match "$TMP/conflict_state.log" '^4'
assert_re "$TMP/conflict_state.log" '^main\.t'"$WINNER"' \([0-9]+ rows\)$'
assert_re "$TMP/conflict_state.log" "^Opened lake version 3 in .* \(1 table\) from s3://$BUCKET/conflict$"

# --- refusal ---------------------------------------------------------------
step "refusal: second .lake save and a planted stray snapshot on $REFUSE"
note "the CLI opens a fresh lake handle per .lake save, so a stale-handle"
note "conflict is not reachable from the REPL; a second save publishes a new"
note "version (documented full-snapshot semantics), and the CLI-reachable"
note "conflict is the snapshot-exists refusal asserted below."

{
    echo ".import $CSV t"
    echo ".lake save $REFUSE"
    echo ".lake save $REFUSE"
    echo ".lake versions $REFUSE"
    echo ".quit"
} | cli_run "$TMP/refuse_twice.log"
assert_status_zero "$TMP/refuse_twice.log" "$?"
assert_re "$TMP/refuse_twice.log" '^Saved lake version 2 in '
assert_re "$TMP/refuse_twice.log" '^Saved lake version 3 in '
assert_re "$TMP/refuse_twice.log" '^2$'
assert_re "$TMP/refuse_twice.log" '^3 \(current\)$'
ok "a second .lake save in the same session published version 3 (no stale view)"

# Plant an unpublished stray at exactly the next version: the save must
# refuse to replace it instead of clobbering (either an existing snapshot or a
# crash leftover).
s3curl -fsS --max-time 30 -X PUT --data-binary 'not a manifest' \
    "$ENDPOINT/$BUCKET/refuse/snapshots/4.json" -o /dev/null || fail "planting the stray snapshot failed"
ok "planted stray refuse/snapshots/4.json"

{ echo ".lake save $REFUSE"; echo ".quit"; } | cli_run "$TMP/refuse_stray.log"
# The refusal is the expected outcome: since wave 40b the CLI exits 1 after a
# failed statement in a piped script; the message asserts below pin it.
assert_contains "$TMP/refuse_stray.log" "Error: write conflict at"
assert_contains "$TMP/refuse_stray.log" "snapshot 4 already exists"
assert_contains "$TMP/refuse_stray.log" "refusing to replace a published version"

{ echo ".lake versions $REFUSE"; echo ".quit"; } | cli_run "$TMP/refuse_versions.log"
assert_status_zero "$TMP/refuse_versions.log" "$?"
assert_re "$TMP/refuse_versions.log" '^3 \(current\)$'
assert_no_match "$TMP/refuse_versions.log" '^4'
ok "the unpublished stray is not advertised (.lake versions stays 1..3)"

s3curl -fsS --max-time 30 -X DELETE "$ENDPOINT/$BUCKET/refuse/snapshots/4.json" -o /dev/null \
    || fail "removing the stray snapshot failed"
{ echo ".lake save $REFUSE"; echo ".quit"; } | cli_run "$TMP/refuse_retry.log"
assert_status_zero "$TMP/refuse_retry.log" "$?"
assert_re "$TMP/refuse_retry.log" '^Saved lake version 4 in '
ok "after removing the stray, the next save published version 4"

# --- summary ---------------------------------------------------------------
step "summary"
printf '  server:        %s (container %s at %s)\n' "$SERVER" "$CONTAINER" "$ENDPOINT"
if [ "$SIG_VERIFIED" = "1" ]; then
    printf '  signatures:    SigV4 enforced — unsigned PUT -> 403 (asserted)\n'
else
    printf '  signatures:    not verified by this endpoint (unsigned accepted; reported, not asserted — see s3_docker_notes.md)\n'
fi
if [ "$STRICT" = "1" ]; then
    printf '  enforcement:   STRICT — one winner per version and catalog == snapshot bytes are hard assertions\n'
else
    printf '  enforcement:   lenient — duplicate race publications reported as the measured s3mock limitation\n'
fi
printf '  roundtrip:     version 2, 3 rows round-tripped through a fresh process; 4 objects listed over REST\n'
if [ "$DUPES" = "0" ]; then
    printf '  concurrency:   race: 9 attempts, %s claims, %s conflict(s), one winner per version;\n' "$CLAIMS" "$RACE_CONFLICTS"
else
    printf '  concurrency:   race: 9 attempts, %s claims, %s conflict(s), %s duplicate claim(s) — server non-atomic;\n' \
        "$CLAIMS" "$RACE_CONFLICTS" "$DUPES"
fi
printf '                 slow-vs-quick: exactly one winner of v3 (the %s save), loser conflicted; catalog == snapshots/3.json\n' "$WINNER"
printf '                 settle save after the race: version %s alone; catalog == snapshots/%s.json\n' "$FINAL" "$FINAL"
printf '  refusal:       second save -> v3 (new version); stray snapshot -> write conflict; removed -> v4\n'
if [ "$ENC_SUPPORTED" = "1" ]; then
    printf '  encrypted:     PINKDB_KEY save/commit/open/attach round trip; ciphertext-only objects;\n'
    printf '                 catalog == snapshot bytes under encryption; no/wrong key refused; a plaintext\n'
    printf '                 lake is never rewritten\n'
else
    printf '  encrypted:     SKIPPED (this CLI build has no `encryption` feature)\n'
fi
printf '\nALL CHECKS PASSED'
if [ "$DUPES" != "0" ]; then
    printf ' (the exactly-one-winner race assertion is not enforced on this non-atomic server)'
fi
printf '\n'
