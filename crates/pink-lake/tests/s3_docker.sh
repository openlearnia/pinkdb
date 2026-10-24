#!/usr/bin/env bash
# ============================================================================
# pink-lake real-S3 integration test: a real S3 server in Docker, release CLI.
# ============================================================================
#
# The in-repo `mock_s3.rs` (used by
# `cargo test -p pink-lake --features s3` and the CLI's
# `lake_s3` tests) is the hermetic protocol suite — it even
# verifies SigV4 on every request — but it is a home-grown server inside the
# test process. This script is the independent check: a real, separate HTTP
# server in a container, speaking real S3 semantics over a socket, driven
# through the release CLI just like a user would.
#
# It needs Docker, python3 and curl 7.75+ (for `--aws-sigv4`, the harness's
# own signed probes) plus cargo for the default CLI build:
#   * SERVER=s3mock     adobe/s3mock:latest — accepts unsigned requests (no
#                       SigV4 verification).
#   * SERVER=seaweedfs  chrislusf/seaweedfs:latest — a container started with
#                       `-s3.config` identities verifies SigV4 (unsigned PUT
#                       -> 403): STRICT.
#                       A pre-existing `pk-seaweed` running SeaweedFS's
#                       DEFAULT config is measured to accept anonymous writes
#                       and is therefore NOT reused: the harness leaves it
#                       untouched and starts its own identity-configured
#                       `pinkdb-seaweed-itest`. (A `pk-seaweed` that verifies
#                       SigV4 is reused.)
#   * SERVER=rustfs     rustfs/rustfs:latest — verifies SigV4 (unsigned and
#                       wrong-key puts -> 403): STRICT.
#   Strict means: an unsigned PUT must be answered 403 (asserted).
#
# The lake's concurrency contract does not depend on the server: since wave 103
# the catalog is a database transaction on local disk and S3 holds data files
# only, so exactly one writer wins each version on every endpoint and the race
# assertions are hard failures everywhere.
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
# S3 conditional PUTs are no longer in the lake's write path, so the harness no
# longer probes them; `s3_docker_notes.md` keeps the pre-wave-103 measurements
# (SeaweedFS/RustFS atomic, s3mock check-then-act) as history.
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
# The harness asserts the CLI's human output (box tables, plain errors); when
# it is run from a coding agent's shell (AI_AGENT, CLAUDECODE, ...) the CLI
# would otherwise switch to agent mode (wave 99).
export PINKDB_AGENT_MODE=0

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
# server capability probe -> enforcement policy
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

# ===========================================================================
# main
# ===========================================================================

printf 'pink-lake real-S3 integration test (SERVER=%s)\n' "$SERVER"
printf 'repo: %s\n' "$ROOT"

# --- CLI binary ------------------------------------------------------------
# Whether a CLI build has the lake + s3 features: its `.help` documents an
# S3 data location for `.lake save`. The help text is captured first, because
# `grep -q` exiting early would SIGPIPE the CLI and, under `pipefail`, make a
# matching build look like a failure.
lake_s3_help() {
    local help
    help="$(printf '.help\n.quit\n' | "$1" 2>/dev/null)" || true
    printf '%s\n' "$help" | grep -qF '.lake save <catalog> [data <dir|s3://bucket/prefix>]'
}

if [ -n "${PINKDB_BIN:-}" ]; then
    BIN="$PINKDB_BIN"
    [ -x "$BIN" ] || fail "PINKDB_BIN=$BIN is not executable"
    if ! lake_s3_help "$BIN"; then
        fail "PINKDB_BIN=$BIN lacks the lake+s3 features (build with --features pinklake,s3)"
    fi
    note "using PINKDB_BIN=$BIN"
elif [ "${SKIP_BUILD:-0}" = "1" ]; then
    BIN="$ROOT/target/release/pinkdb"
    [ -x "$BIN" ] || fail "SKIP_BUILD=1 but $BIN does not exist"
    if ! lake_s3_help "$BIN"; then
        fail "SKIP_BUILD=1 but $BIN lacks the lake+s3 features (another build may have overwritten it; rebuild with: cargo build --release -p pinkdb-cli --features pinklake,s3)"
    fi
    note "SKIP_BUILD=1: using $BIN"
else
    step "build the CLI (cargo build --release -p pinkdb-cli --features pinklake,s3)"
    (cd "$ROOT" && cargo build --release -p pinkdb-cli --features pinklake,s3) \
        || fail "cargo build --release -p pinkdb-cli --features pinklake,s3 failed"
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
note "policy: the catalog database decides every commit, so the race assertions"
note "  are hard failures on every endpoint (STRICT adds only the SigV4 check)"

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
ITEST_CAT="pink:$TMP/itest.pink"
ENC_CAT="pink:$TMP/enc.pink"
ENC_PLAIN_CAT="pink:$TMP/enc-plain.pink"
CONC_CAT="pink:$TMP/conc.pink"
CONFLICT_CAT="pink:$TMP/conflict.pink"
REFUSE_CAT="pink:$TMP/refuse.pink"

# --- roundtrip -------------------------------------------------------------
step "roundtrip: typed CSV -> .lake save $ITEST_CAT data $ITEST -> fresh process .lake open + SELECT"
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
    echo ".lake save $ITEST_CAT data $ITEST"
    echo ".lake versions $ITEST_CAT"
    echo ".lake tables $ITEST_CAT"
    echo ".lake info $ITEST_CAT"
    echo ".quit"
} | cli_run "$TMP/roundtrip_save.log"
assert_status_zero "$TMP/roundtrip_save.log" "$?"
assert_contains "$TMP/roundtrip_save.log" "Imported 3 rows"
# A fresh location: open_or_create publishes the empty version 1, the save
# publishes version 2 (documented full-snapshot semantics).
assert_re "$TMP/roundtrip_save.log" '^Saved lake version 2 in .* \(1 table, 3 rows, [0-9]+ bytes written\)$'
assert_contains "$TMP/roundtrip_save.log" "lake: $ITEST"
assert_contains "$TMP/roundtrip_save.log" "Lake $ITEST"   # .lake info reports the data location
assert_re "$TMP/roundtrip_save.log" '^1$'
assert_re "$TMP/roundtrip_save.log" '^2 \(current\)$'
assert_contains "$TMP/roundtrip_save.log" "main.t (3 rows)"
assert_contains "$TMP/roundtrip_save.log" "format version:  7"
assert_contains "$TMP/roundtrip_save.log" "current version: 2"
assert_contains "$TMP/roundtrip_save.log" "snapshots:       2 (1, 2)"
assert_contains "$TMP/roundtrip_save.log" "tables (1):"

{
    echo ".lake open $ITEST_CAT"
    echo "SELECT * FROM t ORDER BY i;"
    echo ".quit"
} | cli_run "$TMP/roundtrip_open.log"
assert_status_zero "$TMP/roundtrip_open.log" "$?"
assert_contains "$TMP/roundtrip_open.log" "Opened lake version 2"
assert_contains "$TMP/roundtrip_open.log" "(1 table) from $ITEST_CAT"

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

# Published objects, listed over the REST API (signed for the strict servers):
# the catalog is a local pinkdb database; S3 holds only data files.
KEYS="$(list_keys "itest/")" || fail "listing itest/ failed"
printf '%s\n' "$KEYS" | grep -qE '^itest/data/main\.t/[0-9a-f-]+\.(pink|parquet)$' \
    || fail "the roundtrip's data object is missing"
if printf '%s\n' "$KEYS" | grep -qE 'itest/(catalog\.json|snapshots/|commits/|highest\.json|lake\.json)'; then
    fail "metadata objects must not be written to S3 anymore: $KEYS"
fi
printf '%s\n' "$KEYS" | grep -qxF "itest/lake-data.json" \
    || fail "the lake-data.json ownership marker is missing"
assert_listing_count "$KEYS" "itest/" 2
[ -f "$TMP/itest.pink" ] || [ -f "$TMP/itest.pink.wal" ] \
    || fail "the local pink catalog is missing"
ok "S3 holds the marker and one data object; the catalog is the local pink: database"

# --- encrypted roundtrip (PINKDB_KEY) --------------------------------------
step "encrypted roundtrip: .lake save/open/attach with PINKDB_KEY on $ENC"
note "a keyed save writes PNKLAKE1 data containers, and a keyed catalog is a"
note "sealed pink: database on local disk; the harness fetches every enc/ data"
note "object over REST and checks it is ciphertext-only. Requires a CLI built"
note 'with the `encryption` feature (the CI build is --features pinklake,s3,encryption);'
note 'a build without it is reported and skipped.'

ENC_SUPPORTED=0
if printf '.lake save %s\n.quit\n' "$TMP/enc-probe" | cli_run_keyed "$TMP/enc_probe.log" "$ENC_KEY"; then
    ENC_SUPPORTED=1
    rm -rf "$TMP/enc-probe"
else
    if grep -qF 'encryption support is not compiled in' "$TMP/enc_probe.log"; then
        warn 'this CLI build has no `encryption` feature: skipping the encrypted roundtrip'
        warn "  (build with: cargo build --release -p pinkdb-cli --features pinklake,s3,encryption)"
    else
        fail "the encryption feature probe failed unexpectedly (see $(basename "$TMP/enc_probe.log"))"
    fi
fi

if [ "$ENC_SUPPORTED" = "1" ]; then

{
    echo ".import $CSV t"
    echo ".lake save $ENC_CAT data $ENC"
    echo ".quit"
} | cli_run_keyed "$TMP/enc_save.log" "$ENC_KEY"
assert_status_zero "$TMP/enc_save.log" "$?"
assert_re "$TMP/enc_save.log" '^Saved lake version 2 in .* \(1 table, 3 rows, [0-9]+ bytes written\)$'
assert_contains "$TMP/enc_save.log" "the lake is encrypted at rest with PINKDB_KEY"
ok "keyed save reported the encrypted lake"

# Only data objects live on S3, each a PNKLAKE1 container.
ENC_KEYS="$(list_keys "enc/")" || fail "listing enc/ failed"
printf '%s\n' "$ENC_KEYS" | grep -qE '^enc/data/main\.t/[0-9a-f-]+\.(pink|parquet)$' \
    || fail "the encrypted roundtrip's data object is missing"
for k in $(printf '%s\n' "$ENC_KEYS" | grep -E '^enc/data/'); do
    s3curl -fsS --max-time 30 -o "$TMP/enc_data.bin" "$ENDPOINT/$BUCKET/$k" \
        || fail "fetching $k failed"
    [ "$(head -c 8 "$TMP/enc_data.bin")" = "PNKLAKE1" ] \
        || fail "$k is not a PNKLAKE1 container"
done
if printf '%s\n' "$ENC_KEYS" | grep -qE 'enc/(catalog\.json|snapshots/|commits/|highest\.json|lake\.json)'; then
    fail "encrypted metadata must not be written to S3: $ENC_KEYS"
fi
ok "every enc/ data object is a PNKLAKE1 container"

# A fresh process with the key opens and queries the encrypted lake.
{
    echo ".lake open $ENC_CAT"
    echo "SELECT i, v FROM t ORDER BY i;"
    echo ".lake tables $ENC_CAT"
    echo ".quit"
} | cli_run_keyed "$TMP/enc_open.log" "$ENC_KEY"
assert_status_zero "$TMP/enc_open.log" "$?"
assert_contains "$TMP/enc_open.log" "Opened lake version 2"
assert_contains "$TMP/enc_open.log" "the lake is encrypted at rest with PINKDB_KEY"
assert_contains "$TMP/enc_open.log" "main.t (3 rows)"

# Attach is statistics-only on an encrypted lake too: it unseals the catalog
# and its statistics and reads no data object, and COUNT(*) is answered from
# the catalog.
{
    echo ".lake attach $ENC_CAT"
    echo "SELECT COUNT(*) FROM t;"
    echo ".quit"
} | cli_run_keyed "$TMP/enc_attach.log" "$ENC_KEY"
assert_status_zero "$TMP/enc_attach.log" "$?"
assert_contains "$TMP/enc_attach.log" "(1 table, statistics-only)"
assert_contains "$TMP/enc_attach.log" "attach unsealed the catalog and its statistics blocks and read no data file"
assert_contains "$TMP/enc_attach.log" "│ 3        │"

# A keyed incremental commit on the encrypted prefix publishes a delete file
# and a new data file as containers; S3 still holds data only.
{
    echo ".lake attach $ENC_CAT write"
    echo "DELETE FROM t WHERE i = 1;"
    echo "INSERT INTO t VALUES (true, 4, 40, 0.5, 'delta', NULL, NULL);"
    echo ".lake commit $ENC_CAT"
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
ok "the encrypted commit's data and delete files are ciphertext"

# No key: refused (fail closed, never a plaintext guess).
{
    echo ".lake open $ENC_CAT"
    echo ".quit"
} | cli_run "$TMP/enc_nokey.log"
[ "$?" != "0" ] || fail "opening an encrypted lake without a key must fail"
assert_contains "$TMP/enc_nokey.log" "the lake is encrypted"

# Wrong key: refused, distinguished from corruption.
{
    echo ".lake open $ENC_CAT"
    echo ".quit"
} | cli_run_keyed "$TMP/enc_wrongkey.log" "$ENC_KEY-wrong"
[ "$?" != "0" ] || fail "opening an encrypted lake with a wrong key must fail"
assert_contains "$TMP/enc_wrongkey.log" "wrong key at"

# A keyed save onto a plaintext lake is refused, never silently rewritten.
{
    echo ".import $CSV t"
    echo ".lake save $ENC_PLAIN_CAT data $ENC_PLAIN"
    echo ".quit"
} | cli_run "$TMP/enc_plain_save.log"
assert_status_zero "$TMP/enc_plain_save.log" "$?"
{
    echo ".lake save $ENC_PLAIN_CAT data $ENC_PLAIN"
    echo ".quit"
} | cli_run_keyed "$TMP/enc_plain_keyed.log" "$ENC_KEY"
[ "$?" != "0" ] || fail "a keyed save onto a plaintext lake must fail"
assert_contains "$TMP/enc_plain_keyed.log" "not an encrypted pink-lake container"
[ -f "$TMP/enc-plain.pink" ] || [ -f "$TMP/enc-plain.pink.wal" ] \
    || fail "the plaintext lake's local catalog is missing"
ok "keyed open of a plaintext lake refused, and the plaintext lake is untouched"

fi # ENC_SUPPORTED

# --- concurrency (a): 3 writers x 3 rounds ---------------------------------
step "concurrency (a): 3 writers x 3 rounds on $CONC (one distinct table each)"

{ echo "CREATE TABLE warm (x INTEGER);"; echo "INSERT INTO warm VALUES (1);"; echo ".lake save $CONC_CAT data $CONC"; echo ".quit"; } \
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
            { echo ".import $TMP/writer$i.csv t$i"; echo ".lake save $CONC_CAT data $CONC"; echo ".quit"; } \
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

# The documented loser's conflict: the handle check at save start, the store's
# commit once the head moved, and the engine's write lease, all rendered as
# "write conflict at <catalog>: …" by the wave-103 catalog database.
CONFLICT_FORMS=("another writer published version" "the catalog head moved to" "write conflict on" "is busy")

python3 - "$TMP" "${CONFLICT_FORMS[@]}" <<'PY' || fail "the race did not behave as documented (see above)"
import glob, os, re, sys

tmp, conflict_forms = sys.argv[1], tuple(sys.argv[2:])
logs = sorted(glob.glob(os.path.join(tmp, "conc_r*_w*.log")))
if len(logs) != 9:
    print(f"expected 9 writer logs, found {len(logs)}", file=sys.stderr)
    sys.exit(1)

claims = {}          # version -> [log names that were told they published it]
conflicts = []
problems = []
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
    m = re.search(r"_w(\d+)\.log$", claims[final][0])
    if m:
        f.write(f"final_table=t{m.group(1)}\n")

print(f"  9 save attempts -> {total_claims} claim(s) of {len(versions)} version(s), {len(conflicts)} conflict(s)")
for v, names in sorted(claims.items()):
    print(f"    version {v}: claimed by {', '.join(names)}")
for name, message in conflicts:
    print(f"    conflict ({name}): {message[:110]}")
for v, names in duplicates.items():
    print(f"    DUPLICATE PUBLISH: version {v} claimed by {', '.join(names)}", file=sys.stderr)
if duplicates:
    print("two writers published one version: the catalog transaction must make that impossible", file=sys.stderr)
    sys.exit(1)
print(f"  ok: exactly one winner per version, no gaps (versions {versions[0]}..{final})")
print(f"  ok: every loser got the documented write conflict ({len(conflicts)} conflict(s))")
PY

FINAL="$(sed -n 's/^final=//p' "$TMP/conc.state")"
CLAIMS="$(sed -n 's/^claims=//p' "$TMP/conc.state")"
RACE_CONFLICTS="$(sed -n 's/^conflicts=//p' "$TMP/conc.state")"
DUPES="$(sed -n 's/^duplicates=//p' "$TMP/conc.state")"
RACE_FINAL_CLAIMANTS="$(sed -n 's/^final_claimants=//p' "$TMP/conc.state")"
RACE_FINAL_TABLE="$(sed -n 's/^final_table=//p' "$TMP/conc.state")"
RACE_FINAL="$FINAL"
[ -n "$RACE_FINAL_TABLE" ] || fail "the race state does not name the final claimant's table"
note "final race version: $FINAL ($CLAIMS claims, $RACE_CONFLICTS conflicts, $DUPES duplicate claim(s), $RACE_FINAL_CLAIMANTS claimant(s))"

# The head the race leaves behind is the final claimant's save: its one table
# and row count, read from the catalog database (no S3 metadata object).
{ echo ".lake info $CONC_CAT"; echo ".lake tables $CONC_CAT"; echo ".quit"; } \
    | cli_run "$TMP/conc_head.log"
assert_status_zero "$TMP/conc_head.log" "$?"
assert_contains "$TMP/conc_head.log" "current version: $FINAL"
assert_contains "$TMP/conc_head.log" "Lake $CONC"
assert_contains "$TMP/conc_head.log" "snapshots:       $FINAL ("
assert_contains "$TMP/conc_head.log" "main.$RACE_FINAL_TABLE ($WROWS rows)"
assert_contains "$TMP/conc_head.log" "tables (1):"
ok "catalog head: v$FINAL, one table (main.$RACE_FINAL_TABLE, $WROWS rows) from the final claimant"

# Reopen and continue (the documented recovery after a conflict): one fresh
# save settles the chain, and its full-snapshot write replaces the head's
# table set from that session's store.
step "settle save after the race (reopen and continue)"
{ echo ".lake info $CONC_CAT"; echo "CREATE TABLE settle (x INTEGER);"; echo "INSERT INTO settle VALUES (7);"; echo ".lake save $CONC_CAT data $CONC"; echo ".quit"; } \
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

{ echo ".lake versions $CONC_CAT"; echo ".lake info $CONC_CAT"; echo ".lake tables $CONC_CAT"; echo ".quit"; } \
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
assert_contains "$TMP/conc_metadata.log" "tables (1):"
# The settle save rewrote the whole lake from its session store, so the head
# holds exactly that one table — the catalog says so, not an S3 object.
assert_re "$TMP/conc_metadata.log" '^main\.settle \(1 row\)$'

# The split lake keeps its data under conc/ and nothing else but the
# `lake-data.json` ownership marker: no catalog, no snapshots, no commits.
CONC_KEYS="$(list_keys "conc/")" || fail "listing conc/ failed"
if printf '%s\n' "$CONC_KEYS" | grep -qE 'conc/(catalog\.json|snapshots/|commits/|highest\.json|lake\.json)'; then
    fail "catalog metadata must not be written to S3: $CONC_KEYS"
fi
printf '%s\n' "$CONC_KEYS" | grep -qxF "conc/lake-data.json" \
    || fail "the split lake's lake-data.json ownership marker is missing"
printf '%s\n' "$CONC_KEYS" | grep -qE '^conc/data/main\.[A-Za-z0-9_-]+/[0-9a-f-]+\.(pink|pinkdel)$' \
    || fail "conc data objects are missing"
DATA_COUNT="$(printf '%s\n' "$CONC_KEYS" | grep -cE '^conc/data/')"
assert_listing_count "$CONC_KEYS" "conc/" "$((1 + DATA_COUNT))"
ok "conc/: $DATA_COUNT data object(s) + the lake-data.json marker, no catalog metadata"

# `.lake files` lists the head manifest's files; `all` adds every `data/`
# object **no retained version references** — the same scan `.lake orphans`
# deletes. So the head run must name one file that exists in S3, and the two
# views must agree on the leftovers: the catalog database decides what
# belongs to the lake, not an S3 metadata object.
printf '%s\n' "$CONC_KEYS" | grep -E '^conc/data/' | sed 's|^conc/||' >"$TMP/conc_objects.txt"
{ echo ".lake files $CONC_CAT"; echo ".quit"; } | cli_run "$TMP/conc_files.log"
assert_status_zero "$TMP/conc_files.log" "$?"
assert_re "$TMP/conc_files.log" '^data/main\.settle/[0-9a-f-]+\.(pink|parquet): table=main\.settle rows=1 '
{ echo ".lake files $CONC_CAT all"; echo ".quit"; } | cli_run "$TMP/conc_files_all.log"
assert_status_zero "$TMP/conc_files_all.log" "$?"
python3 - "$TMP/conc_files.log" "$TMP/conc_files_all.log" "$TMP/conc_objects.txt" "$FINAL" "$TMP/conc.leftover" <<'PY' || fail "the catalog's files and the S3 objects disagree"
import re, sys

head_text = open(sys.argv[1], encoding="utf-8").read()
all_text = open(sys.argv[2], encoding="utf-8").read()
objects = {l.strip() for l in open(sys.argv[3], encoding="utf-8") if l.strip()}
final = int(sys.argv[4])

line_re = re.compile(r"^(data/\S+): (.*)$")
head = []
for line in head_text.splitlines():
    m = line_re.match(line)
    if m:
        head.append((m.group(1), m.group(2)))
listed = {}
summary = None
for line in all_text.splitlines():
    m = line_re.match(line)
    if m:
        listed[m.group(1)] = "unreferenced" in m.group(2)
        continue
    m = re.match(r"^(\d+) files? \((\d+) referenced, (\d+) unreferenced\)$", line)
    if m:
        summary = tuple(int(x) for x in m.groups())

problems = []
if len(head) != 1:
    problems.append(f"the head lists {len(head)} file(s), expected the settle save's one")
for path, fields in head:
    if path not in objects:
        problems.append(f"the head file {path} is not in the bucket")
    if "table=main.settle" not in fields or "rows=1" not in fields:
        problems.append(f"the head file line does not say table=main.settle rows=1: {path}: {fields}")
if summary is None:
    problems.append("no `.lake files all` summary line found")
else:
    total, referenced, unreferenced = summary
    if total != len(listed):
        problems.append(f"summary counts {total} files, listed {len(listed)}")
    if referenced != 1:
        problems.append(f"{referenced} files are referenced at the head, expected 1")
    if unreferenced != sum(1 for p in listed.values() if p):
        problems.append(f"summary says {unreferenced} unreferenced, listed {sum(1 for p in listed.values() if p)}")
    if sum(1 for p in listed.values() if not p) != 1:
        problems.append("`all` must mark exactly the head's file as referenced")
    claimed = {p for p, unref in listed.items() if not unref}
    if not claimed <= objects:
        problems.append(f"referenced but missing from the bucket: {sorted(claimed - objects)[:4]}")
    if len(objects) != (final - 1) + unreferenced:
        problems.append(f"the bucket holds {len(objects)} object(s), expected {final - 1} retained file(s) + {unreferenced} leftover(s)")
if problems:
    print("`.lake files` vs the conc/ listing:", file=sys.stderr)
    for p in problems:
        print("  " + p, file=sys.stderr)
    sys.exit(1)
open(sys.argv[5], "w", encoding="utf-8").write(f"{unreferenced}\n")
print(f"  ok: the head names its one data file; {len(objects)} object(s) = {final - 1} retained + {unreferenced} leftover")
PY
CONC_ORPHANS="$(cat "$TMP/conc.leftover")"

# Orphans cleanup: `all` skips the age gate. The dry-run must list exactly the
# leftovers `.lake files all` reported, the confirm deletes exactly those, and
# a second dry-run proves that cleanup is idempotent. The catalog's versions
# and head file must survive both.
{ echo ".lake orphans $CONC_CAT all dry-run"; echo ".quit"; } | cli_run "$TMP/conc_orphans.log"
assert_status_zero "$TMP/conc_orphans.log" "$?"
CONC_LEFTOVER="$(grep -c '^would delete ' "$TMP/conc_orphans.log")"
[ "$CONC_LEFTOVER" = "$CONC_ORPHANS" ] \
    || fail ".lake files all saw $CONC_ORPHANS leftover(s), the orphan dry-run would delete $CONC_LEFTOVER"
if [ "$CONC_LEFTOVER" != "0" ]; then
    { echo ".lake orphans $CONC_CAT all confirm"; echo ".quit"; } | cli_run "$TMP/conc_orphans_confirm.log"
    assert_status_zero "$TMP/conc_orphans_confirm.log" "$?"
    assert_re "$TMP/conc_orphans_confirm.log" "^Deleted $CONC_LEFTOVER objects? from "
    ok "deleted the $CONC_LEFTOVER leftover data object(s)"
else
    assert_contains "$TMP/conc_orphans.log" "Nothing to orphans"
    ok "no leftover from the losing writers"
fi
{ echo ".lake orphans $CONC_CAT all dry-run"; echo ".quit"; } | cli_run "$TMP/conc_orphans_after.log"
assert_status_zero "$TMP/conc_orphans_after.log" "$?"
assert_contains "$TMP/conc_orphans_after.log" "Nothing to orphans"
ok "a second dry-run finds no orphan (cleanup is exact and idempotent)"

CONC_KEYS="$(list_keys "conc/")" || fail "listing conc/ after cleanup failed"
DATA_COUNT="$(printf '%s\n' "$CONC_KEYS" | grep -cE '^conc/data/')"
[ "$DATA_COUNT" = "$((FINAL - 1))" ] \
    || fail "after cleanup conc/data has $DATA_COUNT object(s); every version above the empty v1 owns exactly one"
assert_listing_count "$CONC_KEYS" "conc/" "$((1 + FINAL - 1))"
printf '%s\n' "$CONC_KEYS" | grep -E '^conc/data/' | sed 's|^conc/||' >"$TMP/conc_objects.txt"
{ echo ".lake versions $CONC_CAT"; echo ".lake files $CONC_CAT all"; echo ".quit"; } | cli_run "$TMP/conc_after.log"
assert_status_zero "$TMP/conc_after.log" "$?"
python3 - "$TMP/conc_after.log" "$TMP/conc_objects.txt" "$FINAL" <<'PY' || fail "cleanup disturbed the catalog"
import re, sys

text = open(sys.argv[1], encoding="utf-8").read()
objects = {l.strip() for l in open(sys.argv[2], encoding="utf-8") if l.strip()}
final = int(sys.argv[3])
versions = [int(m.group(1)) for m in re.finditer(r"^(\d+)( \(current\))?$", text, re.M)]
listed = {m.group(1) for m in re.finditer(r"^(data/\S+): ", text, re.M)}
summary = re.search(r"^(\d+) files? \((\d+) referenced, (\d+) unreferenced\)$", text, re.M)
problems = []
if versions != list(range(1, final + 1)):
    problems.append(f".lake versions listed {versions}, expected 1..{final}")
if f"{final} (current)" not in text:
    problems.append(f".lake versions did not mark {final} as current")
if len(objects) != final - 1:
    problems.append(f"the bucket holds {len(objects)} data object(s), expected {final - 1}")
if summary is None:
    problems.append("no `.lake files all` summary line found")
else:
    total, referenced, unreferenced = (int(x) for x in summary.groups())
    if (total, referenced, unreferenced) != (1, 1, 0):
        problems.append(f"after cleanup expected 1 file (1 referenced, 0 unreferenced), got {total}/{referenced}/{unreferenced}")
if not listed <= objects:
    problems.append(f"listed but missing from the bucket: {sorted(listed - objects)[:4]}")
if problems:
    print("after cleanup:", file=sys.stderr)
    for p in problems:
        print("  " + p, file=sys.stderr)
    sys.exit(1)
print(f"  ok: cleanup left versions 1..{final} and their {final - 1} data files intact")
PY

# --- concurrency (b): a slow save deterministically loses one version ------
step "concurrency (b): slow save vs quick save for the same next version on $CONFLICT"
note "the slow writer opens the lake at v2 and encodes $SLOWROWS rows; the quick"
note "writer publishes v3 while it is busy. Exactly one of them may win v3: the"
note "catalog transaction rejects the other one's commit with the moved-head"
note "conflict (deterministic once the quick writer's commit lands first)."
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

{ echo "CREATE TABLE warm (x INTEGER);"; echo "INSERT INTO warm VALUES (1);"; echo ".lake save $CONFLICT_CAT data $CONFLICT"; echo ".quit"; } \
    | cli_run "$TMP/conflict_seed.log"
assert_status_zero "$TMP/conflict_seed.log" "$?"
assert_contains "$TMP/conflict_seed.log" "Saved lake version 2"

(
    { echo ".import $TMP/slow.csv tslow"; echo ".lake save $CONFLICT_CAT data $CONFLICT"; echo ".quit"; } \
        | cli_run "$TMP/conflict_slow.log"
    echo "$?" >"$TMP/conflict_slow.exit"
) &
SLOW_PID=$!
# Head start: long enough that the slow writer has read the catalog (v2) and
# is inside its encode/upload when the quick writer reads it too, short
# enough that the slow writer has not published v3 yet. With SLOWROWS sized
# to ~1s of encoding the window is wide on any local server.
sleep 0.3
{ echo ".import $TMP/quick.csv tquick"; echo ".lake save $CONFLICT_CAT data $CONFLICT"; echo ".quit"; } \
    | cli_run "$TMP/conflict_quick.log"
assert_status_zero "$TMP/conflict_quick.log" "$?"
wait "$SLOW_PID"
# The slow writer is the expected loser. Since wave 40b the CLI exits 1 after
# a failed statement in a piped script, so a nonzero exit is correct here; the
# python block below classifies its log (exactly one error: the documented
# write conflict). The exit file is kept for diagnostics only.

python3 - "$TMP" "${CONFLICT_FORMS[@]}" <<'PY' || fail "the slow-save conflict did not behave as documented (see above)"
import os, re, sys

tmp, conflict_forms = sys.argv[1], tuple(sys.argv[2:])
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
if "write conflict at" not in message or not any(p in message for p in conflict_forms):
    print(f"{loser}: unexpected conflict message: {message}", file=sys.stderr)
    sys.exit(1)
with open(os.path.join(tmp, "conflict.state"), "w", encoding="utf-8") as f:
    f.write(f"winner={winner}\nloser={loser}\n")
print(f"  ok: exactly one winner of version 3: the {winner} save")
print(f"  ok: the {loser} save got the documented conflict: {message[:110]}")
PY
WINNER="$(sed -n 's/^winner=//p' "$TMP/conflict.state")"

CONFLICT_KEYS="$(list_keys "conflict/")" || fail "listing conflict/ failed"
if printf '%s\n' "$CONFLICT_KEYS" | grep -qE 'conflict/(catalog\.json|snapshots/|commits/|highest\.json|lake\.json)'; then
    fail "catalog metadata must not be written to S3: $CONFLICT_KEYS"
fi
printf '%s\n' "$CONFLICT_KEYS" | grep -qE '^conflict/data/main\.(tslow|tquick)/[0-9a-f-]+\.(pink|pinkdel)$' \
    || fail "the conflict run's data objects are missing"
ok "conflict/: data objects only, no catalog metadata (a loser's upload, if any, is a documented orphan)"

{ echo ".lake versions $CONFLICT_CAT"; echo ".lake info $CONFLICT_CAT"; echo ".lake tables $CONFLICT_CAT"; echo ".lake open $CONFLICT_CAT"; echo ".quit"; } \
    | cli_run "$TMP/conflict_state.log"
assert_status_zero "$TMP/conflict_state.log" "$?"
assert_re "$TMP/conflict_state.log" '^3 \(current\)$'
assert_no_match "$TMP/conflict_state.log" '^4'
assert_contains "$TMP/conflict_state.log" "snapshots:       3 (1, 2, 3)"
assert_re "$TMP/conflict_state.log" '^main\.t'"$WINNER"' \([0-9]+ rows\)$'
assert_contains "$TMP/conflict_state.log" "(1 table) from $CONFLICT_CAT"

# --- refusal ---------------------------------------------------------------
step "refusal: an s3:// URL is a data location, not a lake"
note "since wave 103 a lake's catalog is a database (pink:<path>, sqlite:,"
note "postgres://); an s3:// location alone is refused with a pointer to the"
note "split form. A second .lake save in the same session reopens the catalog"
note "and publishes the next version (full-snapshot semantics)."

{ echo ".lake open $REFUSE"; echo ".lake save $REFUSE"; echo ".quit"; } | cli_run "$TMP/refuse_s3.log"
[ "$?" != "0" ] || fail "opening a lake by an s3:// location alone must fail"
assert_contains "$TMP/refuse_s3.log" "is a data location, not a lake catalog"
assert_contains "$TMP/refuse_s3.log" ".lake save pink:lake.pink data $REFUSE"
ok "s3:// alone is refused as a lake (both .lake open and .lake save)"

{
    echo ".import $CSV t"
    echo ".lake save $REFUSE_CAT data $REFUSE"
    echo ".lake save $REFUSE_CAT data $REFUSE"
    echo ".lake versions $REFUSE_CAT"
    echo ".quit"
} | cli_run "$TMP/refuse_twice.log"
assert_status_zero "$TMP/refuse_twice.log" "$?"
assert_re "$TMP/refuse_twice.log" '^Saved lake version 2 in '
assert_re "$TMP/refuse_twice.log" '^Saved lake version 3 in '
assert_re "$TMP/refuse_twice.log" '^2$'
assert_re "$TMP/refuse_twice.log" '^3 \(current\)$'
ok "a second .lake save in the same session published version 3 (no stale view)"

{ echo ".lake versions $REFUSE_CAT"; echo ".quit"; } | cli_run "$TMP/refuse_versions.log"
assert_status_zero "$TMP/refuse_versions.log" "$?"
assert_re "$TMP/refuse_versions.log" '^3 \(current\)$'
assert_no_match "$TMP/refuse_versions.log" '^4'
ok "the refused s3:// attempt published nothing (.lake versions stays 1..3)"

{ echo ".lake save $REFUSE_CAT data $REFUSE"; echo ".quit"; } | cli_run "$TMP/refuse_retry.log"
assert_status_zero "$TMP/refuse_retry.log" "$?"
assert_re "$TMP/refuse_retry.log" '^Saved lake version 4 in '
ok "the split form still publishes: version 4"

REFUSE_KEYS="$(list_keys "refuse/")" || fail "listing refuse/ failed"
if printf '%s\n' "$REFUSE_KEYS" | grep -qE 'refuse/(catalog\.json|snapshots/|commits/|highest\.json|lake\.json)'; then
    fail "catalog metadata must not be written to S3: $REFUSE_KEYS"
fi
printf '%s\n' "$REFUSE_KEYS" | grep -qxF "refuse/lake-data.json" \
    || fail "the refuse/ ownership marker is missing"
ok "refuse/: data + the lake-data.json marker only"

# --- summary ---------------------------------------------------------------
step "summary"
printf '  server:        %s (container %s at %s)\n' "$SERVER" "$CONTAINER" "$ENDPOINT"
if [ "$SIG_VERIFIED" = "1" ]; then
    printf '  signatures:    SigV4 enforced — unsigned PUT -> 403 (asserted)\n'
else
    printf '  signatures:    not verified by this endpoint (unsigned accepted; reported, not asserted — see s3_docker_notes.md)\n'
fi
if [ "$STRICT" = "1" ]; then
    printf '  enforcement:   STRICT — SigV4 asserted; catalog/race assertions are hard failures everywhere\n'
else
    printf '  enforcement:   lenient — SigV4 reported, not asserted; catalog/race assertions are hard failures everywhere\n'
fi
printf '  roundtrip:     version 2, 3 rows round-tripped through a fresh process; 2 objects on S3\n'
printf '                 (the lake-data.json owner marker + one data object)\n'
printf '  concurrency:   race: 9 attempts, %s claims, %s conflict(s), %s duplicate claim(s);\n' \
    "$CLAIMS" "$RACE_CONFLICTS" "$DUPES"
printf '                 versions 1..%s gapless; head = final claimant table; settle save -> v%s\n' \
    "$RACE_FINAL" "$FINAL"
printf '                 slow-vs-quick: exactly one winner of v3 (the %s save), loser conflicted\n' "$WINNER"
printf '                 conc/data: %s retained file(s); %s leftover(s) from losing writers cleaned\n' \
    "$((FINAL - 1))" "$CONC_LEFTOVER"
printf '  refusal:       s3:// alone refused (not a catalog); second save -> v3; split form -> v4\n'
if [ "$ENC_SUPPORTED" = "1" ]; then
    printf '  encrypted:     PINKDB_KEY save/commit/open/attach round trip; ciphertext-only objects;\n'
    printf '                 sealed catalog; no/wrong key refused; a plaintext lake is never rewritten\n'
else
    printf '  encrypted:     SKIPPED (this CLI build has no `encryption` feature)\n'
fi
printf '\nALL CHECKS PASSED\n'
