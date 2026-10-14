#!/usr/bin/env bash
# ============================================================================
# pink-lake real-S3 integration test: adobe/s3mock in Docker, release CLI.
# ============================================================================
#
# The in-repo `mock_s3.rs` (used by `cargo test -p pink-lake --features s3`
# and the CLI's `lake_s3` tests) is the hermetic protocol suite — it even
# verifies SigV4 on every request — but it is a home-grown server inside the
# test process. This script is the independent check: a real, separate HTTP
# server in a container, speaking real S3 semantics over a socket, driven
# through the release CLI just like a user would.
#
# It needs Docker, python3 and curl (plus cargo for the default CLI build):
#   * starts its own s3mock container (`pinkdb-s3mock-itest`, initial bucket
#     `pinklake`) on 127.0.0.1; an already-running container of that name is
#     reused (`KEEP=1` leaves one behind on exit, the next run picks it up),
#   * waits for a PUT/GET readiness probe,
#   * checks the server implements conditional writes (`If-None-Match: *` /
#     `If-Match`, the single-writer contract), so the safe (non-degraded)
#     client path is exercised, not the opt-in fallback,
#   * probes whether those conditionals are ATOMIC under true concurrency
#     (two racing PUTs to a fresh key: an atomic server lets exactly one
#     win). s3mock's PutObject precondition check runs *outside* its
#     per-object lock, so it is check-then-act: both racers can get 200. The
#     harness reports that as the server quirk it is (see
#     `s3_docker_notes.md`) and enforces the one-winner-per-version
#     assertion only on servers that can uphold it (AWS S3 / R2 — use
#     `PINKDB_S3_LIVE=1` for the real-service run; the in-repo mock verifies
#     the client logic against atomic conditionals),
#   * roundtrip: a typed CSV (NULL, `-0.0`, bigint, DATE/TIMESTAMP, multibyte)
#     imported and saved from one CLI process, then reopened and SELECTed in a
#     fresh process; exact values are compared by python3, and the published
#     objects are listed over the unsigned REST API (`?list-type=2`),
#   * concurrency: (a) 3 writer processes x 3 rounds against one prefix; every
#     attempt must win or get the documented `write conflict`, the published
#     version chain must be gapless, and a settle save afterwards pins the
#     prefix's final version to one claimant (the documented recovery: reopen
#     and continue), so the final-version byte check is enforced; (b) a
#     deterministic slow-save vs quick-save pair racing for the *same* next
#     version — exactly one winner there, and the loser's conflict asserted;
#     `catalog.json` is byte-identical to `snapshots/<v>.json` for the
#     single-claimant versions (fetched with curl, `cmp`),
#   * refusal: a second `.lake save` (the CLI opens a fresh handle per save,
#     so it publishes a new version — pinned explicitly) and the
#     snapshot-exists refusal on a planted stray `snapshots/<v>.json`
#     (removed afterwards; the next save then succeeds).
#
# MinIO note: MinIO images are currently NOT anonymously pullable (Docker Hub
# answers "pull access denied ... may require 'docker login'", quay.io answers
# 401 UNAUTHORIZED), so s3mock is the documented recipe. Anything
# S3-compatible works otherwise; for a real AWS/R2 bucket use the opt-in
# `PINKDB_S3_LIVE=1` test (README "Object storage").
#
# Usage:
#   crates/pink-lake/tests/s3_docker.sh
#
# Environment knobs:
#   PORT=9090            host port; a free one is auto-picked if 9090 is busy
#   CONTAINER=pinkdb-s3mock-itest   container name
#   BUCKET=pinklake                 initial bucket
#   KEEP=1               leave the container running on exit (reused next run)
#                        and keep the run's temp dir (raw CLI logs + CSVs)
#   PINKDB_BIN=...       use this CLI binary (default: build lake,s3 release)
#   SKIP_BUILD=1         do not run cargo build (use target/release/pinkdb)
#   WROWS=20000          rows per racing writer's table
#   SLOWROWS=400000      rows of the slow save in the deterministic conflict
#   QUICKROWS=1000       rows of the quick save that wins that version
#
# Every failure prints the failing step and the raw output and exits non-zero.
# The container is removed on exit unless KEEP=1. `cargo test` is untouched:
# this script is not wired into the test suite (it needs Docker).

set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

IMAGE="adobe/s3mock:latest"
CONTAINER="${CONTAINER:-pinkdb-s3mock-itest}"
BUCKET="${BUCKET:-pinklake}"
KEEP="${KEEP:-0}"
WROWS="${WROWS:-20000}"
SLOWROWS="${SLOWROWS:-400000}"
QUICKROWS="${QUICKROWS:-1000}"
PORT="${PORT:-}"
REGION="us-east-1"
ACCESS_KEY="pinkdb-itest"
SECRET_KEY="pinkdb-itest-secret"

TMP="$(mktemp -d "${TMPDIR:-/tmp}/pinkdb_s3_docker.XXXXXX")" || exit 1
CURRENT_LOG=""
STEP=""
ENDPOINT=""
ATOMIC=1

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
    if docker inspect "$CONTAINER" >/dev/null 2>&1; then
        printf -- '--- last container log lines ---\n' >&2
        docker logs --tail 20 "$CONTAINER" >&2 2>&1 || true
        printf -- '--- (end container log) ---\n' >&2
    fi
    exit 1
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    if docker inspect "$CONTAINER" >/dev/null 2>&1; then
        if [ "$KEEP" = "1" ]; then
            printf '\nkept container %s (KEEP=1) on port %s' "$CONTAINER" "${PORT:-?}"
            printf ' — remove it with: docker rm -f %s\n' "$CONTAINER"
            printf 'kept run directory %s (raw CLI logs + generated CSVs, KEEP=1)\n' "$TMP"
        else
            docker rm -f "$CONTAINER" >/dev/null 2>&1 \
                && printf '\nremoved container %s\n' "$CONTAINER"
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

# Run the CLI with exactly the s3mock environment (the AWS variables are
# pinned so a developer's real credentials can never leak into s3mock), stdin
# from the caller, combined output into <log>. Returns the CLI's exit status.
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
# REST helpers (all unsigned: s3mock accepts them; the CLI itself signs)
# ---------------------------------------------------------------------------

# Unsigned `?list-type=2` GET, one key per line (≤1000 keys — plenty here; a
# full prefix is refused below rather than silently truncated).
list_keys() {
    local enc body
    enc="$(python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.argv[1]))' "$1")" || return 1
    body="$(curl -fsS --max-time 30 "$ENDPOINT/$BUCKET/?list-type=2&prefix=$enc&max-keys=1000")" || return 1
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
        curl -fsS --max-time 30 -X DELETE "$ENDPOINT/$BUCKET/$k" -o /dev/null \
            || fail "unsigned DELETE of $k failed"
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

start_or_reuse_container() {
    local state image mapped
    if docker inspect "$CONTAINER" >/dev/null 2>&1; then
        image="$(docker inspect -f '{{.Config.Image}}' "$CONTAINER" 2>/dev/null || true)"
        [ "$image" = "$IMAGE" ] \
            || fail "container $CONTAINER already exists but runs \"$image\", not $IMAGE; remove it first (docker rm -f $CONTAINER)"
        state="$(docker inspect -f '{{.State.Status}}' "$CONTAINER" 2>/dev/null || true)"
        if [ "$state" != "running" ]; then
            note "container $CONTAINER exists but is $state; starting it (reuse)"
            docker start "$CONTAINER" >/dev/null || fail "docker start $CONTAINER failed"
        else
            note "reusing running container $CONTAINER (left by an earlier run, KEEP=1)"
        fi
        mapped="$(docker port "$CONTAINER" 9090/tcp 2>/dev/null | sed -n 's/.*:\([0-9][0-9]*\)$/\1/p' | head -1)"
        [ -n "$mapped" ] || fail "container $CONTAINER has no host mapping for 9090/tcp"
        if [ -n "$PORT" ] && [ "$PORT" != "$mapped" ]; then
            fail "PORT=$PORT was requested but the running container is mapped to $mapped; remove it (docker rm -f $CONTAINER) or unset PORT"
        fi
        PORT="$mapped"
        ok "reuse: container $CONTAINER is running on 127.0.0.1:$PORT"
        return 0
    fi

    # Fresh container: pick a port (explicit PORT must be free; otherwise the
    # default 9090, or a free port — clearly logged — when 9090 is taken).
    if [ -n "$PORT" ]; then
        port_free "$PORT" || fail "PORT=$PORT is already in use on 127.0.0.1; choose another port or unset PORT (nothing was started)"
    else
        if port_free 9090; then
            PORT=9090
        else
            PORT="$(free_port)"
            note "port 9090 is busy (another container?); using free port $PORT instead"
        fi
    fi

    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        note "image $IMAGE is not local; pulling it (MinIO images are not anonymously pullable right now — see the README)"
        docker pull "$IMAGE" >/dev/null || fail "docker pull $IMAGE failed"
        ok "pulled $IMAGE"
    fi

    note "creating container $CONTAINER on 127.0.0.1:$PORT (bucket $BUCKET)"
    docker run -d --name "$CONTAINER" \
        --label pinkdb-s3-itest=1 \
        -p "127.0.0.1:${PORT}:9090" \
        -e COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS="$BUCKET" \
        "$IMAGE" >/dev/null || fail "docker run $IMAGE failed"
    ok "container $CONTAINER started"
}

wait_ready() {
    local tries=0 probe body
    probe="itest-readiness-probe"
    while [ "$tries" -lt 120 ]; do
        # CreateBucket when the initial-bucket env did not stick (reused
        # container); 200/409/405 all mean "the endpoint answered".
        curl -sS --max-time 5 -X PUT "$ENDPOINT/$BUCKET" -o /dev/null 2>/dev/null || true
        if body="$(curl -fsS --max-time 5 "$ENDPOINT/$BUCKET/$probe" 2>/dev/null)"; then
            if [ "$body" = "ready" ]; then
                curl -sS --max-time 5 -X DELETE "$ENDPOINT/$BUCKET/$probe" -o /dev/null 2>/dev/null || true
                ok "s3mock answered the readiness GET ($tries retries)"
                return 0
            fi
        fi
        printf 'ready' | curl -fsS --max-time 5 -X PUT --data-binary @- "$ENDPOINT/$BUCKET/$probe" -o /dev/null 2>/dev/null || true
        tries=$((tries + 1))
        sleep 0.5
    done
    fail "s3mock did not become ready at $ENDPOINT within 60s"
}

# s3mock must enforce the conditional writes the single-writer contract needs
# (the client refuses a degraded endpoint by default; this proves the safe
# path is the one being tested).
check_conditional_writes() {
    local key="itest-conditional-probe" code etag headers
    headers="$(curl -sS --max-time 30 -D - -o /dev/null -X PUT --data-binary 'first' "$ENDPOINT/$BUCKET/$key")" \
        || fail "conditional probe PUT failed"
    etag="$(printf '%s' "$headers" | tr -d '\r' | awk 'tolower($1) == "etag:" { print $2; exit }')"
    [ -n "$etag" ] || fail "conditional probe: the server returned no ETag"

    code="$(curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H 'If-None-Match: *' --data-binary 'second' "$ENDPOINT/$BUCKET/$key")"
    [ "$code" = "412" ] || fail "If-None-Match: * on an existing key returned $code, expected 412 (conditional writes not enforced?)"

    code="$(curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H "If-Match: $etag" --data-binary 'third' "$ENDPOINT/$BUCKET/$key")"
    [ "$code" = "200" ] || fail "If-Match with the current ETag returned $code, expected 200"

    code="$(curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H 'If-Match: "deadbeefdeadbeef"' --data-binary 'fourth' "$ENDPOINT/$BUCKET/$key")"
    [ "$code" = "412" ] || fail "If-Match with a wrong ETag returned $code, expected 412"

    curl -fsS --max-time 30 -X DELETE "$ENDPOINT/$BUCKET/$key" -o /dev/null || fail "conditional probe DELETE failed"
    ok "server enforces If-None-Match: * (412) and If-Match (200 / 412) — checked sequentially"
}

# Is the server's conditional evaluation atomic with the write? Two racing
# conditional PUTs to one fresh key must leave exactly one 200 for a
# concurrent-safe endpoint (AWS S3 / Cloudflare R2 evaluate the precondition
# as part of the write). s3mock's PutObject checks the precondition outside
# its per-object lock (check-then-act), so both racers can be told 200; the
# harness reports that and does not pretend the one-winner assertion ran.
probe_conditional_atomicity() {
    local trials=4 t a b both=0 create_dup=0 replace_dup=0 key
    for t in 1 2 3 4; do
        key="itest-atomic-probe-$t"
        (
            curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H 'If-None-Match: *' \
                --data-binary "racer-a-$t" "$ENDPOINT/$BUCKET/$key" >"$TMP/atomic_a.$t"
        ) &
        (
            curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H 'If-None-Match: *' \
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
        curl -fsS --max-time 30 -o /dev/null -X PUT --data-binary 'base' "$ENDPOINT/$BUCKET/$key" || fail "atomic probe PUT failed"
        a="$(curl -sS --max-time 30 -D - -o /dev/null -X PUT --data-binary 'base2' "$ENDPOINT/$BUCKET/$key" | tr -d '\r' | awk 'tolower($1) == "etag:" { print $2; exit }')"
        [ -n "$a" ] || fail "atomic probe: no ETag"
        (
            curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H "If-Match: $a" \
                --data-binary "repl-a-$t" "$ENDPOINT/$BUCKET/$key" >"$TMP/atomic_c.$t"
        ) &
        (
            curl -sS --max-time 30 -o /dev/null -w '%{http_code}' -X PUT -H "If-Match: $a" \
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
        curl -sS --max-time 30 -X DELETE "$ENDPOINT/$BUCKET/$key" -o /dev/null || true
    done

    if [ "$both" = "0" ] && [ "$replace_dup" = "0" ]; then
        ATOMIC=1
        ok "conditional PUTs are atomic under concurrency (4+4 racing trials, one winner each)"
    else
        ATOMIC=0
        warn "conditional PUTs are NOT atomic under concurrency on this server"
        warn "  If-None-Match races with both PUTs answered 200: $both/4"
        warn "  If-Match races with both PUTs answered 200:      $replace_dup/4"
        warn "  (s3mock checks the precondition outside the store lock: adobe/S3Mock"
        warn "   ObjectController.putObject -> verifyObjectMatching, then putObject)"
        warn "  the race's one-winner-per-version assertion is REPORTED here, not enforced;"
        warn "  use AWS S3/R2 (PINKDB_S3_LIVE=1) or the in-repo atomic mock for the enforced check"
    fi
}

# ===========================================================================
# main
# ===========================================================================

printf 'pink-lake real-S3 integration test (s3mock in Docker)\n'
printf 'repo: %s\n' "$ROOT"

# --- CLI binary ------------------------------------------------------------
if [ -n "${PINKDB_BIN:-}" ]; then
    BIN="$PINKDB_BIN"
    [ -x "$BIN" ] || fail "PINKDB_BIN=$BIN is not executable"
    if ! printf '.lake\n.quit\n' | "$BIN" 2>/dev/null | grep -qF 'lake save <dir|s3://bucket/prefix>'; then
        fail "PINKDB_BIN=$BIN lacks the lake+s3 features (build with --features lake,s3)"
    fi
    note "using PINKDB_BIN=$BIN"
elif [ "${SKIP_BUILD:-0}" = "1" ]; then
    BIN="$ROOT/target/release/pinkdb"
    [ -x "$BIN" ] || fail "SKIP_BUILD=1 but $BIN does not exist"
    note "SKIP_BUILD=1: using $BIN"
else
    step "build the CLI (cargo build --release -p pinkdb-cli --features lake,s3)"
    (cd "$ROOT" && cargo build --release -p pinkdb-cli --features lake,s3) \
        || fail "cargo build --release -p pinkdb-cli --features lake,s3 failed"
    BIN="$ROOT/target/release/pinkdb"
    ok "built $BIN"
fi

# --- container -------------------------------------------------------------
step "s3mock container"
start_or_reuse_container
ENDPOINT="http://127.0.0.1:$PORT"
note "endpoint: $ENDPOINT (bucket $BUCKET)"
wait_ready
check_conditional_writes
probe_conditional_atomicity

# --- clean slate for the run's prefixes ------------------------------------
step "clean slate (unsigned list + delete of the run's prefixes)"
for p in "itest/" "conc/" "conflict/" "refuse/"; do
    wipe_prefix "$p"
done

ITEST="s3://$BUCKET/itest"
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
assert_contains "$TMP/roundtrip_save.log" "format version:  2"
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

# Published objects, over the unsigned REST API.
KEYS="$(list_keys "itest/")" || fail "listing itest/ failed"
printf '%s\n' "$KEYS" | grep -qxF "itest/catalog.json" || fail "itest/catalog.json is missing"
printf '%s\n' "$KEYS" | grep -qxF "itest/snapshots/1.json" || fail "itest/snapshots/1.json is missing"
printf '%s\n' "$KEYS" | grep -qxF "itest/snapshots/2.json" || fail "itest/snapshots/2.json is missing"
printf '%s\n' "$KEYS" | grep -qE '^itest/data/main\.t/[0-9a-f-]+\.vortex$' \
    || fail "the roundtrip's Vortex data object is missing"
assert_listing_count "$KEYS" "itest/" 4
note "keys: $(printf '%s ' $KEYS)"

# catalog.json == snapshots/<version>.json (bytes), fetched unsigned + cmp.
curl -fsS --max-time 30 -o "$TMP/rt_catalog.json" "$ENDPOINT/$BUCKET/itest/catalog.json" || fail "fetching catalog.json failed"
curl -fsS --max-time 30 -o "$TMP/rt_snapshot.json" "$ENDPOINT/$BUCKET/itest/snapshots/2.json" || fail "fetching snapshots/2.json failed"
cmp -s "$TMP/rt_catalog.json" "$TMP/rt_snapshot.json" \
    || fail "catalog.json is not byte-identical to snapshots/2.json"
ok "catalog.json == snapshots/2.json (bytes)"

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

for f in "$TMP"/conc_r*_w*.exit; do
    assert_status_zero "$f" "$(cat "$f")"
done
for f in "$TMP"/conc_r*_w*.log; do
    assert_contains "$f" "Imported $WROWS rows"
done

python3 - "$TMP" "$ATOMIC" <<'PY' || fail "the race did not behave as documented (see above)"
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
    if len(saved) == 1 and not errors:
        claims.setdefault(saved[0], []).append(name)
    elif not saved and len(errors) == 1:
        message = errors[0]
        if "write conflict at" not in message or not any(p in message for p in conflict_forms):
            problems.append(f"{name}: save failed without the documented conflict: {message}")
        else:
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

# Reopen and continue (the documented recovery after a conflict): one fresh
# save settles the chain. Its version has exactly one claimant by
# construction, so the catalog/snapshot byte-equality below is enforced even
# on a server with non-atomic conditionals.
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
curl -fsS --max-time 30 -o "$TMP/conc_catalog.json" "$ENDPOINT/$BUCKET/conc/catalog.json" || fail "fetching conc/catalog.json failed"
curl -fsS --max-time 30 -o "$TMP/conc_snapshot.json" "$ENDPOINT/$BUCKET/conc/snapshots/$FINAL.json" \
    || fail "fetching conc/snapshots/$FINAL.json failed"
V_JSON="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["version"])' "$TMP/conc_catalog.json")" \
    || fail "conc/catalog.json does not parse as JSON"
[ "$V_JSON" = "$FINAL" ] || fail "catalog.json declares version $V_JSON, expected $FINAL"
cmp -s "$TMP/conc_catalog.json" "$TMP/conc_snapshot.json" \
    || fail "conc: catalog.json is not byte-identical to snapshots/$FINAL.json"
ok "catalog.json == snapshots/$FINAL.json (bytes, version field $V_JSON, single claimant)"

CONC_KEYS="$(list_keys "conc/")" || fail "listing conc/ failed"
for v in $(seq 1 "$FINAL"); do
    printf '%s\n' "$CONC_KEYS" | grep -qxF "conc/snapshots/$v.json" \
        || fail "conc/snapshots/$v.json is missing"
done
ok "conc/snapshots/1..$FINAL all exist"
DATA_COUNT="$(printf '%s\n' "$CONC_KEYS" | grep -cE '^conc/data/')"
printf '%s\n' "$CONC_KEYS" | grep -qE '^conc/data/main\.[A-Za-z0-9_-]+/[0-9a-f-]+\.vortex$' \
    || fail "conc data objects are missing"
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
assert_listing_count "$CONC_KEYS" "conc/" "$((1 + FINAL + DATA_COUNT))"
ok "conc/data: $DATA_COUNT object(s), $((DATA_COUNT - (FINAL - 1))) unreferenced leftover(s) from conflicted writers"

# --- concurrency (b): a slow save deterministically loses one version ------
step "concurrency (b): slow save vs quick save for the same next version on $CONFLICT"
note "the slow writer opens the lake at v2 and encodes $SLOWROWS rows; the quick"
note "writer publishes v3 while it is busy. Exactly one of them may win v3; the"
note "loser must get a documented write conflict (this is deterministic by"
note "construction: the winner's snapshot is already published when the slow"
note "writer gets to its own snapshot PUT — no atomicity needed for that PUT)."

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
sleep 0.1
{ echo ".import $TMP/quick.csv tquick"; echo ".lake save $CONFLICT"; echo ".quit"; } \
    | cli_run "$TMP/conflict_quick.log"
assert_status_zero "$TMP/conflict_quick.log" "$?"
wait "$SLOW_PID"
assert_status_zero "$TMP/conflict_slow.exit" "$(cat "$TMP/conflict_slow.exit")"

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
    print(f"both writers won: {saved}", file=sys.stderr)
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

curl -fsS --max-time 30 -o "$TMP/conflict_catalog.json" "$ENDPOINT/$BUCKET/conflict/catalog.json" || fail "fetching conflict/catalog.json failed"
curl -fsS --max-time 30 -o "$TMP/conflict_snapshot.json" "$ENDPOINT/$BUCKET/conflict/snapshots/3.json" \
    || fail "fetching conflict/snapshots/3.json failed"
cmp -s "$TMP/conflict_catalog.json" "$TMP/conflict_snapshot.json" \
    || fail "conflict: catalog.json is not byte-identical to snapshots/3.json"
ok "catalog.json == snapshots/3.json (bytes; single claimant)"

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
curl -fsS --max-time 30 -X PUT --data-binary 'not a manifest' \
    "$ENDPOINT/$BUCKET/refuse/snapshots/4.json" -o /dev/null || fail "planting the stray snapshot failed"
ok "planted stray refuse/snapshots/4.json"

{ echo ".lake save $REFUSE"; echo ".quit"; } | cli_run "$TMP/refuse_stray.log"
assert_status_zero "$TMP/refuse_stray.log" "$?"
assert_contains "$TMP/refuse_stray.log" "Error: write conflict at"
assert_contains "$TMP/refuse_stray.log" "snapshot 4 already exists"
assert_contains "$TMP/refuse_stray.log" "refusing to replace a published version"

{ echo ".lake versions $REFUSE"; echo ".quit"; } | cli_run "$TMP/refuse_versions.log"
assert_status_zero "$TMP/refuse_versions.log" "$?"
assert_re "$TMP/refuse_versions.log" '^3 \(current\)$'
assert_no_match "$TMP/refuse_versions.log" '^4'
ok "the unpublished stray is not advertised (.lake versions stays 1..3)"

curl -fsS --max-time 30 -X DELETE "$ENDPOINT/$BUCKET/refuse/snapshots/4.json" -o /dev/null \
    || fail "removing the stray snapshot failed"
{ echo ".lake save $REFUSE"; echo ".quit"; } | cli_run "$TMP/refuse_retry.log"
assert_status_zero "$TMP/refuse_retry.log" "$?"
assert_re "$TMP/refuse_retry.log" '^Saved lake version 4 in '
ok "after removing the stray, the next save published version 4"

# --- summary ---------------------------------------------------------------
step "summary"
printf '  roundtrip:     version 2, 3 rows round-tripped through a fresh process; 4 objects listed unsigned\n'
if [ "$DUPES" = "0" ]; then
    printf '  concurrency:   race: 9 attempts, %s claims, %s conflict(s), one winner per version;\n' "$CLAIMS" "$RACE_CONFLICTS"
else
    printf '  concurrency:   race: 9 attempts, %s claims, %s conflict(s), %s duplicate claim(s) — server non-atomic;\n' \
        "$CLAIMS" "$RACE_CONFLICTS" "$DUPES"
fi
printf '                 slow-vs-quick: exactly one winner of v3 (the %s save), loser conflicted; catalog == snapshots/3.json\n' "$WINNER"
printf '                 settle save after the race: version %s alone; catalog == snapshots/%s.json\n' "$FINAL" "$FINAL"
printf '  refusal:       second save -> v3 (new version); stray snapshot -> write conflict; removed -> v4\n'
printf '\nALL CHECKS PASSED'
if [ "$DUPES" != "0" ]; then
    printf ' (the exactly-one-winner race assertion is not enforced on this non-atomic server)'
fi
printf '\n'
