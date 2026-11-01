#!/usr/bin/env bash
#
# Smoke-test a built pinkdb image: the container contract, the protocol
# health check, the image's own CLI in remote mode, and persistence across a
# restart. The `lake` variant additionally exercises `.lake save` / `.lake
# open` on the /data volume. The `quacklab` variant serves the QuackLab HTTP
# contract on $PORT (published on loopback here) and keeps its data under
# /var/lib/pinkdb; it is checked with /health, /sql and a write lease.
#
#   scripts/docker/smoke.sh --image pinkdb:default  --variant default
#   scripts/docker/smoke.sh --image pinkdb:lake     --variant lake
#   scripts/docker/smoke.sh --image pinkdb:quacklab --variant quacklab
#
# Everything is cleaned up: the container, the named volume and the temporary
# directory. No registry is contacted. The quacklab variant needs curl.
set -euo pipefail

die() {
    echo "docker-smoke: error: $*" >&2
    exit 1
}

usage() {
    cat <<'EOF'
Usage: scripts/docker/smoke.sh --image IMAGE --variant default|lake|quacklab

  --image NAME    the locally built image to exercise
  --variant NAME  default (pinklake CLI), lake (parquet CLI) or quacklab
  -h, --help      show this help
EOF
}

image=""
variant=""
while [ $# -gt 0 ]; do
    case "$1" in
        --image)
            [ $# -ge 2 ] || die "--image requires a value"
            image=$2
            shift 2
            ;;
        --variant)
            [ $# -ge 2 ] || die "--variant requires a value"
            variant=$2
            shift 2
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            die "unknown argument: $1 (try --help)"
            ;;
    esac
done

[ -n "$image" ] || die "--image is required"
[ -n "$variant" ] || die "--variant is required"
case "$variant" in
    default | lake) data_dir=/data ;;
    quacklab) data_dir=/var/lib/pinkdb ;;
    *) die "--variant must be default, lake or quacklab, got '$variant'" ;;
esac
command -v docker >/dev/null 2>&1 || die "docker is not installed"

tmp=$(mktemp -d "${TMPDIR:-/tmp}/pinkdb-docker-smoke.XXXXXX")
name="pinkdb-smoke-$$"
volume="pinkdb-smoke-vol-$$"

cleanup() {
    docker rm -f "$name" >/dev/null 2>&1 || true
    docker volume rm "$volume" >/dev/null 2>&1 || true
    rm -rf "$tmp"
}
trap cleanup EXIT HUP INT TERM

step() {
    printf 'docker-smoke: %s\n' "$*"
}

wait_healthy() {
    deadline=$((SECONDS + 120))
    status=""
    while [ "$SECONDS" -lt "$deadline" ]; do
        status=$(docker inspect -f \
            '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
            "$name" 2>/dev/null || true)
        case "$status" in
            healthy)
                return 0
                ;;
            unhealthy)
                docker logs "$name" >&2 || true
                die "container became unhealthy"
                ;;
        esac
        if ! docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null | grep -q true; then
            docker logs "$name" >&2 || true
            die "container exited before it became healthy (last status: ${status:-missing})"
        fi
        sleep 1
    done
    docker logs "$name" >&2 || true
    die "container did not become healthy within 120s (last status: ${status:-missing})"
}

docker volume create "$volume" >/dev/null

run_args=""
token="smoke-$$-$RANDOM"
quacklab_port=8080
case "$variant" in
    quacklab)
        command -v curl >/dev/null 2>&1 || die "the quacklab smoke needs curl"
        # Cloud Run's contract: the service listens on $PORT, which the host
        # publishes on a free loopback port.
        run_args="-e PORT=$quacklab_port -e DUCKDB_SQL_TOKEN=$token -p 127.0.0.1::$quacklab_port"
        ;;
esac

step "start $image as $name"
docker run -d --name "$name" -v "$volume:$data_dir" $run_args "$image" >/dev/null
wait_healthy
step "built-in HEALTHCHECK reports healthy"

# Non-root identity and the CLI in the same image.
uid=$(docker run --rm "$image" id -u)
[ "$uid" = "10001" ] || die "expected uid 10001, got '$uid'"
docker run --rm "$image" pinkdb --version | grep -q '^pinkdb ' ||
    die "the image's CLI does not report a version"
step "runs as uid 10001 and ships the pinkdb CLI"

# The server logs are useful in a failure report.
docker logs "$name" 2>&1 | grep -q 'listening on' ||
    die "server did not log its listening address"

# Protocol health check: success, and a nonzero exit for a dead address.
docker exec "$name" pinkdb-server --health-check >/dev/null
if docker exec "$name" pinkdb-server --health-check 127.0.0.1:1 >/dev/null 2>&1; then
    die "--health-check accepted a dead port"
fi
docker run --rm --network "container:$name" "$image" \
    pinkdb-server --health-check >/dev/null
step "--health-check exits 0 for the server, nonzero for a dead port"

# Remote CLI mode through the image's own CLI (pass-through entrypoint).
remote() {
    docker run --rm --network "container:$name" "$image" pinkdb \
        --host 127.0.0.1:5433 "$@"
}

remote -c "CREATE TABLE smoke (id INTEGER, name VARCHAR)" >/dev/null
remote -c "INSERT INTO smoke VALUES (1, 'alpha'), (2, 'beta')" >/dev/null
answer=$(remote -c "SELECT (count(*) = 2 AND sum(id) = 3) AS ok FROM smoke")
printf '%s\n' "$answer" | grep -q 'true' ||
    die "remote SELECT did not return true:\n$answer"
step "remote CLI created, inserted and queried rows"

# Durable mode: restart the same container and read the same rows back.
docker restart "$name" >/dev/null
wait_healthy
answer=$(remote -c "SELECT (count(*) = 2 AND sum(id) = 3) AS ok FROM smoke")
printf '%s\n' "$answer" | grep -q 'true' ||
    die "rows did not survive the restart:\n$answer"
step "rows survived a container restart"

if [ "$variant" = lake ]; then
    # The lake CLI: publish a lake snapshot into /data and
    # open it back in the same session.
    lake_out=$(docker run --rm -v "$volume:/data" "$image" pinkdb \
        -c "CREATE TABLE lake_smoke (id INTEGER)" \
        -c "INSERT INTO lake_smoke VALUES (1), (2)" \
        -c ".lake save /data/lake-smoke" \
        -c ".lake open /data/lake-smoke" \
        -c "SELECT count(*) = 2 AS ok FROM lake_smoke")
    printf '%s\n' "$lake_out" | grep -q 'true' ||
        die ".lake save/open did not round-trip:\n$lake_out"
    step ".lake save / .lake open round-tripped on the /data volume"
fi

if [ "$variant" = quacklab ]; then
    # The image entrypoint passed --quacklab-bind 0.0.0.0:$PORT; the wire
    # protocol stays on loopback and must not be published.
    [ -z "$(docker port "$name" 5433/tcp 2>/dev/null)" ] ||
        die "the quacklab image published the wire-protocol port"
    base="http://127.0.0.1:$(docker port "$name" "$quacklab_port/tcp" | sed 's/.*://')"
    # Reply body lands in $tmp/reply.json; curl prints the status code.
    http_get() {
        curl -sS -o "$tmp/reply.json" -w '%{http_code}' "$base$1"
    }
    http_post() { # path json-body [token]
        if [ -n "${3:-}" ]; then
            curl -sS -o "$tmp/reply.json" -w '%{http_code}' \
                -H 'Content-Type: application/json' \
                -H "Authorization: Bearer $3" -d "$2" "$base$1"
        else
            curl -sS -o "$tmp/reply.json" -w '%{http_code}' \
                -H 'Content-Type: application/json' -d "$2" "$base$1"
        fi
    }
    [ "$(http_get /health)" = 200 ] || die "GET /health was not 200"
    grep -q '"ok":true' "$tmp/reply.json" || die "GET /health: $(cat "$tmp/reply.json")"
    step "GET /health answered ok on \$PORT"

    [ "$(http_post /sql '{"sql":"SELECT 1 AS one"}')" = 401 ] ||
        die "POST /sql without a token was not 401"
    grep -q '"code":"unauthorized"' "$tmp/reply.json" ||
        die "POST /sql without a token: $(cat "$tmp/reply.json")"
    [ "$(http_post /sql '{"sql":"CREATE TABLE http_smoke (id INTEGER)"}' "$token")" = 200 ] ||
        die "POST /sql (create) failed"
    [ "$(http_post /sql '{"sql":"INSERT INTO http_smoke VALUES (7)"}' "$token")" = 200 ] ||
        die "POST /sql (insert) failed"
    [ "$(http_post /sql '{"sql":"SELECT sum(id) AS s FROM http_smoke"}' "$token")" = 200 ] ||
        die "POST /sql (select) failed"
    grep -q '"rows":\[\[7\]\]' "$tmp/reply.json" ||
        die "POST /sql returned $(cat "$tmp/reply.json")"
    step "POST /sql rejected an anonymous request and round-tripped rows with a token"

    # Lease control is loopback-only in the contract (the Go reference and this
    # adapter both answer 403 loopback_only), so a published port cannot reach
    # it — Cloud Run never does; its writes go to the lake. The check runs on
    # the container's own loopback, with the busybox wget the musl runtime has.
    [ "$(http_post /write-lease/acquire '{"owner":"smoke"}' "$token")" = 403 ] ||
        die "write-lease acquire from the published port was not 403"
    grep -q '"code":"loopback_only"' "$tmp/reply.json" ||
        die "write-lease acquire from the published port: $(cat "$tmp/reply.json")"
    if docker exec "$name" wget --help >/dev/null 2>&1; then
        lease_out=$(docker exec "$name" wget -q -O - \
            --header "Authorization: Bearer $token" \
            --post-data '{"owner":"smoke"}' \
            "http://127.0.0.1:$quacklab_port/write-lease/acquire")
        lease=$(printf '%s' "$lease_out" |
            sed -n 's/.*"leaseId":"\([0-9a-f]\{32\}\)".*/\1/p')
        [ -n "$lease" ] || die "write-lease acquire returned $lease_out"
        docker exec "$name" wget -q -O /dev/null \
            --header "Authorization: Bearer $token" \
            --post-data "{\"leaseId\":\"$lease\"}" \
            "http://127.0.0.1:$quacklab_port/write-lease/release" ||
            die "write-lease release failed"
        step "write lease refused on the published port and acquired/released on loopback"
    else
        step "write lease refused on the published port (no wget in the runtime to test loopback)"
    fi
fi

size=$(docker image inspect -f '{{.Size}}' "$image")
printf 'docker-smoke: image/%s ok (%s bytes)\n' "$variant" "$size"
