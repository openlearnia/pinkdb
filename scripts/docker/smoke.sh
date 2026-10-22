#!/usr/bin/env bash
#
# Smoke-test a built pinkdb image: the container contract, the protocol
# health check, the image's own CLI in remote mode, and persistence across a
# restart. The `lake` variant additionally exercises `.lake save` / `.lake
# open` on the /data volume.
#
#   scripts/docker/smoke.sh --image pinkdb:default --variant default
#   scripts/docker/smoke.sh --image pinkdb:lake    --variant lake
#
# Everything is cleaned up: the container, the named volume and the temporary
# directory. No registry is contacted.
set -euo pipefail

die() {
    echo "docker-smoke: error: $*" >&2
    exit 1
}

usage() {
    cat <<'EOF'
Usage: scripts/docker/smoke.sh --image IMAGE --variant default|lake

  --image NAME    the locally built image to exercise
  --variant NAME  default (pinklake CLI) or lake (vortex/parquet CLI)
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
    default | lake) ;;
    *) die "--variant must be default or lake, got '$variant'" ;;
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

step "start $image as $name"
docker run -d --name "$name" -v "$volume:/data" "$image" >/dev/null
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
    # `pinklake` / `vortexlake` CLI: publish a lake snapshot into /data and
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

size=$(docker image inspect -f '{{.Size}}' "$image")
printf 'docker-smoke: image/%s ok (%s bytes)\n' "$variant" "$size"
