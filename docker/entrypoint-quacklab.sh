#!/bin/sh
# POSIX entrypoint for the `quacklab` image variant (waves 138c/138d).
#
# The container is a QuackLab SQL service: pinkdb-server serves the HTTP
# contract (`POST /sql`, `GET /health`, `POST /recover`, `/write-lease/*`) on
# 0.0.0.0:${PORT:-1294} — $PORT is the port Cloud Run routes. There is no
# wire-protocol and no console port published: the binary protocol listener
# stays on loopback for the image HEALTHCHECK and for a debugging session, and
# nothing outside the container reaches it. `DUCKDB_BIND` (the QuackLab
# deployment's own knob) overrides the adapter address.
#
# QuackLab's deployer starts its own image with `--args=duckdb` for the quack
# and quack-ro services (`go/internal/crserverless`), so `duckdb` as the first
# argument means "serve", exactly like no argument at all.
#
# Storage:
# - GRAIN_DUCKLAKE=1 (or PINKDB_QUACKLAB_LAKE=1): the warehouse is a pinkdb
#   lake database (catalog DUCKLAKE_PG_URI, data DUCKLAKE_DATA_PATH). Durable
#   state is the lake, so the server's own store runs --in-memory, like the
#   Go service's ephemeral DuckDB; PINKDB_DATA_DIR still forces a data
#   directory.
# - otherwise: a local warehouse database in /var/lib/pinkdb (Cloud Run's
#   filesystem is in memory and goes away with the instance).
#
# `pinkdb …` runs the CLI in the same image (pass-through); any other non-flag
# command is run as given. `exec` keeps the server as PID 1, so SIGTERM drains
# in-flight requests, checkpoints the store, releases the lake's writer lease
# and exits — Cloud Run allows 10 s.
set -eu

if [ "${1:-}" = "pinkdb" ]; then
    shift
    exec pinkdb "$@"
fi

if [ "${1:-}" = "duckdb" ]; then
    shift
fi

case "${1:-}" in
    "" | -*) ;;
    *) exec "$@" ;;
esac

# Go's truthy rule (1, true, yes; any case).
truthy() {
    case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]' | tr -d ' ')" in
        1 | true | yes) return 0 ;;
        *) return 1 ;;
    esac
}

if [ -z "${PINKDB_DATA_DIR:-}" ] && { truthy "${GRAIN_DUCKLAKE:-}" || truthy "${PINKDB_QUACKLAB_LAKE:-}"; }; then
    set -- --in-memory "$@"
else
    set -- --data-dir "${PINKDB_DATA_DIR:-/var/lib/pinkdb}" "$@"
fi

# Explicit arguments win (the server parser is last-wins).
exec pinkdb-server \
    --host 127.0.0.1 \
    --port "${PINKDB_PORT:-5433}" \
    --quacklab-bind "${DUCKDB_BIND:-0.0.0.0:${PORT:-1294}}" \
    "$@"
