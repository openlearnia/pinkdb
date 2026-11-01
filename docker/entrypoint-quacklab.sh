#!/bin/sh
# POSIX entrypoint for the `quacklab` image variant (wave 138d).
#
# The container is a QuackLab SQL service: pinkdb-server serves the HTTP
# contract (`POST /sql`, `GET /health`, `POST /recover`, `/write-lease/*`) on
# 0.0.0.0:${PORT:-1294} — $PORT is the port Cloud Run routes. There is no
# wire-protocol and no console port published: the binary protocol listener
# stays on loopback for the image HEALTHCHECK and for a debugging session, and
# nothing outside the container reaches it. `DUCKDB_BIND` (the QuackLab
# deployment's own knob) overrides the adapter address.
#
# Durable state is the lake, not the container: Cloud Run's filesystem is
# in-memory and goes away with the instance, so /var/lib/pinkdb holds only the
# local warehouse the adapter opens (wave 138c moves it to the lake). Losing it
# on a cold start loses no committed data.
#
# `pinkdb …` runs the CLI in the same image (pass-through); any other non-flag
# command is run as given. `exec` keeps the server as PID 1, so SIGTERM drains
# in-flight requests, checkpoints the store and exits — Cloud Run's 10 s.
set -eu

if [ "${1:-}" = "pinkdb" ]; then
    shift
    exec pinkdb "$@"
fi

case "${1:-}" in
    "" | -*) ;;
    *) exec "$@" ;;
esac

# Explicit arguments win (the server parser is last-wins).
exec pinkdb-server \
    --host 127.0.0.1 \
    --port "${PINKDB_PORT:-5433}" \
    --data-dir "${PINKDB_DATA_DIR:-/var/lib/pinkdb}" \
    --quacklab-bind "${DUCKDB_BIND:-0.0.0.0:${PORT:-1294}}" \
    "$@"
