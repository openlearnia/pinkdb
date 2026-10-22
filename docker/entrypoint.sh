#!/bin/sh
# POSIX entrypoint for the pinkdb image.
#
# Two jobs:
#   1. `pinkdb …` runs the CLI in the same image (pass-through);
#   2. otherwise the container runs pinkdb-server with container defaults,
#      while every server flag still works and PINKDB_* environment variables
#      provide the common ones.
#
# Precedence: image defaults < PINKDB_* environment < explicit arguments.
# The defaults are prepended to "$@", so a later CLI flag wins (the server
# parser is last-wins).
set -eu

# The CLI is a first-class citizen of the image.
if [ "${1:-}" = "pinkdb" ]; then
    shift
    exec pinkdb "$@"
fi

# An explicit non-flag command (`sh`, `pinkdb-server`, …) is run as given.
case "${1:-}" in
    "" | -*) ;;
    *) exec "$@" ;;
esac

# Optional environment -> server flags. Each is prepended; the base defaults
# are prepended last, so the final order is defaults, environment, arguments.
if [ -n "${PINKDB_TRANSPORT:-}" ]; then
    set -- --transport "$PINKDB_TRANSPORT" "$@"
fi
if [ -n "${PINKDB_TLS_CERT:-}" ]; then
    set -- --tls-cert "$PINKDB_TLS_CERT" "$@"
fi
if [ -n "${PINKDB_TLS_KEY:-}" ]; then
    set -- --tls-key "$PINKDB_TLS_KEY" "$@"
fi
if [ -n "${PINKDB_MAX_CONNECTIONS:-}" ]; then
    set -- --max-connections "$PINKDB_MAX_CONNECTIONS" "$@"
fi
if [ -n "${PINKDB_IDLE_TIMEOUT:-}" ]; then
    set -- --idle-timeout "$PINKDB_IDLE_TIMEOUT" "$@"
fi
if [ -n "${PINKDB_STATEMENT_TIMEOUT_MS:-}" ]; then
    set -- --statement-timeout-ms "$PINKDB_STATEMENT_TIMEOUT_MS" "$@"
fi
if [ -n "${PINKDB_MAX_RESULT_ROWS:-}" ]; then
    set -- --max-result-rows "$PINKDB_MAX_RESULT_ROWS" "$@"
fi
if [ -n "${PINKDB_MAX_RESULT_BYTES:-}" ]; then
    set -- --max-result-bytes "$PINKDB_MAX_RESULT_BYTES" "$@"
fi
if [ -n "${PINKDB_MAX_CONCURRENT_LOGINS:-}" ]; then
    set -- --max-concurrent-logins "$PINKDB_MAX_CONCURRENT_LOGINS" "$@"
fi
case "${PINKDB_ALLOW_LOAD_EXTENSION:-}" in
    1 | true | TRUE | yes) set -- --allow-load-extension "$@" ;;
esac
if [ -n "${PINKDB_EXTENSION_ROOT:-}" ]; then
    set -- --extension-root "$PINKDB_EXTENSION_ROOT" "$@"
fi
case "${PINKDB_ALLOW_COPY_TO:-}" in
    1 | true | TRUE | yes) set -- --allow-copy-to "$@" ;;
esac
if [ -n "${PINKDB_COPY_ROOT:-}" ]; then
    set -- --copy-root "$PINKDB_COPY_ROOT" "$@"
fi

# Container defaults: all interfaces, the documented port, and a durable
# database on the /data volume. PINKDB_HOST/PORT/FILE override them (and the
# HEALTHCHECK reads the same variables).
set -- \
    --host "${PINKDB_HOST:-0.0.0.0}" \
    --port "${PINKDB_PORT:-5433}" \
    --file "${PINKDB_FILE:-/data/pinkdb.pink}" \
    "$@"

exec pinkdb-server "$@"
