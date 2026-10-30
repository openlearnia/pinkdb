#!/bin/sh
# Local pinkdb services for development (`make console`, `make stop`, …):
# the server with its warehouse console, run in the background from this
# checkout's debug build. Its data directory, pid file and log live under
# .dev/.
#
#   scripts/dev-services.sh start|stop|restart|status|logs
#
# Settings (environment): DATA_DIR (.dev/data), PORT (5491), UI_PORT (8091),
# UI_AUTH (password | none), AGENT=1 (the data agent behind the ask box),
# MEMORY=1 (agent memory).
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DEV="$ROOT/.dev"
DATA_DIR=${DATA_DIR:-$DEV/data}
# A relative data directory is relative to the checkout, wherever make runs
# from.
case $DATA_DIR in
/*) ;;
*) DATA_DIR="$ROOT/$DATA_DIR" ;;
esac
PORT=${PORT:-5491}
UI_PORT=${UI_PORT:-8091}
UI_AUTH=${UI_AUTH:-password}
AGENT=${AGENT:-}
MEMORY=${MEMORY:-}
PID="$DEV/pinkdb-server.pid"
LOG="$DEV/pinkdb-server.log"
BIN="$ROOT/target/debug/pinkdb-server"

case $(uname -s) in
Darwin) EXT=dylib ;;
*) EXT=so ;;
esac

# Build an extension and print its library's path.
extension() {
    (cd "$ROOT" && cargo build -q -p "pinkdb-$1")
    lib="$ROOT/target/debug/libpinkdb_$1.$EXT"
    # macOS 27 refuses a library whose string table is mis-aligned.
    if [ "$EXT" = dylib ]; then
        python3 "$ROOT/scripts/macos-align-linkedit.py" "$lib" >/dev/null
    fi
    echo "$lib"
}

listening() {
    lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

# The running servers' pids, space-separated (digits only, so the callers
# split them on purpose).
servers() {
    pgrep -x pinkdb-server | tr '\n' ' ' | sed 's/ *$//' || true
}

start() {
    for port in "$PORT" "$UI_PORT"; do
        if listening "$port"; then
            echo "port $port is in use (make status; make stop)" >&2
            exit 1
        fi
    done
    (cd "$ROOT" && cargo build -q -p pinkdb-server --features ui)
    set -- --data-dir "$DATA_DIR" --port "$PORT" --enable-ui --ui-port "$UI_PORT" --ui-auth "$UI_AUTH"
    if [ -n "$AGENT" ]; then
        set -- "$@" --agent-extension "$(extension agent)"
    fi
    if [ -n "$MEMORY" ]; then
        set -- "$@" --memory-extension "$(extension memory)"
    fi
    mkdir -p "$DEV"
    nohup "$BIN" "$@" >"$LOG" 2>&1 &
    echo $! >"$PID"
    # Wait for the console to listen, or for the server to fail.
    tries=0
    until grep -q "warehouse console on" "$LOG"; do
        if ! kill -0 "$(cat "$PID")" 2>/dev/null; then
            cat "$LOG" >&2
            rm -f "$PID"
            exit 1
        fi
        tries=$((tries + 1))
        if [ "$tries" -gt 150 ]; then
            echo "the console did not start in 30 s; see $LOG" >&2
            exit 1
        fi
        sleep 0.2
    done
    cat "$LOG"
    echo "pid $(cat "$PID"); log $LOG; stop with: make stop"
}

stop() {
    pids=$(servers)
    rm -f "$PID"
    if [ -z "$pids" ]; then
        echo "no pinkdb-server is running"
        return
    fi
    echo "stopping pinkdb-server (pid $pids)"
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || true
    tries=0
    while [ -n "$(servers)" ] && [ "$tries" -lt 50 ]; do
        tries=$((tries + 1))
        sleep 0.2
    done
    if [ -n "$(servers)" ]; then
        echo "still running after 10 s; killing"
        # shellcheck disable=SC2046
        kill -9 $(servers) 2>/dev/null || true
    fi
}

status() {
    pids=$(servers)
    if [ -z "$pids" ]; then
        echo "no pinkdb-server is running"
        return
    fi
    list=$(echo "$pids" | tr ' ' ',')
    ps -o pid=,command= -p "$list"
    lsof -nP -a -iTCP -sTCP:LISTEN -p "$list" 2>/dev/null |
        awk 'NR > 1 { print "  pid " $2 " listening on " $9 }'
}

case "${1:-}" in
start) start ;;
stop) stop ;;
restart) stop && start ;;
status) status ;;
logs) tail -f "$LOG" ;;
*)
    echo "usage: $0 start|stop|restart|status|logs" >&2
    exit 2
    ;;
esac
