#!/usr/bin/env bash
#
# Smoke-test one built pinkdb artifact before it is packaged or published.
#
#   scripts/release/smoke.sh --kind cli --variant default --bin target/release/pinkdb
#   scripts/release/smoke.sh --kind server --variant async-server \
#       --bin target/release/pinkdb-server
#
# CLI probes:
#   * every build: `--version` (stable `pinkdb <semver>` output);
#   * default: SELECT, json_valid, exp(1), strftime on a TIMESTAMP;
#   * lean: SELECT works and the default-feature json/exp functions are absent;
#   * lake variants: SELECT plus `.help` advertises the `.lake` surface
#     (`s3://` URLs only in the s3 variants; no network is touched);
#   * every variant: `SELECT 42` (a statement really executes).
#
# Server probes (TCP only, no protocol client needed):
#   * start on an ephemeral port (`--host 127.0.0.1 --port 0`), wait for the
#     documented "listening on HOST:PORT" readiness line, connect with python3,
#     then shut down;
#   * async-server builds start with `--transport async` and refuse
#     `--transport async` + TLS flags (the H-2 no-silent-plaintext rule);
#   * tls builds start with a throwaway self-signed certificate and refuse
#     `--transport async` (no async feature);
#   * default builds refuse `--transport async`.
#
# Cross-built binaries on an Apple Silicon host are run through `arch -x86_64`
# when Rosetta is available; otherwise the smoke fails (never skip silently).
set -euo pipefail

die() {
    echo "smoke: error: $*" >&2
    exit 1
}

usage() {
    cat <<'EOF'
Usage: scripts/release/smoke.sh --kind cli|server --variant NAME --bin PATH

  --kind cli|server   artifact family
  --variant NAME      feature variant used to choose the probes
  --bin PATH          built binary to exercise
  -h, --help          show this help
EOF
}

kind=""
variant=""
bin=""

while [ $# -gt 0 ]; do
    case "$1" in
        --kind)
            [ $# -ge 2 ] || die "--kind requires a value"
            kind=$2
            shift 2
            ;;
        --variant)
            [ $# -ge 2 ] || die "--variant requires a value"
            variant=$2
            shift 2
            ;;
        --bin)
            [ $# -ge 2 ] || die "--bin requires a path"
            bin=$2
            shift 2
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *) die "unknown argument: $1 (try --help)" ;;
    esac
done

[ -n "$kind" ] || die "--kind is required"
[ -n "$variant" ] || die "--variant is required"
[ -n "$bin" ] || die "--bin is required"
[ -f "$bin" ] || die "binary does not exist: $bin"
[ -x "$bin" ] || die "binary is not executable: $bin"

tmp=$(mktemp -d "${TMPDIR:-/tmp}/pinkdb-smoke.XXXXXX")
server_pid=""
cleanup() {
    if [ -n "$server_pid" ]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
        server_pid=""
    fi
    rm -rf "$tmp"
}
trap cleanup EXIT HUP INT TERM

# Run a cross-built x86_64 macOS binary through Rosetta when this host is
# arm64 and the tool is available. Everything else runs directly.
run_prefix=()
if [ "$(uname -s)" = Darwin ] && [ "$(uname -m)" = arm64 ]; then
    if command -v file >/dev/null 2>&1 &&
        file -b "$bin" 2>/dev/null | grep -q 'x86_64'; then
        command -v arch >/dev/null 2>&1 || die "cross-built binary needs Rosetta (arch) to smoke"
        arch -x86_64 /usr/bin/true >/dev/null 2>&1 ||
            die "cross-built binary needs Rosetta; install it or smoke on an x86_64 host"
        run_prefix=(arch -x86_64)
    fi
fi

run_bin() {
    if [ "${#run_prefix[@]}" -gt 0 ]; then
        "${run_prefix[@]}" "$bin" "$@"
    else
        "$bin" "$@"
    fi
}

expect_contains() {
    label=$1
    needle=$2
    shift 2
    status=0
    out=$(run_bin "$@" 2>&1) || status=$?
    if [ "$status" -ne 0 ]; then
        printf '%s\n' "$out" >&2
        die "$label failed (exit status $status)"
    fi
    case "$out" in
        *"$needle"*) ;;
        *)
            printf '%s\n' "$out" >&2
            die "$label: output does not contain '$needle'"
            ;;
    esac
}

expect_absent() {
    label=$1
    needle=$2
    shift 2
    out=$(run_bin "$@" 2>&1) || {
        printf '%s\n' "$out" >&2
        die "$label failed"
    }
    case "$out" in
        *"$needle"*)
            printf '%s\n' "$out" >&2
            die "$label: output unexpectedly contains '$needle'"
            ;;
    esac
}

expect_refusal() {
    label=$1
    needle=$2
    shift 2
    status=0
    out=$(run_bin "$@" 2>&1) || status=$?
    if [ "$status" -eq 0 ]; then
        printf '%s\n' "$out" >&2
        die "$label was accepted (expected a nonzero exit)"
    fi
    case "$out" in
        *"$needle"*) ;;
        *)
            printf '%s\n' "$out" >&2
            die "$label: refusal does not mention '$needle'"
            ;;
    esac
}

probe_tcp() {
    port=$1
    python3 - "$port" <<'PY'
import socket
import sys

port = int(sys.argv[1])
with socket.create_connection(("127.0.0.1", port), timeout=5):
    pass
print(f"tcp probe ok on 127.0.0.1:{port}")
PY
}

start_server() {
    log="$tmp/server.log"
    : >"$log"
    if [ "${#run_prefix[@]}" -gt 0 ]; then
        "${run_prefix[@]}" "$bin" --host 127.0.0.1 --port 0 "$@" >"$log" 2>&1 &
    else
        "$bin" --host 127.0.0.1 --port 0 "$@" >"$log" 2>&1 &
    fi
    server_pid=$!

    addr=""
    i=0
    while [ "$i" -lt 300 ]; do
        addr=$(sed -n 's/.*listening on \([^ ]*\).*/\1/p' "$log" | head -n 1)
        [ -n "$addr" ] && break
        if ! kill -0 "$server_pid" 2>/dev/null; then
            cat "$log" >&2
            die "server exited before it was ready"
        fi
        sleep 0.1
        i=$((i + 1))
    done
    [ -n "$addr" ] || {
        cat "$log" >&2
        die "server did not print its listening address within 30s"
    }
    port=${addr##*:}
    case "$port" in
        '' | *[!0-9]*)
            cat "$log" >&2
            die "could not parse the listening port from '$addr'"
            ;;
    esac
    probe_tcp "$port"
    grep -m1 'listening on' "$log" || true
}

stop_server() {
    if [ -n "$server_pid" ]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
        server_pid=""
    fi
}

if [ "$kind" = cli ]; then
    expect_contains "cli --version" "pinkdb " --version
    expect_contains "cli SELECT 42" "42" -c "SELECT 42 AS answer"

    case "$variant" in
        default)
            expect_contains "json_valid" "true" -c "SELECT json_valid('{}') AS ok"
            expect_contains "exp" "2.718281828459045" -c "SELECT exp(1) AS e"
            expect_contains "strftime" "2024" \
                -c "SELECT strftime(TIMESTAMP '2024-01-02 00:00:00', '%Y') AS y"
            ;;
        lean)
            expect_refusal "lean json_valid" "json" -c "SELECT json_valid('{}')"
            expect_refusal "lean exp" "exp" -c "SELECT exp(1)"
            ;;
        pinklake)
            expect_contains "pinklake .help" ".lake save" -c ".help"
            expect_contains "pinklake build marker" "pinklake build" -c ".help"
            ;;
        pinklake-s3)
            expect_contains "pinklake-s3 .help" ".lake save" -c ".help"
            expect_contains "pinklake-s3 build marker" "pinklake build" -c ".help"
            expect_contains "pinklake-s3 URLs" "s3://bucket/prefix" -c ".help"
            ;;
        vortexlake)
            expect_contains "vortexlake .help" ".lake save" -c ".help"
            expect_absent "vortexlake is not pinklake" "pinklake build" -c ".help"
            ;;
        vortexlake-s3)
            expect_contains "vortexlake-s3 .help" ".lake save" -c ".help"
            expect_absent "vortexlake-s3 is not pinklake" "pinklake build" -c ".help"
            expect_contains "vortexlake-s3 URLs" "s3://bucket/prefix" -c ".help"
            ;;
        *)
            die "unknown CLI variant '$variant'"
            ;;
    esac

    echo "smoke: cli/$variant ok"
    exit 0
fi

[ "$kind" = server ] || die "--kind must be 'cli' or 'server'"

case "$variant" in
    default)
        start_server
        stop_server
        expect_refusal "default async transport" \
            "requires a build with the \`async-server\` feature" --transport async
        ;;
    async-server)
        start_server --transport async
        stop_server
        expect_refusal "async + TLS" \
            "does not support TLS" --transport async \
            --tls-cert "$tmp/cert.pem" --tls-key "$tmp/key.pem"
        ;;
    tls)
        command -v openssl >/dev/null 2>&1 || die "the tls smoke needs openssl to make a certificate"
        openssl req -x509 -newkey rsa:2048 -sha256 -days 1 -nodes \
            -keyout "$tmp/key.pem" -out "$tmp/cert.pem" -subj "/CN=localhost" \
            >/dev/null 2>&1 || die "openssl could not generate a test certificate"
        start_server --tls-cert "$tmp/cert.pem" --tls-key "$tmp/key.pem"
        stop_server
        expect_refusal "tls build async transport" \
            "requires a build with the \`async-server\` feature" --transport async
        ;;
    *)
        die "unknown server variant '$variant'"
        ;;
esac

echo "smoke: server/$variant ok"
