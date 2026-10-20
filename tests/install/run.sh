#!/usr/bin/env bash
#
# Local installer harness. Serves a fake release tree with
# `python3 -m http.server` and exercises install.sh end to end:
#
#   * latest (GitHub `releases/latest` JSON) and pinned versions;
#   * variant slug mapping (`pinklake,s3` -> `pinklake-s3`);
#   * checksum verification, including a tampered SHA256SUMS (fail closed);
#   * missing checksums, unsupported OS/arch/variant;
#   * existing-install refusal and --force;
#   * --dry-run (no download, no install);
#   * prefix/PATH guidance and temporary-directory cleanup.
#
# Run from anywhere:  tests/install/run.sh
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
install_sh="$repo_root/install.sh"
[ -f "$install_sh" ] || {
    echo "error: missing $install_sh" >&2
    exit 1
}

tmp=$(mktemp -d "${TMPDIR:-/tmp}/pinkdb-install-test.XXXXXX")
server_pid=""
cleanup() {
    if [ -n "$server_pid" ]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    rm -rf "$tmp"
}
trap cleanup EXIT HUP INT TERM

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

serve="$tmp/serve"
mkdir -p "$serve/api/repos/openlearnia/pinkdb/releases" "$serve/download"
cat >"$serve/api/repos/openlearnia/pinkdb/releases/latest" <<'JSON'
{"tag_name":"v1.2.3","name":"v1.2.3"}
JSON

# make_asset VERSION OS ARCH VARIANT — a tiny tar.gz with a fake pinkdb and a
# SHA256SUMS entry for its version directory.
make_asset() {
    version=$1
    os=$2
    arch=$3
    variant=$4
    stem="pinkdb-$version-$os-$arch-$variant"
    build="$tmp/build/$stem"
    mkdir -p "$build" "$serve/download/v$version"
    cat >"$build/pinkdb" <<EOF
#!/bin/sh
echo "pinkdb $version ($os-$arch-$variant) fake"
EOF
    chmod 755 "$build/pinkdb"
    tar -czf "$serve/download/v$version/$stem.tar.gz" -C "$tmp/build" "$stem"
    if [ ! -f "$serve/download/v$version/SHA256SUMS" ]; then
        : >"$serve/download/v$version/SHA256SUMS"
    fi
    printf '%s  %s\n' "$(sha256_of "$serve/download/v$version/$stem.tar.gz")" \
        "$stem.tar.gz" >>"$serve/download/v$version/SHA256SUMS"
}

make_asset 1.2.3 darwin aarch64 default
make_asset 0.9.0 linux x86_64 lean
make_asset 0.9.0 darwin aarch64 pinklake-s3
make_asset 0.9.0 darwin aarch64 default
make_asset 3.0.0 darwin aarch64 default
make_asset 4.0.0 darwin aarch64 default

# Version 3: deliberately wrong checksum. Version 4: no checksum manifest.
stem3="pinkdb-3.0.0-darwin-aarch64-default.tar.gz"
printf '%s  %s\n' \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "$stem3" >"$serve/download/v3.0.0/SHA256SUMS"
rm -f "$serve/download/v4.0.0/SHA256SUMS"

# A free port and a static file server for the fake releases.
port=$(python3 - <<'PY'
import socket
with socket.socket() as s:
    s.bind(("127.0.0.1", 0))
    print(s.getsockname()[1])
PY
)
(cd "$serve" && python3 -m http.server "$port" --bind 127.0.0.1 >"$tmp/server.log" 2>&1) &
server_pid=$!

i=0
while [ "$i" -lt 100 ]; do
    if python3 - "$port" <<'PY' 2>/dev/null
import socket
import sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=1):
    pass
PY
    then
        break
    fi
    sleep 0.1
    i=$((i + 1))
done
[ "$i" -lt 100 ] || {
    cat "$tmp/server.log" >&2
    echo "error: fake release server did not start" >&2
    exit 1
}

api_base="http://127.0.0.1:$port/api"
releases_base="http://127.0.0.1:$port/download"
mkdir -p "$tmp/tmpdir"
TEST_OS=darwin
TEST_ARCH=aarch64

pass=0
failed=0
fail() {
    echo "FAIL: $*" >&2
    failed=$((failed + 1))
}
ok() {
    echo "ok   $*"
    pass=$((pass + 1))
}

run_install() {
    env \
        PINKDB_API_BASE="$api_base" \
        PINKDB_RELEASES_BASE="$releases_base" \
        PINKDB_OS="$TEST_OS" \
        PINKDB_ARCH="$TEST_ARCH" \
        TMPDIR="$tmp/tmpdir" \
        sh "$install_sh" "$@"
}

expect_success() {
    label=$1
    shift
    if output=$(run_install "$@" 2>&1); then
        ok "$label"
    else
        status=$?
        printf '%s\n' "$output" >&2
        fail "$label (exit $status)"
    fi
}

expect_failure() {
    label=$1
    needle=$2
    shift 2
    if output=$(run_install "$@" 2>&1); then
        fail "$label (expected failure, got success)"
        return
    fi
    case "$output" in
        *"$needle"*) ok "$label" ;;
        *)
            printf '%s\n' "$output" >&2
            fail "$label (output does not contain '$needle')"
            ;;
    esac
}

expect_installed() {
    label=$1
    path=$2
    needle=$3
    if [ ! -x "$path" ]; then
        fail "$label: $path is missing or not executable"
        return
    fi
    if ! "$path" | grep -Fq "$needle"; then
        fail "$label: $path did not print '$needle'"
        return
    fi
    ok "$label"
}

# 1. Latest, default variant, PATH guidance.
prefix1="$tmp/prefix1"
expect_success "latest install" --prefix "$prefix1"
expect_installed "latest binary" "$prefix1/bin/pinkdb" "pinkdb 1.2.3 (darwin-aarch64-default)"
if output=$(run_install --prefix "$tmp/noop" --dry-run 2>&1); then
    case "$output" in
        *"pinkdb-1.2.3-darwin-aarch64-default.tar.gz"*) ok "dry-run resolves latest asset" ;;
        *) fail "dry-run did not name the asset" ;;
    esac
    [ ! -e "$tmp/noop" ] || fail "dry-run created the prefix"
else
    fail "dry-run failed"
fi

# 2. Pinned version and variant mapping (pinklake,s3 -> pinklake-s3).
prefix2="$tmp/prefix2"
TEST_OS=linux
TEST_ARCH=x86_64
expect_success "pinned linux/lean" --version 0.9.0 --variant lean --prefix "$prefix2"
expect_installed "pinned binary" "$prefix2/bin/pinkdb" "pinkdb 0.9.0 (linux-x86_64-lean)"
TEST_OS=darwin
TEST_ARCH=aarch64
prefix3="$tmp/prefix3"
expect_success "cargo variant spelling" --version v0.9.0 --variant "pinklake,s3" --prefix "$prefix3"
expect_installed "mapped variant binary" "$prefix3/bin/pinkdb" "darwin-aarch64-pinklake-s3"
prefix_env="$tmp/prefix-env"
if output=$(PINKDB_VERSION=0.9.0 run_install --prefix "$prefix_env" 2>&1); then
    expect_installed "PINKDB_VERSION env" "$prefix_env/bin/pinkdb" "pinkdb 0.9.0"
else
    fail "PINKDB_VERSION env install"
fi
prefix_prefix_env="$tmp/prefix-prefix-env"
TEST_OS=linux
TEST_ARCH=x86_64
export PREFIX=$prefix_prefix_env
export PINKDB_VARIANT=lean
expect_success "PREFIX + PINKDB_VARIANT env" --version 0.9.0
unset PREFIX PINKDB_VARIANT
TEST_OS=darwin
TEST_ARCH=aarch64
expect_installed "PREFIX env binary" "$prefix_prefix_env/bin/pinkdb" "linux-x86_64-lean"

# 3. Tampered checksum and missing checksum fail closed.
expect_failure "tampered checksum" "checksum mismatch" --version 3.0.0 --prefix "$tmp/bad"
[ ! -e "$tmp/bad/bin/pinkdb" ] || fail "tampered install left a binary"
expect_failure "missing checksums" "refusing to install without checksums" \
    --version 4.0.0 --prefix "$tmp/bad2"
[ ! -e "$tmp/bad2/bin/pinkdb" ] || fail "checksum-less install left a binary"

# 4. Unsupported platform / variant.
TEST_OS=plan9
expect_failure "unsupported OS override" "unsupported operating system" --dry-run
TEST_OS=darwin
TEST_ARCH=riscv64
expect_failure "unsupported arch override" "unsupported architecture" --dry-run
TEST_ARCH=aarch64
expect_failure "unsupported variant" "unsupported variant" --variant s3 --dry-run
expect_failure "empty prefix" "must not be empty" --prefix "" --dry-run

# 5. Existing install refusal and --force.
prefix4="$tmp/prefix4"
expect_success "first install for refusal test" --prefix "$prefix4"
if output=$(run_install --prefix "$prefix4" 2>&1); then
    fail "second install should refuse an existing binary"
else
    case "$output" in
        *"already exists"*) ok "existing install refused" ;;
        *) fail "existing-install refusal message was unclear" ;;
    esac
fi
expect_success "force replaces existing install" --force --prefix "$prefix4"

# 6. Prefix inside PATH: no PATH guidance. Prefix outside PATH: guidance.
prefix5="$tmp/prefix5"
saved_path=$PATH
PATH="$prefix5/bin:$saved_path"
if output=$(run_install --prefix "$prefix5" 2>&1); then
    case "$output" in
        *"not on your PATH"*) fail "PATH guidance printed for a prefix already on PATH" ;;
        *) ok "no PATH guidance when the prefix is on PATH" ;;
    esac
else
    fail "PATH-prefix install"
fi
PATH=$saved_path
if output=$(run_install --prefix "$tmp/prefix6" 2>&1); then
    case "$output" in
        *"not on your PATH"*) ok "PATH guidance when the prefix is absent from PATH" ;;
        *) fail "missing PATH guidance" ;;
    esac
else
    fail "PATH-guidance install"
fi

# 7. Temporary directories cleaned up (the harness owns TMPDIR).
leftover=$(find "$tmp/tmpdir" -mindepth 1 -maxdepth 1 -name 'pinkdb-install.*' -print 2>/dev/null | wc -l | tr -d ' ')
if [ "$leftover" = "0" ]; then
    ok "temporary directories cleaned up"
else
    fail "installer left $leftover temporary directories"
fi

# 8. Shell syntax.
if sh -n "$install_sh"; then ok "sh -n install.sh"; else fail "sh -n install.sh"; fi

echo
echo "installer harness: $pass passed, $failed failed"
[ "$failed" -eq 0 ]
