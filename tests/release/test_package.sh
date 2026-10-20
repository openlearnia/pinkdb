#!/usr/bin/env bash
#
# Fast local tests for the release tooling that do not need a cargo build:
#
#   * package.sh names/content/checksum for CLI and server variants;
#   * manifest.sh generation + validation, and a tampered sidecar failing;
#   * matrix.py row counts and fields;
#   * extensions.py discovery + manifest staging.
#
# Run: tests/release/test_package.sh
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
package="$repo_root/scripts/release/package.sh"
manifest="$repo_root/scripts/release/manifest.sh"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/pinkdb-release-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

pass=0
failed=0
ok() {
    echo "ok   $*"
    pass=$((pass + 1))
}
fail() {
    echo "FAIL: $*" >&2
    failed=$((failed + 1))
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

fake="$tmp/pinkdb"
cat >"$fake" <<'EOF'
#!/bin/sh
echo "fake pinkdb"
EOF
chmod 755 "$fake"

# ---- package.sh: CLI lean on a pinned target --------------------------------
out1="$tmp/dist1"
if output=$("$package" --kind cli --variant lean --version 9.9.9 \
    --target x86_64-unknown-linux-gnu --bin "$fake" --skip-smoke --out "$out1" 2>&1); then
    asset="pinkdb-9.9.9-linux-x86_64-lean.tar.gz"
    if [ -f "$out1/$asset" ]; then ok "cli asset name: $asset"; else fail "missing $asset"; fi
    case "$output" in
        *"asset: $asset"*) ok "package.sh prints the asset name" ;;
        *) fail "package.sh did not print 'asset: $asset'" ;;
    esac
    if tar -tzf "$out1/$asset" | grep -qx "pinkdb-9.9.9-linux-x86_64-lean/pinkdb"; then
        ok "archive contains the top-level binary"
    else
        fail "archive layout is wrong"
    fi
    if tar -tzf "$out1/$asset" | grep -qx "pinkdb-9.9.9-linux-x86_64-lean/README.md"; then
        ok "archive includes README.md"
    else
        fail "archive is missing README.md"
    fi
    expected=$(sha256_of "$out1/$asset")
    sidecar=$(awk '{print $1}' "$out1/$asset.sha256")
    if [ "$expected" = "$sidecar" ]; then
        ok ".sha256 matches"
    else
        fail ".sha256 mismatch"
    fi
    if "$manifest" "$out1" >/dev/null && "$manifest" --check "$out1" >/dev/null; then
        ok "manifest.sh generate + check"
    else
        fail "manifest.sh generate + check"
    fi
    # Tamper with the sidecar: --check must refuse it.
    printf '%064d  %s\n' 0 "$asset" >"$out1/$asset.sha256"
    if "$manifest" --check "$out1" >/dev/null 2>&1; then
        fail "manifest.sh accepted a tampered sidecar"
    else
        ok "manifest.sh rejects a tampered sidecar"
    fi
else
    printf '%s\n' "$output" >&2
    fail "package.sh cli"
fi

# ---- package.sh: server tls -------------------------------------------------
out2="$tmp/dist2"
if output=$("$package" --kind server --variant tls --version 9.9.9 \
    --target aarch64-unknown-linux-gnu --bin "$fake" --skip-smoke --out "$out2" 2>&1); then
    asset2="pinkdb-server-9.9.9-linux-aarch64-tls.tar.gz"
    if [ -f "$out2/$asset2" ]; then
        ok "server asset name: $asset2"
    else
        fail "missing $asset2"
    fi
else
    printf '%s\n' "$output" >&2
    fail "package.sh server"
fi

# Unknown variants must be rejected before any build.
if "$package" --kind cli --variant s3 --bin "$fake" --skip-smoke --out "$tmp/nope" >/dev/null 2>&1; then
    fail "package.sh accepted an out-of-matrix variant"
else
    ok "package.sh rejects out-of-matrix variants"
fi

# ---- matrix.py --------------------------------------------------------------
if python3 "$repo_root/scripts/release/matrix.py" --kind all >"$tmp/matrix.json"; then
    if python3 - "$tmp/matrix.json" <<'PY'
import json
import sys

rows = json.load(open(sys.argv[1], encoding="utf-8"))
cli = [r for r in rows if r["kind"] == "cli"]
server = [r for r in rows if r["kind"] == "server"]
assert len(cli) == 24, len(cli)
assert len(server) == 12, len(server)
assert {r["variant"] for r in cli} == {
    "default", "lean", "pinklake", "pinklake-s3", "vortexlake", "vortexlake-s3"
}
assert {r["platform"] for r in rows} == {
    "linux-x86_64", "linux-aarch64", "darwin-aarch64", "darwin-x86_64"
}
assert all(r["runner"] for r in rows)
assert [r for r in rows if r["platform"] == "darwin-x86_64"][0]["target"] == "x86_64-apple-darwin"
PY
    then
        ok "matrix.py: 24 CLI + 12 server rows"
    else
        fail "matrix.py row validation"
    fi
else
    fail "matrix.py"
fi

if python3 "$repo_root/scripts/release/matrix.py" --kind extensions >"$tmp/ext-matrix.json" &&
    python3 - "$tmp/ext-matrix.json" <<'PY'
import json
import sys

rows = json.load(open(sys.argv[1], encoding="utf-8"))
assert [r["platform"] for r in rows] == [
    "linux-x64", "linux-arm64", "darwin-arm64", "darwin-x64"
]
assert [r for r in rows if r["platform"] == "darwin-x64"][0]["target"] == "x86_64-apple-darwin"
PY
then
    ok "matrix.py: extension platforms"
else
    fail "matrix.py extension platforms"
fi

# ---- extensions.py discovery ------------------------------------------------
extroot="$tmp/extroot"
mkdir -p "$extroot/ext-sdk/c/examples" "$extroot/ext-sdk/c/build" \
    "$extroot/extensions/foo/build"
: >"$extroot/ext-sdk/c/examples/math_ext.c"
: >"$extroot/ext-sdk/c/build/math_ext.dylib"
: >"$extroot/extensions/foo/build/libfoo.dylib"
printf 'Foo helpers\n' >"$extroot/extensions/foo/description.txt"
if python3 "$repo_root/scripts/release/extensions.py" discover \
    --platform darwin-arm64 --version 9.9.9 --root "$extroot" --out "$tmp/extout" \
    >/dev/null 2>&1; then
    if python3 - "$tmp/extout/manifest.tsv" <<'PY'
import sys

rows = [line.rstrip("\n").split("\t") for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
assert len(rows) == 2, rows
by_name = {r[0]: r for r in rows}
assert by_name["math_ext"][3] == "math_ext.dylib"
assert by_name["foo"][3] == "libfoo.dylib"
assert by_name["foo"][4] == "Foo helpers"
assert all(len(r) == 6 and len(r[5]) == 64 for r in rows)
PY
    then
        ok "extensions.py stages C example + extensions/foo"
    else
        fail "extensions.py manifest contents"
    fi
else
    fail "extensions.py discovery"
fi

echo
echo "release tooling tests: $pass passed, $failed failed"
[ "$failed" -eq 0 ]
