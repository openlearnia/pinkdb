#!/usr/bin/env bash
#
# pinkdb C/C++ extension SDK smoke test.
#
# End-to-end proof that the SDK works against a real host:
#   1. builds the release CLI (`cargo build --release -p pinkdb-cli`);
#   2. builds the three examples with the SDK Makefile (build/*.dylib);
#   3. runs a deterministic SQL session in a scratch directory that loads
#      each example via `LOAD EXTENSION '<path>'` and exercises the
#      registered functions: numeric results, NULL inputs, a VARCHAR result
#      (allocated with api->alloc), and both error paths;
#   4. greps the expected outputs and exits non-zero on any mismatch.
#
# The scratch directory is removed on exit (including on failure).
set -euo pipefail
# `LOAD EXTENSION` is off by default (security review C-2): this smoke script
# deliberately loads native code, so it opts in explicitly.
export PINKDB_ALLOW_LOAD_EXTENSION=1

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
cli="$repo/target/release/pinkdb"

# Shared-library suffix: .dylib on macOS, .so elsewhere.
case "$(uname -s)" in
    Darwin) so_ext="dylib" ;;
    *)      so_ext="so" ;;
esac

echo "==> cargo build --release -p pinkdb-cli"
(cd "$repo" && cargo build --release -p pinkdb-cli)

echo "==> make -C ext-sdk/c (examples)"
make -C "$here" -s all

math="$here/build/math_ext.$so_ext"
string="$here/build/string_ext.$so_ext"
cpp="$here/build/cpp_ext.$so_ext"
for lib in "$math" "$string" "$cpp"; do
    if [ ! -f "$lib" ]; then
        echo "FAIL: expected example library is missing: $lib" >&2
        exit 1
    fi
done

# One scratch session directory; cleaned up whatever happens. (TMPDIR on
# macOS carries a trailing slash — strip it so the mktemp template is tidy.)
scratch_root="${TMPDIR:-/tmp}"
scratch_root="${scratch_root%/}"
tmp="$(mktemp -d "$scratch_root/pinkdb-ext-sdk-smoke.XXXXXX")"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT

# The SQL session. Each CASE check returns a unique marker string so the
# grep below proves exactly which behavior matched. Bare NULL literals are
# typeless and pass through for any registered argument type; non-NULL
# BIGINT/DOUBLE arguments need an explicit CAST (ABI v1 is strictly typed).
statements=(
    "LOAD EXTENSION '$math';"
    "LOAD EXTENSION '$string';"
    "LOAD EXTENSION '$cpp';"

    # Display queries: what the report shows. Values are grepped below.
    "SELECT ext_clamp(CAST(5 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT)) AS clamped, ext_deg2rad(CAST(180 AS DOUBLE)) AS radians;"
    "SELECT ext_repeat('ab', 3) AS repeated, ext_title('hello wORLD') AS titled;"
    "SELECT ext_add_checked(CAST(2 AS BIGINT), CAST(3 AS BIGINT)) AS sum;"

    # Numeric behavior, NULL handling, VARCHAR ownership and error paths.
    "SELECT CASE WHEN ext_clamp(CAST(5 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT)) = 5 AND ext_clamp(CAST(-5 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT)) = 0 AND ext_clamp(CAST(50 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT)) = 10 THEN 'clamp ok' ELSE 'clamp FAIL' END;"
    "SELECT CASE WHEN ext_deg2rad(CAST(180 AS DOUBLE)) > 3.14159 AND ext_deg2rad(CAST(180 AS DOUBLE)) < 3.1416 AND ext_deg2rad(CAST(0 AS DOUBLE)) = 0.0 THEN 'degrees ok' ELSE 'degrees FAIL' END;"
    "SELECT CASE WHEN ext_clamp(NULL, CAST(0 AS BIGINT), CAST(10 AS BIGINT)) IS NULL AND ext_deg2rad(NULL) IS NULL AND ext_repeat(NULL, 3) IS NULL AND ext_repeat('x', NULL) IS NULL AND ext_title(NULL) IS NULL AND ext_add_checked(NULL, CAST(5 AS BIGINT)) IS NULL THEN 'null ok' ELSE 'null FAIL' END;"
    "SELECT CASE WHEN ext_repeat('ab', 3) = 'ababab' AND ext_repeat('héllo', 2) = 'héllohéllo' AND ext_repeat('ab', 0) IS NOT NULL AND length(ext_repeat('ab', 0)) = 0 AND upper(ext_repeat('ab', 2)) = 'ABAB' THEN 'repeat ok' ELSE 'repeat FAIL' END;"
    "SELECT CASE WHEN ext_title('hello wORLD') = 'Hello World' AND ext_title('pinkdb') = 'Pinkdb' THEN 'title ok' ELSE 'title FAIL' END;"
    "SELECT CASE WHEN ext_add_checked(CAST(2 AS BIGINT), CAST(3 AS BIGINT)) = 5 AND ext_add_checked(9223372036854775807, CAST(0 AS BIGINT)) = 9223372036854775807 AND ext_add_checked(CAST(-2 AS BIGINT), CAST(-3 AS BIGINT)) = -5 THEN 'add ok' ELSE 'add FAIL' END;"
    "SELECT ext_add_checked(9223372036854775807, CAST(1 AS BIGINT));"
    "SELECT ext_clamp(CAST(1 AS BIGINT), CAST(10 AS BIGINT), CAST(0 AS BIGINT));"
)

echo "==> running the session in $tmp"
# The session deliberately includes failing statements (error paths), so
# the CLI exits 1 by the scripting convention; anything else (a crash, a
# signal) is a failure.
status=0
out="$(printf '%s\n' "${statements[@]}" ".quit" | (cd "$tmp" && "$cli") 2>&1)" || status=$?
if [ "$status" -ne 0 ] && [ "$status" -ne 1 ]; then
    printf 'FAIL: pinkdb exited with %s\n--- output:\n%s\n' "$status" "$out" >&2
    exit 1
fi

echo "--- session output ---"
printf '%s\n' "$out"
echo "----------------------"

checks=0
failures=0
check() { # check <description> <needle>: output must contain the literal text
    if printf '%s' "$out" | grep -qF -- "$2"; then
        printf 'ok: %s\n' "$1"
        checks=$((checks + 1))
    else
        printf 'FAIL: %s (expected output to contain: %s)\n' "$1" "$2" >&2
        failures=$((failures + 1))
    fi
}

check_marker() { # check_marker <description> <marker>: one result ROW is exactly the marker
    # The echoed SQL in the column header contains the marker text too, so the
    # positive check must match a whole table row (`│ marker ...spaces... │`).
    if printf '%s\n' "$out" | grep -Eq "^│ ${2} +│$"; then
        printf 'ok: %s\n' "$1"
        checks=$((checks + 1))
    else
        printf 'FAIL: %s (expected a result row "│ %s │")\n' "$1" "$2" >&2
        failures=$((failures + 1))
    fi
}

check "math_ext loaded"            "Extension math_ext loaded."
check "string_ext loaded"          "Extension string_ext loaded."
check "cpp_ext loaded"             "Extension cpp_ext loaded."
check_marker "ext_clamp applied"       "clamp ok"
check_marker "ext_deg2rad applied"     "degrees ok"
check_marker "NULL inputs -> NULL"     "null ok"
check_marker "ext_repeat + ownership"  "repeat ok"
check_marker "ext_title applied"       "title ok"
check_marker "ext_add_checked applied" "add ok"
check "ext_clamp/deg2rad display row"  "│ 5       │ 3.141592653589793 │"
check "VARCHAR display row"            "│ ababab   │ Hello World │"
check "ext_add_checked display row"    "│ 5      │"
check "ext_add_checked overflow error" "ext_add_checked() failed with code 1"
check "ext_clamp lo > hi error"        "ext_clamp() failed with code 1"

if [ "$failures" -ne 0 ]; then
    printf 'FAILED: %d of %d checks failed\n' "$failures" "$((checks + failures))" >&2
    exit 1
fi
printf 'all %d checks passed\n' "$checks"
