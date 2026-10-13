#!/usr/bin/env bash
#
# pinkdb Go extension SDK smoke test.
#
# End-to-end proof that the SDK works against a real host:
#   1. builds the release CLI (`cargo build --release -p pinkdb-cli`);
#   2. builds the example with the SDK Makefile (build/string_ext.dylib);
#   3. runs a deterministic SQL session in a scratch directory that loads the
#      extension via `LOAD EXTENSION '<abs path>'` and exercises the
#      registered functions: VARCHAR results, NULL inputs, a multibyte
#      round trip, composition with built-ins, both error paths, and
#      multi-row queries over 5,000 rows;
#   4. greps the expected outputs and exits non-zero on any mismatch.
#
# The 5,000-row queries go through the executor's parallel filter/projection
# paths: the callbacks are invoked from the host's rayon worker threads
# (threads the Go runtime did not create), which is exactly the concurrency
# contract the ABI documents.
#
# The scratch directory is removed on exit (including on failure).
set -euo pipefail

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

echo "==> make -C ext-sdk/go (example)"
make -C "$here" -s all

lib="$here/build/string_ext.$so_ext"
if [ ! -f "$lib" ]; then
    echo "FAIL: expected example library is missing: $lib" >&2
    exit 1
fi

# One scratch session directory; cleaned up whatever happens. (TMPDIR on
# macOS carries a trailing slash — strip it so the mktemp template is tidy.)
scratch_root="${TMPDIR:-/tmp}"
scratch_root="${scratch_root%/}"
tmp="$(mktemp -d "$scratch_root/pinkdb-go-ext-sdk-smoke.XXXXXX")"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT

# 5,000 rows, one INSERT. The parallel filter/projection paths below then run
# ext_go_len/ext_go_repeat callbacks from multiple rayon workers at once.
rows="$(awk 'BEGIN { for (i = 1; i <= 5000; i++) printf "%s(%d)", (i == 1 ? "" : ","), i }')"

# The SQL session. Each CASE check returns a unique marker string so the grep
# below proves exactly which behavior matched. Bare NULL literals are typeless
# and pass through for any registered argument type; non-NULL BIGINT/INTEGER
# arguments need an explicit CAST (ABI v1 is strictly typed).
statements=(
    "LOAD EXTENSION '$lib';"
    "CREATE TABLE t (i BIGINT);"
    "INSERT INTO t VALUES $rows;"

    # Display queries: what the report shows. Values are grepped below.
    "SELECT ext_go_repeat('ab', 3) AS repeated, ext_go_len('héllo') AS bytes, length('héllo') AS chars;"
    "SELECT ext_go_repeat('héllo', 2) AS multi, upper(ext_go_repeat('ab', 2)) AS up;"
    "SELECT COUNT(*) AS rows_ok FROM t WHERE ext_go_len(CAST(i AS VARCHAR)) > 0;"
    "SELECT SUM(ext_go_len(CAST(i AS VARCHAR))) AS total_bytes FROM t;"

    # NULL handling, VARCHAR ownership and semantics.
    "SELECT CASE WHEN ext_go_repeat(NULL, 3) IS NULL AND ext_go_repeat('x', NULL) IS NULL AND ext_go_len(NULL) IS NULL THEN 'null ok' ELSE 'null FAIL' END;"
    "SELECT CASE WHEN ext_go_repeat('ab', 3) = 'ababab' AND ext_go_repeat('héllo', 2) = 'héllohéllo' AND ext_go_repeat('ab', 0) IS NOT NULL AND length(ext_go_repeat('ab', 0)) = 0 AND upper(ext_go_repeat('ab', 2)) = 'ABAB' THEN 'repeat ok' ELSE 'repeat FAIL' END;"
    "SELECT CASE WHEN ext_go_len('') = 0 AND ext_go_len('abc') = 3 AND ext_go_len('héllo') = 6 AND ext_go_len('héllo') <> length('héllo') THEN 'len ok' ELSE 'len FAIL' END;"
    "SELECT CASE WHEN ext_go_len(upper('abc')) = 3 AND length(ext_go_repeat('ab', 3)) = 6 THEN 'compose ok' ELSE 'compose FAIL' END;"

    # 5,000 rows. Digit counts: 1..9 = 1 digit (9 rows), 10..99 = 2 (180),
    # 100..999 = 3 (2700), 1000..5000 = 4 (4001 rows) — 18,893 bytes total.
    "SELECT CASE WHEN COUNT(*) = 5000 THEN 'rows ok' ELSE 'rows FAIL' END AS marker FROM t WHERE ext_go_len(CAST(i AS VARCHAR)) > 0;"
    "SELECT CASE WHEN SUM(ext_go_len(CAST(i AS VARCHAR))) = 18893 THEN 'bytes ok' ELSE 'bytes FAIL' END AS marker FROM t;"
    "SELECT CASE WHEN COUNT(*) = 4001 THEN 'digits ok' ELSE 'digits FAIL' END AS marker FROM t WHERE ext_go_len(CAST(i AS VARCHAR)) = 4;"
    "SELECT CASE WHEN COUNT(*) = 5000 THEN 'repeat rows ok' ELSE 'repeat rows FAIL' END AS marker FROM (SELECT ext_go_repeat(CAST(i AS VARCHAR), 2) AS r FROM t WHERE ext_go_len(CAST(i AS VARCHAR)) > 0) AS d;"

    # Error paths: the repeat cap is error code 2; ABI v1 is strictly typed.
    "SELECT ext_go_repeat('ab', 1000000);"
    "SELECT ext_go_len(1);"
)

echo "==> running the session in $tmp"
if ! out="$(printf '%s\n' "${statements[@]}" ".quit" | (cd "$tmp" && "$cli") 2>&1)"; then
    printf 'FAIL: pinkdb exited non-zero\n--- output:\n%s\n' "$out" >&2
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

check "string_ext loaded"             "Extension string_ext loaded."
check "ext_go_repeat/ext_go_len row"  "│ ababab   │ 6      │ 5       │"
check "multibyte + built-in upper"    "│ héllohéllo │ ABAB    │"
check "5,000-row display row"         "│ 5000    │"
check "5,000-row byte-sum row"        "│ 18893       │"
check_marker "NULL inputs -> NULL"    "null ok"
check_marker "ext_go_repeat applied"  "repeat ok"
check_marker "ext_go_len applied"     "len ok"
check_marker "built-in composition"   "compose ok"
check_marker "5,000 rows filtered"    "rows ok"
check_marker "5,000 rows byte sum"    "bytes ok"
check_marker "4-digit row count"      "digits ok"
check_marker "5,000 repeats projected" "repeat rows ok"
check "ext_go_repeat cap error"       "ext_go_repeat() failed with code 2"
check "strict argument typing"        "ext_go_len argument 1 must be VARCHAR, got INTEGER"

if [ "$failures" -ne 0 ]; then
    printf 'FAILED: %d of %d checks failed\n' "$failures" "$((checks + failures))" >&2
    exit 1
fi
printf 'all %d checks passed\n' "$checks"
