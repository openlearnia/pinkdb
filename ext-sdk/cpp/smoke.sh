#!/usr/bin/env bash
#
# pinkdb C++ extension SDK smoke test.
#
# End-to-end proof that the wrapper works against a real host:
#   1. builds the release CLI (`cargo build --release -p pinkdb-cli`);
#   2. builds the example and the conformance fixture with the SDK Makefile;
#   3. runs one SQL session that loads both libraries and exercises the
#      example: ext_add_checked (the exact function ext-sdk/c/smoke.sh also
#      asserts on), the VARCHAR path with multibyte input and an empty
#      result, the volatile path (allowed in a plain SELECT, refused in a
#      WAL-logged INSERT), and both error paths;
#   4. asserts the security posture in separate processes: the statement is
#      off without the opt-in, a bare name and a `..` path are refused, and
#      a library outside --extension-root is refused;
#   5. greps the expected output and exits non-zero on any mismatch.
#
# The scratch directory is removed on exit (including on failure).
set -euo pipefail
# `LOAD EXTENSION` is off by default (security review C-2): this smoke script
# deliberately loads native code, so it opts in explicitly.
# Assertions read the CLI's human output; run from a coding agent's shell
# (AI_AGENT, CLAUDECODE, ...) the CLI would switch to agent mode (wave 99).
export PINKDB_AGENT_MODE=0
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

echo "==> make -C ext-sdk/cpp (example + conformance fixture)"
make -C "$here" -s all

cpp="$here/build/cpp_ext.$so_ext"
conf="$here/build/conformance_ext.$so_ext"
for lib in "$cpp" "$conf"; do
    if [ ! -f "$lib" ]; then
        echo "FAIL: expected SDK library is missing: $lib" >&2
        exit 1
    fi
done

# One scratch session directory; cleaned up whatever happens. (TMPDIR on
# macOS carries a trailing slash — strip it so the mktemp template is tidy.)
scratch_root="${TMPDIR:-/tmp}"
scratch_root="${scratch_root%/}"
tmp="$(mktemp -d "$scratch_root/pinkdb-cpp-sdk-smoke.XXXXXX")"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT
mkdir -p "$tmp/root"

# The SQL session. Each CASE check returns a unique marker string so the grep
# below proves exactly which behavior matched. Bare NULL literals are typeless
# and pass through for any registered argument type; non-NULL BIGINT arguments
# need an explicit CAST (ABI v1 is strictly typed).
statements=(
    "LOAD EXTENSION '$cpp';"
    "LOAD EXTENSION '$conf';"

    # Display queries: what the report shows. Values are grepped below.
    "SELECT ext_add_checked(CAST(2 AS BIGINT), CAST(3 AS BIGINT)) AS sum;"
    "SELECT ext_reverse('héllo') AS reversed;"
    "SELECT ext_now() AS now;"

    # Numeric behavior, NULL handling, VARCHAR ownership, volatile semantics.
    "SELECT CASE WHEN ext_add_checked(CAST(2 AS BIGINT), CAST(3 AS BIGINT)) = 5 AND ext_add_checked(9223372036854775807, CAST(0 AS BIGINT)) = 9223372036854775807 AND ext_add_checked(CAST(-2 AS BIGINT), CAST(-3 AS BIGINT)) = -5 THEN 'add ok' ELSE 'add FAIL' END;"
    "SELECT CASE WHEN ext_add_checked(NULL, CAST(5 AS BIGINT)) IS NULL AND ext_add_checked(CAST(5 AS BIGINT), NULL) IS NULL AND ext_reverse(NULL) IS NULL THEN 'null ok' ELSE 'null FAIL' END;"
    "SELECT CASE WHEN ext_reverse('héllo') = 'olléh' AND length(ext_reverse('héllo')) = 5 AND ext_reverse('ab') = 'ba' THEN 'reverse ok' ELSE 'reverse FAIL' END;"
    "SELECT CASE WHEN ext_reverse('') IS NOT NULL AND length(ext_reverse('')) = 0 THEN 'reverse empty ok' ELSE 'reverse empty FAIL' END;"
    "SELECT CASE WHEN ext_now() IS NOT NULL THEN 'volatile read ok' ELSE 'volatile read FAIL' END;"
    "CREATE TABLE t (x TIMESTAMP);"
    "INSERT INTO t VALUES (ext_now());"

    # Error paths: i64 overflow and a volatile call in a WAL-logged statement.
    "SELECT ext_add_checked(9223372036854775807, CAST(1 AS BIGINT));"
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
check_in() { # check_in <description> <needle> <text>: text must contain the literal needle
    if printf '%s' "$3" | grep -qF -- "$2"; then
        printf 'ok: %s\n' "$1"
        checks=$((checks + 1))
    else
        printf 'FAIL: %s (expected output to contain: %s)\n' "$1" "$2" >&2
        failures=$((failures + 1))
    fi
}

check() { check_in "$1" "$2" "$out"; }

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

check "cpp_ext loaded"               "Extension cpp_ext loaded."
check "conformance_ext loaded"       "Extension conformance_ext loaded."
check_marker "ext_add_checked applied"    "add ok"
check_marker "NULL inputs -> NULL"        "null ok"
check_marker "ext_reverse + UTF-8"        "reverse ok"
check_marker "ext_reverse('') is empty"   "reverse empty ok"
check_marker "volatile read in SELECT"    "volatile read ok"
check "ext_add_checked display row"       "│ 5      │"
check_marker "ext_reverse display row"   "olléh"
check "ext_add_checked overflow error"    "ext_add_checked() failed with code 1"
check "volatile refused in WAL-logged stmt" \
    "ext_now() is volatile (nondeterministic) and cannot be called"

# --- security posture ------------------------------------------------------
# (a) The statement is off unless the operator opts in.
deny_status=0
denied="$(env -u PINKDB_ALLOW_LOAD_EXTENSION "$cli" \
    -c "LOAD EXTENSION '$cpp';" 2>&1)" || deny_status=$?
if [ "$deny_status" -ne 1 ]; then
    printf 'FAIL: loading without the opt-in should exit 1, got %s\n--- output:\n%s\n' \
        "$deny_status" "$denied" >&2
    failures=$((failures + 1))
fi
check_in "load refused without opt-in" "LOAD EXTENSION is disabled" "$denied"

# (b) A bare name would go through the dynamic loader's search path; a `..`
# component is an escape vector. Both are refused before any dlopen.
bare_status=0
bare="$(cd "$tmp" && "$cli" -c "LOAD EXTENSION 'cpp_ext.$so_ext';" 2>&1)" || bare_status=$?
if [ "$bare_status" -ne 1 ]; then
    printf 'FAIL: a bare name should exit 1, got %s\n--- output:\n%s\n' \
        "$bare_status" "$bare" >&2
    failures=$((failures + 1))
fi
check_in "bare name refused" \
    "a bare name would be resolved through the dynamic loader's search path" "$bare"

dotdot_status=0
dotdot="$(cd "$tmp" && "$cli" -c "LOAD EXTENSION '$here/build/../build/cpp_ext.$so_ext';" 2>&1)" \
    || dotdot_status=$?
if [ "$dotdot_status" -ne 1 ]; then
    printf 'FAIL: a `..` path should exit 1, got %s\n--- output:\n%s\n' \
        "$dotdot_status" "$dotdot" >&2
    failures=$((failures + 1))
fi
check_in 'a ".." path is refused' 'the path contains a `..` component' "$dotdot"

# (c) --extension-root confines every load to one directory. The example lives
# in <repo>/ext-sdk/cpp/build, outside the scratch root.
root_status=0
confined="$("$cli" --allow-load-extension --extension-root "$tmp/root" \
    -c "LOAD EXTENSION '$cpp';" 2>&1)" || root_status=$?
if [ "$root_status" -ne 1 ]; then
    printf 'FAIL: a library outside the root should exit 1, got %s\n--- output:\n%s\n' \
        "$root_status" "$confined" >&2
    failures=$((failures + 1))
fi
check_in "library outside --extension-root refused" "outside the extension root" "$confined"

if [ "$failures" -ne 0 ]; then
    printf 'FAILED: %d of %d checks failed\n' "$failures" "$((checks + failures))" >&2
    exit 1
fi
printf 'all %d checks passed\n' "$checks"
