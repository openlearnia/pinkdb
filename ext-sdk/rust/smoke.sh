#!/usr/bin/env bash
#
# pinkdb Rust extension SDK smoke test.
#
# End-to-end proof that the SDK works against a real host:
#   1. checks the native-export invariant (`check-native-exports.sh`):
#      linked-only rlibs carry no `pinkdb_extension_*` globals, the example
#      cdylib exports all three;
#   2. builds the release CLI (`cargo build --release -p pinkdb-cli`);
#   3. builds the worked example with the SDK Makefile
#      (target/release/librust_string_ext.dylib — one `pinkdb_extension!`
#      declaration plus the explicit `pinkdb_abi_exports!()` opt-in);
#   4. runs a deterministic SQL session in a scratch directory that loads the
#      extension via `LOAD EXTENSION '<abs path>'` and exercises the
#      registered functions: VARCHAR results (borrowed `&str` in, owned
#      `String` out), NULL handling through `Option`, bytes-vs-characters
#      length, a DATE round trip, composition with built-ins, the error path,
#      the volatile-function refusal, and a WAL-logged deterministic INSERT;
#   5. greps the expected outputs and exits non-zero on any mismatch.
#
# `cargo test -p pinkdb-ext-sdk` covers the same example through the SDK's own
# native round-trip test (compiled with rustc) plus the linked model and a
# durable replay; this script is the CLI-visible end-to-end proof, like the
# C/C++ and Go smoke scripts.
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

# Linked-only extensions (the rlibs every in-repo core extension builds into
# the host) must carry no `pinkdb_extension_*` globals; only a cdylib opts in.
# This is a link-level invariant the macOS linker cannot prove (it tolerates
# duplicates), so check the artifacts directly before the end-to-end session.
echo "==> ./ext-sdk/rust/check-native-exports.sh"
"$here/check-native-exports.sh"

echo "==> cargo build --release -p pinkdb-cli"
(cd "$repo" && cargo build --release -p pinkdb-cli)

echo "==> make -C ext-sdk/rust (example)"
make -C "$here" -s all

lib="$repo/target/release/librust_string_ext.$so_ext"
if [ ! -f "$lib" ]; then
    echo "FAIL: expected example library is missing: $lib" >&2
    exit 1
fi

# One scratch session directory; cleaned up whatever happens. (TMPDIR on
# macOS carries a trailing slash — strip it so the mktemp template is tidy.)
scratch_root="${TMPDIR:-/tmp}"
scratch_root="${scratch_root%/}"
tmp="$(mktemp -d "$scratch_root/pinkdb-rust-ext-sdk-smoke.XXXXXX")"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT

# The SQL session. Each CASE check returns a unique marker string so the grep
# below proves exactly which behavior matched. ABI v1 is strictly typed: a
# bare NULL literal is typeless, but non-NULL INTEGER literals need an
# explicit CAST to BIGINT.
statements=(
    "LOAD EXTENSION '$lib';"

    # Display queries: what the report shows. Values are grepped below.
    "SELECT ext_rs_repeat('ab', CAST(3 AS BIGINT)) AS repeated, ext_rs_len('héllo') AS bytes, length('héllo') AS chars;"
    "SELECT ext_rs_clamp(CAST(5 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT)) AS clamped, ext_rs_next_day(DATE '2024-02-28') AS next_day;"

    # Behavior checks, one marker each.
    "SELECT CASE WHEN ext_rs_repeat('ab', CAST(3 AS BIGINT)) = 'ababab' AND ext_rs_repeat('héllo', CAST(2 AS BIGINT)) = 'héllohéllo' AND ext_rs_repeat('ab', CAST(0 AS BIGINT)) IS NOT NULL AND length(ext_rs_repeat('ab', CAST(0 AS BIGINT))) = 0 THEN 'repeat ok' ELSE 'repeat FAIL' END;"
    "SELECT CASE WHEN ext_rs_len('') = 0 AND ext_rs_len('abc') = 3 AND ext_rs_len('héllo') = 6 AND ext_rs_len('héllo') <> length('héllo') THEN 'len ok' ELSE 'len FAIL' END;"
    "SELECT CASE WHEN ext_rs_repeat(NULL, CAST(3 AS BIGINT)) IS NULL AND ext_rs_repeat('x', NULL) IS NOT NULL AND ext_rs_len(NULL) IS NULL AND ext_rs_next_day(NULL) IS NULL THEN 'null ok' ELSE 'null FAIL' END;"
    "SELECT CASE WHEN ext_rs_clamp(CAST(5 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT)) = 5 AND ext_rs_clamp(CAST(-5 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT)) = 0 AND ext_rs_clamp(CAST(50 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT)) = 10 THEN 'clamp ok' ELSE 'clamp FAIL' END;"
    "SELECT CASE WHEN ext_rs_next_day(DATE '2024-02-28') = DATE '2024-02-29' THEN 'day ok' ELSE 'day FAIL' END;"
    "SELECT CASE WHEN upper(ext_rs_repeat('ab', CAST(2 AS BIGINT))) = 'ABAB' THEN 'compose ok' ELSE 'compose FAIL' END;"

    # A deterministic extension statement is WAL-loggable (here: a plain INSERT
    # in the session; the durable replay proof is in the SDK's cargo tests).
    "CREATE TABLE src (i BIGINT);"
    "INSERT INTO src VALUES (1), (5), (9);"
    "CREATE TABLE t (i BIGINT);"
    "INSERT INTO t SELECT ext_rs_clamp(i, CAST(0 AS BIGINT), CAST(3 AS BIGINT)) FROM src;"
    "SELECT CASE WHEN COUNT(*) = 3 AND SUM(i) = 7 THEN 'insert ok' ELSE 'insert FAIL' END AS marker FROM t;"

    # The volatile function works in a read-only statement...
    "SELECT CASE WHEN ext_rs_tick() >= 0 THEN 'tick ok' ELSE 'tick FAIL' END;"
    # ...and is refused in a statement the engine WAL-logs, before it runs.
    "INSERT INTO t VALUES (ext_rs_tick());"

    # Error paths: the repeat cap and a NULL into a plain (non-Option)
    # parameter are both code 1; ABI v1 is strictly typed.
    "SELECT ext_rs_repeat('x', CAST(100000 AS BIGINT));"
    "SELECT ext_rs_clamp(NULL, CAST(1 AS BIGINT), CAST(2 AS BIGINT));"
    "SELECT ext_rs_len(1);"
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

check "rust_string_ext loaded"        "Extension rust_string_ext loaded."
check "repeat/len display row"        "│ ababab   │ 6      │ 5       │"
check "clamp/next_day display row"    "│ 5       │ 2024-02-29 │"
check_marker "repeat applied"         "repeat ok"
check_marker "byte length applied"    "len ok"
check_marker "NULLs through Option"   "null ok"
check_marker "clamp applied"          "clamp ok"
check_marker "DATE round trip"        "day ok"
check_marker "built-in composition"   "compose ok"
check_marker "deterministic INSERT"   "insert ok"
check_marker "volatile read allowed"  "tick ok"
check "volatile INSERT refused"       "ext_rs_tick() is volatile (nondeterministic) and cannot be called in a statement the engine WAL-logs"
check "repeat cap error (code 1)"     "extension function ext_rs_repeat() failed with code 1"
check "NULL into plain param (code 1)" "extension function ext_rs_clamp() failed with code 1"
check "strict argument typing"        "ext_rs_len argument 1 must be VARCHAR, got INTEGER"

if [ "$failures" -ne 0 ]; then
    printf 'FAILED: %d of %d checks failed\n' "$failures" "$((checks + failures))" >&2
    exit 1
fi
printf 'all %d checks passed\n' "$checks"
