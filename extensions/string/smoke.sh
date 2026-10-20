#!/usr/bin/env bash
#
# pinkdb core string extension — end-to-end smoke test against the release
# CLI, through the **native** loading path (`LOAD EXTENSION '<abs path>'`).
#
#   1. builds the release CLI (`cargo build --release -p pinkdb-cli`);
#   2. builds the extension (`make -C extensions/string`, a release cdylib);
#   3. runs a deterministic SQL session in a scratch directory: display
#      queries plus one marker check per behavior (semantics, NULLs, the
#      regex option surface, error paths, a WAL-logged deterministic INSERT);
#   4. greps the expected outputs and exits non-zero on any mismatch.
#
# `cargo test -p pinkdb-ext-string` covers the same surface through the linked
# model; this script is the CLI-visible proof of the native one.
set -euo pipefail
# `LOAD EXTENSION` is off by default (security review C-2): this smoke script
# deliberately loads native code, so it opts in explicitly.
export PINKDB_ALLOW_LOAD_EXTENSION=1

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
cli="$repo/target/release/pinkdb"

case "$(uname -s)" in
    Darwin) so_ext="dylib" ;;
    *)      so_ext="so" ;;
esac

echo "==> cargo build --release -p pinkdb-cli"
(cd "$repo" && cargo build --release -p pinkdb-cli)

echo "==> make -C extensions/string"
make -C "$here" -s all

lib="$repo/target/release/libstring_ext.$so_ext"
if [ ! -f "$lib" ]; then
    echo "FAIL: expected extension library is missing: $lib" >&2
    exit 1
fi

scratch_root="${TMPDIR:-/tmp}"
scratch_root="${scratch_root%/}"
tmp="$(mktemp -d "$scratch_root/pinkdb-string-ext-smoke.XXXXXX")"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT

statements=(
    "LOAD EXTENSION '$lib';"

    "SELECT split_part('a,b,c', ',', CAST(2 AS BIGINT)) AS part, reverse('héllo') AS rev;"

    "SELECT CASE WHEN split_part('a,b,c', ',', CAST(3 AS BIGINT)) = 'c' AND split_part('a,b,c', ',', CAST(-1 AS BIGINT)) = 'c' AND split_part('a,b,c', ',', CAST(0 AS BIGINT)) = '' AND split_part('abc', '', CAST(2 AS BIGINT)) = 'b' THEN 'split ok' ELSE 'split FAIL' END;"
    "SELECT CASE WHEN reverse('héllo') = 'olléh' AND reverse('') = '' AND reverse(NULL) IS NULL THEN 'reverse ok' ELSE 'reverse FAIL' END;"
    "SELECT CASE WHEN left('hello', CAST(-2 AS BIGINT)) = 'hel' AND right('hello', CAST(-2 AS BIGINT)) = 'llo' AND right('héllo', CAST(2 AS BIGINT)) = 'lo' AND left('hello', CAST(0 AS BIGINT)) = '' THEN 'leftright ok' ELSE 'leftright FAIL' END;"
    "SELECT CASE WHEN strpos('héllo', 'llo') = 3 AND strpos('hello', 'z') = 0 AND strpos('a', '') = 1 THEN 'strpos ok' ELSE 'strpos FAIL' END;"
    "SELECT CASE WHEN starts_with('hello', 'he') AND ends_with('hello', 'lo') AND NOT starts_with('hello', 'el') AND starts_with('x', '') THEN 'prefix ok' ELSE 'prefix FAIL' END;"
    "SELECT CASE WHEN regexp_extract('abc-123', '([a-z]+)-([0-9]+)', 2, '') = '123' AND regexp_extract('hello', 'z', 0, '') = '' AND regexp_extract('HELLO', 'hello', 0, 'i') = 'HELLO' THEN 'extract ok' ELSE 'extract FAIL' END;"
    "SELECT CASE WHEN regexp_matches('hello', 'l', '') AND NOT regexp_matches('hello', '^e', '') AND regexp_matches('abc', '.b', '') THEN 'matches ok' ELSE 'matches FAIL' END;"
    "SELECT CASE WHEN regexp_match('aa1b22', '[0-9]+') = '1' AND regexp_match('abc', 'z') = '' THEN 'match ok' ELSE 'match FAIL' END;"

    # A deterministic extension statement is WAL-loggable (here: a plain
    # INSERT); the durable replay proof is in the crate's cargo tests.
    "CREATE TABLE src (w VARCHAR);"
    "INSERT INTO src VALUES ('a,bc'), ('x,yz');"
    "CREATE TABLE t (v VARCHAR);"
    "INSERT INTO t SELECT reverse(split_part(w, ',', CAST(-1 AS BIGINT))) FROM src;"
    "SELECT CASE WHEN COUNT(*) = 2 AND MIN(v) = 'cb' AND MAX(v) = 'zy' THEN 'insert ok' ELSE 'insert FAIL' END FROM t;"

    # Error paths: a bad group index, a refused option, and ABI v1 strict
    # typing (no implicit INTEGER -> BIGINT cast).
    "SELECT regexp_extract('abc', 'a(b)c', 10, '');"
    "SELECT regexp_matches('a', 'a', 'g');"
    "SELECT left('hello', 2);"

    # SQL options with inline flags and an empty options string work.
    "SELECT CASE WHEN regexp_matches('abc', '(?i)ABC', '') THEN 'inline ok' ELSE 'inline FAIL' END;"
)

echo "==> running the session in $tmp"
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
check() { # check <description> <needle>
    if printf '%s' "$out" | grep -qF -- "$2"; then
        printf 'ok: %s\n' "$1"
        checks=$((checks + 1))
    else
        printf 'FAIL: %s (expected output to contain: %s)\n' "$1" "$2" >&2
        failures=$((failures + 1))
    fi
}

check_marker() { # check_marker <description> <marker>
    if printf '%s\n' "$out" | grep -Eq "^│ ${2} +│$"; then
        printf 'ok: %s\n' "$1"
        checks=$((checks + 1))
    else
        printf 'FAIL: %s (expected a result row "│ %s │")\n' "$1" "$2" >&2
        failures=$((failures + 1))
    fi
}

check "string extension loaded"    "Extension string loaded."
check "split/reverse display row"  "│ b       │ olléh   │"
check_marker "split_part applied"  "split ok"
check_marker "reverse applied"     "reverse ok"
check_marker "left/right applied"  "leftright ok"
check_marker "strpos applied"      "strpos ok"
check_marker "prefixes applied"    "prefix ok"
check_marker "regexp_extract"      "extract ok"
check_marker "regexp_matches"      "matches ok"
check_marker "regexp_match pin"    "match ok"
check_marker "deterministic INSERT" "insert ok"
check_marker "inline regex flags"  "inline ok"
# Native ABI v1 carries an error code, not text: the messages below survive
# only on the engine's built-in path (and under the linked model, covered by
# `cargo test -p pinkdb-ext-string`).
check "group range refused"        "extension function regexp_extract() failed with code 1"
check "'g' option refused"         "extension function regexp_matches() failed with code 1"
check "strict typing refused"      "left argument 2 must be BIGINT, got INTEGER (use CAST)"

if [ "$failures" -ne 0 ]; then
    printf 'FAILED: %d of %d checks failed\n' "$failures" "$((checks + failures))" >&2
    exit 1
fi
printf 'all %d checks passed\n' "$checks"
