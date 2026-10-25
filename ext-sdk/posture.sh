#!/bin/sh
# The security posture every SDK README promises, checked once against the
# release CLI:
#
#   1. loading is opt-in           (nothing loads without --allow-load-extension
#                                   or PINKDB_ALLOW_LOAD_EXTENSION=1);
#   2. --extension-root confines   (a path outside the root is refused, and the
#                                   refusal does not depend on the file);
#   3. paths are not names         (a bare name, `..` and a symlink are refused);
#   4. --extension-pin pins bytes  (a library whose SHA-256 is not pinned, or
#                                   differs from its pin, is refused).
#
# The SDK smokes cover what a *loaded* extension does; this script covers what
# the host refuses, which is where the trust boundary actually lives.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/.." && pwd)
cli="$repo/target/release/pinkdb"

so_ext=so
[ "$(uname -s)" = Darwin ] && so_ext=dylib

if [ ! -x "$cli" ]; then
    echo "build the release CLI first: cargo build --release -p pinkdb-cli" >&2
    exit 1
fi

# One tiny extension is enough: every check below fails (or succeeds) before or
# independently of what the library does.
make -C "$here/c" -s all >/dev/null

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT INT TERM
root="$scratch/root"
mkdir -p "$root/lib" "$scratch/outside"
cp "$here/c/build/string_ext.$so_ext" "$root/lib/string_ext.$so_ext"
cp "$here/c/build/string_ext.$so_ext" "$scratch/outside/string_ext.$so_ext"
ln -s "$root/lib/string_ext.$so_ext" "$root/link.$so_ext"
lib="$root/lib/string_ext.$so_ext"

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}
pin="$(sha256_of "$lib")"

checks=0
failures=0

# check <description> <expected text> <args...> -- <script>
# Runs the CLI with a small script from a scratch directory and asserts the
# combined output contains <expected text>.
check() {
    description=$1
    expected=$2
    shift 2
    script=$1
    shift
    out=$(printf '%s\n.quit\n' "$script" | (cd "$scratch" && "$cli" "$@") 2>&1) || true
    checks=$((checks + 1))
    if printf '%s' "$out" | grep -qF -- "$expected"; then
        echo "  ok   $description"
    else
        echo "  FAIL $description"
        echo "       expected to find: $expected"
        printf '%s\n' "$out" | sed 's/^/       | /'
        failures=$((failures + 1))
    fi
}

echo "== loading is opt-in"
check "no flag: LOAD EXTENSION is refused" "LOAD EXTENSION is disabled" \
    "LOAD EXTENSION '$lib';"
check "flag: the library loads" "Extension string_ext loaded." \
    "LOAD EXTENSION '$lib'; SELECT ext_repeat('ab', 2);" --allow-load-extension
out=$(printf "LOAD EXTENSION '%s';\n.quit\n" "$lib" | (cd "$scratch" && PINKDB_ALLOW_LOAD_EXTENSION=1 "$cli") 2>&1) || true
checks=$((checks + 1))
if printf '%s' "$out" | grep -qF "Extension string_ext loaded."; then
    echo "  ok   env var: PINKDB_ALLOW_LOAD_EXTENSION=1 turns loading on"
else
    echo "  FAIL env var: PINKDB_ALLOW_LOAD_EXTENSION=1 turns loading on"
    printf '%s\n' "$out" | sed 's/^/       | /'
    failures=$((failures + 1))
fi

echo "== --extension-root confines the load"
check "a path inside the root loads" "Extension string_ext loaded." \
    "LOAD EXTENSION '$lib';" --allow-load-extension --extension-root "$root"
check "a path outside the root is refused" "outside the extension root" \
    "LOAD EXTENSION '$scratch/outside/string_ext.$so_ext';" \
    --allow-load-extension --extension-root "$root"

echo "== paths, not names"
check "a bare name is refused" "is not a loadable path" \
    "LOAD EXTENSION 'string_ext';" --allow-load-extension
check "a '..' component is refused" "is not a loadable path" \
    "LOAD EXTENSION '$root/lib/../lib/string_ext.$so_ext';" --allow-load-extension
check "a symlink is refused" "is a symlink" \
    "LOAD EXTENSION '$root/link.$so_ext';" --allow-load-extension
check "a relative path is refused under a root" "outside the extension root" \
    "LOAD EXTENSION 'lib/string_ext.$so_ext';" \
    --allow-load-extension --extension-root "$root"

echo "== --extension-pin pins the bytes"
check "a pinned library loads" "Extension string_ext loaded." \
    "LOAD EXTENSION '$lib';" --allow-load-extension --extension-pin "$lib=$pin"
check "an unpinned library is refused" "is not pinned" \
    "LOAD EXTENSION '$lib';" --allow-load-extension \
    --extension-pin "$scratch/outside/string_ext.$so_ext=$pin"
other=$(printf 'not this library' | { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi } | cut -d' ' -f1)
check "a library whose hash differs from its pin is refused" \
    "does not match its pinned SHA-256" "LOAD EXTENSION '$lib';" \
    --allow-load-extension --extension-pin "$lib=$other"
check "a malformed pin is a startup error" "invalid pin" \
    "SELECT 1;" --allow-load-extension --extension-pin "nonsense"

echo
if [ "$failures" -ne 0 ]; then
    echo "$failures of $checks posture checks failed" >&2
    exit 1
fi
echo "all $checks posture checks passed"
