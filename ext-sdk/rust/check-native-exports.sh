#!/usr/bin/env bash
#
# Regression guard: linked-only extension rlibs must carry no ABI v1 exports.
#
# The three `pinkdb_extension_*` symbols are `#[no_mangle] extern "C"` globals.
# They belong to exactly one kind of artifact — a **cdylib** loaded with
# `LOAD EXTENSION '<path>'` (the SDK's `pinkdb_abi_exports!()` call). An rlib
# the host links in (every in-repo core extension: math, datetime, string's
# linked half, ...) must not carry them: two such rlibs in one binary fail to
# link on Linux with
#
#     duplicate symbol: pinkdb_extension_init
#     duplicate symbol: pinkdb_extension_name
#
# (rust-lld / GNU ld reject duplicates; the macOS linker tolerates them, which
# is why this check does not rely on the local linker).
#
# The check inspects the freshly built debug rlibs of the linked-only in-repo
# extensions, then a positive control: the SDK example cdylib (which opts in)
# must export all three symbols, so a macro regression that stopped emitting
# them cannot pass silently either.
#
# Usage: ./ext-sdk/rust/check-native-exports.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
cd "$repo"

# `nm` prints local and foreign symbols alike; `-g` restricts to external ones.
nm_bin="${NM:-nm}"
command -v "$nm_bin" >/dev/null 2>&1 || {
    echo "FAIL: '$nm_bin' not found (set NM=...)" >&2
    exit 1
}

# --- 1. linked-only rlibs: no ABI exports ---------------------------------
#
# Clean first so the glob cannot pick a stale pre-fix artifact out of deps/.
echo "==> cargo clean -p pinkdb-math -p pinkdb-datetime -p pinkdb-ext-string"
cargo clean -p pinkdb-math -p pinkdb-datetime -p pinkdb-ext-string >/dev/null
echo "==> cargo build -p pinkdb-math -p pinkdb-datetime -p pinkdb-ext-string"
cargo build -p pinkdb-math -p pinkdb-datetime -p pinkdb-ext-string

failures=0
checked=0
for crate in pinkdb_math pinkdb_datetime string_ext; do
    shopt -s nullglob
    # Workspace-member libs may be emitted as a top-level `lib<name>.rlib`
    # and/or a hashed `deps/lib<name>-<hash>.rlib` (when another crate links
    # them). Check every artifact either way.
    rlibs=("$repo"/target/debug/lib"$crate".rlib "$repo"/target/debug/deps/lib"$crate"-*.rlib)
    shopt -u nullglob
    if [ "${#rlibs[@]}" -eq 0 ]; then
        echo "FAIL: no rlib for $crate in target/debug/{,deps/}" >&2
        failures=$((failures + 1))
        continue
    fi
    for rlib in "${rlibs[@]}"; do
        checked=$((checked + 1))
        hits="$("$nm_bin" -g "$rlib" 2>/dev/null | grep -E 'pinkdb_extension_(abi_version|name|init)$' || true)"
        if [ -n "$hits" ]; then
            echo "FAIL: linked-only rlib carries ABI v1 exports: $rlib" >&2
            printf '%s\n' "$hits" | sed 's/^/    /' >&2
            failures=$((failures + 1))
        else
            echo "ok: $(basename "$rlib") carries no pinkdb_extension_* exports"
        fi
    done
done

# --- 2. positive control: the cdylib build does export them ---------------
#
# `pinkdb-ext-sdk-example` is cdylib-only and calls `pinkdb_abi_exports!()`
# unconditionally, so the dylib must expose the ABI v1 entry points. The
# Makefile builds it; use the workspace target dir.
echo "==> make -C ext-sdk/rust (example cdylib)"
make -C "$here" -s all

case "$(uname -s)" in
    Darwin) so_ext="dylib" ;;
    *)      so_ext="so" ;;
esac
example="$repo/target/release/librust_string_ext.$so_ext"
if [ ! -f "$example" ]; then
    echo "FAIL: expected example cdylib is missing: $example" >&2
    failures=$((failures + 1))
else
    exported="$("$nm_bin" -g "$example" 2>/dev/null | grep -cE 'pinkdb_extension_(abi_version|name|init)$' || true)"
    if [ "$exported" -eq 3 ]; then
        echo "ok: $(basename "$example") exports all three ABI v1 symbols"
    else
        echo "FAIL: $(basename "$example") exports $exported of 3 ABI v1 symbols" >&2
        "$nm_bin" -g "$example" | grep -E 'pinkdb_extension_' >&2 || true
        failures=$((failures + 1))
    fi
fi

if [ "$failures" -ne 0 ]; then
    echo "FAILED: $failures native-export check(s) failed" >&2
    exit 1
fi
echo "all native-export checks passed ($checked linked-only rlib(s) clean, cdylib exports present)"
