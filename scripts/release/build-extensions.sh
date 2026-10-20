#!/usr/bin/env bash
#
# Build, architecture-check, and stage the extension catalog artifacts for one
# platform tag. The publish workflow runs this in a matrix (one job per
# platform), downloads every staged directory, then publishes once.
#
#   scripts/release/build-extensions.sh --platform darwin-arm64 --version 0.1.0
#   scripts/release/build-extensions.sh --platform darwin-x64 --version 0.1.0
#
# Builds (from the existing Makefiles, not a parallel recipe):
#   ext-sdk/c       the C/C++ worked examples       -> catalog candidates
#   ext-sdk/go      the Go worked example            -> built + smoked
#   ext-sdk/rust    the Rust SDK example             -> built + smoked
#   extensions/*/   each directory with a Makefile   -> catalog candidates
#
# Catalog artifacts today are the three C/C++ examples (math_ext, string_ext,
# cpp_ext) and extensions/string (libstring_ext). Cargo-built core extensions
# are linked into the CLI, and the Go/Rust SDK examples are not catalog
# entries; docs/releasing.md records that gap precisely.
#
# darwin-x64 is cross-built on the Apple Silicon runner: C with `-arch
# x86_64`, Go with GOARCH=amd64 + cgo, and Rust with the
# x86_64-apple-darwin target. A cross job verifies every library's
# architecture with `file`; it cannot run the host-native smoke scripts, so
# those are skipped with a warning (the native platform jobs still smoke).
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

die() {
    echo "error: $*" >&2
    exit 1
}

usage() {
    cat <<'EOF'
Usage: scripts/release/build-extensions.sh --platform TAG --version VERSION [options]

  --platform TAG    darwin-arm64 | darwin-x64 | linux-x64 | linux-arm64
  --version VERSION catalog version (the release tag without its `v`)
  --out DIR         staging directory (default: ext-dist/<platform>)
  --name NAME       stage only this extension (manual dispatch filter)
  --skip-smoke      skip the native SDK smoke scripts
  -h, --help        show this help
EOF
}

platform=""
version=""
out=""
name_filter=""
skip_smoke=0

while [ $# -gt 0 ]; do
    case "$1" in
        --platform)
            [ $# -ge 2 ] || die "--platform requires a value"
            platform=$2
            shift 2
            ;;
        --version)
            [ $# -ge 2 ] || die "--version requires a value"
            version=$2
            shift 2
            ;;
        --out)
            [ $# -ge 2 ] || die "--out requires a value"
            out=$2
            shift 2
            ;;
        --name)
            [ $# -ge 2 ] || die "--name requires a value"
            name_filter=$2
            shift 2
            ;;
        --skip-smoke)
            skip_smoke=1
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *) die "unknown argument: $1 (try --help)" ;;
    esac
done

[ -n "$platform" ] || die "--platform is required"
[ -n "$version" ] || die "--version is required"
out=${out:-"$repo_root/ext-dist/$platform"}
mkdir -p "$out"
out=$(cd "$out" && pwd)

host_os=$(uname -s)
host_arch=$(uname -m)
case "$host_arch" in
    arm64 | aarch64) host_arch=aarch64 ;;
    x86_64 | amd64) host_arch=x86_64 ;;
esac

cross=0
target=""
case "$platform" in
    darwin-arm64)
        [ "$host_os" = Darwin ] || die "darwin-arm64 must be built on macOS"
        [ "$host_arch" = aarch64 ] || die "darwin-arm64 needs an arm64 host"
        lib_ext=dylib
        ;;
    darwin-x64)
        [ "$host_os" = Darwin ] || die "darwin-x64 must be built on macOS"
        lib_ext=dylib
        if [ "$host_arch" != x86_64 ]; then
            cross=1
            target=x86_64-apple-darwin
        fi
        ;;
    linux-x64)
        [ "$host_os" = Linux ] || die "linux-x64 must be built on Linux"
        [ "$host_arch" = x86_64 ] || die "linux-x64 needs an x86_64 host"
        lib_ext=so
        ;;
    linux-arm64)
        [ "$host_os" = Linux ] || die "linux-arm64 must be built on Linux"
        [ "$host_arch" = aarch64 ] || die "linux-arm64 needs an arm64 host"
        lib_ext=so
        ;;
    *)
        die "unsupported extension platform '$platform' (Windows is not built; see docs/releasing.md)"
        ;;
esac

echo "==> platform $platform (host $host_os/$host_arch, cross=$cross)"
if [ "$cross" -eq 1 ]; then
    rustup target add "$target" >/dev/null
fi

# Stale build/ libraries from another architecture are the one way this script
# could stage a binary that does not match its platform tag, so remove them
# before every build (the Makefiles' timestamps cannot see GOARCH/TARGET_ARCH).
echo "==> cleaning stale extension outputs"
make -C "$repo_root/ext-sdk/c" clean >/dev/null 2>&1 || true
make -C "$repo_root/ext-sdk/go" clean >/dev/null 2>&1 || true
for dir in "$repo_root"/extensions/*/; do
    [ -f "$dir/Makefile" ] || continue
    make -C "$dir" clean >/dev/null 2>&1 || true
done

# ---- build every Makefile ----------------------------------------------------
if [ "$cross" -eq 1 ]; then
    echo "==> make -C ext-sdk/c (TARGET_ARCH=x86_64)"
    make -C "$repo_root/ext-sdk/c" all TARGET_ARCH=x86_64

    echo "==> make -C ext-sdk/go (GOOS=darwin GOARCH=amd64)"
    GOOS=darwin GOARCH=amd64 CGO_ENABLED=1 CC="cc -arch x86_64" \
        make -C "$repo_root/ext-sdk/go" all

    echo "==> make -C ext-sdk/rust (TARGET=x86_64-apple-darwin)"
    make -C "$repo_root/ext-sdk/rust" all TARGET=x86_64-apple-darwin

    for dir in "$repo_root"/extensions/*/; do
        [ -f "$dir/Makefile" ] || continue
        echo "==> make -C ${dir#"$repo_root"/} (TARGET=x86_64-apple-darwin)"
        make -C "$dir" all TARGET=x86_64-apple-darwin
    done
else
    echo "==> make -C ext-sdk/c"
    make -C "$repo_root/ext-sdk/c" all

    echo "==> make -C ext-sdk/go"
    make -C "$repo_root/ext-sdk/go" all

    echo "==> make -C ext-sdk/rust"
    make -C "$repo_root/ext-sdk/rust" all

    for dir in "$repo_root"/extensions/*/; do
        [ -f "$dir/Makefile" ] || continue
        echo "==> make -C ${dir#"$repo_root"/}"
        make -C "$dir" all
    done
fi

# ---- architecture check ------------------------------------------------------
# Every catalog candidate (plus the SDK examples we just built) must match the
# catalog's platform tag. This is what makes a cross-built darwin-x64 publish
# trustworthy even though it cannot run natively on this runner.
shopt -s nullglob
libs=(
    "$repo_root/ext-sdk/c/build/"*."$lib_ext"
    "$repo_root/ext-sdk/go/build/"*."$lib_ext"
    "$repo_root/extensions/"*/build/*."$lib_ext"
    "$repo_root/extensions/"*/dist/*."$lib_ext"
)
if [ "$cross" -eq 1 ]; then
    libs+=("$repo_root/target/$target/release/librust_string_ext.$lib_ext")
else
    libs+=("$repo_root/target/release/librust_string_ext.$lib_ext")
fi
[ "${#libs[@]}" -gt 0 ] || die "no extension libraries were built"
python3 "$repo_root/scripts/release/extensions.py" verify-arch \
    --platform "$platform" "${libs[@]}"

# ---- smoke (native only) -----------------------------------------------------
if [ "$cross" -eq 1 ]; then
    echo "warning: cross-built $platform cannot run the host-native SDK smoke scripts; architecture was verified with file(1)" >&2
elif [ "$skip_smoke" -eq 1 ]; then
    echo "warning: --skip-smoke: SDK smoke scripts were not run" >&2
else
    "$repo_root/ext-sdk/c/smoke.sh"
    "$repo_root/ext-sdk/go/smoke.sh"
    "$repo_root/ext-sdk/rust/smoke.sh"
fi

# ---- stage catalog artifacts -------------------------------------------------
discover_args=(
    discover
    --platform "$platform"
    --version "$version"
    --root "$repo_root"
    --out "$out"
)
if [ -n "$name_filter" ]; then
    discover_args+=(--name "$name_filter")
fi
python3 "$repo_root/scripts/release/extensions.py" "${discover_args[@]}"

echo "extensions staged: $out/manifest.tsv"
