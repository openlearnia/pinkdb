#!/usr/bin/env bash
#
# Build, smoke-test, and package one pinkdb release artifact.
#
#   scripts/release/package.sh --kind cli --variant default
#   scripts/release/package.sh --kind server --variant tls --version 0.1.0
#   scripts/release/package.sh --kind cli --variant lean \
#       --target aarch64-unknown-linux-gnu
#
# Output (in --out, default ./dist):
#   pinkdb-<version>-<os>-<arch>-<variant>.tar.gz
#   pinkdb-server-<version>-<os>-<arch>-<variant>.tar.gz
#   <asset>.sha256                     (standard "hash  filename" line)
#
# The archive has one top-level directory named after the asset and contains
# the binary, README.md and any LICENSE* file at the repository root. The
# `.sha256` sidecar is written only after the smoke test passes: a release
# artifact that has not been exercised is never packaged.
#
# Asset naming is part of the release contract: os is `linux` or `darwin`,
# arch is `x86_64` or `aarch64`, and variant is one of the slugs below. The
# installer maps `arm64`/`aarch64` to the same `aarch64` spelling.
#
# CLI variants          cargo features
#   default             (default features: json, datetime, math)
#   lean                --no-default-features
#   pinklake            pinklake
#   pinklake-s3         pinklake,s3
#   vortexlake          vortexlake
#   vortexlake-s3       vortexlake,s3
#
# Server variants       cargo features
#   default             (default features)
#   async-server        async-server
#   tls                 tls
#
# The GitHub release workflow calls this script; local runs exercise exactly
# the same build + smoke + tar + checksum code.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

die() {
    echo "error: $*" >&2
    exit 1
}

usage() {
    cat <<'EOF'
Usage: scripts/release/package.sh [options]

  --kind cli|server     artifact family (default: cli)
  --variant NAME        feature variant (default: default)
  --version VERSION     release version (default: crates/pinkdb-cli/Cargo.toml)
  --target TRIPLE       cargo target triple (default: host)
  --out DIR             output directory (default: ./dist)
  --bin PATH            package this already-built binary (skips cargo build)
  --skip-smoke          skip smoke.sh (local packaging experiments only)
  -h, --help            show this help
EOF
}

kind=cli
variant=default
version=""
target=""
out=""
prebuilt=""
skip_smoke=0

while [ $# -gt 0 ]; do
    case "$1" in
        --kind)
            [ $# -ge 2 ] || die "--kind requires a value"
            kind=$2
            shift 2
            ;;
        --kind=*)
            kind=${1#*=}
            shift
            ;;
        --variant)
            [ $# -ge 2 ] || die "--variant requires a value"
            variant=$2
            shift 2
            ;;
        --variant=*)
            variant=${1#*=}
            shift
            ;;
        --version)
            [ $# -ge 2 ] || die "--version requires a value"
            version=$2
            shift 2
            ;;
        --version=*)
            version=${1#*=}
            shift
            ;;
        --target)
            [ $# -ge 2 ] || die "--target requires a value"
            target=$2
            shift 2
            ;;
        --target=*)
            target=${1#*=}
            shift
            ;;
        --out)
            [ $# -ge 2 ] || die "--out requires a value"
            out=$2
            shift 2
            ;;
        --out=*)
            out=${1#*=}
            shift
            ;;
        --bin)
            [ $# -ge 2 ] || die "--bin requires a path"
            prebuilt=$2
            shift 2
            ;;
        --bin=*)
            prebuilt=${1#*=}
            shift
            ;;
        --skip-smoke)
            skip_smoke=1
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            die "unknown argument: $1 (try --help)"
            ;;
    esac
done

case "$kind" in
    cli | server) ;;
    *) die "--kind must be 'cli' or 'server', got '$kind'" ;;
esac

# ---- variant -> cargo features ------------------------------------------------
# Keep this table in sync with scripts/release/matrix.py and docs/releasing.md.
features=""
no_default=0
case "$kind/$variant" in
    cli/default) ;;
    cli/lean) no_default=1 ;;
    cli/pinklake) features="pinklake" ;;
    cli/pinklake-s3) features="pinklake,s3" ;;
    cli/vortexlake) features="vortexlake" ;;
    cli/vortexlake-s3) features="vortexlake,s3" ;;
    server/default) ;;
    server/async-server) features="async-server" ;;
    server/tls) features="tls" ;;
    *)
        die "unknown $kind variant '$variant' (CLI: default lean pinklake pinklake-s3 vortexlake vortexlake-s3; server: default async-server tls)"
        ;;
esac

# ---- version -----------------------------------------------------------------
if [ -z "$version" ]; then
    version=$(grep -m1 '^version' "$repo_root/crates/pinkdb-cli/Cargo.toml" | cut -d'"' -f2)
fi
case "$version" in
    '' | *[!0-9A-Za-z._+-]*) die "invalid version '$version'" ;;
esac

# ---- os/arch -----------------------------------------------------------------
normalize_arch() {
    case "$1" in
        arm64 | aarch64) echo aarch64 ;;
        x86_64 | amd64) echo x86_64 ;;
        *) return 1 ;;
    esac
}

if [ -n "$target" ]; then
    case "$target" in
        *-apple-darwin)
            os=darwin
            arch=$(normalize_arch "${target%%-*}") ||
                die "unsupported architecture in target '$target'"
            ;;
        *-linux-gnu)
            os=linux
            arch=$(normalize_arch "${target%%-*}") ||
                die "unsupported architecture in target '$target'"
            ;;
        *windows*)
            die "Windows is not a supported release target (see docs/releasing.md)"
            ;;
        *) die "unsupported --target '$target' (expected *-apple-darwin or *-linux-gnu)" ;;
    esac
    [ -n "$arch" ] || die "unsupported architecture in target '$target'"
else
    case "$(uname -s)" in
        Darwin) os=darwin ;;
        Linux) os=linux ;;
        MINGW* | MSYS* | CYGWIN*) die "Windows is not a supported release target (see docs/releasing.md)" ;;
        *) die "unsupported host OS: $(uname -s)" ;;
    esac
    arch=$(normalize_arch "$(uname -m)") || die "unsupported host architecture: $(uname -m)"
fi

if [ "$kind" = server ]; then
    package_name=pinkdb-server
    asset_prefix=pinkdb-server
    bin_name=pinkdb-server
else
    package_name=pinkdb-cli
    asset_prefix=pinkdb
    bin_name=pinkdb
fi
asset_stem="${asset_prefix}-${version}-${os}-${arch}-${variant}"

# ---- build -------------------------------------------------------------------
if [ -n "$prebuilt" ]; then
    bin=$prebuilt
    [ -f "$bin" ] || die "--bin '$bin' does not exist"
else
    build_args=(build --release --locked -p "$package_name")
    if [ -n "$target" ]; then
        build_args+=(--target "$target")
    fi
    if [ "$no_default" -eq 1 ]; then
        build_args+=(--no-default-features)
    fi
    if [ -n "$features" ]; then
        build_args+=(--features "$features")
    fi
    echo "==> cargo ${build_args[*]}" >&2
    (cd "$repo_root" && cargo "${build_args[@]}")
    if [ -n "$target" ]; then
        bin="$repo_root/target/$target/release/$bin_name"
    else
        bin="$repo_root/target/release/$bin_name"
    fi
    [ -f "$bin" ] || die "expected binary is missing: $bin"
fi

if [ "$skip_smoke" -eq 1 ]; then
    echo "warning: --skip-smoke: not smoke-testing $bin" >&2
else
    "$repo_root/scripts/release/smoke.sh" --kind "$kind" --variant "$variant" --bin "$bin"
fi

# ---- stage + archive ---------------------------------------------------------
out=${out:-"$repo_root/dist"}
mkdir -p "$out"
out=$(cd "$out" && pwd)

stage_root=$(mktemp -d "${TMPDIR:-/tmp}/pinkdb-package.XXXXXX")
cleanup() { rm -rf "$stage_root"; }
trap cleanup EXIT HUP INT TERM

stage="$stage_root/$asset_stem"
mkdir -p "$stage"
cp "$bin" "$stage/$bin_name"
chmod 755 "$stage/$bin_name"
for extra in README.md LICENSE LICENSE-MIT LICENSE-APACHE LICENSE.md; do
    if [ -f "$repo_root/$extra" ]; then
        cp "$repo_root/$extra" "$stage/$extra"
    fi
done

# Deterministic-ish: fixed member mtimes, a sorted member list where the tar
# implementation supports it, no resource forks, and gzip with no timestamp
# (`-n`). `tar` and `gzip` are the only build-time tools needed here.
find "$stage" -exec touch -t 200001010000.00 {} + 2>/dev/null || true
asset="$out/$asset_stem.tar.gz"
if tar --version 2>/dev/null | grep -q 'GNU tar'; then
    (cd "$stage_root" && COPYFILE_DISABLE=1 tar \
        --sort=name --mtime='UTC 2000-01-01' --owner=0 --group=0 --numeric-owner \
        --format=ustar -cf - "$asset_stem") | gzip -9n >"$asset"
else
    (cd "$stage_root" && COPYFILE_DISABLE=1 tar -cf - "$asset_stem") | gzip -9n >"$asset"
fi

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    elif command -v openssl >/dev/null 2>&1; then
        openssl dgst -sha256 "$1" | sed 's/^.*= //'
    else
        die "no SHA-256 tool found (needed for the release manifest)"
    fi
}
digest=$(sha256_of "$asset")
printf '%s  %s\n' "$digest" "$(basename "$asset")" >"$asset.sha256"

echo "asset: $(basename "$asset")"
echo "path: $asset"
echo "sha256: $digest"
echo "size: $(wc -c <"$asset" | tr -d ' ') bytes"
