#!/bin/sh
#
# pinkdb installer for Linux and macOS.
#
#   curl -fsSLO https://raw.githubusercontent.com/openlearnia/pinkdb/main/install.sh
#   sh install.sh
#
# Downloads a released CLI artifact, verifies it against the release's
# SHA256SUMS (fail closed), and installs it atomically as
# ${PREFIX:-$HOME/.local}/bin/pinkdb. It never runs sudo, never pipes a
# downloaded binary into a shell, and cleans up its temporary directory.
#
# Options:
#   --version VERSION   install a pinned version (default: the latest release)
#   --variant NAME      default | lean | pinklake | pinklake-s3 |
#                       vortexlake | vortexlake-s3   (default: default)
#   --prefix DIR        install prefix (default: $PREFIX or $HOME/.local)
#   --dry-run           resolve and print the plan; download/install nothing
#   --force             replace an existing pinkdb at the destination
#   -h, --help          show this help
#
# Environment:
#   PINKDB_VERSION, PINKDB_VARIANT, PREFIX   same as the flags
#   PINKDB_API_BASE       GitHub API base          (test/dev override)
#   PINKDB_RELEASES_BASE  release download base    (test/dev override)
#   PINKDB_OS, PINKDB_ARCH override uname detection (test/dev only)
#
# Windows is not supported: pinkdb does not build there yet, so there is no
# install.ps1 and this script refuses MSYS/MinGW/Cygwin shells. See
# docs/releasing.md.
set -eu

die() {
    echo "install.sh: error: $*" >&2
    exit 1
}

usage() {
    sed -n '3,30p' "$0" | sed 's/^# \{0,1\}//'
}

version=${PINKDB_VERSION:-}
variant=${PINKDB_VARIANT:-default}
prefix=${PREFIX:-"$HOME/.local"}
dry_run=0
force=0

while [ $# -gt 0 ]; do
    case "$1" in
        -h | --help)
            usage
            exit 0
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
        --variant)
            [ $# -ge 2 ] || die "--variant requires a value"
            variant=$2
            shift 2
            ;;
        --variant=*)
            variant=${1#*=}
            shift
            ;;
        --prefix)
            [ $# -ge 2 ] || die "--prefix requires a value"
            prefix=$2
            shift 2
            ;;
        --prefix=*)
            prefix=${1#*=}
            shift
            ;;
        --dry-run)
            dry_run=1
            shift
            ;;
        --force)
            force=1
            shift
            ;;
        *)
            die "unknown option '$1' (try --help)"
            ;;
    esac
done

[ -n "$prefix" ] || die "--prefix must not be empty"

# Variant slugs are the release asset names; accept the cargo spelling too.
case "$variant" in
    default | lean | pinklake | vortexlake | pinklake-s3 | vortexlake-s3) ;;
    pinklake,s3) variant=pinklake-s3 ;;
    vortexlake,s3) variant=vortexlake-s3 ;;
    *)
        die "unsupported variant '$variant' (choose: default, lean, pinklake, pinklake-s3, vortexlake, vortexlake-s3)"
        ;;
esac

os=${PINKDB_OS:-$(uname -s)}
case "$os" in
    Linux | linux) os=linux ;;
    Darwin | darwin) os=darwin ;;
    MINGW* | MSYS* | CYGWIN* | Windows* | windows*)
        die "Windows is not supported: pinkdb does not ship Windows binaries (see docs/releasing.md)"
        ;;
    *)
        die "unsupported operating system '$os' (supported: linux, darwin)"
        ;;
esac

arch=${PINKDB_ARCH:-$(uname -m)}
case "$arch" in
    x86_64 | amd64) arch=x86_64 ;;
    arm64 | aarch64) arch=aarch64 ;;
    i386 | i486 | i586 | i686)
        die "32-bit x86 is not supported (supported: x86_64, aarch64/arm64)"
        ;;
    *)
        die "unsupported architecture '$arch' (supported: x86_64, aarch64/arm64)"
        ;;
esac

api_base=${PINKDB_API_BASE:-https://api.github.com}
releases_base=${PINKDB_RELEASES_BASE:-https://github.com/openlearnia/pinkdb/releases/download}
while [ "${api_base%/}" != "$api_base" ]; do api_base=${api_base%/}; done
while [ "${releases_base%/}" != "$releases_base" ]; do releases_base=${releases_base%/}; done

tmp=$(mktemp -d "${TMPDIR:-/tmp}/pinkdb-install.XXXXXX") ||
    die "could not create a temporary directory"
staged=""
cleanup() {
    rm -rf "$tmp"
    if [ -n "$staged" ]; then
        rm -f "$staged"
    fi
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

fetch() {
    fetch_url=$1
    fetch_dest=$2
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL --retry 2 --retry-delay 1 -o "$fetch_dest" "$fetch_url" ||
            return 1
    elif command -v wget >/dev/null 2>&1; then
        wget -q -O "$fetch_dest" "$fetch_url" ||
            return 1
    else
        die "neither curl nor wget is available"
    fi
}

if [ -z "$version" ] || [ "$version" = latest ]; then
    api_url="$api_base/repos/openlearnia/pinkdb/releases/latest"
    if ! fetch "$api_url" "$tmp/latest.json"; then
        die "could not resolve the latest release from $api_url (network or GitHub API error)"
    fi
    tag=$(sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$tmp/latest.json" | head -n 1)
    [ -n "$tag" ] || die "the latest-release response has no tag_name (from $api_url)"
else
    tag=$version
    case "$tag" in
        v*) tag=${tag#v} ;;
    esac
    tag="v$tag"
fi

version=${tag#v}
[ -n "$version" ] || die "could not determine a release version"
case "$version" in
    *[!0-9A-Za-z._+-]*) die "invalid release version '$version'" ;;
esac

asset="pinkdb-$version-$os-$arch-$variant.tar.gz"
download_url="$releases_base/$tag/$asset"
sums_url="$releases_base/$tag/SHA256SUMS"
bin_dir="$prefix/bin"
dest="$bin_dir/pinkdb"

if [ "$dry_run" -eq 1 ]; then
    echo "pinkdb install plan (dry run)"
    echo "  version:   $version"
    echo "  tag:       $tag"
    echo "  os:        $os"
    echo "  arch:      $arch"
    echo "  variant:   $variant"
    echo "  asset:     $asset"
    echo "  url:       $download_url"
    echo "  checksums: $sums_url"
    echo "  install:   $dest"
    exit 0
fi

if [ -e "$dest" ] && [ "$force" -ne 1 ]; then
    die "$dest already exists; use --force to replace it (or --prefix to install elsewhere)"
fi

if ! fetch "$sums_url" "$tmp/SHA256SUMS"; then
    die "could not download $sums_url (refusing to install without checksums)"
fi
expected=$(awk -v a="$asset" '$2 == a || $2 == "*" a { print $1; exit }' "$tmp/SHA256SUMS")
[ -n "$expected" ] ||
    die "SHA256SUMS at $sums_url has no entry for $asset (refusing to install)"

if ! fetch "$download_url" "$tmp/$asset"; then
    die "could not download $download_url"
fi

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    elif command -v openssl >/dev/null 2>&1; then
        openssl dgst -sha256 "$1" | sed 's/^.*= //'
    else
        die "no SHA-256 tool found (need sha256sum, shasum, or openssl)"
    fi
}

actual=$(sha256_of "$tmp/$asset")
[ "$actual" = "$expected" ] ||
    die "checksum mismatch for $asset: expected $expected, got $actual (refusing to install)"

# Reject absolute paths and `..` components before extracting. Release
# archives are generated by this repository, but a download is still input.
tar -tzf "$tmp/$asset" >"$tmp/contents" ||
    die "$asset is not a readable tar.gz archive"
if grep -Eq '(^|/)\.\.(/|$)|^/' "$tmp/contents"; then
    die "$asset contains an unsafe path (refusing to extract)"
fi

mkdir -p "$tmp/extract"
tar -xzf "$tmp/$asset" -C "$tmp/extract" ||
    die "could not extract $asset"

find "$tmp/extract" -type f -name pinkdb -print >"$tmp/found"
count=$(wc -l <"$tmp/found" | tr -d ' ')
[ "$count" -eq 1 ] ||
    die "expected exactly one pinkdb binary in $asset, found $count"
source_bin=$(sed -n '1p' "$tmp/found")

mkdir -p "$bin_dir" ||
    die "could not create $bin_dir (choose a writable --prefix; this script never uses sudo)"
staged=$(mktemp "$bin_dir/.pinkdb.XXXXXX") ||
    die "could not create a staging file in $bin_dir"
cp "$source_bin" "$staged"
chmod 755 "$staged"
mv -f "$staged" "$dest" ||
    die "could not install $dest"
staged=""

echo "Installed pinkdb $version ($os-$arch, $variant)"
echo "  path:   $dest"
echo "  sha256: $actual"

case ":$PATH:" in
    *":$bin_dir:"*) ;;
    *)
        echo
        echo "$bin_dir is not on your PATH. Add it with:"
        echo "  export PATH=\"$bin_dir:\$PATH\""
        ;;
esac
