#!/usr/bin/env bash
#
# Regenerate or verify the release SHA256SUMS manifest.
#
#   scripts/release/manifest.sh dist              # write dist/SHA256SUMS
#   scripts/release/manifest.sh --check dist      # verify the existing file
#
# `SHA256SUMS` has one standard "hash  filename" line per *.tar.gz asset,
# sorted by filename. Generate mode also checks every per-asset .sha256
# sidecar produced by package.sh; a mismatch stops the manifest.
#
# The publish job downloads the per-job artifacts, then runs this once. That
# keeps the manifest a single source of truth and avoids concurrent appends.
set -euo pipefail

die() {
    echo "error: $*" >&2
    exit 1
}

check=0
if [ "${1:-}" = "--check" ]; then
    check=1
    shift
fi
dir=${1:-}
[ -n "$dir" ] || die "usage: manifest.sh [--check] <directory>"
[ -d "$dir" ] || die "not a directory: $dir"

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    elif command -v openssl >/dev/null 2>&1; then
        openssl dgst -sha256 "$1" | sed 's/^.*= //'
    else
        die "no SHA-256 tool found"
    fi
}

tax=$(find "$dir" -maxdepth 1 -type f -name '*.tar.gz' -print | sort)
[ -n "$tax" ] || die "no *.tar.gz assets in $dir"

count=0
while IFS= read -r asset; do
    [ -n "$asset" ] || continue
    base=$(basename "$asset")
    digest=$(sha256_of "$asset")
    sidecar="$asset.sha256"
    if [ -f "$sidecar" ]; then
        expected=$(awk '{print $1; exit}' "$sidecar")
        [ "$expected" = "$digest" ] ||
            die "$base: .sha256 says $expected, file hashes to $digest"
    elif [ "$check" -eq 1 ]; then
        die "$base: missing .sha256 sidecar"
    fi
    count=$((count + 1))
done <<EOF
$tax
EOF

manifest="$dir/SHA256SUMS"
if [ "$check" -eq 1 ]; then
    [ -f "$manifest" ] || die "missing $manifest"
    # Every listed file must exist and match; every tarball must be listed.
    while IFS= read -r line; do
        [ -n "$line" ] || continue
        expected=${line%% *}
        name=${line#* }
        name=${name# }
        name=${name#\*}
        [ -n "$name" ] || die "malformed SHA256SUMS line: $line"
        [ -f "$dir/$name" ] || die "SHA256SUMS lists a missing file: $name"
        actual=$(sha256_of "$dir/$name")
        [ "$actual" = "$expected" ] ||
            die "$name: SHA256SUMS says $expected, file hashes to $actual"
    done <"$manifest"

    while IFS= read -r asset; do
        [ -n "$asset" ] || continue
        base=$(basename "$asset")
        awk -v name="$base" '$2 == name || $2 == "*" name { found = 1 } END { exit !found }' \
            "$manifest" || die "$base is not listed in SHA256SUMS"
    done <<EOF
$tax
EOF
    echo "SHA256SUMS ok: $count asset(s) in $dir"
    exit 0
fi

tmp="$manifest.tmp.$$"
trap 'rm -f "$tmp"' EXIT HUP INT TERM
: >"$tmp"
while IFS= read -r asset; do
    [ -n "$asset" ] || continue
    base=$(basename "$asset")
    digest=$(sha256_of "$asset")
    printf '%s  %s\n' "$digest" "$base" >>"$tmp"
done <<EOF
$tax
EOF
mv "$tmp" "$manifest"
trap - EXIT HUP INT TERM
echo "wrote $manifest ($count asset(s))"
