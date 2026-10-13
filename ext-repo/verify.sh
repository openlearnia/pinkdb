#!/usr/bin/env bash
# Verify a published artifact against the repository index.
#
#   ./verify.sh <artifact-url> [index-url]
#
#   <artifact-url>  e.g. https://<host>/artifacts/math_ext/0.1.0/darwin-arm64/math_ext.dylib
#   [index-url]     defaults to <origin>/index.json
#
# Downloads the artifact with curl, looks it up in the catalog by its path, and
# compares the SHA-256 with the entry's `sha256`. Exits non-zero on mismatch.
set -euo pipefail

usage() {
  echo "usage: $0 <artifact-url> [index-url]" >&2
  exit 2
}

artifact_url=${1:-}
index_url=${2:-}
if [ -z "$artifact_url" ]; then
  usage
fi

origin=$(python3 - "$artifact_url" <<'PY'
import sys, urllib.parse
u = urllib.parse.urlsplit(sys.argv[1])
print(f"{u.scheme}://{u.netloc}")
PY
)
relpath=$(python3 - "$artifact_url" <<'PY'
import sys, urllib.parse
print(urllib.parse.urlsplit(sys.argv[1]).path.lstrip("/"))
PY
)
if [ -z "$index_url" ]; then
  index_url="$origin/index.json"
fi

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

curl -fsSL -o "$tmpdir/index.json" "$index_url"
curl -fsSL -o "$tmpdir/artifact" "$artifact_url"

expected=$(python3 - "$tmpdir/index.json" "$relpath" <<'PY'
import json, sys
with open(sys.argv[1], "r", encoding="utf-8") as f:
    index = json.load(f)
path = sys.argv[2]
matches = [e for e in index.get("extensions", []) if e.get("file") == path]
if not matches:
    sys.exit(f"verify: no index entry for {path!r}")
if len(matches) > 1:
    sys.exit(f"verify: {len(matches)} index entries for {path!r} (corrupt index)")
print(matches[0]["sha256"])
PY
)

actual=$(python3 - "$tmpdir/artifact" <<'PY'
import hashlib, sys
digest = hashlib.sha256()
with open(sys.argv[1], "rb") as f:
    for chunk in iter(lambda: f.read(1 << 20), b""):
        digest.update(chunk)
print(digest.hexdigest())
PY
)

size=$(wc -c < "$tmpdir/artifact" | tr -d ' ')

if [ "$expected" = "$actual" ]; then
  echo "OK   $artifact_url"
  echo "     sha256 $actual (matches the index entry; $size bytes)"
else
  echo "FAIL $artifact_url" >&2
  echo "     index:    $expected" >&2
  echo "     download: $actual" >&2
  exit 1
fi
