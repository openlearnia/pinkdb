#!/usr/bin/env bash
# Verify the extension repository: the detached Ed25519 catalog signature and
# every artifact's SHA-256.
#
# Local mode (default, no positional arguments):
#
#   ./verify.sh [--index PATH] [--sig PATH] [--pubkey PATH] [--skip-artifacts]
#
#   Verifies <index> + <sig> against <pubkey>, then every entry's artifact at
#   <index dir>/<file> (sha256; a missing artifact or mismatch fails). The
#   defaults are ./index.json, ./index.json.sig and
#   ./keys/extension-signing.pub.pem next to this script.
#
# Remote mode (kept from the pre-signing verifier):
#
#   ./verify.sh <artifact-url> [index-url] [--pubkey PATH]
#
#   Fetches <index-url> and its .sig, verifies the catalog signature, then
#   downloads the one artifact and compares its SHA-256 with the now-signed
#   catalog. <index-url> defaults to <origin>/index.json.
#
# Exits non-zero on ANY failure: missing/malformed/unknown-key/invalid
# signature, missing artifact, hash mismatch, or a failed download. See
# ../docs/extension-signing.md.
set -euo pipefail

here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

usage() {
  sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

fail() {
  echo "FAIL $*" >&2
  exit 1
}

index_path="$here/index.json"
sig_path=""
pubkey="$here/keys/extension-signing.pub.pem"
skip_artifacts=0
artifact_url=""
index_url=""

while [ $# -gt 0 ]; do
  case "$1" in
    --index)
      [ $# -ge 2 ] || usage
      index_path=$2
      shift 2
      ;;
    --sig)
      [ $# -ge 2 ] || usage
      sig_path=$2
      shift 2
      ;;
    --pubkey)
      [ $# -ge 2 ] || usage
      pubkey=$2
      shift 2
      ;;
    --skip-artifacts)
      skip_artifacts=1
      shift
      ;;
    -h | --help)
      usage
      ;;
    --*)
      echo "verify: unknown option $1" >&2
      usage
      ;;
    *)
      if [ -z "$artifact_url" ]; then
        artifact_url=$1
      elif [ -z "$index_url" ]; then
        index_url=$1
      else
        echo "verify: too many positional arguments" >&2
        usage
      fi
      shift
      ;;
  esac
done
if [ -z "$sig_path" ]; then
  sig_path="$index_path.sig"
fi

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

command -v openssl >/dev/null 2>&1 || fail "openssl is required (OpenSSL 3.x)"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"
[ -f "$pubkey" ] || fail "public key not found: $pubkey"

# First 16 hex of SHA-256(SPKI DER), printed for the given public PEM.
pubkey_id() {
  openssl pkey -pubin -in "$1" -outform DER -out "$tmpdir/pub.der" 2>/dev/null \
    || fail "cannot parse public key $1"
  python3 - "$tmpdir/pub.der" <<'PY'
import hashlib, sys
with open(sys.argv[1], "rb") as f:
    print(hashlib.sha256(f.read()).hexdigest()[:16])
PY
}

# verify_signature <index> <sig> <pubkey-pem>
verify_signature() {
  local index=$1 sig=$2 pub=$3 expected actual

  [ -f "$index" ] || fail "catalog not found: $index"
  [ -f "$sig" ] || fail "catalog signature not found: $sig"

  expected=$(pubkey_id "$pub")
  actual=$(
    python3 - "$sig" "$tmpdir/sig.bin" <<'PY'
import base64, binascii, sys

text = open(sys.argv[1], "rb").read()
lines = [line for line in text.splitlines() if line.strip()]
if len(lines) != 1:
    sys.exit("envelope must be exactly one non-empty line")
fields = lines[0].split()
if len(fields) != 3:
    sys.exit("envelope must have exactly three fields")
if fields[0] != b"PINKDB-SIG1":
    sys.exit(f"unsupported envelope prefix {fields[0]!r}")
key_id = fields[1].decode("ascii")
if len(key_id) != 16 or any(c not in "0123456789abcdef" for c in key_id):
    sys.exit(f"key id {key_id!r} is not 16 lowercase hex characters")
try:
    signature = base64.b64decode(fields[2], validate=True)
except (binascii.Error, ValueError) as e:
    sys.exit(f"invalid base64 signature: {e}")
if len(signature) != 64:
    sys.exit(f"expected 64 signature bytes, found {len(signature)}")
open(sys.argv[2], "wb").write(signature)
print(key_id)
PY
  ) || fail "malformed catalog signature $sig"

  if [ "$actual" != "$expected" ]; then
    fail "signature key id $actual does not match pinned key $expected ($pub)"
  fi
  if ! openssl pkeyutl -verify -rawin -pubin -inkey "$pub" \
    -in "$index" -sigfile "$tmpdir/sig.bin" >/dev/null 2>&1; then
    fail "catalog signature does not verify: $index"
  fi
  echo "OK   catalog signature $index (key $expected)"
}

# verify_artifacts <index> <repository-root>
verify_artifacts() {
  python3 - "$1" "$2" <<'PY'
import hashlib
import json
import os
import sys

index_path, root = sys.argv[1], sys.argv[2]
try:
    with open(index_path, encoding="utf-8") as f:
        catalog = json.load(f)
except (OSError, ValueError) as e:
    sys.exit(f"FAIL cannot read catalog {index_path}: {e}")
entries = catalog.get("extensions")
if not isinstance(entries, list):
    sys.exit(f"FAIL catalog {index_path} has no extensions array")

base = os.path.realpath(root)
failures = 0
checked = 0
for entry in entries:
    name = entry.get("name", "?")
    version = entry.get("version", "?")
    platform = entry.get("platform", "?")
    file_field = entry.get("file")
    expected = entry.get("sha256")
    label = f"{name} {version} ({platform})"
    if not isinstance(file_field, str) or not isinstance(expected, str):
        print(f"FAIL {label}: malformed catalog entry", file=sys.stderr)
        failures += 1
        continue
    path = os.path.realpath(os.path.join(root, *file_field.split("/")))
    if not path.startswith(base + os.sep):
        print(f"FAIL {label}: unsafe artifact path {file_field!r}", file=sys.stderr)
        failures += 1
        continue
    if not os.path.isfile(path):
        print(f"FAIL {label}: artifact missing: {path}", file=sys.stderr)
        failures += 1
        continue
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
            size += len(chunk)
    actual = digest.hexdigest()
    checked += 1
    if actual != expected.lower():
        print(
            f"FAIL {label}: sha256 mismatch\n"
            f"     catalog:  {expected}\n"
            f"     artifact: {actual} ({path})",
            file=sys.stderr,
        )
        failures += 1
    else:
        print(f"OK   {label}: sha256 {actual} ({size} bytes)")

print(f"checked {checked} artifact(s), {failures} failure(s)")
sys.exit(1 if failures else 0)
PY
}

# verify_one_artifact <index> <repo-relative-path> <downloaded-file>
verify_one_artifact() {
  python3 - "$1" "$2" "$3" <<'PY'
import hashlib
import json
import sys

index_path, relpath, downloaded = sys.argv[1], sys.argv[2], sys.argv[3]
with open(index_path, encoding="utf-8") as f:
    catalog = json.load(f)
matches = [e for e in catalog.get("extensions", []) if e.get("file") == relpath]
if not matches:
    sys.exit(f"FAIL no catalog entry for {relpath!r}")
if len(matches) > 1:
    sys.exit(f"FAIL {len(matches)} catalog entries for {relpath!r}")
expected = matches[0].get("sha256")
digest = hashlib.sha256()
size = 0
with open(downloaded, "rb") as f:
    for chunk in iter(lambda: f.read(1 << 20), b""):
        digest.update(chunk)
        size += len(chunk)
actual = digest.hexdigest()
if actual != expected:
    sys.exit(
        f"FAIL {relpath}: sha256 mismatch\n"
        f"     catalog:  {expected}\n"
        f"     download: {actual}"
    )
print(f"OK   {relpath}: sha256 {actual} ({size} bytes)")
PY
}

if [ -n "$artifact_url" ]; then
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

  echo "fetching $index_url"
  curl -fsSL -o "$tmpdir/index.json" "$index_url" \
    || fail "cannot download $index_url"
  sig_url=$(python3 - "$index_url" <<'PY'
import sys, urllib.parse
u = urllib.parse.urlsplit(sys.argv[1])
path = u.path + ".sig"
print(urllib.parse.urlunsplit((u.scheme, u.netloc, path, u.query, "")))
PY
  )
  curl -fsSL -o "$tmpdir/index.json.sig" "$sig_url" \
    || fail "cannot download $sig_url (the catalog is unsigned or the signature is not served)"
  verify_signature "$tmpdir/index.json" "$tmpdir/index.json.sig" "$pubkey"

  echo "fetching $artifact_url"
  curl -fsSL -o "$tmpdir/artifact" "$artifact_url" \
    || fail "cannot download $artifact_url"
  verify_one_artifact "$tmpdir/index.json" "$relpath" "$tmpdir/artifact"
  echo "OK   $artifact_url"
else
  verify_signature "$index_path" "$sig_path" "$pubkey"
  if [ "$skip_artifacts" -eq 0 ]; then
    verify_artifacts "$index_path" "$(dirname -- "$index_path")"
  else
    echo "skipped artifact verification (--skip-artifacts)"
  fi
fi
