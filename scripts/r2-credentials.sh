#!/usr/bin/env bash
# Create a bucket-scoped Cloudflare R2 S3 credential for pinkdb's benchmark
# and CI buckets, and store it without ever printing the secret:
#   - GitHub repository secrets R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY
#   - a local env file (mode 600) exporting the AWS_* variables pink-lake reads
#
# R2's S3 credentials derive from an account API token: the access key id is
# the token id, the secret access key is the SHA-256 (hex) of the token value.
#
# Requires: cf (authenticated, allowed to create account tokens), gh
# (authenticated with access to the repository), python3.
#
# Usage: scripts/r2-credentials.sh
# Overrides: REPO, BUCKETS ("a b"), ENV_FILE, TOKEN_NAME, CLOUDFLARE_ACCOUNT_ID
set -euo pipefail

REPO=${REPO:-openlearnia/pinkdb}
BUCKETS=${BUCKETS:-"pinkdb-bench pinkdb-ci"}
ENV_FILE=${ENV_FILE:-$HOME/.config/pinkdb/r2.env}
TOKEN_NAME=${TOKEN_NAME:-pinkdb-r2-$(date +%Y%m%d)}

# Workers R2 Storage Bucket Item Write / Read (bucket-scoped permission groups).
PG_WRITE=2efd5506f9c8494dacb1fa10a3e7d5b6
PG_READ=6a018a9f2fc74eb6b293b0c548f38b39

if [ -z "${CLOUDFLARE_ACCOUNT_ID:-}" ]; then
  CLOUDFLARE_ACCOUNT_ID=$(cf auth whoami 2>/dev/null | python3 -c '
import json, sys
accounts = json.load(sys.stdin).get("accounts", [])
if len(accounts) != 1:
    sys.exit("set CLOUDFLARE_ACCOUNT_ID: %d accounts are visible" % len(accounts))
print(accounts[0]["id"])')
fi
export CLOUDFLARE_ACCOUNT_ID

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
chmod 700 "$tmp"

# shellcheck disable=SC2086 # BUCKETS is a space-separated list, split on purpose
python3 - "$CLOUDFLARE_ACCOUNT_ID" "$PG_WRITE" "$PG_READ" $BUCKETS > "$tmp/policy.json" <<'PY'
import json, sys
account, write, read, *buckets = sys.argv[1:]
resources = {f"com.cloudflare.edge.r2.bucket.{account}_default_{b}": "*" for b in buckets}
print(json.dumps([{"effect": "allow",
                   "permission_groups": [{"id": write}, {"id": read}],
                   "resources": resources}]))
PY

# The response holds the token value: it goes to a private temp file only.
cf accounts tokens create --name "$TOKEN_NAME" --policies "@$tmp/policy.json" > "$tmp/token.json"

python3 - "$tmp/token.json" "$tmp" <<'PY'
import hashlib, json, os, sys
path, out = sys.argv[1:]
doc = json.load(open(path))
token = doc.get("result", doc)
key_id, value = token["id"], token["value"]
for name, data in (("key_id", key_id), ("secret", hashlib.sha256(value.encode()).hexdigest())):
    fd = os.open(os.path.join(out, name), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.write(fd, data.encode())
    os.close(fd)
PY

gh secret set R2_ACCESS_KEY_ID --repo "$REPO" < "$tmp/key_id"
gh secret set R2_SECRET_ACCESS_KEY --repo "$REPO" < "$tmp/secret"

mkdir -p "$(dirname "$ENV_FILE")"
umask 077
{
  echo "export AWS_ACCESS_KEY_ID=$(cat "$tmp/key_id")"
  echo "export AWS_SECRET_ACCESS_KEY=$(cat "$tmp/secret")"
  echo "export AWS_ENDPOINT_URL=https://${CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com"
  echo "export AWS_REGION=auto"
} > "$ENV_FILE"
chmod 600 "$ENV_FILE"

echo "created token \"$TOKEN_NAME\" scoped to: $BUCKETS"
echo "GitHub secrets R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY set on $REPO"
echo "local env written to $ENV_FILE (source it before R2 runs)"
