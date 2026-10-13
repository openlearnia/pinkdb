#!/usr/bin/env sh
# Build the docs site and deploy it to Cloudflare Workers.
# Run from anywhere; the script works relative to its own directory.
set -eu

cd "$(dirname "$0")"
python3 build.py
exec wrangler deploy
