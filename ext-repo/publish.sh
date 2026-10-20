#!/bin/sh
# Publish one built extension into the pinkdb extension repository.
#
#   ./publish.sh <dylib> --name NAME --version VERSION --platform PLATFORM \
#                --description DESC [--repo-dir DIR]
#
# Thin wrapper around publish.py (stdlib only). See FORMAT.md / README.md and
# ../docs/extension-signing.md.
#
# Signing: export PINKDB_EXT_SIGNING_KEY (a PEM file path, PEM content, or
# base64 of the PEM) and publish.py signs the exact index.json bytes with
# Ed25519, writing and uploading index.json.sig next to the catalog. The
# private key is only passed to openssl via a 0600 temporary file (for inline
# content) and is never echoed. Without the variable the catalog is published
# unsigned with a loud warning — clients that pin the key will refuse it.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 "$here/publish.py" "$@"
