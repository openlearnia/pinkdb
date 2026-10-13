#!/bin/sh
# Publish one built extension into the pinkdb extension repository.
#
#   ./publish.sh <dylib> --name NAME --version VERSION --platform PLATFORM \
#                --description DESC [--repo-dir DIR]
#
# Thin wrapper around publish.py (stdlib only). See FORMAT.md / README.md.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 "$here/publish.py" "$@"
