#!/bin/sh
# Thin wrapper so the suite has one entry point.
exec python3 "$(dirname "$0")/run.py" "$@"
