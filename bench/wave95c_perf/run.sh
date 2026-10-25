#!/bin/sh
# Interleaved A/B: base (the commit before wave 95c) vs new, five rounds.
# Both binaries run the same script on a private copy of the base store.
set -e
BASE=$1
NEW=$2
STORE=${3:-/tmp/pinkdb-wave95c-perf.pink}
here=$(cd "$(dirname "$0")" && pwd)
work=/tmp/pinkdb-wave95c-work.pink
for round in 1 2 3 4 5; do
  for which in base new; do
    if [ "$which" = base ]; then exe=$BASE; else exe=$NEW; fi
    for wl in merge update_from; do
      # A private copy of the checkpoint; the stale WAL sidecar of the
      # previous run must go or replay would re-apply it.
      cp "$STORE" "$work"
      rm -f "$work.wal" "$work.lock"
      t=$("$exe" -no-agent -c ".open $work" -c ".timer on" -c "$(cat "$here/$wl.sql")" 2>&1 \
          | sed -n 's/^Run Time (s): real \([0-9.]*\).*/\1/p' | tail -1)
      echo "$round $which $wl $t"
    done
  done
done
rm -f "$work" "$work.wal" "$work.lock"
