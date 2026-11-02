#!/usr/bin/env bash
# Regenerates bench/quacklab/plan/nyc_taxi_essential1.json from QuackLab's
# compiler. main.go imports QuackLab's internal packages, so it is compiled
# inside QuackLab's Go module through `go run -overlay`: the overlay maps a
# virtual cmd/pinkbench-compile/main.go onto this directory's file, and the
# QuackLab checkout is never written to.
#
#   GRAIN=~/Desktop/projects/grain/grain bench/quacklab/compile/compile.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
QL="$(cd "$HERE/.." && pwd)"
GRAIN="${GRAIN:-$HOME/Desktop/projects/grain/grain}"
MODEL="$GRAIN/fixtures/shared/models/model_nyc_taxi_essential1.json"
OVERLAY="$(mktemp -t pinkbench-overlay)"
trap 'rm -f "$OVERLAY"' EXIT
printf '{"Replace":{"%s/go/cmd/pinkbench-compile/main.go":"%s/main.go"}}' "$GRAIN" "$HERE" > "$OVERLAY"
mkdir -p "$QL/plan"
OUT="$QL/plan/nyc_taxi_essential1.json"
(cd "$GRAIN/go" && go run -overlay "$OVERLAY" ./cmd/pinkbench-compile "$MODEL" "$QL/charts.json") > "$OUT.tmp"
# Record which QuackLab commit the plan came from next to the plan itself.
python3 - "$OUT.tmp" "$OUT" "$(git -C "$GRAIN" rev-parse HEAD)" <<'PY'
import json, sys
plan = json.load(open(sys.argv[1]))
plan = {"quacklabCommit": sys.argv[3], "model": "nyc_taxi_essential1", **plan}
with open(sys.argv[2], "w") as f:
    json.dump(plan, f, indent=1, sort_keys=True)
    f.write("\n")
PY
rm -f "$OUT.tmp"
echo "wrote $OUT"
