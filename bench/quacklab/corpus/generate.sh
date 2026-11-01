#!/usr/bin/env bash
# Regenerates bench/quacklab/corpus/<model>.sql.json: the SQL a dashboard sends
# for each case in <model>.queries.json, compiled by QuackLab itself. As in
# ../compile/compile.sh the tool is compiled inside QuackLab's Go module through
# `go run -overlay`, so the QuackLab checkout is never written to.
#
#   GRAIN=~/Desktop/projects/grain/grain bench/quacklab/corpus/generate.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
GRAIN="${GRAIN:-$HOME/Desktop/projects/grain/grain}"
OVERLAY="$(mktemp -t pinkbench-chartcorpus)"
trap 'rm -f "$OVERLAY" "$HERE"/*.sql.json.tmp' EXIT
printf '{"Replace":{"%s/go/cmd/pinkbench-chartcorpus/main.go":"%s/emit/main.go"}}' "$GRAIN" "$HERE" > "$OVERLAY"
COMMIT="$(git -C "$GRAIN" rev-parse HEAD)"
for MODEL in nyc_taxi_essential1 \
             single_source_by_category_required_attrs \
             shared_view_id_collision; do
  OUT="$HERE/$MODEL.sql.json"
  (cd "$GRAIN/go" && go run -overlay "$OVERLAY" ./cmd/pinkbench-chartcorpus \
     "$GRAIN/fixtures/shared/models/model_$MODEL.json" "$HERE/$MODEL.queries.json") > "$OUT.tmp"
  # Record which QuackLab commit the SQL came from next to the SQL itself.
  python3 - "$OUT.tmp" "$OUT" "$COMMIT" <<'PY'
import json, sys
corpus = json.load(open(sys.argv[1]))
corpus = {"quacklabCommit": sys.argv[3], **corpus}
with open(sys.argv[2], "w") as f:
    json.dump(corpus, f, indent=1, sort_keys=True)
    f.write("\n")
PY
  rm -f "$OUT.tmp"
  echo "wrote $OUT"
done
