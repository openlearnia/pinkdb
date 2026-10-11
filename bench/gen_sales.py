#!/usr/bin/env python3
"""Generate a deterministic 1M-row sales CSV for the pinkdb benchmark."""
import random
import sys

rows = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
out = sys.argv[2] if len(sys.argv) > 2 else "sales_1m.csv"

rng = random.Random(42)
regions = ["west", "east", "north", "south"]
with open(out, "w") as f:
    f.write("id,region,total,active\n")
    for i in range(1, rows + 1):
        region = regions[rng.randrange(4)]
        # ~1% NULL totals.
        total = "" if rng.random() < 0.01 else f"{rng.uniform(1, 1000):.2f}"
        active = "true" if rng.random() < 0.7 else "false"
        f.write(f"{i},{region},{total},{active}\n")
print(f"wrote {rows} rows to {out}")
