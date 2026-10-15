#!/usr/bin/env python3
"""`.lake attach` cold-process benchmark: projection pushdown before/after.

Wave-E companion to `bench/lake_bench.py`. It measures the exact shape stage 2
is about — fresh process -> `.lake attach` -> one fallback query -> exit — for
two pinkdb binaries (a `--pinkdb-before` one, e.g. the wave-D commit, and the
current `--pinkdb`), interleaved best-of-N so process/cache noise hits both
arms equally, and reports the per-query data-file decodes the current binary
prints via `.lake attached` (`decodes: N loads, M columns`).

The lake itself is written once, by the current binary: the writer is
untouched by wave E (byte-stability is a hard invariant), so both binaries
read the very same files.

Usage:
    python3 bench/lake_attach_bench.py --rows 1000000 --runs 5
    python3 bench/lake_attach_bench.py --rows 10000000 --runs 5 \
        --pinkdb-before /path/to/wave-d/pinkdb --json out.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
ROOT = BENCH_DIR.parent
DEFAULT_CSV = BENCH_DIR / "sales_1m.csv"
GEN_SALES = BENCH_DIR / "gen_sales.py"

# The four `lake_bench.py` queries plus the stats fast-path probe.
QUERIES = [
    ("count", "SELECT COUNT(*) FROM {t};"),
    ("filter", "SELECT COUNT(*) FROM {t} WHERE total > 500;"),
    ("orderby", "SELECT id, total FROM {t} ORDER BY total DESC LIMIT 10;"),
    (
        "groupby",
        "SELECT region, COUNT(*), AVG(total), SUM(total), MIN(total), "
        "MAX(total) FROM {t} GROUP BY region;",
    ),
]

FASTPATH_SQL = "SELECT COUNT(*) FROM {t};\nSELECT MIN(total), MAX(total) FROM {t};\n"

RUN_TIME_RE = re.compile(r"Run Time \(s\): real ([\d.]+)")
DECODES_RE = re.compile(r"decodes: (\d+) loads?, (\d+) columns?")
PINK_IMPORT_RE = re.compile(r"Imported (\d+) rows in ([\d.]+)s")
PINK_SAVE_RE = re.compile(
    r"Saved lake version (\d+) in ([\d.]+)s "
    r"\((\d+) tables?, (\d+) rows?, (\d+) bytes written\)"
)


def die(message: str, hint: str | None = None) -> None:
    text = f"error: {message}"
    if hint:
        text += f"\n  hint: {hint}"
    raise SystemExit(text)


def run_cmd(cmd: list[str], stdin: str, cwd: Path | None = None) -> tuple[float, str, int]:
    t0 = time.perf_counter()
    proc = subprocess.run(
        cmd,
        input=stdin,
        text=True,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
    )
    wall = time.perf_counter() - t0
    if proc.returncode != 0:
        die(
            f"{' '.join(cmd)} exited with {proc.returncode}\n"
            f"stdin:\n{stdin}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    return wall, proc.stdout, proc.returncode


def best(times: list[float]) -> float:
    return min(times) if times else float("nan")


def dot_str(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024.0
    return f"{n:.1f} TB"


def render_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    out = [sep, "|" + "|".join(f" {h:<{w}} " for h, w in zip(headers, widths)) + "|", sep]
    for row in rows:
        out.append("|" + "|".join(f" {c:<{w}} " for c, w in zip(row, widths)) + "|")
    out.append(sep)
    return "\n".join(out)


def prepare_csv(rows: int, work: Path, given: Path | None) -> tuple[Path, bool]:
    if given is not None:
        if not given.exists():
            die(f"csv not found: {given}")
        return given, False
    if rows == 1_000_000 and DEFAULT_CSV.exists():
        return DEFAULT_CSV, False
    csv = work / f"sales_{rows}.csv"
    if not GEN_SALES.exists():
        die(f"missing generator: {GEN_SALES}")
    wall, out, _ = run_cmd([sys.executable, str(GEN_SALES), str(rows), str(csv)], "")
    print(f"generated {rows:,} rows -> {csv} in {wall:.1f}s ({human_bytes(csv.stat().st_size)})")
    return csv, True


def ingest(pinkdb: Path, csv: Path, lake_dir: Path, rows: int) -> dict:
    """Create the lake once with the current writer (the writer is unchanged
    across the two binaries, so both read identical files)."""
    script = (
        ".timer on\n"
        "CREATE DATABASE lake;\n"
        "USE lake;\n"
        f".import {dot_str(str(csv))} sales\n"
        f".lake save {dot_str(str(lake_dir))}\n"
        ".quit\n"
    )
    wall, out, _ = run_cmd([str(pinkdb)], script, cwd=ROOT)
    imp = PINK_IMPORT_RE.search(out)
    sav = PINK_SAVE_RE.search(out)
    if not imp or not sav:
        die(f"ingest: could not parse output\n{out}")
    if int(imp.group(1)) != rows:
        die(f"ingest imported {imp.group(1)} rows, expected {rows}")
    return {
        "wall_s": wall,
        "import_printed_s": float(imp.group(2)),
        "save_printed_s": float(sav.group(2)),
        "bytes_written": int(sav.group(5)),
    }


def cold_attach_query(
    pinkdb: Path,
    lake_dir: Path,
    query: str,
    instrument: bool,
    expect: str | None = None,
) -> tuple[float, float | None, tuple[int, int] | None]:
    """Fresh process -> attach -> one query -> (`.lake attached`) -> exit.
    Returns (wall seconds, printed query seconds, decode stats)."""
    script = f".timer on\n.lake attach {dot_str(str(lake_dir))}\n{query}\n"
    if instrument:
        script += ".lake attached\n"
    script += ".quit\n"
    wall, out, _ = run_cmd([str(pinkdb)], script, cwd=ROOT)
    bad = [
        line for line in out.splitlines() if "error" in line.lower() and "Run Time" not in line
    ]
    if bad:
        die("attach query failed:\n" + "\n".join(bad))
    if expect is not None and expect not in out:
        die(f"attach query result check failed ({expect} not in output):\n{out}")
    times = [float(m) for m in RUN_TIME_RE.findall(out)]
    if not times:
        die(f"attach query: no timer output:\n{out}")
    printed = times[-2] if instrument and len(times) >= 2 else times[-1]
    stats = None
    if instrument:
        m = DECODES_RE.search(out)
        if not m:
            die(f"attach query: no decode stats in output:\n{out}")
        stats = (int(m.group(1)), int(m.group(2)))
    return wall, printed, stats


def main() -> None:
    ap = argparse.ArgumentParser(
        description="`.lake attach` cold-process before/after benchmark",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--rows", type=int, default=1_000_000, help="row count")
    ap.add_argument("--runs", type=int, default=5, help="interleaved repetitions, best-of")
    ap.add_argument("--pinkdb", default=str(ROOT / "target/release/pinkdb"), help="current CLI")
    ap.add_argument(
        "--pinkdb-before",
        default=None,
        help="pre-wave-E CLI to compare against (e.g. the wave-D build)",
    )
    ap.add_argument("--json", default=None, help="also write the JSON dump here")
    ap.add_argument("--csv", default=None, help="use this CSV instead of the 1M default/regenerating")
    ap.add_argument("--keep", action="store_true", help="keep the temp lake")
    args = ap.parse_args()

    after = Path(args.pinkdb).resolve()
    if not after.exists():
        die(f"pinkdb not found: {after}", "cargo build --release -p pinkdb-cli --features lake")
    before = Path(args.pinkdb_before).resolve() if args.pinkdb_before else None
    if before is not None and not before.exists():
        die(f"pinkdb-before not found: {before}")

    tmp = Path(tempfile.mkdtemp(prefix="lake_attach_bench_"))
    print("=== .lake attach cold-process benchmark (projection pushdown) ===")
    print(f"temp dir: {tmp}  (kept with --keep)")
    print(f"rows: {args.rows:,}  runs: {args.runs}  cpus: {os.cpu_count()}  {platform.platform()}")
    print(f"after:  {after}")
    if before is not None:
        print(f"before: {before}")
    print()

    try:
        csv, generated = prepare_csv(
            args.rows, tmp, Path(args.csv).resolve() if args.csv else None
        )
        lake_dir = tmp / "pink_lake"
        info = ingest(after, csv, lake_dir, args.rows)
        print(
            f"ingested: import {info['import_printed_s']:.3f}s, save "
            f"{info['save_printed_s']:.3f}s ({human_bytes(info['bytes_written'])}); csv_generated={generated}"
        )
        print()

        table = "lake.sales"
        arms: list[tuple[str, Path]] = [("after", after)]
        if before is not None:
            arms.append(("before", before))

        # Interleaved timed runs: fresh process -> attach -> one query -> exit.
        # Arm order alternates per run so cache/thermal drift is shared.
        results: dict[str, dict] = {}
        for name, _ in QUERIES:
            results[name] = {arm: {"wall": [], "printed": []} for arm, _ in arms}
        for run in range(args.runs):
            order = list(arms)
            if run % 2 == 1:
                order.reverse()
            for arm_name, binary in order:
                for qname, template in QUERIES:
                    query = template.format(t=table)
                    expect = str(args.rows) if qname == "count" else None
                    wall, printed, _ = cold_attach_query(
                        binary, lake_dir, query, False, expect
                    )
                    results[qname][arm_name]["wall"].append(wall)
                    results[qname][arm_name]["printed"].append(printed)
            print(f"  run {run + 1}/{args.runs} done", flush=True)
        print()

        # Decode instrumentation (one extra run per query, after-binary only).
        decodes: dict[str, tuple[int, int]] = {}
        for qname, template in QUERIES:
            _, _, stats = cold_attach_query(after, lake_dir, template.format(t=table), True)
            assert stats is not None
            decodes[qname] = stats
        fp_wall, _, fp_stats = cold_attach_query(
            after, lake_dir, FASTPATH_SQL.format(t=table), True
        )
        assert fp_stats is not None

        print("-- cold process: fresh process -> .lake attach -> one query -> exit --")
        rows = []
        for qname, _ in QUERIES:
            cells = [qname]
            for arm_name, _ in arms:
                cells.append(f"{best(results[qname][arm_name]['wall']) * 1000:.2f}")
            if len(arms) == 2:
                b = best(results[qname]["before"]["wall"]) * 1000
                a = best(results[qname]["after"]["wall"]) * 1000
                cells.append(f"{b / a:.2f}x" if a else "n/a")
            rows.append(cells)
        headers = ["query", *[f"{arm} wall (ms)" for arm, _ in arms]]
        if len(arms) == 2:
            headers.append("before/after")
        print(render_table(headers, rows))
        print("(wall includes process start + attach + query + exit; best-of-N)")
        print()

        print("-- query timer (printed `real`, best-of-N, ms) --")
        rows = []
        for qname, _ in QUERIES:
            cells = [qname]
            for arm_name, _ in arms:
                cells.append(f"{best(results[qname][arm_name]['printed']) * 1000:.2f}")
            rows.append(cells)
        print(render_table(["query", *[f"{arm} (ms)" for arm, _ in arms]], rows))
        print()

        print("-- data-file decodes per query (after binary; via .lake attached) --")
        rows = [
            [qname, str(decodes[qname][0]), str(decodes[qname][1])]
            for qname, _ in QUERIES
        ]
        print(render_table(["query", "decodes (loads)", "columns decoded"], rows))
        rows = [["COUNT(*) + MIN(total), MAX(total)", str(fp_stats[0]), str(fp_stats[1])]]
        print(render_table(["statistics fast paths", "decodes (loads)", "columns decoded"], rows))
        assert fp_stats == (0, 0), f"fast paths must decode nothing, got {fp_stats}"
        print(f"(fast-path probe wall: {fp_wall * 1000:.2f} ms)")
        print()

        doc = {
            "harness": "bench/lake_attach_bench.py",
            "rows": args.rows,
            "runs": args.runs,
            "csv_generated": generated,
            "pinkdb": str(after),
            "pinkdb_before": str(before) if before else None,
            "ingest": info,
            "results": results,
            "decodes_after": {k: {"loads": v[0], "columns": v[1]} for k, v in decodes.items()},
            "fastpath_decodes_after": {"loads": fp_stats[0], "columns": fp_stats[1]},
            "best_wall_ms": {
                q: {arm: best(results[q][arm]["wall"]) * 1000 for arm, _ in arms}
                for q, _ in QUERIES
            },
        }
        print("--- JSON ---")
        print(json.dumps(doc, indent=2, sort_keys=True))
        if args.json:
            Path(args.json).write_text(json.dumps(doc, indent=2, sort_keys=True))
            print(f"(JSON also written to {args.json})")
    finally:
        if args.keep:
            print(f"temp dir kept: {tmp}")
        else:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
