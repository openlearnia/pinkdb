#!/usr/bin/env python3
"""Lake-format benchmark: pink-lake (Vortex) vs DuckLake (DuckDB).

Same data, same four queries, two different lakehouse architectures:

  pink-lake (Vortex)   CSV --(.import)--> in-memory store --(.lake save)-->
                       lake dir (JSON catalog + snapshots + one Vortex file
                       per table).  `.lake open` eagerly loads the whole
                       table back into engine memory; queries then run
                       in-memory.
  DuckLake (DuckDB)    CSV --(CREATE TABLE ... AS SELECT)--> transactional
                       SQL catalog (SQLite file) + Parquet data files.
                       Queries scan Parquet lazily at query time.
                       ATTACH is the "open".

This is "same data, same queries, different architectures" — not an
apples-to-apples product comparison.  The fairness notes printed at the end
of a run spell out the differences; the raw numbers are dumped as JSON too.

Python 3 standard library only.  Usage:

    python3 bench/lake_bench.py                       # 1M rows (bench/sales_1m.csv)
    python3 bench/lake_bench.py --rows 10000000       # generates a 10M-row CSV
    python3 bench/lake_bench.py --runs 5 --keep
    python3 bench/lake_bench.py --pinkdb target/release/pinkdb --duckdb duckdb
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
DEFAULT_ROWS = 1_000_000

# (name, SQL template).  {t} is the table reference: `lake.sales` on both
# arms (pink-lake is told to put the table in a database literally named
# `lake`), so the SQL text is identical for the two engines.
QUERIES = [
    ("count", "SELECT COUNT(*) FROM {t};"),
    (
        "groupby",
        "SELECT region, COUNT(*), AVG(total), SUM(total), MIN(total), "
        "MAX(total) FROM {t} GROUP BY region;",
    ),
    ("filter", "SELECT COUNT(*) FROM {t} WHERE total > 500;"),
    ("orderby", "SELECT id, total FROM {t} ORDER BY total DESC LIMIT 10;"),
]

RUN_TIME_RE = re.compile(r"Run Time \(s\): real ([\d.]+)")
PINK_IMPORT_RE = re.compile(r"Imported (\d+) rows in ([\d.]+)s")
PINK_SAVE_RE = re.compile(
    r"Saved lake version (\d+) in ([\d.]+)s "
    r"\((\d+) tables?, (\d+) rows?, (\d+) bytes written\)"
)
PINK_OPEN_RE = re.compile(r"Opened lake version (\d+) in ([\d.]+)s")


class BenchError(SystemExit):
    def __init__(self, message: str, hint: str | None = None):
        text = f"error: {message}"
        if hint:
            text += f"\n  hint: {hint}"
        super().__init__(text)


def die(message: str, hint: str | None = None) -> None:
    raise BenchError(message, hint)


# --------------------------------------------------------------------------
# process helpers


def run_cmd(cmd: list[str], stdin: str, cwd: Path | None = None) -> tuple[float, str, str, int]:
    """Run a process, feed it stdin, return (wall_s, stdout, stderr, rc)."""
    t0 = time.perf_counter()
    proc = subprocess.run(
        cmd,
        input=stdin,
        text=True,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
    )
    wall = time.perf_counter() - t0
    return wall, proc.stdout, proc.stderr, proc.returncode


def check_output(out: str, err: str, rc: int, what: str) -> None:
    """Fail loudly on an engine error instead of timing a failed statement."""
    if rc != 0:
        die(f"{what}: process exited with {rc}\nstdout:\n{out}\nstderr:\n{err}")
    bad = [
        line
        for line in out.splitlines()
        if "error" in line.lower() and "Run Time" not in line
    ]
    if bad:
        die(f"{what}: output contains an error line\n" + "\n".join(bad))


def parse_real_times(out: str) -> list[float]:
    return [float(m) for m in RUN_TIME_RE.findall(out)]


def real_times_for(out: str, n: int, what: str) -> list[float]:
    times = parse_real_times(out)
    if len(times) != n:
        die(
            f"{what}: expected {n} 'Run Time (s): real ...' lines, got {len(times)}\n"
            f"--- output ---\n{out}"
        )
    return times


def best(times: list[float]) -> float:
    return min(times) if times else float("nan")


def dot_str(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024.0
    return f"{n} B"


# --------------------------------------------------------------------------
# storage walkers


def walk_storage(root: Path) -> dict:
    """Total bytes/files under a lake, bucketed per kind."""
    kinds: dict[str, dict] = {}

    def bucket(name: str) -> dict:
        return kinds.setdefault(name, {"bytes": 0, "files": 0})

    total_bytes = 0
    total_files = 0
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        size = path.stat().st_size
        total_bytes += size
        total_files += 1
        rel = str(path.relative_to(root))
        low = rel.lower()
        if low.startswith("data/") and low.endswith(".vortex"):
            kind = "vortex data"
        elif low.startswith("data/") and low.endswith(".parquet"):
            kind = "parquet data"
        elif low.startswith("data/"):
            kind = "data (other)"
        elif low.startswith("snapshots/"):
            kind = "snapshots"
        elif path.name == "catalog.json":
            kind = "catalog.json"
        elif path.name.startswith("meta.ducklake"):
            kind = "ducklake metadata"
        else:
            kind = "other"
        b = bucket(kind)
        b["bytes"] += size
        b["files"] += 1
    return {"total_bytes": total_bytes, "files": total_files, "per_kind": kinds}


# --------------------------------------------------------------------------
# preflight


def resolve_duckdb(path: str) -> str:
    if os.sep in path:
        p = Path(path)
        if not p.exists():
            die(f"duckdb binary not found: {p}", "pass --duckdb PATH")
        return str(p)
    found = shutil.which(path)
    if not found:
        die(f"duckdb binary not found on PATH: {path}", "pass --duckdb PATH")
    return found


def preflight(pinkdb: Path, duckdb: str) -> dict:
    """Fail with actionable errors before any measurement starts."""
    if not pinkdb.exists():
        die(
            f"pinkdb binary not found: {pinkdb}",
            "build it with: cargo build --release -p pinkdb-cli --features lake",
        )
    if not os.access(pinkdb, os.X_OK):
        die(f"pinkdb binary is not executable: {pinkdb}")

    wall, out, err, rc = run_cmd([str(pinkdb)], ".timer on\n.lake\n.quit\n")
    check_output(out, err, rc, "pinkdb preflight")
    if "built without the lake feature" in out:
        die(
            f"{pinkdb} was built without the lake feature",
            "rebuild with: cargo build --release -p pinkdb-cli --features lake",
        )
    if "Usage: .lake" not in out:
        die(f"pinkdb preflight: unexpected .lake output\n{out}\n{err}")

    pinkdb_info = {"path": str(pinkdb), "bytes": pinkdb.stat().st_size}

    wall, out, err, rc = run_cmd(
        [duckdb], ".timer on\nSELECT version();\n"
    )
    check_output(out, err, rc, "duckdb preflight")
    m = re.search(r"v[\d.]+(?:\s+\([^)]*\))?", out)
    duckdb_version = m.group(0) if m else "(unknown)"
    duckdb_info = {"path": duckdb, "version": duckdb_version}

    # LOAD ducklake, INSTALLing once if the extension is not cached yet.
    wall, out, err, rc = run_cmd([duckdb], ".timer on\nLOAD ducklake;\n")
    installed = False
    if rc != 0 or "error" in out.lower():
        wall, out, err, rc = run_cmd([duckdb], ".timer on\nINSTALL ducklake;\nLOAD ducklake;\n")
        check_output(out, err, rc, "duckdb preflight (INSTALL ducklake)")
        installed = True
    duckdb_info["ducklake_install_needed"] = installed
    return {"pinkdb": pinkdb_info, "duckdb": duckdb_info}


# --------------------------------------------------------------------------
# pink-lake (Vortex) arm


def measure_pink_lake(
    pinkdb: Path, csv: Path, work: Path, rows: int, runs: int, log
) -> dict:
    lake_dir = work / "pink_lake"
    table = "lake.sales"

    log("[pink-lake] ingest: .import + .lake save")
    ingest_in = (
        "CREATE DATABASE lake;\n"
        "USE lake;\n"
        f".import {dot_str(str(csv))} sales\n"
        f".lake save {dot_str(str(lake_dir))}\n"
    )
    wall, out, err, rc = run_cmd([str(pinkdb)], ".timer on\n" + ingest_in, cwd=ROOT)
    check_output(out, err, rc, "pink-lake ingest")
    imp = PINK_IMPORT_RE.search(out)
    sav = PINK_SAVE_RE.search(out)
    if not imp or not sav:
        die(f"pink-lake ingest: could not parse output\n--- output ---\n{out}")
    imported_rows = int(imp.group(1))
    if imported_rows != rows:
        die(f"pink-lake ingest: imported {imported_rows} rows, expected {rows}")
    ingest = {
        "wall_s": wall,
        "import_printed_s": float(imp.group(2)),
        "save_printed_s": float(sav.group(2)),
        "lake_version": int(sav.group(1)),
        "saved_rows": int(sav.group(4)),
        "bytes_written_reported": int(sav.group(5)),
        "command": ingest_in.strip().splitlines(),
    }
    storage = walk_storage(lake_dir)

    log("[pink-lake] cold open (.lake open), fresh process per run")
    open_printed: list[float] = []
    open_wall: list[float] = []
    open_in = f".lake open {dot_str(str(lake_dir))}\n.quit\n"
    for _ in range(runs):
        wall, out, err, rc = run_cmd([str(pinkdb)], ".timer on\n" + open_in, cwd=ROOT)
        check_output(out, err, rc, "pink-lake .lake open")
        m = PINK_OPEN_RE.search(out)
        if not m:
            die(f"pink-lake .lake open: could not parse output\n--- output ---\n{out}")
        open_printed.append(float(m.group(2)))
        open_wall.append(wall)

    # Eager load, then best-of-R per query in one session (queries run from
    # the in-memory store).
    log(f"[pink-lake] {len(QUERIES)} queries x {runs} runs in one open session")
    session_in = f".lake open {dot_str(str(lake_dir))}\n"
    for _ in range(runs):
        for _, template in QUERIES:
            session_in += template.format(t=table) + "\n"
    session_in += ".quit\n"
    wall, out, err, rc = run_cmd([str(pinkdb)], ".timer on\n" + session_in, cwd=ROOT)
    check_output(out, err, rc, "pink-lake query session")
    times = real_times_for(out, len(QUERIES) * runs, "pink-lake query session")
    queries = {}
    for i, (name, _) in enumerate(QUERIES):
        per_run = [times[r * len(QUERIES) + i] for r in range(runs)]
        queries[name] = {
            "best_s": best(per_run),
            "all_s": per_run,
            "session_wall_s": wall,
        }
    if str(rows) not in out:
        die(f"pink-lake query session: result check failed ({rows} not in output)")

    # Cold process, per query: fresh process -> .lake open -> one query ->
    # exit, the same shape as the DuckLake arm's per-query runs, so both
    # engines have directly comparable cold-process numbers.
    log(f"[pink-lake] {len(QUERIES)} queries x {runs} cold-process runs (fresh process each)")
    for name, template in QUERIES:
        query = template.format(t=table)
        query_in = f".lake open {dot_str(str(lake_dir))}\n" + query + "\n.quit\n"
        per_run: list[float] = []
        per_wall: list[float] = []
        for _ in range(runs):
            wall, out, err, rc = run_cmd([str(pinkdb)], ".timer on\n" + query_in, cwd=ROOT)
            check_output(out, err, rc, f"pink-lake cold query {name}")
            # `.timer on` prints one Run Time line per statement; the query is
            # the only statement in this process, so it is the last line.
            times = parse_real_times(out)
            if not times:
                die(f"pink-lake cold query {name}: no timer output\n--- output ---\n{out}")
            per_run.append(times[-1])
            per_wall.append(wall)
        queries[name].update(
            {
                "cold_printed_best_s": best(per_run),
                "cold_printed_all_s": per_run,
                "cold_wall_best_s": best(per_wall),
                "cold_wall_all_s": per_wall,
            }
        )

    # Cold end-to-end: fresh process -> .lake open -> first query -> exit.
    log("[pink-lake] cold end-to-end (start -> open -> first query -> exit)")
    cold_wall: list[float] = []
    cold_in = (
        f".lake open {dot_str(str(lake_dir))}\n"
        + QUERIES[0][1].format(t=table)
        + "\n.quit\n"
    )
    for _ in range(runs):
        wall, out, err, rc = run_cmd([str(pinkdb)], ".timer on\n" + cold_in, cwd=ROOT)
        check_output(out, err, rc, "pink-lake cold end-to-end")
        cold_wall.append(wall)

    return {
        "lake_dir": str(lake_dir),
        "table": table,
        "ingest": ingest,
        "storage": storage,
        "open": {
            "printed_best_s": best(open_printed),
            "printed_all_s": open_printed,
            "wall_best_s": best(open_wall),
            "wall_all_s": open_wall,
        },
        "queries": queries,
        "cold_e2e_wall_best_s": best(cold_wall),
        "cold_e2e_wall_all_s": cold_wall,
    }


# --------------------------------------------------------------------------
# DuckLake arm


def measure_ducklake(duckdb: str, csv: Path, work: Path, rows: int, runs: int, log) -> dict:
    lake_dir = work / "ducklake"
    lake_dir.mkdir(parents=True, exist_ok=True)
    data_dir = lake_dir / "data"
    meta = lake_dir / "meta.ducklake"
    attach = (
        f"ATTACH 'ducklake:{meta}' AS lake (DATA_PATH '{data_dir}');"
    )
    table = "lake.sales"

    log("[ducklake] ingest: INSTALL/LOAD + ATTACH + CREATE TABLE AS SELECT")
    ingest_in = (
        ".timer on\n"
        "INSTALL ducklake;\n"
        "LOAD ducklake;\n"
        f"{attach}\n"
        f"CREATE TABLE lake.sales AS SELECT * FROM read_csv_auto('{csv}');\n"
    )
    wall, out, err, rc = run_cmd([duckdb], ingest_in, cwd=ROOT)
    check_output(out, err, rc, "ducklake ingest")
    times = parse_real_times(out)
    if not times:
        die(f"ducklake ingest: no timer output\n--- output ---\n{out}")
    ingest = {
        "wall_s": wall,
        # Last timed statement is the CREATE TABLE ... AS SELECT (the actual
        # ingest); the earlier timers cover INSTALL/LOAD/ATTACH setup.
        "create_printed_s": times[-1],
        "setup_printed_s": sum(times[:-1]),
        "all_printed_s": times,
        "command": ingest_in.strip().splitlines(),
    }
    storage = walk_storage(lake_dir)

    log(f"[ducklake] {len(QUERIES)} queries x {runs} cold runs (fresh process each)")
    queries: dict[str, dict] = {}
    cold_wall: list[float] = []
    for name, template in QUERIES:
        query = template.format(t=table)
        query_in = (
            ".timer on\n"
            "LOAD ducklake;\n"
            f"{attach}\n"
            f"{query}\n"
        )
        per_run: list[float] = []
        per_wall: list[float] = []
        for _ in range(runs):
            wall, out, err, rc = run_cmd([duckdb], query_in, cwd=ROOT)
            check_output(out, err, rc, f"ducklake query {name}")
            times = parse_real_times(out)
            if not times:
                die(f"ducklake query {name}: no timer output\n--- output ---\n{out}")
            if name == "count" and str(rows) not in out:
                die(f"ducklake query {name}: result check failed ({rows} not in output)")
            per_run.append(times[-1])
            per_wall.append(wall)
        queries[name] = {
            "best_s": best(per_run),
            "all_s": per_run,
            "wall_best_s": best(per_wall),
            "wall_all_s": per_wall,
        }
        if name == QUERIES[0][0]:
            cold_wall = per_wall

    return {
        "lake_dir": str(lake_dir),
        "table": table,
        "ingest": ingest,
        "storage": storage,
        "open": {
            # DuckLake has no explicit open: ATTACH in each fresh process is
            # the equivalent; its cost is inside the cold-query wall times.
            "attach_sql": attach,
        },
        "queries": queries,
        "cold_e2e_wall_best_s": best(cold_wall),
        "cold_e2e_wall_all_s": cold_wall,
    }


# --------------------------------------------------------------------------
# reporting


def render_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    out = [sep]
    out.append("|" + "|".join(f" {h:<{w}} " for h, w in zip(headers, widths)) + "|")
    out.append(sep)
    for row in rows:
        out.append("|" + "|".join(f" {c:<{w}} " for c, w in zip(row, widths)) + "|")
    out.append(sep)
    return "\n".join(out)


DATA_KINDS = ("vortex data", "parquet data", "data (other)")


def kind_totals(storage: dict, data: bool) -> tuple[int, int]:
    kinds = storage["per_kind"]
    selected = [(k, b) for k, b in kinds.items() if (k in DATA_KINDS) == data]
    return (
        sum(b["bytes"] for _, b in selected),
        sum(b["files"] for _, b in selected),
    )


def side_by_side(pink: dict, duck: dict, rows: int) -> str:
    p_ing, d_ing = pink["ingest"], duck["ingest"]
    p_st, d_st = pink["storage"], duck["storage"]
    p_open = pink["open"]

    lines = []
    lines.append("-- ingest (CSV -> lake), wall clock --")
    p_data = kind_totals(p_st, data=True)
    p_meta = kind_totals(p_st, data=False)
    d_data = kind_totals(d_st, data=True)
    d_meta = kind_totals(d_st, data=False)
    lines.append(
        render_table(
            ["metric", "pink-lake (Vortex)", "DuckLake (DuckDB)"],
            [
                ["rows", f"{rows:,}", f"{rows:,}"],
                ["ingest wall (s)", f"{p_ing['wall_s']:.3f}", f"{d_ing['wall_s']:.3f}"],
                [
                    "ingest printed (s)",
                    f"{p_ing['save_printed_s']:.3f} (lake save)",
                    f"{d_ing['create_printed_s']:.3f} (CTAS)",
                ],
                [
                    "  import/scan printed (s)",
                    f"{p_ing['import_printed_s']:.3f} (import)",
                    f"{d_ing['setup_printed_s']:.3f} (install+load+attach)",
                ],
                ["stored total", human_bytes(p_st["total_bytes"]), human_bytes(d_st["total_bytes"])],
                ["stored files", str(p_st["files"]), str(d_st["files"])],
                [
                    "  data",
                    f"{human_bytes(p_data[0])} ({p_data[1]} files)",
                    f"{human_bytes(d_data[0])} ({d_data[1]} files)",
                ],
                [
                    "  metadata",
                    f"{human_bytes(p_meta[0])} ({p_meta[1]} files)",
                    f"{human_bytes(d_meta[0])} ({d_meta[1]} files)",
                ],
            ],
        )
    )
    lines.append("")
    lines.append("-- cold open --")
    lines.append(
        render_table(
            ["metric", "pink-lake (Vortex)", "DuckLake (DuckDB)"],
            [
                [
                    "open printed best (s)",
                    f"{p_open.get('printed_best_s', float('nan')):.3f} (.lake open)",
                    "n/a (ATTACH, in cold wall)",
                ],
                [
                    "open wall best (s)",
                    f"{p_open.get('wall_best_s', float('nan')):.3f} (process+.lake open+.quit)",
                    "n/a (no explicit open)",
                ],
                [
                    "cold e2e wall best (s)",
                    f"{pink['cold_e2e_wall_best_s']:.3f}",
                    f"{duck['cold_e2e_wall_best_s']:.3f}",
                ],
            ],
        )
    )
    lines.append("")
    lines.append("-- queries: best-of-R wall time (ms) --")
    qrows = []
    for name, _ in QUERIES:
        p_q = pink["queries"].get(name, {})
        d_q = duck["queries"].get(name, {})
        p_ms = p_q.get("best_s", float("nan")) * 1000
        d_ms = d_q.get("best_s", float("nan")) * 1000
        if not d_ms or not p_ms:
            ratio = "n/a"
        elif p_ms < 0.5:
            ratio = f"<{0.5 / d_ms:.2f}x"
        elif d_ms < 0.5:
            ratio = f">{p_ms / 0.5:.2f}x"
        else:
            ratio = f"{p_ms / d_ms:.2f}x"
        qrows.append(
            [name, f"{p_ms:.2f}", f"{d_ms:.2f} (cold)", ratio]
        )
    lines.append(
        render_table(
            ["query", "pink-lake best (ms)", "DuckLake best (ms)", "pink/duck"],
            qrows,
        )
    )
    lines.append(
        "(both CLIs print the timer as `real X.XXX`, i.e. 1 ms resolution, so "
        "0.00 means < 1 ms)"
    )
    lines.append("")
    lines.append("-- per-query wall, in-session (pink-lake, in-memory) vs fresh process (DuckLake) --")
    wrows = []
    for name, _ in QUERIES:
        p_q = pink["queries"].get(name, {})
        d_q = duck["queries"].get(name, {})
        wrows.append(
            [
                name,
                f"{p_q.get('best_s', float('nan')) * 1000:.2f} (in-memory)",
                f"{d_q.get('wall_best_s', float('nan')) * 1000:.2f} (LOAD+ATTACH+query+exit)",
            ]
        )
    lines.append(render_table(["query", "pink-lake (ms)", "DuckLake (ms)"], wrows))
    lines.append("")
    lines.append(
        "-- per-query cold process (fresh process -> open/attach -> one query -> exit) --"
    )
    crows = []
    for name, _ in QUERIES:
        p_q = pink["queries"].get(name, {})
        d_q = duck["queries"].get(name, {})
        p_cold = p_q.get("cold_wall_best_s", float("nan")) * 1000
        d_cold = d_q.get("wall_best_s", float("nan")) * 1000
        ratio = "n/a" if not d_cold else f"{p_cold / d_cold:.2f}x"
        crows.append([name, f"{p_cold:.2f}", f"{d_cold:.2f}", ratio])
    lines.append(
        render_table(
            ["query", "pink-lake cold (ms)", "DuckLake cold (ms)", "pink/duck"], crows
        )
    )
    lines.append(
        "(cold wall = process start + open/attach + query + exit on both sides; "
        "pink-lake's open eagerly loads Vortex while DuckLake's attach is lazy "
        "and each query scans Parquet)"
    )
    return "\n".join(lines)


def fairness_notes(rows: int) -> list[str]:
    return [
        "Fairness notes (read before drawing conclusions):",
        f"- Same data ({rows:,} rows, bench/gen_sales.py seed 42), same four SQL",
        "  queries; the table is `lake.sales` on both sides (pink-lake is told to",
        "  import into a database named `lake`), so the SQL text is identical.",
        "- DIFFERENT ARCHITECTURES: pink-lake's `.lake open` eagerly loads the whole",
        "  table from Vortex back into the in-memory engine, so its in-session query",
        "  timings are in-memory scans and the load cost is the separate `.lake open`",
        "  row. DuckLake is lazy: ATTACH only opens the SQL catalog (SQLite metadata),",
        "  and every query scans Parquet at query time; no in-memory warm query",
        "  number exists for DuckLake. The per-query cold-process table measures BOTH",
        "  engines as fresh process -> open/attach -> one query -> exit, so those",
        "  numbers include each engine's open/attach cost and are directly comparable.",
        "- DuckLake is a full lakehouse format: transactional SQL catalog, MVCC-ish",
        "  snapshots and time travel. This run is a single ingest; DuckLake's",
        "  catalog DB (meta.ducklake) grows with every write/snapshot, while",
        "  pink-lake publishes a full snapshot per save and keeps old versions'",
        "  files (this run has one data file and one snapshot).",
        "- `INSTALL ducklake` runs once in the ingest process and is cached",
        "  afterwards (~0 ms here); cold-query processes use LOAD only.",
        "- Both arms are time-to-first-result from a fresh process for the cold",
        "  end-to-end row; CPU/disk cache state after the ingest favors both arms",
        "  equally and no cache was dropped.",
        "- Both CLIs print timings with 1 ms resolution (`real X.XXX`), so a",
        "  printed 0.000 means < 1 ms and per-run noise of ±1 ms is expected;",
        "  best-of-R is used everywhere to blunt that.",
    ]


# --------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="pink-lake (Vortex) vs DuckLake benchmark",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--rows", type=int, default=DEFAULT_ROWS, help="row count")
    ap.add_argument("--runs", type=int, default=5, help="repetitions, best-of")
    ap.add_argument("--keep", action="store_true", help="keep temp dirs")
    ap.add_argument(
        "--pinkdb", default=str(ROOT / "target/release/pinkdb"), help="pinkdb CLI"
    )
    ap.add_argument("--duckdb", default="duckdb", help="duckdb CLI")
    ap.add_argument("--json", default=None, help="also write the JSON dump here")
    args = ap.parse_args()

    if args.runs < 1:
        die("--runs must be >= 1")
    if args.rows < 1:
        die("--rows must be >= 1")

    pinkdb = Path(args.pinkdb).resolve()
    duckdb = resolve_duckdb(args.duckdb)
    tmp = Path(tempfile.mkdtemp(prefix="lake_bench_"))
    load_at_start = os.getloadavg() if hasattr(os, "getloadavg") else None

    def log(msg: str) -> None:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

    print("=== lake benchmark: pink-lake (Vortex) vs DuckLake ===")
    print(f"temp dir: {tmp}  (kept with --keep)")
    print(f"cpus: {os.cpu_count()}  platform: {platform.platform()}")
    if load_at_start:
        print(f"load average at start: {load_at_start[0]:.2f} {load_at_start[1]:.2f} {load_at_start[2]:.2f}")
    print()

    # --- data file -------------------------------------------------------
    csv = DEFAULT_CSV
    generated = False
    if args.rows == DEFAULT_ROWS and DEFAULT_CSV.exists():
        log(f"using existing {csv}")
    else:
        csv = tmp / f"sales_{args.rows}.csv"
        if not GEN_SALES.exists():
            die(f"missing generator: {GEN_SALES}")
        log(f"generating {args.rows:,} rows -> {csv}")
        wall, out, err, rc = run_cmd(
            [sys.executable, str(GEN_SALES), str(args.rows), str(csv)], ""
        )
        check_output(out, err, rc, "gen_sales.py")
        generated = True
        log(f"generated in {wall:.1f}s ({human_bytes(csv.stat().st_size)})")
    csv = csv.resolve()

    pre = preflight(pinkdb, duckdb)
    print(
        f"pinkdb: {pre['pinkdb']['path']} ({human_bytes(pre['pinkdb']['bytes'])}, lake feature)"
    )
    print(
        f"duckdb: {pre['duckdb']['path']} ({pre['duckdb']['version']}, ducklake"
        f"{' freshly installed' if pre['duckdb']['ducklake_install_needed'] else ' already cached'})"
    )
    print()

    try:
        pink = measure_pink_lake(pinkdb, csv, tmp, args.rows, args.runs, log)
        duck = measure_ducklake(duckdb, csv, tmp, args.rows, args.runs, log)

        print()
        print(side_by_side(pink, duck, args.rows))
        print()
        print("\n".join(fairness_notes(args.rows)))
        print()

        doc = {
            "harness": "bench/lake_bench.py",
            "rows": args.rows,
            "runs": args.runs,
            "csv": str(csv),
            "csv_bytes": csv.stat().st_size,
            "csv_generated": generated,
            "pinkdb": pre["pinkdb"],
            "duckdb": pre["duckdb"],
            "pink_lake": pink,
            "ducklake": duck,
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

    load_at_end = os.getloadavg() if hasattr(os, "getloadavg") else None
    if load_at_start and load_at_end:
        print(
            f"load average at end: {load_at_end[0]:.2f} {load_at_end[1]:.2f} {load_at_end[2]:.2f}"
        )


if __name__ == "__main__":
    main()
