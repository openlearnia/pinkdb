#!/usr/bin/env python3
"""pinkdb benchmark suite.

One runner for the families that matter to this engine: TPC-H against the
local DuckDB CLI, ingest, durable writes, the lake, search and the release
footprint. It writes ``results/<date>-<commit>.json`` plus a Markdown
summary, records the machine and the load it ran under, and keeps every
timed number a median of warm runs.

    python3 bench/suite/run.py                    # all families, SF 0.1
    python3 bench/suite/run.py --only tpch --scale 1
    python3 bench/suite/run.py --only ci          # the small regression guard

Only the Python standard library is used. DuckDB is the reference engine and
is never asked to ``INSTALL`` anything; TPC-H data comes from its built-in
``dbgen``. Nothing here downloads a file or talks to a real bucket.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DUCKDB = os.environ.get("DUCKDB", "/tmp/duckdb-2.0.0/duckdb")

# The CLI features the suite needs: the lake, both catalog backends, Parquet,
# FTS and HNSW. `--no-default-features` builds stay available for footprint.
CLI_FEATURES = "pinklake,s3,catalog-sqlite,catalog-s3,parquetlake,parquet,fts,vss"

TPCH_TABLES = [
    "lineitem",
    "orders",
    "partsupp",
    "part",
    "customer",
    "supplier",
    "nation",
    "region",
]

TIMER_RE = re.compile(r"Run Time \(s\): real ([0-9.]+)")
IMPORT_RE = re.compile(r"Imported (\d+) rows in ([0-9.]+)s")

FAMILIES = ["tpch", "ingest", "durable", "lake", "search", "footprint", "ci"]


def now() -> float:
    return time.monotonic()


def sh(
    args,
    input_text: str | None = None,
    timeout: float = 600,
    env: dict | None = None,
    cwd: Path = ROOT,
) -> subprocess.CompletedProcess:
    """Run a command, capturing text output; never raises on a non-zero exit."""
    merged = dict(os.environ)
    merged.pop("PINKDB_AGENT_MODE", None)
    if env:
        merged.update(env)
    try:
        return subprocess.run(
            [str(a) for a in args],
            input=input_text,
            text=True,
            capture_output=True,
            timeout=timeout,
            env=merged,
            cwd=str(cwd),
        )
    except subprocess.TimeoutExpired as exc:
        # The timeout payload is bytes even with text=True, and callers
        # concatenate it with str.
        def text(x):
            return x.decode("utf-8", "replace") if isinstance(x, bytes) else (x or "")

        return subprocess.CompletedProcess(args, 124, text(exc.stdout), text(exc.stderr))


def timer_values(out: str) -> list[float]:
    return [float(x) for x in TIMER_RE.findall(out)]


def load_average() -> list[float]:
    try:
        return list(os.getloadavg())
    except OSError:
        return []


def busy_reason() -> str | None:
    """A rough quiet-machine check: a build or another benchmark is enough to
    make a timed run meaningless, so the suite records it rather than hiding it."""
    probe = sh(["pgrep", "-fl", "cargo (build|bench|run)|pinkdb_bench|bench/suite"], timeout=5)
    lines = [l for l in probe.stdout.splitlines() if "pgrep" not in l]
    return "; ".join(lines[:3]) if lines else None


def machine_info() -> dict:
    def sysctl(name: str) -> str:
        r = sh(["sysctl", "-n", name], timeout=10)
        return r.stdout.strip()

    os_name = platform.platform()
    version = sh(["sw_vers", "-productVersion"], timeout=10).stdout.strip()
    rustc = sh(["rustc", "-V"], timeout=10).stdout.strip()
    branch = sh(["git", "rev-parse", "--abbrev-ref", "HEAD"]).stdout.strip()
    commit = sh(["git", "rev-parse", "HEAD"]).stdout.strip()
    short = sh(["git", "rev-parse", "--short", "HEAD"]).stdout.strip()
    dirty = sh(["git", "status", "--porcelain"]).stdout.strip()
    return {
        "host": platform.node(),
        "os": os_name,
        "os_version": version,
        "cpu": sysctl("machdep.cpu.brand_string"),
        "physical_cores": int(sysctl("hw.physicalcpu") or 0),
        "logical_cores": int(sysctl("hw.logicalcpu") or 0),
        "memory_gb": round(int(sysctl("hw.memsize") or 0) / 1e9, 2),
        "rustc": rustc,
        "duckdb": sh([DEFAULT_DUCKDB, "--version"]).stdout.strip(),
        "branch": branch,
        "commit": commit,
        "commit_short": short,
        "dirty": bool(dirty),
        "dirty_files": len(dirty.splitlines()),
        "python": platform.python_version(),
    }


# --------------------------------------------------------------------------
# Engine drivers


def pinkdb_bin() -> Path:
    return ROOT / "target" / "release" / "pinkdb"


def pink(script: str, timeout: float = 600, env: dict | None = None, agent: bool = False):
    return sh(
        [pinkdb_bin(), "-agent" if agent else "-no-agent"],
        input_text=script,
        timeout=timeout,
        env=env,
    )


def duck(script: str, db: Path | str | None = None, timeout: float = 600):
    args = [DEFAULT_DUCKDB]
    if db is not None:
        args.append(str(db))
    args.append("-no-agent")
    return sh(args, input_text=script, timeout=timeout)


def parse_box(out: str) -> list[list[str]]:
    """The last box table: data rows only (the header and type rows are before
    the first `├`, the bottom border closes the block)."""
    rows: list[list[str]] = []
    inside = False
    for line in out.splitlines():
        if line.startswith("├"):
            rows, inside = [], True
            continue
        if inside and line.startswith("└"):
            break
        if inside and line.startswith("│"):
            cells = [c.strip() for c in line.strip("│").split("│")]
            rows.append(cells)
    return rows


def parse_md(out: str) -> list[list[str]]:
    """Data rows of the last Markdown table (agent mode)."""
    lines = [l for l in out.splitlines() if l.startswith("|")]
    rows = []
    for line in lines[2:]:
        cells = [c.strip().replace("&#124;", "|") for c in line.strip("|").split("|")]
        rows.append(cells)
    return rows


def is_null(cell: str) -> bool:
    return cell == "" or cell.upper() == "NULL"


def as_number(cell: str):
    if is_null(cell):
        return None
    try:
        return float(cell)
    except ValueError:
        return None


def canonical_rows(rows: list[list[str]]) -> tuple[list[tuple[str, ...]], list[tuple[str, ...]]]:
    """(sorted canonical rows for hashing, rounded rows for a readable diff)."""
    exact, rounded = [], []
    for row in rows:
        cells, nums = [], []
        for cell in row:
            if is_null(cell):
                cells.append("∅")
                nums.append("∅")
            else:
                n = as_number(cell)
                if n is not None:
                    cells.append(f"{n:.9g}")
                    nums.append(f"{n:.12g}")
                else:
                    cells.append(cell)
                    nums.append(cell)
        exact.append(tuple(cells))
        rounded.append(tuple(nums))
    return sorted(exact), sorted(rounded)


def checksum(rows: list[list[str]]) -> str:
    exact, _ = canonical_rows(rows)
    payload = "\n".join("\t".join(r) for r in exact).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


def rows_equal(a: list[list[str]], b: list[list[str]]) -> tuple[bool, str]:
    if len(a) != len(b):
        return False, f"row count {len(a)} vs {len(b)}"
    wide = max((len(r) for r in a + b), default=0)
    if any(len(r) != wide for r in a + b):
        return False, "column count differs"
    _, ra = canonical_rows(a)
    _, rb = canonical_rows(b)
    for i, (x, y) in enumerate(zip(ra, rb)):
        for j, (p, q) in enumerate(zip(x, y)):
            if p == q:
                continue
            fn, rn = as_number(p), as_number(q)
            if fn is not None and rn is not None:
                if abs(fn - rn) <= max(1e-9 * max(abs(fn), abs(rn)), 1e-12):
                    continue
            return False, f"row {i} col {j}: {p!r} vs {q!r}"
    return True, ""


def median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


# --------------------------------------------------------------------------
# TPC-H data and stores


def tpch_dir(work: Path, scale: float) -> Path:
    return work / f"tpch_sf{scale:g}"


def ensure_tpch(work: Path, scale: float, force: bool = False) -> Path:
    """Generate TPC-H into Parquet with DuckDB's built-in dbgen."""
    base = tpch_dir(work, scale)
    pq = base / "parquet"
    gen = base / "gen.duckdb"
    complete = all((pq / f"{t}.parquet").exists() for t in TPCH_TABLES)
    if complete and not force:
        return pq
    if force:
        shutil.rmtree(base, ignore_errors=True)
    pq.mkdir(parents=True, exist_ok=True)
    copies = "".join(
        f"COPY {t} TO '{pq / (t + '.parquet')}' (FORMAT parquet);\n" for t in TPCH_TABLES
    )
    script = f"CALL dbgen(sf={scale});\n{copies}"
    r = duck(script, gen, timeout=1800)
    if r.returncode != 0:
        raise SystemExit(f"dbgen failed: {r.stderr[-2000:]}")
    return pq


def build_pink_store(pq: Path, store: Path, scale: float) -> dict:
    store.unlink(missing_ok=True)
    Path(str(store) + ".wal").unlink(missing_ok=True)
    Path(str(store) + ".lock").unlink(missing_ok=True)
    lines = [".timer on"]
    for t in TPCH_TABLES:
        lines.append(f"CREATE TABLE {t} AS SELECT * FROM read_parquet('{pq / (t + '.parquet')}');")
    lines.append(f".save {store}")
    t0 = now()
    r = pink("\n".join(lines) + "\n", timeout=3600)
    elapsed = now() - t0
    if r.returncode != 0:
        raise SystemExit(f"pinkdb store build failed: {r.stderr[-2000:]}")
    return {"seconds": elapsed, "bytes": store.stat().st_size if store.exists() else 0}


def build_duck_store(pq: Path, db: Path, threads: int) -> dict:
    db.unlink(missing_ok=True)
    lines = [f"SET threads={threads};"]
    for t in TPCH_TABLES:
        lines.append(f"CREATE TABLE {t} AS SELECT * FROM read_parquet('{pq / (t + '.parquet')}');")
    t0 = now()
    r = duck("\n".join(lines) + "\n", db, timeout=3600)
    elapsed = now() - t0
    if r.returncode != 0:
        raise SystemExit(f"duckdb store build failed: {r.stderr[-2000:]}")
    return {"seconds": elapsed, "bytes": db.stat().st_size if db.exists() else 0}


def tpch_queries() -> dict[int, str]:
    # The query text contains newlines, so ask DuckDB for JSON rather than a
    # box table.
    r = sh(
        [DEFAULT_DUCKDB, "-json", "-c", "SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr;"],
        timeout=120,
    )
    rows = json.loads(r.stdout)
    queries = {}
    for row in rows:
        # DuckDB's text runs unchanged (Q22's `substring(... FROM 1 FOR 2)`
        # parses since wave 126b).
        queries[int(row["query_nr"])] = row["query"].strip()
    return queries


# --------------------------------------------------------------------------
# Families


class Suite:
    def __init__(self, args):
        self.args = args
        self.work = Path(args.work_dir).expanduser()
        self.work.mkdir(parents=True, exist_ok=True)
        self.threads = args.threads
        os.environ["RAYON_NUM_THREADS"] = str(args.threads)
        self.results: dict = {}
        self.machine = machine_info()
        self.started = time.strftime("%Y-%m-%dT%H:%M:%S")
        self.loads: list[dict] = []
        self.duck_script = f"SET threads={self.threads};\n"

    def note_load(self, stage: str):
        self.loads.append(
            {"stage": stage, "at": time.strftime("%H:%M:%S"), "load": load_average(), "busy": busy_reason()}
        )

    def report(self) -> str:
        return f"{self.machine['commit_short'] or 'nocommit'} dirty={self.machine['dirty_files']}"

    def tpch(self):
        scale = self.args.scale
        pq = ensure_tpch(self.work, scale, force=self.args.regen)
        store = tpch_dir(self.work, scale) / "store.pink"
        db = tpch_dir(self.work, scale) / "store.duckdb"
        if not store.exists() or self.args.rebuild:
            print("  building the pinkdb store ...", flush=True)
            info = build_pink_store(pq, store, scale)
            print(f"  pinkdb store: {info['seconds']:.1f}s, {info['bytes'] / 1e6:.1f} MB", flush=True)
        if not db.exists() or self.args.rebuild:
            print("  building the DuckDB store ...", flush=True)
            info = build_duck_store(pq, db, self.threads)
            print(f"  duckdb store: {info['seconds']:.1f}s, {info['bytes'] / 1e6:.1f} MB", flush=True)

        queries = tpch_queries()
        timeout = 900 if scale >= 1 else 300
        rows = []
        for nr in sorted(queries):
            q = queries[nr]
            self.note_load(f"tpch q{nr}")
            duck_rows = self._output_rows("duck", q, db, timeout)
            pink_rows = self._output_rows("pink", q, store, timeout)
            ok, why = rows_equal(pink_rows, duck_rows)
            if not ok:
                rows.append(
                    {
                        "query": nr,
                        "status": "mismatch",
                        "reason": why,
                        "pinkdb_rows": len(pink_rows),
                        "duckdb_rows": len(duck_rows),
                        "pinkdb_checksum": checksum(pink_rows),
                        "duckdb_checksum": checksum(duck_rows),
                    }
                )
                print(f"  q{nr:<2} MISMATCH: {why}", flush=True)
                continue
            ptimes, perr = self._timed("pink", q, store, self.args.runs, self.args.warmup, timeout)
            dtimes, derr = self._timed("duck", q, db, self.args.runs, self.args.warmup, timeout)
            both = median(ptimes)
            dmed = median(dtimes)
            ratio = both / dmed if both and dmed else None
            status = "ok" if ptimes and dtimes else ("timeout" if perr == "timeout" or derr == "timeout" else "error")
            rows.append(
                {
                    "query": nr,
                    "status": status,
                    "pinkdb_ms": both,
                    "duckdb_ms": dmed,
                    "ratio": ratio,
                    "pinkdb_runs_ms": ptimes,
                    "duckdb_runs_ms": dtimes,
                    "rows": len(pink_rows),
                    "checksum": checksum(pink_rows),
                    "error": perr if perr not in (None, "timeout") else derr,
                }
            )
            print(
                f"  q{nr:<2} pink={both and round(both, 2)}ms duck={dmed and round(dmed, 2)}ms "
                f"ratio={ratio and round(ratio, 2)} rows={len(pink_rows)}",
                flush=True,
            )
        self.results["tpch"] = {
            "scale": scale,
            "threads": {"pinkdb": self.threads, "duckdb": self.threads},
            "warmup": self.args.warmup,
            "runs": self.args.runs,
            "parquet": str(pq),
            "pinkdb_store_bytes": store.stat().st_size if store.exists() else 0,
            "duckdb_store_bytes": db.stat().st_size if db.exists() else 0,
            "queries": rows,
        }

    def _output_rows(self, engine: str, sql: str, where, timeout: float) -> list[list[str]]:
        if engine == "pink":
            # Agent mode has no 10k-row display cap (the box renderer's
            # DISPLAY_CAP), but it does cap a statement at 1M materialized
            # rows; `.agent budget 0` lifts that too, so large TPC-H joins
            # can be checked row for row.
            script = f".open {where}\n.agent rows 0\n.agent budget 0\n{sql}\n"
            r = pink(script, timeout=timeout, agent=True)
            return parse_md(r.stdout + "\n" + r.stderr)
        script = f"{self.duck_script}.maxrows 1000000\n{sql}\n"
        r = duck(script, where, timeout=timeout)
        return parse_box(r.stdout + "\n" + r.stderr)

    def _timed(self, engine: str, sql: str, where, runs: int, warmup: int, timeout: float):
        if runs <= 0:
            return [], None
        body = "\n".join([sql] * (runs + warmup))
        if engine == "pink":
            script = f".open {where}\n.timer on\n{body}\n"
            r = pink(script, timeout=timeout)
        else:
            script = f"{self.duck_script}.timer on\n{body}\n"
            r = duck(script, where, timeout=timeout)
        times = timer_values(r.stdout + r.stderr)
        if not times:
            err = [l for l in (r.stdout + r.stderr).splitlines() if l.startswith("Error:")]
            return [], ("timeout" if r.returncode == 124 else (err[0] if err else "no timer output"))
        return [t * 1000.0 for t in times[-runs:]], None

    # -- ingest --------------------------------------------------------

    def ingest(self):
        scale = self.args.scale
        pq = ensure_tpch(self.work, scale, force=False)
        base = tpch_dir(self.work, scale)
        csv_dir = base / "csv"
        csv_dir.mkdir(exist_ok=True)
        targets = ["lineitem", "orders"]
        for t in targets:
            f = csv_dir / f"{t}.csv"
            if not f.exists():
                duck(
                    f"COPY {t} TO '{f}' (HEADER, DELIMITER ',');",
                    base / "gen.duckdb",
                    timeout=900,
                )
        probe = duck(
            "SELECT 'lineitem', count(*) FROM lineitem UNION ALL SELECT 'orders', count(*) FROM orders;",
            base / "gen.duckdb",
        )
        counts = {}
        for row in parse_box(probe.stdout):
            counts[row[0]] = int(row[1])

        out = []
        for t in targets:
            for fmt, path in (("csv", csv_dir / f"{t}.csv"), ("parquet", pq / f"{t}.parquet")):
                for engine in ("pink", "duck"):
                    samples = []
                    for _ in range(3):
                        if engine == "pink":
                            store = self.work / f"ingest_{fmt}_{t}.pink"
                            store.unlink(missing_ok=True)
                            if fmt == "csv":
                                script = f".open {store}\n.import {path} {t}\n"
                                r = pink(script, timeout=1800)
                                m = IMPORT_RE.search(r.stdout + r.stderr)
                                ms = float(m.group(2)) * 1000 if m else None
                            else:
                                script = (
                                    f".open {store}\n.timer on\n"
                                    f"CREATE TABLE {t} AS SELECT * FROM read_parquet('{path}');\n"
                                )
                                r = pink(script, timeout=1800)
                                tv = timer_values(r.stdout + r.stderr)
                                ms = tv[-1] * 1000 if tv else None
                            for p in (store, Path(str(store) + ".wal"), Path(str(store) + ".lock")):
                                p.unlink(missing_ok=True)
                        else:
                            db = self.work / f"ingest_{fmt}_{t}.duckdb"
                            db.unlink(missing_ok=True)
                            fn = "read_csv_auto" if fmt == "csv" else "read_parquet"
                            extra = ", header=true" if fmt == "csv" else ""
                            script = (
                                f"{self.duck_script}.timer on\n"
                                f"CREATE TABLE {t} AS SELECT * FROM {fn}('{path}'{extra});\n"
                            )
                            r = duck(script, db, timeout=1800)
                            tv = timer_values(r.stdout + r.stderr)
                            ms = tv[-1] * 1000 if tv else None
                            db.unlink(missing_ok=True)
                        if ms is None:
                            raise SystemExit(f"ingest {engine} {fmt} {t} failed: {(r.stdout + r.stderr)[-1500:]}")
                        samples.append(ms)
                    med = median(samples)
                    size = path.stat().st_size
                    out.append(
                        {
                            "table": t,
                            "format": fmt,
                            "engine": engine,
                            "rows": counts[t],
                            "bytes": size,
                            "median_ms": med,
                            "runs_ms": samples,
                            "rows_per_s": counts[t] / (med / 1000.0),
                            "mb_per_s": size / 1e6 / (med / 1000.0),
                        }
                    )
                    print(
                        f"  {t:<8} {fmt:<7} {engine:<4} {med:8.1f} ms "
                        f"{counts[t] / (med / 1000.0) / 1e6:7.2f} M rows/s",
                        flush=True,
                    )
        self.results["ingest"] = {"scale": scale, "tables": out}

    # -- durable -------------------------------------------------------

    def durable(self):
        n = self.args.durable_rows
        bench = self._durable_binary()
        out = {"n": n, "bench": {}}
        if bench:
            for tag, env in (("Sync", {}), ("Batch", {"PINKDB_BENCH_BATCH": "1"})):
                r = sh(
                    [bench],
                    timeout=1800,
                    env={"PINKDB_BENCH_N": str(n), "PINKDB_BENCH_CHECKPOINTS": "3", **env},
                )
                vals = dict(
                    (k, float(v))
                    for k, v in re.findall(r"(inserts_ms|selects_ms|checkpoints_ms)=([0-9.]+)", r.stdout)
                )
                out["bench"][tag] = vals
                print(f"  durable {tag}: {vals}", flush=True)
        # Single-row autocommit through the CLI: the client-visible cost.
        store = self.work / "durable_cli.pink"
        for tag, rows in (("autocommit", n), ("batched", n)):
            store.unlink(missing_ok=True)
            lines = [".timer on", "CREATE TABLE t (i BIGINT);"]
            if tag == "autocommit":
                lines += [f"INSERT INTO t VALUES ({i});" for i in range(rows)]
            else:
                values = ",".join(f"({i})" for i in range(rows))
                lines.append(f"INSERT INTO t VALUES {values};")
            t0 = now()
            r = pink("\n".join(lines) + "\n", timeout=1800)
            wall = now() - t0
            if r.returncode != 0:
                out[tag] = {"error": (r.stdout + r.stderr)[-800:]}
            else:
                out[tag] = {"rows": rows, "wall_s": wall, "rows_per_s": rows / wall}
            print(f"  cli {tag}: {out[tag]}", flush=True)
            store.unlink(missing_ok=True)
        # Multi-process writers: the checked-in harness, with small counts.
        mp = sh(
            [
                sys.executable,
                str(ROOT / "bench" / "mp_optimistic.py"),
                "--bin",
                str(pinkdb_bin()),
                "--procs",
                "2",
                "--txns",
                "50",
                "--runs",
                "1",
                "--rows",
                "100",
                "--read-rows",
                "1000",
                "--reads-per-txn",
                "5",
            ],
            timeout=1800,
        )
        out["multi_process"] = {
            "mode": dict(
                (m, (float(w), s))
                for m, w, s in re.findall(r"mode=(\w+) workload=\w+ procs=\d+ txns=\d+ wall_s=([0-9.]+)\nverify=(\w+)", mp.stdout)
            ),
            "output": mp.stdout.strip().splitlines()[-1:] if mp.returncode == 0 else (mp.stdout + mp.stderr)[-500:],
        }
        print(f"  multi-process: {out['multi_process']['output']}", flush=True)
        self.results["durable"] = out

    def _durable_binary(self) -> Path | None:
        cache = self.work / "durable.path"
        if cache.exists():
            p = Path(cache.read_text().strip())
            if p.exists():
                return p
        r = sh(
            ["cargo", "bench", "-p", "pinkdb", "--bench", "durable", "--no-run", "--message-format=json"],
            timeout=3600,
        )
        exe = None
        for line in r.stdout.splitlines():
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if msg.get("reason") == "compiler-artifact" and msg.get("executable"):
                exe = msg["executable"]
        if not exe:
            print("  durable bench binary not found; skipping the WAL policies", flush=True)
            return None
        cache.write_text(exe)
        return Path(exe)

    # -- lake ----------------------------------------------------------

    def lake(self):
        scale = self.args.scale
        base = tpch_dir(self.work, scale)
        pq = ensure_tpch(self.work, scale)
        store = base / "store.pink"
        if not store.exists():
            build_pink_store(pq, store, scale)
        out = {"scale": scale}
        lake_root = self.work / "lake"
        shutil.rmtree(lake_root, ignore_errors=True)
        lake_root.mkdir(parents=True)
        catalogs = {}

        def save(tag: str, catalog: str, data: str | None):
            cmd = f".lake save {catalog}" + (f" data {data}" if data else "")
            script = f".open {store}\n{cmd}\n.quit\n"
            t0 = now()
            r = pink(script, timeout=3600)
            wall = now() - t0
            if r.returncode != 0:
                return {"error": (r.stdout + r.stderr)[-1200:], "seconds": wall}
            return {"seconds": wall, "catalog": catalog}

        def scan(catalog: str, runs: int = 5, env: dict | None = None) -> dict:
            q = "SELECT sum(l_extendedprice) FROM lineitem;"
            r = pink(
                f".lake attach {catalog} read-only\n.timer on\n" + (q + "\n") * runs + ".quit\n",
                timeout=1800,
                env=env,
            )
            tv = timer_values(r.stdout + r.stderr)
            if not tv:
                return {"error": (r.stdout + r.stderr)[-1200:]}
            return {"runs_ms": [t * 1000 for t in tv], "median_ms": median([t * 1000 for t in tv])}

        def open_time(catalog: str) -> dict:
            # Startup is subtracted so the number is the attach itself.
            starts = []
            for _ in range(3):
                t0 = now()
                pink(".quit\n")
                starts.append((now() - t0) * 1000)
            walls = []
            for _ in range(3):
                t0 = now()
                pink(f".lake attach {catalog} read-only\n.quit\n", timeout=900)
                walls.append((now() - t0) * 1000)
            return {
                "startup_ms": median(starts),
                "attach_wall_ms": median(walls),
                "attach_ms": median(walls) - median(starts) if walls and starts else None,
            }

        pink_cat = f"pink:{lake_root / 'pink' / 'catalog.pink'}"
        catalogs["pink"] = save("pink", pink_cat, str(lake_root / "pink" / "data"))
        if "error" not in catalogs["pink"]:
            catalogs["pink"].update(scan(pink_cat))
            catalogs["pink"].update(open_time(pink_cat))

        sqlite_cat = f"sqlite:{lake_root / 'sqlite' / 'catalog.db'}"
        catalogs["sqlite"] = save("sqlite", sqlite_cat, str(lake_root / "sqlite" / "data"))
        if "error" not in catalogs["sqlite"]:
            catalogs["sqlite"].update(scan(sqlite_cat))
            catalogs["sqlite"].update(open_time(sqlite_cat))

        # Time travel: a second commit, then the same table at two versions.
        # The session must not open the pink store, or a local table would
        # shadow the lake's lineitem.
        if "error" not in catalogs["pink"]:
            script = (
                f".lake attach {pink_cat} write\n"
                "INSERT INTO lineitem SELECT * FROM lineitem LIMIT 1000;\n"
                f".lake commit {pink_cat}\n.quit\n"
            )
            r = pink(script, timeout=3600)
            committed = "Committed lake version" in (r.stdout + r.stderr)
            catalogs["pink"]["second_commit_ok"] = committed
            for v, label in ((2, "v2"), (3, "v3")):
                r = pink(
                    f".lake attach {pink_cat} read-only {v}\n"
                    "SELECT count(*) FROM lineitem;\n.quit\n",
                    timeout=900,
                )
                rows = parse_box(r.stdout)
                catalogs["pink"][f"time_travel_rows_{label}"] = rows[-1][-1] if rows else None
            print(
                f"  time travel rows v2/v3: {catalogs['pink']['time_travel_rows_v2']}/"
                f"{catalogs['pink']['time_travel_rows_v3']}",
                flush=True,
            )

        # S3: a throwaway mock on loopback, never a real bucket.
        server = MockS3.start()
        try:
            url = f"s3://bench-bucket/lakes/pink"
            cat = "pink+s3://bench-bucket/catalogs/pink"
            env = server.env()
            r = pink(
                f".open {store}\n.lake save {cat} data {url}\n.quit\n",
                timeout=3600,
                env=env,
            )
            wall = 0.0
            if r.returncode == 0:
                info = {"catalog": cat, "objects": len(server.objects)}
                t0 = now()
                r2 = pink(f".lake attach {cat} read-only\n.quit\n", timeout=900, env=env)
                info["attach_wall_ms"] = (now() - t0) * 1000
                info["attach_ok"] = r2.returncode == 0
                if not info["attach_ok"]:
                    info["attach_error"] = (r2.stdout + r2.stderr)[-800:]
                else:
                    info.update(scan(cat, env=env))
                catalogs["s3"] = info
            else:
                catalogs["s3"] = {"error": (r.stdout + r.stderr)[-1200:]}
        finally:
            server.stop()
        out["catalogs"] = catalogs
        self.results["lake"] = out
        for name, info in catalogs.items():
            print(f"  lake {name}: {json.dumps(info)[:300]}", flush=True)

    # -- search --------------------------------------------------------

    def search(self):
        out = {}
        fts = self._fts()
        if fts:
            out["fts"] = fts
        try:
            out["hnsw"] = self._hnsw()
        except SystemExit as exc:
            out["hnsw"] = {"error": str(exc)}
        self.results["search"] = out

    def _fts(self):
        docs = 100_000
        store = self.work / "fts.pink"
        csv = self.work / "fts_docs.csv"
        if not csv.exists():
            words = (
                "relational database search engine bm25 index query document table row "
                "storage column vector graph latency commit snapshot catalog lake parquet "
                "optimizer binder planner execution memory transaction durability"
            ).split()
            with open(csv, "w") as f:
                f.write("id,title,body\n")
                for i in range(docs):
                    body = " ".join(words[(i * 7 + j * 13) % len(words)] for j in range(12))
                    f.write(f'{i},"document {i}","{body}"\n')
        store.unlink(missing_ok=True)
        r = pink(
            f".open {store}\n.import {csv} docs\n.timer on\nCALL create_fts_index('docs', 'id', 'title', 'body');\n",
            timeout=3600,
        )
        build = timer_values(r.stdout + r.stderr)
        if not build:
            return {"error": (r.stdout + r.stderr)[-1200:]}
        q = (
            "SELECT id FROM docs WHERE match_bm25('docs', id, 'relational database') IS NOT NULL "
            "ORDER BY match_bm25('docs', id, 'relational database') DESC LIMIT 10;"
        )
        r = pink(f".open {store}\n.timer on\n" + (q + "\n") * 6, timeout=900)
        tv = timer_values(r.stdout + r.stderr)
        store.unlink(missing_ok=True)
        result = {
            "docs": docs,
            "build_ms": build[-1] * 1000,
            "query_ms": median([t * 1000 for t in tv[1:]]) if len(tv) > 1 else None,
            "query_runs_ms": [t * 1000 for t in tv],
        }
        print(f"  fts: build={result['build_ms']:.0f}ms query={result['query_ms']}ms", flush=True)
        return result

    def _hnsw(self):
        store = self.work / "hnsw.pink"
        out = {}
        for tag, threads in (("single", 1), ("all", self.threads)):
            args = [
                "cargo",
                "run",
                "--release",
                "-p",
                "pinkdb-vss",
                "--example",
                "bench_vector",
                "--",
                "100000",
                "768",
                "32",
                "200",
                str(store),
            ]
            r = sh(args, timeout=3600, env={"RAYON_NUM_THREADS": str(threads)})
            text = r.stdout + r.stderr
            build = re.search(r"build=([0-9.]+)s", text)
            size = re.search(r"size=(\d+) B", text)
            recalls = {
                int(ef): float(rec)
                for ef, rec in re.findall(r"ef_search=\s*(\d+) recall@10=([0-9.]+)", text)
            }
            lat = {}
            for ef, p50, p95 in re.findall(
                r"ef_search=\s*(\d+) recall@10=[0-9.]+ p50=([0-9.]+)ms p95=([0-9.]+)ms", text
            ):
                lat[int(ef)] = {"p50": float(p50), "p95": float(p95)}
            if not build:
                out[tag] = {"error": text[-1200:]}
            else:
                out[tag] = {
                    "threads": threads,
                    "build_s": float(build.group(1)),
                    "index_bytes": int(size.group(1)) if size else None,
                    "recall_at_10": recalls,
                    "latency": lat,
                }
            print(f"  hnsw {tag}: {out[tag]}", flush=True)
        store.unlink(missing_ok=True)
        return out

    # -- footprint -----------------------------------------------------

    def footprint(self):
        out = {"binaries": {}, "startup_ms": None}
        specs = [
            ("pinkdb-full", ["cargo", "build", "--release", "-p", "pinkdb-cli", "--features", CLI_FEATURES]),
            ("pinkdb-default", ["cargo", "build", "--release", "-p", "pinkdb-cli"]),
            (
                "pinkdb-lean",
                ["cargo", "build", "--release", "-p", "pinkdb-cli", "--no-default-features"],
            ),
        ]
        for name, cmd in specs:
            r = sh(cmd, timeout=3600)
            if r.returncode != 0:
                out["binaries"][name] = {"error": (r.stdout + r.stderr)[-800:]}
                continue
            out["binaries"][name] = {"bytes": pinkdb_bin().stat().st_size}
            print(f"  {name}: {out['binaries'][name]}", flush=True)
        r = sh(["cargo", "build", "--release", "-p", "pinkdb-server"], timeout=3600)
        server = ROOT / "target" / "release" / "pinkdb-server"
        out["binaries"]["pinkdb-server"] = (
            {"bytes": server.stat().st_size} if r.returncode == 0 and server.exists() else {"error": "build failed"}
        )
        # The built-in extensions ship inside the CLI; measure each as the
        # delta over the default build rather than a separate binary.
        for feat in ("fts", "vss", "parquet", "pinklake"):
            r = sh(
                ["cargo", "build", "--release", "-p", "pinkdb-cli", "--features", feat],
                timeout=3600,
            )
            out["binaries"][f"pinkdb+{feat}"] = (
                {"bytes": pinkdb_bin().stat().st_size}
                if r.returncode == 0
                else {"error": (r.stdout + r.stderr)[-400:]}
            )
        # Loadable extensions are their own cdylibs.
        for pkg, libname in (
            ("pinkdb-agent", "pinkdb_agent"),
            ("pinkdb-mcp", "pinkdb_mcp"),
            ("pinkdb-memory", "pinkdb_memory"),
            ("pinkdb-ext-string", "string_ext"),
        ):
            r = sh(["cargo", "build", "--release", "-p", pkg], timeout=3600)
            lib = ROOT / "target" / "release" / (
                f"lib{libname}.dylib" if sys.platform == "darwin" else f"lib{libname}.so"
            )
            out["binaries"][f"lib{libname}"] = (
                {"bytes": lib.stat().st_size}
                if r.returncode == 0 and lib.exists()
                else {"error": "build failed"}
            )
        # A full-featured binary for the timing below.
        sh(["cargo", "build", "--release", "-p", "pinkdb-cli", "--features", CLI_FEATURES], timeout=3600)
        empty = self.work / "empty.pink"
        empty.unlink(missing_ok=True)
        runs = []
        for _ in range(11):
            t0 = now()
            pink(f".open {empty}\n.quit\n")
            runs.append((now() - t0) * 1000)
        out["startup_ms"] = median(runs)
        print(f"  startup: {out['startup_ms']:.1f} ms", flush=True)
        scale = self.args.scale
        store = tpch_dir(self.work, scale) / "store.pink"
        out["peak_rss_scale"] = scale
        rss = {}
        if store.exists():
            for nr, sql in RSS_QUERIES.items():
                r = sh(
                    ["/usr/bin/time", "-l", str(pinkdb_bin()), "-no-agent"],
                    input_text=f".open {store}\n{sql}\n",
                    timeout=1800,
                )
                m = re.search(r"(\d+)\s+maximum resident set size", r.stderr)
                rss[f"q{nr}"] = int(m.group(1)) if m else None
                print(f"  rss q{nr}: {rss[f'q{nr}']}", flush=True)
        out["peak_rss_bytes"] = rss
        self.results["footprint"] = out

    # -- ci ------------------------------------------------------------

    def ci(self):
        """The small, DuckDB-free guard: a deterministic TPC-H-shaped lineitem
        at SF 0.01, the scan and correlated-subquery shapes, a micro insert and
        the binary size. Baselines live in bench/suite/baseline-ci.json; a 2x
        regression fails."""
        rows = 60_000
        csv = self.work / "ci_lineitem.csv"
        gen_ci_lineitem(csv, rows)
        store = self.work / "ci.pink"
        store.unlink(missing_ok=True)
        r = pink(f".open {store}\n.import {csv} lineitem\n")
        if r.returncode != 0:
            raise SystemExit(f"ci import failed: {(r.stdout + r.stderr)[-1000:]}")
        out = {"rows": rows}
        queries = (("q1", CI_Q1), ("q6", CI_Q6)) + CI_CORRELATED
        for name, sql in queries:
            r = pink(f".open {store}\n.timer on\n" + (sql + "\n") * 6)
            tv = timer_values(r.stdout + r.stderr)
            out[name] = {"median_ms": median([t * 1000 for t in tv[1:]]), "runs_ms": [t * 1000 for t in tv]}
        t0 = now()
        pink(f".open {store}\n" + "".join(f"INSERT INTO lineitem VALUES ({i},1,1.0,0.0,0.0,'1994-01-01','O','F');\n" for i in range(200)))
        out["insert_200_ms"] = (now() - t0) * 1000
        out["binary_bytes"] = pinkdb_bin().stat().st_size
        store.unlink(missing_ok=True)
        baseline_path = ROOT / "bench" / "suite" / "baseline-ci.json"
        baseline = json.loads(baseline_path.read_text()) if baseline_path.exists() else None
        regressions = []
        if baseline and not self.args.update_baseline:
            for key, _ in queries:
                base = baseline.get(key, {}).get("median_ms")
                got = out[key]["median_ms"]
                if base and got and got > max(2 * base, base + 5):
                    regressions.append(f"{key}: {got:.1f}ms vs baseline {base:.1f}ms")
            if out["binary_bytes"] > baseline.get("binary_bytes", 0) * 1.1:
                regressions.append(
                    f"binary {out['binary_bytes']} B vs baseline {baseline.get('binary_bytes')} B"
                )
        out["regressions"] = regressions
        if self.args.update_baseline or not baseline:
            baseline_path.write_text(json.dumps(out, indent=2) + "\n")
            print(f"  ci baseline written to {baseline_path}", flush=True)
        self.results["ci"] = out
        for name, _ in queries:
            print(f"  ci {name}: {out[name]['median_ms']:.1f} ms", flush=True)
        if regressions:
            print("  CI REGRESSIONS: " + "; ".join(regressions), flush=True)


RSS_QUERIES = {
    1: "SELECT l_returnflag, l_linestatus, sum(l_quantity), sum(l_extendedprice), "
    "sum(l_extendedprice * (1 - l_discount)), "
    "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)), avg(l_quantity), "
    "avg(l_extendedprice), avg(l_discount), count(*) FROM lineitem "
    "WHERE l_shipdate <= CAST('1998-09-02' AS date) GROUP BY 1, 2 ORDER BY 1, 2;",
    9: "SELECT n_name, extract(year FROM o_orderdate), sum(l_extendedprice * (1 - l_discount)) "
    "FROM part, supplier, lineitem, partsupp, orders, nation "
    "WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey "
    "AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey "
    "AND p_name LIKE '%green%' GROUP BY 1, 2 ORDER BY 1, 2;",
    10: "SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)), c_acctbal, "
    "n_name, c_address, c_phone, c_comment FROM customer, orders, lineitem, nation "
    "WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey "
    "AND o_orderdate >= CAST('1993-10-01' AS date) "
    "AND o_orderdate < CAST('1994-01-01' AS date) "
    "AND l_returnflag = 'R' AND c_nationkey = n_nationkey "
    "GROUP BY 1, 2, 3, 4, 5, 6, 7, 8 ORDER BY 3 DESC LIMIT 20;",
}


def gen_ci_lineitem(path: Path, rows: int):
    """TPC-H-shaped lineitem for the CI guard. Deterministic, no DuckDB, and
    only the columns Q1/Q6 read plus the two the insert micro-benchmark needs."""
    flags = ("A", "N", "R")
    status = ("F", "O")
    with open(path, "w") as f:
        f.write("l_orderkey,l_quantity,l_extendedprice,l_discount,l_tax,l_shipdate,l_returnflag,l_linestatus\n")
        for i in range(rows):
            qty = 1 + (i * 7919) % 50
            price = 900.0 + (i * 104729) % 90000
            disc = ((i * 13) % 11) / 100.0
            tax = ((i * 17) % 9) / 100.0
            day = (i * 31 + 7) % 2557
            # 1992-01-01 + day; a real calendar so the date type accepts it.
            from datetime import date as _date, timedelta

            date = str(_date(1992, 1, 1) + timedelta(days=day))
            f.write(
                f"{i},{qty},{price:.2f},{disc:.2f},{tax:.2f},{date},"
                f"{flags[(i * 7) % 3]},{status[(i * 5) % 2]}\n"
            )


CI_Q1 = (
    "SELECT l_returnflag, l_linestatus, sum(l_quantity), sum(l_extendedprice), "
    "sum(l_extendedprice * (1 - l_discount)), avg(l_discount), count(*) "
    "FROM lineitem WHERE CAST(l_shipdate AS DATE) <= CAST('1998-09-02' AS date) "
    "GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;"
)
# Wave 120 shapes: a correlated scalar aggregate (the Q17 rewrite), a
# correlated EXISTS (semi join) and a correlated NOT IN (mark join with
# three-valued logic). l_returnflag repeats, so it is the correlation key.
CI_CORRELATED = (
    (
        "corr_scalar",
        "SELECT count(*) FROM lineitem WHERE l_quantity < (SELECT 0.2 * avg(l_quantity) "
        "FROM lineitem i2 WHERE i2.l_returnflag = lineitem.l_returnflag);",
    ),
    (
        "corr_exists",
        "SELECT count(*) FROM lineitem WHERE EXISTS (SELECT 1 FROM lineitem i2 "
        "WHERE i2.l_returnflag = lineitem.l_returnflag AND i2.l_quantity > 40);",
    ),
    (
        "corr_not_in",
        "SELECT count(*) FROM lineitem WHERE l_quantity NOT IN (SELECT l_quantity "
        "FROM lineitem i2 WHERE i2.l_returnflag = lineitem.l_returnflag "
        "AND i2.l_discount = 0.10);",
    ),
)
CI_Q6 = (
    "SELECT sum(l_extendedprice * l_discount) FROM lineitem "
    "WHERE CAST(l_shipdate AS DATE) >= CAST('1994-01-01' AS date) "
    "AND CAST(l_shipdate AS DATE) < CAST('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 "
    "AND l_quantity < 24;"
)


# --------------------------------------------------------------------------
# A minimal S3 server for the `pink+s3://` lake numbers. It ignores SigV4
# (loopback throwaway only) and speaks exactly the calls the client makes.


class MockS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.lock = threading.Lock()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            # Object clients drop keep-alive connections; that is normal and
            # must not print a traceback per request.
            def handle_one_request(self):
                try:
                    super().handle_one_request()
                except ConnectionResetError:
                    self.close_connection = True

            def log_message(self, fmt, *args):
                pass

            def _split(self):
                # Path-style requests carry the bucket first; object keys are
                # stored without it so one mock serves one bucket.
                path = self.path.split("?", 1)[0].lstrip("/")
                query = self.path.split("?", 1)[1] if "?" in self.path else ""
                bucket, _, key = path.partition("/")
                return bucket, key, query

            def do_GET(self):
                bucket, key, query = self._split()
                if "list-type=2" in query:
                    return self._list(bucket, query)
                with outer.lock:
                    data = outer.objects.get(key)
                if data is None:
                    return self._error(404, "NoSuchKey")
                rng = self.headers.get("Range")
                if rng:
                    m = re.match(r"bytes=(\d+)-(\d*)", rng)
                    start = int(m.group(1))
                    end = int(m.group(2)) if m.group(2) else len(data) - 1
                    part = data[start : end + 1]
                    self.send_response(206)
                    self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
                    self.send_header("Content-Length", str(len(part)))
                    self.send_header("ETag", '"' + hashlib.md5(data).hexdigest() + '"')
                    self.end_headers()
                    self.wfile.write(part)
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("ETag", '"' + hashlib.md5(data).hexdigest() + '"')
                self.end_headers()
                self.wfile.write(data)

            def do_HEAD(self):
                _, key, _ = self._split()
                with outer.lock:
                    data = outer.objects.get(key)
                if data is None:
                    return self._error(404, "NoSuchKey")
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("ETag", '"' + hashlib.md5(data).hexdigest() + '"')
                self.end_headers()

            def do_PUT(self):
                _, key, _ = self._split()
                n = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(n)
                with outer.lock:
                    if self.headers.get("If-None-Match") == "*" and key in outer.objects:
                        return self._error(412, "PreconditionFailed")
                    etag = self.headers.get("If-Match")
                    if etag and key in outer.objects:
                        have = '"' + hashlib.md5(outer.objects[key]).hexdigest() + '"'
                        if etag.strip() != have:
                            return self._error(412, "PreconditionFailed")
                    outer.objects[key] = body
                self.send_response(200)
                self.send_header("ETag", '"' + hashlib.md5(body).hexdigest() + '"')
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_DELETE(self):
                _, key, _ = self._split()
                with outer.lock:
                    outer.objects.pop(key, None)
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def _list(self, bucket, query):
                from urllib.parse import parse_qs

                q = parse_qs(query)
                prefix = q.get("prefix", [""])[0]
                delimiter = q.get("delimiter", [""])[0]
                max_keys = int(q.get("max-keys", ["1000"])[0])
                token = q.get("continuation-token", [""])[0]
                if token:
                    try:
                        token = base64.b64decode(token).decode()
                    except Exception:
                        token = ""
                with outer.lock:
                    keys = sorted(k for k in outer.objects if k.startswith(prefix) and k > token)
                contents = []
                prefixes = set()
                for k in keys:
                    if delimiter:
                        rest = k[len(prefix):]
                        if delimiter in rest:
                            prefixes.add(prefix + rest.split(delimiter, 1)[0] + delimiter)
                            continue
                    contents.append(k)
                page = contents[:max_keys]
                truncated = len(contents) > max_keys
                body = ['<?xml version="1.0" encoding="UTF-8"?>']
                body.append('<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">')
                body.append(f"<Name>{bucket}</Name><Prefix>{prefix}</Prefix>")
                body.append(f"<KeyCount>{len(page) + len(prefixes)}</KeyCount><MaxKeys>{max_keys}</MaxKeys>")
                body.append(f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>")
                if truncated and page:
                    nxt = base64.b64encode(page[-1].encode()).decode()
                    body.append(f"<NextContinuationToken>{nxt}</NextContinuationToken>")
                for k in page:
                    data = outer.objects[k]
                    body.append(
                        f"<Contents><Key>{k}</Key><Size>{len(data)}</Size>"
                        f"<ETag>\"{hashlib.md5(data).hexdigest()}\"</ETag>"
                        "<StorageClass>STANDARD</StorageClass></Contents>"
                    )
                for p in sorted(prefixes):
                    body.append(f"<CommonPrefixes><Prefix>{p}</Prefix></CommonPrefixes>")
                body.append("</ListBucketResult>")
                payload = "".join(body).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/xml")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def _error(self, code, name):
                payload = (
                    f'<?xml version="1.0"?><Error><Code>{name}</Code><Message>{name}</Message></Error>'
                ).encode()
                self.send_response(code)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @classmethod
    def start(cls):
        return cls()

    def port(self) -> int:
        return self.httpd.server_address[1]

    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port()}"

    def env(self) -> dict:
        return {
            "AWS_ACCESS_KEY_ID": "MOCKACCESSKEY",
            "AWS_SECRET_ACCESS_KEY": "mock-secret",
            "AWS_SESSION_TOKEN": "mock-session-token",
            "AWS_ENDPOINT_URL": self.endpoint(),
            "AWS_ENDPOINT_URL_S3": self.endpoint(),
            "AWS_REGION": "auto",
            "PINKDB_S3_ALLOW_HTTP": "1",
        }

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


# --------------------------------------------------------------------------
# Output


def write_results(suite: Suite, selected: list[str]):
    # The CI family is a pass/fail guard, not a benchmark: it only maintains
    # baseline-ci.json, and writing it to results/ would replace a full run.
    if selected == ["ci"]:
        return None
    results = {
        "meta": {
            **suite.machine,
            "started": suite.started,
            "finished": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "scale": suite.args.scale,
            "threads": suite.threads,
            "runs": suite.args.runs,
            "warmup": suite.args.warmup,
            "families": selected,
            "loads": suite.loads,
        },
        "families": suite.results,
    }
    date = time.strftime("%Y-%m-%d")
    short = suite.machine["commit_short"] or "nocommit"
    out = ROOT / "bench" / "suite" / "results" / f"{date}-{short}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2) + "\n")
    md = out.with_suffix(".md")
    md.write_text(markdown_summary(results))
    print(f"\nwrote {out}\nwrote {md}")
    return out


def markdown_summary(results: dict) -> str:
    meta = results["meta"]
    fam = results["families"]
    lines = [
        f"# pinkdb benchmark — {meta['started'][:10]} ({meta['commit_short']}, dirty={meta['dirty_files']})",
        "",
        f"{meta['cpu']} · {meta['physical_cores']} physical / {meta['logical_cores']} logical cores · "
        f"{meta['memory_gb']} GB RAM · {meta['os']} {meta['os_version']} · {meta['rustc']}",
        "",
        f"pinkdb threads: {meta['threads']} · runs: {meta['runs']} (+{meta['warmup']} warm-up) · "
        f"scale: {meta['scale']:g}",
        "",
    ]
    if "tpch" in fam:
        t = fam["tpch"]
        lines += ["## TPC-H", "", f"scale {t['scale']:g}; warm cache; median of {t['runs']} runs.", ""]
        lines += ["| query | pinkdb ms | DuckDB ms | ratio | rows | status |", "|---|---|---|---|---|---|"]
        for q in t["queries"]:
            p = f"{q['pinkdb_ms']:.2f}" if q.get("pinkdb_ms") else "—"
            d = f"{q['duckdb_ms']:.2f}" if q.get("duckdb_ms") else "—"
            r = f"{q['ratio']:.2f}×" if q.get("ratio") else "—"
            lines.append(f"| {q['query']} | {p} | {d} | {r} | {q.get('rows', '—')} | {q['status']} |")
        ok = [q for q in t["queries"] if q["status"] == "ok"]
        if ok:
            ratios = [q["ratio"] for q in ok if q.get("ratio")]
            geom = statistics.geometric_mean(ratios) if ratios else None
            if geom:
                lines += ["", f"Geometric mean pinkdb/DuckDB over {len(ok)} supported queries: **{geom:.2f}×**."]
        lines.append("")
    if "ingest" in fam:
        lines += ["## Ingest", "", "| table | format | engine | ms | M rows/s | MB/s |", "|---|---|---|---|---|---|"]
        for r in fam["ingest"]["tables"]:
            lines.append(
                f"| {r['table']} | {r['format']} | {r['engine']} | {r['median_ms']:.1f} | "
                f"{r['rows_per_s'] / 1e6:.2f} | {r['mb_per_s']:.1f} |"
            )
        lines.append("")
    if "durable" in fam:
        d = fam["durable"]
        lines += ["## Durable writes", ""]
        if d.get("bench"):
            lines += ["| policy | inserts ms | selects ms | checkpoints ms |", "|---|---|---|---|"]
            for tag, v in d["bench"].items():
                lines.append(f"| {tag} | {v.get('inserts_ms', '—')} | {v.get('selects_ms', '—')} | {v.get('checkpoints_ms', '—')} |")
            lines.append("")
        for tag in ("autocommit", "batched"):
            if tag in d:
                lines.append(f"- CLI {tag}: {json.dumps(d[tag])}")
        if d.get("multi_process"):
            lines.append(f"- two writers: {json.dumps(d['multi_process'])}")
        lines.append("")
    if "lake" in fam:
        lines += ["## Lake", "", "| catalog | save s | scan ms | attach ms |", "|---|---|---|---|"]
        for name, info in fam["lake"].get("catalogs", {}).items():
            if "error" in info:
                lines.append(f"| {name} | error | — | — |")
            else:
                lines.append(
                    f"| {name} | {info.get('seconds', '—')} | {info.get('median_ms', '—')} | {info.get('attach_ms', '—')} |"
                )
        lines.append("")
    if "search" in fam:
        s = fam["search"]
        lines += ["## Search", ""]
        if "fts" in s:
            lines.append(f"- FTS 100k docs: build {s['fts'].get('build_ms', 0):.0f} ms, top-10 {s['fts'].get('query_ms')} ms")
        for tag, h in s.get("hnsw", {}).items():
            if "error" in h:
                lines.append(f"- HNSW {tag}: error")
            else:
                lines.append(
                    f"- HNSW {tag} ({h['threads']} threads): build {h['build_s']:.1f} s, "
                    f"recall@10 {h['recall_at_10']}, p50/p95 {h['latency']}"
                )
        lines.append("")
    if "footprint" in fam:
        f = fam["footprint"]
        lines += ["## Footprint", ""]
        for name, info in f.get("binaries", {}).items():
            if "bytes" in info:
                lines.append(f"- {name}: {info['bytes']:,} B")
        if f.get("startup_ms") is not None:
            lines.append(f"- CLI start-up: {f['startup_ms']:.1f} ms (median of 11)")
        if f.get("peak_rss_bytes"):
            lines.append(f"- peak RSS: {json.dumps(f['peak_rss_bytes'])}")
        lines.append("")
    if "ci" in fam:
        c = fam["ci"]
        lines += ["## CI guard", "", f"- q1 {c.get('q1', {}).get('median_ms')} ms · q6 {c.get('q6', {}).get('median_ms')} ms"]
        if c.get("regressions"):
            lines.append("- **regressions**: " + "; ".join(c["regressions"]))
        lines.append("")
    loads = meta.get("loads") or []
    if loads:
        lines += ["## Load", "", "| stage | time | load | busy |", "|---|---|---|---|"]
        for item in loads[:40]:
            lines.append(
                f"| {item['stage']} | {item['at']} | {' '.join(f'{x:.2f}' for x in item['load'])} | {item['busy'] or ''} |"
            )
    return "\n".join(lines) + "\n"


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--only", default=",".join(FAMILIES), help="comma list: " + ", ".join(FAMILIES))
    p.add_argument("--scale", type=float, default=0.1, help="TPC-H scale (default 0.1; 1 for SF 1)")
    p.add_argument("--runs", type=int, default=5, help="timed runs per query (default 5)")
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--threads", type=int, default=min(8, os.cpu_count() or 1))
    p.add_argument("--work-dir", default=os.environ.get("PINKDB_BENCH_DIR", str(Path(tempfile.gettempdir()) / "pinkdb_bench_suite")))
    p.add_argument("--durable-rows", type=int, default=2000)
    p.add_argument("--skip-build", action="store_true")
    p.add_argument("--regen", action="store_true", help="regenerate TPC-H data")
    p.add_argument("--rebuild", action="store_true", help="rebuild the engine stores")
    p.add_argument("--update-baseline", action="store_true", help="rewrite bench/suite/baseline-ci.json")
    args = p.parse_args(argv)

    selected = [f.strip() for f in args.only.split(",") if f.strip()]
    unknown = [f for f in selected if f not in FAMILIES]
    if unknown:
        p.error(f"unknown family(s): {unknown}")

    if not args.skip_build and ("tpch" in selected or "ingest" in selected or "lake" in selected or "search" in selected or "footprint" in selected):
        print("building release pinkdb ...", flush=True)
        r = sh(["cargo", "build", "--release", "-p", "pinkdb-cli", "--features", CLI_FEATURES], timeout=3600)
        if r.returncode != 0:
            raise SystemExit((r.stdout + r.stderr)[-3000:])

    suite = Suite(args)
    print(
        f"pinkdb bench: {suite.machine['commit_short']} on {suite.machine['cpu']}, "
        f"{suite.machine['physical_cores']} cores, load {load_average()}, threads {suite.threads}",
        flush=True,
    )
    suite.note_load("start")
    for name in selected:
        print(f"\n[{name}]", flush=True)
        if name == "tpch" and suite.machine["dirty"]:
            print("  note: the tree is dirty; the commit names the base, the JSON records the diff count", flush=True)
        getattr(suite, name)()
        suite.note_load(f"after {name}")
    write_results(suite, selected)
    if "ci" in suite.results and suite.results["ci"].get("regressions"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
