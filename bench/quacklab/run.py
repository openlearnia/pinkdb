#!/usr/bin/env python3
"""Wave 130 — QuackLab's NYC-taxi workload on pinkdb and DuckDB.

Renders the compiled QuackLab plan (``plan/nyc_taxi_essential1.json``) into
setup / ETL / mart-refresh statements, runs one CLI process per phase (so a
phase's wall time is its process's wall time), compares row counts and chart
results between the engines, then measures the 12 widgets through the pinkdb
server and DuckDB's ``quack`` extension.

Standard library only.  External tools: the release pinkdb binaries (built by
the README's commands), the DuckDB CLI, PostgreSQL (``initdb``/``pg_ctl``/
``createdb``) and Docker (RustFS, digest-pinned below).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import secrets
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bench" / "suite"))
import run as suite  # noqa: E402  (house style: machine probe, sh(), timers)

PLAN_PATH = ROOT / "bench/quacklab/plan/nyc_taxi_essential1.json"
PLAN = json.loads(PLAN_PATH.read_text())
DEFAULT_GRAIN = Path.home() / "Desktop/projects/grain/grain"
DEFAULT_WORK = Path.home() / "Desktop/projects/pinkdb-wt/bench/quacklab-work"
RESULTS = ROOT / "bench/quacklab/results"

RUSTFS_IMAGE = (
    "rustfs/rustfs:latest@sha256:"
    "9ca9559533b537d10482fe923288b676ad08f92a7069d9a17327fbae3cab688b"
)
PG_PORT = 55433
SEED = 130
# Bounds that turn a stuck child into a recorded failure instead of a hang: a
# server that never listens, and a chart client that never returns.
SERVER_READY_TIMEOUT = 180
CHART_TIMEOUT = 300
DUCKLAKE_URI = "ducklake:postgres:dbname=duck host=127.0.0.1 port=55433 user=postgres password=quacklab"
PINK_LAKE_URI = (
    "ducklake:postgres:dbname=pink_r2_cat host=127.0.0.1 port=55433 "
    "user=postgres password=quacklab"
)

SIZES = {
    "smoke": [
        "batch-smoke-2024-01-08",
        "batch-trips-2024-08-12",
        "batch-trips-2024-08-12-correction",
    ],
    "small": [
        "batch-trips-2024-07",
        "batch-trips-2024-08-12",
        "batch-trips-2024-08-12-correction",
    ],
    "large": ["batch-trips-2024-q1", "batch-trips-2024-q2"],
}

TYPE_MAP = {
    "string": "VARCHAR",
    "timestamp": "TIMESTAMP",
    "number": "DOUBLE",
    "integer": "INTEGER",
    "boolean": "BOOLEAN",
    "date": "DATE",
}

# Runtime catalog fields whose map is keyed by table name (so a batch's
# manifest selects the entries).  The dimension-validation map is keyed by
# dimension ids and is therefore never filtered.
MANIFEST_FILTERED = {
    "stageLoadByTable",
    "stagingResetByTable",
    "stagingValidationByTable",
    "stageToIngestByTable",
    "ingestValidationByTable",
    "ingestDeleteByTable",
    "ingestResetByTable",
    "ingestToMasterByTable",
}

SPECS = PLAN["tableSpecs"]
SPEC = {t["id"]: t for t in SPECS}
SCHEMAS = sorted({t["schema"] for t in SPECS})
COUNT_IDS = [
    t["id"] for t in SPECS if t["schema"] == "master" or t["type"] in ("mart", "mart_dimension")
]


def q(name: str) -> str:
    """Quote an identifier."""
    return '"' + str(name).replace('"', '""') + '"'


def lit(text: str) -> str:
    """Quote a string literal."""
    return "'" + str(text).replace("'", "''") + "'"


def ty(dtype: str) -> str:
    return TYPE_MAP.get(dtype, "VARCHAR")


def _mv_order():
    """Materialized views in dependency order (stable: ids sorted)."""
    by_id = {t["id"]: t for t in SPECS if t["lifecycle"]["materialization"] == "materialized_view"}
    state, order = {}, []

    def visit(spec_id):
        if state.get(spec_id) == 2:
            return
        if state.get(spec_id) == 1:
            raise SystemExit(f"dependency cycle at {spec_id}")
        state[spec_id] = 1
        for dep in by_id[spec_id].get("dependsOn") or []:
            if dep in by_id:
                visit(dep)
        state[spec_id] = 2
        order.append(by_id[spec_id])

    for spec_id in sorted(by_id):
        visit(spec_id)
    return order


MV_ORDER = _mv_order()


def inject_field(marker: str) -> str:
    """``staging_reset_sqls`` → ``stagingResetByTable`` (QuackLab's catalog names)."""
    key = marker
    if key.endswith("_sqls"):
        key = key[: -len("_sqls")]
    parts = key.split("_")
    return parts[0] + "".join(p[:1].upper() + p[1:] for p in parts[1:]) + "ByTable"


def batch_files(data: Path, batch: str):
    """``TABLE -> file`` for a batch; ``manifest.csv`` for the initial/full load."""
    directory = data / batch
    manifest = directory / "manifest.csv"
    if not manifest.exists():
        manifest = directory / "manifest_incremental.csv"
    files = {}
    for line in manifest.read_text().splitlines():
        if line.strip():
            table, name = line.split("|", 1)
            files[table.strip()] = name.strip()
    return files


def free_port(preferred=None):
    if preferred:
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", preferred))
                return preferred
            except OSError:
                pass
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def port_open(host, port):
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def wait_tcp(host, port, timeout, proc=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"process exited with {proc.returncode} before listening on {port}")
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"nothing listening on {host}:{port} after {timeout}s")


def wait_http(url, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            out = subprocess.run(
                ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "2", url],
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.strip()
            if out in ("200", "403"):
                return
        except (OSError, subprocess.SubprocessError):
            pass
        time.sleep(0.2)
    raise RuntimeError(f"{url} did not answer after {timeout}s")


def rss_mb(text: str):
    m = re.search(r"(\d+)\s+maximum resident set size", text)
    return round(int(m.group(1)) / 1e6, 1) if m else None


def first_error(text: str) -> str:
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("Error: "):
            return s[len("Error: ") :]
        if re.match(r"^[A-Za-z]+ Error: ", s):
            return s
    for line in text.splitlines():
        if "Error" in line:
            return line.strip()
    return ""


def tail(text: str, n: int = 800) -> str:
    return (text or "").strip()[-n:]


def all_boxes(text: str):
    """Every box table's data rows (the name + type rows before the rule dropped)."""
    boxes, rows, header = [], None, False
    for line in text.splitlines():
        if line.startswith("┌"):
            rows, header = [], True
        elif rows is None:
            continue
        elif line.startswith("├") and header:
            header = False
        elif line.startswith("└"):
            boxes.append(rows)
            rows = None
        elif not header and line.startswith("│"):
            rows.append(_cells(line))
    return boxes


def _cells(line):
    s = line.strip()
    if s.startswith("│"):
        s = s[1:]
    if s.endswith("│"):
        s = s[:-1]
    return [c.strip() for c in s.split("│")]


def canon_rows(rows):
    out = []
    for row in rows:
        cells = []
        for cell in row:
            s = (cell or "").strip()
            try:
                f = float(s)
                if math.isfinite(f):
                    s = f"{f:.9g}"
            except ValueError:
                pass
            cells.append(s)
        out.append(tuple(cells))
    return out


def cells_agree(a, b) -> bool:
    """Compare at six significant digits (1e-6 relative), not by string."""
    try:
        fa, fb = float(a), float(b)
    except (TypeError, ValueError):
        return (a or "").strip() == (b or "").strip()
    if math.isnan(fa) or math.isnan(fb):
        return math.isnan(fa) and math.isnan(fb)
    return math.isclose(fa, fb, rel_tol=1e-6, abs_tol=1e-12)


def rows_close(a, b) -> bool:
    return len(a) == len(b) and all(cells_agree(x, y) for x, y in zip(a, b))


def same_rows(a, b) -> bool:
    ca, cb = canon_rows(a), canon_rows(b)
    return len(ca) == len(cb) and all(rows_close(x, y) for x, y in zip(sorted(ca), sorted(cb)))


def rows_agree(sql: str, a, b) -> bool:
    # ORDER BY … LIMIT keeps the returned order, except that equal keys may tie
    # in any order: the same multiset is the same top-N.
    ordered = "ORDER BY" in sql.upper() and "LIMIT" in sql.upper()
    if ordered and len(a) == len(b):
        return all(rows_close(x, y) for x, y in zip(a, b))
    return same_rows(a, b)


def tie_order(sql: str, a, b) -> bool:
    """True when the only difference is the order of equal keys at a LIMIT."""
    ordered = "ORDER BY" in sql.upper() and "LIMIT" in sql.upper()
    return ordered and not rows_agree(sql, a, b) and same_rows(a, b)


def percentiles(values):
    if not values:
        return None, None, None
    sv = sorted(values)
    n = len(sv)

    def pct(p):
        k = (n - 1) * p
        lo = math.floor(k)
        hi = min(lo + 1, n - 1)
        return sv[lo] + (sv[hi] - sv[lo]) * (k - lo)

    return round(pct(0.50), 3), round(pct(0.95), 3), round(pct(0.99), 3)


# ---------------------------------------------------------------------------
# Statement rendering
# ---------------------------------------------------------------------------


def create_table(spec, prefixed: bool) -> str:
    cols = []
    for col in spec["columns"]:
        ddl = f"{q(col['name'])} {ty(col['dataType'])}"
        if not col.get("nullable", True):
            ddl += " NOT NULL"
        cols.append(ddl)
    body = ",\n  ".join(cols)
    return f"CREATE TABLE IF NOT EXISTS {table_ref(spec, prefixed)} (\n  {body}\n);"


def table_ref(spec, prefixed: bool) -> str:
    return ("r2_cat." if prefixed else "") + f"{q(spec['schema'])}.{q(spec['table'])}"


def setup_units(engine, storage, mode, lake):
    units = []
    prefixed = engine == "duckdb" or mode == "emitted"
    if engine == "pinkdb" and mode == "emitted" and storage == "lake":
        units.append(("secret", lake.duck_secret()))
        units.append(
            ("attach", f"ATTACH '{PINK_LAKE_URI}' AS r2_cat (DATA_PATH 's3://lake/pink/');")
        )
        units.append(("use", "USE r2_cat;"))
    if engine == "pinkdb" and mode == "workaround":
        for schema in SCHEMAS:
            sql = lake.pink_lake_create(schema) if storage == "lake" else f"CREATE DATABASE {q(schema)};"
            units.append((f"database:{schema}", sql))
    else:
        for schema in SCHEMAS:
            # pinkdb's emitted lake path already ran USE r2_cat.
            qualifier = "" if engine == "pinkdb" and mode == "emitted" else "r2_cat."
            units.append(
                (f"schema:{schema}", f"CREATE SCHEMA IF NOT EXISTS {qualifier}{q(schema)};")
            )
    for spec in SPECS:
        units.append((f"table:{spec['id']}", create_table(spec, prefixed)))
    return units


def walk_plan():
    """Execution-plan steps in emitted order; returns (units, skipped).

    Each unit is ``(id, catalog_field_or_None, sql, table_or_None)``.  Group
    children are followed in ``steps`` order; a ``$inject`` marker expands to
    the catalog map's entries (sorted, for a deterministic order).
    """
    units, skipped = [], []

    def walk(step):
        sid = step.get("id", "?")
        sqls = step.get("sqls")
        if isinstance(sqls, list):
            for i, item in enumerate(sqls):
                uid = sid if len(sqls) == 1 else f"{sid}#{i}"
                if isinstance(item, str):
                    units.append((uid, None, item, None))
                elif isinstance(item, dict) and "$inject" in item:
                    field = inject_field(item["$inject"])
                    mapping = PLAN["runtimeSqlCatalog"].get(field) or {}
                    for table in sorted(mapping):
                        units.append((f"{sid}:{table}", field, mapping[table], table))
        elif step.get("sql") is not None:
            units.append((sid, None, step["sql"], None))
        elif step.get("macro"):
            skipped.append(f"{sid} ({step['macro']}, runs in the refresh phase)")
        elif step.get("type") == "group":
            for child in step.get("steps") or []:
                walk(child)
        else:
            skipped.append(f"{sid} (no SQL)")

    for step in PLAN["executionPlan"]["steps"]:
        walk(step)
    return units, skipped


PLAN_ETL_UNITS, PLAN_SKIPPED = walk_plan()


def expand_merge_insert(sql: str, table: str) -> str:
    """pinkdb has no ``INSERT BY NAME``; spell the merge insert out."""
    if "INSERT BY NAME" not in sql.upper():
        return sql
    spec = SPEC[f"ingest.{table}"]
    cols = [c["name"] for c in spec["columns"]]
    m = re.search(r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)\s+ON\b", sql)
    alias = m.group(1) if m else "source"
    columns = ", ".join(q(c) for c in cols)
    values = ", ".join(f"{alias}.{q(c)}" for c in cols)
    replacement = f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})"
    return re.sub(
        r"WHEN NOT MATCHED THEN INSERT BY NAME", replacement, sql, count=1, flags=re.IGNORECASE
    )


def substitute(sql: str, batch: str, batch_type: str, table, files, raw_prefix: str, data: Path):
    sql = sql.replace("$batchId", batch).replace("$batchType", batch_type)
    if "$rawBucket/$sourcePath" in sql:
        assert table is not None, "source marker in a statement without a table"
        url = (
            f"{raw_prefix}/{batch}/{files[table]}"
            if raw_prefix.startswith("s3://")
            else str(data / batch / files[table])
        )
        sql = sql.replace("s3://$rawBucket/$sourcePath", url)
    return sql


def strip_types(sql: str) -> str:
    """Drop DuckDB's `types={...}` (a struct literal pinkdb's parser lacks)."""
    return re.sub(r",\s*types\s*=\s*\{[^}]*\}", "", sql)


def stage_projection(sql: str, table: str):
    """Column names of the emitted load's ``SELECT`` list, in output order."""
    m = re.search(r"(?is)\bSELECT\s+(.*?)\s+FROM\s+read_csv\b", sql)
    if m:
        cols = [part.strip().strip('"') for part in m.group(1).split(",")]
        if all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", c) for c in cols):
            return cols
    return [c["name"] for c in SPEC[f"staging.{table}"]["columns"]]


def cast_projection(sql: str, table: str) -> str:
    """Apply the staging spec's declared types as casts over the CSV scan.

    pinkdb's read_csv has no supported way to name column types, and its
    inference turns the numeric-looking string columns (ZONE_ID, …) into
    INTEGER and `DIM_DATE.DAY_ID` into TIMESTAMP.  Casting the projection
    keeps the declared schema without rewriting the data.
    """
    spec = SPEC[f"staging.{table}"]
    types = {col["name"]: TYPE_MAP[col["dataType"]] for col in spec["columns"]}
    cols = stage_projection(sql, table)
    projection = ", ".join(f"CAST({q(name)} AS {types[name]})" for name in cols)
    text, count = re.subn(
        r"(?is)\bSELECT\s+.*?\s+FROM\s+read_csv\b",
        f"SELECT {projection} FROM read_csv",
        sql,
        count=1,
    )
    if not count:
        raise SystemExit(f"cannot rewrite the staging load for {table}: {sql[:160]}")
    return text


def set_insert_columns(sql: str, cols) -> str:
    """Replace ``INSERT INTO t BY NAME`` with an explicit column list."""
    columns = ", ".join(q(c) for c in cols)
    return re.sub(
        r"(?i)(INSERT\s+INTO\s+\S+)\s+BY\s+NAME\s+",
        lambda m: f"{m.group(1)} ({columns}) ",
        sql,
        count=1,
    )


def etl_units(engine, storage, mode, batch, files, raw_prefix, data, gaps):
    """Statements for one batch plus the names skipped because of the manifest."""
    units, skipped = [], []
    for uid, field, sql, table in PLAN_ETL_UNITS:
        if field in MANIFEST_FILTERED and table not in files:
            continue
        text = substitute(
            sql, batch, "full" if len(files) > 1 else "incremental", table, files, raw_prefix, data
        )
        if engine == "pinkdb":
            if mode == "workaround":
                text = text.replace("r2_cat.", "")
            if field == "stageLoadByTable":
                if gaps.get("csv_types"):
                    text = cast_projection(strip_types(text), table)
                if gaps.get("stage_insert_by_name"):
                    # The insert must name the projection, never assume its order
                    # happens to equal the table's.
                    text = set_insert_columns(text, stage_projection(sql, table))
            elif field == "stageToIngestByTable" and gaps.get("merge_insert_by_name"):
                text = expand_merge_insert(text, table)
        units.append((uid, text))
    return units


def refresh_units(engine, mode, gaps, mv_columns):
    units = []
    for spec in MV_ORDER:
        prefixed = engine == "duckdb" or mode == "emitted"
        ref = table_ref(spec, prefixed)
        select = (spec.get("selectSql") or "").rstrip().rstrip(";")
        if engine == "pinkdb" and mode == "workaround":
            select = select.replace("r2_cat.", "")
        columns = mv_columns.get(spec["id"]) or [c["name"] for c in spec["columns"]]
        if engine == "pinkdb" and gaps.get("refresh_insert_by_name"):
            insert = f"INSERT INTO {ref} ({', '.join(q(c) for c in columns)}) {select};"
        else:
            insert = f"INSERT INTO {ref} BY NAME {select};"
        units.append((f"refresh:{spec['id']}", f"DELETE FROM {ref};"))
        units.append((f"refresh:{spec['id']}", insert))
    return units


# ---------------------------------------------------------------------------
# Lake services (Postgres catalog + RustFS raw/lake buckets)
# ---------------------------------------------------------------------------


class Lake:
    def __init__(self, work: Path, data: Path):
        self.work = work
        self.data = data
        self.pgdir = work / "pg"
        self.s3_port = None
        self.container = None
        self.access = "quacklab"
        self.secret = secrets.token_hex(16)

    # -- lifecycle ---------------------------------------------------------
    def start(self):
        self.work.mkdir(parents=True, exist_ok=True)
        self._start_pg()
        self._start_s3()

    def _start_pg(self):
        env = os.environ.copy()
        env["LC_ALL"] = "C"
        r = subprocess.run(
            [
                "initdb",
                "-D",
                str(self.pgdir),
                "-U",
                "postgres",
                "--auth=trust",
                "--locale=C",
                "--encoding=UTF8",
                "--no-sync",
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=600,
        )
        if r.returncode:
            raise RuntimeError(f"initdb failed: {tail(r.stderr)}")
        log = self.work / "pg.log"
        if port_open("127.0.0.1", PG_PORT):
            raise RuntimeError(
                f"port {PG_PORT} is already in use: a previous --keep run may still "
                "have Postgres running; stop it before measuring again"
            )
        r = subprocess.run(
            [
                "pg_ctl",
                "-D",
                str(self.pgdir),
                "-l",
                str(log),
                "-o",
                f"-p {PG_PORT} -h 127.0.0.1 -k {self.pgdir}",
                "-w",
                "start",
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=120,
        )
        if r.returncode:
            raise RuntimeError(f"pg_ctl failed: {tail(r.stderr)}\n{tail(log.read_text())}")
        self._createdb("duck")
        self._createdb("pink_r2_cat")
        for schema in SCHEMAS:
            self._createdb(f"pink_{schema.lower()}")

    def _createdb(self, name):
        r = subprocess.run(
            ["createdb", "-h", "127.0.0.1", "-p", str(PG_PORT), "-U", "postgres", name],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if r.returncode:
            raise RuntimeError(f"createdb {name} failed: {tail(r.stderr)}")

    def _start_s3(self):
        self.s3_port = free_port(9100)
        self.container = f"quacklab-rustfs-{os.getpid()}"
        r = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                self.container,
                "--label",
                "quacklab=1",
                "-p",
                f"127.0.0.1:{self.s3_port}:9000",
                "-e",
                f"RUSTFS_ACCESS_KEY={self.access}",
                "-e",
                f"RUSTFS_SECRET_KEY={self.secret}",
                RUSTFS_IMAGE,
            ],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if r.returncode:
            raise RuntimeError(f"docker run failed: {tail(r.stderr)}")
        wait_http(f"http://127.0.0.1:{self.s3_port}/", 120)
        for bucket in ("raw", "lake"):
            code = self.curl("PUT", f"/{bucket}")
            if code not in (200, 204):
                raise RuntimeError(f"bucket {bucket}: HTTP {code}")

    def stop(self):
        if self.container:
            subprocess.run(
                ["docker", "rm", "-f", self.container], capture_output=True, text=True, timeout=120
            )
            self.container = None
        if self.pgdir.exists():
            subprocess.run(
                ["pg_ctl", "-D", str(self.pgdir), "-m", "fast", "stop"],
                capture_output=True,
                text=True,
                timeout=120,
            )

    # -- helpers -----------------------------------------------------------
    def endpoint(self):
        return f"http://127.0.0.1:{self.s3_port}"

    def curl(self, method, path, upload=None):
        cmd = [
            "curl",
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "--aws-sigv4",
            "aws:amz:us-east-1:s3",
            "--user",
            f"{self.access}:{self.secret}",
            "-X",
            method,
            "--max-time",
            "300",
        ]
        if upload is not None:
            cmd += ["--upload-file", str(upload)]
        cmd.append(self.endpoint() + path)
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=360)
        try:
            return int(r.stdout.strip() or 0)
        except ValueError:
            return 0

    def upload(self, batch: str, files):
        for table, name in files.items():
            code = self.curl("PUT", f"/raw/{batch}/{name}", upload=self.data / batch / name)
            if code not in (200, 204):
                raise RuntimeError(f"upload {batch}/{name}: HTTP {code}")

    def pink_env(self):
        return {
            "AWS_ACCESS_KEY_ID": self.access,
            "AWS_SECRET_ACCESS_KEY": self.secret,
            "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT_URL_S3": self.endpoint(),
            "AWS_S3_FORCE_PATH_STYLE": "true",
            "PINKDB_S3_ALLOW_HTTP": "1",
        }

    def duck_secret(self):
        return (
            "CREATE SECRET (TYPE s3, "
            f"KEY_ID '{self.access}', SECRET '{self.secret}', REGION 'us-east-1', "
            f"ENDPOINT '127.0.0.1:{self.s3_port}', URL_STYLE 'path', USE_SSL false);"
        )

    def ducklake_attach(self):
        return f"ATTACH '{DUCKLAKE_URI}' AS r2_cat (DATA_PATH 's3://lake/duck/');"

    def pink_lake_create(self, schema):
        db = f"pink_{schema.lower()}"
        return (
            f"CREATE DATABASE {q(schema)} (TYPE LAKE, "
            f"CATALOG 'postgres://postgres@127.0.0.1:{PG_PORT}/{db}', "
            f"DATA 's3://lake/pink/{schema.lower()}/');"
        )


# ---------------------------------------------------------------------------
# Processes and phases
# ---------------------------------------------------------------------------


def run_plain(cmd, script, env, timeout=1800):
    try:
        p = subprocess.run(
            cmd, input=script, text=True, capture_output=True, timeout=timeout, env=env
        )
        return {"rc": p.returncode, "stdout": p.stdout, "stderr": p.stderr, "timeout": False}
    except subprocess.TimeoutExpired as e:
        out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        err = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
        return {"rc": 124, "stdout": out, "stderr": err, "timeout": True}


def timed(cmd, script, env, timeout=3600):
    start = time.monotonic()
    r = run_plain(["/usr/bin/time", "-l", *cmd], script, env, timeout)
    wall = time.monotonic() - start
    combined = r["stdout"] + "\n" + r["stderr"]
    return {
        "wall_s": round(wall, 3),
        "returncode": r["rc"],
        "ok": r["rc"] == 0,
        "stdout": r["stdout"],
        "stderr": r["stderr"],
        "max_rss_mb": rss_mb(r["stderr"]),
        "timers": suite.timer_values(r["stdout"]),
        "error": first_error(combined) if r["rc"] else "",
    }


def terminate(sql: str) -> str:
    """A unit is one statement; plan strings are not always semicolon-terminated."""
    return sql.rstrip().rstrip(";") + ";"


def phase_script(ctx, engine, storage, store, units, use_lake=True):
    env = os.environ.copy()
    env["PINKDB_AGENT_MODE"] = "0"
    lines = []
    if engine == "pinkdb":
        lines.append(f".open {store}")
        if (
            use_lake
            and storage == "lake"
            and ctx["results"]["storages"][storage]["engines"][engine]["mode"] == "emitted"
        ):
            lines.append("USE r2_cat;")
        lines.append(".timer on")
        lines.extend(terminate(sql) for _, sql in units)
        lines.append(f".save {store}")
        lines.append(".quit")
        cmd = [ctx["bin"]["pink"], "-no-agent"]
        if storage == "lake":
            env.update(ctx["lake"].pink_env())
    else:
        lines.extend(duck_prefix(ctx, storage))
        lines.append(".timer on")
        lines.extend(terminate(sql) for _, sql in units)
        lines.append(".quit")
        cmd = [ctx["bin"]["duckdb"], "-no-agent", ":memory:"]
    return cmd, "\n".join(lines) + "\n", env


def duck_prefix(ctx, storage):
    scratch = ctx["work"] / "duck-prefix.out"
    lines = [f".output {scratch}"]
    if storage == "lake":
        lines.append(ctx["lake"].duck_secret())
        lines.append("LOAD httpfs;")
        lines.append("LOAD ducklake;")
        lines.append(ctx["lake"].ducklake_attach())
    else:
        lines.append(f"ATTACH '{ctx['store'][storage]['duckdb']}' AS r2_cat;")
    lines.append("USE r2_cat;")
    lines.append(".output")
    return lines


def validate_rows(boxes):
    """Rows whose any cell is ``fail`` (validation SELECTs print these)."""
    bad = []
    for box in boxes:
        for row in box:
            if any(cell.strip().lower() == "fail" for cell in row):
                bad.append(row)
    return bad[:10]


def run_phase(ctx, engine, storage, store, units, label):
    cmd, script, env = phase_script(ctx, engine, storage, store, units, label != "setup")
    log_dir = ctx["work"] / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{engine}-{storage}-{label}.log"
    res = timed(cmd, script, env, timeout=ctx["args"].phase_timeout)
    log_path.write_text(res["stdout"] + "\n--- stderr ---\n" + res["stderr"])
    timers = res.pop("timers")
    res["n_units"] = len(units)
    res["n_timers"] = len(timers)
    res["validation_fails"] = validate_rows(all_boxes(res["stdout"]))
    res["status"] = "ok" if res["ok"] else "failed"
    if not res["ok"] and not res["error"]:
        res["error"] = tail(res["stdout"] + res["stderr"])
    # The log keeps the per-statement timers and both streams; repeating them in
    # every phase record is what pushed the committed JSON past 100 KB.
    res.pop("stdout", None)
    res.pop("stderr", None)
    return res


def probe_pink(ctx, storage, probe_store):
    """Decision 6: emit QuackLab's own statements first; record the first blocker."""
    probe_store.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", ".wal"):
        path = Path(str(probe_store) + suffix)
        if path.exists():
            path.unlink()
    env = os.environ.copy()
    if storage == "lake":
        env.update(ctx["lake"].pink_env())
    blocked = None
    if storage == "lake":
        r = run_plain(
            [ctx["bin"]["pink"], "-no-agent"],
            f".open {probe_store}\n"
            f"{ctx['lake'].duck_secret()}\n"
            f"ATTACH '{PINK_LAKE_URI}' AS r2_cat (DATA_PATH 's3://lake/pink/');\n"
            "USE r2_cat;\n",
            env,
            timeout=300,
        )
        if r["rc"]:
            blocked = {"status": "blocked", "error": first_error(r["stdout"] + r["stderr"])}
    if blocked is None:
        r = run_plain(
            [ctx["bin"]["pink"], "-no-agent"],
            f".open {probe_store}\nUSE r2_cat;\n"
            'CREATE SCHEMA IF NOT EXISTS r2_cat."staging";\n',
            env,
            timeout=300,
        )
        if r["rc"]:
            blocked = {"status": "blocked", "error": first_error(r["stdout"] + r["stderr"])}
    for suffix in ("", ".wal"):
        path = Path(str(probe_store) + suffix)
        if path.exists():
            path.unlink()
    return blocked


def probe_gaps(ctx):
    """Probe each shimmed emitted form once on a throwaway pinkdb store.

    A shim is applied only when its emitted form fails, so a later engine wave
    that fixes a gap turns the shim off without editing this file.  The store
    is a local workaround store (databases + empty tables), never the measured
    store or a lake; each probe runs on its own copy so a successful probe that
    writes rows cannot affect the next one.
    """
    probe = ctx["work"] / "probe" / "gaps.pink"
    probe.parent.mkdir(parents=True, exist_ok=True)
    setup = run_phase(
        ctx,
        "pinkdb",
        "local",
        probe,
        setup_units("pinkdb", "local", "workaround", None),
        "gaps-setup",
    )
    if setup["status"] != "ok":
        raise RuntimeError(f"pinkdb gap probe setup failed: {setup['error']}")

    batch = SIZES[ctx["args"].size][0]
    files = batch_files(ctx["data"], batch)
    stage = next(u for u in PLAN_ETL_UNITS if u[1] == "stageLoadByTable")
    stage_sql = substitute(
        stage[2], batch, "full", stage[3], files, str(ctx["data"] / batch), ctx["data"]
    ).replace("r2_cat.", "")
    start = stage_sql.find("read_csv(")
    depth, end = 0, None
    for i in range(start, len(stage_sql)):
        if stage_sql[i] == "(":
            depth += 1
        elif stage_sql[i] == ")":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if start < 0 or end is None:
        raise RuntimeError("cannot find the emitted read_csv call")
    csv_call = stage_sql[start:end]

    merge = next(u for u in PLAN_ETL_UNITS if u[1] == "stageToIngestByTable")
    merge_sql = substitute(
        merge[2], batch, "full", merge[3], files, str(ctx["data"] / batch), ctx["data"]
    ).replace("r2_cat.", "")

    dim_cols = ", ".join(q(c["name"]) for c in SPEC["staging.DIM_DATE"]["columns"])
    probes = [
        ("csv_types", f"SELECT * FROM {csv_call};"),
        (
            "stage_insert_by_name",
            'INSERT INTO "staging"."DIM_DATE" BY NAME SELECT * FROM "staging"."DIM_DATE";',
        ),
        ("merge_insert_by_name", merge_sql),
        (
            "refresh_insert_by_name",
            'INSERT INTO "staging"."DIM_DATE" BY NAME SELECT '
            f'{dim_cols} FROM "staging"."DIM_DATE";',
        ),
    ]
    env = os.environ.copy()
    env["PINKDB_AGENT_MODE"] = "0"
    out = {}
    for i, (name, emitted) in enumerate(probes):
        copy = probe.with_name(f"probe-{i}.pink")
        shutil.copyfile(probe, copy)
        r = run_plain(
            [ctx["bin"]["pink"], "-no-agent"],
            f".open {copy}\n{terminate(emitted)}\n",
            env,
            timeout=300,
        )
        out[name] = {
            "shim": name,
            "emitted": emitted[:500] + ("…" if len(emitted) > 500 else ""),
            "error": first_error(r["stdout"] + r["stderr"]) if r["rc"] else "",
            "applied": r["rc"] != 0,
        }
        for suffix in ("", ".wal"):
            path = Path(str(copy) + suffix)
            if path.exists():
                path.unlink()
    return out


def describe_mv_columns(ctx, storage):
    """DuckDB's output column names per materialized view, in select order.

    pinkdb's workaround insert spells the target columns out explicitly, so it
    must know the select's projection order rather than assume the spec order.
    """
    units = []
    for spec in MV_ORDER:
        select = (spec.get("selectSql") or "").rstrip().rstrip(";")
        units.append((f"describe:{spec['id']}", f"SELECT {lit('mv:' + spec['id'])} AS \"mv\";"))
        units.append(
            (f"describe:{spec['id']}", f"SELECT column_name FROM (DESCRIBE {select});")
        )
    cmd, script, env = phase_script(
        ctx, "duckdb", storage, ctx["store"][storage]["duckdb"], units
    )
    r = run_plain(cmd, script, env, timeout=900)
    out, current = {}, None
    for box in all_boxes(r["stdout"]):
        if len(box) == 1 and len(box[0]) == 1 and box[0][0].startswith("mv:"):
            current = box[0][0][3:]
        elif current is not None:
            names = [row[0] for row in box if row and row[0]]
            if names:
                out[current] = names
    return out


# ---------------------------------------------------------------------------
# Agreement runs
# ---------------------------------------------------------------------------


def count_rows(ctx, engine, storage, mode):
    prefixed = engine == "duckdb" or mode == "emitted"
    units = [
        (
            f"count:{spec_id}",
            f"SELECT '{spec_id}' AS \"table_id\", count(*) AS \"n\" FROM "
            f"{'r2_cat.' if prefixed else ''}{q(SPEC[spec_id]['schema'])}.{q(SPEC[spec_id]['table'])};",
        )
        for spec_id in COUNT_IDS
    ]
    cmd, script, env = phase_script(ctx, engine, storage, ctx["store"][storage][engine], units)
    # Counts are not a timed phase: drop the timers and the final save.
    lines = script.splitlines()
    lines = [line for line in lines if line != ".timer on" and not line.startswith(".save ")]
    r = run_plain(cmd, "\n".join(lines) + "\n", env, timeout=1800)
    got = {}
    for box in all_boxes(r["stdout"]):
        for row in box:
            if len(row) >= 2 and row[0] in COUNT_IDS:
                try:
                    got[row[0]] = int(row[1].replace(",", ""))
                except ValueError:
                    pass
    return {
        "status": "ok" if r["rc"] == 0 and len(got) == len(COUNT_IDS) else "failed",
        "counts": got,
        "error": first_error(r["stdout"] + r["stderr"]) if r["rc"] else "",
    }


def agreement_prefix(ctx, engine, storage, mode):
    """Session prefix that opens the store/lake the batch phases wrote.

    Chart SQL already names tables as `"schema"."table"`, which is the
    database.table spelling pinkdb's workaround databases use and a two-part
    name resolved inside `r2_cat` on DuckDB.
    """
    if engine == "pinkdb":
        return [f".open {ctx['store'][storage]['pinkdb']}"]
    return duck_prefix(ctx, storage)


def chart_rows(ctx, engine, storage, mode):
    """Run the widget statements in one fresh process; rows by widget.

    A `SELECT '<name>'` marker precedes each widget so a statement pinkdb
    cannot parse does not shift the result boxes of the widgets after it.
    """
    prefix = agreement_prefix(ctx, engine, storage, mode)
    names = sorted(PLAN["chartSql"])
    body = []
    for name in names:
        body.append(f"SELECT {lit(name)} AS \"widget\";")
        body.append(terminate(PLAN["chartSql"][name]))
    script = "\n".join(prefix + body + [".quit"]) + "\n"
    env = os.environ.copy()
    env["PINKDB_AGENT_MODE"] = "0"
    if engine == "pinkdb":
        if storage == "lake":
            env.update(ctx["lake"].pink_env())
        cmd = [ctx["bin"]["pink"], "-no-agent"]
    else:
        cmd = [ctx["bin"]["duckdb"], "-no-agent", ":memory:"]
    r = run_plain(cmd, script, env, timeout=1800)
    boxes = all_boxes(r["stdout"])
    out, current = {}, None
    for box in boxes:
        if len(box) == 1 and len(box[0]) == 1 and box[0][0] in PLAN["chartSql"]:
            current = box[0][0]
        elif current is not None:
            out[current] = box
    missing = [name for name in names if name not in out]
    errors = {}
    for name in missing:
        single = "\n".join(prefix + [terminate(PLAN["chartSql"][name]), ".quit"]) + "\n"
        r1 = run_plain(cmd, single, env, timeout=600)
        errors[name] = first_error(r1["stdout"] + r1["stderr"]) or "no result box"
    return {
        "status": "ok" if not errors else "partial",
        "rows": out,
        "errors": errors,
        "error": first_error(r["stdout"] + r["stderr"]) if r["rc"] else "",
    }


# ---------------------------------------------------------------------------
# Servers and chart load
# ---------------------------------------------------------------------------


def start_pink_server(ctx, storage):
    workdir = ctx["work"] / "servers" / f"pinkdb-{storage}"
    workdir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ctx["store"][storage]["pinkdb"], workdir / "pinkdb.pink")
    port = free_port()
    log = open(workdir / "server.log", "w")
    env = os.environ.copy()
    env["PINKDB_AGENT_MODE"] = "0"
    if storage == "lake":
        env.update(ctx["lake"].pink_env())
    proc = subprocess.Popen(
        [
            ctx["bin"]["server"],
            "--data-dir",
            str(workdir),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
        env=env,
    )
    try:
        wait_tcp("127.0.0.1", port, SERVER_READY_TIMEOUT, proc)
    except RuntimeError:
        stop_server({"proc": proc, "log": log})
        raise
    return {"proc": proc, "port": port, "log": log, "workdir": workdir}


def stop_server(server):
    if not server:
        return
    proc = server["proc"]
    if proc.stdin is not None:
        # The DuckDB CLI does not break out of a running `quack_serve` on SIGINT;
        # EOF on the stdin pipe it was started with ends the session.
        try:
            proc.stdin.close()
        except OSError:
            pass
    try:
        proc.send_signal(signal.SIGINT)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            pass  # a process that outlives SIGKILL must not stall the run
    server["log"].close()


def start_duck_server(ctx, storage):
    port = free_port()
    token = secrets.token_hex(8)
    log_path = ctx["work"] / "servers" / f"duck-{storage}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log = open(log_path, "w")
    script = "\n".join(
        duck_prefix(ctx, storage)
        + [
            "LOAD quack;",
            f"SELECT * FROM quack_serve('quack://127.0.0.1:{port}', "
            f"disable_ssl := true, token := '{token}');",
        ]
    ) + "\n"
    proc = subprocess.Popen(
        [ctx["bin"]["duckdb"], "-no-agent", ":memory:"],
        stdin=subprocess.PIPE,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        proc.stdin.write(script.encode())
        proc.stdin.flush()
    except BrokenPipeError:
        pass
    try:
        wait_tcp("127.0.0.1", port, SERVER_READY_TIMEOUT, proc)
    except RuntimeError:
        stop_server({"proc": proc, "log": log})
        raise
    return {"proc": proc, "port": port, "token": token, "log": log, "workdir": log_path.parent}


def rss_sampler(proc):
    peak = [0]
    stop = threading.Event()

    def sample():
        while not stop.is_set():
            try:
                out = subprocess.run(
                    ["ps", "-o", "rss=", "-p", str(proc.pid)],
                    capture_output=True,
                    text=True,
                    timeout=5,
                ).stdout.strip()
                if out:
                    peak[0] = max(peak[0], int(out.split()[0]))
            except (OSError, subprocess.SubprocessError, ValueError):
                pass
            stop.wait(0.1)

    thread = threading.Thread(target=sample, daemon=True)
    thread.start()
    return peak, stop


def latency_record(clients, requests, wall_s, peak_rss_kb):
    ms = [r["ms"] for r in requests if r["ms"] is not None]
    p50, p95, p99 = percentiles(ms)
    per_widget = {}
    per_widget_p95 = {}
    if clients == 1:
        by_name = {}
        for r in requests:
            if r["ms"] is not None:
                by_name.setdefault(r["name"], []).append(r["ms"])
        for name, vals in sorted(by_name.items()):
            per_widget[name] = round(statistics.median(vals), 3)
            per_widget_p95[name] = percentiles(vals)[1]
    return {
        "clients": clients,
        "requests": len(requests),
        "wall_s": round(wall_s, 3),
        "throughput_rps": round(len(requests) / wall_s, 2) if wall_s else None,
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
        "per_widget_p50_ms": per_widget,
        "per_widget_p95_ms": per_widget_p95,
        "server_peak_rss_mb": round(peak_rss_kb[0] / 1024, 1) if peak_rss_kb[0] else None,
    }


def chart_server_failure(clients, exc):
    """A server that never listened fails that phase; it does not abort the run."""
    return [{"clients": n, "status": "failed", "error": f"server: {exc}"} for n in clients]


def run_pink_charts(ctx, storage, clients, rounds, widgets):
    try:
        server = start_pink_server(ctx, storage)
    except (OSError, RuntimeError) as exc:
        return chart_server_failure(clients, exc)
    peak, stop = rss_sampler(server["proc"])
    try:
        # The emitted lake keeps the widget schemas in `r2_cat` and a server
        # session starts in the store's `main`, so the widgets name the
        # catalog, as the DuckDB client below does for its `memory` catalog.
        emitted_lake = (
            storage == "lake"
            and ctx["results"]["storages"][storage]["engines"]["pinkdb"]["mode"]
            == "emitted"
        )
        queries = ctx["work"] / "charts.json"
        queries.write_text(
            json.dumps(
                {
                    name: (
                        qualify_catalog(PLAN["chartSql"][name])
                        if emitted_lake
                        else PLAN["chartSql"][name]
                    )
                    for name in widgets
                }
            )
        )
        out = []
        for n in clients:
            try:
                r = subprocess.run(
                    [
                        ctx["bin"]["chart_load"],
                        f"127.0.0.1:{server['port']}",
                        str(n),
                        str(rounds),
                        str(queries),
                        str(SEED),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=CHART_TIMEOUT,
                )
            except subprocess.TimeoutExpired:
                out.append(
                    {
                        "clients": n,
                        "error": f"chart_load did not finish within {CHART_TIMEOUT}s",
                        "status": "failed",
                    }
                )
                continue
            if r.returncode:
                out.append(
                    {
                        "clients": n,
                        "error": first_error(r.stdout + r.stderr) or tail(r.stderr),
                        "status": "failed",
                    }
                )
                continue
            lines = [line for line in r.stdout.splitlines() if line.strip().startswith("{")]
            if not lines:
                out.append(
                    {
                        "clients": n,
                        "error": "chart_load printed no JSON: " + tail(r.stdout + r.stderr),
                        "status": "failed",
                    }
                )
                continue
            data = json.loads(lines[-1])
            rec = latency_record(n, data["requests"], data["wall_s"], peak)
            rec["status"] = "ok"
            out.append(rec)
            ctx["raw_latencies"].append(
                {
                    "storage": storage,
                    "engine": "pinkdb",
                    "clients": n,
                    "wall_s": data["wall_s"],
                    "requests": data["requests"],
                }
            )
        return out
    finally:
        stop.set()
        stop_server(server)


def qualify_catalog(sql: str) -> str:
    """Name ``r2_cat`` on a two-part widget table for a remote session.

    DuckDB's quack client starts in ``memory`` and a pinkdb server session
    starts in the store's ``main``; neither has the lake's schemas in scope.
    """
    return re.sub(r'\b(FROM|JOIN)\s+("[^"]+"\."[^"]+")', r'\1 "r2_cat".\2', sql)


def duck_client_script(widget_sqls, port, token, order):
    uri = f"quack://127.0.0.1:{port}"
    lines = ["LOAD quack;"]
    for name in sorted(widget_sqls):
        lines.append(
            f"SELECT * FROM quack_query('{uri}', $q${widget_sqls[name]}$q$, "
            f"disable_ssl := true, token := '{token}');"
        )
    lines.append(".timer on")
    for name in order:
        lines.append(
            f"SELECT * FROM quack_query('{uri}', $q${widget_sqls[name]}$q$, "
            f"disable_ssl := true, token := '{token}');"
        )
    lines.append(".quit")
    return "\n".join(lines) + "\n"


def run_duck_charts(ctx, storage, clients, rounds, widgets):
    try:
        server = start_duck_server(ctx, storage)
    except (OSError, RuntimeError) as exc:
        return chart_server_failure(clients, exc)
    peak, stop = rss_sampler(server["proc"])
    widget_sqls = {
        name: qualify_catalog(PLAN["chartSql"][name]) for name in widgets
    }
    try:
        out = []
        for n in clients:
            order = []
            for client in range(n):
                rng = random.Random(SEED + client)
                names = sorted(widget_sqls)
                for _ in range(rounds):
                    rng.shuffle(names)
                    order.extend(names)
            results = [None] * n
            client_errors = []
            barrier = threading.Barrier(n + 1)

            def worker(i):
                # A different shuffle per client and round; the order list is
                # shared but each client's slice starts at i*rounds*12.
                try:
                    start = i * rounds * len(widget_sqls)
                    mine = order[start : start + rounds * len(widget_sqls)]
                    script = duck_client_script(widget_sqls, server["port"], server["token"], mine)
                    barrier.wait()
                    p = subprocess.run(
                        [ctx["bin"]["duckdb"], "-no-agent", ":memory:"],
                        input=script,
                        text=True,
                        capture_output=True,
                        timeout=CHART_TIMEOUT,
                    )
                    results[i] = (p, mine)
                except Exception as exc:  # noqa: BLE001 - reported through results
                    results[i] = (None, [])
                    client_errors.append(str(exc))
                    try:
                        barrier.abort()
                    except Exception:
                        pass

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
            for t in threads:
                t.start()
            started = time.monotonic()
            try:
                barrier.wait(timeout=CHART_TIMEOUT)
            except threading.BrokenBarrierError:
                pass
            deadline = time.monotonic() + CHART_TIMEOUT + 30
            for i, t in enumerate(threads):
                t.join(max(0.0, deadline - time.monotonic()))
                if t.is_alive():
                    results[i] = (None, [])
                    client_errors.append(f"client {i} did not finish within {CHART_TIMEOUT}s")
            wall = time.monotonic() - started
            requests = []
            for i, (p, mine) in enumerate(results):
                if p is None or p.returncode:
                    client_errors.append(
                        first_error((p.stdout if p else "") + (p.stderr if p else "")) or "client failed"
                    )
                    continue
                timers = suite.timer_values(p.stdout)
                for name, seconds in zip(mine, timers):
                    requests.append(
                        {"client": i, "name": name, "ms": round(seconds * 1000, 3)}
                    )
            if client_errors:
                out.append({"clients": n, "status": "failed", "error": client_errors[0]})
                continue
            rec = latency_record(n, requests, wall, peak)
            rec["status"] = "ok"
            out.append(rec)
            ctx["raw_latencies"].append(
                {
                    "storage": storage,
                    "engine": "duckdb",
                    "clients": n,
                    "wall_s": round(wall, 3),
                    "requests": requests,
                }
            )
            peak[0] = 0
        return out
    finally:
        stop.set()
        stop_server(server)


def cold_start(ctx, storage, engine, mode):
    kpi = PLAN["chartSql"]["kpi_totals"]
    env = os.environ.copy()
    env["PINKDB_AGENT_MODE"] = "0"
    if engine == "pinkdb":
        if storage == "lake":
            env.update(ctx["lake"].pink_env())
        cmd = [ctx["bin"]["pink"], "-no-agent"]
        script = f".open {ctx['store'][storage]['pinkdb']}\n{terminate(kpi)}\n.quit\n"
    else:
        cmd = [ctx["bin"]["duckdb"], "-no-agent", ":memory:"]
        script = "\n".join(duck_prefix(ctx, storage) + [terminate(kpi), ".quit"]) + "\n"
    times = []
    for _ in range(5):
        start = time.monotonic()
        r = run_plain(cmd, script, env, timeout=900)
        if r["rc"]:
            return {"status": "failed", "error": first_error(r["stdout"] + r["stderr"])}
        times.append(round(time.monotonic() - start, 3))
    return {"status": "ok", "s": times, "median_s": round(statistics.median(times), 3)}


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def setup_engine(ctx, storage, engine):
    e = ctx["results"]["storages"][storage]["engines"][engine]
    store = ctx["store"][storage][engine]
    store.parent.mkdir(parents=True, exist_ok=True)
    if engine == "pinkdb":
        e["blocked"] = probe_pink(ctx, storage, ctx["work"] / "probe" / f"{storage}.pink")
        e["mode"] = "workaround" if e["blocked"] else "emitted"
    else:
        e["mode"] = "emitted"
    units = setup_units(engine, storage, e["mode"], ctx["lake"])
    e["setup"] = run_phase(ctx, engine, storage, store, units, "setup")
    if e["setup"]["status"] != "ok":
        e["alive"] = False


def compare_counts(ctx, storage, batch, engines):
    counts = {}
    for engine, e in engines.items():
        counts[engine] = count_rows(ctx, engine, storage, e["mode"])
        e["counts"][batch] = counts[engine]
    a, b = counts.get("pinkdb"), counts.get("duckdb")
    rec = {"checked": len(COUNT_IDS)}
    if a and b and a["status"] == "ok" and b["status"] == "ok":
        mismatch = [
            {"table": spec_id, "pinkdb": a["counts"].get(spec_id), "duckdb": b["counts"].get(spec_id)}
            for spec_id in COUNT_IDS
            if a["counts"].get(spec_id) != b["counts"].get(spec_id)
        ]
        rec["ok"] = not mismatch
        rec["mismatch"] = mismatch[:20]
    else:
        rec["ok"] = None
        rec["error"] = (a or {}).get("error") or (b or {}).get("error") or "engine did not run"
    ctx["results"]["storages"][storage]["agreement"]["counts"][batch] = rec
    if rec["ok"] is False:
        ctx["results"]["mismatch"] = True
        ctx["results"]["failures"].append(
            f"counts differ for {batch}: {rec['mismatch'][:3]}"
        )


def compare_charts(ctx, storage, engines):
    rows = {}
    for engine, e in engines.items():
        rows[engine] = chart_rows(ctx, engine, storage, e["mode"])
    a, b = rows.get("pinkdb"), rows.get("duckdb")
    rec = {"checked": len(PLAN["chartSql"])}
    if a and b:
        shared = [n for n in sorted(PLAN["chartSql"]) if n in a["rows"] and n in b["rows"]]
        bad, ties = [], []
        for n in shared:
            sql = PLAN["chartSql"][n]
            if rows_agree(sql, a["rows"][n], b["rows"][n]):
                continue
            (ties if tie_order(sql, a["rows"][n], b["rows"][n]) else bad).append(n)
        if shared:
            rec["ok"] = not bad
            rec["compared"] = len(shared)
            rec["widgets"] = shared
            rec["mismatch"] = bad
            rec["tie_order"] = ties
        else:
            rec["ok"] = None
            rec["error"] = "no widget ran on both engines"
        unsupported = {
            engine: rows[engine]["errors"] for engine in rows if rows[engine]["errors"]
        }
        if unsupported:
            rec["unsupported"] = unsupported
    else:
        rec["ok"] = None
        rec["error"] = (a or {}).get("error") or (b or {}).get("error") or "engine did not run"
    ctx["results"]["storages"][storage]["agreement"]["charts"] = rec
    if rec["ok"] is False:
        ctx["results"]["mismatch"] = True
        ctx["results"]["failures"].append(f"chart rows differ: {rec['mismatch']}")


def run_storage(ctx, storage):
    args = ctx["args"]
    st = ctx["results"]["storages"][storage]
    engines = {}
    for engine in args.engines:
        st["engines"][engine] = {
            "mode": None,
            "blocked": None,
            "setup": None,
            "scenarios": [],
            "alive": True,
            "counts": {},
            "cold_start": None,
            "charts": {},
        }
        engines[engine] = st["engines"][engine]
        setup_engine(ctx, storage, engine)
    # The insert column lists come from DuckDB's own output names; derive them
    # once from the setup store rather than trusting the spec order.
    if engines.get("duckdb") and engines["duckdb"]["setup"]["status"] == "ok":
        ctx["mv_columns"][storage] = describe_mv_columns(ctx, storage)
        missing = [s["id"] for s in MV_ORDER if s["id"] not in ctx["mv_columns"][storage]]
        if missing:
            ctx["results"]["notes"].append(
                f"{storage}: DuckDB DESCRIBE returned no columns for {len(missing)} materialized view(s); "
                "their pinkdb inserts fall back to the spec column order: " + ", ".join(missing[:5])
            )
    for batch in SIZES[args.size]:
        files = batch_files(ctx["data"], batch)
        if storage == "lake":
            ctx["lake"].upload(batch, files)
        raw_prefix = "s3://raw" if storage == "lake" else str(ctx["data"] / batch)
        for engine, e in engines.items():
            if not e["alive"]:
                continue
            units = etl_units(
                engine, storage, e["mode"], batch, files, raw_prefix, ctx["data"], ctx["gaps"]
            )
            res = run_phase(ctx, engine, storage, ctx["store"][storage][engine], units, f"etl-{batch}")
            e["scenarios"].append({"batch": batch, "etl": res, "refresh": None})
            if res["status"] != "ok":
                e["alive"] = False
                ctx["results"]["failures"].append(f"{engine}/{storage} ETL {batch}: {res['error']}")
        for engine, e in engines.items():
            if not e["alive"]:
                continue
            units = refresh_units(
                engine, e["mode"], ctx["gaps"], ctx["mv_columns"].get(storage) or {}
            )
            res = run_phase(
                ctx, engine, storage, ctx["store"][storage][engine], units, f"refresh-{batch}"
            )
            e["scenarios"][-1]["refresh"] = res
            if res["status"] != "ok":
                e["alive"] = False
                ctx["results"]["failures"].append(f"{engine}/{storage} refresh {batch}: {res['error']}")
        if all(e["alive"] for e in engines.values()):
            compare_counts(ctx, storage, batch, engines)
    if all(e["alive"] for e in engines.values()):
        compare_charts(ctx, storage, engines)
        charts = st["agreement"]["charts"] or {}
        # Measure the widgets both engines executed, so the latency numbers
        # compare the same work; the rest are recorded as unsupported below.
        widgets = charts.get("widgets") or sorted(PLAN["chartSql"])
        for engine, e in engines.items():
            e["cold_start"] = cold_start(ctx, storage, engine, e["mode"])
            e["charts_widgets"] = {
                "measured": len(widgets),
                "skipped": (charts.get("unsupported") or {}).get(engine, {}),
            }
        for engine in engines:
            if engine == "pinkdb":
                recs = run_pink_charts(ctx, storage, args.clients, args.rounds, widgets)
            else:
                recs = run_duck_charts(ctx, storage, args.clients, args.rounds, widgets)
            for rec in recs:
                st["engines"][engine]["charts"][str(rec["clients"])] = rec
    else:
        ctx["results"]["notes"].append(
            f"{storage}: charts not measured because a batch/refresh phase failed"
        )


def disk_free_gb():
    try:
        st = os.statvfs("/System/Volumes/Data")
        return round(st.f_bavail * st.f_frsize / 1e9, 1)
    except OSError:
        return None


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="QuackLab workload on pinkdb and DuckDB")
    p.add_argument("--size", choices=sorted(SIZES), default="small")
    p.add_argument("--storage", default="local,lake")
    p.add_argument("--engines", default="pinkdb,duckdb")
    p.add_argument("--clients", default="1,4,16")
    p.add_argument("--rounds", type=int, default=10)
    p.add_argument("--work-dir", default=str(DEFAULT_WORK))
    p.add_argument("--grain", default=None, help="QuackLab checkout (default: $GRAIN or ~/Desktop/projects/grain/grain)")
    p.add_argument("--keep", action="store_true")
    p.add_argument("--pink-cli", default=None)
    p.add_argument("--pink-server", default=None)
    p.add_argument("--chart-load", default=None)
    p.add_argument("--duckdb", default=None)
    p.add_argument("--phase-timeout", type=int, default=7200, help=argparse.SUPPRESS)
    return p.parse_args(argv)


def project_bin(name, explicit):
    if explicit:
        return str(Path(explicit).expanduser().resolve())
    cand = ROOT / "target/release" / name
    if cand.exists():
        return str(cand)
    found = shutil.which(name)
    if found:
        return str(Path(found).resolve())
    raise SystemExit(f"missing {name}: build it first (see bench/quacklab/README.md)")


def main(argv=None):
    args = parse_args(argv)
    args.storage = [s.strip() for s in args.storage.split(",") if s.strip()]
    args.engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    args.clients = [int(c) for c in args.clients.split(",") if c.strip()]
    if "duckdb" not in args.engines:
        raise SystemExit("the agreement checks need the duckdb engine")
    grain = Path(args.grain or os.environ.get("GRAIN") or DEFAULT_GRAIN).expanduser().resolve()
    data = grain / "dummy-data"
    if not data.is_dir():
        raise SystemExit(
            f"QuackLab batch data not found: {data} (set GRAIN or pass --grain <checkout>)"
        )
    duckdb = args.duckdb or str(Path.home() / ".local/bin/duckdb")
    if not Path(duckdb).exists():
        duckdb = shutil.which("duckdb") or duckdb
    suite.DEFAULT_DUCKDB = duckdb

    work = Path(args.work_dir).expanduser().resolve()
    if "lake" in args.storage and port_open("127.0.0.1", PG_PORT):
        raise SystemExit(
            f"port {PG_PORT} is already in use: a previous --keep run may still have "
            "Postgres running; stop it before measuring again (the work dir is kept)"
        )
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    RESULTS.mkdir(parents=True, exist_ok=True)

    ctx = {
        "args": args,
        "work": work,
        "lake": None,
        "grain": str(grain),
        "data": data,
        "gaps": {},
        "mv_columns": {},
        "raw_latencies": [],
        "bin": {
            "pink": project_bin("pinkdb", args.pink_cli),
            "server": project_bin("pinkdb-server", args.pink_server),
            "chart_load": project_bin("examples/chart_load", args.chart_load),
            "duckdb": str(Path(duckdb).resolve()),
        },
        "store": {
            storage: {
                "pinkdb": work / storage / "pink.pink",
                "duckdb": work / storage / "duck.duckdb",
            }
            for storage in args.storage
        },
    }
    results = {
        "wave": 130,
        "size": args.size,
        "grain": str(grain),
        "scenarios": SIZES[args.size],
        "storages_requested": args.storage,
        "clients": args.clients,
        "rounds": args.rounds,
        "seed": SEED,
        "machine": suite.machine_info(),
        "load_average": {"before": [round(x, 2) for x in suite.load_average()]},
        "disk_free_gb": {"before": disk_free_gb()},
        "plan": {
            "file": str(PLAN_PATH.relative_to(ROOT)),
            "build_id": PLAN["executionPlan"].get("buildId"),
            "version": PLAN["executionPlan"].get("version"),
            "skipped_steps": PLAN_SKIPPED,
            "tables": len(SPECS),
            "materialized_views": len(MV_ORDER),
            "count_tables": len(COUNT_IDS),
            "shims": {
                "pinkdb": [
                    "strip r2_cat. prefix (no schemas: database.table)",
                    "shims are probe-driven: see pinkdb_gaps (each emitted form is run once; shimmed only if it fails)",
                ],
                "duckdb_quack": "chart SQL table refs qualified with r2_cat. (remote sessions start in memory)",
            },
        },
        "results_path": None,
        "pinkdb_gaps": {},
        "storages": {},
        "mismatch": False,
        "failures": [],
        "notes": [],
        "error": None,
    }
    ctx["results"] = results

    def interrupted(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)

    lake = None
    started = time.monotonic()
    try:
        if "pinkdb" in args.engines:
            ctx["gaps"] = probe_gaps(ctx)
            results["pinkdb_gaps"] = ctx["gaps"]
        if "lake" in args.storage:
            lake = Lake(work, data)
            lake.start()
            ctx["lake"] = lake
        for storage in args.storage:
            results["storages"][storage] = {
                "engines": {},
                "agreement": {"counts": {}, "charts": None},
            }
            run_storage(ctx, storage)
    except KeyboardInterrupt:
        results["error"] = "interrupted"
        results["failures"].append("interrupted")
    except Exception:
        results["error"] = traceback.format_exc()
    finally:
        results["wall_s"] = round(time.monotonic() - started, 1)
        results["load_average"]["after"] = [round(x, 2) for x in suite.load_average()]
        results["disk_free_gb"]["after"] = disk_free_gb()
        if lake is not None and not args.keep:
            try:
                lake.stop()
            except Exception:
                results["notes"].append("lake stop failed")
        try:
            if ctx["raw_latencies"]:
                raw = work / "raw-latencies.json"
                raw.write_text(json.dumps(ctx["raw_latencies"], separators=(",", ":")) + "\n")
                results["raw_latencies_file"] = "raw-latencies.json (in the work dir; deleted unless --keep)"
            json_path, md_path = write_results(results, args)
            results["results_path"] = str(json_path.relative_to(ROOT))
            print(f"results: {json_path}")
            print(f"summary: {md_path}")
        except Exception:
            print(traceback.format_exc(), file=sys.stderr)
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print(f"kept work dir: {work}")

    if results["error"]:
        print(results["error"], file=sys.stderr)
    if results["mismatch"] or results["failures"]:
        return 1
    return 0


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


def write_results(results, args):
    machine = results["machine"]
    sha = machine.get("commit_short") or "unknown"
    day = time.strftime("%Y-%m-%d")
    base = RESULTS / f"{day}-{sha}-{args.size}"
    json_path = base.with_suffix(".json")
    json_path.write_text(json.dumps(results, separators=(",", ":")) + "\n")
    md_path = base.with_suffix(".md")
    md_path.write_text(render_markdown(results) + "\n")
    return json_path, md_path


def fmt(value, digits=3):
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def ratio(a, b, digits=2):
    if a is None or not b:
        return "—"
    return f"{a / b:.{digits}f}×"


def md_cell(value, limit=200):
    text = str(value or "").replace("|", "/").replace("\n", " ").strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


def render_markdown(results) -> str:
    m = results["machine"]
    out = []
    out.append(f"# QuackLab on pinkdb — wave 130 ({results['size']})")
    out.append("")
    out.append(
        "QuackLab's compiled NYC-taxi workload rendered from "
        "`bench/quacklab/plan/nyc_taxi_essential1.json`; pinkdb release binaries "
        "versus DuckDB v2.0.0-alpha46057 on the same machine. Wall times are one "
        "CLI process per phase (setup / ETL / mart refresh); charts run the widgets "
        "both engines executed, per client, through pinkdb-server and DuckDB `quack` "
        "(widgets pinkdb cannot parse are listed under *Charts*)."
    )
    out.append("")
    out.append("## Machine and conditions")
    out.append("")
    out.append("| field | value |")
    out.append("|---|---|")
    for key in (
        "host",
        "os",
        "os_version",
        "cpu",
        "physical_cores",
        "logical_cores",
        "memory_gb",
        "rustc",
        "duckdb",
        "branch",
        "commit",
        "dirty",
        "python",
    ):
        out.append(f"| {key} | {m.get(key)} |")
    out.append(f"| load average before | {results['load_average']['before']} |")
    out.append(f"| load average after | {results['load_average']['after']} |")
    out.append(f"| free disk GB | {results['disk_free_gb']} |")
    out.append(f"| scenarios | {', '.join(results['scenarios'])} |")
    out.append(f"| clients / rounds | {results['clients']} / {results['rounds']} |")
    out.append(f"| total wall | {results['wall_s']} s |")
    out.append("")
    skipped = results["plan"]["skipped_steps"]
    macros = [x for x in skipped if "refresh_mart" in x]
    other = [x for x in skipped if "refresh_mart" not in x]
    out.append(
        f"Plan: {results['plan']['tables']} table specs, "
        f"{results['plan']['materialized_views']} materialized views, "
        f"{results['plan']['count_tables']} count-agreement tables. "
        f"Skipped workload steps without SQL: {', '.join(other) or 'none'}; "
        f"{len(macros)} `refresh_mart` macros run in the refresh phase (listed in the JSON)."
    )
    out.append("")

    blocked = []
    for storage, st in results["storages"].items():
        for engine, e in st["engines"].items():
            if e.get("blocked"):
                b = e["blocked"]
                blocked.append((storage, engine, b.get("by"), b.get("error")))
    out.append("## Blocked emitted statements (decision 6)")
    out.append("")
    if blocked:
        out.append("| storage | engine | blocked by | pinkdb error |")
        out.append("|---|---|---|---|")
        for storage, engine, by, err in blocked:
            out.append(f"| {storage} | {engine} | wave {by} | {str(err).replace('|', '/')} |")
    else:
        out.append("None: pinkdb ran QuackLab's emitted statements unchanged.")
    out.append("")

    gaps = results.get("pinkdb_gaps") or {}
    out.append("## pinkdb dialect gaps")
    out.append("")
    if gaps:
        out.append(
            "Each shimmed form was run once on a throwaway workaround store before the "
            "measured phases; a shim is applied only when that emitted form fails, so a "
            "form fixed in a later wave stops being shimmed automatically."
        )
        out.append("")
        out.append("| shim | emitted (truncated) | pinkdb error | shim applied |")
        out.append("|---|---|---|---|")
        for name, rec in gaps.items():
            out.append(
                f"| {name} | `{md_cell(rec.get('emitted'))}` | "
                f"{md_cell(rec.get('error')) or '—'} | "
                f"{'yes' if rec.get('applied') else 'no'} |"
            )
    else:
        out.append("No pinkdb gap probe was run.")
    out.append("")

    out.append("## Setup")
    out.append("")
    out.append("| storage | engine | mode | status | wall s | RSS MB | statements |")
    out.append("|---|---|---|---|---|---|---|")
    for storage, st in results["storages"].items():
        for engine, e in st["engines"].items():
            s = e.get("setup") or {}
            out.append(
                f"| {storage} | {engine} | {e.get('mode')} | {s.get('status')} | "
                f"{fmt(s.get('wall_s'))} | {fmt(s.get('max_rss_mb'))} | {s.get('n_units')} |"
            )
    out.append("")

    out.append("## ETL wall time per scenario (s)")
    out.append("")
    out.append("| scenario | storage | pinkdb | DuckDB | pink/Duck |")
    out.append("|---|---|---|---|---|")
    for batch in results["scenarios"]:
        for storage, st in results["storages"].items():
            vals = {}
            for engine, e in st["engines"].items():
                rec = next((r for r in e["scenarios"] if r["batch"] == batch), None)
                vals[engine] = ((rec or {}).get("etl") or {}).get("wall_s")
            out.append(
                f"| {batch} | {storage} | {fmt(vals.get('pinkdb'))} | "
                f"{fmt(vals.get('duckdb'))} | {ratio(vals.get('pinkdb'), vals.get('duckdb'))} |"
            )
    out.append("")

    out.append("## Validation SELECTs reporting `fail` (not fatal)")
    out.append("")
    out.append("| storage | engine | scenario | fail rows | checks |")
    out.append("|---|---|---|---|---|")
    for batch in results["scenarios"]:
        for storage, st in results["storages"].items():
            for engine, e in st["engines"].items():
                rec = next((r for r in e["scenarios"] if r["batch"] == batch), None)
                fails = ((rec or {}).get("etl") or {}).get("validation_fails") or []
                names = ", ".join(f"{f[0]}/{f[1]}" for f in fails[:4])
                if len(fails) > 4:
                    names += f", … (+{len(fails) - 4})"
                out.append(
                    f"| {storage} | {engine} | {batch} | {len(fails)} | {names or '—'} |"
                )
    out.append("")

    out.append(
        "## Mart refresh wall time per scenario (s, "
        f"{results['plan']['materialized_views']} materialized views)"
    )
    out.append("")
    out.append("| scenario | storage | pinkdb | DuckDB | pink/Duck |")
    out.append("|---|---|---|---|---|")
    for batch in results["scenarios"]:
        for storage, st in results["storages"].items():
            vals = {}
            for engine, e in st["engines"].items():
                rec = next((r for r in e["scenarios"] if r["batch"] == batch), None)
                vals[engine] = ((rec or {}).get("refresh") or {}).get("wall_s")
            out.append(
                f"| {batch} | {storage} | {fmt(vals.get('pinkdb'))} | "
                f"{fmt(vals.get('duckdb'))} | {ratio(vals.get('pinkdb'), vals.get('duckdb'))} |"
            )
    out.append("")

    out.append("## Charts")
    out.append("")
    for storage, st in results["storages"].items():
        for engine, e in st["engines"].items():
            w = e.get("charts_widgets") or {}
            skipped = ", ".join(sorted(w.get("skipped") or {})) or "none"
            out.append(
                f"- {storage}/{engine}: {w.get('measured', 12)} of {len(PLAN['chartSql'])} "
                f"widgets measured; unsupported: {skipped}"
            )
    widget_errors = []
    for storage, st in results["storages"].items():
        charts = (st.get("agreement") or {}).get("charts") or {}
        for name, err in sorted(((charts.get("unsupported") or {}).get("pinkdb") or {}).items()):
            widget_errors.append((storage, name, err))
    if widget_errors:
        out.append("")
        out.append("Widgets whose emitted SQL pinkdb rejected (recorded, not timed):")
        out.append("")
        out.append("| storage | widget | pinkdb error |")
        out.append("|---|---|---|")
        for storage, name, err in widget_errors:
            out.append(f"| {storage} | {name} | {md_cell(err)} |")
    out.append("")
    out.append(
        "| storage | clients | pinkdb p50 | pinkdb p95 | pinkdb req/s | "
        "DuckDB p50 | DuckDB p95 | DuckDB req/s | p50 pink/Duck | "
        "server MB pink | server MB Duck |"
    )
    out.append("|---|---|---|---|---|---|---|---|---|---|---|")

    for storage, st in results["storages"].items():
        for clients in results["clients"]:
            p = st["engines"].get("pinkdb", {}).get("charts", {}).get(str(clients))
            d = st["engines"].get("duckdb", {}).get("charts", {}).get(str(clients))
            if (p or {}).get("status") != "ok" or (d or {}).get("status") != "ok":
                for engine, rec in (("pinkdb", p), ("duckdb", d)):
                    if (rec or {}).get("status") != "ok":
                        out.append(
                            f"| {storage} | {clients} | {engine} failed: "
                            f"{str((rec or {}).get('error')).replace('|', '/')} |"
                        )
                continue
            out.append(
                f"| {storage} | {clients} | {fmt(p['p50_ms'])} | {fmt(p['p95_ms'])} | "
                f"{fmt(p['throughput_rps'], 2)} | {fmt(d['p50_ms'])} | {fmt(d['p95_ms'])} | "
                f"{fmt(d['throughput_rps'], 2)} | "
                f"{ratio(p['p50_ms'], d['p50_ms'])} | "
                f"{fmt(p['server_peak_rss_mb'])} | {fmt(d['server_peak_rss_mb'])} |"
            )
    out.append("")
    out.append(
        "pinkdb per-request latency is client-side `Instant` timing inside chart_load. "
        "DuckDB per-request latency includes its client CLI and is parsed from 1 ms "
        "`Run Time` lines, so its p50 is quantised and sub-millisecond comparisons "
        "reflect the clients rather than the servers. Throughput at 16 clients (and the "
        "server peak RSS) is the server-bound comparison."
    )
    out.append("")

    out.append("## Per-widget p50 at 1 client (ms)")
    out.append("")
    out.append("| storage | widget | pinkdb | DuckDB |")
    out.append("|---|---|---|---|")
    for storage, st in results["storages"].items():
        p = st["engines"].get("pinkdb", {}).get("charts", {}).get("1", {})
        d = st["engines"].get("duckdb", {}).get("charts", {}).get("1", {})
        for name in sorted(PLAN["chartSql"]):
            pv = (p.get("per_widget_p50_ms") or {}).get(name) if p.get("status") == "ok" else None
            dv = (d.get("per_widget_p50_ms") or {}).get(name) if d.get("status") == "ok" else None
            out.append(f"| {storage} | {name} | {fmt(pv)} | {fmt(dv)} |")
    out.append("")

    out.append("## Cold start (`kpi_totals`, fresh process, median of 5)")
    out.append("")
    out.append("| storage | engine | median s | runs s |")
    out.append("|---|---|---|---|")
    for storage, st in results["storages"].items():
        for engine, e in st["engines"].items():
            c = e.get("cold_start") or {}
            out.append(
                f"| {storage} | {engine} | {fmt(c.get('median_s'))} | "
                f"{', '.join(str(x) for x in c.get('s', [])) or c.get('error', '—')} |"
            )
    out.append("")

    out.append("## Peak RSS per phase (MB)")
    out.append("")
    out.append("| storage | phase | pinkdb | DuckDB |")
    out.append("|---|---|---|---|")
    for storage, st in results["storages"].items():
        phases = ["setup"]
        for batch in results["scenarios"]:
            phases += [f"etl {batch}", f"refresh {batch}"]
        for phase in phases:
            vals = {}
            for engine, e in st["engines"].items():
                rec = e.get("setup") if phase == "setup" else None
                if rec is None:
                    for s in e["scenarios"]:
                        if phase == f"etl {s['batch']}":
                            rec = s.get("etl")
                        elif phase == f"refresh {s['batch']}":
                            rec = s.get("refresh")
                vals[engine] = (rec or {}).get("max_rss_mb")
            out.append(
                f"| {storage} | {phase} | {fmt(vals.get('pinkdb'))} | {fmt(vals.get('duckdb'))} |"
            )
    out.append("")

    out.append("## Agreement (decision 8)")
    out.append("")
    out.append("| storage | check | result | detail |")
    out.append("|---|---|---|---|")
    for storage, st in results["storages"].items():
        for batch, rec in st["agreement"]["counts"].items():
            detail = (
                "; ".join(
                    f"{x['table']}: pink={x['pinkdb']} duck={x['duckdb']}"
                    for x in rec.get("mismatch", [])
                )
                or rec.get("error", "")
            )
            out.append(f"| {storage} | counts {batch} | {rec.get('ok')} | {detail} |")
        charts = st["agreement"]["charts"]
        if charts:
            detail = ", ".join(charts.get("mismatch", [])) or charts.get("error", "")
            if charts.get("tie_order"):
                detail += ("; " if detail else "") + (
                    "tie order differs (same rows): " + ", ".join(charts["tie_order"])
                )
            out.append(f"| {storage} | widget rows | {charts.get('ok')} | {detail} |")
    out.append("")

    fails = []
    for storage, st in results["storages"].items():
        for engine, e in st["engines"].items():
            if e.get("setup") and e["setup"]["status"] != "ok":
                fails.append(f"setup {engine}/{storage}: {e['setup'].get('error')}")
            for s in e["scenarios"]:
                for phase in ("etl", "refresh"):
                    rec = s.get(phase)
                    if rec and rec.get("status") != "ok":
                        fails.append(f"{phase} {engine}/{storage} {s['batch']}: {rec.get('error')}")
    out.append("## Failed phases")
    out.append("")
    if fails:
        for line in fails:
            out.append(f"- {line}")
    else:
        out.append("None.")
    out.append("")

    out.append("## Dialect shims used for the pinkdb workaround")
    out.append("")
    out.append("- strip the r2_cat. prefix (pinkdb has no schemas: two-part names are database.table)")
    gaps = results.get("pinkdb_gaps") or {}
    for name, rec in gaps.items():
        state = "applied" if rec.get("applied") else "not needed (emitted form ran)"
        out.append(f"- {name}: {state}")
    out.append(f"- DuckDB quack: {results['plan']['shims']['duckdb_quack']}")
    out.append("")

    out.append("## Notes")
    out.append("")
    for line in results["notes"]:
        out.append(f"- {line}")
    if results["error"]:
        out.append(f"- run error: {results['error'].splitlines()[-1]}")
    if not results["notes"] and not results["error"]:
        out.append("None.")
    out.append("")
    return "\n".join(out)


if __name__ == "__main__":
    sys.exit(main())
