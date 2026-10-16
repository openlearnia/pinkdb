#!/usr/bin/env python3
"""Multi-process chaos soak for pinkdb's durable `.pink` writers (wave 45n).

One shared durable database, N worker processes with mixed roles:

  * writers      commit units (autocommit inserts or short transactions) and
                 count a unit as acknowledged only when the harness observed
                 the *statement's own success output* in a clean window
                 (`Inserted 1 row(s).` / `Transaction committed.`, with no
                 `Error:` line since the unit started). The CLI keeps
                 executing lines after an error, so an asynchronously-fed
                 marker could otherwise "ack" a write that never committed —
                 the drive is therefore synchronous, with a checked `.open`
                 handshake and a BEGIN confirmation before a transaction's
                 inserts. A unit that reports an error is retried; before
                 every retry a verified cleanup (DELETE of that unit's rows,
                 proven by a zero-rows probe) removes any partial effect, so
                 a retry can never duplicate. A busy unit is retried until
                 its per-unit deadline, then the writer stops (busy abandons
                 are counted);
  * readers      run counting SELECTs in a loop (refresh paths under load),
  * checkpointer runs `checkpoint;` plus reads in a loop (rolls + rebuilds),
  * open-close   short-lived processes that `.open`, count, and exit
                 (recovery/epoch prechecks against a busy database).

Busy opens are retried, then reported as failed opens — a failed open no
longer cascades `table not found` noise through the whole run.

The harness randomly `kill -9`s live workers, respawns replacements with
fresh writer ids, and samples per-worker fd count + RSS over time (leak
detection). At the end a FRESH process dumps the final state and the
harness verifies, per writer:

  * every acknowledged unit is present exactly once, with every row of the
    unit (transaction atomicity: a transaction's rows appear together),
  * present units are a contiguous prefix of the attempted sequence (a
    crash between COMMIT and the ack leaves a durable unit unacknowledged
    but present; an abandoned busy unit leaves a gap-free shorter prefix),
  * no duplicate rows, no rows from units that were never attempted, and
    the COUNT/SUM aggregates match the dumped rows exactly,
  * the dump is identical on a second fresh open (no delayed loss).

Usage:
  python3 bench/mp_soak.py --ci                       # ~15 s, 6 writers
  python3 bench/mp_soak.py --duration 300 --workers 8 --min-kills 40
  PINKDB_KEY=pass python3 bench/mp_soak.py --encrypted --duration 300 ...
  python3 bench/mp_soak.py --contention 20            # 20-writer hammer

Contention mode (`--contention N`) spawns N writers at once, each committing
M units with a SHORT busy timeout and no kills; it measures wall time,
per-writer skew, and requires: every acknowledged write present exactly once,
no livelock/deadlock inside the deadline, and every error (if any) the
documented busy kind.

`--encrypted` reads the passphrase from `PINKDB_KEY` in the environment
(never from the command line) and needs an encryption-feature binary
(`cargo build -p pinkdb-cli --features encryption`).

Exit code 0 iff every check passed; failing evidence stays under the printed
work directory.
"""

import argparse
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_BIN = os.path.join(ROOT, "target", "debug", "pinkdb")
WORK = tempfile.mkdtemp(prefix="pinkdb_mpsoak_")
DB = "x.pink"

ERROR_MARK = "Error:"
BUSY_RE = re.compile(r"busy|timeout", re.IGNORECASE)
INSERT_OK = "Inserted 1 row(s)."
TXN_BEGIN_OK = "Transaction started."
TXN_COMMIT_OK = "Transaction committed."
DELETE_OK_RE = re.compile(r"Deleted \d+ row\(s\)\.")
COUNT_ROW_RE = re.compile(r"│\s*(\d+)\s*│")

# Per-writer unit budget before the process exits and the harness respawns a
# fresh writer id (churn keeps the id space and the WAL lineage moving).
UNITS = 200
TXNR = 5

# Statement window used between the harness and the CLI (busy waits inside
# the CLI can be long; the driver's per-unit deadline bounds the total).
_UNIT_WINDOW_S = 90.0


def unit_window():
    return _UNIT_WINDOW_S


def parse_three_cols(out):
    """Parse rows of a `SELECT w, u, j FROM t` boxed table."""
    rows = []
    for line in out.splitlines():
        if not line.startswith("│"):
            continue
        parts = [p.strip() for p in line.strip("│").split("│")]
        if (
            len(parts) == 3
            and re.fullmatch(r"w\d+", parts[0])
            and re.fullmatch(r"\d+", parts[1])
            and re.fullmatch(r"\d+", parts[2])
        ):
            rows.append((parts[0], int(parts[1]), int(parts[2])))
    return rows


def parse_two_cols(out):
    """Parse rows of a `SELECT w, u FROM t` boxed table."""
    rows = []
    for line in out.splitlines():
        if not line.startswith("│"):
            continue
        parts = [p.strip() for p in line.strip("│").split("│")]
        if len(parts) == 2 and re.fullmatch(r"\d+", parts[0]) and re.fullmatch(r"\d+", parts[1]):
            rows.append((int(parts[0]), int(parts[1])))
    return rows


# Subprocess environment; `--encrypted` replaces it with a copy carrying
# PINKDB_KEY (the passphrase never appears on a command line).
ENV = None
ENCRYPTED = False


def cli(binary, cmds, cwd, env=None, timeout=None):
    """One-shot CLI run; returns merged stdout+stderr (a timeout appends an
    explicit error line)."""
    try:
        p = subprocess.run(
            [binary],
            input=cmds.encode(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=cwd,
            env=env if env is not None else ENV,
            timeout=timeout,
        )
        return p.stdout.decode(errors="replace")
    except subprocess.TimeoutExpired as e:
        text = e.stdout.decode(errors="replace") if e.stdout else ""
        return text + f"\nError: CLI run exceeded its {timeout}s timeout\n"


def is_busy(line):
    return bool(BUSY_RE.search(line))


# ---------------------------------------------------------------- workers


class Worker:
    """One spawned CLI process plus its reader thread and samples."""

    def __init__(self, kind, wid, proc):
        self.kind = kind
        self.wid = wid
        self.proc = proc
        self.lines = []
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.eof = False
        self.spawned = time.time()
        self.samples = []  # (t, rss_kb, fd_count)
        self.acked = set()
        self.units = {}  # unit -> row count (attempted, contiguous prefix)
        self.abandoned = False
        self.finished = False
        self.kill_stuck = 0
        self.died_at = None
        self.open_failed = None
        self.masked = []  # (start, end) line ranges excluded from error scans
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self):
        try:
            for raw in iter(self.proc.stdout.readline, b""):
                line = raw.decode(errors="replace")
                with self.cond:
                    self.lines.append(line)
                    self.cond.notify_all()
        except Exception:
            pass
        finally:
            with self.cond:
                self.eof = True
                self.cond.notify_all()

    def send(self, text):
        try:
            self.proc.stdin.write(text.encode())
            self.proc.stdin.flush()
            return True
        except Exception:
            return False

    def alive(self):
        return self.proc.poll() is None

    def kill(self):
        """SIGKILL the worker; returns False when it was still alive after a
        10 s wait (a kill that did not take effect — e.g. a process stuck in
        an uninterruptible syscall — keeps holding the lease)."""
        try:
            self.proc.kill()
        except ProcessLookupError:
            pass
        try:
            self.proc.wait(timeout=10)
        except Exception:
            if self.proc.poll() is None:
                self.kill_stuck += 1
                return False
        return True

    def quit(self):
        self.send(".quit\n")
        try:
            self.proc.wait(timeout=10)
        except Exception:
            self.kill()

    def count(self):
        with self.cond:
            return len(self.lines)

    def window(self, start):
        with self.cond:
            return "".join(self.lines[start:])

    def wait_for(self, start, marker, timeout):
        """Wait until `marker` or an error appears in lines[start:] (or the
        worker eofs / dies / the timeout expires). Returns the window text
        and its error lines."""
        deadline = time.time() + timeout
        with self.cond:
            while True:
                text = "".join(self.lines[start:])
                errs = [l for l in self.lines[start:] if ERROR_MARK in l]
                if marker in text or errs or self.eof:
                    return text, errs
                remaining = deadline - time.time()
                if remaining <= 0:
                    return text, errs
                self.cond.wait(min(remaining, 0.25))

    def drain(self, quiet=0.06, cap=1.5):
        """Wait for output quiescence (consumes stragglers without parsing)."""
        deadline = time.time() + cap
        last = self.count()
        while time.time() < deadline:
            self.cond.acquire()
            try:
                self.cond.wait(quiet)
            finally:
                self.cond.release()
            now = self.count()
            if now == last:
                return True
            last = now
        return False

    def output(self):
        with self.cond:
            return "".join(self.lines)


def spawn(binary, kind, wid, cwd, env=None):
    p = subprocess.Popen(
        [binary],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=cwd,
        env=env if env is not None else ENV,
    )
    return Worker(kind, wid, p)


def wait_opened(w, timeout=120.0, log=None):
    """Handshake: `.open DB`, retrying busy/temporary errors. Sets
    `open_failed` when the deadline expires with the process still alive
    (a worker killed during its open was chaos, not an open failure);
    returns True on success."""
    deadline = time.time() + timeout
    attempts = 0
    while time.time() < deadline and w.alive():
        attempts += 1
        start = w.count()
        if not w.send(f".open {DB}\n"):
            break
        text, errs = w.wait_for(
            start, f"Opened {DB}", timeout=max(1.0, min(60.0, deadline - time.time()))
        )
        if f"Opened {DB}" in text and not errs:
            return True
        if not w.alive():
            return False
        if errs and log is not None:
            log.append(f"{w.kind} {w.wid}: open attempt {attempts}: {errs[-1].strip()[:160]}")
        time.sleep(0.1)
    if w.alive():
        w.open_failed = f"{w.kind} {w.wid}: could not open after {attempts} attempts"
    return False


# ------------------------------------------------------- writer unit driver
#
# Ack protocol (the harness' durability contract):
#   1. send the unit's statements synchronously;
#   2. the unit is acknowledged iff the statement's own success output
#      appeared with no `Error:` line in the same window (the CLI prints a
#      statement's error *instead of* its success line, so success is proof
#      the write committed — never a marker printed past a failed write);
#   3. on any error/ambiguity, a verified cleanup runs before the retry:
#      DELETE this unit's rows and prove zero remain with a
#      `HAVING COUNT(*) = 0` probe, so a retry can never duplicate;
#   4. a unit that cannot settle before its deadline stops the writer
#      (`abandoned`); everything it acknowledged stays exactly-once.


def settle(w, start, marker, timeout):
    """Wait for a unit statement's success marker. Returns True iff the
    marker is present and no error line is in the window."""
    text, errs = w.wait_for(start, marker, timeout)
    return marker in text and not errs, text, errs


def cleanup_unit(w, wid, unit_where, deadline):
    """Remove every durable effect of unit `u` (rows matching `unit_where`),
    with a probe proving zero remain. Returns True when the unit is clean.

    Starts with ROLLBACK + drain: a failed statement inside an explicit
    transaction leaves it open, and closing it is part of the cleanup; the
    error a ROLLBACK reports when no transaction is open is expected and is
    masked from the run's error scan (`w.masked`)."""
    rstart = w.count()
    w.send("ROLLBACK;\n")
    w.drain()
    w.masked.append((rstart, w.count()))
    if not w.alive():
        return False
    while w.alive() and time.time() < deadline:
        start = w.count()
        w.send(f"DELETE FROM t WHERE {unit_where};\n")
        ok, text, errs = settle(w, start, "row(s).", unit_window())
        end = w.count()
        w.masked.append((start, end))
        if not w.alive():
            return False
        if not ok or not DELETE_OK_RE.search(text):
            # A hard (non-busy) error cannot be retried away; stop the writer
            # rather than risk duplicates. Busy errors retry.
            if any(not is_busy(e) for e in errs):
                return False
            continue
        start = w.count()
        w.send(
            f"SELECT 'CLEAN {wid}' AS m FROM t WHERE {unit_where} HAVING COUNT(*) = 0;\n"
        )
        text, errs = w.wait_for(start, f"CLEAN {wid}", unit_window())
        end = w.count()
        w.masked.append((start, end))
        if not w.alive():
            return False
        if f"CLEAN {wid}" in text and not errs:
            return True
        if any(not is_busy(e) for e in errs):
            return False
    return False


def drive_unit(w, wid, unit_sql, txn, unit_where, deadline):
    """One unit, retried (with verified cleanup) until acknowledged or the
    deadline. Returns True when acknowledged."""
    first = True
    while w.alive() and time.time() < deadline:
        if not first:
            # Consume stragglers from the failed attempt before a retry.
            w.drain(quiet=0.03, cap=0.5)
            if not w.alive():
                return False
        first = False
        if txn:
            # Confirm BEGIN before sending the inserts: if BEGIN failed and
            # the inserts ran anyway they would autocommit one by one.
            start = w.count()
            w.send("BEGIN;\n")
            ok, text, _ = settle(w, start, TXN_BEGIN_OK, unit_window())
            if not ok:
                if not cleanup_unit(w, wid, unit_where, deadline):
                    return False
                continue
            start = w.count()
            for sql in unit_sql:
                w.send(sql + "\n")
            w.send("COMMIT;\n")
            ok, text, _ = settle(w, start, TXN_COMMIT_OK, unit_window())
            if ok:
                return True
        else:
            start = w.count()
            w.send(unit_sql[0] + "\n")
            ok, text, _ = settle(w, start, INSERT_OK, unit_window())
            if ok:
                return True
        if not w.alive():
            return False
        # Anything else: settle the unit's durable state, then retry.
        if not cleanup_unit(w, wid, unit_where, deadline):
            return False
    return False


def drive_writer(w, wid, seed, stop, unit_timeout):
    rng = random.Random(seed)
    if not wait_opened(w):
        return
    u = 0
    while not stop.is_set() and w.alive() and u < UNITS:
        n_rows = TXNR if rng.random() < 0.25 else 1
        u += 1
        w.units[u] = n_rows
        deadline = time.time() + unit_timeout
        unit_where = f"w = '{wid}' AND u = {u}"
        if n_rows == 1:
            sql = [f"INSERT INTO t VALUES ('{wid}', {u}, 0);"]
            txn = False
        else:
            sql = [f"INSERT INTO t VALUES ('{wid}', {u}, {j});" for j in range(n_rows)]
            txn = True
        if drive_unit(w, wid, sql, txn, unit_where, deadline):
            w.acked.add(u)
        else:
            # A killed worker stops here (its attempted-but-unacked unit may
            # be present: legal). An alive worker that could not settle is a
            # busy abandon; the soak reports it.
            if w.alive() and not stop.is_set():
                w.abandoned = True
            break
    w.finished = u >= UNITS
    try:
        w.proc.stdin.close()
    except Exception:
        pass


def drive_reader(w, wid, stop, iters=20000):
    if not wait_opened(w):
        return
    for i in range(1, iters + 1):
        if stop.is_set() or not w.alive():
            break
        w.send("SELECT COUNT(*) FROM t;\n")
        w.send(f"SELECT 'R {wid} {i}' AS m;\n")
        time.sleep(0.002)
    try:
        w.proc.stdin.close()
    except Exception:
        pass


def drive_checkpointer(w, wid, stop, iters=4000):
    if not wait_opened(w):
        return
    for i in range(1, iters + 1):
        if stop.is_set() or not w.alive():
            break
        w.send("SELECT COUNT(*) FROM t;\n")
        w.send("checkpoint;\n")
        w.send(f"SELECT 'C {wid} {i}' AS m;\n")
        time.sleep(0.005)
    try:
        w.proc.stdin.close()
    except Exception:
        pass


# ---------------------------------------------------------------- sampling


def fd_count(pid):
    """Open fd count, or None when it cannot be sampled cheaply."""
    d = f"/proc/{pid}/fd"
    if os.path.isdir(d):
        try:
            return len(os.listdir(d))
        except OSError:
            return None
    try:
        out = subprocess.run(
            ["lsof", "-p", str(pid), "-Fn"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
        ).stdout
        return sum(1 for line in out.splitlines() if line.startswith(b"n"))
    except Exception:
        return None


def rss_kb(pid):
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
        ).stdout
        text = out.decode().strip()
        return int(text) if text else None
    except Exception:
        return None


def monitor(stop, workers_ref):
    while not stop.is_set():
        for w in list(workers_ref):
            if w.alive():
                rss = rss_kb(w.proc.pid)
                fds = fd_count(w.proc.pid)
                cpu = cpu_seconds(w.proc.pid)
                if rss is not None or fds is not None or cpu is not None:
                    w.samples.append((time.time(), rss, fds, cpu))
        stop.wait(2.0)


def cpu_seconds(pid):
    """Cumulative CPU seconds, or None. Distinguishes a spinning process from
    one blocked in the kernel (a kill that does not take effect)."""
    try:
        out = subprocess.run(
            ["ps", "-o", "time=", "-p", str(pid)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
        ).stdout
        text = out.decode().strip()
        if not text:
            return None
        parts = text.split(":")
        parts = [float(p) for p in parts]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        if len(parts) == 2:
            return parts[0] * 60 + parts[1]
        return parts[0]
    except Exception:
        return None


def scan_errors(w):
    """(busy count, non-busy lines) over the worker's whole output, skipping
    cleanup windows the driver masked (an expected `ROLLBACK` with no open
    transaction is not a run error)."""
    busy = 0
    other = []
    masked = set()
    for (start, end) in w.masked:
        masked.update(range(start, end))
    for i, line in enumerate(w.output().splitlines()):
        if i in masked or ERROR_MARK not in line:
            continue
        if is_busy(line):
            busy += 1
        else:
            other.append(line.strip())
    return busy, other


# ---------------------------------------------------------------- soak


def verify_final(binary, cwd, acked_by_wid, units_by_wid):
    """Fresh-process verification of exact final state; returns (ok, detail)."""
    writer_ids = sorted(units_by_wid)
    chunks = [writer_ids[i : i + 10] for i in range(0, len(writer_ids), 10)]

    def dump_rows():
        rows = set()
        for chunk in chunks:
            quoted = ", ".join(f"'{w}'" for w in chunk)
            out = cli(
                binary,
                f".open {DB}\nSELECT w, u, j FROM t WHERE w IN ({quoted}) "
                "ORDER BY w, u, j;\n.quit\n",
                cwd,
                timeout=300,
            )
            if "Opened" not in out:
                return None, out
            parsed = parse_three_cols(out)
            if not parsed and "no rows" not in out and "0 rows" not in out:
                return None, out
            rows.update(parsed)
        return rows, None

    rows, err_out = dump_rows()
    if rows is None:
        return False, f"dump failed:\n{(err_out or '')[-400:]}"
    rows2, _ = dump_rows()
    if rows != rows2:
        return False, "second fresh open changed the row set"

    agg = cli(
        binary,
        f".open {DB}\nSELECT COUNT(*) FROM t;\n"
        "SELECT COALESCE(SUM(u), 0) FROM t;\nSELECT COALESCE(SUM(j), 0) FROM t;\n.quit\n",
        cwd,
        timeout=300,
    )
    counts = [int(x) for x in re.findall(r"^\s*│\s*(\d+)\s*│\s*$", agg, re.M)]
    if len(counts) != 3:
        return False, f"could not parse aggregate results from:\n{agg[-400:]}"
    want = (len(rows), sum(u for _, u, _ in rows), sum(j for _, _, j in rows))
    if counts != list(want):
        return False, (
            f"aggregates {counts} do not match the dump {want} "
            "(rows outside the tracked writer ids?)"
        )

    by_wid = {}
    for wid, u, j in rows:
        by_wid.setdefault(wid, {}).setdefault(u, set()).add(j)

    for wid, units in units_by_wid.items():
        present = by_wid.get(wid, {})
        acked = acked_by_wid.get(wid, set())
        attempted = set(units)
        umax = max(present) if present else 0
        if set(present) != set(range(1, umax + 1)):
            gaps = sorted(set(range(1, umax + 1)) - set(present))[:5]
            return False, f"writer {wid}: gaps {gaps} in the durable prefix"
        unknown = set(present) - attempted
        if unknown:
            return False, (
                f"writer {wid}: rows from never-attempted units {sorted(unknown)[:5]}"
            )
        missing = acked - set(present)
        if missing:
            return False, f"writer {wid}: acknowledged units missing {sorted(missing)[:5]}"
        for u, js in present.items():
            want_rows = set(range(units[u]))
            if js != want_rows:
                return False, (
                    f"writer {wid} unit {u}: rows {sorted(js)} != {sorted(want_rows)} "
                    "(a committed unit must be atomic)"
                )
    return True, f"verified {len(rows)} rows across {len(by_wid)} writers"


def run_soak(args, binary):
    global ENV, _UNIT_WINDOW_S
    duration = args.duration
    cwd = os.path.join(WORK, "db")
    os.makedirs(cwd, exist_ok=True)
    _UNIT_WINDOW_S = max(60.0, args.busy_ms / 1000.0 + 30.0)
    base_env = dict(ENV if ENV is not None else os.environ)
    base_env["PINKDB_BUSY_TIMEOUT_MS"] = str(args.busy_ms)
    ENV = base_env

    # Bootstrap: create the table once and prove it is durable.
    out = cli(binary, f".open {DB}\nCREATE TABLE t (w VARCHAR, u BIGINT, j INTEGER);\n.quit\n", cwd)
    if "Error" in out or "Opened" not in out:
        print(f"bootstrap failed:\n{out}", file=sys.stderr)
        return False

    rng = random.Random(args.seed)
    workers_ref = []
    units_by_wid = {}
    acked_by_wid = {}
    counter = 0
    kills = 0
    kill_kinds = {}
    open_failures = []
    open_notes = []
    stop = threading.Event()

    def spawn_writer():
        nonlocal counter
        counter += 1
        wid = f"w{counter}"
        w = spawn(binary, "writer", wid, cwd)
        units_by_wid[wid] = w.units
        acked_by_wid[wid] = w.acked
        t = threading.Thread(
            target=drive_writer,
            args=(w, wid, rng.randrange(1 << 30), stop, args.unit_timeout),
            daemon=True,
        )
        t.start()
        w.driver = t
        workers_ref.append(w)
        return w

    def spawn_reader():
        nonlocal counter
        counter += 1
        wid = f"r{counter}"
        w = spawn(binary, "reader", wid, cwd)
        t = threading.Thread(target=drive_reader, args=(w, wid, stop), daemon=True)
        t.start()
        w.driver = t
        workers_ref.append(w)
        return w

    def spawn_checkpointer():
        nonlocal counter
        counter += 1
        wid = f"c{counter}"
        w = spawn(binary, "checkpointer", wid, cwd)
        t = threading.Thread(target=drive_checkpointer, args=(w, wid, stop), daemon=True)
        t.start()
        w.driver = t
        workers_ref.append(w)
        return w

    def maintain():
        live = []
        for w in workers_ref:
            if w.alive():
                live.append(w)
            elif w.died_at is None:
                w.died_at = time.time()
        for kind, target, spawner in (
            ("writer", args.workers, spawn_writer),
            ("reader", args.readers, spawn_reader),
            ("checkpointer", args.checkpointers, spawn_checkpointer),
        ):
            n = sum(1 for w in live if w.kind == kind)
            for _ in range(max(0, target - n)):
                spawner()

    maintain()

    mon = threading.Thread(target=monitor, args=(stop, workers_ref), daemon=True)
    mon.start()

    start = time.time()
    open_closes = 0
    open_close_errors = []
    next_open_close = start + 1.0
    next_kill = start + rng.uniform(*args.kill_every)

    busy_open = [0]

    def open_close_once():
        out = cli(
            binary,
            f".open {DB}\nSELECT COUNT(*) FROM t;\n.quit\n",
            cwd,
            timeout=max(60.0, args.busy_ms / 1000.0 + 30.0),
        )
        err_lines = [l for l in out.splitlines() if ERROR_MARK in l]
        bad = [l for l in err_lines if not is_busy(l)]
        if "Opened" in out:
            if bad:
                open_close_errors.append(f"open-close: {out.strip()[-260:]}")
            return
        if err_lines and is_busy(err_lines[0]):
            # The documented busy outcome: the one-shot CLI waited out its
            # timeout for the lease. Its follow-up SELECT cascades
            # `table not found` because no database is open — not a
            # second failure.
            busy_open[0] += 1
            return
        open_close_errors.append(f"open-close: {out.strip()[-260:]}")

    try:
        while time.time() - start < duration:
            now = time.time()
            # Occasional short-lived open+count+quit processes (background:
            # a busy one-shot CLI must not stall the kill loop).
            if args.open_close and now >= next_open_close:
                open_closes += 1
                threading.Thread(target=open_close_once, daemon=True).start()
                next_open_close = now + rng.uniform(1.0, 3.0)
            # Random kill of a random live worker.
            if now >= next_kill:
                live = [w for w in workers_ref if w.alive()]
                writers_live = [w for w in live if w.kind == "writer"]
                if writers_live and (len(writers_live) >= 2 or rng.random() < 0.5):
                    victim = rng.choice(writers_live)
                elif live:
                    victim = rng.choice(live)
                else:
                    victim = None
                if victim is not None:
                    victim.kill()
                    kills += 1
                    kill_kinds[victim.kind] = kill_kinds.get(victim.kind, 0) + 1
                next_kill = now + rng.uniform(*args.kill_every)
            maintain()
            stop.wait(0.2)
    finally:
        stop.set()

    # Stop everything (kill, so no new acks are printed after the dump).
    for w in workers_ref:
        if w.alive():
            w.kill()
        if hasattr(w, "driver"):
            w.driver.join(timeout=5)
        w.reader.join(timeout=5)

    elapsed = time.time() - start

    # Aggregate per-worker results (after all readers have joined, so no
    # line is missed). Writer acks are the harness-observed sets, never
    # re-derived from output text.
    busy_errors = 0
    other_errors = []
    error_kinds = {}
    for w in workers_ref:
        b, other = scan_errors(w)
        busy_errors += b
        for e in other[:20]:
            other_errors.append(f"{w.kind} {w.wid}: {e[:200]}")
        for line in w.output().splitlines():
            if ERROR_MARK in line:
                kind = line.split(ERROR_MARK, 1)[-1].strip()[:90]
                error_kinds[kind] = error_kinds.get(kind, 0) + 1
        if w.open_failed:
            open_failures.append(w.open_failed)
        if w.kind == "writer":
            acked_by_wid[w.wid] = set(w.acked)

    # Leak checks (fd/RSS over each worker's lifetime).
    leak_fail = []
    fd_reports = []
    for w in workers_ref:
        samples = [(t, rss, fds) for (t, rss, fds, _cpu) in w.samples if fds is not None]
        if len(samples) >= 3:
            first = samples[0]
            last = samples[-1]
            lifetime = last[0] - first[0]
            fd_delta = last[2] - first[2]
            max_fd = max(s[2] for s in samples)
            fd_reports.append((w.kind, w.wid, first[2], last[2], max_fd))
            if max_fd > 64 or (lifetime >= 10 and fd_delta > 8):
                leak_fail.append(f"{w.kind} {w.wid}: fds {first[2]}..{max_fd} (delta {fd_delta})")
        rs = [(t, rss) for (t, rss, _fds, _cpu) in w.samples if rss is not None]
        if len(rs) >= 3:
            lifetime = rs[-1][0] - rs[0][0]
            rss_delta = rs[-1][1] - rs[0][1]
            max_rss = max(r for _, r in rs)
            if max_rss > 512 * 1024 or (lifetime >= 30 and rss_delta > 128 * 1024):
                leak_fail.append(
                    f"{w.kind} {w.wid}: rss {rs[0][1]}..{max_rss} KB (delta {rss_delta})"
                )

    ok, detail = verify_final(binary, cwd, acked_by_wid, units_by_wid)
    total_acked = sum(len(a) for a in acked_by_wid.values())
    spawned = {
        k: sum(1 for w in workers_ref if w.kind == k)
        for k in ("writer", "reader", "checkpointer")
    }
    abandoned = sum(1 for w in workers_ref if w.kind == "writer" and w.abandoned)
    kill_stuck = sum(w.kill_stuck for w in workers_ref)
    no_progress = []
    for w in workers_ref:
        if w.kind == "writer" and not w.units:
            cpu = next(
                (c for (_, _, _, c) in reversed(w.samples) if c is not None), None
            )
            life = (w.died_at or time.time()) - w.spawned
            no_progress.append((w.wid, round(life, 1), cpu))
    no_progress.sort(key=lambda t: -t[1])
    print(
        f"=== soak: {elapsed:.1f}s, writers={spawned['writer']} (live target {args.workers}), "
        f"readers={spawned['reader']}, checkpointers={spawned['checkpointer']}, "
        f"kills={kills} {kill_kinds}, open-close cycles={open_closes} "
        f"(busy opens={busy_open[0]}), kill-stuck={kill_stuck}"
    )
    print(
        f"acked units={total_acked}, busy errors={busy_errors}, "
        f"other errors={len(other_errors)}, failed opens={len(open_failures)}, "
        f"busy-abandoned writers={abandoned}"
    )
    if no_progress:
        print(
            f"writers with no attempted unit: {len(no_progress)} "
            f"(wid, age_at_death_s, cpu_s): {no_progress[:6]}"
        )
    if error_kinds:
        print("error kinds (deduped):")
        for kind, n in sorted(error_kinds.items(), key=lambda kv: -kv[1])[:12]:
            print(f"  {n:5d}  {kind}")
    if open_failures:
        print("failed opens:")
        for e in open_failures[:10]:
            print(f"  {e}")
    if open_notes:
        print(f"open retries observed: {len(open_notes)}")
        for e in open_notes[:5]:
            print(f"  {e}")
    if fd_reports:
        max_delta = max(r[3] - r[2] for r in fd_reports)
        print(
            f"fd samples: {len(fd_reports)} workers, max observed fd={max(r[4] for r in fd_reports)}, "
            f"max delta={max_delta}"
        )
    if other_errors:
        print("non-busy errors:")
        for e in other_errors[:10]:
            print(f"  {e}")
    if open_close_errors:
        print("open-close errors:")
        for e in open_close_errors[:5]:
            print(f"  {e}")
    print(f"final-state verification: {'OK' if ok else 'FAIL'}: {detail}")
    if leak_fail:
        print("leak check FAILED:")
        for e in leak_fail[:10]:
            print(f"  {e}")
    if kills < args.min_kills:
        print(f"FAIL: only {kills} kills, wanted >= {args.min_kills}")
    passed = (
        ok
        and not leak_fail
        and not other_errors
        and not open_failures
        and not open_close_errors
        and kills >= args.min_kills
    )
    if not passed:
        # Keep per-worker evidence: full outputs and the ack/attempt tables.
        try:
            ev = os.path.join(WORK, "evidence")
            os.makedirs(ev, exist_ok=True)
            for w in workers_ref:
                with open(os.path.join(ev, f"{w.kind}-{w.wid}.log"), "w") as f:
                    f.write(w.output())
                with open(os.path.join(ev, f"{w.kind}-{w.wid}.state"), "w") as f:
                    f.write(
                        f"alive={w.alive()} abandoned={w.abandoned} finished={w.finished}\n"
                        f"open_failed={w.open_failed}\n"
                        f"acked={sorted(w.acked)}\n"
                        f"attempted={ {u: n for u, n in sorted(w.units.items())} }\n"
                        f"elapsed={time.time() - w.spawned:.1f}\n"
                    )
            print(f"evidence kept under {WORK}")
        except Exception as e:  # evidence is best-effort
            print(f"could not write evidence: {e} (dir {WORK})")
    return passed


# ---------------------------------------------------------------- contention


def run_contention(args, binary, n):
    global ENV, _UNIT_WINDOW_S
    cwd = os.path.join(WORK, "hammer")
    os.makedirs(cwd, exist_ok=True)
    out = cli(binary, f".open {DB}\nCREATE TABLE t (w INTEGER, u BIGINT);\n.quit\n", cwd)
    if "Error" in out or "Opened" not in out:
        print(f"bootstrap failed:\n{out}", file=sys.stderr)
        return False
    base_env = ENV if ENV is not None else os.environ.copy()
    env = dict(base_env)
    env["PINKDB_BUSY_TIMEOUT_MS"] = str(args.contention_busy_ms)
    ENV = env
    _UNIT_WINDOW_S = max(30.0, args.contention_busy_ms / 1000.0 + 15.0)

    stop = threading.Event()
    workers = []
    threads = []
    for i in range(n):
        wid = f"h{i}"
        w = spawn(binary, "hammer", wid, cwd)
        workers.append(w)
        t = threading.Thread(
            target=drive_contention_writer,
            args=(w, wid, stop, args),
            daemon=True,
        )
        t.start()
        threads.append(t)

    t0 = time.time()
    deadline = t0 + args.contention_deadline
    while time.time() < deadline and any(t.is_alive() for t in threads):
        time.sleep(0.2)
    elapsed = time.time() - t0
    timed_out = any(t.is_alive() for t in threads)
    stop.set()
    for w in workers:
        if w.alive():
            w.kill()
    for t in threads:
        t.join(timeout=5)

    acked = {w.wid: set(w.acked) for w in workers}
    busy = 0
    other = []
    for w in workers:
        b, errs = scan_errors(w)
        busy += b
        for e in errs[:10]:
            other.append(f"{w.wid}: {e[:200]}")
        if w.open_failed:
            other.append(w.open_failed)

    # Fresh verification: exactly-once acked state and total counts.
    dump = cli(binary, f".open {DB}\nSELECT w, u FROM t;\n.quit\n", cwd, timeout=300)
    counts = {}
    rows = 0
    for wid_i, u in parse_two_cols(dump):
        wid = f"h{wid_i}"
        counts.setdefault(wid, []).append(u)
        rows += 1
    problems = []
    for wid, got in acked.items():
        present = counts.get(wid, [])
        want = set()
        for u in got:
            if args.contention_txns and u % 4 == 0:
                want.add(1000000 + u)
                want.add(2000000 + u)
            else:
                want.add(u)
        missing = want - set(present)
        if missing:
            problems.append(f"{wid}: missing {sorted(missing)[:5]}")
        if len(present) != len(set(present)):
            problems.append(f"{wid}: duplicate rows")
    print(
        f"=== contention: {n} writers x {args.contention_units} units "
        f"({'mixed txns' if args.contention_txns else 'autocommit'}), "
        f"busy timeout {args.contention_busy_ms} ms"
    )
    times = [len(w.acked) for w in workers]
    print(
        f"wall time={elapsed:.2f}s, rows={rows}, acked units={sum(len(a) for a in acked.values())}, "
        f"busy errors={busy}, other errors={len(other)}, "
        f"per-writer acked min/median/max={min(times) if times else 0}/"
        f"{sorted(times)[len(times) // 2] if times else 0}/{max(times) if times else 0}"
    )
    for e in other[:10]:
        print(f"  {e}")
    if problems:
        print("FAIL:")
        for p in problems[:10]:
            print(f"  {p}")
    if timed_out:
        print("FAIL: writers still running at the deadline (livelock/deadlock?)")
    passed = not problems and not other and not timed_out
    if not passed:
        print(f"evidence kept under {WORK}")
    return passed


def drive_contention_writer(w, wid, stop, args):
    """The soak's ack protocol on the contention row shapes: unit `u` is one
    row `(i, u)`, or (with `--contention-txns`, every 4th unit) a two-row
    transaction inserting `(i, 1000000+u)` and `(i, 2000000+u)`."""
    i = wid[1:]
    if not wait_opened(w):
        return
    for u in range(1, args.contention_units + 1):
        if stop.is_set() or not w.alive():
            return
        txn = args.contention_txns and u % 4 == 0
        if txn:
            w.units[u] = 2
            sql = [
                f"INSERT INTO t VALUES ({i}, {1000000 + u});",
                f"INSERT INTO t VALUES ({i}, {2000000 + u});",
            ]
        else:
            w.units[u] = 1
            sql = [f"INSERT INTO t VALUES ({i}, {u});"]
        deadline = time.time() + args.contention_deadline
        unit_where = f"w = {i} AND u = {u}"
        if not drive_unit(w, wid, sql, txn, unit_where, deadline):
            if w.alive() and not stop.is_set():
                w.abandoned = True
            return
        w.acked.add(u)


# ---------------------------------------------------------------- main


def main():
    global ENV, ENCRYPTED
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", default=DEFAULT_BIN)
    ap.add_argument("--duration", type=float, default=300.0, help="soak duration seconds")
    ap.add_argument("--workers", type=int, default=8, help="target live writers")
    ap.add_argument("--readers", type=int, default=2)
    ap.add_argument("--checkpointers", type=int, default=1)
    ap.add_argument("--min-kills", type=int, default=40)
    ap.add_argument("--busy-ms", type=int, default=30000,
                    help="worker busy timeout (PINKDB_BUSY_TIMEOUT_MS)")
    ap.add_argument("--unit-timeout", type=float, default=180.0,
                    help="seconds before a writer abandons the current unit")
    ap.add_argument("--kill-every", type=float, nargs=2, default=(0.5, 2.5),
                    metavar=("MIN", "MAX"), help="random delay between kills")
    ap.add_argument("--no-open-close", dest="open_close", action="store_false")
    ap.add_argument("--ci", action="store_true",
                    help="short CI-friendly run: 15 s, 6 writers, >=10 kills")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--encrypted", action="store_true",
                    help="run with PINKDB_KEY from the environment")
    ap.add_argument("--contention", type=int, default=0, metavar="N",
                    help="contention mode: N simultaneous writers")
    ap.add_argument("--contention-units", type=int, default=30)
    ap.add_argument("--contention-txns", action="store_true",
                    help="every 4th unit is a BEGIN..COMMIT of two rows")
    ap.add_argument("--contention-busy-ms", type=int, default=10000)
    ap.add_argument("--contention-deadline", type=float, default=180.0,
                    help="seconds before the hammer is declared stuck")
    args = ap.parse_args()

    if args.encrypted:
        passphrase = os.environ.get("PINKDB_KEY", "")
        if not passphrase:
            print("--encrypted requires PINKDB_KEY in the environment", file=sys.stderr)
            return 2
        ENV = dict(os.environ)
        ENCRYPTED = True
        print(f"encrypted mode: PINKDB_KEY from the environment ({len(passphrase)} chars)")
    if not os.path.exists(args.bin):
        print(f"missing binary: {args.bin}", file=sys.stderr)
        return 2
    if args.ci:
        args.duration = 15.0
        args.workers = 6
        args.min_kills = 10
        args.kill_every = (0.3, 1.0)
        print("CI mode: duration=15s workers=6 min_kills=10 kill_every=0.3..1.0")

    if args.contention:
        ok = run_contention(args, args.bin, args.contention)
    else:
        ok = run_soak(args, args.bin)
    if ok:
        shutil.rmtree(WORK, ignore_errors=True)
        print("PASS")
    else:
        print(f"FAIL (evidence under {WORK})")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
