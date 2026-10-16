#!/usr/bin/env python3
"""Disjoint-table multi-process contention benchmark (wave 45o).

Runs the *same* workload against a pinkdb binary in two orders:

  serial      -- the processes run one after another (same total work);
  concurrent  -- all processes at once, each on its own tables.

With the pre-45o lease (an explicit transaction holds the write lease from
BEGIN to COMMIT) the concurrent order degenerates to the serial one: the
single-process wall time is unchanged when transactions overlap, because they
queue on the lease. With optimistic commits the transaction bodies overlap and
only the (short) commit spans serialize, so concurrent wall time drops.

Workloads (tables are per-process, so every touched set is disjoint):
  txn-read    -- BEGIN; `--reads-per-txn` heavy reads over the shared `r`
                 table; INSERT INTO tP; COMMIT.  The lease-relevant span (the
                 reads) overlaps; each frame is one tiny insert. This is the
                 workload the old model serialized completely.
  txn-write   -- BEGIN; UPDATE tP (all `--rows` rows); COMMIT. Execution is
                 the long pole, but every frame is a heavy UPDATE that each
                 process must also replay for the others, so wall time stays
                 close to the serial one (reported honestly).
  autocommit  -- the same UPDATE without a transaction.

Each mode is the best of `--runs` repetitions (machine noise), and both modes
run inside one invocation so the comparison is paired. `verify=ok` re-opens
the database in a fresh process and checks every table's count and sum.

Usage:
  python3 bench/mp_optimistic.py --bin target/release/pinkdb \
      --procs 4 --runs 2 [--workload txn-read] \
      [--txns 1] [--reads-per-txn 40] [--read-rows 200000] [--rows 1000]
"""

import argparse
import os
import shutil
import subprocess
import tempfile
import threading
import time

HEAVY_READ = "SELECT COUNT(*) FROM r AS x JOIN r AS y ON x.a = y.a + 1;"


class Runner:
    """One CLI process with its stdout drained by a reader thread (a full
    pipe would block the child and corrupt the wall time)."""

    def __init__(self, binpath, dirpath, proc):
        self.proc = subprocess.Popen(
            [binpath],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=dirpath,
            env=bench_env(),
        )
        self.lines = []
        self.opened = threading.Event()
        self.done = threading.Event()
        self.proc_id = proc
        threading.Thread(target=self._drain, daemon=True).start()

    def _drain(self):
        for line in self.proc.stdout:
            self.lines.append(line)
            if "Opened" in line:
                self.opened.set()
            if f"done-{self.proc_id}" in line:
                self.done.set()

    def open(self):
        self.proc.stdin.write(".open db.pink\n")
        self.proc.stdin.flush()
        if not self.opened.wait(timeout=300):
            raise SystemExit(f"process {self.proc_id} never opened:\n{''.join(self.lines)}")

    def run(self, body):
        self.proc.stdin.write(body)
        self.proc.stdin.flush()

    def finish(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=3600)
        if self.proc.returncode != 0:
            raise SystemExit(
                f"process {self.proc_id} failed (rc={self.proc.returncode}):\n"
                f"{''.join(self.lines)}"
            )


def bench_env():
    env = dict(os.environ)
    # The pre-45o baseline serializes concurrent transactions by *waiting*
    # for the lease; a long timeout keeps it from erroring out.
    env["PINKDB_BUSY_TIMEOUT_MS"] = "600000"
    return env


def write_csv(dirpath, rows, name, header="a"):
    path = os.path.join(dirpath, name)
    with open(path, "w") as f:
        f.write(header + "\n")
        for i in range(rows):
            f.write(f"{i}\n")
    return path


def setup_database(binpath, dirpath, procs, rows, write_csv_path, read_csv_path):
    script = [".open db.pink"]
    # One shared read table (heavy scans) plus one write table per process
    # (`.import` creates each table from its CSV header).
    script.append(f".import {read_csv_path} r")
    for p in range(procs):
        script.append(f".import {write_csv_path} t{p}")
    script.append("SELECT 'setup-ok' AS m;")
    proc = subprocess.run(
        [binpath],
        input="\n".join(script) + "\n",
        capture_output=True,
        text=True,
        cwd=dirpath,
        env=bench_env(),
    )
    if "setup-ok" not in proc.stdout or proc.returncode != 0:
        raise SystemExit(f"setup failed:\n{proc.stdout}\n{proc.stderr}")


def workload_sql(proc, txns, workload, reads, read_sql):
    stmts = []
    read = read_sql if read_sql else HEAVY_READ
    write = f"UPDATE t{proc} SET a = a + 1 WHERE a >= 0;"
    if workload == "txn-read":
        for _ in range(txns):
            body = " ".join([read] * reads)
            stmts.append(f"BEGIN; {body} INSERT INTO t{proc} VALUES (-1); COMMIT;")
    elif workload == "txn-write":
        for _ in range(txns):
            stmts.append(f"BEGIN; {write} COMMIT;")
    else:
        for _ in range(txns):
            stmts.append(write)
    stmts.append(f"SELECT 'done-{proc}' AS m;")
    return "\n".join(stmts) + "\n"


def run_mode_once(binpath, dirpath, procs, txns, workload, mode, reads, read_sql):
    runners = [Runner(binpath, dirpath, p) for p in range(procs)]
    for r in runners:
        r.open()
    t0 = time.monotonic()
    if mode == "serial":
        for p, r in enumerate(runners):
            r.run(workload_sql(p, txns, workload, reads, read_sql))
            if not r.done.wait(timeout=3600):
                raise SystemExit(f"process {p} never finished:\n{''.join(r.lines)}")
            r.finish()
    else:
        for p, r in enumerate(runners):
            r.run(workload_sql(p, txns, workload, reads, read_sql))
        for p, r in enumerate(runners):
            if not r.done.wait(timeout=3600):
                raise SystemExit(f"process {p} never finished:\n{''.join(r.lines)}")
        for r in runners:
            r.finish()
    return time.monotonic() - t0


def run_mode(binpath, dirpath, procs, txns, workload, mode, reads, read_sql, runs, restore):
    best = None
    for i in range(runs):
        if i > 0:
            # Every repetition starts from the same committed state.
            restore()
        wall = run_mode_once(binpath, dirpath, procs, txns, workload, mode, reads, read_sql)
        best = wall if best is None else min(best, wall)
    print(f"mode={mode} workload={workload} procs={procs} txns={txns} wall_s={best:.3f}")
    return best


def verify(binpath, dirpath, procs, rows, txns, workload):
    if workload == "txn-read":
        expected_count = rows + txns
        expected_sum = rows * (rows - 1) // 2 - txns
    else:
        expected_count = rows
        expected_sum = rows * (rows - 1) // 2 + txns * rows
    script = [".open db.pink"]
    for p in range(procs):
        script.append(
            f"SELECT 'count-{p}' AS m, COUNT(*) AS c FROM t{p} "
            f"HAVING COUNT(*) = {expected_count};"
        )
        script.append(f"SELECT 'sum-{p}' AS m FROM t{p} HAVING SUM(a) = {expected_sum};")
    proc = subprocess.run(
        [binpath],
        input="\n".join(script) + "\n",
        capture_output=True,
        text=True,
        cwd=dirpath,
        env=bench_env(),
    )
    for p in range(procs):
        if f"count-{p}" not in proc.stdout or f"sum-{p}" not in proc.stdout:
            raise SystemExit(
                f"verification failed for t{p}:\n{proc.stdout[-2000:]}\n{proc.stderr[-2000:]}"
            )
    print("verify=ok")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--procs", type=int, default=4)
    ap.add_argument("--txns", type=int, default=1)
    ap.add_argument("--runs", type=int, default=2, help="best-of-N per mode")
    ap.add_argument("--rows", type=int, default=1000, help="write-table rows")
    ap.add_argument("--read-rows", type=int, default=200_000)
    ap.add_argument("--reads-per-txn", type=int, default=40)
    ap.add_argument(
        "--read-sql",
        default=HEAVY_READ,
        help="the heavy read statement (must not write anything)",
    )
    ap.add_argument(
        "--workload", choices=["txn-read", "txn-write", "autocommit"], default="txn-read"
    )
    ap.add_argument("--serial-only", action="store_true")
    ap.add_argument("--concurrent-only", action="store_true")
    args = ap.parse_args()

    binpath = os.path.abspath(args.bin)
    dirpath = tempfile.mkdtemp(prefix="pinkdb_mp_opt_")
    try:
        write_csv_path = write_csv(dirpath, args.rows, "rows.csv")
        read_csv_path = write_csv(dirpath, args.read_rows, "read.csv")
        setup_database(
            binpath, dirpath, args.procs, args.rows, write_csv_path, read_csv_path
        )
        # Each mode starts from the same committed state.
        base_ckpt = os.path.join(dirpath, "db.pink")
        base_wal = base_ckpt + ".wal"
        base_state = (open(base_ckpt, "rb").read(), open(base_wal, "rb").read())

        def restore():
            open(base_ckpt, "wb").write(base_state[0])
            open(base_wal, "wb").write(base_state[1])

        serial = concurrent = None
        if not args.concurrent_only:
            serial = run_mode(
                binpath, dirpath, args.procs, args.txns, args.workload, "serial",
                args.reads_per_txn, args.read_sql, args.runs, restore,
            )
            verify(binpath, dirpath, args.procs, args.rows, args.txns, args.workload)
            restore()
        if not args.serial_only:
            concurrent = run_mode(
                binpath, dirpath, args.procs, args.txns, args.workload, "concurrent",
                args.reads_per_txn, args.read_sql, args.runs, restore,
            )
            verify(binpath, dirpath, args.procs, args.rows, args.txns, args.workload)
        if serial is not None and concurrent is not None:
            print(f"speedup={serial / concurrent:.2f}x")
    finally:
        shutil.rmtree(dirpath, ignore_errors=True)


if __name__ == "__main__":
    main()
