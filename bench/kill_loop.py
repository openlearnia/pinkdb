#!/usr/bin/env python3
"""Crash-durability stress for pinkdb's WAL.

Random mode: run a REPL script with random `.save` checkpoints, SIGKILL it at
a random moment, then verify with repeated clean opens that every row whose
ack-marker was printed before the kill is present exactly once and that later
opens see the same rows (no delayed loss, no duplication).

Multi-db mode (`--multi-db`): the same loop over `db1` + `USE` + a cross-db
transaction — rows land in `main.t` and `db1.t`, and both must survive the
kill exactly once.

Targeted mode: kill exactly inside the checkpoint publish window (after the
WAL roll, before the `.pink` rename) and verify the rows across repeated
opens — the crash image that used to be "replayed into memory then deleted".

Encrypted mode (`--encrypted`): the same random/multi-db/targeted loops with
`PINKDB_KEY` set, so the checkpoint and the sidecar WAL are encrypted
containers. The binary must be built with the encryption feature
(`cargo build -p pinkdb-cli --features encryption`). The passphrase is read
from the environment (`PINKDB_KEY=... python3 bench/kill_loop.py --encrypted`),
never from the command line, so it stays out of the process table.

Usage:
  python3 bench/kill_loop.py [ROUNDS] [--seed N] [--bin PATH]
  python3 bench/kill_loop.py [ROUNDS] --multi-db [--seed N] [--bin PATH]
  python3 bench/kill_loop.py [ROUNDS] --extension PATH [--seed N] [--bin PATH]
  python3 bench/kill_loop.py --targeted N [--seed N] [--bin PATH]
  PINKDB_KEY=pass python3 bench/kill_loop.py [ROUNDS] --encrypted [--bin PATH]

Extension mode (`--extension`): random mode only. Every iteration loads the
extension after `.open` and computes every inserted VARCHAR value with one of
its functions, so the WAL must carry `LOAD EXTENSION` prefixes across random
`.save` checkpoints (the touched-only prefixing) for replay to succeed.
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
WORK = tempfile.mkdtemp(prefix="pinkdb_killloop_")

ACK_RE = re.compile(r"(ACK|TXN) (\d+)")
ROW_RE = re.compile(r"^│\s*(-?\d+)\s*│", re.M)

# Subprocess environment: None = inherit. `--encrypted` passes a copy with the
# PINKDB_KEY taken from the environment (the passphrase never appears on a
# command line).
ENV = None


def cli(binary, cmds, cwd):
    p = subprocess.run(
        [binary], input=cmds.encode(), capture_output=True, cwd=cwd, env=ENV
    )
    return p.stdout.decode(errors="replace")


def fresh(name):
    d = os.path.join(WORK, name)
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    return d


def open_rows(binary, cwd, db="x.pink"):
    """(opened, rows, out): rows is None when the table does not exist yet."""
    out = cli(binary, f".open {db}\nSELECT a FROM t ORDER BY a;\n.quit\n", cwd)
    if "Opened" not in out:
        return False, None, out
    if "Error" in out:
        return True, None, out
    return True, [int(x) for x in ROW_RE.findall(out)], out


def verify(binary, cwd, expected, opens=3):
    """Returns (kind, detail); kind is ok | open_fail | missing | dup | drift."""
    opened, first, out = open_rows(binary, cwd)
    if not opened:
        return "open_fail", f"first reopen failed: {out[:160]!r}"
    if first is None:
        if not expected:
            return "ok", "no commits before kill (table never created)"
        return "missing", f"table missing after ack: {out[:160]!r}"
    missing = sorted(set(expected) - set(first))
    if missing:
        return "missing", f"lost committed rows {missing} (present={sorted(first)})"
    if len(first) != len(set(first)):
        dups = sorted(x for x in set(first) if first.count(x) > 1)
        return "dup", f"duplicate rows {dups}"
    for i in range(1, opens):
        opened, again, out = open_rows(binary, cwd)
        if not opened:
            return "drift", f"open #{i + 1} failed after a successful open"
        again = again if again is not None else []
        if again != first:
            return "drift", f"open #{i + 1} changed rows: {len(first)} -> {len(again)}"
    return "ok", f"rows={len(first)}"


def run_and_kill(binary, cmds, cwd, delay):
    """Feed cmds, sleep delay, SIGKILL, return all stdout."""
    p = subprocess.Popen(
        [binary], stdin=subprocess.PIPE, stdout=subprocess.PIPE, cwd=cwd, bufsize=0,
        env=ENV,
    )
    chunks = []

    def reader():
        try:
            while True:
                b = p.stdout.read(65536)
                if not b:
                    break
                chunks.append(b)
        except Exception:
            pass

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    try:
        p.stdin.write(cmds.encode())
        p.stdin.flush()
    except Exception:
        pass
    time.sleep(delay)
    try:
        p.kill()
    except ProcessLookupError:
        pass
    p.wait()
    t.join(timeout=5)
    return b"".join(chunks).decode(errors="replace")


def acked_ids(out, units):
    seen = set(ACK_RE.findall(out))
    acked = set()
    for marker, ids in units:
        if tuple(marker.split()) in seen:
            acked.update(ids)
    return acked


def random_script(rng, extension=None):
    units = []
    nxt = 0
    for _ in range(rng.randint(4, 16)):
        if rng.random() < 0.35:
            ids = [nxt, nxt + 1, nxt + 2]
            units.append((f"TXN {ids[0]}", ids))
            nxt += 3
        else:
            units.append((f"ACK {nxt}", [nxt]))
            nxt += 1
    lines = ["CREATE TABLE t (a INTEGER, v VARCHAR);"]
    if extension:
        # Load a native extension and make every insert call one of its
        # functions: a checkpoint rolls the LOAD record away, so the
        # post-checkpoint records must be `LOAD EXTENSION`-prefixed for
        # replay (the C1 mechanism). The verifier only reads column `a`.
        lines.insert(0, f"LOAD EXTENSION '{extension}';")
    save_every = rng.choice([0, 3, 5, 8])
    for i, (marker, ids) in enumerate(units):
        if marker.startswith("TXN"):
            lines.append("BEGIN;")
            for rid in ids:
                lines.append(insert_line(rid, extension))
            lines.append("COMMIT;")
        else:
            lines.append(insert_line(ids[0], extension))
        lines.append(f"SELECT '{marker}' AS m;")
        if save_every and i % save_every == save_every - 1:
            lines.append(".save x.pink")
    return units, "\n".join(lines) + "\n"


def insert_line(rid, extension):
    """One insert; with `extension`, the VARCHAR value comes from ext_shout."""
    v = f"ext_shout('v{rid}')" if extension else f"'v{rid}'"
    return f"INSERT INTO t VALUES ({rid}, {v});"


def run_random(binary, rounds, seed, extension=None):
    rng = random.Random(seed)
    kinds = {}
    for i in range(rounds):
        units, cmds = random_script(rng, extension)
        d = fresh(f"q{i}")
        out = run_and_kill(binary, ".open x.pink\n" + cmds, d, rng.uniform(0.001, 0.05))
        acked = acked_ids(out, units)
        kind, detail = verify(binary, d, acked)
        kinds[kind] = kinds.get(kind, 0) + 1
        if kind != "ok":
            keep = os.path.join(WORK, f"keep_{i}")
            shutil.rmtree(keep, ignore_errors=True)
            shutil.copytree(d, keep)
            print(f"FAIL iter {i}: {kind}: {detail}\n  kept: {keep}")
        elif i % 50 == 0:
            print(f"iter {i}: ok ({detail})")
        shutil.rmtree(d, ignore_errors=True)
    print(f"=== random kill loop: {rounds} iterations, kinds={kinds} ===")
    return kinds.get("ok", 0) == rounds


# ---------- multi-database mode ----------

def random_script_multi(rng):
    """db1 + `USE` + a cross-db transaction, with random `.save`s.

    Markers: `ACK n` = autocommit insert into `main.t`, `USE n` = insert into
    `db1.t` via `USE`, `TXN n` = one transaction writing both tables. The
    verifier maps each acked marker back to the table(s) it wrote.
    """
    units = []
    nxt = 0
    for _ in range(rng.randint(3, 10)):
        r = rng.random()
        if r < 0.3:
            ids = [nxt, nxt + 1]
            units.append((f"TXN {ids[0]}", ids))
            nxt += 2
        elif r < 0.6:
            units.append((f"USE {nxt}", [nxt]))
            nxt += 1
        else:
            units.append((f"ACK {nxt}", [nxt]))
            nxt += 1
    lines = [
        "CREATE DATABASE db1;",
        "CREATE TABLE main.t (a INTEGER, v VARCHAR);",
        "CREATE TABLE db1.t (a INTEGER, v VARCHAR);",
    ]
    save_every = rng.choice([0, 3, 5])
    for i, (marker, ids) in enumerate(units):
        kind = marker.split()[0]
        if kind == "TXN":
            lines.append("BEGIN;")
            for rid in ids:
                lines.append(f"INSERT INTO main.t VALUES ({rid}, 'm{rid}');")
                lines.append(f"INSERT INTO db1.t VALUES ({rid}, 'd{rid}');")
            lines.append("COMMIT;")
        elif kind == "USE":
            rid = ids[0]
            lines.append("USE db1;")
            lines.append(f"INSERT INTO t VALUES ({rid}, 'd{rid}');")
            lines.append("USE main;")
        else:
            rid = ids[0]
            lines.append(f"INSERT INTO main.t VALUES ({rid}, 'm{rid}');")
        lines.append(f"SELECT '{marker}' AS m;")
        if save_every and i % save_every == save_every - 1:
            lines.append(".save x.pink")
    return units, "\n".join(lines) + "\n"


def acked_rows_multi(out, units):
    """(main_ids, db1_ids) whose ack-marker was printed before the kill."""
    seen = set(ACK_RE.findall(out))
    main, db1 = set(), set()
    for marker, ids in units:
        if tuple(marker.split()) not in seen:
            continue
        kind = marker.split()[0]
        if kind == "ACK":
            main.update(ids)
        elif kind == "USE":
            db1.update(ids)
        else:  # TXN writes both tables
            main.update(ids)
            db1.update(ids)
    return main, db1


def open_rows_in(binary, cwd, table):
    """(opened, rows, out) for `table` (qualified); rows None when missing."""
    out = cli(binary, f".open x.pink\nSELECT a FROM {table} ORDER BY a;\n.quit\n", cwd)
    if "Opened" not in out:
        return False, None, out
    if "Error" in out:
        return True, None, out
    return True, [int(x) for x in ROW_RE.findall(out)], out


def verify_multi(binary, cwd, expected_main, expected_db1, opens=3):
    """Returns (kind, detail) for both tables; kind is ok | open_fail |
    missing | dup | drift."""
    expected = {"main.t": expected_main, "db1.t": expected_db1}
    first = {}
    for table, want in expected.items():
        opened, rows, out = open_rows_in(binary, cwd, table)
        if not opened:
            return "open_fail", f"first reopen failed: {out[:160]!r}"
        if rows is None:
            if want:
                return "missing", f"{table} missing after ack: {out[:160]!r}"
            rows = []
        missing = sorted(set(want) - set(rows))
        if missing:
            return "missing", f"lost committed rows in {table}: {missing} (present={sorted(rows)})"
        if len(rows) != len(set(rows)):
            dups = sorted(x for x in set(rows) if rows.count(x) > 1)
            return "dup", f"duplicate rows in {table}: {dups}"
        first[table] = rows
    for i in range(1, opens):
        for table, rows0 in first.items():
            opened, rows, out = open_rows_in(binary, cwd, table)
            if not opened:
                return "drift", f"open #{i + 1} failed after a successful open"
            rows = rows if rows is not None else []
            if rows != rows0:
                return "drift", f"open #{i + 1} changed {table}: {len(rows0)} -> {len(rows)}"
    return "ok", f"main={len(first['main.t'])} db1={len(first['db1.t'])}"


def run_multi(binary, rounds, seed):
    rng = random.Random(seed)
    kinds = {}
    for i in range(rounds):
        units, cmds = random_script_multi(rng)
        d = fresh(f"m{i}")
        out = run_and_kill(binary, ".open x.pink\n" + cmds, d, rng.uniform(0.001, 0.05))
        em, ed = acked_rows_multi(out, units)
        kind, detail = verify_multi(binary, d, em, ed)
        kinds[kind] = kinds.get(kind, 0) + 1
        if kind != "ok":
            keep = os.path.join(WORK, f"keep_m{i}")
            shutil.rmtree(keep, ignore_errors=True)
            shutil.copytree(d, keep)
            print(f"FAIL multi iter {i}: {kind}: {detail}\n  kept: {keep}")
        elif i % 50 == 0:
            print(f"multi iter {i}: ok ({detail})")
        shutil.rmtree(d, ignore_errors=True)
    print(f"=== multi-db kill loop: {rounds} iterations, kinds={kinds} ===")
    return kinds.get("ok", 0) == rounds


def checkpoint_image(binary, d, prior_checkpoint):
    """A database whose next `.save` is killed in the roll->publish window."""
    big = "z" * 1_000_000
    if prior_checkpoint:
        setup = (
            ".open x.pink\nCREATE TABLE t (a INTEGER, v VARCHAR);\n"
            f"INSERT INTO t VALUES (0, '{big}');\n.save x.pink\n"
            f"INSERT INTO t VALUES (1, '{big}');\n.quit\n"
        )
        expected = [0, 1]
    else:
        setup = (
            ".open x.pink\nCREATE TABLE t (a INTEGER, v VARCHAR);\n"
            f"INSERT INTO t VALUES (1, '{big}');\nINSERT INTO t VALUES (2, '{big}');\n.quit\n"
        )
        expected = [1, 2]
    cli(binary, setup, d)

    p = subprocess.Popen(
        [binary], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, cwd=d, env=ENV
    )
    try:
        p.stdin.write(b".open x.pink\n.save x.pink\n.quit\n")
        p.stdin.flush()
    except Exception:
        pass
    hit = False
    deadline = time.time() + 10
    while time.time() < deadline:
        names = os.listdir(d)
        rolled = any(n.startswith("x.pink.wal.") and n.endswith(".roll") for n in names)
        # After the roll, before the publish: `.tmp` is still present (the
        # publish rename removes it). This holds with or without a prior
        # checkpoint file.
        if rolled and "x.pink.tmp" in names:
            hit = True
            break
        if p.poll() is not None:
            break
    try:
        p.kill()
    except ProcessLookupError:
        pass
    p.wait()
    # If the publish won the race, `.tmp` is already gone: a control image.
    hit = hit and os.path.exists(os.path.join(d, "x.pink.tmp"))
    return expected, hit


def run_targeted(binary, rounds, seed):
    rng = random.Random(seed)
    kinds = {}
    hits = 0
    for i in range(rounds):
        prior = i % 2 == 1
        d = fresh(f"t{i}")
        expected, hit = checkpoint_image(binary, d, prior)
        hits += hit
        kind, detail = verify(binary, d, expected)
        kinds[kind] = kinds.get(kind, 0) + 1
        if kind != "ok":
            print(f"FAIL iter {i} (prior_checkpoint={prior}, caught={hit}): {kind}: {detail}")
        elif i % 10 == 0:
            print(f"iter {i}: ok ({detail}, caught={hit})")
        shutil.rmtree(d, ignore_errors=True)
    print(
        f"=== targeted kill-during-checkpoint: {rounds} iterations, "
        f"caught_in_window={hits}, kinds={kinds} ==="
    )
    return kinds.get("ok", 0) == rounds


def main():
    global ENV
    ap = argparse.ArgumentParser()
    ap.add_argument("rounds", nargs="?", type=int, default=100)
    ap.add_argument("--targeted", type=int, default=0)
    ap.add_argument("--multi-db", action="store_true")
    ap.add_argument("--encrypted", action="store_true",
                    help="run with the PINKDB_KEY from the environment "
                         "(needs an encryption-feature binary)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--bin", default=DEFAULT_BIN)
    ap.add_argument(
        "--extension",
        default=None,
        help="load this extension in every random-mode iteration and call it "
             "from every insert (exercises the touched-only LOAD EXTENSION "
             "WAL prefixing across checkpoints)",
    )
    args = ap.parse_args()
    if not os.path.exists(args.bin):
        print(f"missing binary: {args.bin}", file=sys.stderr)
        return 2
    extension = None
    if args.extension:
        extension = os.path.abspath(args.extension)
        if not os.path.exists(extension):
            print(f"missing extension: {extension}", file=sys.stderr)
            return 2
        if args.multi_db or args.targeted:
            print("--extension supports the random mode only", file=sys.stderr)
            return 2
        print(f"extension mode: {extension}")
    if args.encrypted:
        passphrase = os.environ.get("PINKDB_KEY", "")
        if not passphrase:
            print(
                "--encrypted requires PINKDB_KEY in the environment; the "
                "passphrase is read from the env, never from the command line",
                file=sys.stderr,
            )
            return 2
        ENV = dict(os.environ)
        print(
            f"encrypted mode: PINKDB_KEY from the environment "
            f"({len(passphrase)} chars)"
        )
    if args.targeted:
        ok = run_targeted(args.bin, args.targeted, args.seed)
    elif args.multi_db:
        ok = run_multi(args.bin, args.rounds, args.seed)
    else:
        ok = run_random(args.bin, args.rounds, args.seed, extension)
    if ok:
        shutil.rmtree(WORK, ignore_errors=True)
    else:
        print(f"evidence kept under {WORK}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
