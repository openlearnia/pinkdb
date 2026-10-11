#!/usr/bin/env python3
"""Differential test: pinkdb vs DuckDB.

Each query runs in a FRESH process on both sides (setup + query), so
mutations never leak between cases. Results compare at value level:
ints exact, floats within tolerance, NULLs, dates/timestamps normalized
to microseconds. Column names are ignored (header formats differ).

Batteries: small relational set (in-SQL setup) + 1M-row CSV set.

Usage: python3 bench/duckdiff.py [--verbose]
Exit code: 0 iff every query matches.
"""

import json
import re
import subprocess
import sys
import datetime

VERBOSE = "--verbose" in sys.argv
PINKDB = ["./target/release/pinkdb"]

PASS = 0
FAIL = 0
FAILED_NAMES = []


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        if VERBOSE:
            print(f"  ok: {name}")
    else:
        FAIL += 1
        FAILED_NAMES.append(name)
        print(f"  FAIL: {name} {detail}")


# ---------- value normalization ----------

TS_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?$"
)
DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
EPOCH = datetime.datetime(1970, 1, 1)


def to_micros(s):
    m = TS_RE.match(s)
    if m:
        y, mo, d, h, mi, sec, frac = m.groups()
        dt = datetime.datetime(int(y), int(mo), int(d), int(h), int(mi), int(sec))
        us = int((dt - EPOCH).total_seconds()) * 1_000_000
        if frac:
            us += int(frac.ljust(6, "0"))
        return us
    if DATE_RE.match(s):
        return to_micros(s + " 00:00:00")
    return None


def num(s):
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def norm_pink(cell, typ):
    if cell == "NULL":
        return None
    if typ in ("INTEGER", "BIGINT"):
        return int(cell)
    if typ == "DOUBLE":
        return float(cell)
    if typ == "BOOLEAN":
        return cell.lower() == "true"
    return cell


def values_equal(a, b):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if isinstance(a, str) and isinstance(b, str):
        ma, mb = to_micros(a), to_micros(b)
        if ma is not None and mb is not None:
            return ma == mb
        return a == b
    fa = a if isinstance(a, (int, float)) else num(a)
    fb = b if isinstance(b, (int, float)) else num(b)
    if fa is not None and fb is not None:
        if isinstance(fa, int) and isinstance(fb, int):
            return fa == fb
        return abs(fa - fb) <= 1e-9 * max(1.0, abs(fa), abs(fb))
    return a == b


def bag_key(rows):
    def rowkey(r):
        return repr(sorted([(type(v).__name__, str(v)) for v in r]))
    return sorted(rows, key=rowkey)


def rows_equal(pink_rows, duck_rows, ordered):
    if len(pink_rows) != len(duck_rows):
        return False
    if not ordered:
        pink_rows, duck_rows = bag_key(pink_rows), bag_key(duck_rows)
    for pr, dr in zip(pink_rows, duck_rows):
        if len(pr) != len(dr):
            return False
        if not all(values_equal(a, b) for a, b in zip(pr, dr)):
            return False
    return True


# ---------- engine drivers (one fresh process per case) ----------

def pink_query(setup, sql):
    """Returns ("rows", types, rows) | ("error", msg)."""
    script = setup + "\n" + sql + "\n.quit\n"
    p = subprocess.run(PINKDB, input=script, capture_output=True, text=True,
                       timeout=300)
    tables = []
    types, rows, state = None, [], 0  # 0 idle, 1 header, 2 types, 3 data
    for line in p.stdout.splitlines():
        s = line.strip()
        if s.startswith("Error:"):
            tables.append(("error", s))
            continue
        if s.startswith("│"):
            cells = [c.strip() for c in s.split("│")[1:-1]]
            if state == 0:
                state = 1  # header row ignored
            elif state == 1:
                types, rows, state = cells, [], 2  # types row
            else:
                rows.append(cells)  # data
            continue
        if s.startswith("└") and state == 2:
            tables.append(("rows", types, rows))
            state = 0
    selects = sum(1 for part in sql.split(";")
                  if part.strip().lower().startswith(("select", "with")))
    if len(tables) != selects:
        return ("error", f"harness: {len(tables)} results for {selects} selects in {sql[:60]!r}")
    # Multi-SELECT cases (unions parsed as one statement each produce one
    # table) — return the LAST select's result like the battery expects.
    return tables[-1]


def duck_query(setup, sql):
    """Returns ("rows", rows) | ("error", msg)."""
    p = subprocess.run(["duckdb", "-json"], input=setup + "\n" + sql + "\n",
                       capture_output=True, text=True, timeout=300)
    err = (p.stderr or "") + p.stdout
    if p.returncode != 0 or "Error" in err:
        first = next((l for l in err.splitlines() if "Error" in l), err[:200])
        return ("error", first.strip())
    docs = []
    for chunk in re.split(r"\n(?=\[)", p.stdout.strip()):
        if chunk.strip():
            docs.append(json.loads(chunk))
    if not docs:
        return ("error", "harness: no SELECT output")
    doc = docs[-1]
    cols = list(doc[0].keys()) if doc else []
    return ("rows", [[r.get(c) for c in cols] for r in doc])


# ---------- batteries ----------

SETUP = """
CREATE TABLE emp (id INTEGER, name VARCHAR, did INTEGER, salary DOUBLE, hired DATE, active BOOLEAN);
INSERT INTO emp VALUES
 (1, 'ann', 10, 150.5, DATE '2024-01-15', true),
 (2, 'bob', 20, 50.0, DATE '2024-02-20', false),
 (3, 'cat', NULL, 200.25, DATE '2023-12-01', true),
 (4, 'dan', 10, NULL, DATE '2024-01-15', NULL),
 (5, 'eve', 20, 75.75, NULL, false);
CREATE TABLE dept (did INTEGER, dname VARCHAR);
INSERT INTO dept VALUES (10, 'eng'), (20, 'ops'), (30, 'hr');
"""

# (label, sql, ordered, expect_error)
QUERIES = [
    ("filter-project", "SELECT id, name FROM emp WHERE salary > 100 ORDER BY id;", True, False),
    ("is-null", "SELECT id FROM emp WHERE did IS NULL;", False, False),
    ("is-not-null", "SELECT id FROM emp WHERE salary IS NOT NULL ORDER BY id;", True, False),
    ("between", "SELECT id FROM emp WHERE salary BETWEEN 50 AND 160 ORDER BY id;", True, False),
    ("not-between", "SELECT id FROM emp WHERE id NOT BETWEEN 2 AND 4 ORDER BY id;", True, False),
    ("group-agg", "SELECT did, COUNT(*), COUNT(salary), SUM(salary), AVG(salary), MIN(salary), MAX(salary) FROM emp GROUP BY did ORDER BY did;", True, False),
    ("having", "SELECT did, AVG(salary) AS a FROM emp GROUP BY did HAVING COUNT(*) > 1 AND AVG(salary) > 10 ORDER BY did;", True, False),
    ("distinct", "SELECT DISTINCT did FROM emp ORDER BY did;", True, False),
    ("scalars", "SELECT upper(name), lower(name), length(name), substr(name, 1, 2) FROM emp ORDER BY id;", True, False),
    ("arith", "SELECT id / 2, id % 2, -id, id + did FROM emp ORDER BY id;", True, False),
    ("bool-logic", "SELECT id FROM emp WHERE active AND salary > 10 ORDER BY id;", True, False),
    ("not-bool", "SELECT id FROM emp WHERE NOT active ORDER BY id;", True, False),
    ("or-null", "SELECT id FROM emp WHERE active OR salary > 100 ORDER BY id;", True, False),
    ("join", "SELECT name, dname FROM emp JOIN dept ON emp.did = dept.did ORDER BY name;", True, False),
    ("join-count", "SELECT COUNT(*) FROM emp JOIN dept ON emp.did = dept.did;", False, False),
    ("self-join", "SELECT a.name FROM emp a JOIN emp b ON a.did = b.did WHERE a.id < b.id ORDER BY 1;", True, False),
    ("union-all", "SELECT id FROM emp WHERE id <= 2 UNION ALL SELECT did FROM dept WHERE did >= 20;", False, False),
    ("union-distinct", "SELECT did FROM emp UNION SELECT did FROM dept;", False, False),
    ("empty-agg", "SELECT COUNT(*), SUM(salary), AVG(salary), MIN(salary) FROM emp WHERE false;", False, False),
    ("scalar-sub", "SELECT name FROM emp WHERE did = (SELECT did FROM dept WHERE dname = 'ops');", False, False),
    ("scalar-empty", "SELECT name FROM emp WHERE did = (SELECT did FROM dept WHERE false);", False, False),
    ("in-sub", "SELECT name FROM emp WHERE did IN (SELECT did FROM dept) ORDER BY name;", True, False),
    ("not-in-nulls", "SELECT name FROM emp WHERE did NOT IN (SELECT did FROM dept) ORDER BY name;", True, False),
    ("not-in-list", "SELECT name FROM emp WHERE id NOT IN (2, 3) ORDER BY name;", True, False),
    ("in-list", "SELECT name FROM emp WHERE id IN (1, 3, 5) ORDER BY name;", True, False),
    ("exists", "SELECT COUNT(*) FROM emp WHERE EXISTS (SELECT * FROM dept WHERE did = 20);", False, False),
    ("not-exists", "SELECT dname FROM dept WHERE NOT EXISTS (SELECT * FROM emp WHERE did = 999) ORDER BY 1;", True, False),
    ("cte", "WITH e AS (SELECT * FROM emp WHERE id > 1) SELECT name FROM e ORDER BY name;", True, False),
    ("cte-chained", "WITH e AS (SELECT * FROM emp WHERE id > 1), n AS (SELECT name FROM e WHERE did IS NOT NULL) SELECT * FROM n;", False, False),
    ("derived", "SELECT name FROM (SELECT * FROM emp WHERE did = 20) AS s ORDER BY name;", True, False),
    ("row-number", "SELECT name, ROW_NUMBER() OVER (PARTITION BY did ORDER BY id) AS rn FROM emp ORDER BY name;", True, False),
    ("rank-dense", "SELECT RANK() OVER (ORDER BY salary) AS r, DENSE_RANK() OVER (ORDER BY salary) AS d FROM emp ORDER BY salary;", True, False),
    ("running-sum", "SELECT id, SUM(salary) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run FROM emp ORDER BY id;", True, False),
    ("win-agg", "SELECT did, SUM(salary) AS s, RANK() OVER (ORDER BY SUM(salary) DESC) AS r FROM emp GROUP BY did ORDER BY r;", True, False),
    ("date-arith", "SELECT hired + 10, hired - DATE '2024-01-01' FROM emp ORDER BY id;", True, False),
    ("date-parts", "SELECT year(hired), month(hired), day(hired) FROM emp ORDER BY id;", True, False),
    ("date-trunc-group", "SELECT date_trunc('month', hired) AS m, COUNT(*) FROM emp GROUP BY m ORDER BY m;", True, False),
    ("date-minmax", "SELECT MIN(hired), MAX(hired), COUNT(hired) FROM emp;", False, False),
    ("like", "SELECT name FROM emp WHERE name LIKE 'a%' OR name LIKE '%e%' ORDER BY name;", True, False),
    ("not-like", "SELECT name FROM emp WHERE name NOT LIKE '%a%' ORDER BY name;", True, False),
    ("cast", "SELECT CAST(id AS VARCHAR), CAST(id AS DOUBLE), CAST(salary AS INTEGER) FROM emp WHERE id = 2;", True, False),
    ("coalesce", "SELECT COALESCE(did, -1), COALESCE(salary, -1) FROM emp ORDER BY 1;", True, False),
    ("no-from", "SELECT 1 + 2 * 3, 7 / 2, 7 % 3;", False, False),
    ("round-abs", "SELECT round(2.567, 1), abs(-3), abs(salary) FROM emp ORDER BY id;", True, False),
    ("desc-nulls", "SELECT id FROM emp ORDER BY salary DESC;", True, False),
    ("update-where", "UPDATE emp SET salary = salary + 1 WHERE id = 1; SELECT salary FROM emp WHERE id = 1;", False, False),
    ("delete-where", "DELETE FROM emp WHERE id = 5; SELECT COUNT(*) FROM emp;", False, False),
    ("view", "CREATE VIEW big AS SELECT * FROM emp WHERE salary > 100; SELECT name FROM big ORDER BY name;", True, False),
    ("bad-col", "SELECT nope FROM emp;", False, True),
]


def run_small():
    print("== small battery ==")
    for label, sql, ordered, expect_error in QUERIES:
        got = pink_query(SETUP, sql)
        if expect_error:
            d = duck_query(SETUP, sql)
            check(label, got[0] == "error" and d[0] == "error",
                  f"pink={got[0]} duck={d[0]}")
            continue
        if got[0] == "error":
            check(label, False, f"pinkdb error: {got[1]}")
            continue
        _, types, prows = got
        d = duck_query(SETUP, sql)
        if d[0] == "error":
            check(label, False, f"duckdb error: {d[1]}")
            continue
        _, drows = d
        prows_n = [[norm_pink(c, t) for c, t in zip(r, types)] for r in prows]
        ok = rows_equal(prows_n, drows, ordered)
        if ok:
            check(label, True)
            if VERBOSE:
                print(f"    rows={len(prows_n)}")
        else:
            check(label, False, f"\npink={prows_n}\nduck={drows}")


BIG_QUERIES = [
    ("big-count", "SELECT COUNT(*) FROM sales;", True, False),
    ("big-group", "SELECT region, COUNT(*), AVG(total), SUM(total), MIN(total), MAX(total) FROM sales GROUP BY region ORDER BY region;", True, False),
    ("big-filter", "SELECT COUNT(*) FROM sales WHERE total > 500;", True, False),
    ("big-having", "SELECT region, AVG(total) FROM sales WHERE active GROUP BY region HAVING COUNT(*) > 100000 ORDER BY 2 DESC LIMIT 3;", True, False),
]


def run_big(csv_path):
    print("== 1M-row battery ==")
    setup = f"CREATE TABLE sales AS SELECT * FROM read_csv('{csv_path}', header=true);"
    psetup = f".import {csv_path} sales"
    for label, sql, ordered, _ in BIG_QUERIES:
        got = pink_query(psetup, sql)
        if got[0] == "error":
            check(f"big: {label}", False, f"pinkdb error: {got[1]}")
            continue
        _, types, prows = got
        d = duck_query(setup, sql)
        if d[0] == "error":
            check(f"big: {label}", False, f"duckdb error: {d[1]}")
            continue
        prows_n = [[norm_pink(c, t) for c, t in zip(r, types)] for r in prows]
        ok = rows_equal(prows_n, d[1], ordered)
        check(f"big: {label}", ok, "" if ok else f"\npink={prows_n}\nduck={d[1]}")


def main():
    import sys
    run_small()
    if "--big" in sys.argv:
        idx = sys.argv.index("--big")
        run_big(sys.argv[idx + 1] if idx + 1 < len(sys.argv) else "/tmp/sales_1m.csv")
    print(f"\n{PASS} passed, {FAIL} failed")
    if FAILED_NAMES:
        print("failures:")
        for n in FAILED_NAMES:
            print(f"  - {n}")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
