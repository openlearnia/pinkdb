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
import os
import re
import subprocess
import sys
import datetime

VERBOSE = "--verbose" in sys.argv
# The CLI under test: defaults to the worktree's release build; PINKDB_CLI
# overrides it (e.g. a copy that cannot be clobbered by a concurrent build).
PINKDB = [os.environ.get("PINKDB_CLI", "./target/release/pinkdb")]

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
    # Errors go to stderr; merge it so an error stays in sequence with the
    # result tables.
    p = subprocess.run(PINKDB, input=script, stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT, text=True, timeout=300)
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
                  if part.strip().lower().startswith(
                      ("select", "with", "show", "describe")))
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
CREATE TABLE jx (x INTEGER, y INTEGER);
INSERT INTO jx VALUES (1, 10), (2, 20), (5, 50);
CREATE TABLE jy (x INTEGER, z VARCHAR);
INSERT INTO jy VALUES (2, 'two'), (3, 'three'), (6, 'six');
CREATE TABLE jz (x INTEGER, w INTEGER);
INSERT INTO jz VALUES (2, 200), (3, 300);
CREATE TABLE pn (k INTEGER, v INTEGER);
INSERT INTO pn VALUES (2, 20), (2, 21), (3, 30), (1, 10), (3, 31);
CREATE TABLE nn (x INTEGER);
INSERT INTO nn VALUES (2), (NULL), (2), (3), (NULL);
CREATE TABLE jn (x INTEGER, y INTEGER);
INSERT INTO jn VALUES (1, NULL), (NULL, 1), (2, 2);
CREATE TABLE dk (k DOUBLE, v INTEGER);
INSERT INTO dk VALUES (1.0, 1), (1.5, 2), (2.0, 3);
CREATE TABLE typed (a BIGINT, b TIMESTAMP);
CREATE VIEW emp_view AS SELECT id, name FROM emp WHERE id > 1;
CREATE TABLE dt (x DECIMAL(6,2), iv INTERVAL);
INSERT INTO dt VALUES ('1.25', '1 day'), ('2.50', '2 hours'), (NULL, NULL);
-- wave 41: declarative constraints (metadata only; DESCRIBE parity + FK joins).
CREATE TABLE keys_p (id INTEGER PRIMARY KEY, code VARCHAR UNIQUE, v INTEGER NOT NULL, w INTEGER);
INSERT INTO keys_p VALUES (1, 'a', 10, 100), (2, 'b', 20, 200);
CREATE TABLE keys_c (cid INTEGER, pid INTEGER NOT NULL REFERENCES keys_p(id), note VARCHAR, PRIMARY KEY (cid, note));
INSERT INTO keys_c VALUES (1, 1, 'x'), (2, 2, 'y');
CREATE TABLE fk_null_c (cid INTEGER, pid INTEGER REFERENCES keys_p(id));
INSERT INTO fk_null_c VALUES (1, 1), (2, NULL);
-- wave 52: a document column for the core json extension probes.
CREATE TABLE jdocs (id INTEGER, doc VARCHAR);
INSERT INTO jdocs VALUES
 (1, '{"t": ["a", "b"], "n": "ann"}'),
 (2, '{"t": [], "n": "bob"}'),
 (3, '{"t": ["c"], "n": "cat"}');
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
    ("left-join", "SELECT emp.id, name, dname FROM emp LEFT JOIN dept ON emp.did = dept.did ORDER BY id;", True, False),
    ("right-join", "SELECT id, name, dept.did, dname FROM emp RIGHT JOIN dept ON emp.did = dept.did ORDER BY dname, id;", True, False),
    ("left-join-where", "SELECT emp.id, dname FROM emp LEFT JOIN dept ON emp.did = dept.did WHERE dept.did > 10 ORDER BY id;", True, False),
    ("left-join-count", "SELECT COUNT(*), COUNT(dept.did) FROM emp LEFT JOIN dept ON emp.did = dept.did;", False, False),
    ("union-all", "SELECT id FROM emp WHERE id <= 2 UNION ALL SELECT did FROM dept WHERE did >= 20;", False, False),
    ("union-distinct", "SELECT did FROM emp UNION SELECT did FROM dept;", False, False),
    ("union-all-order-limit", "SELECT id FROM emp WHERE id <= 3 UNION ALL SELECT did FROM dept WHERE did >= 20 ORDER BY id DESC LIMIT 4;", True, False),
    ("union-in-derived", "SELECT x FROM (SELECT id AS x FROM emp WHERE id <= 2 UNION ALL SELECT did AS x FROM dept WHERE did >= 20) s ORDER BY x;", True, False),
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
    ("case-searched", "SELECT id, CASE WHEN id = 1 THEN 'one' WHEN id = 2 THEN 'two' ELSE 'other' END FROM emp ORDER BY id;", True, False),
    ("case-simple", "SELECT CASE id WHEN 1 THEN 'one' ELSE 'many' END FROM emp ORDER BY id;", True, False),
    ("case-nulls", "SELECT CASE WHEN did IS NULL THEN 'none' WHEN salary > 100 THEN 'big' ELSE 'small' END FROM emp ORDER BY id;", True, False),
    ("case-numeric", "SELECT CASE WHEN id = 1 THEN 1 WHEN id = 2 THEN 2.5 ELSE 0 END FROM emp ORDER BY id;", True, False),
    ("case-agg", "SELECT SUM(CASE WHEN did = 10 THEN salary ELSE 0 END) FROM emp;", False, False),
    # --- wave: concat / trim / replace / nullif / greatest-least ---
    ("concat-op", "SELECT id, name || '-' || did, did || '-x' FROM emp ORDER BY id;", True, False),
    ("concat-fn", "SELECT concat(name, '-', did), concat('x'), concat(did, did) FROM emp ORDER BY id;", True, False),
    ("concat-precedence", "SELECT 'a' || 'b' = 'ab', 1 + 2 || '3', 'x' || 1 + 2;", False, False),
    ("trim-family", "SELECT '[' || trim('  ab  ') || ']', '[' || ltrim('  ab  ') || ']', '[' || rtrim('  ab  ') || ']', '[' || trim('xxabxx', 'x') || ']', '[' || ltrim('xxabxx', 'x') || ']', '[' || rtrim('xxabxx', 'x') || ']', '[' || trim('ab', '') || ']', '[' || trim(name || '!', 'x') || ']' FROM emp ORDER BY id;", True, False),
    ("replace-fn", "SELECT replace(name, 'a', 'A'), replace('aaa', 'a', 'b'), replace(name, '', '!') FROM emp ORDER BY id;", True, False),
    ("nullif-fn", "SELECT nullif(id, 1), nullif(did, 10), nullif(name, 'ann'), nullif(id, NULL), nullif(NULL, id) FROM emp ORDER BY id;", True, False),
    ("greatest-least", "SELECT greatest(id, did, 3), least(id, did, 3), greatest(salary, 100.0), least(salary, 100.0), greatest(id, 99), greatest(NULL, NULL) FROM emp ORDER BY id;", True, False),
    # --- wave: extract / date_part ---
    ("extract-date", "SELECT extract(year FROM hired), extract(month FROM hired), extract(day FROM hired), extract(dow FROM hired), extract(isodow FROM hired), extract(doy FROM hired), extract(week FROM hired), extract(quarter FROM hired), extract(epoch FROM hired) FROM emp ORDER BY id;", True, False),
    ("extract-ts", "SELECT extract(hour FROM TIMESTAMP '2024-02-29 13:45:56.789012'), extract(minute FROM TIMESTAMP '2024-02-29 13:45:56.789012'), extract(second FROM TIMESTAMP '2024-02-29 13:45:56.789012'), extract(millisecond FROM TIMESTAMP '2024-02-29 13:45:56.789012'), extract(microsecond FROM TIMESTAMP '2024-02-29 13:45:56.789012'), extract(epoch FROM TIMESTAMP '1969-12-31 23:59:59.5'), extract(isoyear FROM DATE '2023-01-01'), extract(week FROM DATE '2023-01-01'), extract(century FROM DATE '2024-01-01'), extract(decade FROM DATE '2024-01-01'), extract(millennium FROM DATE '2024-01-01');", False, False),
    ("date-part-fn", "SELECT date_part('year', hired), date_part('dow', hired), date_part('ms', TIMESTAMP '2024-01-01 00:00:56.789012'), date_part('us', TIMESTAMP '2024-01-01 00:00:56.789012'), date_part('sec', TIMESTAMP '2024-01-01 00:00:56.789012') FROM emp ORDER BY id;", True, False),
    ("extract-string-part", "SELECT extract('year' FROM hired), extract('week' FROM hired), extract('hour' FROM hired) FROM emp ORDER BY id;", True, False),
    ("date-part-dynamic", "SELECT date_part(p, d), date_part(p2, d) FROM (SELECT 'year' AS p, 'dow' AS p2, DATE '2024-03-03' AS d) AS s;", False, False),
    # --- wave: numeric scalars ---
    ("numeric-fns", "SELECT sqrt(id), sqrt(salary), floor(salary), ceil(salary), floor(id), ceil(-1.5), sign(id - 3), sign(salary), power(id, 2), pow(2, 0.5), mod(id, 2), mod(id, 3), mod(salary, 2) FROM emp ORDER BY id;", True, False),
    # --- wave: window functions lag/lead/ntile/first_value/last_value ---
    ("win-lag-lead", "SELECT id, lag(name) OVER (ORDER BY id), lead(name) OVER (ORDER BY id), lag(salary, 2) OVER (ORDER BY id), lead(salary, 1, -1.0) OVER (ORDER BY id), lag(id, -1) OVER (ORDER BY id) FROM emp ORDER BY id;", True, False),
    ("win-lag-default", "SELECT id, lag(id, 1, 99) OVER (ORDER BY id), lag(id, 2, 99) OVER (ORDER BY id), lag(id, 0) OVER (ORDER BY id) FROM emp ORDER BY id;", True, False),
    ("win-lag-partition", "SELECT id, lag(salary) OVER (PARTITION BY did ORDER BY id), lead(name) OVER (PARTITION BY did ORDER BY id) FROM emp ORDER BY id;", True, False),
    ("win-ntile", "SELECT id, ntile(3) OVER (ORDER BY id), ntile(2) OVER (PARTITION BY did ORDER BY id), ntile(8) OVER (ORDER BY id) FROM emp ORDER BY id;", True, False),
    ("win-first-last", "SELECT id, first_value(salary) OVER (ORDER BY id), last_value(salary) OVER (ORDER BY id), first_value(name) OVER (PARTITION BY did ORDER BY id), last_value(name) OVER (PARTITION BY did ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM emp ORDER BY id;", True, False),
    ("win-first-last-frame", "SELECT id, first_value(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), last_value(id) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING), first_value(id) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) FROM emp ORDER BY id;", True, False),
    ("win-lag-ignore-frame", "SELECT id, lag(id) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW), lead(id) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) FROM emp ORDER BY id;", True, False),
    ("win-no-order", "SELECT id, first_value(salary) OVER (), last_value(salary) OVER (), lag(id) OVER (), lead(id) OVER () FROM emp ORDER BY id;", True, False),
    # --- wave: error cases (both engines must reject) ---
    ("concat-empty", "SELECT concat() FROM emp;", False, True),
    ("ntile-zero", "SELECT ntile(0) OVER (ORDER BY id) FROM emp;", False, True),
    ("lag-bad-offset", "SELECT lag(id, 1.5) OVER (ORDER BY id) FROM emp;", False, True),
    ("lag-bad-default", "SELECT lag(id, 1, 'x') OVER (ORDER BY id) FROM emp;", False, True),
    ("extract-bad-part", "SELECT extract(fortnight FROM hired) FROM emp;", False, True),
    ("sign-bool", "SELECT sign(active) FROM emp;", False, True),
    ("mod-zero", "SELECT mod(id, 0) FROM emp;", False, True),
    # ---- wave 58: SQL completeness (joins / set ops / DML-DDL / windows) ----
    ("multi-join", "SELECT a.x AS ax, b.x AS bx, c.x AS cx FROM jx a JOIN jy b ON a.x < b.x JOIN jz c ON b.x < c.x ORDER BY 1, 2, 3;", True, False),
    ("multi-join-left-chain", "SELECT a.x, b.z, c.w FROM jx a LEFT JOIN jy b ON a.x = b.x LEFT JOIN jz c ON b.x = c.x ORDER BY 1, 2, 3;", True, False),
    ("join-composite-on", "SELECT a.x AS ax, b.x AS bx FROM jx a JOIN jy b ON a.x = b.x AND a.y = b.x * 10;", True, False),
    ("join-composite-null-keys", "SELECT COUNT(*) FROM jn a JOIN jn b ON a.x = b.x AND a.y = b.y;", False, False),
    ("join-single-null-keys", "SELECT COUNT(*) FROM jn a JOIN jn b ON a.x = b.x;", False, False),
    ("full-join-null-keys", "SELECT COUNT(*) FROM jn a FULL JOIN jn b ON a.x = b.x;", False, False),
    ("join-non-equi-lt", "SELECT a.x AS ax, b.x AS bx FROM jx a JOIN jy b ON a.x < b.x ORDER BY a.x, b.x;", True, False),
    ("join-non-equi-ne", "SELECT a.x AS ax, b.x AS bx FROM jx a JOIN jy b ON a.x <> b.x ORDER BY a.x, b.x;", True, False),
    ("join-cross", "SELECT COUNT(*) FROM jx CROSS JOIN jy;", False, False),
    ("join-cross-proj", "SELECT a.x, b.z FROM jx a CROSS JOIN jy b WHERE a.x > 1 ORDER BY 1, 2;", True, False),
    ("full-join", "SELECT jx.x AS ax, jy.x AS bx, y, z FROM jx FULL JOIN jy ON jx.x = jy.x ORDER BY ax NULLS LAST, bx NULLS LAST;", True, False),
    ("full-join-unmatched", "SELECT jx.x AS ax, jy.x AS bx FROM jx FULL JOIN jy ON jx.x = jy.x WHERE jx.x IS NULL OR jy.x IS NULL ORDER BY ax NULLS LAST, bx NULLS LAST;", True, False),
    ("full-join-count", "SELECT COUNT(*), COUNT(jx.x), COUNT(jy.x) FROM jx FULL JOIN jy ON jx.x = jy.x;", False, False),
    ("full-join-chain", "SELECT a.x AS ax, b.x AS bx, c.x AS cx FROM jx a FULL JOIN jy b ON a.x = b.x FULL JOIN jz c ON b.x = c.x ORDER BY ax NULLS LAST, bx NULLS LAST, cx NULLS LAST;", True, False),
    ("intersect", "SELECT x FROM nn INTERSECT SELECT x FROM jx ORDER BY 1;", True, False),
    ("intersect-all", "SELECT x FROM nn INTERSECT ALL SELECT x FROM jx ORDER BY 1;", True, False),
    ("except", "SELECT x FROM nn EXCEPT SELECT x FROM jx ORDER BY 1;", True, False),
    ("except-all", "SELECT x FROM nn EXCEPT ALL SELECT x FROM jx ORDER BY 1;", True, False),
    ("setop-precedence", "SELECT x FROM jx UNION SELECT x FROM jy INTERSECT SELECT x FROM jz ORDER BY 1;", True, False),
    ("setop-precedence-except", "SELECT x FROM jx EXCEPT SELECT x FROM jy INTERSECT SELECT x FROM jz ORDER BY 1;", True, False),
    ("setop-chain-limit", "SELECT x FROM jx UNION SELECT x FROM jy INTERSECT SELECT x FROM jz ORDER BY x DESC LIMIT 3;", True, False),
    ("setop-in-derived", "SELECT x FROM (SELECT x FROM jx INTERSECT SELECT x FROM jy) t ORDER BY x;", True, False),
    ("insert-select", "CREATE TABLE t_ins (p INTEGER, q DOUBLE); INSERT INTO t_ins (q, p) SELECT y * 1.0, x FROM jx WHERE x = 2; SELECT * FROM t_ins;", True, False),
    ("insert-select-cols", "CREATE TABLE t_ins2 (p INTEGER, q INTEGER); INSERT INTO t_ins2 (q, p) SELECT y, x FROM jx WHERE x = 1; SELECT * FROM t_ins2;", True, False),
    ("ctas", "CREATE TABLE t_ctas AS SELECT x, y * 2 AS d FROM jx WHERE x > 1; SELECT * FROM t_ctas ORDER BY x;", True, False),
    ("ctas-union", "CREATE TABLE t_ctas_u AS SELECT x FROM jx UNION SELECT x FROM jy INTERSECT SELECT x FROM jz; SELECT x FROM t_ctas_u ORDER BY x;", True, False),
    ("insert-select-union", "CREATE TABLE t_isu (x INTEGER); INSERT INTO t_isu SELECT x FROM jx WHERE x = 1 UNION ALL SELECT x FROM jy WHERE x = 3; SELECT x FROM t_isu ORDER BY x;", True, False),
    ("ctas-if-not-exists", "CREATE TABLE IF NOT EXISTS t_ctas2 AS SELECT 1 AS one; CREATE TABLE IF NOT EXISTS t_ctas2 AS SELECT 2; SELECT * FROM t_ctas2;", True, False),
    ("drop-if-exists", "DROP TABLE IF EXISTS no_such_table_here; SELECT 7;", True, False),
    ("offset-alone", "SELECT x FROM nn ORDER BY x OFFSET 2;", True, False),
    ("offset-limit", "SELECT x FROM nn ORDER BY x LIMIT 2 OFFSET 1;", True, False),
    ("nulls-first-asc", "SELECT x FROM nn ORDER BY x NULLS FIRST;", True, False),
    ("nulls-first-desc", "SELECT x FROM nn ORDER BY x DESC NULLS FIRST;", True, False),
    ("nulls-last-desc", "SELECT x FROM nn ORDER BY x DESC NULLS LAST;", True, False),
    ("nulls-default-desc", "SELECT x FROM nn ORDER BY x DESC;", True, False),
    ("count-distinct", "SELECT COUNT(DISTINCT x), COUNT(x), COUNT(*) FROM nn;", False, False),
    ("count-distinct-varchar", "SELECT COUNT(DISTINCT z) FROM jy;", False, False),
    ("count-distinct-empty", "SELECT COUNT(DISTINCT did) FROM emp WHERE did IS NULL;", False, False),
    ("count-distinct-group", "SELECT did, COUNT(DISTINCT salary) FROM emp GROUP BY did ORDER BY did;", True, False),
    ("win-default-peers", "SELECT k, SUM(v) OVER (ORDER BY k) AS s FROM pn ORDER BY k;", True, False),
    ("win-default-peers-desc", "SELECT k, SUM(v) OVER (ORDER BY k DESC) AS s FROM pn ORDER BY k DESC;", True, False),
    ("win-default-peers-nulls", "SELECT x, SUM(x) OVER (ORDER BY x) AS s FROM nn ORDER BY x NULLS LAST;", True, False),
    ("range-offsets", "SELECT k, SUM(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM pn ORDER BY k;", True, False),
    ("range-current-following", "SELECT k, SUM(v) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS s FROM pn ORDER BY k;", True, False),
    ("range-desc", "SELECT k, SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM pn ORDER BY k DESC;", True, False),
    ("range-unbounded", "SELECT k, SUM(v) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s FROM pn ORDER BY k;", True, False),
    ("range-double-keys", "SELECT k, SUM(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM dk ORDER BY k;", True, False),
    ("range-date-keys", "SELECT hired, SUM(id) OVER (ORDER BY hired RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM emp ORDER BY hired NULLS LAST;", True, False),
    ("range-unbounded-noorder", "SELECT id, SUM(id) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM emp ORDER BY id;", True, False),
    ("show-tables", "SHOW TABLES;", True, False),
    ("describe-table", "DESCRIBE emp;", True, False),
    ("describe-view", "DESCRIBE emp_view;", True, False),
    ("describe-all-types", "DESCRIBE typed;", True, False),
    ("describe-empty-names", "DESCRIBE jz;", True, False),
    # --- wave 41: declarative constraints, DESCRIBE keys + FK joins ---
    ("describe-keys", "DESCRIBE keys_p;", True, False),
    ("describe-keys-fk", "DESCRIBE keys_c;", True, False),
    ("fk-join-elim", "SELECT c.cid FROM keys_c c JOIN keys_p p ON c.pid = p.id ORDER BY 1;", True, False),
    ("fk-join-parent-col", "SELECT c.cid, p.v FROM keys_c c JOIN keys_p p ON c.pid = p.id ORDER BY 1;", True, False),
    ("fk-nullable-join", "SELECT n.cid FROM fk_null_c n JOIN keys_p p ON n.pid = p.id ORDER BY 1;", True, False),
    ("fk-dangling-ddl", "CREATE TABLE fk_bad_ddl (x INTEGER REFERENCES no_such_table(id));", False, True),
    ("left-join-non-equi", "SELECT a.x AS ax, b.x AS bx FROM jx a LEFT JOIN jy b ON a.x > b.x ORDER BY ax NULLS LAST, bx NULLS LAST;", True, False),
    ("right-join-non-equi", "SELECT a.x AS ax, b.x AS bx FROM jx a RIGHT JOIN jy b ON a.x > b.x ORDER BY bx NULLS LAST, ax NULLS LAST;", True, False),
    ("full-join-non-equi", "SELECT a.x AS ax, b.x AS bx FROM jx a FULL JOIN jy b ON a.x > b.x ORDER BY ax NULLS LAST, bx NULLS LAST;", True, False),
    ("intersect-double", "SELECT 1 AS v INTERSECT SELECT 1.0 AS v;", True, False),
    ("offset-past-end", "SELECT x FROM nn ORDER BY x OFFSET 100;", True, False),
    ("win-nulls-first", "SELECT x, SUM(x) OVER (ORDER BY x NULLS FIRST) AS s FROM nn ORDER BY x NULLS FIRST;", True, False),
    ("ctas-empty", "CREATE TABLE t_empty AS SELECT x FROM jx WHERE false; SELECT COUNT(*) FROM t_empty;", False, False),
    ("range-zero-offset", "SELECT k, SUM(v) OVER (ORDER BY k RANGE BETWEEN 0 PRECEDING AND CURRENT ROW) AS s FROM pn ORDER BY k;", True, False),
    ("setop-cte-head", "WITH t AS (SELECT x FROM jx) SELECT x FROM t INTERSECT SELECT 2 AS x ORDER BY 1;", True, False),
    ("bad-cross-on", "SELECT * FROM jx CROSS JOIN jy ON jx.x = jy.x;", False, True),
    ("bad-range-no-order", "SELECT x, SUM(x) OVER (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM nn;", False, True),
    ("bad-range-varchar", "SELECT x, SUM(x) OVER (ORDER BY CAST(x AS VARCHAR) RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM nn;", False, True),
    ("bad-count-distinct-star", "SELECT COUNT(DISTINCT *) FROM nn;", False, True),
    # --- wave 62: DECIMAL / INTERVAL ---
    ("dec-add", "SELECT CAST('12.34' AS DECIMAL(6,2)) + CAST('1.66' AS DECIMAL(6,2));", False, False),
    ("dec-display", "SELECT CAST('5' AS DECIMAL(6,2)), CAST('-0.05' AS DECIMAL(6,2)), CAST('12.345' AS DECIMAL(6,1));", False, False),
    ("dec-mul-div-mod", "SELECT CAST('1.5' AS DECIMAL(4,1)) * CAST('2.0' AS DECIMAL(4,1)), CAST('1.0' AS DECIMAL(4,1)) / CAST('3.0' AS DECIMAL(4,1)), CAST('6.0' AS DECIMAL(4,1)) % CAST('4.0' AS DECIMAL(4,1));", False, False),
    ("dec-cast-cmp", "SELECT CAST(CAST('1.5' AS DECIMAL(4,1)) AS DOUBLE), CAST(CAST('1.5' AS DECIMAL(4,1)) AS VARCHAR), CAST('3.5' AS DECIMAL(4,1)) = 3.5, CAST('3.5' AS DECIMAL(4,1)) > 3.4;", False, False),
    ("dec-agg", "SELECT SUM(x), AVG(x), MIN(x), MAX(x), COUNT(x) FROM dt;", False, False),
    ("interval-lit", "SELECT INTERVAL '1 day 02:00:00', INTERVAL '2 months' - INTERVAL '15 days';", False, False),
    ("interval-arith", "SELECT INTERVAL '1 day' * 2, INTERVAL '1 day' / 2;", False, False),
    ("interval-date", "SELECT DATE '2024-01-31' + INTERVAL '1 month', TIMESTAMP '2024-03-31 10:00:00' + INTERVAL '2 days 3 hours';", False, False),
    ("interval-cast-agg", "SELECT CAST('1 day' AS INTERVAL), MIN(iv), MAX(iv) FROM dt;", False, False),
    ("interval-ident", "SELECT 7 AS interval;", False, False),
    # --- wave 52: core json extension (JSON as VARCHAR in v1) ---
    # The default CLI links `pinkdb-json` and loads it into every store, so no
    # `LOAD EXTENSION` is needed; DuckDB has json built in, so both engines
    # answer the same SQL. `json_extract` returns DuckDB's JSON type, which
    # `-json` would render as nested JSON, so probes cast it — the cast's
    # text is the same compact serialization pinkdb returns.
    ("json-extract-nested", "SELECT json_extract('{\"a\": {\"b\" : [1,  2] }}', '$.a')::VARCHAR;", False, False),
    ("json-extract-string-escapes", "SELECT json_extract('{\"a\":\"he said \\\"hi\\\"\"}', '$.a')::VARCHAR, json_extract('{\"a\":\"/\\b\\f\\r\\t\"}', '$.a')::VARCHAR;", False, False),
    ("json-extract-unicode", "SELECT json_extract('{\"a\":\"\\u0041\\u00e9\\uD83D\\uDE00\"}', '$.a')::VARCHAR;", False, False),
    ("json-extract-paths", "SELECT json_extract('[10,20,30]', '$[1]')::VARCHAR, json_extract('[1,2,3]', '$[-1]')::VARCHAR, json_extract('[1,2,3]', '$[-4]')::VARCHAR, json_extract('[1,2]', '$.a')::VARCHAR, json_extract('{\"a\":1}', '$[0]')::VARCHAR;", False, False),
    ("json-extract-quoted-keys", "SELECT json_extract('{\"a b\": 3}', '$.\"a b\"')::VARCHAR, json_extract('{\"a.b\":1}', '$.\"a.b\"')::VARCHAR, json_extract('{\"a.b\":1}', '$.a.b')::VARCHAR;", False, False),
    ("json-extract-duplicates", "SELECT json_extract('{\"a\":1,\"b\":2,\"a\":3}', '$')::VARCHAR, json_extract('{\"a\":1,\"a\":2}', '$.a')::VARCHAR;", False, False),
    ("json-extract-root-whitespace", "SELECT json_extract('[true,false,null,1,\"s\"]', '$')::VARCHAR, json_extract(' {\"a\" : 1 } ', '$')::VARCHAR;", False, False),
    ("json-extract-null-args", "SELECT json_extract(NULL, '$.a')::VARCHAR, json_extract('{\"a\":1}', NULL)::VARCHAR;", False, False),
    ("json-extract-numbers", "SELECT json_extract('{\"a\": 1.50}', '$.a')::VARCHAR, json_extract('{\"a\": 1e2}', '$.a')::VARCHAR, json_extract('{\"a\":-0.0}', '$.a')::VARCHAR, json_extract('[-0, 1E+3, 0.0, 1.0]', '$')::VARCHAR, json_extract('[0.30000000000000004,9.9e20,1e-7,5e-324]', '$')::VARCHAR;", False, False),
    ("json-extract-wide-numbers", "SELECT json_extract('{\"a\":18446744073709551615}', '$.a')::VARCHAR, json_extract('{\"a\":18446744073709551616}', '$.a')::VARCHAR, json_extract('{\"a\":1e400}', '$.a')::VARCHAR, json_extract('{\"a\":1e-400}', '$.a')::VARCHAR;", False, False),
    ("json-extract-nested-arrays", "SELECT json_extract('{\"a\":[{\"b\":true}]}', '$.a[0].b')::VARCHAR, json_extract('{\"a\":[{\"b\":[1,2]}]}', '$.a[-1].b[-2]')::VARCHAR;", False, False),
    ("json-value", "SELECT json_value('{\"a\":1}', '$.a'), json_value('{\"a\":1.5}', '$.a'), json_value('{\"a\":true}', '$.a'), json_value('{\"a\":\"x\"}', '$.a'), json_value('{\"a\":null}', '$.a'), json_value('{\"a\":[1]}', '$.a'), json_value('{\"a\":{\"b\":1}}', '$.a'), json_value('{\"a\":1}', '$.missing'), json_value(NULL, '$.a');", False, False),
    ("json-valid", "SELECT json_valid('{\"a\":1}'), json_valid('  {\"a\":1}  '), json_valid('{\"a\":1,}'), json_valid('nan'), json_valid('Infinity'), json_valid('1e400'), json_valid('{'), json_valid(''), json_valid(NULL), json_valid('01'), json_valid('{\"a\":1} trailing');", False, False),
    ("json-valid-escapes", "SELECT json_valid('\"a\\u0041b\"'), json_valid('\"a\\u12\"'), json_valid('\"a\\x41b\"'), json_valid('\"\\uD800\"');", False, False),
    ("json-array-length", "SELECT json_array_length('[]'), json_array_length('[1,2,3]'), json_array_length('{\"a\":[1,2]}', '$.a'), json_array_length('{}'), json_array_length('1'), json_array_length('[]', '$.missing'), json_array_length(NULL), json_array_length('[]', NULL);", False, False),
    ("json-object", "SELECT json_object('a','1','b','x')::VARCHAR, json_object()::VARCHAR, json_object('a',NULL,'b','1')::VARCHAR, json_object('a','1','a','2')::VARCHAR;", False, False),
    ("json-array", "SELECT json_array('x','y')::VARCHAR, json_array()::VARCHAR, json_array(NULL,'1')::VARCHAR, json_array('{\"a\":1}')::VARCHAR;", False, False),
    ("json-column-extract", "SELECT json_extract(doc, '$.t')::VARCHAR, json_value(doc, '$.n') FROM jdocs ORDER BY id;", True, False),
    ("json-where-length", "SELECT id FROM jdocs WHERE json_array_length(doc, '$.t') > 1 ORDER BY id;", True, False),
    ("json-group-length", "SELECT json_array_length(doc, '$.t') AS n, COUNT(*) FROM jdocs GROUP BY n ORDER BY n;", True, False),
    ("json-compose-builders", "SELECT json_object('n', json_value(doc, '$.n'))::VARCHAR FROM jdocs WHERE id = 1;", True, False),
    # error cases: both engines must refuse (DuckDB at bind time, pinkdb at
    # execution time for the callback-driven ones — both are errors).
    ("json-extract-invalid", "SELECT json_extract('{', '$');", False, True),
    ("json-extract-bad-path", "SELECT json_extract('{\"a\":1}', '$.a.');", False, True),
    ("json-object-odd-args", "SELECT json_object('a');", False, True),
    ("json-object-null-key", "SELECT json_object(CAST(NULL AS VARCHAR), 'v');", False, True),
]

# ---- wave 53: core string extension (`extensions/string`) ----
#
# These probes need the loadable extension on the pinkdb side only; DuckDB
# has the same functions built in. Build it first (`make -C extensions/string`,
# or `cargo build --release -p pinkdb-ext-string`), then the pinkdb side of
# every probe is prefixed with `LOAD EXTENSION '<abs path>'` — engine-specific
# setup outside the compared SQL, so both engines still run the SAME text.
#
# ABI v1 inserts no implicit casts for extension arguments, so BIGINT
# parameters are spelled `CAST(n AS BIGINT)` (DuckDB accepts that too).
STRING_EXT_SO = ".dylib" if sys.platform == "darwin" else ".so"
STRING_EXT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "target",
    "release",
    "libstring_ext" + STRING_EXT_SO,
)
STRING_EXT_SETUP = f"LOAD EXTENSION '{STRING_EXT_PATH}';"

# (label, sql, ordered, expect_error) — same shape as QUERIES.
STRING_EXT_QUERIES = [
    ("str-split-part", "SELECT split_part('a,b,c', ',', CAST(1 AS BIGINT)), split_part('a,b,c', ',', CAST(3 AS BIGINT)), split_part('a,b,c', ',', CAST(4 AS BIGINT)), split_part('a,b,c', ',', CAST(0 AS BIGINT)), split_part('a,b,c', ',', CAST(-1 AS BIGINT)), split_part('a,b,c', ',', CAST(-3 AS BIGINT)), split_part('a,b,c', ',', CAST(-4 AS BIGINT)), split_part('abc', '', CAST(2 AS BIGINT)), split_part('héllo,world', ',', CAST(1 AS BIGINT));", False, False),
    ("str-split-part-empty", "SELECT split_part('a', ',', CAST(1 AS BIGINT)), split_part('a', ',', CAST(2 AS BIGINT)), split_part('', ',', CAST(1 AS BIGINT)), split_part('', '', CAST(1 AS BIGINT));", False, False),
    ("str-split-part-null", "SELECT split_part(NULL, ',', CAST(1 AS BIGINT)) IS NULL, split_part('a,b', NULL, CAST(1 AS BIGINT)) IS NULL, split_part('a,b', ',', NULL) IS NULL;", False, False),
    ("str-reverse", "SELECT reverse('hello'), reverse('héllo'), reverse(''), reverse('日本');", False, False),
    ("str-reverse-null", "SELECT reverse(NULL) IS NULL;", False, False),
    ("str-left-right", "SELECT left('hello', CAST(2 AS BIGINT)), left('hello', CAST(0 AS BIGINT)), left('hello', CAST(-2 AS BIGINT)), left('hello', CAST(99 AS BIGINT)), right('hello', CAST(2 AS BIGINT)), right('hello', CAST(0 AS BIGINT)), right('hello', CAST(-2 AS BIGINT)), right('hello', CAST(99 AS BIGINT));", False, False),
    ("str-left-right-unicode", "SELECT left('héllo', CAST(4 AS BIGINT)), right('héllo', CAST(4 AS BIGINT)), left('héllo', CAST(-1 AS BIGINT)), right('héllo', CAST(-1 AS BIGINT));", False, False),
    ("str-left-right-null", "SELECT left(NULL, CAST(1 AS BIGINT)) IS NULL, left('x', NULL) IS NULL, right(NULL, CAST(1 AS BIGINT)) IS NULL, right('x', NULL) IS NULL;", False, False),
    ("str-strpos", "SELECT strpos('hello', 'll'), strpos('hello', 'z'), strpos('hello', ''), strpos('', 'x'), strpos('héllo', 'llo'), strpos('aé b', 'b');", False, False),
    ("str-strpos-null", "SELECT strpos(NULL, 'x') IS NULL, strpos('x', NULL) IS NULL;", False, False),
    ("str-prefix", "SELECT starts_with('hello', 'he'), starts_with('hello', 'el'), starts_with('hello', ''), ends_with('hello', 'lo'), ends_with('hello', 'he'), ends_with('', ''), starts_with('héllo', 'hé');", False, False),
    ("str-prefix-null", "SELECT starts_with(NULL, 'a') IS NULL, starts_with('a', NULL) IS NULL, ends_with(NULL, 'a') IS NULL, ends_with('a', NULL) IS NULL;", False, False),
    ("str-regexp-extract", "SELECT regexp_extract('hello', 'l+', 0, ''), regexp_extract('hello', '(l+)', 1, ''), regexp_extract('abc-123', '([a-z]+)-([0-9]+)', 2, ''), regexp_extract('hello', 'z', 0, ''), regexp_extract('abc', 'a(b)c', 3, ''), regexp_extract('abc', '', 0, '');", False, False),
    ("str-regexp-extract-options", "SELECT regexp_extract('HELLO world', 'hello', 0, 'i'), regexp_extract('HELLO', 'hello', 0, 'ic'), regexp_extract('HELLO', 'hello', 0, 'ci'), regexp_extract('a.b', 'a.b', 0, 'l'), regexp_extract('axb', 'a.b', 0, 'l'), regexp_extract('héllo', '(é)', 1, '');", False, False),
    ("str-regexp-extract-null", "SELECT regexp_extract(NULL, 'a', 0, '') IS NULL, regexp_extract('a', NULL, 0, '') IS NULL, regexp_extract('abc', 'b', NULL, '') = '';", False, False),
    ("str-regexp-matches", "SELECT regexp_matches('hello', 'l', ''), regexp_matches('hello', '^h', ''), regexp_matches('hello', '^e', ''), regexp_matches('hello', 'o$', ''), regexp_matches('abc', '^abc$', ''), regexp_matches('abc', '', ''), regexp_matches('HELLO', 'hello', 'i'), regexp_matches('HELLO', 'hello', 'c');", False, False),
    ("str-regexp-matches-dotall", "SELECT regexp_matches('a\nb', 'a.b', ''), regexp_matches('a\nb', 'a.b', 's'), regexp_matches('a\nb', 'a.b', 'sm'), regexp_matches('a\nb', 'a.b', 'ms'), regexp_matches('a\nb', '(?s)a.b', ''), regexp_matches('a.b', 'a.b', 'l'), regexp_matches('axb', 'a.b', 'l');", False, False),
    ("str-regexp-matches-null", "SELECT regexp_matches(NULL, 'a', '') IS NULL, regexp_matches('a', NULL, '') IS NULL;", False, False),
    ("str-regexp-bad-group", "SELECT regexp_extract('abc', 'a(b)c', 10, '');", False, True),
    ("str-regexp-bad-group-negative", "SELECT regexp_extract('abc', 'a(b)c', -1, '');", False, True),
    ("str-regexp-g-option", "SELECT regexp_matches('a', 'a', 'g');", False, True),
    ("str-regexp-bad-option", "SELECT regexp_matches('a', 'a', 'q');", False, True),
    ("str-regexp-bad-pattern", "SELECT regexp_matches('a', '(', '');", False, True),
    ("str-regexp-null-options", "SELECT regexp_matches('a', 'a', NULL);", False, True),
    ("str-where", "SELECT name FROM emp WHERE starts_with(name, 'a') OR strpos(name, 'ev') > 0 ORDER BY name;", True, False),
    ("str-group", "SELECT left(name, CAST(1 AS BIGINT)) AS initial, COUNT(*) FROM emp GROUP BY initial ORDER BY initial;", True, False),
    ("str-compose", "SELECT upper(reverse(name)) FROM emp ORDER BY 1;", True, False),
]


def run_small():
    print("== small battery ==")
    try:
        ref = subprocess.run(["duckdb", "--version"], capture_output=True, text=True)
        version = (ref.stdout + ref.stderr).strip().splitlines()[0]
        print(f"reference: {version}")
    except (OSError, IndexError):
        pass
    if not os.path.exists(STRING_EXT_PATH):
        print(f"note: string extension not built at {STRING_EXT_PATH}; "
              "run `make -C extensions/string` for the wave-53 probes")
    for label, sql, ordered, expect_error in QUERIES:
        run_case(label, sql, ordered, expect_error, SETUP)
    for label, sql, ordered, expect_error in STRING_EXT_QUERIES:
        run_case(label, sql, ordered, expect_error, SETUP + "\n" + STRING_EXT_SETUP)


def run_case(label, sql, ordered, expect_error, setup):
    got = pink_query(setup, sql)
    if expect_error:
        d = duck_query(SETUP, sql)
        check(label, got[0] == "error" and d[0] == "error",
              f"pink={got[0]} duck={d[0]}")
        return
    if got[0] == "error":
        check(label, False, f"pinkdb error: {got[1]}")
        return
    _, types, prows = got
    d = duck_query(SETUP, sql)
    if d[0] == "error":
        check(label, False, f"duckdb error: {d[1]}")
        return
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
