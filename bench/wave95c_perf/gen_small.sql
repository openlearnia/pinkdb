-- The same shape at a hundredth of the size (100 000 target rows, 10 000
-- source rows): the pre-change binary's nested loop is quadratic, so it is
-- measured here and the 1 000 000 x 100 000 run stays for the hash join.
CREATE TABLE n(i INT);
INSERT INTO n VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t
SELECT a.i * 10000 + b.i * 1000 + c.i * 100 + d.i * 10 + e.i AS id,
       (a.i + b.i + c.i + d.i + e.i) % 1000 AS v
FROM n a CROSS JOIN n b CROSS JOIN n c CROSS JOIN n d CROSS JOIN n e;
CREATE TABLE s(id INTEGER, v INTEGER);
INSERT INTO s
SELECT (a.i * 1000 + b.i * 100 + c.i * 10 + d.i) * 10 AS id,
       (a.i + b.i + c.i + d.i) % 100 AS v
FROM n a CROSS JOIN n b CROSS JOIN n c CROSS JOIN n d;
SELECT count(*) FROM t;
SELECT count(*) FROM s;
.save /tmp/pinkdb-wave95c-perf-small.pink
