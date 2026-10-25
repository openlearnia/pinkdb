CREATE TABLE n(i INT);
INSERT INTO n VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
-- 1 000 000 target rows; the source holds every 10th id (100 000 rows), so a
-- MERGE on `id` matches 100 000 targets and leaves 900 000 unmatched.
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t
SELECT a.i * 100000 + b.i * 10000 + c.i * 1000 + d.i * 100 + e.i * 10 + f.i AS id,
       (a.i + b.i + c.i + d.i + e.i + f.i) % 1000 AS v
FROM n a CROSS JOIN n b CROSS JOIN n c CROSS JOIN n d CROSS JOIN n e CROSS JOIN n f;
CREATE TABLE s(id INTEGER, v INTEGER);
INSERT INTO s
SELECT (a.i * 10000 + b.i * 1000 + c.i * 100 + d.i * 10 + e.i) * 10 AS id,
       (a.i + b.i + c.i + d.i + e.i) % 100 AS v
FROM n a CROSS JOIN n b CROSS JOIN n c CROSS JOIN n d CROSS JOIN n e;
SELECT count(*) FROM t;
SELECT count(*) FROM s;
.save /tmp/pinkdb-wave95c-perf.pink
