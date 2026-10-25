-- A size both binaries finish quickly (10 000 target rows, 1 000 source
-- rows), for the interleaved A/B of the two DML join shapes.
CREATE TABLE n(i INT);
INSERT INTO n VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER);
INSERT INTO t
SELECT a.i * 1000 + b.i * 100 + c.i * 10 + d.i AS id,
       (a.i + b.i + c.i + d.i) % 1000 AS v
FROM n a CROSS JOIN n b CROSS JOIN n c CROSS JOIN n d;
CREATE TABLE s(id INTEGER, v INTEGER);
INSERT INTO s
SELECT (a.i * 100 + b.i * 10 + c.i) * 10 AS id,
       (a.i + b.i + c.i) % 100 AS v
FROM n a CROSS JOIN n b CROSS JOIN n c;
SELECT count(*) FROM t;
SELECT count(*) FROM s;
.save /tmp/pinkdb-wave95c-perf-tiny.pink
