CREATE TABLE n(i INT);
INSERT INTO n VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
-- 1 000 000 keyed rows: the primary key makes every write statement pay for
-- the uniqueness check this wave added.
CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER, g VARCHAR);
INSERT INTO t
SELECT a.i * 100000 + b.i * 10000 + c.i * 1000 + d.i * 100 + e.i * 10 + f.i AS id,
       (a.i * 7 + b.i * 13 + c.i * 17 + d.i * 19 + e.i * 23 + f.i * 29) % 1000 AS v,
       CASE WHEN (b.i + d.i + f.i) % 3 = 0 THEN 'r' WHEN (b.i + d.i + f.i) % 3 = 1 THEN 'a' ELSE 'n' END AS g
FROM n a CROSS JOIN n b CROSS JOIN n c CROSS JOIN n d CROSS JOIN n e CROSS JOIN n f;
SELECT count(*) FROM t;
.save /tmp/pinkdb-wave95b-perf.pink
