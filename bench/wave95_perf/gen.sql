CREATE TABLE n(i INT);
INSERT INTO n VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
CREATE TABLE lineitem(l_orderkey BIGINT, l_partkey BIGINT, l_quantity DOUBLE, l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE, l_shipdate DATE, l_linestatus VARCHAR, l_returnflag VARCHAR);
INSERT INTO lineitem
SELECT a.i * 100000 + b.i * 10000 + c.i * 1000 + d.i * 100 + e.i * 10 + f.i AS l_orderkey,
       (a.i * 7919 + b.i * 104729 + c.i * 1299709 + d.i * 15485863 + e.i * 32452843 + f.i * 49979687) % 200000 AS l_partkey,
       1 + ((a.i + b.i + c.i + d.i + e.i + f.i) % 50) AS l_quantity,
       900 + ((b.i * 331 + c.i * 41 + d.i * 7 + e.i * 3 + f.i) % 90000) AS l_extendedprice,
       ((c.i * 10 + d.i) % 11) / 100.0 AS l_discount,
       ((e.i * 10 + f.i) % 9) / 100.0 AS l_tax,
       DATE '1992-01-01' + ((a.i * 313 + b.i * 29 + c.i * 71 + d.i * 13 + e.i * 17 + f.i * 19) % 2557) AS l_shipdate,
       CASE WHEN (a.i + c.i + e.i) % 2 = 0 THEN 'O' ELSE 'F' END AS l_linestatus,
       CASE WHEN (b.i + d.i + f.i) % 3 = 0 THEN 'R' WHEN (b.i + d.i + f.i) % 3 = 1 THEN 'A' ELSE 'N' END AS l_returnflag
FROM n a CROSS JOIN n b CROSS JOIN n c CROSS JOIN n d CROSS JOIN n e CROSS JOIN n f;
SELECT count(*) FROM lineitem;
.save /tmp/pinkdb-wave95-perf.pink
