SELECT l_partkey, count(*) AS c, sum(l_quantity) AS s FROM lineitem GROUP BY l_partkey ORDER BY c DESC, s DESC, l_partkey LIMIT 20
