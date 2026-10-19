-- The committed parquet fixtures the duckdiff "parquet" battery reads from
-- both engines. Regenerate with the pinned reference:
--
--   duckdb -no-agent < bench/parquet_fixtures/gen_parquet_fixtures.sql
--
-- (run from the repository root; every path is relative to it).

-- people.parquet: the core type surface, a NULL in a string and a double.
CREATE TABLE people AS SELECT
  range AS id,
  CASE WHEN range = 3 THEN NULL ELSE 'name' || range END AS name,
  CASE WHEN range = 5 THEN NULL ELSE CAST(range * 1.5 AS DOUBLE) END AS score,
  DATE '2024-01-01' + CAST(range AS INTEGER) AS hired,
  (range % 2 = 0) AS flag,
  (range / 100.0)::DECIMAL(9,2) AS dec
FROM range(8);
COPY people TO 'bench/parquet_fixtures/people.parquet' (FORMAT parquet);

-- rg_stats.parquet: four row groups of 2048 rows with disjoint id ranges,
-- so a range filter visibly prunes whole row groups.
CREATE TABLE rg_rows AS SELECT CAST(range AS INTEGER) AS id FROM range(8192);
COPY rg_rows TO 'bench/parquet_fixtures/rg_stats.parquet'
  (FORMAT parquet, ROW_GROUP_SIZE 2048);

-- Hive-partitioned files: region=<value> directories.
CREATE TABLE hive_rows AS SELECT range AS id, 'p' || range AS note FROM range(6);
COPY (SELECT * FROM hive_rows WHERE id < 3)
  TO 'bench/parquet_fixtures/hive/region=us/part0.parquet' (FORMAT parquet);
COPY (SELECT * FROM hive_rows WHERE id >= 3)
  TO 'bench/parquet_fixtures/hive/region=eu/part1.parquet' (FORMAT parquet);

-- multi/: two files with different column order, one missing column and one
-- with an extra column (union_by_name, and the schema-mismatch error without).
COPY (SELECT 1 AS id, 'a' AS name, 1.5 AS score)
  TO 'bench/parquet_fixtures/multi/a.parquet' (FORMAT parquet);
COPY (SELECT 'b' AS name, 2 AS id, 2.5 AS score, 7 AS extra)
  TO 'bench/parquet_fixtures/multi/b.parquet' (FORMAT parquet);

-- conflict/: the same column name with incompatible types (DOUBLE vs VARCHAR):
-- union_by_name unifies to VARCHAR and renders the values as text.
CREATE TABLE conflict_ok AS SELECT 1 AS id, 1.5 AS score;
CREATE TABLE conflict_text AS SELECT 2 AS id, 'n/a' AS score;
COPY (SELECT * FROM conflict_ok)
  TO 'bench/parquet_fixtures/conflict/a.parquet' (FORMAT parquet);
COPY (SELECT * FROM conflict_text)
  TO 'bench/parquet_fixtures/conflict/b.parquet' (FORMAT parquet);

-- badglob/: a file that does not have the first file's column. Reading the
-- glob without union_by_name is α's schema-mismatch error (the same fixture
-- with union_by_name reads NULL for the missing column).
CREATE TABLE badglob_base AS SELECT 1 AS id;
CREATE TABLE badglob_other AS SELECT 2 AS other;
COPY (SELECT * FROM badglob_base)
  TO 'bench/parquet_fixtures/badglob/a.parquet' (FORMAT parquet);
COPY (SELECT * FROM badglob_other)
  TO 'bench/parquet_fixtures/badglob/b.parquet' (FORMAT parquet);

-- kv.parquet: key-value metadata.
COPY (SELECT 1 AS id) TO 'bench/parquet_fixtures/kv.parquet'
  (FORMAT parquet, KV_METADATA {owner: 'pinkdb', wave: '70'});

-- gzip.parquet: a gzip-compressed copy of people.
COPY people TO 'bench/parquet_fixtures/gzip.parquet' (FORMAT parquet, COMPRESSION gzip);
