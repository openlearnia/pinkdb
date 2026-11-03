#!/bin/sh
# Regenerate the DuckLake catalog snapshots `tests/ducklake_ddl_snapshot.rs`
# compares pink-lake's catalogs with. DuckDB's ducklake extension creates a
# fresh catalog on each backend; every `ducklake_*` column (type,
# nullability, default, key) and the creation metadata keys are dumped, plus
# the inlined-data table DuckDB creates for a table holding one column of each
# type pinkdb has (an inlined column's backend type follows its logical type).
#
#   DUCKDB=~/.local/bin/duckdb PG_CONNINFO="host=127.0.0.1 port=55433 user=postgres dbname=x" \
#     crates/pink-lake/tests/fixtures/ducklake-v2.0.0/regen.sh
#
# The Postgres dump needs an empty database it may write to; without
# PG_CONNINFO it is skipped. Taken from DuckDB v2.0.0 (d54cbbe3df).
set -eu
DUCKDB=${DUCKDB:-duckdb}
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
TYPED="CREATE TABLE l.t (b BOOLEAN, i INTEGER, g BIGINT, h HUGEINT, uh UHUGEINT, d DOUBLE,
  n DECIMAL(10,2), v VARCHAR, dt DATE, ts TIMESTAMP, iv INTERVAL, u UUID);
  INSERT INTO l.t VALUES (true, 1, 2, 3, 4, 1.5, 2.25, 'x', DATE '2026-01-01',
  TIMESTAMP '2026-01-01 01:02:03', INTERVAL 1 DAY, '00000000-0000-0000-0000-000000000001');"
"$DUCKDB" -no-agent -c "LOAD ducklake; LOAD sqlite;
  ATTACH 'ducklake:sqlite:$tmp/c.sqlite' AS l (DATA_PATH '$tmp/data/', DATA_INLINING_ROW_LIMIT 10);
  $TYPED" >/dev/null
sqlite3 "$tmp/c.sqlite" "SELECT m.name, p.cid, p.name, p.type, p.\"notnull\", quote(p.dflt_value), p.pk
  FROM sqlite_master m, pragma_table_info(m.name) p
  WHERE m.type = 'table' AND m.name LIKE 'ducklake%' AND m.name NOT GLOB 'ducklake_inlined_data_[0-9]*'
  ORDER BY m.name, p.cid;" >"$here/sqlite-columns.txt"
sqlite3 "$tmp/c.sqlite" "SELECT p.cid, p.name, p.type FROM pragma_table_info('ducklake_inlined_data_1_1') p
  ORDER BY p.cid;" >"$here/sqlite-inlined.txt"
sqlite3 "$tmp/c.sqlite" "SELECT key FROM ducklake_metadata ORDER BY rowid;" >"$here/metadata-keys.txt"
"$DUCKDB" -no-agent -noheader -list -c "SELECT version();" >"$here/duckdb-version.txt"
if [ -n "${PG_CONNINFO:-}" ]; then
  "$DUCKDB" -no-agent -c "LOAD ducklake; LOAD postgres_scanner;
    ATTACH 'ducklake:postgres:$PG_CONNINFO' AS l (DATA_PATH '$tmp/pgdata/', DATA_INLINING_ROW_LIMIT 10);
    $TYPED" >/dev/null
  psql "$PG_CONNINFO" -At -F '|' -c "SELECT table_name, ordinal_position, column_name, data_type,
    is_nullable, coalesce(column_default, 'NULL')
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name LIKE 'ducklake%'
      AND table_name NOT LIKE 'ducklake\\_inlined\\_data\\_%\\_%'
    ORDER BY table_name, ordinal_position;" >"$here/postgres-columns.txt"
  psql "$PG_CONNINFO" -At -F '|' -c "SELECT ordinal_position, column_name, data_type
    FROM information_schema.columns WHERE table_name = 'ducklake_inlined_data_1_1'
    ORDER BY ordinal_position;" >"$here/postgres-inlined.txt"
fi
