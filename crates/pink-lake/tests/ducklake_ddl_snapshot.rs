//! The catalog DDL pink-lake writes, compared exactly against the DuckLake
//! catalog DuckDB v2.0.0 writes.
//!
//! `tests/fixtures/ducklake-v2.0.0/` holds ground truth produced by DuckDB
//! v2.0.0's ducklake extension (417a9117f5) — every `ducklake_*` catalog
//! column, the inlined-data table DuckDB builds for a table with one column of
//! each pinkdb scalar type, and the metadata keys. `regen.sh` in that
//! directory regenerates them; re-run it and commit the result whenever the
//! pinned DuckDB release changes. Regeneration of the fixtures is the only
//! step that needs DuckDB: the tests here read the recorded files.
//!
//! The SQLite section reads the catalog file directly with `rusqlite`. The
//! Postgres section needs a server in `PINKDB_TEST_POSTGRES_URL` (one
//! throwaway database or server, like the other Postgres suites); unset
//! prints one skip line.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use pink_lake::{Commit, Lake};
use pinkdb::{DataTable, DataType, Value, Vector};

/// The one DuckLake catalog version this build writes.
const CATALOG_VERSION: &str = "1.1";

fn fixture(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/ducklake-v2.0.0")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

/// Whole-text equality; on a mismatch, print the differing lines.
fn assert_fixture(label: &str, actual: &str, expected: &str) {
    if actual == expected {
        return;
    }
    let mut diff = String::new();
    let mut lines = 0;
    for (index, (a, e)) in actual.lines().zip(expected.lines()).enumerate() {
        lines = index + 1;
        if a != e {
            diff.push_str(&format!(
                "  line {}:\n    pink-lake: {a}\n    duckdb:    {e}\n",
                index + 1
            ));
        }
    }
    let actual_lines = actual.lines().count();
    let expected_lines = expected.lines().count();
    if actual_lines != expected_lines {
        diff.push_str(&format!(
            "  {actual_lines} line(s), the fixture has {expected_lines} (compared {lines})\n"
        ));
    }
    panic!("{label} does not match the DuckDB v2.0.0 fixture:\n{diff}");
}

/// A one-row, one-chunk vector of `dtype` holding `value`.
fn vector(dtype: DataType, value: Value) -> Vector {
    let mut v = Vector::new_empty(dtype);
    v.push_valid(value).unwrap();
    v
}

/// The table the fixtures describe: one column of each pinkdb scalar type.
///
/// The row goes in as a typed chunk because [`DataTable::append_row`] cannot
/// store a HUGEINT or UHUGEINT cell (`Table::coerce` has no arm for either
/// 128-bit type); the catalog's column types are what the fixtures compare.
fn twelve_type_table() -> Arc<DataTable> {
    let schema = vec![
        ("b".to_string(), DataType::Boolean),
        ("i".to_string(), DataType::Integer),
        ("g".to_string(), DataType::Bigint),
        ("h".to_string(), DataType::Hugeint),
        ("uh".to_string(), DataType::Uhugeint),
        ("d".to_string(), DataType::Double),
        ("n".to_string(), pinkdb::types::decimal_type(10, 2).unwrap()),
        ("v".to_string(), DataType::Varchar),
        ("dt".to_string(), DataType::Date),
        ("ts".to_string(), DataType::Timestamp),
        ("iv".to_string(), DataType::Interval),
        ("u".to_string(), DataType::Uuid),
    ];
    let mut table = DataTable::new("t".to_string(), schema.clone()).unwrap();
    table
        .append_chunk(
            schema
                .into_iter()
                .map(|(_, dtype)| match dtype {
                    DataType::Boolean => vector(dtype, Value::Boolean(true)),
                    DataType::Integer => vector(dtype, Value::Integer(7)),
                    DataType::Bigint => vector(dtype, Value::Bigint(7)),
                    DataType::Hugeint => vector(dtype, Value::Hugeint(7)),
                    DataType::Uhugeint => vector(dtype, Value::Uhugeint(7)),
                    DataType::Double => vector(dtype, Value::Double(2.5)),
                    DataType::Decimal { .. } => vector(
                        dtype,
                        Value::Decimal {
                            value: 225,
                            precision: 10,
                            scale: 2,
                        },
                    ),
                    DataType::Varchar => vector(dtype, Value::Varchar("x".to_string())),
                    DataType::Date => vector(dtype, Value::Date(0)),
                    DataType::Timestamp => vector(dtype, Value::Timestamp(0)),
                    DataType::Interval => vector(
                        dtype,
                        Value::Interval {
                            months: 1,
                            days: 0,
                            micros: 0,
                        },
                    ),
                    DataType::Uuid => vector(dtype, Value::Uuid(7)),
                    other => panic!("unexpected column type {other:?}"),
                })
                .collect(),
        )
        .unwrap();
    Arc::new(table)
}

/// A fresh lake with one inlined row of [`twelve_type_table`]. DuckDB names
/// the table for table 1 at schema version 1 `ducklake_inlined_data_1_1`.
fn lake_with_one_inlined_row(catalog: &str, data: &Path) -> Lake {
    let mut lake = Lake::create_with_catalog(catalog, data.to_str().unwrap()).unwrap();
    let mut commit = Commit::new();
    commit.append("main.t", twelve_type_table());
    lake.commit(&commit).unwrap();
    lake
}

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pink_lake_ducklake_ddl_{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[cfg(feature = "catalog-sqlite")]
#[test]
fn sqlite_catalog_matches_the_duckdb_v2_fixture() {
    let dir = scratch("sqlite");
    let catalog = dir.join("catalog.sqlite");
    let data = dir.join("data");
    lake_with_one_inlined_row(&format!("sqlite:{}", catalog.display()), &data);

    let conn = rusqlite::Connection::open(&catalog).unwrap();
    let mut statement = conn
        .prepare(
            "SELECT m.name, p.cid, p.name, p.type, p.\"notnull\", quote(p.dflt_value), p.pk
             FROM sqlite_master m, pragma_table_info(m.name) p
             WHERE m.type = 'table' AND m.name LIKE 'ducklake%'
               AND m.name NOT GLOB 'ducklake_inlined_data_[0-9]*'
             ORDER BY m.name, p.cid;",
        )
        .unwrap();
    let columns: Vec<String> = statement
        .query_map([], |row| {
            Ok(format!(
                "{}|{}|{}|{}|{}|{}|{}",
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_fixture(
        "the SQLite catalog columns",
        &format!("{}\n", columns.join("\n")),
        &fixture("sqlite-columns.txt"),
    );

    let inlined: Vec<String> = conn
        .prepare(
            "SELECT p.cid, p.name, p.type FROM pragma_table_info('ducklake_inlined_data_1_1') p
             ORDER BY p.cid;",
        )
        .unwrap()
        .query_map([], |row| {
            Ok(format!(
                "{}|{}|{}",
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_fixture(
        "the SQLite inlined-data columns",
        &format!("{}\n", inlined.join("\n")),
        &fixture("sqlite-inlined.txt"),
    );

    let keys: Vec<String> = conn
        .prepare("SELECT key FROM ducklake_metadata ORDER BY rowid;")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_fixture(
        "the SQLite metadata keys",
        &format!("{}\n", keys.join("\n")),
        &fixture("metadata-keys.txt"),
    );

    let version: String = conn
        .query_row(
            "SELECT value FROM ducklake_metadata WHERE key = 'version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        version, CATALOG_VERSION,
        "pink-lake writes the DuckDB v2.0.0 catalog version"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(feature = "catalog-postgres")]
mod postgres_fixture {
    use super::*;

    use postgres::NoTls;
    use postgres::config::SslMode;

    /// Drops the test schema even when an assertion fails.
    struct DropSchema {
        url: String,
        schema: String,
    }

    impl Drop for DropSchema {
        fn drop(&mut self) {
            let Ok(mut config) = self.url.parse::<postgres::Config>() else {
                return;
            };
            config.ssl_mode(SslMode::Disable);
            if let Ok(mut client) = config.connect(NoTls) {
                let _ = client.batch_execute(&format!(
                    "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                    self.schema
                ));
            }
        }
    }

    fn query_lines(url: &str, schema: &str, sql: &str) -> String {
        let mut config = url
            .parse::<postgres::Config>()
            .expect("PINKDB_TEST_POSTGRES_URL");
        config.ssl_mode(SslMode::Disable);
        let mut client = config.connect(NoTls).expect("the test server");
        client
            .batch_execute(&format!("SET search_path TO \"{schema}\""))
            .unwrap();
        let rows = client.query(sql, &[]).unwrap();
        let mut out = String::new();
        for row in rows {
            let mut cells = Vec::new();
            for (index, column) in row.columns().iter().enumerate() {
                let cell: Option<String> = match *column.type_() {
                    postgres::types::Type::INT4 => {
                        row.get::<_, Option<i32>>(index).map(|v| v.to_string())
                    }
                    _ => row.get::<_, Option<String>>(index),
                };
                cells.push(cell.unwrap_or_else(|| "NULL".to_string()));
            }
            out.push_str(&cells.join("|"));
            out.push('\n');
        }
        out
    }

    #[test]
    fn postgres_catalog_matches_the_duckdb_v2_fixture() {
        let Some(base) = std::env::var("PINKDB_TEST_POSTGRES_URL").ok() else {
            println!(
                "Postgres catalog test postgres_catalog_matches_the_duckdb_v2_fixture: skipped (PINKDB_TEST_POSTGRES_URL unset)"
            );
            return;
        };
        let schema = format!(
            "ducklake_ddl_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let separator = if base.contains('?') { '&' } else { '?' };
        let url = format!("{base}{separator}schema={schema}");
        let cleanup = DropSchema {
            url: base.clone(),
            schema: schema.clone(),
        };

        let dir = scratch("postgres");
        let data = dir.join("data");
        lake_with_one_inlined_row(&url, &data);

        let columns = query_lines(
            &base,
            &schema,
            "SELECT table_name, ordinal_position, column_name, data_type, is_nullable,
                    coalesce(column_default, 'NULL')
             FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name LIKE 'ducklake%'
               AND table_name NOT LIKE 'ducklake\\_inlined\\_data\\_%\\_%'
             ORDER BY table_name, ordinal_position;",
        );
        assert_fixture(
            "the Postgres catalog columns",
            &columns,
            &fixture("postgres-columns.txt"),
        );

        let inlined = query_lines(
            &base,
            &schema,
            "SELECT ordinal_position, column_name, data_type
             FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = 'ducklake_inlined_data_1_1'
             ORDER BY ordinal_position;",
        );
        assert_fixture(
            "the Postgres inlined-data columns",
            &inlined,
            &fixture("postgres-inlined.txt"),
        );

        // Postgres has no rowid, so the keys are compared as a set.
        let mut keys: Vec<String> = query_lines(
            &base,
            &schema,
            "SELECT key FROM ducklake_metadata ORDER BY key;",
        )
        .lines()
        .map(str::to_string)
        .collect();
        keys.sort();
        let mut expected: Vec<String> = fixture("metadata-keys.txt")
            .lines()
            .map(str::to_string)
            .collect();
        expected.sort();
        assert_eq!(
            keys, expected,
            "the Postgres metadata keys are the DuckDB v2.0.0 fixture's"
        );

        let version = query_lines(
            &base,
            &schema,
            "SELECT value FROM ducklake_metadata WHERE key = 'version';",
        );
        assert_eq!(
            version.trim_end(),
            CATALOG_VERSION,
            "pink-lake writes the DuckDB v2.0.0 catalog version"
        );

        drop(cleanup);
        std::fs::remove_dir_all(&dir).ok();
    }
}
