//! Wave 131c: DuckDB-compatible catalog functions, `information_schema`
//! views and `SHOW ALL TABLES` over pinkdb's schemas.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{
    AttachedLake, Connection, DataTable, DataType, Database, DatabaseOpener, LakeContent,
    LakeDatabase, LakeOpenRequest, PinkDb, QueryResult, SyncPolicy, Value,
};

static SEQ: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    dir: PathBuf,
    file: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_catalog_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("db.pink");
        Self { dir, file }
    }

    fn path(&self) -> &str {
        self.file.to_str().unwrap()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn open(scratch: &Scratch) -> PinkDb {
    PinkDb::open_durable_with(scratch.path(), SyncPolicy::Batch)
        .unwrap()
        .0
}

fn run(db: &mut PinkDb, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn result(db: &mut PinkDb, sql: &str) -> QueryResult {
    db.execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .pop()
        .unwrap()
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match result(db, sql) {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn strings(db: &mut PinkDb, sql: &str) -> Vec<String> {
    rows(db, sql)
        .into_iter()
        .map(|row| match &row[0] {
            Value::Varchar(s) => s.clone(),
            other => panic!("expected a string, got {other:?}"),
        })
        .collect()
}

fn conn_rows(conn: &mut Connection, sql: &str) -> Vec<Vec<Value>> {
    match conn
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .pop()
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn conn_strings(conn: &mut Connection, sql: &str) -> Vec<String> {
    conn_rows(conn, sql)
        .into_iter()
        .map(|row| match &row[0] {
            Value::Varchar(s) => s.clone(),
            other => panic!("expected a string, got {other:?}"),
        })
        .collect()
}

fn fixture(db: &mut PinkDb) {
    run(db, "CREATE DATABASE alpha;");
    run(db, "CREATE DATABASE beta;");
    run(db, "USE alpha;");
    run(db, "CREATE SCHEMA s;");
    run(
        db,
        "CREATE TABLE alpha.main.m (x INTEGER PRIMARY KEY, y VARCHAR);",
    );
    run(
        db,
        "CREATE TABLE s.t (a INTEGER PRIMARY KEY, b VARCHAR NOT NULL DEFAULT 'z', c DECIMAL(18,3));",
    );
    run(db, "CREATE INDEX t_b ON s.t(b);");
    run(db, "CREATE VIEW s.v AS SELECT a, b FROM s.t;");
    run(db, "CREATE MACRO s.plus1(a) AS a + 1;");
    run(db, "USE beta;");
    run(db, "CREATE SCHEMA s;");
    run(db, "CREATE TABLE s.u (id BIGINT, at TIMESTAMP);");
    run(db, "USE alpha;");
}

#[test]
fn catalog_functions_report_duckdb_shape_and_join_on_oids() {
    let mut db = PinkDb::new();
    fixture(&mut db);
    assert_eq!(
        strings(
            &mut db,
            "SELECT database_name FROM duckdb_databases() ORDER BY 1"
        ),
        vec!["alpha", "beta", "main", "system"]
    );
    let db_row = rows(
        &mut db,
        "SELECT database_name, database_oid, path, internal, type, readonly FROM duckdb_databases() \
         WHERE database_name = 'alpha'",
    );
    assert_eq!(db_row.len(), 1);
    assert_eq!(db_row[0][0], Value::Varchar("alpha".into()));
    assert_eq!(db_row[0][1], Value::Bigint(1));
    assert_eq!(db_row[0][2], Value::Null);
    assert_eq!(db_row[0][3], Value::Boolean(false));
    assert_eq!(db_row[0][4], Value::Varchar("pinkdb".into()));
    assert_eq!(db_row[0][5], Value::Boolean(false));

    assert_eq!(
        strings(
            &mut db,
            "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'alpha' ORDER BY 1"
        ),
        vec!["main", "s"]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT schema_name, internal FROM duckdb_schemas() WHERE database_name = 'system'              ORDER BY 1"
        ),
        vec![
            vec![
                Value::Varchar("information_schema".into()),
                Value::Boolean(true)
            ],
            vec![Value::Varchar("main".into()), Value::Boolean(true)],
            vec![Value::Varchar("pg_catalog".into()), Value::Boolean(true)],
        ]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT schema_name FROM duckdb_schemas() WHERE schema_name IN \
             ('information_schema', 'pg_catalog') AND database_name <> 'system'"
        ),
        Vec::<String>::new()
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT database_name, path, internal, type, readonly FROM duckdb_databases() \
             WHERE database_name = 'system'"
        ),
        vec![vec![
            Value::Varchar("system".into()),
            Value::Null,
            Value::Boolean(true),
            Value::Varchar("pinkdb".into()),
            Value::Boolean(false),
        ]]
    );

    let tables = rows(
        &mut db,
        "SELECT schema_name, table_name, column_count, has_primary_key, estimated_size, \
                index_count, internal, temporary FROM duckdb_tables() \
         WHERE database_name = 'alpha' ORDER BY schema_name, table_name",
    );
    assert_eq!(
        tables,
        vec![
            vec![
                Value::Varchar("main".into()),
                Value::Varchar("m".into()),
                Value::Bigint(2),
                Value::Boolean(true),
                Value::Bigint(0),
                Value::Bigint(0),
                Value::Boolean(false),
                Value::Boolean(false),
            ],
            vec![
                Value::Varchar("s".into()),
                Value::Varchar("t".into()),
                Value::Bigint(3),
                Value::Boolean(true),
                Value::Bigint(0),
                Value::Bigint(1),
                Value::Boolean(false),
                Value::Boolean(false),
            ],
        ]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'alpha' \
             AND schema_name = 's' ORDER BY 1"
        ),
        vec!["t"]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT view_name, column_count, is_bound, internal FROM duckdb_views() \
             WHERE database_name = 'alpha'"
        ),
        vec![vec![
            Value::Varchar("v".into()),
            Value::Bigint(2),
            Value::Boolean(true),
            Value::Boolean(false),
        ]]
    );

    assert_eq!(
        rows(
            &mut db,
            "SELECT \"database\", \"schema\", name, kind FROM pinkdb_catalog() \
             WHERE kind = 'SCHEMA' AND \"database\" = 'alpha' ORDER BY \"schema\""
        ),
        vec![
            vec![
                Value::Varchar("alpha".into()),
                Value::Varchar("main".into()),
                Value::Varchar("main".into()),
                Value::Varchar("SCHEMA".into()),
            ],
            vec![
                Value::Varchar("alpha".into()),
                Value::Varchar("s".into()),
                Value::Varchar("s".into()),
                Value::Varchar("SCHEMA".into()),
            ],
        ]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT \"schema\" FROM pinkdb_catalog() WHERE kind = 'TABLE' AND name = 't'"
        ),
        vec!["s"]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT viewname FROM pg_catalog.pg_views WHERE schemaname = 's' ORDER BY 1"
        ),
        vec!["v"]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT nspname FROM pg_catalog.pg_namespace ORDER BY 1"
        ),
        vec!["main", "s"]
    );

    // The OIDs are shared by the functions of one statement.
    assert_eq!(
        rows(
            &mut db,
            "SELECT count(*) FROM duckdb_schemas() s \
             JOIN duckdb_databases() d ON s.database_oid = d.database_oid"
        ),
        vec![vec![Value::Bigint(8)]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT count(*) FROM duckdb_tables() t \
             JOIN duckdb_schemas() s ON t.schema_oid = s.oid AND t.database_oid = s.database_oid"
        ),
        vec![vec![Value::Bigint(3)]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT count(*) FROM duckdb_columns() c \
             JOIN duckdb_tables() t ON c.table_oid = t.table_oid \
             AND c.database_oid = t.database_oid"
        ),
        vec![vec![Value::Bigint(7)]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT count(*) FROM duckdb_views() v \
             JOIN duckdb_columns() c ON c.table_oid = v.view_oid \
             AND c.database_oid = v.database_oid"
        ),
        vec![vec![Value::Bigint(2)]]
    );
}

#[test]
fn catalog_columns_use_duckdb_spelling_and_metadata() {
    let mut db = PinkDb::new();
    fixture(&mut db);
    let cols = rows(
        &mut db,
        "SELECT column_name, column_index, data_type, data_type_id, is_nullable, \
                column_default, numeric_precision, numeric_precision_radix, numeric_scale \
         FROM duckdb_columns() WHERE database_name = 'alpha' AND schema_name = 's' \
         AND table_name = 't' ORDER BY column_index",
    );
    assert_eq!(
        cols,
        vec![
            vec![
                Value::Varchar("a".into()),
                Value::Integer(1),
                Value::Varchar("INTEGER".into()),
                Value::Bigint(13),
                Value::Boolean(false),
                Value::Null,
                Value::Integer(32),
                Value::Integer(2),
                Value::Integer(0),
            ],
            vec![
                Value::Varchar("b".into()),
                Value::Integer(2),
                Value::Varchar("VARCHAR".into()),
                Value::Bigint(25),
                Value::Boolean(false),
                Value::Varchar("'z'".into()),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Varchar("c".into()),
                Value::Integer(3),
                Value::Varchar("DECIMAL(18,3)".into()),
                Value::Bigint(21),
                Value::Boolean(true),
                Value::Null,
                Value::Integer(18),
                Value::Integer(10),
                Value::Integer(3),
            ],
        ]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT column_name, data_type, is_nullable FROM duckdb_columns() \
             WHERE table_name = 'v' ORDER BY column_index"
        ),
        vec![
            vec![
                Value::Varchar("a".into()),
                Value::Varchar("INTEGER".into()),
                Value::Boolean(true),
            ],
            vec![
                Value::Varchar("b".into()),
                Value::Varchar("VARCHAR".into()),
                Value::Boolean(true),
            ],
        ]
    );
}

#[test]
fn table_sql_is_duckdb_generated_form_and_view_sql_is_stored() {
    let mut db = PinkDb::new();
    fixture(&mut db);
    assert_eq!(
        strings(
            &mut db,
            "SELECT sql FROM duckdb_tables() WHERE schema_name = 's' AND table_name = 't'"
        ),
        vec![
            "CREATE TABLE s.t(a INTEGER, b VARCHAR DEFAULT('z') NOT NULL, c DECIMAL(18,3), \
             PRIMARY KEY(a));"
        ]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT sql FROM duckdb_views() WHERE schema_name = 's' AND view_name = 'v'"
        ),
        vec!["CREATE VIEW s.v AS SELECT a, b FROM s.t"]
    );
}

#[test]
fn information_schema_views_list_every_database_in_all_qualifications() {
    let mut db = PinkDb::new();
    fixture(&mut db);
    assert_eq!(
        strings(
            &mut db,
            "SELECT catalog_name || '.' || schema_name FROM information_schema.schemata \
             ORDER BY 1"
        ),
        vec![
            "alpha.main",
            "alpha.s",
            "beta.main",
            "beta.s",
            "main.main",
            "system.information_schema",
            "system.main",
            "system.pg_catalog",
        ]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT table_catalog, table_schema, table_name, table_type \
             FROM information_schema.tables WHERE table_schema = 's' \
             ORDER BY table_catalog, table_name"
        ),
        vec![
            vec![
                Value::Varchar("alpha".into()),
                Value::Varchar("s".into()),
                Value::Varchar("t".into()),
                Value::Varchar("BASE TABLE".into()),
            ],
            vec![
                Value::Varchar("alpha".into()),
                Value::Varchar("s".into()),
                Value::Varchar("v".into()),
                Value::Varchar("VIEW".into()),
            ],
            vec![
                Value::Varchar("beta".into()),
                Value::Varchar("s".into()),
                Value::Varchar("u".into()),
                Value::Varchar("BASE TABLE".into()),
            ],
        ]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT table_catalog, table_name FROM alpha.information_schema.tables \
             WHERE table_schema = 's' ORDER BY table_catalog, table_name"
        ),
        vec![
            vec![Value::Varchar("alpha".into()), Value::Varchar("t".into()),],
            vec![Value::Varchar("alpha".into()), Value::Varchar("v".into()),],
            vec![Value::Varchar("beta".into()), Value::Varchar("u".into()),],
        ]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT column_name, ordinal_position, data_type FROM information_schema.columns \
             WHERE table_schema = 's' AND table_name = 't' ORDER BY ordinal_position"
        ),
        vec![
            vec![
                Value::Varchar("a".into()),
                Value::Integer(1),
                Value::Varchar("INTEGER".into()),
            ],
            vec![
                Value::Varchar("b".into()),
                Value::Integer(2),
                Value::Varchar("VARCHAR".into()),
            ],
            vec![
                Value::Varchar("c".into()),
                Value::Integer(3),
                Value::Varchar("DECIMAL(18,3)".into()),
            ],
        ]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT table_name FROM information_schema.views WHERE table_schema = 's' \
             ORDER BY 1"
        ),
        vec!["v"]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'alpha' \
             ORDER BY 1"
        ),
        vec!["main", "s"]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT viewname FROM system.pg_catalog.pg_views WHERE schemaname = 's' ORDER BY 1"
        ),
        vec!["v"]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT tablename FROM system.pg_catalog.pg_tables WHERE schemaname = 's' ORDER BY 1"
        ),
        vec!["t", "u"]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT table_name FROM system.information_schema.tables \
             WHERE table_catalog = 'alpha' AND table_schema = 's' ORDER BY 1"
        ),
        vec!["t", "v"]
    );
    // A database named information_schema does not shadow the built-in schema
    // for an unqualified two-part reference.
    run(&mut db, "CREATE DATABASE information_schema;");
    assert_eq!(
        strings(
            &mut db,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 's' \
             AND table_catalog = 'alpha' ORDER BY 1"
        ),
        vec!["t", "v"]
    );
    assert_eq!(
        rows(&mut db, "DESCRIBE information_schema.tables").len(),
        13
    );
}

#[test]
fn show_all_tables_matches_duckdb_columns_and_lists() {
    let mut db = PinkDb::new();
    fixture(&mut db);
    assert_eq!(
        rows(&mut db, "SHOW ALL TABLES"),
        vec![
            vec![
                Value::Varchar("alpha".into()),
                Value::Varchar("main".into()),
                Value::Varchar("m".into()),
                Value::Varchar("[x, y]".into()),
                Value::Varchar("[INTEGER, VARCHAR]".into()),
                Value::Boolean(false),
            ],
            vec![
                Value::Varchar("alpha".into()),
                Value::Varchar("s".into()),
                Value::Varchar("t".into()),
                Value::Varchar("[a, b, c]".into()),
                Value::Varchar("[INTEGER, VARCHAR, 'DECIMAL(18,3)']".into()),
                Value::Boolean(false),
            ],
            vec![
                Value::Varchar("alpha".into()),
                Value::Varchar("s".into()),
                Value::Varchar("v".into()),
                Value::Varchar("[a, b]".into()),
                Value::Varchar("[INTEGER, VARCHAR]".into()),
                Value::Boolean(false),
            ],
            vec![
                Value::Varchar("beta".into()),
                Value::Varchar("s".into()),
                Value::Varchar("u".into()),
                Value::Varchar("[id, at]".into()),
                Value::Varchar("[BIGINT, TIMESTAMP]".into()),
                Value::Boolean(false),
            ],
        ]
    );
    assert_eq!(strings(&mut db, "SHOW TABLES"), vec!["m"]);
}

#[test]
fn catalog_functions_filter_by_select_grants() {
    let shared = Database::from_store(PinkDb::new());
    let mut admin = shared.connect();
    admin
        .execute(
            "CREATE USER alice PASSWORD 'alice-secret'; \
             CREATE SCHEMA s1; CREATE SCHEMA s2; \
             CREATE TABLE s1.t (x INTEGER); CREATE TABLE s2.u (y INTEGER); \
             GRANT SELECT ON SCHEMA s1 TO alice",
        )
        .unwrap();
    let mut alice = shared.connect();
    alice.login("alice", "alice-secret").unwrap();
    assert_eq!(
        conn_strings(
            &mut alice,
            "SELECT schema_name FROM duckdb_schemas() WHERE database_name <> 'system' \
             AND schema_name NOT IN ('information_schema', 'pg_catalog') ORDER BY 1"
        ),
        vec!["s1"]
    );
    assert_eq!(
        conn_strings(
            &mut alice,
            "SELECT table_name FROM duckdb_tables() ORDER BY 1"
        ),
        vec!["t"]
    );
    assert_eq!(
        conn_strings(
            &mut alice,
            "SELECT table_name FROM information_schema.tables ORDER BY 1"
        ),
        vec!["t"]
    );
    assert_eq!(
        conn_strings(
            &mut alice,
            "SELECT table_name FROM duckdb_columns() ORDER BY 1"
        ),
        vec!["t"]
    );
}

#[test]
fn durable_database_path_is_the_store_file() {
    let scratch = Scratch::new("path");
    let mut db = open(&scratch);
    assert_eq!(
        rows(
            &mut db,
            "SELECT path, type FROM duckdb_databases() WHERE database_name = 'main'"
        ),
        vec![vec![
            Value::Varchar(scratch.path().to_string()),
            Value::Varchar("pinkdb".into()),
        ]]
    );
}

#[test]
fn table_sql_includes_foreign_keys() {
    let scratch = Scratch::new("fk");
    let mut db = open(&scratch);
    run(&mut db, "CREATE TABLE p(id INTEGER PRIMARY KEY)");
    run(
        &mut db,
        "CREATE TABLE c(id INTEGER PRIMARY KEY, pid INTEGER, \
         FOREIGN KEY(pid) REFERENCES p(id))",
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT sql FROM duckdb_tables() WHERE table_name = 'c'",
        ),
        vec![
            "CREATE TABLE c(id INTEGER, pid INTEGER, PRIMARY KEY(id), \
             FOREIGN KEY (pid) REFERENCES p(id));"
        ]
    );
}

#[test]
fn show_all_tables_prints_types_like_duckdb_lists() {
    let scratch = Scratch::new("list");
    let mut db = open(&scratch);
    run(
        &mut db,
        "CREATE TABLE t(a INTEGER, d DECIMAL(18,3), k INTEGER[3])",
    );
    let row = rows(&mut db, "SHOW ALL TABLES").pop().unwrap();
    assert_eq!(
        row[3],
        Value::Varchar("[a, d, k]".to_string()),
        "column names"
    );
    assert_eq!(
        row[4],
        Value::Varchar("[INTEGER, 'DECIMAL(18,3)', 'INTEGER[3]']".to_string()),
        "column types"
    );
}

#[test]
fn system_database_is_reserved_and_virtual() {
    let mut db = PinkDb::new();
    let create = db
        .execute("CREATE DATABASE system;")
        .unwrap_err()
        .to_string();
    assert!(create.contains("reserved"), "{create}");
    let attach = db
        .execute("ATTACH ':memory:' AS system;")
        .unwrap_err()
        .to_string();
    assert!(attach.contains("reserved"), "{attach}");
    assert_eq!(
        strings(
            &mut db,
            "SELECT database_name FROM duckdb_databases() ORDER BY 1"
        ),
        vec!["main", "system"]
    );
}

/// A writable lake database with `master.fact` and an empty `staging`, served
/// by a fake opener: pinkdb has no lake implementation of its own, and this
/// file is where the catalog functions over one belong.
#[derive(Debug)]
struct FakeLake {
    location: String,
}

impl LakeDatabase for FakeLake {
    fn location(&self) -> String {
        self.location.clone()
    }
    fn read_only(&self) -> bool {
        false
    }
    fn version(&self) -> u64 {
        1
    }
    fn schemas(&self) -> pinkdb::Result<Vec<String>> {
        Ok(vec![
            "main".to_string(),
            "master".to_string(),
            "staging".to_string(),
        ])
    }
    fn reader(&self) -> pinkdb::Result<Arc<dyn AttachedLake>> {
        Err(pinkdb::PinkDbError::Execution(
            "the fake lake is writable".to_string(),
        ))
    }
    fn content(&self) -> pinkdb::Result<LakeContent> {
        let mut fact = DataTable::new(
            "fact".to_string(),
            vec![("id".to_string(), DataType::Integer)],
        )
        .expect("the fake table's schema");
        fact.append_row(vec![Value::Integer(1)])
            .expect("the fake table's row");
        Ok(LakeContent {
            image: None,
            tables: vec![("master".to_string(), "fact".to_string(), Arc::new(fact))],
        })
    }
    fn publish(
        &self,
        _image: &[u8],
        _schemas: &[String],
        _tables: &[(String, String, Arc<DataTable>)],
    ) -> pinkdb::Result<u64> {
        Ok(1)
    }
    fn tables_at(&self, _version: u64) -> pinkdb::Result<Vec<(String, String, Arc<DataTable>)>> {
        Ok(Vec::new())
    }
}

struct FakeOpener;

impl DatabaseOpener for FakeOpener {
    fn open(&self, request: &LakeOpenRequest) -> pinkdb::Result<Arc<dyn LakeDatabase>> {
        Ok(Arc::new(FakeLake {
            location: request.catalog.clone(),
        }))
    }
}

#[test]
fn catalog_functions_list_a_lake_databases_schemas_and_tables() {
    static REGISTER: std::sync::Once = std::sync::Once::new();
    REGISTER.call_once(|| pinkdb::register_database_opener(Arc::new(FakeOpener)));
    let mut db = PinkDb::new();
    run(
        &mut db,
        "CREATE DATABASE lake (TYPE LAKE, CATALOG 'fake:lake');",
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'lake' ORDER BY 1"
        ),
        vec!["main", "master", "staging"]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'master'"
        ),
        vec!["fact"]
    );
    let listed = rows(
        &mut db,
        "SELECT table_catalog, table_schema, table_name FROM information_schema.tables \
         WHERE table_catalog = 'lake'",
    );
    assert_eq!(
        listed,
        vec![vec![
            Value::Varchar("lake".to_string()),
            Value::Varchar("master".to_string()),
            Value::Varchar("fact".to_string()),
        ]]
    );
    // The empty schema survived the engine image round trip: only the schema
    // list carries it.
    assert!(
        strings(
            &mut db,
            "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'lake'"
        )
        .contains(&"staging".to_string())
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT name FROM pinkdb_catalog() \
             WHERE \"database\" = 'lake' AND \"schema\" = 'master' AND kind = 'TABLE'"
        ),
        vec!["fact"]
    );
    assert_eq!(
        strings(
            &mut db,
            "SELECT column_name FROM information_schema.columns \
             WHERE table_catalog = 'lake' AND table_schema = 'master'"
        ),
        vec!["id"]
    );
}
