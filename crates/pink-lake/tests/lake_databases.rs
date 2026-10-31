//! Lake databases (wave 124): `CREATE DATABASE … (TYPE LAKE)`, `ATTACH`,
//! `DETACH`, commit routing, pinned replay and unavailable lakes, through the
//! engine with pink-lake's opener registered.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use pink_lake::Lake;
use pinkdb::{Database, DurableOpenOptions, PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// A scratch directory removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pink_lake_dbs_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        pink_lake::database::register();
        Scratch(dir)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }

    fn s(&self, name: &str) -> String {
        self.path(name).to_string_lossy().into_owned()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn ints(db: &mut PinkDb, sql: &str) -> Vec<i64> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows
            .into_iter()
            .map(|r| match &r[0] {
                Value::Integer(v) => *v as i64,
                Value::Bigint(v) => *v,
                v => panic!("not an integer: {v:?}"),
            })
            .collect(),
        r => panic!("expected rows, got {r:?}"),
    }
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql).unwrap_err().to_string()
}

fn open_durable(path: &Path, lakes: &Path) -> PinkDb {
    let opts = DurableOpenOptions {
        lake_dir: Some(lakes.to_path_buf()),
        ..DurableOpenOptions::default()
    };
    PinkDb::open_durable_with_options(path.to_str().unwrap(), opts)
        .unwrap()
        .0
}

fn create_lake_sql(name: &str, scratch: &Scratch, tag: &str) -> String {
    format!(
        "CREATE DATABASE {name} (TYPE LAKE, CATALOG 'pink:{}', DATA '{}');",
        scratch.s(&format!("{tag}.pink")),
        scratch.s(&format!("{tag}.files"))
    )
}

#[test]
fn a_lake_database_publishes_each_commit_and_attaches_read_only_elsewhere() {
    let scratch = Scratch::new("publish");
    let mut db = PinkDb::new();
    db.execute(&create_lake_sql("sales", &scratch, "sales"))
        .unwrap();
    db.execute("CREATE TABLE sales.orders (id INTEGER, amount DOUBLE);")
        .unwrap();
    db.execute("INSERT INTO sales.orders VALUES (1, 9.5), (2, 3.0);")
        .unwrap();
    // Every commit is one lake version: create (1), CREATE TABLE, INSERT.
    let lake = Lake::open_with_catalog(&format!("pink:{}", scratch.s("sales.pink")), None).unwrap();
    assert!(lake.current_version() >= 3, "{}", lake.current_version());

    // A second store reads it through a read-only attachment: the lake's
    // `main` schema is the database's name.
    let mut other = PinkDb::new();
    other
        .execute(&format!(
            "ATTACH 'lake:pink:{}' AS archive (READ_ONLY);",
            scratch.s("sales.pink")
        ))
        .unwrap();
    assert_eq!(
        ints(&mut other, "SELECT id FROM archive.orders ORDER BY id;"),
        vec![1, 2]
    );
    let e = err(&mut other, "INSERT INTO archive.orders VALUES (3, 1.0);");
    assert!(e.contains("read-only lake"), "{e}");
    let e = err(&mut other, "CREATE TABLE archive.x (a INTEGER);");
    assert!(e.contains("read-only lake"), "{e}");
    match other.execute("SHOW DATABASES;").unwrap().pop().unwrap() {
        QueryResult::Selected { rows, names, .. } => {
            assert_eq!(names[1], "storage");
            let archive = rows
                .iter()
                .find(|r| r[0] == Value::Varchar("archive".into()))
                .unwrap();
            assert_eq!(archive[1], Value::Varchar("lake".into()));
            assert_eq!(archive[3], Value::Boolean(true));
            assert_eq!(archive[5], Value::Varchar("ok".into()));
        }
        r => panic!("{r:?}"),
    }
    other.execute("DETACH archive;").unwrap();
    assert!(!other.database_exists("archive"));
    let e = err(&mut other, "SELECT * FROM archive.orders;");
    assert!(e.contains("not found"), "{e}");
    // DETACH never deletes the lake (a `pink:` catalog may still be only its
    // WAL before its first checkpoint).
    assert!(scratch.path("sales.pink").exists() || scratch.path("sales.pink.wal").exists());
}

#[test]
fn a_second_writer_is_refused_by_the_lease() {
    let scratch = Scratch::new("lease");
    let mut a = PinkDb::new();
    a.execute(&create_lake_sql("ev", &scratch, "ev")).unwrap();
    let mut b = PinkDb::new();
    let e = err(
        &mut b,
        &format!("ATTACH 'lake:pink:{}' AS ev;", scratch.s("ev.pink")),
    );
    assert!(e.contains("already served by"), "{e}");
    // Read-only attachments take no lease.
    b.execute(&format!(
        "ATTACH 'lake:pink:{}' AS ev (READ_ONLY);",
        scratch.s("ev.pink")
    ))
    .unwrap();
    // The same lake twice in one store is refused before any lease.
    let e = err(
        &mut a,
        &format!(
            "ATTACH 'lake:pink:{}' AS again (READ_ONLY);",
            scratch.s("ev.pink")
        ),
    );
    assert!(e.contains("already attached as database"), "{e}");
    // DETACH releases the lease.
    a.execute("CREATE DATABASE other;").unwrap();
    a.execute("DETACH ev;").unwrap();
    let mut c = PinkDb::new();
    c.execute(&format!(
        "ATTACH 'lake:pink:{}' AS ev;",
        scratch.s("ev.pink")
    ))
    .unwrap();
}

#[test]
fn a_durable_store_reopens_its_lake_databases() {
    let scratch = Scratch::new("reopen");
    let file = scratch.path("store.pink");
    let lakes = scratch.path("lakes");
    {
        let mut db = open_durable(&file, &lakes);
        // A managed lake under the lake directory.
        db.execute("CREATE DATABASE scratch (TYPE LAKE);").unwrap();
        db.execute("CREATE TABLE scratch.t (a INTEGER PRIMARY KEY, b VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO scratch.t (a) VALUES (1), (2), (3);")
            .unwrap();
        db.execute("CREATE MACRO scratch.twice(x) AS x * 2;")
            .unwrap();
        db.execute("CREATE TABLE main.local (a INTEGER);").unwrap();
        db.execute("INSERT INTO main.local VALUES (10);").unwrap();
    }
    let catalog = lakes.join("scratch").join("catalog.pink");
    assert!(catalog.exists() || lakes.join("scratch").join("catalog.pink.wal").exists());
    {
        // Reopen from the WAL (no checkpoint yet).
        let mut db = open_durable(&file, &lakes);
        assert_eq!(
            ints(&mut db, "SELECT a FROM scratch.t ORDER BY a;"),
            vec![1, 2, 3]
        );
        db.execute("USE scratch;").unwrap();
        assert_eq!(
            ints(&mut db, "SELECT twice(a) FROM t WHERE a = 3;"),
            vec![6]
        );
        db.execute("USE main;").unwrap();
        assert_eq!(ints(&mut db, "SELECT a FROM main.local;"), vec![10]);
        db.execute("INSERT INTO scratch.t (a) VALUES (4);").unwrap();
        db.execute("CHECKPOINT;").unwrap();
    }
    {
        // Reopen from the checkpoint: the footer names the lake database.
        let mut db = open_durable(&file, &lakes);
        assert_eq!(
            ints(&mut db, "SELECT a FROM scratch.t ORDER BY a;"),
            vec![1, 2, 3, 4]
        );
        // The primary key came back from the database image.
        match db.execute("DESCRIBE scratch.t;").unwrap().pop().unwrap() {
            QueryResult::Selected { rows, .. } => {
                let text = format!("{rows:?}");
                assert!(text.contains("PRI"), "{text}");
            }
            r => panic!("{r:?}"),
        }
        let e = err(&mut db, "DROP DATABASE scratch;");
        assert!(e.contains("DETACH"), "{e}");
    }
}

#[test]
fn a_unit_writes_one_storage() {
    let scratch = Scratch::new("mixed");
    let mut db = PinkDb::new();
    db.execute(&create_lake_sql("lk", &scratch, "lk")).unwrap();
    db.execute("CREATE TABLE lk.t (a INTEGER);").unwrap();
    db.execute("CREATE TABLE main.t (a INTEGER);").unwrap();
    // A transaction that writes both storages is refused at the second.
    db.execute("BEGIN;").unwrap();
    db.execute("INSERT INTO main.t VALUES (1);").unwrap();
    let e = err(&mut db, "INSERT INTO lk.t VALUES (1);");
    assert!(e.contains("not both"), "{e}");
    db.execute("COMMIT;").unwrap();
    assert_eq!(ints(&mut db, "SELECT count(*) FROM lk.t;"), vec![0]);
    assert_eq!(ints(&mut db, "SELECT count(*) FROM main.t;"), vec![1]);

    // A trigger that writes the other storage is caught after it ran, and
    // the statement is undone.
    db.execute(
        "CREATE TRIGGER copy AFTER INSERT ON main.t FOR EACH ROW \
         EXECUTE INSERT INTO lk.t VALUES (NEW.a);",
    )
    .unwrap();
    let e = err(&mut db, "INSERT INTO main.t VALUES (2);");
    assert!(e.contains("not both"), "{e}");
    assert_eq!(ints(&mut db, "SELECT count(*) FROM main.t;"), vec![1]);
    assert_eq!(ints(&mut db, "SELECT count(*) FROM lk.t;"), vec![0]);

    // Reading a lake while writing locally is fine.
    db.execute("DROP TRIGGER copy;").unwrap();
    db.execute("INSERT INTO lk.t VALUES (7);").unwrap();
    db.execute("INSERT INTO main.t SELECT a FROM lk.t;")
        .unwrap();
    assert_eq!(
        ints(&mut db, "SELECT a FROM main.t ORDER BY a;"),
        vec![1, 7]
    );
}

#[test]
fn a_local_write_that_read_a_lake_replays_against_the_version_it_read() {
    let scratch = Scratch::new("pinned");
    let file = scratch.path("store.pink");
    let lakes = scratch.path("lakes");
    {
        let mut db = open_durable(&file, &lakes);
        db.execute(&create_lake_sql("lk", &scratch, "lk")).unwrap();
        db.execute("CREATE TABLE lk.src (a INTEGER);").unwrap();
        db.execute("INSERT INTO lk.src VALUES (1), (2);").unwrap();
        db.execute("CREATE TABLE main.copy AS SELECT a FROM lk.src;")
            .unwrap();
        // The lake moves on after the local write read it.
        db.execute("INSERT INTO lk.src VALUES (3);").unwrap();
        db.execute("DELETE FROM lk.src WHERE a = 1;").unwrap();
    }
    let mut db = open_durable(&file, &lakes);
    // The replayed CREATE TABLE … AS read the lake as it was: 1 and 2.
    assert_eq!(
        ints(&mut db, "SELECT a FROM main.copy ORDER BY a;"),
        vec![1, 2]
    );
    // The lake database itself is at its latest version.
    assert_eq!(
        ints(&mut db, "SELECT a FROM lk.src ORDER BY a;"),
        vec![2, 3]
    );
}

#[test]
fn an_unavailable_lake_does_not_block_the_open() {
    let scratch = Scratch::new("unavailable");
    let file = scratch.path("store.pink");
    let lakes = scratch.path("lakes");
    {
        let mut db = open_durable(&file, &lakes);
        db.execute(&create_lake_sql("gone", &scratch, "gone"))
            .unwrap();
        db.execute("CREATE TABLE gone.t (a INTEGER);").unwrap();
        db.execute("CREATE TABLE main.t (a INTEGER);").unwrap();
        db.execute("INSERT INTO main.t VALUES (1);").unwrap();
        db.execute("CHECKPOINT;").unwrap();
    }
    for file in ["gone.pink", "gone.pink.wal", "gone.pink.lock"] {
        let _ = std::fs::remove_file(scratch.path(file));
    }
    let mut db = open_durable(&file, &lakes);
    assert_eq!(ints(&mut db, "SELECT a FROM main.t;"), vec![1]);
    let e = err(&mut db, "SELECT * FROM gone.t;");
    assert!(e.contains("unavailable"), "{e}");
    match db.execute("SHOW DATABASES;").unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => {
            let gone = rows
                .iter()
                .find(|r| r[0] == Value::Varchar("gone".into()))
                .unwrap();
            match &gone[5] {
                Value::Varchar(s) => assert!(s.starts_with("unavailable"), "{s}"),
                v => panic!("{v:?}"),
            }
        }
        r => panic!("{r:?}"),
    }
    // It can be detached.
    db.execute("DETACH gone;").unwrap();
}

/// The rows of a `SELECT` over `information_schema.tables`/`duckdb_tables`,
/// as `db.schema.table` strings.
fn table_names(db: &mut PinkDb, sql: &str) -> Vec<String> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, names, .. } => {
            let db_at = names
                .iter()
                .position(|n| n == "database_name" || n == "database" || n == "table_catalog")
                .unwrap();
            let schema_at = names
                .iter()
                .position(|n| n == "schema_name" || n == "table_schema" || n == "schema")
                .unwrap();
            let table_at = names
                .iter()
                .position(|n| n == "table_name" || n == "name")
                .unwrap();
            let mut out: Vec<String> = rows
                .iter()
                .map(|r| match (&r[db_at], &r[schema_at], &r[table_at]) {
                    (Value::Varchar(db), Value::Varchar(schema), Value::Varchar(table)) => {
                        format!("{db}.{schema}.{table}")
                    }
                    other => panic!("{other:?}"),
                })
                .collect();
            out.sort();
            out
        }
        r => panic!("expected rows, got {r:?}"),
    }
}

#[test]
fn every_lake_schema_is_a_database_schema_through_every_path() {
    let scratch = Scratch::new("schemas");
    let file = scratch.path("store.pink");
    let lakes = scratch.path("lakes");
    {
        let mut db = open_durable(&file, &lakes);
        db.execute(&create_lake_sql("r2_cat", &scratch, "r2"))
            .unwrap();
        // DuckLake's `master`, `ingest` and an empty `staging`, as QuackLab's
        // lake has.
        db.execute("CREATE SCHEMA r2_cat.master; CREATE SCHEMA r2_cat.ingest;")
            .unwrap();
        db.execute("CREATE TABLE r2_cat.master.fact (id INTEGER, amount DOUBLE);")
            .unwrap();
        db.execute("INSERT INTO r2_cat.master.fact VALUES (1, 2.5), (2, 3.5);")
            .unwrap();
        db.execute("CREATE TABLE r2_cat.ingest.events (id INTEGER);")
            .unwrap();
        db.execute("INSERT INTO r2_cat.ingest.events VALUES (7);")
            .unwrap();
        db.execute("CREATE SCHEMA r2_cat.staging;").unwrap();
        // The spelling is DuckLake's, the lookup is case-insensitive.
        assert_eq!(
            ints(&mut db, "SELECT id FROM R2_CAT.Master.FACT ORDER BY id;"),
            vec![1, 2]
        );
        // Two-part names still mean the default schema, `main`.
        let e = err(&mut db, "SELECT * FROM r2_cat.fact;");
        assert!(e.contains("not found"), "{e}");
        db.execute("CHECKPOINT;").unwrap();
    }
    // The lake's own catalog carries one schema row per engine schema, the
    // empty one included.
    let lake = Lake::open_with_catalog(&format!("pink:{}", scratch.s("r2.pink")), None).unwrap();
    let first_head = lake.current_version();
    let mut schemas = lake.schemas().unwrap();
    schemas.sort();
    assert_eq!(schemas, vec!["ingest", "main", "master", "staging"]);

    {
        // Reopen writable: the image restored the empty schema and both
        // tables, and the engine can create and drop schemas.
        let mut db = open_durable(&file, &lakes);
        assert_eq!(
            ints(&mut db, "SELECT id FROM r2_cat.master.fact ORDER BY id;"),
            vec![1, 2]
        );
        assert_eq!(
            ints(&mut db, "SELECT id FROM r2_cat.ingest.events;"),
            vec![7]
        );
        assert_eq!(
            table_names(
                &mut db,
                "SELECT * FROM information_schema.tables WHERE table_catalog = 'r2_cat';"
            ),
            vec!["r2_cat.ingest.events", "r2_cat.master.fact"]
        );
        assert_eq!(
            table_names(
                &mut db,
                "SELECT * FROM duckdb_tables() WHERE database_name = 'r2_cat';"
            ),
            vec!["r2_cat.ingest.events", "r2_cat.master.fact"]
        );
        // The empty schema survived the reopen and is listed.
        let schemata = match db
            .execute("SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'r2_cat';")
            .unwrap()
            .pop()
            .unwrap()
        {
            QueryResult::Selected { rows, .. } => rows
                .into_iter()
                .map(|r| match &r[0] {
                    Value::Varchar(s) => s.clone(),
                    v => panic!("{v:?}"),
                })
                .collect::<Vec<_>>(),
            r => panic!("{r:?}"),
        };
        assert_eq!(schemata, vec!["ingest", "main", "master", "staging"]);
        // A write in a schema is one lake version.
        db.execute("INSERT INTO r2_cat.master.fact VALUES (3, 1.5);")
            .unwrap();
        // CASCADE drops the schema's tables in the lake too.
        db.execute("DROP SCHEMA r2_cat.ingest CASCADE;").unwrap();
        db.execute("DROP SCHEMA r2_cat.staging;").unwrap();
        db.execute("CHECKPOINT;").unwrap();
    }
    let lake = Lake::open_with_catalog(&format!("pink:{}", scratch.s("r2.pink")), None).unwrap();
    assert_eq!(lake.schemas().unwrap(), vec!["main", "master"]);
    assert_eq!(lake.tables().unwrap(), vec![("master.fact".to_string(), 3)]);
    // The snapshot changes use DuckLake's spellings: a created schema by its
    // quoted name, a dropped one by its id (ducklake_transaction_state.cpp).
    drop(lake);
    // The `pink:` catalog is a durable store (checkpoint plus WAL).
    let catalog = Database::open(&scratch.s("r2.pink")).unwrap();
    let changes = match catalog
        .connect()
        .execute("SELECT changes_made FROM ducklake_snapshot_changes;")
        .unwrap()
        .pop()
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => rows
            .into_iter()
            .filter_map(|r| match &r[0] {
                Value::Varchar(s) => Some(s.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(","),
        r => panic!("{r:?}"),
    };
    assert!(changes.contains("created_schema:\"staging\""), "{changes}");
    let dropped: Vec<&str> = changes
        .split(',')
        .filter_map(|c| c.strip_prefix("dropped_schema:"))
        .collect();
    assert_eq!(dropped.len(), 2, "{changes}");
    assert!(
        dropped.iter().all(|id| id.parse::<i64>().is_ok()),
        "{changes}"
    );
    drop(catalog);

    // A read-only attachment shows the same schemas.
    let mut reader = PinkDb::new();
    reader
        .execute(&format!(
            "ATTACH 'lake:pink:{}' AS r2_cat (READ_ONLY);",
            scratch.s("r2.pink")
        ))
        .unwrap();
    assert_eq!(
        ints(
            &mut reader,
            "SELECT id FROM r2_cat.master.fact ORDER BY id;"
        ),
        vec![1, 2, 3]
    );
    assert_eq!(
        table_names(
            &mut reader,
            "SELECT * FROM duckdb_tables() WHERE database_name = 'r2_cat';"
        ),
        vec!["r2_cat.master.fact"]
    );
    let e = err(&mut reader, "SELECT * FROM r2_cat.ingest.events;");
    assert!(e.contains("not found"), "{e}");
    // A pinned read of one schema's table.
    let e = err(
        &mut reader,
        "SELECT * FROM r2_cat.master.fact AT (VERSION => 1);",
    );
    assert!(e.contains("does not exist at version 1"), "{e}");
    assert_eq!(
        ints(
            &mut reader,
            &format!("SELECT id FROM r2_cat.master.fact AT (VERSION => {first_head}) ORDER BY id;")
        ),
        vec![1, 2]
    );
    // Read-only lake databases refuse schema changes.
    let e = err(&mut reader, "CREATE SCHEMA r2_cat.other;");
    assert!(e.contains("read-only lake"), "{e}");
    let e = err(&mut reader, "DROP SCHEMA r2_cat.master CASCADE;");
    assert!(e.contains("read-only lake"), "{e}");
}

#[test]
fn a_lake_written_with_two_schemas_attaches_read_only() {
    let scratch = Scratch::new("api_schemas");
    // A lake written by pink-lake's own API, not through the engine: QuackLab
    // writes `master.fact` and `ingest.events`.
    let mut store = PinkDb::new();
    store
        .execute("CREATE DATABASE master; CREATE TABLE master.fact (x INTEGER);")
        .unwrap();
    store
        .execute("INSERT INTO master.fact VALUES (1), (2);")
        .unwrap();
    store
        .execute("CREATE DATABASE ingest; CREATE TABLE ingest.events (y INTEGER);")
        .unwrap();
    store
        .execute("INSERT INTO ingest.events VALUES (9);")
        .unwrap();
    let dir = scratch.s("two");
    pink_lake::save_store(&store, &dir).unwrap();
    let mut db = PinkDb::new();
    db.execute(&format!("ATTACH 'lake:{dir}' AS r2_cat (READ_ONLY);"))
        .unwrap();
    assert_eq!(
        ints(&mut db, "SELECT x FROM r2_cat.master.fact ORDER BY x;"),
        vec![1, 2]
    );
    assert_eq!(
        ints(&mut db, "SELECT y FROM r2_cat.ingest.events;"),
        vec![9]
    );
    assert_eq!(
        table_names(
            &mut db,
            "SELECT * FROM information_schema.tables WHERE table_catalog = 'r2_cat';"
        ),
        vec!["r2_cat.ingest.events", "r2_cat.master.fact"]
    );
    assert_eq!(
        ints(
            &mut db,
            "SELECT count(*) FROM r2_cat.ingest.events WHERE y = 9;"
        ),
        vec![1]
    );
    // The lake's other schema is not the default one: a two-part name stays
    // in `main`.
    let e = err(&mut db, "SELECT * FROM r2_cat.fact;");
    assert!(e.contains("not found"), "{e}");
    // `pinkdb_catalog()` names the lake schema too.
    let catalog = match db
        .execute(
            "SELECT \"database\", \"schema\", name, kind FROM pinkdb_catalog() \
             WHERE \"database\" = 'r2_cat' AND kind = 'LAKE TABLE' \
             ORDER BY \"schema\", name;",
        )
        .unwrap()
        .pop()
        .unwrap()
    {
        QueryResult::Selected { rows, .. } => rows,
        r => panic!("{r:?}"),
    };
    let listed: Vec<String> = catalog
        .iter()
        .map(|row| match (&row[0], &row[1], &row[2], &row[3]) {
            (
                Value::Varchar(db),
                Value::Varchar(schema),
                Value::Varchar(table),
                Value::Varchar(kind),
            ) => {
                assert_eq!(kind, "LAKE TABLE");
                format!("{db}.{schema}.{table}")
            }
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(listed, vec!["r2_cat.ingest.events", "r2_cat.master.fact"]);
}

#[test]
fn connections_route_lake_commits_too() {
    let scratch = Scratch::new("connections");
    let db = Database::new();
    let mut conn = db.connect();
    conn.execute(&create_lake_sql("lk", &scratch, "lk"))
        .unwrap();
    conn.execute("CREATE TABLE lk.t (a INTEGER);").unwrap();
    conn.execute("BEGIN;").unwrap();
    conn.execute("INSERT INTO lk.t VALUES (1), (2);").unwrap();
    conn.execute("COMMIT;").unwrap();
    let lake = Lake::open_with_catalog(&format!("pink:{}", scratch.s("lk.pink")), None).unwrap();
    let before = lake.current_version();
    conn.execute("INSERT INTO lk.t VALUES (3);").unwrap();
    let lake = Lake::open_with_catalog(&format!("pink:{}", scratch.s("lk.pink")), None).unwrap();
    assert_eq!(lake.current_version(), before + 1);
    // A read-only reader sees all three rows.
    let mut reader = PinkDb::new();
    reader
        .execute(&format!(
            "ATTACH 'lake:pink:{}' AS lk (READ_ONLY);",
            scratch.s("lk.pink")
        ))
        .unwrap();
    assert_eq!(ints(&mut reader, "SELECT count(*) FROM lk.t;"), vec![3]);
    // Mixed in a connection transaction: refused at the second storage.
    conn.execute("CREATE TABLE main.m (a INTEGER);").unwrap();
    conn.execute("BEGIN;").unwrap();
    conn.execute("INSERT INTO lk.t VALUES (4);").unwrap();
    let e = conn
        .execute("INSERT INTO main.m VALUES (4);")
        .unwrap_err()
        .to_string();
    assert!(e.contains("not both"), "{e}");
    conn.execute("ROLLBACK;").unwrap();
}

#[test]
fn passwords_in_lake_urls_are_refused() {
    let _scratch = Scratch::new("password");
    let mut db = PinkDb::new();
    let e = err(
        &mut db,
        "ATTACH 'lake:postgres://u:hunter2@localhost/x' AS p (READ_ONLY);",
    );
    assert!(e.contains("carries a password"), "{e}");
    assert!(!e.contains("hunter2"), "{e}");
    let e = err(&mut db, "ATTACH '/tmp/x.pink' AS p;");
    assert!(e.contains("not a lake"), "{e}");
}
