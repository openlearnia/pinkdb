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

#[test]
fn a_lake_with_two_schemas_is_refused() {
    let scratch = Scratch::new("schemas");
    let mut store = PinkDb::new();
    store
        .execute(
            "CREATE DATABASE a; CREATE TABLE a.t (x INTEGER); CREATE TABLE main.u (y INTEGER);",
        )
        .unwrap();
    let dir = scratch.s("two");
    pink_lake::save_store(&store, &dir).unwrap();
    let mut db = PinkDb::new();
    let e = err(&mut db, &format!("ATTACH 'lake:{dir}' AS two (READ_ONLY);"));
    assert!(e.contains("2 schemas"), "{e}");
    let e = err(&mut db, &format!("ATTACH 'lake:{dir}' AS two;"));
    assert!(e.contains("2 schemas"), "{e}");
    assert!(!db.database_exists("two"));
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
