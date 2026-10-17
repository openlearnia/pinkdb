//! Wave 65: `ALTER TABLE` (ADD COLUMN with DEFAULT, RENAME COLUMN, DROP
//! COLUMN, RENAME TO) end to end in the engine — parsing, catalog semantics,
//! defaults on later INSERTs, DESCRIBE, `.pink` persistence round-trips,
//! transaction rollback and durable WAL replay.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{DataType, PinkDb, QueryResult, Value};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_alter_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

fn err(db: &mut PinkDb, sql: &str) -> String {
    db.execute(sql).unwrap_err().to_string()
}

/// `t (i INTEGER, s VARCHAR)` with three rows.
fn seeded() -> PinkDb {
    let mut db = PinkDb::new();
    db.execute("CREATE TABLE t (i INTEGER, s VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');")
        .unwrap();
    db
}

#[test]
fn add_column_with_default_materializes_and_later_inserts_use_it() {
    let mut db = seeded();
    let results = db
        .execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 42;")
        .unwrap();
    assert_eq!(results[0], QueryResult::AlteredTable { name: "t".into() });
    // Existing rows read the default; the rows are untouched otherwise.
    assert_eq!(
        rows(&mut db, "SELECT i, s, n FROM t ORDER BY i;"),
        vec![
            vec![
                Value::Integer(1),
                Value::Varchar("a".into()),
                Value::Integer(42)
            ],
            vec![
                Value::Integer(2),
                Value::Varchar("b".into()),
                Value::Integer(42)
            ],
            vec![
                Value::Integer(3),
                Value::Varchar("c".into()),
                Value::Integer(42)
            ],
        ]
    );
    // A later INSERT that omits the column writes the default (DuckDB).
    db.execute("INSERT INTO t (i, s) VALUES (4, 'd');").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT n FROM t WHERE i = 4;"),
        vec![vec![Value::Integer(42)]]
    );
    // An INSERT .. SELECT respects it too.
    db.execute("CREATE TABLE u (i INTEGER);").unwrap();
    db.execute("INSERT INTO u VALUES (9);").unwrap();
    db.execute("INSERT INTO t (i, s) SELECT i, 'sel' FROM u;")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT i, s, n FROM t WHERE i = 9;"),
        vec![vec![
            Value::Integer(9),
            Value::Varchar("sel".into()),
            Value::Integer(42)
        ]]
    );
    // An explicit value wins.
    db.execute("INSERT INTO t VALUES (5, 'e', 7);").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT n FROM t WHERE i = 5;"),
        vec![vec![Value::Integer(7)]]
    );
    // ADD COLUMN without DEFAULT writes NULL, not the previous default.
    db.execute("ALTER TABLE t ADD COLUMN m INTEGER;").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT m FROM t WHERE i = 1;"),
        vec![vec![Value::Null]]
    );
    db.execute("INSERT INTO t (i, s, n) VALUES (6, 'f', 1);")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT m FROM t WHERE i = 6;"),
        vec![vec![Value::Null]]
    );
    // The engine reports the defaults in DESCRIBE.
    let described = rows(&mut db, "DESCRIBE t;");
    assert_eq!(described.len(), 4);
    assert_eq!(described[2][0], Value::Varchar("n".into()));
    assert_eq!(described[2][4], Value::Varchar("42".into()));
    assert_eq!(described[3][4], Value::Null, "no default: NULL");
}

#[test]
fn rename_column_keeps_values_and_case_only_changes_work() {
    let mut db = seeded();
    db.execute("ALTER TABLE t RENAME COLUMN s TO label;")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT label FROM t ORDER BY i;"),
        vec![
            vec![Value::Varchar("a".into())],
            vec![Value::Varchar("b".into())],
            vec![Value::Varchar("c".into())],
        ]
    );
    assert!(db.execute("SELECT s FROM t;").is_err());
    // A case-only rename is allowed and updates the display name.
    db.execute("ALTER TABLE t RENAME COLUMN label TO LABEL;")
        .unwrap();
    let described = rows(&mut db, "DESCRIBE t;");
    assert_eq!(described[1][0], Value::Varchar("LABEL".into()));
    assert_eq!(
        rows(&mut db, "SELECT LABEL FROM t WHERE i = 2;"),
        vec![vec![Value::Varchar("b".into())]]
    );
}

#[test]
fn drop_column_reindexes_constraints_and_refuses_keyed_columns() {
    let mut db = PinkDb::new();
    db.execute(
        "CREATE TABLE t (a INTEGER, b VARCHAR NOT NULL, c INTEGER, CONSTRAINT pk PRIMARY KEY (a));",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'x', 2), (3, 'y', 4);")
        .unwrap();
    // Dropping a middle column reindexes NOT NULL (b moves 1 -> 0).
    db.execute("ALTER TABLE t DROP COLUMN b;").unwrap();
    let described = rows(&mut db, "DESCRIBE t;");
    assert_eq!(described.len(), 2, "{described:?}");
    assert_eq!(described[0][0], Value::Varchar("a".into()));
    assert_eq!(described[0][3], Value::Varchar("PRI".into()));
    assert_eq!(described[1][0], Value::Varchar("c".into()));
    assert_eq!(
        rows(&mut db, "SELECT a, c FROM t ORDER BY a;"),
        vec![
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(3), Value::Integer(4)],
        ]
    );
    // The primary-key column itself is protected.
    let msg = err(&mut db, "ALTER TABLE t DROP COLUMN a;");
    assert!(
        msg.contains("Cannot drop column \"a\" because there is a PRIMARY KEY constraint"),
        "{msg}"
    );
    // A foreign-key child column is protected too.
    db.execute("CREATE TABLE p (k INTEGER UNIQUE, v INTEGER);")
        .unwrap();
    db.execute("CREATE TABLE c (fk INTEGER REFERENCES p(k), w INTEGER);")
        .unwrap();
    let msg = err(&mut db, "ALTER TABLE c DROP COLUMN fk;");
    assert!(
        msg.contains("Cannot drop column \"fk\" because there is a FOREIGN KEY constraint"),
        "{msg}"
    );
    // Dropping the only column is refused (a table needs one).
    let mut one = PinkDb::new();
    one.execute("CREATE TABLE t (x INTEGER);").unwrap();
    assert!(one.execute("ALTER TABLE t DROP COLUMN x;").is_err());
}

#[test]
fn rename_table_supports_qualified_names_and_refuses_collisions() {
    let mut db = seeded();
    db.execute("ALTER TABLE t RENAME TO renamed;").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM renamed;"),
        vec![vec![Value::Bigint(3)]]
    );
    assert!(db.execute("SELECT * FROM t;").is_err());
    // Qualified form.
    db.execute("CREATE DATABASE other;").unwrap();
    db.execute("CREATE TABLE other.o (x INTEGER);").unwrap();
    db.execute("CREATE TABLE other.taken (x INTEGER);").unwrap();
    db.execute("ALTER TABLE other.o RENAME TO o2;").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM other.o2;"),
        vec![vec![Value::Bigint(0)]]
    );
    // A same-named table in *another* database does not collide...
    db.execute("ALTER TABLE other.o2 RENAME TO renamed;")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM other.renamed;"),
        vec![vec![Value::Bigint(0)]]
    );
    // ...but one in the same database does.
    let msg = err(&mut db, "ALTER TABLE other.renamed RENAME TO taken;");
    assert!(
        msg.contains("Could not rename \"renamed\" to \"taken\""),
        "{msg}"
    );
}

#[test]
fn alter_parses_optionally_quoted_and_positional_forms() {
    let mut db = seeded();
    // COLUMN is optional; constraints may precede or follow DEFAULT; quoted
    // identifiers work.
    db.execute("ALTER TABLE t ADD v INTEGER;").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN w INTEGER NOT NULL DEFAULT 3;")
        .unwrap();
    db.execute("ALTER TABLE t ADD COLUMN \"odd name\" VARCHAR DEFAULT 'q';")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT w, \"odd name\", v FROM t WHERE i = 1;"),
        vec![vec![
            Value::Integer(3),
            Value::Varchar("q".into()),
            Value::Null
        ]]
    );
    db.execute("ALTER TABLE t DROP COLUMN v;").unwrap();
    db.execute("ALTER TABLE t RENAME COLUMN w TO weight;")
        .unwrap();
    let described = rows(&mut db, "DESCRIBE t;");
    let names: Vec<&str> = described
        .iter()
        .map(|r| match &r[0] {
            Value::Varchar(s) => s.as_str(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(names, vec!["i", "s", "weight", "odd name"]);
    // `alter`, `add`, `column`, `to` and `default` stay usable as identifiers.
    let mut words = PinkDb::new();
    words
        .execute("CREATE TABLE alter (column INTEGER, \"to\" INTEGER, \"default\" INTEGER);")
        .unwrap();
    words
        .execute("INSERT INTO alter (column, \"to\", \"default\") VALUES (1, 2, 3);")
        .unwrap();
    assert_eq!(
        rows(&mut words, "SELECT column FROM alter;"),
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn alter_rolls_back_with_a_transaction() {
    let mut db = seeded();
    db.execute("BEGIN;").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 1;")
        .unwrap();
    db.execute("ALTER TABLE t RENAME COLUMN s TO label;")
        .unwrap();
    db.execute("ROLLBACK;").unwrap();
    let described = rows(&mut db, "DESCRIBE t;");
    let names: Vec<String> = described
        .iter()
        .map(|r| match &r[0] {
            Value::Varchar(s) => s.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(names, vec!["i", "s"]);
    // And the same statements commit cleanly.
    db.execute("BEGIN;").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 1;")
        .unwrap();
    db.execute("COMMIT;").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT n FROM t WHERE i = 1;"),
        vec![vec![Value::Integer(1)]]
    );
}

#[test]
fn defaults_survive_pink_save_and_load() {
    let dir = scratch("persist");
    let path = dir.join("db.pink");
    let mut db = seeded();
    db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 42;")
        .unwrap();
    db.execute("ALTER TABLE t ADD COLUMN d DECIMAL(5,2) DEFAULT 1.25;")
        .unwrap();
    db.execute("ALTER TABLE t ADD COLUMN dt DATE DEFAULT DATE '2020-01-02';")
        .unwrap();
    db.execute("ALTER TABLE t ADD COLUMN v VARCHAR DEFAULT 'hi';")
        .unwrap();
    db.save(path.to_str().unwrap()).unwrap();
    let mut db = PinkDb::open(path.to_str().unwrap()).unwrap();
    // The defaults round-trip: a later INSERT omitting them writes them.
    db.execute("INSERT INTO t (i, s) VALUES (9, 'x');").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT n, d, dt, v FROM t WHERE i = 9;"),
        vec![vec![
            Value::Integer(42),
            Value::Decimal(125, 2),
            Value::Date(pinkdb::types::parse_date("2020-01-02").unwrap()),
            Value::Varchar("hi".into()),
        ]]
    );
    // DESCRIBE still reports them.
    let described = rows(&mut db, "DESCRIBE t;");
    assert_eq!(described[2][4], Value::Varchar("42".into()));
    assert_eq!(
        described[3][4],
        Value::Varchar("CAST('1.25' AS DECIMAL(38,2))".into())
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn durable_wal_replay_reproduces_alter_and_its_defaults() {
    let dir = scratch("wal");
    let path = dir.join("db.pink");
    // A durable session: ALTER is WAL-logged and replayed on reopen.
    {
        let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
        db.execute("CREATE TABLE t (i INTEGER, s VARCHAR);")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a');").unwrap();
        db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 7;")
            .unwrap();
        db.checkpoint().unwrap();
        db.execute("ALTER TABLE t RENAME COLUMN s TO label;")
            .unwrap();
        db.execute("INSERT INTO t (i, label) VALUES (2, 'b');")
            .unwrap();
    }
    let mut db = PinkDb::open_durable(path.to_str().unwrap()).unwrap();
    let described = rows(&mut db, "DESCRIBE t;");
    let names: Vec<String> = described
        .iter()
        .map(|r| match &r[0] {
            Value::Varchar(s) => s.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(names, vec!["i", "label", "n"]);
    assert_eq!(
        rows(&mut db, "SELECT i, label, n FROM t ORDER BY i;"),
        vec![
            vec![
                Value::Integer(1),
                Value::Varchar("a".into()),
                Value::Integer(7)
            ],
            vec![
                Value::Integer(2),
                Value::Varchar("b".into()),
                Value::Integer(7)
            ],
        ]
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn attach_lake_read_only_tables_refuse_alter() {
    // The lake crate owns the real attachment; this pins the engine-side
    // refusal a stub produces: ALTER must never look like a no-op on an
    // attached name.
    let mut db = seeded();
    let msg = err(&mut db, "ALTER TABLE nosuch ADD COLUMN n INTEGER;");
    assert!(msg.contains("not found"), "{msg}");
    // A rename to itself succeeds (and a case change updates the name).
    db.execute("ALTER TABLE t RENAME TO t;").unwrap();
    db.execute("ALTER TABLE t RENAME TO T;").unwrap();
    assert_eq!(
        rows(&mut db, "SELECT COUNT(*) FROM T;"),
        vec![vec![Value::Bigint(3)]]
    );
    // ALTER names a type the engine does not know.
    assert!(db
        .execute("ALTER TABLE t ADD COLUMN z INTEGER DEFAULT 'x';")
        .is_err());
}

#[test]
fn alter_is_written_into_explain_and_the_wal_touched_sets_compile() {
    // EXPLAIN of a SELECT still works after an ALTER (scan shapes rebuilt).
    let mut db = seeded();
    db.execute("ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 1;")
        .unwrap();
    let explained = rows(&mut db, "EXPLAIN SELECT n FROM t WHERE i > 1;");
    assert!(!explained.is_empty());
    let text: String = explained
        .iter()
        .map(|r| match &r[0] {
            Value::Varchar(s) => s.clone(),
            other => panic!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("Scan"), "{text}");
    assert!(
        text.contains("AlterTable") || text.contains("Project"),
        "{text}"
    );
}

#[test]
fn adding_a_column_with_a_typed_default_coerces_and_rejects_bad_values() {
    let mut db = seeded();
    // INTEGER into BIGINT widens (INSERT coercion rules).
    db.execute("ALTER TABLE t ADD COLUMN big BIGINT DEFAULT 5;")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT big FROM t WHERE i = 1;"),
        vec![vec![Value::Bigint(5)]]
    );
    // A string literal into INTEGER is a clean type error.
    let msg = err(&mut db, "ALTER TABLE t ADD COLUMN bad INTEGER DEFAULT 'x';");
    assert!(
        msg.contains("cannot coerce") || msg.contains("type"),
        "{msg}"
    );
    // A constant expression is folded.
    db.execute("ALTER TABLE t ADD COLUMN sum INTEGER DEFAULT 1 + 2;")
        .unwrap();
    assert_eq!(
        rows(&mut db, "SELECT sum FROM t WHERE i = 2;"),
        vec![vec![Value::Integer(3)]]
    );
    // NULL into a NOT NULL column on a non-empty table is the constraint
    // error; on an empty table it is allowed.
    let msg = err(
        &mut db,
        "ALTER TABLE t ADD COLUMN nn INTEGER NOT NULL DEFAULT NULL;",
    );
    assert!(msg.contains("NOT NULL constraint failed"), "{msg}");
    let mut empty = PinkDb::new();
    empty.execute("CREATE TABLE e (x INTEGER);").unwrap();
    empty
        .execute("ALTER TABLE e ADD COLUMN y INTEGER NOT NULL;")
        .unwrap();
    assert_eq!(rows(&mut empty, "DESCRIBE e;").len(), 2);
}

#[test]
fn alter_result_maps_to_protocol_and_ffi_kinds() {
    // `AlteredTable` is a real outcome: the connection path returns it (the
    // server/FFI map it like their other simple Done kinds — pinned by those
    // crates' own round-trips).
    let db = pinkdb::Database::new();
    let mut conn = db.connect();
    let results = conn.execute("CREATE TABLE t (x INTEGER);").unwrap();
    assert_eq!(results[0], QueryResult::Created);
    let results = conn.execute("ALTER TABLE t ADD COLUMN y INTEGER;").unwrap();
    assert_eq!(results[0], QueryResult::AlteredTable { name: "t".into() });
    // A typed default keeps its dtype through the connection path.
    conn.execute("ALTER TABLE t ADD COLUMN d DECIMAL(4,1) DEFAULT 2.5;")
        .unwrap();
    let described = match conn.execute("DESCRIBE t;").unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        described[2][1],
        Value::Varchar("DECIMAL(4,1)".into()),
        "{described:?}"
    );
    assert_eq!(
        described[2][4],
        Value::Varchar("CAST('2.5' AS DECIMAL(38,1))".into())
    );
    let _ = DataType::Integer;
}
