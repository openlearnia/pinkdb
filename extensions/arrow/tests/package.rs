//! Wave 86 T2: the arrow extension as an **extension package**.
//!
//! The package registry is process-global, so [`register_package`] is called
//! once behind a [`Once`]; this binary deliberately never calls
//! `ensure_registered` (the global model), because a package whose source or
//! format name is already registered globally is refused at load.
//!
//! A store opts in with `LOAD EXTENSION 'arrow'` (SQL loading allowed, as the
//! engine's package tests do), gets the Arrow reader/writer on that store
//! only, and a second store stays without them.

use std::path::PathBuf;
use std::sync::Once;
use std::sync::atomic::{AtomicU64, Ordering};

use pinkdb::{PinkDb, QueryResult, Value};

static REGISTER_PACKAGE: Once = Once::new();

/// Publish `pinkdb_arrow::package()` process-wide exactly once.
fn register_package_once() {
    REGISTER_PACKAGE.call_once(|| {
        pinkdb_arrow::register_package().expect("register the arrow extension package");
    });
}

/// A scratch directory that is removed when the guard drops.
struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_arrow_package_{tag}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create the scratch directory");
        Scratch { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Rows of the last statement in `sql` (which must be a SELECT-like result).
fn query(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    let mut out = db.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(out.len(), 1, "{sql} must be one statement");
    match out.pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

/// Not active before `LOAD`, active on the loaded store afterwards: the
/// package's COPY sink writes an Arrow file and its read source reads it
/// back identically. The SQL gate still applies.
#[test]
fn a_store_gets_the_package_only_through_load_extension() {
    register_package_once();
    let scratch = Scratch::new("activation");
    let file = scratch.path("x.arrow");

    let mut db = PinkDb::new();
    assert!(db.table_source("arrow").is_none());
    assert!(db.file_format("arrow").is_none());
    assert!(!db.is_extension_loaded("arrow"));

    db.execute("CREATE TABLE t (a INTEGER, s VARCHAR);")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'x'), (2, NULL);")
        .unwrap();

    // Both surfaces are absent before the load: no sink …
    let err = db
        .execute(&format!("COPY t TO '{}' (FORMAT arrow);", file.display()))
        .unwrap_err()
        .to_string();
    assert!(err.contains("no file format \"arrow\""), "{err}");
    // … and no read source.
    let err = db
        .execute(&format!("SELECT * FROM '{}';", file.display()))
        .unwrap_err()
        .to_string();
    assert!(err.contains("cannot read"), "{err}");

    // The SQL gate is unchanged: LOAD EXTENSION is off by default.
    let err = db
        .execute("LOAD EXTENSION 'arrow';")
        .unwrap_err()
        .to_string();
    assert!(err.contains("LOAD EXTENSION is disabled"), "{err}");

    // The documented opt-in activates the package on this store.
    db.set_allow_sql_load_extension(true);
    let result = db
        .execute("LOAD EXTENSION 'arrow';")
        .unwrap()
        .pop()
        .unwrap();
    assert!(
        matches!(&result, QueryResult::LoadedExtension { name } if name == "arrow"),
        "{result:?}"
    );
    assert!(db.is_extension_loaded("arrow"));
    assert!(db.table_source("arrow").is_some());
    assert!(db.file_format("arrow").is_some());

    // The package's sink writes …
    assert_eq!(
        query(
            &mut db,
            &format!("COPY t TO '{}' (FORMAT arrow);", file.display())
        ),
        vec![vec![Value::Bigint(2)]]
    );
    // … and its source reads the file back identically.
    assert_eq!(
        query(
            &mut db,
            &format!("SELECT a, s FROM '{}' ORDER BY a;", file.display())
        ),
        vec![
            vec![Value::Integer(1), Value::Varchar("x".to_string())],
            vec![Value::Integer(2), Value::Null],
        ]
    );
}

/// Activation is per store: a second store has no Arrow source or sink even
/// though the package is registered process-wide.
#[test]
fn a_second_store_stays_without_the_package() {
    register_package_once();
    let scratch = Scratch::new("per_store");
    let file = scratch.path("x.arrow");

    let mut first = PinkDb::new();
    first.set_allow_sql_load_extension(true);
    first.execute("LOAD EXTENSION 'arrow';").unwrap();
    first.execute("CREATE TABLE t (a INTEGER);").unwrap();
    first.execute("INSERT INTO t VALUES (7);").unwrap();
    query(
        &mut first,
        &format!("COPY t TO '{}' (FORMAT arrow);", file.display()),
    );

    let mut second = PinkDb::new();
    assert!(second.table_source("arrow").is_none());
    assert!(second.file_format("arrow").is_none());
    assert!(!second.is_extension_loaded("arrow"));
    // The file exists and the package is registered, but this store never
    // loaded it: no read source …
    let err = second
        .execute(&format!("SELECT * FROM '{}';", file.display()))
        .unwrap_err()
        .to_string();
    assert!(err.contains("cannot read"), "{err}");
    // … and no COPY sink, even with SQL loading allowed (it still has to opt
    // in with its own LOAD EXTENSION).
    second.execute("CREATE TABLE u (a INTEGER);").unwrap();
    second.set_allow_sql_load_extension(true);
    let err = second
        .execute(&format!(
            "COPY u TO '{}' (FORMAT arrow);",
            scratch.path("y.arrow").display()
        ))
        .unwrap_err()
        .to_string();
    assert!(err.contains("no file format \"arrow\""), "{err}");
}
