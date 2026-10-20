//! C-1 / C-2: opening a database file must never execute native code
//! recorded in its WAL, and `LOAD EXTENSION` is off by default.
//!
//! The A.1 reproduction, as a test:
//!
//! - the fixture extension ([`fixtures/marker_ext.c`]) has a constructor
//!   that writes a marker file when the library is *mapped*;
//! - a durable store that used it is created **in a child process**, so
//!   this process never has the library mapped (a process that has it
//!   mapped may legitimately reuse it — that is the documented rule);
//! - the marker is deleted and the store is reopened: the reopen must not
//!   run the constructor (the marker stays absent) and — without the
//!   operator's opt-in — the open must fail with the documented error;
//! - with the extension-replay opt-in and a matching extension root, the
//!   library is loaded from the root (the marker is written again);
//! - with the opt-in but a root that does not contain the library, the
//!   open still fails.

use std::path::{Path, PathBuf};
use std::process::Command;

use pinkdb::{DurableOpenOptions, PinkDb, QueryResult, Value};

/// The environment variable that tells the helper child where to create the
/// store.
const CHILD_CKPT: &str = "PINKDB_TEST_CHILD_CKPT";
/// The environment variable that tells the helper child which extension to
/// load.
const CHILD_EXT: &str = "PINKDB_TEST_CHILD_EXT";

// ---------- helpers ----------

/// A scratch directory that is removed when the guard drops.
struct Scratch {
    dir: PathBuf,
}

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "pinkdb_replay_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&dir);
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

/// Compile the fixture extension with the system C compiler; the marker
/// path is compiled in. `cc` is invoked directly — no build dependency.
fn compile_fixture(dir: &Path, marker: &Path) -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = manifest.join("tests/fixtures/marker_ext.c");
    let include = manifest.join("../pinkdb-ext-loader/include");
    let library = dir.join(format!("marker_ext{}", std::env::consts::DLL_SUFFIX));
    let cc = std::env::var("CC").unwrap_or_else(|_| "cc".to_string());
    let output = Command::new(&cc)
        .arg("-std=c99")
        .arg("-O1")
        .arg("-fPIC")
        .arg("-shared")
        .arg("-I")
        .arg(&include)
        .arg(format!(
            "-DPINKDB_TEST_MARKER_PATH={:?}",
            marker.to_string_lossy()
        ))
        .arg("-o")
        .arg(&library)
        .arg(&source)
        .output()
        .unwrap_or_else(|e| panic!("could not run the C compiler `{cc}`: {e} (set CC?)"));
    assert!(
        output.status.success(),
        "compiling marker_ext.c failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    library
}

/// Run the test binary again, as a child process, to create the store
/// there: the extension is loaded in the *child*, so this process never has
/// the library mapped.
fn create_store_in_child(ckpt: &Path, extension: &Path) {
    let exe = std::env::current_exe().expect("the test binary's own path");
    let output = Command::new(exe)
        .arg("--exact")
        .arg("child_creates_the_store")
        .arg("--ignored")
        .arg("--nocapture")
        .env(CHILD_CKPT, ckpt)
        .env(CHILD_EXT, extension)
        .output()
        .expect("run the helper child process");
    assert!(
        output.status.success(),
        "the helper child failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The helper child: creates the durable store that used the extension.
/// It is `#[ignore]`d: it only runs when the parent asks for it by name.
#[test]
#[ignore = "helper: run as a child process by wal_replay_never_loads_a_recorded_path"]
fn child_creates_the_store() {
    let (Ok(ckpt), Ok(extension)) = (std::env::var(CHILD_CKPT), std::env::var(CHILD_EXT)) else {
        return;
    };
    let mut db = PinkDb::open_durable(&ckpt).expect("open the durable store");
    // The statement is off by default (it loads and runs native code).
    db.set_allow_sql_load_extension(true);
    db.execute(&format!(
        "LOAD EXTENSION '{extension}'; CREATE TABLE t (a INTEGER);"
    ))
    .expect("load the extension");
    // The row is computed by the extension, so the WAL record that carries
    // it is prefixed with `LOAD EXTENSION '<path>'`.
    db.execute("INSERT INTO t VALUES (ext_marker_identity(1));")
        .expect("insert through the extension");
    db.flush_wal().expect("flush the WAL");
}

/// Rows of the last statement in `sql` (which must be a SELECT).
fn rows(db: &mut PinkDb, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().pop().unwrap() {
        QueryResult::Selected { rows, .. } => rows,
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

// ---------- the tests ----------

/// C-1 (A.1): a durable store that used an extension is never re-loaded
/// from the file's own record.
#[test]
fn wal_replay_never_loads_a_recorded_path() {
    let scratch = Scratch::new("a1");
    let marker = scratch.path("marker.txt");
    let extension = compile_fixture(&scratch.dir, &marker);
    let ckpt = scratch.path("db.pink");

    // The store is created in a child process: this process must not have
    // the library mapped, or the replay could reuse it (which is allowed,
    // but would not prove anything).
    create_store_in_child(&ckpt, &extension);
    assert!(
        marker.exists(),
        "the fixture's constructor must have run in the child process"
    );
    // The record really does carry the extension's path (the WAL is a
    // binary file: the statement text is in its payload).
    let wal = std::fs::read(format!("{}.wal", ckpt.display())).unwrap();
    assert!(
        wal.windows(b"LOAD EXTENSION".len())
            .any(|w| w == b"LOAD EXTENSION"),
        "the WAL must record the extension load"
    );

    // A.1: delete the marker and reopen the store.
    std::fs::remove_file(&marker).unwrap();
    let err = PinkDb::open_durable(ckpt.to_str().unwrap())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("WAL replay refused to load the extension"),
        "{err}"
    );
    // The error is actionable: it names the opt-in and the root.
    assert!(err.contains("allow_extension_replay"), "{err}");
    assert!(err.contains("extension root"), "{err}");
    assert!(
        !marker.exists(),
        "the reopen must not have executed the recorded path's constructor"
    );

    // The opt-in alone is not enough: the library must be inside the
    // configured extension root.
    let other_root = scratch.path("elsewhere");
    std::fs::create_dir_all(&other_root).unwrap();
    let opts = DurableOpenOptions {
        allow_extension_replay: true,
        extension_root: Some(other_root),
        ..DurableOpenOptions::default()
    };
    let err = PinkDb::open_durable_with_options(ckpt.to_str().unwrap(), opts)
        .unwrap_err()
        .to_string();
    assert!(err.contains("outside the extension root"), "{err}");
    assert!(
        !marker.exists(),
        "a refused load must not have executed the constructor either"
    );

    // With the opt-in and a matching root, the replay loads the library
    // from the root — and only then does the constructor run.
    let opts = DurableOpenOptions {
        allow_extension_replay: true,
        extension_root: Some(scratch.dir.clone()),
        ..DurableOpenOptions::default()
    };
    let (mut db, _) = PinkDb::open_durable_with_options(ckpt.to_str().unwrap(), opts).unwrap();
    assert_eq!(
        rows(&mut db, "SELECT a FROM t;"),
        vec![vec![Value::Integer(7)]]
    );
    assert!(
        marker.exists(),
        "the opt-in loads the library from the root: the constructor runs"
    );
}

/// C-2: `LOAD EXTENSION` is refused by default and works after the
/// documented opt-in.
#[test]
fn load_extension_is_refused_by_default() {
    let scratch = Scratch::new("default_off");
    let marker = scratch.path("marker.txt");
    let extension = compile_fixture(&scratch.dir, &marker);
    let extension = extension.to_string_lossy().into_owned();

    let mut db = PinkDb::new();
    let err = db
        .execute(&format!("LOAD EXTENSION '{extension}';"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("LOAD EXTENSION is disabled"), "{err}");
    assert!(
        !marker.exists(),
        "the refused load must not have run any code"
    );

    // The documented opt-in flips it.
    db.set_allow_sql_load_extension(true);
    let results = db
        .execute(&format!("LOAD EXTENSION '{extension}';"))
        .unwrap();
    assert!(
        matches!(&results[0], QueryResult::LoadedExtension { name } if name == "marker_ext"),
        "{results:?}"
    );
    assert_eq!(
        rows(&mut db, "SELECT ext_marker_identity(1);"),
        vec![vec![Value::Integer(7)]]
    );
    assert!(marker.exists(), "the opted-in load runs the constructor");
}

/// The default is off for every store: the engine's own `Database` handle
/// refuses the statement until it is told otherwise.
#[test]
fn the_database_handle_refuses_the_statement_by_default() {
    let scratch = Scratch::new("handle");
    let marker = scratch.path("marker.txt");
    let extension = compile_fixture(&scratch.dir, &marker);
    let extension = extension.to_string_lossy().into_owned();

    let db = pinkdb::Database::new();
    let mut conn = db.connect();
    let err = conn
        .execute(&format!("LOAD EXTENSION '{extension}';"))
        .unwrap_err()
        .to_string();
    assert!(err.contains("LOAD EXTENSION is disabled"), "{err}");

    db.set_allow_sql_load_extension(true);
    let mut conn = db.connect();
    conn.execute(&format!("LOAD EXTENSION '{extension}';"))
        .expect("the opt-in flips the behavior");
    assert!(marker.exists());
}
