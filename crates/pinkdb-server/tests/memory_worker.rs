//! Wave 121: the memory distillation worker's drain, against the real
//! memory extension (its cdylib) and a store with users.
//!
//! One test in its own binary: it changes the provider environment.

use std::path::{Path, PathBuf};
use std::process::Command;

use pinkdb::Database;
use pinkdb::serve::{CommandEvents, ServePolicy, memory_policies};
use pinkdb_server::memory_worker::drain_once;

fn library() -> PathBuf {
    let name = if cfg!(target_os = "macos") {
        "libpinkdb_memory.dylib"
    } else if cfg!(target_os = "windows") {
        "pinkdb_memory.dll"
    } else {
        "libpinkdb_memory.so"
    };
    let status = Command::new(env!("CARGO"))
        .args(["build", "-q", "-p", "pinkdb-memory"])
        .status()
        .expect("run cargo");
    assert!(status.success(), "cargo build -p pinkdb-memory failed");
    let exe = std::env::current_exe().unwrap();
    let path = exe.parent().and_then(Path::parent).unwrap().join(name);
    assert!(path.is_file(), "no memory library at {}", path.display());
    path
}

struct Quiet;
impl CommandEvents for Quiet {
    fn emit(&mut self, _: i32, _: &str) {}
}

#[test]
fn the_worker_drains_as_each_owner_and_needs_a_provider() {
    // SAFETY: the only test of this binary.
    unsafe {
        std::env::remove_var("PINKDB_AGENT_PROVIDER");
    }
    let db = Database::new();
    // Not loaded: a clear error, nothing else.
    let err = drain_once(&db).unwrap_err();
    assert!(err.contains("not loaded"), "{err}");

    db.load_extension(library().to_str().unwrap()).unwrap();
    // Loaded, nothing added yet: nothing to do.
    assert_eq!(drain_once(&db).unwrap(), Vec::<String>::new());

    // A user adds a turn (as the CLI's agent seam does).
    db.connect()
        .execute("CREATE USER dana PASSWORD 'pw'")
        .unwrap();
    let mut conn = db.connect();
    conn.impersonate("dana").unwrap();
    let (tool, internal) = memory_policies(&ServePolicy::default());
    conn.run_command(
        "memory",
        "add --speaker user --conversation c1 the warehouse runs on pinkdb",
        &tool,
        &internal,
        &mut Quiet,
    )
    .expect("memory add");

    // Work waits for dana; with no provider the drain says so (once per
    // tick) and the turn stays queued.
    let err = drain_once(&db).unwrap_err();
    assert!(err.to_lowercase().contains("provider"), "{err}");
    let pending = db
        .connect()
        .execute("SELECT owner FROM pinkdb_memory.l0_turns WHERE distilled = false")
        .unwrap();
    assert!(format!("{pending:?}").contains("dana"), "{pending:?}");

    // Wave 121e: with a state store, memory keeps its layers there — the
    // warehouse never lists them — and each owner is still the caller.
    let warehouse = Database::new();
    let state = Database::new();
    warehouse.attach_state_store(state.clone()).unwrap();
    warehouse
        .load_extension(library().to_str().unwrap())
        .unwrap();
    warehouse
        .connect()
        .execute("CREATE USER dana PASSWORD 'pw'; CREATE USER eli PASSWORD 'pw'")
        .unwrap();
    for (user, text) in [("dana", "dana likes tea"), ("eli", "eli likes coffee")] {
        let mut conn = warehouse.connect();
        conn.impersonate(user).unwrap();
        conn.run_command(
            "memory",
            &format!("add --speaker user --conversation c1 {text}"),
            &tool,
            &internal,
            &mut Quiet,
        )
        .expect("memory add");
    }
    let listed = warehouse.connect().execute("SHOW DATABASES").unwrap();
    assert!(
        !format!("{listed:?}").contains("pinkdb_memory"),
        "{listed:?}"
    );
    let turns = state
        .connect()
        .execute("SELECT owner, count(*) FROM pinkdb_memory.l0_turns GROUP BY 1 ORDER BY 1")
        .unwrap();
    let turns = format!("{turns:?}");
    assert!(turns.contains("dana") && turns.contains("eli"), "{turns}");
    let err = drain_once(&warehouse).unwrap_err();
    assert!(err.to_lowercase().contains("provider"), "{err}");
}
