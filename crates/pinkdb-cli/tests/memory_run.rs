//! Wave 109: `.memory`, driven through the real CLI.
//!
//! The extension does the work (its own tests cover distillation, the ACLs and
//! the bridge); these cover the host contract — the dot-command reaches the
//! loaded extension, the store round-trips, a missing provider is one clear
//! error, and a store without the extension says which file to load.
//!
//! Build the extension first (`cargo build -p pinkdb-memory`); the tests ask
//! `PINKDB_MEMORY_LIB` or the workspace target directory for it.

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use common::run_merged;

fn cli() -> &'static str {
    env!("CARGO_BIN_EXE_pinkdb")
}

fn library_name() -> &'static str {
    if cfg!(target_os = "macos") {
        "libpinkdb_memory.dylib"
    } else if cfg!(target_os = "windows") {
        "pinkdb_memory.dll"
    } else {
        "libpinkdb_memory.so"
    }
}

/// The extension cdylib, resolved (and built if missing) once per test binary.
fn memory_library() -> PathBuf {
    static MEMORY: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    MEMORY.get_or_init(resolve_memory_library).clone()
}

fn resolve_memory_library() -> PathBuf {
    let name = library_name();
    if let Ok(path) = std::env::var("PINKDB_MEMORY_LIB") {
        return PathBuf::from(path);
    }
    // Rebuild before looking: `cargo test -p pinkdb-cli` does not build the
    // extension's cdylib, so a dylib left from an earlier edit would run the
    // old code and the tests would pass against it.
    let _ = Command::new(env!("CARGO"))
        .args(["build", "-q", "-p", "pinkdb-memory"])
        .status();
    let mut candidates = Vec::new();
    let exe = Path::new(cli());
    if let Some(dir) = exe.parent() {
        candidates.push(dir.join(name));
        if let Some(target) = dir.parent() {
            candidates.extend([
                target.join("debug").join(name),
                target.join("release").join(name),
            ]);
        }
    }
    for path in &candidates {
        if path.is_file() {
            return path.clone();
        }
    }
    panic!(
        "no {}: build the extension with `cargo build -p pinkdb-memory` or set PINKDB_MEMORY_LIB",
        name
    );
}

/// A store directory under the test target, removed on drop.
struct Store {
    dir: PathBuf,
    path: PathBuf,
}

impl Store {
    fn new(tag: &str) -> Store {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("memory-cli-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the store directory");
        Store {
            path: dir.join("store.pink"),
            dir,
        }
    }

    /// A CLI invocation on this store. A developer's shell may export a real
    /// provider or key; none of it may reach a child here.
    fn command(&self) -> Command {
        let mut command = Command::new(cli());
        for variable in [
            "PINKDB_AGENT_PROVIDER",
            "PINKDB_AGENT_MODEL",
            "PINKDB_AGENT_API_KEY",
            "PINKDB_MEMORY_DISTILL_EVERY",
            "OPENAI_API_KEY",
            "ANTHROPIC_API_KEY",
            "ZAI_API_KEY",
        ] {
            command.env_remove(variable);
        }
        let root = memory_library()
            .parent()
            .map(|dir| dir.to_path_buf())
            .unwrap_or_default();
        command
            .arg("--allow-load-extension")
            .arg("--extension-root")
            .arg(root);
        command
    }

    /// Open, load the extension, run `lines`, save.
    fn script(&self, lines: &[&str]) -> String {
        let path = self.path.display();
        let mut script = format!(
            ".open {path}\nLOAD EXTENSION '{}';\n",
            memory_library().display()
        );
        for line in lines {
            script.push_str(line);
            script.push('\n');
        }
        script.push_str(&format!(".save {path}\n"));
        script
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn the_help_lists_the_verbs_and_the_database() {
    let store = Store::new("help");
    let (out, status) = run_merged(store.command(), ".memory help\n");
    assert!(status.success(), "{out}");
    for verb in [
        "add", "search", "update", "forget", "distill", "persona", "show", "serve",
    ] {
        assert!(out.contains(verb), "help misses {verb}: {out}");
    }
    assert!(out.contains("pinkdb_memory"), "{out}");
    assert!(out.contains("allow-memory"), "{out}");
    // The same text is reachable as `.help memory`.
    let (alias, _) = run_merged(store.command(), ".help memory\n");
    assert!(alias.contains("The pinkdb agent memory"), "{alias}");
}

#[test]
fn add_search_and_show_round_trip_through_the_store() {
    let store = Store::new("round-trip");
    let script = store.script(&[
        ".memory add my warehouse is TPC-H SF1",
        ".memory search the warehouse",
        ".memory show",
    ]);
    let (out, status) = run_merged(store.command(), &script);
    assert!(status.success(), "{out}");
    assert!(out.contains("added turn t"), "{out}");
    // The search reports its mode and its hits with layer and source; the turn
    // is all there is until a model distills it, so the hit is an L0 one.
    assert!(out.contains("mode: bm25"), "{out}");
    assert!(out.contains("L0 "), "{out}");
    assert!(out.contains("bm25"), "{out}");
    assert!(out.contains("TPC-H SF1"), "{out}");
    assert!(out.contains("turns: 1"), "{out}");
    assert!(out.contains("atoms: 0"), "{out}");

    // A second process reads the same store: the turn was saved, not just
    // kept in memory.
    let (again, status) = run_merged(
        store.command(),
        &store.script(&[".memory show", ".memory search warehouse"]),
    );
    assert!(status.success(), "{again}");
    assert!(again.contains("turns: 1"), "{again}");
    assert!(again.contains("TPC-H SF1"), "{again}");
}

#[test]
fn distill_without_a_provider_is_one_clear_error_and_the_session_survives() {
    let store = Store::new("no-provider");
    let script = store.script(&[
        ".memory add my warehouse is TPC-H SF1",
        ".memory distill",
        ".memory search the warehouse",
    ]);
    // A failed command exits nonzero, which is the point: the operator sees a
    // failure. What matters here is that the session kept going afterwards.
    let (out, _status) = run_merged(store.command(), &script);
    assert!(out.contains("provider"), "{out}");
    // The failed distill did not take the session down: the search after it
    // still answered with the turn and its mode.
    assert!(out.contains("no provider"), "{out}");
    assert!(out.contains("mode: bm25"), "{out}");
    assert!(out.contains("L0 "), "{out}");
}

#[test]
fn a_store_without_the_extension_names_the_library_to_load() {
    let store = Store::new("no-extension");
    let script = format!(
        ".open {}\n.memory add my warehouse is TPC-H SF1\n",
        store.path.display()
    );
    let (out, _status) = run_merged(store.command(), &script);
    assert!(out.contains("the memory extension is not loaded"), "{out}");
    assert!(out.contains(library_name()), "{out}");
}

/// Wave 121e: memory keeps its layers in the state store next to the store
/// file, never in the store a user's SQL reaches.
#[test]
fn memory_lives_in_the_state_store_next_to_the_file() {
    let store = Store::new("state");
    let script = store.script(&[".memory add the warehouse runs on pinkdb"]);
    let (out, status) = run_merged(store.command(), &script);
    assert!(status.success(), "{out}");
    let state = store.dir.join("store.state.pink");
    let state_wal = store.dir.join("store.state.pink.wal");
    assert!(
        state.exists() || state_wal.exists(),
        "no state store next to the file: {out}"
    );
    let path = store.path.display();
    let (listed, status) = run_merged(store.command(), &format!(".open {path}\n.databases\n"));
    assert!(status.success(), "{listed}");
    assert!(!listed.contains("pinkdb_memory"), "{listed}");
    let (atoms, _) = run_merged(
        store.command(),
        &format!(
            ".open {}\nSELECT count(*) FROM pinkdb_memory.l0_turns;\n",
            state.display()
        ),
    );
    assert!(atoms.contains('1'), "{atoms}");
}
