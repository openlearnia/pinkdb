//! CLI coverage for a SQLite lake catalog (wave 96a): `.lake save <catalog>
//! data <dir>`, `.lake open <catalog>`, `.lake attach <catalog>` and the
//! writable attach + `.lake commit <catalog>`, with every metadata object in
//! SQL and the data files in a separate directory.

#![cfg(all(feature = "pinklake", feature = "catalog-sqlite"))]

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_sqlite_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

fn cli_binary() -> String {
    std::env::var("PINKDB_CLI_BIN").unwrap_or_else(|_| env!("CARGO_BIN_EXE_pinkdb").to_string())
}

fn run(cmds: &str) -> String {
    let command = Command::new(cli_binary());
    let (out, status) = common::run_merged(command, cmds);
    common::assert_script_exit(status);
    out
}

/// A catalog URL and data location inside one scratch directory.
fn paths(dir: &Path) -> (String, String) {
    (
        format!("sqlite:{}", dir.join("catalog.db").display()),
        dir.join("data-root").display().to_string(),
    )
}

#[test]
fn save_open_attach_and_commit_through_a_sqlite_catalog() {
    let dir = scratch("roundtrip");
    let (catalog, data) = paths(&dir);

    // Save: the catalog is created and the data location is recorded once.
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
         INSERT INTO t VALUES (1, 'x'), (2, NULL);\n\
         .lake save {catalog} data {data}\n\
         .lake versions {catalog}\n\
         .lake tables {catalog}\n\
         .lake open {catalog}\n\
         SELECT a, b FROM t ORDER BY a;\n\
         .quit\n"
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    assert!(out.contains(&format!("lake: {data}")), "{out}");
    assert!(out.contains("Opened lake version 2"), "{out}");
    assert!(out.contains("│ 1       │ x       │"), "{out}");
    assert!(out.contains("│ 2       │ NULL    │"), "{out}");

    // The metadata is SQL; the data location has the marker and files only.
    assert!(dir.join("catalog.db").is_file(), "the catalog file exists");
    assert!(
        dir.join("data-root").join("lake-data.json").is_file(),
        "the ownership marker is at the data root"
    );
    assert!(
        !dir.join("data-root").join("catalog.json").exists(),
        "no object-store metadata at the data location"
    );

    // Read-only attach by the catalog URL, an explicit `data` open (the moved
    // location override), and detach by the same URL.
    let out = run(&format!(
        ".lake attach {catalog}\n\
         SELECT COUNT(*) FROM t;\n\
         .lake detach {catalog}\n\
         .lake open {catalog} data {data}\n\
         SELECT COUNT(*) FROM t;\n\
         .quit\n"
    ));
    assert!(out.contains("statistics-only"), "{out}");
    assert!(out.contains("│ 2        │"), "{out}");
    assert!(out.contains(&format!("Detached {catalog}")), "{out}");

    // Writable attach + commit, all keyed by the catalog URL.
    let out = run(&format!(
        ".lake attach {catalog} write\n\
         INSERT INTO t VALUES (3, 'z');\n\
         .lake commit {catalog}\n\
         .quit\n"
    ));
    assert!(out.contains("Committed lake version 3"), "{out}");
    assert!(!out.contains("no writable attach"), "{out}");

    // The new version is visible from a fresh session.
    let out = run(&format!(
        ".lake open {catalog}\nSELECT COUNT(*) FROM t;\n.quit\n"
    ));
    assert!(out.contains("Opened lake version 3"), "{out}");
    assert!(out.contains("│ 3        │"), "{out}");

    cleanup(&dir);
}

/// A fresh SQL catalog needs `data <location>` on save; the error names both
/// the flag and the library call instead of creating a lake with no data root.
#[test]
fn a_sqlite_save_without_data_is_explained() {
    let dir = scratch("needs-data");
    let (catalog, _) = paths(&dir);
    let out = run(&format!(".lake save {catalog}\n.quit\n"));
    assert!(out.contains("needs a data location"), "{out}");
    assert!(out.contains("data <"), "{out}");
    cleanup(&dir);
}
