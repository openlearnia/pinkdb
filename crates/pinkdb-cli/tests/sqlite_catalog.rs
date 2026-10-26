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
        !dir.join("data-root").join("catalog.db").exists(),
        "the catalog stays at its own location, not in the data root"
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

/// A fresh SQLite catalog defaults its data root to DuckLake's
/// `<catalog path>.files/` when `data` is omitted, and records it so later
/// opens need only the catalog URL.
#[test]
fn a_sqlite_save_without_data_uses_the_default_data_root() {
    let dir = scratch("default-data");
    let (catalog, _) = paths(&dir);
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (1);\n\
         .lake save {catalog}\n\
         .quit\n"
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    let data = dir.join("catalog.db.files");
    assert!(out.contains(&format!("lake: {}", data.display())), "{out}");
    assert!(
        data.join("lake-data.json").is_file(),
        "the default data root sits next to the catalog"
    );
    assert!(
        data.join("data").is_dir(),
        "the data files are under the default root"
    );

    let out = run(&format!(
        ".lake open {catalog}\nSELECT COUNT(*) FROM t;\n.quit\n"
    ));
    assert!(out.contains("Opened lake version 2"), "{out}");
    assert!(out.contains("│ 1        │"), "{out}");
    cleanup(&dir);
}

/// Lake-wide maintenance and settings through the CLI: the SQL catalog must
/// see its tables, not a table-less head. Each command's effect is checked by
/// a later read, not just by the absence of an error.
#[test]
fn maintenance_and_settings_commands_work() {
    let dir = scratch("maintenance");
    let (catalog, data) = paths(&dir);

    // Flush: three inlined rows become one data file; values survive.
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
         INSERT INTO t VALUES (1, 'one');\n\
         .lake save {catalog} data {data}\n\
         .lake attach {catalog} write\n\
         INSERT INTO t VALUES (2, 'two'), (3, 'three'), (4, 'four');\n\
         .lake commit {catalog}\n\
         .lake detach {catalog}\n\
         .lake flush {catalog}\n\
         .lake open {catalog}\n\
         SELECT count(*) FROM t;\n\
         SELECT a, b FROM t ORDER BY a;\n\
         .quit\n"
    ));
    assert!(out.contains("Flushed 3 inlined rows"), "{out}");
    assert!(out.contains("│ 4        │"), "{out}");
    assert!(out.contains("│ 4       │ four    │"), "{out}");
    assert!(!out.contains("Error"), "{out}");

    // Merge / rewrite / settings on a second catalog, with inlining off so
    // every commit is a data file.
    let (maint, maint_data) = (
        format!("{}maint", catalog),
        dir.join("maintenance-data").display().to_string(),
    );
    let mut script = format!(
        "CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (1);\n\
         .lake save {maint} data {maint_data}\n\
         .lake inlining {maint} t 0\n\
         .lake attach {maint} write\n"
    );
    for i in 2..6 {
        script.push_str(&format!(
            "INSERT INTO t VALUES ({i});\n.lake commit {maint}\n"
        ));
    }
    script.push_str(&format!(
        ".lake detach {maint}\n\
         .lake merge {maint}\n\
         .lake attach {maint} write\n\
         DELETE FROM t WHERE a = 1;\n\
         .lake commit {maint}\n\
         .lake detach {maint}\n\
         .lake rewrite {maint} delete_threshold=0.0\n\
         .lake partition {maint} t by (a)\n\
         .lake inlining {maint} t 5\n\
         .lake open {maint}\n\
         SELECT count(*) FROM t;\n\
         .quit\n"
    ));
    let out = run(&script);
    assert!(out.contains("Merged"), "{out}");
    assert!(!out.contains("Nothing to merge"), "{out}");
    assert!(out.contains("Rewrote"), "{out}");
    assert!(!out.contains("Nothing to rewrite"), "{out}");
    assert!(out.contains("Partitioned"), "{out}");
    assert!(out.contains("5 rows are stored"), "{out}");
    assert!(out.contains("│ 4        │"), "{out}");
    assert!(!out.contains("Error"), "{out}");

    // Register an existing data file written by another lake.
    let source_catalog = format!("{}source", catalog);
    let source_data = dir.join("source-data").display().to_string();
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (7), (8);\n\
         .lake save {source_catalog} data {source_data}\n\
         .quit\n"
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    let source_dir = std::path::Path::new(&source_data).join("data/main.t");
    let file = std::fs::read_dir(&source_dir)
        .expect("source data files")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .find(|name| name.ends_with(".pink"))
        .expect("one .pink data file");
    let rel = format!("data/main.t/{file}");
    let target_catalog = format!("{}target", catalog);
    let target_data = dir.join("target-data").display().to_string();
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\n\
         .lake save {target_catalog} data {target_data}\n\
         .quit\n"
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    let target_file = std::path::Path::new(&target_data)
        .join("data/main.t")
        .join(&file);
    std::fs::create_dir_all(target_file.parent().unwrap()).unwrap();
    std::fs::copy(source_dir.join(&file), &target_file).unwrap();
    let out = run(&format!(
        ".lake add_data_files {target_catalog} t {rel}\n\
         .lake open {target_catalog}\n\
         SELECT count(*) FROM t;\n\
         .quit\n"
    ));
    assert!(out.contains("Registered"), "{out}");
    assert!(out.contains("2 rows"), "{out}");
    assert!(out.contains("│ 2        │"), "{out}");
    assert!(!out.contains("Error"), "{out}");

    cleanup(&dir);
}
