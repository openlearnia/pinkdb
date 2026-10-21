//! CLI coverage for the `parquetlake` feature (wave 87): `.lake format
//! parquet` selects the provider, `.lake save` writes `.parquet` data files,
//! `.lake open` reads them back, `.lake attach` answers from the manifest
//! statistics, and an unknown format id is a clean error.
//!
//! Only compiled when the `parquetlake` feature is on.

#![cfg(feature = "parquetlake")]

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_parquetlake_{tag}_{}_{}",
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

/// The CLI under test: `PINKDB_CLI_BIN` wins over the test-profile binary.
fn cli_binary() -> String {
    std::env::var("PINKDB_CLI_BIN").unwrap_or_else(|_| env!("CARGO_BIN_EXE_pinkdb").to_string())
}

fn run(cmds: &str) -> String {
    let (out, status) = common::run_merged(Command::new(cli_binary()), cmds);
    common::assert_script_exit(status);
    out
}

/// Every data file under `dir/data`, recursively, sorted.
fn data_files(dir: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, out);
            } else {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(&dir.join("data"), &mut out);
    out.sort();
    out
}

#[test]
fn format_parquet_then_save_and_open_round_trips() {
    let dir = scratch("roundtrip");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (i INTEGER, s VARCHAR);\n\
         INSERT INTO t VALUES (1, 'x'), (2, NULL);\n\
         CREATE DATABASE analytics;\n\
         CREATE TABLE analytics.u (k BIGINT);\n\
         INSERT INTO analytics.u VALUES (7);\n\
         .lake format\n\
         .lake format parquet\n\
         .lake format\n\
         .lake save {lake}\n\
         .lake open {lake}\n\
         SELECT i, s FROM t ORDER BY i;\n\
         SELECT COUNT(*) FROM analytics.u;\n\
         .quit\n",
        lake = lake.display()
    ));
    // The no-argument form names the build default and the registered ids;
    // the id form switches the subsequent save.
    assert!(out.contains("Lake write format: pink"), "{out}");
    assert!(out.contains("Registered formats: pink, parquet"), "{out}");
    assert!(
        out.contains("Lake write format set to \"parquet\""),
        "{out}"
    );
    assert!(out.contains("Saved lake version 2"), "{out}");
    assert!(out.contains("Opened lake version 2"), "{out}");
    assert!(out.contains("│ 1       │ x       │"), "{out}");
    assert!(out.contains("│ 2       │ NULL    │"), "{out}");
    assert!(out.contains("│ 1        │"), "{out}");

    // The lake pins the provider and stores real `.parquet` files.
    let catalog = std::fs::read_to_string(lake.join("catalog.json")).unwrap();
    assert!(catalog.contains("\"format\": \"parquet\""), "{catalog}");
    let files = data_files(&lake);
    assert_eq!(files.len(), 2, "{files:?}");
    for file in &files {
        assert_eq!(file.extension().unwrap(), "parquet", "{file:?}");
        assert!(
            std::fs::read(file).unwrap().starts_with(b"PAR1"),
            "{} is not a parquet file",
            file.display()
        );
    }
    assert!(!dir.join("lake.db").exists());

    cleanup(&dir);
}

#[test]
fn attach_answers_from_the_parquet_lake_statistics() {
    let dir = scratch("attach");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (i INTEGER, s VARCHAR);\n\
         INSERT INTO t VALUES (1, 'x'), (2, NULL), (3, 'z');\n\
         .lake format parquet\n\
         .lake save {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");

    let out = run(&format!(
        ".lake attach {lake}\n\
         SELECT COUNT(*), COUNT(s), MIN(i), MAX(i), MIN(s), MAX(s) FROM t;\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("statistics-only"), "{out}");
    assert!(
        out.contains("│ 3        │ 2        │ 1       │ 3       │ x       │ z       │"),
        "{out}"
    );

    cleanup(&dir);
}

#[test]
fn an_unknown_format_is_a_clear_error_and_changes_nothing() {
    let dir = scratch("unknown");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (i INTEGER);\n\
         INSERT INTO t VALUES (1);\n\
         .lake format nosuch\n\
         .lake format\n\
         .lake save {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("unknown lake format \"nosuch\""), "{out}");
    assert!(out.contains("registered formats: pink, parquet"), "{out}");
    // The failed selection did not change the default.
    assert!(out.contains("Lake write format: pink"), "{out}");
    // The save still happened under the build default (`.pink` files).
    assert!(out.contains("Saved lake version 2"), "{out}");
    let files = data_files(&lake);
    assert_eq!(files.len(), 1, "{files:?}");
    assert_eq!(files[0].extension().unwrap(), "pink", "{files:?}");

    cleanup(&dir);
}
