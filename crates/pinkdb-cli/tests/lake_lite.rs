//! CLI coverage for the lean `lake-lite` feature: `.lake save/open/versions/
//! tables/info` end to end over v1 `.pink` data files (no Vortex), the
//! `.lake attach` feature error, the refusal of a v2/v3 Vortex lake, and the
//! `s3` variant.
//!
//! This file only compiles when `lake-lite` is on and `lake` (the full,
//! Vortex-enabled feature) is not; the full feature-on behavior is covered by
//! `tests/lake.rs` and `tests/lake_s3.rs`.

#![cfg(all(feature = "lake-lite", not(feature = "lake")))]

#[cfg(feature = "s3")]
#[path = "../../pink-lake/src/mock_s3.rs"]
mod mock_s3;

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one test.
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_lake_lite_{tag}_{}_{}",
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

/// Run the REPL to a clean exit and return its merged output.
fn run(cmds: &str) -> String {
    run_with_env(cmds, &[])
}

/// [`run`] with extra environment variables.
fn run_with_env(cmds: &str, env: &[(&str, &str)]) -> String {
    let mut command = Command::new(cli_binary());
    for (key, value) in env {
        command.env(key, value);
    }
    let (out, status) = common::run_merged(command, cmds);
    common::assert_script_exit(status);
    out
}

/// Like [`run`], but with the `AWS_*` environment cleared first (mock S3).
#[cfg(feature = "s3")]
fn run_mock(mock: &mock_s3::MockS3, cmds: &str) -> String {
    let mut command = Command::new(cli_binary());
    for name in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_S3_FORCE_PATH_STYLE",
        "PINKDB_S3_ALLOW_DEGRADED",
        "PINKDB_S3_ALLOW_HTTP",
    ] {
        command.env_remove(name);
    }
    for (key, value) in [
        ("AWS_ACCESS_KEY_ID", mock.access_key_id()),
        ("AWS_SECRET_ACCESS_KEY", "mock-secret".to_string()),
        ("AWS_ENDPOINT_URL", mock.endpoint()),
        ("AWS_ENDPOINT_URL_S3", mock.endpoint()),
        ("AWS_REGION", "auto".to_string()),
        ("PINKDB_S3_ALLOW_HTTP", "1".to_string()),
    ] {
        command.env(key, value);
    }
    let (out, status) = common::run_merged(command, cmds);
    common::assert_script_exit(status);
    out
}

/// Every data file under `dir`, recursively, sorted.
fn data_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(data_files(&path));
        } else {
            out.push(path);
        }
    }
    out.sort();
    out
}

/// `.lake save` publishes a v1 lake (no Vortex anywhere), `.lake open` loads
/// it back, and the metadata commands describe it.
#[test]
fn lean_save_open_versions_tables_info() {
    let dir = scratch("roundtrip");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
         INSERT INTO t VALUES (1, 'x'), (2, NULL);\n\
         CREATE DATABASE analytics;\n\
         CREATE TABLE analytics.u (k BIGINT);\n\
         INSERT INTO analytics.u VALUES (7);\n\
         .lake save {lake}\n\
         .lake versions {lake}\n\
         .lake tables {lake}\n\
         .lake info {lake}\n\
         .lake open {lake}\n\
         SELECT a, b FROM t ORDER BY a;\n\
         SELECT COUNT(*) FROM analytics.u;\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    assert!(out.contains("2 tables, 3 rows"), "{out}");
    assert!(out.contains("Opened lake version 2"), "{out}");
    assert!(out.contains("2 (current)"), "{out}");
    assert!(out.contains("format version:  1"), "{out}");
    assert!(out.contains("main.t (2 rows)"), "{out}");
    assert!(out.contains("analytics.u (1 row)"), "{out}");
    // The reloaded values are the inserted ones (NULL included).
    assert!(out.contains("│ 1       │ x       │"), "{out}");
    assert!(out.contains("│ 2       │ NULL    │"), "{out}");

    // The on-disk shape: a v1 catalog, no `format` fields, `.pink` data
    // files with the engine's own magic.
    let catalog = std::fs::read_to_string(lake.join("catalog.json")).unwrap();
    assert!(catalog.contains("\"format_version\": 1"), "{catalog}");
    assert!(!catalog.contains("\"format\""), "{catalog}");
    let files = data_files(&lake.join("data"));
    assert_eq!(files.len(), 2);
    for file in &files {
        assert_eq!(file.extension().unwrap(), "pink");
        assert!(
            std::fs::read(file).unwrap().starts_with(b"PINKDB04"),
            "{} is not engine v4 `.pink` bytes",
            file.display()
        );
    }
    cleanup(&dir);

    // The lean `--features lake-lite` help describes the lean lake, and does
    // not advertise `.lake attach`.
    let help = run(".help\n.quit\n");
    assert!(help.contains(".lake save"), "{help}");
    assert!(help.contains("lean lake build"), "{help}");
    assert!(
        !help.contains(".lake attach <dir"),
        "help must not offer attach in a lean build:\n{help}"
    );
}

/// `.lake attach` reports the missing full-lake feature; it never pretends a
/// v1 lake has statistics.
#[test]
fn lean_attach_reports_the_missing_feature() {
    let dir = scratch("attach");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (1);\n\
         .lake save {lake}\n\
         .lake attach {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains("Error: .lake attach is not available"),
        "{out}"
    );
    assert!(out.contains("built without Vortex format support"), "{out}");
    assert!(
        out.contains("cargo build -p pinkdb-cli --features lake"),
        "{out}"
    );
    cleanup(&dir);
}

/// A v2/v3 Vortex lake is refused by every `.lake` command with the clear
/// feature error (never "corrupt" and never a wrong-format guess).
#[test]
fn vortex_lakes_are_refused_with_a_feature_error() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../pink-lake/tests/fixtures/lake_v2");
    for cmd in [".lake open", ".lake versions", ".lake tables", ".lake info"] {
        let out = run(&format!("{cmd} {}\n.quit\n", fixture.display()));
        assert!(
            out.contains("built without Vortex format support"),
            "{cmd} must name the missing feature:\n{out}"
        );
        assert!(
            !out.to_lowercase().contains("corrupt"),
            "{cmd} must not read as corrupt:\n{out}"
        );
    }

    // And saving on top of a Vortex lake is refused too (before any write).
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\n.lake save {}\n.quit\n",
        fixture.display()
    ));
    assert!(out.contains("built without Vortex format support"), "{out}");
}

/// The lean lake over mock S3: `.lake save`/`open` work against
/// `s3://bucket/prefix` and the data files are `.pink` objects.
#[cfg(feature = "s3")]
#[test]
fn lean_s3_save_open_roundtrip() {
    let mock = mock_s3::MockS3::start();
    let url = mock.url("cli-lite-bucket", "lakes/lite");
    let out = run_mock(
        &mock,
        &format!(
            "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
             INSERT INTO t VALUES (1, 'x'), (2, NULL);\n\
             .lake save {url}\n\
             .lake open {url}\n\
             SELECT b FROM t ORDER BY a;\n\
             .quit\n"
        ),
    );
    assert!(out.contains("Saved lake version 2"), "{out}");
    assert!(out.contains("Opened lake version 2"), "{out}");
    assert!(out.contains("│ x "), "{out}");
    let keys = mock.keys();
    assert!(
        keys.iter().any(|k| k.ends_with(".pink")),
        "expected `.pink` objects: {keys:?}"
    );
    assert!(
        !keys.iter().any(|k| k.ends_with(".vortex")),
        "a lean build must write no Vortex objects: {keys:?}"
    );
}
