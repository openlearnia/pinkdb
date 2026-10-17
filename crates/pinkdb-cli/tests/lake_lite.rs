//! CLI coverage for the lean `lake-lite` feature: `.lake save/open/versions/
//! tables/info` end to end over v1 `.pink` data files (no Vortex),
//! `.lake attach` over the published statistics block, the refusal of a
//! stats-less v1 lake and of a v2/v3 Vortex lake, and the `s3` variant.
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
    assert!(out.contains("format version:  4"), "{out}");
    assert!(out.contains("main.t (2 rows)"), "{out}");
    assert!(out.contains("analytics.u (1 row)"), "{out}");
    // The reloaded values are the inserted ones (NULL included).
    assert!(out.contains("│ 1       │ x       │"), "{out}");
    assert!(out.contains("│ 2       │ NULL    │"), "{out}");

    // The on-disk shape: a v1 catalog, no `format` fields, `.pink` data
    // files with the engine's own magic.
    let catalog = std::fs::read_to_string(lake.join("catalog.json")).unwrap();
    assert!(catalog.contains("\"format_version\": 4"), "{catalog}");
    assert!(catalog.contains("\"format\": \"pink\""), "{catalog}");
    let files = data_files(&lake.join("data"));
    assert_eq!(files.len(), 2);
    for file in &files {
        assert_eq!(file.extension().unwrap(), "pink");
        assert!(
            std::fs::read(file).unwrap().starts_with(b"PINKDB05"),
            "{} is not engine v5 `.pink` bytes",
            file.display()
        );
    }
    cleanup(&dir);

    // The lean `--features lake-lite` help describes the lean lake and
    // offers `.lake attach` (over the v1 statistics block).
    let help = run(".help\n.quit\n");
    assert!(help.contains(".lake save"), "{help}");
    assert!(help.contains("lean lake build"), "{help}");
    assert!(
        help.contains(".lake attach <dir"),
        "help must offer attach in a lean build (statistics-only):\n{help}"
    );
    assert!(
        !help.contains("not built in the lean lake"),
        "help must not claim attach is missing:\n{help}"
    );
}

/// `.lake attach` answers exact COUNT/COUNT(col)/MIN/MAX from the published
/// statistics without decoding (decode counters stay at zero), and a fallback
/// query materializes the table exactly once and caches it.
#[test]
fn lean_attach_answers_from_statistics_without_decoding() {
    let dir = scratch("attach");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE DATABASE lake;\nUSE lake;\n\
         CREATE TABLE t (i INTEGER, g BIGINT, d DOUBLE, s VARCHAR, b BOOLEAN);\n\
         INSERT INTO t VALUES (1, 10, 1.5, 'x', true), (2, NULL, NULL, NULL, false), \
         (3, -9223372036854775807, -2.5, 'a', true);\n\
         CREATE TABLE empty (x INTEGER);\n\
         .lake save {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");

    // A fresh process: attach from the statistics block, then fast paths.
    let out = run(&format!(
        ".lake attach {lake}\nUSE lake;\n\
         SELECT COUNT(*), COUNT(i), COUNT(g), COUNT(s) FROM t;\n\
         SELECT MIN(i), MAX(i), MIN(g), MAX(g), MIN(s), MAX(s), MIN(b), MAX(b) FROM t;\n\
         SELECT COUNT(*), COUNT(x), MIN(x) FROM empty;\n\
         .lake attached\n\
         SELECT SUM(i) FROM t;\n\
         .lake attached\n\
         SELECT SUM(i), SUM(g) FROM t;\n\
         .lake attached\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("statistics-only"), "{out}");
    assert!(!out.contains("Error"), "{out}");
    // The stats answers.
    assert!(
        out.contains("│ 3        │ 3        │ 2        │ 2        │"),
        "{out}"
    );
    // The fast paths decoded nothing; the fallback materialized once and the
    // second fallback reused the cache.
    let zero = out.matches("decodes: 0 loads, 0 columns").count();
    assert_eq!(zero, 1, "fast paths must not decode:\n{out}");
    assert_eq!(
        out.matches("decodes: 1 load, 5 columns").count(),
        2,
        "one decode, then cached:\n{out}"
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

/// The core lean equivalence gate: the same data, the same query battery,
/// three arms — in-memory, `.lake open`, `.lake attach` — produce identical
/// transcripts, and the attach arm really ran the statistics fast paths
/// (zero data-file decodes for the COUNT/COUNT(col)/MIN/MAX shapes).
#[test]
fn lean_attach_answers_equal_open_and_in_memory() {
    let dir = scratch("attach_equiv");
    let lake = dir.join("lake");
    let corpus = common::attach_corpus(&dir);
    let queries = common::attach_corpus_queries();

    let out = run(&format!(
        "{corpus}.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");

    let in_memory = run(&format!("{corpus}{queries}.quit\n"));
    let opened = run(&format!(
        ".lake open {lake}\nUSE lake;\n{queries}.quit\n",
        lake = lake.display()
    ));
    let attached = run(&format!(
        ".lake attach {lake}\nUSE lake;\n{queries}.quit\n",
        lake = lake.display()
    ));

    let base = common::tail_after_marker(&in_memory);
    assert_eq!(
        common::tail_after_marker(&opened),
        base,
        "`.lake open` diverged from the in-memory answers"
    );
    assert_eq!(
        common::tail_after_marker(&attached),
        base,
        "lean `.lake attach` diverged from the in-memory answers"
    );
    assert!(attached.contains("statistics-only"), "{attached}");

    // The fast paths are statistics-only: after COUNT/COUNT(col)/MIN/MAX the
    // decode counters stay at zero, and the first fallback materializes once.
    let stats_arm = run(&format!(
        ".lake attach {lake}\nUSE lake;\n\
         SELECT COUNT(*) FROM t;\n\
         SELECT MIN(i), MAX(i), COUNT(s), MIN(b), MAX(b) FROM t;\n\
         .lake attached\n\
         SELECT SUM(i) FROM t;\n\
         .lake attached\n\
         SELECT AVG(i) FROM t;\n\
         .lake attached\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(
        stats_arm.contains("decodes: 0 loads, 0 columns"),
        "fast paths must not decode:\n{stats_arm}"
    );
    assert_eq!(
        stats_arm.matches("decodes: 1 load, 8 columns").count(),
        2,
        "the fallbacks decode the whole table (v1 has no projected reader) once, then cache:\n{stats_arm}"
    );
    cleanup(&dir);
}

/// A v1 lake saved before statistics existed is refused with a clear message
/// (never a guess); a corrupted statistics block is refused fail-closed while
/// `.lake open` still loads the data.
#[test]
fn lean_attach_refuses_stats_less_and_corrupted_lakes() {
    let dir = scratch("attach_refuse");

    // The wave-34..45 v1 fixture: no stats block anywhere.
    let stats_less =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../pink-lake/tests/fixtures/lake_v1_v4");
    let out = run(&format!(".lake attach {}\n.quit\n", stats_less.display()));
    assert!(out.contains("without a statistics block"), "{out}");
    assert!(out.contains("use .lake open"), "{out}");

    // A corrupted statistics block: an edited min without a re-sealed CRC.
    // (No JSON parser here: a textual edit of the manifest is enough to
    // break the block's CRC.)
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../pink-lake/tests/fixtures/lake_v1_v4_stats");
    let tampered = dir.join("tampered");
    copy_dir(&fixture, &tampered);
    let catalog_path = tampered.join("catalog.json");
    let text = std::fs::read_to_string(&catalog_path).unwrap();
    let fid = text.find("\"main.fid\"").expect("main.fid in catalog");
    let min_at = fid + text[fid..].find("\"min\": \"").expect("a min statistic") + 8;
    let min_end = min_at + text[min_at..].find('"').expect("min value end");
    let tampered_text = format!("{}{}{}", &text[..min_at], "999999999", &text[min_end..]);
    assert_ne!(tampered_text, text, "the edit must change the manifest");
    std::fs::write(&catalog_path, tampered_text).unwrap();

    let out = run(&format!(".lake attach {}\n.quit\n", tampered.display()));
    assert!(out.contains("failed its checksum"), "{out}");
    assert!(out.contains("use .lake open"), "{out}");

    // `.lake open` reads the data files, not the statistics: it still loads.
    let out = run(&format!(
        ".lake open {}\nSELECT COUNT(*) FROM main.fid;\n.quit\n",
        tampered.display()
    ));
    assert!(out.contains("Opened lake version 2"), "{out}");
    assert!(out.contains("│ 6 "), "{out}");
    cleanup(&dir);
}

/// The cross-build pin: the checked-in `lake_v1_v4_stats` fixture (a v1 lake
/// with statistics blocks, written by this lean writer and attached by the
/// full build in `tests/lake.rs`) attaches in the lean CLI with answers
/// identical to `.lake open`.
#[test]
fn lean_cli_attaches_the_cross_build_stats_fixture() {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../pink-lake/tests/fixtures/lake_v1_v4_stats");
    let queries = "SELECT '===BEGIN===' AS marker;\n\
         SELECT COUNT(*), COUNT(s), MIN(a), MAX(a) FROM main.big;\n\
         SELECT MIN(s), MAX(s) FROM main.big;\n\
         SELECT COUNT(*), COUNT(i), MIN(i), MAX(i), MIN(g), MAX(g), MIN(v), MAX(v) FROM main.fid;\n\
         SELECT MIN(b), MAX(b), MIN(dt), MAX(dt), MIN(ts), MAX(ts) FROM main.fid;\n\
         SELECT MIN(d), MAX(d), COUNT(d) FROM main.fid;\n\
         SELECT COUNT(*), COUNT(x), COUNT(y), MIN(x), MAX(y) FROM main.empty;\n\
         SELECT COUNT(*), MIN(k), MAX(k) FROM analytics.u;\n\
         SELECT COUNT(*), MIN(z), MAX(z) FROM analytics.\"a.b\";\n\
         .quit\n";
    let opened = run(&format!(".lake open {}\n{queries}", fixture.display()));
    let attached = run(&format!(".lake attach {}\n{queries}", fixture.display()));
    assert!(attached.contains("statistics-only"), "{attached}");
    assert_eq!(
        common::tail_after_marker(&attached),
        common::tail_after_marker(&opened),
        "the lean attach diverged from `.lake open` over the cross-build fixture"
    );
}

/// Recursively copy a directory (the fixtures are small).
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dst = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &dst);
        } else {
            std::fs::copy(entry.path(), dst).unwrap();
        }
    }
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

/// `.lake partition` in the lean build (wave 66): `.pink` commits split by
/// partition value and reads prune whole `.pink` files (which carry their
/// zone maps in the catalog's CRC-protected per-file blocks).
#[test]
fn lean_partition_splits_commits_and_prunes() {
    let dir = scratch("partition");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (id INTEGER, region VARCHAR);\n\
         INSERT INTO t VALUES (1, 'east'), (2, 'west');\n\
         .lake save {lake}\n\
         .lake partition {lake} t by (region)\n\
         .lake attach {lake} write\n\
         INSERT INTO t VALUES (3, 'south'), (4, 'south'), (5, 'north');\n\
         .lake commit {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Partitioned main.t by (region)"), "{out}");
    assert!(out.contains("2 files: 2 data, 0 delete"), "{out}");
    let catalog = std::fs::read_to_string(lake.join("catalog.json")).unwrap();
    assert!(catalog.contains("\"partition_by\""), "{catalog}");
    assert!(catalog.contains(".pink"), "{catalog}");
    assert!(!catalog.contains(".vortex"), "{catalog}");

    // Read-only attach: a partitioned predicate decodes only the surviving
    // files (the v2 file plus the matching partition file).
    let out = run(&format!(
        ".lake attach {lake}\n\
         SELECT id FROM t WHERE region = 'north';\n\
         .lake attached\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Attached"), "{out}");
    assert!(out.contains("│ 5 "), "{out}");
    assert!(out.contains("decodes: 1 load, 2 columns, 2 files"), "{out}");
    cleanup(&dir);
}
