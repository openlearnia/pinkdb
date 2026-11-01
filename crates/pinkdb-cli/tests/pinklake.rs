//! CLI coverage for the `pinklake` feature: `.lake save/open/versions/
//! tables/info` end to end over `.pink` data files, read-only lake databases
//! (`ATTACH 'lake:…' (READ_ONLY)`) over the published statistics block, and
//! the `s3` variant.
//!
//! `tests/lake.rs` covers the broader CLI surface; this file focuses on the
//! statistics-only attach and the saved shapes.

#![cfg(feature = "pinklake")]

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
        "pinkdb_cli_pinklake_{tag}_{}_{}",
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

/// `.lake save` publishes a `.pink` lake, `.lake open` loads
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
    assert!(out.contains("format version:  7"), "{out}");
    assert!(out.contains("main.t (2 rows)"), "{out}");
    assert!(out.contains("analytics.u (1 row)"), "{out}");
    // The reloaded values are the inserted ones (NULL included).
    assert!(out.contains("│ 1       │ x       │"), "{out}");
    assert!(out.contains("│ 2       │ NULL    │"), "{out}");

    // The on-disk shape: a v7 catalog (the `.lake tables` line above says
    // so) and `.pink` data files with the engine's own magic.
    let files = data_files(&PathBuf::from(format!("{}.files", lake.display())).join("data"));
    assert_eq!(files.len(), 2);
    for file in &files {
        assert_eq!(file.extension().unwrap(), "pink");
        assert!(
            pinkdb::persist::is_pinkdb06(&std::fs::read(file).unwrap()),
            "{} is not engine `.pink` bytes",
            file.display()
        );
    }
    cleanup(&dir);

    // The `--features pinklake` help describes the lake build and offers
    // lake databases (statistics-only attach).
    let help = run(".help\n.quit\n");
    assert!(help.contains(".lake save"), "{help}");
    assert!(help.contains("catalog format_version 7"), "{help}");
    assert!(
        help.contains("ATTACH 'lake:"),
        "help must offer ATTACH in a pinklake build (statistics-only):\n{help}"
    );
    assert!(
        !help.contains("needs the `pinklake` feature"),
        "help must not claim the lake commands are missing:\n{help}"
    );
}

/// A read-only lake database answers exact COUNT/COUNT(col)/MIN/MAX from the published
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
    let d = common::DECODES;
    let out = run(&format!(
        "ATTACH 'lake:{lake}' AS lake (READ_ONLY);\nUSE lake.lake;\n\
         SELECT COUNT(*), COUNT(i), COUNT(g), COUNT(s) FROM t;\n\
         SELECT MIN(i), MAX(i), MIN(g), MAX(g), MIN(s), MAX(s), MIN(b), MAX(b) FROM t;\n\
         SELECT COUNT(*), COUNT(x), MIN(x) FROM empty;\n{d}\
         SELECT SUM(i) FROM t;\n{d}\
         SELECT SUM(i), SUM(g) FROM t;\n{d}\
         .quit\n",
        lake = lake.display()
    ));
    assert!(!out.contains("Error"), "{out}");
    // The stats answers.
    assert!(
        out.contains("│ 3            │ 3        │ 2        │ 2        │"),
        "{out}"
    );
    // The fast paths decoded nothing; each fallback decoded only the columns
    // it needed.
    // `.pink` files decode only the columns a query needs (wave 102): the
    // first fallback reads one column, the second a further one.
    let decodes = common::decode_lines(&out);
    assert_eq!(
        decodes,
        vec![
            "decodes: 0 loads, 0 columns, 0 files",
            "decodes: 1 loads, 1 columns, 1 files",
            "decodes: 2 loads, 2 columns, 2 files",
        ],
        "fast paths must not decode; projected decodes, one column at a time:\n{out}"
    );
    cleanup(&dir);
}

/// The core lean equivalence gate: the same data, the same query battery,
/// three arms — in-memory, `.lake open`, a read-only lake database — produce identical
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
        "ATTACH 'lake:{lake}' AS lake (READ_ONLY);\nUSE lake.lake;\n{queries}.quit\n",
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
        "the lean lake database diverged from the in-memory answers"
    );

    // The fast paths are statistics-only: after COUNT/COUNT(col)/MIN/MAX the
    // decode counters stay at zero, and the first fallback materializes once.
    let d = common::DECODES;
    let stats_arm = run(&format!(
        "ATTACH 'lake:{lake}' AS lake (READ_ONLY);\nUSE lake.lake;\n\
         SELECT COUNT(*) FROM t;\n\
         SELECT MIN(i), MAX(i), COUNT(s), MIN(b), MAX(b) FROM t;\n{d}\
         SELECT SUM(i) FROM t;\n{d}\
         SELECT AVG(i) FROM t;\n{d}\
         .quit\n",
        lake = lake.display()
    ));
    assert_eq!(
        common::decode_lines(&stats_arm),
        vec![
            "decodes: 0 loads, 0 columns, 0 files",
            "decodes: 1 loads, 1 columns, 1 files",
            "decodes: 1 loads, 1 columns, 1 files",
        ],
        "fast paths must not decode; the fallback decodes only its column once \
         (wave 102), then the cache serves it:\n{stats_arm}"
    );
    cleanup(&dir);
}

/// The pinklake build over mock S3: `.lake save`/`open` work against
/// `s3://bucket/prefix` and the data files are `.pink` objects.
#[cfg(feature = "s3")]
#[test]
fn lean_s3_save_open_roundtrip() {
    let mock = mock_s3::MockS3::start();
    let dir = scratch("s3");
    let catalog = format!("pink:{}", dir.join("catalog.pink").display());
    let url = mock.url("cli-lite-bucket", "lakes/lite");
    let out = run_mock(
        &mock,
        &format!(
            "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
             INSERT INTO t VALUES (1, 'x'), (2, NULL);\n\
             .lake save {catalog} data {url}\n\
             .lake open {catalog}\n\
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
    cleanup(&dir);
}

/// `.lake partition` in the lean build (wave 66): `.pink` commits split by
/// partition value and reads prune whole `.pink` files (which carry their
/// zone maps in the catalog's CRC-protected per-file blocks).
#[test]
fn lean_partition_splits_commits_and_prunes() {
    let dir = scratch("partition");
    let lake = dir.join("lake");
    // Wave 68: inlining is on by default; this test pins the file shape via
    // the handle-default env knob (which publishes no version of its own).
    let out = run_with_env(
        &format!(
            "CREATE TABLE t (id INTEGER, region VARCHAR);\n\
         INSERT INTO t VALUES (1, 'east'), (2, 'west');\n\
         .lake save {lake}\n\
         .lake partition {lake} t by (region)\n\
         ATTACH 'lake:{lake}' AS lk;\n\
         USE lk;\n\
         INSERT INTO t VALUES (3, 'south'), (4, 'south'), (5, 'north');\n\
         .quit\n",
            lake = lake.display()
        ),
        &[("PINKDB_LAKE_INLINING_ROW_LIMIT", "0")],
    );
    assert!(out.contains("Partitioned main.t by (region)"), "{out}");
    assert!(!out.contains("Error"), "{out}");

    // Read-only attach: a partitioned predicate decodes only the surviving
    // files (the v2 file plus the matching partition file).
    let out = run(&format!(
        "ATTACH 'lake:{lake}' AS lk (READ_ONLY);\nUSE lk;\n\
         SELECT id FROM t WHERE region = 'north';\n{d}\
         .quit\n",
        lake = lake.display(),
        d = common::DECODES,
    ));
    assert!(out.contains("│ 5 "), "{out}");
    assert_eq!(
        common::decode_lines(&out),
        vec!["decodes: 1 loads, 2 columns, 2 files"],
        "{out}"
    );
    cleanup(&dir);
}

/// Wave 67 in the lean build: `CALL ducklake_merge_adjacent_files` consolidates
/// `.pink` data files, `ducklake_expire_snapshots`/`ducklake_cleanup_old_files`
/// prune the metadata and unreferenced objects, and the read functions report them.
#[test]
fn lean_maintenance_merge_expire_cleanup() {
    let dir = scratch("maint");
    let lake = dir.join("lake");
    // Wave 68: inlining is on by default; this test pins the file shape.
    run_with_env(
        &format!(
            "CREATE TABLE t (id INTEGER);\n\
         INSERT INTO t VALUES (1), (2);\n\
         .lake save {lake}\n\
         ATTACH 'lake:{lake}' AS lk;\n\
         INSERT INTO lk.t VALUES (3);\n\
         DETACH lk;\n\
         .quit\n",
            lake = lake.display()
        ),
        &[("PINKDB_LAKE_INLINING_ROW_LIMIT", "0")],
    );
    let data_files = || -> Vec<String> {
        std::fs::read_dir(PathBuf::from(format!("{}.files", lake.display())).join("data/main.t"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect()
    };
    let out = run(&format!(
        "ATTACH 'lake:{lake}' AS lk;\n\
         CALL ducklake_merge_adjacent_files('lk');\n\
         SELECT 'files=' || CAST(COUNT(*) AS VARCHAR) AS files \
           FROM ducklake_list_files('lk', 't');\n\
         SELECT 'rows=' || CAST(COUNT(*) AS VARCHAR) AS row_count FROM lk.t;\n\
         CALL ducklake_expire_snapshots('lk', versions := [1, 2]);\n\
         SELECT 'left=' || CAST(COUNT(*) AS VARCHAR) AS left_count \
           FROM ducklake_snapshots('lk') WHERE snapshot_id IN (1, 2);\n\
         CALL ducklake_cleanup_old_files('lk', cleanup_all := true, dry_run := true);\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("files=1"), "{out}");
    assert!(out.contains("rows=3"), "{out}");
    assert!(out.contains("left=0"), "{out}");
    assert!(!out.contains("Error:"), "{out}");
    // The dry run reports both merged sources but deletes nothing.
    assert_eq!(data_files().len(), 3, "{:?}", data_files());

    let out = run(&format!(
        "ATTACH 'lake:{lake}' AS lk;\n\
         CALL ducklake_cleanup_old_files('lk', cleanup_all := true);\n\
         SELECT 'version=' || CAST(id + 1 AS VARCHAR) AS version \
           FROM ducklake_current_snapshot('lk');\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("version=4"), "{out}");
    assert!(!out.contains("Error:"), "{out}");
    // The consolidated data file is a `.pink` file.
    let data = data_files();
    assert_eq!(data.len(), 1, "{data:?}");
    assert!(data[0].ends_with(".pink"), "{data:?}");
    cleanup(&dir);
}

fn catalog_sql(lake_dir: &Path, sql: &str) {
    let helper = lake_dir.join("catalog.pink");
    let path = if helper.exists() || lake_dir.join("catalog.pink.wal").exists() {
        helper
    } else {
        lake_dir.to_path_buf()
    };
    let mut db = pinkdb::PinkDb::open_durable(&path.display().to_string()).unwrap();
    db.execute(sql).unwrap();
}

#[test]
fn lean_attach_refuses_stats_less_and_corrupted_lakes() {
    let dir = scratch("attach_refuse");

    // A corrupted (undecodable) statistic is refused fail-closed and names
    // the column; `.lake open` still reads the data.
    let tampered = dir.join("tampered");
    run(&format!(
        "CREATE TABLE fid (i INTEGER);\n\
         INSERT INTO fid VALUES (1), (5), (9);\n\
         .lake save {}\n\
         .quit\n",
        tampered.display()
    ));
    catalog_sql(
        &tampered,
        "UPDATE ducklake_file_column_stats SET min_value = 'not-an-int' \
         WHERE column_id = (SELECT MIN(column_id) FROM ducklake_column);",
    );
    let out = run(&format!(
        "ATTACH 'lake:{}' AS lk (READ_ONLY);\n.quit\n",
        tampered.display()
    ));
    assert!(out.contains("does not decode"), "{out}");

    // No statistics rows at all: a clear stats-less refusal.
    let blank = dir.join("stats_less");
    run(&format!(
        "CREATE TABLE fid (i INTEGER);\nINSERT INTO fid VALUES (1);\n\
         .lake save {}\n.quit\n",
        blank.display()
    ));
    catalog_sql(&blank, "DELETE FROM ducklake_file_column_stats;");
    let out = run(&format!(
        "ATTACH 'lake:{}' AS lk (READ_ONLY);\n.quit\n",
        blank.display()
    ));
    assert!(out.contains("without a statistics block"), "{out}");
    assert!(out.contains("use .lake open"), "{out}");

    cleanup(&dir);
}
