//! CLI coverage for the opt-in `lake` feature: `.lake save/open/versions/
//! tables/info` end to end, in a temp lake directory.
//!
//! This file only compiles when the feature is ON; the feature-less behavior
//! is covered by `tests/lake_disabled.rs`.
//!
//! Set `PINKDB_CLI_BIN` to run the whole file against another CLI build
//! (e.g. a release binary) — the overflow tests then prove release behavior.

#![cfg(feature = "vortexlake")]

mod common;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one test.
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_lake_{tag}_{}_{}",
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

/// The CLI under test: `PINKDB_CLI_BIN` wins over the test-profile binary so
/// the same suite can be pointed at a release build.
fn cli_binary() -> String {
    std::env::var("PINKDB_CLI_BIN").unwrap_or_else(|_| env!("CARGO_BIN_EXE_pinkdb").to_string())
}

/// Run the REPL to a clean exit and return its stdout.
fn run(cmds: &str) -> String {
    run_with_env(cmds, &[])
}

/// [`run`] with extra environment variables (e.g. `PINKDB_KEY`).
fn run_with_env(cmds: &str, env: &[(&str, &str)]) -> String {
    let mut command = Command::new(cli_binary());
    for (key, value) in env {
        command.env(key, value);
    }
    let (out, status) = common::run_merged(command, cmds);
    common::assert_script_exit(status);
    out
}

/// Like [`run`], but captures stderr too and returns the exit code instead of
/// asserting success (the caller decides what a clean exit means).
fn run_captured(cmds: &str) -> (String, String, i32) {
    let mut child = Command::new(cli_binary())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pinkdb CLI");
    child
        .stdin
        .take()
        .expect("child stdin")
        .write_all(cmds.as_bytes())
        .expect("write to CLI stdin");
    let out = child.wait_with_output().expect("wait for the CLI");
    (
        String::from_utf8(out.stdout).expect("CLI stdout is UTF-8"),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.code().unwrap_or(-1),
    )
}

/// The `version` field of a manifest, dependency-free: the manifests are
/// `serde_json` pretty output with a single `"version": <digits>` line.
fn declared_version(json: &str) -> u64 {
    let idx = json.find("\"version\":").expect("version field");
    json[idx + "\"version\":".len()..]
        .trim_start()
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .expect("version digits")
}

#[test]
fn save_open_query_and_metadata_commands() {
    let dir = scratch("roundtrip");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
         INSERT INTO t VALUES (1, 'x'), (2, NULL);\n\
         CREATE DATABASE analytics;\n\
         CREATE TABLE analytics.u (k BIGINT);\n\
         INSERT INTO analytics.u VALUES (7);\n\
         .lake save {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    assert!(out.contains("2 tables, 3 rows"), "{out}");
    assert!(out.contains("bytes written"), "{out}");

    // The lake layout exists on disk, with version 1 created up front.
    assert!(lake.join("catalog.json").is_file());
    assert!(lake.join("snapshots/1.json").is_file());
    assert!(lake.join("snapshots/2.json").is_file());
    let catalog = std::fs::read_to_string(lake.join("catalog.json")).unwrap();
    assert!(catalog.contains("\"format_version\": 6"), "{catalog}");
    assert!(catalog.contains("\"version\": 2"), "{catalog}");
    assert!(catalog.contains("\"main.t\""), "{catalog}");
    assert!(catalog.contains("\"analytics.u\""), "{catalog}");
    assert!(catalog.contains("\"rows\": 2"), "{catalog}");
    assert!(catalog.contains("\"rows\": 1"), "{catalog}");
    assert!(catalog.contains("\"format\": \"vortex\""), "{catalog}");
    assert!(catalog.contains(".vortex"), "{catalog}");
    let data = std::fs::read_dir(lake.join("data/main.t")).unwrap();
    for entry in data {
        let path = entry.unwrap().path();
        assert_eq!(path.extension().unwrap(), "vortex", "{}", path.display());
    }

    // Info/versions/tables report the metadata without touching the store.
    let out = run(&format!(
        ".lake info {lake}\n.lake versions {lake}\n.lake tables {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("format version:  6"), "{out}");
    assert!(out.contains("current version: 2"), "{out}");
    assert!(out.contains("snapshots:       2 (1, 2)"), "{out}");
    assert!(out.contains("tables (2):"), "{out}");
    assert!(out.contains("main.t (2 rows)"), "{out}");
    assert!(out.contains("analytics.u (1 row)"), "{out}");
    assert!(out.contains("2 (current)"), "{out}");

    // Open the latest version and query it.
    let out = run(&format!(
        ".lake open {lake}\nSELECT a, b FROM t ORDER BY a;\nSELECT * FROM analytics.u;\n.quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains("Opened lake version 2 in") && out.contains("(2 tables) from"),
        "{out}"
    );
    assert!(out.contains("x"), "{out}");
    assert!(out.contains("2 rows"), "{out}");
    assert!(out.contains("7"), "{out}");
    cleanup(&dir);
}

#[test]
fn snapshots_accumulate_and_explicit_versions_load() {
    let dir = scratch("versions");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (1);\n\
         .lake save {lake}\n\
         INSERT INTO t VALUES (2);\n\
         .lake save {lake}\n\
         .lake versions {lake}\n\
         .lake tables {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    assert!(out.contains("Saved lake version 3"), "{out}");
    assert!(out.contains("1\n2\n3 (current)"), "{out}");
    assert!(
        out.contains("main.t (2 rows)"),
        "current tables take the latest snapshot: {out}"
    );

    // Opening version 2 sees only the first row; the latest sees both.
    let out = run(&format!(
        ".lake open {lake} 2\nSELECT a FROM t;\n.quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains("Opened lake version 2 in") && out.contains("(1 table) from"),
        "{out}"
    );
    assert!(out.contains("1 row\n"), "{out}");

    let out = run(&format!(
        ".lake open {lake}\nSELECT a FROM t;\n.quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains("Opened lake version 3 in") && out.contains("(1 table) from"),
        "{out}"
    );
    assert!(out.contains("2 rows"), "{out}");

    // A second save after an open appends to the same lake.
    let out = run(&format!(
        ".lake open {lake} 2\nINSERT INTO t VALUES (99);\n.lake save {lake}\n.lake tables {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 4"), "{out}");
    assert!(out.contains("main.t (2 rows)"), "{out}");
    cleanup(&dir);
}

#[test]
fn empty_lake_and_feature_notes() {
    let dir = scratch("empty");
    let lake = dir.join("lake");
    // A lake created explicitly by saving an empty store still has `main`.
    let out = run(&format!(
        ".lake save {lake}\n.lake tables {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    assert!(out.contains("0 tables, 0 rows"), "{out}");
    assert!(out.contains("(no tables)"), "{out}");

    // Views are session-local: saving notes it, the lake has no view.
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\nCREATE VIEW v AS SELECT a FROM t;\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains("Note: views are session-local and were not saved."),
        "{out}"
    );
    cleanup(&dir);
}

#[test]
fn lake_errors_are_clear() {
    let dir = scratch("errors");
    let lake = dir.join("lake");
    let missing = dir.join("no-such-lake");

    let out = run(&format!(
        ".lake open {missing}\n.lake tables {missing}\n.quit\n",
        missing = missing.display()
    ));
    assert!(out.contains("is not a pink-lake directory"), "{out}");

    // Usage errors.
    let out = run(
        ".lake save\n.lake open\n.lake versions\n.lake snapshots\n.lake tables\n.lake info\n.lake\n\
         .quit\n",
    );
    assert!(out.contains("Usage: .lake save <dir>"), "{out}");
    assert!(out.contains("Usage: .lake open <dir> [version]"), "{out}");
    assert!(out.contains("Usage: .lake versions <dir>"), "{out}");
    assert!(out.contains("Usage: .lake snapshots <dir>"), "{out}");
    assert!(out.contains("Usage: .lake tables <dir>"), "{out}");
    assert!(out.contains("Usage: .lake info <dir>"), "{out}");
    assert!(
        out.contains(
            "Usage: .lake <save|open|attach|commit|partition|inlining|flush|changes|\
             add_data_files|merge|rewrite|expire|cleanup|orphans|files|table_info|options|\
             settings|detach|attached|versions|snapshots|tables|info> ..."
        ),
        "{out}"
    );

    // Unknown subcommand.
    let out = run(".lake dunno\n.quit\n");
    assert!(out.contains("unknown .lake subcommand \"dunno\""), "{out}");

    // A missing version names the lake and the version.
    run(&format!(
        "CREATE TABLE t (a INTEGER);\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    let out = run(&format!(
        ".lake open {lake} 99\n.lake open {lake} notanumber\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("has no snapshot version 99"), "{out}");
    assert!(out.contains("Usage: .lake open <dir> [version]"), "{out}");
    cleanup(&dir);
}

#[test]
fn failed_saves_do_not_leave_a_partial_lake() {
    let dir = scratch("partial");
    let dotted = dir.join("dotted");
    let out = run(&format!(
        "CREATE DATABASE \"a.b\";\nCREATE TABLE \"a.b\".t (x INTEGER);\n.lake save {d}\n.quit\n",
        d = dotted.display()
    ));
    assert!(out.contains("contains \".\""), "{out}");
    assert!(
        !dotted.join("catalog.json").exists(),
        "a rejected save must not leave a lake: {out}"
    );

    // A table whose encoded `data/` directory blows the filesystem name
    // limit: refused up front (the old behavior created an empty v1 lake and
    // then died with ENAMETOOLONG).
    let long = dir.join("long");
    let name = "é".repeat(100);
    let out = run(&format!(
        "CREATE TABLE \"{name}\" (x INTEGER);\n.lake save {d}\n.quit\n",
        d = long.display()
    ));
    assert!(out.contains("over the 200-byte limit"), "{out}");
    assert!(
        !long.join("catalog.json").exists(),
        "a rejected save must not leave a lake: {out}"
    );
    cleanup(&dir);
}

#[test]
fn open_notes_the_session_reset_and_carries_the_key() {
    let dir = scratch("open_notes");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1);\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));

    // `.lake open` replaces the store: the session database resets to main
    // and the output says so.
    let out = run(&format!(
        "CREATE DATABASE analytics;\nUSE analytics;\n.lake open {lake}\n.databases\n.quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains("Note: the session database was reset to \"main\"."),
        "{out}"
    );
    assert!(out.contains("main (current)"), "{out}");

    // With encryption, the session key governs the lake too: a keyed save
    // writes PNKLAKE1 containers, a keyed open reads them, and the key
    // survives the store replacement for a later `.save`.
    #[cfg(feature = "encryption")]
    {
        let enc_lake = dir.join("enc_lake");
        let key = [("PINKDB_KEY", "lake-review-key")];
        let out = run_with_env(
            &format!(
                "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (7);\n.lake save {lake}\n.quit\n",
                lake = enc_lake.display()
            ),
            &key,
        );
        assert!(out.contains("Saved lake version 2"), "{out}");
        assert!(
            out.contains("the lake is encrypted at rest with PINKDB_KEY"),
            "{out}"
        );
        let catalog = std::fs::read(enc_lake.join("catalog.json")).unwrap();
        assert_eq!(
            &catalog[..8],
            b"PNKLAKE1",
            "a keyed .lake save must write encrypted containers"
        );
        let out = run_with_env(
            &format!(
                ".lake open {lake}\nSELECT a FROM t;\n.save {out}\n.quit\n",
                lake = enc_lake.display(),
                out = dir.join("out.pink").display()
            ),
            &key,
        );
        assert!(
            out.contains("Note: the lake is encrypted at rest with PINKDB_KEY"),
            "{out}"
        );
        assert!(out.contains("7"), "{out}");
        let bytes = std::fs::read(dir.join("out.pink")).unwrap();
        assert_eq!(
            &bytes[..8],
            b"PINKENC1",
            "the .save after .lake open must keep PINKDB_KEY (encrypted container)"
        );
    }
    cleanup(&dir);
}

/// `PINKDB_KEY` in the `.lake` commands: a keyed save/open/versions/tables/
/// info/attach round trip, and every fail-closed path (a missing key, a wrong
/// key, and a plaintext lake a key must not rewrite).
#[cfg(feature = "encryption")]
#[test]
fn keyed_lake_commands_roundtrip_and_fail_closed() {
    /// Run the REPL with `PINKDB_KEY` definitely unset (the parent
    /// environment is not trusted to be clean).
    fn run_no_key(cmds: &str) -> String {
        let mut command = Command::new(cli_binary());
        command.env_remove("PINKDB_KEY");
        let (out, status) = common::run_merged(command, cmds);
        common::assert_script_exit(status);
        out
    }

    let dir = scratch("keyed_lake");
    let lake = dir.join("lake");
    let key = [("PINKDB_KEY", "cli-lake-secret")];
    let wrong = "cli-lake-wrong";

    let out = run_with_env(
        &format!(
            "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
             INSERT INTO t VALUES (1, 'x'), (2, NULL);\n\
             .lake save {lake}\n.quit\n",
            lake = lake.display()
        ),
        &key,
    );
    assert!(out.contains("Saved lake version 2"), "{out}");
    assert!(out.contains("the lake is encrypted at rest"), "{out}");

    // Metadata commands unseal the catalog (a missing key would fail here).
    let out = run_with_env(
        &format!(
            ".lake versions {lake}\n.lake tables {lake}\n.lake info {lake}\n.quit\n",
            lake = lake.display()
        ),
        &key,
    );
    assert!(out.contains("2 (current)"), "{out}");
    assert!(out.contains("main.t (2 rows)"), "{out}");
    assert!(out.contains("format version:  6"), "{out}");

    // A fresh process loads the encrypted version and answers queries.
    let out = run_with_env(
        &format!(
            ".lake open {lake}\nSELECT b, a FROM t ORDER BY a;\n.quit\n",
            lake = lake.display()
        ),
        &key,
    );
    assert!(out.contains("Opened lake version 2"), "{out}");
    assert!(out.contains("the lake is encrypted at rest"), "{out}");
    assert!(out.contains("NULL"), "{out}");

    // Attach unseals the footer/statistics and answers counts.
    let out = run_with_env(
        &format!(
            ".lake attach {lake}\nSELECT COUNT(*) FROM t;\n.quit\n",
            lake = lake.display()
        ),
        &key,
    );
    assert!(out.contains("Attached"), "{out}");
    assert!(out.contains("the lake is encrypted"), "{out}");
    assert!(out.contains("2"), "{out}");

    // The write-side maintenance subcommands open through `open_lake` too:
    // a keyed process must be able to change an encrypted lake's partition
    // spec (before the fix this used the keyless constructor and refused).
    let out = run_with_env(
        &format!(
            ".lake partition {lake} t by (a)\n.quit\n",
            lake = lake.display()
        ),
        &key,
    );
    assert!(
        out.contains("Partitioned main.t by (a)"),
        "a keyed .lake partition must unseal the lake: {out}"
    );
    // The other maintenance subcommands use the same keyed open.
    let out = run_with_env(
        &format!(
            ".lake inlining {lake} main.t 0\n\
             .lake inlining {lake} main.t default\n\
             .lake flush {lake}\n\
             .lake add_data_files {lake} main.t data/main.t/does-not-exist.vortex\n\
             .quit\n",
            lake = lake.display()
        ),
        &key,
    );
    assert!(
        !out.contains("the lake is encrypted"),
        "inlining/flush/add_data_files must unseal a keyed lake: {out}"
    );
    // ... and without the key the same subcommands still fail closed.
    for sub in [
        ".lake partition {lake} t clear",
        ".lake inlining {lake} main.t 0",
        ".lake flush {lake}",
        ".lake add_data_files {lake} main.t data/main.t/x.vortex",
    ] {
        let command = sub.replace("{lake}", &lake.display().to_string());
        let out = run_no_key(&format!("{command}\n.quit\n"));
        assert!(
            out.contains("the lake is encrypted"),
            "a keyless {sub} must refuse an encrypted lake: {out}"
        );
    }

    // Failures: no key, wrong key, and a keyed save onto a plaintext lake.
    let out = run_no_key(&format!(
        ".lake open {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("the lake is encrypted"), "{out}");

    for sub in [".lake open", ".lake tables"] {
        let mut wrong_cmd = Command::new(cli_binary());
        wrong_cmd.env("PINKDB_KEY", wrong);
        let (out, status) = common::run_merged(
            wrong_cmd,
            &format!("{sub} {lake}\n.quit\n", lake = lake.display()),
        );
        assert_ne!(
            status.code(),
            Some(0),
            "{sub} with a wrong key must fail: {out}"
        );
        assert!(out.contains("wrong key"), "{sub}: {out}");
    }

    let plain = dir.join("plain");
    let out = run_no_key(&format!(
        "CREATE TABLE u (x INTEGER);\n.lake save {plain}\n.quit\n",
        plain = plain.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    let mut keyed_save = Command::new(cli_binary());
    keyed_save.env("PINKDB_KEY", key[0].1);
    let (out, status) = common::run_merged(
        keyed_save,
        &format!(".lake save {plain}\n.quit\n", plain = plain.display()),
    );
    assert_ne!(status.code(), Some(0));
    assert!(
        out.contains("not an encrypted pink-lake container"),
        "a keyed save must refuse a plaintext lake: {out}"
    );
    // ... and the plaintext lake is untouched.
    let out = run_no_key(&format!(
        ".lake info {plain}\n.quit\n",
        plain = plain.display()
    ));
    assert!(out.contains("current version: 2"), "{out}");
    cleanup(&dir);
}

#[test]
fn tampered_version_catalogs_are_clean_cli_errors() {
    let dir = scratch("bad_versions");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER);\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    let catalog = std::fs::read_to_string(lake.join("catalog.json")).unwrap();
    assert!(catalog.contains("\"version\": 2"), "{catalog}");

    for bad in ["0", "18446744073709551615"] {
        let tampered = catalog.replacen("\"version\": 2", &format!("\"version\": {bad}"), 1);
        std::fs::write(lake.join("catalog.json"), &tampered).unwrap();
        let out = run(&format!(
            ".lake info {lake}\n.lake open {lake}\n.lake save {lake}\n.lake versions {lake}\n.quit\n",
            lake = lake.display()
        ));
        assert!(
            out.contains("corrupt lake metadata") && out.contains("outside the supported range"),
            "version {bad}: {out}"
        );
        assert!(!out.contains("panicked"), "version {bad} panicked: {out}");
        assert!(
            !out.contains("Saved lake version"),
            "version {bad} wrapped: {out}"
        );
    }

    // Restoring the catalog heals the lake (nothing was written).
    std::fs::write(lake.join("catalog.json"), &catalog).unwrap();
    let out = run(&format!(
        ".lake info {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("current version: 2"), "{out}");
    cleanup(&dir);
}

#[test]
fn missing_data_files_are_warned_about() {
    let dir = scratch("warn_missing");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1);\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));

    let mut files: Vec<PathBuf> = std::fs::read_dir(lake.join("data/main.t"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert_eq!(files.len(), 1);
    std::fs::remove_file(files.pop().unwrap()).unwrap();

    let out = run(&format!(
        ".lake tables {lake}\n.lake info {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains("warning: table \"main.t\" references missing data file"),
        "{out}"
    );
    assert_eq!(
        out.matches("warning: table").count(),
        2,
        "both .lake tables and .lake info warn: {out}"
    );

    // The metadata still reads; a load fails clearly.
    let out = run(&format!(
        ".lake open {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Error:"), "{out}");
    cleanup(&dir);
}

#[test]
fn concurrent_lake_saves_stay_consistent() {
    let dir = scratch("race");
    for round in 0..6 {
        let lake = dir.join(format!("lake{round}"));
        // Warm-up publishes v2, so all writers race on the same chain.
        run(&format!(
            "CREATE TABLE warm (x INTEGER);\n.lake save {lake}\n.quit\n",
            lake = lake.display()
        ));

        let outs: Vec<String> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..4)
                .map(|i| {
                    let lake = lake.clone();
                    scope.spawn(move || {
                        run(&format!(
                            "CREATE TABLE t{i} (a INTEGER);\nINSERT INTO t{i} VALUES (1), (2), (3);\n\
                             .lake save {lake}\n.quit\n",
                            lake = lake.display()
                        ))
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("writer thread"))
                .collect()
        });

        // Every writer either published or lost with the documented conflict.
        let mut winners: Vec<u64> = Vec::new();
        for out in &outs {
            assert!(!out.contains("No such file or directory"), "{out}");
            assert!(!out.contains("panicked"), "{out}");
            for line in out.lines() {
                if let Some(rest) = line.strip_prefix("Saved lake version ") {
                    winners.push(rest.split_whitespace().next().unwrap().parse().unwrap());
                } else if line.starts_with("Error:") {
                    assert!(line.contains("write conflict"), "unexpected error: {line}");
                }
            }
        }
        let mut unique = winners.clone();
        unique.dedup();
        assert_eq!(
            winners, unique,
            "round {round}: duplicate published versions"
        );

        // The catalog parses (`.lake info` uses the library reader) and is
        // byte-identical to the snapshot of its version; the version chain
        // has no gaps.
        let catalog_bytes = std::fs::read(lake.join("catalog.json")).unwrap();
        let version = declared_version(&String::from_utf8(catalog_bytes.clone()).unwrap());
        assert_eq!(
            catalog_bytes,
            std::fs::read(lake.join(format!("snapshots/{version}.json"))).unwrap(),
            "round {round}: catalog.json != snapshots/{version}.json"
        );
        assert_eq!(
            version,
            2 + winners.len() as u64,
            "round {round}: version chain has a gap"
        );
        // The winner *set* is the invariant; publish order is a race artifact
        // (a slow stdout flush can report v4 before v3), so compare sorted.
        let mut sorted = winners.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (3..=version).collect::<Vec<_>>(), "round {round}");

        let out = run(&format!(
            ".lake info {lake}\n.quit\n",
            lake = lake.display()
        ));
        assert!(!out.contains("Error:"), "{out}");
        assert!(
            out.contains(&format!("current version: {version}")),
            "{out}"
        );

        // Snapshot files are exactly the published chain, canonically named.
        let mut names: Vec<String> = std::fs::read_dir(lake.join("snapshots"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_str().unwrap().to_string())
            .collect();
        names.sort();
        let expected: Vec<String> = (1..=version).map(|v| format!("{v}.json")).collect();
        assert_eq!(names, expected, "round {round}");
    }
    cleanup(&dir);
}

/// A tampered lake data file must produce a clean CLI error and leave the
/// REPL (and the process) alive: `.lake open` runs the guarded Vortex decode,
/// so even a flip that panics the decoder is caught as a corrupt-file error.
#[test]
fn tampered_data_files_are_clean_cli_errors_and_the_repl_stays_alive() {
    let dir = scratch("tamper_data");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (b BOOLEAN, i INTEGER, g BIGINT, d DOUBLE, v VARCHAR);\n\
         INSERT INTO t VALUES (true, 1, 2, 3.5, 'x'), (false, -2, -3, 0.25, 'héllo'),\n\
         (NULL, NULL, NULL, NULL, NULL);\n\
         .lake save {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    let file = std::fs::read_dir(lake.join("data/main.t"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .next()
        .expect("one data file");
    let original = std::fs::read(&file).unwrap();
    assert!(original.len() > 512, "fixture file unexpectedly small");

    let mut tampers: Vec<(&str, Vec<u8>)> = Vec::new();
    // Single-byte flips at a few offsets, including the kind of layout
    // metadata position the review found panicking the decoder.
    for pos in [74usize, original.len() / 3, original.len() / 2] {
        let mut b = original.clone();
        b[pos] ^= 0x80;
        tampers.push(("byte flip", b));
    }
    // A zeroed window in the middle of the encoded data.
    {
        let mut b = original.clone();
        let start = original.len() / 4;
        for x in &mut b[start..start + 64] {
            *x = 0;
        }
        tampers.push(("zeroed window", b));
    }
    // Broken `pinkdb.table` JSON (the identity/schema segment).
    {
        let needle = b"{\"db\":";
        let pos = original
            .windows(needle.len())
            .position(|w| w == needle)
            .expect("pinkdb.table metadata JSON");
        let mut b = original.clone();
        b[pos + 1] = b'x';
        tampers.push(("metadata JSON", b));
    }

    let mut saw_error = false;
    for (what, bytes) in &tampers {
        std::fs::write(&file, bytes).unwrap();
        let (stdout, stderr, code) = run_captured(&format!(
            ".lake open {lake}\n.databases\n.quit\n",
            lake = lake.display()
        ));
        // A failed `.lake open` is a script error (exit 1), never a crash.
        assert!(
            code == 0 || code == 1,
            "{what}: the CLI must exit cleanly, got {code} (stderr: {stderr})"
        );
        assert!(
            !stdout.contains("panicked"),
            "{what}: a panic reached the REPL: {stdout}"
        );
        assert!(
            stdout.contains("main (current)"),
            "{what}: the REPL died after the failed open: {stdout}"
        );
        if stderr.contains("Error:") {
            saw_error = true;
            assert_eq!(code, 1, "{what}: a failed open must fail the script");
            assert!(
                stderr.contains("corrupt lake metadata")
                    || stderr.contains("lake vortex error")
                    || stderr.contains("not a valid Vortex file"),
                "{what}: error is not a clean corrupt-file error: {stderr}"
            );
        }
    }
    assert!(
        saw_error,
        "at least one tamper must fail `.lake open` with a clean error"
    );

    // The same tampers against `.lake attach` + a projected fallback query:
    // attach may fail at the footer (clean) or succeed and fail at the
    // decode (clean); a panic or a dead REPL is never acceptable.
    for (what, bytes) in &tampers {
        std::fs::write(&file, bytes).unwrap();
        let (stdout, stderr, code) = run_captured(&format!(
            ".lake attach {lake}\nSELECT SUM(i) FROM t;\nSELECT * FROM t;\n.databases\n.quit\n",
            lake = lake.display()
        ));
        assert!(
            code == 0 || code == 1,
            "{what}: the attach CLI must exit cleanly, got {code} (stderr: {stderr})"
        );
        assert!(
            !stdout.contains("panicked") && !stderr.contains("panicked"),
            "{what}: a panic escaped under attach:\n{stdout}\n{stderr}"
        );
        assert!(
            stdout.contains("main (current)"),
            "{what}: the REPL died after an attach-path failure: {stdout}"
        );
        if code == 1 {
            assert!(
                stderr.contains("corrupt lake metadata")
                    || stderr.contains("lake vortex error")
                    || stderr.contains("not a valid Vortex file")
                    || stderr.contains("decoding a Vortex chunk"),
                "{what}: attach-path error is not a clean corrupt-file error: {stderr}"
            );
        }
    }

    // Restoring the file heals the lake.
    std::fs::write(&file, &original).unwrap();
    let (stdout, stderr, code) = run_captured(&format!(
        ".lake open {lake}\nSELECT count(*) FROM t;\n.quit\n",
        lake = lake.display()
    ));
    assert_eq!(
        code, 0,
        "restored open exited with {code} (stderr: {stderr})"
    );
    assert!(stdout.contains("Opened lake version 2"), "{stdout}");
    cleanup(&dir);
}

/// Without the `s3` feature, an `s3://` location is a clear error pointing at
/// the rebuild command — never a silent local directory named `s3:`.
#[cfg(not(feature = "s3"))]
#[test]
fn s3_urls_need_the_s3_feature() {
    let out = run(".lake save s3://bucket/prefix\n\
         .lake open s3://bucket/prefix\n\
         .lake versions s3://bucket/prefix\n\
         .lake tables s3://bucket/prefix\n\
         .lake info s3://bucket/prefix\n\
         .quit\n");
    assert!(out.contains("does not include the `s3` feature"), "{out}");
    assert!(
        out.contains("cargo build -p pinkdb-cli --features vortexlake,s3"),
        "{out}"
    );
    assert_eq!(
        out.matches("Error:").count(),
        5,
        "every .lake subcommand reports the feature error: {out}"
    );
    // The URL is named so the typo is obvious.
    assert!(out.contains("s3://bucket/prefix"), "{out}");
    // A malformed s3 URL is a location error, not a feature error.
    let out = run(".lake info s3://\n.quit\n");
    assert!(out.contains("Error:"), "{out}");
    assert!(out.contains("invalid lake location"), "{out}");
}

/// Two REPL sessions open the same lake version; the first to save wins and
/// the second gets a conflict instead of silently overwriting that work.
/// `force` saves on purpose.
#[test]
fn second_session_save_conflicts_instead_of_losing_an_update() {
    use std::io::{BufRead, BufReader};

    let dir = scratch("lost_update");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1);\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version"), "{out}");

    // Session A: open the lake and pause.
    let (a_reader, a_writer) = std::io::pipe().unwrap();
    let mut a = Command::new(cli_binary())
        .stdin(Stdio::piped())
        .stdout(a_writer.try_clone().unwrap())
        .stderr(a_writer)
        .spawn()
        .expect("spawn session A");
    let mut a_in = a.stdin.take().unwrap();
    let mut a_out = BufReader::new(a_reader);
    writeln!(a_in, ".lake open {}", lake.display()).unwrap();
    a_in.flush().unwrap();
    let mut seen = String::new();
    loop {
        let mut line = String::new();
        assert!(
            a_out.read_line(&mut line).unwrap() > 0,
            "session A exited early: {seen}"
        );
        seen.push_str(&line);
        if line.starts_with("Opened lake version") || line.starts_with("Error:") {
            break;
        }
    }
    assert!(seen.contains("Opened lake version"), "{seen}");

    // Session B: open, change, save (publishes the next version).
    let out = run(&format!(
        ".lake open {lake}\nINSERT INTO t VALUES (2);\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version"), "{out}");

    // Session A saves its own change: refused, then forced.
    writeln!(
        a_in,
        "INSERT INTO t VALUES (3);\n.lake save {lake}\n.lake save {lake} force\n.quit",
        lake = lake.display()
    )
    .unwrap();
    drop(a_in);
    let mut rest = String::new();
    let status = a.wait().unwrap();
    std::io::Read::read_to_string(&mut a_out, &mut rest).unwrap();
    // The refused save makes the script fail (exit 1), after the forced
    // save still ran.
    assert_eq!(status.code(), Some(1), "{rest}");
    assert!(rest.contains("Error: write conflict"), "{rest}");
    assert!(
        rest.contains("Saved lake version"),
        "forced save publishes: {rest}"
    );

    // After a successful save the session's base moved: a plain save works.
    let out = run(&format!(
        ".lake open {lake}\n.lake save {lake}\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(!out.contains("Error:"), "{out}");
    cleanup(&dir);
}

/// `.lake save`/`.lake open` refuse inside a transaction, and `.lake open`
/// refuses to detach a durable database.
#[test]
fn lake_commands_respect_transactions_and_durable_sessions() {
    let dir = scratch("guards");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\nBEGIN;\nINSERT INTO t VALUES (1);\n\
         .lake save {lake}\n.lake open {lake}\nROLLBACK;\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert_eq!(
        out.matches("not allowed inside a transaction").count(),
        2,
        "{out}"
    );
    assert!(out.contains("Saved lake version"), "{out}");
    let out = run(&format!(
        ".open {db}\n.lake open {lake}\n.quit\n",
        db = dir.join("x.pink").display(),
        lake = lake.display()
    ));
    assert!(out.contains("would detach this session"), "{out}");
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// `.lake attach`: footer-only, lazy, read-only.

/// The core equivalence gate: the same data, the same queries, three arms —
/// in-memory store, `.lake open`, `.lake attach` — must produce identical
/// transcripts (values included).
#[test]
fn attach_answers_equal_open_and_in_memory() {
    let dir = scratch("attach_equiv");
    let lake = dir.join("lake");
    let corpus = common::attach_corpus(&dir);
    let queries = common::attach_corpus_queries();

    // Create the lake from the corpus.
    let out = run(&format!(
        "{corpus}.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");

    // Arm 1: the in-memory store.
    let in_memory = run(&format!("{corpus}{queries}.quit\n"));
    // Arm 2: `.lake open` (the eager loader).
    let opened = run(&format!(
        ".lake open {lake}\nUSE lake;\n{queries}.quit\n",
        lake = lake.display()
    ));
    // Arm 3: `.lake attach` (footer-only + lazy materialization).
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
        "`.lake attach` diverged from the in-memory answers"
    );
    // The fast paths really ran: the attach arm printed the footer-only line
    // and the eager loader was not used.
    assert!(attached.contains("footer-only"), "{attached}");
    cleanup(&dir);
}

/// `.lake attached` reports data-file decodes end to end: statistics-only
/// queries decode nothing, projection pushdown decodes exactly the
/// referenced columns, and columns already decoded stay cached.
#[test]
fn attach_decode_stats_show_projection_pushdown() {
    let dir = scratch("attach_stats");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER, b VARCHAR, c DOUBLE);\n\
         INSERT INTO t VALUES (1, 'x', 1.5), (2, NULL, -2.5), (3, 'zz', 0.5);\n\
         .lake save {lake}\n.quit\n",
        lake = lake.display()
    ));

    let out = run(&format!(
        ".lake attach {lake}\n\
         .lake attached\n\
         SELECT COUNT(*), COUNT(b), MIN(a), MAX(a), MIN(c), MAX(c) FROM t;\n\
         .lake attached\n\
         SELECT b FROM t ORDER BY b;\n\
         .lake attached\n\
         SELECT c FROM t ORDER BY c;\n\
         .lake attached\n\
         SELECT a FROM t ORDER BY a;\n\
         .lake attached\n\
         SELECT * FROM t;\n\
         .lake attached\n\
         .quit\n",
        lake = lake.display()
    ));
    let stats: Vec<&str> = out
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("decodes:"))
        .collect();
    assert_eq!(
        stats,
        vec![
            "decodes: 0 loads, 0 columns, 0 files", // attach reads footers only
            "decodes: 0 loads, 0 columns, 0 files", // every aggregate came from statistics
            "decodes: 1 load, 1 column, 1 file",    // ORDER BY b: b only
            "decodes: 2 loads, 2 columns, 2 files", // + c (the file decodes again)
            "decodes: 3 loads, 3 columns, 3 files", // + a
            "decodes: 3 loads, 3 columns, 3 files", // SELECT *: everything was cached
        ],
        "{out}"
    );
    cleanup(&dir);
}

/// `.lake attach` lifecycle: output, listing, `.tables`/`.schema`, detach,
/// and the read-only refusals.
#[test]
fn attach_lifecycle_and_read_only_rules() {
    let dir = scratch("attach_life");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
         INSERT INTO t VALUES (1, 'x'), (2, NULL), (3, 'zz');\n\
         CREATE TABLE u (k BIGINT);\n\
         INSERT INTO u VALUES (7);\n\
         .lake save {lake}\n.quit\n",
        lake = lake.display()
    ));

    let out = run(&format!(
        ".lake attach {lake}\n\
         .lake attached\n.tables\n.schema t\n\
         SELECT COUNT(*), COUNT(b), MIN(a), MAX(b) FROM t;\n\
         .lake detach {lake}\n\
         SELECT COUNT(*) FROM t;\n\
         .lake attached\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains(&format!("Attached {} version 2 in", lake.display()))
            && out.contains("(2 tables, footer-only)"),
        "{out}"
    );
    assert!(out.contains("main.t (3 rows)"), "{out}");
    assert!(out.contains("main.u (1 row)"), "{out}");
    assert!(out.contains("t (3 rows, attached)"), "{out}");
    assert!(out.contains("-- 3 rows, attached (read-only)"), "{out}");
    assert!(out.contains("│ 3 "), "{out}");
    assert!(out.contains("│ 2 "), "{out}");
    assert!(out.contains("Detached"), "{out}");
    assert!(out.contains("table \"t\" not found"), "{out}");
    assert!(out.contains("(no attached lakes)"), "{out}");

    // Mutations against an attached table are refused with the read-only
    // message, and CREATE TABLE over the name is refused too.
    let out = run(&format!(
        ".lake attach {lake}\n\
         INSERT INTO t VALUES (4, 'q');\n\
         UPDATE t SET a = 9;\n\
         DELETE FROM t;\n\
         DROP TABLE t;\n\
         CREATE TABLE t (a INTEGER, b VARCHAR);\n\
         .quit\n",
        lake = lake.display()
    ));
    assert_eq!(
        out.matches("attached lake tables are read-only").count(),
        5,
        "{out}"
    );
    assert!(out.contains("use .lake open"), "{out}");

    // Attaching inside a transaction is refused (a ROLLBACK cannot undo it),
    // and so is a second attach of the same lake.
    let out = run(&format!(
        "BEGIN;\n.lake attach {lake}\nROLLBACK;\n\
         .lake attach {lake}\n.lake attach {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("not allowed inside a transaction"), "{out}");
    assert!(out.contains("already attached"), "{out}");

    // A collision with an in-memory table is refused, atomically.
    let out = run(&format!(
        "CREATE TABLE t (x INTEGER);\n\
         INSERT INTO t VALUES (42);\n\
         .lake attach {lake}\n\
         SELECT * FROM t;\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("already exists in the session store"), "{out}");
    assert!(
        out.contains("42"),
        "the store's table still resolves: {out}"
    );

    // `.lake save` while attached: attached tables are not saved (the note
    // says so) and the new lake has no tables at all.
    let other = dir.join("other");
    let out = run(&format!(
        ".lake attach {lake}\n\
         .lake save {other}\n\
         .lake tables {other}\n\
         .quit\n",
        lake = lake.display(),
        other = other.display()
    ));
    assert!(
        out.contains("attached lake tables are session-local and were not saved"),
        "{out}"
    );
    assert!(out.contains("(no tables)"), "{out}");
    cleanup(&dir);
}

/// Attach failures are clean errors: not a lake, a v1 lake, a missing data
/// file, a truncated data file, a row-count mismatch, and a multi-file
/// table.
#[test]
fn attach_errors_are_clear_and_never_panic() {
    let dir = scratch("attach_err");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1);\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));

    // Not a lake at all.
    let out = run(&format!(
        ".lake attach {missing}\n.quit\n",
        missing = dir.join("no-such-lake").display()
    ));
    assert!(out.contains("is not a pink-lake directory"), "{out}");

    // The v1 fixture: no statistics, refused with a clear message.
    let v1 = Path::new(env!("CARGO_MANIFEST_DIR")).join("../pink-lake/tests/fixtures/lake_v1");
    let (out, err, code) = run_captured(&format!(".lake attach {}\n.quit\n", v1.display()));
    assert_eq!(code, 1);
    assert!(
        err.contains("without a statistics block") && err.contains("use .lake open"),
        "{out}\n{err}"
    );

    // A missing data file.
    let missing_lake = dir.join("missing_lake");
    copy_dir(&lake, &missing_lake);
    let data = std::fs::read_dir(missing_lake.join("data/main.t"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_file(&data).unwrap();
    let (out, err, code) =
        run_captured(&format!(".lake attach {}\n.quit\n", missing_lake.display()));
    assert_eq!(code, 1);
    assert!(
        err.contains("is missing") || err.contains("loading data file"),
        "{out}\n{err}"
    );

    // A truncated data file: the footer cannot be read; a clean error, never
    // a crash.
    let truncated = dir.join("truncated_lake");
    copy_dir(&lake, &truncated);
    let data = std::fs::read_dir(truncated.join("data/main.t"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let bytes = std::fs::read(&data).unwrap();
    std::fs::write(&data, &bytes[..bytes.len() / 4]).unwrap();
    let (out, err, code) = run_captured(&format!(".lake attach {}\n.quit\n", truncated.display()));
    assert_eq!(code, 1);
    assert!(err.contains("Error:"), "{out}\n{err}");

    // A manifest whose row count disagrees with the file: corrupt.
    let wrong_rows = dir.join("wrong_rows_lake");
    copy_dir(&lake, &wrong_rows);
    for name in ["catalog.json", "snapshots/2.json"] {
        let path = wrong_rows.join(name);
        let text = std::fs::read_to_string(&path).unwrap();
        let patched = text.replace("\"rows\": 1", "\"rows\": 2");
        assert_ne!(text, patched, "rows field not found in {name}");
        std::fs::write(&path, patched).unwrap();
    }
    let (out, err, code) = run_captured(&format!(".lake attach {}\n.quit\n", wrong_rows.display()));
    assert_eq!(code, 1);
    assert!(
        err.contains("has 1 rows, but the manifest says 2"),
        "{out}\n{err}"
    );

    // A table whose file list does not carry row counts (a hand-edited v4
    // manifest) is refused at parse.
    let no_rows = dir.join("no_row_counts_lake");
    copy_dir(&lake, &no_rows);
    for name in ["catalog.json", "snapshots/2.json"] {
        let path = no_rows.join(name);
        let text = std::fs::read_to_string(&path).unwrap();
        // Remove the *file object's* `"rows"` (the first occurrence, inside
        // the files array); the entry-level one must stay.
        let needle = "\"rows\": 1,\n          \"schema_version\": 1";
        let patched = text.replacen(needle, "\"schema_version\": 1", 1);
        assert_ne!(text, patched, "file rows field not found in {name}");
        std::fs::write(&path, patched).unwrap();
    }
    let (out, err, code) = run_captured(&format!(".lake attach {}\n.quit\n", no_rows.display()));
    assert_eq!(code, 1);
    assert!(err.contains("without its row count"), "{out}\n{err}");

    // A table with two data files (a v4 incremental append) attaches and
    // unions them in manifest order: the descriptors merge the files'
    // statistics. Built through the real write path, not a hand-edit: a
    // hand-edit would have to fix the catalog zone map's CRC.
    let two_files = dir.join("two_file_lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER, s VARCHAR);\n\
         INSERT INTO t VALUES (1, 'x');\n\
         .lake save {two_files}\n\
         .lake attach {two_files} write\n\
         INSERT INTO t VALUES (2, 'y');\n\
         .lake commit {two_files}\n\
         .quit\n",
        two_files = two_files.display()
    ));
    let out = run(&format!(
        ".lake attach {}\nSELECT COUNT(*) FROM t;\nSELECT MIN(a), MAX(s) FROM t;\n         .lake attached\n.quit\n",
        two_files.display()
    ));
    assert!(out.contains("Attached"), "{out}");
    assert!(out.contains("│ 2 "), "both files' rows: {out}");
    assert!(out.contains("2 row"), "{out}");
    // The merged statistics answer COUNT/MIN/MAX over both files without
    // decoding either.
    assert!(
        out.contains("decodes: 0 loads, 0 columns, 0 files"),
        "{out}"
    );
    cleanup(&dir);
}

/// Attach and its queries never write to the lake: `catalog.json` stays
/// byte-identical to the snapshot of its version, and every data file is
/// untouched.
#[test]
fn attach_never_touches_the_lake() {
    let dir = scratch("attach_ro");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER, s VARCHAR);\n\
         INSERT INTO t VALUES (1, 'x'), (2, 'y');\n\
         .lake save {lake}\n.quit\n",
        lake = lake.display()
    ));

    let catalog = std::fs::read(lake.join("catalog.json")).unwrap();
    let snapshot = std::fs::read(lake.join("snapshots/2.json")).unwrap();
    assert_eq!(catalog, snapshot, "the invariant holds before attach");
    let data_dir = lake.join("data/main.t");
    let data_files: Vec<PathBuf> = std::fs::read_dir(&data_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    let before: Vec<Vec<u8>> = data_files
        .iter()
        .map(|p| std::fs::read(p).unwrap())
        .collect();

    // Attach, run both fast and fully materializing queries, then detach.
    let out = run(&format!(
        ".lake attach {lake}\n\
         SELECT COUNT(*), MIN(a), MAX(s) FROM t;\n\
         SELECT SUM(a) FROM t;\n\
         SELECT * FROM t;\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(!out.contains("Error:"), "{out}");

    assert_eq!(
        std::fs::read(lake.join("catalog.json")).unwrap(),
        catalog,
        "attach must not rewrite the catalog"
    );
    assert_eq!(
        std::fs::read(lake.join("snapshots/2.json")).unwrap(),
        snapshot
    );
    for (path, bytes) in data_files.iter().zip(&before) {
        assert_eq!(&std::fs::read(path).unwrap(), bytes, "{}", path.display());
    }
    // No stray files appeared in the lake.
    let mut entries: Vec<String> = std::fs::read_dir(&lake)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    entries.sort();
    assert_eq!(
        entries,
        vec![
            "catalog.json",
            "commits",
            "data",
            "highest.json",
            "lake.json",
            "snapshots"
        ],
        "{out}"
    );
    cleanup(&dir);
}

/// The checked-in version 2 fixture lake attaches (its files were written by
/// the older writer), and the answers match `.lake open`.
#[test]
fn attach_of_the_v2_fixture_matches_open() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../pink-lake/tests/fixtures/lake_v2");
    let queries = "SELECT '===BEGIN===' AS marker;\n\
                   SELECT COUNT(*) FROM fid;\n\
                   SELECT COUNT(v), MIN(i), MAX(i), MIN(g), MAX(g) FROM fid;\n\
                   SELECT MIN(b), MAX(b), MIN(dt), MAX(dt), MIN(ts), MAX(ts) FROM fid;\n\
                   SELECT MIN(d), MAX(d) FROM fid;\n\
                   SELECT s FROM big ORDER BY a LIMIT 3;\n\
                   SELECT a FROM big WHERE s = 'region1' ORDER BY a LIMIT 2;\n\
                   SELECT a, s FROM empty;\n\
                   SELECT y FROM empty ORDER BY x;\n\
                   SELECT '===END===' AS marker;\n";
    let opened = run(&format!(
        ".lake open {}\nUSE main;\n{queries}.quit\n",
        fixture.display()
    ));
    let attached = run(&format!(
        ".lake attach {}\nUSE main;\n{queries}.quit\n",
        fixture.display()
    ));
    assert!(
        attached.contains("footer-only"),
        "attach output: {attached}"
    );
    let base = common::tail_after_marker(&opened);
    assert_eq!(common::tail_after_marker(&attached), base);
    // The fixture's DOUBLE column holds NaN payloads, so its MIN/MAX take
    // the materializing path on both arms. Under DuckDB's NaN order (NaN
    // sorts above +inf), MAX over a column holding NaN is NaN.
    assert!(base.contains("-0.0") && base.contains("NaN"), "{base}");
}

/// `.lake open` after an attach still replaces the store and drops the
/// attachment (same output as always).
#[test]
fn open_after_attach_resets_the_session() {
    let dir = scratch("attach_then_open");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1);\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    let out = run(&format!(
        ".lake attach {lake}\n\
         SELECT COUNT(*) FROM t;\n\
         .lake open {lake}\n\
         SELECT COUNT(*) FROM t;\n\
         .lake attached\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Opened lake version 2 in"), "{out}");
    assert!(out.contains("(no attached lakes)"), "{out}");
    assert!(!out.contains("Error:"), "{out}");
    cleanup(&dir);
}

/// Every file under `dir`, recursively, sorted.
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

/// Copy one directory tree (a lake is a directory of JSON + data files).
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// `.lake attach <dir> [version]` pins the requested version, like
/// `.lake open`.
#[test]
fn attach_pins_the_requested_version() {
    let dir = scratch("attach_pin");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (1);\n\
         .lake save {lake}\n\
         INSERT INTO t VALUES (2);\n\
         .lake save {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    let out = run(&format!(
        ".lake attach {lake} 2\nSELECT COUNT(*) FROM t;\n\
         .lake detach {lake}\n\
         .lake attach {lake}\nSELECT COUNT(*) FROM t;\n.quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains(&format!("Attached {} version 2 in", lake.display())),
        "{out}"
    );
    assert!(
        out.contains("│ 1 "),
        "the version-2 attachment sees one row: {out}"
    );
    assert!(
        out.contains("│ 2 "),
        "the latest attachment sees two rows: {out}"
    );
    cleanup(&dir);
}

/// The lean writer's v1 lake (`crates/pink-lake/tests/fixtures/lake_v1_v4/`:
/// engine v4 `.pink` data files under a v1 catalog) opens in the full build,
/// answers the same as a fresh store, and migrates to v3 on the next save —
/// the full-reads / lean-writes compatibility pin, end to end through the
/// CLI. The lean side reads the same directory in
/// `crates/pink-lake/tests/pinklake.rs`.
#[test]
fn lean_written_v1_v4_fixture_opens_queries_and_migrates() {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../pink-lake/tests/fixtures/lake_v1_v4");
    let dir = scratch("v1_v4_fixture");
    let lake = dir.join("lake");
    copy_dir(&fixture, &lake);

    let out = run(&format!(
        ".lake info {lake}\n\
         .lake open {lake}\n\
         SELECT COUNT(*) FROM main.fid;\n\
         SELECT COUNT(*), MIN(i), MAX(g) FROM fid;\n\
         SELECT COUNT(*) FROM big;\n\
         SELECT COUNT(*) FROM big WHERE s = 'region1';\n\
         SELECT z FROM analytics.\"a.b\" ORDER BY z;\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("format version:  1"), "{out}"); // the v1 fixture stays v1
    assert!(out.contains("Opened lake version 2"), "{out}");
    assert!(out.contains("│ 6 "), "fid has 6 rows: {out}");
    assert!(out.contains("│ 2148 "), "big has 2148 rows: {out}");
    assert!(out.contains("│ 716 "), "region1 has 716 rows: {out}");
    assert!(out.contains("-0.0"), "{out}");

    // Saving migrates to v3 (Vortex); the v1 files stay untouched and the
    // migrated version loads the same values.
    let before: Vec<PathBuf> = data_files(&lake.join("data"));
    let out = run(&format!(
        ".lake open {lake}\n\
         .lake save {lake}\n\
         SELECT COUNT(*) FROM main.fid;\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 3"), "{out}");
    let catalog = std::fs::read_to_string(lake.join("catalog.json")).unwrap();
    assert!(catalog.contains("\"format_version\": 6"), "{catalog}");
    assert!(catalog.contains(".vortex"), "{catalog}");
    for path in before {
        assert!(path.is_file(), "v1 file {} must stay", path.display());
    }
    cleanup(&dir);
}

/// Cross-build pin: the lean-written `lake_v1_v4_stats` fixture (v1 `.pink`
/// data files + CRC-protected statistics blocks) attaches in the full
/// (Vortex) build from the manifest alone — `Attached ... statistics-only`,
/// no data file read — and every answer equals `.lake open` over the same
/// fixture (which is the same battery the lean CLI runs in
/// `tests/pinklake.rs`).
#[test]
fn lean_written_v1_stats_fixture_attaches_in_the_full_build() {
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
        "the full-build attach diverged from `.lake open` over the lean fixture"
    );
    // The statistics-only arm decoded nothing for the stats shapes; only
    // `d`'s NaN-gated MIN/MAX falls back (once, cached).
    let probe = run(&format!(
        ".lake attach {}\nUSE main;\n\
         SELECT COUNT(*) FROM fid;\n\
         SELECT MIN(i), MAX(i), MIN(s), MAX(s) FROM fid;\n\
         .lake attached\n\
         SELECT SUM(i) FROM fid;\n\
         .lake attached\n\
         .quit\n",
        fixture.display()
    ));
    assert!(
        probe.contains("decodes: 0 loads, 0 columns"),
        "statistics fast paths must not decode:\n{probe}"
    );
    assert!(
        probe.contains("decodes: 1 load"),
        "the fallback must materialize:\n{probe}"
    );
}

/// `.lake save author/message` records a commit message on the version it
/// publishes, and `.lake snapshots` lists every version's record — time,
/// author, message, counts, change summary (wave 64).
#[test]
fn save_records_messages_and_snapshots_lists_them() {
    let dir = scratch("snapshot_records");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE DATABASE lake;\n\
         USE lake;\n\
         CREATE TABLE t (x INTEGER);\n\
         INSERT INTO t VALUES (1), (2);\n\
         .lake save {lake} author 'alice' message 'first save'\n\
         INSERT INTO t VALUES (3);\n\
         .lake save {lake} message \"second save\"\n\
         .lake snapshots {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    assert!(out.contains("Saved lake version 3"), "{out}");
    assert!(out.contains("version 3 (current)"), "{out}");
    // v2's record: author, message and the change summary of a first save.
    let v2 = out
        .split("version 2\n")
        .nth(1)
        .unwrap_or_else(|| panic!("{out}"));
    assert!(v2.contains("author:  alice"), "{v2}");
    assert!(v2.contains("message: first save"), "{v2}");
    assert!(v2.contains("tables:  1 (2 rows)"), "{v2}");
    assert!(v2.contains("changes: created: lake.t"), "{v2}");
    // 2 is not current anymore.
    assert!(!out.contains("version 2 (current)"), "{out}");
    // v3's record: the message only — the v2 message must not repeat.
    let v3 = out
        .split("version 3 (current)")
        .nth(1)
        .unwrap_or_else(|| panic!("{out}"));
    assert!(v3.contains("message: second save"), "{v3}");
    assert!(!v3.contains("author:"), "{v3}");
    assert!(v3.contains("tables:  1 (3 rows)"), "{v3}");
    assert!(
        v3.contains("changes: rewritten: lake.t; rows: lake.t +1"),
        "{v3}"
    );
    // v1 (the create) records nothing.
    let v1 = out
        .split("version 1\n")
        .nth(1)
        .unwrap_or_else(|| panic!("{out}"));
    assert!(v1.contains("changes: (not recorded)"), "{v1}");
    // Every record carries a publish time.
    assert!(out.contains("time:    20"), "{out}");
    // A malformed option prints the usage line (like every other `.lake`
    // usage error) and leaves the session running.
    let (stdout, stderr, code) = run_captured(&format!(
        "CREATE TABLE t (x INTEGER);\n.lake save {lake} message\n.quit\n",
        lake = lake.display()
    ));
    assert_eq!(code, 0, "usage lines do not fail the script");
    assert!(
        stdout.contains("Usage: .lake save <dir> [force] [author '<name>'] [message '<text>']"),
        "stdout: {stdout} stderr: {stderr}"
    );
    cleanup(&dir);
}

/// Time travel through the CLI: `AT (VERSION => n)` reads an attached lake's
/// older version, `AT (TIMESTAMP => …)` takes the time `.lake snapshots`
/// prints, and every refusal (unknown version, store table, malformed clause)
/// is a clean error that leaves the session alive.
#[test]
fn time_travel_queries_and_errors_on_an_attached_lake() {
    let dir = scratch("time_travel");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE DATABASE lake;\n\
         USE lake;\n\
         CREATE TABLE t (x INTEGER);\n\
         INSERT INTO t VALUES (1);\n\
         .lake save {lake} message 'one row'\n\
         INSERT INTO t VALUES (2), (3);\n\
         .lake save {lake} message 'three rows'\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 3"), "{out}");

    // The printed publish times, straight out of the listing.
    let listing = run(&format!(
        ".lake snapshots {lake}\n.quit\n",
        lake = lake.display()
    ));
    let mut times = listing
        .lines()
        .filter_map(|l| l.trim().strip_prefix("time:").map(str::trim))
        .collect::<Vec<_>>();
    assert_eq!(times.len(), 3, "{listing}");
    let v3_time = times.pop().unwrap().to_string();
    let v2_time = times.pop().unwrap().to_string();

    let (stdout, stderr, code) = run_captured(&format!(
        ".lake attach {lake}\n\
         USE lake;\n\
         SELECT 'at-v2' AS m, COUNT(*) AS v FROM t AT (VERSION => 2);\n\
         SELECT 'at-v3-1' AS m, x AS v FROM t AT (VERSION => 3) WHERE x = 1;\n\
         SELECT 'at-v3-3' AS m, x AS v FROM t AT (VERSION => 3) WHERE x = 3;\n\
         SELECT 'at-v3-ts' AS m, COUNT(*) AS v FROM t AT (TIMESTAMP => TIMESTAMP '{v3_time}');\n\
         SELECT 'at-v2-ts' AS m, COUNT(*) AS v FROM t AS old AT (TIMESTAMP => '{v2_time}');\n\
         SELECT 'current' AS m, COUNT(*) AS v FROM t;\n\
         SELECT * FROM t AT (VERSION => 1);\n\
         SELECT * FROM t AT (VERSION => 99);\n\
         CREATE TABLE plain (x INTEGER);\n\
         SELECT * FROM plain AT (VERSION => 2);\n\
         SELECT * FROM t AT (VERSION 2);\n\
         SELECT 'still-alive' AS m, COUNT(*) AS v FROM t;\n\
         .quit\n",
        lake = lake.display()
    ));
    // Each marked row pairs the probe name with the version's answer: v2
    // holds one row, v3 three, both timestamp forms resolve to the same
    // versions, and the current version is untouched by any of it.
    let marked = |marker: &str, value: &str| {
        stdout
            .lines()
            .any(|l| l.starts_with(&format!("│ {marker}")) && l.contains(&format!("│ {value} ")))
    };
    for (marker, value) in [
        ("at-v2", "1"),
        ("at-v3-1", "1"),
        ("at-v3-3", "3"),
        ("at-v3-ts", "3"),
        ("at-v2-ts", "1"),
        ("current", "3"),
        ("still-alive", "3"),
    ] {
        assert!(
            marked(marker, value),
            "{marker} → {value} missing from\n{stdout}\n{stderr}"
        );
    }
    // Every refusal names its reason.
    let errors = stderr.matches("Error: ").count();
    assert_eq!(errors, 4, "stderr: {stderr}");
    assert!(
        stderr.contains("table \"lake.t\" does not exist at version 1"),
        "{stderr}"
    );
    assert!(
        stderr.contains("no snapshot found at version 99"),
        "{stderr}"
    );
    assert!(
        stderr.contains("has no snapshot history"),
        "store tables cannot time-travel: {stderr}"
    );
    assert!(stderr.contains("parse error"), "{stderr}");
    // A script that reported errors exits non-zero (the CLI's contract).
    assert_eq!(code, 1);
    cleanup(&dir);
}

/// The writable attach route: `.lake attach <dir> write` materializes the
/// lake into the session store, DML accumulates there, and `.lake commit`
/// publishes exactly the difference as one incremental version — appended
/// rows land in new data files and deletes become positional delete files,
/// while every previously published data file stays byte- and
/// mtime-identical. A commit with nothing to publish is a no-op.
#[test]
fn writable_attach_commit_appends_without_rewriting() {
    let dir = scratch("commit");
    let lake = dir.join("lake");
    let mut script = String::from("CREATE TABLE t (a INTEGER, s VARCHAR);\n");
    for chunk in (0..200).collect::<Vec<_>>().chunks(50) {
        let values: Vec<String> = chunk.iter().map(|i| format!("({i}, 'v{i}')")).collect();
        script.push_str(&format!("INSERT INTO t VALUES {};\n", values.join(", ")));
    }
    script.push_str(&format!(
        ".lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    let out = run(&script);
    assert!(out.contains("Saved lake version 2"), "{out}");

    // Snapshot the published data files: bytes + mtime.
    let before: Vec<(PathBuf, Vec<u8>, std::time::SystemTime)> = data_files(&lake.join("data"))
        .into_iter()
        .map(|p| {
            let bytes = std::fs::read(&p).unwrap();
            let mtime = std::fs::metadata(&p).unwrap().modified().unwrap();
            (p, bytes, mtime)
        })
        .collect();
    assert_eq!(before.len(), 1, "one file per table per save");

    // Materialize writable, mutate through DML, commit. (Wave 68: inlining
    // is on by default; this test pins the file/delete-file shape via the
    // handle-default env knob.)
    let out = run_with_env(
        &format!(
            ".lake attach {lake} write\n\
         INSERT INTO t VALUES (999, 'late');\n\
         UPDATE t SET s = 'up' WHERE a = 0;\n\
         DELETE FROM t WHERE a = 1;\n\
         .lake commit {lake}\n\
         SELECT COUNT(*), MIN(a), MAX(a), COUNT(s) FROM t;\n\
         .lake attached\n\
         .quit\n",
            lake = lake.display()
        ),
        &[("PINKDB_LAKE_INLINING_ROW_LIMIT", "0")],
    );
    assert!(
        out.contains("Attached") && out.contains("writable"),
        "the writable attach must report itself:\n{out}"
    );
    assert!(out.contains("Committed lake version 3"), "{out}");
    assert!(out.contains("2 files: 1 data, 1 delete"), "{out}");
    // UPDATE = delete + append: the insert and the updated value are the two
    // appended rows; the deleted row and the updated-away row are the two
    // positional deletes.
    assert!(out.contains("+2 rows"), "{out}");
    assert!(out.contains("-2 rows"), "{out}");
    assert!(out.contains("0 retries"), "{out}");
    // 200 - 2 deleted + 2 appended (the insert and the updated value): the
    // session sees its own rows.
    assert!(out.contains("│ 200 "), "{out}");
    assert!(out.contains("│ 999 "), "{out}");
    // The attachment is clean now and stays attached.
    assert!(out.contains("(writable)"), "{out}");
    assert!(out.contains("no uncommitted changes"), "{out}");

    // No published data file was rewritten or touched.
    for (path, bytes, mtime) in &before {
        let found = data_files(&lake.join("data"))
            .into_iter()
            .find(|p| p == path);
        let found = found.expect("the original data file must stay");
        assert_eq!(&std::fs::read(&found).unwrap(), bytes, "{}", path.display());
        assert_eq!(
            std::fs::metadata(&found).unwrap().modified().unwrap(),
            *mtime,
            "{} was touched",
            path.display()
        );
    }
    // The new version lists both data files plus the delete file, and the
    // v4 manifest adds up.
    let catalog = std::fs::read_to_string(lake.join("catalog.json")).unwrap();
    assert!(catalog.contains("\"format_version\": 6"), "{catalog}");
    assert!(catalog.contains("\"rows\": 200"), "{catalog}");
    assert!(
        catalog.contains("\"rows\": 2"),
        "the appended file carries its own rows: {catalog}"
    );
    assert!(catalog.contains("\"count\": 2"), "{catalog}");
    assert!(catalog.contains("pinkdel"), "{catalog}");
    assert!(
        lake.join("commits/3.json").is_file(),
        "each commit writes its delta manifest"
    );

    // Committing again publishes nothing (idempotent).
    let out = run(&format!(
        ".lake attach {lake} write\n.lake commit {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains("Nothing to commit") && out.contains("version 3"),
        "{out}"
    );

    // The committed lake reads back with the exact logical content.
    let out = run(&format!(
        ".lake open {lake}\n\
         SELECT COUNT(*), MIN(a), MAX(a), COUNT(s) FROM t;\n\
         SELECT s FROM t WHERE a = 0;\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Opened lake version 3"), "{out}");
    assert!(out.contains("│ 200 "), "{out}");
    assert!(out.contains("up"), "the updated value survives: {out}");
    cleanup(&dir);
}

/// Wave 65: schema evolution through the writable attach — `ALTER TABLE`
/// (ADD COLUMN with DEFAULT, RENAME COLUMN, DROP COLUMN) runs against the
/// materialized session copy, `.lake commit` publishes one schema record,
/// old data files stay untouched and readable (the default materializes on
/// read; dropped columns stay on disk), and time travel reads each version's
/// own schema.
#[test]
fn writable_attach_schema_evolution_publishes_and_time_travels() {
    let dir = scratch("schema");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (i INTEGER, s VARCHAR);\n\
         INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three');\n\
         .lake save {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    let files_before = data_files(&lake.join("data")).len();
    assert_eq!(files_before, 1);

    let out = run_with_env(
        &format!(
            ".lake attach {lake} write\n\
         ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 42;\n\
         ALTER TABLE t ADD COLUMN tmp INTEGER;\n\
         UPDATE t SET n = 9 WHERE i = 2;\n\
         ALTER TABLE t RENAME COLUMN s TO label;\n\
         ALTER TABLE t DROP COLUMN tmp;\n\
         INSERT INTO t (i, n) VALUES (4, 7);\n\
         .lake commit {lake}\n\
         SELECT i, n FROM t ORDER BY i;\n\
         .quit\n",
            lake = lake.display()
        ),
        &[("PINKDB_LAKE_INLINING_ROW_LIMIT", "0")],
    );
    assert!(out.contains("Table t altered."), "{out}");
    assert!(
        out.contains("1 schema change"),
        "the commit reports its schema record: {out}"
    );
    assert!(out.contains("Committed lake version 3"), "{out}");
    assert!(out.contains("│ 9  "), "the updated row: {out}");

    // Old data files were never rewritten; the catalog carries the record.
    // The commit added one data file (the INSERT) and one delete file (the
    // UPDATE's old row).
    let data_only = |dir: &Path| {
        data_files(dir)
            .into_iter()
            .filter(|p| p.extension().is_some_and(|e| e != "pinkdel"))
            .count()
    };
    assert_eq!(data_only(&lake.join("data")), files_before + 1);
    let catalog = std::fs::read_to_string(lake.join("catalog.json")).unwrap();
    assert!(catalog.contains("\"format_version\": 6"), "{catalog}");
    assert!(catalog.contains("\"schema_version\": 2"), "{catalog}");
    assert!(catalog.contains("\"name\": \"n\""), "{catalog}");
    assert!(catalog.contains("\"default\": \"42\""), "{catalog}");
    assert!(catalog.contains("\"renames\""), "{catalog}");
    assert!(
        catalog.contains("\"from\": \"s\"") && catalog.contains("\"to\": \"label\""),
        "{catalog}"
    );

    // `.lake open` reads the evolved schema: old rows materialize 42.
    let out = run(&format!(
        ".lake open {lake}\n\
         SELECT i, n FROM t ORDER BY i;\n\
         .lake snapshots {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Opened lake version 3"), "{out}");
    assert!(
        out.contains("│ 42 "),
        "old rows read the published default: {out}"
    );
    assert!(out.contains("schema:  version 2"), "{out}");
    assert!(out.contains("schema:  version 1"), "{out}");

    // Time travel: the version before the schema change has the old name and
    // no `n`; the latest has `n` and no `s`.
    let out = run(&format!(
        ".lake attach {lake}\n\
         SELECT i, s FROM t AT (VERSION => 2) ORDER BY i;\n\
         SELECT i, n FROM t ORDER BY i;\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("one"), "{out}");
    assert!(out.contains("three"), "{out}");
    assert!(out.contains("│ 42 "), "{out}");
    cleanup(&dir);
}

/// A RENAME TABLE through a writable attach is refused with the way out
/// (incremental commits publish data and schema, not names).
#[test]
fn writable_attach_refuses_rename_table_with_a_hint() {
    let dir = scratch("rename-table");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (i INTEGER);\nINSERT INTO t VALUES (1);\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    let out = run(&format!(
        ".lake attach {lake} write\n\
         ALTER TABLE t RENAME TO t2;\n\
         .lake commit {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains("RENAME TABLE is not published through .lake commit"),
        "{out}"
    );
    assert!(out.contains(".lake save"), "{out}");
    // Nothing was published.
    let out = run(&format!(
        ".lake versions {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("2 (current)"), "{out}");
    cleanup(&dir);
}

/// Writable-attach lifecycle guards: detaching with pending changes refuses
/// (force drops the attachment and keeps the tables), `.lake open` refuses to
/// discard pending changes, and detaching clean is fine.
#[test]
fn writable_attach_detach_and_open_guards() {
    let dir = scratch("commit_guards");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1), (2);\n\
         .lake save {lake}\n.quit\n",
        lake = lake.display()
    ));

    // Pending changes: detach refuses; open refuses.
    let out = run(&format!(
        ".lake attach {lake} write\n\
         INSERT INTO t VALUES (3);\n\
         .lake detach {lake}\n\
         .lake open {lake}\n\
         .lake detach {lake} force\n\
         .lake open {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains("uncommitted changes") && out.contains("commit"),
        "{out}"
    );
    assert!(
        out.contains("would discard the uncommitted changes"),
        "{out}"
    );
    assert!(
        out.contains("Detached writable attach") && out.contains("ordinary tables"),
        "{out}"
    );
    // After the forced detach the attachment is gone, so open replaces the
    // store (the row that was never committed is gone with it).
    assert!(out.contains("Opened lake version 2"), "{out}");

    // Clean detach: no refusal.
    let out = run(&format!(
        ".lake attach {lake} write\n\
         .lake commit {lake}\n\
         .lake detach {lake}\n\
         .lake attached\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Nothing to commit"), "{out}");
    assert!(out.contains("Detached writable attach"), "{out}");
    assert!(out.contains("(no attached lakes)"), "{out}");

    // `.lake commit` without a writable attach is a clear error.
    let (out, err, code) = run_captured(&format!(".lake commit {}\n.quit\n", lake.display()));
    assert_eq!(code, 1);
    assert!(
        err.contains("no writable attach") && err.contains("write first"),
        "{out}\n{err}"
    );

    // `.lake save` publishes the store and re-pins the writable attach.
    let out = run(&format!(
        ".lake attach {lake} write\n\
         INSERT INTO t VALUES (7);\n\
         .lake save {lake}\n\
         .lake commit {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 3"), "{out}");
    assert!(out.contains("re-pinned to version 3"), "{out}");
    assert!(out.contains("Nothing to commit"), "{out}");
    cleanup(&dir);
}

/// A durable session's WAL covers writable-attach commits: statements are
/// logged, the materialized tables are checkpointed at attach time, and a
/// crash-recovered session re-attaches by adoption and re-commits
/// idempotently — a commit that already landed publishes nothing, and a
/// change that never reached the lake is published exactly once.
#[test]
fn durable_session_wal_covers_lake_commits() {
    let dir = scratch("commit_wal");
    let lake = dir.join("lake");
    let db = dir.join("d.pink");
    let db = db.display().to_string();

    // Session 1: durable, save v2, writable attach (checkpoints), insert,
    // commit v3, exit without a final checkpoint (the WAL holds the insert).
    let out = run(&format!(
        ".open {db}\n\
         CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (1), (2);\n\
         .lake save {lake}\n\
         .lake attach {lake} write\n\
         INSERT INTO t VALUES (3);\n\
         .lake commit {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Committed lake version 3"), "{out}");

    // Session 2: reopen (WAL replay), adopt the recovered table, and commit:
    // the change is already in the lake, so nothing is published.
    let out = run(&format!(
        ".open {db}\n\
         .lake attach {lake} write\n\
         .lake commit {lake}\n\
         SELECT COUNT(*) FROM t;\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Replayed"), "the WAL must replay: {out}");
    assert!(out.contains("adopted"), "{out}");
    assert!(out.contains("Nothing to commit"), "{out}");
    assert!(out.contains("│ 3 "), "{out}");

    // Session 3: insert without committing, then exit (a crash window).
    let out = run(&format!(
        ".open {db}\n\
         .lake attach {lake} write\n\
         INSERT INTO t VALUES (4);\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Opened"), "{out}");

    // Session 4: reopen, re-attach, commit: exactly the lost change lands.
    let out = run(&format!(
        ".open {db}\n\
         .lake attach {lake} write\n\
         .lake commit {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Committed lake version 4"), "{out}");
    assert!(out.contains("+1 row"), "{out}");

    // Session 5: idempotent again, and the lake reads back with all rows.
    let out = run(&format!(
        ".lake open {lake}\n\
         SELECT COUNT(*), MIN(a), MAX(a) FROM t;\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Opened lake version 4"), "{out}");
    assert!(out.contains("│ 4 "), "{out}");
    cleanup(&dir);
}

/// `.lake partition` end to end (wave 66): set a spec, commit split by value,
/// read with file pruning (the decode counters prove which files were
/// touched), then clear the spec and commit unpartitioned again.
#[test]
fn partition_spec_splits_commits_prunes_and_clears() {
    let dir = scratch("partition");
    let lake = dir.join("lake");

    // Session 1: create, save, partition, commit appended rows.
    let out = run_with_env(
        &format!(
            "CREATE TABLE t (id INTEGER, region VARCHAR);\n\
             INSERT INTO t VALUES (1, 'east'), (2, 'west');\n\
             .lake save {lake}\n\
             .lake partition {lake} t by (region)\n\
             .lake attach {lake} write\n\
             INSERT INTO t VALUES (3, 'south'), (4, 'south'), (5, 'north');\n\
             .lake commit {lake}\n\
             .quit\n",
            lake = lake.display()
        ),
        &[("PINKDB_LAKE_INLINING_ROW_LIMIT", "0")],
    );
    assert!(out.contains("Partitioned main.t by (region)"), "{out}");
    // The three appended rows split into two files (south ×2, north ×1).
    assert!(out.contains("2 files: 2 data, 0 delete"), "{out}");
    let catalog = std::fs::read_to_string(lake.join("catalog.json")).unwrap();
    assert!(catalog.contains("\"partition_by\""), "{catalog}");
    assert!(catalog.contains("\"column\": \"region\""), "{catalog}");
    assert!(catalog.contains("\"south\""), "{catalog}");
    assert!(catalog.contains("\"north\""), "{catalog}");

    // Session 2: read-only attach: predicates prune whole files from the zone
    // maps, and the counters show exactly which files decoded.
    let out = run(&format!(
        ".lake attach {lake}\n\
         .lake attached\n\
         SELECT id FROM t WHERE region = 'north';\n\
         .lake attached\n\
         SELECT id FROM t WHERE region = 'south';\n\
         .lake attached\n\
         SELECT MIN(id), MAX(id) FROM t;\n\
         .lake attached\n\
         .quit\n",
        lake = lake.display()
    ));
    let stats: Vec<&str> = out
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("decodes:"))
        .collect();
    assert_eq!(
        stats,
        vec![
            "decodes: 0 loads, 0 columns, 0 files",
            // v2's unpartitioned file (region range east..west) plus the
            // north file: the south file is pruned by its partition value.
            "decodes: 1 load, 2 columns, 2 files",
            // Same shape for south; the north file is pruned.
            "decodes: 2 loads, 4 columns, 4 files",
            // MIN/MAX answer from the published statistics (no deletes).
            "decodes: 2 loads, 4 columns, 4 files",
        ],
        "{out}"
    );
    assert!(out.contains("│ 5 "), "{out}"); // the north row
    assert!(out.contains("│ 3 ") && out.contains("│ 4 "), "{out}");

    // Session 3: clear the spec, then a new commit is one unpartitioned file
    // (inlining off: this pins the file shape).
    let out = run_with_env(
        &format!(
            ".lake partition {lake} t clear\n\
         .lake partition {lake} t clear\n\
         .lake attach {lake} write\n\
         INSERT INTO t VALUES (6, 'east');\n\
         .lake commit {lake}\n\
         .quit\n",
            lake = lake.display()
        ),
        &[("PINKDB_LAKE_INLINING_ROW_LIMIT", "0")],
    );
    assert!(out.contains("Cleared the partition specification"), "{out}");
    assert!(out.contains("has no partition specification"), "{out}");
    assert!(out.contains("1 file: 1 data, 0 delete"), "{out}");
    let catalog = std::fs::read_to_string(lake.join("catalog.json")).unwrap();
    assert!(!catalog.contains("partition_by"), "{catalog}");
    // Old files keep their recorded values; the new one carries none.
    assert!(catalog.contains("\"south\""), "{catalog}");
    cleanup(&dir);
}

/// `.lake partition` argument and DDL errors, pinned to the probed DuckLake
/// rules: unsupported transforms name DuckLake's supported set, duplicate
/// keys are refused, and missing tables/columns are clean errors.
#[test]
fn partition_command_errors_are_reported() {
    let dir = scratch("partition_err");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (id INTEGER, region VARCHAR);\n\
         INSERT INTO t VALUES (1, 'east');\n\
         .lake save {lake}\n\
         .quit\n",
        lake = lake.display()
    ));

    let out = run(&format!(
        ".lake partition {lake} t by (truncate(2, region))\n\
         .lake partition {lake} t by (region, region)\n\
         .lake partition {lake} t by (nope)\n\
         .lake partition {lake} ghost by (x)\n\
         .lake partition {lake} t\n\
         .lake partition {lake} t by ()\n\
         .lake partition {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains("unsupported partition key \"truncate(2, region)\""),
        "{out}"
    );
    assert!(out.contains("year, month, day, hour"), "{out}");
    assert!(out.contains("duplicate partition key \"region\""), "{out}");
    assert!(out.contains("does not exist"), "{out}");
    assert!(out.contains("does not exist in the lake"), "{out}");
    assert!(out.contains("Usage: .lake partition"), "{out}");
    // The failed commands published nothing: the lake is still version 2.
    let catalog = std::fs::read_to_string(lake.join("catalog.json")).unwrap();
    assert!(catalog.contains("\"version\": 2"), "{catalog}");
    cleanup(&dir);
}

/// Wave 67: the maintenance commands end to end — merge, files, table_info,
/// expire (dry-run then real), cleanup (dry-run, refusal, confirm), options
/// and settings — with the published-data immutability checked on disk.
#[test]
fn maintenance_commands_merge_expire_and_cleanup() {
    let dir = scratch("maint");
    let lake = dir.join("lake");
    // v2 saves one file; two writable-attach commits append two more.
    // (Wave 68: inlining is on by default; this test pins file writes via the
    // handle-default env knob, which publishes no version of its own.)
    run_with_env(
        &format!(
            "CREATE TABLE t (id INTEGER, s VARCHAR);\n\
             INSERT INTO t VALUES (1, 'a'), (2, 'b');\n\
             .lake save {lake}\n\
             .lake attach {lake} write\n\
             INSERT INTO t VALUES (3, 'c');\n\
             .lake commit {lake}\n\
             INSERT INTO t VALUES (4, 'd');\n\
             .lake commit {lake}\n\
             .lake detach {lake}\n\
             .quit\n",
            lake = lake.display()
        ),
        &[("PINKDB_LAKE_INLINING_ROW_LIMIT", "0")],
    );
    let data_dir = lake.join("data/main.t");
    let before: Vec<(PathBuf, Vec<u8>)> = std::fs::read_dir(&data_dir)
        .unwrap()
        .map(|e| {
            let path = e.unwrap().path();
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect();
    assert_eq!(before.len(), 3, "three data files before the merge");

    // Merge and inspect. The 2-day cleanup gate protects the merged sources
    // even after `all` until their snapshots are expired, so this run shows
    // "nothing to clean" and the dry-run expiration of version 2.
    let out = run(&format!(
        ".lake merge {lake}\n\
         .lake files {lake}\n\
         .lake table_info {lake}\n\
         .lake cleanup {lake} all dry-run\n\
         .lake expire {lake} versions=2 dry-run\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("main.t: merged 3 files into 1"), "{out}");
    assert!(out.contains("Merged 3 files into 1 in"), "{out}");
    assert!(out.contains("version 5"), "{out}");
    // The head file listing: one consolidated file with the exact rows.
    assert!(out.contains("table=main.t rows=4"), "{out}");
    assert!(out.contains("schema_version=1"), "{out}");
    // Table info: one file, four rows.
    assert!(out.contains("main.t: 1 file,"), "{out}");
    assert!(out.contains(", 4 rows,"), "{out}");
    // Every source is still referenced by a retained snapshot.
    assert!(out.contains("Nothing to cleanup in"), "{out}");
    assert!(out.contains("would expire version 2"), "{out}");
    assert!(!out.contains("Error:"), "{out}");

    // The merge never mutated a published file: byte-identical sources plus
    // the new consolidated one.
    for (path, bytes) in &before {
        assert_eq!(
            &std::fs::read(path).unwrap(),
            bytes,
            "{} changed",
            path.display()
        );
    }
    assert_eq!(
        std::fs::read_dir(&data_dir).unwrap().count(),
        4,
        "three sources + one merged file"
    );

    // Expire every pre-merge snapshot: the three sources become
    // unreferenced. Cleanup previews, refuses without confirmation, then
    // deletes on confirm; time travel to the expired version fails cleanly.
    let out = run(&format!(
        ".lake expire {lake} versions=2,3,4\n\
         .lake cleanup {lake} all dry-run\n\
         .lake cleanup {lake} all\n\
         .lake cleanup {lake} all confirm\n\
         .lake files {lake} all\n\
         .lake options {lake}\n\
         .lake settings {lake}\n\
         .lake open {lake} 2\n\
         .lake open {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("expired version 2"), "{out}");
    assert!(out.contains("expired version 3"), "{out}");
    assert!(out.contains("expired version 4"), "{out}");
    assert!(out.contains("Expired 3 snapshots from"), "{out}");
    assert!(out.contains("3 objects would be cleaned up"), "{out}");
    assert!(
        out.contains("refusing to delete 3 unreferenced lake object(s) without confirmation"),
        "{out}"
    );
    assert!(out.contains("Deleted 3 objects from"), "{out}");
    assert!(
        out.contains("1 file (1 referenced, 0 unreferenced)"),
        "{out}"
    );
    assert!(out.contains("data_format: "), "{out}");
    assert!(out.contains("data_path: "), "{out}");
    assert!(out.contains("catalog_type:      json"), "{out}");
    assert!(out.contains("extension_version: "), "{out}");
    assert!(out.contains("has no snapshot version 2"), "{out}");
    assert!(!out.contains("Error: corrupt"), "{out}");
    // Only the consolidated head file remains.
    assert_eq!(std::fs::read_dir(&data_dir).unwrap().count(), 1);
    cleanup(&dir);
}

/// Wave 68: a small writable-attach commit inlines its rows into the catalog
/// (no data file, reported by `.lake commit`), `.lake flush` materializes
/// them into a data file, `.lake changes` renders the version range's
/// insertions/deletions, and `.lake inlining` persists a per-table limit.
#[test]
fn inlining_flush_changes_and_inlining_limit_round_trip() {
    let dir = scratch("inlining");
    let lake = dir.join("lake");
    let script = format!(
        "CREATE TABLE t (i INTEGER, s VARCHAR);\n\
         INSERT INTO t VALUES (1, 'a');\n\
         .lake save {lake}\n\
         .quit\n",
        lake = lake.display()
    );
    let out = run(&script);
    assert!(out.contains("Saved lake version 2"), "{out}");

    // A one-row append inlines: no data file, the report says so.
    let out = run(&format!(
        ".lake attach {lake} write\n\
         INSERT INTO t VALUES (2, 'b');\n\
         .lake commit {lake}\n\
         SELECT COUNT(*) FROM t;\n\
         .lake detach {lake}\n\
         .lake flush {lake}\n\
         .lake changes {lake} main.t 2 3\n\
         .lake inlining {lake} main.t 0\n\
         .lake inlining {lake} main.t default\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("0 files: 0 data, 0 delete"), "{out}");
    assert!(out.contains("(1 row inlined)"), "{out}");
    assert!(out.contains("│ 2 "), "the inlined row is visible: {out}");
    assert!(
        out.contains("Flushed 1 inlined row of main.t"),
        "the flush materializes it: {out}"
    );
    // `.lake changes`: the v2 save's row and the v3 inlined row, inclusive.
    assert!(out.contains("version 2  rowid 0  insert  1 'a'"), "{out}");
    assert!(out.contains("version 3  rowid 1  insert  2 'b'"), "{out}");
    assert!(
        out.contains("commits appending at most 0 rows are stored in the catalog metadata"),
        "{out}"
    );
    assert!(
        out.contains("Cleared the inlining limit of main.t"),
        "{out}"
    );
    // The flush published a real data file.
    assert_eq!(data_files(&lake.join("data")).len(), 2, "save + flush");
    cleanup(&dir);
}

/// Wave 68: `.lake add_data_files` registers an existing in-lake data file as
/// rows of a table, `.lake files` names it, and the rows read back; a path
/// outside `data/` is refused with the documented message.
#[test]
fn add_data_files_registers_through_the_cli() {
    let dir = scratch("register");
    let lake = dir.join("lake");
    let values: Vec<String> = (0..12).map(|i| format!("({i})")).collect();
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES {};\n\
         .lake save {lake}\n\
         .lake files {lake}\n\
         .lake add_data_files {lake} main.t data/main.t/nope.vortex\n\
         .lake add_data_files {lake} main.t elsewhere/x.vortex\n\
         .quit\n",
        values.join(", "),
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    assert!(out.contains("no files found that match"), "{out}");
    assert!(out.contains("is not a lake-relative"), "{out}");

    // Register the saved file itself (its rows appear a second time).
    let files_line = out
        .lines()
        .find(|l| l.starts_with("data/main.t/"))
        .expect("`.lake files` lists the data file")
        .to_string();
    let path = files_line.split(':').next().unwrap().to_string();
    let out = run(&format!(
        ".lake add_data_files {lake} main.t {path}\n\
         .lake open {lake}\n\
         SELECT COUNT(*) FROM t;\n\
         .lake changes {lake} main.t\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains(&format!("Registered {path}")), "{out}");
    assert!(out.contains("Opened lake version 3"), "{out}");
    assert!(out.contains("│ 24 "), "12 saved + 12 registered: {out}");
    assert!(out.contains("version 3  rowid"), "{out}");
    cleanup(&dir);
}
