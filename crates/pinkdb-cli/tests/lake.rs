//! CLI coverage for the opt-in `pinklake` feature: `.lake save/open/versions/
//! tables/info` end to end, in a temp lake directory.
//!
//! This file only compiles when the feature is ON; the feature-less behavior
//! is covered by `tests/lake_disabled.rs`.
//!
//! Set `PINKDB_CLI_BIN` to run the whole file against another CLI build
//! (e.g. a release binary) — the overflow tests then prove release behavior.

#![cfg(feature = "pinklake")]

mod common;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// The `data <location>` usage option, printed by a build with a lake or SQL
/// catalog feature (a `.pink` catalog accepts it too).
#[cfg(any(
    feature = "pinklake",
    feature = "catalog-sqlite",
    feature = "catalog-postgres"
))]
const DATA_OPT: &str = " [data <dir|s3://…>]";
#[cfg(not(any(
    feature = "pinklake",
    feature = "catalog-sqlite",
    feature = "catalog-postgres"
)))]
const DATA_OPT: &str = "";

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

fn catalog_sql(lake: &Path, sql: &str) {
    let helper = lake.join("catalog.pink");
    let path = if helper.exists() || lake.join("catalog.pink.wal").exists() {
        helper
    } else {
        lake.to_path_buf()
    };
    let mut db = pinkdb::PinkDb::open_durable(&path.display().to_string()).unwrap();
    db.execute(sql).unwrap();
}

/// A bare-path lake's data root: `<path>.files/` (the library default).
fn data_root(lake: &Path) -> PathBuf {
    PathBuf::from(format!("{}.files", lake.display())).join("data")
}

/// The `pink:` catalog URL for a bare-path lake.
fn catalog_url(lake: &Path) -> String {
    format!("pink:{}", lake.display())
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

    // The lake is a catalog database plus its `<path>.files/` data root.
    assert!(PathBuf::from(format!("{}.files", lake.display())).is_dir());
    let data = std::fs::read_dir(data_root(&lake).join("main.t")).unwrap();
    for entry in data {
        let path = entry.unwrap().path();
        assert_eq!(path.extension().unwrap(), "pink", "{}", path.display());
    }

    // Info/versions/tables report the metadata without touching the store.
    let out = run(&format!(
        ".lake info {lake}\n.lake versions {lake}\n.lake tables {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("format version:  7"), "{out}");
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
    assert!(out.contains("is not a pink lake"), "{out}");

    // Usage errors.
    let out = run(
        ".lake save\n.lake open\n.lake versions\n.lake snapshots\n.lake tables\n.lake info\n.lake\n\
         .quit\n",
    );
    assert!(out.contains("Usage: .lake save <catalog>"), "{out}");
    assert!(
        out.contains(&format!("Usage: .lake open <catalog>{DATA_OPT} [version]")),
        "{out}"
    );
    assert!(out.contains("Usage: .lake versions <dir>"), "{out}");
    assert!(out.contains("Usage: .lake snapshots <dir>"), "{out}");
    assert!(out.contains("Usage: .lake tables <dir>"), "{out}");
    assert!(out.contains("Usage: .lake info <dir>"), "{out}");
    assert!(
        out.contains(
            "Usage: .lake <save|open|replicate|partition|inlining|format|rewrite|versions|\
             snapshots|tables|info> ..."
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
    assert!(
        out.contains(&format!("Usage: .lake open <catalog>{DATA_OPT} [version]")),
        "{out}"
    );
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
        !dotted.join("data").exists(),
        "a rejected save must write no data: {out}"
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
        !long.join("data").exists(),
        "a rejected save must write no data: {out}"
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
        let data_file = data_files(&data_root(&enc_lake))
            .into_iter()
            .next()
            .expect("a data file");
        let bytes = std::fs::read(&data_file).unwrap();
        assert_eq!(
            &bytes[..8],
            b"PNKLAKE1",
            "a keyed .lake save must write encrypted data containers"
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
    assert!(out.contains("format version:  7"), "{out}");

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

    // A lake database unseals the footer/statistics and answers counts.
    let out = run_with_env(
        &format!(
            "{attach}SELECT COUNT(*) FROM t;\n.quit\n",
            attach = attach_ro(&lake),
        ),
        &key,
    );
    assert!(out.contains("Database lk created."), "{out}");
    assert!(!out.contains("Error:"), "{out}");
    assert!(out.contains("│ 2 "), "{out}");

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
    // The other maintenance paths use the same keyed open: an attachment and
    // the write functions unseal an encrypted lake.
    let out = run_with_env(
        &format!(
            ".lake inlining {lake} main.t 0\n\
             .lake inlining {lake} main.t default\n\
             ATTACH 'lake:{lake}' AS lk;\n\
             CALL ducklake_flush_inlined_data('lk');\n\
             .quit\n",
            lake = lake.display()
        ),
        &key,
    );
    assert!(
        !out.contains("the lake is encrypted"),
        "inlining/attach/flush must unseal a keyed lake: {out}"
    );
    // ... and without the key the same work still fails closed.
    for work in [
        ".lake partition {lake} t clear",
        ".lake inlining {lake} main.t 0",
        "ATTACH 'lake:{lake}' AS lk;\nCALL ducklake_flush_inlined_data('lk');",
        "ATTACH 'lake:{lake}' AS lk;\n\
         CALL ducklake_add_data_files('lk', 'main.t', './x.pink');",
    ] {
        let script = work.replace("{lake}", &lake.display().to_string());
        let out = run_no_key(&format!("{script}\n.quit\n"));
        assert!(
            out.contains("the lake is encrypted"),
            "a keyless {work} must refuse an encrypted lake: {out}"
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
fn missing_data_files_are_warned_about() {
    let dir = scratch("warn_missing");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1);\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));

    let mut files: Vec<PathBuf> = std::fs::read_dir(data_root(&lake).join("main.t"))
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

        // The version chain has no gaps.
        let version = 2 + winners.len() as u64;
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
    }
    cleanup(&dir);
}

/// A tampered lake data file must produce a clean CLI error and leave the
/// REPL (and the process) alive.
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
    let file = std::fs::read_dir(data_root(&lake).join("main.t"))
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
    // A broken trailer (the last bytes carry the footer location and magic).
    {
        let mut b = original.clone();
        let last = b.len() - 1;
        b[last] ^= 0xFF;
        tampers.push(("broken trailer", b));
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
                    || stderr.contains("lake data error")
                    || stderr.contains("corrupt"),
                "{what}: error is not a clean corrupt-file error: {stderr}"
            );
        }
    }
    assert!(
        saw_error,
        "at least one tamper must fail `.lake open` with a clean error"
    );

    // The same tampers against a read-only lake database + a projected
    // fallback query:
    // attach may fail at the footer (clean) or succeed and fail at the
    // decode (clean); a panic or a dead REPL is never acceptable.
    for (what, bytes) in &tampers {
        std::fs::write(&file, bytes).unwrap();
        let (stdout, stderr, code) = run_captured(&format!(
            "{attach}SELECT SUM(i) FROM t;\nSELECT * FROM t;\nUSE main;\n.databases\n.quit\n",
            attach = attach_ro(&lake),
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
                    || stderr.contains("lake data error")
                    || stderr.contains("corrupt"),
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
    // An `s3://` URL alone is a data location, not a catalog: the error
    // names the catalog + data form.
    let dir = scratch("s3_feature");
    let lake = dir.join("lake");
    let out = run(&format!(
        ".lake save s3://bucket/prefix\n\
         .lake save pink:{lake} data s3://bucket/prefix\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains("is a data location, not a lake catalog"),
        "{out}"
    );
    assert!(
        out.contains(".lake save pink:lake.pink data s3://bucket/prefix"),
        "{out}"
    );
    // A catalog with an S3 data location needs the `s3` feature.
    assert!(out.contains("does not include the `s3` feature"), "{out}");
    assert!(out.contains("--features pinklake,s3"), "{out}");
    assert!(out.contains("s3://bucket/prefix"), "{out}");
    // A malformed s3 URL is a location error, not a feature error.
    let out = run(".lake info s3://\n.quit\n");
    assert!(out.contains("invalid lake location"), "{out}");
    cleanup(&dir);
}

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
// Lake databases (wave 124): `ATTACH 'lake:…' AS name [(READ_ONLY)]` replaced
// `.lake attach`, `.lake detach`, `.lake attached` and `.lake commit`.

/// `ATTACH '<lake>' AS lk (READ_ONLY); USE lk;` — a read-only lake database.
fn attach_ro(lake: &Path) -> String {
    format!(
        "ATTACH 'lake:{}' AS lk (READ_ONLY);\nUSE lk;\n",
        lake.display()
    )
}

/// `ATTACH '<lake>' AS lk; USE lk;` — a writable lake database.
fn attach_rw(lake: &Path) -> String {
    format!("ATTACH 'lake:{}' AS lk;\nUSE lk;\n", lake.display())
}

/// The core equivalence gate: the same data, the same queries, three arms —
/// in-memory store, `.lake open`, a read-only lake database — must produce
/// identical transcripts (values included).
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
    // Arm 3: a read-only lake database (statistics first + lazy
    // materialization), under the lake schema's own name.
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
        "the read-only lake database diverged from the in-memory answers"
    );
    cleanup(&dir);
}

/// `pinkdb_lake_decodes()` reports data-file decodes end to end:
/// statistics-only queries decode nothing, projection pushdown decodes
/// exactly the referenced columns, and columns already decoded stay cached.
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
    let d = common::DECODES;
    let out = run(&format!(
        "{attach}{d}\
         SELECT COUNT(*), COUNT(b), MIN(a), MAX(a), MIN(c), MAX(c) FROM t;\n{d}\
         SELECT b FROM t ORDER BY b;\n{d}\
         SELECT c FROM t ORDER BY c;\n{d}\
         SELECT a FROM t ORDER BY a;\n{d}\
         SELECT * FROM t;\n{d}\
         .quit\n",
        attach = attach_ro(&lake),
    ));
    assert_eq!(
        common::decode_lines(&out),
        vec![
            "decodes: 0 loads, 0 columns, 0 files", // attach reads footers only
            "decodes: 0 loads, 0 columns, 0 files", // every aggregate came from statistics
            "decodes: 1 loads, 1 columns, 1 files", // ORDER BY b: b only
            "decodes: 2 loads, 2 columns, 2 files", // + c (the file decodes again)
            "decodes: 3 loads, 3 columns, 3 files", // + a
            "decodes: 3 loads, 3 columns, 3 files", // SELECT *: everything was cached
        ],
        "{out}"
    );
    cleanup(&dir);
}

/// A read-only lake database's lifecycle: SHOW DATABASES, queries, DETACH,
/// the read-only refusals, and the attach guards.
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
        "{attach}SHOW DATABASES;\n\
         SELECT COUNT(*), COUNT(b), MIN(a), MAX(b) FROM t;\n\
         SELECT k FROM lk.u;\n\
         USE main;\n\
         DETACH lk;\n\
         SELECT COUNT(*) FROM lk.t;\n\
         SHOW DATABASES;\n\
         .quit\n",
        attach = attach_ro(&lake),
    ));
    assert!(out.contains("Database lk created."), "{out}");
    assert!(
        out.contains("│ lake "),
        "SHOW DATABASES names the storage: {out}"
    );
    assert!(out.contains("│ 3 "), "{out}");
    assert!(out.contains("│ 2 "), "{out}");
    assert!(out.contains("│ 7 "), "{out}");
    assert!(out.contains("Database lk dropped."), "{out}");
    assert!(out.contains("not found"), "{out}");

    // Mutations against a read-only lake database are refused, and so is a
    // new table in it.
    let out = run(&format!(
        "{attach}INSERT INTO t VALUES (4, 'q');\n\
         UPDATE t SET a = 9;\n\
         DELETE FROM t;\n\
         DROP TABLE t;\n\
         CREATE TABLE t2 (a INTEGER, b VARCHAR);\n\
         .quit\n",
        attach = attach_ro(&lake),
    ));
    assert_eq!(out.matches("read-only lake").count(), 5, "{out}");
    assert!(out.contains("without READ_ONLY"), "{out}");

    // Attaching inside a transaction is refused (a ROLLBACK cannot undo it),
    // and so is a second attach of the same lake, and a name that exists.
    let out = run(&format!(
        "BEGIN;\nATTACH 'lake:{lake}' AS lk (READ_ONLY);\nROLLBACK;\n\
         ATTACH 'lake:{lake}' AS lk (READ_ONLY);\n\
         ATTACH 'lake:{lake}' AS again (READ_ONLY);\n\
         ATTACH 'lake:{lake}' AS main (READ_ONLY);\n.quit\n",
        lake = lake.display()
    ));
    assert!(
        out.contains("ATTACH is not allowed inside a transaction"),
        "{out}"
    );
    assert!(out.contains("already attached as database \"lk\""), "{out}");
    assert!(out.contains("database \"main\" already exists"), "{out}");

    // DROP DATABASE does not forget a lake; DETACH does. DETACH refuses a
    // local database.
    let out = run(&format!(
        "{attach}USE main;\nDROP DATABASE lk;\nDETACH main;\nDETACH IF EXISTS nope;\n.quit\n",
        attach = attach_ro(&lake),
    ));
    assert!(out.contains("DETACH it (the lake keeps its data)"), "{out}");
    assert!(out.contains("is a local database"), "{out}");
    cleanup(&dir);
}

/// Attach and its queries never write to the lake: the catalog keeps its
/// version and every data file is untouched.
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

    let data_dir = data_root(&lake).join("main.t");
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
        "{attach}SELECT COUNT(*), MIN(a), MAX(s) FROM t;\n\
         SELECT SUM(a) FROM t;\n\
         SELECT * FROM t;\n\
         .quit\n",
        attach = attach_ro(&lake),
    ));
    assert!(!out.contains("Error:"), "{out}");

    let info = run(&format!(".lake info {}\n.quit\n", lake.display()));
    assert!(info.contains("current version: 2"), "{info}");
    for (path, bytes) in data_files.iter().zip(&before) {
        assert_eq!(&std::fs::read(path).unwrap(), bytes, "{}", path.display());
    }
    cleanup(&dir);
}

/// `.lake open` replaces the store: its lake databases go with it.
#[test]
fn open_after_attach_resets_the_session() {
    let dir = scratch("attach_then_open");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1);\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    let out = run(&format!(
        "{attach}SELECT COUNT(*) FROM t;\n\
         .lake open {lake}\n\
         SELECT COUNT(*) FROM t;\n\
         SELECT COUNT(*) FROM lk.t;\n\
         .quit\n",
        attach = attach_ro(&lake),
        lake = lake.display()
    ));
    assert!(out.contains("Opened lake version 2 in"), "{out}");
    assert!(
        out.contains("database \"lk\" not found") || out.contains("not found"),
        "{out}"
    );
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

/// `ATTACH … (READ_ONLY, VERSION n)` pins one lake version.
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
        "ATTACH 'lake:{lake}' AS v2 (READ_ONLY, VERSION 2);\n\
         SELECT 'pinned' AS m, COUNT(*) AS v FROM v2.t;\n\
         DETACH v2;\n\
         ATTACH 'lake:{lake}' AS lk (READ_ONLY);\n\
         SELECT 'latest' AS m, COUNT(*) AS v FROM lk.t;\n\
         ATTACH 'lake:{lake}' AS w (VERSION 2);\n\
         .quit\n",
        lake = lake.display()
    ));
    let marked = |marker: &str, value: &str| {
        out.lines()
            .any(|l| l.starts_with(&format!("│ {marker}")) && l.contains(&format!("│ {value} ")))
    };
    assert!(
        marked("pinned", "1"),
        "the version-2 attachment sees one row: {out}"
    );
    assert!(
        marked("latest", "2"),
        "the latest attachment sees two rows: {out}"
    );
    assert!(out.contains("VERSION pins a read-only attachment"), "{out}");
    cleanup(&dir);
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
        stdout.contains(&format!(
            "Usage: .lake save <catalog>{DATA_OPT} [force] [author '<name>'] [message '<text>']"
        )),
        "stdout: {stdout} stderr: {stderr}"
    );
    cleanup(&dir);
}

/// Time travel through the CLI: `AT (VERSION => n)` reads a lake database's
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
        "ATTACH 'lake:{lake}' AS lake (READ_ONLY);\n\
         USE lake.lake;\n\
         SELECT 'at-v2' AS m, COUNT(*) AS v FROM t AT (VERSION => 2);\n\
         SELECT 'at-v3-1' AS m, x AS v FROM t AT (VERSION => 3) WHERE x = 1;\n\
         SELECT 'at-v3-3' AS m, x AS v FROM t AT (VERSION => 3) WHERE x = 3;\n\
         SELECT 'at-v3-ts' AS m, COUNT(*) AS v FROM t AT (TIMESTAMP => TIMESTAMP '{v3_time}');\n\
         SELECT 'at-v2-ts' AS m, COUNT(*) AS v FROM t AS old AT (TIMESTAMP => '{v2_time}');\n\
         SELECT 'current' AS m, COUNT(*) AS v FROM t;\n\
         SELECT * FROM t AT (VERSION => 1);\n\
         SELECT * FROM t AT (VERSION => 99);\n\
         CREATE TABLE main.main.plain (x INTEGER);\n\
         SELECT * FROM main.main.plain AT (VERSION => 2);\n\
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

/// A writable lake database: every committed unit publishes exactly its
/// difference as one incremental version — appended rows land in new data
/// files and deletes become positional delete files, while every previously
/// published data file stays byte- and mtime-identical.
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
    let before: Vec<(PathBuf, Vec<u8>, std::time::SystemTime)> = data_files(&data_root(&lake))
        .into_iter()
        .map(|p| {
            let bytes = std::fs::read(&p).unwrap();
            let mtime = std::fs::metadata(&p).unwrap().modified().unwrap();
            (p, bytes, mtime)
        })
        .collect();
    assert_eq!(before.len(), 1, "one file per table per save");

    // Mutate through DML in one transaction: one lake version. (Wave 68:
    // inlining is on by default; this test pins the file/delete-file shape
    // via the handle-default env knob.)
    let out = run_with_env(
        &format!(
            "{attach}BEGIN;\n\
             INSERT INTO t VALUES (999, 'late');\n\
             UPDATE t SET s = 'up' WHERE a = 0;\n\
             DELETE FROM t WHERE a = 1;\n\
             COMMIT;\n\
             SELECT COUNT(*), MIN(a), MAX(a), COUNT(s) FROM t;\n\
             .quit\n",
            attach = attach_rw(&lake),
        ),
        &[("PINKDB_LAKE_INLINING_ROW_LIMIT", "0")],
    );
    assert!(!out.contains("Error:"), "{out}");
    // 200 - 2 deleted + 2 appended (the insert and the updated value): the
    // session sees its own rows.
    assert!(out.contains("│ 200 "), "{out}");
    assert!(out.contains("│ 999 "), "{out}");
    let info = run(&format!(".lake info {}\n.quit\n", lake.display()));
    assert!(info.contains("current version: 3"), "{info}");

    // No published data file was rewritten or touched.
    for (path, bytes, mtime) in &before {
        let found = data_files(&data_root(&lake))
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
    // The new version wrote one data file plus one delete file.
    let files = data_files(&data_root(&lake));
    assert!(
        files
            .iter()
            .any(|p| p.extension().is_some_and(|e| e == "pinkdel")),
        "a delete file was written: {files:?}"
    );

    // Attaching again and changing nothing publishes nothing.
    let out = run(&format!(
        "{attach}SELECT 1;\n.quit\n",
        attach = attach_rw(&lake)
    ));
    assert!(!out.contains("Error:"), "{out}");
    let info = run(&format!(".lake info {}\n.quit\n", lake.display()));
    assert!(info.contains("current version: 3"), "{info}");

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

/// A RENAME TABLE in a writable lake database publishes the table under its
/// new name (the old one is dropped from the lake, the rows move with it).
#[test]
fn writable_attach_publishes_a_rename_table() {
    let dir = scratch("rename-table");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (i INTEGER);\nINSERT INTO t VALUES (1);\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    let out = run(&format!(
        "{attach}ALTER TABLE t RENAME TO t2;\n.quit\n",
        attach = attach_rw(&lake),
    ));
    assert!(!out.contains("Error:"), "{out}");
    let out = run(&format!(
        ".lake tables {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("main.t2"), "{out}");
    assert!(!out.contains("main.t "), "{out}");
    cleanup(&dir);
}

/// A durable session with a writable lake database: the lake is part of the
/// store (persisted in its checkpoint and WAL), every statement on it is
/// published to the lake rather than logged, and a reopened session reopens
/// the lake.
#[test]
fn durable_session_wal_covers_lake_commits() {
    let dir = scratch("commit_wal");
    let lake = dir.join("lake");
    let db = dir.join("d.pink");
    let db = db.display().to_string();

    // Session 1: save a lake, attach it writable in a durable store, insert,
    // and exit without a checkpoint (the WAL holds the ATTACH).
    let out = run(&format!(
        ".open {db}\n\
         CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES (1), (2);\n\
         .lake save {lake}\n\
         DROP TABLE t;\n\
         {attach}\
         INSERT INTO t VALUES (3);\n\
         .quit\n",
        lake = lake.display(),
        attach = attach_rw(&lake),
    ));
    assert!(!out.contains("Error:"), "{out}");

    // Session 2: reopen (WAL replay) reattaches the lake with all rows.
    let out = run(&format!(
        ".open {db}\n\
         SELECT COUNT(*) FROM lk.t;\n\
         INSERT INTO lk.t VALUES (4);\n\
         CHECKPOINT;\n\
         .quit\n"
    ));
    assert!(out.contains("Replayed"), "the WAL must replay: {out}");
    assert!(out.contains("│ 3 "), "{out}");

    // Session 3: reopen from the checkpoint; the lake reads back too.
    let out = run(&format!(
        ".open {db}\nSELECT COUNT(*), MAX(a) FROM lk.t;\n.quit\n"
    ));
    assert!(out.contains("│ 4 "), "{out}");
    let out = run(&format!(
        ".lake open {lake}\n\
         SELECT COUNT(*), MIN(a), MAX(a) FROM t;\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("│ 4 "), "{out}");
    cleanup(&dir);
}

/// `.lake partition` end to end (wave 66): set a spec, commit split by value,
/// read with file pruning, then clear the spec and commit unpartitioned
/// again.
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
             {attach}\
             INSERT INTO t VALUES (3, 'south'), (4, 'south'), (5, 'north');\n\
             .quit\n",
            lake = lake.display(),
            attach = attach_rw(&lake),
        ),
        &[("PINKDB_LAKE_INLINING_ROW_LIMIT", "0")],
    );
    assert!(out.contains("Partitioned main.t by (region)"), "{out}");
    // The three appended rows split into two files (south ×2, north ×1):
    // v2's file plus two.
    assert_eq!(data_files(&data_root(&lake)).len(), 3, "{out}");

    // Session 2: a read-only lake database: predicates prune whole files
    // from the zone maps, and the counters show exactly which files decoded.
    let d = common::DECODES;
    let out = run(&format!(
        "{attach}{d}\
         SELECT 'north' AS m, id AS v FROM t WHERE region = 'north';\n{d}\
         SELECT id FROM t WHERE region = 'south' ORDER BY id;\n{d}\
         SELECT MIN(id), MAX(id) FROM t;\n{d}\
         .quit\n",
        attach = attach_ro(&lake),
    ));
    assert_eq!(
        common::decode_lines(&out),
        vec![
            "decodes: 0 loads, 0 columns, 0 files",
            // v2's unpartitioned file (region range east..west) plus the
            // north file: the south file is pruned by its partition value.
            "decodes: 1 loads, 2 columns, 2 files",
            // Same shape for south; the north file is pruned.
            "decodes: 2 loads, 4 columns, 4 files",
            // MIN/MAX answer from the published statistics (no deletes).
            "decodes: 2 loads, 4 columns, 4 files",
        ],
        "{out}"
    );
    assert!(
        out.lines()
            .any(|l| l.contains("│ north ") && l.contains("│ 5 ")),
        "{out}"
    ); // the north row
    assert!(out.contains("│ 3 ") && out.contains("│ 4 "), "{out}");

    // Session 3: clear the spec, then a new commit is one unpartitioned file
    // (inlining off: this pins the file shape).
    let out = run_with_env(
        &format!(
            ".lake partition {lake} t clear\n\
         .lake partition {lake} t clear\n\
         {attach}\
         INSERT INTO t VALUES (6, 'east');\n\
         .quit\n",
            lake = lake.display(),
            attach = attach_rw(&lake),
        ),
        &[("PINKDB_LAKE_INLINING_ROW_LIMIT", "0")],
    );
    assert!(out.contains("Cleared the partition specification"), "{out}");
    assert!(out.contains("has no partition specification"), "{out}");
    assert_eq!(
        data_files(&data_root(&lake)).len(),
        4,
        "one more file: {out}"
    );
    // Old files keep their recorded values; the new one carries none (the
    // queries above prove both).
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
    let info = run(&format!(".lake info {}\n.quit\n", lake.display()));
    assert!(info.contains("current version: 2"), "{info}");
    cleanup(&dir);
}

/// Wave 67: the maintenance commands end to end — merge, files, table_info,
/// expire (dry-run then real), cleanup (dry-run, refusal, confirm), options
/// and settings — with the published-data immutability checked on disk.
#[test]
fn maintenance_commands_merge_expire_and_cleanup() {
    let dir = scratch("maint");
    let lake = dir.join("lake");
    // v2 saves one file; two writable lake database commits append two more.
    // (Wave 68: inlining is on by default; this test pins file writes via the
    // handle-default env knob, which publishes no version of its own.)
    run_with_env(
        &format!(
            "CREATE TABLE t (id INTEGER, s VARCHAR);\n\
             INSERT INTO t VALUES (1, 'a'), (2, 'b');\n\
             .lake save {lake}\n\
             {attach}\
             INSERT INTO t VALUES (3, 'c');\n\
             INSERT INTO t VALUES (4, 'd');\n\
             USE main;\n\
             DETACH lk;\n\
             .quit\n",
            lake = lake.display(),
            attach = attach_rw(&lake),
        ),
        &[("PINKDB_LAKE_INLINING_ROW_LIMIT", "0")],
    );
    let data_dir = data_root(&lake).join("main.t");
    let before: Vec<(PathBuf, Vec<u8>)> = std::fs::read_dir(&data_dir)
        .unwrap()
        .map(|e| {
            let path = e.unwrap().path();
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect();
    assert_eq!(before.len(), 3, "three data files before the merge");

    // Merge and inspect through the DuckLake SQL functions.
    let out = run(&format!(
        "{attach}\
         CALL ducklake_merge_adjacent_files('lk');\n\
         SELECT 'version=' || CAST(id AS VARCHAR) AS version \
           FROM ducklake_current_snapshot('lk');\n\
         SELECT 'files=' || CAST(COUNT(*) AS VARCHAR) AS files \
           FROM ducklake_list_files('lk', 't');\n\
         SELECT 'rows=' || CAST(COUNT(*) AS VARCHAR) AS n FROM t;\n\
         SELECT 'table_files=' || CAST(SUM(file_count) AS VARCHAR) AS table_files \
           FROM ducklake_table_info('lk');\n\
         CALL ducklake_cleanup_old_files('lk', cleanup_all := true, dry_run := true);\n\
         SELECT 'still=' || CAST(COUNT(*) AS VARCHAR) AS still \
           FROM ducklake_list_files('lk', 't');\n\
         CALL ducklake_expire_snapshots('lk', versions := [1], dry_run := true);\n\
         SELECT 'kept=' || CAST(COUNT(*) AS VARCHAR) AS kept \
           FROM ducklake_snapshots('lk') WHERE snapshot_id = 1;\n\
         .quit\n",
        attach = attach_rw(&lake),
    ));
    assert!(out.contains("version=4"), "{out}");
    assert!(out.contains("files=1"), "{out}");
    assert!(out.contains("rows=4"), "{out}");
    assert!(out.contains("table_files=1"), "{out}");
    assert!(out.contains("still=1"), "{out}");
    assert!(out.contains("kept=1"), "{out}");
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
    // unreferenced. Cleanup previews, then deletes on the real call; time
    // travel to an expired version fails cleanly.
    let out = run(&format!(
        "{attach}\
         CALL ducklake_expire_snapshots('lk', versions := [1]);\n\
         CALL ducklake_expire_snapshots('lk', versions := [2]);\n\
         CALL ducklake_expire_snapshots('lk', versions := [3]);\n\
         SELECT 'left=' || CAST(COUNT(*) AS VARCHAR) AS remaining \
           FROM ducklake_snapshots('lk') WHERE snapshot_id = 1 OR snapshot_id = 2 OR snapshot_id = 3;\n\
         CALL ducklake_cleanup_old_files('lk', cleanup_all := true, dry_run := true);\n\
         SELECT 'preview=' || CAST(COUNT(*) AS VARCHAR) AS preview \
           FROM ducklake_list_files('lk', 't');\n\
         CALL ducklake_cleanup_old_files('lk', cleanup_all := true);\n\
         SELECT 'files=' || CAST(COUNT(*) AS VARCHAR) AS files \
           FROM ducklake_list_files('lk', 't');\n\
         SELECT 'data_path=' || value AS data_path FROM ducklake_options('lk') \
          WHERE option_name = 'data_path';\n\
         SELECT 'catalog=' || catalog_type AS catalog FROM ducklake_settings('lk');\n\
         SELECT 'ext=' || extension_version AS ext FROM ducklake_settings('lk');\n\
         DETACH lk;\n\
         ATTACH 'lake:{lake}' AS old (READ_ONLY, VERSION 2);\n\
         SELECT COUNT(*) FROM old.main.t;\n\
         .quit\n",
        attach = attach_rw(&lake),
        lake = lake.display(),
    ));
    assert!(out.contains("left=0"), "{out}");
    assert!(out.contains("preview=1"), "{out}");
    assert!(out.contains("files=1"), "{out}");
    assert!(out.contains("data_path="), "{out}");
    assert!(out.contains("catalog="), "{out}");
    assert!(out.contains("ext="), "{out}");
    assert!(out.contains("has no snapshot version 2"), "{out}");
    assert!(!out.contains("Error: corrupt"), "{out}");
    // Only the consolidated head file remains.
    assert_eq!(std::fs::read_dir(&data_dir).unwrap().count(), 1);
    cleanup(&dir);
}

/// Wave 68: a small lake database commit inlines its rows into the catalog
/// (no data file), `CALL ducklake_flush_inlined_data` materializes
/// them into a data file, `ducklake_table_changes` renders the version range's
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

    // A one-row append inlines: no data file. The SQL functions flush it and
    // read the version range's changes.
    let out = run(&format!(
        "{attach}\
         INSERT INTO t VALUES (2, 'b');\n\
         SELECT COUNT(*) FROM t;\n\
         CALL ducklake_flush_inlined_data('lk');\n\
         SELECT 'files=' || CAST(COUNT(*) AS VARCHAR) AS files \
           FROM ducklake_list_files('lk', 't');\n\
         SELECT 'change=' || CAST(snapshot_id AS VARCHAR) || ':' || \
                CAST(rowid AS VARCHAR) || ':' || change_type || ':' || \
                CAST(i AS VARCHAR) || ':' || s AS change \
           FROM ducklake_table_changes('lk', 'main', 't', 1, 2);\n\
         USE main;\n\
         DETACH lk;\n\
         .lake inlining {lake} main.t 0\n\
         .lake inlining {lake} main.t default\n\
         .quit\n",
        lake = lake.display(),
        attach = attach_rw(&lake),
    ));
    assert!(out.contains("│ 2 "), "the inlined row is visible: {out}");
    assert!(out.contains("files=2"), "the flush materializes it: {out}");
    // The v2 save's row and the v3 inlined row, inclusive.
    assert!(out.contains("change=1:0:insert:1:a"), "{out}");
    assert!(out.contains("change=2:1:insert:2:b"), "{out}");
    assert!(
        out.contains("commits appending at most 0 rows are stored in the catalog metadata"),
        "{out}"
    );
    assert!(
        out.contains("Cleared the inlining limit of main.t"),
        "{out}"
    );
    // The flush published a real data file.
    assert_eq!(data_files(&data_root(&lake)).len(), 2, "save + flush");
    cleanup(&dir);
}

/// Wave 68: `ducklake_add_data_files` registers an existing in-lake data file
/// as rows of a table, `ducklake_list_files` names it, and the rows read back;
/// a path that escapes the lake is refused.
#[test]
fn add_data_files_registers_through_the_cli() {
    let dir = scratch("register");
    let lake = dir.join("lake");
    let values: Vec<String> = (0..12).map(|i| format!("({i})")).collect();
    let out = run(&format!(
        "CREATE TABLE t (a INTEGER);\n\
         INSERT INTO t VALUES {};\n\
         .lake save {lake}\n\
         .quit\n",
        values.join(", "),
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");

    let missing = data_root(&lake).join("main.t/nope.pink");
    let out = run(&format!(
        "{attach}\
         CALL ducklake_add_data_files('lk', 'main.t', '{}');\n\
         CALL ducklake_add_data_files('lk', 'main.t', '../escape.pink');\n\
         .quit\n",
        missing.display(),
        attach = attach_rw(&lake),
    ));
    assert!(out.contains("no files found that match"), "{out}");
    assert!(out.contains("is not under the lake's data path"), "{out}");

    // Register the saved file itself (its rows appear a second time).
    let data_dir = data_root(&lake).join("main.t");
    let path = std::fs::read_dir(&data_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let out = run(&format!(
        "{attach}\
         CALL ducklake_add_data_files('lk', 'main.t', '{}');\n\
         SELECT COUNT(*) AS row_count FROM t;\n\
         SELECT 'change=' || change_type || ':' || CAST(a AS VARCHAR) AS change \
           FROM ducklake_table_changes('lk', 'main', 't', 0, 2) WHERE rowid = 12;\n\
         .quit\n",
        path.display(),
        attach = attach_rw(&lake),
    ));
    assert!(out.contains("│ 24"), "{out}");
    assert!(out.contains("change=insert:0"), "{out}");
    cleanup(&dir);
}
#[test]
fn unavailable_tables_print_one_warning_on_open_and_attach() {
    let dir = scratch("unavailable_warn");
    let lake = dir.join("lake");
    let out = run(&format!(
        "CREATE TABLE t (i INTEGER);\n\
         INSERT INTO t VALUES (1), (2);\n\
         .lake save {lake}\n\
         .quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");

    // Give the one entry a format id no provider serves (as a newer writer's
    // extension id would).
    pink_lake::catalog_set_table_format(&catalog_url(&lake), "main.t", "nosuch").unwrap();

    // Open succeeds and warns that the table was skipped.
    let out = run(&format!(
        ".lake open {lake}\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("Opened lake version 2"), "{out}");
    assert!(
        out.contains(
            "warning: table main.t uses data-file format \"nosuch\", which this build cannot \
             read; it was skipped"
        ),
        "{out}"
    );

    // A read-only lake database lists it; the read fails naming the id.
    let out = run(&format!(
        "{attach}SELECT COUNT(*) FROM t;\n.quit\n",
        attach = attach_ro(&lake),
    ));
    assert!(out.contains("Database lk created."), "{out}");
    assert!(out.contains("nosuch"), "{out}");
    assert!(
        out.contains("no lake format provider is registered"),
        "{out}"
    );

    cleanup(&dir);
}

/// Wave 65: schema evolution through a writable lake database — `ALTER TABLE`
/// (ADD COLUMN with DEFAULT, RENAME COLUMN, DROP COLUMN) runs against the
/// materialized copy, the transaction's commit publishes one schema record,
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
    let files_before = data_files(&data_root(&lake)).len();
    assert_eq!(files_before, 1);

    let out = run_with_env(
        &format!(
            "{attach}\
         BEGIN;\n\
         ALTER TABLE t ADD COLUMN n INTEGER DEFAULT 42;\n\
         ALTER TABLE t ADD COLUMN tmp INTEGER;\n\
         UPDATE t SET n = 9 WHERE i = 2;\n\
         ALTER TABLE t RENAME COLUMN s TO label;\n\
         ALTER TABLE t DROP COLUMN tmp;\n\
         INSERT INTO t (i, n) VALUES (4, 7);\n\
         COMMIT;\n\
         SELECT i, n FROM t ORDER BY i;\n\
         .quit\n",
            attach = attach_rw(&lake),
        ),
        &[("PINKDB_LAKE_INLINING_ROW_LIMIT", "0")],
    );
    assert!(out.contains("Table t altered."), "{out}");
    assert!(!out.contains("Error:"), "{out}");
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
    assert_eq!(data_only(&data_root(&lake)), files_before + 1);
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
        "{attach}\
         SELECT i, s FROM t AT (VERSION => 2) ORDER BY i;\n\
         SELECT i, n FROM t ORDER BY i;\n\
         .quit\n",
        attach = attach_ro(&lake),
    ));
    assert!(out.contains("one"), "{out}");
    assert!(out.contains("three"), "{out}");
    assert!(out.contains("│ 42 "), "{out}");
    cleanup(&dir);
}

#[test]
fn attach_errors_are_clear_and_never_panic() {
    let dir = scratch("attach_err");

    // Not a lake at all.
    let out = run(&format!(
        "ATTACH 'lake:{missing}' AS lk (READ_ONLY);\n.quit\n",
        missing = dir.join("no-such-lake").display()
    ));
    assert!(out.contains("not a pink lake"), "{out}");

    // A missing data file.
    let missing_lake = dir.join("missing_lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1);\n\
         .lake save {}\n.quit\n",
        missing_lake.display()
    ));
    let data = data_files(&data_root(&missing_lake))
        .into_iter()
        .next()
        .expect("a data file");
    std::fs::remove_file(data).unwrap();
    // Attach itself is statistics-only and succeeds; the first read reports
    // the missing file.
    let (out, err, code) = run_captured(&format!(
        "{}SELECT SUM(a) FROM t;\n.quit\n",
        attach_ro(&missing_lake)
    ));
    assert_eq!(code, 1);
    assert!(
        err.contains("is missing") || err.contains("loading data file"),
        "{out}\n{err}"
    );

    // A truncated data file: a clean error, never a crash.
    let truncated = dir.join("truncated_lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1);\n\
         .lake save {}\n.quit\n",
        truncated.display()
    ));
    let data = data_files(&data_root(&truncated))
        .into_iter()
        .next()
        .expect("a data file");
    let bytes = std::fs::read(&data).unwrap();
    std::fs::write(&data, &bytes[..bytes.len() / 4]).unwrap();
    let (out, err, code) = run_captured(&format!(
        "{}SELECT SUM(a) FROM t;\n.quit\n",
        attach_ro(&truncated)
    ));
    assert_eq!(code, 1);
    assert!(err.contains("Error:"), "{out}\n{err}");

    // Corrupt row metadata (a negative record_count) is a clean error.
    let wrong_rows = dir.join("wrong_rows_lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1);\n\
         .lake save {}\n.quit\n",
        wrong_rows.display()
    ));
    catalog_sql(
        &wrong_rows,
        "UPDATE ducklake_data_file SET record_count = -1;",
    );
    let (out, err, code) = run_captured(&format!(
        "ATTACH 'lake:{}' AS lk (READ_ONLY);\n.quit\n",
        wrong_rows.display()
    ));
    assert_eq!(code, 1);
    assert!(
        err.contains("record_count") || err.contains("corrupt"),
        "{out}\n{err}"
    );

    // A two-file table attaches and unions both commits' rows.
    let two_files = dir.join("two_file_lake");
    run_with_env(
        &format!(
            "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (1);\n\
             .lake save {lake}\nDROP TABLE t;\n{attach}INSERT INTO t VALUES (2);\n.quit\n",
            lake = two_files.display(),
            attach = attach_rw(&two_files),
        ),
        &[("PINKDB_LAKE_INLINING_ROW_LIMIT", "0")],
    );
    let out = run(&format!(
        "{}SELECT SUM(a) FROM t;\n.quit\n",
        attach_ro(&two_files)
    ));
    assert!(!out.contains("Error:"), "{out}");
    assert!(out.contains("│ 3 "), "{out}");
    cleanup(&dir);
}

/// Triggers on a read-only lake database are refused; a writable one
/// publishes a trigger's effects with the statement that fired it, and keeps
/// the definition in its database image (which `.lake open`, a data-only
/// loader, does not read).
#[test]
fn lake_attachments_refuse_and_publish_trigger_effects() {
    let dir = scratch("trigger");
    let lake = dir.join("lake");
    run(&format!(
        "CREATE TABLE t (a INTEGER);\nCREATE TABLE log (msg VARCHAR);\n\
         INSERT INTO t VALUES (1);\n.lake save {lake}\n.quit\n",
        lake = lake.display()
    ));

    // Read-only: no trigger can be created in the lake database.
    let out = run(&format!(
        "{attach}\
         CREATE TRIGGER tr AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('x');\n\
         .quit\n",
        attach = attach_ro(&lake),
    ));
    assert!(out.contains("read-only lake"), "{out}");

    // Writable: the trigger fires, the effects are part of the commit.
    let out = run(&format!(
        "{attach}\
         CREATE TRIGGER tr AFTER INSERT ON t EXECUTE INSERT INTO log VALUES ('x');\n\
         INSERT INTO t VALUES (2);\n\
         .quit\n",
        attach = attach_rw(&lake),
    ));
    assert!(out.contains("Trigger tr created."), "{out}");
    assert!(!out.contains("Error:"), "{out}");

    // A reopened lake has both rows and no trigger definition.
    let out = run(&format!(
        ".lake open {lake}\nSELECT COUNT(*) FROM t;\nSELECT COUNT(*) FROM log;\n\
         SHOW TRIGGERS;\n.quit\n",
        lake = lake.display()
    ));
    assert!(out.contains("│ 2 "), "{out}");
    assert!(out.contains("│ 1 "), "{out}");
    assert!(
        !out.contains("tr:VARCHAR"),
        "a lake carries no triggers:\n{out}"
    );
    cleanup(&dir);
}
