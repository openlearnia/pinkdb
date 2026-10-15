//! CLI coverage for the opt-in `lake` feature: `.lake save/open/versions/
//! tables/info` end to end, in a temp lake directory.
//!
//! This file only compiles when the feature is ON; the feature-less behavior
//! is covered by `tests/lake_disabled.rs`.
//!
//! Set `PINKDB_CLI_BIN` to run the whole file against another CLI build
//! (e.g. a release binary) — the overflow tests then prove release behavior.

#![cfg(feature = "lake")]

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
    assert!(catalog.contains("\"format_version\": 3"), "{catalog}");
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
    assert!(out.contains("format version:  3"), "{out}");
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
    let out =
        run(".lake save\n.lake open\n.lake versions\n.lake tables\n.lake info\n.lake\n.quit\n");
    assert!(out.contains("Usage: .lake save <dir>"), "{out}");
    assert!(out.contains("Usage: .lake open <dir> [version]"), "{out}");
    assert!(out.contains("Usage: .lake versions <dir>"), "{out}");
    assert!(out.contains("Usage: .lake tables <dir>"), "{out}");
    assert!(out.contains("Usage: .lake info <dir>"), "{out}");
    assert!(
        out.contains("Usage: .lake <save|open|attach|detach|attached|versions|tables|info> ..."),
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

    // With encryption, the session key survives the store replacement: the
    // later `.save` must be an encrypted container, and the plaintext note
    // must say the lake itself is not.
    #[cfg(feature = "encryption")]
    {
        let key = [("PINKDB_KEY", "lake-review-key")];
        run_with_env(
            &format!(
                "CREATE TABLE t (a INTEGER);\nINSERT INTO t VALUES (7);\n.lake save {lake}\n.quit\n",
                lake = lake.display()
            ),
            &key,
        );
        let out = run_with_env(
            &format!(
                ".lake open {lake}\nSELECT a FROM t;\n.save {out}\n.quit\n",
                lake = lake.display(),
                out = dir.join("out.pink").display()
            ),
            &key,
        );
        assert!(
            out.contains(
                "Note: lake data files are plaintext; the session key applies to \
                 .save/.open, not to the lake."
            ),
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
        assert_eq!(winners, (3..=version).collect::<Vec<_>>(), "round {round}");

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
        out.contains("cargo build -p pinkdb-cli --features lake,s3"),
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
            "decodes: 0 loads, 0 columns", // attach reads footers only
            "decodes: 0 loads, 0 columns", // every aggregate came from statistics
            "decodes: 1 load, 1 column",   // ORDER BY b: b only
            "decodes: 2 loads, 2 columns", // + c
            "decodes: 3 loads, 3 columns", // + a
            "decodes: 3 loads, 3 columns", // SELECT *: everything was cached
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
        err.contains("format_version 1") && err.contains("use .lake open"),
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
        err.contains("has 1 rows, but the catalog says 2"),
        "{out}\n{err}"
    );

    // A table with two data files is refused (this build attaches one file
    // per table; use `.lake open`).
    let two_files = dir.join("two_file_lake");
    copy_dir(&lake, &two_files);
    let data = std::fs::read_dir(two_files.join("data/main.t"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let second = data.with_file_name("second.vortex");
    std::fs::copy(&data, &second).unwrap();
    let first_rel = format!(
        "data/main.t/{}",
        data.file_name().unwrap().to_string_lossy()
    );
    let second_rel = format!(
        "data/main.t/{}",
        second.file_name().unwrap().to_string_lossy()
    );
    for name in ["catalog.json", "snapshots/2.json"] {
        let path = two_files.join(name);
        let text = std::fs::read_to_string(&path).unwrap();
        let needle = format!("\"{first_rel}\"");
        let patched = text.replace(&needle, &format!("\"{first_rel}\", \"{second_rel}\""));
        assert_ne!(text, patched, "file list not found in {name}");
        std::fs::write(&path, patched).unwrap();
    }
    let (out, err, code) = run_captured(&format!(".lake attach {}\n.quit\n", two_files.display()));
    assert_eq!(code, 1);
    assert!(err.contains("data files"), "{out}\n{err}");
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
    assert_eq!(entries, vec!["catalog.json", "data", "snapshots"], "{out}");
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
    // the materializing path on both arms.
    assert!(base.contains("-0.0") && base.contains("inf"), "{base}");
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
/// `crates/pink-lake/tests/lake_lite.rs`.
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
    assert!(out.contains("format version:  1"), "{out}");
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
    assert!(catalog.contains("\"format_version\": 3"), "{catalog}");
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
/// `tests/lake_lite.rs`).
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
