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
    assert!(catalog.contains("\"format_version\": 2"), "{catalog}");
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
    assert!(out.contains("format version:  2"), "{out}");
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
    assert!(out.contains("main.t (2 rows)"), "current tables take the latest snapshot: {out}");

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
    let out = run(".lake save\n.lake open\n.lake versions\n.lake tables\n.lake info\n.lake\n.quit\n");
    assert!(out.contains("Usage: .lake save <dir>"), "{out}");
    assert!(out.contains("Usage: .lake open <dir> [version]"), "{out}");
    assert!(out.contains("Usage: .lake versions <dir>"), "{out}");
    assert!(out.contains("Usage: .lake tables <dir>"), "{out}");
    assert!(out.contains("Usage: .lake info <dir>"), "{out}");
    assert!(out.contains("Usage: .lake <save|open|versions|tables|info> ..."), "{out}");

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
        assert!(!out.contains("Saved lake version"), "version {bad} wrapped: {out}");
    }

    // Restoring the catalog heals the lake (nothing was written).
    std::fs::write(lake.join("catalog.json"), &catalog).unwrap();
    let out = run(&format!(".lake info {lake}\n.quit\n", lake = lake.display()));
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
    let out = run(&format!(".lake open {lake}\n.quit\n", lake = lake.display()));
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
        assert_eq!(winners, unique, "round {round}: duplicate published versions");

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

        let out = run(&format!(".lake info {lake}\n.quit\n", lake = lake.display()));
        assert!(!out.contains("Error:"), "{out}");
        assert!(out.contains(&format!("current version: {version}")), "{out}");

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
    let out = run(
        ".lake save s3://bucket/prefix\n\
         .lake open s3://bucket/prefix\n\
         .lake versions s3://bucket/prefix\n\
         .lake tables s3://bucket/prefix\n\
         .lake info s3://bucket/prefix\n\
         .quit\n",
    );
    assert!(
        out.contains("does not include the `s3` feature"),
        "{out}"
    );
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
        assert!(a_out.read_line(&mut line).unwrap() > 0, "session A exited early: {seen}");
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
    assert!(rest.contains("Saved lake version"), "forced save publishes: {rest}");

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
    assert_eq!(out.matches("not allowed inside a transaction").count(), 2, "{out}");
    assert!(out.contains("Saved lake version"), "{out}");
    let out = run(&format!(
        ".open {db}\n.lake open {lake}\n.quit\n",
        db = dir.join("x.pink").display(),
        lake = lake.display()
    ));
    assert!(out.contains("would detach this session"), "{out}");
    cleanup(&dir);
}
