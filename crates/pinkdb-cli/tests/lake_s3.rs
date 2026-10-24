//! CLI coverage for the opt-in `s3` feature: `.lake save/open/versions/
//! tables/info` against a hermetic mock S3 server (no cloud, no credentials),
//! driven as child processes so the `AWS_*` environment is exactly what each
//! test sets.
//!
//! This file only compiles when `pinklake` **and** `s3` are on.

#![cfg(all(feature = "pinklake", feature = "s3"))]

#[path = "../../pink-lake/src/mock_s3.rs"]
mod mock_s3;

mod common;

use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// The CLI under test: `PINKDB_CLI_BIN` wins over the test-profile binary.
fn cli_binary() -> String {
    std::env::var("PINKDB_CLI_BIN").unwrap_or_else(|_| env!("CARGO_BIN_EXE_pinkdb").to_string())
}

/// Run the REPL with exactly `env` added and the AWS variables cleared first,
/// so a developer's real credentials can never leak into the mock.
fn run_with_env(cmds: &str, env: &[(&str, &str)]) -> String {
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
    for (key, value) in env {
        command.env(key, value);
    }
    let (out, status) = common::run_merged(command, cmds);
    common::assert_script_exit(status);
    out
}

/// The mock environment for one run.
fn mock_env(mock: &mock_s3::MockS3) -> Vec<(&'static str, String)> {
    vec![
        ("AWS_ACCESS_KEY_ID", mock.access_key_id()),
        ("AWS_SECRET_ACCESS_KEY", "mock-secret".to_string()),
        ("AWS_SESSION_TOKEN", "mock-session-token".to_string()),
        ("AWS_ENDPOINT_URL", mock.endpoint()),
        ("AWS_ENDPOINT_URL_S3", mock.endpoint()),
        ("AWS_REGION", "auto".to_string()),
        // The mock is a local plain-http test server.
        ("PINKDB_S3_ALLOW_HTTP", "1".to_string()),
    ]
}

/// Borrow the owned env strings for `run_with_env`.
fn run_mock(mock: &mock_s3::MockS3, cmds: &str) -> String {
    let owned = mock_env(mock);
    let borrowed: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
    run_with_env(cmds, &borrowed)
}

/// A fresh local `pink:` catalog for S3 data.
fn catalog(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_lake_s3_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    format!("pink:{}", dir.join("catalog.pink").display())
}

#[test]
fn lake_s3_save_open_versions_tables_info() {
    let mock = mock_s3::MockS3::start();
    let url = mock.url("cli-bucket", "lakes/cli");
    let catalog = catalog("roundtrip");

    let out = run_mock(
        &mock,
        &format!(
            "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
             INSERT INTO t VALUES (1, 'x'), (2, NULL);\n\
             CREATE DATABASE analytics;\n\
             CREATE TABLE analytics.u (k BIGINT);\n\
             INSERT INTO analytics.u VALUES (7);\n\
             .lake save {catalog} data {url}\n\
             .quit\n"
        ),
    );
    assert!(out.contains("Saved lake version 2"), "{out}");
    assert!(out.contains("2 tables, 3 rows"), "{out}");
    assert!(
        out.contains("lake: s3://cli-bucket/lakes/cli"),
        "the lake URL is echoed: {out}"
    );
    let keys = mock.keys();
    assert!(
        !keys.iter().any(|k| k.ends_with("catalog.json")),
        "metadata is in the catalog database, not object storage: {keys:?}"
    );
    assert_eq!(
        keys.iter().filter(|k| k.contains("/data/")).count(),
        2,
        "{keys:?}"
    );

    // info/versions/tables read the same lake back; the metadata commands
    // never touch the store.
    let out = run_mock(
        &mock,
        &format!(".lake info {catalog}\n.lake versions {catalog}\n.lake tables {catalog}\n.quit\n"),
    );
    assert!(out.contains("format version:  7"), "{out}");
    assert!(out.contains("current version: 2"), "{out}");
    assert!(out.contains("snapshots:       2 (1, 2)"), "{out}");
    assert!(out.contains("main.t (2 rows)"), "{out}");
    assert!(out.contains("analytics.u (1 row)"), "{out}");
    assert!(out.contains("2 (current)"), "{out}");
    assert!(!out.contains("warning:"), "{out}");

    // open loads the latest and queries it; a second save appends.
    let out = run_mock(
        &mock,
        &format!(
            ".lake open {catalog}\nSELECT a, b FROM t ORDER BY a;\nINSERT INTO analytics.u VALUES (8);\n\
             .lake save {catalog} data {url}\n.quit\n"
        ),
    );
    assert!(
        out.contains("Opened lake version 2 in") && out.contains("(2 tables) from pink:"),
        "{out}"
    );
    assert!(out.contains("Saved lake version 3"), "{out}");
    let out = run_mock(&mock, &format!(".lake versions {catalog}\n.quit\n"));
    assert!(out.contains("1\n2\n3 (current)"), "{out}");

    // The whole conversation was SigV4-signed with the R2 `auto` region and
    // the session token, path-style against the endpoint.
    let auths = mock.authorizations();
    assert!(!auths.is_empty());
    for auth in &auths {
        assert!(
            auth.contains(&format!("Credential={}/", mock.access_key_id())),
            "{auth}"
        );
        assert!(auth.contains("/auto/s3/aws4_request"), "{auth}");
    }
    assert!(
        mock.requests()
            .iter()
            .any(|r| r.starts_with("PUT /cli-bucket/lakes/cli/")),
        "{:?}",
        mock.requests()
    );
}

#[test]
fn lake_s3_errors_are_clear() {
    let mock = mock_s3::MockS3::start();

    // An S3 URL alone is data, not a lake: the error names the fix before
    // any request is made.
    let out = run_with_env(
        ".lake save s3://bucket/prefix\n.quit\n",
        &[("AWS_ENDPOINT_URL", &mock.endpoint())],
    );
    assert!(out.contains("data location, not a lake catalog"), "{out}");
    assert!(out.contains("data s3://bucket/prefix"), "{out}");
    assert!(mock.requests().is_empty(), "{:?}", mock.requests());

    // A malformed URL never touches the network.
    let before = mock.requests().len();
    let out = run_mock(&mock, ".lake save s3://\n.quit\n");
    assert!(out.contains("invalid lake location"), "{out}");
    assert_eq!(mock.requests().len(), before, "{:?}", mock.requests());
}
#[test]
fn concurrent_lake_saves_over_s3_stay_consistent() {
    let mock = mock_s3::MockS3::start();
    let url = mock.url("race-bucket", "lakes/race");
    let catalog = catalog("race");
    let scratch = std::env::temp_dir().join(format!(
        "pinkdb_cli_lake_s3_race_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&scratch).ok();

    let mut next_version = 2u64; // the create-time v1 + the first warm-up
    for round in 0..3 {
        // Warm-up publishes the next version, so all writers race on the same
        // chain.
        let out = run_mock(
            &mock,
            &format!("CREATE TABLE warm (x INTEGER);\n.lake save {catalog} data {url}\n.quit\n"),
        );
        assert!(
            out.contains(&format!("Saved lake version {next_version}")),
            "round {round}: {out}"
        );
        next_version += 1;

        let outs: Vec<String> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..3)
                .map(|i| {
                    let url = url.clone();
                    let catalog = catalog.clone();
                    let mock = &mock;
                    scope.spawn(move || {
                        run_mock(
                            mock,
                            &format!(
                                "CREATE TABLE t{i} (a INTEGER);\nINSERT INTO t{i} VALUES (1), (2);\n\
                                 .lake save {catalog} data {url}\n.quit\n"
                            ),
                        )
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("writer thread"))
                .collect()
        });

        // Every writer either published or lost with the documented conflict;
        // no panic, no I/O noise.
        let mut winners: Vec<u64> = Vec::new();
        for out in &outs {
            assert!(!out.contains("panicked"), "{out}");
            assert!(!out.contains("No such file or directory"), "{out}");
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

        let version = next_version - 1 + winners.len() as u64;
        assert_eq!(
            winners,
            (next_version..=version).collect::<Vec<_>>(),
            "round {round}: version chain has a gap"
        );
        next_version = version + 1;

        // The lake loads through the CLI (metadata + the store).
        let out = run_mock(&mock, &format!(".lake info {catalog}\n.quit\n"));
        assert!(!out.contains("Error:"), "{out}");
        assert!(
            out.contains(&format!("current version: {version}")),
            "{out}"
        );
    }
    std::fs::remove_dir_all(&scratch).ok();
}

/// `.lake attach` over the mock S3: allowed, footer-only in the decode sense
/// (whole objects, as the note says), with working statistics fast paths and
/// lazy materialization.
#[test]
fn lake_s3_attach_reports_and_queries() {
    let mock = mock_s3::MockS3::start();
    let url = mock.url("cli-bucket", "lakes/attach");
    let catalog = catalog("attach");

    let out = run_mock(
        &mock,
        &format!(
            "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
             INSERT INTO t VALUES (1, 'x'), (2, NULL);\n\
             .lake save {catalog} data {url}\n\
             .quit\n"
        ),
    );
    assert!(out.contains("Saved lake version 2"), "{out}");

    let out = run_mock(
        &mock,
        &format!(
            ".lake attach {catalog}\n\
             .lake attached\n\
             SELECT COUNT(*), COUNT(b), MIN(a), MAX(b) FROM t;\n\
             SELECT SUM(a) FROM t;\n\
             .lake detach {catalog}\n\
             .quit\n"
        ),
    );
    assert!(!out.contains("footer-only"), "{out}");
    assert!(
        out.contains("statistics-only") && out.contains("decodes: 0 loads"),
        "{out}"
    );
    assert!(
        !out.contains("Transport:"),
        "attach reads the catalog, not a data object:\n{out}"
    );
    assert!(out.contains("main.t (2 rows)"), "{out}");
    assert!(out.contains("Detached"), "{out}");
    assert!(!out.contains("Error:"), "{out}");
}

/// A plain-http endpoint still needs the explicit test opt-in.
#[test]
fn plain_http_and_degraded_endpoints_need_explicit_cli_opt_ins() {
    let mock = mock_s3::MockS3::start();
    let endpoint = mock.endpoint();
    let access_key = mock.access_key_id();
    let catalog = catalog("plain-http");
    let out = run_with_env(
        &format!(".lake save {catalog} data s3://bucket/prefix\n.quit\n"),
        &[
            ("AWS_ACCESS_KEY_ID", access_key.as_str()),
            ("AWS_SECRET_ACCESS_KEY", "mock-secret"),
            ("AWS_ENDPOINT_URL", endpoint.as_str()),
            ("AWS_REGION", "auto"),
        ],
    );
    assert!(out.contains("Error: "), "{out}");
    assert!(out.contains("unencrypted"), "{out}");
    assert!(out.contains("PINKDB_S3_ALLOW_HTTP"), "{out}");
    assert!(mock.requests().is_empty(), "{:?}", mock.requests());
}
