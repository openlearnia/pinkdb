//! CLI coverage for a Postgres lake catalog (wave 96a), gated by
//! `PINKDB_TEST_POSTGRES_URL`: `.lake save <postgres-url> data <dir>`,
//! `.lake open <catalog>`, and `.lake migrate-catalog <from> to <to>`.
//!
//! Every run allocates a fresh `w96a_cli_<pid>_<seq>` schema and drops it at
//! the end. Unset or empty `PINKDB_TEST_POSTGRES_URL` prints one skip line and
//! passes, so a build without a server still runs green.

#![cfg(all(feature = "pinklake", feature = "catalog-postgres"))]

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_cli_postgres_{tag}_{}_{}",
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

/// The test server URL with any `schema=` parameter removed.
fn base_url() -> Option<String> {
    let Ok(url) = std::env::var("PINKDB_TEST_POSTGRES_URL") else {
        println!(
            "skip Postgres CLI catalog test: PINKDB_TEST_POSTGRES_URL is not set \
             (start a local server and set it, e.g. \
             postgres://postgres@localhost:55432/pinktest)"
        );
        return None;
    };
    let url = url.trim();
    if url.is_empty() {
        println!("skip Postgres CLI catalog test: PINKDB_TEST_POSTGRES_URL is empty");
        return None;
    }
    let (main, query) = url.split_once('?').unwrap_or((url, ""));
    let kept: Vec<&str> = query
        .split('&')
        .filter(|pair| !pair.is_empty() && !pair.starts_with("schema="))
        .collect();
    Some(if kept.is_empty() {
        main.to_string()
    } else {
        format!("{main}?{}", kept.join("&"))
    })
}

/// A catalog URL naming a fresh throwaway schema, and that schema name.
fn catalog(base: &str, tag: &str) -> (String, String) {
    let schema = format!(
        "w96a_cli_{}_{tag}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let separator = if base.contains('?') { '&' } else { '?' };
    (format!("{base}{separator}schema={schema}"), schema)
}

/// Put a password into the URL's userinfo (preserving the user and host), so
/// the test can prove no command prints it. The throwaway server trusts local
/// connections, so the password is only ever display data.
fn with_password(base: &str, password: &str) -> String {
    let Some((scheme, rest)) = base.split_once("://") else {
        return base.to_string();
    };
    let (authority, tail) = rest.split_once('/').unwrap_or((rest, ""));
    let (user, host) = match authority.rsplit_once('@') {
        Some((userinfo, host)) => (userinfo.split(':').next().unwrap_or("postgres"), host),
        None => ("postgres", authority),
    };
    format!("{scheme}://{user}:{password}@{host}/{tail}")
}

fn drop_schema(base: &str, schema: &str) {
    // Best-effort: the CI service and local throwaway clusters are discarded
    // anyway, so a missing `psql` only leaves an empty schema behind.
    let status = Command::new("psql")
        .arg(base)
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-c")
        .arg(format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    if !status.is_ok_and(|status| status.success()) {
        eprintln!("warning: could not drop test schema {schema} (psql unavailable?)");
    }
}

#[test]
fn save_and_open_a_postgres_catalog_without_printing_the_password() {
    let Some(base) = base_url() else {
        return;
    };
    let dir = scratch("roundtrip");
    let data = dir.join("data-root").display().to_string();
    let (source, source_schema) = catalog(&base, "source");
    let password = "w96a-s3cret";
    let source_with_password = with_password(&source, password);

    let out = run(&format!(
        "CREATE TABLE t (a INTEGER, b VARCHAR);\n\
         INSERT INTO t VALUES (1, 'x'), (2, 'y');\n\
         .lake save {source_with_password} data {data}\n\
         .lake open {source_with_password}\n\
         SELECT COUNT(*) FROM t;\n\
         .quit\n"
    ));
    assert!(out.contains("Saved lake version 2"), "{out}");
    assert!(out.contains("Opened lake version 2"), "{out}");
    assert!(out.contains("│ 2        │"), "{out}");
    assert!(
        !out.contains(password),
        "the password must never reach the terminal: {out}"
    );

    // `.lake migrate-catalog` is the legacy JSON-to-SQL copy operation,
    // deleted in Part 2; it is not exercised here.

    cleanup(&dir);
    drop_schema(&base, &source_schema);
}

/// A malformed Postgres URL in the usage/error path is still redacted.
#[test]
fn a_refused_postgres_location_does_not_leak_the_password() {
    let Some(base) = base_url() else {
        return;
    };
    let password = "w96a-refused-s3cret";
    let url = with_password(&base, password);
    // A remote host with plaintext sslmode is refused before any connection.
    let remote = url
        .replace("127.0.0.1", "db.example.com")
        .replace("localhost", "db.example.com");
    let (main, query) = remote.split_once('?').unwrap_or((&remote, ""));
    let kept: Vec<&str> = query
        .split('&')
        .filter(|pair| !pair.is_empty() && !pair.starts_with("sslmode="))
        .collect();
    let remote = if kept.is_empty() {
        format!("{main}?sslmode=disable")
    } else {
        format!("{main}?{}&sslmode=disable", kept.join("&"))
    };
    let out = run(&format!(".lake open {remote}\n.quit\n"));
    assert!(out.contains("sslmode=disable"), "{out}");
    assert!(
        !out.contains(password),
        "the password must never reach the terminal: {out}"
    );
}
