//! CLI coverage for the `httpfs` feature (wave 75): `.import <url>`,
//! `read_csv('url')`, the `.csv` replacement scan, and the plain-http
//! opt-in. The transport races against an in-test `std::net::TcpListener`
//! server (the same one `pinkdb-httpfs`'s unit tests use), so nothing here
//! touches the network.
//!
//! `s3://` end to end is covered by pink-lake's mock-S3 tests (the shared
//! client) and `pinkdb-httpfs`'s unit tests; a local `.csv` replacement scan
//! is checked here without TLS.

#![cfg(feature = "httpfs")]

#[path = "../../pinkdb-httpfs/src/test_http.rs"]
mod test_http;

mod common;

use std::process::Command;

use test_http::TestHttpServer;

#[test]
fn import_read_csv_and_replacement_scan_over_http() {
    let server = TestHttpServer::start();
    server.put(
        "/data/sales.csv",
        b"id,region,total\n1,west,10.5\n2,east,7\n",
    );
    let url = server.url("/data/sales.csv");

    // A local `.csv` (the replacement scan's local half).
    let dir = std::env::temp_dir().join(format!("pinkdb_cli_httpfs_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let local = dir.join("local.csv");
    std::fs::write(&local, "a,b\n1,x\n2,y\n").unwrap();

    let script = format!(
        ".import {url}\n\
         SELECT COUNT(*), SUM(total) FROM sales;\n\
         SELECT COUNT(*) FROM read_csv('{url}');\n\
         SELECT region FROM read_csv('{url}') WHERE total > 8 ORDER BY region;\n\
         SELECT a, b FROM '{}' ORDER BY a;\n\
         SELECT region FROM '{url}';\n\
         .quit\n",
        local.display()
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    command.env("PINKDB_HTTPFS_ALLOW_HTTP", "1");
    let (text, status) = common::run_merged(command, &script);
    common::assert_script_exit(status);
    assert!(text.contains("Imported 2 rows"), "{text}");
    assert!(text.contains("2") && text.contains("17.5"), "{text}");
    assert!(text.contains("west"), "{text}");
    // The http:// replacement scan is deliberately unclaimed (claims are
    // `.csv` local paths plus https:// and s3:// only), so it errors.
    assert!(text.contains("Error:"), "{text}");
    assert!(
        text.contains("by path") && text.contains("do not serve it"),
        "the http replacement scan must be refused, not claimed:\n{text}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn plain_http_is_refused_without_the_opt_in() {
    let server = TestHttpServer::start();
    server.put("/data/sales.csv", b"id\n1\n");
    let url = server.url("/data/sales.csv");
    let script = format!(".import {url}\n.quit\n");
    let command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    let (text, status) = common::run_merged(command, &script);
    common::assert_script_exit(status);
    assert!(text.contains("Error:"), "{text}");
    assert!(text.contains("PINKDB_HTTPFS_ALLOW_HTTP"), "{text}");
    assert!(
        server.requests().is_empty(),
        "no request may go out: {:?}",
        server.requests()
    );
}

/// The format crates' `remote` halves compose with the CLI's `httpfs`
/// feature: remote parquet replacement scans/read_parquet and remote arrow
/// reads work in one binary. Gated per format so `--features httpfs` alone
/// still compiles and tests transport + CSV.
#[test]
#[cfg(any(feature = "parquet", feature = "arrow"))]
fn remote_format_sources_compose() {
    let server = TestHttpServer::start();

    #[cfg(feature = "parquet")]
    {
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../bench/parquet_fixtures/rg_stats.parquet");
        let bytes = std::fs::read(&fixture).expect("the committed rg_stats fixture");
        server.put("/rg.parquet", &bytes);
        let url = server.url("/rg.parquet");
        let script = format!(
            "SELECT COUNT(*), MIN(id), MAX(id) FROM read_parquet('{url}');\n\
             SELECT COUNT(*) FROM read_parquet('{url}') WHERE id >= 7000;\n\
             SELECT COUNT(*) FROM '{url}';\n\
             .quit\n"
        );
        let mut command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
        command.env("PINKDB_HTTPFS_ALLOW_HTTP", "1");
        let (text, status) = common::run_merged(command, &script);
        common::assert_script_exit(status);
        assert!(!text.contains("Error:"), "{text}");
        assert!(text.contains("8192") && text.contains("8191"), "{text}");
        assert!(text.contains("1192"), "{text}");
    }

    #[cfg(feature = "arrow")]
    {
        let dir =
            std::env::temp_dir().join(format!("pinkdb_cli_httpfs_arrow_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let local = dir.join("probe.arrow");
        let create = format!(
            "CREATE TABLE t (i INTEGER, s VARCHAR);\n\
             INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, 'c');\n\
             COPY t TO '{}' (FORMAT arrow);\n\
             .quit\n",
            local.display()
        );
        let (text, status) =
            common::run_merged(Command::new(env!("CARGO_BIN_EXE_pinkdb")), &create);
        common::assert_script_exit(status);
        assert!(!text.contains("Error:"), "{text}");
        let bytes = std::fs::read(&local).unwrap();
        server.put("/probe.arrow", &bytes);
        let url = server.url("/probe.arrow");
        let script = format!(
            "SELECT COUNT(*), SUM(i), COUNT(s) FROM read_arrow('{url}');\n\
             SELECT i FROM '{url}' ORDER BY i;\n\
             .quit\n"
        );
        let mut command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
        command.env("PINKDB_HTTPFS_ALLOW_HTTP", "1");
        let (text, status) = common::run_merged(command, &script);
        common::assert_script_exit(status);
        assert!(!text.contains("Error:"), "{text}");
        assert!(text.contains('3'), "{text}");
        std::fs::remove_dir_all(&dir).ok();
    }
}
