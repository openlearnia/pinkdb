//! Shared by the console storage tests (wave 121b): a small HTTP client for
//! the console and the lake round trip every backend runs.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pinkdb_server::ui::{UiAuth, UiOptions, UiServer, storage_info};
use pinkdb_server::{DataDir, lake_policy};
use serde_json::{Value, json};

/// A fresh directory under the system temp dir.
pub fn tempdir(tag: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinkdb-121b-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// `(status, body)` of one request.
pub fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, String, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    let mut text = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\n");
    for (k, v) in headers {
        text.push_str(&format!("{k}: {v}\r\n"));
    }
    text.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    ));
    let mut bytes = text.into_bytes();
    bytes.extend_from_slice(body);
    stream.write_all(&bytes).unwrap();
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    let raw = String::from_utf8_lossy(&buf).into_owned();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status, head.to_string(), body.to_string())
}

/// A logged-in console session.
pub struct Session {
    pub addr: SocketAddr,
    cookie: String,
    csrf: String,
}

impl Session {
    /// Log in to a console without authentication.
    pub fn open(addr: SocketAddr) -> Session {
        let (status, head, body) = request(addr, "POST", "/api/login", &[], b"{}");
        assert_eq!(status, 200, "{body}");
        let cookie = head
            .lines()
            .find_map(|l| l.strip_prefix("set-cookie: "))
            .and_then(|c| c.split(';').next())
            .unwrap()
            .to_string();
        let csrf = serde_json::from_str::<Value>(&body).unwrap()["csrf"]
            .as_str()
            .unwrap()
            .to_string();
        Session { addr, cookie, csrf }
    }

    pub fn get(&self, path: &str) -> Value {
        let (status, _, body) = request(self.addr, "GET", path, &[("Cookie", &self.cookie)], b"");
        assert_eq!(status, 200, "{body}");
        serde_json::from_str(&body).unwrap()
    }

    fn post(&self, path: &str, content_type: &str, body: &[u8]) -> (u16, String) {
        let (status, _, body) = request(
            self.addr,
            "POST",
            path,
            &[
                ("Cookie", &self.cookie),
                ("X-CSRF-Token", &self.csrf),
                ("Content-Type", content_type),
            ],
            body,
        );
        (status, body)
    }

    /// Run SQL that must succeed; the last result's first cell.
    pub fn scalar(&self, sql: &str) -> String {
        static RUN: AtomicU64 = AtomicU64::new(0);
        let request =
            json!({ "sql": sql, "run_id": format!("r{}", RUN.fetch_add(1, Ordering::Relaxed)) });
        let (status, body) = self.post(
            "/api/query",
            "application/json",
            request.to_string().as_bytes(),
        );
        assert_eq!(status, 200, "{body}");
        let out: Value = serde_json::from_str(&body).unwrap();
        assert!(out["error"].is_null(), "{sql}: {out}");
        let last = out["results"].as_array().unwrap().last().unwrap().clone();
        last["rows"][0][0].as_str().unwrap_or_default().to_string()
    }

    /// Upload a CSV file into `table`.
    pub fn upload_csv(&self, table: &str, csv: &str) -> Value {
        self.upload_csv_into("main", table, csv)
    }

    /// Upload a CSV file into `db.table`.
    pub fn upload_csv_into(&self, db: &str, table: &str, csv: &str) -> Value {
        let (status, body) = self.post(
            &format!("/api/upload?db={db}&table={table}&name={table}.csv"),
            "application/octet-stream",
            csv.as_bytes(),
        );
        assert_eq!(status, 200, "{body}");
        serde_json::from_str(&body).unwrap()
    }
}

/// The console over a lake database (wave 124): worksheet DDL/DML and an
/// upload into a lake database publish to its lake; a reopen of the data
/// directory sees them; the catalog names the storage and hides secrets.
pub fn lake_round_trip(catalog: &str, data: &str, backend: &str) {
    pink_lake::database::register();
    let base = tempdir("console-data");
    let dir = DataDir::prepare(&base).unwrap();
    let open = || {
        let opts = pinkdb::DurableOpenOptions {
            lake_dir: Some(dir.lakes()),
            ..pinkdb::DurableOpenOptions::default()
        };
        let db = pinkdb::Database::open_with_options(&dir.store().to_string_lossy(), opts)
            .unwrap_or_else(|e| panic!("open the data directory: {e}"));
        // The test's catalogs live in its own temporary directories.
        db.set_lake_policy(Some(lake_policy(
            Some(dir.lakes()),
            vec![std::env::temp_dir().canonicalize().unwrap()],
        )));
        db
    };
    let db = open();
    let server = UiServer::bind(
        "127.0.0.1:0",
        db.clone(),
        // These are storage tests: no sign-in (the default since wave 121d
        // asks for one).
        UiOptions {
            storage: storage_info(Some(&dir), &[], vec!["upload:csv".to_string()]),
            auth: UiAuth::None,
            scratch_dir: Some(dir.tmp()),
            ..UiOptions::default()
        },
    )
    .unwrap();
    let addr = server.local_addr();
    let handle = server.spawn().unwrap();
    let session = Session::open(addr);

    session.scalar(&format!(
        "CREATE DATABASE lk (TYPE LAKE, CATALOG '{catalog}', DATA '{data}'); SELECT 1"
    ));
    let version = |session: &Session| -> i64 {
        session
            .scalar("SELECT version FROM pinkdb_catalog() WHERE kind = 'DATABASE' AND name = 'lk'")
            .parse()
            .unwrap()
    };
    let before = version(&session);
    let rows: Vec<String> = (1..=40).map(|i| format!("({i}, 'row-{i}')")).collect();
    session.scalar(&format!(
        "CREATE TABLE lk.items (id INTEGER, name VARCHAR); \
         INSERT INTO lk.items VALUES {}; SELECT 1",
        rows.join(", ")
    ));
    let upload = session.upload_csv_into("lk", "sales", "region,total\nnorth,10\nsouth,7.5\n");
    assert_eq!(upload["rows"], 2);
    assert_eq!(session.scalar("SELECT count(*) FROM lk.items"), "40");
    assert!(version(&session) > before, "the writes published");
    assert_eq!(
        session.scalar(
            "SELECT storage || ' ' || status FROM pinkdb_catalog() \
             WHERE kind = 'DATABASE' AND name = 'lk'"
        ),
        "lake ok"
    );
    let location = session
        .scalar("SELECT location FROM pinkdb_catalog() WHERE kind = 'DATABASE' AND name = 'lk'");
    assert!(
        location.contains(backend) || backend.is_empty(),
        "{location}"
    );
    assert!(!location.contains("password"), "{location}");
    let storage = session.get("/api/storage");
    assert_eq!(storage["mode"], "durable", "{storage}");
    // The console's catalog names the lake database's storage (wave 124c).
    let catalog = session.get("/api/catalog");
    let lk = catalog["databases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["name"] == "lk")
        .unwrap_or_else(|| panic!("{catalog}"))
        .clone();
    assert_eq!(lk["storage"], "lake", "{lk}");
    assert_eq!(lk["status"], "ok", "{lk}");
    assert_eq!(lk["read_only"], false, "{lk}");
    assert!(lk["version"].as_i64().is_some_and(|v| v > before), "{lk}");
    // Nothing the console staged stays behind.
    assert_eq!(std::fs::read_dir(dir.tmp()).unwrap().count(), 0);

    handle.join();
    db.close_lake_databases();
    drop(db);

    // The data directory reopened: the lake database comes back with it.
    let reopened = open();
    let mut conn = reopened.connect();
    let out = conn
        .execute("SELECT (SELECT count(*) FROM lk.items), (SELECT sum(total) FROM lk.sales)")
        .unwrap();
    let text = format!("{out:?}");
    assert!(
        text.contains("Bigint(40)") && text.contains("17.5"),
        "{text}"
    );
    reopened.close_lake_databases();
    let _ = std::fs::remove_dir_all(&base);
}
