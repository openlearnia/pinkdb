//! Shared by the console storage tests (wave 121b): a small HTTP client for
//! the console and the lake round trip every backend runs.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pinkdb_server::CatalogStore;
use pinkdb_server::ui::{UiAuth, UiOptions, UiServer, storage_info};
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
        let (status, body) = self.post(
            &format!("/api/upload?table={table}&name={table}.csv"),
            "application/octet-stream",
            csv.as_bytes(),
        );
        assert_eq!(status, 200, "{body}");
        serde_json::from_str(&body).unwrap()
    }
}

/// The console over a lake catalog: worksheet DDL/DML and an upload publish
/// to the catalog; a reopen under a new lease sees them; the Storage panel
/// names the backend and hides secrets.
pub fn lake_round_trip(catalog: &str, data: &str, backend: &str) {
    let store = CatalogStore::open(catalog, data, "w121b console first")
        .unwrap_or_else(|e| panic!("open {catalog}: {e}"));
    let storage = storage_info(
        None,
        Some(catalog),
        Some(data),
        Some(store.version_reader()),
        vec!["upload:csv".to_string()],
    );
    let server = UiServer::bind(
        "127.0.0.1:0",
        store.database(),
        // These are storage tests: no sign-in (the default since wave 121d
        // asks for one).
        UiOptions {
            storage,
            auth: UiAuth::None,
            ..UiOptions::default()
        },
    )
    .unwrap();
    let addr = server.local_addr();
    let handle = server.spawn().unwrap();
    let session = Session::open(addr);

    let before: u64 = session.get("/api/storage")["facts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["label"] == "lake version")
        .and_then(|f| f["value"].as_str()?.parse().ok())
        .unwrap_or(0);
    let rows: Vec<String> = (1..=40).map(|i| format!("({i}, 'row-{i}')")).collect();
    session.scalar(&format!(
        "CREATE TABLE items (id INTEGER, name VARCHAR); \
         INSERT INTO items VALUES {}; \
         CREATE VIEW late AS SELECT id FROM items WHERE id > 35; SELECT 1",
        rows.join(", ")
    ));
    let upload = session.upload_csv("sales", "region,total\nnorth,10\nsouth,7.5\n");
    assert_eq!(upload["rows"], 2);
    assert_eq!(session.scalar("SELECT count(*) FROM items"), "40");

    let storage = session.get("/api/storage");
    assert_eq!(storage["mode"], "lake", "{storage}");
    let facts = storage["facts"].to_string();
    assert!(facts.contains(backend), "{facts}");
    let after: u64 = storage["facts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["label"] == "lake version")
        .and_then(|f| f["value"].as_str()?.parse().ok())
        .unwrap();
    assert!(after > before, "the writes published: {before} → {after}");
    // No password of the catalog location reaches the browser: a URL's
    // `user:password@` shows as `user:***@`.
    if let Some((scheme, rest)) = catalog.split_once("://")
        && let Some((userinfo, host)) = rest.split_once('@')
        && let Some((user, password)) = userinfo.split_once(':')
        && !password.is_empty()
    {
        let location = storage["location"].as_str().unwrap();
        assert!(
            location.starts_with(&format!(
                "{scheme}://{user}:***@{}",
                host.split('/').next().unwrap()
            )),
            "{location}"
        );
        assert!(!location.contains(&format!(":{password}@")), "{location}");
    }

    handle.join();
    drop(store);

    // A new server on the same catalog sees everything the console wrote.
    let reopened = CatalogStore::open(catalog, data, "w121b console second")
        .unwrap_or_else(|e| panic!("reopen {catalog}: {e}"));
    let mut conn = reopened.database().connect();
    let out = conn
        .execute("SELECT (SELECT count(*) FROM items), (SELECT count(*) FROM late), (SELECT sum(total) FROM sales)")
        .unwrap();
    let text = format!("{out:?}");
    assert!(
        text.contains("Bigint(40)") && text.contains("Bigint(5)") && text.contains("17.5"),
        "{text}"
    );
}
