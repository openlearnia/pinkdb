//! Wave 121b over real HTTP: the Access panel's API (users, roles, grants
//! under RBAC), imports from a path or URL (a local HTTP server and a mock
//! S3), uploads (CSV and Parquet, parsed by the server and inserted as the
//! user), uploads into a durable store, and the Storage panel.
//!
//! One binary sets the process environment once (plain `http://` allowed for
//! the local server, the mock S3's endpoint and credentials) before any test
//! reads it; every test then uses the same values.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use pinkdb::Database;
use pinkdb_server_ui::{StorageInfo, UiAuth, UiHandle, UiOptions, UiServer};
use serde_json::{Value, json};

/// A console without sign-in (`--ui-auth none`): the store has no users.
fn open_options() -> UiOptions {
    UiOptions {
        auth: UiAuth::None,
        ..UiOptions::default()
    }
}

#[path = "../../pink-lake/src/mock_s3.rs"]
mod mock_s3;

// ---------- environment ----------

struct Env {
    mock: mock_s3::MockS3,
    http: SocketAddr,
}

const SALES_CSV: &str = "region,total\nnorth,10\nsouth,7.5\nwest,12\n";

/// The shared environment: started once, before any store exists.
fn env() -> &'static Env {
    static ENV: OnceLock<Env> = OnceLock::new();
    ENV.get_or_init(|| {
        let mock = mock_s3::MockS3::start();
        // SAFETY: set once, inside the `OnceLock` initializer, before any test
        // of this binary reads the environment.
        unsafe {
            std::env::set_var("PINKDB_HTTPFS_ALLOW_HTTP", "1");
            std::env::set_var("PINKDB_S3_ALLOW_HTTP", "1");
            std::env::set_var("AWS_ACCESS_KEY_ID", mock.access_key_id());
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "mock-secret");
            std::env::set_var("AWS_ENDPOINT_URL", mock.endpoint());
            std::env::set_var("AWS_ENDPOINT_URL_S3", mock.endpoint());
            std::env::set_var("AWS_REGION", "auto");
        }
        // The sources read their configuration when they register.
        pinkdb_httpfs::ensure_registered();
        pinkdb_parquet::ensure_registered();
        mock.put_object("data/sales.csv", SALES_CSV.as_bytes());
        mock.put_object("data/items.parquet", &parquet_bytes());
        Env {
            mock,
            http: serve_files(),
        }
    })
}

/// A Parquet file of three rows, written by the Parquet extension.
fn parquet_bytes() -> Vec<u8> {
    let path = temp("items.parquet");
    let mut scratch = pinkdb::PinkDb::new();
    scratch
        .execute(&format!(
            "CREATE TABLE items (id INTEGER, name VARCHAR); \
             INSERT INTO items VALUES (1, 'bolt'), (2, 'nut'), (3, NULL); \
             COPY items TO '{}' (FORMAT parquet);",
            path.display()
        ))
        .expect("write the parquet fixture");
    std::fs::read(&path).unwrap()
}

/// A tiny HTTP file server: `GET /sales.csv` answers [`SALES_CSV`].
fn serve_files() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).unwrap_or(0);
            let head = String::from_utf8_lossy(&buf[..n]).to_string();
            let found = head.starts_with("GET /sales.csv") || head.starts_with("HEAD /sales.csv");
            let body = if found { SALES_CSV } else { "" };
            let status = if found { "200 OK" } else { "404 Not Found" };
            let _ = stream.write_all(
                format!(
                    "HTTP/1.1 {status}\r\ncontent-type: text/csv\r\ncontent-length: {}\r\n\
                     connection: close\r\n\r\n{}",
                    body.len(),
                    if head.starts_with("HEAD") { "" } else { body }
                )
                .as_bytes(),
            );
        }
    });
    addr
}

fn temp(name: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinkdb-w121b-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

// ---------- an HTTP client ----------

#[derive(Debug)]
struct Response {
    status: u16,
    head: String,
    body: String,
}

impl Response {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body)
            .unwrap_or_else(|e| panic!("not JSON ({e}): {} {}", self.status, self.body))
    }
}

fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Response {
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
    Response {
        status,
        head: head.to_string(),
        body: body.to_string(),
    }
}

struct Client {
    addr: SocketAddr,
    cookie: String,
    csrf: String,
}

impl Client {
    fn get(&self, path: &str) -> Response {
        request(self.addr, "GET", path, &[("Cookie", &self.cookie)], b"")
    }

    fn post(&self, path: &str, body: &Value) -> Response {
        request(
            self.addr,
            "POST",
            path,
            &[
                ("Cookie", &self.cookie),
                ("X-CSRF-Token", &self.csrf),
                ("Content-Type", "application/json"),
            ],
            body.to_string().as_bytes(),
        )
    }

    fn upload(&self, query: &str, bytes: &[u8]) -> Response {
        request(
            self.addr,
            "POST",
            &format!("/api/upload?{query}"),
            &[
                ("Cookie", &self.cookie),
                ("X-CSRF-Token", &self.csrf),
                ("Content-Type", "application/octet-stream"),
            ],
            bytes,
        )
    }

    /// Run SQL; the whole answer.
    fn query(&self, sql: &str) -> Value {
        use std::sync::atomic::{AtomicU64, Ordering};
        static RUN: AtomicU64 = AtomicU64::new(0);
        let r = self.post(
            "/api/query",
            &json!({ "sql": sql, "run_id": format!("r{}", RUN.fetch_add(1, Ordering::Relaxed)) }),
        );
        assert_eq!(r.status, 200, "{}", r.body);
        r.json()
    }

    /// Run SQL that must succeed; the first result's first cell.
    fn scalar(&self, sql: &str) -> String {
        let out = self.query(sql);
        assert!(out["error"].is_null(), "{sql}: {out}");
        let last = out["results"].as_array().unwrap().last().unwrap().clone();
        last["rows"][0][0].as_str().unwrap_or_default().to_string()
    }

    /// Run SQL that must fail; the error message.
    fn refused(&self, sql: &str) -> String {
        let out = self.query(sql);
        out["error"]["message"]
            .as_str()
            .unwrap_or_else(|| panic!("{sql} should have failed: {out}"))
            .to_string()
    }
}

fn login(addr: SocketAddr, body: Value) -> Result<Client, Response> {
    let r = request(
        addr,
        "POST",
        "/api/login",
        &[("Content-Type", "application/json")],
        body.to_string().as_bytes(),
    );
    if r.status != 200 {
        return Err(r);
    }
    let cookie = r
        .head
        .lines()
        .find_map(|l| l.strip_prefix("set-cookie: "))
        .and_then(|c| c.split(';').next())
        .unwrap()
        .to_string();
    let csrf = r.json()["csrf"].as_str().unwrap().to_string();
    Ok(Client { addr, cookie, csrf })
}

struct Console {
    addr: SocketAddr,
    handle: UiHandle,
}

impl Drop for Console {
    fn drop(&mut self) {
        self.handle.shutdown();
    }
}

fn start(db: Database, opts: UiOptions) -> Console {
    let server = UiServer::bind("127.0.0.1:0", db, opts).unwrap();
    let addr = server.local_addr();
    Console {
        addr,
        handle: server.spawn().unwrap(),
    }
}

fn basic() -> UiOptions {
    UiOptions {
        auth: UiAuth::Password,
        ..open_options()
    }
}

/// alice (ADMIN), bob (member of `analyst`, which may read `t`), and carol
/// (no grants).
fn store_with_users() -> Database {
    let db = Database::new();
    db.connect()
        .execute(
            "CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1), (2); \
             CREATE USER alice PASSWORD 'alice-pw'; GRANT ADMIN TO alice; \
             CREATE USER bob PASSWORD 'bob-pw'; CREATE ROLE analyst; \
             GRANT SELECT ON TABLE main.t TO analyst; GRANT analyst TO bob; \
             CREATE USER carol PASSWORD 'carol-pw';",
        )
        .unwrap();
    db
}

fn user(addr: SocketAddr, name: &str) -> Client {
    login(
        addr,
        json!({ "user": name, "password": format!("{name}-pw") }),
    )
    .unwrap()
}

// ---------- access ----------

#[test]
fn the_access_catalog_shows_admins_everything_and_users_themselves() {
    env();
    let console = start(store_with_users(), basic());
    let alice = user(console.addr, "alice");
    let bob = user(console.addr, "bob");
    assert_eq!(alice.get("/api/session").json()["admin"], true);
    assert_eq!(bob.get("/api/session").json()["admin"], false);

    let all = alice.get("/api/access").json();
    assert_eq!(all["admin"], true);
    let names: Vec<&str> = all["principals"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["alice", "bob", "carol", "analyst"], "{all}");
    assert_eq!(all["grants"].as_array().unwrap().len(), 3, "{all}");

    // Bob sees himself and his role, nothing about alice or carol.
    let mine = bob.get("/api/access").json();
    assert_eq!(mine["admin"], false);
    let names: Vec<&str> = mine["principals"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["bob", "analyst"], "{mine}");
    assert!(!mine.to_string().contains("alice"), "{mine}");
    assert!(!mine.to_string().contains("carol"), "{mine}");
    // Through SQL too: the relations filter the same way.
    assert_eq!(bob.scalar("SELECT count(*) FROM pinkdb_users()"), "2");
}

#[test]
fn admins_manage_users_and_grants_from_the_console() {
    env();
    let console = start(store_with_users(), basic());
    let alice = user(console.addr, "alice");
    let bob = user(console.addr, "bob");

    // A new user, through the dedicated endpoint.
    let r = alice.post(
        "/api/access/user",
        &json!({ "name": "dave", "password": "dave-password" }),
    );
    assert_eq!(r.status, 200, "{}", r.body);
    // A console-made password needs ten characters (wave 121d).
    let r = alice.post(
        "/api/access/user",
        &json!({ "name": "erin", "password": "short" }),
    );
    assert_eq!(r.status, 400, "{}", r.body);
    let dave = login(
        console.addr,
        json!({ "user": "dave", "password": "dave-password" }),
    )
    .unwrap();
    let message = dave.refused("SELECT * FROM t");
    assert!(message.to_lowercase().contains("permission"), "{message}");

    // A grant from the console takes effect at once.
    alice.scalar("GRANT SELECT ON TABLE main.t TO dave; SELECT 1");
    assert_eq!(dave.scalar("SELECT count(*) FROM t"), "2");
    alice.scalar("REVOKE SELECT ON TABLE main.t FROM dave; SELECT 1");
    assert!(
        dave.refused("SELECT * FROM t")
            .to_lowercase()
            .contains("permission")
    );

    // A password change: the old one stops working.
    let r = alice.post(
        "/api/access/user",
        &json!({ "name": "dave", "password": "new-password", "alter": true }),
    );
    assert_eq!(r.status, 200, "{}", r.body);
    // A reset ends the user's sessions (wave 121d).
    assert_eq!(dave.get("/api/session").status, 401);
    assert!(
        login(
            console.addr,
            json!({ "user": "dave", "password": "dave-password" })
        )
        .is_err()
    );
    assert!(
        login(
            console.addr,
            json!({ "user": "dave", "password": "new-password" })
        )
        .is_ok()
    );

    // A non-admin cannot create users or grant.
    let r = bob.post(
        "/api/access/user",
        &json!({ "name": "eve", "password": "a long password" }),
    );
    assert_eq!(r.status, 403, "{}", r.body);
    assert!(
        bob.refused("GRANT SELECT ON TABLE main.t TO carol")
            .to_lowercase()
            .contains("permission")
    );
    // Bad names are refused before any statement.
    let r = alice.post(
        "/api/access/user",
        &json!({ "name": "x\"; DROP", "password": "x" }),
    );
    assert_eq!(r.status, 400);

    // Passwords stay out of the run history, even typed into a worksheet.
    alice.scalar("CREATE USER frank PASSWORD 'hunter2-secret'; SELECT 1");
    let history = alice.get("/api/history").json().to_string();
    assert!(!history.contains("hunter2-secret"), "{history}");
    assert!(
        !history.contains("new-pw") && !history.contains("dave-pw"),
        "{history}"
    );
    assert!(history.contains("not recorded"), "{history}");
}

// ---------- imports from a path or URL ----------

#[test]
fn url_imports_run_as_the_user() {
    let env = env();
    let console = start(store_with_users(), basic());
    let alice = user(console.addr, "alice");
    let bob = user(console.addr, "bob");
    let url = format!("http://{}/sales.csv", env.http);

    let preview = alice.query(&format!("SELECT * FROM read_csv('{url}') LIMIT 50"));
    assert_eq!(preview["results"][0]["row_count"], 3, "{preview}");
    alice.scalar(&format!(
        "CREATE TABLE main.sales AS SELECT * FROM read_csv('{url}'); SELECT 1"
    ));
    assert_eq!(alice.scalar("SELECT sum(total) FROM sales"), "29.5");

    // A non-admin cannot make the server fetch a URL or read its files.
    let message = bob.refused(&format!("SELECT * FROM read_csv('{url}')"));
    assert!(message.to_lowercase().contains("admin"), "{message}");
    let local = temp("server.csv");
    std::fs::write(&local, SALES_CSV).unwrap();
    let message = bob.refused(&format!("SELECT * FROM read_csv('{}')", local.display()));
    assert!(message.to_lowercase().contains("admin"), "{message}");
}

#[test]
fn s3_imports_read_csv_and_parquet() {
    let env = env();
    let console = start(Database::new(), open_options());
    let operator = login(console.addr, json!({})).unwrap();
    let csv = env.mock.url("bucket", "data/sales.csv");
    let parquet = env.mock.url("bucket", "data/items.parquet");
    // The mock serves one bucket regardless of its name; the URLs are its.
    operator.scalar(&format!(
        "CREATE TABLE s3_sales AS SELECT * FROM read_csv('{csv}'); SELECT 1"
    ));
    assert_eq!(operator.scalar("SELECT count(*) FROM s3_sales"), "3");
    operator.scalar(&format!(
        "CREATE TABLE items AS SELECT * FROM read_parquet('{parquet}'); SELECT 1"
    ));
    assert_eq!(
        operator.scalar("SELECT count(*) FROM items WHERE name IS NULL"),
        "1"
    );
    // Append and replace, as the Import dialog writes them.
    operator.scalar(&format!(
        "INSERT INTO s3_sales SELECT * FROM read_csv('{csv}'); SELECT 1"
    ));
    assert_eq!(operator.scalar("SELECT count(*) FROM s3_sales"), "6");
    operator.scalar(&format!(
        "DROP TABLE IF EXISTS main.s3_sales; CREATE TABLE main.s3_sales AS SELECT * FROM read_csv('{csv}'); SELECT 1"
    ));
    assert_eq!(operator.scalar("SELECT count(*) FROM s3_sales"), "3");
}

// ---------- uploads ----------

#[test]
fn uploads_are_parsed_by_the_server_and_inserted_as_the_user() {
    env();
    let db = store_with_users();
    db.connect()
        .execute("GRANT CREATE ON DATABASE main TO bob")
        .unwrap();
    let console = start(db, basic());
    let bob = user(console.addr, "bob");
    let carol = user(console.addr, "carol");

    // A CSV upload by a non-admin with CREATE on the database.
    let r = bob.upload("table=uploaded&name=sales.csv", SALES_CSV.as_bytes());
    assert_eq!(r.status, 200, "{}", r.body);
    let result = r.json();
    assert_eq!(result["rows"], 3);
    assert_eq!(result["database"], "main");
    assert_eq!(result["columns"][1]["name"], "total");
    // The creator reads it back (or an admin does when the creator may not).
    let alice = user(console.addr, "alice");
    assert_eq!(alice.scalar("SELECT sum(total) FROM uploaded"), "29.5");

    // Existing table: create fails, replace and append work.
    let r = bob.upload("table=uploaded&name=sales.csv", SALES_CSV.as_bytes());
    assert_ne!(r.status, 200);
    let r = bob.upload(
        "table=uploaded&name=sales.csv&mode=replace",
        SALES_CSV.as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.body);
    let r = alice.upload(
        "table=uploaded&name=sales.csv&mode=append",
        SALES_CSV.as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(alice.scalar("SELECT count(*) FROM uploaded"), "6");

    // A Parquet upload.
    let r = alice.upload("table=items&format=parquet", &parquet_bytes());
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(alice.scalar("SELECT count(*) FROM items"), "3");

    // Without CREATE: refused, and nothing is left behind.
    let r = carol.upload("table=nope&name=x.csv", SALES_CSV.as_bytes());
    assert_eq!(r.status, 403, "{}", r.body);
    assert!(alice.refused("SELECT * FROM nope").contains("nope"));

    // Garbage and unknown formats are refused cleanly.
    let r = alice.upload("table=bad&format=parquet", b"not parquet");
    assert_eq!(r.status, 400, "{}", r.body);
    let r = alice.upload("table=bad&name=x.xlsx", b"x");
    assert_eq!(r.status, 400, "{}", r.body);
    let r = alice.upload("table=bad&name=x.csv", &[0xff, 0xfe, 0x00]);
    assert_eq!(r.status, 400, "{}", r.body);

    // The upload is a POST: it needs the CSRF token like the rest.
    let r = request(
        console.addr,
        "POST",
        "/api/upload?table=x&name=x.csv",
        &[("Cookie", &alice.cookie)],
        SALES_CSV.as_bytes(),
    );
    assert_eq!(r.status, 403);
}

#[test]
fn an_upload_into_a_durable_store_survives_a_reopen() {
    env();
    let path = temp("durable.pink");
    let db = Database::open(path.to_str().unwrap()).unwrap();
    let console = start(db, open_options());
    let operator = login(console.addr, json!({})).unwrap();
    let r = operator.upload("table=kept&name=kept.csv", SALES_CSV.as_bytes());
    assert_eq!(r.status, 200, "{}", r.body);
    drop(operator);
    drop(console);
    // The console's runtime is gone; drop every handle, then reopen: the
    // WAL holds rows, not a reference to the uploaded file.
    std::thread::sleep(Duration::from_millis(200));
    let reopened = Database::open(path.to_str().unwrap()).unwrap();
    let out = reopened
        .connect()
        .execute("SELECT count(*), sum(total) FROM kept")
        .unwrap();
    let text = format!("{out:?}");
    assert!(
        text.contains("Bigint(3)") && text.contains("29.5"),
        "{text}"
    );
}

#[test]
fn uploads_are_size_capped_and_refused_when_read_only() {
    env();
    let console = start(
        Database::new(),
        UiOptions {
            max_upload_bytes: 16,
            ..open_options()
        },
    );
    let operator = login(console.addr, json!({})).unwrap();
    let r = operator.upload("table=x&name=x.csv", SALES_CSV.as_bytes());
    assert_eq!(r.status, 413, "{}", r.body);

    let read_only = start(
        Database::new(),
        UiOptions {
            read_only: true,
            ..open_options()
        },
    );
    let operator = login(read_only.addr, json!({})).unwrap();
    let r = operator.upload("table=x&name=x.csv", SALES_CSV.as_bytes());
    assert_eq!(r.status, 403, "{}", r.body);
    let r = operator.post("/api/access/user", &json!({ "name": "x", "password": "y" }));
    assert_eq!(r.status, 403, "{}", r.body);
}

// ---------- storage ----------

#[test]
fn the_storage_panel_reports_the_store() {
    env();
    let console = start(Database::new(), open_options());
    let operator = login(console.addr, json!({})).unwrap();
    let storage = operator.get("/api/storage").json();
    assert_eq!(storage["mode"], "in-memory");
    assert!(storage.to_string().contains("\"durable\""), "{storage}");

    let path = temp("panel.pink");
    let info = StorageInfo {
        mode: "durable".to_string(),
        location: path.display().to_string(),
        facts: vec![("format".to_string(), ".pink file + WAL".to_string())],
        live: Some(std::sync::Arc::new(|| {
            vec![("checked".to_string(), "now".to_string())]
        })),
        import_formats: vec!["upload:csv".to_string(), "url:csv".to_string()],
    };
    let console = start(
        Database::open(path.to_str().unwrap()).unwrap(),
        UiOptions {
            storage: info,
            ..open_options()
        },
    );
    let operator = login(console.addr, json!({})).unwrap();
    let storage = operator.get("/api/storage").json();
    assert_eq!(storage["mode"], "durable");
    assert_eq!(storage["import_formats"][1], "url:csv");
    let facts = storage["facts"].to_string();
    assert!(
        facts.contains("checked") && facts.contains("yes (WAL)"),
        "{facts}"
    );
    // CHECKPOINT from the console (the panel's button).
    assert!(operator.query("CHECKPOINT")["error"].is_null());
    // The redaction helper the server uses for locations.
    assert_eq!(
        pinkdb_server_ui::redact("postgres://u:pw@h/db"),
        "postgres://u:***@h/db"
    );
}

#[test]
fn a_large_upload_loads_in_one_statement() {
    env();
    let console = start(Database::new(), open_options());
    let operator = login(console.addr, json!({})).unwrap();
    let mut csv = String::from("id,name,price,day\n");
    for i in 0..50_000 {
        csv.push_str(&format!(
            "{i},item {i},{}.25,2026-01-{:02}\n",
            i % 1000,
            i % 28 + 1
        ));
    }
    let started = std::time::Instant::now();
    let r = operator.upload("table=big&name=big.csv", csv.as_bytes());
    let elapsed = started.elapsed();
    assert_eq!(r.status, 200, "{}", r.body);
    eprintln!("50,000-row upload: {elapsed:?}");
    assert_eq!(operator.scalar("SELECT count(*) FROM big"), "50000");
    assert_eq!(
        operator.scalar("SELECT typeof(day) FROM big LIMIT 1"),
        "DATE"
    );
    let r = operator.upload("table=big&name=big.csv&mode=append", csv.as_bytes());
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(operator.scalar("SELECT count(*) FROM big"), "100000");
}
