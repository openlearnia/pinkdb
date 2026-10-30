//! Wave 121: the worksheet console over real HTTP — login sessions, CSRF,
//! the `Host`/`Origin` checks, the security headers, SQL as the logged-in
//! user (RBAC), `--ui-read-only`, cancellation, worksheets and history,
//! export and the catalog.
//!
//! A std-only HTTP client, as in `console.rs`: every test is a real
//! listener on an ephemeral loopback port and an in-memory store.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use pinkdb::Database;
use pinkdb_server_ui::{UiAuth, UiHandle, UiOptions, UiServer};
use serde_json::{Value, json};

/// A console without sign-in (`--ui-auth none`): the store has no users.
fn open_options() -> UiOptions {
    UiOptions {
        auth: UiAuth::None,
        ..UiOptions::default()
    }
}

// ---------- a tiny HTTP client ----------

#[derive(Debug)]
struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn json(&self) -> Value {
        serde_json::from_str(&self.body)
            .unwrap_or_else(|e| panic!("not JSON ({e}): {} {}", self.status, self.body))
    }
}

fn dechunk(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        let Ok(n) = usize::from_str_radix(size.trim(), 16) else {
            break;
        };
        if n == 0 || tail.len() < n {
            break;
        }
        out.push_str(&tail[..n]);
        rest = tail[n..].strip_prefix("\r\n").unwrap_or(&tail[n..]);
    }
    out
}

fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> Response {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    let mut text = format!("{method} {path} HTTP/1.1\r\n");
    if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("host")) {
        text.push_str("Host: localhost\r\n");
    }
    for (k, v) in headers {
        text.push_str(&format!("{k}: {v}\r\n"));
    }
    if let Some(body) = body {
        text.push_str("Content-Type: application/json\r\n");
        text.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    text.push_str("Connection: close\r\n\r\n");
    if let Some(body) = body {
        text.push_str(body);
    }
    stream.write_all(text.as_bytes()).unwrap();
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    let raw = String::from_utf8_lossy(&buf).into_owned();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    let chunked = headers
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("transfer-encoding") && v.contains("chunked"));
    let body = if chunked {
        dechunk(body)
    } else {
        body.to_string()
    };
    Response {
        status,
        headers,
        body,
    }
}

/// A logged-in browser: the session cookie and the CSRF token.
struct Client {
    addr: SocketAddr,
    cookie: String,
    csrf: String,
}

impl Client {
    fn get(&self, path: &str) -> Response {
        request(self.addr, "GET", path, &[("Cookie", &self.cookie)], None)
    }

    fn post(&self, path: &str, body: &Value) -> Response {
        request(
            self.addr,
            "POST",
            path,
            &[("Cookie", &self.cookie), ("X-CSRF-Token", &self.csrf)],
            Some(&body.to_string()),
        )
    }

    fn query(&self, sql: &str) -> Value {
        let response = self.post(
            "/api/query",
            &json!({ "sql": sql, "run_id": format!("r{}", rand_id()) }),
        );
        assert_eq!(response.status, 200, "{}", response.body);
        response.json()
    }
}

fn rand_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn login(addr: SocketAddr, body: Value) -> Result<Client, Response> {
    let response = request(addr, "POST", "/api/login", &[], Some(&body.to_string()));
    if response.status != 200 {
        return Err(response);
    }
    let cookie = response
        .header("set-cookie")
        .and_then(|c| c.split(';').next())
        .expect("a session cookie")
        .to_string();
    let csrf = response.json()["csrf"].as_str().unwrap().to_string();
    Ok(Client { addr, cookie, csrf })
}

// ---------- fixtures ----------

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
    let server = UiServer::bind("127.0.0.1:0", db, opts).expect("bind");
    let addr = server.local_addr();
    let handle = server.spawn().expect("spawn");
    Console { addr, handle }
}

fn options() -> UiOptions {
    UiOptions {
        read_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(30),
        ..open_options()
    }
}

fn store() -> Database {
    let db = Database::new();
    let mut conn = db.connect();
    conn.execute(
        "CREATE TABLE t (id INTEGER, name VARCHAR); \
         INSERT INTO t VALUES (1, 'alpha'), (2, '<script>alert(1)</script>'), (3, NULL); \
         CREATE VIEW v AS SELECT id FROM t; CREATE INDEX t_id ON t(id);",
    )
    .unwrap();
    db
}

/// A store with an administrator and a user without grants.
fn store_with_users() -> Database {
    let db = store();
    let mut conn = db.connect();
    conn.execute(
        "CREATE USER alice PASSWORD 'alice-pw'; GRANT ADMIN TO alice; \
         CREATE USER bob PASSWORD 'bob-pw';",
    )
    .unwrap();
    db
}

fn basic() -> UiOptions {
    UiOptions {
        auth: UiAuth::Password,
        ..options()
    }
}

// ---------- sessions and the browser defenses ----------

#[test]
fn login_sets_a_strict_http_only_cookie() {
    let console = start(store(), options());
    let response = request(console.addr, "POST", "/api/login", &[], Some("{}"));
    assert_eq!(response.status, 200, "{}", response.body);
    let cookie = response.header("set-cookie").unwrap();
    assert!(cookie.starts_with("pinkdb_session="), "{cookie}");
    assert!(cookie.contains("HttpOnly"), "{cookie}");
    assert!(cookie.contains("SameSite=Strict"), "{cookie}");
    assert!(cookie.contains("Path=/"), "{cookie}");
    assert!(!cookie.contains("Secure"), "plaintext console: {cookie}");
    let body = response.json();
    assert_eq!(body["auth"], "none");
    assert_eq!(body["csrf"].as_str().unwrap().len(), 43);
    // The cookie is not the CSRF token.
    assert!(!cookie.contains(body["csrf"].as_str().unwrap()));

    let secure = start(
        store(),
        UiOptions {
            secure_cookies: true,
            ..options()
        },
    );
    let response = request(secure.addr, "POST", "/api/login", &[], Some("{}"));
    assert!(response.header("set-cookie").unwrap().contains("; Secure"));
}

#[test]
fn every_post_needs_the_cookie_and_the_csrf_token() {
    let console = start(store(), options());
    let client = login(console.addr, json!({})).unwrap();
    let body = json!({ "sql": "SELECT 1", "run_id": "r1" }).to_string();
    // No cookie: not logged in.
    let r = request(console.addr, "POST", "/api/query", &[], Some(&body));
    assert_eq!(r.status, 401, "{}", r.body);
    // The cookie without the token.
    let r = request(
        console.addr,
        "POST",
        "/api/query",
        &[("Cookie", &client.cookie)],
        Some(&body),
    );
    assert_eq!(r.status, 403, "{}", r.body);
    // A wrong token.
    let r = request(
        console.addr,
        "POST",
        "/api/query",
        &[("Cookie", &client.cookie), ("X-CSRF-Token", "x")],
        Some(&body),
    );
    assert_eq!(r.status, 403, "{}", r.body);
    // The right token from another site's page.
    let r = request(
        console.addr,
        "POST",
        "/api/query",
        &[
            ("Cookie", &client.cookie),
            ("X-CSRF-Token", &client.csrf),
            ("Origin", "https://evil.example"),
        ],
        Some(&body),
    );
    assert_eq!(r.status, 403, "{}", r.body);
    // A login from another site's page is refused too.
    let r = request(
        console.addr,
        "POST",
        "/api/login",
        &[("Origin", "https://evil.example")],
        Some("{}"),
    );
    assert_eq!(r.status, 403);
    // Same origin, cookie and token: served.
    let origin = "http://localhost";
    let r = request(
        console.addr,
        "POST",
        "/api/query",
        &[
            ("Cookie", &client.cookie),
            ("X-CSRF-Token", &client.csrf),
            ("Origin", origin),
        ],
        Some(&body),
    );
    assert_eq!(r.status, 200, "{}", r.body);
    // Logout ends the session.
    assert_eq!(client.post("/api/logout", &json!({})).status, 200);
    assert_eq!(
        client
            .post("/api/query", &json!({"sql": "SELECT 1", "run_id": "r2"}))
            .status,
        401
    );
    assert_eq!(client.get("/api/session").status, 401);
}

#[test]
fn a_foreign_host_name_is_refused() {
    let console = start(store(), options());
    // A DNS-rebinding page reaches 127.0.0.1 with its own name in Host.
    for host in ["evil.example", "evil.example:8080", "127.0.0.1.nip.io"] {
        let r = request(
            console.addr,
            "GET",
            "/api/databases",
            &[("Host", host)],
            None,
        );
        assert_eq!(r.status, 421, "{host}: {}", r.body);
        let r = request(
            console.addr,
            "POST",
            "/api/login",
            &[("Host", host)],
            Some("{}"),
        );
        assert_eq!(r.status, 421, "{host}");
    }
    for host in [
        "localhost",
        "127.0.0.1",
        &console.addr.to_string(),
        "[::1]:9",
    ] {
        let r = request(
            console.addr,
            "GET",
            "/api/databases",
            &[("Host", host)],
            None,
        );
        assert_eq!(r.status, 200, "{host}: {}", r.body);
    }
    // An operator-listed name is allowed.
    let proxied = start(
        store(),
        UiOptions {
            allowed_hosts: vec!["console.internal".to_string()],
            ..options()
        },
    );
    let r = request(
        proxied.addr,
        "GET",
        "/api/databases",
        &[("Host", "console.internal:443")],
        None,
    );
    assert_eq!(r.status, 200);
}

#[test]
fn every_response_carries_the_security_headers() {
    let console = start(store(), options());
    for path in [
        "/",
        "/api/session",
        "/assets/console.css",
        "/nope",
        "/preview?table=t",
    ] {
        let r = request(console.addr, "GET", path, &[], None);
        let csp = r.header("content-security-policy").unwrap_or_default();
        assert!(csp.contains("default-src 'none'"), "{path}: {csp}");
        assert!(
            csp.contains("script-src 'self' 'wasm-unsafe-eval'"),
            "{path}: {csp}"
        );
        assert!(!csp.contains("unsafe-inline"), "{path}: {csp}");
        assert!(csp.contains("frame-ancestors 'none'"), "{path}: {csp}");
        assert_eq!(
            r.header("x-content-type-options"),
            Some("nosniff"),
            "{path}"
        );
        assert_eq!(r.header("x-frame-options"), Some("DENY"), "{path}");
        assert_eq!(r.header("referrer-policy"), Some("no-referrer"), "{path}");
    }
    // The shell has no inline script: every script is a file of the console.
    let shell = request(console.addr, "GET", "/", &[], None).body;
    for (i, _) in shell.match_indices("<script") {
        let tag = &shell[i..shell[i..].find('>').map_or(shell.len(), |e| i + e)];
        assert!(tag.contains("src=\"/assets/"), "an inline script: {tag}");
    }
}

#[test]
fn values_and_identifiers_stay_data() {
    let console = start(store(), options());
    let client = login(console.addr, json!({})).unwrap();
    let out = client.query("SELECT name AS \"<b onmouseover=x>\" FROM t ORDER BY id");
    let result = &out["results"][0];
    assert_eq!(result["columns"][0]["name"], "<b onmouseover=x>");
    assert_eq!(result["rows"][1][0], "<script>alert(1)</script>");
    assert_eq!(result["rows"][2][0], Value::Null);
    // JSON is the only encoding: the response is application/json, never
    // HTML, so the browser never parses it as markup.
    let r = client.post("/api/query", &json!({ "sql": "SELECT 1", "run_id": "x1" }));
    assert_eq!(r.header("content-type"), Some("application/json"));
}

// ---------- running SQL ----------

#[test]
fn statements_run_in_order_and_stop_at_the_first_error() {
    let console = start(store(), options());
    let client = login(console.addr, json!({})).unwrap();
    let out = client.query(
        "CREATE TABLE w (a INTEGER); INSERT INTO w VALUES (1), (2); \
         SELECT sum(a) AS s FROM w; SELECT * FROM missing; SELECT 1;",
    );
    let results = out["results"].as_array().unwrap();
    assert_eq!(results.len(), 3, "{out}");
    assert_eq!(results[0]["kind"], "ok");
    assert_eq!(results[1]["message"], "Inserted 2 rows");
    assert_eq!(results[2]["rows"][0][0], "3");
    assert_eq!(results[2]["columns"][0]["type"], "BIGINT");
    assert!(results[2]["profile"]["total_ms"].as_f64().unwrap() >= 0.0);
    assert_eq!(out["error"]["index"], 3);
    assert!(
        out["error"]["message"]
            .as_str()
            .unwrap()
            .contains("missing")
    );
    // A parse error runs nothing.
    let out = client.query("INSERT INTO w VALUES (9); SELEC oops");
    assert!(out["results"].as_array().unwrap().is_empty());
    let out = client.query("SELECT count(*) FROM w");
    assert_eq!(out["results"][0]["rows"][0][0], "2");
    // USE carries over to later statements, and the answer says where.
    let out = client.query("CREATE DATABASE other; USE other; CREATE TABLE o (x INT)");
    assert_eq!(out["db"], "other");
}

#[test]
fn shown_rows_are_capped_and_counted() {
    let console = start(
        store(),
        UiOptions {
            max_shown_rows: 2,
            ..options()
        },
    );
    let client = login(console.addr, json!({})).unwrap();
    let out = client.query("SELECT * FROM t");
    let result = &out["results"][0];
    assert_eq!(result["rows"].as_array().unwrap().len(), 2);
    assert_eq!(result["row_count"], 3);
    assert_eq!(result["truncated"], true);
}

#[test]
fn worksheets_run_under_the_users_grants() {
    let db = store_with_users();
    let console = start(db.clone(), basic());
    // A wrong password is refused, a right one starts a session.
    let r = login(console.addr, json!({ "user": "bob", "password": "nope" }));
    assert_eq!(r.err().unwrap().status, 401);
    let bob = login(console.addr, json!({ "user": "bob", "password": "bob-pw" })).unwrap();
    let alice = login(
        console.addr,
        json!({ "user": "alice", "password": "alice-pw" }),
    )
    .unwrap();
    assert_eq!(bob.get("/api/session").json()["user"], "bob");

    let out = bob.query("SELECT * FROM t");
    assert!(out["results"].as_array().unwrap().is_empty());
    let message = out["error"]["message"]
        .as_str()
        .unwrap()
        .to_ascii_lowercase();
    assert!(
        message.contains("permission") || message.contains("privilege"),
        "{message}"
    );
    let out = bob.query("CREATE TABLE mine (a INT)");
    assert!(out["error"].is_object(), "bob may not create tables: {out}");

    let out = alice.query("SELECT count(*) FROM t");
    assert_eq!(out["results"][0]["rows"][0][0], "3", "{out}");

    // The catalog shows bob only what he may read.
    let catalog = bob.get("/api/catalog").json();
    assert!(
        !catalog["objects"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["name"] == "t"),
        "{catalog}"
    );
    let catalog = alice.get("/api/catalog").json();
    let names: Vec<&str> = catalog["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"t") && names.contains(&"v") && names.contains(&"t_id"),
        "{names:?}"
    );

    // A dropped user's session ends with them.
    db.connect().execute("DROP USER bob").unwrap();
    let out = bob.query("SELECT 1");
    assert!(
        out["error"]["message"]
            .as_str()
            .unwrap()
            .contains("no longer exists"),
        "{out}"
    );
}

#[test]
fn read_only_mode_refuses_writes_for_everyone() {
    let console = start(
        store(),
        UiOptions {
            read_only: true,
            ..options()
        },
    );
    let client = login(console.addr, json!({})).unwrap();
    assert_eq!(client.get("/api/session").json()["read_only"], true);
    let out = client.query("SELECT 1; INSERT INTO t VALUES (9, 'x')");
    // Nothing ran: the check happens before the first statement.
    assert!(out["results"].as_array().unwrap().is_empty(), "{out}");
    assert_eq!(out["error"]["index"], 1);
    assert!(
        out["error"]["message"]
            .as_str()
            .unwrap()
            .contains("read-only")
    );
    let out = client.query("SELECT count(*) FROM t; EXPLAIN SELECT * FROM t; SHOW TABLES");
    assert_eq!(out["results"].as_array().unwrap().len(), 3, "{out}");
    assert_eq!(out["results"][0]["rows"][0][0], "3");
}

/// A store whose cross join runs for a long time without materializing much.
fn slow_store() -> Database {
    let db = Database::new();
    let mut conn = db.connect();
    conn.execute("CREATE TABLE big (x INTEGER)").unwrap();
    let values: Vec<String> = (0..20_000).map(|i| format!("({i})")).collect();
    conn.execute(&format!("INSERT INTO big VALUES {}", values.join(",")))
        .unwrap();
    db
}

const SLOW: &str = "SELECT count(*) FROM big a, big b WHERE a.x + b.x < -1";

#[test]
fn a_running_query_can_be_cancelled() {
    let console = start(
        slow_store(),
        UiOptions {
            session_queries: 1,
            ..options()
        },
    );
    let client = login(console.addr, json!({})).unwrap();
    let addr = console.addr;
    let (cookie, csrf) = (client.cookie.clone(), client.csrf.clone());
    let started = Instant::now();
    let runner = std::thread::spawn(move || {
        request(
            addr,
            "POST",
            "/api/query",
            &[("Cookie", &cookie), ("X-CSRF-Token", &csrf)],
            Some(&json!({ "sql": format!("{SLOW}; SELECT 1"), "run_id": "slow-1" }).to_string()),
        )
    });
    std::thread::sleep(Duration::from_millis(400));
    // The session may run one query at a time.
    let busy = client.post(
        "/api/query",
        &json!({ "sql": "SELECT 1", "run_id": "other" }),
    );
    assert_eq!(busy.status, 429, "{}", busy.body);
    // Another session cannot cancel it.
    let stranger = login(console.addr, json!({})).unwrap();
    let r = stranger.post("/api/cancel", &json!({ "run_id": "slow-1" }));
    assert_eq!(r.json()["cancelled"], false);
    // Its own session can.
    let r = client.post("/api/cancel", &json!({ "run_id": "slow-1" }));
    assert_eq!(r.json()["cancelled"], true, "{}", r.body);
    let response = runner.join().unwrap();
    let elapsed = started.elapsed();
    assert_eq!(response.status, 200, "{}", response.body);
    let out = response.json();
    assert_eq!(out["error"]["cancelled"], true, "{out}");
    assert_eq!(out["error"]["index"], 0);
    // The second statement did not run.
    assert!(out["results"].as_array().unwrap().is_empty());
    assert!(elapsed < Duration::from_secs(20), "cancel took {elapsed:?}");
    // The session is free again, and the run is in the history.
    let out = client.query("SELECT 1");
    assert_eq!(out["results"][0]["rows"][0][0], "1");
    let history = client.get("/api/history").json();
    assert!(
        history["history"]
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h["status"] == "cancelled"),
        "{history}"
    );
}

// ---------- worksheets, history, export ----------

#[test]
fn worksheets_are_private_until_shared() {
    let console = start(store_with_users(), basic());
    let alice = login(
        console.addr,
        json!({ "user": "alice", "password": "alice-pw" }),
    )
    .unwrap();
    let bob = login(console.addr, json!({ "user": "bob", "password": "bob-pw" })).unwrap();

    let created = alice.post(
        "/api/worksheets",
        &json!({ "title": "Q1 'draft'", "sql": "SELECT 'it''s' AS s" }),
    );
    assert_eq!(created.status, 200, "{}", created.body);
    let sheet = created.json();
    let id = sheet["id"].as_str().unwrap().to_string();
    assert_eq!(sheet["owner"], "alice");
    assert_eq!(sheet["sql"], "SELECT 'it''s' AS s");

    // Bob sees nothing and can change nothing.
    assert!(
        bob.get("/api/worksheets").json()["worksheets"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let r = bob.post(
        "/api/worksheets",
        &json!({ "id": id, "title": "x", "sql": "DROP TABLE t" }),
    );
    assert_eq!(r.status, 404);
    let r = bob.post("/api/worksheets/delete", &json!({ "id": id }));
    assert_eq!(r.status, 404);
    // Nobody reaches the worksheet table through SQL: it is in the state
    // store, not the warehouse (wave 121e).
    let out = bob.query("SELECT * FROM pinkdb_worksheets.worksheets");
    assert!(out["error"].is_object(), "{out}");
    let out = alice.query("SELECT * FROM pinkdb_worksheets.worksheets");
    assert!(out["error"].is_object(), "not even an administrator: {out}");

    // Shared with bob: he sees it, read-only.
    let r = alice.post(
        "/api/worksheets",
        &json!({ "id": id, "title": "Q1", "sql": "SELECT 2", "shared_with": "Bob" }),
    );
    assert_eq!(r.status, 200);
    let seen = bob.get("/api/worksheets").json();
    let list = seen["worksheets"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["mine"], false);
    assert_eq!(list[0]["sql"], "SELECT 2");
    assert_eq!(
        bob.post("/api/worksheets/delete", &json!({ "id": id }))
            .status,
        404
    );

    assert_eq!(
        alice
            .post("/api/worksheets/delete", &json!({ "id": id }))
            .status,
        200
    );
    assert!(
        alice.get("/api/worksheets").json()["worksheets"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn runs_go_to_the_users_history() {
    let console = start(store(), options());
    let client = login(console.addr, json!({})).unwrap();
    client.query("SELECT 41 + 1");
    client.query("SELECT * FROM nope");
    let history = client.get("/api/history").json();
    let list = history["history"].as_array().unwrap();
    assert_eq!(list.len(), 2, "{history}");
    assert_eq!(list[0]["status"], "error");
    assert_eq!(list[1]["sql"], "SELECT 41 + 1");
    assert_eq!(list[1]["rows"], 1);
}

#[test]
fn a_query_exports_as_csv_or_json() {
    let console = start(store(), options());
    let client = login(console.addr, json!({})).unwrap();
    let r = client.post(
        "/api/export",
        &json!({ "sql": "SELECT id, name FROM t ORDER BY id", "format": "csv" }),
    );
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(
        r.header("content-disposition")
            .unwrap()
            .contains("attachment")
    );
    assert_eq!(
        r.body,
        "id,name\r\n1,alpha\r\n2,<script>alert(1)</script>\r\n3,\r\n"
    );
    let r = client.post(
        "/api/export",
        &json!({ "sql": "SELECT id FROM t ORDER BY id", "format": "json" }),
    );
    assert_eq!(r.json(), json!([{ "id": 1 }, { "id": 2 }, { "id": 3 }]));
    // Export re-runs the statement, so only a query can be exported.
    let r = client.post(
        "/api/export",
        &json!({ "sql": "INSERT INTO t VALUES (4, 'd')", "format": "csv" }),
    );
    assert_eq!(r.status, 400);
    let r = client.post(
        "/api/export",
        &json!({ "sql": "SELECT 1; SELECT 2", "format": "csv" }),
    );
    assert_eq!(r.status, 400);
}

#[test]
fn the_catalog_lists_views_indexes_and_column_stats() {
    let db = store();
    db.connect()
        .execute(
            "CREATE PROCEDURE add_t(v INTEGER) AS $$ BEGIN INSERT INTO t VALUES (v, 'p'); END $$",
        )
        .unwrap();
    let console = start(db, options());
    let client = login(console.addr, json!({})).unwrap();
    let catalog = client.get("/api/catalog").json();
    let objects = catalog["objects"].as_array().unwrap();
    let find = |name: &str| objects.iter().find(|o| o["name"] == name).unwrap().clone();
    let t = find("t");
    assert_eq!(t["kind"], "TABLE");
    assert_eq!(t["rows"], 3);
    assert_eq!(t["columns"][1]["name"], "name");
    assert_eq!(t["columns"][1]["null_count"], 1);
    assert_eq!(find("v")["kind"], "VIEW");
    assert_eq!(find("v")["columns"][0]["name"], "id");
    let index = find("t_id");
    assert_eq!(index["kind"], "INDEX");
    assert_eq!(index["table_name"], "t");
    assert_eq!(catalog["current"], "main");
    // Every object names its schema (one per database), and procedures are
    // listed with their signature.
    assert!(objects.iter().all(|o| o["schema"] == "main"), "{objects:?}");
    let procedure = find("add_t");
    assert_eq!(procedure["kind"], "PROCEDURE");
    assert_eq!(procedure["detail"], "v INTEGER · SQL");
}

#[test]
fn the_browse_api_takes_the_session_cookie() {
    let console = start(store_with_users(), basic());
    // Without a credential the wave-78 API is closed.
    assert_eq!(
        request(console.addr, "GET", "/api/databases", &[], None).status,
        401
    );
    let bob = login(console.addr, json!({ "user": "bob", "password": "bob-pw" })).unwrap();
    assert_eq!(bob.get("/api/databases").status, 200);
    // Bob's grants apply to the browse API too.
    assert_ne!(bob.get("/api/rows?db=main&table=t").status, 200);
}

#[test]
fn an_unauthenticated_console_ends_when_the_store_gains_a_user() {
    let db = store();
    let console = start(db.clone(), options());
    let client = login(console.addr, json!({})).unwrap();
    assert_eq!(client.query("SELECT 1")["results"][0]["rows"][0][0], "1");
    db.connect()
        .execute("CREATE USER carol PASSWORD 'pw'")
        .unwrap();
    assert_eq!(
        client
            .post("/api/query", &json!({ "sql": "SELECT 1", "run_id": "z" }))
            .status,
        401
    );
    assert_eq!(login(console.addr, json!({})).err().unwrap().status, 401);
}

#[test]
fn a_slow_request_body_is_cut_off() {
    let console = start(
        store(),
        UiOptions {
            read_timeout: Duration::from_millis(500),
            ..options()
        },
    );
    let mut stream = TcpStream::connect(console.addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    // A head that promises a body, then nothing.
    stream
        .write_all(
            b"POST /api/login HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
              Content-Length: 100\r\n\r\n{",
        )
        .unwrap();
    let started = Instant::now();
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    assert!(started.elapsed() < Duration::from_secs(5));
    let text = String::from_utf8_lossy(&buf);
    assert!(text.starts_with("HTTP/1.1 408"), "{text}");
}

// ---------- wave 121e: the console's state is not in the warehouse ----------

#[test]
fn the_consoles_state_stays_out_of_the_warehouse() {
    let dir = std::env::temp_dir().join(format!("pinkdb-121e-{}", rand_id()));
    std::fs::create_dir_all(&dir).unwrap();
    let state_path = dir.join("warehouse.state.pink");
    let warehouse = store();
    let state = Database::open(state_path.to_str().unwrap()).unwrap();
    warehouse.attach_state_store(state).unwrap();
    {
        let console = start(warehouse.clone(), options());
        let client = login(console.addr, json!({})).unwrap();
        let r = client.post(
            "/api/worksheets",
            &json!({ "title": "kept", "sql": "SELECT 1" }),
        );
        assert_eq!(r.status, 200, "{}", r.body);
        client.query("SELECT 1");
        // Neither the catalog nor SQL on the warehouse shows it.
        let catalog = client.get("/api/catalog").json();
        let names = catalog["databases"].to_string();
        assert!(!names.contains("pinkdb_"), "{names}");
        let listed = client.query("SHOW DATABASES").to_string();
        assert!(!listed.contains("pinkdb_worksheets"), "{listed}");
    }
    // A new warehouse handle, the same state file: the worksheet is there.
    let state = Database::open(state_path.to_str().unwrap()).unwrap();
    let again = store();
    again.attach_state_store(state).unwrap();
    let console = start(again, options());
    let client = login(console.addr, json!({})).unwrap();
    let list = client.get("/api/worksheets").json();
    assert!(list.to_string().contains("\"kept\""), "{list}");
    drop(console);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_catalog_answers_after_its_database_is_dropped() {
    let console = start(store(), options());
    let client = login(console.addr, json!({})).unwrap();
    client.query("CREATE DATABASE sales; CREATE TABLE sales.orders (id INTEGER)");
    assert_eq!(client.get("/api/catalog?db=sales").status, 200);
    client.query("DROP DATABASE sales");
    // The browser still names it: the catalog answers from the default, so
    // the app can choose an existing database.
    let r = client.get("/api/catalog?db=sales");
    assert_eq!(r.status, 200, "{}", r.body);
    let catalog = r.json();
    assert!(
        !catalog["databases"].to_string().contains("sales"),
        "{catalog}"
    );
    assert_eq!(catalog["current"], "main");
}
