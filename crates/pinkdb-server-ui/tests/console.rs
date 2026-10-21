//! The console's integration tests: a **real listener** on an ephemeral
//! port, driven by a tiny std-only HTTP client (no test HTTP framework, no
//! async test harness — the console owns its own runtime).
//!
//! Every test is hermetic: an in-memory store, a loopback port and no
//! network access beyond it.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use pinkdb::{Database, QueryResult};
use pinkdb_server_ui::{UiAuth, UiHandle, UiOptions, UiServer};

// ---------- a tiny HTTP client ----------

/// One HTTP response.
struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl HttpResponse {
    /// The value of a response header (case-insensitive).
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// The body parsed as JSON.
    fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).unwrap_or_else(|err| {
            panic!("the body is not JSON ({err}): {}", self.body);
        })
    }
}

/// Send a raw request and read the whole response.
fn raw(addr: SocketAddr, request: &str) -> HttpResponse {
    let mut stream = TcpStream::connect(addr).expect("connect to the console");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .expect("write timeout");
    stream
        .write_all(request.as_bytes())
        .expect("write the request");
    stream.flush().expect("flush");
    let mut buf = Vec::new();
    // A refused request (an oversized header, say) may be closed without a
    // response: that is a refusal too, not a test failure.
    match stream.read_to_end(&mut buf) {
        Ok(_) => {}
        Err(err)
            if matches!(
                err.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof
            ) => {}
        Err(err) => panic!("read the response: {err}"),
    }
    parse(&buf)
}

/// Parse a raw HTTP response.
fn parse(buf: &[u8]) -> HttpResponse {
    let text = String::from_utf8_lossy(buf).into_owned();
    let (head, body) = match text.split_once("\r\n\r\n") {
        Some((head, body)) => (head, body.to_string()),
        None => (text.as_str(), String::new()),
    };
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
        .collect();
    HttpResponse {
        status,
        headers,
        body,
    }
}

/// `GET path`.
fn get(addr: SocketAddr, path: &str) -> HttpResponse {
    raw(
        addr,
        &format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
    )
}

/// `GET path` with extra headers.
fn get_with(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> HttpResponse {
    let mut request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n");
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("Connection: close\r\n\r\n");
    raw(addr, &request)
}

// ---------- the fixture ----------

/// The global engine hook can only hold one test's closure at a time: the
/// two tests that install it take this lock, so they never overlap.
static HOOK_LOCK: Mutex<()> = Mutex::new(());

/// A running console plus the store it serves.
struct Fixture {
    addr: SocketAddr,
    handle: UiHandle,
}

impl Fixture {
    /// Start a console on an ephemeral loopback port.
    fn start(db: Database, opts: UiOptions) -> Fixture {
        let server = UiServer::bind("127.0.0.1:0", db.clone(), opts).expect("bind the console");
        let addr = server.local_addr();
        let handle = server.spawn().expect("spawn the console");
        Fixture { addr, handle }
    }

    /// The console's address.
    fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.handle.shutdown();
    }
}

/// A store with a small table (including a cell that carries markup).
fn store() -> Database {
    let db = Database::new();
    let mut conn = db.connect();
    conn.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
        .expect("create the table");
    conn.execute(
        "INSERT INTO t VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma'), (4, NULL), \
         (5, '<script>alert(1)</script>')",
    )
    .expect("insert the rows");
    conn.execute("CREATE TABLE other (x BIGINT)")
        .expect("create the second table");
    conn.execute("INSERT INTO other VALUES (10), (20)")
        .expect("insert the second table's rows");
    db
}

/// The default options (fast timeouts, a small page bound).
fn options() -> UiOptions {
    UiOptions {
        read_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(20),
        max_page_size: 10,
        ..UiOptions::default()
    }
}

// ---------- the shell and the assets ----------

/// The shell page loads, and it is entirely self-hosted.
#[test]
fn the_shell_page_is_self_hosted() {
    let fixture = Fixture::start(store(), options());
    let response = get(fixture.addr(), "/");
    assert_eq!(response.status, 200, "body: {}", response.body);
    assert!(
        response
            .header("content-type")
            .unwrap_or_default()
            .starts_with("text/html"),
        "content type: {:?}",
        response.header("content-type")
    );
    let body = response.body;
    // The assets are served by the console itself.
    assert!(body.contains("/assets/console.css"), "{body}");
    assert!(body.contains("/assets/console.js"), "{body}");
    // No CDN, no external font, no external script.
    assert!(
        !body.contains("http://"),
        "an external URL in the shell: {body}"
    );
    assert!(
        !body.contains("https://"),
        "an external URL in the shell: {body}"
    );
    assert!(
        !body.contains("//cdn"),
        "a CDN reference in the shell: {body}"
    );
    assert!(
        !body.contains("@import"),
        "a CSS import in the shell: {body}"
    );
}

#[test]
fn the_assets_are_served_from_the_binary() {
    let fixture = Fixture::start(store(), options());
    let css = get(fixture.addr(), "/assets/console.css");
    assert_eq!(css.status, 200);
    assert!(
        css.header("content-type")
            .unwrap_or_default()
            .contains("text/css")
    );
    assert!(css.body.contains(".grid"), "the stylesheet looks empty");

    let js = get(fixture.addr(), "/assets/console.js");
    assert_eq!(js.status, 200);
    assert!(
        js.header("content-type")
            .unwrap_or_default()
            .contains("javascript")
    );
    assert!(
        js.body.contains("/api/"),
        "the script does not call the API"
    );
    // No external resource in the script either.
    assert!(
        !js.body.contains("http://"),
        "an external URL in the script"
    );
    assert!(
        !js.body.contains("https://"),
        "an external URL in the script"
    );
}

// ---------- the JSON API ----------

#[test]
fn the_databases_endpoint_returns_the_catalog() {
    let fixture = Fixture::start(store(), options());
    let response = get(fixture.addr(), "/api/databases");
    assert_eq!(response.status, 200);
    let body = response.json();
    assert_eq!(body["current"], "main");
    let names: Vec<&str> = body["databases"]
        .as_array()
        .expect("an array of databases")
        .iter()
        .map(|db| db["name"].as_str().expect("a database name"))
        .collect();
    assert!(names.contains(&"main"), "{names:?}");
}

#[test]
fn the_schemas_endpoint_returns_the_schemas() {
    let fixture = Fixture::start(store(), options());
    let response = get(fixture.addr(), "/api/schemas?db=main");
    assert_eq!(response.status, 200);
    let body = response.json();
    assert_eq!(body["db"], "main");
    let schemas = body["schemas"].as_array().expect("an array of schemas");
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0]["name"], "main");
    assert_eq!(schemas[0]["relations"], 2);
}

#[test]
fn the_tables_endpoint_returns_the_relations_and_their_columns() {
    let fixture = Fixture::start(store(), options());
    let response = get(fixture.addr(), "/api/tables?db=main&schema=main");
    assert_eq!(response.status, 200);
    let body = response.json();
    assert_eq!(body["db"], "main");
    assert_eq!(body["schema"], "main");
    let tables = body["tables"].as_array().expect("an array of tables");
    assert_eq!(tables.len(), 2);
    let first = &tables[0];
    assert_eq!(first["name"], "other");
    let columns = first["columns"].as_array().expect("an array of columns");
    assert_eq!(columns[0]["name"], "x");
    assert_eq!(columns[0]["type"], "BIGINT");
}

#[test]
fn the_rows_endpoint_previews_a_page() {
    let fixture = Fixture::start(store(), options());
    let response = get(
        fixture.addr(),
        "/api/rows?db=main&table=t&page=1&page_size=2",
    );
    assert_eq!(response.status, 200);
    let body = response.json();
    assert_eq!(body["db"], "main");
    assert_eq!(body["table"], "t");
    assert_eq!(body["page"], 1);
    assert_eq!(body["page_size"], 2);
    assert_eq!(body["total_rows"], 5);
    assert_eq!(body["total_pages"], 3);
    let columns = body["columns"].as_array().expect("an array of columns");
    assert_eq!(columns[0]["name"], "id");
    assert_eq!(columns[0]["type"], "INTEGER");
    let rows = body["rows"].as_array().expect("an array of rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0]["text"], "1");
    assert_eq!(rows[0][1]["text"], "alpha");
}

#[test]
fn the_rows_endpoint_paginates_and_reports_the_totals() {
    let fixture = Fixture::start(store(), options());
    let first = get(
        fixture.addr(),
        "/api/rows?db=main&table=t&page=1&page_size=2",
    )
    .json();
    let second = get(
        fixture.addr(),
        "/api/rows?db=main&table=t&page=2&page_size=2",
    )
    .json();
    let third = get(
        fixture.addr(),
        "/api/rows?db=main&table=t&page=3&page_size=2",
    )
    .json();
    let fourth = get(
        fixture.addr(),
        "/api/rows?db=main&table=t&page=4&page_size=2",
    )
    .json();

    // The totals are the *true* totals, on every page.
    for page in [&first, &second, &third, &fourth] {
        assert_eq!(page["total_rows"], 5);
        assert_eq!(page["total_pages"], 3);
    }
    // The pages do not overlap and cover every row.
    let mut seen = Vec::new();
    for page in [&first, &second, &third] {
        for row in page["rows"].as_array().expect("rows") {
            seen.push(row[0]["text"].as_str().expect("a cell").to_string());
        }
    }
    assert_eq!(seen, vec!["1", "2", "3", "4", "5"]);
    // Past the last page there is nothing left.
    assert_eq!(fourth["rows"].as_array().expect("rows").len(), 0);
}

#[test]
fn the_page_size_is_bounded() {
    let fixture = Fixture::start(store(), options());
    let body = get(fixture.addr(), "/api/rows?db=main&table=t&page_size=100000").json();
    assert_eq!(body["page_size"], 10, "the page size must be clamped");
    let rows = body["rows"].as_array().expect("rows");
    assert!(rows.len() <= 10, "the page must be bounded: {}", rows.len());
}

#[test]
fn a_zero_page_is_refused() {
    let fixture = Fixture::start(store(), options());
    let response = get(fixture.addr(), "/api/rows?db=main&table=t&page=0");
    assert_eq!(response.status, 400);
    assert!(
        response.body.contains("1-based"),
        "the error must explain the page numbering: {}",
        response.body
    );
}

#[test]
fn a_missing_table_parameter_is_refused() {
    let fixture = Fixture::start(store(), options());
    let response = get(fixture.addr(), "/api/rows?db=main");
    assert_eq!(response.status, 400);
}

#[test]
fn the_console_browses_another_database() {
    let db = store();
    {
        let mut conn = db.connect();
        conn.execute("CREATE DATABASE sales")
            .expect("create the second database");
        conn.execute("USE sales")
            .expect("switch to the second database");
        conn.execute("CREATE TABLE orders (id INTEGER)")
            .expect("create a table in the second database");
        conn.execute("INSERT INTO orders VALUES (1), (2)")
            .expect("insert the rows");
    }
    let fixture = Fixture::start(db, options());

    // The catalog lists both databases.
    let body = get(fixture.addr(), "/api/databases").json();
    let names: Vec<&str> = body["databases"]
        .as_array()
        .expect("an array of databases")
        .iter()
        .map(|db| db["name"].as_str().expect("a name"))
        .collect();
    assert!(names.contains(&"main"), "{names:?}");
    assert!(names.contains(&"sales"), "{names:?}");

    // The other database's relations and rows are reachable.
    let tables = get(fixture.addr(), "/api/tables?db=sales").json();
    let tables = tables["tables"].as_array().expect("an array of tables");
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0]["name"], "orders");

    let rows = get(fixture.addr(), "/api/rows?db=sales&table=orders").json();
    assert_eq!(rows["total_rows"], 2);
    assert_eq!(rows["rows"].as_array().expect("rows").len(), 2);

    // And the first database still answers with its own relations.
    let tables = get(fixture.addr(), "/api/tables?db=main").json();
    assert_eq!(tables["tables"].as_array().expect("tables").len(), 2);
}

#[test]
fn the_stats_endpoint_reports_row_counts_and_sizes() {
    let fixture = Fixture::start(store(), options());
    let response = get(fixture.addr(), "/api/stats?db=main");
    assert_eq!(response.status, 200);
    let body = response.json();
    assert_eq!(body["db"], "main");
    let tables = body["tables"].as_array().expect("an array of tables");
    assert_eq!(tables.len(), 2);
    let t = tables
        .iter()
        .find(|table| table["name"] == "t")
        .expect("the table t");
    assert_eq!(t["rows"], 5);
    assert_eq!(t["columns"], 2);
    // INTEGER + VARCHAR: 4 + 16 bytes per row.
    assert_eq!(t["estimated_bytes"], 5 * 20);
}

// ---------- the method and path handling ----------

#[test]
fn an_unknown_path_is_404() {
    let fixture = Fixture::start(store(), options());
    let response = get(fixture.addr(), "/api/nope");
    assert_eq!(response.status, 404);
    let response = get(fixture.addr(), "/nope");
    assert_eq!(response.status, 404);
}

#[test]
fn a_mutating_method_is_405() {
    let fixture = Fixture::start(store(), options());
    for path in ["/", "/api/databases", "/api/rows?db=main&table=t"] {
        let response = raw(
            fixture.addr(),
            &format!("POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n"),
        );
        assert_eq!(response.status, 405, "path {path}");
        assert_eq!(
            response.header("allow"),
            Some("GET, HEAD"),
            "the 405 must name the allowed methods"
        );
    }
    // A DELETE is refused the same way.
    let response = raw(
        fixture.addr(),
        "DELETE /api/databases HTTP/1.1\r\nHost: localhost\r\n\r\n",
    );
    assert_eq!(response.status, 405);
}

#[test]
fn a_head_request_works() {
    let fixture = Fixture::start(store(), options());
    let response = raw(
        fixture.addr(),
        "HEAD / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    );
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty(), "HEAD must not carry a body");
}

// ---------- the limits ----------

#[test]
fn an_oversized_header_is_refused() {
    let fixture = Fixture::start(store(), options());
    let huge = "x".repeat(64 * 1024);
    let response = raw(
        fixture.addr(),
        &format!("GET / HTTP/1.1\r\nHost: localhost\r\nX-Huge: {huge}\r\n\r\n"),
    );
    assert!(
        response.status == 0 || response.status >= 400,
        "an oversized header must be refused, got {}",
        response.status
    );
}

#[test]
fn an_oversized_body_is_refused_without_being_read() {
    let fixture = Fixture::start(store(), options());
    // The console never reads a body: a request that declares a huge one is
    // refused at the edge, immediately, without buffering it.
    let response = raw(
        fixture.addr(),
        "POST /api/rows HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100000000\r\n\r\n",
    );
    assert_eq!(response.status, 413, "body: {}", response.body);
}

#[test]
fn a_slow_client_cannot_pin_a_worker() {
    let mut opts = options();
    opts.read_timeout = Duration::from_millis(500);
    opts.workers = 2;
    let fixture = Fixture::start(store(), opts);

    // A client that sends a partial request head and stalls.
    let mut slow = TcpStream::connect(fixture.addr()).expect("connect");
    slow.set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    slow.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
        .expect("write the partial head");
    slow.flush().expect("flush");

    // The console keeps serving: the slow client does not hold the server.
    let started = Instant::now();
    let response = get(fixture.addr(), "/api/databases");
    assert_eq!(response.status, 200);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the console stalled behind the slow client"
    );

    // The slow client is dropped once the read timeout expires.
    let mut buf = Vec::new();
    let _ = slow.read_to_end(&mut buf);
    assert!(
        buf.is_empty(),
        "the slow connection should have been closed, got {} bytes",
        buf.len()
    );

    // And the console still works afterwards.
    assert_eq!(get(fixture.addr(), "/api/databases").status, 200);
}

#[test]
fn path_traversal_attempts_are_refused() {
    let fixture = Fixture::start(store(), options());
    for path in [
        "/../../etc/passwd",
        "/assets/../../etc/passwd",
        "/%2e%2e%2fetc%2fpasswd",
        "/api/../../../etc/passwd",
        "/..%2f..%2fetc%2fpasswd",
    ] {
        let response = get(fixture.addr(), path);
        assert!(
            response.status == 404 || response.status == 400,
            "path {path} returned {}",
            response.status
        );
        assert!(
            !response.body.contains("root:"),
            "path {path} leaked a file: {}",
            response.body
        );
    }
}

// ---------- the exposure policy ----------

#[test]
fn a_non_loopback_bind_is_refused() {
    let err = UiServer::bind("0.0.0.0:0", store(), options()).expect_err("must be refused");
    let message = err.to_string();
    assert!(
        message.contains("loopback"),
        "the error must explain the policy: {message}"
    );
    // The policy function itself agrees.
    let err = pinkdb_server_ui::check_bind_policy(
        "192.0.2.1".parse().expect("an address"),
        pinkdb_server_ui::ConsoleTls::Absent,
    )
    .expect_err("must be refused");
    assert!(err.to_string().contains("loopback"));
}

#[test]
fn a_store_with_users_refuses_to_start_unauthenticated() {
    let db = store();
    let mut conn = db.connect();
    conn.execute("CREATE USER alice PASSWORD 'secret'")
        .expect("create the user");
    drop(conn);

    let err = UiServer::bind("127.0.0.1:0", db.clone(), options()).expect_err("must be refused");
    let message = err.to_string();
    assert!(
        message.contains("--ui-auth basic") || message.contains("--ui-token"),
        "the error must name the way out: {message}"
    );

    // With an explicit credential the console starts.
    let opts = UiOptions {
        auth: UiAuth::Basic,
        ..options()
    };
    let server = UiServer::bind("127.0.0.1:0", db, opts).expect("bind with basic auth");
    let addr = server.local_addr();
    let handle = server.spawn().expect("spawn");
    // Without credentials: refused.
    let response = get(addr, "/api/databases");
    assert_eq!(response.status, 401);
    assert_eq!(
        response.header("www-authenticate"),
        Some("Basic realm=\"pinkdb\""),
        "the 401 must challenge the browser"
    );
    // With the right credentials: served.
    let encoded = base64("alice:secret");
    let response = get_with(
        addr,
        "/api/databases",
        &[("Authorization", &format!("Basic {encoded}"))],
    );
    assert_eq!(response.status, 200, "body: {}", response.body);
    handle.shutdown();
}

#[test]
fn basic_auth_rejects_a_wrong_password() {
    let db = store();
    let mut conn = db.connect();
    conn.execute("CREATE USER alice PASSWORD 'secret'")
        .expect("create the user");
    conn.execute("GRANT SELECT ON TABLE main.t TO alice")
        .expect("grant the select privilege");
    drop(conn);

    let opts = UiOptions {
        auth: UiAuth::Basic,
        ..options()
    };
    let server = UiServer::bind("127.0.0.1:0", db, opts).expect("bind");
    let addr = server.local_addr();
    let handle = server.spawn().expect("spawn");

    let wrong = base64("alice:wrong");
    let response = get_with(
        addr,
        "/api/rows?db=main&table=t",
        &[("Authorization", &format!("Basic {wrong}"))],
    );
    assert_eq!(response.status, 401, "body: {}", response.body);

    let right = base64("alice:secret");
    let response = get_with(
        addr,
        "/api/rows?db=main&table=t",
        &[("Authorization", &format!("Basic {right}"))],
    );
    assert_eq!(response.status, 200, "body: {}", response.body);
    handle.shutdown();
}

#[test]
fn a_bearer_token_guards_the_api() {
    let opts = UiOptions {
        auth: UiAuth::Token("s3cret".to_string()),
        ..options()
    };
    let server = UiServer::bind("127.0.0.1:0", store(), opts).expect("bind");
    let addr = server.local_addr();
    let handle = server.spawn().expect("spawn");

    // Without the token: refused.
    assert_eq!(get(addr, "/api/databases").status, 401);
    // With the wrong token: refused.
    assert_eq!(
        get_with(addr, "/api/databases", &[("Authorization", "Bearer nope")]).status,
        401
    );
    // With the token: served.
    let response = get_with(
        addr,
        "/api/databases",
        &[("Authorization", "Bearer s3cret")],
    );
    assert_eq!(response.status, 200, "body: {}", response.body);
    handle.shutdown();
}

// ---------- the read-only guarantee ----------

#[test]
fn browsing_does_not_change_the_store() {
    let db = store();
    let before = snapshot(&db);
    let fixture = Fixture::start(db.clone(), options());

    // Browse everything the console exposes.
    for path in [
        "/",
        "/preview?db=main&table=t&page=1&page_size=2",
        "/api/databases",
        "/api/schemas?db=main",
        "/api/tables?db=main",
        "/api/rows?db=main&table=t&page=1&page_size=2",
        "/api/rows?db=main&table=t&page=2&page_size=2",
        "/api/stats?db=main",
    ] {
        assert_eq!(get(fixture.addr(), path).status, 200, "path {path}");
    }

    let after = snapshot(&db);
    assert_eq!(before, after, "browsing changed the store");
}

/// The tables and their row counts of a store.
fn snapshot(db: &Database) -> Vec<(String, i64)> {
    let mut conn = db.connect();
    let results = conn.execute("SHOW TABLES").expect("show the tables");
    let mut out = Vec::new();
    for result in results {
        if let QueryResult::Selected { rows, .. } = result {
            for row in rows {
                let name = match row.first() {
                    Some(pinkdb::Value::Varchar(name)) => name.clone(),
                    _ => continue,
                };
                let count = conn
                    .execute(&format!("SELECT COUNT(*) FROM \"{name}\""))
                    .expect("count the rows")
                    .into_iter()
                    .find_map(|result| match result {
                        QueryResult::Selected { rows, .. } => rows.first().and_then(|row| {
                            row.first().and_then(|value| match value {
                                pinkdb::Value::Bigint(n) => Some(*n),
                                _ => None,
                            })
                        }),
                        _ => None,
                    })
                    .unwrap_or(-1);
                out.push((name, count));
            }
        }
    }
    out.sort();
    out
}

// ---------- the preview escaping ----------

#[test]
fn a_cell_that_carries_markup_is_escaped() {
    let fixture = Fixture::start(store(), options());
    let response = get(
        fixture.addr(),
        "/preview?db=main&table=t&page=1&page_size=10",
    );
    assert_eq!(response.status, 200);
    let body = response.body;
    assert!(
        !body.contains("<script>"),
        "the cell was rendered as markup: {body}"
    );
    assert!(
        body.contains("&lt;script&gt;"),
        "the cell was not escaped: {body}"
    );
    // The API keeps the raw text (JSON is not HTML).
    let api = get(
        fixture.addr(),
        "/api/rows?db=main&table=t&page=1&page_size=10",
    );
    assert!(
        api.body.contains("<script>"),
        "the API must keep the raw text"
    );
}

#[test]
fn a_null_cell_renders_as_null() {
    let fixture = Fixture::start(store(), options());
    let body = get(
        fixture.addr(),
        "/api/rows?db=main&table=t&page=1&page_size=10",
    )
    .json();
    let rows = body["rows"].as_array().expect("rows");
    let nulls: Vec<&str> = rows
        .iter()
        .filter(|row| row[1]["null"] == serde_json::Value::Bool(true))
        .map(|row| row[1]["text"].as_str().expect("a cell"))
        .collect();
    assert_eq!(nulls, vec!["NULL"]);
}

// ---------- the worker model ----------

#[test]
fn the_engine_runs_on_the_bounded_blocking_pool() {
    let _hook_lock = HOOK_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // The engine hook observes the thread the engine runs on and how many
    // engine calls are in flight at once; the reactor hook observes the
    // thread the HTTP layer runs on. The engine must never run on the
    // runtime's thread, and the pool's size is the configured bound.
    //
    // The engine hook only counts this test's statements (the ones that name
    // the `probe` table), so the other tests running in parallel cannot
    // influence the measurement.
    let engine_threads: Arc<Mutex<Vec<ThreadId>>> = Arc::new(Mutex::new(Vec::new()));
    let reactor_threads: Arc<Mutex<Vec<ThreadId>>> = Arc::new(Mutex::new(Vec::new()));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let hook_threads = Arc::clone(&engine_threads);
    let hook_active = Arc::clone(&active);
    let hook_peak = Arc::clone(&peak);
    *pinkdb_server_ui::ENGINE_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(move |sql| {
        if !sql.contains("probe") {
            return;
        }
        hook_threads
            .lock()
            .unwrap()
            .push(std::thread::current().id());
        let now = hook_active.fetch_add(1, Ordering::AcqRel) + 1;
        hook_peak.fetch_max(now, Ordering::AcqRel);
        std::thread::sleep(Duration::from_millis(40));
        hook_active.fetch_sub(1, Ordering::AcqRel);
    }));
    let hook_reactor = Arc::clone(&reactor_threads);
    *pinkdb_server_ui::REACTOR_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(move || {
        hook_reactor
            .lock()
            .unwrap()
            .push(std::thread::current().id());
    }));

    let db = store();
    {
        let mut conn = db.connect();
        conn.execute("CREATE TABLE probe (id INTEGER)")
            .expect("create the probe table");
        conn.execute("INSERT INTO probe VALUES (1), (2), (3)")
            .expect("insert the probe rows");
    }
    let opts = UiOptions {
        max_blocking_threads: 2,
        ..options()
    };
    let fixture = Fixture::start(db, opts);

    // Four concurrent requests: the engine calls must not exceed the pool.
    let addr = fixture.addr();
    let handles: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(move || {
                let response = get(addr, "/api/rows?db=main&table=probe&page=1&page_size=2");
                assert_eq!(response.status, 200);
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("the request thread");
    }

    *pinkdb_server_ui::ENGINE_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    *pinkdb_server_ui::REACTOR_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;

    let engine = engine_threads.lock().unwrap().clone();
    let reactor = reactor_threads.lock().unwrap().clone();
    assert!(!engine.is_empty(), "the engine hook never ran");
    assert!(!reactor.is_empty(), "the reactor hook never ran");
    for id in &engine {
        assert!(
            !reactor.contains(id),
            "the engine ran on the runtime's thread {id:?}"
        );
    }
    let peak = peak.load(Ordering::Acquire);
    assert!(
        peak <= 2,
        "more engine calls ran at once than the pool allows: {peak}"
    );
}

#[test]
fn a_slow_engine_call_does_not_block_the_console() {
    let _hook_lock = HOOK_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // The engine's work runs on the blocking pool, so a slow statement
    // cannot stall the runtime: the console keeps answering while the
    // engine is busy.
    let hook = Arc::new(AtomicUsize::new(0));
    let hook_counter = Arc::clone(&hook);
    *pinkdb_server_ui::ENGINE_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(move |sql| {
        if sql.contains("probe") {
            hook_counter.fetch_add(1, Ordering::AcqRel);
            std::thread::sleep(Duration::from_millis(900));
        }
    }));

    let db = store();
    {
        let mut conn = db.connect();
        conn.execute("CREATE TABLE probe (id INTEGER)")
            .expect("create the probe table");
        conn.execute("INSERT INTO probe VALUES (1)")
            .expect("insert the probe rows");
    }
    let opts = UiOptions {
        // One runtime thread: if the engine ran on the runtime, the console
        // would stall completely.
        workers: 1,
        max_blocking_threads: 2,
        ..options()
    };
    let fixture = Fixture::start(db, opts);
    let addr = fixture.addr();

    let busy = std::thread::spawn(move || {
        let response = get(addr, "/api/rows?db=main&table=probe&page=1&page_size=2");
        assert_eq!(response.status, 200);
    });

    // While the engine is busy, an asset request must be served at once.
    std::thread::sleep(Duration::from_millis(150));
    let started = Instant::now();
    let response = get(fixture.addr(), "/assets/console.css");
    assert_eq!(response.status, 200);
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(600),
        "the console stalled behind the engine call: {elapsed:?}"
    );

    busy.join().expect("the busy request thread");
    *pinkdb_server_ui::ENGINE_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
}

/// base64-encode a credential.
fn base64(input: &str) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((triple >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((triple >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(triple & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}
