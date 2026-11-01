//! The write lease: the real Go service's recording
//! (`fixtures/go-lease-reference.json`) replayed in order, then the races it
//! cannot show — a statement drained by `acquire`, a drain that times out, a
//! TTL, and a release racing that TTL — plus the gate's edges.
//!
//! The sandbox half is only what pinkdb has: `LOAD`/`INSTALL` are refused.
//! pinkdb has no file-read roots (`ServePolicy.allow_files` is a single bool)
//! and no configuration lock, so a read outside a sandbox root cannot be
//! refused here; see `src/sandbox.rs`.

use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use pinkdb::Database;
use pinkdb_quacklab::{QuacklabHandle, QuacklabOptions, QuacklabServer};
use serde_json::{Value, json};

const TOKEN: &str = "lease-secret";
const WRONG_ID: &str = "00000000000000000000000000000000";

/// Holds the store's write lock long enough for a drain to wait on it and,
/// with a short drain timeout, long enough to defeat that drain.
const SLOW_WRITE: &str = "INSERT INTO t SELECT count(*) FROM range(60000000)";

fn options() -> QuacklabOptions {
    QuacklabOptions {
        sql_token: TOKEN.to_string(),
        ..QuacklabOptions::default()
    }
}

fn bearer() -> String {
    format!("Bearer {TOKEN}")
}

fn start(options: QuacklabOptions) -> (QuacklabHandle, SocketAddr) {
    start_with(Database::new(), options)
}

fn start_with(db: Database, options: QuacklabOptions) -> (QuacklabHandle, SocketAddr) {
    let server = QuacklabServer::bind("127.0.0.1:0", db, options).expect("bind");
    let addr = server.local_addr();
    (server.spawn().expect("spawn"), addr)
}

fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .expect("a read timeout");
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    if let Some(body) = body {
        head.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).expect("write the head");
    if let Some(body) = body {
        stream.write_all(body.as_bytes()).expect("write the body");
    }
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("read the response");
    let text = String::from_utf8(response).expect("a UTF-8 response");
    let (head, body) = text.split_once("\r\n\r\n").expect("a whole response");
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
        .expect("a status code");
    (status, body.to_string())
}

/// One call; `authorization` is the header verbatim (`None` sends none).
fn call(
    addr: SocketAddr,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    body: Option<&str>,
) -> (u16, Value) {
    let headers: Vec<(&str, &str)> = match authorization {
        Some(value) => vec![("Authorization", value)],
        None => Vec::new(),
    };
    let (status, text) = request(addr, method, path, &headers, body);
    let body = serde_json::from_str(&text).unwrap_or(Value::Null);
    (status, body)
}

fn post(addr: SocketAddr, path: &str, body: &Value) -> (u16, Value) {
    call(addr, "POST", path, Some(&bearer()), Some(&body.to_string()))
}

fn health(addr: SocketAddr) -> (u16, Value) {
    call(addr, "GET", "/health", None, None)
}

fn wait_for_health(addr: SocketAddr, what: &str, mut matches: impl FnMut(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let (status, body) = health(addr);
        assert_eq!(status, 200, "{what}: {body}");
        if matches(&body) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}; the last answer was {body}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn the_go_lease_recording_replays() {
    let (_handle, addr) = start(options());
    let cases: Value = serde_json::from_str(include_str!("fixtures/go-lease-reference.json"))
        .expect("the recording");
    let cases = cases.as_array().expect("an array of cases");
    assert_eq!(cases.len(), 18);
    let mut lease = String::new();
    for case in cases {
        let step = case["step"].as_str().expect("a step name");
        let method = case["method"].as_str().unwrap_or("POST");
        let path = case["path"].as_str().unwrap_or("/write-lease/acquire");
        let authorization = match case.get("auth").and_then(Value::as_bool) {
            Some(false) => None,
            // The recording's "wrong token" step sends a token that is not
            // the configured one.
            _ if step.contains("wrong token") => Some("Bearer not-the-token".to_string()),
            _ => Some(bearer()),
        };
        // A recorded `null` body means the request carried none.
        let body = case.get("body").filter(|body| !body.is_null()).map(|body| {
            // The recording's "wrong id" and "unknown" steps sent an id that
            // was not the active lease's; the mask hides which.
            substitute(
                body,
                &lease,
                step.contains("wrong id") || step.contains("unknown"),
            )
            .to_string()
        });
        let (status, response) = call(
            addr,
            method,
            path,
            authorization.as_deref(),
            body.as_deref(),
        );
        assert_eq!(
            status,
            case["status"].as_u64().expect("a status") as u16,
            "{step}: {response}"
        );
        assert_masked(&case["response"], &response, step);
        if status == 200 && path == "/write-lease/acquire" {
            lease = response["leaseId"].as_str().unwrap_or_default().to_string();
        }
    }
    // The last acquire (step 16) was ended by `/recover` (step 17).
    let (status, body) = post(addr, "/write-lease/release", &json!({ "leaseId": lease }));
    assert_eq!(status, 404, "{body}");
    assert_eq!(body["error"]["code"], "unknown_lease");
}

/// Replaces the recording's id mask with the lease the replay is holding.
fn substitute(value: &Value, lease: &str, wrong: bool) -> Value {
    match value {
        Value::String(text) if text == "<lease>" => {
            Value::String(if wrong { WRONG_ID } else { lease }.to_string())
        }
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), substitute(value, lease, wrong)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| substitute(item, lease, wrong))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn assert_masked(expected: &Value, actual: &Value, step: &str) {
    match expected {
        Value::String(mask) if mask == "<ms>" => {
            actual
                .as_u64()
                .unwrap_or_else(|| panic!("{step}: expected milliseconds, got {actual}"));
        }
        Value::String(mask) if mask == "<rfc3339>" => {
            let text = actual
                .as_str()
                .unwrap_or_else(|| panic!("{step}: expected a timestamp, got {actual}"));
            assert!(is_rfc3339(text), "{step}: {text} is not RFC 3339");
        }
        Value::String(mask) if mask == "<id:32 hex>" => {
            let text = actual
                .as_str()
                .unwrap_or_else(|| panic!("{step}: expected an id, got {actual}"));
            let hex = text.len() == 32
                && text
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
            assert!(hex, "{step}: {text} is not 32 lowercase hex chars");
        }
        Value::String(mask) if mask == "<..>" => {
            assert!(!actual.is_null(), "{step}: expected a value, got null");
        }
        Value::Object(expected_fields) => {
            let actual_fields = actual
                .as_object()
                .unwrap_or_else(|| panic!("{step}: expected an object, got {actual}"));
            let mut expected_keys: Vec<&String> = expected_fields.keys().collect();
            let mut actual_keys: Vec<&String> = actual_fields.keys().collect();
            expected_keys.sort();
            actual_keys.sort();
            assert_eq!(expected_keys, actual_keys, "{step}: key sets");
            for (key, expected_value) in expected_fields {
                assert_masked(expected_value, &actual_fields[key], step);
            }
        }
        Value::Array(expected_items) => {
            let actual_items = actual
                .as_array()
                .unwrap_or_else(|| panic!("{step}: expected an array, got {actual}"));
            assert_eq!(
                expected_items.len(),
                actual_items.len(),
                "{step}: array length"
            );
            for (expected_item, actual_item) in expected_items.iter().zip(actual_items) {
                assert_masked(expected_item, actual_item, step);
            }
        }
        other => assert_eq!(other, actual, "{step}"),
    }
}

fn is_rfc3339(text: &str) -> bool {
    let bytes = text.as_bytes();
    let digits = |range: std::ops::Range<usize>| {
        text.get(range)
            .is_some_and(|part| part.bytes().all(|byte| byte.is_ascii_digit()))
    };
    bytes.len() >= 20
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':'
        && text.ends_with('Z')
        && digits(0..4)
        && digits(5..7)
        && digits(8..10)
        && digits(11..13)
        && digits(14..16)
        && digits(17..19)
        && (bytes.len() == 20 || bytes[19] == b'.')
}

#[test]
fn acquire_drains_the_statement_in_flight() {
    let (_handle, addr) = start(options());
    let (status, body) = post(addr, "/sql", &json!({ "sql": "CREATE TABLE t(a BIGINT)" }));
    assert_eq!(status, 200, "{body}");
    let slow = std::thread::spawn(move || post(addr, "/sql", &json!({ "sql": SLOW_WRITE })));
    wait_for_health(addr, "the write to take the store", |body| {
        body["writerBusy"] == json!(true)
    });
    let acquiring = std::thread::spawn(move || {
        post(addr, "/write-lease/acquire", &json!({ "owner": "drain" }))
    });
    // The lease is installed before its drain starts; until then health
    // reports the running writer instead.
    wait_for_health(addr, "writeLeased", |body| {
        body["writeLeased"] == json!(true)
    });
    // New statements are refused at once instead of queueing behind the one
    // being drained.
    let refused_at = Instant::now();
    let (status, body) = post(addr, "/sql", &json!({ "sql": "SELECT 1" }));
    assert_eq!(status, 423, "{body}");
    assert_eq!(body["error"]["code"], "warehouse_write_leased");
    assert!(
        refused_at.elapsed() < Duration::from_secs(2),
        "the refusal waited behind the writer: {:?}",
        refused_at.elapsed()
    );
    let (status, grant) = acquiring.join().expect("the acquire thread");
    assert_eq!(status, 200, "{grant}");
    assert!(
        grant["drainMs"].as_u64().unwrap_or(0) >= 1,
        "the acquire waited for the statement: {grant}"
    );
    let (status, slow_body) = slow.join().expect("the slow thread");
    assert_eq!(
        status, 200,
        "the drained statement finished normally: {slow_body}"
    );
    assert_eq!(slow_body["rowsAffected"], 1);
    // The holder's own statement runs; `/sql` stays closed.
    let lease_id = grant["leaseId"].as_str().expect("an id");
    let holder = json!({ "leaseId": lease_id, "sql": "INSERT INTO t VALUES (7)" });
    let (status, body) = post(addr, "/write-lease/sql", &holder);
    assert_eq!(status, 200, "{body}");
    let (status, body) = post(addr, "/sql", &json!({ "sql": "SELECT 1" }));
    assert_eq!(status, 423, "{body}");
    let (status, body) = post(
        addr,
        "/write-lease/release",
        &json!({ "leaseId": lease_id }),
    );
    assert_eq!(status, 200, "{body}");
    let (status, body) = post(
        addr,
        "/sql",
        &json!({ "sql": "SELECT count(*) AS n FROM t" }),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rows"][0][0], 2);
}

#[test]
fn a_drain_timeout_abandons_the_lease() {
    let mut options = options();
    options.lease_drain_timeout = Duration::from_millis(100);
    let (_handle, addr) = start(options);
    let (status, body) = post(addr, "/sql", &json!({ "sql": "CREATE TABLE t(a BIGINT)" }));
    assert_eq!(status, 200, "{body}");
    let slow = std::thread::spawn(move || post(addr, "/sql", &json!({ "sql": SLOW_WRITE })));
    // The statement runs for seconds; a 100 ms drain cannot outlast it.
    std::thread::sleep(Duration::from_millis(200));
    let (status, body) = post(addr, "/write-lease/acquire", &json!({ "owner": "timeout" }));
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["error"]["code"], "lease_drain_failed");
    // The aborted lease never touches the warehouse: admission is open again
    // and the statement that defeated the drain finishes normally.
    let (status, body) = health(addr);
    assert_eq!(status, 200);
    assert!(
        body.get("writeLeased").is_none(),
        "the abandoned lease is gone: {body}"
    );
    let (status, body) = post(addr, "/sql", &json!({ "sql": "SELECT 1" }));
    assert_eq!(status, 200, "{body}");
    let (status, body) = slow.join().expect("the slow thread");
    assert_eq!(status, 200, "{body}");
}

#[test]
fn a_lease_expires_and_the_warehouse_reopens() {
    let mut options = options();
    // The clamp's lower bound is 30 s; this is the test hook that shortens it
    // so expiry can be observed without sleeping that long.
    options.lease_ttl_floor = Duration::from_millis(200);
    let (_handle, addr) = start(options);
    let (status, grant) = post(
        addr,
        "/write-lease/acquire",
        &json!({ "owner": "ttl", "ttlSeconds": 1 }),
    );
    assert_eq!(status, 200, "{grant}");
    assert_eq!(grant["ttlSeconds"], 1);
    let (status, _) = post(addr, "/sql", &json!({ "sql": "SELECT 1" }));
    assert_eq!(status, 423);
    wait_for_health(addr, "the expiry", |body| {
        body["ok"] == json!(true) && body.get("writeLeased").is_none()
    });
    let lease_id = grant["leaseId"].as_str().expect("an id");
    let (status, body) = post(
        addr,
        "/write-lease/release",
        &json!({ "leaseId": lease_id }),
    );
    assert_eq!(status, 404, "the expired lease is unknown: {body}");
    assert_eq!(body["error"]["code"], "unknown_lease");
    let (status, body) = post(addr, "/sql", &json!({ "sql": "SELECT 1" }));
    assert_eq!(status, 200, "{body}");
}

#[test]
fn release_and_expiry_end_the_lease_once() {
    let mut options = options();
    options.lease_ttl_floor = Duration::from_millis(200);
    let (_handle, addr) = start(options);
    let (status, grant) = post(
        addr,
        "/write-lease/acquire",
        &json!({ "owner": "race", "ttlSeconds": 1 }),
    );
    assert_eq!(status, 200, "{grant}");
    let lease_id = grant["leaseId"].as_str().expect("an id").to_string();
    // Line the releases up with the TTL the lease was armed for.
    std::thread::sleep(Duration::from_millis(995));
    let releases: Vec<_> = (0..8)
        .map(|_| {
            let lease_id = lease_id.clone();
            std::thread::spawn(move || {
                post(
                    addr,
                    "/write-lease/release",
                    &json!({ "leaseId": lease_id }),
                )
            })
        })
        .collect();
    let answers: Vec<(u16, Value)> = releases
        .into_iter()
        .map(|release| release.join().expect("a release thread"))
        .collect();
    let statuses: Vec<u16> = answers.iter().map(|(status, _)| *status).collect();
    let released = statuses.iter().filter(|status| **status == 200).count();
    assert!(released <= 1, "the lease ended more than once: {answers:?}");
    assert!(
        statuses.iter().all(|status| matches!(status, 200 | 404)),
        "{answers:?}"
    );
    // Whichever won, the lease is gone exactly once and a new one is free.
    let (status, body) = post(
        addr,
        "/write-lease/release",
        &json!({ "leaseId": lease_id }),
    );
    assert_eq!(status, 404, "{body}");
    let (status, body) = post(addr, "/sql", &json!({ "sql": "SELECT 1" }));
    assert_eq!(status, 200, "{body}");
    let (status, grant) = post(addr, "/write-lease/acquire", &json!({ "owner": "next" }));
    assert_eq!(status, 200, "{grant}");
    let (status, body) = post(
        addr,
        "/write-lease/release",
        &json!({ "leaseId": grant["leaseId"] }),
    );
    assert_eq!(status, 200, "{body}");
}

#[test]
fn a_read_only_warehouse_has_no_lease_routes() {
    let mut options = options();
    options.read_only = true;
    let (_handle, addr) = start(options);
    for (path, body) in [
        ("/write-lease/acquire", json!({ "owner": "x" })),
        ("/write-lease/renew", json!({ "leaseId": WRONG_ID })),
        ("/write-lease/release", json!({ "leaseId": WRONG_ID })),
        (
            "/write-lease/sql",
            json!({ "leaseId": WRONG_ID, "sql": "SELECT 1" }),
        ),
    ] {
        let (status, _) = post(addr, path, &body);
        assert_eq!(status, 404, "{path} is hidden in read-only mode");
    }
    let (status, body) = post(addr, "/sql", &json!({ "sql": "SELECT 1" }));
    assert_eq!(status, 200, "{body}");
}

#[test]
fn lease_auth_matches_the_go_service() {
    // No token configured: plain SQL is open, so the lease surface says why it
    // will not run instead of accepting anyone.
    let (_handle, addr) = start(QuacklabOptions::default());
    let (status, body) = call(
        addr,
        "POST",
        "/write-lease/acquire",
        None,
        Some(r#"{"owner":"x"}"#),
    );
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["error"]["code"], "lease_auth_unavailable");
    assert_eq!(
        body["error"]["message"],
        "write lease authentication is not configured"
    );
    let (status, body) = call(addr, "POST", "/sql", None, Some(r#"{"sql":"SELECT 1"}"#));
    assert_eq!(status, 200, "{body}");
    let (status, body) = call(
        addr,
        "POST",
        "/sql",
        Some("Bearer anything"),
        Some(r#"{"sql":"SELECT 1"}"#),
    );
    assert_eq!(status, 200, "an unconfigured token accepts any: {body}");

    // A configured token: a wrong one is 401, the right one is accepted.
    let (_handle, addr) = start(options());
    let (status, body) = call(
        addr,
        "POST",
        "/write-lease/acquire",
        Some("Bearer not-the-token"),
        Some(r#"{"owner":"x"}"#),
    );
    assert_eq!(status, 401, "{body}");
    assert_eq!(body["error"]["code"], "unauthorized");
    assert_eq!(body["error"]["message"], "bearer token required");
    // The body cap: a truncated document is bad JSON, not a defaults request.
    let padded = format!(r#"{{"owner":"{}"}}"#, "x".repeat(5000));
    let (status, body) = call(
        addr,
        "POST",
        "/write-lease/acquire",
        Some(&bearer()),
        Some(&padded),
    );
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"]["code"], "bad_json");
    let (status, grant) = post(addr, "/write-lease/acquire", &json!({ "owner": "x" }));
    assert_eq!(status, 200, "{grant}");

    // Serverless mode skips both token checks, as it does for `/sql`.
    let mut options = options();
    options.iam_trust = true;
    options.sql_token = String::new();
    let (_handle, addr) = start(options);
    let (status, grant) = call(
        addr,
        "POST",
        "/write-lease/acquire",
        None,
        Some(r#"{"owner":"x"}"#),
    );
    assert_eq!(status, 200, "{grant}");
}

#[test]
fn a_non_loopback_peer_cannot_hold_a_lease() {
    let Some(ip) = non_loopback_ip() else {
        // The peer check is also unit-tested in `src/app.rs`.
        eprintln!("no non-loopback address on this machine; skipped the bound check");
        return;
    };
    let server = QuacklabServer::bind((ip, 0), Database::new(), options()).expect("bind");
    let addr = server.local_addr();
    let _handle = server.spawn().expect("spawn");
    let (status, body) = post(addr, "/write-lease/acquire", &json!({ "owner": "remote" }));
    assert_eq!(status, 403, "{body}");
    assert_eq!(body["error"]["code"], "loopback_only");
    assert_eq!(
        body["error"]["message"],
        "write lease control is loopback-only"
    );
    // `/sql` is not local-only, only the lease's control surface is.
    let (status, body) = post(addr, "/sql", &json!({ "sql": "SELECT 1" }));
    assert_eq!(status, 200, "{body}");
}

fn non_loopback_ip() -> Option<IpAddr> {
    // A UDP "connect" only picks a route; it sends nothing.
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("192.0.2.1:9").ok()?;
    let ip = socket.local_addr().ok()?.ip();
    (!ip.is_loopback() && !ip.is_unspecified()).then_some(ip)
}

#[test]
fn the_sandbox_refuses_load_and_install() {
    let db = Database::new();
    db.set_allow_sql_load_extension(true);
    // Off: the embedder's choice stands, as in the Go service.
    let mut open = options();
    open.sandbox = false;
    let (handle, addr) = start_with(db.clone(), open);
    let (status, body) = post(
        addr,
        "/sql",
        &json!({ "sql": "LOAD EXTENSION 'no-such-extension'" }),
    );
    assert_eq!(status, 400, "{body}");
    assert!(
        !body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("is disabled on this connection"),
        "the gate is open when the sandbox is off: {body}"
    );
    drop(handle);

    let mut sandboxed = options();
    sandboxed.sandbox = true;
    let (_handle, addr) = start_with(db, sandboxed);
    let (status, body) = post(
        addr,
        "/sql",
        &json!({ "sql": "LOAD EXTENSION 'no-such-extension'" }),
    );
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"]["code"], "sql_error");
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("is disabled on this connection"),
        "LOAD is refused by the gate: {body}"
    );
    let (status, body) = post(addr, "/sql", &json!({ "sql": "INSTALL nobody" }));
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"]["code"], "sql_error");
}
