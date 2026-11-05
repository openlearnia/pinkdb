//! The QuackLab contract: replay the real Go service's answers
//! (`fixtures/go-reference.json`, see `fixtures/README.md`) against the
//! adapter, in order, on one in-memory warehouse, then the behaviours the
//! recording does not cover: the token and IAM modes, the row cap's edges, a
//! transaction a script leaves open, `/health`, `/recover` and malformed
//! bodies.
//!
//! Where pinkdb cannot reproduce a body exactly, the case is listed below and
//! asserted on status, error code and DuckDB's prefix instead.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use pinkdb::Database;
use pinkdb_quacklab::{QuacklabHandle, QuacklabOptions, QuacklabServer};
use serde_json::{Value, json};

/// Recorded answers pinkdb cannot reproduce body-for-body.
///
/// - 4: the statement uses `TIME`, which pinkdb's parser and
///   type system do not have, so it answers 400 where DuckDB answered 200.
///   (Cell encoding is unit-tested in `src/encode.rs`.)
/// - 15, 16, 17: the error's wording after DuckDB's prefix is pinkdb's
///   ("table \"nope\" not found", the parser's caret, "cannot CAST …"), while
///   DuckDB's own text is the recording's.
const BODY_DEVIATIONS: &[usize] = &[4, 15, 16, 17];

/// Recorded answers checked field by field: the 5,000-row answer keeps only
/// its first rows in the fixture.
const SHORTENED: usize = 18;

fn start(options: QuacklabOptions) -> (QuacklabHandle, SocketAddr) {
    let server = QuacklabServer::bind("127.0.0.1:0", Database::new(), options).expect("bind");
    let addr = server.local_addr();
    (server.spawn().expect("spawn"), addr)
}

fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&[u8]>,
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
        stream.write_all(body).expect("write the body");
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

/// `POST /sql` with a JSON body that the test itself escapes.
fn sql(addr: SocketAddr, statement: &str) -> (u16, Value) {
    let body = json!({ "sql": statement }).to_string();
    let (status, text) = request(addr, "POST", "/sql", &[], Some(body.as_bytes()));
    (status, serde_json::from_str(&text).expect("a JSON body"))
}

fn strip_timing(body: &mut Value) {
    let Some(fields) = body.as_object_mut() else {
        return;
    };
    for key in [
        "waitMs",
        "gateWaitMs",
        "poolWaitMs",
        "setupMs",
        "execMs",
        "rowScanMs",
        "tookMs",
        "serializationMs",
    ] {
        fields.remove(key);
    }
}

#[test]
fn the_go_reference_replays() {
    let (_handle, addr) = start(QuacklabOptions::default());
    let cases: Value =
        serde_json::from_str(include_str!("fixtures/go-reference.json")).expect("the fixture");
    let cases = cases.as_array().expect("an array of cases");
    assert_eq!(cases.len(), 21);
    for (index, case) in cases.iter().enumerate() {
        let statement = case["sql"].as_str().expect("a sql string");
        let body = if statement == "<no body>" {
            None
        } else {
            Some(json!({ "sql": statement }).to_string().into_bytes())
        };
        let (status, text) = request(addr, "POST", "/sql", &[], body.as_deref());
        if BODY_DEVIATIONS.contains(&index) {
            let actual: Value = serde_json::from_str(&text).expect("a JSON body");
            assert_error(&actual, "sql_error");
            // Case 4 cannot even parse here, so its status differs too.
            let expected: u16 = if index == 4 {
                400
            } else {
                case["status"].as_u64().unwrap() as u16
            };
            assert_eq!(
                status, expected,
                "case {index} `{statement}` answered {text}"
            );
            let message = actual["error"]["message"].as_str().unwrap_or_default();
            let prefix = match index {
                4 => "Parser Error: ",
                15 => "Catalog Error: ",
                16 => "Parser Error: ",
                _ => "Conversion Error: ",
            };
            assert!(
                message.starts_with(prefix),
                "case {index}: {message:?} starts with {prefix:?}"
            );
            continue;
        }
        assert_eq!(
            status, case["status"],
            "case {index} `{statement}` answered {text}"
        );
        let mut actual: Value = serde_json::from_str(&text).expect("a JSON body");
        if index == SHORTENED {
            assert_shortened(&actual, &case["body"]);
            continue;
        }
        strip_timing(&mut actual);
        assert_eq!(actual, case["body"], "case {index} `{statement}`");
    }
}

/// The 5,005-row answer: the exact count, the flag, 5,000 rows, and the first
/// rows the recording kept.
fn assert_shortened(actual: &Value, recorded: &Value) {
    assert_eq!(actual["columns"], recorded["columns"]);
    assert_eq!(actual["rowCount"], json!(5005));
    assert_eq!(actual["truncated"], json!(true));
    assert_eq!(actual["rowsAffected"], json!(0));
    let rows = actual["rows"].as_array().expect("rows is an array");
    assert_eq!(rows.len(), 5000);
    assert_eq!(&rows[..3], &recorded["rows"].as_array().unwrap()[..3]);
    assert_eq!(rows[4999], json!([4999]));
}

fn assert_error(body: &Value, code: &str) {
    assert_eq!(body["error"]["code"], json!(code), "{body}");
    assert!(body["error"]["message"].is_string(), "{body}");
}

#[test]
fn the_token_gate() {
    let (_handle, addr) = start(QuacklabOptions {
        sql_token: "s3cret".to_string(),
        ..QuacklabOptions::default()
    });

    let body = json!({ "sql": "SELECT 1 AS one" }).to_string();
    let (status, text) = request(addr, "POST", "/sql", &[], Some(body.as_bytes()));
    assert_eq!(status, 401);
    assert_eq!(
        text,
        "{\"error\":{\"code\":\"unauthorized\",\"message\":\"bearer token required\"}}"
    );

    let (status, _) = request(
        addr,
        "POST",
        "/sql",
        &[("Authorization", "Bearer wrong")],
        Some(body.as_bytes()),
    );
    assert_eq!(status, 401);

    let (status, text) = request(
        addr,
        "POST",
        "/sql",
        &[("Authorization", "Bearer s3cret")],
        Some(body.as_bytes()),
    );
    assert_eq!(status, 200, "{text}");
    let (status, text) = request(
        addr,
        "POST",
        "/recover",
        &[("Authorization", "Bearer s3cret")],
        None,
    );
    assert_eq!(status, 200, "{text}");
    let (status, _) = request(addr, "POST", "/recover", &[], None);
    assert_eq!(status, 401);

    // Health stays open: a load balancer has no token.
    let (status, text) = request(addr, "GET", "/health", &[], None);
    assert_eq!(status, 200);
    assert_eq!(text, "{\"ok\":true}");
}

#[test]
fn iam_trust_skips_the_token() {
    let (_handle, addr) = start(QuacklabOptions {
        sql_token: "s3cret".to_string(),
        iam_trust: true,
        ..QuacklabOptions::default()
    });
    let (status, body) = sql(addr, "SELECT 7 AS seven");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rows"], json!([[7]]));
}

#[test]
fn the_row_cap_is_exact() {
    let (_handle, addr) = start(QuacklabOptions {
        row_limit: 3,
        ..QuacklabOptions::default()
    });
    let (status, body) = sql(addr, "SELECT i FROM range(10) t(i)");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rowCount"], json!(10));
    assert_eq!(body["truncated"], json!(true));
    assert_eq!(body["rows"], json!([[0], [1], [2]]));

    // Exactly at the cap: not truncated.
    let (status, body) = sql(addr, "SELECT i FROM range(3) t(i)");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rowCount"], json!(3));
    assert_eq!(body["truncated"], json!(false));
    assert_eq!(body["rows"].as_array().unwrap().len(), 3);
}

#[test]
fn a_cap_of_100000_keeps_every_row() {
    let (_handle, addr) = start(QuacklabOptions {
        row_limit: 100_000,
        ..QuacklabOptions::default()
    });
    let (status, body) = sql(addr, "SELECT i FROM range(5005) t(i)");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rowCount"], json!(5005));
    assert_eq!(body["truncated"], json!(false));
    assert_eq!(body["rows"].as_array().unwrap().len(), 5005);
}

#[test]
fn an_open_transaction_is_rolled_back() {
    let (_handle, addr) = start(QuacklabOptions::default());
    let (status, body) = sql(addr, "CREATE TABLE tx(a INT)");
    assert_eq!(status, 200, "{body}");
    let (status, body) = sql(addr, "INSERT INTO tx VALUES (1), (2)");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rowsAffected"], json!(2));

    // The script never commits: the rows must not outlive the request.
    let (status, body) = sql(
        addr,
        "BEGIN; INSERT INTO tx VALUES (3); INSERT INTO tx VALUES (4)",
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rowsAffected"], json!(1));

    let (status, body) = sql(addr, "SELECT count(*) FROM tx");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rows"], json!([[2]]));

    // An explicit COMMIT still works, and the next request sees its rows.
    let (status, body) = sql(addr, "BEGIN; INSERT INTO tx VALUES (5); COMMIT");
    assert_eq!(status, 200, "{body}");
    let (status, body) = sql(addr, "SELECT count(*) FROM tx");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rows"], json!([[3]]));
}

#[test]
fn health_and_recover() {
    let (_handle, addr) = start(QuacklabOptions::default());
    for path in ["/health", "/healthz"] {
        let (status, text) = request(addr, "GET", path, &[], None);
        assert_eq!(status, 200, "{path}");
        assert_eq!(text, "{\"ok\":true}", "{path}");
    }
    let (status, body) = request(addr, "POST", "/recover", &[], None);
    assert_eq!(status, 200);
    let body: Value = serde_json::from_str(&body).expect("a JSON body");
    assert_eq!(body["ok"], json!(true));
    assert!(body["reopenMs"].is_number(), "{body}");
}

#[test]
fn malformed_bodies_are_bad_json() {
    let (_handle, addr) = start(QuacklabOptions::default());

    let (status, text) = request(addr, "POST", "/sql", &[], Some(b""));
    assert_eq!(status, 400);
    assert_eq!(
        text,
        "{\"error\":{\"code\":\"bad_json\",\"message\":\"EOF\"}}"
    );

    let (status, text) = request(addr, "POST", "/sql", &[], Some(b"  \n"));
    assert_eq!(status, 400);
    assert_eq!(
        text,
        "{\"error\":{\"code\":\"bad_json\",\"message\":\"EOF\"}}"
    );

    // Go's decoder message for a truncated object; only the code is contract.
    let (status, text) = request(addr, "POST", "/sql", &[], Some(b"{\"sql\":"));
    assert_eq!(status, 400, "{text}");
    let body: Value = serde_json::from_str(&text).expect("a JSON body");
    assert_error(&body, "bad_json");

    for body in [&b"{}"[..], &b"{\"sql\":\"  \\n \"}"[..]] {
        let (status, text) = request(addr, "POST", "/sql", &[], Some(body));
        assert_eq!(status, 400, "{text}");
        assert_eq!(
            text,
            "{\"error\":{\"code\":\"empty_sql\",\"message\":\"sql required\"}}"
        );
    }

    let (status, text) = request(addr, "POST", "/sql", &[], Some(b"{\"sql\":1}"));
    assert_eq!(status, 400, "{text}");
    let body: Value = serde_json::from_str(&text).expect("a JSON body");
    assert_error(&body, "bad_json");
}

#[test]
fn a_failed_script_keeps_earlier_effects() {
    let (_handle, addr) = start(QuacklabOptions::default());
    let (status, body) = sql(
        addr,
        "CREATE TABLE half(a INT); INSERT INTO half VALUES (1)",
    );
    assert_eq!(status, 200, "{body}");
    // The second statement fails; the first one's row is still there.
    let (status, body) = sql(addr, "INSERT INTO half VALUES (2); SELECT * FROM nope");
    assert_eq!(status, 400, "{body}");
    assert_error(&body, "sql_error");
    let (status, body) = sql(addr, "SELECT count(*) FROM half");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rows"], json!([[2]]));
}

#[test]
fn a_read_only_warehouse_refuses_writes() {
    let (_handle, addr) = start(QuacklabOptions {
        read_only: true,
        ..QuacklabOptions::default()
    });
    let (status, body) = sql(addr, "SELECT 1 AS one");
    assert_eq!(status, 200, "{body}");
    let (status, body) = sql(addr, "CREATE TABLE nope(a INT)");
    assert_eq!(status, 400, "{body}");
    assert_error(&body, "sql_error");
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.starts_with("Invalid Input Error: "),
        "{message:?} names the read-only warehouse"
    );
}
