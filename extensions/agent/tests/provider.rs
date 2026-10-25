//! The provider against a recorded stream.
//!
//! Each script in `tests/fixtures/` is a canned SSE body a real provider could
//! have sent. A `TcpListener` on port 0 replays one script per connection, in
//! order, so the client — retries included — is exercised without a network.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pinkdb_agent::{Client, Config, DEFAULT_MODEL, Message, Provider, Turn};
use serde_json::json;

/// The key the mock provider gets. It must never show up in an error.
const SENTINEL: &str = "sk-sentinel-provider-test-91ffffff";

/// What one connection answers with.
enum Reply {
    /// A recorded stream, verbatim.
    Fixture(&'static str),
    /// A status line and a small JSON body.
    Status(u16, &'static str),
}

fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

struct Mock {
    port: u16,
    accepted: Arc<AtomicUsize>,
    bodies: Arc<Mutex<Vec<String>>>,
}

impl Mock {
    fn start(replies: Vec<Reply>) -> Mock {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a mock provider");
        let port = listener.local_addr().expect("local address").port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let accepted_thread = Arc::clone(&accepted);
        let bodies_thread = Arc::clone(&bodies);
        std::thread::spawn(move || {
            for reply in replies {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                accepted_thread.fetch_add(1, Ordering::SeqCst);
                if let Some(body) = read_request(&mut stream) {
                    bodies_thread.lock().unwrap().push(body);
                }
                match reply {
                    Reply::Fixture(name) => {
                        let body = fixture(name);
                        let head = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\
                             content-length: {}\r\nconnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(head.as_bytes());
                        let _ = stream.write_all(&body);
                    }
                    Reply::Status(status, body) => {
                        let head = format!(
                            "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\n\
                             content-length: {}\r\nconnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(head.as_bytes());
                        let _ = stream.write_all(body.as_bytes());
                    }
                }
                let _ = stream.flush();
                let _ = stream.shutdown(std::net::Shutdown::Write);
            }
        });
        Mock {
            port,
            accepted,
            bodies,
        }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}/v1/chat/completions", self.port)
    }

    fn client(&self) -> Client {
        Client::new(Config::with_key(self.url(), DEFAULT_MODEL, SENTINEL))
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

fn read_request(stream: &mut std::net::TcpStream) -> Option<String> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
        let Some(head_end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
        let length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())?
            })
            .unwrap_or(0);
        let body_start = head_end + 4;
        while buffer.len() < body_start + length {
            let read = stream.read(&mut chunk).ok()?;
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
        }
        return Some(String::from_utf8_lossy(&buffer[body_start..]).to_string());
    }
}

fn ask(client: &mut Client) -> Result<Turn, String> {
    client.complete(&[Message::user("how many?")], &[])
}

#[test]
fn a_recorded_answer_streams_back() {
    let mock = Mock::start(vec![Reply::Fixture("single.sse")]);
    let mut client = mock.client();
    let turn = ask(&mut client).expect("the recorded stream parses");
    assert_eq!(turn.content, "4");
    assert_eq!(turn.finish_reason, "stop");
    assert!(turn.tool_calls.is_empty());
    assert_eq!(turn.usage.total(), 15);
    assert_eq!(mock.accepted(), 1);
    let body: serde_json::Value =
        serde_json::from_str(&mock.bodies.lock().unwrap()[0]).expect("the request is JSON");
    assert_eq!(body["stream"], json!(true));
    assert_eq!(body["messages"][0]["role"], json!("user"));
}

#[test]
fn a_recorded_tool_loop_parses_both_calls() {
    let mock = Mock::start(vec![Reply::Fixture("tool_loop.sse")]);
    let mut client = mock.client();
    let turn = ask(&mut client).expect("the tool stream parses");
    assert_eq!(turn.finish_reason, "tool_calls");
    assert_eq!(turn.tool_calls.len(), 2);
    assert_eq!(turn.tool_calls[0].id, "call_1");
    assert_eq!(turn.tool_calls[0].name, "tables");
    assert_eq!(turn.tool_calls[0].arguments, r#"{"database":"sales"}"#);
    assert_eq!(turn.tool_calls[1].name, "describe");
}

#[test]
fn recorded_arguments_split_across_chunks_are_reassembled() {
    let mock = Mock::start(vec![Reply::Fixture("split_args.sse")]);
    let mut client = mock.client();
    let turn = ask(&mut client).expect("the split stream parses");
    assert_eq!(turn.tool_calls.len(), 1);
    assert_eq!(turn.tool_calls[0].arguments, r#"{"sql":"SELECT 1"}"#);
}

#[test]
fn a_stream_cut_mid_event_is_requested_again() {
    let mock = Mock::start(vec![
        Reply::Fixture("interrupted.sse"),
        Reply::Fixture("single.sse"),
    ]);
    let mut client = mock.client();
    let turn = ask(&mut client).expect("the second attempt finishes");
    assert_eq!(turn.content, "4");
    assert_eq!(mock.accepted(), 2, "the cut stream was re-requested");
}

#[test]
fn a_429_is_retried() {
    let mock = Mock::start(vec![
        Reply::Status(429, r#"{"error":"slow down"}"#),
        Reply::Fixture("single.sse"),
    ]);
    let mut client = mock.client();
    let turn = ask(&mut client).expect("the retry succeeds");
    assert_eq!(turn.content, "4");
    assert_eq!(mock.accepted(), 2);
}

#[test]
fn a_401_stops_with_a_clear_error_and_no_key() {
    let mock = Mock::start(vec![Reply::Status(
        401,
        r#"{"error":{"message":"invalid api key sk-sentinel-provider-test-91ffffff"}}"#,
    )]);
    let mut client = mock.client();
    let error = ask(&mut client).expect_err("a bad key is not retried");
    assert!(error.contains("rejected the API key (HTTP 401)"), "{error}");
    assert!(
        !error.contains(SENTINEL),
        "the key reached the error: {error}"
    );
    assert_eq!(mock.accepted(), 1, "a 401 is not retried");
}

#[test]
fn the_client_debug_output_redacts_the_key() {
    let mock = Mock::start(Vec::new());
    let client = mock.client();
    let printed = format!("{client:?}");
    assert!(!printed.contains(SENTINEL), "{printed}");
}
