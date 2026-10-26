//! The client against a recorded stream, in both wire protocols.
//!
//! Each script in `tests/fixtures/` is a canned SSE body a real provider could
//! have sent. A `TcpListener` on port 0 replays one script per connection, in
//! order, so the client — retries, headers and all — is exercised without a
//! network. Nothing here leaves 127.0.0.1.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pinkdb_llm::registry::Plan;
use pinkdb_llm::{Client, Config, Message, Protocol, Provider, Turn};
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

/// One request as the mock saw it: the head (the request line and headers,
/// lowercased) and the body.
struct Request {
    head: String,
    body: String,
}

struct Mock {
    port: u16,
    accepted: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<Request>>>,
}

impl Mock {
    fn start(replies: Vec<Reply>) -> Mock {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a mock provider");
        let port = listener.local_addr().expect("local address").port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let accepted_thread = Arc::clone(&accepted);
        let requests_thread = Arc::clone(&requests);
        std::thread::spawn(move || {
            for reply in replies {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                accepted_thread.fetch_add(1, Ordering::SeqCst);
                if let Some(request) = read_request(&mut stream) {
                    requests_thread.lock().unwrap().push(request);
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
            requests,
        }
    }

    /// A plan pointing at this mock, as the registry would have built one.
    fn plan(&self, provider: &'static str, protocol: Protocol) -> Plan {
        Plan {
            provider,
            protocol,
            base_url: format!("http://127.0.0.1:{}/v1", self.port),
            model: "mock-model".to_string(),
            max_tokens: 1024,
        }
    }

    /// A client speaking chat completions, as `zai` does.
    fn client(&self) -> Client {
        Client::new(Config::with_key(
            self.plan("zai", Protocol::OpenAi),
            SENTINEL,
        ))
    }

    /// A client speaking the Messages API, as `anthropic` does.
    fn anthropic(&self) -> Client {
        Client::new(Config::with_key(
            self.plan("anthropic", Protocol::Anthropic),
            SENTINEL,
        ))
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    fn request(&self, index: usize) -> (String, serde_json::Value) {
        let requests = self.requests.lock().unwrap();
        let request = requests.get(index).expect("a recorded request");
        (
            request.head.clone(),
            serde_json::from_str(&request.body).expect("the request body is JSON"),
        )
    }
}

fn read_request(stream: &mut std::net::TcpStream) -> Option<Request> {
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
        return Some(Request {
            head: head.to_lowercase(),
            body: String::from_utf8_lossy(&buffer[body_start..]).to_string(),
        });
    }
}

fn ask(client: &mut Client) -> Result<Turn, String> {
    client.complete(&[Message::user("how many?")], &[])
}

// ----------------------------------------------------------------- chat completions

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
    let (head, body) = mock.request(0);
    assert!(head.starts_with("post /v1/chat/completions "), "{head}");
    assert!(
        head.contains(&format!("authorization: bearer {SENTINEL}")),
        "{head}"
    );
    assert!(!head.contains("x-api-key"), "{head}");
    assert!(!head.contains("anthropic-version"), "{head}");
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
    // The message names the provider and the variable to look at.
    assert!(error.contains("zai provider"), "{error}");
    assert!(error.contains("ZAI_API_KEY or Z_API_KEY"), "{error}");
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

// ------------------------------------------------------------------- messages api

#[test]
fn an_anthropic_answer_streams_back_with_its_own_headers() {
    let mock = Mock::start(vec![Reply::Fixture("single_anthropic.sse")]);
    let mut client = mock.anthropic();
    let turn = ask(&mut client).expect("the recorded stream parses");
    assert_eq!(turn.content, "4");
    assert_eq!(turn.finish_reason, "stop");
    assert!(turn.tool_calls.is_empty());
    assert_eq!(turn.usage.prompt_tokens, 11);
    assert_eq!(turn.usage.completion_tokens, 4);
    assert_eq!(mock.accepted(), 1);

    let (head, body) = mock.request(0);
    // A different path, a different key header, and the version header.
    assert!(head.starts_with("post /v1/messages "), "{head}");
    assert!(head.contains(&format!("x-api-key: {SENTINEL}")), "{head}");
    assert!(head.contains("anthropic-version: 2023-06-01"), "{head}");
    assert!(
        !head.contains("authorization"),
        "a Bearer header would send the key twice: {head}"
    );
    assert_eq!(body["stream"], json!(true));
    assert_eq!(body["max_tokens"], json!(1024));
    assert_eq!(body["messages"][0]["role"], json!("user"));
    assert_eq!(
        body["messages"][0]["content"][0]["text"],
        json!("how many?")
    );
}

#[test]
fn an_anthropic_tool_loop_parses_both_calls_and_drops_the_text_block() {
    let mock = Mock::start(vec![Reply::Fixture("tool_loop_anthropic.sse")]);
    let mut client = mock.anthropic();
    let turn = ask(&mut client).expect("the tool stream parses");
    assert_eq!(turn.finish_reason, "tool_calls");
    assert_eq!(turn.content, "Let me look at the schema.");
    assert_eq!(turn.tool_calls.len(), 2, "{:?}", turn.tool_calls);
    assert_eq!(turn.tool_calls[0].id, "call_1");
    assert_eq!(turn.tool_calls[0].name, "tables");
    assert_eq!(turn.tool_calls[0].arguments, r#"{"database":"sales"}"#);
    assert_eq!(turn.tool_calls[1].id, "call_2");
    assert_eq!(turn.tool_calls[1].name, "describe");
    assert_eq!(turn.tool_calls[1].arguments, r#"{"table":"orders"}"#);
    assert_eq!(turn.usage.prompt_tokens, 120);
    assert_eq!(turn.usage.completion_tokens, 58);
}

#[test]
fn anthropic_input_json_deltas_split_across_chunks_are_reassembled() {
    let mock = Mock::start(vec![Reply::Fixture("split_args_anthropic.sse")]);
    let mut client = mock.anthropic();
    let turn = ask(&mut client).expect("the split stream parses");
    assert_eq!(turn.tool_calls.len(), 1);
    assert_eq!(turn.tool_calls[0].arguments, r#"{"sql":"SELECT 1"}"#);
}

#[test]
fn an_anthropic_stream_cut_mid_event_is_requested_again() {
    let mock = Mock::start(vec![
        Reply::Fixture("interrupted_anthropic.sse"),
        Reply::Fixture("single_anthropic.sse"),
    ]);
    let mut client = mock.anthropic();
    let turn = ask(&mut client).expect("the second attempt finishes");
    assert_eq!(turn.content, "4", "the half turn was discarded");
    assert_eq!(mock.accepted(), 2, "the cut stream was re-requested");
}

/// 529 is Anthropic's "overloaded": a server condition outside the 5xx range,
/// so it needs its own arm to be retried at all.
#[test]
fn a_529_is_retried() {
    let mock = Mock::start(vec![
        Reply::Status(
            529,
            r#"{"type":"error","error":{"type":"overloaded_error"}}"#,
        ),
        Reply::Fixture("single_anthropic.sse"),
    ]);
    let mut client = mock.anthropic();
    let turn = ask(&mut client).expect("the retry succeeds");
    assert_eq!(turn.content, "4");
    assert_eq!(mock.accepted(), 2);
}

#[test]
fn an_anthropic_429_is_retried() {
    let mock = Mock::start(vec![
        Reply::Status(
            429,
            r#"{"type":"error","error":{"type":"rate_limit_error"}}"#,
        ),
        Reply::Fixture("single_anthropic.sse"),
    ]);
    let mut client = mock.anthropic();
    let turn = ask(&mut client).expect("the retry succeeds");
    assert_eq!(turn.content, "4");
    assert_eq!(mock.accepted(), 2);
}

#[test]
fn an_anthropic_401_names_its_own_variable_and_no_other() {
    let mock = Mock::start(vec![Reply::Status(
        401,
        r#"{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key sk-sentinel-provider-test-91ffffff"}}"#,
    )]);
    let mut client = mock.anthropic();
    let error = ask(&mut client).expect_err("a bad key is not retried");
    assert!(error.contains("rejected the API key (HTTP 401)"), "{error}");
    assert!(error.contains("anthropic provider"), "{error}");
    assert!(error.contains("ANTHROPIC_API_KEY"), "{error}");
    assert!(!error.contains("ZAI"), "no other provider: {error}");
    assert!(
        !error.contains(SENTINEL),
        "the key reached the error: {error}"
    );
    assert_eq!(mock.accepted(), 1, "a 401 is not retried");
}

/// A 403 is a rejected credential too, and must not be retried either.
#[test]
fn a_403_stops_without_a_retry() {
    let mock = Mock::start(vec![Reply::Status(403, r#"{"error":"forbidden"}"#)]);
    let mut client = mock.anthropic();
    let error = ask(&mut client).expect_err("a forbidden key is not retried");
    assert!(error.contains("rejected the API key (HTTP 403)"), "{error}");
    assert_eq!(mock.accepted(), 1);
}

/// A keyless provider (`ollama`, a local `custom` gateway) sends no credential
/// header at all, rather than an empty one.
#[test]
fn an_empty_key_sends_no_authorization_header() {
    let mock = Mock::start(vec![Reply::Fixture("single.sse")]);
    let mut client = Client::new(Config::with_key(mock.plan("ollama", Protocol::OpenAi), ""));
    ask(&mut client).expect("the recorded stream parses");
    let (head, _) = mock.request(0);
    assert!(!head.contains("authorization"), "{head}");
    assert!(!head.contains("x-api-key"), "{head}");
}

/// The same transcript, encoded for both protocols out of the same data: this
/// is what a fork across providers relies on.
#[test]
fn one_transcript_encodes_for_either_protocol() {
    let history = [
        Message::system("you are an analyst"),
        Message::user("how many?"),
        Message::Assistant {
            content: String::new(),
            tool_calls: vec![pinkdb_llm::ToolCall {
                id: "call_1".to_string(),
                name: "query".to_string(),
                arguments: r#"{"sql":"SELECT 1"}"#.to_string(),
            }],
        },
        Message::tool("call_1", "| 1 |"),
    ];
    let tools = pinkdb_warehouse_tools::schemas();

    let openai = Mock::start(vec![Reply::Fixture("single.sse")]);
    let mut client = openai.client();
    client.complete(&history, &tools).expect("an answer");
    let (_, body) = openai.request(0);
    assert_eq!(body["messages"][0]["role"], json!("system"));
    assert_eq!(body["tools"][0]["type"], json!("function"));

    let anthropic = Mock::start(vec![Reply::Fixture("single_anthropic.sse")]);
    let mut client = anthropic.anthropic();
    client.complete(&history, &tools).expect("an answer");
    let (_, body) = anthropic.request(0);
    assert_eq!(body["system"], json!("you are an analyst"));
    assert_eq!(body["messages"][1]["content"][0]["type"], json!("tool_use"));
    assert_eq!(
        body["messages"][2]["content"][0]["type"],
        json!("tool_result")
    );
    assert_eq!(body["messages"][2]["content"][0]["tool_use_id"], "call_1");
    assert!(body["tools"][0]["input_schema"].is_object(), "{body}");
}
