//! Wave 121: the ask box, end to end — the real agent extension (its cdylib)
//! loaded into the served store, a mock model provider on 127.0.0.1, and
//! the console's event stream.
//!
//! One test in its own binary: it sets the provider's environment
//! variables, which are process-wide.
//!
//! Proves: the stream carries the tool calls (the SQL and its reply) and the
//! answer; the agent's statements run read-only; the key reaches the
//! provider and never a response body; without a provider the run fails
//! with the "no provider" answer instead of picking one.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pinkdb::Database;
use pinkdb_server_ui::{UiAuth, UiOptions, UiServer};
use serde_json::{Value, json};

/// A console without sign-in (`--ui-auth none`): the store has no users.
fn open_options() -> UiOptions {
    UiOptions {
        auth: UiAuth::None,
        ..UiOptions::default()
    }
}

/// The key the mock provider must see and no console response may carry.
const SENTINEL: &str = "sk-wave121-sentinel-never-in-a-response";

fn library_name() -> &'static str {
    if cfg!(target_os = "macos") {
        "libpinkdb_agent.dylib"
    } else if cfg!(target_os = "windows") {
        "pinkdb_agent.dll"
    } else {
        "libpinkdb_agent.so"
    }
}

/// Build the agent cdylib and find it next to this test's target directory.
fn agent_library() -> PathBuf {
    if let Ok(path) = std::env::var("PINKDB_AGENT_LIB") {
        return PathBuf::from(path);
    }
    let status = Command::new(env!("CARGO"))
        .args(["build", "-q", "-p", "pinkdb-agent"])
        .status()
        .expect("run cargo");
    assert!(status.success(), "cargo build -p pinkdb-agent failed");
    let exe = std::env::current_exe().unwrap();
    // target/<profile>/deps/ask-<hash> → target/<profile>
    let profile = exe.parent().and_then(Path::parent).unwrap();
    let path = profile.join(library_name());
    assert!(path.is_file(), "no agent library at {}", path.display());
    path
}

// ---------- the mock provider (chat completions over SSE) ----------

fn chunk(delta: Value, finish: Option<&str>) -> String {
    json!({
        "id": "mock", "object": "chat.completion.chunk", "created": 1, "model": "mock-model",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    })
    .to_string()
}

fn usage() -> String {
    json!({"id": "mock", "model": "mock-model", "choices": [],
           "usage": {"prompt_tokens": 10, "completion_tokens": 5}})
    .to_string()
}

fn sql_call(id: &str, sql: &str) -> Vec<String> {
    let call = json!({"index": 0, "id": id, "type": "function",
                      "function": {"name": "query", "arguments": json!({"sql": sql}).to_string()}});
    vec![
        chunk(json!({"tool_calls": [call]}), None),
        chunk(json!({}), Some("tool_calls")),
        usage(),
    ]
}

fn answer(text: &str) -> Vec<String> {
    vec![
        chunk(json!({"content": text}), None),
        chunk(json!({}), Some("stop")),
        usage(),
    ]
}

struct Mock {
    port: u16,
    heads: Arc<Mutex<Vec<String>>>,
}

impl Mock {
    fn start(replies: Vec<Vec<String>>) -> Mock {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let heads = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&heads);
        std::thread::spawn(move || {
            let mut queue = replies.into_iter();
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let Some(reply) = queue.next() else { break };
                let head = read_head_and_body(&mut stream);
                seen.lock().unwrap().push(head);
                let mut body = String::new();
                for line in reply {
                    body.push_str(&format!("data: {line}\n\n"));
                }
                body.push_str("data: [DONE]\n\n");
                let _ = stream.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\
                         connection: close\r\n\r\n{body}"
                    )
                    .as_bytes(),
                );
            }
        });
        Mock { port, heads }
    }
}

fn read_head_and_body(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) | Err(_) => break,
            Ok(_) => head.push(byte[0]),
        }
    }
    let head = String::from_utf8_lossy(&head).to_string();
    let length = head
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let mut body = vec![0u8; length];
    let _ = stream.read_exact(&mut body);
    head
}

// ---------- the console client ----------

fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> (u16, String, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    let mut text = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\n");
    for (k, v) in headers {
        text.push_str(&format!("{k}: {v}\r\n"));
    }
    if let Some(body) = body {
        text.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ));
    }
    text.push_str("Connection: close\r\n\r\n");
    text.push_str(body.unwrap_or_default());
    stream.write_all(text.as_bytes()).unwrap();
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

/// The events of an SSE body (chunked transfer decoded loosely: the event
/// lines are found wherever they are).
fn events(body: &str) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    let mut name = String::new();
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("event: ") {
            name = rest.to_string();
        } else if let Some(rest) = line.strip_prefix("data: ") {
            out.push((
                name.clone(),
                serde_json::from_str(rest).unwrap_or(Value::Null),
            ));
        }
    }
    out
}

#[test]
fn the_ask_box_streams_an_agent_run() {
    let library = agent_library();
    let mock = Mock::start(vec![
        sql_call("c1", "SELECT count(*) AS n FROM t"),
        answer("There are 3 rows in t."),
        // The second run: the model tries to write.
        sql_call("c2", "INSERT INTO t VALUES (4)"),
        answer("I could not write."),
    ]);
    // SAFETY: this binary has exactly one test, so no other thread reads the
    // environment while it is changed.
    unsafe {
        std::env::remove_var("PINKDB_AGENT_PROVIDER");
        std::env::set_var(
            "PINKDB_AGENT_BASE_URL",
            format!("http://127.0.0.1:{}/v1", mock.port),
        );
        std::env::set_var("PINKDB_AGENT_API_KEY", SENTINEL);
    }

    let db = Database::new();
    db.connect()
        .execute("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1), (2), (3);")
        .unwrap();
    db.load_extension(library.to_str().unwrap())
        .expect("load the agent extension");
    let server = UiServer::bind("127.0.0.1:0", db.clone(), open_options()).unwrap();
    let addr = server.local_addr();
    let handle = server.spawn().unwrap();

    let (status, head, body) = http(addr, "POST", "/api/login", &[], Some("{}"));
    assert_eq!(status, 200, "{body}");
    let cookie = head
        .lines()
        .find_map(|l| l.strip_prefix("set-cookie: "))
        .and_then(|c| c.split(';').next())
        .unwrap()
        .to_string();
    let session: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(session["agent"], true);
    let csrf = session["csrf"].as_str().unwrap().to_string();
    let auth = [("Cookie", cookie.as_str()), ("X-CSRF-Token", csrf.as_str())];

    // The provider table: names and states, never a key or a URL.
    let (status, _, body) = http(addr, "GET", "/api/agent", &auth, None);
    assert_eq!(status, 200, "{body}");
    assert!(
        !body.contains(SENTINEL),
        "the key reached the browser: {body}"
    );
    assert!(
        !body.contains(&mock.port.to_string()),
        "the base URL reached the browser: {body}"
    );
    let info: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(info["loaded"], true);
    assert_eq!(info["allow_writes"], false);
    let custom = info["providers"]["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "custom")
        .unwrap()
        .clone();
    assert_eq!(custom["ready"], true, "{custom}");
    assert_eq!(info["providers"]["default"], Value::Null);

    // A run: the tool call and its reply, then the answer.
    let ask = json!({ "question": "How many rows are in t?", "provider": "custom",
                      "model": "mock-model" })
    .to_string();
    let (status, head, body) = http(addr, "POST", "/api/ask", &auth, Some(&ask));
    assert_eq!(status, 200, "{body}");
    assert!(head.contains("text/event-stream"), "{head}");
    assert!(
        !body.contains(SENTINEL),
        "the key reached the browser: {body}"
    );
    let run = events(&body);
    let tool = run
        .iter()
        .find(|(name, _)| name == "tool")
        .unwrap_or_else(|| panic!("no tool event: {body}"));
    assert_eq!(tool.1["sql"], "SELECT count(*) AS n FROM t");
    assert_eq!(tool.1["ok"], true);
    assert!(tool.1["reply"].as_str().unwrap().contains('3'), "{tool:?}");
    let answer_text: String = run
        .iter()
        .filter(|(name, _)| name == "answer")
        .map(|(_, data)| data["text"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(answer_text.contains("There are 3 rows"), "{body}");
    assert_eq!(run.last().unwrap().0, "done", "{body}");
    // The key went to the provider, in its own header.
    assert!(
        mock.heads.lock().unwrap()[0].contains(SENTINEL),
        "the provider did not get the key"
    );

    // The agent is read-only: its write fails as a tool error, the store is
    // unchanged.
    let ask =
        json!({ "question": "Add a row", "provider": "custom", "model": "mock-model" }).to_string();
    let (_, _, body) = http(addr, "POST", "/api/ask", &auth, Some(&ask));
    let run = events(&body);
    let tool = run.iter().find(|(name, _)| name == "tool").unwrap();
    assert_eq!(tool.1["ok"], false, "{body}");
    let count = db.connect().execute("SELECT count(*) FROM t").unwrap();
    assert!(format!("{count:?}").contains("Bigint(3)"), "{count:?}");
    // The runs are recorded in the agent's own database, in the state store,
    // which the warehouse does not list.
    let state = db
        .state_store()
        .expect("the console attached a state store");
    let databases = format!("{:?}", state.connect().execute("SHOW DATABASES").unwrap());
    assert!(databases.contains("pinkdb_agent"), "{databases}");
    let databases = format!("{:?}", db.connect().execute("SHOW DATABASES").unwrap());
    assert!(!databases.contains("pinkdb_agent"), "{databases}");

    // No provider named, none in the environment: the run says so.
    let ask = json!({ "question": "Anything?" }).to_string();
    let (_, _, body) = http(addr, "POST", "/api/ask", &auth, Some(&ask));
    let run = events(&body);
    assert_eq!(run.last().unwrap().0, "error", "{body}");
    assert!(body.contains("no provider"), "{body}");
    assert!(!body.contains(SENTINEL));

    // A provider name cannot smuggle a flag into the command line.
    let ask = json!({ "question": "q", "provider": "custom allow-writes" }).to_string();
    let (status, _, _) = http(addr, "POST", "/api/ask", &auth, Some(&ask));
    assert_eq!(status, 400);

    handle.shutdown();
}
