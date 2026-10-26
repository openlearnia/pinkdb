//! Waves 106 and 106b, end to end: the `agent` extension, driven from the CLI.
//!
//! Every model call here goes to a mock provider on 127.0.0.1: an SSE stream
//! written by a `TcpListener` thread, one scripted reply per connection, so
//! the tests are offline and deterministic. The mock speaks both wire
//! protocols (chat completions and the Anthropic Messages API) and records
//! every request's headers, which is what the key-isolation test reads.
//! `pinkdb-llm`'s own tests cover the SSE parsers and the encoders; these
//! cover the host contract — the CLI, the policy, the store and the
//! transcript — and the provider selection across process boundaries.
//!
//! Build the extension first (`cargo build -p pinkdb-agent`); the tests ask
//! `PINKDB_AGENT_LIB` or the workspace target directory for it.

mod common;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::run_merged;
use serde_json::{Value, json};

fn cli() -> &'static str {
    env!("CARGO_BIN_EXE_pinkdb")
}

fn library_name() -> &'static str {
    if cfg!(target_os = "macos") {
        "libpinkdb_agent.dylib"
    } else if cfg!(target_os = "windows") {
        "pinkdb_agent.dll"
    } else {
        "libpinkdb_agent.so"
    }
}

/// The extension cdylib, resolved (and built if missing) once per test
/// binary: several tests building it at the same time re-link it under each
/// other, and a path one test just found vanishes before its child loads it.
fn agent_library() -> PathBuf {
    static AGENT: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    AGENT.get_or_init(resolve_agent_library).clone()
}

fn resolve_agent_library() -> PathBuf {
    let name = library_name();
    if let Ok(path) = std::env::var("PINKDB_AGENT_LIB") {
        return PathBuf::from(path);
    }
    // Rebuild before looking: `cargo test -p pinkdb-cli` does not build the
    // extension's cdylib, so a dylib left from an earlier edit would run the
    // old code and the tests would pass against it.
    let _ = Command::new(env!("CARGO"))
        .args(["build", "-q", "-p", "pinkdb-agent"])
        .status();
    let mut candidates = Vec::new();
    let exe = Path::new(cli());
    if let Some(dir) = exe.parent() {
        candidates.push(dir.join(name));
        if let Some(target) = dir.parent() {
            candidates.extend([
                target.join("debug").join(name),
                target.join("release").join(name),
            ]);
        }
    }
    for path in &candidates {
        if path.is_file() {
            return path.clone();
        }
    }
    let built = Command::new(env!("CARGO"))
        .args(["build", "-q", "-p", "pinkdb-agent"])
        .status()
        .is_ok_and(|status| status.success());
    if built && let Some(dir) = Path::new(cli()).parent() {
        for path in [dir.join(name), dir.join("deps").join(name)] {
            if path.is_file() {
                return path;
            }
        }
    }
    panic!(
        "no {} found (set PINKDB_AGENT_LIB); tried {}",
        name,
        candidates
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
}

// ---------------------------------------------------------------- mock provider

/// One scripted reply, written when a connection's request has been read.
enum Reply {
    /// A complete chat-completions stream: each chunk is a `data:` payload,
    /// then `[DONE]`.
    Sse(Vec<String>),
    /// A complete Messages API stream: each event is a `data:` payload, ended
    /// by `message_stop` instead of `[DONE]`.
    Anthropic(Vec<String>),
    /// A status code and a body (401, 429, 529, …).
    Status(u16, String),
    /// A stream cut off in the middle of an event: the socket closes.
    Cut,
    /// Read the request and never answer: the test kills the client.
    Stall,
}

fn chunk(delta: Value, finish: Option<&str>) -> String {
    json!({
        "id": "mock",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "glm-5.3",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    })
    .to_string()
}

fn usage_chunk(prompt: u64, completion: u64) -> String {
    json!({
        "id": "mock",
        "model": "glm-5.3",
        "choices": [],
        "usage": {"prompt_tokens": prompt, "completion_tokens": completion},
    })
    .to_string()
}

/// A plain answer: one content chunk, a stop chunk, a usage chunk.
fn answer(text: &str) -> Reply {
    Reply::Sse(vec![
        chunk(json!({"content": text}), None),
        chunk(json!({}), Some("stop")),
        usage_chunk(10, 5),
    ])
}

/// The Messages API's version of [`answer`]: usage, one text block, a stop.
fn anthropic_answer(text: &str) -> Reply {
    Reply::Anthropic(vec![
        json!({"type": "message_start",
               "message": {"usage": {"input_tokens": 10, "output_tokens": 1}}})
        .to_string(),
        json!({"type": "content_block_start", "index": 0,
               "content_block": {"type": "text", "text": ""}})
        .to_string(),
        json!({"type": "content_block_delta", "index": 0,
               "delta": {"type": "text_delta", "text": text}})
        .to_string(),
        json!({"type": "content_block_stop", "index": 0}).to_string(),
        json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"},
               "usage": {"output_tokens": 5}})
        .to_string(),
        json!({"type": "message_stop"}).to_string(),
    ])
}

/// The Messages API's version of [`tool_calls`]: one `tool_use` block per
/// call, its arguments as one `input_json_delta`.
fn anthropic_tool_calls(calls: &[(&str, &str, &str)]) -> Reply {
    let mut events = vec![
        json!({"type": "message_start",
               "message": {"usage": {"input_tokens": 10, "output_tokens": 1}}})
        .to_string(),
    ];
    for (index, (id, name, arguments)) in calls.iter().enumerate() {
        events.push(
            json!({"type": "content_block_start", "index": index,
                   "content_block": {"type": "tool_use", "id": id, "name": name, "input": {}}})
            .to_string(),
        );
        events.push(
            json!({"type": "content_block_delta", "index": index,
                   "delta": {"type": "input_json_delta", "partial_json": arguments}})
            .to_string(),
        );
        events.push(json!({"type": "content_block_stop", "index": index}).to_string());
    }
    events.push(
        json!({"type": "message_delta", "delta": {"stop_reason": "tool_use"},
               "usage": {"output_tokens": 5}})
        .to_string(),
    );
    events.push(json!({"type": "message_stop"}).to_string());
    Reply::Anthropic(events)
}

/// One assistant turn asking for `calls` = `(id, tool, arguments)`.
fn tool_calls(calls: &[(&str, &str, &str)]) -> Reply {
    let calls: Vec<Value> = calls
        .iter()
        .enumerate()
        .map(|(index, (id, name, arguments))| {
            json!({
                "index": index,
                "id": id,
                "type": "function",
                "function": {"name": name, "arguments": arguments},
            })
        })
        .collect();
    Reply::Sse(vec![
        chunk(json!({"tool_calls": calls}), None),
        chunk(json!({}), Some("tool_calls")),
        usage_chunk(10, 5),
    ])
}

/// One request as the mock saw it. The head is kept because the key-isolation
/// test has to read the credential headers, not only the body.
#[derive(Clone, Default)]
struct Request {
    head: String,
    body: String,
}

struct Mock {
    port: u16,
    requests: Arc<Mutex<Vec<Request>>>,
    accepted: Arc<AtomicUsize>,
}

impl Mock {
    fn start(replies: Vec<Reply>) -> Mock {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the mock provider");
        let port = listener.local_addr().expect("mock address").port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(AtomicUsize::new(0));
        let (seen, count) = (requests.clone(), accepted.clone());
        std::thread::spawn(move || {
            let mut queue = replies.into_iter();
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let Some(reply) = queue.next() else { break };
                count.fetch_add(1, Ordering::SeqCst);
                let request = read_request(&mut stream);
                seen.lock().unwrap_or_else(|p| p.into_inner()).push(request);
                write_reply(&mut stream, reply);
            }
        });
        Mock {
            port,
            requests,
            accepted,
        }
    }

    /// The API **root**: the protocol appends `/chat/completions` or
    /// `/messages` itself, so one mock serves either.
    fn base(&self) -> String {
        format!("http://127.0.0.1:{}/v1", self.port)
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    fn requests(&self) -> Vec<Request> {
        self.requests
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// The request bodies that arrived, parsed.
    fn bodies(&self) -> Vec<Value> {
        self.requests()
            .iter()
            .map(|request| serde_json::from_str(&request.body).unwrap_or(Value::Null))
            .collect()
    }
}

/// Read one HTTP/1.1 request: the head, then `content-length` bytes of body.
fn read_request(stream: &mut std::net::TcpStream) -> Request {
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("read timeout");
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) | Err(_) => return Request::default(),
            Ok(_) => head.push(byte[0]),
        }
    }
    let head = String::from_utf8_lossy(&head).to_string();
    let length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let mut body = vec![0u8; length];
    let mut filled = 0;
    while filled < length {
        match stream.read(&mut body[filled..]) {
            Ok(0) | Err(_) => break,
            Ok(read) => filled += read,
        }
    }
    body.truncate(filled);
    Request {
        head,
        body: String::from_utf8_lossy(&body).to_string(),
    }
}

fn write_reply(stream: &mut std::net::TcpStream, reply: Reply) {
    let _ = stream.flush();
    match reply {
        Reply::Sse(chunks) => {
            let mut body = String::new();
            for line in chunks {
                body.push_str("data: ");
                body.push_str(&line);
                body.push_str("\n\n");
            }
            body.push_str("data: [DONE]\n\n");
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n{}",
                body
            );
            let _ = stream.write_all(head.as_bytes());
        }
        Reply::Anthropic(events) => {
            // The real API names each event on its own line too; the client
            // reads the `type` inside the payload, so the name is decoration.
            let mut body = String::new();
            for event in events {
                let name = serde_json::from_str::<Value>(&event)
                    .ok()
                    .and_then(|v| v["type"].as_str().map(str::to_string))
                    .unwrap_or_default();
                body.push_str(&format!("event: {name}\ndata: {event}\n\n"));
            }
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n{}",
                body
            );
            let _ = stream.write_all(head.as_bytes());
        }
        Reply::Status(status, body) => {
            let head = format!(
                "HTTP/1.1 {status} status\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(head.as_bytes());
        }
        Reply::Cut => {
            // A real interrupted stream: the event never completed.
            let head =
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n";
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(b"data: {\"choices\":[{\"delta\":{\"content\":\"par");
        }
        Reply::Stall => std::thread::sleep(Duration::from_secs(60)),
    }
    let _ = stream.shutdown(std::net::Shutdown::Both);
}

// ---------------------------------------------------------------------- fixture

const FIXTURE: &str = "\
CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR);
CREATE TABLE lineitem (customer_id INTEGER, quantity INTEGER, price INTEGER);
INSERT INTO customers VALUES (1, 'Acme'), (2, 'Bravo'), (3, 'Cedar'), (4, 'Delta'), (5, 'Echo');
INSERT INTO lineitem VALUES (1, 3, 100), (1, 2, 50), (2, 5, 10), (3, 2, 30), (4, 4, 5), (5, 1, 1000);
";

/// The sentinel key every run in this file uses; nothing it touches may keep
/// it. Unique per test so a stale file from another test cannot match.
fn sentinel(tag: &str) -> String {
    format!("sk-sentinel-{}-{tag}", std::process::id())
}

/// Every variable the provider registry reads.
const SCRUBBED: [&str; 22] = [
    "PINKDB_AGENT_PROVIDER",
    "PINKDB_AGENT_MODEL",
    "PINKDB_AGENT_BASE_URL",
    "PINKDB_AGENT_ZAI_BASE_URL",
    "PINKDB_AGENT_ANTHROPIC_BASE_URL",
    "PINKDB_AGENT_OPENAI_BASE_URL",
    "PINKDB_AGENT_DEEPSEEK_BASE_URL",
    "PINKDB_AGENT_OPENROUTER_BASE_URL",
    "PINKDB_AGENT_GEMINI_BASE_URL",
    "PINKDB_AGENT_OLLAMA_BASE_URL",
    "PINKDB_AGENT_PROTOCOL",
    "PINKDB_AGENT_API_KEY",
    "PINKDB_AGENT_MAX_TOKENS",
    "ZAI_API_KEY",
    "Z_API_KEY",
    "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY",
    "DEEPSEEK_API_KEY",
    "OPENROUTER_API_KEY",
    "GEMINI_API_KEY",
    "PINKDB_AGENT_LIVE",
    "PINKDB_MEMORY_DISTILL_EVERY",
];

/// Point every provider at `mock`, each through its own root variable (a
/// provider reads no other).
fn point_at<'a>(command: &'a mut Command, mock: &Mock) -> &'a mut Command {
    for variable in SCRUBBED.iter().filter(|name| name.ends_with("_BASE_URL")) {
        command.env(variable, mock.base());
    }
    command
}

/// A fresh durable store with the fixture, seeded by a CLI script.
struct Store {
    dir: PathBuf,
    path: PathBuf,
}

impl Store {
    fn new(tag: &str) -> Store {
        use std::sync::atomic::AtomicU32;
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinkdb-agent-{}-{tag}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the store directory");
        let store = Store {
            path: dir.join("store.pink"),
            dir,
        };
        let script = store.script(&[FIXTURE.to_string()]);
        let (out, status) = run_merged(store.command(&Mock::start(Vec::new())), &script);
        assert!(status.success(), "seeding failed: {out}");
        store
    }

    /// A CLI invocation against this store, pointed at `mock` as the `zai`
    /// provider, with a sentinel in zai's own key variable.
    fn command(&self, mock: &Mock) -> Command {
        let mut command = self.plain_command();
        point_at(&mut command, mock)
            .env("PINKDB_AGENT_PROVIDER", "zai")
            .env("ZAI_API_KEY", sentinel("env"));
        command
    }

    /// A CLI invocation that may replay this store's WAL: the records name the
    /// agent library, so replay needs the opt-in and an extension root.
    fn plain_command(&self) -> Command {
        let mut command = Command::new(cli());
        // A developer's shell may export real keys or a provider. None of it
        // may reach a child here: the tests must not depend on it, and a real
        // key must never be sent to a mock.
        for variable in SCRUBBED {
            command.env_remove(variable);
        }
        let root = agent_library()
            .parent()
            .map(|dir| dir.to_path_buf())
            .unwrap_or_default();
        command
            .arg("--allow-load-extension")
            .arg("--extension-root")
            .arg(root);
        command
    }

    /// The script for a session: open, load the extension, run `lines`, save.
    fn script(&self, lines: &[String]) -> String {
        let path = self.path.display();
        let mut script = format!(
            ".open {path}\nLOAD EXTENSION '{}';\n",
            agent_library().display()
        );
        for line in lines {
            script.push_str(line);
            script.push('\n');
        }
        script.push_str(&format!(".save {path}\n"));
        script
    }

    /// The script for a session that runs the agent *and* the memory
    /// extension: memory is loaded first, the order this seam was built in.
    /// The other order is pinned by
    /// `loading_the_agent_before_memory_still_runs_every_statement`.
    fn memory_script(&self, lines: &[String]) -> String {
        let path = self.path.display();
        let mut script = format!(
            ".open {path}\nLOAD EXTENSION '{}';\nLOAD EXTENSION '{}';\n",
            agent_library().display(),
            memory_library().display()
        );
        for line in lines {
            script.push_str(line);
            script.push('\n');
        }
        script.push_str(&format!(".save {path}\n"));
        script
    }

    /// Replay the WAL and checkpoint it into the snapshot.
    fn fold(&self) {
        let path = self.path.display();
        let (out, status) = run_merged(
            self.plain_command(),
            &format!(".open {path}\n.save {path}\n"),
        );
        assert!(status.success(), "fold failed:\n{out}");
    }

    fn run(&self, mock: &Mock, lines: &[String]) -> (String, i32) {
        let script = self.script(lines);
        let (out, status) = run_merged(self.command(mock), &script);
        (out, status.code().unwrap_or(-1))
    }

    /// ``.agent ask …`` against `mock`, returning the merged output.
    fn ask(&self, mock: &Mock, question: &str) -> String {
        let (out, code) = self.run(mock, &[format!(".agent ask \"{question}\"")]);
        assert_eq!(code, 0, "the run failed:\n{out}");
        out
    }

    /// One read-only SQL session: rows as the CLI prints them.
    fn query(&self, sql: &str) -> String {
        let path = self.path.display();
        let script = format!(".open {path}\n{sql}\n");
        let (out, status) = run_merged(self.plain_command(), &script);
        assert!(status.success(), "query {sql} failed:\n{out}");
        out
    }

    /// The first data cell of `sql`'s Markdown table.
    fn cell(&self, sql: &str) -> String {
        cells(&self.query(sql))
    }

    fn bytes(&self) -> Vec<u8> {
        let mut bytes = std::fs::read(&self.path).unwrap_or_default();
        if let Ok(wal) = std::fs::read(format!("{}.wal", self.path.display())) {
            bytes.extend(wal);
        }
        bytes
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The memory extension next to the CLI, built on demand: only the test that
/// loads both extensions needs it, and `cargo test -p pinkdb-cli` does not
/// build the extension's cdylib.
fn memory_library() -> PathBuf {
    static MEMORY: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    MEMORY
        .get_or_init(|| {
            let name = if cfg!(target_os = "macos") {
                "libpinkdb_memory.dylib"
            } else if cfg!(target_os = "windows") {
                "pinkdb_memory.dll"
            } else {
                "libpinkdb_memory.so"
            };
            let _ = Command::new(env!("CARGO"))
                .args(["build", "-q", "-p", "pinkdb-memory"])
                .status();
            let mut candidates = Vec::new();
            if let Some(dir) = Path::new(cli()).parent() {
                candidates.push(dir.join(name));
                if let Some(target) = dir.parent() {
                    candidates.push(target.join("debug").join(name));
                }
            }
            candidates
                .into_iter()
                .find(|path| path.is_file())
                .unwrap_or_else(|| PathBuf::from(name))
        })
        .clone()
}

/// The first data cell of a table the CLI printed.
///
/// The CLI has two layouts: Markdown (a header row, a separator row, then
/// data) in agent mode, and a box table (names, types, then data) otherwise.
/// The tests read either.
fn cells(out: &str) -> String {
    let boxed = out.contains('\u{2502}');
    let rows: Vec<Vec<String>> = out
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let inner = line
                .strip_prefix('|')
                .and_then(|rest| rest.strip_suffix('|'))
                .or_else(|| {
                    line.strip_prefix('\u{2502}')
                        .and_then(|rest| rest.strip_suffix('\u{2502}'))
                })?;
            let cells: Vec<String> = inner
                .split(['|', '\u{2502}'])
                .map(|cell| cell.trim().to_string())
                .collect();
            // The separator row of a Markdown table is not data.
            if cells
                .iter()
                .all(|cell| !cell.is_empty() && cell.chars().all(|c| c == '-'))
            {
                return None;
            }
            Some(cells)
        })
        .collect();
    // Box tables print a type row under the names; Markdown does not.
    let first_data = if boxed { 2 } else { 1 };
    rows.get(first_data)
        .and_then(|row| row.first())
        .cloned()
        .unwrap_or_default()
}

// ------------------------------------------------------------------------ tests

#[test]
fn a_single_answer_is_printed_and_recorded() {
    let store = Store::new("single");
    let mock = Mock::start(vec![answer("There are 5 customers.")]);
    let out = store.ask(&mock, "how many customers are there?");

    assert!(out.contains("There are 5 customers."), "output: {out}");
    assert!(out.contains("pinkdb agent: session "), "output: {out}");
    assert_eq!(mock.accepted(), 1, "one model call");
    let body = &mock.bodies()[0];
    assert_eq!(body["model"], json!("glm-5.3"));
    assert_eq!(body["stream"], json!(true));
    // The system prompt names the tools and states the policy.
    let system = body["messages"][0]["content"].as_str().unwrap_or_default();
    assert!(system.contains("query"), "system prompt: {system}");
    assert!(system.contains("read-only"), "system prompt: {system}");

    assert_eq!(
        store.cell("SELECT status FROM pinkdb_agent.sessions"),
        "done"
    );
    assert_eq!(store.cell("SELECT steps FROM pinkdb_agent.sessions"), "1");
    assert_eq!(
        store.cell("SELECT count(*) FROM pinkdb_agent.messages"),
        "2",
        "one user message and one answer"
    );
    assert_eq!(
        store.cell("SELECT prompt_tokens FROM pinkdb_agent.usage"),
        "10"
    );
}

#[test]
fn a_tool_loop_runs_real_queries_against_the_store() {
    let store = Store::new("loop");
    let mock = Mock::start(vec![
        tool_calls(&[
            ("call_1", "tables", r#"{"database":"main"}"#),
            (
                "call_2",
                "query",
                r#"{"sql":"SELECT c.name, SUM(l.quantity * l.price) AS revenue FROM lineitem l JOIN customers c ON c.id = l.customer_id GROUP BY c.name ORDER BY revenue DESC LIMIT 3"}"#,
            ),
        ]),
        answer("Acme made the most, then Echo, then Bravo."),
    ]);
    let out = store.ask(&mock, "top 3 customers by revenue");

    assert!(
        out.contains("Acme made the most, then Echo, then Bravo."),
        "output: {out}"
    );
    assert_eq!(mock.accepted(), 2, "the tool results go back to the model");
    let second = &mock.bodies()[1];
    let messages = second["messages"].as_array().expect("messages");
    let tools: Vec<&Value> = messages
        .iter()
        .filter(|m| m["role"] == json!("tool"))
        .collect();
    assert_eq!(
        tools.len(),
        2,
        "both results are in the transcript: {second}"
    );
    assert_eq!(tools[0]["tool_call_id"], json!("call_1"));
    assert!(
        tools[0]["content"]
            .as_str()
            .unwrap_or_default()
            .contains("customers"),
        "tables output: {}",
        tools[0]["content"]
    );
    assert!(
        tools[1]["content"]
            .as_str()
            .unwrap_or_default()
            .contains("Acme"),
        "query output: {}",
        tools[1]["content"]
    );

    assert_eq!(store.cell("SELECT steps FROM pinkdb_agent.sessions"), "2");
    assert_eq!(
        store.cell("SELECT count(*) FROM pinkdb_agent.tool_calls"),
        "2"
    );
    assert_eq!(
        store.cell("SELECT count(*) FROM pinkdb_agent.tool_calls WHERE state = 'recorded'"),
        "2"
    );
    assert_eq!(
        store.cell("SELECT count(*) FROM pinkdb_agent.tool_calls WHERE code = 0"),
        "2"
    );
    // Every step has a checkpoint.
    assert_eq!(
        store.cell("SELECT count(*) FROM pinkdb_agent.checkpoints"),
        "3",
        "step 0 (the question) and one per model turn"
    );
}

#[test]
fn tool_arguments_split_across_chunks_are_reassembled() {
    let store = Store::new("split");
    let mock = Mock::start(vec![
        Reply::Sse(vec![
            chunk(
                json!({"tool_calls": [{"index": 0, "id": "call_1", "type": "function",
                    "function": {"name": "query", "arguments": "{\"sql\":\"SELECT "}}]}),
                None,
            ),
            chunk(
                json!({"tool_calls": [{"index": 0,
                    "function": {"arguments": "count(*) FROM customers\"}"}}]}),
                None,
            ),
            chunk(json!({}), Some("tool_calls")),
            usage_chunk(10, 5),
        ]),
        answer("5 rows."),
    ]);
    let out = store.ask(&mock, "how many customers?");
    assert!(out.contains("5 rows."), "output: {out}");
    assert_eq!(
        store.cell("SELECT arguments FROM pinkdb_agent.tool_calls"),
        "{\"sql\":\"SELECT count(*) FROM customers\"}"
    );
    let second = &mock.bodies()[1];
    let tool = second["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .find(|m| m["role"] == json!("tool"))
        .cloned()
        .expect("a tool result");
    assert!(
        tool["content"].as_str().unwrap_or_default().contains('5'),
        "tool result: {tool}"
    );
}

#[test]
fn an_interrupted_stream_is_requested_again() {
    let store = Store::new("cut");
    let mock = Mock::start(vec![Reply::Cut, answer("recovered")]);
    let out = store.ask(&mock, "say something");
    assert!(out.contains("recovered"), "output: {out}");
    assert_eq!(mock.accepted(), 2, "the request is retried");
    assert!(out.contains("retrying"), "the retry is reported: {out}");
    assert_eq!(store.cell("SELECT steps FROM pinkdb_agent.sessions"), "1");
}

#[test]
fn a_429_is_retried() {
    let store = Store::new("429");
    let mock = Mock::start(vec![
        Reply::Status(429, r#"{"error":"rate limited"}"#.to_string()),
        answer("after the wait"),
    ]);
    let out = store.ask(&mock, "slow down");
    assert!(out.contains("after the wait"), "output: {out}");
    assert_eq!(mock.accepted(), 2);
    assert!(out.contains("rate limiting"), "output: {out}");
}

#[test]
fn a_401_stops_with_a_clear_error_and_no_key() {
    let store = Store::new("401");
    let mock = Mock::start(vec![Reply::Status(
        401,
        r#"{"error":"bad key"}"#.to_string(),
    )]);
    let (out, _) = store.run(&mock, &[".agent ask \"anything\"".to_string()]);
    assert!(
        out.contains("rejected the API key (HTTP 401)"),
        "output: {out}"
    );
    assert_eq!(mock.accepted(), 1, "a 401 is not retried");
    assert!(
        !out.contains(&sentinel("env")),
        "the key reached the output: {out}"
    );
    assert_eq!(
        store.cell("SELECT status FROM pinkdb_agent.sessions"),
        "failed"
    );
}

/// A distinct sentinel in each provider's own key variable, all exported at
/// once: `(variable, sentinel)`.
fn all_sentinels(tag: &str) -> Vec<(&'static str, String)> {
    vec![
        ("ZAI_API_KEY", sentinel(&format!("{tag}-zai"))),
        ("ANTHROPIC_API_KEY", sentinel(&format!("{tag}-anthropic"))),
        ("OPENROUTER_API_KEY", sentinel(&format!("{tag}-openrouter"))),
        ("PINKDB_AGENT_API_KEY", sentinel(&format!("{tag}-custom"))),
    ]
}

#[test]
fn the_key_never_reaches_the_store_or_the_output() {
    let store = Store::new("sentinel");
    let keys = all_sentinels("leak");
    let mock = Mock::start(vec![
        tool_calls(&[("call_1", "databases", "{}")]),
        answer("one database"),
    ]);
    let mut command = store.command(&mock);
    for (variable, key) in &keys {
        command.env(variable, key);
    }
    let (out, status) = run_merged(
        command,
        &store.script(&[".agent ask \"list the databases\"".to_string()]),
    );
    assert!(status.success(), "the run failed:\n{out}");
    assert!(out.contains("one database"), "output: {out}");

    let bytes = store.bytes();
    let tables: Vec<(&str, String)> =
        ["sessions", "messages", "tool_calls", "checkpoints", "usage"]
            .into_iter()
            .map(|table| {
                (
                    table,
                    store.query(&format!("SELECT * FROM pinkdb_agent.{table}")),
                )
            })
            .collect();
    for (variable, key) in &keys {
        // The output the operator sees.
        assert!(
            !out.contains(key),
            "{variable} reached stdout/stderr: {out}"
        );
        // The store's bytes, snapshot and WAL.
        assert!(
            !bytes
                .windows(key.len())
                .any(|window| window == key.as_bytes()),
            "{variable} reached the .pink file"
        );
        // Every table, including the session's own.
        for (table, rows) in &tables {
            assert!(!rows.contains(key), "{variable} reached {table}:\n{rows}");
        }
    }
}

/// The security rule of wave 106b, across the process boundary: with every
/// sentinel exported, a run against each provider's mock carries exactly that
/// provider's key, in that protocol's header, and no other sentinel anywhere.
#[test]
fn each_provider_receives_only_its_own_key() {
    let store = Store::new("isolation");
    let keys = all_sentinels("iso");
    let key_of = |variable: &str| {
        keys.iter()
            .find(|(name, _)| *name == variable)
            .map(|(_, key)| key.clone())
    };
    // (provider, the reply it speaks, the header its key must arrive in).
    let cases: [(&str, Reply, Option<String>); 5] = [
        (
            "zai",
            answer("zai answered"),
            key_of("ZAI_API_KEY").map(|k| format!("authorization: bearer {k}")),
        ),
        (
            "anthropic",
            anthropic_answer("anthropic answered"),
            key_of("ANTHROPIC_API_KEY").map(|k| format!("x-api-key: {k}")),
        ),
        (
            "openrouter",
            answer("openrouter answered"),
            key_of("OPENROUTER_API_KEY").map(|k| format!("authorization: bearer {k}")),
        ),
        (
            "custom",
            answer("custom answered"),
            key_of("PINKDB_AGENT_API_KEY").map(|k| format!("authorization: bearer {k}")),
        ),
        ("ollama", answer("ollama answered"), None),
    ];
    for (provider, reply, expected) in cases {
        let mock = Mock::start(vec![reply]);
        let mut command = store.plain_command();
        for (variable, key) in &keys {
            command.env(variable, key);
        }
        point_at(&mut command, &mock);
        let (out, status) = run_merged(
            command,
            &store.script(&[format!(
                ".agent ask \"who are you?\" provider {provider} model mock-model"
            )]),
        );
        assert!(status.success(), "{provider} run failed:\n{out}");
        assert!(out.contains(&format!("{provider} answered")), "{out}");
        let requests = mock.requests();
        assert_eq!(requests.len(), 1, "{provider}: one request");
        let request = &requests[0];
        let head = request.head.to_lowercase();
        let path = if provider == "anthropic" {
            "post /v1/messages "
        } else {
            "post /v1/chat/completions "
        };
        assert!(head.starts_with(path), "{provider}: {head}");
        match &expected {
            Some(header) => assert!(
                head.contains(&header.to_lowercase()),
                "{provider} did not get its own key in {header:?}: {head}"
            ),
            None => {
                assert!(!head.contains("authorization"), "{provider}: {head}");
                assert!(!head.contains("x-api-key"), "{provider}: {head}");
            }
        }
        // Exactly one credential header, and no stranger's key anywhere.
        let credentials = head
            .lines()
            .filter(|line| line.starts_with("authorization:") || line.starts_with("x-api-key:"))
            .count();
        assert_eq!(
            credentials,
            usize::from(expected.is_some()),
            "{provider}: {head}"
        );
        for (variable, key) in &keys {
            let own = expected
                .as_deref()
                .is_some_and(|header| header.ends_with(key.as_str()));
            let in_head = head.contains(&key.to_lowercase());
            assert_eq!(
                in_head, own,
                "{provider}: {variable} in the headers: {head}"
            );
            assert!(
                !request.body.contains(key.as_str()),
                "{provider}: {variable} in the body"
            );
        }
    }
}

#[test]
fn a_killed_run_resumes_from_its_checkpoint() {
    let store = Store::new("resume");
    let first = Mock::start(vec![
        tool_calls(&[(
            "call_1",
            "query",
            r#"{"sql":"SELECT count(*) FROM customers"}"#,
        )]),
        Reply::Stall,
    ]);
    // Start the run, wait until step 1 is checkpointed (the second request),
    // then kill it like a crash.
    let mut command = store.command(&first);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut child = command.spawn().expect("spawn pinkdb");
    let script = store.script(&[".agent ask \"how many customers?\"".to_string()]);
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(script.as_bytes())
        .expect("feed the script");
    let deadline = Instant::now() + Duration::from_secs(60);
    while first.accepted() < 2 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(first.accepted(), 2, "the tool step was checkpointed");
    child.kill().expect("kill the run");
    child.wait().expect("reap the killed run");

    // Replay and checkpoint once, so the store opens without needing to load
    // the agent from the WAL (which would collide with the script's LOAD).
    store.fold();

    // The session row survived the kill; resume it with a fresh provider.
    let id = store.cell("SELECT id FROM pinkdb_agent.sessions");
    assert!(id.starts_with('a'), "unexpected id {id:?}");
    let second = Mock::start(vec![answer("5 customers.")]);
    let (out, code) = store.run(&second, &[format!(".agent resume {id}")]);
    assert_eq!(code, 0, "resume failed:\n{out}");
    assert!(out.contains("5 customers."), "output: {out}");

    assert_eq!(mock_text(&store, "steps"), "2");
    assert_eq!(mock_text(&store, "status"), "done");
    assert_eq!(
        store.cell("SELECT count(*) FROM pinkdb_agent.tool_calls"),
        "1",
        "the tool call is not duplicated"
    );
    assert_eq!(
        store.cell("SELECT count(*) FROM pinkdb_agent.messages"),
        "4",
        "question, tool call, tool result, answer"
    );
    // The resumed request replays the recorded tool result instead of
    // re-running it.
    let messages = second.bodies()[0]["messages"].clone();
    assert!(
        messages.to_string().contains("| 5 |") || messages.to_string().contains('5'),
        "the recorded result is in the request: {messages}"
    );
}

fn mock_text(store: &Store, column: &str) -> String {
    store.cell(&format!("SELECT {column} FROM pinkdb_agent.sessions"))
}

#[test]
fn a_fork_copies_the_prefix_and_diverges() {
    let store = Store::new("fork");
    let first = Mock::start(vec![
        tool_calls(&[(
            "call_1",
            "query",
            r#"{"sql":"SELECT count(*) FROM customers"}"#,
        )]),
        answer("5 customers."),
    ]);
    let out = store.ask(&first, "how many customers?");
    assert!(out.contains("5 customers."), "output: {out}");
    let original = store.cell("SELECT id FROM pinkdb_agent.sessions");

    let second = Mock::start(vec![answer("Bravo is second.")]);
    let (out, code) = store.run(
        &second,
        &[format!(".agent fork {original} 2 \"who is second?\"")],
    );
    assert_eq!(code, 0, "fork failed:\n{out}");
    assert!(out.contains("Bravo is second."), "output: {out}");

    assert_eq!(
        store.cell("SELECT count(*) FROM pinkdb_agent.sessions"),
        "2"
    );
    // The original is untouched.
    assert_eq!(
        store.cell(&format!(
            "SELECT steps FROM pinkdb_agent.sessions WHERE id = '{original}'"
        )),
        "2"
    );
    assert_eq!(
        store.cell(&format!(
            "SELECT count(*) FROM pinkdb_agent.messages WHERE session = '{original}'"
        )),
        "4"
    );
    // The fork records where it came from and carries the new answer.
    let fork = store.cell(&format!(
        "SELECT id FROM pinkdb_agent.sessions WHERE origin = '{original}'"
    ));
    assert!(fork.starts_with('a'), "no fork row: {fork}");
    assert_eq!(
        store.cell(&format!(
            "SELECT origin_step FROM pinkdb_agent.sessions WHERE id = '{fork}'"
        )),
        "2"
    );
    let fork_messages = store.query(&format!(
        "SELECT role, content FROM pinkdb_agent.messages WHERE session = '{fork}' ORDER BY step, seq"
    ));
    assert!(
        fork_messages.contains("Bravo is second."),
        "{fork_messages}"
    );
    // The request the fork sent starts from the copied prefix.
    let body = &second.bodies()[0];
    let text = body["messages"].to_string();
    assert!(text.contains("how many customers?"), "fork request: {text}");
    assert!(text.contains("who is second?"), "fork request: {text}");
}

#[test]
fn every_write_is_refused_by_default() {
    let store = Store::new("readonly");
    let copy = store.dir.join("out.csv");
    let copy_sql = format!("COPY customers TO '{}'", copy.display());
    let load_sql = format!("LOAD EXTENSION '{}'", store.dir.join("nope").display());
    let statements = [
        "INSERT INTO customers VALUES (99, 'Mallory')",
        "CREATE TABLE scratch (a INT)",
        copy_sql.as_str(),
        load_sql.as_str(),
        "DROP DATABASE pinkdb_agent",
        "INSERT INTO pinkdb_agent.sessions (id) VALUES ('x')",
    ];
    let calls: Vec<(String, String, String)> = statements
        .iter()
        .enumerate()
        .map(|(index, sql)| {
            (
                format!("call_{index}"),
                "query".to_string(),
                json!({"sql": sql}).to_string(),
            )
        })
        .collect();
    let calls: Vec<(&str, &str, &str)> = calls
        .iter()
        .map(|(id, name, arguments)| (id.as_str(), name.as_str(), arguments.as_str()))
        .collect();
    // `.lake` reaches the store as a dot command, which the tool channel
    // refuses before the policy even sees it.
    let mut calls = calls;
    calls.push((
        "call_lake",
        "query",
        r#"{"sql":".lake bulk_commit \"cat\""}"#,
    ));
    let mock = Mock::start(vec![tool_calls(&calls), answer("refused")]);
    let out = store.ask(&mock, "try every write");
    assert!(out.contains("refused"), "output: {out}");

    assert_eq!(
        store.cell("SELECT count(*) FROM pinkdb_agent.tool_calls"),
        "7"
    );
    assert_eq!(
        store.cell("SELECT count(*) FROM pinkdb_agent.tool_calls WHERE code = 1"),
        "7",
        "every write is refused: {}",
        store.query("SELECT name, code, reply FROM pinkdb_agent.tool_calls")
    );
    // The read-only allowlist refuses every one of them before the store is
    // touched, the lake dot command included.
    let replies = store.query("SELECT reply FROM pinkdb_agent.tool_calls");
    // Six permission refusals and the lake dot command: a plain build has no
    // lake hook, a pinklake build refuses it as read-only. Both are errors.
    assert_eq!(
        replies.matches("\"error\"").count(),
        7,
        "replies: {replies}"
    );
    assert!(replies.contains(".lake"), "replies: {replies}");
    // Nothing was written: the fixture is unchanged.
    assert_eq!(store.cell("SELECT count(*) FROM customers"), "5");
    assert!(!copy.exists(), "COPY wrote a file");
}

#[test]
fn allow_writes_permits_the_data_but_not_the_transcript() {
    let store = Store::new("writes");
    let calls = vec![
        (
            "call_1",
            "query",
            r#"{"sql":"INSERT INTO customers VALUES (99, 'Mallory')"}"#,
        ),
        (
            "call_2",
            "query",
            r#"{"sql":"CREATE TABLE scratch (a INT)"}"#,
        ),
        (
            "call_3",
            "query",
            r#"{"sql":"UPDATE customers SET name = 'X' WHERE id = 1"}"#,
        ),
        (
            "call_4",
            "query",
            r#"{"sql":"INSERT INTO pinkdb_agent.sessions (id) VALUES ('x')"}"#,
        ),
    ];
    let mock = Mock::start(vec![tool_calls(&calls), answer("done")]);
    let (out, code) = store.run(
        &mock,
        &[".agent ask \"write something\" allow-writes".to_string()],
    );
    assert_eq!(code, 0, "the run failed:\n{out}");
    assert!(out.contains("done"), "output: {out}");
    assert_eq!(
        store.cell("SELECT count(*) FROM pinkdb_agent.tool_calls WHERE code = 0"),
        "3",
        "DML and DDL succeed: {}",
        store.query("SELECT name, code, reply FROM pinkdb_agent.tool_calls")
    );
    assert_eq!(
        store.cell("SELECT count(*) FROM pinkdb_agent.tool_calls WHERE code = 1"),
        "1"
    );
    // The transcript stays off-limits even to a model that may write.
    let refusal = store.query("SELECT reply FROM pinkdb_agent.tool_calls WHERE code = 1");
    assert!(
        refusal.contains("the agent's own tables"),
        "the agent's tables have their own refusal: {refusal}"
    );
    assert_eq!(store.cell("SELECT count(*) FROM customers"), "6");
    assert_eq!(store.cell("SELECT name FROM customers WHERE id = 1"), "X");
    assert_eq!(
        store.cell("SELECT allow_writes FROM pinkdb_agent.sessions"),
        "true"
    );
}

#[test]
fn the_commands_need_the_extension_loaded() {
    let store = Store::new("notloaded");
    let path = store.path.display();
    let script = format!(".open {path}\n.agent ask \"anything\"\n");
    let (out, _) = run_merged(Command::new(cli()), &script);
    assert!(
        out.contains("the agent extension is not loaded"),
        "output: {out}"
    );
    assert!(
        out.contains(library_name()),
        "the hint names the library: {out}"
    );
}

#[test]
fn budgets_stop_a_run() {
    let store = Store::new("budget");
    let calls = vec![("call_1", "databases", "{}")];
    let mut replies = Vec::new();
    for _ in 0..5 {
        replies.push(tool_calls(&calls));
    }
    let mock = Mock::start(replies);
    let (out, code) = store.run(
        &mock,
        &[".agent ask \"loop forever\" max-steps 2".to_string()],
    );
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("stopped: max steps (2)"), "output: {out}");
    assert_eq!(
        store.cell("SELECT status FROM pinkdb_agent.sessions"),
        "stopped"
    );
    assert_eq!(store.cell("SELECT steps FROM pinkdb_agent.sessions"), "2");
    assert_eq!(
        store.cell("SELECT max_steps FROM pinkdb_agent.sessions"),
        "2"
    );
    assert_eq!(mock.accepted(), 2, "the budget stops further calls");
}

// ------------------------------------------------------- providers (wave 106b)

/// A run with nothing but `extra` in the provider environment, against `mock`
/// as its API root.
fn run_with(store: &Store, mock: &Mock, extra: &[(&str, &str)], line: &str) -> (String, i32) {
    let mut command = store.plain_command();
    point_at(&mut command, mock);
    for (variable, value) in extra {
        command.env(variable, value);
    }
    let (out, status) = run_merged(command, &store.script(&[line.to_string()]));
    (out, status.code().unwrap_or(-1))
}

#[test]
fn no_provider_is_an_error_even_with_a_key_exported() {
    let store = Store::new("noprovider");
    let mock = Mock::start(vec![answer("never")]);
    let key = sentinel("noprov");
    let (out, _) = run_with(
        &store,
        &mock,
        &[("ZAI_API_KEY", &key), ("ANTHROPIC_API_KEY", &key)],
        ".agent ask \"anything\"",
    );
    assert!(
        out.contains("no provider: pass provider <name> or set PINKDB_AGENT_PROVIDER"),
        "{out}"
    );
    assert!(out.contains("one of: zai, anthropic"), "{out}");
    assert_eq!(mock.accepted(), 0, "no provider was guessed from a key");
    assert!(!out.contains(&key), "{out}");
}

#[test]
fn an_unknown_provider_lists_the_valid_ones() {
    let store = Store::new("unknown");
    let mock = Mock::start(vec![answer("never")]);
    let (out, _) = run_with(&store, &mock, &[], ".agent ask \"q\" provider groq");
    assert!(out.contains("unknown provider \"groq\""), "{out}");
    assert!(out.contains("zai, anthropic, openai"), "{out}");
    assert_eq!(mock.accepted(), 0);
}

#[test]
fn a_provider_without_a_default_model_needs_one() {
    let store = Store::new("nomodel");
    let mock = Mock::start(vec![answer("never")]);
    let (out, _) = run_with(
        &store,
        &mock,
        &[("OPENROUTER_API_KEY", "sk-unused")],
        ".agent ask \"q\" provider openrouter",
    );
    assert!(
        out.contains("provider openrouter has no default model"),
        "{out}"
    );
    assert!(out.contains("model <name>"), "{out}");
    assert!(out.contains("PINKDB_AGENT_MODEL"), "{out}");
    assert_eq!(mock.accepted(), 0);
}

#[test]
fn a_run_argument_beats_the_environment() {
    let store = Store::new("precedence");
    let mock = Mock::start(vec![anthropic_answer("from anthropic")]);
    let (out, code) = run_with(
        &store,
        &mock,
        &[
            ("PINKDB_AGENT_PROVIDER", "zai"),
            ("PINKDB_AGENT_MODEL", "glm-env"),
            ("ZAI_API_KEY", "sk-zai-unused"),
            ("ANTHROPIC_API_KEY", "sk-anthropic-used"),
        ],
        ".agent ask \"q\" provider anthropic model claude-arg",
    );
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("from anthropic"), "{out}");
    let request = &mock.requests()[0];
    assert!(
        request.head.starts_with("POST /v1/messages "),
        "{}",
        request.head
    );
    let body = &mock.bodies()[0];
    assert_eq!(body["model"], json!("claude-arg"));
    assert_eq!(
        store.cell("SELECT provider FROM pinkdb_agent.sessions"),
        "anthropic"
    );
    assert_eq!(
        store.cell("SELECT model FROM pinkdb_agent.sessions"),
        "claude-arg"
    );

    // With no argument the environment decides, and zai keeps its default model.
    let store = Store::new("precedence-env");
    let mock = Mock::start(vec![answer("from zai")]);
    let (out, code) = run_with(
        &store,
        &mock,
        &[("PINKDB_AGENT_PROVIDER", "zai"), ("ZAI_API_KEY", "sk-zai")],
        ".agent ask \"q\"",
    );
    assert_eq!(code, 0, "{out}");
    assert_eq!(mock.bodies()[0]["model"], json!("glm-5.3"));
}

/// `--agent FILE "q" --agent-provider … --agent-model …` reaches the same
/// grammar as `.agent ask … provider … model …`.
#[test]
fn the_cli_flags_select_the_provider() {
    let store = Store::new("cliflags");
    // Leave the WAL folded so `--agent` does not have to replay a LOAD.
    store.fold();
    let mock = Mock::start(vec![anthropic_answer("flagged")]);
    let mut command = store.plain_command();
    point_at(&mut command, &mock)
        .env("ANTHROPIC_API_KEY", sentinel("flags"))
        .arg("--agent")
        .arg(&store.path)
        .arg("how many customers?")
        .arg("--agent-provider")
        .arg("anthropic")
        .arg("--agent-model")
        .arg("claude-flag");
    let output = command
        .stdin(Stdio::null())
        .output()
        .expect("run pinkdb --agent");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    // `--agent` loads the library installed next to the binary, which is
    // where `agent_library()` (called by `Store::new`) built it.
    assert!(output.status.success(), "{out}");
    assert!(out.contains("flagged"), "{out}");
    assert_eq!(mock.bodies()[0]["model"], json!("claude-flag"));
    let flagless = Command::new(cli())
        .arg("--agent-provider")
        .arg("zai")
        .stdin(Stdio::null())
        .output()
        .expect("run pinkdb");
    assert!(!flagless.status.success());
    assert!(
        String::from_utf8_lossy(&flagless.stderr).contains("configure one --agent FILE"),
        "{}",
        String::from_utf8_lossy(&flagless.stderr)
    );
}

#[test]
fn an_anthropic_tool_loop_runs_real_queries() {
    let store = Store::new("anthropic-loop");
    let mock = Mock::start(vec![
        anthropic_tool_calls(&[
            ("toolu_1", "tables", r#"{"database":"main"}"#),
            (
                "toolu_2",
                "query",
                r#"{"sql":"SELECT count(*) AS n FROM customers"}"#,
            ),
        ]),
        anthropic_answer("There are 5 customers."),
    ]);
    let key = sentinel("anthropic-loop");
    let (out, code) = run_with(
        &store,
        &mock,
        &[
            ("PINKDB_AGENT_PROVIDER", "anthropic"),
            ("ANTHROPIC_API_KEY", &key),
        ],
        ".agent ask \"how many customers?\"",
    );
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("There are 5 customers."), "{out}");
    assert_eq!(mock.accepted(), 2);
    let second = &mock.bodies()[1];
    // The system prompt is hoisted; the results ride in one user message.
    assert!(
        second["system"]
            .as_str()
            .unwrap_or_default()
            .contains("read-only"),
        "{second}"
    );
    let messages = second["messages"].as_array().expect("messages");
    let roles: Vec<&str> = messages
        .iter()
        .map(|m| m["role"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(roles, ["user", "assistant", "user"], "{second}");
    let results = messages[2]["content"].as_array().expect("result blocks");
    assert_eq!(results.len(), 2, "{second}");
    assert_eq!(results[0]["tool_use_id"], json!("toolu_1"));
    assert_eq!(results[1]["tool_use_id"], json!("toolu_2"));
    assert!(
        results[1]["content"]
            .as_str()
            .unwrap_or_default()
            .contains('5'),
        "{second}"
    );
    assert_eq!(
        store.cell("SELECT count(*) FROM pinkdb_agent.usage WHERE provider = 'anthropic' AND model = 'claude-sonnet-5-5'"),
        "2",
        "usage is attributed per step"
    );
    assert!(!out.contains(&key), "{out}");
}

/// A 529 from Anthropic is retried like a 5xx, through the whole stack.
#[test]
fn an_anthropic_529_is_retried() {
    let store = Store::new("529");
    let mock = Mock::start(vec![
        Reply::Status(
            529,
            r#"{"type":"error","error":{"type":"overloaded_error"}}"#.to_string(),
        ),
        anthropic_answer("after the overload"),
    ]);
    let (out, code) = run_with(
        &store,
        &mock,
        &[("ANTHROPIC_API_KEY", "sk-x")],
        ".agent ask \"q\" provider anthropic",
    );
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("after the overload"), "{out}");
    assert!(out.contains("overloaded (HTTP 529)"), "{out}");
    assert_eq!(mock.accepted(), 2);
}

/// A `PINKDB_AGENT_BASE_URL` left over from a `custom` run never carries
/// `ANTHROPIC_API_KEY` to that host: anthropic reads only its own root.
#[test]
fn the_custom_root_never_receives_a_builtin_providers_key() {
    let store = Store::new("custom-root");
    let anthropic = Mock::start(vec![anthropic_answer("from the anthropic root")]);
    let custom = Mock::start(vec![answer("hijacked")]);
    let key = sentinel("custom-root");
    let mut command = store.plain_command();
    command
        .env("PINKDB_AGENT_BASE_URL", custom.base())
        .env("PINKDB_AGENT_ANTHROPIC_BASE_URL", anthropic.base())
        .env("ANTHROPIC_API_KEY", &key);
    let (out, status) = run_merged(
        command,
        &store.script(&[".agent ask \"q\" provider anthropic".to_string()]),
    );
    assert!(status.success(), "{out}");
    assert!(out.contains("from the anthropic root"), "{out}");
    let requests = anthropic.requests();
    assert_eq!(requests.len(), 1, "{out}");
    assert!(
        requests[0]
            .head
            .to_lowercase()
            .contains(&format!("x-api-key: {key}")),
        "{}",
        requests[0].head
    );
    assert_eq!(custom.accepted(), 0, "the custom root was contacted");
    assert!(
        custom
            .requests()
            .iter()
            .all(|request| !request.head.contains(&key) && !request.body.contains(&key)),
        "the anthropic key reached the custom root"
    );
}

#[test]
fn resume_keeps_the_sessions_provider() {
    let store = Store::new("resume-provider");
    // An Anthropic run that fails on its first call leaves a resumable session.
    let first = Mock::start(vec![
        Reply::Status(500, "{}".to_string()),
        Reply::Status(500, "{}".to_string()),
        Reply::Status(500, "{}".to_string()),
    ]);
    let (out, _) = run_with(
        &store,
        &first,
        &[("ANTHROPIC_API_KEY", "sk-a")],
        ".agent ask \"how many?\" provider anthropic model claude-recorded",
    );
    assert!(out.contains("HTTP 500"), "{out}");
    let id = store.cell("SELECT id FROM pinkdb_agent.sessions");
    assert_eq!(
        store.cell("SELECT status FROM pinkdb_agent.sessions"),
        "failed"
    );

    // The resume names no provider, and the environment names a different
    // one: the session's record wins over the environment.
    let second = Mock::start(vec![anthropic_answer("resumed on anthropic")]);
    let (out, code) = run_with(
        &store,
        &second,
        &[
            ("PINKDB_AGENT_PROVIDER", "zai"),
            ("ZAI_API_KEY", "sk-z"),
            ("ANTHROPIC_API_KEY", "sk-a"),
        ],
        &format!(".agent resume {id}"),
    );
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("resumed on anthropic"), "{out}");
    let request = &second.requests()[0];
    assert!(
        request.head.starts_with("POST /v1/messages "),
        "{}",
        request.head
    );
    assert_eq!(second.bodies()[0]["model"], json!("claude-recorded"));
    assert_eq!(
        store.cell("SELECT provider FROM pinkdb_agent.sessions"),
        "anthropic"
    );
}

/// A z.ai session forked onto Anthropic: the recorded transcript is re-encoded
/// for the other protocol, and the original is untouched.
#[test]
fn a_fork_moves_a_session_from_zai_to_anthropic() {
    let store = Store::new("fork-provider");
    let zai = Mock::start(vec![
        tool_calls(&[(
            "call_1",
            "query",
            r#"{"sql":"SELECT count(*) FROM customers"}"#,
        )]),
        answer("5 customers."),
    ]);
    let out = store.ask(&zai, "how many customers?");
    assert!(out.contains("5 customers."), "{out}");
    let original = store.cell("SELECT id FROM pinkdb_agent.sessions");
    assert_eq!(
        store.cell("SELECT provider FROM pinkdb_agent.sessions"),
        "zai"
    );
    let before = store.query(&format!(
        "SELECT step, seq, role, content FROM pinkdb_agent.messages WHERE session = '{original}' ORDER BY step, seq"
    ));

    let anthropic = Mock::start(vec![anthropic_answer("Bravo is second.")]);
    let (out, code) = run_with(
        &store,
        &anthropic,
        &[
            ("ANTHROPIC_API_KEY", "sk-fork"),
            ("ZAI_API_KEY", "sk-zai-fork"),
        ],
        &format!(".agent fork {original} 2 provider anthropic \"who is second?\""),
    );
    assert_eq!(code, 0, "fork failed:\n{out}");
    assert!(out.contains("Bravo is second."), "{out}");

    // The fork's request is the z.ai transcript in Anthropic's shape.
    let request = &anthropic.requests()[0];
    assert!(
        request.head.starts_with("POST /v1/messages "),
        "{}",
        request.head
    );
    assert!(!request.body.contains("sk-zai-fork"), "{}", request.body);
    let body = &anthropic.bodies()[0];
    assert_eq!(body["model"], json!("claude-sonnet-5-5"));
    assert!(body["system"].is_string(), "{body}");
    let messages = body["messages"].as_array().expect("messages");
    let roles: Vec<&str> = messages
        .iter()
        .map(|m| m["role"].as_str().unwrap_or_default())
        .collect();
    // question, the tool call, its result, the answer, then the new question.
    assert_eq!(
        roles,
        ["user", "assistant", "user", "assistant", "user"],
        "{body}"
    );
    assert_eq!(messages[1]["content"][0]["type"], json!("tool_use"));
    assert_eq!(messages[1]["content"][0]["id"], json!("call_1"));
    assert_eq!(messages[2]["content"][0]["type"], json!("tool_result"));
    assert_eq!(messages[2]["content"][0]["tool_use_id"], json!("call_1"));
    assert!(body.to_string().contains("who is second?"), "{body}");

    // The original is untouched; the fork records the switch.
    assert_eq!(
        store.query(&format!(
            "SELECT step, seq, role, content FROM pinkdb_agent.messages WHERE session = '{original}' ORDER BY step, seq"
        )),
        before
    );
    assert_eq!(
        store.cell(&format!(
            "SELECT provider FROM pinkdb_agent.sessions WHERE id = '{original}'"
        )),
        "zai"
    );
    let fork = store.cell(&format!(
        "SELECT id FROM pinkdb_agent.sessions WHERE origin = '{original}'"
    ));
    assert_eq!(
        store.cell(&format!(
            "SELECT provider FROM pinkdb_agent.sessions WHERE id = '{fork}'"
        )),
        "anthropic"
    );
    // The copied steps keep the provider that earned them; the new step is
    // Anthropic's.
    assert_eq!(
        store.cell(&format!(
            "SELECT count(*) FROM pinkdb_agent.usage WHERE session = '{fork}' AND provider = 'zai'"
        )),
        "2"
    );
    assert_eq!(
        store.cell(&format!(
            "SELECT count(*) FROM pinkdb_agent.usage WHERE session = '{fork}' AND provider = 'anthropic'"
        )),
        "1"
    );
}

#[test]
fn providers_lists_the_table_and_never_a_key() {
    let store = Store::new("providers");
    let mock = Mock::start(Vec::new());
    let keys = all_sentinels("list");
    let mut command = store.plain_command();
    for (variable, key) in &keys {
        command.env(variable, key);
    }
    let openrouter_root = "http://127.0.0.1:9/openrouter-proxy";
    command
        .env("PINKDB_AGENT_BASE_URL", mock.base())
        .env("PINKDB_AGENT_OPENROUTER_BASE_URL", openrouter_root);
    let (out, status) = run_merged(command, &store.script(&[".agent providers".to_string()]));
    assert!(status.success(), "{out}");
    let line = |name: &str| {
        out.lines()
            .find(|line| line.split_whitespace().next() == Some(name))
            .unwrap_or_else(|| panic!("no {name} line in:\n{out}"))
            .to_string()
    };
    // Each row names its own override variable and shows the root in effect:
    // custom's and openrouter's are overridden, nobody else's moved.
    for name in [
        "zai",
        "anthropic",
        "openai",
        "deepseek",
        "openrouter",
        "gemini",
        "ollama",
    ] {
        let variable = format!("PINKDB_AGENT_{}_BASE_URL", name.to_uppercase());
        assert!(line(name).contains(&variable), "{out}");
        assert!(!line(name).contains(&mock.base()), "{out}");
    }
    assert!(line("custom").contains("PINKDB_AGENT_BASE_URL"), "{out}");
    assert!(line("custom").contains(&mock.base()), "{out}");
    assert!(line("openrouter").contains(openrouter_root), "{out}");
    assert!(
        !line("openrouter").contains("https://openrouter.ai"),
        "{out}"
    );
    let zai = line("zai");
    assert!(zai.contains("openai"), "{zai}");
    assert!(zai.contains("https://api.z.ai/api/paas/v4"), "{zai}");
    assert!(zai.contains("glm-5.3"), "{zai}");
    assert!(zai.contains("key: set"), "{zai}");
    let anthropic = line("anthropic");
    assert!(
        anthropic.contains("https://api.anthropic.com/v1"),
        "{anthropic}"
    );
    assert!(anthropic.contains("claude-sonnet-5-5"), "{anthropic}");
    assert!(anthropic.contains("key: set"), "{anthropic}");
    assert!(line("openai").contains("key: missing"), "{out}");
    assert!(line("openai").contains("(model required)"), "{out}");
    assert!(line("openrouter").contains("key: set"), "{out}");
    assert!(line("ollama").contains("no key needed"), "{out}");
    assert!(line("custom").contains("key: set"), "{out}");
    for name in ["deepseek", "gemini"] {
        assert!(line(name).contains("key: missing"), "{out}");
    }
    for (variable, key) in &keys {
        assert!(!out.contains(key.as_str()), "{variable} printed:\n{out}");
        // Not a prefix of it either.
        assert!(
            !out.contains(&key[..key.len() - 4]),
            "{variable} prefix:\n{out}"
        );
    }
    assert!(out.contains("there is no default provider"), "{out}");
    assert_eq!(mock.accepted(), 0, "listing calls no provider");
}

/// The live providers: only with `PINKDB_AGENT_LIVE=1` and that provider's key
/// exported. Never run in CI, and never with a key checked into the
/// repository. The run names its provider explicitly, so only that provider's
/// key variable is read.
fn live(provider: &str, variables: &[&str]) {
    if std::env::var("PINKDB_AGENT_LIVE").ok().as_deref() != Some("1") {
        return;
    }
    let has_key = variables.iter().any(|name| {
        std::env::var(name)
            .ok()
            .is_some_and(|value| !value.trim().is_empty())
    });
    if !has_key {
        eprintln!(
            "no {} exported; the live test has nothing to run",
            variables.join(" or ")
        );
        return;
    }
    let store = Store::new(&format!("live-{provider}"));
    let path = store.path.display();
    let script = format!(
        ".open {path}\nLOAD EXTENSION '{}';\n.agent ask \"How many rows are in the customers table? Answer with the number.\" provider {provider}\n",
        agent_library().display()
    );
    let mut command = Command::new(cli());
    command.arg("--allow-load-extension");
    let (out, status) = run_merged(command, &script);
    assert!(status.success(), "the live {provider} run failed:\n{out}");
    assert!(out.contains('5'), "the answer should be 5:\n{out}");
}

#[test]
#[ignore = "needs PINKDB_AGENT_LIVE=1, a real key in ZAI_API_KEY or Z_API_KEY, and network"]
fn a_live_zai_provider_answers() {
    live("zai", &["ZAI_API_KEY", "Z_API_KEY"]);
}

#[test]
#[ignore = "needs PINKDB_AGENT_LIVE=1, a real key in ANTHROPIC_API_KEY, and network"]
fn a_live_anthropic_provider_answers() {
    live("anthropic", &["ANTHROPIC_API_KEY"]);
}

#[test]
fn an_agent_run_records_its_turns_in_memory() {
    let store = Store::new("memory");
    let mock = Mock::start(vec![answer("There are 5 customers.")]);
    let lines = [
        ".agent ask \"how many customers are there?\"".to_string(),
        ".memory show".to_string(),
    ];
    let (out, status) = run_merged(store.command(&mock), &store.memory_script(&lines));
    assert_eq!(status.code(), Some(0), "the run failed:\n{out}");
    // The question and the answer are both L0 turns under the caller's
    // identity, so a later run starts from what this one said.
    assert!(out.contains("turns: 2"), "output: {out}");

    let quiet = Mock::start(Vec::new());
    let (found, status) = run_merged(
        store.command(&quiet),
        &store.memory_script(&[".memory search customers".to_string()]),
    );
    assert_eq!(status.code(), Some(0), "{found}");
    assert!(found.contains("how many customers are there"), "{found}");
    assert!(found.contains("There are 5 customers"), "{found}");
}

/// Two `LOAD EXTENSION` statements followed by two more, in one
/// non-interactive script, with the agent's library first: every line runs.
///
/// Wave 109 recorded the opposite for this shape — the second load and the
/// statement after it apparently dropped — and could not reproduce it later
/// with the same order. A statement is silently swallowed only when an earlier
/// one was left unterminated (`Input` keeps buffering until a line's last code
/// token is `;`, so the next line joins it), and `LOAD` lines from an earlier
/// draft of this harness lacked that `;`. This test pins the shape so a
/// dropped load or statement fails loudly instead of reading as "no output".
#[test]
fn loading_the_agent_before_memory_still_runs_every_statement() {
    let store = Store::new("load-order");
    let path = store.path.display();
    let script = format!(
        ".open {path}\nLOAD EXTENSION '{}';\nLOAD EXTENSION '{}';\n\
         SELECT 'first' AS marker;\nSELECT 'second' AS marker;\n.save {path}\n",
        agent_library().display(),
        memory_library().display()
    );
    let (out, status) = run_merged(store.plain_command(), &script);
    assert!(status.success(), "the script must succeed:\n{out}");
    for expected in [
        "Extension agent loaded.",
        "Extension memory loaded.",
        "first",
        "second",
    ] {
        assert!(out.contains(expected), "missing {expected:?} in:\n{out}");
    }
}
