//! Test plumbing for the memory extension: a store, one session per
//! principal, a scripted provider, and a loopback embeddings endpoint.
//!
//! Nothing here leaves the loopback interface. The embedding provider is a
//! plain TCP listener this process owns, and the "model" in the distillation
//! tests is a `Provider` implementation that replays a script, so no test ever
//! calls a real provider.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use pinkdb::serve::{ServePolicy, SessionHost};
use pinkdb::{Connection, Database, PinkDb, QueryResult};
use pinkdb_ext_loader::serve::StatementHost;
use pinkdb_llm::{Message, Provider, Turn, Usage};
use pinkdb_memory::host::Host as MemoryHost;
use serde_json::{Value, json};

/// The password every test user has.
pub const PASSWORD: &str = "memory-test-secret";
/// The database the extension owns; the policy below confines it.
pub const DATABASE: &str = "pinkdb_memory";

/// One store, shared by every session a test opens over it.
///
/// A `Database` (not a `PinkDb`) because that is what hands out
/// [`Connection`]s, and the ACL cases need real logins. Trusted statements —
/// the administrative path — run on a never-logged-in connection, exactly as
/// the CLI's bootstrap does.
pub struct Store {
    pub db: Database,
}

impl Store {
    /// A fresh in-memory store.
    pub fn memory() -> Store {
        Store {
            db: Database::from_store(PinkDb::new()),
        }
    }

    /// A trusted connection: no principal, so no privilege check.
    pub fn connect(&self) -> Connection {
        self.db.connect()
    }

    /// Run one administrative statement.
    pub fn sql(&self, sql: &str) -> Vec<QueryResult> {
        self.connect()
            .execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    /// Run one statement that must fail, and answer why.
    pub fn error(&self, sql: &str) -> String {
        self.connect()
            .execute(sql)
            .map(|_| String::new())
            .unwrap_or_else(|e| e.to_string())
    }

    /// Load a linked extension package (the registry name, not a path).
    pub fn load(&self, name: &str) {
        self.db.set_allow_sql_load_extension(true);
        let mut conn = self.connect();
        conn.execute(&format!("LOAD EXTENSION '{name}'"))
            .unwrap_or_else(|e| panic!("load {name}: {e}"));
    }
}

/// A durable store in a scratch directory; the directory is removed when the
/// guard drops.
pub struct Durable {
    pub dir: PathBuf,
    pub store: Store,
}

impl Drop for Durable {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Open a durable store under `$TMPDIR/pinkdb_memory_<tag>_<pid>_<seq>`.
pub fn durable(tag: &str) -> Durable {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_memory_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    let db = PinkDb::open_durable(dir.join("db.pink").to_str().unwrap()).unwrap();
    Durable {
        dir,
        store: Store {
            db: Database::from_store(db),
        },
    }
}

/// The policy the command and the bridge run under, as the CLI sets it: the
/// public channel cannot read or write memory, and the internal channel reads
/// and writes memory only.
pub fn policy() -> ServePolicy {
    ServePolicy {
        deny_read_databases: vec![DATABASE.to_string()],
        deny_write_databases: vec![DATABASE.to_string()],
        internal: Some(Box::new(ServePolicy {
            allow_writes: true,
            write_database: Some(DATABASE.to_string()),
            read_databases: Some(vec![DATABASE.to_string()]),
            // Embeddings run through the internal channel, and an embedding
            // call is an external service: the trusted channel is the one
            // allowed to reach it, while files stay closed to it.
            allow_external_services: true,
            ..ServePolicy::default()
        })),
        ..ServePolicy::default()
    }
}

/// A store with the two users, two roles and memberships the ACL tests need.
/// The roles the ACL tests use: alice is in t1, bob in t2. A store without
/// users is a store where every principal is trusted, so the tests that name a
/// user install these first.
pub fn users(store: &Store) {
    for sql in [
        "CREATE USER IF NOT EXISTS alice PASSWORD 'memory-test-secret'",
        "CREATE USER IF NOT EXISTS bob PASSWORD 'memory-test-secret'",
        "CREATE ROLE t1",
        "CREATE ROLE t2",
        "GRANT t1 TO alice",
        "GRANT t2 TO bob",
    ] {
        let _ = store.connect().execute(sql);
    }
}

/// One principal's session: the public channel (a real `Connection`, so RBAC
/// applies) and the extension's internal channel (the same `SessionHost` the
/// server builds), over one store.
pub struct TestHost {
    conn: Connection,
    policy: ServePolicy,
    /// Channel 0 output, in order, for the command tests.
    pub out: Vec<String>,
    /// Channel 1 output (diagnostics), in order.
    pub diag: Vec<String>,
}

impl TestHost {
    /// A session for `user`, or a trusted embedded session when `user` is
    /// `None`.
    pub fn open(store: &Store, user: Option<&str>) -> TestHost {
        let mut conn = store.connect();
        if let Some(user) = user {
            if !store.db.has_users() {
                users(store);
            }
            conn.login(user, PASSWORD).expect("login");
        }
        TestHost {
            conn,
            policy: policy(),
            out: Vec::new(),
            diag: Vec::new(),
        }
    }

    /// The internal channel's raw reply.
    pub fn raw_internal(&mut self, sql: &str) -> (i32, String) {
        let mut host = SessionHost::new(&mut self.conn, &self.policy);
        StatementHost::execute_internal(&mut host, sql)
    }

    /// The public channel's raw reply.
    pub fn raw_statement(&mut self, sql: &str) -> (i32, String) {
        let mut host = SessionHost::new(&mut self.conn, &self.policy);
        StatementHost::execute_statement(&mut host, sql)
    }

    /// One internal query, the rows as JSON.
    pub fn rows(&mut self, sql: &str) -> Vec<Vec<Value>> {
        let (code, text) = self.raw_internal(sql);
        assert_eq!(code, 0, "{sql}: {text}");
        pinkdb_memory::host::rows(&text).expect("the internal channel answers JSON rows")
    }

    /// The single cell of a one-row query, as text.
    pub fn cell(&mut self, sql: &str) -> String {
        let rows = self.rows(sql);
        let row = rows.first().expect("one row");
        pinkdb_memory::host::cell_str(row, 0)
    }

    /// A statement the public channel must refuse, with the reason.
    pub fn refused(&mut self, sql: &str) -> String {
        let (code, text) = self.raw_statement(sql);
        assert_ne!(code, 0, "{sql} should have been refused: {text}");
        text
    }
}

impl MemoryHost for TestHost {
    fn internal(&mut self, sql: &str) -> Result<String, String> {
        let (code, text) = self.raw_internal(sql);
        if code == 0 { Ok(text) } else { Err(text) }
    }

    fn tool(&mut self, sql: &str) -> (i32, String) {
        self.raw_statement(sql)
    }

    fn emit(&mut self, channel: i32, text: &str) {
        if channel == pinkdb_memory::ANSWER {
            self.out.push(text.to_string());
        } else {
            self.diag.push(text.to_string());
        }
    }
}

impl pinkdb_mcp_protocol::Host for TestHost {
    fn execute(&mut self, statement: &str) -> pinkdb_ext_sdk::StatementReply {
        let (code, text) = self.raw_statement(statement);
        pinkdb_ext_sdk::StatementReply { code, text }
    }

    fn execute_internal(&mut self, statement: &str) -> pinkdb_ext_sdk::StatementReply {
        let (code, text) = self.raw_internal(statement);
        pinkdb_ext_sdk::StatementReply { code, text }
    }
}

/// A scripted model: one answer per step, in order. A `Err` is what a
/// provider failure (a 429, a malformed answer, no key) looks like to the
/// caller.
#[derive(Default)]
pub struct Mock {
    answers: VecDeque<Result<String, String>>,
    /// Every prompt the distiller sent, in order, for assertions.
    pub prompts: Vec<String>,
    /// Every system prompt, in order.
    pub systems: Vec<String>,
}

impl Mock {
    pub fn new(answers: Vec<Result<String, String>>) -> Mock {
        Mock {
            answers: answers.into(),
            ..Mock::default()
        }
    }

    /// A provider whose answers are the given JSON documents.
    pub fn json(answers: &[&str]) -> Mock {
        Mock::new(
            answers
                .iter()
                .map(|answer| Ok(answer.to_string()))
                .collect(),
        )
    }

    pub fn then(&mut self, answer: &str) -> &mut Mock {
        self.answers.push_back(Ok(answer.to_string()));
        self
    }

    pub fn fail(&mut self, message: &str) -> &mut Mock {
        self.answers.push_back(Err(message.to_string()));
        self
    }

    pub fn calls(&self) -> usize {
        self.prompts.len()
    }
}

impl Provider for Mock {
    fn provider(&self) -> &str {
        "mock"
    }

    fn model(&self) -> &str {
        "mock-model"
    }

    fn base_url(&self) -> &str {
        "http://127.0.0.1:9/mock"
    }

    fn complete(&mut self, messages: &[Message], _tools: &[Value]) -> Result<Turn, String> {
        let mut prompt = String::new();
        for message in messages {
            match message {
                Message::System(text) => self.systems.push(text.clone()),
                Message::User(text) => {
                    self.prompts.push(text.clone());
                    prompt.push_str(text);
                }
                Message::Assistant { content, .. } | Message::Tool { content, .. } => {
                    self.prompts.push(content.clone());
                    prompt.push_str(content);
                }
            }
        }
        let Some(answer) = self.answers.pop_front() else {
            return Err("the script has no more answers".to_string());
        };
        answer.map(|content| Turn {
            content: fill(&content, &prompt),
            tool_calls: Vec::new(),
            finish_reason: "stop".to_string(),
            usage: Usage::default(),
        })
    }
}

/// Replace `$IDS` in a scripted answer with the atom ids the prompt listed,
/// so a scenario can cite the atoms the previous step wrote.
fn fill(answer: &str, prompt: &str) -> String {
    if !answer.contains("$IDS") {
        return answer.to_string();
    }
    let ids: Vec<String> = prompt
        .lines()
        .filter_map(|line| {
            let id = line.split_whitespace().next()?;
            id.starts_with('a')
                .then(|| (id, id.chars().all(|c| c.is_ascii_alphanumeric())))
                .and_then(|(id, plain)| (plain && id.len() > 8).then(|| format!("\"{id}\"")))
        })
        .collect();
    answer.replace("$IDS", &ids.join(","))
}

/// A loopback OpenAI-compatible embeddings endpoint.
///
/// The vector is a one-hot of "does this text mention the warehouse", so the
/// hybrid ranking is hand-checkable: a query about the warehouse is distance 0
/// from the warehouse atom and √2 from every other.
pub struct Embeddings {
    port: u16,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    requests: Arc<Mutex<usize>>,
}

impl Embeddings {
    pub fn start() -> Embeddings {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind the embeddings mock");
        let port = listener.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(0usize));
        let thread = {
            let stop = Arc::clone(&stop);
            let requests = Arc::clone(&requests);
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let Ok(mut stream) = stream else { continue };
                    let _ = answer(&mut stream, &requests);
                }
            })
        };
        Embeddings {
            port,
            stop,
            thread: Some(thread),
            requests,
        }
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}/embeddings", self.port)
    }

    pub fn count(&self) -> usize {
        *self.requests.lock().unwrap()
    }
}

impl Drop for Embeddings {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake the blocked `accept`.
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn answer(stream: &mut TcpStream, requests: &Arc<Mutex<usize>>) -> std::io::Result<()> {
    let body = read_request(stream)?;
    *requests.lock().unwrap() += 1;
    let request: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let dimensions = request["dimensions"].as_u64().unwrap_or(4).max(1) as usize;
    let inputs = request["input"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|input| input.as_str().unwrap_or_default().to_string())
        .collect::<Vec<_>>();
    let data = inputs
        .iter()
        .enumerate()
        .map(|(index, text)| json!({"index": index, "embedding": vector(text, dimensions)}))
        .collect::<Vec<_>>();
    let reply = json!({"object": "list", "model": request["model"], "data": data}).to_string();
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        reply.len(),
        reply
    )?;
    stream.flush()
}

/// The one-hot the hybrid test rides on.
fn vector(text: &str, dimensions: usize) -> Vec<f64> {
    let mut vector = vec![0.0; dimensions];
    let slot = if text.to_lowercase().contains("warehouse") {
        0
    } else {
        1.min(dimensions - 1)
    };
    vector[slot] = 1.0;
    vector
}

fn read_request(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        let text = String::from_utf8_lossy(&buffer).to_string();
        if let Some(end) = text.find("\r\n\r\n") {
            let head = &text[..end];
            let length = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            if buffer.len() >= end + 4 + length {
                return Ok(String::from_utf8_lossy(&buffer[end + 4..end + 4 + length]).to_string());
            }
        }
    }
    Ok(String::from_utf8_lossy(&buffer).to_string())
}

/// Point the embeddings package at `base_url`. The env is process-wide, so a
/// test that uses it holds [`ENV_LOCK`] for as long as the guard lives.
#[derive(Default)]
pub struct Env {
    saved: Vec<(&'static str, Option<String>)>,
}

impl Env {
    pub fn embeddings(base_url: &str) -> Env {
        let mut env = Env { saved: Vec::new() };
        env.set("PINKDB_EMBEDDINGS_BASE_URL", base_url);
        env.set("PINKDB_EMBEDDINGS_MODEL", "mock-embed");
        env.set("PINKDB_EMBEDDINGS_API_KEY", "sk-memory-test");
        env.set("PINKDB_EMBEDDINGS_DIMENSIONS", "4");
        env
    }

    pub fn set(&mut self, name: &'static str, value: &str) -> &mut Env {
        self.saved.push((name, std::env::var(name).ok()));
        unsafe { std::env::set_var(name, value) };
        self
    }

    pub fn unset(&mut self, name: &'static str) -> &mut Env {
        self.saved.push((name, std::env::var(name).ok()));
        unsafe { std::env::remove_var(name) };
        self
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        for (name, value) in self.saved.drain(..).rev() {
            match value {
                Some(value) => unsafe { std::env::set_var(name, value) },
                None => unsafe { std::env::remove_var(name) },
            }
        }
    }
}

/// The lock every env-mutating test holds.
pub fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|error| error.into_inner())
}
