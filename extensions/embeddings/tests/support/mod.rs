//! Test plumbing for the embeddings extension: a recorded OpenAI-compatible
//! provider bound to `127.0.0.1:0`, a process-wide environment lock, and
//! scratch directories.
//!
//! Nothing here leaves the loopback interface: the provider is a plain TCP
//! listener this process owns, and every test points
//! `PINKDB_EMBEDDINGS_BASE_URL` at its real bound port.

#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// The key every test exports. A provider that ever saw it echoed back, or
/// a store that captured it, is a leak this constant makes greppable.
pub const SENTINEL: &str = "sk-sentinel-embeddings-provider-2f4a91";

/// One request as the mock read it off the wire.
#[derive(Clone)]
pub struct Recorded {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Recorded {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header == name)
            .map(|(_, value)| value.as_str())
    }

    pub fn json(&self) -> Value {
        serde_json::from_str(&self.body).expect("the request body is JSON")
    }

    pub fn inputs(&self) -> Vec<String> {
        self.json()["input"]
            .as_array()
            .expect("`input` is an array")
            .iter()
            .map(|text| text.as_str().unwrap_or_default().to_string())
            .collect()
    }
}

pub enum Reply {
    Json(Value),
    Status(u16, String),
    /// Accept the request and never answer, to exercise the read timeout.
    Hang(Duration),
}

/// A recorded provider: `handler` answers, and every request is kept.
pub struct Mock {
    port: u16,
    requests: Arc<Mutex<Vec<Recorded>>>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Mock {
    pub fn start(handler: impl Fn(&Recorded) -> Reply + Send + Sync + 'static) -> Mock {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a loopback port");
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let requests = requests.clone();
            let stop = stop.clone();
            let handler = Arc::new(handler);
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let Ok(mut stream) = stream else { break };
                    let Some(recorded) = read_request(&mut stream) else {
                        continue;
                    };
                    let reply = handler(&recorded);
                    requests.lock().unwrap().push(recorded);
                    match reply {
                        Reply::Json(value) => write_reply(&mut stream, 200, &value.to_string()),
                        Reply::Status(status, body) => write_reply(&mut stream, status, &body),
                        Reply::Hang(duration) => std::thread::sleep(duration),
                    }
                }
            })
        };
        Mock {
            port,
            requests,
            stop,
            thread: Some(thread),
        }
    }

    /// A provider that answers [`embed_reply`].
    pub fn embeddings() -> Mock {
        Mock::start(embed_reply)
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}/embeddings", self.port)
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }

    pub fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake the blocked `accept`, which then sees the stop flag.
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Answer an embeddings request with one deterministic vector per input,
/// as wide as the request's `dimensions`.
pub fn embed_reply(recorded: &Recorded) -> Reply {
    let body = recorded.json();
    let dimensions = body["dimensions"]
        .as_u64()
        .expect("the request sent `dimensions`") as usize;
    let data: Vec<Value> = recorded
        .inputs()
        .iter()
        .enumerate()
        .map(|(index, text)| json!({"index": index, "embedding": vector(text, dimensions)}))
        .collect();
    Reply::Json(json!({"object": "list", "model": body["model"], "data": data}))
}

/// A stable vector for `text`: SHA-256 blocks mapped to [-1, 1).
pub fn vector(text: &str, dimensions: usize) -> Vec<f64> {
    let mut values = Vec::with_capacity(dimensions);
    let mut block = 0u64;
    while values.len() < dimensions {
        let digest = Sha256::digest(format!("{text}:{block}").as_bytes());
        for chunk in digest.as_chunks::<4>().0 {
            if values.len() == dimensions {
                break;
            }
            let number = u32::from_le_bytes(*chunk);
            values.push(f64::from(number) / f64::from(u32::MAX));
        }
        block += 1;
    }
    values
}

fn read_request(stream: &mut TcpStream) -> Option<Recorded> {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .ok()?;
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte).ok()? == 0 || head.len() > 64 * 1024 {
            return None;
        }
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head).to_string();
    let mut lines = head.lines();
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_string();
    let path = request_line.next()?.to_string();
    let mut headers = Vec::new();
    let mut content_length = 0usize;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':')?;
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_string();
        if name == "content-length" {
            content_length = value.parse().ok()?;
        }
        headers.push((name, value));
    }
    let mut body = vec![0u8; content_length];
    stream.read_exact(&mut body).ok()?;
    Some(Recorded {
        method,
        path,
        headers,
        body: String::from_utf8_lossy(&body).to_string(),
    })
}

fn write_reply(stream: &mut TcpStream, status: u16, body: &str) {
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Status",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\n\
         content-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

/// Serializes every test that reads or writes the process environment: the
/// variables are global, so a test that set its own endpoint while another
/// read it would talk to the wrong provider.
static ENV: Mutex<()> = Mutex::new(());

/// Set environment variables for the test's lifetime; they are removed on
/// drop, so a later test cannot inherit a stale key or endpoint.
pub struct Env {
    _lock: MutexGuard<'static, ()>,
    names: Vec<&'static str>,
}

impl Env {
    pub fn set(values: Vec<(&'static str, String)>) -> Env {
        let lock = ENV.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut names = Vec::new();
        for (name, value) in values {
            // SAFETY: every environment-reading test holds this same lock.
            unsafe { std::env::set_var(name, value) };
            names.push(name);
        }
        Env { _lock: lock, names }
    }

    /// Remove variables the host may have exported, so a test that needs
    /// them absent (an unset key) sees the same environment everywhere.
    pub fn unset(&mut self, names: &[&'static str]) {
        for &name in names {
            // SAFETY: the environment lock is held.
            unsafe { std::env::remove_var(name) };
            self.names.push(name);
        }
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        for name in &self.names {
            // SAFETY: the environment lock is still held.
            unsafe { std::env::remove_var(name) };
        }
    }
}

/// The model name every test configures (there is no default).
pub const MODEL: &str = "test-embedding-model";

/// Point the extension at `base_url` with the sentinel key and a four-wide
/// result (small vectors keep the tests fast).
pub fn engine_env(base_url: &str, cache_dir: Option<&std::path::Path>) -> Env {
    let mut values = vec![
        ("PINKDB_EMBEDDINGS_BASE_URL", base_url.to_string()),
        ("PINKDB_EMBEDDINGS_MODEL", MODEL.to_string()),
        ("PINKDB_EMBEDDINGS_API_KEY", SENTINEL.to_string()),
        ("PINKDB_EMBEDDINGS_DIMENSIONS", "4".to_string()),
    ];
    if let Some(dir) = cache_dir {
        values.push(("PINKDB_EMBEDDINGS_CACHE_DIR", dir.display().to_string()));
    }
    Env::set(values)
}

/// Register the package into this process's registry. The declared width is
/// fixed by the first call, so every test in a binary must use the same
/// `PINKDB_EMBEDDINGS_DIMENSIONS` (`engine_env` sets four).
pub fn register() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        pinkdb_embeddings::ensure_registered().expect("register the embeddings package");
    });
}

/// A loaded store: the package is registered and loaded into a fresh
/// in-memory store.
pub fn store() -> pinkdb::PinkDb {
    register();
    let mut db = pinkdb::PinkDb::new();
    db.load_extension_package(pinkdb_embeddings::EXTENSION_NAME)
        .expect("load the embeddings package");
    db
}

pub fn scratch(tag: &str) -> std::path::PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "pinkdb_embeddings_{tag}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
