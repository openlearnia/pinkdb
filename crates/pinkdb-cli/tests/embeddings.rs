//! The CLI's opt-in `embeddings` feature (wave 107c): the package is
//! published at startup and loaded into every store the session opens, so
//! `embed(text)` works without a `LOAD EXTENSION`, and the persistent cache
//! outlives the process.
#![cfg(feature = "embeddings")]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A key that must never reach the store, the WAL or a message.
const SENTINEL: &str = "sk-sentinel-embeddings-cli-7c3d11";
const DIMENSIONS: u32 = 2;

/// A loopback embeddings endpoint that answers each input with a fixed-width
/// vector derived from the text, and counts the requests it served.
struct Mock {
    port: u16,
    dimensions: u32,
    requests: Arc<AtomicUsize>,
}

impl Mock {
    fn start(dimensions: u32) -> Mock {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&requests);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let Some(body) = request_body(&mut stream) else {
                    continue;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                let payload = reply(&body, dimensions);
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{payload}",
                    payload.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        Mock {
            port,
            dimensions,
            requests,
        }
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}/embeddings", self.port)
    }

    fn count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

/// The JSON body of one HTTP/1.1 request, read to its declared length.
fn request_body(stream: &mut std::net::TcpStream) -> Option<String> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte).ok()? == 0 {
            return None;
        }
        head.push(byte[0]);
    }
    let head = String::from_utf8(head).ok()?;
    let length: usize = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())?
        })
        .unwrap_or(0);
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).ok()?;
    String::from_utf8(body).ok()
}

fn reply(body: &str, dimensions: u32) -> String {
    let request: serde_json::Value = serde_json::from_str(body).unwrap();
    let input = request["input"].as_array().unwrap();
    let data: Vec<serde_json::Value> = input
        .iter()
        .enumerate()
        .map(|(index, text)| {
            let text = text.as_str().unwrap();
            let embedding: Vec<f64> = (0..dimensions)
                .map(|position| (text.len() + position as usize) as f64)
                .collect();
            serde_json::json!({"index": index, "embedding": embedding})
        })
        .collect();
    serde_json::json!({"object": "list", "data": data}).to_string()
}

/// Run the CLI with the mock provider configured, returning stdout, stderr
/// and the exit code.
fn run(script: &str, mock: &Mock, cache: &std::path::Path) -> (String, String, Option<i32>) {
    spawn(
        script,
        &mock.base_url(),
        Some(SENTINEL),
        mock.dimensions,
        cache,
    )
}

/// Run the CLI against a dead endpoint and no key: a WAL replay must answer
/// `embed()` from its memo, so this process never needs the provider.
fn run_offline(
    script: &str,
    cache: &std::path::Path,
    dimensions: u32,
) -> (String, String, Option<i32>) {
    spawn(
        script,
        "http://127.0.0.1:9/embeddings",
        None,
        dimensions,
        cache,
    )
}

fn spawn(
    script: &str,
    base_url: &str,
    key: Option<&str>,
    dimensions: u32,
    cache: &std::path::Path,
) -> (String, String, Option<i32>) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pinkdb"));
    command
        .env("PINKDB_EMBEDDINGS_BASE_URL", base_url)
        .env("PINKDB_EMBEDDINGS_MODEL", "test-embedding-model")
        .env("PINKDB_EMBEDDINGS_DIMENSIONS", dimensions.to_string())
        .env("PINKDB_EMBEDDINGS_CACHE_DIR", cache);
    match key {
        Some(key) => command.env("PINKDB_EMBEDDINGS_API_KEY", key),
        None => command.env_remove("PINKDB_EMBEDDINGS_API_KEY"),
    };
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    (
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
        out.status.code(),
    )
}

fn scratch(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "pinkdb-cli-embeddings-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    directory
}

#[test]
fn embed_works_without_a_load_statement_and_the_cache_survives_the_process() {
    let mock = Mock::start(DIMENSIONS);
    let cache = scratch("cache");
    let script = "SELECT embed('alpha'), typeof(embed('alpha'));\n";

    let (out, err, code) = run(script, &mock, &cache);
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("FLOAT[2]"), "{out}");
    assert!(
        !out.contains(SENTINEL) && !err.contains(SENTINEL),
        "{out}{err}"
    );
    assert_eq!(mock.count(), 1, "the first run made no request");

    let (out, err, code) = run(script, &mock, &cache);
    assert_eq!(code, Some(0), "{err}");
    assert!(out.contains("FLOAT[2]"), "{out}");
    assert_eq!(
        mock.count(),
        1,
        "the second process re-requested a cached text"
    );
    let _ = std::fs::remove_dir_all(&cache);
}

#[test]
fn the_key_never_reaches_the_store_or_its_wal() {
    let mock = Mock::start(DIMENSIONS);
    let cache = scratch("cache");
    let directory = scratch("store");
    let path = directory.join("db.pink");
    let script = format!(
        ".open {}\n\
         CREATE TABLE docs (body VARCHAR, v FLOAT[2]);\n\
         INSERT INTO docs VALUES ('a private note', NULL);\n\
         UPDATE docs SET v = embed(body);\n\
         SELECT COUNT(*) FROM docs;\n",
        path.display()
    );

    let (_, err, code) = run(&script, &mock, &cache);
    assert_eq!(code, Some(0), "{err}");
    assert!(!err.contains(SENTINEL), "{err}");
    assert_eq!(mock.count(), 1);

    let mut files = 0;
    for entry in std::fs::read_dir(&directory).unwrap() {
        files += 1;
        let bytes = std::fs::read(entry.unwrap().path()).unwrap();
        assert!(
            !bytes
                .windows(SENTINEL.len())
                .any(|window| window == SENTINEL.as_bytes()),
            "the store holds the API key"
        );
    }
    assert!(files > 0, "the store wrote nothing to scan");
    let _ = std::fs::remove_dir_all(&directory);
    let _ = std::fs::remove_dir_all(&cache);
}

/// The WAL is logical, so a statement that calls `embed()` carries its
/// vectors in the record's memo. A replay therefore needs neither a key nor
/// the provider: a fresh process with a dead endpoint answers the query.
#[test]
fn a_replay_answers_from_the_wal_memo_not_the_provider() {
    let mock = Mock::start(DIMENSIONS);
    let cache = scratch("cache");
    let offline_cache = scratch("offline-cache");
    let directory = scratch("replay");
    let path = directory.join("db.pink");
    let script = format!(
        ".open {}\n\
         CREATE TABLE docs (id INTEGER, body VARCHAR, v FLOAT[2]);\n\
         INSERT INTO docs VALUES (1, 'abcde', NULL), (2, 'abcdefg', NULL);\n\
         UPDATE docs SET v = embed(body);\n",
        path.display()
    );
    let (_, err, code) = run(&script, &mock, &cache);
    assert_eq!(code, Some(0), "{err}");
    assert_eq!(mock.count(), 1);

    let (out, err, code) = run_offline(
        &format!(
            ".open {}\nSELECT id, v[1], v[2] FROM docs ORDER BY id;\n",
            path.display()
        ),
        &offline_cache,
        DIMENSIONS,
    );
    assert_eq!(code, Some(0), "a replay must not need the provider: {err}");
    // The mock maps a text to [len, len + 1].
    assert!(out.contains("5") && out.contains("6"), "{out}");
    assert!(out.contains("7") && out.contains("8"), "{out}");
    assert!(!err.contains("no embeddings API key"), "{err}");
    assert_eq!(mock.count(), 1, "the replay must make no request");
    let _ = std::fs::remove_dir_all(&directory);
    let _ = std::fs::remove_dir_all(&cache);
    let _ = std::fs::remove_dir_all(&offline_cache);
}

/// The memo is the price of logical logging: one 1024-wide vector per
/// distinct input, ~4 KiB each. This measures the WAL delta for 1,000 rows.
#[test]
fn a_thousand_wide_vectors_are_recorded_in_the_wal_memo() {
    const WIDE: u32 = 1024;
    let mock = Mock::start(WIDE);
    let rows: Vec<String> = (0..1000)
        .map(|i| format!("({i}, 'text {i}', NULL)"))
        .collect();
    let insert = format!("INSERT INTO docs VALUES {};\n", rows.join(", "));
    let script = |update: &str, path: &std::path::Path| {
        format!(
            ".open {}\n\
             CREATE TABLE docs (id INTEGER, body VARCHAR, v FLOAT[1024]);\n\
             {insert}{update}",
            path.display()
        )
    };
    let plain_dir = scratch("wide-plain");
    let update_dir = scratch("wide-update");
    let cache = scratch("cache");
    let (_, err, code) = run(&script("", &plain_dir.join("db.pink")), &mock, &cache);
    assert_eq!(code, Some(0), "{err}");
    let (_, err, code) = run(
        &script(
            "UPDATE docs SET v = embed(body);\n",
            &update_dir.join("db.pink"),
        ),
        &mock,
        &cache,
    );
    assert_eq!(code, Some(0), "{err}");
    assert_eq!(mock.count(), 8, "1000 inputs at 128 per request");

    let size = |dir: &std::path::Path| -> u64 {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len())
            .sum()
    };
    let memo = size(&update_dir) as i64 - size(&plain_dir) as i64;
    eprintln!(
        "wal memo for 1000 rows at 1024 dims: {} bytes ({:.1} KiB per row)",
        memo,
        memo as f64 / 1024.0 / 1000.0
    );
    assert!(
        (3_900_000..4_400_000).contains(&memo),
        "expected ~4 KiB per vector, measured {memo} bytes"
    );
    let _ = std::fs::remove_dir_all(&plain_dir);
    let _ = std::fs::remove_dir_all(&update_dir);
    let _ = std::fs::remove_dir_all(&cache);
}
