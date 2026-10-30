//! Wave 105: `pinkdb-server --mcp-extension` bridges the server's own store
//! over a loopback HTTP listener, read-only and under the store's logins.
//!
//! The bridge is an ordinary extension: the test builds `pinkdb-mcp` and
//! points the server at the cdylib, then speaks JSON-RPC over HTTP to it.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

fn server() -> &'static str {
    env!("CARGO_BIN_EXE_pinkdb-server")
}

/// The bridge cdylib, built by `cargo build -p pinkdb-mcp`.
/// The bridge library, resolved (and built if missing) **once per test
/// binary**. Tests run in parallel: when several each ran `cargo build -p
/// pinkdb-mcp`, the concurrent builds re-linked the cdylib under each
/// other, so a path one test had just found vanished before its child could
/// `dlopen` it ("No such file or directory"). The lock makes one thread
/// build while the others wait for its answer.
fn bridge_library() -> PathBuf {
    static BRIDGE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    BRIDGE.get_or_init(resolve_bridge_library).clone()
}

fn resolve_bridge_library() -> PathBuf {
    let name = if cfg!(target_os = "macos") {
        "libpinkdb_mcp.dylib"
    } else if cfg!(target_os = "windows") {
        "pinkdb_mcp.dll"
    } else {
        "libpinkdb_mcp.so"
    };
    let exe = Path::new(server());
    let mut candidates = Vec::new();
    if let Some(dir) = exe.parent() {
        candidates.push(dir.join(name));
        candidates.push(dir.join("deps").join(name));
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
        .args(["build", "-q", "-p", "pinkdb-mcp"])
        .status()
        .is_ok_and(|status| status.success());
    if built {
        for path in &candidates {
            if path.is_file() {
                return path.clone();
            }
        }
    }
    panic!(
        "the MCP bridge is not built: {} (run `cargo build -p pinkdb-mcp`)",
        candidates
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
}

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("pinkdb-server-mcp-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create the scratch directory");
    dir
}

/// Create a durable store and run `statements` against it.
///
/// The server test seeds through the engine (the CLI binary belongs to
/// another crate, so `CARGO_BIN_EXE_pinkdb` is not available here).
fn seed(store: &Path, statements: &[&str]) {
    let path = store.display().to_string();
    let mut db = pinkdb::PinkDb::open_durable(&path).expect("create the store");
    for statement in statements {
        db.execute(statement)
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    db.checkpoint().expect("write the store");
}

/// A spawned server, killed on drop so a failed assertion does not leak it.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(args: &[&str], envs: &[(&str, &str)]) -> Server {
    let mut command = Command::new(server());
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    for (name, value) in envs {
        command.env(name, value);
    }
    Server(command.spawn().expect("spawn pinkdb-server"))
}

/// The port the bridge bound, from its `listening on http://…` line.
///
/// `--mcp-port 0` lets the kernel pick it: a port reserved here could be taken
/// by a parallel test between the check and the bridge's bind, and the client
/// would then reach the wrong server. Lines stream through the channel and are
/// drained after the port arrives, so the server never blocks on a full pipe
/// and a slow start reports what it did print.
fn bridge_port(server: &mut Server) -> u16 {
    let stderr = server.0.stderr.take().expect("the server's stderr");
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        for line in reader.by_ref().lines() {
            let Ok(line) = line else { break };
            if sender.send(line).is_err() {
                break;
            }
        }
        let mut rest = String::new();
        let _ = reader.read_to_string(&mut rest);
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut seen = String::new();
    loop {
        let Some(left) = deadline.checked_duration_since(Instant::now()) else {
            panic!("the server never announced the bridge's port; it printed:\n{seen}");
        };
        match receiver.recv_timeout(left) {
            Ok(line) => {
                if let Some((_, rest)) = line.split_once("listening on http://")
                    && let Some((_, digits)) = rest.rsplit_once(':')
                {
                    // Another thread sharing stderr can interleave into this
                    // line; the port still ends at the first non-digit.
                    let digits: String = digits
                        .trim()
                        .chars()
                        .take_while(char::is_ascii_digit)
                        .collect();
                    if let Ok(port) = digits.parse() {
                        return port;
                    }
                }
                seen.push_str(&line);
                seen.push('\n');
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("the server exited before announcing the bridge's port; it printed:\n{seen}")
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!("the server never announced the bridge's port; it printed:\n{seen}")
            }
        }
    }
}

/// One JSON-RPC message over the bridge's streamable HTTP transport.
fn post(port: u16, message: &Value) -> (u16, Option<Value>) {
    let body = message.to_string();
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to the bridge");
    write!(
        stream,
        "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("write the request");
    stream.flush().expect("flush");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read the reply");
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("a status line");
    let payload = response.split("\r\n\r\n").nth(1).unwrap_or("");
    let value = if payload.trim().is_empty() {
        None
    } else {
        Some(serde_json::from_str(payload.trim()).expect("a JSON-RPC reply"))
    };
    (status, value)
}

fn initialize(port: u16) {
    let (status, reply) = post(
        port,
        &json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                           "clientInfo": {"name": "pinkdb-test", "version": "0"}}}),
    );
    assert_eq!(status, 200);
    let reply = reply.expect("an initialize reply");
    assert_eq!(reply["result"]["serverInfo"]["name"], "pinkdb");
}

fn tool(port: u16, id: u64, name: &str, arguments: Value) -> (bool, String) {
    let (status, reply) = post(
        port,
        &json!({"jsonrpc": "2.0", "id": id, "method": "tools/call",
                "params": {"name": name, "arguments": arguments}}),
    );
    assert_eq!(status, 200, "tools/call answered over HTTP");
    let reply = reply.expect("a tools/call reply");
    let result = &reply["result"];
    let is_error = result["isError"].as_bool().unwrap_or(false);
    let text = result["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    (is_error, text)
}

/// `--mcp-extension` on a store with users must not run unauthenticated.
#[test]
fn a_store_with_users_refuses_an_unauthenticated_bridge() {
    let dir = scratch("users");
    let store = dir.join("pinkdb.pink");
    seed(&store, &["CREATE USER agent PASSWORD 'secret'"]);

    let mut server = spawn(
        &[
            "--data-dir",
            dir.to_str().expect("a UTF-8 path"),
            "--port",
            "0",
            "--mcp-extension",
            bridge_library().to_str().expect("a UTF-8 path"),
            "--mcp-port",
            "0",
        ],
        &[],
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = server.0.try_wait().expect("poll the server") {
            break status;
        }
        if Instant::now() > deadline {
            panic!("the server kept running without an --mcp-user");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let mut stderr = String::new();
    if let Some(mut pipe) = server.0.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    assert!(
        !status.success(),
        "the bridge started without a login: {stderr}"
    );
    assert!(stderr.contains("--mcp-user"), "{stderr}");
}

/// The read-only bridge answers queries from the server's store and refuses
/// every write, including for a logged-in user.
#[test]
fn the_bridge_serves_the_servers_store_read_only() {
    let dir = scratch("readonly");
    let store = dir.join("pinkdb.pink");
    seed(
        &store,
        &[
            "CREATE TABLE sales (id INTEGER, amount INTEGER)",
            "INSERT INTO sales VALUES (1, 10), (2, 20), (3, 30)",
        ],
    );

    let mut server = spawn(
        &[
            "--data-dir",
            dir.to_str().expect("a UTF-8 path"),
            "--port",
            "0",
            "--mcp-extension",
            bridge_library().to_str().expect("a UTF-8 path"),
            "--mcp-port",
            "0",
        ],
        &[],
    );
    let port = bridge_port(&mut server);
    initialize(port);

    let (is_error, text) = tool(
        port,
        2,
        "query",
        json!({"sql": "SELECT sum(amount) FROM sales"}),
    );
    assert!(!is_error, "{text}");
    assert!(text.contains("60"), "{text}");

    let (is_error, text) = tool(port, 3, "tables", json!({}));
    assert!(!is_error, "{text}");
    assert!(text.contains("sales"), "{text}");

    let (is_error, text) = tool(
        port,
        4,
        "query",
        json!({"sql": "INSERT INTO sales VALUES (4, 40)"}),
    );
    assert!(is_error, "{text}");
    assert!(text.contains("permission"), "{text}");
}

/// A logged-in bridge serves the same store; the policy is still read-only.
#[test]
fn a_logged_in_bridge_serves_the_store() {
    let dir = scratch("login");
    let store = dir.join("pinkdb.pink");
    seed(
        &store,
        &[
            "CREATE USER agent PASSWORD 'secret'",
            "CREATE TABLE t (a INTEGER)",
            "INSERT INTO t VALUES (7)",
        ],
    );

    let mut server = spawn(
        &[
            "--data-dir",
            dir.to_str().expect("a UTF-8 path"),
            "--port",
            "0",
            "--mcp-extension",
            bridge_library().to_str().expect("a UTF-8 path"),
            "--mcp-port",
            "0",
            "--mcp-user",
            "agent",
        ],
        &[("PINKDB_MCP_PASSWORD", "secret")],
    );
    let port = bridge_port(&mut server);
    initialize(port);

    let (is_error, text) = tool(port, 2, "query", json!({"sql": "SELECT 1 AS n"}));
    assert!(!is_error, "{text}");
    assert!(text.contains("| n:"), "{text}");

    let (is_error, text) = tool(
        port,
        3,
        "query",
        json!({"sql": "CREATE TABLE scratch (a INT)"}),
    );
    assert!(is_error, "{text}");
    assert!(text.contains("permission"), "{text}");
}
