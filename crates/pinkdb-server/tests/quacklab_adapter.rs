//! Wave 138a: `pinkdb-server --quacklab-bind` serves QuackLab's `/sql`
//! adapter — the same HTTP surface the customer's Go service speaks — over the
//! server's own store, on its own listener, and stops with the server.
//!
//! The adapter's announcement line precedes the server's own listening line
//! (it starts before the bind), so a server started without an address is
//! known to have no adapter as soon as its listening line arrives: no timeout
//! of absence is needed.

use std::io::Read;
#[cfg(feature = "quacklab")]
use std::io::{BufRead, BufReader, Write};
#[cfg(feature = "quacklab")]
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
#[cfg(feature = "quacklab")]
use std::time::{Duration, Instant};

fn server() -> &'static str {
    env!("CARGO_BIN_EXE_pinkdb-server")
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
        .env_remove("DUCKDB_BIND")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (name, value) in envs {
        command.env(name, value);
    }
    Server(command.spawn().expect("spawn pinkdb-server"))
}

/// The adapter's address from its announcement, or `None` when the server
/// reached its own listening line without one. Panics if the server exits
/// first, quoting what it printed.
#[cfg(feature = "quacklab")]
fn adapter_addr(server: &mut Server) -> Option<String> {
    let stdout = server.0.stdout.take().expect("the server's stdout");
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
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
            panic!("the server never started; it printed:\n{seen}");
        };
        match receiver.recv_timeout(left) {
            Ok(line) => {
                if let Some((_, rest)) = line.split_once("QuackLab adapter on http://") {
                    let address = rest
                        .split_whitespace()
                        .next()
                        .expect("an address after `http://`");
                    return Some(address.to_string());
                }
                if line.contains(" listening on ") {
                    return None;
                }
                seen.push_str(&line);
                seen.push('\n');
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("the server exited before it started; it printed:\n{seen}")
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!("the server never started; it printed:\n{seen}")
            }
        }
    }
}

#[cfg(feature = "quacklab")]
fn request(addr: &str, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect to the adapter");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("a read timeout");
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
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
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read the reply");
    let (head, body) = response.split_once("\r\n\r\n").expect("a whole response");
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("a status code");
    (status, body.to_string())
}

#[cfg(feature = "quacklab")]
#[test]
fn the_flag_starts_the_adapter() {
    let mut server = spawn(
        &[
            "--in-memory",
            "--port",
            "0",
            "--quacklab-bind",
            "127.0.0.1:0",
        ],
        &[],
    );
    let addr = adapter_addr(&mut server).expect("the adapter started");
    let (status, body) = request(
        &addr,
        "POST",
        "/sql",
        Some("{\"sql\":\"SELECT 42 AS answer\"}"),
    );
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"rows\":[[42]]"), "{body}");
    let (status, body) = request(&addr, "GET", "/health", None);
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, "{\"ok\":true}");
}

#[cfg(feature = "quacklab")]
#[test]
fn duckdb_bind_starts_it_without_the_flag() {
    let mut server = spawn(
        &["--in-memory", "--port", "0"],
        &[("DUCKDB_BIND", "127.0.0.1:0")],
    );
    let addr = adapter_addr(&mut server).expect("DUCKDB_BIND started the adapter");
    let (status, body) = request(&addr, "POST", "/sql", Some("{\"sql\":\"SELECT 1\"}"));
    assert_eq!(status, 200, "{body}");
    assert_eq!(body.matches("\"rowCount\":1").count(), 1, "{body}");
}

#[cfg(feature = "quacklab")]
#[test]
fn the_adapter_is_off_unless_asked_for() {
    let mut server = spawn(&["--in-memory", "--port", "0"], &[]);
    assert_eq!(adapter_addr(&mut server), None);
}

#[cfg(feature = "quacklab")]
#[test]
fn an_empty_bind_is_refused() {
    let mut server = spawn(&["--in-memory", "--port", "0", "--quacklab-bind", ""], &[]);
    let status = server.0.wait().expect("the server exits on a bad flag");
    assert_eq!(status.code(), Some(2));
    let mut message = String::new();
    server
        .0
        .stderr
        .take()
        .expect("the server's stderr")
        .read_to_string(&mut message)
        .expect("read stderr");
    assert!(
        message.contains("--quacklab-bind needs an address"),
        "{message}"
    );
}

#[cfg(not(feature = "quacklab"))]
#[test]
fn the_flag_names_the_missing_feature() {
    let mut server = spawn(
        &[
            "--in-memory",
            "--port",
            "0",
            "--quacklab-bind",
            "127.0.0.1:0",
        ],
        &[],
    );
    let status = server.0.wait().expect("the server exits");
    assert_eq!(status.code(), Some(2));
    let mut message = String::new();
    server
        .0
        .stderr
        .take()
        .expect("the server's stderr")
        .read_to_string(&mut message)
        .expect("read stderr");
    assert!(
        message.contains("needs a build with the `quacklab` feature"),
        "{message}"
    );
}
