// A hermetic, dependency-free HTTP server for the transport tests.
//
// Test code only (included by `lib.rs` under `cfg(test)` and available to
// integration tests via `#[path]`), so it must stay self-contained and
// std-only. `std::net::TcpListener` + one thread per connection; no async, no
// crates. It serves registered paths, honors `HEAD` and `Range:
// bytes=start-end` (206 + `Content-Range`) when enabled, records every
// request line, and can inject statuses (one-shot 503s for the retry tests,
// permanent 403/500s).
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Clone)]
struct File {
    body: Vec<u8>,
    status: u16,
}

struct State {
    files: Mutex<BTreeMap<String, File>>,
    requests: Mutex<Vec<String>>,
    /// The `Range` header of every request (`""` when absent), parallel to
    /// `requests`.
    ranges: Mutex<Vec<String>>,
    range_enabled: AtomicBool,
    /// Answer HEAD with 405 (the "size unknown" backend).
    head_unsupported: AtomicBool,
    /// `(path, remaining)` one-shot failures.
    failures: Mutex<Vec<(String, usize, u16)>>,
    /// `(path, request number)` one-shot failures: fail that request only, so
    /// a read-ahead test can break a chunk in the middle of a file.
    fail_at: Mutex<Vec<(String, usize, u16)>>,
    /// Requests served per path, for [`TestHttpServer::fail_at`].
    served: Mutex<BTreeMap<String, usize>>,
    /// Requests currently being answered, and the high-water mark (the
    /// read-ahead concurrency tests).
    active: AtomicUsize,
    max_active: AtomicUsize,
    /// An artificial response delay, so a concurrent burst is observable.
    delay: Mutex<Option<Duration>>,
    /// Truncate ranged response bodies by this many bytes (short-read tests).
    short_read: AtomicUsize,
    stop: AtomicBool,
}

/// A running test server. Dropping it stops the listener.
pub struct TestHttpServer {
    addr: SocketAddr,
    state: Arc<State>,
    join: Option<thread::JoinHandle<()>>,
}

impl TestHttpServer {
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test HTTP server");
        let addr = listener.local_addr().expect("test HTTP server address");
        listener
            .set_nonblocking(true)
            .expect("nonblocking test HTTP server");
        let state = Arc::new(State {
            files: Mutex::new(BTreeMap::new()),
            requests: Mutex::new(Vec::new()),
            ranges: Mutex::new(Vec::new()),
            range_enabled: AtomicBool::new(true),
            head_unsupported: AtomicBool::new(false),
            failures: Mutex::new(Vec::new()),
            fail_at: Mutex::new(Vec::new()),
            served: Mutex::new(BTreeMap::new()),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            delay: Mutex::new(None),
            short_read: AtomicUsize::new(0),
            stop: AtomicBool::new(false),
        });
        let thread_state = Arc::clone(&state);
        let join = thread::spawn(move || {
            while !thread_state.stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let state = Arc::clone(&thread_state);
                        thread::spawn(move || handle(stream, state));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(std::time::Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            addr,
            state,
            join: Some(join),
        }
    }

    /// The absolute base URL (`http://127.0.0.1:PORT`).
    pub fn base(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{}", self.base(), path)
    }

    /// Serve `body` at `path` with status 200.
    pub fn put(&self, path: &str, body: &[u8]) {
        self.state.files.lock().unwrap().insert(
            path.to_string(),
            File {
                body: body.to_vec(),
                status: 200,
            },
        );
    }

    /// Serve `body` at `path` with an explicit status.
    pub fn put_status(&self, path: &str, status: u16, body: &[u8]) {
        self.state.files.lock().unwrap().insert(
            path.to_string(),
            File {
                body: body.to_vec(),
                status,
            },
        );
    }

    /// Answer request number `request` (1-based) for `path` with `status`,
    /// once, and every other request normally.
    pub fn fail_at(&self, path: &str, request: usize, status: u16) {
        self.state
            .fail_at
            .lock()
            .unwrap()
            .push((path.to_string(), request, status));
    }

    /// Answer the next `remaining` requests for `path` with `status`, then
    /// serve the registered file normally (the retry test).
    pub fn fail_next(&self, path: &str, remaining: usize, status: u16) {
        self.state
            .failures
            .lock()
            .unwrap()
            .push((path.to_string(), remaining, status));
    }

    pub fn set_range_enabled(&self, on: bool) {
        self.state.range_enabled.store(on, Ordering::Relaxed);
    }

    /// Answer every HEAD with 405 (the transport's size-unknown path).
    pub fn set_head_unsupported(&self, on: bool) {
        self.state.head_unsupported.store(on, Ordering::Relaxed);
    }

    /// Delay every response by `delay` (makes concurrent read-ahead visible).
    pub fn set_delay(&self, delay: Option<Duration>) {
        *self.state.delay.lock().unwrap() = delay;
    }

    /// The high-water mark of simultaneously served requests.
    pub fn max_active(&self) -> usize {
        self.state.max_active.load(Ordering::Relaxed)
    }

    /// The number of requests being served right now.
    pub fn active(&self) -> usize {
        self.state.active.load(Ordering::Relaxed)
    }

    /// Drop `bytes` off every ranged (`206`) response body, simulating a
    /// short read.
    pub fn set_short_read(&self, bytes: usize) {
        self.state.short_read.store(bytes, Ordering::Relaxed);
    }

    /// The `Range` header of every request (`""` when absent), in request
    /// order.
    pub fn ranges(&self) -> Vec<String> {
        self.state.ranges.lock().unwrap().clone()
    }

    /// Every request line received, in order (`"GET /path HTTP/1.1"`).
    pub fn requests(&self) -> Vec<String> {
        self.state.requests.lock().unwrap().clone()
    }
}

impl Drop for TestHttpServer {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::Relaxed);
        // Nudge the non-blocking accept loop awake.
        let _ = TcpStream::connect(self.addr);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn handle(mut stream: TcpStream, state: Arc<State>) {
    let _active = ActiveGuard::new(&state);
    let _ = stream.set_nonblocking(false);
    let reader_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut reader = BufReader::new(reader_stream);
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let request_line = line.trim_end().to_string();
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    let mut headers: Vec<(String, String)> = Vec::new();
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).unwrap_or(0) == 0 {
            break;
        }
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
        }
    }
    let body_len = headers
        .iter()
        .find(|(n, _)| n == "content-length")
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; body_len];
    if body_len > 0 && reader.read_exact(&mut body).is_err() {
        return;
    }
    state.requests.lock().unwrap().push(request_line);
    let range_header = headers
        .iter()
        .find(|(n, _)| n == "range")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    state.ranges.lock().unwrap().push(range_header);

    let path = target
        .split_once('?')
        .map(|(p, _)| p)
        .unwrap_or(&target)
        .to_string();

    // One-shot injected failures first.
    let ordinal = {
        let mut served = state.served.lock().unwrap();
        let n = served.entry(path.clone()).or_insert(0);
        *n += 1;
        *n
    };
    let injected_at = {
        let mut fail_at = state.fail_at.lock().unwrap();
        let position = fail_at
            .iter()
            .position(|(p, request, _)| *p == path && *request == ordinal);
        position.map(|index| fail_at.remove(index).2)
    };
    let injected = if injected_at.is_some() {
        injected_at
    } else {
        let mut failures = state.failures.lock().unwrap();
        if let Some(entry) = failures
            .iter_mut()
            .find(|(p, remaining, _)| *p == path && *remaining > 0)
        {
            entry.1 -= 1;
            Some(entry.2)
        } else {
            None
        }
    };

    let file = state.files.lock().unwrap().get(&path).cloned();
    let (status, bytes) = if method == "HEAD" && state.head_unsupported.load(Ordering::Relaxed) {
        (405, Vec::new())
    } else {
        match injected {
            Some(status) => (status, b"injected".to_vec()),
            None => match file {
                Some(file) => (file.status, file.body),
                None => (404, b"not found".to_vec()),
            },
        }
    };

    let range = headers
        .iter()
        .find(|(n, _)| n == "range")
        .map(|(_, v)| v.clone());
    let ranged = if status == 200 && state.range_enabled.load(Ordering::Relaxed) {
        range.as_deref().and_then(|r| parse_range(r, bytes.len()))
    } else {
        None
    };
    let (status, bytes, content_range) = match ranged {
        Some(Ok((start, end))) => (
            206,
            bytes[start..end].to_vec(),
            Some(format!(
                "bytes {start}-{}/{len}",
                end - 1,
                len = bytes.len()
            )),
        ),
        Some(Err(())) => (416, Vec::new(), Some(format!("bytes */{}", bytes.len()))),
        None => (status, bytes, None),
    };
    // Short-read simulation: a 206 with fewer bytes than its Content-Range
    // claims. The parquet reader must surface that, never recover silently.
    let bytes = if status == 206 {
        let drop = state.short_read.load(Ordering::Relaxed);
        if drop > 0 && bytes.len() > drop {
            bytes[..bytes.len() - drop].to_vec()
        } else {
            bytes
        }
    } else {
        bytes
    };

    let mut out = Vec::new();
    write!(
        out,
        "HTTP/1.1 {status} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        status_text(status),
        bytes.len()
    )
    .unwrap();
    if let Some(content_range) = content_range {
        write!(out, "Content-Range: {content_range}\r\n").unwrap();
    }
    out.extend_from_slice(b"\r\n");
    if method != "HEAD" {
        out.extend_from_slice(&bytes);
    }
    if let Some(delay) = *state.delay.lock().unwrap() {
        thread::sleep(delay);
    }
    let _ = stream.write_all(&out);
    let _ = stream.flush();
}

/// Tracks one in-flight request and its high-water mark.
struct ActiveGuard<'a> {
    active: &'a AtomicUsize,
}

impl<'a> ActiveGuard<'a> {
    fn new(state: &'a State) -> Self {
        let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
        state.max_active.fetch_max(active, Ordering::SeqCst);
        Self {
            active: &state.active,
        }
    }
}

impl Drop for ActiveGuard<'_> {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

fn parse_range(value: &str, len: usize) -> Option<std::result::Result<(usize, usize), ()>> {
    let spec = value.trim().strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
    let Ok(start) = start.parse::<usize>() else {
        return Some(Err(()));
    };
    if start >= len {
        return Some(Err(()));
    }
    let end = if end.is_empty() {
        len
    } else {
        match end.parse::<usize>() {
            Ok(end) => end.saturating_add(1).min(len),
            Err(_) => return Some(Err(())),
        }
    };
    if end <= start {
        return Some(Err(()));
    }
    Some(Ok((start, end)))
}

fn status_text(status: u16) -> &'static str {
    match status {
        200 => "OK",
        206 => "Partial Content",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        416 => "Range Not Satisfiable",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "Status",
    }
}
