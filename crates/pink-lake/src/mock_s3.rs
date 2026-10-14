// A hermetic, dependency-free mock of the S3 REST surface the lake uses.
//
// Test code: compiled only for the crate's own tests, and included by the
// pink-lake and pinkdb-cli integration tests via `#[path]` (so it must stay
// self-contained and std-only — no `crate::` imports, no outer/inner doc
// comments; SHA-256/HMAC-SHA256 are hand-rolled below). `std::net::TcpListener`
// + one thread per connection; no async, no crates. It implements exactly the
// operations the S3 storage backend needs:
//
// - `GET` / `HEAD` / `PUT` / `DELETE` one object (with ETags),
// - `PUT` with `If-None-Match: *` (412 when the key exists) and `If-Match`
//   (412 on a changed ETag; 404 on a vanished key, like AWS),
// - `ListObjectsV2` (`list-type=2`, `prefix`, `delimiter=/`, paging with
//   `continuation-token` / `NextContinuationToken`),
// - **full SigV4 verification**: the canonical request is rebuilt from the
//   received bytes (method, encoded path, canonical query, signed headers,
//   `x-amz-content-sha256`) and the HMAC signing-key chain is recomputed with
//   the mock's test credentials. A fabricated signature, a stale
//   `x-amz-date` (>15 min skew, code `RequestTimeTooSkewed`), a wrong
//   `x-amz-content-sha256` body hash, a missing signature or a presigned-shaped
//   request are all rejected with 403. Every header is recorded for
//   assertions, and `verified_requests()` counts strictly verified requests.
// - fault injection: a global forced status (e.g. 403), endpoints that
//   reject conditional headers (`set_conditional_rejection`, e.g. 501
//   `NotImplemented` or 400 `InvalidRequest`), a one-shot ETag bump on the
//   next `If-Match` PUT, injected statuses for one method/target, a dropped
//   response (request processed, connection closed before the reply),
//   a request gate that parks one request until released (deterministic
//   interleavings), an always-truncated listing, and restricted bucket sets
//   (any other bucket answers 404 `NoSuchBucket`).
//
// Addressing is path-style (`/{bucket}/{key}`) by default; `set_virtual_hosted`
// switches to `bucket.<host>/{key}` with the bucket taken from the `Host`
// header (used by the virtual-hosted end-to-end test through `*.localhost`).
// Percent-encoded paths are decoded back to object keys, so keys holding
// literal `%2F` (encoded table names) work.

// The mock exposes a wider surface than any one test uses.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// One stored object: bytes + version tag (the "ETag", quotes included).
#[derive(Clone)]
struct Object {
    bytes: Vec<u8>,
    etag: String,
}

#[derive(Default)]
struct Store {
    objects: BTreeMap<String, Object>,
    /// Monotonic ETag counter: a write always produces a new tag.
    counter: u64,
}

/// A status/body injected for a matching method+target for `remaining` hits.
struct InjectedError {
    method: String,
    target_contains: String,
    status: u16,
    code: String,
    message: String,
    remaining: usize,
}

/// The request gate: one parked request, released from the test thread.
#[derive(Default)]
struct GateState {
    /// The next matching request to park.
    spec: Option<HoldSpec>,
    /// A parked request has arrived and is waiting.
    reached: bool,
    /// The parked request may proceed.
    released: bool,
}

/// One armed gate: park the next request with this method whose target
/// contains the text and which does not carry `without_header`.
struct HoldSpec {
    method: String,
    target_contains: String,
    without_header: Option<String>,
}

#[derive(Default)]
struct Gate {
    state: Mutex<GateState>,
    cv: Condvar,
}

struct State {
    store: Mutex<Store>,
    requests: Mutex<Vec<String>>,
    authorizations: Mutex<Vec<String>>,
    /// The access key id the mock accepts.
    access_key_id: Mutex<String>,
    /// The secret key the mock verifies signatures with.
    secret_access_key: Mutex<String>,
    /// Forced status for every request (fault injection; `None` = normal).
    deny: Mutex<Option<u16>>,
    /// Rejection `(status, code)` for conditional PUTs.
    conditional_rejection: Mutex<Option<(u16, String)>>,
    /// How many conditional PUTs were rejected.
    conditional_rejections: AtomicUsize,
    /// How many conditional PUTs were attempted (rejected or not).
    conditional_attempts: AtomicUsize,
    /// Bump the target's ETag just before the next `If-Match` PUT.
    bump_etag_next_if_match: AtomicBool,
    /// ListObjectsV2 server-side page size.
    page_size: Mutex<usize>,
    /// Always answer a truncated listing with the same continuation token.
    always_truncated: AtomicBool,
    /// Requests whose SigV4 signature verified.
    verified_requests: AtomicUsize,
    /// Seconds added to the server clock for freshness checks (test knob).
    clock_offset_secs: Mutex<i64>,
    /// Injected statuses for matching requests.
    injected: Mutex<Vec<InjectedError>>,
    /// Drop the response of the next matching `(method, target_contains)`.
    drop_response: Mutex<Option<(String, String)>>,
    /// Corrupt `x-amz-content-sha256` just before verification.
    tamper_body_hash: AtomicBool,
    /// Conditional PUTs still answered with 409 `Conflict`.
    conflict_409s: AtomicUsize,
    /// `Some(set)` restricts the buckets that exist; others 404 NoSuchBucket.
    buckets: Mutex<Option<BTreeSet<String>>>,
    /// Take the bucket from the `Host` header (`bucket.host/key`).
    virtual_hosted: AtomicBool,
    gate: Gate,
    stop: AtomicBool,
}

/// A running mock server. Dropping it stops the listener.
pub struct MockS3 {
    addr: SocketAddr,
    state: Arc<State>,
    join: Option<thread::JoinHandle<()>>,
}

impl MockS3 {
    /// Start a mock on an ephemeral localhost port.
    pub fn start() -> MockS3 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock S3");
        let addr = listener.local_addr().expect("mock S3 address");
        listener.set_nonblocking(true).expect("nonblocking mock S3");
        let state = Arc::new(State {
            store: Mutex::new(Store::default()),
            requests: Mutex::new(Vec::new()),
            authorizations: Mutex::new(Vec::new()),
            access_key_id: Mutex::new("MOCKACCESSKEY".to_string()),
            secret_access_key: Mutex::new("mock-secret".to_string()),
            deny: Mutex::new(None),
            conditional_rejection: Mutex::new(None),
            conditional_rejections: AtomicUsize::new(0),
            conditional_attempts: AtomicUsize::new(0),
            bump_etag_next_if_match: AtomicBool::new(false),
            page_size: Mutex::new(1000),
            always_truncated: AtomicBool::new(false),
            verified_requests: AtomicUsize::new(0),
            clock_offset_secs: Mutex::new(0),
            injected: Mutex::new(Vec::new()),
            drop_response: Mutex::new(None),
            tamper_body_hash: AtomicBool::new(false),
            conflict_409s: AtomicUsize::new(0),
            buckets: Mutex::new(None),
            virtual_hosted: AtomicBool::new(false),
            gate: Gate::default(),
            stop: AtomicBool::new(false),
        });
        let thread_state = state.clone();
        let join = thread::spawn(move || {
            while !thread_state.stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let state = thread_state.clone();
                        thread::spawn(move || handle(stream, state));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        MockS3 {
            addr,
            state,
            join: Some(join),
        }
    }

    /// `http://127.0.0.1:<port>` — the value for `AWS_ENDPOINT_URL`.
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// `http://<host>:<port>` (e.g. `localhost` for virtual-hosted tests).
    pub fn endpoint_with_host(&self, host: &str) -> String {
        format!("http://{host}:{}", self.addr.port())
    }

    /// The lake URL for a bucket+prefix on this mock (`s3://bucket/prefix`).
    pub fn url(&self, bucket: &str, prefix: &str) -> String {
        if prefix.is_empty() {
            format!("s3://{bucket}")
        } else {
            format!("s3://{bucket}/{prefix}")
        }
    }

    /// The access key id the mock accepts (the tests use this in configs).
    pub fn access_key_id(&self) -> String {
        self.state.access_key_id.lock().unwrap().clone()
    }

    /// The secret key the mock accepts (tests sign with this by default).
    pub fn secret_access_key(&self) -> String {
        self.state.secret_access_key.lock().unwrap().clone()
    }

    /// Replace the accepted credentials.
    pub fn set_credentials(&self, access_key_id: &str, secret_access_key: &str) {
        *self.state.access_key_id.lock().unwrap() = access_key_id.to_string();
        *self.state.secret_access_key.lock().unwrap() = secret_access_key.to_string();
    }

    /// Seed an object directly (bypassing HTTP), for `open`-side tests.
    pub fn put_object(&self, key: &str, bytes: &[u8]) {
        let mut store = self.state.store.lock().unwrap();
        let etag = next_etag(&mut store);
        store.objects.insert(
            key.to_string(),
            Object {
                bytes: bytes.to_vec(),
                etag,
            },
        );
    }

    /// Read one stored object back (for assertions).
    pub fn object(&self, key: &str) -> Option<Vec<u8>> {
        self.state
            .store
            .lock()
            .unwrap()
            .objects
            .get(key)
            .map(|o| o.bytes.clone())
    }

    /// All stored keys, sorted.
    pub fn keys(&self) -> Vec<String> {
        self.state
            .store
            .lock()
            .unwrap()
            .objects
            .keys()
            .cloned()
            .collect()
    }

    /// Remove one stored object directly (simulating external deletion).
    pub fn remove_object(&self, key: &str) {
        self.state.store.lock().unwrap().objects.remove(key);
    }

    /// Every request as `METHOD /path[?query]`, in order.
    pub fn requests(&self) -> Vec<String> {
        self.state.requests.lock().unwrap().clone()
    }

    /// Every `Authorization` header seen, in order.
    pub fn authorizations(&self) -> Vec<String> {
        self.state.authorizations.lock().unwrap().clone()
    }

    /// Force one status on every request (403 for auth tests).
    pub fn set_deny(&self, status: u16) {
        *self.state.deny.lock().unwrap() = Some(status);
    }

    pub fn clear_deny(&self) {
        *self.state.deny.lock().unwrap() = None;
    }

    /// Make conditional PUTs fail with 501 `NotImplemented` (an endpoint
    /// that ignores conditional writes).
    pub fn reject_conditional_writes(&self, reject: bool) {
        *self.state.conditional_rejection.lock().unwrap() =
            reject.then(|| (501, "NotImplemented".to_string()));
    }

    /// Make conditional PUTs fail with an arbitrary `(status, code)`.
    pub fn set_conditional_rejection(&self, status: u16, code: &str) {
        *self.state.conditional_rejection.lock().unwrap() = Some((status, code.to_string()));
    }

    pub fn clear_conditional_rejection(&self) {
        *self.state.conditional_rejection.lock().unwrap() = None;
    }

    /// How many conditional PUTs were rejected with the configured status.
    pub fn conditional_rejections(&self) -> usize {
        self.state.conditional_rejections.load(Ordering::Relaxed)
    }

    /// How many conditional PUTs were attempted (rejection or not).
    pub fn conditional_attempts(&self) -> usize {
        self.state.conditional_attempts.load(Ordering::Relaxed)
    }

    /// How many requests carried a fully verified SigV4 signature.
    pub fn verified_requests(&self) -> usize {
        self.state.verified_requests.load(Ordering::Relaxed)
    }

    /// Shift the server clock `secs` forward for `x-amz-date` freshness
    /// checks (a test stand-in for clock skew between client and server).
    pub fn set_clock_offset_secs(&self, secs: i64) {
        *self.state.clock_offset_secs.lock().unwrap() = secs;
    }

    /// Corrupt the next request's `x-amz-content-sha256` right before
    /// verification (must be rejected as a body-hash mismatch).
    pub fn tamper_next_body_hash(&self) {
        self.state.tamper_body_hash.store(true, Ordering::Relaxed);
    }

    /// Bump the ETag of the next `If-Match` PUT target right before checking
    /// the condition: the deterministic stand-in for a concurrent writer.
    pub fn bump_etag_on_next_if_match_put(&self) {
        self.state
            .bump_etag_next_if_match
            .store(true, Ordering::Relaxed);
    }

    /// Answer the next `n` conditional PUTs with 409 `Conflict` (a concurrent
    /// delete raced the write; the client retries, per AWS guidance).
    pub fn conflict_on_next_conditional_puts(&self, n: usize) {
        self.state.conflict_409s.store(n, Ordering::Relaxed);
    }

    /// Inject `status`/`code`/`message` for the next `times` requests whose
    /// method matches and whose target contains `target_contains`.
    pub fn inject_error(
        &self,
        method: &str,
        target_contains: &str,
        status: u16,
        code: &str,
        message: &str,
        times: usize,
    ) {
        self.state.injected.lock().unwrap().push(InjectedError {
            method: method.to_string(),
            target_contains: target_contains.to_string(),
            status,
            code: code.to_string(),
            message: message.to_string(),
            remaining: times,
        });
    }

    /// Process the next matching request but close the connection without
    /// sending a response (a lost response: the write may have landed).
    pub fn drop_next_response(&self, method: &str, target_contains: &str) {
        *self.state.drop_response.lock().unwrap() =
            Some((method.to_string(), target_contains.to_string()));
    }

    /// Park the next matching request before it is handled. Pair with
    /// [`MockS3::wait_until_hold_reached`] and [`MockS3::release_hold`].
    pub fn hold_next_request(&self, method: &str, target_contains: &str) {
        self.arm_hold(method, target_contains, None);
    }

    /// Like [`MockS3::hold_next_request`], but only for requests that do
    /// **not** carry `header` (e.g. the unconditional fallback PUT, not the
    /// conditional probe).
    pub fn hold_next_request_without_header(
        &self,
        method: &str,
        target_contains: &str,
        header: &str,
    ) {
        self.arm_hold(method, target_contains, Some(header.to_string()));
    }

    fn arm_hold(&self, method: &str, target_contains: &str, without_header: Option<String>) {
        let mut gate = self.state.gate.state.lock().unwrap();
        gate.spec = Some(HoldSpec {
            method: method.to_string(),
            target_contains: target_contains.to_string(),
            without_header,
        });
        gate.reached = false;
        gate.released = false;
    }

    /// Wait until a parked request has arrived (bounded by `timeout`).
    pub fn wait_until_hold_reached(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut gate = self.state.gate.state.lock().unwrap();
        while !gate.reached {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, _) = self
                .state
                .gate
                .cv
                .wait_timeout(gate, deadline - now)
                .unwrap();
            gate = guard;
        }
        true
    }

    /// Let the parked request proceed.
    pub fn release_hold(&self) {
        let mut gate = self.state.gate.state.lock().unwrap();
        gate.released = true;
        self.state.gate.cv.notify_all();
    }

    /// The `ListObjectsV2` page size this server returns.
    pub fn set_page_size(&self, page_size: usize) {
        *self.state.page_size.lock().unwrap() = page_size;
    }

    /// Always answer listings as truncated with a repeating continuation
    /// token (a broken/hostile endpoint; the client must give up cleanly).
    pub fn set_always_truncated(&self, on: bool) {
        self.state.always_truncated.store(on, Ordering::Relaxed);
    }

    /// Only the listed buckets exist; any other bucket answers 404
    /// `NoSuchBucket` (including its HEAD responses, which carry no body).
    pub fn restrict_buckets(&self, buckets: &[&str]) {
        *self.state.buckets.lock().unwrap() = Some(buckets.iter().map(|b| b.to_string()).collect());
    }

    /// Take the bucket from the `Host` header (`bucket.host/key`): the
    /// virtual-hosted request shape.
    pub fn set_virtual_hosted(&self, on: bool) {
        self.state.virtual_hosted.store(on, Ordering::Relaxed);
    }

    /// Current ETags (quoted), per key.
    pub fn etags(&self) -> BTreeMap<String, String> {
        self.state
            .store
            .lock()
            .unwrap()
            .objects
            .iter()
            .map(|(k, o)| (k.clone(), o.etag.clone()))
            .collect()
    }

    /// Send a raw HTTP/1.1 request to the mock and return the whole response
    /// (test helper for malformed/unsigned/presigned requests).
    pub fn raw_request(&self, request: &str) -> String {
        let mut stream = TcpStream::connect(self.addr).expect("connect mock S3");
        stream
            .write_all(request.as_bytes())
            .expect("write raw request");
        stream.flush().expect("flush raw request");
        let mut out = String::new();
        let _ = stream.read_to_string(&mut out);
        out
    }
}

impl Drop for MockS3 {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::Relaxed);
        // Wake the accept loop.
        let _ = TcpStream::connect(self.addr);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn next_etag(store: &mut Store) -> String {
    store.counter += 1;
    format!("\"{}\"", store.counter)
}

// ---------------------------------------------------------------------------
// Request handling

fn handle(mut stream: TcpStream, state: Arc<State>) {
    // The listener is non-blocking; on BSD/macOS the accepted stream can
    // inherit that flag, which would turn reads into WouldBlock.
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
    let mut parts = line.trim_end().split(' ');
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
    let body_len = header(&headers, "content-length")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; body_len];
    if body_len > 0 && reader.read_exact(&mut body).is_err() {
        return;
    }

    state
        .requests
        .lock()
        .unwrap()
        .push(format!("{method} {target}"));
    if let Some(auth) = header(&headers, "authorization") {
        state.authorizations.lock().unwrap().push(auth.to_string());
    }

    maybe_hold(&state, &method, &target, &headers);

    let response = respond(&state, &method, &target, &headers, &body);

    if drop_matches(&state, &method, &target) {
        // The request was processed; the response is "lost" on the wire.
        return;
    }

    let mut out = Vec::new();
    write!(
        out,
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        status_text(response.status),
        response.body.len()
    )
    .unwrap();
    if let Some(etag) = &response.etag {
        write!(out, "ETag: {etag}\r\n").unwrap();
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(&response.body);
    let _ = stream.write_all(&out);
    let _ = stream.flush();
    // The socket closes on drop.
}

/// Park the request when it matches an armed gate; the test releases it.
fn maybe_hold(state: &State, method: &str, target: &str, headers: &[(String, String)]) {
    let mut gate = state.gate.state.lock().unwrap();
    let matches = gate.spec.as_ref().is_some_and(|spec| {
        spec.method == method
            && target.contains(spec.target_contains.as_str())
            && spec
                .without_header
                .as_ref()
                .is_none_or(|name| header(headers, name).is_none())
    });
    if !matches {
        return;
    }
    gate.spec = None;
    gate.reached = true;
    state.gate.cv.notify_all();
    while !gate.released {
        gate = state.gate.cv.wait(gate).unwrap();
    }
}

/// Whether this request's response should be dropped (one-shot).
fn drop_matches(state: &State, method: &str, target: &str) -> bool {
    let mut drop = state.drop_response.lock().unwrap();
    let matches = drop
        .as_ref()
        .is_some_and(|(m, t)| m == method && target.contains(t.as_str()));
    if matches {
        *drop = None;
    }
    matches
}

struct Response {
    status: u16,
    etag: Option<String>,
    body: Vec<u8>,
}

impl Response {
    fn new(status: u16, etag: Option<String>, body: Vec<u8>) -> Self {
        Self { status, etag, body }
    }

    fn text(status: u16, body: &str) -> Self {
        Self::new(status, None, body.as_bytes().to_vec())
    }

    fn error(status: u16, code: &str, message: &str) -> Self {
        Self::text(
            status,
            &format!("<?xml version=\"1.0\"?><Error><Code>{code}</Code><Message>{message}</Message></Error>"),
        )
    }
}

fn respond(
    state: &State,
    method: &str,
    target: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Response {
    if let Some(status) = *state.deny.lock().unwrap() {
        let code = if status == 403 {
            "AccessDenied"
        } else {
            "InjectedError"
        };
        return Response::error(status, code, "injected by the mock");
    }

    // A request whose content hash was tampered with never reaches the
    // signature recomputation below with the value it was signed over.
    let mut headers_owned: Vec<(String, String)> = headers.to_vec();
    if state.tamper_body_hash.swap(false, Ordering::Relaxed) {
        let fake = hex(&sha256(b"tampered-by-the-test"));
        if let Some(entry) = headers_owned
            .iter_mut()
            .find(|(name, _)| name == "x-amz-content-sha256")
        {
            entry.1 = fake;
        }
    }
    let headers = headers_owned.as_slice();

    if let Some(error) = check_auth(state, method, target, headers, body) {
        return error;
    }

    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path, query),
        None => (target, ""),
    };
    let decoded = percent_decode(path);
    let (bucket, key) = if state.virtual_hosted.load(Ordering::Relaxed) {
        let host = header(headers, "host").unwrap_or_default();
        let host = host.rsplit_once(':').map(|(h, _)| h).unwrap_or(host);
        let bucket = host.split('.').next().unwrap_or_default().to_string();
        (bucket, decoded.trim_start_matches('/').to_string())
    } else {
        let rest = decoded.strip_prefix('/').unwrap_or(&decoded);
        match rest.split_once('/') {
            Some((bucket, key)) => (bucket.to_string(), key.to_string()),
            None => (rest.to_string(), String::new()),
        }
    };
    if let Some(buckets) = state.buckets.lock().unwrap().as_ref() {
        if !buckets.contains(&bucket) {
            return Response::error(404, "NoSuchBucket", "The specified bucket does not exist.");
        }
    }

    if let Some(injected) = take_injected(state, method, target) {
        return Response::error(injected.0, &injected.1, &injected.2);
    }

    let params = parse_query(query);

    match method {
        "GET" if params.get("list-type").map(String::as_str) == Some("2") => {
            list_objects_v2(state, &params)
        }
        "GET" => match state.store.lock().unwrap().objects.get(&key).cloned() {
            Some(object) => Response::new(200, Some(object.etag), object.bytes),
            None => Response::error(404, "NoSuchKey", "The specified key does not exist."),
        },
        "HEAD" => match state.store.lock().unwrap().objects.get(&key).cloned() {
            Some(object) => Response::new(200, Some(object.etag), Vec::new()),
            None => Response::new(404, None, Vec::new()),
        },
        "PUT" => put_object(state, &key, headers, body),
        "DELETE" => {
            state.store.lock().unwrap().objects.remove(&key);
            Response::new(204, None, Vec::new())
        }
        other => Response::error(
            405,
            "MethodNotAllowed",
            &format!("unsupported method {other}"),
        ),
    }
}

/// Consume one matching injected error, if any.
fn take_injected(state: &State, method: &str, target: &str) -> Option<(u16, String, String)> {
    let mut injected = state.injected.lock().unwrap();
    let index = injected.iter().position(|e| {
        e.remaining > 0 && e.method == method && target.contains(e.target_contains.as_str())
    })?;
    let error = &mut injected[index];
    error.remaining -= 1;
    Some((error.status, error.code.clone(), error.message.clone()))
}

fn put_object(state: &State, key: &str, headers: &[(String, String)], body: &[u8]) -> Response {
    let if_none_match = header(headers, "if-none-match");
    let if_match = header(headers, "if-match");
    let conditional = if_none_match.is_some() || if_match.is_some();
    if conditional {
        state.conditional_attempts.fetch_add(1, Ordering::Relaxed);
    }
    if conditional {
        if let Some((status, code)) = state.conditional_rejection.lock().unwrap().clone() {
            state.conditional_rejections.fetch_add(1, Ordering::Relaxed);
            return Response::error(status, &code, "conditional writes are not supported");
        }
    }
    if conditional && state.conflict_409s.load(Ordering::Relaxed) > 0 {
        state.conflict_409s.fetch_sub(1, Ordering::Relaxed);
        return Response::error(409, "Conflict", "a concurrent delete raced the write");
    }
    let mut store = state.store.lock().unwrap();
    if if_match.is_some() && state.bump_etag_next_if_match.swap(false, Ordering::Relaxed) {
        // Simulate another writer landing between the read and the write.
        let etag = next_etag(&mut store);
        if let Some(object) = store.objects.get_mut(key) {
            object.etag = etag;
        }
    }
    if let Some(expected) = if_none_match {
        if expected == "*" && store.objects.contains_key(key) {
            return Response::error(412, "PreconditionFailed", "object exists");
        }
    }
    if let Some(expected) = if_match {
        match store.objects.get(key) {
            None => return Response::error(404, "NoSuchKey", "The specified key does not exist."),
            Some(object) if object.etag != expected => {
                return Response::error(412, "PreconditionFailed", "etag mismatch")
            }
            Some(_) => {}
        }
    }
    let etag = next_etag(&mut store);
    store.objects.insert(
        key.to_string(),
        Object {
            bytes: body.to_vec(),
            etag: etag.clone(),
        },
    );
    Response::new(200, Some(etag), Vec::new())
}

fn list_objects_v2(state: &State, params: &BTreeMap<String, String>) -> Response {
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let delimiter = params.get("delimiter").cloned().unwrap_or_default();
    let token = params.get("continuation-token").cloned();
    let page_size = *state.page_size.lock().unwrap();

    let store = state.store.lock().unwrap();
    let mut entries: Vec<String> = Vec::new();
    let mut common: Vec<String> = Vec::new();
    for key in store.objects.keys() {
        let Some(rest) = key.strip_prefix(&prefix) else {
            continue;
        };
        if !delimiter.is_empty() {
            if let Some(pos) = rest.find(&delimiter) {
                let dir = format!("{}{}", prefix, &rest[..pos + delimiter.len()]);
                if !common.contains(&dir) {
                    common.push(dir);
                }
                continue;
            }
        }
        entries.push(key.clone());
    }
    let mut all: Vec<String> = entries.into_iter().chain(common).collect();
    all.sort();
    let start = match &token {
        Some(token) => all.iter().position(|e| e > token).unwrap_or(all.len()),
        None => 0,
    };
    let page: Vec<String> = all.iter().skip(start).take(page_size).cloned().collect();
    let truncated =
        state.always_truncated.load(Ordering::Relaxed) || start + page.len() < all.len();
    let next_token = if state.always_truncated.load(Ordering::Relaxed) {
        // A hostile/broken server: the same token forever.
        Some(token.unwrap_or_else(|| "loop".to_string()))
    } else if truncated {
        page.last().cloned()
    } else {
        None
    };

    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult>");
    xml.push_str(&format!(
        "<Name>mock</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount>",
        xml_escape(&prefix),
        page.len()
    ));
    xml.push_str(&format!(
        "<Delimiter>{}</Delimiter><IsTruncated>{}</IsTruncated>",
        xml_escape(&delimiter),
        truncated
    ));
    if let Some(next) = &next_token {
        xml.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            xml_escape(next)
        ));
    }
    for entry in &page {
        if entry.ends_with('/') {
            xml.push_str(&format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                xml_escape(entry)
            ));
        } else if let Some(object) = store.objects.get(entry) {
            xml.push_str(&format!(
                "<Contents><Key>{}</Key><ETag>{}</ETag><Size>{}</Size></Contents>",
                xml_escape(entry),
                xml_escape(&object.etag),
                object.bytes.len()
            ));
        }
    }
    xml.push_str("</ListBucketResult>");
    Response::text(200, &xml)
}

/// Full SigV4 verification of one request: field shapes, the required signed
/// headers, `x-amz-content-sha256 == sha256(body)`, `x-amz-date` freshness,
/// and the recomputed canonical request → string to sign → HMAC chain →
/// signature. Presigned-shaped requests and missing signatures are rejected.
fn check_auth(
    state: &State,
    method: &str,
    target: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Option<Response> {
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path, query),
        None => (target, ""),
    };
    let params = parse_query(query);
    let presigned = params.contains_key("X-Amz-Signature")
        || params.contains_key("X-Amz-Credential")
        || params.contains_key("X-Amz-Algorithm");
    let Some(authorization) = header(headers, "authorization") else {
        return Some(if presigned {
            Response::error(
                403,
                "AccessDenied",
                "presigned URL requests are not supported by this mock",
            )
        } else {
            Response::error(403, "AccessDenied", "missing Authorization header")
        });
    };
    if presigned {
        return Some(Response::error(
            403,
            "AccessDenied",
            "presigned URL requests are not supported by this mock",
        ));
    }
    let Some(rest) = authorization.strip_prefix("AWS4-HMAC-SHA256 ") else {
        return Some(Response::error(
            403,
            "AccessDenied",
            "not an AWS4-HMAC-SHA256 authorization",
        ));
    };
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for field in rest.split(',') {
        let field = field.trim();
        if let Some(value) = field.strip_prefix("Credential=") {
            credential = Some(value.to_string());
        } else if let Some(value) = field.strip_prefix("SignedHeaders=") {
            signed_headers = Some(value.to_string());
        } else if let Some(value) = field.strip_prefix("Signature=") {
            signature = Some(value.to_string());
        }
    }
    let Some(credential) = credential else {
        return Some(Response::error(403, "AccessDenied", "no Credential"));
    };
    let parts: Vec<&str> = credential.split('/').collect();
    let expected_access_key = state.access_key_id.lock().unwrap().clone();
    let valid = parts.len() == 5
        && parts[4] == "aws4_request"
        && parts[3] == "s3"
        && parts[1].len() == 8
        && parts[1].bytes().all(|b| b.is_ascii_digit())
        && parts[0] == expected_access_key;
    if !valid {
        return Some(Response::error(
            403,
            "SignatureDoesNotMatch",
            &format!("the request signature we calculated does not match ({credential})"),
        ));
    }
    let Some(signature) = signature else {
        return Some(Response::error(403, "AccessDenied", "no Signature"));
    };
    if signature.len() != 64 || !signature.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Some(Response::error(
            403,
            "SignatureDoesNotMatch",
            "signature is not 64 hex digits",
        ));
    }
    let signed_headers = signed_headers.unwrap_or_default();
    let names: Vec<&str> = signed_headers.split(';').collect();
    for required in ["host", "x-amz-date", "x-amz-content-sha256"] {
        if !names.contains(&required) {
            return Some(Response::error(
                403,
                "AccessDenied",
                &format!("{required} is not signed"),
            ));
        }
    }
    // The client must actually send every header it signs.
    for name in &names {
        if header(headers, name).is_none() {
            return Some(Response::error(
                403,
                "AccessDenied",
                &format!("signed header {name} was not sent"),
            ));
        }
    }
    // A body-carrying request must announce the payload hash header, and for
    // every request it must match the received bytes.
    let Some(content_sha) = header(headers, "x-amz-content-sha256") else {
        return Some(Response::error(
            403,
            "AccessDenied",
            "missing x-amz-content-sha256",
        ));
    };
    if content_sha != hex(&sha256(body)) {
        return Some(Response::error(
            403,
            "SignatureDoesNotMatch",
            "x-amz-content-sha256 does not match the SHA-256 of the request body",
        ));
    }
    let Some(amz_date) = header(headers, "x-amz-date") else {
        return Some(Response::error(403, "AccessDenied", "missing x-amz-date"));
    };
    let Some(request_epoch) = epoch_from_amz_date(amz_date) else {
        return Some(Response::error(
            403,
            "AccessDenied",
            "x-amz-date is not YYYYMMDDTHHMMSSZ",
        ));
    };
    if amz_date.len() < 8 || parts[1] != &amz_date[..8] {
        return Some(Response::error(
            403,
            "SignatureDoesNotMatch",
            "the credential scope date does not match x-amz-date",
        ));
    }

    // Rebuild the canonical request from the received bytes.
    let canonical_uri = if path.is_empty() { "/" } else { path };
    let canonical_query = canonical_query_from_wire(query);
    let mut canonical_headers = String::new();
    for name in &names {
        let value = header(headers, name).unwrap_or_default();
        canonical_headers.push_str(&format!("{}:{}\n", name, collapse_spaces(value)));
    }
    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{content_sha}"
    );
    let scope = format!("{}/{}", parts[1], parts[2]);
    let scope = format!("{scope}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(&sha256(canonical_request.as_bytes()))
    );
    let secret = state.secret_access_key.lock().unwrap().clone();
    let key = signing_key(&secret, parts[1], parts[2]);
    let expected = hex(&hmac_sha256(&key, string_to_sign.as_bytes()));
    if !expected.eq_ignore_ascii_case(&signature) {
        return Some(Response::error(
            403,
            "SignatureDoesNotMatch",
            "the request signature we calculated does not match the signature you provided",
        ));
    }
    // The signature is authentic; now the replay window.
    let offset = *state.clock_offset_secs.lock().unwrap();
    let server_now = now_epoch() + offset;
    if (request_epoch - server_now).abs() > MAX_SKEW_SECS {
        return Some(Response::error(
            403,
            "RequestTimeTooSkewed",
            "the difference between the request time and the current time is too large",
        ));
    }
    state.verified_requests.fetch_add(1, Ordering::Relaxed);
    None
}

/// The AWS4 signing key: `HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region),
/// service), "aws4_request")`.
fn signing_key(secret: &str, date_stamp: &str, region: &str) -> [u8; 32] {
    let mut key = hmac_sha256(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    key = hmac_sha256(&key, region.as_bytes());
    key = hmac_sha256(&key, b"s3");
    hmac_sha256(&key, b"aws4_request")
}

/// The maximum accepted `x-amz-date` skew (AWS's documented 15 minutes).
const MAX_SKEW_SECS: i64 = 900;

fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// `YYYYMMDDTHHMMSSZ` → Unix seconds (days-from-civil arithmetic).
fn epoch_from_amz_date(date: &str) -> Option<i64> {
    let bytes = date.as_bytes();
    if bytes.len() != 16 || bytes[8] != b'T' || bytes[15] != b'Z' {
        return None;
    }
    let digits = |range: std::ops::Range<usize>| -> Option<i64> {
        let text = &date[range];
        if text.bytes().all(|b| b.is_ascii_digit()) {
            text.parse().ok()
        } else {
            None
        }
    };
    let year = digits(0..4)?;
    let month = digits(4..6)?;
    let day = digits(6..8)?;
    let hour = digits(9..11)?;
    let minute = digits(11..13)?;
    let second = digits(13..15)?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let y = year - i64::from(month <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + hour * 3600 + minute * 60 + second)
}

/// Canonicalize the raw wire query: percent-decode, re-encode, sort by key.
fn canonical_query_from_wire(query: &str) -> String {
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((key, value)) => (key.to_string(), value.to_string()),
            None => (pair.to_string(), String::new()),
        })
        .map(|(key, value)| {
            (
                uri_encode(&percent_decode(&key), true),
                uri_encode(&percent_decode(&value), true),
            )
        })
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// RFC 3986 `uri-encode` with S3's rules (matching the client).
fn uri_encode(input: &str, encode_slash: bool) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        let unreserved =
            b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~';
        if unreserved || (b == b'/' && !encode_slash) {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0xF) as usize] as char);
        }
    }
    out
}

/// AWS header canonicalization: trim, collapse internal space runs.
fn collapse_spaces(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut in_space = false;
    for ch in value.trim().chars() {
        if ch == ' ' || ch == '\t' {
            in_space = true;
        } else {
            if in_space && !out.is_empty() {
                out.push(' ');
            }
            in_space = false;
            out.push(ch);
        }
    }
    out
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.as_str())
}

fn parse_query(query: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((key, value)) => (key, value),
            None => (pair, ""),
        };
        out.insert(percent_decode(key), percent_decode(value));
    }
    out
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn xml_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            other => out.push(other),
        }
    }
    out
}

fn status_text(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        301 => "Moved Permanently",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        412 => "Precondition Failed",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

// ---------------------------------------------------------------------------
// SHA-256 / HMAC-SHA256 (hand-rolled, std-only: the mock has no crate deps).

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

const SHA256_H0: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

/// SHA-256 of one buffer (FIPS 180-4).
fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = SHA256_H0;
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut msg = Vec::with_capacity(data.len() + 72);
    msg.extend_from_slice(data);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[4 * i],
                chunk[4 * i + 1],
                chunk[4 * i + 2],
                chunk[4 * i + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(SHA256_K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (i, value) in [a, b, c, d, e, f, g, hh].iter().enumerate() {
            h[i] = h[i].wrapping_add(*value);
        }
    }

    let mut out = [0u8; 32];
    for i in 0..8 {
        out[4 * i..4 * i + 4].copy_from_slice(&h[i].to_be_bytes());
    }
    out
}

/// HMAC-SHA256 (RFC 2104; keys longer than the block are hashed first).
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&sha256(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= block[i];
        opad[i] ^= block[i];
    }
    let mut inner = Vec::with_capacity(64 + data.len());
    inner.extend_from_slice(&ipad);
    inner.extend_from_slice(data);
    let inner_hash = sha256(&inner);
    let mut outer = Vec::with_capacity(96);
    outer.extend_from_slice(&opad);
    outer.extend_from_slice(&inner_hash);
    sha256(&outer)
}

/// Lowercase hex.
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0xF) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_matches_published_vectors() {
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex(&sha256(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        // A 56-byte input crosses the first length-padding boundary.
        let long = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert_eq!(
            hex(&sha256(long)),
            "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a"
        );
    }

    #[test]
    fn hmac_sha256_matches_rfc4231_vectors() {
        // Test case 1.
        assert_eq!(
            hex(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        // Test case 2.
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // A key longer than the block is hashed first.
        let long_key = [0xaa; 131];
        assert_eq!(
            hex(&hmac_sha256(
                &long_key,
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn amz_dates_parse_and_scope_checks_work() {
        // 2015-08-30T12:36:00Z
        assert_eq!(
            epoch_from_amz_date("20150830T123600Z").unwrap(),
            1_440_938_160
        );
        assert_eq!(epoch_from_amz_date("19700101T000000Z").unwrap(), 0);
        assert!(epoch_from_amz_date("20240101T000000").is_none());
        assert!(epoch_from_amz_date("2024-01-01T00:00:00Z").is_none());
    }
}
