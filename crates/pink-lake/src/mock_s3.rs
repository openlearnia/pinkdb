// A hermetic, dependency-free mock of the S3 REST surface the lake uses.
//
// Test code: compiled only for the crate's own tests, and included by the
// pink-lake and pinkdb-cli integration tests via `#[path]` (so it must stay
// self-contained and std-only — no `crate::` imports, no outer/inner doc
// comments). `std::net::TcpListener` + one thread per connection; no async,
// no crates. It implements exactly the operations the S3 storage backend
// needs:
//
// - `GET` / `HEAD` / `PUT` / `DELETE` one object (with ETags),
// - `PUT` with `If-None-Match: *` (412 when the key exists) and `If-Match`
//   (412 on a changed ETag; 404 on a vanished key, like AWS),
// - `ListObjectsV2` (`list-type=2`, `prefix`, `delimiter=/`, paging with
//   `continuation-token` / `NextContinuationToken`),
// - a structural check of the `Authorization: AWS4-HMAC-SHA256 …` header
//   (exact signatures are unit-tested against the AWS vectors; the mock
//   verifies the credential id, service, date shape, 64-hex signature, and
//   the required signed headers, and records every header for assertions),
// - fault injection: a global forced status (e.g. 403), endpoints that
//   reject conditional headers with 501 (`reject_conditional_writes`), and
//   a one-shot ETag bump on the next `If-Match` PUT (simulating another
//   writer landing between a read and a conditional write).
//
// Addressing is path-style (`/{bucket}/{key}`); the virtual-hosted URL shape
// is covered by unit tests instead (a `bucket.127.0.0.1` host does not
// resolve). Percent-encoded paths are decoded back to object keys, so keys
// holding literal `%2F` (encoded table names) work.

// The mock exposes a wider surface than any one test uses.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

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

struct State {
    store: Mutex<Store>,
    requests: Mutex<Vec<String>>,
    authorizations: Mutex<Vec<String>>,
    /// The access key id the mock accepts.
    access_key_id: Mutex<String>,
    /// Forced status for every request (fault injection; `None` = normal).
    deny: Mutex<Option<u16>>,
    /// Reject conditional PUTs with 501 `NotImplemented`.
    reject_conditional: AtomicBool,
    /// How many conditional PUTs were rejected with 501.
    conditional_rejections: AtomicUsize,
    /// Bump the target's ETag just before the next `If-Match` PUT.
    bump_etag_next_if_match: AtomicBool,
    /// ListObjectsV2 server-side page size.
    page_size: Mutex<usize>,
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
        listener
            .set_nonblocking(true)
            .expect("nonblocking mock S3");
        let state = Arc::new(State {
            store: Mutex::new(Store::default()),
            requests: Mutex::new(Vec::new()),
            authorizations: Mutex::new(Vec::new()),
            access_key_id: Mutex::new("MOCKACCESSKEY".to_string()),
            deny: Mutex::new(None),
            reject_conditional: AtomicBool::new(false),
            conditional_rejections: AtomicUsize::new(0),
            bump_etag_next_if_match: AtomicBool::new(false),
            page_size: Mutex::new(1000),
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

    /// Seed an object directly (bypassing HTTP), for `open`-side tests.
    pub fn put_object(&self, key: &str, bytes: &[u8]) {
        let mut store = self.state.store.lock().unwrap();
        let etag = next_etag(&mut store);
        store
            .objects
            .insert(key.to_string(), Object { bytes: bytes.to_vec(), etag });
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
        self.state.reject_conditional.store(reject, Ordering::Relaxed);
    }

    /// How many conditional PUTs were rejected with 501 so far.
    pub fn conditional_rejections(&self) -> usize {
        self.state.conditional_rejections.load(Ordering::Relaxed)
    }

    /// Bump the ETag of the next `If-Match` PUT target right before checking
    /// the condition: the deterministic stand-in for a concurrent writer.
    pub fn bump_etag_on_next_if_match_put(&self) {
        self.state
            .bump_etag_next_if_match
            .store(true, Ordering::Relaxed);
    }

    /// The `ListObjectsV2` page size this server returns.
    pub fn set_page_size(&self, page_size: usize) {
        *self.state.page_size.lock().unwrap() = page_size;
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

    let response = respond(&state, &method, &target, &headers, &body);
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

struct Response {
    status: u16,
    etag: Option<String>,
    body: Vec<u8>,
}

impl Response {
    fn new(status: u16, etag: Option<String>, body: Vec<u8>) -> Self {
        Self {
            status,
            etag,
            body,
        }
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
        let code = if status == 403 { "AccessDenied" } else { "InjectedError" };
        return Response::error(status, code, "injected by the mock");
    }
    if let Some(error) = check_auth(state, method, headers) {
        return error;
    }

    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path, query),
        None => (target, ""),
    };
    let decoded = percent_decode(path);
    let rest = decoded.strip_prefix('/').unwrap_or(&decoded);
    let (bucket, key) = match rest.split_once('/') {
        Some((bucket, key)) => (bucket, key),
        None => (rest, ""),
    };
    let _ = bucket;
    let params = parse_query(query);

    match method {
        "GET" if params.get("list-type").map(String::as_str) == Some("2") => {
            list_objects_v2(state, &params)
        }
        "GET" => match state.store.lock().unwrap().objects.get(key).cloned() {
            Some(object) => Response::new(200, Some(object.etag), object.bytes),
            None => Response::error(404, "NoSuchKey", "The specified key does not exist."),
        },
        "HEAD" => match state.store.lock().unwrap().objects.get(key).cloned() {
            Some(object) => Response::new(200, Some(object.etag), Vec::new()),
            None => Response::new(404, None, Vec::new()),
        },
        "PUT" => put_object(state, key, headers, body),
        "DELETE" => {
            state.store.lock().unwrap().objects.remove(key);
            Response::new(204, None, Vec::new())
        }
        other => Response::error(405, "MethodNotAllowed", &format!("unsupported method {other}")),
    }
}

fn put_object(
    state: &State,
    key: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Response {
    let if_none_match = header(headers, "if-none-match");
    let if_match = header(headers, "if-match");
    let conditional = if_none_match.is_some() || if_match.is_some();
    if conditional && state.reject_conditional.load(Ordering::Relaxed) {
        state.conditional_rejections.fetch_add(1, Ordering::Relaxed);
        return Response::error(501, "NotImplemented", "conditional writes are not supported");
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
            None => {
                return Response::error(404, "NoSuchKey", "The specified key does not exist.")
            }
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
    let truncated = start + page.len() < all.len();
    let next_token = if truncated { page.last().cloned() } else { None };

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

/// A structural check of the SigV4 header (exact signatures are unit-tested
/// against the AWS vectors; this proves the client signs every request with
/// the configured credential, service and headers — including the
/// conditional preconditions).
fn check_auth(state: &State, method: &str, headers: &[(String, String)]) -> Option<Response> {
    let Some(authorization) = header(headers, "authorization") else {
        return Some(Response::error(
            403,
            "AccessDenied",
            "missing Authorization header",
        ));
    };
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
    let valid = parts.len() == 5
        && parts[4] == "aws4_request"
        && parts[3] == "s3"
        && parts[1].len() == 8
        && parts[1].bytes().all(|b| b.is_ascii_digit())
        && parts[0] == state.access_key_id.lock().unwrap().as_str();
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
    // A body-carrying request must announce the payload hash header.
    if method == "PUT" && header(headers, "x-amz-content-sha256").is_none() {
        return Some(Response::error(
            403,
            "AccessDenied",
            "missing x-amz-content-sha256",
        ));
    }
    None
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
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        412 => "Precondition Failed",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        _ => "Unknown",
    }
}
