//! Wave 121d: the console's authentication over real HTTP — first-run
//! setup, password sign-in, the TOTP second factor and recovery codes,
//! personal access tokens, the account (password, sessions) and the
//! administrator's actions, and the console's own HTTPS.
//!
//! A std-only HTTP client (rustls for the HTTPS test), as in the other
//! console tests: every test is a real listener on an ephemeral loopback
//! port and an in-memory store.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use pinkdb::Database;
use pinkdb_server_ui::{UiAuth, UiHandle, UiOptions, UiServer, UiTls, step_of, totp_at};
use serde_json::{Value, json};

// ---------- a tiny HTTP client ----------

#[derive(Debug)]
struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn json(&self) -> Value {
        serde_json::from_str(&self.body)
            .unwrap_or_else(|e| panic!("not JSON ({e}): {} {}", self.status, self.body))
    }
}

fn raw_request(method: &str, path: &str, headers: &[(&str, &str)], body: Option<&str>) -> String {
    let mut text = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\n");
    for (k, v) in headers {
        text.push_str(&format!("{k}: {v}\r\n"));
    }
    if let Some(body) = body {
        text.push_str("Content-Type: application/json\r\n");
        text.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    text.push_str("Connection: close\r\n\r\n");
    if let Some(body) = body {
        text.push_str(body);
    }
    text
}

fn parse(raw: &[u8]) -> Response {
    let raw = String::from_utf8_lossy(raw).into_owned();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    Response {
        status,
        headers,
        body: body.to_string(),
    }
}

fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> Response {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    stream
        .write_all(raw_request(method, path, headers, body).as_bytes())
        .unwrap();
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    parse(&buf)
}

/// A signed-in browser: the session cookie and the CSRF token.
struct Browser {
    addr: SocketAddr,
    cookie: String,
    csrf: String,
}

impl Browser {
    fn from(addr: SocketAddr, response: &Response) -> Browser {
        assert_eq!(response.status, 200, "{}", response.body);
        let cookie = response
            .header("set-cookie")
            .and_then(|c| c.split(';').next())
            .expect("a session cookie")
            .to_string();
        let csrf = response.json()["csrf"].as_str().unwrap().to_string();
        Browser { addr, cookie, csrf }
    }

    fn get(&self, path: &str) -> Response {
        request(self.addr, "GET", path, &[("Cookie", &self.cookie)], None)
    }

    fn post(&self, path: &str, body: &Value) -> Response {
        request(
            self.addr,
            "POST",
            path,
            &[("Cookie", &self.cookie), ("X-CSRF-Token", &self.csrf)],
            Some(&body.to_string()),
        )
    }
}

fn post(addr: SocketAddr, path: &str, body: &Value) -> Response {
    request(addr, "POST", path, &[], Some(&body.to_string()))
}

fn sign_in(addr: SocketAddr, user: &str, password: &str) -> Response {
    post(
        addr,
        "/api/login",
        &json!({ "user": user, "password": password }),
    )
}

// ---------- fixtures ----------

struct Console {
    addr: SocketAddr,
    setup_code: String,
    handle: UiHandle,
}

impl Drop for Console {
    fn drop(&mut self) {
        self.handle.shutdown();
    }
}

fn start(db: Database, opts: UiOptions) -> Console {
    let server = UiServer::bind("127.0.0.1:0", db, opts).unwrap();
    let addr = server.local_addr();
    let setup_code = server.setup_code().to_string();
    Console {
        addr,
        setup_code,
        handle: server.spawn().unwrap(),
    }
}

/// alice (ADMIN), bob (no grants). Sign-in is the default mode.
fn store_with_users() -> Database {
    let db = Database::new();
    db.connect()
        .execute(
            "CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1), (2); \
             CREATE USER alice PASSWORD 'alice-password'; GRANT ADMIN TO alice; \
             CREATE USER bob PASSWORD 'bob-password';",
        )
        .unwrap();
    db
}

/// The authenticator code `steps` steps from now.
fn code(secret: &str, steps: i64) -> String {
    let now = step_of(SystemTime::now()) as i64;
    totp_at(secret, (now + steps) as u64).unwrap()
}

/// Turn two-factor on for a signed-in browser; returns the secret and the
/// recovery codes.
fn enrol(browser: &Browser) -> (String, Vec<String>) {
    let r = browser.post("/api/account/totp/start", &json!({}));
    assert_eq!(r.status, 200, "{}", r.body);
    let enrolment = r.json();
    let secret = enrolment["secret"].as_str().unwrap().to_string();
    assert!(
        enrolment["uri"]
            .as_str()
            .unwrap()
            .starts_with("otpauth://totp/pinkdb:")
    );
    assert!(enrolment["qr_path"].as_str().unwrap().starts_with('M'));
    let r = browser.post(
        "/api/account/totp/confirm",
        &json!({ "code": code(&secret, 0) }),
    );
    assert_eq!(r.status, 200, "{}", r.body);
    let codes: Vec<String> = r.json()["codes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c.as_str().unwrap().to_string())
        .collect();
    assert_eq!(codes.len(), 10);
    (secret, codes)
}

// ---------- first-run setup ----------

#[test]
fn a_store_without_users_starts_in_setup() {
    let db = Database::new();
    let console = start(db.clone(), UiOptions::default());
    let addr = console.addr;

    let r = request(addr, "GET", "/api/session", &[], None);
    assert_eq!(r.status, 401);
    assert_eq!(r.json()["auth"], "setup");
    // Nothing is served before the first account exists.
    assert_eq!(
        request(addr, "GET", "/api/databases", &[], None).status,
        401
    );
    assert_eq!(sign_in(addr, "anyone", "anything-at-all").status, 409);

    let setup = |code: &str, user: &str, password: &str| {
        post(
            addr,
            "/api/setup",
            &json!({ "code": code, "user": user, "password": password }),
        )
    };
    let r = setup("AAAA-BBBB-CCCC", "admin", "a long enough password");
    assert_eq!(r.status, 401, "a wrong code: {}", r.body);
    let r = setup(&console.setup_code, "admin", "short");
    assert_eq!(r.status, 400, "a short password: {}", r.body);
    let r = setup(&console.setup_code, "bad name", "a long enough password");
    assert_eq!(r.status, 400, "a bad name: {}", r.body);

    // The code is case- and dash-insensitive.
    let typed = console.setup_code.to_lowercase().replace('-', "");
    let r = setup(&typed, "Admin", "a long enough password");
    let admin = Browser::from(addr, &r);
    let session = r.json();
    assert_eq!(session["user"], "admin");
    assert_eq!(session["admin"], true);
    assert_eq!(session["auth"], "password");
    assert!(db.has_users());

    // Setup is over: a second one is refused, sign-in works.
    let r = setup(&console.setup_code, "eve", "a long enough password");
    assert_eq!(r.status, 409, "{}", r.body);
    assert_eq!(
        request(addr, "GET", "/api/session", &[], None).json()["auth"],
        "password"
    );
    Browser::from(addr, &sign_in(addr, "admin", "a long enough password"));
    assert_eq!(admin.get("/api/databases").status, 200);
}

// ---------- the second factor ----------

#[test]
fn two_factor_sign_in_with_codes_and_recovery_codes() {
    let console = start(store_with_users(), UiOptions::default());
    let addr = console.addr;
    let bob = Browser::from(addr, &sign_in(addr, "bob", "bob-password"));
    assert_eq!(bob.get("/api/session").json()["two_factor"], false);
    // A code that does not match does not turn it on.
    bob.post("/api/account/totp/start", &json!({}));
    let r = bob.post("/api/account/totp/confirm", &json!({ "code": "000000" }));
    assert_eq!(r.status, 400, "{}", r.body);
    let (secret, recovery) = enrol(&bob);
    assert_eq!(bob.get("/api/session").json()["two_factor"], true);
    assert_eq!(bob.get("/api/account").json()["recovery_left"], 10);

    // The password alone no longer starts a session.
    let r = sign_in(addr, "bob", "bob-password");
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(r.header("set-cookie").is_none(), "no session yet");
    assert_eq!(r.json()["second_factor"], "totp");
    let challenge = r.json()["challenge"].as_str().unwrap().to_string();
    let verify = |challenge: &str, code: &str| {
        post(
            addr,
            "/api/login/verify",
            &json!({ "challenge": challenge, "code": code }),
        )
    };
    assert_eq!(verify(&challenge, "000000").status, 401);
    // The confirming code's step is spent; the next step's code works once.
    let next = code(&secret, 1);
    Browser::from(addr, &verify(&challenge, &next));
    let r = sign_in(addr, "bob", "bob-password");
    let challenge = r.json()["challenge"].as_str().unwrap().to_string();
    assert_eq!(verify(&challenge, &next).status, 401, "a replayed code");
    // A recovery code works once.
    Browser::from(addr, &verify(&challenge, &recovery[0]));
    let r = sign_in(addr, "bob", "bob-password");
    let challenge = r.json()["challenge"].as_str().unwrap().to_string();
    assert_eq!(verify(&challenge, &recovery[0]).status, 401, "a used code");
    // A challenge runs out of tries.
    for _ in 0..4 {
        verify(&challenge, "123456");
    }
    assert_eq!(verify(&challenge, &recovery[1]).status, 401, "out of tries");
    // An unknown challenge is refused.
    assert_eq!(verify("nope", &recovery[1]).status, 401);

    // Turning it off needs the password.
    let r = bob.post("/api/account/totp/disable", &json!({ "password": "wrong" }));
    assert_eq!(r.status, 401, "{}", r.body);
    let r = bob.post(
        "/api/account/totp/disable",
        &json!({ "password": "bob-password" }),
    );
    assert_eq!(r.status, 200, "{}", r.body);
    Browser::from(addr, &sign_in(addr, "bob", "bob-password"));
}

#[test]
fn an_administrator_resets_a_lost_second_factor() {
    let console = start(store_with_users(), UiOptions::default());
    let addr = console.addr;
    let bob = Browser::from(addr, &sign_in(addr, "bob", "bob-password"));
    enrol(&bob);
    let alice = Browser::from(addr, &sign_in(addr, "alice", "alice-password"));

    let access = alice.get("/api/access").json();
    let row = access["principals"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "bob")
        .unwrap()
        .clone();
    assert_eq!(row["two_factor"], true);
    assert_eq!(row["sessions"], 1);

    // Bob may not reset anyone; alice may.
    let r = bob.post("/api/access/totp/reset", &json!({ "name": "alice" }));
    assert_eq!(r.status, 403, "{}", r.body);
    let r = alice.post("/api/access/totp/reset", &json!({ "name": "bob" }));
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(r.json()["was_on"], true);
    assert_eq!(bob.get("/api/session").status, 401, "bob's sessions ended");
    let r = sign_in(addr, "bob", "bob-password");
    assert!(r.header("set-cookie").is_some(), "the password alone again");

    // Ending someone's sessions.
    let bob = Browser::from(addr, &r);
    let r = alice.post("/api/access/sessions/end", &json!({ "name": "bob" }));
    assert_eq!(r.json()["ended"], 1);
    assert_eq!(bob.get("/api/session").status, 401);
}

// ---------- the account ----------

#[test]
fn changing_the_password_ends_the_other_sessions() {
    let console = start(store_with_users(), UiOptions::default());
    let addr = console.addr;
    let here = Browser::from(addr, &sign_in(addr, "bob", "bob-password"));
    let there = Browser::from(addr, &sign_in(addr, "bob", "bob-password"));
    let account = here.get("/api/account").json();
    assert_eq!(account["sessions"].as_array().unwrap().len(), 2);
    assert_eq!(
        account["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|s| s["current"] == true)
            .count(),
        1
    );

    let change = |current: &str, new: &str| {
        here.post(
            "/api/account/password",
            &json!({ "current": current, "new": new }),
        )
    };
    assert_eq!(change("wrong", "a new long password").status, 401);
    assert_eq!(change("bob-password", "short").status, 400);
    let r = change("bob-password", "a new long password");
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(r.json()["ended"], 1);
    assert_eq!(
        there.get("/api/session").status,
        401,
        "the other session ended"
    );
    assert_eq!(here.get("/api/session").status, 200, "this one stays");
    assert_eq!(sign_in(addr, "bob", "bob-password").status, 401);
    Browser::from(addr, &sign_in(addr, "bob", "a new long password"));

    // Ending sessions one by one, then every other one.
    let account = here.get("/api/account").json();
    let other = account["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["current"] == false)
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let r = here.post("/api/account/sessions/end", &json!({ "id": other }));
    assert_eq!(r.json()["ended"], 1);
    Browser::from(addr, &sign_in(addr, "bob", "a new long password"));
    let r = here.post("/api/account/sessions/end", &json!({ "others": true }));
    assert_eq!(r.json()["ended"], 1);
    assert_eq!(
        here.get("/api/account").json()["sessions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn personal_access_tokens_work_until_revoked() {
    let console = start(store_with_users(), UiOptions::default());
    let addr = console.addr;
    let bob = Browser::from(addr, &sign_in(addr, "bob", "bob-password"));
    let r = bob.post(
        "/api/account/tokens",
        &json!({ "name": "nightly job", "days": 30 }),
    );
    assert_eq!(r.status, 200, "{}", r.body);
    let created = r.json();
    let secret = created["secret"].as_str().unwrap().to_string();
    let id = created["token"]["id"].as_str().unwrap().to_string();
    assert!(secret.starts_with("pdbt_"));
    assert!(created["token"]["expires_at"].as_u64().unwrap() > 0);
    let listed = bob.get("/api/account").json()["tokens"].clone();
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert!(
        !listed.to_string().contains(&secret),
        "the secret is shown once"
    );

    let bearer = format!("Bearer {secret}");
    let auth = [("Authorization", bearer.as_str())];
    // The browse API and the worksheet API, as bob (his grants apply).
    assert_eq!(
        request(addr, "GET", "/api/databases", &auth, None).status,
        200
    );
    let r = request(
        addr,
        "POST",
        "/api/query",
        &auth,
        Some(&json!({ "sql": "SELECT current_user()", "run_id": "r1" }).to_string()),
    );
    assert_eq!(r.status, 200, "no CSRF token is needed: {}", r.body);
    assert_eq!(r.json()["results"][0]["rows"][0][0], "bob");
    let r = request(
        addr,
        "POST",
        "/api/query",
        &auth,
        Some(&json!({ "sql": "SELECT * FROM t", "run_id": "r2" }).to_string()),
    );
    assert!(
        r.json()["error"].is_object(),
        "bob has no grant on t: {}",
        r.body
    );
    // A token cannot change credentials.
    let r = request(
        addr,
        "POST",
        "/api/account/tokens",
        &auth,
        Some(&json!({ "name": "more", "days": 0 }).to_string()),
    );
    assert_eq!(r.status, 403, "{}", r.body);
    // A wrong token, a password in a header: refused.
    let wrong = [(
        "Authorization",
        "Bearer pdbt_0000000000000000000000000000000000000000000",
    )];
    assert_eq!(
        request(addr, "GET", "/api/databases", &wrong, None).status,
        401
    );

    let r = bob.post("/api/account/tokens/revoke", &json!({ "id": id }));
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        request(addr, "GET", "/api/databases", &auth, None).status,
        401
    );
    let r = bob.post("/api/account/tokens", &json!({ "name": "", "days": 0 }));
    assert_eq!(r.status, 400);
}

#[test]
fn a_dropped_users_credentials_are_swept() {
    let db = store_with_users();
    let console = start(db.clone(), UiOptions::default());
    let addr = console.addr;
    let bob = Browser::from(addr, &sign_in(addr, "bob", "bob-password"));
    let secret = bob
        .post("/api/account/tokens", &json!({ "name": "t", "days": 0 }))
        .json()["secret"]
        .as_str()
        .unwrap()
        .to_string();
    enrol(&bob);
    // Dropped outside the console; an administrator opens the Access page
    // (which sweeps, as the minute timer does); a new bob appears later.
    db.connect().execute("DROP USER bob").unwrap();
    let alice = Browser::from(addr, &sign_in(addr, "alice", "alice-password"));
    assert_eq!(alice.get("/api/access").status, 200);
    db.connect()
        .execute("CREATE USER bob PASSWORD 'another-password'")
        .unwrap();
    let bearer = format!("Bearer {secret}");
    assert_eq!(
        request(
            addr,
            "GET",
            "/api/databases",
            &[("Authorization", &bearer)],
            None
        )
        .status,
        401
    );
    let r = sign_in(addr, "bob", "another-password");
    assert!(
        r.header("set-cookie").is_some(),
        "no second factor inherited"
    );
}

// ---------- HTTPS ----------

fn tls_options() -> (UiOptions, rustls::pki_types::CertificateDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let der = cert.cert.der().clone();
    let opts = UiOptions {
        tls: Some(UiTls {
            cert_pem: cert.cert.pem().into_bytes(),
            key_pem: cert.signing_key.serialize_pem().into_bytes(),
        }),
        ..UiOptions::default()
    };
    (opts, der)
}

fn https(
    addr: SocketAddr,
    root: &rustls::pki_types::CertificateDer<'static>,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Response {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(root.clone()).unwrap();
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let conn = rustls::ClientConnection::new(Arc::new(config), name).unwrap();
    let sock = TcpStream::connect(addr).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let mut tls = rustls::StreamOwned::new(conn, sock);
    tls.write_all(raw_request(method, path, &[], body).as_bytes())
        .unwrap();
    let mut buf = Vec::new();
    let _ = tls.read_to_end(&mut buf);
    parse(&buf)
}

#[test]
fn the_console_serves_https() {
    let (opts, root) = tls_options();
    let server = UiServer::bind("127.0.0.1:0", store_with_users(), opts).unwrap();
    let addr = server.local_addr();
    assert!(server.url().starts_with("https://"));
    let handle = server.spawn().unwrap();

    let r = https(addr, &root, "GET", "/", None);
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        r.header("strict-transport-security"),
        Some("max-age=31536000")
    );
    let body = json!({ "user": "alice", "password": "alice-password" }).to_string();
    let r = https(addr, &root, "POST", "/api/login", Some(&body));
    assert_eq!(r.status, 200, "{}", r.body);
    let cookie = r.header("set-cookie").unwrap();
    assert!(cookie.contains("; Secure"), "{cookie}");

    // Plain HTTP on the TLS port gets no answer.
    let r = request(addr, "GET", "/", &[], None);
    assert_ne!(r.status, 200);
    handle.shutdown();

    // A key that does not match the certificate is refused at bind.
    let (mut opts, _) = tls_options();
    let (other, _) = tls_options();
    opts.tls.as_mut().unwrap().key_pem = other.tls.unwrap().key_pem;
    let err = UiServer::bind("127.0.0.1:0", Database::new(), opts).unwrap_err();
    assert!(err.to_string().contains("TLS"), "{err}");
}

#[test]
fn none_mode_is_refused_with_users_and_beyond_loopback() {
    let opts = UiOptions {
        auth: UiAuth::None,
        ..UiOptions::default()
    };
    let err = UiServer::bind("127.0.0.1:0", store_with_users(), opts.clone()).unwrap_err();
    assert!(err.to_string().contains("--ui-auth none"), "{err}");
    let err = pinkdb_server_ui::check_bind_policy(
        "192.0.2.1".parse().unwrap(),
        pinkdb_server_ui::ConsoleTls::Configured,
        &UiAuth::None,
    )
    .unwrap_err();
    assert!(err.to_string().contains("sign-in"), "{err}");
}
