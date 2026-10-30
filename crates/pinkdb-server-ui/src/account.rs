//! Signing in and the account (wave 121d): first-run setup, the password
//! and TOTP sign-in, sessions, personal access tokens, and the
//! administrator's actions on other users' credentials.
//!
//! The sign-in steps:
//!
//! 1. `GET /api/session` — the session, or `401` with the form to show:
//!    `setup` (a store without users), `password`, or `none`.
//! 2. `POST /api/setup` — the first account, with the setup code the server
//!    printed; it becomes an administrator and is signed in.
//! 3. `POST /api/login` — the password, verified by the engine. A user with
//!    two-factor gets a short-lived challenge instead of a session.
//! 4. `POST /api/login/verify` — the challenge and an authenticator code
//!    (or a recovery code) start the session.
//!
//! Account changes (password, two-factor, tokens, sessions) need a signed-in
//! **browser** session: a personal access token may run queries, but it may
//! not mint more tokens or turn the second factor off.

// A handler's `Err` is the HTTP response axum sends as is.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use pinkdb::QueryResult;
use pinkdb_server_ui_api::{
    AccountInfo, CreatedToken, EndSessions, LoginRequest, NewToken, PasswordChange, PasswordCheck,
    RecoveryCodes, RevokeToken, SecondFactor, SessionInfo, SessionRow, SetupRequest, TokenRow,
    TotpConfirm, TotpEnrolment, UserAction, VerifyRequest,
};
use serde_json::json;

use crate::api::json_error;
use crate::auth::constant_time_eq;
use crate::factors::{new_totp_secret, normalize_code, otpauth_uri, qr_path, step_of, totp_verify};
use crate::policy::UiAuth;
use crate::security::{clear_session_cookie, origin_allowed, set_session_cookie};
use crate::server::Shared;
use crate::sessions::{Identity, Origin, Sessions, UiSession};
use crate::store::quote_ident;
use crate::vault::{TokenRecord, now_secs};
use crate::work::{blocking, cookie_session, get_session, json_ok, post_session, read_json};

/// The shortest password the console accepts for a new password.
pub const MIN_PASSWORD: usize = 10;

/// How long a two-factor challenge lives.
pub const CHALLENGE_TTL: Duration = Duration::from_secs(5 * 60);

/// How many codes one challenge may try.
pub const CHALLENGE_TRIES: u32 = 5;

/// The most challenges kept at once.
const MAX_CHALLENGES: usize = 1024;

/// The authenticator apps' label for the console.
const ISSUER: &str = "pinkdb";

// ---------- helpers ----------

/// Why a new password is not good enough, if it is not.
pub fn password_problem(user: &str, password: &str) -> Option<String> {
    if password.chars().count() < MIN_PASSWORD {
        return Some(format!(
            "a password needs at least {MIN_PASSWORD} characters"
        ));
    }
    if password.eq_ignore_ascii_case(user) {
        return Some("the password must not be the user name".to_string());
    }
    None
}

/// A user name the statements can carry.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
}

/// Where a request comes from (the session list shows it).
fn origin_of(headers: &HeaderMap, peer: SocketAddr) -> Origin {
    Origin {
        ip: Some(peer.ip()),
        user_agent: headers
            .get(header::USER_AGENT)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .chars()
            .take(200)
            .collect(),
    }
}

/// The mode's name in `SessionInfo::auth`.
fn auth_name(mode: &UiAuth) -> &'static str {
    match mode {
        UiAuth::Password => "password",
        UiAuth::None => "none",
    }
}

/// `429` for an address the failed-login limiter blocks.
fn blocked(shared: &Shared, ip: IpAddr) -> Option<Response> {
    shared.auth.blocked_for(ip).map(|wait| {
        json_error(
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "too many failed sign-ins from this address; try again in {} s",
                wait.as_secs().max(1)
            ),
        )
    })
}

fn bad_request(message: impl Into<String>) -> Response {
    json_error(StatusCode::BAD_REQUEST, message.into())
}

fn server_error(message: impl Into<String>) -> Response {
    json_error(StatusCode::INTERNAL_SERVER_ERROR, message.into())
}

/// Verify `user`'s password through the engine (bounded by the login gate).
async fn check_password(
    shared: &Arc<Shared>,
    ip: IpAddr,
    user: &str,
    password: &str,
) -> Result<(), Response> {
    if let Some(response) = blocked(shared, ip) {
        return Err(response);
    }
    let Some(permit) = shared.auth.try_verify() else {
        return Err(json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "too many sign-ins at once; try again shortly",
        ));
    };
    let db = shared.db.clone();
    let (user, password) = (user.to_string(), password.to_string());
    let ok = blocking(move || {
        let _permit = permit;
        db.connect().login(&user, &password).is_ok()
    })
    .await?;
    if ok {
        Ok(())
    } else {
        shared.auth.record_failure(ip);
        Err(json_error(
            StatusCode::UNAUTHORIZED,
            "the password is wrong",
        ))
    }
}

/// Run `ALTER USER … PASSWORD` (or `CREATE USER`) on a trusted connection:
/// the console decided the request may (the user proved the current
/// password, or an administrator asked).
fn set_password(
    db: &pinkdb::Database,
    user: &str,
    password: &str,
    create: bool,
) -> Result<(), String> {
    let sql = format!(
        "{} USER {} PASSWORD '{}'",
        if create { "CREATE" } else { "ALTER" },
        quote_ident(user),
        password.replace('\'', "''")
    );
    db.connect()
        .execute(&sql)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// The users of the store (names, lower case).
pub(crate) fn store_users(db: &pinkdb::Database) -> Result<Vec<String>, String> {
    let mut conn = db.connect();
    match conn
        .execute("SELECT \"name\" FROM pinkdb_users() WHERE \"kind\" = 'USER'")
        .map_err(|e| e.to_string())?
        .pop()
    {
        Some(QueryResult::Selected { rows, .. }) => Ok(rows
            .iter()
            .filter_map(|r| r.first())
            .map(|v| v.to_string().to_ascii_lowercase())
            .collect()),
        _ => Ok(Vec::new()),
    }
}

/// Drop the credentials of users that no longer exist (a user dropped and
/// later recreated under the same name must not inherit them).
pub(crate) fn sweep(shared: &Shared) {
    // A console without sign-in has no users and no credentials.
    if *shared.auth.mode() == UiAuth::None {
        return;
    }
    if let Ok(users) = store_users(&shared.db) {
        let _ = shared.vault.sweep(&users);
    }
}

/// Start a session and answer with it (and its cookie).
async fn start_session(
    shared: &Arc<Shared>,
    identity: Identity,
    user: String,
    origin: Origin,
) -> Response {
    let session = shared.sessions.create(identity, user, origin);
    let mut response = session_info(shared, &session).await;
    if let Ok(cookie) = HeaderValue::from_str(&set_session_cookie(
        Sessions::id_of(&session),
        shared.opts.secure_cookies,
    )) {
        response.headers_mut().insert(header::SET_COOKIE, cookie);
    }
    response
}

/// The session's own answer; `admin` and `two_factor` are read each time
/// (a grant or an enrolment since the sign-in counts at once).
pub(crate) async fn session_info(shared: &Arc<Shared>, session: &Arc<UiSession>) -> Response {
    let worker = Arc::clone(shared);
    let who = Arc::clone(session);
    let (admin, two_factor) = tokio::task::spawn_blocking(move || {
        let admin = crate::admin::session_is_admin(&worker, &who);
        let two_factor = match &who.identity {
            Identity::User(user) => worker.vault.totp(user).ok().flatten().is_some(),
            Identity::Operator => false,
        };
        (admin, two_factor)
    })
    .await
    .unwrap_or((false, false));
    json_ok(&SessionInfo {
        user: session.user.clone(),
        csrf: session.csrf().to_string(),
        auth: auth_name(shared.auth.mode()).to_string(),
        read_only: shared.opts.read_only,
        agent: crate::ask::agent_loaded(shared),
        admin,
        max_shown_rows: shared.opts.max_shown_rows,
        two_factor,
    })
}

/// `current_user()` of a trusted session.
fn operator_name(db: &pinkdb::Database) -> String {
    let mut conn = db.connect();
    match conn
        .execute("SELECT current_user()")
        .ok()
        .and_then(|mut r| r.pop())
    {
        Some(QueryResult::Selected { rows, .. }) => rows
            .first()
            .and_then(|row| row.first())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "pinkdb".to_string()),
        _ => "pinkdb".to_string(),
    }
}

// ---------- two-factor challenges ----------

struct Challenge {
    user: String,
    ip: IpAddr,
    created: Instant,
    tries: u32,
}

/// The password-verified sign-ins waiting for their second factor.
#[derive(Default)]
pub struct Challenges {
    map: Mutex<HashMap<String, Challenge>>,
}

impl Challenges {
    /// A challenge for `user`, answerable from `ip` only.
    pub fn create(&self, user: &str, ip: IpAddr) -> String {
        let id = crate::security::random_token();
        let now = Instant::now();
        let mut map = self.map.lock().unwrap_or_else(|p| p.into_inner());
        map.retain(|_, c| now.duration_since(c.created) < CHALLENGE_TTL);
        while map.len() >= MAX_CHALLENGES {
            let Some(oldest) = map
                .iter()
                .min_by_key(|(_, c)| c.created)
                .map(|(id, _)| id.clone())
            else {
                break;
            };
            map.remove(&oldest);
        }
        map.insert(
            id.clone(),
            Challenge {
                user: user.to_string(),
                ip,
                created: now,
                tries: 0,
            },
        );
        id
    }

    /// Spend one try of a challenge: its user, while it is live, from the
    /// same address, with tries left.
    pub fn attempt(&self, id: &str, ip: IpAddr) -> Option<String> {
        let mut map = self.map.lock().unwrap_or_else(|p| p.into_inner());
        let challenge = map.get_mut(id)?;
        if challenge.created.elapsed() >= CHALLENGE_TTL
            || challenge.ip != ip
            || challenge.tries >= CHALLENGE_TRIES
        {
            map.remove(id);
            return None;
        }
        challenge.tries += 1;
        Some(challenge.user.clone())
    }

    /// The challenge is answered.
    pub fn finish(&self, id: &str) {
        self.map
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(id);
    }
}

// ---------- signing in ----------

/// `GET /api/session`: the session, or `401` with the form to show.
pub async fn session(State(shared): State<Arc<Shared>>, headers: HeaderMap) -> Response {
    match cookie_session(&shared, &headers) {
        Some(session) => session_info(&shared, &session).await,
        None => {
            let form = match shared.auth.mode() {
                UiAuth::None => "none",
                UiAuth::Password if !shared.db.has_users() => "setup",
                UiAuth::Password => "password",
            };
            let mut response = json_ok(&json!({ "error": "login required", "auth": form }));
            *response.status_mut() = StatusCode::UNAUTHORIZED;
            response
        }
    }
}

/// `POST /api/setup`: the first account of a store without users.
pub async fn setup(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    peer: ConnectInfo<SocketAddr>,
    body: Body,
) -> Response {
    if !origin_allowed(&headers, shared.opts.secure_cookies) {
        return json_error(StatusCode::FORBIDDEN, "cross-origin request refused");
    }
    let body: SetupRequest = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let ip = peer.0.ip();
    if let Some(response) = blocked(&shared, ip) {
        return response;
    }
    if *shared.auth.mode() != UiAuth::Password || shared.db.has_users() {
        return json_error(
            StatusCode::CONFLICT,
            "the store already has users: sign in instead",
        );
    }
    let expected = normalize_code(&shared.setup_code);
    if !constant_time_eq(normalize_code(&body.code).as_bytes(), expected.as_bytes()) {
        shared.auth.record_failure(ip);
        return json_error(
            StatusCode::UNAUTHORIZED,
            "the setup code is wrong (it is in the server's log)",
        );
    }
    let user = body.user.trim().to_ascii_lowercase();
    if !valid_name(&user) {
        return bad_request("a user name is letters, digits, `_`, `-` and `.`");
    }
    if let Some(problem) = password_problem(&user, &body.password) {
        return bad_request(problem);
    }
    let worker = Arc::clone(&shared);
    let (name, password) = (user.clone(), body.password);
    let created = blocking(move || -> Result<(), String> {
        // One setup at a time, and only while the store has no users: two
        // browsers racing for the first account cannot both win.
        let _guard = worker.setup_lock.lock().unwrap_or_else(|p| p.into_inner());
        if worker.db.has_users() {
            return Err("the store already has users: sign in instead".to_string());
        }
        worker.vault.forget(&name)?;
        set_password(&worker.db, &name, &password, true)?;
        worker
            .db
            .connect()
            .execute(&format!("GRANT ADMIN TO {}", quote_ident(&name)))
            .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await;
    match created {
        Ok(Ok(())) => {}
        Ok(Err(message)) => return json_error(StatusCode::CONFLICT, message),
        Err(response) => return response,
    }
    shared.auth.record_success(ip);
    eprintln!("pinkdb-server-ui: first-run setup: created administrator {user}");
    start_session(
        &shared,
        Identity::User(user.clone()),
        user,
        origin_of(&headers, peer.0),
    )
    .await
}

/// `POST /api/login`: the password, once. A user with two-factor gets a
/// challenge; everyone else a session.
pub async fn login(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    peer: ConnectInfo<SocketAddr>,
    body: Body,
) -> Response {
    if !origin_allowed(&headers, shared.opts.secure_cookies) {
        return json_error(StatusCode::FORBIDDEN, "cross-origin request refused");
    }
    let body: LoginRequest = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let ip = peer.0.ip();
    if let Some(response) = blocked(&shared, ip) {
        return response;
    }
    let origin = origin_of(&headers, peer.0);
    match shared.auth.mode() {
        UiAuth::None => {
            if shared.db.has_users() {
                return json_error(
                    StatusCode::UNAUTHORIZED,
                    "the store has users: this console must be restarted with sign-in",
                );
            }
            let db = shared.db.clone();
            let user = blocking(move || operator_name(&db))
                .await
                .unwrap_or_else(|_| "pinkdb".to_string());
            shared.auth.record_success(ip);
            start_session(&shared, Identity::Operator, user, origin).await
        }
        UiAuth::Password => {
            let (Some(user), Some(password)) = (body.user, body.password) else {
                return bad_request("a user and a password are required");
            };
            if !shared.db.has_users() {
                return json_error(
                    StatusCode::CONFLICT,
                    "the store has no users yet: create the first account (setup)",
                );
            }
            let Some(permit) = shared.auth.try_verify() else {
                return json_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "too many sign-ins at once; try again shortly",
                );
            };
            let worker = Arc::clone(&shared);
            let verified = blocking(move || -> Option<(String, bool)> {
                let _permit = permit;
                worker.db.connect().login(&user, &password).ok()?;
                let user = user.to_ascii_lowercase();
                let two_factor = worker.vault.totp(&user).ok().flatten().is_some();
                Some((user, two_factor))
            })
            .await;
            match verified {
                Ok(Some((user, false))) => {
                    shared.auth.record_success(ip);
                    start_session(&shared, Identity::User(user.clone()), user, origin).await
                }
                Ok(Some((user, true))) => json_ok(&SecondFactor {
                    second_factor: "totp".to_string(),
                    challenge: shared.challenges.create(&user, ip),
                }),
                Ok(None) => {
                    shared.auth.record_failure(ip);
                    json_error(
                        StatusCode::UNAUTHORIZED,
                        "the user or the password is wrong",
                    )
                }
                Err(response) => response,
            }
        }
    }
}

/// `POST /api/login/verify`: the second factor — an authenticator code or a
/// recovery code — for a challenge.
pub async fn verify(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    peer: ConnectInfo<SocketAddr>,
    body: Body,
) -> Response {
    if !origin_allowed(&headers, shared.opts.secure_cookies) {
        return json_error(StatusCode::FORBIDDEN, "cross-origin request refused");
    }
    let body: VerifyRequest = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let ip = peer.0.ip();
    if let Some(response) = blocked(&shared, ip) {
        return response;
    }
    let Some(user) = shared.challenges.attempt(&body.challenge, ip) else {
        return json_error(
            StatusCode::UNAUTHORIZED,
            "this sign-in expired; enter the password again",
        );
    };
    let worker = Arc::clone(&shared);
    let (who, code) = (user.clone(), body.code);
    let accepted = blocking(move || -> Result<bool, String> {
        let Some((secret, last)) = worker.vault.totp(&who)? else {
            return Ok(false);
        };
        let digits: String = code.chars().filter(|c| !c.is_whitespace()).collect();
        if digits.len() == 6 && digits.chars().all(|c| c.is_ascii_digit()) {
            return match totp_verify(
                &secret,
                &digits,
                step_of(std::time::SystemTime::now()),
                last,
            ) {
                Some(step) => worker.vault.spend_step(&who, step),
                None => Ok(false),
            };
        }
        worker.vault.use_recovery_code(&who, &code)
    })
    .await;
    match accepted {
        Ok(Ok(true)) => {
            shared.challenges.finish(&body.challenge);
            shared.auth.record_success(ip);
            start_session(
                &shared,
                Identity::User(user.clone()),
                user,
                origin_of(&headers, peer.0),
            )
            .await
        }
        Ok(Ok(false)) => {
            shared.auth.record_failure(ip);
            json_error(
                StatusCode::UNAUTHORIZED,
                "the code is wrong or was already used",
            )
        }
        Ok(Err(message)) => server_error(message),
        Err(response) => response,
    }
}

/// `POST /api/logout`.
pub async fn logout(State(shared): State<Arc<Shared>>, headers: HeaderMap) -> Response {
    let session = match post_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    shared.sessions.remove(Sessions::id_of(&session));
    let mut response = json_ok(&json!({ "ok": true }));
    if let Ok(cookie) = HeaderValue::from_str(&clear_session_cookie(shared.opts.secure_cookies)) {
        response.headers_mut().insert(header::SET_COOKIE, cookie);
    }
    response
}

// ---------- the account ----------

/// A signed-in **browser** session for an account change (a personal access
/// token may not change credentials), with the CSRF check.
async fn browser_session(
    shared: &Arc<Shared>,
    headers: &HeaderMap,
) -> Result<(Arc<UiSession>, String), Response> {
    if cookie_session(shared, headers).is_none() {
        return Err(json_error(
            StatusCode::FORBIDDEN,
            "account changes need a signed-in browser session, not an access token",
        ));
    }
    let session = post_session(shared, headers).await?;
    match &session.identity {
        Identity::User(user) => {
            let user = user.clone();
            Ok((session, user))
        }
        Identity::Operator => Err(bad_request(
            "this console runs without sign-in: there is no account to change",
        )),
    }
}

fn token_row(t: TokenRecord) -> TokenRow {
    TokenRow {
        id: t.id,
        name: t.name,
        hint: t.hint,
        created_at: t.created_at,
        expires_at: t.expires_at,
        last_used_at: t.last_used_at,
    }
}

/// `GET /api/account`.
pub async fn account(State(shared): State<Arc<Shared>>, headers: HeaderMap) -> Response {
    let session = match get_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let sessions: Vec<SessionRow> = shared
        .sessions
        .of_user(&session.user)
        .iter()
        .map(|s| SessionRow {
            id: s.public_id.clone(),
            started: s
                .started
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
            idle_secs: Sessions::last_seen(s).elapsed().as_secs(),
            ip: s.origin.ip.map(|ip| ip.to_string()).unwrap_or_default(),
            user_agent: s.origin.user_agent.clone(),
            current: s.public_id == session.public_id,
        })
        .collect();
    let worker = Arc::clone(&shared);
    let who = Arc::clone(&session);
    let facts = blocking(
        move || -> Result<(bool, bool, usize, Vec<TokenRow>), String> {
            let admin = crate::admin::session_is_admin(&worker, &who);
            match &who.identity {
                Identity::Operator => Ok((admin, false, 0, Vec::new())),
                Identity::User(user) => Ok((
                    admin,
                    worker.vault.totp(user)?.is_some(),
                    worker.vault.recovery_left(user)?,
                    worker
                        .vault
                        .tokens(user)?
                        .into_iter()
                        .map(token_row)
                        .collect(),
                )),
            }
        },
    )
    .await;
    match facts {
        Ok(Ok((admin, two_factor, recovery_left, tokens))) => json_ok(&AccountInfo {
            user: session.user.clone(),
            admin,
            has_password: matches!(session.identity, Identity::User(_)),
            two_factor,
            recovery_left,
            sessions,
            tokens,
        }),
        Ok(Err(message)) => server_error(message),
        Err(response) => response,
    }
}

/// `POST /api/account/password`: the current password, then the new one.
/// Every other session of the user ends.
pub async fn change_password(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    peer: ConnectInfo<SocketAddr>,
    body: Body,
) -> Response {
    let (session, user) = match browser_session(&shared, &headers).await {
        Ok(pair) => pair,
        Err(response) => return response,
    };
    let body: PasswordChange = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    if let Some(problem) = password_problem(&user, &body.new) {
        return bad_request(problem);
    }
    if let Err(response) = check_password(&shared, peer.0.ip(), &user, &body.current).await {
        return response;
    }
    let db = shared.db.clone();
    let (who, new) = (user.clone(), body.new);
    match blocking(move || set_password(&db, &who, &new, false)).await {
        Ok(Ok(())) => {
            let ended = shared
                .sessions
                .end_user(&user, Some(Sessions::id_of(&session)));
            json_ok(&json!({ "ok": true, "ended": ended }))
        }
        Ok(Err(message)) => server_error(message),
        Err(response) => response,
    }
}

/// `POST /api/account/sessions/end`: one of the user's sessions, or every
/// other one.
pub async fn end_sessions(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let session = match post_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let body: EndSessions = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let ended = if body.others {
        shared
            .sessions
            .end_user(&session.user, Some(Sessions::id_of(&session)))
    } else {
        match body.id {
            Some(id) => usize::from(shared.sessions.end_public(&session.user, &id)),
            None => return bad_request("name a session, or ask for every other one"),
        }
    };
    json_ok(&json!({ "ok": true, "ended": ended }))
}

/// `POST /api/account/tokens`: a new personal access token (shown once).
pub async fn create_token(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (_, user) = match browser_session(&shared, &headers).await {
        Ok(pair) => pair,
        Err(response) => return response,
    };
    let body: NewToken = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let name = body.name.trim().to_string();
    if name.is_empty() || name.chars().count() > 64 || name.chars().any(char::is_control) {
        return bad_request("a token needs a name of up to 64 characters");
    }
    if body.days > 3650 {
        return bad_request("a token expires within ten years, or never (0 days)");
    }
    let expires = if body.days == 0 {
        0
    } else {
        now_secs() + u64::from(body.days) * 86_400
    };
    let worker = Arc::clone(&shared);
    match blocking(move || worker.vault.create_token(&user, &name, expires)).await {
        Ok(Ok((record, secret))) => json_ok(&CreatedToken {
            token: token_row(record),
            secret,
        }),
        Ok(Err(message)) => bad_request(message),
        Err(response) => response,
    }
}

/// `POST /api/account/tokens/revoke`.
pub async fn revoke_token(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (_, user) = match browser_session(&shared, &headers).await {
        Ok(pair) => pair,
        Err(response) => return response,
    };
    let body: RevokeToken = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let worker = Arc::clone(&shared);
    match blocking(move || worker.vault.revoke_token(&user, &body.id)).await {
        Ok(Ok(true)) => json_ok(&json!({ "ok": true })),
        Ok(Ok(false)) => json_error(StatusCode::NOT_FOUND, "no such token"),
        Ok(Err(message)) => server_error(message),
        Err(response) => response,
    }
}

/// `POST /api/account/totp/start`: a new secret to enrol (held in the
/// session until a code confirms it).
pub async fn totp_start(State(shared): State<Arc<Shared>>, headers: HeaderMap) -> Response {
    let (session, user) = match browser_session(&shared, &headers).await {
        Ok(pair) => pair,
        Err(response) => return response,
    };
    let secret = new_totp_secret();
    let uri = otpauth_uri(ISSUER, &user, &secret);
    let Some((qr_path, qr_size)) = qr_path(&uri) else {
        return server_error("cannot draw the QR code");
    };
    *session
        .pending_totp
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = Some(secret.clone());
    json_ok(&TotpEnrolment {
        secret,
        uri,
        qr_path,
        qr_size,
    })
}

/// `POST /api/account/totp/confirm`: a code from the app turns two-factor on;
/// the answer holds the recovery codes (shown once).
pub async fn totp_confirm(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let (session, user) = match browser_session(&shared, &headers).await {
        Ok(pair) => pair,
        Err(response) => return response,
    };
    let body: TotpConfirm = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let Some(secret) = session
        .pending_totp
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
    else {
        return bad_request("start the set-up first");
    };
    let Some(step) = totp_verify(
        &secret,
        &body.code,
        step_of(std::time::SystemTime::now()),
        None,
    ) else {
        return bad_request("the code does not match: check the app's clock and try the next code");
    };
    let worker = Arc::clone(&shared);
    match blocking(move || worker.vault.enable_totp(&user, &secret, step)).await {
        Ok(Ok(codes)) => {
            *session
                .pending_totp
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = None;
            json_ok(&RecoveryCodes { codes })
        }
        Ok(Err(message)) => server_error(message),
        Err(response) => response,
    }
}

/// `POST /api/account/totp/disable`: the password turns two-factor off.
pub async fn totp_disable(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    peer: ConnectInfo<SocketAddr>,
    body: Body,
) -> Response {
    let (_, user) = match browser_session(&shared, &headers).await {
        Ok(pair) => pair,
        Err(response) => return response,
    };
    let body: PasswordCheck = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    if let Err(response) = check_password(&shared, peer.0.ip(), &user, &body.password).await {
        return response;
    }
    let worker = Arc::clone(&shared);
    match blocking(move || worker.vault.disable_totp(&user)).await {
        Ok(Ok(_)) => json_ok(&json!({ "ok": true })),
        Ok(Err(message)) => server_error(message),
        Err(response) => response,
    }
}

/// `POST /api/account/recovery`: the password replaces the recovery codes.
pub async fn new_recovery(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    peer: ConnectInfo<SocketAddr>,
    body: Body,
) -> Response {
    let (_, user) = match browser_session(&shared, &headers).await {
        Ok(pair) => pair,
        Err(response) => return response,
    };
    let body: PasswordCheck = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    if let Err(response) = check_password(&shared, peer.0.ip(), &user, &body.password).await {
        return response;
    }
    let worker = Arc::clone(&shared);
    match blocking(move || -> Result<Option<Vec<String>>, String> {
        if worker.vault.totp(&user)?.is_none() {
            return Ok(None);
        }
        worker.vault.new_recovery_codes(&user).map(Some)
    })
    .await
    {
        Ok(Ok(Some(codes))) => json_ok(&RecoveryCodes { codes }),
        Ok(Ok(None)) => bad_request("two-factor is off: there are no recovery codes"),
        Ok(Err(message)) => server_error(message),
        Err(response) => response,
    }
}

// ---------- an administrator's actions ----------

/// An administrator's session (and the session itself), for an action on
/// another user's credentials.
async fn admin_session(
    shared: &Arc<Shared>,
    headers: &HeaderMap,
) -> Result<Arc<UiSession>, Response> {
    let session = post_session(shared, headers).await?;
    let worker = Arc::clone(shared);
    let who = Arc::clone(&session);
    let admin = blocking(move || crate::admin::session_is_admin(&worker, &who)).await?;
    if admin {
        Ok(session)
    } else {
        Err(json_error(
            StatusCode::FORBIDDEN,
            "only an administrator may do that",
        ))
    }
}

/// The cookie to keep when an action ends a user's sessions: the caller's
/// own, when they act on themselves.
fn keep_own<'a>(session: &'a UiSession, user: &str) -> Option<&'a str> {
    session
        .user
        .eq_ignore_ascii_case(user)
        .then(|| Sessions::id_of(session))
}

/// `POST /api/access/sessions/end`: end every console session of a user.
pub async fn admin_end_sessions(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let session = match admin_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let body: UserAction = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let ended = shared
        .sessions
        .end_user(&body.name, keep_own(&session, &body.name));
    json_ok(&json!({ "ok": true, "ended": ended }))
}

/// `POST /api/access/totp/reset`: turn a user's two-factor off (a lost
/// phone) and end their sessions.
pub async fn admin_reset_totp(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let session = match admin_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let body: UserAction = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let worker = Arc::clone(&shared);
    let name = body.name.to_ascii_lowercase();
    let who = name.clone();
    match blocking(move || worker.vault.disable_totp(&who)).await {
        Ok(Ok(was_on)) => {
            let ended = shared.sessions.end_user(&name, keep_own(&session, &name));
            json_ok(&json!({ "ok": true, "was_on": was_on, "ended": ended }))
        }
        Ok(Err(message)) => server_error(message),
        Err(response) => response,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passwords_and_names() {
        assert!(password_problem("ana", "short").is_some());
        assert!(password_problem("anastasia12", "ANASTASIA12").is_some());
        assert!(password_problem("ana", "correct horse").is_none());
        assert!(valid_name("ana.b-c_1"));
        assert!(!valid_name("a b"));
        assert!(!valid_name("x'y"));
        assert!(!valid_name(""));
    }

    #[test]
    fn challenges_expire_and_run_out_of_tries() {
        let challenges = Challenges::default();
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let other: IpAddr = "127.0.0.2".parse().unwrap();
        let id = challenges.create("ana", ip);
        assert_eq!(challenges.attempt(&id, ip).as_deref(), Some("ana"));
        assert_eq!(challenges.attempt(&id, other), None, "another address");
        assert_eq!(challenges.attempt(&id, ip), None, "a wrong address ends it");
        let id = challenges.create("ana", ip);
        for _ in 0..CHALLENGE_TRIES {
            assert!(challenges.attempt(&id, ip).is_some());
        }
        assert_eq!(challenges.attempt(&id, ip), None, "out of tries");
        let id = challenges.create("ana", ip);
        challenges.finish(&id);
        assert_eq!(challenges.attempt(&id, ip), None);
    }
}
