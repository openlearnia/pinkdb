//! The worksheet API (wave 121): login sessions, running SQL, cancelling it,
//! exporting a result, worksheets, history and the catalog.
//!
//! Every `POST` here goes through [`post_session`]: the session cookie, the
//! CSRF header and the `Origin` check (see [`crate::security`]). Every
//! statement runs on a connection of the session's identity
//! ([`UiSession::connect`]): a user's grants apply statement by statement,
//! exactly as for a protocol client.

// A handler's `Err` is the HTTP response axum sends as is; boxing it would
// only add an allocation per refusal.
#![allow(clippy::result_large_err)]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use pinkdb::{CancelFlag, Connection, ExecLimits, QueryProfile, QueryResult, Value};
use pinkdb_server_ui_api::{
    CancelRequest, Catalog, CatalogColumn, CatalogObject, DatabaseInfo, DeleteWorksheet,
    ExportRequest, HistoryList, Profile, QueryRequest, QueryResponse, ResultColumn, SaveWorksheet,
    StatementError, StatementResult, WorksheetList,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::api::json_error;
use crate::auth::{AuthState, Credentials};
use crate::book::{self, RunRecord};
use crate::policy::UiAuth;
use crate::preview::display_value;
use crate::security::{CSRF_HEADER, origin_allowed, session_cookie};
use crate::server::Shared;
use crate::sessions::{Identity, RunRefused, Sessions, UiSession};
use crate::store::quote_ident;

/// The longest text one result cell carries; longer text is cut with `…`.
pub const MAX_CELL_TEXT: usize = 32 * 1024;

// ---------- request plumbing ----------

/// Read a JSON body within the size limit and the read timeout: the head
/// timeout does not cover the body, so a slow body is bounded here.
pub(crate) async fn read_json<T: DeserializeOwned>(
    shared: &Shared,
    body: Body,
) -> Result<T, Response> {
    let limit = shared.opts.max_body_bytes;
    let bytes =
        match tokio::time::timeout(shared.opts.read_timeout, axum::body::to_bytes(body, limit))
            .await
        {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(_)) => {
                return Err(json_error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    format!("the request body is over the console's limit ({limit} bytes)"),
                ));
            }
            Err(_) => {
                return Err(json_error(
                    StatusCode::REQUEST_TIMEOUT,
                    "the request body was not received in time",
                ));
            }
        };
    serde_json::from_slice(&bytes)
        .map_err(|err| json_error(StatusCode::BAD_REQUEST, format!("invalid request: {err}")))
}

/// The session of a request that carries a live session cookie.
pub(crate) fn cookie_session(shared: &Shared, headers: &HeaderMap) -> Option<Arc<UiSession>> {
    let session = shared.sessions.get(&session_cookie(headers)?)?;
    // The empty-catalog policy holds for sessions too: once the store has a
    // user, an operator session of an unauthenticated console is over.
    if matches!(shared.auth.mode(), UiAuth::None) && shared.db.has_users() {
        shared.sessions.remove(Sessions::id_of(&session));
        return None;
    }
    Some(session)
}

/// The one-request session of a personal access token
/// (`Authorization: Bearer pdbt_…`, wave 121d): the token's owner, looked up
/// on the blocking pool.
pub(crate) async fn token_session(
    shared: &Arc<Shared>,
    headers: &HeaderMap,
) -> Option<Arc<UiSession>> {
    if matches!(shared.auth.mode(), UiAuth::None) {
        return None;
    }
    let Credentials::Token(token) = AuthState::parse(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    ) else {
        return None;
    };
    if !crate::factors::looks_like_token(&token) {
        return None;
    }
    let worker = Arc::clone(shared);
    let owner = tokio::task::spawn_blocking(move || worker.vault.token_owner(&token))
        .await
        .ok()?
        .ok()??;
    Some(UiSession::transient(Identity::User(owner.clone()), owner))
}

/// A `GET` of the API: the session cookie, or a personal access token.
pub(crate) async fn get_session(
    shared: &Arc<Shared>,
    headers: &HeaderMap,
) -> Result<Arc<UiSession>, Response> {
    if let Some(session) = cookie_session(shared, headers) {
        return Ok(session);
    }
    token_session(shared, headers)
        .await
        .ok_or_else(login_required)
}

/// A `POST`: the cookie, the CSRF token and the `Origin` — or a personal
/// access token (no cookie, so nothing for another site to ride on: the
/// token is the credential, and no CSRF token is needed).
pub(crate) async fn post_session(
    shared: &Arc<Shared>,
    headers: &HeaderMap,
) -> Result<Arc<UiSession>, Response> {
    if let Some(session) = cookie_session(shared, headers) {
        if !origin_allowed(headers, shared.opts.secure_cookies) {
            return Err(json_error(
                StatusCode::FORBIDDEN,
                "cross-origin request refused",
            ));
        }
        let token = headers
            .get(CSRF_HEADER)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if !session.csrf_matches(token) {
            return Err(json_error(
                StatusCode::FORBIDDEN,
                "missing or wrong CSRF token (X-CSRF-Token)",
            ));
        }
        return Ok(session);
    }
    token_session(shared, headers)
        .await
        .ok_or_else(login_required)
}

pub(crate) fn login_required() -> Response {
    json_error(StatusCode::UNAUTHORIZED, "login required")
}

/// Run blocking engine work on the bounded pool.
pub(crate) async fn blocking<T, F>(work: F) -> Result<T, Response>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(work).await.map_err(|err| {
        json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the console's worker failed: {err}"),
        )
    })
}

/// A JSON response.
pub(crate) fn json_ok<T: Serialize>(payload: &T) -> Response {
    match serde_json::to_vec(payload) {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(err) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cannot serialize the response: {err}"),
        ),
    }
}

// ---------- running SQL ----------

/// The wave-84 profile in milliseconds.
fn profile_info(p: &QueryProfile) -> Profile {
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    Profile {
        parse_ms: ms(p.parse),
        bind_ms: ms(p.bind),
        optimize_ms: ms(p.optimize),
        execute_ms: ms(p.execute),
        log_ms: ms(p.log),
        total_ms: ms(p.total),
        kind: p.kind.clone(),
        rows: p.rows,
    }
}

/// A cell as the browser gets it: `None` for NULL, the box renderer's text
/// otherwise (a VARCHAR verbatim), capped at [`MAX_CELL_TEXT`].
fn cell_text(value: &Value) -> Option<String> {
    let text = match value {
        Value::Null => return None,
        Value::Varchar(s) => s.clone(),
        other => display_value(other),
    };
    if text.len() > MAX_CELL_TEXT {
        let mut end = MAX_CELL_TEXT;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        Some(format!("{}…", &text[..end]))
    } else {
        Some(text)
    }
}

/// Convert one engine result.
fn statement_result(
    statement: String,
    result: QueryResult,
    profile: &QueryProfile,
    max_rows: usize,
) -> StatementResult {
    match result {
        QueryResult::Selected { names, types, rows } => {
            let row_count = rows.len() as u64;
            let truncated = rows.len() > max_rows;
            StatementResult {
                statement,
                kind: "rows".to_string(),
                message: format!("{} rows", pinkdb::agent::format_count(row_count)),
                columns: names
                    .into_iter()
                    .zip(types)
                    .map(|(name, dtype)| ResultColumn {
                        name,
                        dtype: dtype.to_string(),
                    })
                    .collect(),
                rows: rows
                    .iter()
                    .take(max_rows)
                    .map(|row| row.iter().map(cell_text).collect())
                    .collect(),
                row_count,
                truncated,
                profile: profile_info(profile),
            }
        }
        other => StatementResult {
            statement,
            kind: "ok".to_string(),
            message: pinkdb::serve::outcome_line(&other),
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: profile.rows,
            truncated: false,
            profile: profile_info(profile),
        },
    }
}

/// What the history keeps of a run's SQL: the text, unless it may carry a
/// password (`CREATE USER … PASSWORD …`, `ALTER USER …`), which stays out of
/// the console's state store.
fn history_text(sql: &str) -> String {
    if sql.to_ascii_lowercase().contains("password") {
        "-- (a statement with a password: not recorded)".to_string()
    } else {
        sql.to_string()
    }
}

/// Is a client run id acceptable (short, URL-safe)?
fn valid_run_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

/// The limits a worksheet statement runs under.
fn query_limits(shared: &Shared) -> ExecLimits {
    ExecLimits {
        statement_timeout: Some(shared.opts.query_timeout),
        max_result_rows: None,
        max_result_bytes: Some(shared.opts.query_max_bytes),
    }
}

/// A session connection set up for worksheet statements.
fn worksheet_conn(
    shared: &Shared,
    session: &UiSession,
    db: Option<&str>,
    schema: Option<&str>,
    limits: ExecLimits,
    cancel: Option<CancelFlag>,
) -> Result<Connection, String> {
    let mut conn = session.connect(&shared.db)?;
    conn.set_exec_limits(limits);
    conn.set_external_services(shared.opts.allow_external_services);
    conn.set_cancel_flag(cancel);
    if let Some(db) = db.filter(|db| !db.is_empty()) {
        let schema = schema.filter(|schema| !schema.is_empty());
        // A session connection starts in its database's `main` schema; the
        // engine decides whether the named one needs a switch.
        if schema.is_some_and(|schema| !schema.eq_ignore_ascii_case("main"))
            || !db.eq_ignore_ascii_case(conn.current_db())
        {
            let sql = match schema {
                Some(schema) => format!("USE {}.{}", quote_ident(db), quote_ident(schema)),
                None => format!("USE {}", quote_ident(db)),
            };
            conn.execute(&sql).map_err(|e| e.to_string())?;
        }
    }
    Ok(conn)
}

/// `POST /api/query`: run every statement of `sql`, in order, until one
/// fails.
pub async fn query(State(shared): State<Arc<Shared>>, headers: HeaderMap, body: Body) -> Response {
    let session = match post_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let body: QueryRequest = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    if !valid_run_id(&body.run_id) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "run_id must be 1-64 URL-safe characters",
        );
    }
    let flag = match session.begin_run(&body.run_id, shared.opts.session_queries) {
        Ok(flag) => flag,
        Err(RunRefused::Busy) => {
            return json_error(
                StatusCode::TOO_MANY_REQUESTS,
                format!(
                    "this session already runs {} queries; wait or cancel one",
                    shared.opts.session_queries
                ),
            );
        }
        Err(RunRefused::Duplicate) => {
            return json_error(StatusCode::CONFLICT, "a run with this run_id is running");
        }
    };
    let run_id = body.run_id.clone();
    let worker = Arc::clone(&shared);
    let run_session = Arc::clone(&session);
    let outcome = blocking(move || run_worksheet(&worker, &run_session, body, flag)).await;
    session.end_run(&run_id);
    match outcome {
        Ok(Ok(response)) => json_ok(&response),
        Ok(Err(response)) => response,
        Err(response) => response,
    }
}

fn run_worksheet(
    shared: &Shared,
    session: &UiSession,
    body: QueryRequest,
    flag: CancelFlag,
) -> Result<QueryResponse, Response> {
    let started_at = SystemTime::now();
    let started = Instant::now();
    let statements = pinkdb::sql::parse_statements(&body.sql);
    let finish = |results: Vec<StatementResult>, error: Option<StatementError>, db: String| {
        let status = match &error {
            None => "ok",
            Some(e) if e.cancelled => "cancelled",
            Some(_) => "error",
        };
        shared.book.record_run(&RunRecord {
            owner: &session.user,
            worksheet_id: &body.worksheet_id,
            sql: &history_text(&body.sql),
            started: started_at,
            elapsed: started.elapsed(),
            status,
            rows: results.last().map_or(0, |r| r.row_count),
            error: error.as_ref().map_or("", |e| e.message.as_str()),
        });
        QueryResponse {
            results,
            error,
            db,
            elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        }
    };
    let requested_db = body.db.clone().unwrap_or_default();
    let statements = match statements {
        Ok(statements) => statements,
        Err(err) => {
            return Ok(finish(
                Vec::new(),
                Some(StatementError {
                    index: 0,
                    statement: body.sql.clone(),
                    message: err.to_string(),
                    cancelled: false,
                }),
                requested_db,
            ));
        }
    };
    let texts = pinkdb::sql::statement_texts(&body.sql).unwrap_or_default();
    if texts.len() != statements.len() {
        return Err(json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "the statement split disagrees with the parser",
        ));
    }
    if shared.opts.read_only
        && let Some(index) = statements
            .iter()
            .position(|stmt| !pinkdb::serve::statement_allowed(stmt, false))
    {
        return Ok(finish(
            Vec::new(),
            Some(StatementError {
                index,
                statement: texts[index].clone(),
                message: "the console is read-only (--ui-read-only): only queries, EXPLAIN, \
                          DESCRIBE, SHOW and USE run here"
                    .to_string(),
                cancelled: false,
            }),
            requested_db,
        ));
    }
    let mut conn = match worksheet_conn(
        shared,
        session,
        body.db.as_deref(),
        body.schema.as_deref(),
        query_limits(shared),
        Some(flag.clone()),
    ) {
        Ok(conn) => conn,
        Err(message) => {
            return Ok(finish(
                Vec::new(),
                Some(StatementError {
                    index: 0,
                    statement: String::new(),
                    message,
                    cancelled: false,
                }),
                requested_db,
            ));
        }
    };
    let mut results = Vec::with_capacity(texts.len());
    let mut error = None;
    for (index, text) in texts.into_iter().enumerate() {
        if flag.is_cancelled() {
            error = Some(StatementError {
                index,
                statement: text,
                message: pinkdb::STATEMENT_CANCELLED.to_string(),
                cancelled: true,
            });
            break;
        }
        store_hook(&text);
        match conn.execute_profiled(&text) {
            Ok(mut outcome) => {
                if let Some((result, profile)) = outcome.pop() {
                    results.push(statement_result(
                        text,
                        result,
                        &profile,
                        shared.opts.max_shown_rows,
                    ));
                }
            }
            Err(err) => {
                let message = err.to_string();
                error = Some(StatementError {
                    index,
                    statement: text,
                    cancelled: message.contains(pinkdb::STATEMENT_CANCELLED) && flag.is_cancelled(),
                    message,
                });
                break;
            }
        }
    }
    let db = conn.current_db().to_string();
    Ok(finish(results, error, db))
}

fn store_hook(sql: &str) {
    crate::store::run_engine_hook(sql);
}

/// `POST /api/cancel`: stop one of this session's runs.
pub async fn cancel(State(shared): State<Arc<Shared>>, headers: HeaderMap, body: Body) -> Response {
    let session = match post_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let body: CancelRequest = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    json_ok(&json!({ "cancelled": session.cancel(&body.run_id) }))
}

// ---------- export ----------

/// `POST /api/export`: re-run **one read-only** statement with the export
/// cap and answer it as a CSV or JSON download.
pub async fn export(State(shared): State<Arc<Shared>>, headers: HeaderMap, body: Body) -> Response {
    let session = match post_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let body: ExportRequest = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let csv = match body.format.as_str() {
        "csv" => true,
        "json" => false,
        _ => return json_error(StatusCode::BAD_REQUEST, "format must be csv or json"),
    };
    let worker = Arc::clone(&shared);
    let outcome = blocking(move || -> Result<Vec<u8>, String> {
        let statements = pinkdb::sql::parse_statements(&body.sql).map_err(|e| e.to_string())?;
        let [statement] = statements.as_slice() else {
            return Err("export runs exactly one statement".to_string());
        };
        if !pinkdb::serve::statement_allowed(statement, false) {
            return Err(
                "export re-runs the statement, so only a query can be exported".to_string(),
            );
        }
        let limits = ExecLimits {
            statement_timeout: Some(worker.opts.query_timeout),
            max_result_rows: Some(worker.opts.max_export_rows),
            max_result_bytes: Some(worker.opts.query_max_bytes),
        };
        let mut conn = worksheet_conn(
            &worker,
            &session,
            body.db.as_deref(),
            body.schema.as_deref(),
            limits,
            None,
        )?;
        store_hook(&body.sql);
        let result = conn
            .execute(&body.sql)
            .map_err(|e| e.to_string())?
            .pop()
            .ok_or("the statement returned no result")?;
        match result {
            QueryResult::Selected { names, rows, .. } => Ok(if csv {
                to_csv(&names, &rows)
            } else {
                to_json(&names, &rows)
            }),
            _ => Err("the statement returned no rows".to_string()),
        }
    })
    .await;
    match outcome {
        Ok(Ok(bytes)) => (
            StatusCode::OK,
            [
                (
                    header::CONTENT_TYPE,
                    if csv {
                        "text/csv; charset=utf-8"
                    } else {
                        "application/json"
                    },
                ),
                (
                    header::CONTENT_DISPOSITION,
                    if csv {
                        "attachment; filename=\"result.csv\""
                    } else {
                        "attachment; filename=\"result.json\""
                    },
                ),
            ],
            bytes,
        )
            .into_response(),
        Ok(Err(message)) => json_error(StatusCode::BAD_REQUEST, message),
        Err(response) => response,
    }
}

/// RFC 4180 CSV: a header row, NULL as an empty field, quotes doubled.
pub fn to_csv(names: &[String], rows: &[Vec<Value>]) -> Vec<u8> {
    fn field(out: &mut String, text: &str) {
        if text.contains([',', '"', '\n', '\r']) {
            out.push('"');
            out.push_str(&text.replace('"', "\"\""));
            out.push('"');
        } else {
            out.push_str(text);
        }
    }
    let mut out = String::new();
    for (i, name) in names.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        field(&mut out, name);
    }
    out.push_str("\r\n");
    for row in rows {
        for (i, value) in row.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            match value {
                Value::Null => {}
                Value::Varchar(s) => field(&mut out, s),
                other => field(&mut out, &display_value(other)),
            }
        }
        out.push_str("\r\n");
    }
    out.into_bytes()
}

/// A JSON array of row objects; numbers and booleans stay JSON scalars.
pub fn to_json(names: &[String], rows: &[Vec<Value>]) -> Vec<u8> {
    let rows: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let mut object = serde_json::Map::new();
            for (name, value) in names.iter().zip(row) {
                let cell = match value {
                    Value::Null => serde_json::Value::Null,
                    Value::Boolean(b) => serde_json::Value::Bool(*b),
                    Value::Integer(n) => json!(n),
                    Value::Bigint(n) => json!(n),
                    Value::Double(x) if x.is_finite() => json!(x),
                    Value::Varchar(s) => serde_json::Value::String(s.clone()),
                    other => serde_json::Value::String(display_value(other)),
                };
                object.insert(name.clone(), cell);
            }
            serde_json::Value::Object(object)
        })
        .collect();
    serde_json::to_vec(&rows).unwrap_or_default()
}

// ---------- worksheets and history ----------

/// `GET /api/worksheets`
pub async fn worksheets(State(shared): State<Arc<Shared>>, headers: HeaderMap) -> Response {
    let session = match get_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let worker = Arc::clone(&shared);
    match blocking(move || worker.book.list(&session.user)).await {
        Ok(Ok(list)) => json_ok(&WorksheetList { worksheets: list }),
        Ok(Err(message)) => json_error(StatusCode::INTERNAL_SERVER_ERROR, message),
        Err(response) => response,
    }
}

/// `POST /api/worksheets`: create (no `id`) or update (the owner only).
pub async fn save_worksheet(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let session = match post_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let body: SaveWorksheet = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let title: String = body.title.trim().chars().take(200).collect();
    let title = if title.is_empty() {
        "Untitled".to_string()
    } else {
        title
    };
    let worker = Arc::clone(&shared);
    let outcome = blocking(
        move || match body.id.as_deref().filter(|id| !id.is_empty()) {
            None => worker
                .book
                .create(&session.user, &title, &body.sql, &body.shared_with)
                .map(Some),
            Some(id) => worker
                .book
                .update(&session.user, id, &title, &body.sql, &body.shared_with),
        },
    )
    .await;
    match outcome {
        Ok(Ok(Some(sheet))) => json_ok(&sheet),
        Ok(Ok(None)) => json_error(
            StatusCode::NOT_FOUND,
            "no such worksheet of yours (only its owner can change it)",
        ),
        Ok(Err(message)) => json_error(StatusCode::INTERNAL_SERVER_ERROR, message),
        Err(response) => response,
    }
}

/// `POST /api/worksheets/delete` (the owner only).
pub async fn delete_worksheet(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let session = match post_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let body: DeleteWorksheet = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let worker = Arc::clone(&shared);
    match blocking(move || worker.book.delete(&session.user, &body.id)).await {
        Ok(Ok(true)) => json_ok(&json!({ "deleted": true })),
        Ok(Ok(false)) => json_error(
            StatusCode::NOT_FOUND,
            "no such worksheet of yours (only its owner can delete it)",
        ),
        Ok(Err(message)) => json_error(StatusCode::INTERNAL_SERVER_ERROR, message),
        Err(response) => response,
    }
}

/// `GET /api/history`: the session user's latest runs.
pub async fn history(State(shared): State<Arc<Shared>>, headers: HeaderMap) -> Response {
    let session = match get_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let worker = Arc::clone(&shared);
    match blocking(move || worker.book.history(&session.user)).await {
        Ok(Ok(rows)) => json_ok(&HistoryList {
            history: rows,
            limit: book::HISTORY_ROWS,
        }),
        Ok(Err(message)) => json_error(StatusCode::INTERNAL_SERVER_ERROR, message),
        Err(response) => response,
    }
}

// ---------- catalog ----------

#[derive(Deserialize)]
pub(crate) struct CatalogQuery {
    db: Option<String>,
}

/// `GET /api/catalog`: every object the session may see, with columns and
/// column statistics (one call feeds the browser and the completion).
pub async fn catalog(
    State(shared): State<Arc<Shared>>,
    Query(query): Query<CatalogQuery>,
    headers: HeaderMap,
) -> Response {
    let session = match get_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let worker = Arc::clone(&shared);
    let outcome = blocking(move || -> Result<Catalog, String> {
        let limits = ExecLimits {
            statement_timeout: Some(worker.opts.statement_timeout),
            max_result_rows: Some(1_000_000),
            max_result_bytes: Some(worker.opts.max_result_bytes.max(64 * 1024 * 1024)),
        };
        // A database the browser still names may have been dropped: the
        // catalog then answers from the session's default, so the app can
        // pick an existing one.
        let mut conn =
            match worksheet_conn(&worker, &session, query.db.as_deref(), None, limits, None) {
                Ok(conn) => conn,
                Err(_) => worksheet_conn(&worker, &session, None, None, limits, None)?,
            };
        let mut select = |sql: &str| -> Result<Vec<Vec<Value>>, String> {
            store_hook(sql);
            match conn.execute(sql).map_err(|e| e.to_string())?.pop() {
                Some(QueryResult::Selected { rows, .. }) => Ok(rows),
                _ => Ok(Vec::new()),
            }
        };
        // Wave 124: every database with its storage (the DATABASE rows).
        let databases: Vec<DatabaseInfo> = select(
            "SELECT \"name\", storage, location, read_only, version, status \
             FROM pinkdb_catalog() WHERE kind = 'DATABASE' ORDER BY 1",
        )?
        .into_iter()
        .map(|row| DatabaseInfo {
            name: text(&row, 0).unwrap_or_default(),
            storage: text(&row, 1).unwrap_or_else(|| "local".to_string()),
            location: text(&row, 2),
            read_only: matches!(row.get(3), Some(Value::Boolean(true))),
            version: int(&row, 4),
            status: text(&row, 5).unwrap_or_else(|| "ok".to_string()),
        })
        .collect();
        let mut objects: Vec<CatalogObject> = select(
            // Plain views are session-local (never stored), so the catalog
            // leaves them out; materialized views are listed. DATABASE rows
            // (wave 124) describe the databases, not objects in them; SCHEMA
            // rows give the tree its empty schemas (wave 131e).
            "SELECT \"database\", \"schema\", \"name\", \"kind\", \"table_name\", \"detail\" \
             FROM pinkdb_catalog() WHERE \"kind\" NOT IN ('VIEW', 'DATABASE')",
        )?
        .into_iter()
        .map(|row| CatalogObject {
            database: text(&row, 0).unwrap_or_default(),
            schema: text(&row, 1).unwrap_or_default(),
            name: text(&row, 2).unwrap_or_default(),
            kind: text(&row, 3).unwrap_or_default(),
            table_name: text(&row, 4),
            detail: text(&row, 5),
            rows: None,
            columns: Vec::new(),
        })
        .collect();
        for row in select(
            "SELECT \"database\", \"table_name\", \"column_name\", \"data_type\", \"row_count\", \
             \"null_count\", \"distinct_count\", \"min\", \"max\" FROM pinkdb_column_stats()",
        )? {
            let (db, table) = (
                text(&row, 0).unwrap_or_default(),
                text(&row, 1).unwrap_or_default(),
            );
            if let Some(object) = objects.iter_mut().find(|o| {
                o.table_name.is_none()
                    && o.database.eq_ignore_ascii_case(&db)
                    && o.name.eq_ignore_ascii_case(&table)
            }) {
                object.rows = int(&row, 4);
                object.columns.push(CatalogColumn {
                    name: text(&row, 2).unwrap_or_default(),
                    dtype: text(&row, 3).unwrap_or_default(),
                    null_count: int(&row, 5),
                    distinct_count: int(&row, 6),
                    min: text(&row, 7),
                    max: text(&row, 8),
                });
            }
        }
        Ok(Catalog {
            current: conn.current_db().to_string(),
            databases,
            objects,
        })
    })
    .await;
    match outcome {
        Ok(Ok(payload)) => json_ok(&payload),
        Ok(Err(message)) => json_error(StatusCode::INTERNAL_SERVER_ERROR, message),
        Err(response) => response,
    }
}

fn text(row: &[Value], i: usize) -> Option<String> {
    match row.get(i) {
        None | Some(Value::Null) => None,
        Some(Value::Varchar(s)) => Some(s.clone()),
        Some(other) => Some(other.to_string()),
    }
}

fn int(row: &[Value], i: usize) -> Option<i64> {
    match row.get(i) {
        Some(Value::Bigint(n)) => Some(*n),
        Some(Value::Integer(n)) => Some(i64::from(*n)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_quotes_what_needs_quoting() {
        let names = vec!["a".to_string(), "b,c".to_string()];
        let rows = vec![
            vec![Value::Integer(1), Value::Varchar("x\"y".into())],
            vec![Value::Null, Value::Varchar("line\nbreak".into())],
        ];
        let csv = String::from_utf8(to_csv(&names, &rows)).unwrap();
        assert_eq!(csv, "a,\"b,c\"\r\n1,\"x\"\"y\"\r\n,\"line\nbreak\"\r\n");
    }

    #[test]
    fn json_keeps_scalars_typed() {
        let names = vec!["n".to_string(), "s".to_string(), "x".to_string()];
        let rows = vec![vec![Value::Bigint(7), Value::Null, Value::Double(1.5)]];
        let out: serde_json::Value = serde_json::from_slice(&to_json(&names, &rows)).unwrap();
        assert_eq!(out, json!([{ "n": 7, "s": null, "x": 1.5 }]));
    }

    #[test]
    fn long_cells_are_cut_on_a_char_boundary() {
        let long = "é".repeat(MAX_CELL_TEXT);
        let cell = cell_text(&Value::Varchar(long)).unwrap();
        assert!(cell.ends_with('…'));
        assert!(cell.len() <= MAX_CELL_TEXT + '…'.len_utf8());
        assert_eq!(cell_text(&Value::Null), None);
        assert_eq!(cell_text(&Value::Double(1.0)).as_deref(), Some("1.0"));
    }

    #[test]
    fn run_ids_are_checked() {
        assert!(valid_run_id("abc-1_2"));
        assert!(!valid_run_id(""));
        assert!(!valid_run_id("a b"));
        assert!(!valid_run_id(&"x".repeat(65)));
    }
}
