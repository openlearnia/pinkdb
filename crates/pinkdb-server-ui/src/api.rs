//! The console's JSON API and its server-rendered pages.
//!
//! Every handler is **read-only**: the router has no mutating route, and the
//! store access layer ([`crate::store`]) can only run catalog reads, counts
//! and `SELECT`s.
//!
//! The engine work runs on the console's **bounded blocking pool**
//! ([`tokio::task::spawn_blocking`]): pinkdb's API is synchronous, so the
//! async runtime must never execute it. The JSON/HTML rendering happens
//! after the blocking task returned, with **no store lock held**.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::auth::{AuthFailure, Credentials, LoginPermit};
use crate::html;
use crate::policy::UiAuth;
use crate::preview::{preview_cell, type_width_bytes};
use crate::server::Shared;
use crate::store::Session;

/// The schema name pinkdb's implicit schema carries (`pinkdb` has no schema
/// layer: every database holds its relations in `main`).
pub const DEFAULT_SCHEMA: &str = "main";

/// The default page size (bounded by [`crate::UiOptions::max_page_size`]).
pub const DEFAULT_PAGE_SIZE: usize = 50;

// ---------- payloads ----------

/// `GET /api/databases`
#[derive(Serialize)]
pub struct DatabasesResponse {
    /// The database the console's session browses.
    pub current: String,
    /// Every database of the store.
    pub databases: Vec<DatabaseInfo>,
}

/// One database.
#[derive(Serialize)]
pub struct DatabaseInfo {
    /// The database name.
    pub name: String,
    /// Is this the session's current database?
    pub current: bool,
}

/// `GET /api/schemas`
#[derive(Serialize)]
pub struct SchemasResponse {
    /// The database the schemas belong to.
    pub db: String,
    /// The schemas (pinkdb has exactly one: `main`).
    pub schemas: Vec<SchemaInfo>,
}

/// One schema.
#[derive(Serialize)]
pub struct SchemaInfo {
    /// The schema name.
    pub name: String,
    /// How many relations the schema holds.
    pub relations: usize,
}

/// `GET /api/tables`
#[derive(Serialize)]
pub struct TablesResponse {
    /// The database the tables belong to.
    pub db: String,
    /// The schema the tables belong to.
    pub schema: String,
    /// The relations of the schema.
    pub tables: Vec<TableInfo>,
}

/// One relation.
#[derive(Serialize)]
pub struct TableInfo {
    /// The relation name.
    pub name: String,
    /// The relation's columns.
    pub columns: Vec<ColumnInfo>,
}

/// One column.
#[derive(Serialize)]
pub struct ColumnInfo {
    /// The column name.
    pub name: String,
    /// The DuckDB-style type name.
    #[serde(rename = "type")]
    pub dtype: String,
    /// Does the column accept NULLs?
    pub nullable: bool,
}

/// `GET /api/rows`
#[derive(Serialize)]
pub struct RowsResponse {
    /// The database the rows come from.
    pub db: String,
    /// The relation the rows come from.
    pub table: String,
    /// The 1-based page number.
    pub page: u64,
    /// The effective page size (bounded by the console's maximum).
    pub page_size: usize,
    /// The exact total number of rows of the relation.
    pub total_rows: u64,
    /// The number of pages (`total_rows / page_size`, rounded up).
    pub total_pages: u64,
    /// The relation's columns.
    pub columns: Vec<ColumnInfo>,
    /// The page's rows.
    pub rows: Vec<Vec<CellInfo>>,
}

/// One previewed cell.
#[derive(Serialize)]
pub struct CellInfo {
    /// The cell's text (the box renderer's rendering).
    pub text: String,
    /// The cell is a SQL NULL.
    pub null: bool,
    /// The text was cut at the console's per-cell cap.
    pub truncated: bool,
}

/// `GET /api/stats`
#[derive(Serialize)]
pub struct StatsResponse {
    /// The database the statistics belong to.
    pub db: String,
    /// One entry per relation.
    pub tables: Vec<TableStats>,
}

/// One relation's statistics.
#[derive(Serialize)]
pub struct TableStats {
    /// The relation name.
    pub name: String,
    /// The exact row count.
    pub rows: u64,
    /// The estimated size in bytes (see [`type_width_bytes`]).
    pub estimated_bytes: u64,
    /// How many columns the relation has.
    pub columns: usize,
}

/// An error payload.
#[derive(Serialize)]
pub struct ErrorResponse {
    /// The error message.
    pub error: String,
}

// ---------- query parameters ----------

/// The `db` parameter.
#[derive(Deserialize)]
pub struct DbQuery {
    /// The database to browse (defaults to the session's database).
    pub db: Option<String>,
}

/// The `db`/`schema` parameters.
#[derive(Deserialize)]
pub struct TablesQuery {
    /// The database to browse (defaults to the session's database).
    pub db: Option<String>,
    /// The schema to browse (pinkdb has exactly one: `main`).
    pub schema: Option<String>,
}

/// The row-preview parameters.
#[derive(Deserialize)]
pub struct RowsQuery {
    /// The database to browse (defaults to the session's database).
    pub db: Option<String>,
    /// The relation to preview.
    pub table: Option<String>,
    /// The 1-based page number.
    pub page: Option<u64>,
    /// The page size (bounded by the console's maximum).
    pub page_size: Option<usize>,
}

// ---------- errors ----------

/// Why a request failed.
pub enum ApiError {
    /// The request was malformed.
    BadRequest(String),
    /// The addressed object does not exist.
    NotFound(String),
    /// The request was not authenticated.
    Auth(AuthFailure),
    /// The engine refused the statement.
    Engine(String),
}

impl ApiError {
    /// The HTTP status and body for this error.
    pub fn into_response(self, mode: &UiAuth) -> Response {
        match self {
            ApiError::BadRequest(msg) => json_error(StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound(msg) => json_error(StatusCode::NOT_FOUND, msg),
            ApiError::Auth(failure) => auth_failure_response(failure, mode),
            ApiError::Engine(msg) => {
                // The engine's "not found" errors are the client's fault.
                if msg.to_ascii_lowercase().contains("not found") {
                    json_error(StatusCode::NOT_FOUND, msg)
                } else {
                    json_error(StatusCode::INTERNAL_SERVER_ERROR, msg)
                }
            }
        }
    }
}

impl From<String> for ApiError {
    fn from(msg: String) -> Self {
        ApiError::Engine(msg)
    }
}

/// A JSON error response.
pub fn json_error(status: StatusCode, message: impl Into<String>) -> Response {
    let body = serde_json::to_vec(&ErrorResponse {
        error: message.into(),
    })
    .unwrap_or_else(|_| b"{\"error\":\"internal error\"}".to_vec());
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// The response for a failed authentication.
fn auth_failure_response(failure: AuthFailure, mode: &UiAuth) -> Response {
    match failure {
        AuthFailure::Missing | AuthFailure::Rejected => {
            let mut response = json_error(
                StatusCode::UNAUTHORIZED,
                match failure {
                    AuthFailure::Missing => "authentication required",
                    _ => "authentication failed",
                },
            );
            if matches!(mode, UiAuth::Basic) {
                response.headers_mut().insert(
                    header::WWW_AUTHENTICATE,
                    header::HeaderValue::from_static("Basic realm=\"pinkdb\""),
                );
            }
            response
        }
        AuthFailure::Blocked => json_error(
            StatusCode::TOO_MANY_REQUESTS,
            "too many failed authentications from this address; try again later",
        ),
        AuthFailure::Busy => json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "too many concurrent logins; try again later",
        ),
    }
}

// ---------- the authentication plan ----------

/// What a request proved about itself.
enum AuthPlan {
    /// No credential is needed (the store has no users).
    Anonymous,
    /// A bearer token was verified.
    Token,
    /// HTTP Basic credentials: the blocking task verifies them through the
    /// engine and runs the statements under that user's grants.
    Basic {
        /// The user name.
        user: String,
        /// The password.
        password: String,
        /// The verification slot (dropped when the blocking task ends).
        _permit: LoginPermit,
    },
}

/// Decide what a request must prove, before any statement runs.
fn plan_auth(
    shared: &Arc<Shared>,
    headers: &HeaderMap,
    peer: Option<SocketAddr>,
) -> Result<AuthPlan, ApiError> {
    let mode = shared.auth.mode().clone();
    let credentials = crate::auth::AuthState::parse(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    );
    match mode {
        UiAuth::None => Ok(AuthPlan::Anonymous),
        UiAuth::Token(_) => match credentials {
            Credentials::Token(token) if shared.auth.token_matches(&token) => Ok(AuthPlan::Token),
            _ => Err(ApiError::Auth(AuthFailure::Missing)),
        },
        UiAuth::Basic => {
            let ip = peer.map(|addr| addr.ip());
            if let Some(ip) = ip
                && shared.auth.blocked_for(ip).is_some()
            {
                return Err(ApiError::Auth(AuthFailure::Blocked));
            }
            match credentials {
                Credentials::Basic { user, password } => {
                    let permit = shared
                        .auth
                        .try_verify()
                        .ok_or(ApiError::Auth(AuthFailure::Busy))?;
                    Ok(AuthPlan::Basic {
                        user,
                        password,
                        _permit: permit,
                    })
                }
                _ => Err(ApiError::Auth(AuthFailure::Missing)),
            }
        }
    }
}

// ---------- the handlers ----------

/// `GET /api/databases`
pub async fn databases(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    peer: ConnectInfo<SocketAddr>,
) -> Response {
    handle(
        shared,
        headers,
        peer,
        |session, _shared| {
            let current = session.current_db().to_string();
            let names = session.databases()?;
            Ok(DatabasesResponse {
                current: current.clone(),
                databases: names
                    .into_iter()
                    .map(|name| DatabaseInfo {
                        current: name.eq_ignore_ascii_case(&current),
                        name,
                    })
                    .collect(),
            })
        },
        json_response,
    )
    .await
}

/// `GET /api/schemas`
pub async fn schemas(
    State(shared): State<Arc<Shared>>,
    Query(query): Query<DbQuery>,
    headers: HeaderMap,
    peer: ConnectInfo<SocketAddr>,
) -> Response {
    handle(
        shared,
        headers,
        peer,
        move |session, _shared| {
            let db = resolve_db(session, query.db.as_deref())?;
            let relations = session.relations(&db)?;
            Ok(SchemasResponse {
                db,
                schemas: vec![SchemaInfo {
                    name: DEFAULT_SCHEMA.to_string(),
                    relations: relations.len(),
                }],
            })
        },
        json_response,
    )
    .await
}

/// `GET /api/tables`
pub async fn tables(
    State(shared): State<Arc<Shared>>,
    Query(query): Query<TablesQuery>,
    headers: HeaderMap,
    peer: ConnectInfo<SocketAddr>,
) -> Response {
    handle(shared, headers, peer, move |session, _shared| {
        let db = resolve_db(session, query.db.as_deref())?;
        if let Some(schema) = query.schema.as_deref()
            && !schema.eq_ignore_ascii_case(DEFAULT_SCHEMA)
        {
            return Err(ApiError::NotFound(format!(
                "schema {schema:?} not found: this store has one schema per database ({DEFAULT_SCHEMA})"
            )));
        }
        let relations = session.relations(&db)?;
        Ok(TablesResponse {
            db,
            schema: DEFAULT_SCHEMA.to_string(),
            tables: relations
                .into_iter()
                .map(|relation| TableInfo {
                    name: relation.name,
                    columns: relation.columns.into_iter().map(column_info).collect(),
                })
                .collect(),
        })
    }, json_response)
    .await
}

/// `GET /api/rows`
pub async fn rows(
    State(shared): State<Arc<Shared>>,
    Query(query): Query<RowsQuery>,
    headers: HeaderMap,
    peer: ConnectInfo<SocketAddr>,
) -> Response {
    handle(
        shared,
        headers,
        peer,
        move |session, shared| {
            let db = resolve_db(session, query.db.as_deref())?;
            let table = query
                .table
                .as_deref()
                .ok_or_else(|| ApiError::BadRequest("missing parameter: table".to_string()))?
                .to_string();
            let page = query.page.unwrap_or(1);
            if page == 0 {
                return Err(ApiError::BadRequest(
                    "the page number is 1-based: page=0 is not a page".to_string(),
                ));
            }
            let page_size = query
                .page_size
                .unwrap_or(DEFAULT_PAGE_SIZE)
                .clamp(1, shared.opts.max_page_size);
            let offset = page.saturating_sub(1).saturating_mul(page_size as u64);
            let offset = usize::try_from(offset).unwrap_or(usize::MAX);
            let total_rows = session.row_count(&db, &table)?;
            let total_pages = if total_rows == 0 {
                0
            } else {
                total_rows.div_ceil(page_size as u64)
            };
            let page = session.rows(&db, &table, page_size, offset)?;
            Ok(RowsResponse {
                db,
                table,
                page: query.page.unwrap_or(1),
                page_size,
                total_rows,
                total_pages,
                columns: page.columns.into_iter().map(column_info).collect(),
                rows: page
                    .rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|value| {
                                let cell = preview_cell(&value);
                                CellInfo {
                                    text: cell.text,
                                    null: cell.is_null,
                                    truncated: cell.truncated,
                                }
                            })
                            .collect()
                    })
                    .collect(),
            })
        },
        json_response,
    )
    .await
}

/// `GET /api/stats`
pub async fn stats(
    State(shared): State<Arc<Shared>>,
    Query(query): Query<DbQuery>,
    headers: HeaderMap,
    peer: ConnectInfo<SocketAddr>,
) -> Response {
    handle(
        shared,
        headers,
        peer,
        move |session, _shared| {
            let db = resolve_db(session, query.db.as_deref())?;
            let relations = session.relations(&db)?;
            let mut tables = Vec::with_capacity(relations.len());
            for relation in relations {
                let rows = session.row_count(&db, &relation.name)?;
                let width: u64 = relation
                    .columns
                    .iter()
                    .map(|column| type_width_bytes(&column.dtype))
                    .sum();
                tables.push(TableStats {
                    name: relation.name,
                    rows,
                    estimated_bytes: rows.saturating_mul(width),
                    columns: relation.columns.len(),
                });
            }
            Ok(StatsResponse { db, tables })
        },
        json_response,
    )
    .await
}

/// `GET /` — the console shell.
pub async fn shell(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    _peer: ConnectInfo<SocketAddr>,
) -> Response {
    // The shell is a static page: in token mode it carries no data, so it
    // is served without a credential (the browser cannot send a bearer
    // token on a top-level navigation). In Basic mode the browser must
    // present credentials — the page carries no data, so the credential is
    // not verified here (every API call verifies it through the engine).
    if matches!(shared.auth.mode(), UiAuth::Basic) {
        let credentials = crate::auth::AuthState::parse(
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
        );
        if !matches!(credentials, Credentials::Basic { .. }) {
            return auth_failure_response(AuthFailure::Missing, shared.auth.mode());
        }
    }
    html::shell_response(&shared)
}

/// `GET /preview` — the server-rendered row preview.
pub async fn preview(
    State(shared): State<Arc<Shared>>,
    Query(query): Query<RowsQuery>,
    headers: HeaderMap,
    peer: ConnectInfo<SocketAddr>,
) -> Response {
    handle(
        shared,
        headers,
        peer,
        move |session, shared| {
            let db = resolve_db(session, query.db.as_deref())?;
            let table = query
                .table
                .as_deref()
                .ok_or_else(|| ApiError::BadRequest("missing parameter: table".to_string()))?
                .to_string();
            let page = query.page.unwrap_or(1).max(1);
            let page_size = query
                .page_size
                .unwrap_or(DEFAULT_PAGE_SIZE)
                .clamp(1, shared.opts.max_page_size);
            let offset = page.saturating_sub(1).saturating_mul(page_size as u64);
            let offset = usize::try_from(offset).unwrap_or(usize::MAX);
            let total_rows = session.row_count(&db, &table)?;
            let total_pages = if total_rows == 0 {
                0
            } else {
                total_rows.div_ceil(page_size as u64)
            };
            let rows = session.rows(&db, &table, page_size, offset)?;
            Ok(html::PreviewPage {
                db,
                table,
                page,
                page_size,
                total_rows,
                total_pages,
                columns: rows.columns.into_iter().map(column_info).collect(),
                rows: rows
                    .rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|value| {
                                let cell = preview_cell(&value);
                                (cell.text, cell.is_null, cell.truncated)
                            })
                            .collect()
                    })
                    .collect(),
            })
        },
        |page| html::preview_response(&page),
    )
    .await
}

/// The database a request addresses (the session's database by default).
fn resolve_db(session: &Session, requested: Option<&str>) -> Result<String, ApiError> {
    match requested {
        Some(name) if !name.is_empty() => Ok(name.to_string()),
        _ => Ok(session.current_db().to_string()),
    }
}

/// The API's column payload.
fn column_info(column: crate::store::ColumnInfo) -> ColumnInfo {
    ColumnInfo {
        name: column.name,
        dtype: column.dtype,
        nullable: column.nullable,
    }
}

/// Run one read-only handler: authenticate, then run the engine work on the
/// bounded blocking pool.
async fn handle<T, F, R>(
    shared: Arc<Shared>,
    headers: HeaderMap,
    peer: ConnectInfo<SocketAddr>,
    body: F,
    render: R,
) -> Response
where
    F: FnOnce(&mut Session, &Arc<Shared>) -> Result<T, ApiError> + Send + 'static,
    T: Send + 'static,
    R: FnOnce(T) -> Response + Send + 'static,
{
    let mode = shared.auth.mode().clone();
    let plan = match plan_auth(&shared, &headers, Some(peer.0)) {
        Ok(plan) => plan,
        Err(err) => return err.into_response(&mode),
    };
    let ip = Some(peer.0.ip());
    let worker = shared.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        let mut session = worker.take_session();
        // The empty-catalog policy is re-checked per request: the store may
        // have gained a user since the console started, and an
        // unauthenticated console must stop serving it.
        if matches!(plan, AuthPlan::Anonymous) && worker.db.has_users() {
            worker.put_session(session);
            return Err(ApiError::Auth(AuthFailure::Rejected));
        }
        if let AuthPlan::Basic { user, password, .. } = &plan
            && session.login(user, password).is_err()
        {
            worker.put_session(session);
            if let Some(ip) = ip {
                worker.auth.record_failure(ip);
            }
            return Err(ApiError::Auth(AuthFailure::Rejected));
        }
        let result = body(&mut session, &worker);
        if let Some(ip) = ip
            && result.is_ok()
        {
            worker.auth.record_success(ip);
        }
        worker.put_session(session);
        result
    })
    .await;
    match outcome {
        Ok(Ok(payload)) => render(payload),
        Ok(Err(err)) => err.into_response(&mode),
        Err(err) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the console's worker failed: {err}"),
        ),
    }
}

/// Render a payload as a JSON response.
fn json_response<T: Serialize>(payload: T) -> Response {
    match serde_json::to_vec(&payload) {
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
