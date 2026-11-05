//! The HTTP surface: `POST /sql`, `POST /recover`, `GET /health` and
//! `/healthz`, plus — unless `DUCKDB_READ_ONLY` — the `/write-lease` control
//! surface (`docs/wave-138-plan.md`).
//!
//! Every request that touches the warehouse opens its own [`Connection`],
//! positions it on the warehouse database (`USE "<alias>"`) and, when the
//! script left a transaction open, rolls it back: the store never sees a
//! half-open transaction from a request that has already answered.
//!
//! The lease state machine itself lives in [`crate::lease`]; this module maps
//! it onto the Go service's statuses, codes and bodies.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use pinkdb::Database;
use pinkdb::QueryResult;
use pinkdb::types::Value;
use serde_json::Map;

use crate::QuacklabOptions;
use crate::encode::{EncodeError, push_json_string, write_row};
use crate::errors::{engine_message, error_body};
use crate::lease::{
    AcquireError, Admit, EndError, Grant, Leases, LocalWarehouse, Released, Warehouse, clamp_ttl,
    rfc3339,
};

/// Go's `maxLeaseBodyBytes`.
const MAX_LEASE_BODY: usize = 4 << 10;

/// How much of a refused request's body is read before answering. Closing a
/// socket with unread bytes pending resets the connection and the client loses
/// the refusal it is owed; Go drains the body for the same reason.
const MAX_DRAIN_BODY: usize = 64 << 10;

const READ_ONLY_MESSAGE: &str = "Invalid Input Error: this warehouse is read-only \
     (DUCKDB_READ_ONLY); only queries, EXPLAIN, DESCRIBE, SHOW and USE run here";

/// The engine handle and options every request shares.
pub(crate) struct Shared {
    pub(crate) db: Database,
    pub(crate) options: QuacklabOptions,
    pub(crate) warehouse: Arc<dyn Warehouse>,
    pub(crate) leases: Arc<Leases>,
    /// Requests whose statement may take the store's write lock: what
    /// `/health` reports as `writerBusy`. pinkdb's store lock has no try-lock,
    /// so the gauge is what keeps the probe from queueing behind a writer.
    writers: AtomicUsize,
}

impl Shared {
    pub(crate) fn new(
        db: Database,
        options: QuacklabOptions,
        warehouse: Arc<dyn Warehouse>,
    ) -> Self {
        crate::sandbox::apply(&db, options.sandbox);
        Self {
            db,
            options,
            warehouse,
            leases: Arc::new(Leases::new()),
            writers: AtomicUsize::new(0),
        }
    }

    fn writer_busy(&self) -> bool {
        self.writers.load(Ordering::Relaxed) > 0
    }

    fn busy(&self) -> Busy<'_> {
        self.writers.fetch_add(1, Ordering::Relaxed);
        Busy { shared: self }
    }

    /// Go's `recoverPoolForce`: end any active lease (its holder's SQL drains
    /// first), reopen the warehouse, then probe it.
    async fn recover_force(self: &Arc<Self>) -> Result<(), String> {
        self.leases
            .end_active("manual", &self.warehouse)
            .await
            .map_err(|error| match error {
                EndError::Reopen(message) => format!("reopen warehouse: {message}"),
                EndError::Unknown => String::new(),
            })?;
        // Go's `reopenWarehouse` waits for in-flight requests before it
        // closes the warehouse under them.
        self.leases.wait_drained().await;
        let warehouse = Arc::clone(&self.warehouse);
        tokio::task::spawn_blocking(move || {
            warehouse.close()?;
            warehouse.open()
        })
        .await
        .map_err(|join| format!("the warehouse worker failed: {join}"))??;
        let shared = Arc::clone(self);
        tokio::task::spawn_blocking(move || shared.probe())
            .await
            .map_err(|join| format!("the warehouse check failed: {join}"))?
    }

    /// `SELECT 1` in the warehouse: what `/health` and `/recover` answer with.
    pub(crate) fn probe(&self) -> Result<(), String> {
        self.warehouse.ensure_open()?;
        let mut conn = self.db.connect();
        conn.execute(&format!("USE {}", quoted(&self.options.warehouse)))
            .map_err(|e| engine_message(&e))?;
        conn.execute("SELECT 1").map_err(|e| engine_message(&e))?;
        Ok(())
    }

    /// Create the warehouse database when the store does not have it yet.
    pub(crate) fn ensure_warehouse(&self) -> Result<(), String> {
        let statement = format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            quoted(&self.options.warehouse)
        );
        self.db
            .connect()
            .execute(&statement)
            .map_err(|e| engine_message(&e))?;
        Ok(())
    }
}

/// A router over `db`, for an embedder that brings its own listener.
pub fn router(db: Database, options: QuacklabOptions) -> Router {
    router_with_warehouse(db, options, Arc::new(LocalWarehouse))
}

/// A router whose leases close and open `warehouse` for their duration (138c
/// gives the lake one that detaches and re-attaches its catalog).
pub fn router_with_warehouse(
    db: Database,
    options: QuacklabOptions,
    warehouse: Arc<dyn Warehouse>,
) -> Router {
    routes(Arc::new(Shared::new(db, options, warehouse)))
}

pub(crate) fn routes(shared: Arc<Shared>) -> Router {
    // The token guards the two endpoints that touch the warehouse; health is
    // open so a load balancer can probe it.
    let guarded = Router::new()
        .route("/sql", post(run_sql))
        .route("/recover", post(recover))
        .route_layer(axum::middleware::from_fn_with_state(
            shared.clone(),
            authorize,
        ));
    let mut router = Router::new()
        .route("/health", get(health))
        .route("/healthz", get(health))
        .merge(guarded);
    if !shared.options.read_only {
        // A read-only warehouse has nothing to lease: the routes do not exist.
        let lease = Router::new()
            .route("/write-lease/acquire", post(acquire))
            .route("/write-lease/renew", post(renew))
            .route("/write-lease/release", post(release))
            .route("/write-lease/sql", post(run_lease_sql))
            .route_layer(axum::middleware::from_fn_with_state(
                shared.clone(),
                authorize_lease,
            ));
        router = router.merge(lease);
    }
    router
        // A script body is as large as the client's statement, not a form:
        // DuckDB has no request bound and neither does this.
        .layer(DefaultBodyLimit::disable())
        .with_state(shared)
}

/// A request that may take the store's write lock, for `/health`'s gauge.
struct Busy<'a> {
    shared: &'a Shared,
}

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        self.shared.writers.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Go's `requireSQLToken`: IAM trust skips the token, an empty token disables
/// it, otherwise `Authorization: Bearer <token>` must match in constant time.
async fn authorize(State(shared): State<Arc<Shared>>, request: Request, next: Next) -> Response {
    let given = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if shared.options.authorized(given) {
        return next.run(request).await;
    }
    drain(request).await;
    fail(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "bearer token required",
    )
}

/// Reads a request body the handler will not, so the connection closes cleanly.
async fn drain(request: Request) {
    let _ = axum::body::to_bytes(request.into_body(), MAX_DRAIN_BODY).await;
}

/// Go's `requireLoopback` and `requireWriteLeaseToken`: the control surface is
/// local-only even when the rest of the API is public, and an unset token
/// refuses it instead of opening it.
async fn authorize_lease(
    State(shared): State<Arc<Shared>>,
    request: Request,
    next: Next,
) -> Response {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0);
    let given = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    match lease_authorized(&shared.options, peer, given) {
        None => next.run(request).await,
        Some(refusal) => {
            drain(request).await;
            refusal
        }
    }
}

fn lease_authorized(
    options: &QuacklabOptions,
    peer: Option<SocketAddr>,
    given: Option<&str>,
) -> Option<Response> {
    if !peer.is_some_and(is_loopback) {
        return Some(fail(
            StatusCode::FORBIDDEN,
            "loopback_only",
            "write lease control is loopback-only",
        ));
    }
    if options.iam_trust {
        return None;
    }
    if options.sql_token.trim().is_empty() {
        return Some(fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "lease_auth_unavailable",
            "write lease authentication is not configured",
        ));
    }
    if options.authorized(given) {
        None
    } else {
        Some(fail(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        ))
    }
}

fn is_loopback(peer: SocketAddr) -> bool {
    // `to_canonical` folds an IPv4-mapped IPv6 peer into IPv4 first.
    peer.ip().to_canonical().is_loopback()
}

async fn health(State(shared): State<Arc<Shared>>) -> Response {
    // Never wait on the store: a writer in flight is reported instead.
    if shared.writer_busy() {
        return json(
            StatusCode::OK,
            "{\"ok\":true,\"writerBusy\":true}".to_string(),
        );
    }
    let Some(admit) = shared.leases.try_admit() else {
        return json(
            StatusCode::OK,
            "{\"ok\":true,\"writeLeased\":true}".to_string(),
        );
    };
    match tokio::task::spawn_blocking(move || {
        let _admitted = admit;
        shared.probe()
    })
    .await
    {
        Ok(Ok(())) => json(StatusCode::OK, "{\"ok\":true}".to_string()),
        Ok(Err(message)) => fail(StatusCode::SERVICE_UNAVAILABLE, "not_ready", &message),
        Err(join) => fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "not_ready",
            &format!("the warehouse check failed: {join}"),
        ),
    }
}

async fn recover(State(shared): State<Arc<Shared>>, _body: Bytes) -> Response {
    let started = Instant::now();
    match shared.recover_force().await {
        Ok(()) => json(
            StatusCode::OK,
            format!(
                "{{\"ok\":true,\"reopenMs\":{}}}",
                started.elapsed().as_millis()
            ),
        ),
        Err(message) => fail(StatusCode::SERVICE_UNAVAILABLE, "recover_failed", &message),
    }
}

async fn run_sql(State(shared): State<Arc<Shared>>, body: Bytes) -> Response {
    let sql = match sql_from_body(&body) {
        Ok(sql) => sql,
        Err(message) => return fail(StatusCode::BAD_REQUEST, "bad_json", &message),
    };
    let sql = sql.trim();
    if sql.is_empty() {
        return fail(StatusCode::BAD_REQUEST, "empty_sql", "sql required");
    }
    let Some(admit) = shared.leases.try_admit() else {
        return fail(
            StatusCode::LOCKED,
            "warehouse_write_leased",
            "warehouse is exclusively leased for a transform run",
        );
    };
    run_statement(shared, admit, sql.to_string()).await
}

/// `POST /write-lease/sql`: the holder's own SQL, same result shape as `/sql`.
async fn run_lease_sql(State(shared): State<Arc<Shared>>, body: Bytes) -> Response {
    let fields = match lease_body(&body) {
        Ok(fields) => fields,
        Err(message) => return fail(StatusCode::BAD_REQUEST, "bad_json", &message),
    };
    let id = match string_field(&fields, "leaseId") {
        Ok(id) => id,
        Err(message) => return fail(StatusCode::BAD_REQUEST, "bad_json", &message),
    };
    let sql = match string_field(&fields, "sql") {
        Ok(sql) => sql,
        Err(message) => return fail(StatusCode::BAD_REQUEST, "bad_json", &message),
    };
    let sql = sql.trim();
    if sql.is_empty() {
        return fail(StatusCode::BAD_REQUEST, "empty_sql", "sql required");
    }
    let Some(admit) = shared.leases.try_admit_holder(id.trim()) else {
        return fail(
            StatusCode::NOT_FOUND,
            "unknown_lease",
            "no matching active write lease",
        );
    };
    run_statement(shared, admit, sql.to_string()).await
}

async fn run_statement(shared: Arc<Shared>, admit: Admit, sql: String) -> Response {
    let limit = shared.options.row_limit;
    // The engine is synchronous and a script can run for a while: it must not
    // hold an async worker.
    match tokio::task::spawn_blocking(move || {
        let _admitted = admit;
        execute(&shared, &sql, limit)
    })
    .await
    {
        Ok(Ok(body)) => json(StatusCode::OK, body),
        Ok(Err(Failure::Engine(message))) => fail(StatusCode::BAD_REQUEST, "sql_error", &message),
        Ok(Err(Failure::Unavailable(message))) => {
            fail(StatusCode::SERVICE_UNAVAILABLE, "conn_error", &message)
        }
        Ok(Err(Failure::Encode(message))) => {
            fail(StatusCode::INTERNAL_SERVER_ERROR, "encode_error", &message)
        }
        Err(join) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "encode_error",
            &format!("the SQL worker failed: {join}"),
        ),
    }
}

async fn acquire(State(shared): State<Arc<Shared>>, body: Bytes) -> Response {
    let fields = match lease_body(&body) {
        Ok(fields) => fields,
        Err(message) => return fail(StatusCode::BAD_REQUEST, "bad_json", &message),
    };
    let owner = match string_field(&fields, "owner") {
        Ok(owner) => owner,
        Err(message) => return fail(StatusCode::BAD_REQUEST, "bad_json", &message),
    };
    let ttl_seconds = match int_field(&fields, "ttlSeconds") {
        Ok(seconds) => seconds,
        Err(message) => return fail(StatusCode::BAD_REQUEST, "bad_json", &message),
    };
    let ttl = clamp_ttl(ttl_seconds, shared.options.lease_ttl_floor);
    match shared
        .leases
        .acquire(
            Arc::clone(&shared.warehouse),
            owner,
            ttl,
            shared.options.lease_drain_timeout,
        )
        .await
    {
        Ok(grant) => json(StatusCode::OK, grant_body(&grant)),
        Err(AcquireError::Held) => fail(
            StatusCode::CONFLICT,
            "warehouse_write_leased",
            "another exclusive write lease is active",
        ),
        Err(AcquireError::Failed(message)) => fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "lease_drain_failed",
            &message,
        ),
    }
}

async fn renew(State(shared): State<Arc<Shared>>, body: Bytes) -> Response {
    let fields = match lease_body(&body) {
        Ok(fields) => fields,
        Err(message) => return fail(StatusCode::BAD_REQUEST, "bad_json", &message),
    };
    let id = match string_field(&fields, "leaseId") {
        Ok(id) => id,
        Err(message) => return fail(StatusCode::BAD_REQUEST, "bad_json", &message),
    };
    let ttl_seconds = match int_field(&fields, "ttlSeconds") {
        Ok(seconds) => seconds,
        Err(message) => return fail(StatusCode::BAD_REQUEST, "bad_json", &message),
    };
    let ttl = clamp_ttl(ttl_seconds, shared.options.lease_ttl_floor);
    match shared
        .leases
        .renew(Arc::clone(&shared.warehouse), id.trim(), ttl)
    {
        Ok(grant) => json(StatusCode::OK, grant_body(&grant)),
        Err(EndError::Unknown) => fail(
            StatusCode::NOT_FOUND,
            "unknown_lease",
            "no matching active write lease",
        ),
        Err(EndError::Reopen(message)) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "lease_renew_failed",
            &format!("reopen warehouse: {message}"),
        ),
    }
}

async fn release(State(shared): State<Arc<Shared>>, body: Bytes) -> Response {
    let fields = match lease_body(&body) {
        Ok(fields) => fields,
        Err(message) => return fail(StatusCode::BAD_REQUEST, "bad_json", &message),
    };
    let id = match string_field(&fields, "leaseId") {
        Ok(id) => id,
        Err(message) => return fail(StatusCode::BAD_REQUEST, "bad_json", &message),
    };
    match shared
        .leases
        .end(id.trim(), None, "released", &shared.warehouse)
        .await
    {
        Ok(released) => json(StatusCode::OK, released_body(&released)),
        Err(EndError::Unknown) => fail(
            StatusCode::NOT_FOUND,
            "unknown_lease",
            "no matching active write lease",
        ),
        Err(EndError::Reopen(message)) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "lease_release_failed",
            &format!("reopen warehouse: {message}"),
        ),
    }
}

enum Failure {
    /// An engine error, already prefixed the way DuckDB prefixes it.
    Engine(String),
    /// A result value Go's `json.Marshal` would refuse (NaN, ±Infinity).
    Encode(String),
    /// The warehouse is closed and would not reopen: Go's `503 conn_error`.
    Unavailable(String),
}

/// One request's whole life on a fresh connection.
fn execute(shared: &Shared, sql: &str, limit: usize) -> Result<String, Failure> {
    // One parse answers both the read-only policy and "can this take the
    // store's write lock?" — the store's own classification, not a guess.
    let statements =
        pinkdb::sql::parse_statements(sql).map_err(|e| Failure::Engine(engine_message(&e)))?;
    let writes = statements
        .iter()
        .any(|statement| !pinkdb::serve::statement_allowed(statement, false));
    if shared.options.read_only && writes {
        return Err(Failure::Engine(READ_ONLY_MESSAGE.to_string()));
    }
    let setup_started = Instant::now();
    shared
        .warehouse
        .ensure_open()
        .map_err(Failure::Unavailable)?;
    let mut conn = shared.db.connect();
    conn.execute(&format!("USE {}", quoted(&shared.options.warehouse)))
        .map_err(|e| Failure::Engine(engine_message(&e)))?;
    let setup_ms = setup_started.elapsed().as_millis();

    // From here the statement may hold the store's write lock.
    let _writer = writes.then(|| shared.busy());
    let exec_started = Instant::now();
    let results = conn.execute(sql);
    // A transaction the script left open is rolled back before the connection
    // goes back to the store: its lock, and its uncommitted rows, must not
    // outlive the request. (A script that failed part-way keeps the effects of
    // the statements before the failure, as in DuckDB.)
    let _ = conn.execute("ROLLBACK");
    let exec_ms = exec_started.elapsed().as_millis();
    let results = results.map_err(|e| Failure::Engine(engine_message(&e)))?;

    let encode_started = Instant::now();
    let payload = match results.last() {
        Some(result) => Payload::of(result, limit).map_err(|e| Failure::Encode(e.0))?,
        // A script of only `;` has no statement to answer for; DuckDB's empty
        // statement list is a Success.
        None => Payload::success(),
    };
    let serialization_ms = encode_started.elapsed().as_millis();
    Ok(payload.body(Timings {
        setup_ms,
        exec_ms,
        serialization_ms,
    }))
}

/// Go's `decodeLeaseBody`: no body means defaults; anything else must be a
/// JSON object, read through the 4 KiB cap.
fn lease_body(body: &[u8]) -> Result<Map<String, serde_json::Value>, String> {
    if body.is_empty() {
        return Ok(Map::new());
    }
    let capped = &body[..body.len().min(MAX_LEASE_BODY)];
    let value: serde_json::Value =
        serde_json::from_slice(capped).map_err(|error| error.to_string())?;
    match value {
        serde_json::Value::Null => Ok(Map::new()),
        serde_json::Value::Object(fields) => Ok(fields),
        other => Err(format!(
            "json: cannot unmarshal {} into Go value of type struct {{ owner string; ttlSeconds int }}",
            go_kind(&other)
        )),
    }
}

fn string_field(fields: &Map<String, serde_json::Value>, name: &str) -> Result<String, String> {
    match fields.get(name) {
        None | Some(serde_json::Value::Null) => Ok(String::new()),
        Some(serde_json::Value::String(value)) => Ok(value.clone()),
        Some(other) => Err(format!(
            "json: cannot unmarshal {} into Go struct field .{name} of type string",
            go_kind(other)
        )),
    }
}

fn int_field(fields: &Map<String, serde_json::Value>, name: &str) -> Result<i64, String> {
    match fields.get(name) {
        None | Some(serde_json::Value::Null) => Ok(0),
        Some(serde_json::Value::Number(value)) => value.as_i64().ok_or_else(|| {
            format!(
                "json: cannot unmarshal number {value} into Go struct field .{name} of type int"
            )
        }),
        Some(other) => Err(format!(
            "json: cannot unmarshal {} into Go struct field .{name} of type int",
            go_kind(other)
        )),
    }
}

fn grant_body(grant: &Grant) -> String {
    let mut out = String::from("{\"leaseId\":");
    push_json_string(&mut out, &grant.id);
    if !grant.owner.is_empty() {
        out.push_str(",\"owner\":");
        push_json_string(&mut out, &grant.owner);
    }
    out.push_str(&format!(
        ",\"ttlSeconds\":{},\"expiresAt\":\"{}\",\"drainMs\":{},\"acquireMs\":{}}}",
        grant.ttl_seconds,
        rfc3339(grant.expires_at),
        grant.drain_ms,
        grant.acquire_ms
    ));
    out
}

fn released_body(released: &Released) -> String {
    format!(
        "{{\"ok\":true,\"heldMs\":{},\"reopenMs\":{}}}",
        released.held_ms, released.reopen_ms
    )
}

/// Go's `json.NewDecoder(body).Decode(&struct{ SQL string }{})`: an empty body
/// is `io.EOF` ("EOF"), a malformed one is the decoder's message, and a
/// missing `sql` decodes to the empty string (which the caller then rejects).
fn sql_from_body(body: &[u8]) -> Result<String, String> {
    if body.iter().all(|byte| byte.is_ascii_whitespace()) {
        return Err("EOF".to_string());
    }
    let value: serde_json::Value = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    match value {
        serde_json::Value::Null => Ok(String::new()),
        serde_json::Value::Object(fields) => match fields.get("sql") {
            None | Some(serde_json::Value::Null) => Ok(String::new()),
            Some(serde_json::Value::String(sql)) => Ok(sql.clone()),
            Some(other) => Err(format!(
                "json: cannot unmarshal {} into Go struct field .sql of type string",
                go_kind(other)
            )),
        },
        other => Err(format!(
            "json: cannot unmarshal {} into Go value of type struct {{ SQL string }}",
            go_kind(&other)
        )),
    }
}

fn go_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// A double-quoted SQL identifier, with any embedded quote doubled.
pub(crate) fn quoted(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// One response's payload: the last statement's result in DuckDB's shapes.
struct Payload {
    columns: Vec<String>,
    /// Rendered JSON arrays, already capped at the row limit.
    rows: Vec<String>,
    row_count: usize,
    truncated: bool,
    rows_affected: i64,
}

impl Payload {
    /// DuckDB's `Success`: a statement that changed no rows reports it as the
    /// only column, with no row at all (`SET`, `BEGIN`, `COMMIT`, `DROP`).
    fn success() -> Self {
        Self {
            columns: vec!["Success".to_string()],
            rows: Vec::new(),
            row_count: 0,
            truncated: false,
            rows_affected: 0,
        }
    }

    /// A `Count` column with the count in it: INSERT, UPDATE, DELETE, MERGE.
    fn count(count: usize) -> Self {
        Self {
            columns: vec!["Count".to_string()],
            rows: vec![format!("[{count}]")],
            row_count: 1,
            truncated: false,
            rows_affected: count as i64,
        }
    }

    /// A `Count` column without a row: CREATE TABLE, CREATE SCHEMA, CREATE VIEW.
    fn created() -> Self {
        Self {
            columns: vec!["Count".to_string()],
            ..Self::success()
        }
    }

    fn of(result: &QueryResult, limit: usize) -> Result<Self, EncodeError> {
        match result {
            QueryResult::Selected { names, types, rows } => {
                let mut rendered = Vec::with_capacity(rows.len().min(limit));
                for row in rows.iter().take(limit) {
                    let mut out = String::new();
                    write_row(&mut out, row, types)?;
                    rendered.push(out);
                }
                Ok(Self {
                    columns: names.clone(),
                    rows: rendered,
                    // rowCount is the result's exact size, not the capped one.
                    row_count: rows.len(),
                    truncated: rows.len() > limit,
                    rows_affected: rows_affected(names, rows),
                })
            }
            QueryResult::Inserted { count }
            | QueryResult::Updated { count }
            | QueryResult::Deleted { count } => Ok(Self::count(*count)),
            QueryResult::Created
            | QueryResult::CreatedView { .. }
            | QueryResult::CreatedIndex { .. }
            | QueryResult::CreatedMaterializedView { .. }
            | QueryResult::CreatedDatabase { .. }
            | QueryResult::CreatedSchema { .. }
            | QueryResult::CreatedProcedure { .. }
            | QueryResult::CreatedTrigger { .. }
            | QueryResult::CreatedMacro { .. } => Ok(Self::created()),
            // `CALL`: the procedure's own result, or Success when it returned
            // none.
            QueryResult::Called {
                result: Some(inner),
                ..
            } => Self::of(inner, limit),
            QueryResult::Called { result: None, .. } => Ok(Self::success()),
            _ => Ok(Self::success()),
        }
    }

    fn body(&self, timings: Timings) -> String {
        let Timings {
            setup_ms,
            exec_ms,
            serialization_ms,
        } = timings;
        let mut out =
            String::with_capacity(160 + self.rows.iter().map(|row| row.len() + 1).sum::<usize>());
        out.push_str("{\"columns\":[");
        for (index, name) in self.columns.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            push_json_string(&mut out, name);
        }
        out.push_str("],\"rows\":[");
        for (index, row) in self.rows.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(row);
        }
        out.push_str(&format!(
            "],\"rowCount\":{},\"truncated\":{},\"rowsAffected\":{},\
             \"waitMs\":0,\"gateWaitMs\":0,\"poolWaitMs\":0,\"setupMs\":{setup_ms},\
             \"execMs\":{exec_ms},\"rowScanMs\":0,\"tookMs\":{exec_ms},\
             \"serializationMs\":{serialization_ms}}}",
            self.row_count, self.truncated, self.rows_affected,
        ));
        out
    }
}

/// The phases pinkdb has. `rowScanMs` is always 0: pinkdb hands the rows over
/// materialized, so there is no separate scan; `tookMs` is Go's
/// `execMs + rowScanMs`.
struct Timings {
    setup_ms: u128,
    exec_ms: u128,
    serialization_ms: u128,
}

/// Go's `rowsAffectedFromResult`: a lone `Count` column's one numeric value.
fn rows_affected(names: &[String], rows: &[Vec<Value>]) -> i64 {
    if names.len() != 1 || !names[0].eq_ignore_ascii_case("Count") {
        return 0;
    }
    match rows.first().and_then(|row| row.first()) {
        Some(Value::Integer(count)) => i64::from(*count),
        Some(Value::Bigint(count)) => *count,
        Some(Value::Double(count)) if count.fract() == 0.0 => *count as i64,
        _ => 0,
    }
}

fn json(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

fn fail(status: StatusCode, code: &str, message: &str) -> Response {
    json(status, error_body(code, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_body_decodes_like_go() {
        assert_eq!(
            sql_from_body(b"{\"sql\":\"SELECT 1\"}").unwrap(),
            "SELECT 1"
        );
        assert_eq!(sql_from_body(b"{}").unwrap(), "");
        assert_eq!(sql_from_body(b"null").unwrap(), "");
        assert_eq!(sql_from_body(b"").unwrap_err(), "EOF");
        assert_eq!(sql_from_body(b"  \n").unwrap_err(), "EOF");
        assert!(
            sql_from_body(b"{\"sql\":1}")
                .unwrap_err()
                .contains("cannot unmarshal number")
        );
        assert!(
            sql_from_body(b"[1]")
                .unwrap_err()
                .contains("into Go value of type struct")
        );
    }

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quoted("warehouse"), "\"warehouse\"");
        assert_eq!(quoted("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn an_empty_result_is_a_success_column() {
        assert_eq!(Payload::success().columns, ["Success".to_string()]);
        assert_eq!(Payload::created().columns, ["Count".to_string()]);
        assert!(Payload::created().rows.is_empty());
    }

    #[test]
    fn a_lone_count_column_carries_rows_affected() {
        let names = vec!["Count".to_string()];
        assert_eq!(rows_affected(&names, &[vec![Value::Bigint(2)]]), 2);
        assert_eq!(rows_affected(&names, &[]), 0);
        let other = vec!["count_star()".to_string()];
        assert_eq!(rows_affected(&other, &[vec![Value::Bigint(2)]]), 0);
    }

    #[test]
    fn lease_bodies_decode_like_go() {
        assert!(lease_body(b"").unwrap().is_empty());
        assert!(lease_body(b"null").unwrap().is_empty());
        let body = lease_body(b"{\"owner\":\"dbt\",\"ttlSeconds\":120}").unwrap();
        assert_eq!(string_field(&body, "owner").unwrap(), "dbt");
        assert_eq!(int_field(&body, "ttlSeconds").unwrap(), 120);
        assert_eq!(string_field(&Map::new(), "owner").unwrap(), "");
        assert_eq!(int_field(&Map::new(), "ttlSeconds").unwrap(), 0);
        assert!(lease_body(b"[]").unwrap_err().contains("Go value"));
        assert!(
            string_field(&lease_body(b"{\"owner\":1}").unwrap(), "owner")
                .unwrap_err()
                .contains("cannot unmarshal number")
        );
        assert!(
            int_field(&lease_body(b"{\"ttlSeconds\":1.5}").unwrap(), "ttlSeconds")
                .unwrap_err()
                .contains("cannot unmarshal number 1.5")
        );
    }

    #[test]
    fn only_local_peers_may_hold_a_lease() {
        assert!(is_loopback("127.0.0.1:5000".parse().unwrap()));
        assert!(is_loopback("[::1]:5000".parse().unwrap()));
        assert!(!is_loopback("192.0.2.1:5000".parse().unwrap()));
        assert!(!is_loopback("0.0.0.0:5000".parse().unwrap()));
    }

    #[test]
    fn lease_auth_needs_loopback_and_a_configured_token() {
        let mut options = QuacklabOptions::default();
        let local: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let remote: SocketAddr = "192.0.2.1:1".parse().unwrap();
        // Loopback stays required even when `/sql` would be open.
        assert_eq!(
            lease_authorized(&options, Some(remote), None)
                .expect("a refusal")
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            lease_authorized(&options, None, None)
                .expect("a refusal")
                .status(),
            StatusCode::FORBIDDEN
        );
        // An unset token refuses instead of opening.
        assert_eq!(
            lease_authorized(&options, Some(local), None)
                .expect("a refusal")
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        options.sql_token = "s3cret".to_string();
        assert_eq!(
            lease_authorized(&options, Some(local), None)
                .expect("a refusal")
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert!(lease_authorized(&options, Some(local), Some("Bearer s3cret")).is_none());
        options.sql_token.clear();
        options.iam_trust = true;
        assert!(lease_authorized(&options, Some(local), None).is_none());
        assert!(lease_authorized(&options, Some(remote), None).is_some());
    }

    #[test]
    fn a_grant_omits_an_empty_owner() {
        let grant = Grant {
            id: "ab".repeat(16),
            owner: String::new(),
            ttl_seconds: 60,
            expires_at: std::time::UNIX_EPOCH,
            drain_ms: 1,
            acquire_ms: 2,
        };
        assert_eq!(
            grant_body(&grant),
            format!(
                "{{\"leaseId\":\"{}\",\"ttlSeconds\":60,\"expiresAt\":\"1970-01-01T00:00:00Z\",\"drainMs\":1,\"acquireMs\":2}}",
                grant.id
            )
        );
        let named = Grant {
            owner: "dbt-run".to_string(),
            ..grant
        };
        assert!(grant_body(&named).contains("\"owner\":\"dbt-run\""));
    }
}
