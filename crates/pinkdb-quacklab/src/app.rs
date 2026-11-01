//! The HTTP surface: `POST /sql`, `POST /recover`, `GET /health` and
//! `/healthz`.
//!
//! Every request that touches the warehouse opens its own [`Connection`],
//! positions it on the warehouse database (`USE "<alias>"`) and, when the
//! script left a transaction open, rolls it back: the store never sees a
//! half-open transaction from a request that has already answered.

use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use pinkdb::Database;
use pinkdb::QueryResult;
use pinkdb::types::Value;

use crate::QuacklabOptions;
use crate::encode::{EncodeError, push_json_string, write_row};
use crate::errors::{engine_message, error_body};

/// The engine handle and options every request shares.
pub(crate) struct Shared {
    pub(crate) db: Database,
    pub(crate) options: QuacklabOptions,
}

impl Shared {
    pub(crate) fn new(db: Database, options: QuacklabOptions) -> Self {
        Self { db, options }
    }

    /// `SELECT 1` in the warehouse: what `/health` and `/recover` answer with.
    pub(crate) fn probe(&self) -> Result<(), String> {
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
    routes(Arc::new(Shared::new(db, options)))
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
    Router::new()
        .route("/health", get(health))
        .route("/healthz", get(health))
        .merge(guarded)
        // A script body is as large as the client's statement, not a form:
        // DuckDB has no request bound and neither does this.
        .layer(DefaultBodyLimit::disable())
        .with_state(shared)
}

/// Go's `requireSQLToken`: IAM trust skips the token, an empty token disables
/// it, otherwise `Authorization: Bearer <token>` must match in constant time.
async fn authorize(State(shared): State<Arc<Shared>>, request: Request, next: Next) -> Response {
    let given = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if shared.options.authorized(given) {
        next.run(request).await
    } else {
        fail(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        )
    }
}

async fn health(State(shared): State<Arc<Shared>>) -> Response {
    // 138b adds `writerBusy`/`writeLeased` to this answer: the lease state is
    // part of the same probe, so the shape is `{"ok":true, …}` here too.
    match tokio::task::spawn_blocking(move || shared.probe()).await {
        Ok(Ok(())) => json(StatusCode::OK, "{\"ok\":true}".to_string()),
        Ok(Err(message)) => fail(StatusCode::SERVICE_UNAVAILABLE, "not_ready", &message),
        Err(join) => fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "not_ready",
            &format!("the warehouse check failed: {join}"),
        ),
    }
}

async fn recover(State(shared): State<Arc<Shared>>) -> Response {
    // A local warehouse has nothing to re-open: the check itself is the
    // recovery. 138c makes this re-attach the DuckLake catalog.
    let started = Instant::now();
    match tokio::task::spawn_blocking(move || shared.probe()).await {
        Ok(Ok(())) => json(
            StatusCode::OK,
            format!(
                "{{\"ok\":true,\"reopenMs\":{}}}",
                started.elapsed().as_millis()
            ),
        ),
        Ok(Err(message)) => fail(StatusCode::SERVICE_UNAVAILABLE, "recover_failed", &message),
        Err(join) => fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "recover_failed",
            &format!("the warehouse check failed: {join}"),
        ),
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
    let sql = sql.to_string();
    let limit = shared.options.row_limit;
    // The engine is synchronous and a script can run for a while: it must not
    // hold an async worker.
    match tokio::task::spawn_blocking(move || execute(&shared, &sql, limit)).await {
        Ok(Ok(body)) => json(StatusCode::OK, body),
        Ok(Err(Failure::Engine(message))) => fail(StatusCode::BAD_REQUEST, "sql_error", &message),
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

enum Failure {
    /// An engine error, already prefixed the way DuckDB prefixes it.
    Engine(String),
    /// A result value Go's `json.Marshal` would refuse (NaN, ±Infinity).
    Encode(String),
}

/// One request's whole life on a fresh connection.
fn execute(shared: &Shared, sql: &str, limit: usize) -> Result<String, Failure> {
    let mut conn = shared.db.connect();

    let setup_started = Instant::now();
    if shared.options.read_only {
        check_read_only(sql).map_err(Failure::Engine)?;
    }
    conn.execute(&format!("USE {}", quoted(&shared.options.warehouse)))
        .map_err(|e| Failure::Engine(engine_message(&e)))?;
    let setup_ms = setup_started.elapsed().as_millis();

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

/// The read-only warehouse (`DUCKDB_READ_ONLY`): the console's policy, so the
/// two surfaces refuse the same statements.
fn check_read_only(sql: &str) -> Result<(), String> {
    let statements = pinkdb::sql::parse_statements(sql).map_err(|e| engine_message(&e))?;
    if statements
        .iter()
        .any(|statement| !pinkdb::serve::statement_allowed(statement, false))
    {
        return Err(
            "Invalid Input Error: this warehouse is read-only (DUCKDB_READ_ONLY); \
                    only queries, EXPLAIN, DESCRIBE, SHOW and USE run here"
                .to_string(),
        );
    }
    Ok(())
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
}
