//! File uploads (wave 121b): a CSV or Parquet file from the browser becomes
//! a table.
//!
//! The server parses the bytes itself — CSV with the engine's own parser,
//! Parquet through a scratch in-memory store and whatever Parquet reader the
//! process registered — and then writes the table **as the session user**, in
//! one transaction, with every value bound as a parameter:
//!
//! - a new (or replaced) table is one `CREATE TABLE … AS SELECT … FROM
//!   (VALUES …)`, so it needs exactly what a `CREATE TABLE AS` needs — the
//!   `CREATE` privilege on the database (in pinkdb's RBAC creating a table
//!   grants nothing on it, so a create-then-insert would need an `INSERT`
//!   grant that cannot exist yet);
//! - an append is batched `INSERT`s, so it needs `INSERT` on the table;
//! - an upload reads no server file on the user's behalf, so it does not
//!   need the `ADMIN` that file and URL reads require;
//! - a durable store logs rows, not a reference to a temporary file, so its
//!   WAL replays without the upload;
//! - a failure part-way leaves nothing (the transaction rolls back).

// A handler's `Err` is the HTTP response axum sends as is.
#![allow(clippy::result_large_err)]

use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use pinkdb::{Connection, DataType, PinkDb, QueryResult, Value};
use pinkdb_server_ui_api::{ResultColumn, UploadResult};
use serde::Deserialize;

use crate::api::json_error;
use crate::server::Shared;
use crate::store::quote_ident;
use crate::work::{json_ok, post_session};

/// Bound values per `INSERT` statement (rows × columns).
const VALUES_PER_STATEMENT: usize = 2_000;

#[derive(Deserialize)]
pub(crate) struct UploadQuery {
    /// The target database (the session's database when absent).
    db: Option<String>,
    /// The target table.
    table: String,
    /// `csv` or `parquet`; inferred from `name` when absent.
    format: Option<String>,
    /// The uploaded file's name (for the format).
    name: Option<String>,
    /// `create` (the default), `replace` or `append`.
    mode: Option<String>,
}

/// What an upload became, before it is written.
struct Parsed {
    names: Vec<String>,
    types: Vec<DataType>,
    rows: Vec<Vec<Value>>,
}

/// The format of an upload.
fn format_of(query: &UploadQuery) -> Result<&'static str, String> {
    let named = query
        .format
        .as_deref()
        .or_else(|| query.name.as_deref().and_then(|n| n.rsplit('.').next()))
        .unwrap_or_default()
        .to_ascii_lowercase();
    match named.as_str() {
        "csv" | "tsv" | "txt" => Ok("csv"),
        "parquet" | "pq" => Ok("parquet"),
        "" => Err("name the format (csv or parquet)".to_string()),
        other => Err(format!(
            "cannot import {other:?} files: the console reads csv and parquet"
        )),
    }
}

/// Parse an upload in a scratch store that is not the served one.
fn parse(format: &str, bytes: &[u8]) -> Result<Parsed, String> {
    let mut scratch = PinkDb::new();
    let result = match format {
        "csv" => {
            let text = std::str::from_utf8(bytes)
                .map_err(|_| "the CSV file is not UTF-8 text".to_string())?;
            pinkdb::csv::import_csv_string(&mut scratch, "upload", text)
                .map_err(|e| format!("cannot read the CSV file: {e}"))?;
            scratch.execute("SELECT * FROM upload")
        }
        _ => {
            // The Parquet reader reads files: a private temporary one, removed
            // whatever happens.
            let path = std::env::temp_dir().join(format!(
                "pinkdb-upload-{}.parquet",
                &crate::security::random_token()[..16]
            ));
            write_private(&path, bytes).map_err(|e| format!("cannot stage the upload: {e}"))?;
            let sql = format!(
                "SELECT * FROM read_parquet('{}')",
                path.to_string_lossy().replace('\'', "''")
            );
            let read = scratch.execute(&sql);
            let _ = std::fs::remove_file(&path);
            read.map_err(|e| {
                let message = e.to_string();
                if message.contains("unknown table function") {
                    pinkdb::PinkDbError::Execution(
                        "this server was built without Parquet support (the `parquet` feature)"
                            .to_string(),
                    )
                } else {
                    e
                }
            })
        }
    };
    match result.map_err(|e| e.to_string())?.pop() {
        Some(QueryResult::Selected { names, types, rows }) => Ok(Parsed { names, types, rows }),
        _ => Err("the file holds no rows".to_string()),
    }
}

/// Write `bytes` to a new file only the server's user can read.
fn write_private(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)
}

/// Write the rows as the connection's user (the caller holds the
/// transaction): a new table is one `CREATE TABLE … AS SELECT` over bound
/// `VALUES`, an append batched `INSERT`s.
fn write_table(
    conn: &mut Connection,
    target: &str,
    mode: &str,
    parsed: &Parsed,
) -> Result<(), String> {
    let run = |conn: &mut Connection, sql: &str| -> Result<(), String> {
        crate::store::run_engine_hook(sql);
        conn.execute(sql).map(|_| ()).map_err(|e| e.to_string())
    };
    let width = parsed.names.len();
    if width == 0 {
        return Err("the file has no columns".to_string());
    }
    // Every placeholder carries its column's type: a NULL binds without a
    // type of its own.
    let typed: Vec<String> = parsed
        .types
        .iter()
        .map(|dtype| format!("CAST(? AS {dtype})"))
        .collect();
    let placeholders = format!("({})", typed.join(", "));
    if mode == "replace" {
        run(conn, &format!("DROP TABLE IF EXISTS {target}"))?;
    }
    if mode != "append" {
        if parsed.rows.is_empty() {
            let columns: Vec<String> = parsed
                .names
                .iter()
                .zip(&parsed.types)
                .map(|(name, dtype)| format!("{} {dtype}", quote_ident(name)))
                .collect();
            return run(
                conn,
                &format!("CREATE TABLE {target} ({})", columns.join(", ")),
            );
        }
        // The VALUES columns are positional (`c1`, `c2`, …), typed by their
        // placeholders; the table takes the file's column names.
        let select: Vec<String> = parsed
            .names
            .iter()
            .zip(&parsed.types)
            .enumerate()
            .map(|(i, (name, _))| format!("c{} AS {}", i + 1, quote_ident(name)))
            .collect();
        let aliases: Vec<String> = (1..=width).map(|i| format!("c{i}")).collect();
        let sql = format!(
            "CREATE TABLE {target} AS SELECT {} FROM (VALUES {}) AS v({})",
            select.join(", "),
            vec![placeholders.as_str(); parsed.rows.len()].join(", "),
            aliases.join(", ")
        );
        crate::store::run_engine_hook(&sql);
        let statement = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let params: Vec<Value> = parsed.rows.iter().flatten().cloned().collect();
        return conn
            .execute_prepared(&statement, &params)
            .map(|_| ())
            .map_err(|e| e.to_string());
    }
    let per_statement = (VALUES_PER_STATEMENT / width).clamp(1, 500);
    let mut prepared: Option<(usize, pinkdb::PreparedStatement)> = None;
    for batch in parsed.rows.chunks(per_statement) {
        if prepared.as_ref().is_none_or(|(len, _)| *len != batch.len()) {
            let sql = format!(
                "INSERT INTO {target} VALUES {}",
                vec![placeholders.as_str(); batch.len()].join(", ")
            );
            prepared = Some((batch.len(), conn.prepare(&sql).map_err(|e| e.to_string())?));
        }
        let (_, statement) = prepared.as_ref().expect("prepared above");
        let params: Vec<Value> = batch.iter().flatten().cloned().collect();
        conn.execute_prepared(statement, &params)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// `POST /api/upload?table=&db=&format=&name=&mode=` with the file as the
/// body.
pub async fn upload(
    State(shared): State<Arc<Shared>>,
    Query(query): Query<UploadQuery>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let session = match post_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    if shared.opts.read_only {
        return json_error(
            StatusCode::FORBIDDEN,
            "the console is read-only (--ui-read-only)",
        );
    }
    let format = match format_of(&query) {
        Ok(format) => format,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };
    let mode = query
        .mode
        .as_deref()
        .unwrap_or("create")
        .to_ascii_lowercase();
    if !matches!(mode.as_str(), "create" | "replace" | "append") {
        return json_error(StatusCode::BAD_REQUEST, "mode is create, replace or append");
    }
    let table = query.table.trim().to_string();
    if table.is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "name the table to import into");
    }
    let limit = shared.opts.max_upload_bytes;
    let bytes = match tokio::time::timeout(
        shared.opts.long_request_timeout,
        axum::body::to_bytes(body, limit),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => {
            return json_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("the file is over the console's upload limit ({limit} bytes)"),
            );
        }
        Err(_) => {
            return json_error(StatusCode::REQUEST_TIMEOUT, "the upload took too long");
        }
    };
    let worker = Arc::clone(&shared);
    let outcome = tokio::task::spawn_blocking(move || -> Result<UploadResult, String> {
        let started = Instant::now();
        let parsed = parse(format, &bytes)?;
        let mut conn = session.connect(&worker.db)?;
        conn.set_exec_limits(pinkdb::ExecLimits {
            statement_timeout: Some(worker.opts.query_timeout),
            max_result_rows: None,
            max_result_bytes: Some(worker.opts.query_max_bytes),
        });
        let database = query
            .db
            .clone()
            .filter(|d| !d.is_empty())
            .unwrap_or_else(|| conn.current_db().to_string());
        let target = format!("{}.{}", quote_ident(&database), quote_ident(&table));
        conn.execute("BEGIN").map_err(|e| e.to_string())?;
        match write_table(&mut conn, &target, &mode, &parsed) {
            Ok(()) => {
                conn.execute("COMMIT").map_err(|e| e.to_string())?;
            }
            Err(message) => {
                let _ = conn.execute("ROLLBACK");
                return Err(message);
            }
        }
        Ok(UploadResult {
            database,
            table,
            rows: parsed.rows.len() as u64,
            columns: parsed
                .names
                .iter()
                .zip(&parsed.types)
                .map(|(name, dtype)| ResultColumn {
                    name: name.clone(),
                    dtype: dtype.to_string(),
                })
                .collect(),
            elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        })
    })
    .await;
    match outcome {
        Ok(Ok(result)) => json_ok(&result),
        Ok(Err(message)) => {
            let lower = message.to_ascii_lowercase();
            let status = if lower.contains("permission") || lower.contains("privilege") {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::BAD_REQUEST
            };
            json_error(status, message)
        }
        Err(err) => json_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(format: Option<&str>, name: Option<&str>) -> UploadQuery {
        UploadQuery {
            db: None,
            table: "t".into(),
            format: format.map(str::to_string),
            name: name.map(str::to_string),
            mode: None,
        }
    }

    #[test]
    fn the_format_comes_from_the_name_or_the_parameter() {
        assert_eq!(format_of(&query(None, Some("orders.CSV"))), Ok("csv"));
        assert_eq!(format_of(&query(None, Some("x.parquet"))), Ok("parquet"));
        assert_eq!(
            format_of(&query(Some("parquet"), Some("x.csv"))),
            Ok("parquet")
        );
        assert!(format_of(&query(None, Some("x.xlsx"))).is_err());
        assert!(format_of(&query(None, None)).is_err());
    }

    #[test]
    fn a_csv_upload_is_parsed_with_types() {
        let parsed = parse("csv", b"id,name,price\n1,a,1.5\n2,\"b, c\",2\n").unwrap();
        assert_eq!(parsed.names, vec!["id", "name", "price"]);
        assert_eq!(parsed.types[0], DataType::Integer);
        assert_eq!(parsed.rows.len(), 2);
        assert_eq!(parsed.rows[1][1], Value::Varchar("b, c".into()));
        assert!(parse("csv", &[0xff, 0xfe]).is_err());
    }
}
