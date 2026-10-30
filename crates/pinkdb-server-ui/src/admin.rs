//! The Access and Storage panels (wave 121b).
//!
//! The access catalog comes from the engine's `pinkdb_users()` and
//! `pinkdb_grants()`, read **as the session user**: an administrator sees
//! every principal and grant, anyone else only themselves and their roles.
//! Every change — create a user, grant, revoke — is a statement run as the
//! session user, so RBAC decides; the console adds no privilege of its own.
//! Creating a user (or setting a password) has its own endpoint so the
//! password is never a worksheet statement and never reaches the history.

// A handler's `Err` is the HTTP response axum sends as is.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use pinkdb::{Connection, ExecLimits, QueryResult, Value};
use pinkdb_server_ui_api::{Access, GrantRow, Principal, Storage, StorageFact, UserRequest};
use serde_json::json;

use crate::api::json_error;
use crate::server::Shared;
use crate::sessions::{Identity, UiSession};
use crate::store::quote_ident;
use crate::work::{get_session, json_ok, post_session, read_json};

/// Short limits for the catalog reads and the user statements.
fn limits(shared: &Shared) -> ExecLimits {
    ExecLimits {
        statement_timeout: Some(shared.opts.statement_timeout),
        max_result_rows: Some(100_000),
        max_result_bytes: Some(64 * 1024 * 1024),
    }
}

fn rows(conn: &mut Connection, sql: &str) -> Result<Vec<Vec<Value>>, String> {
    crate::store::run_engine_hook(sql);
    match conn.execute(sql).map_err(|e| e.to_string())?.pop() {
        Some(QueryResult::Selected { rows, .. }) => Ok(rows),
        _ => Ok(Vec::new()),
    }
}

fn text(row: &[Value], i: usize) -> Option<String> {
    match row.get(i) {
        None | Some(Value::Null) => None,
        Some(Value::Varchar(s)) => Some(s.clone()),
        Some(other) => Some(other.to_string()),
    }
}

/// The access catalog as `session` may see it.
fn read_access(shared: &Shared, session: &UiSession) -> Result<Access, String> {
    let mut conn = session.connect(&shared.db)?;
    conn.set_exec_limits(limits(shared));
    let principals: Vec<Principal> = rows(
        &mut conn,
        "SELECT \"name\", \"kind\", \"is_admin\", \"roles\" FROM pinkdb_users() ORDER BY 2 DESC, 1",
    )?
    .into_iter()
    .map(|r| Principal {
        name: text(&r, 0).unwrap_or_default(),
        kind: text(&r, 1).unwrap_or_default(),
        is_admin: matches!(r.get(2), Some(Value::Boolean(true))),
        roles: text(&r, 3).unwrap_or_default(),
    })
    .collect();
    let grants: Vec<GrantRow> = rows(
        &mut conn,
        "SELECT \"principal\", \"privilege\", \"object_type\", \"database\", \"name\" \
         FROM pinkdb_grants() ORDER BY 1, 3, 4, 5, 2",
    )?
    .into_iter()
    .map(|r| GrantRow {
        principal: text(&r, 0).unwrap_or_default(),
        privilege: text(&r, 1).unwrap_or_default(),
        object_type: text(&r, 2).unwrap_or_default(),
        database: text(&r, 3),
        name: text(&r, 4),
    })
    .collect();
    let admin = match &session.identity {
        Identity::Operator => true,
        Identity::User(me) => principals
            .iter()
            .any(|p| p.kind == "USER" && p.name.eq_ignore_ascii_case(me) && p.is_admin),
    };
    Ok(Access {
        me: session.user.clone(),
        admin,
        principals,
        grants,
    })
}

/// Is the session an administrator (or the trusted operator session)?
pub(crate) fn session_is_admin(shared: &Shared, session: &UiSession) -> bool {
    match &session.identity {
        Identity::Operator => true,
        Identity::User(_) => read_access(shared, session).is_ok_and(|a| a.admin),
    }
}

/// `GET /api/access`
pub async fn access(State(shared): State<Arc<Shared>>, headers: HeaderMap) -> Response {
    let session = match get_session(&shared, &headers) {
        Ok(session) => session,
        Err(response) => return response,
    };
    let worker = Arc::clone(&shared);
    match tokio::task::spawn_blocking(move || read_access(&worker, &session)).await {
        Ok(Ok(access)) => json_ok(&access),
        Ok(Err(message)) => json_error(StatusCode::INTERNAL_SERVER_ERROR, message),
        Err(err) => json_error(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

/// A user name the statement can carry: no quote games, no empty name.
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
}

/// `POST /api/access/user`: create a user, or set a user's password, as the
/// session user (the engine requires `ADMIN` for either, except a user
/// changing their own password where the engine allows it).
pub async fn user(State(shared): State<Arc<Shared>>, headers: HeaderMap, body: Body) -> Response {
    let session = match post_session(&shared, &headers) {
        Ok(session) => session,
        Err(response) => return response,
    };
    let body: UserRequest = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    if !valid_name(&body.name) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "a user name is letters, digits, `_`, `-` and `.`",
        );
    }
    if body.password.is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "the password is empty");
    }
    if shared.opts.read_only {
        return json_error(
            StatusCode::FORBIDDEN,
            "the console is read-only (--ui-read-only)",
        );
    }
    let worker = Arc::clone(&shared);
    let outcome = tokio::task::spawn_blocking(move || -> Result<String, String> {
        let mut conn = session.connect(&worker.db)?;
        conn.set_exec_limits(limits(&worker));
        let verb = if body.alter { "ALTER" } else { "CREATE" };
        // The password is a string literal of this one statement: quoted,
        // never logged by the console, never in the run history.
        let sql = format!(
            "{verb} USER {} PASSWORD '{}'",
            quote_ident(&body.name),
            body.password.replace('\'', "''")
        );
        conn.execute(&sql).map_err(|e| e.to_string())?;
        Ok(if body.alter {
            format!("Changed the password of {}", body.name)
        } else {
            format!("Created user {}", body.name)
        })
    })
    .await;
    match outcome {
        Ok(Ok(message)) => json_ok(&json!({ "message": message })),
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

/// `GET /api/storage`: the Storage panel.
pub async fn storage(State(shared): State<Arc<Shared>>, headers: HeaderMap) -> Response {
    if let Err(response) = get_session(&shared, &headers) {
        return response;
    }
    let info = &shared.opts.storage;
    let mut facts: Vec<StorageFact> = info
        .facts
        .iter()
        .map(|(label, value)| StorageFact {
            label: label.clone(),
            value: value.clone(),
        })
        .collect();
    if let Some(live) = &info.live {
        let live = Arc::clone(live);
        if let Ok(more) = tokio::task::spawn_blocking(move || live()).await {
            facts.extend(
                more.into_iter()
                    .map(|(label, value)| StorageFact { label, value }),
            );
        }
    }
    facts.push(StorageFact {
        label: "durable".to_string(),
        value: if shared.db.is_durable() {
            "yes (WAL)"
        } else {
            "no"
        }
        .to_string(),
    });
    json_ok(&Storage {
        mode: info.mode.clone(),
        location: info.location.clone(),
        facts,
        import_formats: info.import_formats.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_names_are_plain() {
        assert!(valid_name("alice"));
        assert!(valid_name("svc.loader-1"));
        assert!(!valid_name(""));
        assert!(!valid_name("a b"));
        assert!(!valid_name("x\"; DROP"));
    }

    #[test]
    fn identifiers_are_quoted_in_user_statements() {
        assert_eq!(quote_ident("ali\"ce"), "\"ali\"\"ce\"");
    }
}
