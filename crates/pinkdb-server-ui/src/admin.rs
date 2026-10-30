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
        two_factor: false,
        sessions: 0,
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
    // An administrator also sees who signs in with a second factor and how
    // many sessions each user has (wave 121d).
    let mut principals = principals;
    if admin {
        crate::account::sweep(shared);
        let totp = shared.vault.totp_users().unwrap_or_default();
        for p in principals.iter_mut().filter(|p| p.kind == "USER") {
            let name = p.name.to_ascii_lowercase();
            p.two_factor = totp.contains(&name);
            p.sessions = shared.sessions.of_user(&name).len();
        }
    }
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
    let session = match get_session(&shared, &headers).await {
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

/// `POST /api/access/user`: create a user, or set a user's password, as the
/// session user (the engine requires `ADMIN` for either, except a user
/// changing their own password where the engine allows it).
pub async fn user(State(shared): State<Arc<Shared>>, headers: HeaderMap, body: Body) -> Response {
    let session = match post_session(&shared, &headers).await {
        Ok(session) => session,
        Err(response) => return response,
    };
    let body: UserRequest = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    if !crate::account::valid_name(&body.name) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "a user name is letters, digits, `_`, `-` and `.`",
        );
    }
    if shared.opts.read_only {
        return json_error(
            StatusCode::FORBIDDEN,
            "the console is read-only (--ui-read-only)",
        );
    }
    if let Some(problem) = crate::account::password_problem(&body.name, &body.password) {
        return json_error(StatusCode::BAD_REQUEST, problem);
    }
    let worker = Arc::clone(&shared);
    let actor = Arc::clone(&session);
    let target = body.name.to_ascii_lowercase();
    let alter = body.alter;
    let outcome = tokio::task::spawn_blocking(move || -> Result<String, String> {
        // A new user starts with no credentials of an earlier user of the
        // same name.
        if !body.alter {
            worker.vault.forget(&body.name.to_ascii_lowercase())?;
        }
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
        Ok(Ok(message)) => {
            // A password set by someone else ends the user's sessions (the
            // administrator's own, when they reset themselves, stays).
            if alter {
                let keep = actor
                    .user
                    .eq_ignore_ascii_case(&target)
                    .then(|| crate::sessions::Sessions::id_of(&actor));
                shared.sessions.end_user(&target, keep);
            }
            json_ok(&json!({ "message": message }))
        }
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
    if let Err(response) = get_session(&shared, &headers).await {
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
    fn identifiers_are_quoted_in_user_statements() {
        assert_eq!(quote_ident("ali\"ce"), "\"ali\"\"ce\"");
    }
}
