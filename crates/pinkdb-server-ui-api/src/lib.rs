//! The warehouse console's wire types (wave 121), shared by the server
//! (`pinkdb-server-ui`) and the browser app (`pinkdb-server-ui-web`, Leptos
//! compiled to wasm), plus the SQL helpers the editor uses ([`sql`]).
//!
//! One definition per payload means the two sides cannot drift: a field the
//! server renames is a compile error in the browser app.

use serde::{Deserialize, Serialize};

pub mod sql;

// ---------- sessions ----------

/// `POST /api/login`: a user and password (a console without
/// authentication takes an empty body).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LoginRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

/// The answer to `POST /api/login`: a session, or (for a user with
/// two-factor) the challenge `POST /api/login/verify` completes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LoginAnswer {
    Session(SessionInfo),
    SecondFactor(SecondFactor),
}

/// The password was right; a second factor is needed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SecondFactor {
    /// `totp`.
    pub second_factor: String,
    /// The challenge id (5 minutes, 5 tries, the same address).
    pub challenge: String,
}

/// `POST /api/login/verify`: an authenticator code, or a recovery code.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VerifyRequest {
    pub challenge: String,
    pub code: String,
}

/// `POST /api/setup`: the first account of a store without users, with the
/// setup code the server printed.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SetupRequest {
    pub code: String,
    pub user: String,
    pub password: String,
}

/// `GET /api/session` and the answer to a login.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionInfo {
    /// `current_user()` of the session.
    pub user: String,
    /// The token every `POST` echoes in `X-CSRF-Token`.
    pub csrf: String,
    /// `password` or `none`.
    pub auth: String,
    /// `--ui-read-only`.
    pub read_only: bool,
    /// The agent extension is loaded.
    pub agent: bool,
    /// The session's identity is an administrator (or the trusted operator
    /// session): the Access and Storage panels offer admin actions.
    #[serde(default)]
    pub admin: bool,
    /// The rows of one result the browser gets.
    pub max_shown_rows: usize,
    /// The user signs in with a second factor.
    #[serde(default)]
    pub two_factor: bool,
}

/// The `401` body of `GET /api/session`: which form to show — `password`
/// (sign in) or `setup` (the first account of a store without users).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoginRequired {
    pub error: String,
    pub auth: String,
}

/// Any error body.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
}

// ---------- running SQL ----------

/// `POST /api/query`
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    #[serde(default)]
    pub db: Option<String>,
    /// The client's id for this run (1–64 URL-safe characters): what
    /// `POST /api/cancel` names.
    pub run_id: String,
    #[serde(default)]
    pub worksheet_id: String,
}

/// `POST /api/cancel`
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CancelRequest {
    pub run_id: String,
}

/// One column of a result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResultColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub dtype: String,
}

/// The wave-84 profile of one statement, in milliseconds.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Profile {
    pub parse_ms: f64,
    pub bind_ms: f64,
    pub optimize_ms: f64,
    pub execute_ms: f64,
    pub log_ms: f64,
    pub total_ms: f64,
    pub kind: String,
    pub rows: u64,
}

/// One statement's result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatementResult {
    /// The statement's own text.
    pub statement: String,
    /// `rows` for a result set, `ok` for anything else.
    pub kind: String,
    /// The outcome line (`Inserted 3 rows`).
    pub message: String,
    pub columns: Vec<ResultColumn>,
    /// The rows: `None` for NULL, else the cell's text.
    pub rows: Vec<Vec<Option<String>>>,
    /// The result's full row count.
    pub row_count: u64,
    /// `rows` holds fewer rows than `row_count`.
    pub truncated: bool,
    pub profile: Profile,
}

/// The statement a run stopped at.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatementError {
    /// Its position in the run (0-based).
    pub index: usize,
    pub statement: String,
    pub message: String,
    pub cancelled: bool,
}

/// The answer to `POST /api/query`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryResponse {
    pub results: Vec<StatementResult>,
    pub error: Option<StatementError>,
    /// The session database after the run (a `USE` changes it).
    pub db: String,
    pub elapsed_ms: f64,
}

/// `POST /api/export`: one read-only statement, re-run as a download.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExportRequest {
    pub sql: String,
    #[serde(default)]
    pub db: Option<String>,
    /// `csv` or `json`.
    pub format: String,
}

// ---------- worksheets ----------

/// One worksheet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Worksheet {
    pub id: String,
    pub owner: String,
    pub title: String,
    pub sql: String,
    pub created_at: String,
    pub updated_at: String,
    /// Comma-separated user names, or `*`.
    pub shared_with: String,
    /// The caller owns it (and may change it).
    pub mine: bool,
}

/// `GET /api/worksheets`
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorksheetList {
    pub worksheets: Vec<Worksheet>,
}

/// `POST /api/worksheets`: create (no `id`) or update (the owner only).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SaveWorksheet {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub sql: String,
    #[serde(default)]
    pub shared_with: String,
}

/// `POST /api/worksheets/delete`
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteWorksheet {
    pub id: String,
}

/// One history row.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub id: String,
    pub worksheet_id: String,
    pub sql: String,
    pub started_at: String,
    pub elapsed_ms: i64,
    pub status: String,
    pub rows: i64,
    pub error: String,
}

/// `GET /api/history`
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryList {
    pub history: Vec<HistoryEntry>,
    pub limit: usize,
}

// ---------- catalog ----------

/// One column of a catalog object, with its statistics when it has them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub dtype: String,
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub min: Option<String>,
    pub max: Option<String>,
}

/// The schema every database holds its objects in (pinkdb has one schema per
/// database, named like DuckDB's default).
pub const DEFAULT_SCHEMA: &str = "main";

/// One row of `pinkdb_catalog()`, with columns for tables and views.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogObject {
    pub database: String,
    /// The schema in the database (one per database today: `main`).
    pub schema: String,
    pub name: String,
    /// `TABLE`, `VIEW`, `MATERIALIZED VIEW`, `INDEX`, `TRIGGER`, `MACRO`,
    /// `PROCEDURE`, `LAKE TABLE`.
    pub kind: String,
    /// The table an index or trigger belongs to.
    pub table_name: Option<String>,
    pub detail: Option<String>,
    pub rows: Option<i64>,
    pub columns: Vec<CatalogColumn>,
}

/// `GET /api/catalog`
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Catalog {
    pub current: String,
    pub databases: Vec<String>,
    pub objects: Vec<CatalogObject>,
}

// ---------- access control (wave 121b) ----------

/// One row of `pinkdb_users()`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Principal {
    pub name: String,
    /// `USER` or `ROLE`.
    pub kind: String,
    pub is_admin: bool,
    /// Direct role memberships, comma-separated.
    pub roles: String,
    /// The user signs in with a second factor (wave 121d; administrators
    /// see it).
    #[serde(default)]
    pub two_factor: bool,
    /// The user's live console sessions (administrators see it).
    #[serde(default)]
    pub sessions: usize,
}

/// `POST /api/access/sessions/end` and `/api/access/totp/reset`: an
/// administrator's action on a user.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct UserAction {
    pub name: String,
}

/// One row of `pinkdb_grants()`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GrantRow {
    pub principal: String,
    /// `SELECT`…`CREATE`, `ADMIN`, or `MEMBER` (a role membership).
    pub privilege: String,
    /// `TABLE`, `DATABASE`, `GLOBAL` or `ROLE`.
    pub object_type: String,
    pub database: Option<String>,
    pub name: Option<String>,
}

/// `GET /api/access`: what the session may see of the access catalog.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Access {
    pub me: String,
    pub admin: bool,
    pub principals: Vec<Principal>,
    pub grants: Vec<GrantRow>,
}

/// `POST /api/access/user`: create a user, or (`alter`) set a user's
/// password. Not a worksheet statement, so the password is never recorded
/// in the run history.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserRequest {
    pub name: String,
    pub password: String,
    #[serde(default)]
    pub alter: bool,
}

// ---------- the account (wave 121d) ----------

/// One console session of the user (`GET /api/account`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionRow {
    /// The public id (not the cookie).
    pub id: String,
    /// Unix seconds.
    pub started: u64,
    /// Seconds since the session's last request.
    pub idle_secs: u64,
    pub ip: String,
    pub user_agent: String,
    /// The session this request came from.
    pub current: bool,
}

/// A personal access token, without its secret.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TokenRow {
    pub id: String,
    pub name: String,
    /// The first characters (`pdbt_AbCd`), to recognize it.
    pub hint: String,
    /// Unix seconds.
    pub created_at: u64,
    /// Unix seconds; 0 = never.
    pub expires_at: u64,
    /// Unix seconds; 0 = never used.
    pub last_used_at: u64,
}

/// `GET /api/account`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AccountInfo {
    pub user: String,
    pub admin: bool,
    /// Only a store user has a password, a second factor and tokens (not
    /// the operator of a console without sign-in).
    pub has_password: bool,
    pub two_factor: bool,
    pub recovery_left: usize,
    pub sessions: Vec<SessionRow>,
    pub tokens: Vec<TokenRow>,
}

/// `POST /api/account/password`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PasswordChange {
    pub current: String,
    pub new: String,
}

/// `POST /api/account/sessions/end`: one session by id, or every other one.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EndSessions {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub others: bool,
}

/// `POST /api/account/tokens`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NewToken {
    pub name: String,
    /// Days until it expires; 0 = never.
    pub days: u32,
}

/// The answer to `POST /api/account/tokens`: the secret, shown once.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreatedToken {
    pub token: TokenRow,
    pub secret: String,
}

/// `POST /api/account/tokens/revoke`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RevokeToken {
    pub id: String,
}

/// The answer to `POST /api/account/totp/start`: what an authenticator app
/// needs. The QR code is one SVG path on a `qr_size` square.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TotpEnrolment {
    pub secret: String,
    pub uri: String,
    pub qr_path: String,
    pub qr_size: usize,
}

/// `POST /api/account/totp/confirm`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TotpConfirm {
    pub code: String,
}

/// `POST /api/account/totp/disable` and `/api/account/recovery`: the
/// password, again.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PasswordCheck {
    pub password: String,
}

/// New recovery codes, shown once.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecoveryCodes {
    pub codes: Vec<String>,
}

// ---------- storage (wave 121b) ----------

/// One labelled fact about the store's storage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StorageFact {
    pub label: String,
    pub value: String,
}

/// `GET /api/storage`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Storage {
    /// `in-memory`, `durable` or `lake`.
    pub mode: String,
    /// The store file or the lake catalog (secrets redacted).
    pub location: String,
    pub facts: Vec<StorageFact>,
    /// File and URL formats an import can read in this server build.
    pub import_formats: Vec<String>,
}

// ---------- uploads (wave 121b) ----------

/// The answer to `POST /api/upload`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UploadResult {
    pub database: String,
    pub table: String,
    pub rows: u64,
    pub columns: Vec<ResultColumn>,
    pub elapsed_ms: f64,
}

// ---------- the agent ----------

/// One provider the agent can use. Never a key, never a URL.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Provider {
    pub name: String,
    pub protocol: String,
    pub default_model: String,
    pub key_set: bool,
    pub key_required: bool,
    pub base_url_set: bool,
    /// Configured: a base URL, and a key when one is required.
    pub ready: bool,
    /// The environment variables that hold the key (names only).
    pub key_variables: Vec<String>,
    pub base_url_variable: String,
}

/// The agent's provider table (`.agent providers json`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Providers {
    pub providers: Vec<Provider>,
    /// `PINKDB_AGENT_PROVIDER`, when the server sets it.
    pub default: Option<String>,
}

/// `GET /api/agent`
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentInfo {
    pub loaded: bool,
    #[serde(default)]
    pub memory: bool,
    #[serde(default)]
    pub allow_writes: bool,
    #[serde(default)]
    pub providers: Option<Providers>,
    #[serde(default)]
    pub hint: Option<String>,
}

/// `POST /api/ask`; the answer is a `text/event-stream` of [`AskEvent`]s.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AskRequest {
    pub question: String,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
}

/// One event of an ask stream (`event: <name>` + `data: <json>`).
#[derive(Clone, Debug, PartialEq)]
pub enum AskEvent {
    /// A diagnostic line of the agent.
    Progress(String),
    /// One statement the model ran, and the reply it got.
    Tool {
        sql: String,
        ok: bool,
        reply: String,
    },
    /// Answer text.
    Answer(String),
    /// The run ended.
    Done,
    /// The run failed or was cancelled.
    Error(String),
}

/// `progress` / `answer` data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TextEvent {
    pub text: String,
}

/// `tool` data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolEvent {
    pub sql: String,
    pub ok: bool,
    pub reply: String,
}

/// `error` data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorEvent {
    pub message: String,
}

/// The event-stream frame for one name and data line.
pub fn sse_frame(name: &str, data: &str) -> String {
    format!("event: {name}\ndata: {data}\n\n")
}

/// Split complete frames off the front of `buffer` (what has arrived of an
/// event stream so far): `(name, data)` per frame; an incomplete tail stays
/// in the buffer.
pub fn take_sse_frames(buffer: &mut String) -> Vec<(String, String)> {
    let mut out = Vec::new();
    while let Some(end) = buffer.find("\n\n") {
        let frame: String = buffer.drain(..end + 2).collect();
        let mut name = String::from("message");
        let mut data = String::new();
        for line in frame.lines() {
            if let Some(rest) = line.strip_prefix("event:") {
                name = rest.trim().to_string();
            } else if let Some(rest) = line.strip_prefix("data:") {
                if !data.is_empty() {
                    data.push('\n');
                }
                data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
            }
        }
        out.push((name, data));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_split_across_chunks() {
        let mut buffer = String::new();
        buffer.push_str(&sse_frame("tool", "{\"sql\":\"SELECT 1\"}"));
        buffer.push_str("event: answer\ndata: {\"te");
        let frames = take_sse_frames(&mut buffer);
        assert_eq!(
            frames,
            vec![("tool".to_string(), "{\"sql\":\"SELECT 1\"}".to_string())]
        );
        buffer.push_str("xt\":\"hi\"}\n\n");
        let frames = take_sse_frames(&mut buffer);
        assert_eq!(frames[0].0, "answer");
        let text: TextEvent = serde_json::from_str(&frames[0].1).unwrap();
        assert_eq!(text.text, "hi");
        assert!(buffer.is_empty());
    }

    #[test]
    fn a_result_round_trips() {
        let result = StatementResult {
            statement: "SELECT 1".into(),
            kind: "rows".into(),
            message: "1 rows".into(),
            columns: vec![ResultColumn {
                name: "x".into(),
                dtype: "INTEGER".into(),
            }],
            rows: vec![vec![Some("1".into())], vec![None]],
            row_count: 2,
            truncated: false,
            profile: Profile::default(),
        };
        let text = serde_json::to_string(&result).unwrap();
        assert!(text.contains("\"type\":\"INTEGER\""));
        let back: StatementResult = serde_json::from_str(&text).unwrap();
        assert_eq!(back, result);
    }
}
