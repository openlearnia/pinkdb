//! Who is asking, and the two channels the extension talks to the host over.
//!
//! The same shape the agent extension uses (`extensions/agent/src/host.rs`):
//! a command has a confined internal channel for its own bookkeeping and a
//! policy-checked public one for the statements a client asked for. A memory
//! tool needs only the internal one — every statement it runs is one the
//! extension itself wrote — but it is the same trait so one implementation
//! serves the dot command and the MCP bridge.

use serde_json::Value;

/// Channel 0: the command's answer, written to stdout by the CLI.
pub const ANSWER: i32 = 0;
/// Channel 1: diagnostics, written to stderr.
pub const DIAGNOSTIC: i32 = 1;

/// The host's side of one memory command or tool call.
pub trait Host {
    /// Run one statement on the extension's own channel. The host applies the
    /// internal policy (`write_database = pinkdb_memory`) and answers with
    /// JSON rows — one `{"columns":[…],"rows":[[…]]}` object per statement.
    fn internal(&mut self, sql: &str) -> Result<String, String>;

    /// Run one statement on the public channel, under the policy the client's
    /// own statements get. A memory tool does not use it; the served protocol
    /// does, for the warehouse tools.
    fn tool(&mut self, sql: &str) -> (i32, String);

    /// One line out: [`ANSWER`] or [`DIAGNOSTIC`].
    fn emit(&mut self, channel: i32, text: &str);
}

impl Host for pinkdb_ext_sdk::CommandSession {
    fn internal(&mut self, sql: &str) -> Result<String, String> {
        self.execute_internal(sql)
    }

    fn tool(&mut self, sql: &str) -> (i32, String) {
        let reply = self.execute_statement(sql);
        (reply.code, reply.text)
    }

    fn emit(&mut self, channel: i32, text: &str) {
        self.emit(channel, text);
    }
}

/// The principal a statement runs as, and its roles — the answer to
/// `SELECT current_user(), current_roles()` on the internal channel.
///
/// The engine binds both builtins at bind time from the session, so the
/// answer is the host's, never something a tool argument could set. An
/// embedded store with no users answers one implicit name (`pinkdb`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Caller {
    pub user: String,
    /// Display names, as `current_roles()` joins them; empty when none.
    pub roles: Vec<String>,
}

impl Caller {
    /// Ask the host who this is.
    pub fn of(host: &mut dyn Host) -> Result<Caller, String> {
        let reply = host.internal("SELECT current_user(), current_roles()")?;
        let first = rows(&reply)?;
        let row = first
            .first()
            .ok_or_else(|| "the host answered the identity query with no row".to_string())?;
        let user = cell_str(row, 0);
        if user.is_empty() {
            return Err("the host answered the identity query with no user".to_string());
        }
        Ok(Caller {
            user,
            roles: cell_str(row, 1)
                .split(',')
                .map(str::trim)
                .filter(|role| !role.is_empty())
                .map(str::to_string)
                .collect(),
        })
    }
}

/// Run a select on the internal channel and return its rows.
pub fn rows(reply: &str) -> Result<Vec<Vec<Value>>, String> {
    let parsed: Value = serde_json::from_str(reply)
        .map_err(|error| format!("the store answered a query with invalid JSON: {error}"))?;
    let Some(first) = parsed.as_array().and_then(|results| results.first()) else {
        return Ok(Vec::new());
    };
    Ok(first
        .get("rows")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row.as_array().cloned())
                .collect()
        })
        .unwrap_or_default())
}

/// Run a select on the internal channel.
pub fn query(host: &mut dyn Host, sql: &str) -> Result<Vec<Vec<Value>>, String> {
    let reply = host.internal(sql)?;
    rows(&reply)
}

pub fn cell_str(row: &[Value], index: usize) -> String {
    match row.get(index) {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

pub fn cell_u64(row: &[Value], index: usize) -> u64 {
    match row.get(index) {
        Some(Value::Number(number)) => number.as_u64().unwrap_or(0),
        Some(Value::String(text)) => text.parse().unwrap_or(0),
        _ => 0,
    }
}

pub fn cell_f64(row: &[Value], index: usize) -> f64 {
    match row.get(index) {
        Some(Value::Number(number)) => number.as_f64().unwrap_or(0.0),
        Some(Value::String(text)) => text.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

pub fn cell_bool(row: &[Value], index: usize) -> bool {
    match row.get(index) {
        Some(Value::Bool(value)) => *value,
        Some(Value::Number(number)) => number.as_u64().unwrap_or(0) != 0,
        Some(Value::String(text)) => text == "true" || text == "1",
        _ => false,
    }
}
