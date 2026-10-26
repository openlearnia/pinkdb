//! The run's transcript, kept in the store it queries.
//!
//! Tables live in the dedicated `pinkdb_agent` database of the connected
//! `.pink` file. Every write goes through the host's internal channel, whose
//! policy confines it to that database; the model's own tool calls go through
//! the normal policy, which refuses those tables, so a prompt injection cannot
//! rewrite the transcript a run resumes from.
//!
//! One row set per step: the assistant turn, one row per tool call (with its
//! reply), and a checkpoint. A run that dies between rows leaves the step
//! *incomplete*, and the resume path reuses whatever was recorded.

use serde_json::Value;

use crate::host::Host;
use pinkdb_llm::{Message, Turn, Usage};

/// The database, shared with the host's internal write policy.
pub const DATABASE: &str = pinkdb_ext_sdk::AGENT_DATABASE;

const SCHEMA: [&str; 5] = [
    "CREATE TABLE IF NOT EXISTS pinkdb_agent.sessions (id VARCHAR, created_at BIGINT, \
     updated_at BIGINT, question VARCHAR, provider VARCHAR, model VARCHAR, base_url VARCHAR, \
     status VARCHAR, stop_reason VARCHAR, max_steps INTEGER, token_budget BIGINT, \
     allow_writes BOOLEAN, allow_files BOOLEAN, origin VARCHAR, origin_step INTEGER, \
     steps INTEGER, prompt_tokens BIGINT, completion_tokens BIGINT, cost DOUBLE)",
    "CREATE TABLE IF NOT EXISTS pinkdb_agent.messages (session VARCHAR, step INTEGER, \
     seq INTEGER, role VARCHAR, content VARCHAR, call_id VARCHAR, tool_name VARCHAR, \
     created_at BIGINT)",
    "CREATE TABLE IF NOT EXISTS pinkdb_agent.tool_calls (session VARCHAR, step INTEGER, \
     seq INTEGER, call_id VARCHAR, name VARCHAR, arguments VARCHAR, state VARCHAR, \
     replay_safe BOOLEAN, code INTEGER, reply VARCHAR, created_at BIGINT)",
    "CREATE TABLE IF NOT EXISTS pinkdb_agent.checkpoints (session VARCHAR, step INTEGER, \
     note VARCHAR, created_at BIGINT)",
    // `provider` and `model` are per step, not only per session: a resume or a
    // fork may switch provider, and a per-step cost comparison between two
    // providers is then a GROUP BY instead of an archaeology project.
    "CREATE TABLE IF NOT EXISTS pinkdb_agent.usage (session VARCHAR, step INTEGER, \
     provider VARCHAR, model VARCHAR, prompt_tokens BIGINT, completion_tokens BIGINT, \
     cost DOUBLE, created_at BIGINT)",
];

/// A step whose rows are all committed. Written last, so `step` in this table
/// always names a step a resume may start after.
pub const CHECKPOINT: &str = "tools";

/// One session, as `sessions` holds it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Session {
    pub id: String,
    pub question: String,
    /// The registry name of the provider this session last ran on.
    pub provider: String,
    pub model: String,
    pub base_url: String,
    pub status: String,
    pub stop_reason: String,
    pub max_steps: u32,
    pub token_budget: u64,
    pub allow_writes: bool,
    pub allow_files: bool,
    pub origin: String,
    pub origin_step: u32,
    pub steps: u32,
    pub usage: Usage,
    pub created_at: u64,
    pub updated_at: u64,
}

/// What a new session records about the run that started it.
#[derive(Debug, Clone)]
pub struct NewSession<'a> {
    pub question: &'a str,
    pub provider: &'a str,
    pub model: &'a str,
    pub base_url: &'a str,
    pub max_steps: u32,
    pub token_budget: u64,
    pub allow_writes: bool,
    pub allow_files: bool,
    pub origin: &'a str,
    pub origin_step: u32,
}

/// One recorded tool call.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ToolCallRow {
    pub call_id: String,
    pub name: String,
    pub arguments: String,
    /// `running` while the tool may still be running, `recorded` once its
    /// reply is stored. Only `recorded` rows are reused.
    pub state: String,
    pub replay_safe: bool,
    pub code: i32,
    pub reply: String,
}

/// Create the agent's database and tables if they are missing.
pub fn ensure_schema(host: &mut dyn Host) -> Result<(), String> {
    let mut sql = format!("CREATE DATABASE IF NOT EXISTS {DATABASE}");
    for statement in SCHEMA {
        sql.push_str("; ");
        sql.push_str(statement);
    }
    host.internal(&sql).map(|_| ())
}

/// A fresh session id: `a` and sixteen hex digits (microseconds + a counter),
/// so two runs started in the same microsecond still differ.
pub fn new_id() -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    format!(
        "a{micros:012x}{:04x}",
        COUNTER.fetch_add(1, Ordering::Relaxed) & 0xffff
    )
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// A SQL text literal. The only thing an agent writes that it does not
/// control is model and tool output, so every string goes through here.
fn text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn opt_text(value: &str) -> String {
    if value.is_empty() {
        "NULL".to_string()
    } else {
        text(value)
    }
}

/// Run one batch on the internal channel.
fn write(host: &mut dyn Host, sql: &str) -> Result<(), String> {
    host.internal(sql).map(|_| ())
}

/// Run a select on the internal channel and return its rows.
fn rows(host: &mut dyn Host, sql: &str) -> Result<Vec<Vec<Value>>, String> {
    let reply = host.internal(sql)?;
    let parsed: Value = serde_json::from_str(&reply)
        .map_err(|e| format!("the store answered a query with invalid JSON: {e}"))?;
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

fn cell_str(row: &[Value], index: usize) -> String {
    match row.get(index) {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

fn cell_u64(row: &[Value], index: usize) -> u64 {
    match row.get(index) {
        Some(Value::Number(number)) => number.as_u64().unwrap_or(0),
        Some(Value::String(text)) => text.parse().unwrap_or(0),
        _ => 0,
    }
}

fn cell_i32(row: &[Value], index: usize) -> i32 {
    match row.get(index) {
        Some(Value::Number(number)) => number.as_i64().unwrap_or(0) as i32,
        Some(Value::String(text)) => text.parse().unwrap_or(0),
        _ => 0,
    }
}

fn cell_bool(row: &[Value], index: usize) -> bool {
    match row.get(index) {
        Some(Value::Bool(value)) => *value,
        Some(Value::Number(number)) => number.as_u64().unwrap_or(0) != 0,
        Some(Value::String(text)) => text == "true" || text == "1",
        _ => false,
    }
}

const SESSION_COLUMNS: &str = "id, question, provider, model, base_url, status, stop_reason, \
     max_steps, token_budget, allow_writes, allow_files, origin, origin_step, steps, \
     prompt_tokens, completion_tokens, created_at, updated_at";

fn session_row(row: &[Value]) -> Session {
    Session {
        id: cell_str(row, 0),
        question: cell_str(row, 1),
        provider: cell_str(row, 2),
        model: cell_str(row, 3),
        base_url: cell_str(row, 4),
        status: cell_str(row, 5),
        stop_reason: cell_str(row, 6),
        max_steps: cell_u64(row, 7) as u32,
        token_budget: cell_u64(row, 8),
        allow_writes: cell_bool(row, 9),
        allow_files: cell_bool(row, 10),
        origin: cell_str(row, 11),
        origin_step: cell_u64(row, 12) as u32,
        steps: cell_u64(row, 13) as u32,
        usage: Usage {
            prompt_tokens: cell_u64(row, 14),
            completion_tokens: cell_u64(row, 15),
        },
        created_at: cell_u64(row, 16),
        updated_at: cell_u64(row, 17),
    }
}

/// Insert a new session row (`status = 'running'`).
pub fn create_session(host: &mut dyn Host, id: &str, new: &NewSession<'_>) -> Result<(), String> {
    let now = now();
    let sql = format!(
        "INSERT INTO {DATABASE}.sessions ({SESSION_COLUMNS}) VALUES ({}, {}, {}, {}, {}, {}, \
         NULL, {}, {}, {}, {}, {}, {}, 0, 0, 0, {}, {})",
        text(id),
        text(new.question),
        text(new.provider),
        text(new.model),
        text(new.base_url),
        text("running"),
        new.max_steps,
        new.token_budget,
        if new.allow_writes { "TRUE" } else { "FALSE" },
        if new.allow_files { "TRUE" } else { "FALSE" },
        opt_text(new.origin),
        new.origin_step,
        now,
        now,
    );
    write(host, &sql)
}

/// Load one session.
pub fn session(host: &mut dyn Host, id: &str) -> Result<Option<Session>, String> {
    let sql = format!(
        "SELECT {SESSION_COLUMNS} FROM {DATABASE}.sessions WHERE id = {}",
        text(id)
    );
    Ok(rows(host, &sql)?.first().map(|row| session_row(row)))
}

/// Every session, newest first.
pub fn sessions(host: &mut dyn Host) -> Result<Vec<Session>, String> {
    let sql = format!("SELECT {SESSION_COLUMNS} FROM {DATABASE}.sessions ORDER BY created_at DESC");
    Ok(rows(host, &sql)?
        .iter()
        .map(|row| session_row(row))
        .collect())
}

/// Record why a run stopped (`done`, `stopped`, `failed`) and its counters.
pub fn finish(
    host: &mut dyn Host,
    id: &str,
    status: &str,
    stop_reason: &str,
    steps: u32,
    usage: Usage,
    cost: f64,
) -> Result<(), String> {
    write(
        host,
        &format!(
            "UPDATE {DATABASE}.sessions SET updated_at = {}, status = {}, stop_reason = {}, \
             steps = {}, prompt_tokens = {}, completion_tokens = {}, cost = {} WHERE id = {}",
            now(),
            text(status),
            opt_text(stop_reason),
            steps,
            usage.prompt_tokens,
            usage.completion_tokens,
            cost,
            text(id),
        ),
    )
}

/// The highest checkpointed step, or 0 when the session has none.
pub fn last_step(host: &mut dyn Host, id: &str) -> Result<u32, String> {
    let sql = format!(
        "SELECT step FROM {DATABASE}.checkpoints WHERE session = {} ORDER BY step DESC LIMIT 1",
        text(id)
    );
    Ok(rows(host, &sql)?
        .first()
        .map(|row| cell_u64(row, 0) as u32)
        .unwrap_or(0))
}

/// Append a checkpoint row: the step's transcript is committed.
pub fn checkpoint(host: &mut dyn Host, id: &str, step: u32, note: &str) -> Result<(), String> {
    write(
        host,
        &format!(
            "INSERT INTO {DATABASE}.checkpoints (session, step, note, created_at) VALUES ({}, {}, \
             {}, {})",
            text(id),
            step,
            text(note),
            now(),
        ),
    )
}

/// Record the user's question (step 0).
pub fn record_question(host: &mut dyn Host, id: &str, question: &str) -> Result<(), String> {
    record_message(host, id, 0, 0, "user", question, "", "")
}

/// Record a follow-up question at the start of a new step (a fork's new
/// question, or a resume with one).
pub fn record_follow_up(host: &mut dyn Host, id: &str, question: &str) -> Result<u32, String> {
    let step = last_step(host, id)? + 1;
    record_message(host, id, step, 0, "user", question, "", "")?;
    Ok(step)
}

#[allow(clippy::too_many_arguments)]
fn record_message(
    host: &mut dyn Host,
    id: &str,
    step: u32,
    seq: u32,
    role: &str,
    content: &str,
    call_id: &str,
    tool_name: &str,
) -> Result<(), String> {
    write(
        host,
        &format!(
            "INSERT INTO {DATABASE}.messages (session, step, seq, role, content, call_id, \
             tool_name, created_at) VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
            text(id),
            step,
            seq,
            text(role),
            text(content),
            opt_text(call_id),
            opt_text(tool_name),
            now(),
        ),
    )
}

/// Only a read-only tool may run twice: after a kill the run reuses a
/// recorded reply, and re-runs only what cannot have changed the store. With
/// `allow-writes`, `query` may have, so it is not replay-safe.
pub fn is_replay_safe(name: &str, allow_writes: bool) -> bool {
    name != "query" || !allow_writes
}

/// Point a session at the provider and model it is actually running on.
///
/// A resume or a fork may name a different provider than the one recorded, and
/// `.agent show` must not then describe a run that is not happening.
pub fn set_provider(
    host: &mut dyn Host,
    id: &str,
    provider: &str,
    model: &str,
    base_url: &str,
) -> Result<(), String> {
    write(
        host,
        &format!(
            "UPDATE {DATABASE}.sessions SET updated_at = {}, provider = {}, model = {}, \
             base_url = {} WHERE id = {}",
            now(),
            text(provider),
            text(model),
            text(base_url),
            text(id),
        ),
    )
}

/// Record an assistant turn and its usage, and open a `running` row for every
/// tool call it asked for (before the calls run, so a kill is visible).
///
/// The usage row carries the session's provider and model, so the per-step
/// cost of a run that switched providers stays attributable.
pub fn record_assistant(
    host: &mut dyn Host,
    session: &Session,
    step: u32,
    turn: &Turn,
) -> Result<(), String> {
    let (id, allow_writes) = (session.id.as_str(), session.allow_writes);
    record_message(host, id, step, 1, "assistant", &turn.content, "", "")?;
    for (seq, call) in turn.tool_calls.iter().enumerate() {
        write(
            host,
            &format!(
                "INSERT INTO {DATABASE}.tool_calls (session, step, seq, call_id, name, \
                 arguments, state, replay_safe, code, reply, created_at) VALUES ({}, {}, {}, {}, \
                 {}, {}, {}, {}, 0, NULL, {})",
                text(id),
                step,
                seq + 2,
                text(&call.id),
                text(&call.name),
                text(&call.arguments),
                text("running"),
                if is_replay_safe(&call.name, allow_writes) {
                    "TRUE"
                } else {
                    "FALSE"
                },
                now(),
            ),
        )?;
    }
    write(
        host,
        &format!(
            "INSERT INTO {DATABASE}.usage (session, step, provider, model, prompt_tokens, \
             completion_tokens, cost, created_at) VALUES ({}, {}, {}, {}, {}, {}, 0, {})",
            text(id),
            step,
            text(&session.provider),
            text(&session.model),
            turn.usage.prompt_tokens,
            turn.usage.completion_tokens,
            now(),
        ),
    )
}

/// The recorded assistant turn for a step, if it was committed.
pub fn assistant(host: &mut dyn Host, id: &str, step: u32) -> Result<Option<Turn>, String> {
    let sql = format!(
        "SELECT content FROM {DATABASE}.messages WHERE session = {} AND step = {} AND role = \
         'assistant' ORDER BY seq LIMIT 1",
        text(id),
        step
    );
    let Some(content) = rows(host, &sql)?.first().map(|row| cell_str(row, 0)) else {
        return Ok(None);
    };
    let calls = tool_calls(host, id, step)?;
    let usage = usage_at(host, id, step)?;
    Ok(Some(Turn {
        content,
        tool_calls: calls
            .iter()
            .map(|row| pinkdb_llm::ToolCall {
                id: row.call_id.clone(),
                name: row.name.clone(),
                arguments: row.arguments.clone(),
            })
            .collect(),
        finish_reason: if calls.is_empty() {
            "stop".to_string()
        } else {
            "tool_calls".to_string()
        },
        usage,
    }))
}

fn usage_at(host: &mut dyn Host, id: &str, step: u32) -> Result<Usage, String> {
    let sql = format!(
        "SELECT prompt_tokens, completion_tokens FROM {DATABASE}.usage WHERE session = {} AND \
         step = {}",
        text(id),
        step
    );
    Ok(rows(host, &sql)?
        .first()
        .map(|row| Usage {
            prompt_tokens: cell_u64(row, 0),
            completion_tokens: cell_u64(row, 1),
        })
        .unwrap_or_default())
}

/// Every usage row, summed.
pub fn usage(host: &mut dyn Host, id: &str) -> Result<Usage, String> {
    let sql = format!(
        "SELECT prompt_tokens, completion_tokens FROM {DATABASE}.usage WHERE session = {}",
        text(id)
    );
    let mut total = Usage::default();
    for row in rows(host, &sql)? {
        total.prompt_tokens += cell_u64(&row, 0);
        total.completion_tokens += cell_u64(&row, 1);
    }
    Ok(total)
}

/// The tool calls of one step, in call order.
pub fn tool_calls(host: &mut dyn Host, id: &str, step: u32) -> Result<Vec<ToolCallRow>, String> {
    let sql = format!(
        "SELECT call_id, name, arguments, state, replay_safe, code, reply FROM \
         {DATABASE}.tool_calls WHERE session = {} AND step = {} ORDER BY seq",
        text(id),
        step
    );
    Ok(rows(host, &sql)?
        .iter()
        .map(|row| ToolCallRow {
            call_id: cell_str(row, 0),
            name: cell_str(row, 1),
            arguments: cell_str(row, 2),
            state: cell_str(row, 3),
            replay_safe: cell_bool(row, 4),
            code: cell_i32(row, 5),
            reply: cell_str(row, 6),
        })
        .collect())
}

/// Store a tool call's reply and mark the call `recorded`.
#[allow(clippy::too_many_arguments)]
pub fn record_tool_result(
    host: &mut dyn Host,
    id: &str,
    step: u32,
    seq: u32,
    call_id: &str,
    name: &str,
    code: i32,
    reply: &str,
) -> Result<(), String> {
    let sql = format!(
        "UPDATE {DATABASE}.tool_calls SET state = 'recorded', code = {}, reply = {} WHERE session \
         = {} AND step = {} AND call_id = {}",
        code,
        text(reply),
        text(id),
        step,
        text(call_id)
    );
    write(host, &sql)?;
    record_message(host, id, step, seq, "tool", reply, call_id, name)
}

/// The conversation so far, as the provider wants it: the question, then each
/// step's assistant turn followed by its tool replies.
pub fn history(host: &mut dyn Host, id: &str) -> Result<Vec<Message>, String> {
    let sql = format!(
        "SELECT step, seq, role, content, call_id FROM {DATABASE}.messages WHERE session = {} \
         ORDER BY step, seq",
        text(id)
    );
    let rows = rows(host, &sql)?;
    // Steps with tool calls need the assistant message to carry them, so the
    // calls are read once per step rather than per message.
    let mut calls: Vec<(u32, Vec<pinkdb_llm::ToolCall>)> = Vec::new();
    let mut history = Vec::new();
    for row in rows {
        let step = cell_u64(&row, 0) as u32;
        let role = cell_str(&row, 2);
        let content = cell_str(&row, 3);
        match role.as_str() {
            "assistant" => {
                let recorded = match calls.iter().find(|(s, _)| *s == step) {
                    Some((_, recorded)) => recorded.clone(),
                    None => {
                        let loaded: Vec<pinkdb_llm::ToolCall> = tool_calls(host, id, step)?
                            .into_iter()
                            .map(|call| pinkdb_llm::ToolCall {
                                id: call.call_id,
                                name: call.name,
                                arguments: call.arguments,
                            })
                            .collect();
                        calls.push((step, loaded.clone()));
                        loaded
                    }
                };
                history.push(Message::Assistant {
                    content,
                    tool_calls: recorded,
                });
            }
            "tool" => history.push(Message::Tool {
                call_id: cell_str(&row, 4),
                content,
            }),
            "user" => history.push(Message::User(content)),
            "system" => history.push(Message::System(content)),
            _ => {}
        }
    }
    Ok(history)
}

/// Copy the rows of `source` with `step <= upto` into `target`, plus the
/// session row itself (as a new session that records where it came from).
///
/// `provider` and `model` are what the fork will run on, which need not be
/// what the source ran on: the copied transcript is provider-neutral, so the
/// next request simply re-encodes it for the new wire protocol.
pub fn copy_into(
    host: &mut dyn Host,
    source: &Session,
    target: &str,
    upto: u32,
    provider: &str,
    model: &str,
) -> Result<(), String> {
    create_session(
        host,
        target,
        &NewSession {
            question: &source.question,
            provider,
            model,
            base_url: &source.base_url,
            max_steps: source.max_steps,
            token_budget: source.token_budget,
            allow_writes: source.allow_writes,
            allow_files: source.allow_files,
            origin: &source.id,
            origin_step: upto,
        },
    )?;
    let filter = format!("session = {} AND step <= {}", text(&source.id), upto);
    // Rows are copied one by one: the loop is short (a step is a handful of
    // rows) and `INSERT … SELECT` would tie the extension to one dialect.
    for row in rows(
        host,
        &format!(
            "SELECT step, seq, role, content, call_id, tool_name FROM {DATABASE}.messages WHERE \
             {filter} ORDER BY step, seq"
        ),
    )? {
        record_message(
            host,
            target,
            cell_u64(&row, 0) as u32,
            cell_u64(&row, 1) as u32,
            &cell_str(&row, 2),
            &cell_str(&row, 3),
            &cell_str(&row, 4),
            &cell_str(&row, 5),
        )?;
    }
    for row in rows(
        host,
        &format!(
            "SELECT step, seq, call_id, name, arguments, state, replay_safe, code, reply FROM \
             {DATABASE}.tool_calls WHERE {filter} ORDER BY step, seq"
        ),
    )? {
        write(
            host,
            &format!(
                "INSERT INTO {DATABASE}.tool_calls (session, step, seq, call_id, name, \
                 arguments, state, replay_safe, code, reply, created_at) VALUES ({}, {}, {}, {}, \
                 {}, {}, {}, {}, {}, {}, {})",
                text(target),
                cell_u64(&row, 0),
                cell_u64(&row, 1),
                text(&cell_str(&row, 2)),
                text(&cell_str(&row, 3)),
                text(&cell_str(&row, 4)),
                text(&cell_str(&row, 5)),
                if cell_bool(&row, 6) { "TRUE" } else { "FALSE" },
                cell_i32(&row, 7),
                opt_text(&cell_str(&row, 8)),
                now(),
            ),
        )?;
    }
    for row in rows(
        host,
        &format!("SELECT step, note FROM {DATABASE}.checkpoints WHERE {filter} ORDER BY step"),
    )? {
        checkpoint(host, target, cell_u64(&row, 0) as u32, &cell_str(&row, 1))?;
    }
    // The copied usage rows keep the provider that actually earned them: the
    // fork's own steps are recorded under its own provider from step N+1.
    for row in rows(
        host,
        &format!(
            "SELECT step, provider, model, prompt_tokens, completion_tokens, cost FROM \
             {DATABASE}.usage WHERE {filter} ORDER BY step"
        ),
    )? {
        write(
            host,
            &format!(
                "INSERT INTO {DATABASE}.usage (session, step, provider, model, prompt_tokens, \
                 completion_tokens, cost, created_at) VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
                text(target),
                cell_u64(&row, 0),
                text(&cell_str(&row, 1)),
                text(&cell_str(&row, 2)),
                cell_u64(&row, 3),
                cell_u64(&row, 4),
                if let Some(Value::Number(number)) = row.get(5) {
                    number.to_string()
                } else {
                    "0".to_string()
                },
                now(),
            ),
        )?;
    }
    Ok(())
}
