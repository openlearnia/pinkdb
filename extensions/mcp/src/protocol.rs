//! The MCP protocol: JSON-RPC 2.0 over one message in, at most one out.
//!
//! Only the methods an MCP client needs to use tools are implemented —
//! `initialize`, `ping`, `tools/list`, `tools/call`, and the
//! `notifications/initialized` notification. No resources, prompts or
//! sampling: pinkdb offers tools, and the client's model does the rest.

use serde_json::{Value, json};

use crate::tools;
use pinkdb_ext_sdk::StatementReply;

/// What a tool call asks the host to run. The implementation is the ABI's
/// statement callback in production and a fake in tests.
pub trait Host {
    /// Run one statement (SQL, or a `.lake …` command on a host that routes
    /// those). The reply's code is `0` success, `1` statement error with a
    /// JSON error line, anything else "no session".
    fn execute(&mut self, statement: &str) -> StatementReply;
}

/// One JSON-RPC dispatcher over a [`Host`].
pub struct Server<H: Host> {
    host: H,
    /// Reported in `initialize`; the host enforces it.
    allow_writes: bool,
    /// The same for external files and URLs (`allow_files` in the host policy).
    allow_files: bool,
}

impl<H: Host> tools::ToolHost for Server<H> {
    fn execute(&mut self, statement: &str) -> tools::ToolReply {
        let reply = self.host.execute(statement);
        tools::ToolReply {
            code: reply.code,
            text: reply.text,
        }
    }
}

impl<H: Host> Server<H> {
    pub fn new(host: H, allow_writes: bool, allow_files: bool) -> Self {
        Server {
            host,
            allow_writes,
            allow_files,
        }
    }

    /// The host, so a test can see which statements were sent.
    pub fn host(&self) -> &H {
        &self.host
    }

    /// Handle one message (a whole line on stdio, a whole body on HTTP).
    /// Returns the response to send, or `None` for a notification, an empty
    /// line, or a message that is not a request.
    pub fn handle(&mut self, message: &str) -> Option<String> {
        if message.trim().is_empty() {
            return None;
        }
        let request: Value = match serde_json::from_str(message) {
            Ok(value) => value,
            Err(error) => {
                return Some(error_response(
                    Value::Null,
                    -32700,
                    &format!("parse error: {error}"),
                ));
            }
        };
        let Some(method) = request.get("method").and_then(Value::as_str) else {
            // A response (to a request the host never sends) or junk without
            // a method: nothing to answer.
            return None;
        };
        if method.starts_with("notifications/") {
            // `notifications/initialized` needs no acknowledgement; unknown
            // notifications are ignored, as JSON-RPC requires.
            return None;
        }
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let params = request.get("params");
        let response = match method {
            "initialize" => success(id, self.initialize()),
            "ping" => success(id, json!({})),
            "tools/list" => success(id, json!({ "tools": tools::schemas() })),
            "tools/call" => self.call_tool(id, params),
            other => error_response(
                id,
                -32601,
                &format!("unknown method \"{other}\": pinkdb serves tools only"),
            ),
        };
        Some(response)
    }

    /// The `initialize` result. `instructions` is a sentence or two of
    /// orientation: MCP clients show it to their model, and it is the one
    /// place the model learns the session's write policy.
    fn initialize(&self) -> Value {
        let writes = if self.allow_writes {
            "This session allows writes: INSERT, UPDATE, DELETE and DDL run when asked."
        } else {
            "This session is READ-ONLY: statements that change the store, write files, load \
             extensions, change roles or attach a lake are refused. Ask the operator to restart \
             with writes enabled if the task needs them."
        };
        let files = if self.allow_files {
            "External files and URLs are readable (`read_csv`, `read_parquet`, bare paths)."
        } else {
            "External files and URLs are refused: only tables in this store are readable."
        };
        let policy = format!("{writes} {files}");
        json!({
            "protocolVersion": "2024-11-05",
            "capabilities": { "tools": { "listChanged": false } },
            "serverInfo": {
                "name": "pinkdb",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "instructions": format!(
                "pinkdb is an embedded SQL database. Call tools/list for the tools; `query` runs \
                 SQL and returns compact Markdown (at most 100 rows, then a notice with the true \
                 row count), `describe`/`tables`/`databases` explore the schema, and `explain` \
                 shows a plan before an expensive query. {policy}"
            ),
        })
    }

    /// `tools/call`: plan the statements for the named tool, run them in
    /// order, and answer with the last one's text. A statement error is a
    /// tool result with `isError: true` — the client's model reads the JSON
    /// error line and can correct itself — not a JSON-RPC error.
    fn call_tool(&mut self, id: Value, params: Option<&Value>) -> String {
        let Some(params) = params else {
            return error_response(id, -32602, "tools/call needs params");
        };
        let Some(name) = params.get("name").and_then(Value::as_str) else {
            return error_response(id, -32602, "tools/call needs a tool name");
        };
        let arguments = match params.get("arguments") {
            None | Some(Value::Null) => serde_json::Map::new(),
            Some(Value::Object(map)) => map.clone(),
            Some(_) => {
                return error_response(id, -32602, "tools/call arguments must be an object");
            }
        };
        let last = match tools::run(self, name, &arguments) {
            Ok(reply) => reply,
            Err(message) => return success(id, tool_result(&message, true)),
        };
        let text = if last.text.is_empty() {
            match last.code {
                0 => "(no output)".to_string(),
                1 => "the statement failed".to_string(),
                _ => "the host could not run a statement: no live session".to_string(),
            }
        } else {
            last.text
        };
        success(id, tool_result(&text, last.code != 0))
    }
}

/// An MCP tool result: one text block, flagged when the call failed.
fn tool_result(text: &str, is_error: bool) -> Value {
    json!({
        "content": [{ "type": "text", "text": text }],
        "isError": is_error,
    })
}

fn success(id: Value, result: Value) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "result": result }).to_string()
}

fn error_response(id: Value, code: i32, message: &str) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    })
    .to_string()
}
