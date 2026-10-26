//! The six `memory_*` tools, and the warehouse tools behind them.
//!
//! A client that connects to this extension sees one bridge: `query`,
//! `describe`, … from [`pinkdb_warehouse_tools`], and memory's own tools. The
//! memory tools are thin argument parsers over [`crate::ops`], the same
//! functions `.memory` calls.
//!
//! The write tools are offered only under `allow-memory`; without it the
//! bridge is read-only for memory (search and persona still work), and a
//! direct call is refused rather than silently ignored. Even with the switch,
//! the host's internal policy confines every statement to `pinkdb_memory` —
//! the switch says "this client may change memory", the policy says "and
//! nothing else".

use serde_json::{Map, Value, json};

use crate::command;
use crate::host::{Caller, Host as MemoryHost};
use crate::ops::{self, Add};
use crate::search::Request;

/// The tools that only read.
pub const READ_TOOLS: [&str; 2] = ["memory_search", "memory_persona"];
/// The tools that change memory, and so need `allow-memory`.
pub const WRITE_TOOLS: [&str; 4] = [
    "memory_add",
    "memory_update",
    "memory_forget",
    "memory_distill",
];
/// Every memory tool, in `tools/list` order.
pub const TOOLS: [&str; 6] = [
    "memory_search",
    "memory_persona",
    "memory_add",
    "memory_update",
    "memory_forget",
    "memory_distill",
];

/// The memory tool table.
#[derive(Clone, Copy, Debug)]
pub struct Memory {
    /// Whether this bridge may change memory (`allow-memory`).
    pub allow_memory: bool,
}

impl pinkdb_mcp_protocol::ToolSet for Memory {
    fn schemas(&self) -> Vec<Value> {
        let mut schemas = pinkdb_warehouse_tools::schemas();
        schemas.extend(
            TOOLS
                .iter()
                .filter(|name| self.allow_memory || READ_TOOLS.contains(name))
                .map(|name| schema(name)),
        );
        schemas
    }

    fn call(
        &mut self,
        host: &mut dyn pinkdb_mcp_protocol::Host,
        name: &str,
        arguments: &Map<String, Value>,
    ) -> Result<pinkdb_mcp_protocol::ToolReply, String> {
        if !TOOLS.contains(&name) {
            let mut warehouse = pinkdb_warehouse_tools::Warehouse;
            return pinkdb_mcp_protocol::ToolSet::call(&mut warehouse, host, name, arguments);
        }
        if WRITE_TOOLS.contains(&name) && !self.allow_memory {
            return Err(format!(
                "{name} changes memory, which this session does not allow: restart the bridge \
                 with the memory write switch (`allow-memory`) if the task needs it"
            ));
        }
        let mut session = Session(host);
        let text = call_memory(&mut session, name, arguments)?;
        Ok(pinkdb_mcp_protocol::ToolReply { code: 0, text })
    }

    fn instructions(&self) -> Option<&'static str> {
        Some(if self.allow_memory {
            "The memory_* tools read and change the agent's long-term memory: memory_search \
             answers what was learned about the user, memory_add records a turn, and memory_distill \
             asks the configured model to distil it."
        } else {
            "The memory_* read tools answer what was learned about the user; the tools that change \
             memory need the operator's `allow-memory` switch."
        })
    }
}

/// One memory tool call: ask who is calling, then run the operation.
fn call_memory(
    host: &mut dyn MemoryHost,
    name: &str,
    arguments: &Map<String, Value>,
) -> Result<String, String> {
    let caller = Caller::of(host)?;
    match name {
        "memory_add" => {
            let id = ops::add(
                host,
                &caller,
                &Add {
                    text: required(arguments, "text")?,
                    visibility: text(arguments, "visibility"),
                    tag: text(arguments, "tag"),
                    acl: text(arguments, "acl"),
                    agent: text(arguments, "agent"),
                    speaker: text(arguments, "speaker"),
                    conversation: text(arguments, "conversation"),
                },
            )?;
            Ok(format!("added turn {id}\n"))
        }
        "memory_search" => {
            let request = Request {
                query: required(arguments, "query")?,
                max_items: number(arguments, "max_items").unwrap_or(5) as usize,
                max_chars: number(arguments, "max_chars").unwrap_or(4000) as usize,
                timeout: ops::timeout_ms(number(arguments, "timeout_ms")),
            };
            let outcome = ops::search(host, &caller, &request)?;
            Ok(command::render_search(&outcome))
        }
        "memory_update" => {
            let id = required(arguments, "id")?;
            let new = ops::update(host, &caller, &id, &required(arguments, "text")?)?;
            Ok(format!("updated {id} -> {new}\n"))
        }
        "memory_forget" => {
            let id = required(arguments, "id")?;
            ops::forget(host, &caller, &id)?;
            Ok(format!("forgot {id}\n"))
        }
        "memory_distill" => {
            let mut provider = crate::distill::provider(
                optional(arguments, "provider").as_deref(),
                optional(arguments, "model").as_deref(),
            )?;
            let report = ops::distill(host, &caller, provider.as_mut())?;
            Ok(command::render_report(&report))
        }
        "memory_persona" => Ok(command::render_persona(&ops::persona(host, &caller)?)),
        other => Err(format!("unknown memory tool \"{other}\"")),
    }
}

/// The MCP host as the memory host. `execute_internal` is the extension's own
/// channel: a non-zero code is an error with the host's text as the reason.
struct Session<'a>(&'a mut dyn pinkdb_mcp_protocol::Host);

impl MemoryHost for Session<'_> {
    fn internal(&mut self, sql: &str) -> Result<String, String> {
        let reply = self.0.execute_internal(sql);
        if reply.code == 0 {
            Ok(reply.text)
        } else if reply.text.is_empty() {
            Err("the host has no internal channel for this session".to_string())
        } else {
            Err(reply.text)
        }
    }

    fn tool(&mut self, sql: &str) -> (i32, String) {
        let reply = self.0.execute(sql);
        (reply.code, reply.text)
    }

    fn emit(&mut self, channel: i32, text: &str) {
        // A tool answers with its return value; a protocol session has no
        // output stream to write to.
        let _ = (channel, text);
    }
}

fn required(arguments: &Map<String, Value>, name: &str) -> Result<String, String> {
    let value = text(arguments, name);
    if value.trim().is_empty() {
        return Err(format!("missing required argument \"{name}\""));
    }
    Ok(value)
}

fn text(arguments: &Map<String, Value>, name: &str) -> String {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string()
}

fn optional(arguments: &Map<String, Value>, name: &str) -> Option<String> {
    let value = text(arguments, name);
    if value.is_empty() { None } else { Some(value) }
}

fn number(arguments: &Map<String, Value>, name: &str) -> Option<u64> {
    arguments.get(name).and_then(Value::as_u64)
}

/// One tool's JSON Schema.
fn schema(name: &str) -> Value {
    let (description, properties, required) = match name {
        "memory_add" => (
            "Record one turn in the user's long-term memory (the L0 layer). Distillation is \
             queued, not run: this returns immediately with the turn's id.",
            json!({
                "text": { "type": "string", "description": "the turn's text, kept verbatim" },
                "speaker": { "type": "string", "enum": ["user", "agent"], "description": "who said it (default user)" },
                "visibility": { "type": "string", "enum": ["private", "team", "restricted", "agent"], "description": "who may read it later (default private)" },
                "tag": { "type": "string", "description": "one word to filter on later" },
                "acl": { "type": "string", "description": "for restricted rows: u:USER,r:ROLE,a:AGENT, comma separated" },
                "conversation": { "type": "string", "description": "a conversation id to group turns" },
            }),
            json!(["text"]),
        ),
        "memory_search" => (
            "Layered, budgeted recall: the user's persona, the best-matching scenarios, then atoms \
             and turns ranked by BM25 fused with embeddings when configured. Every hit carries its \
             layer, id, score and source.",
            json!({
                "query": { "type": "string", "description": "what to recall" },
                "max_items": { "type": "integer", "minimum": 1, "maximum": 50, "description": "the item budget (default 5)" },
                "max_chars": { "type": "integer", "minimum": 100, "description": "the character budget (default 4000)" },
                "timeout_ms": { "type": "integer", "minimum": 1, "description": "the time budget in milliseconds (default 5000)" },
            }),
            json!(["query"]),
        ),
        "memory_update" => (
            "Correct one atom: writes a new atom that supersedes it and keeps the old one for \
             history.",
            json!({
                "id": { "type": "string", "description": "the atom to supersede" },
                "text": { "type": "string", "description": "the corrected fact" },
            }),
            json!(["id", "text"]),
        ),
        "memory_forget" => (
            "Forget one atom: it stops appearing in search, and the row and an audit entry stay.",
            json!({ "id": { "type": "string", "description": "the atom to forget" } }),
            json!(["id"]),
        ),
        "memory_distill" => (
            "Drain the distillation queue: turns to atoms, atoms to scenarios, then the persona. \
             One model call per step; needs a configured provider.",
            json!({
                "provider": { "type": "string", "description": "a provider name from the shared registry (default: the environment's)" },
                "model": { "type": "string", "description": "a model name (default: the provider's)" },
            }),
            json!([]),
        ),
        _ => (
            "The user's long-term persona, as the last distillation wrote it.",
            json!({}),
            json!([]),
        ),
    };
    json!({
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": false,
        },
    })
}
