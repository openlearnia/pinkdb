//! The tools a model sees, and the statement each call becomes. Shared by
//! the MCP bridge (wave 105) and the agent extension (wave 106) so both expose
//! the same warehouse surface.
//!
//! Every statement goes to the host unchanged (SQL, or a `.lake …` command on
//! a host that routes those); the host decides whether it may run. The
//! argument validation here is about producing a readable error, not about
//! permission: a malformed name must not turn into a surprising statement.

use serde_json::{Map, Value, json};

/// What the host answered for one statement.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ToolReply {
    /// `0` ok, `1` statement error, `2` the host has no live session.
    pub code: i32,
    pub text: String,
}

/// The host side of a tool call: run one statement, return its rendered text.
///
/// The host applies its own policy and budgets; this crate never decides what
/// a tool may run.
pub trait ToolHost {
    fn execute(&mut self, statement: &str) -> ToolReply;
}

/// Run a tool call and keep the last reply: that is the answer.
///
/// A failing statement stops the call — the model reads the one-line JSON
/// error and can correct itself — and an unknown tool or bad arguments come
/// back as `Err` (also shown to the model).
pub fn run(
    host: &mut dyn ToolHost,
    name: &str,
    args: &Map<String, Value>,
) -> Result<ToolReply, String> {
    let statements = statements(name, args)?;
    let mut last = ToolReply::default();
    for statement in statements {
        last = host.execute(&statement);
        if last.code != 0 {
            break;
        }
    }
    Ok(last)
}

/// Every tool, in `tools/list` order.
pub fn tool_names() -> Vec<&'static str> {
    vec![
        "query",
        "describe",
        "tables",
        "databases",
        "explain",
        "lake_tables",
        "lake_versions",
        "lake_changes",
    ]
}

/// The `tools/list` result entries: a precise JSON Schema and a one-line
/// description written for a model deciding what to call next.
pub fn schemas() -> Vec<Value> {
    let sql = json!({
        "type": "string",
        "description": "One SQL statement: SELECT, WITH, DESCRIBE, SHOW or EXPLAIN.",
    });
    vec![
        tool(
            "query",
            "Run one read-only SQL query; returns compact Markdown with at most 100 rows and the true row count.",
            json!({
                "sql": sql,
            }),
            &["sql"],
        ),
        tool(
            "describe",
            "Show a table's columns, types and nullability. Use it before writing joins.",
            json!({
                "table": {
                    "type": "string",
                    "description": "Table name, optionally database-qualified (`main.orders`).",
                },
            }),
            &["table"],
        ),
        tool(
            "tables",
            "List the tables of a database (the current one unless `database` is given).",
            json!({
                "database": {
                    "type": "string",
                    "description": "Database to list; omit for the current one.",
                },
            }),
            &[],
        ),
        tool(
            "databases",
            "List the databases of this store.",
            json!({}),
            &[],
        ),
        tool(
            "explain",
            "Show the plan of a query, including a scan-size estimate for expensive inputs.",
            json!({
                "sql": {
                    "type": "string",
                    "description": "The query to plan; it is not run.",
                },
            }),
            &["sql"],
        ),
        tool(
            "lake_tables",
            "List the tables and row counts of a lake catalog (needs a CLI host built with lake support).",
            json!({
                "catalog": {
                    "type": "string",
                    "description": "Lake catalog location: a directory, `s3://…`, or a catalog URL.",
                },
            }),
            &["catalog"],
        ),
        tool(
            "lake_versions",
            "List a lake catalog's version history, newest last.",
            json!({
                "catalog": {
                    "type": "string",
                    "description": "Lake catalog location, as in lake_tables.",
                },
            }),
            &["catalog"],
        ),
        tool(
            "lake_changes",
            "List a lake catalog's row-level changes between two versions (inclusive).",
            json!({
                "catalog": {
                    "type": "string",
                    "description": "Lake catalog location, as in lake_tables.",
                },
                "table": {
                    "type": "string",
                    "description": "Restrict the change log to one table; omit for every table.",
                },
                "from": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "First version, inclusive (default 1).",
                },
                "to": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Last version, inclusive (default: the current version).",
                },
            }),
            &["catalog"],
        ),
    ]
}

fn tool(name: &str, description: &str, properties: Value, required: &[&str]) -> Value {
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

/// The statements a tool call runs, in order. `Err` is the tool error text
/// shown to the model (bad arguments or an unknown tool).
pub fn statements(name: &str, args: &Map<String, Value>) -> Result<Vec<String>, String> {
    let statements = match name {
        "query" => vec![string_arg(args, "sql")?.to_string()],
        "describe" => {
            let table = string_arg(args, "table")?;
            check_identifier("table", table)?;
            vec![format!("DESCRIBE {table}")]
        }
        "tables" => match optional_string(args, "database")? {
            Some(database) => {
                check_identifier("database", database)?;
                // The extension function has no `SHOW TABLES FROM`, so move
                // the session's current database first.
                vec![format!("USE {database}"), "SHOW TABLES".to_string()]
            }
            None => vec!["SHOW TABLES".to_string()],
        },
        "databases" => vec!["SHOW DATABASES".to_string()],
        "explain" => vec![format!("EXPLAIN {}", string_arg(args, "sql")?)],
        "lake_tables" => vec![format!(".lake tables {}", lake_path(args, "catalog")?)],
        "lake_versions" => vec![format!(".lake versions {}", lake_path(args, "catalog")?)],
        "lake_changes" => lake_changes(args)?,
        other => {
            return Err(format!(
                "unknown tool \"{other}\"; this server has: {}",
                tool_names().join(", ")
            ));
        }
    };
    Ok(statements)
}

fn lake_changes(args: &Map<String, Value>) -> Result<Vec<String>, String> {
    let mut command = format!(".lake changes {}", lake_path(args, "catalog")?);
    if let Some(table) = optional_string(args, "table")? {
        command.push(' ');
        command.push_str(&quoted_text("table", table)?);
    }
    if let Some(from) = optional_version(args, "from")? {
        command.push_str(&format!(" {from}"));
    }
    if let Some(to) = optional_version(args, "to")? {
        if optional_version(args, "from")?.is_none() {
            return Err("\"to\" needs \"from\" (versions are a range)".to_string());
        }
        command.push_str(&format!(" {to}"));
    }
    Ok(vec![command])
}

fn lake_path(args: &Map<String, Value>, key: &str) -> Result<String, String> {
    quoted_text("catalog", string_arg(args, key)?)
}

fn string_arg<'a>(args: &'a Map<String, Value>, key: &str) -> Result<&'a str, String> {
    match args.get(key).and_then(Value::as_str) {
        Some(text) if !text.trim().is_empty() => Ok(text),
        Some(_) => Err(format!("\"{key}\" must not be empty")),
        None => Err(format!("missing required string argument \"{key}\"")),
    }
}

fn optional_string<'a>(args: &'a Map<String, Value>, key: &str) -> Result<Option<&'a str>, String> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.trim().is_empty() => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(_) => Err(format!("\"{key}\" must be a string")),
    }
}

fn optional_version(args: &Map<String, Value>, key: &str) -> Result<Option<u64>, String> {
    let value = match args.get(key) {
        None | Some(Value::Null) => return Ok(None),
        Some(value) => value,
    };
    let number = match value {
        // Models often send numeric arguments as strings; accept both.
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    };
    match number {
        Some(version) if version >= 1 => Ok(Some(version)),
        _ => Err(format!("\"{key}\" must be a version number >= 1")),
    }
}

/// A table or database name the engine can take unquoted: letters, digits,
/// `_`, `$`, `.` and `"` (a quoted identifier). Everything else is refused
/// rather than spliced into a statement.
fn check_identifier(kind: &str, name: &str) -> Result<(), String> {
    if name
        .chars()
        .all(|c| c.is_alphanumeric() || matches!(c, '_' | '$' | '.' | '"'))
    {
        Ok(())
    } else {
        Err(format!(
            "{kind} \"{name}\" is not a plain name (letters, digits, _, $, . and \" only)"
        ))
    }
}

/// One lake-command argument, double-quoted because that is what the CLI's
/// `.lake` tokenizer unquotes (`""` is one quote). Control characters are
/// refused: a newline would end the command.
fn quoted_text(kind: &str, text: &str) -> Result<String, String> {
    if text.chars().any(char::is_control) {
        return Err(format!("{kind} \"{text}\" contains a control character"));
    }
    Ok(format!("\"{}\"", text.replace('"', "\"\"")))
}

/// The warehouse tool table as an MCP [`ToolSet`]: the eight tools above,
/// planned into statements by [`run`]. The MCP bridge (wave 105) and agent
/// memory (wave 109) both serve it, so a client sees one warehouse surface
/// whichever extension it connects to.
///
/// [`ToolSet`]: pinkdb_mcp_protocol::ToolSet
#[derive(Default)]
pub struct Warehouse;

impl pinkdb_mcp_protocol::ToolSet for Warehouse {
    fn schemas(&self) -> Vec<Value> {
        schemas()
    }

    fn call(
        &mut self,
        host: &mut dyn pinkdb_mcp_protocol::Host,
        name: &str,
        arguments: &Map<String, Value>,
    ) -> Result<pinkdb_mcp_protocol::ToolReply, String> {
        let reply = run(&mut Bridge(host), name, arguments)?;
        Ok(pinkdb_mcp_protocol::ToolReply {
            code: reply.code,
            text: reply.text,
        })
    }
}

/// The MCP host as this crate's [`ToolHost`]. The public channel only: a tool
/// call is a statement the host's policy decides on, never the extension's
/// own bookkeeping.
struct Bridge<'a>(&'a mut dyn pinkdb_mcp_protocol::Host);

impl ToolHost for Bridge<'_> {
    fn execute(&mut self, statement: &str) -> ToolReply {
        let reply = self.0.execute(statement);
        ToolReply {
            code: reply.code,
            text: reply.text,
        }
    }
}
