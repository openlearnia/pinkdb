//! MCP conformance without a host: a fake [`Host`] answers the statement
//! calls, so these tests cover the wire shape (initialize, tools/list,
//! tools/call, errors, notifications) and the statement each tool sends.
//! The end-to-end path through a real store lives in
//! `crates/pinkdb-cli/tests/mcp.rs`.

use std::collections::VecDeque;

use pinkdb_ext_sdk::StatementReply;
use pinkdb_mcp::server as bridge;
use pinkdb_mcp::{Host, Server, bind_address, is_loopback, tool_names};
use serde_json::{Value, json};

/// Answers each statement with the queued replies (the last one repeats) and
/// records what it was asked.
struct FakeHost {
    statements: Vec<String>,
    replies: VecDeque<StatementReply>,
}

impl FakeHost {
    fn new(replies: Vec<StatementReply>) -> Self {
        FakeHost {
            statements: Vec::new(),
            replies: replies.into(),
        }
    }
}

impl Host for FakeHost {
    fn execute(&mut self, statement: &str) -> StatementReply {
        self.statements.push(statement.to_string());
        if self.replies.len() > 1 {
            self.replies.pop_front().expect("checked length")
        } else {
            self.replies.front().cloned().unwrap_or(StatementReply {
                code: 0,
                text: String::new(),
            })
        }
    }
}

fn ok(text: &str) -> StatementReply {
    StatementReply {
        code: 0,
        text: text.to_string(),
    }
}

fn failed(text: &str) -> StatementReply {
    StatementReply {
        code: 1,
        text: text.to_string(),
    }
}

fn respond(server: &mut Server<FakeHost>, message: Value) -> Value {
    let response = server
        .handle(&message.to_string())
        .expect("a request must have a response");
    serde_json::from_str(&response).expect("the response is JSON")
}

fn call(server: &mut Server<FakeHost>, name: &str, arguments: Value) -> Value {
    respond(
        server,
        json!({
            "jsonrpc": "2.0", "id": 7, "method": "tools/call",
            "params": { "name": name, "arguments": arguments },
        }),
    )
}

/// The text of a tool result.
fn text(result: &Value) -> &str {
    result["result"]["content"][0]["text"]
        .as_str()
        .expect("one text block")
}

#[test]
fn initialize_reports_tools_and_the_write_policy() {
    let mut server = bridge(FakeHost::new(vec![ok("")]), false, false);
    let response = respond(
        &mut server,
        json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} }),
    );
    let result = &response["result"];
    assert_eq!(result["protocolVersion"], "2024-11-05");
    assert_eq!(result["capabilities"]["tools"]["listChanged"], false);
    assert_eq!(result["serverInfo"]["name"], "pinkdb");
    let instructions = result["instructions"].as_str().unwrap();
    assert!(instructions.contains("READ-ONLY"), "{instructions}");
    assert!(instructions.contains("query"), "{instructions}");
    assert!(
        instructions.contains("files and URLs are refused"),
        "{instructions}"
    );

    // `allow-files` is its own switch: reads stay read-only, files open.
    let mut reader = bridge(FakeHost::new(vec![ok("")]), false, true);
    let response = respond(
        &mut reader,
        json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize" }),
    );
    let instructions = response["result"]["instructions"].as_str().unwrap();
    assert!(instructions.contains("READ-ONLY"), "{instructions}");
    assert!(
        instructions.contains("files and URLs are readable"),
        "{instructions}"
    );

    // The same server reports writes only when the host enabled them.
    let mut writer = bridge(FakeHost::new(vec![ok("")]), true, false);
    let response = respond(
        &mut writer,
        json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize" }),
    );
    assert!(
        response["result"]["instructions"]
            .as_str()
            .unwrap()
            .contains("allows writes")
    );

    let pong = respond(
        &mut server,
        json!({ "jsonrpc": "2.0", "id": 2, "method": "ping" }),
    );
    assert_eq!(pong["result"], json!({}));
}

#[test]
fn tools_list_has_a_precise_schema_per_tool() {
    let mut server = bridge(FakeHost::new(vec![ok("")]), false, false);
    let response = respond(
        &mut server,
        json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
    );
    let tools = response["result"]["tools"].as_array().expect("tools array");
    let names: Vec<&str> = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, tool_names());
    for tool in tools {
        let schema = &tool["inputSchema"];
        assert_eq!(schema["type"], "object", "{tool}");
        assert_eq!(schema["additionalProperties"], false, "{tool}");
        assert!(schema["properties"].is_object(), "{tool}");
        assert!(schema["required"].is_array(), "{tool}");
        let description = tool["description"].as_str().unwrap();
        assert!(!description.is_empty(), "{tool}");
        // One line, written for a model.
        assert!(!description.contains('\n'), "{tool}");
        // Every required property is declared.
        for required in schema["required"].as_array().unwrap() {
            assert!(
                schema["properties"]
                    .get(required.as_str().unwrap())
                    .is_some(),
                "{tool}"
            );
        }
    }
}

#[test]
fn query_sends_the_sql_and_returns_the_host_text() {
    let mut server = bridge(FakeHost::new(vec![ok("| n |\n|---|\n| 1 |")]), false, false);
    let response = call(&mut server, "query", json!({ "sql": "SELECT 1 AS n" }));
    assert_eq!(response["result"]["isError"], false);
    assert_eq!(text(&response), "| n |\n|---|\n| 1 |");
    assert_eq!(server.host().statements, ["SELECT 1 AS n"]);
}

#[test]
fn truncated_markdown_is_passed_through_unchanged() {
    // The host applies the display cap and appends the notice; the extension
    // must not touch that text.
    let rendered = "| a |\n|---|\n| 1 |\n\n(999 more rows)";
    let mut server = bridge(FakeHost::new(vec![ok(rendered)]), false, false);
    let response = call(&mut server, "query", json!({ "sql": "SELECT a FROM t" }));
    assert_eq!(text(&response), rendered);
}

#[test]
fn a_statement_error_is_a_tool_result_the_model_can_read() {
    let error = r#"{"error":{"kind":"bind","message":"table nope does not exist"}}"#;
    let mut server = bridge(FakeHost::new(vec![failed(error)]), false, false);
    let response = call(&mut server, "query", json!({ "sql": "SELECT * FROM nope" }));
    assert_eq!(response["result"]["isError"], true);
    assert_eq!(text(&response), error);
    assert!(response.get("error").is_none());
}

#[test]
fn a_host_without_a_session_is_an_error_not_a_panic() {
    let mut server = bridge(
        FakeHost::new(vec![StatementReply {
            code: 2,
            text: String::new(),
        }]),
        false,
        false,
    );
    let response = call(&mut server, "databases", json!({}));
    assert_eq!(response["result"]["isError"], true);
    assert!(text(&response).contains("no live session"));
}

#[test]
fn unknown_tool_and_bad_arguments_are_tool_errors() {
    let mut server = bridge(FakeHost::new(vec![ok("")]), false, false);
    let response = call(&mut server, "drop_everything", json!({}));
    assert_eq!(response["result"]["isError"], true);
    assert!(text(&response).contains("unknown tool"));

    let response = call(&mut server, "query", json!({}));
    assert_eq!(response["result"]["isError"], true);
    assert!(text(&response).contains("missing required string argument"));

    let response = call(
        &mut server,
        "describe",
        json!({ "table": "a; DROP TABLE b" }),
    );
    assert_eq!(response["result"]["isError"], true);
    assert!(text(&response).contains("not a plain name"));
    assert!(server.host().statements.is_empty());
}

#[test]
fn tools_build_the_statements_the_engine_takes() {
    let mut describe = bridge(FakeHost::new(vec![ok("| t |")]), false, false);
    call(&mut describe, "describe", json!({ "table": "main.orders" }));
    assert_eq!(describe.host().statements, ["DESCRIBE main.orders"]);

    let mut tables = bridge(FakeHost::new(vec![ok("| t |")]), false, false);
    call(&mut tables, "tables", json!({ "database": "sales" }));
    assert_eq!(tables.host().statements, ["USE sales", "SHOW TABLES"]);

    let mut shape = bridge(FakeHost::new(vec![ok("| t |")]), false, false);
    call(&mut shape, "databases", json!({}));
    call(&mut shape, "explain", json!({ "sql": "SELECT * FROM t" }));
    assert_eq!(
        shape.host().statements,
        ["SHOW DATABASES", "EXPLAIN SELECT * FROM t"]
    );

    let mut lake = bridge(FakeHost::new(vec![ok("| t |")]), false, false);
    call(&mut lake, "lake_tables", json!({ "catalog": "/data/lake" }));
    call(
        &mut lake,
        "lake_versions",
        json!({ "catalog": "s3://b/lake" }),
    );
    call(
        &mut lake,
        "lake_changes",
        json!({ "catalog": "/d", "table": "main.t", "from": 2, "to": 5 }),
    );
    assert_eq!(
        lake.host().statements,
        [
            ".lake tables \"/data/lake\"",
            ".lake versions \"s3://b/lake\"",
            ".lake changes \"/d\" \"main.t\" 2 5",
        ]
    );

    // A version range only has the CLI's `[from] [to]` shape.
    let response = call(
        &mut lake,
        "lake_changes",
        json!({ "catalog": "/d", "to": 5 }),
    );
    assert_eq!(response["result"]["isError"], true);
}

#[test]
fn notifications_get_no_response() {
    let mut server = bridge(FakeHost::new(vec![ok("")]), false, false);
    assert_eq!(
        server.handle(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#),
        None
    );
    assert_eq!(
        server.handle(r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{}}"#),
        None
    );
    // A parse error is still answered, with a null id.
    let response: Value = serde_json::from_str(&server.handle("{not json").unwrap()).unwrap();
    assert_eq!(response["error"]["code"], -32700);
}

#[test]
fn unknown_methods_are_json_rpc_errors() {
    let mut server = bridge(FakeHost::new(vec![ok("")]), false, false);
    let response = respond(
        &mut server,
        json!({ "jsonrpc": "2.0", "id": 3, "method": "resources/list" }),
    );
    assert_eq!(response["error"]["code"], -32601);
    assert_eq!(response["id"], 3);
}

#[test]
fn http_binds_loopback_only() {
    assert_eq!(
        bind_address("http:127.0.0.1:7801").unwrap(),
        "127.0.0.1:7801"
    );
    assert_eq!(bind_address("http:localhost:0").unwrap(), "localhost:0");
    assert_eq!(bind_address("http:::1:7801").unwrap(), "[::1]:7801");
    let refused = bind_address("http:0.0.0.0:7801").unwrap_err();
    assert!(refused.contains("localhost only"), "{refused}");
    assert!(bind_address("http:192.168.1.10:7801").is_err());
    assert!(bind_address("tcp:127.0.0.1:1").is_err());
    assert!(is_loopback("127.0.0.1"));
    assert!(is_loopback("127.9.9.9"));
    assert!(is_loopback("::1"));
    assert!(!is_loopback("0.0.0.0"));
    assert!(!is_loopback("example.com"));
}
