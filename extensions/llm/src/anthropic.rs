//! The Anthropic Messages wire protocol: `POST <root>/messages`.
//!
//! Three things differ from chat completions, and all three are encoding:
//!
//! * the system prompt is a top-level `system` field, not a message;
//! * content is a list of blocks, and a tool call (`tool_use`) and its reply
//!   (`tool_result`) are blocks rather than messages of their own — a reply
//!   rides inside a *user* message, paired to its call by id;
//! * roles must alternate, so consecutive same-role messages are merged.
//!
//! The stream is SSE like the other protocol, but every event carries its own
//! `type` and the stream ends with `message_stop` instead of `[DONE]`.
//! Extended thinking is not enabled, and a `thinking_delta` that arrives
//! anyway is dropped like `reasoning_content` is on the other side.

use serde_json::{Value, json};

use crate::chat::{Message, PartialCall, Turn, Usage, finish_calls, slot};

/// The path this protocol appends to a provider's base URL.
pub const PATH: &str = "/messages";

/// The API version header this encoder was written against.
pub const VERSION: &str = "2023-06-01";

/// `max_tokens` is required by this API, so it needs a value even when the
/// caller has no opinion. Large enough for a table-shaped answer, small
/// enough to bound one runaway turn; `PINKDB_AGENT_MAX_TOKENS` overrides it.
pub const DEFAULT_MAX_TOKENS: u64 = 8192;

/// The request body for one turn.
///
/// `tools` are the shared MCP-shaped definitions (`name`, `description`,
/// `inputSchema`); this API wants `input_schema` and no wrapper.
pub fn request_body(model: &str, max_tokens: u64, messages: &[Message], tools: &[Value]) -> Value {
    let mut body = json!({
        "model": model,
        "max_tokens": max_tokens,
        "messages": encode_messages(messages),
        "stream": true,
    });
    let system = encode_system(messages);
    if !system.is_empty() {
        body["system"] = Value::String(system);
    }
    if !tools.is_empty() {
        body["tools"] = Value::Array(tools.iter().map(tool).collect());
    }
    body
}

/// Every system message, hoisted out of the conversation and joined.
///
/// The harness sends one, but a session that grew a second (a fork with a
/// changed prompt) must not silently lose it.
fn encode_system(messages: &[Message]) -> String {
    messages
        .iter()
        .filter_map(|message| match message {
            Message::System(text) if !text.trim().is_empty() => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// The conversation as alternating user/assistant messages of content blocks.
fn encode_messages(messages: &[Message]) -> Vec<Value> {
    let mut out: Vec<(&'static str, Vec<Value>)> = Vec::new();
    for message in messages {
        let (role, blocks) = match message {
            // Hoisted into `system`.
            Message::System(_) => continue,
            Message::User(text) => ("user", vec![json!({"type": "text", "text": text})]),
            Message::Assistant {
                content,
                tool_calls,
            } => {
                let mut blocks = Vec::new();
                if !content.is_empty() {
                    blocks.push(json!({"type": "text", "text": content}));
                }
                for call in tool_calls {
                    blocks.push(json!({
                        "type": "tool_use",
                        "id": call.id,
                        "name": call.name,
                        // The API wants an object, not the model's raw string.
                        // A fragment that does not parse would be rejected, so
                        // it degrades to an empty input the model can see went
                        // wrong, rather than failing the whole request.
                        "input": serde_json::from_str::<Value>(&call.arguments)
                            .ok()
                            .filter(Value::is_object)
                            .unwrap_or_else(|| json!({})),
                    }));
                }
                if blocks.is_empty() {
                    continue;
                }
                ("assistant", blocks)
            }
            // A tool reply is a block in a *user* message on this API.
            Message::Tool { call_id, content } => (
                "user",
                vec![json!({
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "content": content,
                })],
            ),
        };
        // One rule covers both "consecutive tool results are one user message"
        // and "roles must alternate".
        match out.last_mut() {
            Some((last_role, last_blocks)) if *last_role == role => last_blocks.extend(blocks),
            _ => out.push((role, blocks)),
        }
    }
    out.into_iter()
        .map(|(role, blocks)| json!({"role": role, "content": blocks}))
        .collect()
}

/// One MCP tool definition as an Anthropic tool.
fn tool(tool: &Value) -> Value {
    json!({
        "name": tool["name"],
        "description": tool["description"],
        "input_schema": tool["inputSchema"],
    })
}

/// Folds the Messages API's SSE events into one [`Turn`].
///
/// `input_json_delta` fragments are concatenated per content-block index,
/// exactly like chat completions' `function.arguments`.
#[derive(Debug, Default)]
pub struct Accumulator {
    content: String,
    calls: Vec<PartialCall>,
    stop_reason: String,
    usage: Usage,
}

impl Accumulator {
    /// Feed one `data:` payload. Returns `false` once the stream is complete
    /// (`message_stop`), the way `[DONE]` ends the other protocol.
    pub fn feed(&mut self, data: &str) -> Result<bool, String> {
        let event: Value = serde_json::from_str(data)
            .map_err(|e| format!("the provider sent an event that is not JSON: {e}"))?;
        match event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "message_start" => {
                if let Some(usage) = event.pointer("/message/usage") {
                    self.usage.prompt_tokens = number(usage.get("input_tokens"));
                    // Usually 1 here and restated by `message_delta`.
                    self.usage.completion_tokens = number(usage.get("output_tokens"));
                }
            }
            "content_block_start" => {
                let index = number(event.get("index")) as usize;
                let block = event.get("content_block");
                if block.and_then(|b| b.get("type")).and_then(Value::as_str) == Some("tool_use") {
                    let slot = slot(&mut self.calls, index);
                    slot.id = string(block.and_then(|b| b.get("id")));
                    slot.name = string(block.and_then(|b| b.get("name")));
                }
            }
            "content_block_delta" => {
                let index = number(event.get("index")) as usize;
                let delta = event.get("delta");
                match delta.and_then(|d| d.get("type")).and_then(Value::as_str) {
                    Some("text_delta") => {
                        self.content
                            .push_str(&string(delta.and_then(|d| d.get("text"))));
                    }
                    Some("input_json_delta") => {
                        let fragment = string(delta.and_then(|d| d.get("partial_json")));
                        slot(&mut self.calls, index).arguments.push_str(&fragment);
                    }
                    // `thinking_delta` / `signature_delta`: extended thinking is
                    // not enabled, and a trace is not an answer. Dropped.
                    _ => {}
                }
            }
            "message_delta" => {
                if let Some(reason) = event
                    .pointer("/delta/stop_reason")
                    .and_then(Value::as_str)
                    .filter(|reason| !reason.is_empty())
                {
                    self.stop_reason = reason.to_string();
                }
                if let Some(usage) = event.get("usage") {
                    let output = number(usage.get("output_tokens"));
                    if output > 0 {
                        self.usage.completion_tokens = output;
                    }
                    // A long run may restate the input count too.
                    let input = number(usage.get("input_tokens"));
                    if input > 0 {
                        self.usage.prompt_tokens = input;
                    }
                }
            }
            "message_stop" => return Ok(false),
            "error" => {
                let message = event
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("no message");
                let kind = event
                    .pointer("/error/type")
                    .and_then(Value::as_str)
                    .unwrap_or("error");
                return Err(format!(
                    "the provider sent a stream error ({kind}): {message}"
                ));
            }
            // `content_block_stop`, `ping`, and anything the API adds later.
            _ => {}
        }
        Ok(true)
    }

    pub fn finish(self) -> Turn {
        Turn {
            content: self.content,
            tool_calls: finish_calls(self.calls),
            finish_reason: finish_reason(&self.stop_reason),
            usage: self.usage,
        }
    }
}

/// `stop_reason` under the chat-completions name for the same thing, so the
/// store, `.agent show` and the run loop read one vocabulary.
fn finish_reason(stop_reason: &str) -> String {
    match stop_reason {
        "tool_use" => "tool_calls",
        "max_tokens" => "length",
        "end_turn" | "stop_sequence" => "stop",
        "" => "",
        other => other,
    }
    .to_string()
}

fn number(value: Option<&Value>) -> u64 {
    value.and_then(Value::as_u64).unwrap_or(0)
}

fn string(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chat::ToolCall;

    fn call(id: &str, name: &str, arguments: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            name: name.to_string(),
            arguments: arguments.to_string(),
        }
    }

    #[test]
    fn the_system_prompt_is_hoisted_out_of_the_messages() {
        let body = request_body(
            "claude",
            64,
            &[Message::system("you are an analyst"), Message::user("hi")],
            &[],
        );
        assert_eq!(body["system"], "you are an analyst");
        assert_eq!(body["max_tokens"], 64);
        assert_eq!(body["stream"], true);
        let messages = body["messages"].as_array().expect("messages");
        assert_eq!(messages.len(), 1, "the system message is not a message");
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"][0]["type"], "text");
        assert_eq!(messages[0]["content"][0]["text"], "hi");
        assert!(body.get("thinking").is_none(), "no extended thinking");
    }

    #[test]
    fn two_system_messages_join_and_no_system_omits_the_field() {
        let body = request_body(
            "claude",
            64,
            &[
                Message::system("one"),
                Message::system("two"),
                Message::user("hi"),
            ],
            &[],
        );
        assert_eq!(body["system"], "one\n\ntwo");
        let bare = request_body("claude", 64, &[Message::user("hi")], &[]);
        assert!(bare.get("system").is_none(), "{bare}");
    }

    #[test]
    fn a_tool_call_becomes_a_tool_use_block_with_parsed_input() {
        let body = request_body(
            "claude",
            64,
            &[
                Message::user("how many?"),
                Message::Assistant {
                    content: "let me look".to_string(),
                    tool_calls: vec![call("c1", "query", r#"{"sql":"SELECT 1"}"#)],
                },
            ],
            &[],
        );
        let blocks = body["messages"][1]["content"]
            .as_array()
            .expect("assistant blocks");
        assert_eq!(body["messages"][1]["role"], "assistant");
        assert_eq!(blocks[0]["type"], "text");
        assert_eq!(blocks[0]["text"], "let me look");
        assert_eq!(blocks[1]["type"], "tool_use");
        assert_eq!(blocks[1]["id"], "c1");
        assert_eq!(blocks[1]["name"], "query");
        assert_eq!(blocks[1]["input"]["sql"], "SELECT 1");
    }

    /// Arguments the model never finished writing would be rejected as a
    /// non-object input; an empty object keeps the request valid.
    #[test]
    fn unparsable_arguments_become_an_empty_input() {
        let body = request_body(
            "claude",
            64,
            &[
                Message::user("q"),
                Message::Assistant {
                    content: String::new(),
                    tool_calls: vec![call("c1", "query", r#"{"sql":"SELE"#)],
                },
            ],
            &[],
        );
        assert_eq!(body["messages"][1]["content"][0]["input"], json!({}));
    }

    #[test]
    fn tool_results_pair_by_id_merge_and_keep_the_roles_alternating() {
        let body = request_body(
            "claude",
            64,
            &[
                Message::system("s"),
                Message::user("q"),
                Message::Assistant {
                    content: String::new(),
                    tool_calls: vec![call("c1", "tables", "{}"), call("c2", "describe", "{}")],
                },
                Message::tool("c1", "| name |"),
                Message::tool("c2", "| column |"),
                Message::Assistant {
                    content: "done".to_string(),
                    tool_calls: Vec::new(),
                },
            ],
            &[],
        );
        let messages = body["messages"].as_array().expect("messages");
        let roles: Vec<&str> = messages
            .iter()
            .map(|m| m["role"].as_str().unwrap_or_default())
            .collect();
        assert_eq!(roles, ["user", "assistant", "user", "assistant"]);
        // Both results merged into the one user message between the turns.
        let results = messages[2]["content"].as_array().expect("result blocks");
        assert_eq!(results.len(), 2, "consecutive results are one message");
        assert_eq!(results[0]["type"], "tool_result");
        assert_eq!(results[0]["tool_use_id"], "c1");
        assert_eq!(results[0]["content"], "| name |");
        assert_eq!(results[1]["tool_use_id"], "c2");
        // Every result's id names a call of the turn before it.
        let calls: Vec<&str> = messages[1]["content"]
            .as_array()
            .expect("call blocks")
            .iter()
            .map(|b| b["id"].as_str().unwrap_or_default())
            .collect();
        for result in results {
            let id = result["tool_use_id"].as_str().unwrap_or_default();
            assert!(calls.contains(&id), "{id} answers no call of {calls:?}");
        }
    }

    /// Two user turns in a row (a follow-up question on a resumed session)
    /// must not reach the API as two user messages.
    #[test]
    fn consecutive_user_messages_merge() {
        let body = request_body(
            "claude",
            64,
            &[Message::user("first"), Message::user("second")],
            &[],
        );
        let messages = body["messages"].as_array().expect("messages");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["content"][0]["text"], "first");
        assert_eq!(messages[0]["content"][1]["text"], "second");
    }

    #[test]
    fn every_tool_goes_out_with_an_input_schema() {
        let tools = pinkdb_warehouse_tools::schemas();
        let body = request_body("claude", 64, &[Message::user("hi")], &tools);
        let sent = body["tools"].as_array().expect("a tools array");
        assert_eq!(sent.len(), tools.len());
        for (wire, tool) in sent.iter().zip(&tools) {
            assert_eq!(wire["name"], tool["name"]);
            assert_eq!(wire["description"], tool["description"]);
            assert_eq!(wire["input_schema"], tool["inputSchema"]);
            assert_eq!(wire["input_schema"]["type"], "object");
            assert!(wire.get("type").is_none(), "no function wrapper: {wire}");
        }
        // No tools at all means the field is absent, not an empty array.
        let bare = request_body("claude", 64, &[Message::user("hi")], &[]);
        assert!(bare.get("tools").is_none(), "{bare}");
    }

    fn feed(events: &[&str]) -> (Turn, bool) {
        let mut acc = Accumulator::default();
        let mut stopped = false;
        for event in events {
            if !acc.feed(event).expect("a valid event") {
                stopped = true;
                break;
            }
        }
        (acc.finish(), stopped)
    }

    #[test]
    fn a_text_answer_reports_usage_and_a_mapped_stop_reason() {
        let (turn, stopped) = feed(&[
            r#"{"type":"message_start","message":{"usage":{"input_tokens":11,"output_tokens":1}}}"#,
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            r#"{"type":"ping"}"#,
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"4 cus"}}"#,
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"tomers."}}"#,
            r#"{"type":"content_block_stop","index":0}"#,
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7}}"#,
            r#"{"type":"message_stop"}"#,
        ]);
        assert!(stopped, "message_stop ends the stream");
        assert_eq!(turn.content, "4 customers.");
        assert_eq!(turn.finish_reason, "stop");
        assert_eq!(turn.usage.prompt_tokens, 11);
        assert_eq!(turn.usage.completion_tokens, 7);
        assert!(turn.tool_calls.is_empty());
    }

    #[test]
    fn input_json_delta_fragments_rebuild_one_call_per_block() {
        let (turn, _) = feed(&[
            r#"{"type":"message_start","message":{"usage":{"input_tokens":20}}}"#,
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"query"}}"#,
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"sql\":"}}"#,
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"SELECT 1\"}"}}"#,
            r#"{"type":"content_block_stop","index":0}"#,
            r#"{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_2","name":"tables"}}"#,
            r#"{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{}"}}"#,
            r#"{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":9}}"#,
            r#"{"type":"message_stop"}"#,
        ]);
        assert_eq!(turn.finish_reason, "tool_calls");
        assert_eq!(turn.tool_calls.len(), 2);
        assert_eq!(turn.tool_calls[0].id, "toolu_1");
        assert_eq!(turn.tool_calls[0].name, "query");
        assert_eq!(turn.tool_calls[0].arguments, r#"{"sql":"SELECT 1"}"#);
        assert_eq!(turn.tool_calls[1].id, "toolu_2");
        assert_eq!(turn.tool_calls[1].arguments, "{}");
    }

    #[test]
    fn thinking_deltas_never_reach_the_answer() {
        let (turn, _) = feed(&[
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking"}}"#,
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"let me reason"}}"#,
            r#"{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"4"}}"#,
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"}}"#,
            r#"{"type":"message_stop"}"#,
        ]);
        assert_eq!(turn.content, "4");
        assert!(turn.tool_calls.is_empty(), "{:?}", turn.tool_calls);
    }

    #[test]
    fn a_stream_error_event_fails_the_request() {
        let mut acc = Accumulator::default();
        let error = acc
            .feed(r#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#)
            .expect_err("an error event");
        assert!(error.contains("overloaded_error"), "{error}");
        assert!(error.contains("Overloaded"), "{error}");
    }

    #[test]
    fn max_tokens_maps_onto_the_length_finish_reason() {
        let (turn, _) = feed(&[
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"a"}}"#,
            r#"{"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":8192}}"#,
            r#"{"type":"message_stop"}"#,
        ]);
        assert_eq!(turn.finish_reason, "length");
        assert_eq!(turn.usage.completion_tokens, 8192);
    }
}
