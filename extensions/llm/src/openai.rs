//! The OpenAI-compatible wire protocol: `POST <root>/chat/completions`.
//!
//! What z.ai, DeepSeek, OpenAI, OpenRouter, Gemini's compatibility endpoint
//! and Ollama all speak. The request is the transcript as `messages`, the
//! reply is an SSE stream of `choices[0].delta` chunks ended by `[DONE]`.

use serde_json::{Value, json};

use crate::chat::{Message, PartialCall, ToolCall, Turn, Usage, finish_calls, slot};

/// The path this protocol appends to a provider's base URL.
pub const PATH: &str = "/chat/completions";

/// The request body for one turn. `tools` are the shared MCP-shaped
/// definitions (`name`, `description`, `inputSchema`); chat completions wants
/// each wrapped as `{"type":"function","function":{…,"parameters":…}}`.
pub fn request_body(model: &str, messages: &[Message], tools: &[Value]) -> Value {
    json!({
        "model": model,
        "messages": messages.iter().map(message).collect::<Vec<_>>(),
        "tools": tools.iter().map(function_tool).collect::<Vec<_>>(),
        "tool_choice": "auto",
        "stream": true,
        "stream_options": {"include_usage": true},
    })
}

/// One transcript message as a chat-completions message.
fn message(message: &Message) -> Value {
    match message {
        Message::System(content) => json!({"role": "system", "content": content}),
        Message::User(content) => json!({"role": "user", "content": content}),
        Message::Assistant {
            content,
            tool_calls,
        } if tool_calls.is_empty() => json!({"role": "assistant", "content": content}),
        Message::Assistant {
            content,
            tool_calls,
        } => json!({
            "role": "assistant",
            "content": content,
            "tool_calls": tool_calls.iter().map(tool_call).collect::<Vec<_>>(),
        }),
        Message::Tool { call_id, content } => {
            json!({"role": "tool", "tool_call_id": call_id, "content": content})
        }
    }
}

fn tool_call(call: &ToolCall) -> Value {
    json!({
        "id": call.id,
        "type": "function",
        "function": {"name": call.name, "arguments": call.arguments},
    })
}

/// One MCP tool definition as a chat-completions function tool.
fn function_tool(tool: &Value) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": tool["name"],
            "description": tool["description"],
            "parameters": tool["inputSchema"],
        },
    })
}

/// Folds streamed chunks into one [`Turn`].
///
/// Tool-call arguments arrive split across chunks: each delta names its call
/// by `index`, and the fragments are concatenated in arrival order.
#[derive(Debug, Default)]
pub struct Accumulator {
    content: String,
    calls: Vec<PartialCall>,
    finish_reason: String,
    usage: Usage,
}

impl Accumulator {
    /// Feed one `data:` payload (already stripped of its `data: ` prefix).
    pub fn feed(&mut self, data: &str) -> Result<(), String> {
        let chunk: Value = serde_json::from_str(data)
            .map_err(|e| format!("the provider sent a chunk that is not JSON: {e}"))?;
        if let Some(usage) = chunk.get("usage").and_then(Value::as_object) {
            self.usage.prompt_tokens = number(usage.get("prompt_tokens"));
            self.usage.completion_tokens = number(usage.get("completion_tokens"));
        }
        let Some(choice) = chunk
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
        else {
            return Ok(());
        };
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
            self.finish_reason = reason.to_string();
        }
        let Some(delta) = choice.get("delta") else {
            return Ok(());
        };
        // `delta.reasoning_content` (z.ai GLM-5.x, DeepSeek's reasoners) is
        // deliberately not read. A reasoning trace is not an answer: folding it
        // into `content` would record it in the transcript and send it back on
        // the next turn, which the providers themselves tell you not to do.
        if let Some(text) = delta.get("content").and_then(Value::as_str) {
            self.content.push_str(text);
        }
        let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) else {
            return Ok(());
        };
        for call in calls {
            let index = call.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
            let slot = slot(&mut self.calls, index);
            if let Some(id) = call.get("id").and_then(Value::as_str)
                && !id.is_empty()
            {
                slot.id = id.to_string();
            }
            let Some(function) = call.get("function") else {
                continue;
            };
            if let Some(name) = function.get("name").and_then(Value::as_str)
                && !name.is_empty()
            {
                slot.name = name.to_string();
            }
            if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                slot.arguments.push_str(arguments);
            }
        }
        Ok(())
    }

    /// True when the payload is this protocol's end-of-stream marker.
    pub fn is_done(data: &str) -> bool {
        data.trim() == "[DONE]"
    }

    pub fn finish(self) -> Turn {
        Turn {
            content: self.content,
            tool_calls: finish_calls(self.calls),
            finish_reason: self.finish_reason,
            usage: self.usage,
        }
    }
}

fn number(value: Option<&Value>) -> u64 {
    value.and_then(Value::as_u64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_split_across_chunks_are_joined_per_call() {
        let mut acc = Accumulator::default();
        for chunk in [
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"query","arguments":"{\"sql\":\"SEL"}}]}}]}"#,
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ECT 1\"}"}}]}}]}"#,
            r#"{"choices":[{"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":11,"completion_tokens":7}}"#,
        ] {
            acc.feed(chunk).expect("a valid chunk");
        }
        let turn = acc.finish();
        assert_eq!(turn.finish_reason, "tool_calls");
        assert_eq!(turn.usage.total(), 18);
        assert_eq!(turn.tool_calls.len(), 1);
        assert_eq!(turn.tool_calls[0].name, "query");
        assert_eq!(turn.tool_calls[0].arguments, r#"{"sql":"SELECT 1"}"#);
    }

    #[test]
    fn a_plain_answer_is_content_with_a_finish_reason() {
        let mut acc = Accumulator::default();
        for chunk in [
            r#"{"choices":[{"delta":{"role":"assistant","content":"3 "}}]}"#,
            r#"{"choices":[{"delta":{"content":"customers."}}]}"#,
            r#"{"choices":[{"delta":{},"finish_reason":"stop"}]}"#,
        ] {
            acc.feed(chunk).expect("a valid chunk");
        }
        let turn = acc.finish();
        assert_eq!(turn.content, "3 customers.");
        assert_eq!(turn.finish_reason, "stop");
        assert!(turn.tool_calls.is_empty());
    }

    /// A reasoner streams its scratchpad as `reasoning_content`. It is not part
    /// of the answer, so it must not reach the turn (and through it the store,
    /// and through that the next request).
    #[test]
    fn reasoning_content_is_dropped() {
        let mut acc = Accumulator::default();
        for chunk in [
            r#"{"choices":[{"delta":{"reasoning_content":"let me think about SELECT"}}]}"#,
            r#"{"choices":[{"delta":{"content":"4"}}]}"#,
            r#"{"choices":[{"delta":{"reasoning_content":"more thinking"},"finish_reason":"stop"}]}"#,
        ] {
            acc.feed(chunk).expect("a valid chunk");
        }
        let turn = acc.finish();
        assert_eq!(turn.content, "4");
        assert!(!turn.content.contains("think"), "{:?}", turn.content);
    }

    #[test]
    fn a_missing_tool_call_id_gets_a_stable_address() {
        let mut acc = Accumulator::default();
        acc.feed(
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"tables"}}]}}]}"#,
        )
        .expect("a valid chunk");
        assert_eq!(acc.finish().tool_calls[0].id, "call_0");
    }

    #[test]
    fn request_body_declares_the_stream_and_its_usage() {
        let body = request_body("m", &[Message::user("hi")], &[]);
        assert_eq!(body["model"], "m");
        assert_eq!(body["stream"], true);
        assert_eq!(body["stream_options"]["include_usage"], true);
        assert_eq!(body["messages"][0]["role"], "user");
    }

    /// Every shared tool goes out as an OpenAI function tool: a provider
    /// refuses a bare MCP definition (z.ai: "tools[0].type cannot be empty").
    #[test]
    fn request_body_wraps_every_tool_as_a_function() {
        let tools = pinkdb_warehouse_tools::schemas();
        let body = request_body("m", &[Message::user("hi")], &tools);
        let sent = body["tools"].as_array().expect("a tools array");
        assert_eq!(sent.len(), tools.len());
        for (wire, tool) in sent.iter().zip(&tools) {
            assert_eq!(wire["type"], "function");
            assert_eq!(wire["function"]["name"], tool["name"]);
            assert_eq!(wire["function"]["description"], tool["description"]);
            assert_eq!(wire["function"]["parameters"], tool["inputSchema"]);
            assert_eq!(wire["function"]["parameters"]["type"], "object");
        }
    }

    /// A tool result is addressed by the id of the call it answers.
    #[test]
    fn a_tool_result_carries_its_call_id() {
        let body = request_body(
            "m",
            &[
                Message::Assistant {
                    content: String::new(),
                    tool_calls: vec![ToolCall {
                        id: "c1".to_string(),
                        name: "tables".to_string(),
                        arguments: "{}".to_string(),
                    }],
                },
                Message::tool("c1", "| name |"),
            ],
            &[],
        );
        assert_eq!(body["messages"][0]["tool_calls"][0]["id"], "c1");
        assert_eq!(body["messages"][1]["role"], "tool");
        assert_eq!(body["messages"][1]["tool_call_id"], "c1");
    }
}
