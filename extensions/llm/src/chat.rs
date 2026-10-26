//! The transcript, in the one shape every provider is encoded from.
//!
//! Only what a data agent needs: system/user/assistant/tool messages, tool
//! calls, the finish reason and token usage. Everything is plain data, so the
//! store can persist it and the tests can compare it — and because nothing
//! here is provider-specific, a session recorded against one provider can be
//! re-encoded for another. That is what makes a fork from z.ai onto Anthropic
//! a re-encode rather than a translation.
//!
//! The wire formats live next door, in [`crate::openai`] and
//! [`crate::anthropic`].

/// One message in the transcript.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    System(String),
    User(String),
    /// Assistant text, plus the tool calls it asked for (usually empty).
    Assistant {
        content: String,
        tool_calls: Vec<ToolCall>,
    },
    /// The result of one tool call, addressed by the call's id.
    Tool {
        call_id: String,
        content: String,
    },
}

impl Message {
    pub fn system(text: impl Into<String>) -> Message {
        Message::System(text.into())
    }

    pub fn user(text: impl Into<String>) -> Message {
        Message::User(text.into())
    }

    pub fn assistant(text: impl Into<String>) -> Message {
        Message::Assistant {
            content: text.into(),
            tool_calls: Vec::new(),
        }
    }

    pub fn tool(call_id: impl Into<String>, content: impl Into<String>) -> Message {
        Message::Tool {
            call_id: call_id.into(),
            content: content.into(),
        }
    }
}

/// One tool call the model asked for: `name` and its JSON `arguments` as the
/// model wrote them (a string, possibly split across stream chunks).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

/// Prompt and completion tokens reported by the provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

impl Usage {
    pub fn total(&self) -> u64 {
        self.prompt_tokens + self.completion_tokens
    }
}

/// One assistant turn: its text, the tool calls it asked for, why it stopped
/// and what it cost.
///
/// `finish_reason` is always one of the chat-completions names, whichever
/// protocol produced the turn, so the transcript means one thing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Turn {
    pub content: String,
    pub tool_calls: Vec<ToolCall>,
    pub finish_reason: String,
    pub usage: Usage,
}

/// A tool call being assembled from stream deltas, in either protocol: the id
/// and name arrive once, the arguments in any number of fragments.
#[derive(Debug, Clone, Default)]
pub(crate) struct PartialCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

/// Turn the partial calls of a finished stream into the turn's calls.
///
/// A call with no name was never started (a text-only block, or an index the
/// provider skipped) and is dropped.
pub(crate) fn finish_calls(calls: Vec<PartialCall>) -> Vec<ToolCall> {
    calls
        .into_iter()
        .filter(|call| !call.name.is_empty())
        .enumerate()
        .map(|(i, call)| ToolCall {
            // A provider that omits ids still needs a stable address.
            id: if call.id.is_empty() {
                format!("call_{i}")
            } else {
                call.id
            },
            name: call.name,
            arguments: call.arguments,
        })
        .collect()
}

/// The slot for `index`, grown as the stream names higher indices.
pub(crate) fn slot(calls: &mut Vec<PartialCall>, index: usize) -> &mut PartialCall {
    if calls.len() <= index {
        calls.resize(index + 1, PartialCall::default());
    }
    &mut calls[index]
}
