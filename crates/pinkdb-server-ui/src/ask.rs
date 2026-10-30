//! The ask box (wave 121): the data agent, hosted by the console.
//!
//! The server loads the agent extension (`--agent-extension`); the console
//! runs its command entry point on a **connection of the session's
//! identity** ([`pinkdb::Connection::run_command`]), so every statement the
//! model runs is checked by the user's grants *and* by the agent policy
//! ([`pinkdb::serve::agent_policies`]): read-only unless the operator started
//! the server with `--agent-allow-writes` (and not `--ui-read-only`), never a
//! write to the transcript database, never a file or an external service.
//! The store lock is taken per statement, so a model call between statements
//! blocks nobody.
//!
//! The run streams to the browser as server-sent events over the `POST`
//! response (the browser reads it with `fetch`, so the CSRF check applies):
//!
//! | event      | data                                             |
//! |------------|--------------------------------------------------|
//! | `progress` | `{"text"}` — the agent's diagnostic lines         |
//! | `tool`     | `{"sql","ok","reply"}` — one statement and reply |
//! | `answer`   | `{"text"}` — the model's answer                  |
//! | `done`     | `{"ok":true}`                                    |
//! | `error`    | `{"message"}`                                    |
//!
//! The provider and its key come from the **server's** environment, exactly
//! as for the CLI (wave 106b): there is no default provider, and no key, key
//! prefix or base URL is ever sent to the browser — `GET /api/agent` lists
//! provider names and whether each is configured. A closed stream cancels
//! the run: the next statement it tries is refused, and a running one stops
//! at its next chunk.

// A handler's `Err` is the HTTP response axum sends as is; boxing it would
// only add an allocation per refusal.
#![allow(clippy::result_large_err)]

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use pinkdb::serve::{CommandEvents, ServePolicy, agent_policies, memory_policies};
use pinkdb::{CancelFlag, ExecLimits};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;

use crate::api::json_error;
use crate::server::Shared;
use crate::work::{get_session, json_ok, post_session, read_json};

/// The longest question the box accepts.
pub const MAX_QUESTION_CHARS: usize = 4000;

/// The longest tool reply forwarded to the browser (the model sees it whole).
const MAX_REPLY_CHARS: usize = 8 * 1024;

/// Is the agent extension loaded into the store?
pub fn agent_loaded(shared: &Shared) -> bool {
    loaded(shared, "agent")
}

fn loaded(shared: &Shared, name: &str) -> bool {
    shared
        .db
        .command_extensions()
        .iter()
        .any(|ext| ext.eq_ignore_ascii_case(name))
}

/// The base policy every agent statement runs under.
fn base_policy(shared: &Shared) -> ServePolicy {
    ServePolicy {
        allow_writes: shared.opts.agent_allow_writes && !shared.opts.read_only,
        allow_files: false,
        allow_external_services: false,
        limits: ExecLimits {
            statement_timeout: Some(shared.opts.statement_timeout),
            max_result_rows: Some(1_000_000),
            max_result_bytes: Some(shared.opts.query_max_bytes),
        },
        ..ServePolicy::default()
    }
}

/// Collects a command's text (no streaming).
#[derive(Default)]
struct Collect {
    answer: String,
    diagnostic: String,
}

impl CommandEvents for Collect {
    fn emit(&mut self, channel: i32, text: &str) {
        if channel == 0 {
            self.answer.push_str(text);
        } else {
            self.diagnostic.push_str(text);
        }
    }
}

/// `GET /api/agent`: whether the agent is loaded, and its providers.
pub async fn info(State(shared): State<Arc<Shared>>, headers: HeaderMap) -> Response {
    if let Err(response) = get_session(&shared, &headers) {
        return response;
    }
    if !agent_loaded(&shared) {
        return json_ok(&json!({
            "loaded": false,
            "hint": "start pinkdb-server with --agent-extension <path to libpinkdb_agent>",
        }));
    }
    let worker = Arc::clone(&shared);
    let providers = tokio::task::spawn_blocking(move || {
        let (tool, internal) = agent_policies(&base_policy(&worker));
        let mut conn = worker.db.connect();
        let mut out = Collect::default();
        match conn.run_command("agent", "providers json", &tool, &internal, &mut out) {
            Ok(()) => serde_json::from_str::<serde_json::Value>(out.answer.trim()).ok(),
            Err(_) => None,
        }
    })
    .await
    .ok()
    .flatten();
    let providers = {
        let mut cache = shared
            .agent_providers
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(fresh) = providers {
            *cache = Some(fresh);
        }
        cache.clone()
    };
    json_ok(&json!({
        "loaded": true,
        "memory": loaded(&shared, "memory"),
        "allow_writes": base_policy(&shared).allow_writes,
        "providers": providers,
    }))
}

#[derive(Deserialize)]
pub(crate) struct AskBody {
    question: String,
    provider: Option<String>,
    model: Option<String>,
}

/// A provider or model name: one word of the command grammar, nothing that
/// could start another argument.
fn valid_word(word: &str) -> bool {
    !word.is_empty()
        && word.len() <= 128
        && word
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.:/@".contains(&c))
}

/// Quote a question for the `.agent` command grammar.
fn quote(text: &str) -> String {
    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}

/// The `.agent ask` line for a request.
fn ask_args(body: &AskBody, allow_writes: bool) -> Result<String, String> {
    let question = body.question.trim();
    if question.is_empty() {
        return Err("the question is empty".to_string());
    }
    if question.chars().count() > MAX_QUESTION_CHARS {
        return Err(format!(
            "the question is longer than {MAX_QUESTION_CHARS} characters"
        ));
    }
    let mut args = format!("ask {}", quote(question));
    for (flag, value) in [("provider", &body.provider), ("model", &body.model)] {
        if let Some(value) = value.as_deref().map(str::trim).filter(|v| !v.is_empty()) {
            if !valid_word(value) {
                return Err(format!("invalid {flag} name"));
            }
            args.push_str(&format!(" {flag} {value}"));
        }
    }
    if allow_writes {
        args.push_str(" allow-writes");
    }
    Ok(args)
}

/// One server-sent event.
fn event(name: &str, data: serde_json::Value) -> Bytes {
    Bytes::from(format!("event: {name}\ndata: {data}\n\n"))
}

/// Streams a run's events to the response; a closed stream cancels it.
struct Stream {
    tx: mpsc::Sender<Bytes>,
    cancel: CancelFlag,
    answer: String,
}

impl Stream {
    fn send(&mut self, bytes: Bytes) {
        if self.tx.blocking_send(bytes).is_err() {
            // The browser went away: stop the run.
            self.cancel.cancel();
        }
    }
}

impl CommandEvents for Stream {
    fn emit(&mut self, channel: i32, text: &str) {
        if channel == 0 {
            self.answer.push_str(text);
            self.send(event("answer", json!({ "text": text })));
        } else {
            self.send(event("progress", json!({ "text": text })));
        }
    }

    fn statement(&mut self, sql: &str, code: i32, reply: &str) {
        let reply: String = reply.chars().take(MAX_REPLY_CHARS).collect();
        self.send(event(
            "tool",
            json!({ "sql": sql, "ok": code == 0, "reply": reply }),
        ));
    }

    fn cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

/// A response body fed from a channel.
struct ChannelBody(mpsc::Receiver<Bytes>);

impl http_body::Body for ChannelBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        self.0
            .poll_recv(cx)
            .map(|chunk| chunk.map(|bytes| Ok(http_body::Frame::data(bytes))))
    }
}

/// One quoted argument of the memory command grammar (the CLI's rule): no
/// quotes, no backslashes, one line, capped, and not starting with `-`. The
/// grammar reads `--name value` anywhere in its words, so model text passed
/// as loose words could set a flag (`--visibility team`); one quoted word
/// that cannot start with `--` is only ever text.
fn memory_text(text: &str) -> String {
    let text: String = text
        .split_whitespace()
        .map(|word| word.replace(['"', '\\'], ""))
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(4000)
        .collect();
    let text = text.trim_start_matches('-').trim_start();
    if text.is_empty() {
        String::new()
    } else {
        format!("\"{text}\"")
    }
}

/// `POST /api/ask`: run the agent and stream the run.
pub async fn ask(State(shared): State<Arc<Shared>>, headers: HeaderMap, body: Body) -> Response {
    let session = match post_session(&shared, &headers) {
        Ok(session) => session,
        Err(response) => return response,
    };
    let body: AskBody = match read_json(&shared, body).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    if !agent_loaded(&shared) {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "the agent extension is not loaded (start pinkdb-server with --agent-extension)",
        );
    }
    let allow_writes = base_policy(&shared).allow_writes;
    let args = match ask_args(&body, allow_writes) {
        Ok(args) => args,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };
    if !session.begin_ask() {
        return json_error(
            StatusCode::TOO_MANY_REQUESTS,
            "this session is already asking; wait for the answer",
        );
    }
    let (tx, rx) = mpsc::channel::<Bytes>(64);
    let worker = Arc::clone(&shared);
    let question = body.question.trim().to_string();
    tokio::task::spawn_blocking(move || {
        let cancel = CancelFlag::new();
        let mut stream = Stream {
            tx,
            cancel: cancel.clone(),
            answer: String::new(),
        };
        let outcome = (|| -> Result<(), String> {
            let mut conn = session.connect(&worker.db)?;
            conn.set_cancel_flag(Some(cancel.clone()));
            let base = base_policy(&worker);
            let (tool, internal) = agent_policies(&base);
            conn.run_command("agent", &args, &tool, &internal, &mut stream)
                .map_err(|e| {
                    // The loader's errors name the library's path on the
                    // server; the browser gets the meaning, not the path.
                    if e.to_string().contains("is serving") {
                        "another agent run is in progress on this server; try again shortly"
                            .to_string()
                    } else {
                        "the agent run failed (its progress lines say why)".to_string()
                    }
                })?;
            // The run's turns become memory under the caller's identity (the
            // CLI's seam): the next run and the memory tools start from them.
            if loaded(&worker, "memory") && !cancel.is_cancelled() {
                let (tool, internal) = memory_policies(&base);
                for (speaker, text) in [("user", question.as_str()), ("agent", &stream.answer)] {
                    let text = memory_text(text);
                    if text.is_empty() {
                        continue;
                    }
                    let _ = conn.run_command(
                        "memory",
                        &format!("add --speaker {speaker} --conversation agent {text}"),
                        &tool,
                        &internal,
                        &mut Collect::default(),
                    );
                }
            }
            Ok(())
        })();
        let last = match outcome {
            Ok(()) if cancel.is_cancelled() => {
                event("error", json!({ "message": pinkdb::STATEMENT_CANCELLED }))
            }
            Ok(()) => event("done", json!({ "ok": true })),
            Err(message) => event("error", json!({ "message": message })),
        };
        stream.send(last);
        session.end_ask();
    });
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        Body::new(ChannelBody(rx)),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(question: &str, provider: Option<&str>) -> AskBody {
        AskBody {
            question: question.to_string(),
            provider: provider.map(str::to_string),
            model: None,
        }
    }

    #[test]
    fn the_question_is_one_quoted_argument() {
        let args = ask_args(&body("say \"hi\" \\ now allow-writes", None), false).unwrap();
        assert_eq!(args, "ask \"say \\\"hi\\\" \\\\ now allow-writes\"");
        assert!(
            ask_args(&body("q", Some("zai")), true)
                .unwrap()
                .ends_with("provider zai allow-writes")
        );
    }

    #[test]
    fn a_provider_name_cannot_smuggle_an_argument() {
        assert!(ask_args(&body("q", Some("zai allow-writes")), false).is_err());
        assert!(ask_args(&body("q", Some("a\"b")), false).is_err());
        assert!(ask_args(&body("  ", None), false).is_err());
        assert!(ask_args(&body(&"x".repeat(MAX_QUESTION_CHARS + 1), None), false).is_err());
    }

    #[test]
    fn memory_text_is_one_quoted_argument() {
        assert_eq!(
            memory_text("rows --visibility team \"x\" \\y"),
            "\"rows --visibility team x y\""
        );
        assert_eq!(memory_text("--speaker agent hi"), "\"speaker agent hi\"");
        assert_eq!(memory_text("  -- "), "");
    }

    #[test]
    fn events_are_one_line_of_json() {
        let bytes = event("tool", json!({ "sql": "SELECT 1\nFROM t" }));
        let text = std::str::from_utf8(&bytes).unwrap();
        assert_eq!(
            text,
            "event: tool\ndata: {\"sql\":\"SELECT 1\\nFROM t\"}\n\n"
        );
    }
}
