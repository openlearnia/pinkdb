//! The model client: one blocking HTTP request per turn, either protocol.
//!
//! Blocking `ureq` with rustls — the crate and features `pinkdb-httpfs`
//! already locks — and hand-parsed SSE, so an extension that links this crate
//! links no async runtime and no provider SDK. The API key is read from the
//! environment when a run starts and lives only in this client: it is never
//! written to a store, a log line or an error message, and it is only ever
//! sent to the provider whose own variable it came from.

use std::fmt;

use serde_json::Value;

use crate::chat::{Message, Turn};
use crate::registry::{Plan, Protocol};
use crate::sse;

/// Attempts per request: the first one plus two retries (250 ms, 500 ms).
const ATTEMPTS: u32 = 3;

/// Where to send a run and as whom: everything a provider needs, and nothing
/// the store may keep.
pub struct Config {
    plan: Plan,
    endpoint: String,
    api_key: String,
}

/// The key must never reach a `Debug` line, a snapshot or a panic message.
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("provider", &self.plan.provider)
            .field("protocol", &self.plan.protocol.name())
            .field("endpoint", &self.endpoint)
            .field("model", &self.plan.model)
            .field("api_key", &"<redacted>")
            .finish()
    }
}

impl Config {
    /// Settings this process was handed directly, the way the tests and a
    /// future server wiring do: a settled plan plus the key for it.
    pub fn with_key(plan: Plan, api_key: impl Into<String>) -> Config {
        Config {
            endpoint: plan.endpoint(),
            plan,
            api_key: api_key.into(),
        }
    }

    /// Settle a plan and read its provider's key from the environment.
    ///
    /// No `.env` file is ever opened: an operator exports the variables (or
    /// sources the file) before starting pinkdb.
    pub fn from_env(provider: Option<&str>, model: Option<&str>) -> Result<Config, String> {
        let plan = Plan::resolve(provider, model)?;
        let api_key = plan.key()?;
        Ok(Config::with_key(plan, api_key))
    }

    pub fn provider(&self) -> &'static str {
        self.plan.provider
    }

    pub fn protocol(&self) -> Protocol {
        self.plan.protocol
    }

    pub fn model(&self) -> &str {
        &self.plan.model
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

/// One turn of the conversation, from wherever the harness gets it.
///
/// The loop takes a `&mut dyn Provider`, so its tests replay canned turns and
/// the mock HTTP server is tested against the real [`Client`].
pub trait Provider {
    /// The registry name of the provider, for the transcript.
    fn provider(&self) -> &str {
        "unknown"
    }

    fn model(&self) -> &str;

    /// The endpoint, for the transcript (never the key).
    fn base_url(&self) -> &str {
        "unknown"
    }

    /// Ask for the next assistant turn.
    fn complete(&mut self, messages: &[Message], tools: &[Value]) -> Result<Turn, String>;
}

/// A blocking client for one endpoint.
#[derive(Debug)]
pub struct Client {
    agent: ureq::Agent,
    config: Config,
}

impl Client {
    pub fn new(config: Config) -> Client {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_connect(Some(std::time::Duration::from_secs(10)))
            .timeout_recv_response(Some(std::time::Duration::from_secs(60)))
            // Per read, not per stream: a long answer may stream for minutes.
            .timeout_recv_body(Some(std::time::Duration::from_secs(120)))
            // 4xx/5xx are read here: 401, 429 and 529 have their own meanings.
            .http_status_as_error(false)
            .max_redirects(4)
            .user_agent(concat!("pinkdb-llm/", env!("CARGO_PKG_VERSION")))
            .build()
            .into();
        Client { agent, config }
    }

    /// The model name a run records in its transcript.
    pub fn model(&self) -> &str {
        self.config.model()
    }

    /// One request, retried on 429, 5xx and 529, and on a stream that ends
    /// early.
    fn request(&self, body: &Value) -> Result<Turn, String> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.once(body) {
                Ok(turn) => return Ok(turn),
                Err(Retry::Wait(message)) if attempt < ATTEMPTS => {
                    eprintln!("pinkdb agent: {message}; retrying");
                    std::thread::sleep(std::time::Duration::from_millis(
                        250 * (1 << (attempt - 1)),
                    ));
                }
                Err(Retry::Wait(message)) => return Err(message),
                Err(Retry::Stop(message)) => return Err(message),
            }
        }
    }

    fn once(&self, body: &Value) -> Result<Turn, Retry> {
        let mut builder = ureq::http::Request::builder()
            .method("POST")
            .uri(&self.config.endpoint)
            .header("content-type", "application/json")
            .header("accept", "text/event-stream");
        // The key goes in the header its own protocol names and nowhere else.
        // An empty key (`ollama`, a keyless `custom` gateway) sends no header
        // at all rather than an empty credential.
        if !self.config.api_key.is_empty() {
            builder = match self.config.protocol() {
                Protocol::OpenAi => {
                    builder.header("authorization", format!("Bearer {}", self.config.api_key))
                }
                Protocol::Anthropic => builder.header("x-api-key", &self.config.api_key),
            };
        }
        if self.config.protocol() == Protocol::Anthropic {
            builder = builder.header("anthropic-version", crate::anthropic::VERSION);
        }
        let request = builder
            .body(body.to_string())
            .map_err(|e| Retry::Stop(format!("could not build the request: {e}")))?;
        let mut response = self.agent.run(request).map_err(|e| {
            Retry::Stop(format!(
                "could not reach the {} provider at {}: {e}",
                self.config.provider(),
                self.config.endpoint
            ))
        })?;
        let status = response.status().as_u16();
        match status {
            200..=299 => {}
            // A rejected credential is never retried, and the message names
            // the provider and the variable to look at — never the key.
            401 | 403 => {
                let entry = self.config.plan.entry();
                let hint = if entry.key_variables.is_empty() {
                    "this provider is sent no key".to_string()
                } else {
                    format!("check {}", entry.key_variables.join(" or "))
                };
                return Err(Retry::Stop(format!(
                    "the {} provider rejected the API key (HTTP {status}); {hint}",
                    self.config.provider()
                )));
            }
            429 => {
                return Err(Retry::Wait(format!(
                    "the {} provider is rate limiting (HTTP 429)",
                    self.config.provider()
                )));
            }
            // Anthropic's "overloaded": a server condition with a code outside
            // the 5xx range, so it has to be named next to it.
            529 => {
                return Err(Retry::Wait(format!(
                    "the {} provider is overloaded (HTTP 529)",
                    self.config.provider()
                )));
            }
            500..=599 => {
                return Err(Retry::Wait(format!(
                    "the {} provider answered HTTP {status} (a server error)",
                    self.config.provider()
                )));
            }
            other => {
                // The body explains a request error (a bad model name, an
                // unsupported parameter); it never contains the key.
                let detail = response
                    .body_mut()
                    .with_config()
                    .limit(400)
                    .read_to_string()
                    .unwrap_or_default();
                return Err(Retry::Stop(format!(
                    "the {} provider answered HTTP {other}: {}",
                    self.config.provider(),
                    shorten(detail.trim())
                )));
            }
        }
        let mut reader = std::io::BufReader::new(response.body_mut().as_reader());
        let streamed = match self.config.protocol() {
            Protocol::OpenAi => {
                let mut accumulator = crate::openai::Accumulator::default();
                let read = sse::read_events(&mut reader, |data| {
                    if crate::openai::Accumulator::is_done(data) {
                        return Ok(false);
                    }
                    accumulator.feed(data)?;
                    Ok(true)
                });
                read.map(|()| accumulator.finish())
            }
            Protocol::Anthropic => {
                let mut accumulator = crate::anthropic::Accumulator::default();
                let read = sse::read_events(&mut reader, |data| accumulator.feed(data));
                read.map(|()| accumulator.finish())
            }
        };
        // A stream that stopped early (or an `error` event) is retryable: the
        // run loop re-requests the step rather than recording half a turn.
        streamed.map_err(Retry::Wait)
    }
}

impl Provider for Client {
    fn provider(&self) -> &str {
        self.config.provider()
    }

    fn model(&self) -> &str {
        self.model()
    }

    fn base_url(&self) -> &str {
        &self.config.endpoint
    }

    fn complete(&mut self, messages: &[Message], tools: &[Value]) -> Result<Turn, String> {
        let body = match self.config.protocol() {
            Protocol::OpenAi => crate::openai::request_body(self.config.model(), messages, tools),
            Protocol::Anthropic => crate::anthropic::request_body(
                self.config.model(),
                self.config.plan.max_tokens,
                messages,
                tools,
            ),
        };
        self.request(&body)
    }
}

/// Why one request failed: retry it after a pause, or report it and stop.
enum Retry {
    Wait(String),
    Stop(String),
}

fn shorten(text: &str) -> String {
    let line: String = text.lines().next().unwrap_or_default().to_string();
    if line.chars().count() > 200 {
        line.chars().take(200).collect::<String>() + "…"
    } else {
        line
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(provider: &'static str, protocol: Protocol) -> Plan {
        Plan {
            provider,
            protocol,
            base_url: "http://127.0.0.1:1/v1".to_string(),
            model: "m".to_string(),
            max_tokens: 8192,
        }
    }

    #[test]
    fn the_key_is_redacted_from_debug_output() {
        let config = Config::with_key(plan("zai", Protocol::OpenAi), "sk-sentinel-never-print");
        let shown = format!("{config:?}");
        assert!(!shown.contains("sk-sentinel-never-print"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
        assert!(shown.contains("zai"), "{shown}");
    }

    #[test]
    fn each_protocol_posts_to_its_own_path() {
        assert_eq!(
            Config::with_key(plan("zai", Protocol::OpenAi), "k").endpoint(),
            "http://127.0.0.1:1/v1/chat/completions"
        );
        assert_eq!(
            Config::with_key(plan("anthropic", Protocol::Anthropic), "k").endpoint(),
            "http://127.0.0.1:1/v1/messages"
        );
    }
}
