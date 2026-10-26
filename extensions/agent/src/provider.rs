//! The model provider: one OpenAI-compatible `/chat/completions` client.
//!
//! Blocking `ureq` with rustls — the crate and features `pinkdb-httpfs`
//! already locks — and a hand-parsed SSE stream, so the extension links no
//! async runtime and no provider SDK. The API key is read from the
//! environment when a run starts and lives only in this client: it is never
//! written to the store, a log line or an error message.

use std::fmt;

use serde_json::Value;

use crate::chat::{Accumulator, Message, Turn};
use crate::sse;

/// z.ai's OpenAI-compatible endpoint.
pub const DEFAULT_BASE_URL: &str = "https://api.z.ai/api/paas/v4/chat/completions";
/// z.ai's current flagship (GLM-5.3). `glm-5.3-flash` is the cheaper, faster
/// alternative through `PINKDB_AGENT_MODEL`.
pub const DEFAULT_MODEL: &str = "glm-5.3";
/// The key, in order of preference. `Z_API_KEY` is what z.ai's own CLI uses.
pub const KEY_VARIABLES: [&str; 2] = ["PINKDB_AGENT_API_KEY", "Z_API_KEY"];

/// Attempts per request: the first one plus two retries (250 ms, 500 ms).
const ATTEMPTS: u32 = 3;

/// Where to send a run and as whom: everything a provider needs, and nothing
/// the store may keep.
pub struct Config {
    pub base_url: String,
    pub model: String,
    api_key: String,
}

/// The key must never reach a `Debug` line, a snapshot or a panic message.
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("api_key", &"<redacted>")
            .finish()
    }
}

impl Config {
    /// Settings this process was handed directly, the way tests and a future
    /// server wiring would: same fields, no environment lookup.
    pub fn with_key(
        base_url: impl Into<String>,
        model: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Config {
        Config {
            base_url: base_url.into(),
            model: model.into(),
            api_key: api_key.into(),
        }
    }

    /// Read the provider settings from the environment.
    ///
    /// No `.env` file is ever opened: an operator exports the variables (or
    /// sources the file) before starting pinkdb.
    pub fn from_env() -> Result<Config, String> {
        let key = KEY_VARIABLES.iter().find_map(|name| {
            std::env::var(name)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        });
        let Some(api_key) = key else {
            return Err(format!(
                "no API key: export {} (or Z_API_KEY) before starting the agent",
                KEY_VARIABLES[0]
            ));
        };
        let setting = |name: &str, default: &str| {
            std::env::var(name)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| default.to_string())
        };
        Ok(Config {
            base_url: setting("PINKDB_AGENT_BASE_URL", DEFAULT_BASE_URL),
            model: setting("PINKDB_AGENT_MODEL", DEFAULT_MODEL),
            api_key,
        })
    }
}

/// One turn of the conversation, from wherever the harness gets it.
///
/// The loop takes a `&mut dyn Provider`, so its tests replay canned turns and
/// the mock HTTP server is tested against the real [`Client`].
pub trait Provider {
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
            // 4xx/5xx are read here: 401 and 429 have their own meanings.
            .http_status_as_error(false)
            .max_redirects(4)
            .user_agent(concat!("pinkdb-agent/", env!("CARGO_PKG_VERSION")))
            .build()
            .into();
        Client { agent, config }
    }

    /// The model name a run records in its transcript.
    pub fn model(&self) -> &str {
        &self.config.model
    }

    /// One request, retried on 429/5xx and on a stream that ends early.
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
        let request = ureq::http::Request::builder()
            .method("POST")
            .uri(&self.config.base_url)
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .header("authorization", format!("Bearer {}", self.config.api_key))
            .body(body.to_string())
            .map_err(|e| Retry::Stop(format!("could not build the request: {e}")))?;
        let mut response = self.agent.run(request).map_err(|e| {
            Retry::Stop(format!(
                "could not reach the model provider at {}: {e}",
                self.config.base_url
            ))
        })?;
        let status = response.status().as_u16();
        match status {
            401 => {
                return Err(Retry::Stop(
                    "the provider rejected the API key (HTTP 401); check \
                     PINKDB_AGENT_API_KEY"
                        .to_string(),
                ));
            }
            200..=299 => {}
            429 => {
                return Err(Retry::Wait(
                    "the provider is rate limiting (HTTP 429)".to_string(),
                ));
            }
            500..=599 => {
                return Err(Retry::Wait(format!(
                    "the provider answered HTTP {status} (a server error)"
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
                    "the provider answered HTTP {other}: {}",
                    shorten(detail.trim())
                )));
            }
        }
        let mut reader = std::io::BufReader::new(response.body_mut().as_reader());
        let mut accumulator = Accumulator::default();
        let streamed = sse::read_events(&mut reader, |data| {
            if data.trim() == "[DONE]" {
                return Ok(false);
            }
            accumulator.feed(data)?;
            Ok(true)
        });
        match streamed {
            Ok(()) => Ok(accumulator.finish()),
            Err(message) => Err(Retry::Wait(message)),
        }
    }
}

impl Provider for Client {
    fn model(&self) -> &str {
        self.model()
    }

    fn base_url(&self) -> &str {
        &self.config.base_url
    }

    fn complete(&mut self, messages: &[Message], tools: &[Value]) -> Result<Turn, String> {
        let body = crate::chat::request_body(&self.config.model, messages, tools);
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

    #[test]
    fn the_key_is_redacted_from_debug_output() {
        let config = Config {
            base_url: DEFAULT_BASE_URL.to_string(),
            model: DEFAULT_MODEL.to_string(),
            api_key: "sk-sentinel-never-print".to_string(),
        };
        let shown = format!("{config:?}");
        assert!(!shown.contains("sk-sentinel-never-print"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
    }

    #[test]
    fn defaults_are_one_constant_each() {
        assert_eq!(
            DEFAULT_BASE_URL,
            "https://api.z.ai/api/paas/v4/chat/completions"
        );
        assert_eq!(DEFAULT_MODEL, "glm-5.3");
        assert_eq!(KEY_VARIABLES, ["PINKDB_AGENT_API_KEY", "Z_API_KEY"]);
    }
}
