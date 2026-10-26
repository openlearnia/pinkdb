//! The OpenAI-compatible embeddings client: one request per batch, bounded
//! retries, and the cache in front of it.
//!
//! The provider contract used here is the one z.ai, OpenAI, DeepSeek and
//! local servers share: `POST <base_url>` with
//! `{"model", "input": [...], "dimensions"}`, answered by
//! `{"data": [{"index", "embedding": [...]}]}` and authenticated with
//! `Authorization: Bearer <key>`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::cache;

/// The width `embed()` declares when `PINKDB_EMBEDDINGS_DIMENSIONS` is unset.
///
/// There is no default provider or model: an operator names both
/// (`PINKDB_EMBEDDINGS_BASE_URL`, `PINKDB_EMBEDDINGS_MODEL`). z.ai's global
/// API, the agent's live-test provider, serves no embeddings model at all.
pub const DEFAULT_DIMENSIONS: u32 = 1024;
/// Inputs per request: enough to amortise a round trip, far inside any
/// provider's per-request input limit.
pub const DEFAULT_BATCH_SIZE: usize = 128;
/// The bearer token. Only this variable: a key exported for another provider
/// (the agent's, z.ai's) is never sent to whatever endpoint embeddings use.
pub const KEY_VARIABLE: &str = "PINKDB_EMBEDDINGS_API_KEY";

/// One initial attempt plus two retries, as the agent (wave 106) does.
const ATTEMPTS: u32 = 3;
const RETRY_BASE_MS: u64 = 250;
/// A batch of 128 wide vectors is a few MiB of JSON; the default body limit
/// would truncate it.
const RESPONSE_LIMIT: u64 = 64 * 1024 * 1024;

/// The endpoint, model, width and credentials of an embeddings provider.
///
/// The key never renders: `Debug` redacts it, and no error message quotes it.
#[derive(Clone)]
pub struct Config {
    pub base_url: String,
    pub model: String,
    pub dimensions: u32,
    pub batch_size: usize,
    pub cache_dir: Option<PathBuf>,
    pub timeout: Duration,
    api_key: String,
}

impl Config {
    /// Read the configuration and the key from the environment.
    ///
    /// No `.env` file is ever opened, and nothing is read from the store: an
    /// operator exports the variables before starting pinkdb.
    pub fn from_env() -> Result<Config, String> {
        let (Some(base_url), Some(model)) = (
            setting("PINKDB_EMBEDDINGS_BASE_URL"),
            setting("PINKDB_EMBEDDINGS_MODEL"),
        ) else {
            return Err(
                "no embeddings provider: export PINKDB_EMBEDDINGS_BASE_URL (an \
                        OpenAI-compatible /embeddings URL) and PINKDB_EMBEDDINGS_MODEL before \
                        calling embed()"
                    .to_string(),
            );
        };
        let dimensions = dimensions_from_env()?;
        let batch_size = match setting("PINKDB_EMBEDDINGS_BATCH_SIZE") {
            None => DEFAULT_BATCH_SIZE,
            Some(value) => value
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| {
                    "PINKDB_EMBEDDINGS_BATCH_SIZE must be a positive integer".to_string()
                })?,
        };
        let timeout = match setting("PINKDB_EMBEDDINGS_TIMEOUT_MS") {
            None => Duration::from_secs(30),
            Some(value) => Duration::from_millis(value.parse::<u64>().map_err(|_| {
                "PINKDB_EMBEDDINGS_TIMEOUT_MS must be a positive integer".to_string()
            })?),
        };
        let cache_dir = setting("PINKDB_EMBEDDINGS_CACHE_DIR").map(PathBuf::from);
        let api_key = setting(KEY_VARIABLE).ok_or_else(|| {
            format!("no embeddings API key: export {KEY_VARIABLE} before calling embed()")
        })?;
        Ok(Config {
            base_url,
            model,
            dimensions,
            batch_size,
            cache_dir,
            timeout,
            api_key,
        })
    }

    /// A configuration with explicit values, for tests.
    #[cfg(test)]
    pub fn with_key(
        base_url: impl Into<String>,
        model: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Config {
        Config {
            base_url: base_url.into(),
            model: model.into(),
            dimensions: DEFAULT_DIMENSIONS,
            batch_size: DEFAULT_BATCH_SIZE,
            cache_dir: None,
            timeout: Duration::from_secs(30),
            api_key: api_key.into(),
        }
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("dimensions", &self.dimensions)
            .field("batch_size", &self.batch_size)
            .field("cache_dir", &self.cache_dir)
            .field("timeout", &self.timeout)
            .field("api_key", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_renders_the_key() {
        let config = Config::with_key("http://127.0.0.1:9/embeddings", "m", "sk-secret-key");
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("sk-secret-key"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
    }
}

/// The declared width, shared by the package (which needs it at bind time)
/// and the client (which sends it as `dimensions`).
pub(crate) fn dimensions_from_env() -> Result<u32, String> {
    match setting("PINKDB_EMBEDDINGS_DIMENSIONS") {
        None => Ok(DEFAULT_DIMENSIONS),
        Some(value) => match value.parse::<u32>() {
            Ok(dimensions) if dimensions > 0 => Ok(dimensions),
            _ => Err("PINKDB_EMBEDDINGS_DIMENSIONS must be a positive integer".to_string()),
        },
    }
}

fn setting(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Why one request did not produce embeddings.
enum Retry {
    /// The provider answered something transient (429/5xx, a cut body).
    Wait(String),
    /// Retrying cannot help (401, a malformed request, a timeout).
    Stop(String),
}

/// A blocking client for one endpoint. It is built per evaluation, so a
/// changed environment takes effect on the next call.
#[derive(Debug)]
pub struct Client {
    agent: ureq::Agent,
    config: Config,
}

impl Client {
    pub fn new(config: Config) -> Client {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_connect(Some(Duration::from_secs(10)))
            .timeout_recv_response(Some(config.timeout))
            .timeout_recv_body(Some(config.timeout))
            // 4xx/5xx are read here: 401 and 429 have their own meanings.
            .http_status_as_error(false)
            .max_redirects(4)
            .user_agent(concat!("pinkdb-embeddings/", env!("CARGO_PKG_VERSION")))
            .build()
            .into();
        Client { agent, config }
    }

    /// Embed every input, in order.
    ///
    /// Repeated texts are sent once, a cached vector is not sent at all, and
    /// the misses go out in batches of [`Config::batch_size`], so `n` rows
    /// with `u` unique uncached texts cost `ceil(u / batch_size)` requests.
    pub fn embed(&self, inputs: &[String]) -> Result<Vec<Arc<[f32]>>, String> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        let dimensions = self.config.dimensions as usize;
        let mut resolved: Vec<Option<Arc<[f32]>>> = vec![None; inputs.len()];
        // Unique misses and the input positions waiting for each, in first-seen
        // order so the request's `index` maps back deterministically.
        let mut misses: Vec<(String, Vec<usize>)> = Vec::new();
        let mut seen: HashMap<&str, usize> = HashMap::new();
        for (position, text) in inputs.iter().enumerate() {
            if let Some(vector) = cache::get(&self.config, text)
                && vector.len() == dimensions
            {
                resolved[position] = Some(vector);
                continue;
            }
            match seen.get(text.as_str()) {
                Some(&slot) => misses[slot].1.push(position),
                None => {
                    seen.insert(text.as_str(), misses.len());
                    misses.push((text.clone(), vec![position]));
                }
            }
        }
        for batch in misses.chunks(self.config.batch_size) {
            let texts: Vec<&str> = batch.iter().map(|(text, _)| text.as_str()).collect();
            let vectors = self.request(&texts)?;
            for ((text, positions), values) in batch.iter().zip(vectors) {
                let values: Arc<[f32]> = values.into();
                cache::put(&self.config, text, values.clone());
                for &position in positions {
                    resolved[position] = Some(values.clone());
                }
            }
        }
        resolved
            .into_iter()
            .map(|vector| vector.ok_or_else(|| "internal: an input was not resolved".to_string()))
            .collect()
    }

    /// Send one batch, retrying transient failures.
    fn request(&self, inputs: &[&str]) -> Result<Vec<Vec<f32>>, String> {
        let body = json!({
            "model": self.config.model,
            "input": inputs,
            "dimensions": self.config.dimensions,
        })
        .to_string();
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.once(&body, inputs.len()) {
                Ok(vectors) => return Ok(vectors),
                Err(Retry::Stop(message)) => return Err(message),
                Err(Retry::Wait(message)) if attempt >= ATTEMPTS => return Err(message),
                Err(Retry::Wait(_)) => {
                    std::thread::sleep(Duration::from_millis(RETRY_BASE_MS << (attempt - 1)));
                }
            }
        }
    }

    /// One HTTP request and its response parsing.
    fn once(&self, body: &str, expected: usize) -> Result<Vec<Vec<f32>>, Retry> {
        let request = ureq::http::Request::builder()
            .method("POST")
            .uri(&self.config.base_url)
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .header("authorization", format!("Bearer {}", self.config.api_key))
            .body(body)
            .map_err(|e| Retry::Stop(format!("could not build the embeddings request: {e}")))?;
        let mut response = self.agent.run(request).map_err(|e| {
            Retry::Stop(format!(
                "could not reach the embeddings provider at {}: {e}",
                self.config.base_url
            ))
        })?;
        let status = response.status().as_u16();
        match status {
            200..=299 => {}
            401 => {
                return Err(Retry::Stop(
                    "the embeddings provider rejected the API key (HTTP 401); \
                     check PINKDB_EMBEDDINGS_API_KEY"
                        .to_string(),
                ));
            }
            429 => {
                return Err(Retry::Wait(
                    "the embeddings provider is rate limiting (HTTP 429)".to_string(),
                ));
            }
            500..=599 => {
                return Err(Retry::Wait(format!(
                    "the embeddings provider answered HTTP {status} (a server error)"
                )));
            }
            other => {
                let detail = response
                    .body_mut()
                    .with_config()
                    .limit(400)
                    .read_to_string()
                    .unwrap_or_default();
                return Err(Retry::Stop(format!(
                    "the embeddings provider answered HTTP {other}: {}",
                    shorten(detail.trim())
                )));
            }
        }
        let text = response
            .body_mut()
            .with_config()
            .limit(RESPONSE_LIMIT)
            .read_to_string()
            .map_err(|e| Retry::Wait(format!("could not read the embeddings response: {e}")))?;
        let parsed: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            Retry::Stop(format!(
                "the embeddings provider returned malformed JSON: {e}"
            ))
        })?;
        let data = parsed
            .get("data")
            .and_then(|data| data.as_array())
            .ok_or_else(|| {
                Retry::Stop("the embeddings provider returned no `data` array".to_string())
            })?;
        if data.len() != expected {
            return Err(Retry::Stop(format!(
                "the embeddings provider returned {} embeddings for {expected} inputs",
                data.len()
            )));
        }
        let dimensions = self.config.dimensions as usize;
        let mut vectors: Vec<Option<Vec<f32>>> = vec![None; expected];
        for (position, entry) in data.iter().enumerate() {
            let index = match entry.get("index") {
                None => position,
                Some(index) => index
                    .as_u64()
                    .and_then(|index| usize::try_from(index).ok())
                    .ok_or_else(|| {
                        Retry::Stop(
                            "the embeddings provider returned a non-integer index".to_string(),
                        )
                    })?,
            };
            if index >= expected || vectors[index].is_some() {
                return Err(Retry::Stop(format!(
                    "the embeddings provider returned a duplicate or out-of-range index {index}"
                )));
            }
            let embedding = entry
                .get("embedding")
                .and_then(|embedding| embedding.as_array())
                .ok_or_else(|| {
                    Retry::Stop(
                        "the embeddings provider returned an entry without `embedding`".to_string(),
                    )
                })?;
            if embedding.len() != dimensions {
                return Err(Retry::Stop(format!(
                    "the embeddings provider returned {} dimensions, but \
                     PINKDB_EMBEDDINGS_DIMENSIONS is {dimensions} (embed() is bound as \
                     FLOAT[{dimensions}]); set the variable to the provider's width and restart",
                    embedding.len()
                )));
            }
            let mut vector = Vec::with_capacity(dimensions);
            for value in embedding {
                let value = value.as_f64().ok_or_else(|| {
                    Retry::Stop(
                        "the embeddings provider returned a non-numeric embedding".to_string(),
                    )
                })?;
                vector.push(value as f32);
            }
            vectors[index] = Some(vector);
        }
        vectors
            .into_iter()
            .map(|vector| {
                vector.ok_or_else(|| {
                    Retry::Stop("the embeddings provider skipped an input".to_string())
                })
            })
            .collect()
    }
}

fn shorten(text: &str) -> String {
    let line: String = text.lines().next().unwrap_or_default().to_string();
    if line.chars().count() > 200 {
        line.chars().take(200).collect::<String>() + "…"
    } else {
        line
    }
}
