//! The built-in provider table and how a run picks one out of it.
//!
//! **There is no default provider.** Every run names one, by argument or by
//! `PINKDB_AGENT_PROVIDER`; no provider is an error, never a fallback. The
//! reason is the key rule below: if the harness guessed a provider from
//! whichever key happened to be exported, a key would eventually be sent to an
//! endpoint its owner never chose.
//!
//! **Key isolation.** A provider's key comes only from that provider's own
//! variables ([`Entry::key_variables`]) and goes only to that provider's base
//! URL. `ANTHROPIC_API_KEY` cannot reach z.ai, a z.ai key cannot reach
//! OpenRouter, and `PINKDB_AGENT_API_KEY` belongs to `custom` alone.
//!
//! The root follows the same rule: each provider's root is overridden only by
//! its own variable ([`Entry::base_url_variable`]). A shared override would
//! send `ANTHROPIC_API_KEY` to whatever host was exported for `custom` last
//! week.

use std::fmt;

/// Which wire protocol a provider speaks ([`crate::openai`],
/// [`crate::anthropic`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    OpenAi,
    Anthropic,
}

impl Protocol {
    /// The name in the table, in `PINKDB_AGENT_PROTOCOL` and in `.agent
    /// providers`.
    pub fn name(&self) -> &'static str {
        match self {
            Protocol::OpenAi => "openai",
            Protocol::Anthropic => "anthropic",
        }
    }

    /// What this protocol appends to a provider's base URL.
    pub fn path(&self) -> &'static str {
        match self {
            Protocol::OpenAi => crate::openai::PATH,
            Protocol::Anthropic => crate::anthropic::PATH,
        }
    }

    fn parse(name: &str) -> Result<Protocol, String> {
        match name {
            "openai" => Ok(Protocol::OpenAi),
            "anthropic" => Ok(Protocol::Anthropic),
            other => Err(format!(
                "unknown protocol {other:?}: PINKDB_AGENT_PROTOCOL is openai or anthropic"
            )),
        }
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// One row of the provider table.
pub struct Entry {
    pub name: &'static str,
    pub protocol: Protocol,
    /// The API **root**: the protocol appends its own path. `""` when the
    /// root must come from [`Entry::base_url_variable`].
    pub base_url: &'static str,
    /// The one variable that overrides this provider's root (a proxy, a
    /// gateway, a mock in a test). No other provider reads it.
    pub base_url_variable: &'static str,
    /// This provider's key variables, in order of preference. Empty means the
    /// provider takes no authorization header at all.
    pub key_variables: &'static [&'static str],
    /// `""` when the provider has no model this wave can verify: the run must
    /// name one.
    pub default_model: &'static str,
    /// False for a provider that may be reached without a key (a local
    /// gateway). An empty `key_variables` implies it.
    pub key_required: bool,
}

/// The provider `custom` reads its protocol from.
pub const PROTOCOL_VARIABLE: &str = "PINKDB_AGENT_PROTOCOL";
/// `custom`'s API root, which it requires; every built-in provider ignores it.
pub const BASE_URL_VARIABLE: &str = "PINKDB_AGENT_BASE_URL";
/// Names the provider when a run does not.
pub const PROVIDER_VARIABLE: &str = "PINKDB_AGENT_PROVIDER";
/// Names the model when a run does not.
pub const MODEL_VARIABLE: &str = "PINKDB_AGENT_MODEL";
/// Overrides Anthropic's required `max_tokens`.
pub const MAX_TOKENS_VARIABLE: &str = "PINKDB_AGENT_MAX_TOKENS";

/// The built-in providers.
///
/// A base URL is a root, never an endpoint. A model is only listed when it is
/// a name this wave could verify; the rest demand one from the run, because an
/// invented model name fails at the provider with a worse message.
pub const PROVIDERS: &[Entry] = &[
    Entry {
        name: "zai",
        protocol: Protocol::OpenAi,
        base_url: "https://api.z.ai/api/paas/v4",
        base_url_variable: "PINKDB_AGENT_ZAI_BASE_URL",
        // `Z_API_KEY` is what z.ai's own CLI exports.
        key_variables: &["ZAI_API_KEY", "Z_API_KEY"],
        default_model: "glm-5.3",
        key_required: true,
    },
    Entry {
        name: "anthropic",
        protocol: Protocol::Anthropic,
        base_url: "https://api.anthropic.com/v1",
        base_url_variable: "PINKDB_AGENT_ANTHROPIC_BASE_URL",
        key_variables: &["ANTHROPIC_API_KEY"],
        default_model: "claude-sonnet-5-5",
        key_required: true,
    },
    Entry {
        name: "openai",
        protocol: Protocol::OpenAi,
        base_url: "https://api.openai.com/v1",
        base_url_variable: "PINKDB_AGENT_OPENAI_BASE_URL",
        key_variables: &["OPENAI_API_KEY"],
        default_model: "",
        key_required: true,
    },
    Entry {
        name: "deepseek",
        protocol: Protocol::OpenAi,
        base_url: "https://api.deepseek.com/v1",
        base_url_variable: "PINKDB_AGENT_DEEPSEEK_BASE_URL",
        key_variables: &["DEEPSEEK_API_KEY"],
        default_model: "",
        key_required: true,
    },
    Entry {
        name: "openrouter",
        protocol: Protocol::OpenAi,
        base_url: "https://openrouter.ai/api/v1",
        base_url_variable: "PINKDB_AGENT_OPENROUTER_BASE_URL",
        key_variables: &["OPENROUTER_API_KEY"],
        default_model: "",
        key_required: true,
    },
    Entry {
        name: "gemini",
        protocol: Protocol::OpenAi,
        base_url: "https://generativelanguage.googleapis.com/v1beta/openai",
        base_url_variable: "PINKDB_AGENT_GEMINI_BASE_URL",
        key_variables: &["GEMINI_API_KEY"],
        default_model: "",
        key_required: true,
    },
    Entry {
        name: "ollama",
        protocol: Protocol::OpenAi,
        base_url: "http://127.0.0.1:11434/v1",
        base_url_variable: "PINKDB_AGENT_OLLAMA_BASE_URL",
        // A local server: no authorization header is sent at all.
        key_variables: &[],
        default_model: "",
        key_required: false,
    },
    Entry {
        name: "custom",
        protocol: Protocol::OpenAi,
        // Required, from BASE_URL_VARIABLE: there is nothing to default to.
        base_url: "",
        base_url_variable: BASE_URL_VARIABLE,
        key_variables: &["PINKDB_AGENT_API_KEY"],
        default_model: "",
        key_required: false,
    },
];

impl Entry {
    /// This provider's root: its own override variable, else its default.
    /// `None` only for a provider with no default whose variable is unset.
    pub fn base_url_with(&self, lookup: Lookup<'_>) -> Option<String> {
        lookup(self.base_url_variable)
            .or_else(|| (!self.base_url.is_empty()).then(|| self.base_url.to_string()))
    }
}

/// The provider of this name, if the table has one.
pub fn provider(name: &str) -> Option<&'static Entry> {
    PROVIDERS.iter().find(|entry| entry.name == name)
}

/// Every provider name, for an error message or `.agent providers`.
pub fn names() -> String {
    PROVIDERS
        .iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>()
        .join(", ")
}

/// A provider and model a run settled on: everything but the key.
///
/// Resolution is two stages because `.agent fork` has to *record* a provider
/// without being able to call it: a fork with no new question needs no key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub provider: &'static str,
    pub protocol: Protocol,
    pub base_url: String,
    pub model: String,
    pub max_tokens: u64,
}

/// How the resolver reads the environment.
///
/// The real one is [`env`]; the tests hand in a fixed table, so precedence is
/// pinned without touching (or racing on) the process environment.
pub type Lookup<'a> = &'a dyn Fn(&str) -> Option<String>;

impl Plan {
    /// Settle on a provider and a model, reading the environment.
    ///
    /// `provider` and `model` are the run's arguments; each falls back to its
    /// environment variable, and the provider has no further fallback.
    pub fn resolve(provider: Option<&str>, model: Option<&str>) -> Result<Plan, String> {
        Plan::resolve_with(provider, model, &env)
    }

    /// [`Plan::resolve`] against a given environment.
    pub fn resolve_with(
        provider: Option<&str>,
        model: Option<&str>,
        lookup: Lookup<'_>,
    ) -> Result<Plan, String> {
        let env = lookup;
        let name = provider
            .map(str::to_string)
            .or_else(|| env(PROVIDER_VARIABLE))
            .ok_or_else(|| {
                format!(
                    "no provider: pass provider <name> or set {PROVIDER_VARIABLE} (one of: {})",
                    names()
                )
            })?;
        let entry = self::provider(&name).ok_or_else(|| {
            format!(
                "unknown provider {name:?}: the built-in providers are {}",
                names()
            )
        })?;
        let protocol = match (entry.name, env(PROTOCOL_VARIABLE)) {
            // Only `custom` has no fixed protocol; for a built-in provider the
            // variable is ignored rather than allowed to misencode a request.
            ("custom", Some(name)) => Protocol::parse(&name)?,
            _ => entry.protocol,
        };
        let base_url = entry.base_url_with(env).ok_or_else(|| {
            format!(
                "provider {} needs an API root: set {}",
                entry.name, entry.base_url_variable
            )
        })?;
        let model = model
            .map(str::to_string)
            .or_else(|| env(MODEL_VARIABLE))
            .or_else(|| (!entry.default_model.is_empty()).then(|| entry.default_model.to_string()))
            .ok_or_else(|| {
                format!(
                    "provider {} has no default model: pass model <name> on the run or set \
                     {MODEL_VARIABLE}",
                    entry.name
                )
            })?;
        let max_tokens = match env(MAX_TOKENS_VARIABLE) {
            Some(value) => value
                .parse()
                .map_err(|_| format!("{MAX_TOKENS_VARIABLE}={value:?} is not a number"))?,
            None => crate::anthropic::DEFAULT_MAX_TOKENS,
        };
        if max_tokens == 0 {
            return Err(format!("{MAX_TOKENS_VARIABLE} must be at least 1"));
        }
        Ok(Plan {
            provider: entry.name,
            protocol,
            base_url,
            model,
            max_tokens,
        })
    }

    /// The table row this plan came from.
    pub fn entry(&self) -> &'static Entry {
        provider(self.provider).expect("a plan names a built-in provider")
    }

    /// The endpoint this plan posts to.
    pub fn endpoint(&self) -> String {
        format!(
            "{}{}",
            self.base_url.trim_end_matches('/'),
            self.protocol.path()
        )
    }

    /// Read this provider's key — and only this provider's key.
    pub fn key(&self) -> Result<String, String> {
        self.key_with(&env)
    }

    /// [`Plan::key`] against a given environment.
    pub fn key_with(&self, lookup: Lookup<'_>) -> Result<String, String> {
        let entry = self.entry();
        let key = entry.key_variables.iter().find_map(|name| lookup(name));
        match key {
            Some(key) => Ok(key),
            None if !entry.key_required => Ok(String::new()),
            None => Err(format!(
                "no API key for provider {}: export {} before starting the agent",
                entry.name,
                entry.key_variables.join(" or ")
            )),
        }
    }
}

/// One environment variable, trimmed, empty treated as unset.
///
/// No `.env` file is ever opened: an operator exports the variables (or
/// sources the file) before starting pinkdb.
pub fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Whether a provider's key is available, for `.agent providers`. Never the
/// key, never a prefix of it, never its length.
pub fn key_state(entry: &Entry) -> &'static str {
    key_state_with(entry, &env)
}

/// [`key_state`] against a given environment.
pub fn key_state_with(entry: &Entry, lookup: Lookup<'_>) -> &'static str {
    if entry.key_variables.is_empty() {
        "no key needed"
    } else if entry
        .key_variables
        .iter()
        .any(|name| lookup(name).is_some())
    {
        "key: set"
    } else if entry.key_required {
        "key: missing"
    } else {
        "key: missing (optional)"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_table_is_the_one_in_the_plan() {
        assert_eq!(
            names(),
            "zai, anthropic, openai, deepseek, openrouter, gemini, ollama, custom"
        );
        let zai = provider("zai").expect("zai");
        assert_eq!(zai.protocol, Protocol::OpenAi);
        assert_eq!(zai.base_url, "https://api.z.ai/api/paas/v4");
        assert_eq!(zai.key_variables, ["ZAI_API_KEY", "Z_API_KEY"]);
        assert_eq!(zai.default_model, "glm-5.3");
        let anthropic = provider("anthropic").expect("anthropic");
        assert_eq!(anthropic.protocol, Protocol::Anthropic);
        assert_eq!(anthropic.key_variables, ["ANTHROPIC_API_KEY"]);
        assert_eq!(anthropic.default_model, "claude-sonnet-5-5");
        assert!(provider("ollama").expect("ollama").key_variables.is_empty());
        assert!(provider("z.ai").is_none());
    }

    /// The security rule of this wave: no provider lists another's variable,
    /// and `PINKDB_AGENT_API_KEY` belongs to `custom` alone.
    #[test]
    fn no_key_variable_is_shared_between_providers() {
        let mut seen: Vec<(&str, &str)> = Vec::new();
        for entry in PROVIDERS {
            for variable in entry.key_variables {
                if let Some((owner, _)) = seen.iter().find(|(_, name)| name == variable) {
                    panic!("{variable} is read for both {owner} and {}", entry.name);
                }
                seen.push((entry.name, variable));
            }
        }
        assert_eq!(
            seen.iter()
                .find(|(_, name)| *name == "PINKDB_AGENT_API_KEY")
                .map(|(owner, _)| *owner),
            Some("custom")
        );
    }

    /// A root plus the protocol's path, with a stray trailing slash tolerated.
    #[test]
    fn an_endpoint_is_the_root_plus_the_protocol_path() {
        for (base, protocol, expected) in [
            (
                "https://api.z.ai/api/paas/v4",
                Protocol::OpenAi,
                "https://api.z.ai/api/paas/v4/chat/completions",
            ),
            (
                "https://api.anthropic.com/v1/",
                Protocol::Anthropic,
                "https://api.anthropic.com/v1/messages",
            ),
        ] {
            let plan = Plan {
                provider: "zai",
                protocol,
                base_url: base.to_string(),
                model: "m".to_string(),
                max_tokens: 1,
            };
            assert_eq!(plan.endpoint(), expected);
        }
    }

    /// An environment of exactly these variables.
    fn environment(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
        }
    }

    fn resolve(
        provider: Option<&str>,
        model: Option<&str>,
        pairs: &[(&str, &str)],
    ) -> Result<Plan, String> {
        Plan::resolve_with(provider, model, &environment(pairs))
    }

    #[test]
    fn no_provider_anywhere_is_an_error_even_with_a_key_exported() {
        let error = resolve(None, None, &[("ANTHROPIC_API_KEY", "sk-x")])
            .expect_err("a key is not a provider");
        assert!(error.starts_with("no provider:"), "{error}");
        assert!(error.contains("provider <name>"), "{error}");
        assert!(error.contains(PROVIDER_VARIABLE), "{error}");
        assert!(error.contains("zai, anthropic"), "{error}");
        // The key that was set is not echoed, and no provider was inferred.
        assert!(!error.contains("sk-x"), "{error}");
    }

    #[test]
    fn a_run_argument_beats_the_environment() {
        let plan = resolve(
            Some("anthropic"),
            Some("claude-x"),
            &[(PROVIDER_VARIABLE, "zai"), (MODEL_VARIABLE, "glm-x")],
        )
        .expect("the argument wins");
        assert_eq!(plan.provider, "anthropic");
        assert_eq!(plan.model, "claude-x");
        // With no argument the environment is what is left.
        let plan = resolve(
            None,
            None,
            &[(PROVIDER_VARIABLE, "zai"), (MODEL_VARIABLE, "glm-x")],
        )
        .expect("the environment names it");
        assert_eq!(plan.provider, "zai");
        assert_eq!(plan.model, "glm-x");
    }

    #[test]
    fn a_model_is_required_when_the_provider_has_no_default() {
        let error = resolve(Some("openai"), None, &[]).expect_err("no model");
        assert!(error.contains("has no default model"), "{error}");
        assert!(error.contains("model <name>"), "{error}");
        assert!(error.contains(MODEL_VARIABLE), "{error}");
        assert_eq!(
            resolve(Some("openai"), Some("gpt-x"), &[])
                .expect("a named model")
                .model,
            "gpt-x"
        );
        // zai's default stays: it is the one model this wave can verify.
        assert_eq!(
            resolve(Some("zai"), None, &[]).expect("zai").model,
            "glm-5.3"
        );
    }

    #[test]
    fn an_unknown_provider_lists_the_valid_ones() {
        let error = resolve(Some("groq"), None, &[]).expect_err("unknown");
        assert!(error.contains("unknown provider \"groq\""), "{error}");
        assert!(error.contains("anthropic"), "{error}");
        assert!(error.contains("ollama"), "{error}");
    }

    #[test]
    fn every_builtin_provider_resolves_with_a_model() {
        for entry in PROVIDERS {
            let pairs: Vec<(&str, &str)> = if entry.base_url.is_empty() {
                vec![(BASE_URL_VARIABLE, "http://127.0.0.1:1/v1")]
            } else {
                Vec::new()
            };
            let plan = resolve(Some(entry.name), Some("m"), &pairs).expect(entry.name);
            assert_eq!(plan.provider, entry.name);
            assert_eq!(plan.protocol, entry.protocol);
            assert!(plan.endpoint().ends_with(entry.protocol.path()), "{plan:?}");
            assert_eq!(plan.max_tokens, crate::anthropic::DEFAULT_MAX_TOKENS);
        }
    }

    #[test]
    fn custom_needs_a_root_and_takes_a_protocol() {
        let error = resolve(Some("custom"), Some("m"), &[]).expect_err("no root");
        assert!(error.contains("provider custom"), "{error}");
        assert!(error.contains(BASE_URL_VARIABLE), "{error}");
        let plan = resolve(
            Some("custom"),
            Some("m"),
            &[
                (BASE_URL_VARIABLE, "http://127.0.0.1:9/v1"),
                (PROTOCOL_VARIABLE, "anthropic"),
            ],
        )
        .expect("a custom provider");
        assert_eq!(plan.protocol, Protocol::Anthropic);
        assert_eq!(plan.endpoint(), "http://127.0.0.1:9/v1/messages");
        // Only `custom` reads the protocol variable: a built-in provider's
        // protocol is a property of the API, not of the environment.
        let zai =
            resolve(Some("zai"), Some("m"), &[(PROTOCOL_VARIABLE, "anthropic")]).expect("zai");
        assert_eq!(zai.protocol, Protocol::OpenAi);
    }

    /// A provider's own override points it at a proxy; the protocol and the
    /// key variables stay the provider's own.
    #[test]
    fn a_base_url_override_keeps_the_providers_protocol_and_key() {
        let plan = resolve(
            Some("anthropic"),
            None,
            &[(
                "PINKDB_AGENT_ANTHROPIC_BASE_URL",
                "http://127.0.0.1:7/gateway",
            )],
        )
        .expect("anthropic through a proxy");
        assert_eq!(plan.endpoint(), "http://127.0.0.1:7/gateway/messages");
        assert_eq!(plan.model, "claude-sonnet-5-5");
        assert_eq!(
            plan.key_with(&environment(&[("ANTHROPIC_API_KEY", "sk-a")]))
                .expect("its own key"),
            "sk-a"
        );
    }

    #[test]
    fn every_provider_has_its_own_root_variable() {
        for entry in PROVIDERS {
            let expected = if entry.name == "custom" {
                BASE_URL_VARIABLE.to_string()
            } else {
                format!("PINKDB_AGENT_{}_BASE_URL", entry.name.to_ascii_uppercase())
            };
            assert_eq!(entry.base_url_variable, expected, "{}", entry.name);
            let owners = PROVIDERS
                .iter()
                .filter(|other| other.base_url_variable == entry.base_url_variable)
                .count();
            assert_eq!(owners, 1, "{} is shared", entry.base_url_variable);
        }
    }

    /// The hole this rule closes: `PINKDB_AGENT_BASE_URL` left over from a
    /// `custom` run must not carry a built-in provider's key to that host.
    #[test]
    fn the_custom_root_never_moves_a_builtin_provider() {
        let evil = [(BASE_URL_VARIABLE, "http://evil")];
        for entry in PROVIDERS.iter().filter(|entry| entry.name != "custom") {
            let plan = resolve(Some(entry.name), Some("m"), &evil).expect(entry.name);
            assert_eq!(plan.base_url, entry.base_url, "{}", entry.name);
        }
        let custom = resolve(Some("custom"), Some("m"), &evil).expect("custom");
        assert_eq!(custom.base_url, "http://evil");
    }

    #[test]
    fn one_providers_override_never_moves_another() {
        for owner in PROVIDERS {
            let moved = [(owner.base_url_variable, "http://127.0.0.1:7/override")];
            for entry in PROVIDERS {
                let root = entry.base_url_with(&environment(&moved));
                if entry.name == owner.name {
                    assert_eq!(root.as_deref(), Some("http://127.0.0.1:7/override"));
                } else {
                    assert_eq!(
                        root.as_deref().unwrap_or(""),
                        entry.base_url,
                        "{} moved {}",
                        owner.base_url_variable,
                        entry.name
                    );
                }
            }
        }
    }

    /// The isolation rule, exercised: every sentinel exported at once, and
    /// each provider reads exactly its own.
    #[test]
    fn a_provider_only_ever_reads_its_own_key() {
        let all = environment(&[
            ("ZAI_API_KEY", "sk-zai"),
            ("ANTHROPIC_API_KEY", "sk-anthropic"),
            ("OPENROUTER_API_KEY", "sk-openrouter"),
            ("OPENAI_API_KEY", "sk-openai"),
            ("DEEPSEEK_API_KEY", "sk-deepseek"),
            ("GEMINI_API_KEY", "sk-gemini"),
            ("PINKDB_AGENT_API_KEY", "sk-custom"),
            (BASE_URL_VARIABLE, "http://127.0.0.1:1/v1"),
        ]);
        for (name, expected) in [
            ("zai", "sk-zai"),
            ("anthropic", "sk-anthropic"),
            ("openrouter", "sk-openrouter"),
            ("openai", "sk-openai"),
            ("deepseek", "sk-deepseek"),
            ("gemini", "sk-gemini"),
            ("custom", "sk-custom"),
            // No variables at all: no header, even with every key exported.
            ("ollama", ""),
        ] {
            let plan = Plan::resolve_with(Some(name), Some("m"), &all).expect(name);
            assert_eq!(plan.key_with(&all).expect(name), expected, "{name}");
        }
    }

    #[test]
    fn a_missing_required_key_names_its_own_variables_only() {
        let plan = resolve(Some("zai"), None, &[]).expect("zai");
        let error = plan
            .key_with(&environment(&[("ANTHROPIC_API_KEY", "sk-a")]))
            .expect_err("zai's key is missing");
        assert!(error.contains("provider zai"), "{error}");
        assert!(error.contains("ZAI_API_KEY or Z_API_KEY"), "{error}");
        assert!(!error.contains("ANTHROPIC"), "{error}");
        assert!(!error.contains("sk-a"), "{error}");
        // z.ai's own CLI variable is the second choice, not a different key.
        assert_eq!(
            plan.key_with(&environment(&[("Z_API_KEY", "sk-z")]))
                .expect("the fallback"),
            "sk-z"
        );
    }

    /// `custom` and `ollama` may be reached without a key; the rest may not.
    #[test]
    fn an_optional_key_is_not_an_error_when_it_is_missing() {
        let empty = environment(&[(BASE_URL_VARIABLE, "http://127.0.0.1:1/v1")]);
        for name in ["custom", "ollama"] {
            let plan = Plan::resolve_with(Some(name), Some("m"), &empty).expect(name);
            assert_eq!(plan.key_with(&empty).expect(name), "");
        }
        for name in ["zai", "anthropic", "openai"] {
            let plan = Plan::resolve_with(Some(name), Some("m"), &empty).expect(name);
            assert!(plan.key_with(&empty).is_err(), "{name} needs a key");
        }
    }

    #[test]
    fn a_key_state_says_set_or_missing_and_nothing_else() {
        let set = environment(&[("ANTHROPIC_API_KEY", "sk-secret")]);
        for (name, expected) in [
            ("anthropic", "key: set"),
            ("zai", "key: missing"),
            ("ollama", "no key needed"),
            ("custom", "key: missing (optional)"),
        ] {
            let entry = provider(name).expect(name);
            let state = key_state_with(entry, &set);
            assert_eq!(state, expected, "{name}");
            assert!(!state.contains("sk-"), "{state}");
        }
    }

    #[test]
    fn max_tokens_is_overridable_and_must_be_positive() {
        let plan =
            resolve(Some("anthropic"), None, &[(MAX_TOKENS_VARIABLE, "256")]).expect("an override");
        assert_eq!(plan.max_tokens, 256);
        assert!(resolve(Some("anthropic"), None, &[(MAX_TOKENS_VARIABLE, "0")]).is_err());
        assert!(resolve(Some("anthropic"), None, &[(MAX_TOKENS_VARIABLE, "lots")]).is_err());
    }

    #[test]
    fn the_protocol_names_round_trip() {
        for protocol in [Protocol::OpenAi, Protocol::Anthropic] {
            assert_eq!(Protocol::parse(protocol.name()), Ok(protocol));
        }
        let error = Protocol::parse("bedrock").expect_err("unknown protocol");
        assert!(error.contains(PROTOCOL_VARIABLE), "{error}");
    }
}
