//! `.agent`: the harness's command surface, as the host dispatches it.
//!
//! The CLI hands the text after `.agent` to the loaded extension unchanged,
//! so the grammar lives here (and `pinkdb --agent FILE "<question>"` builds
//! the same `ask` line). Nothing in this module knows about the CLI.

use serde_json::{Value, json};

use pinkdb_llm::registry;
use pinkdb_llm::{Client, Config, ToolCall};

use crate::host::{ANSWER, DIAGNOSTIC, Host};
use crate::run::{self, Options};
use crate::store;

/// The default step budget (wave 99's `PINKDB_AGENT_*` variables configure the
/// rest of the agent surface).
pub const DEFAULT_MAX_STEPS: u32 = 20;

const USAGE: &str = "\
.agent ask \"<question>\" [provider <name>] [model <name>] [allow-writes] [allow-files]
                         [max-steps N] [tokens N]
.agent resume <session> [provider <name>] [model <name>]
.agent sessions
.agent show <session>
.agent fork <session> <step> [provider <name>] [model <name>] [\"<new question>\"]
.agent providers
.agent rows|cols|cell|budget|timeout|hint [N]   (wave 99: agent output settings)
";

const RESUME_USAGE: &str = "usage: .agent resume <session> [provider <name>] [model <name>] (a session keeps the \
     policy and budgets it started with)";

const FORK_USAGE: &str = "usage: .agent fork <session> <step> [provider <name>] \
                          [model <name>] [\"<new question>\"]";

/// Which provider and model a run names, before the registry settles them.
///
/// Both are optional at this level: the registry applies the precedence (a run
/// argument, then a recorded session, then the environment) and refuses when
/// nothing names a provider.
#[derive(Debug, Clone, Default)]
struct Selection {
    provider: Option<String>,
    model: Option<String>,
}

/// Entry point: run one command, report failures on the diagnostic channel
/// and return the extension's exit code.
pub fn run(session: &mut pinkdb_ext_sdk::CommandSession, args: &str) -> i32 {
    match dispatch(session, args) {
        Ok(()) => 0,
        Err(message) => {
            session.emit(DIAGNOSTIC, &format!("pinkdb agent: {message}\n"));
            1
        }
    }
}

fn dispatch(session: &mut dyn Host, args: &str) -> Result<(), String> {
    let words = split(args)?;
    let Some(verb) = words.first() else {
        emit(session, USAGE);
        return Ok(());
    };
    match verb.as_str() {
        "help" => {
            emit(session, USAGE);
            Ok(())
        }
        "ask" => ask(session, &words[1..]),
        "resume" => resume(session, &words[1..]),
        "sessions" => sessions(session),
        "show" => {
            let id = words.get(1).ok_or("usage: .agent show <session>")?;
            show(session, id)
        }
        "fork" => fork(session, &words[1..]),
        "providers" => providers(session),
        other => Err(format!(
            "unknown .agent command {other:?}: try .agent ask, resume, sessions, show, fork or \
             providers"
        )),
    }
}

fn ask(session: &mut dyn Host, words: &[String]) -> Result<(), String> {
    let (question, options, selection) = settings(words)?;
    let Some(question) = question else {
        return Err(
            "usage: .agent ask \"<question>\" [provider <name>] [model <name>] [allow-writes] \
             [allow-files]"
                .to_string(),
        );
    };
    let mut provider = connect(&selection)?;
    let id = run::ask(
        session,
        &mut provider,
        &Options {
            question,
            ..options
        },
    )?;
    session.emit(
        DIAGNOSTIC,
        &format!(
            "pinkdb agent: session {id} is in {database} (`.agent show {id}`, `.agent resume {id}`)\n",
            database = store::DATABASE
        ),
    );
    Ok(())
}

/// `.agent resume <session> [provider <name>] [model <name>]`.
///
/// The session's recorded provider and model are the defaults, so a resume
/// continues on what the run started on unless it says otherwise. The write
/// policy and the budgets are *not* resumable arguments: they were recorded
/// when the session started, and a resume must not be able to widen them.
fn resume(session: &mut dyn Host, words: &[String]) -> Result<(), String> {
    let id = words.first().ok_or(RESUME_USAGE)?.clone();
    let (extra, selection) = selection(&words[1..], RESUME_USAGE)?;
    if let Some(word) = extra {
        return Err(format!("unexpected argument {word:?}: {RESUME_USAGE}"));
    }
    store::ensure_schema(session)?;
    let recorded = store::session(session, &id)?;
    let mut provider = connect(&selection.or_recorded(recorded.as_ref()))?;
    run::resume(session, &mut provider, &id)
}

fn fork(session: &mut dyn Host, words: &[String]) -> Result<(), String> {
    let id = words.first().ok_or(FORK_USAGE)?.clone();
    let step = words.get(1).ok_or(FORK_USAGE)?;
    let step: u32 = step
        .parse()
        .map_err(|_| format!("{step:?} is not a step number"))?;
    let (question, selection) = selection(&words[2..], FORK_USAGE)?;
    let forked = run::fork(
        session,
        &id,
        step,
        selection.provider.as_deref(),
        selection.model.as_deref(),
    )?;
    session.emit(
        DIAGNOSTIC,
        &format!("pinkdb agent: forked {id} at step {step} into {forked}\n"),
    );
    if let Some(question) = question {
        // The fork row already records the provider and model the fork settled
        // on, so continuing it reads them back instead of resolving twice.
        let recorded = store::session(session, &forked)?;
        let mut provider = connect(&Selection::default().or_recorded(recorded.as_ref()))?;
        run::continue_with(session, &mut provider, &forked, &question)?;
    }
    Ok(())
}

/// `.agent providers`: the built-in table and whether each provider's key is
/// available. Never a key, never a prefix of one, never its length.
fn providers(session: &mut dyn Host) -> Result<(), String> {
    emit(
        session,
        &format!(
            "{:<11} {:<10} {:<58} {:<33} {:<18} {}\n",
            "provider", "protocol", "base url", "override", "default model", "key"
        ),
    );
    for entry in registry::PROVIDERS {
        emit(
            session,
            &format!(
                "{:<11} {:<10} {:<58} {:<33} {:<18} {}\n",
                entry.name,
                entry.protocol.name(),
                entry
                    .base_url_with(&registry::env)
                    .unwrap_or_else(|| "(unset)".to_string()),
                entry.base_url_variable,
                if entry.default_model.is_empty() {
                    "(model required)"
                } else {
                    entry.default_model
                },
                registry::key_state(entry),
            ),
        );
    }
    emit(
        session,
        "there is no default provider: name one with `provider <name>` on the run or \
         PINKDB_AGENT_PROVIDER\n",
    );
    Ok(())
}

impl Selection {
    /// Fill the gaps from a recorded session, which sits between the run's
    /// arguments and the environment: a resume or a fork continues on what the
    /// session ran on unless it names something else.
    fn or_recorded(mut self, recorded: Option<&store::Session>) -> Selection {
        let Some(session) = recorded else {
            return self;
        };
        // A recorded model belongs to the recorded provider: carried onto
        // another provider it names a model that provider does not serve.
        let switched = self
            .provider
            .as_ref()
            .is_some_and(|name| *name != session.provider);
        if self.provider.is_none() && !session.provider.is_empty() {
            self.provider = Some(session.provider.clone());
        }
        if self.model.is_none() && !switched && !session.model.is_empty() {
            self.model = Some(session.model.clone());
        }
        self
    }
}

/// Parse `[provider <name>] [model <name>]` plus at most one bare word (a
/// fork's new question), in any order.
fn selection(words: &[String], usage: &str) -> Result<(Option<String>, Selection), String> {
    let mut bare = None;
    let mut selection = Selection::default();
    let mut index = 0;
    while index < words.len() {
        match words[index].as_str() {
            key @ ("provider" | "model") => {
                let value = words
                    .get(index + 1)
                    .ok_or_else(|| format!("{key} needs a name"))?;
                match key {
                    "provider" => selection.provider = Some(value.clone()),
                    _ => selection.model = Some(value.clone()),
                }
                index += 1;
            }
            other if bare.is_none() => bare = Some(other.to_string()),
            other => return Err(format!("unexpected argument {other:?}: {usage}")),
        }
        index += 1;
    }
    Ok((bare, selection))
}

/// Parse the words of one command: an optional question, then the provider
/// selection, the per-run policy and the budgets.
fn settings(words: &[String]) -> Result<(Option<String>, Options, Selection), String> {
    let mut question = None;
    let mut selection = Selection::default();
    let mut options = Options {
        max_steps: default_max_steps()?,
        token_budget: default_token_budget()?,
        ..Options::default()
    };
    let mut index = 0;
    while index < words.len() {
        match words[index].as_str() {
            "allow-writes" => options.allow_writes = true,
            "allow-files" => options.allow_files = true,
            key @ ("provider" | "model") => {
                let value = words
                    .get(index + 1)
                    .ok_or_else(|| format!("{key} needs a name"))?;
                match key {
                    "provider" => selection.provider = Some(value.clone()),
                    _ => selection.model = Some(value.clone()),
                }
                index += 1;
            }
            "max-steps" => {
                let value = words.get(index + 1).ok_or("max-steps needs a number")?;
                options.max_steps = value
                    .parse()
                    .map_err(|_| format!("max-steps: {value:?} is not a number"))?;
                index += 1;
            }
            "tokens" => {
                let value = words.get(index + 1).ok_or("tokens needs a number")?;
                options.token_budget = value
                    .parse()
                    .map_err(|_| format!("tokens: {value:?} is not a number"))?;
                index += 1;
            }
            other if question.is_none() => question = Some(other.to_string()),
            other => {
                return Err(format!(
                    "unexpected argument {other:?}: quote the question in one set of quotes"
                ));
            }
        }
        index += 1;
    }
    if options.max_steps == 0 {
        return Err("max-steps must be at least 1".to_string());
    }
    Ok((question, options, selection))
}

fn default_max_steps() -> Result<u32, String> {
    let Some(value) = env("PINKDB_AGENT_MAX_STEPS") else {
        return Ok(DEFAULT_MAX_STEPS);
    };
    let steps: u32 = value
        .parse()
        .map_err(|_| format!("PINKDB_AGENT_MAX_STEPS={value:?} is not a number"))?;
    if steps == 0 {
        return Err("PINKDB_AGENT_MAX_STEPS must be at least 1".to_string());
    }
    Ok(steps)
}

fn default_token_budget() -> Result<u64, String> {
    let Some(value) = env("PINKDB_AGENT_TOKEN_BUDGET") else {
        return Ok(0);
    };
    value
        .parse()
        .map_err(|_| format!("PINKDB_AGENT_TOKEN_BUDGET={value:?} is not a number"))
}

fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Build the client for this run. The key is read here, from the named
/// provider's own variables, and never leaves the client.
fn connect(selection: &Selection) -> Result<Client, String> {
    Ok(Client::new(Config::from_env(
        selection.provider.as_deref(),
        selection.model.as_deref(),
    )?))
}

fn sessions(session: &mut dyn Host) -> Result<(), String> {
    store::ensure_schema(session)?;
    let list = store::sessions(session)?;
    if list.is_empty() {
        emit(
            session,
            "no sessions in this store yet: `.agent ask \"<question>\"` starts one\n",
        );
        return Ok(());
    }
    emit(
        session,
        &format!(
            "{:<18} {:>5} {:>7} {:>8}  {}\n",
            "session", "steps", "status", "tokens", "question"
        ),
    );
    for row in list {
        let question = row.question.lines().next().unwrap_or_default();
        emit(
            session,
            &format!(
                "{:<18} {:>5} {:>7} {:>8}  {}\n",
                row.id,
                row.steps,
                row.status,
                row.usage.total(),
                preview(question, 60)
            ),
        );
    }
    Ok(())
}

fn show(session: &mut dyn Host, id: &str) -> Result<(), String> {
    store::ensure_schema(session)?;
    let Some(row) = store::session(session, id)? else {
        return Err(format!(
            "no session {id:?} in this store: `.agent sessions` lists them"
        ));
    };
    emit(
        session,
        &format!(
            "session {} ({}{})\n",
            row.id,
            row.status,
            if row.stop_reason.is_empty() {
                String::new()
            } else {
                format!(": {}", row.stop_reason)
            }
        ),
    );
    emit(session, &format!("question: {}\n", row.question));
    emit(
        session,
        &format!(
            "provider: {}  model: {}  write policy: {}  steps: {}  tokens: {}+{}  fork: {} \
             step {}\n",
            if row.provider.is_empty() {
                "-"
            } else {
                row.provider.as_str()
            },
            row.model,
            if row.allow_writes {
                "allow-writes"
            } else {
                "read-only"
            },
            row.steps,
            row.usage.prompt_tokens,
            row.usage.completion_tokens,
            if row.origin.is_empty() {
                "-"
            } else {
                row.origin.as_str()
            },
            row.origin_step
        ),
    );
    for step in 1..=store::last_step(session, id)? {
        let Some(turn) = store::assistant(session, id, step)? else {
            continue;
        };
        if !turn.content.trim().is_empty() {
            emit(
                session,
                &format!("step {step} assistant: {}\n", preview(&turn.content, 300)),
            );
        }
        for call in store::tool_calls(session, id, step)? {
            emit(
                session,
                &format!(
                    "step {step} tool {} ({}): {}\n",
                    call.name,
                    call.state,
                    preview(&call.reply, 160)
                ),
            );
        }
    }
    Ok(())
}

/// Run one tool call the model asked for: the shared warehouse tool set,
/// dispatched to the host's statement callback.
pub fn call_tool(host: &mut dyn Host, call: &ToolCall) -> (i32, String) {
    let arguments = match serde_json::from_str::<Value>(&call.arguments) {
        Ok(Value::Object(map)) => map,
        Ok(_) => return (1, tool_error("the tool arguments must be a JSON object")),
        Err(e) => {
            return (
                1,
                tool_error(&format!("the tool arguments were not JSON: {e}")),
            );
        }
    };
    struct Adapter<'a>(&'a mut dyn Host);
    impl pinkdb_warehouse_tools::ToolHost for Adapter<'_> {
        fn execute(&mut self, statement: &str) -> pinkdb_warehouse_tools::ToolReply {
            let (code, text) = self.0.tool(statement);
            pinkdb_warehouse_tools::ToolReply { code, text }
        }
    }
    let mut adapter = Adapter(host);
    match pinkdb_warehouse_tools::run(&mut adapter, &call.name, &arguments) {
        Ok(reply) => (reply.code, reply.text),
        Err(message) => (1, tool_error(&message)),
    }
}

fn tool_error(message: &str) -> String {
    json!({"error": {"message": message, "kind": "tool"}}).to_string()
}

fn emit(session: &mut dyn Host, text: &str) {
    session.emit(ANSWER, text);
}

fn preview(text: &str, limit: usize) -> String {
    let line: String = text
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_default()
        .to_string();
    if line.chars().count() > limit {
        line.chars().take(limit).collect::<String>() + "…"
    } else {
        line
    }
}

/// Split a command line on whitespace, keeping `"quoted text"` (with `\"` and
/// `\\` escapes) as one word. The question is the one argument that needs it.
fn split(text: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quoted = false;
    let mut started = false;
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' if quoted => match chars.next() {
                Some(escaped) => {
                    word.push(escaped);
                    started = true;
                }
                None => return Err("the command ends with a backslash".to_string()),
            },
            '"' => {
                quoted = !quoted;
                started = true;
            }
            c if c.is_whitespace() && !quoted => {
                if started {
                    words.push(std::mem::take(&mut word));
                    started = false;
                }
            }
            c => {
                word.push(c);
                started = true;
            }
        }
    }
    if quoted {
        return Err("unterminated quote in the command".to_string());
    }
    if started {
        words.push(word);
    }
    Ok(words)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_keep_a_question_together() {
        assert_eq!(
            split(r#"ask "top 3 customers by revenue" allow-writes"#).expect("valid"),
            ["ask", "top 3 customers by revenue", "allow-writes"]
        );
    }

    #[test]
    fn escapes_survive() {
        assert_eq!(
            split(r#"ask "say \"hi\""  "#).expect("valid"),
            ["ask", r#"say "hi""#]
        );
    }

    #[test]
    fn an_unterminated_quote_is_an_error() {
        assert!(split(r#"ask "half"#).is_err());
    }

    fn words(line: &str) -> Vec<String> {
        split(line).expect("a valid command line")
    }

    #[test]
    fn the_defaults_are_wave_99_and_twenty_steps() {
        let (question, options, selection) = settings(&words("what?")).expect("valid");
        assert_eq!(question.as_deref(), Some("what?"));
        assert_eq!(options.max_steps, DEFAULT_MAX_STEPS);
        assert_eq!(options.token_budget, 0);
        assert!(!options.allow_writes);
        // No provider is named here: the registry refuses later, so that one
        // error message covers the argument and the environment alike.
        assert_eq!(selection.provider, None);
        assert_eq!(selection.model, None);
    }

    #[test]
    fn budgets_and_policy_are_per_run() {
        let (_, options, _) =
            settings(&words("q allow-writes allow-files max-steps 3 tokens 100")).expect("valid");
        assert!(options.allow_writes);
        assert!(options.allow_files);
        assert_eq!(options.max_steps, 3);
        assert_eq!(options.token_budget, 100);
    }

    #[test]
    fn two_questions_are_an_error() {
        assert!(settings(&words("a b")).is_err());
    }

    #[test]
    fn a_run_names_its_provider_and_model_in_any_order() {
        let (question, options, selection) = settings(&words(
            r#"model glm-5.3-flash "top 3?" provider zai allow-writes"#,
        ))
        .expect("valid");
        assert_eq!(question.as_deref(), Some("top 3?"));
        assert_eq!(selection.provider.as_deref(), Some("zai"));
        assert_eq!(selection.model.as_deref(), Some("glm-5.3-flash"));
        assert!(options.allow_writes);
    }

    #[test]
    fn provider_and_model_need_a_name() {
        assert!(settings(&words("\"q\" provider")).is_err());
        assert!(settings(&words("\"q\" model")).is_err());
        assert!(selection(&words("provider"), FORK_USAGE).is_err());
    }

    #[test]
    fn a_fork_takes_a_selection_and_one_question() {
        let (question, selection) =
            selection(&words(r#"provider anthropic "who is second?""#), FORK_USAGE).expect("valid");
        assert_eq!(question.as_deref(), Some("who is second?"));
        assert_eq!(selection.provider.as_deref(), Some("anthropic"));
        assert_eq!(selection.model, None);
        // Two bare words is the same mistake as two questions.
        assert!(selection2("a b").is_err());
    }

    fn selection2(line: &str) -> Result<(Option<String>, Selection), String> {
        selection(&words(line), FORK_USAGE)
    }

    /// A resume inherits the session's provider and model, but a run argument
    /// still wins — and an empty recorded field is not an answer.
    #[test]
    fn a_recorded_session_fills_the_gaps_a_run_left() {
        let recorded = store::Session {
            provider: "zai".to_string(),
            model: "glm-5.3".to_string(),
            ..store::Session::default()
        };
        let inherited = Selection::default().or_recorded(Some(&recorded));
        assert_eq!(inherited.provider.as_deref(), Some("zai"));
        assert_eq!(inherited.model.as_deref(), Some("glm-5.3"));

        // Switching provider drops the recorded model: glm-5.3 is not a model
        // Anthropic serves, so the new provider's default (or an error) wins.
        let switched = Selection {
            provider: Some("anthropic".to_string()),
            model: None,
        }
        .or_recorded(Some(&recorded));
        assert_eq!(switched.provider.as_deref(), Some("anthropic"));
        assert_eq!(switched.model, None);

        // Naming the same provider keeps the recorded model.
        let same = Selection {
            provider: Some("zai".to_string()),
            model: None,
        }
        .or_recorded(Some(&recorded));
        assert_eq!(same.model.as_deref(), Some("glm-5.3"));

        let named = Selection {
            provider: Some("anthropic".to_string()),
            model: Some("claude-x".to_string()),
        }
        .or_recorded(Some(&recorded));
        assert_eq!(named.model.as_deref(), Some("claude-x"));

        let blank = Selection::default().or_recorded(Some(&store::Session::default()));
        assert_eq!(blank.provider, None);
        assert_eq!(blank.model, None);
    }
}
