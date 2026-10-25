//! `.agent`: the harness's command surface, as the host dispatches it.
//!
//! The CLI hands the text after `.agent` to the loaded extension unchanged,
//! so the grammar lives here (and `pinkdb --agent FILE "<question>"` builds
//! the same `ask` line). Nothing in this module knows about the CLI.

use serde_json::{Value, json};

use crate::chat::ToolCall;
use crate::host::{ANSWER, DIAGNOSTIC, Host};
use crate::provider::{Client, Config};
use crate::run::{self, Options};
use crate::store;

/// The default step budget (wave 99's `PINKDB_AGENT_*` variables configure the
/// rest of the agent surface).
pub const DEFAULT_MAX_STEPS: u32 = 20;

const USAGE: &str = "\
.agent ask \"<question>\" [allow-writes] [allow-files] [max-steps N] [tokens N]
.agent resume <session>
.agent sessions
.agent show <session>
.agent fork <session> <step> [\"<new question>\"]
.agent rows|cols|cell|budget|timeout|hint [N]   (wave 99: agent output settings)
";

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
        "resume" => {
            let id = words.get(1).ok_or("usage: .agent resume <session>")?;
            if words.len() > 2 {
                // The write policy and the budgets were recorded when the
                // session started; a resume must not be able to widen them.
                return Err(format!(
                    "usage: .agent resume {id} (a session keeps the policy and budgets it                      started with)"
                ));
            }
            let mut provider = connect()?;
            run::resume(session, &mut provider, id)
        }
        "sessions" => sessions(session),
        "show" => {
            let id = words.get(1).ok_or("usage: .agent show <session>")?;
            show(session, id)
        }
        "fork" => fork(session, &words[1..]),
        other => Err(format!(
            "unknown .agent command {other:?}: try .agent ask, resume, sessions, show or fork"
        )),
    }
}

fn ask(session: &mut dyn Host, words: &[String]) -> Result<(), String> {
    let (question, options) = settings(words)?;
    let Some(question) = question else {
        return Err("usage: .agent ask \"<question>\" [allow-writes] [allow-files]".to_string());
    };
    let mut provider = connect()?;
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

fn fork(session: &mut dyn Host, words: &[String]) -> Result<(), String> {
    let id = words
        .first()
        .ok_or("usage: .agent fork <session> <step> [\"<new question>\"]")?;
    let step = words
        .get(1)
        .ok_or("usage: .agent fork <session> <step> [\"<new question>\"]")?;
    let step: u32 = step
        .parse()
        .map_err(|_| format!("{step:?} is not a step number"))?;
    let forked = run::fork(session, id, step)?;
    session.emit(
        DIAGNOSTIC,
        &format!("pinkdb agent: forked {id} at step {step} into {forked}\n"),
    );
    if let Some(question) = words.get(2) {
        let mut provider = connect()?;
        run::continue_with(session, &mut provider, &forked, question)?;
    }
    Ok(())
}

/// Parse the words of one command: an optional question, then the per-run
/// policy and budgets.
fn settings(words: &[String]) -> Result<(Option<String>, Options), String> {
    let mut question = None;
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
    Ok((question, options))
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

fn connect() -> Result<Client, String> {
    Ok(Client::new(Config::from_env()?))
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
            "model: {}  write policy: {}  steps: {}  tokens: {}+{}  fork: {} step {}\n",
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

    #[test]
    fn the_defaults_are_wave_99_and_twenty_steps() {
        let (question, options) = settings(&["what?".to_string()]).expect("valid");
        assert_eq!(question.as_deref(), Some("what?"));
        assert_eq!(options.max_steps, DEFAULT_MAX_STEPS);
        assert_eq!(options.token_budget, 0);
        assert!(!options.allow_writes);
    }

    #[test]
    fn budgets_and_policy_are_per_run() {
        let (_, options) = settings(&[
            "q".to_string(),
            "allow-writes".to_string(),
            "allow-files".to_string(),
            "max-steps".to_string(),
            "3".to_string(),
            "tokens".to_string(),
            "100".to_string(),
        ])
        .expect("valid");
        assert!(options.allow_writes);
        assert!(options.allow_files);
        assert_eq!(options.max_steps, 3);
        assert_eq!(options.token_budget, 100);
    }

    #[test]
    fn two_questions_are_an_error() {
        assert!(settings(&["a".to_string(), "b".to_string()]).is_err());
    }
}
