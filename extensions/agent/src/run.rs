//! The run loop: one model turn per step, its tool calls through the host,
//! every step a checkpoint.
//!
//! Resume is the reason the transcript lives in the store instead of in
//! memory. A step whose assistant turn is recorded is not asked again; a tool
//! call whose reply is recorded is not run again; a stream that ended early
//! was never recorded, so the step is requested again. Only a replay-safe tool
//! (a read-only one) may run twice.

use serde_json::json;

use pinkdb_llm::{Message, Provider, ToolCall, Usage};

use crate::host::{ANSWER, DIAGNOSTIC, Host};
use crate::store::{self, NewSession, Session};

/// What the model is told about the session it is driving.
pub fn system_prompt(session: &Session) -> String {
    let writes = if session.allow_writes {
        "The operator started this run with allow-writes, so `query` may write. \
         The agent's own database is still off-limits."
    } else {
        "This run is read-only: the host refuses any statement that would \
         change the store, including transactions and COPY."
    };
    format!(
        "You are the pinkdb agent: a data analyst answering questions about the \
         database this session is connected to.\n\n\
         Use the tools to look at the data before you answer: `databases`, `tables` and \
         `describe` for the schema, `query` for SQL, `explain` for a plan, and the \
         `lake_*` tools for lake catalogs. Never guess a table or column name: if a \
         query fails, read the error and try a different statement.\n\n\
         {writes}\n\n\
         Answer in Markdown: one short sentence that answers the question, then a \
         table when the answer has rows. Every number must come from a query result \
         in this run. Do not describe the tools you used unless asked.\n"
    )
}

/// One run's settings. The budgets are recorded in the transcript when the
/// session is created, so a resume continues under the same limits.
#[derive(Debug, Clone, Default)]
pub struct Options {
    pub question: String,
    pub max_steps: u32,
    pub token_budget: u64,
    pub allow_writes: bool,
    pub allow_files: bool,
}

/// Start a session: record the question, then drive it.
pub fn ask(
    host: &mut dyn Host,
    provider: &mut dyn Provider,
    options: &Options,
) -> Result<String, String> {
    store::ensure_schema(host)?;
    let id = store::new_id();
    let name = provider.provider().to_string();
    let model = provider.model().to_string();
    let base_url = provider.base_url().to_string();
    store::create_session(
        host,
        &id,
        &NewSession {
            question: &options.question,
            provider: &name,
            model: &model,
            base_url: &base_url,
            max_steps: options.max_steps,
            token_budget: options.token_budget,
            // The flags live in the transcript, not in the prompt: a resume
            // must not be able to widen its own policy.
            allow_writes: options.allow_writes,
            allow_files: options.allow_files,
            origin: "",
            origin_step: 0,
        },
    )?;
    store::record_question(host, &id, &options.question)?;
    store::checkpoint(host, &id, 0, "question")?;
    continue_session(host, provider, &id)?;
    Ok(id)
}

/// Continue a session from its last checkpoint.
pub fn resume(host: &mut dyn Host, provider: &mut dyn Provider, id: &str) -> Result<(), String> {
    store::ensure_schema(host)?;
    let session = store::session(host, id)?.ok_or_else(|| unknown_session(id))?;
    if session.status == "done" {
        return Err(format!(
            "session {id} already finished: `.agent fork {id} {}` continues a copy of it",
            session.steps
        ));
    }
    continue_session(host, provider, id)
}

/// Continue a session with a new question: it becomes the user message of the
/// next step, so the model sees the earlier turns as context.
pub fn continue_with(
    host: &mut dyn Host,
    provider: &mut dyn Provider,
    id: &str,
    question: &str,
) -> Result<(), String> {
    let session = store::session(host, id)?.ok_or_else(|| unknown_session(id))?;
    if session.status == "done" {
        return Err(format!(
            "session {id} already finished: `.agent fork {id} {} \"{question}\"` continues a copy",
            session.steps
        ));
    }
    store::record_follow_up(host, id, question)?;
    continue_session(host, provider, id)
}

/// Copy a session's prefix up to `step` into a new session.
///
/// `provider` and `model` are the fork's run arguments; each falls back to
/// what the source session recorded, then to the environment. A fork may
/// therefore land on a different provider than the session it came from: the
/// transcript is provider-neutral, so continuing it is a re-encode.
///
/// The plan is settled *before* anything is written, so an unknown provider or
/// a missing model fails without leaving a half-made session behind. No key is
/// read: a fork with no new question never calls a provider.
pub fn fork(
    host: &mut dyn Host,
    id: &str,
    step: u32,
    provider: Option<&str>,
    model: Option<&str>,
) -> Result<String, String> {
    store::ensure_schema(host)?;
    let session = store::session(host, id)?.ok_or_else(|| unknown_session(id))?;
    let recorded = store::last_step(host, id)?;
    if step == 0 || step > recorded {
        return Err(format!(
            "session {id} has steps up to {recorded}; fork at a step in 1..={recorded}"
        ));
    }
    // A recorded model belongs to the recorded provider: carried onto another
    // provider it names a model that provider does not serve.
    let switched = provider.is_some_and(|name| name != session.provider);
    let recorded_model = if switched { None } else { some(&session.model) };
    let plan = pinkdb_llm::Plan::resolve(
        provider.or(some(&session.provider)),
        model.or(recorded_model),
    )?;
    let forked = store::new_id();
    store::copy_into(host, &session, &forked, step, plan.provider, &plan.model)?;
    Ok(forked)
}

/// A recorded field, or `None` when the session never had one.
fn some(recorded: &str) -> Option<&str> {
    (!recorded.is_empty()).then_some(recorded)
}

fn unknown_session(id: &str) -> String {
    format!("no session {id:?} in this store: `.agent sessions` lists them")
}

fn continue_session(
    host: &mut dyn Host,
    provider: &mut dyn Provider,
    id: &str,
) -> Result<(), String> {
    let mut session = store::session(host, id)?.ok_or_else(|| unknown_session(id))?;
    // A resume or a fork may name a different provider than the session last
    // ran on. The row follows the run, so `.agent show` never describes a
    // provider that is not the one answering; the per-step `usage` rows keep
    // the earlier provider's share.
    if session.provider != provider.provider() || session.model != provider.model() {
        session.provider = provider.provider().to_string();
        session.model = provider.model().to_string();
        session.base_url = provider.base_url().to_string();
        store::set_provider(
            host,
            id,
            &session.provider,
            &session.model,
            &session.base_url,
        )?;
    }
    let tools = pinkdb_warehouse_tools::schemas();
    let mut history: Option<Vec<Message>> = None;
    loop {
        let usage = store::usage(host, id)?;
        let step = store::last_step(host, id)? + 1;
        if session.token_budget > 0 && usage.total() >= session.token_budget {
            return stop(
                host,
                id,
                &format!(
                    "token budget ({} tokens) reached after {} steps",
                    session.token_budget,
                    step - 1
                ),
                usage,
            );
        }
        if step > session.max_steps {
            return stop(
                host,
                id,
                &format!("max steps ({}) reached", session.max_steps),
                usage,
            );
        }
        let recorded = store::assistant(host, id, step)?;
        let resumed = recorded.is_some();
        let turn = match recorded {
            Some(turn) => {
                host.emit(
                    DIAGNOSTIC,
                    &format!("pinkdb agent: step {step} resumed from the transcript\n"),
                );
                turn
            }
            None => {
                if history.is_none() {
                    let mut messages = vec![Message::system(system_prompt(&session))];
                    messages.extend(store::history(host, id)?);
                    history = Some(messages);
                }
                let messages = history.as_deref().unwrap_or_default();
                match provider.complete(messages, &tools) {
                    Ok(turn) => turn,
                    Err(message) => {
                        let steps = step.saturating_sub(1);
                        store::finish(host, id, "failed", &message, steps, usage, 0.0)?;
                        return Err(message);
                    }
                }
            }
        };
        if !resumed {
            store::record_assistant(host, &session, step, &turn)?;
        }
        if turn.tool_calls.is_empty() {
            store::checkpoint(host, id, step, store::CHECKPOINT)?;
            if !turn.content.is_empty() {
                host.emit(ANSWER, &turn.content);
                host.emit(ANSWER, "\n");
            }
            let usage = store::usage(host, id)?;
            store::finish(host, id, "done", "", step, usage, 0.0)?;
            return Ok(());
        }
        run_tools(host, id, step, &turn.tool_calls, resumed)?;
        store::checkpoint(host, id, step, store::CHECKPOINT)?;
        // The transcript changed, so the next request is rebuilt from it.
        history = None;
    }
}

/// Run (or reuse) one step's tool calls, in order.
///
/// `resumed` is true when this step's assistant turn was already in the
/// store: a call still `running` then was interrupted by a crash. A call this
/// process just recorded also reads `running`, so the flag — not the state
/// alone — decides whether that is a crash or the normal path.
fn run_tools(
    host: &mut dyn Host,
    id: &str,
    step: u32,
    calls: &[ToolCall],
    resumed: bool,
) -> Result<(), String> {
    let recorded = store::tool_calls(host, id, step)?;
    for (index, call) in calls.iter().enumerate() {
        let row = recorded.iter().find(|row| row.call_id == call.id);
        if let Some(row) = row
            && row.state == "recorded"
        {
            host.emit(
                DIAGNOSTIC,
                &format!(
                    "pinkdb agent: step {step} reused the recorded {} call\n",
                    call.name
                ),
            );
            continue;
        }
        if resumed
            && let Some(row) = row
            && !row.replay_safe
        {
            // The run died while this call may have been writing. It is not
            // safe to run it again, so the model gets an error to react to.
            let reply = json!({
                "error": {
                    "message": "this call was interrupted while it may have written to the \
                                store, so it was not run again",
                    "kind": "interrupted",
                }
            })
            .to_string();
            store::record_tool_result(
                host,
                id,
                step,
                index as u32 + 2,
                &call.id,
                &call.name,
                1,
                &reply,
            )?;
            continue;
        }
        host.emit(
            DIAGNOSTIC,
            &format!(
                "pinkdb agent: step {step} {}({})\n",
                call.name,
                shorten(&call.arguments, 200)
            ),
        );
        let (code, reply) = crate::command::call_tool(host, call);
        store::record_tool_result(
            host,
            id,
            step,
            index as u32 + 2,
            &call.id,
            &call.name,
            code,
            &reply,
        )?;
    }
    Ok(())
}

/// Mark a run `stopped` and say why; the reason is in the transcript too.
fn stop(host: &mut dyn Host, id: &str, reason: &str, usage: Usage) -> Result<(), String> {
    host.emit(DIAGNOSTIC, &format!("pinkdb agent: stopped: {reason}\n"));
    let steps = store::last_step(host, id)?;
    store::finish(host, id, "stopped", reason, steps, usage, 0.0)
}

fn shorten(text: &str, limit: usize) -> String {
    if text.chars().count() > limit {
        text.chars().take(limit).collect::<String>() + "…"
    } else {
        text.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(allow_writes: bool) -> Session {
        Session {
            allow_writes,
            ..Session::default()
        }
    }

    #[test]
    fn the_prompt_states_the_write_policy() {
        assert!(system_prompt(&session(false)).contains("read-only"));
        assert!(system_prompt(&session(true)).contains("allow-writes"));
        assert!(system_prompt(&session(true)).contains("off-limits"));
    }

    #[test]
    fn only_a_read_only_query_replays() {
        assert!(store::is_replay_safe("query", false));
        assert!(!store::is_replay_safe("query", true));
        assert!(store::is_replay_safe("describe", true));
    }

    #[test]
    fn long_arguments_are_shortened() {
        assert_eq!(shorten("abc", 10), "abc");
        assert_eq!(shorten("abcdef", 3), "abc…");
    }
}
