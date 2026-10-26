//! The operations behind both surfaces: the `.memory` dot command and the
//! `memory_*` MCP tools call the same functions, so the two cannot drift.
//!
//! Each one asks the host who the caller is, applies the store's visibility,
//! and touches only the internal channel.

use std::time::Duration;

use crate::distill::{self, Report, Summary};
use crate::host::{Caller, Host};
use crate::search::{self, Outcome, Request};
use crate::store::{self, Acl, Atom, Draft};

/// What `.memory add` and `memory_add` carry.
#[derive(Clone, Debug, Default)]
pub struct Add {
    pub text: String,
    pub visibility: String,
    pub tag: String,
    pub acl: String,
    pub agent: String,
    /// `user` for something the user said, `agent` for an answer (the two
    /// speakers an L0 turn can have).
    pub speaker: String,
    /// Which conversation the turn belongs to; empty means a fresh id.
    pub conversation: String,
}

/// Append one turn to L0. This is the only write an add does: distillation is
/// queued, never run here.
pub fn add(host: &mut dyn Host, caller: &Caller, add: &Add) -> Result<String, String> {
    if add.text.trim().is_empty() {
        return Err("memory_add needs text".to_string());
    }
    let visibility = if add.visibility.is_empty() {
        "private"
    } else {
        add.visibility.as_str()
    };
    store::check_visibility(visibility)?;
    let acl = Acl::parse(&add.acl)?;
    let speaker = if add.speaker.is_empty() {
        "user"
    } else {
        add.speaker.as_str()
    };
    if !matches!(speaker, "user" | "agent") {
        return Err(format!(
            "unknown speaker \"{speaker}\": expected user or agent"
        ));
    }
    store::ensure_schema(host)?;
    let id = store::add_turn(
        host,
        caller,
        &add.conversation,
        speaker,
        add.text.trim(),
        &add.tag,
        visibility,
        &acl,
        &add.agent,
    )?;
    // Every-N-turns distillation is an operator setting, and it only queues.
    distill::after_add(host, caller)?;
    Ok(id)
}

/// A budgeted, layered search.
pub fn search(host: &mut dyn Host, caller: &Caller, request: &Request) -> Result<Outcome, String> {
    store::ensure_schema(host)?;
    search::search(host, caller, request)
}

/// The caller's persona, if any.
pub fn persona(host: &mut dyn Host, caller: &Caller) -> Result<Option<String>, String> {
    store::ensure_schema(host)?;
    store::persona(host, caller)
}

/// Counts and the newest rows, for `.memory show`.
pub fn show(host: &mut dyn Host, caller: &Caller) -> Result<Summary, String> {
    distill::summary(host, caller)
}

/// The active atoms the caller may see.
pub fn atoms(host: &mut dyn Host, caller: &Caller) -> Result<Vec<Atom>, String> {
    store::ensure_schema(host)?;
    store::atoms(host, caller, true)
}

/// Drain the caller's distillation queue.
pub fn distill(
    host: &mut dyn Host,
    caller: &Caller,
    provider: &mut dyn pinkdb_llm::Provider,
) -> Result<Report, String> {
    store::ensure_schema(host)?;
    distill::drain(host, caller, provider)
}

/// Supersede one atom with a corrected text.
pub fn update(
    host: &mut dyn Host,
    caller: &Caller,
    id: &str,
    text: &str,
) -> Result<String, String> {
    if text.trim().is_empty() {
        return Err("memory_update needs text".to_string());
    }
    store::ensure_schema(host)?;
    store::update_atom(host, caller, id, text.trim())
}

/// Hide one atom, keeping the row and the audit trail.
pub fn forget(host: &mut dyn Host, caller: &Caller, id: &str) -> Result<(), String> {
    store::ensure_schema(host)?;
    store::forget(host, caller, id)
}

/// Parse the `--timeout-ms`/`timeout_ms` budget, defaulting to five seconds.
pub fn timeout_ms(value: Option<u64>) -> Duration {
    Duration::from_millis(value.unwrap_or(5000))
}

/// A draft from an operator's `update`: the same identity as the atom it
/// supersedes.
pub fn draft_for(atom: &Atom, text: &str) -> Draft {
    Draft {
        text: text.to_string(),
        tag: atom.tag.clone(),
        confidence: atom.confidence,
        visibility: atom.visibility.clone(),
        acl: Acl::default(),
        supersedes: atom.id.clone(),
    }
}
