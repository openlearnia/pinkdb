//! `.memory`: the command surface, as the host dispatches it.
//!
//! The CLI hands the text after `.memory` to the loaded extension unchanged,
//! so the grammar lives here. Every operation is the same function the MCP
//! tools call ([`crate::ops`]), so the two surfaces cannot drift — only the
//! rendering differs.

use std::collections::BTreeMap;

use crate::distill::{self, Report, Summary};
use crate::host::{ANSWER, DIAGNOSTIC, Host};
use crate::ops::{self, Add};
use crate::search::{Outcome, Request};

pub const USAGE: &str = "\
.memory add [--visibility private|team|restricted|agent] [--tag TAG] [--acl u:NAME,r:NAME,a:NAME]
            [--speaker user|agent] [--conversation ID] [--agent NAME] <text>
.memory search [--items N] [--chars N] [--timeout-ms N] <query>
.memory update <atom> <text>
.memory forget <atom>
.memory distill [--provider NAME] [--model NAME]
.memory persona
.memory show
";

/// Entry point: run one command, report failures on the diagnostic channel
/// and return the extension's exit code.
pub fn run(session: &mut pinkdb_ext_sdk::CommandSession, args: &str) -> i32 {
    match dispatch(session, args) {
        Ok(()) => 0,
        Err(message) => {
            session.emit(DIAGNOSTIC, &format!("pinkdb memory: {message}\n"));
            1
        }
    }
}

pub fn dispatch(session: &mut dyn Host, args: &str) -> Result<(), String> {
    let words = split(args)?;
    let Some(verb) = words.first() else {
        emit(session, USAGE);
        return Ok(());
    };
    let (positional, flags) = parse(&words[1..])?;
    match verb.as_str() {
        "help" => {
            emit(session, USAGE);
            Ok(())
        }
        "add" => add(session, &positional, &flags),
        "search" => search(session, &positional, &flags),
        "update" => {
            let id = positional
                .first()
                .ok_or("usage: .memory update <atom> <text>")?;
            let text = positional[1..].join(" ");
            if text.trim().is_empty() {
                return Err("usage: .memory update <atom> <text>".to_string());
            }
            let caller = caller(session)?;
            let new = ops::update(session, &caller, id, &text)?;
            emit(session, &format!("updated {id} -> {new}\n"));
            Ok(())
        }
        "forget" => {
            let id = positional.first().ok_or("usage: .memory forget <atom>")?;
            let caller = caller(session)?;
            ops::forget(session, &caller, id)?;
            emit(session, &format!("forgot {id}\n"));
            Ok(())
        }
        "distill" => distill(session, &flags),
        "persona" => {
            let caller = caller(session)?;
            match ops::persona(session, &caller)? {
                Some(persona) => emit(session, &format!("{persona}\n")),
                None => emit(
                    session,
                    "no persona yet: `.memory distill` builds one once there are facts\n",
                ),
            }
            Ok(())
        }
        "show" => show(session),
        other => Err(format!(
            "unknown memory verb \"{other}\": try `.memory help`"
        )),
    }
}

fn add(
    session: &mut dyn Host,
    positional: &[String],
    flags: &BTreeMap<String, String>,
) -> Result<(), String> {
    let text = positional.join(" ");
    if text.trim().is_empty() {
        return Err("usage: .memory add [options] <text>".to_string());
    }
    let caller = caller(session)?;
    let id = ops::add(
        session,
        &caller,
        &Add {
            text,
            visibility: flag(flags, "visibility"),
            tag: flag(flags, "tag"),
            acl: flag(flags, "acl"),
            agent: flag(flags, "agent"),
            speaker: flag(flags, "speaker"),
            conversation: flag(flags, "conversation"),
        },
    )?;
    emit(session, &format!("added turn {id}\n"));
    Ok(())
}

fn search(
    session: &mut dyn Host,
    positional: &[String],
    flags: &BTreeMap<String, String>,
) -> Result<(), String> {
    let query = positional.join(" ");
    if query.trim().is_empty() {
        return Err("usage: .memory search [options] <query>".to_string());
    }
    let request = Request {
        query,
        max_items: number(flags, "items")?.unwrap_or(5) as usize,
        max_chars: number(flags, "chars")?.unwrap_or(4000) as usize,
        timeout: ops::timeout_ms(number(flags, "timeout-ms")?),
    };
    let caller = caller(session)?;
    let outcome = ops::search(session, &caller, &request)?;
    emit(session, &render_search(&outcome));
    Ok(())
}

fn distill(session: &mut dyn Host, flags: &BTreeMap<String, String>) -> Result<(), String> {
    // The provider is resolved first: "no provider configured" must be one
    // clear error, not a queue that half-drained.
    let mut provider = distill::provider(
        optional(flags, "provider").as_deref(),
        optional(flags, "model").as_deref(),
    )?;
    let caller = caller(session)?;
    let report = ops::distill(session, &caller, provider.as_mut())?;
    emit(session, &render_report(&report));
    Ok(())
}

fn show(session: &mut dyn Host) -> Result<(), String> {
    let caller = caller(session)?;
    let summary = ops::show(session, &caller)?;
    emit(session, &render_show(&summary));
    Ok(())
}

/// The caller, as the host answers it. One helper so every verb asks the same
/// way (`SELECT current_user(), current_roles()` on the internal channel).
pub(crate) fn caller(host: &mut dyn Host) -> Result<crate::host::Caller, String> {
    crate::host::Caller::of(host)
}

// ---------------------------------------------------------------------------
// Rendering: the same text the MCP tools return
// ---------------------------------------------------------------------------

/// A search answer: the persona, the scenarios that matched, then the ranked
/// hits with their layer, id, score and source, and finally the mode and the
/// budget that stopped the list.
pub(crate) fn render_search(outcome: &Outcome) -> String {
    let mut out = String::new();
    if let Some(persona) = &outcome.persona {
        out.push_str(&format!("persona: {persona}\n"));
    }
    for scenario in &outcome.scenarios {
        out.push_str(&format!(
            "{} {} ({}): {}\n",
            scenario.id,
            scenario.title,
            scenario.atom_ids.len(),
            scenario.summary
        ));
    }
    for hit in &outcome.hits {
        out.push_str(&format!(
            "{} {} {:.4} {} {} {}\n",
            hit.layer,
            hit.id,
            hit.score,
            hit.source.name(),
            hit.visibility,
            hit.text
        ));
    }
    if out.is_empty() {
        out.push_str("nothing remembered matches that yet\n");
    }
    let mode = match outcome.mode {
        Some(crate::search::Mode::Hybrid) => "hybrid",
        _ => "bm25",
    };
    out.push_str(&format!("mode: {mode}: {}\n", outcome.note));
    if let Some(stopped) = outcome.stopped {
        out.push_str(&format!("stopped: {}\n", stopped.name()));
    }
    out
}

pub(crate) fn render_persona(persona: &Option<String>) -> String {
    match persona {
        Some(persona) => format!("{persona}\n"),
        None => "no persona yet: run `memory_distill` once there are facts\n".to_string(),
    }
}

pub(crate) fn render_report(report: &Report) -> String {
    let mut out = String::new();
    for step in &report.steps {
        out.push_str(step);
        out.push('\n');
    }
    out.push_str(&format!(
        "distilled {} turns into {} atoms, {} scenarios{}; {} failed\n",
        report.turns,
        report.atoms,
        report.scenarios,
        if report.persona {
            ", persona updated"
        } else {
            ""
        },
        report.failed
    ));
    out
}

pub(crate) fn render_show(summary: &Summary) -> String {
    let mut out = format!(
        "turns: {}  atoms: {}  scenarios: {}  queued: {}\n",
        summary.turns,
        summary.atoms.len(),
        summary.scenarios.len(),
        summary.pending
    );
    match &summary.persona {
        Some(persona) => out.push_str(&format!("persona: {persona}\n")),
        None => out.push_str("persona: (none)\n"),
    }
    for atom in summary.atoms.iter().take(20) {
        out.push_str(&format!(
            "{} {} {} {}\n",
            atom.id, atom.visibility, atom.tag, atom.text
        ));
    }
    for scenario in summary.scenarios.iter().take(10) {
        out.push_str(&format!(
            "{} {}: {}\n",
            scenario.id, scenario.title, scenario.summary
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// Arguments
// ---------------------------------------------------------------------------

fn emit(session: &mut dyn Host, text: &str) {
    session.emit(ANSWER, text);
}

fn flag(flags: &BTreeMap<String, String>, name: &str) -> String {
    flags.get(name).cloned().unwrap_or_default()
}

fn optional(flags: &BTreeMap<String, String>, name: &str) -> Option<String> {
    flags.get(name).cloned()
}

/// A numeric flag: `None` when absent, an error when it is not a number.
fn number(flags: &BTreeMap<String, String>, name: &str) -> Result<Option<u64>, String> {
    match flags.get(name) {
        None => Ok(None),
        Some(value) => value
            .parse()
            .map(Some)
            .map_err(|_| format!("--{name} needs a number, not \"{value}\"")),
    }
}

/// Split words the way the CLI does, honouring `"…"` and `\` escapes.
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

/// `--flag value` into a map, everything else into the positional words.
fn parse(words: &[String]) -> Result<(Vec<String>, BTreeMap<String, String>), String> {
    let mut positional = Vec::new();
    let mut flags = BTreeMap::new();
    let mut iter = words.iter();
    while let Some(word) = iter.next() {
        match word.strip_prefix("--") {
            Some(name) => {
                let Some(value) = iter.next() else {
                    return Err(format!("--{name} needs a value"));
                };
                flags.insert(name.to_string(), value.clone());
            }
            None => positional.push(word.clone()),
        }
    }
    Ok((positional, flags))
}
