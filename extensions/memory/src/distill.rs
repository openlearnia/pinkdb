//! Distillation: L0 turns → L1 atoms → L2 scenarios → L3 persona.
//!
//! Three properties decide the shape of this module:
//!
//! - **Never on the path of an add.** `memory_add` appends a turn and, when
//!   the operator asked for it, queues a job; only [`drain`] calls a model.
//! - **One model call per step, with a JSON contract.** The answer is parsed
//!   and validated *before* anything is written, so a malformed answer changes
//!   nothing and is retried on a later drain (attempts are counted in the
//!   queue; after [`MAX_ATTEMPTS`] the job is `failed` and stays for an
//!   operator to read).
//! - **No provider, no problem.** The provider table, its keys and its wire
//!   protocols come from `pinkdb-llm`, which has no default provider: a store
//!   with no provider configured answers a clear error from `distill`, while
//!   `add` and `search` keep working.

use pinkdb_llm::{Message, Provider};

use crate::host::{Caller, Host};
use crate::store::{self, Acl, Atom, Draft, Scenario};

/// How often a job may fail before it stops being retried.
pub const MAX_ATTEMPTS: i32 = 3;
/// How many turns one L0 step reads: a conversation segment, not a transcript.
const TURN_BATCH: usize = 24;
/// How many atoms one L2 step groups.
const ATOM_BATCH: usize = 40;
/// The longest one atom's text may be; a longer answer is truncated by the
/// validator rather than written whole.
const ATOM_CHARS: usize = 500;
/// The environment variable that turns "distill every N turns" on.
pub const EVERY_VARIABLE: &str = "PINKDB_MEMORY_DISTILL_EVERY";

/// What one drain did.
#[derive(Clone, Debug, Default)]
pub struct Report {
    pub turns: usize,
    pub atoms: usize,
    pub scenarios: usize,
    pub persona: bool,
    pub failed: usize,
    /// One line per step that ran, in order, for `.memory distill` to print.
    pub steps: Vec<String>,
}

/// The provider the caller named, or the environment's. An error here is the
/// "no default provider" answer from `pinkdb-llm`, passed through unchanged.
pub fn provider(name: Option<&str>, model: Option<&str>) -> Result<Box<dyn Provider>, String> {
    let config = pinkdb_llm::Config::from_env(name, model)?;
    Ok(Box::new(pinkdb_llm::Client::new(config)))
}

/// Drain this caller's queue: every pending job, then any undistilled turns.
/// The L0 step queues the L2 and L3 steps, so one drain builds the whole
/// pyramid. `Ok` with a report does not mean every job succeeded — `failed`
/// counts the ones that did not, and each one's error is in the queue.
pub fn drain(
    host: &mut dyn Host,
    caller: &Caller,
    provider: &mut dyn Provider,
) -> Result<Report, String> {
    store::ensure_schema(host)?;
    store::retry_failed(host, caller)?;
    let mut report = Report::default();
    // Bounded so a store with a corrupt job cannot loop forever: each pass
    // either consumes a job or makes progress on the turns.
    for _ in 0..4 * MAX_ATTEMPTS as usize + 4 {
        let jobs = store::pending(host, caller)?;
        let Some(job) = jobs.first() else {
            // No queued work: distill whatever turns are still raw, but only
            // if this pass has not already failed something — a batch that
            // keeps failing must not be re-queued in the same pass.
            if report.failed > 0 || store::undistilled_turns(host, caller, 1)?.is_empty() {
                break;
            }
            store::enqueue(host, caller, "turn", "")?;
            continue;
        };
        let job = job.clone();
        let step = match job.kind.as_str() {
            "turn" => step_turns(host, caller, provider, &mut report),
            "scenario" => step_scenarios(host, caller, provider, &mut report),
            "persona" => step_persona(host, caller, provider, &mut report),
            other => {
                let error = format!("unknown distillation step \"{other}\"");
                store::fail_job(host, &job.id, job.attempts + 1, &error)?;
                report.failed += 1;
                report.steps.push(format!("{other}: {error}"));
                continue;
            }
        };
        match step {
            Ok(()) => store::finish_job(host, &job.id)?,
            Err(error) => {
                report.failed += 1;
                store::fail_job(host, &job.id, job.attempts + 1, &error)?;
                report.steps.push(format!("{}: {error}", job.kind));
            }
        }
    }
    Ok(report)
}

/// Queue the steps an add asks for. Called after a turn is written; with
/// `PINKDB_MEMORY_DISTILL_EVERY=N` every Nth turn queues a drain, otherwise an
/// add never queues anything and `distill` is entirely an operator's decision.
pub fn after_add(host: &mut dyn Host, caller: &Caller) -> Result<bool, String> {
    let every = std::env::var(EVERY_VARIABLE)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0);
    if every == 0 {
        return Ok(false);
    }
    let turns = store::turn_count(host, caller)?;
    if turns == 0 || !turns.is_multiple_of(every) {
        return Ok(false);
    }
    store::enqueue(host, caller, "turn", "")?;
    Ok(true)
}

/// L0 → L1: read the undistilled turns, one model call, atoms.
fn step_turns(
    host: &mut dyn Host,
    caller: &Caller,
    provider: &mut dyn Provider,
    report: &mut Report,
) -> Result<(), String> {
    let turns = store::undistilled_turns(host, caller, TURN_BATCH)?;
    if turns.is_empty() {
        return Ok(());
    }
    let mut user = String::from("Conversation:\n");
    for turn in &turns {
        if turn.tag.is_empty() {
            user.push_str(&format!("[{}] {}\n", turn.speaker, turn.content));
        } else {
            user.push_str(&format!(
                "[{}] ({}) {}\n",
                turn.speaker, turn.tag, turn.content
            ));
        }
    }
    // The facts the caller already has, so a contradicting fact can name the
    // atom it replaces; capped, because this prompt is the whole point of a
    // bounded step.
    let known = store::atoms(host, caller, true)?;
    if !known.is_empty() {
        user.push_str("\nKnown facts:\n");
        for atom in known.iter().take(ATOM_BATCH) {
            user.push_str(&format!("{}: {}\n", atom.id, truncate(&atom.text, 160)));
        }
    }
    let answer = ask(host, caller, provider, "turn", SYSTEM_L0, &user)?;
    let parsed = parse(&answer, "L0")?;
    let mut drafts = Vec::new();
    for fact in array(&parsed, "atoms", "L0")?.iter().take(32) {
        let fact_text = field_text(fact, "text");
        if fact_text.is_empty() {
            continue;
        }
        drafts.push(Draft {
            text: truncate(&fact_text, ATOM_CHARS),
            tag: tag(&field_text(fact, "tag")),
            confidence: field_f64(fact, "confidence").clamp(0.0, 1.0),
            // The distiller reads only the caller's own turns, so what it
            // learns is the caller's own: private unless an operator widens
            // it later through `memory_update`'s visibility argument.
            visibility: "private".to_string(),
            acl: Acl::default(),
            supersedes: field_text(fact, "supersedes"),
        });
    }
    // Everything above is validated; only now does anything get written.
    let mut ids = Vec::new();
    for draft in &drafts {
        ids.push(store::add_atom(host, caller, draft, "")?);
    }
    let turn_ids: Vec<String> = turns.iter().map(|turn| turn.id.clone()).collect();
    store::mark_distilled(host, &turn_ids)?;
    report.turns += turns.len();
    report.atoms += ids.len();
    report
        .steps
        .push(format!("{} turns -> {} atoms", turns.len(), ids.len()));
    // The next levels have new input; queue them so one drain builds the
    // pyramid and a later drain can top up the same way.
    store::enqueue(host, caller, "scenario", "")?;
    store::enqueue(host, caller, "persona", "")?;
    Ok(())
}

/// L1 → L2: group the atoms no scenario has grouped yet.
fn step_scenarios(
    host: &mut dyn Host,
    caller: &Caller,
    provider: &mut dyn Provider,
    report: &mut Report,
) -> Result<(), String> {
    let atoms = store::atoms(host, caller, true)?;
    let scenarios = store::scenarios(host, caller)?;
    let grouped: std::collections::HashSet<String> = scenarios
        .iter()
        .flat_map(|scenario| scenario.atom_ids.iter().cloned())
        .collect();
    let fresh: Vec<&Atom> = atoms
        .iter()
        .filter(|atom| !grouped.contains(&atom.id))
        .take(ATOM_BATCH)
        .collect();
    if fresh.is_empty() {
        return Ok(());
    }
    let mut user = String::from("Facts:\n");
    for atom in &fresh {
        user.push_str(&format!("{} ({}): {}\n", atom.id, atom.tag, atom.text));
    }
    let answer = ask(host, caller, provider, "scenario", SYSTEM_L2, &user)?;
    let parsed = parse(&answer, "L2")?;
    let known: std::collections::HashSet<&str> =
        fresh.iter().map(|atom| atom.id.as_str()).collect();
    let mut drafts: Vec<(String, String, Vec<String>, String)> = Vec::new();
    for scenario in array(&parsed, "scenarios", "L2")?.iter().take(8) {
        let title = field_text(scenario, "title");
        let summary = field_text(scenario, "summary");
        if title.is_empty() || summary.is_empty() {
            continue;
        }
        let ids = field_ids(scenario, "atoms")
            .into_iter()
            .filter(|id| known.contains(id.as_str()))
            .collect::<Vec<_>>();
        if ids.is_empty() {
            // A scenario about no fact it was shown is a hallucination, not
            // knowledge: drop it rather than write an unsupported summary.
            continue;
        }
        drafts.push((
            truncate(&title, 120),
            truncate(&summary, 2000),
            ids,
            tag(&field_text(scenario, "tag")),
        ));
    }
    let mut written = 0;
    for (title, summary, ids, tag) in &drafts {
        store::add_scenario(host, caller, title, summary, ids, tag)?;
        written += 1;
    }
    report.scenarios += written;
    report
        .steps
        .push(format!("{} atoms -> {written} scenarios", fresh.len()));
    Ok(())
}

/// L1+L2 → L3: the caller's profile.
fn step_persona(
    host: &mut dyn Host,
    caller: &Caller,
    provider: &mut dyn Provider,
    report: &mut Report,
) -> Result<(), String> {
    let atoms = store::atoms(host, caller, true)?;
    let scenarios = store::scenarios(host, caller)?;
    if atoms.is_empty() {
        return Ok(());
    }
    let mut user = String::from("Facts:\n");
    for atom in atoms.iter().take(ATOM_BATCH) {
        user.push_str(&format!("- {}\n", atom.text));
    }
    if !scenarios.is_empty() {
        user.push_str("Projects:\n");
        for scenario in scenarios.iter().take(8) {
            user.push_str(&format!("- {}: {}\n", scenario.title, scenario.summary));
        }
    }
    let answer = ask(host, caller, provider, "persona", SYSTEM_L3, &user)?;
    let parsed = parse(&answer, "L3")?;
    let persona = field_text(&parsed, "persona");
    if persona.is_empty() {
        return Err("the model's L3 answer has an empty persona".to_string());
    }
    store::add_persona(host, caller, &truncate(&persona, 4000))?;
    report.persona = true;
    report.steps.push("persona updated".to_string());
    Ok(())
}

// ---------------------------------------------------------------------------
// The contract and the call
// ---------------------------------------------------------------------------

const SYSTEM_L0: &str = "You distill an agent conversation into long-term facts. \
Answer with JSON only, no prose and no code fence: \
{\"atoms\":[{\"text\":\"one standalone fact\",\"confidence\":0.0-1.0,\"tag\":\"project\",\
\"supersedes\":\"atom id or empty\"}]}. \
A fact states something that stays true after the conversation ends (a name, a \
preference, a schema, a decision). Never restate the question, never guess, and \
never include a secret, a key or a password. If a fact replaces an earlier atom \
you were shown, name that atom's id in supersedes. A turn may start with a tag \
in parentheses: use it as the tag of the facts you take from that turn when it \
fits. At most 12 atoms.";

const SYSTEM_L2: &str = "You group facts about one project into scenarios. \
Answer with JSON only, no prose and no code fence: \
{\"scenarios\":[{\"title\":\"short title\",\"summary\":\"two or three sentences\",\
\"atoms\":[\"atom ids you were given\"],\"tag\":\"project\"}]}. \
Group only facts that belong together, cite only ids you were given, and write no \
scenario for a fact that belongs to no project. At most 5 scenarios.";

const SYSTEM_L3: &str = "You write a long-term profile of one user for an agent. \
Answer with JSON only, no prose and no code fence: {\"persona\":\"...\"}. \
Two to six sentences: what they work on, the tools and conventions they prefer, and \
what they have asked an agent to avoid. Use only the facts and projects given. No \
secrets, no guesses, no flattery.";

/// Parse one step's answer. Anything that is not a JSON object fails here,
/// before the step looks at a field, so a stray sentence is one error rather
/// than a silent empty answer.
fn parse(answer: &str, step: &str) -> Result<serde_json::Value, String> {
    let parsed: serde_json::Value = serde_json::from_str(answer)
        .map_err(|error| format!("the model's {step} answer is not the JSON contract: {error}"))?;
    if !parsed.is_object() {
        return Err(format!("the model's {step} answer is not a JSON object"));
    }
    Ok(parsed)
}

/// One contract field that must be an array.
fn array<'a>(
    answer: &'a serde_json::Value,
    field: &str,
    step: &str,
) -> Result<&'a [serde_json::Value], String> {
    answer
        .get(field)
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("the model's {step} answer has no \"{field}\" array"))
}

fn field_text(item: &serde_json::Value, name: &str) -> String {
    item.get(name)
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string()
}

fn field_f64(item: &serde_json::Value, name: &str) -> f64 {
    item.get(name)
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.7)
}

fn field_ids(item: &serde_json::Value, name: &str) -> Vec<String> {
    item.get(name)
        .and_then(serde_json::Value::as_array)
        .map(|ids| {
            ids.iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// One model call: log it, and strip the code fence a model adds half the
/// time even when told not to.
fn ask(
    host: &mut dyn Host,
    caller: &Caller,
    provider: &mut dyn Provider,
    kind: &str,
    system: &str,
    user: &str,
) -> Result<String, String> {
    let messages = [Message::system(system), Message::user(user)];
    let turn = provider.complete(&messages, &[]);
    match turn {
        Ok(turn) => {
            store::record_run(
                host,
                caller,
                kind,
                provider.provider(),
                provider.model(),
                "ok",
                "",
            )?;
            Ok(unfence(&turn.content))
        }
        Err(error) => {
            store::record_run(
                host,
                caller,
                kind,
                provider.provider(),
                provider.model(),
                "error",
                &error,
            )?;
            Err(error)
        }
    }
}

/// Strip a surrounding Markdown code fence, if the model added one.
pub fn unfence(answer: &str) -> String {
    let trimmed = answer.trim();
    let Some(rest) = trimmed.strip_prefix("```") else {
        return trimmed.to_string();
    };
    let rest = rest.split_once('\n').map(|(_, body)| body).unwrap_or(rest);
    rest.strip_suffix("```").unwrap_or(rest).trim().to_string()
}

/// A tag is one word of letters, digits, `_` or `-`; anything else is dropped
/// rather than written (a tag is a filter, not prose).
fn tag(raw: &str) -> String {
    let tag: String = raw
        .trim()
        .chars()
        .filter(|c| c.is_alphanumeric() || matches!(c, '_' | '-'))
        .collect();
    truncate(&tag, 32)
}

/// Cut to `chars` characters on a character boundary, appending `…` so a
/// truncated value is visibly truncated.
fn truncate(text: &str, chars: usize) -> String {
    if text.chars().count() <= chars {
        return text.to_string();
    }
    let mut out: String = text.chars().take(chars.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// The atoms and scenarios a `memory_show` prints, from the store's own rows.
pub fn summary(host: &mut dyn Host, caller: &Caller) -> Result<Summary, String> {
    store::ensure_schema(host)?;
    Ok(Summary {
        turns: store::turn_count(host, caller)?,
        atoms: store::atoms(host, caller, true)?,
        scenarios: store::scenarios(host, caller)?,
        persona: store::persona(host, caller)?,
        pending: store::pending_count(host, caller)?,
    })
}

/// What `.memory show` prints: counts and the newest rows.
pub struct Summary {
    pub turns: u64,
    pub atoms: Vec<Atom>,
    pub scenarios: Vec<Scenario>,
    pub persona: Option<String>,
    pub pending: u64,
}
