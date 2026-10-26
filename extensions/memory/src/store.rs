//! The `pinkdb_memory` database: schema, identity columns, visibility, and
//! the statements every operation writes.
//!
//! Every row in every table carries the same seven identity columns —
//! `owner`, `team`, `visibility`, `acl_users`, `acl_roles`, `acl_agents`,
//! `agent` — and every read goes through [`visible`], so there is one place
//! to look for "who may see this". The database is written only on the
//! host's internal channel, confined to it by
//! `ServePolicy::write_database = "pinkdb_memory"`.
//!
//! Nothing here hard-deletes: `forget` sets `status = 'forgotten'` and appends
//! an audit row, and `update` writes a new atom that supersedes the old one, so
//! "what did I believe last week" stays answerable.

use crate::host::{Caller, Host, cell_f64, cell_str, cell_u64, query};

/// The database this extension owns. Nothing else may write it.
pub const DATABASE: &str = "pinkdb_memory";

/// The identity columns, in the order every insert below lists them.
const IDENTITY: &str = "owner, team, visibility, acl_users, acl_roles, acl_agents, agent";

const SCHEMA: [&str; 7] = [
    // L0: the conversation as it happened.
    "CREATE TABLE IF NOT EXISTS pinkdb_memory.l0_turns(id VARCHAR, owner VARCHAR, team VARCHAR, \
     visibility VARCHAR, acl_users VARCHAR, acl_roles VARCHAR, acl_agents VARCHAR, agent VARCHAR, \
     conversation VARCHAR, speaker VARCHAR, content VARCHAR, tag VARCHAR, created_at BIGINT, \
     distilled BOOLEAN)",
    // L1: standalone facts, with provenance and supersession.
    "CREATE TABLE IF NOT EXISTS pinkdb_memory.atoms(id VARCHAR, owner VARCHAR, team VARCHAR, \
     visibility VARCHAR, acl_users VARCHAR, acl_roles VARCHAR, acl_agents VARCHAR, agent VARCHAR, \
     text VARCHAR, confidence DOUBLE, status VARCHAR, superseded_by VARCHAR, source_l0 VARCHAR, \
     tag VARCHAR, created_at BIGINT, updated_at BIGINT)",
    // L2: project knowledge grouping atoms.
    "CREATE TABLE IF NOT EXISTS pinkdb_memory.scenarios(id VARCHAR, owner VARCHAR, team VARCHAR, \
     visibility VARCHAR, acl_users VARCHAR, acl_roles VARCHAR, acl_agents VARCHAR, agent VARCHAR, \
     title VARCHAR, summary VARCHAR, atom_ids VARCHAR, tag VARCHAR, created_at BIGINT, \
     updated_at BIGINT)",
    // L3: one long-term profile per principal; append-only, latest wins.
    "CREATE TABLE IF NOT EXISTS pinkdb_memory.personas(id VARCHAR, owner VARCHAR, team VARCHAR, \
     visibility VARCHAR, acl_users VARCHAR, acl_roles VARCHAR, acl_agents VARCHAR, agent VARCHAR, \
     text VARCHAR, tag VARCHAR, created_at BIGINT)",
    // The distillation queue: queued here, never on the path of an add.
    "CREATE TABLE IF NOT EXISTS pinkdb_memory.distill_queue(id VARCHAR, owner VARCHAR, team VARCHAR, \
     visibility VARCHAR, acl_users VARCHAR, acl_roles VARCHAR, acl_agents VARCHAR, agent VARCHAR, \
     kind VARCHAR, subject VARCHAR, state VARCHAR, attempts INTEGER, last_error VARCHAR, \
     created_at BIGINT, updated_at BIGINT)",
    // Every change, so a forget is visible as a change and not as a gap.
    "CREATE TABLE IF NOT EXISTS pinkdb_memory.audit(id VARCHAR, at BIGINT, actor VARCHAR, \
     action VARCHAR, subject VARCHAR, detail VARCHAR)",
    // One row per model call, so a queue row's last_error has context.
    "CREATE TABLE IF NOT EXISTS pinkdb_memory.runs(id VARCHAR, owner VARCHAR, kind VARCHAR, \
     provider VARCHAR, model VARCHAR, state VARCHAR, error VARCHAR, created_at BIGINT)",
];

/// Create the database and its tables. Idempotent: every statement is
/// `IF NOT EXISTS`, so the extension calls it before every write.
pub fn ensure_schema(host: &mut dyn Host) -> Result<(), String> {
    let mut sql = format!("CREATE DATABASE IF NOT EXISTS {DATABASE}");
    for statement in SCHEMA {
        sql.push_str("; ");
        sql.push_str(statement);
    }
    host.internal(&sql).map(|_| ())
}

/// A fresh id: one letter and sixteen hex digits (microseconds + a counter),
/// so two rows written in the same microsecond still differ.
pub fn new_id(prefix: char) -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    format!(
        "{prefix}{micros:012x}{:04x}",
        COUNTER.fetch_add(1, Ordering::Relaxed) & 0xffff
    )
}

/// Microseconds since the epoch: all timestamps in this database.
pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// A SQL text literal. The text a user or a model writes is the only thing
/// here that is not ours, so every string goes through this.
pub fn text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// `NULL` for an empty string, so a column's "unset" is one value.
fn opt_text(value: &str) -> String {
    if value.is_empty() {
        "NULL".to_string()
    } else {
        text(value)
    }
}

/// Run one write batch on the internal channel.
pub fn write(host: &mut dyn Host, sql: &str) -> Result<(), String> {
    host.internal(sql).map(|_| ())
}

/// The extra readers of a `restricted` row.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Acl {
    pub users: Vec<String>,
    pub roles: Vec<String>,
    pub agents: Vec<String>,
}

impl Acl {
    /// Parse `u:alice,r:ops,a:bot` as the CLI and the `memory_add` tool write
    /// it. An empty string is no extra readers.
    pub fn parse(text: &str) -> Result<Acl, String> {
        let mut acl = Acl::default();
        for item in text.split(',').map(str::trim).filter(|i| !i.is_empty()) {
            let Some((kind, name)) = item.split_once(':') else {
                return Err(format!(
                    "invalid ACL entry \"{item}\": expected u:USER, r:ROLE or a:AGENT"
                ));
            };
            let name = name.trim();
            if name.is_empty() {
                return Err(format!("invalid ACL entry \"{item}\": no name"));
            }
            match kind.trim() {
                "u" | "user" => acl.users.push(name.to_string()),
                "r" | "role" => acl.roles.push(name.to_string()),
                "a" | "agent" => acl.agents.push(name.to_string()),
                other => {
                    return Err(format!(
                        "invalid ACL entry \"{item}\": \"{other}\" is not u, r or a"
                    ));
                }
            }
        }
        Ok(acl)
    }

    /// The three ACL columns, as SQL values.
    fn sql(&self) -> String {
        format!(
            "{}, {}, {}",
            opt_text(&self.users.join(",")),
            opt_text(&self.roles.join(",")),
            opt_text(&self.agents.join(","))
        )
    }
}

// ---------------------------------------------------------------------------
// Visibility
// ---------------------------------------------------------------------------

/// The one predicate every read applies. `alias` is the table's name (or its
/// alias) in the statement, so a self-join can ask it twice.
///
/// - `private`: the owner.
/// - `team`: the owner, and members of the row's `team` role.
/// - `restricted`: the owner, and the users/roles named in the ACL columns.
/// - `agent`: the owner, and the principal the row names as its agent.
///
/// ACL columns are comma-joined names (`','||column||',' LIKE '%,name,%'`
/// cannot match a prefix of a longer name); role names cannot contain a comma,
/// and a user name cannot either (both are engine identifiers).
pub fn visible(alias: &str, caller: &Caller) -> String {
    let user = text(&caller.user);
    let mut clauses = vec![format!("{alias}.owner = {user}")];
    if !caller.roles.is_empty() {
        let teams = caller
            .roles
            .iter()
            .map(|role| text(role))
            .collect::<Vec<_>>()
            .join(", ");
        clauses.push(format!(
            "({alias}.visibility = 'team' AND {alias}.team IN ({teams}))"
        ));
    }
    let mut members = vec![named("{alias}.acl_users", &caller.user)];
    for role in &caller.roles {
        members.push(named("{alias}.acl_roles", role));
    }
    members.push(named("{alias}.acl_agents", &caller.user));
    clauses.push(format!(
        "({alias}.visibility = 'restricted' AND ({}))",
        members.join(" OR ").replace("{alias}", alias)
    ));
    clauses.push(format!(
        "({alias}.visibility = 'agent' AND {alias}.agent = {user})"
    ));
    format!("({})", clauses.join(" OR "))
}

/// One ACL membership test; `{alias}` is filled in by the caller.
fn named(column: &str, name: &str) -> String {
    format!(
        "','||coalesce({column},'')||',' LIKE {}",
        text(&format!("%,{name},%"))
    )
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

/// One L0 turn.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Turn {
    pub id: String,
    pub speaker: String,
    pub content: String,
    /// The operator's hint for this turn, if any: the L0 step passes it to the
    /// model, which may use it as the tag of the facts it finds there.
    pub tag: String,
    pub created_at: u64,
}

/// One L1 atom.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Atom {
    pub id: String,
    pub text: String,
    pub tag: String,
    pub confidence: f64,
    pub status: String,
    pub superseded_by: String,
    pub source_l0: String,
    pub owner: String,
    pub visibility: String,
    pub agent: String,
    pub updated_at: u64,
}

impl Atom {
    /// Whether this atom may still answer a query: not forgotten, and not
    /// superseded by a newer one.
    pub fn active(&self) -> bool {
        self.status == "active"
    }
}

/// One L2 scenario.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Scenario {
    pub id: String,
    pub title: String,
    pub summary: String,
    /// The atom ids this scenario groups, in the order the distiller named
    /// them.
    pub atom_ids: Vec<String>,
    pub tag: String,
    pub owner: String,
    pub visibility: String,
    pub updated_at: u64,
}

/// One queue row.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Job {
    pub id: String,
    pub kind: String,
    pub subject: String,
    pub attempts: i32,
    pub last_error: String,
}

/// What the extension writes for a new atom: everything but the id and the
/// timestamps.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Draft {
    pub text: String,
    pub tag: String,
    pub confidence: f64,
    pub visibility: String,
    pub acl: Acl,
    /// The atom this one contradicts or replaces, if any.
    pub supersedes: String,
}

const ATOM_COLUMNS: &str = "id, text, tag, confidence, status, superseded_by, source_l0, owner, \
     visibility, agent, updated_at";

fn atom_row(row: &[serde_json::Value]) -> Atom {
    Atom {
        id: cell_str(row, 0),
        text: cell_str(row, 1),
        tag: cell_str(row, 2),
        confidence: cell_f64(row, 3),
        status: cell_str(row, 4),
        superseded_by: cell_str(row, 5),
        source_l0: cell_str(row, 6),
        owner: cell_str(row, 7),
        visibility: cell_str(row, 8),
        agent: cell_str(row, 9),
        updated_at: cell_u64(row, 10),
    }
}

/// The identity columns of a row written by `caller`, as SQL values.
fn identity(caller: &Caller, visibility: &str, agent: &str, acl: &Acl) -> String {
    let team = caller.roles.first().map(String::as_str).unwrap_or("");
    format!(
        "{}, {}, {}, {}, {}",
        text(&caller.user),
        text(team),
        text(visibility),
        acl.sql(),
        opt_text(agent)
    )
}

/// Reject a visibility this store does not know. A typo must not become a
/// row nobody can read.
pub fn check_visibility(visibility: &str) -> Result<(), String> {
    match visibility {
        "private" | "team" | "restricted" | "agent" => Ok(()),
        other => Err(format!(
            "unknown visibility \"{other}\": expected private, team, restricted or agent"
        )),
    }
}

// ---------------------------------------------------------------------------
// Conversation (L0)
// ---------------------------------------------------------------------------

/// Append one turn and return its id.
#[allow(clippy::too_many_arguments)] // a turn is its body plus the identity it is filed under
pub fn add_turn(
    host: &mut dyn Host,
    caller: &Caller,
    conversation: &str,
    speaker: &str,
    content: &str,
    tag: &str,
    visibility: &str,
    acl: &Acl,
    agent: &str,
) -> Result<String, String> {
    check_visibility(visibility)?;
    let id = new_id('t');
    let sql = format!(
        "INSERT INTO {DATABASE}.l0_turns(id, {IDENTITY}, conversation, speaker, content, tag, \
         created_at, distilled) VALUES ({}, {}, {}, {}, {}, {}, {}, false)",
        text(&id),
        identity(caller, visibility, agent, acl),
        text(conversation),
        text(speaker),
        text(content),
        text(tag),
        now()
    );
    write(host, &sql)?;
    Ok(id)
}

/// The turns this caller wrote and that no distillation step has read yet.
pub fn undistilled_turns(
    host: &mut dyn Host,
    caller: &Caller,
    limit: usize,
) -> Result<Vec<Turn>, String> {
    let sql = format!(
        "SELECT id, speaker, content, tag, created_at FROM {DATABASE}.l0_turns WHERE owner = {} \
         AND distilled = false ORDER BY created_at LIMIT {limit}",
        text(&caller.user)
    );
    Ok(query(host, &sql)?
        .iter()
        .map(|row| Turn {
            id: cell_str(row, 0),
            speaker: cell_str(row, 1),
            content: cell_str(row, 2),
            tag: cell_str(row, 3),
            created_at: cell_u64(row, 4),
        })
        .collect())
}

/// Mark turns as read by a distillation step.
pub fn mark_distilled(host: &mut dyn Host, ids: &[String]) -> Result<(), String> {
    if ids.is_empty() {
        return Ok(());
    }
    let ids = ids.iter().map(|id| text(id)).collect::<Vec<_>>().join(", ");
    write(
        host,
        &format!("UPDATE {DATABASE}.l0_turns SET distilled = true WHERE id IN ({ids})"),
    )
}

/// How many turns this caller wrote, distilled or not.
pub fn turn_count(host: &mut dyn Host, caller: &Caller) -> Result<u64, String> {
    let sql = format!(
        "SELECT count(*) FROM {DATABASE}.l0_turns WHERE owner = {}",
        text(&caller.user)
    );
    Ok(query(host, &sql)?
        .first()
        .map(|row| cell_u64(row, 0))
        .unwrap_or(0))
}

// ---------------------------------------------------------------------------
// Atoms (L1)
// ---------------------------------------------------------------------------

/// Write one atom. `supersedes` marks the old atom superseded and points it at
/// the new one; the old row stays, so history stays linear.
///
/// Returns the new atom's id, or the existing atom's id when an identical
/// active text is already visible to the caller (dedupe: the same fact is one
/// row, not two).
pub fn add_atom(
    host: &mut dyn Host,
    caller: &Caller,
    draft: &Draft,
    agent: &str,
) -> Result<String, String> {
    check_visibility(&draft.visibility)?;
    let text_value = text(&draft.text);
    let existing = format!(
        "SELECT id FROM {DATABASE}.atoms WHERE text = {text_value} AND status = 'active' AND {} \
         LIMIT 1",
        visible("atoms", caller)
    );
    let found = query(host, &existing)?;
    if let Some(id) = found.first().map(|row| cell_str(row, 0))
        && !id.is_empty()
    {
        return Ok(id);
    }
    let id = new_id('a');
    let at = now();
    let mut sql = format!(
        "INSERT INTO {DATABASE}.atoms(id, {IDENTITY}, text, confidence, status, superseded_by, \
         source_l0, tag, created_at, updated_at) VALUES ({}, {}, {}, {}, 'active', NULL, NULL, {}, \
         {at}, {at})",
        text(&id),
        identity(caller, &draft.visibility, agent, &draft.acl),
        text_value,
        draft.confidence,
        opt_text(&draft.tag)
    );
    if !draft.supersedes.is_empty() {
        // Only a row the caller can see may be superseded, and only one that
        // is still active: superseding a forgotten atom would resurrect it.
        sql.push_str(&format!(
            "; UPDATE {DATABASE}.atoms SET status = 'superseded', superseded_by = {}, \
             updated_at = {} WHERE id = {} AND status = 'active' AND {}",
            text(&id),
            at,
            text(&draft.supersedes),
            visible("atoms", caller)
        ));
        sql.push_str(&format!(
            "; INSERT INTO {DATABASE}.audit(id, at, actor, action, subject, detail) VALUES ({}, {}, \
             {}, 'supersede', {}, {})",
            text(&new_id('u')),
            at,
            text(&caller.user),
            text(&draft.supersedes),
            text(&format!("superseded by {id}"))
        ));
    }
    write(host, &sql)?;
    Ok(id)
}

/// The atoms `caller` may see, newest first.
pub fn atoms(host: &mut dyn Host, caller: &Caller, active_only: bool) -> Result<Vec<Atom>, String> {
    let status = if active_only {
        " AND atoms.status = 'active'"
    } else {
        ""
    };
    let sql = format!(
        "SELECT {ATOM_COLUMNS} FROM {DATABASE}.atoms AS atoms WHERE {}{status} \
         ORDER BY atoms.created_at DESC",
        visible("atoms", caller)
    );
    Ok(query(host, &sql)?.iter().map(|row| atom_row(row)).collect())
}

/// One atom by id, if `caller` may see it.
pub fn atom(host: &mut dyn Host, caller: &Caller, id: &str) -> Result<Option<Atom>, String> {
    let sql = format!(
        "SELECT {ATOM_COLUMNS} FROM {DATABASE}.atoms AS atoms WHERE atoms.id = {} AND {}",
        text(id),
        visible("atoms", caller)
    );
    Ok(query(host, &sql)?.first().map(|row| atom_row(row)))
}

/// Forget one atom: hide it from every read, keep the row and log the change.
/// `Err` when the caller cannot see the atom (so a bad id is not an oracle for
/// other owners' rows) or when it is already forgotten.
pub fn forget(host: &mut dyn Host, caller: &Caller, id: &str) -> Result<(), String> {
    let Some(atom) = atom(host, caller, id)? else {
        return Err(format!("no atom \"{id}\" is visible to you"));
    };
    if atom.status == "forgotten" {
        return Err(format!("atom \"{id}\" is already forgotten"));
    }
    let at = now();
    let sql = format!(
        "UPDATE {DATABASE}.atoms SET status = 'forgotten', updated_at = {at} WHERE id = {}; \
         INSERT INTO {DATABASE}.audit(id, at, actor, action, subject, detail) VALUES ({}, {at}, \
         {}, 'forget', {}, {})",
        text(id),
        text(&new_id('u')),
        text(&caller.user),
        text(id),
        text(&format!("was {}: {}", atom.status, atom.text))
    );
    write(host, &sql)
}

/// Change an atom's text without losing the old one: a new atom supersedes it,
/// keeping owner, visibility and tag.
pub fn update_atom(
    host: &mut dyn Host,
    caller: &Caller,
    id: &str,
    text: &str,
) -> Result<String, String> {
    let Some(old) = atom(host, caller, id)? else {
        return Err(format!("no atom \"{id}\" is visible to you"));
    };
    if !old.active() {
        return Err(format!(
            "atom \"{id}\" is {} and cannot be updated",
            old.status
        ));
    }
    let draft = Draft {
        text: text.to_string(),
        tag: old.tag.clone(),
        confidence: old.confidence,
        visibility: old.visibility.clone(),
        acl: Acl::default(),
        supersedes: id.to_string(),
    };
    add_atom(host, caller, &draft, &old.agent)
}

// ---------------------------------------------------------------------------
// Scenarios (L2) and persona (L3)
// ---------------------------------------------------------------------------

/// Append one scenario and return its id.
pub fn add_scenario(
    host: &mut dyn Host,
    caller: &Caller,
    title: &str,
    summary: &str,
    atom_ids: &[String],
    tag: &str,
) -> Result<String, String> {
    let id = new_id('s');
    let at = now();
    let sql = format!(
        "INSERT INTO {DATABASE}.scenarios(id, {IDENTITY}, title, summary, atom_ids, tag, \
         created_at, updated_at) VALUES ({}, {}, {}, {}, {}, {}, {at}, {at})",
        text(&id),
        identity(caller, "private", "", &Acl::default()),
        text(title),
        text(summary),
        text(&atom_ids.join(",")),
        opt_text(tag)
    );
    write(host, &sql)?;
    Ok(id)
}

/// The scenarios `caller` may see, newest first.
pub fn scenarios(host: &mut dyn Host, caller: &Caller) -> Result<Vec<Scenario>, String> {
    let sql = format!(
        "SELECT id, title, summary, atom_ids, tag, owner, visibility, updated_at FROM \
         {DATABASE}.scenarios AS scenarios WHERE {} ORDER BY scenarios.updated_at DESC",
        visible("scenarios", caller)
    );
    Ok(query(host, &sql)?
        .iter()
        .map(|row| Scenario {
            id: cell_str(row, 0),
            title: cell_str(row, 1),
            summary: cell_str(row, 2),
            atom_ids: cell_str(row, 3)
                .split(',')
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(str::to_string)
                .collect(),
            tag: cell_str(row, 4),
            owner: cell_str(row, 5),
            visibility: cell_str(row, 6),
            updated_at: cell_u64(row, 7),
        })
        .collect())
}

/// The caller's newest persona text, if they have one.
pub fn persona(host: &mut dyn Host, caller: &Caller) -> Result<Option<String>, String> {
    let sql = format!(
        "SELECT text FROM {DATABASE}.personas AS personas WHERE personas.owner = {} AND {} \
         ORDER BY personas.created_at DESC LIMIT 1",
        text(&caller.user),
        visible("personas", caller)
    );
    Ok(query(host, &sql)?.first().map(|row| cell_str(row, 0)))
}

/// Append a persona row: the newest one is the profile, the older ones stay.
pub fn add_persona(host: &mut dyn Host, caller: &Caller, persona: &str) -> Result<String, String> {
    let id = new_id('p');
    let sql = format!(
        "INSERT INTO {DATABASE}.personas(id, {IDENTITY}, text, tag, created_at) VALUES ({}, {}, {}, \
         NULL, {})",
        text(&id),
        identity(caller, "private", "", &Acl::default()),
        text(persona),
        now()
    );
    write(host, &sql)?;
    Ok(id)
}

// ---------------------------------------------------------------------------
// The queue, the audit trail and the run log
// ---------------------------------------------------------------------------

/// Queue one distillation step. A step already queued for the same subject is
/// not queued twice: an add that happens twice before a drain is one job.
pub fn enqueue(
    host: &mut dyn Host,
    caller: &Caller,
    kind: &str,
    subject: &str,
) -> Result<(), String> {
    let queued = format!(
        "SELECT count(*) FROM {DATABASE}.distill_queue WHERE owner = {} AND kind = {} AND subject \
         = {} AND state = 'pending'",
        text(&caller.user),
        text(kind),
        text(subject)
    );
    if query(host, &queued)?
        .first()
        .map(|row| cell_u64(row, 0))
        .unwrap_or(0)
        > 0
    {
        return Ok(());
    }
    let at = now();
    let sql = format!(
        "INSERT INTO {DATABASE}.distill_queue(id, {IDENTITY}, kind, subject, state, attempts, \
         last_error, created_at, updated_at) VALUES ({}, {}, {}, {}, 'pending', 0, NULL, {at}, {at})",
        text(&new_id('q')),
        identity(caller, "private", "", &Acl::default()),
        text(kind),
        text(subject)
    );
    write(host, &sql)
}

/// The pending jobs this caller queued, oldest first.
pub fn pending(host: &mut dyn Host, caller: &Caller) -> Result<Vec<Job>, String> {
    let sql = format!(
        "SELECT id, kind, subject, attempts, last_error FROM {DATABASE}.distill_queue WHERE owner \
         = {} AND state = 'pending' ORDER BY created_at",
        text(&caller.user)
    );
    Ok(query(host, &sql)?
        .iter()
        .map(|row| Job {
            id: cell_str(row, 0),
            kind: cell_str(row, 1),
            subject: cell_str(row, 2),
            attempts: cell_u64(row, 3) as i32,
            last_error: cell_str(row, 4),
        })
        .collect())
}

/// How many jobs are pending for this caller.
pub fn pending_count(host: &mut dyn Host, caller: &Caller) -> Result<u64, String> {
    let sql = format!(
        "SELECT count(*) FROM {DATABASE}.distill_queue WHERE owner = {} AND state = 'pending'",
        text(&caller.user)
    );
    Ok(query(host, &sql)?
        .first()
        .map(|row| cell_u64(row, 0))
        .unwrap_or(0))
}

/// A job that succeeded: it leaves the pending set.
pub fn finish_job(host: &mut dyn Host, id: &str) -> Result<(), String> {
    write(
        host,
        &format!(
            "UPDATE {DATABASE}.distill_queue SET state = 'done', updated_at = {} WHERE id = {}",
            now(),
            text(id)
        ),
    )
}

/// Requeue the jobs that ran out of attempts. Automatic retries stop at
/// `MAX_ATTEMPTS` so a poison batch cannot spend model calls forever; draining
/// again is an operator's decision, and it starts from attempts zero.
pub fn retry_failed(host: &mut dyn Host, caller: &Caller) -> Result<(), String> {
    write(
        host,
        &format!(
            "UPDATE {DATABASE}.distill_queue SET state = 'pending', attempts = 0, updated_at = {} WHERE owner = {} AND state = 'failed'",
            now(),
            text(&caller.user)
        ),
    )
}

/// A job that failed: the error is kept and the job stays pending until it
/// has failed [`MAX_ATTEMPTS`](crate::distill::MAX_ATTEMPTS) times.
pub fn fail_job(host: &mut dyn Host, id: &str, attempts: i32, error: &str) -> Result<(), String> {
    let state = if attempts >= crate::distill::MAX_ATTEMPTS {
        "failed"
    } else {
        "pending"
    };
    write(
        host,
        &format!(
            "UPDATE {DATABASE}.distill_queue SET state = '{}', attempts = {attempts}, last_error = \
             {}, updated_at = {} WHERE id = {}",
            state,
            text(error),
            now(),
            text(id)
        ),
    )
}

/// Append one audit row.
pub fn audit(
    host: &mut dyn Host,
    actor: &str,
    action: &str,
    subject: &str,
    detail: &str,
) -> Result<(), String> {
    let sql = format!(
        "INSERT INTO {DATABASE}.audit(id, at, actor, action, subject, detail) VALUES ({}, {}, {}, \
         {}, {}, {})",
        text(&new_id('u')),
        now(),
        text(actor),
        text(action),
        text(subject),
        text(detail)
    );
    write(host, &sql)
}

/// The audit rows about `subject`, oldest first.
pub fn audits(
    host: &mut dyn Host,
    subject: &str,
) -> Result<Vec<(u64, String, String, String)>, String> {
    let sql = format!(
        "SELECT at, actor, action, detail FROM {DATABASE}.audit WHERE subject = {} ORDER BY at",
        text(subject)
    );
    Ok(query(host, &sql)?
        .iter()
        .map(|row| {
            (
                cell_u64(row, 0),
                cell_str(row, 1),
                cell_str(row, 2),
                cell_str(row, 3),
            )
        })
        .collect())
}

/// Log one model call.
pub fn record_run(
    host: &mut dyn Host,
    caller: &Caller,
    kind: &str,
    provider: &str,
    model: &str,
    state: &str,
    error: &str,
) -> Result<(), String> {
    let sql = format!(
        "INSERT INTO {DATABASE}.runs(id, owner, kind, provider, model, state, error, created_at) \
         VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
        text(&new_id('r')),
        text(&caller.user),
        text(kind),
        text(provider),
        text(model),
        text(state),
        opt_text(error),
        now()
    );
    write(host, &sql)
}
