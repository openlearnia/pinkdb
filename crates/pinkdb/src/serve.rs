//! Handing the store to an extension that serves a client (ABI minor 2).
//!
//! `LOAD EXTENSION` registers functions; an extension that also exports
//! `pinkdb_ext_serve` can be given a client (`stdio`, or a localhost HTTP
//! endpoint) and serve it — the MCP bridge in `extensions/mcp` is the one this
//! crate ships. The extension brings the protocol; this module is the host
//! side of [`pinkdb_ext_loader::serve::StatementHost`]: the read-only policy,
//! the budgets and the agent formatting all live here, so an extension cannot
//! widen what a client may do.
//!
//! Statements cross the boundary as text in both directions — SQL in,
//! `pinkdb::agent` Markdown (or a one-line JSON error) out — so no engine
//! structure is reachable from extension code.

use crate::agent::{self, AgentDisplay};
use crate::exec::{ExecLimits, QueryResult};
use crate::sql::ast::Statement;
use crate::storage::PinkDb;
use crate::types::{PinkDbError, Result};
use pinkdb_ext_loader::ExtHandle;
use pinkdb_ext_loader::serve::StatementHost;

/// What a served extension may ask the store to do.
#[derive(Debug, Clone)]
pub struct ServePolicy {
    /// Run statements that change the store. Off by default: an agent gets a
    /// read-only store unless the operator started the server with writes
    /// enabled, and the check happens here — never in the extension.
    pub allow_writes: bool,
    /// Let a statement read external data: table functions (`read_csv`,
    /// `read_parquet`, a bare `FROM 'file.parquet'`) open files and URLs.
    /// Off by default, and independent of [`Self::allow_writes`]: the caller
    /// of a served session is an agent, which a prompt injection can steer
    /// into `read_csv('/home/u/.ssh/id_rsa')` or a URL carrying the data out.
    /// The check happens here, before the statement runs.
    pub allow_files: bool,
    /// Let a statement reach an external service: a host scalar that declared
    /// itself external I/O, such as wave 107c's `embed()`, which calls an
    /// embeddings provider. Off by default. Independent of
    /// [`Self::allow_files`], and split from it because the two are not the
    /// same risk: a model call leaks the text it is given, a file read leaks
    /// whatever the path names, and an extension's own bookkeeping channel may
    /// need the first while it must never have the second. The operator's
    /// `allow-files` switch sets both, so the documented MCP behaviour is
    /// unchanged.
    pub allow_external_services: bool,
    /// Caps for the Markdown formatter (rows, columns, cell width).
    pub display: AgentDisplay,
    /// Budget for every statement the extension runs.
    pub limits: ExecLimits,
    /// Host commands that are not SQL — the CLI's `.lake` family, which the
    /// engine does not link (pink-lake belongs to the CLI's `pinklake`
    /// feature). `None` on a host without one; `read_only` is
    /// `!allow_writes`, and the hook must refuse writes then.
    pub command: Option<CommandHook>,
    /// Print wave 99's scan estimate for `EXPLAIN` once it reaches this many
    /// rows; `0` never prints it. The CLI fills this from
    /// `PINKDB_AGENT_HINT_ROWS` so both sides of one session agree.
    pub hint_rows: u64,
    /// Databases a write may not touch, whatever [`Self::allow_writes`] says.
    /// The agent harness lists [`AGENT_DATABASE`] here for the model's tool
    /// channel, so a prompt injection cannot rewrite the transcript the run
    /// resumes from.
    pub deny_write_databases: Vec<String>,
    /// When set, every write must stay inside this database: the agent
    /// harness's own bookkeeping channel, which is nothing but its session
    /// tables.
    pub write_database: Option<String>,
    /// Databases a statement may not *read*, whatever its privileges. A host
    /// names a database whose rows are filtered per caller by an extension
    /// (wave 109's memory tables): the extension's tools serve them, a direct
    /// `SELECT` from the served session does not.
    pub deny_read_databases: Vec<String>,
    /// When set, a statement may read *only* the tables of these databases.
    /// An extension's bookkeeping channel runs without the RBAC gate (it keeps
    /// its own state, and the caller it serves is not the user whose memory it
    /// filters), so the policy — not the extension's quoting — is what keeps a
    /// mistyped identifier or a slip in a generated predicate inside the
    /// extension's own tables. Reads are collected from the bound plan, so a
    /// view, a CTE or a macro body cannot reach around the list, and a table
    /// function or a bare path names no table the list could vouch for.
    pub read_databases: Option<Vec<String>>,
    /// The policy an extension's own bookkeeping channel
    /// ([`StatementHost::execute_internal`]) runs under. `None` — the default —
    /// means this host has no such channel and refuses it; a host that loads an
    /// extension keeping its own state names the extension's database in
    /// [`Self::write_database`] here.
    pub internal: Option<Box<ServePolicy>>,
}

/// The database a command extension keeps its state in. [`ServePolicy::
/// deny_write_databases`] names it for the model's tool channel and
/// [`ServePolicy::write_database`] confines the extension's own channel to it;
/// `pinkdb_ext_sdk::AGENT_DATABASE` is the same name on the extension side.
pub const AGENT_DATABASE: &str = "pinkdb_agent";

/// The database the agent-memory extension (wave 109) keeps its layers in.
/// The engine does not create it: the extension owns the shape, and
/// [`memory_policies`] are what keep every other statement out of it.
pub const MEMORY_DATABASE: &str = "pinkdb_memory";

/// The two policies an agent command (`.agent …`) runs under, built from the
/// host's base policy (its write/file switches, budgets and display):
///
/// * the **tool** channel — the model's statements — gets the base policy and
///   may never write [`AGENT_DATABASE`], so a prompt injection cannot rewrite
///   the transcript a run resumes from;
/// * the **internal** channel — the extension's bookkeeping — may write and
///   read only [`AGENT_DATABASE`], and reaches no file and no service. It runs
///   without the RBAC gate, so this read list, not the extension's quoting, is
///   what keeps a mistyped identifier inside the transcript.
pub fn agent_policies(base: &ServePolicy) -> (ServePolicy, ServePolicy) {
    let tool = ServePolicy {
        deny_write_databases: vec![AGENT_DATABASE.to_string()],
        internal: None,
        ..base.clone()
    };
    let internal = ServePolicy {
        allow_writes: true,
        allow_files: false,
        allow_external_services: false,
        write_database: Some(AGENT_DATABASE.to_string()),
        read_databases: Some(vec![AGENT_DATABASE.to_string()]),
        internal: None,
        ..base.clone()
    };
    (tool, internal)
}

/// The two policies a memory command (`.memory …`) runs under: the tool
/// channel cannot read or write [`MEMORY_DATABASE`] (its rows are filtered per
/// caller by the extension, never served raw), and the internal channel may
/// read and write only that database and reach the embeddings provider.
pub fn memory_policies(base: &ServePolicy) -> (ServePolicy, ServePolicy) {
    let tool = ServePolicy {
        deny_read_databases: vec![MEMORY_DATABASE.to_string()],
        deny_write_databases: vec![MEMORY_DATABASE.to_string()],
        internal: None,
        ..base.clone()
    };
    let internal = ServePolicy {
        allow_writes: true,
        allow_files: false,
        allow_external_services: true,
        write_database: Some(MEMORY_DATABASE.to_string()),
        read_databases: Some(vec![MEMORY_DATABASE.to_string()]),
        internal: None,
        ..base.clone()
    };
    (tool, internal)
}

/// A host command handler: text starting with `.`, the store, and whether the
/// session is read-only.
pub type CommandHook = fn(&mut PinkDb, &str, bool) -> std::result::Result<String, String>;

impl Default for ServePolicy {
    /// Read-only, [`AgentDisplay::DEFAULT`], and the budgets agent mode uses:
    /// 30 s per statement, a million result rows.
    fn default() -> Self {
        ServePolicy {
            allow_writes: false,
            allow_files: false,
            allow_external_services: false,
            display: AgentDisplay::DEFAULT,
            limits: ExecLimits {
                statement_timeout: Some(std::time::Duration::from_secs(30)),
                max_result_rows: Some(1_000_000),
                max_result_bytes: None,
            },
            command: None,
            hint_rows: 100_000,
            deny_write_databases: Vec::new(),
            write_database: None,
            deny_read_databases: Vec::new(),
            read_databases: None,
            internal: None,
        }
    }
}

/// The reason a write was refused, returned to the client as a `permission`
/// JSON error.
const READ_ONLY_HINT: &str = "read-only session: only queries are allowed (start the server with \
                              allow-writes to change the store)";

/// What an embedded store answers for a CLI-style command: the lake readers
/// live in the CLI host, not in the engine.
const NO_COMMANDS_HINT: &str = "this host cannot run \".lake\" commands: lake catalogs are \
                                 served by the pinkdb CLI (pinkdb --mcp <file>), not by an \
                                 embedded store";

/// What a write to the agent's own tables gets, whatever else the policy
/// allows: the transcript is the record a run resumes from.
fn agent_table_hint(database: &str) -> String {
    format!(
        "this session may not write the agent's own tables in \"{database}\": they are the \
         transcript an agent run resumes from, and a model must not rewrite it"
    )
}

/// What a bookkeeping write outside the agent's database gets.
const INTERNAL_WRITE_HINT: &str = "the agent keeps its state in one database: this statement \
                                   writes outside it";

/// What a direct read of a per-caller database gets. The rows are filtered by
/// the extension that owns them, so the served session reaches them through
/// its tools instead.
fn masked_read_hint(database: &str) -> String {
    format!(
        "this session may not read \"{database}\" directly: its rows are filtered per caller by \
         the extension that owns them, which serves them through its own tools"
    )
}

/// What a read outside a confined channel's own databases gets. The channel
/// runs without the RBAC gate, so the policy, not the extension's quoting, is
/// what keeps it at home; a table function or a path names nothing the list
/// could vouch for.
fn confined_read_hint(databases: &[String]) -> String {
    format!(
        "this session may read only the tables of {}: it is a confined channel for one \
         extension's own state, and that extension serves everything else through its tools",
        databases.join(", ")
    )
}

/// What a write whose database cannot be resolved gets while a policy
/// confines writes.
const UNRESOLVED_WRITE_HINT: &str = "cannot tell which database this statement writes (it is \
                                     unqualified and the session has no current database): the \
                                     agent's write policy needs a qualified target";

/// What a confined channel (the agent's tools, or its own bookkeeping) gets
/// for a statement that may write outside any one database.
const GLOBAL_WRITE_HINT: &str = "this statement may write outside one database (CALL, DO, a trigger \
                                 that runs one, extension loading, RBAC, CHECKPOINT or COPY … TO), \
                                 which the agent's write policy refuses";

/// What a statement that opens a file or a URL gets while `allow_files` is
/// off. Naming the switch matters: the operator, not the agent, decides.
const FILES_HINT: &str = "external files and URLs are disabled: this session reads tables only \
                          (start the server with allow-files to permit read_csv, read_parquet \
                          and file paths)";

pub use crate::package::SERVICES_HINT;

/// What a statement writes, for the two database-confinement rules of
/// [`ServePolicy`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteTarget {
    /// A query: it writes nothing.
    Nothing,
    /// Writes inside exactly these databases (already resolved against the
    /// session's current database; lowercased for comparison).
    Databases(Vec<String>),
    /// Writes a table whose database is unknown: unqualified, in a session
    /// with no current database (a [`crate::db::Connection`]).
    Unknown,
    /// A change outside any one database: `CHECKPOINT`, extension loading,
    /// RBAC, `COPY … TO` a file, and `CALL`/`DO`, whose bodies may write
    /// anywhere (as may a trigger whose action is one of them).
    Global,
}

impl WriteTarget {
    /// The combined target of two writes in one statement: `Global` beats
    /// `Unknown`, which beats a database list (lists are unioned).
    fn merge(self, other: WriteTarget) -> WriteTarget {
        use WriteTarget::*;
        match (self, other) {
            (Global, _) | (_, Global) => Global,
            (Unknown, _) | (_, Unknown) => Unknown,
            (Nothing, t) | (t, Nothing) => t,
            (Databases(mut a), Databases(b)) => {
                for db in b {
                    if !a.contains(&db) {
                        a.push(db);
                    }
                }
                Databases(a)
            }
        }
    }
}

/// What `stmt` writes, given the session's `current_db`.
///
/// Conservative by construction: a statement kind this function does not
/// classify is a [`WriteTarget::Global`], and one whose table database cannot
/// be resolved is [`WriteTarget::Unknown`] — both are refused once a policy
/// confines writes.
pub fn write_target(stmt: &Statement, current_db: Option<&str>) -> WriteTarget {
    use Statement::*;
    /// A catalog write that names its database, or inherits the session's.
    fn table(db: &Option<String>, current_db: Option<&str>) -> WriteTarget {
        match db.as_deref().or(current_db) {
            Some(db) => WriteTarget::Databases(vec![db.to_ascii_lowercase()]),
            None => WriteTarget::Unknown,
        }
    }
    match stmt {
        // Queries and metadata reads.
        Describe { .. }
        | DescribeMacro { .. }
        | ShowTables
        | ShowDatabases
        | ShowMaterializedViews
        | ShowMacros
        | ShowTriggers { .. }
        | Use { .. }
        | Analyze { .. }
        // Transaction control and session variables change no table by
        // themselves; every statement inside the transaction is checked.
        | Begin
        | Commit
        | Rollback
        | SetVariable { .. } => WriteTarget::Nothing,
        // A `WITH` may carry data-modifying bodies (`WITH x AS (INSERT …)`).
        Select(sel) => sel
            .with
            .iter()
            .filter_map(|cte| match &cte.query {
                crate::sql::ast::CteBody::Dml(body) => Some(write_target(body, current_db)),
                crate::sql::ast::CteBody::Query(_) => None,
            })
            .fold(WriteTarget::Nothing, WriteTarget::merge),
        Explain(inner) => write_target(inner, current_db),
        // A trigger writes wherever its action does each time it fires, not
        // only the database of the table it is attached to.
        CreateTrigger { db, action, .. } => {
            table(db, current_db).merge(write_target(action, current_db))
        }
        CreateTable { db, .. }
        | CreateTableAs { db, .. }
        | Insert { db, .. }
        | Update { db, .. }
        | Delete { db, .. }
        | Merge { db, .. }
        | CreateView { db, .. }
        | DropTable { db, .. }
        | AlterTable { db, .. }
        | DropView { db, .. }
        | CreateMaterializedView { db, .. }
        | RefreshMaterializedView { db, .. }
        | DropMaterializedView { db, .. }
        | CreateIndex { db, .. }
        | DropIndex { db, .. }
        | CreateProcedure { db, .. }
        | DropProcedure { db, .. }
        | DropTrigger { db, .. }
        | CreateMacro { db, .. }
        | DropMacro { db, .. } => table(db, current_db),
        CreateDatabase { name, .. } | DropDatabase { name, .. } => {
            WriteTarget::Databases(vec![name.to_ascii_lowercase()])
        }
        // Everything else a policy can see here changes the store outside one
        // database, or runs SQL this function cannot inspect.
        LoadExtension { .. }
        | InstallExtension { .. }
        | LoadInstalledExtension { .. }
        | Checkpoint
        | CopyTo { .. }
        | RefreshMaterializedViews
        | Call { .. }
        | Do { .. }
        | CreateUser { .. }
        | AlterUser { .. }
        | DropUser { .. }
        | CreateRole { .. }
        | DropRole { .. }
        | Grant { .. }
        | Revoke { .. } => WriteTarget::Global,
    }
}

/// Whether `stmt` may run under `allow_writes`.
///
/// The engine's [`crate::plan::is_read_only`] answers a different question —
/// whether the statement needs the store's write lock — and counts `COPY … TO`
/// as read-only because it only writes an external file. The policy is
/// stricter: an allowlist of queries, so DML and DDL, `COPY … TO`,
/// `LOAD EXTENSION`, transaction control, `CALL`/`DO`, `CHECKPOINT` and the
/// RBAC statements are all refused by default, and a statement kind added
/// later is refused until it is reviewed here.
///
/// `USE` is allowed: it only moves the session's current database, and
/// `tables(database)` needs it. `ATTACH` and `SET` never reach this function:
/// the engine has no such statements (attaching a lake is the CLI's
/// `.lake attach`, and security settings are startup flags).
pub fn statement_allowed(stmt: &Statement, allow_writes: bool) -> bool {
    if allow_writes {
        return true;
    }
    // A `WITH` whose bodies write is a `Select` on the surface, so the
    // allowlist has to look inside it.
    if let Statement::Select(sel) = stmt {
        return sel
            .with
            .iter()
            .all(|cte| matches!(cte.query, crate::sql::ast::CteBody::Query(_)));
    }
    // `EXPLAIN` is as allowed as what it explains: `EXPLAIN SELECT` stays
    // readable, `EXPLAIN INSERT …` is a write path.
    if let Statement::Explain(inner) = stmt {
        return statement_allowed(inner, allow_writes);
    }
    matches!(
        stmt,
        Statement::Describe { .. }
            | Statement::DescribeMacro { .. }
            | Statement::ShowTables
            | Statement::ShowDatabases
            | Statement::ShowMaterializedViews
            | Statement::ShowMacros
            | Statement::Use { .. }
    )
}

/// Run one statement for a serving extension: the policy check, the statement
/// under the policy's budget, then the agent rendering.
///
/// Returns `(0, markdown)` or `(1, error_json)` — the codes
/// [`pinkdb_ext_loader::abi`] documents.
pub fn execute(db: &mut PinkDb, policy: &ServePolicy, sql: &str) -> (i32, String) {
    match run(db, policy, sql) {
        Ok(text) => (0, text),
        Err(text) => (1, text),
    }
}

/// Run one statement for a command extension's own bookkeeping: the same
/// policy check as [`execute`], but the answer is JSON — an array of
/// `{"columns":[…],"rows":[[…],…]}` objects, one per statement.
///
/// [`ServePolicy::write_database`] is what confines those writes; the model's
/// tool channel goes through [`execute`] with [`ServePolicy::
/// deny_write_databases`] instead.
pub fn execute_internal(db: &mut PinkDb, policy: &ServePolicy, sql: &str) -> (i32, String) {
    let text = sql.trim();
    if text.starts_with('.') {
        return (1, agent::error_json(NO_COMMANDS_HINT, "execution", None));
    }
    if let Err(error) = checked(policy, sql, Some(&mut *db)) {
        return (1, error);
    }
    // The statement's own executor enforces this for calls that only bind
    // after an earlier statement of the batch ran (wave 107c).
    db.set_external_services(policy.allow_external_services);
    match db.execute_with_limits(sql, policy.limits) {
        Ok(results) => (0, render_json(&results)),
        Err(e) => (1, agent::error_json_for(&e, sql)),
    }
}

/// Render statement results as JSON rows: cell values keep their JSON type
/// where it is exact (null, boolean, integers, finite doubles), and every
/// other type is its display text.
pub fn render_json(results: &[QueryResult]) -> String {
    fn quoted(out: &mut String, text: &str) {
        out.push('"');
        agent::json_escape_into(out, text);
        out.push('"');
    }
    fn cell(out: &mut String, value: &crate::Value) {
        use std::fmt::Write;
        match value {
            crate::Value::Null => out.push_str("null"),
            crate::Value::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
            crate::Value::Integer(n) => {
                let _ = write!(out, "{n}");
            }
            crate::Value::Bigint(n) => {
                let _ = write!(out, "{n}");
            }
            crate::Value::Double(x) if x.is_finite() => {
                let _ = write!(out, "{x}");
            }
            // Non-finite doubles have no JSON number form; dates, timestamps,
            // decimals, intervals and variants stay exact as their text.
            other => quoted(out, &other.to_string()),
        }
    }
    let mut out = String::from("[");
    for (i, result) in results.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"columns\":[");
        let (columns, rows): (Vec<&str>, Option<&Vec<Vec<crate::Value>>>) = match result {
            QueryResult::Selected { names, rows, .. } => {
                (names.iter().map(String::as_str).collect(), Some(rows))
            }
            // A statement without rows answers with its outcome line.
            _ => (vec!["result"], None),
        };
        let names = columns;
        for (j, name) in names.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            quoted(&mut out, name);
        }
        out.push_str("],\"rows\":[");
        match rows {
            Some(rows) => {
                for (r, row) in rows.iter().enumerate() {
                    if r > 0 {
                        out.push(',');
                    }
                    out.push('[');
                    for (c, value) in row.iter().enumerate() {
                        if c > 0 {
                            out.push(',');
                        }
                        cell(&mut out, value);
                    }
                    out.push(']');
                }
            }
            None => {
                out.push('[');
                quoted(&mut out, &outcome_line(result));
                out.push(']');
            }
        }
        out.push_str("]}");
    }
    out.push(']');
    out
}

/// What a serve call runs statements on: the embedded store (no privilege
/// check) or a logged-in [`crate::db::Connection`] (a server, where the
/// connection's login makes every statement pass the role checks).
pub trait Session {
    /// Run `sql` under `limits`.
    fn run_statement(&mut self, sql: &str, limits: ExecLimits) -> Result<Vec<QueryResult>>;

    /// The store, for wave 99's cost hint; `None` where the hint is not
    /// reachable (a connection's store is behind the shared lock).
    fn hint_store(&self) -> Option<&PinkDb>;

    /// Show the store to a policy check, this one time. A policy that names
    /// databases to refuse (or the only ones it allows) is unenforceable
    /// without the catalog, so this is the hook it runs through: the
    /// connection borrows the store under its shared lock for the call, and a
    /// host with no store answers `None` — which refuses nothing, the same
    /// answer as a policy that names no database.
    fn check_store(&mut self, check: &mut dyn FnMut(&PinkDb) -> Option<String>) -> Option<String>;

    /// Install the policy's external-services switch for the next statement
    /// (wave 107c): refused calls fail at the call site, before any request.
    fn set_external_services(&mut self, allowed: bool);

    /// Run `sql` for the extension's own bookkeeping
    /// ([`StatementHost::execute_internal`]): the host's internal policy is the
    /// only limit on what it may touch, so no privilege check runs here.
    fn run_internal(&mut self, sql: &str, limits: ExecLimits) -> Result<Vec<QueryResult>> {
        self.run_statement(sql, limits)
    }
}

impl Session for PinkDb {
    fn run_statement(&mut self, sql: &str, limits: ExecLimits) -> Result<Vec<QueryResult>> {
        self.execute_with_limits(sql, limits)
    }

    fn hint_store(&self) -> Option<&PinkDb> {
        Some(self)
    }

    fn check_store(&mut self, check: &mut dyn FnMut(&PinkDb) -> Option<String>) -> Option<String> {
        check(self)
    }

    fn set_external_services(&mut self, allowed: bool) {
        PinkDb::set_external_services(self, allowed);
    }
}

impl Session for crate::db::Connection {
    fn run_statement(&mut self, sql: &str, limits: ExecLimits) -> Result<Vec<QueryResult>> {
        self.set_exec_limits(limits);
        self.execute(sql)
    }

    fn hint_store(&self) -> Option<&PinkDb> {
        None
    }

    fn check_store(&mut self, check: &mut dyn FnMut(&PinkDb) -> Option<String>) -> Option<String> {
        self.with_store(|db| check(db))
    }

    fn set_external_services(&mut self, allowed: bool) {
        crate::db::Connection::set_external_services(self, allowed);
    }

    fn run_internal(&mut self, sql: &str, limits: ExecLimits) -> Result<Vec<QueryResult>> {
        self.set_exec_limits(limits);
        self.execute_as_extension(sql)
    }
}

/// The [`StatementHost`] a server uses: statements run on a [`Session`] —
/// typically a logged-in [`crate::db::Connection`], which applies the role
/// checks and takes the store lock only for the statement's own duration, so
/// the serve loop never blocks the server's other connections.
///
/// Commands (text starting with `.`) are refused here: lake catalogs belong to
/// the CLI host, not to a served store.
pub struct SessionHost<'a, R: Session + ?Sized> {
    session: &'a mut R,
    policy: &'a ServePolicy,
}

impl<'a, R: Session + ?Sized> SessionHost<'a, R> {
    pub fn new(session: &'a mut R, policy: &'a ServePolicy) -> Self {
        SessionHost { session, policy }
    }
}

impl<R: Session + ?Sized> StatementHost for SessionHost<'_, R> {
    fn execute_statement(&mut self, sql: &str) -> (i32, String) {
        if sql.trim_start().starts_with('.') {
            return (1, agent::error_json(NO_COMMANDS_HINT, "permission", None));
        }
        execute_on(self.session, self.policy, sql)
    }

    fn execute_internal(&mut self, sql: &str) -> (i32, String) {
        match &self.policy.internal {
            Some(internal) => internal_on(self.session, internal, sql),
            None => (2, String::new()),
        }
    }
}

/// [`execute_internal`] on any [`Session`]: the same checks and budget, without
/// the privilege gate.
fn internal_on<R: Session + ?Sized>(
    session: &mut R,
    policy: &ServePolicy,
    sql: &str,
) -> (i32, String) {
    if sql.trim().starts_with('.') {
        return (1, agent::error_json(NO_COMMANDS_HINT, "execution", None));
    }
    if let Err(error) = checked(policy, sql, Some(&mut *session)) {
        return (1, error);
    }
    session.set_external_services(policy.allow_external_services);
    match session.run_internal(sql, policy.limits) {
        Ok(results) => (0, render_json(&results)),
        Err(e) => (1, agent::error_json_for(&e, sql)),
    }
}

/// [`execute`] on any [`Session`]: the same policy check, budgets and
/// formatting, for a host that serves through a login.
pub fn execute_on<R: Session + ?Sized>(
    session: &mut R,
    policy: &ServePolicy,
    sql: &str,
) -> (i32, String) {
    match run_on(session, policy, sql) {
        Ok(text) => (0, text),
        Err(text) => (1, text),
    }
}

fn run(db: &mut PinkDb, policy: &ServePolicy, sql: &str) -> std::result::Result<String, String> {
    let text = sql.trim();
    // A dot command is the host's, not SQL: only the CLI host has `.lake`.
    if text.starts_with('.') {
        let Some(command) = policy.command else {
            return Err(agent::error_json(NO_COMMANDS_HINT, "execution", None));
        };
        return command(db, text, !policy.allow_writes);
    }
    let statements = checked(policy, sql, Some(&mut *db))?;
    db.set_external_services(policy.allow_external_services);
    render(db, policy, sql, &statements)
}

fn run_on<R: Session + ?Sized>(
    session: &mut R,
    policy: &ServePolicy,
    sql: &str,
) -> std::result::Result<String, String> {
    let statements = checked(policy, sql, Some(&mut *session))?;
    session.set_external_services(policy.allow_external_services);
    render(session, policy, sql, &statements)
}

/// Parse a batch and apply the policy to every statement of it. The error is
/// the JSON line the client receives, so a refusal explains itself.
///
/// `store` is the session's store when the host has one. A `Connection` (the
/// network server's bridge) does not expose it, because its store is behind
/// the shared lock: there the login's RBAC gate already restricts file scans
/// to administrators (`rbac` M-4), so the policy does not need to see the plan.
fn checked<R: Session + ?Sized>(
    policy: &ServePolicy,
    sql: &str,
    session: Option<&mut R>,
) -> std::result::Result<Vec<Statement>, String> {
    let statements =
        crate::sql::parse_statements(sql).map_err(|e| agent::error_json_for(&e, sql))?;
    if statements.is_empty() {
        return Err(agent::error_json("empty statement", "parse", None));
    }
    if statements
        .iter()
        .any(|s| !statement_allowed(s, policy.allow_writes))
    {
        return Err(agent::error_json(READ_ONLY_HINT, "permission", None));
    }
    let mut session = session;
    let current_db = session
        .as_deref_mut()
        .and_then(|s| s.check_store(&mut |db| Some(db.current_db().to_string())));
    let confining = !policy.deny_write_databases.is_empty() || policy.write_database.is_some();
    for statement in &statements {
        match write_target(statement, current_db.as_deref()) {
            WriteTarget::Nothing => {}
            // `CALL`/`DO` bodies, extension loading, RBAC and the like may
            // write anywhere, so a confined channel refuses them.
            WriteTarget::Global if confining => {
                return Err(agent::error_json(GLOBAL_WRITE_HINT, "permission", None));
            }
            WriteTarget::Global => {}
            WriteTarget::Databases(databases) => {
                if let Some(denied) = databases.iter().find(|db| {
                    policy
                        .deny_write_databases
                        .iter()
                        .any(|d| d.eq_ignore_ascii_case(db))
                }) {
                    return Err(agent::error_json(
                        &agent_table_hint(denied),
                        "permission",
                        None,
                    ));
                }
                if let Some(allowed) = &policy.write_database
                    && databases.iter().any(|db| !db.eq_ignore_ascii_case(allowed))
                {
                    return Err(agent::error_json(INTERNAL_WRITE_HINT, "permission", None));
                }
            }
            WriteTarget::Unknown if confining => {
                return Err(agent::error_json(UNRESOLVED_WRITE_HINT, "permission", None));
            }
            WriteTarget::Unknown => {}
        }
    }
    // The two catalog checks need the store, which a connection lends only for
    // the call (`Session::check_store`). A read is named before an external
    // switch: a statement that reads a table it may not read is refused for
    // that, whichever way it would also have left the store.
    if !policy.deny_read_databases.is_empty()
        || policy.read_databases.is_some()
        || !policy.allow_files
        || !policy.allow_external_services
    {
        for statement in &statements {
            let reason = session
                .as_deref_mut()
                .and_then(|s| s.check_store(&mut |db| statement_reason(db, policy, statement)));
            if let Some(hint) = reason {
                return Err(agent::error_json(&hint, "permission", None));
            }
        }
    }
    Ok(statements)
}

/// The first policy reason one statement is refused, or `None`: what it reads,
/// then whether it leaves the store.
fn statement_reason(db: &PinkDb, policy: &ServePolicy, statement: &Statement) -> Option<String> {
    if let Some(hint) = read_reason(db, policy, statement) {
        return Some(hint);
    }
    external_reason(db, statement, policy).map(str::to_string)
}

/// Why one statement would read a database the policy refuses, or `None`.
///
/// The read set comes from the bound plan (views inlined, CTEs walked), so a
/// view over a refused table is refused like the table itself. A statement
/// that does not bind has no read set to hide: it fails at bind time with its
/// own error.
fn read_reason(db: &PinkDb, policy: &ServePolicy, statement: &Statement) -> Option<String> {
    let allowed = policy.read_databases.as_deref();
    let session_db = db.current_db().to_string();
    let Ok((plan, _)) = crate::sql::bind_tracked(db, statement, &session_db) else {
        // Nothing to check: the statement fails at bind time with its own
        // error. A file source whose function is not installed is the
        // exception — it must be refused for reading a file, not for an
        // unknown function.
        if let Some(allowed) = allowed
            && ast_scans_files(statement)
        {
            return Some(confined_read_hint(allowed));
        }
        return None;
    };
    let mut reads = std::collections::HashSet::new();
    crate::plan::collect_tables_read(&plan, &session_db, &mut reads);
    if let Some(allowed) = allowed {
        // A view over a table function binds to a scan the syntax check cannot
        // see, so the plan is asked too.
        if crate::plan::contains_table_function(&crate::optimizer::optimize(db, plan.clone())) {
            return Some(confined_read_hint(allowed));
        }
        if reads
            .iter()
            .any(|(database, _)| !allowed.iter().any(|a| a.eq_ignore_ascii_case(database)))
        {
            return Some(confined_read_hint(allowed));
        }
    }
    let mut denied: Vec<&String> = reads
        .iter()
        .map(|(db, _)| db)
        .filter(|db| {
            policy
                .deny_read_databases
                .iter()
                .any(|d| d.eq_ignore_ascii_case(db))
        })
        .collect();
    denied.sort();
    denied.dedup();
    denied.first().map(|db| masked_read_hint(db))
}

/// Why one statement would reach outside the store, or `None`.
///
/// Two checks, because either alone has a blind spot. The syntax check sees
/// a file source before binding, so it also holds on a host with no file
/// source registered (where binding fails and nothing could be read anyway).
/// The plan check sees what the binder rewrites a plain `FROM` into — a view
/// over `read_csv`, say — and reaches table functions inside expressions.
/// The dynamic functions the binder recorded are checked in between: a host
/// scalar that declared itself external I/O is gated by its own switch, so an
/// extension's bookkeeping channel can call a model while it still cannot
/// open a file.
fn external_reason(
    db: &PinkDb,
    statement: &Statement,
    policy: &ServePolicy,
) -> Option<&'static str> {
    if policy.allow_files && policy.allow_external_services {
        return None;
    }
    let session_db = db.current_db().to_string();
    let Ok((plan, dynamic)) = crate::sql::bind_tracked(db, statement, &session_db) else {
        // The bound plan is the sharper test; the syntax alone is all there is
        // when a source's function is not installed, and reading files is
        // still the reason to refuse it.
        return (!policy.allow_files && ast_scans_files(statement)).then_some(FILES_HINT);
    };
    if !policy.allow_external_services
        && dynamic
            .iter()
            .any(|name| db.host_scalar(name).is_some_and(|h| h.is_external_io()))
    {
        return Some(crate::package::SERVICES_HINT);
    }
    if !policy.allow_files
        && crate::plan::contains_table_function(&crate::optimizer::optimize(db, plan))
    {
        return Some(FILES_HINT);
    }
    None
}

/// The syntactic half of [`external_reason`]: any `FROM` source that is a call
/// or a bare path, in every `SELECT` of the statement (CTEs, derived tables,
/// joins, set-operation arms).
fn ast_scans_files(statement: &Statement) -> bool {
    match statement {
        Statement::Select(select) => select_scans_files(select),
        Statement::Explain(inner) => ast_scans_files(inner),
        _ => false,
    }
}

fn select_scans_files(select: &crate::sql::ast::Select) -> bool {
    select
        .with
        .iter()
        .any(|cte| cte.query.query_select().is_some_and(select_scans_files))
        || select.from.as_ref().is_some_and(from_scans_files)
        || select
            .unions
            .iter()
            .any(|(_, branch)| select_scans_files(branch))
}

fn from_scans_files(from: &crate::sql::ast::FromClause) -> bool {
    source_scans_files(&from.source)
        || from
            .joins
            .iter()
            .any(|join| source_scans_files(&join.source))
}

fn source_scans_files(source: &crate::sql::ast::FromSource) -> bool {
    use crate::sql::ast::FromSource;
    match source {
        FromSource::Function { .. } | FromSource::Path(_) => true,
        FromSource::Values(_) => false,
        FromSource::Derived(select) => select_scans_files(select),
        FromSource::Table { .. } => false,
    }
}

fn render<R: Session + ?Sized>(
    session: &mut R,
    policy: &ServePolicy,
    sql: &str,
    statements: &[Statement],
) -> std::result::Result<String, String> {
    // The cost hint belongs to `EXPLAIN` alone (wave 99): it describes the
    // plan the client just asked for, not a following statement's.
    let explained = matches!(statements, [Statement::Explain(_)]);
    match session.run_statement(sql, policy.limits) {
        Ok(results) => {
            let mut text = render_results(&results, policy.display);
            if explained
                && let Some(store) = session.hint_store()
                && let Some(hint) = explain_hint(store, sql, policy.hint_rows)
            {
                text.push_str("\n\n");
                text.push_str(&hint);
            }
            Ok(text)
        }
        Err(e) => Err(agent::error_json_for(&e, sql)),
    }
}

/// Render every statement result in the store's Markdown form.
///
/// A selected result is a table (`pinkdb::agent`); anything else is one short
/// outcome line, and several statements are joined with a blank line.
pub fn render_results(results: &[QueryResult], display: AgentDisplay) -> String {
    let mut out = String::new();
    for result in results {
        if !out.is_empty() {
            out.push_str("\n\n");
        }
        match result {
            QueryResult::Selected { names, types, rows } => {
                out.push_str(&agent::format_table(names, types, rows, display));
            }
            other => out.push_str(&outcome_line(other)),
        }
    }
    out
}

/// One line for a statement that returned no rows. Only reachable with
/// `allow_writes`, so a read-only client never sees these.
/// One line describing a statement's outcome (`Inserted 3 rows`, `Created view
/// v`), with names sanitized for a terminal. A `SELECT` answers its row count;
/// hosts render its rows themselves.
pub fn outcome_line(result: &QueryResult) -> String {
    /// Names are user data: a table or role name may carry a terminal escape
    /// sequence, and the client renders this text.
    fn name(name: &str) -> String {
        crate::sanitize_terminal(name)
    }
    let rows = |count: usize| agent::format_count(count as u64);
    match result {
        QueryResult::Selected { rows: selected, .. } => {
            format!("Selected {} rows", rows(selected.len()))
        }
        QueryResult::Created => "Created".to_string(),
        QueryResult::Inserted { count } => format!("Inserted {} rows", rows(*count)),
        QueryResult::Updated { count } => format!("Updated {} rows", rows(*count)),
        QueryResult::Deleted { count } => format!("Deleted {} rows", rows(*count)),
        QueryResult::Began => "Began transaction".to_string(),
        QueryResult::Committed => "Committed".to_string(),
        QueryResult::RolledBack => "Rolled back".to_string(),
        QueryResult::CreatedView { name: view } => format!("Created view {}", name(view)),
        QueryResult::DroppedView { name: view } => format!("Dropped view {}", name(view)),
        QueryResult::DroppedTable { name: table } => format!("Dropped table {}", name(table)),
        QueryResult::CreatedIndex { name: index } => format!("Created index {}", name(index)),
        QueryResult::DroppedIndex { name: index } => format!("Dropped index {}", name(index)),
        QueryResult::CreatedMaterializedView { name: view } => {
            format!("Created materialized view {}", name(view))
        }
        QueryResult::RefreshedMaterializedView { name: view } => {
            format!("Refreshed materialized view {}", name(view))
        }
        QueryResult::RefreshedMaterializedViews { names } => format!(
            "Refreshed materialized views {}",
            names.iter().map(|n| name(n)).collect::<Vec<_>>().join(", ")
        ),
        QueryResult::DroppedMaterializedView { name: view } => {
            format!("Dropped materialized view {}", name(view))
        }
        QueryResult::CreatedDatabase { name: db } => format!("Created database {}", name(db)),
        QueryResult::DroppedDatabase { name: db } => format!("Dropped database {}", name(db)),
        QueryResult::Used { db } => format!("Using database {}", name(db)),
        QueryResult::SetVariable { name: variable } => {
            format!("Set variable {}", name(variable))
        }
        QueryResult::LoadedExtension { name: ext } => format!("Loaded extension {}", name(ext)),
        QueryResult::AlteredTable { name: table } => format!("Altered table {}", name(table)),
        QueryResult::Checkpointed => "Checkpointed".to_string(),
        QueryResult::CreatedProcedure { name: proc } => format!("Created procedure {}", name(proc)),
        QueryResult::DroppedProcedure { name: proc } => format!("Dropped procedure {}", name(proc)),
        QueryResult::CreatedTrigger { name: trig } => format!("Created trigger {}", name(trig)),
        QueryResult::DroppedTrigger { name: trig } => format!("Dropped trigger {}", name(trig)),
        QueryResult::CreatedMacro { name: macro_ } => format!("Created macro {}", name(macro_)),
        QueryResult::DroppedMacro { name: macro_ } => format!("Dropped macro {}", name(macro_)),
        QueryResult::CreatedUser { name: user } => format!("Created user {}", name(user)),
        QueryResult::AlteredUser { name: user } => format!("Altered user {}", name(user)),
        QueryResult::DroppedUser { name: user } => format!("Dropped user {}", name(user)),
        QueryResult::CreatedRole { name: role } => format!("Created role {}", name(role)),
        QueryResult::DroppedRole { name: role } => format!("Dropped role {}", name(role)),
        QueryResult::Granted { description } => format!("Granted {}", name(description)),
        QueryResult::Revoked { description } => format!("Revoked {}", name(description)),
        QueryResult::Called {
            name: proc,
            statements,
            result,
            output,
        } => {
            let mut line = String::new();
            for printed in output {
                line.push_str(printed);
                line.push('\n');
            }
            line.push_str(&format!("Called {} ({statements} statements)", name(proc)));
            if let Some(inner) = result {
                line.push_str("\n\n");
                match inner.as_ref() {
                    QueryResult::Selected { names, types, rows } => {
                        line.push_str(&agent::format_table(
                            names,
                            types,
                            rows,
                            AgentDisplay::DEFAULT,
                        ));
                    }
                    other => line.push_str(&outcome_line(other)),
                }
            }
            line
        }
    }
}

/// Wave 99's cost hint, for `EXPLAIN`: the estimated scan size, when it is
/// large enough to be worth acting on.
fn explain_hint(db: &PinkDb, sql: &str, hint_rows: u64) -> Option<String> {
    let estimate = agent::estimate_scan(db, sql)?;
    if hint_rows == 0 || estimate.rows < hint_rows {
        return None;
    }
    let scan = if estimate.remote {
        "remote scan"
    } else {
        "local scan"
    };
    Some(format!(
        "Estimated {} rows scanned ({scan}).",
        agent::format_count(estimate.rows)
    ))
}

/// The host half of a serve call for an embedded store: SQL (and, when the
/// policy carries a [`CommandHook`], `.lake` text) checked and run under
/// [`ServePolicy`].
pub struct ServeHost<'a> {
    db: &'a mut PinkDb,
    policy: &'a ServePolicy,
}

impl StatementHost for ServeHost<'_> {
    fn execute_statement(&mut self, sql: &str) -> (i32, String) {
        execute(self.db, self.policy, sql)
    }

    fn execute_internal(&mut self, sql: &str) -> (i32, String) {
        match &self.policy.internal {
            Some(internal) => execute_internal(self.db, internal, sql),
            None => (2, String::new()),
        }
    }
}

impl<'a> ServeHost<'a> {
    /// A host for `db` that answers under `policy`.
    pub fn new(db: &'a mut PinkDb, policy: &'a ServePolicy) -> Self {
        ServeHost { db, policy }
    }
}

impl PinkDb {
    /// The native extension loaded under `name` (case-insensitive), for a host
    /// that drives its `pinkdb_ext_serve` entry point itself. A linked
    /// extension has no library and therefore no entry point.
    pub fn dynamic_extension(&self, name: &str) -> Result<pinkdb_ext_loader::LoadedExtension> {
        let handle = self
            .loaded_extensions
            .get(&name.to_lowercase())
            .cloned()
            .ok_or_else(|| PinkDbError::Execution(format!("extension \"{name}\" is not loaded")))?;
        match handle {
            ExtHandle::Dynamic(ext) => Ok(ext),
            ExtHandle::Linked(_) => Err(PinkDbError::Execution(format!(
                "extension \"{name}\" is linked: only a native extension has an entry point"
            ))),
        }
    }

    /// Hand a client to the extension `name` (`pinkdb_ext_serve`), blocking
    /// until it returns. `transport` is `stdio` or `http:HOST:PORT`; `options`
    /// is the extension's own option string.
    ///
    /// Load the extension first ([`PinkDb::load_extension`]); a linked
    /// extension cannot be served (it has no entry point symbol). A host that
    /// needs a command hook builds a [`ServeHost`] and calls [`serve_with`].
    pub fn serve_extension(
        &mut self,
        name: &str,
        transport: &str,
        options: &str,
        policy: ServePolicy,
    ) -> Result<()> {
        let ext = self.dynamic_extension(name)?;
        let mut host = ServeHost::new(self, &policy);
        serve_with(&ext, transport, options, &mut host)
    }

    /// The declared names of the loaded extensions that export
    /// `pinkdb_ext_serve`, so a host can offer `serve` only where it works.
    pub fn serveable_extensions(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .loaded_extensions
            .values()
            .filter(|h| match h {
                ExtHandle::Dynamic(ext) => ext.serve_fn().is_some(),
                ExtHandle::Linked(_) => false,
            })
            .map(|h| h.name().to_string())
            .collect();
        names.sort();
        names
    }
}

/// Call `ext`'s serve entry point, with `host` answering its statement calls.
/// Blocks until the client disconnects or the extension stops.
pub fn serve_with(
    ext: &pinkdb_ext_loader::LoadedExtension,
    transport: &str,
    options: &str,
    host: &mut dyn StatementHost,
) -> Result<()> {
    pinkdb_ext_loader::serve::serve(ext, transport, options, host)
        .map_err(|e| PinkDbError::Execution(e.to_string()))
}

/// The host half of a command call: the extension's own bookkeeping runs under
/// `internal`, everything else under `policy`, and text goes to `sink`
/// (channel `0` for the answer, `1` for a diagnostic).
///
/// The two policies are what separate an agent's transcript from the data: a
/// command may write only its own database ([`ServePolicy::write_database`])
/// while the statements it runs on the model's behalf cannot touch it
/// ([`ServePolicy::deny_write_databases`]).
pub struct CommandSession<'a> {
    db: &'a mut PinkDb,
    policy: ServePolicy,
    internal: ServePolicy,
    sink: &'a mut dyn FnMut(i32, &str),
}

impl pinkdb_ext_loader::CommandHost for CommandSession<'_> {
    fn execute_statement(&mut self, sql: &str) -> (i32, String) {
        execute(self.db, &self.policy, sql)
    }

    fn execute_internal(&mut self, sql: &str) -> (i32, String) {
        execute_internal(self.db, &self.internal, sql)
    }

    fn emit(&mut self, channel: i32, text: &str) {
        (self.sink)(channel, text);
    }
}

/// What a command run on a [`crate::db::Connection`] reports back to its
/// host ([`crate::db::Connection::run_command`], wave 121).
pub trait CommandEvents {
    /// Text from the extension: channel `0` is the answer, `1` a diagnostic.
    fn emit(&mut self, channel: i32, text: &str);

    /// One statement the extension ran on the public (tool) channel, with its
    /// reply: `(0, markdown)` or `(1, error_json)`. A console shows these as
    /// the run's tool calls.
    fn statement(&mut self, _sql: &str, _code: i32, _reply: &str) {}

    /// Whether the host wants the run stopped: every later statement of the
    /// run, on either channel, is refused with [`crate::STATEMENT_CANCELLED`].
    fn cancelled(&self) -> bool {
        false
    }
}

/// The host half of a command run on a connection: each statement takes the
/// store lock for its own duration, so a long run (a model call between
/// statements) never blocks the store's other sessions.
struct ConnectionCommand<'a> {
    conn: &'a mut crate::db::Connection,
    policy: &'a ServePolicy,
    internal: &'a ServePolicy,
    events: &'a mut dyn CommandEvents,
}

impl pinkdb_ext_loader::CommandHost for ConnectionCommand<'_> {
    fn execute_statement(&mut self, sql: &str) -> (i32, String) {
        let (code, reply) = if self.events.cancelled() {
            (
                1,
                agent::error_json(crate::exec::STATEMENT_CANCELLED, "execution", None),
            )
        } else if sql.trim_start().starts_with('.') {
            (1, agent::error_json(NO_COMMANDS_HINT, "permission", None))
        } else {
            execute_on(self.conn, self.policy, sql)
        };
        self.events.statement(sql, code, &reply);
        (code, reply)
    }

    fn execute_internal(&mut self, sql: &str) -> (i32, String) {
        if self.events.cancelled() {
            return (
                1,
                agent::error_json(crate::exec::STATEMENT_CANCELLED, "execution", None),
            );
        }
        internal_on(self.conn, self.internal, sql)
    }

    fn emit(&mut self, channel: i32, text: &str) {
        self.events.emit(channel, text);
    }
}

impl crate::db::Connection {
    /// Run one of `name`'s commands (`pinkdb_ext_command`) on this connection,
    /// blocking until it returns (wave 121). The connection's login applies:
    /// the tool channel runs under `policy` *and* the principal's grants; the
    /// extension's bookkeeping runs under `internal` (no privilege gate, as
    /// [`crate::db::Connection::execute_as_extension`] documents).
    ///
    /// Unlike [`PinkDb::run_command`], the store lock is taken per statement.
    pub fn run_command(
        &mut self,
        name: &str,
        args: &str,
        policy: &ServePolicy,
        internal: &ServePolicy,
        events: &mut dyn CommandEvents,
    ) -> Result<()> {
        let ext = self.database().dynamic_extension(name)?;
        if ext.command_fn().is_none() {
            return Err(PinkDbError::Execution(format!(
                "extension \"{name}\" exports no pinkdb_ext_command (it has no commands)"
            )));
        }
        let mut host = ConnectionCommand {
            conn: self,
            policy,
            internal,
            events,
        };
        pinkdb_ext_loader::command::run_command(&ext, args, &mut host)
            .map_err(|e| PinkDbError::Execution(e.to_string()))
    }
}

impl PinkDb {
    /// The declared names of the loaded extensions that export
    /// `pinkdb_ext_command`, so a host can offer the commands only where they
    /// work.
    pub fn command_extensions(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .loaded_extensions
            .values()
            .filter(|h| match h {
                ExtHandle::Dynamic(ext) => ext.command_fn().is_some(),
                ExtHandle::Linked(_) => false,
            })
            .map(|h| h.name().to_string())
            .collect();
        names.sort();
        names
    }

    /// Run one of `name`'s commands (`pinkdb_ext_command`), blocking until it
    /// returns. `args` is the text after the command name, exactly as typed.
    /// Load the extension first ([`PinkDb::load_extension`]).
    pub fn run_command(
        &mut self,
        name: &str,
        args: &str,
        policy: ServePolicy,
        internal: ServePolicy,
        sink: &mut dyn FnMut(i32, &str),
    ) -> Result<()> {
        let ext = self.dynamic_extension(name)?;
        if ext.command_fn().is_none() {
            return Err(PinkDbError::Execution(format!(
                "extension \"{name}\" exports no pinkdb_ext_command (it has no commands)"
            )));
        }
        let mut session = CommandSession {
            db: self,
            policy,
            internal,
            sink,
        };
        pinkdb_ext_loader::command::run_command(&ext, args, &mut session)
            .map_err(|e| PinkDbError::Execution(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> PinkDb {
        let mut db = PinkDb::new();
        db.execute("CREATE TABLE t (a INT, s VARCHAR)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')")
            .unwrap();
        db
    }

    /// The agent error line is one physical line of JSON; its `kind` is what
    /// a client switches on, so tests read it as text rather than pull in a
    /// JSON parser.
    fn assert_kind(text: &str, kind: &str) {
        assert!(text.starts_with('{'), "not a JSON error: {text}");
        assert!(text.contains(&format!("\"kind\":\"{kind}\"")), "{text}");
    }

    /// Stands in for the first-party file sources (`read_csv`/`read_parquet`
    /// live in the extensions): it records what a scan was asked to open and
    /// returns no rows, so a test can prove what the policy let through.
    #[derive(Debug, Default)]
    struct Files {
        seen: std::sync::Mutex<Vec<String>>,
    }

    impl Files {
        /// The path (or URL) a call names, whether it is a function or a
        /// replacement scan.
        fn opened(call: &crate::table_source::TableFunctionCall) -> Option<String> {
            use crate::table_source::TableFunctionCall;
            match call {
                TableFunctionCall::Function { name, args }
                    if name.eq_ignore_ascii_case("read_csv")
                        || name.eq_ignore_ascii_case("read_parquet") =>
                {
                    match args.first().map(|arg| &arg.value) {
                        Some(crate::Value::Varchar(path)) => Some(path.clone()),
                        _ => None,
                    }
                }
                TableFunctionCall::Path(path) => Some(path.clone()),
                _ => None,
            }
        }
    }

    impl crate::table_source::TableSource for Files {
        fn name(&self) -> &str {
            "files"
        }

        fn bind(
            &self,
            call: &crate::table_source::TableFunctionCall,
        ) -> Result<Option<Vec<(String, crate::DataType)>>> {
            Ok(Self::opened(call).map(|_| vec![("bytes".to_string(), crate::DataType::Bigint)]))
        }

        fn scan(
            &self,
            call: &crate::table_source::TableFunctionCall,
            _projection: Option<&[usize]>,
            _filters: &[crate::attached::PrunePredicate],
        ) -> Result<crate::table_source::TableScanOutput> {
            let Some(path) = Self::opened(call) else {
                return Ok(crate::table_source::TableScanOutput::default());
            };
            let entry = if path.starts_with("http://") || path.starts_with("https://") {
                format!("url:{path}")
            } else {
                match std::fs::metadata(&path) {
                    Ok(meta) => format!("file:{path}:{}", meta.len()),
                    Err(e) => {
                        return Err(PinkDbError::Execution(format!("cannot read {path}: {e}")));
                    }
                }
            };
            self.seen.lock().unwrap().push(entry);
            Ok(crate::table_source::TableScanOutput::default())
        }
    }

    /// A store with [`Files`] registered, plus the log the test reads.
    fn files_store() -> (PinkDb, std::sync::Arc<Files>) {
        let mut db = store();
        let files = std::sync::Arc::new(Files::default());
        db.register_table_source(files.clone());
        (db, files)
    }

    #[test]
    fn files_and_urls_need_allow_files() {
        let (mut db, files) = files_store();
        // Every shape the planner can turn into a table-function scan.
        let sqls = [
            "SELECT * FROM read_csv('/x.csv')",
            "SELECT * FROM read_parquet('/x.parquet')",
            "SELECT * FROM '/x.parquet'",
            "WITH c AS (SELECT * FROM read_csv('/x.csv')) SELECT * FROM c",
            "SELECT * FROM (SELECT * FROM read_csv('/x.csv')) AS s",
            "SELECT 1 UNION ALL SELECT * FROM read_csv('/x.csv')",
            "SELECT * FROM read_parquet('/x.parquet') AS p JOIN t ON p.bytes = t.a",
            "SELECT (SELECT count(*) FROM read_csv('/x.csv')) AS n",
            "EXPLAIN SELECT * FROM read_csv('/x.csv')",
        ];
        for sql in sqls {
            let (code, text) = execute(&mut db, &ServePolicy::default(), sql);
            assert_eq!(code, 1, "{sql}");
            assert_kind(&text, "permission");
            assert!(text.contains("allow-files"), "{sql}: {text}");
        }
        // The refusal is decided from the statement, before anything is
        // opened.
        assert!(files.seen.lock().unwrap().is_empty());

        // A host with no file source at all still refuses the syntax: the
        // policy cannot depend on an extension being loaded.
        let mut bare = store();
        let (code, text) = execute(&mut bare, &ServePolicy::default(), sqls[0]);
        assert_eq!(code, 1, "{text}");
        assert_kind(&text, "permission");

        // Plain table queries are unaffected.
        assert_eq!(
            execute(&mut db, &ServePolicy::default(), "SELECT a FROM t").0,
            0
        );

        // With `allow_files` the same calls reach the source: a real local
        // file and a URL (the source does not fetch it, so no network).
        let path = std::env::temp_dir().join(format!("pinkdb_serve_{}.csv", std::process::id()));
        std::fs::write(&path, "a\n1\n2\n").unwrap();
        let policy = ServePolicy {
            allow_files: true,
            ..Default::default()
        };
        let sql = format!("SELECT * FROM read_csv('{}')", path.display());
        let (code, text) = execute(&mut db, &policy, &sql);
        assert_eq!(code, 0, "{text}");
        let url = "http://127.0.0.1:1/x.parquet";
        let (code, text) = execute(
            &mut db,
            &policy,
            &format!("SELECT * FROM read_parquet('{url}')"),
        );
        assert_eq!(code, 0, "{text}");
        let seen = files.seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![
                format!(
                    "file:{}:{}",
                    path.display(),
                    std::fs::metadata(&path).unwrap().len()
                ),
                format!("url:{url}"),
            ]
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn only_queries_pass_the_read_only_policy() {
        // One statement of every kind the parser can produce; a new kind
        // must be added here (and to the allowlist) on purpose.
        let sql = "SELECT 1; EXPLAIN SELECT 1; DESCRIBE t; SHOW TABLES; SHOW DATABASES; \
                   SHOW MATERIALIZED VIEWS; SHOW MACROS; USE main; \
                   INSERT INTO t VALUES (1, 'x'); UPDATE t SET a = 1; DELETE FROM t; \
                   INSERT INTO t VALUES (1, 'x') ON CONFLICT DO NOTHING; \
                   INSERT OR REPLACE INTO t VALUES (1, 'x'); \
                   INSERT INTO t VALUES (1, 'x') RETURNING *; \
                   UPDATE t SET a = 1 FROM u WHERE t.a = u.a; \
                   DELETE FROM t USING u WHERE t.a = u.a; \
                   MERGE INTO t USING u ON t.a = u.a WHEN MATCHED THEN DELETE; \
                   CREATE TABLE u (a INT); CREATE TABLE u AS SELECT 1; DROP TABLE t; \
                   ALTER TABLE t ADD COLUMN b INT; CREATE VIEW v AS SELECT 1; DROP VIEW v; \
                   CREATE DATABASE d; DROP DATABASE d; COPY t TO 'x.parquet'; \
                   LOAD EXTENSION 'x'; CHECKPOINT; CREATE PROCEDURE p() AS 'x'; \
                   INSTALL x; FORCE INSTALL x FROM 'https://example.test'; \
                   LOAD x; \
                   DROP PROCEDURE p; CREATE MACRO m() AS 1; DROP MACRO m; \
                   CREATE USER u PASSWORD 'x'; ALTER USER u PASSWORD 'y'; DROP USER u; \
                   CREATE ROLE r; DROP ROLE r; BEGIN; COMMIT; ROLLBACK; \
                   CREATE MATERIALIZED VIEW mv AS SELECT 1; \
                   REFRESH MATERIALIZED VIEW mv; REFRESH MATERIALIZED VIEWS; \
                   DROP MATERIALIZED VIEW mv; CREATE INDEX i ON t (a); DROP INDEX i; \
                   DESCRIBE MACRO m";
        // The independent expectation: these are the statement kinds a
        // read-only client may run, spelled out again on purpose.
        let allowed = [
            "Select",
            "Explain",
            "Describe",
            "DescribeMacro",
            "ShowTables",
            "ShowDatabases",
            "ShowMaterializedViews",
            "ShowMacros",
            "Use",
        ];
        let parsed = crate::sql::parse_statements(sql).unwrap();
        let mut seen = std::collections::BTreeSet::new();
        for stmt in &parsed {
            let name = format!("{stmt:?}");
            let name = name
                .split([' ', '('])
                .next()
                .unwrap_or_default()
                .to_string();
            seen.insert(name.clone());
            assert!(
                statement_allowed(stmt, true),
                "{name} must pass with writes"
            );
            assert_eq!(
                statement_allowed(stmt, false),
                allowed.contains(&name.as_str()),
                "{name}"
            );
        }
        for name in allowed {
            assert!(seen.contains(name), "the sample script has no {name}");
        }
    }

    #[test]
    fn a_data_modifying_cte_is_refused_read_only() {
        // The statement parses as a `Select`, so the allowlist must look at
        // its `WITH` bodies.
        for sql in [
            "WITH x AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM x",
            "WITH x AS (UPDATE t SET a = 1 RETURNING *) SELECT * FROM x",
            "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x",
        ] {
            let stmts = crate::sql::parse_statements(sql).unwrap();
            assert!(!statement_allowed(&stmts[0], false), "{sql}");
            assert!(statement_allowed(&stmts[0], true), "{sql}");
        }
    }

    #[test]
    fn a_query_renders_as_agent_markdown() {
        let mut db = store();
        let (code, text) = execute(
            &mut db,
            &ServePolicy::default(),
            "SELECT a FROM t ORDER BY a",
        );
        assert_eq!(code, 0, "{text}");
        assert!(text.starts_with("| a:INTEGER |"), "{text}");
        assert!(text.contains("| 3 |"), "{text}");
    }

    #[test]
    fn writes_are_refused_until_the_policy_allows_them() {
        let mut db = store();
        let policy = ServePolicy::default();
        let (code, text) = execute(&mut db, &policy, "INSERT INTO t VALUES (4, 'four')");
        assert_eq!(code, 1);
        assert_kind(&text, "permission");
        let count = db.execute("SELECT count(*) FROM t").unwrap();
        assert!(
            matches!(&count[0], QueryResult::Selected { rows, .. } if rows[0][0] == crate::Value::Bigint(3))
        );

        let policy = ServePolicy {
            allow_writes: true,
            ..ServePolicy::default()
        };
        let (code, text) = execute(&mut db, &policy, "INSERT INTO t VALUES (4, 'four')");
        assert_eq!(code, 0, "{text}");
        assert_eq!(text, "Inserted 1 rows");
    }

    #[test]
    fn the_display_caps_truncate_with_a_notice() {
        let mut db = store();
        let policy = ServePolicy {
            display: AgentDisplay {
                max_rows: 2,
                ..AgentDisplay::DEFAULT
            },
            ..ServePolicy::default()
        };
        let (code, text) = execute(&mut db, &policy, "SELECT a FROM t");
        assert_eq!(code, 0);
        assert!(text.contains("… 1 more row (3 total)"), "{text}");
    }

    #[test]
    fn the_row_budget_stops_a_large_result() {
        let mut db = store();
        let policy = ServePolicy {
            limits: ExecLimits {
                max_result_rows: Some(1),
                ..ExecLimits::default()
            },
            ..ServePolicy::default()
        };
        let (code, text) = execute(&mut db, &policy, "SELECT a FROM t");
        assert_eq!(code, 1);
        assert!(text.contains("result limit exceeded"), "{text}");
    }

    #[test]
    fn explain_carries_the_cost_hint_only_past_its_threshold() {
        let mut db = store();
        let quiet = execute(&mut db, &ServePolicy::default(), "EXPLAIN SELECT * FROM t");
        assert_eq!(quiet.0, 0, "{}", quiet.1);
        assert!(!quiet.1.contains("Estimated"), "{}", quiet.1);

        let policy = ServePolicy {
            hint_rows: 1,
            ..ServePolicy::default()
        };
        let loud = execute(&mut db, &policy, "EXPLAIN SELECT * FROM t");
        assert_eq!(loud.0, 0, "{}", loud.1);
        assert!(
            loud.1.contains("Estimated 3 rows scanned (local scan)."),
            "{}",
            loud.1
        );
    }

    #[test]
    fn several_statements_are_joined_in_order() {
        let mut db = store();
        let (code, text) = execute(
            &mut db,
            &ServePolicy::default(),
            "SELECT 1 AS a; SELECT 2 AS b",
        );
        assert_eq!(code, 0, "{text}");
        assert!(text.contains("| 1 |") && text.contains("| 2 |"), "{text}");
        assert!(text.find("1 |").unwrap() < text.find("2 |").unwrap());
    }

    #[test]
    fn an_empty_script_and_a_bad_statement_answer_with_json_errors() {
        let mut db = store();
        let empty = execute(&mut db, &ServePolicy::default(), "  -- nothing");
        assert_eq!(empty.0, 1);
        assert!(empty.1.contains("empty statement"), "{}", empty.1);

        let bad = execute(&mut db, &ServePolicy::default(), "SELECT FROM");
        assert_eq!(bad.0, 1);
        assert_kind(&bad.1, "parse");
    }

    #[test]
    fn dot_commands_need_a_host_hook() {
        let mut db = store();
        let refused = execute(&mut db, &ServePolicy::default(), ".lake tables c");
        assert_eq!(refused.0, 1);
        assert!(refused.1.contains("lake catalogs"), "{}", refused.1);

        fn hook(
            _db: &mut PinkDb,
            text: &str,
            read_only: bool,
        ) -> std::result::Result<String, String> {
            Ok(format!("{text} (read-only: {read_only})"))
        }
        let policy = ServePolicy {
            command: Some(hook),
            ..ServePolicy::default()
        };
        assert_eq!(
            execute(&mut db, &policy, " .lake tables c "),
            (0, ".lake tables c (read-only: true)".to_string())
        );
        let writes = ServePolicy {
            allow_writes: true,
            ..policy
        };
        assert_eq!(
            execute(&mut db, &writes, ".lake commit c"),
            (0, ".lake commit c (read-only: false)".to_string())
        );
    }

    /// A served session is one client: the MCP bridge (and the server) hand
    /// each client its own connection, so `SET VARIABLE` in one session is
    /// invisible to the next — it used to live on the store and leaked.
    #[test]
    fn served_sessions_keep_their_variables_apart() {
        let db = crate::db::Database::new();
        let mut first = db.connect();
        let mut second = db.connect();
        let policy = ServePolicy {
            allow_writes: true,
            ..ServePolicy::default()
        };
        {
            let mut host = SessionHost::new(&mut first, &policy);
            assert_eq!(
                host.execute_statement("SET VARIABLE x = 41"),
                (0, "Set variable x".to_string())
            );
            assert_eq!(host.execute_statement("SELECT $x").0, 0);
        }
        {
            let mut host = SessionHost::new(&mut second, &policy);
            let (code, text) = host.execute_statement("SELECT $x");
            assert_eq!(code, 1);
            assert!(text.contains("Variable x does not exist"), "{text}");
            // Its own value shadows nothing in the other session.
            assert_eq!(host.execute_statement("SET VARIABLE x = 7").0, 0);
            assert!(host.execute_statement("SELECT $x").1.contains('7'));
        }
        let mut host = SessionHost::new(&mut first, &policy);
        assert!(host.execute_statement("SELECT $x").1.contains("41"));
    }

    #[test]
    fn a_session_host_refuses_dot_commands() {
        let mut db = store();
        let policy = ServePolicy::default();
        let mut host = SessionHost::new(&mut db, &policy);
        let (code, text) = host.execute_statement(".lake tables c");
        assert_eq!(code, 1);
        assert!(text.contains("lake catalogs"), "{text}");
        let (code, _) = host.execute_statement("SELECT 1");
        assert_eq!(code, 0);
    }

    /// The model's tool channel under `allow-writes` may change the store but
    /// never the agent's own database — not directly, and not through a
    /// data-modifying `WITH`, a trigger action, `CALL` or `DO`.
    #[test]
    fn the_tool_channel_cannot_reach_the_agent_database_indirectly() {
        let mut db = store();
        db.execute("CREATE DATABASE pinkdb_agent").unwrap();
        db.execute("CREATE TABLE pinkdb_agent.messages (n INT)")
            .unwrap();
        db.execute(
            "CREATE PROCEDURE scrub() AS $$ BEGIN DELETE FROM pinkdb_agent.messages; END $$",
        )
        .unwrap();
        let policy = ServePolicy {
            allow_writes: true,
            deny_write_databases: vec![AGENT_DATABASE.to_string()],
            ..ServePolicy::default()
        };
        for sql in [
            "INSERT INTO pinkdb_agent.messages VALUES (1)",
            "WITH x AS (INSERT INTO pinkdb_agent.messages VALUES (1) RETURNING n) SELECT * FROM x",
            "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW \
             EXECUTE INSERT INTO pinkdb_agent.messages VALUES (1)",
            "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE PROCEDURE scrub()",
            "CALL scrub()",
            "DO $$ BEGIN DELETE FROM pinkdb_agent.messages; END $$",
        ] {
            let (code, text) = execute(&mut db, &policy, sql);
            assert_eq!(code, 1, "{sql} was allowed: {text}");
            assert_kind(&text, "permission");
        }
        // Ordinary writes elsewhere still go through.
        let (code, text) = execute(&mut db, &policy, "INSERT INTO t VALUES (4, 'four')");
        assert_eq!(code, 0, "{text}");
        let (code, text) = execute(
            &mut db,
            &policy,
            "CREATE TRIGGER audit AFTER INSERT ON t FOR EACH ROW EXECUTE INSERT INTO t VALUES (0, 'x')",
        );
        assert_eq!(code, 0, "{text}");
    }

    /// The extension's own channel is confined the other way: its database
    /// only, and nothing that could write elsewhere.
    #[test]
    fn the_internal_channel_cannot_leave_the_agent_database() {
        let mut db = store();
        db.execute("CREATE DATABASE pinkdb_agent").unwrap();
        db.execute("CREATE TABLE pinkdb_agent.messages (n INT)")
            .unwrap();
        let policy = ServePolicy {
            allow_writes: true,
            write_database: Some(AGENT_DATABASE.to_string()),
            ..ServePolicy::default()
        };
        for sql in [
            "INSERT INTO t VALUES (9, 'x')",
            "WITH x AS (DELETE FROM t RETURNING a) SELECT * FROM x",
            "CREATE TRIGGER tr AFTER INSERT ON pinkdb_agent.messages FOR EACH ROW \
             EXECUTE DELETE FROM t",
            "DO $$ BEGIN DELETE FROM t; END $$",
        ] {
            let (code, text) = execute_internal(&mut db, &policy, sql);
            assert_eq!(code, 1, "{sql} was allowed: {text}");
            assert_kind(&text, "permission");
        }
        let (code, text) = execute_internal(
            &mut db,
            &policy,
            "INSERT INTO pinkdb_agent.messages VALUES (1)",
        );
        assert_eq!(code, 0, "{text}");
    }
}
