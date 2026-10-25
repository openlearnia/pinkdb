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
            display: AgentDisplay::DEFAULT,
            limits: ExecLimits {
                statement_timeout: Some(std::time::Duration::from_secs(30)),
                max_result_rows: Some(1_000_000),
                max_result_bytes: None,
            },
            command: None,
            hint_rows: 100_000,
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

/// What a statement that opens a file or a URL gets while `allow_files` is
/// off. Naming the switch matters: the operator, not the agent, decides.
const FILES_HINT: &str = "external files and URLs are disabled: this session reads tables only \
                          (start the server with allow-files to permit read_csv, read_parquet \
                          and file paths)";

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

/// What a serve call runs statements on: the embedded store (no privilege
/// check) or a logged-in [`crate::db::Connection`] (a server, where the
/// connection's login makes every statement pass the role checks).
pub trait Session {
    /// Run `sql` under `limits`.
    fn run_statement(&mut self, sql: &str, limits: ExecLimits) -> Result<Vec<QueryResult>>;

    /// The store, for wave 99's cost hint; `None` where the hint is not
    /// reachable (a connection's store is behind the shared lock).
    fn hint_store(&self) -> Option<&PinkDb>;
}

impl Session for PinkDb {
    fn run_statement(&mut self, sql: &str, limits: ExecLimits) -> Result<Vec<QueryResult>> {
        self.execute_with_limits(sql, limits)
    }

    fn hint_store(&self) -> Option<&PinkDb> {
        Some(self)
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
    let statements = checked(policy, sql, Some(&*db))?;
    render(db, policy, sql, &statements)
}

fn run_on<R: Session + ?Sized>(
    session: &mut R,
    policy: &ServePolicy,
    sql: &str,
) -> std::result::Result<String, String> {
    let statements = checked(policy, sql, session.hint_store())?;
    render(session, policy, sql, &statements)
}

/// Parse a batch and apply the policy to every statement of it. The error is
/// the JSON line the client receives, so a refusal explains itself.
///
/// `store` is the session's store when the host has one. A `Connection` (the
/// network server's bridge) does not expose it, because its store is behind
/// the shared lock: there the login's RBAC gate already restricts file scans
/// to administrators (`rbac` M-4), so the policy does not need to see the plan.
fn checked(
    policy: &ServePolicy,
    sql: &str,
    store: Option<&PinkDb>,
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
    if !policy.allow_files
        && let Some(db) = store
        && statements.iter().any(|s| scans_files(db, s))
    {
        return Err(agent::error_json(FILES_HINT, "permission", None));
    }
    Ok(statements)
}

/// Whether one statement would read external data.
///
/// Two checks, because either alone has a blind spot. The syntax check sees
/// a file source before binding, so it also holds on a host with no file
/// source registered (where binding fails and nothing could be read anyway).
/// The plan check sees what the binder rewrites a plain `FROM` into — a view
/// over `read_csv`, say — and reaches table functions inside expressions.
fn scans_files(db: &PinkDb, statement: &Statement) -> bool {
    if ast_scans_files(statement) {
        return true;
    }
    let session_db = db.current_db().to_string();
    let Ok((plan, _)) = crate::sql::bind_tracked(db, statement, &session_db) else {
        return false;
    };
    crate::plan::contains_table_function(&crate::optimizer::optimize(db, plan))
}

/// The syntactic half of [`scans_files`]: any `FROM` source that is a call or
/// a bare path, in every `SELECT` of the statement (CTEs, derived tables,
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
fn outcome_line(result: &QueryResult) -> String {
    /// Names are user data: a table or role name may carry a terminal escape
    /// sequence, and the client renders this text.
    fn name(name: &str) -> String {
        crate::sanitize_terminal(name)
    }
    let rows = |count: usize| agent::format_count(count as u64);
    match result {
        QueryResult::Selected { .. } => unreachable!("selected results are rendered as a table"),
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
                "extension \"{name}\" is linked: only a native extension can serve a client"
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
}
