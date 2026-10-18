//! pinkdb REPL — thin consumer of the `pinkdb` library.
//!
//! Line editing via rustyline, multi-line input until `;`, dot-commands,
//! and DuckDB-style box-drawn result tables with a type row.
//!
//! Scripting follows sqlite3/duckdb conventions: errors go to stderr; when
//! stdin is not a terminal (or with `-c`), the banner and per-statement
//! timing are off and the process exits 1 if any statement or dot-command
//! failed. A statement still open at end of input runs as if terminated.

use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// Set by [`cli_error!`]; decides the exit status of a non-interactive run.
static HAD_ERROR: AtomicBool = AtomicBool::new(false);

/// `.timer on|off` (default: on for an interactive terminal, else off).
static TIMER: AtomicBool = AtomicBool::new(false);

/// Report a failed statement or command: `Error: …` on stderr, and remember
/// it for the exit status.
macro_rules! cli_error {
    ($($arg:tt)*) => {{
        $crate::HAD_ERROR.store(true, ::std::sync::atomic::Ordering::Relaxed);
        eprintln!("Error: {}", format_args!($($arg)*));
    }};
}

use pinkdb::{DataType, DurableOpenOptions, EncryptionKey, PinkDb, QueryResult, Value};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

#[cfg(feature = "client")]
use pinkdb_server::Client;

#[cfg(feature = "repo")]
mod repo;

#[cfg(any(feature = "lake", feature = "lake-lite"))]
mod lake;

const PROMPT: &str = "pinkdb> ";
const CONT_PROMPT: &str = "   ...> ";
/// Rows shown before truncating with a "(N more rows)" note.
const DISPLAY_CAP: usize = 10_000;

fn print_help() {
    println!("Dot-commands:");
    println!("  .help                  show this help");
    println!("  .quit | .exit          leave the shell");
    println!("  .read <file.sql>       run SQL and dot-commands from a file");
    println!("  .timer on|off          print each statement's run time");
    println!("  .databases             list databases (the current one is marked)");
    println!("  .tables                list tables and views of the current database");
    println!("  .schema <table|view>   show a schema (accepts db.table; quoting honored)");
    println!("  .import <file.csv> [table]  import CSV (table defaults to file stem;");
    println!("                         db.table targets another database)");
    println!("  .save <file.pink>      checkpoint the bound file; snapshot elsewhere");
    println!("  .open <file.pink>      open durable: loads the file, replays <file.pink.wal>");
    if cfg!(any(feature = "lake", feature = "lake-lite")) {
        #[cfg(feature = "s3")]
        const LAKE: &str = "<dir|s3://bucket/prefix>";
        #[cfg(not(feature = "s3"))]
        const LAKE: &str = "<dir>";
        println!("  .lake save {LAKE} [force] [author '…'] [message '…']");
        println!("                                snapshot the store into a lake");
        println!("  .lake open {LAKE} [v]      load the lake's latest (or version v) into memory");
        println!("  .lake attach {LAKE} [v] [read-only|write]");
        println!("        read-only (default): query the lake from its statistics");
        println!("        write: materialize its tables into the session store");
        println!(
            "  .lake commit {LAKE}        publish the writable attach as one incremental version"
        );
        println!("  .lake partition {LAKE} <t> by (<key>, ...) | clear");
        println!("                                set/clear a table's partition spec (identity or");
        println!("                                bucket(N, col)); new writes split per value set");
        println!(
            "  .lake merge {LAKE} [t] [min_file_size=N] [max_file_size=N] [max_compacted_files=N]"
        );
        println!(
            "                                consolidate adjacent data files into one version"
        );
        println!("  .lake rewrite {LAKE} [t] [delete_threshold=F] [max_compacted_files=N]");
        println!("                                materialize delete files into fresh data files");
        println!("  .lake expire {LAKE} [versions=a,b] [older_than=T] [keep=N] [dry-run]");
        println!("                                remove old snapshots + commit manifests");
        println!("  .lake cleanup {LAKE} [older_than=T] [all] [dry-run] [confirm]");
        println!("  .lake orphans {LAKE} [older_than=T] [all] [dry-run] [confirm]");
        println!("                                delete data objects no version references");
        println!("  .lake files {LAKE} [t] [all] [version=N]");
        println!("                                list data files (rows, size, partition, epoch)");
        println!("  .lake table_info {LAKE}     per-table file/delete counts and sizes");
        println!("  .lake options {LAKE}        catalog facts (data path, format, encryption)");
        println!("  .lake settings {LAKE}       catalog_type / extension_version / data_path");
        println!("  .lake detach {LAKE} [force]  detach one attached lake");
        println!("  .lake attached              list attached lakes and their tables");
        println!("  .lake versions {LAKE}       list the lake's published versions");
        println!(
            "  .lake snapshots {LAKE}      per-version records: time, author, message, changes"
        );
        println!("  .lake tables {LAKE}         list the current version's tables and row counts");
        println!(
            "  .lake info {LAKE}           show the lake's format version, versions, and tables"
        );
        println!(
            "  FROM t AT (VERSION => n)     query an attached lake table at a version \
             (or AT (TIMESTAMP => TIMESTAMP '…'))"
        );
        if !cfg!(feature = "s3") {
            println!("  .lake s3:// URLs need a build with the `s3` feature");
        }
        if !cfg!(feature = "lake") {
            println!(
                "  (lean lake build: `.pink` data files, format_version 4); Vortex needs `lake`"
            );
        }
    } else {
        println!("  .lake <subcommand>     (not built; needs the `lake` or `lake-lite` feature)");
    }
    if cfg!(feature = "repo") {
        println!("  .install <name[@version]>   install from a repository, then load it");
        println!("  .repositories [list | add <url> | remove <url>]  manage repositories");
        println!("  .installed             list cached extensions under $PINKDB_HOME");
    } else {
        println!("  .install/.repositories/.installed  (not built; needs the `repo` feature)");
    }
    println!("Encryption: PINKDB_KEY=<passphrase> (valid UTF-8) makes .save write and .open");
    println!(
        "            read an encrypted database (needs a build with the `encryption` feature)."
    );
    println!("SQL: end statements with `;`. `USE <db>` switches the session database;");
    println!("     `CREATE DATABASE <db>` / `DROP DATABASE <db>` manage databases;");
    println!("     `LOAD EXTENSION '<path>'` loads a native extension (ABI v1).");
}

/// The `.install`/`.repositories`/`.installed` message when the binary was
/// built without the opt-in `repo` feature.
#[cfg(not(feature = "repo"))]
fn repo_unavailable(cmd: &str) {
    cli_error!("{cmd} is not available: this pinkdb binary was built without the repo feature;");
    eprintln!("       rebuild with `cargo build -p pinkdb-cli --features repo`.");
}

/// The `.lake` message when the binary was built without either lake feature.
#[cfg(not(any(feature = "lake", feature = "lake-lite")))]
fn lake_unavailable(cmd: &str) {
    cli_error!("{cmd} is not available: this pinkdb binary was built without the lake feature;");
    eprintln!(
        "       rebuild with `cargo build -p pinkdb-cli --features lake` (full: Vortex data \
         files, incremental writes, attach, s3 with `,s3`) or `--features lake-lite` (lean: \
         `.pink` data files)."
    );
}

/// `.install <name[@version]>`: resolve, download, verify, cache, and load.
#[cfg(feature = "repo")]
fn cmd_install(db: &mut PinkDb, arg: &str) {
    let arg = arg.trim();
    if arg.is_empty() || arg.split_whitespace().count() != 1 {
        println!("Usage: .install <name[@version]>");
        return;
    }
    let (name, version) = match arg.split_once('@') {
        Some((name, version)) => (name, Some(version)),
        None => (arg, None),
    };
    if name.is_empty() || version.is_some_and(str::is_empty) {
        println!("Usage: .install <name[@version]>");
        return;
    }
    let home = match repo::pinkdb_home() {
        Ok(home) => home,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let repositories = match repo::load_repositories() {
        Ok(repositories) => repositories,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let agent = repo::agent();
    match repo::install(&agent, &repositories, &home, name, version) {
        Ok(installed) => {
            println!(
                "Installed {} {} ({})",
                installed.entry.name, installed.entry.version, installed.entry.platform
            );
            println!("  repository: {}", installed.repository);
            println!("  path:       {}", installed.path.display());
            println!("  sha256:     {}", installed.sha256);
            if !installed.downloaded {
                println!("  cached:     yes");
            }
            match db.load_extension(&installed.path.to_string_lossy()) {
                Ok(loaded) => println!("Extension {loaded} loaded."),
                Err(e) => cli_error!("{e}"),
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.repositories list | add <url> | remove <url>`: manage
/// `$PINKDB_HOME/repositories.json`.
#[cfg(feature = "repo")]
fn cmd_repositories(arg: &str) {
    let (sub, rest) = match arg.find(char::is_whitespace) {
        Some(i) => (&arg[..i], arg[i..].trim()),
        None => (arg, ""),
    };
    match sub {
        "" | "list" => match repo::load_repositories() {
            Ok(urls) => print_repositories(&urls),
            Err(e) => cli_error!("{e}"),
        },
        "add" | "remove" => {
            if rest.is_empty() || rest.split_whitespace().count() != 1 {
                println!("Usage: .repositories {sub} <url>");
                return;
            }
            let mut urls = match repo::load_repositories() {
                Ok(urls) => urls,
                Err(e) => {
                    cli_error!("{e}");
                    return;
                }
            };
            let result = if sub == "add" {
                repo::add_repository(&mut urls, rest)
            } else {
                repo::remove_repository(&mut urls, rest)
            };
            let canonical = match result {
                Ok(canonical) => canonical,
                Err(e) => {
                    cli_error!("{e}");
                    return;
                }
            };
            if let Err(e) = repo::save_repositories(&urls) {
                cli_error!("{e}");
                return;
            }
            if sub == "add" {
                println!("Added repository {canonical}");
            } else {
                println!("Removed repository {canonical}");
                if urls.is_empty() {
                    println!(
                        "Note: no repositories are configured; .install will fail until one is added."
                    );
                }
            }
        }
        _ => println!("Usage: .repositories [list | add <url> | remove <url>]"),
    }
}

#[cfg(feature = "repo")]
fn print_repositories(urls: &[String]) {
    println!("Repositories (in order; the first repository with a match wins):");
    if urls.is_empty() {
        println!("  (none configured — add one with .repositories add <url>)");
        return;
    }
    for (i, url) in urls.iter().enumerate() {
        println!("  {}. {url}", i + 1);
    }
}

/// `.installed`: list cached artifacts under `$PINKDB_HOME/extensions`.
#[cfg(feature = "repo")]
fn cmd_installed() {
    let home = match repo::pinkdb_home() {
        Ok(home) => home,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    match repo::list_installed(&home) {
        Ok(files) if files.is_empty() => println!("(no extensions installed)"),
        Ok(files) => {
            println!(
                "Installed extensions ({}):",
                repo::extensions_dir(&home).display()
            );
            for f in files {
                println!(
                    "  {} {} {} {}",
                    f.name,
                    f.version,
                    f.platform,
                    f.path.display()
                );
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

fn cmd_databases(db: &PinkDb) {
    let current = db.current_db();
    for name in db.list_databases() {
        let nt = db.list_tables_in(&name).len();
        let nv = db.list_views_in(&name).len();
        let marker = if name.eq_ignore_ascii_case(current) {
            " (current)"
        } else {
            ""
        };
        let tables = format!("{nt} table{}", plural(nt));
        let label = if nv == 0 {
            format!("({tables})")
        } else {
            format!("({tables}, {nv} view{})", plural(nv))
        };
        println!("{name}{marker} {label}");
    }
}

fn plural(n: usize) -> &'static str {
    if n == 1 {
        ""
    } else {
        "s"
    }
}

fn cmd_tables(db: &PinkDb) {
    let tables = db.list_tables();
    let views = db.list_views();
    let attached: Vec<pinkdb::AttachedTable> = db
        .attached_tables()
        .into_iter()
        .filter(|t| t.db.eq_ignore_ascii_case(db.current_db()))
        .collect();
    if tables.is_empty() && views.is_empty() && attached.is_empty() {
        println!("(no tables)");
        return;
    }
    for t in tables {
        let n = db.table(&t).map(|x| x.num_rows()).unwrap_or(0);
        println!("{t} ({n} rows)");
    }
    for t in attached {
        println!("{} ({} rows, attached)", t.table, t.rows);
    }
    for v in views {
        println!("{v} (view)");
    }
}

fn cmd_schema(db: &mut PinkDb, arg: &str) {
    let name = arg.trim();
    if name.is_empty() {
        println!("Usage: .schema <table|view>");
        return;
    }
    let (qdb, table) = match pinkdb::csv::parse_table_target(name) {
        Ok(parsed) => parsed,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    // `db.table` is a qualifier only when `db` names an existing database;
    // otherwise the whole string is the table name (same rule as `.import`).
    let (lookup_db, lookup_name) = match qdb {
        Some(q) if db.database_exists(&q) => (Some(q), table),
        Some(q) => (None, format!("{q}.{table}")),
        None => (None, table),
    };
    let table_result = match &lookup_db {
        Some(q) => db.table_in(q, &lookup_name),
        None => db.table(&lookup_name),
    };
    match table_result {
        Ok(t) => {
            println!("CREATE TABLE {} (", t.name);
            let schema = t.schema();
            for (i, (col, dtype)) in schema.iter().enumerate() {
                let comma = if i + 1 == schema.len() { "" } else { "," };
                println!("  {col} {dtype}{comma}");
            }
            println!("); -- {} rows, {} chunks", t.num_rows(), t.num_chunks());
        }
        Err(table_err) => {
            // An attached lake table is not in the store but is queryable;
            // `.schema` shows its footer-read schema and row count.
            let attached = match &lookup_db {
                Some(q) => db.attached_table_in(q, &lookup_name),
                None => db.attached_table_in(db.current_db(), &lookup_name),
            };
            if let Some(att) = attached {
                println!("CREATE TABLE {} (", att.table);
                for (i, (col, dtype)) in att.schema.iter().enumerate() {
                    let comma = if i + 1 == att.schema.len() { "" } else { "," };
                    println!("  {col} {dtype}{comma}");
                }
                println!("); -- {} rows, attached (read-only)", att.rows);
                return;
            }
            // Not a table: `.schema db.v` on a view shows the view's output
            // columns (bound through `LIMIT 0`, so a broken body is a clear
            // error, never stale columns).
            let view_result = match &lookup_db {
                Some(q) => db.view_in(q, &lookup_name),
                None => db.view(&lookup_name),
            };
            match view_result {
                Ok(v) => {
                    let view_name = v.name.clone();
                    let qualified = match &lookup_db {
                        Some(q) => format!("{}.{}", quote_ident(q), quote_ident(&view_name)),
                        None => quote_ident(&view_name),
                    };
                    match db.execute(&format!("SELECT * FROM {qualified} LIMIT 0;")) {
                        Ok(results) => match results.first() {
                            Some(QueryResult::Selected { names, types, .. }) => {
                                println!("CREATE VIEW {view_name} (");
                                for (i, (n, t)) in names.iter().zip(types).enumerate() {
                                    let comma = if i + 1 == names.len() { "" } else { "," };
                                    println!("  {n} {t}{comma}");
                                }
                                println!("); -- view (session-local)");
                            }
                            _ => {
                                cli_error!("view \"{view_name}\" did not produce a schema")
                            }
                        },
                        Err(e) => cli_error!("{e}"),
                    }
                }
                Err(_) => cli_error!("{table_err}; no such view either"),
            }
        }
    }
}

/// Split one whitespace-delimited token off the front of `input`, keeping
/// double-quoted spans together (the quotes stay in the token so the
/// identifier parser can honor them).
fn take_token(input: &str) -> Option<(String, &str)> {
    let s = input.trim_start();
    if s.is_empty() {
        return None;
    }
    let mut in_quotes = false;
    let mut end = s.len();
    for (i, c) in s.char_indices() {
        if c == '"' {
            in_quotes = !in_quotes;
        } else if c.is_whitespace() && !in_quotes {
            end = i;
            break;
        }
    }
    Some((s[..end].to_string(), &s[end..]))
}

/// Strip surrounding quotes from a token and unescape `""` (file paths).
fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
        t[1..t.len() - 1].replace("\"\"", "\"")
    } else {
        t.to_string()
    }
}

/// Quote an identifier for SQL (embedded quotes doubled).
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn cmd_import(db: &mut PinkDb, arg: &str) {
    let Some((file, rest)) = take_token(arg) else {
        println!("Usage: .import <file.csv> [table]");
        return;
    };
    let file = unquote(&file);
    let table = match take_token(rest) {
        Some((t, tail)) if tail.trim().is_empty() => Some(t),
        Some(_) => {
            println!("Usage: .import <file.csv> [table]");
            return;
        }
        None => None,
    };
    let t0 = Instant::now();
    match pinkdb::csv::import_csv(db, &file, table.as_deref()) {
        Ok(n) => println!("Imported {n} rows in {:.3}s", t0.elapsed().as_secs_f64()),
        Err(e) => cli_error!("{e}"),
    }
}

/// One REPL session: in-process, or (with the `client` feature) remote.
/// Exactly one exists per process, so the size gap between the variants is
/// irrelevant.
#[allow(clippy::large_enum_variant)]
enum Session {
    Local(LocalSession),
    #[cfg(feature = "client")]
    Remote(Client),
}

/// Bounded retries for a transaction whose COMMIT hits a cross-process write
/// conflict (wave 45o): the engine returns the conflict and aborts the
/// transaction, so the CLI re-runs the buffered transaction on the refreshed
/// state. `PINKDB_TXN_RETRIES` overrides the default (0 = report the first
/// conflict, no retry).
const DEFAULT_TXN_RETRIES: usize = 3;

/// One local session: the store plus the retryable-transaction buffer.
struct LocalSession {
    db: PinkDb,
    /// Raw SQL of every statement executed since BEGIN, in order (cleared
    /// when the transaction ends). A conflicting COMMIT re-runs it after a
    /// `USE <pre-BEGIN session>` prefix.
    txn_sql: String,
    /// Session database before BEGIN, restored on a retry.
    txn_pre_db: Option<String>,
    /// Bounded retry budget (see [`DEFAULT_TXN_RETRIES`]).
    txn_retries: usize,
}

impl LocalSession {
    fn new(db: PinkDb) -> Self {
        let txn_retries = std::env::var("PINKDB_TXN_RETRIES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_TXN_RETRIES);
        LocalSession {
            db,
            txn_sql: String::new(),
            txn_pre_db: None,
            txn_retries,
        }
    }

    /// Execute one SQL string, maintaining the transaction buffer and
    /// retrying a conflicting COMMIT (bounded). When a retry succeeds only
    /// the transaction's outcome is reported (the earlier attempt's
    /// statement results were already rendered and its effects discarded by
    /// the abort).
    fn run_sql(&mut self, sql: &str) -> Result<Vec<QueryResult>, pinkdb::PinkDbError> {
        let was = self.db.in_transaction();
        let pre_db = self.db.current_db().to_string();
        match self.db.execute(sql) {
            Ok(results) => {
                let now = self.db.in_transaction();
                if !was && now {
                    self.txn_pre_db = Some(pre_db);
                    self.txn_sql.clear();
                    self.txn_sql.push_str(sql);
                    self.txn_sql.push('\n');
                } else if was && now {
                    self.txn_sql.push_str(sql);
                    self.txn_sql.push('\n');
                } else if was && !now {
                    self.txn_sql.clear();
                    self.txn_pre_db = None;
                }
                Ok(results)
            }
            Err(e) => {
                let now = self.db.in_transaction();
                // Retry only a conflict that ended the transaction on a
                // COMMIT (the last statement of this call). Anything else
                // (a failed statement inside the txn, a lease failure that
                // left it open, a non-conflict error) is reported as-is. The
                // transaction may have been opened by an earlier call or by
                // this one (`BEGIN; …; COMMIT;` in a single string).
                let started_here = !was && starts_transaction(sql);
                if (was || started_here)
                    && !now
                    && self.txn_retries > 0
                    && is_conflict_error(&e)
                    && ends_with_commit(sql)
                {
                    let pre = self.txn_pre_db.take().unwrap_or_else(|| pre_db.clone());
                    let mut retry_sql = format!("USE \"{}\";\n", pre.replace('"', "\"\""));
                    retry_sql.push_str(&self.txn_sql);
                    retry_sql.push_str(sql);
                    retry_sql.push('\n');
                    self.txn_sql.clear();
                    let mut last = e;
                    for attempt in 1..=self.txn_retries {
                        // The conflict is reported (the engine aborted the
                        // transaction with that error), then the retry runs;
                        // only a retry that exhausts the budget fails the run.
                        eprintln!("Error: {last}");
                        eprintln!(
                            "Retrying the transaction after a cross-process write conflict \
                             (attempt {attempt}/{})",
                            self.txn_retries
                        );
                        match self.db.execute(&retry_sql) {
                            Ok(mut results) => {
                                self.txn_pre_db = None;
                                // Only the transaction's outcome: the
                                // statement results of the first attempt
                                // were already printed.
                                return Ok(vec![results.pop().unwrap_or(QueryResult::Committed)]);
                            }
                            Err(e2) => {
                                if self.db.in_transaction() {
                                    // A retry that failed mid-transaction
                                    // (not a conflict) left it open: drop it
                                    // before deciding.
                                    let _ = self.db.execute("ROLLBACK;");
                                }
                                if !is_conflict_error(&e2) {
                                    return Err(e2);
                                }
                                last = e2;
                            }
                        }
                    }
                    Err(last)
                } else {
                    Err(e)
                }
            }
        }
    }
}

/// A retryable cross-process conflict (`PinkDbError` has no dedicated
/// variant: the message prefix is the stable contract, like `busy`).
fn is_conflict_error(e: &pinkdb::PinkDbError) -> bool {
    e.to_string().contains("write conflict on")
}

/// Does this SQL string end with `COMMIT` (the only statement a conflicting
/// transaction is retried on)?
fn ends_with_commit(sql: &str) -> bool {
    match pinkdb::sql::parse_statements(sql) {
        Ok(stmts) => matches!(stmts.last(), Some(pinkdb::sql::ast::Statement::Commit)),
        Err(_) => false,
    }
}

/// Does this SQL string open a transaction (`BEGIN; …; COMMIT;` in a single
/// call, where the session was not in one when the call started)?
fn starts_transaction(sql: &str) -> bool {
    match pinkdb::sql::parse_statements(sql) {
        Ok(stmts) => matches!(stmts.first(), Some(pinkdb::sql::ast::Statement::Begin)),
        Err(_) => false,
    }
}

/// Returns false when the REPL should exit.
fn run_dot(session: &mut Session, line: &str) -> bool {
    let (cmd, rest) = match line.find(char::is_whitespace) {
        Some(i) => (&line[..i], line[i..].trim()),
        None => (line, ""),
    };
    // Session-independent commands (local and remote alike).
    match cmd {
        ".timer" => {
            match rest {
                "on" => TIMER.store(true, Ordering::Relaxed),
                "off" => TIMER.store(false, Ordering::Relaxed),
                _ => println!("Usage: .timer on|off"),
            }
            return true;
        }
        ".read" => {
            let path = unquote(rest);
            if path.is_empty() {
                println!("Usage: .read <file.sql>");
                return true;
            }
            return run_script_file(session, &path);
        }
        _ => {}
    }
    match session {
        Session::Local(local) => run_dot_local(&mut local.db, line),
        #[cfg(feature = "client")]
        Session::Remote(_) => run_dot_remote(line),
    }
}

/// Returns false when the REPL should exit.
fn run_dot_local(db: &mut PinkDb, line: &str) -> bool {
    let (cmd, rest) = match line.find(char::is_whitespace) {
        Some(i) => (&line[..i], line[i..].trim()),
        None => (line, ""),
    };
    match cmd {
        ".help" => print_help(),
        ".quit" | ".exit" => return false,
        ".databases" => cmd_databases(db),
        ".tables" => cmd_tables(db),
        ".schema" => cmd_schema(db, rest),
        ".import" => cmd_import(db, rest),
        ".install" => {
            #[cfg(feature = "repo")]
            cmd_install(db, rest);
            #[cfg(not(feature = "repo"))]
            repo_unavailable(".install");
        }
        ".repositories" => {
            #[cfg(feature = "repo")]
            cmd_repositories(rest);
            #[cfg(not(feature = "repo"))]
            repo_unavailable(".repositories");
        }
        ".installed" => {
            #[cfg(feature = "repo")]
            cmd_installed();
            #[cfg(not(feature = "repo"))]
            repo_unavailable(".installed");
        }
        ".save" => {
            let path = take_token(rest)
                .map(|(p, _)| unquote(&p))
                .unwrap_or_default();
            if path.is_empty() {
                println!("Usage: .save <file.pink>");
            } else {
                let t0 = Instant::now();
                // Saving to the bound database is a checkpoint: atomic
                // publish plus a WAL generation roll. The target is compared
                // by filesystem identity, so `./x.pink`, absolute paths, and
                // case-only differences on case-insensitive filesystems all
                // checkpoint instead of silently snapshotting (which would
                // let the WAL replay committed rows twice). Any other target
                // is a snapshot, refused when it would clobber another
                // database whose WAL still holds records.
                let result = if db.is_bound_to(&path) {
                    db.checkpoint()
                } else {
                    db.save_snapshot(&path)
                };
                match result {
                    Ok(()) => {
                        let any_views = db
                            .list_databases()
                            .iter()
                            .any(|d| !db.list_views_in(d).is_empty());
                        if any_views {
                            println!("Note: views are session-local and were not saved.");
                        }
                        let attached = db.attached_tables().len();
                        if attached > 0 {
                            println!(
                                "Note: attached lake tables are session-local and were not \
                                 saved ({attached} table{}).",
                                plural(attached)
                            );
                        }
                        println!("Saved in {:.3}s", t0.elapsed().as_secs_f64())
                    }
                    Err(e) => cli_error!("{e}"),
                }
            }
        }
        ".open" => {
            let path = take_token(rest)
                .map(|(p, _)| unquote(&p))
                .unwrap_or_default();
            if path.is_empty() {
                println!("Usage: .open <file.pink>");
            } else {
                // Re-opening the database this session already holds: release
                // the current store first so its WAL handle is closed (the
                // per-operation lease means this is an optimization, not a
                // lock requirement).
                if db.is_bound_to(&path) {
                    let key = db.encryption_key().cloned();
                    *db = PinkDb::new();
                    db.set_encryption_key(key);
                }
                // `PINKDB_KEY` (if set) is already installed on the store, so
                // both the checkpoint and the snapshot path encrypt.
                let opts = durable_options();
                let result = match db.encryption_key() {
                    Some(k) => PinkDb::open_durable_with_key_and_options(&path, k, opts),
                    None => PinkDb::open_durable_with_options(&path, opts),
                };
                match result {
                    Ok((new_db, info)) => {
                        if info.checkpoints_completed > 0 {
                            println!("Completed an interrupted checkpoint from its .tmp state");
                        }
                        if info.replayed_records > 0 {
                            println!(
                                "Replayed {} WAL record(s) ({} statement(s))",
                                info.replayed_records, info.replayed_statements
                            );
                        }
                        if info.truncated_bytes > 0 {
                            println!(
                                "Truncated {} byte(s) of torn WAL tail",
                                info.truncated_bytes
                            );
                        }
                        *db = new_db;
                        println!("Opened {path}");
                    }
                    Err(e) => cli_error!("{e}"),
                }
            }
        }
        ".lake" => {
            #[cfg(any(feature = "lake", feature = "lake-lite"))]
            lake::cmd_lake(db, rest);
            #[cfg(not(any(feature = "lake", feature = "lake-lite")))]
            lake_unavailable(".lake");
        }
        _ => cli_error!("unknown dot-command \"{cmd}\". Try .help"),
    }
    true
}

/// Remote-mode dot-commands: the file and catalog commands need the local
/// store (`.import`/`.save`/`.open`/`.databases`/`.tables`/`.schema`) and are
/// not available over the wire; the repository commands would load a native
/// extension into the client process, not the server, so they are rejected
/// too. SQL (including `USE`) works unchanged.
#[cfg(feature = "client")]
fn run_dot_remote(line: &str) -> bool {
    let (cmd, _rest) = match line.find(char::is_whitespace) {
        Some(i) => (&line[..i], line[i..].trim()),
        None => (line, ""),
    };
    match cmd {
        ".help" => print_help_remote(),
        ".quit" | ".exit" => return false,
        ".databases" | ".tables" | ".schema" | ".import" | ".save" | ".open" | ".install"
        | ".repositories" | ".installed" | ".lake" => {
            cli_error!("{cmd} is not available in remote mode");
        }
        _ => cli_error!("unknown dot-command \"{cmd}\". Try .help"),
    }
    true
}

#[cfg(feature = "client")]
fn print_help_remote() {
    println!("Dot-commands (remote mode):");
    println!("  .help                  show this help");
    println!("  .quit | .exit          leave the shell");
    println!("  .read <file.sql>       run SQL and dot-commands from a file");
    println!("  .timer on|off          print each statement's run time");
    println!("  .databases/.tables/.schema/.import/.save/.open are not available in remote mode");
    println!("  .install/.repositories/.installed are not available in remote mode");
    println!("  .lake is not available in remote mode");
    println!("SQL: end statements with `;`. `USE <db>` switches the session database;");
    println!("     `CREATE DATABASE <db>` / `DROP DATABASE <db>` manage databases;");
    println!("     `LOAD EXTENSION '<path>'` loads a native extension (ABI v1).");
}

fn display_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        // DuckDB prints `1.0`, not `1`.
        Value::Double(x) => {
            if x.fract() == 0.0 && x.is_finite() && x.abs() < 1e15 {
                format!("{x:.1}")
            } else {
                format!("{x}")
            }
        }
        // Stored text is untrusted: never emit raw control characters (a
        // terminal escape sequence in a row would inject output).
        Value::Varchar(s) => sanitize_cell(s),
        _ => v.to_string(),
    }
}

/// Escape control characters in a displayed VARCHAR cell: `\n`/`\r` become a
/// space (a newline would break the box) and every other control (notably ESC)
/// is shown as `\u{..}`.
fn sanitize_cell(s: &str) -> String {
    if !s.chars().any(char::is_control) {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\t' => out.push('\t'),
            '\n' | '\r' => out.push(' '),
            c if c.is_control() => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{{{:x}}}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

/// Formats a `SELECT` result as the DuckDB-style box table: header names,
/// type row, up to [`DISPLAY_CAP`] data rows, and the trailing summary line.
///
/// Only the shown rows are formatted: column widths are computed from the
/// header names, the type row, and those rows, so huge results don't pay for
/// rows that are never displayed. The summary still reports the true total
/// row count.
///
/// The result has no trailing newline; `render_results` prints it with
/// `println!` so a single call reproduces the old per-line output byte for
/// byte.
fn format_select(names: &[String], types: &[DataType], rows: &[Vec<Value>]) -> String {
    let shown = rows.len().min(DISPLAY_CAP);
    let cells: Vec<Vec<String>> = rows
        .iter()
        .take(shown)
        .map(|r| r.iter().map(display_value).collect())
        .collect();
    let type_row: Vec<String> = types.iter().map(|t| t.name().to_string()).collect();
    let mut widths: Vec<usize> = names.iter().map(|n| n.chars().count().max(1)).collect();
    for (i, t) in type_row.iter().enumerate() {
        widths[i] = widths[i].max(t.chars().count());
    }
    for row in &cells {
        for (i, c) in row.iter().enumerate() {
            widths[i] = widths[i].max(c.chars().count());
        }
    }

    let mut out = String::new();
    let border = |out: &mut String, l: &str, m: &str, r: &str| {
        out.push_str(l);
        for (i, w) in widths.iter().enumerate() {
            if i > 0 {
                out.push_str(m);
            }
            out.push_str(&"─".repeat(w + 2));
        }
        out.push_str(r);
        out.push('\n');
    };
    let row_out = |out: &mut String, items: &[String]| {
        out.push('│');
        for (i, c) in items.iter().enumerate() {
            let w = widths[i];
            out.push(' ');
            out.push_str(c);
            out.push_str(&" ".repeat(w - c.chars().count() + 1));
            out.push('│');
        }
        out.push('\n');
    };

    border(&mut out, "┌", "┬", "┐");
    row_out(&mut out, names);
    row_out(&mut out, &type_row);
    border(&mut out, "├", "┼", "┤");
    for row in &cells {
        row_out(&mut out, row);
    }
    border(&mut out, "└", "┴", "┘");
    if rows.len() > DISPLAY_CAP {
        out.push_str(&format!(
            "({} more rows not shown)\n",
            rows.len() - DISPLAY_CAP
        ));
    }
    let noun = if rows.len() == 1 { "row" } else { "rows" };
    out.push_str(&format!("{} {noun}", rows.len()));
    out
}

/// Print each statement result with the shared DuckDB-style renderer.
fn render_results(results: &[QueryResult]) {
    for r in results {
        match r {
            QueryResult::Created => println!("Table created."),
            QueryResult::Inserted { count } => println!("Inserted {count} row(s)."),
            QueryResult::Updated { count } => println!("Updated {count} row(s)."),
            QueryResult::Deleted { count } => println!("Deleted {count} row(s)."),
            QueryResult::Began => println!("Transaction started."),
            QueryResult::Committed => println!("Transaction committed."),
            QueryResult::RolledBack => println!("Transaction rolled back."),
            QueryResult::CreatedView { name } => println!("View {name} created."),
            QueryResult::DroppedTable { name } => println!("Table {name} dropped."),
            QueryResult::CreatedIndex { name } => println!("Index {name} created."),
            QueryResult::DroppedIndex { name } => println!("Index {name} dropped."),
            QueryResult::CreatedMaterializedView { name } => {
                println!("Materialized view {name} created.")
            }
            QueryResult::RefreshedMaterializedView { name } => {
                println!("Materialized view {name} refreshed.")
            }
            QueryResult::DroppedMaterializedView { name } => {
                println!("Materialized view {name} dropped.")
            }
            QueryResult::DroppedView { name } => println!("View {name} dropped."),
            QueryResult::CreatedDatabase { name } => {
                println!("Database {name} created.")
            }
            QueryResult::DroppedDatabase { name } => {
                println!("Database {name} dropped.")
            }
            QueryResult::Used { db } => println!("Using database {db}."),
            QueryResult::LoadedExtension { name } => {
                println!("Extension {name} loaded.")
            }
            QueryResult::Checkpointed => println!("Checkpoint complete."),
            QueryResult::AlteredTable { name } => println!("Table {name} altered."),
            QueryResult::Selected { names, types, rows } => {
                if rows.is_empty() {
                    println!("(no rows)");
                }
                let table = format_select(names, types, rows);
                println!("{table}");
            }
        }
    }
}

/// Run one SQL string in the session's mode (local or remote) and print the
/// results with the same renderer and the same timing line either way.
fn run_sql_session(session: &mut Session, sql: &str) {
    let t0 = Instant::now();
    match session {
        Session::Local(local) => match local.run_sql(sql) {
            Ok(results) => render_results(&results),
            Err(e) => cli_error!("{e}"),
        },
        #[cfg(feature = "client")]
        Session::Remote(client) => match client.execute(sql) {
            Ok(results) => render_results(&results),
            Err(e) => cli_error!("{e}"),
        },
    }
    if TIMER.load(Ordering::Relaxed) {
        println!("Run Time (s): real {:.3}", t0.elapsed().as_secs_f64());
    }
}

/// Parsed command line.
struct Opts {
    host: Option<String>,
    /// `-c` arguments, run in order instead of reading stdin.
    commands: Vec<String>,
}

fn print_usage() {
    println!("Usage: pinkdb [--host HOST:PORT] [-c COMMAND]...");
    println!();
    println!("  -c COMMAND         run SQL or a dot-command, then exit (repeatable)");
    println!("  --host HOST:PORT   run as a client of a pinkdb-server (remote mode)");
    if cfg!(feature = "client") {
        println!("                     [client feature: enabled]");
    } else {
        println!("                     [client feature: disabled in this build]");
    }
}

fn parse_args() -> Result<Opts, String> {
    let mut host = None;
    let mut commands = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            "--host" => {
                let value = args.next().ok_or("--host requires HOST:PORT")?;
                host = Some(value);
            }
            other if other.starts_with("--host=") => {
                host = Some(other["--host=".len()..].to_string());
            }
            "-c" => {
                let value = args.next().ok_or("-c requires a command")?;
                commands.push(value);
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    Ok(Opts { host, commands })
}

/// Build the REPL session: local by default, remote when `--host` is given
/// (the `client` feature; without it, `--host` is rejected). The `PINKDB_KEY`
/// passphrase, when set, becomes the store's at-rest key in local mode; in
/// remote mode the variable is inert (the server's own at-rest configuration
/// applies), so a set key is called out instead of being silently ignored.
fn build_session(host: Option<String>, key: Option<EncryptionKey>) -> Session {
    if host.is_some() && key.is_some() {
        eprintln!(
            "warning: PINKDB_KEY is set but is not used in remote mode; \
             the server's at-rest configuration applies"
        );
    }
    match host {
        None => {
            let mut db = PinkDb::new();
            db.set_encryption_key(key);
            Session::Local(LocalSession::new(db))
        }
        #[cfg(feature = "client")]
        Some(host) => match Client::connect(&host) {
            Ok(client) => Session::Remote(client),
            Err(e) => {
                eprintln!("Error: cannot connect to {host}: {e}");
                std::process::exit(1);
            }
        },
        #[cfg(not(feature = "client"))]
        Some(_) => {
            eprintln!(
                "Error: --host was given, but this pinkdb binary was built without the client feature;"
            );
            eprintln!("       rebuild with `cargo build -p pinkdb-cli --features client`.");
            std::process::exit(2);
        }
    }
}

/// Durable-open options from the environment: `PINKDB_BUSY_TIMEOUT_MS`
/// (optional) bounds how long `.open` and later statements wait for another
/// process's write lease before reporting the database busy.
fn durable_options() -> DurableOpenOptions {
    let mut opts = DurableOpenOptions::default();
    if let Ok(ms) = std::env::var("PINKDB_BUSY_TIMEOUT_MS") {
        if let Ok(ms) = ms.trim().parse::<u64>() {
            opts.busy_timeout = std::time::Duration::from_millis(ms);
        }
    }
    opts
}

fn main() {
    let opts = match parse_args() {
        Ok(opts) => opts,
        Err(msg) => {
            eprintln!("Error: {msg}");
            print_usage();
            std::process::exit(2);
        }
    };
    let host = opts.host;
    let mut session = build_session(
        host.clone(),
        match EncryptionKey::from_env() {
            Ok(key) => key,
            Err(e) => {
                // A set-but-non-UTF-8 PINKDB_KEY must not silently fall back to
                // plaintext.
                eprintln!("Error: {e}");
                std::process::exit(2);
            }
        },
    );

    // `-c`: run the given commands as one script and exit.
    if !opts.commands.is_empty() {
        let mut input = Input::new();
        for command in &opts.commands {
            for line in command.lines() {
                if !input.feed(&mut session, line) {
                    exit_for_script();
                }
            }
            // Each -c argument is complete on its own.
            input.finish(&mut session);
        }
        exit_for_script();
    }

    let interactive = std::io::stdin().is_terminal();
    TIMER.store(interactive, Ordering::Relaxed);
    if interactive {
        match &session {
            Session::Local(_) => {
                println!("pinkdb v0.1.0 — mini DuckDB (in-memory). Type .help, end SQL with ;")
            }
            #[cfg(feature = "client")]
            Session::Remote(client) => println!(
                "pinkdb v0.1.0 — mini DuckDB (remote {}, server v{}). Type .help, end SQL with ;",
                host.as_deref().unwrap_or(""),
                client.server_version()
            ),
        }
    }
    let mut rl = match DefaultEditor::new() {
        Ok(rl) => rl,
        Err(e) => {
            // No line editor (unusual terminal): read plain lines.
            eprintln!("rustyline init failed ({e}); reading plain stdin");
            run_plain_stdin(&mut session);
            exit_for_script();
        }
    };

    let mut input = Input::new();
    loop {
        let prompt = if input.is_empty() {
            PROMPT
        } else {
            CONT_PROMPT
        };
        match rl.readline(prompt) {
            Ok(line) => {
                if !line.trim().is_empty() {
                    if let Some(entry) = input.history_entry(&line) {
                        rl.add_history_entry(entry.as_str()).ok();
                    }
                }
                if !input.feed(&mut session, &line) {
                    break;
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C: drop pending input, stay in shell.
                input.clear();
                println!("(^C — input cleared)");
            }
            Err(ReadlineError::Eof) => {
                input.finish(&mut session);
                break;
            }
            Err(e) => {
                eprintln!("Readline error: {e}");
                break;
            }
        }
    }
    if !interactive {
        exit_for_script();
    }
}

/// Run a script file's SQL and dot-commands in this session (`.read`).
/// Nested `.read` works; false when the script asked to quit.
fn run_script_file(session: &mut Session, path: &str) -> bool {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) => {
            cli_error!("cannot read {path}: {e}");
            return true;
        }
    };
    let mut input = Input::new();
    for line in text.lines() {
        if !input.feed(session, line) {
            return false;
        }
    }
    input.finish(session);
    true
}

/// Exit after a script: status 1 when anything failed (sqlite3/duckdb
/// convention), else 0.
fn exit_for_script() -> ! {
    std::process::exit(i32::from(HAD_ERROR.load(Ordering::Relaxed)));
}

/// Line-by-line stdin without rustyline (fallback).
fn run_plain_stdin(session: &mut Session) {
    let mut input = Input::new();
    for line in std::io::stdin().lines() {
        let Ok(line) = line else { break };
        if !input.feed(session, &line) {
            return;
        }
    }
    input.finish(session);
}

/// Accumulates input lines into statements: dot-commands at the start of a
/// fresh statement run immediately; SQL runs once a line completes it.
struct Input {
    pending: String,
}

impl Input {
    fn new() -> Self {
        Input {
            pending: String::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn clear(&mut self) {
        self.pending.clear();
    }

    /// The history entry `line` completes (a dot-command or a whole
    /// statement), computed before [`Input::feed`] consumes it.
    fn history_entry(&self, line: &str) -> Option<String> {
        let trimmed = line.trim();
        if self.pending.is_empty() && trimmed.starts_with('.') {
            return Some(line.to_string());
        }
        let mut joined = self.pending.clone();
        joined.push_str(line);
        joined.push('\n');
        statement_is_complete(&joined).then_some(joined)
    }

    /// Feed one line; false when the session should end (`.quit`).
    fn feed(&mut self, session: &mut Session, line: &str) -> bool {
        let trimmed = line.trim();
        if trimmed.is_empty() && self.pending.is_empty() {
            return true;
        }
        // Dot-commands only at the start of a fresh statement.
        if self.pending.is_empty() && trimmed.starts_with('.') {
            return run_dot(session, trimmed);
        }
        self.pending.push_str(line);
        self.pending.push('\n');
        if statement_is_complete(&self.pending) {
            let sql = std::mem::take(&mut self.pending);
            run_sql_session(session, sql.trim());
        }
        true
    }

    /// End of input: a statement still open runs as if terminated (unless
    /// it is only whitespace and comments).
    fn finish(&mut self, session: &mut Session) {
        let sql = std::mem::take(&mut self.pending);
        if has_sql_content(&sql) {
            run_sql_session(session, sql.trim());
        }
    }
}

/// Lexical state while scanning SQL for statement ends.
#[derive(Clone, Copy, PartialEq)]
enum Lex {
    Code,
    Str,
    Ident,
    LineComment,
    BlockComment,
}

/// Scan `sql`, returning the final lexical state and whether the last
/// significant (non-space, non-comment) character in code was `;` — plus
/// whether any code at all was seen.
fn scan_sql(sql: &str) -> (Lex, bool, bool) {
    let mut state = Lex::Code;
    let mut last_semicolon = false;
    let mut any_code = false;
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        let next = bytes.get(i + 1).copied();
        match state {
            Lex::Code => match c {
                b'\'' => {
                    state = Lex::Str;
                    last_semicolon = false;
                    any_code = true;
                }
                b'"' => {
                    state = Lex::Ident;
                    last_semicolon = false;
                    any_code = true;
                }
                b'-' if next == Some(b'-') => {
                    state = Lex::LineComment;
                    i += 1;
                }
                b'/' if next == Some(b'*') => {
                    state = Lex::BlockComment;
                    i += 1;
                }
                b';' => {
                    last_semicolon = true;
                    any_code = true;
                }
                c if c.is_ascii_whitespace() => {}
                _ => {
                    last_semicolon = false;
                    any_code = true;
                }
            },
            // `''` inside a string is an escaped quote: leaving and
            // re-entering the string state handles it naturally.
            Lex::Str => {
                if c == b'\'' {
                    state = Lex::Code;
                }
            }
            Lex::Ident => {
                if c == b'"' {
                    state = Lex::Code;
                }
            }
            Lex::LineComment => {
                if c == b'\n' {
                    state = Lex::Code;
                }
            }
            Lex::BlockComment => {
                if c == b'*' && next == Some(b'/') {
                    state = Lex::Code;
                    i += 1;
                }
            }
        }
        i += 1;
    }
    (state, last_semicolon, any_code)
}

/// True when `sql` ends a statement: the last significant character outside
/// strings, quoted identifiers and comments is `;` (so `SELECT 1; -- note`
/// is complete, and `SELECT ';'` is not).
fn statement_is_complete(sql: &str) -> bool {
    let (state, last_semicolon, _) = scan_sql(sql);
    matches!(state, Lex::Code | Lex::LineComment) && last_semicolon
}

/// True when `sql` holds anything besides whitespace and comments.
fn has_sql_content(sql: &str) -> bool {
    scan_sql(sql).2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_small_result_exactly() {
        let names = vec!["id".to_string(), "name".to_string()];
        let types = vec![DataType::Integer, DataType::Varchar];
        let rows = vec![
            vec![Value::Integer(1), Value::Varchar("alice".to_string())],
            vec![Value::Integer(2), Value::Varchar("bob".to_string())],
        ];
        let expected = concat!(
            "┌─────────┬─────────┐\n",
            "│ id      │ name    │\n",
            "│ INTEGER │ VARCHAR │\n",
            "├─────────┼─────────┤\n",
            "│ 1       │ alice   │\n",
            "│ 2       │ bob     │\n",
            "└─────────┴─────────┘\n",
            "2 rows",
        );
        assert_eq!(format_select(&names, &types, &rows), expected);
    }

    #[test]
    fn caps_formatting_but_reports_true_total() {
        let names = vec!["v".to_string()];
        let types = vec![DataType::Varchar];
        let total = DISPLAY_CAP + 5;
        // A uniquely wide cell lives only beyond the display cap; if it were
        // formatted, it would widen the column and leak into the output.
        let rows: Vec<Vec<Value>> = (0..total)
            .map(|i| {
                if i == total - 1 {
                    vec![Value::Varchar("w".repeat(60))]
                } else {
                    vec![Value::Varchar(format!("r{i}"))]
                }
            })
            .collect();

        let out = format_select(&names, &types, &rows);

        assert!(
            out.contains("(5 more rows not shown)"),
            "missing truncation note:\n{out}"
        );
        assert!(
            out.contains(&format!("{total} rows")),
            "summary must report the true total:\n{out}"
        );
        // Widths come from the shown rows only, so the border stays narrow
        // (VARCHAR is 7 wide) instead of 60 wide.
        assert_eq!(out.lines().next().unwrap(), "┌─────────┐");
        assert!(
            !out.contains(&"w".repeat(60)),
            "beyond-cap cell leaked into output"
        );
    }

    #[test]
    fn single_row_summary_is_singular() {
        let names = vec!["n".to_string()];
        let types = vec![DataType::Bigint];
        let rows = vec![vec![Value::Bigint(42)]];

        let out = format_select(&names, &types, &rows);

        assert!(
            out.ends_with("\n1 row"),
            "expected singular summary, got:\n{out}"
        );
    }
}
