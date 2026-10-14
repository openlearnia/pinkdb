//! pinkdb REPL — thin consumer of the `pinkdb` library.
//!
//! Line editing via rustyline, multi-line input until `;`, dot-commands,
//! and DuckDB-style box-drawn result tables with a type row.

use std::time::Instant;

use pinkdb::{DataType, EncryptionKey, PinkDb, QueryResult, SyncPolicy, Value};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

#[cfg(feature = "client")]
use pinkdb_server::Client;

#[cfg(feature = "repo")]
mod repo;

#[cfg(feature = "lake")]
mod lake;

const PROMPT: &str = "pinkdb> ";
const CONT_PROMPT: &str = "   ...> ";
/// Rows shown before truncating with a "(N more rows)" note.
const DISPLAY_CAP: usize = 10_000;

fn print_help() {
    println!("Dot-commands:");
    println!("  .help                  show this help");
    println!("  .quit | .exit          leave the shell");
    println!("  .databases             list databases (the current one is marked)");
    println!("  .tables                list tables and views of the current database");
    println!("  .schema <table|view>   show a schema (accepts db.table; quoting honored)");
    println!("  .import <file.csv> [table]  import CSV (table defaults to file stem;");
    println!("                         db.table targets another database)");
    println!("  .save <file.pink>      checkpoint the bound file; snapshot elsewhere");
    println!("  .open <file.pink>      open durable: loads the file, replays <file.pink.wal>");
    if cfg!(feature = "lake") {
        if cfg!(feature = "s3") {
            println!("  .lake save <dir|s3://bucket/prefix>   snapshot the store into a lake");
            println!("  .lake open <dir|s3://…> [v]  load the lake's latest (or version v) into memory");
        } else {
            println!("  .lake save <dir>       snapshot the store into a lake directory");
            println!("  .lake open <dir> [v]   load the lake's latest (or version v) into memory");
        }
        println!("  .lake versions <dir|s3://…>  list the lake's published versions");
        println!("  .lake tables <dir|s3://…>    list the current version's tables and row counts");
        println!("  .lake info <dir|s3://…>      show the lake's format version, versions, and tables");
        if !cfg!(feature = "s3") {
            println!("  .lake s3:// URLs need a build with the `s3` feature");
        }
    } else {
        println!("  .lake <subcommand>     (not built; needs the `lake` feature)");
    }
    if cfg!(feature = "repo") {
        println!("  .install <name[@version]>   install from a repository, then load it");
        println!("  .repositories [list | add <url> | remove <url>]  manage repositories");
        println!("  .installed             list cached extensions under $PINKDB_HOME");
    } else {
        println!("  .install/.repositories/.installed  (not built; needs the `repo` feature)");
    }
    println!("Encryption: PINKDB_KEY=<passphrase> (valid UTF-8) makes .save write and .open");
    println!("            read an encrypted database (needs a build with the `encryption` feature).");
    println!("SQL: end statements with `;`. `USE <db>` switches the session database;");
    println!("     `CREATE DATABASE <db>` / `DROP DATABASE <db>` manage databases;");
    println!("     `LOAD EXTENSION '<path>'` loads a native extension (ABI v1).");
}

/// The `.install`/`.repositories`/`.installed` message when the binary was
/// built without the opt-in `repo` feature.
#[cfg(not(feature = "repo"))]
fn repo_unavailable(cmd: &str) {
    println!(
        "Error: {cmd} is not available: this pinkdb binary was built without the repo feature;"
    );
    println!("       rebuild with `cargo build -p pinkdb-cli --features repo`.");
}

/// The `.lake` message when the binary was built without the opt-in `lake`
/// feature.
#[cfg(not(feature = "lake"))]
fn lake_unavailable(cmd: &str) {
    println!(
        "Error: {cmd} is not available: this pinkdb binary was built without the lake feature;"
    );
    println!(
        "       rebuild with `cargo build -p pinkdb-cli --features lake` (add `,s3` for \
         s3://bucket/prefix URLs)."
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
            println!("Error: {e}");
            return;
        }
    };
    let repositories = match repo::load_repositories() {
        Ok(repositories) => repositories,
        Err(e) => {
            println!("Error: {e}");
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
                Err(e) => println!("Error: {e}"),
            }
        }
        Err(e) => println!("Error: {e}"),
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
            Err(e) => println!("Error: {e}"),
        },
        "add" | "remove" => {
            if rest.is_empty() || rest.split_whitespace().count() != 1 {
                println!("Usage: .repositories {sub} <url>");
                return;
            }
            let mut urls = match repo::load_repositories() {
                Ok(urls) => urls,
                Err(e) => {
                    println!("Error: {e}");
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
                    println!("Error: {e}");
                    return;
                }
            };
            if let Err(e) = repo::save_repositories(&urls) {
                println!("Error: {e}");
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
            println!("Error: {e}");
            return;
        }
    };
    match repo::list_installed(&home) {
        Ok(files) if files.is_empty() => println!("(no extensions installed)"),
        Ok(files) => {
            println!("Installed extensions ({}):", repo::extensions_dir(&home).display());
            for f in files {
                println!("  {} {} {} {}", f.name, f.version, f.platform, f.path.display());
            }
        }
        Err(e) => println!("Error: {e}"),
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
    if tables.is_empty() && views.is_empty() {
        println!("(no tables)");
        return;
    }
    for t in tables {
        let n = db.table(&t).map(|x| x.num_rows()).unwrap_or(0);
        println!("{t} ({n} rows)");
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
            println!("Error: {e}");
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
                                println!("Error: view \"{view_name}\" did not produce a schema")
                            }
                        },
                        Err(e) => println!("Error: {e}"),
                    }
                }
                Err(_) => println!("Error: {table_err}; no such view either"),
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
        Err(e) => println!("Error: {e}"),
    }
}

/// One REPL session: in-process, or (with the `client` feature) remote.
enum Session {
    Local(PinkDb),
    #[cfg(feature = "client")]
    Remote(Client),
}

/// Returns false when the REPL should exit.
fn run_dot(session: &mut Session, line: &str) -> bool {
    match session {
        Session::Local(db) => run_dot_local(db, line),
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
            let path = rest.split_whitespace().next().unwrap_or("");
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
                let result = if db.is_bound_to(path) {
                    db.checkpoint()
                } else {
                    db.save_snapshot(path)
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
                        println!("Saved in {:.3}s", t0.elapsed().as_secs_f64())
                    }
                    Err(e) => println!("Error: {e}"),
                }
            }
        }
        ".open" => {
            let path = rest.split_whitespace().next().unwrap_or("");
            if path.is_empty() {
                println!("Usage: .open <file.pink>");
            } else {
                // `PINKDB_KEY` (if set) is already installed on the store, so
                // both the checkpoint and the snapshot path encrypt.
                let result = match db.encryption_key() {
                    Some(k) => {
                        PinkDb::open_durable_with_key_and_policy(path, SyncPolicy::Sync, k)
                    }
                    None => PinkDb::open_durable_with(path, SyncPolicy::Sync),
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
                    Err(e) => println!("Error: {e}"),
                }
            }
        }
        ".lake" => {
            #[cfg(feature = "lake")]
            lake::cmd_lake(db, rest);
            #[cfg(not(feature = "lake"))]
            lake_unavailable(".lake");
        }
        _ => println!("Unknown dot-command \"{cmd}\". Try .help"),
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
            println!("Error: {cmd} is not available in remote mode");
        }
        _ => println!("Unknown dot-command \"{cmd}\". Try .help"),
    }
    true
}

#[cfg(feature = "client")]
fn print_help_remote() {
    println!("Dot-commands (remote mode):");
    println!("  .help                  show this help");
    println!("  .quit | .exit          leave the shell");
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
        _ => v.to_string(),
    }
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
        Session::Local(db) => match db.execute(sql) {
            Ok(results) => render_results(&results),
            Err(e) => println!("Error: {e}"),
        },
        #[cfg(feature = "client")]
        Session::Remote(client) => match client.execute(sql) {
            Ok(results) => render_results(&results),
            Err(e) => println!("Error: {e}"),
        },
    }
    println!("Run Time (s): real {:.3}", t0.elapsed().as_secs_f64());
}

/// Parsed command line.
struct Opts {
    host: Option<String>,
}

fn print_usage() {
    println!("Usage: pinkdb [--host HOST:PORT]");
    println!();
    println!("  --host HOST:PORT   run as a client of a pinkdb-server (remote mode)");
    if cfg!(feature = "client") {
        println!("                     [client feature: enabled]");
    } else {
        println!("                     [client feature: disabled in this build]");
    }
}

fn parse_args() -> Result<Opts, String> {
    let mut host = None;
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
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    Ok(Opts { host })
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
            Session::Local(db)
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

fn main() {
    let host = match parse_args() {
        Ok(opts) => opts.host,
        Err(msg) => {
            eprintln!("Error: {msg}");
            print_usage();
            std::process::exit(2);
        }
    };
    let mut session = build_session(host.clone(), match EncryptionKey::from_env() {
        Ok(key) => key,
        Err(e) => {
            // A set-but-non-UTF-8 PINKDB_KEY must not silently fall back to
            // plaintext.
            eprintln!("Error: {e}");
            std::process::exit(2);
        }
    });
    let mut rl = DefaultEditor::new().unwrap_or_else(|e| {
        eprintln!("rustyline init failed ({e}); falling back to stdin");
        DefaultEditor::new().expect("rustyline fallback")
    });
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

    let mut pending = String::new();
    loop {
        let prompt = if pending.is_empty() {
            PROMPT
        } else {
            CONT_PROMPT
        };
        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                // Dot-commands only at the start of a fresh statement.
                if pending.is_empty() && trimmed.starts_with('.') {
                    rl.add_history_entry(line.as_str()).ok();
                    if !run_dot(&mut session, trimmed) {
                        break;
                    }
                    continue;
                }
                pending.push_str(&line);
                pending.push('\n');
                if trimmed.ends_with(';') {
                    rl.add_history_entry(pending.as_str()).ok();
                    let sql = pending.trim().to_string();
                    pending.clear();
                    run_sql_session(&mut session, &sql);
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C: drop pending input, stay in shell.
                pending.clear();
                println!("(^C — input cleared)");
            }
            Err(ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("Readline error: {e}");
                break;
            }
        }
    }
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
