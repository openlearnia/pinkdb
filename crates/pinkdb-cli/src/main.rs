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
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Set by [`cli_error!`]; decides the exit status of a non-interactive run.
static HAD_ERROR: AtomicBool = AtomicBool::new(false);

/// `.timer on|off` (default: on for an interactive terminal, else off).
static TIMER: AtomicBool = AtomicBool::new(false);

/// Whether the SQL `LOAD EXTENSION` statement may run in this session
/// (`--allow-load-extension`, or `PINKDB_ALLOW_LOAD_EXTENSION=1`).
///
/// **Off by default**: the statement makes the process load and run a native
/// library from a path the SQL text names.
static ALLOW_LOAD_EXTENSION: AtomicBool = AtomicBool::new(false);

/// The directory every extension load is confined to (`--extension-root`).
static EXTENSION_ROOT: Mutex<Option<std::path::PathBuf>> = Mutex::new(None);

/// Report a failed statement or command: `Error: …` on stderr, and remember
/// it for the exit status.
macro_rules! cli_error {
    ($($arg:tt)*) => {{
        $crate::HAD_ERROR.store(true, ::std::sync::atomic::Ordering::Relaxed);
        let message = format!($($arg)*);
        eprintln!("Error: {}", pinkdb::sanitize_terminal(&message));
    }};
}

use pinkdb::{
    DataType, DurableOpenOptions, EncryptionKey, PinkDb, QueryProfile, QueryResult, Value,
};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

#[cfg(feature = "client")]
use pinkdb_server::Client;
#[cfg(feature = "tls")]
use pinkdb_server::ClientTrust;

#[cfg(feature = "repo")]
mod repo;

#[cfg(any(feature = "pinklake", feature = "vortexlake"))]
mod lake;

const PROMPT: &str = "pinkdb> ";
const CONT_PROMPT: &str = "   ...> ";
/// Rows shown before truncating with a "(N more rows)" note.
const DISPLAY_CAP: usize = 10_000;

/// Publish the linked extensions the CLI ships in the process-global
/// registry — the host-bootstrap step `LOAD EXTENSION '<name>'` resolves
/// against, on every target. With the default features those are
/// `pinkdb-json`, `pinkdb-datetime` and `pinkdb-math`; the opt-in `arrow`
/// feature additionally registers the Arrow IPC **file format**
/// process-wide, so every store created afterwards (including a durable
/// reopen that replays a file-reading record) resolves `FROM 'x.arrow'`,
/// `read_arrow` and `COPY … (FORMAT arrow)`. A host that uses the library
/// instead does the same explicitly (`pinkdb_json::ensure_registered()`,
/// `pinkdb_datetime::ensure_registered()`, `pinkdb_math::ensure_registered()`,
/// `pinkdb_arrow::ensure_registered()`).
fn register_builtin_extensions() {
    #[cfg(feature = "json")]
    if let Err(e) = pinkdb_json::ensure_registered() {
        eprintln!("warning: the json extension could not be registered: {e}");
    }
    #[cfg(feature = "datetime")]
    if let Err(e) = pinkdb_datetime::ensure_registered() {
        eprintln!("warning: the datetime extension could not be registered: {e}");
    }
    #[cfg(feature = "math")]
    if let Err(e) = pinkdb_math::ensure_registered() {
        eprintln!("warning: the math extension could not be registered: {e}");
    }
    // The arrow and parquet extensions are engine-adjacent, not scalar
    // extensions: they register their file surfaces process-wide, before any
    // store is created, so a durable `.open` can resolve file sources while
    // it replays (a replay re-reads the file, DuckDB's rule).
    #[cfg(feature = "arrow")]
    pinkdb_arrow::ensure_registered();
    #[cfg(feature = "parquet")]
    pinkdb_parquet::ensure_registered();
    // The lake's Parquet data-file format provider (`parquetlake`): registered
    // process-wide before any handle exists, so `.lake format parquet` and
    // the save/commit paths resolve the id.
    #[cfg(feature = "parquetlake")]
    if let Err(e) = pink_lake_parquet::register() {
        eprintln!("warning: the parquet lake format could not be registered: {e}");
    }
    // The httpfs transport + `read_csv` / `.import <url>` source, registered
    // process-wide for the same replay reason.
    #[cfg(feature = "httpfs")]
    pinkdb_httpfs::ensure_registered();
}

/// Load the CLI's built-in extensions into a store it just created or
/// opened, so `json_*`, the datetime functions and `exp(...)`/`random()` work
/// without a `LOAD EXTENSION` statement (the DuckDB behavior). A store whose
/// WAL replay already loaded one — a replayed record that calls a function is
/// prefixed with `LOAD EXTENSION '<name>'` — keeps it:
/// [`PinkDb::is_extension_loaded`] is consulted first, because a second load
/// is a documented error.
///
/// The built-in **file sources** are registered process-wide at startup
/// (`register_builtin_extensions`), before any store exists, so every store
/// this session opens — including a durable `.open` that replays a
/// file-reading record — starts with them.
fn load_builtin_extensions(#[allow(unused_variables)] db: &mut PinkDb) {
    #[cfg(feature = "json")]
    load_builtin_extension(db, pinkdb_json::EXTENSION_NAME);
    #[cfg(feature = "datetime")]
    load_builtin_extension(db, pinkdb_datetime::EXTENSION_NAME);
    #[cfg(feature = "math")]
    load_builtin_extension(db, pinkdb_math::EXTENSION_NAME);
}

/// Load one built-in extension by its registry name (shared by the feature
/// blocks above; absent when no core-extension feature is built).
#[cfg(any(feature = "json", feature = "datetime", feature = "math"))]
fn load_builtin_extension(db: &mut PinkDb, name: &str) {
    if db.is_extension_loaded(name) {
        return;
    }
    if let Err(e) = db.load_linked_extension(name) {
        eprintln!("warning: the {name} extension could not be loaded: {e}");
    }
}

/// The `.parquet` message when the binary was built without the feature.
#[cfg(not(feature = "parquet"))]
fn parquet_unavailable(cmd: &str) {
    cli_error!("{cmd} is not available: this pinkdb binary was built without the parquet feature;");
    eprintln!("       rebuild with `cargo build -p pinkdb-cli --features parquet`.");
}

/// `.parquet [reset]`: the parquet reader's pushdown counters — files read,
/// row groups visited and pruned by statistics, columns decoded, rows
/// returned. `reset` zeroes them.
#[cfg(feature = "parquet")]
fn cmd_parquet(db: &PinkDb, arg: &str) {
    match arg {
        "" | "stats" => {
            let stats = db.table_source_stats();
            if stats.is_empty() {
                println!("(no table sources registered)");
                return;
            }
            for s in stats {
                println!("{}", s.name);
                println!(
                    "  scans: {}, rows: {}, files: {}, row groups: {} ({} pruned by statistics), columns decoded: {}",
                    s.scans, s.rows, s.files, s.row_groups, s.row_groups_pruned, s.columns_decoded
                );
            }
            println!(
                "  read_parquet('…') / parquet_scan('…') / FROM 'file.parquet'; projections and \
                 WHERE conjuncts are pushed into the scan"
            );
        }
        "reset" => {
            for source in db.table_sources() {
                source.reset_stats();
            }
            println!("parquet counters reset");
        }
        _ => println!("Usage: .parquet [stats|reset]"),
    }
}

fn print_help() {
    println!("Dot-commands:");
    println!("  .help                  show this help");
    println!("  .quit | .exit          leave the shell");
    println!("  .read <file.sql>       run SQL and dot-commands from a file");
    println!("  .timer on|off          print each statement's run time with a phase breakdown");
    println!("  .databases             list databases (the current one is marked)");
    println!("  .tables                list tables and views of the current database");
    println!("  .schema <table|view>   show a schema (accepts db.table; quoting honored)");
    if cfg!(feature = "httpfs") {
        println!("  .import <file.csv|https://…|s3://…> [table]");
        println!("                         import CSV from a local file or a remote object");
    } else {
        println!("  .import <file.csv> [table]  import CSV (table defaults to file stem;");
        println!("                         db.table targets another database)");
    }
    if cfg!(feature = "parquet") {
        println!("  .parquet [stats|reset] parquet reader counters (pushdown evidence)");
    }
    println!("  .save <file.pink>      checkpoint the bound file; snapshot elsewhere");
    println!("  .open <file.pink>      open durable: loads the file, replays <file.pink.wal>");
    if cfg!(any(feature = "pinklake", feature = "vortexlake")) {
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
        println!("  .lake inlining {LAKE} <t> <limit|default>");
        println!("                                set/clear the per-table data_inlining_row_limit");
        println!(
            "                                (0 disables; commits at most `limit` rows inline)"
        );
        println!("  .lake flush {LAKE} [t]");
        println!("                                materialize inlined rows into data files");
        println!("  .lake changes {LAKE} [t] [from] [to]");
        println!(
            "                                row-level changes of a version range (inclusive)"
        );
        println!("  .lake add_data_files {LAKE} <t> <path> [path ...]");
        println!("                                register existing data/ files as table rows");
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
        println!("  .lake format [<id>]         show or choose the data-file format for");
        println!("                                subsequent .lake save / .lake commit writes");
        println!("  .lake replicate create {LAKE} from <dir|s3://...|store:<db>> [every s] [...]");
        println!("                                table-level replication jobs (wave 89)");
        println!(
            "  .lake replicate catchup|start|stop|pause|resume|resume-all|status|tables|drop ..."
        );
        println!(
            "                                run/inspect jobs; start keeps a session supervisor"
        );
        println!(
            "  FROM t AT (VERSION => n)     query an attached lake table at a version \
             (or AT (TIMESTAMP => TIMESTAMP '…'))"
        );
        if !cfg!(feature = "s3") {
            println!("  .lake s3:// URLs need a build with the `s3` feature");
        }
        if !cfg!(feature = "vortexlake") {
            println!(
                "  (pinklake build: `.pink` data files, format_version 4); Vortex needs `vortexlake`"
            );
        }
    } else {
        println!(
            "  .lake <subcommand>     (not built; needs the `pinklake` or `vortexlake` feature)"
        );
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
    if cfg!(feature = "json") {
        println!("JSON: json_extract, json_value, json_valid, json_array_length, json_object,");
        println!("      json_array are built in and loaded into every store (`json` feature); a");
        println!("      `LOAD EXTENSION 'json'` would be a duplicate load.");
    } else {
        println!("JSON: json_* functions are not built (rebuild with the `json` feature, or link");
        println!("      `pinkdb-json` and register it).");
    }
    if cfg!(feature = "datetime") {
        println!("DATETIME: strftime, strptime, epoch, epoch_ms, dayofweek, dayofyear, week,");
        println!("      last_day, age, make_date are built in and loaded into every store");
        println!("      (`datetime` feature); DATE arguments take `::TIMESTAMP`.");
    } else {
        println!("DATETIME: the datetime functions are not built (rebuild with the `datetime`");
        println!("      feature, or link `pinkdb-datetime` and register it).");
    }
    if cfg!(feature = "math") {
        println!("MATH: exp, ln, log, log2, log10, sin, cos, tan, cot, asin, acos, atan, atan2,");
        println!("      degrees, radians, pi and the volatile random() are built in and loaded");
        println!("      into every store (`math` feature); sqrt, pow, floor, ceil, sign, greatest");
        println!(
            "      and least are core functions. `LOAD EXTENSION 'math'` would be a duplicate."
        );
    } else {
        println!(
            "MATH: the math functions (exp, log, sin, ..., random) are not built (rebuild with"
        );
        println!("      the `math` feature, or link `pinkdb-math` and register it).");
    }
    if cfg!(feature = "arrow") {
        println!("ARROW: `FROM 'f.arrow'` / `read_arrow('f.arrow')` read an Arrow IPC file and");
        println!("      `COPY t TO 'f.arrow' (FORMAT arrow)` writes one (`arrow` feature; the");
        println!("      format is registered for every store this build opens).");
    } else {
        println!("ARROW: the arrow file format is not built (rebuild with the `arrow` feature, or");
        println!("      link `pinkdb-arrow` and register the format on the store).");
    }
}

/// The `.install`/`.repositories`/`.installed` message when the binary was
/// built without the opt-in `repo` feature.
#[cfg(not(feature = "repo"))]
fn repo_unavailable(cmd: &str) {
    cli_error!("{cmd} is not available: this pinkdb binary was built without the repo feature;");
    eprintln!("       rebuild with `cargo build -p pinkdb-cli --features repo`.");
}

/// The `.lake` message when the binary was built without either lake feature.
#[cfg(not(any(feature = "pinklake", feature = "vortexlake")))]
fn lake_unavailable(cmd: &str) {
    cli_error!("{cmd} is not available: this pinkdb binary was built without a lake feature;");
    eprintln!(
        "       rebuild with `cargo build -p pinkdb-cli --features vortexlake` (Vortex data \
         files, incremental writes, attach) or `--features pinklake` (lean: `.pink` data \
         files); combine either with `,s3` (`vortexlake,s3` / `pinklake,s3`) for s3:// \
         locations."
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
            match &installed.trust {
                repo::Trust::Signed { key_id } => {
                    println!("  signature:  verified (key {key_id})");
                }
                repo::Trust::UnsignedDev => {
                    println!(
                        "  signature:  NOT VERIFIED (PINKDB_EXT_ALLOW_UNSIGNED=1 is set — INSECURE)"
                    );
                }
            }
            if !installed.downloaded {
                println!("  cached:     yes");
            }
            // Copy the verified bytes into a fresh 0700 directory with
            // `create_new`, re-hash the copy, and only then load it. The
            // staging directory is removed when `staged` drops; the loaded
            // library stays mapped.
            let staged = match repo::stage_for_loading(&installed.entry, &installed.path) {
                Ok(staged) => staged,
                Err(e) => {
                    cli_error!("{e}");
                    return;
                }
            };
            match db.load_extension(&staged.path().to_string_lossy()) {
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
                pinkdb::sanitize_terminal(&repo::extensions_dir(&home).display().to_string())
            );
            for f in files {
                println!(
                    "  {} {} {} {}",
                    pinkdb::sanitize_terminal(&f.name),
                    pinkdb::sanitize_terminal(&f.version),
                    pinkdb::sanitize_terminal(&f.platform),
                    pinkdb::sanitize_terminal(&f.path.display().to_string())
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
        println!("{}{marker} {label}", pinkdb::sanitize_terminal(&name));
    }
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
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
        println!("{} ({n} rows)", pinkdb::sanitize_terminal(&t));
    }
    for t in attached {
        println!(
            "{} ({} rows, attached)",
            pinkdb::sanitize_terminal(&t.table),
            t.rows
        );
    }
    for v in views {
        println!("{} (view)", pinkdb::sanitize_terminal(&v));
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
            println!("CREATE TABLE {} (", pinkdb::sanitize_terminal(&t.name));
            let schema = t.schema();
            for (i, (col, dtype)) in schema.iter().enumerate() {
                let comma = if i + 1 == schema.len() { "" } else { "," };
                println!("  {} {dtype}{comma}", pinkdb::sanitize_terminal(col));
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
                println!("CREATE TABLE {} (", pinkdb::sanitize_terminal(&att.table));
                for (i, (col, dtype)) in att.schema.iter().enumerate() {
                    let comma = if i + 1 == att.schema.len() { "" } else { "," };
                    println!("  {} {dtype}{comma}", pinkdb::sanitize_terminal(col));
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
                                println!("CREATE VIEW {} (", pinkdb::sanitize_terminal(&view_name));
                                for (i, (n, t)) in names.iter().zip(types).enumerate() {
                                    let comma = if i + 1 == names.len() { "" } else { "," };
                                    println!("  {} {t}{comma}", pinkdb::sanitize_terminal(n));
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
        println!("Usage: .import <file.csv|https://…|s3://…> [table]");
        return;
    };
    let file = unquote(&file);
    let table = match take_token(rest) {
        Some((t, tail)) if tail.trim().is_empty() => Some(t),
        Some(_) => {
            println!("Usage: .import <file.csv|https://…|s3://…> [table]");
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

    /// Execute one SQL string without profiling: the historical zero-cost
    /// path.
    fn run_sql(&mut self, sql: &str) -> Result<Vec<QueryResult>, pinkdb::PinkDbError> {
        self.run_sql_with(sql, false).map(|(results, _)| results)
    }

    /// Execute one SQL string with the profiling runner (wave 84), returning
    /// the results and one profile per statement.
    fn run_sql_profiled(
        &mut self,
        sql: &str,
    ) -> Result<(Vec<QueryResult>, Vec<QueryProfile>), pinkdb::PinkDbError> {
        self.run_sql_with(sql, true)
    }

    /// Execute one SQL string, maintaining the transaction buffer and
    /// retrying a conflicting COMMIT (bounded). When a retry succeeds only
    /// the transaction's outcome is reported (the earlier attempt's
    /// statement results were already rendered and its effects discarded by
    /// the abort).
    fn run_sql_with(
        &mut self,
        sql: &str,
        profiled: bool,
    ) -> Result<(Vec<QueryResult>, Vec<QueryProfile>), pinkdb::PinkDbError> {
        let was = self.db.in_transaction();
        let pre_db = self.db.current_db().to_string();
        let executed = match profiled {
            true => self
                .db
                .execute_profiled(sql)
                .map(|pairs| pairs.into_iter().unzip()),
            false => self.db.execute(sql).map(|results| (results, Vec::new())),
        };
        match executed {
            Ok((results, profiles)) => {
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
                Ok((results, profiles))
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
                        let retried = match profiled {
                            true => self
                                .db
                                .execute_profiled(&retry_sql)
                                .map(|pairs| pairs.into_iter().unzip()),
                            false => self
                                .db
                                .execute(&retry_sql)
                                .map(|results| (results, Vec::new())),
                        };
                        match retried {
                            Ok((mut results, mut profiles)) => {
                                self.txn_pre_db = None;
                                // Only the transaction's outcome: the
                                // statement results of the first attempt
                                // were already printed.
                                let profile = profiles.pop();
                                return Ok((
                                    vec![results.pop().unwrap_or(QueryResult::Committed)],
                                    profile.into_iter().collect(),
                                ));
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
                "on" => {
                    TIMER.store(true, Ordering::Relaxed);
                    #[cfg(feature = "client")]
                    if let Session::Remote(client) = session
                        && let Err(e) = client.set_profiling(true)
                    {
                        cli_error!("{e}");
                    }
                }
                "off" => {
                    TIMER.store(false, Ordering::Relaxed);
                    #[cfg(feature = "client")]
                    if let Session::Remote(client) = session
                        && let Err(e) = client.set_profiling(false)
                    {
                        cli_error!("{e}");
                    }
                }
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
        ".parquet" => {
            #[cfg(feature = "parquet")]
            cmd_parquet(db, rest);
            #[cfg(not(feature = "parquet"))]
            parquet_unavailable(".parquet");
        }
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
                    apply_extension_policy(db);
                    load_builtin_extensions(db);
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
                        apply_extension_policy(db);
                        load_builtin_extensions(db);
                        println!("Opened {path}");
                    }
                    Err(e) => cli_error!("{e}"),
                }
            }
        }
        ".lake" => {
            #[cfg(any(feature = "pinklake", feature = "vortexlake"))]
            lake::cmd_lake(db, rest);
            #[cfg(not(any(feature = "pinklake", feature = "vortexlake")))]
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
    println!("  .timer on|off          print each statement's run time with a phase breakdown");
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
        Value::Varchar(s) => pinkdb::sanitize_terminal(s),
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
    // Column names are untrusted too (a quoted identifier may contain ESC, and
    // a hostile server may send arbitrary bytes): sanitize before measuring,
    // so the width and the printed text agree.
    let names: Vec<String> = names.iter().map(|n| pinkdb::sanitize_terminal(n)).collect();
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
    row_out(&mut out, &names);
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
    print!("{}", render_results_text(results));
}

/// The renderer's output as a string (the unit tests assert on it directly).
fn render_results_text(results: &[QueryResult]) -> String {
    let mut out = String::new();
    render_results_into(&mut out, results);
    out
}

fn render_results_into(out: &mut String, results: &[QueryResult]) {
    use std::fmt::Write as _;
    // Every object name in a `QueryResult` originates in SQL (a quoted
    // identifier may contain ESC) or on the wire (a hostile server): render
    // each through the shared sanitizer.
    let clean = pinkdb::sanitize_terminal;
    for r in results {
        match r {
            QueryResult::Created => {
                let _ = writeln!(out, "Table created.");
            }
            QueryResult::Inserted { count } => {
                let _ = writeln!(out, "Inserted {count} row(s).");
            }
            QueryResult::Updated { count } => {
                let _ = writeln!(out, "Updated {count} row(s).");
            }
            QueryResult::Deleted { count } => {
                let _ = writeln!(out, "Deleted {count} row(s).");
            }
            QueryResult::Began => {
                let _ = writeln!(out, "Transaction started.");
            }
            QueryResult::Committed => {
                let _ = writeln!(out, "Transaction committed.");
            }
            QueryResult::RolledBack => {
                let _ = writeln!(out, "Transaction rolled back.");
            }
            QueryResult::CreatedView { name } => {
                let _ = writeln!(out, "View {} created.", clean(name));
            }
            QueryResult::DroppedTable { name } => {
                let _ = writeln!(out, "Table {} dropped.", clean(name));
            }
            QueryResult::CreatedIndex { name } => {
                let _ = writeln!(out, "Index {} created.", clean(name));
            }
            QueryResult::DroppedIndex { name } => {
                let _ = writeln!(out, "Index {} dropped.", clean(name));
            }
            QueryResult::CreatedMaterializedView { name } => {
                let _ = writeln!(out, "Materialized view {} created.", clean(name));
            }
            QueryResult::RefreshedMaterializedView { name } => {
                let _ = writeln!(out, "Materialized view {} refreshed.", clean(name));
            }
            QueryResult::RefreshedMaterializedViews { names } => {
                let _ = writeln!(out, "Refreshed {} materialized view(s).", names.len());
            }
            QueryResult::DroppedMaterializedView { name } => {
                let _ = writeln!(out, "Materialized view {} dropped.", clean(name));
            }
            QueryResult::DroppedView { name } => {
                let _ = writeln!(out, "View {} dropped.", clean(name));
            }
            QueryResult::CreatedDatabase { name } => {
                let _ = writeln!(out, "Database {} created.", clean(name));
            }
            QueryResult::DroppedDatabase { name } => {
                let _ = writeln!(out, "Database {} dropped.", clean(name));
            }
            QueryResult::Used { db } => {
                let _ = writeln!(out, "Using database {}.", clean(db));
            }
            QueryResult::LoadedExtension { name } => {
                let _ = writeln!(out, "Extension {} loaded.", clean(name));
            }
            QueryResult::Checkpointed => {
                let _ = writeln!(out, "Checkpoint complete.");
            }
            QueryResult::AlteredTable { name } => {
                let _ = writeln!(out, "Table {} altered.", clean(name));
            }
            QueryResult::CreatedProcedure { name } => {
                let _ = writeln!(out, "Procedure {} created.", clean(name));
            }
            QueryResult::DroppedProcedure { name } => {
                let _ = writeln!(out, "Procedure {} dropped.", clean(name));
            }
            QueryResult::CreatedMacro { name } => {
                let _ = writeln!(out, "Macro {} created.", clean(name));
            }
            QueryResult::DroppedMacro { name } => {
                let _ = writeln!(out, "Macro {} dropped.", clean(name));
            }
            QueryResult::CreatedUser { name } => {
                let _ = writeln!(out, "User {} created.", clean(name));
            }
            QueryResult::AlteredUser { name } => {
                let _ = writeln!(out, "User {} altered.", clean(name));
            }
            QueryResult::DroppedUser { name } => {
                let _ = writeln!(out, "User {} dropped.", clean(name));
            }
            QueryResult::CreatedRole { name } => {
                let _ = writeln!(out, "Role {} created.", clean(name));
            }
            QueryResult::DroppedRole { name } => {
                let _ = writeln!(out, "Role {} dropped.", clean(name));
            }
            QueryResult::Granted { description } => {
                let _ = writeln!(out, "Granted {}.", clean(description));
            }
            QueryResult::Revoked { description } => {
                let _ = writeln!(out, "Revoked {}.", clean(description));
            }
            QueryResult::Called {
                name,
                statements,
                result,
                output,
            } => {
                // A JavaScript body's `print` lines come first, in order;
                // they are script-controlled text, so they are sanitized
                // like every other rendered string.
                for line in output {
                    let _ = writeln!(out, "{}", clean(line));
                }
                let _ = writeln!(
                    out,
                    "Procedure {} called ({statements} statement{}).",
                    clean(name),
                    if *statements == 1 { "" } else { "s" }
                );
                if let Some(inner) = result {
                    render_results_into(out, std::slice::from_ref(&**inner));
                }
            }
            QueryResult::Selected { names, types, rows } => {
                if rows.is_empty() {
                    let _ = writeln!(out, "(no rows)");
                }
                let table = format_select(names, types, rows);
                let _ = writeln!(out, "{table}");
            }
        }
    }
}

/// Run one SQL string in the session's mode (local or remote) and print the
/// results with the same renderer and the same timing line either way.
///
/// With the timer on, the engine (local) or the server (remote) also profiles
/// every statement: the line keeps its historical `Run Time (s): real X.XXX`
/// prefix and appends the summed phases on the same line. Remote mode reports
/// only the statements the server profiled (none when it could not be
/// enabled), in which case the line is the historical one unchanged.
fn run_sql_session(session: &mut Session, sql: &str) {
    let t0 = Instant::now();
    let timer = TIMER.load(Ordering::Relaxed);
    let mut profiles: Option<Vec<QueryProfile>> = None;
    match session {
        Session::Local(local) => {
            if timer {
                match local.run_sql_profiled(sql) {
                    Ok((results, ps)) => {
                        render_results(&results);
                        profiles = Some(ps);
                    }
                    Err(e) => cli_error!("{e}"),
                }
            } else {
                match local.run_sql(sql) {
                    Ok(results) => render_results(&results),
                    Err(e) => cli_error!("{e}"),
                }
            }
        }
        #[cfg(feature = "client")]
        Session::Remote(client) => {
            if timer {
                match client.execute_profiled(sql) {
                    Ok(pairs) => {
                        let (results, ps): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();
                        render_results(&results);
                        profiles = Some(ps.into_iter().flatten().collect());
                    }
                    Err(e) => cli_error!("{e}"),
                }
            } else {
                match client.execute(sql) {
                    Ok(results) => render_results(&results),
                    Err(e) => cli_error!("{e}"),
                }
            }
        }
    }
    if timer {
        print!("Run Time (s): real {:.3}", t0.elapsed().as_secs_f64());
        if let Some(profiles) = profiles.filter(|ps| !ps.is_empty()) {
            print_phase_breakdown(&profiles);
        }
        println!();
    }
}

/// Append ` (parse A, bind B, optimize C, execute D, log E ms)` in
/// milliseconds with three decimals: bind/optimize/execute/log summed over
/// `profiles`; `parse` counted once — the whole SQL string is parsed once and
/// every statement's profile reports that same shared time.
fn print_phase_breakdown(profiles: &[QueryProfile]) {
    let parse = profiles
        .iter()
        .map(|p| p.parse)
        .max()
        .unwrap_or(Duration::ZERO);
    let mut bind = Duration::ZERO;
    let mut optimize = Duration::ZERO;
    let mut execute = Duration::ZERO;
    let mut log = Duration::ZERO;
    for profile in profiles {
        bind += profile.bind;
        optimize += profile.optimize;
        execute += profile.execute;
        log += profile.log;
    }
    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    print!(
        " (parse {:.3}, bind {:.3}, optimize {:.3}, execute {:.3}, log {:.3} ms)",
        ms(parse),
        ms(bind),
        ms(optimize),
        ms(execute),
        ms(log)
    );
}

/// Parsed command line.
struct Opts {
    host: Option<String>,
    /// `--user NAME`: log in to the remote server as this user (wave 48).
    user: Option<String>,
    /// `--password PASS` (or `PINKDB_PASSWORD`): the login password. Visible
    /// in the process list and the shell history when passed as a flag, so
    /// the environment variable is the better habit.
    password: Option<String>,
    /// `-c` arguments, run in order instead of reading stdin.
    commands: Vec<String>,
    /// `--tls`: encrypt the remote connection (requires a trust source).
    tls: bool,
    /// `--tls-ca-cert PATH`: PEM trust anchor for the server certificate.
    tls_ca_cert: Option<String>,
    /// `--tls-insecure`: skip certificate verification (local tests only).
    tls_insecure: bool,
    /// `--allow-load-extension`: let the SQL `LOAD EXTENSION` statement run
    /// (it loads and runs native code). Also honoured through
    /// `PINKDB_ALLOW_LOAD_EXTENSION=1`.
    allow_load_extension: bool,
    /// `--extension-root PATH`: confine every extension load to this
    /// directory.
    extension_root: Option<String>,
}

fn print_usage() {
    println!("Usage: pinkdb [--host HOST:PORT] [--user NAME] [--password PASS] [-c COMMAND]...");
    println!();
    println!("  -V, --version      print the version and exit");
    println!("  -c COMMAND         run SQL or a dot-command, then exit (repeatable)");
    println!("  --host HOST:PORT   run as a client of a pinkdb-server (remote mode)");
    if cfg!(feature = "client") {
        println!("                     [client feature: enabled]");
    } else {
        println!("                     [client feature: disabled in this build]");
    }
    println!("  --user NAME        log in to the server as NAME (requires --host)");
    println!("  --password PASS    the login password; PINKDB_PASSWORD is read when the");
    println!("                     flag is absent (a flag value is visible to other local");
    println!("                     users through the process list and the shell history)");
    println!("  --allow-load-extension");
    println!("                     let SQL `LOAD EXTENSION '<path>'` run: it loads and runs");
    println!("                     native code from a path the SQL names (off by default;");
    println!("                     PINKDB_ALLOW_LOAD_EXTENSION=1 does the same)");
    println!("  --extension-root PATH");
    println!("                     confine every extension load to this directory (a path");
    println!("                     outside it, a bare library name, a `..` component and a");
    println!("                     symlinked library are always refused)");
    if cfg!(feature = "tls") {
        println!("  --tls              encrypt the remote connection (TLS)");
        println!("  --tls-ca-cert PATH PEM certificate to trust for the server (a CA, or the");
        println!("                     self-signed server certificate); implies --tls");
        println!("  --tls-insecure     DANGER: skip certificate verification; local tests only");
    } else {
        println!("  --tls …            [tls feature: disabled in this build]");
    }
}

fn parse_args() -> Result<Opts, String> {
    let mut opts = Opts {
        host: None,
        user: None,
        password: None,
        commands: Vec::new(),
        tls: false,
        tls_ca_cert: None,
        tls_insecure: false,
        allow_load_extension: false,
        extension_root: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            // Release tooling (install.sh, smoke.sh) identifies a binary
            // without running a SQL session; keep the format stable:
            // `pinkdb <semver>` on stdout.
            "-V" | "--version" => {
                println!("pinkdb {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            "--host" => {
                let value = args.next().ok_or("--host requires HOST:PORT")?;
                opts.host = Some(value);
            }
            other if other.starts_with("--host=") => {
                opts.host = Some(other["--host=".len()..].to_string());
            }
            "--user" => {
                let value = args.next().ok_or("--user requires a name")?;
                opts.user = Some(value);
            }
            other if other.starts_with("--user=") => {
                opts.user = Some(other["--user=".len()..].to_string());
            }
            "--password" => {
                let value = args.next().ok_or("--password requires a value")?;
                opts.password = Some(value);
            }
            other if other.starts_with("--password=") => {
                opts.password = Some(other["--password=".len()..].to_string());
            }
            "--tls" => opts.tls = true,
            "--tls-insecure" => opts.tls_insecure = true,
            "--allow-load-extension" => opts.allow_load_extension = true,
            "--extension-root" => {
                let value = args.next().ok_or("--extension-root requires a path")?;
                opts.extension_root = Some(value);
            }
            other if other.starts_with("--extension-root=") => {
                opts.extension_root = Some(other["--extension-root=".len()..].to_string());
            }
            "--tls-ca-cert" => {
                let value = args.next().ok_or("--tls-ca-cert requires a path")?;
                opts.tls_ca_cert = Some(value);
            }
            other if other.starts_with("--tls-ca-cert=") => {
                opts.tls_ca_cert = Some(other["--tls-ca-cert=".len()..].to_string());
            }
            "-c" => {
                let value = args.next().ok_or("-c requires a command")?;
                opts.commands.push(value);
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    validate_tls_args(&opts)?;
    validate_auth_args(&mut opts)?;
    Ok(opts)
}

/// Reject credential-flag combinations the client cannot honor, and pick up
/// `PINKDB_PASSWORD` when `--password` is absent **for a remote session**
/// (wave 48). Credentials are a remote-mode feature: without `--host` there
/// is nothing to log in to, and a machine that exports the variable for an
/// unrelated purpose must still be able to start the local REPL.
fn validate_auth_args(opts: &mut Opts) -> Result<(), String> {
    if opts.host.is_some()
        && opts.password.is_none()
        && let Ok(value) = std::env::var("PINKDB_PASSWORD")
        && !value.is_empty()
    {
        opts.password = Some(value);
    }
    let requested = opts.user.is_some() || opts.password.is_some();
    if !requested {
        return Ok(());
    }
    if !cfg!(feature = "client") {
        return Err(
            "credentials were given, but this pinkdb binary was built without the client \
             feature;\n       rebuild with `cargo build -p pinkdb-cli --features client`."
                .to_string(),
        );
    }
    if opts.host.is_none() {
        return Err(
            "--user/--password require --host (local sessions are already trusted; \
                    a local store's users only gate network logins)"
                .to_string(),
        );
    }
    if opts.user.is_none() {
        return Err("--password requires --user".to_string());
    }
    if opts.password.is_none() {
        return Err(
            "--user needs a password: pass --password PASS or set PINKDB_PASSWORD".to_string(),
        );
    }
    Ok(())
}

/// Reject TLS flag combinations the client cannot honor, with a precise
/// message naming what is missing. `--tls-ca-cert` and `--tls-insecure` imply
/// `--tls`; TLS itself needs `--host` (there is nothing to encrypt locally).
fn validate_tls_args(opts: &Opts) -> Result<(), String> {
    let requested = opts.tls || opts.tls_ca_cert.is_some() || opts.tls_insecure;
    if !requested {
        return Ok(());
    }
    // The build's capabilities come first: a binary without the feature
    // cannot honor any TLS request, whatever the flags say.
    if !cfg!(feature = "tls") {
        return Err(
            "TLS was requested, but this pinkdb binary was built without the tls feature;\n       \
             rebuild with `cargo build -p pinkdb-cli --features client,tls`."
                .to_string(),
        );
    }
    if opts.host.is_none() {
        return Err(
            "TLS flags (--tls / --tls-ca-cert / --tls-insecure) require --host".to_string(),
        );
    }
    if opts.tls_ca_cert.is_some() && opts.tls_insecure {
        return Err(
            "--tls-ca-cert and --tls-insecure are mutually exclusive: pick one trust source"
                .to_string(),
        );
    }
    if opts.tls_ca_cert.is_none() && !opts.tls_insecure {
        return Err(
            "--tls needs a trust source: pass --tls-ca-cert PATH (PEM), or --tls-insecure \
             (local testing only)"
                .to_string(),
        );
    }
    Ok(())
}

/// The name verified against the server certificate: the host part of
/// `--host HOST:PORT`, with `[::1]` brackets stripped.
#[cfg(feature = "tls")]
fn tls_server_name(host: &str) -> String {
    if let Some(rest) = host.strip_prefix('[')
        && let Some((inside, _)) = rest.split_once(']')
    {
        return inside.to_string();
    }
    if host.parse::<std::net::IpAddr>().is_ok() {
        return host.to_string();
    }
    match host.rsplit_once(':') {
        Some((name, _)) if !name.is_empty() && !name.contains(':') => name.to_string(),
        _ => host.to_string(),
    }
}

/// Build the REPL session: local by default, remote when `--host` is given
/// (the `client` feature; without it, `--host` is rejected). Over TLS when
/// the `tls` feature is built and the TLS flags ask for it. The `PINKDB_KEY`
/// passphrase, when set, becomes the store's at-rest key in local mode; in
/// remote mode the variable is inert (the server's own at-rest configuration
/// applies), so a set key is called out instead of being silently ignored.
fn build_session(opts: &Opts, key: Option<EncryptionKey>) -> Session {
    let host = opts.host.clone();
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
            apply_extension_policy(&mut db);
            load_builtin_extensions(&mut db);
            Session::Local(LocalSession::new(db))
        }
        #[cfg(feature = "client")]
        Some(host) => {
            if let Some(session) = open_tls_session(&host, opts) {
                return session;
            }
            match Client::connect(&host) {
                Ok(client) => remote_session(client, opts),
                Err(e) => {
                    eprintln!("Error: cannot connect to {host}: {e}");
                    std::process::exit(1);
                }
            }
        }
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

/// The TLS remote session when the flags asked for one (crate feature `tls`);
/// `None` = plaintext. Startup validation has already vetted the flag
/// combination and feature availability, so a TLS request that reaches here
/// always has exactly one trust source.
#[cfg(feature = "tls")]
fn open_tls_session(host: &str, opts: &Opts) -> Option<Session> {
    if !(opts.tls || opts.tls_ca_cert.is_some() || opts.tls_insecure) {
        return None;
    }
    if opts.tls_insecure {
        eprintln!(
            "warning: --tls-insecure skips server certificate verification; \
             use it only for local testing"
        );
    }
    let trust = match &opts.tls_ca_cert {
        Some(path) => ClientTrust::CaFile(std::path::PathBuf::from(path)),
        None => ClientTrust::Insecure,
    };
    match Client::connect_tls(host, &tls_server_name(host), trust) {
        Ok(client) => Some(remote_session(client, opts)),
        Err(e) => {
            eprintln!("Error: cannot connect to {host} over TLS: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(all(feature = "client", not(feature = "tls")))]
fn open_tls_session(_host: &str, _opts: &Opts) -> Option<Session> {
    // `validate_tls_args` rejects TLS flags in this build.
    None
}

/// Wrap a connected client as the REPL session, logging in first when
/// `--user` was given (wave 48). A failed login is fatal: the credentials were
/// explicit, so continuing unauthenticated would silently run as the server's
/// anonymous client.
#[cfg(feature = "client")]
fn remote_session(mut client: Client, opts: &Opts) -> Session {
    if let Some(user) = &opts.user {
        let password = opts.password.as_deref().unwrap_or_default();
        match client.login(user, password) {
            Ok(name) => {
                eprintln!("pinkdb: logged in as {name}");
            }
            Err(e) => {
                eprintln!("Error: cannot log in as {user}: {e}");
                std::process::exit(1);
            }
        }
    }
    // The timer defaults on for an interactive terminal; enable the server's
    // per-connection profiling now so the first statement already reports a
    // breakdown.
    if TIMER.load(Ordering::Relaxed)
        && let Err(e) = client.set_profiling(true)
    {
        cli_error!("{e}");
    }
    Session::Remote(client)
}

/// Durable-open options from the environment: `PINKDB_BUSY_TIMEOUT_MS`
/// (optional) bounds how long `.open` and later statements wait for another
/// process's write lease before reporting the database busy.
///
/// The extension policy rides along: a WAL that records an extension is
/// replayed into a load only when the operator opted in
/// (`--allow-load-extension`) *and* the library is inside the extension root
/// (`--extension-root`). Without the opt-in, such a database fails to open
/// with an actionable error instead of loading a path the file names.
fn durable_options() -> DurableOpenOptions {
    let mut opts = DurableOpenOptions::default();
    if let Ok(ms) = std::env::var("PINKDB_BUSY_TIMEOUT_MS")
        && let Ok(ms) = ms.trim().parse::<u64>()
    {
        opts.busy_timeout = std::time::Duration::from_millis(ms);
    }
    opts.allow_extension_replay = ALLOW_LOAD_EXTENSION.load(Ordering::Relaxed);
    opts.extension_root = extension_root();
    opts
}

/// The configured extension root, if any.
fn extension_root() -> Option<std::path::PathBuf> {
    EXTENSION_ROOT
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}

/// Apply the CLI's extension policy to a store: whether the SQL
/// `LOAD EXTENSION` statement may run, and the directory every extension
/// load is confined to.
fn apply_extension_policy(db: &mut PinkDb) {
    db.set_allow_sql_load_extension(ALLOW_LOAD_EXTENSION.load(Ordering::Relaxed));
    db.set_extension_root(extension_root());
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
    // The extension policy of this session: `LOAD EXTENSION` is off unless
    // the operator asked for it (the flag or the environment variable), and
    // every load is confined to the extension root when one is configured.
    let allow_load_extension = opts.allow_load_extension
        || matches!(
            std::env::var("PINKDB_ALLOW_LOAD_EXTENSION")
                .ok()
                .as_deref()
                .map(str::trim),
            Some("1") | Some("true") | Some("yes")
        );
    ALLOW_LOAD_EXTENSION.store(allow_load_extension, Ordering::Relaxed);
    if let Some(root) = &opts.extension_root {
        let root = std::path::PathBuf::from(root);
        if !root.is_dir() {
            eprintln!(
                "Error: --extension-root {:?} is not a directory",
                root.display()
            );
            std::process::exit(2);
        }
        *EXTENSION_ROOT.lock().unwrap_or_else(|p| p.into_inner()) = Some(root);
    }
    // The linked registry must know the CLI's built-in extensions before any
    // store is created: a durable reopen whose WAL replays a json record
    // resolves the name here during replay.
    register_builtin_extensions();
    // Only the remote banner (client feature) needs the host string.
    #[cfg(feature = "client")]
    let host = opts.host.clone();
    // The timer defaults on for an interactive terminal. Decide it before the
    // session exists so a remote connect can enable the server's profiling;
    // `-c` runs as a script, where the timer stays off unless asked for.
    let interactive = std::io::stdin().is_terminal();
    if opts.commands.is_empty() {
        TIMER.store(interactive, Ordering::Relaxed);
    }
    let mut session = build_session(
        &opts,
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

    if interactive {
        match &session {
            Session::Local(_) => {
                println!(
                    "pinkdb v{} — mini DuckDB (in-memory). Type .help, end SQL with ;",
                    env!("CARGO_PKG_VERSION")
                )
            }
            #[cfg(feature = "client")]
            Session::Remote(client) => println!(
                "pinkdb v{} — mini DuckDB (remote {}, server v{}). Type .help, end SQL with ;",
                env!("CARGO_PKG_VERSION"),
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
                if !line.trim().is_empty()
                    && let Some(entry) = input.history_entry(&line)
                {
                    rl.add_history_entry(entry.as_str()).ok();
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

    /// M-9: every name-carrying `QueryResult` variant is sanitized. A single
    /// ESC in any of them must never reach the rendered text raw.
    #[test]
    fn every_result_variant_sanitizes_its_names() {
        let evil = "\u{1b}]0;pwned\u{7}";
        let n = || evil.to_string();
        let results = vec![
            QueryResult::Created,
            QueryResult::Inserted { count: 1 },
            QueryResult::Updated { count: 1 },
            QueryResult::Deleted { count: 1 },
            QueryResult::Began,
            QueryResult::Committed,
            QueryResult::RolledBack,
            QueryResult::CreatedView { name: n() },
            QueryResult::DroppedTable { name: n() },
            QueryResult::CreatedIndex { name: n() },
            QueryResult::DroppedIndex { name: n() },
            QueryResult::CreatedMaterializedView { name: n() },
            QueryResult::RefreshedMaterializedView { name: n() },
            QueryResult::RefreshedMaterializedViews { names: vec![n()] },
            QueryResult::DroppedMaterializedView { name: n() },
            QueryResult::DroppedView { name: n() },
            QueryResult::CreatedDatabase { name: n() },
            QueryResult::DroppedDatabase { name: n() },
            QueryResult::Used { db: n() },
            QueryResult::LoadedExtension { name: n() },
            QueryResult::Checkpointed,
            QueryResult::AlteredTable { name: n() },
            QueryResult::CreatedProcedure { name: n() },
            QueryResult::DroppedProcedure { name: n() },
            QueryResult::CreatedMacro { name: n() },
            QueryResult::DroppedMacro { name: n() },
            QueryResult::CreatedUser { name: n() },
            QueryResult::AlteredUser { name: n() },
            QueryResult::DroppedUser { name: n() },
            QueryResult::CreatedRole { name: n() },
            QueryResult::DroppedRole { name: n() },
            QueryResult::Granted { description: n() },
            QueryResult::Revoked { description: n() },
            QueryResult::Called {
                name: n(),
                statements: 1,
                result: None,
                output: vec![n()],
            },
            QueryResult::Selected {
                names: vec![n()],
                types: vec![DataType::Varchar],
                rows: vec![vec![Value::Varchar(n())]],
            },
        ];
        let text = render_results_text(&results);
        assert!(
            !text.chars().any(|c| c.is_control() && c != '\n'),
            "raw control in rendered results: {text:?}"
        );
        assert!(text.contains("\\u{1b}"), "{text}");
        assert!(text.contains("Procedure"), "{text}");
    }

    #[test]
    fn sanitized_column_names_keep_the_box_aligned() {
        let names = vec!["a\u{1b}[31mB".to_string(), "Chloé".to_string()];
        let types = vec![DataType::Integer, DataType::Varchar];
        let rows = vec![vec![Value::Integer(1), Value::Varchar("x".to_string())]];
        let out = format_select(&names, &types, &rows);
        assert!(!out.contains('\u{1b}'));
        // The sanitized header occupies its escaped width; every border line
        // must have the same display length.
        let widths: Vec<usize> = out
            .lines()
            .filter(|l| l.starts_with('│'))
            .map(|l| l.chars().count())
            .collect();
        assert!(widths.windows(2).all(|w| w[0] == w[1]), "{out}");
    }

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
