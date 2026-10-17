//! `.lake` dot-commands: snapshot the in-memory store into a pinkdb lake and
//! load versions back.
//!
//! Built with the `lake` feature (full: Vortex data files, format_version 4,
//! `.lake attach` over footers) or `lake-lite` (lean: `.pink` data files,
//! format_version 4, `.lake attach` over the published statistics blocks).
//!
//! The lake itself lives in the `pink-lake` crate; this module is only the
//! REPL surface. Locations are local directories, or `s3://bucket/prefix`
//! URLs when the `s3` feature is built (AWS S3, Cloudflare R2; credentials
//! and endpoint come from the usual `AWS_*` environment; the safety opt-ins
//! `PINKDB_S3_ALLOW_HTTP` and `PINKDB_S3_ALLOW_DEGRADED` work exactly as the
//! library documents). `.lake open` replaces the in-memory store:
//!
//! - the session database is reset to `main` (the lake has no session state;
//!   the output says so when it changes);
//! - the session's `PINKDB_KEY` is carried over so a later `.save`/`.open`
//!   stays encrypted, while the lake's own files stay plaintext (the output
//!   says so when a key is set);
//! - a lake has **one live writer**: the session remembers the version it
//!   last opened or saved per lake, and a later `.lake save` to that lake
//!   fails with a conflict error when another writer published in between
//!   (no silent lost update); `.lake save <dir> force` overrides it on
//!   purpose. Inside one save the handle check is atomic with the write
//!   (over S3 the catalog replace is a conditional `If-Match` write);
//! - `.lake save` validates the store *before* creating a lake, so a store
//!   the catalog cannot represent (dotted database name, over-long table
//!   name) leaves no stray empty lake behind.
//!
//! # Incremental writes: `.lake attach <dir> write` and `.lake commit`
//!
//! `.lake attach <dir> write` opens the lake *writable* (the attached-write
//! route; the default attach stays read-only): every table of the pinned
//! version is materialized into the session store, and ordinary
//! `INSERT`/`UPDATE`/`DELETE` against those tables accumulate there. `.lake
//! commit <dir>` publishes exactly the difference as one incremental lake
//! version — appended rows become new immutable data files and removed or
//! updated rows become positional delete files, **never a rewrite of a
//! published file** (see the `pink-lake` incremental module). The commit
//! retries a conflicting publish with DuckLake's settings (defaults
//! `max_retry_count = 10`, `retry_backoff = 1.5`, `retry_wait_ms = 100`,
//! overridable with `PINKDB_LAKE_MAX_RETRY_COUNT`,
//! `PINKDB_LAKE_RETRY_BACKOFF`, `PINKDB_LAKE_RETRY_WAIT_MS`).
//!
//! Tables of the lake that already exist in the session store are *adopted*
//! as the writable copy when their schema matches (the crash-recovery path: a
//! durable session's WAL replay rebuilds the tables, and re-attaching
//! continues from them); a schema mismatch is refused. Store tables outside
//! the attachment are not committed (`.lake save` publishes them). In a
//! durable session, `.lake attach … write` checkpoints the materialized
//! tables (so the WAL covers every later change) and `.lake commit` flushes
//! the WAL before publishing, which makes the commit idempotent across a
//! crash: replaying a recovered session re-attaches and `.lake commit`
//! publishes nothing when the change already landed.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

use pink_lake::{CommitSettings, Lake, WritableAttachment};
use pinkdb::PinkDb;

use crate::{plural, take_token, unquote};

/// The one-line pointer to object-storage locations appended to usage text:
/// with the `s3` feature `s3://` URLs are accepted, without it they are not.
#[cfg(feature = "s3")]
const HINT: &str = "   (locations may be s3://bucket/prefix)";
#[cfg(not(feature = "s3"))]
const HINT: &str = "   (an s3://bucket/prefix location needs the `s3` feature)";

/// Per-lake base version of this REPL session (one session per process):
/// the version `.lake open` last loaded from or `.lake save` last published
/// to, keyed by the lake's resolved location. A save checks that the lake
/// has not moved past it, so two sessions that opened the same version can
/// never silently overwrite each other's work.
static BASELINES: Mutex<BTreeMap<String, u64>> = Mutex::new(BTreeMap::new());

fn baseline_key(lake: &Lake) -> String {
    lake.path().display().to_string()
}

fn baseline(lake: &Lake) -> Option<u64> {
    BASELINES
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&baseline_key(lake))
        .copied()
}

fn set_baseline(lake: &Lake, version: u64) {
    BASELINES
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(baseline_key(lake), version);
}

/// The session's writable attachments, one per resolved lake location.
static WRITABLES: Mutex<BTreeMap<String, WritableAttachment>> = Mutex::new(BTreeMap::new());

fn writables() -> MutexGuard<'static, BTreeMap<String, WritableAttachment>> {
    WRITABLES.lock().unwrap_or_else(|p| p.into_inner())
}

/// The commit retry settings of a `.lake commit`, from the environment with
/// DuckLake's defaults (`PINKDB_LAKE_MAX_RETRY_COUNT`,
/// `PINKDB_LAKE_RETRY_BACKOFF`, `PINKDB_LAKE_RETRY_WAIT_MS`). An unparseable
/// value is reported and the default is used.
fn commit_settings() -> CommitSettings {
    let mut settings = CommitSettings::default();
    if let Ok(raw) = std::env::var("PINKDB_LAKE_MAX_RETRY_COUNT") {
        match raw.trim().parse::<u32>() {
            Ok(v) => settings.max_retry_count = v,
            Err(_) => eprintln!(
                "warning: PINKDB_LAKE_MAX_RETRY_COUNT={raw:?} is not a count; using {}",
                settings.max_retry_count
            ),
        }
    }
    if let Ok(raw) = std::env::var("PINKDB_LAKE_RETRY_BACKOFF") {
        match raw.trim().parse::<f64>() {
            Ok(v) if v >= 0.0 => settings.retry_backoff = v,
            _ => eprintln!(
                "warning: PINKDB_LAKE_RETRY_BACKOFF={raw:?} is not a non-negative number; \
                 using {}",
                settings.retry_backoff
            ),
        }
    }
    if let Ok(raw) = std::env::var("PINKDB_LAKE_RETRY_WAIT_MS") {
        match raw.trim().parse::<u64>() {
            Ok(v) => settings.retry_wait_ms = v,
            Err(_) => eprintln!(
                "warning: PINKDB_LAKE_RETRY_WAIT_MS={raw:?} is not milliseconds; using {}",
                settings.retry_wait_ms
            ),
        }
    }
    settings
}

/// Dispatch `.lake <subcommand> ...`.
pub(crate) fn cmd_lake(db: &mut PinkDb, arg: &str) {
    let (sub, rest) = match arg.find(char::is_whitespace) {
        Some(i) => (&arg[..i], arg[i..].trim()),
        None => (arg, ""),
    };
    match sub {
        "save" => cmd_save(db, rest),
        "open" => cmd_open(db, rest),
        "attach" => cmd_attach(db, rest),
        "commit" => cmd_commit(db, rest),
        "detach" => cmd_detach(db, rest),
        "attached" => cmd_attached(db, rest),
        "versions" => cmd_versions(rest),
        "snapshots" => cmd_snapshots(rest),
        "tables" => cmd_tables(rest),
        "info" => cmd_info(rest),
        "" => println!(
            "Usage: .lake <save|open|attach|commit|detach|attached|versions|snapshots|tables|info> ..."
        ),
        other => cli_error!("unknown .lake subcommand \"{other}\". Try .help"),
    }
}

/// The `.lake save` usage line (kept in one place: the argument parser and
/// both of its error paths print it).
const SAVE_USAGE: &str = "Usage: .lake save <dir> [force] [author '<name>'] [message '<text>']";

/// One option *value*: `"…"` / `'…'` (spaces included) or a bare word, plus
/// the remaining text. Quotes are stripped.
fn take_value(input: &str) -> Option<(String, &str)> {
    let s = input.trim_start();
    let mut chars = s.char_indices();
    match chars.next() {
        Some((_, q @ ('"' | '\''))) => {
            let mut value = String::new();
            for (i, c) in chars {
                if c == q {
                    return Some((value, &s[i + c.len_utf8()..]));
                }
                value.push(c);
            }
            None
        }
        Some((_, _)) => {
            let end = s.find(char::is_whitespace).unwrap_or(s.len());
            Some((s[..end].to_string(), &s[end..]))
        }
        None => None,
    }
}

/// `.lake save <location>`: snapshot every database/table of the current
/// store (a directory or an `s3://bucket/prefix` URL with the `s3` feature).
/// `author '<name>'` / `message '<text>'` record the version's commit
/// metadata, like DuckLake's `ducklake_set_commit_message`.
fn cmd_save(db: &PinkDb, arg: &str) {
    let Some((dir, mut rest)) = take_token(arg) else {
        println!("{SAVE_USAGE}{HINT}");
        return;
    };
    let mut force = false;
    let mut author: Option<String> = None;
    let mut message: Option<String> = None;
    loop {
        let tail = rest.trim_start();
        if tail.is_empty() {
            break;
        }
        let Some((token, after)) = take_token(tail) else {
            break;
        };
        match token.to_ascii_lowercase().as_str() {
            "force" => {
                force = true;
                rest = after;
            }
            "author" | "message" => {
                let Some((value, after)) = take_value(after) else {
                    println!("{SAVE_USAGE}{HINT}");
                    return;
                };
                if token.eq_ignore_ascii_case("author") {
                    author = Some(value);
                } else {
                    message = Some(value);
                }
                rest = after;
            }
            _ => {
                println!("{SAVE_USAGE}{HINT}");
                return;
            }
        }
    }
    let dir = unquote(&dir);
    // An open transaction's rows are applied in place but uncommitted: a
    // lake snapshot must never publish them.
    if db.in_transaction() {
        cli_error!(".lake save is not allowed inside a transaction; COMMIT or ROLLBACK first");
        return;
    }
    // Reject an unrepresentable store before `open_or_create` can leave a
    // fresh empty lake behind (e.g. a dotted database name or a table name
    // whose encoded data directory would exceed the filesystem's limit).
    if let Err(e) = pink_lake::validate_store(db) {
        cli_error!("{e}");
        return;
    }
    let mut lake = match Lake::open_or_create_url(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    if let Some(base) = baseline(&lake) {
        let current = lake.current_version();
        if current != base && !force {
            cli_error!(
                "write conflict: lake {dir} moved from version {base} to {current} \
                 since this session opened or saved it; saving would overwrite that \
                 work. Reopen it with .lake open, save elsewhere, or use \
                 .lake save <dir> force"
            );
            return;
        }
    }
    if author.is_some() || message.is_some() {
        lake.set_commit_message(author, message);
    }
    let t0 = Instant::now();
    match lake.write_from_store_report(db) {
        Ok(report) => {
            set_baseline(&lake, report.version);
            // A writable attachment of this lake just had its whole change
            // published by the save: re-pin it to the new version with the
            // store's current content (nothing pending any more).
            let key = lake.path().display().to_string();
            let mut writables = writables();
            if let Some(wa) = writables.get_mut(&key) {
                match wa.repin_at(report.version, db) {
                    Ok(()) => println!(
                        "  (writable attach at {dir} re-pinned to version {})",
                        report.version
                    ),
                    Err(e) => cli_error!("{e}"),
                }
            }
            drop(writables);
            if db
                .list_databases()
                .iter()
                .any(|d| !db.list_views_in(d).is_empty())
            {
                println!("Note: views are session-local and were not saved.");
            }
            if db.encryption_key().is_some() {
                println!("Note: lake data files are plaintext; PINKDB_KEY does not encrypt them.");
            }
            let attached = db.attached_tables().len();
            if attached > 0 {
                println!(
                    "Note: attached lake tables are session-local and were not saved \
                     ({attached} table{}).",
                    plural(attached)
                );
            }
            println!(
                "Saved lake version {} in {:.3}s ({} table{}, {} row{}, {} bytes written)",
                report.version,
                t0.elapsed().as_secs_f64(),
                report.tables,
                plural(report.tables),
                report.rows,
                plural(report.rows as usize),
                report.bytes_written
            );
            println!("  lake: {}", lake.path().display());
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake open <location> [version]`: replace the in-memory store with the lake's
/// latest (or the given) version.
fn cmd_open(db: &mut PinkDb, arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake open <dir> [version]{HINT}");
        return;
    };
    let dir = unquote(&dir);
    let version = if rest.trim().is_empty() {
        None
    } else {
        match rest.trim().parse::<u64>() {
            Ok(v) => Some(v),
            Err(_) => {
                println!("Usage: .lake open <dir> [version]{HINT}");
                return;
            }
        }
    };
    // `.lake open` replaces the whole store: refuse when that would silently
    // discard an open transaction or detach a durable database (later
    // statements would no longer be logged to its WAL).
    if db.in_transaction() {
        cli_error!(".lake open is not allowed inside a transaction; COMMIT or ROLLBACK first");
        return;
    }
    if db.is_durable() {
        cli_error!(
            ".lake open would detach this session from its durable database (later \
             statements would not be written to its WAL); start a new session for the lake"
        );
        return;
    }
    // A writable attachment's tables are store tables: replacing the store
    // would silently discard uncommitted changes (or orphan the attachment).
    {
        let writables = writables();
        if !writables.is_empty() {
            let mut pending: Vec<String> = Vec::new();
            for (location, wa) in writables.iter() {
                let changed = wa.changed_tables(db).unwrap_or_default();
                if !changed.is_empty() {
                    pending.push(format!("{location} ({} table(s))", changed.len()));
                }
            }
            if !pending.is_empty() {
                cli_error!(
                    ".lake open would discard the uncommitted changes of the writable \
                     attachment(s) {}; run .lake commit (or .lake detach <dir> force) first",
                    pending.join(", ")
                );
                return;
            }
        }
    }
    let lake = match Lake::open_url(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let t0 = Instant::now();
    match lake.load(version) {
        Ok(mut new_db) => {
            // Replacing the store must not silently drop the session's
            // `PINKDB_KEY`: a later `.save`/`.open` would write plaintext.
            let key = db.encryption_key().cloned();
            let keyed = key.is_some();
            if key.is_some() {
                new_db.set_encryption_key(key);
            }
            let previous_db = db.current_db().to_string();
            let tables: usize = new_db
                .list_databases()
                .iter()
                .map(|d| new_db.list_tables_in(d).len())
                .sum();
            let shown = version.unwrap_or_else(|| lake.current_version());
            *db = new_db;
            // Saving after opening an older version deliberately publishes
            // on top of the latest one (a restore), so the base is current.
            set_baseline(&lake, lake.current_version());
            // Writable attachments describe store tables that the replace
            // just discarded: drop them (their changes were either committed
            // or are gone with the store — the pending check above already
            // refused the latter).
            if !writables().is_empty() {
                writables().clear();
                println!("Note: .lake open discarded the session's writable attachment(s).");
            }
            println!(
                "Opened lake version {shown} in {:.3}s ({tables} table{}) from {dir}",
                t0.elapsed().as_secs_f64(),
                plural(tables)
            );
            if db.current_db() != previous_db {
                println!(
                    "Note: the session database was reset to \"{}\".",
                    db.current_db()
                );
            }
            if keyed {
                println!(
                    "Note: lake data files are plaintext; the session key applies to \
                     .save/.open, not to the lake."
                );
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake attach <location> [version] [read-only|write]`: attach the lake.
///
/// The default (`read-only`) attach reads the published statistics only: a
/// Vortex data file contributes its footer, metadata segment and stored
/// statistics; a `.pink` file contributes its manifest statistics block (no
/// data file is opened). No column data is decoded; the one table a query
/// actually scans materializes lazily on first use.
///
/// `write` opens the lake for incremental writes (see the module docs):
/// every table of the pinned version is materialized into the session store
/// as a writable copy, DML accumulates there, and `.lake commit <dir>`
/// publishes exactly the difference. A same-named store table with the
/// matching schema is adopted as the writable copy (the recovery path); a
/// schema mismatch is refused.
fn cmd_attach(db: &mut PinkDb, arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake attach <dir> [version] [read-only|write]{HINT}");
        return;
    };
    let dir = unquote(&dir);
    let mut tokens = rest.split_whitespace();
    let (version, mut mode) = match tokens.next() {
        None => (None, None),
        Some("read-only") | Some("read") | Some("ro") => (None, Some("read-only")),
        Some("write") | Some("read-write") | Some("read_write") | Some("rw") => {
            (None, Some("write"))
        }
        Some(v) => match v.parse::<u64>() {
            Ok(v) => (Some(v), None),
            Err(_) => {
                println!("Usage: .lake attach <dir> [version] [read-only|write]{HINT}");
                return;
            }
        },
    };
    if mode.is_none() {
        mode = match tokens.next() {
            None => Some("read-only"),
            Some("read-only") | Some("read") | Some("ro") => Some("read-only"),
            Some("write") | Some("read-write") | Some("read_write") | Some("rw") => Some("write"),
            Some(_) => None,
        };
    }
    let (version, write) = match mode {
        Some("read-only") => (version, false),
        Some("write") => (version, true),
        _ => {
            println!("Usage: .lake attach <dir> [version] [read-only|write]{HINT}");
            return;
        }
    };
    if write {
        cmd_attach_write(db, &dir, version);
        return;
    }
    // Attaching changes session state that a ROLLBACK cannot undo (like
    // `.lake save`, not like `.lake open`, which replaces the store).
    if db.in_transaction() {
        cli_error!(".lake attach is not allowed inside a transaction; COMMIT or ROLLBACK first");
        return;
    }
    let lake = match Lake::open_url(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let t0 = Instant::now();
    match lake.attach(version) {
        Ok(report) => match db.attach_lake(report.lake) {
            Ok(tables) => {
                let shape = if report.statistics_only {
                    "statistics-only"
                } else {
                    "footer-only"
                };
                println!(
                    "Attached {dir} version {} in {:.3}s ({} table{}, {shape})",
                    report.version,
                    t0.elapsed().as_secs_f64(),
                    tables.len(),
                    plural(tables.len())
                );
                if report.remote {
                    println!(
                        "Note: object storage has no ranged reads in this build: attach read \
                         the Vortex data files whole (queries still skip the decode)."
                    );
                }
            }
            Err(e) => cli_error!("{e}"),
        },
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake attach <dir> write`: the writable (incremental) attach. Materializes
/// the pinned version's tables into the session store and remembers the base
/// version; `.lake commit <dir>` publishes the difference.
fn cmd_attach_write(db: &mut PinkDb, dir: &str, version: Option<u64>) {
    if db.in_transaction() {
        cli_error!(".lake attach is not allowed inside a transaction; COMMIT or ROLLBACK first");
        return;
    }
    let t0 = Instant::now();
    let wa = match WritableAttachment::open(dir, version) {
        Ok(wa) => wa,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let key = wa.location().display().to_string();
    if writables().contains_key(&key) {
        cli_error!(
            "{dir} is already attached writable in this session; .lake commit it or \
             .lake detach it first"
        );
        return;
    }
    let base = wa.base_version();
    let mut created = 0usize;
    let mut adopted = 0usize;
    for (table_key, rows) in wa.tables() {
        let Some((dbname, table)) = table_key.split_once('.') else {
            cli_error!("lake table key \"{table_key}\" is not \"db.table\"");
            return;
        };
        let Some(base_table) = wa.base_table(&table_key) else {
            continue;
        };
        match db.table_in(dbname, table) {
            Ok(existing) => {
                if existing.schema() != base_table.schema() {
                    cli_error!(
                        "cannot attach {dir} writable: table \"{table_key}\" already exists \
                         in the session with a different shape — the lake has ({}) and the \
                         store has ({}); drop or rename one of them",
                        schema_text(&base_table.schema()),
                        schema_text(&existing.schema())
                    );
                    return;
                }
                adopted += 1;
                let _ = rows;
            }
            Err(_) => {
                if !db.database_exists(dbname) {
                    if let Err(e) = db.create_database(dbname) {
                        cli_error!("{e}");
                        return;
                    }
                }
                if let Err(e) = db.create_table_in(dbname, table, base_table.schema()) {
                    cli_error!("{e}");
                    return;
                }
                let dst = match db.table_mut_in(dbname, table) {
                    Ok(t) => t,
                    Err(e) => {
                        cli_error!("{e}");
                        return;
                    }
                };
                for c in 0..base_table.num_chunks() {
                    let Some(chunk) = base_table.scan_chunk(c) else {
                        continue;
                    };
                    if let Err(e) = dst.append_shared_chunk(&chunk.columns) {
                        cli_error!("{e}");
                        return;
                    }
                }
                created += 1;
            }
        }
    }
    writables().insert(key.clone(), wa);
    // A durable session's WAL must be able to replay the statements that
    // mutate the writable tables: the tables themselves are not logged (the
    // attach is a session action), so persist them now. Later INSERTs/UPDATEs
    // replay onto the checkpointed tables.
    let mut checkpointed = false;
    if db.is_durable() {
        match db.checkpoint() {
            Ok(()) => checkpointed = true,
            Err(e) => {
                writables().remove(&key);
                cli_error!("could not checkpoint the writable tables: {e}");
                return;
            }
        }
    }
    println!(
        "Attached {dir} version {base} writable in {:.3}s ({created} table{} materialized, \
         {adopted} adopted)",
        t0.elapsed().as_secs_f64(),
        plural(created)
    );
    if checkpointed {
        println!("  (durable session: the materialized tables were checkpointed)");
    }
    // The commit reconciles each table to the session's content; when the
    // session's tables already differ from the pinned version (adopted
    // recovery tables, or edits made before re-attaching), say exactly what
    // that means instead of letting a diff quietly drop rows.
    if adopted > 0 {
        if let Some(wa) = writables().get(&key) {
            let changed = wa.changed_tables(db).unwrap_or_default();
            if !changed.is_empty() {
                println!(
                    "Note: {} session table(s) differ from version {base} ({}); .lake commit \
                     reconciles the lake to the session's content — rows the lake has and the \
                     session lacks are deleted.",
                    changed.len(),
                    changed.join(", ")
                );
            }
        }
    }
    println!("  publish changes with .lake commit {dir}");
}

fn schema_text(schema: &[(String, pinkdb::DataType)]) -> String {
    schema
        .iter()
        .map(|(n, t)| format!("{n} {t}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `.lake commit <dir>`: publish the writable attachment's changes as one
/// incremental lake version (appended data files + positional delete files;
/// no published file is rewritten). The commit retries a conflicting publish
/// with the DuckLake-parity settings (see the module docs) and is idempotent:
/// committing with nothing to publish is a no-op.
fn cmd_commit(db: &mut PinkDb, arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake commit <dir>{HINT}");
        return;
    };
    if !rest.trim().is_empty() {
        println!("Usage: .lake commit <dir>{HINT}");
        return;
    }
    let dir = unquote(&dir);
    if db.in_transaction() {
        cli_error!(".lake commit is not allowed inside a transaction; COMMIT or ROLLBACK first");
        return;
    }
    let Some(wa) = writables().remove(&dir) else {
        cli_error!(
            "no writable attach at {dir}; run .lake attach {dir} write first (read-only \
             attaches are never written)"
        );
        return;
    };
    let mut wa = wa;
    // The WAL covers the changes first: every statement feeding this commit
    // is durable before the lake publishes, so a crash can only lose the
    // publication, never the change (re-running .lake commit after reopen is
    // the recovery, and it is idempotent).
    if db.is_durable() {
        if let Err(e) = db.flush_wal() {
            writables().insert(dir.clone(), wa);
            cli_error!("could not flush the WAL before committing: {e}");
            return;
        }
    }
    let settings = commit_settings();
    let t0 = Instant::now();
    match wa.commit(db, &settings) {
        Ok(report) if !report.committed => {
            println!(
                "Nothing to commit to {dir}: no change against version {}",
                report.version
            );
        }
        Ok(report) => {
            set_baseline_version_only(&dir, report.version);
            println!(
                "Committed lake version {} onto {dir} in {:.3}s ({} file{}: {} data, {} \
                 delete; +{} row{}, -{} row{}; {} bytes; {} retr{})",
                report.version,
                t0.elapsed().as_secs_f64(),
                report.data_files + report.delete_files,
                plural(report.data_files + report.delete_files),
                report.data_files,
                report.delete_files,
                report.rows_added,
                plural(report.rows_added as usize),
                report.rows_deleted,
                plural(report.rows_deleted as usize),
                report.bytes_written,
                report.retries,
                if report.retries == 1 { "y" } else { "ies" },
            );
        }
        Err(e) => {
            cli_error!("{e}");
            let text = e.to_string();
            if text.contains("changed in the lake since this change was computed") {
                println!(
                    "hint: .lake detach {dir} force + .lake attach {dir} write re-materializes \
                     the base at the current version (the session's tables are adopted, and \
                     the next commit reconciles the lake to them)"
                );
            }
        }
    }
    writables().insert(dir, wa);
}

/// Set the save-conflict baseline for a location that was never `.lake
/// open`ed (the writable attach path): a later `.lake save` to the same lake
/// checks against the version this session last saw.
fn set_baseline_version_only(location: &str, version: u64) {
    BASELINES
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(location.to_string(), version);
}

/// `.lake detach <dir> [force]`: drop one attached lake from this session. A
/// writable attach with uncommitted changes refuses unless `force` is given
/// (which keeps the store tables as ordinary tables and abandons the
/// attachment's base — the changes are then not publishable through it).
fn cmd_detach(db: &mut PinkDb, arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake detach <dir> [force]{HINT}");
        return;
    };
    let dir = unquote(&dir);
    let force = match rest.trim() {
        "" => false,
        "force" => true,
        _ => {
            println!("Usage: .lake detach <dir> [force]{HINT}");
            return;
        }
    };
    let mut writable = writables().remove(&dir);
    if let Some(wa) = &writable {
        let changed = wa.changed_tables(db).unwrap_or_default();
        if !changed.is_empty() && !force {
            writables().insert(dir.clone(), writable.take().expect("writable present"));
            cli_error!(
                "writable attach {dir} has uncommitted changes in {} table(s) ({}); \
                 .lake commit {dir} first, or detach with `force` to keep the session \
                 tables as ordinary tables and abandon the attachment",
                changed.len(),
                changed.join(", ")
            );
            return;
        }
        println!("Detached writable attach {dir} (the session tables stay as ordinary tables)");
    }
    if db.detach_lake(&dir) {
        println!("Detached {dir}");
        return;
    }
    if writable.is_none() {
        cli_error!("no lake is attached at {dir} (see .lake attached)");
    }
}

/// `.lake attached`: the lakes attached to this session and their tables,
/// plus each lake's data-file decode counters (statistics-only queries and a
/// projected decode show up here). Writable attaches list their base version
/// and pending tables.
fn cmd_attached(db: &PinkDb, arg: &str) {
    if !arg.trim().is_empty() {
        println!("Usage: .lake attached");
        return;
    }
    let lakes = db.attached_lakes();
    if lakes.is_empty() {
        println!("(no attached lakes)");
    }
    let stats = db.attached_decode_stats();
    for ((location, tables), (_, loads, columns)) in lakes.into_iter().zip(stats) {
        println!("{location}");
        for t in tables {
            println!(
                "  {}.{} ({} row{})",
                t.db,
                t.table,
                t.rows,
                plural(t.rows as usize)
            );
        }
        println!(
            "  decodes: {loads} load{}, {columns} column{}",
            plural(loads as usize),
            plural(columns as usize)
        );
    }
    for (location, wa) in writables().iter() {
        let changed = wa.changed_tables(db).unwrap_or_default();
        println!("{location} (writable)");
        for (key, rows) in wa.tables() {
            println!("  {key} ({rows} row{})", plural(rows as usize));
        }
        if changed.is_empty() {
            println!(
                "  base version: {} (no uncommitted changes)",
                wa.base_version()
            );
        } else {
            println!(
                "  base version: {} (uncommitted: {})",
                wa.base_version(),
                changed.join(", ")
            );
        }
    }
}

/// `.lake versions <location>`: the published snapshot versions, current marked.
fn cmd_versions(arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake versions <dir>{HINT}");
        return;
    };
    if !rest.trim().is_empty() {
        println!("Usage: .lake versions <dir>{HINT}");
        return;
    }
    let dir = unquote(&dir);
    let lake = match Lake::open_url(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    match lake.versions() {
        Ok(versions) => {
            let current = lake.current_version();
            for v in versions {
                if v == current {
                    println!("{v} (current)");
                } else {
                    println!("{v}");
                }
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake snapshots <dir>`: every published version's record — publish time,
/// author, commit message, table/row counts and the change summary relative
/// to the version before it (DuckLake's `snapshots()` row, one per version).
fn cmd_snapshots(arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake snapshots <dir>{HINT}");
        return;
    };
    if !rest.trim().is_empty() {
        println!("Usage: .lake snapshots <dir>{HINT}");
        return;
    }
    let dir = unquote(&dir);
    let lake = match Lake::open_url(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    match lake.snapshots() {
        Ok(records) if records.is_empty() => println!("(no snapshots)"),
        Ok(records) => {
            let current = lake.current_version();
            for info in records {
                if info.version == current {
                    println!("version {} (current)", info.version);
                } else {
                    println!("version {}", info.version);
                }
                println!("  time:    {}", info.time);
                if let Some(author) = &info.author {
                    println!("  author:  {author}");
                }
                if let Some(message) = &info.message {
                    println!("  message: {message}");
                }
                println!(
                    "  tables:  {} ({} row{})",
                    info.tables,
                    info.rows,
                    plural(info.rows as usize)
                );
                match &info.changes {
                    None => println!("  changes: (not recorded)"),
                    Some(changes) if changes.is_empty() => println!("  changes: (none)"),
                    Some(changes) => {
                        let mut parts = Vec::new();
                        if !changes.tables_created.is_empty() {
                            parts.push(format!("created: {}", changes.tables_created.join(", ")));
                        }
                        if !changes.tables_dropped.is_empty() {
                            parts.push(format!("dropped: {}", changes.tables_dropped.join(", ")));
                        }
                        if !changes.tables_rewritten.is_empty() {
                            parts.push(format!(
                                "rewritten: {}",
                                changes.tables_rewritten.join(", ")
                            ));
                        }
                        let mut rows: Vec<String> = changes
                            .rows_added
                            .iter()
                            .map(|(table, n)| format!("{table} +{n}"))
                            .collect();
                        rows.extend(
                            changes
                                .rows_removed
                                .iter()
                                .map(|(table, n)| format!("{table} -{n}")),
                        );
                        if !rows.is_empty() {
                            parts.push(format!("rows: {}", rows.join(", ")));
                        }
                        println!("  changes: {}", parts.join("; "));
                    }
                }
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake tables <location>`: the current version's `db.table` names + row counts.
fn cmd_tables(arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake tables <dir>{HINT}");
        return;
    };
    if !rest.trim().is_empty() {
        println!("Usage: .lake tables <dir>{HINT}");
        return;
    }
    let dir = unquote(&dir);
    let lake = match Lake::open_url(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    match lake.tables() {
        Ok(tables) if tables.is_empty() => println!("(no tables)"),
        Ok(tables) => {
            for (name, rows) in tables {
                println!("{name} ({rows} row{})", plural(rows as usize));
            }
            warn_missing_data_files(&lake);
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake info <location>`: format version, current version, snapshots, tables.
fn cmd_info(arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake info <dir>{HINT}");
        return;
    };
    if !rest.trim().is_empty() {
        println!("Usage: .lake info <dir>{HINT}");
        return;
    }
    let dir = unquote(&dir);
    let lake = match Lake::open_url(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let tables = match lake.tables() {
        Ok(tables) => tables,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    println!("Lake {}", lake.path().display());
    println!("  format version:  {}", lake.format_version());
    println!("  current version: {}", lake.current_version());
    match lake.versions() {
        Ok(versions) => println!(
            "  snapshots:       {} ({})",
            versions.len(),
            versions
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Err(e) => println!("  snapshots:       error: {e}"),
    }
    match lake.commits() {
        Ok(commits) if !commits.is_empty() => println!(
            "  commit manifests: {} ({})",
            commits.len(),
            commits
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Ok(_) => println!("  commit manifests: none (an older lake, or none read)"),
        Err(e) => println!("  commit manifests: error: {e}"),
    }
    println!("  tables ({}):", tables.len());
    for (name, rows) in tables {
        println!("    {name} ({rows} row{})", plural(rows as usize));
    }
    warn_missing_data_files(&lake);
}

/// One warning per table whose referenced data file is gone: `.lake tables`/
/// `.lake info` must not advertise data that `load` cannot read.
fn warn_missing_data_files(lake: &Lake) {
    match lake.missing_data_files() {
        Ok(missing) => {
            for (table, path) in missing {
                println!(
                    "warning: table \"{table}\" references missing data file {}",
                    path.display()
                );
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}
