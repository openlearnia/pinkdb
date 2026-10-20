//! `.lake` dot-commands: snapshot the in-memory store into a pinkdb lake and
//! load versions back.
//!
//! Built with the `vortexlake` feature (Vortex data files, format_version 4,
//! `.lake attach` over footers) or `pinklake` (`.pink` data files,
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
//! - `PINKDB_KEY` — when set — is the lake's at-rest key too (with a build
//!   that has both a lake feature and `encryption`): `.lake save` writes
//!   `PNKLAKE1` containers (catalog, snapshots, commit deltas, data files,
//!   delete files) and every `.lake` command unseals them, with the engine's
//!   exact key convention (a keyed open of a plaintext lake is refused; a
//!   wrong key fails closed with `wrong key`; a missing key refuses with
//!   "the lake is encrypted"). Without the variable a lake stays plaintext
//!   exactly as before. The session key is also carried over to the store
//!   `.lake open` builds, so a later `.save`/`.open` stays encrypted;
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
//!
//! # Partitioning (wave 66)
//!
//! `.lake partition <dir> <table> by (<key>, …)` sets a table's partition
//! specification (DuckLake's `ALTER TABLE … SET PARTITIONED BY`) as one
//! catalog version; `… clear` removes it. A key is a column name (identity)
//! or `bucket(N, column)`; DuckLake's other transforms are refused with its
//! own supported set named in the message, duplicate keys are refused like
//! DuckLake's "matches an earlier partition key", and every key column must
//! exist in the table's schema. New writes then split into **one data file
//! per partition value set** — a `.lake commit` of appended rows and a
//! `.lake save` of the table alike — and the catalog records each file's
//! values; existing files are never rewritten (DuckLake semantics).
//!
//! Read-only attaches prune: a `WHERE` carrying simple comparisons (`col =
//! literal`, ranges, `IS [NOT] NULL`) over zoned/partitioned columns decodes
//! only the data files that can still match, and `.lake attached` shows the
//! files decoded per lake.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

use pink_lake::{
    CleanupSettings, CommitSettings, ExpireSettings, Lake, MergeSettings, RewriteSettings,
    WritableAttachment,
};
use pinkdb::PinkDb;

use crate::{plural, take_token, unquote};

/// The lake key from `PINKDB_KEY`, the engine's convention (a non-empty value
/// is a passphrase; unset/empty means plaintext). A set-but-non-UTF-8 value
/// is a hard error — never a silent plaintext lake. The CLI already exits on
/// such a value at startup (`EncryptionKey::from_env` in `main`); this is the
/// same check at the lake boundary.
fn lake_key() -> Option<pink_lake::EncryptionKey> {
    match pink_lake::EncryptionKey::from_env() {
        Ok(key) => key,
        Err(e) => {
            // Fail closed: a set-but-invalid PINKDB_KEY must never fall through
            // to a plaintext lake. (The REPL also checks at startup.)
            cli_error!("{e}");
            std::process::exit(2);
        }
    }
}

/// The keyed or plaintext [`Lake`] open the CLI needs for `dir`: the keyed
/// constructor when `PINKDB_KEY` is set (so the key can never be silently
/// ignored), the plaintext one otherwise.
fn open_lake(dir: &str) -> pink_lake::Result<Lake> {
    match lake_key() {
        Some(key) => Lake::open_url_with_key(dir, key),
        None => Lake::open_url(dir),
    }
}

/// [`open_lake`] for `.lake save`: create a keyed lake at a fresh location,
/// append to an existing keyed one, and refuse a *plaintext* lake when a key
/// is set (never a silent rewrite — save to a new location to migrate).
fn open_or_create_lake(dir: &str) -> pink_lake::Result<Lake> {
    match lake_key() {
        Some(key) => Lake::open_or_create_url_with_key(dir, key),
        None => Lake::open_or_create_url(dir),
    }
}

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
            Ok(v) if v.is_finite() && v >= 0.0 => settings.retry_backoff = v,
            _ => eprintln!(
                "warning: PINKDB_LAKE_RETRY_BACKOFF={raw:?} is not a finite non-negative \
                 number; using {}",
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
        "partition" => cmd_partition(db, rest),
        "inlining" => cmd_inlining(rest),
        "flush" => cmd_flush(rest),
        "changes" => cmd_changes(rest),
        "add_data_files" | "add_files" => cmd_add_data_files(rest),
        "merge" => cmd_merge(rest),
        "rewrite" => cmd_rewrite(rest),
        "expire" => cmd_expire(rest),
        "cleanup" => cmd_cleanup(rest, CleanupKind::Old),
        "orphans" => cmd_cleanup(rest, CleanupKind::Orphaned),
        "files" => cmd_files(rest),
        "table_info" | "tableinfo" => cmd_table_info(rest),
        "options" => cmd_options(rest),
        "settings" => cmd_settings(rest),
        "detach" => cmd_detach(db, rest),
        "attached" => cmd_attached(db, rest),
        "versions" => cmd_versions(rest),
        "snapshots" => cmd_snapshots(rest),
        "tables" => cmd_tables(rest),
        "info" => cmd_info(rest),
        "" => println!(
            "Usage: .lake <save|open|attach|commit|partition|inlining|flush|changes|\
             add_data_files|merge|rewrite|expire|cleanup|orphans|files|table_info|options|\
             settings|detach|attached|versions|snapshots|tables|info> ..."
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
    let mut lake = match open_or_create_lake(&dir) {
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
            if lake.is_encrypted() {
                println!(
                    "Note: the lake is encrypted at rest with PINKDB_KEY (AES-256-GCM, \
                     one Argon2id key per session)."
                );
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
    let lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let encrypted = lake.is_encrypted();
    let t0 = Instant::now();
    match lake.load(version) {
        Ok(mut new_db) => {
            // Replacing the store must not silently drop the session's
            // `PINKDB_KEY`: a later `.save`/`.open` would write plaintext.
            let key = db.encryption_key().cloned();
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
            if encrypted {
                println!(
                    "Note: the lake is encrypted at rest with PINKDB_KEY; the same key is \
                     installed on the store for later .save/.open."
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
    let lake = match open_lake(&dir) {
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
                if report.encrypted {
                    if report.statistics_only {
                        println!(
                            "Note: the lake is encrypted; attach unsealed the manifests and \
                             their statistics blocks and read no data file."
                        );
                    } else {
                        println!(
                            "Note: the lake is encrypted: attach read the whole ciphertext of \
                             each Vortex data file and unsealed it (queries still skip the \
                             decode; ranged footer reads for encrypted files are a follow-up)."
                        );
                    }
                } else if report.remote {
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
/// The writable attach's default `data_inlining_row_limit`, from
/// `PINKDB_LAKE_INLINING_ROW_LIMIT` (DuckLake's session setting
/// `ducklake_default_data_inlining_row_limit`; 0 disables inlining so every
/// commit writes data files). Unset keeps the handle default (10); an
/// unparseable value is reported and the default is used. A table's persisted
/// `.lake inlining` limit always wins.
fn attach_inlining_limit() -> Option<u64> {
    let raw = std::env::var("PINKDB_LAKE_INLINING_ROW_LIMIT").ok()?;
    match raw.trim().parse::<u64>() {
        Ok(v) => Some(v),
        Err(_) => {
            eprintln!(
                "warning: PINKDB_LAKE_INLINING_ROW_LIMIT={raw:?} is not a count; using the \
                 default"
            );
            None
        }
    }
}

fn cmd_attach_write(db: &mut PinkDb, dir: &str, version: Option<u64>) {
    if db.in_transaction() {
        cli_error!(".lake attach is not allowed inside a transaction; COMMIT or ROLLBACK first");
        return;
    }
    let t0 = Instant::now();
    let wa = match lake_key() {
        Some(key) => WritableAttachment::open_with_key(dir, version, key),
        None => WritableAttachment::open(dir, version),
    };
    let mut wa = match wa {
        Ok(wa) => wa,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    if let Some(limit) = attach_inlining_limit() {
        wa.set_data_inlining_row_limit(limit);
    }
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
                if !db.database_exists(dbname)
                    && let Err(e) = db.create_database(dbname)
                {
                    cli_error!("{e}");
                    return;
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
                // The lake's schema record is authoritative for column
                // defaults (wave 65): an INSERT that omits an added column
                // writes them, exactly like the version it materializes.
                for (col, src) in dst.columns.iter_mut().zip(&base_table.columns) {
                    col.default = src.default.clone();
                }
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
    if adopted > 0
        && let Some(wa) = writables().get(&key)
    {
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
    if db.is_durable()
        && let Err(e) = db.flush_wal()
    {
        writables().insert(dir.clone(), wa);
        cli_error!("could not flush the WAL before committing: {e}");
        return;
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
                 delete{};{} +{} row{}, -{} row{}; {} bytes; {} retr{})",
                report.version,
                t0.elapsed().as_secs_f64(),
                report.data_files + report.delete_files,
                plural(report.data_files + report.delete_files),
                report.data_files,
                report.delete_files,
                if report.rows_inlined > 0 {
                    format!(
                        " ({} row{} inlined)",
                        report.rows_inlined,
                        plural(report.rows_inlined as usize)
                    )
                } else {
                    String::new()
                },
                if report.schema_changes > 0 {
                    format!(
                        " {} schema change{};",
                        report.schema_changes,
                        plural(report.schema_changes)
                    )
                } else {
                    String::new()
                },
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

/// `.lake partition <location> <table> by (<key>, …)` — DuckLake's
/// `ALTER TABLE … SET PARTITIONED BY`, published as one catalog version:
///
/// ```text
/// .lake partition ./lake t by (region)
/// .lake partition ./lake main.t by (bucket(4, id), region)
/// .lake partition ./lake t clear
/// ```
///
/// A key is a column name (identity) or `bucket(N, column)`; DuckLake's
/// transforms this build does not implement (`year`, `month`, `day`, `hour`,
/// `epoch_*`) and anything else are refused with DuckLake's own supported
/// set in the message. The table must already exist in the lake
/// (`<name>` means `main.<name>`); every key column must exist in its
/// schema. Existing files keep their recorded values — the spec governs new
/// writes — and new commits (and a `.lake save` of the table) split rows
/// into one data file per partition value set.
fn cmd_partition(db: &PinkDb, arg: &str) {
    if db.in_transaction() {
        cli_error!(".lake partition is not allowed inside a transaction; COMMIT or ROLLBACK first");
        return;
    }
    let usage = "Usage: .lake partition <dir> <table> by (<key>, ...) | clear{HINT}";
    let Some((dir, rest)) = take_token(arg) else {
        println!("{usage}");
        return;
    };
    let Some((table, rest)) = take_token(rest) else {
        println!("{usage}");
        return;
    };
    let dir = unquote(&dir);
    let table = unquote(&table);
    // A bare table name means main.<name> (the flat catalog's key shape).
    let key = if table.contains('.') {
        table.clone()
    } else {
        format!("main.{table}")
    };
    let rest = rest.trim();
    let lower = rest.to_ascii_lowercase();
    // `Some(keys)` with an empty vector is `clear`; `None` is a usage error.
    let keys: Option<Vec<String>> = if lower == "clear" {
        Some(Vec::new())
    } else if let Some(group) = lower.strip_prefix("by").map(str::trim).and_then(|r| {
        r.strip_prefix('(')
            .and_then(|r| r.strip_suffix(')'))
            .map(|inner| inner.to_string())
    }) {
        // Split on top-level commas only: `bucket(4, id)` keeps its inner
        // comma.
        let mut parts = Vec::new();
        let mut depth = 0usize;
        let mut current = String::new();
        for c in group.chars() {
            match c {
                '(' => {
                    depth += 1;
                    current.push(c);
                }
                ')' => {
                    depth = depth.saturating_sub(1);
                    current.push(c);
                }
                ',' if depth == 0 => {
                    parts.push(current.trim().to_string());
                    current.clear();
                }
                _ => current.push(c),
            }
        }
        parts.push(current.trim().to_string());
        parts.retain(|p| !p.is_empty());
        (!parts.is_empty()).then_some(parts)
    } else {
        None
    };
    let Some(keys) = keys else {
        println!("{usage}");
        return;
    };
    // A writable attachment of the same lake would be invalidated by the
    // new version (its base changes): refuse and point at the recovery.
    {
        let writables = writables();
        if writables.contains_key(&dir) {
            cli_error!(
                "{dir} is attached writable in this session; .lake commit (or .lake detach \
                 {dir}) it before changing its partition specification"
            );
            return;
        }
    }
    let mut lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let settings = commit_settings();
    let t0 = Instant::now();
    let result = if keys.is_empty() {
        lake.clear_partition_spec(&key, &settings)
    } else {
        lake.set_partition_spec(&key, &keys, &settings)
    };
    match result {
        Ok(report) if !report.committed => {
            if keys.is_empty() {
                println!("Table {key} in {dir} has no partition specification (nothing to clear)");
            } else {
                println!(
                    "Table {key} in {dir} is already partitioned by ({}); nothing published",
                    keys.join(", ")
                );
            }
        }
        Ok(report) => {
            set_baseline_version_only(&dir, report.version);
            if keys.is_empty() {
                println!(
                    "Cleared the partition specification of {key} in {dir} (version {} in \
                     {:.3}s); existing files keep their values",
                    report.version,
                    t0.elapsed().as_secs_f64(),
                );
            } else {
                println!(
                    "Partitioned {key} by ({}) in {dir} (version {} in {:.3}s; new writes \
                     split into one data file per {} value)",
                    keys.join(", "),
                    report.version,
                    t0.elapsed().as_secs_f64(),
                    if keys.len() == 1 {
                        "partition"
                    } else {
                        "partition-value set"
                    },
                );
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake inlining <dir> <table> <limit|default>`: set (or clear) one
/// table's persisted `data_inlining_row_limit` — DuckLake's per-table option
/// of the same name, set through `ducklake_set_option(..., table_name =>
/// 't')`. `0` disables inlining for the table; `default` clears the limit so
/// the handle's default applies again. Published as one metadata-only
/// version.
fn cmd_inlining(arg: &str) {
    let usage = "Usage: .lake inlining <dir> <table> <limit|default>{HINT}";
    let Some((dir, rest)) = take_token(arg) else {
        println!("{usage}");
        return;
    };
    let Some((table, rest)) = take_token(rest) else {
        println!("{usage}");
        return;
    };
    let value = rest.trim();
    if value.is_empty() || value.split_whitespace().count() != 1 {
        println!("{usage}");
        return;
    }
    let dir = unquote(&dir);
    let table = flat_table_key(&unquote(&table));
    let limit: Option<u64> = if value.eq_ignore_ascii_case("default") {
        None
    } else {
        match value.parse::<u64>() {
            Ok(n) => Some(n),
            Err(_) => {
                cli_error!("invalid inlining limit {value:?}: expected a count or `default`");
                return;
            }
        }
    };
    {
        let writables = writables();
        if writables.contains_key(&dir) {
            cli_error!(
                "{dir} is attached writable in this session; .lake commit (or .lake detach \
                 {dir}) it before changing its inlining setting"
            );
            return;
        }
    }
    let mut lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let settings = commit_settings();
    match lake.set_inlining_limit(&table, limit, &settings) {
        Ok(report) if !report.committed => {
            println!("Table {table} in {dir} already has that inlining limit; nothing published");
        }
        Ok(report) => {
            set_baseline_version_only(&dir, report.version);
            match limit {
                Some(limit) => println!(
                    "Table {table} in {dir}: commits appending at most {limit} row{} are stored \
                     in the catalog metadata (version {}); 0 disables inlining",
                    if limit == 1 { "" } else { "s" },
                    report.version,
                ),
                None => println!(
                    "Cleared the inlining limit of {table} in {dir} (version {}); the \
                     handle's data_inlining_row_limit default applies",
                    report.version,
                ),
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake flush <dir> [table]`: materialize inlined rows into data files,
/// DuckLake's `ducklake_flush_inlined_data`. Without a table, every table of
/// the lake is flushed in one version.
fn cmd_flush(arg: &str) {
    let usage = "Usage: .lake flush <dir> [table]{HINT}";
    let Some((dir, rest)) = take_token(arg) else {
        println!("{usage}");
        return;
    };
    let dir = unquote(&dir);
    let rest = rest.trim();
    let table = if rest.is_empty() {
        None
    } else if rest.split_whitespace().count() == 1 {
        Some(flat_table_key(&unquote(rest)))
    } else {
        println!("{usage}");
        return;
    };
    {
        let writables = writables();
        if writables.contains_key(&dir) {
            cli_error!(
                "{dir} is attached writable in this session; .lake commit (or .lake detach \
                 {dir}) it before flushing its inlined rows"
            );
            return;
        }
    }
    let mut lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let t0 = Instant::now();
    match lake.flush_inlined_data(table.as_deref()) {
        Ok(report) if !report.committed => match table {
            Some(table) => println!("Table {table} in {dir} has no live inlined rows to flush"),
            None => println!("{dir} has no live inlined rows to flush"),
        },
        Ok(report) => {
            set_baseline_version_only(&dir, report.version);
            for (key, rows) in &report.tables {
                println!(
                    "Flushed {rows} inlined row{} of {key} into {dir} data files (version {})",
                    plural(*rows as usize),
                    report.version,
                );
            }
            println!(
                "{} data file{} written in {:.3}s; the flushed rows' records stay as the \
                 change log",
                report.data_files,
                plural(report.data_files),
                t0.elapsed().as_secs_f64(),
            );
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake changes <dir> [table] [from] [to]`: render a version range's
/// row-level changes — the `ducklake_table_changes` shape (`snapshot_id`,
/// `rowid`, `change_type`, the table's columns), inclusive on both ends.
/// `from` defaults to version 1 and `to` to the current version; without a
/// table every table of the range is listed.
fn cmd_changes(arg: &str) {
    let usage = "Usage: .lake changes <dir> [table] [from] [to]{HINT}";
    let Some((dir, rest)) = take_token(arg) else {
        println!("{usage}");
        return;
    };
    let dir = unquote(&dir);
    let mut table: Option<String> = None;
    let mut from: Option<u64> = None;
    let mut to: Option<u64> = None;
    for token in rest.split_whitespace() {
        match token.parse::<u64>() {
            Ok(n) if from.is_none() => from = Some(n),
            Ok(n) if to.is_none() => to = Some(n),
            Ok(_) => {
                println!("{usage}");
                return;
            }
            Err(_) if table.is_none() => table = Some(flat_table_key(&unquote(token))),
            Err(_) => {
                println!("{usage}");
                return;
            }
        }
    }
    let lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let current = lake.current_version();
    let from = from.unwrap_or(1);
    let to = to.unwrap_or(current);
    let keys: Vec<String> = match &table {
        Some(table) => vec![table.clone()],
        None => match lake.tables() {
            Ok(tables) => tables.into_iter().map(|(key, _)| key).collect(),
            Err(e) => {
                cli_error!("{e}");
                return;
            }
        },
    };
    if keys.is_empty() {
        println!("(no tables)");
        return;
    }
    let mut any = false;
    for key in keys {
        let rows = match lake.table_changes(&key, from, Some(to)) {
            Ok(rows) => rows,
            Err(e) => {
                cli_error!("{e}");
                return;
            }
        };
        for row in rows {
            any = true;
            let rowid = match row.row_id {
                Some(id) => id.to_string(),
                None => "NULL".to_string(),
            };
            let values = if row.values.is_empty() {
                String::new()
            } else {
                format!(
                    "  {}",
                    row.values
                        .iter()
                        .map(render_change_value)
                        .collect::<Vec<_>>()
                        .join(" ")
                )
            };
            println!(
                "{key}  version {}  rowid {rowid}  {}{values}",
                row.version, row.change_type
            );
        }
    }
    if !any {
        println!("(no changes in versions {from}..={to})");
    }
}

/// One change-row value: VARCHARs quoted (so NULL is unambiguous), everything
/// else in its canonical display.
fn render_change_value(value: &pinkdb::Value) -> String {
    match value {
        pinkdb::Value::Varchar(s) => format!("'{}'", s.replace('\'', "''")),
        other => other.to_string(),
    }
}

/// `.lake add_data_files <dir> <table> <path> [path ...]`: register existing
/// data files of the lake as rows of a table, DuckLake's
/// `ducklake_add_data_files`. The paths are lake-relative `data/…` paths;
/// every file is validated (format, table identity, exact schema, readable
/// row count) before the version is published.
fn cmd_add_data_files(arg: &str) {
    let usage = "Usage: .lake add_data_files <dir> <table> <path> [path ...]{HINT}";
    let Some((dir, rest)) = take_token(arg) else {
        println!("{usage}");
        return;
    };
    let Some((table, rest)) = take_token(rest) else {
        println!("{usage}");
        return;
    };
    let paths: Vec<String> = rest
        .split_whitespace()
        .map(|p| unquote(p).to_string())
        .collect();
    if paths.is_empty() {
        println!("{usage}");
        return;
    }
    let dir = unquote(&dir);
    let table = flat_table_key(&unquote(&table));
    {
        let writables = writables();
        if writables.contains_key(&dir) {
            cli_error!(
                "{dir} is attached writable in this session; .lake commit (or .lake detach \
                 {dir}) it before registering files"
            );
            return;
        }
    }
    let mut lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let settings = commit_settings();
    match lake.add_data_files_with_settings(&table, &paths, &settings) {
        Ok(report) => {
            set_baseline_version_only(&dir, report.version);
            for (_, path, rows) in &report.files {
                println!(
                    "Registered {path} ({rows} row{}) as {table} in {dir} (version {})",
                    plural(*rows as usize),
                    report.version,
                );
            }
        }
        Err(e) => cli_error!("{e}"),
    }
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
    for ((location, tables), (_, loads, columns, files)) in lakes.into_iter().zip(stats) {
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
            "  decodes: {loads} load{}, {columns} column{}, {files} file{}",
            plural(loads as usize),
            plural(columns as usize),
            plural(files as usize)
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
    let lake = match open_lake(&dir) {
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
    let lake = match open_lake(&dir) {
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
                println!("  schema:  version {}", info.schema_version);
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
    let lake = match open_lake(&dir) {
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
    let lake = match open_lake(&dir) {
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
    if let Some(latest) = lake.snapshots().ok().and_then(|s| s.last().cloned()) {
        println!("  schema version:  {}", latest.schema_version);
    }
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

// ---------------------------------------------------------------------------
// Maintenance (wave 67): merge / rewrite / expire / cleanup / orphans, and
// the files / table_info / options / settings introspection.

/// The parsed shape every maintenance command shares: the location, bare
/// tokens (a table name, or flags like `all`/`dry-run`), and `key=value`
/// options.
struct MaintenanceArgs {
    dir: String,
    bare: Vec<String>,
    options: Vec<(String, String)>,
}

/// Parse `<dir> [bare…] [key=value…]` for one maintenance command; `usage`
/// is printed on any malformed token.
fn parse_maintenance_args(usage: &str, arg: &str) -> Option<MaintenanceArgs> {
    let Some((dir, mut rest)) = take_token(arg) else {
        println!("{usage}");
        return None;
    };
    let dir = unquote(&dir);
    let mut bare = Vec::new();
    let mut options = Vec::new();
    loop {
        let tail = rest.trim_start();
        if tail.is_empty() {
            break;
        }
        let Some((token, after)) = take_token(tail) else {
            break;
        };
        rest = after;
        let token = unquote(&token);
        match token.split_once('=') {
            Some((key, value)) => options.push((key.to_ascii_lowercase(), value.to_string())),
            None => bare.push(token),
        }
    }
    Some(MaintenanceArgs { dir, bare, options })
}

/// A bare table token → the flat catalog key (like `.lake partition`:
/// `t` means `main.t`; a dotted token is used as written).
fn flat_table_key(token: &str) -> String {
    if token.contains('.') {
        token.to_string()
    } else {
        format!("main.{token}")
    }
}

/// A refused-with-summary usage error for an unexpected bare token.
fn unexpected_token(usage: &str) {
    println!("{usage}");
}

/// `older_than=<iso8601>` → microseconds since the Unix epoch (the unit the
/// maintenance settings use; [`pinkdb::types::parse_iso8601_utc`] is the
/// same parser the engine's `AT (TIMESTAMP => …)` uses).
fn parse_time_option(key: &str, value: &str) -> Option<i64> {
    match pinkdb::types::parse_iso8601_utc(value.trim_matches('\'')) {
        Some(micros) => Some(micros),
        None => {
            cli_error!(
                "invalid {key}={value:?}: expected an ISO-8601 UTC timestamp like \
                 2026-10-17T00:00:00Z"
            );
            None
        }
    }
}

/// `versions=2,3` / `versions=[2,3]` → the version list.
fn parse_versions_option(value: &str) -> Option<Vec<u64>> {
    let inner = value.trim().trim_start_matches('[').trim_end_matches(']');
    let mut out = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        match part.parse::<u64>() {
            Ok(version) => out.push(version),
            Err(_) => {
                cli_error!("invalid version {part:?}: expected a positive integer");
                return None;
            }
        }
    }
    Some(out)
}

/// The shared refuse-when-attached-writable guard for the commands that
/// publish a version (a writable attachment's base would go stale; the
/// session's `.lake commit` must land or be detached first).
fn writable_refusal(dir: &str, what: &str) -> Option<String> {
    writables().contains_key(dir).then(|| {
        format!(
            "{dir} is attached writable in this session; .lake commit (or .lake detach \
                 {dir}) it before {what}"
        )
    })
}

/// `.lake merge <dir> [table] [min_file_size=N] [max_file_size=N]
/// [max_compacted_files=N]` — DuckLake's `ducklake_merge_adjacent_files`:
/// publish one new version whose tables reference consolidated data files,
/// never touching a published file. Files merge only when they share
/// partition values and schema epoch, have no positional deletes, and pass
/// the size filters; a group needs at least two files.
fn cmd_merge(arg: &str) {
    let usage = "Usage: .lake merge <dir> [table] [min_file_size=N] [max_file_size=N] \
                 [max_compacted_files=N]";
    let Some(args) = parse_maintenance_args(usage, arg) else {
        return;
    };
    if args.bare.len() > 1 {
        unexpected_token(usage);
        return;
    }
    let table = args.bare.first().map(|token| flat_table_key(token));
    let mut settings = MergeSettings::default();
    for (key, value) in &args.options {
        let parsed = value.parse::<u64>();
        match (key.as_str(), parsed) {
            ("min_file_size", Ok(v)) => settings.min_file_size = Some(v),
            ("max_file_size", Ok(v)) => settings.max_file_size = Some(v),
            ("max_compacted_files", Ok(v)) => settings.max_compacted_files = Some(v),
            (key @ ("min_file_size" | "max_file_size" | "max_compacted_files"), Err(_)) => {
                cli_error!("invalid {key}={value:?}: expected a non-negative integer");
                return;
            }
            _ => {
                unexpected_token(usage);
                return;
            }
        }
    }
    let dir = args.dir;
    if let Some(refusal) = writable_refusal(&dir, "merging its files") {
        cli_error!("{refusal}");
        return;
    }
    let mut lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let t0 = Instant::now();
    match lake.merge_adjacent_files(table.as_deref(), &settings) {
        Ok(report) if !report.committed => println!(
            "Nothing to merge in {dir} (no eligible file group: candidates must share \
             partition values and schema epoch, carry no deletes, and number at least two)"
        ),
        Ok(report) => {
            set_baseline_version_only(&dir, report.version);
            for table in &report.tables {
                println!(
                    "{}: merged {} file{} into {}",
                    table.table,
                    table.files_processed,
                    plural(table.files_processed as usize),
                    table.files_created
                );
            }
            println!(
                "Merged {} file{} into {} in {dir} (version {}, {} bytes written, {:.3}s)",
                report.files_processed(),
                plural(report.files_processed() as usize),
                report.files_created(),
                report.version,
                report.bytes_written,
                t0.elapsed().as_secs_f64(),
            );
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake rewrite <dir> [table] [delete_threshold=F] [max_compacted_files=N]`
/// — DuckLake's `ducklake_rewrite_data_files`: materialize the deletes of the
/// files whose delete ratio reaches the threshold (default 0.95) into fresh
/// data files, dropping their delete files from the new version.
fn cmd_rewrite(arg: &str) {
    let usage = "Usage: .lake rewrite <dir> [table] [delete_threshold=F] \
                 [max_compacted_files=N]";
    let Some(args) = parse_maintenance_args(usage, arg) else {
        return;
    };
    if args.bare.len() > 1 {
        unexpected_token(usage);
        return;
    }
    let table = args.bare.first().map(|token| flat_table_key(token));
    let mut settings = RewriteSettings::default();
    for (key, value) in &args.options {
        match key.as_str() {
            "delete_threshold" => match value.parse::<f64>() {
                Ok(v) if (0.0..=1.0).contains(&v) => settings.delete_threshold = v,
                _ => {
                    cli_error!(
                        "invalid delete_threshold={value:?}: expected a ratio between 0 and 1"
                    );
                    return;
                }
            },
            "max_compacted_files" => match value.parse::<u64>() {
                Ok(v) => settings.max_compacted_files = Some(v),
                Err(_) => {
                    cli_error!(
                        "invalid max_compacted_files={value:?}: expected a non-negative integer"
                    );
                    return;
                }
            },
            _ => {
                unexpected_token(usage);
                return;
            }
        }
    }
    let dir = args.dir;
    if let Some(refusal) = writable_refusal(&dir, "rewriting its files") {
        cli_error!("{refusal}");
        return;
    }
    let mut lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let t0 = Instant::now();
    match lake.rewrite_data_files(table.as_deref(), &settings) {
        Ok(report) if !report.committed => println!(
            "Nothing to rewrite in {dir} (no file's delete ratio reaches the threshold; the \
             default is 0.95 — pass delete_threshold=0 to rewrite any file with deletes)"
        ),
        Ok(report) => {
            set_baseline_version_only(&dir, report.version);
            for table in &report.tables {
                println!(
                    "{}: rewrote {} file{} into {}",
                    table.table,
                    table.files_processed,
                    plural(table.files_processed as usize),
                    table.files_created
                );
            }
            println!(
                "Rewrote {} file{} into {} in {dir} (version {}, {} bytes written, {:.3}s)",
                report.files_processed(),
                plural(report.files_processed() as usize),
                report.files_created(),
                report.version,
                report.bytes_written,
                t0.elapsed().as_secs_f64(),
            );
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake expire <dir> [versions=a,b,c] [older_than=<iso8601>] [keep=N]
/// [dry-run]` — DuckLake's `ducklake_expire_snapshots`: remove old
/// `snapshots/<v>.json` + `commits/<v>.json`, never the current version.
/// `keep=N` always retains the newest N versions (a pinkdb addition);
/// `versions` and `older_than` are mutually exclusive (DuckLake's rule).
fn cmd_expire(arg: &str) {
    let usage = "Usage: .lake expire <dir> [versions=a,b,c] [older_than=<iso8601>] [keep=N] \
                 [dry-run]";
    let Some(args) = parse_maintenance_args(usage, arg) else {
        return;
    };
    let mut settings = ExpireSettings::default();
    let mut bare_ok = true;
    for token in &args.bare {
        match token.to_ascii_lowercase().as_str() {
            "dry-run" | "dry_run" | "dryrun" => settings.dry_run = true,
            _ => bare_ok = false,
        }
    }
    if !bare_ok {
        unexpected_token(usage);
        return;
    }
    for (key, value) in &args.options {
        match key.as_str() {
            "versions" => match parse_versions_option(value) {
                Some(versions) => settings.versions = Some(versions),
                None => return,
            },
            "older_than" => match parse_time_option("older_than", value) {
                Some(micros) => settings.older_than_micros = Some(micros),
                None => return,
            },
            "keep" => match value.parse::<u64>() {
                Ok(v) => settings.retain_last = Some(v),
                Err(_) => {
                    cli_error!("invalid keep={value:?}: expected a non-negative integer");
                    return;
                }
            },
            _ => {
                unexpected_token(usage);
                return;
            }
        }
    }
    let dir = args.dir;
    let mut lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    match lake.expire_snapshots(&settings) {
        Ok(report) if report.versions.is_empty() => {
            if settings.dry_run {
                println!("Nothing to expire in {dir} (no version matched the rule)");
            } else {
                println!("Nothing expired in {dir} (no version matched the rule)");
            }
        }
        Ok(report) => {
            for info in &report.versions {
                println!(
                    "{} version {} ({})",
                    if report.dry_run {
                        "would expire"
                    } else {
                        "expired"
                    },
                    info.version,
                    info.time,
                );
            }
            if report.dry_run {
                println!(
                    "{} snapshot{} would be expired ({} bytes); re-run without dry-run to \
                     delete them",
                    report.versions.len(),
                    plural(report.versions.len()),
                    report.bytes,
                );
            } else {
                println!(
                    "Expired {} snapshot{} from {dir} ({} bytes freed; earlier versions can no \
                     longer be opened)",
                    report.versions.len(),
                    plural(report.versions.len()),
                    report.bytes,
                );
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// Which cleanup flavor a command line is.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CleanupKind {
    /// `ducklake_cleanup_old_files` — `.lake cleanup`.
    Old,
    /// `ducklake_delete_orphaned_files` — `.lake orphans` (the same scan:
    /// pinkdb has no scheduled-deletion queue, see the `pink-lake`
    /// maintenance module docs).
    Orphaned,
}

/// `.lake cleanup <dir> [older_than=<iso8601>] [all] [dry-run] [confirm]` /
/// `.lake orphans …` — delete `data/` objects no retained version references,
/// behind an age gate (default 2 days, DuckLake's `delete_older_than`) and an
/// explicit `dry-run`/`confirm`.
fn cmd_cleanup(arg: &str, kind: CleanupKind) {
    let (sub, what) = match kind {
        CleanupKind::Old => ("cleanup", "cleaned up"),
        CleanupKind::Orphaned => ("orphans", "deleted"),
    };
    let usage =
        format!("Usage: .lake {sub} <dir> [older_than=<iso8601>] [all] [dry-run] [confirm]");
    let Some(args) = parse_maintenance_args(&usage, arg) else {
        return;
    };
    let mut settings = CleanupSettings::default();
    let mut bare_ok = true;
    for token in &args.bare {
        match token.to_ascii_lowercase().as_str() {
            "all" => settings.all = true,
            "dry-run" | "dry_run" | "dryrun" => settings.dry_run = true,
            "confirm" => settings.confirm = true,
            _ => bare_ok = false,
        }
    }
    if !bare_ok {
        unexpected_token(&usage);
        return;
    }
    for (key, value) in &args.options {
        match key.as_str() {
            "older_than" => match parse_time_option("older_than", value) {
                Some(micros) => settings.older_than_micros = Some(micros),
                None => return,
            },
            _ => {
                unexpected_token(&usage);
                return;
            }
        }
    }
    let dir = args.dir;
    let mut lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let result = match kind {
        CleanupKind::Old => lake.cleanup_old_files(&settings),
        CleanupKind::Orphaned => lake.delete_orphaned_files(&settings),
    };
    match result {
        Ok(report) if report.paths.is_empty() => {
            println!("Nothing to {sub} in {dir} (no object is unreferenced)");
        }
        Ok(report) => {
            for path in &report.paths {
                if report.dry_run {
                    println!("would delete {path}");
                } else {
                    println!("deleted {path}");
                }
            }
            if report.dry_run {
                println!(
                    "{} object{} would be {} ({} bytes); re-run with confirm to delete",
                    report.paths.len(),
                    plural(report.paths.len()),
                    what,
                    report.bytes,
                );
            } else {
                println!(
                    "Deleted {} object{} from {dir} ({} bytes freed; still-referenced versions \
                     are untouched)",
                    report.paths.len(),
                    plural(report.paths.len()),
                    report.bytes,
                );
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake files <dir> [table] [all] [version=N]` — DuckLake's
/// `ducklake_list_files`: one line per data file (and per delete-file
/// pairing). `all` also lists `data/` objects no retained version references
/// (marked `unreferenced`), exactly the cleanup scan.
fn cmd_files(arg: &str) {
    let usage = "Usage: .lake files <dir> [table] [all] [version=N]";
    let Some(args) = parse_maintenance_args(usage, arg) else {
        return;
    };
    let mut table: Option<String> = None;
    let mut all = false;
    let mut bare_ok = true;
    for token in &args.bare {
        if token.eq_ignore_ascii_case("all") {
            all = true;
        } else if table.is_none() {
            table = Some(flat_table_key(token));
        } else {
            bare_ok = false;
        }
    }
    if !bare_ok {
        unexpected_token(usage);
        return;
    }
    let mut version: Option<u64> = None;
    for (key, value) in &args.options {
        match key.as_str() {
            "version" => match value.parse::<u64>() {
                Ok(v) => version = Some(v),
                Err(_) => {
                    cli_error!("invalid version={value:?}: expected a positive integer");
                    return;
                }
            },
            _ => {
                unexpected_token(usage);
                return;
            }
        }
    }
    let dir = args.dir;
    let lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    match lake.list_files(table.as_deref(), version, all) {
        Ok(files) if files.is_empty() => println!("(no files)"),
        Ok(files) => {
            let referenced = files.iter().filter(|info| info.referenced).count();
            for info in &files {
                print_file_info(info);
            }
            if all {
                println!(
                    "{} file{} ({} referenced, {} unreferenced)",
                    files.len(),
                    plural(files.len()),
                    referenced,
                    files.len() - referenced,
                );
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// One `.lake files` line: the path, then the fields this file records (or,
/// for an unreferenced object, just the size).
fn print_file_info(info: &pink_lake::LakeFileInfo) {
    let mut parts: Vec<String> = Vec::new();
    if let Some(table) = &info.table {
        parts.push(format!("table={table}"));
    }
    if let Some(rows) = info.rows {
        parts.push(format!("rows={rows}"));
    }
    if let Some(size) = info.size_bytes {
        parts.push(format!("size={size}"));
    }
    if let Some(schema_version) = info.schema_version {
        parts.push(format!("schema_version={schema_version}"));
    }
    if let Some(partition) = &info.partition {
        parts.push(format!("partition={}", partition_text(partition)));
    }
    if let Some(delete) = &info.delete_file {
        let rows = info.delete_file_rows.unwrap_or(0);
        parts.push(format!(
            "delete={delete} ({rows} position{})",
            plural(rows as usize)
        ));
    }
    if !info.referenced {
        parts.push("unreferenced".to_string());
    }
    println!("{}: {}", info.path, parts.join(" "));
}

/// `{region=east, day=NULL}`-style text for a file's partition values.
fn partition_text(partition: &BTreeMap<String, Option<String>>) -> String {
    partition
        .iter()
        .map(|(key, value)| match value {
            Some(value) => format!("{key}={value}"),
            None => format!("{key}=NULL"),
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// `.lake table_info <dir>` — DuckLake's `ducklake_table_info`: per-table
/// file/delete-file counts and sizes, rows, schema epoch and partition spec.
fn cmd_table_info(arg: &str) {
    let usage = "Usage: .lake table_info <dir>";
    let Some(args) = parse_maintenance_args(usage, arg) else {
        return;
    };
    if !args.bare.is_empty() || !args.options.is_empty() {
        unexpected_token(usage);
        return;
    }
    let dir = args.dir;
    let lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    match lake.table_info() {
        Ok(tables) if tables.is_empty() => println!("(no tables)"),
        Ok(tables) => {
            for info in tables {
                let partition = if info.partition_by.is_empty() {
                    String::new()
                } else {
                    format!(", partitioned by ({})", info.partition_by.join(", "))
                };
                let inlining = if info.inlined_history > 0 {
                    format!(
                        ", {} inlined row{} ({} recorded{})",
                        info.inlined_rows,
                        plural(info.inlined_rows as usize),
                        info.inlined_history,
                        match info.inlined_limit {
                            Some(limit) => format!(", limit {limit}"),
                            None => String::new(),
                        }
                    )
                } else {
                    String::new()
                };
                println!(
                    "{}: {} file{}, {}, {} delete file{}, {}, {} row{}, schema version {}{}{}",
                    info.table,
                    info.file_count,
                    plural(info.file_count as usize),
                    bytes_text(info.file_size_bytes),
                    info.delete_file_count,
                    plural(info.delete_file_count as usize),
                    bytes_text(info.delete_file_size_bytes),
                    info.rows,
                    plural(info.rows as usize),
                    info.schema_version,
                    partition,
                    inlining,
                );
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `123 bytes`, or `(size unknown)` when no stat could read it.
fn bytes_text(bytes: Option<u64>) -> String {
    match bytes {
        Some(bytes) => format!("{bytes} bytes"),
        None => "(size unknown)".to_string(),
    }
}

/// `.lake options <dir>` — DuckLake's `ducklake_options`: the catalog's
/// read-only facts (pinkdb has no settings store, so these are facts, not
/// knobs).
fn cmd_options(arg: &str) {
    let usage = "Usage: .lake options <dir>";
    let Some(args) = parse_maintenance_args(usage, arg) else {
        return;
    };
    if !args.bare.is_empty() || !args.options.is_empty() {
        unexpected_token(usage);
        return;
    }
    let dir = args.dir;
    let lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    match lake.options() {
        Ok(options) => {
            for option in options {
                println!(
                    "{}: {}  ({})",
                    option.name, option.value, option.description
                );
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake settings <dir>` — DuckLake's `ducklake_settings` trio.
fn cmd_settings(arg: &str) {
    let usage = "Usage: .lake settings <dir>";
    let Some(args) = parse_maintenance_args(usage, arg) else {
        return;
    };
    if !args.bare.is_empty() || !args.options.is_empty() {
        unexpected_token(usage);
        return;
    }
    let dir = args.dir;
    let lake = match open_lake(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let settings = lake.settings();
    println!("catalog_type:      {}", settings.catalog_type);
    println!("extension_version: {}", settings.extension_version);
    println!("data_path:         {}", settings.data_path);
}
