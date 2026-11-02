//! `.lake` dot-commands: snapshot the in-memory store into a pinkdb lake and
//! load versions back.
//!
//! Built with the `pinklake` feature (`.pink` data files, catalog
//! format_version 7). Attaching a lake is SQL since wave 124 — `ATTACH
//! 'lake:<catalog>' AS name [(READ_ONLY)]`, `CREATE DATABASE name (TYPE
//! LAKE …)`, `DETACH name` — through the opener this build registers.
//!
//! The lake itself lives in the `pink-lake` crate; this module is only the
//! REPL surface. A lake is a catalog (`pink:`, `sqlite:` or `postgres://`)
//! plus data on local disk or, with the `s3` feature, `s3://bucket/prefix`
//! (credentials/endpoint from the usual `AWS_*` environment; a plain-http
//! test endpoint additionally needs `PINKDB_S3_ALLOW_HTTP`).
//!
//! # SQL catalogs (wave 96a)
//!
//! With the `catalog-sqlite` feature a lake's metadata may live in a SQLite
//! file instead of the data location: `<catalog>` is then `sqlite:<path>` and
//! `.lake save <catalog> data <dir|s3://…>` gives the data location once, at
//! create (it is recorded in the catalog). Every later command needs only the
//! catalog URL; an explicit `data <location>` on `.lake open` overrides the
//! recorded one for that session (a moved directory or bucket).
//! Without the feature a `sqlite:` URL is the library's clear rebuild hint.
//! The object-store layout is unchanged when no SQL catalog is used.
//!
//! `.lake open` replaces the in-memory store:
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
//! # Lake databases (wave 124)
//!
//! `.lake attach`, `.lake detach`, `.lake attached` and `.lake commit` are
//! gone: a lake is a database (see `pinkdb::lake_db`). A writable lake
//! database publishes every committed statement as one incremental lake
//! version; a read-only one is the lazy, statistics-first attachment. The
//! maintenance commands below refuse a lake whose engine lease a writable
//! lake database holds.
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
//! per partition value set** — a lake database's commit of appended rows and a
//! `.lake save` of the table alike — and the catalog records each file's
//! values; existing files are never rewritten (DuckLake semantics).
//!
//! Read-only attaches prune: a `WHERE` carrying simple comparisons (`col =
//! literal`, ranges, `IS [NOT] NULL`) over zoned/partitioned columns decodes
//! only the data files that can still match.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use pink_lake::replication::{self, JobSpec, Source, Supervisor};
use pink_lake::{CommitSettings, Lake};
use pinkdb::{PinkDb, sanitize_terminal};

use crate::{plural, take_token, unquote};

/// A location as it may appear in output: a Postgres catalog URL loses its
/// password (every other location is unchanged). Callers keep the raw string
/// for the actual open/save/commit and print this one.
fn shown(location: &str) -> String {
    pink_lake::redact_location(location)
}

/// The key from `PINKDB_KEY`, the engine's convention (a non-empty value
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
    open_lake_with_data(dir, None)
}

/// [`open_lake`] with an optional explicit data location (wave 96a).
///
/// A SQL catalog (`sqlite:<path>`) records its data location, so `data`
/// overrides it for this session without rewriting the catalog; without
/// `data` the plain open reads the recorded location. A `data` on a plain
/// location is refused by the library (it has no separate data root).
fn open_lake_with_data(catalog: &str, data: Option<&str>) -> pink_lake::Result<Lake> {
    match (lake_key(), data) {
        (Some(key), Some(data)) => Lake::open_with_catalog_with_key(catalog, Some(data), key),
        (None, Some(data)) => Lake::open_with_catalog(catalog, Some(data)),
        (Some(key), None) => Lake::open_url_with_key(catalog, key),
        (None, None) => Lake::open_url(catalog),
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

/// [`open_or_create_lake`] with a data location for a SQL catalog.
///
/// A split lake is always created through `create_with_catalog` (there is no
/// `open_or_create` variant in the library because the first create needs the
/// data location, not every open). The open attempt distinguishes a fresh
/// catalog (`NotALake`) from every other failure, so a wrong key or a corrupt
/// catalog is never silently masked by a create. Without `data` a fresh SQLite
/// catalog gets DuckLake's default `<catalog>.files/` data root (the library's
/// `open_or_create_url`); Postgres still explains that a create needs `data`.
fn open_or_create_lake_with_data(catalog: &str, data: Option<&str>) -> pink_lake::Result<Lake> {
    let Some(data) = data else {
        return open_or_create_lake(catalog);
    };
    match open_lake_with_data(catalog, Some(data)) {
        Ok(lake) => Ok(lake),
        Err(pink_lake::LakeError::NotALake { .. }) => match lake_key() {
            Some(key) => Lake::create_with_catalog_with_key(catalog, data, key),
            None => Lake::create_with_catalog(catalog, data),
        },
        Err(e) => Err(e),
    }
}

/// The one-line pointer to object-storage and SQL-catalog locations appended
/// to usage text: each accepted location shape is named only when the build
/// has its feature, so the hint never offers something the binary refuses.
#[cfg(all(
    feature = "s3",
    feature = "catalog-sqlite",
    feature = "catalog-postgres"
))]
const HINT: &str = "   (locations may be s3://bucket/prefix, pink:<path> [data <dir|s3://…>], \
                    sqlite:<path> [data <dir|s3://…>], or postgres://… data <dir>)";
#[cfg(all(
    feature = "s3",
    feature = "catalog-sqlite",
    not(feature = "catalog-postgres")
))]
const HINT: &str = "   (locations may be s3://bucket/prefix, pink:<path> [data <dir|s3://…>], \
                    or sqlite:<path> [data <dir|s3://…>])";
#[cfg(all(
    feature = "s3",
    not(feature = "catalog-sqlite"),
    feature = "catalog-postgres"
))]
const HINT: &str = "   (locations may be s3://bucket/prefix, pink:<path> [data <dir|s3://…>], \
                    or postgres://… data <dir>)";
#[cfg(all(
    feature = "s3",
    not(feature = "catalog-sqlite"),
    not(feature = "catalog-postgres")
))]
const HINT: &str = "   (locations may be s3://bucket/prefix, pink:<path> [data <dir|s3://…>])";
#[cfg(all(
    not(feature = "s3"),
    feature = "catalog-sqlite",
    feature = "catalog-postgres"
))]
const HINT: &str = "   (an s3://bucket/prefix location needs the `s3` feature; pink:<path> or \
                    sqlite:<path> [data <dir>]; postgres://… catalogs need `data <dir>`)";
#[cfg(all(
    not(feature = "s3"),
    feature = "catalog-sqlite",
    not(feature = "catalog-postgres")
))]
const HINT: &str = "   (an s3://bucket/prefix location needs the `s3` feature; a pink:<path> \
                    or sqlite:<path> catalog accepts optional `data <dir>`)";
#[cfg(all(
    not(feature = "s3"),
    not(feature = "catalog-sqlite"),
    feature = "catalog-postgres"
))]
const HINT: &str = "   (an s3://bucket/prefix location needs the `s3` feature; a pink:<path> \
                    catalog accepts optional `data <dir>`; postgres://… needs `data <dir>`)";
#[cfg(all(
    not(feature = "s3"),
    not(feature = "catalog-sqlite"),
    not(feature = "catalog-postgres")
))]
const HINT: &str = "   (an s3://bucket/prefix location needs the `s3` feature; a pink:<path> \
                    catalog accepts optional `data <dir>`)";

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

/// The session's `.lake format <id>` choice: the data-file format every
/// subsequent full save / commit / inlined flush writes. `None` keeps the
/// build default (`.pink`). Per session, not persisted (a lake's existing
/// files keep their entry's format).
static WRITE_FORMAT: Mutex<Option<String>> = Mutex::new(None);

/// The session's `.lake format <id> for <db.table>` choices (wave 88):
/// per-table data-file formats keyed by the flat catalog key. Applied to every
/// handle about to write, like [`WRITE_FORMAT`]; not persisted.
static TABLE_FORMATS: Mutex<BTreeMap<String, String>> = Mutex::new(BTreeMap::new());

/// The session's replication supervisors (wave 89), one per resolved
/// destination location. `.lake replicate start` inserts a supervisor and
/// `.lake replicate stop` removes and joins it; a session that exits with one
/// still running ends the process (and its thread).
static SUPERVISORS: Mutex<BTreeMap<String, Supervisor>> = Mutex::new(BTreeMap::new());

fn supervisors() -> MutexGuard<'static, BTreeMap<String, Supervisor>> {
    SUPERVISORS.lock().unwrap_or_else(|p| p.into_inner())
}

/// The resolved location that keys [`SUPERVISORS`] (the same string the
/// supervisor's registry and `catch_up`'s status hook use).
fn supervisor_key(dir: &str) -> Option<String> {
    // The supervisor registry is keyed by the location it was started with.
    open_lake(dir).ok().map(|_| dir.to_string())
}

fn chosen_write_format() -> Option<String> {
    WRITE_FORMAT
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}

fn table_format_choices() -> BTreeMap<String, String> {
    TABLE_FORMATS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}

/// The build's default write format, matching `pink-lake`'s choice.
fn current_write_format_id() -> String {
    chosen_write_format().unwrap_or_else(|| "pink".to_string())
}

/// The ids this binary has providers for. The lake registry is keyed and has
/// no listing API; these are the ids any pinkdb build can register (`pink`
/// always, `parquet` with `parquetlake`).
fn registered_format_ids() -> Vec<&'static str> {
    ["pink", "parquet"]
        .into_iter()
        .filter(|id| pink_lake::format::lake_format(id).is_some())
        .collect()
}

/// Apply the session's chosen formats to a handle that is about to write:
/// the handle-wide `.lake format <id>` choice first, then every per-table
/// `.lake format <id> for <db.table>` choice (wave 88).
fn apply_write_format(lake: &mut Lake) -> pink_lake::Result<()> {
    if let Some(id) = chosen_write_format() {
        lake.set_write_format(&id)?;
    }
    for (key, id) in table_format_choices() {
        let (db, table) = key.split_once('.').unwrap_or(("main", key.as_str()));
        lake.set_table_format(db, table, &id)?;
    }
    Ok(())
}

/// The commit retry settings of a lake commit, from the environment with
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
        "replicate" => cmd_replicate(db, rest),
        "partition" => cmd_partition(db, rest),
        "inlining" => cmd_inlining(rest),
        "format" => cmd_format(rest),
        "rewrite" => cmd_rewrite(rest),
        // Wave 124: attaching a lake is SQL now.
        "attach" | "detach" | "attached" | "commit" => cli_error!(
            ".lake {sub} is gone: lakes are databases now. Use ATTACH 'lake:<catalog>' AS name \
             [(READ_ONLY)], DETACH name, SHOW DATABASES; a writable lake database commits with \
             every statement"
        ),
        "versions" => cmd_versions(rest),
        "snapshots" => cmd_snapshots(rest),
        "tables" => cmd_tables(rest),
        "info" => cmd_info(rest),
        "" => println!(
            "Usage: .lake <save|open|replicate|partition|inlining|format|rewrite|versions|\
             snapshots|tables|info> ..."
        ),
        other => cli_error!("unknown .lake subcommand \"{other}\". Try .help"),
    }
}

/// The `data <location>` option in usage text (the same rule as [`HINT`]).
macro_rules! data_opt {
    () => {
        " [data <dir|s3://…>]"
    };
}

/// The `.lake save` usage line (kept in one place: the argument parser and
/// both of its error paths print it).
const SAVE_USAGE: &str = concat!(
    "Usage: .lake save <catalog>",
    data_opt!(),
    " [force] [author '<name>'] [message '<text>']"
);

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
/// store (a directory or an `s3://bucket/prefix` URL with the `s3` feature;
/// a `sqlite:<path>` catalog with `data <location>` when `catalog-sqlite` is
/// built). `author '<name>'` / `message '<text>'` record the version's commit
/// metadata, like DuckLake's `ducklake_set_commit_message`.
fn cmd_save(db: &PinkDb, arg: &str) {
    let Some((dir, mut rest)) = take_token(arg) else {
        println!("{SAVE_USAGE}{HINT}");
        return;
    };
    let mut force = false;
    let mut author: Option<String> = None;
    let mut message: Option<String> = None;
    let mut data: Option<String> = None;
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
            "data" => {
                let Some((value, after)) = take_value(after) else {
                    println!("{SAVE_USAGE}{HINT}");
                    return;
                };
                data = Some(unquote(&value));
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
    let dir_shown = shown(&dir);
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
    let mut lake = match open_or_create_lake_with_data(&dir, data.as_deref()) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    if let Err(e) = apply_write_format(&mut lake) {
        cli_error!("{e}");
        return;
    }
    if let Some(base) = baseline(&lake) {
        let current = lake.current_version();
        if current != base && !force {
            cli_error!(
                "write conflict: lake {dir_shown} moved from version {base} to {current} \
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

/// One warning line per table whose data-file format this build cannot read
/// (wave 88, rule 5): the table is listed/skipped and any read of it fails.
/// `outcome` completes the sentence (`it was skipped`, `reading it will fail`).
fn warn_unavailable_tables(lake: &Lake, version: Option<u64>, outcome: &str) {
    let tables = match lake.unavailable_tables_at(version) {
        Ok(tables) => tables,
        Err(_) => return,
    };
    for table in tables {
        println!(
            "warning: table {} uses data-file format {:?}, which this build cannot read; \
             {outcome}",
            sanitize_terminal(&table.table),
            sanitize_terminal(&table.format),
        );
    }
}

/// `.lake open <catalog> [data <dir|s3://…>] [version]`: replace the
/// in-memory store with the lake's latest (or the given) version.
fn cmd_open(db: &mut PinkDb, arg: &str) {
    const USAGE: &str = concat!("Usage: .lake open <catalog>", data_opt!(), " [version]");
    let Some((dir, mut rest)) = take_token(arg) else {
        println!("{USAGE}{HINT}");
        return;
    };
    let dir = unquote(&dir);
    let dir_shown = shown(&dir);
    let mut data: Option<String> = None;
    let mut version: Option<u64> = None;
    loop {
        let tail = rest.trim_start();
        if tail.is_empty() {
            break;
        }
        let Some((token, after)) = take_token(tail) else {
            break;
        };
        if token.eq_ignore_ascii_case("data") {
            let Some((value, after)) = take_value(after) else {
                println!("{USAGE}{HINT}");
                return;
            };
            data = Some(unquote(&value));
            rest = after;
        } else {
            match token.parse::<u64>() {
                Ok(v) if version.is_none() => {
                    version = Some(v);
                    rest = after;
                }
                _ => {
                    println!("{USAGE}{HINT}");
                    return;
                }
            }
        }
    }
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
    let lake = match open_lake_with_data(&dir, data.as_deref()) {
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
            println!(
                "Opened lake version {shown} in {:.3}s ({tables} table{}) from {dir_shown}",
                t0.elapsed().as_secs_f64(),
                plural(tables)
            );
            warn_unavailable_tables(&lake, version, "it was skipped");
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
    let dir_shown = shown(&dir);
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
    if let Some(refusal) = writable_refusal(&dir, "changing its partition specification") {
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
                println!(
                    "Table {} in {dir_shown} has no partition specification (nothing to clear)",
                    sanitize_terminal(&key)
                );
            } else {
                println!(
                    "Table {} in {dir_shown} is already partitioned by ({}); nothing published",
                    sanitize_terminal(&key),
                    keys.iter()
                        .map(|k| sanitize_terminal(k))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }
        Ok(report) => {
            set_baseline_version_only(&dir, report.version);
            if keys.is_empty() {
                println!(
                    "Cleared the partition specification of {} in {dir_shown} (version {} in \
                     {:.3}s); existing files keep their values",
                    sanitize_terminal(&key),
                    report.version,
                    t0.elapsed().as_secs_f64(),
                );
            } else {
                println!(
                    "Partitioned {} by ({}) in {dir_shown} (version {} in {:.3}s; new writes \
                     split into one data file per {} value)",
                    sanitize_terminal(&key),
                    keys.iter()
                        .map(|k| sanitize_terminal(k))
                        .collect::<Vec<_>>()
                        .join(", "),
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

/// `.lake rewrite <dir> <table> format <id>`: convert every data file of
/// one table to another registered format in a single maintenance version.
/// DuckLake has no format-conversion function, so this stays a dot-command
/// (the rewrite knob itself is `CALL ducklake_rewrite_data_files(...)`).
fn cmd_rewrite(arg: &str) {
    let usage = "Usage: .lake rewrite <dir> <table> format <id>";
    let Some((dir, rest)) = take_token(arg) else {
        println!("{usage}");
        return;
    };
    let Some((table, rest)) = take_token(rest) else {
        println!("{usage}");
        return;
    };
    let Some((keyword, rest)) = take_token(rest) else {
        println!("{usage}");
        return;
    };
    let id = rest.trim();
    if !keyword.eq_ignore_ascii_case("format")
        || id.is_empty()
        || id.split_whitespace().count() != 1
    {
        println!("{usage}");
        return;
    }
    let dir = unquote(&dir);
    let dir_shown = shown(&dir);
    let table = flat_table_key(&unquote(&table));
    let (db, name) = table
        .split_once('.')
        .expect("flat_table_key yields a db.table key");
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
    match lake.rewrite_table_format(db, name, id) {
        Ok(report) if !report.committed => println!(
            "Nothing to convert in {dir_shown}: table \"{table}\" already uses format \"{id}\""
        ),
        Ok(report) => {
            set_baseline_version_only(&dir, report.version);
            println!(
                "Converted {table} to format \"{id}\" ({} file{} into {}, version {}, \
                 {} bytes written, {:.3}s)",
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
    let dir_shown = shown(&dir);
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
    if let Some(refusal) = writable_refusal(&dir, "changing its inlining setting") {
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
    let settings = commit_settings();
    match lake.set_inlining_limit(&table, limit, &settings) {
        Ok(report) if !report.committed => {
            println!(
                "Table {} in {dir_shown} already has that inlining limit; nothing published",
                sanitize_terminal(&table)
            );
        }
        Ok(report) => {
            set_baseline_version_only(&dir, report.version);
            match limit {
                Some(limit) => println!(
                    "Table {} in {dir_shown}: commits appending at most {limit} row{} are stored \
                     in the catalog metadata (version {}); 0 disables inlining",
                    sanitize_terminal(&table),
                    if limit == 1 { "" } else { "s" },
                    report.version,
                ),
                None => println!(
                    "Cleared the inlining limit of {} in {dir_shown} (version {}); the \
                     handle's data_inlining_row_limit default applies",
                    sanitize_terminal(&table),
                    report.version,
                ),
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

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
                println!("  time:    {}", sanitize_terminal(&info.time));
                if let Some(author) = &info.author {
                    println!("  author:  {}", sanitize_terminal(author));
                }
                if let Some(message) = &info.message {
                    println!("  message: {}", sanitize_terminal(message));
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
                        let names = |list: &[String]| {
                            list.iter()
                                .map(|n| sanitize_terminal(n))
                                .collect::<Vec<_>>()
                                .join(", ")
                        };
                        let mut parts = Vec::new();
                        if !changes.tables_created.is_empty() {
                            parts.push(format!("created: {}", names(&changes.tables_created)));
                        }
                        if !changes.tables_dropped.is_empty() {
                            parts.push(format!("dropped: {}", names(&changes.tables_dropped)));
                        }
                        if !changes.tables_rewritten.is_empty() {
                            parts.push(format!("rewritten: {}", names(&changes.tables_rewritten)));
                        }
                        let mut rows: Vec<String> = changes
                            .rows_added
                            .iter()
                            .map(|(table, n)| format!("{} +{n}", sanitize_terminal(table)))
                            .collect();
                        rows.extend(
                            changes
                                .rows_removed
                                .iter()
                                .map(|(table, n)| format!("{} -{n}", sanitize_terminal(table))),
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

/// Run a `.lake` command for a served client (the MCP bridge's `lake_*`
/// tools) and return the answer as agent Markdown.
///
/// Only the catalog readers are served. Maintenance (`commit`, `merge`,
/// `expire`, …) changes a catalog that no session owns, so it stays a CLI
/// operation even in an `allow-writes` session; `read_only` only words the
/// refusal.
pub(crate) fn lake_markdown(
    _db: &mut PinkDb,
    text: &str,
    read_only: bool,
) -> Result<String, String> {
    let rest = text.trim().strip_prefix(".lake").unwrap_or(text).trim();
    let (command, args) = match rest.split_once(char::is_whitespace) {
        Some((command, args)) => (command, args.trim()),
        None => (rest, ""),
    };
    match command {
        "tables" => {
            let catalog = one_token(".lake tables", args)?;
            let lake = open_lake(&catalog).map_err(lake_error)?;
            let tables = lake.tables().map_err(lake_error)?;
            let rows = tables
                .into_iter()
                .map(|(name, rows)| vec![text_value(name), count_value(rows)])
                .collect();
            Ok(catalog_table(
                &["table", "rows"],
                &[pinkdb::DataType::Varchar, pinkdb::DataType::Bigint],
                rows,
            ))
        }
        "versions" => {
            let catalog = one_token(".lake versions", args)?;
            let lake = open_lake(&catalog).map_err(lake_error)?;
            let current = lake.current_version();
            let rows = lake
                .versions()
                .map_err(lake_error)?
                .into_iter()
                .map(|version| {
                    vec![
                        count_value(version),
                        pinkdb::Value::Boolean(version == current),
                    ]
                })
                .collect();
            Ok(catalog_table(
                &["version", "current"],
                &[pinkdb::DataType::Bigint, pinkdb::DataType::Boolean],
                rows,
            ))
        }
        "changes" => {
            let (catalog, table, from, to) = change_args(args)?;
            let lake = open_lake(&catalog).map_err(lake_error)?;
            let to = to.unwrap_or_else(|| lake.current_version());
            let keys = match table {
                Some(table) => vec![table],
                None => lake
                    .tables()
                    .map_err(lake_error)?
                    .into_iter()
                    .map(|(key, _)| key)
                    .collect(),
            };
            let mut rows = Vec::new();
            for key in keys {
                for change in lake
                    .table_changes(&key, from, Some(to))
                    .map_err(lake_error)?
                {
                    rows.push(vec![
                        text_value(key.clone()),
                        count_value(change.version),
                        match change.row_id {
                            Some(id) => count_value(id),
                            None => pinkdb::Value::Null,
                        },
                        text_value(change.change_type.as_str()),
                        text_value(
                            change
                                .values
                                .iter()
                                .map(|value| value.to_string())
                                .collect::<Vec<_>>()
                                .join(", "),
                        ),
                    ]);
                }
            }
            Ok(catalog_table(
                &["table", "version", "row_id", "change", "values"],
                &[
                    pinkdb::DataType::Varchar,
                    pinkdb::DataType::Bigint,
                    pinkdb::DataType::Bigint,
                    pinkdb::DataType::Varchar,
                    pinkdb::DataType::Varchar,
                ],
                rows,
            ))
        }
        other => {
            let why = if read_only {
                "read-only session: lake catalogs are read-only here"
            } else {
                "lake maintenance stays a CLI operation: a served session only reads catalogs"
            };
            Err(pinkdb::agent::error_json(
                &format!(
                    "\".lake {other}\" is not available over MCP ({why}); \
                     served commands are tables, versions and changes"
                ),
                "permission",
                None,
            ))
        }
    }
}

/// One catalog token, unquoted.
fn one_token(usage: &str, args: &str) -> Result<String, String> {
    let Some((token, rest)) = take_token(args) else {
        return Err(pinkdb::agent::error_json(
            &format!("usage: {usage} <catalog>"),
            "parameter",
            None,
        ));
    };
    if !rest.trim().is_empty() {
        return Err(pinkdb::agent::error_json(
            &format!("usage: {usage} <catalog>"),
            "parameter",
            None,
        ));
    }
    Ok(unquote(&token))
}

/// The `.lake changes` arguments: catalog, optional table, optional versions.
fn change_args(args: &str) -> Result<(String, Option<String>, u64, Option<u64>), String> {
    let Some((catalog, rest)) = take_token(args) else {
        return Err(pinkdb::agent::error_json(
            "usage: .lake changes <catalog> [table] [from] [to]",
            "parameter",
            None,
        ));
    };
    let usage = || {
        pinkdb::agent::error_json(
            "usage: .lake changes <catalog> [table] [from] [to]",
            "parameter",
            None,
        )
    };
    let mut table = None;
    let mut from = None;
    let mut to = None;
    for token in rest.split_whitespace() {
        match token.parse::<u64>() {
            Ok(n) if from.is_none() => from = Some(n),
            Ok(n) if to.is_none() => to = Some(n),
            Ok(_) => return Err(usage()),
            Err(_) if table.is_none() => table = Some(flat_table_key(&unquote(token))),
            Err(_) => return Err(usage()),
        }
    }
    Ok((unquote(&catalog), table, from.unwrap_or(1), to))
}

/// A lake error as the client receives it: the library's message, execution
/// kind.
fn lake_error(error: pink_lake::LakeError) -> String {
    pinkdb::agent::error_json(&error.to_string(), "execution", None)
}

fn text_value(text: impl Into<String>) -> pinkdb::Value {
    pinkdb::Value::Varchar(sanitize_terminal(&text.into()))
}

fn count_value(count: u64) -> pinkdb::Value {
    pinkdb::Value::Bigint(count as i64)
}

/// Rows in the wave-99 agent format, so a lake answer looks like a query.
fn catalog_table(
    names: &[&str],
    types: &[pinkdb::DataType],
    rows: Vec<Vec<pinkdb::Value>>,
) -> String {
    let names: Vec<String> = names.iter().map(|name| name.to_string()).collect();
    pinkdb::agent::format_table(&names, types, &rows, pinkdb::agent::AgentDisplay::DEFAULT)
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
                println!(
                    "{} ({rows} row{})",
                    sanitize_terminal(&name),
                    plural(rows as usize)
                );
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
        println!(
            "    {} ({rows} row{})",
            sanitize_terminal(&name),
            plural(rows as usize)
        );
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
                    "warning: table \"{}\" references missing data file {}",
                    sanitize_terminal(&table),
                    sanitize_terminal(&path.display().to_string())
                );
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

// ---------------------------------------------------------------------------
// Replication (wave 89): `.lake replicate …` — table-level jobs between a
// source (another lake, or one database of the session store) and a writable
// destination lake. Jobs and their cursors live in the destination's
// catalog; `start` keeps a supervisor for this session.

/// The `.lake replicate` command list.
fn replicate_usage() {
    println!(
        "Usage: .lake replicate create <dest> from <source-dir|s3://...|store:<db>> \
         [every <secs>]"
    );
    println!("                              [include <patterns>] [exclude <patterns>]");
    println!("                              [watermark <db.table=col,...>]");
    println!("       .lake replicate catchup <dest> <id>");
    println!("       .lake replicate start|stop <dest>");
    println!("       .lake replicate pause|resume <dest> <id>");
    println!("       .lake replicate resume-all <dest>");
    println!("       .lake replicate status <dest>");
    println!("       .lake replicate tables <dest> [<id>]");
    println!("       .lake replicate drop <dest> <id>{HINT}");
}

fn cmd_replicate(db: &mut PinkDb, arg: &str) {
    let Some((sub, rest)) = take_token(arg) else {
        replicate_usage();
        return;
    };
    match sub.to_ascii_lowercase().as_str() {
        "create" => cmd_replicate_create(rest),
        "catchup" | "catch_up" => cmd_replicate_catchup(db, rest),
        "start" => cmd_replicate_start(rest),
        "stop" => cmd_replicate_stop(rest),
        "pause" => cmd_replicate_pause(rest),
        "resume" => cmd_replicate_resume(rest),
        "resume-all" | "resume_all" => cmd_replicate_resume_all(rest),
        "status" => cmd_replicate_status(rest),
        "tables" => cmd_replicate_tables(rest),
        "drop" => cmd_replicate_drop(rest),
        other => cli_error!("unknown .lake replicate subcommand \"{other}\". Try .help"),
    }
}

/// Parse `<dest>` alone (a trailing token is a usage error).
fn replicate_dest(arg: &str, usage: &str) -> Option<String> {
    let Some((dest, rest)) = take_token(arg) else {
        println!("{usage}");
        return None;
    };
    if !rest.trim().is_empty() {
        println!("{usage}");
        return None;
    }
    Some(unquote(&dest))
}

/// Parse `<dest> <id>` (a trailing token is a usage error).
fn replicate_dest_id(arg: &str, usage: &str) -> Option<(String, u64)> {
    let Some((dest, rest)) = take_token(arg) else {
        println!("{usage}");
        return None;
    };
    let Some((id, tail)) = take_token(rest) else {
        println!("{usage}");
        return None;
    };
    if !tail.trim().is_empty() {
        println!("{usage}");
        return None;
    }
    let Ok(id) = id.parse::<u64>() else {
        println!("{usage}");
        return None;
    };
    Some((unquote(&dest), id))
}

/// Run `change` under the session supervisor for `dest`, or `None` when no
/// supervisor is running there.
fn with_supervisor<T>(
    dest: &str,
    change: impl FnOnce(&Supervisor) -> pink_lake::Result<T>,
) -> Option<pink_lake::Result<T>> {
    let key = supervisor_key(dest)?;
    let supervisors = supervisors();
    supervisors.get(&key).map(change)
}

/// `.lake replicate create <dest> from <source> …`: record a job in the
/// destination (creating the destination lake when it does not exist yet).
fn cmd_replicate_create(arg: &str) {
    let usage = "Usage: .lake replicate create <dest> from <source-dir|s3://...|store:<db>> \
                 [every <secs>] [include <patterns>] [exclude <patterns>] \
                 [watermark <db.table=col,...>]";
    let Some((dest_token, rest)) = take_token(arg) else {
        replicate_usage();
        return;
    };
    let dest = unquote(&dest_token);
    let Some((from, rest)) = take_token(rest) else {
        println!("{usage}");
        return;
    };
    if !from.eq_ignore_ascii_case("from") {
        println!("{usage}");
        return;
    }
    let Some((source_token, mut rest)) = take_token(rest) else {
        println!("{usage}");
        return;
    };
    let source_text = unquote(&source_token);
    let source = match source_text.strip_prefix("store:") {
        Some(database) if !database.is_empty() => Source::Store {
            database: database.to_string(),
        },
        Some(_) => {
            cli_error!("invalid store source \"{source_text}\": expected store:<database>");
            return;
        }
        None => Source::Lake {
            location: source_text.clone(),
        },
    };
    let mut spec = JobSpec::new(source);
    loop {
        let tail = rest.trim_start();
        if tail.is_empty() {
            break;
        }
        let Some((token, after)) = take_token(tail) else {
            break;
        };
        match token.to_ascii_lowercase().as_str() {
            "every" => {
                let Some((value, after)) = take_token(after) else {
                    println!("{usage}");
                    return;
                };
                match value.parse::<u64>() {
                    Ok(secs) => spec.interval = Duration::from_secs(secs),
                    Err(_) => {
                        cli_error!("invalid every {value:?}: expected whole seconds");
                        return;
                    }
                }
                rest = after;
            }
            "include" | "exclude" | "watermark" => {
                let Some((value, after)) = take_value(after) else {
                    println!("{usage}");
                    return;
                };
                match token.to_ascii_lowercase().as_str() {
                    "include" => spec.include = value,
                    "exclude" => spec.exclude = value,
                    _ => spec.watermark_columns = value,
                }
                rest = after;
            }
            _ => {
                println!("{usage}");
                return;
            }
        }
    }
    let source_label = shown(&spec.source.to_string());
    let interval_secs = spec.interval.as_secs();
    let dest_shown = shown(&dest);
    let mut lake = match open_or_create_lake(&dest) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    match replication::create(&mut lake, spec) {
        Ok(id) => println!(
            "Created replication job {id} on {dest_shown} from {source_label} (every {interval_secs}s)"
        ),
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake replicate catchup <dest> <id>`: run one cycle now (store-source jobs
/// use this session's store).
fn cmd_replicate_catchup(db: &mut PinkDb, arg: &str) {
    let usage = "Usage: .lake replicate catchup <dest> <id>";
    let Some((dest, id)) = replicate_dest_id(arg, usage) else {
        return;
    };
    let dest_shown = shown(&dest);
    let mut lake = match open_lake(&dest) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    match replication::catch_up(&mut lake, id, Some(db), lake_key()) {
        Ok(report) => println!(
            "Caught up replication job {id} on {dest_shown}: {}",
            report.message
        ),
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake replicate start <dest>`: start the session supervisor (one thread,
/// kept in [`SUPERVISORS`] and stopped on `.lake replicate stop` or exit).
fn cmd_replicate_start(arg: &str) {
    let usage = "Usage: .lake replicate start <dest>";
    let Some(dest) = replicate_dest(arg, usage) else {
        return;
    };
    let dest_shown = shown(&dest);
    if supervisor_key(&dest).is_some_and(|key| supervisors().contains_key(&key)) {
        println!("A replication supervisor for {dest_shown} is already running in this session");
        return;
    }
    match Supervisor::start(&dest, lake_key()) {
        Ok(supervisor) => {
            let location = supervisor.destination().to_string();
            let skipped = supervisor.skipped();
            supervisors().insert(location, supervisor);
            println!("Started replication supervisor for {dest_shown}");
            for (id, reason) in skipped {
                println!("  job {id} skipped: {reason}");
            }
        }
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake replicate stop <dest>`: stop and join the session supervisor.
fn cmd_replicate_stop(arg: &str) {
    let usage = "Usage: .lake replicate stop <dest>";
    let Some(dest) = replicate_dest(arg, usage) else {
        return;
    };
    let dest_shown = shown(&dest);
    let Some(key) = supervisor_key(&dest) else {
        cli_error!("no replication supervisor for {dest_shown} is running in this session");
        return;
    };
    let supervisor = supervisors().remove(&key);
    match supervisor {
        Some(supervisor) => {
            supervisor.stop();
            println!("Stopped replication supervisor for {dest_shown}");
        }
        None => {
            cli_error!("no replication supervisor for {dest_shown} is running in this session")
        }
    }
}

/// `.lake replicate pause <dest> <id>`.
fn cmd_replicate_pause(arg: &str) {
    let usage = "Usage: .lake replicate pause <dest> <id>";
    let Some((dest, id)) = replicate_dest_id(arg, usage) else {
        return;
    };
    let dest_shown = shown(&dest);
    let result = match with_supervisor(&dest, |supervisor| supervisor.pause(id)) {
        Some(result) => result,
        None => open_lake(&dest).and_then(|mut lake| replication::pause(&mut lake, id)),
    };
    match result {
        Ok(()) => println!("Paused replication job {id} on {dest_shown}"),
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake replicate resume <dest> <id>`.
fn cmd_replicate_resume(arg: &str) {
    let usage = "Usage: .lake replicate resume <dest> <id>";
    let Some((dest, id)) = replicate_dest_id(arg, usage) else {
        return;
    };
    let dest_shown = shown(&dest);
    let result = match with_supervisor(&dest, |supervisor| supervisor.resume(id)) {
        Some(result) => result,
        None => open_lake(&dest).and_then(|mut lake| replication::resume(&mut lake, id)),
    };
    match result {
        Ok(()) => println!("Resumed replication job {id} on {dest_shown}"),
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake replicate resume-all <dest>`.
fn cmd_replicate_resume_all(arg: &str) {
    let usage = "Usage: .lake replicate resume-all <dest>";
    let Some(dest) = replicate_dest(arg, usage) else {
        return;
    };
    let dest_shown = shown(&dest);
    let result = match with_supervisor(&dest, |supervisor| supervisor.resume_all()) {
        Some(result) => result,
        None => open_lake(&dest).and_then(|mut lake| replication::resume_all(&mut lake)),
    };
    match result {
        Ok(ids) if ids.is_empty() => println!("No paused or errored jobs on {dest_shown}"),
        Ok(ids) => println!(
            "Resumed {count} replication job{plural} on {dest_shown}: {list}",
            count = ids.len(),
            plural = plural(ids.len()),
            list = ids
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Err(e) => cli_error!("{e}"),
    }
}

/// `.lake replicate status <dest>`: one line per job (id, source, status,
/// versions behind, last success and last error).
fn cmd_replicate_status(arg: &str) {
    let usage = "Usage: .lake replicate status <dest>";
    let Some(dest) = replicate_dest(arg, usage) else {
        return;
    };
    let lake = match open_lake(&dest) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let jobs = match replication::jobs(&lake) {
        Ok(jobs) => jobs,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    if jobs.is_empty() {
        println!("(no replication jobs)");
        return;
    }
    for job in jobs {
        let behind = match replication::versions_behind(&lake, job.id) {
            Ok(Some(versions)) => versions.to_string(),
            Ok(None) => "-".to_string(),
            Err(e) => format!("unknown ({e})"),
        };
        println!(
            "job {}  {}  status={}  behind={}  last ok={}  last error={}",
            job.id,
            sanitize_terminal(&shown(&job.source.to_string())),
            job.status.as_str(),
            behind,
            sanitize_terminal(job.last_ok.as_deref().unwrap_or("-")),
            job.last_error
                .as_deref()
                .map(sanitize_terminal)
                .unwrap_or_else(|| "-".to_string()),
        );
    }
}

/// `.lake replicate tables <dest> [<id>]`: per-table state rows (every job
/// when no id is given).
fn cmd_replicate_tables(arg: &str) {
    let usage = "Usage: .lake replicate tables <dest> [<id>]";
    let Some((dest_token, rest)) = take_token(arg) else {
        println!("{usage}");
        return;
    };
    let dest = unquote(&dest_token);
    let rest = rest.trim();
    let selected = if rest.is_empty() {
        None
    } else {
        match rest.parse::<u64>() {
            Ok(id) => Some(id),
            Err(_) => {
                println!("{usage}");
                return;
            }
        }
    };
    let lake = match open_lake(&dest) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    let ids: Vec<u64> = match selected {
        Some(id) => vec![id],
        None => match replication::jobs(&lake) {
            Ok(jobs) => jobs.into_iter().map(|job| job.id).collect(),
            Err(e) => {
                cli_error!("{e}");
                return;
            }
        },
    };
    let mut any = false;
    for job_id in ids {
        let tables = match replication::tables(&lake, job_id) {
            Ok(tables) => tables,
            Err(e) => {
                cli_error!("{e}");
                return;
            }
        };
        for table in tables {
            any = true;
            println!(
                "job {job_id}  {} -> {}  strategy={}  rows={}  watermark={}  source version={}  \
                 full sync={}  synced={}  error={}",
                sanitize_terminal(&shown(&table.source)),
                sanitize_terminal(&shown(&table.dest)),
                table.strategy,
                table
                    .rows_dest
                    .map(|rows| rows.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                table.watermark_column.as_deref().unwrap_or("-"),
                table
                    .last_source_version
                    .map(|version| version.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                table
                    .last_full_sync_version
                    .map(|version| version.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                sanitize_terminal(table.synced.as_deref().unwrap_or("-")),
                table
                    .last_error
                    .as_deref()
                    .map(sanitize_terminal)
                    .unwrap_or_else(|| "-".to_string()),
            );
        }
    }
    if !any {
        println!("(no replication tables)");
    }
}

/// `.lake replicate drop <dest> <id>`: remove a job's state; the replicated
/// tables stay.
fn cmd_replicate_drop(arg: &str) {
    let usage = "Usage: .lake replicate drop <dest> <id>";
    let Some((dest, id)) = replicate_dest_id(arg, usage) else {
        return;
    };
    let dest_shown = shown(&dest);
    let mut lake = match open_lake(&dest) {
        Ok(lake) => lake,
        Err(e) => {
            cli_error!("{e}");
            return;
        }
    };
    match replication::drop(&mut lake, id) {
        Ok(()) => {
            println!("Dropped replication job {id} from {dest_shown} (its replicated tables stay)")
        }
        Err(e) => cli_error!("{e}"),
    }
}

// ---------------------------------------------------------------------------
// Maintenance (wave 67): merge / rewrite / expire / cleanup / orphans, and
// the files / table_info / options / settings introspection.

fn flat_table_key(token: &str) -> String {
    if token.contains('.') {
        token.to_string()
    } else {
        format!("main.{token}")
    }
}

fn writable_refusal(dir: &str, what: &str) -> Option<String> {
    let lake = open_lake(dir).ok()?;
    match pink_lake::EngineLease::acquire(&lake, "pinkdb-cli-maintenance") {
        Err(pink_lake::LeaseError::Held { owner, .. }) => Some(format!(
            "{} is attached as a writable lake database by {owner}; DETACH it before {what}",
            shown(dir)
        )),
        _ => None,
    }
}

fn cmd_format(arg: &str) {
    let usage = "Usage: .lake format [<id> [for <db.table>]]";
    let arg = arg.trim();
    if arg.is_empty() {
        println!("Lake write format: {}", current_write_format_id());
        println!("Registered formats: {}", registered_format_ids().join(", "));
        let choices = table_format_choices();
        if choices.is_empty() {
            println!("Per-table formats: (none)");
        } else {
            let text = choices
                .iter()
                .map(|(key, id)| format!("{key}={id}"))
                .collect::<Vec<_>>()
                .join(", ");
            println!("Per-table formats: {text}");
        }
        println!("{usage}");
        return;
    }
    let Some((token, rest)) = take_token(arg) else {
        println!("{usage}");
        return;
    };
    let id = unquote(&token);
    let rest = rest.trim();
    let table = if rest.is_empty() {
        None
    } else {
        let Some((word, after)) = take_token(rest) else {
            println!("{usage}");
            return;
        };
        if !word.eq_ignore_ascii_case("for") {
            println!("{usage}");
            return;
        }
        let Some((table_token, tail)) = take_token(after) else {
            println!("{usage}");
            return;
        };
        if !tail.trim().is_empty() {
            println!("{usage}");
            return;
        }
        Some(unquote(&table_token))
    };
    if pink_lake::format::lake_format(&id).is_none() {
        let ids = registered_format_ids();
        cli_error!(
            "unknown lake format \"{id}\"; registered formats: {} (a lake format provider is \
             registered by the build feature that ships it, e.g. `parquetlake` for parquet)",
            ids.join(", ")
        );
        return;
    }
    let Some(table) = table else {
        *WRITE_FORMAT.lock().unwrap_or_else(|p| p.into_inner()) = Some(id.clone());
        println!("Lake write format set to \"{id}\" for subsequent .lake save writes");
        return;
    };
    let key = flat_table_key(&table);
    TABLE_FORMATS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(key.clone(), id.clone());
    println!(
        "Lake format for \"{key}\" set to \"{id}\" for subsequent full saves (an existing \
         table keeps its pinned format on appends)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A served session reads catalogs; maintenance is an operator action even
    /// when the session allows writes.
    #[test]
    fn lake_markdown_serves_only_the_catalog_readers() {
        let mut db = PinkDb::new();
        let mut refused = |text: &str, read_only: bool| -> String {
            lake_markdown(&mut db, text, read_only).expect_err("maintenance is not served")
        };
        for command in [".lake commit /tmp/catalog", ".lake merge /tmp/catalog"] {
            for read_only in [true, false] {
                let text = refused(command, read_only);
                assert!(text.contains("\"kind\":\"permission\""), "{text}");
                assert!(text.contains("not available over MCP"), "{text}");
            }
        }
        let text = lake_markdown(&mut db, ".lake nope /tmp/catalog", true).unwrap_err();
        assert!(text.contains("\"kind\":\"permission\""), "{text}");
    }
}
