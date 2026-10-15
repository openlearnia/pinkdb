//! `.lake` dot-commands: snapshot the in-memory store into a pinkdb lake and
//! load versions back.
//!
//! Built with the `lake` feature (full: Vortex data files, format_version 3,
//! `.lake attach` over footers) or `lake-lite` (lean: v1 `.pink` data files,
//! format_version 1, `.lake attach` over the published statistics block — see
//! [`cmd_attach`]).
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

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Instant;

use pink_lake::Lake;
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
        "detach" => cmd_detach(db, rest),
        "attached" => cmd_attached(db, rest),
        "versions" => cmd_versions(rest),
        "tables" => cmd_tables(rest),
        "info" => cmd_info(rest),
        "" => println!("Usage: .lake <save|open|attach|detach|attached|versions|tables|info> ..."),
        other => cli_error!("unknown .lake subcommand \"{other}\". Try .help"),
    }
}

/// `.lake save <location>`: snapshot every database/table of the current
/// store (a directory or an `s3://bucket/prefix` URL with the `s3` feature).
fn cmd_save(db: &PinkDb, arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake save <dir> [force]{HINT}");
        return;
    };
    let force = match rest.trim() {
        "" => false,
        "force" => true,
        _ => {
            println!("Usage: .lake save <dir> [force]{HINT}");
            return;
        }
    };
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

/// `.lake attach <location> [version]`: attach the lake read-only, from its
/// published statistics. A Vortex data file contributes its footer, metadata
/// segment and stored statistics; a v1 `.pink` entry contributes the
/// manifest's CRC-protected statistics block (no data file is opened). No
/// column data is decoded either way; the one table a query actually scans
/// materializes lazily on first use.
fn cmd_attach(db: &mut PinkDb, arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake attach <dir> [version]{HINT}");
        return;
    };
    let dir = unquote(&dir);
    let version = if rest.trim().is_empty() {
        None
    } else {
        match rest.trim().parse::<u64>() {
            Ok(v) => Some(v),
            Err(_) => {
                println!("Usage: .lake attach <dir> [version]{HINT}");
                return;
            }
        }
    };
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
                let shape = if report.format_version >= 2 {
                    "footer-only"
                } else {
                    "statistics-only"
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

/// `.lake detach <location>`: drop one attached lake from this session.
fn cmd_detach(db: &mut PinkDb, arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake detach <dir>{HINT}");
        return;
    };
    if !rest.trim().is_empty() {
        println!("Usage: .lake detach <dir>{HINT}");
        return;
    }
    let dir = unquote(&dir);
    if db.detach_lake(&dir) {
        println!("Detached {dir}");
    } else {
        cli_error!("no lake is attached at {dir} (see .lake attached)");
    }
}

/// `.lake attached`: the lakes attached to this session and their tables,
/// plus each lake's data-file decode counters (statistics-only queries and a
/// projected decode show up here).
fn cmd_attached(db: &PinkDb, arg: &str) {
    if !arg.trim().is_empty() {
        println!("Usage: .lake attached");
        return;
    }
    let lakes = db.attached_lakes();
    if lakes.is_empty() {
        println!("(no attached lakes)");
        return;
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
