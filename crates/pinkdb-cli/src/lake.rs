//! `.lake` dot-commands (the `lake` feature): snapshot the in-memory store
//! into a pinkdb lake directory and load versions back.
//!
//! The lake itself lives in the `pink-lake` crate; this module is only the
//! REPL surface. All commands are local-only — they read and write the
//! process's filesystem, and `.lake open` replaces the in-memory store:
//!
//! - the session database is reset to `main` (the lake has no session state;
//!   the output says so when it changes);
//! - the session's `PINKDB_KEY` is carried over so a later `.save`/`.open`
//!   stays encrypted, while the lake's own files stay plaintext in v1 (the
//!   output says so when a key is set);
//! - a lake directory has **one live writer**: a `.lake save` whose handle
//!   another writer outran fails with a conflict error naming the published
//!   version — reopen the lake to continue;
//! - `.lake save` validates the store *before* creating a lake, so a store
//!   the v1 catalog cannot represent (dotted database name, over-long table
//!   name) leaves no stray empty lake behind.

use std::time::Instant;

use pink_lake::Lake;
use pinkdb::PinkDb;

use crate::{plural, take_token, unquote};

/// Dispatch `.lake <subcommand> ...`.
pub(crate) fn cmd_lake(db: &mut PinkDb, arg: &str) {
    let (sub, rest) = match arg.find(char::is_whitespace) {
        Some(i) => (&arg[..i], arg[i..].trim()),
        None => (arg, ""),
    };
    match sub {
        "save" => cmd_save(db, rest),
        "open" => cmd_open(db, rest),
        "versions" => cmd_versions(rest),
        "tables" => cmd_tables(rest),
        "info" => cmd_info(rest),
        "" => println!("Usage: .lake <save|open|versions|tables|info> ..."),
        other => println!("Unknown .lake subcommand \"{other}\". Try .help"),
    }
}

/// `.lake save <dir>`: snapshot every database/table of the current store.
fn cmd_save(db: &PinkDb, arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake save <dir>");
        return;
    };
    if !rest.trim().is_empty() {
        println!("Usage: .lake save <dir>");
        return;
    }
    let dir = unquote(&dir);
    // Reject an unrepresentable store before `open_or_create` can leave a
    // fresh empty lake behind (e.g. a dotted database name or a table name
    // whose encoded data directory would exceed the filesystem's limit).
    if let Err(e) = pink_lake::validate_store(db) {
        println!("Error: {e}");
        return;
    }
    let mut lake = match Lake::open_or_create(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            println!("Error: {e}");
            return;
        }
    };
    let t0 = Instant::now();
    match lake.write_from_store_report(db) {
        Ok(report) => {
            if db
                .list_databases()
                .iter()
                .any(|d| !db.list_views_in(d).is_empty())
            {
                println!("Note: views are session-local and were not saved.");
            }
            if db.encryption_key().is_some() {
                println!(
                    "Note: lake data files are plaintext (v1); PINKDB_KEY does not encrypt them."
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
        Err(e) => println!("Error: {e}"),
    }
}

/// `.lake open <dir> [version]`: replace the in-memory store with the lake's
/// latest (or the given) version.
fn cmd_open(db: &mut PinkDb, arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake open <dir> [version]");
        return;
    };
    let dir = unquote(&dir);
    let version = if rest.trim().is_empty() {
        None
    } else {
        match rest.trim().parse::<u64>() {
            Ok(v) => Some(v),
            Err(_) => {
                println!("Usage: .lake open <dir> [version]");
                return;
            }
        }
    };
    let lake = match Lake::open(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            println!("Error: {e}");
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
                    "Note: lake data files are plaintext (v1); the session key applies to \
                     .save/.open, not to the lake."
                );
            }
        }
        Err(e) => println!("Error: {e}"),
    }
}

/// `.lake versions <dir>`: the published snapshot versions, current marked.
fn cmd_versions(arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake versions <dir>");
        return;
    };
    if !rest.trim().is_empty() {
        println!("Usage: .lake versions <dir>");
        return;
    }
    let dir = unquote(&dir);
    let lake = match Lake::open(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            println!("Error: {e}");
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
        Err(e) => println!("Error: {e}"),
    }
}

/// `.lake tables <dir>`: the current version's `db.table` names + row counts.
fn cmd_tables(arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake tables <dir>");
        return;
    };
    if !rest.trim().is_empty() {
        println!("Usage: .lake tables <dir>");
        return;
    }
    let dir = unquote(&dir);
    let lake = match Lake::open(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            println!("Error: {e}");
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
        Err(e) => println!("Error: {e}"),
    }
}

/// `.lake info <dir>`: format version, current version, snapshots, tables.
fn cmd_info(arg: &str) {
    let Some((dir, rest)) = take_token(arg) else {
        println!("Usage: .lake info <dir>");
        return;
    };
    if !rest.trim().is_empty() {
        println!("Usage: .lake info <dir>");
        return;
    }
    let dir = unquote(&dir);
    let lake = match Lake::open(&dir) {
        Ok(lake) => lake,
        Err(e) => {
            println!("Error: {e}");
            return;
        }
    };
    let tables = match lake.tables() {
        Ok(tables) => tables,
        Err(e) => {
            println!("Error: {e}");
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
        Err(e) => println!("Error: {e}"),
    }
}
