//! QuackLab's `/sql` adapter (wave 138a).
//!
//! The adapter lives in the sibling crate
//! [`pinkdb_quacklab`](https://docs.rs/pinkdb-quacklab); this module
//! re-exports its surface so an embedder of `pinkdb-server` does not have to
//! depend on the adapter crate directly:
//!
//! ```no_run
//! use pinkdb::Database;
//! use pinkdb_server::quacklab::{QuacklabOptions, QuacklabServer};
//!
//! let server = QuacklabServer::bind(
//!     "127.0.0.1:1294",
//!     Database::new(),
//!     QuacklabOptions::default(),
//! )?;
//! let handle = server.spawn()?;
//! println!("QuackLab on {}", handle.url());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! It is **feature-gated**: it exists only in a build with the `quacklab`
//! cargo feature (off by default). It serves over the server's own store — the
//! same `Database`, data directory and extensions — on its own listener and
//! its own runtime thread.

pub use pinkdb_quacklab::*;

/// The QuackLab settings the adapter accepts and does not use: DuckDB's own
/// engine knobs, the sandbox's file-path allow-list (pinkdb has no file-read
/// root policy; only `LOAD`/`INSTALL` are refused) and the DuckLake attach
/// (wave 138c), which pinkdb's engine has no counterpart for. They are named
/// here so the server can say once, at start, that it is ignoring them instead
/// of silently accepting a configuration it cannot honour.
pub const IGNORED_ENVIRONMENT: &[&str] = &[
    "DUCKDB_PATH",
    "DUCKDB_MEMORY_LIMIT",
    "DUCKDB_THREADS",
    "GRAIN_DUCKDB_ALLOWED_PATHS",
    "GRAIN_DUCKLAKE",
    "DUCKLAKE_PG_URI",
    "DUCKLAKE_DATA_PATH",
    "DUCKLAKE_ENCRYPTED",
];

/// The settings from [`IGNORED_ENVIRONMENT`] present in the environment, for
/// the server's one log line.
pub fn ignored_settings_from_env() -> Vec<&'static str> {
    IGNORED_ENVIRONMENT
        .iter()
        .copied()
        .filter(|name| std::env::var_os(name).is_some())
        .collect()
}

/// The adapter's bind address: `--quacklab-bind` when the operator passed it,
/// else `DUCKDB_BIND` (the QuackLab deployment's own knob), else none — the
/// adapter is opt-in, so a server nobody asked for starts no listener.
pub fn bind(flag: Option<String>) -> Option<String> {
    if flag.is_some() {
        return flag;
    }
    std::env::var("DUCKDB_BIND")
        .ok()
        .filter(|bind| !bind.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_flag_wins_over_the_environment() {
        assert_eq!(
            bind(Some("127.0.0.1:7001".to_string())),
            Some("127.0.0.1:7001".to_string())
        );
    }

    #[test]
    fn every_ignored_setting_is_named_once() {
        let mut names = IGNORED_ENVIRONMENT.to_vec();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), IGNORED_ENVIRONMENT.len());
    }
}
