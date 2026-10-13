//! Extension-repository client (the `repo` feature).
//!
//! The CLI's `.install` / `.repositories` / `.installed` dot-commands are
//! thin wrappers around this module:
//!
//! - [`config`] owns `$PINKDB_HOME` (default `~/.pinkdb`) and the ordered
//!   repository list in `$PINKDB_HOME/repositories.json`;
//! - [`catalog`] parses the repository's `index.json` (format v1) and
//!   resolves `name[@version]` for the current platform and ABI;
//! - [`client`] fetches the catalog and artifact over HTTP, verifies the
//!   artifact's SHA-256, and caches it under
//!   `$PINKDB_HOME/extensions/<name>/<version>/<platform>/<file>`.
//!
//! The module is compiled only with the `repo` feature; the default CLI
//! binary links no HTTP/TLS/JSON/SHA-256 code.

pub mod catalog;
pub mod client;
pub mod config;

pub use client::{agent, install};
pub use config::{
    add_repository, extensions_dir, list_installed, load_repositories, pinkdb_home,
    remove_repository, save_repositories,
};
