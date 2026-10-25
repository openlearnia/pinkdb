//! Extension-repository client (the `repo` feature).
//!
//! It lives in the engine, not the CLI, because the SQL surface needs it:
//! `INSTALL … [FROM …]` and `LOAD <name>` (the `install` module) execute the
//! same code as the CLI's `.install` / `.repositories` / `.installed`
//! dot-commands, which are thin wrappers around this module.
//!
//! - [`config`] owns `$PINKDB_HOME` (default `~/.pinkdb`) and the ordered
//!   repository list in `$PINKDB_HOME/repositories.json`;
//! - [`catalog`] parses the repository's `index.json` (format v1) and
//!   resolves `name[@version]` for the current platform and ABI;
//! - [`signature`] verifies the detached Ed25519 signature over the exact
//!   catalog bytes against the key pinned in the binary
//!   (`docs/extension-signing.md`);
//! - [`client`] fetches the catalog + signature, verifies the signature
//!   before any artifact request, downloads and SHA-256-verifies the
//!   artifact into the local cache, and re-hashes the cached file before
//!   `INSTALL` loads it.
//!
//! The module is compiled only with the `repo` feature (native only); a
//! default `pinkdb` build links no HTTP/TLS/JSON/SHA-256/Ed25519 code.

pub mod catalog;
pub mod client;
pub mod config;
pub mod signature;

pub use client::{agent, install};
pub use config::{
    add_repository, extensions_dir, list_installed, load_repositories, pinkdb_home,
    remove_repository, save_repositories, validate_repository_url,
};
pub use signature::Trust;
