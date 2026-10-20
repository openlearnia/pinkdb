//! Extension-repository client (the `repo` feature).
//!
//! The CLI's `.install` / `.repositories` / `.installed` dot-commands are
//! thin wrappers around this module:
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
//!   artifact, stages it in a private `0700` temporary directory, re-hashes
//!   the staged file, and returns it for `dlopen`.
//!
//! The module is compiled only with the `repo` feature; the default CLI
//! binary links no HTTP/TLS/JSON/SHA-256/Ed25519 code.

pub mod catalog;
pub mod client;
pub mod config;
pub mod signature;

pub use client::{agent, install, stage_for_loading};
pub use config::{
    add_repository, extensions_dir, list_installed, load_repositories, pinkdb_home,
    remove_repository, save_repositories,
};
pub use signature::Trust;
