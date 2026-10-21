//! The warehouse browsing UI (wave 78).
//!
//! The console itself lives in the sibling crate
//! [`pinkdb_server_ui`](https://docs.rs/pinkdb-server-ui); this module
//! re-exports its surface so an embedder of `pinkdb-server` does not have to
//! depend on the console crate directly:
//!
//! ```no_run
//! use pinkdb::Database;
//! use pinkdb_server::ui::{UiAuth, UiOptions, UiServer};
//!
//! let opts = UiOptions {
//!     auth: UiAuth::Basic,
//!     ..UiOptions::default()
//! };
//! let server = UiServer::bind("127.0.0.1:8080", Database::new(), opts)?;
//! let handle = server.spawn()?;
//! println!("console on {}", handle.local_addr());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! The console is **read-only** and **feature-gated**: it exists only in a
//! build with the `ui` cargo feature, it binds loopback by default, and it
//! refuses to serve a store that has users without an explicit credential.
//! See `docs/server-ui.md`.

pub use pinkdb_server_ui::{
    ConsoleTls, UiAuth, UiError, UiHandle, UiOptions, UiServer, check_auth_policy,
    check_bind_policy,
};
