//! pinkdb-server warehouse browsing UI (wave 78) — a **read-only** web
//! console for the served store.
//!
//! The console is a small HTTP/1.1 service that renders a browser page and
//! answers a JSON API about the store it is attached to:
//!
//! - `GET /` — the console shell (server-rendered HTML, self-hosted assets);
//! - `GET /preview?db=&table=&page=` — a server-rendered row preview (the
//!   no-JavaScript fallback, and the page that proves cell escaping);
//! - `GET /assets/console.css`, `GET /assets/console.js` — the embedded,
//!   self-hosted assets (no CDN, no external font or script);
//! - `GET /api/databases`, `/api/schemas`, `/api/tables`, `/api/rows`,
//!   `/api/stats` — the JSON API (catalog, paged row previews, row counts and
//!   size estimates).
//!
//! # Read-only by construction
//!
//! The console issues **no DML and no DDL**: the only statements it can run
//! are `SHOW DATABASES`, `SHOW TABLES`, `DESCRIBE`, `SELECT COUNT(*)` and
//! `SELECT … LIMIT … OFFSET …`. There is no endpoint that accepts a
//! statement, a table name to write to, or a body to mutate anything — the
//! router has no `POST`/`PUT`/`DELETE` route at all.
//!
//! Every store read is a **short, bounded read**: it runs under the engine's
//! own statement deadline and result caps (the pass-5 limits, see
//! [`UiOptions`]), and the store lock is released before any HTTP work
//! happens — the engine call returns materialized rows, and the JSON/HTML
//! rendering runs afterwards, outside the lock.
//!
//! # Async carve-out (the second one, after wave 47)
//!
//! The workspace's design rule is **zero async**: the engine, the planner,
//! the storage and the default server are synchronous. The console is a
//! documented carve-out, scoped to the **I/O boundary only**:
//!
//! 1. **The console owns its own tokio runtime, on its own thread.** It does
//!    not care whether the server next to it runs the blocking (`sync`)
//!    transport or the tokio transport (`async-server`, wave 47): the
//!    console's runtime is independent, so the server's transport choice
//!    never changes the console's behavior.
//! 2. **The engine stays synchronous.** pinkdb's API is blocking, so every
//!    store read runs through [`tokio::task::spawn_blocking`] on a **bounded
//!    blocking pool** ([`UiOptions::max_blocking_threads`], set explicitly —
//!    the tokio default is not accepted). The async runtime never executes
//!    engine code, so a slow scan cannot stall the reactor.
//! 3. **The default build gains no async runtime.** `pinkdb-server`'s
//!    `ui` feature is **off by default**: without it, this crate is not even
//!    compiled into the server binary, and `cargo tree -p pinkdb-server`
//!    shows no tokio (the wave-47 invariant still holds).
//!
//! Scope limits, stated honestly: the console adds no async storage, no
//! `async fn` engine API and no engine change of any kind — it is an HTTP
//! surface around the existing synchronous engine.
//!
//! # Exposure policy (fail closed)
//!
//! - The console is **off by default**: the operator turns it on explicitly
//!   (`--enable-ui`).
//! - It binds **127.0.0.1** unless told otherwise, and a **non-loopback
//!   bind is refused** (see [`check_bind_policy`]): the console is a browser
//!   UI and cannot use the server's TLS transport, so a non-localhost
//!   exposure needs console TLS (wave 46) *and* RBAC (wave 48) first.
//! - When the store **has users** (wave 48's RBAC catalog), the console
//!   refuses to serve them unauthenticated (see [`check_auth_policy`]): the
//!   operator must ask for HTTP Basic authentication (verified through the
//!   engine's own password path, so the console's statements run under that
//!   user's grants) or for a bearer token.
//! - Serving without authentication prints a **loud warning**.
//!
//! # The preview renderer
//!
//! The console **mirrors** the repository's box renderer (the CLI's
//! `format_select`/`display_value`/`sanitize_cell` in
//! `crates/pinkdb-cli/src/main.rs`): `NULL` renders as `NULL`, a `DOUBLE`
//! prints DuckDB-style (`1.0`, not `1`), and a `VARCHAR` cell never carries a
//! raw control character. The renderer lives in a *binary* crate, so it is
//! mirrored here rather than extracted (extracting it would move the CLI's
//! renderer into a shared crate and change the CLI's own build); the mirror
//! is pinned by tests. See [`preview`] for the exact rules and the one
//! addition (a per-cell width cap, which a browser table needs and a
//! terminal box does not).

mod api;
mod assets;
mod auth;
mod html;
mod policy;
mod preview;
mod server;
mod store;

pub use policy::{ConsoleTls, UiAuth, UiError, check_auth_policy, check_bind_policy};
pub use server::{UiHandle, UiOptions, UiServer};

/// The test-only reactor hook (hidden from the docs).
#[doc(hidden)]
pub use server::REACTOR_HOOK;
/// The test-only engine hook (hidden from the docs).
#[doc(hidden)]
pub use store::ENGINE_HOOK;
