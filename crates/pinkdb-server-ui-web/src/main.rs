//! The warehouse console's browser app (wave 121): Leptos, rendered in the
//! browser, talking to the console's JSON API. Built for wasm32 by
//! `scripts/build-console.sh` and embedded in `pinkdb-server-ui`.

mod access;
mod api;
mod app;
mod ask;
mod chart;
mod editor;
mod import;
mod results;
mod sidebar;
mod state;

fn main() {
    leptos::mount::mount_to_body(app::Root);
}
