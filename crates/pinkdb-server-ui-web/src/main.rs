//! The warehouse console's browser app (waves 121, 121c): Leptos, rendered
//! in the browser, talking to the console's JSON API. Built for wasm32 by
//! `scripts/build-console.sh` and embedded in `pinkdb-server-ui`. The design
//! system it follows is `docs/console-design/README.md`.

mod access;
mod api;
mod app;
mod ask;
mod catalog;
mod chart;
mod editor;
mod explorer;
mod history;
mod icon;
mod import;
mod results;
mod route;
mod state;
mod storage;
mod ui;

fn main() {
    leptos::mount::mount_to_body(app::Root);
}
