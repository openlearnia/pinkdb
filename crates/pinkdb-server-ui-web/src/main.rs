//! The warehouse console's browser app (waves 121, 121c): Leptos, rendered
//! in the browser, talking to the console's JSON API. Built for wasm32 by
//! `scripts/build-console.sh` and embedded in `pinkdb-server-ui`. The design
//! system it follows is `docs/console-design/README.md`.

mod access;
mod account;
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
    // A panic aborts the wasm instance (`panic = "abort"`), which the browser
    // reports only as `RuntimeError: unreachable`; print the message first so
    // a broken page says why.
    std::panic::set_hook(Box::new(|info| {
        web_sys::console::error_1(&format!("pinkdb console panicked: {info}").into());
    }));
    leptos::mount::mount_to_body(app::Root);
}
