// pinkdb console: load the Leptos app (wave 121). A module script file, not
// an inline one: the console's Content-Security-Policy allows scripts from
// its own origin only.
import init from "/assets/pinkdb-console.js";
init({ module_or_path: "/assets/pinkdb-console_bg.wasm" });
