// pinkdb console: load the Leptos app (wave 121). A module script file, not
// an inline one: the console's Content-Security-Policy allows scripts from
// its own origin only.
import init from "/assets/pinkdb-console.js";
// The theme this browser picked (the app's theme switch stores it), applied
// before the app loads so the page never flashes the other one.
try {
  const theme = localStorage.getItem("pinkdb-console-theme");
  if (theme === "light" || theme === "dark") document.documentElement.dataset.theme = theme;
} catch (_) {}
init({ module_or_path: "/assets/pinkdb-console_bg.wasm" });
