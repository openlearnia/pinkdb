#!/usr/bin/env node
// verify-browser.mjs — headless-Chrome pass over the live docs + repository.
//
//   node verify-browser.mjs [docs-base-url] [repo-base-url]
//
// Defaults to the live sites:
//   docs  https://pinkdb-docs.defaultcms.workers.dev
//   repo  https://pinkdb-ext-repo.defaultcms.workers.dev
//
// Drives real headless Chromium over the DevTools protocol (no dependencies;
// Node's built-in WebSocket) and checks, end to end:
//   - the docs Extensions page renders the catalog card for the first
//     extension (name, version, platform, ABI, truncated SHA-256, size or
//     em dash, Download + Details links pointing at the repository);
//   - the live filter and its count, the empty state and the clear button;
//   - the #/<name> details route: `.install <name>` / `.install
//     <name>@<version>` commands, metadata table, download URL, raw entry;
//   - the not-found hash route;
//   - the Worker `/` info page: heading, docs link, and /index.json answering
//     200 with Access-Control-Allow-Origin: * (from the page context);
//   - no console errors (CORS failures, exceptions, failed resources) during
//     the whole pass, favicon noise excepted.
//
// Needs Google Chrome or Chromium; set CHROME=/path/to/chrome to override.
// Exits non-zero on any failed check or if the browser cannot be driven.

import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const DOCS = (process.argv[2] || "https://pinkdb-docs.defaultcms.workers.dev").replace(/\/+$/, "");
const REPO = (process.argv[3] || "https://pinkdb-ext-repo.defaultcms.workers.dev").replace(/\/+$/, "");

const CANDIDATES = [
  process.env.CHROME,
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  "/Applications/Chromium.app/Contents/MacOS/Chromium",
  "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
  "/usr/bin/google-chrome",
  "/usr/bin/chromium",
  "/usr/bin/chromium-browser",
].filter(Boolean);
const chromePath = CANDIDATES.find((candidate) => existsSync(candidate));
if (!chromePath) {
  console.error("FAIL  no Chrome/Chromium found — install one or set CHROME=/path/to/chrome");
  process.exit(1);
}

let passed = 0;
let failed = 0;

function check(ok, label, detail) {
  if (ok) {
    passed += 1;
    console.log(`ok    ${label}`);
  } else {
    failed += 1;
    console.error(`FAIL  ${label}${detail ? ` — ${detail}` : ""}`);
  }
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function formatSize(bytes) {
  if (typeof bytes !== "number" || !isFinite(bytes) || bytes < 0) return "—";
  if (bytes < 1024) return String(bytes) + " B";
  let value = bytes;
  let unit = "B";
  for (const next of ["KiB", "MiB", "GiB", "TiB"]) {
    value = value / 1024;
    unit = next;
    if (value < 1024) break;
  }
  const rounded = value >= 100 ? Math.round(value) : Math.round(value * 10) / 10;
  return String(rounded) + " " + unit;
}

class Cdp {
  constructor(socket) {
    this.socket = socket;
    this.pending = new Map();
    this.listeners = new Set();
    this.nextId = 1;
    socket.addEventListener("message", (event) => {
      const message = JSON.parse(event.data);
      if (message.id !== undefined) {
        const pending = this.pending.get(message.id);
        if (!pending) return;
        this.pending.delete(message.id);
        if (message.error) pending.reject(new Error(message.error.message));
        else pending.resolve(message.result);
        return;
      }
      for (const listener of this.listeners) {
        try { listener(message); } catch { /* listener errors are not fatal */ }
      }
    });
  }

  static async connect(url) {
    const socket = new WebSocket(url);
    await new Promise((resolve, reject) => {
      socket.addEventListener("open", resolve, { once: true });
      socket.addEventListener("error", () => reject(new Error(`cannot connect to ${url}`)), { once: true });
    });
    return new Cdp(socket);
  }

  send(method, params = {}) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.socket.send(JSON.stringify({ id, method, params }));
    });
  }

  onEvent(listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  waitEvent(method, timeout = 20000) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        remove();
        reject(new Error(`timed out waiting for ${method}`));
      }, timeout);
      const remove = this.onEvent((message) => {
        if (message.method === method) {
          clearTimeout(timer);
          remove();
          resolve(message.params);
        }
      });
    });
  }

  async navigate(url) {
    const loaded = this.waitEvent("Page.loadEventFired");
    await this.send("Page.navigate", { url });
    await loaded;
  }

  async evaluate(expression) {
    const result = await this.send("Runtime.evaluate", {
      expression,
      returnByValue: true,
      awaitPromise: true,
    });
    if (result.exceptionDetails) {
      throw new Error(`page evaluate failed: ${result.exceptionDetails.text || "exception"}`);
    }
    return result.result.value;
  }

  async waitFor(expression, { timeout = 20000, interval = 150 } = {}) {
    const start = Date.now();
    for (;;) {
      try {
        if (await this.evaluate(`Boolean(${expression})`)) return;
      } catch {
        // Navigation may be in flight; retry until the timeout.
      }
      if (Date.now() - start > timeout) throw new Error(`timed out waiting for: ${expression}`);
      await sleep(interval);
    }
  }

  close() {
    try { this.socket.close(); } catch { /* already closed */ }
  }
}

function waitForDevTools(child) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("timed out waiting for Chrome DevTools (20s)"));
    }, 20000);
    let buffer = "";
    const onData = (chunk) => {
      buffer += chunk.toString();
      const match = buffer.match(/DevTools listening on (ws:\/\/\S+)/);
      if (match) {
        cleanup();
        resolve(match[1]);
      }
    };
    const onExit = (code) => {
      cleanup();
      reject(new Error(`Chrome exited early (code ${code})`));
    };
    const cleanup = () => {
      clearTimeout(timer);
      child.stderr.off("data", onData);
      child.off("exit", onExit);
    };
    child.stderr.on("data", onData);
    child.on("exit", onExit);
  });
}

const userDataDir = mkdtempSync(join(tmpdir(), "pinkdb-verify-browser-"));
const child = spawn(chromePath, [
  "--headless=new",
  "--disable-gpu",
  "--hide-scrollbars",
  "--mute-audio",
  "--no-first-run",
  "--no-default-browser-check",
  "--user-data-dir=" + userDataDir,
  "--remote-debugging-port=0",
  "about:blank",
], { stdio: ["ignore", "ignore", "pipe"] });

const consoleErrors = [];
let cdp = null;

try {
  const browserWs = await waitForDevTools(child);
  const port = new URL(browserWs).port;
  const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
  const page = targets.find((target) => target.type === "page");
  if (!page) throw new Error("Chrome exposed no page target");
  cdp = await Cdp.connect(page.webSocketDebuggerUrl);

  cdp.onEvent((message) => {
    if (message.method === "Runtime.consoleAPICalled") {
      if (message.params.type === "error" || message.params.type === "assert") {
        consoleErrors.push(message.params.args.map((arg) => arg.value ?? arg.description ?? "").join(" "));
      }
    } else if (message.method === "Runtime.exceptionThrown") {
      consoleErrors.push(message.params.exceptionDetails.text || "uncaught exception");
    } else if (message.method === "Log.entryAdded") {
      const entry = message.params.entry;
      if (entry.level === "error") {
        // Include the URL: favicon 404s carry no "favicon" in the text.
        consoleErrors.push([entry.text, entry.url].filter(Boolean).join(" "));
      }
    }
  });

  await cdp.send("Page.enable");
  await cdp.send("Runtime.enable");
  await cdp.send("Log.enable");

  // ---------------------------------------------------------------- catalog

  const catalogResponse = await fetch(`${REPO}/index.json`);
  if (!catalogResponse.ok) throw new Error(`cannot fetch the catalog: HTTP ${catalogResponse.status}`);
  const catalog = await catalogResponse.json();
  const entries = Array.isArray(catalog.extensions) ? catalog.extensions : [];
  const entry = entries[0] || null;
  const total = entries.length;
  const totalText = total === 1 ? "1 extension" : `${total} extensions`;

  await cdp.navigate(`${DOCS}/extensions`);
  await cdp.waitFor(`document.getElementById("catalog") && (document.querySelectorAll("#catalog .card").length > 0 || document.getElementById("catalog").textContent.includes("No extensions"))`);
  check((await cdp.evaluate("document.title")) === "Extensions — pinkdb",
    "docs /extensions loads with the page title", await cdp.evaluate("document.title"));
  check(await cdp.evaluate(`!document.getElementById("ext-intro").hidden`),
    "docs list route shows the static intro");

  if (entry === null) {
    check(await cdp.evaluate(`document.getElementById("catalog").textContent.includes("No extensions have been published yet.")`),
      "docs page renders the empty-catalog state (the repository has no extensions)");
  } else {
    const script = await cdp.evaluate(`(() => {
      const card = document.querySelector("#catalog .card");
      return {
        text: card.textContent,
        download: card.querySelector("a[download]").getAttribute("href"),
        titleHref: card.querySelector("h3 a").getAttribute("href"),
        hash: card.querySelector(".hash").textContent,
        size: card.querySelector(".size").textContent,
      };
    })()`);
    check(script.text.includes(entry.name) && script.text.includes(entry.version) &&
      script.text.includes(entry.platform) && script.text.includes("abi " + entry.abi),
      `card renders ${entry.name} ${entry.version} (${entry.platform}, abi ${entry.abi})`);
    check(script.hash === entry.sha256.slice(0, 12) + "…",
      "card shows the truncated SHA-256", script.hash);
    check(script.download === `${REPO}/${entry.file}`,
      "card Download points at the repository artifact", script.download);
    check(script.titleHref === "#/" + encodeURIComponent(entry.name),
      "card title links to the details route", script.titleHref);
    check(script.size === formatSize(entry.size),
      `card size tolerates the catalog's ${Object.prototype.hasOwnProperty.call(entry, "size") ? "size" : "missing size"}`,
      script.size);

    const count = await cdp.evaluate(`document.getElementById("count").textContent`);
    check(count === totalText, "live filter starts with the full count", count);
    check(await cdp.evaluate(`document.querySelectorAll("#catalog [data-copy]").length > 0`),
      "cards expose copy buttons for hashes and commands");

    // Filter: no matches -> empty state + clear button; clear -> full grid.
    await cdp.evaluate(`(() => {
      const input = document.getElementById("filter");
      input.value = "zz-no-such-extension-zz";
      input.dispatchEvent(new Event("input", { bubbles: true }));
    })()`);
    await cdp.waitFor(`document.querySelector("#catalog .empty") !== null`);
    const emptyText = await cdp.evaluate(`document.getElementById("grid").textContent`);
    check(emptyText.includes("No extensions match"),
      "filter with no matches renders the empty state", emptyText);
    const filteredCount = await cdp.evaluate(`document.getElementById("count").textContent`);
    const totalWord = total === 1 ? "extension" : "extensions";
    check(filteredCount === `0 of ${total} ${totalWord}`,
      "filter count shows '0 of N extensions'", filteredCount);
    await cdp.evaluate(`document.querySelector("[data-clear-filter]").click()`);
    await cdp.waitFor(`document.querySelectorAll("#catalog .card").length > 0`);
    check((await cdp.evaluate(`document.getElementById("count").textContent`)) === totalText,
      "clear filter restores the full grid");

    // Filter by name -> at least that entry's card.
    await cdp.evaluate(`(() => {
      const input = document.getElementById("filter");
      input.value = ${JSON.stringify(entry.name)};
      input.dispatchEvent(new Event("input", { bubbles: true }));
    })()`);
    await cdp.waitFor(`document.querySelectorAll("#catalog .card").length > 0`);
    const names = await cdp.evaluate(`Array.from(document.querySelectorAll("#catalog .card h3")).map((h) => h.textContent)`);
    check(names.includes(entry.name), `filter by name finds ${entry.name}`, names.join(", "));
    await cdp.evaluate(`(() => {
      const input = document.getElementById("filter");
      input.value = "";
      input.dispatchEvent(new Event("input", { bubbles: true }));
    })()`);

    // Details: click the card title (real hash navigation).
    await cdp.evaluate(`document.querySelector("#catalog .card h3 a").click()`);
    await cdp.waitFor(`document.querySelector("#catalog h1") && document.querySelector("#catalog h1").textContent === ${JSON.stringify(entry.name)}`);
    const detailText = await cdp.evaluate(`document.getElementById("catalog").textContent`);
    check(detailText.includes(`.install ${entry.name}`) &&
      detailText.includes(`.install ${entry.name}@${entry.version}`),
      `details render '.install ${entry.name}' and the pinned command`);
    check(detailText.includes("Metadata") && detailText.includes(entry.sha256),
      "details render the metadata table with the full SHA-256");
    check(detailText.includes("All versions and platforms") && detailText.includes(entry.platform),
      "details render the all-versions & platforms table");
    check(detailText.includes("Raw catalog entry"),
      "details render the raw catalog entry");
    check(await cdp.evaluate(`document.getElementById("ext-intro").hidden`),
      "details route hides the static intro");
    const detailTitle = await cdp.evaluate("document.title");
    check(detailTitle === `${entry.name} — pinkdb`, "details route sets the page title", detailTitle);
    const downloads = await cdp.evaluate(`Array.from(document.querySelectorAll("#catalog a[download]")).map((a) => a.getAttribute("href"))`);
    check(downloads.length > 0 && downloads.every((href) => href === `${REPO}/${entry.file}`),
      "every detail Download points at the repository artifact", downloads.join(", "));

    // Not-found route.
    await cdp.evaluate(`location.hash = "#/definitely-not-in-the-catalog"`);
    await cdp.waitFor(`document.getElementById("catalog").textContent.includes("No such extension")`);
    check(true, "unknown details hash renders the not-found state");
    await cdp.evaluate(`location.hash = "#/"`);
    await cdp.waitFor(`document.querySelectorAll("#catalog .card").length > 0`);
    check(true, "back to the list route renders the cards again");
  }

  // ----------------------------------------------------------- worker page

  await cdp.navigate(`${REPO}/`);
  check((await cdp.evaluate("document.title")) === "pinkdb extension repository",
    "repository / loads the info page", await cdp.evaluate("document.title"));
  check((await cdp.evaluate(`document.querySelector("h1").textContent`)) === "pinkdb extension repository",
    "info page carries the repository heading");
  const docsLink = await cdp.evaluate(`(() => {
    const link = Array.from(document.querySelectorAll("a")).find((a) => (a.getAttribute("href") || "").endsWith("/extensions"));
    return link ? link.getAttribute("href") : null;
  })()`);
  check(Boolean(docsLink), "info page links to the docs Extensions page", String(docsLink));
  const indexFetch = await cdp.evaluate(`fetch("/index.json").then((response) => ({
    status: response.status,
    cors: response.headers.get("access-control-allow-origin"),
  }))`);
  check(indexFetch.status === 200 && indexFetch.cors === "*",
    "info page context: /index.json answers 200 with Access-Control-Allow-Origin: *",
    JSON.stringify(indexFetch));

  // ------------------------------------------------------------- console

  const errors = consoleErrors.filter((line) => !/favicon/i.test(line));
  check(errors.length === 0, "no console errors during the browser pass", errors.join(" | "));
} catch (error) {
  check(false, "headless browser run completes", error.message);
} finally {
  if (cdp) cdp.close();
  child.kill();
  await new Promise((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) return resolve();
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      resolve();
    }, 5000);
    child.once("exit", () => {
      clearTimeout(timer);
      resolve();
    });
  });
  try {
    rmSync(userDataDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
  } catch (error) {
    console.warn(`warn  could not remove the Chrome profile ${userDataDir}: ${error.message}`);
  }
}

console.log(`\n${passed} passed, ${failed} failed (docs ${DOCS}, repo ${REPO})`);
process.exit(failed === 0 ? 0 : 1);
