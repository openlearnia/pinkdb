// Playground UI. The worker owns the engine; this module only builds DOM,
// sends messages, and renders results. User data reaches the page as text
// nodes (textContent) — never innerHTML.

const EXAMPLES = [
  {
    id: "trips",
    title: "Create, insert, aggregate",
    sql: `DROP TABLE IF EXISTS trips;
CREATE TABLE trips (
  city VARCHAR,
  day DATE,
  riders INTEGER,
  fare DECIMAL(8,2)
);
INSERT INTO trips VALUES
  ('Berlin', DATE '2024-05-01', 120, 3.50),
  ('Berlin', DATE '2024-05-02', 180, 4.10),
  ('Lisbon', DATE '2024-05-01', 90, 2.75),
  ('Lisbon', DATE '2024-05-02', 140, 3.20),
  ('Lisbon', DATE '2024-05-03', 160, NULL);
SELECT city, COUNT(*) AS days, SUM(riders) AS riders,
       MIN(fare) AS cheapest
FROM trips
GROUP BY city
ORDER BY riders DESC;
`,
  },
  {
    id: "window",
    title: "Running totals with window functions",
    sql: `DROP TABLE IF EXISTS readings;
CREATE TABLE readings (sensor VARCHAR, ts DATE, reading DOUBLE);
INSERT INTO readings VALUES
  ('north', DATE '2024-06-01', 12.5),
  ('north', DATE '2024-06-02', 14.0),
  ('north', DATE '2024-06-03', 9.75),
  ('south', DATE '2024-06-01', 20.0),
  ('south', DATE '2024-06-02', 18.5);
SELECT sensor, ts, reading,
       SUM(reading) OVER (PARTITION BY sensor ORDER BY ts) AS running,
       ROW_NUMBER() OVER (PARTITION BY sensor ORDER BY ts) AS n
FROM readings
ORDER BY sensor, ts;
`,
  },
  {
    id: "join",
    title: "CTE + join",
    sql: `DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (id INTEGER, name VARCHAR, region VARCHAR);
CREATE TABLE orders (id INTEGER, customer_id INTEGER, total DECIMAL(10,2));
INSERT INTO customers VALUES (1, 'Ada', 'EU'), (2, 'Grace', 'US'), (3, 'Linus', 'EU');
INSERT INTO orders VALUES (10, 1, 25.50), (11, 1, 10.00), (12, 2, 99.99), (13, 3, 5.25);
WITH spend AS (
  SELECT customer_id, SUM(total) AS total
  FROM orders
  GROUP BY customer_id
)
SELECT c.name, c.region, s.total
FROM customers c
JOIN spend s ON s.customer_id = c.id
ORDER BY s.total DESC;
`,
  },
  {
    id: "types",
    title: "Types, NULLs and exact numbers",
    sql: `SELECT 42 AS answer,
       9007199254740993 AS big,
       2.5 AS pi_ish,
       'pink' AS colour,
       DATE '2024-01-15' AS day,
       CAST('1.25' AS DECIMAL(5,2)) AS price,
       NULL AS missing,
       TRUE AS flag;
`,
  },
];

const BACKEND_KEY = "pinkdb-playground-backend";
const AUTOSAVE_TEXT = {
  idle: "ready",
  scheduled: "in 0.5 s",
  saving: "saving…",
  saved: "saved",
  memory: "session only",
  readonly: "off (read-only)",
  protected: "off (refused snapshot)",
  error: "failed",
};
const NUMERIC_TYPES = new Set([
  "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
  "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
  "FLOAT", "REAL", "DOUBLE", "DECIMAL",
]);

const editor = document.getElementById("pg-editor");
const examples = document.getElementById("pg-examples");
const backendSelect = document.getElementById("pg-backend");
const runButton = document.getElementById("pg-run");
const saveButton = document.getElementById("pg-save");
const exportButton = document.getElementById("pg-export");
const importButton = document.getElementById("pg-import");
const importFile = document.getElementById("pg-import-file");
const clearButton = document.getElementById("pg-clear");
const keepButton = document.getElementById("pg-keep");
const resultsElement = document.getElementById("pg-results");
const noteElement = document.getElementById("pg-note");
const statusBackend = document.getElementById("pg-status-backend");
const statusAutosave = document.getElementById("pg-status-autosave");
const statusStorage = document.getElementById("pg-status-storage");
const statusVersion = document.getElementById("pg-status-version");

const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });
const pending = new Map();
let nextId = 1;
let runtime = { backend: "memory", label: "Memory", version: "", readonly: false, guarded: false };
let storageInfo = { usage: null, quota: null, persisted: null };
let autosave = { state: "idle", at: null };
let running = false;
let switching = false;

// ---------------------------------------------------------------- plumbing

function request(type, payload, transfer) {
  const id = nextId++;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    worker.postMessage({ type, id, ...(payload || {}) }, transfer || []);
  });
}

worker.addEventListener("message", (event) => {
  const message = event.data || {};
  if (message.type === "autosave") {
    autosave = message;
    renderStatus();
    return;
  }
  if (message.type === "storage") {
    storageInfo = { usage: message.usage, quota: message.quota, persisted: message.persisted };
    renderStatus();
    return;
  }
  if (message.type === "note") {
    showNote(message.message, message.tone);
    return;
  }
  const slot = message.id == null ? null : pending.get(message.id);
  if (!slot) {
    if (message.type === "error") {
      showNote(message.error || "the playground worker failed", "error");
    }
    return;
  }
  pending.delete(message.id);
  if (message.type === "error") {
    slot.reject(new Error(message.error || "the playground worker failed"));
  } else {
    slot.resolve(message);
  }
});

worker.addEventListener("error", (event) => {
  const text = event.message || "the playground worker failed";
  for (const slot of pending.values()) {
    slot.reject(new Error(text));
  }
  pending.clear();
  showNote(text, "error");
});

worker.addEventListener("messageerror", () => {
  showNote("The playground worker sent a message this browser could not read.", "error");
});

function readBackend() {
  try {
    const value = localStorage.getItem(BACKEND_KEY);
    if (value === "memory" || value === "indexeddb" || value === "opfs") {
      return value;
    }
  } catch (err) {
    // Storage may be denied; memory is always a safe start.
  }
  return "memory";
}

function rememberBackend(value) {
  try {
    localStorage.setItem(BACKEND_KEY, value);
  } catch (err) {
    // A denied localStorage does not affect the selected backend.
  }
}

function formatBytes(value) {
  if (!Number.isFinite(value)) {
    return "?";
  }
  if (value < 1024) {
    return `${value} B`;
  }
  const units = ["KB", "MB", "GB", "TB"];
  let size = value;
  let unit = -1;
  do {
    size /= 1024;
    unit += 1;
  } while (size >= 1024 && unit < units.length - 1);
  return `${size >= 10 ? size.toFixed(0) : size.toFixed(1)} ${units[unit]}`;
}

function formatMs(value) {
  if (!Number.isFinite(value)) {
    return "? ms";
  }
  return value < 10 ? `${value.toFixed(2)} ms` : `${value.toFixed(0)} ms`;
}

function clock(at) {
  const date = new Date(at);
  const pad = (n) => String(n).padStart(2, "0");
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function isNumeric(type) {
  return NUMERIC_TYPES.has(String(type || "").replace(/\(.*$/, "").toUpperCase());
}

function label(value) {
  return { memory: "Memory", indexeddb: "IndexedDB", opfs: "OPFS" }[value] || value;
}

// ---------------------------------------------------------------- status

function showNote(message, tone) {
  const text = message == null ? "" : String(message);
  noteElement.textContent = text;
  noteElement.dataset.tone = tone || "info";
  noteElement.hidden = text.length === 0;
}

function renderStatus() {
  const flags = [];
  if (runtime.readonly) {
    flags.push("read-only");
  }
  if (runtime.guarded) {
    flags.push("snapshot refused");
  }
  statusBackend.textContent = `store: ${runtime.label || label(runtime.backend)}${flags.length ? ` (${flags.join(", ")})` : ""}`;

  let autosaveText = AUTOSAVE_TEXT[autosave.state] || autosave.state || "ready";
  if (autosave.state === "saved" && Number.isFinite(autosave.at)) {
    autosaveText = `saved ${clock(autosave.at)}`;
  }
  statusAutosave.textContent = `autosave: ${autosaveText}`;

  if (Number.isFinite(storageInfo.usage) && Number.isFinite(storageInfo.quota)) {
    const persistent = storageInfo.persisted === true ? " · persistent" : "";
    statusStorage.textContent = `storage: ${formatBytes(storageInfo.usage)} of ${formatBytes(storageInfo.quota)} used${persistent}`;
  } else {
    statusStorage.textContent = "storage: quota unknown";
  }

  statusVersion.textContent = runtime.version
    ? `engine: pinkdb ${runtime.version} (wasm)`
    : "engine: loading…";
}

function updateControls() {
  runButton.disabled = running;
  saveButton.disabled = running || switching || runtime.readonly || runtime.guarded;
  importButton.disabled = running || switching || runtime.readonly || runtime.guarded;
  clearButton.disabled = running || switching || runtime.readonly;
  exportButton.disabled = running || switching;
  keepButton.disabled = switching || !(navigator.storage && typeof navigator.storage.persist === "function");

  saveButton.title = runtime.readonly
    ? "Open in another tab — this copy is read-only"
    : runtime.guarded
      ? "The saved snapshot was refused; use Clear first"
      : "Write a .pink snapshot to the selected storage";
  // The read-only / refused-snapshot reasons are shared; the normal action
  // differs (Import restores a file, Save writes the current database).
  importButton.title =
    runtime.readonly || runtime.guarded
      ? saveButton.title
      : "Restore a .pink file into the engine and save it to the selected storage";
  clearButton.title = runtime.readonly
    ? "Open in another tab — this copy is read-only"
    : "Delete the saved snapshot and start with an empty database";
}

function applyReady(message) {
  runtime = {
    backend: message.backend || "memory",
    label: message.label || label(message.backend),
    version: message.version || runtime.version,
    readonly: !!message.readonly,
    guarded: !!message.guarded,
  };
  if (message.storage) {
    storageInfo = message.storage;
  }
  if (message.autosave) {
    autosave = { state: message.autosave };
  }
  if (message.note) {
    showNote(message.note, "info");
  }

  backendSelect.value = runtime.backend;
  for (const option of backendSelect.options) {
    const probe = (message.availability || []).find((entry) => entry.name === option.value);
    option.disabled = !!(probe && !probe.ok);
    option.title = probe && !probe.ok ? probe.reason || "unavailable" : "";
  }
  renderStatus();
  updateControls();
}

// ---------------------------------------------------------------- results

function badge(text, extraClass) {
  const span = document.createElement("span");
  span.className = extraClass ? `badge ${extraClass}` : "badge";
  span.textContent = text;
  return span;
}

function makeResultHead(entry, meta) {
  const head = document.createElement("div");
  head.className = "pg-result-head";
  head.append(badge(entry.kind || "Result"));
  if (meta) {
    const metaElement = document.createElement("span");
    metaElement.className = "pg-result-meta";
    metaElement.textContent = meta;
    head.append(metaElement);
  }
  return head;
}

function renderResult(entry) {
  const card = document.createElement("article");
  card.className = "pg-result";

  if (entry && entry.kind === "Selected") {
    const names = Array.isArray(entry.names) ? entry.names : [];
    const types = Array.isArray(entry.types) ? entry.types : [];
    const rows = Array.isArray(entry.rows) ? entry.rows : [];
    card.append(
      makeResultHead(
        entry,
        `${rows.length} row${rows.length === 1 ? "" : "s"} × ${names.length} column${names.length === 1 ? "" : "s"}`,
      ),
    );

    const wrap = document.createElement("div");
    wrap.className = "table-wrap";
    const table = document.createElement("table");
    const head = table.createTHead();
    const headRow = head.insertRow();
    for (let column = 0; column < names.length; column += 1) {
      const th = document.createElement("th");
      th.scope = "col";
      if (isNumeric(types[column])) {
        th.className = "num";
      }
      const name = document.createElement("span");
      name.className = "pg-col-name";
      name.textContent = names[column] || `column ${column + 1}`;
      const type = document.createElement("span");
      type.className = "pg-col-type";
      type.textContent = types[column] || "";
      th.append(name, type);
      headRow.append(th);
    }
    const body = table.createTBody();
    for (const row of rows) {
      const tr = body.insertRow();
      for (let column = 0; column < names.length; column += 1) {
        const value = row[column];
        const td = tr.insertCell();
        if (isNumeric(types[column])) {
          td.className = "num";
        }
        if (value === null || value === undefined) {
          const nullValue = document.createElement("span");
          nullValue.className = "pg-null";
          nullValue.textContent = "NULL";
          td.append(nullValue);
        } else if (typeof value === "object") {
          td.textContent = JSON.stringify(value);
        } else {
          td.textContent = String(value);
        }
      }
    }
    wrap.append(table);
    card.append(wrap);
    if (rows.length === 0) {
      const empty = document.createElement("p");
      empty.className = "pg-empty";
      empty.textContent = "No rows.";
      card.append(empty);
    }
    return card;
  }

  let message = entry && typeof entry.message === "string" ? entry.message : "";
  if (!message && entry && typeof entry.count === "number") {
    message = `${entry.count} row${entry.count === 1 ? "" : "s"} affected.`;
  }
  if (!message) {
    message = entry && entry.kind ? `${entry.kind}.` : "Statement finished.";
  }
  card.append(makeResultHead(entry || { kind: "Result" }));
  const line = document.createElement("p");
  line.className = "pg-message";
  line.textContent = message;
  card.append(line);
  return card;
}

function renderError(message, elapsedMs) {
  const card = document.createElement("article");
  card.className = "pg-result pg-result-error";
  const head = document.createElement("div");
  head.className = "pg-result-head";
  head.append(badge("Error", "now"));
  if (Number.isFinite(elapsedMs)) {
    const meta = document.createElement("span");
    meta.className = "pg-result-meta";
    meta.textContent = formatMs(elapsedMs);
    head.append(meta);
  }
  const line = document.createElement("p");
  line.className = "pg-message";
  line.textContent = message;
  card.append(head, line);
  resultsElement.append(card);
}

function renderRun(message) {
  resultsElement.replaceChildren();

  const summary = document.createElement("p");
  summary.className = "pg-run-summary";
  summary.textContent = message.ok
    ? `${message.results.length} statement${message.results.length === 1 ? "" : "s"} in ${formatMs(message.elapsedMs)}`
    : `Failed after ${formatMs(message.elapsedMs)}`;
  resultsElement.append(summary);

  if (!message.ok) {
    renderError(message.error || "the statement failed", message.elapsedMs);
    return;
  }
  if (message.results.length === 0) {
    const empty = document.createElement("p");
    empty.className = "pg-empty";
    empty.textContent = "No statements ran.";
    resultsElement.append(empty);
    return;
  }
  for (const entry of message.results) {
    resultsElement.append(renderResult(entry));
  }
}

// ---------------------------------------------------------------- actions

function setRunning(value) {
  running = value;
  runButton.textContent = value ? "Running…" : "Run";
  runButton.setAttribute("aria-busy", String(value));
  updateControls();
}

async function runSql() {
  const sql = editor.value;
  if (!sql.trim()) {
    showNote("Write a statement first.", "error");
    return;
  }
  setRunning(true);
  try {
    const message = await request("run", { sql });
    renderRun(message);
    if (!message.ok) {
      showNote(message.error || "the statement failed", "error");
    }
  } catch (err) {
    resultsElement.replaceChildren();
    renderError(err.message || String(err));
    showNote(err.message || String(err), "error");
  } finally {
    setRunning(false);
  }
}

async function saveNow() {
  try {
    const message = await request("save");
    showNote(message.note || `Saved ${formatBytes(message.bytes)}.`, "ok");
  } catch (err) {
    showNote(err.message || String(err), "error");
  }
}

async function exportNow() {
  try {
    const message = await request("export");
    const bytes = message.bytes instanceof Uint8Array ? message.bytes : new Uint8Array(message.bytes);
    const blob = new Blob([bytes], { type: "application/octet-stream" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "pinkdb-playground.pink";
    document.body.append(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    showNote(`Exported ${formatBytes(bytes.length)} as pinkdb-playground.pink.`, "ok");
  } catch (err) {
    showNote(err.message || String(err), "error");
  }
}

async function importFileNow(file) {
  if (!file) {
    return;
  }
  if (file.size === 0) {
    showNote("That file is empty.", "error");
    return;
  }
  try {
    const bytes = new Uint8Array(await file.arrayBuffer());
    const message = await request("import", { bytes }, [bytes.buffer]);
    showNote(message.note || `Imported ${formatBytes(message.bytes)}.`, "ok");
  } catch (err) {
    showNote(err.message || String(err), "error");
  }
}

async function clearNow() {
  const owner = runtime.label || label(runtime.backend);
  const question = runtime.backend === "memory"
    ? "Reset the in-memory database? This cannot be undone."
    : `Delete the saved ${owner} snapshot and start with an empty database? This cannot be undone.`;
  const ok = window.confirm(question);
  if (!ok) {
    return;
  }
  try {
    const message = await request("clear");
    resultsElement.replaceChildren();
    showNote(message.note || "Cleared.", "ok");
  } catch (err) {
    showNote(err.message || String(err), "error");
  }
}

async function keepData() {
  if (!navigator.storage || typeof navigator.storage.persist !== "function") {
    showNote("This browser does not expose persistent storage.", "error");
    return;
  }
  keepButton.disabled = true;
  try {
    const granted = await navigator.storage.persist();
    if (navigator.storage.estimate) {
      const estimate = await navigator.storage.estimate();
      storageInfo = {
        usage: typeof estimate.usage === "number" ? estimate.usage : storageInfo.usage,
        quota: typeof estimate.quota === "number" ? estimate.quota : storageInfo.quota,
        persisted: granted,
      };
    } else {
      storageInfo = { ...storageInfo, persisted: granted };
    }
    renderStatus();
    showNote(
      granted
        ? "The browser granted persistent storage: it will not evict the snapshot to save space."
        : "The browser kept storage best-effort; it may evict the snapshot under pressure.",
      granted ? "ok" : "info",
    );
  } catch (err) {
    showNote(err.message || String(err), "error");
  } finally {
    updateControls();
  }
}

async function switchBackend(requested) {
  switching = true;
  backendSelect.disabled = true;
  updateControls();
  showNote(`Switching to ${label(requested)}…`);
  try {
    const message = await request("switch", { backend: requested });
    applyReady(message);
    rememberBackend(message.backend);
    showNote(message.note || `Using ${message.label}.`, message.readonly || message.guarded ? "info" : "ok");
  } catch (err) {
    showNote(err.message || String(err), "error");
    backendSelect.value = runtime.backend;
  } finally {
    switching = false;
    backendSelect.disabled = false;
    updateControls();
  }
}

// ---------------------------------------------------------------- wiring

examples.replaceChildren();
const placeholder = document.createElement("option");
placeholder.value = "";
placeholder.textContent = "Examples…";
examples.append(placeholder);
for (const example of EXAMPLES) {
  const option = document.createElement("option");
  option.value = example.id;
  option.textContent = example.title;
  examples.append(option);
}
editor.value = EXAMPLES[0].sql;

examples.addEventListener("change", () => {
  const example = EXAMPLES.find((entry) => entry.id === examples.value);
  if (example) {
    editor.value = example.sql;
    showNote(`Loaded example: ${example.title}.`);
    editor.focus();
  }
  examples.value = "";
});

runButton.addEventListener("click", runSql);
saveButton.addEventListener("click", saveNow);
exportButton.addEventListener("click", exportNow);
importButton.addEventListener("click", () => importFile.click());
importFile.addEventListener("change", async () => {
  const file = importFile.files && importFile.files[0];
  importFile.value = "";
  await importFileNow(file);
});
clearButton.addEventListener("click", clearNow);
keepButton.addEventListener("click", keepData);

backendSelect.addEventListener("change", () => {
  const requested = backendSelect.value;
  if (requested !== runtime.backend) {
    switchBackend(requested);
  }
});

editor.addEventListener("keydown", (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
    event.preventDefault();
    if (!running) {
      runSql();
    }
  }
});

renderStatus();
updateControls();

const initial = readBackend();
backendSelect.value = initial;
request("init", { backend: initial })
  .then((message) => {
    applyReady(message);
    rememberBackend(message.backend);
    showNote(message.note || `Using ${message.label}.`, "info");
  })
  .catch((err) => {
    runtime = { ...runtime, version: "" };
    renderStatus();
    showNote(`Could not start the playground: ${err.message || err}`, "error");
  });
