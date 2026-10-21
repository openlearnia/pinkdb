// The playground worker: it owns the wasm engine and the storage adapter, and
// the page talks to it with postMessage. Keeping the engine here makes a slow
// query or a large snapshot a non-event for the UI thread.
//
// Page -> worker messages: init {backend}, run {sql}, save, export,
// import {bytes}, clear, switch {backend}. Every message carries an id;
// replies carry the same id (ready, results, saved, exported, imported,
// cleared, error).
//
// Worker -> page events without an id: autosave {state}, storage
// {usage, quota, persisted}, note {tone, message}.

import { load } from "./pinkdb.js";
import {
  createIndexedDbAdapter,
  createMemoryAdapter,
  createOpfsAdapter,
} from "./storage.js";

const WASM_URL = new URL("./pinkdb.wasm", import.meta.url).href;
const AUTOSAVE_DELAY = 500;

const LABELS = {
  memory: "Memory",
  indexeddb: "IndexedDB",
  opfs: "OPFS",
};

const adapters = new Map([
  ["memory", createMemoryAdapter()],
  ["indexeddb", createIndexedDbAdapter()],
  ["opfs", createOpfsAdapter()],
]);

let availabilityPromise = null;
let engine = null;
let version = "";
let backend = "memory";
let adapter = adapters.get("memory");
let sourceAdapter = adapter;
let currentLock = null;
let readonly = false;
let guarded = false;
let autosaveTimer = 0;

// ---------------------------------------------------------------- messaging

function post(message, transfer) {
  self.postMessage(message, transfer || []);
}

function postNote(tone, message) {
  post({ type: "note", tone, message });
}

function postAutosave(state, extra) {
  post({ type: "autosave", state, ...(extra || {}) });
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

async function storageInfo() {
  let usage = null;
  let quota = null;
  let persisted = null;
  try {
    if (navigator.storage && typeof navigator.storage.estimate === "function") {
      const estimate = await navigator.storage.estimate();
      usage = typeof estimate.usage === "number" ? estimate.usage : null;
      quota = typeof estimate.quota === "number" ? estimate.quota : null;
    }
  } catch (err) {
    // Quota is advisory; the status line simply shows it as unknown.
  }
  try {
    if (navigator.storage && typeof navigator.storage.persisted === "function") {
      persisted = await navigator.storage.persisted();
    }
  } catch (err) {
    persisted = null;
  }
  return { usage, quota, persisted };
}

async function postStorage() {
  post({ type: "storage", ...(await storageInfo()) });
}

// ---------------------------------------------------------------- backend

async function probeAvailability() {
  if (!availabilityPromise) {
    availabilityPromise = (async () => {
      const result = [];
      for (const candidate of adapters.values()) {
        try {
          const probe = await candidate.available();
          result.push({
            name: candidate.name,
            ok: !!probe.ok,
            reason: probe.reason ? String(probe.reason) : "",
          });
        } catch (err) {
          result.push({
            name: candidate.name,
            ok: false,
            reason: err && err.message ? err.message : String(err),
          });
        }
      }
      return result;
    })();
  }
  return availabilityPromise;
}

function acquireLock(name) {
  if (!navigator.locks || typeof navigator.locks.request !== "function") {
    return Promise.resolve({ granted: true, release() {}, done: Promise.resolve() });
  }
  let release;
  const held = new Promise((resolve) => {
    release = resolve;
  });
  let decide;
  const granted = new Promise((resolve) => {
    decide = resolve;
  });
  const done = navigator.locks.request(
    `pinkdb-playground-${name}`,
    { ifAvailable: true },
    async (lock) => {
      if (!lock) {
        decide(false);
        return;
      }
      decide(true);
      await held;
    },
  ).catch(() => {
    decide(false);
  });
  return granted.then((ok) => ({ granted: ok, release, done }));
}

async function releaseCurrentLock() {
  const lock = currentLock;
  currentLock = null;
  if (!lock) {
    return;
  }
  lock.release();
  try {
    await lock.done;
  } catch (err) {
    // The lock is gone either way.
  }
}

async function ensureEngine() {
  if (!engine) {
    engine = await load(WASM_URL);
    version = engine.version();
  }
  engine.open();
  return engine;
}

function autosaveState() {
  if (readonly) {
    return "readonly";
  }
  if (guarded) {
    return "protected";
  }
  if (backend === "memory") {
    return "memory";
  }
  return "idle";
}

async function init(requested) {
  if (autosaveTimer) {
    clearTimeout(autosaveTimer);
    autosaveTimer = 0;
    // A run scheduled a save and the visitor switched before it fired: write
    // it to the old backend before the engine is replaced.
    if (!readonly && !guarded && backend !== "memory") {
      try {
        await doSave();
      } catch (err) {
        postNote("error", `Could not save before switching: ${err && err.message ? err.message : err}`);
      }
    }
  }
  await releaseCurrentLock();

  readonly = false;
  guarded = false;
  const notes = [];

  const availability = await probeAvailability();
  let target = adapters.has(requested) ? requested : "memory";
  const probe = availability.find((entry) => entry.name === target);
  if (target !== "memory" && (!probe || !probe.ok)) {
    const reason = probe && probe.reason ? probe.reason : "unknown reason";
    notes.push(`${LABELS[target] || target} is unavailable: ${reason}. Using memory instead.`);
    target = "memory";
  }

  backend = target;
  sourceAdapter = adapters.get(backend) || adapters.get("memory");
  adapter = sourceAdapter;

  if (backend !== "memory") {
    currentLock = await acquireLock(backend);
    if (!currentLock.granted) {
      readonly = true;
      adapter = adapters.get("memory");
      notes.push("Open in another tab — this tab runs on an in-memory copy (read-only).");
    }
  }

  await ensureEngine();

  let saved = null;
  try {
    saved = await sourceAdapter.load();
  } catch (err) {
    guarded = true;
    adapter = adapters.get("memory");
    notes.push(
      `Could not read the saved snapshot: ${err && err.message ? err.message : err}. ` +
        "It was left untouched and this tab runs on a new in-memory database.",
    );
  }

  if (saved && saved.length > 0) {
    const restored = engine.restore(saved);
    if (restored.ok) {
      notes.push(`Restored ${formatBytes(saved.length)} from ${LABELS[backend] || backend}.`);
    } else {
      guarded = true;
      adapter = adapters.get("memory");
      notes.push(
        `The saved snapshot was refused: ${restored.error || "unknown error"}. ` +
          "It was left untouched; this tab runs on a new in-memory database.",
      );
    }
  } else if (!guarded) {
    notes.push(
      backend === "memory"
        ? "New in-memory database. Memory is session-only."
        : `New database. Writes autosave a .pink snapshot to ${LABELS[backend] || backend}.`,
    );
  }

  const storage = await storageInfo();
  return {
    type: "ready",
    backend,
    label: LABELS[backend] || backend,
    version,
    readonly,
    guarded,
    note: notes.join(" "),
    autosave: autosaveState(),
    availability,
    storage,
  };
}

// ---------------------------------------------------------------- operations

async function doSave() {
  if (!engine) {
    throw new Error("the engine is not ready");
  }
  if (readonly) {
    throw new Error("Open in another tab — this copy is read-only.");
  }
  if (guarded) {
    throw new Error("The saved snapshot was refused; use Clear to remove it before saving.");
  }
  const bytes = engine.snapshot();
  postAutosave("saving");
  try {
    await adapter.save(bytes);
  } catch (err) {
    postAutosave("error", { message: err && err.message ? err.message : String(err) });
    await postStorage();
    throw err;
  }
  postAutosave("saved", { at: Date.now(), bytes: bytes.length, persisted: backend !== "memory" });
  await postStorage();
  return bytes;
}

function scheduleAutosave() {
  if (autosaveTimer) {
    clearTimeout(autosaveTimer);
  }
  postAutosave("scheduled");
  autosaveTimer = setTimeout(() => {
    autosaveTimer = 0;
    enqueue(() => doSave()).catch((err) => {
      postNote("error", `Autosave failed: ${err && err.message ? err.message : err}`);
    });
  }, AUTOSAVE_DELAY);
}

async function handleRun(message) {
  if (!engine) {
    throw new Error("the engine is not ready");
  }
  const started = performance.now();
  const result = engine.run(typeof message.sql === "string" ? message.sql : "");
  const elapsedMs = performance.now() - started;

  if (!readonly && !guarded && backend !== "memory") {
    const changed = !result.ok
      || (Array.isArray(result.results) && result.results.some((entry) => entry.kind !== "Selected"));
    if (changed) {
      scheduleAutosave();
    }
  }

  return {
    type: "results",
    id: message.id,
    ok: result.ok,
    results: Array.isArray(result.results) ? result.results : [],
    error: result.ok ? undefined : result.error,
    elapsedMs,
    autosave: autosaveState(),
  };
}

async function handleSave(message) {
  if (!engine) {
    throw new Error("the engine is not ready");
  }
  if (readonly) {
    throw new Error("Open in another tab — this copy is read-only.");
  }
  if (guarded) {
    throw new Error("The saved snapshot was refused; use Clear to remove it before saving.");
  }
  if (backend === "memory") {
    const bytes = engine.snapshot();
    postAutosave("memory");
    await postStorage();
    return {
      type: "saved",
      id: message.id,
      bytes: bytes.length,
      persisted: false,
      note: "Memory is session-only; Export downloads the snapshot.",
    };
  }
  const bytes = await doSave();
  return {
    type: "saved",
    id: message.id,
    bytes: bytes.length,
    persisted: true,
  };
}

function handleExport(message) {
  if (!engine) {
    throw new Error("the engine is not ready");
  }
  const bytes = engine.snapshot();
  post({ type: "exported", id: message.id, bytes }, [bytes.buffer]);
}

async function handleImport(message) {
  if (!engine) {
    throw new Error("the engine is not ready");
  }
  if (readonly) {
    throw new Error("Open in another tab — this copy is read-only.");
  }
  if (guarded) {
    throw new Error("The saved snapshot was refused; use Clear to remove it before importing.");
  }
  let bytes = message.bytes;
  if (!(bytes instanceof Uint8Array)) {
    bytes = new Uint8Array(bytes);
  }
  const restored = engine.restore(bytes);
  if (!restored.ok) {
    throw new Error(restored.error || "the engine refused the snapshot");
  }
  let note = "Imported into this tab only; the memory backend does not persist.";
  let persisted = false;
  if (backend !== "memory") {
    try {
      await doSave();
      persisted = true;
      note = `Imported ${formatBytes(bytes.length)} into ${LABELS[backend] || backend}.`;
    } catch (err) {
      note = `Imported, but saving failed: ${err && err.message ? err.message : err}`;
    }
  }
  return {
    type: "imported",
    id: message.id,
    bytes: bytes.length,
    persisted,
    note,
  };
}

async function handleClear(message) {
  if (!engine) {
    throw new Error("the engine is not ready");
  }
  if (readonly) {
    throw new Error("Open in another tab — this copy is read-only.");
  }
  if (autosaveTimer) {
    clearTimeout(autosaveTimer);
    autosaveTimer = 0;
  }
  await sourceAdapter.clear();
  engine.open();
  guarded = false;
  adapter = backend !== "memory" && currentLock && currentLock.granted
    ? sourceAdapter
    : adapters.get("memory");
  postAutosave(autosaveState());
  await postStorage();
  return {
    type: "cleared",
    id: message.id,
    note: "Saved snapshot removed; the database is empty.",
  };
}

async function handleMessage(message) {
  switch (message.type) {
    case "init":
    case "switch": {
      const ready = await init(message.backend);
      ready.id = message.id;
      post(ready);
      return;
    }
    case "run":
      post(await handleRun(message));
      return;
    case "save":
      post(await handleSave(message));
      return;
    case "export":
      handleExport(message);
      return;
    case "import":
      post(await handleImport(message));
      return;
    case "clear":
      post(await handleClear(message));
      return;
    default:
      throw new Error(`unknown message: ${message.type}`);
  }
}

// All messages run in order; the autosave timer joins the same queue so it
// can never race a switch or a clear.
let queue = Promise.resolve();

function enqueue(task) {
  const run = queue.then(task, task);
  queue = run.catch(() => {});
  return run;
}

self.addEventListener("message", (event) => {
  const message = event.data || {};
  enqueue(() => handleMessage(message)).catch((err) => {
    const text = err && err.message ? err.message : String(err);
    post({ type: "error", id: message.id == null ? null : message.id, error: text });
    postNote("error", text);
  });
});
