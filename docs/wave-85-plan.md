# Wave 85 — browser playground + storage (plan)

Started 2026-10-21; **done** the same day. ROADMAP row 85: run pinkdb in the browser on the docs
website — a `pinkdb-wasm` glue crate over the wasm32-unknown-unknown core,
an interactive SQL editor + result grid as self-hosted assets (no CDNs),
memory / IndexedDB / OPFS storage behind one adapter over the snapshot
bytes, a Web Worker, autosave, quota UX, Web Locks, atomic saves, clean
refusal of bad snapshots, no COOP/COEP/SharedArrayBuffer. WAL-grade VFS is
a follow-up; v1 persists whole snapshots.

## Decisions

- **No wasm-bindgen.** The toolchain here has no wasm-bindgen/wasm-pack
  and the roadmap allows "JS exports": `pinkdb-wasm` is a `cdylib` with a
  tiny raw C-ABI (below) and a hand-written ES module loader. The only
  `unsafe` is pointer/length marshaling in that crate (same carve-out as
  `pinkdb-ffi`). Release profile tuned for size; no wasm-opt.
- **Results cross as JSON** (UTF-8). BIGINT/DECIMAL values that a JS number
  cannot hold exactly cross as strings; DATE/TIMESTAMP/INTERVAL as their
  canonical text.
- **Engine API addition:** `PinkDb::from_pink_bytes(&[u8]) -> Result<PinkDb>`
  (public wrapper over the private loader, same validation and refusals —
  a newer format or corrupt bytes are clean errors).
- **Build:** `website/tools/build-wasm.sh` builds
  `cargo build -p pinkdb-wasm --target wasm32-unknown-unknown --release`
  (with the rustup stable toolchain; see the script) and copies the module
  to `website/src/assets/playground/pinkdb.wasm`; `build.py` copies assets
  to `public/` as usual; `deploy.sh` runs the wasm build first.

## The wasm ABI (`crates/pinkdb-wasm`)

All strings are UTF-8 byte ranges in wasm linear memory; every buffer the
module returns is length-prefixed and must be freed by the caller.

```text
pw_alloc(len: u32) -> *mut u8              // caller fills it, then passes it in
pw_free(ptr: *mut u8, len: u32)            // frees pw_alloc buffers and returned buffers
pw_open() -> u32                           // new in-memory database; handle > 0
pw_close(handle: u32)
pw_exec(handle: u32, sql: *const u8, len: u32) -> *mut u8
    // returns a buffer: [u32 LE byte length][UTF-8 JSON]; free with
    // pw_free(ptr, 4 + length). JSON: {"ok":true,"results":[...]} or
    // {"ok":false,"error":"..."} (the results of statements that ran before
    // the error are not reported, matching PinkDb::execute)
pw_snapshot(handle: u32) -> *mut u8
    // [u32 LE length][.pink bytes] or [u32 0xFFFFFFFF][u32 len][UTF-8 error]
pw_restore(handle: u32, bytes: *const u8, len: u32) -> *mut u8
    // replaces the handle's database with the snapshot; returns a JSON
    // buffer like pw_exec: {"ok":true} or {"ok":false,"error":"..."}
pw_version() -> *mut u8                    // JSON {"version":"…"}
```

One result object per statement: `{"kind":"Selected","names":[...],
"types":[...],"rows":[[...]]}`; `{"kind":"Inserted","count":3}` (also
Updated/Deleted); other kinds `{"kind":"<QueryResult variant>",
"message":"<the CLI's one-line text if easy, else the kind>"}`. Panics are
caught (`catch_unwind`) and reported as `{"ok":false}` with "internal
error".

## The site (`website/`)

- New page `src/pages/playground.html` → `public/playground.html` (top-nav
  entry "Playground"), layout `home`-like width, zine design tokens from
  `style.css`.
- `src/assets/playground/`: `pinkdb.wasm`, `pinkdb.js` (ES module loader:
  instantiate, marshal strings, run SQL -> results), `worker.js` (owns
  the engine and the storage adapter; the page talks to it with
  `postMessage`), `storage.js` (memory / IndexedDB / OPFS adapters with
  one interface: `load() -> Uint8Array|null`, `save(bytes)`, `clear()`,
  `name`), `playground.js` (UI), `playground.css`.
- **Storage:** memory (default, session-only); IndexedDB (one object store,
  one key, written in a single transaction — atomic); OPFS
  (`createSyncAccessHandle` inside the worker; write `db.pink.tmp`, flush,
  then swap to `db.pink` via `move()` when available, else a versioned
  file + a small pointer file). Feature-detect and disable unavailable
  backends with a reason.
- **Autosave:** after any statement that is not a pure read, debounce 500 ms
  then snapshot + save; explicit Save, Export (download `.pink`), Import
  (file picker → `pw_restore`), Clear.
- **Quota UX:** `navigator.storage.estimate()` shown; "Keep data" button
  calls `navigator.storage.persist()`.
- **Multi-tab:** `navigator.locks.request("pinkdb-playground-<backend>",
  {ifAvailable:true})` held by the tab that owns a persistent backend;
  other tabs show "open in another tab" and run read-only (memory copy).
- **Errors:** a snapshot that `pw_restore` refuses (newer format, corrupt)
  is reported with the engine's message and never overwritten silently.
- **Editor:** textarea with Ctrl/Cmd+Enter to run, a few example queries,
  a result grid (escaped text, NULL styled), errors shown inline, timing
  per run.

## Tasks

- **T1 (OpenCode):** `crates/pinkdb-wasm` (workspace member), the ABI above,
  `PinkDb::from_pink_bytes`, `website/tools/build-wasm.sh`, and
  `website/tools/wasm-smoke.mjs` — a Node script that loads the built
  module and exercises every export (create/insert/select, error JSON,
  snapshot → restore into a new handle, a corrupt snapshot refused). Any
  wasm runtime panic in the core (e.g. a clock read) is fixed minimally
  in the core with a comment.
- **T2 (CommandCode):** the site page + assets above, `build.py` page entry
  and nav, `deploy.sh` wasm step, no third-party requests.
- **T3 (me):** browser verification in the built-in browser (memory,
  IndexedDB, OPFS round trips, reload persistence, import/export, two
  tabs), gates, docs.

## Rules for delegated agents

Do not commit, push or create branches. Touch only the files your task
names. Run the task's checks before finishing and report what you changed
and the results.

## How it went

- T1 first went to OpenCode, which hung for 55 minutes with no output or
  file changes; it was stopped and the same brief ran on CommandCode
  (~10 min). No core changes were needed for wasm: rayon 1.13 falls back
  to one thread, and the plain execute path reads no clock. Module:
  2,359,016 bytes.
- T2 (CommandCode) built the page, worker, storage adapters and site
  integration, tested with harnesses over the real wasm (stubbed locks and
  OPFS).
- Verified in the desktop app's browser (Chromium) against `wrangler dev`:
  all four examples; Memory, IndexedDB and OPFS each persisting across a
  reload (OPFS left only `db.pink`: the atomic rename path); a corrupt
  import refused with "not a .pink file (bad magic)" and the stored
  snapshot byte-identical afterwards; a valid import + Export round trip
  byte-identical (`PINKDB05`); a second tab read-only with the lock held by
  the first; no console errors; no horizontal overflow at 375 px.
- Review fix: the Import button's tooltip repeated Save's text.
- CI's wasm job now builds the module fresh and runs the Node smoke test.
