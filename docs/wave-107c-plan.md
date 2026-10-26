# Wave 107c — the `embeddings` extension

Row 107 of the roadmap (part 3), after 107a (the fixed-size `ARRAY` type) and
107b (`vss`). This wave adds `embed(text)`: a linked extension package that
turns text into a `FLOAT[N]` embedding by calling an OpenAI-compatible
`/embeddings` endpoint, batched and cached, off by default.

The shape follows `extensions/agent` (wave 106) and `extensions/fts`
(wave 108a): the crate is `pinkdb-embeddings`, an `rlib` package (a linked
package is statically linked and exports no ABI entry point, so no `cdylib` —
same as `fts`) published with `register_extension_package`, behind an
`embeddings` cargo feature of `pinkdb-cli` and `pinkdb-server`. The engine grows by two small,
separate changes (§4 batching, §7 policy); no HTTP, key or cache code lives in
`crates/pinkdb`.

## 1. Packaging and size

- `extensions/embeddings`, crate `pinkdb-embeddings`, `[lib] name =
  "pinkdb_embeddings"` (an `rlib`: the package is linked into the host, so it
  exports no C entry point), edition 2024, `publish = false`.
- Dependencies: `pinkdb` (host hooks), `ureq 3.4` (`default-features = false,
  features = ["rustls"]`, as the agent uses it), `serde_json 1`, `sha2 0.10`.
  All three are already in `Cargo.lock` (the agent and the CLI's dev-deps), so
  the lock does not change.
- No shared crate with `extensions/agent`: the agent's provider is tied to
  chat completions (SSE accumulator, message shapes). The pieces worth
  sharing are ~60 lines (key resolution, redacted `Debug`, retry loop); a
  `pinkdb-warehouse-tools`-style crate for that is more code than it saves.
  The embeddings client mirrors the agent's request loop instead.
- `crates/pinkdb-cli` and `crates/pinkdb-server` gain an optional dependency
  and a feature `embeddings = ["dep:pinkdb-embeddings"]`, **off by default**.
  `register_builtin_extensions()` publishes the package and
  `load_builtin_extensions()` activates it on each opened store, like `fts`.
  Default binary sizes, with and without the feature, are measured and
  reported (one scratch target dir, deleted afterwards).

## 2. The type at bind time

`embed(text)` has the static result type `FLOAT[N]`. `HostScalar` already
declares its result as `ExtType`, and 107a made that carry the length:
`ExtType::ArrayFloat { len }` (ABI tag 12) maps to
`DataType::Array { elem: ArrayElem::Float, len }` (`crates/pinkdb/src/ext.rs`,
`dtype_of_ext`). So no engine change is needed for the type.

`N` comes from `PINKDB_EMBEDDINGS_DIMENSIONS` (default `1024`, z.ai's
documented default output size for `embedding-3`) and is read when the
package is built, i.e. at `register_builtin_extensions()` / `ensure_registered()`
— before any statement binds, but never during binding and never over the
network. It is also sent as the OpenAI `dimensions` request parameter, so the
provider returns exactly `N` floats. A response of any other length is an
error naming `PINKDB_EMBEDDINGS_DIMENSIONS`, never a silently truncated or
padded vector.

The two-argument form `embed(text, model)` is **not** implemented. The model
is a session setting (below); a per-row model would split every chunk into
per-model batches, change the cache key per row, and produce a result type
that depends on data. The brief allows dropping it.

Dimension read at registration, not at first call: the result type must be
static at bind time, so `N` is fixed for the process. The key, base URL and
model are still resolved lazily (§3) — nothing fails at startup, and the
default build has no such code at all.

## 3. Configuration

Read from the process environment, once per batch call (cheap; a changed key
or URL takes effect on the next statement, and tests stay independent):

| variable | default | meaning |
| --- | --- | --- |
| `PINKDB_EMBEDDINGS_BASE_URL` | required | endpoint URL |
| `PINKDB_EMBEDDINGS_MODEL` | required | model name |
| `PINKDB_EMBEDDINGS_DIMENSIONS` | `1024` | `N` in `FLOAT[N]`; sent as `dimensions` |
| `PINKDB_EMBEDDINGS_BATCH_SIZE` | `128` | inputs per HTTP request |
| `PINKDB_EMBEDDINGS_CACHE_DIR` | unset | optional on-disk cache directory |
| `PINKDB_EMBEDDINGS_API_KEY` | required | key; no fallback to another provider's variable |
| `PINKDB_EMBEDDINGS_LIVE` | unset | only read by the ignored live test |

**No default provider (reviewer, 2026-10-26).** The first draft defaulted to
z.ai (`https://api.z.ai/api/paas/v4/embeddings`, `embedding-3`). The live test
showed z.ai's global API answers HTTP 400 "Unknown Model" for it (`embedding-3`
is served only by Zhipu's `open.bigmodel.cn`), and the user decided the agent
has no default provider either. So the endpoint and the model are required,
and only `DEFAULT_DIMENSIONS` remains a compiled-in default. The key fallback to
`PINKDB_AGENT_API_KEY`/`Z_API_KEY` is gone too: a key is only sent to the
provider it was exported for.

Key rules, identical to wave 106:

- never auto-load `.env`; the key is read from the process environment only;
- never persisted: not in a table, not in the WAL, not in a log line, not in
  an error, not in `Debug` (the config's `Debug` prints `<redacted>`);
- `SET` cannot reach it (it is not a session variable);
- a sentinel-key test scans the store directory, the WAL and stderr.

The client is built on a blocking `ureq::Agent` (as the agent):
`timeout_connect(10s)`, `timeout_recv_response(60s)`, `http_status_as_error(false)`,
`max_redirects(4)`, `user_agent("pinkdb-embeddings/<version>")`.

## 4. Batching (the engine change)

Today `eval_dynamic_scalar` (`crates/pinkdb/src/exec/expr.rs`) calls a host
scalar once per row with `&[Value]`, so a scalar `embed()` would be one HTTP
request per row. Change, separate and small:

- `HostScalar` gains an optional chunk evaluator:
  `HostScalar::batched(name, params, ret, f)` where
  `f: Fn(&PinkDb, &[Vector]) -> Result<Vector>` receives every argument vector
  for the chunk at once and returns the result column. The existing per-row
  `new` keeps working; a scalar has at most one of the two evaluators (a
  batched one has no per-row fallback, and calling `call()` on it is an
  internal error).
- `eval_dynamic_scalar` prefers the chunk evaluator: evaluate the arguments
  vector-at-a-time (which it already does), call it once, validate the
  returned vector's type against the bound `dtype`, and return. The per-row
  loop is untouched for every existing host scalar.

The extension then, per chunk (`CHUNK_SIZE = 2048` rows):

1. collects the non-NULL inputs, deduplicated (`HashMap<&str, first index>`);
2. serves what the cache has; splits the rest into batches of
   `PINKDB_EMBEDDINGS_BATCH_SIZE`;
3. one request per batch, parsing `data[i].embedding` by `index` (the provider
   may return them out of order);

`NULL` inputs produce `NULL` and are never sent. Empty strings are sent as
the provider expects (OpenAI accepts them and returns a vector of zeros);
that is documented, not silently skipped.

## 5. Volatility and replay

`embed()` is nondeterministic in principle (a network call, provider
dependent): it must never be constant-folded, evaluated during binding, or
evaluated by a `WHERE` that does not select it. That is already guaranteed
without a flag: `BoundExpr::DynamicScalar` is opaque to the optimizer (never
folded, never pushed through a join) and the binder only resolves it.

The engine's `volatile` bit means something narrower: a statement the engine
WAL-logs may not call a volatile *dynamic* function
(`bind_tracked_with`, `crates/pinkdb/src/sql/binder.rs`). That guard exists
because replay re-executes the logged SQL text. Applying it to `embed` would
refuse `UPDATE docs SET v = embed(body)` — the primary use case — in every
store, including the in-memory one (the guard is applied uniformly). So
`embed` is *not* registered WAL-volatile; it is registered opaque (the
default for a host scalar).

Opaque is not enough for durability. A replay that calls the provider cannot
open the store when the provider is unreachable or the key is unset at
reopen, and it diverges when the provider answers differently. So the engine
records a **replay memo** (§11): every result an `external_io` host scalar
produced for a logged statement, keyed by (function, arguments),
deduplicated, and carried in that statement's WAL record. Replay serves the
memo and never invokes the function; a memo miss is a hard error naming the
function. The function must still be *registered* (the record's
`LOAD EXTENSION` prefix resolves the package), so a store opened by a build
without the `embeddings` feature fails at the recorded `LOAD EXTENSION` with
the engine's documented fail-closed error — a deliberate consequence,
reported rather than papered over.

The tests pin the observable guarantees: no request during `EXPLAIN`, a
`WHERE false` (zero rows) makes no request, an `UPDATE` is accepted, a
replayed `UPDATE` with the provider stopped and the key unset returns the
recorded vectors bit for bit, and a provider that answers differently the
second time cannot change the replayed store.

## 6. Caching

- In-process LRU, bounded in bytes (64 MiB), keyed by
  `sha256(base_url \0 model \0 dimensions \0 text)`, value `Arc<[f32]>`.
  Bounded by bytes, not entries: a vector is `4*N` bytes.
- Optional persistent cache: `PINKDB_EMBEDDINGS_CACHE_DIR`, one file per key,
  named by the hex digest, containing `N` little-endian `f32`s. Written
  atomically (`<digest>.tmp-<pid>` + rename); a wrong-length file is ignored
  (a miss), never an error. The directory is created on first write; it is
  read-only-failure tolerant (a cache that cannot be written is a warning-free
  miss — the SQL result is what matters).
- A cache hit makes no request; the persistent cache survives a restart (two
  CLI processes in the test, the second makes zero requests).
- Nothing is written into the user's store from inside a `SELECT`.

## 7. Network gating (MCP / agent)

`ServePolicy::allow_files` is today's single "this session may reach outside
the store" switch; the serve check refuses statements that read files or
URLs. `embed()` is external I/O to a paid service, so it is gated by the same
switch, with a distinct hint.

`HostScalar` gains an `external_io()` flag (builder, docs: "a call leaves the
process and may cost money"). The gate has two layers, because binding is not
a security boundary:

- the **bind-time** check in `serve::checked` refuses a statement whose bound
  plan recorded an `external_io` host scalar (an early, friendly error), and
- the **call-site** check in the executor refuses the call itself. The
  session's setting rides in the per-statement `ExecBudget` (a store-level
  bool would race between concurrent sessions on one `Database`, and a
  thread-local is wrong under rayon), so a call reached through a
  multi-statement batch, a macro, a view, a trigger action, a `CALL`/`DO`
  body or a prepared statement is refused before any request.

The refusal is a `permission` error naming `allow-files`
(`PinkDbError::Permission`, mapped to `"kind":"permission"`).

Result: the read-only MCP/agent default (and `ServePolicy::default()`)
refuses `embed()` with `allow-files` off; `--mcp-allow-files` /
`--agent-allow-files` / `.mcp serve allow-files` permit it — exactly today's
file switch, no new flag. Both layers are tested, including the batch,
macro, view, trigger and `DO` cases.

## 8. Errors, retries, timeouts

Same shape as the agent:

- 401 → `"the embeddings provider rejected the API key (HTTP 401); check
  PINKDB_EMBEDDINGS_API_KEY"` (the key never appears, the body is not echoed);
- 429 and 5xx → retry, 3 attempts total, 250 ms then 500 ms backoff;
- transport failure / timeout → a clear error naming the URL; not retried
  (a timeout is already bounded by the client's receive timeout);
- other 4xx → the provider's body, truncated to 200 chars of its first line
  (`shorten`), never the request body (which holds user text) and never the
  key;
- a wrong vector length → an error naming `PINKDB_EMBEDDINGS_DIMENSIONS`;
- a malformed body → `"the embeddings provider returned malformed JSON"` plus
  a short reason.

## 9. Tests (all CI-safe, 127.0.0.1 only)

The mock provider is the agent's recorded pattern: a `TcpListener` on
`127.0.0.1:0`, the real port read back, requests parsed (request line,
headers, `Content-Length` body), replies deterministic vectors derived from
the input text (so a test can predict the value), and a counter plus the
recorded bodies kept for assertions. The `Authorization` header must be
`Bearer <sentinel>`.

`extensions/embeddings/tests/provider.rs` (client only, no store):

- request shape: model, `input` array, `dimensions`, bearer sentinel;
- batches of 128 for 300 unique inputs → 3 requests; duplicates deduplicated;
- dimension mismatch names the setting; malformed JSON; 401 (key not echoed);
  429 then success; 5xx retries; timeout (short client timeout in the test);
- cache: second call makes zero requests; a bounded LRU evicts.

`extensions/embeddings/tests/engine.rs` (package + `PinkDb`):

- `typeof(embed('x'))` is `FLOAT[N]`; binder type is `DataType::Array`;
- 1,000 rows with 300 unique texts → 3 requests; NULL rows → `NULL` and no
  input for them;
- second query over the same texts → zero requests (cache);
- dimension mismatch through SQL; `EXPLAIN` and `WHERE false` make no
  request; `UPDATE` is accepted (volatility);
- gate: `serve::execute` with the default policy refuses with `allow-files`
  off (mentions `allow-files`), succeeds with it on; a batch whose `SELECT`
  cannot bind at check time, a macro, a view, a trigger action and a `DO`
  body are each refused with zero requests and allowed with the switch;
- end to end: `CREATE TABLE docs`, `INSERT`, `UPDATE docs SET v = embed(body)`,
  then a brute-force top-k with plain SQL arithmetic (107b's
  `array_cosine_distance` is not on main; the distance is computed in the test
  as `sum(((a[i] - b[i]) * (a[i] - b[i])))` over a fixed `N`), never behind a
  `cfg` that would silently skip it.

`crates/pinkdb-cli/tests/embeddings.rs` (feature `embeddings`, subprocesses):

- the sentinel key never appears in the store directory, the WAL or stderr
  (mock provider, real CLI process);
- persistent cache: two CLI runs with one `PINKDB_EMBEDDINGS_CACHE_DIR`, the
  second makes zero requests (a real process restart);
- no key at all is a clear error naming `PINKDB_EMBEDDINGS_API_KEY`.

One `#[ignore]`d live test, run only with `PINKDB_EMBEDDINGS_LIVE=1` and a key
in the environment: it is written and never run here.

## 10. Deliverables

Code, then ROADMAP row 107 (107c done), CHANGELOG, README "Embeddings"
section, the website SQL reference page, `ci.yml` wiring, and the regenerated
website (`make-changelog.py`, `build.py`; the playground wasm is not rebuilt).

Gates: the eight commands listed in the brief (fmt, clippy default and with
`--features embeddings`, the workspace tests, the embeddings crate tests, the
CLI with `embeddings`, the CLI with `pinklake,s3,parquetlake`, and the
`wasm32-unknown-unknown` engine build), with `df -h` checked before each
feature set. The release-size builds are not gates: they briefly filled the
shared disk, and their two target dirs were deleted afterwards.

## 11. As built

- Files: `extensions/embeddings/{Cargo.toml,src/{lib,client,cache}.rs,
  tests/{support/mod.rs,engine.rs,live.rs}}`,
  `crates/pinkdb-cli/tests/embeddings.rs`; engine: `crates/pinkdb/src/
  package.rs` (`HostScalar::batched`, `HostScalar::external_io`),
  `crates/pinkdb/src/exec/expr.rs` (the chunk call, its row-count and dtype
  checks, the replay lookup, the policy gate and the memo recording),
  `crates/pinkdb/src/exec/mod.rs` (`ExecBudget` carries the per-statement
  `external_services` switch and the call collector), `crates/pinkdb/src/
  wal.rs` (`ExtCall`, the v5 memo section and its codec), `crates/pinkdb/
  src/{storage,db,lib}.rs` (the collector, `wal_txn_memo`, the `Txn` memo,
  `ext_replay`), `crates/pinkdb/src/{procedure,trigger}.rs` (budget
  threading through bodies, `CALL`/`DO`, trigger actions and `WHEN`),
  `crates/pinkdb/src/{types,agent}.rs` (`PinkDbError::Permission` and its
  `"permission"` kind), `crates/pinkdb/src/plan.rs`
  (`dynamic_scalar_names`), `crates/pinkdb/src/serve.rs` (`SERVICES_HINT`,
  the `set_external_services` plumbing); `pinkdb-cli`/`pinkdb-server`
  feature wiring.
- Release sizes: `pinkdb` 5 208 656 B without `embeddings`, 7 252 992 B with
  it (+2 044 336 B, +39.2%); `pinkdb-server` 4 821 232 → 6 845 456 B
  (+2 024 224 B, +42.0%). The default builds are untouched.
- Replay memo: a logged statement's record carries every `external_io`
  host-scalar result it produced, keyed by (function, arguments) and
  deduplicated. The collector lives in the statement's `ExecBudget`, so
  trigger bodies, procedure bodies and `CALL`/`DO` statements contribute to
  the same record; an in-transaction statement buffers into `wal_txn_memo`,
  and every append funnel (autocommit, `COMMIT`, CALL segments, MV refresh)
  passes it to `Wal::append`. WAL payload version 5 appends the memo after
  the statements; a record without one (v3/v4 payload, or no external call)
  decodes as empty. Replay installs the record's memo on the store, and
  `eval_dynamic_scalar` answers `external_io` calls from it before anything
  else: no call, no key, no network. A miss fails the open naming the
  function.
- Policy at the call site: the session's `allow-files` setting rides in the
  per-statement `ExecBudget` (`PinkDb` and `Connection` each carry a
  per-session copy, set by `serve::execute`/`execute_internal`), and
  `eval_dynamic_scalar` refuses an `external_io` call with
  `PinkDbError::Permission(SERVICES_HINT)` before any request. A statement
  whose `SELECT` cannot bind at check time (a multi-statement batch), a
  macro, a view, a trigger action and a `CALL`/`DO` body are therefore all
  refused with the same error; the bind-time check in `external_reason`
  stays as the earlier, friendlier message.
- WAL size, measured: 1,000 rows at `PINKDB_EMBEDDINGS_DIMENSIONS=1024` add
  4 133 005 bytes to the store directory over the same 1,000 rows without
  the `UPDATE` (4 032 B of vector + ~101 B of memo framing per row, 4.0 KiB
  per distinct text), in 8 provider requests (1,000 inputs at 128 per
  request). That is the price of logical logging; a wide backfill should be
  checkpointed after it.
- Tests: 19 engine tests, 1 unit test (`Debug` redaction), 4 CLI subprocess
  tests, 1 `#[ignore]`d live test. The batching test measures 3 requests for
  1,000 rows holding 300 distinct texts (300 inputs sent, every request ≤
  128). The replay tests cover a single statement, a two-statement
  transaction, a provider that answers differently the second time, the CLI
  across two processes with a fresh cache, no key and a dead endpoint, and
  the 1024-wide size above.
- Deliberate deviation from the brief: `embed()` is not registered volatile.
  Volatility in this engine means "refused in a WAL-logged statement", which
  would reject `UPDATE docs SET v = embed(body)`; the memo supplies the
  replay guarantee the flag exists for.
- A store written by a build *with* the feature is refused by a build
  *without* it at the recorded `LOAD EXTENSION`, with the engine's existing
  fail-closed message ("a database file never loads native code on its
  own"); the message and the rule are covered by
  `crates/pinkdb/tests/extension_replay.rs`, not re-tested in a subprocess
  here.
- The `config → dimensions` coupling means a change to
  `PINKDB_EMBEDDINGS_DIMENSIONS` needs a restart; the extension reports the
  mismatch by name rather than re-binding mid-session.
