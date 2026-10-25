# Wave 106 plan — `pinkdb-agent`: a pinkdb-native data agent

Row 106 of the roadmap. The harness is a **pinkdb extension** (`extensions/agent`,
crate `pinkdb-agent`, a `cdylib` plus `rlib` like `extensions/mcp`), loaded with
`LOAD EXTENSION`. No LLM code, HTTP client or key handling exists in the engine,
`pinkdb` or `pinkdb-server`: those grow only by the host-side ABI addition in §1.

## 1. ABI (additive, minor bump)

`crates/pinkdb-ext-loader/src/abi.rs`:

- New optional entry point, detected by symbol presence like `pinkdb_ext_serve`:

  ```c
  typedef void (*PinkdbExtSinkFn)(void *ctx, int32_t channel, const char *text, size_t len);
  // channel 0 = answer/progress text (stdout), 1 = diagnostic (stderr).
  typedef int32_t (*PinkdbExtCommandFn)(const PinkdbHostApi *api, void *host_ctx,
                                        const char *args,
                                        PinkdbExtSinkFn sink, void *sink_ctx);
  ```

  `pinkdb_ext_command(api, host_ctx, args, sink, sink_ctx)`: the host calls it for
  one dot command (the text after `.agent`), with a live statement session and a
  sink for streamed text. Return 0 = handled, non-zero = failure (the extension
  has already written the message to the sink).

- New appended `PinkdbHostApi` field:

  ```c
  typedef int32_t (*PinkdbExtExecuteStatementFn)(void *ctx, const char *sql,
      size_t sql_len, char **out_text, size_t *out_len);
  …
  PinkdbExtExecuteStatementFn execute_statement;  // tool policy, Markdown results
  PinkdbExtExecuteStatementFn execute_internal;   // session policy, JSON results
  ```

  `execute_internal` exists only during a command call: it runs the harness's own
  bookkeeping under the internal policy (§4) and answers with JSON
  (`[{"columns":[…],"rows":[[…],…]}, …]`), because Markdown tables are not a
  machine-readable store format. `struct_size` covers the new field, so a host
  that does not publish it leaves the SDK's stub returning 2.

- `PINKDB_EXT_ABI_MINOR` becomes **4**, not 3: minor 3 is already taken on this
  branch by the VARIANT value tag (wave 101b). The bump is documentation — every
  new surface is detected by `struct_size`/symbol presence — and v1, minor-2 and
  minor-3 extensions and the four SDKs keep loading unchanged.
- Landmarks stay numerically valid for old hosts:
  `PINKDB_EXT_API_MINOR2_SIZE` = `offsetof(execute_internal)`, which equals
  `sizeof` on a minor-2/3 host; add `PINKDB_EXT_API_MINOR4_SIZE` = `sizeof` and
  `PINKDB_EXT_HOST_HAS_INTERNAL(api)`.
- `crates/pinkdb-ext-loader/src/command.rs`: `pub trait CommandHost`
  (`execute_statement`, `execute_internal`, `emit`) and
  `run_command(&LoadedExtension, args, &mut dyn CommandHost)`; the library is
  pinned in the existing "actively serving" set for the duration, like `serve`.
- Rust SDK: `CommandSession` (`execute_statement`, `execute_internal`, `emit`)
  and `pinkdb_abi_exports_with_command!(run)` in
  `ext-sdk/rust/src/{serve.rs,macros.rs}`; `host_api_copy` gains the third tier.
- C header (all five copies): new typedefs, field, landmark, macro,
  `PINKDB_EXT_ABI_MINOR 4`, a "what a command extension is" paragraph.
  The C, C++, Go and Python SDKs only need the header copy and, for a command
  extension, a `pinkdb_ext_command` shim; nothing to implement in them for this
  wave (they do not build a command extension).

## 2. Shared tool code

New rlib `extensions/warehouse-tools` (`pinkdb-warehouse-tools`), no deps beyond
`serde_json`, holding today's `extensions/mcp/src/tools.rs` verbatim in behaviour:
`tool_names`, `schemas`, `statements`, plus a small `ToolHost` trait and
`call(host, name, args)` that runs a tool's statements and joins the replies.
`pinkdb-mcp` keeps its public surface by re-exporting
(`pub use pinkdb_warehouse_tools as tools;` etc.), so its tests and behaviour do
not change. The agent extension uses the same `schemas()`/`statements()` and
dispatches through the host callback, so the two surfaces cannot drift.

The tool set is row 105's: `query`, `describe`, `tables`, `databases`, `explain`,
`lake_tables`, `lake_versions`, `lake_changes`. Worksheets (named in the roadmap
row) belong to the WebUI wave; no shell, file, web or extension-loading tool.

## 3. Invocation

CLI (`crates/pinkdb-cli`):

```
.agent ask "<question>" [allow-writes] [allow-files]
.agent resume <session>
.agent sessions
.agent show <session>
.agent fork <session> <step> ["<new question>"]
```

`.agent` keeps its wave-99 settings subcommands (`rows`, `cols`, `cell`,
`budget`, `timeout`, `hint`); the five verbs above dispatch to the loaded `agent`
extension. Without it loaded the CLI prints a hint naming the library file and
`--agent`, never a silent no-op.

`pinkdb --agent <store.pink> "<question>"` runs one question against a store and
exits, loading `agent` from the install directory exactly like `--mcp` loads
`mcp` (`--agent-allow-writes`, `--agent-allow-files`). `--agent` already means
"agent display mode" (wave 99) with no value; it keeps that meaning when the
next argument is absent or starts with `-`, and means the harness run when the
next argument names a file (`--agent=FILE` also works). This is the one place a
flag had to be overloaded; the alternative (`--agent-run`) contradicts the
brief.

Server and WebUI integration is the WebUI wave: the seam is
`pinkdb::serve::run_command` (host side) plus the extension's command entry
point, so the server only has to build a `CommandHost` over its connection.

## 4. Policy

`pinkdb::serve::ServePolicy` gains:

- `deny_write_databases: Vec<String>` — databases a client may never write,
  even with `allow_writes`. The model's tool policy sets it to
  `["pinkdb_agent"]`: a prompt injection must not rewrite history.
- `write_database: Option<String>` — the harness's internal policy: writes are
  allowed **only** inside that database (and non-writing statements), so the
  extension can keep its own tables and nothing else.

Both are enforced in `checked()` through a new `write_targets(stmt, current_db)`
that maps each mutating statement to the database(s) it writes (`None` for
store-wide effects such as `LOAD EXTENSION`, `CHECKPOINT`, RBAC, `CALL`), so the
two directions are testable: default tools refuse every write path; the model
still cannot touch `pinkdb_agent.*` with `allow-writes`; the extension can.

`execute_internal` uses the same checker with `write_database = Some("pinkdb_agent")`.

## 5. Session state

Database `pinkdb_agent` in the connected store, created on first run:

```sql
sessions(session_id, origin, origin_step, question, model, base_url,
         allow_writes, allow_files, max_steps, token_budget,
         state, stop_reason, prompt_tokens, completion_tokens, cost_micros,
         created_at, updated_at)
messages(session_id, step, seq, role, content, tool_calls, finish_reason, created_at)
tool_calls(session_id, step, call_index, call_id, name, arguments, result,
           replay_safe, state, created_at)
checkpoints(session_id, step, label, created_at)
usage(session_id, step, prompt_tokens, completion_tokens, cost_micros)
```

No primary keys or foreign keys: the tables are append-only bookkeeping written
by one process, and the resume path reads them in insert order. One "step" is one
model turn; after the assistant message and its tool results are recorded, a
`checkpoints` row commits the step in the same internal statement batch
(`BEGIN; …; COMMIT;`), so a step is all-or-nothing.

Time and ids are written by the extension (`unix micros`, `session_id` =
`a<8 hex>`), not by engine defaults, so the schema stays portable.

## 6. Resume, fork, budgets

- **Resume** loads `sessions`; if `state = 'done'` it prints the recorded final
  answer. Otherwise it replays `messages`/`tool_calls` in order. A tool result
  already `recorded` is reused, never re-run. A call left `running` (the process
  died during it) is re-run only if `replay_safe` (all eight tools are read-only
  until `allow-writes`); otherwise it becomes an error result the model sees.
  A step with no assistant message (the stream died) is re-requested.
- **Fork** copies the session row, and every `messages`/`tool_calls`/`usage`/
  `checkpoints` row with `step ≤ N`, into a new session that records
  `origin`/`origin_step`, then optionally appends the new question and continues.
  The source session is never updated.
- **Budgets**: `max_steps` (default 20), `token_budget` (default 0 = none, else
  prompt+completion tokens over the run), and the per-statement `ExecLimits`
  from wave 99 (row/timeout budget, `.agent budget|timeout`). Any stop is
  recorded as `stop_reason` on the session plus a `notice` message.
- **Lake stores**: a `.pink` store has no time travel, so resume/fork copy rows
  and branch by *copying*, not by a snapshot. On a lake-attached store the same
  code can later pin `checkpoints.step` to a lake version and fork at a version
  boundary instead of copying; the tables and the checkpoint row already carry
  everything that needs (the plan defers the lake wiring to the lake wave).

## 7. Provider

One OpenAI-compatible `/chat/completions` client, blocking `ureq` 3.4 with
`rustls` (the exact crate/features `pinkdb-httpfs` locks; **not** a dependency on
`pinkdb-httpfs`, which links the engine), `serde_json` for bodies, SSE parsed by
hand:

- `POST` with `stream: true`, `stream_options.include_usage`, `tools`;
- `delta.content` accumulated; `delta.tool_calls[i]` accumulated **by index**
  (`id`/`name` only on the first chunk, `function.arguments` split across any
  number of chunks); `finish_reason`; final `usage`.
- `timeout_global(120s)`; bounded retry (3 attempts, 250 ms then 500 ms) on 429
  and 5xx; a mid-event EOF is a retryable transport error, handled by the run
  loop as "re-request this step".
- 401 → `the provider rejected the API key (HTTP 401); check PINKDB_AGENT_API_KEY`
  with no key, no headers and no request/response body in the message.
  Other statuses report `HTTP <n>` and a truncated body.

Defaults, each one constant: base URL
`https://api.z.ai/api/paas/v4/chat/completions`, model `glm-4.6`. Overridden by
`PINKDB_AGENT_BASE_URL`/`PINKDB_AGENT_MODEL`; the key comes from
`PINKDB_AGENT_API_KEY`, falling back to `Z_API_KEY`, read when a run starts. No
`.env` is ever read. `Debug` for the client and config redacts the key, and the
key is never written to a table, the WAL, a log line or an error.

## 8. Tests

`extensions/agent/tests/` — CI-safe, nothing but 127.0.0.1:

- `mock.rs`: an in-process SSE provider (`TcpListener`, port 0, real port read
  back) replaying fixtures in `tests/fixtures/`: single answer; multi-step tool
  loop; tool-call arguments split across chunks; interrupted stream (socket
  closes mid-event) then success; 429 then success; 401.
- `session.rs`: resume after kill (a child `pinkdb` killed after a checkpoint,
  then `.agent resume`, same final transcript, no duplicated tool calls); fork
  leaves the source untouched and diverges; budgets stop a run and record why.
- `policy.rs`: every write path refused by default (DML, DDL, `COPY … TO`,
  `LOAD EXTENSION`, `ATTACH` write, lake commit) and allowed with
  `allow-writes`, except writes to `pinkdb_agent.*`, refused for the model and
  allowed for the extension.
- `secrets.rs`: a full mock session with a sentinel key, then every table, the
  `.pink` bytes and the captured stderr are grepped for the sentinel.
- `e2e.rs`: the mock answers "top 3 customers by revenue" with real tool calls
  against a generated TPC-H-style store.
- `abi.rs` (cli or loader tests): a minor-2 extension (`mcp`) and a v1 extension
  still load; `run_command` runs and returns.
- `live.rs`: ignored by default, runs only with `PINKDB_AGENT_LIVE=1` and a key.

## 9. Gates, docs, sizes

Gates as listed in the brief, heavy feature matrix last. Docs: ROADMAP row 106
done with measured sizes, CHANGELOG, README "The pinkdb agent", a website page
linked from the agents page (then `python3 website/build.py`), CI entries for the
new crates. Sizes: baseline `pinkdb`/`pinkdb-server` built now in a throwaway
target dir, compared with the final release build, plus the extension's size and
its zero new `Cargo.lock` crates.
