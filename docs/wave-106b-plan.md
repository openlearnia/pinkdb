# Wave 106b plan — several model providers for `pinkdb-agent`

Row 106b of the roadmap, a follow-up to wave 106. Wave 106 shipped one
OpenAI-compatible client with z.ai as its default. This wave makes the provider
an explicit, per-run choice out of a built-in table, adds the Anthropic Messages
API as a second wire protocol, and moves the model client into a crate wave 109
can share. **There is no default provider** (user, 2026-10-26): every run names
one, and z.ai is only the provider the live tests use.

Nothing in `crates/pinkdb` changes. The ABI does not change (minor stays 5).

## 1. A shared crate: `extensions/llm` (`pinkdb-llm`)

The model client moves out of the agent extension into a small rlib so wave 109
(agent memory) reuses it instead of forking it:

| moved from | to | what changes |
|---|---|---|
| `extensions/agent/src/sse.rs` | `extensions/llm/src/sse.rs` | nothing |
| `extensions/agent/src/chat.rs` (transcript half) | `extensions/llm/src/chat.rs` | `request_body`/`Accumulator` leave |
| `extensions/agent/src/chat.rs` (wire half) | `extensions/llm/src/openai.rs` | `reasoning_content` is explicitly dropped |
| `extensions/agent/src/provider.rs` | `extensions/llm/src/client.rs` | two protocols, per-provider keys |
| — | `extensions/llm/src/anthropic.rs` | new: the Messages API encoder and its SSE reader |
| — | `extensions/llm/src/registry.rs` | new: the provider table and selection |
| `extensions/agent/tests/provider.rs` + `tests/fixtures/` | `extensions/llm/tests/` | the client's tests follow the client |

Dependencies: `serde_json` and `ureq` 3.4 with `default-features = false,
features = ["rustls"]` — the exact crate and features wave 106 already locked —
plus `pinkdb-warehouse-tools` as a *dev*-dependency, so the test that proves
every shared tool is encoded as a function tool moves with the encoder.
**Zero new crates in `Cargo.lock`**: every one is already a workspace path
dependency or already locked.

`pinkdb-agent` keeps its public surface by re-exporting
(`pub use pinkdb_llm::{Message, Provider, …}`), the way `pinkdb-mcp` re-exports
`pinkdb-warehouse-tools`, so `extensions/agent`'s behaviour and every existing
agent test stay as they are.

The crate is an **rlib only** (no cdylib): it is not loadable on its own, it is
linked into an extension that is.

## 2. Two wire protocols behind one `Provider` trait

`Protocol` is a two-variant enum chosen by the registry; `Client` holds one and
dispatches request encoding and stream decoding on it. The transcript
(`chat::Message`) is untouched: it was already provider-neutral, which is what
makes a fork across providers a re-encode rather than a translation.

### `openai` — `/chat/completions`

What wave 106 shipped, with one addition: `delta.reasoning_content` (z.ai
GLM-5.x, DeepSeek reasoners) is **dropped on the floor**. It is never
concatenated into `content`, so it is never recorded in `messages` and never
sent back on the next turn. A unit test pins that.

### `anthropic` — `POST /v1/messages`

- Headers `x-api-key` and `anthropic-version: 2023-06-01`; `stream: true`.
- The **system prompt is hoisted** into the top-level `system` field. Several
  `Message::System` entries join with a blank line; none means the field is
  omitted.
- Tools are `{name, description, input_schema}` — the MCP `inputSchema` under
  its Anthropic name, no `type: "function"` wrapper.
- Content is always a **block array**, which is what makes merging work:
  - `Message::Assistant` → `{"type":"text",…}` when the text is non-empty, then
    one `{"type":"tool_use","id","name","input"}` per call. `input` is the
    model's `arguments` string parsed back to JSON; a fragment that does not
    parse becomes `{}` rather than a request the API rejects.
  - `Message::Tool` → a user message holding
    `{"type":"tool_result","tool_use_id","content"}`, paired to its `tool_use`
    **by id**.
  - **Consecutive same-role messages merge** into one message, which satisfies
    both "consecutive tool results are one user message" and "roles must
    alternate" with one rule instead of two.
- `max_tokens` is required by this API: `DEFAULT_MAX_TOKENS = 8192`, overridden
  by `PINKDB_AGENT_MAX_TOKENS`.
- Extended thinking is **not** enabled (no `thinking` field). A `thinking_delta`
  that arrives anyway is ignored like `reasoning_content`.

The SSE events are parsed by hand on top of the same `sse.rs` reader. That
reader only reads `data:` fields, and every Anthropic payload carries its own
`"type"`, so the event name line needs no special handling:

| event | what is taken |
|---|---|
| `message_start` | `message.usage.input_tokens` (and `output_tokens` if present) |
| `content_block_start` | a `tool_use` block's `id` and `name`, by `index` |
| `content_block_delta` | `text_delta.text`; `input_json_delta.partial_json`, appended per index |
| `content_block_stop`, `ping` | ignored |
| `message_delta` | `delta.stop_reason`, `usage.output_tokens` |
| `message_stop` | the stream is complete (the `[DONE]` of this protocol) |
| `error` | the request failed, with the API's message |

`stop_reason` is mapped onto the names the store and `.agent show` already use,
so a transcript reads the same whichever provider wrote it: `end_turn` and
`stop_sequence` → `stop`, `tool_use` → `tool_calls`, `max_tokens` → `length`.

A stream that ends without `message_stop` is "interrupted", exactly like one
that ends without `[DONE]`: the run loop re-requests the step.

### Shared transport rules

One `Client`, one retry policy, one set of timeouts for both protocols:
3 attempts with 250 ms then 500 ms backoff on 429, 5xx and Anthropic's **529**
(overloaded). 401 and 403 stop immediately with a message that names the
provider and its key variable and never echoes the key or any part of it.

## 3. The provider registry

One `const` table in `registry.rs`:

| name | protocol | base URL | root override | key variable(s) | default model |
|---|---|---|---|---|---|
| `zai` | openai | `https://api.z.ai/api/paas/v4` | `PINKDB_AGENT_ZAI_BASE_URL` | `ZAI_API_KEY`, then `Z_API_KEY` | `glm-5.3` |
| `anthropic` | anthropic | `https://api.anthropic.com/v1` | `PINKDB_AGENT_ANTHROPIC_BASE_URL` | `ANTHROPIC_API_KEY` | `claude-sonnet-5-5` |
| `openai` | openai | `https://api.openai.com/v1` | `PINKDB_AGENT_OPENAI_BASE_URL` | `OPENAI_API_KEY` | none |
| `deepseek` | openai | `https://api.deepseek.com/v1` | `PINKDB_AGENT_DEEPSEEK_BASE_URL` | `DEEPSEEK_API_KEY` | none |
| `openrouter` | openai | `https://openrouter.ai/api/v1` | `PINKDB_AGENT_OPENROUTER_BASE_URL` | `OPENROUTER_API_KEY` | none |
| `gemini` | openai | `https://generativelanguage.googleapis.com/v1beta/openai` | `PINKDB_AGENT_GEMINI_BASE_URL` | `GEMINI_API_KEY` | none |
| `ollama` | openai | `http://127.0.0.1:11434/v1` | `PINKDB_AGENT_OLLAMA_BASE_URL` | none (no auth header) | none |
| `custom` | `PINKDB_AGENT_PROTOCOL` (default `openai`) | none | `PINKDB_AGENT_BASE_URL` (required) | `PINKDB_AGENT_API_KEY` (optional) | none |

- The base URL is a **root**; the protocol appends `/chat/completions` or
  `/messages` (one trailing `/` on the root is tolerated). This is a change from
  wave 106, where the constant was the full endpoint.
- Each provider's root is overridden only by its own variable,
  `PINKDB_AGENT_<NAME>_BASE_URL` (the `base_url_variable` column of the
  `Entry` row, so the table stays the single source), for a proxy, a gateway
  or a test mock. `PINKDB_AGENT_BASE_URL` belongs to `custom` alone, which
  requires it, and every built-in provider ignores it. (Reviewer fix: the
  first cut let `PINKDB_AGENT_BASE_URL` move *any* provider while the key
  stayed the provider's own, so a root exported once for `custom` would later
  receive `ANTHROPIC_API_KEY` on a `provider anthropic` run.)
- "none" means a model must be named, and the error says how: `provider openai
  needs a model: pass \`model <name>\` or set PINKDB_AGENT_MODEL`. No model name
  is invented for a provider whose catalogue this wave cannot verify.
- **Key isolation is the security rule of this wave.** A key variable is only
  ever read for its own provider and only ever sent to that provider's base
  URL. `ANTHROPIC_API_KEY` cannot reach z.ai, a z.ai key cannot reach
  OpenRouter, and `PINKDB_AGENT_API_KEY` belongs to `custom` alone. This
  replaces wave 106's "`PINKDB_AGENT_API_KEY` then `Z_API_KEY`" rule outright —
  pre-1.0, no compatibility shim — so every document that states the old rule is
  updated in this wave.
- `ollama` has an empty variable list, so it sends no authorization header at
  all. `custom` has one variable but does not require it, for a local gateway
  that wants no key.

Selection is two stages, because a `fork` must be able to *record* a provider
without needing its key:

- `Plan::resolve(provider, model)` validates the name, the protocol, the base
  URL and the model. No key is read.
- `Plan::into_config()` reads exactly that provider's key variables and builds
  the `Config` the `Client` holds.

`Debug` on both redacts the key, as in wave 106.

## 4. Selection and precedence

```
.agent ask "<q>" [provider <name>] [model <name>] [allow-writes] [allow-files] [max-steps N] [tokens N]
.agent resume <session> [provider <name>] [model <name>]
.agent fork <session> <step> [provider <name>] [model <name>] ["<new question>"]
pinkdb --agent FILE "q" --agent-provider <name> --agent-model <name>
```

Precedence: **run argument > environment** (`PINKDB_AGENT_PROVIDER`,
`PINKDB_AGENT_MODEL`). For `resume` and `fork` the session's recorded provider
and model sit between them — run argument > the session's record > environment —
because continuing a transcript on a different model by accident is worse than
being explicit (brief: "`.agent resume` uses the session's recorded provider and
model unless the resume names others").

No provider anywhere is an **error**, never a fallback:

```
no provider: pass provider <name> or set PINKDB_AGENT_PROVIDER
(one of: zai, anthropic, openai, deepseek, openrouter, gemini, ollama, custom)
```

A provider is never inferred from whichever key happens to be exported: that is
how a key reaches the wrong endpoint. An unknown name lists the valid ones.
Wave 106's `DEFAULT_BASE_URL`/`DEFAULT_MODEL` constants are deleted; the
per-provider default model (z.ai's `glm-5.3`) stays, in the table.

The CLI only ever appends the words to the `.agent ask` line it already builds,
so there is one grammar and the extension remains the only parser.

## 5. Sessions record the provider

- `pinkdb_agent.sessions` gains `provider`; `pinkdb_agent.usage` gains
  `provider` and `model`, so a per-step cost comparison across providers is a
  `GROUP BY`. The schema changes outright — pre-1.0, no migration, and an old
  store's `sessions` simply has no such column.
- `.agent show` prints the provider next to the model.
- `.agent fork` may switch provider. Forking a z.ai session onto Anthropic
  works because the stored transcript is the provider-neutral
  `chat::Message`: the fork re-encodes it for the new wire protocol. That is
  the property this wave is really testing.
- A session's recorded model belongs to its recorded provider: when resume or
  fork switches provider without naming a model, the recorded model is
  dropped and the new provider's default (or `PINKDB_AGENT_MODEL`) applies.
  Sending `glm-5.3` to Anthropic would only buy a 400.

## 6. `.agent providers`

One line per provider: name, protocol, base URL, default model, and whether its
key variable is set — `key: set` / `key: missing` / `no key needed`. The key
itself, and any prefix or length of it, is never printed. It is a new verb in
the CLI's `AGENT_VERBS` list, so `.agent providers` dispatches to the extension
rather than being read as a wave-99 display setting.

## 7. Tests (CI-safe: 127.0.0.1 only, no provider is ever called)

`extensions/llm/tests/provider.rs` — the moved mock-server suite, extended:
the mock speaks both protocols, and new Anthropic fixtures cover a single
answer, a tool loop, `input_json_delta` split across chunks, an interrupted
stream re-requested, 529-then-success, 429-then-success and 401.

Unit tests in the crate:

- the Anthropic **request shape**: system hoisting, `tool_use`/`tool_result`
  pairing by id, merged consecutive tool results, alternating roles,
  `input_schema`, `max_tokens`;
- the registry: every protocol's URL join, no-provider, unknown-provider,
  no-model, key isolation (each entry reads only its own variables);
- `reasoning_content` and `thinking_delta` never reach the turn.

`crates/pinkdb-cli/tests/agent_run.rs` — the host contract:

- **key isolation**: sentinels for `zai`, `anthropic`, `openrouter` and `custom`
  exported at once; a run against each provider's mock records every request's
  headers, and each request must carry exactly its own provider's key in the
  right header (`Authorization: Bearer` or `x-api-key`) with no other sentinel
  anywhere in the headers or the body. `ollama` sends no auth header. The
  wave-106 sentinel test (tables, `.pink` bytes, WAL, stderr) re-runs with every
  sentinel exported. Every provider reaches its mock through its own root
  variable, and one test points `PINKDB_AGENT_BASE_URL` at a second mock and
  `PINKDB_AGENT_ANTHROPIC_BASE_URL` at the first: a `provider anthropic` run
  hits only the first, and the second sees no request and no sentinel.
- **precedence**: a run argument beats the environment; no provider errors even
  when a key is set; an unknown name lists the valid ones; a provider with no
  default model and no model given errors out.
- **resume** keeps the session's provider; a **fork from zai to anthropic**
  (two mocks) continues correctly and leaves the original session untouched.
- **`.agent providers`** output, including that it shows no key bytes.
- Live tests, both `#[ignore]` and env-gated, written and not run: the existing
  z.ai one (now `PINKDB_AGENT_PROVIDER=zai`) and an Anthropic one
  (`PINKDB_AGENT_LIVE=1` plus `ANTHROPIC_API_KEY`).

## 8. Gates, docs, size

Gates as the brief lists them, `--no-fail-fast` on the workspace run, plus the
wasm32 build. Then ROADMAP row 106b, a CHANGELOG entry, the README "The pinkdb
agent" section (the table, selection, key isolation, `.agent providers`),
`website/src/pages/agent.html` and a website regeneration, and the new crate in
`.github/workflows/ci.yml`.

Size: `libpinkdb_agent.dylib`, release, measured in a scratch target dir that is
deleted afterwards. The second protocol is a JSON encoder and an SSE dispatcher
over the same transport, so it should cost tens of KiB.

## Decisions where the brief left a choice

- **Fixtures live in the new crate**, not `extensions/agent/tests/fixtures/`:
  the client they exercise moved, and a fixture directory two crates away from
  its reader rots.
- **`pinkdb-llm` is rlib-only.** A cdylib would be a loadable extension that
  registers nothing.
- **`stop_reason` is normalised to the OpenAI names** rather than stored raw, so
  `finish_reason` in `messages` and `.agent show` means one thing.
- **Consecutive same-role merging is one rule**, applied to every role, instead
  of a tool-result-specific merge plus a separate alternation fixup.
- **For `resume`/`fork` the session's record beats the environment** but loses to
  a run argument (see §4).
- **`PINKDB_AGENT_MAX_TOKENS`** is the override for Anthropic's required
  `max_tokens`; it is ignored by the `openai` protocol, which does not need it.
