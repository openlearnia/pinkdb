# Wave 109 — agent memory (long-term memory for agents)

ROADMAP row 109. After 108 (BM25, fusion), before 106's follow-ups. The design
is a reimplementation of TencentDB-Agent-Memory's four layers (L0 conversation,
L1 atoms, L2 scenarios, L3 persona) on pinkdb tables, with the model calls made
through `pinkdb-llm` (wave 106b) and enforced identity (wave 48's RBAC) as the
one thing the reference does not have to answer for.

**This wave ships.** A `.memory` command surface, six MCP tools, an engine
builtin pair (`current_user()`, `current_roles()`), storage in one dedicated
database, layered budgeted retrieval, queued distillation with a strict JSON
contract, and CI-safe tests (mock providers only).

**Not in this wave** (recorded in ROADMAP row 109's follow-ups): Skill assets,
LLM-Wiki with a link graph, Code-Graph, a server background distillation
worker, and row-level security in the engine.

## 1. Packaging

| crate | kind | notes |
|---|---|---|
| `pinkdb-memory` (`extensions/memory`) | `cdylib` + `rlib` | the extension: storage, identity, search, distillation, `.memory`, its MCP `serve` loop |
| `pinkdb-mcp-protocol` (`extensions/mcp-protocol`) | `rlib` | the JSON-RPC/MCP dispatcher and the two transports, moved out of `pinkdb-mcp` so both bridges share one implementation |

`pinkdb-memory` depends on `pinkdb-ext-sdk`, `pinkdb-llm`,
`pinkdb-warehouse-tools`, `pinkdb-mcp-protocol`, `serde_json` — all workspace
crates, no new third-party dependency. The host seam is the wave-105/106 one
(`pinkdb_ext_command`, `pinkdb_ext_serve`, `statement.callback`), with one
addition described in §3.

`pinkdb-mcp` keeps its behaviour and loses three files to the shared crate; its
`Host` trait grows an `execute_internal` default method, which the warehouse
tools never call.

## 2. Storage

One database, `pinkdb_memory`, created by the extension on its first write
(`CREATE DATABASE IF NOT EXISTS`). Every write goes through the host's
**internal** channel, whose policy the host confines with
`ServePolicy::write_database = Some("pinkdb_memory")` — the same seam wave 106
uses for `pinkdb_agent`. The model's tool channel denies writes there, so a
prompt injection cannot rewrite memory through `query`.

```sql
l0_turns(id, {identity}, conversation, speaker, content, tag,
         created_at BIGINT, distilled BOOLEAN)
atoms(id, {identity}, text, confidence DOUBLE, status, superseded_by,
      source_l0, tag, created_at BIGINT, updated_at BIGINT)
scenarios(id, {identity}, title, summary, atom_ids, tag,
          created_at BIGINT, updated_at BIGINT)
personas(id, {identity}, text, tag, created_at BIGINT)
distill_queue(id, {identity}, kind, subject, state, attempts INTEGER,
              last_error, created_at BIGINT, updated_at BIGINT)
audit(id, at BIGINT, actor, action, subject, detail)
runs(id, owner, kind, provider, model, state, error, created_at BIGINT)
atom_vectors(atom VARCHAR, v FLOAT[N])          -- created lazily, §5

-- {identity} = owner, team, visibility, acl_users, acl_roles, acl_agents, agent
```

Every memory row carries `owner`, `team`, `visibility`, the three ACL columns
and `agent`; `status` is `active` | `superseded` | `forgotten`. There is no hard
delete anywhere: `forget` sets `status = 'forgotten'` and appends an `audit`
row. `superseded_by` keeps history, and a superseded atom still answers "what
did I believe last week".

Written rows are append-mostly, so the extension also treats `atoms.status`
as the one mutable field; `text` never changes in place (`memory_update`
creates a new atom that supersedes the old one, so history stays linear).

## 3. Identity: `current_user()` and `current_roles()`

The extension ABI does not say who the caller is, and the engine has users and
roles (wave 48) but no way to ask. The narrowest seam that fixes both is a
**SQL builtin pair**, not an ABI bump:

```sql
SELECT current_user();   -- 'alice', or 'pinkdb' on an embedded trusted store
SELECT current_roles();  -- 'analyst,ops' (comma-joined display names), '' when none
```

Both are zero-argument and bound to `BoundExpr::Literal` at bind time, from the
session's principal. That is the whole reason they are not `ScalarFunc`
variants: `eval_scalar` derives its chunk length from the first argument
(`crates/pinkdb/src/exec/expr.rs:1682`), so a zero-argument scalar would produce
an empty chunk; a literal has no such rule, and it is already constant for the
optimizer.

- The carrier is `storage::Session` (`Arc<Mutex<…>>`, already cloned into every
  binder and executor), which gains one `Option<String>` field. `Connection`'s
  own session is the one a network statement binds with, so a server caller
  sees its login, and an embedded store (no login, no users) sees the implicit
  principal `'pinkdb'`.
- The dependency token is `$current_user` / `$current_roles`. That reuses the
  existing WAL guard: a write statement that reads session state is refused
  ("session variable … cannot be read by a statement the engine WAL-logs"), so
  a logged `INSERT … VALUES (current_user())` cannot replay against a different
  user. Reads are unaffected.
- No ABI change, so the four SDKs, every loaded extension and the wasm/FFI
  surfaces are untouched. The extension asks for its caller by running
  `SELECT current_user(), current_roles()` on the **internal** channel — the
  response is the host's, not the client's, so an MCP tool argument cannot
  spoof it.

`current_roles()` joins display names with commas because pinkdb has no
variable-length `LIST` yet (107a2); a comma cannot appear in a role name.

## 4. Visibility

Four classes, enforced by the extension on **every** read (search, persona,
show, distill input):

| visibility | readable by |
|---|---|
| `private` | the owner |
| `team` | the owner, and members of the row's `team` role |
| `restricted` | the owner, and the users/roles/agents named in the ACL |
| `agent` | the named agent identity only (`agent` = the caller) |

The predicate is computed in the extension from `current_user()` /
`current_roles()`, so it also holds for rows the caller did not write. Roles are
the engine's; the extension never writes a role, it only compares names.

**No bypass by `SELECT`.** Memory tables are never granted to a non-admin user,
and a read of a table the caller has no grant on is refused by the engine
(`permission denied: user "alice" needs the SELECT privilege on table
"pinkdb_memory.atoms"`). Admins can always read (the engine's admin bypass);
that is documented, not a hole in the design. On an **embedded** store there
are no users, so there is no identity to protect — the trusted session is the
operator, exactly as with `pinkdb_agent`.

## 5. Retrieval

`memory_search(query, …)` is layered:

1. the caller's L3 persona, one row (latest `personas` row for the caller);
2. the best-matching L2 scenarios (BM25 over `title` + `summary`);
3. L1 atoms and L0 turns, ranked by **BM25 + RRF** (and, in `hybrid` mode, also
   by vector distance, fused the same way).

**BM25 in the extension, not through an fts index.** 108a's index is *static*:
every `INSERT` into the atoms table would make the index stale, and the index
lives in its own database (`fts_main_…`), which the extension's confined
channel cannot create (a `CALL` writes globally, so the policy refuses it under
`write_database`). So the extension scores BM25 itself over the atoms of the
caller's visible set, with 108a's exact formula (`k1 = 1.2`, `b = 0.75`,
`idf = ln(1 + (N - df + 0.5)/(df + 0.5))`; a document matching no term scores
NULL, i.e. it is dropped) and fuses lists with `rrf(rank) = 1/(60 + rank)`,
108a's constant. A ported query and `memory_search` therefore rank alike.

**Vectors.** `hybrid` mode requires the `vss` and `embeddings` packages to be
loaded *and* an embeddings provider to be configured. The extension then

1. probes `typeof(embed('probe'))` to learn `FLOAT[N]` (the provider's width is
   a startup setting, so it cannot be baked into this crate's DDL);
2. creates `pinkdb_memory.atom_vectors(atom VARCHAR, v FLOAT[N])` on first use;
3. embeds atoms that have no vector (`INSERT … SELECT id, embed(text)`), which
   needs `allow-external-services` on the internal channel — the operator opted
   into an embedding provider, and the mock/live switch is the same one 107c
   uses, now its own switch rather than the files one (§11);
4. ranks with `array_distance` and fuses with the BM25 list.

Any step failing (no package, no provider, a dimension change) falls back to
BM25-only and the result says so (`"mode": "bm25"`, with a `note`), rather than
answering half a hybrid.

**Budgets, per call:** `max_items`, `max_chars`, `timeout_ms`. All three stop
retrieval where they hit; the result reports which one did
(`stopped_by: "items" | "chars" | "timeout" | "exhausted"`). Persona and
scenarios count against the same budget as atoms and turns, because a bootstrap
that ignores the budget is not budgeted.

Every hit is `{layer, id, score, source, text}`: `layer` is `l3| l2 | l1 | l0`,
`source` names where it came from (`persona`, `scenario <id>`, `atom <id>`,
`turn <id>`), and `score` is the fused score (0 for persona).

## 6. Operations

**CLI** (needs `LOAD EXTENSION` of the memory library, exactly like `.agent`):

```
.memory add <text> [--visibility private|team|restricted|agent] [--tag <name>]
                   [--acl u:bob,r:ops] [--speaker user|agent] [--conversation <name>]
                   [--agent <identity>]
.memory search <query> [--items N] [--chars N] [--timeout-ms MS]
.memory update <atom> <text>          -- a new atom that supersedes the old one
.memory forget <id>                   -- soft delete plus an audit row
.memory distill [--provider <name>] [--model <name>]
.memory persona
.memory show                          -- counts per layer, the persona, recent rows
.memory serve [stdio | http <port>] [allow-memory] [allow-writes] [allow-files]
```

**MCP**, through the memory extension's own `serve` loop (an extension cannot
call another extension, and the memory reads must use the internal channel; the
loop is the shared `pinkdb-mcp-protocol` crate):

`memory_add`, `memory_search`, `memory_update`, `memory_forget`,
`memory_distill`, `memory_persona`, alongside the unchanged warehouse tools, so
a client pointed at `<libpinkdb_memory> serve stdio allow-memory` gets one
complete server. Under the read-only policy only `memory_search` and
`memory_persona` are listed and the write tools are refused; they need
`allow-memory` (`Memory{allow_memory}`), and the host's policies still confine
every statement they run to `pinkdb_memory`.

**pinkdb-agent**: an agent run's question and answer are recorded as two L0
turns under the caller's identity when the memory extension is loaded. The two
extensions cannot call each other (the ABI carries statements, not commands),
so the seam is the host that owns both: `crates/pinkdb-cli`'s `.agent` sink
keeps the printed answer and, after a successful run, calls the memory
extension's `add` twice on its own confined channel. A memory failure cannot
fail a finished run. The agent does not get a `memory_search` tool this wave
(recorded as a follow-up: it changes the agent's tool set and its prompt).

## 7. Distillation

Asynchronous in the only sense that matters: `.memory add` never calls a model.
`distill` drains a queue, one model call per step, each recorded in `runs`. A
job is `kind = 'turn' | 'scenario' | 'persona'`; the turn step enqueues the
other two when it has written atoms. `PINKDB_MEMORY_DISTILL_EVERY=N` makes
`add` enqueue a turn job every N turns (off by default, no model call at add
time).

1. **L0 → L1.** The prompt carries the not-yet-distilled turns of that owner
   plus the owner's active atoms, and asks for
   `{"atoms": [{"text": "…", "confidence": 0.0-1.0, "tag": "…", "supersedes": "<atom id>"}]}`.
2. **L1 → L2.** One call over the atoms no scenario names yet, asking for
   `{"scenarios": [{"title": "…", "summary": "…", "atoms": ["<atom id>", …], "tag": "…"}]}`;
   a scenario that names no known atom is dropped rather than stored empty.
3. **L2 → L3.** One call over the owner's scenarios, asking for
   `{"persona": "…"}`; a new `personas` row is appended (latest wins, so the
   previous profile is still there).

Each answer is parsed and **validated before anything is written**: required
fields, types, length caps, confidence in `[0, 1]`, and every referenced atom
id must exist and be visible to the owner. A malformed or partial answer writes
`runs.state = 'failed'` with the error, leaves the queue row `pending` and
appends nothing — never a half-applied step. The queue row's `attempts` grows;
after three failures it is `failed` and `distill` reports it.

Dedupe and supersession: an atom whose text is (case-insensitively) equal to an
active atom of the same owner is skipped; an atom listing `supersedes` ids marks
those `superseded` with `superseded_by` set, in the same statement batch as the
insert. A contradicting fact therefore supersedes rather than duplicates, and
the old row stays readable by `show`.

**No provider** is not a fallback: `distill` and `persona` return
`pinkdb-llm`'s error ("no provider named: pass provider <name> or export
PINKDB_AGENT_PROVIDER"), while `add` and `search` keep working. Provider
settings, key isolation and retries (429 included) are `pinkdb-llm`'s.

A `failed` job is not retried automatically (that is what `attempts` bounds);
an operator's `.memory distill` resets failed jobs to `pending` first, which is
the same seam a future background worker will use. The worker itself is a
follow-up (WebUI wave).

## 8. Tests (CI-safe, localhost mocks only)

Integration (`extensions/memory/tests/`), over the same `SessionHost` the CLI
and server build (a real `Connection`, the internal channel, the real
policies):

- **end to end** on a recorded mock provider: L0 → `distill` → atoms → scenarios
  → persona, then `memory_search` returns the persona and the right atoms;
- **ACL isolation**: users A and B, teams T1 and T2, one row per class;
- **supersession and forget**: contradicting fact supersedes; forget hides from
  search and leaves an audit row;
- **hybrid** with 107c's mock embeddings server and the `vss` package;
- **robustness**: malformed JSON, a 429 then success, no provider → clear
  error while add/search work;
- **key sentinel** never in the store, its WAL, or stderr (106's pattern).

`crates/pinkdb/tests/current_user.rs` for the builtins (literal binding, WAL
refusal, RBAC-visible roles, embedded default).
`crates/pinkdb/tests/internal_channel.rs` for the confined channel (its own
database reads and writes; a user table refused directly, through a view, a
CTE, a macro and a scalar subquery; `read_csv`/`read_parquet`/a bare path
refused; the services switch on its own; the agent's channel the same way).
`crates/pinkdb-cli/tests/memory_run.rs` for the CLI surface (add → search →
show round trip in one process and across two, the provider error, the missing
extension); `crates/pinkdb-cli/tests/agent_run.rs` for the agent seam (a run
records two L0 turns, a later run searches them; and
`loading_the_agent_before_memory_still_runs_every_statement` pins two loads and
two statements in one non-interactive script, agent's library first).
`extensions/mcp/tests/protocol.rs` keeps its coverage against the moved crate.

One `#[ignore]`d live test: `PINKDB_MEMORY_LIVE=1` plus a provider. Written,
not run.

## 9. Gates

```
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test -q -p pinkdb-memory
cargo test -q -p pinkdb-agent
cargo test -q -p pinkdb-mcp
cargo test -q -p pinkdb-cli --test agent_run
cargo test -q -p pinkdb-cli --test memory_run
cargo test -q -p pinkdb-ext-loader
bash ext-sdk/posture.sh
the four ext-sdk smoke.sh scripts
cargo test -q --workspace --no-fail-fast
RUSTC=$(rustup which --toolchain stable rustc) cargo +stable build -p pinkdb \
      --target wasm32-unknown-unknown
```

## 10. Deviations from the brief, and why

| brief | what lands | why |
|---|---|---|
| SQL table functions / `CALL` procedures for add/search/… | `.memory` command + the memory extension's MCP tools; no SQL function | the ABI registers scalar functions only (no table function, and a scalar never sees the store), and a SQL procedure body cannot call a model or check row visibility. The tables are still ordinary SQL for an admin (`SELECT … FROM pinkdb_memory.atoms`). Recorded as a follow-up: row-level security in the engine. |
| BM25 through the fts extension | BM25 with 108a's formula, inside the extension | the fts index is static and cannot be created from a confined channel; see §5. |
| `memory_search` uses `array_distance`/HNSW | `array_distance` (exact scan) in hybrid mode; no HNSW index | an HNSW index is built by a `CALL`, refused by the same confinement; atom counts are small. |
| server background distillation worker | CLI/`distill` drain only | the worker needs the server's statement loop (WebUI wave); the queue is the seam it will use. |
| agent records L0 and gets `memory_search` | the CLI host records the question and answer as L0 turns; no `memory_search` tool yet | the two extensions cannot call each other, and adding a tool changes the agent's tool set and prompt; recorded as a follow-up. |
| an extension is told who the caller is through the ABI | SQL builtins only | no ABI bump, so the four SDKs and every loaded extension are untouched. |
| `.memory` reads memory through the public channel | the extension asks on the internal channel; the public channel cannot read `pinkdb_memory` at all | otherwise a client could `SELECT` the tables and skip the row filter. |

## 11. As built

What the plan did not anticipate, and the numbers.

**The served session needed a trusted channel.** A memory session that the CLI
or the server *serves* (`.memory serve`) is a `Connection` under a login, and
before this wave a served extension could only run the client's statements:
`HostSession` had `execute_statement` and nothing else. Memory's bookkeeping
(and `current_user()` behind it) must run confined but unprivileged, so the
seam grew by one policy field and no ABI version:

- `ServePolicy::internal: Option<Box<ServePolicy>>` — the policy an extension's
  own channel runs under (`None` = no such channel, which a command-only host
  keeps);
- `Session::run_internal` (the `Connection` impl runs the statement with the
  RBAC gate skipped and the session's principal kept, so `current_user()` still
  answers the caller);
- `StatementHost::execute_internal` wired to the `execute_internal` slot ABI
  minor 4 already carries (wave 106), so the loader, the SDK and the wire
  format are unchanged and an older extension simply sees code 2 "no channel".

**The tables are masked, not just ungranted.** `ServePolicy::deny_read_databases`
makes a served statement that reads `pinkdb_memory` fail with a hint that names
the tools instead of the tables. It is enforced by binding the statement and
collecting the tables it reads, so a CTE, a view or a macro that reaches the
database is caught too. Without it, an admin's MCP client could read every
principal's rows through `query`.

**The internal channel is confined on reads too.** `Session::run_internal` skips
the RBAC gate (that is what makes it usable for bookkeeping), so the policy is
the only thing standing between one quoting slip in an extension's query and a
read of every table or every file. `ServePolicy::read_databases` is the
allow-list: when it is set, a statement on that channel may read only tables in
those databases, checked from the bound plan (views, CTEs and macros included)
and refusing table functions and bare paths before they could be bound. The
memory channel is `read_databases: [pinkdb_memory]`; the agent's is
`[pinkdb_agent]`.

That work exposed a latent hole: `Connection::hint_store()` answers `None`, so
*every* catalog-based policy check was silently skipped for a served session
over a `Connection` — the server's bridge and this extension's bridge alike.
`SELECT * FROM main.secret` went through a channel that named only
`pinkdb_memory` (the test that found it is
`crates/pinkdb/tests/internal_channel.rs::a_user_table_is_refused_every_way_a_read_can_reach_it`).
`Session::check_store` now lends the store for the check — the connection under
its shared lock for the length of the call — and `deny_read_databases` is
enforced on that path from then on.

**External services are their own switch.** `allow_files` used to gate both
`read_csv`/URLs and `external_io` host scalars such as 107c's `embed()`. They
leak different things (a path, or the text handed to a provider), so
`ServePolicy::allow_external_services` now gates the service call and the
operator's `allow-files` sets both; the memory channel takes services without
files. The call-site check in the executor and the bind-time `SERVICES_HINT`
both use the new switch.

**The MCP wire is shared.** `extensions/mcp-protocol` (new rlib) holds the
JSON-RPC dispatcher, the tool-set trait and both transports; `pinkdb-mcp` and
`pinkdb-memory` are two `ToolSet`s over the same `Host`. `pinkdb-mcp` loses
three files and keeps its tests (11) unchanged in behaviour.

**Measured gates** (all run on this worktree, one pass each):

| gate | result |
|---|---|
| `cargo fmt --all -- --check` | clean |
| `cargo clippy --all-targets -- -D warnings` | clean |
| `cargo test -q -p pinkdb-memory` | 13 passed, 1 ignored (live) |
| `cargo test -q -p pinkdb-agent` | 13 passed |
| `cargo test -q -p pinkdb-mcp` | 11 passed |
| `cargo test -q -p pinkdb-cli --test agent_run --test memory_run --test mcp` | 39 passed, 2 ignored |
| `cargo test -q -p pinkdb-cli --features embeddings --test embeddings` | 4 passed |
| `cargo test -q -p pinkdb-embeddings` | 20 passed, 1 ignored |
| `cargo test -q -p pinkdb-ext-loader` | 122 passed |
| `bash ext-sdk/posture.sh` | 13 checks |
| four `ext-sdk/*/smoke.sh` | rust 15, c 13, cpp 15, go 15 checks |
| `cargo test -q -p pinkdb --test current_user` | 6 passed |
| `cargo test -q -p pinkdb --test internal_channel` | 5 passed |
| `cargo test -q -p pinkdb --no-fail-fast` | 937 passed, 6 ignored |
| `cargo test -q --workspace --no-fail-fast` | 2,235 passed, 0 failed, 14 ignored, 210 binaries |
| wasm32 build of `pinkdb` | clean |

**Sizes** (release, default features): `pinkdb` 5,382,768 B,
`pinkdb-server` 4,977,888 B, `libpinkdb_memory.dylib` 2,746,160 B. The memory
extension is a cdylib and is not linked into either binary (`otool -L` on the
CLI lists no memory library); the linked seam is the two builtins, the internal
channel and the `.memory` verbs. Neither binary gained a dependency: the
extension's crates are workspace-only (`cargo tree` shows no third-party crate
that pinkdb did not already have).

**The `LOAD EXTENSION` order is not a factor — root cause found.** The wave
report recorded that two `LOAD EXTENSION` statements in one non-interactive
script drop the second (and the statement after it) when the agent's library is
loaded first. Re-running that exact order, through the same harness and the
same test, runs every statement: the pairing works. The only mechanism in the
tree that produces that symptom is an **unterminated statement**: the CLI
buffers SQL until a line's last code token is `;`, so a `LOAD` line without one
swallows the following line into a single statement, which then fails to parse
(`expected one of ";", EOF`) and both loads are lost. Reproduced directly: two
loads on one line and the following `SELECT` vanished exactly as reported; the
parse error goes to stderr in plain mode, so a run that watches stdout sees "no
output". The harness that produced the original observation built its `LOAD`
lines without a trailing `;`; today every load line ends with one, the stale
comment in `crates/pinkdb-cli/tests/agent_run.rs` is gone, and
`loading_the_agent_before_memory_still_runs_every_statement` pins the
agent-then-memory order end to end (both `Extension … loaded.` lines and both
following statements). No engine change was needed.
