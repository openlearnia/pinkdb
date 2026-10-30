# Wave 121e — internal state in its own store (plan)

Started and done 2026-10-30. The user, after dropping `main` and seeing
`pinkdb_worksheets` alone in the console's catalog: "we won't be revealing
the console's internal databases, we can keep them separate right?" They
chose to move the agent's databases out too, and to require an explicit
path when there is no file to put the state next to.

## What exists

Three databases live inside the user's warehouse:

| Database | Writer | Holds |
|---|---|---|
| `pinkdb_worksheets` | the console (`Book`, `Vault`) | worksheets, run history, access-token hashes, TOTP secrets, recovery-code hashes |
| `pinkdb_agent` | the agent extension, internal channel | sessions, messages, tool calls, usage, checkpoints |
| `pinkdb_memory` | the memory extension, internal channel | atoms, personas, runs, audit, distill queue |

They show in `SHOW DATABASES`, the console's catalog, completion and the
database selector, and an administrator can drop them (losing worksheets,
tokens and second factors; the console then fails until restart because its
tables are created once per process).

## Decisions

1. **A state store**: a second pinkdb `Database` attached to the warehouse
   (`Database::attach_state`, `PinkDb::state_store`). The warehouse never
   lists it, and no statement on the warehouse can reach it.
2. **Extensions' bookkeeping channel goes there.** Every host already runs
   an extension's own statements through one channel (`execute_internal`,
   `internal_on`), confined by policy to `pinkdb_agent` / `pinkdb_memory`.
   When the warehouse has a state store, that channel runs on a connection
   to the state store instead. The extensions do not change. The model's
   tool channel stays on the warehouse.
3. **Identity carries over.** Memory isolates users by asking
   `current_user(), current_roles()` on its internal channel. The state
   store has no users, so its connection gets the caller's principal and an
   explicit role list (`Connection::adopt_identity`); the binder answers the
   two builtins from them. Without this every user would share memory.
4. **Host-side reads go there too**: the server's memory worker finds its
   queue on the state store.
5. **Host scalars the bookkeeping uses** (`embed()` for memory's vector
   search) must exist in the state store: hosts that load the embeddings
   extension load it into the state store too. Without it memory falls back
   to BM25, which is what it already does when embeddings are absent.
6. **Where the state store lives** (server and CLI alike):
   - `--state PATH` names it.
   - Otherwise, a warehouse file `warehouse.pink` gets `warehouse.state.pink`
     next to it (any other name gets `.state.pink` appended).
   - An in-memory warehouse gets an in-memory state store.
   - A lake whose catalog is a local file (`.pink`, SQLite) gets the state
     store next to the catalog file; a remote catalog (Postgres, S3) with the
     console, the agent or memory enabled **requires** `--state` (the server
     refuses to start otherwise: in-memory state would drop second factors
     and tokens on restart).
   - An encrypted warehouse (CLI) gets a state store under the same key.
7. **The console** keeps worksheets and credentials in the state store
   (`Book`, `Vault` take it). An embedder that attached none gets an
   in-memory one (tests). (Re-creating the tables on demand turned out moot:
   nothing can drop the state store's databases any more.)
8. **No migration** (no legacy compatibility): existing `pinkdb_*` databases
   in a warehouse become ordinary databases an administrator may drop.
9. **A dropped current database**: the console falls back to an existing
   database instead of pointing at nothing.

## Tests

Engine: the internal channel writes the state store (not the warehouse),
`current_user()`/`current_roles()` there answer the caller's, two users'
memory stays apart through the state store. Server: sibling path rules,
`--state`, remote-lake refusal. Console: no `pinkdb_*` database in the
catalog, worksheets and credentials survive a restart from the state file,
current-database fallback. Extensions: agent and memory suites with a state
store attached. CLI: `.agent`/`.memory` on a file store write the sibling (the opening is lazy: a
session that runs neither creates no file).
