# Wave 96b — a stateless pinkdb server

`pinkdb-server --catalog <pink:|sqlite:|postgres://…> --data <dir|s3://…>` serves
a store whose **catalog metadata and table data both live in a lake**: restart
the server anywhere on the same catalog + data and get the same databases,
tables, views, macros, materialized views, procedures, triggers, RBAC,
extensions and rows. Nothing is read from local scratch state.

This document is the design pass ROADMAP row 96 asks for: what goes where, what
publishes when, what the local WAL is still for, and the phase split.

## 1. Where the metadata goes

The `ducklake_*` tables are fixed by DuckDB interop, so the engine's catalog
cannot be columns on them. It goes in **`pinkdb_engine_catalog`**, beside them
in the same catalog database (created lazily, never touched by DuckDB):

```sql
CREATE TABLE IF NOT EXISTS pinkdb_engine_catalog (
    id        INTEGER PRIMARY KEY,  -- always 1
    revision  BIGINT NOT NULL,      -- the lake version the image was published with
    encoded   BLOB   NOT NULL       -- the engine catalog image (crc32c trailer inside)
);
```

One row, replaced in place; the image's trailing crc32c is the integrity check, so
a truncated or edited image is refused instead of loaded silently. It is written through the same
`MetadataStore::commit`/`commit_in_transaction` path the lake's own deltas use
(`CatalogChange`), so **one catalog transaction publishes data files + lake
delta + engine metadata**; a crash before the transaction commits publishes
nothing at all (the data files are orphans the next commit's head does not
name), a crash after it publishes both.

Why one opaque image and not a table per object kind (`pinkdb_engine_table`,
`pinkdb_engine_view`, …): the engine catalog is one object graph — a view names
tables, a procedure names tables, a foreign key names a table, a grant names a
table — and every consumer of it wants the whole graph at once (the store is
built from it in one step, and it is replaced as a whole on every commit). A
relational decomposition would duplicate the engine's footer codec
(`crates/pinkdb/src/persist.rs`) in a second encoder/decoder pair, with no
reader that wants the parts. The lake catalog is the wrong place to re-model
the engine's catalog; it is the right place to *store* it atomically. This also
keeps the DuckDB-visible schema untouched: `pinkdb_engine_catalog` is not a
`ducklake_*` table and DuckDB never reads it.

The image carries exactly what the `.pink` footer carries today — the same
encoder, with the row pages omitted — plus the sections the footer does not have
(views, loaded extensions). Concretely, `PINKCAT1`:

```
magic "PINKCAT1" | u32 format | u64 len | .pink image (no rows)
u32 extensions  | (str source)*
u32 views       | (str db, str name, str CREATE VIEW text)*
u32 crc32c
```

Loaded-extension `source` strings are replayed through the same loader path a
`LOAD EXTENSION` statement uses, so a restart's `--allow-load-extension` /
`--extension-root` policy still decides what the image may load.

Settings: the engine has no store-level `SET`/`PRAGMA` registry, so there is
nothing to carry. The settings that exist are the lake's (data inlining limit,
per-table partition specs, write format) and they already live in the DuckLake
catalog.

The image is a *metadata* image: `to_catalog_image` runs the `.pink` encoder
with `omit_rows`, so table entries keep columns, constraints, defaults, indexes,
macros, materialized-view definitions, procedures, triggers and RBAC, and every
table records `row_count = 0` with no row groups. Rows come from the lake's data
files and inlined rows; column statistics are not carried (they are derived
from rows, are an optimization only, and the lake's file footers already carry
them).

**Table data stays in lake data files** (unchanged format: `.pink` data files).
A server store therefore has no local state at all: `--data` is a directory or
`s3://` prefix, `--catalog` is the metadata database.

Engine-core edits are kept to the minimum: a `ViewDef` gains the `CREATE VIEW`
source text (a bound `Select` cannot be rendered back to SQL — the same reason
`MaterializedViewDef` already stores `query_sql`), the store gains
`to_catalog_image`/`load_catalog_image` and one publish hook seam. Nothing in
planning, binding or execution changes.

### Table data vs. metadata on load

`load_catalog_image` loads the image (which creates every database, table,
macro, materialized view, procedure, trigger, view, extension and RBAC entry),
then materializes the lake's tables into the store:

- a table the image knows keeps its image metadata (constraints, defaults,
  indexes — the authoritative copy) and receives the lake's rows;
- a table only the lake knows (created by DuckDB, or by an engine catalog
  written before this wave) is created from the lake's data-file footer, which
  carries its full `.pink` metadata.

So a table DuckDB creates in a catalog-mode lake loads too. A lake with no
engine image at all is refused (`the lake at … holds tables but no engine
catalog image`): such a lake was not written by a `--catalog` server, and
phase 1 does not invent an engine catalog for it.

## 2. One transaction per commit

The publisher is a `Database`-level hook the server installs
(`Database::set_commit_hook`), called after every committed statement of a
catalog-mode store:

1. the hook takes the pending statement's committed state (`&PinkDb`),
2. builds the engine image (`to_catalog_image`),
3. diffs the store against the attachment's pinned base
   (`WritableAttachment::commit`, the CLI's own commit loop — no second
   transaction path),
4. with `Lake::set_engine_catalog(image)` the image rides the very same
   `CatalogChange`, so `try_publish_commit` writes it inside the lake's publish
   transaction.

A metadata-only statement (`CREATE VIEW`, `GRANT`, `CREATE ROLE`,
`LOAD EXTENSION`) changes no table, so it produces no lake delta: the engine
image is published by `Lake::publish_engine_catalog`, one small transaction that
replaces the row and leaves the lake version alone. `CREATE VIEW` must not mint
a lake version (DuckDB would see a snapshot with no change) and does not need to.

Rule of thumb: **one committed statement = at most one catalog transaction**.
A statement inside `BEGIN … COMMIT` publishes once, at the `COMMIT`; a rollback
publishes nothing.

## 3. The WAL's role

Today the local `.pink` file + WAL is the store's durable record; a WAL append
is what makes a statement durable, and the WAL lease is what keeps two
processes from writing one file.

In catalog mode the catalog transaction **is** the durable record: it names the
data files (and their deletes) and the metadata image, atomically. So:

- the catalog-mode store is **in memory**; there is no local WAL, no
  checkpoint, no replay, and nothing to recover on startup;
- `--file` is untouched: same `.pink` + WAL, same lease, same replay, same
  durability — and it stays the way to run a store with no lake.

What the WAL still does, and what it does not:

| job | `--file` | `--catalog` |
|---|---|---|
| durability of a statement | WAL append | catalog transaction |
| crash recovery | WAL replay | re-read the catalog + data |
| one writer per store | WAL lease | engine lease row (section 4) |
| statement history | WAL frames | not kept (the lake's `pinkdb_table_change` keeps the lake's own) |

This is why "stateless" is honest: the only durable thing a catalog-mode server
writes is the catalog and the lake.

## 4. One server at a time (phase 1)

A second server on the same catalog must be refused, not allowed to race the
first one's publishes. Phase 1 uses a lease row in the catalog's opaque-object
table (`pinkdb_object` under `engine/lease`): one line,
`<owner>\t<heartbeat_ms>`, where the owner names the host and process.

- open: no row, or a heartbeat older than `LEASE_TIMEOUT` (6 s) → take it; a
  fresh one → fail with `already served by <owner> (heartbeat <n>s ago)`;
- a background thread refreshes the row every 2 s, so an idle server keeps it;
- every publish re-reads the row first and refuses with `lease lost` if another
  owner holds it, so a takeover cannot interleave two writers silently;
- clean shutdown deletes the row.

A crashed server's lease goes stale after 6 s and the next server takes it
over — which is what the crash test needs, and why the test sleeps just over
`LEASE_TIMEOUT`. This is deliberately the *small* lease: fencing, epochs and
cache invalidation are phase 2.

## 5. Secrets

Unchanged from today, and better placed:

- RBAC password hashes are Argon2id PHC strings (`$argon2id$…`) — the image
  carries the hash, never a password;
- catalog and lake encryption keys never enter the image or the catalog: a
  keyed lake is opened with `Lake::open_with_catalog_with_key` and the key comes
  from the operator (flag/env). `pinkdb-server` has no key flag yet (the same
  limit as `--file`), so phase 1 serves an unkeyed lake;
- extension paths are recorded, extension *content* is not: a restart re-loads
  through the server's `--allow-load-extension` + `--extension-root` policy, and
  a path the policy refuses fails the startup with the loader's error;
- the image is stored as the lake stores every other catalog fact: plaintext
  unless the lake is keyed. A password hash is not a password, but an operator
  who wants the catalog at rest encrypted uses a keyed lake.

## 6. Phases

**Phase 1 (this wave).** One server, stateless across restarts:

- `--catalog` + `--data` modes; `--file` unchanged;
- every metadata object round-trips through `pinkdb_engine_catalog`;
- one catalog transaction per commit; crash-consistency;
- single-server lease with a clear refusal;
- RBAC (login + grants) after restart; views, macros, materialized views,
  procedures, triggers, extensions round-trip.

**Phase 2 (scoped, not implemented).** Several servers on one catalog:

- real leases: fencing tokens, per-object epoch checks, so a stolen lease
  cannot produce a lost update;
- cache invalidation: a server that did not publish must notice another's
  publish before serving (per-table version / commit-sequence polling or
  LISTEN/NOTIFY on Postgres);
- `pinkdb_engine_catalog` becomes append-only (one row per version) so a stale
  server can be told what it missed, and old rows are pruned by the same
  retention pass that prunes lake snapshots;
- the server hook learns to publish partial images (the objects a statement
  touched) instead of the whole catalog image.

**Not in scope either way:** multiple engines writing one *lake* through
different catalogs (that is DuckLake's own concurrency contract), and any
local caching layer.

## 7. Known limits of phase 1

- A publish holds the store's write lock: a large commit stalls other
  statements on that server (the CLI has the same property).
- The pinned base is materialized in memory (as for `.lake attach`), so memory
  is store + base, not store alone.
- An idle server that loses its lease without publishing (a takeover after a
  network partition) only finds out on its next publish; phase 2's fencing
  makes that check atomic with the commit.
- Column statistics are not carried in the image and are not recomputed at
  load; plans after a restart may differ, results do not.
