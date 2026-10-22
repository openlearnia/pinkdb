# Wave 89 — lake replication jobs (plan)

Started 2026-10-22; **done** the same day. ROADMAP row 89, reshaped 2026-10-22 after reviewing
the ducklake-vortex implementation
(`~/Desktop/projects/ducklake-vortex/ducklake/src/replication/ducklake_replication.cpp`):
replication is **table-level jobs between a source and a writable
destination lake**, not a byte mirror of a whole lake. Decided:
copy-only (share mode deferred), whole-lake byte mirror deferred, the
supervisor included.

## Model

- A **destination** is an ordinary writable lake. It may hold the user's own
  tables next to replicated ones, and any number of jobs.
- A **job** replicates the tables of one **source** into the destination
  under the same `db.table` names:
  - `Source::Lake { location, key }` — another lake (local or `s3://`).
  - `Source::Store { database }` — a database of the caller's live
    `PinkDb` store (the CLI's session store).
- Jobs and their per-table state live **in the destination lake**, so any
  process that opens the destination sees them.
- Every destination write is an ordinary incremental lake commit
  (`Lake::commit_from_tables` with only that table in `base`/`current`): a
  replicated version is a normal version (time travel, attach, CDC,
  maintenance all apply), and the existing concurrent-writer rules (rebase /
  retry / conflict) govern races with other writers.

## State: `replication/state.json` in the destination

```json
{
  "replication_format": 1,
  "next_id": 3,
  "jobs": [{
    "id": 1,
    "source": { "kind": "lake", "location": "/data/primary" },
    "status": "created",
    "interval_ms": 60000,
    "include": "main.*", "exclude": "", "watermark_columns": "main.events=ts",
    "created": "…", "last_run": "…", "last_ok": "…", "last_error": null,
    "tables": [{
      "source": "main.events", "dest": "main.events",
      "strategy": "watermark", "watermark_column": "ts", "pk_columns": null,
      "source_table_id": "…", "last_watermark": "2026-10-22 10:00:00",
      "last_source_version": 41, "last_full_sync_version": 12,
      "rows_dest": 1234, "last_error": null, "synced": "…"
    }]
  }]
}
```

- `source.kind` is `"lake"` (`location`) or `"store"` (`database`). A
  lake source's encryption key is never stored; it is supplied per run.
- `status`: `created` (never supervised), `running`, `paused`, `error`.
- Written through the `Storage` layer with conditional writes
  (`put_new` / `put_if_match`): a lost race re-reads and retries (bounded),
  never overwrites a concurrent change. Sealed like every other artifact
  when the destination is encrypted. Lake cleanup never touches
  `replication/` (it only deletes unreferenced `data/` objects); verify.

## Table identity (new, small)

Catalog table entries gain `table_id` (random 128-bit hex,
serde-default, omitted when absent):

- assigned when a table is created (full save of a new key, commit
  creating a table), **preserved** for the same key across later saves,
  commits and maintenance, **new** when a table is dropped and recreated;
- any version written by this build assigns ids to entries that lack one
  (old lakes acquire ids on their next write);
- old builds ignore the field.

Replication compares the source entry's `table_id` with the one recorded at
the last sync: a change (drop + recreate) forces a reseed. A source entry
with no id cannot be trusted across cycles: a snapshot-strategy table then
reseeds each cycle until the source writes again.

## Strategy per table (ducklake order)

1. **watermark** — configured in `watermark_columns` as `db.table=column`
   (must exist; error naming it otherwise). Append-only cursor: NULL values in
   the column refuse the table before any change; each cycle deletes
   destination rows with `col >= last_watermark`, then inserts source rows
   with `last_watermark <= col <= high` (boundary replay catches rows that
   arrived with the previous maximum); a `high` below `last_watermark`
   (source recreated or its max row deleted) reseeds. Works for both source
   kinds.
2. **snapshot** — lake sources. Cursor `last_source_version`. Apply
   `pink_lake::cdc::table_changes(low+1 ..= high)` per version in order:
   inserts deduplicated by full-row match against the destination, then
   deletes by full-row match (NULL-safe). Then verify the destination equals
   the source at `high` as a multiset (both directions); any difference
   reseeds from the source at `high`. Reseed instead of applying when: no
   prior cursor, identity changed, the CDC read refuses the range (a
   full-save version inside it, or expired history), or the cursor is below
   the source's oldest retained version.
3. **merge** — store sources whose table declares a PRIMARY KEY: delete
   destination rows whose key is missing in the source or whose non-key
   values changed, insert source rows whose key is missing in the
   destination (unchanged rows keep their positions, so the commit is
   minimal).
4. **full** — everything else: destination := source.

## One catch-up cycle

`replication::catch_up(dest: &mut Lake, job_id, store: Option<&PinkDb>,
source_key: Option<EncryptionKey>) -> Result<CatchUpReport>`:

1. Refuse a `paused` job. **Cycle guard:** an in-process set keyed by
   (destination location, job id); a second concurrent cycle is refused.
2. List source tables (`db.table`), minus `include`/`exclude` globs
   (comma-separated, `*` and `?`, matched on `db.table`; `*` / `*.*` match
   all). A lake source's unavailable-format tables (wave 88) fail per table.
   A source that cannot be listed fails the whole cycle (never treated as
   empty — that would drop every replica table).
3. **Drop stale:** tables in the job's state whose source table is gone or
   now excluded → commit a drop of the destination table, delete the state
   row. Failures are recorded per table.
4. **Each source table**, isolated (one failure never aborts the cycle):
   - **Ownership:** another job's state already claims the destination key
     → error, no state row written. The destination table exists but this
     job has no state row for it → "exists and is not managed by this
     job", no state row. Otherwise claim it (state row with no cursor)
     before the first data commit.
   - **Schema:** identical keeps data; a strict name+type prefix extension
     adds the new columns in place if the lake commit path supports it
     (wave 65 schema evolution), otherwise recreates; anything else drops
     and recreates (which reseeds). Extension also reseeds watermark and
     snapshot tables; merge backfills through its diff.
   - Compute the destination table's new content by the strategy, commit
     it (one lake commit for this table; an empty diff publishes nothing),
     then record the state row: strategy, cursors, `source_table_id`,
     `rows_dest`, `synced`, `last_error = null`.
   - On failure: roll nothing forward, record `last_error` on that table's
     row (only if this job owns it).
5. Job status: success → `running` if a live supervisor owns it, else
   `created` (a concurrent `paused` always wins); any table failure →
   `error` with `"N table(s) failed: …"`. `last_run` always, `last_ok` on
   success.

`CatchUpReport`: tables synced / failed / dropped, rows written, message.

## Job API and supervisor

- `replication::create(dest, JobSpec { source, interval, include, exclude,
  watermark_columns }) -> job id` (refuses a lake source equal to the
  destination), `drop(dest, id)` (replicated tables stay; state removed),
  `jobs(dest)`, `tables(dest, id)`, `pause`, `resume`, `resume_all`,
  `versions_behind(dest, id)` (lake sources: source head − min
  `last_source_version`; `None` for store sources).
- `Supervisor` — one thread per destination per process: runs due
  `running`/`error` jobs with a **lake** source, each on its interval
  (minimum 250 ms), backing off exponentially up to 60 s after a failed
  cycle; `start(dest_location, key)` marks runnable jobs `running`,
  `stop()` joins, `pause/resume` per job, a `notify` to run a job now.
  Store-source jobs are not supervised this wave (the CLI's store is
  single-threaded): `start` reports them as skipped; they run through
  `catchup`. Shutdown aborts an in-flight cycle at the next table boundary.

## CLI

```text
.lake replicate create <dest> from <source-dir|s3://…|store:<db>> [every <secs>]
                       [include <patterns>] [exclude <patterns>] [watermark <db.t=col,…>]
.lake replicate catchup <dest> <id>
.lake replicate start|stop <dest>
.lake replicate pause|resume <dest> <id>
.lake replicate resume-all <dest>
.lake replicate status <dest>            -- jobs: id, source, status, behind, last ok/error
.lake replicate tables <dest> [<id>]     -- per-table state
.lake replicate drop <dest> <id>
```

Encrypted lakes use the session key exactly like the other `.lake`
commands. If the destination is also attached writable in the session, its
attachment base goes stale after a replication commit; the existing
`.lake commit` rebase/conflict rules apply (documented).

## Tests

Lake→lake and store→lake jobs: seed, incremental cycles for each strategy
(watermark boundary replay, NULL refusal, backwards reseed; snapshot with
inserts/deletes/updates, a full-save version in range → reseed, expired
history → reseed, verification mismatch → reseed; merge insert/update/
delete with unchanged rows untouched; full), include/exclude, stale-table
drop (source drop and newly excluded), identity change (drop + recreate on
the source) → reseed, schema drift (same, additive, incompatible),
ownership (unmanaged existing table, two jobs on one table), per-table
failure isolation (one bad table, others commit), cycle guard, paused job
refused, state after crash between commit and state write converges on the
next cycle, encrypted source and destination, mock-S3 destination,
`table_id` assignment/preservation/legacy upgrade, supervisor
start/pause/resume/stop/backoff/notify with millisecond intervals, CLI
commands end to end.

## Not in this wave

Share mode (zero-copy file registration), whole-lake byte mirror (DR copy),
supervising store-source jobs, update pre/post-image pairing in CDC, SQL
table-function surface for the job API.

## Tasks

- **T1 (pink-lake):** `table_id`, `pink_lake::replication` (state, jobs,
  strategies, catch-up, report, versions_behind), tests
  `crates/pink-lake/tests/replication.rs`.
- **T2 (after T1):** `Supervisor` in `pink_lake::replication`, CLI
  `.lake replicate …` in `crates/pinkdb-cli/src/lake.rs`, CLI tests.

## Rules for delegated agents

Do not commit, push or create branches. Touch only the files the task
names. Use `CARGO_INCREMENTAL=0`. Report what changed and every check.

## How it was built

The first draft of this plan was a whole-lake byte mirror; after reviewing
ducklake-vortex's job-based replication the wave was re-planned around jobs
(the mirror and share mode are deferred). A delegated run of the mirror plan
that had been stopped kept running as an orphaned process and added a
`replica` module next to T1's work; it was removed in review.

Delegated to CommandCode (DeepSeek V4.1 Flash), reviewed here: **T1** —
`table_id`, `pink_lake::replication` (state, jobs, the four strategies,
catch-up, ownership, identity and schema-drift reseeds), tests; **T2** — the
`Supervisor` (one thread per destination, condvar wake, 250 ms minimum
interval, exponential backoff to 60 s, abort at table boundaries, a
process-global registry behind `supervisor_owns`), the `.lake replicate …`
CLI and README section. T2 also serialized in-process state writers with a
lock: a local conditional write has a tag-check → rename window in which an
in-flight cycle's status write could overwrite a concurrent `pause`.

Review fixes:

- **Bit-exact replication.** Row matching used the engine's SQL equality
  (`-0.0 = 0.0`, all NaNs equal), so snapshot de-duplication, the merge's
  changed-row test and the post-apply verification could keep `0.0` where
  the source has `-0.0`. Those paths now compare bit-exactly; primary keys
  still match with SQL equality.
- **Linear snapshot apply.** Applying a version's inserts and deletes
  scanned the whole destination per changed row (quadratic); it now indexes
  row positions by an exact row key.

Known limits recorded: store-source jobs are not supervised (the CLI store is
single-threaded); a drop + recreate entirely between two full saves keeps
the table id (the engine store has no table identity), while a published drop
mints a new one; `versions_behind` for an encrypted source needs the key and
reports the keyed-lake error otherwise; when the destination cannot be opened
the supervisor retries each second without backoff.
