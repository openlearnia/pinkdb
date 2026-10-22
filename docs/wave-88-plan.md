# Wave 88 — per-table file formats (mixed lakes) (plan)

Started 2026-10-21; **done** 2026-10-22. ROADMAP row 88: one lake catalog may hold tables in
different data-file formats; `.pink` stays the default; Parquet/Vortex
resolve through the wave-87 provider seam; the statistics contract holds
for every provider; unknown formats refuse cleanly; changing a table's
format is an explicit rewrite (in scope only if small).

## Where things stand after wave 87

Each catalog table entry already pins its `format` id, and attach/read
dispatch per entry through the provider. A handle has one write format
(`Lake::set_write_format`, default by build). What is missing is choosing a
format per table, keeping an existing table's format stable across saves,
and converting a table.

## Rules

1. **Format resolution for a full save**, in order: an explicit per-table
   choice on the handle (`Lake::set_table_format(db, table, id)`); else the
   entry's format if the entry is **pinned**; else the handle's write format
   (`set_write_format`), else the build default.
2. **Pinning (hybrid migration rule, decided 2026-10-22).** A format chosen
   explicitly — a per-table choice, an explicit `set_write_format`, or a
   `rewrite_table_format` — is recorded in the table's catalog entry as
   `"format_pinned": true` (omitted when false, so catalogs written by
   default stay byte-identical; older builds ignore the field). An entry
   written under the build default is **not** pinned and keeps the
   wave-45 behavior: a full save re-encodes it into the saving build's
   write format (a pinklake-written lake migrates to Vortex on its next save
   in a vortexlake build, as the README promises). An entry whose format is
   not a built-in default (anything but `pink`/`vortex`, e.g. `parquet`) is
   always treated as pinned.
3. **Appends** always use the entry's format and keep its pin flag; a table
   created by a commit uses the per-table choice, else the write format
   (pinned when explicitly chosen).
4. **Converting** a table is explicit: `Lake::rewrite_table_format(db,
   table, id)` / `.lake rewrite <dir> <table> format <id>` rewrites every
   file into the new format in one atomically published version and pins
   it; old files are retired by the existing expire/cleanup path.
5. **Unavailable formats are refused per table, not per lake.** In a
   format_version 4–6 catalog, an entry whose format id has no registered
   provider (`UnknownFormat`) or needs a feature the build lacks
   (`FeatureMissing`, e.g. `vortex` without `vortexlake`) no longer refuses
   the whole lake:
   - `open`/`attach` succeed; `Lake::unavailable_tables()` lists each such
     table with its format id and the error.
   - `attach`: the table is listed, and any read of it (scan, COUNT/MIN/MAX
     fast path) fails with that error naming the table and the id.
   - `load` (`.lake open`): the readable tables are materialized, the
     unavailable ones are skipped, and the CLI prints one warning line per
     skipped table.
   - A full save carries an unavailable entry forward **verbatim** (same
     files, statistics, pin) when the store has no table under that key; if
     the store has a table with that key the save is refused (it would
     replace data the build cannot read).
   - A commit/append, rewrite or compaction that targets an unavailable
     table fails with the error; lake-wide maintenance skips it and says so.
     Expire/cleanup work on files and are unaffected.
   - v2/v3 catalogs (all-Vortex by definition) keep the whole-lake
     `FeatureMissing` refusal, and corrupt entries (a format that is
     invalid for the catalog version) still refuse the lake.
6. **Statistics:** attach answers exact COUNT/MIN/MAX per entry from that
   entry's provider path (footer for Vortex, manifest block for pink and
   parquet) — mixed lakes must give the same answers as `.lake open`.
7. **One shared format contract test** runs the same checks against every
   provider (`pink`, `vortex` with `vortexlake`, `parquet` from
   `pink-lake-parquet`): type round-trips incl. NULLs, NaN/±0.0/±inf,
   unicode, empty tables and multi-batch tables; clean refusal (never a lossy
   write) of a type a provider cannot store; save → open equals the source;
   attach COUNT/MIN/MAX equal the opened store; appends keep the format;
   `rewrite_table_format` to and from the provider preserves the data.

## CLI

`.lake format [<id>]` (wave 87) gains `.lake format <id> for <db.table>`
(per-table choice for later saves) and `.lake format` without arguments
also lists per-table choices. A rewrite into another format:
`.lake rewrite <dir> <table> format <id>` (match the existing `.lake
rewrite` syntax), only if rule 3 is implemented.

## Tests

A mixed lake with three tables in `pink`, `vortex` and `parquet` (a test
build with both `vortexlake` and the Parquet provider): save → open equals
the source; attach COUNT/MIN/MAX equal to open for each table; appends keep
each table's format; a whole-store save with a different handle default
keeps every existing table's pinned format; a per-table choice applies to a
new table; rewrite converts one table (if implemented) and attach/open
agree before and after; an entry naming an unknown id refuses the lake.

## Rules for delegated agents

Do not commit, push or create branches. Touch only the files the task
names. Use `CARGO_INCREMENTAL=0`. Report what changed and every check.

## How it was built

The first cut (per-table choice, rewrite into another format, CLI) kept
every existing table's format on save, which broke the README's wave-45
promise that an old lake migrates on its next save. Decided 2026-10-22: the
hybrid pinning rule (rule 2), per-table refusal of unavailable formats
(rule 5) and the shared contract test (rule 7); the ROADMAP row's "per
version" wording was dropped (one entry has one format).

Delegated to CommandCode (DeepSeek V4.1 Flash), reviewed here: T1 pinning +
per-table refusal (catalog `format_pinned`, `Lake::unavailable_tables`,
attach/load/save/commit/maintenance paths, CLI warnings, migration tests
restored unchanged), T2 the contract (`crates/pink-lake/tests/common/
format_contract.rs`, run by `pink-lake` for `pink`/`vortex` and by
`pink-lake-parquet` for all three).

The contract found two bugs, fixed in this wave:

- **Vortex panicked on DECIMAL/INTERVAL** instead of refusing them:
  `encode_table_into` built the file metadata (whose type tags have no
  mapping for them) before the `NotRepresentable` check. The check now runs
  first.
- **Extreme INTERVAL statistics were undecodable**: the `.pink` statistics
  codec decoded the canonical interval text through the SQL parser, which
  accumulates in `f64` — `micros = i64::MIN` failed (and `attach` refused
  the lake), magnitudes above 2^53 rounded. An exact integer decoder for the
  canonical form now runs first; older blocks still decode through the old
  path, and the text written is unchanged.

An unreadable attached table advertises `pink_lake::UNAVAILABLE_ROWS`
(`u64::MAX`), which the engine's statistics fast paths already refuse
(`i64::try_from`), so `COUNT`/`MIN`/`MAX` fall back to the loader and return
the per-table error.
