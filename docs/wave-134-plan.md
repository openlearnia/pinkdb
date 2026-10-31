# Wave 134 — writes straight to lakes

Planned 2026-10-31, stream B of the QuackLab track, after wave 133. QuackLab's ETL writes lake
tables all day (`DELETE` + `INSERT` resets, `MERGE` upserts, metadata rows). A pinkdb lake
database has to write the way DuckLake does: new data files for inserted rows and delete files for
removed ones, published by an optimistic commit. Nothing may be held or diffed in memory.

## What exists

- **Wave 124 materializes writable lakes** and commits with `Lake::commit_from_tables`, which
  matches every base row against every current row to find the delta (`pink-lake/src/incremental.rs`
  "The diff"). On the 1.87M-row benchmark a lake refresh after a 50k-row append takes 62.8 s
  (DuckDB 4.7 s) and peaks at 5.5 GB.
- **pink-lake already has the low-level commit** (`Commit`: `append`, `delete_positions` per data
  file, `delete_inlined_rows`, partitioning, inlining limits) with DuckLake's writer retry
  (`CommitSettings`: 10 retries, 1.5 backoff, 100 ms) and rebase onto a moved head.
- **Delete files are pinkdb's own** `PINKDL1` container on the write side; 132a reads DuckLake's
  Parquet delete files.
- **The engine's DML** (`INSERT`, `UPDATE`, `DELETE`, `MERGE`) mutates `DataTable`s in place.

## Decisions

1. **A `TableWriter` seam** for DML: `append(chunk)` and `delete(rowids)`, implemented by store
   tables (today's in-place path) and by lake tables. `UPDATE` = delete + append (DuckLake's
   `ducklake_update` does the same); `MERGE` = matched rowids deleted or rewritten, unmatched
   source rows appended; `DELETE` collects rowids from the 133 scan's `rowid` column.
2. **Transaction-local lake state** per table: appended rows (in memory up to a bound, then
   spilled to a staging Parquet file in the data path) and a sorted set of deleted rowids. Reads
   inside the transaction see the snapshot minus those rowids, plus the appended rows minus
   their own deletes. Other sessions see the snapshot only.
3. **Commit = one DuckLake snapshot**: appends become Parquet data files (rows under
   `data_inlining_row_limit` are inlined, as DuckDB does), deletes become DuckLake Parquet delete
   files, merged with the data file's existing delete file the way `ducklake_delete.cpp` does.
   Published through `Commit` with the existing retry. DuckLake's conflict rules apply: concurrent
   deletes from the same data file conflict; appends and deletes in different files commute.
4. **Deleted, not kept**: `Lake::commit_from_tables`, the materializing writable opener, the
   whole-table publish of `LakeDatabase`, and the `PINKDL1` delete container with its reader
   (no legacy compatibility; a lake with `PINKDL1` files is refused with a clear message).
5. **Unchanged**: lake units stay out of the local WAL (wave 124); the writable lease stays for
   now; DuckLake has no enforced keys, so `PRIMARY KEY`/`UNIQUE`, indexes and triggers on lake
   tables are refused at `CREATE`.
6. **DuckDB interop both ways**: DuckDB and pinkdb write the same lake one after the other (and
   concurrently from two processes for the retry path), and each reads the other's snapshots.

## Briefs (pi)

- **134a** (engine): `TableWriter`, transaction lake state, DML through it, read-your-writes.
- **134b** (pink-lake): commit from a change set — Parquet data files, Parquet delete files,
  inlining, conflict rules; delete `commit_from_tables` and `PINKDL1`.
- **134c**: interop tests with DuckDB writing the same lake; benchmark rerun.

## Gate

Full gate. TPC-H refresh functions (RF1/RF2) against a lake match DuckDB; the QuackLab bench's
lake ETL and refresh agree with DuckDB.

## Measured

Before (wave 130, workaround lake path): ETL 44.6 / 35.2 / 19.0 s for initial / append /
correction (DuckDB 2.97 / 1.78 / 0.58); refresh 14.5 / 62.8 / 71.0 s (DuckDB 3.27 / 4.72 / 4.09).
The engine stream's MERGE and decorrelation fixes (etl-bugs-repro) change the ETL baseline too,
so rerun the bench right before 134 to separate the two effects. After: to fill in.
