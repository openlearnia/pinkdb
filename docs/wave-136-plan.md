# Wave 136 — Memory: a limit, lazy storage, spill (ROADMAP 124)

Planned 2026-11-01, stream C of the QuackLab track. QuackLab runs its
servers and ETL workers on memory-capped Cloud Run instances; ROADMAP 124
asks for a memory budget (`SET memory_limit`) with spill for joins,
aggregates and sorts, and the track adds lazy loading of storage. Gate from
ROADMAP 124: TPC-H SF 10 completes under a 2 GB limit with checksums
matching DuckDB; a test forces spilling at tiny budgets for every operator;
no slowdown when nothing spills.

## What exists

- **No memory budget.** `ExecBudget` (`exec/mod.rs`) caps a statement's
  materialized rows/bytes (`ExecLimits::max_result_*`, the server's
  per-session limits) and fails when they are exceeded; nothing tracks what
  operators hold, nothing is shared across statements, and nothing spills.
  The streaming table-function fold has its own in-flight row counter
  (`stream_hold` / `stream_release`).
- **The whole store is decoded at open.** `persist.rs` `PinkDb::open` reads
  the `.pink` file into memory and `load_table_data` decodes every column
  chunk into `Column { chunks: Vec<Arc<Vector>> }` (a public field). The
  file's compression (bitpack, RLE, ALP, dictionary, FSST, LZ4) is undone
  up front: QuackLab's 94 MB store is 0.9 GB of RSS before the first query,
  and opening it costs 0.17 s against DuckDB's 0.04 s (its server holds
  850 MB against DuckDB's 70–90 MB).
- **Operators hold everything.** Hash-join builds, aggregate partials
  (one per chunk, wave 135d), sorts and materialized CTEs live in memory
  until the statement ends. Measured peaks (wave 135d binary): QuackLab
  refresh statement `r50` 2.4 GB, the refresh phase 3.2–3.6 GB against
  DuckDB's 0.87–0.91 GB; TPC-H Q9 at SF 1 676 MB (wave 111).

## Decisions

1. **One memory pool per process** (DuckDB `BufferPool`,
   `standard_buffer_manager.cpp`, `temporary_memory_manager.cpp`): an atomic
   byte count with reservations. Operators that can hold a lot reserve
   before they grow (hash-join builds, aggregate partials and merged tables,
   sort buffers, collect-sink and CTE outputs, decoded storage chunks) and
   release when they drop. `SET memory_limit = '2GB'` (DuckDB's name and
   unit syntax), process-wide like DuckDB's global setting (an
   administrator's statement on the server); the default is 80 % of physical RAM, or
   of the cgroup limit (`/sys/fs/cgroup/memory.max`, cgroup v1 fallback) when
   one is set — Cloud Run's cap. `current_setting('memory_limit')` and the
   statement profile report the limit and the peak.
2. **Over the limit, first evict, then spill, then fail.** Decoded storage
   chunks are a cache and are evicted first (decision 3). An operator whose
   reservation still fails spills if it can (decisions 4–6); otherwise the
   statement fails with DuckDB's "Out of Memory Error" wording naming the
   limit. Nothing is half-written: spilling happens before any write-back.
3. **Lazy storage.** Opening a store keeps each column chunk encoded (the
   file's bytes, shared) and decodes it on first use into a cache charged to
   the pool; zone maps and statistics stay in memory (they are in the
   footer). Scans that zone maps prune never decode. Writes decode the
   chunks they change (copy-on-write, as today). `Column.chunks` stops being
   a public `Vec<Arc<Vector>>`; callers go through accessors. Format and
   public-type change → full gate.
4. **Aggregate spill.** The flat and general hash aggregates already
   partition groups by hash for the merge (wave 111c/135d). Over budget, a
   partial's partitions are written to a temporary file and merged partition
   by partition, in chunk order, so groups, first-seen order and float sums
   are exactly the in-memory ones.
5. **Join spill (grace hash join).** A build that does not fit is
   partitioned by key hash with its probe side and joined partition by
   partition (DuckDB `physical_hash_join.cpp` external join,
   `radix_partitioned_hashtable`). Rows and matches are unchanged; the
   output order of a spilled join may differ from the in-memory order, which
   only happens where the statement would otherwise have failed.
6. **External sort.** Sorted runs written to temporary files and merged
   (DuckDB `physical_order.cpp`, the external sort); stable, so the order is
   exactly the in-memory sort's.
7. **Temporary files** live in `SET temp_directory` (default
   `<store>.tmp/`, or the OS temp dir for an in-memory store), are encrypted
   with the store's key when the store is encrypted (wave 46), deleted when
   the statement ends (success, error or cancel), and swept at open.

## Steps (one pi brief each)

| step | content | gate |
|---|---|---|
| 136a | the pool, reservations in the big operators, `SET memory_limit`, the default from RAM/cgroup, reporting, the out-of-memory error | targeted |
| 136b | lazy storage (decision 3) | full |
| 136c | aggregate spill | targeted |
| 136d | grace hash join | targeted |
| 136e | external sort; TPC-H SF 10 under 2 GB | full |

## Measurements

Per step: TPC-H SF 1 A/B (no slowdown when nothing spills), peak RSS of
TPC-H Q9/Q18/Q21 at SF 1 and of the QuackLab refresh, QuackLab store open
time and server RSS, and (136e) TPC-H SF 10 under `memory_limit = '2GB'`
against DuckDB.

## Tests

Every spilling operator forced to spill at a tiny limit (results equal the
in-memory run), out-of-memory errors for operators that cannot spill,
cancellation and errors during a spill (temporary files removed), an
encrypted store's temporary files unreadable without the key, a crash
leaving temporary files that the next open removes, lazy storage under
eviction (results equal a fully decoded store), wasm32 (no temporary files:
the limit fails instead of spilling).
