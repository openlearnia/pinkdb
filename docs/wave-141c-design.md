# Wave 141c — a row-layout build side for hash joins: design note

Status: proposal. No code yet. 141a (typed gathers resolved once per join) lands first; this note
says what the next step would change, when it pays off, and how it sets up join spill (136e).

## The cost it attacks

A hash join's output gathers the build side's payload columns for every match. Today the build side
is the build input's own chunks, kept columnar. A match is a `(chunk, row)` location, and gathering
`k` payload columns for it reads `k` separate column payloads. Each read lands in a different chunk's
allocation, so on a build side larger than the caches it is `k` cache misses per match.

In TPC-H Q10 at SF10:
- the last join's build side is nation ⋈ customer ⋈ orders, about 570k rows;
- it carries 7 payload columns (`c_custkey`, `c_name`, `c_address`, `c_phone`, `c_comment`,
  `c_acctbal`, `n_name`);
- filtered `lineitem` probes it, giving about 1.1M matches, so about 7.7M scattered reads.

Before 141a, `gather_located` was 11,444 of about 25,000 worker samples (46 %). 141a removes the
per-row type dispatch, so what remains is mostly the memory traffic of those scattered reads.

## The design (after DuckDB v2.0.0, credited)

DuckDB keeps a join's build side in a `TupleDataCollection` (`src/common/types/row/`): one row per
build tuple, laid out by `TupleDataLayout` as
`[validity bytes | fixed-width column values | hash | next-in-chain pointer]`. Strings are stored as
16-byte `string_t` with the bytes in a separate heap block. The probe finds row pointers.
`ScanStructure::GatherResult` and `JoinHashTable::GatherRHS` then fill each output column from
`row_ptr + column_offset` (`TupleDataCollection::Gather`). One match's columns therefore sit in the
same few cache lines.

For pinkdb:
- **Layout.** A `RowBlock` holds the payload columns only, fixed width per row: a validity word
  (64 columns per word), then each column at a fixed offset. Integers, dates and decimals are stored
  inline. VARCHAR is stored as pinkdb's 16-byte `StrView` (already the in-memory string form, inline
  up to 12 bytes), with long strings' bytes in a per-block heap, as `StrVec`'s shared buffers are
  today. The keys stay where the match table needs them; the row holds only what the output
  gathers.
- **Build.** The build sink scatters each incoming chunk's payload columns into rows of the
  thread's current block: per-thread blocks, no lock. The match table stores a row reference
  `(block, row)` in place of `(chunk, row)`.
- **Gather.** Per output column, a strided read at `rows[i].base + offset`. All columns of a match
  share the row's cache lines. Strings copy their 16-byte view and share the block heap through an
  `Arc`, as `StrVec::gather` shares buffers today.
- **Columns no consumer reads** (wave 127's placeholders) are left out of the layout entirely, so
  the row is no wider than the gathered set.

## When it pays

Let `k` be the gathered payload columns, `m` the matches, and `B` the build side's bytes:
- **Columnar:** about `k · m` scattered reads.
- **Row layout:** about `m · ⌈row_width / 64⌉` reads, plus the scatter at build time, which is one
  sequential write of `B`.

It wins when:
1. **`k ≥ 3`.** With one or two payload columns the row is one or two lines either way, and the
   scatter is pure overhead.
2. **The build side exceeds the last-level cache** (several MB). A build that fits in L2 or L3
   already gathers from cache.
3. **`m` is at least a few times the build rows** (a fan-out probe), or the build side is wide.
   A probe with few matches does not repay the scatter.

Q10, Q7, Q5, Q8 and Q9 at SF10 meet all three. Small dimension joins (nation, region) do not.

So the build sink chooses per join: the **row layout when `k ≥ 3` and the estimated build bytes are
above about 4 MB**, otherwise today's columnar chunks. The decision uses the build's actual size,
known when the build completes. It does not rely on an estimate, because the build is fully
materialized before any probe.

Measurement plan: an interleaved A/B against 141a on SF1 and SF10, per query, with the choice forced
both ways (`SET join_build_layout = 'rows' | 'columns' | 'auto'`, test-only) to confirm the
threshold. It lands only with geomean ≤ 1.00 and no query above 1.05 in `auto`.

## Spill (136e)

The row layout is also what a spilling hash join needs. DuckDB partitions the build rows by the high
bits of the hash into a `PartitionedTupleData` (`partitioned_tuple_data.cpp`). Under memory pressure
it writes whole partitions' row blocks to the temp directory, then joins partition by partition: a
grace hash join. pinkdb's version:
- **Partitioned build.** Each thread's `RowBlock`s are bucketed by radix (for example 16 to 64
  partitions, by hash bits). That also gives the parallel build a lock-free merge, because each
  partition's match table is built independently.
- **Spill unit = a partition's row blocks.** These are fixed-width rows plus a heap. On spill, string
  views that point into the block heap are rewritten to heap offsets (DuckDB's pointer
  "unswizzling"), and back on reload. Blocks are charged to the memory pool; under pressure the
  largest partitions are written out first.
- **Probe.** In-memory partitions probe as today. Probe rows that hash to a spilled partition are
  themselves partitioned and spilled. Each spilled partition is then joined after the in-memory
  pass, with its build reloaded on its own.
- The columnar build side cannot do this without a copy anyway: spill writes rows. So 141c is the
  layout 136e builds on, not a detour.

## Not in scope

- The probe side stays columnar, and the key match table is unchanged apart from its stored
  reference.
- Semi, anti and mark joins gather no build payload and keep today's path.
- An outer join's unmatched build rows are emitted by scanning the row blocks with a "matched" bit,
  as DuckDB does with its `found_match` array.
