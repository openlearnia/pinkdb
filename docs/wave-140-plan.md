# Wave 140 — ETL hot spots (correlated EXISTS over derived tables, MERGE, CTAS)

Planned and started 2026-10-31, stream C of the QuackLab track. Stream B's
QuackLab benchmark (wave 130, `bench/quacklab/`) measured QuackLab's ETL at
42.9 s on pinkdb against 1.84 s on DuckDB for the 1.87M-row initial batch,
and 12.0 s against 0.10 s for the one-row correction batch. Almost all of
the gap is three engine defects (`pinkdb-wt/briefs/etl-bugs-repro.md`):

| step | pinkdb | DuckDB |
|---|---|---|
| staging validation (7 foreign-key `NOT EXISTS` checks per fact table) | 29.9 s | 0.10 s |
| `MERGE` staging → ingest, full batch | 9.9 s | 0.21 s |
| `MERGE`, one-row correction | 10.9 s | < 0.1 s |
| `CREATE TABLE … AS SELECT` of 1.92M × 18 | 7.9 s | < 1 s |

Goal: QuackLab's ETL around 3 s on the small batch, results unchanged.

## What exists

- **Decorrelation happens in the binder** (wave 120, `sql/binder.rs`
  `decorrelate_clause` → `decorrelate_predicate` → `correlated_body` →
  `dependent_join`). A correlated `EXISTS` / `IN` / scalar body becomes a
  semi, anti, mark or left join when its base is a *from tree*: scans,
  values, table functions and inner/cross joins, with the correlation
  conjuncts in `Filter`s that `strip_from_filters` lifts out. Anything
  else (`is_from_tree` false) keeps the per-outer-row executor path.
- **The body is optimized before decorrelation** (`optimize_subquery`), so
  filter pushdown has already moved a correlated conjunct as deep as it
  goes: through a `UNION` it is cloned into both branches
  (`optimizer.rs`, the `SetOp` arm of pushdown), through a derived table's
  projection and below a grouped aggregate's keys.
- QuackLab's check is
  `NOT EXISTS (SELECT 1 FROM (SELECT k FROM a UNION SELECT k FROM b) p WHERE p.k = c.k)`.
  Its body is `Project(SetOp(Filter(…a…), Filter(…b…)))`: not a from tree,
  so 1.92M outer rows each run the union. Synthetic repro (1.92M outer rows,
  265 + 100 parent keys): 5.0 s; the same check against one table: an anti
  join, milliseconds.
- **MERGE runs row at a time** (`exec/dml.rs` `merge`, wave 95b):
  - `snapshot(t)` copies the whole target into boxed `Value` rows before
    matching;
  - `DmlIndex::probe` (`exec/mod.rs`) evaluates the target-side key
    expressions over the *whole target chunk* once per target row, so every
    target row costs a chunk's worth of key evaluation: O(rows × 2048 ×
    keys). This is the 12 s one-row MERGE. `UPDATE … FROM` and
    `DELETE … USING` share it;
  - every candidate pair builds a one-row joined chunk to evaluate `ON`, the
    clause conditions and the `SET` expressions;
  - `apply` writes one `update_rows` / `insert_rows` call per row, each
    sizing a per-chunk vector over the whole table.
- **CTAS and `INSERT … SELECT` stage rows as `Vec<Value>`** and append them
  one `append_row` at a time (per-row coercion, statistics, zone and version
  bump). 1.92M × 18 takes 3.7 s (CTAS) and 4.0 s (`INSERT … SELECT`) on the
  synthetic table; `append_chunk`, which the CSV and lake loaders use,
  shares whole vectors.

## Decisions

1. **An uncorrelated subtree is a leaf of the dependent join** (DuckDB:
   `FlattenDependentJoins::PushDownCorrelatedNodeInternal` attaches the
   domain to any subtree that `RequiresDomain` says is independent, and
   pushes the domain through set operations, projections, filters and
   aggregates; `src/planner/subquery/flatten_dependent_join.cpp`). pinkdb
   keeps its binder-level rewrite and gets the same reach by *pulling the
   correlation up* instead of pushing the domain down: a new extraction
   replaces `strip_from_filters` + `is_from_tree` and returns the body's
   base with every correlated conjunct lifted to the base's output space.
   - `Filter`: correlated conjuncts are lifted; uncorrelated ones stay.
   - Inner/cross `Join` / `NestedLoopJoin`: both sides, offset as today.
   - `Project`: lifted conjuncts are rewritten over the projection's
     output (a column the projection passes through, or a subexpression
     equal to one of its expressions); anything else fails.
   - `Distinct`, and `SetOp` `UNION` / `UNION ALL`: lifted from every
     branch; the branches must lift the same conjunct set in output space
     (pushdown cloned them from one predicate), else fail. A filter on
     output columns commutes with the union and the duplicate elimination.
   - Grouped `Aggregate`: only conjuncts over group-key columns lift,
     rewritten to the key's output column.
   - Any other node, or any node reached with correlation still inside
     (limit, window, sort, `EXCEPT`/`INTERSECT`, a correlated nested
     subquery), fails: the per-row path stays the fallback.
   - Any subtree with no correlation is a leaf, whatever its shape.
   The optimizer is not changed, so fallback shapes keep today's plans.
2. **MERGE becomes a vectorized hash match with batched actions**
   (DuckDB plans MERGE as a join of source and target and routes the joined
   chunks to per-action sinks; `src/planner/binder/statement/bind_merge_into.cpp`,
   `src/execution/operator/persistent/physical_merge_into.cpp`). In
   `exec/dml.rs`, without a planner change:
   - the source hash table stays (`DmlIndex`); the target-side keys are
     evaluated once per target chunk and probed as a vector;
   - candidate pairs are gathered into joined `[source…, target…]` chunks
     of up to `CHUNK_SIZE` rows and `ON`, each clause's condition and the
     `SET` / `INSERT` expressions are evaluated once per batch;
   - with no `WHEN NOT MATCHED BY SOURCE` clause, target chunks whose zone
     maps cannot hold any source key are skipped, using the same
     `JoinFilter` min/max a hash join derives from its build side;
   - no whole-table snapshot: only touched rows' old images are read, and
     the key space is the table's cached one (`keys::take_or_build`);
   - updates go to `update_rows` once and inserts to `insert_rows` once,
     updates before inserts as today, so triggers see the same order;
   - the duplicate-match error, clause order (first clause in written order
     wins), the atomicity rule and `RETURNING` are unchanged.
   `UPDATE … FROM` and `DELETE … USING` get the per-chunk key evaluation.
3. **CTAS and `INSERT … SELECT` append chunks.** CTAS's schema is the
   query's, so its chunks append as they are (`append_chunk`, re-chunked to
   `CHUNK_SIZE`). `INSERT … SELECT` casts each source vector to its target
   column type once per chunk and fills unlisted columns with their default,
   then appends chunks whenever the row path adds nothing: no `ON CONFLICT`,
   no row triggers, no capture (`RETURNING`, transition tables), no declared
   keys. Otherwise the row path stays. A cast failure still fails the
   statement before the first append.

## Measurements

Synthetic repros (`pinkdb-wt/bench-c/scratch`, release CLI, in-memory,
1.92M-row fact), before (236c4f2) and after (525e1e6):

| statement | before | after |
|---|---|---|
| `NOT EXISTS` over a `UNION` of 265 + 100 keys (140a) | 4.99 s | 0.007 s |
| `MERGE`, 1 source row, 7-key `ON`, 9 columns (140b) | 6.64 s | 0.09 s |
| `MERGE`, 960k matched source rows (140b) | 45.2 s | 0.67 s |
| `MERGE` of 1.92M rows into an empty table, all inserts (140d) | 1.05 s after 140b | 0.24 s |
| CTAS `SELECT *`, 18 columns (140c) | 1.45 s | 0.06 s |
| `INSERT … SELECT *`, 18 columns (140c) | 1.51 s | 0.06 s |

QuackLab benchmark (`bench/quacklab/run.py --size small --storage local
--clients 1 --rounds 1`, stream B's harness at ae486e1 with this branch's
binaries; load average 4–8 from the other streams, so DuckDB's own numbers
move between runs). Results in `pinkdb-wt/bench-c/results/`.

| phase | before (236c4f2) | after (525e1e6) | DuckDB (same run) |
|---|---|---|---|
| ETL 2024-07 (1.87M rows) | 68.3 s | **1.81 s** | 2.20 s |
| ETL 2024-08-12 | 43.3 s | **1.49 s** | 0.34 s |
| ETL one-row correction | 14.8 s | **1.35 s** | 0.09 s |
| mart refresh 2024-07 | 23.5 s | 9.1 s | 2.5 s |
| mart refresh 2024-08-12 | 17.2 s | 8.7 s | 2.4 s |
| peak RSS, ETL 2024-07 | 4.45 GB | 2.11 GB | 0.78 GB |

Table counts and widget rows agree with DuckDB before and after. What is
left of the ETL is mostly process overhead (open the store, `.save`:
0.6–1.1 s against DuckDB's 0.04–0.28 s, wave 136's lazy storage), the
correction batch's `MERGE` (0.20 s; its keys are VARCHAR/DATE columns whose
zones do not prune the unsorted fact table) and the staging `DELETE`
(0.15 s).

TPC-H SF 1, interleaved A/B of 236c4f2 against 140c (5 rounds, load ~10,
`pinkdb-wt/tools/tpch-ab.py`): geometric mean 0.97; the two outliers were
noise (a 9-round rerun: Q2 1.00, Q20 1.03). TPC-H's correlated bodies are
plain scan and join trees, so their plans do not change. 22/22 match DuckDB
at SF 0.01 and SF 1.

## Landed

- **140a** (c2171dd) decision 1; the review's differential run caught a
  correlated `HAVING` that the first lift broke and a missing guard for
  projections that read the enclosing row, both now refused (per-row path).
- **140b** (d6b7363) decision 2; the review caught joined batches that could
  pass `CHUNK_SIZE` (a "vector overflow" error), now split.
- **140c** (7dc6e79) decision 3, plus the pre-existing `RETURNING` /
  data-modifying CTE / transition-table failure past 2048 rows.
- **140d** (525e1e6): `MERGE`'s `NOT MATCHED` inserts take 140c's chunk path
  under the same conditions, with one `NOT MATCHED` clause (several keep
  their clause-major order on the row path).

## Follow-ups (not in this wave)

- `trigger::predicted_id` and `storage.rs` `extend_key_index` predict a new
  row's id as `num_rows() / CHUNK_SIZE`, wrong once a delete has left an
  interior chunk short (keyed tables). 140b's `MERGE` derives it from the
  tail chunk instead.
- `DELETE … RETURNING` (any capture) deletes one row per call in
  `trigger::delete_rows`: 1.92M rows take 34 s against 0.016 s without
  `RETURNING`.
- Store open and `.save` per process (0.6–1.1 s on QuackLab's store): wave
  136's lazy storage.
- Mart refresh is 3.7–4× DuckDB: wave 135's pipelines.

## Tests

Engine (`crates/pinkdb`): `NOT EXISTS` / `EXISTS` / `IN` / scalar bodies over
a `UNION`, `UNION ALL`, a derived table with a projection, a `DISTINCT`
and a grouped aggregate, each asserting both the rows and (through
`EXPLAIN`) that a join replaced the per-row path; shapes that must keep the
fallback (a correlated `LIMIT`, `EXCEPT`) still answer correctly; NULL keys
on both sides of the anti join. MERGE: the 7-key shape, every clause kind,
a clause condition that differs per row, a duplicate match error after a
partial batch, `RETURNING`, a keyed target, a trigger on the target, `NOT
MATCHED BY SOURCE` with zone-skipping off, and a multi-chunk target (more
than 2048 rows) where only some chunks match. CTAS / `INSERT … SELECT`:
multi-chunk sources, a column list with defaults, casts that fail midway
(table untouched), keyed and triggered targets take the row path.
duckdiff cases for the new decorrelated shapes and MERGE. Targeted gate
(engine-only wave): fmt, changelog, clippy default + stable, workspace
tests, wasm, TPC-H SF 0.01 and SF 1 vs DuckDB.
