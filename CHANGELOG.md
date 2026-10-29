# Changelog

pinkdb is developed in numbered waves: each wave ships one focused capability
(or a performance pass) with its own tests and CI gates. This file is the
user-facing summary — one section per wave, newest first — while the
[ROADMAP progress log](ROADMAP.md#progress-log) carries the full engineering
detail (probes, gate counts, integration notes). Entries are appended at
integration time starting with wave 63; earlier waves are backfilled as
one-liners below.

## 2026-10-29
### Wave 123h — `IN` subqueries skip ordering their groups ✅

- A `GROUP BY` inside an `IN (…)` subquery is only used as a set of
  values, so it no longer sorts its groups back into first-seen order
  (unless a `LIMIT` or window inside the subquery depends on that order).
  TPC-H SF 1: **Q18 −15 %**.

### Wave 127h — text filters decide each stored string once ✅

- A filter comparing a text column with a constant (`l_returnflag = 'R'`,
  `p_brand <> 'Brand#45'`) remembers its answer per stored string, so rows
  sharing one dictionary-encoded string are not compared again. TPC-H SF 1:
  Q12 −7 %, Q7 −6 %, Q19 −6 %.

### Wave 127g — `CASE` a vector at a time ✅

- `CASE WHEN … THEN … ELSE … END` now tests each condition over the rows
  still undecided as a filter, takes constant results without evaluating
  them, and builds its output in one typed pass instead of one value per
  row. TPC-H SF 1: **Q12 −52 %**.

### Wave 123g — shared arithmetic in aggregate arguments computed once ✅

- When several aggregates of a `GROUP BY` share a calculation (TPC-H Q1
  sums `l_extendedprice * (1 - l_discount)` on its own and again times
  `(1 + l_tax)`), it is now computed once per chunk. TPC-H SF 1: Q1 −8 %.

### Wave 123f — low-cardinality text keys grouped by reference ✅

- Grouping by a text column with a handful of values (TPC-H Q1's flags)
  now recognizes rows that share one stored string — which the store's
  dictionary encoding gives them — without reading the string again.
  TPC-H SF 1: Q1 −9 % (CPU time −9 to −30 %).

### Wave 122d — join filters test a chunk at a time ✅

- The filter a join hands its probe side (the build keys' range and
  bloom filter, wave 122) is now applied to a whole chunk of integer keys
  in one tight loop instead of row by row. TPC-H SF 1: Q8 −18 %,
  Q17 −17 %, Q2 −14 %.

### Wave 111e — join order follows inferred keys; joins hash their smaller input ✅

- `a.k = b.k AND c.k = b.k` now also lets the join order join `a` with `c`
  directly (DuckDB infers these the same way): TPC-H Q9 joins `part` with
  `partsupp` before touching `lineitem`, and Q5 joins `customer` with
  `supplier` on their shared nation.
- A join whose input is itself a join now has a size estimate, so the
  engine hashes the smaller side there too instead of always the right one.
- TPC-H SF 1: **Q5 −61 %**, **Q8 −54 %**, **Q3 −38 %**, **Q18 −34 %**,
  Q9 −27 %, Q7 −28 %, Q21 −23 %, Q2 −12 %; Q10 +26 % (now hashes its
  small, wide side — follow-up). All 22: 789 → 627 ms.

### Wave 125c — joins on two integer columns use one packed key ✅

- A join on two integer columns (TPC-H Q9's `l_partkey = ps_partkey AND
  l_suppkey = ps_suppkey`) packs both values into one 64-bit key when they
  fit, and joins like a single-column integer join instead of encoding
  both values per row. TPC-H SF 1: Q9 −12 %.

### Wave 123e — GROUP BY on text keys in flat tables ✅

- Text group keys (`GROUP BY n_name`, `c_name`, …) now use the flat group
  tables of wave 123d too: a group keeps a shared reference to its string
  instead of a copy, and a chunk whose keys hold only a few values (TPC-H
  Q1's two flags) is grouped through a small dense table. TPC-H SF 1:
  **Q10 −27 %**, Q9 −5 %.

### Wave 125b — hash join probes load their buckets in one pass ✅

- A join on an integer key now hashes a whole chunk of probe keys and
  loads their buckets before walking any of them, so the memory reads of
  different rows overlap instead of waiting one after another. TPC-H SF 1:
  Q13 −16 %, Q18 −8 %, Q21 −8 %.

### Wave 127f — `IN` lists evaluate their operand once ✅

- A filter like `substring(c_phone, 1, 2) IN ('13', '31', …)` computed the
  substring once per listed value; it is now computed once per row and
  looked up in the list. A filter that only needs a few columns of a row
  also stops copying the others. TPC-H SF 1: **Q22 −72 %**, Q19 −9 %,
  Q16 −6 %.

### Wave 125a — `EXISTS` hashes the smaller side ✅

- An `EXISTS` / `NOT EXISTS` join whose outer side is smaller than the
  subquery's now hashes the outer rows and lets the subquery's rows mark
  them, as DuckDB's `RIGHT_SEMI` join does: TPC-H Q4 hashes 57 thousand
  orders instead of 3.8 million `lineitem` rows. An extra non-equality
  condition (Q21's `l2.l_suppkey <> l1.l_suppkey`) is checked on every
  matching pair. TPC-H SF 1: **Q4 −68 %**, **Q21 −38 %**, Q22 −9 %.

### Wave 127e — `column < column` filters compare in place ✅

- A filter comparing two columns of the same type (TPC-H Q4's
  `l_commitdate < l_receiptdate`) compares the rows a filter still keeps
  directly, instead of first building a TRUE/FALSE column over a copy of
  them. TPC-H SF 1: Q12 −17 %, Q4 −12 %.

### Wave 123d — GROUP BY on integer keys without per-group allocations ✅

- A `GROUP BY` whose keys are all fixed-width numbers (INTEGER, BIGINT,
  DATE, TIMESTAMP, BOOLEAN, DECIMAL up to 18 digits) and whose aggregates
  are `COUNT`, `SUM`, `AVG`, `MIN` or `MAX` now keeps its groups in flat
  arrays: keys as machine words, accumulators side by side, each chunk's
  groups already laid out by merge partition. Before, every group of every
  chunk allocated its own key and accumulator lists. Results and their
  order are unchanged.
- `GROUP BY l_orderkey` over TPC-H SF 1's 6 million `lineitem` rows: 3×
  faster. TPC-H SF 1: **Q18 −58 %**, Q15 −38 %, Q11 −20 %, Q13 −12 %.

### Wave 127d — filters compare in the column's own type ✅

- A filter like `l_discount BETWEEN 0.05 AND 0.07` or `l_quantity < 24`
  compares each row as one integer comparison: the literal is converted to
  the column's type once, not each row to the literal's. A DECIMAL column
  against a DOUBLE literal keeps exactly the rows it kept before.
- TPC-H SF 1: **Q6 −42 %**, Q19 −24 %, Q14 −20 %, Q20 −14 %.

## 2026-10-28
### Wave 123c (second part) — hash tables no longer collide on encoded keys ✅

- The engine's fast hash (FNV, used by every grouping, `DISTINCT` and
  `IN`-set table) now ends with a mixing step. Without it, every encoded
  integer group key produced the same low 8 bits — the bits the hash table
  picks buckets from — so a table of a million groups degenerated into long
  probe chains. TPC-H SF 1: **Q18 −20 %** (its 1.5-million-group merge);
  the rest unchanged.

### Wave 111d — join order reaches chains under nested `EXISTS` ✅

- The cost-based join order (wave 111a) now also reorders a comma-join chain
  that sits under several decorrelated joins, such as TPC-H Q21's
  `EXISTS ... AND NOT EXISTS ...` (an anti join over a semi join over the
  chain). Q21 now joins `nation = 'SAUDI ARABIA'` to `supplier` first
  instead of last. TPC-H SF 1: **Q21 −30 %**.

### Fix — join residuals over more candidate pairs than a chunk holds ✅

- A join with a residual condition (an `ON` or decorrelated `EXISTS` with a
  non-equality, like TPC-H Q21's `l2.l_suppkey <> l1.l_suppkey`) failed with
  "vector overflow: len 2048 >= CHUNK_SIZE 2048" when one probe chunk had
  more than 2,048 candidate pairs. The residual is now tested a chunk's
  worth of pairs at a time. (Present since wave 111c; TPC-H's plans
  happened not to reach it.)

### Wave 127c — filter conjuncts run cheapest first ✅

- A filter's conditions are ordered by estimated cost (DuckDB's expression
  heuristics): integer and date comparisons run before string comparisons,
  `LIKE` and `IN` lists, which then only see the rows that survive. The
  order only changes when no condition can raise an error, so a guard like
  `v <> 'x' AND CAST(v AS INTEGER) > 6` keeps its written order.
- **Fix:** an `IN` list was evaluated twice when the optimizer derived it
  again from an `OR` (TPC-H Q12 compared every row's `l_shipmode` four
  times). `false OR x` and `true AND x` now fold to `x`, and a derived
  condition equal to one already present is dropped.
- TPC-H SF 1: **Q12 −34 %**.
### Wave 123c (first part) — faster `GROUP BY`: parallel merge and emit, small key domains, bulk decimal sums ✅

- A `GROUP BY` with many groups no longer ends in a single-threaded tail.
  The output chunks are finalized and built in parallel. A large merge also
  sorts its group order in parallel and emits each chunk straight from the
  merge partitions, instead of moving every group into one list first.
- The merge's scatter sorts each chunk's groups by partition once instead
  of growing a list per partition per chunk (about 190,000 small lists for
  Q18), and orders groups by their exact first-seen position.
- A `GROUP BY` over 2–3 columns with few distinct values per chunk (Q1's
  `l_returnflag, l_linestatus`) codes each column with a short scan and
  indexes a dense table, instead of encoding and hashing every row's key.
- `SUM`/`AVG` of a `DECIMAL` of up to 18 digits add each chunk's values per
  group into one local sum, and hand every accumulator one total.
- **Fix (DuckDB parity):** a `DECIMAL` `AVG` divided its exact sum by
  `count × 10^scale` in one step; DuckDB divides the sum, as a double, by the
  count (its optimizer rewrites `avg` to `sum / count`). The last digit of
  some averages now matches.
- TPC-H SF 1: Q18 (1.5 million groups in its `IN` subquery) **−45 %**
  (−29 % parallel emit, −13 % scatter, −12 % bulk sums); **Q1 −11 %**; Q13,
  Q3 and Q16 −6 to −9 %.

### Wave 126d — `LIKE` segment search without per-call setup ✅

- A `%`-pattern segment is now found by scanning for its rarest byte and
  comparing the segment in place, instead of setting up a substring searcher
  for every segment of every row. TPC-H SF 1: Q20 −13 %, Q13 −7 %, Q16 −5 %.

### Wave 125 — hash joins: flat hash table, vector-at-a-time assembly ✅

- **A new join hash table, after DuckDB's `JoinHashTable`:** build keys and
  their 64-bit hashes are computed in parallel per chunk into flat per-row
  arrays, and a power-of-two bucket array chains the rows. A probe compares
  the full hash before the key, so other keys in its bucket cost no key
  compare. It replaces a single-threaded `HashMap` with an allocation per
  distinct key. Matches still come out in build input order, so results and
  their order are unchanged.
- **Join output is assembled a vector at a time:** the probe collects
  matched (probe row, build row) pairs for a chunk, then builds each output
  column with one typed gather, instead of copying every column of every
  matched row one value at a time.
- **Cheaper everywhere:** `Vector::take`/`gather` copy by type instead of
  row by row (filters, sorts and every gather use them), memory accounting
  reads string lengths instead of building a `Value` per cell, and the join
  filter's range check (wave 122) compares payloads.
- TPC-H SF 1 (A/B against 126c, alternating, median of 5 rounds, rechecks
  at 10): all 22 **3.0 → 1.9 s (−37 %)**; Q9 −70 %, Q17 −63 %, Q21 −51 %,
  Q12 −47 %, Q2 −46 %, Q11 −46 %, Q7 −41 %, Q22 −39 %, Q5 −37 %, Q3 −36 %,
  Q4 −32 %, Q8 −31 %; no query slower. Results match DuckDB at SF 0.01 and
  SF 1.

### Wave 126c — `LIKE` compiled once, no per-row allocation ✅

- **Faster `LIKE`/`NOT LIKE`:** a constant pattern is compiled once per
  chunk. A pattern of only `%` and literal text (`'%special%requests%'`) is
  matched the way DuckDB's `LikeMatcher` does it: prefix, suffix, and the
  segments between found in order with a substring search. Other patterns
  (`_`, `ESCAPE`) use a wildcard matcher that walks the string in place.
  Before, every row copied both strings and collected their characters.
  TPC-H SF 1: **Q13 250 → 108 ms (−57 %)**, Q20 −34 %, Q14 −15 %, Q16 −11 %,
  Q9 −8 %.
- **Fix (DuckDB parity):** a bare `NULL` in a comparison or `LIKE` takes the
  other side's type, so `NULL LIKE 'a%'`, `'a' = NULL` and
  `NULL < DATE '2020-01-01'` are `NULL` instead of a type error.

### Wave 126b — `substr` with DuckDB's semantics, vectorized ✅

- **Behaviour change (DuckDB parity):** `substr`/`substring` now follow
  DuckDB for every offset and length. A start of 0 begins one character
  before the first, a negative start counts from the end, and a negative
  length reaches backwards: `substr('hello', 0, 2)` is `h`,
  `substr('hello', -2)` is `lo`, `substr('hello', -3, 2)` is `ll` and
  `substr('hello', 2, -1)` is `h` (they were `he`, `hello`, `he` and an empty
  string). Characters are code points.
- **`substring` works as a name, and the SQL forms parse:**
  `substring(s FROM 2 FOR 3)`, `substring(s FROM 2)` and
  `substring(s FOR 2)`, as in DuckDB (TPC-H Q22 uses them).
- **Start and length must be integers**, as in DuckDB
  (`substr('hello', 1.5)` is an error), and values outside ±2³² raise
  DuckDB's range error. A `NULL` argument gives `NULL`.
- **Faster:** a column kernel slices each string in place and returns the
  input string itself when the result is the whole of it, instead of copying
  the string and collecting its characters per row. TPC-H Q22 at SF 1:
  182 → 70 ms (−62 %).

### Wave 126 — faster decimal arithmetic, DuckDB's multiply typing, exact AVG ✅

- **Decimal `+`, `-` and `*` skip the per-row overflow check when a chunk
  can't overflow.** One pass over each operand finds its largest magnitude;
  when the result is provably in range, the loop runs unchecked, and a product
  of operands that fit 64 bits uses a 64-bit multiply instead of a 128-bit
  one. A chunk that might overflow takes the checked loop, so errors are
  unchanged.
- **Behaviour change (DuckDB parity): multiplying two decimals of at most 18
  digits is typed `DECIMAL(18, s1 + s2)`**, as in DuckDB, instead of widening
  to `DECIMAL(p1 + p2)`. `DECIMAL(15,2) * DECIMAL(15,2)` is now
  `DECIMAL(18,4)`, and a product past 18 digits raises DuckDB's error word for
  word ("Overflow in multiplication of DECIMAL(18) (… * …). You might want to
  add an explicit cast to a bigger decimal."). Inputs wider than 18 digits
  still widen, capped at 38.
- **`AVG` is exact, like DuckDB:** integer and decimal averages keep an exact
  128-bit sum and divide once at the end, so `AVG` of large `BIGINT`s no
  longer loses digits to a running `DOUBLE`.
- **Fix:** an aggregate reading through a filter (wave 123a) evaluated its
  arguments over the whole chunk, so a row the `WHERE` clause dropped could
  still raise an error, such as a decimal overflow. It now retries over the
  kept rows only, and the error stands only if one of them raises it, as in
  DuckDB.
- Measured at TPC-H SF 1 (two release binaries, alternating, median of 5
  rounds): **Q1 65 → 48 ms (−26 %)**; the other 21 queries are within noise
  (all 22: 2,065 → 2,047 ms). Q22 measures +4.7 % over 10 rounds; its time is
  in `substr`, which allocates per row and makes the query sensitive to code
  layout (follow-up 126b).

### Wave 127b — projections evaluate only the outputs that are read ✅

- A `Project` whose consumer reads only some of its outputs now evaluates
  only those expressions; the rest are zero-length placeholders, and only the
  read expressions bound the columns its input must produce. The restore
  projection a join reorder adds (which lists every column) no longer pins
  the whole join chain, so wave 127's pruning reaches Q7/Q9-shaped plans.
- **Behaviour change (DuckDB parity):** a projected expression no consumer
  reads is no longer evaluated, so it can no longer raise an error. DuckDB
  removes such columns before execution:
  `SELECT region, COUNT(*) FROM (SELECT region, CAST(x AS INTEGER) AS junk
  FROM t) s GROUP BY region` now answers, as it does in DuckDB, instead of
  failing on the unused cast.

### Wave 127 — joins carry only the columns their consumers read ✅

Column pruning used to stop at every join: a table under a join produced every
column, and the join copied every column of both sides into its output, one
value at a time. Now the column analysis runs through hash joins.

- **Each join knows which of its output columns are read.** The bound splits
  across the two sides, plus each side's own keys and residual columns;
  semi/anti/mark joins need only key and residual columns from the right.
- **The probe assembles only those columns**; every other output column is a
  shared zero-length placeholder (the same mechanism scans already used), so
  nothing is copied for it. `SELECT COUNT(*) FROM t a JOIN t b ON a.i = b.i`
  reads one column per side instead of all of them.
- **Fail-safe by construction:** a column the analysis leaves out is a hard
  internal error when read, never a NULL. Two gaps it surfaced are fixed:
  aggregate/window references (`AggRef`/`WindowRef`) now count as column
  reads, and semi/anti/mark joins carry placeholders through.
- **TPC-H SF 1** (vs main, same machine): total 7.2 s → 3.7 s; geometric mean
  vs DuckDB 14.1× → 8.0×. Q21 2,407 → 393 ms, Q5 412 → 108, Q2 185 → 53,
  Q14 99 → 34, Q3/Q4/Q12/Q13/Q20 −53 to −59%. Q1/Q6/Q7/Q9 unchanged
  (head-to-head A/B); Q7/Q9's reordered chains end in a restore projection
  that still references every column (follow-up). All 22 queries match
  DuckDB.

### Wave 111c — join residuals and a parallel aggregate merge ✅

The three TPC-H queries that were still 60–130× DuckDB at SF 1 fell to 10–75×,
and no query result changed: every answer is compared with DuckDB's rows at
three scales, and two new differential tests pin the new paths against a
hand-written reference.

- **A hash join can carry a residual predicate.** `LogicalPlan::Join` gained
  `residual`: a predicate on a *candidate key match*, evaluated in joined space
  on that pair alone, before anything is materialized. A pair that fails it is
  not a match — `INNER` drops the pair, `SEMI`/`ANTI` treat the pair as absent,
  so a preserved row can end up with zero matches. The executor gathers only
  the columns the residual reads for the candidate pairs, evaluates the
  predicate once vectorized over that dense chunk, and emits the surviving
  pairs (DuckDB's `physical_hash_join.cpp` design). With no residual the probe
  loop is the old one.
- **`EXISTS` with a non-equality correlation uses it.** A correlated `EXISTS`
  whose body has an equality key plus another comparison — Q21's
  `l2.l_suppkey <> l1.l_suppkey` — used to fall back to
  `body ⨯ Distinct(Project(outer))` with the comparison in a `Filter` above the
  join: the 36-column outer chain was cloned and deduplicated once per
  subquery, three times in Q21. It now keeps the plain shape and hands the
  comparison to the semi/anti join. Q21 SF 1 **2,333 → 1,352 ms**, SF 0.1
  232 → 114 ms, and its plan is one outer chain instead of three.
- **The aggregate merge is parallel.** After the per-chunk partial tables are
  built (already parallel), `merge_partials` hashed the keys into up to 64
  partitions, merged the partitions in parallel, and restored the exact
  first-seen group order with one `u32` sort — the order a serial walk would
  produce, so results are bit-identical. The first attempt did not scale at all
  (1.46 s, the same as serial): it masked the low bits of an FNV hash, which
  barely mix, so nearly every key landed in one partition. Q18 SF 1
  **1,390 → 251 ms** (merge alone 1,284 → 56 ms).
- **A duplicate-eliminated key side carries the enclosing filters.** The key
  side that a correlated `IN`/scalar subquery probes was built from an
  unfiltered scan plus a `Distinct`, and an enclosing conjunct containing a
  subquery was dropped from it ("it would run twice"). It is now carried — the
  subquery is uncorrelated and only filters rows the key side reads anyway — so
  Q20's key side is `Distinct(Filter(ps_partkey IN (…))(partsupp))` (11k keys
  instead of 800k). Q20 SF 1 **604 → 100 ms**, and its SF 0.1 → SF 1 scaling
  went from 15.1× to 10.0× for 10× the rows (the wave-120 superlinearity).
- **Join order may start from any leaf when the chain is small.** The dynamic
  program anchored the FROM clause's first relation, which made an
  intermediate blow up before the fact table arrived (Q11 built 800k rows from
  `partsupp ⋈ supplier` first). With at most four leaves any relation may
  start. Q11 SF 1 **129 → 26 ms**; five- and six-leaf chains keep the anchor
  (freeing it cost Q5 3.6× and Q9 2.6×).
- **Measured.** TPC-H SF 1 total 7,782 → 5,023 ms, geomean 166.5 → 128.1 ms
  (1.30×); SF 0.1 geomean 14.6 → 12.4 ms. Q18 5.5×, Q20 6.0×, Q11 5.0×, Q21
  1.7×. No query regressed beyond run-to-run noise. No new dependencies; the
  CLI binary grew 123 KB (+0.8 %).

```sql
-- one outer chain, two joins, no materialized key copies; same answer
SELECT s_name, count(*) AS numwait FROM supplier, lineitem l1, orders, nation
 WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F'
   AND l1.l_receiptdate > l1.l_commitdate AND EXISTS (SELECT 1 FROM lineitem l2
   WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
   AND NOT EXISTS (SELECT 1 FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey
   AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
   AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA'
 GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100;
```

- **`pink_lake::save_store_s3` takes an explicit catalog** (reviewer fix,
  found by a test flake): it used to put the lake's catalog under
  `$TMPDIR/pinkdb-s3-catalogs/<pid>/`, so the metadata lived in a temp
  directory and a recycled process id inherited another run's catalog. The
  catalog location is now an argument (`pink:`/`sqlite:`/`postgres://`); the
  temp default is gone, and the test helper keys its catalogs per run.

### Wave 123a — selection vectors and fixed-width expression kernels ✅

Filters no longer copy the rows that survive them, and comparisons and decimal
arithmetic stopped boxing each value. TPC-H Q1 went from 164 ms to 64 ms and Q6
from 80 ms to 13 ms at SF 1. No query result changed: every answer is checked
against DuckDB's rows at three scales, and a seeded fuzz compares the new
selection path against a row-at-a-time reference evaluator.

- **A filter produces a selection, not a copy.** `WHERE` evaluation returns the
  row indices that survive the chunk; comparisons and `AND`/`OR` build them
  directly, so a later conjunct only looks at rows the earlier ones kept, and a
  later `OR` term only at the rows the earlier ones did not take. An error in a
  conjunct (division by zero, decimal overflow) is raised only for rows that
  actually reach it — the same behaviour DuckDB's `Select` path has.
- **Aggregates read through the selection.** `GROUP BY` over a filtered scan
  folds the chunk through the surviving row indices instead of a compacted
  copy, and only copies when fewer than one row in eight survives.
- **Typed comparison and arithmetic kernels.** `DECIMAL` compared with a
  `DOUBLE`, an integer or a differently-scaled `DECIMAL` now runs in a tight
  typed loop instead of a `Value` per row (a `BETWEEN` on a decimal column went
  from 38 ms to 20 ms at SF 1). Decimal arithmetic borrows its operands, reuses
  the column's validity mask and writes directly into a pre-sized result.
- **No new dependencies**; the CLI binary grew 50 KB (+0.34 %).

```sql
-- both now run typed kernels end to end; results unchanged
SELECT l_returnflag, l_linestatus, sum(l_extendedprice * (1 - l_discount)) …
  FROM lineitem WHERE l_shipdate <= DATE '1998-08-15' GROUP BY 1, 2;
SELECT sum(l_extendedprice * l_discount) FROM lineitem
 WHERE l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;
```

### Wave 122 — statistics and join-filter pushdown ✅

The optimizer now knows what is in a table, and a hash join can tell the scan
under its probe side which rows cannot match. No query result changed; every
answer is checked against DuckDB's rows.

- **Column statistics live in the catalog.** Per column: exact row and NULL
  counts, min/max, and a hand-written HyperLogLog distinct estimate (4,096
  registers, 1.6 % standard error, no new dependency); per 2,048-row chunk: its
  own bounds and NULL/NaN counts. They are maintained on every write path —
  INSERT, UPDATE, DELETE, COPY/import, CTAS, lake inlining — not recomputed by
  rescanning: a delete only widens the bounds when the deleted rows held one
  (as DuckDB does), and the sketch becomes an upper bound, which
  `pinkdb_column_stats()` reports as `stats_approximate`. `ANALYZE [TABLE] t`
  recomputes everything exactly.
- **They persist.** The `.pink` footer (version 1 → 2) carries the sketches and
  the per-chunk zone maps, so `.open` restores statistics instead of hashing
  the data again.
- **You can see them.** `SELECT * FROM pinkdb_column_stats(['db.]table')`
  returns database, table, column, data type, row count, NULL count, distinct,
  min, max and `stats_approximate` — for local tables and, for lake tables, the
  per-file statistics DuckLake already keeps.
- **The cost model uses them.** Equality and `IN` selectivities come from the
  distinct estimate, ranges from the min/max spread, `IS [NOT] NULL` from the
  NULL count; equality-class domains take the smallest exact distinct. The
  fixed `0.2`/`0.5` defaults remain the fallback for columns without
  statistics, and the inner-join build-side choice now uses the same estimates.
- **Zone maps skip chunks.** A `.pink` scan drops a chunk whose `[min,max]`
  cannot satisfy the filter, exact at the boundaries and for NULL-only chunks;
  lake Parquet row-group skipping already existed and now shares the predicates.
- **Join filters.** An inner or semi hash join builds first, derives the build
  key's range and a blocked Bloom filter (2 probes, 1 % false positives, sized
  from the build key's distinct estimate), and hands them to the probe side
  before it runs: local scans skip chunks and test each row before reading its
  other columns, lake scans get the bounds as row-group predicates. Left/right/
  full joins (they owe every preserved row an output) and anti/mark joins (a
  false positive would drop a surviving row) are excluded. `SET
  join_filter_pushdown = false` turns it off for debugging.
- **Measured.** TPC-H SF 1 total 9.70 s → 9.14 s (−5.8 %), essentially all of it
  **Q21 3,831 → 3,196 ms (−16.6 %)**; the switch alone shows **−12.9 %** on
  that query. Q3/Q5/Q7/Q8/Q9/Q10 are flat (within ±7 %): their build sides are
  millions of rows, where a filter was measured slower, so `filter_pays` only
  filters a build of ≤ 10,000 rows. SF 0.1 flat (728 → 726 ms). CSV ingest pays
  12–15 % for the exact fold (lineitem 227 → 260 ms); Parquet and durable
  inserts are unchanged — the 5 % write-path bound is missed on CSV only, and
  row 122a is opened for it. A statistics-driven join-cardinality estimate was
  tried and reverted (Q8 −47 % but Q7 +41 %, Q21 +23 %).
- **Size.** Default CLI 5,608,752 → 5,690,064 B (+1.4 %), all-features
  14,842,064 → 14,942,048 B (+0.7 %), no new dependencies.
- **Tests.** New `crates/pinkdb/tests/stats.rs` (7): seeded random
  insert/update/delete against exact SQL recomputation, sketch error bound,
  persist/reload, `ANALYZE`, zone-map boundaries and NULL-only chunks, 330
  differential join cases with filters on versus off, joins against DuckDB, and
  the catalog function. Late materialization measured and deferred to row 122b.

## 2026-10-27
### Wave 111ab — query planning and execution tuning ✅

Two roadmap rows in one wave: cost-based join ordering (111a) and the
execution/subquery paths (111b). No SQL syntax was added and every TPC-H
answer is row-for-row DuckDB's.

- **The planner orders comma chains by cost now.** A query graph over the
  chain's equality edges, cardinalities from base row counts plus predicate
  selectivities (0.2 per equality or `IN` value capped at one, 0.5 per range;
  no histograms), a subset DP up to twelve relations and a greedy path above,
  anchored at the FROM clause's first relation. The restore projection keeps
  the output column order, and predicates that span the chain's boundary stay
  above it. Q7 SF 1 1.25 s → 246 ms.
- **`OR` predicates filter through.** A lone `OR`, or an `OR` of conjunctions
  with `IN` lists inside the branches, now derives per-column `IN`/range
  filters that reach the scans, with the *union* bound across branches (the
  old code kept the tighter bound, which is only valid for an intersection).
  Q19 SF 1 3.86 s → 112 ms.
- **Uncorrelated subqueries run once, and nothing waits.** The memo stores the
  `IN` set behind an `Arc`, and every operator fills it for the subqueries of
  an expression *before* its chunks fan out, so no thread holds or waits on a
  lock while a parallel evaluation is pending. That is the review fix for the
  first version's deadlock (a per-key flight lock held across the body's own
  evaluation, which starved the worker pool whenever the subquery sat outside
  a filter) and for wave 120's Q20 hang — 30 runs of the reviewer's repro, 0
  hangs. Q18 5.5 s → 1.45 s, Q20 4.9 s → 0.61 s, Q21 10.4 s → 2.25 s, Q22
  680 → 166 ms at SF 1.
- **Measured.** TPC-H SF 1 geometric mean **38.5× → 21.8×** DuckDB (30.3 s →
  8.0 s of pinkdb time); SF 0.1 **9.5× → 6.5×**. Q9 (57 → 60 ms at SF 0.1) is
  within the run-to-run spread and Q5 is 31 → 31 ms (it measured 35 ms mid-wave
  and recovered with the review fix); both are recorded rather than hidden. The
  default CLI grew 37.8 kB (+0.68 %) with no new dependencies.
- **What is left** is written down as follow-ups: parallelising the aggregate
  merge (Q18's remaining 1.4 s), a delim join sharing the decorrelated key
  side (Q21), selection-vector join output, perfect-hash aggregation, and
  Parquet decode throughput. All 22 TPC-H queries still match DuckDB's rows at
  SF 0.01 and 0.1; `bench/duckdiff.py` is green.

Design, defaults and the before/after tables are in
`docs/wave-111ab-plan.md` and `docs/benchmarks.md`.

### Wave 120 — correlated subqueries ✅

A subquery can now read the row around it, which is the last thing that kept
pinkdb from running the whole TPC-H set.

- **Any enclosing block, any depth.** `WHERE EXISTS (SELECT 1 FROM lineitem
  WHERE l_orderkey = orders.o_orderkey)`, `l_quantity < (SELECT 0.2 *
  avg(l_quantity) …)`, `x IN (…)` inside `CASE`, a correlated derived table, a
  correlation two levels deep: the binder resolves an outer name through a
  scope chain and remembers which enclosing columns each body reads.
- **Planned as joins, not rerun per row.** `EXISTS`/`NOT EXISTS` become
  semi/anti hash joins, `IN`/`NOT IN` become a **mark join** that computes SQL's
  three-valued answer (`1 NOT IN (2, NULL)` is NULL, `NOT IN (empty)` is TRUE),
  and a correlated scalar aggregate groups by the correlation keys and
  left-joins the block. `count` is coalesced to 0 for a group the body never
  produced — the classic COUNT bug — while `sum`/`avg`/`min`/`max` stay NULL,
  as DuckDB does.
- **`= ANY` / `<> ALL` and friends** are in the grammar now (`ALL` is
  `NOT(inv(op) ANY …)`, so it rides the same machinery).
- **What runs where.** The rewrites cover the shapes TPC-H and real queries use;
  the rest (a body with `LIMIT`/`GROUP BY`/`DISTINCT`, two-level correlation, a
  pure range correlation, a scalar under `NOT`) run once per distinct outer key,
  which is correct and linear, and `EXPLAIN` shows which one you got.
- **Measured.** All 22 TPC-H queries now match DuckDB's rows exactly at SF 0.01,
  0.1 and 1. At SF 0.1 the suite's geometric mean over all 22 is **9.3×**
  DuckDB (8.0× over the 16 queries wave 111 could run); Q4 16 ms, Q17 7 ms, Q20
  50 ms, Q21 407 ms, Q22 57 ms. The remaining gap for this family is the
  duplicate-eliminated key side being recomputed per subquery (DuckDB shares it
  with a delim join) — recorded as a follow-up, with Q20's scaling as the
  evidence.

- **Review round.** An `IN`/`NOT IN` whose operand column is not the column
  that drives the correlation read one column past the mark join's output; the
  empty-group rule (`x NOT IN (no rows)` is TRUE even for a NULL `x`) was also
  applied after the NULL checks instead of before. A correlated subquery in a
  grouped block now refuses an outer column the grouping does not carry, as
  DuckDB does, instead of reading an arbitrary group member, and a derived table
  inside a body that reads the enclosing row is counted in the body's
  references. A seeded differential test (`correlated_fuzz.rs`, 500 cases per
  run, soaks to thousands) compares generated cases against DuckDB, which is
  where the last two came from.

Design and the shape measurements are in `docs/wave-120-plan.md`.

## 2026-10-26
### Wave 109 — agent memory ✅

Long-term memory for agents, stored in pinkdb tables and served by a new native
extension (`pinkdb-memory`). A conversation becomes facts, facts become projects,
projects become a profile — and a later run starts from all three instead of
from nothing.

- **Four layers.** `memory_add` (or `.memory add …`) appends an **L0** turn;
  `.memory distill` turns undistilled turns into **L1** atoms ("the user's
  warehouse is TPC-H SF1", with a confidence, a tag and its sources),
  atoms into **L2** scenarios (a titled summary naming its atoms) and
  scenarios into an **L3** persona — one long-term profile per principal.
  Everything lives in one database, `pinkdb_memory`.
- **Distillation never blocks an add.** A model call happens only in
  `.memory distill` (or the `memory_distill` tool), through the shared
  `pinkdb-llm` client. Each answer is validated against a strict JSON contract
  before a single row is written; a malformed answer is recorded and retried
  later, never half-applied. No provider configured is one clear error — add and
  search keep working. `PINKDB_MEMORY_DISTILL_EVERY=N` queues a distillation
  every N turns.
- **Visibility is enforced on every read**, not by grants:
  `--visibility private` (the owner), `team` (the owner's role), `restricted`
  (the users, roles and agents in `--acl u:bob,r:ops`) or `agent` (the named
  agent identity). The memory tables are granted to nobody, so a client cannot
  `SELECT` around the filter; an ordinary user's `SELECT` fails with the
  engine's `permission denied` and the memory tools come back with only the
  rows that principal may see.
- **The internal channel is confined on both sides.** The channel the extension
  uses for its own state skips the RBAC gate, so the policy is what limits it:
  `ServePolicy::read_databases` is a read allow-list checked against the bound
  plan (a view, CTE or macro cannot reach around it; a table function or bare
  path is refused before it binds), and external services got their own
  `allow_external_services` switch instead of riding on `allow-files` (the
  operator's `allow-files` sets both, so nothing user-facing changed). Writing
  that test found a latent hole: `Connection::hint_store()` answers `None`, so
  *no* catalog-based policy check ran for a served session over a connection —
  `SELECT * FROM main.secret` went through a channel that named only
  `pinkdb_memory`. `Session::check_store` lends the store for the check and
  `deny_read_databases` is enforced on that path from now on.
- **Identity comes from the engine now.** `SELECT current_user()` and
  `SELECT current_roles()` answer the logged-in user and its (transitive) role
  names, comma-joined; an embedded store answers `pinkdb`. The extension asks
  the host on its own confined channel, so an MCP client cannot spoof who it is.
- **Layered, budgeted retrieval.** `memory_search` returns the persona, the
  best-matching scenarios, then atoms and turns ranked by **BM25 + RRF**, and
  in hybrid mode also by `embed()` + `array_distance` when the embeddings and
  vss packages are loaded — the result says which mode it used and why. Three
  budgets (items, characters, timeout) each stop it, and every hit carries its
  layer, id, score, source and visibility.
- **History, not deletion.** A contradicting fact supersedes the old atom
  (`superseded_by`) and `forget` marks a row `forgotten` with an audit entry;
  nothing is hard-deleted, so `show` still answers what you believed last week.
- **Surfaces.** `.memory add|search|update|forget|distill|persona|show|serve`
  next to `.agent`; six MCP tools (`memory_add`, `memory_search`,
  `memory_update`, `memory_forget`, `memory_distill`, `memory_persona`) on the
  memory extension's own bridge, which shares the new `pinkdb-mcp-protocol`
  crate with `pinkdb-mcp`. The write tools need `allow-memory`; read-only
  clients get the two read tools only. With the memory extension loaded, an
  `.agent` run records its question and answer as L0 turns.

```sh
cargo build --release -p pinkdb-cli -p pinkdb-memory

./target/release/pinkdb sales.pink
pinkdb> LOAD EXTENSION 'target/release/libpinkdb_memory.dylib';
pinkdb> .memory add my warehouse is TPC-H SF1 --tag tpch
pinkdb> .memory distill --provider zai      # needs that provider's key variable
pinkdb> .memory search how big is the warehouse
```

Follow-ups recorded on the roadmap: Skill assets (109b), an LLM-Wiki with a
link graph (109c) and a Code-Graph (109d), plus the agent's own `memory_search`
tool and the server's background distillation worker.

### Wave 110 — S3 catalog store (`catalog-s3`) ✅
- **`pink+s3://bucket/prefix` lake catalogs.** With the new off-by-default
  `catalog-s3` feature (forwarded by the CLI and server), the lake catalog
  database itself lives on object storage: no database server, nothing
  durable on local disk. Data defaults to `s3://bucket/prefix/data/`.
- **Delta-only commits.** Each commit uploads only its own records as
  immutable `prefix/catalog/log/<seq>` objects (one per record; still one
  request for a typical single-row commit); a full
  `prefix/catalog/checkpoint/<seq>.pink` image is uploaded every 100 records
  (`PINKDB_CATALOG_S3_CHECKPOINT_INTERVAL`),
  `prefix/catalog/head` records the newest one for operators, and readers
  find checkpoints by listing. Commit cost is O(delta), never O(catalog). Measured
  against the mock endpoint: a one-row commit is 3 S3 requests (2 LIST, 1
  conditional PUT) and 2.9 KB uploaded, and a cold reopen downloads one
  checkpoint image plus the logs written since. Writers race on conditional creates of
  the next log object; the loser reloads and retries through the existing
  conflict path, and an endpoint without conditional writes is read-only.
- **Encryption and maintenance.** A keyed lake seals checkpoint and log
  objects in `PINKENC1`; snapshot expiration (never a commit) prunes folded
  log objects and superseded checkpoints. Checkpoint images are cached
  locally by bucket/prefix/sequence.
- **Interop note.** DuckDB's DuckLake extension cannot attach a `pink+s3://`
  catalog; migrate by saving the loaded lake to a `pink:`/`sqlite:`/
  `postgres://` catalog (and back). Without the feature the location fails
  with a clear `FeatureMissing` naming `catalog-s3`.

### Wave 106b — several model providers for pinkdb-agent ✅
- **No default provider.** Every run names one: `.agent ask "…" provider
  <name> [model <name>]`, `pinkdb --agent FILE "q" --agent-provider <name>
  --agent-model <name>`, or `PINKDB_AGENT_PROVIDER`/`PINKDB_AGENT_MODEL` (a run
  argument beats the environment). With none, the run fails with
  `no provider: pass provider <name> or set PINKDB_AGENT_PROVIDER (one of: …)`
  and the agent never infers a provider from an exported key.
- **Eight built-in providers:** `zai` (`glm-5.3`), `anthropic`
  (`claude-sonnet-5-5`), `openai`, `deepseek`, `openrouter`, `gemini`,
  `ollama` (no auth header) and `custom` (`PINKDB_AGENT_BASE_URL`,
  `PINKDB_AGENT_PROTOCOL`, optional `PINKDB_AGENT_API_KEY`). Base URLs are
  roots, each overridden only by its own variable:
  `PINKDB_AGENT_ZAI_BASE_URL`, `PINKDB_AGENT_ANTHROPIC_BASE_URL`,
  `PINKDB_AGENT_OPENAI_BASE_URL`, `PINKDB_AGENT_DEEPSEEK_BASE_URL`,
  `PINKDB_AGENT_OPENROUTER_BASE_URL`, `PINKDB_AGENT_GEMINI_BASE_URL`,
  `PINKDB_AGENT_OLLAMA_BASE_URL`. `PINKDB_AGENT_BASE_URL` is `custom`'s alone;
  built-in providers ignore it, so a root exported for a custom endpoint never
  receives a built-in provider's key. A provider without a default model needs
  `model <name>`. `.agent providers` lists them with each override variable,
  the root in effect, and `key: set` / `key: missing` / `no key needed`, never
  a key.
- **Key isolation.** A key variable is only ever sent to its own provider
  (`ZAI_API_KEY`/`Z_API_KEY`, `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, …), in
  that protocol's header. This replaces wave 106's "`PINKDB_AGENT_API_KEY`
  then `Z_API_KEY`" rule: `PINKDB_AGENT_API_KEY` now belongs to `custom` only.
- **The Anthropic Messages API** as a second wire protocol: `x-api-key` and
  `anthropic-version: 2023-06-01`, the system prompt hoisted into `system`,
  `tool_use`/`tool_result` blocks paired by id with consecutive results merged
  and roles alternating, `input_schema` tools, `max_tokens` 8192
  (`PINKDB_AGENT_MAX_TOKENS`), no extended thinking, and a hand-parsed event
  stream (`message_start` … `message_stop`, `ping`, `error`). 529
  (overloaded) is retried like 429/5xx; 401/403 name the provider and its key
  variable. OpenAI-compatible reasoners' `reasoning_content` is dropped, never
  recorded or sent back.
- **Sessions record the provider.** `pinkdb_agent.sessions` gains `provider`
  and `usage` gains `provider`/`model` per step (schema changed outright; no
  migration). `.agent resume` keeps the recorded provider and model unless it
  names others; `.agent fork <s> <step> [provider …] [model …]` may switch
  provider — a z.ai session forked onto Anthropic is the same provider-neutral
  transcript re-encoded.
- **`extensions/llm` (`pinkdb-llm`)**, a new rlib holding the transcript, the
  registry, both protocols and the client, for the agent and wave 109's
  memory extension. Zero new crates in `Cargo.lock`; the release
  `libpinkdb_agent.dylib` grew 2,471,968 → 2,548,240 bytes (+76,272, +3.1%).

### pinkdb-agent: GLM-5.3 by default
- The agent's default model is now z.ai's `glm-5.3` (was `glm-4.6`);
  `PINKDB_AGENT_MODEL=glm-5.3-flash` picks the faster, cheaper variant. Both
  are reasoning models: their hidden reasoning tokens count against the token
  budget and are recorded in `usage` like any other completion tokens.

### Wave 107d — parallel HNSW build ✅
- **Parallel, still deterministic.** `CREATE INDEX … USING HNSW` and
  multi-row inserts into an indexed table now build in parallel
  (batched insertion in `pinkdb-vss`: parallel search against the graph
  frozen at each batch start, sequential in-order commit). The graph
  depends only on the data and the insert order — 1, 4 or 16 threads
  produce byte-identical graphs, so a replica or a WAL crash replay
  rebuilds exactly the original index. Small appends (under 32 rows)
  and single-row inserts keep the sequential path; graphs of at most
  1,024 vectors are unchanged from 107b. Threads come from rayon's
  global pool (`RAYON_NUM_THREADS` applies); no new crates.
- **Measured (Apple M4, release).** 100k × 768 build 111.5 s sequential
  → 32.1 s at 4 threads (3.5×), 30.4 s on all cores (3.7×), recall@10
  0.878 at ef_search 64 (sequential 0.874). 1M × 128 build 867 s →
  139.1 s (6.2×), recall 0.751 (sequential 0.740). Search latency is
  unchanged.

### Wave 107b — vector similarity search ✅
- **Distance functions (core SQL).** `array_distance` / `array_l2_distance`
  (Euclidean, not squared), `array_cosine_distance`,
  `array_cosine_similarity`, `array_inner_product` and
  `array_negative_inner_product` over fixed-length `FLOAT[N]`/`DOUBLE[N]`
  arrays. `DOUBLE`/mixed inputs compute in `f64`, `FLOAT`/`FLOAT` in `f32` and
  return `DOUBLE` (pinkdb has no scalar `FLOAT`; DuckDB returns `FLOAT`, and
  the widened values agree). A `NULL` array gives `NULL`; a `NULL` element or
  mismatched lengths is a clear error, as in DuckDB. The constant query vector
  is folded once at bind time and broadcast per chunk.
- **HNSW (`--features vss`).** `extensions/vss` (`pinkdb-vss`) is a linked
  extension package, off by default, with no new dependency. `CREATE INDEX …
  USING HNSW (v) WITH (metric = 'l2sq'|'cosine'|'ip', M = 16,
  ef_construction = 128, ef_search = 64, M0 = 2*M)` builds a deterministic
  graph: levels come from a seeded, serialized PRNG, so the same insert order
  (including crash replay) builds the same graph. The optimizer turns exactly
  `ORDER BY <matching distance>(col, constant) ASC LIMIT k` into a
  `VectorTopK` index scan shown by `EXPLAIN`; `DESC`, a second key, a `WHERE`,
  a metric/dimension mismatch or a different function fall back to an exact
  top-k heap. Results are approximate when the index is used; `SET VARIABLE
  hnsw_exact = true` forces exact and `SET VARIABLE hnsw_ef_search = N`
  widens the search.
- **Mutations and persistence.** Insert, update (delete + insert) and delete
  are applied to the graph; deletes are tombstones and `CALL
  hnsw_compact_index('table', 'index')` rebuilds without them. The graph is a
  `.pink` footer section (new flag `0x100`) written at checkpoint and loaded
  on open when the `vss` package is available; opening without it keeps the
  graph opaque and falls back to exact. WAL replay re-applies the statements.
  No experimental-persistence flag: the graph rides the ordinary checksummed,
  atomically replaced store section.
- **Limits.** Native tables only; lake tables, views, non-`ARRAY` columns and
  a second index on the same column are refused by name. `CREATE INDEX` and
  `DROP INDEX` use the existing index privilege, and a read-only MCP session
  can run top-k but not create or compact an index.
- **Measured (release, clustered Gaussian mixture, 200 queries).**
  100k × 768: build 94.5 s, graph 318 MB, recall@10 0.522/0.874/0.963 at
  ef_search 16/64/128, p50 0.53/0.86/1.13 ms versus 17.99 ms brute force.
  1M × 128: build 867 s, graph 617 MB, recall 0.414/0.740/0.862, p50
  0.26/0.49/0.76 ms versus 69.6 ms brute force. Exact-mode and the plain
  `ORDER BY` heap take 15.2 ms and 29.4 ms respectively. The default build's
  CLI is 5,304,688 bytes, `--features vss` 5,390,816 bytes (+1.6%); TPC-H Q1
  is unchanged and Q6 is 1.7% slower.

### Wave 107c — the `embeddings` extension: `embed(text)` → `FLOAT[N]` ✅
- **`extensions/embeddings` = `pinkdb-embeddings`.** `embed(text)` calls an
  OpenAI-compatible `POST {base_url}` endpoint and returns a `FLOAT[N]`
  vector, so a table gets its embedding column with `UPDATE docs SET v =
  embed(body)` and no client glue. The width is
  `PINKDB_EMBEDDINGS_DIMENSIONS` (default 1024): read when the package is
  published, sent as the request's `dimensions`, and a provider answering
  another width is an error that names the variable. **There is no default
  provider:** `PINKDB_EMBEDDINGS_BASE_URL` (any OpenAI-compatible `/embeddings`
  URL) and `PINKDB_EMBEDDINGS_MODEL` must both be set, or `embed()` errors
  before any request. (z.ai's global API, the live-test provider for the
  agent, answers "Unknown Model" for every embedding model; `embedding-3`
  lives only on Zhipu's `open.bigmodel.cn`.) The key is only
  `PINKDB_EMBEDDINGS_API_KEY`: another provider's key (`PINKDB_AGENT_API_KEY`,
  `Z_API_KEY`) is never sent to the embeddings endpoint. The key is environment-only (no
  `.env`, no `SET`, no store, WAL, log or `Debug`), a 401 is reported without
  the provider's body, and a sentinel-key test scans the store's files.
- **Batching, in the engine.** `HostScalar::batched` (a chunk-at-a-time host
  scalar; the engine's `eval_dynamic_scalar` calls it once per chunk and
  checks the row count and dtype) plus the extension's dedupe inside the
  chunk: 1000 rows with 300 distinct texts make 3 requests at the default 128
  inputs per request, `NULL`s make none, duplicates cost nothing.
- **Cache.** A 64 MiB in-process LRU keyed by (base URL, model, dimensions,
  SHA-256(text)), plus optional content-addressed files under
  `PINKDB_EMBEDDINGS_CACHE_DIR` written atomically, so a second query makes
  no request and the cache survives a restart. Nothing is written into the
  user's store from a `SELECT`.
- **Not WAL-volatile; a replay memo instead.** `embed()` is opaque to the
  planner (never constant-folded, never evaluated at bind or optimize,
  `WHERE false` makes no call) and is *not* declared volatile in the sense
  that refuses WAL-logged statements: that guard would refuse `UPDATE … SET v
  = embed(body)`, the function's whole use. Instead a logged statement's
  record carries a **replay memo** — every `external_io` host-scalar
  `(name, arguments) → result`, deduplicated, in a new trailing payload
  section (WAL format v5; older builds refuse the file with the usual version
  message, and no converter is kept). Replay answers from the memo and never
  invokes the function, so a crash-replay needs neither a key nor the
  provider and cannot diverge from the first run; a miss is a hard error
  naming the function. Price, measured: 1,000 rows at 1024 dimensions add
  4 133 005 bytes (4.0 KiB per distinct text) to the record — a wide
  backfill should be checkpointed.
- **Policy at the call site.** `HostScalar::external_io()` also rides in the
  executor: with `allow-files` off, a call is refused with the same
  `SERVICES_HINT` permission error *before any request*, however it is
  reached — a batch whose `SELECT` cannot bind at check time, a macro, a
  view, a trigger action, or a `CALL`/`DO` body. The bind-time check stays as
  the early, friendlier error, and `PinkDbError::Permission` keeps the kind
  for the client. Retries: 3 attempts with 250/500 ms backoff on 429/5xx,
  `PINKDB_EMBEDDINGS_TIMEOUT_MS` (default 30 s) per request.
- **Tests.** 19 engine tests against a recorded loopback provider (request
  shape, batching counts, cache across a process, dimension mismatch, NULL,
  429→success, 401, timeout, key order, missing key, EXPLAIN/`WHERE false`
  without calls, replay from the WAL memo with the provider stopped and the
  key unset, a two-statement transaction's memo, a provider that answers
  differently the second time, the execution-time policy for a batch, a
  macro, a view, a trigger and a `DO` block, the store/WAL sentinel scan, the
  policy both ways and an insert → `UPDATE` → brute-force top-k pipeline),
  4 CLI tests (the feature loads without `LOAD EXTENSION`; the persistent
  cache answers a second process; a replay answers offline from a fresh
  cache; the sentinel is absent from the store; the 1024-wide memo size) and
  one `#[ignore]`d live test gated on `PINKDB_EMBEDDINGS_LIVE=1`.
- **Cost.** Off by default; no new dependencies (ureq 3.4 + rustls and sha2
  were already in the lock file; the lock only gains the workspace crate).
  Release binaries: `pinkdb` 5 208 656 → 7 252 992 B (+2 044 336 B, +39.2%),
  `pinkdb-server` 4 821 232 → 6 845 456 B (+2 024 224 B, +42.0%).

### Wave 103c — errors name the real lake catalog ✅
- Every lake error that reports a catalog problem now names the catalog's
  actual location as it was given — `pink:/path/lake.pink`,
  `sqlite:/path/cat.db`, or the Postgres URL with its password redacted —
  instead of pointing at a JSON catalog file that has not existed since wave
  103. A lake opened from a bare path reports the resolved `pink:` URL, a
  SQLite catalog reports its URL form, and a Postgres catalog never echoes its
  password.
- `Lake::catalog_label()` exposes that location (computed once at open); the
  metadata store labels its catalog-row errors with the same redacted URL, and
  a SQLite catalog's I/O errors name the `.db` file.
- The deleted JSON layout's file name no longer appears in `crates/`, the
  README or the website: source, comments, tests and fixtures that still used
  it were rewritten or dropped, and the S3 tests now use a neutral object key.
- Tests: one per backend (pink, SQLite, PostgreSQL) removes the catalog's
  DuckLake metadata and asserts the failure names the catalog location and
  carries no stale file name; the PostgreSQL one also asserts the password is
  not echoed. No behaviour change beyond messages, no new dependencies.

## 2026-10-25
### Wave 107a — the fixed-size `ARRAY` type ✅
- **The type.** A column can now be `FLOAT[N]`/`REAL[N]`, `DOUBLE[N]`,
  `INTEGER[N]` or `BIGINT[N]`. `FLOAT[N]` is a real 4-byte element kind — an
  embedding column costs 4 bytes per element — and since pinkdb has no
  `FLOAT` *scalar* an element read widens to `DOUBLE` (the one difference from
  DuckDB, which returns a `FLOAT`); the `DOUBLE[N] → FLOAT[N]` cast follows
  DuckDB 2.0 (round to nearest, a finite value that would become infinite is
  an error, underflow is silent, text parses straight to f32).
  Values are stored columnar — one flat buffer of `rows × N` elements plus a
  row validity bitmap, and an element bitmap that exists only when some row has
  a `NULL` element — so a 768-d or 1536-d embedding costs no per-row
  allocation. `typeof` reports `DOUBLE[3]` and friends. `T[]` (a
  variable-length `LIST`) is a parse error that names the follow-up wave 107a2.
- **Literals and functions.** `[1.0, 2.0, 3.0]` and `array_value(…)` build an
  array per row from any expression (columns included) and infer the element
  type with the numeric tower; `[1, 2.5]` is a `DOUBLE[2]`. Element access
  `a[i]` and `array_extract(a, i)` are 1-based and forgiving (out of range,
  `NULL` index, `NULL` array and `NULL` element all give `NULL`); `len` and
  `array_length` report `N` and `NULL` for a `NULL` array. Casts run
  `VARCHAR ↔ ARRAY`, `ARRAY ↔ ARRAY` (same length, element type may change) and
  `ARRAY ↔ VARIANT`, plus `TRY_CAST`; a size mismatch is an error that says the
  size is part of the type.
- **Semantics.** Structural equality across element types, with `NULL`
  elements comparing structurally (two `NULL`s at one position are equal);
  element-wise lexicographic ordering with a `NULL` element after every value
  and a `NULL` array last, shared by `ORDER BY`, `MIN`/`MAX`,
  `GROUP BY`/`DISTINCT` and join keys through a canonical hash key that folds
  `-0.0`/`0.0` and every `NaN`.
- **Persistence.** `.pink` dtype tag 10 (element kind + length, in table and
  procedure schemas), default-value tag 10, and page-encoding tag 11: a
  `rows | layout | element bitmap | values` payload, row-major or
  byte-planed (whichever is smaller) behind the existing LZ frame, with no
  min/max statistics (only the null count). Save/open, WAL replay (the value
  literal is `CAST('[..]' AS DOUBLE[N])`) and checkpoint round-trip.
  Measured: 100k × 768 pseudo-random `DOUBLE` rows insert in 0.31 s, scan in
  0.31 s and occupy 585 MiB — 8 bytes per element, no overhead — while the
  same table as `FLOAT` occupies 292 MiB, exactly 4 bytes per element, and
  structured data still compresses (`sin` 108×, a constant column 253×).
- **Encodings.** Wire protocol v13 (value tag 11, dtype tag 10 with the element
  kind — 0–3, `FLOAT` appended — and length); C ABI `PINKDB_TYPE_ARRAY`
  (`N × 8` bytes of `f64`) and `PINKDB_TYPE_ARRAY_FLOAT` (`N × 4` bytes of
  `f32`), each with the element count packed into the type tag (`NULL`
  elements refused with a message); Node `Float32Array`/`Float64Array`,
  Python `list[float]`, Go `[]float32`/`[]float64`; extension ABI minor 5
  adds `ExtType::ArrayFloat` and `ExtType::ArrayDouble`, borrowed
  pointer-plus-length views of the engine's element buffer with no
  per-element boxing, which the `vss` and `embeddings` extensions use.
- **Parquet, lakes.** DuckDB 2.0's three-level `LIST` group both ways (DuckDB
  reads our files, we read its `FLOAT[3]`/`INTEGER[3]` files), with the length
  in a `pinkdb.array.<column>` key-value entry, an `f32` element leaf for
  `FLOAT[N]`, and a clear refusal for a variable-length list; `.pink` lake
  data files round-trip the column, and the DuckLake catalog records it as
  DuckDB records a nested type (a `list` parent row plus an `element` child
  row with `parent_column`, the real dtype in the `pinkdb_column` side row).
  Lake data files stamp `field_id = ducklake_column.column_id` on every
  top-level column and on the ARRAY child, which is what DuckDB's DuckLake
  reader matches nested fields by — so DuckDB reads exact vector values
  through a pinkdb lake, including after pinkdb added or dropped a column on
  the same table (the ids are planned before the commit that creates them,
  and a live column keeps its id). Standalone `COPY … TO 'x.parquet'` writes
  no ids, as DuckDB's own files do not.
- **Interop checks.** A DuckDB comparison test asserts both engines agree on
  literals, subscripts, out-of-range access, `len`/`array_length`, structural
  equality and `array_extract`.
- **Cost.** TPC-H Q1/Q6 over a 10M-row `lineitem` are unchanged (best of 13
  interleaved rounds against a binary built from `main`: 0.1228/0.0634 s
  before, 0.1225/0.0633 s after); a query that does not touch an `ARRAY`
  takes the same path as before. Release binaries: `pinkdb` 5 073 744 →
  5 129 344 bytes (+1.10%), `pinkdb-server` 4 686 080 → 4 761 344 (+1.61%).
  No new dependencies.

### Wave 108a — full-text search: a BM25 index with DuckDB-fts parity ✅
- **`extensions/fts` = `pinkdb-fts`.** DuckDB's `fts` surface over pinkdb
  tables: `CALL create_fts_index(input_table, input_id, *input_values,
  stemmer := 'porter', stopwords := 'english', ignore := '(\\.|[^a-z])+',
  strip_accents := 1, lower := 1, overwrite := 0)`, `CALL
  drop_fts_index(table)`, `match_bm25(index, id, query, fields := NULL,
  k := 1.2, b := 0.75, conjunctive := 0)`, `stem(word)` and
  `rrf(rank, k := 60)`. Argument names and defaults are DuckDB's; `PRAGMA`
  becomes `CALL`, and the index name is `match_bm25`'s first argument (a
  function cannot be schema-qualified).
- **The index is a database, not a sidecar.** `fts_main_<table>` holds
  `meta`, `docs`, `dict`, `terms`, `term_stats` and `field_stats`, written
  deterministically (terms sorted before ids, postings in
  `(term_id, doc_id, field_id)` order), so it saves with the store and a WAL
  replay rebuilds the same bytes. It is **static like DuckDB**: after a base
  table change `match_bm25` reads the old index until `overwrite := 1`
  re-creates it; incremental indexing is a follow-up.
- **Scoring is DuckDB's BM25, checked independently.** `idf = ln(1 + (N − df
  + 0.5)/(df + 0.5))`, per-field lengths and average lengths, deduplicated
  query terms, `fields` filtering both query and statistics, `conjunctive`
  requiring every term; no match scores `NULL`. A stdlib-only Python
  reference (`extensions/fts/tests/reference/bm25.py`) generates the fixture
  the Rust tests compare against to 1e-9, including custom `k`/`b`, `fields`
  and `conjunctive` top-k order.
- **Text processing.** DuckDB's default `ignore` pattern (a built-in
  scanner, no `regex` in a plain `fts` build; a custom pattern needs the
  `fts-regex` feature), optional lowercasing/accent folding (Latin table),
  DuckDB's `english` stopwords (the SMART system's 571 words), and Porter
  stemming. `stemmer := 'porter'` is libstemmer's Porter, i.e.
  Snowball's `porter.sbl`, not the 1980 paper — the hand-written stemmer
  (no dependency) matches a 2,994-word fixture derived from
  `/usr/share/dict/words` with an independent Snowball implementation, 0
  mismatches (the paper-following first version disagreed on 65).
- **One small engine hook, no new plan node.** `ExtensionPackage` gains an
  optional `HostScalar` (reads the store, e.g. `match_bm25`) and
  `HostStatement` (runs SQL and inserts rows, e.g. `create_fts_index`),
  bound through the existing `DynamicScalar` node and `LogicalPlan::Call`;
  `PinkDb::store_id()` keys the extension's in-process index cache,
  `Database::is_extension_loaded` guards the CLI/server startup load, and
  `CALL` now accepts `name := value` for host statements/scalars.
- **Off by default.** `fts` is a CLI and server cargo feature (not in the
  default set); the default binaries are unchanged. With it: `pinkdb`
  5,113,776 → 5,271,568 bytes (+3.1%), `pinkdb-server` 4,727,056 →
  4,885,552 (+3.4%). A custom `ignore` pattern (`fts-regex`, which enables
  `pinkdb-fts/custom-ignore` and the `regex` crate — already in the
  workspace graph) builds 6,614,512 and 6,229,568 bytes.
- **Read-only MCP.** `match_bm25` ranks under the read-only `query` tool;
  `create_fts_index`/`drop_fts_index` are `CALL` (global) statements and are
  refused there, tested end to end.
- **RBAC.** The server checks `match_bm25('docs', …)` as a read of `docs`
  (`SELECT` on the base table), so a session without grants cannot probe the
  indexed text through the function; the index name must be constant, so the
  check is static.
- **Tests.** 18 in `pinkdb-fts` (6 unit, 8 engine/flags, 2 reference, 2
  stemmer) plus an ignored 100k-document benchmark; 2 CLI end-to-end runs
  (fresh session, durable reopen) and 1 MCP read-only test with
  `--features fts`; workspace 2,065 passed / 0 failed / 11 ignored. 100k
  docs × 2 fields: load 0.80 s, index build 12.3 s, 2.82M postings,
  top-10 query 103 ms mean (259 ms slowest) over a full scan.
- **Pending.** A differential run against a real DuckDB `fts` install (not
  available offline) is a reviewer task; ingestion, `rag_search` and the MCP
  tools of row 108 remain queued.

### Wave 106 — `pinkdb-agent`: a data agent harness as an extension ✅
- **An agent that lives in the store.** `pinkdb-agent` is a native extension
  (`LOAD EXTENSION 'libpinkdb_agent…'`) that runs the loop itself: it asks an
  OpenAI-compatible endpoint what to do, executes the warehouse tools through
  the host, and keeps sessions, messages, tool calls and their results,
  checkpoints and token usage in the `pinkdb_agent` database. The engine, the
  CLI and the server still contain no model or HTTP client code; the release
  binaries grew only by the host-side ABI addition, 0.76% (`pinkdb`,
  5,035,312 → 5,073,504 bytes) and 0.03% (`pinkdb-server`, 4,668,016 →
  4,669,264 bytes), while the extension itself is 2,471,632 bytes.
- **Commands.** `.agent ask "<question>" [allow-writes] [allow-files]
  [max-steps N] [tokens N]`, `.agent resume`, `.agent sessions`,
  `.agent show`, `.agent fork <session> <step> ["<question>"]`, and
  `pinkdb --agent FILE "question"` (with `--agent-allow-writes` /
  `--agent-allow-files`) which loads the extension from the install
  directory like `--mcp`. The wave-99 `.agent rows|cols|cell|…` settings keep
  working when no run verb is given.
- **The key never lands anywhere.** `PINKDB_AGENT_API_KEY`, falling back to
  `Z_API_KEY`, read from the environment when a run starts (never from a
  `.env` file); `PINKDB_AGENT_BASE_URL` and `PINKDB_AGENT_MODEL` override the
  z.ai defaults. It is not in a table, the WAL, a log line, an error or
  `Debug` output, and a test greps the tables, the `.pink` bytes and stderr
  for a sentinel.
- **Two policies.** The model's calls run under the row-105 read-only policy
  unless the run says `allow-writes` (`allow-files` stays separate), and the
  agent's own tables are off-limits to it even then; the extension writes its
  transcript over an internal channel that may only write `pinkdb_agent`.
  Both directions are tested.
- **Resume and fork.** Every step commits a checkpoint. A killed run resumes
  from the last one, re-requests an interrupted stream, reuses recorded tool
  results and re-runs only replay-safe (read-only) tools. A fork copies the
  transcript up to a step into a new session and diverges; the original is
  untouched. Budgets: 20 steps by default, an optional token budget, and the
  wave-99 per-tool limits, all set per run and recorded when they stop it.
- **Provider.** One hand-parsed SSE `/chat/completions` client: streaming,
  tool calls with arguments split across chunks, usage, `finish_reason`,
  timeouts, three attempts with backoff on 429/5xx, and a 401 that says the
  key was rejected without echoing it. Blocking `ureq` with rustls, the same
  crate and features `pinkdb-httpfs` locks; no async runtime, no client crate,
  no new third-party crate in `Cargo.lock`.
- **Shared tools, ABI minor 4.** The eight warehouse tools moved to
  `extensions/warehouse-tools`, used by both `pinkdb-mcp` (behaviour
  unchanged) and `pinkdb-agent`. The ABI gains an optional
  `pinkdb_ext_command(api, host_ctx, args, sink, sink_ctx)` export and an
  `execute_internal` host field returning JSON rows; both are detected by
  symbol presence and `struct_size`, so v1, minor-2 and minor-3 extensions and
  the four SDKs load unchanged.
- **Tests.** 26 in `pinkdb-agent` (unit + a recorded mock provider over
  checked-in SSE fixtures), 13 + 1 ignored end-to-end CLI runs
  (`crates/pinkdb-cli/tests/agent_run.rs`) against a localhost mock: a single
  answer, a multi-step tool loop, split arguments, an interrupted stream, 429,
  401, kill-and-resume, fork, write refusals, the key sentinel, a budget stop
  and a TPC-H top-3 question; workspace 2,040 tests in 179 suites, all gates
  green.
- **Live check (reviewer, z.ai `glm-4.6`, default endpoint).** The first live
  run found that tools went out in their MCP shape; they are now wrapped as
  chat-completions function tools (`{"type":"function","function":{…}}`), and
  a unit test pins the shape. The opt-in live test then passed, and a manual
  run answered a revenue question in 3 tool steps (4,202 tokens); the key's
  bytes were absent from the store and its WAL.
- **Policy hardening (reviewer).** The model's write ban on `pinkdb_agent`
  also covers indirect writes under `allow-writes`: a data-modifying `WITH`
  body, a trigger whose action writes there (or runs `CALL`), and `CALL`/`DO`,
  which a confined channel now refuses outright. The extension's own channel
  is confined the same way in the other direction. Tests for both.

### Wave 101b — `VARIANT` in Parquet, shredding and path statistics ✅
- **Shredding.** A `VARIANT` column now splits into typed sub-columns: a
  `metadata` dictionary shared by the whole row group, a residual `value`
  buffer, and a `typed_value` group with one column per shredded object path.
  The policy is inferred per column over all of its rows — a key is shredded
  when it appears in at least 32 rows and in at least half of them, every
  occurrence carries the same primitive kind (BOOLEAN, BIGINT, DOUBLE,
  VARCHAR, DATE, TIMESTAMP), and at most eight keys qualify, most frequent
  first. Present-but-null keys and every other shape stay in the residual, so
  `{}` and `{"a": null}` remain distinct, and the reader reconciles both
  buffers exactly.
- **Parquet `VARIANT`.** `COPY … TO '….parquet'` and `read_parquet` no longer
  refuse `VARIANT`: the writer emits the specification's `Variant` logical
  type (specification version 1) and the reader accepts it, including a
  per-row metadata dictionary and partially shredded files. DuckDB 2.0 reads a
  pinkdb file and pinkdb reads a DuckDB file; the interop test runs both
  directions and is skipped when the DuckDB binary is absent.
- **Path statistics and pruning.** Every shredded path gets exact min/max and
  null-count statistics, keyed the way DuckDB spells a path (`"a"`,
  `"user"."id"`). So `WHERE payload['a'] = 5` or
  `variant_extract(payload, '$.a') = 5` skips files whose bounds exclude the
  value in `.pink` and Parquet lakes alike. A SQL catalog stores those facts in
  DuckLake's own `ducklake_file_variant_stats` table — one row per summed path
  per data file, written in the same catalog transaction as
  `ducklake_file_column_stats` and deleted with the file by `expire` and
  `cleanup`, on `pink:`, `sqlite:` and Postgres catalogs. DuckDB 2.0 attaches a
  pinkdb-written lake and reads the rows, and pinkdb prunes on rows DuckDB
  wrote; both directions are tested. Parquet row groups prune too, from the
  shredded leaves' own statistics, which are trusted only for files whose
  footer declares the shredding complete (`pinkdb.variant.complete`).
- **Comparisons coerce the scalar side.** `payload['a'] = 5` and
  `variant_extract(payload, '$.kind') = 'buy'` bind, with the scalar put on
  `VARIANT`'s side; booleans, numbers, decimals, dates and timestamps compare
  by value, a literal string becomes a variant string (not JSON), and a
  non-literal `VARCHAR` is still a bind error.
- **Extension ABI.** New `ExtType::Variant` (tag 10) carrying pinkdb's binary
  encoding on the text channel; the ABI minor version goes 2 → 3, v1 and v1.2
  extensions keep loading, and the header, Rust SDK and conformance fixture
  (`cf_variant`, all four SDKs) were updated.
- **Faster reassembly.** A leaf's records are stored flat with one offset per
  row, and its byte values in one pool per leaf, instead of a vector and a
  buffer per row; a full-value scan of a shredded Parquet column went from
  0.695 s to 0.525 s over 1M rows and its ratio against the unshredded layout
  from 1.40x to 1.18x.
- **Measured.** 1M rows of mixed shapes in ten files: Parquet 12.3 MB shredded
  against 14.4 MB unshredded (same data as JSON text: 12.0 MB), a selective
  path filter 0.027 s against 0.260 s unshredded, 0.020 s against 0.209 s in a
  `.pink` lake; a full scan of the column costs 1.18x the unshredded layout
  when read straight from the file and is at parity at lake level. TPC-H
  Q1/Q6 over 10M rows unchanged within noise (0.168/0.065 s against
  0.166/0.066 s).

### Wave 96b — a stateless server: metadata in the lake catalog ✅ (phase 1)
- **`pinkdb-server --catalog … --data …`.** A new durable mode beside `--file`:
  the engine catalog (databases, table definitions with constraints, defaults and
  indexes, views, macros, materialized views, procedures, triggers,
  users/roles/grants, loaded-extension sources) lives in a lake catalog database,
  and the rows live in lake data files. Restart the server — or start it on
  another machine — on the same two URLs and you get the identical store; the
  server process keeps nothing durable (no local WAL, no checkpoint, no replay).
  `--catalog` takes `pink:<path>`, `sqlite:<path>` and a postgres:// catalog (the
  `catalog-sqlite` / `catalog-postgres` features); `--data` takes a directory or
  `s3://bucket/prefix` (feature `lake-s3`). `--file` behaviour is unchanged. The
  mode is behind the server's `catalog` feature (the backend features imply
  it), so the default binary links no lake code and stays the same size
  (4.67 MB vs 4.65 MB on main, darwin-aarch64; linking `pink-lake`
  unconditionally had cost +1.9 MB).
- **Where the metadata goes.** The `ducklake_*` schema belongs to DuckDB, so the
  engine catalog is one row of `pinkdb_engine_catalog` beside it — an image of
  the engine's own catalog with the row pages omitted (`PINKCAT1`: the `.pink`
  footer image, plus views as their `CREATE VIEW` source text and the loaded
  extension sources), checksummed with crc32c. A password appears only as its
  Argon2id hash. Rationale and phase split: `docs/wave-96b-plan.md`.
- **One transaction per commit.** The image rides the same `CatalogChange` as
  the lake's own delta, so data files + lake delta + metadata publish together;
  a metadata-only statement (`CREATE VIEW`, `GRANT`, `CREATE ROLE`,
  `LOAD EXTENSION`) replaces the image without minting a lake version. A crash
  before the commit publishes nothing, one after it publishes everything.
- **One server per catalog (phase 1).** A lease row with a 2 s heartbeat makes a
  second server fail with `already served by …`; a crashed server's lease goes
  stale after 6 s and the next server takes over. Several servers on one catalog
  (fencing tokens, cache invalidation) are phase 2.
- **Views round-trip in this mode.** `ViewDef` gained the view's SQL text (a
  bound `Select` cannot be rendered back to SQL); `--file` still does not
  persist views.
- **Found, not fixed:** the documented wire bootstrap (`CREATE USER …` then
  `GRANT ADMIN …`) cannot work in any mode — the engine's per-statement login
  gate refuses the second statement once the first has armed it. Server tests
  build users through the embedded handle; recorded in the ROADMAP.
- Tests: the full round trip on `pink:` and `sqlite:` catalogs and on mock S3
  data (rows above the inlining limit, so they really come from the bucket),
  Postgres when `PINKDB_TEST_POSTGRES_URL` is set, RBAC after restart, the
  second-server refusal, a killed server reopening complete, Argon2id-only
  secrets. Gates: fmt, clippy, workspace tests (1,968 passed, 9 ignored),
  `pinkdb-server` 121 (126 with all catalog/data features), `pink-lake`
  `catalog-sqlite,s3,encryption` 317, wasm32 build.

### Wave 95c — SQL dialect completeness, batch 3 (query forms) ✅
- **LATERAL and comma-separated `FROM`.** `FROM a, b` is a cross join in the
  same left-deep list as `JOIN`, so `FROM a, b JOIN c ON a.x = c.z` binds like
  DuckDB. `LATERAL (SELECT …)` may sit in the head position, after a comma, or
  after any `JOIN` (including `CROSS JOIN LATERAL` and `LEFT JOIN LATERAL … ON
  true`). pinkdb binds derived plans without an outer scope, so a lateral
  subquery that references a column of an earlier `FROM` item is refused by
  name instead of silently returning the uncorrelated answer.
- **Ordered-set aggregates.** `percentile_cont`/`quantile_cont`,
  `percentile_disc`/`quantile_disc` and `mode` with `WITHIN GROUP (ORDER BY …)`,
  `FILTER` before or after the `WITHIN GROUP` clause, and the DuckDB result
  types (a `DECIMAL` percentile stays `DECIMAL`, an integer one becomes
  `DOUBLE`, `percentile_disc`/`mode` keep the input type, empty input is
  `NULL`, ties in `mode` pick the smallest value). A non-constant fraction, a
  missing `WITHIN GROUP`, or `OVER ()` on an ordered-set aggregate are named
  errors.
- **`TABLESAMPLE` / `USING SAMPLE`** with DuckDB's methods — `bernoulli`,
  `system`, `reservoir` — for `n ROWS` and `n PERCENT`, plus `REPEATABLE
  (seed)`. Sampling is deterministic under a seed (pinkdb's own SplitMix64
  stream, so the same seed picks different rows than DuckDB but the same rows
  every run); a discrete count with `bernoulli`/`system` is refused with
  DuckDB's message. `USING SAMPLE 2 ROWS REPEATABLE(7)` (no method) is accepted
  as a superset of DuckDB, which needs the parenthesised method form.
- **Window `FILTER` and windows built on windows.** `agg(x) FILTER (WHERE …)
  OVER (…)` skips non-matching rows frame-relatively (DuckDB's answer, `NULL`
  where nothing matches); a non-aggregate window function with `FILTER` is
  refused by name. `WINDOW w AS (…)` definitions can now build on an earlier
  window (`OVER (w ORDER BY …)`, `WINDOW v AS (w ROWS 1 PRECEDING)`), with
  DuckDB's merge rules: the base's `PARTITION BY`/`ORDER BY` are inherited, a
  derived clause may fill a gap but not override one, and copying a base that
  has a frame clause is an error.
- **Row constructors.** `ROW(a, b)` and `(a, b)` compare field-wise in `=`,
  `<>`, `<`, `<=`, `>`, `>=` and work as the left side of `IN (…)` with a
  value list. DuckDB's semantics are reproduced exactly: row comparison treats
  `NULL` as an ordinary value (`(1,NULL) = (1,NULL)` is true, `NULL` sorts
  greater), while `IN (SELECT …)` keeps three-valued logic. A standalone row
  and `(a, b) IN (SELECT …)` are refused by name (pinkdb has no `STRUCT`
  type and its `IN` probe is single-column).
- **`AT TIME ZONE` and `timezone(zone, ts)`.** Folded into one function. pinkdb
  timestamps are naive and it has no time-zone database, so a UTC-family zone
  (`UTC`, `GMT`, `Z`, `Etc/UTC`, `+00:00`) is the identity on a `TIMESTAMP`
  (a `DATE`/`VARCHAR` is cast first) and any other zone is refused with a
  message naming the missing time-zone database.
- **JSON mutation functions.** `json_set`, `json_insert`, `json_replace` and
  `json_remove` are engine functions with DuckDB's path semantics (missing
  parents are created, `json_insert` never overwrites, `json_replace` never
  creates, an array index past the end is ignored, `$` is the whole document,
  `$."quoted key"` works, any `NULL` argument gives `NULL`). They accept JSON
  text or a `VARIANT` and return the same kind, and a `VARIANT` value argument
  needs no `::JSON` cast — a documented superset of DuckDB, whose signatures
  require `JSON`.
- **Session variables.** `SET VARIABLE name = <expr>`, `getvariable('name')` and
  the `$name` shorthand. The value is spliced at bind time, so a later statement
  sees it (a variable set inside a multi-statement string is visible to the
  statements after it). `getvariable` of an unset name is `NULL` like DuckDB;
  `$name` errors, also like DuckDB. The state is **per connection**, like
  DuckDB's session: the embedded handle is one session, every network
  `Connection` and every served/MCP session its own, so one client's variable is
  invisible to — and cannot be overwritten by — another's. Variables are never
  persisted or WAL-logged, and a logged statement may not read one (replay
  would bind its text against a fresh session), which the binder refuses by
  name.
- **`FOR UPDATE` / `FOR SHARE`** stay refused, with a message naming the reason:
  pinkdb has no row locks and its MVCC already serializes writers. DuckDB 2.0
  rejects the clause too.
- **`MERGE` and `UPDATE … FROM` now hash-join their equality keys.** The ON /
  WHERE equality conjuncts build a hash table over the source once and probe it
  per target row (residual predicates still filter), keeping the old nested loop
  for anything without an equality key. Measured on 10 000 × 1 000 rows,
  interleaved: `MERGE` 3.18 s → 0.05 s (~65×), `UPDATE … FROM` 3.09 s → 0.006 s
  (~500×); 100 000 × 10 000: 340 s → 0.47 s and 318 s → 0.03 s; 1 000 000 ×
  100 000: `MERGE` 4.9 s, `UPDATE … FROM` 0.28 s (the nested-loop baseline at
  that size is an extrapolation of ~9 h, not a measurement). TPC-H Q1/Q6 and a
  group-by/sort query show no change (interleaved A/B, medians equal).
- **Docs, tests, wire protocol.** `docs/sql-conformance.md` gains the full
  batch-3 matrix (no form left unassigned) and `bench/duckdiff.py` 40 new cases
  (376 pass). The conformance suite adds 26 DuckDB-checked cases plus
  pinkdb-only cases for the forms whose spelling differs, seeded-sampling
  determinism and session variables. `SET VARIABLE` reports
  `PINKDB_STMT_SET_VARIABLE` in the C ABI and `DoneKind::SetVariable` (tag 26)
  on the server wire, which bumps the protocol to v12.

### Wave 93b — SQL `INSTALL`/`LOAD`, and a released `repo` variant ✅
- **`INSTALL` is SQL now.** `INSTALL name [FROM '<repo-or-url>']`,
  `FORCE INSTALL …` (re-download an identical cached copy) and `LOAD name`
  (resolve an installed extension by name, no-op if it is already loaded) run
  the same code as `.install` — one implementation in the engine
  (`crates/pinkdb/src/install.rs` plus the repository client moved from the CLI
  to `crates/pinkdb/src/repo/`), with `.install` reduced to a console alias.
  Signature verification, SHA-256 checking, the catalog format and the cache
  layout are unchanged.
- **The trust boundary is explicit.** Both statements need the `repo` feature;
  without it the error is `this build has no extension repository support
  (rebuild with --features repo, or use the pinkdb-repo release variant)`.
  Installing loads native code, so they take the `LOAD EXTENSION` opt-in
  (`--allow-load-extension` / `PINKDB_ALLOW_LOAD_EXTENSION=1`), need `ADMIN`,
  are refused inside a transaction, and a server refuses them for remote
  clients unless started with `--allow-load-extension`; the MCP read-only
  policy refuses them like every other write.
- **WAL decision.** `INSTALL` is deliberately **not** logged: downloading is
  not replay-deterministic and replay must never reach the network. `LOAD
  <name>` is logged like `LOAD EXTENSION`, and replay re-resolves it from the
  cache. Loading now happens from the verified cache path instead of a deleted
  private staging copy — that is what keeps the recorded path replayable — with
  a re-hash immediately before the load (a file that changed in between is
  refused). `LOAD <name>` trusts the per-user cache.
- **Release variant `repo`.** `install.sh --variant repo` (asset
  `pinkdb-<ver>-<os>-<arch>-repo.tar.gz`, features `repo`) is now part of the
  release matrix, smoke test and docs, so `.install`/`INSTALL` and the Ed25519
  check are no longer source-build-only. Measured on darwin-aarch64: release
  binary **7,040,240 B** with `repo` vs **4,920,832 B** default (+43.1%;
  tarball 3,161,246 vs 2,151,249 B).
- **Tests.** SQL-level end-to-end against the existing loopback fixture
  repository: install, `FROM`-only (no configured repositories), `LOAD` by name
  in a fresh process (no download), `FORCE` re-download, a tampered catalog
  signature refused before any artifact request, the connection gate firing
  before the network; non-admin RBAC refusals, MCP policy refusals and the
  no-repo messages; `writes_tables` pins the WAL decision. Workspace **1,914**
  tests, CLI with `repo` **123**, engine with `repo` **912**, release tooling
  **12/12**, wasm32 build green.

### Wave 95b — SQL dialect completeness, batch 2 (DML) ✅
- **Upserts.** `INSERT … ON CONFLICT [(cols)] DO NOTHING | DO UPDATE SET … [WHERE …]`
  with `excluded.col` (the proposed row) and the target table readable in the
  `SET`/`WHERE`; `INSERT OR REPLACE` (= `DO UPDATE` over the columns the
  statement writes, so unlisted columns keep their stored values) and
  `INSERT OR IGNORE` (= `DO NOTHING` for every key). A conflict target must be
  a declared `PRIMARY KEY`/`UNIQUE` constraint — DuckDB's refusal, verbatim —
  and a duplicated key inside one statement keeps the first input row.
  `ON DUPLICATE KEY UPDATE` and `REPLACE INTO` stay refused by name (DuckDB
  has no `REPLACE INTO` either).
- **Keys are enforced now.** Wave 103 left `PRIMARY KEY`/`UNIQUE` as metadata;
  `ON CONFLICT` without enforcement is incoherent, so `INSERT`/`UPDATE`/`MERGE`
  check every declared key per statement (`crates/pinkdb/src/keys.rs`: one hash
  map per statement, built from the table and updated as rows are staged). A
  duplicate — or a `NULL` in a primary key, or a multi-row insert whose rows
  collide with each other — raises DuckDB's message and the statement leaves
  the store unchanged. `UNIQUE` accepts any number of `NULL`s; `REFERENCES`
  stays a declaration (`DESCRIBE`/FK→PK join elimination only). Cost, measured:
  1 M-row `INSERT … SELECT` into a PK table 0.095 → 0.381 s (median of five,
  ~3.5 M keys/s); a bulk `UPDATE` that does not write a key column and a
  `DELETE` are unchanged.
- **`UPDATE … FROM` / `DELETE … USING`.** The source is bound before the
  target, so `FROM`/`JOIN` columns come first in expressions. Several source
  rows matching one target row: `UPDATE … FROM` keeps the last one in source
  order (DuckDB's answer — no error, unlike Postgres), `DELETE … USING`
  deletes once.
- **`RETURNING`** on `INSERT`/`UPDATE`/`DELETE`, on `ON CONFLICT` (scalar
  subqueries in the projection included) and on `MERGE`. The rows come from the
  write funnels, so they are the final image: a `BEFORE` row trigger's rewrite
  of `NEW` is what `RETURNING` reports, and a `DELETE` reports its rows in scan
  order. An `ON CONFLICT` action that stores the values already there still
  counts as affected, like DuckDB.
- **`MERGE INTO … USING … ON …`** with `WHEN MATCHED [AND …] THEN UPDATE SET …
  | DELETE`, `WHEN NOT MATCHED [AND …] THEN INSERT [(cols)] VALUES (…)`,
  `WHEN NOT MATCHED BY SOURCE [AND …]`, first matching clause wins, a target
  row affected by more than one source row refused with DuckDB's "cannot affect
  the same target row more than once" and the statement atomic. The source is
  materialized once; matching is a nested loop (a `shortcut:` comment documents
  a hash join as the upgrade path).
- **Data-modifying CTEs.** `WITH x AS (INSERT/UPDATE/DELETE … [RETURNING …])
  SELECT … FROM x`: the body runs once, at the top level (a nested one is
  DuckDB's "must be at the top level" error), and its rows are served from an
  in-memory hand-off (`db.dml_cte_outputs`) because the evaluator holds the
  store immutably. `COPY` as a CTE step stays refused (it is ADMIN-gated as a
  statement; a CTE step would bypass that gate).
- **AST, parser, binder, plan, executor, EXPLAIN.** `Statement`/`LogicalPlan`
  carry `conflict`/`returning`/`from`/`using`, plus `LogicalPlan::Merge` and a
  `CteBody::Dml` CTE body; the AST walkers (depth guard, parameter renumbering)
  recurse into all of them; `EXPLAIN` prints the conflict action and the MERGE
  clauses.
- **Durability.** The WAL logs each statement's SQL text, so every new form
  replays by re-execution. `crates/pinkdb/tests/dml_replay.rs` writes upserts,
  `MERGE`, `UPDATE … FROM`, `DELETE … USING` and a DML CTE, drops the process
  without a checkpoint, reopens (replay) and compares; then checkpoints and
  reopens again.
- **Privileges and the MCP.** The per-statement gate sees every table: `MERGE`
  needs the privilege of each action its clauses spell out plus `SELECT` on its
  `USING` source, `UPDATE … FROM`/`DELETE … USING` need `SELECT` on the source,
  a DML CTE's target needs its write privilege, and the read pass walks the CTE
  body. The MCP read-only policy refuses every new statement kind and a `WITH`
  whose body writes (tests in `serve.rs`).
- **Tests.** `sql_conformance.rs` gained 25 DML cases pinned to DuckDB and
  re-measured on DuckDB 2.0.0 (the same 25 run in `bench/duckdiff.py`, 336/336
  passing), the error cases, key enforcement, `RETURNING` after triggers, the
  DML-CTE `EXPLAIN`, and MERGE's multiple-match; `rbac.rs`, `dml_replay.rs` and
  `serve.rs` cover the gates. Gates: clippy clean, workspace 1856 passed /
  0 failed (8 ignored), `-p pinkdb` 855 (6 ignored), `-p pinkdb-cli --features
  pinklake,s3,parquetlake` 168, wasm32 build, duckdiff 336/336.
- **No regression** for existing DML: interleaved A/B against HEAD `6531691`
  (own worktree + target dir), five rounds, medians —
  `INSERT INTO t2 SELECT * FROM t` 1 M rows 0.095 → 0.381 s (the key check),
  `UPDATE t SET v = v + 1` 1 M rows 0.025 → 0.025 s, `DELETE FROM t WHERE id %
  2 = 0` 500 k rows 0.019 → 0.019 s (`bench/wave95b_perf/`).

### Wave 93a — one ABI in four SDKs, one conformance fixture, pinned libraries ✅
- **ABI 1.2, negotiated.** The extension header gained the size constants and macros a
  library uses to ask what the host actually publishes — `PINKDB_EXT_API_V1_SIZE`,
  `PINKDB_EXT_API_FLAGS_SIZE`, `PINKDB_EXT_API_MINOR2_SIZE`,
  `PINKDB_EXT_HOST_HAS_FLAGS()` and `PINKDB_EXT_HOST_HAS_EXECUTE()` — plus
  `pinkdb_ext_copy_api()`, which copies only the bytes `struct_size` promises (the old
  `g_api = *api` read past a smaller host struct). The four vendored copies
  (`ext-sdk/{c,cpp,go,rust}/include/pinkdb_extension.h`) are byte-identical to the loader's
  and `tests/header_sync.rs` fails `cargo test` if they drift.
- **C, C++, Go and Rust at the same ABI.** All four cover `register_scalar_flags` and
  `PINKDB_EXT_FLAG_VOLATILE` (refusing to register a volatile function on a host too old
  for the flag, instead of silently calling it deterministic), the full type-tag set
  including INTERVAL's packed months/days with micros, the init-only host-API copy, the
  name/arity/tag bounds, host `alloc`/`free` text ownership, and `execute_statement`. The
  C++ SDK is new (`ext-sdk/cpp`, header-only `pinkdb::ext` with a move-only owned `Text`, a
  `Writer` for results and an exception guard), and the copy-pasted Go shims became one
  package (`ext-sdk/go/pinkdb`). Table functions, scans and copy sinks are still
  linked-only, and every README says so.
- **One fixture, four SDKs.** `ext-sdk/conformance/README.md` defines
  `conformance_ext`: 15 functions, a canonical `cf_all` row that renders every tag
  bit-exactly, `cf_fail` (publishes text, then fails), `cf_unwind` (panics/thows) and
  introspection counters. Each SDK builds it, and the loader drives all four
  (`crates/pinkdb-ext-loader/tests/conformance.rs`, 11 tests): tag round-trips with NULL,
  embedded NUL, empty strings and 64 MiB + 1 results refused, error codes, panic
  containment (a statement fails and the extension still answers afterwards), 100 callbacks
  from four foreign threads, and `struct_size`/`abi_version` negotiation in a child process
  (old host → refused non-zero with nothing registered as deterministic; full host → all 15
  registered). `tests/conformance_ownership.rs` checks the host allocates and frees exactly
  once per VARCHAR result and leaks nothing on empty, NULL or error paths.
- **Local libraries can be pinned.** `--extension-pin PATH=SHA256` (CLI and server,
  repeatable) installs a SHA-256 allowlist in the loader: once it is set, every load must
  match a pin — an unpinned library, a hash mismatch, or a file that changed between the
  hash and the `dlopen` is refused with the reason. The hash is read from an open file and
  the file's `(dev, ino, len, mtime)` re-checked immediately before mapping; the residual
  check-then-map window (there is no `fdlopen` on Linux or macOS) is documented. Local
  libraries are still unsigned: Ed25519 signatures cover `.install` packages only.
- **Posture, stated and tested.** `ext-sdk/posture.sh` drives the release CLI through the
  refusals every README promises: `LOAD EXTENSION` off unless
  `--allow-load-extension`/`PINKDB_ALLOW_LOAD_EXTENSION=1`, `--extension-root`
  confinement, bare names and `..` and symlinks refused, pins enforced. CI runs it after
  the four smokes and the C++ build, with the conformance matrix under
  `PINKDB_CONFORMANCE_STRICT=1` so a missing toolchain fails instead of skipping.
- **93b** shipped separately (see below): SQL `INSTALL`/`LOAD <name>` and a
  released CLI variant with the `repo` feature.

## 2026-10-25
### Wave 95a — SQL dialect completeness, batch 1 ✅
- **The matrix first.** `docs/sql-conformance.md`
  lists every query form row 95 scopes, each checked by running it against
  DuckDB 2.0.0: supported (same answer), divergent (and how), or refused with
  the exact error — and each assigned to batch 95a, 95b or 95c. Row 95 is
  large, so 95a is the query forms, 95b the DML forms, 95c the rest.
- **New query forms.** `QUALIFY`; `TRY_CAST(expr AS type)` (NULL on failure,
  every target type); subscripts `v['a']`, `v[0]`, `v.a` and string
  `'hello'[2]` / `'hello'[2:4]` (1-based, negative from the end, like DuckDB);
  `IS [NOT] DISTINCT FROM`; `VALUES` as a query, in `FROM` with column aliases
  and as the column-name source (`col0`, `col1`, …), plus `TABLE t`;
  `FETCH FIRST n ROWS ONLY` and `OFFSET … ROWS`; `LIKE … ESCAPE`; `SIMILAR TO`
  as the fully anchored regex DuckDB actually implements (a hand-written NFA in
  `crates/pinkdb/src/regex.rs` — no new dependency); `DISTINCT ON (…)`
  (first row per key in `ORDER BY` order); aggregate `FILTER (WHERE …)`, also
  with `DISTINCT`; named `WINDOW w AS (…)` clauses; `GROUPING SETS`, `ROLLUP`,
  `CUBE` with `GROUPING()`/`GROUPING(a, b)` (big-endian mask, DuckDB's set
  order); `WITH RECURSIVE` (semi-naive iteration, `UNION` de-duplicating,
  `UNION ALL` not; bounded at 100 000 iterations and 10 000 000 rows — both
  are errors, so a runaway recursion cannot hang the engine); and
  `UNION`/`INTERSECT`/`EXCEPT` `ALL` are covered by the corpus now.
- **Everything else fails by name.** Every form 95c will add is recognised
  and refused with a `wave 95c` sentence (`LATERAL`, `WITHIN GROUP`, `TABLESAMPLE`, `FOR UPDATE`/`SHARE`,
  `AT TIME ZONE`, row constructors, list literals, `FILTER` on a window, a
  window built on a window, VARIANT slicing, `SET VARIABLE`, `$name`), and the
  refusal is reported on its own instead of inside peg's expected-token list.
- **Plan and EXPLAIN.** `LogicalPlan` gained `Values`, a `recursive` step on
  `Cte` and an `on` column subset on `Distinct`; `EXPLAIN` prints `Values (n
  rows)` and `Recursive step (UNION ALL)`, and every new form shows its
  operator (`Window` for `QUALIFY`, `Distinct` for `DISTINCT ON`, …).
- **Tests.** `crates/pinkdb/tests/sql_conformance.rs`: 25 shared-corpus cases
  pinned to DuckDB's answers and re-measured on DuckDB when
  `/tmp/duckdb-2.0.0/duckdb` exists, the EXPLAIN shapes, the 95c refusals,
  and the pinkdb-only shapes (VARIANT subscripts, grouping masks, recursion
  bounds). A `SELECT 1.e2` regression from the new `.field` postfix is a parse
  error again.
- **No regression.** Interleaved A/B against HEAD `1d199fb` on a
  1 000 000-row TPC-H-shaped store (release: TPC-H Q1 0.012 → 0.012 s, Q6
  0.007 → 0.007 s, group/sort 0.131 → 0.132 s; medians of six runs,
  `bench/wave95_perf/`).

## 2026-10-24
### Wave 76 — CSV import options ✅
- **One option set, two front ends.** `.import` and `read_csv` now share
  `pinkdb::csv::CsvOptions`, so `.import f.csv t sep=';' header=true` and
  `read_csv('f.csv', sep := ';', header := true)` read a file identically —
  locally, over `https://` and from `s3://`. Named parameters use DuckDB's
  names (`delimiter`/`sep`/`delim`/`separator`, `quote`, `escape`, `encoding`,
  `header`, `nullstr`/`null`, `names`/`column_names`,
  `ignore_errors`/`errors`, `allow_quoted_nulls`, `force_not_null`); an unknown
  or unsupported name is an error naming it, never silently ignored.
- **Dialect.** `delimiter` takes any single byte (a literal tab or the `\t`
  spelling), `quote` any byte, `escape` a byte or off (the default). A
  multi-byte delimiter is refused instead of truncated.
- **Encodings.** `latin-1`, `windows-1252`, `utf-16le`, `utf-16be`, and the
  default `utf-8`; the default (`auto`) also strips a UTF-8 BOM and switches to
  UTF-16 on a LE/BE BOM. The decoders are hand-written and stream — no new
  dependency — so the wave-98c read-ahead and ranged remote reads keep working
  and the sniff window never needs the whole file.
- **Header and column matching.** `header=auto` applies DuckDB's rule: infer
  the types over every row and again over the rows below the first; row 1 is a
  header when the two disagree or when the data is all text. Importing into an
  existing table matches columns **by header name**, case-insensitively:
  missing columns become `NULL`, and file columns the table has no home for
  follow the error policy. `names=` overrides the header, `header=false` reads
  the first line as data (`column0`, `column1`, …).
- **NULLs.** `nullstr='NA,N/A'` adds tokens, `allow_quoted_nulls=false` keeps a
  quoted empty field as `''`, `force_not_null='a'` keeps a column's empties as
  `''` (which widens the column to `VARCHAR`). The default is DuckDB's measured
  behaviour, not the folklore: an empty field is `NULL` whether quoted or not.
  A quoted cell keeps its own whitespace (only unquoted cells are trimmed), so
  `"  x  "` stays `"  x  "` while `" 1 "` still imports as a number.
- **`errors=warn`** (DuckDB's `ignore_errors := true`, accepted by that name
  too) skips bad rows — a value that does not parse, or a row with more fields
  than the header — and `.import` prints a rejects report (line, column,
  reason; the first ten plus a total). A `read_csv` scan has nowhere to print
  that report, so a scan skips silently: `warn` means skip, never "count the
  bad rows". `error` stays the default: the first bad row fails the import and
  leaves no table behind, and the error names the line. Storing rejects in a
  table remains the recorded follow-up.
- **The `csv` crate is gone.** Wave 75 pulled it in for the record reader; the
  engine's own streaming reader replaced it and the dependency was left behind,
  unused, with only `PinkDbError::Csv(csv::Error)` still naming it. That variant
  is now pinkdb's own `PinkDbError::Csv { message, line, column }` (the FFI,
  agent-JSON and Python bindings keep their stable `"csv"` error kind, and a
  structural error now names its line), and `csv` left `Cargo.toml` and the
  lockfile (22 lines). Size of the default release `pinkdb` binary:
  **4,524,032 → 4,536,208 bytes** (+12,176, +0.27 %), which is the new error
  payload and formatting, not the dependency — re-adding the unused `csv` crate
  to the same tree builds a binary 16 bytes *smaller* (build metadata), so it
  contributed no code.
- **No import-speed regression.** The options layer initially cost 7× on
  `.import` (18.2 s for TPC-H `lineitem`, 773 MB / 6 001 215 rows, against
  2.54 s before the wave). Two causes: the type sniff walked the *whole* file
  through the byte-at-a-time record reader, and the parallel batch parser
  refused any file containing a `"`, so any quoted column fell back to it. The
  sniff is now sample-limited (20 480 rows, DuckDB's `sample_size`; a column
  with no value in the sample still triggers the exact pass), and the batch
  parser understands RFC 4180 quoting — fields are split with the quoting
  rules, `""` unescapes, a quoted cell keeps its whitespace — so a file is
  parsed in parallel exactly as the record reader would read it — a file the
  scan cannot prove falls back to the reader rather than being read wrong.
  Measured with the reviewer's harness, interleaved, median of 3: `lineitem`
  **2.54 s → 0.87 s**, `orders` (174 MB) **0.42 s → 0.21 s** — twice as fast
  as before the wave, not just within budget. Slower by design, and measured
  on the same 773 MB file: `errors=warn` 12.2 s (the rejects report needs the
  sequential reader), `encoding=latin-1` 14.2 s, `encoding=utf-16` 13.3 s
  (1.5 GB, BOM autodetected) — transcoding and warn still run through the
  record reader, which is the recorded next target.
- **`read_csv` scans share it.** A table-function scan parsed with the record
  reader too (7.4 s for `SELECT count(*) FROM read_csv('lineitem.csv')`
  against 5.03 s before the wave). It now runs the same batch parser: a file
  under the 8 MiB sniff window at once, anything larger — a remote object, or
  a local file past that window — in 8 MiB windows of the wave-98c prefetch,
  each window cut at its last provable record end so a record can straddle two
  windows, and a window the scan cannot prove is finished by the reader. 5.03 s → **1.36 s**, and
  the same 33.9 MB file through the test HTTP server 486 ms → **120 ms**
  (local 522 → 77 ms). Values, not just counts, match `.import` on lineitem
  (`sum(length(l_comment))`, `sum(l_orderkey)`, date bounds, `sum(l_quantity)`).
- **Unchanged:** a sampled column that a later row contradicts still falls
  back to whole-file inference, and a durable import still checkpoints before
  it returns. An import that appends to an existing table is atomic too: a
  strict failure restores the table instead of leaving half the file in it.
- Tests: `pinkdb` CSV module 34 (from 19), including a DuckDB-oracle test for
  the empty-field rule and header auto-detection that skips when the α binary
  is absent; `pinkdb-httpfs` 62 (local-vs-remote option parity, `errors=warn`
  in a scan); 4 new `.import` CLI tests; workspace **1865** tests / 161 suites,
  9 ignored (one of them the scan speed aid).

### Wave 101a — the `VARIANT` type core ✅
- **The type.** `VARIANT` is now a column type: one column, any shape per row —
  a JSON null, boolean, integer, double, decimal, string, date/timestamp, array
  or object. Values live in a binary, self-describing encoding (a kind tag per
  value plus the payload), not in JSON text, and objects keep their keys sorted
  so equal objects encode identically. `typeof(v)` reports `'VARIANT'`.
- **Casts.** Any type → `VARIANT`; `VARIANT` → every scalar, with an error that
  names the kind and the value (`cannot CAST VARIANT(VARCHAR) value "hi" to
  INTEGER`) instead of a wrong answer. `'{"a":1}'::VARIANT` parses JSON — the
  literal form the parser, the WAL and prepared statements already carry (this
  is where we differ from DuckDB, where `VARCHAR → VARIANT` is the identity
  and the JSON extension is required first). A JSON null reads as SQL `NULL`
  for every target except `VARCHAR`, which keeps the text `null`, so
  `VARIANT → VARCHAR → VARIANT` is lossless for JSON's value model.
- **Functions.** `variant_type(v)`, `variant_keys(v)` (a JSON array of the
  keys; pinkdb has no list type yet) and `variant_extract(v, path)` with
  0-based JSONPath (`'a.b[1]'`, `'$.a[0]'`) — a missing path is SQL `NULL`.
  The JSON extension's functions (`json_extract`, `json_array_length`, …)
  accept a `VARIANT` and see its JSON text.
- **Semantics, defined and tested.** Equality is structural, with numbers
  comparing by value across kinds (`1` = `1.0` = `1.00`, `'1'` ≠ `1`). One
  documented total order serves `ORDER BY`, `MIN`/`MAX` and range comparisons:
  bool < number < string < date < timestamp < array < object < JSON null, with
  SQL `NULL` last. `GROUP BY`, `DISTINCT` and joins hash a canonical key that
  agrees with `=` by construction. A JSON null is its own group and is never
  equal to SQL `NULL`.
- **Persistence.** New `.pink` dtype tag (9) and default-value tag; a
  `VARIANT` column survives save/open, WAL replay, checkpoint and
  `ALTER TABLE … ADD COLUMN … DEFAULT`. VARIANT chunks carry no min/max
  statistics. An older build that meets the new tag reports a clear
  `corrupt file: bad dtype tag 9` — no legacy reader.
- **Encodings elsewhere.** Wire protocol v11 (new dtype and value tags; a
  `VARIANT` rides as a length-prefixed blob so an old client fails cleanly);
  FFI `PINKDB_TYPE_VARIANT` (the JSON text); the JS/WASM binding hands it over
  as a native JSON value. Refused with clear errors: the extension C ABI
  (like `DECIMAL`), the Arrow extension, and Parquet write.
- **Lake.** A `VARIANT` column round-trips in a `.pink` lake data file; a
  Parquet-format lake refuses it with a message naming wave 101b rather than
  writing it as text.
- **Not in this wave.** `TRY_CAST` (does not exist anywhere in pinkdb),
  DuckDB's `variant_extract_string`/`variant_array_length`/`variant_exists`/
  `variant_normalize`/`variant_comparator`, array/field subscript syntax
  (`v['a']`), and Parquet `VARIANT` — that is 101b, together with shredding
  into typed sub-columns and statistics on shredded paths. The plan for it,
  the full tag table, the semantics and every deliberate difference from
  DuckDB are written in `docs/wave-101-plan.md`.
### Wave 72 — Core extension: iceberg ✅
- **`iceberg_scan`, `iceberg_snapshots`, `iceberg_metadata`** in a new
  engine-adjacent crate, `extensions/iceberg` (`pinkdb-iceberg`), behind the
  opt-in `iceberg` CLI feature (which turns `parquet` on — the data files are
  read by that reader). A table location is the table directory, its
  `metadata/` directory or one `…/metadata/vN.metadata.json`; a directory is
  resolved through `metadata/version-hint.text`, else the newest
  `vN.metadata.json`. Paths inside the metadata resolve against the table root,
  so a copied or mocked table works unchanged — what DuckDB calls
  `allow_moved_paths`.
- **Iceberg v2 semantics.** The current snapshot, or one named by
  `snapshot_from_id` / `snapshot_from_timestamp` (DuckDB's names); the manifest
  list, then the Avro manifests, then the live Parquet data files; **positional
  delete files** applied; **equality deletes refused by name** (a documented
  follow-up — DuckDB applies them, and the test pins that difference); schema
  evolution by **field id**, so a renamed column projects under its new name
  and an added one reads NULL in files written before it; **partition-summary
  pruning** (identity transforms) and **lower/upper-bound + NULL-count pruning**
  from the manifest entries; `count(*)` answered from the manifest record counts
  without opening a data file.
- **A hand-written Avro decoder**, not apache-avro: container header, sync
  markers, `null`/`deflate` block codecs, and the records, unions, arrays,
  maps, enums and fixed types the Iceberg manifest schemas use (600 lines
  against a dependency tree). `serde_json` parses the metadata JSON (already in
  the lock file).
- **Remote tables through the shared transport.** The `remote` feature (enabled
  by the CLI's `httpfs` feature) reads `https://`/`s3://` tables through
  `pinkdb-httpfs` — the same path `read_parquet` uses — with windowed 256 KiB
  read-ahead for the metadata JSON and the Avro files, then Parquet's ranged
  reader for the data. A remote table needs a `version-hint.text`; without one,
  pass the metadata file.
- **Fixtures without downloads.** `extensions/iceberg/tests/fixtures/gen.py`
  writes Iceberg v2 tables with the standard library alone — metadata JSON, an
  Avro container writer and manifests by hand, Parquet through the DuckDB 2.0
  binary — for six tables (two snapshots, positional deletes, equality
  deletes, a delete file older than the data file it names, schema evolution,
  partitioned), 232 KB committed. The **DuckDB 2.0 α oracle** compares every
  fixture's results (13 query/table pairs) and runs whenever the binary is
  present; a mock S3 and the local HTTP test server prove the remote path with
  ranged request counts.
- **Cost:** the crate's release rlib is 1,165,120 B; the CLI binary grows
  254,560 B over `parquet` (6,475,744 B with `iceberg`, 8,626,896 B with
  `iceberg,httpfs`) and the default binary is untouched — the crate is not in
  its dependency graph.

## 2026-10-24
### Wave 92 — Language bindings: Node.js, Python DB-API 2.0, Go ✅
- **One conformance corpus.** `bindings/conformance/corpus.json` is the
  language-neutral contract every binding is tested against: every SQL type's
  round trip (including NULLs, unicode, bigint extremes, NaN/±Infinity,
  DECIMAL, DATE and TIMESTAMP), positional parameters, a stable error class
  for parse/catalog/type/parameter/execution/constraint failures, explicit
  transactions and autocommit, multi-statement batches with several result
  sets, empty results, and a 100k-row stream that must not be materialized.
- **Node.js** (`bindings/node`): a napi-rs addon that links `pinkdb-ffi`
  statically, so the `.node` file has no runtime library dependency.
  `open(path?)` / `close()`, `run(sql, params?)` for batches, `query(sql,
  params?)` for row objects, `prepare()` with `run`/`all`/`iterate`, explicit
  `begin`/`commit`/`rollback`, `PinkdbError` carrying a `.class`, and the
  `pinkdb_abi_version` gate at load. BIGINT maps to `BigInt`, DECIMAL and
  INTERVAL to their canonical strings, DATE/TIMESTAMP to strings. Builds with
  plain `cargo` through a small copy step; `npm pack` works, nothing is
  published.
- **Python** (`bindings/python`): the `ctypes` binding is now also a PEP 249
  DB-API 2.0 driver — `connect()`, `Connection`, `Cursor` with
  `execute`/`executemany`/`fetchone`/`fetchmany`/`fetchall`, `description`,
  `rowcount`, `qmark` parameters, the standard exception hierarchy mapped from
  the C ABI's error class, and the type objects. Cursors fetch one row at a
  time: the 100k-row case peaks at about a kilobyte of Python allocations.
  Transactions are explicit, matching DuckDB: DDL is refused inside them, so
  there is no implicit `BEGIN`.
- **A real wheel.** `pip wheel . --no-deps -w dist` compiles `libpinkdb_ffi`
  and bundles it in `pinkdb/`, producing a platform wheel that installs and
  runs without a local build (`PINKDB_FFI_LIB` skips the compile).
- **Go** (`bindings/go`): the module is usable outside the checkout — the cgo
  preamble carries the ABI declarations instead of pointing at the header, and
  the library location comes from `PINKDB_LIB_DIR`/`CGO_LDFLAGS` (with the
  in-repo `target/release` as the convenience default). Errors now keep their
  class in `*pinkdb.Error`, and an ABI test checks the carried declarations
  against `pinkdb.h` whenever the checkout is present.
- **C ABI.** `pinkdb_last_error_class()` returns the stable error class next
  to the message (additive to ABI v1), so bindings no longer parse text to
  tell a parse error from a constraint violation. The engine and default CLI
  binaries are unchanged.
- **CI.** The bindings job now builds and tests the Node addon, the corpus and
  `npm pack`; Go keeps `gofmt`/`vet`/`-race`; a new `python-wheel` job builds
  the wheel, installs it into a throwaway venv, and runs the corpus from the
  installed package.

## 2026-10-24
### Wave 100 — Triggers ✅
- **Triggers.** `CREATE [OR REPLACE] TRIGGER name {BEFORE|AFTER}
  {INSERT|UPDATE [OF col, …]|DELETE} [OR …] ON table [REFERENCING OLD TABLE
  AS o NEW TABLE AS n] [FOR EACH {ROW|STATEMENT}] [WHEN (cond)] EXECUTE
  {PROCEDURE p(args) | <statement>}`, `DROP TRIGGER [IF EXISTS] name [ON
  table]` and `SHOW TRIGGERS [ON table]`. Bodies run through the stored
  procedure machinery (`ParamSlots` + `run_call_statement`), so each body
  statement is bound, optimized and executed like any other statement.
- **Semantics.** Triggers of one event fire in creation order; every BEFORE
  trigger runs before the row change and every AFTER trigger after it.
  `OLD.col` / `NEW.col` are available in `WHEN` and in the body; a BEFORE ROW
  trigger may assign `NEW.col` (the assignment is coerced to the column type
  and the written value is what lands in the table), `OLD` is read-only, and
  there is no row-skipping signal in v1 — returning from a BEFORE ROW trigger
  always lets the change through. `REFERENCING OLD TABLE / NEW TABLE` are
  readable relations (ordinary CTEs to the body, so any query shape works) and
  are accepted on AFTER STATEMENT triggers only. `UPDATE OF` narrows to the
  columns the statement assigns. Recursion is bounded at 16 levels with a
  clear error; a trigger error rolls back the whole firing statement,
  including in autocommit.
- **Replay.** The WAL still logs the firing statement and nothing else, so a
  replay re-executes it and the triggers fire again exactly once; a trigger
  is never logged separately, which is what keeps replay from double-counting.
  Volatile extension functions are already refused in logged statements, so
  bodies are deterministic; `now()`/`random()` are not builtins in pinkdb.
- **Persistence.** Definitions live in a new `.pink` footer section under
  flags bit `0x80`, written as their `CREATE TRIGGER` text in creation order
  so a reopen reproduces the firing order. A file without the bit has no
  triggers; no old format is read or converted.
- **RBAC.** Creating or dropping a trigger needs `CREATE` on the database —
  the same requirement as `ALTER TABLE` and the index DDL. Bodies run with
  **invoker** rights: every body statement is re-checked against the firing
  user, so a low-privilege user cannot use a trigger to write a table they
  cannot write. `SHOW TRIGGERS` is a metadata read available to any user.
- **Interactions.** Trigger-written tables join the statement's MVCC touched
  set (a concurrent writer conflicts instead of silently losing), materialized
  views refresh for triggered changes like any other change, and `.lake commit`
  of a writable attachment publishes trigger effects with the rest of the
  session's writes. Trigger definitions are not carried into a lake (it stores
  data).
- **Refused, clearly.** Triggers on views, on attached lake tables, on a
  missing table, `REFERENCING` on a ROW trigger or a BEFORE trigger, unknown
  `UPDATE OF` columns, and `RETURNING` on a triggered table (pinkdb has no
  `RETURNING`). `CREATE`/`DROP TRIGGER` inside a transaction is refused like
  the other catalog DDL.
- **No cost without triggers.** The DML fast path is untouched: the trigger
  check is one empty-slice lookup and the batch insert/update/delete code is
  byte-identical when a table has no triggers. 1,048,576-row `INSERT …
  SELECT` 34.4–34.8 ms (HEAD 34.1–34.3 ms) and bulk `UPDATE` 19.9–20.4 ms
  (HEAD 20.3–20.5 ms) — both within run-to-run noise.
- 39 trigger integration tests plus an ignored crash/replay child, a CLI lake
  test, and the full gate set green (workspace 1,796 tests / 156 suites).

### Wave 105 — agent bridge: MCP server, no model code ✅
- **A store is an MCP server.** `pinkdb --mcp <file.pink>` loads the
  `pinkdb-mcp` extension from the install directory and serves one MCP
  client over stdio (JSON-RPC 2.0); `.mcp serve [stdio|http <port>]
  [allow-writes]` does the same after `LOAD EXTENSION`, and
  `pinkdb-server --mcp-extension <path>` bridges the server's own store over
  loopback HTTP (`--mcp-port`, default 7801) under its logins
  (`--mcp-user` + `PINKDB_MCP_PASSWORD` on a store with users, RBAC applied).
- Eight tools, each with a JSON Schema and a model-facing description:
  `query(sql)`, `describe(table)`, `tables(database?)`, `databases()`,
  `explain(sql)` (with wave 99's scan-size hint) and the lake readers
  `lake_tables(catalog)`, `lake_versions(catalog)`,
  `lake_changes(catalog, table?, from?, to?)`. Answers are the wave-99
  Markdown tables with truncation notices and one-line JSON errors, under the
  same 1,000,000-row / 30 s budget.
- **Read-only by default, enforced in the host.** The statement callback
  parses every statement and, without `allow-writes`, refuses everything but
  `SELECT`/`EXPLAIN`/`DESCRIBE`/`SHOW …`/`USE`: DML, DDL, `COPY … TO`,
  `LOAD EXTENSION`, `CHECKPOINT`, role changes and lake maintenance answer
  `{"error":…,"kind":"permission"}`. Lake maintenance is never served. The
  HTTP transport binds loopback only and refuses any other address.
- **No external files or URLs by default.** A query alone can leak data, so
  the host also binds every statement and refuses it when the plan or the
  `FROM` clause reaches a table function or a bare path
  (`read_csv('/home/u/.ssh/id_rsa')`, `read_parquet('https://…')`,
  `FROM 'x.parquet'`), including through CTEs, subqueries, set-operation arms,
  joins and `EXPLAIN`; `allow-files` (`--mcp-allow-files`,
  `.mcp serve … allow-files`) lifts it and is independent of `allow-writes`.
  The `pinkdb-server` bridge keeps its login's grants instead, where file
  scans are administrator-only.
- **Extension ABI minor 2, additive.** `PinkdbHostApi::execute_statement`
  runs SQL text in the host and returns formatted text (no engine pointer
  crosses the boundary), and the optional `pinkdb_ext_serve` export hands the
  extension a transport descriptor (`stdio` or `http:HOST:PORT`) plus an
  options string. Detection is by symbol presence and `struct_size`, so every
  ABI 1 extension loads unchanged; the loader refuses to load or serve a
  library twice at once. The C header documents both, vendored into the three
  SDKs; the C/C++/Go wrappers get the surface in the wave-93 refresh.
- The extension is `extensions/mcp` (`pinkdb-mcp`, a cdylib using
  `serde_json` only); the host's policy, budgets and formatting live in
  `pinkdb::serve`, and the CLI routes `.lake` reads back through the same
  callback, so the extension never links pink-lake. Neither default binary
  grows beyond the host-side field.
- Any MCP client connects with no adapter: `pi mcp add pinkdb -- pinkdb --mcp
  store.pink` (pi) or `claude mcp add pinkdb -- pinkdb --mcp store.pink`
  (Claude Code).
- Tests: the loader's C serve extension (6), engine serve policy/rendering
  (8), MCP protocol conformance and transport rules (11), a CLI end-to-end
  set that drives `pinkdb --mcp` over stdio through a TPC-H style
  top-3-customers question, every write refusal, a truncation case and the
  `allow-writes` counterparts over HTTP (6, plus the lake catalog reader
  with `pinklake`, 7), and the server bridge under logins (3). Workspace:
  1,753 tests in 155 suites, 0 failed, 7 ignored (pre-existing).
- Release sizes, unchanged feature sets: `pinkdb` 4,221,296 → **4,260,048 B**
  (+0.9%), `pinkdb-server` 3,802,464 → **3,927,120 B** (+3.3%); the bridge
  itself is a 565,824 B cdylib that no binary links. `pinkdb` still builds
  for wasm32.

- **Agent mode** turns on with `-agent`, or automatically when stdout is not
  a terminal and one of `AI_AGENT`, `CLAUDECODE`, `CLAUDE_CODE`, `PI_AGENT`,
  `OPENCODE`, `CODEX_SANDBOX`, `CURSOR_AGENT` or `GEMINI_CLI` is set. A flag
  wins over the environment; `-no-agent` and `PINKDB_AGENT_MODE=0` force it
  off, and an interactive terminal never enables it by itself. One line on
  stderr announces the mode.
- Results are compact Markdown tables with types in the header, `NULL` for
  nulls, control characters escaped, and explicit truncation notices such as
  `… 1,234 more rows (1,284 total); 3 columns hidden: a, b, c`. Defaults are
  100 rows, 32 columns and 500 characters per cell.
- Errors are one line of JSON on stderr (`{"error": …,"kind": "parse",
  "position": 6}`), with byte offsets for parse errors. Exit codes are
  unchanged.
- A runaway-query guard (default 1,000,000 rows and 30,000 ms per statement)
  cancels the statement cleanly and leaves the session usable. Before a scan
  the engine estimates as expensive (default 100,000 rows) the CLI prints one
  cost hint on stderr naming the estimate and whether a remote scan is
  involved.
- The formatter, error encoder and scan estimate live in the reusable
  `pinkdb::agent` engine module (`crates/pinkdb/src/agent.rs`); the guard is
  exposed as `PinkDb::execute_with_limits`. `.help agent` documents flags,
  caps, settings (`PINKDB_AGENT_DISPLAY_ROWS`, `_COLUMNS`, `_CELL_CHARS`,
  `PINKDB_AGENT_SCAN_ROWS`, `_TIMEOUT_MS`, `_HINT_ROWS`) and `.agent`.
- Measured on a locally generated TPC-H-style dataset (10,000-row tables,
  10 queries, one 10,000-row result and one error): normal output 485,403 B /
  10,114 lines, agent output 4,162 B / 180 lines — 99.1% fewer bytes and
  98.2% fewer lines for the same answers and exit code.

## 2026-10-24
### Wave 104 — the legacy lake data format removed ✅
- **Breaking:** a lake whose catalog names the removed columnar data format
  no longer opens, with a clear message asking for a re-save with an older
  build. There is no reader and no converter. `.pink` (`PINKDB06`) and
  Parquet (the `parquetlake` provider, for DuckDB interop) remain.
- The old full-lake cargo feature and its large dependency tree are gone, so
  `pinklake` is the lake build. The release, install and Docker variants drop
  the old names. Tests that used the removed format for general subjects
  (format contract, mixed lakes, ranged/projected reads, partition pruning,
  schema evolution, fast reads) now run on `.pink` or Parquet.
- Leftover wave-103 crypto removed: PNKLAKE1 version-1 reading and the
  Argon2id KDF path. Per-file keys are raw, and `argon2` is no longer a
  pink-lake dependency.
- Size (release, Apple Silicon): `pinkdb-cli --features
  pinklake,s3,encryption` is **8,780,560 B**, down 41,632 B from the same
  build at HEAD; the removed full-lake build was **51,407,280 B**. The
  dependency tree is 109 lines (was 422 with the old format, 110 for HEAD
  `pinklake`), pink-lake is 51,714 → 47,865 `.rs` lines across the crate
  (−7.4%; `src/` 26,598 → 23,183, −12.8%), and a clean debug build takes
  12.95 → 13.69 s (noise: the removed feature was already off in the
  `pinklake` set).

## 2026-10-24
### Wave 103 — Lake catalogs are databases (DuckLake v1.1 tables) ✅
- **The catalog is always a database.** A lake is a `pink:<path>` durable
  pinkdb catalog (default), `sqlite:<path>` (`catalog-sqlite`) or
  `postgres://…` (`catalog-postgres`), all using DuckLake v1.1's
  `ducklake_*` tables with `begin_snapshot`/`end_snapshot`. Commits insert a
  typed delta and one snapshot row instead of rewriting a JSON catalog and
  its snapshot files; `count(*)` is `SUM(record_count)`, and
  time travel, expire, cleanup, CDC, replication state and statistics read
  the same rows. DuckDB's `ducklake` extension can attach the SQLite/Postgres
  catalogs, and pinkdb opens DuckDB-written DuckLakes.
- **Locations.** A bare path means `pink:<path>` with data in
  `<path>.files/`. A catalog records its data location, so
  `.lake open <catalog>` is enough. `s3://…` alone is no longer a lake: pass
  a catalog and `data s3://…` (`.lake save pink:lake.pink data
  s3://bucket/prefix`), or use a `postgres://` catalog for multi-machine
  lakes. `.lake migrate-catalog` is removed.
- **Per-file encryption** stays: each data/delete file is a PNKLAKE1
  raw-key container with its own random 256-bit key recorded in the
  catalog; a `pink:` catalog may itself be engine-encrypted. Manifest
  sealing, the passphrase-derived lake key, the JSON rollback marker and the
  marker files, and the degraded/conditional metadata-write paths are gone.
- **Breaking:** lakes saved by an older build in the JSON layout do not open.
  There is no converter; re-save them with an older build (for example
  `.lake open <old>` then `.lake save pink:new.pink`) before opening with this
  build.
- **Fix after review:** file statistics were resolved against the column's
  *current* name, so a file written before a RENAME lost its old physical
  column when attach rebuilt the zone map. Statistics now resolve through the
  `ducklake_column` definition visible at the file's `begin_snapshot`; a v1
  file projects through the rename at head, and time travel to v1 sees its
  original names and rows. Positional deletes keep addressing the same rows
  across ADD/DROP COLUMN. The schema, time-travel, replication, S3,
  encryption, corruption and attach-error tests deleted in the first pass were
  restored (and the schema/ replication ones run on both `pink:` and
  `sqlite:`).
- Result at 10,000 files: catalog transaction stays flat (pink 7.1–9.8 ms,
  sqlite 0.7 ms), pink catalog 3.43 MB with statistics, attach + `count(*)`
  184 ms. Data formats (`.pink`, Parquet) and the S3 data path are
  unchanged.

## 2026-10-24
### Wave 98c — streaming remote scans, a smaller Parquet writer, CSV read-ahead, an optional file cache ✅
- `COPY … TO '….parquet'` and Parquet lake files are now Snappy-compressed
  and dictionary-encoded like DuckDB's. TPC-H SF1 `lineitem` is 238 MB
  instead of 388 MB (DuckDB writes 205 MB) and takes 3.7 s instead of 3.4 s.
  A column whose dictionary grows past its chunk budget falls back to plain
  pages for that chunk. DuckDB reads the files (an interop test checks it).
- A query that aggregates over a remote file or a lake table no longer
  loads the table first: each decoded chunk goes straight into the
  aggregate, folded by the same worker pool the materialized path uses, so
  peak memory is one read-ahead window plus about two chunks per fold
  worker, however large the file is — and the fusion costs no speed.
  Measured with the fold pinned at two workers: the executor never held more
  than `2n+1` chunks, 2 000 rows on a mock-S3 file of 200 row groups (80 000
  rows) and 10 240 rows on a 60-file/180 000-row lake. A lake streams one
  data file at a time — `.pink` files chunk by chunk — so its bound is the
  largest data file, not the largest table.
- No regression, measured as an interleaved A/B against HEAD (5 alternating
  runs each, medians, local SF1): lake Q6 0.340 → 0.337 s, lake Q1
  0.390 → 0.396 s, local `read_parquet` Q6 0.212 → 0.210 s, an HTTP-served
  `lineitem.parquet` Q6 1.317 → 1.315 s (44.27 → 44.64 s with ranged GETs
  forced serial). Every aggregate the binder builds is order-insensitive, so
  chunks may be folded by any worker.
- `read_csv('s3://…')` and `read_csv('https://…')` download 1 MiB ranges a
  window of 8 MiB ahead of the parser, with one request in flight, so
  parsing overlaps the download and memory stays bounded; a transient
  mid-file error is retried.
- An optional on-disk cache keeps remote byte ranges between queries (**off
  by default**): `PINKDB_EXTERNAL_FILE_CACHE=1` (with
  `PINKDB_EXTERNAL_FILE_CACHE_DIR` and `_BYTES`, default 1 GiB) or
  `--file-cache DIR`. Ranges are keyed by URL and ETag, so a changed object
  is never served stale; the directory is size-capped with LRU eviction,
  entries are `0600` in a `0700` tree, and a corrupt entry is a miss.
- Release size: the default `pinkdb-cli` is 4,183,936 B, 63,552 B larger
  than HEAD (the streaming fold is in the engine); the rest is behind the
  `httpfs`/`parquet` features.

## 2026-10-23
### Wave 96a (part 2) — Postgres catalogs and catalog migration ✅
- A lake's metadata can now live in **Postgres**, not just SQLite or the
  object-store layout: `.lake save postgres://user:password@host/db?schema=lake
  data <dir|s3://…>` keeps the same lake, with the two catalog tables in a
  schema you choose and the data files where they were. Publication is one
  database transaction with a per-schema advisory lock, so concurrent writers
  keep the conflict/rebase rules.
- TLS verifies remote servers by default (`verify-full`, with
  `sslrootcert=<pem file>` for a private CA); `sslmode=require` opts into
  encrypt-only, and `disable` is allowed only for loopback hosts. Passwords are never printed —
  every error and command output shows `***`.
- `.lake migrate-catalog <from> to <to> [data <location>]` copies metadata
  byte-for-byte between the object-store, SQLite and Postgres backends in one
  publish scope, verifies it, and retires the source; opening the old location
  says where it moved. Encrypted lakes migrate without their key (sealed blobs
  are copied verbatim), and a non-empty target is refused.
- The Postgres catalog runs through the same shared test suite as SQLite in CI
  against a throwaway `postgres:17` service, and locally when
  `PINKDB_TEST_POSTGRES_URL` is set. The default CLI still links neither
  `rusqlite` nor the Postgres client.

### Wave 96a (part 1) — SQL catalogs: SQLite and split data locations ✅
- A lake's metadata may live in a **SQLite** file (`sqlite:<path>`) while its
  data files stay on local disk or S3/R2: `.lake save <catalog> data
  <dir|s3://…>` records the data location once, and later `.lake
  open`/`attach`/`commit` commands need only the catalog URL. An explicit
  `data <location>` overrides a moved data root for one session.
- Catalogs are a `CatalogStore` seam inside `pink-lake`: the object-store
  layout remains the default, and the SQL backends store the same
  byte-identical manifests, snapshots and commit deltas. Version uniqueness,
  crash safety, time travel, CDC, attach statistics, replication state and
  encryption keep their existing rules.
- The `catalog-sqlite` and `catalog-postgres` features are opt-in
  (`pinklake` combines with them explicitly), and the default binary stays
  lean.

## 2026-10-22
### Wave 91 — Docker image ✅
- pinkdb ships as a container: `docker run -p 5433:5433 -v pinkdb-data:/data
  ghcr.io/openlearnia/pinkdb` runs the server with a durable database on the
  volume; `docker run --rm -it ghcr.io/openlearnia/pinkdb pinkdb` gives the
  CLI. A `lake` tag adds the Parquet lake feature.
- Small (about 37 MB), runs as a non-root user, configurable with
  `PINKDB_*` environment variables, and reports its health to Docker.
- Images are built and published automatically with each release (the
  first images appear with the next release tag).

### Wave 102 (part 2) — `.pink` lakes read only what a query needs ✅
- Querying a `.pink` lake on S3/R2 now reads just the needed columns of the
  row groups that can match the query, with several range requests in
  flight; `COUNT(*)` and similar answers still come from the catalog alone.
- Lakes use one catalog version (7), which records where each `.pink` file's
  directory is, so no extra request is needed to find it.
- A table now always keeps its file format until you change it explicitly.
- Lakes written by earlier pinkdb versions are no longer readable (early
  development: one current format); they are refused with a clear message.

### Wave 102 (part 1) — a new, faster `.pink` file format
- `.pink` files use a new layout (`PINKDB06`): column data in row groups
  with a directory at the end, a checksum per column chunk, and new
  encodings (FastLanes bit-packing, FSST for text, LZ4).
- TPC-H SF1 saved as `.pink` is now 321 MB instead of 713 MB — slightly
  smaller than the same data as DuckDB Parquet — and opens far faster.
- Files from earlier pinkdb versions are no longer readable (pinkdb is in
  early development; there is one current format). They are refused with a
  clear message.

### Wave 98b — fast lakes on S3/R2 ✅
- Lakes on object storage read only what a query needs: lake files fetch just
  the queried columns with concurrent range requests, and whole files
  download in parallel parts. On R2, TPC-H Q6 over a Parquet lake went from
  23 s to 6 s.
- Encrypted lakes keep reading whole files (each file is one sealed
  container).
- `.lake attach` reports the requests and bytes it transferred.

### Wave 98a — fast remote Parquet ✅
- Reading Parquet from S3/R2/HTTPS is dramatically faster: TPC-H Q6 on a
  196 MB file went from 6 min 46 s to about 4 s, on par with DuckDB 2.0, and
  `count(*)` is answered from the file footer (29 s → 0.6 s).
- pinkdb now fetches only the columns a query needs, in one request per
  column chunk instead of many small ones, with several requests in flight
  while earlier data is decoded. `PINKDB_HTTPFS_PARALLELISM` sets how many
  (default 8; 1 restores the old serial reads).
- `EXPLAIN` shows how many columns a file scan reads (`4 of 16 columns`).

### Wave 94 — `database.table.column` references ✅
- Columns can be qualified with their database: `SELECT db1.t.a FROM db1.t`,
  and `db1.t.*` in a SELECT list. It works in every clause, in `UPDATE … SET`
  targets, and through the current database (`SELECT main.t.a FROM t`).
- Joining two tables with the same name from different databases now works;
  a plain `t.a` there is reported as ambiguous instead of silently picking
  one of them.
- An alias hides the table's own name, as in Postgres and DuckDB: with
  `FROM db1.t AS x`, write `x.a` (the error says so).

### Wave 89 — lake replication jobs ✅
- A lake can now keep copies of another lake's tables, or of a database in
  your session, in sync: `.lake replicate create <dest> from <source>`, then
  `.lake replicate catchup` or a background `.lake replicate start`.
- Each table syncs the cheapest safe way: by a timestamp/ID column you name,
  by replaying the source lake's changes, by primary key, or by a full copy —
  and falls back to a full copy whenever it can't be sure.
- One failing table doesn't stop the others; a job never overwrites a table
  it doesn't manage; a table dropped or excluded on the source is dropped from
  the copy; a recreated source table is re-copied from scratch.
- `.lake replicate status` and `tables` show how far behind each job and
  table is and the last error. Copied tables are ordinary lake tables (time
  travel, attach and changes all work).

### Wave 88 — mixed-format lakes ✅
- One lake can hold tables in different formats: `.lake format parquet for
  main.events` picks a table's format, and `.lake rewrite <dir> <table>
  format <id>` converts an existing table in one step.
- A format you choose explicitly is remembered in the catalog ("pinned"), so
  later saves and appends keep it. Tables written with the build's default
  still migrate to the saving build's format, as before.
- A table in a format this build can't read no longer blocks the whole lake:
  the other tables open, the CLI warns about the skipped one, reading it
  gives a clear error, and saving keeps it unchanged.
- Every format (`.pink`, Parquet) now passes the same contract test suite.
  It caught two bugs, now fixed: saving a DECIMAL or INTERVAL table crashed
  instead of giving an error, and very large INTERVAL values made
  `.lake attach` refuse the lake.

## 2026-10-21
### Wave 87 — lake data formats are pluggable; Parquet lakes ✅
- A lake's data files are now written by a registered **format provider**:
  `.pink` is built in, and the new `parquetlake` CLI feature adds
  **Parquet**.
- `.lake format parquet` (or `Lake::set_write_format`) picks the format for
  the next saves; attach, commits, S3, encryption and maintenance work the
  same for every format. INTERVAL columns are refused in Parquet (it cannot
  store microseconds exactly).
- A lake that names a format this build doesn't have is refused with a clear
  error; existing lakes are unchanged.

### Wave 86 — extensions can provide table functions and COPY formats ✅
- New **extension packages**: a host registers a named bundle of scalar
  functions, table functions/file readers and `COPY` formats; `LOAD
  EXTENSION '<name>'` turns it on for one database, atomically (a name
  clash refuses the whole package).
- Durable stores replay package usage after a reopen, and new log records
  that read through a package re-load it automatically.
- The Arrow extension can now be offered this way
  (`pinkdb_arrow::register_package()`, then `LOAD EXTENSION 'arrow'`), in
  addition to its existing always-on registration.

### Wave 85 — pinkdb in the browser (Playground) ✅
- The docs site has a **Playground**: the real engine compiled to wasm,
  running in your tab with no server, with example queries and a result
  grid.
- Keep your database in Memory, IndexedDB or OPFS; writes autosave, and you
  can Export/Import `.pink` files (the same format the CLI uses). Damaged
  files are refused without touching your saved data, and a second tab
  opens a read-only copy.
- New public `PinkDb::from_pink_bytes` loads a database from snapshot bytes.

### Wave 84 — query execution profiles ✅
- `execute_profiled` (embedded `PinkDb` and `Connection`) returns each
  statement's timings — parse, bind, optimize, execute, WAL log, total —
  with its rows, columns, kind and transaction state. Results and WAL bytes
  are unchanged, and plain `execute` costs exactly what it did.
- Server protocol **v10**: a client can turn profiling on per connection and
  receives one profile after each statement (`Client::set_profiling`,
  `Client::execute_profiled`). v9 clients are refused at the handshake.
- The CLI's `.timer on` line now shows the phase breakdown, locally and
  against a server.
- C API: `pinkdb_set_profiling` and `pinkdb_result_profile_*` accessors.

### Wave 83 — procedures can COMMIT, catch errors, and `DO` blocks ✅
- A procedure called **outside** a transaction can now `COMMIT` and
  `ROLLBACK` in its body (Postgres-style): long batch jobs keep the progress
  they committed if they later fail. Inside `BEGIN … COMMIT` this is refused
  ("started at a different scope"); a plain `CALL` stays all-or-nothing.
- `BEGIN … EXCEPTION WHEN OTHERS THEN … END` blocks catch errors: the
  block's work is undone and the handler runs with `SQLERRM`.
- New `DO $$ BEGIN … END $$` statement runs an anonymous block once.
- Durable stores log a committing procedure's work as self-contained
  segments, so replay is exact even if the procedure is later changed or
  dropped.
- Fixed: quoted variable names (`DECLARE "my var" …`) work, including
  non-ASCII names.

### Wave 81 — JavaScript procedures run (quickjs-ng) ✅
- With the `js` feature (+1.1 MB), `CREATE PROCEDURE … LANGUAGE JAVASCRIPT`
  now works: the body runs in quickjs-ng with `print`, `db.exec`,
  `db.query`, `db.value` and a seeded `db.random()`; `?` parameters are
  bound as values.
- Same guarantees as SQL procedures: a `CALL` is all-or-nothing, transaction
  control is refused, and durable stores replay calls exactly (no clock, no
  `Math.random`, no I/O, no `eval`).
- Runaway scripts stop cleanly at step, host-call, memory and stack limits;
  syntax errors are reported at `CREATE` time.
- `print` lines come back with the call and the CLI shows them (not yet over
  the server protocol or FFI). Native targets only for now.

### Fix — DOUBLE NaN comparison semantics now match DuckDB ✅
- `NaN = NaN` is true, NaN equals no other value and sorts above every other
  double (`+inf` included), and `-0.0 = 0.0` — in filters, ORDER BY (NaN last
  ascending, first descending), MIN/MAX (`max` over a column with a NaN is
  NaN), range joins and window ordering. Scans used to treat NaN as equal to
  every value (`WHERE d = 0.0` matched NaN rows), disagreeing with the hash
  index; an index once again changes only the plan, never the result.
- Parquet row-group pruning keeps a DOUBLE row group for `>`/`>=`/`= NaN`
  (its statistics never cover NaN); ORDER BY over NaN keys now takes the fast
  byte-key sort instead of falling back.

### Index persistence in `.pink` (wave 79, part 2) ✅
- Index definitions are now stored **in the `.pink` file** (v5 flags bit 3,
  about 30 bytes per index), so a checkpoint publishes indexes atomically with
  the data and no longer re-logs them into the fresh WAL.
- The hash maps are rebuilt from the table's rows when the file is opened, so
  an index can never disagree with the data it indexes. (Storing the maps
  themselves was measured: no faster to open once verified, and 2–4 MB larger
  per million-row index.)
- Stores without indexes are byte-identical to before; files with indexes
  need this build or newer (older builds refuse them with "upgrade pinkdb").
- Still open in wave 79: using indexes/zone maps for range predicates.

### Security hardening — pass 7 (final): terminal escaping, crypto/WAL/loader/FFI hardening ✅
- A single terminal sanitizer now covers **every rendered identifier** (all
  result variants, column names, catalog/lake output, errors), closing ANSI/OSC
  injection while preserving legitimate UTF-8.
- Engine containers v4 and lake containers v2 use a 128-bit nonce base (96-bit
  derived GCM nonces); WAL v4 authenticates its publish block; tampering is
  rejected. Legacy formats still open, and are rewritten on the next save.
- The extension loader validates zero-arg types, owns its collector, refuses
  post-init registration, bounds VARCHAR copies and ignores non-VARCHAR frees;
  the FFI bounds `value_count` and pins `PinkValue`'s layout. The sync transport
  enforces a per-frame deadline; the CI installer and Docker images are pinned.

### Security hardening — pass 6: filesystem privileges, file modes, lake binding ✅
- `COPY … TO` now needs `ADMIN` and is **off by default on the server**; with
  the opt-in it is confined to a configured root, refuses traversal/symlinks,
  and writes atomically as a `0600` temp file. Table-function scans likewise
  require `ADMIN` in authenticated sessions, including nested/CTE shapes.
- WALs, checkpoints, lock files and lake artifacts are created `0600` (lake
  directories `0700`), and existing engine/lake files are tightened on open.
- Lake catalogs are now **v6**: manifests are bound to `(lake_id, version)`,
  a separate rollback marker detects rollbacks, and cross-lake/foreign-manifest
  substitution is refused. Local tags now include `(dev, ino)` so a
  same-size/same-second replacement can no longer slip past the CAS.


### Wave 78 — server warehouse browsing UI ✅
- A feature-gated (`ui`, default off) read-only console served by
  `pinkdb-server`: embedded self-hosted assets, paged row previews, catalog and
  row-count/size JSON endpoints. Built on **axum + tokio + maud** (a documented
  async carve-out; the console owns its runtime and every engine read runs on a
  bounded blocking pool).
- Fails closed: opt-in, loopback-only, non-loopback binds refused, and a store
  with RBAC users refuses to start without Basic auth (per-user grants) or a
  bearer token. The default server binary grows by only ~128 B.

## 2026-10-20

### Wave 90 — tag-driven release train + `install.sh` ✅
- A `v*` tag now builds and publishes 36 smoke-tested artifacts (24 CLI variants ×
  12 server variants) for Linux x86_64/aarch64 and macOS arm64/x86_64, with an
  immutable `SHA256SUMS` manifest and a single publish job that refuses to overwrite
  an existing release.
- Extensions build per platform on the same tag and fan in to one signed catalog
  publish (plus one Worker deploy), so releases and the extension repository stay in
  lockstep.
- `install.sh` installs latest or a pinned version with checksum verification,
  variant selection, atomic install to `~/.local/bin`, and no sudo; the docs site has
  an install page, and the CLI reports `--version`.

### Security hardening — pass 5: auth DoS/timing/hash policy + resource bounds ✅
- Logins no longer run Argon2id while holding the store lock; failed attempts
  are rate-limited per connection and per IP, and concurrent verifications are
  capped — credential-free clients can no longer starve writers or allocate
  GiBs of transient memory.
- Unknown users pay the same dummy Argon2id cost as wrong passwords (no timing
  enumeration), and `PASSWORD_HASH` values are checked against an Argon2id
  work-factor floor **and** ceiling.
- The server enforces a 30 s statement timeout, 10M-row and 256 MiB total
  result caps (configurable), and refused connections no longer spawn a thread.

### Index DDL durability (minimal half of wave 79) ✅
- `CREATE INDEX` / `DROP INDEX` now survive reopen **and** checkpoints: the DDL
  is WAL-logged as canonical SQL (quoting preserved), and each checkpoint
  re-logs the live definitions into the fresh WAL generation. No `.pink`
  format change — indexes are still rebuilt from data at open, and flags bit 3
  remains reserved for the full persistence work.

### Security hardening — pass 4: index lifecycle ✅
- Indexed lookups are now **always re-checked**: the optimizer keeps `Filter`
  above `IndexScan`, and any INSERT/UPDATE/DELETE/COPY marks indexes stale
  (rebuilt lazily at the next statement boundary) — a stale index can no longer
  return wrong rows.
- Out-of-range row ids in an index are skipped instead of panicking (closing a
  remote DoS), probe keys are canonicalized to the column's type (BIGINT/
  DOUBLE/DECIMAL + integer literals now find rows), and `DROP COLUMN` reindexes
  correctly.
- Indexes remain **session-lifetime**: the hash maps are not stored in `.pink`
  and the DDL is not replayed from the WAL yet, so reopening a store currently
  loses its indexes (queries fall back to correct but slower scans). Index
  persistence is wave 79.

### Security hardening — pass 3: async TLS downgrade ✅
- `--transport async` now **refuses** `--tls-cert`/`--tls-key` at startup with a
  clear error instead of silently serving plaintext. TLS remains available on
  the default sync transport (or via a TLS terminator in front); native async
  TLS is recorded as a follow-up.

### Security hardening — extension repository signing ✅
- The extension catalog is now **Ed25519-signed** (`index.json.sig`), and
  `.install` verifies it against a key **pinned in the binary before it
  fetches or loads any artifact** — a compromised repository origin can no
  longer serve a matching hash for its own payload. Unsigned catalogs are
  refused unless an explicit dev override is set.
- `publish.py` and `verify.sh` sign/verify; the publish workflow signs with the
  `PINKDB_EXT_SIGNING_KEY` secret and adds a verification gate. No new default
  dependencies (+~319 KiB only in `repo` builds).

### Security hardening — pass 2: authentication gate ✅
- The login gate is now evaluated **per statement inside a batch** (both
  transports, prepared statements included): an unauthenticated `CREATE USER;
  GRANT ADMIN` batch can no longer promote itself — the created user exists
  without admin rights.
- "Empty catalog ⇒ trusted" now applies only to embedded/trusted sessions; an
  authenticated session whose catalog is emptied is denied, not promoted.
  Role-granted ADMIN is honored consistently, and the server's startup text no
  longer claims there is no authentication.

### Security hardening — pass 1: extension loading ✅
- **`LOAD EXTENSION` is off by default** everywhere (engine, CLI, C ABI,
  bindings); enable it explicitly with `--allow-load-extension` /
  `PINKDB_ALLOW_LOAD_EXTENSION`, the new C-ABI switch, or the binding opt-ins.
- **Opening a database file can no longer execute native code recorded in its
  WAL**: replay refuses a recorded `LOAD EXTENSION` unless the library is
  already mapped in the process or the new replay opt-in + extension root
  allow it — otherwise it is a hard, actionable error (the audit's reproduced
  attack is now a regression test, marker absent).
- Extensions are path-confined: no bare loader-path names, no `..`, no
  symlinked libraries, plus an optional `--extension-root` on server and CLI.

### Security hardening — low-severity batch ✅
- Redacted the login password from the server protocol's `Debug` output (L-2)
  and the SigV4 canonical request / session token from `Signature`'s (L-11).
- Constant-time key-material comparison in the encryption layer (L-3).
- Every lake commit-delta path is validated, and local storage refuses
  absolute/`..` paths (L-12); the stale extracted SigV4 copy is deleted.
- Checked arithmetic in all `.pink` RLE decoders (32-bit overflow, L-13).
- The Cloudflare API token is scoped to the deploy step only (L-14).
- The four `.lake` subcommands that bypassed `PINKDB_KEY` now use the keyed
  constructor; the S3 test harness pins image digests.

## 2026-10-19

### Wave 75 — core `httpfs` extension (remote files for every format) ✅
- New shared transport `pinkdb-httpfs`: the lake's SigV4 S3 client extracted
  behavior-identically, plus https GET and **byte-range reads**, with
  `Fetcher` as the contract.
- Every registered format reads remote paths: CSV (whole-object), **Parquet
  via a ranged `ChunkReader`** (footer/row-group pages — no whole-file
  downloads when the size is known), Arrow (whole-object, documented).
  Claims are by extension regardless of scheme; `--features
  parquet,arrow,httpfs` composes, and default binaries stay transport-free.

### Wave 77 — SQL macros ✅
- `CREATE [OR REPLACE] MACRO … AS expr` and `AS TABLE SELECT …`, with
  `DROP MACRO [IF EXISTS]`, parameter defaults and named arguments, expanded
  by **bind-time AST substitution** (no dynamic SQL — call-site typing,
  invoker rights and injection safety hold). Usable in expressions, FROM,
  aggregates, windows, CTEs, prepared statements, views and other macros;
  recursion is refused cleanly.
- `SHOW MACROS` / `DESCRIBE MACRO`; macros persist in `.pink` (flag bit 6,
  loaded before MVs) and are WAL-logged DDL; invocation keeps table grants
  enforced. +98 KB on the default binary.

### Wave 70 — core `parquet` extension (read-side) ✅
- Parquet reads with the DuckDB surface: `read_parquet`/`parquet_scan`
  (globs, directories, multi-file, `union_by_name`, hive partitions,
  `filename`/`file_row_number`), four metadata functions, a replacement scan
  (`FROM 'x.parquet'`), projection pushdown and row-group-statistics filter
  pruning with visible counters.
- Landed with the **unified file-format surface**: one read path
  (`TableSource`) shared with arrow, one grammar (`Function` + `Path`), the
  COPY sink kept separate, and logged file reads replay by re-reading
  (DuckDB's rule). `parquet` is opt-in (+1.1 MiB); the default binary pays
  ~18 KB for the surface.

### Wave 71 — core `arrow` extension (Arrow IPC) ✅
- New engine **file-format extension surface** (`pinkdb::filefmt`): arrow
  files work through `FROM 'x.arrow'`, `read_arrow`/`read_ipc`/`read_feather`
  and `COPY … (FORMAT arrow)`, with durable replay re-reading files and lean
  builds refusing everything with a precise error.
- `arrow` is an **opt-in CLI feature** (+≈3.0 MiB; the default binary grows
  only 22 KB for the seam). Round-tripped against pyarrow in both directions;
  DuckDB has no Arrow support to diff against (documented). The Arrow C Data
  Interface is pinned as a follow-up. `.pink` remains the native format.

### Wave 54 — core `math` extension ✅
- Seventeen functions via the Rust SDK: `exp`/`ln`/`log`/`log2`/`log10`, the
  trigonometric family, `degrees`/`radians`/`pi`, and the first **volatile**
  function — `random()` refuses to run in any WAL-logged statement (replay
  could diverge), while reads work normally. `sqrt`/`pow`/`floor`/`ceil`/
  `sign`/`greatest`/`least` remain core built-ins.
- SDK gained ranged arity (optional/variadic — linked-only by construction),
  and integer/DECIMAL arguments now widen into DOUBLE parameters like DuckDB.
  `math` joins json+datetime as a **default feature**; +25 KB default.

### Wave 69 — prepared statements in the Python and Go bindings ✅
- Both bindings gained idiomatic prepared-statement APIs over the FFI: Python
  `db.prepare(sql)` → `Statement.execute(*params)`, Go `db.Prepare(sql)` →
  `Stmt.Execute(args...)` — typed parameters, NULLs, reuse and clean close
  semantics, with native error strings passed through unchanged.
- No C-ABI changes; the bindings also caught up on the RBAC result kinds
  (17–23). Python 56 tests, Go 38 (with `-race`).

### Wave 56 — core `datetime` extension ✅
- Ten DuckDB-pinned datetime functions over naive `DATE`/`TIMESTAMP`:
  `strftime`, `strptime`, `epoch`/`epoch_ms`, `dayofweek`/`dayofyear`/`week`,
  `last_day`, `age`, `make_date` — hand-rolled civil-date and format engines,
  zero new dependencies.
- The extension ABI gained an additive **INTERVAL** value type so `age` returns
  a real interval; `datetime` joins `json` as a **default CLI feature**
  (`--no-default-features` for lean). +40 KB on the default binary.

### Wave 53 — core `string` extension ✅
- Ten DuckDB-pinned string functions built with the Rust SDK: `split_part`,
  `reverse`, `left`/`right` (now parse unquoted), `strpos`,
  `starts_with`/`ends_with`, `regexp_extract`, `regexp_matches`, plus pinkdb's
  `regexp_match` (a pinned divergence — DuckDB has no such function) with
  DuckDB's full option surface.
- `trim`/`ltrim`/`rtrim`/`replace` stay core built-ins (extensions can't shadow
  them). Loaded at runtime; the default CLI binary grows **0 bytes**.

### Wave 52 — core `json` extension ✅
- Six DuckDB-pinned JSON functions over `VARCHAR` — `json_extract`,
  `json_value`, `json_valid`, `json_array_length`, `json_object`,
  `json_array` — with a hand-rolled parser (zero new dependencies) that matches
  DuckDB on the messy corners: trailing commas, `NaN`/`Infinity`, duplicate
  keys, canonical number rendering, escapes.
- `json` is a **default CLI feature** (`--no-default-features` for the lean
  binary); the functions register through the linked registry, so durable
  replay resolves them automatically.

### Wave 51 — Rust extension SDK + volatile flag ✅
- New `ext-sdk/rust`: declare an extension once (`pinkdb_extension!`) and get
  both the native ABI v1 cdylib exports and the wasm/all-target linked
  registration from safe Rust — typed args, `Option` NULLs, `Result` errors,
  no `unsafe` in author code.
- Additive **ABI v1 volatile flag**: functions can declare themselves
  nondeterministic, and the engine refuses to call them in any statement it
  WAL-logs (replay could diverge) with an explicit error; deterministic
  extensions are unchanged.

- **Fixed:** the SDK's ABI exports (`pinkdb_extension_abi_version` /
  `_name` / `_init`) are now **opt-in** — `pinkdb_abi_exports!()` (or the
  `abi-exports` feature for dual-artifact crates, as `extensions/string`
  does) — because emitting them from linked-only extensions caused duplicate
  symbols on Linux linkers. Cdylib authors add the macro; linked-only crates
  need nothing.

### Wave 50 — performance pass ✅
- High-cardinality `GROUP BY` / `COUNT(DISTINCT)` got a typed fast path (no
  per-row values, chunked output, adaptive one-shot reserve): `group_id`
  15.8 → 4.4 ms and `distinct_id` 15.6 → 4.6 ms — both now ahead of the DuckDB
  reference on the same corpus.
- Byte-slice hashing went word-wise: `distinct_lowcard` 3.8 → 3.1 ms and
  `join_probe` 5.3 → 4.6 ms.
- Standing battery total **101.5 → 75.7 ms (−25%)**; all correctness gates
  unchanged.

## 2026-10-18

### Wave 48 — RBAC: users, roles and enforced grants ✅
- New **users and roles** hashed with Argon2id: `CREATE USER`/`CREATE ROLE`,
  `GRANT`/`REVOKE` (table `SELECT`/`INSERT`/`UPDATE`/`DELETE`, database `CREATE`,
  global `ADMIN`), persisted in the catalog + WAL — plaintext passwords never
  hit disk.
- The network server gains **session login** (protocol v9) and **per-statement
  enforcement**: an empty catalog trusts connections with a loud startup warning;
  once a user exists every statement is checked against their grants. Embedded,
  FFI and wasm use stays trusted by design.
- Remote CLI gains `--user`/`--password` (or `PINKDB_PASSWORD`); without the
  `tls` feature, login passwords cross the wire in cleartext — documented.

### Wave 49 — extensions on wasm ✅
- Extensions now work on **every target** through a new **linked registration**
  model: extensions register through a safe-Rust API and feed the same engine
  registry as native ABI v1 extensions. On wasm (where `dlopen` cannot work),
  `LOAD EXTENSION '<name>'` resolves a registered name; unknown names fail with
  an actionable error.
- The **JS / wasm-bindgen host contract** is pinned — surface, SQL↔JS value
  mappings and sandbox rules, documented in `docs/wasm-extensions.md` — with no
  new dependencies added.
- Native CLI grows ~3.7 KB; the wasm dependency tree gains nothing.

### Wave 46 — network server TLS ✅
- Opt-in `tls` feature: the server serves the same protocol over
  **rustls**-encrypted TCP (`--tls-cert/--tls-key`), and the CLI's remote mode
  gained `--tls`, `--tls-ca-cert` and `--tls-insecure`. Certificate problems
  refuse startup with precise messages; handshake failures never disturb the
  accept loop; the default build and dependency graph are unchanged.
- Still **no authentication** — TLS encrypts the transport only, which the docs
  and startup warning state explicitly.

### Wave 47 — async I/O at the boundary ✅
- `pinkdb-server` gains an opt-in **`async-server`** transport: a tokio
  listener with one task per connection, engine calls on the blocking pool,
  and **byte-identical protocol frames** to the synchronous server (same
  client works unchanged). `--transport sync|async`.
- The "zero async" rule now has its documented carve-out: async lives only at
  the server I/O boundary — the executor, storage and WAL stay synchronous —
  and the default build links no runtime (+0.38 MB only with the feature).

### Wave 82 — Rust toolchain upgrade ✅
- All crates now require **Rust 1.97** and compile as **edition 2024**; CI's
  MSRV job pins 1.97 (was 1.89 core / 1.95 pink-lake), and the wasm check
  passes again.
- Modernized the code where the new edition/compiler asks for it — let-chains
  (~60 collapsed conditionals), `unsafe(...)` attribute syntax, reserved
  `gen` renames — with semantics unchanged (duckdiff still 171/171).

### Wave 44 — stored procedures ✅
- **SQL procedures**: `CREATE [OR REPLACE] PROCEDURE p(a INTEGER, …) AS $$ … $$`
  (typed `DECLARE`/`SET`, `IF/ELSIF/ELSE`, `WHILE`, nested blocks, `RETURN`,
  `RAISE`), `DROP PROCEDURE [IF EXISTS]`, `CALL p(args)` — parameters and
  variables are bound as **values, never spliced SQL**, so injection-shaped
  input is inert by construction. `CALL` is statement-atomic, works in
  transactions, is persisted per database and replays exactly from the WAL.
- **JavaScript procedures**: the sandbox **contract** ships (`pinkdb-js`:
  host API, denied-intrinsics determinism policy, conversions) with a clean
  refusal — the engine binding (quickjs-ng, ~1.1 MB) is the queued follow-up
  (wave 81).

### Wave 74 — lake naming sweep ✅
- The lake builds are now **`pinklake`** (the `.pink`-file lake; formerly
  `lake-lite`) and `lake` (the full lake; later removed),
  in cargo features, help text, tests, CI, README and the docs site.
- **`s3` is pure storage**: it implies no lake build — combine it
  explicitly (`pinklake,s3`; `--features s3` alone has no `.lake` commands
  and says so). No old aliases survive.

### Wave 80 — materialized-view refresh policies ✅
- **Manual**: `REFRESH MATERIALIZED VIEW name` (and `REFRESH name`, plus
  `REFRESH MATERIALIZED VIEWS` refreshing every view of the database
  all-or-nothing); `SHOW MATERIALIZED VIEWS` lists schedule, last refresh
  time/count and staleness.
- **Scheduled**: `CREATE MATERIALIZED VIEW v REFRESH MANUAL | EVERY INTERVAL
  '5 minutes' | ON COMMIT AS SELECT …` — enforced **daemonless** at statement
  boundaries (embedded and `pinkdb-server` alike; never mid-transaction), with
  automatic refreshes WAL-logged and replay-exact.
- Fix: MV definitions are now WAL-logged and persisted — previously a durable
  reopen silently lost them.

### Wave 68 — inlined small writes & CDC ✅
- Tiny commits (≤ 10 rows by default; per-table limits, `PINKDB_LAKE_INLINING_ROW_LIMIT`)
  live **in the catalog metadata** instead of new data files — scans, stats,
  time travel, partitions and encryption all see them correctly. `.lake flush`
  materializes them into real files and keeps the history.
- **CDC reads**: `.lake changes` (and the library's
  `table_insertions`/`table_deletions`/`table_changes`) replay each version's
  deltas as `snapshot_id`/`rowid`/`change_type` rows. Updates read as
  delete + insert (documented).
- `.lake add_data_files` registers existing lake files into a version with
  exact schema and statistics checks — encrypted lakes included.

### Wave 42 — hash indexes ✅
- `CREATE INDEX [IF NOT EXISTS] name ON t (col)` and
  `DROP INDEX [IF EXISTS] name` (per-database): a single-column **hash index**
  turns `col = literal` lookups into an `IndexScan` instead of a full scan —
  results are identical either way. `.pink` persistence is queued as the
  follow-up (wave 79).

### Wave 43 — materialized views ✅
- `CREATE MATERIALIZED VIEW … AS SELECT`, `REFRESH [MATERIALIZED VIEW]` (full
  recompute) and `DROP MATERIALIZED VIEW [IF EXISTS]`: stored as a queryable
  table plus its refresh query, per-database, WAL-logged and persisted.

### Engine fixes and performance
- Bug-review remediation: server ETag validation, both FFI result-building
  paths guarded, VARCHAR string-offset-limit refusal, attach row-count
  re-checks. Performance: COUNT(*) dead-work elimination and fast DISTINCT for
  integer/date/timestamp keys.

## 2026-10-17

### Bug-review remediation — P1 + P2 ✅
- **P1 (12)** — Window filter pushdown (wrong ranks), mixed-operand JOIN keys
  (panic), `ALTER … DEFAULT` depth/parameter guards, JOIN/UNION plan depth,
  RANGE-frame NULL keys, plaintext WAL mid-file corruption (silent data loss),
  atomic + guarded `save`, legacy-head lake commits (unreadable catalog),
  delete-file count overflow, plaintext staging copies, and the server's
  `ExecutePrepared` / `Rows` allocation caps.
- **P2 (engine)** — `||` on the right of `=`/`LIKE`/`BETWEEN`, SELECT-alias
  shadowing within one list, set-op `NULL` arms, checked decimal/temporal
  overflow, an optimizer index guard, INTERVAL hashing, lazy CASE/COALESCE,
  WAL directory fsync + checked LSN/decoders, the COMMIT conflict set, and
  unsupported WAL container versions.
- **P2 (lake/S3/CLI/server/bindings)** — monotonic lake `schema_version`,
  partition error names, container-version classification, conditional-retry
  guard, SigV4 whitespace, CLI fail-closed key/backoff/partition guards,
  loopback parsing, atomic + Windows-safe repo files, terminal-escape output
  escaping, checked prepared handles, an FFI unwind guard, DECIMAL/INTERVAL/
  `ALTERED_TABLE` in the Python and Go bindings, a client batch cap + socket
  timeouts, off-thread over-limit rejection, and documentation drift.
- No deferred items: the last two (a `.pink` VARCHAR chunk over the `u32`
  string-offset limit is refused at serialization; attach recomputes the row
  count from the deduped delete layout) are fixed too.

### Wave 67 — lake maintenance ✅
- **Compaction**: `merge_adjacent_files` publishes one new version referencing
  consolidated files — groups of delete-free files sharing partition values and
  schema epoch (≥2 members) merge into one; source files are **never touched**
  (bytes and mtimes pinned by tests) and row counts/zone maps stay exact.
  `rewrite_data_files` materializes deletes above a threshold.
- **Retention**: `expire_snapshots` removes old snapshots + commit deltas
  (explicit versions, `older_than`, or `retain_last` — never the current
  version); `cleanup_old_files` / `delete_orphaned_files` delete only
  unreferenced `data/` objects, behind a 2-day age gate and an explicit
  dry-run/confirm; encrypted lakes included.
- **Introspection**: `.lake files / table_info / options / settings` (+
  `merge`, `rewrite`, `expire`, `cleanup`, `orphans`).

### Wave 57 — lake encryption at rest ✅
- With the `encryption` feature, a keyed session (`PINKDB_KEY`, consistent with
  the engine) seals **every lake object** — catalogs, snapshots, commit deltas,
  data files, delete files — as `PNKLAKE1` **AES-256-GCM** containers under one
  Argon2id derivation per session. A `Storage`-level decorator means commits,
  deletes, time travel and retry paths are covered with no per-call-site code.
- The manifest byte-identity invariant survives (seal once, publish the same
  ciphertext); **S3 conditional-write CAS is unchanged**;
  statistics-fast attach on encrypted lean lakes still decodes zero data files.
- Wrong/missing keys and tampered containers fail closed with authenticated
  errors (never “corrupt file”); plaintext lakes stay byte-identical and are
  refused by keyed sessions. Encrypted lake attach reads whole objects in
  stage 1 (segment-level reads documented as the follow-up).

### Wave 65 — lake schema evolution ✅
- `ALTER TABLE` on writable-attached lake tables — `ADD COLUMN … DEFAULT`,
  `RENAME COLUMN`, `DROP COLUMN` (plus the engine-wide `ALTER TABLE` support
  behind it: defaults, `NOT NULL` rules, `DESCRIBE` defaults, WAL replay) —
  published as **metadata-only** catalog schema versions (format now **v5**):
  old data files are never rewritten; they project through rename chains,
  missing columns materialize their defaults, and dropped columns keep their
  bytes (row positions unchanged — delete files stay valid).
- **Time travel reads each version with that version's schema and names**;
  writes across versions keep working through the projection.
- Limits (documented): `RENAME TABLE` through a lake commit is refused with a
  `.lake save` hint; no `ALTER COLUMN TYPE` / `SET|DROP DEFAULT` /
  `ADD CONSTRAINT`; read-only attachments refuse `ALTER`.

### Wave 66 — lake partitioning & pruning ✅
- Lake tables can be **partitioned**: identity values or `bucket(N)` labels are
  recorded per file, commits write **one data file per partition value set**,
  and spec changes (`.lake partition … by (…) | clear`) never rewrite old
  files.
- Every v4 data file publishes a CRC-protected **zone map** in the catalog;
  the planner prunes whole files for exact predicates —
  `WHERE region = 'x'` reads only that partition's file, and disjoint ranges
  read zero.
- `COUNT`/`MIN`/`MAX` honesty rules are preserved (fall back once positional
  deletes exist); pruning stays correct with deletes and across time travel.
- Limits (documented): bucket labels are pinkdb's own hash (no cross-engine
  numbers, no bucket-value pruning); `year/month/…` transforms are refused by
  name.

### Wave 73 — docs catch-up, changelog & site wiring
- Follow-up: a **SQL dialect reference** page — statements / queries / types
  tables plus the known-divergences list (correlated subqueries, USING/NATURAL
  joins, three-part references, CTE `AT` handling) — added under Reference.
- Website docs brought current through waves 58–64: **SQL** (prepared statements
  with the `prepare` → `bind` → `execute_bound` flow, DECIMAL/INTERVAL with the
  DuckDB promotion rules, declarative keys + FK→PK elimination, full/multi-way
  joins, the new function set, `EXPLAIN`/`SHOW`/`DESCRIBE`, set operations),
  **persistence** (`.pink` v5 with the constraint section and type tags; WAL v4
  touched sets; optimistic commits and `PINKDB_TXN_RETRIES`), **lake**
  (incremental commits with the 1M-row numbers and DuckLake retry knobs, writable
  attach, `AT (VERSION/TIMESTAMP)` time travel, per-version records, catalog
  `format_version` 4), **CLI** (new `.lake` commands, author/message, retry env
  vars), **server** (protocol v6, `Prepare`/`ExecutePrepared`/`Prepared` frames),
  plus landing/roadmap/API/guide updates.
- New **Changelog** docs page generated from this file
  (`website/tools/make-changelog.py`), linked from the sidebar, footer and
  roadmap page.
- CI's docs job now regenerates the changelog fragment + the site and **fails on a
  stale diff** — regenerate-and-commit is enforced; `website/README.md` documents
  the tooling.

### Wave 63 — incremental lake writes ✅
- Lake commits now **append**: each commit writes one new immutable data file
  per table (with its exact row count in the catalog) and never rewrites a
  published file — a 1M-row lake committed 4 rows in **7.6 KB / 0.54 s with 0
  bytes rewritten** (vs a 2.6 MB full save).
- **DELETE / UPDATE** write positional delete files applied at read time
  (merge-on-read); every published version also writes a small commit delta
  manifest.
- **Writable attached lakes**: `.lake attach <dir> write`, mutate with normal
  SQL, `.lake commit` publishes the diff; guarded `.lake detach`; durable
  sessions replay commits idempotently after a crash.
- **Concurrent-writer retry** with DuckLake's settings and defaults
  (`max_retry_count` 10, `retry_backoff` 1.5, `retry_wait_ms` 100 — rebase on
  conflict, bounded, clean failure).
- Fixes after landing: the feature-less build message and the S3 Docker
  expectations updated for the new catalog format (`format_version` 4).

### Wave 64 — lake time travel & snapshot metadata ✅
- Query any published lake version: `SELECT … FROM t AT (VERSION => n)` /
  `AT (TIMESTAMP => ts)` on attached lake tables, with DuckLake-pinned
  resolution (timestamp = newest snapshot at or before it, inclusive).
- **Per-version records**: `.lake snapshots` lists time, author, message,
  `(tables, rows)` and a change summary; `.lake save … author '…' message '…'`
  attaches commit metadata.
- Statistics fast paths work per version: `COUNT`/`MIN`/`MAX` over an older
  version decode no data files.
- `ducklake_default_version` deliberately not mirrored (probed: it selects a
  new-catalog storage format, not a read default).

### Wave 41 — primary / foreign keys ✅
- Declarative `PRIMARY KEY` / `UNIQUE` / `NOT NULL` / `REFERENCES` in column
  and table form (`CONSTRAINT` names; `RELY` / `[NOT] ENFORCED` accepted and
  dropped), validated at DDL time like DuckDB.
- Constraints are **metadata-only** — writes never enforce them; `DESCRIBE`
  reports `PRI`/`UNI` markers and nullability.
- Optimizer: conservative **FK→PK join elimination** (declarations are
  trusted, not verified — documented).

### Wave 62 — DECIMAL and INTERVAL types ✅
- Full DECIMAL(38) and INTERVAL support: literals, arithmetic with DuckDB's
  promotion rules, comparisons, sums, CSV import/export, date ± interval.
- `.pink` v5 (new type tags), server protocol v6, FFI text cells for both.

### Wave 59 — prepared statements ✅
- `?` placeholders with bind-time type inference; engine `prepare` → `bind` →
  `execute`; server protocol `Prepare`/`ExecutePrepared`; FFI
  prepare/bind/execute. (Python/Go wrappers: wave 69, queued.)

### Process notes (no user-visible change)
- DuckLake feature-gap audit (live probes) queued as waves 63–68; extension
  waves 70–72 queued (parquet, arrow, iceberg).
- Disk-full incident and recovery: the four-worktree batch was salvaged and
  landed wave by wave.
- Multiprocess test-harness determinism fix (macOS CI flake).
- Fresh-day benchmark record: query battery 234 ms; multi-writer 2.55×; lake
  25.1 MB vs DuckLake 91.4 MB at 10M rows.

## 2026-10-16
- **Wave 55** — performance round (column-vs-literal comparison fast path;
  battery best 235.5 ms).
- **Wave 58** — SQL completeness (multi-way/composite/non-equi/CROSS/FULL
  joins, INSERT…SELECT, CTAS, IF [NOT] EXISTS, OFFSET, NULLS FIRST/LAST,
  INTERSECT/EXCEPT, COUNT(DISTINCT), EXPLAIN, SHOW/DESCRIBE, window frames).
- **Wave 60** — functions (`lag`/`lead`/`ntile`/`first_value`/`last_value` +
  scalar gaps).
- **Wave 61** — optimizer (projection pushdown, LIMIT short-circuit, join
  build-side choice, filter-through-aggregate).
- **Wave 45o** — optimistic multi-writer commits (stage 5): concurrent local
  writers on one durable store with WAL touched sets and automatic retry.
- **Wave 45n** — multi-process stability hardening (chaos soak harness).
- **Wave 45m** — multi-process durable `.pink` writers.
- Benchmark audit (battery ≈253 ms on an idle machine) and integration
  close-out (45o → 60 → 61 → 58 → CI green); first fully green nine-job CI
  pipeline.

## 2026-10-15
- **Wave 45k** — exact attach fast paths for lean lakes (published per-column
  statistics blocks).
- **Wave 45j** — `.pink` LZ frames (text corpora 3–5× smaller).
- **Wave 45i** — lean lake build (`lake-lite`, 2.69 MB).
- **Wave 45h** — `.pink` format v4 (flags word, typed codecs).
- **Wave 45e** — `.lake attach` projection pushdown.
- **Wave 45d** — `.lake attach` (footer-only, statistics-driven).
- **Wave 45c** — lake open 3.7× faster.
- **Wave 45b** — writer block-size A/B.
- **Wave 45** — recurring performance wave, expanded into the lake-scan
  program (sub-waves 45b–45o).

## 2026-10-14
- **Wave 40c** — CI + docs truthfulness (fmt gate, macOS/MSRV/audit jobs,
  docs sync).
- **Wave 40b** — durability and operability hardening.
- **Wave 40a** — safety hardening from the waves 0–40 gap audit.
- **Wave 40** — performance tuning.
- **Wave 39** — CI/CD publishing of extensions.
- **Wave 36** — S3 bucket support for lake storage + file reading.
- **Waves 35 + 35b** — lake data format + parser cleanup.
- **Wave 34** — pink-lake minimal lake layer.
- **Wave 33** — custom repositories + client-side INSTALL.
- **Wave 32** — extensions repository.
- **Wave 31** — Go extension SDK.
- **Wave 30** — C/C++ extension SDK.

## 2026-10-13
- **Wave 29** — extensions support (native ABI v1 + runtime loader).
- **Wave 28** — Go binding.
- **Waves 37–38** — documentation website + hosting.
- **Wave 27** — Python binding.
- **Wave 26** — C ABI (`pinkdb-ffi`).

## 2026-10-12
- **Wave 25** — at-rest encryption (`PINKENC1`: AES-256-GCM + Argon2id).
- **Wave 24** — network server (`pinkdb-server`, protocol v2).
- **Wave 23** — multi-database (`CREATE/DROP DATABASE`, `USE`, `db.table`).
- **Wave 22** — WAL / crash recovery.
- **Wave 21** — PEG parser default.
- **Wave 20** — outer joins.
- **Wave 19** — join fast path.

## 2026-10-11
- **Wave 18** — UNION binder fix for derived tables/CTEs/views/subqueries.
- **Wave 17** — performance tuning.

Waves 0–16 (through 2026-10-10) predate this changelog — core engine, SQL,
CSV, CLI, persistence and the first extension groundwork; see the git history
and the ROADMAP wave table.
