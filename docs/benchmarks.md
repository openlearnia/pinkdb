# Benchmarks

pinkdb is measured against DuckDB v2.0.0 on the same data, on one quiet
machine, with warm caches and the same worker count. The suite that produces
these tables is committed: `bench/suite/run.py` records the machine, the
commit, the load average and every raw timing, and it refuses to compare two
engines on different data. This page is the reading of the latest run; the
numbers under `bench/suite/results/` are the record.

The suite compares answers as well as times: row counts must match and a
checksum of the normalized rows must match, so a fast wrong answer cannot make
the table.

## The machine

| | |
|---|---|
| CPU | Apple M4, 10 physical / 10 logical cores |
| RAM | 17.18 GB |
| OS | macOS 27.0.1 arm64 |
| rustc | 1.97.1 |
| DuckDB | v2.0.0 `d54cbbe3df` |
| pinkdb | commit `9f7df56` plus the uncommitted wave-111c tree |
| threads | 8 for both engines (`RAYON_NUM_THREADS=8`, `SET threads=8`) |
| runs | 1 warm-up + 3 timed, median reported |
| load average | 2.3–3.0, recorded per stage in the JSON |

TPC-H data is generated once by DuckDB's built-in `dbgen`, written to Parquet,
and the *same* Parquet files are loaded into a `.pink` store and a DuckDB
database. DuckDB's timer has 1 ms resolution, so its smallest figures are upper
bounds.

## TPC-H

### SF 0.1 (default)

All 22 queries, before and after wave 111ab. "before" is commit 38e2b08
(wave 120) on the same machine, same store, same eight threads; the baseline
run is in `docs/wave-111ab-plan.md` and `/tmp/w111ab/base-sf0.1.log`.

| query | before ms | after ms | DuckDB ms | ratio | rows |
|---|---:|---:|---:|---:|---:|
| 1 | 16 | 16 | 3 | 5.3× | 4 |
| 2 | 20 | 20 | 3 | 6.7× | 44 |
| 3 | 18 | 18 | 2 | 9.0× | 10 |
| 4 | 16 | 16 | 2 | 8.0× | 5 |
| 5 | 31 | 31 | 3 | 10.3× | 5 |
| 6 | 8 | 8 | 1 | 8.0× | 1 |
| 7 | 111 | **31** | 3 | 10.3× | 4 |
| 8 | 22 | **19** | 3 | 6.3× | 2 |
| 9 | 57 | 60 | 6 | 10.0× | 175 |
| 10 | 18 | **16** | 4 | 4.0× | 20 |
| 11 | 17 | **13** | 2 | 6.5× | 2541 |
| 12 | 13 | 16 | 2 | 8.0× | 2 |
| 13 | 40 | 40 | 11 | 3.6× | 37 |
| 14 | 7 | 7 | 2 | 3.5× | 1 |
| 15 | 3 | 3 | 1 | 3.0× | 1 |
| 16 | 8 | **7** | 3 | 2.3× | 2762 |
| 17 | 7 | 8 | 2 | 4.0× | 1 |
| 18 | 74 | **25** | 4 | 6.2× | 5 |
| 19 | 231 | **11** | 3 | 3.7× | 1 |
| 20 | 51 | **42** | 3 | 14.0× | 9 |
| 21 | 425 | **232** | 5 | 46.4× | 47 |
| 22 | 73 | **17** | 3 | 5.7× | 7 |

Geometric mean over all 22 queries: **9.5× before → 6.5× after**. Total
pinkdb time fell 1,266 ms → 681 ms. Three sub-10 ms queries (Q15, Q6, Q14)
divide by a 1–2 ms DuckDB timer and move the mean by several tenths; the
per-query column is the honest comparison.

### SF 1

| query | before ms | after ms | DuckDB ms | ratio | rows |
|---|---:|---:|---:|---:|---:|
| 1 | 170 | **156** | 13 | 12.0× | 4 |
| 2 | 197 | **185** | 3 | 61.7× | 100 |
| 3 | 192 | **181** | 8 | 22.6× | 10 |
| 4 | 167 | **152** | 7 | 21.7× | 5 |
| 5 | 336 | 342 | 9 | 38.0× | 5 |
| 6 | 85 | **78** | 3 | 26.0× | 1 |
| 7 | 1253 | **246** | 9 | 27.3× | 4 |
| 8 | 270 | **216** | 8 | 27.0× | 2 |
| 9 | 715 | **675** | 26 | 26.0× | 175 |
| 10 | 231 | **168** | 16 | 10.5× | 20 |
| 11 | 510 | **120** | 3 | 40.0× | 1048 |
| 12 | 147 | 172 | 9 | 19.1× | 2 |
| 13 | 450 | 453 | 37 | 12.2× | 42 |
| 14 | 71 | 71 | 7 | 10.1× | 1 |
| 15 | 36 | **33** | 5 | 6.6× | 1 |
| 16 | 70 | **58** | 10 | 5.8× | 18314 |
| 17 | 92 | **90** | 7 | 12.9× | 1 |
| 18 | 5515 | **1449** | 22 | 65.9× | 57 |
| 19 | 3855 | **114** | 10 | 11.4× | 1 |
| 20 | 4855 | **606** | 10 | 60.6× | 186 |
| 21 | 10391 | **2252** | 18 | 125.1× | 100 |
| 22 | 680 | **166** | 11 | 15.1× | 7 |

Geometric mean: **38.5× before → 21.8× after** over all 22 queries; total
pinkdb time 30,288 ms → 7,983 ms (3.8×). The four queries that were 82 % of
the old total (Q18–Q21) are now 55 % of a much smaller one. Load average was
2.1 when the baseline run started and 3.4–4.7 for the after run, both recorded
in the JSON.

## Wave 111c — join residuals and a parallel aggregate merge

This wave changes no answer either. Measured as an A/B between two release
builds of the same sources — the baseline binary built from `HEAD` (`15,009,008`
bytes, same diff as the recorded wave-123a tree) and the branch — back to back
on the same stores, `bench/suite --only tpch --runs 3 --skip-build`. Load
average 2.3–3.0 for the baseline runs and 2.6–3.0 for the after runs (the
machine could not be made quieter); run-to-run spread is ±5 %, larger on the
sub-20 ms queries.

### SF 1

| query | before ms | after ms | DuckDB ms | ratio before → after |
| --- | ---: | ---: | ---: | --- |
| Q1 | 70 | 70 | 13 | 5.4× → 5.4× |
| Q2 | 174 | 177 | 4 | 43.5× → 44.2× |
| Q3 | 178 | 177 | 8 | 22.3× → 22.1× |
| Q4 | 161 | 162 | 7 | 23.0× → 23.1× |
| Q5 | 325 | 325 | 8 | 40.6× → 40.6× |
| Q6 | 13 | 13 | 4 | 3.3× → 3.2× |
| Q7 | 253 | 241 | 9 | 28.1× → 26.8× |
| Q8 | 215 | 198 | 9 | 23.9× → 22.0× |
| Q9 | 763 | 756 | 26 | 29.3× → 29.1× |
| Q10 | 163 | 158 | 16 | 10.2× → 9.9× |
| **Q11** | **129** | **26** | 3 | 43.0× → **8.7×** |
| Q12 | 171 | 171 | 9 | 19.0× → 19.0× |
| Q13 | 440 | 436 | 38 | 11.6× → 11.5× |
| Q14 | 74 | 72 | 7 | 10.6× → 10.3× |
| Q15 | 24 | 25 | 5 | 4.8× → 5.0× |
| Q16 | 46 | 43 | 10 | 4.6× → 4.3× |
| Q17 | 88 | 83 | 7 | 12.6× → 11.9× |
| **Q18** | **1,390** | **251** | 22 | 63.2× → **11.4×** |
| Q19 | 44 | 45 | 11 | 4.0× → 4.1× |
| **Q20** | **604** | **100** | 10 | 60.4× → **10.0×** |
| **Q21** | **2,333** | **1,352** | 18 | 129.6× → **75.1×** |
| Q22 | 124 | 142 | 12 | 10.3× → 11.8× |

Geometric mean **166.5 ms → 128.1 ms (1.30×)**, total over the 22 queries
7,782 → 5,023 ms (1.55×). Q18 5.5×, Q20 6.0×, Q11 5.0×, Q21 1.7×. Q22's
124 → 142 ms is the suite's ordering, not the query: timed alone, three runs
each on the same store, it is 65/74/65 ms before and 78/60/68 ms after. Q9's
756 ms carries a ±3 % spread and is unchanged by this wave.

### SF 0.1

Same two binaries, same store.

| query | before ms | after ms | | query | before ms | after ms |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| Q1 | 7 | 7 | | Q12 | 16 | 16 |
| Q2 | 19 | 18 | | Q13 | 42 | 40 |
| Q3 | 17 | 17 | | Q14 | 6 | 6 |
| Q4 | 15 | 15 | | Q15 | 2 | 2 |
| Q5 | 31 | 30 | | Q16 | 6 | 6 |
| Q6 | 1 | 1 | | Q17 | 7 | 7 |
| Q7 | 30 | 30 | | Q18 | 25 | **20** |
| Q8 | 18 | 18 | | Q19 | 5 | 4 |
| Q9 | 61 | 60 | | Q20 | 40 | **10** |
| Q10 | 16 | 16 | | Q21 | 232 | **114** |
| Q11 | 13 | **5** | | Q22 | 15 | 15 |

Geometric mean **14.6 ms → 12.4 ms (1.18×)**, total 624 → 457 ms.

### Scaling SF 0.1 → SF 1

| query | before | after |
| --- | --- | --- |
| Q20 | 40 → 604 ms = **15.1×** for 10× the rows | 10 → 100 ms = **10.0×** |
| Q21 | 232 → 2,333 ms = 10.1× | 114 → 1,352 ms = 11.9× |
| Q18 | 25 → 1,390 ms = 55.6× | 20 → 251 ms = 12.6× |

The wave-120 report's superlinearity (Q20 38.7× for 10× the rows) is gone: the
key side that Q20's subquery probes is now built from the filtered `partsupp`
(11k keys instead of 800k), so the probe side's work is linear in the data it
actually has to touch. Q21's 11.9× is its two 6 M-row `lineitem` scans — that
is the query, and it was 10.1× on the duplicated-plan version because the
duplicated plan was dominated by the *outer* chain, which does not grow with SF.

### Where the time went

Q21 at SF 1, `sample` of 12 back-to-back runs, share of samples (before →
after): `Vector::push_from` 5.1 % → 5.2 %, `Vector::take` 3.2 % → 1.8 %,
`filter_chunk` 3.6 % → 1.9 %, `DataChunk::take` 3.1 % → 1.8 %,
`Vector::with_capacity` 2.2 % → 2.1 %; the residual path's own frames are
`ProbeSink::push` 2.7 % and `chunk_approx_bytes` 2.7 %. The top is rayon's
bridge (8.7 %) and `join_context` (6.0 %) — the two 6 M-row `lineitem` scans,
their filters and the match-table builds, which is what Q21 now *is*.

Q18 after: rayon bridge 11.2 % + `join_context` 11.0 %, phase-1 `fold_chunk`
4.6 %, allocation 7 %; the merge no longer appears (it was the single largest
frame before).

## Wave 123a — selection vectors and fixed-width kernels

This wave changes no answer either: a filter now produces a *selection* (row
indices) instead of copying survivors out, aggregates fold through it, and
comparisons and decimal arithmetic run in typed loops instead of boxing a
`Value` per row. Measured as an A/B between two release builds of the same
tree, back to back on the same store: the baseline rebuilt from `HEAD`, then
the branch, `bench/suite --only tpch --runs 3`. Load average 2.5–3.2 (the
machine could not be made quieter than that); run-to-run spread is ±5–10 %.

### SF 1

| query | before ms | after ms | DuckDB ms | ratio before → after |
| --- | ---: | ---: | ---: | --- |
| **Q1** | **164** | **64** | 13 | 12.6× → **4.6×** |
| Q2 | 190 | 173 | 4 | 47.5× → 43.3× |
| Q3 | 180 | 180 | 9 | 22.5× → 20.0× |
| Q4 | 165 | 161 | 7 | 23.6× → 23.0× |
| Q5 | 332 | 342 | 9 | 36.9× → 38.0× |
| **Q6** | **80** | **13** | 3 | 26.7× → **3.3×** |
| Q7 | 262 | 254 | 9 | 29.1× → 28.2× |
| Q8 | 239 | 209 | 9 | 26.6× → 23.2× |
| Q9 | 713 | 702 | 26 | 27.4× → 27.0× |
| Q10 | 169 | 165 | 16 | 10.6× → 10.3× |
| Q11 | 124 | 128 | 3 | 41.3× → 42.7× |
| Q12 | 165 | 172 | 9 | 18.3× → 19.1× |
| Q13 | 442 | 440 | 39 | 11.3× → 11.0× |
| Q14 | 80 | 73 | 8 | 10.0× → 9.1× |
| Q15 | 35 | 25 | 5 | 7.0× → 5.0× |
| Q16 | 60 | 45 | 10 | 6.0× → 4.5× |
| Q17 | 89 | 84 | 8 | 11.1× → 12.0× |
| Q18 | 1499 | 1500 | 22 | 68.1× → 68.2× |
| **Q19** | **115** | **50** | 10 | 11.5× → **5.0×** |
| Q20 | 616 | 609 | 10 | 61.6× → 60.9× |
| Q21 | 2300 | 2371 | 18 | 127.8× → 131.7× |
| Q22 | 152 | 154 | 12 | 12.7× → 14.0× |

Geometric mean **21.56× → 17.30×** (−19.8 %). Row counts and checksums are
identical to DuckDB's on every query, at SF 0.01, 0.1 and 1.

The four filter-heavy targets moved (Q1 2.6×, Q6 6.2×, Q19 2.3×, Q16 1.3×).
Q12 and Q14 are join-shaped, so their cost is the join and the projection, not
the filter; Q12 165→172 and Q14 80→73 are inside the spread. Q18/Q21 never
enter this path (join and subquery bound).

### SF 0.1

| query | before ms | after ms | | query | before ms | after ms |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| Q1 | 17 | **7** | | Q12 | 16 | 16 |
| Q2 | 19 | 19 | | Q13 | 41 | 39 |
| Q3 | 18 | 18 | | Q14 | 7 | 6 |
| Q4 | 16 | 16 | | Q15 | 2 | 2 |
| Q5 | 30 | 30 | | Q16 | 6 | 6 |
| Q6 | 8 | **1** | | Q17 | 7 | 7 |
| Q7 | 30 | 30 | | Q18 | 25 | 25 |
| Q8 | 18 | 18 | | Q19 | 11 | **5** |
| Q9 | 60 | 60 | | Q20 | 41 | 41 |
| Q10 | 16 | 16 | | Q21 | 235 | 240 |
| Q11 | 13 | 13 | | Q22 | 14 | 14 |

Geometric mean **6.35× → 5.04×**. At SF 0.01 all 22 pass; Q1 is 1 ms and Q6
below the 1 ms timer.

### Where the time went

Q1 at SF 1, `sample`, top of stack, before → after (share of samples):

| frame | before | after |
| --- | ---: | ---: |
| `Vector::push_from` (filter's copy) | 11.4 % | — |
| `expr::eval_arith` | 9.8 % | 28 % |
| `aggregate::Acc::add_decimal` | 3.9 % | 20 % |
| `aggregate::fold_chunk` | 5.0 % | 16 % |
| `expr::coerce_to` | 1.4 % | 15 % |
| `Vector::encode_into` (key encoding) | — | 7 % |
| rayon waits | ~30 % | 18 % |
| filter (`select_predicate` + kernels) | — | 3 % |

The copy is gone and the filter is no longer a phase; what is left is decimal
arithmetic feeding the accumulators plus the two-key grouping. Both are
follow-up rows (123b projector fusion, 123c perfect-hash/radix aggregation).

### Compacting a sparse selection: measured threshold

Q1 keeps 98 % of rows, Q6 1.8 %. The aggregate reads through the selection and
compacts only when the selection is sparser than 1/8; both extremes were built
and measured (median of 7, SF 1):

| compaction | Q1 | Q6 |
| --- | ---: | ---: |
| always | 138.3 ms | 13.4 ms |
| below 1/8 (shipped) | **58.5 ms** | **11.9 ms** |
| never | 66.1 ms | 16.7 ms |

Copying a dense selection costs more than the indirection; reading a sparse one
indirectly loses locality. Any divisor in 2..=55 behaves the same for these two
queries, so 1/8 is a middle value rather than a sharp optimum.

### Ingest, durable and size

| case | before | after |
| --- | ---: | ---: |
| `lineitem.csv` import | 2.10 M rows/s | 2.11 M rows/s |
| `lineitem.parquet` CTAS | 0.80 M rows/s | 0.80 M rows/s |
| `orders.csv` import | 1.39 M rows/s | 1.44 M rows/s |
| 2,000 single-row `Sync` inserts | 7744.9 ms | 7746.8 ms |
| checkpoints | 50.4 ms | 49.8 ms |
| CLI autocommit / batched | 115.9k / 373.0k rows/s | 113.9k / 379.0k rows/s |
| all-features CLI binary | 14,958,864 B | 15,009,008 B (+0.34 %) |

No new dependencies. Ingest and durable are unchanged within noise.

## Wave 122 — column statistics and join filters

This wave changes no answer: every TPC-H query still matches DuckDB's row count
and checksum. It changes *plans* and *scans*, so it is measured as an A/B
between two release CLIs on the same generated Parquet — the wave-111ab tree
(`e4a8677`) and the wave-122 tree, each loading its own `.pink` store from the
same files. Per query the two binaries alternate process runs of 1 warm-up + 6
timed executions; the two best medians are reported. Load average was 2.5–2.7
for SF 1 and 5.4 for SF 0.1 (another agent shared the machine); run-to-run
spread is ±5 %, so anything inside that band is called flat.

### SF 1

| query | before ms | after ms | delta | what moved |
| --- | --- | --- | --- | --- |
| Q1 | 146 | 150 | +2.7 % | flat |
| Q2 | 192 | 194 | +1.0 % | flat |
| Q3 | 200 | 192 | −4.0 % | flat |
| Q4 | 169 | 174 | +3.0 % | flat |
| Q5 | 354 | 369 | +4.1 % | flat |
| Q6 | 75 | 75 | 0.0 % | zone maps skip nothing (date range spans all chunks) |
| Q7 | 255 | 272 | +6.9 % | flat within spread |
| Q8 | 220 | 234 | +6.4 % | flat within spread |
| Q9 | 763 | 778 | +2.0 % | flat |
| Q10 | 180 | 172 | −4.2 % | flat |
| Q11 | 130 | 146 | +11.9 % | flat within spread (small query, high variance) |
| Q12 | 194 | 183 | −5.4 % | flat |
| Q13 | 468 | 484 | +3.5 % | flat |
| Q14 | 83 | 82 | −1.8 % | flat |
| Q15 | 40 | 40 | +1.3 % | flat |
| Q16 | 68 | 68 | 0.0 % | flat |
| Q17 | 94 | 89 | −4.8 % | flat |
| Q18 | 1520 | 1514 | −0.5 % | flat |
| Q19 | 102 | 105 | +2.4 % | static zone maps (3 date/ship predicates) |
| Q20 | 620 | 624 | +0.6 % | flat |
| **Q21** | **3831** | **3196** | **−16.6 %** | probe-side bloom + range on the 10k-row build |
| Q22 | (0.0 s) | (0.0 s) | n/a | below the suite's 1 ms timer |

Total over the 21 timed queries 9.70 s → 9.14 s (**−5.8 %**), all of it Q21.

### SF 0.1

| query | before ms | after ms | delta |
| --- | --- | --- | --- |
| Q1 | 15 | 15 | 0.0 % |
| Q2 | 20 | 21 | +5.0 % |
| Q3 | 18 | 18 | 0.0 % |
| Q4 | 16 | 16 | 0.0 % |
| Q5 | 34 | 35 | +2.9 % |
| Q6 | 8 | 8 | 0.0 % |
| Q7 | 36 | 36 | 0.0 % |
| Q8 | 20 | 21 | +5.0 % |
| Q9 | 65 | 69 | +6.2 % |
| Q10 | 20 | 20 | 0.0 % |
| Q11 | 15 | 15 | 0.0 % |
| Q12 | 18 | 18 | 0.0 % |
| Q13 | 46 | 48 | +3.2 % |
| Q14 | 7 | 7 | 0.0 % |
| Q15 | 3 | 3 | 0.0 % |
| Q16 | 8 | 8 | 0.0 % |
| Q17 | 8 | 8 | 0.0 % |
| Q18 | 28 | 28 | −1.8 % |
| Q19 | 10 | 11 | +10.0 % (1 ms) |
| Q20 | 47 | 47 | 0.0 % |
| Q21 | 262 | 274 | +4.6 % |
| Q22 | (0.0 s) | (0.0 s) | n/a |

Total 728 → 726 ms: flat. At this scale the build sides are 60k–600k rows, above
the 10k-row gate below which a bloom pays, so no star query is filtered.

### The filter's own A/B

The same binary with `SET join_filter_pushdown = false` versus `true`
(alternating, 6 timed runs each, SF 1):

| query | filters on | off | delta |
| --- | --- | --- | --- |
| Q21 | 2092 ms | 2402 ms | **−12.9 %** |
| Q5 | 386 | 368 | +5.0 % |
| Q3 | 191 | 188 | +1.6 % |
| Q7 | 248 | 243 | +2.3 % |
| Q9 | 547 | 556 | −1.7 % |
| Q18 | 1462 | 1458 | +0.2 % |
| Q20 | 598 | 602 | −0.8 % |
| Q8 | 86 | 86 | 0.0 % |

Only Q21 gets a filter (its build side is 10k rows against a 3M-row probe). An
earlier version derived one for every join: that was a measured loss on every
query with a large build side (Q3 +8 %, Q5 +12.8 %, Q7 +6.9 %, Q9 +7.5 %,
Q10 +4.4 %, Q20 +6.3 %), because hashing a multi-million-row build costs more
than the rows a bloom then rejects. The gate that came out of those numbers is
"build side ≤ 10 000 rows"; the ratio-based alternative is in the follow-up row.

### What the statistics cost on the write path

`ingest` family, SF 0.1, A/B of the two binaries, 10 runs each, medians:

| path | before | after | delta |
| --- | --- | --- | --- |
| `lineitem.csv` → `.import` (600k rows, 16 columns) | 227 ms | 260 ms | **+14.5 %** |
| `orders.csv` → `.import` (150k rows, 9 columns) | 91 ms | 102 ms | **+12.1 %** |
| `lineitem.parquet` → `CREATE TABLE AS` | — | — | unchanged (CTAS builds from columns, not chunk appends) |
| 2,000 single-row `Sync` inserts (`durable`) | 7.78 s | 7.76 s | 0.0 % |
| 2,000-row batched inserts | 63 ms | 59.8 ms | within noise |
| checkpoints | 47–53 ms | 47–53 ms | within noise |
| CLI autocommit / batched | 119k / 380k rows/s | 117k / 375k rows/s | within noise |

The CSV import pays for the exact per-chunk zone maps and the per-column
sketch: about 33 ms over 9.6 M cells, of which the sketch is ~8 ms and the
zone fold the rest. Parquet ingest, single-row inserts and checkpoints do not
pay. The brief's 5 % bound on the ingest family is therefore **missed on the CSV
path only**, and cheap bulk statistics is a follow-up row; the exactness is
kept because every later estimate (selectivity, join domain, bloom sizing)
reads it.

### Sizes

| binary | before | after |
| --- | --- | --- |
| default CLI | 5,608,752 B | 5,690,064 B (+1.4 %) |
| all features CLI | 14,842,064 B | 14,942,048 B (+0.7 %) |

No new dependencies: the sketch, the Bloom filter and the statistics are
hand-written in `crates/pinkdb/src/stats.rs`.

## Ingest (SF 0.1)

| table | format | engine | ms | M rows/s | MB/s |
|---|---|---|---:|---:|---:|
| lineitem | CSV | pinkdb | 263 | 2.28 | 288 |
| lineitem | CSV | DuckDB | 366 | 1.64 | 207 |
| lineitem | Parquet | pinkdb | 488 | 1.23 | 39 |
| lineitem | Parquet | DuckDB | 104 | 5.77 | 181 |
| orders | CSV | pinkdb | 94 | 1.60 | 182 |
| orders | CSV | DuckDB | 95 | 1.58 | 180 |
| orders | Parquet | pinkdb | 95 | 1.58 | 56 |
| orders | Parquet | DuckDB | 63 | 2.38 | 84 |

pinkdb's CSV reader beats DuckDB's on the 16-column lineitem file; its Parquet
reader is about 4.5× slower because DuckDB decodes larger column chunks in
parallel.

## Durable writes (pinkdb only)

| work | time |
|---|---:|
| 2,000 single-row autocommit inserts, `Sync` | 7.78 s |
| 2,000 inserts, batched (`SyncPolicy::Batch`) | 63 ms |
| 200 selects after each policy | 34–63 ms |
| 3 checkpoints | 40–50 ms |
| CLI single-row autocommit, warm | 119k rows/s |
| CLI batched `VALUES`, warm | 380k rows/s |
| two concurrent writers, optimistic transactions | 0.96× speedup |

There is no DuckDB comparison here: DuckDB has no per-statement WAL with fsync
policies. The single-process `Sync` cost is dominated by fsync, and the
two-writer number is honest about contention: two processes do not go faster
than one.

## Lake

SF 0.1 lineitem (600,572 rows), 8-thread attach, warm object cache:

| catalog | attach ms | attach + scan `sum(l_extendedprice)` ms |
|---|---:|---:|
| `pink:` | 21.5 | 13 |
| `sqlite:` | 2.1 | 14 |
| `pink+s3://` (mock S3, 61 objects) | — | 83 |

The S3 number is a local, in-process mock endpoint with no network latency; it
measures request count and object layout, not the internet. Commit latency is
not separately timed here — wave 110 measured one commit as 3 S3 requests and
2,954 bytes, independent of lake size.

## Search

| workload | result |
|---|---|
| FTS index build, 100k documents × 2 fields | 822 ms |
| FTS top-10 query, same corpus | <1 ms (below the CLI's 1 ms timer resolution; wave 108a's 103 ms was cold) |
| HNSW build, 100k × 768, 1 thread | 126.5 s |
| HNSW build, 100k × 768, 8 threads | 40.5 s |
| HNSW index size | 318,915,668 B |
| recall@10, `ef_search` 16 / 64 / 128 / 256 | 0.535 / 0.878 / 0.961 / 0.991 |
| p50/p95 latency (ms), 8 threads, same ef | 0.57/0.65, 0.89/1.03, 1.20/1.35, 1.57/1.79 |

DuckDB's `fts` and `vss` extensions need `INSTALL`, which this project does not
run without an explicit go-ahead, so there is no cross-engine search comparison
yet. It is a follow-up, not an omission by accident.

## Footprint

| build | bytes |
|---|---:|
| CLI, default features | 5,880,048 (+122,608 since 9f7df56) |
| CLI, `--no-default-features` | 5,722,000 |
| CLI, all measured features | 15,131,904 (+122,896 since 9f7df56, +0.8 %) |
| server, default features | 5,203,312 |
| CLI + `fts` | 5,612,736 (+175,568) |
| CLI + `vss` | 5,570,128 (+132,960) |
| CLI + `parquet` | 7,459,728 (+2,022,560) |
| CLI + `pinklake` | 8,265,536 (+2,828,368) |
| `libpinkdb_agent` | 2,548,752 |
| `libpinkdb_mcp` | 609,200 |
| `libpinkdb_memory` | 2,746,160 |
| `libstring_ext` | 16,720 |

CLI start-up (open an empty store, quit): 5.2 ms. Peak RSS at SF 0.1: Q1
313 MB, Q9 674 MB, Q10 247 MB — Q9 materializes the largest join output.

## Where pinkdb is slower, and why

**Q21 (75×).** Wave 111c removed the duplicated outer chain and the two
36-column distincts: the non-equality conditions are join residuals now, so the
plan is one outer chain plus `Semi`/`Anti` over the body's own keys. What is
left is the query itself — two `lineitem` scans of 6 M rows, each filtered on
`l_receiptdate > l_commitdate`, and a 6 M-row match table built for each. The
next real lever is bloom-filtering *inside* the scan (test the join key while
the filter runs, before the payload columns are materialized); wave 122's
dynamic filter only skips whole chunks by zone bounds, which cannot help a
bloom over 6 M rows ordered by `l_orderkey`.

**Q18 (11.4×).** The `IN` subquery's aggregate — `GROUP BY l_orderkey` over
6 M rows — was the whole cost, and its phase-2 merge was serial. It is now
hash-partitioned and merged in parallel (merge 1,284 → 56 ms), so what is left
is phase 1's per-chunk fold plus the join probe above it. A perfect-hash or
partitioned *phase 1* (one table per thread rather than one per chunk) is the
next step; row 111b1 (2) and (3) record it.

**Q2 (44×).** A correlated subquery's body is optimized when it is *bound*,
while its correlation conjunct still carries an outer column — and
`reorder_comma_join` refuses a chain whose predicate reads an outer column
because the unnester pattern-matches that body's from-tree. The statement-level
optimizer then only sees converted hash joins, so the body keeps the FROM
clause's own order: Q2's `partsupp ⋈ supplier ⋈ nation ⋈ region` runs
partsupp-first (115 ms) where region-first is 9 ms, and it materializes 19
columns to use one. Fixing it needs the join ordering to run after
decorrelation, which means a pass over `Join` trees (or statement-level
recursion into `BoundExpr` subquery plans — `fold_expr` has no subquery arms
today). Wave 111c left Q2 at 174 → 177 ms and recorded the two causes.

**Q5 (41×).** Scans and joins at 10–40× the DuckDB time per row: the hash join
copies every surviving row into fresh vectors at each probe instead of writing
a selection vector and materializing once, and the aggregate emits group by
group with a value copy per cell. Q11 used to be the same shape and is now
8.7×, because its chain was small enough (four leaves) for the join order to
start from `supplier` instead of `partsupp`; Q5's five- and Q9's six-leaf
chains keep the anchored start, since freeing it there was measured worse
(Q5 3.6×, Q9 2.6×).

**Q1/Q6 (12–26×).** Both are scan-bound. pinkdb's filter and aggregate paths
are vectorized, but they lack DuckDB's perfect-hash aggregate and
selection-vector shortcuts; the cost is flat per row and does not change with
scale.

**Q9 (26×).** The cost model picks
`part ⋈ lineitem ⋈ supplier ⋈ nation ⋈ orders ⋈ partsupp`, which joins the
two-key `partsupp` last. The best order measured by hand was 13 % faster, so
the join order is not the whole gap; the row-copying join and the aggregate
are. The wave left Q9 715 → 675 ms at SF 1 and 57 → 60 ms at SF 0.1 (within the
run-to-run spread); Q5's 31 → 31 ms is unchanged.

**Parquet ingest.** DuckDB reads and decodes in larger parallel batches; pinkdb
decodes column chunk by column chunk.

## Correlated subqueries

Wave 120 rewrites a correlated subquery into a join (see
[docs/wave-120-plan.md](wave-120-plan.md)): `EXISTS`/`NOT EXISTS` become a
semi/anti hash join on the correlation keys, `IN`/`NOT IN`/`ANY`/`ALL` a mark
join that computes SQL's three-valued answer, and a correlated scalar
aggregate groups by the correlation keys and left-joins the block, with
`count` coalesced to 0. Q2, Q4, Q17, Q20, Q21 and Q22 therefore answer at all,
and they answer in the same time class as the rest.

Two shape choices were measured rather than assumed, at SF 0.1 on the same
store, one release binary and the same suite:

| shape | Q4 | Q17 | Q20 | Q21 | Q22 |
|---|---:|---:|---:|---:|---:|
| duplicate-eliminated key side for every form | 16 | 7 | 50 | 407 | 57 |
| body's own key columns for `EXISTS`/`NOT EXISTS` | 16 | 39 | 30 | 427 | 66 |

So `EXISTS`/anti joins use the body's own keys (no second scan of the outer
block, which is the asymptotically important half) and scalar aggregates keep
the key side (the aggregate then groups rows the join has already narrowed,
which is what makes Q17 cheap). Both are visible in `EXPLAIN`; nothing is
hidden.

Wave 111c closed two of the three gaps this section used to record. Q21's
non-equality condition (`l3_suppkey <> l1_suppkey`) is a join residual now, so
`EXISTS`/`NOT EXISTS` keep the plain shape and the outer chain appears once:
232 → 114 ms at SF 0.1 and 2,333 → 1,352 ms at SF 1. The duplicate-eliminated
key side also carries the enclosing filters, so Q20's keys come from 11k
filtered `partsupp` rows instead of 800k and its scaling is 10.0× for 10× the
rows instead of 15.1× (it was 38.7× when wave 120 recorded it). What is still
a follow-up is *sharing* one key side across several subqueries of a statement
(a delim join or a materialised CTE-like node); no TPC-H query needs it now,
since Q20 and Q21 are the only delim shapes left and both are linear.

Q2's 44× is a different cause and is recorded above: its subquery body is
ordered before decorrelation, and its projection copies columns nobody reads.

For reference, the pre-existing gap is unrelated: Q18 (11.4×) and Q19 (4.1×)
have no correlated subquery at all — Q18 is the aggregate inside a plain `IN`,
Q19 the nested `OR` lists.

## What changed in wave 111c

| change | measurement |
| --- | --- |
| a hash join carries an optional residual predicate on candidate key matches, evaluated in joined space on that pair before anything is materialized (only the residual's columns are gathered, into one dense chunk); `INNER` drops a failing pair, `SEMI`/`ANTI` treat it as absent | Q21 SF 1 2,333 → 1,352 ms (−42 %), SF 0.1 232 → 114 ms; 3 clones of a 36-column chain + 2 distincts → one chain |
| `dependent_join` keeps the *plain* shape for an `EXISTS` whose body has equality keys plus a residual, with the residual rewritten into joined space; outer and mark joins reject one at execution | the Q21 plan above; `tests/residual_joins.rs` compares inner/semi/anti × 5 residual forms against a nested-loop reference and DuckDB |
| the duplicate-eliminated key side carries the enclosing predicate, including one that holds an uncorrelated subquery (the second evaluation only filters rows the key side reads anyway) | Q20 SF 1 604 → 100 ms (−83 %), SF 0.1 40 → 10 ms; keys 800k → 11k; SF 0.1→1 scaling 15.1× → 10.0× |
| `merge_partials` hash-partitions the partial group tables and merges the partitions in parallel, then restores the exact first-seen order with one sort over `(partial, position)` | Q18 SF 1 1,390 → 251 ms (5.5×); merge alone 1,284 → 56 ms (scatter 22 + merge 56 + sort 17 + assemble 9) |
| join order may start from any leaf when the chain has at most four relations (found while diagnosing Q11) | Q11 SF 1 129 → 26 ms (5.0×), SF 0.1 13 → 5 ms; five/six-leaf chains keep the anchor, because freeing it there cost Q5 3.6× and Q9 2.6× |

SF 1 geometric mean 166.5 → 128.1 ms (1.30×), total 7,782 → 5,023 ms (1.55×);
SF 0.1 geomean 14.6 → 12.4 ms. Every answer is identical to DuckDB's (row
counts and checksums) at SF 0.01, 0.1 and 1. No new dependencies; the CLI grew
123 KB (+0.8 %).

## What changed in wave 123a

| change | measurement |
| --- | --- |
| filters produce a selection (row indices) instead of a compacted copy, and `GROUP BY` / `COUNT(*)` fold through it | Q1 SF 1 164 → 64 ms (2.6×), Q19 115 → 50 ms (2.3×); the copy was 11 % of Q1's profile |
| select-style predicate evaluation: comparisons and `AND`/`OR` build the selection directly, `AND` narrows progressively and `OR` only looks at the rows its left side did not take; a conjunct's error fires only for rows that reach it | Q6 SF 1 80 → 13 ms (6.2×); the filter is < 3 % of Q1's remaining profile |
| one typed comparison kernel (15 layouts) for both the vector and the selection path, so `DECIMAL` against `DOUBLE`/`INTEGER`/`BIGINT`/any-scale `DECIMAL` stops going through a `Value` per row | `l_discount BETWEEN 0.05 AND 0.07` over 6 M rows 38.3 → 19.7 ms; Q6's filter is where that landed |
| decimal arithmetic borrows its payloads, reuses the column validity mask and writes a pre-sized result; column-vs-constant decimal arithmetic skips the constant vector | Q1 73.5 → 65.7 ms from the constant-operand path alone |
| compaction threshold for a sparse selection, measured rather than guessed | always 138.3 ms / 1/8 58.5 / never 66.1 on Q1, and 13.4 / 11.9 / 16.7 on Q6 |

SF 1 geometric mean 21.56× → 17.30×, SF 0.1 6.35× → 5.04×, ingest and
durable flat. The wave gate (Q1/Q6 within 3× of DuckDB at SF 1) is **partly
met**: Q6 is at 3.3×, Q1 at 4.6×. The rest of Q1 is the grouping and the
accumulator updates, which are 123b/123c work — the numbers are in the section
above.

## What changed in wave 122

| change | measurement |
| --- | --- |
| join-filter pushdown: a hash join with a small build side hands its key bounds + a blocked Bloom filter to the probe-side scan, which skips non-overlapping chunks and drops non-matching rows before materialization | Q21 SF 1 3831 → 3196 ms (−16.6 %); with the session switch, 2402 → 2092 ms (−12.9 %). Every join with a build side above 10k rows is left unfiltered: deriving the filter there was a measured loss (Q5 +12.8 %, Q3 +8 %, Q7 +6.9 %) |
| catalog column statistics: exact row/NULL count and min/max, HyperLogLog distinct estimate (p = 12), maintained on insert/update/delete/copy, persisted in the `.pink` footer, recomputable exactly with `ANALYZE`, readable through `pinkdb_column_stats()` | the ingest table above: +14.5 % on `lineitem.csv`, nothing on the write paths that matter (durable inserts, checkpoints, Parquet CTAS) |
| selectivity from statistics in the cost model: equality `1/distinct`, ranges as the uniform fraction of the stored min/max, `IN` `k/distinct`, `IS NULL` the NULL fraction, and equality-class join domains from distinct counts, with the old 0.2/0.5 fallbacks kept | no TPC-H plan moved outside the flat band: the star queries were already near the DP optimum; the cheaper estimates matter on shapes TPC-H does not have (small dimension joins, skewed filters) |
| zone maps in `.pink` scans: per-chunk min/max/NULL count, exact on write, only widened by a delete | Q19 SF 1 102 → 105 ms (flat): its date predicates already prune row groups in the same way; the win is on selective point/range filters over wide tables, not on TPC-H |
| a join-cardinality estimate `|L|·|R| / max(ndv)` from the catalog distinct counts, tried and **reverted** | Q8 SF 1 218 → 116 ms with it, but Q7 +41 %, Q21 +22.7 %, Q5 +16.7 %: a wrong build side costs more than an unestimated one. The fallback it needs (a guessed domain) misplaces the side too often, so the estimate only applies where both keys are catalog columns — reported as a follow-up |

PF 1 geometric mean over the 22 queries 39.0× → 36.9×; the wave's star-query
gate is **partly met**: Q21 improves by 4–17 %, the other star queries
(Q3/Q5/Q7/Q8/Q9/Q10) are flat, which the section above explains and measures.

## What changed in wave 111ab

Two roadmap rows in one wave: cost-based join ordering with a build-side
choice (111a) and the execution/subquery paths (111b). No SQL feature was
added; every answer is checked against DuckDB's rows.

| change | measurement |
|---|---|
| cost-based comma-chain order (query graph, DP up to 12 relations, greedy above, FK-aware cardinalities, anchored at the FROM clause's first relation) | Q7 SF 1 1.25 s → 246 ms; Q7 SF 0.1 111 → 32 ms; Q5 SF 0.1 31 → 35 ms is the one regression |
| per-column filters derived through a lone `OR`, with `IN` lists inside branches and union (not intersection) bounds | Q19 SF 1 3.86 s → 112 ms, SF 0.1 231 → 11 ms |
| subquery memo filled by the operator before its chunks fan out, so no lock is ever taken across an evaluation | Q18 SF 1 5.5 s → 1.45 s, Q20 4.9 s → 0.61 s, Q22 680 → 166 ms; the deadlock is gone (30 repro runs, 0 hangs) |
| subquery bodies are left un-reordered (a restore projection hides the from-tree the binder decorrelates by pattern) | Q2 stayed at 21 ms instead of planning a 2·10¹³-row cross product |
| scanner/suite: timeout payloads are decoded before concatenation | the suite no longer crashes when a query times out |

SF 1 geometric mean **38.5× → 21.8×**, total time 30.3 s → 8.0 s. SF 0.1
geometric mean **9.5× → 6.5×**, total 1,266 → 681 ms. Binary size +37.8 kB
(+0.68 %) for the default CLI, no new dependencies.

## What changed in wave 111

| change | measurement |
|---|---|
| comma joins become hash joins (Filter over a condition-less cross join) | Q9 SF 1 100.9 s → 0.73 s; Q8 11.7 s → 0.33 s |
| join reordering for disconnected comma chains (greedy equality graph, restore projection) | Q9 0.73 s → 0.66 s; Q8 0.33 s → 0.29 s |
| per-table filters derived from an `OR` of conjunctions | Q7 SF 0.1 219 → 114 ms; SF 1 2.35 → 1.20 s |
| `IN` set capacity reserved from the subquery row count | 8.8 s → 5.8 s on the probe query |
| subquery memoization (single-flight) | Q18 SF 0.1 36 ms, but deadlocked once at SF 1; **reverted**, follow-up |
| pink-lake corruption test cleans its scratch dirs | no benchmark; removes ~1,500 leaked `$TMPDIR` directories |

SF 1 geometric mean over the same 15 queries: **33.1× before → 32.0×
after** (29.0× over all 16). At SF 0.1 the geomean moved 7.4× → 8.6×: Q7
halved and Q9 fell 451 → 60 ms, but reverting the memo left Q18 at 76 ms
(36 with it), and the sub-5 ms queries swing a ratio by a whole unit.

## Reproduce

```sh
# default: TPC-H SF 0.1, every family, about ten minutes
bench/suite/run.sh

# one family at SF 1 (about 1 GB per engine; run it on a quiet machine)
bench/suite/run.sh --only tpch --scale 1

# the CI guard: 60k rows, no DuckDB, compares against bench/suite/baseline-ci.json
python3 bench/suite/run.py --only ci
```

`--only` takes a comma-separated subset of
`tpch,ingest,durable,lake,search,footprint,ci`; `--work-dir` keeps the
generated stores between runs and `--skip-build` reuses the release binary.
Nothing in the suite downloads data or installs a DuckDB extension.

## Gaps and follow-ups

- **Delim join for decorrelated subqueries** (Q21's shared key side) and the
  Q21 non-equality condition as a join residual. Q21 is the largest single
  query left: 2.25 s at SF 1, 125× DuckDB.
- **Parallel aggregate merge.** `merge_partials` merges every chunk's partial
  table on one thread; Q18's remaining 1.4 s is mostly this, and Q1/Q9/Q13 pay
  a share of it.
- **Scatter-then-gather join output** (selection vectors instead of copying
  rows) for Q2/Q5/Q11 and the join-heavy scans.
- **Perfect-hash aggregation and selection-vector filters** for Q1/Q6.
- **Q9's join order**: the estimator joins the two-key `partsupp` last. A
  finer model (or a rule that joins the table with the most equality edges
  first) would recover the 13 % measured by hand.
- **Ingest**: Parquet decode in larger parallel batches.
- A DuckDB `fts`/`vss` comparison, once `INSTALL` is allowed.
- The CI guard is non-blocking until it has a stable history on GitHub's
  shared runners.
