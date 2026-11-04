# Wave 141 — the SF10 join path

Planned 2026-11-04 after the quiet-machine baseline against DuckDB v2.0.0 (`pinkdb-wt/logs/cmp-16bb4b8`):
TPC-H SF10 at 2.73× DuckDB. The worst queries are joins: Q10 5.96×, Q2 5.47×, Q7 4.92×, Q21 4.16×.
Done directly in the stream-C session, one measured hot path at a time.

## Profile (main 16bb4b8, SF10, `sample` of repeated runs, inclusive samples per query)

| query | probe | output assembly | of which `gather_located` | probe_rows | hash build | other |
|---|---|---|---|---|---|---|
| Q10 | 14,500 (58 %) | ~12,000 | 11,444 (strings 3,440) | 1,158 | negligible | idle ~17 % per worker |
| Q7 | 17,600 | 8,400 | 7,500 | 4,040 | small | `row_filter_pass` 2,418 |
| Q21 | 7,600 | 3,900 | 3,800 | 977 | 1,139 | residual pairs/selection ~5,700 |

The join output's gather of build-side columns dominates; the hash build does not. Column pruning
through joins already works (wave 127): the `Project` listing every column in `EXPLAIN` is the
binder's view. Q10's idle time is the main thread's serial work while workers sleep: dropping the
build outputs (~240 samples), `FlatSink::merge` (~145) and `pipeline::steps` (~135).

## Steps

- **141a — typed gathers resolved once per join.** `GatherColumn` (`vector.rs`) resolves every build
  chunk's payload slice and validity mask, and whether the column has a NULL, once per join
  (`ProbePlan`). Gathering a match is then an indexed copy over the picks, with no per-row type
  dispatch, and an all-valid mask when nothing can be NULL. The join residual (`residual_selection`)
  uses the same columns. `StrVec::gather` maps buffers through a small map instead of a vector sized
  to the number of build chunks. One rejected attempt is worth recording: building the typed slices
  per call was O(build chunks) per output chunk, and made Q7 2.9× slower. The slices must be built
  once per join.
- **141b — the build outputs are freed by every worker.** The 141a profile showed no allocation
  cost worth reusing buffers for: `ProbeChunk::new` is the hash-table head lookup, not malloc. What
  the workers did wait on was the main thread freeing the join build outputs serially: 12 % of Q7
  and Q10, while every worker slept. `Compiled`'s drop now frees the build chunks with
  `into_par_iter`, before the statement returns. The first try freed them on a background rayon
  task. That only moved the work into the next statement (Q22 1.12–1.28 in interleaved runs), so it
  was replaced.
- **141c — a row-layout build side** (design: `docs/wave-141c-design.md`).
- Later, from the profile: a parallel or deferred drop of the build outputs, and a parallel
  `FlatSink::merge`.

## 141a results

Interleaved A/B against main 16bb4b8 (`tools/tpch-ab.py`, same store, same machine):

| | geomean | notable |
|---|---|---|
| SF1, 5 rounds, load 7.6–8.5 | 0.986 | — |
| SF10, 3 rounds, load 10–15 | **0.888** | Q10 0.53, Q7 0.76, Q9 0.82, Q21 0.87, Q2 0.92, Q5 0.93, Q8 0.93 |

The SF10 outliers Q6 1.33 and Q15 1.25 were rechecked at 6 rounds: Q6 0.99, Q15 1.01, Q17 0.96.

`tools/tpch-vs-duckdb.py` (store mode): SF1 1.56× (baseline 1.63×). SF10 2.89× against the
baseline's 2.73×, but that run's load was 10–15 against the baseline's 4–11, and DuckDB's own times
were 30–60 % slower than in the baseline (Q2 17 → 28 ms, Q7 77 → 109 ms). So the A/B above is the
landing evidence, and the quiet-machine comparison is rerun when the parent schedules one. Q10
against DuckDB went from 5.96× to 5.16× even under that load.

Gates: `cargo test -p pinkdb` (0 failures), `cargo clippy --workspace --all-targets -D warnings`
clean, `cargo fmt`.

## 141b results

The re-profile after 141a, main thread's serial share while the workers idle: Q7 had the serial hash
build at about 16 % (`MatchTable::build`) and build-output drops at 12 %; Q10 had drops at 12 %, the
build at 6 % and `FlatSink::merge` at 6 %.

Interleaved A/B against 141a:

| | geomean | notable |
|---|---|---|
| SF1, 5 rounds, load 8 | **0.945** | Q22 0.80, Q20 0.82, Q4/Q8 0.88 |
| SF10, 3 rounds, load 11–24 | **0.930** | Q7 0.86, Q5 0.90; per-query swings from load (Q21 0.33, Q22 2.05) |
| SF10, 6 rounds, selected queries | — | Q10 0.78, Q7 0.85, Q5 0.80, Q22 0.99 |

The SF10 Q22 outlier, rechecked at 8 rounds: 1.015. The SF1 Q11 outlier: 1.00.

Next by measured share: a parallel `MatchTable::build` (up to 16 % of Q7), then a parallel
aggregate merge, then 141c.

## Tried and not landed: parallel chain linking (2026-11-04)

`MatchTable::from_key_vectors` linked its chains serially. A parallel version split the bucket array
into one contiguous range per task, each walking all rows in reverse, so the chains stayed identical.
It measured as noise:
- SF1 1.014 and SF10 0.965 over the 22 queries;
- reruns flipping per-query signs (Q21 0.72 one run, 1.07 the next; SF1 Q4 1.29, then 0.88).

That was at load 11–23. A re-profile showed why: the build's serial share is spread out, not in
linking. Once linking ran in parallel, Q7's main thread spent its remaining build time in
`chunk_approx_bytes` (the build's byte estimate), about 48 samples, and in concatenating per-chunk
key arrays, about 30. Neither is worth a wave alone. Not landed: no change without a measured gain.
