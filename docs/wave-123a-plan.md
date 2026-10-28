# Wave 123a — selection vectors and fixed-width expression kernels

First step of ROADMAP row 123 ("vectorized pipeline execution"). Row 123 had
four parts: selection vectors + fixed-width kernels (this wave), morsel-driven
push pipelines (123b), perfect-hash and radix-partitioned aggregation (123c),
and keeping `ExecLimits`, cancellation, profiles, zone maps and wasm32 working.

**This wave is performance only: no query result may change.** Baseline commit
`0a45bfb` (wave 122), branch `stream-a`.

## Problem

At SF 1, Q1 is 12.8× and Q6 20× slower than DuckDB at the same data, over the
same Parquet-shaped store. Both are single-table scan → filter → aggregate with
no join planning. Measured on the baseline (`bench/suite`, median of 3, 8
threads):

| query | pinkdb | DuckDB | ratio |
|---|---:|---:|---:|
| Q1  | 166 ms | 13 ms | 12.8× |
| Q6  | 80 ms | 4 ms | 20.0× |
| Q12 | 164 ms | 9 ms | 18.2× |
| Q14 | 79 ms | 8 ms | 9.9× |
| Q19 | 119 ms | 11 ms | 10.8× |

## Where the time goes (baseline, measured)

`sample` of a 200×Q1 / 150×Q6 loop on the SF 1 store (`/tmp/w123a/*.sample.txt`),
plus phase-isolating SQL variants (median of 20–30 runs, `.timer on`). SF 1
`lineitem` is 6 001 215 rows in 2930 chunks of 2048.

Q1: `Filter(Aggregate keys=[l_returnflag,l_linestatus])`:

| phase | measured |
|---|---:|
| whole query | 148 ms |
| filter predicate (date range) alone (`count(*)`) | 4.7 ms |
| the same aggregate **without** the filter | 95 ms |
| ⇒ `filter_chunk`'s `chunk.gather` copy (7 columns × 5.9M rows) | ≈ 48 ms |
| grouping by the 2 VARCHAR keys, no aggregates | 12.6 ms |
| `sum(l_extendedprice*(1-l_discount))` added | +20.6 ms |
| `sum(l_extendedprice*(1-l_discount)*(1+l_tax))` added | +34.0 ms |
| `sum(l_quantity)`, `sum(l_extendedprice)` | +3.1, +3.5 ms |
| the three `avg()` | +18.1 ms |

Q6: `Filter(Aggregate [])` with four conjuncts:

| phase | measured |
|---|---:|
| whole query | 70–75 ms |
| `count(*)` with no filter (statistics shortcut) | 0.3 ms |
| filter with all four conjuncts | 62 ms |
| `l_shipdate` range alone | 5.0 ms |
| `l_quantity < 24` alone | 12.2 ms |
| `l_discount BETWEEN 0.05 AND 0.07` alone | 38.3 ms |
| `l_extendedprice * l_discount` + `sum`, no filter | 14.1 ms |

Single-predicate costs over the same 6M rows (5 ms is the floor: BIGINT `<`
costs 4.9 ms, DATE `>=` 5.0 ms, VARCHAR `=` 9.8 ms):

| predicate | cost |
|---|---:|
| `l_quantity < 24` (DECIMAL vs integer literal) | 12.2 ms |
| `l_extendedprice > 0` | 11.1 ms |
| `l_discount <= 0.07` | 15.7 ms |
| `l_discount >= 0.05` | 18.0 ms |
| `l_discount BETWEEN 0.05 AND 0.07` | 38.3 ms |

The three DECIMAL predicates are 2–3.5× the cost of the same shape on an
integer or date column. Cause: the binder keeps the literal as `Value::Double`
(or `Integer`/`Bigint`) and the column as `DECIMAL`, so `eval_comparison`'s
column-vs-literal fast path matches no arm (it only covers same-dtype and
DECIMAL-vs-DECIMAL-literal-at-the-same-scale) and the comparison falls through
to the generic mixed-dtype loop: two `Vector::get` calls plus a
`compare_values` per row, i.e. a `Value` per operand per row.

So the two costs 123a attacks are exactly the ones the brief names:

1. **`DataChunk::gather` after every filter** (48 ms of Q1): the filter
   materializes a filtered copy of every projected column, because
   `filter_chunk` has no way to hand the surviving row indices to its consumer.
2. **Mixed-dtype DECIMAL comparisons** (50 ms of Q6): per-row `Value` boxing
   where a typed loop over the i128 payloads gives the same answer.
3. Per-row `Value` boxing in `CASE` (`eval_case` keeps a `Vec<Option<Value>>`
   and `gather`s per branch) and in `IN`-lists (bound as an `OR` chain, so it
   pays `eval_logic`'s boolean-vector walk).

## Design

### 1. `Selection` (`exec/selection.rs`, new)

`Selection = Vec<u32>` of ascending row indices into a `DataChunk`: the rows a
filter kept, in chunk order. `u32` rather than `usize` halves the index traffic
(`select_true` built a `Vec<usize>`); a selection never spans chunks.

Helpers: `all_rows(n)`, `retain_where`, and `DataChunk::take(&[u32])` (a
`gather` that takes a selection; strings stay shared `Arc`s) for the one place
that must materialize.

### 2. Select-style predicates (`expr::select_predicate`)

`select_predicate(expr, chunk, cx, &mut sel)` narrows `sel` in place:

- `AND` narrows progressively: the left conjunct filters `sel`, the right one
  only ever looks at survivors (DuckDB's select path for conjunctions).
- `OR` evaluates both sides over the original selection and unions the two
  results (order-preserving merge, so the selection stays ascending).
- `column <op> literal` (either operand order) runs a typed select kernel over
  the column's payload slice: INTEGER, BIGINT, DOUBLE, DATE, TIMESTAMP,
  DECIMAL and VARCHAR — plus the new DECIMAL-vs-numeric-literal arms.
- anything else falls back to `eval_expr` + `select_true_in`, which keeps
  three-valued logic in one place (`NULL` never survives a `WHERE`).

`ERRATA / semantics`: narrowing means a later conjunct is not evaluated at all
for rows an earlier conjunct already rejected, so a division-by-zero or
DECIMAL-overflow error inside that conjunct is raised only when a *surviving*
row reaches it. That matches DuckDB's `Select` conjunction path and the brief;
it is the one intended behaviour change, and the differential test pins it.

### 3. Filters hand selections downstream instead of materializing

`filter_chunk` keeps its current signature and behaviour for parents that need
a materialized chunk (projection, join build, sort, result sink).

The aggregate gets a selection-aware entry point: when its input is a `Filter`
chain, `exec::mod`'s `Aggregate` arm evaluates the filter to `(DataChunk,
Selection)` and folds **through** the selection — `group_ids` and `accumulate`
index the key/argument vectors at `sel[k]` instead of reading a gathered copy.
The key/argument vectors themselves are evaluated over the whole chunk exactly
as today (`eval_agg_vec`), so accumulator updates, group order and arithmetic
are unchanged — only the row lookup differs, and it is an extra load, not a
copy.

**Sparse threshold** (`MATERIALIZE_DIVISOR`): a selection keeps at most
`1/MATERIALIZE_DIVISOR` of the chunk to be worth materializing once (a
sequential `take`) and reading sequentially afterwards, rather than paying an
indirection per column read. Q6 keeps 1.8 % of each chunk and has one aggregate
argument, Q1 keeps 98 % and has eight — the two extremes the threshold sits
between. The plan measures both sides of the threshold (§Measurement) and fixes
it from those numbers.

### 4. Fixed-width kernels

- **DECIMAL comparisons** against `DOUBLE`, `INTEGER`, `BIGINT` and any-scale
  `DECIMAL` literal, in both `eval_comparison`'s scalar fast path and
  `select_predicate`. Semantics are `compare_values`'s, one row at a time:
  `Decimal(x, sx) <op> Double(y)` compares `decimal_to_f64(x, sx)` with
  `num_cmp`, `Decimal <op> integer` compares `decimal_cmp(x, sx, y, 0)`, and
  `Decimal <op> Decimal` compares `decimal_cmp(x, sx, y, sy)` — done on the
  i128 payloads, no `Value`.
- **Arithmetic**: `decimal_operand` currently clones both i128 payloads of the
  chunk before the multiply loop; borrow instead. Kernels write into a
  pre-sized payload (`Vector::with_capacity`) so the row loop has no growth
  checks and no per-row validity-word arithmetic.
- **CASE**: replace `eval_case`'s `Vec<Option<Value>>` + per-branch `gather`
  with a typed scatter: evaluate a branch's value vector for the rows that
  choose it, then copy each element into the output at that row in the
  output's own layout (`Vector::copy_row_from`), marking validity directly.
  Branch conditions keep the cooperative narrowing they have today (an
  unselected branch is never evaluated).
  **Not done** — see “Undone” below: measured at 6 % of Q14, below the risk of
  rewriting a path every `CASE` in the engine depends on.
- **IN-lists** are `OR` chains over comparisons, so they get the typed
  `select_predicate` path from §2 with no separate kernel.

### 5. Removed `push_from` copies

`filter_chunk`'s `DataChunk::gather` is the copy wave 111 profiled
(`push_from` 11 % of Q1). The aggregate now reads through the selection, so the
copy disappears for the `Filter → Aggregate` shape. `push_from` stays for the
boundaries that truly need a compact chunk.

## Correctness

- **Differential fuzz** (`exec/expr.rs` tests, xorshift64 seeds, no new
  dependency): random predicates over INTEGER/BIGINT/DOUBLE/DATE/TIMESTAMP/
  DECIMAL(15,2)/VARCHAR, with column-vs-column, column-vs-literal and
  literal-vs-literal operands and mixed numeric types (which is how a
  `DECIMAL` column meets a `DOUBLE` literal — the Q6 root cause). 24 seeds ×
  10 cases over chunks of 0, 1, 2, 63, 64, 65 and 130 rows: every chunk has an
  all-NULL row, an all-valid row and 1-in-5 NULLs elsewhere. Leaf values are
  the type extremes (i32/i64 min/max, ±0.0, NaN, ±Inf, empty string,
  out-of-precision decimals) so overflow and division-by-zero actually fire.
  Three paths must agree: `select_predicate`, `select_true(eval_expr(..))`,
  and a `#[cfg(test)]` reference that evaluates the predicate one row at a
  time on a one-row chunk. Asserted: **all selected rows are equal**; that
  error cases were generated and that at least one of them is *narrowed away*
  by `AND`/`OR` (the mass-evaluation raises where the reference does not); and
  that a conjunct error fires only for rows that reach it, with the error text
  identical to the unmasked path. `AND`, `OR`, `NOT` and `IS [NOT] NULL` are
  synthesised; `BETWEEN` and `IN` bind to the `AND`/`OR` chains above, and
  `CASE` is exercised by the aggregate test through its `FILTER` clause.
- **Aggregate read-through test** (`exec/aggregate.rs` tests):
  `aggregate_selected` over a sparse selection equals `aggregate` over
  `chunk.take(sel)` cell for cell, for one integer key, one `VARCHAR` key and
  both, with `COUNT(*)`, `SUM`/`AVG`/`MIN`/`MAX` over `DECIMAL`, `BIGINT` and
  `DOUBLE`, plus `SUM(x) FILTER (WHERE …)` to pin that `keep` stays indexed by
  absolute row.
- Existing suites: `bench/duckdiff.py` (404 cases), TPC-H 22-query correctness
  at SF 0.01, 0.1 and 1, wave 120's correlated fuzz, wave 122's stats tests.

## Measurement

1. `bench/suite --only tpch` at SF 0.1 and SF 1, all 22 queries, median of 3,
   before (the baseline rebuilt from `HEAD`'s sources in the same tree, since
   this branch is the same commit) and after; targets Q1, Q6, Q12, Q14, Q19,
   plus the whole-suite geomean.
2. Ingest and durable families before/after (must not regress).
3. Phase isolation for Q1/Q6 as in §"Where the time goes".
4. The sparse threshold: Q6 and Q1 folded with `take`-if-sparse on and off.
5. `sample` of Q1/Q6 after the change, top frames in this file.

## Gates (once, at the end)

```
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test -q --workspace --no-fail-fast
cargo test -q -p pinkdb-server --features catalog-sqlite,catalog-postgres,lake-s3 --test catalog
python3 bench/duckdiff.py
bench/suite TPC-H correctness, all 22, SF 0.01 and 0.1
the CI regression bench
RUSTC=$(rustup which --toolchain stable rustc) ~/.cargo/bin/cargo +stable build -p pinkdb --target wasm32-unknown-unknown
```

## Measured threshold for materializing a selection

The aggregate reads through the selection; a sparse selection could be cheaper
as a compacted copy. `bench` on Q1 (98 % of rows kept, eight arguments) and Q6
(1.8 % kept, one argument), SF 1, median of 7, back to back:

| `MATERIALIZE_DIVISOR` | behaviour | Q1 | Q6 |
|---|---|---|---|
| 1 | always `take` | 138.33 ms | 13.42 ms |
| 8 | `take` below 12.5 % | **58.46 ms** | **11.89 ms** |
| 1 << 20 | never `take` | 66.08 ms | 16.68 ms |

Both extremes lose: copying a dense selection costs more than the indirection
(Q1), and indirect reads of a sparse one lose locality (Q6). For these two
queries every divisor in 2..=55 behaves the same, so 8 is a middle value, not a
sharp optimum. The same binary re-measured after the three builds read 64.6 ms
/ 13.1 ms, i.e. ~10 % drift from a busier machine — the differences above are
far outside it.

## Results

All numbers are `bench/suite --only tpch --runs 3` on the same machine, back to
back: baseline from `git show HEAD:` sources, then the branch build. Load
2.5–3.2 (the machine could not be made quieter than that).

### TPC-H SF 1 (ms, pinkdb; DuckDB median and the ratio after)

| query | before | after | DuckDB | ratio before → after |
|---|---|---|---|---|
| q1 | 164 | **64** | 13 | 12.62× → 4.57× |
| q2 | 190 | 173 | 4 | 47.50× → 43.25× |
| q3 | 180 | 180 | 9 | 22.50× → 20.00× |
| q4 | 165 | 161 | 7 | 23.57× → 23.00× |
| q5 | 332 | 342 | 9 | 36.89× → 38.00× |
| q6 | 80 | **13** | 3 | 26.67× → 3.25× |
| q7 | 262 | 254 | 9 | 29.11× → 28.22× |
| q8 | 239 | 209 | 9 | 26.56× → 23.22× |
| q9 | 713 | 702 | 26 | 27.42× → 27.00× |
| q10 | 169 | 165 | 16 | 10.56× → 10.31× |
| q11 | 124 | 128 | 3 | 41.33× → 42.67× |
| q12 | 165 | 172 | 9 | 18.33× → 19.11× |
| q13 | 442 | 440 | 39 | 11.33× → 11.00× |
| q14 | 80 | 73 | 8 | 10.00× → 9.12× |
| q15 | 35 | 25 | 5 | 7.00× → 5.00× |
| q16 | 60 | 45 | 10 | 6.00× → 4.50× |
| q17 | 89 | 84 | 8 | 11.12× → 12.00× |
| q18 | 1499 | 1500 | 22 | 68.14× → 68.18× |
| q19 | 115 | **50** | 10 | 11.50× → 5.00× |
| q20 | 616 | 609 | 10 | 61.60× → 60.90× |
| q21 | 2300 | 2371 | 18 | 127.78× → 131.72× |
| q22 | 152 | 154 | 12 | 12.67× → 14.00× |
| **geomean** | **21.56×** | **17.30×** | | −19.8 % |

Row counts and checksums identical to DuckDB's on every query, at SF 0.01,
0.1 and 1.

The four filter-heavy targets moved: Q1 2.6×, Q6 6.2×, Q19 2.3×, Q16 1.3×.
Q12 and Q14 are join-shaped — their cost is the join and the projection, not
the filter — q12 165→172 and q14 80→73 are inside the load drift above.
Q18/Q21 (1499→1500, 2300→2371) do not touch this path: they are join and
subquery bound.

### TPC-H SF 0.1 (ms, pinkdb)

| query | before | after | query | before | after |
|---|---|---|---|---|---|
| q1 | 17 | **7** | q12 | 16 | 16 |
| q2 | 19 | 19 | q13 | 41 | 39 |
| q3 | 18 | 18 | q14 | 7 | 6 |
| q4 | 16 | 16 | q15 | 2 | 2 |
| q5 | 30 | 30 | q16 | 6 | 6 |
| q6 | 8 | **1** | q17 | 7 | 7 |
| q7 | 30 | 30 | q18 | 25 | 25 |
| q8 | 18 | 18 | q19 | 11 | **5** |
| q9 | 60 | 60 | q20 | 41 | 41 |
| q10 | 16 | 16 | q21 | 235 | 240 |
| q11 | 13 | 13 | q22 | 14 | 14 |
| geomean | 6.35× | **5.04×** | | | |

SF 0.01: all 22 pass; Q1 1 ms, Q6 0 ms (below the 1 ms timer resolution).

### Ingest and durable (must not regress)

| case | before | after |
|---|---|---|
| lineitem CSV | 2.10 M rows/s | 2.11 M rows/s |
| lineitem Parquet | 0.80 M rows/s | 0.80 M rows/s |
| orders CSV | 1.39 M rows/s | 1.44 M rows/s |
| orders Parquet | 1.05 M rows/s | 1.05 M rows/s |
| durable Sync inserts / selects / checkpoints | 7744.9 / 54.6 / 50.4 ms | 7746.8 / 56.2 / 49.8 ms |
| durable Batch | 64.4 / 28.3 / 45.6 ms | 66.5 / 28.4 / 44.1 ms |
| CLI autocommit / batched | 115930 / 373004 rows/s | 113944 / 379048 rows/s |
| multi-process speedup | 1.00× | 1.07× |

### Profiles

Before (Q1, 148 ms, `sample` of 400 statements) — the filter's copy dominates:
`Vector::push_from` 9.6 + 1.8 %, `eval_arith` 8.2 + 1.6 %, rayon
plumbing/idle ~30 %, `fold_chunk` 5.0 %, `Acc::add_decimal` 2.6 + 1.3 %,
`Vector::push_decimal` 2.4 + 2.0 %, `filter_chunk → gather → push_from` the
single largest caller chain.

After (Q1, median ~58–65 ms, top of stack over ~2300 samples):

| frame | share |
|---|---|
| `expr::eval_arith` | 28 % |
| `aggregate::Acc::add_decimal` | 20 % |
| `__psynch_cvwait` (rayon idle) | 18 % |
| `aggregate::fold_chunk` | 16 % |
| `expr::coerce_to` | 15 % |
| `vector::Vector::encode_into` | 7 % |
| `memmove` / `__floattidf` | 6 % / 6 % |
| `select_predicate` + `retain_cmp` | 3 % |

`push_from` and `push_decimal` are gone from the top frames; the filter is no
longer a phase. What remains is the arithmetic feeding the accumulators and the
accumulator updates themselves, which is 123b/123c work (fusing argument
evaluation into the accumulator loop, and a specialised path per aggregate).

### What is left undone

1. **Typed `CASE` scatter.** Measured: Q14 SF 1 is 113.51 ms with the `CASE`
   and 106.64 ms with the branch replaced by its `THEN` expression (median of
   5 each), so the per-row `Value` path costs ~7 ms, 6 % of that query, and
   less elsewhere. Rewriting `eval_case`/`eval_coalesce` (every `CASE` in the
   engine) is a regression risk out of proportion to that, so it moved to
   123b, where the projector is rewritten anyway.
2. **Fusing argument evaluation into the accumulator loop** — Q1's remaining
   ~32 ms of `eval_arith` + `Acc::add_decimal`. Needs the accumulator to own
   the arithmetic (123c), because `SUM(a * b)` cannot be decomposed into
   per-operand accumulators without changing results.
3. **`IN`-lists** have no dedicated kernel: they arrive as `OR` chains and use
   the narrowing `OR` path, which is where their win comes from. A separate
   `IN` kernel would only help `VARCHAR` lists, which no TPC-H query uses.
4. **Nested filters** (`Filter` directly over `Filter`) materialize at the
   inner one; only `Aggregate`-over-`Filter` and `COUNT(*)`-over-`Filter` read
   through a selection.
