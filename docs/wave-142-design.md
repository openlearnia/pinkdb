# Wave 142 — DECIMAL with a 64-bit physical type: design note

Status: approved and landed on 2026-11-04 (results at the end).

## Why

Every DECIMAL vector holds `i128` values today (`VectorData::Decimal { values: Vec<i128>, .. }`),
whatever its precision. DuckDB picks the physical type from the precision: `int16` up to 4 digits,
`int32` up to 9, `int64` up to 18, `hugeint` above. All of TPC-H's decimals are `DECIMAL(15,2)`.

The profile at SF10 (wave 141 binary) shows the cost:
- **Q1** (4.6× DuckDB): `Spec::fold`'s accumulate 5,222 samples and argument evaluation 3,611,
  including `eval_decimal_mul` 1,285 and `arith_decimal_const` 889. That is `i128` arithmetic and
  16-byte loads on values that fit in 8 bytes.
- **Q10:** its top cost after 141a, `Vector::take` at 3,049, mostly moves 16-byte decimals.
- **Memory:** a decoded decimal column takes twice the bytes it needs, which feeds the 4.1× peak RSS
  at SF10.

## The change

1. **A new payload variant** `VectorData::Decimal64 { values: Vec<i64>, precision: u8, scale: u8 }`.
   - **Invariant:** a vector of type `DECIMAL(p, s)` holds `Decimal64` when `p ≤ 18`, and the
     existing `Decimal` (`i128`) when `p > 18`. `Vector::dtype()` returns `DECIMAL(p, s)` for both.
   - Only the precision decides the payload, never the values. So two vectors of one type always
     match, and the gathers, takes, concatenations and merges that assume one payload per type stay
     simple.
   - A separate variant, rather than a width flag inside `Decimal`, makes the compiler list every
     match site. There are 68 of them, mostly in `vector.rs`, `exec/expr.rs`, `exec/aggregate.rs`,
     `persist.rs`, `stats.rs` and `group_table.rs`, plus the Arrow, Parquet and FFI conversions.
2. **Constructors.** `VectorData::decimal_from_i128(values, p, s)` narrows to `Decimal64` when
   `p ≤ 18`; `Vector::decimal_at(i) -> i128` reads either payload. Cold paths use these two helpers:
   casts to text, `Value` conversion, Arrow/Parquet/FFI export, and stats folding.
3. **Hot paths get typed `i64` code:**
   - decode from storage (§4);
   - `take`, `select_rows` and the 141a `GatherColumn`;
   - comparisons and filters, including zone-map constants;
   - `+`, `-` and `*` (§5);
   - SUM, AVG, MIN and MAX accumulation (§6);
   - group and join keys (`group_table` key words, join key encoding, hashing);
   - DECIMAL to DOUBLE casts.

## Widening rules (results unchanged)

The binder's result types do not change. They are already DuckDB's:
- `+`/`-` give `p = max(p1−s1, p2−s2) + s + 1`;
- `*` gives `s = s1 + s2` and `p = p1 + p2`, capped at `DECIMAL(18, s)` when both inputs have at
  most 18 digits and `s < 18`, as in DuckDB's `BindDecimalMultiply`;
- `SUM(DECIMAL(p,s))` is `DECIMAL(38,s)`.

The physical type follows the result type, so:

| operation | inputs | result type | how it is computed |
|---|---|---|---|
| `+` / `−` | both `i64` | `p ≤ 18` | `i64` `checked_add`/`checked_sub` after rescaling (rescale is `checked_mul` by a power of ten). Overflow cannot happen for a `p ≤ 18` result whose inputs are in range, but it is checked, and a failure takes today's exact error path through `i128`. |
| `+` / `−` | any | `p > 18` | Widen the inputs to `i128`, compute as today, store `Decimal` (`i128`). |
| `*` | both `i64` | `DECIMAL(18, s)` (capped) | Multiply in `i128` (an `i64 × i64` product always fits), then the existing precision check against 18 digits. In range: narrow to `i64`. Out of range: today's overflow error, same text. |
| `*` | any | `p > 18` | `i128`, as today. |
| `/`, `%` | — | unchanged | They operate on the `i128` view (`decimal_at`); a `p ≤ 18` result is narrowed. These are not hot in TPC-H. |
| `SUM` | `i64` values | `DECIMAL(38,s)` | Accumulate into the existing `i128` sum (an `i64 → i128` add per row). The overflow rule against `DECIMAL(38)` is unchanged. |
| `AVG` | `i64` values | DOUBLE or DECIMAL as today | Accumulate into `i128` as today. |
| `MIN` / `MAX` | `i64` values | same type | Compare in `i64`. |
| cast `DECIMAL(p,s)` → `DECIMAL(p2,s2)` | any | target `p2` | Compute as today in `i128` with today's range check, then store by `p2`. |

**Proof obligations:**
- duckdiff unchanged (766/768 today with the default CLI, 768 with the full features);
- the whole `cargo test` suite;
- new tests at the edges:
  - `DECIMAL(18,0)` at ±999…9;
  - a `DECIMAL(18,4)` product just inside and just past 18 digits (the error text must match today's);
  - a sum of `i64::MAX`-scale values crossing into `i128`;
  - `+`/`−` at the 18-digit edge;
  - casts 18 → 19 and 19 → 18;
  - group by and join on `DECIMAL(15,2)` against `DECIMAL(20,2)`, through a common-type cast.

## Storage

The on-disk encoding does not change in this wave. Stream B is changing the encoders,
byte-identically. Decimal pages are integer-encoded today (bit-packing or FastLanes over the
unscaled value). For `p ≤ 18` the decoder writes straight into `Vec<i64>` instead of widening to
`i128`; for `p > 18` it is unchanged. Zone maps and statistics keep their `i128` form.

## Measurement

- An interleaved A/B against main for Q1, Q6, Q14, Q19 and Q10 at SF1 and SF10, plus the full
  22-query `tpch-ab` at both scales.
- Peak RSS for the same queries.
- Run under the build lock's new exclusive benchmark mode when it is available.

## Not in scope

`int16` and `int32` physical types for `p ≤ 9` (DuckDB has them). They would halve again for small
decimals, but TPC-H gains nothing from them, so this is `i64` only.

## Results (2026-11-04)

Interleaved A/B against `main` 88f1d94, run in the build lock's exclusive bench mode, on the shared
`cmp/` stores:

| | geomean | notable |
|---|---|---|
| SF1, all 22 queries, 3 rounds | 0.975 | Q7 0.88, Q6 0.89, Q1 0.92 |
| SF10, all 22 queries, 2 rounds | **0.869** | Q19 0.31, Q8 0.59, Q1 0.68, Q10 0.70, Q7 0.74, Q9 0.83 |
| SF10, Q1/Q6/Q14/Q19/Q10, 5 rounds | 0.752 | Q1 0.70, Q6 0.90, Q14 1.00, Q19 0.55, Q10 0.69 |

**Q17 at SF10: 1.065 over 10 rounds, not traced to decimal code.** A/B of its parts:
- the decimal `avg` subquery: 0.74;
- the decimal filter: 0.85;
- the join plus `sum(l_extendedprice)`: 1.10 in one run and 1.01 in the next;
- the same join with `count(*)`, no decimals: 1.03.

Profiles of the two builds show no new hotspot. It is recorded as unexplained, most likely code
layout and run-to-run variance on a ~100 ms join.

**Peak RSS at SF10:** Q6 3,308 → 1,931 MB. Q1, Q9, Q10, Q18 and Q19 are within ±3 %: their peak is
the decoded cache of non-decimal columns and operator state, not decimals.

**Gates:** the full gate passes 82/82. The `tests/decimal64.rs` expectations were taken from the
pre-change binary's output.
