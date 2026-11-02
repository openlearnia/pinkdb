# Chart corpus before-report

Measured 2026-11-01 on `pinkdb-before`
(`~/Desktop/projects/pinkdb-wt/scratch-a/bin/pinkdb-before`) against DuckDB
v2.0.0-alpha46057 (`~/.local/bin/duckdb`) with:

```
GRAIN=~/Desktop/projects/grain/grain bench/quacklab/corpus/generate.sh
CORPUS=charts PINK=~/Desktop/projects/pinkdb-wt/scratch-a/bin/pinkdb-before \
  OUT=$TMPDIR/charts.json python3 ~/Desktop/projects/pinkdb-wt/tools/quacklab-goldens.py
```

The SQL comes from QuackLab's compiler at `24efcd0d` (see `INVENTORY.md`);
121 cases over three fixture models (`nyc_taxi_essential1` 84,
`single_source_by_category_required_attrs` 20, `shared_view_id_collision` 17).
Every case runs on both engines over 198 deterministic rows per mart table
(dates across the last 400 days relative to `current_date`, plus eight fixed
2024 dates so the absolute ranges are non-empty; NULLs in nullable dimensions
and measures; `%`, `_`, `'` and `\` in string values). Column names,
types-as-text and rows are compared (row order only when `ORDER BY` is total;
floats with 1e-9 relative tolerance).

## Before counts

| verdict | cases |
| --- | --- |
| `ok` (names, types, rows identical) | **46** |
| `types` (names and rows identical, type row differs) | 47 |
| `pink-error` (DuckDB ran, pinkdb rejects) | 27 |
| `both-error` (QuackLab emits SQL both engines reject) | 1 |
| `rows` / `names` / `duck-error` | 0 |

**46 of 121 cases match, 121/121 compile and run on DuckDB.** Ignoring the
`int128` spelling in gap 3, 93 of 121 return identical data.

## Gap 1 — pinkdb has no `ILIKE` (20 cases)

`compileFilter` and `CompileMemberValues` search with `ILIKE … ESCAPE '\'`
(`query.go:172`, `:486`). pinkdb's parser stops at the `ILIKE` token:

```sql
SELECT "ZONE_NAME" AS "ZONE_NAME", SUM("TRIPS") AS "TRIPS"
FROM "DAILY_ZONE_PERFORMANCE"."BY_OD_DAY"
WHERE CAST("ZONE_NAME" AS VARCHAR) ILIKE '%100\%%' ESCAPE '\'
GROUP BY "ZONE_NAME" ORDER BY "ZONE_NAME" ASC
```

pinkdb: `Error: parse error: error at 1:143: expected one of at, fetch, for, keyword, qualify, similar, window`

DuckDB returns one row, `Midtown_100% | 4835`. `LIKE '%100\%%' ESCAPE '\'`
alone works on pinkdb (verified directly), so only the `ILIKE` keyword is
missing. Cases: every `contains`/`startsWith` filter (`filter_contains*`,
`filter_starts_with*`, in all three models) and every filter-picker search
(`values_*_search*`, 10 cases).

## Gap 2 — `GROUP BY` on an expression is rejected (7 cases)

A granularity buckets with `date_trunc('<g>', col)` in `SELECT` **and**
`GROUP BY` (`query.go:381-385`). pinkdb rejects the group key even though the
two expressions are textually identical:

```sql
SELECT date_trunc('week', "DAY_ID") AS "DAY_ID", SUM("TRIPS") AS "TRIPS"
FROM "DAILY_ZONE_PERFORMANCE"."BY_DAY"
WHERE "DAY_ID" >= (date_trunc('month', current_date) - INTERVAL 11 MONTH)
  AND "DAY_ID" < (current_date + INTERVAL 1 DAY)
GROUP BY date_trunc('week', "DAY_ID")
ORDER BY date_trunc('week', "DAY_ID") ASC
```

pinkdb: `Error: SELECT/HAVING expression must be an aggregate or appear in GROUP BY`

DuckDB returns the weekly buckets (`2025-12-01 00:00:00 | 156`,
`2025-12-08 00:00:00 | 220`, …). `GROUP BY "DAY_ID"` (the alias) and `GROUP BY 1`
both work on pinkdb, so the binder fails to match function-call group keys.
Cases: `granularity_{day,week,month,quarter,year}`,
`granularity_month_absolute`, `rollup_granularity`.

## Gap 3 — `SUM(INTEGER)` is `BIGINT`, DuckDB says `HUGEINT` (47 cases)

Any case whose projection contains `SUM(<integer column>)` (every `TRIPS`,
`PASSENGERS` rollup) reports `int128` on DuckDB and `bigint` on pinkdb. Names
and rows are identical in all 47 — this is the type row only:

```sql
SELECT SUM("TRIPS") AS "TRIPS", SUM("TOTAL_AMOUNT") AS "TOTAL_AMOUNT"
FROM "DAILY_ZONE_PERFORMANCE"."BY_DAY"
WHERE "DAY_ID" >= current_date AND "DAY_ID" < (current_date + INTERVAL 1 DAY)
```

DuckDB: `int128, double` → `15 | 3.875`; pinkdb: `bigint, double` → the same
row. Affected: every relative preset, the absolute ranges with a sum, the
dashboard-filter cases, `agg_sum_dims`, `rollup_*`, `order_*_limit` and the
filter cases that group and sum.

## Gap 4 — QuackLab orders by a member it does not select (1 case, both engines)

Not a pinkdb gap: when a dashboard sorts by a measure it does not display,
`CompileQuery` appends the member's column to `ORDER BY` although it is neither
selected nor grouped (`query.go:420-441`), so both engines reject the statement:

```sql
SELECT "ZONE_NAME" AS "ZONE_NAME", SUM("TRIPS") AS "TRIPS"
FROM "DAILY_ZONE_PERFORMANCE"."BY_ZONE_DAY" GROUP BY "ZONE_NAME"
ORDER BY "FARE_AMOUNT" DESC
```

Both engines: `Binder Error: column "FARE_AMOUNT" must appear in the GROUP BY
clause or must be part of an aggregate function.` Case:
`order_measure_not_selected`. A widget like this fails on DuckDB too.

## What already matches

46 cases are completely identical, and 93 are identical once gap 3 is
discounted: all absolute date ranges, every filter operator that does not use
`ILIKE` (`set`, `notSet`, `in`, `notIn` incl. the `OR IS NULL` rewrite,
`between` on numbers/strings/time, `equals`/`notEquals` incl. `IS DISTINCT FROM`,
`gt`/`gte`/`lt`/`lte`, boolean and `DATE`/`TIMESTAMP` literals), rollup vs. no
rollup, `SUM`/`AVG` rollups, ordering by dimension or measure with `LIMIT`,
the `LIMIT` clamp, the no-`LIMIT` path, dashboard date/select cross-filters
(including the excluded-widget case), and all member-value lookups without a
search string (with and without a limit).

## The ETL corpus is unchanged (gate evidence)

The ETL gate (`PINK=… PINK_DEFAULTS=1 OUT=$TMPDIR/ql.json python3 …/quacklab-goldens.py
&& python3 …/goldens-check.py $TMPDIR/ql.json`) is red on `pinkdb-before` for a
reason that predates this corpus: pinkdb cannot serve `read_csv` at all —

```
SELECT * FROM read_csv('/tmp/x.csv')
Error: unknown table function "read_csv": the registered table sources do not serve it
```

All 17 `read_csv` statements in the ETL corpus fail on pinkdb (17/17, regardless
of path, `.gz` or `compression=`), and 6 of them are statements DuckDB runs, so
`goldens-check.py` exits 1.

Proof that this is not caused by the `CORPUS=charts` change: the untouched
pre-change driver (`tools/quacklab-goldens.py.pre-charts`) produces a
byte-identical result file and the same six failures:

```
$ diff <(python3 -c 'import json;print(json.load(open("ql.json")))') \
       <(python3 -c 'import json;print(json.load(open("ql-baseline.json")))')
(no output)
Counter({('duck', True): 117, ('pink', True): 111, ('pink', False): 39, ('duck', False): 33})   # both
117 DuckDB-passing statements, 6 failing on pinkdb                                              # both
```

The six: `runtime.stageLoadByTable.DIM_DATE|DIM_PRODUCT` and
`runtime.dimensionConflictCheckByTable.DIM_DATE|DIM_PRODUCT` in
`model_{multi_source_by_day_derived,shared_view_id_collision,single_source_by_category_required_attrs}.json`.
