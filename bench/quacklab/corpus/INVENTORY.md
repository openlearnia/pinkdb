# QuackLab query-path inventory (# chart corpus)

Every SQL template QuackLab builds for the read path (dashboards, semantic apps,
filter pickers), with the corpus case that runs it. The corpus is generated from
QuackLab's own compiler, never written by hand:

```
GRAIN=~/Desktop/projects/grain/grain bench/quacklab/corpus/generate.sh
```

`generate.sh` runs `emit/main.go` inside QuackLab's Go module through
`go run -overlay` (same trick as `../compile/compile.sh`), so the QuackLab
checkout is never written to. It turns each `<model>.queries.json` spec into the
SQL `CompileQuery` / `CompileMemberValues` / `semanticompile.ApplyDashboardFilters`
produce and writes `<model>.sql.json` (`{name: {sql, entryPoint, spec}}`).

Models compiled here (of the four in `fixtures/shared/models/`);
`CORPUS=charts` replays them against DuckDB and pinkdb and compares results:

| model | cases |
| --- | --- |
| `nyc_taxi_essential1` | 84 |
| `single_source_by_category_required_attrs` | 20 |
| `shared_view_id_collision` | 17 |

`multi_source_by_day_derived` is not compiled: `physical.BuildTableSpecs`
rejects the fixture at this commit — `marts.by_day.views.by_day.measures:
Aggregation "avg" for measure "aov" cannot be pre-aggregated across multiple
physical sources. Supported multi-source aggregations: sum, count, min, max.`
(`internal/validation/compiler_capabilities.go:168`). Its golden fixture is
stale relative to that rule; when it becomes loadable, add it to
`generate.sh`'s model list with a spec file of its own.

## 1. `go/internal/compiler/semantics/query.go` — the query compiler

### `CompileQuery` (:320) skeleton

| emits | line | cases |
| --- | --- | --- |
| `SELECT <parts> FROM <schema>.<table>` | `query.go:403` | every `query` case |
| dimension: `<COL> AS <COL>` + `GROUP BY <COL>` | `query.go:349` | `rollup_no_dims_borough`, `order_two_members`, `dashboard_filter_select` |
| time dimension: `date_trunc('<g>', <COL>) AS <COL>`, same expr in `GROUP BY`/`ORDER BY`, forces rollup | `query.go:381-385` | `granularity_day/week/month/quarter/year`, `rollup_granularity` |
| measure without rollup: `<COL> AS <COL>` (raw stored value) | `query.go:294` | `agg_avg_no_rollup`, `rollup_none_grain_all`, `norollup_grain_all`, `daily_sku_norollup` |
| measure with rollup: `SUM`/`AVG`/`MIN`/`MAX(<COL>) AS <COL>` | `query.go:300-310` | `agg_sum_dims`, `agg_avg_dims`, `rollup_no_dims`, `rollup_subset`, `category_monthly_rollup` |
| `WHERE a AND b …` (date ranges first, then filters) | `query.go:412` | `filter_and_time_range`, `dashboard_filter_date` |
| `GROUP BY <exprs>` only when rollup and a grain exists | `query.go:417` | `rollup_subset`, `granularity_*` |
| `ORDER BY <expr> ASC\|DESC` (selected members use the SELECT expression, others the quoted column) | `query.go:440` | `order_dim_asc_limit`, `order_measure_desc_limit`, `order_two_members`, `order_measure_not_selected` |
| `LIMIT n` when `n > 0`, clamped to `QueryMaxLimit` = 10 000 | `query.go:443-449` | `order_limit_clamped` (20 000 → 10 000), `order_dim_asc_limit`, `limit_negative` (no clause) |

Rollup decision (`needsFurtherRollup`, `query.go:60`): no dimensions, a selected
SQL set smaller than the view grain, or a grain member not selected. Covered by
`rollup_no_dims` (0 dims), `rollup_subset` (subset of grain → GROUP BY),
`rollup_none_grain_all` (whole grain → raw rows), `rollup_extra_non_grain_dim`
(`hour_id` + `day_id` vs. the `by_hour_day` grain → GROUP BY).

Measures by aggregation: the corpus models only declare `sum` and `avg`
(`21 × sum`, `9 × avg` across all four fixture models), so `MIN`/`MAX`/`COUNT`
rollups (`query.go:306-310`) are unreachable from fixtures; they are plain
templates with no cases here. `CompileNonAdditive` (`rollup` over a stored rate
that is not sum/avg/min/max) likewise has no fixture. Ratio measures are
approximated: rolling up `AVG(x)` is an unweighted mean of the per-day value
(`query.go:303` comment).

### `compileFilter` (:136) — every operator

Values are typed by the member's dimension type (`memberType` `query.go:92`;
measures count as `number`), then quoted by `typedLiteral` (`query.go:100`):
number → raw token (:104), boolean → `true`/`false` for `true|1|yes` /
`false|0|no` (:109), time → `DATE '<v>'` for `YYYY-MM-DD` else
`TIMESTAMP '<v>'` for RFC3339 / `2006-01-02 15:04:05` / `2006-01-02T15:04:05`
(:117-122), everything else → doubled-quote string literal (:127).

| operator | emits | line | cases |
| --- | --- | --- | --- |
| `set` | `<COL> IS NOT NULL` | :147 | `filter_set` |
| `notSet` | `<COL> IS NULL` | :149 | `filter_not_set` |
| `contains` | `CAST(<COL> AS VARCHAR) ILIKE '%<pat>%' ESCAPE '\'` | :166-172 | `filter_contains`, `filter_contains_percent`, `filter_contains_quote`, `filter_contains_underscore` |
| `startsWith` | `CAST(<COL> AS VARCHAR) ILIKE '<pat>%' ESCAPE '\'` | :166-172 | `filter_starts_with`, `filter_starts_with_escape`, `filter_starts_with_quote`, `dashboard_filter_select` (via `in`) |
| `in` | `<COL> IN (a, b)` | :174-179 | `filter_in`, `filter_in_months`, `dashboard_filter_select` |
| `notIn` | `(<COL> NOT IN (…) OR <COL> IS NULL)` | :180 | `filter_not_in` |
| `between` | `<COL> BETWEEN a AND b` (exactly two values) | :183-191 | `filter_between_number`, `filter_between_string`, `filter_between_time`, `filter_between` |
| `equals` | `<COL> = <lit>` | :199 | `filter_equals_string`, `filter_equals_number`, `filter_equals_boolean`, `filter_equals_time`, `filter_equals_timestamp`, `filter_measure_gt`, `filter_equals` |
| `notEquals` | `<COL> IS DISTINCT FROM <lit>` | :201 | `filter_not_equals`, `filter_not_equals_nullable` |
| `gt` / `gte` / `lt` / `lte` | `<COL> > / >= / < / <= <lit>` | :203-209 | `filter_gt`, `filter_gte`, `filter_lt`, `filter_lte`, `filter_measure_gt` |

`likePattern` (`query.go:131`) backslash-escapes `\`, `%` and `_` in the search
text; the ESCAPE character is a backslash. `filter_starts_with_escape` searches
`Midtown_` (would otherwise match `MidtownX`), `values_zone_name_search_backslash`
searches `a\b`.

### `compileDateRange` (:261) and the relative presets (`:217`)

`RelativeDateRanges` (`query.go:255`, 15 tokens) each maps to a half-open
`>= lo AND < hi` pair over `current_date`. Presets that need a calendar boundary
call `date_trunc('<unit>', current_date)` inline (:220).

| preset | `lo` | `hi` | case |
| --- | --- | --- | --- |
| `today` | `current_date` | `(current_date + INTERVAL 1 DAY)` | `preset_today` |
| `yesterday` | `(current_date - INTERVAL 1 DAY)` | `current_date` | `preset_yesterday` |
| `last_7_days` | `… - INTERVAL 6 DAY` | `+ INTERVAL 1 DAY` | `preset_last_7_days` |
| `last_30_days` | `- 29 DAY` | `+ 1 DAY` | `preset_last_30_days` |
| `last_90_days` | `- 89 DAY` | `+ 1 DAY` | `preset_last_90_days` |
| `this_week` | `date_trunc('week', current_date)` | `+ 1 DAY` | `preset_this_week` |
| `last_week` | `date_trunc('week', …) - INTERVAL 7 DAY` | `date_trunc('week', …)` | `preset_last_week` |
| `this_month` | `date_trunc('month', …)` | `+ 1 DAY` | `preset_this_month` |
| `last_month` | `date_trunc('month', …) - INTERVAL 1 MONTH` | `date_trunc('month', …)` | `preset_last_month` |
| `this_quarter` | `date_trunc('quarter', …)` | `+ 1 DAY` | `preset_this_quarter` |
| `last_quarter` | `date_trunc('quarter', …) - INTERVAL 3 MONTH` | `date_trunc('quarter', …)` | `preset_last_quarter` |
| `this_year`, `ytd` | `date_trunc('year', …)` | `+ 1 DAY` | `preset_this_year`, `preset_ytd` |
| `last_year` | `date_trunc('year', …) - INTERVAL 1 YEAR` | `date_trunc('year', …)` | `preset_last_year` |
| `last_12_months` | `date_trunc('month', …) - INTERVAL 11 MONTH` | `+ 1 DAY` | `preset_last_12_months` |

Absolute ranges: `>= DATE '<from>'` (:279), `< (DATE '<to>' + INTERVAL 1 DAY)`
(:281); one-sided and both-sided forms are `range_absolute_from`,
`range_absolute_to`, `range_absolute_both`, plus `granularity_month_absolute`
(range + granularity).

### `CompileMemberValues` (:459) — filter value pickers

| emits | line | cases |
| --- | --- | --- |
| `SELECT DISTINCT <COL> AS <COL> FROM <schema>.<table> WHERE <COL> IS NOT NULL` | :484 | `values_zone_name`, `values_day_id`, `values_hour_of_day`, `values_is_weekend`, `values_borough_name`, `values_service_zone`, `values_do_zone_name`, `values_month_id`, `values_category_name`, `values_category_id`, `values_brand…` |
| `AND CAST(<COL> AS VARCHAR) ILIKE '%<likePattern(search)>%' ESCAPE '\'` | :486 | `values_zone_name_search_percent`, `…_underscore`, `…_quote`, `…_backslash`, `values_month_id_search`, `values_brand_search_quote`, `values_brand_search_underscore`, `values_category_name_search` |
| `ORDER BY 1 LIMIT n` with `n` capped at `MemberValuesMaxLimit` = 200 (:455, :488) | :488 | `values_zone_name_search_limit` (limit 2), `values_day_id_limit3`, `values_category_id_limit`, `values_month_id_limit` |

The dashboards' picker request always passes `limit` 0
(`semantic_member_values.go:52`) so the cap applies; the specs also use non-zero
limits because `emit` passes them straight through.

### `widget.go` — no SQL

`ValidateWidgetQuery` (`widget.go:44`) rejects impossible widget/query pairings
(kpi needs exactly 1 measure and 0 dimensions; bar/line/area ≤ 4 measures and
≤ 2 dimensions; pie 1/1; scatter 1/2), `CanonicalWidgetTypes` `widget.go:10`,
`AxisDimensionCount` `widget.go:32`, `ValidateWidgetMarkdown` `widget.go:96`.
`NormalizeQuerySemanticID` (`widget.go:21`) only defaults an empty `semanticId`
to the widget's semantic model. Nothing here changes the SQL, so these are
covered only indirectly: the corpus specs are all valid widget queries, and one
`semanticId`-less form is unnecessary (the compiler requires an id either way).

### `emit.go` — catalog, not a query template

`EmitCatalog` (`emit.go:19`) is what turns a model into the `SemanticsCatalog`
`CompileQuery` consumes: one `SemanticModel` per mart view with dimension
`SQL = naming.Column(levelID)` and `Type = normalizeDimensionType(level.Type)`
(`emit.go:435`: number/integer/decimal/float/double/numeric → `number`,
time/date/timestamp/… → `time`, boolean/bool → `boolean`), measures with
`Aggregation` (default `sum`) and `Additive`. `columnFromMetricExpr`
(`emit.go:413`) strips `SUM(...)`/`AVG(...)` so a measure is a bare column, which
is why rollup re-aggregates a column rather than nesting aggregates. Those types
drive which filter literal form is emitted — covered by the
`filter_equals_{string,number,boolean,time,timestamp}` and `values_is_weekend`
(boolean), `values_day_id` (time) cases. `format.go` (`NormalizeMeasureFormat`
:15, `QueryMemberDisplayMeta` :34) is presentation metadata and emits no SQL.
`WithQueryIndex` (`emit.go:51`) is an in-memory lookup index.

## 2. `go/pkg/semanticompile` — the dashboard wrapper

| function | where | SQL effect | case |
| --- | --- | --- | --- |
| `CompileReadOnly` | `pkg/semanticompile/compile.go:73` | `semantics.CompileQuery` + read-only check | all `query` cases |
| `CompileWidget` / `CompileWidgetJSON` | `compile.go:32` / `:62` | widget validation, then `CompileReadOnly` | same |
| `ValidateWidgetJSON` | `compile.go:93` | none | — |
| `ApplyDashboardFilters` | `pkg/semanticompile/filters.go:55` | merges dashboard filters into the widget query *before* compiling: a `date` filter appends `SemanticTimeDimension{Dimension: <bound member>, DateRange}` (→ the `compileDateRange` range), a `select` filter appends `SemanticFilter{Operator: in}` (:70-81); widgets listed in `ExcludeWidgetIDs` are skipped (`excluded` `filters.go:86`) | `dashboard_filter_date`, `dashboard_filter_select`, `dashboard_filter_excluded` |

`ValidateDashboardFilters` (`filters.go:29`) bounds the request (≤ 16 filters,
≤ 200 values) and emits no SQL. `emit` drives `ApplyDashboardFilters` with the
widget id `w0`, which is why `dashboard_filter_excluded` carries
`excludeWidgetIds: ["w0"]`.

## 3. `go/internal/httpapi` — callers of the compiler

| route / function | where | what it builds | drivable through Go? |
| --- | --- | --- | --- |
| hosted semantic-app batch execute | `hosted_semantic_apps.go:366` `compileReadOnlySemanticSQL` → `semanticompile.CompileReadOnly`; per widget `compileAndExecuteSemanticWidget` `:583` | `CompileQuery` per widget, then `executeWarehouseSQL[OnTarget]` | yes, via the compiler (that is exactly what the corpus does); the route itself needs a server + warehouse |
| widget compile endpoint | `hosted_semantic_apps.go:358` `compileSemanticWidgetSQL` → `semanticompile.CompileWidget` | `CompileQuery` | yes |
| dashboard cross-filter merge | `hosted_semantic_apps.go:575` `ApplyDashboardFilters` (called from `executeOneSemanticWidget` `:524`, after `NormalizeQuerySemanticID` and `ValidateWidgetQuery` `:559`) | range + `in` filter merge | yes — `emit` calls `ApplyDashboardFilters` directly |
| member-values picker | `semantic_member_values.go:52` → `CompileMemberValues(…, 0)`, executed with `semantics.MemberValuesMaxLimit` (`:107`, `:185`) | `CompileMemberValues` | yes |
| semantic query endpoints | `hosted_proxy.go:429`, `:497` → `semantics.CompileQuery` (comment `:446`: "SemanticQuery in → DuckDB SQL (CompileQuery) → read-only execute") | `CompileQuery` | yes |
| agent warehouse tools | `internal/ai/warehouse_tools.go:318` (`compileSemanticQuery`), `:510` (batch `create_dashboard` widgets) | `CompileQuery` | yes |
| SQL execution gate | `internal/httpapi/ai_warehouse_sql.go:28`, `:61` → `authz.IsReadOnlySQL` (`internal/authz/sql.go:11`); `parseHostedSQLExecResult` `:104` caps rows | none (post-processing) | n/a |
| sys dashboard / publish / reuse | `sys_dashboard.go`, `dashboard_publish.go:171`, `semantic_reuse.go:54`, `:112` | none — catalog reads, payload building, reuse keys | n/a |

`sys_dashboard.go` and `dashboard_publish.go` contain no `SELECT`: they read
stored dashboard JSON and call the compiler through the paths above.
`semantic_reuse.go` only fingerprints `SemanticQuery` (`semanticQueryReuseKey`
`:57`, `semanticWidgetReuseKey` `:112`).

## 4. `go/internal/physical/mart_select.go`

No SQL. `SelectMartsToRefresh` (:7) and `martClosureTouchesUploaded` (:37) pick
which marts a refresh touches; they never emit a statement.

## 5. Window functions, ranking and `map()` — not on this path

Every `OVER (…)`, `RANK()`, `ROW_NUMBER()`, `LAG`/`LEAD`, `FIRST_VALUE`/
`LAST_VALUE` and padding-join in QuackLab is emitted by
`internal/compiler/sqlgen/sqlgen.go` while generating mart SQL:
`rank(expr, 'asc'|'desc')` → `RANK() OVER (ORDER BY expr ASC|DESC)`
(`sqlgen.go:1094`, `:1278`), `SUM(...) OVER (...)` with rolling padding
(`:1171`, `:1174-1180`), `AVG(...) OVER (...)` (`:1211`), `LAG`/`LEAD`
(`:1238`, `:1240`), `FIRST_VALUE`/`LAST_VALUE IGNORE NULLS` (`:1261`, `:1263`),
`ROW_NUMBER() OVER (…) [= 1]` (`:1291-1295`), and `map()` lookups are compiled
as expression functions in the same generator. Those statements are the marts'
`selectSql`, i.e. ETL-side, so they are exercised by the **ETL corpus**
(`CORPUS=etl`, `tools/quacklab-goldens.py`) — not by `CompileQuery`, which only
ever emits the six clause shapes in §1.

## 6. Paths the corpus cannot drive, and why

| template | why no case |
| --- | --- |
| compile errors: unknown semantic/dimension/measure, `between` with ≠ 2 values, missing values, unsupported operation, `MIN/MAX/COUNT` rollup, non-time dimension in `timeDimensions`, duplicate dimension, unknown granularity, empty date range | produce no SQL; `emit` panics so `generate.sh` fails loudly instead of silently dropping a spec. Covered by QuackLab's own Go tests. |
| `MIN`/`MAX`/`COUNT` measure rollup, `CompileNonAdditive` | no fixture declares those aggregations (only `sum`, `avg`) — see §1. |
| `multi_source_by_day_derived` | fixture rejected by `physical.BuildTableSpecs` at this commit (see the top of this file). |
| dashboard publish / reuse / sys dashboard routes | build no SQL (they call the compiler through §3). |
| DuckDB-side wrapping | none: `CompileQuery`'s string is executed verbatim; the only post-processing is `authz.IsReadOnlySQL` and a row cap. |

## 7. Harness contract

`CORPUS=charts` in `tools/quacklab-goldens.py` replays `<model>.sql.json`:

- tables come from the same `tableSpecs` the ETL corpus uses, built with the
  mart specs materialized as tables (the ETL corpus makes them views);
- 198 synthetic rows per table, dates spread over the last 400 days relative to
  `current_date` (every relative preset matches), NULLs in nullable dimension
  and measure columns, and string values containing `%`, `_`, `'` and `\`;
- each case's columns, types-as-text and rows are compared (rows are compared in
  order only when the query has `ORDER BY` and the spec is not marked
  `"unordered": true`, i.e. its ORDER BY keys do not identify a row; floats with
  1e-9 relative tolerance);
- verdicts: `ok`, `rows`, `names`, `types`, `pink-error`, `duck-error`,
  `both-error`; the first differing cell is printed.
