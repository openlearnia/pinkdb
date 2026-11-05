# SQL dialect conformance (wave 95)

What pinkdb's SQL accepts, where it differs from DuckDB 2.0.0, and what it
refuses — with the exact error. Every row below was checked by running the
form through the CLI (`pinkdb -no-agent -c …`) against
`/tmp/duckdb-2.0.0/duckdb` as the reference; results are pinned in
`crates/pinkdb/tests/sql_conformance.rs`, which re-measures them on DuckDB
when that binary is present.

Row 95 is big, so it is split:

| batch | scope | state |
|---|---|---|
| **95a** | the query forms: `QUALIFY`, `TRY_CAST`, subscripts, `IS [NOT] DISTINCT FROM`, `VALUES`/`TABLE`, `FETCH FIRST`, `LIKE … ESCAPE`, `SIMILAR TO`, `DISTINCT ON`, aggregate `FILTER`, named `WINDOW`, `GROUPING SETS`/`ROLLUP`/`CUBE` + `GROUPING()`, `WITH RECURSIVE`, set operations with `ALL` | **done** (this page) |
| **95b** | the DML forms: `INSERT … ON CONFLICT` (and `INSERT OR REPLACE` / `INSERT OR IGNORE`), `UPDATE … FROM`, `DELETE … USING`, `RETURNING`, `MERGE`, data-modifying CTEs, primary/unique key enforcement | **done** (this page) |
| **95c** | the rest: `LATERAL`, ordered-set aggregates (`WITHIN GROUP`), `TABLESAMPLE`, `FOR UPDATE`/`SHARE`, `AT TIME ZONE`, row constructors, list literals/`LIST`, `FILTER` on a window aggregate, windows built on another window, VARIANT slicing, the JSON mutation functions, `SET VARIABLE`/`$name` | **done** (this page) |
| **slt-2** | the corpus-driven batch: `PRAGMA` statements, DuckDB's settings catalogue, `ATTACH` a plain database, `count()`, `GROUP BY ALL`/`ORDER BY ALL`, `CHAR(n)`, keyword-named columns, `default_null_order`/`default_order` | **done** (this page) |

Legend: **✅ supported** (same answer as DuckDB 2.0.0 on the shared corpus) ·
**≈ divergent** (accepted, different from DuckDB, in the stated way) ·
**✗ refused** (the exact message follows).

## Query forms — batch 1 (95a)

| form | status | notes (DuckDB 2.0.0 reference) |
|---|---|---|
| `QUALIFY expr` | ✅ | Runs after `WINDOW` and before `ORDER BY`/`LIMIT`, like DuckDB (`SELECT … WINDOW w AS (…) QUALIFY s > 10` is accepted; `QUALIFY` before `WINDOW` is rejected by both). The predicate may name select aliases, aggregates and window functions, and becomes a filter over the window output. |
| the fixed-size `ARRAY` (`DOUBLE[3]`, `INTEGER[4]`, `BIGINT[2]`), `[a, b, c]`, `array_value(…)`, `a[i]`, `array_extract`, `len`/`array_length` | ≈ | Same answers as DuckDB 2.0 for literals and the numeric tower (`[1, 2.5]` is `DOUBLE[2]`), 1-based forgiving element access (out of range, `NULL` index, `NULL` array and `NULL` element all give `NULL`), `len`, casts (`VARCHAR ↔ ARRAY`, `ARRAY ↔ ARRAY` at the same length, `ARRAY ↔ VARIANT`), structural equality including `NULL` elements, and the lexicographic order `ORDER BY`/`MIN`/`MAX` use. Differences: `FLOAT[N]` is `DOUBLE[N]` (pinkdb has no 32-bit float scalar); `FLOAT`/`DOUBLE` are one type; a bare `[1, 2, 3]` is a fixed-size `INTEGER[3]` rather than a `LIST`; and `array_value`/`[a, b]` accept any per-row expression, which DuckDB spells `list_value`. A variable-length list is refused (see below). |
| `TRY_CAST(expr AS type)` | ✅ | `NULL` when the cast fails, for every target type including `VARIANT`; the same code path as `CAST`, so a *missing* conversion (e.g. `DATE → BOOLEAN`) is still an error rather than `NULL`. |
| VARIANT subscripts `v['a']`, `v[0]`, `v.a` | ✅ | 1-based array indices like DuckDB (`[0]` errors with DuckDB's wording, out of range and missing keys are SQL `NULL`). Chains: `js['a']['b']`, `js.a[2]`, `js.a['b']`. `v.a` is `VARIANT`, never a plain scalar. |
| string subscripts/slices `'hello'[2]`, `'hello'[2:4]`, `'hello'[-1]` | ✅ | 1-based, character-wise (not bytes), negative counts from the end, out-of-range clamps, `from > to` is `''`. |
| `IS [NOT] DISTINCT FROM` | ✅ | NULL-safe equality (`1 IS DISTINCT FROM NULL` is true, `NULL IS NOT DISTINCT FROM NULL` is true). Desugared to `COALESCE(a = b, FALSE) OR (a IS NULL AND b IS NULL)`. |
| `VALUES (…), (…)` as a query | ✅ | Column names `col0`, `col1`, … like DuckDB; a trailing `ORDER BY`/`LIMIT` works. |
| `(VALUES …) t(x, y)` in `FROM` | ✅ | Column aliases rename positionally, on the first `FROM` source and on join sources alike (`… CROSS JOIN (VALUES (9)) u(z)`); arity mismatch is a bind error. Cells must be constants (no column references or subqueries) and each column takes the common type of its cells. |
| correlated subqueries: `EXISTS`/`NOT EXISTS`, `IN`/`NOT IN`, `= ANY`/`<> ALL` (all comparison quantifiers), scalar subqueries | ✅ (wave 120) | A subquery may reference a column of any enclosing block, at any depth, in `WHERE`, `SELECT`, `HAVING`, `ON` and `CASE`. Truth is three-valued: `1 NOT IN (2, NULL)` is NULL, `1 NOT IN (1, NULL)` is FALSE, `NOT IN` over an empty set is TRUE, `NULL = ANY (…)` is NULL; a scalar subquery gives NULL for zero rows and DuckDB's error for more than one row, and `count(…)` over no rows is 0. The planner decorrelates into semi/anti/mark hash joins, or an aggregate over the correlation keys plus a left join, so a correlated query is not rerun per row; the shapes it cannot unnest yet (a body with `LIMIT`/`DISTINCT`/`GROUP BY`, correlation two levels deep, a correlation with no equality key, a scalar under `NOT`, and subqueries in `ON`/`ORDER BY`/`GROUP BY`/window arguments) run once per distinct outer key, which is correct and linear. `EXPLAIN` shows which plan you got. A correlated body in a grouped block may only read group keys (a bind error otherwise, as in DuckDB). `(a, b) IN (SELECT …)` stays refused (DuckDB also refuses it). |
| `TABLE t` | ✅ | Parsed as `SELECT * FROM t`; trailing `ORDER BY`/`LIMIT` accepted like DuckDB. |
| `FETCH FIRST n ROWS ONLY` | ✅ | Also `FETCH NEXT`/`ROW`; `OFFSET n ROW\|ROWS` accepted too. `LIMIT` and `FETCH FIRST` together are refused (`LIMIT cannot be combined with FETCH FIRST`) — DuckDB is a syntax error there. |
| `LIKE … ESCAPE 'x'` | ≈ | Supported, plus the `like_escape(s, p, esc)` scalar. The escape string must be empty or one character (DuckDB's message, verbatim). Divergence: pinkdb has no default escape character, so `'a%b' LIKE 'a\%b'` without `ESCAPE` matches nothing extra — DuckDB behaves the same. |
| `SIMILAR TO pattern` | ≈ | Implemented as a **fully anchored regular expression**, which is what DuckDB 2.0 actually does (`%` and `_` are literals, `.*` is the wildcard). Supports literals, `.`, `[...]`/`[^...]` ranges, `*`, `+`, `?`, `{m}`, `{m,}`, `{m,n}`, `(…)`, `\|`, `\x` escapes and `^`/`$`; patterns over 1024 characters or repetitions over 1000 fail with `SIMILAR TO pattern too complex`. Divergence: `SIMILAR TO … ESCAPE` is refused (DuckDB refuses it too) and a malformed repetition such as `a{1,2,3}` is read literally instead of raising DuckDB's regex error. |
| `DISTINCT ON (…)` | ✅ | Keeps the first row per key in `ORDER BY` order, like DuckDB; the key need not be selected and `ORDER BY` need not contain it. Combined with plain `DISTINCT` is refused (`DISTINCT and DISTINCT ON cannot be combined`); with plain `DISTINCT` semantics also reachable via `GROUP BY`. |
| aggregate `FILTER (WHERE …)` | ✅ | `sum(a) FILTER (WHERE a > 1)`, `count(*) FILTER (…)`, `count(DISTINCT a) FILTER (…)`; the filtered rows contribute nothing (not a zero). Never takes a fused aggregate fast path, so it cannot silently read unfiltered columns. |
| named `WINDOW w AS (…)` | ✅ | Definitions may reference select aliases and each other only left-to-right; `OVER w` resolves through them. A window may build on another window (95c): see the batch-3 table. |
| `GROUP BY GROUPING SETS (…)` | ✅ | Sets, `ROLLUP(…)` (longest prefix first) and `CUBE(…)` (DuckDB's order); multiple group-set items cross-product with the plain keys. Implemented as a union of aggregates, so `HAVING`/`ORDER BY`/`LIMIT` apply once. `CUBE` over more than 12 expressions is refused. |
| `GROUPING(a, b)`, `GROUPING()` | ✅ | Big-endian mask like DuckDB: `GROUPING(a, b) = 2·g(a) + g(b)`; the zero-argument form is `0` for detail sets and `1` for the grand total. Naming a non-grouped expression is an error. |
| `WITH RECURSIVE r(…) AS (anchor UNION [ALL] step)` | ✅ | Semi-naive iteration: the step sees only the rows the previous round added, `UNION` (not `ALL`) also drops rows already produced, and iteration stops when the step adds nothing. Bounds: **100 000 iterations** (`recursive CTE "r" still produced rows after 100000 iterations`) and **10 000 000 rows** (`recursive CTE "r" passed 10000000 rows`) — a runaway recursion is an error, never a hang. The step's columns are cast to the anchor's types. |
| `UNION ALL` / `INTERSECT ALL` / `EXCEPT ALL` | ✅ | Multiset semantics, verified on the shared corpus. Plain `UNION`/`INTERSECT`/`EXCEPT` unchanged. |
| `CORRESPONDING` | ✗ | `parse error` — DuckDB 2.0 also rejects `UNION CORRESPONDING`. Documented, not planned. |

## DML forms — batch 2 (95b)

| form | status | notes (DuckDB 2.0.0 reference) |
|---|---|---|
| `INSERT … ON CONFLICT DO NOTHING` | ✅ | The conflict target is optional; with one (`ON CONFLICT (a)`), only that constraint is checked and a hit elsewhere still errors, as in DuckDB. |
| `INSERT … ON CONFLICT [(cols)] DO UPDATE SET … [WHERE …]` | ✅ | `excluded.col` is the proposed row and the target's own columns read the row already stored; the optional `WHERE` is evaluated on the existing row and skips the update when false. The target must be a declared `PRIMARY KEY` or `UNIQUE` constraint (same refusal as DuckDB). Within one statement a duplicated key keeps the **first** row, like DuckDB. |
| `INSERT OR REPLACE …` | ✅ | Equivalent to `ON CONFLICT DO UPDATE SET` over the columns the statement writes; with a partial column list the unlisted columns keep their **existing** values (the DuckDB behaviour, not defaults). |
| `INSERT OR IGNORE …` | ✅ | Equivalent to `ON CONFLICT DO NOTHING` for **every** key, not just the named one. |
| `INSERT … ON DUPLICATE KEY UPDATE …` | ✗ | `INSERT ... ON DUPLICATE KEY UPDATE is not supported: write ON CONFLICT ... DO UPDATE SET` (DuckDB accepts the MySQL spelling; pinkdb takes the standard one only) |
| `REPLACE INTO …` | ✗ | `REPLACE INTO is not supported (DuckDB has no REPLACE INTO): spell it INSERT OR REPLACE` (DuckDB 2.0 is a parser error too) |
| `UPDATE t SET … FROM other WHERE …` | ✅ | The source is bound **before** the target, so `FROM`/`JOIN` columns come first in expressions. When several source rows match one target row the **last** source row wins — no error, matching DuckDB (probed; Postgres would error). |
| `DELETE FROM t USING other WHERE …` | ✅ | Same source-first binding. A target row matched several times is still deleted once. |
| `RETURNING …` on `INSERT`/`UPDATE`/`DELETE` | ✅ | Also on `ON CONFLICT` (returns the final row of each affected row, inserted or updated) and on `MERGE`. Projections may be `*`, plain columns, expressions and aliases. The reported image is the row **after** `BEFORE` triggers ran (DuckDB has no triggers; this is pinkdb's contract, tested). |
| `MERGE INTO t USING s ON … WHEN MATCHED [AND …] THEN UPDATE SET … / DELETE`, `WHEN NOT MATCHED [AND …] THEN INSERT [(cols)] VALUES (…)` | ✅ | Clauses are tried in order, first match wins (DuckDB's rule). `WHEN NOT MATCHED BY SOURCE` is supported too. A target row matched by more than one source row **and** affected by a matched clause is DuckDB's error, verbatim: `MERGE INTO command cannot affect the same target row more than once. A target row matched more than one source row; ensure the source rows are deduplicated or the ON condition is unique.` The statement is atomic. |
| data-modifying CTE: `WITH x AS (INSERT/UPDATE/DELETE … [RETURNING …]) SELECT … FROM x` | ✅ | Top level only (a nested one is refused with DuckDB's `WITH clause containing a data-modifying statement must be at the top level`); the body runs once, even when the CTE is referenced several times. |
| `COPY` as a CTE step | ✗ | `COPY inside WITH is not supported: run COPY as its own statement` (DuckDB accepts it; `COPY … TO` is ADMIN-gated in pinkdb, and a CTE step would sidestep that check) |
| `PRIMARY KEY` / `UNIQUE` enforcement | ✅ | **Behaviour change:** the constraints stop being metadata. `INSERT`/`UPDATE`/`MERGE`/`COPY`-free writes reject a duplicate key with DuckDB's wording (`PRIMARY KEY or UNIQUE constraint violation: duplicate key "1"` / `Duplicate key "a: 1" violates unique constraint.`), a `NULL` in a primary key with `NOT NULL constraint failed: t.a`, and a rejected statement leaves the store unchanged. A multi-column key is checked as a whole; a `UNIQUE` column accepts any number of `NULL`s. Foreign keys stay metadata (wave 41's `DESCRIBE`/join-elimination only). |

A conflict target that is not backed by a key is refused like DuckDB:
`the specified columns as conflict target are not referenced by a UNIQUE/PRIMARY KEY CONSTRAINT`.

## The rest — batch 3 (95c)

| form | status | notes (DuckDB 2.0.0 reference) |
|---|---|---|
| `LATERAL` derived subquery, `CROSS JOIN LATERAL`, comma-lateral | ≈ | A lateral source is bound after the items to its left, so it sees their columns — but pinkdb binds a derived plan without an outer scope, so a subquery that **references** an earlier `FROM` item is refused by name: `a LATERAL subquery cannot reference columns of an earlier FROM item: pinkdb binds derived plans without an outer scope (uncorrelated LATERAL only)`. Everything uncorrelated matches DuckDB, including `FROM LATERAL (…) s` in the head position (a no-op there, as in DuckDB). A lateral **table function** is refused: `a LATERAL table function is not supported: only LATERAL subqueries are`. |
| comma-separated `FROM a, b` | ✅ | A comma is a `CROSS JOIN` in the same left-deep chain, so a later `JOIN … ON` may reference an item left of the comma — DuckDB's rule (`FROM a, b JOIN c ON a.x = c.z` binds). |
| `FILTER` on a window aggregate | ✅ | `sum(a) FILTER (WHERE a > 1) OVER (ORDER BY a)` skips the non-matching rows of the frame (they contribute nothing, so the result is `NULL` until the frame has a match), like DuckDB. On a non-aggregate window function it is refused with `FILTER is not implemented for the window function "row_number"` (DuckDB: Binder Error, same shape). |
| a window built on another window (`OVER (w ORDER BY a)`, `OVER (w ROWS …)`, `WINDOW v AS (w …)`) | ✅ | The base's `PARTITION BY`/`ORDER BY` are inherited and the derived specification may add a clause the base leaves empty; a clause the base already has cannot be overridden (`Cannot override ORDER BY clause of window "w"`) and copying a base that has a frame is refused (`cannot copy window "w" because it has a frame clause`) — DuckDB's wording, reported at bind time. |
| ordered-set aggregates `percentile_cont`/`percentile_disc` (`quantile_*`), `mode()` `WITHIN GROUP (ORDER BY e)` | ✅ | `percentile_cont` interpolates and returns `DOUBLE` (a `DECIMAL` input keeps its type), `percentile_disc` returns the input type, `mode()` the most frequent value with the **smallest** winning ties. `NULL`s are skipped, an empty input is `NULL`, `FILTER` may be written before or after `WITHIN GROUP`, and `ORDER BY … DESC` is accepted and ignored (DuckDB ignores it too). The fraction must be a constant in `[0, 1]`. Not windowable: `percentile_cont() WITHIN GROUP cannot be used as a window function`. |
| `TABLESAMPLE` / `USING SAMPLE` (`bernoulli`, `system`, `reservoir`; `n ROWS`, `n PERCENT`, `n%`) with `REPEATABLE (seed)` | ≈ | Counts and method rules match DuckDB: `USING SAMPLE n ROWS` is reservoir and yields exactly `n` rows (fewer when the input is smaller), `PERCENT` is per row with `bernoulli`, per chunk with `system` and a `round(p/100·n)`-row reservoir with `reservoir`; a discrete `ROWS` count with `bernoulli`/`system` is refused with DuckDB's message. The **rows** a seeded draw picks differ, because each engine has its own generator — `REPEATABLE (seed)` is reproducible within pinkdb, not across engines. Superset: the bare `USING SAMPLE 2 ROWS REPEATABLE(7)` parses (DuckDB needs the parenthesised method form). `EXPLAIN` prints `Sample reservoir(1 ROWS) REPEATABLE(7)`. |
| row constructors `ROW(a, b)`, `(a, b)` in a comparison or `IN` | ✅ | Field by field with NULL-as-a-value semantics, exactly DuckDB's: `=`/`<>` compare each field with `IS [NOT] DISTINCT FROM`, the ordered operators are lexicographic with `NULL` sorting greater, and a row `IN` list is the OR of those field-wise equalities. A standalone row is refused (`a row constructor is only supported in a comparison or an IN list: the engine has no STRUCT type`) and so is `(a, b) IN (SELECT …)` (`a row constructor in IN (SELECT ...) is not supported: compare the columns one by one`). |
| `AT TIME ZONE`, `timezone(zone, ts)` | ≈ | pinkdb has no `TIMESTAMPTZ` and no time zone database, so a UTC-family zone (`UTC`, `GMT`, `Z`, `Zulu`, `UCT`, `Universal`, `Etc/UTC`, `Etc/GMT`, `+00:00`) is the identity on the naive `TIMESTAMP` — what DuckDB's UTC round trip returns — and any other zone is refused: `AT TIME ZONE 'EUROPE/PARIS' needs a time zone database: pinkdb has no TIMESTAMPTZ (only UTC-family zones are an identity on the naive TIMESTAMP)`. The parser folds the operator to `timezone(zone, ts)`, so `EXPLAIN` shows one form. |
| `json_set` / `json_insert` / `json_replace` / `json_remove` | ≈ | Implemented in the engine on JSON **text** (`VARCHAR`) or `VARIANT`, and the result keeps the document's kind (`VARCHAR` text or `VARIANT`). DuckDB needs `::JSON` arguments and refuses `VARIANT` ones, so pinkdb is a superset: the value argument may be any SQL value (a `VARCHAR` value is read as JSON text, so `'hi'` needs `'"hi"'`). `NULL` document, path or value give `NULL`; paths are `$.a[0]`, `$['a b']`, `$."a b"`, and `$` alone means the whole document. `json_insert` never overwrites an existing path, `json_replace` never creates a missing one, and `json_remove` takes exactly two arguments (`json_remove takes exactly 2 arguments (json, path)`, DuckDB's arity). |
| `SET VARIABLE x = …`, `getvariable('x')`, `$x` | ✅ | The value is bound once per statement and spliced in as a literal, so later statements of the session see it. **Session state is per connection** (the embedded handle is one session; each network `Connection` and each served/MCP session its own), so one client's variable is invisible to another's. Variables are never persisted or logged, and a logged statement that reads one is refused by name (`replay would bind the logged text against an empty session`). `EXPLAIN SET VARIABLE x = 41` renders `SetVariable x = 41` (DuckDB shows a plan for it too). `getvariable` of an unset name is `NULL` while `$x` is an error (`Variable nope does not exist`) — DuckDB's exact split. |
| `current_user()`, `current_roles()` | ≈ | pinkdb answers who is asking: `current_user()` is the logged-in principal (or `pinkdb` for a trusted embedded session, where there is no identity to report) and `current_roles()` its transitive roles, comma-joined in sorted order and an empty string when there are none. Both are bound once per statement as literals, so a statement can be planned without asking again, and a statement the engine WAL-logs that reads one is refused by name (`replay would bind the logged text against an empty session`). An argument is an error (`current_user() takes no arguments`). DuckDB has `current_user` as an alias of `user`, the operating-system user name, and no role list. |
| `FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE` | ✗ | `SELECT ... FOR UPDATE/SHARE is not supported: pinkdb has no row locks (its MVCC already serializes writers)` — DuckDB 2.0 is a syntax error at `FOR`; pinkdb names the reason instead. |
| a variable-length `LIST` (`T[]`, `unnest`, slicing a list) | ✗ | `T[] is a variable-length LIST; pinkdb has fixed-size ARRAY only (wave 107a2)` — the fixed-size `ARRAY` (`DOUBLE[3]`) landed in wave 107a, and a general `LIST` is the queued follow-up. |
| slicing a `VARIANT` (`v[1:2]`) | ✗ | `slicing a VARIANT is not supported: ARRAY_SLICE needs a LIST and pinkdb has no LIST type (use variant_extract with a path)` — DuckDB refuses a VARIANT slice too (`ARRAY_SLICE can only operate on LISTs and VARCHARs`). |
| subscripting another type (`a[1]` on `INT`) | ✗ | `subscripting a INTEGER is not supported: only ARRAY, VARCHAR and VARIANT have subscripts` |

## slt-2 — the corpus-driven batch

DuckDB's own sqllogictest corpus (`bench/sqllogictest/`) found these: most
failing files stop at their first unsupported statement, and these were the
cheapest wide ones. Every behaviour was checked live against
`~/.local/bin/duckdb -no-agent` (v2.0.0-alpha46057), and the same probes run
in `bench/duckdiff.py`.

Measured on the corpus: **444 files pass** (from 344), **100 files newly
pass, 0 regressions**; 83.6% of non-skipped records pass. Of the newly passing
files, 60 were blocked by a `PRAGMA`, 38 by a `SET`/`RESET`, 26 by the null
order, 3 by `ATTACH ':memory:'`, 4 by `GROUP BY ALL`/`ORDER BY ALL`, 3 by
`CHAR(n)` and 2 by `count()` (a file can count for several).
`docs/sqllogictest-baseline.md` has the full run.

### PRAGMA

| form | behaviour |
|---|---|
| `PRAGMA show_tables` | the current database's tables, as `SHOW TABLES` |
| `PRAGMA table_info('t')`, `PRAGMA table_info(t)`, `SELECT * FROM pragma_table_info('t')` | DuckDB's rows: `cid` (0-based), `name`, `type`, `notnull`, `dflt_value`, `pk` |
| `PRAGMA database_list` | `seq`, `name`, `file` (`NULL` for an in-memory database) |
| `PRAGMA version` | `library_version`, `source_id`, `codename` — pinkdb's version, an empty source id, `pinkdb` |
| `PRAGMA platform` | one row, `<os>_<arch>` |
| `PRAGMA <setting> = <value>` | the same as `SET <setting> = <value>`, including `PRAGMA threads=4` and a `MAP {…}` value |
| `PRAGMA <statement pragma>` | DuckDB's `Success` (BOOLEAN) result with no rows, for the no-op list below |
| anything else | DuckDB's `Catalog Error: Pragma Function with name <x> does not exist!` |

Statement pragmas accepted as no-ops (the verification and tuning knobs the
corpus switches on and off): `asof_loop_join_threshold`,
`checkpoint_on_shutdown`, `debug_asof_iejoin`, `debug_checkpoint_abort`,
`debug_force_external`, `debug_force_no_cross_product`,
`debug_print_bind_context`, `debug_print_optimized_plan`,
`debug_print_plans`, `debug_set_forced_table_scan_info`,
`debug_skip_checkpoint_on_commit`, `debug_verify_serializer`,
`debug_verify_vector`, `debug_window_mode`,
`disable_checkpoint_on_shutdown`, `disable_logging`, `disable_object_cache`,
`disable_optimizer`, `disable_print_progress_bar`, `disable_profiling`,
`disable_progress_bar`, `disable_verification`,
`disable_verify_parallelism`, `enable_checkpoint_on_shutdown`,
`enable_logging`, `enable_object_cache`, `enable_optimizer`,
`enable_print_progress_bar`, `enable_profiling`, `enable_progress_bar`,
`enable_verification`, `force_checkpoint`, `force_compression`,
`verify_external`, `verify_parallelism`, `verify_serializer`.

A bare `PRAGMA <setting>` (no `= value`) is the Catalog Error above, as in
DuckDB.

### Settings

`SET`/`SET SESSION`/`SET GLOBAL`/`SET LOCAL`, `RESET`, `RESET ALL` and
`current_setting('…')`.

Implemented: `threads` (the machine's core count by default; the value is
reported and unused — execution uses the process's rayon pool, size it with
`RAYON_NUM_THREADS`), `default_null_order`,
`default_order`, `memory_limit`/`max_memory` (enforced process-wide, wave
136a: the default is 80% of the memory the process may use — physical RAM, or
a tighter cgroup limit; `'-1'` means no limit, `RESET` restores the default,
and an operator that would pass the limit fails with DuckDB's `Out of Memory
Error: failed to allocate data of size …`), `decoded_cache_limit` (a
process-wide cap on clean decoded chunks; `'0'` keeps no cache, `RESET`
returns to whatever room `memory_limit` leaves free) and `wal_sync_method`
(`full`, `fsync` or `fdatasync`; per process, on durable stores).

Accepted and stored with no engine effect: every other name
`duckdb_settings()` returns (the display and tuning knobs, `timezone`,
`calendar`, `wal_autocheckpoint`, `tracked_metrics`, `disabled_optimizers`,
`profiling_*` and the rest), plus DuckDB's deprecated spellings
(`profile_output`, `profiling_mode`) and the knobs it registers only in debug
and extension builds (`debug_*`, `force_variant_shredding`,
`enable_caching_operators`, … — the corpus files that set them are written for
those builds). Six more names are pinkdb's own switches, not DuckDB's:
`decoded_cache_limit`, `hnsw_ef_search`, `hnsw_exact`, `join_filter_pushdown`,
`pipeline_execution` and `wal_sync_method`. A name outside the
list keeps DuckDB's `Catalog Error: unrecognized configuration parameter
"…"`, and so does
`current_setting` of one. Settings are separate from `SET VARIABLE` variables
(`$x`, `getvariable`), which are unchanged.

Every scope spelling is accepted: `SET LOCAL`/`SESSION` of a global-only
setting (`threads`, `memory_limit`, `max_memory`) is an error in DuckDB
(`cannot be set locally`), and a session value there is accepted here.
`threads` is session-wide, but `memory_limit`/`max_memory` still apply
process-wide whatever the scope spelling: any `SET` of it moves the pool.

`default_null_order` accepts `NULLS_FIRST`, `NULLS_LAST`,
`NULLS_FIRST_ON_ASC_LAST_ON_DESC`, `NULLS_LAST_ON_ASC_FIRST_ON_DESC` and
DuckDB's `SQLite`/`MySQL`/`Postgres` spellings, and reports the normalised
form; `default_order` takes `ASC`/`ASCENDING`/`DESC`/`DESCENDING`. Both apply
to every `ORDER BY` (an item's own `ASC`/`DESC` and `NULLS FIRST`/`NULLS LAST`
win), to a window `ORDER BY`, and to ordered aggregates where pinkdb has
them.

### Dialect items

| item | behaviour |
|---|---|
| `count()` | `count(*)`, in `OVER` too |
| `GROUP BY ALL` | one grouping set of every non-aggregate select item, in select order; mixes with plain keys and grouping sets like DuckDB |
| `ORDER BY ALL` | every visible column, in order; `DESC`/`NULLS FIRST` on the item apply to all |
| `CHAR(n)`, `CHARACTER(n)`, `BPCHAR`, `VARCHAR(n)` | `VARCHAR` — the length is ignored, as DuckDB ignores it |
| type names as column names (`date`, `timestamp`, `range`, `text`, …) | accepted: `date`, `timestamp` and `range` became nameable without making the grammar rules that need them ambiguous |
| `read_csv_auto(…)` | an alias of `read_csv` (with the `parquetlake`/`httpfs` features) |

### ATTACH ':memory:' (a plain database)

`ATTACH [DATABASE] ':memory:' AS name`, `DETACH [IF EXISTS] name`, `USE name`,
`SHOW DATABASES`.

- `:memory:` is an in-memory database of the store, gone at `DETACH` (the rows
  are the store's own, checkpointed with it, like DuckDB's memory database is
  not in any file).
- A **file path is refused**: `ATTACH 'x.db' AS x` (with or without
  `DATABASE`, `READ_ONLY` or `TYPE`) answers
  `ATTACH 'x.db' is not a lake: pinkdb attaches ":memory:" or a lake (a lake:
  or ducklake: catalog, or TYPE LAKE); attaching a plain database file is not
  supported`. A second durable database behind one name has no owner: two
  processes could attach the same file and one `DETACH` would silently
  overwrite the other, and a durable session store would checkpoint the
  attached rows into its own file as well. Use a lake for a separate durable
  database, `EXPORT`/`COPY` for a file.
- A path that *is* in DuckDB's format is still refused by its header, so the
  reason is the format and not the missing feature: `ATTACH: "…" is a DuckDB
  database file; pinkdb cannot read DuckDB files (EXPORT the data from DuckDB
  as CSV or Parquet and read that instead)`.
- `ATTACH ':memory:' (READ_ONLY)` is DuckDB's error: `Cannot launch in-memory
  database in read-only mode!`.
- Divergences: `USE`/`DETACH` of the current database is not special-cased.
  Two corpus files that used a plain database file
  (`attach/attach_issue16122.test`,
  `parallelism/interquery/concurrent_checkpoint_insert.test`) are expected
  failures for that reason.

## Divergences worth knowing

These are accepted by both engines with different answers, or accepted by one
only. They are deliberate (each has a test or a comment at the site) and are
not planned to change.

| case | pinkdb | DuckDB 2.0.0 |
|---|---|---|
| `'{"a":1}'::VARIANT` | the **object** (VARCHAR→VARIANT parses JSON, which gives the type a literal form the parser/WAL/prepared statements carry) | the **string**; `json(...)::VARIANT` is needed for an object |
| `1.e2`, `5.` (a point with no digits after it) | parse error, `a number with an exponent needs digits after the dot (write 1.0e2)` | `5.` is `DECIMAL(1,0)`, `1.e2` is DOUBLE |
| an unaliased decimal literal written `1_000.5` or `00.5` | the column is named from the folded value (`1000.5`, `0.5`); the written *scale* survives, so `1.50` stays `1.50` | the source text (`1_000.5`, `00.5`) |
| `SET LOCAL`/`SET SESSION` of a global-only setting (`threads`, `memory_limit`) | accepted: `threads` is session-wide, `memory_limit`/`max_memory` still apply process-wide | `Catalog Error: option "threads" cannot be set locally` |
| `ATTACH 'x.db' AS x` | Catalog Error naming `:memory:` and lakes as the supported forms | attaches `x.db` as its own durable database |
| `[1.5, 2.5]` (an ARRAY of decimal literals) | `DOUBLE[2]`: the fixed-size `ARRAY` has no DECIMAL element kind, so a decimal element types as DOUBLE (wave 126a) | `DECIMAL(2,1)[2]`; a column of that ARRAY must be `DOUBLE[]` in pinkdb |
| `v.js.a` (database-qualified three-part column vs a VARIANT chain) | `database "v" not found` — wave 94 keeps `db.table.column` authoritative; write `js.a['b']` for nested VARIANT access | a VARIANT path |
| `SIMILAR TO 'a{1,2,3}'` | literal text (matches nothing) | invalid-regular-expression error |
| `SIMILAR TO … ESCAPE '\'` | parse error (unsupported) | `Unrecognized Regex option` error |
| `LIMIT 1 FETCH FIRST 2 ROWS ONLY` | `LIMIT cannot be combined with FETCH FIRST` | syntax error |
| `CUBE` over > 12 expressions | refused | allowed (explodes) |
| recursive CTE without a terminating predicate | error after 100 000 iterations | loops until memory runs out |
| `UPDATE … FROM` with several matching source rows | last source row wins (same as DuckDB) | last source row wins |
| `MERGE … RETURNING` over several affected rows | every affected row, in clause order (matched updates/deletes, then inserts) | every affected row; the relative order is not defined by DuckDB |
| duplicate keys in one `INSERT` | the first row wins, the rest are dropped for `ON CONFLICT`/`OR REPLACE` (DuckDB's behaviour) | first row wins |
| a table written by a pre-95b build that already holds duplicate keys | refuses to open on WAL replay with the duplicate-key error (no silent fix-up; pre-1.0, no migration) | n/a |
| `LATERAL (SELECT … WHERE t.a …)` | the named "cannot reference columns of an earlier FROM item" error (derived plans have no outer scope) | correlates the subquery with the item to its left |
| `REPEATABLE (seed)` | reproducible **within pinkdb** (one generator per sample) | reproducible within DuckDB; the two engines pick different rows |
| `AT TIME ZONE 'Europe/Paris'` | refused: no time zone database | converts to `TIMESTAMPTZ` |
| `json_set(doc, path, value)` with a `VARCHAR` document | in the engine, on JSON text or `VARIANT`; any SQL value may be the value argument | needs `::JSON` arguments and refuses `VARIANT` ones |
| `(a, b) IN (SELECT …)` | refused by name (compare the columns one by one) | three-valued row `= ANY` |
| a correlated subquery shape the unnesting does not cover (see the forms table) | correct, but run once per distinct outer key; `EXPLAIN` keeps the body inside the predicate | always a join after its delimiter |
| `USING SAMPLE 2 ROWS REPEATABLE(7)` | accepted | parser error (needs `reservoir(2 ROWS)`) |

## What is covered by tests

`crates/pinkdb/tests/sql_conformance.rs`:

* `batch_one_query_forms_match_duckdb` — 25 cases over one shared corpus
  (`t(a, b, g)` with five rows), each pinned to DuckDB's answer and
  re-measured on DuckDB when `/tmp/duckdb-2.0.0/duckdb` exists;
* `explain_shows_the_new_operators` — `EXPLAIN` shows `Window`, `Distinct`,
  `Values`, `Cte` + `Recursive step (UNION ALL)` for the batch-1 forms and
  `Sample reservoir(2 ROWS) REPEATABLE(7)`, the ordered-set aggregate text and
  `SetVariable x = 41` for batch 3;
* `batch_two_dml_forms_match_duckdb` — 25 DML cases (upserts, `RETURNING`,
  `UPDATE … FROM`, `DELETE … USING`, `MERGE`, data-modifying CTEs), each pinned
  to DuckDB's answer and re-measured on DuckDB when it is present (the same
  cases run in `bench/duckdiff.py` as the `dml-*` battery);
* `dml_errors_are_precise` — the wave-95b refusals above, each also checked to
  be an error on DuckDB;
* `primary_and_unique_keys_are_enforced` — duplicate key, `NULL` in a key,
  atomic rollback of a rejected statement, and the conflict forms that accept
  what a plain `INSERT` rejects;
* `returning_reports_the_post_trigger_row` — a `BEFORE` trigger's rewrite of
  `NEW` is what `RETURNING` reports (pinkdb only: DuckDB has no triggers);
* `explain_names_the_dml_cte_forms` — `EXPLAIN` renders each DML form (the
  conflict action, the `MERGE` clauses, the source/using join) and refuses a
  DDL statement with a sentence;
* `batch_three_query_forms_match_duckdb` — 26 batch-3 cases (`LATERAL`, comma `FROM`, ordered sets, sampling counts, row comparisons, window `FILTER`, windows on windows), each pinned to DuckDB's answer and re-measured on DuckDB when it is present (the same cases run in `bench/duckdiff.py` as the wave-95c batteries);
* `batch_three_pinkdb_only_forms` — the forms whose spelling differs in DuckDB (JSON mutators on text and on `VARIANT`, `AT TIME ZONE 'UTC'`);
* `seeded_samples_are_reproducible` — the same `REPEATABLE` seed gives the same rows, a different seed gives different ones;
* `session_variables_are_visible_to_later_statements` — `SET VARIABLE` + `getvariable` + `$name`, a variable inside `WHERE`, and the `getvariable`/`$name` split for an unset name;
* `session_variables_are_per_connection` (db) and `set_variable_is_per_connection` (server) — two clients binding the same name see their own values and cannot read each other's;
* `served_sessions_keep_their_variables_apart` — the same isolation for served/MCP sessions;
* `crates/pinkdb/tests/session_variables.rs` — variables are never logged or persisted (a crash replays the writes and finds an empty session), and a logged statement that reads one is refused by name;
* `crates/pinkdb/tests/current_user.rs` — the implicit principal, login and transitive role display names, logout, the
  functions inside an expression, the argument refusal, and the WAL refusal for a logged statement that reads one;
* `refusals_name_the_missing_feature` — every remaining refusal above (`FOR UPDATE`, a non-UTC `AT TIME ZONE`, a standalone row, a list literal, a correlated `LATERAL`, a lateral table function, `(a,b) IN (SELECT …)`, `json_remove` arity, a non-constant percentile, a windowed percentile, `FILTER` on `row_number`, `TABLESAMPLE BERNOULLI(10)`, a window copied from a framed base), plus the two window-merge errors;
* `crates/pinkdb/tests/dml_replay.rs` — a store written with `ON CONFLICT`,
  `MERGE`, `UPDATE … FROM`, `DELETE … USING` and a data-modifying CTE is
  dropped without a checkpoint and reopened; the WAL replays the statements
  (found verbatim in the record) and reproduces the same rows, before and
  after a checkpoint;
* `crates/pinkdb/tests/rbac.rs` — `the_dml_forms_check_every_table_they_touch`:
  `MERGE` needs the privilege of each action its clauses spell out, the
  `UPDATE … FROM`/`DELETE … USING` source needs `SELECT`, and a data-modifying
  CTE's target needs the write privilege;
* `crates/pinkdb/src/serve.rs` — the read-only allowlist refuses every new
  statement kind and a `WITH` whose body writes.
* `crates/pinkdb/src/sql/peg_parser.rs` — `PRAGMA`/`SET`/`RESET` forms,
  `GROUP BY ALL`/`ORDER BY ALL`, `CHAR(n)`/`CHARACTER(n)`/`BPCHAR` and
  keyword-named columns parse to the expected statements;
* `crates/pinkdb/src/db.rs` — `duckdb_settings_round_trip`,
  `pragma_rows_are_duckdb_shaped`, `count_star_without_an_argument`,
  `group_by_all_and_order_by_all`, `null_and_direction_defaults_reach_every_order`,
  and the ATTACH round trips (memory, file write-back and reopen, `READ_ONLY`
  refusal, a DuckDB-format file refused);
* `bench/duckdiff.py` — the settings, pragma and ATTACH batteries re-measure
  each of the above against DuckDB.

## Performance

No regression for existing DML. The wave-95b A/B ran a locally generated
1 000 000-row keyed table (`bench/wave95b_perf/` holds the generator, the three
workloads and `run.sh`), alternating the pre-change binary (a `git worktree` at
HEAD `6531691` with its own `CARGO_TARGET_DIR`) and the new one, five
interleaved rounds, each statement timed in its own process with `.timer on`,
on a private copy of the base store:

| workload | HEAD (median of 5) | this change (median of 5) | delta |
|---|---|---|---|
| `INSERT INTO t2 SELECT * FROM t` (1 M rows, PK on `t2.id`) | 0.095 s | 0.381 s | +0.286 s — the uniqueness check (~3.5 M keys/s) |
| `UPDATE t SET v = v + 1` (1 M rows, key not written) | 0.025 s | 0.025 s | none |
| `DELETE FROM t WHERE id % 2 = 0` (500 k rows) | 0.019 s | 0.019 s | none |

The insert delta is the price of enforcing the key: every staged row is hashed
into one map per table (`crates/pinkdb/src/keys.rs`), built lazily on the first
write after a table is opened or changed by another path and then kept in step
with appends, updates and deletes. The first write to a large table therefore
pays one scan to build the map (about 0.2 s for 1 M rows); later single-row
writes in the same process are constant time — a one-row `INSERT` or
`INSERT … ON CONFLICT` into a 1 M-row keyed table executes in under 0.1 ms
once warm, against 0.9 s when the map was rebuilt per statement. An `UPDATE`
that does not write a key column skips the check entirely, which is why the
bulk update is unchanged. The 95a query A/B (Q1/Q6/group-sort) also still holds: its operators are
untouched by wave 95b.

Wave 95c added no cost to existing queries either. The A/B ran the 1 M-row
`lineitem` store from `bench/wave95_perf/` against a pre-change binary (a
`git worktree` at HEAD `a7044e6` with its own `CARGO_TARGET_DIR`), five
interleaved rounds each:

| workload | HEAD (median) | this change (median) |
|---|---|---|
| TPC-H Q1 | 0.012 s | 0.012 s |
| TPC-H Q6 | 0.007 s | 0.007 s |
| group by + sort + limit (`bench/wave95_perf/sort_group.sql`) | 0.1285 s | 0.1295 s |

The group-sort row is the median of 12 interleaved rounds whose two
distributions overlap (HEAD 0.124–0.139 s, this change 0.125–0.137 s), so it is
noise rather than a step. It was a real 5 % step at first: `Acc` had gained an
eighth variant, which pulled drop glue into the serial partial-merge loop. The
ordered-set state now shares the `CountDistinct` variant's slot (`Acc::Collect`),
so `Acc` is back to seven variants and the step is gone.

`MERGE` and `UPDATE … FROM` no longer nest-loop over the source: the equality
keys of the `ON`/`WHERE` predicate drive a hash table (`DmlIndex`), with the
non-equality conjuncts still evaluated per candidate. `bench/wave95c_perf/`
holds the generator, the two statements and `run.sh`; each run copies the store
so the statement sees the same input.

| workload | HEAD | this change |
|---|---|---|
| `MERGE` 1 M target × 100 k source | not runnable (10¹¹ pairs; ≈9.4 h extrapolated) | 4.9 s |
| `UPDATE … FROM` 1 M × 100 k | not runnable (≈8.8 h extrapolated) | 0.28 s |
| `MERGE` 100 k × 10 k | 340.2 s | 0.47 s |
| `UPDATE … FROM` 100 k × 10 k | 318.2 s | 0.027 s |
| `MERGE` 10 k × 1 k (median of 5) | 3.18 s | 0.048 s |
| `UPDATE … FROM` 10 k × 1 k (median of 5) | 3.09 s | 0.006 s |

The two large-target rows are extrapolations of the measured nested-loop cost
per candidate pair, not measurements — the old code cannot finish them here.
