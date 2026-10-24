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
| **95b** | the DML forms: `INSERT … ON CONFLICT` (and `INSERT OR REPLACE` / `REPLACE INTO`), `UPDATE … FROM`, `DELETE … USING`, `RETURNING`, `MERGE`, data-modifying CTEs, multi-row-match semantics | queued — every form is refused with a `wave 95b` error |
| **95c** | the rest: `LATERAL`, ordered-set aggregates (`WITHIN GROUP`), `TABLESAMPLE`, `FOR UPDATE`/`SHARE`, `AT TIME ZONE`, row constructors, list literals/`LIST`, `FILTER` on a window aggregate, windows built on another window, VARIANT slicing, the JSON mutation functions, `SET VARIABLE`/`$name` | queued — every form is refused with a `wave 95c` error |

Legend: **✅ supported** (same answer as DuckDB 2.0.0 on the shared corpus) ·
**≈ divergent** (accepted, different from DuckDB, in the stated way) ·
**✗ refused** (the exact message follows).

## Query forms — batch 1 (95a)

| form | status | notes (DuckDB 2.0.0 reference) |
|---|---|---|
| `QUALIFY expr` | ✅ | Runs after `WINDOW` and before `ORDER BY`/`LIMIT`, like DuckDB (`SELECT … WINDOW w AS (…) QUALIFY s > 10` is accepted; `QUALIFY` before `WINDOW` is rejected by both). The predicate may name select aliases, aggregates and window functions, and becomes a filter over the window output. |
| `TRY_CAST(expr AS type)` | ✅ | `NULL` when the cast fails, for every target type including `VARIANT`; the same code path as `CAST`, so a *missing* conversion (e.g. `DATE → BOOLEAN`) is still an error rather than `NULL`. |
| VARIANT subscripts `v['a']`, `v[0]`, `v.a` | ✅ | 1-based array indices like DuckDB (`[0]` errors with DuckDB's wording, out of range and missing keys are SQL `NULL`). Chains: `js['a']['b']`, `js.a[2]`, `js.a['b']`. `v.a` is `VARIANT`, never a plain scalar. |
| string subscripts/slices `'hello'[2]`, `'hello'[2:4]`, `'hello'[-1]` | ✅ | 1-based, character-wise (not bytes), negative counts from the end, out-of-range clamps, `from > to` is `''`. |
| `IS [NOT] DISTINCT FROM` | ✅ | NULL-safe equality (`1 IS DISTINCT FROM NULL` is true, `NULL IS NOT DISTINCT FROM NULL` is true). Desugared to `COALESCE(a = b, FALSE) OR (a IS NULL AND b IS NULL)`. |
| `VALUES (…), (…)` as a query | ✅ | Column names `col0`, `col1`, … like DuckDB; a trailing `ORDER BY`/`LIMIT` works. |
| `(VALUES …) t(x, y)` in `FROM` | ✅ | Column aliases rename positionally, on the first `FROM` source and on join sources alike (`… CROSS JOIN (VALUES (9)) u(z)`); arity mismatch is a bind error. Cells must be constants (no column references or subqueries) and each column takes the common type of its cells. |
| `TABLE t` | ✅ | Parsed as `SELECT * FROM t`; trailing `ORDER BY`/`LIMIT` accepted like DuckDB. |
| `FETCH FIRST n ROWS ONLY` | ✅ | Also `FETCH NEXT`/`ROW`; `OFFSET n ROW\|ROWS` accepted too. `LIMIT` and `FETCH FIRST` together are refused (`LIMIT cannot be combined with FETCH FIRST`) — DuckDB is a syntax error there. |
| `LIKE … ESCAPE 'x'` | ≈ | Supported, plus the `like_escape(s, p, esc)` scalar. The escape string must be empty or one character (DuckDB's message, verbatim). Divergence: pinkdb has no default escape character, so `'a%b' LIKE 'a\%b'` without `ESCAPE` matches nothing extra — DuckDB behaves the same. |
| `SIMILAR TO pattern` | ≈ | Implemented as a **fully anchored regular expression**, which is what DuckDB 2.0 actually does (`%` and `_` are literals, `.*` is the wildcard). Supports literals, `.`, `[...]`/`[^...]` ranges, `*`, `+`, `?`, `{m}`, `{m,}`, `{m,n}`, `(…)`, `\|`, `\x` escapes and `^`/`$`; patterns over 1024 characters or repetitions over 1000 fail with `SIMILAR TO pattern too complex`. Divergence: `SIMILAR TO … ESCAPE` is refused (DuckDB refuses it too) and a malformed repetition such as `a{1,2,3}` is read literally instead of raising DuckDB's regex error. |
| `DISTINCT ON (…)` | ✅ | Keeps the first row per key in `ORDER BY` order, like DuckDB; the key need not be selected and `ORDER BY` need not contain it. Combined with plain `DISTINCT` is refused (`DISTINCT and DISTINCT ON cannot be combined`); with plain `DISTINCT` semantics also reachable via `GROUP BY`. |
| aggregate `FILTER (WHERE …)` | ✅ | `sum(a) FILTER (WHERE a > 1)`, `count(*) FILTER (…)`, `count(DISTINCT a) FILTER (…)`; the filtered rows contribute nothing (not a zero). Never takes a fused aggregate fast path, so it cannot silently read unfiltered columns. |
| named `WINDOW w AS (…)` | ✅ | Definitions may reference select aliases and each other only left-to-right; `OVER w` resolves through them. A window that builds on another window (`WINDOW v AS (w ORDER BY a)`, `OVER (w ROWS …)`) is **✗** `a window that builds on another window is not supported (wave 95c)`. |
| `GROUP BY GROUPING SETS (…)` | ✅ | Sets, `ROLLUP(…)` (longest prefix first) and `CUBE(…)` (DuckDB's order); multiple group-set items cross-product with the plain keys. Implemented as a union of aggregates, so `HAVING`/`ORDER BY`/`LIMIT` apply once. `CUBE` over more than 12 expressions is refused. |
| `GROUPING(a, b)`, `GROUPING()` | ✅ | Big-endian mask like DuckDB: `GROUPING(a, b) = 2·g(a) + g(b)`; the zero-argument form is `0` for detail sets and `1` for the grand total. Naming a non-grouped expression is an error. |
| `WITH RECURSIVE r(…) AS (anchor UNION [ALL] step)` | ✅ | Semi-naive iteration: the step sees only the rows the previous round added, `UNION` (not `ALL`) also drops rows already produced, and iteration stops when the step adds nothing. Bounds: **100 000 iterations** (`recursive CTE "r" still produced rows after 100000 iterations`) and **10 000 000 rows** (`recursive CTE "r" passed 10000000 rows`) — a runaway recursion is an error, never a hang. The step's columns are cast to the anchor's types. |
| `UNION ALL` / `INTERSECT ALL` / `EXCEPT ALL` | ✅ | Multiset semantics, verified on the shared corpus. Plain `UNION`/`INTERSECT`/`EXCEPT` unchanged. |
| `CORRESPONDING` | ✗ | `parse error` — DuckDB 2.0 also rejects `UNION CORRESPONDING`. Documented, not planned. |

## DML forms — batch 2 (95b, queued)

Every one of these is refused with the wave name, so a script fails on the
feature instead of on a misleading syntax error.

| form | status | error |
|---|---|---|
| `INSERT … ON CONFLICT …` | ✗ | `INSERT ... ON CONFLICT is not supported (wave 95b)` |
| `INSERT … ON DUPLICATE KEY UPDATE …` | ✗ | `INSERT ... ON DUPLICATE KEY UPDATE is not supported (wave 95b)` |
| `INSERT OR REPLACE …` | ✗ | `INSERT OR REPLACE is not supported (wave 95b): use INSERT ... ON CONFLICT when it lands` |
| `REPLACE INTO …` | ✗ | `REPLACE INTO is not supported (wave 95b): use INSERT ... ON CONFLICT when it lands` |
| `UPDATE … FROM …` | ✗ | `UPDATE ... FROM is not supported (wave 95b)` |
| `DELETE … USING …` | ✗ | `DELETE ... USING is not supported (wave 95b)` |
| `RETURNING …` | ✗ | `RETURNING is not supported (wave 95b)` |
| `MERGE INTO …` | ✗ | `MERGE is not supported (wave 95b)` |
| data-modifying CTEs (`WITH x AS (INSERT …)`) | ✗ | `data-modifying CTEs (INSERT/UPDATE/DELETE inside WITH) are not supported (wave 95b)` |
| `COPY` as a CTE step | ✗ | same data-modifying-CTE error |

## The rest — batch 3 (95c, queued)

| form | status | error |
|---|---|---|
| `LATERAL` (in `FROM` and after `JOIN`) | ✗ | `LATERAL is not supported (wave 95c)` |
| ordered-set aggregates `WITHIN GROUP (ORDER BY …)` | ✗ | `ordered-set aggregates (WITHIN GROUP) are not supported (wave 95c)` |
| `TABLESAMPLE` | ✗ | `TABLESAMPLE is not supported (wave 95c)` |
| `FOR UPDATE` / `FOR SHARE` | ✗ | `SELECT ... FOR UPDATE/SHARE is not supported (wave 95c): the engine has no row locks` |
| `AT TIME ZONE` | ✗ | `AT TIME ZONE is not supported (wave 95c): cast to TIMESTAMP and use date_trunc instead` |
| row constructors: `ROW(1, 2)`, `(1, 2)` | ✗ | `row constructors are not supported (wave 95c): use VALUES or separate columns` |
| list literals and the `LIST` type: `[1, 2]`, `t[1]` on a list | ✗ | `list literals are not supported (wave 95c): the engine has no LIST type` |
| `FILTER` on a window aggregate | ✗ | `FILTER on a window function is not supported (wave 95c)` |
| a window built on another window | ✗ | `a window that builds on another window is not supported (wave 95c)` |
| slicing a VARIANT (`v[1:2]`) | ✗ | `slicing a VARIANT is not supported (wave 95c): only VARCHAR slices` |
| subscripting another type (`a[1]` on `INT`) | ✗ | `subscripting a INTEGER is not supported (wave 95c): only VARCHAR and VARIANT have subscripts` |
| `json_set` / `json_insert` / `json_replace` / `json_remove` | ✗ | `unknown function "json_set"` (the JSON extension has the read-only functions; the mutators are wave 95c) |
| `SET VARIABLE x = 1` | ✗ | `SET VARIABLE is not supported (wave 95c): use a parameter or a table` |
| `$x` shorthand | ✗ | `the \`$name\` variable shorthand is not supported (wave 95c): use SET VARIABLE/getvariable` |

## Divergences worth knowing

These are accepted by both engines with different answers, or accepted by one
only. They are deliberate (each has a test or a comment at the site) and are
not planned to change.

| case | pinkdb | DuckDB 2.0.0 |
|---|---|---|
| `'{"a":1}'::VARIANT` | the **object** (VARCHAR→VARIANT parses JSON, which gives the type a literal form the parser/WAL/prepared statements carry) | the **string**; `json(...)::VARIANT` is needed for an object |
| `1.e2` | parse error, `a number with an exponent needs digits after the dot (write 1.0e2)` | a float (`100`) |
| `v.js.a` (database-qualified three-part column vs a VARIANT chain) | `database "v" not found` — wave 94 keeps `db.table.column` authoritative; write `js.a['b']` for nested VARIANT access | a VARIANT path |
| `SIMILAR TO 'a{1,2,3}'` | literal text (matches nothing) | invalid-regular-expression error |
| `SIMILAR TO … ESCAPE '\'` | parse error (unsupported) | `Unrecognized Regex option` error |
| `LIMIT 1 FETCH FIRST 2 ROWS ONLY` | `LIMIT cannot be combined with FETCH FIRST` | syntax error |
| `CUBE` over > 12 expressions | refused | allowed (explodes) |
| recursive CTE without a terminating predicate | error after 100 000 iterations | loops until memory runs out |

## What is covered by tests

`crates/pinkdb/tests/sql_conformance.rs`:

* `batch_one_query_forms_match_duckdb` — 25 cases over one shared corpus
  (`t(a, b, g)` with five rows), each pinned to DuckDB's answer and
  re-measured on DuckDB when `/tmp/duckdb-2.0.0/duckdb` exists;
* `explain_shows_the_new_operators` — `EXPLAIN` shows `Window`, `Distinct`,
  `Values` and `Cte` + `Recursive step (UNION ALL)` for the new forms;
* `later_waves_are_named_in_the_error` — every 95b/95c refusal above;
* `unsupported_variants_have_precise_errors` — the fine-grained refusals
  (multi-character escape, `FILTER` on a window, VARIANT slicing, a subscript
  on a non-VARIANT/VARCHAR column, `LIMIT`+`FETCH`, runaway pattern);
* `variant_subscripts_follow_duckdb_indexing`,
  `grouping_masks_and_rollup_shapes`,
  `recursive_cte_terminates_and_reports_a_runaway` — the pinkdb-only shapes
  (which DuckDB spells with `json(...)` or answers identically but with a
  different plan).

## Performance

No regression for existing queries. Measured on a locally generated
1 000 000-row `lineitem`-shaped store (`bench/wave95_perf/` holds the
generator and the three queries), alternating the pre-change binary (a
`git worktree` at HEAD `1d199fb` with its own `CARGO_TARGET_DIR`) and the new
one, medians of six interleaved runs, each query timed in its own process with
`.timer on`:

| build | Q1 (group by 2 keys, 8 aggregates) | Q6 (filter + one aggregate) | group/sort (`l_partkey`, top 20) |
|---|---|---|---|
| release, HEAD | 0.012 s | 0.007 s | 0.131 s |
| release, this change | 0.012 s | 0.007 s | 0.132 s |
| debug, HEAD | 0.140 s | 0.063 s | 0.785 s |
| debug, this change | 0.143 s | 0.063 s | 0.781 s |

The debug Q1 delta (+2 %, spread 0.136–0.146 s) is code-layout noise: the
release build, where execution dominates, is identical.
