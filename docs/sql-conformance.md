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
| `UPDATE … FROM` with several matching source rows | last source row wins (same as DuckDB) | last source row wins |
| `MERGE … RETURNING` over several affected rows | every affected row, in clause order (matched updates/deletes, then inserts) | every affected row; the relative order is not defined by DuckDB |
| duplicate keys in one `INSERT` | the first row wins, the rest are dropped for `ON CONFLICT`/`OR REPLACE` (DuckDB's behaviour) | first row wins |
| a table written by a pre-95b build that already holds duplicate keys | refuses to open on WAL replay with the duplicate-key error (no silent fix-up; pre-1.0, no migration) | n/a |

## What is covered by tests

`crates/pinkdb/tests/sql_conformance.rs`:

* `batch_one_query_forms_match_duckdb` — 25 cases over one shared corpus
  (`t(a, b, g)` with five rows), each pinned to DuckDB's answer and
  re-measured on DuckDB when `/tmp/duckdb-2.0.0/duckdb` exists;
* `explain_shows_the_new_operators` — `EXPLAIN` shows `Window`, `Distinct`,
  `Values` and `Cte` + `Recursive step (UNION ALL)` for the new forms;
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
* `later_waves_are_named_in_the_error` — every 95c refusal above;
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
bulk update is unchanged. The 95a query A/B (Q1/Q6/group-sort) also still holds: its
operators are untouched by wave 95b.
