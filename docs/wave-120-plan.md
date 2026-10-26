# Wave 120 — correlated subqueries (decorrelation with DuckDB parity)

Status: done. This file is the design record, written before the code and kept
current as the wave landed; everything below describes the shipped shape unless
it is under "Fallback shapes" or "Follow-ups".

## Problem

`crates/pinkdb/src/sql/binder.rs` bound every subquery in a fresh scope with no
parent, so a subquery that named an outer column failed with
`no column "o_orderkey"`. TPC-H Q2, Q4, Q17, Q20, Q21 and Q22 were unbindable
(`bench/suite/run.py` listed them in `UNSUPPORTED_BY_DESIGN`).

## What is supported now

A subquery may reference columns of any enclosing query block, at any depth, in
`WHERE`, `SELECT`, `HAVING`, `ON` and `CASE`. Forms:

- `EXISTS` / `NOT EXISTS`;
- `IN` / `NOT IN`, with SQL's three-valued semantics — `x NOT IN (… NULL …)` is
  NULL, not TRUE; an empty subquery makes `IN` FALSE and `NOT IN` TRUE;
- `= ANY` / `<> ALL` and the other comparison quantifiers (`ANY`/`SOME`/`ALL`,
  added to the parser in this wave — `ALL` desugars to `NOT (inv(op) ANY …)`,
  the identity DuckDB's parser applies in
  `src/parser/peg/transformer/transform_expression.cpp`);
- scalar subqueries, including correlated aggregates. Zero rows gives NULL,
  more than one row is a runtime error, and `count(…)` over no rows is 0 — the
  classic COUNT bug, which has its own cases in the probe corpus.

The probe corpus is `crates/pinkdb/tests/correlated_subqueries.rs`: 68 cases,
every expectation pinned *and* re-checked against the DuckDB CLI when the binary
is present. `crates/pinkdb/tests/correlated_fuzz.rs` generates the spaces
between them and compares every case against DuckDB (see "Tests").

## 1. Binding: a scope chain

`Binder` gained `outer: Option<Rc<OuterScope>>`, a linked chain of the enclosing
blocks' `ScopeTable` lists. A subquery binds through `subquery_scope()`, which
pushes the current block's tables as a new frame; a lookup that misses the
subquery's own FROM walks the chain and yields

```rust
BoundExpr::OuterRef { depth: usize, index: usize, dtype: DataType }
```

`depth` 0 is the immediately enclosing block, 1 its parent, and so on; `index`
addresses that block's row exactly like `Column`. A three-part name
(`db.table.column`) resolves through the same chain, so it now agrees with the
two-part spelling.

`OuterRef` is opaque to everything that rewrites or moves columns:

- `has_column_ref() == true`, so constant folding never freezes one;
- `is_aggregate_free() == true`, and `validate_grouped` accepts it — an outer
  value is loop-invariant for this block's grouping;
- `columns_in` (filter pushdown) ignores it: an outer column is not a column of
  the join side being pushed into;
- `shift_columns`, `map_columns`, `resolve_agg_refs`, `rewrite_through_keys`
  pass it through;
- `collect_expr_projections` *does* keep the columns a correlated body reads
  (`outer.row_columns()`), so a scan projection cannot prune them away.

Each subquery expression carries the correlation it needs at run time:

```rust
pub struct OuterBind {
    pub refs: Arc<[(usize, usize)]>,     // (depth, index) inside the body plan
    pub deep: bool,                     // a subquery inside the body reads a row above this block
    pub keys: Option<Arc<[Option<usize>]>>, // from-space column -> aggregate-output position
}
```

`refs` is collected by the binder itself (`Binder.outer_uses`), not by walking
the finished plan: `lookup_column` records every outer reference it resolves, and
`bind_subquery` takes the child's list. A reference to the block's own row is
plan-internal and is not propagated upwards, so `refs` on an expression always
means "columns of the row this expression is evaluated against".

## 2. Execution: the per-key fallback

The evaluator keeps a stack of materialised outer rows in `Cx`
(`outer_rows: Vec<Arc<[Value]>>`, innermost last), and `OuterRef { depth, index }`
reads `outer_rows[len - 1 - depth][index]`. A correlated subquery is evaluated
by grouping the current chunk's rows by the outer columns the body reads,
pushing one representative row per distinct key, and running the body once per
key (`eval_correlated` in `crates/pinkdb/src/exec/expr.rs`). Because the context
is cloned per key rather than mutated, the path is reentrant and runs under the
existing parallel chunk execution.

This is the *fallback*: it is correct for every form and every depth — a
correlation two levels deep works because the stack holds each level — and it is
the only path that can report "more than one row" for a scalar subquery the
planner could not prove unique. Its cost is one body execution per distinct key,
visible in `EXPLAIN` as the body still rendered inside the predicate.

**Memoization must not cross outer rows.** The uncorrelated path memoizes a
subquery result under the plan node's address; that answer is row-independent, so
it is shared for the whole statement. A correlated body's answer is not, so the
correlated paths never read or write that memo (nested *uncorrelated* subqueries
inside a correlated body still use it, which is sound). Getting this wrong
silently returned the first row's answer for every row; it is a rule, not an
optimization detail.

## 3. Decorrelation into joins (Neumann & Kemper, BTW 2015)

The rewrite lives in `crates/pinkdb/src/sql/binder.rs` next to the block binder
(`decorrelate_clause` for `WHERE`/`HAVING`/`QUALIFY`, `decorrelate_project` for
the select list). It runs after the block's plan is assembled and before
`ORDER BY`/`LIMIT`, i.e. on the plan the optimizer and executor will see, so the
existing rules (comma-join hash conversion, filter pushdown, join reordering)
apply to the new joins exactly as they do to the old ones.

A conjunct is handled bottom-up:

1. `decorrelate_predicate` recognizes a whole conjunct that *is* `EXISTS`,
   `IN`/`ANY`/`ALL` or a bare scalar subquery;
2. otherwise `decorrelate_expr` rewrites the first scalar subquery found
   anywhere in the expression (`l_quantity < (SELECT …)`, `CASE`, `COALESCE`,
   arithmetic) and the loop repeats, because the plan widens by one join per
   rewrite and the next pass needs the new schema.

The body is analysed by `correlated_body`: it strips `Project`s, then strips the
whole `Filter` chain (including filters sitting on leaves inside the FROM tree,
with their column indices rebased), and requires what remains to be a FROM tree
(scans, `Values`, inner/cross joins, table functions). Conjuncts are classified:

| conjunct | goes to |
|---|---|
| equality between a body column and an outer expression | join key |
| reads no outer column | the body's own filter |
| reads an outer column but is not an equality | residual (forces the keyed shape) |

`dependent_join` then builds the correlation side. Two shapes exist, both
producing a join with the enclosing plan on the left:

- **keyed** (scalars, `IN`): the correlation keys are duplicate-eliminated into
  `Distinct(Project(Filter(enclosing plan)))`, the body joins that key side, and
  the residual is a `Filter` above the join. The body's own key column is what
  the join hashes on, so no body expression is renumbered.
- **plain** (`EXISTS`/`NOT EXISTS`): the body's own key columns are used as the
  right keys directly, so the enclosing block is not scanned twice. A semi/anti
  join does not care about duplicate body rows, which is why this is safe here
  and not for a scalar aggregate.

Then:

| form | plan |
|---|---|
| `EXISTS` / `NOT EXISTS` | `Join { kind: Semi \| Anti }` on the correlation keys; the conjunct is dropped from the predicate |
| `IN` / `NOT IN` / `ANY` / `ALL` (`=`, the marked `<> ANY`) | `Join { kind: Mark }` — output `left ++ [BOOLEAN __mark]` — predicate `__mark` or `NOT __mark` |
| scalar subquery | `Aggregate { keys: correlation keys }` over the keyed body, then `Join { kind: Left }` on those keys; the expression is rewritten to the aggregate output column, with `count` wrapped in `COALESCE(x, 0)` |

`ALL` never reaches the planner as `ALL`: the parser inverts the comparison and
negates, so a mark join computes it.

### The COUNT bug

A naive "group the body, left join" gives NULL for a group the body never
produced, but `count(*)` and `count(x)` must give 0. The bundle of keys the left
side has and the body did not produce is exactly the NULL-extended join row, so
each `count` aggregate is wrapped in `COALESCE(col, 0)` while `sum`, `avg`, `min`
and `max` keep the NULL. That matches DuckDB 2.0, where only `count` declares
`FunctionNullHandling::SPECIAL_HANDLING`
(`src/function/aggregate/distributive/count.cpp`) and the ungrouped case reads the
empty-group value via a marker aggregate
(`FlattenDependentJoins::PushDownAggregate`, `src/planner/subquery/flatten_dependent_join.cpp`).
`scalar_count_star` and `scalar_count_col` in the corpus pin it, including the
group that appears twice in the outer table and the group that appears not at all.

### Join kinds

`JoinKind` gained `Semi`, `Anti` and `Mark` (`crates/pinkdb/src/sql/ast.rs`), all
implemented in the **hash** join, never as nested loops:

- `hash_mark_join` (`crates/pinkdb/src/exec/mod.rs`) probes the build side for
  existence and, for `Mark`, also looks the correlation-key prefix up in a
  `count_star`/`count(value)` table. Semantics per probe row: a full-key hit is
  TRUE; no hit but the prefix saw a NULL value is NULL; a NULL left key is NULL;
  no prefix at all is FALSE. `NOT IN` is `NOT marker`, and the surrounding
  `Filter` drops NULLs, so the three-valued table comes out right — including
  `1 NOT IN (2, NULL)` = NULL and `1 NOT IN (empty)` = TRUE.
  (`physical_hash_join.cpp` / `join_hashtable.cpp`'s `NextMarkJoin` is the
  reference for that table.)
- `Semi`/`Anti` output the left schema only; `Mark` appends `__mark`.

Schema, EXPLAIN and the optimizer were taught the new kinds: `plan_schema` and
the executor's join arm compute the kind-dependent width, `join_name` prints
`semi`/`anti`/`mark`, and `column_origin` never marks a left column as
NULL-extended.

### ANY/ALL execution

`IN` over `=` uses the hash-set probe it always used. The ordered quantifiers
collapse the set to its extremes: `x < ANY(s)` is `x < max(s)`, `x > ANY(s)` is
`x > min(s)`, `x <> ANY(s)` is `x <> min(u) OR x <> max(u)`, and a set that held
a NULL turns a non-match into NULL. `ALL` is the parser's `NOT(ANY(inv))`.
Verdicts are OR-ed in three-valued logic with FALSE as the identity, which is what
makes an empty set FALSE rather than NULL.

## 4. Optimizer

- **A correlated expression never moves.** `has_correlated_subquery` guards
  `push_one_level`, so a conjunct holding a correlated subquery stays where it
  was bound; pushing it into a join side or through a projection would renumber
  the row it reads. `correlated_columns` is folded into the FK→PK elimination
  pass's live-column set for the same reason.
- **Semi/anti/mark are inert** for the comma-join passes (`comma_join_leaves`,
  `reorder_comma_join`, the nested-loop→hash upgrade all accept `Inner`/`Cross`
  only).
- **Pushdown**: nothing is pushed into a semi/anti build side (`Semi|Anti` only
  accept a left-only conjunct, which is the shape that keeps the result the
  same); a mark join appends its marker after the left columns, so conjuncts
  above it stay put (`Full|Mark` keep).
- The key side of the keyed shape carries the block's own filters (the clause's
  other conjuncts, minus anything with a dynamic scalar or a subquery, which must
  not run twice), so the optimizer can turn that copy into the same hash joins
  instead of a cartesian product. Without it, Q21's four-table comma chain was
  cloned as a cross product — the difference between 7.2 s and a hang.

## Fallback shapes

These stay on the per-key path (correct, linear in the number of distinct outer
keys, visible in `EXPLAIN` because the subquery stays in the predicate):

- a body whose FROM is a projection, `LIMIT`, `DISTINCT`, a set operation, a
  window or an already-grouped aggregate — i.e. anything that is not a FROM tree
  after the filters are stripped. A derived table *inside* the body is therefore
  a fallback (Q2's shape was fixed by stripping filters off the FROM tree's
  leaves, not by opening this up);
- `outer.deep` — a subquery nested inside the body that itself reads a row above
  this block (two levels deep, e.g. an `EXISTS` inside an `EXISTS` body). The
  rewrite can carry one outer row, not two;
- a correlated scalar subquery whose body aggregates with `GROUP BY`: it may
  return several rows, and the "more than one row" error must survive, so it is
  not rewritten;
- pure range correlation with no equality key (`BETWEEN`, a bare `<`): it would
  degenerate into a cartesian product of the blocks, so the per-key path is both
  simpler and cheaper. A mixed correlation (equality plus a range) *is*
  rewritten, with the range as the residual `Filter`;
- a correlated scalar subquery under `NOT`;
- `IN`/`ANY` with an operator other than `=` (only equality is a hashable key);
- a correlated subquery in a *grouped* block whose body reads an outer column
  that is not a group key. There is no single value for such a column in a
  group, so this is a bind error (`SELECT/HAVING expression must be an
  aggregate or appear in GROUP BY`), which is also what DuckDB says; reading an
  arbitrary group member would be silently wrong;
- subqueries in `ON`, in `ORDER BY`, in `GROUP BY` keys and in window arguments.
  The fallback's pushed row must be the block's FROM row: true for `WHERE`
  (and through `OuterBind::keys` for `SELECT`/`HAVING`/`QUALIFY`, where the row
  is the aggregate's output). An `ON` clause sees the join's own concatenation,
  which is the block's FROM prefix in the plans the binder builds, and the corpus
  pins one case there.

None of these is required by TPC-H or by the corpus's harder cases; they are the
recorded work left for a later wave.

## Fix round (review)

Three bugs survived the first landing; all three are fixed, pinned, and the last
two were found by the generator rather than by a hand-written case.

1. **The mark join's value key.** `decorrelate_predicate`'s `IN` arm built the
   mark join's value key as `Column { index: key_base + nkeys }` — one past the
   dependent join's output. The value column never moves (the body sits on the
   dependent join's left, unshifted), so the operand must be the body's own
   projected expression. Every pinned `IN` case was *uncorrelated* and took the
   memoized set path, so nothing executed the mark join; the reviewer's
   `WHERE g NOT IN (SELECT v FROM i WHERE i.oid = o.id)` (operand column ≠ the
   correlation column) hit it with `column 3 out of range (chunk has 3 columns)`.
2. **The empty-group rule.** `x IN (no rows)` is FALSE even when `x` is NULL, so
   `NOT IN` is TRUE. `hash_mark_join` decided the NULL-ness of the operand
   before looking at the correlation group, and the per-row `probe_in` consulted
   the NULL flag before the empty-set flag; both now decide the group first. The
   same fix corrects the pre-existing uncorrelated `NULL IN (SELECT … WHERE
   1=0)` answer (it was NULL, DuckDB says FALSE).
3. **The derived-table census.** `Binder::child()` gave every nested block a
   fresh `outer_uses` list, but only subquery bodies ever folded that list back,
   so an outer reference made one block in — a derived table inside a correlated
   body reading the enclosing row — vanished from the body's census and the
   per-key fallback pushed a row with that column NULL. Nested blocks that keep
   the same `outer` chain now share the list; only `subquery_scope()` starts a
   new one.

The generator also pushed the grouped-block rule above into place: a correlated
subquery inside a grouped block read a non-grouped outer column, silently
returning an arbitrary group member. `rebase_to_keys` additionally never built
the key map for `ScalarSubquery`/`Exists` (only `InSubquery`), so in a grouped
block the body's outer references addressed FROM-space positions instead of the
aggregate's output — invisible for a single group key in column 0, wrong for any
other. Both are fixed (`validate_correlated_grouped`, and all three subquery
forms now take `through_keys`).

## Tests

- `crates/pinkdb/tests/correlated_subqueries.rs` — 5 tests:
  - `correlated_forms_match_duckdb`: 68 pinned cases, each also re-run against
    the DuckDB CLI when the binary is present (`/tmp/duckdb-2.0.0/duckdb` or
    `PINKDB_DUCKDB`), so a pin cannot silently drift. Covers NULLs on both
    sides, duplicates, empty inner and outer inputs, the COUNT bug, multi-column
    correlation, correlation two levels deep, a correlated subquery inside a
    derived table and inside `CASE`, non-equality correlation (`<`, `BETWEEN`),
    `EXISTS`/`IN` inside `HAVING`, `ON`, the select list and behind `OR`, and the
    scalar-select-list form with duplicates on the outer side;
  - `scalar_subquery_multiple_rows_errors`: the runtime error, message included;
  - `explain_shows_the_decorrelated_plans`: `Join semi`, `Join anti`, `Join mark`
    and `Join left` over an `Aggregate keys=[…]` are what `EXPLAIN` prints;
  - `correlated_dml_replays_from_the_wal`: correlated `UPDATE … WHERE` and
    `DELETE … WHERE` (including a COUNT-bug scalar and a NULL-returning one),
    then the same statements replayed from the WAL on reopen;
  - `correlated_outer_column_must_be_grouped`: a correlated subquery in a
    grouped block may only read group keys; DuckDB refuses the same shapes.
- `crates/pinkdb/tests/correlated_fuzz.rs` — a seeded generator (stdlib
  `xorshift64*`, no new dependency) builds three small tables (`fa(a,b,c)`,
  `fb(d,e,f)`, `fc(g,h)`, 0–8 rows, 15% NULLs, values 0..5) and draws correlated
  predicates from a grammar: `EXISTS`/`NOT EXISTS`, `IN`/`NOT IN` with the
  operand equal to or different from the correlation column, `<op> ANY|SOME|ALL`,
  scalar comparisons, `CASE`, `NOT`, `OR`, two-level nesting and a derived table
  inside the body, over equality, `<` and `BETWEEN` correlation, single and
  two-column. Sites: `WHERE`, the select list, `CASE`, both `HAVING` forms, and
  projection + filter together. Every case runs on both engines and both results
  are sorted and compared as text; error kinds must agree ("more than one row"
  vs "other"). Default run: seeds 1–5 × 100 cases = 500, about 11 s;
  `W120_FUZZ_SEEDS=6,7,8` and `W120_FUZZ_CASES=150` widen it (a 3000-case soak at
  seeds 20–39 took 64 s; seeds 6–13 × 150 = 1200 also pass). It skips with a
  note when the reference binary is absent, and prints the SQL, both outcomes and
  the whole dataset on a mismatch.
  Scalars are aggregates only: a plain column can return several rows, and
  DuckDB raises that error in only *some* plan shapes (`SELECT (subq)` and
  `(subq) = 0` error, `a < (subq)` quietly keeps the first row), so it cannot be
  compared case by case. pinkdb always raises it, per the standard; the corpus
  pins the message and DuckDB's agreement in the projection shape.
- `crates/pinkdb/tests/three_part_columns.rs`: the old "correlated outer
  three-part reference is refused" test became "… resolves like the two-part
  spelling" — its subject changed, so it was ported rather than deleted.
  Likewise `exec/mod.rs`'s `correlated_is_rejected` became
  `correlated_exists_reads_the_outer_row`.
- `bench/suite`: `UNSUPPORTED_BY_DESIGN` is gone (all 22 queries run), and the
  DuckDB-free `ci` family gained `corr_scalar`, `corr_exists` and `corr_not_in`
  so the three rewrites have a regression baseline of their own.
- TPC-H: all 22 queries at SF 0.01 and SF 0.1 match DuckDB's rows and checksums
  (the suite compares both before it times), and SF 1 does too.

## Measurements (Apple M4, 10 cores, 8 threads, DuckDB 2.0.0)

All 22 queries at SF 0.1, release build: geometric mean **9.3×** DuckDB
(8.0× over the 16 queries wave 111 could run, against 8.6× then). The five
queries the brief names: Q4 16 ms (8.0×), Q17 7 ms (3.5×), Q20 50 ms (16.7×),
Q21 407 ms (81.4×), Q22 57 ms (19.0×). At SF 1: Q4 163 ms (23.3×), Q17 94 ms
(11.8×), Q20 1933 ms (175.7×), Q21 7222 ms (401.2×), Q22 794 ms (56.7×).

Not quadratic, with the exception of Q20: for 10× the rows, Q4 16 → 163 ms,
Q17 7 → 94 ms, Q22 57 → 794 ms, Q21 407 → 7222 ms, Q20 50 → 1933 ms (38×).
Q20's growth is the duplicate-eliminated key side — a `Distinct(Project(…))` over
a copy of the enclosing block, recomputed per subquery.

The shape choice above was measured, not assumed: with the keyed shape for every
form, SF 0.1 gave Q4 16, Q17 7, Q20 50, Q21 407, Q22 57 ms; with the body's own
keys for `EXISTS`/anti, Q4 16, Q17 39, Q20 30, Q21 427, Q22 66 ms. Hence
`EXISTS`/anti use the plain shape (no second scan of the outer block) and scalar
aggregates the keyed one.

## Follow-ups recorded, not done here

- **Delim join**: materialise the correlation keys once and share them across
  every subquery of a block (`src/execution/physical_plan/plan_delim_join.cpp`,
  `src/optimizer/deliminator.cpp`). This is the fix for Q20's superlinear growth
  and for the second scan of the outer block in the scalar shape.
- **A join-residual slot**: `Join` carries only equi keys, so a non-equality
  correlation becomes a `Filter` above the join. DuckDB keeps it in the join
  (`physical_hash_join.cpp`'s extra conditions), which is what Q21 needs
  (`l3_suppkey <> l1_suppkey` forces the keyed shape and a pair filter).
- Unnesting the fallback shapes: a body with `LIMIT`/`DISTINCT`/`GROUP BY`/a set
  operation, two-level correlation, scalar-under-`NOT`, and the `ON`/`ORDER BY`/
  `GROUP BY`/window-argument sites.
- Multi-column `IN`/`ANY` (row constructors), which DuckDB also refuses today.
