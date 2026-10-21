# Wave 83 — procedure transaction control + EXCEPTION handlers (plan)

Started 2026-10-21; **done** the same day. Scope (ROADMAP row 83): Postgres-style `COMMIT` /
`ROLLBACK` inside SQL procedure bodies, allowed only when the `CALL` has no
enclosing transaction; block-scoped `EXCEPTION` handlers (savepoint under
the hood, plpgsql-style); a docs note on when a procedure should manage its
own transaction, compared with Snowflake.

## Semantics

### Transaction control (`COMMIT` / `ROLLBACK` in a body)

- Allowed only in the **outermost** procedure of a **standalone** `CALL`
  (no `BEGIN` open). Inside an explicit transaction the statement fails with
  a "started at a different scope" error naming the remedy; in a nested
  procedure (depth > 1), a `DO` block or a JavaScript body it is refused.
- Without any `COMMIT`/`ROLLBACK`, a `CALL` is unchanged: statement-atomic,
  logged as `CALL p(args)`, replayed by re-execution.
- `COMMIT` makes everything since the call started (or since the previous
  `COMMIT`) durable and starts a new transaction inside the call; the
  call's atomic restore point moves to it. `ROLLBACK` restores that point
  and continues. If the body later fails, the store returns to the last
  `COMMIT` — committed work survives, the failed tail does not.
- Refused statically (at `CREATE`) inside a block that has an `EXCEPTION`
  handler (body or handler), like Postgres's "cannot commit while a
  subtransaction is active".
- On-commit materialized views refresh at the next statement boundary
  after the `CALL` (boundaries are never mid-statement).

### WAL: committed segments

A durable store runs a standalone `CALL` under the WAL lease. At each
`COMMIT` the executor appends one record: the **segment** — every writing
statement that ran since the previous commit, each rendered as a
self-contained anonymous block

```sql
DO 'BEGIN DECLARE n INTEGER DEFAULT 3; DECLARE p VARCHAR DEFAULT ''x''; <statement text>; END'
```

that declares every variable visible to that statement with its exact value
(`value_literal`, the prepared-statement literal renderer) and then runs the
statement's own source text. Replay rebinds it exactly like the live call
did, so the effect is identical without re-running the procedure — robust
to other writers' frames interleaving between segments. At the end of a
call that used transaction control, the runner logs the final segment
instead of `CALL p(args)`; a failed call logs nothing after its last
`COMMIT`. A `ROLLBACK` discards the pending segment.

### `DO` blocks

`DO '<body>'` (any string literal, `$$…$$` included) runs an anonymous SQL
procedural block — Postgres syntax. Atomic like a `CALL`, no parameters,
transaction control refused, logged as its own text. It is the replay
vehicle for committed segments and useful on its own.

### `EXCEPTION` handlers

```sql
BEGIN
  INSERT INTO t VALUES (1);
  INSERT INTO t VALUES (1 / 0);
EXCEPTION WHEN OTHERS THEN
  INSERT INTO errors VALUES (SQLERRM);
END
```

- Only `WHEN OTHERS` (pinkdb errors carry no SQLSTATE yet); other
  conditions are refused at `CREATE` with that explanation.
- Entering the block takes a savepoint (a store snapshot — like Postgres,
  a block with a handler costs more than one without). An error inside the
  block restores it (data and catalog), drops the block's statements from
  the pending WAL segment, and runs the handler with `SQLERRM` (VARCHAR)
  bound to the message. Variable values are **not** rolled back (plpgsql).
- An error raised in the handler propagates normally.
- A WAL write failure (fail-stop) is never caught.
- The call's touched set keeps the failed statements' reads (conflict
  detection stays conservative).

## What changed during implementation

- **Two pre-existing parser bugs fixed** in `parse_ident_at` (the
  procedure body's identifier reader): quoted names were decoded byte by
  byte (non-ASCII names were mangled), and a quoted `DECLARE` name returned
  an already-trimmed remainder, so `DECLARE "x" INTEGER` always failed with
  "not a whole identifier". Committed segments re-declare variables by
  name, so both had to work.
- **Stack use.** Savepoints and restore points are whole-store clones;
  `Box::new(db.clone())` builds the clone on the stack first, so the clones
  live in `#[inline(never)]` helpers (`boxed_snapshot`, `exec_guarded`,
  `txn_control`) to keep the recursive `exec_stmts` frame small. Debug
  builds still lose about one level of recursion headroom on a 2 MiB
  thread; the pre-existing gap (overflow below `MAX_CALL_DEPTH` in debug)
  is recorded in `docs/GAPS.md`.
- Body `SQL` statements now keep their full source text (`source`) next to
  the 60-character message snippet.

## Compared with Snowflake (docs note)

Snowflake lets a procedure run scoped/autonomous transactions, but a
procedure called outside a transaction is **not atomic** — a failure
leaves earlier statements committed. pinkdb keeps a standalone `CALL`
atomic by default and makes partial commits explicit (`COMMIT` in the
body). Snowflake-style autonomy (an inner commit surviving an outer
rollback) remains a documented follow-up.

## Not in this wave

`COMMIT AND CHAIN`, savepoint SQL (`SAVEPOINT`), named exception
conditions / SQLSTATE, transaction control in JavaScript bodies or `DO`
blocks, `GET DIAGNOSTICS`.
