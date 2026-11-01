# QuackLab shadow run

Replays one ordered request log against both implementations of the QuackLab
HTTP contract and diffs the answers request by request:

* `--ref` — the Go service (`cmd/quacklab-vps-duckdb` in the grain repo), the
  reference the adapter is written against;
* `--pink` — `pinkdb-server --quacklab-bind …` (`crates/pinkdb-quacklab`).

Both sides receive the same requests in the same order, so state from an
earlier request is visible to a later one. Nothing is reset between requests
except by the log itself.

```
python3 bench/quacklab-shadow/shadow.py \
    --ref http://127.0.0.1:12940 --pink http://127.0.0.1:12941 \
    bench/quacklab-shadow/corpora/types.jsonl
```

`--only N` reruns request N (the prefix is replayed: it is that request's
state), `--from N` reruns from N on, `--verbose` prints a row for every request
instead of only the differences. Exit code 0 iff every request matches.

## What is compared

Per request: the HTTP status; for errors the `code` and the DuckDB message
prefix (`Catalog Error:`, `Binder Error:`, `Conversion Error:`, … — the rest of
the message is engine wording and is not compared); for results `columns`,
`rows`, `rowCount`, `truncated` and `rowsAffected`.

Rows compare by value **and JSON type** (`1`, `1.0` and `"1"` are three
different cells) in order, unless the request is marked `"unordered"`: row
order is then a multiset. Every timing field is ignored. Health and lease
bodies carry none of those fields, so only their status is compared — their
payloads are pinned by `cargo test -p pinkdb-quacklab`.

A lease id returned by `/write-lease/acquire` (or `renew`) is substituted into
later requests, per side: any JSON string equal to `"$lease"` becomes that
side's own id. A log that uses `$lease` before an acquire fails on the side
that has none.

## Request log format

JSON lines, one request per line:

```json
{"path": "/sql", "method": "POST", "body": {"sql": "SELECT 1"},
 "auth": "token", "unordered": false, "note": "a line for the report"}
```

`auth` is `token` (send the bearer token) or `none`. `note` is free text and
appears in the output. A missing `body` sends no request body.

## Running the reference

```
cd ~/Desktop/projects/grain/grain/vps
go build -o /tmp/ref/qvd ./cmd/quacklab-vps-duckdb
DUCKDB_BIND=127.0.0.1:12940 DUCKDB_PATH=/tmp/ref/warehouse.duckdb \
DUCKDB_SQL_TOKEN=tok DUCKDB_TEMP_DIRECTORY=/tmp/ref/t \
DUCKDB_MEMORY_LIMIT=1GB /tmp/ref/qvd
```

The file name matters: DuckDB names a file catalog after the file, so
`DUCKDB_PATH=…/warehouse.duckdb` makes `warehouse` the catalog name, which is
what pinkdb's adapter serves. Both sides then run the same three-part
`warehouse.<schema>.<table>` SQL. (The alternative — attaching `r2_cat` on
one side and creating it on the other — would test a setup the deployment
never has.)

## Running pinkdb

```
cargo build --release -p pinkdb-server --features quacklab,httpfs,parquet
DUCKDB_SQL_TOKEN=tok ./target/release/pinkdb-server \
    --in-memory --host 127.0.0.1 --port 55431 \
    --quacklab-bind 127.0.0.1:12941
```

`httpfs` (and `parquet`) are what the image ships: the goldens read their
fixtures with `read_csv`, and without the readers every one of those
statements answers `Catalog Error: unknown table function "read_csv"` and the
rows after it differ too.

## Corpora

| corpus | from | requests |
|---|---|---|
| `types.jsonl` | the typed-cell cases of `crates/pinkdb-quacklab/tests/fixtures/go-reference.json` | 7 |
| `lease.jsonl` | the choreography of `crates/pinkdb-quacklab/tests/fixtures/go-lease-reference.json`, with `$lease` | 18 |
| `goldens.jsonl` | grain's `fixtures/shared/goldens/*.json`, via `make_goldens.py` | 249 |

`make_goldens.py` regenerates `goldens.jsonl`; it replays each fixture as one
session: drop the fixture's schemas, `CREATE SCHEMA` / `CREATE TABLE` /
`CREATE VIEW` (statement extraction and setup follow
`tools/quacklab-goldens.py` in the pinkdb worktree), then every statement the
fixture carries. Two rewrites: `r2_cat.` becomes `warehouse.` (see above),
`read_csv('s3://$rawBucket/$sourcePath', …)` becomes a local gzip CSV under
`/tmp/qlaudit` and `$batchId` becomes `batch-1` (the same substitution the
reference tool makes). A query without a top-level `ORDER BY` is marked
`unordered`.

## Differences that are expected

Deliberate, per the wave 138 plan:

* timings and request ids are not part of the contract (not compared);
* lease ids are UUIDs: the Go service formats them one way, the adapter
  another — the log only checks that an id round-trips;
* `rows` beyond `GRAIN_SQL_RESULT_ROW_LIMIT` truncate at the adapter's own
  limit, so a corpus that changes the limit changes both sides.

## Last measured run (2026-11-01)

Against the Go service in file mode (`DUCKDB_PATH=/tmp/ref/warehouse.duckdb`)
and `pinkdb-server --features quacklab,httpfs,parquet`:

| corpus | match | differ |
|---|---|---|
| `types.jsonl` | 6 | 1 |
| `lease.jsonl` | 18 | 0 |
| `goldens.jsonl` | 224 | 25 |

The remaining differences are all engine gaps in pinkdb, not adapter bugs;
every successful statement answers byte-for-byte the same result:

* `types.jsonl`: one statement (the typed literals) — pinkdb has no `TIME`
  type or literal (`TIME '01:02:03'`, `'…'::TIME`) and no 128-bit integer,
  so `170141183460469231731687303715884105727::HUGEINT` is "integer literal
  out of range". Its other cells (`INTERVAL 3 DAY`, `1e300`, `-0.0`,
  `DECIMAL(38,3)`, timestamps, dates) match.
* `goldens.jsonl`, 10 statements `DELETE FROM <a view>`: DuckDB says
  `Binder Error: Can only delete from base table`, pinkdb resolves the target
  as a base table only and says `Catalog Error: Table with name … does not
  exist!`.
* `goldens.jsonl`, 10 statements `INSERT INTO <a view> SELECT * FROM (…)`:
  DuckDB binds and rejects the view, pinkdb's parser stops first with
  `Parser Error: … derived table needs an alias` (DuckDB accepts an unaliased
  derived table).
* `goldens.jsonl`, 5 `read_csv` loads: the fixture rows are `v0,v1,…`, so both
  sides fail the cast to `BIGINT`; DuckDB reports `Conversion Error: CSV Error
  on Line: 2 …`, pinkdb `Binder Error: cannot CAST "v1" AS BIGINT` from the
  reader's bind step, with no line context.
