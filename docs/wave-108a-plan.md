# Wave 108a — full-text search (BM25 index, DuckDB fts parity)

ROADMAP row 108, part (1). Row 109 (agent memory) needs BM25 ranking and
Reciprocal Rank Fusion, so this lands first; the rest of 108 (splitters,
ingestion, `rag_search`) stays queued. DuckDB 2.0's `fts` extension is the
reference; it is **not installed and is not downloaded** — the behaviour
below comes from its documentation and its `fts_indexing.cpp` macros.

## SQL surface (DuckDB names and defaults)

```sql
CALL create_fts_index(
  input_table, input_id, *input_values,
  stemmer := 'porter',            -- 'porter' | 'none'
  stopwords := 'english',         -- 'english' | 'none'
  ignore := '(\\.|[^a-z])+',      -- the default token separator regex
  strip_accents := 1, lower := 1, overwrite := 0);

CALL drop_fts_index(input_table); -- drops the index database

SELECT *, match_bm25('docs', id, 'the query', fields := 'title,body',
                     k := 1.2, b := 0.75, conjunctive := 0) AS score
FROM docs ORDER BY score DESC;

SELECT stem('running');            -- 'run'
SELECT rrf(rank, k := 60);         -- 1 / (k + rank), NULL below rank 1
```

Differences from DuckDB, all deliberate:

| DuckDB | here | why |
|---|---|---|
| `PRAGMA create_fts_index` / `PRAGMA drop_fts_index` | `CALL create_fts_index(...)` / `CALL drop_fts_index(...)` | pinkdb has no PRAGMA; a package-provided `CALL` is the closest shape and needs no new statement kind. Both are `Global` statements, so they need ADMIN and are refused in a read-only session (an index build writes). |
| `fts_main_docs.match_bm25(id, query, ...)` (a macro in the index schema) | `match_bm25('docs', id, query, ...)` | pinkdb cannot schema-qualify a function call, so the index (the base table name, as given to `create_fts_index`) is the first argument. |
| macro over a table, `fields` is a list | host scalar over the table, `fields` is a comma-separated VARCHAR | `LIST` does not exist yet (wave A). `fields := NULL` means all fields. |
| index schema holds tables *and* the two macros | the index database holds only tables; the functions are the extension's | same functions serve every index. |

`create_fts_index` arguments are positional-then-named with DuckDB's names,
so a ported query reads the same apart from `PRAGMA` → `CALL`. A host
statement and a host scalar declare their parameter names, so the
`CALL`/scalar parser accepts `name := value` for them; a stored procedure
still binds positionally and refuses a named argument, as before.

## How the index is built — a small, separate engine hook

pinkdb has databases (namespaces), tables and macros, so DuckDB's shape —
a database `fts_main_<table>` holding the index tables — works unchanged.
What pinkdb lacks is a way for a linked extension to *run SQL and write
tables*: linked scalar functions see values only. Three options were
considered:

1. a `PRAGMA` statement kind — new parser/binder/exec surface for one
   extension;
2. a hard-coded `CALL create_fts_index` in the engine — puts fts policy in
   the engine, which must not link SQLite/Postgres and should stay lean;
3. **two optional surfaces on `ExtensionPackage`** (chosen):
   - `with_host_scalars`: a per-row function that also receives the store
     (`&PinkDb`) — `match_bm25`. It is bound and evaluated through the
     existing `DynamicScalar` node, with named arguments;
   - `with_host_statements`: a `CALL <name>(…)` that runs when no stored
     procedure of that name exists, with a `HostCtx` giving the store,
     `execute(sql)` (through the internal statement path, so the CALL stays
     one WAL record), and `insert_rows` (the ordinary insert funnel: keys,
     triggers, constraints, versions).

The engine addition is ~300 lines, all of it in the open: two optional
surfaces on `ExtensionPackage` (a `HostScalar` with `&PinkDb`, a
`HostStatement` running on a `HostCtx`), the binder/executor wiring that
reuses the existing `DynamicScalar` node and `LogicalPlan::Call`, a
`PinkDb::store_id()` (a process-unique id the extension keys its in-process
cache on), `Database::is_extension_loaded` (the replay guard a host uses
before loading a built-in package), and the `pinkdb::ExtType` re-export the
extension needs to name its parameter types. No new plan node, no new
statement kind, no new dependency, nothing that links SQLite or Postgres. A statement is `CALL`, hence `Global`: refused in a
read-only session and refused under the read-only MCP policy, which is
exactly the wanted behaviour for a write.

## Permissions

`match_bm25` reads the index tables, which hold the base table's text, so a
host scalar that only reads must not bypass RBAC: the package's `ReadsFn`
derives the base table from the constant index-name argument, the binder
records it on the `DynamicScalar` expression, and
`plan::collect_tables_read` includes it, so the server checks `SELECT` on
the base table exactly as for a scan. A non-constant index name is refused
at bind time, so the check is static. `create_fts_index`/`drop_fts_index`
are package host statements behind `CALL`, which the server already treats as
`Global` (ADMIN, refused in a read-only session). The embedded engine
applies no RBAC, so the CLI path is unchanged.

## Index layout (`fts_main_<table>`)

| table | columns |
|---|---|
| `meta` | `key VARCHAR, value VARCHAR` — format, stemmer, stopwords, ignore, lower, strip_accents |
| `docs` | `doc_id BIGINT, field_id INTEGER, len BIGINT` — one row per (doc, field), zero lengths included |
| `dict` | `term_id INTEGER, term VARCHAR` — unique stems, `term_id` ascending by term |
| `terms` | `term_id INTEGER, doc_id BIGINT, field_id INTEGER, tf BIGINT` — postings |
| `term_stats` | `term_id INTEGER, df BIGINT, idf DOUBLE` |
| `field_stats` | `field_id INTEGER, name VARCHAR, avg_len DOUBLE` |

`doc_id` is the base table's id-column value (the column named by
`input_id`), so a query joins the score back on the same key it selected.
Building is deterministic: terms are sorted before ids are assigned and
postings are written in `(term_id, doc_id, field_id)` order, so a replayed
`CALL` rebuilds the identical tables. `meta.format` lets a reader refuse an
index another version wrote.

## Scoring — DuckDB's BM25

```
N     = number of documents           df(t) = documents containing t in any field
idf   = ln(1 + (N - df + 0.5) / (df + 0.5))
score = Σ_t idf(t) · Σ_f tf(t,d,f)·(k1+1) / (tf + k1·(1 - b + b·len(d,f)/avg_len(f)))
```

Defaults `k1 = 1.2`, `b = 0.75`; query terms are deduplicated; only fields in
`fields` are scored (NULL/`*` = all indexed fields); `conjunctive := 1`
keeps only documents that contain **every** query term (score unchanged).
A document with no matching term scores NULL, as DuckDB's macro returns.
This is the canonical BM25 that
DuckDB's generated macro computes, and it is checked against a Python
reference implementation (`extensions/fts/tests/reference/bm25.py`, stdlib
only) that generates the expected-score fixture; the Rust test asserts
`|a - b| < 1e-9` and the same top-k order.

A real DuckDB comparison is **pending**: it needs `fts` installed, which
this environment forbids. That check is a reviewer task.

## Text processing

- tokenizer: lower-case → strip accents (Latin-1/Latin Extended-A table) →
  split on the `ignore` pattern (default: DuckDB's `(\\.|[^a-z])+` — DuckDB
  strings do not escape, so each separator is a backslash plus the next
  character, or any character outside `a-z`; `foo\nbar` with a literal
  backslash-n is `foo`, `bar`), drop empty tokens.
  The default pattern is a hand-written scanner, so a plain `fts` build does
  not link `regex`; test `the_default_scanner_equals_the_default_regex`
  fuzzes the scanner against `regex::Regex::new(DEFAULT_IGNORE)` (a
  dev-dependency) on 2000 random strings. Any other `ignore` needs the
  crate feature `custom-ignore` (CLI/server feature `fts-regex`); without
  it, `create_fts_index` fails with a message naming `fts-regex`.
- stopwords: DuckDB's `stopwords := 'english'` list — the SMART system's
  571 English stop words, checked in as `ENGLISH_STOPWORDS` (a unit test
  asserts the count). Transcribed offline, so a byte-for-byte diff against
  DuckDB's copy is a reviewer follow-up with the DuckDB comparison; the
  list's contraction entries (`don't`) can never match, because the default
  `ignore` splits on the apostrophe first — the same in DuckDB.
  `stopwords := 'none'` disables them.
- stemmer: Porter. DuckDB's `stemmer := 'porter'` is libstemmer's `porter`,
  i.e. Snowball's `porter.sbl` — **not** the 1980 paper's algorithm (its
  step 2 has `abli`, no `logi`, and it computes R1/R2 regions, not the
  paper's `m`). Hand-implemented from the `.sbl` rules with no dependency
  and checked against a checked-in word list: 2994 words from
  `/usr/share/dict/words` and their expected stems, generated by
  `tests/reference/porter.py` (stdlib only, an independent Snowball
  implementation checked in beside the fixture; re-check it with
  `--wordlist tests/fixtures/porter_stems.tsv`). The first paper-following
  version disagreed on 65 of them.

The stopword and `ignore` details come from DuckDB's documentation, not from
its installed extension, so the differential run with `fts` available
(reviewer task above) also diffs the 571-word list and the default
tokenizer's behaviour on the corpus.

## Static index

Like DuckDB, the index is a snapshot: after `INSERT`/`UPDATE`/`DELETE` on the
base table the index is **stale** until `create_fts_index(..., overwrite :=
1)` is run again. `meta.source_version` records the base table version at
build time; a stale index still answers (DuckDB's behaviour). Incremental
maintenance is a follow-up.

## Rank fusion (for row 109)

`rrf(rank, k := 60)` = `1 / (k + rank)`, 1-based rank; plus a documented SQL
pattern (union two ranked lists, `GROUP BY id`, `sum(rrf(rank))`, order by
score) in the README. Two `rrf` rows agreeing on one document add up, which
is the usual RRF.

## Tests

- `extensions/fts/tests/reference.rs`: scores and top-k order against the
  Python reference (fields, conjunctive, custom k/b), plus one tokenizer
  case through `match_bm25`;
- `extensions/fts/tests/stemmer.rs`: the fixture word list;
- `extensions/fts/tests/engine.rs`: errors (overwrite refused without
  `overwrite := 1`; missing table, id or value column; non-text column;
  unknown stemmer, unknown field, bad `ignore`, bad `k`), stale index
  semantics, `drop_fts_index`, save/open, WAL replay determinism;
- `crates/pinkdb-cli/tests/fts.rs` (feature `fts`): the SQL works without a
  `LOAD EXTENSION`, and a durable store keeps ranking after a reopen;
- `crates/pinkdb-cli/tests/mcp.rs` (feature `fts`): `match_bm25` runs under
  the read-only MCP policy while `create_fts_index`/`drop_fts_index` are
  refused;
- `crates/pinkdb-server/tests/fts_rbac.rs` (feature `fts`): a principal
  without `SELECT` on the base table is refused with a permission error, the
  same principal works after `GRANT SELECT`, and ADMIN works;
- `extensions/fts/tests/scale.rs` (`#[ignore]`): a 100k-document corpus,
  build time and query latency, reported in the wave report.

## Gates

```
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets -p pinkdb-cli --features fts -- -D warnings
cargo clippy --all-targets -p pinkdb-cli --features fts-regex -- -D warnings
cargo test --workspace -q
cargo test -p pinkdb-fts
cargo test -p pinkdb-cli --features fts
cargo test -p pinkdb-cli --features fts-regex
cargo test -p pinkdb-server --features fts
cargo test -p pinkdb-cli --features pinklake,s3,parquetlake
RUSTC=$(rustup which --toolchain stable rustc) ~/.cargo/bin/cargo +stable build -p pinkdb --target wasm32-unknown-unknown
```
