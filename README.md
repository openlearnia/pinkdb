# pinkdb — a mini DuckDB in Rust

An embedded **analytical** SQL database: in-memory columnar storage, vectorized
execution, SQL with aggregates and joins, CSV import. Ships as a library crate
(`pinkdb`), a CLI REPL (`pinkdb-cli`, a thin consumer of the library), and a
network server (`pinkdb-server`, a separate crate so the core stays lean).

```sql
.import sales.csv sales
SELECT region, AVG(total) FROM sales GROUP BY region ORDER BY 2 DESC;
```

```
┌─────────┬────────────┐
│ region  │ avg(total) │
│ VARCHAR │ DOUBLE     │
├─────────┼────────────┤
│ west    │ 175.0      │
│ east    │ 50.5       │
└─────────┴────────────┘
2 rows
Run Time (s): real 0.000
```

## Quick start

```sh
cargo run -p pinkdb-cli          # debug REPL
cargo build --release -p pinkdb-cli
```

Library use:

```rust
use pinkdb::PinkDb;

let mut db = PinkDb::new();
pinkdb::csv::import_csv_string(&mut db, "t", "a,b\n1,x\n2,y\n")?;
for result in db.execute("SELECT a, upper(b) FROM t WHERE a > 1;")? {
    println!("{result:?}");
}
```

## Architecture

```
SQL text
  → PEG parser → AST
  → Binder (resolve tables/columns, assign types)
  → Logical plan (scan → filter → join → aggregate → project → sort → limit)
  → Optimizer (predicate pushdown, constant folding)
  → Vectorized executor (operators exchange DataChunks of ≤2048 rows)
  → In-memory columnar storage
```

| Crate / module | Role |
|---|---|
| `pinkdb::types` | `DataType`, `Value`, `PinkDbError` |
| `pinkdb::vector` | `Vector` (typed data + validity bitmask, bit = valid) + `gather`, `DataChunk` |
| `pinkdb::storage` | `DataTable` (per-column `Vector` chunks), `PinkDb` catalog (databases → tables/views) |
| `pinkdb::csv` | CSV import + type inference (rayon-parallel parse) |
| `pinkdb::sql` | PEG grammar, AST, binder |
| `pinkdb::plan` | Logical plan tree |
| `pinkdb::optimizer` | Constant folding, predicate pushdown (incl. across joins) |
| `pinkdb::exec` | Vectorized expression eval, hash aggregate, hash join, operators (rayon-parallel) |
| `pinkdb::persist` | `.pink` single-file columnar persistence, v2 (databases + RLE/dictionary/bitpack; v1 files load) |
| `pinkdb::crypto` | optional at-rest encryption (`PINKENC1` container: AES-256-GCM + Argon2id) behind the `encryption` feature; a parse/identity stub otherwise |
| `pinkdb::db` | Multi-writer `Database`/`Connection` over `Arc<RwLock<PinkDb>>` |
| `pinkdb-cli` | rustyline REPL, dot-commands, box-drawn output |
| `pinkdb-server` | TCP server + blocking client (framed binary protocol, thread-per-connection); no external deps |
| `pink-lake` | minimal lake layer: JSON catalog + snapshots + immutable per-table Vortex data files, v1 `.pink` files still readable (opt-in CLI `lake` feature) |

Design rules: Rust edition 2021, zero async; the core is zero-unsafe, with
`unsafe` contained to exactly two crates (the C ABI `pinkdb-ffi` and the
extension loader `pinkdb-ext-loader`). Dependencies are `csv` + `rustyline`,
plus `rayon` for parallel execution and `peg` for the SQL parser. The server
crate adds no external dependencies; the extension loader adds `libloading`
(dlopen) and the core links it on native targets only (see Extensions below).
With the `encryption` feature the audited RustCrypto crates `aes-gcm` +
`argon2` are linked (optional dependencies: the default build stays
crypto-free).

SQL is parsed by a `peg`-generated grammar — the only parser path. A committed
corpus plus a seeded randomized fuzz test (`cargo test`) check that valid input
parses and invalid input returns a clean `Parse` error; parse-error messages are
positional.

## SQL surface (v1)

```sql
CREATE TABLE t (id BIGINT, name VARCHAR, price DOUBLE);
CREATE TABLE db1.t (...);            -- qualified: any db.table target works
INSERT INTO t VALUES (1, 'x', 9.5), (2, NULL, 3.0);
UPDATE t SET price = price * 1.1 WHERE id > 1;   -- SET exprs see pre-update values
DELETE FROM t WHERE price IS NULL;               -- NULL never matches; both are statement-atomic
BEGIN; UPDATE t SET price = 0; ROLLBACK;         -- flat snapshot transactions (in-memory only)
CREATE DATABASE db1;                 -- new, empty database
DROP DATABASE db1;                   -- last database cannot be dropped; dropping the
                                     -- current one switches the session to main
USE db1;                             -- per-session current database (default: main)
LOAD EXTENSION 'extensions/hello.dylib';   -- load a native extension (ABI v1)
SELECT [DISTINCT] expr [AS alias], ...
FROM t [JOIN u ON t.x = u.y]      -- INNER / LEFT [OUTER] / RIGHT [OUTER]
                                  -- equi-join (single equality)
  | FROM db1.t                    -- qualified tables resolve in that database
  | FROM (SELECT ...) AS s         -- derived tables (alias required)
  | WITH w AS (SELECT ...) ...     -- CTEs, incl. WITH w(a, b) AS column aliases
WHERE expr                        -- incl. IS [NOT] NULL, BETWEEN,
                                  -- IN (subquery|list), EXISTS (subquery)
GROUP BY ... HAVING ...
ORDER BY expr [ASC|DESC], ...     -- NULLS LAST both ways; ORDER BY 2 positional
LIMIT n;
-- Scalar subqueries `(SELECT ...)` anywhere an expression goes
-- (uncorrelated only: exactly 1 row × 1 col, 0 rows → NULL).
```

### Databases

Databases are named, case-insensitive namespaces (`main` by default). A
session (the embedded `PinkDb`, or each `Connection`) has a current database;
unqualified table names resolve there. `USE db` switches it and never affects
other sessions — `Database`/`Connection` carry their own current database, the
shared store is untouched. `db.table` qualifies any table reference in
`FROM`/`JOIN`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `DROP TABLE`,
`CREATE VIEW` and `DROP VIEW`; cross-database joins (`db1.t JOIN db2.u`) work.
Three-part column references (`db.table.col`) are not supported (clear parse
error). CTEs shadow unqualified names only; a qualified name never resolves to
a CTE. Views are session-local *and* per-database: created in the current
database and resolved there, with their body resolving unqualified names in
that same database. View bodies are self-contained: the caller's CTEs never
leak in, so a CTE named like a table in the body does not shadow that table.
`DROP DATABASE` refuses the last database and switches a
session that was using the dropped database to `main` (or the first remaining
database). `CREATE`/`DROP DATABASE` are refused inside a transaction.

- Aggregates: `COUNT(*)` / `COUNT(x)`, `SUM`, `AVG`, `MIN`, `MAX`
- Window functions: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, aggregates `OVER`
  (`PARTITION BY` / `ORDER BY` / `ROWS` frames; default frame is cumulative
  with ORDER BY, whole partition without; no `RANGE` frames, no nesting)
- Scalar functions: `abs`, `round` (1–2 args), `upper`, `lower`, `length`, `substr`
- `CAST(x AS type)` (numeric tower both ways, bool↔numeric, varchar parse/format,
  date↔timestamp; bad input errors), `COALESCE(a, b, ...)` (common type),
  `LIKE`/`NOT LIKE` (`%`/`_` patterns)
- `CASE WHEN c THEN v ... [ELSE e] END` + simple `CASE x WHEN v` form
  (NULL conditions never match; result types unify like COALESCE)
- Set ops: `UNION [ALL|DISTINCT]` (numeric types unify; ORDER BY/LIMIT after
  the set operation apply to the whole result — branch-level clauses need
  parentheses, unsupported in v1); `CREATE VIEW v [(cols)] AS ...` (session-local,
  rebound per use), `DROP TABLE` / `DROP VIEW`
- Types: `BOOLEAN`, `INTEGER` (i32), `BIGINT` (i64), `DOUBLE` (f64), `VARCHAR`,
  `DATE` (days since epoch), `TIMESTAMP` (microseconds, naive, no time zone)
- Literals: `DATE '2024-02-29'`, `TIMESTAMP '2024-03-15 12:30:45.5'`
- Date ops: `date ± integer` (days), `date - date` → BIGINT days;
  `year/month/day/hour/minute/second(x)`, `date_trunc(part, x)`
- CSV auto-detects `YYYY-MM-DD` → DATE and `YYYY-MM-DD HH:MM:SS[.f]` → TIMESTAMP
- Coercion: INTEGER → BIGINT → DOUBLE; `int / int` → DOUBLE (DuckDB rule)
- Three-valued logic: NULL fails `WHERE`; `COUNT(x)`/`SUM`/`AVG` skip NULLs;
  NULL group keys group together; NULL join keys never match; LEFT/RIGHT
  joins NULL-extend the unmatched side's columns
- Integer division/modulo by zero is an error; `0.0` float division is IEEE
- `SUM` over integers accumulates in i64 (wraps on overflow)

REPL dot-commands: `.help`, `.quit` / `.exit`, `.databases`, `.tables`,
`.schema <t|v>` (accepts `db.table`; views show their output columns),
`.import <file.csv> [table]` (table defaults to the file stem; `db.table`
targets another database), `.save <file.pink>`, `.open <file.pink>`.
`.databases` marks the current one and counts tables and views separately
(`main (current) (2 tables, 1 view)`). `.tables` and `.schema` scope to the
current database; `USE <db>` switches it. Double-quoted identifiers with
spaces/quotes work in dot-command arguments (`"My Db".t`). For `.import`, a
dotted target is `db.table` only when the left side names an existing
database — otherwise the whole string is the table name, so a file stem like
`data.2024.csv` imports as `data.2024`.

CSV inference (per column, over all non-empty cells): all i32 → INTEGER, all
i64 → BIGINT, all f64 → DOUBLE, all true/false (case-insensitive) → BOOLEAN,
else VARCHAR. Empty cell → NULL.

Types are inferred from the first 20 480 rows (DuckDB's sample size); if a
later row disagrees with the sampled schema, the import transparently reruns
with whole-file inference, so the final schema always matches the rule above.
Files containing no `"` and no `\r` bytes are parsed in parallel across the
rayon pool; anything else goes through the `csv` crate sequentially.

## Persistence

`db.save("x.pink")` writes every database and table in a single-file
columnar format (v2: `PINKDB02`); `PinkDb::open("x.pink")` loads it back
(`.save` / `.open` in the REPL). v1 files (`PINKDB01`, a flat table list)
still load: their tables become the single `main` database. Views are
session-local and never persisted. Round-trips are bit-exact (including
`-0.0` and NaN payloads); truncated or corrupt files return errors, never
panics.

Durable opens add a sidecar write-ahead log: `PinkDb::open_durable("x.pink")`
(the REPL's `.open`) loads the checkpoint, recovers an interrupted checkpoint,
replays `x.pink.wal`, then logs each committed transaction unit — an
autocommit statement individually, `BEGIN..COMMIT` as one record at `COMMIT`
(a `ROLLBACK` or conflict abort logs nothing). `USE`, `CREATE DATABASE` and
`DROP DATABASE` are logged like any other mutating statement: replay starts in
the checkpoint's session and the logged `USE` records switch the replay
session. Because the WAL is shared by every `Connection` while `USE` is
per-session, a record is made self-contained — when the replay session differs
from the statement's session (after a checkpoint, or with multiple connections
on different databases) a `USE <db>` statement is prepended to the record, so
replay always reproduces each statement in the database it ran in. A `USE`
prefix is never emitted for a database that no longer exists (the `DROP` is
already earlier in the WAL): the record is appended without it, and a
transaction whose write target or `USE` target was dropped since `BEGIN`
fails `COMMIT` with a write conflict instead of logging an unreplayable
record. `SyncPolicy::Sync` fsyncs every record; `Batch` fsyncs on
`flush_wal`/checkpoint.

The WAL format is `PINKWAL1` + version `2` + a generation number, and rolled
generations are named `<wal>.<gen>.roll`. A checkpoint writes `.pink.tmp`
(fsynced, directory fsynced), renames the live WAL to
`.wal.<gen>.roll` and starts a fresh WAL of generation `gen + 1`, publishes
`.pink.tmp` with an atomic rename, then removes the roll. Recovery never
removes anything before the recovered state is durable:

- `.tmp` present and the live WAL still has records: the live WAL is
  authoritative, `.tmp` (possibly a partial write) is discarded.
- `.tmp` present and the live WAL is empty/absent/torn: the checkpoint
  reached its WAL roll, which proves `.tmp` was fully written, so `.tmp` is
  *published* first and the rolls are removed afterwards.
- no `.tmp`: every roll is already folded into `.pink`; its generation (and
  the live WAL generation) must prove that, otherwise recovery refuses rather
  than delete uncertain records.

Recovery is idempotent, a crash inside recovery re-runs from the same files,
and a torn WAL header (a file no longer than one header) is recreated while
invalid magic/version on a file that carries records is a hard error. A
mid-file framing corruption (short record, bad CRC, or a non-continuous LSN)
stops replay at the first bad record; later committed records are dropped and
the discarded byte count is reported (`RecoveryInfo::truncated_bytes`). A
CRC-valid record that fails to parse, bind, or execute is a hard error —
committed data is never silently dropped.

Durability notes: `.save <bound path>` checkpoints; the target is compared by
filesystem identity, so `./x.pink`, absolute paths and case-only differences
compare equal. `.save <other>` is a plain snapshot and is refused when the
target has a sidecar WAL with committed records. `.import` is made durable by
a checkpoint before it returns (and is refused inside a transaction), so an
import survives a crash immediately after it acks. A mutating statement that
reads a session-local view is refused on durable databases: views are never
persisted, so replaying its logged SQL could not rebuild it. A database file
is single-writer: no file locking is taken, so run one process per `.pink`
path. Two processes on one durable database corrupt the WAL framing at best;
on an encrypted database they can also seal two records under one
`(key, nonce)` (both writers append under the same container prefix), which is
keystream reuse, not merely corruption. The keyed open rebuilds the WAL with a
fresh nonce prefix at open time, which shrinks but does not remove that race; a
lockfile is deliberately not taken (stale-lock handling would be fragile), so
the single-writer precondition stays documented and enforced by convention.

### Encryption (feature `encryption`)

At-rest encryption is opt-in: `cargo build --features encryption` links the
audited RustCrypto crates `aes-gcm` (AES-256-GCM) and `argon2` (Argon2id); the
default build stays crypto-free and unencrypted files are byte-identical.
`PinkDb::save_with_key` / `open_with_key` write and read `PINKENC1`
containers; `open_durable_with_key` (and the multi-writer
`Database::open_with_key`) apply the same key to the sidecar WAL, and a keyed
store's checkpoints and snapshots encrypt as well.

The container is a plaintext header (magic `PINKENC1`, version, cipher id,
KDF id + parameters + salt, a random per-container nonce prefix, and an AEAD
key-check block) followed by AEAD-sealed payloads: 1 MiB segments for a
checkpoint, one unit per WAL record (with the LSN bound into the AAD). The
nonce is `prefix || counter`, counters are unique per container. A keyed open
rebuilds the WAL with a fresh random prefix on **every** open (re-sealing the
intact records atomically), not only after a torn-tail truncation: a file cut
exactly at a record boundary — a `Batch`-mode tail lost at a boundary, or a
restored older backup — is byte-identical to a cleanly shorter WAL, so only a
fresh prefix guarantees the discarded record's `(key, nonce, AAD)` is never
handed out again. Checkpoint containers end with a terminal zero-length
segment, so a container cut at a segment boundary is rejected instead of
loading a silently shorter database. Encrypted WAL records always frame
`crc = 0` (GCM authenticates the payload); a nonzero crc on a record whose
ciphertext authenticates is refused as framing tampering. Keys are zeroed on
drop on a best-effort basis (safe writes only, no unsafe).

The key is a raw 32-byte key or a passphrase stretched with Argon2id (defaults
m=19 MiB, t=2, p=1; the salt and parameters live in the header, so raising the
defaults later keeps old files readable). Header parameters are validated on
read against a small multiple of the defaults (m ≤ 76 MiB, t ≤ 4, p ≤ 4, and
`m·t` ≤ 8× the default work), so a crafted header cannot make an open burn
minutes deriving (the worst accepted combination costs ≈ 0.11 s in release
here):

```rust
use pinkdb::{EncryptionKey, PinkDb};

let key = EncryptionKey::from_passphrase("correct horse battery staple");
db.save_with_key("x.pink", &key)?;                       // encrypted snapshot
let db = PinkDb::open_with_key("x.pink", &key)?;         // decrypts
let db = PinkDb::open_durable_with_key("x.pink", &key)?; // encrypted WAL
```

The REPL reads `PINKDB_KEY` from the environment: `.save` writes encrypted and
`.open` decrypts (the durable WAL uses the same key); unset, behavior is
byte-identical to an unkeyed build. The value must be valid UTF-8: a
set-but-non-UTF-8 variable is a hard error (exit 2) instead of a silent
fallback to plaintext, and remote mode (`--host`) warns that the variable is
inert (the server's own configuration applies; `pinkdb-server` has no key
option, so its `--file` must be a plaintext database). A wrong key (or a
tampered header) fails the key-check block with a clear `wrong key` error,
never "corrupt file". A tampered payload byte fails AEAD authentication: for a
`.pink` checkpoint that is a hard error (loads are all-or-nothing), and for the
WAL a complete record that fails authentication with more records after it is
a hard error too — at the very tail it is indistinguishable from a torn write,
so the wave-22 torn-tail policy applies and the tail is truncated. Encrypted
and plaintext artifacts never mix: a keyed open refuses a plaintext
checkpoint/WAL and a plaintext open refuses an encrypted one, so a key can
never be silently ignored; without the feature, opening an encrypted file
reports `file is encrypted; build with the encryption feature`. A live store's
mode and key are fixed at open: changing `set_encryption_key` afterwards is
refused at the next `checkpoint()` (`cannot change encryption mode on a live
store; reopen it instead`), and `save_snapshot` refuses a target `.pink` or
sidecar WAL in the other mode or under a different key. Nothing but the
header is readable in the clear — payloads (rows, SQL text) are always
ciphertext, covered by tests that grep the files for a known marker string.
Release `pinkdb` binary: 2.21 MB default, 2.28 MB with `--features encryption`
(Apple Silicon, thin LTO; the default build grows ~18 KB over the
pre-encryption 2.19 MB — most of it segment page rounding, ~4.6 KB of code).

Each ≤2048-row column chunk picks its smallest encoding: integers choose
between raw, RLE, and bitpacking (min + width + packed bits); doubles raw or
RLE; booleans bitpacked or RLE; strings raw, dictionary (u32 codes), or RLE.
NULLs ride in a separate validity bitmap. 1M-row sales table: 24.5 MB CSV →
14.0 MB `.pink`, saved in 0.16 s, reopened and queried in 0.25 s.

## Concurrency

```rust
use pinkdb::{Database, PinkDb};

// One shared store, one connection per thread.
let db = Database::new();
let mut c = db.connect();
c.execute("CREATE TABLE t (a INTEGER);")?;
```

Queries run under a shared read lock (parallel readers, parallel chunks via
rayon inside each query). Writes take the write lock per statement.
`BEGIN..COMMIT` works on a private snapshot: commits to disjoint tables all
succeed; overlapping writes abort the second committer with a write-conflict
error (retry the transaction) — never silent lost updates. This is snapshot
isolation without predicate tracking (write-skew is possible). The plain
`PinkDb` API remains for single-connection embedding.

## Lake layer (feature `lake`)

`pink-lake` is the minimal lake: **metadata and data storage decoupled** — a
JSON catalog plus immutable per-table data files, local disk first. Wave 35
makes **Vortex the default data format**; wave 36 adds S3 storage.

A lake is a directory:

```text
mylake/
├── catalog.json               current state (authoritative "latest")
├── snapshots/<version>.json   immutable manifest per published version
├── data/<table>/<uuid>.vortex immutable per-table Vortex files (v2, default)
└── data/<table>/<uuid>.pink   v1 data files (engine persistence bytes, still read)
```

The manifests (`catalog.json` and every snapshot) share one shape:

```json
{
  "format_version": 2,
  "updated": "2026-10-14T02:11:50.760Z",
  "version": 2,
  "tables": {
    "analytics.u": { "files": ["data/analytics.u/e9a4….vortex"], "rows": 1, "format": "vortex" },
    "main.t":      { "files": ["data/main.t/831a….vortex"], "rows": 2, "format": "vortex" }
  }
}
```

**Format history and defaults:**

| catalog `format_version` | data file | written by | read by this build |
|---|---|---|---|
| 1 | `data/<table>/<uuid>.pink` — the engine's single-table persistence bytes | wave 34 | yes (entries have no `format` field; `.pink` opens standalone with `PinkDb::open`) |
| 2 | `data/<table>/<uuid>.vortex` — one Vortex file per table (one `files` entry), `"format": "vortex"` on every entry | this build (default) | yes |

**Migration:** open an old lake and `.lake save` again — the next snapshot is a
full rewrite in Vortex (`format_version` 2); old snapshots and their `.pink`
files stay untouched, so every published version still loads. Any unknown
`format_version`, a v2 entry without a `format` field, and a v2 entry
declaring anything but `"vortex"` (the v2 writer emits Vortex only) are clear
errors, never a guess.

- **Schema and typing live in the data files.** A v2 file is a plain Vortex
  file: one struct array, one field per column, chunk boundaries preserved
  (row blocks of exactly `CHUNK_SIZE` rows, one stream element per engine
  chunk), so a loaded table re-saves to the same `.pink` bytes. DOUBLES keep
  raw IEEE-754 bits (NaN payloads, `-0.0`), `i32`/`i64` cover their full
  ranges, VARCHAR is `utf8`. DATE/TIMESTAMP are stored as their raw `i32`/`i64`
  storage integers plus the exact pinkdb schema in a `pinkdb.table` metadata
  segment (with the table's `db`/`table` identity, which is what rejects a
  data file swapped in from a *differently named* table): the native
  `vortex.timestamp` extension converts values
  through a calendar type limited to about ±20 000 years and **panics** on
  extreme timestamps (±6.31e17 µs) during statistics computation, so no bit
  pattern is left to chance — see below.
- **Every save is a full snapshot.** `write_from_store` writes every table of
  every database to a fresh data file, publishes `snapshots/<version>.json`
  (`version = previous + 1`, via `checked_add` and capped at `MAX_VERSION`,
  so a tampered `version: u64::MAX` or a lake at the cap is a clear error,
  never a wrap), then replaces `catalog.json` with the same bytes. Old
  versions and their data files are never modified — and a snapshot file that
  already exists is never replaced: `save` that would land on a published
  version (e.g. after restoring an older `catalog.json`) fails with a
  conflict instead of clobbering it. `load(Some(v))` restores any published
  version, `load(None)` the latest. The writer emits exactly one data file
  per table per version — a very large table still becomes one file
  (multi-file entries load fine; they are reserved for future writers). There
  is no appending, compaction or GC — each save costs one full rewrite, and
  old versions' files stay.
- **Atomicity order: data files → snapshot → catalog.** Every write goes to a
  *unique* temp sibling (`<name>.<uuid>.tmp`, created with `create_new(true)`
  so a planted symlink or an older writer's temp file is never followed or
  truncated), is fsynced, then renamed into place (catalog, data files) or
  **hard-linked** into place (snapshots: an existing version can never be
  replaced). The parent directory is fsynced afterwards, best-effort. This is
  **process-crash safety**: after a `kill -9` the previous version always
  loads, and stray data files, `.tmp` files and snapshots above the catalog's
  version are ignored (`versions` lists only manifests that parse and declare
  their own canonical version). It is *not* a power-loss guarantee on APFS:
  `fsync(2)`/`File::sync_all` can return before the drive flushes its cache;
  only `F_FULLFSYNC` would, and this crate deliberately adds no
  unsafe/libc dependency for it (a future opt-in mode could). One leftover is
  visible: a stray manifest at exactly the *next* version can't be told apart
  from a published one, so a save landing on it refuses with a clear conflict
  ("if catalog.json still points at the previous version it was never
  published — remove the stray snapshot and retry"). Corrupt metadata is a
  clear error; a truncated/corrupt Vortex data file, or one whose
  `pinkdb.table` metadata does not parse, is the same `Corrupt` error. Panics
  inside Vortex's decoder for malformed layouts (there are several, including
  `debug_assert!` sites) are caught — the whole decode boundary (`open_buffer`,
  metadata/schema checks, scan, per-column decode) runs under
  `catch_unwind` — and surfaced as a `Corrupt` error naming the data file, so
  a decoder panic is an error, not a process death. The default panic hook may
  still print one line to stderr before the caught error; the library does not
  swap the global hook (that would race other threads).
- **Flat `db.table` names**, restored by splitting on the first dot; a table
  name may contain dots, a database name containing `.` is refused at save
  time. Views are session-local and are not saved. A lake records tables,
  so a database with no tables is not preserved and a dropped `main` comes
  back empty (loading always recreates it). Data files are plaintext
  (`PINKDB_KEY` does not encrypt a lake; the CLI keeps the session key
  across `.lake open` for later `.save`/`.open`). Data files carry no
  checksums: mutating one in place is undetected by design. The checks that
  do run on load are structural — a v2 file's `pinkdb.table` metadata must
  name the catalog's table, agree with the Vortex field names/types, and the
  file's row count must match the catalog — so a file swapped in from a
  *differently named* table is rejected. Swapping two files of the **same**
  table (same name, same schema, same row count) cannot be detected, and a
  tampered file may still decode to different values or fail with a clean
  `Corrupt` error (caught decoder panics included).
  `save_store`/`.lake save` pre-validate the store (dotted database names,
  over-long encoded table names) *before* creating a lake, so a rejected save
  leaves nothing behind.
- **One live writer handle per lake directory** (no file locking; concurrent
  readers of published versions are fine). A handle that saves after another
  handle/process moved the catalog fails with `LakeError::Conflict`
  ("another writer published version N; reopen the lake") *before* replacing
  the catalog, and two writers racing for one version cannot both win: the
  snapshot link is atomic, so exactly one publishes it, and `catalog.json` is
  byte-identical to the snapshot of its version. All I/O goes through a
  private `LocalStorage` handle; a `Storage` trait is deliberately deferred
  until wave 36 needs a second (S3) implementation.
- **Measured on this machine (2026-10, release, Apple Silicon, 1M-row
  `bench/sales_1m.csv`: id/region/total/active):** one Vortex data file is
  3,117,738 B vs 14,042,574 B for the equivalent `.pink` bytes (0.22×, ≈4.5×
  smaller); a lake save takes 0.20 s vs 0.09 s (Vortex compresses on write)
  and a load 0.07 s vs 0.11 s. Reproduce with
  `cargo test --release -p pink-lake --test bench_1m -- --ignored --nocapture`.
  **Dependency/build note:** `crates/pink-lake` now depends on the `vortex`
  crate directly (`default-features = false, features = ["files"]`); that
  pulls in ~218 additional crates (vortex-array/file/layout + encodings,
  arrow, prost/flatbuffers, smol/async, jiff) and adds ≈42 MB to the release
  `pinkdb` binary built with `--features lake`. A cold build is
  machine-dependent (minutes for the debug and release trees on Apple
  Silicon; the exact numbers were not reproduced precisely). Note that
  `pink-lake` is a workspace member, so root workspace commands
  (`cargo build`/`cargo test` at the repo root) compile the Vortex tree as
  well — only the feature-less `pinkdb-cli` binary itself is Vortex-free and
  keeps its size (the default release `pinkdb` is unchanged).

Library:

```rust
use pink_lake::{Lake, save_store};
use pinkdb::PinkDb;

let mut db = PinkDb::new();
db.execute("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2);")?;

let version = save_store(&db, "mylake")?;  // creates the lake, publishes v2
let mut lake = Lake::open("mylake")?;      // v1 is the create-time empty catalog
let v3 = lake.write_from_store(&db)?;      // full snapshot in Vortex, version 3
let tables = lake.tables()?;               // [("main.t", 2)]
let versions = lake.versions()?;           // [1, 2, 3]
let latest = lake.load(None)?;             // rebuild the latest
let old = lake.load(Some(2))?;             // rebuild a specific snapshot
```

REPL (opt-in `lake` feature — the default binary stays lean):

```sh
cargo build -p pinkdb-cli --features lake
```

```text
.lake save <dir>          snapshot the store (version, tables, rows, bytes)
.lake open <dir> [ver]    replace the in-memory store with the lake
                          (session database resets to main; PINKDB_KEY is kept)
.lake versions <dir>      published versions (current marked)
.lake tables <dir>        current version's db.table names + row counts
.lake info <dir>          format version, current version, snapshots, tables
```

`.lake tables`/`.lake info` also warn about tables whose referenced data file
is missing, instead of advertising data a load cannot read. Without the
feature, `.lake` reports `this pinkdb binary was built without the lake
feature`. Release `pinkdb`: 2,289,616 B default (**unchanged**) vs 44,639,248 B
with `--features lake` (38,171,440 B stripped) — the Vortex array encodings
dominate; see the dependency/build note above.

## Network server

`pinkdb-server` is a separate workspace crate (packaging: the core stays
lean): a TCP server plus a blocking client for a hand-rolled binary protocol.
No external dependencies, zero async (one std thread per connection), no
unsafe. Release binaries: `pinkdb` 2.21 MB (2.28 MB with `--features
encryption`), `pinkdb-server` 1.85 MB (Apple Silicon, `--release`, thin LTO).

```sh
# in-memory server on 127.0.0.1:5433
cargo run --release -p pinkdb-server

# durable server: same checkpoint + WAL guarantees as the embedded path
cargo run --release -p pinkdb-server -- --file sales.pink

# remote REPL: build the CLI with the optional `client` feature, then
cargo build --release -p pinkdb-cli --features client
./target/release/pinkdb --host 127.0.0.1:5433
```

`pinkdb-server --help` lists the flags: `--host` (default `127.0.0.1`),
`--port` (default `5433`; `0` picks an ephemeral port), and `--file` (durable
`.pink`: the checkpoint loads and the sidecar WAL replays on restart;
in-memory when omitted). The server prints its listening address on startup
and runs until terminated.

Server robustness, in one place:

- **16 MiB stack per connection thread.** The default 2 MiB thread stack sits
  too close to the engine's worst-case recursion; connection threads ask for
  16 MiB and the engine's depth guards (below) reject hostile statements
  before they can recurse.
- **Transient accept errors never stop the server.** `EMFILE`/`ENFILE`/`ECONNABORTED`/
  `ENOBUFS`/... are logged and retried with a 10 ms→1 s backoff, so an fd
  spike no longer kills the process (`ulimit -n` pressure, many short-lived
  clients). Only the nonblocking poll and `EINTR` follow the fast path.
- **128 concurrent connections** (`DEFAULT_MAX_CONNECTIONS`). Over the cap the
  server sends a best-effort `Error` frame and closes, bounding fds/threads.
- **Panics are isolated.** A panic while executing one statement is caught,
  logged, and reported as an `Error` frame; the connection and every other
  client stay alive. A panic while the store's write lock is held poisons it;
  the engine logs once and recovers via `PoisonError::into_inner()`, so one
  bad statement cannot permanently DoS the database (the state may be
  inconsistent — recovery is loud).
- **No read/write timeouts.** A slow or stalled client holds one thread and
  one fd until it disconnects; a client that stops reading a large result
  blocks only its own connection thread (the accept loop is never blocked).
  The connection cap bounds that pressure.
- **Depth guards.** Statements nested deeper than 24 units (parens, `CASE`,
  prefix unary) or with an AST deeper than 64 nodes (left-associative
  operator chains, `IN` lists, `AND`/`OR` runs, nested subqueries) are
  rejected with a clean `Parse` error before bind/execute/drop can recurse.

### Protocol

One TCP connection is one session. Every message is a frame: `u32` little-
endian length prefix + `u8` message type + body. Frames larger than 64 MiB
are rejected before allocation, and every inner length (strings, columns,
rows, values) is validated against the bytes remaining in the frame — a
malformed or truncated peer can never make the decoder read out of bounds or
over-allocate.

Requests: `Hello { protocol_version }` (required first; a mismatched version
is rejected with a clear error), `Execute { sql }`, `Ping`. Responses:
`HelloOk { server_version, protocol_version }`, `Pong`, and per statement one
result group — `ResultSet { names, types }`, then chunked `Rows { count,
columns, values... }` frames (typed value encoding: a tag byte + payload for
NULL/BOOL/I32/I64/F64/VARCHAR/DATE/TIMESTAMP; `DataType` tag byte), then
`Done { kind }`. A successful `Execute` batch ends with exactly one
`BatchDone { statements }` frame and the client reads result groups until it,
so neither side parses SQL just to count statements. A failing statement ends
the batch with a single `Error { message }` instead; SQL errors arrive as
`Error` frames and the connection stays open. The protocol version is 3
(v3 added the `Done` kind `LoadedExtension` for `LOAD EXTENSION`).

The client (`pinkdb_server::Client`) reconstructs pinkdb's public
`QueryResult`/`Value`/`DataType`, so the CLI renders remote results with the
exact same renderer as local ones. Each connection gets its own
`Connection`, so wave-23 sessions (`USE`, transactions) are isolated per TCP
client. After a decode or transport error the client marks itself broken and
fails fast on later calls instead of reading a desynchronized stream.

### Remote CLI mode

`pinkdb --host HOST:PORT` sends every SQL statement to the server and renders
results with the same box-drawn output and timing line as the embedded REPL.
SQL (including `USE`, `CREATE DATABASE`, `BEGIN`/`COMMIT`) works unchanged.
The file and catalog dot-commands (`.import`, `.save`, `.open`, `.databases`,
`.tables`, `.schema`) are not available in remote mode and report
`Error: <cmd> is not available in remote mode`. The feature is opt-in: the
default build rejects `--host` with "built without the client feature".
`PINKDB_KEY` is inert in remote mode (the server's own at-rest configuration
applies, and `pinkdb-server` currently has no key option), so a set value is
reported as a warning instead of being silently ignored.

### Security posture

**No authentication, no TLS, no authorization in this wave.** Anyone who can
open a TCP connection to the port gets the full SQL surface of the served
database (queries, writes, DDL) on the one shared store. The server binds
`127.0.0.1` by default and must stay on localhost or a trusted network: do
not expose it to the internet or to untrusted clients. Auth and TLS are
planned; they are explicitly out of scope for this wave.

## C ABI (`pinkdb-ffi`)

The `pinkdb-ffi` crate exposes the engine over a small C ABI as
`libpinkdb_ffi.dylib` / `libpinkdb_ffi.a` for foreign bindings (Python `cffi`,
Go `cgo`, ...). It is the only crate in the repo that contains `unsafe`; the
core stays zero-unsafe.

```sh
cargo build --release -p pinkdb-ffi
make -C crates/pinkdb-ffi hello   # compiles and runs the C example
```

- Header: `crates/pinkdb-ffi/include/pinkdb.h` (ABI v1; `pinkdb_abi_version()`).
- Handles (`pinkdb_open` for durable files, `pinkdb_open_memory`) are not
  thread-safe — use one per thread. Errors are thread-local
  (`pinkdb_last_error`).
- `pinkdb_execute(handle, sql, len)` takes UTF-8 bytes (embedded NULs allowed)
  and returns a result that round-trips every statement kind and value type:
  typed `pink_value` by value, with VARCHAR text borrowed from the result.
- Ownership: free results with `pinkdb_result_free`, handles with
  `pinkdb_close`; the full contract (pointers, threading, text lifetimes) is in
  the header.

Artifacts: `libpinkdb_ffi.dylib` ≈ 1.70 MB, `libpinkdb_ffi.a` ≈ 15.5 MB (build
archive); nothing is added to the `pinkdb` binary.

## Extensions (loadable)

`pinkdb` can load shared libraries that register scalar SQL functions at
runtime — no recompilation of the engine. This is the repo's second versioned
C ABI: it is **separate** from the `pinkdb-ffi` embedding ABI above (which is
host→foreign-binding); an extension is a foreign library→host callback ABI.

- Header: `crates/pinkdb-ext-loader/include/pinkdb_extension.h` (ABI v1;
  plain C99, no pinkdb libraries needed to build an extension).
- Loader crate: `pinkdb-ext-loader` (owns every `unsafe` here: `dlopen`,
  symbol lookup, callback invocation, result-text ownership).
- Engine API: `PinkDb::load_extension(path) -> name`; the SQL statement is
  `LOAD EXTENSION '<path>'` (string-literal path). Loaded functions resolve
  case-insensitively like built-ins and cannot shadow one.

A minimal extension registering one `hello_add(BIGINT, BIGINT) -> BIGINT`:

```c
/* hello_ext.c */
#include "pinkdb_extension.h"

static int32_t hello_add(void *user_data, const pinkdb_ext_value *args,
                         size_t n_args, pinkdb_ext_value *out) {
    (void)user_data; (void)n_args;
    if (args[0].is_null || args[1].is_null) {   /* NULL in → NULL out */
        out->is_null = 1;
        return 0;
    }
    out->type_tag = PINKDB_EXT_TYPE_BIGINT;
    out->is_null = 0;
    out->int64 = args[0].int64 + args[1].int64;
    return 0;
}

int32_t pinkdb_extension_init(const PinkdbHostApi *api, void *host_ctx) {
    static const int32_t arg_types[2] = {
        PINKDB_EXT_TYPE_BIGINT, PINKDB_EXT_TYPE_BIGINT
    };
    return api->register_scalar(host_ctx, "hello_add", 2, arg_types,
                                PINKDB_EXT_TYPE_BIGINT, hello_add, NULL);
}

uint32_t    pinkdb_extension_abi_version(void) { return PINKDB_EXT_ABI_VERSION; }
const char *pinkdb_extension_name(void)        { return "hello_ext"; }
```

```sh
cc -std=c99 -O2 -fPIC -shared -I crates/pinkdb-ext-loader/include \
   -o hello_ext.dylib hello_ext.c          # .so on Linux, .dll with -shared on Windows
./target/release/pinkdb
```

```sql
LOAD EXTENSION 'hello_ext.dylib';
-- Extension hello_ext loaded.
SELECT hello_add(40, 2);                   -- 42
```

Contract highlights (the header is complete):

- **Thread safety:** callbacks run concurrently on the executor's rayon
  workers — a registered function may be invoked on several threads at once,
  so its state must be guarded (atomics/locks/TLS). `alloc`/`free` are
  thread-safe; `register_scalar` is only valid inside `pinkdb_extension_init`.
- **Typing:** ABI v1 is strictly typed. Arguments must match the registered
  types exactly (`NULL` literals are typeless and pass through as NULL; write
  `CAST(...)` for a different numeric type) and the result must carry the
  registered type tag.
- **Result text:** a non-NULL VARCHAR result must be allocated with
  `api->alloc(size)`; the host copies `text_len` bytes and frees it with
  `api->free` immediately after the call — on every path, including error
  returns and NULL results. A rejected registration (including a NULL `fn`)
  fails the whole load atomically — nothing half-registers.
- **Errors:** returning non-zero aborts the enclosing statement with an error
  naming the function and code; `init` returning non-zero (or a rejected
  registration) fails the load naming the code/reason. `init` and callbacks
  must not panic/unwind across the ABI boundary.

### Writing extensions (C/C++)

The in-repo C/C++ SDK (`ext-sdk/c/`) bundles the vendored ABI header, a
commented skeleton (`ext-sdk/c/template/`), three worked examples (C numeric,
C string, C++), a Makefile, and an end-to-end smoke test:

```sh
make -C ext-sdk/c        # builds the examples into ext-sdk/c/build/
./ext-sdk/c/smoke.sh     # builds them, loads each via the release CLI, greps the results
```

`ext-sdk/c/README.md` walks through the contract (exports, the host API
struct, value tags, VARCHAR `alloc`/`free` ownership, NULL semantics,
thread-safety, no unwinding) and the `LOAD EXTENSION '<path>'` workflow. The
SDK's header is kept byte-identical to the loader crate's copy by a
`cargo test` check.

A Go SDK (`ext-sdk/go/`) builds the same ABI v1 with cgo
(`go build -buildmode=c-shared`): the three exports come from `//export`
directives, and small static C shims stand in for the host's function
pointers (`register_scalar`/`alloc`/`free`), which Go cannot call directly.
`ext-sdk/go/README.md` documents the cgo patterns and the sharp edges — the
Go runtime is linked into the host process, callbacks are invoked from
foreign rayon worker threads, VARCHAR results must be copied into
host-`alloc` memory, panics must be recovered before they cross the ABI
boundary, and `-buildmode=c-shared` symbol-export rules — and
`./ext-sdk/go/smoke.sh` builds and exercises the example end to end. Its
header copy is pinned by the same `cargo test` check.

### Extension repository

Built extensions are published to a hosted catalog (Cloudflare Worker + R2,
format v1) at <https://pinkdb-ext-repo.defaultcms.workers.dev>: `GET
/index.json` is the catalog and `GET /artifacts/…` serves the bytes. The
`ext-repo/` directory holds the format spec (`ext-repo/FORMAT.md`), the
publisher (`publish.sh`/`publish.py`), the verifier (`verify.sh`), and the
worker source. The first published artifact is the C SDK example `math_ext`
0.1.0 (`darwin-arm64`).

#### Installing extensions (`.install`)

The CLI can install extensions from repositories. The HTTP/TLS/JSON/SHA-256
client is an opt-in feature so the default binary stays lean:

```sh
cargo build -p pinkdb-cli --features repo     # the `repo` feature
```

Then, inside the REPL:

```text
pinkdb> .install math_ext
Installed math_ext 0.1.0 (darwin-arm64)
  repository: https://pinkdb-ext-repo.defaultcms.workers.dev
  path:       /Users/you/.pinkdb/extensions/math_ext/0.1.0/darwin-arm64/math_ext.dylib
  sha256:     38abc8b345466e0710bf9c2497a99b0b5c0a3e906874cae1c3323ee3e754b9b2
Extension math_ext loaded.
pinkdb> SELECT ext_clamp(CAST(42 AS BIGINT), CAST(0 AS BIGINT), CAST(10 AS BIGINT));
```

- **Config:** `$PINKDB_HOME/repositories.json` (default
  `~/.pinkdb/repositories.json`) is a JSON object with an ordered list,
  `{"repositories": ["https://…"]}`. When the file is absent the official
  repository above is the default; the first mutation materializes it.
  `.repositories list` / `.repositories add <url>` /
  `.repositories remove <url>` manage it: URLs are validated and
  canonicalized (trailing slashes stripped, `http://` only for local/testing),
  duplicates are refused, order is preserved, and an empty list is allowed
  (`.install` then says what to add). Writes are atomic (temp + rename).
- **Resolution:** repositories are tried in config order and the first one
  with a match wins. `.install <name>` picks the highest semver-ish version
  for this platform — derived at runtime from the OS and architecture, e.g.
  `darwin-arm64` — with ABI v1; `.install <name>@<version>` is exact.
- **Cache + integrity:** artifacts land under
  `$PINKDB_HOME/extensions/<name>/<version>/<platform>/<file>`, are written
  atomically (temp file + rename), and are verified against the catalog's
  SHA-256 before the rename; a mismatch is refused and leaves nothing behind.
  Installing the same coordinates again reuses the verified copy (no
  re-download). `.installed` lists the cache.
- **Load:** `.install` finishes by `LOAD EXTENSION`-ing the artifact into the
  current session; the extension then behaves exactly as described below
  (process-level, WAL replay re-loads it by path).

Semantics and persistence:

- Extensions are **process-level**: they are not stored in `.pink` snapshots
  and not part of a transaction. `LOAD EXTENSION` is refused inside
  `BEGIN..COMMIT` (`LOAD EXTENSION is not allowed inside a transaction`), like
  `CREATE DATABASE`. An accepted extension stays loaded (its library stays
  mapped) for the lifetime of the store — even one that registers no functions
  — and its declared name is pinned: loading it again is a clear
  `extension "x" is already loaded` error (a failed load registers nothing).
- For a **durable** database the statement *is* WAL-logged, and every logged
  record that *calls* an extension function is prefixed with
  `LOAD EXTENSION '<path>'` unless the current WAL generation already loaded
  that path (touched-only prefixing; the logged `LOAD EXTENSION` itself is
  never prefixed with itself). Replay therefore re-loads exactly the
  extensions the replayed records need. A missing/unloadable file at replay
  time is a hard recovery error naming the path (never a silent drop); prefer
  absolute paths, since the recorded path is re-resolved against the
  process's working directory at reopen.
- A **checkpoint** rolls the WAL and never writes an extension into the
  `.pink` (extensions are process-level). An open after a checkpoint starts
  with no extension loaded; the first record that touches one re-loads it by
  path, so its file must still exist. An extension loaded before the last
  checkpoint is gone after a reopen until it is loaded again.
- **Remote** use (`pinkdb-server`): `LOAD EXTENSION` loads native code into
  the *server* process, not the client. The server trusts its clients, so
  treat remote access as a trusted-clients posture — anyone who can send SQL
  can load a shared library with the server's privileges.
- **ABI policy:** the extension ABI version bumps only for breaking changes.
  New result/statement kinds (e.g. `LOAD EXTENSION` itself, which added
  `PINKDB_STMT_LOADED_EXTENSION = 14` to the `pinkdb-ffi` embedding ABI and a
  `loaded_extension` kind to the Python/Go bindings) are additive and keep
  ABI v1 — existing extensions and embedders keep working unchanged.
- wasm32 builds do not link the loader: `LOAD EXTENSION` fails cleanly with
  `extensions are not supported on wasm targets`.

```sh
cargo test -p pinkdb-ext-loader   # compiles the C test extensions with cc
```

## Python binding (`bindings/python`)

A thin, dependency-free wrapper over the C ABI, implemented with the standard
library `ctypes` module (no `cffi`, no pip installs, no build step for the
package itself — the roadmap's `cffi` wording is a deliberate deviation here;
the binding model is the same). Build the shared library first:

```sh
cargo build --release -p pinkdb-ffi
```

```python
import pinkdb

with pinkdb.connect_memory() as db:            # or pinkdb.connect("data.pink")
    db.execute("CREATE TABLE t (x INTEGER, s VARCHAR);")
    db.execute("INSERT INTO t VALUES (1, 'héllo'), (NULL, NULL);")
    result = db.execute("SELECT x, s FROM t ORDER BY x;")[0]
    print(result.kind, result.rows)  # selected [[1, 'héllo'], [None, None]]
```

`Database.execute(sql)` returns one `Result` per `;`-separated statement:

- `kind`: `selected`, `created`, `inserted`, `updated`, `deleted`, `began`,
  `committed`, `rolledback`, `created_view`, `dropped_table`, `dropped_view`,
  `created_database`, `dropped_database`, `used`, `loaded_extension`;
- `columns` / `column_types` (SELECT only; types `boolean`, `integer`,
  `bigint`, `double`, `varchar`, `date`, `timestamp`), `rows`;
- `affected` (INSERT/UPDATE/DELETE) and `name` (view/table/database/`USE`/
  extension).

Values map to `None`/`bool`/`int`/`float`/`str`/`datetime.date`/naive-UTC
`datetime.datetime`; errors raise `pinkdb.Error` with the message from
`pinkdb_last_error` and leave the handle usable. The library is located via
`PINKDB_FFI_LIB` (full dylib/so path) or, by default, the repo-relative
`target/release/libpinkdb_ffi.{dylib,so}`.

```sh
python3 -m unittest discover -s bindings/python/tests -v
PINKDB_FFI_LIB="$PWD/target/release/libpinkdb_ffi.dylib" \
    python3 -m unittest discover -s bindings/python/tests -v   # env override
```

## Go binding (`bindings/go`)

A cgo wrapper over the same C ABI v1 (module
`github.com/openlearnia/pinkdb/bindings/go`). Build the shared library first:

```sh
cargo build --release -p pinkdb-ffi
```

```go
import pinkdb "github.com/openlearnia/pinkdb/bindings/go"

db, err := pinkdb.OpenMemory() // or pinkdb.Open("data.pink")
if err != nil {
	panic(err)
}
defer db.Close()
results, err := db.Execute(
	"CREATE TABLE t (x INTEGER, s VARCHAR);" +
		"INSERT INTO t VALUES (1, 'héllo'), (NULL, NULL);" +
		"SELECT x, s FROM t ORDER BY x;")
if err != nil {
	panic(err)
}
fmt.Println(results[2].Kind, results[2].Rows) // selected [[1 héllo] [<nil> <nil>]]
```

`Execute` returns materialized `[]Result`s (`Kind`, `Columns`, `ColumnTypes`,
`Rows`, `Affected`, `Name`); values map to `nil`/`bool`/`int64`/`float64`/
`string`/UTC `time.Time`, and errors carry the native `pinkdb_last_error`
message. All native calls are serialized behind a per-`DB` mutex (the C
handle itself is single-threaded), `Close` is idempotent, and use after close
returns `ErrClosed`. See `bindings/go/README.md` for the value-mapping table
and linking notes.

```sh
cd bindings/go && go test ./...
```

## Benchmark (1M rows, release, Apple Silicon)

Generated with `bench/gen_sales.py` (deterministic seed 42), measured with `cargo run --release -p pinkdb --example query_bench` (best of 5):

For the join rows, "before" is the pre-wave-19 baseline (join_self 66, join_group 68, join_probe 64); the materializing mixed-predicate join is new, so it has no comparable baseline ("—").

| Query | Now (ms) | Before (ms) |
|---|---|---|
| `.import` 1M rows (24 MB CSV) | 25 | 28 |
| `SELECT COUNT(*) FROM sales` | 0.01 | 0.02 |
| `SELECT COUNT(*) ... WHERE total > 500` | 5.1 | 5.8 |
| `SELECT COUNT(*) ... WHERE region = 'west'` | 4.1 | 27 |
| `SELECT upper(region), length(region)` (1M rows) | 15 | 35 |
| GROUP BY region + 5 aggregates | 7.7 | 9 |
| GROUP BY id (1M groups) | 66 | 141 |
| ORDER BY total DESC LIMIT 10 | 3.0 | 100 |
| ORDER BY region (full sort) | 40 | 104 |
| DISTINCT region, active | 22 | 22 |
| DISTINCT id (1M values) | 64 | 75 |
| ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) | 93 | 165 |
| COUNT(*) OVER (PARTITION BY region) | 79 | minutes (O(n²)) |
| 1M-row self hash join + COUNT (fused) | 9.0 | 66 |
| 1M-row self join + GROUP BY region (fused) | 16 | 68 |
| 1M-row self join, mixed predicate (materializing) | 26 | — |
| 1M probes vs 4-row string-key side (fused) | 24 | 64 |

For reference, DuckDB 1.5.6 imports the same file in 0.06 s on this machine
(`read_csv_auto` → table), single connection.

### Lake format comparison (Vortex vs DuckLake)

`bench/lake_bench.py` (stdlib-only Python, one file) runs the same 1M-row CSV
through both lake formats and the same four queries, and prints the raw
numbers as JSON:

```sh
cargo build --release -p pinkdb-cli --features lake
python3 bench/lake_bench.py --rows 1000000 --runs 5
```

Both arms store the table as `lake.sales`, so the SQL text is identical
(pink-lake is told to import into a database named `lake`; DuckLake attaches
a catalog named `lake`). Measured 2026-10-14 on this machine (macOS 26.6.2,
Apple Silicon, 10 CPUs, load average ≈3 during the run; `pinkdb`
44,567,680 B with `--features lake`; `duckdb` v1.5.5 with the `ducklake`
extension already cached, `INSTALL` ≈1 ms):

| Metric (1M rows) | pink-lake (Vortex) | DuckLake (DuckDB) |
|---|---|---|
| ingest wall (s) | 0.243 (import 0.031 + lake save 0.174) | 0.125 (CTAS 0.091) |
| stored total | 3.12 MB (4 files) | 13.26 MB (2 files) |
|  data | 3,117,735 B Vortex (1 file) | 9,313,736 B Parquet (1 file) |
|  metadata | 643 B (catalog 272 B + 2 snapshots) | 3,944,448 B SQLite catalog |
| cold open printed (s) | 0.061 (`.lake open`) | n/a (ATTACH ≈3 ms, lazy) |
| cold end-to-end wall (s) | 0.077 (start→open→first query→exit) | 0.036 (start→attach→query→exit) |
| `COUNT(*)` best-of-5 (ms) | <1 | 7 (printed; 36 as fresh-process wall) |
| GROUP BY region + 5 aggs (ms) | 10 | 12 (printed; 41 as fresh-process wall) |
| `WHERE total > 500` (ms) | 5 | 11 (printed; 39 as fresh-process wall) |
| ORDER BY total DESC LIMIT 10 (ms) | 3 | 12 (printed; 41 as fresh-process wall) |

Both CLIs print `real X.XXX` (1 ms resolution; 0.000 means <1 ms), both
numbers above are best-of-5, and the run-to-run spread is ±1 ms at this
resolution. A 10M-row run (`--rows 10000000`, generated
deterministically, 243 MB CSV) shows DuckLake winning the larger scans:
ingest 0.368 s vs 1.943 s, stored 97.1 MB (93.1 MB Parquet + 3.9 MB catalog)
vs 31.1 MB Vortex, cold open 0.036 s vs 0.724 s, and query best-of-5
group-by 37 ms vs 93 ms, filter 24 ms vs 48 ms, order-by 28 ms vs 30 ms —
pink-lake keeps only `COUNT(*)` (<1 ms vs 8 ms). No cherry-picking: at 10M
DuckLake wins ingest, cold start and three of the four queries.

**Fairness caveat — this is "same data, same queries, different
architectures", not an apples-to-apples product comparison.** `.lake open`
eagerly loads the whole table from Vortex back into pinkdb's in-memory
engine, so pink-lake's per-query lines are in-memory scans and the load cost
is the separate open row; DuckLake is lazy — ATTACH only opens the SQL
catalog and every query scans Parquet at query time, so its printed query
times include the scan and its fresh-process wall adds ~30 ms of process
start + LOAD + ATTACH at 1M on top of the query. DuckLake is a full
lakehouse format (transactional SQL
catalog, MVCC-ish snapshots, time travel) and its catalog DB grows with
every write/snapshot (this run is a single ingest), while pink-lake
publishes a full snapshot per save and keeps old versions' files. No cache
was dropped on either side, and both engines returned matching results
(float sums differ in the last few digits from different summation orders;
ties in ORDER BY have no stable order on either engine).

## Known v1 limits

- No nested transactions or savepoints; one process per `.pink` path
  (single-writer: no file locking is taken); `.open` replaces the whole store
- `CREATE DATABASE` / `DROP DATABASE` / `LOAD EXTENSION` are refused inside a
  transaction; the last database cannot be dropped; `DROP DATABASE` does not
  remove views from other databases (views are per-database and session-local)
- Loadable extensions are native-only (wasm builds refuse `LOAD EXTENSION`),
  process-level, and not part of `.pink` snapshots. A durable database
  WAL-logs `LOAD EXTENSION`, and records that call an extension are prefixed
  with the `LOAD EXTENSION` statements replay needs (touched-only; a
  checkpoint resets that coverage). A missing file at replay time is a hard
  error, and loading the same extension twice reports
  `extension "x" is already loaded`
- Three-part column references (`db.table.col`) are unsupported; use an alias
  (`SELECT x.col FROM db.table AS x`)
- `USE`, `DATABASE`, `LOAD`, `EXTENSION` and
  `LEFT`/`RIGHT`/`OUTER`/`FULL`/`CROSS` are reserved
  words; quote identifiers to use them as names (`"use"` works)
- Durable commits refuse statements that read a session-local view (views are
  never persisted); `.import` checkpoints, so it needs a settled transaction
- Correlated subqueries are rejected (uncorrelated only); no FULL joins and
  CROSS joins are rejected
- Window functions are SELECT/ORDER-BY only (not WHERE/GROUP BY/HAVING)
- Aggregates in ORDER BY need a select alias or position
- General ORDER BY expressions must be aliases, positions, or GROUP BY keys
- `%` is integer-only; `SUM` overflow wraps
- Mixed DATE/TIMESTAMP comparison is rejected; timestamps never mix with
  integers (no INTERVAL type in v1); no time zones (naive UTC)
- Expression nesting is capped at 24 levels (parentheses, `CASE` arms and
  consecutive unary operators counted together); deeper input returns a clean
  `Parse` error (`expression nesting too deep`) instead of
  overflowing the parser stack. Separately, the parser-to-AST depth is capped
  at 64 nodes: left-associative operator chains (`1+1+...`), long `IN` lists,
  `AND`/`OR` runs and deeply nested subqueries return `expression too deep
  (limit 64)` — the AST is freed iteratively, so even a multi-megabyte chain
  cannot overflow the stack during rejection

Post-v1 roadmap: C/Python/Go language bindings, Parquet reader (see
PLAN.md). At-rest encryption shipped in wave 25 behind the `encryption`
feature; the network server (`pinkdb-server`) shipped in wave 24; the lake
layer (`pink-lake`, CLI `lake` feature) shipped in wave 34 and became
Vortex-first in wave 35 (v1 `.pink` lakes still load; open + save migrates);
authentication and TLS are deferred.

## Development

```sh
cargo build            # workspace
cargo test             # 100+ unit + end-to-end tests
cargo test -p pinkdb-server                          # protocol + server + durable tests
cargo test -p pinkdb-cli --features client           # remote-mode CLI tests
cargo test -p pinkdb-cli --features lake             # lake round-trip + CLI tests
cargo build -p pinkdb-cli --features lake            # REPL with .lake commands
PINKDB_CLI_BIN=./target/release/pinkdb cargo test -p pinkdb-cli --features lake  # release CLI (overflow checks off)
cargo test -p pinkdb-ext-loader                      # loadable extensions (compiles C test extensions with cc)
cargo test --features encryption                     # encrypted round-trips + WAL recovery
cargo build -p pinkdb-cli --features encryption      # REPL with PINKDB_KEY support
cargo clippy --all-targets   # must stay clean
python3 bench/duckdiff.py [--big sales.csv]  # differential test vs DuckDB
PINKDB_KEY=pass python3 bench/kill_loop.py 30 --encrypted  # SIGKILL stress, encrypted WAL
cargo run --release -p pinkdb --example import_bench  # CSV import timing
cargo run --release -p pinkdb --example query_bench  # full query battery (ms)
```
