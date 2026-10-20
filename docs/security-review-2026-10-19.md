# Security review — pinkdb (2026-10-19)

A security audit of the pinkdb workspace: the `pinkdb` engine, `pinkdb-server`
(network service), `pinkdb-ffi` (C ABI), `pinkdb-ext-loader` (native
extension loading), `pinkdb-cli`, `pink-lake` (lakehouse + S3), the
Python/Go bindings, and the CI workflows.

Paths are repo-relative. Line numbers refer to commit `2653591`.

## Method

Six subsystem audits ran in parallel (server/auth, crypto/TLS, FFI + extension
loader, SQL engine, lake storage/S3, CLI/bindings/CI). Every candidate finding
was then re-checked against the source by the reviewer, and the highest-impact
claims were **reproduced** against a built server and a real extension
(evidence in [Appendix A](#appendix-a--reproduced-evidence)).

Findings marked **[reproduced]** were independently confirmed by running the
exploit; the rest are traced to source with an exact line reference.

The crypto layer is in genuinely good shape: real AES-256-GCM, real Argon2id,
OS randomness, authenticated headers, constant-time tag comparison, no padding
oracle. Almost every finding below is a **policy, trust-boundary, or
lifecycle** defect rather than a broken primitive.

## Severity summary

| Severity | Count | Themes |
|---|---|---|
| Critical | 2 | Arbitrary native code execution via `LOAD EXTENSION` |
| High | 7 | Auth bypass, TLS downgrade, silently wrong query results, remote DoS |
| Medium | 12 | Privilege escalation, file disclosure, resource exhaustion, integrity |
| Low | 14 | Footguns, hardening, consistency, stale docs |

---

## Critical

### C-1 · Opening a database file executes native code recorded in its WAL **[reproduced]**

**Files:** `crates/pinkdb/src/wal.rs:4068-4075` (replay force-allows),
`crates/pinkdb/src/wal.rs:1838` (WAL record text), `crates/pinkdb/src/ext.rs:128`
→ `crates/pinkdb-ext-loader/src/lib.rs:474` (`dlopen`)

`LOAD EXTENSION '<path>'` is prefixed into the WAL record text for any
transaction that touched a loaded extension:

```rust
// crates/pinkdb/src/wal.rs:1838
rec.push(format!("LOAD EXTENSION '{}'", path.replace('\'', "''")));
```

WAL replay then **unconditionally re-enables** the front-end gate before
re-executing the records:

```rust
// crates/pinkdb/src/wal.rs:4074
let allow = self.allow_sql_load_extension;
self.allow_sql_load_extension = true;
```

The path is fully attacker-controlled by anyone who can produce a database
file. There is no signature, hash, allowlist, or extension-root confinement.
The `dlopen` happens *before* the ABI symbol check, so the library's
constructors and initializers run regardless of whether the load "succeeds".

**Reproduction** (see [A.1](#a-1-wal-replay-auto-loads-an-extension-path)):
a valid ABI-v1 extension with a `__attribute__((constructor))` was loaded into
a durable store, the store was closed, the marker file was deleted, and the
store was reopened. The marker was rewritten — the reopen `dlopen`ed the path
recorded inside the file.

**Impact.** Any user who opens a crafted `.pink` database — the CLI, the C
ABI, an embedder, a CI job that restores a backup — executes attacker code in
the pinkdb process, with the privileges of that process. This is a
file-format-to-RCE primitive: no authentication, no network access, and no
server involvement required.

**Remediation.** Never auto-`dlopen` a path recovered from a file. Record a
stable extension *name* plus a content hash in the WAL and resolve it against
a host-configured extension directory, or gate replay-time extension loading
behind an explicit opt-in (`DurableOpenOptions::allow_extension_replay`) with
a directory allowlist. At minimum, refuse paths that are not under the
configured extension root and are not already loaded in this process.

---

### C-2 · `LOAD EXTENSION` is enabled by default in the embedded and C ABI

**Files:** `crates/pinkdb/src/storage.rs:1028` (default `true`),
`crates/pinkdb/src/exec/mod.rs:898-905` (the only gate),
`crates/pinkdb-ffi/src/lib.rs` (no switch exposed), `crates/pinkdb/src/db.rs:1063-1073`

The store-level gate exists, but the default is permissive and the C ABI
exposes no way to change it:

```rust
// crates/pinkdb/src/storage.rs:1028
allow_sql_load_extension: true,
```

The embedded session (`principal == None`) skips the privilege check
entirely (`db.rs:1063-1073`), and `pinkdb-ffi` exports no
`pinkdb_set_allow_sql_load_extension` — the only handle mutator is
`pinkdb_execute`. The C header (`crates/pinkdb-ffi/include/pinkdb.h:128-132`)
documents `pinkdb_execute` as "Execute one or more `;`-separated SQL
statements" with no mention that it can load native code.

**Impact.** Every embedder is one SQL string away from arbitrary native code
execution. The Python binding (`bindings/python/pinkdb/__init__.py:583`) and
the Go binding (`bindings/go/pinkdb.go:250`) both expose `execute(sql)`
verbatim, so an application that forwards user input to `execute()` inherits
the RCE. The `pinkdb-server` binary *does* disable this by default
(`server.rs:188-190`, plus `--allow-load-extension`), which is why the network
surface is not affected — but the library surface is.

**Remediation.** Default `allow_sql_load_extension` to `false`; add
`pinkdb_set_allow_sql_load_extension(handle, allow)` to the C ABI and a
`--no-load-extension` CLI flag; document in `pinkdb.h` that `pinkdb_execute`
can load native code.

---

## High

### H-1 · The authentication gate is evaluated per *batch*, not per statement **[reproduced]**

**Files:** `crates/pinkdb-server/src/server.rs:577-586` (blocking),
`crates/pinkdb-server/src/async_server.rs:456-469` (async),
`crates/pinkdb-server/src/server.rs:898-900` (`auth_required`),
contradicted by `crates/pinkdb-server/src/lib.rs:67-69`

`auth_required(&db, &conn)` is checked **once**, before `conn.execute(&sql)`.
`Connection::execute` then runs every statement of the batch in order
(`crates/pinkdb/src/db.rs:355-374`), and each statement's `enforce()` is a
no-op because the connection has no principal (`db.rs:1069-1072`). So one
batch can both create the first user and grant it ADMIN.

The crate documentation claims the opposite: *"The gate is evaluated per
statement, so a connection that creates the first user must log in before its
next statement."* That is false for multi-statement batches.

**Reproduction** ([A.2](#a-2-unauthenticated-batch-creates-and-promotes-an-admin)):

```
unauth batch -> Done("evil" created), Done("ADMIN TO evil"), BatchDone(2)
login evil/p  -> LoggedIn("evil")
SELECT 1      -> ResultSet(1)                 # running as a global ADMIN
CREATE USER mallory PASSWORD 'x'  -> Done("mallory" created)
```

**Impact.** Any client that can reach the port of a freshly-provisioned
server becomes a global administrator — including `CREATE USER`, `GRANT
ADMIN`, `LOAD EXTENSION` (if enabled) and `CALL`. The only mitigation today
is the startup warning.

**Remediation.** Re-evaluate the gate *between* statements of a batch (execute
statement-by-statement, or have the engine re-check `has_users()` before each
statement and abort the batch). Consider an explicit opt-in for remote user
creation (`--bootstrap-admin-user`) or restricting `CREATE USER` to loopback.
Fix the `lib.rs:67-69` documentation.

---

### H-2 · `--transport async` silently discards `--tls-cert`/`--tls-key`

**Files:** `crates/pinkdb-server/src/bin/pinkdb-server.rs:210-233` (sync —
reads the flags), `:260-283` (async — never reads them),
`crates/pinkdb-server/src/async_server.rs:128-171` (no TLS support at all)

`parse_args` validates that the two flags are paired and `main` refuses to
start when the `tls` feature is missing, but the async branch never reads
`args.tls_cert`/`args.tls_key`. `AsyncServer` has no TLS field and no
`bind_tls`. The flags are accepted, validated, and then discarded.

**Reproduction** ([A.3](#a-3-async-transport-serves-plaintext-despite-tls-flags)):

```
$ pinkdb-server --host 127.0.0.1 --port 5607 --transport async \
      --tls-cert cert.pem --tls-key key.pem
pinkdb-server v0.1.0 listening on 127.0.0.1:5607 (in-memory, transport: async)
pinkdb-server: no authentication, no TLS on this transport — keep the bind address trusted
```

A raw plaintext `Hello` frame was answered with `HelloOk`, proving a
plaintext listener. The startup line says `transport: async` and never
mentions TLS, so log-scraping and audit tooling cannot detect the downgrade.

**Impact.** An operator who explicitly configured TLS gets an unencrypted
port; login passwords and all data cross the network in cleartext. This is a
silent downgrade of a requested security control.

**Remediation.** Either add a TLS path to `AsyncServer` (rustls over tokio) or
make the async branch **refuse to start** when `--tls-cert`/`--tls-key` are
supplied. Never accept-and-ignore a security flag.

---

### H-3 · The hash index is a stale snapshot — silently wrong results after any DML **[reproduced]**

**Files:** `crates/pinkdb/src/storage.rs:1831-1864` (index built once),
`crates/pinkdb/src/storage.rs:496` / `:642` / `:674` (append/update/delete —
no index maintenance), `crates/pinkdb/src/optimizer.rs:41-85` (rewrites
`Filter` → `IndexScan`, so the predicate is *not* re-checked)

`self.indexes` is touched only at `CREATE INDEX` (`storage.rs:1858`) and
`DROP INDEX` (`storage.rs:1869`). Nothing updates it on mutation, yet the
optimizer replaces the filter with an index scan, so the engine trusts a map
describing the table as it looked at index-creation time.

This directly contradicts the documented contract in `CHANGELOG.md:199-200`:
*"turns `col = literal` lookups into an `IndexScan` instead of a full scan —
**results are identical either way**"*. There is not a single `CREATE INDEX`
test in the engine crate, which is why the gap is invisible.

**Reproduction** ([A.4](#a-4-stale-index-returns-the-wrong-row)):

```sql
CREATE TABLE accounts (id INTEGER, owner VARCHAR);
INSERT INTO accounts VALUES (1, 'alice'), (2, 'bob');
CREATE INDEX ix ON accounts(id);
UPDATE accounts SET id = 99 WHERE id = 1;
SELECT * FROM accounts WHERE id = 1;   -- returns the row with id = 99
SELECT * FROM accounts WHERE id = 99;  -- returns NOTHING
```

**Impact.** Any application that resolves a record or an authorization
decision with `WHERE <key> = <value>` against an indexed column gets the
**wrong record** or **no record**. In a multi-tenant schema (`owner_id`,
`tenant_id`, `account_id`) this is a cross-record disclosure primitive: the
lookup for tenant A can return tenant B's row. It is also a fail-open hazard
for policy code that treats "no matching row" as "allowed".

**Remediation.** Maintain the index on every mutation, or invalidate/drop the
table's indexes on any DML and rebuild lazily. Independently, keep the
`Filter` node *above* the `IndexScan` so a stale index can only cost
performance, never correctness.

---

### H-4 · `DELETE` after `CREATE INDEX` panics the engine — remote DoS **[reproduced]**

**Files:** `crates/pinkdb/src/vector.rs:321`,
`crates/pinkdb/src/exec/mod.rs:1347-1350`

Same root cause as H-3, but the stale entry is a **row index that no longer
exists**, and the index-scan gather path performs no bounds check.

**Reproduction** ([A.5](#a-5-delete-after-create-index-panics)):

```sql
CREATE TABLE t (id INTEGER, s VARCHAR);
INSERT INTO t VALUES (1, 'a'), (2, 'b');
CREATE INDEX ix ON t(id);
DELETE FROM t WHERE id = 2;
SELECT * FROM t WHERE id = 2;
-- thread 'main' panicked at crates/pinkdb/src/vector.rs:321:9:
-- vector index out of bounds
```

**Impact.** In the network server the panic is caught
(`pinkdb-server/src/server.rs:584-647`) so the process survives, but the
store's `RwLock` is poisoned and the client gets `internal error: statement
panicked`. The panic is repeatable, so one client can poison and recover the
store lock on demand. In the embedded API and the CLI there is no catcher — the
panic unwinds into the caller's thread and aborts the process. This is a
trivially reachable remote DoS.

**Remediation.** Fix H-3, plus a defensive bounds check in `index_scan_output`
(`exec/mod.rs:1347-1350`): skip or reject row indices outside the current
chunk set.

---

### H-5 · The index probe key is type-tagged — a keyed lookup silently returns zero rows **[reproduced]**

**Files:** `crates/pinkdb/src/exec/aggregate.rs:19-79` (`encode_value` adds a
type tag), `crates/pinkdb/src/storage.rs:1845-1854` (map keyed by the
*column's* type), `crates/pinkdb/src/exec/mod.rs:1650-1652` (lookup keyed by
the *literal's* type), `crates/pinkdb/src/optimizer.rs:55-79` (no type match
required)

The index map is keyed with the column's value type; the lookup key is the
literal's own type. The binder deliberately keeps the literal's type for
numeric comparisons (`sql/binder.rs:758-786` — `comparison_ok` accepts any
numeric/numeric pair), and the optimizer does not require the two to match.

**Reproduction** ([A.6](#a-6-type-tagged-index-key-returns-zero-rows)):

```sql
CREATE TABLE t (id BIGINT);
INSERT INTO t VALUES (5);
CREATE INDEX ix ON t(id);
SELECT * FROM t WHERE id = 5;   -- 0 rows   (correct answer: 1 row)
-- without the index, the same query returns 1 row
```

Also affected: `DOUBLE` column + integer literal, `DECIMAL` column + integer
literal, `INTEGER` column + `CAST(5 AS BIGINT)`, and `DECIMAL` columns whose
scale byte differs.

**Impact.** Reachable with a *single* statement and no DML. A policy or
authorization check that queries by an integer id on a `BIGINT`/`DECIMAL`/
`DOUBLE` column returns an empty set — a fail-open hazard for any "does this
row exist / does this principal have access" query.

**Remediation.** Canonicalize the probe key to the indexed column's type
before encoding (coerce the literal through `DataTable::coerce` /
`cast_value`), or encode numeric keys type-independently, or refuse the
index rewrite when the literal's type differs from the column's.

---

### H-6 · Unauthenticated logins hold the store's `RwLock` read lock across Argon2id

**Files:** `crates/pinkdb/src/db.rs:390-394`, `crates/pinkdb/src/rbac.rs:423-433`,
`crates/pinkdb/src/rbac.rs:368-393`, `crates/pinkdb-server/src/server.rs:556-576`

```rust
pub fn login(&mut self, user: &str, password: &str) -> Result<String> {
    let name = read_lock(&self.db.shared).authenticate(user, password)?;  // guard alive
    self.principal = Some(user.to_ascii_lowercase());
```

The temporary `RwLockReadGuard` is alive for the whole call, so the ~19 MiB /
t=2 Argon2id derivation runs **while the shared store's read lock is held**.
Every write path needs the write lock, so it blocks for the full duration.
The server applies no rate limiting, lockout, or delay to `Request::Login`,
and `Login` is reachable without any credentials.

**Impact.** With `DEFAULT_MAX_CONNECTIONS = 128` (`server.rs:84`) and no rate
limit, 128 connections looping `Login` requests keep the read lock
permanently saturated: all writers (`INSERT`/`UPDATE`/`DELETE`/DDL) starve
and the database becomes effectively read-only for legitimate users, while
128 concurrent Argon2id derivations also burn ~2.4 GiB of transient RSS. A
credential-free, one-connection-cheap DoS. The memory-amplification ratio is
roughly 19 MiB allocated per 40-byte request.

**Remediation.** Copy the user record out from under the lock, release it,
and only then run the verification (it is pure computation over the stored PHC
string). Add a per-connection and per-IP failed-login rate limit with backoff,
and cap concurrent in-flight verifications.

---

### H-7 · `LOAD EXTENSION` has no path restriction, allowlist, or signature check

**Files:** `crates/pinkdb-ext-loader/src/lib.rs:464-477`,
`crates/pinkdb-server/src/server.rs:199`

The only validation of the path is "not empty":

```rust
pub fn load(path: &str) -> Result<LoadedExtension, ExtError> {
    if path.is_empty() { /* ... */ }
    let library = unsafe { libloading::Library::new(path) }.map_err(|e| ExtError::Load { /* ... */ });
```

No canonicalization, no root-directory restriction, no `..` rejection, no
symlink check, no code-signing or hash pinning anywhere in the workspace. The
server restricts *who* may issue the statement (ADMIN, and only with
`--allow-load-extension`) but not *what* may be loaded.

Aggravating detail: `dlopen` with a name containing no `/` searches the
dynamic loader's search path (`LD_LIBRARY_PATH`, `/usr/lib`, …), so
`LOAD EXTENSION 'libfoo.so'` maps and runs the constructor of a library from
the *system* loader path — the attack surface is wider than "a file the
attacker could write".

**Impact.** On a server with `--allow-load-extension`, an ADMIN user can map
any shared object on the host into the server process. Combined with C-1 this
is also the path an attacker uses to weaponize a database file.

**Remediation.** Resolve the path against a configured extension root,
`canonicalize()` it, reject symlinks and `..` escapes, and pin extensions by
name + SHA-256 rather than by raw path.

---

## Medium

### M-1 · Emptying the RBAC catalog silently promotes a live session to full access **[reproduced]**

**Files:** `crates/pinkdb/src/rbac.rs:1079-1081`, `crates/pinkdb/src/db.rs:1063-1073`

```rust
if rbac.users.is_empty() && rbac.roles.is_empty() && rbac.admins.is_empty() {
    return Ok(());          // "nothing is configured → skip every check"
}
```

That decision is made against the *current* catalog, but the principal is a
**sticky** value captured at login. `enforce()` only skips when
`principal == None` (a trusted embedded session). A session that authenticated
while the catalog was populated keeps its principal, but the moment the
catalog is emptied the check is skipped for it too.

**Reproduction** ([A.7](#a-7-empty-rbac-catalog-promotes-a-live-session)):

```
alice create table            -> error: permission denied: user "alice" needs the CREATE privilege
root drops all users          -> Done, Done
alice CREATE USER mallory     -> Done("mallory" created)     # was denied before
```

**Impact.** A silent, in-place privilege escalation for any session
authenticated before the last user was removed. The server's documented
"no users ⇒ trust every connection" policy governs *new* connections; it does
not authorise an *existing* authenticated session to jump from "no
privileges" to "unrestricted".

**Remediation.** Move the "empty catalog ⇒ skip" shortcut into `enforce()`
(`db.rs:1063-1073`) so it applies only when `principal == None`. An
authenticated principal must always be checked, even if it no longer exists in
the catalog.

---

### M-2 · Login timing side channel enumerates valid user names **[reproduced]**

**Files:** `crates/pinkdb/src/rbac.rs:983-991`, `crates/pinkdb/src/rbac.rs:423-433`

```rust
let Some(def) = self.rbac.users.get(&user.to_ascii_lowercase()) else {
    return Err(auth_failed());        // returns immediately, no hashing
};
if !verify_password(&def.password_hash, password) { /* 19 MiB Argon2id */ }
```

The error text is deliberately identical for an unknown user and a wrong
password, but the timing is not.

**Reproduction** ([A.8](#a-8-login-timing-enumerates-user-names)):

```
== EXISTING user, wrong password ==
     35.52 ms   22.87 ms   17.29 ms
== NON-EXISTENT user ==
      0.09 ms    0.09 ms    0.06 ms
```

A ~200-500x difference makes offline enumeration of the user list trivial,
defeating the stated goal of the identical error message.

**Remediation.** Always perform a dummy Argon2id verification against a fixed
hash when the user is unknown, so both paths cost the same.

---

### M-3 · `COPY <table> TO '<path>'` writes an arbitrary file under a mere `SELECT` grant

**Files:** `crates/pinkdb/src/rbac.rs:1113-1149` (no `CopyTo` arm),
`crates/pinkdb/src/plan.rs:673-675` (only `SELECT` is collected),
`crates/pinkdb/src/exec/mod.rs:1206` (`format.write(Path::new(path), t)`),
`crates/pinkdb/src/filefmt.rs:243` (`fs::write` — follows symlinks, truncates),
`extensions/arrow/src/write.rs:34` (`File::create`)

The RBAC model has no "write to the filesystem" privilege. `check_statement`
has no `CopyTo` arm, so the only requirement is `SELECT` on the source table.
The destination path is taken verbatim from the client — no canonicalization,
no base-directory restriction, no path-traversal check, no `O_EXCL`/
`O_NOFOLLOW`.

**Reachability.** The shipped `pinkdb-server` binary registers no file
format, so `COPY` currently fails with *"no file format … is registered"*.
The gap is live in the CLI (`--features arrow`) and in any embedder that
calls `pinkdb_arrow::ensure_registered()`. The server has an explicit opt-out
for the comparable `LOAD EXTENSION` risk (`Server::allow_load_extension`,
`server.rs:251-257`) but **no equivalent guard for `COPY`**.

**Remediation.** Add a server-level opt-in mirroring `allow_load_extension`
(`Server::allow_copy_to(bool)`, default off) and/or confine `COPY`
destinations to a configured base directory; add an explicit RBAC privilege
for filesystem writes rather than reusing `SELECT`; write through a
`create_new` temp + `rename` and open the output `0600`.

---

### M-4 · Table-function scans bypass every privilege check

**Files:** `crates/pinkdb/src/rbac.rs:1113-1149` (no `TableFunction` arm),
`crates/pinkdb/src/plan.rs:722-725` (`LogicalPlan::TableFunction { .. } => {}`),
`crates/pinkdb/src/rbac.rs:1166-1181` (read pass finds nothing to check)

For a `TableFunction` plan the statement-kind arm falls through to `_ => {}`,
the write arm falls through, and the read pass collects zero catalog tables.
The result is that a table-function statement requires **no privilege at
all** — not even ADMIN — and the file path is fully client-controlled with no
base-directory restriction.

**Impact.** A user with zero grants can read arbitrary parquet/arrow files
readable by the server process via `SELECT * FROM read_parquet('/path')`.
Same qualification as M-3: the parquet/arrow extensions register their
sources **process-globally** (`register_global_table_source`), so any host that
loads them exposes them through the server.

**Remediation.** Require ADMIN (or a dedicated privilege) for any
`TableFunction` plan and/or restrict table-source paths to a configured base
directory.

---

### M-5 · WAL, checkpoint, lock file and lake data files are world-readable (0644) **[reproduced]**

**Files:** `crates/pinkdb/src/wal.rs:1389-1394` (WAL),
`crates/pinkdb/src/persist.rs:2453-2459` (checkpoint),
`crates/pinkdb/src/persist.rs:2474-2477` (staging file),
`crates/pinkdb/src/wal.rs:591-596` (lock file),
`crates/pink-lake/src/local.rs:398-405` (lake data files)

```rust
// crates/pinkdb/src/wal.rs:1389
let mut f = OpenOptions::new()
    .read(true).write(true).create(true).truncate(true)
    .open(path)?;                       // <-- no .mode(0o600)
```

`OpenOptionsExt::mode` / `PermissionsExt` appear **only** in
`crates/pink-lake/src/storage.rs:130` and `:145` — i.e. only for the
*decrypted staging copies* of lake objects (correctly 0700/0600). Every other
artifact falls back to the process umask.

**Reproduction** ([A.9](#a-9-durable-artifacts-are-world-readable-and-the-wal-holds-plaintext-sql)):

```
audit.pink.wal    mode=644
audit.pink.lock   mode=644
WAL printable content: PINKWAL1X6h #J8v4maintCREATE TABLE t (a INTEGER)7eb9maintINSERT INTO t VALUES (42)
```

**Impact.** On a multi-user host, any local account can read the whole
database, every lake data file, and the **WAL**, which stores the *literal SQL
text* of every committed transaction — `INSERT INTO users VALUES ('hunter2')`
sits on disk verbatim, long after the checkpoint rolls it in. This directly
undercuts the at-rest encryption feature: a keyed lake's data files are
encrypted, but the engine's WAL and checkpoint are not.

**Remediation.** Create the WAL, checkpoint, lock file and lake data files
with `OpenOptionsExt::mode(0o600)` (and the containing directory `0700` where
it is not shared).

---

### M-6 · No statement timeout, no result-size cap, and the write lock is held for the whole statement

**Files:** `crates/pinkdb/src/db.rs:687` (write lock across the statement),
`crates/pinkdb/src/db.rs:595-602` (read lock across the query),
`crates/pinkdb-server/src/server.rs:584-611` (no timeout, no row cap),
`crates/pinkdb/src/exec/mod.rs:1154-1176` (whole result set materialized),
`crates/pinkdb/src/exec/mod.rs:4155-4242` (`nested_loop_join` has no output cap)

The server's `ROWS_FRAME_BUDGET` (8 MiB) only bounds the size of one *wire*
frame — the total is unbounded. There is no statement timeout anywhere in the
engine or the server (the socket idle timeout covers socket I/O, not engine
execution), and `nested_loop_join` has no output-row cap, so `CROSS JOIN` is
an unbounded Cartesian product.

**Impact.** A single connection can hold the exclusive write lock
indefinitely (a large `UPDATE`), starving every other client, and/or
materialize an unbounded result set in host memory. In the blocking
transport, the connection thread is pinned for the duration; with 128 slots
all pinned, the server is unresponsive.

**Remediation.** Add a per-statement deadline/cancellation check in the
executor's chunk loop and a cap on the materialized result size (or stream
it). At minimum, do not hold the store write lock across unbounded user
work.

---

### M-7 · Lake manifest containers are bound to neither the lake nor the version → rollback

**Files:** `crates/pink-lake/src/storage.rs:357-375` (empty binding),
`crates/pink-lake/src/crypto.rs:863-872` (the binding check),
`crates/pink-lake/src/lib.rs:2212-2232` (`read_manifest` — no version cross-check)

```rust
// crates/pink-lake/src/storage.rs:357
if rel == CATALOG_FILE || rel.starts_with("snapshots/") {
    (crypto::Domain::Manifest, "")     // <-- binding is EMPTY
}
```

The manifest container's AEAD binding is `""`, and the AAD binds only the
public header — not the manifest's version, lake identity, or content. The
open path therefore accepts *any* manifest container produced under the same
key. `header.binding != binding` with `""` on both sides never fires.

**Impact.** An attacker with write access to lake storage can copy
`lakeB/catalog.json` (or an older `lakeA/snapshots/3.json`) over
`lakeA/catalog.json`. The AEAD authenticates it, and the lake silently
resumes at the older version or serves another lake's rows. Version 5's data
files become orphans and the next save collides (`LakeError::Conflict`),
permanently wedging the lake for writes. The source comment calls this "a
known rollback surface", but understates it: **any** version of **any** lake
under the key authenticates.

**Remediation.** Give each lake a random `lake_id` stored in every container
header and checked on open; bind the manifest domain to `(lake_id, version)`;
publish a separate monotonically-increasing authenticated "highest version"
object so a rollback is detectable at `Lake::open`.

---

### M-8 · `PASSWORD_HASH` bypasses the Argon2id work-factor policy

**Files:** `crates/pinkdb/src/rbac.rs:438-448` (`validate_password_hash`),
`crates/pinkdb/src/rbac.rs:423-433` (`verify_password`)

`validate_password_hash` checks **only** the algorithm string:

```rust
if parsed.algorithm.as_str() != "argon2id" { /* reject */ }
Ok(())
```

The `password-hash` crate's blanket `PasswordVerifier` impl re-derives using
the parameters **embedded in the PHC string**, not the verifier's own. So
`CREATE USER bob PASSWORD_HASH '$argon2id$v=19$m=8,t=1,p=1$...'` is accepted,
and every subsequent login for `bob` verifies against 8 KiB / 1 iteration —
crackable at ~10^10 guesses/s on a GPU. The same path also accepts an
unbounded `m_cost` (`m=4000000` → 4 GiB per login attempt → self-DoS).

**Remediation.** In `validate_password_hash`, parse the PHC parameters and
enforce a floor (`m ≥ 19456`, `t ≥ 2`, `p ≥ 1`) and a sane ceiling before
accepting the string.

---

### M-9 · Terminal escape injection — untrusted identifiers are rendered raw

**Files:** `crates/pinkdb-cli/src/main.rs:1044-1060` (`sanitize_cell`),
`:1114-1127` (`row_out` — column names unsanitized),
`:1156-1193` (`QueryResult` name payloads),
`crates/pinkdb-cli/src/lake.rs:1276`, `crates/pinkdb-server/src/client.rs:372`

The CLI has an explicit sanitizer for untrusted text, but applies it to **cell
values only**:

```rust
// crates/pinkdb-cli/src/main.rs:1044
Value::Varchar(s) => sanitize_cell(s),
```

**Column names and object names bypass it entirely.** `row_out` pushes
strings with no escaping, and `format_select` feeds the result-set column
names straight into it. Every `QueryResult` variant that carries a name
(`CreatedView`, `DroppedTable`, `CreatedDatabase`, `LoadedExtension`,
`CreatedUser`, `CreatedRole`, `Granted`) is printed raw.

Quoted identifiers accept any byte except `"` and NUL
(`crates/pinkdb/src/sql/peg_parser.rs:862-874`), so a table named
`"a\e[31mRED\e[0m"` is legal SQL, and a hostile server can send arbitrary
bytes as column names over the wire.

**Impact.** Terminal escape injection: screen clearing, title/clipboard
manipulation via OSC, output spoofing of query results, and exploitation of
any terminal-emulator CVE. A malicious pinkdb-server can spoof an entire
result table in the client's terminal with no user action beyond connecting.

**Remediation.** Run every rendered identifier through `sanitize_cell`
(renamed to something not value-specific) before printing — column names,
all `QueryResult` name payloads, `cmd_databases`/`cmd_tables`/`cmd_schema`,
and the lake module's table keys and change values.

---

### M-10 · `.install` executes unsigned native code — integrity anchored only in an unsigned catalog

**Files:** `crates/pinkdb-cli/src/repo/client.rs:228` (SHA-256 check),
`crates/pinkdb-cli/src/repo/catalog.rs:99`, `crates/pinkdb-cli/src/main.rs:365`
(`dlopen`), `crates/pinkdb-cli/src/repo/config.rs:26` (default repository),
`ext-repo/FORMAT.md:126` ("repository provides no signatures in v1")

The download is verified against a SHA-256 that is fetched **from the same
origin as the artifact**, in a catalog that is not signed. The verified
artifact is then `dlopen`ed. The default repository is a single third-party
host.

**Impact.** Compromise of the repository Worker, the R2 bucket, the
Cloudflare account, or the TLS path to that host lets an attacker serve a
modified `index.json` (with the sha256 of their payload) plus the payload
dylib. The CLI verifies the hash, prints "sha256: …" and loads
attacker-controlled native code — arbitrary code execution. The SHA-256 only
protects against corruption, not against a hostile origin. There is also a
narrow TOCTOU window between the hash check and the `dlopen`.

**Remediation.** Sign the catalog and each artifact (minisign/sigstore) with a
key id in the catalog, and verify the signature in `.install` **before**
`dlopen`; pin a repository public key in the binary or in `$PINKDB_HOME`;
close the TOCTOU window by loading the already-open file descriptor.

---

### M-11 · Unbounded thread creation for connections refused by the connection cap

**Files:** `crates/pinkdb-server/src/server.rs:290-306` (the cap check),
`crates/pinkdb-server/src/server.rs:369-379` (`reject_over_limit`)

Every connection arriving while the cap is reached spawns a **new OS
thread** that writes an error frame and exits. There is no bound on how many
such threads exist at once, and `std::thread::spawn` failures are silently
ignored (`server.rs:373`). Each live connection thread also holds a 16 MiB
stack (`server.rs:81`).

**Remediation.** Write the refusal on the accept loop with a bounded
non-blocking write, or use a small bounded pool of reject writers. (The
async transport's `tokio::spawn` does not have this problem.)

---

### M-12 · Local `read_tagged` returns an inconsistent `(bytes, tag)` pair → the conditional catalog replace can clobber a concurrent writer

**Files:** `crates/pink-lake/src/local.rs:212-216` (read + separate stat),
`crates/pink-lake/src/local.rs:144-156` (`(length, mtime)` tag),
`crates/pink-lake/src/lib.rs:1713` (staleness check)

```rust
fn read_tagged(&self, rel: &str, context: &str) -> Result<(Vec<u8>, VersionTag)> {
    let bytes = self.read(rel, context)?;          // syscall #1 (T1)
    let tag = local_tag(&self.abs(rel))?;          // syscall #2 (T2)
    Ok((bytes, tag))
}
```

Two separate syscalls: if the file is replaced between T1 and T2, the bytes
are the old file's and the tag is the new file's. The S3 backend is not
affected — it returns bytes and ETag from the *same* HTTP response.

Compounding it, the local tag is `(length, mtime)` — not a file identity
(`crates/pink-lake/src/local.rs:144-156`). The catalog for two consecutive
lake versions usually has the same byte length (same digit count in the
version, fixed-width ISO-8601 `updated`), so the tag discriminates only by
mtime — which is nanosecond-valued on APFS but degrades to whole seconds on
filesystems with coarse timestamp granularity (HFS+, FAT, some NFS mounts),
where two different catalogs written in the same second share a tag.

**Impact.** A concurrent writer's published catalog can be silently clobbered;
the lake loses a published version and wedges for the next save. The repo
already has the right primitive two crates over: `crates/pinkdb/src/wal.rs:445-458`
(`identity_of` → `(dev, ino)`).

**Remediation.** Open the file once, `fstat` the *same* fd for the tag, and
read from that fd; include `(dev, ino)` in the tag.

---

## Low

| # | Finding | Evidence | Remediation |
|---|---|---|---|
| L-1 | `ALTER TABLE … DROP COLUMN` does not reindex the table's hash indexes; a later `ADD COLUMN` inherits a stale index → wrong results. **[reproduced]** | `crates/pinkdb/src/storage.rs:361-390` | Apply the same `reindex` closure to every `Index::column`; drop indexes on the dropped column. |
| L-2 | `Request` derives `Debug`, so `format!("{req:?}")` prints the **plaintext login password**. The engine deliberately redacts the equivalent type. | `crates/pinkdb-server/src/protocol.rs:208-229` | Implement `Debug` for `Request` manually, redacting `Login::password`. |
| L-3 | Non-constant-time comparison of derived key material (`derived.bytes == self.key.bytes` — short-circuiting `memcmp`). | `crates/pinkdb/src/crypto.rs:858-863` | Use `subtle::ConstantTimeEq`. |
| L-4 | 64-bit random nonce prefix in both container formats; the residual birthday bound (`P ≈ n²/2⁶⁵`) applies to the *key-check* unit, which is exactly the fixed-plaintext GCM nonce-reuse condition that enables tag forgery. | `crates/pinkdb/src/crypto.rs:89`, `crates/pink-lake/src/crypto.rs:111` | Widen the prefix to 96+ random bits, or derive a 128-bit per-key base with a counter. |
| L-5 | The WAL publish block / coordination slot sits outside the AEAD (covered only by a CRC). No data-integrity bypass found, but the slot payload is unauthenticated. | `crates/pinkdb/src/wal.rs:238-239`, `:842-870` | Fold the publish block into the AAD base, or authenticate it with its own AEAD unit. |
| L-6 | `arg_type_at` underflows (`n_args() - 1`) for a 0-argument function: panic in debug, OOB index in release. Public API on a public type. | `crates/pinkdb-ext-loader/src/lib.rs:190`, `linked.rs:363-366` | Return `Option<ExtType>`, or guard with `is_empty()`. |
| L-7 | `Vec::with_capacity(value_count)` in the C FFI uses an untrusted `size_t`; `value_count = 2^62` aborts the process before the array is validated. | `crates/pinkdb-ffi/src/lib.rs:650-654` | Validate `value_count` against a sane bound *before* allocating. |
| L-8 | The host hands a **stack-allocated** `Collector` to extension code; a late `register_scalar` writes into a dead stack frame. The "only inside init" rule is a promise, not an enforcement. | `crates/pinkdb-ext-loader/src/lib.rs:539-550`, `host.rs:148` | Heap-allocate the collector and keep it alive with the library; add an `init_complete` flag. |
| L-9 | The host performs an unbounded, extension-controlled OOB read when copying a VARCHAR result, and `free()`s a pointer it cannot validate. | `crates/pinkdb-ext-loader/src/function.rs:286-293`, `:227` | Cap the copied length; only honour the free when the decoded type is `Varchar`. |
| L-10 | `PinkValue` crosses the C ABI by value with **no compile-time layout pin**, and the header-sync test does not cover it. Reordering a field compiles cleanly and silently reinterprets every value in all three consumers. | `crates/pinkdb-ffi/src/lib.rs:126-135` (cf. the pins at `ext-loader/src/abi.rs:117-129`) | Add `offset_of!`/`size_of` const-assertions and extend `header_sync.rs`. |
| L-11 | `Signature` derives `Debug`; its `canonical_request` contains the signed header list, including `x-amz-security-token` — a live AWS session token. `Credentials` and `S3Config` already redact manually. | `crates/pink-lake/src/s3/sigv4.rs:77-87` | Implement `Debug` manually and redact `canonical_request`. |
| L-12 | Commit-delta file paths are not validated (the *main* manifest is, via `validate_rel_path`); `LocalStorage::abs` uses `Path::join`, which replaces the root for an absolute path. | `crates/pink-lake/src/cdc.rs:520`, `local.rs:159-165` | Call `validate_rel_path` on every commit-delta path; reject absolute/`..` paths in `abs`. |
| L-13 | 32-bit-only integer overflow in the `.pink` RLE decoders: `out.len() + len` wraps on a 32-bit target, bypassing the guard before a multi-gigabyte `repeat_n`. | `crates/pinkdb/src/persist.rs:1688-1701` (and 4 siblings) | Use `checked_add`. |
| L-14 | The Cloudflare Workers API token is in the job-level `env:` of `deploy-docs.yml`, so it is exposed to `npm install -g wrangler` and to `website/build.py`. The project's other workflow already scopes it to the deploy step. | `.github/workflows/deploy-docs.yml:30-42` (cf. `publish-extensions.yml:267-272`) | Move the token to the `wrangler deploy` step's `env:`. |

### Also noted (informational, no action strictly required)

- **Role-derived `ADMIN` is not honored by the enforcement path.** `principal_is_admin`
  resolves role membership (`rbac.rs:940-948`) but `check_statement` uses the
  direct-membership check (`rbac.rs:1082`, `:561-563`). Fails **closed** (denies
  rather than escalates) but contradicts the documented model, and an embedder
  consulting `principal_is_admin` reaches a different conclusion from the engine.
- **The server's startup banner is stale.** `usage()` and `announce()` still say
  *"no authentication"* (`bin/pinkdb-server.rs:84-86`, `:288-304`) even though
  the server enforces authentication when the store has users (wave 48). The
  accurate message is printed separately by `warn_auth_mode`, so operators see
  contradictory guidance.
- **The idle timeout is per socket operation, not per frame.** A client that
  sends one byte every 299 s keeps its connection (and its 16 MiB-stack
  thread) alive forever, so all 128 slots can be pinned.
  (`server.rs:435-452`, `protocol.rs:670-696`.)
- **CI pipes an unpinned remote installer into a shell**
  (`.github/workflows/ci.yml:196`: `curl -fsSL https://install.duckdb.org | sh`)
  and pulls mutable Docker image tags
  (`crates/pink-lake/tests/s3_docker.sh:122`). No secrets are in scope for
  those jobs, so the impact is CI tampering / test-result forgery.
- **An ambient `PINKDB_PASSWORD` breaks the local CLI** — the env var is
  consulted even without `--host`, so a machine that exports it for an
  unrelated purpose makes the local REPL refuse to start.
  (`crates/pinkdb-cli/src/main.rs:1343`.)
- **Four `.lake` subcommands bypass `PINKDB_KEY`** — `.lake partition`,
  `.lake inlining`, `.lake flush` and `.lake add_data_files` use the keyless
  constructor instead of `open_lake()`. Fail-closed for encrypted lakes, but a
  plaintext lake is published as plaintext while the operator believes the
  session is keyed. (`crates/pinkdb-cli/src/lake.rs:1003`, `:1100`, `:1161`, `:1331`.)
- **`--tls-insecure` / `ClientTrust::Insecure`** accepts any certificate — a
  documented, loudly warned escape hatch. Not a bug; a footgun.

---

## Areas checked and found safe

Recording these explicitly so a follow-up review does not re-audit them.

**Cryptography — solid.**
Real AES-256-GCM (`aes-gcm` 0.10.3), not a hand-rolled construction. The
authentication tag is verified before any plaintext is returned
(`crypto.rs:722-729`, `pink-lake/src/crypto.rs:914-929`); tag comparison uses
`subtle::ConstantTimeEq` inside the library. No padding oracle (GCM has no
padding; the "wrong key" vs "corrupt" split comes from a dedicated key-check
block). Nonces are a 64-bit OS-random prefix + a 32-bit counter, minted fresh
per container, with counter overflow handled as a clean error. Argon2id v0x13
at m=19 MiB / t=2 / p=1 with a 16-byte OS-random per-user salt; no fast hash,
no pepper needed (per-user salt present). Header-supplied KDF parameters are
bounded, so a crafted header cannot force an unbounded derivation. Key
material is redacted in every `Debug` impl, zeroized on drop, and never
persisted to disk. The whole public header is bound as AAD, so a `.pink` and
a WAL can never authenticate as each other. Mixed-mode protection is
enforced in both directions.

**SQL injection — none found.** The server never concatenates client input
into SQL. The only re-parse path is prepared-statement substitution, which
escapes `'` → `''` (`prepared.rs:274-334`); the grammar has **no** backslash
escapes (`peg_parser.rs:881-883`), so quote-doubling is sufficient. Procedure
variables are bound as values, not spliced. Identifier re-emission
(`quote_ident`) doubles `"` → `""`, exactly the grammar's escape.

**Protocol framing — no amplification or overflow.** The length prefix is
validated against `MAX_FRAME_SIZE` before any allocation; the body buffer
grows with bytes actually received (never pre-sized from the untrusted
prefix); every inner length is checked against the bytes remaining in the
frame. Frame decoding is fail-closed.

**Parser/recursion — bounded.** Two independent guards
(`sql/mod.rs:39` `MAX_NESTING_DEPTH = 24` pre-parse, `sql/mod.rs:28`
`MAX_AST_DEPTH = 64` post-parse with an iterative teardown), plus
`MAX_CALL_DEPTH = 32` and `MAX_BODY_NESTING = 16` for procedures. A 200 000-case
differential fuzz of the statement splitter against the parser found zero
mismatches.

**Integer arithmetic — checked.** `checked_int_arith!` for INTEGER/BIGINT,
`checked_*` on `i128` for DECIMAL, `checked_add`/`checked_mul` for date and
timestamp arithmetic. DECIMAL precision/scale underflow is rejected at
construction. A 4 000-case adversarial literal corpus produced zero panics.

**RBAC statement coverage — complete for the statement kinds that exist.**
Every grammar statement kind is matched by `check_statement`; a 40-statement
matrix against a logged-in non-admin found no bypass. There is no `ATTACH`
statement in the SQL surface, so there is no attach-based escalation. Views and
materialized views rebind to the base table, so `CREATE` on a database cannot
be used to read a table you cannot read.

**Path traversal in the lake — safe for catalog and partition paths.**
`encode_key` percent-encodes everything outside `[A-Za-z0-9_-]`, so a table
key can never contain a path separator. `validate_rel_path` requires a
relative path with all-normal components under `data/`, and is applied to
every main-manifest file path, delete path and attach layout path. Partition
values live in the catalog, never in a path.

**Symlink attacks on lake writes — safe.** Writes use `create_new` temp +
`rename` and `hard_link`, neither of which follows a symlink at the target.
`remove_dir_all` does not follow symlinks, so the temp-dir sweeper cannot be
abused for arbitrary directory deletion.

**WAL crash recovery — no panic or OOB path.** Every length is bounds-checked
with `checked_add` / `end > data.len()`; the LSN must strictly increase; the
record size is capped; torn tails truncate; CRC-valid-but-malformed records
are hard errors.

**Vortex / delete-file parsing — bounded.** Every decode entry point runs
under `catch_unwind`; the metadata segment is capped at 1 MiB; stored chunk
rows are capped; the root dtype and field count are cross-checked against the
metadata. Delete files use exact length checks and `checked_mul`/`checked_add`.

**CI — good hygiene.** Every `uses:` in all three workflows is pinned to a
full commit SHA. No `pull_request_target`. The secret-bearing workflows are
`workflow_dispatch`-only or path-filtered to `main`. No `${{ github.event.* }}`
is interpolated into a `run:` block. No `.cargo/config.toml` source
replacement; every dependency resolves to crates.io.

**Command injection — none.** No `std::process::Command` / `sh -c` /
`subprocess` with `shell=True` in any production path. The only `Command::new`
uses are in test harnesses, and all pass argv positionally without a shell.

**Client TLS verification — correct.** rustls with a real `RootCertStore`
built from the PEM trust anchor (fail-closed, no system root store), and the
server name is bound to the connection so the hostname is checked. There is no
SSH-style "trust this host key" TOFU flow anywhere.

**Extension ABI version checking — present and correct.** The loader looks up
`pinkdb_extension_abi_version` and rejects any value ≠ `PINKDB_EXT_ABI_VERSION`
**before** any other symbol is used. `struct_size` is published, and the
additive `register_scalar_flags` field is appended last so it cannot shift
earlier fields — pinned by const-assertions and by a `legacy_api_ext.c`
fixture.

**Extension free/alloc accounting — airtight.** Every error path frees
exactly what it allocated; the tests assert `allocations == frees` and
`live() == 0` across the error, NULL-with-buffer, invalid-UTF-8,
NULL-pointer-with-length and zero-length paths. `host_alloc` → `malloc` and
`host_free` → `free` are the same allocator; the Go and Python bindings
materialize results before freeing.

---

## Test coverage gaps

The bugs in H-3, H-4, H-5 and M-1 cluster in exactly the areas the test suite
does not cover.

| Gap | Suggested location |
|---|---|
| `CREATE INDEX` — no test exists anywhere in the engine crate | `crates/pinkdb/tests/` |
| Multi-statement batch that creates the first user **and** grants ADMIN (H-1); `tests/rbac.rs:108` only tests that the *next* statement is refused | `crates/pinkdb-server/tests/rbac.rs` |
| `--transport async` combined with `--tls-cert`/`--tls-key` (H-2); `tests/tls.rs:606` only tests the default sync transport | `crates/pinkdb-server/tests/tls.rs` |
| Write latency under concurrent unauthenticated logins (H-6) | `crates/pinkdb-server/tests/rbac.rs` |
| Timing equalization between an unknown user and a wrong password (M-2); `tests/rbac.rs:83` only asserts the error *text* is identical | `crates/pinkdb-server/tests/rbac.rs` |
| An authenticated session after the last user is dropped (M-1) | `crates/pinkdb-server/tests/rbac.rs` |
| `COPY` and table-function privilege behaviour over the wire (M-3, M-4) | `crates/pinkdb-server/tests/rbac.rs` |
| WAL replay must not `dlopen` a recorded path (C-1) | `crates/pinkdb/tests/` |
| Durable artifact file modes (M-5) | `crates/pinkdb/tests/encryption.rs` |
| Result-size / statement-timeout limits (M-6) | `crates/pinkdb-server/tests/server.rs` |

---

## Recommended remediation order

1. **C-1, C-2, H-7** — stop the extension-loading RCE. Default
   `allow_sql_load_extension` to `false`; expose a C-ABI switch; never
   auto-`dlopen` a path recovered from a file; confine extension paths to a
   configured root.
2. **H-1, M-1** — close the authentication and privilege-escalation paths.
   Evaluate the auth gate per statement; move the "empty catalog ⇒ skip"
   shortcut into `enforce()` so it only applies to `principal == None`.
3. **H-2** — refuse to start the async transport with TLS flags rather than
   silently serving plaintext.
4. **H-3, H-4, H-5, L-1** — fix the index lifecycle. This is a small, contained
   change (maintain or invalidate on mutation; canonicalize the probe key)
   that removes a silent-wrong-answer class and a reachable panic.
5. **H-6, M-2, M-6, M-11** — bound the resource paths: release the store lock
   before verifying passwords, equalize the login timing, add a statement
   timeout and a result cap, bound the reject-thread pool.
6. **M-5, M-3, M-4, M-7** — tighten the data-at-rest and filesystem
   boundaries: `0600` on durable artifacts, confine `COPY`/table-function
   paths, bind lake manifests to a lake id and version.
7. **M-8, M-9, M-10** — enforce the password policy, escape identifiers in
   the CLI, and sign the extension repository.
8. **Low / informational** — the footgun and consistency items.

---

## Appendix A — reproduced evidence

Each item below was run against a build of the current tree
(`cargo build --release`) during this review. All probe artifacts were
removed afterwards; the working tree is unchanged.

### A.1 WAL replay auto-loads an extension path

A C extension exporting a valid ABI-v1 surface
(`pinkdb_extension_abi_version` → `1`, `pinkdb_extension_name`,
`pinkdb_extension_init` → `0`) and an `__attribute__((constructor))` that
appends to a marker file:

```
LOAD EXTENSION -> Ok(1)
marker exists after load: true
MARKER AFTER REOPEN: "EXECUTED\n"      <-- written by the reopen, with no LOAD in sight
```

The reopen executed the constructor recorded in the WAL. See C-1.

### A.2 Unauthenticated batch creates and promotes an admin

Against a fresh server with an empty user catalog, one unauthenticated
`Execute` frame:

```
unauth batch -> Done("evil" created), Done("ADMIN TO evil"), BatchDone(2)
login evil/p  -> LoggedIn("evil")
SELECT 1      -> ResultSet(1)
CREATE USER mallory PASSWORD 'x'  -> Done("mallory" created)
```

See H-1.

### A.3 Async transport serves plaintext despite TLS flags

```
$ pinkdb-server --host 127.0.0.1 --port 5607 --transport async \
      --tls-cert cert.pem --tls-key key.pem
pinkdb-server v0.1.0 listening on 127.0.0.1:5607 (in-memory, transport: async)

plaintext Hello -> 10 b'\x05\x00\x00\x000.1.0\t\x00\x00\x00'
```

A plaintext frame was answered by a plaintext listener. See H-2.

### A.4 Stale index returns the wrong row

```
CREATE TABLE accounts (id INTEGER, owner VARCHAR);
INSERT INTO accounts VALUES (1, 'alice'), (2, 'bob');
CREATE INDEX ix ON accounts(id);
UPDATE accounts SET id = 99 WHERE id = 1;

SELECT * FROM accounts WHERE id = 1;   -- id | owner  ->  99 | alice
SELECT * FROM accounts WHERE id = 99;  -- 0 rows
```

See H-3.

### A.5 DELETE after CREATE INDEX panics

```
CREATE TABLE t (id INTEGER, s VARCHAR);
INSERT INTO t VALUES (1, 'a'), (2, 'b');
CREATE INDEX ix ON t(id);
DELETE FROM t WHERE id = 2;
SELECT * FROM t WHERE id = 2;

thread 'main' panicked at crates/pinkdb/src/vector.rs:321:9:
vector index out of bounds
```

See H-4.

### A.6 Type-tagged index key returns zero rows

```
WITH index:     SELECT * FROM t WHERE id = 5;   -- 0 rows
WITHOUT index:  SELECT * FROM t WHERE id = 5;   -- 1 row (id = 5)
```

See H-5.

### A.7 Empty RBAC catalog promotes a live session

```
bootstrap        -> Done(alice), Done(root), Done(ADMIN TO root)
alice create table  -> error: permission denied: user "alice" needs the CREATE privilege on data
root drops all users -> Done(alice dropped), Done(root dropped)
alice CREATE USER mallory PASSWORD 'x' -> Done("mallory" created)
```

See M-1.

### A.8 Login timing enumerates user names

```
== EXISTING user, wrong password ==
     35.52 ms   22.87 ms   17.29 ms
== NON-EXISTENT user ==
      0.09 ms    0.09 ms    0.06 ms
```

Identical error text in both cases; ~200-500x timing difference. See M-2.

### A.9 Durable artifacts are world-readable and the WAL holds plaintext SQL

```
audit.pink.wal    mode=644
audit.pink.lock   mode=644
WAL printable content: PINKWAL1X6h #J8v4maintCREATE TABLE t (a INTEGER)7eb9maintINSERT INTO t VALUES (42)
```

See M-5.

---

## Appendix B — scope

**Reviewed in full:**
`crates/pinkdb/src/{db,storage,wal,persist,rbac,crypto,ext,table_source,csv,
filefmt,optimizer,plan,procedure,prepared,attached,mv,js,types,vector}.rs`,
`crates/pinkdb/src/sql/{mod,ast,binder,peg_parser,split,keyword}.rs`,
`crates/pinkdb/src/exec/{mod,expr,aggregate,window}.rs`,
`crates/pinkdb-server/src/{protocol,server,async_server,client,tls,lib}.rs`,
`crates/pinkdb-server/src/bin/pinkdb-server.rs`,
`crates/pinkdb-ffi/src/lib.rs`,
`crates/pinkdb-ext-loader/src/{lib,abi,host,function,linked}.rs`,
`crates/pinkdb-cli/src/{main,lake}.rs` and `src/repo/*`,
`crates/pink-lake/src/{storage,catalog,attach,local,inlined,cdc,incremental,
zone,delete_file,vortex_file,s3/*}.rs` and `src/crypto.rs`,
`crates/pinkdb-js/src/lib.rs`, `bindings/python/**`, `bindings/go/**`,
`.github/workflows/*`, `ext-repo/**`.

**Not covered:** `website/` (static docs), `bench/` (benchmark harnesses), the
generated `target/` tree, and the `ext-repo`/`ext-sdk` upstream mirrors beyond
the parts referenced above. The `pinkdb-js` WASM engine is a contract-only
stub in this tree — `engine_available()` returns `Err` and
`CREATE PROCEDURE … LANGUAGE JAVASCRIPT` is refused — so there is no WASM
sandbox to assess today. `JS_SANDBOX_RULES` (`ext-loader/src/linked.rs:700-711`)
is explicitly labelled "requirements, not enforcement"; when the engine lands,
`DENIED_INTRINSICS` and the statement/step limits must be enforced by the
engine binding rather than documented only.
