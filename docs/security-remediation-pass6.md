# Security remediation pass 6 — filesystem writes, durable modes, lake binding

This document records the design choices for the pass-6 fixes derived from
`docs/security-review-2026-10-19.md` (M-3, M-4, M-5, M-7, M-12 and Appendix
A.9). It is the detailed companion to the short README notes.

## M-3 — `COPY … TO` is ADMIN-only and server opt-in

### Privilege choice

`COPY … TO` now requires **`ADMIN`** in an authenticated session. A dedicated
filesystem-write privilege was considered and rejected: it would add a new
grant keyword, a new persisted privilege tag in the `.pink` RBAC section, and
a new WAL/DDL surface, i.e. a `.pink` persistence-format change. The task
explicitly prefers ADMIN when the dedicated privilege cannot be added without
that bump. `SELECT` never authorizes a write.

The check lives in `rbac::check_statement` (`LogicalPlan::CopyTo`), so the
per-statement auth gate and the prepared-statement path both apply. Embedded
and trusted sessions (`principal == None`) are unchanged.

### Server policy

`PinkDb` carries a `CopyToPolicy`:

| policy | used by | behavior |
|---|---|---|
| `Unrestricted` | embedded/trusted default | writes at the caller's path through the safe-write helper |
| `Disabled` | every network server at bind time | refuses every `COPY TO`, even ADMIN |
| `RootRequired` | opt-in without a root | refuses with "requires a destination root" |
| `Confined(root)` | `--allow-copy-to --copy-root DIR` | destination must stay under `root` |

`Server::bind*` installs `Disabled`; `Server::allow_copy_to(bool)` and
`Server::copy_root(Some(path))` install the opt-in. The same methods exist on
`AsyncServer`, and the binary wires both transports. `--allow-copy-to`
without `--copy-root` is a startup error (exit 2); `--copy-root` must be an
existing directory.

### Safe write

`pinkdb::fsutil::resolve_copy_destination` refuses:

- an empty path or a missing final component;
- any `..` component;
- a parent that does not exist/is not a directory;
- a confined destination whose canonical parent is outside the canonical
  root (this covers absolute escapes and symlinked parents);
- a symlinked parent component (checked from the root down) and a symlinked
  final target.

The writer then uses `pinkdb::write_atomic_private`: a unique `create_new`
temp sibling in the same directory with mode 0600, callback write, flush +
fsync, atomic rename over the target (COPY keeps its historical truncate/
replace semantics), then a best-effort parent fsync. A failure removes the
temp and leaves the previous target intact. The Arrow extension's writer uses
the same helper, as does the in-crate test sink; the engine never calls
`fs::write`/`File::create` at a client-named COPY path.

Table-function path confinement (the optional half of M-4) is **not**
implemented: arguments are source-owned literals (globs, URLs, option names),
and the engine cannot generically distinguish a filesystem path from any
other argument. The ADMIN gate is enforced before execution; path-level
confinement belongs to each table source's own configuration.

## M-4 — table-function scans require ADMIN

`plan::contains_table_function` walks the whole bound statement — CTE bodies,
`UNION`/set-operation arms, subqueries in expressions, materialized-view
queries, `INSERT … SELECT`/`CREATE TABLE AS` sources and `EXPLAIN` — and
`check_statement` denies the statement unless the principal is ADMIN. This
covers `read_parquet`, `read_arrow`, the replacement scan
`FROM 'file.parquet'`, and any future host-registered source. Trusted
(embedded) sessions are unchanged, and `CALL` is already ADMIN-only.

## M-5 — durable artifacts are 0600 (0700 directories)

Every engine-created durable file now goes through
`pinkdb::fsutil`:

- WAL (fresh and recovery-recreated), the `.rewrite`/`.upgrade` temp files,
  the checkpoint `.pink`, the checkpoint `.tmp` staging file, and the lock
  file are created with `OpenOptionsExt::mode(0o600)`;
- `PinkDb::save`/`save_sync` and the atomic `.pink` writer use the same mode;
- pink-lake's `create_unique_tmp` (catalog, snapshots, commits, data files,
  delete files, `lake.json`, `highest.json`) creates 0600 temps;
- new lake directories are created with `DirBuilderExt::mode(0o700)`.
  Existing, user-supplied parent directories are **not** chmodded: only
  directories the lake itself creates are private.

On open, the engine best-effort tightens an existing checkpoint, WAL, lock
file and `.tmp` file whose group/other bits are set. pink-lake does the same
for every file under its root (catalog, identity, highest marker, snapshots,
commits and `data/**`) on `Lake::open`. Permission errors are ignored: the
open never fails because hardening a file the process does not own was not
possible. Non-Unix targets compile with no mode calls.

Deliberate exclusions: a database path the user explicitly saves to is
created 0600, but an existing directory around it is never changed; S3 has no
file modes; a user who copies an artifact elsewhere owns the resulting mode.

## M-7 — lake identity, version binding and rollback detection

### Format

The catalog format is bumped to **6** (`FORMAT_VERSION`/`MAX_FORMAT_VERSION`
= 6; `MIN_FORMAT_VERSION` stays 1 so old fixtures remain readable). A v6
manifest adds `"lake_id": "<32 lowercase hex>"`; v1–v5 manifests never
require it.

A new `lake.json` records the lake's random 128-bit identity
(`{"lake_format":1,"lake_id":"…"}`). It is created once and never replaced.
A new `highest.json` records the monotonic published version
(`{"lake_format":1,"lake_id":"…","version":N}`). It is rewritten after every
successful catalog replacement (all publishing paths funnel through
`replace_catalog`).

### Binding

- `catalog.json` and `snapshots/<v>.json` are sealed under
  `Domain::Manifest` with binding `lake:<lake_id>:<version>` (not `""`).
  Because the two objects are byte-identical by construction, the binding
  cannot name the path; the lake id supplies identity and the version
  supplies freshness.
- `highest.json` is sealed under `Domain::Highest` with the same binding
  shape.
- Reading a catalog parses the authenticated header binding, requires its
  lake id to match `lake.json`, and cross-checks the decrypted
  `(lake_id, version)` against the binding. Reading a snapshot derives the
  expected binding from its file name and the session identity.
- `Lake::open` additionally requires `catalog.version` to equal the version
  of `snapshots/<catalog.version>.json`, and both to carry the same lake id.
- `Lake::open` reads `highest.json` and refuses `catalog.version <
  highest.version` with "rollback detected". A catalog **above** the marker
  is the narrow crash window between the catalog replace and the marker
  write; the open heals the marker forward. A missing marker for a v6 lake is
  a hard error (deleting it would restore the rollback surface).

### Legacy / upgrade policy

- **Plaintext v1–v5** lakes still open; the first save writes a v6 manifest
  and creates `lake.json`/`highest.json`. No in-place rewrite happens on a
  read-only open.
- **Keyed v1–v5** lakes are refused with a clean "pre-v6 encrypted lake"
  error: their manifest containers carry the empty binding and cannot be
  upgraded safely in place. The upgrade path is to load them with a pre-M-7
  build and re-create/re-save under the same key with this build.
- A v6 manifest without a valid `lake_id`, a `lake_id`/identity mismatch, a
  catalog/snapshot version or identity mismatch, a cross-lake container
  substitution, and a catalog below `highest.json` are all clean errors — no
  partial load.

`LakeAttachment` (footer-only attach and time travel) carries the pinned
lake id and refuses a manifest/snapshot from another lake as well.

## M-12 — local `read_tagged` is one fd and includes `(dev, ino)`

`LocalStorage::read_tagged` opens the file once, calls `fstat` on that fd to
build the tag, and reads the bytes from the same fd. The tag is
`file:<length>:<mtime_ns>:<dev>:<ino>` on Unix (`0:0` elsewhere), so a
same-length replacement inside one timestamp tick is distinguishable and the
returned bytes/tag always describe one incarnation. `put_if_match` re-stats
the path and rejects a stale tag before the atomic replace. The S3 backend is
unchanged: bytes and ETag still come from one response.

## Residual risk

- **Full storage control:** an attacker who can rewrite `catalog.json`,
  `highest.json`, `lake.json`, the snapshots and the data files consistently
  can still roll the whole lake back. `highest.json` is an in-band witness;
  it does not defeat an attacker who replaces every object. An external,
  append-only witness (or object-lock/versioning on the bucket) is required
  for that threat.
- **Legacy encrypted lakes** are refused rather than silently upgraded; a
  migration requires a pre-M-7 build.
- **Coarse timestamps** no longer matter for identity because the inode is in
  the tag, but `(dev, ino)` is only meaningful on filesystems that provide
  stable inode numbers (some network filesystems reuse them). The length and
  mtime remain in the tag as additional evidence.
- **COPY overwrite semantics** are intentionally unchanged: `COPY` atomically
  replaces an existing file (never appends). The temp+rename closes
  symlink-following and torn-write windows, but a destination path is still a
  namespace the caller names; use `--copy-root` and OS permissions for
  isolation.
- **Table-function paths** are not confined by the engine; ADMIN is the gate,
  and each source's own path policy applies.
