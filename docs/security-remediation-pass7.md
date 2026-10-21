# Security remediation pass 7 — terminal injection, crypto nonce width, WAL AAD, loader/FFI hardening

This is the closing pass for `docs/security-review-2026-10-19.md`: M-9, L-4…L-10,
the actionable informational items, and the final status matrix for every
finding (C-1…L-14). Earlier passes are identified by their commit so the matrix
can be checked against `git log`.

No public C ABI version changed. The only intentional public Rust API change is
the L-6 signature (`arg_type_at` now returns `Option<ExtType>`); see “Design
decisions”.

## M-9 — one terminal sanitizer for every rendered name

`pinkdb::text::sanitize_terminal` (re-exported as `pinkdb::sanitize_terminal`)
is now the single renderer-side sanitizer. It preserves valid UTF-8 (including
non-ASCII letters such as `Chloé` and `日本語`) and:

- maps `\n` and `\r` to a space (a raw newline would break the result box),
- keeps a tab (the historical cell behaviour), and
- renders every C0/C1 control — notably ESC, the introducer of every
  CSI/OSC/DCS sequence — as `\u{..}`,
- renders invisible Unicode format characters (bidi overrides/isolates,
  zero-width joiners/marks, BOM) the same way.

It is applied to:

- the CLI’s result renderer: every name-carrying `QueryResult` variant, the
  `Selected` column names (before width computation, so the box stays aligned
  on the sanitized text), and VARCHAR cells (the previous `sanitize_cell`
  behaviour);
- `cmd_databases`, `cmd_tables`, `cmd_schema`, and `.installed` metadata;
- CLI error text (a hostile server can put ESC in an `Error` message);
- the `.lake` renderer: table keys, change-log values and change types,
  partition keys/values, snapshot author/message/change lists, catalog option
  names/values, file paths and table metadata;
- the remote path implicitly: remote results are reconstructed by
  `pinkdb-server::Client` and rendered by the CLI’s one renderer, so a hostile
  server’s column/object names go through the same sanitizer.

The unit test `every_result_variant_sanitizes_its_names` covers every
`QueryResult` variant; `sanitized_column_names_keep_the_box_aligned` pins the
width behaviour; `terminal_safety.rs` covers local quoted identifiers,
legitimate UTF-8, and a fake server sending ESC/OSC column and object names.

## L-4 — 96-bit nonce space from a 128-bit random base

Both container formats gained a new version:

| format | old | new | nonce |
|---|---|---|---|
| engine checkpoint / WAL | container v1–v3 | v4 | `truncate96(SHA-256("pinkdb-nonce-v4" \|\| domain \|\| base128 \|\| counter))` |
| lake artifact | v1 | v2 | same construction (domain is the lake domain byte) |

Every container mints a fresh 128-bit random `nonce_base` (header field; 16
bytes). The key-check uses counter 0 and payload units use 1, 2, …, exactly as
before. The birthday bound for the fixed-plaintext key-check is now
`P ≈ n²/2⁹⁷` instead of `n²/2⁶⁵`. The nonce does not reveal the counter in the
clear, unlike the legacy `prefix || counter` construction.

**Old-format policy: read-old / write-new.** Versions 1–3 (engine) and 1
(lake) still open with the legacy nonce construction. WALs are rebuilt in the
current version at every keyed open (the existing wave-22 rule); checkpoints
are upgraded on their next save; lake artifacts are rewritten by the next
save/commit. The version word is authenticated (it is part of the AAD), so a
version flip cannot select a different nonce construction. A newer version is
a clean refusal.

Tests: `v4_nonce_base_is_128_random_bits_and_drives_a_derived_nonce`,
`legacy_v2_checkpoint_is_read_and_upgraded_on_save`,
`v2_nonce_base_is_128_bits_and_domain_separated`,
`legacy_v1_lake_container_is_read_and_writes_are_v2`, plus the existing
round-trip/tamper/truncation, encrypted WAL/checkpoint, multiprocess refresh and
lake encryption suites.

## L-5 — the encrypted WAL publish block is authenticated

Current WAL containers (v4) use a 144-byte publish block:

```text
meta:     wal_id [16] | generation u64 | block version u16 | reserved u16   (28)
meta tag: nonce [12] | tag [16]                                           (28)
slot A:   seq u64 | value u64 | nonce [12] | tag [16]                     (44)
slot B:   seq u64 | value u64 | nonce [12] | tag [16]                     (44)
```

The static meta is an AEAD unit whose AAD includes the WAL header AAD base, and
its tag is verified on every keyed recovery. The slots mutate on each publish,
so each slot is its own AEAD unit with a fresh random 96-bit nonce; a slot
whose tag does not verify is ignored as a hint (the frames are authoritative,
and a torn slot write during a crash must not brick recovery). Tampering with
`wal_id`/generation is a hard recovery error.

**Old-format policy:** v2/v3 blocks (`wal_id | CRC | slots`) are still parsed
long enough to upgrade the file, then rewritten as v4. Every keyed open already
rebuilds a WAL with a fresh incarnation, so the old block never survives a
keyed open.

Tests: `tampered_publish_block_identity_is_rejected_on_recovery`; the existing
`fuzz_epoch_region_and_commit_slots_are_hints_only` (slot corruption is
ignored) and the encrypted WAL/checkpoint/multiprocess suites (round-trips and
refresh paths).

## L-6 — `arg_type_at` is total for zero-argument functions

`ExtFunction::arg_type_at` and `LinkedFunction::arg_type_at` now return
`Option<ExtType>`; a zero-argument declaration returns `None` instead of
computing `len - 1` (debug panic / release OOB). The executor reports a clean
internal error if a call supplies an argument for a zero-argument function; the
linked call path checks the same before comparing types.

Tests: the linked zero-argument test now asserts `None` for index 0 and
`usize::MAX`; the math extension asserts `pi()`/`random()` have no argument
type; both the native (`ext_tick`) and linked zero-argument paths remain in the
suites.

## L-7 — FFI bound count validation before allocation

`pinkdb_execute_prepared` validates `value_count` **before**
`Vec::with_capacity`: a hard ceiling of 65 536 and an exact match against the
prepared statement’s `parameter_count`. The error text is the engine’s
pre-existing wording, so the Python and Go bindings’ exact-message assertions
are unchanged.

Test: `absurd_prepared_value_count_is_rejected_before_allocation` passes
`1 << 62` values through the C entry point (no abort), then exercises a normal
count and the handle’s continued usability.

## L-8 — heap-owned collector and enforced init window

The loader allocates `Collector` on the heap (`Arc<Collector>`), stores it in
`LoadedExtension` and the process registry, and sets `init_complete` as soon as
`pinkdb_extension_init` returns (success or failure). A later `register_scalar`
sees the flag and returns a deterministic error instead of writing into a dead
stack frame. `into_functions` still yields the same functions (cloning when the
registry shares the collector).

Tests: `late_registration_after_init_is_rejected_without_ub` uses a C fixture
that keeps the host context and calls `register_scalar` after init; the normal
load/registry/ownership suites pin function ownership and no leaks.

## L-9 — bounded VARCHAR copy and type-gated free

The host now:

- decodes the type tag first; a `text_ptr`/`text_len` under a non-VARCHAR tag
  is ignored and **never freed**;
- rejects a VARCHAR result whose `text_len` exceeds 64 MiB **before** forming
  a slice or freeing the pointer;
- frees a non-NULL text pointer only when the tag decodes to VARCHAR (including
  the NULL-result path, preserving the existing allocation accounting).

Tests: `malformed_extension_varchar_results_are_rejected_safely` (a `SIZE_MAX`
length and a non-VARCHAR pointer to a static buffer; neither is read or freed),
plus the existing ownership accounting, NULL, UTF-8, and normal VARCHAR
round-trips.

## L-10 — `PinkValue` layout pin

`crates/pinkdb-ffi/src/lib.rs` now has `offset_of!`/`size_of` const assertions
for every `PinkValue` field (64-bit and 32-bit layouts), and
`tests/header_sync.rs` parses the `pink_value` typedef, lays it out under the C
`repr(C)` rules, and compares every offset and the total size with Rust. A
field reorder now fails to compile; a header drift fails `cargo test -p
pinkdb-ffi`.

## Informational items

1. **Per-frame idle deadline (sync transport).** `FrameDeadline` wraps the
   socket/TLS stream: before every underlying read it shrinks `SO_RCVTIMEO` to
   the time remaining in the frame, and `begin_frame` resets the deadline only
   between frames. A slow-drip client is disconnected; a normal client is
   unaffected. Test: `slow_drip_client_is_disconnected_at_the_frame_deadline`.
2. **Ambient `PINKDB_PASSWORD`.** The CLI consults the variable only when
   `--host` is present. Test: `ambient_password_does_not_break_a_local_session`
   (env set, no host, REPL runs) plus the existing remote env-login test.
3. **`.lake` maintenance key bypass.** The four subcommands already went
   through `open_lake` after the pass-4 commit; this pass extends the keyed
   regression to `inlining`, `flush` and `add_data_files` (keyed success,
   keyless clean refusal of an encrypted lake).
4. **CI installer and Docker tags.** The DuckDB alpha is downloaded from a
   pinned staging URL (`d54cbbe3df/v2.0.0-alpha49225`) and verified with a
   SHA-256 before extraction; no remote installer is piped into a shell. The
   lake integration images are pinned by digest (pass 4). `actionlint`,
   PyYAML and `bash -n` pass.
5. **`--tls-insecure`.** No code change: it remains a documented, loudly
   warned footgun (see residual risks).

## Final matrix

“Pass” means the commit that fixed the finding; pass 7 is this worktree.

| Finding | Status |
|---|---|
| C-1 | Fixed — pass 2 (`d592cfc`) |
| C-2 | Fixed — pass 2 (`d592cfc`) |
| H-1 | Fixed — pass 3 (`d886fde`) |
| H-2 | Fixed — pass 3 (`9384a42`): async+TLS refuses to start |
| H-3 | Fixed — pass 1 (`e766ba0`) |
| H-4 | Fixed — pass 1 (`e766ba0`) |
| H-5 | Fixed — pass 1 (`e766ba0`) |
| H-6 | Fixed — pass 5 (`87d464f`) |
| H-7 | Fixed — pass 2 (`d592cfc`) |
| M-1 | Fixed — pass 3 (`d886fde`) |
| M-2 | Fixed — pass 5 (`87d464f`) |
| M-3 | Fixed — pass 6 (`1c32ae5`) |
| M-4 | Fixed — pass 6 (`1c32ae5`) |
| M-5 | Fixed — pass 6 (`1c32ae5`) |
| M-6 | Fixed — pass 5 (`87d464f`) |
| M-7 | Fixed — pass 6 (`1c32ae5`) |
| M-8 | Fixed — pass 5 (`87d464f`) |
| M-9 | **Fixed — pass 7** |
| M-10 | Fixed — pass 5 (`b5a7b16`); residual: single pinned key, no revocation |
| M-11 | Fixed — pass 5 (`87d464f`) |
| M-12 | Fixed — pass 6 (`1c32ae5`) |
| L-1 | Fixed — pass 1 (`e766ba0`) |
| L-2 | Fixed — pass 4 (`4e71d1c`) |
| L-3 | Fixed — pass 4 (`4e71d1c`) |
| L-4 | **Fixed — pass 7** |
| L-5 | **Fixed — pass 7** |
| L-6 | **Fixed — pass 7** (public API: `Option<ExtType>`) |
| L-7 | **Fixed — pass 7** |
| L-8 | **Fixed — pass 7** |
| L-9 | **Fixed — pass 7** |
| L-10 | **Fixed — pass 7** |
| L-11 | Fixed — pass 4 (`4e71d1c`) |
| L-12 | Fixed — pass 4 (`4e71d1c`) |
| L-13 | Fixed — pass 4 (`4e71d1c`) |
| L-14 | Fixed — pass 4 (`4e71d1c`) |
| Role-derived ADMIN inconsistency | Fixed — pass 3 (`d886fde`) |
| Stale startup banner | Fixed — pass 3 (`d886fde`) |
| Per-socket idle timeout | **Fixed — pass 7** (sync transport; per-frame deadline) |
| Unpinned CI installer | **Fixed — pass 7** (pinned artifact + SHA-256) |
| Mutable Docker tags | Fixed — pass 4 (`4e71d1c`, digest pins) |
| Ambient `PINKDB_PASSWORD` | **Fixed — pass 7** |
| `.lake` keyed subcommands | Fixed — pass 4 (`4e71d1c`); regression extended in pass 7 |
| `--tls-insecure` | Documented residual (no code change) |

## Residual risks (by design)

- **`dlopen` TOCTOU (M-10).** The catalog/artifact signature is verified, but
  the library is loaded by path after verification. A local attacker able to
  replace the verified file between the check and `dlopen` wins; closing that
  needs `fexecve`/`dlopenat`-style loading or an in-memory image.
- **Single pinned signing key, no revocation.** Repository signing uses one
  key pinned in the binary/`$PINKDB_HOME`; compromise requires shipping a new
  build. There is no CRL/expiry channel.
- **Environment overrides.** `PINKDB_PASSWORD`, `PINKDB_KEY`,
  `PINKDB_ALLOW_LOAD_EXTENSION`, the S3 safety opt-ins (`PINKDB_S3_ALLOW_HTTP`,
  `PINKDB_S3_ALLOW_DEGRADED`) and retry knobs are operator policy. A process
  that inherits a hostile environment inherits that policy.
- **Async transport does not support TLS.** H-2 makes the combination a
  startup error instead of silent plaintext; TLS remains sync-only.
- **Unsigned local extensions.** `LOAD EXTENSION` is opt-in, confined by the
  loader’s path rules and (optionally) an extension root, but a local library
  placed inside the root is trusted and unsigned.
- **Index maintenance cost.** Indexes are maintained/invalidated on DML and
  rebuilt after column DDL; the rebuild is O(rows) and unbounded by design.
- **Lake rollback under full storage control.** Manifests are bound to
  `(lake_id, version)` and the highest-version marker is protected, but an
  attacker who can rewrite/delete *all* lake objects (including the marker) can
  still present a complete older state. A remote monotonic witness is out of
  scope.
- **Table-function / `COPY TO` path policy.** Privileges and server opt-ins
  are enforced, and `COPY TO` can be root-confined, but a trusted session with
  the opt-in can name any path under its configured root; the policy is
  operator-controlled.
- **Terminal sanitization is renderer-side.** API consumers that print
  `QueryResult` names themselves (e.g. `Debug`) are outside the CLI’s
  protection; the shared helper is public for them to use.
- **Legacy encrypted files.** Old containers remain readable until rewritten
  (WAL at keyed open, checkpoints/lake artifacts on next save), so their
  historical 64-bit-prefix birthday bound applies to those bytes until then.
- **WAL publish slots are hints.** They are authenticated, but deleting/zeroing
  a slot only removes a hint; the frames remain authoritative. This is safe
  but means slot tampering is ignored rather than fatal.
- **`--tls-insecure`** accepts any certificate by design; it is documented and
  loudly warned.

## Gate results (all green)

| Gate | Result |
|---|---|
| `cargo test -p pinkdb` | 693 passed (`689` baseline + new sanitizer/crypto/WAL tests) |
| `cargo test -p pinkdb --features encryption` | 737 passed |
| `cargo test -p pinkdb-server` | 95 passed |
| `… --features async-server` | 115 passed |
| `… --features tls` | 106 passed |
| `… --features tls,async-server` | 126 passed |
| `… --features ui` | 96 passed |
| `… --features ui,tls` | 107 passed |
| `cargo test -p pinkdb-cli` | 81 passed (88 with `client`) |
| `cargo test --workspace` | 1346 passed |
| `cargo test -p pink-lake` | 124 passed |
| `… --features encryption,vortexlake,s3` | 253 passed |
| `cargo test -p pinkdb-arrow` | 30 passed |
| `cargo test -p pinkdb-ffi` | 19 passed (10 unit + 6 C ABI + 3 header-sync) |
| `cargo test -p pinkdb-ext-loader` | 83 passed |
| `python3 bench/duckdiff.py` | 339 passed, 0 failed |
| Python binding suite | 60 passed |
| Go binding suite | `ok` (all tests) |
| wasm default | `cargo build -p pinkdb --target wasm32-unknown-unknown` green |
| rustfmt | 1.97.1 and stable, `--check` green |
| clippy | 1.97.1 and stable workspace `-D warnings`; server `ui,tls,async-server` both green |
| CI checks | `actionlint`, YAML parse, `bash -n` green |

Each server count is one above the task’s baseline because the new
slow-drip regression runs in every server feature combination; the CLI count
is three above baseline for the ambient-password, terminal-safety and lake
unit tests.

## Release size deltas

Measured against a clean `1c32ae5` build with the same release profile and the
same feature set (`pinkdb-cli --features arrow,parquet,httpfs`, default
`pinkdb-server`, `pinkdb-ffi`):

| Artifact | Base | Pass 7 | Delta |
|---|---:|---:|---:|
| engine `libpinkdb.rlib` | 19,056,784 | 19,095,264 | +38,480 (+0.20%) |
| `pinkdb-server` | 3,536,208 | 3,537,664 | +1,456 (+0.041%) |
| `pinkdb` CLI | 10,380,672 | 10,381,840 | +1,168 (+0.011%) |
| `libpinkdb_ffi.dylib` | 3,318,832 | 3,321,056 | +2,224 (+0.067%) |

## Intentional API/format changes

- `ExtFunction::arg_type_at` and `LinkedFunction::arg_type_at` return
  `Option<ExtType>` (L-6). This is the only public Rust API signature change.
- On-disk container version bumps: engine checkpoint/WAL v4, lake container
  v2. Old versions are read; new writes use the new versions. The C ABI
  version stays 1, and `PinkValue`’s layout/ABI is unchanged.
- The `pinkdb_execute_prepared` arity errors keep their previous text but are
  now produced before allocation.
