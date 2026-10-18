# Bug-review remediation — status (2026-10-17)

Remediation of [bug-review-2026-10-17-1827.md](bug-review-2026-10-17-1827.md) on
branch `hardening-review-fixes`. Every "fixed" item has a regression test that
was verified to fail on the pre-fix code (except defensive-only items, noted).

Gates after the work: `cargo test` + `cargo clippy --all-targets -D warnings`
green; duckdiff **171/171** (175/175 with the 1M battery); the encryption,
pink-lake (`s3`/`vortex-format`), CLI (`lake,s3`/`client`/`repo`), Python (37)
and Go suites green.

## P1 — all fixed

| Finding | Status |
|---|---|
| Filter pushed below a Window | fixed |
| Mixed-operand JOIN equi key underflow | fixed |
| `ALTER … ADD COLUMN … DEFAULT` depth/parameter guards | fixed |
| Unbounded plan depth from JOIN/UNION chains | fixed |
| RANGE frame + offset + NULL ordering key | fixed |
| Plaintext WAL mid-file CRC mismatch truncated | fixed |
| `save`/`save_with_key` non-atomic + unguarded | fixed |
| Commit on a legacy (v1–3) head published an unreadable catalog | fixed (refused with a migration hint) |
| `delete_file::decode` overflow / over-allocation | fixed |
| Decrypted plaintext staging copy leaked | fixed (private 0700/0600 dir + sweep) |
| `ExecutePrepared` value-count OOM | fixed (cap) |
| `Rows` decode over-allocation | fixed (cap) |

## P2 — engine / persistence

| Finding | Status |
|---|---|
| `\|\|` rejected on the right of comparison/LIKE/BETWEEN | fixed |
| SELECT alias shadows a same-list input column | fixed |
| Set-op rejects a bare `NULL` arm | fixed |
| `round(decimal, i64::MIN)` negation overflow | fixed |
| `timestamp − timestamp` unchecked subtraction | fixed |
| Unchecked index in `rewrite_through_project` | fixed (defensive) |
| INTERVAL hashing disagrees with comparison order | fixed |
| CASE/COALESCE evaluate unselected arms | fixed (lazy, `DataChunk::gather`) |
| Session DBs in the optimistic-commit conflict set | changed to real DDL names (the documented spurious conflict did not reproduce; code now matches its contract) |
| New/recreated WAL files not directory-fsynced | fixed |
| Unchecked LSN `+1` | fixed |
| `pos + n` overflow in WAL decoders (32-bit) | fixed |
| Unsupported container version treated as torn | fixed |
| `.pink` string offsets truncate to `u32` | fixed — serialization refuses a VARCHAR chunk whose bytes exceed `u32` (the offsets cannot be represented) |

## P2 — lake / S3 / CLI

| Finding | Status |
|---|---|
| `is_container` heuristic can skip encryption | documented (not reachable with current writers); a test now pins the already-sealed marker behavior |
| Attach `COUNT` with overlapping delete files | fixed — attach recomputes the row count from the deduped layout and rejects a mismatch (as `Lake::load` does) |
| Full save resets `schema_version` on an unreadable predecessor | fixed (falls back to the catalog head) |
| Partition error names the partition key, not the table | fixed |
| Container version mismatch reported as `Tampered` | fixed (`UnsupportedContainer`) |
| Sealed manifests bound to nothing | documented (deliberate cost of `catalog == snapshot`) |
| 429/503 retried for conditional PUTs | fixed |
| 400 `InvalidRequest` permanently degrades an endpoint | **verified deliberate** (test-pinned probe signal) |
| `lake_key()` falls back to plaintext | fixed (fail closed) |
| Crafted `binding_len` panic on 32-bit | fixed |
| SigV4 does not collapse internal whitespace | fixed |
| `PINKDB_LAKE_RETRY_BACKOFF=inf` | fixed (finite check) |
| `.lake partition` lacks the transaction guard | fixed |
| `S3Config` `Debug` prints the access-key id | **verified deliberate** (non-secret, test-pinned) |
| Server-provided ETags echoed into `If-Match` unvalidated | fixed (conservative `valid_etag` guard) |

## P2 — server / FFI / bindings / CLI

| Finding | Status |
|---|---|
| Client `read_batch` unbounded | fixed (10M-row cap) |
| Client has no socket timeouts | fixed (`PINKDB_CLIENT_TIMEOUT_MS`, default 5 min) |
| `reject_over_limit` writes on the accept thread | fixed (own thread + 1 s write timeout) |
| `pinkdb_open_memory` not under `catch_unwind` | fixed (open_memory + both `build_result` paths) |
| `is_loopback_host` mis-parses IPv6 brackets | fixed |
| Repo temp file `fs::write` (predictable, follows symlinks) | fixed (`create_new`, unique name) |
| Bindings miss DECIMAL/INTERVAL/`ALTERED_TABLE` | fixed (Python + Go) |
| CLI `.save`/`.open` ignore quoting | fixed |
| CLI prints stored VARCHAR verbatim | fixed (control chars escaped) |
| Repo path validation Unix-only | fixed (Windows reserved names, `:`, trailing dots) |
| Unchecked `next_handle += 1` | fixed |
| Doc drift (protocol version history, `pinkdb.h`, README, FFI error) | fixed |

## P3 — doc/cosmetic

The review lists ~6 P3 items ("cosmetic messages, spec-divergence, doc
comments") without a separate section; the doc-drift set (protocol version
history, `pinkdb.h` handle note, README limits, FFI DECIMAL/INTERVAL error) and
the stale `Cx` subquery doc were corrected here.
