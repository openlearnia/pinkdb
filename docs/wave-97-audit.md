# Wave 97 — stale-row audit: 40h / 40i / 40j / 96

Audited 2026-10-24 against the tree at `736e970` (wave 104). Each item was
re-checked in the current code, not against the roadmap's own claims: a
verdict of **done** below names the code and a test that exist in the tree
today, and every citation was grepped. No implementation was needed — every
surviving item was already shipped, obsolete, or belongs to a later row.

| row | outcome |
|---|---|
| 40h | all five items closed: four shipped (waves 45h, 63, 64, 67), the encode panic guard is obsolete |
| 40i | all five closed: signatures (security pass 3) and Linux artifacts (wave 90) shipped; SQL `INSTALL` → 93, PyPI wheel + Go module → 92 |
| 40j | all four shipped (waves 39, 45n, 102) |
| 96 | 96a superseded by wave 103; 96b (stateless engine metadata) is still open — row re-scoped and left `queued` |

## 40h — lake

| item | verdict | evidence |
|---|---|---|
| incremental saves | done (wave 63) | `crates/pink-lake/src/incremental.rs:1300` `Lake::commit`, `:1319` `commit_with_settings`, `:1406` `commit_from_tables`; `crates/pink-lake/tests/incremental.rs:158` `commit_appends_without_rewriting_and_the_manifest_records_the_ranges`, `commit_from_tables_diffs_inserts_updates_and_deletes_exactly`, `racing_commits_retry_and_rebase`; `crates/pinkdb-cli/tests/lake.rs` `writable_attach_commit_appends_without_rewriting`. Survived the wave-103 rewrite: the delta is now `ducklake_data_file`/`ducklake_delete_file` rows rather than `commits/<v>.json`. |
| `.lake gc` | done — shipped as `.lake cleanup` + `.lake orphans` (wave 67); no `gc` command ever existed | `crates/pinkdb-cli/src/lake.rs:403-404` dispatch, `:2946` `cmd_cleanup`; `crates/pink-lake/src/maintenance.rs:1176` `cleanup_old_files`, `:1184` `delete_orphaned_files`, `:1107` `expire_snapshots`; `crates/pink-lake/tests/maintenance.rs:988` `cleanup_dry_run_deletes_nothing_and_confirm_deletes_once`, `:1197` `list_files_reports_rows_sizes_partitions_and_unreferenced_objects`, `:789` `expire_snapshots_dry_run_then_versions_with_clean_time_travel_errors`. |
| `lake_scan` SQL access | obsolete — never implemented (`git log --all -S lake_scan` finds only ROADMAP/website prose); the capability shipped as the `AT` clause (wave 64) | `crates/pinkdb/src/attached.rs:44` `AtSpec::{Version,Timestamp}`; grammar `crates/pinkdb/src/sql/peg_parser.rs:1702-1707` (`at_clause`/`at_spec`), tests `:4638+`; `crates/pinkdb-cli/tests/lake.rs:1350` `time_travel_queries_and_errors_on_an_attached_lake`, `:2151` `writable_attach_schema_evolution_publishes_and_time_travels`. `.lake versions` / `.lake snapshots` / `.lake open <catalog> [version]` cover the CLI half. |
| bulk typed decode | done (wave 45h, rebuilt for `PINKDB06` in wave 102) | `crates/pinkdb/src/persist.rs:90-99` (module doc: no per-row `Value` round trip), `:2031` `encode_chunk_typed`, `:2689` `decode_chunk_typed` → `Vector::from_raw`, `:3878` `decode_projected_columns`; lake read path `crates/pink-lake/src/format.rs:735`. |
| encode panic guard | obsolete — the failure mode it guarded (a truncated store from a mid-encode panic) is closed by atomic publication and by per-statement unwind containment | `crates/pinkdb/src/persist.rs:3280` `write_atomic` (unique temp sibling → fsync → rename → parent fsync; `save`/`save_sync` at `:3221`/`:3249`), `crates/pink-lake/src/local.rs:91` `write_atomic` for catalog and data objects; `crates/pinkdb-server/src/server.rs:1175` / `:1365` `catch_unwind` per statement (plain and bound, documented at `:1019`). The only encode/decode guard that ever existed was the Vortex decoder guard, deleted with wave 104 (`736e970`). Judgement call: an encoder panic still unwinds a CLI process, but a `catch_unwind` inside the encoder would turn a bug into a bogus error, so none was added. |

## 40i — extension trust and distribution

| item | verdict | evidence |
|---|---|---|
| Ed25519 artifact signatures | done (security pass 3, M-10, commit `b5a7b16`) | `crates/pinkdb-cli/src/repo/signature.rs:33` `PINNED_PUBKEY_PEM`, `:171` `Verifier`, `:227` `verify`; enforced before any artifact request in `crates/pinkdb-cli/src/repo/client.rs:116`; `crates/pinkdb-cli/tests/repo.rs:554` `unsigned_catalog_is_refused_before_any_artifact_request`, `:612` `tampered_catalog_signature_is_refused_before_any_artifact_request`, `:657` `signature_with_an_unknown_key_id_is_refused_against_the_pinned_key`, `:681` `tampered_artifact_is_refused_and_never_loaded`; signing side `ext-repo/verify.sh`, `ext-repo/publish.py`, `ext-repo/tests/test_signing.py`. |
| SQL `INSTALL` | still open → merged into row 93 | No `Install` in `crates/pinkdb/src/sql/keyword.rs` (only `Load`), no AST node, no grammar rule; only the CLI dot-command exists (`crates/pinkdb-cli/src/main.rs:1007`). |
| Linux artifacts | done (wave 90) | `scripts/release/matrix.py:79` `PLATFORMS` with `linux-x86_64` (`:81`) and `linux-aarch64` (`:88`); `.github/workflows/release.yml:89` builds the matrix; the `v0.1.0` release built and smoke-tested all 36 artifacts. |
| Python wheel | still open → merged into row 92 | `bindings/python/pyproject.toml` packages a stdlib-ctypes shim that needs a locally built `libpinkdb_ffi`; no `maturin`/`python -m build`/`twine`/PyPI step anywhere in `scripts/` or `.github/workflows/`; CI only runs `python3 -m unittest discover -s bindings/python/tests` (`.github/workflows/ci.yml:350`). |
| standalone Go module | still open → merged into row 92 | `bindings/go/go.mod:1` declares `github.com/openlearnia/pinkdb/bindings/go`, but `bindings/go/pinkdb.go:24-25` links `-L${SRCDIR}/../../target/release`, which cannot resolve from a module cache, so `go get` cannot work; no `bindings/go/v*` tag and no proxy publish. |

Distribution gap found while auditing item 1 and merged into row 93: the
`repo` feature is opt-in (`crates/pinkdb-cli/Cargo.toml:77`) and no release
variant enables it (`scripts/release/matrix.py:33-38`, `scripts/release/package.sh:148-155`),
so `.install` — and with it the signed-catalog verification — exists only in
source builds, while the website advertises `.install <name>`
(`website/src/pages/extensions.html`).

## 40j — test depth

| item | verdict | evidence |
|---|---|---|
| window / plan unit tests | done | 8 in-crate window tests: `crates/pinkdb/src/exec/mod.rs:7772` `filter_above_window_keeps_every_input_row`, `:7854` `window_agg_frames`, `:8367` `window_preceding_end_before_partition_is_empty`, `:8415` `window_following_bounds_and_float_minmax`, `:8491` `window_agg_single_partition_is_linear`, `:8527` `window_over_agg_and_rejections`, `:8570` `window_lag_lead`, `:8669` `window_ntile_first_last_value`; 11 plan-tree tests in `crates/pinkdb/src/optimizer.rs:2000` `mod tests` (e.g. `pushdown_splits_join_conjuncts`, `left_join_pushdown_is_kind_aware`, `having_on_aggregates_and_constants_stays_above`, `optimization_is_deterministic`) plus `crates/pinkdb/src/plan.rs:2303` `scan_filter_tests`. Residual: `crates/pinkdb/src/exec/window.rs` still has no `#[cfg(test)] mod`; its helpers are exercised through the SQL-level tests above, which is what the item asked for. |
| WAL fuzz | done (wave 45n, commit `6d227ec`) | `crates/pinkdb/src/wal.rs:7870` `mod wave45n`: `:8100` `fuzz_wal_truncation_at_every_offset` (every byte, reopen, committed prefix must survive), `:8122` `fuzz_wal_byte_flips_never_yield_wrong_values`, `:8147` `fuzz_epoch_region_and_commit_slots_are_hints_only`, `:8240` `fuzz_v2_upgrade_temp_is_ignored_and_crash_reruns`, `:8319` `fuzz_checkpoint_tmp_windows`; encrypted variant `:7783` `fuzz_encrypted_wal_truncation_and_byte_flips`. 8 `fuzz_*` functions, exhaustive per offset rather than coverage-guided. |
| persist property tests | done (hand-rolled, wave 102) | `crates/pinkdb/src/persist.rs` (62 `#[test]`): `:6464` `roundtrip_corpora_are_bit_exact` (9 dtypes × sizes 0…4672 × null densities), `:7233` `seeded_encode_decode_loop_is_bit_exact` (2000 seeded cases), `:6127` `truncation_at_every_offset_errors`, `:6060` `per_chunk_corruption_is_detected`, `:7203` `forged_counts_are_capped_and_rejected`, `:6028` `corrupt_files_error_without_panic`. No `proptest`/`quickcheck` dependency was added — the properties are loops over generated inputs, so the default build stays dependency-free. |
| duckdiff gated in CI | done (since wave 39) | `.github/workflows/ci.yml:288` job `duckdiff`, `needs: release-cli`, DuckDB 2.0-alpha pinned and SHA-256 verified (`:312-322`), `python3 bench/duckdiff.py` (`:324-325`) which exits non-zero on a failure (`bench/duckdiff.py:1285`); no `continue-on-error` and no path filter — it runs on every push and PR. `--big` (1M-row battery) stays local-only. |

## 96 — catalog metadata

| item | verdict | evidence |
|---|---|---|
| 96a — pluggable lake catalogs (JSON manifests in SQLite/Postgres rows, `CatalogStore` seam, `migrate-catalog`) | superseded by wave 103 | The JSON layout, the blob tables and `migrate-catalog` were deleted in `182e1db`; a catalog is now a DuckLake v1.1-schema database — `pink:`, `sqlite:` or `postgres://` — with `ducklake_*` tables (`crates/pink-lake/src/metadata/store.rs:4`, `:940`), and `crates/pink-lake/src/catalog_store/mod.rs:1-15` is only the backend constructor now. |
| 96b — stateless engine metadata (`pinkdb-server --catalog postgres://… --data s3://…`, engine metadata in the catalog, one transaction publishing lake + metadata) | still open | `pinkdb-server` accepts only `--file` / in-memory (`crates/pinkdb-server/src/bin/pinkdb-server.rs:145-157`); engine metadata (databases, tables, views, RBAC, settings) still lives inside the `.pink` file + WAL, so a store cannot be shared across machines or run without local state. Needs a re-design against 103/104 first: the `ducklake_*` schema is fixed by DuckDB interop, so the engine's own catalog must be either a separate catalog database or ordinary pinkdb tables beside the lake tables. |

## Doc drift found (out of scope, not edited)

- `docs/GAPS.md:72-82` still lists the pre-audit gaps ("every save rewrites
  every table, and there is no garbage collection", "decode is per-row", "no
  artifact signatures", "the Python binding has no wheel", "no WAL decoder
  fuzz", "duckdiff runs outside the gated jobs"); its last edit (`b1e3dd0`,
  2026-10-21) predates waves 90/102/103.
- `crates/pink-lake/src/lib.rs:17-330` and `incremental.rs:1-30` still
  describe `catalog.json` / `snapshots/<v>.json` / `commits/<v>.json`,
  deleted in wave 103.
- `crates/pink-lake/src/lib.rs:398` still promises "a caught decoder panic" —
  that guard was Vortex-only and went with wave 104; `LakeError::CatalogMoved`
  (`crates/pink-lake/src/lib.rs:522-528`) and
  `crates/pinkdb-cli/tests/postgres_catalog.rs:3,142` still name the deleted
  `migrate-catalog`.
- Row 90's own text says "signing (Ed25519) stays a follow-up" and row 39's
  says "signing is a documented hook only" although signing landed before the
  `v0.1.0` release.
