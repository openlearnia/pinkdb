# Wave 87 — lake format providers (plan)

Started 2026-10-21; **done** the same day. ROADMAP row 87: make the lake's data-file format a
runtime extension seam — `.pink` stays built in; extensions register
additional formats (writer / reader / statistics contract) so the lake's
catalog, commit, attach, S3 and encryption machinery works over them; the
catalog pins the format id and unknown formats refuse cleanly; Parquet is
the first provider; Iceberg reads through the same seam.

## Where things stand

`crates/pink-lake` (~21.6k lines) has a closed `catalog::FileFormat` enum
(`Pink`, `Vortex`, serialized as `"pink"` / `"vortex"`), and the build
feature decides what a lake writes (`Lake::write_format`: `vortexlake` →
Vortex, else `.pink`). About 18 sites in `catalog.rs`, `attach.rs`,
`maintenance.rs` and `lib.rs` match on the enum (write, read, attach
footer/statistics paths, maintenance rewrite/merge, CDC).

## Design

- **`pink_lake::format::LakeFormat`** (public trait, `Send + Sync`): a stable
  lowercase `id()` (the catalog string), the data-file `extension()`,
  `write(&DataTable) -> Result<EncodedFile>` (bytes + the exact per-column
  statistics the catalog block carries, same `SavedTableStats` facts the
  `.pink` writer publishes), `read(&[u8]) -> Result<DataTable>` (or the
  crate's existing decode shape), and an optional
  `footer_stats(&[u8])` for formats whose files carry their own statistics
  (Vortex). Exactness rules for statistics stay where they are today
  (`pink-lake`'s shared gate), applied uniformly to every provider.
- **Registry:** process-global, by id; built-ins `pink` (always) and
  `vortex` (`vortexlake` feature) registered by the crate itself;
  `pink_lake::format::register_lake_format(Arc<dyn LakeFormat>)` for
  extensions; duplicate ids refused.
- **Catalog:** the entry's `format` string is resolved through the registry.
  An id that is not registered is a clean error naming the id and how to
  enable it — at open/attach/read/maintenance — never a panic or a silent
  fallback. Existing catalogs are byte-identical (`"pink"`/`"vortex"`
  unchanged; v1 entries without `format` still mean `pink`).
- **Write format:** still the build default unless the caller chooses one
  (`Lake::set_write_format(id)` or the equivalent option on the save path);
  the chosen id must be registered.
- **Out of scope here:** Iceberg (a separate project; recorded follow-up),
  per-table formats in one lake (wave 88).

## Tasks

- **T1 — the seam (no behavior change):** the trait, registry, built-in
  `pink` and `vortex` providers, every enum match site routed through the
  provider, unknown-id refusal, `set_write_format`, docs. Proof: the whole
  pink-lake + CLI lake feature matrix stays green with unchanged
  expectations, plus new tests: unknown id refused cleanly on open/attach;
  a test-only provider (e.g. a trivial format wrapping `.pink` bytes with
  a different id) registered and used end to end (save → open → attach →
  append/merge maintenance).
- **T2 — Parquet provider:** a Parquet *writer* in `extensions/parquet`
  (all engine types, NULLs, row groups, column statistics) and a bridge
  crate `crates/pink-lake-parquet` implementing `LakeFormat` with the
  existing reader + the new writer; CLI `parquetlake` feature registering
  it and a way to choose it for a save; round-trip and attach tests.
- **T3 — docs** (README lake section, website lake page, CHANGELOG via
  `website/build.py`).

## Rules for delegated agents

Do not commit, push or create branches. Touch only the files your task
names. Run the task's checks and report what changed and the results.

## How it went

- T1 (CommandCode): `format.rs` seam; `FileFormat` became a
  `#[serde(transparent)]` newtype over the id, so catalogs are
  byte-identical; built-in `pink`/`vortex` providers with capability
  methods (`has_footer_stats`, `supports_projection`, `write_streamed`);
  unknown ids refused with the established "corrupt lake metadata" prefix
  plus the id; the whole lake/CLI matrix stayed green with no test edits.
- T2 (CommandCode): Parquet writer (`extensions/parquet/src/write.rs`,
  identity in footer key-value metadata), `crates/pink-lake-parquet`, CLI
  `parquetlake` feature and `.lake format [<id>]`. INTERVAL refused.
- T3 (docs) done here. Iceberg stays a follow-up.
