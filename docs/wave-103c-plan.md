# Wave 103c — the JSON catalog's last traces (plan)

Started 2026-10-26, follows wave 103 (catalog is always a database).
Wave 103 dropped the JSON layout (`catalog.json`, `snapshots/<v>.json`,
`commits/<v>.json`, `highest.json`) but the name survived in errors, doc
comments and fixtures. This wave removes it: every catalog error names the
lake's real catalog location.

## Design

- `Lake` keeps the catalog location as given at open (`pink:<path>`,
  `sqlite:<path>`, or the redacted Postgres URL) in one `catalog: String`
  field, exposed as `pub fn catalog_label(&self) -> &str`. `LakeAttachment`
  carries the same label, passed down when the attachment is built, so
  time-travel views inherit it.
- `manifest_for(store, catalog_label, version)` takes the label instead of
  the data root and returns it as the attribution `path` for the catalog it
  loaded (`<label> (snapshot <v>)` for an old snapshot). Downstream
  `split_key`/`format_for`/`Corrupt { path }` sites therefore name the
  catalog without changing their signatures.
- Error `path` fields stay `PathBuf`: a `PathBuf` holds the label string
  verbatim and `display()` prints it unchanged, so the type change the brief
  allows is not needed and would touch ~127 constructions for no output
  difference.
- The metadata store names its own catalog (redacted URL, not the raw one,
  which can contain a password) in errors raised below the lake handle.
- `CATALOG_FILE` is deleted; the one directory check that used it
  (`ensure_data_location_free`) drops it.
- Tests assert the real label; s3 unit tests use `object.bin` as an
  arbitrary key. One corrupt-catalog test per backend: `pink:`, `sqlite:`
  (feature `catalog-sqlite`), Postgres (env-gated suite), asserting the
  location is named and no `catalog.json` appears, and the password is
  redacted for Postgres.

No behaviour change beyond messages; no new dependencies.

## Gates

The gate list from the brief (fmt, clippy across feature sets, pink-lake,
pink-lake-parquet, pinkdb-cli, workspace tests), then CHANGELOG, ROADMAP
row 103c, and the website regeneration.
