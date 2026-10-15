# pink-lake test fixtures

## `lake_v1/` — a v1 lake (wave-34 writer)

A complete lake directory written by the **wave-34 (pre-Vortex) writer**, before
`format_version: 2` / `"format": "vortex"` existed: `catalog.json` and
`snapshots/*.json` are v1 manifests, data files are `data/<table>/<uuid>.pink`
— exactly the engine's single-table `.pink` persistence bytes.

It was generated with `pink_lake::save_store(&common::fidelity_store(), …)` on
the wave-34 code (the store: all 7 types, NULLs, empty strings, embedded NULs,
multibyte strings, `-0.0`, NaN payloads, i32/i64/DATE/TIMESTAMP extremes, a
multi-chunk table, an empty table, two databases, dotted + unicode table
names). `crates/pink-lake/tests/lake.rs::v1_fixture_lake_still_loads_bit_exactly`
loads it with the current reader and compares values bit-for-bit and each data
file byte-for-byte (load → engine save → same bytes).

The fixture is **immutable**: it pins reader backwards compatibility. Do not
regenerate it with a writer newer than wave 34 — that is what this forbids.

## `lake_v1_v4/` — a v1 lake (the lean writer, `.pink` v4 bytes)

A complete `format_version: 1` lake written by the **lean `vortex-format`-less
writer** (the `lake-lite` build) with the current engine — the same
`pink_lake::save_store(&common::fidelity_store(), …)` call as `lake_v1/`, but
the data files are the engine's **v4** `.pink` bytes (`PINKDB04…`). It pins the
cross-build compatibility contract:

- the lean build writes exactly this shape (`tests/lake_lite.rs` reloads it
  bit-exactly), and a full (`vortex-format`) build opens and migrates it
  (`tests/lake.rs::v1_v4_fixture_written_by_the_lean_writer_loads_and_migrates`);
- a full build that can read this directory proves a lean-written lake is
  portable, and vice versa.

Immutable like the others: it is regenerated only deliberately, never with a
different writer than the wave-45i lean one.

## `lake_v2/` — a v2 lake (waves 35–40 Vortex writer)

A complete `format_version: 2` lake written by the **wave-35..40 Vortex
writer** (one Vortex row block per 2,048-row engine chunk), generated on
2026-10-15 from commit `c7f24c9` with
`Lake::create(…).write_from_store(&common::fidelity_store())` — the same store
as `lake_v1/`. `crates/pink-lake/tests/lake.rs::v2_fixture_lake_still_loads`
loads it with the current reader (format_version 3 writes 8,192-row blocks)
and compares values bit-for-bit, then re-saves it and checks the migration.

Immutable for the same reason as `lake_v1/`: it pins that v2 lakes stay
readable.
