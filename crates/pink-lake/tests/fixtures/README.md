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
