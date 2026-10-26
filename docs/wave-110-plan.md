# Wave 110 — S3-backed lake catalogs

## Decision

Add the optional `catalog-s3` store at `pink+s3://bucket/prefix`. The distinct
scheme keeps catalog locations unambiguous from `s3://` data locations. A new
catalog records `s3://bucket/prefix/data/` as its data path unless the caller
supplies one.

The durable catalog is the existing pinkdb database split across immutable S3
objects:

- `prefix/catalog/checkpoint/<seq>.pink` is a full catalog image at sequence
  `seq`.
- `prefix/catalog/log/<seq>` is exactly one existing pinkdb WAL record, with a
  20-digit zero-padded sequence.
- `prefix/catalog/head` is an overwriteable checkpoint hint. Readers verify it
  against LIST and remain correct if it is absent, stale, or malformed.

This deliberately does not add another JSON metadata format. A successful
catalog commit uploads only its WAL delta, independent of catalog size.

## Engine seam

Expose the narrowest reusable pinkdb API needed to:

1. open an in-memory database from an existing `.pink` image plus ordered WAL
   record bytes;
2. capture the WAL record produced by one committed transaction; and
3. serialize a checkpoint image.

The record remains pinkdb's versioned, checksummed logical WAL encoding and
its touched-set conflict metadata remains the authority for revalidation.
No second catalog transaction encoding will be introduced.

## Store and concurrency

`pink-lake` will reuse `pinkdb-httpfs` for GET, LIST, DELETE, ordinary PUT, and
conditional `put_new`. On open it selects the newest listed checkpoint,
preferring a valid `head` only as a starting hint, caches the downloaded image
by endpoint/bucket/prefix/sequence, and replays the listed logs after that
sequence. An already-open store refreshes with one log LIST after its applied
sequence.

A writer builds a transaction against its applied sequence and conditionally
creates `log/<next>`. Exactly one writer wins. A precondition loser refreshes,
replays missed records, and uses pinkdb's existing touched-set validation:
non-overlapping work retries and overlapping work returns the existing clear
catalog conflict. `Put::ConditionalUnsupported` is a hard write refusal;
there is no check-then-put fallback.

The recommended checkpoint interval is 100 commits and is configurable with
the existing lake settings mechanism. `CHECKPOINT` also creates one. A
checkpoint is uploaded with `put_new`, followed by the best-effort `head`
hint. Expire/vacuum, never commit, removes logs older than the oldest retained
checkpoint. Retention and time travel otherwise keep current expire semantics.

For a keyed lake, checkpoint images and log records use the engine's
`PINKENC1` encryption and key. Plain lakes upload plaintext engine artifacts.
The local cache stores the same encrypted checkpoint bytes and has no
plaintext durable copy.

## Features and compatibility

`pink-lake/catalog-s3` depends on its existing `s3` feature and is forwarded by
`pinkdb-cli` and `pinkdb-server`; it is off by default. A build without it
returns `FeatureMissing` naming `catalog-s3`. No dependency is added.

DuckDB's DuckLake extension cannot attach `pink+s3://` catalogs. The existing
catalog migration command will treat this as another metadata store and copy
in both directions with `pink:`, `sqlite:`, and `postgres://`.

## Implementation order

1. Add and test the minimal pinkdb WAL/image seam.
2. Add the feature-gated S3 catalog adapter and URL routing.
3. Wire commit arbitration, refresh, cache, checkpoints, encryption, and
   maintenance.
4. Forward the feature through CLI/server and cover the CLI mock-S3 path.
5. Add mock-only tests for recovery, races, delta size, conditional-write
   refusal, encryption, migration, time travel, and checkpoint GET counts.
6. Update ROADMAP, CHANGELOG, README, lake website source/generated output,
   and CI.
7. Run each requested gate once, then measure feature/default release binary
   sizes only if at least 12 GB is free. Any measurement scratch target will
   be deleted immediately.

## Verification

All S3 tests use `crates/pink-lake/src/mock_s3.rs` or the
`pinkdb-httpfs` localhost harness. No real endpoint or credential is used.
The final report will list exact test counts, measured sizes, ignored/deleted
tests, and gaps. The recommended defaults above are selected wherever the
brief leaves a choice; no open design question blocks implementation.

## Changes during implementation

Four things the plan above got wrong or left open, and one measurement the
tests forced:

1. **`head` is written, never read.** The plan had readers confirm the hint
   against LIST. Listing is already the truth and the hint only added a GET to
   every open, so the store writes `catalog/head` for operators and derives the
   base from `catalog/checkpoint/` alone. Stale, missing, or malformed hints
   cannot affect a reader.
2. **No LIST per statement.** "Refreshes with one log LIST after its applied
   sequence" became one LIST *after this handle published*, not before every
   statement: an autocommit commit runs many statements, and the first version
   cost 45 S3 requests per one-row commit. A handle now reads as of its last
   load or publish (like the `pink:` and `sqlite:` stores); commits still
   arbitrate through `put_new`, so staleness costs a retry, not correctness.
   A one-row commit measured 3 requests (2 LIST, 1 conditional PUT).
3. **A newer checkpoint fences a writer.** `expire`/`vacuum` checkpoints and
   prunes from another handle. If that happens mid-transaction, the next log
   sequence is already below the newest checkpoint and writing it would fork a
   sequence later opens ignore. `publish` therefore lists checkpoints first and
   returns `Conflict` (after rebasing) when a newer one exists.
4. **Contiguity failure in `refresh` means rebased, not corrupt.** A log gap
   can mean `expire` folded our base into a checkpoint; `refresh` rebuilds
   instead of failing forever. `load()` still re-validates above the newest
   checkpoint, so real corruption is still reported.
5. **Delta test needs one commit per data file.** Appends inside one commit are
   merged into one data file, so the 1,000-file case runs 1,000 one-row commits
   rather than one commit of 1,000 rows.
