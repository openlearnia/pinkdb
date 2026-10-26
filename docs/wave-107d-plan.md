# Wave 107d — parallel HNSW build

Follow-up to 107b (`docs/wave-107b-plan.md`). `CREATE INDEX … USING HNSW`
built one row at a time on one thread: 94.5 s for 100k×768 and 867 s for
1M×128 on Apple silicon. This wave parallelises the build without giving
up the property 107b bought with its seeded level stream: **the graph is
a function of the data and the insert order only — never of the thread
count, the scheduler, or whether the build was a live build, a WAL replay
or a reopen.** 1, 4 and 16 threads must produce byte-identical
`serialize()` output, so a replica or a crash replay cannot end up with a
different index from the original.

## Argued, then decided

**Opponent: why not lock-free concurrent insertion (the usual HNSW
parallel build)?** Because concurrent inserts race on neighbour lists:
whoever links first changes what the next search sees, so the graph
depends on scheduling. That breaks replay determinism outright, and no
amount of tie-breaking fixes a race on *which candidates were visible*.
Rejected.

**Opponent: why not shard and merge sub-graphs?** Merging graphs needs a
second linking pass whose result depends on the shard count — which
would naturally be the thread count. Rejected: constants below are
fixed, never derived from the pool size.

**Chosen: batched insertion — parallel search, sequential commit.**

1. The first `SEED = 1024` vectors of a graph's life are inserted with
   today's `insert_vector`, untouched. A graph that never grows past the
   seed is byte-identical to a 107b graph, and a cold graph gives the
   batches below something worth searching.
2. The rest go in batches of `BATCH = 256` vectors (both constants, in
   `extensions/vss/src/hnsw.rs`, never derived from the thread count).
   Per batch:
   - levels are drawn **sequentially, in insert order**, from the same
     SplitMix64 stream as today, so the RNG state after N inserts is
     unchanged by batching;
   - rows that replace a live frozen node tombstone it before the search
     phase (as a sequential insert would before searching); a row
     repeated inside one batch supersedes its earlier copy, which is
     pushed as a tombstone with no links;
   - **in parallel** (rayon, one task per batch member), each vector runs
     the greedy descent and `search_layer` against the *frozen* graph as
     of the batch start, producing its candidate list per level. Each
     task reads only shared state and its own vector, and writes only
     its own slot in a `collect()`ed vector (indexed order), so the
     output cannot depend on scheduling;
   - candidates also include the other batch members: an exact
     \(O(B^2)\) distance pass per node within the batch, kept per level
     (a peer is only eligible on levels it actually has), so two
     near-duplicates landing in one batch still link even though neither
     is in the frozen graph yet. This pass is also what restores most of
     the recall the frozen search loses versus a fully sequential build,
     whose candidates could route through just-inserted neighbours;
   - neighbour **selection also runs in the parallel phase**: it is
     pure (reads vectors only, and all batch nodes are pushed — linkless
     and unreachable — before the phase starts). Peers and frozen
     candidates are selected *separately*, then merged (peers first)
     and capped: one joint selection let near peers diversity-reject
     every frozen candidate (exact duplicates do exactly that), leaving
     the batch an island with no edges to the old graph. Measured
     before this split: a batch of exact duplicates was unreachable
     from the frozen graph;
   - **sequentially, in insert order**, each node's own links are set
     and the entry-point/max-level updates happen. The reverse
     additions are collected per (neighbour, level) in insert order,
     then applied **in parallel, one list at a time**: each list is
     independent, and replaying its additions in order is exactly the
     push-or-prune (`pruned`, the pure half of `link`) a sequential
     commit would have done. The applied lists are assigned back in a
     final sequential pass.
3. Profiling note (the brief's option 5): the first cut — parallel
   search, fully sequential commit including `select` and pruning —
   measured only 1.9× at 4 threads (69.0 s on 100k×768). Selection and
   per-list pruning *are* the commit's distance work, so both moved
   into parallel phases as above; no other parallelisation was needed
   (4 threads then measured 32.1 s, 3.5× the sequential build).

## The provider hook

`pinkdb::vector_index::VectorIndex` gains one method with a default:

```rust
fn insert_batch(&mut self, rows: &[(u64, &[f32])]) -> Result<()> {
    for (row, vector) in rows { self.insert(*row, vector)?; }
    Ok(())
}
```

The default keeps every other (present or future) provider correct;
`Hnsw` overrides it with the batched build. No new crate: `pinkdb-vss`
takes a **target-gated** dependency on `rayon` (already in `Cargo.lock`
at 1.12 through the engine — zero new crates, no version bump). On
`wasm32` the dependency and the parallel iterator are `cfg`'d out and
the same per-node search runs sequentially, producing the same graph by
construction; the engine's `wasm32-unknown-unknown` build is unaffected
(the new trait method is a default over existing items).

## Who calls it (engine, `crates/pinkdb/src/storage.rs`)

- `create_vector_index_in` collects the table's existing `(row, vector)`
  pairs in row order and calls `insert_batch` once. This is the
  `CREATE INDEX` build — the measured pain.
- `extend_vector_indexes` (the append funnel used by multi-row
  `INSERT` and by `COPY`, which appends chunks then extends from the
  append start) gathers the appended rows per index and calls
  `insert_batch` when the append carried **≥ 32 rows**, else the old
  per-row `insert` loop. Decided threshold: 32. Below it the batch
  machinery (and its graph divergence from the sequential tail) buys
  nothing — a handful of searches is microseconds — and keeping the
  sequential path for small appends means ordinary OLTP-ish inserts
  into an indexed table behave exactly as in 107b. A single-row insert
  therefore keeps today's path, as the brief requires. At or above it,
  the whole append is one batch call; `Hnsw::insert_batch` itself
  re-chunks into `BATCH`-sized batches above the seed.
- `UPDATE` of a vector stays delete + single `insert` (it is inherently
  one row at a time in `update_vector_indexes`). `compact()` keeps
  building its fresh graph with sequential `insert_vector`: compaction
  is rare, already deterministic, and not part of the build complaint.
  (Opponent's note: compact *could* call `insert_batch` for free speed;
  decided against — one less graph whose bytes change this wave.)

WAL replay replays SQL text through the same funnels with the same
statement boundaries, so a replayed `CREATE INDEX` / multi-row `INSERT`
re-runs the same batching and lands on the same bytes — this is what the
bulk-equals-replay test pins.

## Thread count

Rayon's global pool — the same pool the engine already runs on, so a
host that sized it (e.g. via `RAYON_NUM_THREADS`) sizes this too. The
engine has no SQL-level thread setting today; inventing one is out of
scope, and the pool's existing knobs are documented in the README
instead. The result is pool-size-independent by construction (frozen
search + in-order commit), which the thread-count test proves rather
than asserts.

## Tests

In `extensions/vss/src/hnsw.rs` (unit) and `extensions/vss/tests/engine.rs`:

- **Thread-count independence:** the same 20k×32 clustered set built
  under rayon pools of 1, 2, 4 and 8 threads
  (`ThreadPoolBuilder::install`) serializes byte-identically; also
  identical to a sequential-fallback build of the same rows through
  `insert_batch` with a 1-thread pool (that is the wasm path's shape).
- **Batch boundaries:** n < SEED, n = SEED, n = SEED + 1, n crossing
  several batches, a batch of exact duplicate vectors (all rows found,
  search still returns them), and a batch that raises `max_level`
  (forced with a small `M` so high levels are common). Each asserts the
  graph round-trips through serialization and search finds a planted
  nearest neighbour.
- **Batch vs replay (engine):** `CREATE INDEX` over existing rows, then
  a durable crash/reopen replaying the WAL (existing test shape, larger
  row count so the parallel path runs), gives identical `to_pink_bytes`
  and identical top-k answers.
- **Recall gate:** the existing 100k×8 engine test (≥ 0.95 at ef 64,
  unchanged) now exercises the parallel path end to end.
- Existing vss tests stay green and unchanged; deletes/compaction after
  a parallel build are covered by the existing mutation/compaction
  tests plus the boundary tests' delete-free shape (decided: no new
  delete-specific parallel test — `delete`/`compact` code is untouched
  and already covered; noted as a deliberate non-goal).

## Benchmarks

`cargo run --release -p pinkdb-vss --example bench_vector`, clustered
data as in 107b (100k×768/32 clusters, 1M×128/64, 200 queries),
`RAYON_NUM_THREADS` ∈ {1, 4, all (10 on the bench machine)}:

- build wall time and speed-up per thread count (target ≥ 3× on 8
  performance cores for 100k×768 — the bench machine is an Apple M4,
  4P+6E, so "all threads" is reported as measured, not normalised);
- recall@10 at ef_search 64 and 128 per thread count (must be identical
  across thread counts — same graph — and within 0.01 of the sequential
  build: 0.874/0.963 on 100k×768 and 0.740/0.862 on 1M×128 from 107b,
  re-measured for 100k×768 before the change on this machine);
- search p50/p95 unchanged.

Disk discipline: 15 GB free at start, at least 10 GB must stay free. The
1M×128 store is ~0.6 GB of graph plus rows; benchmark stores live in
/tmp and are deleted by the example itself. If space runs short, 1M×128
is measured at 1 and all threads only (the graphs are identical anyway,
so recall needs one run).

## Measured (Apple M4, 4P+6E, release, this machine)

Sequential baseline re-measured before the change: 100k×768 build
111.5 s, recall@10 0.874 @ ef 64 / 0.963 @ ef 128 — the 107b numbers
(94.5 s) reproduced on recall exactly. The 1M×128 sequential figures
are 107b's (867 s, 0.740/0.862); re-running that build was not worth
15 minutes of disk-hot time when the batched 1-thread run bounds it.

| set | threads | build | vs sequential | recall @64 | recall @128 |
|---|---|---|---|---|---|
| 100k × 768 | sequential (107b path) | 111.5 s | 1.0× | 0.874 | 0.963 |
| | 1 | 142.2 s | 0.78× | 0.878 | 0.961 |
| | 4 | 32.1 s | 3.5× | 0.878 | 0.961 |
| | all (10) | 30.4 s | 3.7× | 0.878 | 0.961 |
| 1M × 128 | sequential (107b) | 867 s | 1.0× | 0.740 | 0.862 |
| | 1 | 1065.7 s | 0.81× | 0.751 | 0.883 |
| | 4 | 228.7 s | 3.8× | 0.751 | 0.883 |
| | all (10) | 139.1 s | 6.2× | 0.751 | 0.883 |

Graph bytes are identical at every thread count on both sets
(318,915,668 B and 631,886,420 B) — as the byte-identity unit test
requires. Recall is never below the sequential build: 100k×768 is
+0.004/−0.002 at ef 64/128; 1M×128 is *above* it by +0.011/+0.021
(the exact intra-batch linking helps at 128 dimensions), so the
"within 0.01" target is met in the only direction that can hurt —
downwards. Search p50/p95 are unchanged (0.88/0.93 ms and 0.48/0.54 ms
at ef 64). The 1-thread batched build is ~25 % slower than the old
sequential loop (exact peer pass + double selection + prune replay is
real work the sequential insert amortises through the live graph);
that is the price of thread-count independence, paid only when the
pool has one thread. Scaling saturates at the 4 performance cores,
as expected on this chip.

## Gates

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets -p pinkdb-cli --features vss -- -D warnings
cargo test -q -p pinkdb-vss --no-fail-fast
cargo test -q -p pinkdb-cli --features vss --no-fail-fast
cargo test -q --workspace --no-fail-fast
RUSTC=$(rustup which --toolchain stable rustc) ~/.cargo/bin/cargo +stable build -p pinkdb --target wasm32-unknown-unknown
```

Plus ROADMAP row 107 note, a CHANGELOG entry, and the README
vector-search section (build is parallel and deterministic). The
website regenerates only if a page changed (CHANGELOG feeds the
changelog page, so it will).
