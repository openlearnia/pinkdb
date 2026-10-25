# wave 95c performance probe

`MERGE` and `UPDATE … FROM` used to compare every target row with every source
row. Wave 95c hashes the equality keys of the `ON`/`WHERE` predicate instead.
This probe measures the difference: one 1 000 000-row keyed table, one 100 000-row
source that matches every tenth target, alternating the pre-change and
post-change binaries.

```sh
# 1 M target rows / 100 k source rows, saved to /tmp/pinkdb-wave95c-perf.pink
# (gen_small.sql writes 100 k / 10 k, gen_tiny.sql 10 k / 1 k)
./target/release/pinkdb -no-agent -c "$(cat bench/wave95c_perf/gen.sql)"

# the A/B: build the pre-change binary from a worktree at the base commit with
# its own CARGO_TARGET_DIR, then alternate five rounds (base first)
git worktree add --detach /tmp/pinkdb-base HEAD
(cd /tmp/pinkdb-base && CARGO_TARGET_DIR=/tmp/pinkdb-base-target \
    cargo build --release -p pinkdb-cli)
cargo build --release -p pinkdb-cli
./bench/wave95c_perf/run.sh /tmp/pinkdb-base-target/release/pinkdb \
                            ./target/release/pinkdb | tee /tmp/ab.txt

# min/median per (build, workload)
python3 - <<'PY'
import statistics
rows = {}
for line in open('/tmp/ab.txt'):
    p = line.split()
    if len(p) == 4:
        rows.setdefault((p[1], p[2]), []).append(float(p[3]))
for k, v in sorted(rows.items()):
    print(f"{k[0]:5} {k[1]:12} min {min(v):.3f} median {statistics.median(v):.3f}")
PY
```

`run.sh` copies the base store for every timed statement and deletes the stale
`.wal` sidecar: replaying the previous run's WAL on top of a fresh copy would
otherwise double-apply the workload.

Measured (release, M-series laptop, 2025-10), `merge.sql` =
`MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN
NOT MATCHED THEN INSERT VALUES (s.id, s.v)`, `update_from.sql` =
`UPDATE t SET v = s.v FROM s WHERE t.id = s.id`:

| target × source | HEAD `a7044e6` | wave 95c | speed-up |
|---|---|---|---|
| `MERGE` 10 k × 1 k (median of 5) | 3.18 s | 0.048 s | ~66× |
| `UPDATE … FROM` 10 k × 1 k (median of 5) | 3.09 s | 0.006 s | ~515× |
| `MERGE` 100 k × 10 k | 340.2 s | 0.47 s | ~720× |
| `UPDATE … FROM` 100 k × 10 k | 318.2 s | 0.027 s | ~11 800× |
| `MERGE` 1 M × 100 k | not runnable (10¹¹ pairs; ≈9.4 h extrapolated) | 4.9 s | — |
| `UPDATE … FROM` 1 M × 100 k | not runnable (≈8.8 h extrapolated) | 0.28 s | — |

The 100 k × 10 k rows are single runs; everything else is a median of five
interleaved rounds. The two 1 M × 100 k HEAD figures are **extrapolations** from
the measured per-pair cost, not measurements — the nested loop cannot finish
them on this machine.

`MERGE` scales worse than `UPDATE … FROM` because it also materialises the
source rows it has to report and re-checks that no target row is affected
twice; the hash table only removes the quadratic scan.
