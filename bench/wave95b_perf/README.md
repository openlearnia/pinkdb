# wave 95b performance probe

Does enforcing `PRIMARY KEY`/`UNIQUE` (and the new DML grammar) cost the
existing write paths anything? One 1 000 000-row keyed table, three writes,
alternating the pre-change and post-change binaries.

```sh
# 1 000 000-row table with a primary key, saved to /tmp/pinkdb-wave95b-perf.pink
./target/release/pinkdb -no-agent -c "$(cat bench/wave95b_perf/gen.sql)"

# the A/B: build the pre-change binary from a worktree at the base commit with
# its own CARGO_TARGET_DIR, then alternate five rounds (base first)
git worktree add /tmp/pinkdb-base HEAD
(cd /tmp/pinkdb-base && CARGO_TARGET_DIR=/tmp/pinkdb-base-target \
    cargo build --release -p pinkdb-cli)
cargo build --release -p pinkdb-cli
./bench/wave95b_perf/run.sh /tmp/pinkdb-base-target/release/pinkdb \
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
    print(f"{k[0]:5} {k[1]:7} min {min(v):.3f} median {statistics.median(v):.3f}")
PY
```

`run.sh` copies the base store for every timed statement and deletes the stale
`.wal` sidecar: replaying the previous run's WAL on top of a fresh copy would
otherwise double-apply the workload.

Medians of five interleaved rounds (release, M-series laptop, 2025-10):

| workload | HEAD `6531691` | wave 95b | delta |
|---|---|---|---|
| `INSERT INTO t2 SELECT * FROM t` (1 M rows, PK on `t2.id`) | 0.095 s | 0.381 s | +0.286 s (~3.5 M keys/s) |
| `UPDATE t SET v = v + 1` (1 M rows, key not written) | 0.025 s | 0.025 s | none |
| `DELETE FROM t WHERE id % 2 = 0` (500 k rows) | 0.019 s | 0.019 s | none |

The insert pays for the uniqueness check every staged row needs. An `UPDATE`
that does not write a key column skips the check; that is the fast path in
`trigger::update_rows`.
