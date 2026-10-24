# wave 95a performance probe

A local TPC-H-shaped store, small enough to build in a debug build, used to
check that the SQL-dialect work did not slow existing queries down
(`docs/sql-conformance.md`, "Performance").

```sh
# 1 000 000-row lineitem-shaped table, five columns of TPC-H types, `.pink`
pinkdb -no-agent -c "$(cat bench/wave95_perf/gen.sql)"

# timed query: open the store, turn the timer on, run the query
pinkdb -no-agent -c ".open /tmp/pinkdb-wave95-perf.pink" -c ".timer on" \
       -c "$(cat bench/wave95_perf/q1.sql)"

# the before/after A/B: build the pre-change binary from a worktree at the
# base commit with its own CARGO_TARGET_DIR, then alternate
for i in 1 2 3; do base…; new…; done
```
