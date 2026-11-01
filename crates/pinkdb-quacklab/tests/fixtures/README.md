# `go-reference.json` — the contract oracle

Recorded from the real QuackLab service, built from
`grain/vps/cmd/quacklab-vps-duckdb` (the customer's Go service in
`~/Desktop/projects/grain/grain`) and run in DuckDB **file mode** against an
empty database, replaying the 21 requests below in order: the request's SQL,
the HTTP status and the response body with the timing fields removed. The
5,005-row answer's row array is shortened to its first rows plus a
`"...(5000 rows)"` placeholder, and the no-body case is written as the literal
`<no body>`.

It is the test oracle for `tests/contract.rs`: the adapter must reproduce these
bodies exactly (key order aside) except where pinkdb cannot, and those cases are
listed and asserted on status, code and DuckDB's error prefix instead.

Taken on 2026-11-01 from the branch the wave-138 brief names.
