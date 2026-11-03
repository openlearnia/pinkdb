# The ABI conformance fixture

One extension — the *conformance fixture* — implemented once per SDK, so the
C, C++, Go and Rust SDKs are measured against the same contract instead of
against their own examples. `crates/pinkdb-ext-loader/tests/conformance.rs`
and `tests/conformance_ownership.rs` build all four libraries and run the same
assertions against each, through the loader's public API.

A new SDK is "ABI complete" when its fixture passes: every value tag in both
directions, NULL and zero-length results, host-allocated VARCHAR results,
error codes, panic/exception containment, the volatile flag, foreign-thread
callbacks, and the `struct_size` negotiation.

## Files

| SDK | source | built by |
|---|---|---|
| C | `ext-sdk/c/conformance/conformance_ext.c` | `cc -std=c99 -shared -fPIC -Iext-sdk/c/include` |
| C++ | `ext-sdk/cpp/conformance/conformance_ext.cpp` | `c++ -std=c++17 -shared -fPIC -Iext-sdk/cpp/include` |
| Go | `ext-sdk/go/conformance/main.go` | `go build -buildmode=c-shared` in `ext-sdk/go` |
| Rust | `ext-sdk/rust/conformance/src/lib.rs` | `cargo build -p pinkdb-ext-conformance` |

`crates/pinkdb-ext-loader/tests/common/conformance.rs` builds all four itself,
into `CARGO_TARGET_TMPDIR/pinkdb_conformance_<pid>/`: C and C++ with the system
compiler, Go with `CGO_ENABLED=1 go build -buildmode=c-shared`, and Rust with a
nested `cargo build -p pinkdb-ext-conformance` in its own `--target-dir` (the
outer `cargo test` holds the main target lock). A missing toolchain skips that
SDK with a printed note, unless `PINKDB_CONFORMANCE_STRICT=1` — CI sets it, so
a missing compiler is a failure there and never a silent gap.

## The contract

The extension's name is `conformance_ext`, and it registers exactly these
functions (case-insensitive names are the host's business; register them
lowercase):

| function | arguments | returns |
|---|---|---|
| `cf_echo` | VARCHAR | the same bytes, verbatim (embedded NULs, multibyte, empty allowed); NULL in → NULL out |
| `cf_bool` | BOOLEAN | the argument; NULL in → NULL out |
| `cf_int` | INTEGER | the argument; NULL in → NULL out |
| `cf_bigint` | BIGINT | the argument; NULL in → NULL out |
| `cf_hugeint` | HUGEINT | the argument's 128 bits; NULL in → NULL out |
| `cf_uhugeint` | UHUGEINT | the argument's 128 bits; NULL in → NULL out |
| `cf_sum_bigint` | BIGINT | the argument; used by the smoke tests on `SUM(int_col)`; NULL in → NULL out |
| `cf_double` | DOUBLE | the argument bit-for-bit (NaN and ±inf included); NULL in → NULL out |
| `cf_date` | DATE | the argument (days since 1970-01-01); NULL in → NULL out |
| `cf_timestamp` | TIMESTAMP | the argument (microseconds since the epoch); NULL in → NULL out |
| `cf_interval` | INTERVAL | the argument (months, days, micros); NULL in → NULL out |
| `cf_variant` | VARIANT | the argument's payload bytes verbatim (opaque pinkdb encoding — the fixture cannot interpret them); NULL in → NULL out |
| `cf_all` | BOOLEAN, INTEGER, BIGINT, DOUBLE, VARCHAR, DATE, TIMESTAMP, INTERVAL | the canonical summary below |
| `cf_empty` | — | a zero-length VARCHAR (`is_null = 0`, `text_len = 0`) |
| `cf_fill` | BIGINT `n` | `n` bytes of `'x'` (host-allocated); `n < 0` → NULL |
| `cf_fail` | BIGINT | never returns a value: allocates 4 bytes of `"oops"`, publishes them on `out` with `type_tag = PINKDB_EXT_TYPE_VARCHAR`, then returns code `1` |
| `cf_unwind` | — | the SDK's worst case: a panic / exception / unwind inside the callback, which must come back as a non-zero code (the code itself is the SDK's choice: C 2, C++ 3, Go 3, Rust 2) |
| `cf_volatile` | — | `42`, registered with `PINKDB_EXT_FLAG_VOLATILE` |
| `cf_thread` | — | `1`; updates the thread counters below |

Every trailing `NULL in → NULL out` means: write nothing (the host pre-fills
`out` with NULL) and return 0.

`cf_variant` is a pass-through like `cf_interval`, not a field in `cf_all`:
the VARIANT payload is an opaque byte string on the text channel, so a fixture
written in a language other than the engine can only prove that the bytes
survive unchanged (empty and embedded-NUL payloads included) and that the tag,
NULL handling and result-buffer ownership are wired. Decoding the payload is
the engine's job and is covered by the engine's own tests.

### `cf_all`'s canonical summary

Comma-separated, no spaces, fixed field order:

```
b=<bool>,i=<int>,big=<bigint>,d=<double bits>,v=<varchar>,date=<days>,ts=<micros>,iv=<months>/<days>/<micros>
```

* `<bool>` is `1` or `0`;
* `<double bits>` is the argument's IEEE-754 bit pattern as 16 lowercase hex
  digits (`0x4012000000000000` for `4.5`) — comparing bits, not a decimal
  spelling, keeps four languages' float formatting out of the contract;
* `<varchar>` is the argument's bytes verbatim;
* a NULL argument renders as the literal `null` for its field (`b=null`).

So `cf_all(true, -2, 3, 4.5, 'xy', 7, 8, 1 month 2 days 3 micros)` returns

```
b=1,i=-2,big=3,d=0x4012000000000000,v=xy,date=7,ts=8,iv=1/2/3
```

### `cf_fail`, `cf_unwind` and the capability word

Two of the required behaviours are not expressible through every SDK's safe
surface, so the fixture advertises what it actually does:

```c
int64_t pinkdb_conformance_caps(void);   /* bit 0: cf_fail */
                                         /* bit 1: cf_unwind */
```

* bit 0 — `cf_fail` really allocates its 4 bytes through the host's `alloc`
  before returning code 1, so the host's error-path free is observable;
* bit 1 — `cf_unwind` really panics/throws (C++, Go, Rust); C has no
  unwinding, so its fixture returns code 2 without setting this bit.

The bit sets today: C `1` (allocating `cf_fail`, no real unwind), C++ `3`,
Go `3`, Rust `2` (the typed writer cannot publish a text result and then fail,
so its `cf_fail` allocates nothing — the shared test asserts the allocation
only when bit 0 says so, and always asserts that `live()` is unchanged).

The shared tests assert the behaviour the bits describe — a failing `cf_fail`
must still free its buffer, a panicking `cf_unwind` must come back as a clean
error code and leave the extension usable — and assert the advertised bit set,
so an SDK cannot silently drop either one.

### Introspection (not part of the ABI)

The loader tests open each library directly and read these symbols, the way
`tests/common/mod.rs` already reads `pinkdb_test_*` from the C fixtures:

```c
int64_t pinkdb_conformance_calls(void);            /* callbacks invoked */
int64_t pinkdb_conformance_foreign_threads(void);  /* ... on another thread */
int64_t pinkdb_conformance_caps(void);             /* the bit set above */
```

`pinkdb_conformance_foreign_threads` counts callbacks whose thread is not the
thread that ran `pinkdb_extension_init`. The host runs callbacks on rayon
workers, but nothing guarantees the init thread is never a worker, so the
shared test drives a many-row statement and only requires the count to be
greater than zero after it.

### `struct_size` and `abi_version` negotiation

The ABI calls `pinkdb_extension_init` exactly once per process, and every
fixture keeps process-global state, so `tests/conformance.rs` runs each
negotiation case in a child process (`current_exe()` re-entered as a
single-test run writes its observations to a file). Three hosts are published:

| host | `abi_version` | `struct_size` | expected |
|---|---|---|---|
| legacy | 1 | `PINKDB_EXT_API_V1_SIZE` (32) | non-zero code, and no function registered as deterministic that the fixture declares volatile |
| full | 1 | `PINKDB_EXT_API_MINOR2_SIZE` (48) | `0`, exactly the 19 registrations, `cf_volatile` the only volatile one |
| future | 999 | `PINKDB_EXT_API_V1_SIZE` | non-zero code, nothing registered |

The loader's own header is never the host in these runs: the child calls the
exported `pinkdb_extension_init` through the stub `PinkdbHostApi` the test
builds, which is what makes the three cases possible at all. Measured refusal codes (not
asserted, since each SDK picks its own): legacy host C 3, C++ 1 (13
registrations accepted before `cf_volatile`, which is what the "nothing
volatile registered as deterministic" assertion pins), Go 10, Rust 4; unknown
ABI C/C++/Go 1, Rust 3. A full host returns 0 with all 19 registrations for all
four.

## What the fixture deliberately does not cover

Registration *refusals* — a NULL callback, a name over 256 bytes, more than 64
arguments, an unknown flag bit, a NULL argument tag — cannot be produced by a
well-behaved SDK: the host must reject the whole load, and the SDKs always
register valid declarations. Those boundaries live in the loader's own C
fixtures (`tests/c/bad_register_ext.c`, `bad_flags_ext.c`,
`null_callback_ext.c`, `wrong_abi_ext.c`, …) and in `src/host.rs`'s unit
tests, where the malformed input is the point.

Likewise the *value-level* malformations (`type_tag = 99`, a VARCHAR length
past 64 MiB, a NULL `text_ptr` with a non-zero length) are covered by
`tests/c/bogus_values_ext.c` and `tests/varchar_ownership.rs` — an SDK would
have to opt out of its own safety checks to trigger them.
