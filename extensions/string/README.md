# `string` — pinkdb's core string extension

Fourteen deterministic string functions for pinkdb, authored with the Rust
extension SDK (ABI v1) and pinned live against DuckDB
`v2.0.0-alpha46057` (`duckdb` CLI). The library is
`target/release/libstring_ext.<dylib|so>`; the extension's SQL name is
**`string`**.

```sql
-- native (this repo):
--   make -C extensions/string
--   (or: cargo build --release -p pinkdb-ext-string --features abi-exports)
LOAD EXTENSION '/abs/path/to/target/release/libstring_ext.dylib';
-- linked (tests, wasm embedders): register at bootstrap, then load by name
LOAD EXTENSION 'string';
SELECT split_part('a,b,c', ',', CAST(2 AS BIGINT)), reverse('héllo');
-- b | olléh
```

`make -C extensions/string` builds the release library and copies it to
`extensions/string/build/` (the artifact `publish-extensions.yml` discovers
for the `string` repo entry). `./extensions/string/smoke.sh` runs the
CLI-visible end-to-end session.

The `abi-exports` feature is what emits the three ABI v1 `#[no_mangle]`
symbols `LOAD EXTENSION '<path>'` looks up. It is off by default because this
crate also builds an rlib (the linked registration, used by the tests): a
linked-only extension must not carry those globals, or two extension rlibs in
one binary fail to link on Linux with
`duplicate symbol: pinkdb_extension_init`. Plain
`cargo build --release -p pinkdb-ext-string` therefore produces a dylib with
no ABI symbols (not loadable natively); the Makefile passes the feature.

## Functions

| function | DuckDB behavior (verified live) | `string` here |
|---|---|---|
| `split_part(string, delimiter, index)` | 1-based; `0`, out-of-range and beyond either end → `''`; negative counts from the end (`-1` = last); empty delimiter splits into characters; NULL args → NULL | same |
| `reverse(string)` | reverses by Unicode scalar (valid UTF-8 out) | same |
| `left(string, count)` / `right(string, count)` | first/last `count` characters; negative `count` drops that many from the other side (`left('hello', -2)` = `'hel'`); past-the-end → whole string; NULL → NULL | same |
| `strpos(string, search)` | 1-based **character** position, `0` when absent; empty search → `1`; NULL → NULL | same |
| `starts_with(string, search)` / `ends_with(string, search)` | `BOOLEAN`; empty search always matches; NULL → NULL | same |
| `regexp_extract(string, pattern [, group [, options]])` | capturing group of the first match; no match or a non-participating group → `''`; `group` is `INTEGER` 0..=9 (outside → error), defaults to 0, and a BIGINT needs `CAST(x AS INTEGER)` like DuckDB; NULL group → `''`; NULL input/pattern → NULL; NULL options → error | same, 2/3/4 arguments |
| `regexp_matches(string, pattern [, options])` | `BOOLEAN`, partial match (anchor with `^`/`$`); NULL input/pattern → NULL; NULL options → error; `g` → error | same, 2/3 arguments |
| `regexp_replace(string, pattern, replacement [, options])` | rewrites the first match, or every match with `g`; `replacement` supports `\0`…`\9` group references and `\\`; NULL input/pattern/replacement → NULL; NULL options → error; an invalid rewrite schema errors even when nothing matches | same |
| `lpad(string, count, character)` / `rpad(…)` | pads to `count` characters by repeating `character`; a longer string is **truncated** to `count`; `count <= 0` → `''`; an empty pad errors only when padding is needed (`Insufficient padding in LPAD.`); NULL → NULL | same |
| `concat_ws(separator, …)` | joins the non-NULL arguments with `separator`; NULL separator → NULL; empty separator concatenates; at least 2 arguments | same, but every value must be `VARCHAR` (see divergence 2) |
| `regexp_match(string, pattern)` | **DuckDB has no such function** (`Catalog Error`; PostgreSQL's returns `TEXT[]` of capture groups) | **pinkdb-pinned**: `VARCHAR` = first match text, `''` when none; equivalent to `regexp_extract(s, p, 0, '')` |

`trim`, `ltrim`, `rtrim` and `replace` are **engine built-ins** already, with
DuckDB-pinned semantics and differential coverage (the `trim-family` and
`replace-fn` probes in `bench/duckdiff.py`): 1- and 2-argument trim (the
second argument is a character set; an empty set is a no-op), and
`replace(string, source, target)` where an empty source is a no-op. Extension
functions cannot shadow a built-in, so a re-registration here would make
`LOAD EXTENSION` fail; the extension deliberately does not provide them.

## Deliberate divergences (all loud, all tested)

1. **One declaration per name.** ABI v1 has no overloads, but the SDK's
   `optional(min)` modifier gives one declaration a trailing argument range,
   so DuckDB's shorthand forms work as written: `regexp_extract(s, p)`,
   `regexp_extract(s, p, 1)`, `regexp_matches(s, p)` and
   `regexp_replace(s, p, r)`. `concat_ws` is `variadic(1)` (the ABI repeats
   its one declared element type), so its "at least 2 arguments" rule is a
   body check and its error text is the runtime one.
2. **Strict typing: few implicit casts.** ABI v1 widens an INTEGER into a
   `BIGINT` parameter (`left(name, 2)`, like DuckDB), a DATE into a
   TIMESTAMP one (`strftime(DATE '2020-01-01', '%Y')`), and any number into a
   `DOUBLE` one, and inserts no other cast: `left(name, '2')` is an error
   naming the fix (*argument 2 must be BIGINT, got VARCHAR (use CAST)*),
   where DuckDB would parse the string. For the same reason every
   `concat_ws` value must be `VARCHAR` (DuckDB casts numbers and booleans to
   text): `concat_ws(',', 1, 2)` needs `CAST(1 AS VARCHAR)`.
3. **No list type.** `regexp_matches` in *PostgreSQL* (and the roadmap's
   `regexp_match` sketch) returns an array of capture groups / matches.
   pinkdb's ABI v1 value model is `BOOLEAN | INTEGER | BIGINT | DOUBLE |
   VARCHAR | DATE | TIMESTAMP` — there is no `VARCHAR[]`. DuckDB's own
   `regexp_matches` is a `BOOLEAN` (verified live), so only the *name*
   differs there; the array-shaped functions (`regexp_extract_all`,
   `regexp_split_to_array`) are out of scope.
4. **Engine, not RE2.** Patterns are compiled by the `regex` crate
   (RE2-style, linear-time, no backtracking — the same safety model as
   RE2). Syntax is shared for the common surface (inline flags `(?i)`/`(?m)`/
   `(?s)`, `\d`/`\w`/`\s`, POSIX classes, `\p{...}`), with one documented
   superset: the `regex` crate's `\d`/`\w`/`\s` are **Unicode-aware** by
   default where RE2's are ASCII-only (`\d` also matches `٣` here, not in
   DuckDB). Use explicit classes (`[0-9]`) for exact RE2 parity. Error
   *messages* differ (invalid pattern, unknown option); both engines refuse
   the statement.
5. **Error text.** Under the linked model a failing function reports
   `extension function <name>() failed: <message>`; a native `dlopen`
   extension reports only `failed with code 1` (ABI v1's error channel is a
   code, not text — the SDK README documents this).

## `regexp_*` options

DuckDB's option letters, applied left to right (later letters win), verified
live:

| letter | meaning | here |
|---|---|---|
| `i` / `c` | case-insensitive / case-sensitive | same |
| `l` | literal pattern (no regex tokens) | same (`regex::escape`) |
| `m`, `n`, `p` | newline-sensitive: `.` does not match `\n` (the default) | same |
| `s` | non-newline-sensitive: `.` matches `\n` | same |
| `g` | global replace, only valid for `regexp_replace` | same: `regexp_replace` accepts it (first match otherwise); every other `regexp_*` refuses it |
| anything else | error | error (`unrecognized regex option …`) |

`regexp_extract`'s NULL handling is pinned exactly: options are checked
first (NULL → error, and an unknown letter or `g` errors even when the
pattern is NULL), then the group range, then the pattern (NULL → NULL,
invalid → error), then the input (NULL → NULL); a NULL group with a non-NULL
input yields `''`. DuckDB also has a `regexp_extract(s, p, options)` overload
whose third argument is a VARCHAR; one ABI declaration cannot carry both, so
spell it `regexp_extract(s, p, 0, options)` here. Inline flags (`(?s)a.b`)
work as in DuckDB.

## Building and testing

```sh
cargo test -p pinkdb-ext-string     # 14 unit + 10 engine tests
make -C extensions/string           # release library -> build/
./extensions/string/smoke.sh        # CLI session (native LOAD EXTENSION path)
python3 bench/duckdiff.py           # adds the string probe set, extension-loaded
```

- **Unit tests** (`src/lib.rs`) call the declared functions directly: unicode,
  empty strings, NULL policies, boundary indexes, every regexp option,
  rewrite schema, padding/truncation and error path.
- **Engine tests** (`tests/engine.rs`) go through a real store via the linked
  registration: `LOAD EXTENSION 'string'`, WHERE/GROUP BY/ORDER BY
  composition, prepared statements (with the strict-typing error pinned), and
  a durable WAL replay proving the functions are deterministic (an `INSERT`
  that calls them replays identically after reopen).
- **Differential tests** (`bench/duckdiff.py`) run the same SQL through
  pinkdb (with this extension loaded) and DuckDB. The probes use explicit
  `CAST(… AS BIGINT)` and `''` options so one SQL text is valid on both
  sides.
