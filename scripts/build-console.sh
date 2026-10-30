#!/bin/sh
# Build the warehouse console's browser app (wave 121) and refresh the
# committed, embedded copy in crates/pinkdb-server-ui/web-dist/.
#
# Needs trunk (https://trunkrs.dev; it fetches the matching wasm-bindgen and
# wasm-opt itself) and a Rust toolchain with the wasm32-unknown-unknown
# target. On a machine whose PATH cargo has no wasm32 std (Homebrew Rust),
# point it at rustup's: RUSTUP_TOOLCHAIN=stable plus rustup's cargo first on
# PATH, e.g.
#   PATH="$HOME/.cargo/bin:$PATH" RUSTUP_TOOLCHAIN=stable \
#     RUSTC=$(rustup which --toolchain stable rustc) scripts/build-console.sh
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WEB="$ROOT/crates/pinkdb-server-ui-web"
OUT="$ROOT/crates/pinkdb-server-ui/web-dist"
STAGE="$ROOT/target/console-dist"

# erase_components type-erases every component's view (Leptos): the wasm is
# about a quarter smaller (wave 121c: 449 KB gzipped without, 339 KB with).
RUSTFLAGS="${RUSTFLAGS:-} --cfg erase_components"
export RUSTFLAGS

(cd "$WEB" && trunk build --release --filehash false --dist "$STAGE")

mkdir -p "$OUT"
# -n: no name or timestamp in the header, so a rebuild of the same output is
# byte-identical.
gzip -9 -n -c "$STAGE/pinkdb-console.js" > "$OUT/pinkdb-console.js.gz"
gzip -9 -n -c "$STAGE/pinkdb-console_bg.wasm" > "$OUT/pinkdb-console_bg.wasm.gz"
python3 "$ROOT/scripts/console_hash.py" > "$OUT/SOURCE_HASH"

printf 'console built: js %s bytes (%s gzipped), wasm %s bytes (%s gzipped)\n' \
  "$(wc -c < "$STAGE/pinkdb-console.js" | tr -d ' ')" \
  "$(wc -c < "$OUT/pinkdb-console.js.gz" | tr -d ' ')" \
  "$(wc -c < "$STAGE/pinkdb-console_bg.wasm" | tr -d ' ')" \
  "$(wc -c < "$OUT/pinkdb-console_bg.wasm.gz" | tr -d ' ')"
