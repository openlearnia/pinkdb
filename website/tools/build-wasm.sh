#!/usr/bin/env bash
# Build the pinkdb wasm module used by the docs playground.
#
# The module is a plain wasm32-unknown-unknown cdylib (no wasm-bindgen, no
# wasm-opt). The root [profile.release] applies as-is (thin LTO, one codegen
# unit) — this script only selects the toolchain and copies the artifact.
#
# On this machine the PATH `cargo` is Homebrew's, which has no wasm32 std, so
# the rustup stable toolchain is preferred: `~/.cargo/bin/cargo +stable` with
# RUSTC pointed at rustup's stable rustc. Where rustup is absent (e.g. CI with
# a system Rust that ships the target) fall back to plain `cargo`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

TARGET="wasm32-unknown-unknown"
OUT="website/src/assets/playground/pinkdb.wasm"

if command -v rustup >/dev/null 2>&1 && [ -x "$HOME/.cargo/bin/cargo" ]; then
  export RUSTC
  RUSTC="$(rustup which --toolchain stable rustc)"
  "$HOME/.cargo/bin/cargo" +stable build -p pinkdb-wasm --target "$TARGET" --release
else
  cargo build -p pinkdb-wasm --target "$TARGET" --release
fi

mkdir -p "$(dirname "$OUT")"
cp "target/$TARGET/release/pinkdb_wasm.wasm" "$OUT"
printf 'built %s (%s bytes)\n' "$OUT" "$(wc -c < "$OUT" | tr -d ' ')"
