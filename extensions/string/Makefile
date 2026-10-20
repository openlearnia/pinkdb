# pinkdb core string extension — build the loadable library.
#
#   make        release cdylib, copied to build/ (what publish-extensions.yml
#               discovers for extensions/string -> the `string` artifact)
#   make debug  debug cdylib (no copy)
#   make clean  remove build/ (the workspace target/ is untouched)
#
# Override the toolchain if needed:  make CARGO=/path/to/cargo

CARGO ?= cargo
ROOT := $(abspath ../..)

# Shared-library suffix per platform: .dylib on macOS, .so elsewhere.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SO_EXT := dylib
else
SO_EXT := so
endif

# Cross-architecture build: `make TARGET=x86_64-apple-darwin` (the release
# workflow's darwin-x64 job uses this; target/release stays host-only).
TARGET ?=
TARGET_DIR := $(if $(TARGET),$(ROOT)/target/$(TARGET),$(ROOT)/target)
CARGO_TARGET_FLAG := $(if $(TARGET),--target $(TARGET),)

all:
	$(CARGO) build --release $(CARGO_TARGET_FLAG) -p pinkdb-ext-string --features abi-exports
	mkdir -p build
	cp "$(TARGET_DIR)/release/libstring_ext.$(SO_EXT)" build/
	@printf 'extension built: %s\n' "extensions/string/build/libstring_ext.$(SO_EXT)"

debug:
	$(CARGO) build $(CARGO_TARGET_FLAG) -p pinkdb-ext-string --features abi-exports
	@printf 'debug library: %s\n' "$(TARGET_DIR)/debug/libstring_ext.$(SO_EXT)"

clean:
	rm -rf build

.PHONY: all debug clean
