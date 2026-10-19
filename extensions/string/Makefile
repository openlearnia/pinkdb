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

all:
	$(CARGO) build --release -p pinkdb-ext-string
	mkdir -p build
	cp "$(ROOT)/target/release/libstring_ext.$(SO_EXT)" build/
	@printf 'extension built: %s\n' "extensions/string/build/libstring_ext.$(SO_EXT)"

debug:
	$(CARGO) build -p pinkdb-ext-string
	@printf 'debug library: %s\n' "$(ROOT)/target/debug/libstring_ext.$(SO_EXT)"

clean:
	rm -rf build

.PHONY: all debug clean
