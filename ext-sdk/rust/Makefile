# pinkdb Rust extension SDK — build the worked example.
#
#   make            build the example (a workspace member) into target/release/
#   make template   build the standalone skeleton in template/
#   make clean      remove the skeleton's own target/ (the example's artifacts
#                   live in the workspace target/ and are cleaned with `cargo
#                   clean -p pinkdb-ext-sdk-example`)
#
# The example is `example/string_ext` — one `pinkdb_extension!` declaration
# that compiles to `librust_string_ext.<ext>`. smoke.sh builds it through this
# Makefile and runs a full SQL session against the release CLI.
#
# Override the toolchain if needed:  make CARGO=/path/to/cargo

CARGO ?= cargo

# Shared-library suffix per platform: .dylib on macOS, .so elsewhere.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SO_EXT := dylib
else
SO_EXT := so
endif

# Cross-architecture build: `make TARGET=x86_64-apple-darwin` (used by the
# release workflow's darwin-x64 job).
TARGET ?=
TARGET_DIR := $(if $(TARGET),target/$(TARGET),target)
CARGO_TARGET_FLAG := $(if $(TARGET),--target $(TARGET),)

all:
	$(CARGO) build --release $(CARGO_TARGET_FLAG) -p pinkdb-ext-sdk-example
	@printf 'example built: %s\n' "$(TARGET_DIR)/release/librust_string_ext.$(SO_EXT)"

template:
	$(CARGO) build --release --manifest-path template/Cargo.toml
	@printf 'template built: %s\n' "ext-sdk/rust/template/target/release/libmy_extension.$(SO_EXT)"

clean:
	rm -rf template/target

.PHONY: all template clean
