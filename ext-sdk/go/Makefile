# pinkdb Go extension SDK — build the worked example.
#
#   make            build examples/string_ext.go -> build/string_ext.dylib
#   make template   build the skeleton in template/ (template/build/)
#   make clean      remove build/ and template/build/
#
# Uses the Go toolchain with cgo (-buildmode=c-shared). The only include path
# needed is the vendored ABI header in include/; no pinkdb libraries are
# linked. `go build` also writes the generated cgo header
# build/string_ext.h next to the library; it is not needed to load the
# extension.
#
# Override the toolchain if needed:  make GO=/usr/local/go/bin/go

GO ?= go

# Shared-library suffix per platform: .dylib on macOS, .so elsewhere.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SO_EXT := dylib
else
SO_EXT := so
endif

BUILD := build

all: $(BUILD)/string_ext.$(SO_EXT)

$(BUILD)/string_ext.$(SO_EXT): examples/string_ext.go include/pinkdb_extension.h go.mod | $(BUILD)
	CGO_ENABLED=1 $(GO) build -buildmode=c-shared -o $@ ./examples

$(BUILD):
	mkdir -p $(BUILD)

# The skeleton in template/ is its own module; build it on demand only.
template:
	$(MAKE) -C template

clean:
	rm -rf $(BUILD)
	$(MAKE) -C template clean

.PHONY: all template clean
