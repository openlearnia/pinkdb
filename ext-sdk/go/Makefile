# pinkdb Go extension SDK — build the worked example and the conformance
# fixture.
#
#   make            build examples/string_ext.go -> build/string_ext.dylib
#                   and conformance/ -> build/conformance_ext.dylib
#   make template   build the skeleton in template/ (template/build/)
#   make clean      remove build/ and template/build/
#
# Uses the Go toolchain with cgo (-buildmode=c-shared). The only include path
# needed is the vendored ABI header in include/; no pinkdb libraries are
# linked. `go build` also writes the generated cgo header
# build/<name>.h next to each library; it is not needed to load the extension.
#
# conformance_ext is the ABI fixture (ext-sdk/conformance/README.md) that the
# loader's own tests build and assert against; building it here is what lets a
# change to the SDK be checked without running cargo.
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

all: $(BUILD)/string_ext.$(SO_EXT) $(BUILD)/conformance_ext.$(SO_EXT)

$(BUILD)/string_ext.$(SO_EXT): examples/string_ext.go pinkdb/*.go include/pinkdb_extension.h go.mod | $(BUILD)
	CGO_ENABLED=1 $(GO) build -buildmode=c-shared -o $@ ./examples

$(BUILD)/conformance_ext.$(SO_EXT): conformance/main.go pinkdb/*.go include/pinkdb_extension.h go.mod | $(BUILD)
	CGO_ENABLED=1 $(GO) build -buildmode=c-shared -o $@ ./conformance

$(BUILD):
	mkdir -p $(BUILD)

# The skeleton in template/ is its own module; build it on demand only.
template:
	$(MAKE) -C template

clean:
	rm -rf $(BUILD)
	$(MAKE) -C template clean

.PHONY: all template clean
