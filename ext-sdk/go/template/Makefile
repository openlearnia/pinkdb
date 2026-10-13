# Standalone Makefile for the pinkdb Go extension skeleton.
#
#   make        build build/my_extension.dylib (my_extension.so on Linux)
#   make clean  remove the built library
#
# Uses the Go toolchain with cgo (-buildmode=c-shared). The only include path
# needed is the vendored ABI header in ../include; no pinkdb libraries are
# linked (the host provides everything through PinkdbHostApi at load time).
# `go build` also writes the generated cgo header build/my_extension.h next to
# the library; it is not needed to load the extension.

GO ?= go
LIB := my_extension

# Shared-library suffix per platform: .dylib on macOS, .so elsewhere.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SUFFIX := dylib
else
SUFFIX := so
endif

all: build/$(LIB).$(SUFFIX)

build/$(LIB).$(SUFFIX): main.go go.mod ../include/pinkdb_extension.h
	mkdir -p build
	CGO_ENABLED=1 $(GO) build -buildmode=c-shared -o $@ .

clean:
	rm -rf build

.PHONY: all clean
