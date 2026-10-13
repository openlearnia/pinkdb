# Build and run the C ABI example against the release artifacts.
#
#   make hello      # builds pinkdb-ffi (release) + compiles/runs hello.c
#   make clean
#
# The dylib is loaded via an rpath pointing at the workspace target dir, so
# `./hello` runs without setting DYLD_LIBRARY_PATH.

CARGO  ?= cargo
CC     ?= cc
TARGET ?= ../../target/release
RPATH  := $(abspath $(TARGET))

hello: examples/hello.c include/pinkdb.h
	$(CARGO) build --release -p pinkdb-ffi
	$(CC) -std=c11 -Wall -Wextra -Iinclude examples/hello.c \
		-L$(TARGET) -Wl,-rpath,$(RPATH) -lpinkdb_ffi -o hello
	./hello

clean:
	rm -f hello

.PHONY: clean
