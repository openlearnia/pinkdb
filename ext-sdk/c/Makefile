# pinkdb C/C++ extension SDK — build the worked examples.
#
#   make            build all examples into build/
#   make template   build the skeleton template too (build/my_extension.<ext>)
#   make clean      remove build/
#
# Everything is built with the system C / C++ compiler and only the vendored
# ABI header in include/ — no pinkdb libraries are linked. The extensions call
# back into the host through the `PinkdbHostApi` struct at load time.
#
# Override the toolchain or flags if needed:
#   make CC=clang CXX=clang++ CFLAGS='-std=c99 -O0 -g -Wall -Wextra -Iinclude'

CC  ?= cc
CXX ?= c++

# Shared-library flag and suffix per platform: .dylib on macOS, .so elsewhere.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SHARED := -dynamiclib
SO_EXT := dylib
else
SHARED := -shared
SO_EXT := so
endif

# Cross-architecture build on macOS (the release workflow builds darwin-x64
# on an Apple Silicon runner): `make TARGET_ARCH=x86_64`.
TARGET_ARCH ?=
ARCH_FLAGS := $(if $(TARGET_ARCH),-arch $(TARGET_ARCH),)

# -Wall -Wextra: the SDK builds warning-free; keep it that way.
CFLAGS   = -std=c99 -O2 -fPIC -Wall -Wextra $(ARCH_FLAGS) -Iinclude
CXXFLAGS = -std=c++17 -O2 -fPIC -Wall -Wextra $(ARCH_FLAGS) -Iinclude

BUILD := build

EXAMPLES := \
	$(BUILD)/math_ext.$(SO_EXT) \
	$(BUILD)/string_ext.$(SO_EXT) \
	$(BUILD)/cpp_ext.$(SO_EXT)

all: $(EXAMPLES)

$(BUILD):
	mkdir -p $(BUILD)

$(BUILD)/math_ext.$(SO_EXT): examples/math_ext.c include/pinkdb_extension.h | $(BUILD)
	$(CC) $(CFLAGS) $(SHARED) -o $@ $<

$(BUILD)/string_ext.$(SO_EXT): examples/string_ext.c include/pinkdb_extension.h | $(BUILD)
	$(CC) $(CFLAGS) $(SHARED) -o $@ $<

$(BUILD)/cpp_ext.$(SO_EXT): examples/cpp_ext.cpp include/pinkdb_extension.h | $(BUILD)
	$(CXX) $(CXXFLAGS) $(SHARED) -o $@ $<

# The skeleton in template/ is not an example; build it on demand only.
template: $(BUILD)/my_extension.$(SO_EXT)

$(BUILD)/my_extension.$(SO_EXT): template/my_extension.c include/pinkdb_extension.h | $(BUILD)
	$(CC) $(CFLAGS) $(SHARED) -o $@ $<

clean:
	rm -rf $(BUILD)

.PHONY: all template clean
