# pinkdb C++ extension SDK — build the examples and the conformance fixture.
#
#   make            build build/cpp_ext.<ext> and build/conformance_ext.<ext>
#   make clean      remove build/
#
# Everything is built with the system C++ compiler and only the vendored ABI
# header plus include/pinkdb_ext.hpp — no pinkdb libraries are linked, no
# third-party libraries are needed. The extensions call back into the host
# through the `PinkdbHostApi` struct at load time.
#
# Override the toolchain or flags if needed:
#   make CXX=clang++ CXXFLAGS='-std=c++17 -O0 -g -Wall -Wextra -Iinclude'

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

# Cross-architecture build on macOS (the release workflow builds darwin-x64 on
# an Apple Silicon runner): `make TARGET_ARCH=x86_64`.
TARGET_ARCH ?=
ARCH_FLAGS := $(if $(TARGET_ARCH),-arch $(TARGET_ARCH),)

# -Werror: the SDK builds warning-free; keep it that way.
CXXFLAGS = -std=c++17 -O2 -fPIC -Wall -Wextra -Werror $(ARCH_FLAGS) -Iinclude

BUILD := build

EXTENSIONS := \
	$(BUILD)/cpp_ext.$(SO_EXT) \
	$(BUILD)/conformance_ext.$(SO_EXT)

all: $(EXTENSIONS)

$(BUILD):
	mkdir -p $(BUILD)

$(BUILD)/cpp_ext.$(SO_EXT): examples/cpp_ext.cpp include/pinkdb_ext.hpp include/pinkdb_extension.h | $(BUILD)
	$(CXX) $(CXXFLAGS) $(SHARED) -o $@ $<

$(BUILD)/conformance_ext.$(SO_EXT): conformance/conformance_ext.cpp include/pinkdb_ext.hpp include/pinkdb_extension.h | $(BUILD)
	$(CXX) $(CXXFLAGS) $(SHARED) -o $@ $<

clean:
	rm -rf $(BUILD)

.PHONY: all clean
