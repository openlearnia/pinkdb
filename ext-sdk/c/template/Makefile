# Standalone Makefile for the pinkdb extension skeleton.
#
#   make        build my_extension.dylib (my_extension.so on Linux)
#   make clean  remove the built library
#
# Uses the system C compiler. The only include path needed is the vendored
# ABI header in ../include; no pinkdb libraries are linked (the host provides
# everything through the PinkdbHostApi struct at load time).

CC ?= cc
CFLAGS ?= -std=c99 -O2 -Wall -Wextra
INCLUDES := -I../include
LIB := my_extension

# Shared-library flag and suffix per platform.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SHARED := -dynamiclib
SUFFIX := dylib
else
SHARED := -shared
SUFFIX := so
endif

all: $(LIB).$(SUFFIX)

$(LIB).$(SUFFIX): my_extension.c ../include/pinkdb_extension.h
	$(CC) $(CFLAGS) -fPIC $(INCLUDES) $(SHARED) -o $@ my_extension.c

clean:
	rm -f $(LIB).$(SUFFIX)

.PHONY: all clean
