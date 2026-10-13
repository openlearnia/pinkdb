// Command my_extension builds a pinkdb loadable extension (ABI v1) as a
// c-shared library with cgo.
//
// Build it (from this directory):
//
//	make                       # -> build/my_extension.dylib (.so on Linux)
//	CGO_ENABLED=1 go build -buildmode=c-shared -o build/my_extension.dylib .
//
// Load it from a pinkdb REPL (release build):
//
//	LOAD EXTENSION '/abs/path/to/my_extension.dylib';
//	SELECT my_ext_greet('world');        -- Hello, world!
//	SELECT my_ext_greet(NULL) IS NULL;   -- true
//
// This file is a complete, buildable extension that registers one scalar:
//
//	my_ext_greet(name VARCHAR) -> VARCHAR      -- "Hello, <name>!"
//
// The contract is documented in ../include/pinkdb_extension.h — read it once
// before writing real code. The Go-specific rules that bite most:
//
//  1. A panic must never cross the ABI boundary. cgo cannot unwind into the
//     host's C frames: a panic in an exported function aborts the whole
//     process. Every callback and pinkdb_extension_init recovers and returns
//     a non-zero code instead.
//  2. VARCHAR results must be allocated with the host's alloc (through the
//     pinkdb_go_alloc shim), never with C.malloc and never in Go memory. The
//     host copies text_len bytes and calls the host's free immediately after
//     the call, on every path (including error and NULL returns). VARCHAR
//     *inputs* are host-owned and borrowed for the duration of the call only.
//  3. Callbacks run CONCURRENTLY on the host's rayon worker threads — threads
//     the Go runtime did not create. All shared state must be synchronized
//     (the host's alloc/free are thread-safe). Do not assume init and the
//     callbacks run on the same thread.
//  4. Go cannot call C function pointers, so the preamble wraps each host
//     service (register_scalar/alloc/free) in a small static C shim, and the
//     //exported callback is handed back to Go by a static accessor.
//
// Keep the preamble free of non-static definitions: with //export present it
// is emitted into two translation units (see the README's sharp edges).
package main

/*
#cgo CFLAGS: -I${SRCDIR}/../include

#include "pinkdb_extension.h"
#include <stdlib.h>

// ---------------------------------------------------------------------------
// C shims. Every one is `static inline`: the //export preamble is compiled
// into two translation units, and a non-static definition would be a
// duplicate symbol. `inline` keeps an unused shim warning-free under -Wall.
// ---------------------------------------------------------------------------

// Host services. Go cannot call C function pointers (api->register_scalar,
// api->alloc, api->free), so each host service gets a one-line shim. The
// PinkdbHostApi pointer is the copy the Go side made during init (the host's
// struct itself is only valid during pinkdb_extension_init).
static inline int32_t pinkdb_go_register_scalar(const PinkdbHostApi *api, void *ctx,
                                                const char *name, uint32_t n_args,
                                                const int32_t *arg_types, int32_t ret_type,
                                                PinkdbExtScalarFn fn, void *user_data) {
    return api->register_scalar(ctx, name, n_args, arg_types, ret_type, fn, user_data);
}

static inline void *pinkdb_go_alloc(const PinkdbHostApi *api, size_t size) {
    return api->alloc(size);
}

static inline void pinkdb_go_free(const PinkdbHostApi *api, void *ptr) {
    api->free(ptr);
}

// The //exported callback, declared exactly as cgo declares it. cgo does not
// emit `const` on pointer parameters, so this spelling must match cgo's
// generated declaration (see the README's sharp edges).
extern int32_t my_ext_greet(void *user_data, pinkdb_ext_value *args,
                            size_t n_args, pinkdb_ext_value *out);

// Go cannot take the address of a C symbol either, so a static accessor hands
// the callback to Go for registration. The cast only drops `const` from the
// argument pointer (const is not part of the calling convention).
static inline PinkdbExtScalarFn my_ext_greet_fn(void) {
    return (PinkdbExtScalarFn)my_ext_greet;
}
*/
import "C"

import (
	"unsafe"
)

// hostAPI is a copy of the host's PinkdbHostApi struct, taken during
// pinkdb_extension_init (the host's struct itself is only valid for that
// call). Callbacks run later, on other threads, and need the host's alloc and
// free; the copy is small POD and read-only after init.
var hostAPI C.PinkdbHostApi

// extensionName is the NUL-terminated name the host copies during the load.
// It is allocated once and intentionally lives for the process lifetime: the
// header promises the buffer may outlive the call.
var extensionName = C.CString("my_extension")

// ---------------------------------------------------------------------------
// Scalar callback
// ---------------------------------------------------------------------------

// my_ext_greet(name VARCHAR) -> VARCHAR
//
// Returns "Hello, <name>!" for a non-NULL input, NULL for NULL input.
//
// The //export comment must sit immediately above the function (no blank
// line); cgo emits a C-ABI wrapper with this exact symbol name. The named
// `code` result lets the deferred recover turn a panic into a non-zero
// return code instead of a process abort.
//
//export my_ext_greet
func my_ext_greet(userData unsafe.Pointer, args *C.pinkdb_ext_value, nArgs C.size_t, out *C.pinkdb_ext_value) (code C.int32_t) {
	// Rule 1: no panics across the boundary.
	defer func() {
		if recover() != nil {
			code = 1
		}
	}()

	// The host calls with exactly the registered arity (1); guard anyway so a
	// contract violation is an error code, not a crash.
	if args == nil || nArgs < 1 {
		return 1
	}
	arg := unsafe.Slice((*C.pinkdb_ext_value)(unsafe.Pointer(args)), int(nArgs))[0]

	// NULL in -> NULL out. `out` arrives pre-filled with NULL; setting
	// is_null is the whole answer.
	if arg.is_null != 0 {
		out.is_null = 1
		return 0
	}

	// Borrow the input bytes for the duration of the call (rule 2).
	name := unsafe.Slice((*byte)(unsafe.Pointer(arg.text_ptr)), int(arg.text_len))

	// Build the message in Go memory first ("Hello, " + name + "!").
	msg := make([]byte, 0, len("Hello, ")+len(name)+1)
	msg = append(msg, "Hello, "...)
	msg = append(msg, name...)
	msg = append(msg, '!')

	// The result buffer must come from the host's alloc: the host copies
	// text_len bytes and frees it with the host's free after the call. A
	// trailing NUL is neither required nor read; we write one for easier
	// debugging.
	buf := C.pinkdb_go_alloc(&hostAPI, C.size_t(len(msg)+1))
	if buf == nil {
		return 1 // allocation failure: non-zero aborts the statement
	}
	dst := unsafe.Slice((*byte)(buf), len(msg)+1)
	copy(dst, msg)
	dst[len(msg)] = 0

	out.type_tag = C.PINKDB_EXT_TYPE_VARCHAR
	out.is_null = 0
	out.text_ptr = (*C.char)(buf)
	out.text_len = C.size_t(len(msg))
	return 0
}

// ---------------------------------------------------------------------------
// ABI entry points (the loader looks these up by name)
// ---------------------------------------------------------------------------

// Must return PINKDB_EXT_ABI_VERSION; any other value aborts the load.
//
//export pinkdb_extension_abi_version
func pinkdb_extension_abi_version() C.uint32_t {
	return C.PINKDB_EXT_ABI_VERSION
}

// NUL-terminated UTF-8, non-empty, at most 256 bytes; copied by the host.
//
//export pinkdb_extension_name
func pinkdb_extension_name() *C.char {
	return extensionName
}

// pinkdb_extension_init runs exactly once, on the loading thread, after the
// version and name checks. Register every function here; a rejected
// registration fails the load even when init returns 0, so check every
// return value. `hostCtx` is opaque: pass it back unchanged.
//
//export pinkdb_extension_init
func pinkdb_extension_init(api *C.PinkdbHostApi, hostCtx unsafe.Pointer) (code C.int32_t) {
	// init is called through the same C boundary as callbacks.
	defer func() {
		if recover() != nil {
			code = 2
		}
	}()

	// Defensive checks: refuse a host we do not understand explicitly.
	if api == nil || api.abi_version != C.PINKDB_EXT_ABI_VERSION {
		return 1
	}
	if uint64(api.struct_size) < uint64(unsafe.Sizeof(hostAPI)) {
		return 2 // host predates this header's struct — too old
	}

	// Keep the host services for the callbacks (valid only during this call).
	hostAPI = *api

	// Registration metadata: the host copies the name and the type tags
	// during the call, so locals are fine. A NULL literal passes through as
	// NULL; a non-NULL argument must be CAST to the exact declared type.
	var argTypes = [1]C.int32_t{C.PINKDB_EXT_TYPE_VARCHAR}
	name := C.CString("my_ext_greet")
	defer C.free(unsafe.Pointer(name))
	if C.pinkdb_go_register_scalar(api, hostCtx, name, 1, &argTypes[0],
		C.PINKDB_EXT_TYPE_VARCHAR, C.my_ext_greet_fn(), nil) != 0 {
		return 10 // the name collides with a built-in, or the registration was invalid
	}
	return 0
}

// A c-shared library still needs a main package and a main function. For
// -buildmode=c-shared it is never called by the host; the Go runtime starts
// when the library is loaded and serves the exported functions.
func main() {}
