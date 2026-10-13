// pinkdb extension example — string_ext (ABI v1, built with cgo).
//
// Registers two scalar functions:
//
//	ext_go_repeat(s VARCHAR, n INTEGER) -> VARCHAR   -- s repeated n times
//	ext_go_len(s VARCHAR)               -> BIGINT    -- UTF-8 byte length
//
// Both demonstrate the ABI's result-text ownership: the bytes are allocated
// with the host's alloc (through the pinkdb_go_alloc shim), and the host
// copies text_len bytes and frees the buffer with the host's free immediately
// after the callback returns. VARCHAR *inputs* are host-owned and must never
// be freed. NULL inputs yield NULL.
//
// Both callbacks are reentrant: the only shared state is the copied
// PinkdbHostApi struct, written once in init and read-only afterwards; the
// host's alloc/free are thread-safe (callbacks run concurrently on the host's
// rayon workers). Build it with:
//
//	make -C ext-sdk/go            # -> build/string_ext.dylib (.so on Linux)
//	LOAD EXTENSION '/abs/path/to/build/string_ext.dylib';
//	SELECT ext_go_repeat('ab', 3), ext_go_len('héllo');
package main

/*
#cgo CFLAGS: -I${SRCDIR}/../include

#include "pinkdb_extension.h"
#include <stdlib.h>

// ---------------------------------------------------------------------------
// C shims. Every one is `static`: the //export preamble is compiled into two
// translation units, and a non-static definition would be a duplicate symbol.
// `inline` keeps an unused shim warning-free under -Wall.
// ---------------------------------------------------------------------------

// Host services. Go cannot call C function pointers (api->register_scalar,
// api->alloc, api->free), so each host service gets a one-line shim. The
// PinkdbHostApi pointer is the copy the Go side made during init.
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

// The //exported callbacks, declared exactly as cgo declares them (cgo does
// not emit `const` on pointer parameters, so this spelling must match).
extern int32_t ext_go_repeat(void *user_data, pinkdb_ext_value *args,
                             size_t n_args, pinkdb_ext_value *out);
extern int32_t ext_go_len(void *user_data, pinkdb_ext_value *args,
                          size_t n_args, pinkdb_ext_value *out);

// Go cannot take the address of a C symbol, so static accessors hand each
// callback to Go for registration. The casts only drop `const` from the
// argument pointer (const is not part of the calling convention).
static inline PinkdbExtScalarFn ext_go_repeat_fn(void) {
    return (PinkdbExtScalarFn)ext_go_repeat;
}
static inline PinkdbExtScalarFn ext_go_len_fn(void) {
    return (PinkdbExtScalarFn)ext_go_len;
}
*/
import "C"

import (
	"unsafe"
)

// maxRepeatBytes caps a single ext_go_repeat result. A huge repeat count is
// an error code, never a crash or an unbounded allocation.
const maxRepeatBytes = 1 << 20 // 1 MiB

// hostAPI is a copy of the host's PinkdbHostApi struct, taken during
// pinkdb_extension_init (the host's struct itself is only valid for that
// call). Callbacks run later, on other threads, and need alloc/free.
var hostAPI C.PinkdbHostApi

// extensionName is the NUL-terminated name the host copies during the load.
// It is allocated once and intentionally lives for the process lifetime.
var extensionName = C.CString("string_ext")

// argAt returns argument i, guarding the arity the way a callback should:
// the host promises exactly the registered arity, but a contract violation
// must become an error code, not a crash.
func argAt(args *C.pinkdb_ext_value, nArgs C.size_t, i int) C.pinkdb_ext_value {
	return unsafe.Slice((*C.pinkdb_ext_value)(unsafe.Pointer(args)), int(nArgs))[i]
}

// bytesOf borrows a VARCHAR argument's bytes for the duration of the call.
func bytesOf(v C.pinkdb_ext_value) []byte {
	if v.text_len == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(v.text_ptr)), int(v.text_len))
}

// ---------------------------------------------------------------------------
// Callbacks
// ---------------------------------------------------------------------------

// ext_go_repeat(s, n): s concatenated n times.
//
//   - NULL in -> NULL out (either argument);
//   - n <= 0 -> an empty, non-NULL string (allocated so the host always has a
//     valid pointer to free);
//   - the result is capped at maxRepeatBytes: longer results are error code 2
//     rather than an unbounded allocation.
//
//export ext_go_repeat
func ext_go_repeat(userData unsafe.Pointer, args *C.pinkdb_ext_value, nArgs C.size_t, out *C.pinkdb_ext_value) (code C.int32_t) {
	defer func() {
		if recover() != nil {
			code = 1
		}
	}()

	if args == nil || nArgs < 2 {
		return 1
	}
	s := argAt(args, nArgs, 0)
	n := argAt(args, nArgs, 1)
	if s.is_null != 0 || n.is_null != 0 {
		out.is_null = 1
		return 0
	}

	src := bytesOf(s)
	reps := int64(n.int64) // INTEGER payload (i32 range), widened

	// Guard the multiplication before allocating: a huge repeat count is an
	// error, not an overflow or a crash.
	var total int64
	if reps > 0 && len(src) > 0 {
		if int64(len(src)) > maxRepeatBytes/reps {
			return 2
		}
		total = int64(len(src)) * reps
	}

	// Allocate one extra byte for a trailing NUL (not required by the ABI;
	// written for easier debugging).
	buf := C.pinkdb_go_alloc(&hostAPI, C.size_t(total+1))
	if buf == nil {
		return 1
	}
	dst := unsafe.Slice((*byte)(buf), int(total)+1)
	for pos := 0; pos < int(total); pos += len(src) {
		copy(dst[pos:], src)
	}
	dst[total] = 0

	out.type_tag = C.PINKDB_EXT_TYPE_VARCHAR
	out.is_null = 0
	out.text_ptr = (*C.char)(buf)
	out.text_len = C.size_t(total)
	return 0
}

// ext_go_len(s): the length of s in UTF-8 *bytes*.
//
// This deliberately differs from the built-in length(), which counts
// characters: ext_go_len('héllo') is 6 while length('héllo') is 5. It also
// proves that multibyte VARCHAR bytes survive the ABI round trip untouched.
//
//export ext_go_len
func ext_go_len(userData unsafe.Pointer, args *C.pinkdb_ext_value, nArgs C.size_t, out *C.pinkdb_ext_value) (code C.int32_t) {
	defer func() {
		if recover() != nil {
			code = 1
		}
	}()

	if args == nil || nArgs < 1 {
		return 1
	}
	s := argAt(args, nArgs, 0)
	if s.is_null != 0 {
		out.is_null = 1
		return 0
	}

	out.type_tag = C.PINKDB_EXT_TYPE_BIGINT
	out.is_null = 0
	out.int64 = C.int64_t(s.text_len)
	return 0
}

// ---------------------------------------------------------------------------
// ABI entry points
// ---------------------------------------------------------------------------

//export pinkdb_extension_abi_version
func pinkdb_extension_abi_version() C.uint32_t {
	return C.PINKDB_EXT_ABI_VERSION
}

//export pinkdb_extension_name
func pinkdb_extension_name() *C.char {
	return extensionName
}

//export pinkdb_extension_init
func pinkdb_extension_init(api *C.PinkdbHostApi, hostCtx unsafe.Pointer) (code C.int32_t) {
	defer func() {
		if recover() != nil {
			code = 2
		}
	}()

	if api == nil || api.abi_version != C.PINKDB_EXT_ABI_VERSION {
		return 1
	}
	if uint64(api.struct_size) < uint64(unsafe.Sizeof(hostAPI)) {
		return 2
	}

	hostAPI = *api

	var tRepeat = [2]C.int32_t{C.PINKDB_EXT_TYPE_VARCHAR, C.PINKDB_EXT_TYPE_INTEGER}
	var tLen = [1]C.int32_t{C.PINKDB_EXT_TYPE_VARCHAR}

	repeatName := C.CString("ext_go_repeat")
	defer C.free(unsafe.Pointer(repeatName))
	if C.pinkdb_go_register_scalar(api, hostCtx, repeatName, 2, &tRepeat[0],
		C.PINKDB_EXT_TYPE_VARCHAR, C.ext_go_repeat_fn(), nil) != 0 {
		return 10
	}

	lenName := C.CString("ext_go_len")
	defer C.free(unsafe.Pointer(lenName))
	if C.pinkdb_go_register_scalar(api, hostCtx, lenName, 1, &tLen[0],
		C.PINKDB_EXT_TYPE_BIGINT, C.ext_go_len_fn(), nil) != 0 {
		return 11
	}

	return 0
}

// A c-shared library still needs a main package and a main function. For
// -buildmode=c-shared it is never called by the host; the Go runtime starts
// when the library is loaded and serves the exported functions.
func main() {}
