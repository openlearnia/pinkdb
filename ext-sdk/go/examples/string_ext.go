// pinkdb extension example — string_ext (ABI v1, built with cgo on the SDK's
// pinkdb package).
//
// Registers two scalar functions:
//
//	ext_go_repeat(s VARCHAR, n INTEGER) -> VARCHAR   -- s repeated n times
//	ext_go_len(s VARCHAR)               -> BIGINT    -- UTF-8 byte length
//
// Both demonstrate the ABI's result-text ownership: the bytes are allocated
// with the host's alloc (through pinkdb.API.SetText), and the host copies
// text_len bytes and frees the buffer with the host's free immediately after
// the callback returns. VARCHAR *inputs* are host-owned and must never be
// freed. NULL inputs yield NULL.
//
// Both callbacks are reentrant: the only shared state is the *pinkdb.API built
// in init and read-only afterwards; the host's alloc/free are thread-safe
// (callbacks run concurrently on the host's rayon workers). Build it with:
//
//	make -C ext-sdk/go            # -> build/string_ext.dylib (.so on Linux)
//	LOAD EXTENSION '/abs/path/to/build/string_ext.dylib';
//	SELECT ext_go_repeat('ab', 3), ext_go_len('héllo');
package main

/*
#cgo CFLAGS: -I${SRCDIR}/../include

#include "pinkdb_extension.h"
#include <stdlib.h>

// The //exported callbacks, declared exactly as cgo declares them: cgo does not
// emit `const` on pointer parameters, and the value structs cross the boundary
// as opaque pointers (the pinkdb package owns their layout).

extern int32_t ext_go_repeat(void *user_data, void *args, size_t n_args, void *out);
extern int32_t ext_go_len(void *user_data, void *args, size_t n_args, void *out);

// Go cannot take the address of a C symbol, so a static accessor hands each
// callback to Go as an opaque pointer for pinkdb.API.Register. The cast changes
// nothing about the calling convention; it only lets Go hold the value.
static inline void *ext_go_repeat_fn(void) {
    return (void *)ext_go_repeat;
}
static inline void *ext_go_len_fn(void) {
    return (void *)ext_go_len;
}
*/
import "C"

import (
	"unsafe"

	"github.com/openlearnia/pinkdb/ext-sdk/go/pinkdb"
)

// maxRepeatBytes caps a single ext_go_repeat result. A huge repeat count is
// an error code, never a crash or an unbounded allocation.
const maxRepeatBytes = 1 << 20 // 1 MiB

// api is the host API copy taken during pinkdb_extension_init. It is written
// once, before the host can call any callback, and read-only afterwards, which
// is what makes it safe for concurrent callbacks.
var api *pinkdb.API

// ---------------------------------------------------------------------------
// Callbacks
// ---------------------------------------------------------------------------

// ext_go_repeat(s, n): s concatenated n times.
//
//   - NULL in -> NULL out (either argument);
//   - n <= 0 -> an empty, non-NULL string (the host pre-fills out with NULL;
//     SetText publishes a zero-length result without allocating);
//   - the result is capped at maxRepeatBytes: longer results are error code 2
//     rather than an unbounded allocation.
//
//export ext_go_repeat
func ext_go_repeat(userData unsafe.Pointer, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return C.int32_t(pinkdb.Guard(func() int32 {
		argv := pinkdb.Args(args, int(nArgs))
		if len(argv) < 2 {
			return 1
		}
		if argv[0].IsNull != 0 || argv[1].IsNull != 0 {
			return 0 // out is already NULL
		}

		src := argv[0].Bytes()
		reps := int64(argv[1].Int())

		// Guard the multiplication before allocating: a huge repeat count is an
		// error, not an overflow or a crash.
		var total int64
		if reps > 0 && len(src) > 0 {
			if int64(len(src)) > maxRepeatBytes/reps {
				return 2
			}
			total = int64(len(src)) * reps
		}

		buf := make([]byte, total)
		for pos := 0; pos < len(buf); pos += len(src) {
			copy(buf[pos:], src)
		}
		return api.SetText(out, buf)
	}))
}

// ext_go_len(s): the length of s in UTF-8 *bytes*.
//
// This deliberately differs from the built-in length(), which counts
// characters: ext_go_len('héllo') is 6 while length('héllo') is 5. It also
// proves that multibyte VARCHAR bytes survive the ABI round trip untouched.
//
//export ext_go_len
func ext_go_len(userData unsafe.Pointer, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return C.int32_t(pinkdb.Guard(func() int32 {
		argv := pinkdb.Args(args, int(nArgs))
		if len(argv) < 1 {
			return 1
		}
		if argv[0].IsNull != 0 {
			return 0 // out is already NULL
		}
		*pinkdb.Out(out) = pinkdb.Bigint(int64(argv[0].TextLen))
		return 0
	}))
}

// ---------------------------------------------------------------------------
// ABI entry points
// ---------------------------------------------------------------------------

// extensionName is the NUL-terminated name the host copies during the load. It
// is allocated once and intentionally lives for the process lifetime.
var extensionName = C.CString("string_ext")

//export pinkdb_extension_abi_version
func pinkdb_extension_abi_version() C.uint32_t {
	return C.PINKDB_EXT_ABI_VERSION
}

//export pinkdb_extension_name
func pinkdb_extension_name() *C.char {
	return extensionName
}

//export pinkdb_extension_init
func pinkdb_extension_init(hostAPI unsafe.Pointer, hostCtx unsafe.Pointer) C.int32_t {
	return C.int32_t(pinkdb.Guard(func() int32 {
		a, code := pinkdb.Init(hostAPI, hostCtx)
		if code != 0 {
			return code
		}
		if c := a.Register("ext_go_repeat",
			[]int32{pinkdb.TypeVarchar, pinkdb.TypeInteger},
			pinkdb.TypeVarchar, C.ext_go_repeat_fn(), nil); c != 0 {
			return 10
		}
		if c := a.Register("ext_go_len",
			[]int32{pinkdb.TypeVarchar},
			pinkdb.TypeBigint, C.ext_go_len_fn(), nil); c != 0 {
			return 11
		}
		api = a
		return 0
	}))
}

// A c-shared library still needs a main package and a main function. For
// -buildmode=c-shared it is never called by the host; the Go runtime starts
// when the library is loaded and serves the exported functions.
func main() {}
