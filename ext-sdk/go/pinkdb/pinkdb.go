// Package pinkdb is the Go side of the pinkdb loadable-extension ABI (v1,
// minor 2). It factors out the cgo plumbing every Go extension repeats —
// the init-scoped host-API copy, registration, result-text ownership, value
// construction and panic containment — so an extension is callbacks plus
// registration, not four copies of the same C shims.
//
// The contract itself lives in the vendored header,
// `ext-sdk/go/include/pinkdb_extension.h`; read it once. What this package
// adds is the Go-specific shape of that contract:
//
//   - Everything a callback needs is reached through an *API built by [Init],
//     which copies the host's PinkdbHostApi (the host documents that struct as
//     valid for the init call only) and records what the host negotiated via
//     `struct_size`. The copy is written once and read-only afterwards, which
//     is why callbacks may use it without synchronization.
//
//   - Callbacks run CONCURRENTLY, on the host's rayon worker threads — threads
//     the Go runtime did not create, and never the thread that ran init. Any
//     state a callback touches beyond its *API and its arguments must be
//     guarded by Go synchronization (sync, sync/atomic); the host's
//     alloc/free are thread-safe.
//
//   - A Go panic must never unwind into C: it aborts the host process. [Guard]
//     wraps a callback body in a deferred recover and turns a panic into a
//     documented error code, so one bad call fails its statement instead of
//     the server.
//
//   - Result text is host memory: [API.SetText] allocates through the host's
//     alloc, and the host copies and frees it immediately after the callback
//     returns. Text that came from the host ([API.ExecuteStatement]) is
//     released through the host's free, never through Go's collector.
//
// This ABI is a scalar-function ABI. Table functions, scans and copy sinks are
// not part of it: those are the *linked* extension model
// (`pinkdb-ext-loader`'s linked registry, compiled into the host), so a loadable
// extension — Go included — cannot provide them, and nothing in this package
// pretends otherwise.
package pinkdb

/*
#cgo CFLAGS: -I${SRCDIR}/../include

#include "pinkdb_extension.h"
#include <stdlib.h>

// ---------------------------------------------------------------------------
// C shims. Every one is `static inline`: they exist only to reach the C
// function pointers in the host API, which Go cannot call directly.
// ---------------------------------------------------------------------------

// The `api` argument is always the Go-owned copy of the host's struct (see
// API.raw), never the host's init-scoped original.
static inline int32_t pinkdb_go_register_scalar(void *api, void *ctx,
                                               const char *name, uint32_t n_args,
                                               const int32_t *arg_types, int32_t ret_type,
                                               void *fn, void *user_data) {
    return ((const PinkdbHostApi *)api)->register_scalar(
        ctx, name, n_args, arg_types, ret_type, (PinkdbExtScalarFn)fn, user_data);
}

static inline int32_t pinkdb_go_register_scalar_flags(void *api, void *ctx,
                                                     const char *name, uint32_t n_args,
                                                     const int32_t *arg_types, int32_t ret_type,
                                                     uint32_t flags, void *fn,
                                                     void *user_data) {
    return ((const PinkdbHostApi *)api)->register_scalar_flags(
        ctx, name, n_args, arg_types, ret_type, flags, (PinkdbExtScalarFn)fn,
        user_data);
}

static inline void *pinkdb_go_alloc(void *api, size_t size) {
    return ((const PinkdbHostApi *)api)->alloc(size);
}

static inline void pinkdb_go_free(void *api, void *ptr) {
    ((const PinkdbHostApi *)api)->free(ptr);
}

static inline int32_t pinkdb_go_execute(void *api, void *ctx, const char *sql,
                                        size_t sql_len, char **out_text,
                                        size_t *out_len) {
    return ((const PinkdbHostApi *)api)->execute_statement(ctx, sql, sql_len,
                                                           out_text, out_len);
}
*/
import "C"

import (
	"fmt"
	"os"
	"unsafe"
)

// Codes this package returns from [Init], [Guard], [API.RegisterVolatile] and
// [API.ExecuteStatement]. Registration codes are the host's own: they are
// returned unchanged, because they name the host's rejection reason.
const (
	// ErrABIVersion: the host speaks a different ABI major version. Never
	// guess — a mismatched major version means the struct layout may differ.
	ErrABIVersion int32 = 1
	// ErrStructSize: the host's PinkdbHostApi is too small for this header
	// (struct_size < PINKDB_EXT_API_V1_SIZE). Also returned by
	// ExecuteStatement when the host predates the execute_statement field.
	ErrStructSize int32 = 2
	// ErrPanic: Guard recovered a Go panic at the ABI boundary.
	ErrPanic int32 = 3
	// ErrNoFlags: RegisterVolatile was used on a host without
	// register_scalar_flags (struct_size < PINKDB_EXT_API_FLAGS_SIZE).
	ErrNoFlags int32 = 4
	// ErrUsage: this package was called wrongly — a nil host API, a nil
	// callback, or a nil out pointer. A programming error, not a host one.
	ErrUsage int32 = 5
)

// Function flags for [API.RegisterVolatile].
const (
	// FlagVolatile declares a nondeterministic function. The engine refuses to
	// execute such a call in any statement it WAL-logs (INSERT, UPDATE,
	// DELETE, DDL, CALL, ...), because replaying the logged SQL later could
	// produce a different value.
	FlagVolatile uint32 = C.PINKDB_EXT_FLAG_VOLATILE
)

// hostHeader mirrors the two fields every host version starts with:
// negotiation reads them before anything else about the struct is known.
type hostHeader struct {
	abiVersion uint32
	structSize uint32
}

// Value mirrors pinkdb_ext_value field for field (see value.go). Asserting the
// sizes match in both directions keeps the mirror honest: a drift makes one
// array length negative and the package stops compiling.
var (
	_ [unsafe.Sizeof(C.pinkdb_ext_value{}) - unsafe.Sizeof(Value{})]byte
	_ [unsafe.Sizeof(Value{}) - unsafe.Sizeof(C.pinkdb_ext_value{})]byte
)

// API holds the host services an extension was granted, negotiated and copied
// during init, plus the opaque host context to pass back to registration and
// statements. The zero value is unusable; build one with [Init].
//
// An *API is immutable after Init: the copy, the context and the negotiation
// flags are written once, before any callback can run (the host only starts
// calling once init has returned). That immutability is what makes it safe
// for concurrent callbacks without locks.
type API struct {
	// raw is a Go-owned byte copy of the host's PinkdbHostApi. The host's
	// struct is init-scoped; this copy outlives it and is what the C shims
	// dereference. It contains only C function pointers, no Go pointers, so
	// handing its address back to C is legal, and it is never written after
	// Init. It is allocated at this header's size (never the host's, which may
	// be larger): bytes past our landmark stay zero and are never read.
	raw []byte

	hostCtx unsafe.Pointer

	hasFlags   bool
	hasExecute bool
}

// ptr returns the address of the copied host struct for the C shims.
func (a *API) ptr() unsafe.Pointer { return unsafe.Pointer(&a.raw[0]) }

// Init negotiates with the host and takes the copy of its API that callbacks
// live on. `api` is the PinkdbHostApi pointer the host passed to
// pinkdb_extension_init (or to pinkdb_ext_serve; both are negotiated the same
// way); pass it as unsafe.Pointer because the C type belongs to the calling
// package.
//
// It returns a non-zero code — and no API — when the host cannot be trusted:
// [ErrABIVersion] for a different ABI major version, [ErrStructSize] when the
// host is older than PINKDB_EXT_API_V1_SIZE. Calling it twice (once in init,
// once per serve call) is expected: each call copies the struct it was handed,
// and only the init-time copy is used by callbacks.
func Init(api unsafe.Pointer, hostCtx unsafe.Pointer) (*API, int32) {
	if api == nil {
		return nil, ErrUsage
	}
	// Reading the two leading fields before checking struct_size is safe: every
	// version of the struct starts with them.
	head := (*hostHeader)(api)
	if head.abiVersion != uint32(C.PINKDB_EXT_ABI_VERSION) {
		return nil, ErrABIVersion
	}
	size := uint64(head.structSize)
	if size < uint64(C.PINKDB_EXT_API_V1_SIZE) {
		return nil, ErrStructSize
	}
	// Copy our own header's size: fields the host appended beyond it are not
	// ours to interpret, and leaving them zero means a stub of ours can never
	// be mistaken for a host function.
	known := int(C.PINKDB_EXT_API_MINOR2_SIZE)
	raw := make([]byte, known)
	n := known
	if size < uint64(known) {
		n = int(size)
	}
	copy(raw, unsafe.Slice((*byte)(api), n))
	return &API{
		raw:        raw,
		hostCtx:    hostCtx,
		hasFlags:   size >= uint64(C.PINKDB_EXT_API_FLAGS_SIZE),
		hasExecute: size >= uint64(C.PINKDB_EXT_API_MINOR2_SIZE),
	}, 0
}

// HasFlags reports whether the host has the additive
// PINKDB_EXT_API_FLAGS_SIZE fields, i.e. register_scalar_flags. A host that
// does not must never be given a volatile registration — see
// [API.RegisterVolatile].
func (a *API) HasFlags() bool { return a != nil && a.hasFlags }

// HasExecute reports whether the host has the ABI minor 2
// execute_statement field. Only a serve-time API set by a current host does.
func (a *API) HasExecute() bool { return a != nil && a.hasExecute }

// Register declares one deterministic scalar function. `args` are the declared
// PINKDB_EXT_TYPE_* tags (no TypeNull: NULL is not an argument type) and must
// be empty for a zero-argument function; `ret` is the tag the callback
// produces; `fn` is the //exported callback as an unsafe.Pointer, because Go
// cannot take the address of a C symbol and the C type belongs to the calling
// package; `userData` is handed back to the callback unchanged and must be
// thread-safe.
//
// Returns 0 when registered, otherwise the host's rejection code: names are
// case-insensitively unique, `fn` must not be nil, `args` must hold valid
// tags, and a rejected registration fails the whole load even if init then
// returns 0. Only valid during pinkdb_extension_init.
func (a *API) Register(name string, args []int32, ret int32, fn, userData unsafe.Pointer) int32 {
	return a.register(name, args, ret, 0, fn, userData)
}

// RegisterVolatile declares one nondeterministic scalar function — one that
// may read the clock, a random source or other ambient state — through the
// host's register_scalar_flags. The engine then refuses to execute it in any
// statement it WAL-logs, which is the only way it can guarantee that a
// replayed statement produces the value the original session saw.
//
// A volatile declaration must never silently become a deterministic one, so on
// a host without register_scalar_flags (an older struct_size) this returns
// [ErrNoFlags] and registers nothing: fail init with that code rather than
// falling back to Register.
func (a *API) RegisterVolatile(name string, args []int32, ret int32, fn, userData unsafe.Pointer) int32 {
	if !a.HasFlags() {
		return ErrNoFlags
	}
	return a.register(name, args, ret, FlagVolatile, fn, userData)
}

func (a *API) register(name string, args []int32, ret int32, flags uint32, fn, userData unsafe.Pointer) int32 {
	if a == nil || len(a.raw) == 0 || fn == nil {
		return ErrUsage
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname)) // C.CString memory, not result text

	// The host copies the tags during the call, so a Go-backed array is fine
	// here: it holds no Go pointers.
	var tags *C.int32_t
	if len(args) > 0 {
		buf := make([]C.int32_t, len(args))
		for i, tag := range args {
			buf[i] = C.int32_t(tag)
		}
		tags = &buf[0]
	}

	if flags == 0 {
		return int32(C.pinkdb_go_register_scalar(a.ptr(), a.hostCtx, cname,
			C.uint32_t(len(args)), tags, C.int32_t(ret), fn, userData))
	}
	return int32(C.pinkdb_go_register_scalar_flags(a.ptr(), a.hostCtx, cname,
		C.uint32_t(len(args)), tags, C.int32_t(ret), C.uint32_t(flags), fn, userData))
}

// SetText publishes b as the callback's VARCHAR result: the bytes are copied
// into a buffer from the host's alloc, and the host frees that buffer right
// after the callback returns — on success, on a NULL result and on an error
// return alike. `out` is the callback's out pointer (as unsafe.Pointer,
// because the C struct type belongs to the calling package).
//
// An empty b sets a zero-length, non-NULL result with no allocation at all,
// which is what the host asks for: NULL text_ptr with text_len 0.
//
// Returns 0 on success, non-zero when the host allocator failed (the result
// then stays NULL, so the statement fails with a code rather than a crash).
func (a *API) SetText(out unsafe.Pointer, b []byte) int32 {
	return a.setBytes(out, TypeVarchar, b)
}

// SetVariant publishes b as the callback's VARIANT result: the bytes are
// opaque pinkdb-encoded data, allocated and freed exactly like VARCHAR text.
func (a *API) SetVariant(out unsafe.Pointer, b []byte) int32 {
	return a.setBytes(out, TypeVariant, b)
}

func (a *API) setBytes(out unsafe.Pointer, tag int32, b []byte) int32 {
	if a == nil || out == nil {
		return ErrUsage
	}
	v := (*Value)(out)
	if len(b) == 0 {
		v.TypeTag = tag
		v.IsNull = 0
		v.TextPtr = nil
		v.TextLen = 0
		return 0
	}
	p := C.pinkdb_go_alloc(a.ptr(), C.size_t(len(b)))
	if p == nil {
		return 1
	}
	copy(unsafe.Slice((*byte)(p), len(b)), b)
	v.TypeTag = tag
	v.IsNull = 0
	v.TextPtr = (*byte)(p)
	v.TextLen = uintptr(len(b))
	return 0
}

// FreeText releases host-allocated text through the host's free. The host's
// alloc/free are a pair; result text that SetText allocated is freed by the
// host for you, so this is for text the host handed to the extension — the
// buffers ExecuteStatement returns. Freeing anything else corrupts the host
// heap.
func (a *API) FreeText(p unsafe.Pointer) {
	if a == nil || p == nil {
		return
	}
	C.pinkdb_go_free(a.ptr(), p)
}

// ExecuteStatement asks the host to run one statement in the live session, the
// ABI minor 2 surface behind pinkdb_ext_serve: the host applies its own
// read-only policy, limits and role checks, and reports what it decided.
//
// It returns the host's code and the result text (Markdown on success, a JSON
// error object for code 1). The text is host-allocated, not NUL-terminated,
// and is copied here and released through the host's free, so the caller owns
// an ordinary Go slice. Code 2 means there is no live session — the API came
// from init (where the host installs a stub), not from serve — and is also
// returned when the host predates the field; treat it as "cannot ask".
//
// This is a serve-path call: it is only meaningful while pinkdb_ext_serve runs,
// and a callback must not use it (callbacks have no session to reach).
func (a *API) ExecuteStatement(sql []byte) (int32, []byte) {
	if !a.HasExecute() {
		return ErrStructSize, nil
	}
	var outText *C.char
	var outLen C.size_t
	var sqlPtr *C.char
	if len(sql) > 0 {
		sqlPtr = (*C.char)(unsafe.Pointer(&sql[0]))
	}
	code := int32(C.pinkdb_go_execute(a.ptr(), a.hostCtx, sqlPtr,
		C.size_t(len(sql)), &outText, &outLen))
	if outText == nil {
		return code, nil
	}
	text := append([]byte(nil), unsafe.Slice((*byte)(unsafe.Pointer(outText)), int(outLen))...)
	a.FreeText(unsafe.Pointer(outText))
	return code, text
}

// Guard runs body and turns a Go panic into [ErrPanic] instead of letting it
// unwind into the host. Every //exported entry point — pinkdb_extension_init,
// pinkdb_ext_serve and every callback — must go through it: a panic crossing an
// `extern "C"` boundary is undefined behavior in the host and aborts the
// process in Go.
//
// A contained panic is reported on stderr with its value, because the error
// code the host prints cannot carry it. Recovered is not repaired: the
// callback's result is abandoned and the enclosing statement fails.
func Guard(body func() int32) (code int32) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr,
				"pinkdb extension: contained panic at the ABI boundary: %v\n", r)
			code = ErrPanic
		}
	}()
	return body()
}
