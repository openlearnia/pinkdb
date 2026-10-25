// Command conformance_ext is the Go implementation of the pinkdb extension ABI
// conformance fixture (see ext-sdk/conformance/README.md). It is a
// -buildmode=c-shared library that the loader's conformance tests load and
// assert against, exactly like the C, C++ and Rust fixtures — one contract,
// four SDKs.
//
// It registers the fixture's functions (cf_echo … cf_thread), all built on the
// SDK's pinkdb package, and additionally exports three introspection symbols
// that are not part of the ABI:
//
//	int64_t pinkdb_conformance_calls(void);            // callbacks invoked
//	int64_t pinkdb_conformance_foreign_threads(void);  // ... on another thread
//	int64_t pinkdb_conformance_caps(void);             // behaviour bit set
//
// Build it:
//
//	make -C ext-sdk/go                                    # -> build/conformance_ext.dylib
//	go build -buildmode=c-shared -o build/conformance_ext.dylib ./conformance
package main

/*
#cgo CFLAGS: -I${SRCDIR}/../include

#include "pinkdb_extension.h"
#include <pthread.h>
#include <stdint.h>

// One //exported callback per fixture function. cgo declares each as
// `int32_t cf_x(void *user_data, void *args, size_t n_args, void *out)` (Go
// hands the value structs over as opaque pointers; the pinkdb package owns
// their layout), and the accessor hands each one to Go as an opaque pointer for
// registration — Go cannot take the address of a C symbol.
#define PINKDB_CF_FUNCTIONS(X)                                                 \
    X(cf_echo) X(cf_bool) X(cf_int) X(cf_bigint) X(cf_double) X(cf_date)       \
    X(cf_timestamp) X(cf_interval) X(cf_variant) X(cf_all) X(cf_empty)       \
    X(cf_fill) X(cf_fail) X(cf_unwind) X(cf_volatile) X(cf_thread)

#define PINKDB_CF_EXTERN(name) \
    extern int32_t name(void *user_data, void *args, size_t n_args, void *out);
#define PINKDB_CF_ACCESSOR(name) \
    static inline void *name##_ptr(void) { return (void *)name; }

PINKDB_CF_FUNCTIONS(PINKDB_CF_EXTERN)
PINKDB_CF_FUNCTIONS(PINKDB_CF_ACCESSOR)

// The OS thread the running callback is on. Callbacks run on the host's rayon
// workers, so comparing this with the thread that ran init is what the
// foreign-thread counter counts.
static inline uintptr_t pinkdb_cf_self(void) {
    return (uintptr_t)pthread_self();
}
*/
import "C"

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/openlearnia/pinkdb/ext-sdk/go/pinkdb"
)

// caps advertises which of the fixture's two awkward behaviours this SDK really
// performs: bit 0 (cf_fail allocates its text before returning code 1) and
// bit 1 (cf_unwind really panics inside the callback, which the SDK contains).
const caps int64 = 1 | 2

// api is the host API copy taken during init; read-only once callbacks run.
var api *pinkdb.API

// The counters behind the introspection exports. Atomics because callbacks run
// concurrently on host threads.
var (
	callCount    atomic.Int64
	foreignCount atomic.Int64
)

// initThread is the OS thread pinkdb_extension_init ran on. Every callback on
// any other thread proves the ABI's "no thread affinity" rule.
var initThread atomic.Uintptr

// impl is one fixture function's body: the arguments it was registered for and
// the host-owned result to fill.
type impl func(args []pinkdb.Value, out *pinkdb.Value) int32

// invoke is the single entry path of every callback. It wraps the body in
// pinkdb.Guard (a panic must never unwind into C), keeps the call counters, and
// converts the raw pointers into the SDK's values.
func invoke(body impl, argsPtr, outPtr unsafe.Pointer, nArgs C.size_t) C.int32_t {
	return C.int32_t(pinkdb.Guard(func() int32 {
		callCount.Add(1)
		if uintptr(C.pinkdb_cf_self()) != initThread.Load() {
			foreignCount.Add(1)
		}
		return body(pinkdb.Args(argsPtr, int(nArgs)), pinkdb.Out(outPtr))
	}))
}

// ---------------------------------------------------------------------------
// Value passthrough: every tag, in both directions, NULL in -> NULL out
// ---------------------------------------------------------------------------

// cfEcho returns the argument's bytes verbatim: embedded NULs, multibyte text
// and the empty string all survive, because the ABI carries a length, never a
// terminator.
func cfEcho(args []pinkdb.Value, out *pinkdb.Value) int32 {
	if len(args) < 1 {
		return 1
	}
	if args[0].IsNull != 0 {
		return 0
	}
	return api.SetText(unsafe.Pointer(out), args[0].Bytes())
}

func cfBool(args []pinkdb.Value, out *pinkdb.Value) int32 {
	if len(args) < 1 {
		return 1
	}
	if args[0].IsNull != 0 {
		return 0
	}
	*out = pinkdb.Bool(args[0].Bool())
	return 0
}

func cfInt(args []pinkdb.Value, out *pinkdb.Value) int32 {
	if len(args) < 1 {
		return 1
	}
	if args[0].IsNull != 0 {
		return 0
	}
	*out = pinkdb.Int(args[0].Int())
	return 0
}

func cfBigint(args []pinkdb.Value, out *pinkdb.Value) int32 {
	if len(args) < 1 {
		return 1
	}
	if args[0].IsNull != 0 {
		return 0
	}
	*out = pinkdb.Bigint(args[0].Bigint())
	return 0
}

// cfDouble returns the argument bit for bit: the payload travels as a double,
// so NaN payloads and ±inf are the host's bits, not a re-formatted decimal.
func cfDouble(args []pinkdb.Value, out *pinkdb.Value) int32 {
	if len(args) < 1 {
		return 1
	}
	if args[0].IsNull != 0 {
		return 0
	}
	*out = pinkdb.Float(args[0].Float())
	return 0
}

func cfDate(args []pinkdb.Value, out *pinkdb.Value) int32 {
	if len(args) < 1 {
		return 1
	}
	if args[0].IsNull != 0 {
		return 0
	}
	*out = pinkdb.Date(args[0].Date())
	return 0
}

func cfTimestamp(args []pinkdb.Value, out *pinkdb.Value) int32 {
	if len(args) < 1 {
		return 1
	}
	if args[0].IsNull != 0 {
		return 0
	}
	*out = pinkdb.Timestamp(args[0].Timestamp())
	return 0
}

// cfInterval round-trips the three calendar fields. months/days are a bit
// pattern, micros a number — the SDK's Interval/accessor pair is what keeps the
// two from being confused.
func cfInterval(args []pinkdb.Value, out *pinkdb.Value) int32 {
	if len(args) < 1 {
		return 1
	}
	if args[0].IsNull != 0 {
		return 0
	}
	months, days, micros := args[0].Interval()
	*out = pinkdb.Interval(months, days, micros)
	return 0
}

// cfVariant passes VARIANT bytes through unchanged: an opaque payload on the
// text channel, published with the host's allocator.
func cfVariant(args []pinkdb.Value, out *pinkdb.Value) int32 {
	if len(args) < 1 {
		return 1
	}
	if args[0].IsNull != 0 {
		return 0
	}
	return api.SetVariant(unsafe.Pointer(out), args[0].VariantBytes())
}

// cfAll renders the canonical one-line summary the fixture defines:
//
//	b=<bool>,i=<int>,big=<bigint>,d=<double bits>,v=<varchar>,date=<days>,ts=<micros>,iv=<months>/<days>/<micros>
//
// Doubles render as their 16-digit hex bit pattern so four languages' float
// formatting never enters the contract; a NULL argument renders as `null`.
func cfAll(args []pinkdb.Value, out *pinkdb.Value) int32 {
	if len(args) < 8 {
		return 1
	}
	var b strings.Builder
	b.WriteString("b=")
	if args[0].IsNull != 0 {
		b.WriteString("null")
	} else if args[0].Bool() {
		b.WriteString("1")
	} else {
		b.WriteString("0")
	}
	writeInt := func(prefix string, v pinkdb.Value, n int64) {
		b.WriteString(prefix)
		if v.IsNull != 0 {
			b.WriteString("null")
			return
		}
		fmt.Fprintf(&b, "%d", n)
	}
	writeInt(",i=", args[1], int64(args[1].Int()))
	writeInt(",big=", args[2], args[2].Bigint())
	b.WriteString(",d=")
	if args[3].IsNull != 0 {
		b.WriteString("null")
	} else {
		fmt.Fprintf(&b, "0x%016x", math.Float64bits(args[3].Float()))
	}
	b.WriteString(",v=")
	if args[4].IsNull != 0 {
		b.WriteString("null")
	} else {
		b.Write(args[4].Bytes())
	}
	writeInt(",date=", args[5], int64(args[5].Date()))
	writeInt(",ts=", args[6], args[6].Timestamp())
	b.WriteString(",iv=")
	if args[7].IsNull != 0 {
		b.WriteString("null")
	} else {
		months, days, micros := args[7].Interval()
		fmt.Fprintf(&b, "%d/%d/%d", months, days, micros)
	}
	return api.SetText(unsafe.Pointer(out), []byte(b.String()))
}

// cfEmpty is a zero-length, non-NULL VARCHAR: no allocation, text_ptr NULL and
// text_len 0, which the host reads as the empty string.
func cfEmpty(args []pinkdb.Value, out *pinkdb.Value) int32 {
	return api.SetText(unsafe.Pointer(out), nil)
}

// cfFill allocates n bytes of 'x' through the host's alloc; a negative n is a
// NULL result, not an error.
func cfFill(args []pinkdb.Value, out *pinkdb.Value) int32 {
	if len(args) < 1 {
		return 1
	}
	if args[0].IsNull != 0 {
		return 0
	}
	n := args[0].Bigint()
	if n < 0 {
		return 0
	}
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = 'x'
	}
	return api.SetText(unsafe.Pointer(out), buf)
}

// cfFail publishes a host-allocated 4-byte text and then fails with code 1: the
// host must free the buffer on its error path, and the fixture's cap bit 0 says
// this really happens (not a pointer that was never allocated).
func cfFail(args []pinkdb.Value, out *pinkdb.Value) int32 {
	if code := api.SetText(unsafe.Pointer(out), []byte("oops")); code != 0 {
		return code
	}
	return 1
}

// cfUnwind is the SDK's worst case: a real Go panic inside the callback. The
// SDK contains it (pinkdb.Guard) and reports a clean error code, and the
// fixture's cap bit 1 says the panic is real rather than a pretend code.
func cfUnwind(args []pinkdb.Value, out *pinkdb.Value) int32 {
	panic("conformance_ext: cf_unwind panics on purpose")
}

// cfVolatile is registered with PINKDB_EXT_FLAG_VOLATILE: a function that is not
// a pure function of its arguments, declared honestly.
func cfVolatile(args []pinkdb.Value, out *pinkdb.Value) int32 {
	*out = pinkdb.Bigint(42)
	return 0
}

// cfThread is a plain callback; the counters in invoke are what the test reads.
func cfThread(args []pinkdb.Value, out *pinkdb.Value) int32 {
	*out = pinkdb.Bigint(1)
	return 0
}

// ---------------------------------------------------------------------------
// Exported callbacks
// ---------------------------------------------------------------------------

// The //export comment must sit immediately above its function.

//export cf_echo
func cf_echo(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfEcho, args, out, nArgs)
}

//export cf_bool
func cf_bool(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfBool, args, out, nArgs)
}

//export cf_int
func cf_int(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfInt, args, out, nArgs)
}

//export cf_bigint
func cf_bigint(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfBigint, args, out, nArgs)
}

//export cf_double
func cf_double(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfDouble, args, out, nArgs)
}

//export cf_date
func cf_date(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfDate, args, out, nArgs)
}

//export cf_timestamp
func cf_timestamp(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfTimestamp, args, out, nArgs)
}

//export cf_interval
func cf_interval(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfInterval, args, out, nArgs)
}

//export cf_variant
func cf_variant(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfVariant, args, out, nArgs)
}

//export cf_all
func cf_all(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfAll, args, out, nArgs)
}

//export cf_empty
func cf_empty(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfEmpty, args, out, nArgs)
}

//export cf_fill
func cf_fill(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfFill, args, out, nArgs)
}

//export cf_fail
func cf_fail(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfFail, args, out, nArgs)
}

//export cf_unwind
func cf_unwind(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfUnwind, args, out, nArgs)
}

//export cf_volatile
func cf_volatile(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfVolatile, args, out, nArgs)
}

//export cf_thread
func cf_thread(userData, args unsafe.Pointer, nArgs C.size_t, out unsafe.Pointer) C.int32_t {
	return invoke(cfThread, args, out, nArgs)
}

// ---------------------------------------------------------------------------
// Introspection (not part of the ABI; the loader tests read these directly)
// ---------------------------------------------------------------------------

//export pinkdb_conformance_calls
func pinkdb_conformance_calls() C.int64_t {
	return C.int64_t(callCount.Load())
}

//export pinkdb_conformance_foreign_threads
func pinkdb_conformance_foreign_threads() C.int64_t {
	return C.int64_t(foreignCount.Load())
}

//export pinkdb_conformance_caps
func pinkdb_conformance_caps() C.int64_t {
	return C.int64_t(caps)
}

// ---------------------------------------------------------------------------
// ABI entry points
// ---------------------------------------------------------------------------

// extensionName is copied by the host during the load; it lives for the process
// lifetime.
var extensionName = C.CString("conformance_ext")

//export pinkdb_extension_abi_version
func pinkdb_extension_abi_version() C.uint32_t {
	return C.PINKDB_EXT_ABI_VERSION
}

//export pinkdb_extension_name
func pinkdb_extension_name() *C.char {
	return extensionName
}

// registration is one row of the fixture's function table.
type registration struct {
	name     string
	args     []int32
	ret      int32
	fn       unsafe.Pointer
	volatile bool
}

// registrations is the fixture's contract table, in README order. cf_volatile is
// the only volatile one: it is here to prove the SDK can declare the flag (and
// fails the load on a host that cannot accept it).
var registrations = []registration{
	{"cf_echo", []int32{pinkdb.TypeVarchar}, pinkdb.TypeVarchar, C.cf_echo_ptr(), false},
	{"cf_bool", []int32{pinkdb.TypeBoolean}, pinkdb.TypeBoolean, C.cf_bool_ptr(), false},
	{"cf_int", []int32{pinkdb.TypeInteger}, pinkdb.TypeInteger, C.cf_int_ptr(), false},
	{"cf_bigint", []int32{pinkdb.TypeBigint}, pinkdb.TypeBigint, C.cf_bigint_ptr(), false},
	{"cf_double", []int32{pinkdb.TypeDouble}, pinkdb.TypeDouble, C.cf_double_ptr(), false},
	{"cf_date", []int32{pinkdb.TypeDate}, pinkdb.TypeDate, C.cf_date_ptr(), false},
	{"cf_timestamp", []int32{pinkdb.TypeTimestamp}, pinkdb.TypeTimestamp, C.cf_timestamp_ptr(), false},
	{"cf_interval", []int32{pinkdb.TypeInterval}, pinkdb.TypeInterval, C.cf_interval_ptr(), false},
	{"cf_variant", []int32{pinkdb.TypeVariant}, pinkdb.TypeVariant, C.cf_variant_ptr(), false},
	{"cf_all", []int32{
		pinkdb.TypeBoolean, pinkdb.TypeInteger, pinkdb.TypeBigint, pinkdb.TypeDouble,
		pinkdb.TypeVarchar, pinkdb.TypeDate, pinkdb.TypeTimestamp, pinkdb.TypeInterval,
	}, pinkdb.TypeVarchar, C.cf_all_ptr(), false},
	{"cf_empty", nil, pinkdb.TypeVarchar, C.cf_empty_ptr(), false},
	{"cf_fill", []int32{pinkdb.TypeBigint}, pinkdb.TypeVarchar, C.cf_fill_ptr(), false},
	{"cf_fail", []int32{pinkdb.TypeBigint}, pinkdb.TypeVarchar, C.cf_fail_ptr(), false},
	{"cf_unwind", nil, pinkdb.TypeBigint, C.cf_unwind_ptr(), false},
	{"cf_volatile", nil, pinkdb.TypeBigint, C.cf_volatile_ptr(), true},
	{"cf_thread", nil, pinkdb.TypeBigint, C.cf_thread_ptr(), false},
}

//export pinkdb_extension_init
func pinkdb_extension_init(hostAPI, hostCtx unsafe.Pointer) C.int32_t {
	return C.int32_t(pinkdb.Guard(func() int32 {
		a, code := pinkdb.Init(hostAPI, hostCtx)
		if code != 0 {
			return code
		}
		// Record the init thread before registering: every callback that lands
		// on a different one is what the foreign-thread counter reports.
		initThread.Store(uintptr(C.pinkdb_cf_self()))
		for _, r := range registrations {
			var c int32
			if r.volatile {
				c = a.RegisterVolatile(r.name, r.args, r.ret, r.fn, nil)
			} else {
				c = a.Register(r.name, r.args, r.ret, r.fn, nil)
			}
			if c != 0 {
				return 10 // rejected registration: the host reports the function
			}
		}
		api = a
		return 0
	}))
}

// A c-shared library still needs a main package and a main function. For
// -buildmode=c-shared it is never called by the host.
func main() {}
