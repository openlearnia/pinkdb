package pinkdb

import (
	"math"
	"unsafe"
)

// Value / argument type tags, in the header's numbering (the pinkdb C ABI's
// numbering, plus 9 for INTERVAL and 10 for VARIANT). Tag 8 is that ABI's
// DECIMAL, which extension ABI v1 deliberately does not support.
const (
	TypeNull      int32 = 0
	TypeBoolean   int32 = 1
	TypeInteger   int32 = 2
	TypeBigint    int32 = 3
	TypeDouble    int32 = 4
	TypeVarchar   int32 = 5
	TypeDate      int32 = 6
	TypeTimestamp int32 = 7
	TypeInterval  int32 = 9
	TypeVariant   int32 = 10
)

// Value mirrors the C `pinkdb_ext_value` struct field for field, so a
// *Value is a *pinkdb_ext_value. It is exported as a Go struct rather than
// used through cgo's C type because the C type belongs to the calling package:
// a callback there receives a pointer, and [Args] / [Out] turn it into a
// []Value / *Value without either side sharing a Go type.
//
// The payload field that carries a value depends on TypeTag: Int64 for
// BOOLEAN (0/1), INTEGER, BIGINT, DATE (days since 1970-01-01) and TIMESTAMP
// (microseconds since the epoch); Double for DOUBLE; TextPtr/TextLen for
// VARCHAR; and for INTERVAL both — micros in Int64, months and days packed
// into Double's bit pattern. Never compare or format Double directly for an
// INTERVAL; use [Value.Interval].
type Value struct {
	TypeTag int32
	IsNull  int32
	Int64   int64
	Double  float64
	TextPtr *byte
	TextLen uintptr
}

// Args turns the callback's args pointer into a slice of the host's argument
// array. The host guarantees exactly the registered arity; the array is
// host-owned and read-only, so take values out of it, never write to it.
//
// A zero-argument callback receives a nil pointer, which Args turns into a
// nil slice.
func Args(args unsafe.Pointer, n int) []Value {
	if args == nil || n <= 0 {
		return nil
	}
	return unsafe.Slice((*Value)(args), n)
}

// Out turns the callback's out pointer into the writable, host-owned result
// the callback fills:
//
//	*pinkdb.Out(out) = pinkdb.Bigint(42)
//
// It arrives pre-filled with NULL, so a callback that returns NULL leaves it
// alone. Assigning a whole Value is deliberate: every non-NULL result must set
// its tag, clear IsNull and fill the matching payload, and a partial write is
// exactly the bug the ABI's "declared type" check catches.
func Out(out unsafe.Pointer) *Value { return (*Value)(out) }

// Null is a NULL value. For a result, leaving out untouched does the same.
func Null() Value { return Value{IsNull: 1} }

// Bool builds a BOOLEAN value.
func Bool(b bool) Value {
	v := Value{TypeTag: TypeBoolean}
	if b {
		v.Int64 = 1
	}
	return v
}

// Int builds an INTEGER value.
func Int(i int32) Value { return Value{TypeTag: TypeInteger, Int64: int64(i)} }

// Bigint builds a BIGINT value.
func Bigint(i int64) Value { return Value{TypeTag: TypeBigint, Int64: i} }

// Float builds a DOUBLE value.
func Float(f float64) Value { return Value{TypeTag: TypeDouble, Double: f} }

// Bytes builds a VARCHAR value that borrows b. A borrowed value is only valid
// until b is freed or overwritten, which is why this form is for arguments and
// for copying elsewhere — a *result* goes through [API.SetText] so the host
// owns and frees the bytes.
func Bytes(b []byte) Value {
	v := Value{TypeTag: TypeVarchar, TextLen: uintptr(len(b))}
	if len(b) > 0 {
		v.TextPtr = &b[0]
	}
	return v
}

// Variant builds a VARIANT value that borrows b: opaque pinkdb-encoded bytes
// on the same text channel as VARCHAR. A *result* goes through
// [API.SetVariant] so the host owns and frees the bytes.
func Variant(b []byte) Value {
	v := Value{TypeTag: TypeVariant, TextLen: uintptr(len(b))}
	if len(b) > 0 {
		v.TextPtr = &b[0]
	}
	return v
}

// Date builds a DATE value from days since 1970-01-01.
func Date(days int32) Value { return Value{TypeTag: TypeDate, Int64: int64(days)} }

// Timestamp builds a TIMESTAMP value from microseconds since the epoch.
func Timestamp(micros int64) Value { return Value{TypeTag: TypeTimestamp, Int64: micros} }

// Interval builds an INTERVAL value: a calendar interval with DuckDB's three
// fields. The by-value struct cannot grow a field without breaking ABI v1, so
// months and days ride in the 64-bit *pattern* of Double —
// `(uint32(months) << 32) | uint32(days)` — and micros in Int64. Both scalars
// are written here so callers never touch the packing.
func Interval(months, days int32, micros int64) Value {
	bits := uint64(uint32(months))<<32 | uint64(uint32(days))
	return Value{
		TypeTag: TypeInterval,
		Int64:   micros,
		Double:  math.Float64frombits(bits),
	}
}

// VariantBytes borrows a VARIANT value's payload (opaque pinkdb-encoded
// bytes). Valid only for the duration of the callback, like [Value.Bytes].
func (v Value) VariantBytes() []byte { return v.Bytes() }

// Bool reads a BOOLEAN value.
func (v Value) Bool() bool { return v.Int64 != 0 }

// Int reads an INTEGER value.
func (v Value) Int() int32 { return int32(v.Int64) }

// Bigint reads a BIGINT value.
func (v Value) Bigint() int64 { return v.Int64 }

// Float reads a DOUBLE value.
func (v Value) Float() float64 { return v.Double }

// Date reads a DATE value as days since 1970-01-01.
func (v Value) Date() int32 { return int32(v.Int64) }

// Timestamp reads a TIMESTAMP value as microseconds since the epoch.
func (v Value) Timestamp() int64 { return v.Int64 }

// Interval reads an INTERVAL value's three fields. The months/days payload is
// a bit pattern in Double, never a number: reading it as a float would compare
// bit patterns that mean nothing as a number (and a NaN pattern would poison
// any arithmetic).
func (v Value) Interval() (months, days int32, micros int64) {
	bits := math.Float64bits(v.Double)
	return int32(bits >> 32), int32(bits), v.Int64
}

// Bytes borrows a VARCHAR value's bytes. The bytes are host-owned and valid
// only for the duration of the callback: copy them (or hand them to
// [API.SetText], which copies) if they must outlive the call.
func (v Value) Bytes() []byte {
	if v.TextPtr == nil || v.TextLen == 0 {
		return nil
	}
	return unsafe.Slice(v.TextPtr, int(v.TextLen))
}
