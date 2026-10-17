// Package pinkdb is a cgo binding for the pinkdb C ABI v1 (the
// crates/pinkdb-ffi crate, header crates/pinkdb-ffi/include/pinkdb.h).
//
// Build the native library from the repo root first:
//
//	cargo build --release -p pinkdb-ffi
//
// The cgo directives below resolve the ABI header and the workspace
// target/release directory relative to this package, so a repo checkout needs
// no environment variables: the linker finds libpinkdb_ffi there and the
// release dylib/so is found again at run time through an rpath to the same
// directory.
//
// A *DB wraps one native handle. The C ABI's handles are single-threaded:
// this package serializes every native call with a per-DB mutex, so a single
// *DB may be shared between goroutines. Close frees the native handle and is
// idempotent: once closed, further Close calls are a no-op and ABI() stays
// callable (it is library-wide and does not touch the handle). Execute is the
// only method that reports a closed database, returning ErrClosed instead of
// touching freed memory.
package pinkdb

/*
#cgo CFLAGS: -I${SRCDIR}/../../crates/pinkdb-ffi/include
#cgo LDFLAGS: -L${SRCDIR}/../../target/release -Wl,-rpath,${SRCDIR}/../../target/release -lpinkdb_ffi
#include <stdlib.h>
#include "pinkdb.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// SupportedABI is the C ABI version this binding speaks. Open and OpenMemory
// refuse a linked library that reports a different version.
const SupportedABI = 1

// Errors returned by this package in addition to the native error messages
// from pinkdb_last_error.
var (
	// ErrClosed is returned by DB.Execute after the database has been closed.
	// Close itself is a no-op on an already-closed DB, and ABI() remains
	// callable.
	ErrClosed = errors.New("pinkdb: database is closed")

	// ErrOutOfRange wraps DATE and TIMESTAMP payloads that cannot be
	// represented by Go's time.Time. The guard is defensive-only: no current
	// producer path can emit such a payload. See dateFromDays and
	// timestampFromMicros.
	ErrOutOfRange = errors.New("pinkdb: value is outside time.Time's range")
)

// Statement kind tags; these mirror the PINKDB_STMT_* values in pinkdb.h and
// surface as the Result.Kind strings.
const (
	kindSelected        int32 = 0
	kindCreated         int32 = 1
	kindInserted        int32 = 2
	kindUpdated         int32 = 3
	kindDeleted         int32 = 4
	kindBegan           int32 = 5
	kindCommitted       int32 = 6
	kindRolledBack      int32 = 7
	kindCreatedView     int32 = 8
	kindDroppedTable    int32 = 9
	kindDroppedView     int32 = 10
	kindCreatedDatabase int32 = 11
	kindDroppedDatabase int32 = 12
	kindUsed            int32 = 13
	// kindLoadedExtension is `LOAD EXTENSION '<path>'`; Name carries the
	// extension's declared name.
	kindLoadedExtension int32 = 14
	// kindCheckpointed is `CHECKPOINT` (a no-op for in-memory databases).
	kindCheckpointed int32 = 15
	// kindAlteredTable is `ALTER TABLE`.
	kindAlteredTable int32 = 16
)

var kindNames = map[int32]string{
	kindSelected:        "selected",
	kindCreated:         "created",
	kindInserted:        "inserted",
	kindUpdated:         "updated",
	kindDeleted:         "deleted",
	kindBegan:           "began",
	kindCommitted:       "committed",
	kindRolledBack:      "rolledback",
	kindCreatedView:     "created_view",
	kindDroppedTable:    "dropped_table",
	kindDroppedView:     "dropped_view",
	kindCreatedDatabase: "created_database",
	kindDroppedDatabase: "dropped_database",
	kindUsed:            "used",
	kindLoadedExtension: "loaded_extension",
	kindCheckpointed:    "checkpointed",
	kindAlteredTable:    "altered_table",
}

// Value and column type tags; these mirror the PINKDB_TYPE_* values in
// pinkdb.h and surface as the Result.ColumnTypes strings.
const (
	typeNull      int32 = 0
	typeBoolean   int32 = 1
	typeInteger   int32 = 2
	typeBigint    int32 = 3
	typeDouble    int32 = 4
	typeVarchar   int32 = 5
	typeDate      int32 = 6
	typeTimestamp int32 = 7
	typeDecimal   int32 = 8
	typeInterval  int32 = 9
)

var typeNames = map[int32]string{
	typeNull:      "null",
	typeBoolean:   "boolean",
	typeInteger:   "integer",
	typeBigint:    "bigint",
	typeDouble:    "double",
	typeVarchar:   "varchar",
	typeDate:      "date",
	typeTimestamp: "timestamp",
	typeDecimal:   "decimal",
	typeInterval:  "interval",
}

// Result is one statement's outcome, materialized as Go values.
type Result struct {
	// Kind is one of: "selected", "created", "inserted", "updated",
	// "deleted", "began", "committed", "rolledback", "created_view",
	// "dropped_table", "dropped_view", "created_database",
	// "dropped_database", "used", "loaded_extension", "checkpointed".
	Kind string
	// Columns and ColumnTypes describe a "selected" result (column names and
	// one of "null", "boolean", "integer", "bigint", "double", "varchar",
	// "date", "timestamp"); both are empty for every other kind.
	Columns     []string
	ColumnTypes []string
	// Rows holds a "selected" result's rows as Go values: nil, bool, int64,
	// float64, string, or time.Time (UTC). Empty for every other kind.
	Rows [][]any
	// Affected is the row count of an "inserted"/"updated"/"deleted"
	// statement; 0 otherwise.
	Affected int64
	// Name is the object/session name of a "created_view"/"dropped_table"/
	// "dropped_view"/"created_database"/"dropped_database"/"used" statement,
	// or the extension name of a "loaded_extension" statement; empty when
	// absent.
	Name string
}

// DB is one pinkdb database handle (durable file or in-memory).
//
// The underlying C handle is single-threaded; DB serializes every native call
// with a mutex, so one *DB is goroutine-safe. It must be Close'd to release
// the native handle.
type DB struct {
	mu     sync.Mutex
	handle *C.pinkdb_handle
	closed bool
}

// Open opens a durable database (checkpoint + WAL) at path. Empty paths and
// paths containing a NUL byte are rejected before any native call; the native
// open error message is returned as-is on failure.
func Open(path string) (*DB, error) {
	if path == "" {
		return nil, errors.New("pinkdb: database path must not be empty")
	}
	if strings.IndexByte(path, 0) >= 0 {
		return nil, errors.New("pinkdb: database path contains a NUL byte; the native library takes NUL-terminated C paths")
	}
	// Pinkdb errors are thread-local, so the failing call and the
	// pinkdb_last_error read that follows it must stay on one OS thread.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := checkABI(); err != nil {
		return nil, err
	}
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	handle := C.pinkdb_open(cpath)
	if handle == nil {
		return nil, nativeError()
	}
	return &DB{handle: handle}, nil
}

// OpenMemory opens a fresh, empty in-memory database.
func OpenMemory() (*DB, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := checkABI(); err != nil {
		return nil, err
	}
	handle := C.pinkdb_open_memory()
	if handle == nil { // documented never to fail; stay safe anyway
		return nil, nativeError()
	}
	return &DB{handle: handle}, nil
}

// ABI returns the C ABI version reported by the linked pinkdb-ffi library
// (currently 1). It is a library-wide constant and does not touch the
// database handle; the mutex keeps all native calls serialized.
func (db *DB) ABI() uint32 {
	db.mu.Lock()
	defer db.mu.Unlock()
	return uint32(C.pinkdb_abi_version())
}

// Execute runs one or more ';'-separated SQL statements and materializes one
// Result per statement.
//
// SQL is passed to the native library as (pointer, length), so embedded NUL
// bytes are preserved. Errors carry the message from pinkdb_last_error and
// leave the database usable. After Close, Execute returns ErrClosed (the only
// method that fails on a closed DB).
func (db *DB) Execute(sql string) ([]Result, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}

	// CBytes + explicit length keeps embedded NULs; free the copy below.
	var sqlPtr *C.char
	if len(sql) > 0 {
		sqlPtr = (*C.char)(C.CBytes([]byte(sql)))
		defer C.free(unsafe.Pointer(sqlPtr))
	}
	result, err := db.executeNative(sqlPtr, C.size_t(len(sql)))
	if err != nil {
		return nil, err
	}
	defer C.pinkdb_result_free(result) // freed on success and error paths
	// materialize only reads the result's own memory, which is not
	// thread-local, so it runs after the OS thread has been released.
	return materialize(result)
}

// executeNative runs pinkdb_execute and, when it fails, reads the thread-local
// pinkdb_last_error on the same OS thread. Only that native call and the error
// read need the pinned thread. The caller must hold db.mu and owns the
// returned result (freed with pinkdb_result_free).
func (db *DB) executeNative(sqlPtr *C.char, sqlLen C.size_t) (*C.pinkdb_result, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	result := C.pinkdb_execute(db.handle, sqlPtr, sqlLen)
	if result == nil {
		return nil, nativeError()
	}
	return result, nil
}

// Close frees the native handle. It is idempotent: after the first call,
// further calls are a no-op (there is no handle left to free), and Execute
// returns ErrClosed from then on. ABI() stays callable.
func (db *DB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return
	}
	db.closed = true
	if db.handle != nil {
		C.pinkdb_close(db.handle)
		db.handle = nil
	}
}

// checkABI fails when the linked library is not the ABI version this package
// targets. Called by Open/OpenMemory before any handle is created.
func checkABI() error {
	if v := uint32(C.pinkdb_abi_version()); v != SupportedABI {
		return fmt.Errorf(
			"pinkdb: ABI version mismatch: linked pinkdb-ffi reports version %d, this binding requires version %d",
			v, SupportedABI)
	}
	return nil
}

// nativeError copies the thread-local pinkdb_last_error into a Go error. The
// caller must keep the goroutine locked to its OS thread, since the message
// only exists on the thread that made the failing call.
func nativeError() error {
	msg := C.pinkdb_last_error()
	if msg == nil {
		return errors.New("pinkdb: native call failed without an error message")
	}
	return errors.New(C.GoString(msg))
}

// materialize snapshots every statement of a pinkdb_result into Results before
// the caller frees it.
func materialize(result *C.pinkdb_result) ([]Result, error) {
	count := int(C.pinkdb_result_statement_count(result))
	results := make([]Result, 0, count)
	for stmt := 0; stmt < count; stmt++ {
		tag := int32(C.pinkdb_result_kind(result, C.size_t(stmt)))
		kind, ok := kindNames[tag]
		if !ok {
			return nil, fmt.Errorf("pinkdb: unknown statement kind %d (ABI mismatch?)", tag)
		}
		r := Result{Kind: kind}
		if tag == kindSelected {
			columns := int(C.pinkdb_result_column_count(result, C.size_t(stmt)))
			r.Columns = make([]string, 0, columns)
			r.ColumnTypes = make([]string, 0, columns)
			for col := 0; col < columns; col++ {
				name := C.pinkdb_result_column_name(result, C.size_t(stmt), C.size_t(col))
				if name == nil {
					return nil, fmt.Errorf("pinkdb: statement %d column %d has no name", stmt, col)
				}
				r.Columns = append(r.Columns, C.GoString(name))
				typeTag := int32(C.pinkdb_result_column_type(result, C.size_t(stmt), C.size_t(col)))
				typeName, ok := typeNames[typeTag]
				if !ok {
					return nil, fmt.Errorf("pinkdb: unknown column type tag %d (ABI mismatch?)", typeTag)
				}
				r.ColumnTypes = append(r.ColumnTypes, typeName)
			}
			rows := int(C.pinkdb_result_row_count(result, C.size_t(stmt)))
			r.Rows = make([][]any, 0, rows)
			for row := 0; row < rows; row++ {
				cells := make([]any, 0, columns)
				for col := 0; col < columns; col++ {
					v := C.pinkdb_result_value(result, C.size_t(stmt), C.size_t(row), C.size_t(col))
					value, err := valueToGo(v)
					if err != nil {
						return nil, fmt.Errorf("pinkdb: statement %d row %d column %d: %w", stmt, row, col, err)
					}
					cells = append(cells, value)
				}
				r.Rows = append(r.Rows, cells)
			}
		}
		r.Affected = int64(C.pinkdb_result_affected(result, C.size_t(stmt)))
		if name := C.pinkdb_result_name(result, C.size_t(stmt)); name != nil {
			r.Name = C.GoString(name)
		}
		results = append(results, r)
	}
	return results, nil
}

// valueToGo maps one pink_value to a Go native: NULL to nil, BOOLEAN to bool,
// INTEGER/BIGINT to int64, DOUBLE to float64 (bit pattern preserved, so -0.0,
// NaN and infinities survive), VARCHAR to string, DATE/TIMESTAMP to UTC
// time.Time.
func valueToGo(v C.pink_value) (any, error) {
	if v.is_null != 0 || int32(v.type_tag) == typeNull {
		return nil, nil
	}
	switch int32(v.type_tag) {
	case typeBoolean:
		return int64(v.int64) != 0, nil
	case typeInteger, typeBigint:
		return int64(v.int64), nil
	case typeDouble:
		return float64(v.double_), nil
	case typeVarchar:
		if v.text_ptr == nil || v.text_len == 0 {
			return "", nil
		}
		if uint64(v.text_len) > math.MaxInt32 {
			return nil, fmt.Errorf("pinkdb: VARCHAR value of %d bytes is too large", uint64(v.text_len))
		}
		// Trust text_len, not NUL termination: VARCHAR payloads may contain
		// embedded NULs.
		return C.GoStringN(v.text_ptr, C.int(v.text_len)), nil
	case typeDate:
		return dateFromDays(int64(v.int64))
	case typeTimestamp:
		return timestampFromMicros(int64(v.int64))
	case typeDecimal, typeInterval:
		// DECIMAL and INTERVAL arrive as text cells (the FFI's canonical
		// text); Go has no built-in decimal, so both surface as strings.
		if v.text_ptr == nil || v.text_len == 0 {
			return "", nil
		}
		if uint64(v.text_len) > math.MaxInt32 {
			return nil, fmt.Errorf(
				"pinkdb: text value of %d bytes is too large", uint64(v.text_len))
		}
		return C.GoStringN(v.text_ptr, C.int(v.text_len)), nil
	default:
		return nil, fmt.Errorf("pinkdb: unknown value type tag %d (ABI mismatch?)", int32(v.type_tag))
	}
}

// unixToInternal is the number of seconds between Go's time.Time zero instant
// (January 1, year 1) and the Unix epoch; time.Time stores
// seconds-since-year-1 in an int64, which sets the representable range.
const unixToInternal = 62135596800

const secondsPerDay = 86400

// Date day counts beyond these bounds would overflow int64 seconds once
// converted; dateFromDays rejects them instead of returning a wrapped
// calendar value.
const (
	maxDateDays = (math.MaxInt64 - unixToInternal) / secondsPerDay
	minDateDays = (math.MinInt64 + unixToInternal) / secondsPerDay
)

// dateFromDays converts a DATE payload (days since the Unix epoch) to a UTC
// time.Time at midnight, or fails with ErrOutOfRange when the value cannot be
// represented as an int64 count of seconds since Go's time zero. The guard is
// defensive-only: no current producer path emits such a day count.
func dateFromDays(days int64) (time.Time, error) {
	if days > maxDateDays || days < minDateDays {
		return time.Time{}, fmt.Errorf(
			"pinkdb: DATE value %d (days since epoch) is outside time.Time's range: %w",
			days, ErrOutOfRange)
	}
	return time.Unix(days*secondsPerDay, 0).UTC(), nil
}

// timestampFromMicros converts a TIMESTAMP payload (microseconds since the
// Unix epoch) to a UTC time.Time. Every int64 microsecond count is
// representable as int64 seconds, so the ErrOutOfRange guard below is
// defensive-only: no producer path can reach it.
func timestampFromMicros(us int64) (time.Time, error) {
	sec := us / 1_000_000
	nsec := (us % 1_000_000) * 1_000
	if nsec < 0 { // floor the division so pre-epoch times keep their instant
		nsec += 1_000_000_000
		sec--
	}
	if sec > math.MaxInt64-unixToInternal || sec < math.MinInt64+unixToInternal {
		return time.Time{}, fmt.Errorf(
			"pinkdb: TIMESTAMP value %d (microseconds since epoch) is outside time.Time's range: %w",
			us, ErrOutOfRange)
	}
	return time.Unix(sec, nsec).UTC(), nil
}
