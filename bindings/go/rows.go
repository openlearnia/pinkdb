package pinkdb

/*
#include <stdint.h>
#include <stddef.h>

typedef struct pinkdb_result pinkdb_result;
typedef struct pinkdb_data_chunk pinkdb_data_chunk;
typedef struct pinkdb_vector pinkdb_vector;

size_t pinkdb_result_statement_count(const pinkdb_result *result);
int32_t pinkdb_result_kind(const pinkdb_result *result, size_t statement);
size_t pinkdb_result_column_count(const pinkdb_result *result, size_t statement);
const char *pinkdb_result_column_name(
	const pinkdb_result *result, size_t statement, size_t column);
int32_t pinkdb_result_column_type(
	const pinkdb_result *result, size_t statement, size_t column);
int32_t pinkdb_result_column_decimal_scale(
	const pinkdb_result *result, size_t statement, size_t column);
const char *pinkdb_result_error(const pinkdb_result *result);
const char *pinkdb_result_error_class(const pinkdb_result *result);
void pinkdb_result_free(pinkdb_result *result);
pinkdb_data_chunk *pinkdb_result_fetch_chunk(
	pinkdb_result *result, size_t statement);
void pinkdb_destroy_chunk(pinkdb_data_chunk *chunk);
size_t pinkdb_chunk_size(const pinkdb_data_chunk *chunk);
const pinkdb_vector *pinkdb_chunk_column(
	const pinkdb_data_chunk *chunk, size_t column);
*/
import "C"

import (
	"fmt"
	"reflect"
)

// Rows iterates a single row-producing statement one chunk at a time. It is
// not safe for concurrent use. Close it (or read to the end) to release the
// native cursor and let the DB run another statement.
type Rows struct {
	db     *DB
	result *C.pinkdb_result
	stmt   C.size_t

	columns     []string
	columnTypes []string
	tags        []int32
	scales      []int32

	chunk   *C.pinkdb_data_chunk
	vectors []*C.pinkdb_vector
	size    int
	row     int

	err    error
	closed bool
}

func newRows(result *C.pinkdb_result) (*Rows, error) {
	if count := int(C.pinkdb_result_statement_count(result)); count != 1 {
		return nil, fmt.Errorf("pinkdb: Query needs exactly one statement, got %d", count)
	}
	tag := int32(C.pinkdb_result_kind(result, C.size_t(0)))
	if tag != kindSelected {
		kind, ok := kindNames[tag]
		if !ok {
			kind = fmt.Sprintf("kind %d", tag)
		}
		return nil, fmt.Errorf("pinkdb: Query needs a row-producing statement, got %q", kind)
	}
	columns := int(C.pinkdb_result_column_count(result, 0))
	r := &Rows{
		result:      result,
		stmt:        0,
		columns:     make([]string, 0, columns),
		columnTypes: make([]string, 0, columns),
		tags:        make([]int32, columns),
		scales:      make([]int32, columns),
	}
	for col := 0; col < columns; col++ {
		name := C.pinkdb_result_column_name(result, 0, C.size_t(col))
		if name == nil {
			return nil, fmt.Errorf("pinkdb: column %d has no name", col)
		}
		r.columns = append(r.columns, C.GoString(name))
		tag := int32(C.pinkdb_result_column_type(result, 0, C.size_t(col)))
		typeName, ok := typeName(tag)
		if !ok {
			return nil, fmt.Errorf("pinkdb: unknown column type tag %d (ABI mismatch?)", tag)
		}
		r.columnTypes = append(r.columnTypes, typeName)
		r.tags[col] = tag
		r.scales[col] = int32(C.pinkdb_result_column_decimal_scale(result, 0, C.size_t(col)))
	}
	return r, nil
}

// Columns returns the column names.
func (r *Rows) Columns() []string { return r.columns }

// ColumnTypes returns one pinkdb type name per column.
func (r *Rows) ColumnTypes() []string { return r.columnTypes }

// Next advances to the next row, returning false at the end or on an error.
// Check Err afterwards to tell the two apart.
func (r *Rows) Next() bool {
	if r.closed {
		return false
	}
	if r.chunk != nil {
		r.row++
		if r.row < r.size {
			return true
		}
		C.pinkdb_destroy_chunk(r.chunk)
		r.chunk = nil
		r.vectors = nil
	}
	for {
		chunk := C.pinkdb_result_fetch_chunk(r.result, r.stmt)
		if chunk == nil {
			err := resultError(r.result)
			r.finish(err)
			return false
		}
		size := int(C.pinkdb_chunk_size(chunk))
		if size == 0 {
			C.pinkdb_destroy_chunk(chunk)
			continue
		}
		r.chunk = chunk
		r.size = size
		r.row = 0
		r.vectors = make([]*C.pinkdb_vector, len(r.columns))
		for col := range r.columns {
			r.vectors[col] = C.pinkdb_chunk_column(chunk, C.size_t(col))
		}
		return true
	}
}

// Scan copies the current row into the destinations, which must be non-nil
// pointers. A nil destination value carries SQL NULL; otherwise the Go value
// must be assignable or convertible to the pointed-to type (so *any,
// *string, *int64, *float64, *bool and *time.Time all work).
func (r *Rows) Scan(dest ...any) error {
	if r.closed || r.chunk == nil || r.row >= r.size {
		return fmt.Errorf("pinkdb: Scan called without Next")
	}
	if len(dest) != len(r.columns) {
		return fmt.Errorf(
			"pinkdb: Scan destinations = %d, columns = %d", len(dest), len(r.columns))
	}
	for col, target := range dest {
		value, err := vectorToGo(r.result, 0, r.row, col, r.vectors[col], r.tags[col], r.scales[col])
		if err != nil {
			return fmt.Errorf("pinkdb: row %d column %d: %w", r.row, col, err)
		}
		if err := assignCell(target, value); err != nil {
			return fmt.Errorf("pinkdb: row %d column %d: %w", r.row, col, err)
		}
	}
	return nil
}

// Err reports why Next stopped, or nil at a clean end.
func (r *Rows) Err() error { return r.err }

// Close releases the native result and the database's busy flag. It is
// idempotent and safe after DB.Close.
func (r *Rows) Close() error {
	r.finish(nil)
	return nil
}

func (r *Rows) finish(err error) {
	if r.closed {
		return
	}
	r.closed = true
	if err != nil {
		r.err = err
	}
	if r.chunk != nil {
		C.pinkdb_destroy_chunk(r.chunk)
		r.chunk = nil
		r.vectors = nil
	}
	if r.result != nil {
		C.pinkdb_result_free(r.result)
		r.result = nil
	}
	if r.db != nil {
		db := r.db
		r.db = nil
		db.mu.Lock()
		if db.rows == r {
			db.rows = nil
		}
		db.mu.Unlock()
	}
}

func assignCell(dest any, value any) error {
	target := reflect.ValueOf(dest)
	if target.Kind() != reflect.Pointer || target.IsNil() {
		return fmt.Errorf("Scan destination must be a non-nil pointer, got %T", dest)
	}
	target = target.Elem()
	if value == nil {
		target.SetZero()
		return nil
	}
	source := reflect.ValueOf(value)
	if source.Type().AssignableTo(target.Type()) {
		target.Set(source)
		return nil
	}
	if source.Type().ConvertibleTo(target.Type()) {
		target.Set(source.Convert(target.Type()))
		return nil
	}
	return fmt.Errorf("cannot store %T in %s", value, target.Type())
}
