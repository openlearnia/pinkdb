package pinkdb

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// ----- helpers -----

func mustOpenMemory(t *testing.T) *DB {
	t.Helper()
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	return db
}

func mustExecute(t *testing.T, db *DB, sql string) []Result {
	t.Helper()
	results, err := db.Execute(sql)
	if err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
	return results
}

func checkStrings(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %q, want %q", what, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s[%d] = %q, want %q (full: %q)", what, i, got[i], want[i], got)
		}
	}
}

func boolAt(t *testing.T, row []any, i int) bool {
	t.Helper()
	v, ok := row[i].(bool)
	if !ok {
		t.Fatalf("row[%d] = %v (%T), want bool", i, row[i], row[i])
	}
	return v
}

func intAt(t *testing.T, row []any, i int) int64 {
	t.Helper()
	v, ok := row[i].(int64)
	if !ok {
		t.Fatalf("row[%d] = %v (%T), want int64", i, row[i], row[i])
	}
	return v
}

func floatAt(t *testing.T, row []any, i int) float64 {
	t.Helper()
	v, ok := row[i].(float64)
	if !ok {
		t.Fatalf("row[%d] = %v (%T), want float64", i, row[i], row[i])
	}
	return v
}

func stringAt(t *testing.T, row []any, i int) string {
	t.Helper()
	v, ok := row[i].(string)
	if !ok {
		t.Fatalf("row[%d] = %v (%T), want string", i, row[i], row[i])
	}
	return v
}

func timeAt(t *testing.T, row []any, i int) time.Time {
	t.Helper()
	v, ok := row[i].(time.Time)
	if !ok {
		t.Fatalf("row[%d] = %v (%T), want time.Time", i, row[i], row[i])
	}
	if loc := v.Location(); loc != time.UTC {
		t.Fatalf("row[%d] location = %v, want UTC", i, loc)
	}
	return v
}

func nilAt(t *testing.T, row []any, i int) {
	t.Helper()
	if row[i] != nil {
		t.Fatalf("row[%d] = %v (%T), want nil", i, row[i], row[i])
	}
}

// ----- tests -----

func TestABIVersion(t *testing.T) {
	if SupportedABI != 1 {
		t.Fatalf("SupportedABI = %d, want 1", SupportedABI)
	}
	db := mustOpenMemory(t)
	defer db.Close()
	if got := db.ABI(); got != 1 {
		t.Fatalf("ABI() = %d, want 1", got)
	}
}

func TestMemoryAllTypesRoundTrip(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()

	results := mustExecute(t, db, `
		CREATE TABLE types (b BOOLEAN, i INTEGER, bi BIGINT, d DOUBLE, s VARCHAR, dt DATE, ts TIMESTAMP);
		INSERT INTO types VALUES
			(TRUE, -2147483647 - 1, 9223372036854775807, -0.0, 'héllo 🌍', DATE '2024-01-15', TIMESTAMP '2024-01-15 10:00:00.123456'),
			(NULL, NULL, NULL, NULL, NULL, NULL, NULL),
			(FALSE, 7, 8, 2.5, '', DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00');
		SELECT * FROM types ORDER BY i;
	`)
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	kinds := make([]string, len(results))
	for i, r := range results {
		kinds[i] = r.Kind
	}
	checkStrings(t, "kinds", kinds, []string{"created", "inserted", "selected"})
	if got := results[1].Affected; got != 3 {
		t.Fatalf("inserted affected = %d, want 3", got)
	}

	selected := results[2]
	checkStrings(t, "columns", selected.Columns, []string{"b", "i", "bi", "d", "s", "dt", "ts"})
	checkStrings(t, "column types", selected.ColumnTypes,
		[]string{"boolean", "integer", "bigint", "double", "varchar", "date", "timestamp"})
	if got := selected.Name; got != "" {
		t.Fatalf("selected name = %q, want empty", got)
	}
	if got := selected.Affected; got != 0 {
		t.Fatalf("selected affected = %d, want 0", got)
	}
	if len(selected.Rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(selected.Rows))
	}

	// Extreme/non-finite row, NULLs sorted last.
	row := selected.Rows[0]
	if got := boolAt(t, row, 0); !got {
		t.Fatalf("row[0].b = %v, want true", got)
	}
	if got := intAt(t, row, 1); got != math.MinInt32 {
		t.Fatalf("row[0].i = %d, want %d", got, math.MinInt32)
	}
	if got := intAt(t, row, 2); got != math.MaxInt64 {
		t.Fatalf("row[0].bi = %d, want %d", got, math.MaxInt64)
	}
	if got := floatAt(t, row, 3); got != 0 || !math.Signbit(got) {
		t.Fatalf("row[0].d = %v (signbit %v), want -0.0", got, math.Signbit(got))
	}
	if got := stringAt(t, row, 4); got != "héllo 🌍" {
		t.Fatalf("row[0].s = %q, want %q", got, "héllo 🌍")
	}
	wantDate := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	if got := timeAt(t, row, 5); !got.Equal(wantDate) {
		t.Fatalf("row[0].dt = %v, want %v", got, wantDate)
	}
	wantTimestamp := time.Date(2024, 1, 15, 10, 0, 0, 123456000, time.UTC)
	if got := timeAt(t, row, 6); !got.Equal(wantTimestamp) {
		t.Fatalf("row[0].ts = %v, want %v", got, wantTimestamp)
	}

	// FALSE / empty string / epoch boundaries.
	row = selected.Rows[1]
	if got := boolAt(t, row, 0); got {
		t.Fatalf("row[1].b = %v, want false", got)
	}
	if got := intAt(t, row, 1); got != 7 {
		t.Fatalf("row[1].i = %d, want 7", got)
	}
	if got := stringAt(t, row, 4); got != "" {
		t.Fatalf("row[1].s = %q, want empty", got)
	}
	if got := timeAt(t, row, 5); !got.Equal(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("row[1].dt = %v, want 1970-01-01", got)
	}
	if got := timeAt(t, row, 6); !got.Equal(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("row[1].ts = %v, want epoch", got)
	}

	// NULL row: every column maps to nil whatever its declared type.
	for i := range selected.Rows[2] {
		nilAt(t, selected.Rows[2], i)
	}
}

func TestNumericExtremesViaExpressions(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()

	results := mustExecute(t, db,
		"SELECT -2147483647 - 1 AS i32min, 2147483647 AS i32max, "+
			"-9223372036854775807 - 1 AS i64min, 9223372036854775807 AS i64max;")
	selected := results[0]
	checkStrings(t, "column types", selected.ColumnTypes,
		[]string{"integer", "integer", "bigint", "bigint"})
	row := selected.Rows[0]
	if got := intAt(t, row, 0); got != math.MinInt32 {
		t.Fatalf("i32min = %d, want %d", got, math.MinInt32)
	}
	if got := intAt(t, row, 1); got != math.MaxInt32 {
		t.Fatalf("i32max = %d, want %d", got, math.MaxInt32)
	}
	if got := intAt(t, row, 2); got != math.MinInt64 {
		t.Fatalf("i64min = %d, want %d", got, math.MinInt64)
	}
	if got := intAt(t, row, 3); got != math.MaxInt64 {
		t.Fatalf("i64max = %d, want %d", got, math.MaxInt64)
	}
}

func TestVarcharEdgeCases(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()

	// The embedded NUL lives in the Go SQL string; the ABI passes (pointer,
	// length), so the engine parses it and text_len brings it back.
	results := mustExecute(t, db,
		"SELECT '' AS empty, 'héllo' AS multibyte, 'a\x00b' AS embedded;")
	selected := results[0]
	checkStrings(t, "columns", selected.Columns, []string{"empty", "multibyte", "embedded"})
	checkStrings(t, "column types", selected.ColumnTypes,
		[]string{"varchar", "varchar", "varchar"})
	row := selected.Rows[0]
	if got := stringAt(t, row, 0); got != "" {
		t.Fatalf("empty = %q, want empty", got)
	}
	if got := stringAt(t, row, 1); got != "héllo" {
		t.Fatalf("multibyte = %q, want %q", got, "héllo")
	}
	if got := stringAt(t, row, 2); got != "a\x00b" {
		t.Fatalf("embedded = %q, want %q", got, "a\x00b")
	}
}

func TestPreEpochAndEpochBoundaries(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()

	results := mustExecute(t, db,
		"SELECT DATE '1940-01-01', DATE '1969-12-31', DATE '1970-01-01', "+
			"TIMESTAMP '1969-12-31 23:59:59.999999', TIMESTAMP '1970-01-01 00:00:00';")
	selected := results[0]
	checkStrings(t, "column types", selected.ColumnTypes,
		[]string{"date", "date", "date", "timestamp", "timestamp"})
	row := selected.Rows[0]
	want := []time.Time{
		time.Date(1940, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC),
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1969, 12, 31, 23, 59, 59, 999999000, time.UTC),
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	for i, w := range want {
		if got := timeAt(t, row, i); !got.Equal(w) {
			t.Fatalf("row[%d] = %v, want %v", i, got, w)
		}
	}

	// Year 0 is out of Python's datetime range but representable in Go's
	// time.Time, so the binding returns it instead of erroring.
	results = mustExecute(t, db, "SELECT DATE '0000-01-01', TIMESTAMP '0000-01-01 00:00:00';")
	row = results[0].Rows[0]
	date := timeAt(t, row, 0)
	if date.Year() != 0 || date.Month() != time.January || date.Day() != 1 {
		t.Fatalf("year-0 date = %v, want year 0 January 1", date)
	}
	timestamp := timeAt(t, row, 1)
	if timestamp.Year() != 0 || timestamp.Month() != time.January || timestamp.Day() != 1 {
		t.Fatalf("year-0 timestamp = %v, want year 0 January 1", timestamp)
	}
}

func TestDoubleSpecialValues(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()

	results := mustExecute(t, db,
		"SELECT 1.0 / 0.0 AS pos, -1.0 / 0.0 AS neg, 0.0 / 0.0 AS nan, -0.0 AS negzero;")
	row := results[0].Rows[0]
	pos := floatAt(t, row, 0)
	if !math.IsInf(pos, 1) {
		t.Fatalf("pos = %v, want +Inf", pos)
	}
	neg := floatAt(t, row, 1)
	if !math.IsInf(neg, -1) {
		t.Fatalf("neg = %v, want -Inf", neg)
	}
	if nan := floatAt(t, row, 2); !math.IsNaN(nan) {
		t.Fatalf("nan = %v, want NaN", nan)
	}
	negzero := floatAt(t, row, 3)
	if negzero != 0 || !math.Signbit(negzero) {
		t.Fatalf("negzero = %v (signbit %v), want -0.0", negzero, math.Signbit(negzero))
	}
}

// TestDateAndTimestampRangeMapping pins the conversion range: values that fit
// Go's time.Time convert exactly; DATE day counts that would overflow its
// int64 seconds representation wrap ErrOutOfRange instead of silently
// producing a wrong calendar value.
func TestDateAndTimestampRangeMapping(t *testing.T) {
	for _, days := range []int64{0, 1, -1, maxDateDays, minDateDays} {
		got, err := dateFromDays(days)
		if err != nil {
			t.Fatalf("dateFromDays(%d): %v", days, err)
		}
		if got.Unix() != days*secondsPerDay {
			t.Fatalf("dateFromDays(%d) = %v, want unix %d", days, got, days*secondsPerDay)
		}
	}
	for _, days := range []int64{maxDateDays + 1, math.MaxInt64, minDateDays - 1, math.MinInt64} {
		got, err := dateFromDays(days)
		if !errors.Is(err, ErrOutOfRange) {
			t.Fatalf("dateFromDays(%d) = (%v, %v), want ErrOutOfRange", days, got, err)
		}
	}

	// Every int64 microsecond count fits time.Time, pre-epoch included.
	for _, us := range []int64{
		0, 1, -1, 999_999, -999_999,
		-62167219200000000, // 0000-01-01 00:00:00
		math.MaxInt64, math.MinInt64,
	} {
		got, err := timestampFromMicros(us)
		if err != nil {
			t.Fatalf("timestampFromMicros(%d): %v", us, err)
		}
		if got.UnixMicro() != us {
			t.Fatalf("timestampFromMicros(%d) = %v (unix micro %d)", us, got, got.UnixMicro())
		}
	}
}

func TestDecimalIntervalAndAlteredTable(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()
	res, err := db.Execute("SELECT 1.5::DECIMAL(4,2) AS d, INTERVAL '1 day' AS iv;")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := res[0].ColumnTypes; len(got) != 2 || got[0] != "decimal" || got[1] != "interval" {
		t.Fatalf("column types: %v", got)
	}
	if got := res[0].Rows[0][0]; got != "1.50" {
		t.Fatalf("decimal cell: %v", got)
	}
	if got, ok := res[0].Rows[0][1].(string); !ok || got == "" {
		t.Fatalf("interval cell: %v", res[0].Rows[0][1])
	}
	if _, err := db.Execute("CREATE TABLE t (x INTEGER);"); err != nil {
		t.Fatalf("create: %v", err)
	}
	res, err = db.Execute("ALTER TABLE t ADD COLUMN y INTEGER DEFAULT 0;")
	if err != nil {
		t.Fatalf("alter: %v", err)
	}
	if res[0].Kind != "altered_table" {
		t.Fatalf("kind: %q", res[0].Kind)
	}
}

func TestErrorKeepsHandleUsable(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()

	_, err := db.Execute("SELECT * FROM missing;")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Execute on missing table = %v, want a 'not found' error", err)
	}
	results := mustExecute(t, db, "SELECT 42;")
	if got := intAt(t, results[0].Rows[0], 0); got != 42 {
		t.Fatalf("after error: got %d, want 42", got)
	}

	if _, err := db.Execute("THIS IS NOT SQL;"); err == nil {
		t.Fatal("Execute on invalid SQL = nil error")
	}
	results = mustExecute(t, db, "SELECT 7;")
	if got := intAt(t, results[0].Rows[0], 0); got != 7 {
		t.Fatalf("after second error: got %d, want 7", got)
	}
}

func TestEmptyAndCommentOnlySQLReturnsNoResults(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()
	for _, sql := range []string{
		"",
		";",
		";;",
		"  \t\n ",
		"-- comment only\n",
		"/* block comment */",
		"-- one\n;\n/* two */",
	} {
		results, err := db.Execute(sql)
		if err != nil {
			t.Fatalf("Execute(%q) error = %v, want nil", sql, err)
		}
		if len(results) != 0 {
			t.Fatalf("Execute(%q) returned %d results, want 0", sql, len(results))
		}
	}
	// The handle stays fully usable afterwards.
	results := mustExecute(t, db, "SELECT 1;")
	if got := intAt(t, results[0].Rows[0], 0); got != 1 {
		t.Fatalf("after empty inputs: SELECT 1 = %d, want 1", got)
	}
}

func TestMultiStatementKindsAndMetadata(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()

	results := mustExecute(t, db,
		"CREATE TABLE t (a INTEGER);"+
			"INSERT INTO t VALUES (1), (2);"+
			"UPDATE t SET a = a + 10;"+
			"DELETE FROM t WHERE a = 11;"+
			"CREATE VIEW v AS SELECT a FROM t;"+
			"DROP VIEW v;"+
			"DROP TABLE t;"+
			"CREATE DATABASE db1;"+
			"USE db1;"+
			"BEGIN;"+
			"COMMIT;"+
			"BEGIN;"+
			"ROLLBACK;"+
			"USE main;"+
			"DROP DATABASE db1;"+
			"SELECT 1;")

	kinds := make([]string, len(results))
	for i, r := range results {
		kinds[i] = r.Kind
	}
	checkStrings(t, "kinds", kinds, []string{
		"created", "inserted", "updated", "deleted",
		"created_view", "dropped_view", "dropped_table",
		"created_database", "used",
		"began", "committed", "began", "rolledback",
		"used", "dropped_database", "selected",
	})

	wantAffected := map[int]int64{1: 2, 2: 2, 3: 1}
	for i, want := range wantAffected {
		if got := results[i].Affected; got != want {
			t.Fatalf("results[%d].Affected = %d, want %d", i, got, want)
		}
	}
	wantNames := map[int]string{4: "v", 5: "v", 6: "t", 7: "db1", 8: "db1", 13: "main", 14: "db1"}
	for i, want := range wantNames {
		if got := results[i].Name; got != want {
			t.Fatalf("results[%d].Name = %q, want %q", i, got, want)
		}
	}

	selected := results[len(results)-1]
	for i, r := range results[:len(results)-1] {
		if len(r.Columns) != 0 || len(r.ColumnTypes) != 0 || len(r.Rows) != 0 {
			t.Fatalf("%s result exposes column surface: %+v", r.Kind, r)
		}
		want, named := wantNames[i]
		if !named {
			want = ""
		}
		if r.Name != want {
			t.Fatalf("results[%d].Name = %q, want %q", i, r.Name, want)
		}
	}
	checkStrings(t, "selected columns", selected.Columns, []string{"1"})
	checkStrings(t, "selected column types", selected.ColumnTypes, []string{"integer"})
	if got := intAt(t, selected.Rows[0], 0); got != 1 {
		t.Fatalf("SELECT 1 = %d, want 1", got)
	}
	if got := selected.Affected; got != 0 {
		t.Fatalf("selected affected = %d, want 0", got)
	}
	if got := selected.Name; got != "" {
		t.Fatalf("selected name = %q, want empty", got)
	}
}

func TestOpenPathValidation(t *testing.T) {
	if _, err := Open(""); err == nil || !strings.Contains(err.Error(), "must not be empty") {
		t.Fatalf("Open(\"\") = %v, want an empty-path error", err)
	}

	dir := t.TempDir()
	_, err := Open(filepath.Join(dir, "a\x00b.pink"))
	if err == nil || !strings.Contains(err.Error(), "NUL") {
		t.Fatalf("Open(path with NUL) = %v, want a NUL-byte error", err)
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("rejected path created %d entries on disk", len(entries))
	}

	_, err = Open(filepath.Join(dir, "no_such_dir", "x.pink"))
	if err == nil {
		t.Fatal("Open into a missing directory = nil error")
	}
	if err.Error() == "" {
		t.Fatal("native open error message is empty")
	}
}

func TestDurableRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "roundtrip.pink")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.Execute(
		"CREATE TABLE t (i INTEGER, s VARCHAR);" +
			"INSERT INTO t VALUES (-2147483647 - 1, 'héllo'), (NULL, NULL);"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	db.Close()
	db.Close() // idempotent

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	results := mustExecute(t, reopened, "SELECT i, s FROM t ORDER BY i;")
	if got := results[0].Kind; got != "selected" {
		t.Fatalf("kind = %q, want selected", got)
	}
	if len(results[0].Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(results[0].Rows))
	}
	row := results[0].Rows[0]
	if got := intAt(t, row, 0); got != math.MinInt32 {
		t.Fatalf("i = %d, want %d", got, math.MinInt32)
	}
	if got := stringAt(t, row, 1); got != "héllo" {
		t.Fatalf("s = %q, want %q", got, "héllo")
	}
	nilAt(t, results[0].Rows[1], 0)
	nilAt(t, results[0].Rows[1], 1)
}

func TestCloseIdempotentAndUseAfterClose(t *testing.T) {
	db := mustOpenMemory(t)
	if _, err := db.Execute("SELECT 1;"); err != nil {
		t.Fatalf("before close: %v", err)
	}
	db.Close()
	db.Close() // idempotent

	if _, err := db.Execute("SELECT 1;"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Execute after close = %v, want ErrClosed", err)
	}
	// ABI() is library-wide and stays callable after Close.
	if got := db.ABI(); got != 1 {
		t.Fatalf("ABI() after close = %d, want 1", got)
	}
}

func TestConcurrentExecute(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()
	if _, err := db.Execute("CREATE TABLE t (x INTEGER);"); err != nil {
		t.Fatalf("setup: %v", err)
	}

	const goroutines = 8
	const perGoroutine = 25
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				sql := fmt.Sprintf("INSERT INTO t VALUES (%d);", g*perGoroutine+i)
				if _, err := db.Execute(sql); err != nil {
					errs <- fmt.Errorf("goroutine %d: %w", g, err)
					return
				}
				results, err := db.Execute("SELECT COUNT(*) FROM t;")
				if err != nil {
					errs <- fmt.Errorf("goroutine %d: %w", g, err)
					return
				}
				if _, ok := results[0].Rows[0][0].(int64); !ok {
					errs <- fmt.Errorf("goroutine %d: COUNT(*) = %T, want int64", g, results[0].Rows[0][0])
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	results := mustExecute(t, db, "SELECT COUNT(*) FROM t;")
	if got := intAt(t, results[0].Rows[0], 0); got != goroutines*perGoroutine {
		t.Fatalf("final COUNT(*) = %d, want %d", got, goroutines*perGoroutine)
	}
}

func TestThousandIterationExecuteLoop(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()

	for i := 0; i < 1000; i++ {
		results, err := db.Execute(fmt.Sprintf("SELECT %d AS n, 'v%d' AS s;", i, i))
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if len(results) != 1 {
			t.Fatalf("iteration %d: got %d results, want 1", i, len(results))
		}
		row := results[0].Rows[0]
		if got := intAt(t, row, 0); got != int64(i) {
			t.Fatalf("iteration %d: n = %d", i, got)
		}
		if got := stringAt(t, row, 1); got != fmt.Sprintf("v%d", i) {
			t.Fatalf("iteration %d: s = %q", i, got)
		}
	}
}

// ----- review-fix coverage: races, exact errors, scale, leak pins -----

// TestCloseRacingExecute hammers one DB with concurrent Execute calls while
// many goroutines call Close. Every Execute either succeeds or returns
// ErrClosed (never another error, never a crash), every executor eventually
// observes the close, and a post-close Execute fails fast. Under -race this
// pins the mutex/closed-flag protocol that makes use-after-close safe.
func TestCloseRacingExecute(t *testing.T) {
	const (
		rounds    = 4
		executors = 8
		closers   = 16
	)

	for round := 0; round < rounds; round++ {
		db := mustOpenMemory(t)
		if _, err := db.Execute("CREATE TABLE t (x INTEGER);"); err != nil {
			t.Fatalf("round %d: setup: %v", round, err)
		}

		var wg sync.WaitGroup
		problems := make(chan error, executors)
		var sawClosed, executes atomic.Int64

		for g := 0; g < executors; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				deadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(deadline) {
					executes.Add(1)
					_, err := db.Execute("SELECT x FROM t;")
					if err == nil {
						continue
					}
					if errors.Is(err, ErrClosed) {
						sawClosed.Add(1)
						return
					}
					problems <- fmt.Errorf("round %d: Execute: %w", round, err)
					return
				}
				problems <- fmt.Errorf("round %d: executor never observed ErrClosed", round)
			}()
		}

		// Give the executors a moment to overlap with the closes.
		time.Sleep(time.Millisecond)

		var closerWG sync.WaitGroup
		for c := 0; c < closers; c++ {
			closerWG.Add(1)
			go func() {
				defer closerWG.Done()
				db.Close()
				db.Close() // no-op: the handle is already freed
			}()
		}
		closerWG.Wait()
		wg.Wait()

		close(problems)
		for err := range problems {
			t.Fatal(err)
		}
		if got := sawClosed.Load(); got != executors {
			t.Fatalf("round %d: %d/%d executors observed ErrClosed", round, got, executors)
		}
		if got := executes.Load(); got < executors {
			t.Fatalf("round %d: only %d Execute calls ran", round, got)
		}
		if _, err := db.Execute("SELECT 1;"); !errors.Is(err, ErrClosed) {
			t.Fatalf("round %d: post-close Execute = %v, want ErrClosed", round, err)
		}
		if got := db.ABI(); got != SupportedABI {
			t.Fatalf("round %d: ABI() after close = %d, want %d", round, got, SupportedABI)
		}
	}
}

// TestNativeErrorMessagesExact asserts the exact, verbatim native messages
// (the pinkdb_last_error string) for representative failing statements —
// full-string equality, not substring checks.
func TestNativeErrorMessagesExact(t *testing.T) {
	cases := []struct {
		name  string
		setup string
		sql   string
		want  string
	}{
		{"missing table", "", "SELECT * FROM nope;", `table "nope" not found`},
		{"drop missing table", "", "DROP TABLE nope;", `table "nope" not found`},
		{"missing database", "", "USE nope;", `database "nope" not found`},
		{"duplicate table", "CREATE TABLE dup (a INTEGER);", "CREATE TABLE dup (a INTEGER);", `table "dup" already exists`},
		{"missing column", "CREATE TABLE t (a INTEGER);", "SELECT nosuchcol FROM t;", `no column "nosuchcol"`},
		{"nested transaction", "", "BEGIN; BEGIN;", "already in a transaction"},
		{"integer division by zero", "", "SELECT 1 / 0;", "integer division by zero"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := mustOpenMemory(t)
			defer db.Close()
			if tc.setup != "" {
				if _, err := db.Execute(tc.setup); err != nil {
					t.Fatalf("setup %q: %v", tc.setup, err)
				}
			}
			_, err := db.Execute(tc.sql)
			if err == nil {
				t.Fatalf("Execute(%q) = nil error, want %q", tc.sql, tc.want)
			}
			if err.Error() != tc.want {
				t.Fatalf("Execute(%q) error = %q, want exactly %q", tc.sql, err, tc.want)
			}
			// A failed statement leaves the handle usable.
			if _, err := db.Execute("SELECT 1;"); err != nil {
				t.Fatalf("handle unusable after %q: %v", tc.sql, err)
			}
		})
	}
}

// TestNonUTF8Inputs pins the native rejection of invalid UTF-8 in SQL and in
// paths: the native message is returned verbatim and the rejected path never
// reaches the filesystem.
func TestNonUTF8Inputs(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()

	for _, sql := range []string{
		"\xff",
		"SELECT 1;\xc3\x28", // truncated 2-byte sequence
		"SELECT '\xfe' AS bad;",
	} {
		_, err := db.Execute(sql)
		if err == nil {
			t.Fatalf("Execute(%q) = nil error, want the UTF-8 error", sql)
		}
		if err.Error() != "SQL is not valid UTF-8" {
			t.Fatalf("Execute(%q) error = %q, want exactly %q", sql, err, "SQL is not valid UTF-8")
		}
	}
	// Rejected input leaves the handle usable.
	results := mustExecute(t, db, "SELECT 7;")
	if got := intAt(t, results[0].Rows[0], 0); got != 7 {
		t.Fatalf("after invalid UTF-8: SELECT 7 = %d, want 7", got)
	}

	dir := t.TempDir()
	_, err := Open(filepath.Join(dir, "bad\xffname.pink"))
	if err == nil {
		t.Fatal("Open(path with invalid UTF-8) = nil error")
	}
	if err.Error() != "path is not valid UTF-8" {
		t.Fatalf("Open(path with invalid UTF-8) error = %q, want exactly %q", err, "path is not valid UTF-8")
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("rejected non-UTF-8 path created %d entries on disk", len(entries))
	}
}

// TestLargeSelectMaterialization materializes a 20k-row SELECT into Go
// values and spot-checks rows at both ends and the middle.
func TestLargeSelectMaterialization(t *testing.T) {
	const rows = 20000
	db := mustOpenMemory(t)
	defer db.Close()

	var b strings.Builder
	b.WriteString("CREATE TABLE big (i INTEGER, s VARCHAR);INSERT INTO big VALUES ")
	for i := 0; i < rows; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "(%d,'row-%d')", i, i)
	}
	b.WriteString(";SELECT i, s FROM big ORDER BY i;")

	results := mustExecute(t, db, b.String())
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	selected := results[2]
	if selected.Kind != "selected" {
		t.Fatalf("kind = %q, want selected", selected.Kind)
	}
	if len(selected.Rows) != rows {
		t.Fatalf("got %d rows, want %d", len(selected.Rows), rows)
	}
	for _, i := range []int{0, 1, rows / 2, rows - 2, rows - 1} {
		if got := intAt(t, selected.Rows[i], 0); got != int64(i) {
			t.Fatalf("row %d: i = %d, want %d", i, got, i)
		}
		if got := stringAt(t, selected.Rows[i], 1); got != fmt.Sprintf("row-%d", i) {
			t.Fatalf("row %d: s = %q, want %q", i, got, fmt.Sprintf("row-%d", i))
		}
	}
}

// TestLargeMultiStatementExecute runs one Execute with 20k statements and
// spot-checks the materialized results.
func TestLargeMultiStatementExecute(t *testing.T) {
	const statements = 20000
	db := mustOpenMemory(t)
	defer db.Close()

	var b strings.Builder
	for i := 0; i < statements; i++ {
		fmt.Fprintf(&b, "SELECT %d AS n;", i)
	}
	results := mustExecute(t, db, b.String())
	if len(results) != statements {
		t.Fatalf("got %d results, want %d", len(results), statements)
	}
	for _, i := range []int{0, statements / 2, statements - 1} {
		r := results[i]
		if r.Kind != "selected" {
			t.Fatalf("results[%d].Kind = %q, want selected", i, r.Kind)
		}
		if got := intAt(t, r.Rows[0], 0); got != int64(i) {
			t.Fatalf("results[%d] n = %d, want %d", i, got, i)
		}
	}
}

// TestMultiMegabyteVarcharRoundTrip round-trips a multi-MB VARCHAR value
// containing multibyte text and embedded NULs, compared by SHA-256 so a
// mismatch does not dump megabytes into the test log.
func TestMultiMegabyteVarcharRoundTrip(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()

	unit := "pinkdb 🚀 多字节 multi-byte "
	payload := "head\x00" + strings.Repeat(unit, 3<<20/len(unit)+1) + "\x00tail"
	if len(payload) < 3<<20 {
		t.Fatalf("payload is only %d bytes", len(payload))
	}
	if strings.ContainsRune(payload, '\'') {
		t.Fatal("payload contains a quote")
	}

	results := mustExecute(t, db, "SELECT '"+payload+"' AS big;")
	selected := results[0]
	checkStrings(t, "column types", selected.ColumnTypes, []string{"varchar"})
	got := stringAt(t, selected.Rows[0], 0)
	if len(got) != len(payload) {
		t.Fatalf("round-tripped %d bytes, want %d", len(got), len(payload))
	}
	if sha256.Sum256([]byte(got)) != sha256.Sum256([]byte(payload)) {
		t.Fatalf("round-tripped payload differs (both %d bytes)", len(got))
	}
}

// maxRSSBytes returns the process's peak resident set size (rusage
// RUSAGE_SELF): bytes on darwin, kilobytes on linux.
func maxRSSBytes(t *testing.T) uint64 {
	t.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		t.Fatalf("Getrusage: %v", err)
	}
	if runtime.GOOS == "linux" {
		return uint64(ru.Maxrss) * 1024
	}
	return uint64(ru.Maxrss)
}

// TestBoundedRSSAcrossSuccessAndErrorLoops pins that alternating successful
// and failing Executes do not leak: peak RSS and Go heap growth over 20k
// iterations stay under deliberately generous bounds (allocator and
// race-build noise cannot flake them), while a missing pinkdb_result_free or
// a leaked handle would blow past them.
func TestBoundedRSSAcrossSuccessAndErrorLoops(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()
	if _, err := db.Execute(
		"CREATE TABLE t (a INTEGER, s VARCHAR);" +
			"INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three');"); err != nil {
		t.Fatalf("setup: %v", err)
	}

	run := func(iterations int) {
		for i := 0; i < iterations; i++ {
			results, err := db.Execute("SELECT a, s FROM t ORDER BY a;")
			if err != nil {
				t.Fatalf("iteration %d: success path: %v", i, err)
			}
			if len(results) != 1 || len(results[0].Rows) != 3 {
				t.Fatalf("iteration %d: got %d results / %d rows", i, len(results), len(results[0].Rows))
			}
			if _, err := db.Execute("SELECT * FROM missing;"); err == nil {
				t.Fatalf("iteration %d: error path succeeded", i)
			}
		}
	}

	run(1000) // warm every allocator path before measuring
	runtime.GC()
	rssBefore := maxRSSBytes(t)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	heapBefore := ms.HeapAlloc

	const iterations = 20000
	run(iterations)

	runtime.GC()
	rssAfter := maxRSSBytes(t)
	runtime.ReadMemStats(&ms)

	const rssLimit = 128 << 20 // generous: observed growth is well under 1 MB
	const heapLimit = 32 << 20
	if growth := int64(rssAfter) - int64(rssBefore); growth > rssLimit {
		t.Fatalf("peak RSS grew %d bytes over %d success+error iterations (limit %d)", growth, iterations, rssLimit)
	}
	if growth := int64(ms.HeapAlloc) - int64(heapBefore); growth > heapLimit {
		t.Fatalf("Go heap grew %d bytes over %d success+error iterations (limit %d)", growth, iterations, heapLimit)
	}
}

// TestGoroutineStabilityAcrossExecutes runs 8k executes from 16 goroutines
// and pins that the goroutine count returns to its baseline: the per-call
// LockOSThread/UnlockOSThread pair and the cgo result materialization must
// not leak goroutines or scheduling state.
func TestGoroutineStabilityAcrossExecutes(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()
	if _, err := db.Execute("CREATE TABLE t (x INTEGER);"); err != nil {
		t.Fatalf("setup: %v", err)
	}

	base := runtime.NumGoroutine()
	const (
		workers   = 16
		perWorker = 500
	)
	var wg sync.WaitGroup
	problems := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if _, err := db.Execute("SELECT x FROM t;"); err != nil {
					problems <- fmt.Errorf("worker %d iteration %d: %w", w, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(problems)
	for err := range problems {
		t.Fatal(err)
	}

	// Give runtime-internal goroutines a moment to exit.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > base {
		t.Fatalf("goroutines after %d executes: got %d, base %d", workers*perWorker, got, base)
	}
	// The DB is still healthy after the hammering (an unbalanced
	// LockOSThread/UnlockOSThread pair would have panicked or wedged long
	// before here).
	results := mustExecute(t, db, "SELECT COUNT(*) FROM t;")
	if got := intAt(t, results[0].Rows[0], 0); got != 0 {
		t.Fatalf("COUNT(*) = %d, want 0", got)
	}
}

// TestThreadLocalErrorIsolation runs one in-memory DB per goroutine, each
// repeatedly failing with a statement unique to that goroutine. Every
// goroutine must see exactly its own native message: a thread-local error
// mix-up (or a cross-thread pinkdb_last_error read) would surface another
// goroutine's message.
func TestThreadLocalErrorIsolation(t *testing.T) {
	const (
		goroutines = 8
		iterations = 200
	)
	var wg sync.WaitGroup
	problems := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			db, err := OpenMemory()
			if err != nil {
				problems <- fmt.Errorf("goroutine %d: OpenMemory: %w", g, err)
				return
			}
			defer db.Close()
			missing := fmt.Sprintf("missing_%d", g)
			want := fmt.Sprintf("table %q not found", missing)
			sql := "SELECT * FROM " + missing + ";"
			for i := 0; i < iterations; i++ {
				_, err := db.Execute(sql)
				if err == nil {
					problems <- fmt.Errorf("goroutine %d iteration %d: nil error, want %q", g, i, want)
					return
				}
				if err.Error() != want {
					problems <- fmt.Errorf("goroutine %d iteration %d: error = %q, want exactly %q", g, i, err, want)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(problems)
	for err := range problems {
		t.Fatal(err)
	}
}
