package pinkdb

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestEmptySQLReturnsNoResults(t *testing.T) {
	db := mustOpenMemory(t)
	defer db.Close()
	results := mustExecute(t, db, "")
	if len(results) != 0 {
		t.Fatalf("Execute(\"\") returned %d results, want 0", len(results))
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
