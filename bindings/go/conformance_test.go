package pinkdb

// Runs the language-neutral corpus in bindings/conformance through this
// binding. The corpus is the same file the Python and Node bindings run, so a
// failure here is a binding difference, not a SQL difference.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

type corpus struct {
	CorpusVersion int          `json:"corpus_version"`
	ABIVersion    int          `json:"abi_version"`
	Cases         []corpusCase `json:"cases"`
}

type corpusCase struct {
	Name        string       `json:"name"`
	Description string       `json:"description"`
	Setup       []string     `json:"setup"`
	Steps       []corpusStep `json:"steps"`
}

type corpusStep struct {
	Op     string          `json:"op"`
	SQL    string          `json:"sql"`
	Params json.RawMessage `json:"params"`
	Class  string          `json:"class"`
	// Message is matched as a substring: the exact wording of an engine error
	// is not part of the ABI, only its class is.
	Message string          `json:"message_contains"`
	Expect  json.RawMessage `json:"expect"`
}

type queryExpect struct {
	Columns []string            `json:"columns"`
	Types   []string            `json:"types"`
	Rows    [][]json.RawMessage `json:"rows"`
}

type statementExpect struct {
	Kind     string              `json:"kind"`
	Affected *int64              `json:"affected"`
	Columns  []string            `json:"columns"`
	Types    []string            `json:"types"`
	Rows     [][]json.RawMessage `json:"rows"`
}

type streamExpect struct {
	Columns   []string            `json:"columns"`
	Types     []string            `json:"types"`
	RowCount  int                 `json:"row_count"`
	FirstRows [][]json.RawMessage `json:"first_rows"`
	LastRows  [][]json.RawMessage `json:"last_rows"`
	Checksum  *int64              `json:"checksum"`
}

// loadCorpus reads the shared corpus, skipping (rather than failing) when the
// module is used outside a pinkdb checkout.
func loadCorpus(t *testing.T) corpus {
	t.Helper()
	path := os.Getenv("PINKDB_CONFORMANCE")
	if path == "" {
		path = filepath.Join("..", "conformance", "corpus.json")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("conformance corpus not found: %v", err)
	}
	var c corpus
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&c); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return c
}

func TestConformanceCorpus(t *testing.T) {
	c := loadCorpus(t)
	if c.CorpusVersion != 1 {
		t.Fatalf("corpus_version = %d, want 1", c.CorpusVersion)
	}
	// The corpus targets a major version and the minor it was written
	// against; a newer minor of the same major only adds calls.
	wantMajor, wantMinor := ABIMajorMinor(uint32(c.ABIVersion))
	gotMajor, gotMinor := ABIMajorMinor(SupportedABI)
	if wantMajor != gotMajor || gotMinor < wantMinor {
		t.Fatalf("corpus targets ABI v%d.%d, binding speaks v%d.%d", wantMajor, wantMinor, gotMajor, gotMinor)
	}
	for _, testCase := range c.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			runCorpusCase(t, testCase)
		})
	}
}

func runCorpusCase(t *testing.T, testCase corpusCase) {
	t.Helper()
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("open in-memory database: %v", err)
	}
	defer db.Close()
	for i, sql := range testCase.Setup {
		if _, err := db.Execute(sql); err != nil {
			t.Fatalf("setup[%d] %q: %v", i, sql, err)
		}
	}
	for i, step := range testCase.Steps {
		label := fmt.Sprintf("step %d (%s: %s)", i, step.Op, step.SQL)
		switch step.Op {
		case "query":
			runQueryStep(t, label, db, step)
		case "prepared_query":
			runPreparedQueryStep(t, label, db, step)
		case "exec":
			runExecStep(t, label, db, step)
		case "exec_prepared":
			runExecPreparedStep(t, label, db, step)
		case "error":
			runErrorStep(t, label, db, step)
		case "stream":
			runStreamStep(t, label, db, step)
		default:
			t.Fatalf("%s: unknown op %q", label, step.Op)
		}
	}
}

// execute runs SQL through the statement cache path (one result), or through
// the multi-statement path when the corpus does not mark the step prepared.
func execute(t *testing.T, label string, db *DB, step corpusStep) Result {
	t.Helper()
	params, hasParams, err := decodeParams(step.Params)
	if err != nil {
		t.Fatalf("%s: decode params: %v", label, err)
	}
	if !hasParams {
		results, err := db.Execute(step.SQL)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if len(results) != 1 {
			t.Fatalf("%s: produced %d results, want 1", label, len(results))
		}
		return results[0]
	}
	stmt, err := db.Prepare(step.SQL)
	if err != nil {
		t.Fatalf("%s: prepare: %v", label, err)
	}
	defer stmt.Close()
	result, err := stmt.Execute(params...)
	if err != nil {
		t.Fatalf("%s: execute: %v", label, err)
	}
	return result
}

func runQueryStep(t *testing.T, label string, db *DB, step corpusStep) {
	t.Helper()
	var want queryExpect
	if err := json.Unmarshal(step.Expect, &want); err != nil {
		t.Fatalf("%s: decode expectation: %v", label, err)
	}
	got := execute(t, label, db, step)
	if got.Kind != "selected" {
		t.Fatalf("%s: kind = %q, want selected", label, got.Kind)
	}
	assertColumns(t, label, got, want.Columns, want.Types)
	assertRows(t, label, got.Rows, want.Rows)
}

func runPreparedQueryStep(t *testing.T, label string, db *DB, step corpusStep) {
	t.Helper()
	var want queryExpect
	if err := json.Unmarshal(step.Expect, &want); err != nil {
		t.Fatalf("%s: decode expectation: %v", label, err)
	}
	params, _, err := decodeParams(step.Params)
	if err != nil {
		t.Fatalf("%s: decode params: %v", label, err)
	}
	stmt, err := db.Prepare(step.SQL)
	if err != nil {
		t.Fatalf("%s: prepare: %v", label, err)
	}
	defer stmt.Close()
	got, err := stmt.Execute(params...)
	if err != nil {
		t.Fatalf("%s: execute: %v", label, err)
	}
	if got.Kind != "selected" {
		t.Fatalf("%s: kind = %q, want selected", label, got.Kind)
	}
	assertColumns(t, label, got, want.Columns, want.Types)
	assertRows(t, label, got.Rows, want.Rows)
}

func runExecStep(t *testing.T, label string, db *DB, step corpusStep) {
	t.Helper()
	var wants []statementExpect
	if err := json.Unmarshal(step.Expect, &wants); err != nil {
		t.Fatalf("%s: decode expectation: %v", label, err)
	}
	results, err := db.Execute(step.SQL)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if len(results) != len(wants) {
		t.Fatalf("%s: produced %d results, want %d", label, len(results), len(wants))
	}
	for i, want := range wants {
		assertStatement(t, fmt.Sprintf("%s result %d", label, i), results[i], want)
	}
}

func runExecPreparedStep(t *testing.T, label string, db *DB, step corpusStep) {
	t.Helper()
	var wants []statementExpect
	if err := json.Unmarshal(step.Expect, &wants); err != nil {
		t.Fatalf("%s: decode expectation: %v", label, err)
	}
	if len(wants) != 1 {
		t.Fatalf("%s: exec_prepared expects exactly one result, corpus has %d", label, len(wants))
	}
	params, _, err := decodeParams(step.Params)
	if err != nil {
		t.Fatalf("%s: decode params: %v", label, err)
	}
	stmt, err := db.Prepare(step.SQL)
	if err != nil {
		t.Fatalf("%s: prepare: %v", label, err)
	}
	defer stmt.Close()
	got, err := stmt.Execute(params...)
	if err != nil {
		t.Fatalf("%s: execute: %v", label, err)
	}
	assertStatement(t, label, got, wants[0])
}

func runErrorStep(t *testing.T, label string, db *DB, step corpusStep) {
	t.Helper()
	params, hasParams, err := decodeParams(step.Params)
	if err != nil {
		t.Fatalf("%s: decode params: %v", label, err)
	}
	if hasParams {
		stmt, perr := db.Prepare(step.SQL)
		if perr != nil {
			err = perr
		} else {
			defer stmt.Close()
			_, err = stmt.Execute(params...)
		}
	} else {
		_, err = db.Execute(step.SQL)
	}
	if err == nil {
		t.Fatalf("%s: succeeded, want a %q error", label, step.Class)
	}
	var pinkdbErr *Error
	if !errors.As(err, &pinkdbErr) {
		t.Fatalf("%s: error %v (%T) is not a *pinkdb.Error", label, err, err)
	}
	if pinkdbErr.Class != step.Class {
		t.Fatalf("%s: class = %q, want %q (message: %s)", label, pinkdbErr.Class, step.Class, pinkdbErr.Message)
	}
	if step.Message != "" && !strings.Contains(pinkdbErr.Message, step.Message) {
		t.Fatalf("%s: message %q does not contain %q", label, pinkdbErr.Message, step.Message)
	}
}

func runStreamStep(t *testing.T, label string, db *DB, step corpusStep) {
	t.Helper()
	var want streamExpect
	if err := json.Unmarshal(step.Expect, &want); err != nil {
		t.Fatalf("%s: decode expectation: %v", label, err)
	}
	stmt, err := db.Prepare(step.SQL)
	if err != nil {
		t.Fatalf("%s: prepare: %v", label, err)
	}
	defer stmt.Close()
	got, err := stmt.Execute()
	if err != nil {
		t.Fatalf("%s: execute: %v", label, err)
	}
	assertColumns(t, label, got, want.Columns, want.Types)
	if len(got.Rows) != want.RowCount {
		t.Fatalf("%s: row count = %d, want %d", label, len(got.Rows), want.RowCount)
	}
	assertRows(t, label+": first rows", got.Rows[:len(want.FirstRows)], want.FirstRows)
	assertRows(t, label+": last rows", got.Rows[len(got.Rows)-len(want.LastRows):], want.LastRows)
	if want.Checksum != nil {
		var sum int64
		for _, row := range got.Rows {
			value, ok := row[0].(int64)
			if !ok {
				t.Fatalf("%s: first column is %T, want int64", label, row[0])
			}
			sum += value
		}
		if sum != *want.Checksum {
			t.Fatalf("%s: checksum = %d, want %d", label, sum, *want.Checksum)
		}
	}
}

func assertStatement(t *testing.T, label string, got Result, want statementExpect) {
	t.Helper()
	if want.Kind != "" && got.Kind != want.Kind {
		t.Fatalf("%s: kind = %q, want %q", label, got.Kind, want.Kind)
	}
	if want.Affected != nil && got.Affected != *want.Affected {
		t.Fatalf("%s: affected = %d, want %d", label, got.Affected, *want.Affected)
	}
	if want.Columns != nil {
		assertColumns(t, label, got, want.Columns, want.Types)
	}
	if want.Rows != nil {
		assertRows(t, label, got.Rows, want.Rows)
	}
}

func assertColumns(t *testing.T, label string, got Result, columns, types []string) {
	t.Helper()
	if !reflect.DeepEqual(got.Columns, columns) {
		t.Fatalf("%s: columns = %v, want %v", label, got.Columns, columns)
	}
	if !reflect.DeepEqual(got.ColumnTypes, types) {
		t.Fatalf("%s: column types = %v, want %v", label, got.ColumnTypes, types)
	}
}

func assertRows(t *testing.T, label string, got [][]any, want [][]json.RawMessage) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d rows, want %d", label, len(got), len(want))
	}
	for row := range want {
		if len(got[row]) != len(want[row]) {
			t.Fatalf("%s: row %d has %d cells, want %d", label, row, len(got[row]), len(want[row]))
		}
		for col := range want[row] {
			wantCell, err := decodeCell(want[row][col])
			if err != nil {
				t.Fatalf("%s: row %d column %d: decode expectation: %v", label, row, col, err)
			}
			where := fmt.Sprintf("%s: row %d column %d", label, row, col)
			assertCell(t, where, wantCell, got[row][col])
		}
	}
}

func assertCell(t *testing.T, label string, want, got any) {
	t.Helper()
	switch want := want.(type) {
	case nil:
		if got != nil {
			t.Fatalf("%s = %v (%T), want NULL", label, got, got)
		}
	case bool:
		got, ok := got.(bool)
		if !ok || got != want {
			t.Fatalf("%s = %v (%T), want %v", label, got, got, want)
		}
	case int64:
		got, ok := got.(int64)
		if !ok || got != want {
			t.Fatalf("%s = %v (%T), want %d", label, got, got, want)
		}
	case float64:
		got, ok := got.(float64)
		if !ok {
			t.Fatalf("%s = %v (%T), want %v", label, got, got, want)
		}
		if math.IsNaN(want) {
			if !math.IsNaN(got) {
				t.Fatalf("%s = %v, want NaN", label, got)
			}
		} else if got != want {
			t.Fatalf("%s = %v, want %v", label, got, want)
		}
	case string:
		got, ok := got.(string)
		if !ok || got != want {
			t.Fatalf("%s = %q (%T), want %q", label, got, got, want)
		}
	case time.Time:
		got, ok := got.(time.Time)
		if !ok || !got.Equal(want) {
			t.Fatalf("%s = %v (%T), want %v", label, got, got, want)
		}
	default:
		t.Fatalf("%s: unsupported expectation type %T", label, want)
	}
}

// decodeParams returns the decoded parameters and whether the step had a
// "params" key at all: an absent key means "run this as a plain statement",
// while an empty array means "prepare it and bind nothing".
func decodeParams(raw json.RawMessage) ([]any, bool, error) {
	if len(raw) == 0 {
		return nil, false, nil
	}
	var cells []json.RawMessage
	if err := json.Unmarshal(raw, &cells); err != nil {
		return nil, true, err
	}
	params := make([]any, len(cells))
	for i, cell := range cells {
		value, err := decodeCell(cell)
		if err != nil {
			return nil, true, fmt.Errorf("parameter %d: %w", i+1, err)
		}
		params[i] = value
	}
	return params, true, nil
}

// decodeCell decodes one corpus value: null, or a [type, value] pair. The
// encoding keeps types that JSON cannot express (bigint, date, timestamp,
// decimal, NaN, Infinity) as canonical strings.
func decodeCell(raw json.RawMessage) (any, error) {
	if string(bytes.TrimSpace(raw)) == "null" {
		return nil, nil
	}
	var pair []json.RawMessage
	if err := json.Unmarshal(raw, &pair); err != nil || len(pair) != 2 {
		return nil, fmt.Errorf("value is not null and not a [type, value] pair: %s", raw)
	}
	var kind string
	if err := json.Unmarshal(pair[0], &kind); err != nil {
		return nil, fmt.Errorf("value type: %w", err)
	}
	switch kind {
	case "null":
		return nil, nil
	case "boolean":
		var value bool
		return value, json.Unmarshal(pair[1], &value)
	case "integer":
		var value json.Number
		if err := json.Unmarshal(pair[1], &value); err != nil {
			return nil, err
		}
		return value.Int64()
	case "bigint":
		var text string
		if err := json.Unmarshal(pair[1], &text); err != nil {
			return nil, err
		}
		return strconv.ParseInt(text, 10, 64)
	case "double":
		var text string
		if err := json.Unmarshal(pair[1], &text); err != nil {
			return nil, err
		}
		switch text {
		case "NaN":
			return math.NaN(), nil
		case "Infinity":
			return math.Inf(1), nil
		case "-Infinity":
			return math.Inf(-1), nil
		}
		return strconv.ParseFloat(text, 64)
	case "varchar", "decimal", "interval", "hugeint", "uhugeint":
		var text string
		return text, json.Unmarshal(pair[1], &text)
	case "date":
		var text string
		if err := json.Unmarshal(pair[1], &text); err != nil {
			return nil, err
		}
		return time.Parse("2006-01-02", text)
	case "timestamp":
		var text string
		if err := json.Unmarshal(pair[1], &text); err != nil {
			return nil, err
		}
		return time.Parse("2006-01-02 15:04:05.000000", text)
	default:
		return nil, fmt.Errorf("unknown value type %q", kind)
	}
}
