package pinkdb

// The package carries its own copy of the C ABI declarations in pinkdb.go so
// it builds from the Go module cache, outside a pinkdb checkout. This test
// keeps that copy honest against the real header.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestABIDeclarationsMatchHeader(t *testing.T) {
	headerPath := filepath.Join("..", "..", "crates", "pinkdb-ffi", "include", "pinkdb.h")
	header, err := os.ReadFile(headerPath)
	if err != nil {
		t.Skipf("pinkdb C header not in this checkout: %v", err)
	}
	source, err := os.ReadFile("pinkdb.go")
	if err != nil {
		t.Fatalf("read pinkdb.go: %v", err)
	}

	function := regexp.MustCompile(`\bpinkdb_[a-z_]+\(`)
	names := function.FindAllString(string(source), -1)
	if len(names) == 0 {
		t.Fatal("found no pinkdb_* functions in pinkdb.go")
	}
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		name = strings.TrimSuffix(name, "(")
		if seen[name] {
			continue
		}
		seen[name] = true
		if !strings.Contains(string(header), name+"(") {
			t.Errorf("%s is used by this package but not declared in pinkdb.h", name)
		}
	}

	// pink_value is passed by value, so its layout is part of the ABI.
	for _, field := range []string{"type_tag", "is_null", "int64", "double_", "text_ptr", "text_len"} {
		if !strings.Contains(string(header), field+";") {
			t.Errorf("pink_value field %s is not declared in pinkdb.h", field)
		}
	}
}
