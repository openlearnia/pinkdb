package pinkdb

import (
	"errors"
	"testing"
)

func TestQueryRowsScanAndBusy(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT i, i * 2 FROM range(0, 5000) r(i)")
	if err != nil {
		t.Fatal(err)
	}
	if got := rows.Columns(); len(got) != 2 || got[0] != "i" {
		t.Fatalf("columns = %v", got)
	}
	if _, err := db.Execute("SELECT 1"); !errors.Is(err, ErrBusy) {
		t.Fatalf("Execute with open Rows = %v, want ErrBusy", err)
	}

	count := 0
	for rows.Next() {
		var i, twice int64
		if err := rows.Scan(&i, &twice); err != nil {
			t.Fatal(err)
		}
		if twice != i*2 {
			t.Fatalf("row %d: %d * 2 = %d", count, i, twice)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 5000 {
		t.Fatalf("rows = %d, want 5000", count)
	}
	rows.Close()
	if _, err := db.Execute("SELECT 1"); err != nil {
		t.Fatalf("Execute after Rows closed: %v", err)
	}
}

func TestQueryRowsEarlyCloseAndPrepared(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT i FROM range(0, 100000) r(i)")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatalf("first Next failed: %v", rows.Err())
	}
	var i int64
	if err := rows.Scan(&i); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare("SELECT ? + 1")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	rows, err = stmt.Query(int64(41))
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatalf("prepared Next failed: %v", rows.Err())
	}
	var answer int64
	if err := rows.Scan(&answer); err != nil {
		t.Fatal(err)
	}
	if answer != 42 {
		t.Fatalf("answer = %d", answer)
	}
	rows.Close()
}
