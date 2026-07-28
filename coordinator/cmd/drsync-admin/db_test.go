package main

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/test.db?_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE shards (
		id INTEGER PRIMARY KEY, pass_id INTEGER, kind TEXT, state TEXT, priority INTEGER, attempt INTEGER
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO shards (id, pass_id, kind, state, priority, attempt) VALUES
		(1, 1, 'dir', 'QUEUED', 0, 0),
		(2, 1, 'entrylist', 'PARKED', 0, 3),
		(3, 1, 'chunk', 'DONE', 10, 0)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestUpdateColumnAllowlisted(t *testing.T) {
	db := testDB(t)
	if err := updateColumn(db, "shards", "id", int64(2), "priority", "25"); err != nil {
		t.Fatalf("expected allowlisted edit to succeed: %v", err)
	}
	var got string
	if err := db.QueryRow(`SELECT priority FROM shards WHERE id=2`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "25" {
		t.Fatalf("priority = %q, want 25", got)
	}
}

func TestUpdateColumnRejectsNonAllowlisted(t *testing.T) {
	db := testDB(t)
	if err := updateColumn(db, "shards", "id", int64(2), "pass_id", "999"); err == nil {
		t.Fatal("expected non-allowlisted column edit to be rejected")
	}
	var got int64
	if err := db.QueryRow(`SELECT pass_id FROM shards WHERE id=2`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("pass_id changed despite rejection: got %d", got)
	}
}

func TestUpdateColumnRejectsUnknownTable(t *testing.T) {
	db := testDB(t)
	if err := updateColumn(db, "agents", "id", "x", "enabled", "0"); err == nil {
		t.Fatal("expected edit on a table with no matching allowlist entry to be rejected")
	}
}

func TestQueryRowsRespectsLimit(t *testing.T) {
	db := testDB(t)
	rows, cols, err := queryRows(db, "shards", "", nil, "id", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2 (limit)", len(rows))
	}
	if len(cols) != 6 {
		t.Fatalf("got %d columns, want 6", len(cols))
	}
}

func TestTableColumnsAndPK(t *testing.T) {
	db := testDB(t)
	cols, err := tableColumns(db, "shards")
	if err != nil {
		t.Fatal(err)
	}
	foundPK := false
	for _, c := range cols {
		if c.Name == "id" && c.PK {
			foundPK = true
		}
	}
	if !foundPK {
		t.Fatal("expected id to be reported as primary key")
	}
}

func TestRowStringHandlesNilAndBytes(t *testing.T) {
	r := Row{Cols: []string{"a", "b", "c"}, Values: []any{nil, []byte("hi"), int64(5)}}
	if r.String(0) != "" {
		t.Errorf("nil -> %q, want empty", r.String(0))
	}
	if r.String(1) != "hi" {
		t.Errorf("[]byte -> %q, want hi", r.String(1))
	}
	if r.String(2) != "5" {
		t.Errorf("int64 -> %q, want 5", r.String(2))
	}
}
