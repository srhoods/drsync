package main

import "testing"

func testCols() []ColumnInfo {
	return []ColumnInfo{{Name: "id"}, {Name: "kind"}, {Name: "state"}, {Name: "priority"}}
}

func TestBuildFilterClauseEquals(t *testing.T) {
	clause, args, err := buildFilterClause(testCols(), "kind=entrylist")
	if err != nil {
		t.Fatal(err)
	}
	if clause != `"kind" = ?` {
		t.Fatalf("clause = %q", clause)
	}
	if len(args) != 1 || args[0] != "entrylist" {
		t.Fatalf("args = %v", args)
	}
}

func TestBuildFilterClauseMultipleTermsAND(t *testing.T) {
	clause, args, err := buildFilterClause(testCols(), "kind=entrylist,state=PARKED")
	if err != nil {
		t.Fatal(err)
	}
	if clause != `"kind" = ? AND "state" = ?` {
		t.Fatalf("clause = %q", clause)
	}
	if len(args) != 2 {
		t.Fatalf("args = %v", args)
	}
}

func TestBuildFilterClauseOperators(t *testing.T) {
	cases := map[string]string{
		"priority>5":  `"priority" > ?`,
		"priority<5":  `"priority" < ?`,
		"state!=DONE": `"state" != ?`,
		"kind~entry":  `"kind" LIKE ?`,
	}
	for term, want := range cases {
		clause, _, err := buildFilterClause(testCols(), term)
		if err != nil {
			t.Fatalf("%s: %v", term, err)
		}
		if clause != want {
			t.Errorf("%s: clause = %q, want %q", term, clause, want)
		}
	}
}

func TestBuildFilterClauseRejectsUnknownColumn(t *testing.T) {
	if _, _, err := buildFilterClause(testCols(), "nope=1"); err == nil {
		t.Fatal("expected error for unknown column")
	}
}

func TestBuildFilterClauseRejectsGarbage(t *testing.T) {
	if _, _, err := buildFilterClause(testCols(), "just garbage no operator"); err == nil {
		t.Fatal("expected error for term with no operator")
	}
}

func TestBuildFilterClauseEmpty(t *testing.T) {
	clause, args, err := buildFilterClause(testCols(), "")
	if err != nil || clause != "" || args != nil {
		t.Fatalf("empty filter should be a no-op, got clause=%q args=%v err=%v", clause, args, err)
	}
}

func TestBuildFilterClauseNeverInterpolatesValueIntoSQL(t *testing.T) {
	// A classic injection payload in the value position must remain a bound
	// parameter, never concatenated into the query string.
	clause, args, err := buildFilterClause(testCols(), `kind=entrylist' OR '1'='1`)
	if err != nil {
		t.Fatal(err)
	}
	if clause != `"kind" = ?` {
		t.Fatalf("clause = %q; value leaked into SQL text", clause)
	}
	if args[0] != `entrylist' OR '1'='1` {
		t.Fatalf("args = %v", args)
	}
}
