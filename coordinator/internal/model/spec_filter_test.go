package model

import (
	"strings"
	"testing"
)

const filterBase = `apiVersion: drsync/v1
kind: Job
metadata:
  name: f
spec:
  source: { path: /src }
  destination: { path: /dst }
`

func TestFiltersParseInOrder(t *testing.T) {
	spec := filterBase + `  filters:
    - exclude: "**/.snapshot/**"
    - exclude: "**/*.tmp"
    - include: "**"
`
	s, err := ParseSpec([]byte(spec))
	if err != nil {
		t.Fatal(err)
	}
	o, err := s.ToJobOptions(1, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(o.Filters) != 3 {
		t.Fatalf("want 3 resolved filters, got %d", len(o.Filters))
	}
	if !o.Filters[0].Exclude || o.Filters[0].Pattern != "**/.snapshot/**" {
		t.Fatalf("rule 0 = %+v", o.Filters[0])
	}
	if o.Filters[2].Exclude || o.Filters[2].Pattern != "**" {
		t.Fatalf("rule 2 = %+v", o.Filters[2])
	}
}

func TestFilterRejectEmptyPattern(t *testing.T) {
	spec := filterBase + `  filters:
    - exclude: ""
`
	if _, err := ParseSpec([]byte(spec)); err == nil {
		t.Fatal("empty pattern should fail validation")
	}
}

func TestFilterRejectOverlongPattern(t *testing.T) {
	spec := filterBase + `  filters:
    - exclude: "` + strings.Repeat("a", maxFilterPattern+1) + `"
`
	if _, err := ParseSpec([]byte(spec)); err == nil {
		t.Fatalf("pattern over %d bytes should fail validation", maxFilterPattern)
	}
}

func TestFilterRejectTooManyRules(t *testing.T) {
	var b strings.Builder
	b.WriteString(filterBase + "  filters:\n")
	for i := 0; i <= maxFilterRules; i++ {
		b.WriteString("    - exclude: \"*.x\"\n")
	}
	if _, err := ParseSpec([]byte(b.String())); err == nil {
		t.Fatalf("more than %d rules should fail validation", maxFilterRules)
	}
}
