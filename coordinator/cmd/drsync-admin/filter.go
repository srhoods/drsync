package main

import (
	"fmt"
	"strings"
)

// buildFilterClause parses a small filter language into a parameterized SQL
// WHERE clause: comma-separated `col=value`, `col!=value`, `col>value`,
// `col<value`, or `col~value` (LIKE substring match) terms, ANDed together.
// Column names are validated against the table's own schema (never
// interpolated from arbitrary user text into the column position), and
// values are always bound as parameters — never concatenated into the query
// string — so this cannot be used for SQL injection.
func buildFilterClause(cols []ColumnInfo, filter string) (clause string, args []any, err error) {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return "", nil, nil
	}
	valid := make(map[string]bool, len(cols))
	for _, c := range cols {
		valid[c.Name] = true
	}

	var parts []string
	for _, term := range strings.Split(filter, ",") {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		col, op, val, err := splitTerm(term)
		if err != nil {
			return "", nil, err
		}
		if !valid[col] {
			return "", nil, fmt.Errorf("unknown column %q", col)
		}
		switch op {
		case "=":
			parts = append(parts, fmt.Sprintf("%q = ?", col))
			args = append(args, val)
		case "!=":
			parts = append(parts, fmt.Sprintf("%q != ?", col))
			args = append(args, val)
		case ">":
			parts = append(parts, fmt.Sprintf("%q > ?", col))
			args = append(args, val)
		case "<":
			parts = append(parts, fmt.Sprintf("%q < ?", col))
			args = append(args, val)
		case "~":
			parts = append(parts, fmt.Sprintf("%q LIKE ?", col))
			args = append(args, "%"+val+"%")
		}
	}
	return strings.Join(parts, " AND "), args, nil
}

// splitTerm splits "col!=value" style terms, checking longer (two-rune)
// operators before the single-rune ones they contain.
func splitTerm(term string) (col, op, val string, err error) {
	for _, candidate := range []string{"!=", "=", ">", "<", "~"} {
		if i := strings.Index(term, candidate); i > 0 {
			return strings.TrimSpace(term[:i]), candidate, strings.TrimSpace(term[i+len(candidate):]), nil
		}
	}
	return "", "", "", fmt.Errorf("invalid filter term %q (expected col=value, col!=value, col>value, col<value, or col~value)", term)
}
