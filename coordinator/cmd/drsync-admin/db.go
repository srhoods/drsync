// Schema introspection and generic row access for drsync-admin. Deliberately
// independent of internal/store's typed API: that API is shaped for the
// coordinator's own operations (lease/grant/complete), not generic browsing
// of arbitrary tables, so this tool opens its own connection with the same
// DSN pragmas and works from sqlite's own catalog (sqlite_master,
// PRAGMA table_info) instead.
package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "modernc.org/sqlite"
)

// editableColumns is the sole allowlist of table.column pairs this tool will
// ever write to. It intentionally mirrors the columns the coordinator itself
// treats as operator-mutable (see coordinator/internal/store), so an edit
// here can never put the row into a state the daemon wouldn't produce itself.
var editableColumns = map[string]map[string]bool{
	"shards": {
		"priority": true, // scheduling lever: bump/lower a stuck or urgent shard
		"state":    true, // e.g. manually requeue a PARKED shard (QUEUED)
		"attempt":  true, // reset retry count alongside a manual requeue
	},
	"agents": {
		"enabled": true, // administrative drain/undrain, same effect as `drsync agent disable`
	},
	"jobs": {
		"state": true, // e.g. force CANCELLED on a stuck job
	},
}

func isEditable(table, col string) bool {
	return editableColumns[table] != nil && editableColumns[table][col]
}

// openDB opens the coordinator's SQLite file with the same WAL/busy_timeout
// pragmas store.Open uses, so this tool is safe to run concurrently with a
// live drsyncd: reads never block on the writer, and a write here waits up
// to 5s rather than racing it.
func openDB(path string, write bool) (*sql.DB, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("database file: %w", err)
	}
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)"
	if !write {
		dsn += "&_pragma=query_only(true)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// tableNames lists user tables (sqlite_% and internal rollup/index tables
// excluded is unnecessary here since schema has none named that way, but we
// filter sqlite_ housekeeping tables defensively).
func tableNames(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

type ColumnInfo struct {
	Name    string
	Type    string
	NotNull bool
	PK      bool
}

func tableColumns(db *sql.DB, table string) ([]ColumnInfo, error) {
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%q)`, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []ColumnInfo
	for rows.Next() {
		var cid int
		var name, ctype string
		var notNull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, ColumnInfo{Name: name, Type: ctype, NotNull: notNull != 0, PK: pk != 0})
	}
	return cols, rows.Err()
}

func tableRowCount(db *sql.DB, table string) (int64, error) {
	var n int64
	err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %q`, table)).Scan(&n)
	return n, err
}

// Row is a generic, ordered set of column values for display/editing.
type Row struct {
	Cols   []string
	Values []any // driver-scanned values: int64, float64, string, []byte, nil
}

func (r *Row) String(i int) string {
	v := r.Values[i]
	switch x := v.(type) {
	case nil:
		return ""
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(x)
	}
}

// queryRows runs a caller-built SELECT (table name and WHERE clause already
// validated/escaped by the caller — see buildFilterClause) and returns
// generic rows, capped at limit.
func queryRows(db *sql.DB, table string, whereClause string, whereArgs []any, orderBy string, limit int) ([]Row, []string, error) {
	cols, err := tableColumns(db, table)
	if err != nil {
		return nil, nil, err
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	q := fmt.Sprintf(`SELECT %s FROM %q`, quoteCols(names), table)
	if whereClause != "" {
		q += " WHERE " + whereClause
	}
	if orderBy != "" {
		q += " ORDER BY " + orderBy
	}
	q += fmt.Sprintf(" LIMIT %d", limit)

	rows, err := db.Query(q, whereArgs...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		vals := make([]any, len(names))
		ptrs := make([]any, len(names))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		out = append(out, Row{Cols: names, Values: vals})
	}
	return out, names, rows.Err()
}

func quoteCols(names []string) string {
	s := ""
	for i, n := range names {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%q", n)
	}
	return s
}

// updateColumn writes a single allowlisted column on a single row identified
// by its primary key (always "id" in this schema, except shard_counts /
// chunk_groups / splits / journal_cursors which are composite-keyed and
// intentionally excluded from editableColumns entirely).
func updateColumn(db *sql.DB, table, idCol string, idVal any, col string, newVal string) error {
	if !isEditable(table, col) {
		return fmt.Errorf("%s.%s is not editable", table, col)
	}
	q := fmt.Sprintf(`UPDATE %q SET %q = ? WHERE %q = ?`, table, col, idCol)
	_, err := db.Exec(q, newVal, idVal)
	return err
}

// pragmaValue reads a single-value PRAGMA (e.g. journal_mode, auto_vacuum).
func pragmaValue(db *sql.DB, pragma string) (string, error) {
	var v string
	err := db.QueryRow(fmt.Sprintf(`PRAGMA %s`, pragma)).Scan(&v)
	return v, err
}
