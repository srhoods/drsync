package main

import (
	"fmt"

	"github.com/rivo/tview"
)

type pendingEdit struct {
	col string
	get func() string
}

// openRowDetail shows every column of a selected row. Columns in
// editableColumns render as input fields; everything else renders read-only.
// Submitting a change always goes through a before/after confirmation modal
// before touching the database.
//
// If nothing on this row is actually editable — read-only mode, or a table
// with no allowlisted columns at all (shard_counts, chunk_groups, splits,
// journal_cursors) — this is a pure viewer: no Save/Cancel pair, just Close.
// Showing Save/Cancel when there is nothing to save was misleading (Save
// always "succeeded" with zero changes, silently).
func (a *App) openRowDetail(tv *TableView, row Row) {
	form := tview.NewForm()
	form.SetBorder(true).SetTitle(fmt.Sprintf(" %s — row %s ", tv.table, row.String(pkIndex(row, tv.pkCol))))

	var edits []pendingEdit

	for i, col := range row.Cols {
		val := row.String(i)
		if a.writable && isEditable(tv.table, col) {
			field := tview.NewInputField().SetLabel(col + " ").SetText(val)
			form.AddFormItem(field)
			edits = append(edits, pendingEdit{col: col, get: field.GetText})
		} else {
			form.AddTextView(col, val, 0, 1, true, false)
		}
	}

	if len(edits) == 0 {
		if !a.writable && hasEditableColumns(tv.table) {
			form.AddTextView("", "[yellow::]read-only mode — restart with --write to edit[-::]", 0, 1, true, false)
		} else {
			form.AddTextView("", "[gray::]no editable fields on this table[-::]", 0, 1, true, false)
		}
		form.AddButton("Close", func() { a.showTableView(tv) })
	} else {
		form.AddButton("Save", func() {
			a.confirmAndApply(tv, row, edits, func() { a.showTableView(tv) })
		})
		form.AddButton("Cancel", func() { a.showTableView(tv) })
	}
	form.SetCancelFunc(func() { a.showTableView(tv) })

	a.pages.AddAndSwitchToPage("row", modalWrap(form, 80, 24), true)
	a.setFocus(form)
}

func pkIndex(row Row, pk string) int {
	for i, c := range row.Cols {
		if c == pk {
			return i
		}
	}
	return 0
}

// confirmAndApply diffs each editable field against its original value and,
// if anything changed, shows a before/after summary requiring explicit
// confirmation before writing — mirroring the double-gate pattern already
// used elsewhere in this codebase for destructive operator actions.
func (a *App) confirmAndApply(tv *TableView, row Row, edits []pendingEdit, onDone func()) {
	type change struct{ col, from, to string }
	var changes []change
	for _, e := range edits {
		i := colIndex(row, e.col)
		from := row.String(i)
		to := e.get()
		if from != to {
			changes = append(changes, change{e.col, from, to})
		}
	}
	if len(changes) == 0 {
		onDone()
		return
	}

	msg := fmt.Sprintf("Apply changes to %s.%v?\n\n", tv.table, row.String(pkIndex(row, tv.pkCol)))
	for _, c := range changes {
		msg += fmt.Sprintf("  %s: %q -> %q\n", c.col, c.from, c.to)
	}

	modal := tview.NewModal().
		SetText(msg).
		AddButtons([]string{"Confirm", "Cancel"}).
		SetDoneFunc(func(idx int, label string) {
			if label == "Confirm" {
				idVal := row.Values[pkIndex(row, tv.pkCol)]
				var lastErr error
				for _, c := range changes {
					if err := updateColumn(a.db, tv.table, tv.pkCol, idVal, c.col, c.to); err != nil {
						lastErr = err
						break
					}
				}
				if lastErr != nil {
					a.flashError(lastErr)
				} else {
					a.flashStatus(fmt.Sprintf("updated %s.%v (%d field(s))", tv.table, idVal, len(changes)))
					tv.reload(tv.filter.GetText())
				}
			}
			onDone()
		})
	a.pages.AddAndSwitchToPage("confirm", modal, true)
	a.setFocus(modal)
}

// openBulkEdit lets the operator set one allowlisted column to one new value
// across every currently-checked row in a table — the priority-bump-on-
// hundreds-of-shards workflow: filter down to the rows that need it (e.g.
// state=PARKED), check some or all of them (space / 'a' for all), then apply
// the change once instead of opening and saving each row individually.
func (a *App) openBulkEdit(tv *TableView) {
	rows := tv.checkedRows()
	if len(rows) == 0 {
		return
	}
	cols := editableColumnNames(tv.table)
	if len(cols) == 0 {
		return
	}

	form := tview.NewForm()
	form.SetBorder(true).SetTitle(fmt.Sprintf(" %s — bulk edit %d row(s) ", tv.table, len(rows)))

	selectedCol := cols[0]
	form.AddDropDown("column", cols, 0, func(option string, _ int) { selectedCol = option })
	value := tview.NewInputField().SetLabel("new value ")
	form.AddFormItem(value)

	form.AddButton("Apply", func() {
		a.confirmBulkApply(tv, rows, selectedCol, value.GetText, func() { a.showTableView(tv) })
	})
	form.AddButton("Cancel", func() { a.showTableView(tv) })
	form.SetCancelFunc(func() { a.showTableView(tv) })

	a.pages.AddAndSwitchToPage("bulk", modalWrap(form, 70, 12), true)
	a.setFocus(form)
}

// confirmBulkApply shows one compact confirmation (row count + before-value
// breakdown, not a line per row) before writing the same column/value to
// every checked row. Distinct row IDs are still tracked individually so a
// per-row failure part-way through is reported precisely.
func (a *App) confirmBulkApply(tv *TableView, rows []Row, col string, getVal func() string, onDone func()) {
	newVal := getVal()

	// Group current values so "247 rows: priority 0 -> 30, 3 rows: priority
	// 5 -> 30" reads at a glance instead of one line per row.
	fromCounts := map[string]int{}
	for _, row := range rows {
		fromCounts[row.String(colIndex(row, col))]++
	}
	unchanged := fromCounts[newVal]

	msg := fmt.Sprintf("Apply %s.%s = %q to %d row(s)?\n\n", tv.table, col, newVal, len(rows))
	for from, n := range fromCounts {
		if from == newVal {
			continue
		}
		msg += fmt.Sprintf("  %d row(s): %q -> %q\n", n, from, newVal)
	}
	if unchanged > 0 {
		msg += fmt.Sprintf("  %d row(s) already %q (no change)\n", unchanged, newVal)
	}
	if unchanged == len(rows) {
		// Every checked row already has this value — nothing to confirm.
		onDone()
		return
	}

	modal := tview.NewModal().
		SetText(msg).
		AddButtons([]string{"Confirm", "Cancel"}).
		SetDoneFunc(func(idx int, label string) {
			if label == "Confirm" {
				applied, failed := 0, 0
				var lastErr error
				for _, row := range rows {
					if row.String(colIndex(row, col)) == newVal {
						continue
					}
					idVal := row.Values[pkIndex(row, tv.pkCol)]
					if err := updateColumn(a.db, tv.table, tv.pkCol, idVal, col, newVal); err != nil {
						failed++
						lastErr = err
						continue
					}
					applied++
				}
				switch {
				case failed == 0:
					a.flashStatus(fmt.Sprintf("updated %s.%s on %d row(s)", tv.table, col, applied))
				case applied == 0:
					a.flashError(fmt.Errorf("bulk update failed on all %d row(s): %w", failed, lastErr))
				default:
					a.flashError(fmt.Errorf("updated %d row(s), %d failed (last error: %w)", applied, failed, lastErr))
				}
				tv.reload(tv.filter.GetText())
			}
			onDone()
		})
	a.pages.AddAndSwitchToPage("confirm", modal, true)
	a.setFocus(modal)
}

func colIndex(row Row, col string) int {
	for i, c := range row.Cols {
		if c == col {
			return i
		}
	}
	return -1
}

// modalWrap centers a primitive in a fixed-size box, the standard tview
// idiom for dialog-like pages.
func modalWrap(p tview.Primitive, width, height int) tview.Primitive {
	return tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 0, 1, false).
			AddItem(p, height, 0, true).
			AddItem(nil, 0, 1, false), width, 0, true).
		AddItem(nil, 0, 1, false)
}
