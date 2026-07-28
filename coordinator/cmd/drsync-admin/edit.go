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

	if !a.writable {
		form.AddTextView("", "[yellow::]read-only mode — restart with --write to edit[-::]", 0, 1, true, false)
	}

	form.AddButton("Save", func() {
		a.confirmAndApply(tv, row, edits, func() { a.showTableView(tv) })
	})
	form.AddButton("Cancel", func() { a.showTableView(tv) })
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
