package main

import (
	"fmt"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// defaultRowLimit caps how many rows a table view fetches at once. shards can
// be millions of rows at PB scale (DESIGN-coordinator.md §3), so browsing
// without a filter must never attempt to load the whole table.
const defaultRowLimit = 500

// TableView renders one table's rows as a tview.Table, with a filter input
// and a primary-key column so edits know which row they're touching.
//
// checked tracks checkbox state per currently-loaded row (index into rows),
// for the bulk-edit workflow: filter down to the rows you want (e.g.
// state=PARKED), check some or all of them, then apply one column/value
// change to every checked row in a single confirmation instead of one
// row-by-row edit each.
type TableView struct {
	app     *App
	table   string
	cols    []ColumnInfo
	pkCol   string
	root    *tview.Flex
	filter  *tview.InputField
	grid    *tview.Table
	status  *tview.TextView
	rows    []Row
	colName []string
	checked map[int]bool
}

func newTableView(app *App, table string) (*TableView, error) {
	cols, err := tableColumns(app.db, table)
	if err != nil {
		return nil, err
	}
	pk := "id"
	for _, c := range cols {
		if c.PK {
			pk = c.Name
			break
		}
	}

	tv := &TableView{app: app, table: table, cols: cols, pkCol: pk, checked: map[int]bool{}}

	tv.filter = tview.NewInputField().SetLabel("filter (col=val, col!=val, col>val, col~substr, comma-joined AND): ")
	tv.grid = tview.NewTable().SetBorders(false).SetSelectable(true, false).SetFixed(1, 0)
	tv.grid.SetSelectedStyle(tcell.StyleDefault.Background(app.theme.ListSelection).Foreground(app.theme.SelectedText))
	tv.status = tview.NewTextView().SetDynamicColors(true)

	tv.filter.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			tv.reload(tv.filter.GetText())
		}
		app.setFocus(tv.grid)
	})

	tv.grid.SetSelectedFunc(func(row, col int) {
		if row <= 0 || row-1 >= len(tv.rows) {
			return
		}
		app.openRowDetail(tv, tv.rows[row-1])
	})
	tv.grid.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch {
		case event.Rune() == '/':
			app.setFocus(tv.filter)
			return nil
		case event.Rune() == ' ':
			r, _ := tv.grid.GetSelection()
			tv.toggleChecked(r - 1)
			return nil
		case event.Rune() == 'a':
			tv.toggleAll()
			return nil
		case event.Rune() == 'e':
			if app.writable && len(tv.checked) > 0 && hasEditableColumns(tv.table) {
				app.openBulkEdit(tv)
			}
			return nil
		}
		return event
	})

	tv.root = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(tv.filter, 1, 0, false).
		AddItem(tv.grid, 0, 1, true).
		AddItem(tv.status, 1, 0, false)
	tv.root.SetBorder(true).SetTitle(fmt.Sprintf(" %s ", table))

	tv.reload("")
	return tv, nil
}

func (tv *TableView) reload(filter string) {
	clause, args, err := buildFilterClause(tv.cols, filter)
	if err != nil {
		tv.status.SetText(fmt.Sprintf("[red::]filter error: %v[-::]", err))
		return
	}
	orderBy := tv.pkCol
	rows, names, err := queryRows(tv.app.db, tv.table, clause, args, orderBy, defaultRowLimit)
	if err != nil {
		tv.status.SetText(fmt.Sprintf("[red::]query error: %v[-::]", err))
		return
	}
	tv.rows = rows
	tv.colName = names
	// Row identity/positions just changed under a new filter — stale
	// checkbox state pointing at different rows would silently bulk-edit
	// the wrong thing, so any change to the result set clears selection.
	tv.checked = map[int]bool{}
	tv.render()
	tv.updateStatus()
}

func (tv *TableView) updateStatus() {
	total, _ := tableRowCount(tv.app.db, tv.table)
	shown := len(tv.rows)
	truncated := ""
	if int64(shown) == int64(defaultRowLimit) && total > int64(defaultRowLimit) {
		truncated = fmt.Sprintf(" (truncated — %d total, narrow with a filter)", total)
	}
	mode := "read-only"
	if tv.app.writable {
		mode = "read-write"
	}
	checkedMsg := ""
	if tv.app.writable && hasEditableColumns(tv.table) {
		checkedMsg = fmt.Sprintf("  |  %d checked (space: check, a: all, e: bulk edit)", len(tv.checked))
	}
	tv.status.SetText(fmt.Sprintf("%d row(s) shown%s  |  mode: %s%s  |  Enter: view/edit row  |  /: filter  |  Esc: back",
		shown, truncated, mode, checkedMsg))
}

func (tv *TableView) toggleChecked(rowIdx int) {
	if rowIdx < 0 || rowIdx >= len(tv.rows) {
		return
	}
	if tv.checked[rowIdx] {
		delete(tv.checked, rowIdx)
	} else {
		tv.checked[rowIdx] = true
	}
	tv.render()
	tv.updateStatus()
}

func (tv *TableView) toggleAll() {
	if len(tv.checked) == len(tv.rows) {
		tv.checked = map[int]bool{}
	} else {
		for i := range tv.rows {
			tv.checked[i] = true
		}
	}
	tv.render()
	tv.updateStatus()
}

// checkedRows returns the currently-checked rows in stable (loaded) order.
func (tv *TableView) checkedRows() []Row {
	var out []Row
	for i, row := range tv.rows {
		if tv.checked[i] {
			out = append(out, row)
		}
	}
	return out
}

func (tv *TableView) render() {
	g := tv.grid
	g.Clear()
	showCheckboxes := tv.app.writable && hasEditableColumns(tv.table)

	colOffset := 0
	if showCheckboxes {
		g.SetCell(0, 0, tview.NewTableCell("sel").SetSelectable(false))
		colOffset = 1
	}
	for c, name := range tv.colName {
		cell := tview.NewTableCell(name).
			SetTextColor(tv.app.theme.Title).
			SetSelectable(false).
			SetAttributes(tcell.AttrBold)
		g.SetCell(0, c+colOffset, cell)
	}
	for r, row := range tv.rows {
		if showCheckboxes {
			// Not "[ ]"/"[x]": verified live that tview swallows those as a
			// style/region tag on the checked row specifically (its own tag
			// parser runs on every TableCell, with no per-cell opt-out) —
			// the checkbox rendered blank on toggle instead of updating.
			// Parens avoid the tag parser's leading '[' trigger entirely.
			box := "( )"
			if tv.checked[r] {
				box = "(x)"
			}
			g.SetCell(r+1, 0, tview.NewTableCell(box).SetTextColor(tv.app.theme.Foreground))
		}
		for c := range tv.colName {
			text := row.String(c)
			color := tv.app.theme.Foreground
			if tv.colName[c] == "state" {
				tag, col := tv.app.theme.StateMarker(text)
				text = tag + " " + text
				color = col
			}
			g.SetCell(r+1, c+colOffset, tview.NewTableCell(text).SetTextColor(color))
		}
	}
}

func (a *App) newTablesScreen() *tview.Flex {
	names, err := tableNames(a.db)
	if err != nil {
		names = nil
	}
	list := tview.NewList().ShowSecondaryText(true)
	list.SetBorder(true).SetTitle(" Tables ")
	// Explicit, not inherited from tview.Styles at construction time: List
	// snapshots the package-level style vars into its own selectedStyle field
	// when NewList() runs, so a global theme applied afterwards would leave
	// the very first (auto-selected) row using tview's built-in default
	// colours instead of this theme's — which is exactly how a mono/
	// high-contrast theme ended up with an invisible black-on-black or
	// white-on-white selected row.
	list.SetSelectedTextColor(a.theme.SelectedText)
	list.SetSelectedBackgroundColor(a.theme.ListSelection)
	for _, t := range names {
		t := t
		n, _ := tableRowCount(a.db, t)
		list.AddItem(t, fmt.Sprintf("%d rows", n), 0, func() {
			a.openTable(t)
		})
	}
	list.AddItem("Database info", "PRAGMA config, sizes, summary counts", 0, func() {
		a.openDBInfo()
	})

	help := tview.NewTextView().SetDynamicColors(true).
		SetText("[::b]drsync-admin[::-]  |  Enter: open  |  q: quit")
	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(list, 0, 1, true).
		AddItem(help, 1, 0, false)
	return root
}
