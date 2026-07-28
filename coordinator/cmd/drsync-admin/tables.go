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

	tv := &TableView{app: app, table: table, cols: cols, pkCol: pk}

	tv.filter = tview.NewInputField().SetLabel("filter (col=val, col!=val, col>val, col~substr, comma-joined AND): ")
	tv.grid = tview.NewTable().SetBorders(false).SetSelectable(true, false).SetFixed(1, 0)
	tv.grid.SetSelectedStyle(tcell.StyleDefault.Background(app.theme.Selection))
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
		if event.Rune() == '/' {
			app.setFocus(tv.filter)
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
	tv.render()

	total, _ := tableRowCount(tv.app.db, tv.table)
	shown := len(rows)
	truncated := ""
	if int64(shown) == int64(defaultRowLimit) && total > int64(defaultRowLimit) {
		truncated = fmt.Sprintf(" (truncated — %d total, narrow with a filter)", total)
	}
	mode := "read-only"
	if tv.app.writable {
		mode = "read-write"
	}
	tv.status.SetText(fmt.Sprintf("%d row(s) shown%s  |  mode: %s  |  /: filter  |  Enter: view/edit row  |  Esc: back to tables",
		shown, truncated, mode))
}

func (tv *TableView) render() {
	g := tv.grid
	g.Clear()
	for c, name := range tv.colName {
		cell := tview.NewTableCell(name).
			SetTextColor(tv.app.theme.Title).
			SetSelectable(false).
			SetAttributes(tcell.AttrBold)
		g.SetCell(0, c, cell)
	}
	for r, row := range tv.rows {
		for c := range tv.colName {
			text := row.String(c)
			color := tv.app.theme.Foreground
			if tv.colName[c] == "state" {
				tag, col := tv.app.theme.StateMarker(text)
				text = tag + " " + text
				color = col
			}
			g.SetCell(r+1, c, tview.NewTableCell(text).SetTextColor(color))
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
