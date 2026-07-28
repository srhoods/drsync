// drsync-admin is a standalone console for browsing and editing the
// coordinator's SQLite state directly — table/schema browser, a database
// configuration screen, and allowlisted field edits (e.g. shard priority),
// filterable by column. It opens the DB file the same way store.Open does
// (WAL, busy_timeout) so it is safe to run alongside a live drsyncd, and
// needs no running coordinator or network access at all.
//
//	drsync-admin -db /var/lib/drsync/state.db            # read-only browser
//	drsync-admin -db /var/lib/drsync/state.db -write     # allow field edits
//	drsync-admin -db state.db -theme high-contrast
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// App holds the shared UI/DB state every screen needs.
type App struct {
	db       *sql.DB
	dbPath   string
	writable bool
	theme    Theme

	tui       *tview.Application
	pages     *tview.Pages
	root      *tview.Flex // holds the current screen plus the persistent status bar
	statusBar *tview.TextView
}

func main() {
	dbPath := flag.String("db", "", "path to the coordinator's SQLite state file (required)")
	write := flag.Bool("write", false, "allow editing allowlisted fields (default: read-only browsing)")
	theme := flag.String("theme", "default", "colour palette: default | high-contrast | mono (mono is forced when NO_COLOR is set)")
	flag.Parse()

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "usage: drsync-admin -db PATH [-write] [-theme default|high-contrast|mono]")
		os.Exit(2)
	}

	db, err := openDB(*dbPath, *write)
	if err != nil {
		fmt.Fprintln(os.Stderr, "drsync-admin:", err)
		os.Exit(1)
	}
	defer db.Close()

	app := newApp(db, *dbPath, *write, resolveTheme(*theme))
	app.tui.EnableMouse(true)
	if err := app.tui.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "drsync-admin:", err)
		os.Exit(1)
	}
}

// newApp wires up the full widget tree against an already-open db. Split out
// from main so tests can drive the real UI against a tcell SimulationScreen
// instead of a real terminal.
func newApp(db *sql.DB, dbPath string, write bool, th Theme) *App {
	th.applyTviewDefaults()
	app := &App{
		db: db, dbPath: dbPath, writable: write, theme: th,
		tui: tview.NewApplication(), pages: tview.NewPages(),
		statusBar: tview.NewTextView().SetDynamicColors(true),
	}
	app.pages.AddPage("tables", app.newTablesScreen(), true, true)

	app.root = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(app.pages, 0, 1, true).
		AddItem(app.statusBar, 1, 0, false)
	app.setStatus(app.modeLabel())
	app.tui.SetRoot(app.root, true)
	app.tui.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		// 'q' quits from anywhere except while text entry has focus (a filter
		// or an edit field), where 'q' must be allowed to type the letter q.
		if event.Rune() == 'q' {
			if _, editing := app.tui.GetFocus().(*tview.InputField); editing {
				return event
			}
			app.tui.Stop()
			return nil
		}
		return event
	})
	return app
}

func (a *App) modeLabel() string {
	if a.writable {
		return "[orange::b]mode: read-write[-::-]  |  db: " + a.dbPath
	}
	return "[gray::]mode: read-only (start with -write to edit)[-::]  |  db: " + a.dbPath
}

func (a *App) setStatus(msg string) {
	a.statusBar.SetText(msg)
}

func (a *App) setFocus(p tview.Primitive) {
	a.tui.SetFocus(p)
}

func (a *App) openTable(name string) {
	tv, err := newTableView(a, name)
	if err != nil {
		a.flashError(err)
		return
	}
	page := "table:" + name
	a.pages.AddAndSwitchToPage(page, backable(tv.root, func() { a.pages.SwitchToPage("tables") }), true)
	a.setFocus(tv.grid)
}

func (a *App) showTableView(tv *TableView) {
	a.pages.SwitchToPage("table:" + tv.table)
	a.setFocus(tv.grid)
}

func (a *App) openDBInfo() {
	view := newDBInfoView(a.db, a.dbPath)
	a.pages.AddAndSwitchToPage("dbinfo", backable(view, func() { a.pages.SwitchToPage("tables") }), true)
	a.setFocus(view)
}

func (a *App) flashStatus(msg string) {
	a.setStatus(msg + "   |   " + a.modeLabel())
}

func (a *App) flashError(err error) {
	modal := tview.NewModal().
		SetText("error: " + err.Error()).
		AddButtons([]string{"OK"}).
		SetDoneFunc(func(idx int, label string) {
			a.pages.RemovePage("error")
		})
	a.pages.AddPage("error", modal, true, true)
}

// backable wires Esc to a "return to previous screen" callback on any
// primitive whose concrete type embeds *tview.Box (everything used as a
// top-level screen here does), without every screen needing its own capture.
func backable(p tview.Primitive, back func()) tview.Primitive {
	type boxer interface {
		SetInputCapture(func(*tcell.EventKey) *tcell.EventKey) *tview.Box
		GetInputCapture() func(*tcell.EventKey) *tcell.EventKey
	}
	b, ok := p.(boxer)
	if !ok {
		return p
	}
	prev := b.GetInputCapture()
	b.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			back()
			return nil
		}
		if prev != nil {
			return prev(event)
		}
		return event
	})
	return p
}
