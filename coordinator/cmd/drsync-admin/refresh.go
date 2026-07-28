package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rivo/tview"
)

// minRefreshInterval is the floor on any timed auto-refresh. Below this a
// tight loop against shards (potentially millions of rows at PB scale) would
// contend with the coordinator's own hot-path writes for no operator benefit
// — nothing in this UI changes meaningfully faster than a few seconds.
const minRefreshInterval = 5 * time.Second

// autoRefresher drives an optional periodic reload for one screen (a table
// view or the DB info view). Refresh is on-demand by default — no ticker
// runs until the operator explicitly sets an interval — matching the "never
// silently poll the database" default this tool otherwise holds to.
//
// Every screen owns its own autoRefresher and stops it on navigating away
// (see backable/showTableView in main.go): a ticker left running against a
// closed or no-longer-visible screen would keep reloading data nobody is
// looking at, and would leak goroutines across repeated open/close cycles.
type autoRefresher struct {
	mu       sync.Mutex
	interval time.Duration // 0 = off
	stop     chan struct{} // non-nil while a ticker goroutine is running
}

// Set starts (or restarts, or stops) periodic refresh. interval <= 0 turns
// it off. A non-zero interval below minRefreshInterval is clamped up, not
// rejected — a fat-fingered "1" should still do something useful rather than
// silently no-op.
func (r *autoRefresher) Set(interval time.Duration, onTick func()) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stop != nil {
		close(r.stop)
		r.stop = nil
	}
	if interval <= 0 {
		r.interval = 0
		return 0
	}
	if interval < minRefreshInterval {
		interval = minRefreshInterval
	}
	r.interval = interval

	stop := make(chan struct{})
	r.stop = stop
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				onTick()
			}
		}
	}()
	return interval
}

// Stop turns off periodic refresh, if any. Safe to call multiple times (on
// every navigate-away path, whether or not refresh was ever enabled).
func (r *autoRefresher) Stop() {
	r.Set(0, nil)
}

// Interval reports the current refresh interval, 0 meaning on-demand only.
func (r *autoRefresher) Interval() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.interval
}

// openRefreshDialog lets the operator set (or turn off) timed auto-refresh
// for whichever screen is open. reloadFn re-runs that screen's query;
// updateStatusFn refreshes its status/footer line to show the new interval.
// Both are invoked from the ticker goroutine via QueueUpdateDraw — tview
// widgets are not safe to mutate off the main event-processing goroutine,
// and a raw background ticker calling into tview directly is exactly the
// kind of bug that only shows up as an intermittent crash under load.
func (a *App) openRefreshDialog(refresher *autoRefresher, reloadFn, updateStatusFn func()) {
	returnTo := a.tui.GetFocus()
	current := refresher.Interval()
	defaultText := "off"
	if current > 0 {
		defaultText = strconv.Itoa(int(current / time.Second))
	}

	form := tview.NewForm()
	form.SetBorder(true).SetTitle(" Timed refresh ")
	form.AddTextView("", fmt.Sprintf("Seconds between refreshes, or \"off\". Minimum %d.", int(minRefreshInterval/time.Second)), 0, 2, true, false)
	field := tview.NewInputField().SetLabel("interval ").SetText(defaultText)
	form.AddFormItem(field)

	apply := func() {
		text := strings.TrimSpace(field.GetText())
		var requested time.Duration
		if text != "" && !strings.EqualFold(text, "off") {
			secs, err := strconv.Atoi(text)
			if err != nil || secs < 0 {
				a.flashError(fmt.Errorf("invalid refresh interval %q: enter whole seconds, or \"off\"", text))
				return
			}
			requested = time.Duration(secs) * time.Second
		}

		applied := refresher.Set(requested, func() {
			a.tui.QueueUpdateDraw(func() {
				reloadFn()
			})
		})

		switch {
		case requested <= 0:
			a.flashStatus("refresh: on-demand")
		case applied != requested:
			a.flashStatus(fmt.Sprintf("refresh: %s (raised from %ds, minimum is %ds)",
				applied, int(requested/time.Second), int(minRefreshInterval/time.Second)))
		default:
			a.flashStatus(fmt.Sprintf("refresh: every %s", applied))
		}
		updateStatusFn()
		a.pages.RemovePage("refresh-dialog")
		a.setFocus(returnTo)
	}

	form.AddButton("Apply", apply)
	form.AddButton("Cancel", func() {
		a.pages.RemovePage("refresh-dialog")
		a.setFocus(returnTo)
	})
	form.SetCancelFunc(func() {
		a.pages.RemovePage("refresh-dialog")
		a.setFocus(returnTo)
	})

	a.pages.AddPage("refresh-dialog", modalWrap(form, 60, 9), true, true)
	a.setFocus(form)
}
