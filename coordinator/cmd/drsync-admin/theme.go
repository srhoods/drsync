package main

import (
	"os"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// Theme is a named colour palette. Every semantic colour (state/status) is
// chosen to stay distinguishable under the common forms of colour-blindness
// (deuteranopia/protanopia red-green confusion, tritanopia blue-yellow
// confusion), and every state is also rendered with a text/symbol marker
// elsewhere in the UI so colour is never the only signal.
type Theme struct {
	Name string

	Background tcell.Color
	Foreground tcell.Color
	Border     tcell.Color
	Title      tcell.Color

	// Semantic accents, deliberately not a red/green pair: blue/orange/yellow
	// remain distinguishable to protanopes, deuteranopes and tritanopes alike.
	Good    tcell.Color // e.g. DONE, connected, enabled
	Warn    tcell.Color // e.g. QUEUED/LEASED, in-progress
	Bad     tcell.Color // e.g. PARKED, FAILED, disconnected
	Neutral tcell.Color // e.g. CREATED/PENDING

	Selection tcell.Color
}

var themes = map[string]Theme{
	// Default: blue/orange/amber triad, high separation for all common CVD
	// types; avoids relying on red vs green.
	"default": {
		Name:       "default",
		Background: tcell.ColorDefault,
		Foreground: tcell.ColorDefault,
		Border:     tcell.ColorSteelBlue,
		Title:      tcell.ColorWhite,
		Good:       tcell.ColorDodgerBlue,
		Warn:       tcell.ColorOrange,
		Bad:        tcell.ColorGold, // deep amber, not red — stays distinct from Warn via shape markers too
		Neutral:    tcell.ColorGray,
		Selection:  tcell.ColorSteelBlue,
	},
	// High-contrast: maximal luminance separation for low-vision users, still
	// no red/green-only pair.
	"high-contrast": {
		Name:       "high-contrast",
		Background: tcell.ColorBlack,
		Foreground: tcell.ColorWhite,
		Border:     tcell.ColorWhite,
		Title:      tcell.ColorWhite,
		Good:       tcell.ColorAqua,
		Warn:       tcell.ColorYellow,
		Bad:        tcell.ColorWhite, // Bad is distinguished by the [!] marker under this theme
		Neutral:    tcell.ColorGray,
		Selection:  tcell.ColorWhite,
	},
	// Mono: no colour at all — text/symbol markers carry 100% of the meaning.
	// Used automatically when NO_COLOR is set or --theme=mono is passed.
	"mono": {
		Name:       "mono",
		Background: tcell.ColorDefault,
		Foreground: tcell.ColorDefault,
		Border:     tcell.ColorDefault,
		Title:      tcell.ColorDefault,
		Good:       tcell.ColorDefault,
		Warn:       tcell.ColorDefault,
		Bad:        tcell.ColorDefault,
		Neutral:    tcell.ColorDefault,
		Selection:  tcell.ColorDefault,
	},
}

func resolveTheme(name string) Theme {
	if _, off := os.LookupEnv("NO_COLOR"); off {
		return themes["mono"]
	}
	if t, ok := themes[name]; ok {
		return t
	}
	return themes["default"]
}

// applyTviewDefaults points tview's package-level style vars at the theme so
// every widget (tables, forms, modals) inherits it without per-widget styling.
func (t Theme) applyTviewDefaults() {
	tview.Styles.PrimitiveBackgroundColor = t.Background
	tview.Styles.ContrastBackgroundColor = t.Selection
	tview.Styles.PrimaryTextColor = t.Foreground
	tview.Styles.BorderColor = t.Border
	tview.Styles.TitleColor = t.Title
	tview.Styles.GraphicsColor = t.Border
}

// StateMarker returns a short text/symbol tag plus the semantic colour for a
// shard/job/agent state string. The tag is always printed — colour is a
// reinforcement, never the sole carrier of meaning.
func (t Theme) StateMarker(state string) (tag string, color tcell.Color) {
	switch state {
	case "DONE", "COMPLETE", "COMPLETED", "connected", "true":
		return "[OK]", t.Good
	case "QUEUED", "LEASED", "RUNNING", "SCANNING", "PROBING", "VERIFY", "DIRFIX", "PAUSED":
		return "[..]", t.Warn
	case "PARKED", "FAILED", "CANCELLED", "disconnected", "false":
		return "[!!]", t.Bad
	default:
		return "[--]", t.Neutral
	}
}
