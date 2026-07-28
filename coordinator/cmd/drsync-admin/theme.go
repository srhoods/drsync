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
//
// Every field here must be an explicit, opaque colour — never
// tcell.ColorDefault. ColorDefault means "leave whatever was drawn before,"
// not "no colour": using it for a semantic accent doesn't produce a neutral
// look, it lets stale colour from a previous draw bleed through, which is
// exactly the "mono still shows colour" bug this comment is here to prevent
// regressing.
type Theme struct {
	Name string

	Background tcell.Color
	Foreground tcell.Color
	Border     tcell.Color
	Title      tcell.Color
	Label      tcell.Color // form field labels (tview's "secondary text")
	Subtle     tcell.Color // list secondary lines, e.g. "N rows" (tview's "tertiary text")

	// Semantic accents, deliberately not a red/green pair: blue/orange/yellow
	// remain distinguishable to protanopes, deuteranopes and tritanopes alike.
	Good    tcell.Color // e.g. DONE, connected, enabled
	Warn    tcell.Color // e.g. QUEUED/LEASED, in-progress
	Bad     tcell.Color // e.g. PARKED, FAILED, disconnected
	Neutral tcell.Color // e.g. CREATED/PENDING

	// ListSelection is the highlighted-row background in tables/lists;
	// SelectedText is the text colour used against it (must contrast with
	// ListSelection specifically, not with Background — a selected row is
	// its own contrast pair).
	ListSelection tcell.Color
	SelectedText  tcell.Color

	// Editable form fields need their own background/text pair, independent
	// of ListSelection: they are a different UI concept (an editable input,
	// not a highlighted row) and reusing one colour for both is what
	// previously produced white-on-white (high-contrast) and yellow labels
	// next to an unreadable value.
	FieldBackground tcell.Color
	FieldText       tcell.Color
}

var themes = map[string]Theme{
	// Default: blue/orange/amber triad on the terminal's own black-on-white
	// (or white-on-black) background, high separation for all common CVD
	// types; avoids relying on red vs green.
	"default": {
		Name:            "default",
		Background:      tcell.ColorDefault,
		Foreground:      tcell.ColorDefault,
		Border:          tcell.ColorSteelBlue,
		Title:           tcell.ColorSteelBlue,
		Label:           tcell.ColorSteelBlue,
		Subtle:          tcell.ColorGray,
		Good:            tcell.ColorDodgerBlue,
		Warn:            tcell.ColorDarkOrange,
		Bad:             tcell.ColorGoldenrod, // deep amber, not red — stays distinct from Warn via shape markers too
		Neutral:         tcell.ColorGray,
		ListSelection:   tcell.ColorSteelBlue,
		SelectedText:    tcell.ColorWhite,
		FieldBackground: tcell.ColorSteelBlue,
		FieldText:       tcell.ColorWhite,
	},
	// High-contrast: black background, maximal luminance separation for
	// low-vision users, still no red/green-only pair. Editable fields get
	// their own white-on-blue pairing so they never collide with the
	// white-on-white row-selection highlight.
	"high-contrast": {
		Name:            "high-contrast",
		Background:      tcell.ColorBlack,
		Foreground:      tcell.ColorWhite,
		Border:          tcell.ColorWhite,
		Title:           tcell.ColorWhite,
		Label:           tcell.ColorWhite,
		Subtle:          tcell.ColorGray,
		Good:            tcell.ColorAqua,
		Warn:            tcell.ColorYellow,
		Bad:             tcell.ColorWhite, // Bad is distinguished by the [!!] marker under this theme
		Neutral:         tcell.ColorGray,
		ListSelection:   tcell.ColorWhite,
		SelectedText:    tcell.ColorBlack,
		FieldBackground: tcell.ColorNavy,
		FieldText:       tcell.ColorWhite,
	},
	// Mono: real black-and-white/greyscale only — every colour value here is
	// explicit (never ColorDefault), so nothing bleeds through and nothing
	// depends on the terminal's own palette. Text/symbol markers carry 100%
	// of the semantic meaning. Used automatically when NO_COLOR is set or
	// --theme=mono is passed.
	"mono": {
		Name:            "mono",
		Background:      tcell.ColorBlack,
		Foreground:      tcell.ColorWhite,
		Border:          tcell.ColorWhite,
		Title:           tcell.ColorWhite,
		Label:           tcell.ColorWhite,
		Subtle:          tcell.ColorWhite,
		Good:            tcell.ColorWhite,
		Warn:            tcell.ColorWhite,
		Bad:             tcell.ColorWhite,
		Neutral:         tcell.ColorWhite,
		ListSelection:   tcell.ColorWhite,
		SelectedText:    tcell.ColorBlack,
		FieldBackground: tcell.ColorWhite,
		FieldText:       tcell.ColorBlack,
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
// every widget (tables, forms, modals) inherits it without per-widget
// styling. FieldBackground/FieldText map to tview's ContrastBackgroundColor/
// PrimaryTextColor specifically because that is the pair InputField actually
// reads for its editable area (see rivo/tview inputfield.go) — ListSelection
// is applied separately, per-widget, via SetSelectedStyle, since tview has
// no single global var for "highlighted list row" independent of that pair.
func (t Theme) applyTviewDefaults() {
	tview.Styles.PrimitiveBackgroundColor = t.Background
	tview.Styles.ContrastBackgroundColor = t.FieldBackground
	tview.Styles.PrimaryTextColor = t.FieldText
	tview.Styles.SecondaryTextColor = t.Label
	tview.Styles.TertiaryTextColor = t.Subtle
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
