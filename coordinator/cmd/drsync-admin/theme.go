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
//
// Every colour is built via tcell.PaletteColor(n) — a fixed index into the
// 256-colour palette — never a named constant (tcell.ColorSteelBlue etc.) or
// an arbitrary RGB triple. Named/RGB colours only render as intended when
// the terminal advertises true-colour support via its terminfo entry; under
// TERM=screen-256color or tmux-256color (every tmux/screen session, on this
// host, regardless of what the real terminal underneath supports) tcell
// quantizes them to the nearest of 256 palette entries, and different
// terminals/multiplexers make that approximation differently — which is
// exactly the "colours get overwritten by tmux or other screen managers"
// complaint this rewrite exists to close. Indices 16-231 (the 6x6x6 colour
// cube, steps 0/95/135/175/215/255 per channel) and 232-255 (the greyscale
// ramp) are numeric palette slots with no quantization step at all: every
// terminal that supports 256 colours renders index 25 as the same colour,
// full stop. See paletteColor()'s doc comment for the exact index derivation.
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

// paletteColor builds a colour from a fixed 256-colour palette index, never
// from a named constant or an RGB triple — see the Theme doc comment for why.
func paletteColor(index int) tcell.Color { return tcell.PaletteColor(index) }

// The 256-colour cube/greyscale indices behind each named value below,
// derived from the Okabe–Ito colour-blind-safe palette snapped to the
// nearest exact 6x6x6-cube grid point — see the palette-preview artifact
// reviewed before this landed. Named here once so the two colour themes
// below both draw from the same verified set rather than picking indices
// ad hoc.
const (
	idxBlue      = 25  // #005faf — Okabe–Ito "blue", snapped
	idxSkyBlue   = 74  // #5fafd7 — Okabe–Ito "sky blue", snapped; selection bg
	idxBluGreen  = 35  // #00af5f — Okabe–Ito "bluish green"; alt good
	idxOrange    = 178 // #d7af00 — Okabe–Ito "orange"; warn
	idxVermilion = 166 // #d75f00 — Okabe–Ito "vermillion"; bad (not red)
	idxGreyDim   = 244 // #808080 — grey ramp; subtle/secondary text
	idxGreyLabel = 250 // #bcbcbc — grey ramp; form labels
	idxWhite     = 231 // #ffffff — cube corner, not the ANSI "white" name
	idxBlack     = 16  // #000000 — cube corner, not the ANSI "black" name
)

var themes = map[string]Theme{
	// Default: blue/orange/vermillion triad on a fixed dark ground (not the
	// terminal's own background — see the Theme doc comment), high
	// separation for all common CVD types, avoids relying on red vs green.
	"default": {
		Name:            "default",
		Background:      paletteColor(idxBlack),
		Foreground:      paletteColor(idxGreyLabel),
		Border:          paletteColor(idxBlue),
		Title:           paletteColor(idxWhite),
		Label:           paletteColor(idxGreyLabel),
		Subtle:          paletteColor(idxGreyDim),
		Good:            paletteColor(idxBluGreen),
		Warn:            paletteColor(idxOrange),
		Bad:             paletteColor(idxVermilion),
		Neutral:         paletteColor(idxGreyDim),
		ListSelection:   paletteColor(idxSkyBlue),
		SelectedText:    paletteColor(idxBlack),
		FieldBackground: paletteColor(idxBlue),
		FieldText:       paletteColor(idxWhite),
	},
	// High-contrast: same fixed dark ground, maximal luminance separation
	// for low-vision users, still no red/green-only pair. Editable fields
	// keep their own background/text pairing so they never collide with the
	// selection highlight the way an overloaded single colour once did.
	"high-contrast": {
		Name:            "high-contrast",
		Background:      paletteColor(idxBlack),
		Foreground:      paletteColor(idxWhite),
		Border:          paletteColor(idxWhite),
		Title:           paletteColor(idxWhite),
		Label:           paletteColor(idxWhite),
		Subtle:          paletteColor(idxGreyDim),
		Good:            paletteColor(idxBluGreen),
		Warn:            paletteColor(idxOrange),
		Bad:             paletteColor(idxWhite), // distinguished by the [!!] marker under this theme
		Neutral:         paletteColor(idxGreyDim),
		ListSelection:   paletteColor(idxWhite),
		SelectedText:    paletteColor(idxBlack),
		FieldBackground: paletteColor(idxBlue),
		FieldText:       paletteColor(idxWhite),
	},
	// Mono: real black-and-white/greyscale only — every colour value here is
	// explicit (never ColorDefault), so nothing bleeds through and nothing
	// depends on the terminal's own palette. Text/symbol markers carry 100%
	// of the semantic meaning. Used automatically when NO_COLOR is set or
	// --theme=mono is passed.
	"mono": {
		Name:            "mono",
		Background:      paletteColor(idxBlack),
		Foreground:      paletteColor(idxWhite),
		Border:          paletteColor(idxWhite),
		Title:           paletteColor(idxWhite),
		Label:           paletteColor(idxWhite),
		Subtle:          paletteColor(idxWhite),
		Good:            paletteColor(idxWhite),
		Warn:            paletteColor(idxWhite),
		Bad:             paletteColor(idxWhite),
		Neutral:         paletteColor(idxWhite),
		ListSelection:   paletteColor(idxWhite),
		SelectedText:    paletteColor(idxBlack),
		FieldBackground: paletteColor(idxWhite),
		FieldText:       paletteColor(idxBlack),
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
//
// These tags start with '[', which tview's TableCell tag parser also treats
// as the start of a style/region tag (no per-cell opt-out) — the checkbox
// column in tables.go hit exactly this, rendering "[ ]"/"[x]" as blank
// instead of a checkbox once tview's parser swallowed them. "[OK]"/"[..]"/
// "[!!]"/"[--]" render correctly today (verified live in the running app),
// but that depends on their exact contents failing to parse as a valid tag —
// it is not a property of "starts with '[' and is short" in general. Prefer
// a non-'[' marker (as tables.go's checkbox now does) for anything new,
// rather than adding another tag here and re-verifying by hand.
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
