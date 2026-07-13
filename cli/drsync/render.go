package main

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"
)

func newTable() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func humanMS(ms int64) string {
	if ms <= 0 {
		return "-"
	}
	d := time.Duration(ms) * time.Millisecond
	if d < time.Minute {
		return d.Round(100 * time.Millisecond).String()
	}
	return d.Round(time.Second).String()
}

func msTime(ms int64) string {
	if ms == 0 {
		return "-"
	}
	return time.UnixMilli(ms).Format("15:04:05")
}

// ANSI colors, applied only when stdout is a terminal and NO_COLOR is unset
// (https://no-color.org). Codes are the "bright" variants: 91 red, 92 green,
// 93 yellow.
const (
	ansiRed    = "\033[91m"
	ansiGreen  = "\033[92m"
	ansiYellow = "\033[93m"
	ansiReset  = "\033[0m"
)

func colorEnabled() bool {
	if _, off := os.LookupEnv("NO_COLOR"); off {
		return false
	}
	fi, err := os.Stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// colorize wraps s in an ANSI color when the terminal supports it, else
// returns s unchanged. Callers pad s first so alignment is unaffected.
func colorize(on bool, code, s string) string {
	if !on || code == "" {
		return s
	}
	return code + s + ansiReset
}
