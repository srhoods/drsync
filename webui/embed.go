// Package webui embeds the read-only monitoring console the coordinator serves
// at GET / (and /ui). The page is a self-contained HTML file that talks to the
// coordinator's REST API, /metrics, and the /api/v1/events WebSocket.
package webui

import _ "embed"

//go:embed console.html
var Console []byte
