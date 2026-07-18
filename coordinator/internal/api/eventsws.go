// GET /api/v1/events — the WebSocket event feed (DESIGN-coordinator §6).
// Emits the events.Bus stream as JSON text frames. Consumers: `drsync events`,
// `drsync job status --watch`, and the WebUI console.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"drsync/coordinator/internal/events"
)

const wsWriteTimeout = 10 * time.Second

func (s *Server) eventsWS(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Token auth already ran in s.auth; the API is not cookie-authed, so
		// cross-origin WS adds no ambient authority to leak.
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		slog.Debug("websocket accept failed", "err", err)
		return
	}
	defer c.CloseNow()

	// Write-only stream: CloseRead pumps control frames (ping/pong/close) and
	// cancels the context when the client goes away.
	ctx := c.CloseRead(r.Context())

	ch, cancel := s.bus.Subscribe()
	defer cancel()

	hello := events.Event{Type: "hello", TsMs: time.Now().UnixMilli(),
		Data: map[string]any{"proto": 1}}
	if err := writeEvent(ctx, c, hello); err != nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			c.Close(websocket.StatusNormalClosure, "")
			return
		case ev := <-ch:
			if err := writeEvent(ctx, c, ev); err != nil {
				return
			}
		}
	}
}

func writeEvent(ctx context.Context, c *websocket.Conn, ev events.Event) error {
	wctx, cancel := context.WithTimeout(ctx, wsWriteTimeout)
	defer cancel()
	return wsjson.Write(wctx, c, ev)
}
