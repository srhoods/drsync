package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/coder/websocket"
)

type client struct {
	server string
	token  string
	http   *http.Client
}

// connFlags registers --server/--token on fs; call the returned func after
// fs.Parse to build the client.
func connFlags(fs *flag.FlagSet) func() *client {
	server := fs.String("server", envOr("DRSYNC_SERVER", "http://127.0.0.1:7441"),
		"coordinator base URL")
	token := fs.String("token", os.Getenv("DRSYNC_TOKEN"), "API bearer token")
	return func() *client {
		return &client{server: strings.TrimRight(*server, "/"), token: *token,
			http: &http.Client{Timeout: 60 * time.Second}}
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// apiError carries the coordinator's {"error": ...} body plus the status code.
type apiError struct {
	Status int
	Msg    string
}

func (e *apiError) Error() string { return e.Msg }

func (c *client) do(method, path string, body []byte, out any) error {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, c.server+path, rd)
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		msg := strings.TrimSpace(string(data))
		if json.Unmarshal(data, &e) == nil && e.Error != "" {
			msg = e.Error
		}
		return &apiError{Status: resp.StatusCode,
			Msg: fmt.Sprintf("%s (HTTP %d)", msg, resp.StatusCode)}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

func (c *client) get(path string, out any) error { return c.do("GET", path, nil, out) }
func (c *client) del(path string, out any) error { return c.do("DELETE", path, nil, out) }
func (c *client) post(path string, body, out any) error {
	var data []byte
	if body != nil {
		var err error
		if data, err = json.Marshal(body); err != nil {
			return err
		}
	}
	return c.do("POST", path, data, out)
}

// dialEvents opens the /api/v1/events WebSocket.
func (c *client) dialEvents(ctx context.Context) (*websocket.Conn, error) {
	u, err := url.Parse(c.server)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}
	u.Path = "/api/v1/events"
	var hdr http.Header
	if c.token != "" {
		hdr = http.Header{"Authorization": {"Bearer " + c.token}}
	}
	conn, _, err := websocket.Dial(ctx, u.String(), &websocket.DialOptions{HTTPHeader: hdr})
	if err != nil {
		return nil, fmt.Errorf("events websocket: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	return conn, nil
}
