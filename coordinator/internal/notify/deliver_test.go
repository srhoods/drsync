package notify

import (
	"bufio"
	"mime"
	"net"
	"net/mail"
	"strconv"
	"strings"
	"testing"
	"time"
)

// mockSMTP speaks just enough of SMTP (no TLS, no auth) to accept one message
// and hand its raw DATA back, so we can verify the real deliver() conversation.
func mockSMTP(t *testing.T) (host string, port int, got chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	got = make(chan string, 1)
	go func() {
		defer ln.Close()
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		r := bufio.NewReader(conn)
		w := bufio.NewWriter(conn)
		write := func(s string) { w.WriteString(s + "\r\n"); w.Flush() }

		write("220 mock ESMTP")
		var body strings.Builder
		inData := false
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			if inData {
				if line == ".\r\n" {
					inData = false
					write("250 OK queued")
					continue
				}
				body.WriteString(line)
				continue
			}
			cmd := strings.ToUpper(strings.TrimSpace(line))
			switch {
			case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
				write("250 mock") // advertise no extensions (no STARTTLS/AUTH)
			case strings.HasPrefix(cmd, "MAIL FROM"), strings.HasPrefix(cmd, "RCPT TO"):
				write("250 OK")
			case strings.HasPrefix(cmd, "DATA"):
				write("354 End data with <CR><LF>.<CR><LF>")
				inData = true
			case strings.HasPrefix(cmd, "QUIT"):
				write("221 Bye")
				got <- body.String()
				return
			default:
				write("250 OK")
			}
		}
	}()
	h, p, _ := net.SplitHostPort(ln.Addr().String())
	port, _ = strconv.Atoi(p)
	return h, port, got
}

func TestDeliverEndToEnd(t *testing.T) {
	host, port, got := mockSMTP(t)
	s := &Sender{cfg: &Config{
		Host: host, Port: port, Security: "none",
		From: "drsync <drsync@example.com>", TimeoutSeconds: 5,
	}}
	// send() is synchronous (sendAsync wraps it in a goroutine); call it directly.
	// The subject carries an em dash to exercise RFC 2047 header encoding.
	const subject = "[drsync] job x — pass 1 complete (ok)"
	if err := s.send([]string{"ops@example.com", "lead@example.com"},
		subject, "<b>hi</b>", "hi"); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case raw := <-got:
		m, err := mail.ReadMessage(strings.NewReader(raw))
		if err != nil {
			t.Fatalf("parse delivered message: %v\n---\n%s", err, raw)
		}
		// Subject round-trips through Q-encoding back to the original (non-ASCII).
		gotSubj, err := (&mime.WordDecoder{}).DecodeHeader(m.Header.Get("Subject"))
		if err != nil {
			t.Fatal(err)
		}
		if gotSubj != subject {
			t.Errorf("subject = %q, want %q", gotSubj, subject)
		}
		if to := m.Header.Get("To"); to != "ops@example.com, lead@example.com" {
			t.Errorf("To = %q", to)
		}
		if ct := m.Header.Get("Content-Type"); !strings.HasPrefix(ct, "multipart/alternative") {
			t.Errorf("Content-Type = %q", ct)
		}
		for _, part := range []string{"text/plain", "text/html", "<b>hi</b>"} {
			if !strings.Contains(raw, part) {
				t.Errorf("delivered body missing %q", part)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for delivered message")
	}
}
