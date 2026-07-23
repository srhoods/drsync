// Package notify delivers best-effort email notifications for job and pass
// lifecycle events. SMTP server settings are loaded from a YAML config
// (conventionally /etc/drsync/smtp.yaml); per-job recipients and triggers come
// from the job spec (model.NotificationSpec).
//
// Delivery is always asynchronous and best-effort: a Sender method returns
// immediately and any transport error is logged, never surfaced to the caller.
// Notifications therefore cannot slow down or fail a migration — the worst case
// is a missing email, which the coordinator log records.
package notify

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the SMTP server configuration, loaded from /etc/drsync/smtp.yaml.
type Config struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
	// From is the envelope/header sender, e.g. "drsync <drsync@example.com>".
	From string `yaml:"from"`
	// Security selects transport security: "starttls" (default), "tls"
	// (implicit TLS, typically port 465) or "none" (plaintext, dev only).
	Security string `yaml:"security,omitempty"`
	// HELO is the EHLO/HELO hostname; defaults to the coordinator's hostname.
	HELO string `yaml:"helo,omitempty"`
	// SubjectPrefix is prepended to every subject line; defaults to "[drsync]".
	SubjectPrefix string `yaml:"subject_prefix,omitempty"`
	// TimeoutSeconds bounds the whole SMTP exchange; defaults to 30.
	TimeoutSeconds int `yaml:"timeout_seconds,omitempty"`
}

// LoadConfig reads, defaults and validates the SMTP config at path. When
// missingOK is true an absent file yields (nil, nil) so a deployment that does
// not want email need not create one; an explicitly-configured path that is
// missing is an error.
func LoadConfig(path string, missingOK bool) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if missingOK && errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // typo safety, matching the job-spec decoder
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse smtp config %s: %w", path, err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("smtp config %s: %w", path, err)
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Security == "" {
		c.Security = "starttls"
	}
	if c.Port == 0 {
		switch c.Security {
		case "tls":
			c.Port = 465
		case "none":
			c.Port = 25
		default:
			c.Port = 587
		}
	}
	if c.SubjectPrefix == "" {
		c.SubjectPrefix = "[drsync]"
	}
	if c.TimeoutSeconds == 0 {
		c.TimeoutSeconds = 30
	}
	if c.HELO == "" {
		if h, err := os.Hostname(); err == nil {
			c.HELO = h
		} else {
			c.HELO = "localhost"
		}
	}
}

func (c *Config) validate() error {
	if c.Host == "" {
		return errors.New("host is required")
	}
	if c.From == "" {
		return errors.New("from is required")
	}
	if _, err := mail.ParseAddress(c.From); err != nil {
		return fmt.Errorf("from %q is not a valid address: %w", c.From, err)
	}
	switch c.Security {
	case "starttls", "tls", "none":
	default:
		return fmt.Errorf("security must be starttls|tls|none, got %q", c.Security)
	}
	return nil
}

// Sender delivers rendered emails over SMTP. A nil *Sender is valid and inert
// (every method is a no-op), so callers can hold a nil notifier when email is
// disabled without branching at every call site.
type Sender struct {
	cfg *Config
}

// NewSender returns a Sender for cfg, or nil when cfg is nil.
func NewSender(cfg *Config) *Sender {
	if cfg == nil {
		return nil
	}
	return &Sender{cfg: cfg}
}

// Enabled reports whether s can actually send (non-nil and configured).
func (s *Sender) Enabled() bool { return s != nil && s.cfg != nil }

// PassComplete emails a per-pass report to recipients. Best-effort and async.
func (s *Sender) PassComplete(recipients []string, r PassReport) {
	if !s.Enabled() || len(recipients) == 0 {
		return
	}
	subject, htmlBody, textBody := renderPass(r)
	s.sendAsync(recipients, subject, htmlBody, textBody)
}

// JobComplete emails the end-of-job summary to recipients. Best-effort/async.
func (s *Sender) JobComplete(recipients []string, r JobReport) {
	if !s.Enabled() || len(recipients) == 0 {
		return
	}
	subject, htmlBody, textBody := renderJob(r)
	s.sendAsync(recipients, subject, htmlBody, textBody)
}

// ParkedShards emails a dedicated alert to recipients when a job run ends
// with shards still parked. Best-effort/async, and independent of
// on_job_complete — a parked shard is an operator action item, not routine
// completion reporting, so it is not gated behind that flag.
func (s *Sender) ParkedShards(recipients []string, r ParkedShardsReport) {
	if !s.Enabled() || len(recipients) == 0 || len(r.Shards) == 0 {
		return
	}
	subject, htmlBody, textBody := renderParkedShards(r)
	s.sendAsync(recipients, subject, htmlBody, textBody)
}

func (s *Sender) sendAsync(to []string, subject, htmlBody, textBody string) {
	full := subject
	if s.cfg.SubjectPrefix != "" {
		full = s.cfg.SubjectPrefix + " " + subject
	}
	go func() {
		if err := s.send(to, full, htmlBody, textBody); err != nil {
			slog.Error("notify: email send failed",
				"to", strings.Join(to, ","), "subject", full, "err", err)
			return
		}
		slog.Info("notify: email sent", "to", strings.Join(to, ","), "subject", full)
	}()
}

func (s *Sender) send(to []string, subject, htmlBody, textBody string) error {
	msg, err := buildMIME(s.cfg.From, to, subject, textBody, htmlBody)
	if err != nil {
		return err
	}
	return s.deliver(to, msg)
}

// deliver performs one SMTP transaction, honouring the configured transport
// security and optional PLAIN auth. A single deadline bounds the whole exchange.
func (s *Sender) deliver(to []string, msg []byte) error {
	c := s.cfg
	addr := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	timeout := time.Duration(c.TimeoutSeconds) * time.Second
	dialer := &net.Dialer{Timeout: timeout}

	var conn net.Conn
	var err error
	if c.Security == "tls" {
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{ServerName: c.Host})
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	// One deadline for the whole conversation; SMTP here is a short exchange.
	_ = conn.SetDeadline(time.Now().Add(timeout))

	client, err := smtp.NewClient(conn, c.Host)
	if err != nil {
		conn.Close()
		return err
	}
	defer client.Close()

	if err := client.Hello(c.HELO); err != nil {
		return fmt.Errorf("HELO: %w", err)
	}
	if c.Security == "starttls" {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return errors.New("server does not advertise STARTTLS")
		}
		if err := client.StartTLS(&tls.Config{ServerName: c.Host}); err != nil {
			return fmt.Errorf("STARTTLS: %w", err)
		}
	}
	if c.Username != "" {
		if err := client.Auth(smtp.PlainAuth("", c.Username, c.Password, c.Host)); err != nil {
			return fmt.Errorf("auth: %w", err)
		}
	}
	from, err := mail.ParseAddress(c.From)
	if err != nil {
		return err
	}
	if err := client.Mail(from.Address); err != nil {
		return fmt.Errorf("MAIL FROM: %w", err)
	}
	for _, rcpt := range to {
		if err := client.Rcpt(rcpt); err != nil {
			return fmt.Errorf("RCPT %s: %w", rcpt, err)
		}
	}
	w, err := client.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(msg); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return client.Quit()
}

// buildMIME assembles a multipart/alternative message (plain + HTML) with
// quoted-printable bodies and a proper header block. Exported-ish shape kept
// simple so render_test can inspect it without an SMTP server.
func buildMIME(from string, to []string, subject, text, htmlBody string) ([]byte, error) {
	boundary := "drsync_" + randToken()
	var b bytes.Buffer
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", strings.Join(to, ", "))
	fmt.Fprintf(&b, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", subject))
	fmt.Fprintf(&b, "Date: %s\r\n", time.Now().Format(time.RFC1123Z))
	fmt.Fprintf(&b, "Message-ID: <%s@drsync>\r\n", randToken())
	b.WriteString("MIME-Version: 1.0\r\n")
	fmt.Fprintf(&b, "Content-Type: multipart/alternative; boundary=\"%s\"\r\n\r\n", boundary)

	if err := writePart(&b, boundary, "text/plain; charset=UTF-8", text); err != nil {
		return nil, err
	}
	if err := writePart(&b, boundary, "text/html; charset=UTF-8", htmlBody); err != nil {
		return nil, err
	}
	fmt.Fprintf(&b, "--%s--\r\n", boundary)
	return b.Bytes(), nil
}

func writePart(b *bytes.Buffer, boundary, contentType, body string) error {
	fmt.Fprintf(b, "--%s\r\n", boundary)
	fmt.Fprintf(b, "Content-Type: %s\r\n", contentType)
	b.WriteString("Content-Transfer-Encoding: quoted-printable\r\n\r\n")
	qp := quotedprintable.NewWriter(b)
	if _, err := qp.Write([]byte(body)); err != nil {
		return err
	}
	if err := qp.Close(); err != nil {
		return err
	}
	b.WriteString("\r\n")
	return nil
}

func randToken() string {
	var buf [12]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}
