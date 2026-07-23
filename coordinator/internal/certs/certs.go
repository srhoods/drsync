// Package certs loads the coordinator's HTTP(S) listener TLS configuration
// from a YAML config (conventionally /etc/drsync/certs.yaml). This is
// unrelated to the agent<->coordinator mTLS material (drsync ca / -tls-cert
// on the agent listener) — it is the cert the WebUI's and API's browser/CLI
// clients see. When no cert/key pair is configured, the HTTP API listener
// falls back to plain http://.
//
// Self-signed cert *generation* for dev/test bootstrapping lives in the CLI
// (`drsync cert generate-self-signed`, cli/drsync/cert.go), not here — this
// package is coordinator-internal and cannot be imported by cli/drsync.
package certs

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"os"

	"gopkg.in/yaml.v3"
)

// Config points at the PEM cert/key pair for the coordinator's HTTP(S)
// listener. Both fields empty (or the file absent) means "no TLS" — the
// caller should serve plain HTTP.
type Config struct {
	// CertFile is the PEM certificate (leaf, optionally followed by any
	// intermediates) presented to clients.
	CertFile string `yaml:"cert_file,omitempty"`
	// KeyFile is the PEM private key matching CertFile.
	KeyFile string `yaml:"key_file,omitempty"`
}

// LoadConfig reads the certs config at path. A missing file is never an
// error — it simply means TLS is not configured — matching the "default to
// http://" requirement. An explicitly present file with only one of
// cert_file/key_file set is a hard error (half-configured TLS is almost
// certainly a mistake, not intentional plaintext).
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse certs config %s: %w", path, err)
	}
	if c.CertFile == "" && c.KeyFile == "" {
		return nil, nil
	}
	if c.CertFile == "" || c.KeyFile == "" {
		return nil, fmt.Errorf("certs config %s: both cert_file and key_file are required together", path)
	}
	return &c, nil
}

// TLSConfig builds a *tls.Config serving c's cert/key pair, or returns nil
// if c is nil (no TLS configured).
func (c *Config) TLSConfig() (*tls.Config, error) {
	if c == nil {
		return nil, nil
	}
	pair, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load HTTP TLS key pair: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{pair},
		MinVersion:   tls.VersionTLS12,
	}, nil
}
