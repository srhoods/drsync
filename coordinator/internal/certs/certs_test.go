package certs

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigMissingFile(t *testing.T) {
	c, err := LoadConfig(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	if err != nil {
		t.Fatalf("expected no error for missing file, got %v", err)
	}
	if c != nil {
		t.Errorf("expected nil config, got %+v", c)
	}
}

func TestLoadConfigHalfConfigured(t *testing.T) {
	path := filepath.Join(t.TempDir(), "certs.yaml")
	if err := os.WriteFile(path, []byte("cert_file: /a/b.crt\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Error("expected error when only cert_file is set")
	}
}

func TestLoadConfigBothSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "certs.yaml")
	if err := os.WriteFile(path, []byte("cert_file: /a/b.crt\nkey_file: /a/b.key\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if c.CertFile != "/a/b.crt" || c.KeyFile != "/a/b.key" {
		t.Errorf("got %+v", c)
	}
}

// writeTestKeyPair generates a throwaway self-signed cert/key pair directly
// (not via the CLI's generator, which this package cannot import) purely to
// exercise TLSConfig's file loading.
func writeTestKeyPair(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath = filepath.Join(dir, "server.crt")
	keyPath = filepath.Join(dir, "server.key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

func TestConfigTLSConfig(t *testing.T) {
	certPath, keyPath := writeTestKeyPair(t)
	c := &Config{CertFile: certPath, KeyFile: keyPath}
	tlsCfg, err := c.TLSConfig()
	if err != nil {
		t.Fatalf("TLSConfig: %v", err)
	}
	if len(tlsCfg.Certificates) != 1 {
		t.Errorf("expected 1 certificate, got %d", len(tlsCfg.Certificates))
	}
}

func TestNilConfigTLSConfig(t *testing.T) {
	var c *Config
	tlsCfg, err := c.TLSConfig()
	if err != nil || tlsCfg != nil {
		t.Errorf("nil config should yield (nil, nil), got (%v, %v)", tlsCfg, err)
	}
}
