package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTokenFile writes contents then explicitly chmods to mode, so the
// resulting permissions are exact regardless of the test process's umask
// (os.WriteFile's mode argument is itself subject to umask).
func writeTokenFile(t *testing.T, contents string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "api-token")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadAPITokenHappyPath(t *testing.T) {
	path := writeTokenFile(t, "s3cr3t-token\n", 0o600)
	tok, err := loadAPIToken(path, false)
	if err != nil {
		t.Fatalf("loadAPIToken: %v", err)
	}
	if tok != "s3cr3t-token" {
		t.Errorf("token = %q, want %q (trailing newline should be trimmed)", tok, "s3cr3t-token")
	}
}

func TestLoadAPITokenTrimsWhitespace(t *testing.T) {
	path := writeTokenFile(t, "  s3cr3t-token  \n\n", 0o600)
	tok, err := loadAPIToken(path, false)
	if err != nil {
		t.Fatalf("loadAPIToken: %v", err)
	}
	if tok != "s3cr3t-token" {
		t.Errorf("token = %q, want trimmed %q", tok, "s3cr3t-token")
	}
}

func TestLoadAPITokenMissingDefaultOK(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nonexistent-token")
	tok, err := loadAPIToken(missing, true)
	if err != nil {
		t.Fatalf("expected no error for missing default file, got %v", err)
	}
	if tok != "" {
		t.Errorf("expected empty token for absent default file, got %q", tok)
	}
}

func TestLoadAPITokenMissingExplicitIsError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nonexistent-token")
	if _, err := loadAPIToken(missing, false); err == nil {
		t.Fatal("expected error for missing explicitly-configured token file")
	}
}

func TestLoadAPITokenEmptyFileIsError(t *testing.T) {
	path := writeTokenFile(t, "", 0o600)
	if _, err := loadAPIToken(path, false); err == nil {
		t.Fatal("expected error for empty token file")
	}
}

func TestLoadAPITokenWhitespaceOnlyIsError(t *testing.T) {
	path := writeTokenFile(t, "   \n\t\n", 0o600)
	if _, err := loadAPIToken(path, false); err == nil {
		t.Fatal("expected error for whitespace-only token file")
	}
}

// The core security property: a token file readable by anyone other than
// its owner must refuse to load, not silently trust the filesystem.
func TestLoadAPITokenRejectsGroupReadable(t *testing.T) {
	path := writeTokenFile(t, "secret\n", 0o640)
	_, err := loadAPIToken(path, false)
	if err == nil {
		t.Fatal("expected error for group-readable (0640) token file")
	}
	if !strings.Contains(err.Error(), "must not be group- or world-readable") {
		t.Errorf("error should explain the permission problem: %v", err)
	}
}

func TestLoadAPITokenRejectsWorldReadable(t *testing.T) {
	path := writeTokenFile(t, "secret\n", 0o644)
	if _, err := loadAPIToken(path, false); err == nil {
		t.Fatal("expected error for world-readable (0644) token file")
	}
}

func TestLoadAPITokenRejectsWorldWritable(t *testing.T) {
	// 0602: owner rw, world w (but not r) — still covered by the 0o077 mask
	// and just as dangerous (anyone can overwrite the coordinator's token).
	path := writeTokenFile(t, "secret\n", 0o602)
	if _, err := loadAPIToken(path, false); err == nil {
		t.Fatal("expected error for world-writable (0602) token file")
	}
}

func TestLoadAPITokenAcceptsOwnerReadOnly(t *testing.T) {
	// 0400 (owner read-only, no write) is stricter than the required 0600
	// and must still be accepted — the check is "no group/world bits", not
	// "exactly 0600".
	path := writeTokenFile(t, "secret\n", 0o400)
	tok, err := loadAPIToken(path, false)
	if err != nil {
		t.Fatalf("loadAPIToken: %v", err)
	}
	if tok != "secret" {
		t.Errorf("token = %q", tok)
	}
}
