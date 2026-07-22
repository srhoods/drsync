package authn

import "testing"

// These SHA-512-crypt ($6$) and MD5-crypt ($1$) hashes were generated with
// `openssl passwd -6/-1 -salt test secret123` — they are test fixtures, not
// real credentials.
const (
	sha512Hash = "$6$test$ENkoqIAFIO89lhPZhBsS2HIq96G0SRdmDOrHETDXze1KjwLkqrdX4uYQv/TNhc4MQDZaBJbWDSKluwd4B4nXX0"
	md5Hash    = "$1$test$OfjwGtGFSDfP7pABquFXV."
)

func TestVerifyCryptSHA512(t *testing.T) {
	if !verifyCrypt(sha512Hash, "secret123") {
		t.Error("expected correct password to verify against sha512 hash")
	}
	if verifyCrypt(sha512Hash, "wrong") {
		t.Error("expected wrong password to fail verification")
	}
}

func TestVerifyCryptMD5(t *testing.T) {
	if !verifyCrypt(md5Hash, "secret123") {
		t.Error("expected correct password to verify against md5 hash")
	}
	if verifyCrypt(md5Hash, "wrong") {
		t.Error("expected wrong password to fail verification")
	}
}

func TestVerifyCryptUnsupportedScheme(t *testing.T) {
	// "$y$" is yescrypt, which this build does not register a scheme for;
	// NewFromHash would panic without the IsHashSupported guard.
	if verifyCrypt("$y$j9T$somesalt$somehash", "anything") {
		t.Error("expected unsupported scheme to fail closed, not verify")
	}
}

func TestVerifyCryptLockedAccount(t *testing.T) {
	for _, hash := range []string{"", "!", "!!", "*", "!$6$test$abc"} {
		if verifyCrypt(hash, "anything") {
			t.Errorf("hash %q: expected locked/empty hash to never verify", hash)
		}
	}
}
