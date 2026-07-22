package authn

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/user"
	"strings"

	"github.com/GehirnInc/crypt"
	_ "github.com/GehirnInc/crypt/md5_crypt"
	_ "github.com/GehirnInc/crypt/sha256_crypt"
	_ "github.com/GehirnInc/crypt/sha512_crypt"
)

// errInvalidCredential is returned for every local-auth failure mode (unknown
// user, locked account, wrong password, unsupported hash) so callers can't
// distinguish "no such user" from "wrong password" and leak account
// existence to an unauthenticated client.
var errInvalidCredential = errors.New("invalid username or password")

// LocalAuthenticator authenticates against the underlying host's user
// accounts by checking the password hash in /etc/shadow. The drsyncd process
// needs read access to /etc/shadow (typically: run as root, or add the
// service account to the shadow group).
type LocalAuthenticator struct{}

func (a *LocalAuthenticator) Authenticate(username, password string) (*User, error) {
	if username == "" || password == "" {
		return nil, errInvalidCredential
	}
	hash, err := shadowHash(username)
	if err != nil {
		return nil, errInvalidCredential
	}
	if !verifyCrypt(hash, password) {
		return nil, errInvalidCredential
	}
	groups, err := localGroups(username)
	if err != nil {
		return nil, fmt.Errorf("resolve groups for %s: %w", username, err)
	}
	return &User{Username: username, Groups: groups}, nil
}

// shadowHash returns the crypt(3) hash field for username from /etc/shadow.
// A locked/disabled account (hash starting with "!" or "*", or empty) is
// treated as not found.
func shadowHash(username string) (string, error) {
	f, err := os.Open("/etc/shadow")
	if err != nil {
		return "", err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		fields := strings.SplitN(line, ":", 3)
		if len(fields) < 2 || fields[0] != username {
			continue
		}
		hash := fields[1]
		if hash == "" || hash == "!" || strings.HasPrefix(hash, "!") || strings.HasPrefix(hash, "*") {
			return "", errInvalidCredential
		}
		return hash, nil
	}
	if err := sc.Err(); err != nil {
		return "", err
	}
	return "", errInvalidCredential
}

// verifyCrypt checks password against a glibc crypt(3) hash ($1$ MD5, $5$
// SHA-256, $6$ SHA-512 — the formats modern Linux shadow files use). Each
// scheme's Verify does a constant-time comparison internally.
func verifyCrypt(hash, password string) bool {
	// NewFromHash panics on an unrecognized prefix (e.g. "$y$" yescrypt or
	// DES crypt, neither of which this package registers a scheme for);
	// guard with IsHashSupported first rather than let one account's hash
	// format take down a login request.
	if !crypt.IsHashSupported(hash) {
		return false
	}
	return crypt.NewFromHash(hash).Verify(hash, []byte(password)) == nil
}

// localGroups returns the names of every group username belongs to
// (primary + supplementary), via the host's NSS-backed os/user lookups.
func localGroups(username string) ([]string, error) {
	u, err := user.Lookup(username)
	if err != nil {
		return nil, err
	}
	gids, err := u.GroupIds()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(gids))
	for _, gid := range gids {
		g, err := user.LookupGroupId(gid)
		if err != nil {
			continue // stale/unresolvable gid; skip rather than fail the login
		}
		names = append(names, g.Name)
	}
	return names, nil
}
