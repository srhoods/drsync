package main

// `drsync ca` — the mTLS trust root for the fleet. The coordinator verifies
// agent client certs against this CA (RequireAndVerifyClientCert) and presents
// a server cert issued by it; agents verify the coordinator the same way. Keys
// are ECDSA P-256 (TLS 1.3, and interoperable with the C agent's OpenSSL).
//
//	drsync ca init  [--dir DIR] [--cn NAME] [--days N] [--force]
//	drsync ca issue --type server|agent --cn NAME [--dir DIR]
//	               [--dns HOST]... [--ip ADDR]... [--out PREFIX] [--days N] [--force]

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func cmdCA(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("ca needs a subcommand: init | issue")
	}
	switch args[0] {
	case "init":
		return caInit(args[1:])
	case "issue":
		return caIssue(args[1:])
	default:
		return fmt.Errorf("unknown ca subcommand %q (want init|issue)", args[0])
	}
}

// serialNumber returns a random 128-bit positive serial.
func serialNumber() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}

// writePEM writes DER blocks as a PEM file. keyMode true → 0600 (private key).
func writePEM(path string, blocks []*pem.Block, keyMode, force bool) error {
	mode := os.FileMode(0644)
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if keyMode {
		mode = 0600
	}
	if !force {
		flags |= os.O_EXCL // refuse to clobber existing key material
	}
	f, err := os.OpenFile(path, flags, mode)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("%s already exists (use --force to overwrite)", path)
		}
		return err
	}
	defer f.Close()
	for _, b := range blocks {
		if err := pem.Encode(f, b); err != nil {
			return err
		}
	}
	return nil
}

func marshalKey(key *ecdsa.PrivateKey) (*pem.Block, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	return &pem.Block{Type: "PRIVATE KEY", Bytes: der}, nil
}

func caInit(args []string) error {
	fs := flag.NewFlagSet("ca init", flag.ExitOnError)
	dir := fs.String("dir", ".", "directory to write ca.crt and ca.key into")
	cn := fs.String("cn", "drsync-ca", "CA common name")
	days := fs.Int("days", 3650, "validity in days")
	force := fs.Bool("force", false, "overwrite existing ca.crt/ca.key")
	fs.Parse(args)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, err := serialNumber()
	if err != nil {
		return err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: *cn},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(0, 0, *days),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true, // issues leaves only, no sub-CAs
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*dir, 0755); err != nil {
		return err
	}
	keyBlock, err := marshalKey(key)
	if err != nil {
		return err
	}
	crtPath := filepath.Join(*dir, "ca.crt")
	keyPath := filepath.Join(*dir, "ca.key")
	if err := writePEM(crtPath, []*pem.Block{{Type: "CERTIFICATE", Bytes: der}}, false, *force); err != nil {
		return err
	}
	if err := writePEM(keyPath, []*pem.Block{keyBlock}, true, *force); err != nil {
		return err
	}
	fmt.Printf("CA created: %s (valid %d days)\n  cert: %s\n  key:  %s\n", *cn, *days, crtPath, keyPath)
	return nil
}

// loadCA reads ca.crt/ca.key from dir for signing leaf certificates.
func loadCA(dir string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	crtPEM, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		return nil, nil, fmt.Errorf("read CA cert (run `drsync ca init` first?): %w", err)
	}
	keyPEM, err := os.ReadFile(filepath.Join(dir, "ca.key"))
	if err != nil {
		return nil, nil, fmt.Errorf("read CA key: %w", err)
	}
	cb, _ := pem.Decode(crtPEM)
	if cb == nil || cb.Type != "CERTIFICATE" {
		return nil, nil, fmt.Errorf("ca.crt is not a PEM certificate")
	}
	caCert, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, nil, err
	}
	kb, _ := pem.Decode(keyPEM)
	if kb == nil {
		return nil, nil, fmt.Errorf("ca.key is not PEM")
	}
	k, err := x509.ParsePKCS8PrivateKey(kb.Bytes)
	if err != nil {
		return nil, nil, err
	}
	key, ok := k.(*ecdsa.PrivateKey)
	if !ok {
		return nil, nil, fmt.Errorf("ca.key is not an ECDSA key")
	}
	return caCert, key, nil
}

func caIssue(args []string) error {
	fs := flag.NewFlagSet("ca issue", flag.ExitOnError)
	dir := fs.String("dir", ".", "directory holding ca.crt/ca.key and where leaves are written")
	typ := fs.String("type", "", "server | agent")
	cn := fs.String("cn", "", "certificate common name (server hostname, or agent id)")
	out := fs.String("out", "", "output basename (default: the CN)")
	days := fs.Int("days", 825, "validity in days")
	force := fs.Bool("force", false, "overwrite existing output")
	var dns multiFlag
	var ips multiFlag
	fs.Var(&dns, "dns", "DNS SAN (repeatable)")
	fs.Var(&ips, "ip", "IP SAN (repeatable)")
	fs.Parse(args)

	if *cn == "" {
		return fmt.Errorf("--cn is required")
	}
	var eku []x509.ExtKeyUsage
	switch *typ {
	case "server":
		eku = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	case "agent":
		eku = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	default:
		return fmt.Errorf("--type must be server or agent")
	}
	caCert, caKey, err := loadCA(*dir)
	if err != nil {
		return err
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, err := serialNumber()
	if err != nil {
		return err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: *cn},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.AddDate(0, 0, *days),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  eku,
		DNSNames:     dns,
	}
	for _, s := range ips {
		ip := net.ParseIP(s)
		if ip == nil {
			return fmt.Errorf("invalid --ip %q", s)
		}
		tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
	}
	// A server the agent verifies by hostname needs at least one SAN; fall back
	// to the CN so a bare `--cn host` still yields a usable server cert.
	if *typ == "server" && len(tmpl.DNSNames) == 0 && len(tmpl.IPAddresses) == 0 {
		if ip := net.ParseIP(*cn); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, *cn)
		}
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	base := *out
	if base == "" {
		base = *cn
	}
	keyBlock, err := marshalKey(key)
	if err != nil {
		return err
	}
	crtPath := filepath.Join(*dir, base+".crt")
	keyPath := filepath.Join(*dir, base+".key")
	if err := writePEM(crtPath, []*pem.Block{{Type: "CERTIFICATE", Bytes: der}}, false, *force); err != nil {
		return err
	}
	if err := writePEM(keyPath, []*pem.Block{keyBlock}, true, *force); err != nil {
		return err
	}
	fmt.Printf("issued %s cert: CN=%s (valid %d days)\n  cert: %s\n  key:  %s\n", *typ, *cn, *days, crtPath, keyPath)
	return nil
}

// multiFlag collects a repeatable string flag.
type multiFlag []string

func (m *multiFlag) String() string { return fmt.Sprint([]string(*m)) }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}
