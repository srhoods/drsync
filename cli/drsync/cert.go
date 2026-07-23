package main

// `drsync cert` — the coordinator's HTTP(S) listener certificate (what
// browsers/API clients see), distinct from `drsync ca` (the agent<->
// coordinator mTLS trust root). A self-signed pair is fine for dev/test; a
// production deployment should supply a CA-issued cert via certs.yaml
// instead.
//
//	drsync cert generate-self-signed --cn NAME [--dns HOST]... [--ip ADDR]...
//	                                  [--out DIR] [--days N] [--force]

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
)

func cmdCert(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("cert needs a subcommand: generate-self-signed")
	}
	switch args[0] {
	case "generate-self-signed":
		return certGenerateSelfSigned(args[1:])
	default:
		return fmt.Errorf("unknown cert subcommand %q (want generate-self-signed)", args[0])
	}
}

func certGenerateSelfSigned(args []string) error {
	fs := flag.NewFlagSet("cert generate-self-signed", flag.ExitOnError)
	cn := fs.String("cn", "drsync-coordinator", "certificate common name (typically the coordinator hostname)")
	out := fs.String("out", ".", "output directory (writes server.crt and server.key)")
	days := fs.Int("days", 825, "validity in days")
	force := fs.Bool("force", false, "overwrite an existing cert/key pair")
	var dns multiFlag
	var ips multiFlag
	fs.Var(&dns, "dns", "DNS SAN (repeatable); defaults to --cn if none given")
	fs.Var(&ips, "ip", "IP SAN (repeatable)")
	fs.Parse(args)

	dnsNames := []string(dns)
	if len(dnsNames) == 0 {
		dnsNames = []string{*cn}
	}
	var ipAddrs []net.IP
	for _, s := range ips {
		ip := net.ParseIP(s)
		if ip == nil {
			return fmt.Errorf("--ip %q is not a valid IP address", s)
		}
		ipAddrs = append(ipAddrs, ip)
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
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: *cn},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(0, 0, *days),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              dnsNames,
		IPAddresses:           ipAddrs,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return err
	}
	keyBlock, err := marshalKey(key)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(*out, 0755); err != nil {
		return err
	}
	certPath := filepath.Join(*out, "server.crt")
	keyPath := filepath.Join(*out, "server.key")
	if err := writePEM(certPath, []*pem.Block{{Type: "CERTIFICATE", Bytes: der}}, false, *force); err != nil {
		return err
	}
	if err := writePEM(keyPath, []*pem.Block{keyBlock}, true, *force); err != nil {
		return err
	}

	fmt.Printf("self-signed certificate created: %s (valid %d days)\n  cert: %s\n  key:  %s\n\n"+
		"Point /etc/drsync/certs.yaml at these paths to enable HTTPS on the coordinator:\n"+
		"  cert_file: %s\n  key_file: %s\n\n"+
		"This is a self-signed cert (fine for dev/test) — browsers and the CLI will warn\n"+
		"about it unless you trust it explicitly. For production, use a CA-issued cert.\n",
		*cn, *days, certPath, keyPath, certPath, keyPath)
	return nil
}
