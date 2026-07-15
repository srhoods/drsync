// drsyncd — the drsync coordinator. See docs/DESIGN-coordinator.md.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"drsync/coordinator/internal/agentsrv"
	"drsync/coordinator/internal/api"
	"drsync/coordinator/internal/events"
	"drsync/coordinator/internal/journal"
	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/notify"
	"drsync/coordinator/internal/passctrl"
	"drsync/coordinator/internal/scheduler"
	"drsync/coordinator/internal/store"
)

// defaultSMTPConfig is the conventional location for SMTP server settings.
// When it is absent, email notifications are simply disabled.
const defaultSMTPConfig = "/etc/drsync/smtp.yaml"

// coordinatorVersion is surfaced in the console header and /api/v1/info.
const coordinatorVersion = "0.1.0-slice5"

func main() {
	var (
		agentAddr  = flag.String("listen-agent", ":7440", "agent protocol listen address")
		httpAddr   = flag.String("listen-http", ":7441", "REST/metrics listen address")
		dataDir    = flag.String("data-dir", "/var/lib/drsync", "state store + journal directory")
		apiToken   = flag.String("api-token", "", "bearer token for the REST API (empty = no auth, dev only)")
		tlsCert    = flag.String("tls-cert", "", "coordinator TLS certificate (PEM)")
		tlsKey     = flag.String("tls-key", "", "coordinator TLS key (PEM)")
		tlsCA      = flag.String("tls-ca", "", "CA bundle for verifying agent client certs (PEM)")
		leaseTTL   = flag.Duration("lease-ttl", 30*time.Second, "shard lease TTL")
		hbEvery    = flag.Duration("heartbeat-interval", 5*time.Second, "agent heartbeat interval")
		logLevel   = flag.String("log-level", "info", "debug|info|warn|error")
		smtpConfig = flag.String("smtp-config", defaultSMTPConfig, "SMTP config for email notifications (absent default = disabled)")
	)
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintln(os.Stderr, "invalid -log-level:", err)
		os.Exit(2)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))

	if err := run(*agentAddr, *httpAddr, *dataDir, *apiToken,
		*tlsCert, *tlsKey, *tlsCA, *smtpConfig, *leaseTTL, *hbEvery); err != nil {
		slog.Error("drsyncd exiting", "err", err)
		os.Exit(1)
	}
}

func run(agentAddr, httpAddr, dataDir, apiToken, tlsCert, tlsKey, tlsCA, smtpConfig string,
	leaseTTL, hbEvery time.Duration) error {

	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return err
	}
	st, err := store.Open(filepath.Join(dataDir, "state.db"))
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()

	jw, err := journal.NewWriter(filepath.Join(dataDir, "journals"))
	if err != nil {
		return fmt.Errorf("open journals: %w", err)
	}
	defer jw.Close()

	met := metrics.New()
	sched := scheduler.New(st, met, leaseTTL)
	journalRoot := filepath.Join(dataDir, "journals")
	pc := passctrl.New(st, journalRoot)

	// Email notifications. An absent default config disables them silently; an
	// explicitly-configured path that is missing or invalid is a hard error.
	smtpCfg, err := notify.LoadConfig(smtpConfig, smtpConfig == defaultSMTPConfig)
	if err != nil {
		return fmt.Errorf("smtp config: %w", err)
	}
	if smtpCfg != nil {
		pc.SetNotifier(notify.NewSender(smtpCfg))
		slog.Info("email notifications enabled", "smtp_host", smtpCfg.Host,
			"security", smtpCfg.Security, "config", smtpConfig)
	} else {
		slog.Info("email notifications disabled (no SMTP config)", "config", smtpConfig)
	}

	bus := events.NewBus()
	poller := events.NewPoller(st, bus)

	tlsConf, err := buildTLS(tlsCert, tlsKey, tlsCA)
	if err != nil {
		return err
	}
	fleetEpoch := randomEpoch()
	asrv := agentsrv.New(agentsrv.Config{
		HeartbeatInterval: hbEvery,
		LeaseTTL:          leaseTTL,
		TLS:               tlsConf,
		FleetEpoch:        fleetEpoch,
	}, st, sched, jw, met)

	apiSrv := api.New(st, pc, met, bus, journalRoot, apiToken)
	apiSrv.ConnectedAgents = asrv.ConnectedAgents
	apiSrv.DropJournal = jw.DropJob
	apiSrv.Info = api.CoordinatorInfo{
		FleetEpoch: fmt.Sprintf("%016x", fleetEpoch),
		LeaseTTLS:  int(leaseTTL / time.Second),
		MTLS:       tlsConf != nil,
		Version:    coordinatorVersion,
	}
	poller.ConnectedAgents = asrv.ConnectedAgents

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go sched.RunSweeper(ctx, leaseTTL/3)
	go pc.Run(ctx, 2*time.Second)
	go poller.Run(ctx, time.Second)
	go func() { // journal fsync heartbeat (gates should move ack here in phase 1)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := jw.Flush(); err != nil {
					slog.Error("journal flush failed", "err", err)
				}
			}
		}
	}()

	agentLn, err := net.Listen("tcp", agentAddr)
	if err != nil {
		return err
	}
	go func() {
		slog.Info("agent listener up", "addr", agentAddr, "tls", tlsConf != nil)
		if err := asrv.Serve(agentLn); err != nil {
			slog.Error("agent listener failed", "err", err)
			stop()
		}
	}()

	httpSrv := &http.Server{Addr: httpAddr, Handler: apiSrv.Handler()}
	go func() {
		slog.Info("http listener up", "addr", httpAddr, "auth", apiToken != "")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http listener failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	agentLn.Close()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpSrv.Shutdown(shutCtx)
	return nil
}

func buildTLS(cert, key, ca string) (*tls.Config, error) {
	if cert == "" && key == "" && ca == "" {
		return nil, nil // dev mode; agentsrv logs a loud warning
	}
	if cert == "" || key == "" || ca == "" {
		return nil, fmt.Errorf("-tls-cert, -tls-key and -tls-ca must all be set for TLS")
	}
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}
	pool := x509.NewCertPool()
	caPEM, err := os.ReadFile(ca)
	if err != nil {
		return nil, err
	}
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no certificates parsed from %s", ca)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{pair},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func randomEpoch() uint64 {
	var b [8]byte
	rand.Read(b[:])
	return binary.LittleEndian.Uint64(b[:])
}
