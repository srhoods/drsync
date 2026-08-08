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
	"strings"
	"syscall"
	"time"

	"drsync/coordinator/internal/agentsrv"
	"drsync/coordinator/internal/api"
	"drsync/coordinator/internal/authn"
	"drsync/coordinator/internal/certs"
	"drsync/coordinator/internal/events"
	"drsync/coordinator/internal/journal"
	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/notify"
	"drsync/coordinator/internal/passctrl"
	"drsync/coordinator/internal/scheduler"
	"drsync/coordinator/internal/store"
)

// defaultAPITokenFile is the conventional location for the REST API bearer
// token. When it is absent, the API runs unauthenticated (dev mode only) —
// unless interactive login is configured, which does not depend on this file.
const defaultAPITokenFile = "/etc/drsync/api-token"

// defaultSMTPConfig is the conventional location for SMTP server settings.
// When it is absent, email notifications are simply disabled.
const defaultSMTPConfig = "/etc/drsync/smtp.yaml"

// defaultAuthConfig is the conventional location for WebUI/API interactive
// login settings (local host accounts or Active Directory). When it is
// absent, interactive login is disabled and the API stays token-only.
const defaultAuthConfig = "/etc/drsync/auth.yaml"

// defaultCertsConfig is the conventional location for the coordinator's
// HTTP(S) listener TLS certificate. When it is absent, the REST/WebUI
// listener serves plain http://.
const defaultCertsConfig = "/etc/drsync/certs.yaml"

// sessionSecretFile is the persisted HMAC secret backing session cookies,
// stored alongside the state DB so sessions survive a coordinator restart.
const sessionSecretFile = "session.key"

// journalFlushInterval bounds how often persisted journal batches are fsynced
// and acked. Small enough that the ack latency it adds to shard completion is
// negligible against the agent's 120s ack timeout, large enough that the fsync
// rate on the journal segments stays cheap.
const journalFlushInterval = 250 * time.Millisecond

// heartbeatFlushInterval bounds how often connected agents' last_heartbeat
// timestamps are batch-persisted (agentsrv.RunHeartbeatFlusher). Display-only
// (the operator-facing agents API), so this is a cosmetic staleness window,
// not a durability concern — wide enough that it costs the write lock only a
// handful of times a minute regardless of fleet size, tight enough that an
// operator watching the agents page never sees a stale-looking timestamp on a
// connection that is actually still healthy.
const heartbeatFlushInterval = 5 * time.Second

// coordinatorVersion is surfaced in the console header and /api/v1/info.
const coordinatorVersion = "0.1.1"

func main() {
	var (
		agentAddr    = flag.String("listen-agent", ":7440", "agent protocol listen address")
		httpAddr     = flag.String("listen-http", ":7441", "REST/metrics listen address")
		dataDir      = flag.String("data-dir", "/var/lib/drsync", "state store + journal directory")
		apiTokenFile = flag.String("api-token-file", defaultAPITokenFile,
			"file holding the REST API bearer token, must be mode 0600 (absent default = no token auth, dev only)")
		tlsCert  = flag.String("tls-cert", "", "coordinator TLS certificate (PEM)")
		tlsKey   = flag.String("tls-key", "", "coordinator TLS key (PEM)")
		tlsCA    = flag.String("tls-ca", "", "CA bundle for verifying agent client certs (PEM)")
		leaseTTL = flag.Duration("lease-ttl", 30*time.Second, "shard lease TTL")
		hbEvery  = flag.Duration("heartbeat-interval", 5*time.Second, "agent heartbeat interval")
		logLevel = flag.String("log-level", "info", "debug|info|warn|error")
		minMinor = flag.Uint("min-agent-minor", 0,
			"refuse agents below this protocol minor (0 = accept all compatible agents)")
		smtpConfig  = flag.String("smtp-config", defaultSMTPConfig, "SMTP config for email notifications (absent default = disabled)")
		authConfig  = flag.String("auth-config", defaultAuthConfig, "WebUI/API login config: local host accounts or Active Directory (absent default = interactive login disabled)")
		certsConfig = flag.String("certs-config", defaultCertsConfig, "HTTP(S) listener TLS certificate config (absent default = plain http://)")
	)
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintln(os.Stderr, "invalid -log-level:", err)
		os.Exit(2)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))

	if *minMinor > agentsrv.ProtoMinor {
		fmt.Fprintf(os.Stderr,
			"invalid -min-agent-minor: %d is above this coordinator's protocol minor (%d); no agent could connect\n",
			*minMinor, agentsrv.ProtoMinor)
		os.Exit(2)
	}

	if err := run(*agentAddr, *httpAddr, *dataDir, *apiTokenFile,
		*tlsCert, *tlsKey, *tlsCA, *smtpConfig, *authConfig, *certsConfig,
		*leaseTTL, *hbEvery, uint32(*minMinor)); err != nil {
		slog.Error("drsyncd exiting", "err", err)
		os.Exit(1)
	}
}

func run(agentAddr, httpAddr, dataDir, apiTokenFile, tlsCert, tlsKey, tlsCA, smtpConfig, authConfig, certsConfig string,
	leaseTTL, hbEvery time.Duration, minAgentMinor uint32) error {

	// REST API bearer token. An absent default file disables token auth
	// silently (dev mode, or interactive login alone); an explicitly
	// configured path that is missing, world/group readable, or empty is a
	// hard error — a token file drsyncd will happily read but the operator
	// didn't intend, or one anyone on the box can also read, is worse than
	// refusing to start.
	apiToken, err := loadAPIToken(apiTokenFile, apiTokenFile == defaultAPITokenFile)
	if err != nil {
		return fmt.Errorf("api token: %w", err)
	}

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
	pc.SetMetrics(met)

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
		MinAgentMinor:     minAgentMinor,
	}, st, sched, jw, met)

	apiSrv := api.New(st, pc, met, bus, journalRoot, apiToken)
	apiSrv.ConnectedAgents = asrv.ConnectedAgents
	apiSrv.AgentInflight = asrv.Inflight
	apiSrv.SetAgentDrain = asrv.SetDrain
	apiSrv.NotifyJobDone = asrv.NotifyJobDone
	pc.NotifyJobDone = asrv.NotifyJobDone
	apiSrv.DropJournal = jw.DropJob
	apiSrv.Info = api.CoordinatorInfo{
		FleetEpoch: fmt.Sprintf("%016x", fleetEpoch),
		LeaseTTLS:  int(leaseTTL / time.Second),
		MTLS:       tlsConf != nil,
		Version:    coordinatorVersion,
	}
	poller.ConnectedAgents = asrv.ConnectedAgents

	// Interactive login (WebUI). An absent default config disables it
	// silently, staying token-only; an explicitly-configured path that is
	// missing or invalid is a hard error.
	authCfg, err := authn.LoadConfig(authConfig, authConfig == defaultAuthConfig)
	if err != nil {
		return fmt.Errorf("auth config: %w", err)
	}
	if authCfg != nil {
		auther, err := authn.New(authCfg)
		if err != nil {
			return fmt.Errorf("auth config: %w", err)
		}
		sessions, err := authn.NewSessionManager(filepath.Join(dataDir, sessionSecretFile),
			time.Duration(authCfg.SessionTTLMinutes)*time.Minute)
		if err != nil {
			return fmt.Errorf("session manager: %w", err)
		}
		apiSrv.SetAuth(authCfg, auther, sessions)
		slog.Info("interactive login enabled", "mode", authCfg.Mode, "config", authConfig)
	} else {
		slog.Info("interactive login disabled (no auth config)", "config", authConfig)
	}

	// HTTP(S) listener TLS. An absent default config falls back to plain
	// http://; an explicitly-configured path that is missing or invalid is a
	// hard error — a half-configured cert must not silently downgrade a
	// deployment that asked for TLS.
	httpTLSCfg, err := certs.LoadConfig(certsConfig)
	if err != nil {
		return fmt.Errorf("certs config: %w", err)
	}
	httpTLSConf, err := httpTLSCfg.TLSConfig()
	if err != nil {
		return fmt.Errorf("certs config: %w", err)
	}
	apiSrv.SetHTTPSEnabled(httpTLSConf != nil)
	if httpTLSConf != nil {
		slog.Info("http listener TLS enabled", "config", certsConfig)
	} else {
		slog.Warn("http listener serving plain http:// (no certs config); "+
			"set up "+certsConfig+" for production use", "config", certsConfig)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go sched.RunSweeper(ctx, leaseTTL/3)
	go st.RunIncrementalVacuum(ctx, 30*time.Second)
	// wal_autocheckpoint is disabled at Open (store.go) — this replaces it on
	// a schedule this process controls, instead of firing inline on whichever
	// write happens to cross SQLite's own WAL-page threshold and blocking
	// s.mu (and therefore every agent's grant/renew/complete call) for
	// however long that copy takes.
	//
	// PASSIVE every 60s, TRUNCATE every 200s — independent cadences (see
	// RunWALCheckpoint in store.go for why they're no longer one ticker with
	// TRUNCATE riding every Nth PASSIVE tick, and the full history below).
	// The original 5-minute, TRUNCATE-only interval measured a live 10-agent
	// median WALCheckpoint hold of 33s (max 144s) — TRUNCATE needs an
	// exclusive lock over the whole WAL to shrink the file, held under s.mu
	// for the full copy-back duration, so every other coordinator write
	// blocked for the whole stretch, not a rare tail (caught via the
	// hold-side half of store.lockTimed, docs/DESIGN-agent.md's diagnostic
	// log catalog). Shortening the interval to 20s (still TRUNCATE-only at
	// the time) helped a lot but scaled sublinearly — 15x shorter interval
	// only bought 7x shorter hold — which means TRUNCATE has real fixed
	// overhead (the truncate step plus its fsync) independent of backlog
	// size. Splitting into frequent cheap PASSIVE checkpoints (drains WAL
	// content, no exclusive lock needed) with TRUNCATE only periodically
	// (reclaims file size, but by then PASSIVE has already drained most of
	// what it would otherwise have to copy) addresses that floor instead of
	// just calling TRUNCATE more often — this got TRUNCATE's own median down
	// to 2.95s (max 8.7s). But a 6-hour, 10-agent test at PASSIVE=20s (one
	// ticker, TRUNCATE riding every 10th tick) found PASSIVE's own median
	// hold (3.4s) running *higher* than TRUNCATE's — at 20s there is so
	// little real WAL content per call that PASSIVE's hold is dominated by
	// the same fixed per-call overhead that limited TRUNCATE before the
	// split, just paid 9x as often. Decoupling the two cadences (this call)
	// lets PASSIVE widen to accumulate more real backlog per call — 60s,
	// still far more frequent than the original 5-minute baseline — without
	// having to also widen TRUNCATE's already-working interval to do it.
	// Moving checkpointing off s.mu entirely was considered and rejected:
	// TRUNCATE's exclusive WAL lock is a SQLite-level constraint, not an
	// app-level overcaution, so that would only trade a blocked goroutine for
	// a SQLITE_BUSY retry loop of similar wall-clock cost. Re-measure after
	// changing either number here.
	go st.RunWALCheckpoint(ctx, 60*time.Second, 200*time.Second)
	go pc.Run(ctx, 2*time.Second)
	go poller.Run(ctx, time.Second)
	// Journal durability: fsync persisted batches, then ack each agent up to its
	// durable high-water. Gating the ack on fsync is what makes JournalAck mean
	// "durable" — see agentsrv.RunJournalFlusher.
	go asrv.RunJournalFlusher(ctx, journalFlushInterval)
	// last_heartbeat batching: see agentsrv.RunHeartbeatFlusher.
	go asrv.RunHeartbeatFlusher(ctx, heartbeatFlushInterval)

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
	if httpTLSConf != nil {
		httpSrv.TLSConfig = httpTLSConf
	}
	go func() {
		slog.Info("http listener up", "addr", httpAddr, "https", httpTLSConf != nil,
			"token_auth", apiToken != "", "login_auth", authCfg != nil)
		var err error
		if httpTLSConf != nil {
			// Cert/key already loaded into httpTLSConf; ListenAndServeTLS
			// re-reads from disk unless passed empty paths, which reuses
			// what's already in http.Server.TLSConfig.Certificates.
			err = httpSrv.ListenAndServeTLS("", "")
		} else {
			err = httpSrv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
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

// loadAPIToken reads the REST API bearer token from path. When missingOK is
// true an absent file yields ("", nil) so a dev deployment need not create
// one; an explicitly configured path that is missing is an error. The file
// must have no group or world permission bits set (0600 or stricter, e.g.
// 0400) — group- or world-readable is refused outright, since anyone who can
// read it can authenticate as the coordinator's own operators. Trailing
// whitespace/newline (the common `echo secret >file` or
// editor artifact) is trimmed; an empty (or all-whitespace) file is an error
// rather than silently falling back to no-auth, since a zero-byte token file
// left behind by e.g. `install -m 600 /dev/null` is far more likely to be a
// deployment mistake than an intentional "no auth" signal — that signal is
// the file being absent, not present-and-empty.
func loadAPIToken(path string, missingOK bool) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		if missingOK && os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return "", fmt.Errorf("%s must not be group- or world-readable (mode %04o); chmod 600 %s", path, perm, path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return token, nil
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
