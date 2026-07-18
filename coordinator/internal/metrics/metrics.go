// Package metrics registers the Prometheus surface (DESIGN-coordinator.md §7).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	Registry *prometheus.Registry

	// Agent-reported cumulative values (gauges: agents report absolutes,
	// Prometheus rate() works over them like counters).
	ScanEntries *prometheus.GaugeVec
	CopyFiles   *prometheus.GaugeVec
	CopyBytes   *prometheus.GaugeVec
	AgentUp     *prometheus.GaugeVec
	AgentRSS    *prometheus.GaugeVec

	// ShardDuration is the agent-measured wall time of a completed shard, by
	// kind. The agent has always sent this (ShardCounters.wall_ms); this is
	// where it becomes visible. Watching the high quantiles per kind is the
	// primary signal for "the job is grinding to a halt" — a rising p99 with a
	// flat median means a few pathological shards, the reverse means everything
	// is uniformly slower (usually the mounts or the scheduler).
	ShardDuration *prometheus.HistogramVec

	// Coordinator-side.
	ShardQueueDepth *prometheus.GaugeVec
	LeaseExpiries   prometheus.Counter
	ShardsParked    prometheus.Counter
	JournalBatches  prometheus.Counter
	JournalFsyncErr prometheus.Counter
	Grants          prometheus.Counter
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		ScanEntries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "drsync_scan_entries_total", Help: "Entries scanned (agent-cumulative)."},
			[]string{"agent"}),
		CopyFiles: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "drsync_copy_files_total", Help: "Files copied (agent-cumulative)."},
			[]string{"agent"}),
		CopyBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "drsync_copy_bytes_total", Help: "Bytes copied (agent-cumulative)."},
			[]string{"agent"}),
		AgentUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "drsync_agent_up", Help: "1 while the agent session is established."},
			[]string{"agent"}),
		AgentRSS: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "drsync_agent_rss_bytes", Help: "Agent resident set size."},
			[]string{"agent"}),
		ShardDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "drsync_shard_duration_seconds",
			Help: "Agent-measured wall time of a completed shard, by kind.",
			// 100ms to ~3.4h: shards range from a trivial dirfix batch to a
			// walk of a pathological directory, and the long tail is the point.
			Buckets: prometheus.ExponentialBuckets(0.1, 3, 12),
		}, []string{"kind"}),
		ShardQueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "drsync_shard_queue_depth", Help: "Shards by state (active passes)."},
			[]string{"state"}),
		LeaseExpiries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "drsync_lease_expiries_total", Help: "Expired shard leases re-queued."}),
		ShardsParked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "drsync_shards_parked_total", Help: "Shards parked for operator attention."}),
		JournalBatches: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "drsync_journal_batches_total", Help: "Journal batches persisted."}),
		JournalFsyncErr: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "drsync_journal_fsync_errors_total",
			Help: "Journal fsync failures; acks withheld until a later flush succeeds."}),
		Grants: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "drsync_work_grants_total", Help: "Work items granted to agents."}),
	}
	reg.MustRegister(m.ScanEntries, m.CopyFiles, m.CopyBytes, m.AgentUp, m.AgentRSS,
		m.ShardDuration, m.ShardQueueDepth, m.LeaseExpiries, m.ShardsParked,
		m.JournalBatches, m.JournalFsyncErr, m.Grants)
	return m
}
