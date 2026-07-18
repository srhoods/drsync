package model

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	drsyncpb "drsync/proto/gen/drsyncpb"
)

// ByteSize accepts plain integers (bytes) or KiB/MiB/GiB/TiB suffixed strings.
type ByteSize uint64

var sizeRe = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)\s*(KiB|MiB|GiB|TiB)?$`)

func (b *ByteSize) UnmarshalYAML(node *yaml.Node) error {
	s := strings.TrimSpace(node.Value)
	m := sizeRe.FindStringSubmatch(s)
	if m == nil {
		return fmt.Errorf("invalid size %q (want bytes or KiB/MiB/GiB/TiB)", s)
	}
	val, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return err
	}
	mult := float64(1)
	switch m[2] {
	case "KiB":
		mult = 1 << 10
	case "MiB":
		mult = 1 << 20
	case "GiB":
		mult = 1 << 30
	case "TiB":
		mult = 1 << 40
	}
	*b = ByteSize(val * mult)
	return nil
}

// JobSpec mirrors docs/DESIGN-jobspec.md. Zero values are replaced by
// ApplyDefaults; only fields the skeleton consumes are modeled so far —
// unknown YAML fields are rejected (strict decode) to keep typo safety.
type JobSpec struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name        string `yaml:"name"`
		Description string `yaml:"description,omitempty"`
	} `yaml:"metadata"`
	Spec struct {
		Source      struct{ Path string } `yaml:"source"`
		Destination struct{ Path string } `yaml:"destination"`
		Filters     []map[string]string   `yaml:"filters,omitempty"`
		Passes      struct {
			Max          int    `yaml:"max"`
			Schedule     string `yaml:"schedule"`
			ConvergeWhen struct {
				DeltaFilesBelow uint64   `yaml:"delta_files_below"`
				DeltaBytesBelow ByteSize `yaml:"delta_bytes_below"`
			} `yaml:"converge_when"`
		} `yaml:"passes"`
		Copy struct {
			ChunkThreshold ByteSize `yaml:"chunk_threshold"`
			ChunkSize      ByteSize `yaml:"chunk_size"`
			BufferSize     ByteSize `yaml:"buffer_size"`
			PreserveSparse *bool    `yaml:"preserve_sparse"`
			ServerSideCopy string   `yaml:"server_side_copy"`
			TempNaming     string   `yaml:"temp_naming"`
			Fsync          string   `yaml:"fsync"`
		} `yaml:"copy"`
		Metadata struct {
			Owner  *bool `yaml:"owner"`
			Mode   *bool `yaml:"mode"`
			Times  *bool `yaml:"times"`
			Xattrs *bool `yaml:"xattrs"`
			ACLs   struct {
				Posix          *bool  `yaml:"posix"`
				NFS4           *bool  `yaml:"nfs4"`
				Untranslatable string `yaml:"untranslatable"`
			} `yaml:"acls"`
			Specials *bool `yaml:"specials"`
		} `yaml:"metadata"`
		Verify struct {
			Mode     string `yaml:"mode"` // on (default) | off — off skips the verify phase entirely
			Checksum struct {
				SampleRate float64 `yaml:"sample_rate"`
				OnMismatch string  `yaml:"on_mismatch"`
			} `yaml:"checksum"`
		} `yaml:"verify"`
		Deletes struct {
			Mode string `yaml:"mode"`
		} `yaml:"deletes"`
		Limits struct {
			BandwidthPerAgent ByteSize `yaml:"bandwidth_per_agent"`
			IOPSPerAgent      uint64   `yaml:"iops_per_agent"`
		} `yaml:"limits"`
		Tuning struct {
			ShardBudget       uint64 `yaml:"shard_budget"`
			DirSplitThreshold uint64 `yaml:"dir_split_threshold"`
			StatxBatch        uint32 `yaml:"statx_batch"`
			MtimeSlopNS       int64  `yaml:"mtime_slop_ns"`
			// Fan-out control. Coordinator-side only: these never reach an agent
			// (D9 — the agent acts on the resolved per-shard overrides it is
			// granted, not on policy). See SpreadPolicy.
			SpreadMode           string `yaml:"spread_mode"`
			SpreadTargetPerAgent uint64 `yaml:"spread_target_per_agent"`
		} `yaml:"tuning"`
		Notifications NotificationSpec `yaml:"notifications,omitempty"`
	} `yaml:"spec"`
}

// NotificationSpec configures email notifications for a job. Delivery depends
// on the coordinator having a valid SMTP config (see -smtp-config); when it
// does not, these flags are inert (the coordinator logs a warning). Sending is
// always best-effort and never affects job outcome.
type NotificationSpec struct {
	// Recipients receive every notification for the job (To: header).
	Recipients []string `yaml:"recipients,omitempty"`
	// OnPassComplete sends a mail as each pass finishes (the convergence trace).
	OnPassComplete bool `yaml:"on_pass_complete,omitempty"`
	// OnJobComplete sends a single summary mail when the job reaches COMPLETED.
	OnJobComplete bool `yaml:"on_job_complete,omitempty"`
}

// Enabled reports whether the spec asks for any email at all.
func (n NotificationSpec) Enabled() bool { return n.OnPassComplete || n.OnJobComplete }

// ParseSpec strictly decodes, defaults and validates a YAML job spec.
func ParseSpec(data []byte) (*JobSpec, error) {
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	var s JobSpec
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("parse spec: %w", err)
	}
	s.ApplyDefaults()
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

func boolDefault(p **bool, v bool) {
	if *p == nil {
		*p = &v
	}
}

func (s *JobSpec) ApplyDefaults() {
	sp := &s.Spec
	if sp.Passes.Max == 0 {
		sp.Passes.Max = 10
	}
	if sp.Passes.Schedule == "" {
		sp.Passes.Schedule = "continuous"
	}
	if sp.Copy.ChunkThreshold == 0 {
		sp.Copy.ChunkThreshold = 1 << 30 // 1 GiB
	}
	if sp.Copy.ChunkSize == 0 {
		// One chunk per GiB by default. With chunk_threshold at 1 GiB, a file
		// only fans out once it exceeds a single chunk (> chunk_size), so an
		// 8 GiB default would have kept every file up to 8 GiB on one agent.
		sp.Copy.ChunkSize = 1 << 30
	}
	if sp.Copy.BufferSize == 0 {
		sp.Copy.BufferSize = 1 << 20
	}
	boolDefault(&sp.Copy.PreserveSparse, true)
	if sp.Copy.ServerSideCopy == "" {
		sp.Copy.ServerSideCopy = "auto"
	}
	if sp.Copy.TempNaming == "" {
		sp.Copy.TempNaming = ".drsync.tmp."
	}
	if sp.Copy.Fsync == "" {
		sp.Copy.Fsync = "per_file"
	}
	boolDefault(&sp.Metadata.Owner, true)
	boolDefault(&sp.Metadata.Mode, true)
	boolDefault(&sp.Metadata.Times, true)
	boolDefault(&sp.Metadata.Xattrs, true)
	boolDefault(&sp.Metadata.ACLs.Posix, true)
	boolDefault(&sp.Metadata.ACLs.NFS4, true)
	if sp.Metadata.ACLs.Untranslatable == "" {
		sp.Metadata.ACLs.Untranslatable = "warn"
	}
	boolDefault(&sp.Metadata.Specials, true)
	if sp.Verify.Mode == "" {
		sp.Verify.Mode = "on"
	}
	if sp.Verify.Checksum.SampleRate == 0 {
		sp.Verify.Checksum.SampleRate = 0.01
	}
	if sp.Verify.Checksum.OnMismatch == "" {
		sp.Verify.Checksum.OnMismatch = "recopy"
	}
	if sp.Deletes.Mode == "" {
		sp.Deletes.Mode = "report"
	}
	if sp.Tuning.ShardBudget == 0 {
		sp.Tuning.ShardBudget = 250_000
	}
	if sp.Tuning.DirSplitThreshold == 0 {
		sp.Tuning.DirSplitThreshold = 50_000
	}
	if sp.Tuning.StatxBatch == 0 {
		sp.Tuning.StatxBatch = 256
	}
	if sp.Tuning.MtimeSlopNS == 0 {
		sp.Tuning.MtimeSlopNS = 1_000_000
	}
	if sp.Tuning.SpreadMode == "" {
		sp.Tuning.SpreadMode = SpreadAuto
	}
	if sp.Tuning.SpreadTargetPerAgent == 0 {
		sp.Tuning.SpreadTargetPerAgent = 32
	}
}

// Spread modes for tuning.spread_mode.
const (
	// SpreadAuto fans out only while the fleet is starved of walk shards.
	SpreadAuto = "auto"
	// SpreadOff never overrides the job's shard_budget: a shard descends until
	// its budget runs out, as it did before fan-out existed.
	SpreadOff = "off"
	// SpreadAlways fans out on every grant, whatever the queue depth. Costs a
	// coordinator round trip per directory — for diagnosis, not production.
	SpreadAlways = "always"

	// maxFilterRules / maxFilterPattern mirror the agent's fixed-size filter
	// table (FILTER_MAX_RULES / FILTER_PATTERN_MAX - 1 in agent/src/msgs.h).
	maxFilterRules   = 64
	maxFilterPattern = 255
)

// SpreadPolicy is the coordinator-side fan-out policy resolved from the spec.
// It is deliberately absent from JobOptions: agents receive the *result* of the
// policy as per-shard WalkOverrides, never the policy itself.
type SpreadPolicy struct {
	Mode           string
	TargetPerAgent uint64
}

// SpreadPolicy resolves the job's fan-out policy. Call on a defaulted spec.
func (s *JobSpec) SpreadPolicy() SpreadPolicy {
	return SpreadPolicy{
		Mode:           s.Spec.Tuning.SpreadMode,
		TargetPerAgent: s.Spec.Tuning.SpreadTargetPerAgent,
	}
}

// PathsOverlap reports whether two absolute paths are equal or one contains the
// other. Comparison is on whole path components — appending "/" to both is what
// keeps /dst/a and /dst/ab from reading as nested — so it is a containment test,
// not a string-prefix test.
func PathsOverlap(a, b string) bool {
	ca := strings.TrimRight(a, "/") + "/"
	cb := strings.TrimRight(b, "/") + "/"
	return ca == cb || strings.HasPrefix(ca, cb) || strings.HasPrefix(cb, ca)
}

func (s *JobSpec) Validate() error {
	if s.APIVersion != "drsync/v1" {
		return fmt.Errorf("apiVersion must be drsync/v1, got %q", s.APIVersion)
	}
	if s.Kind != "Job" {
		return fmt.Errorf("kind must be Job, got %q", s.Kind)
	}
	if s.Metadata.Name == "" {
		return fmt.Errorf("metadata.name is required")
	}
	src, dst := s.Spec.Source.Path, s.Spec.Destination.Path
	if src == "" || dst == "" {
		return fmt.Errorf("spec.source.path and spec.destination.path are required")
	}
	if !strings.HasPrefix(src, "/") || !strings.HasPrefix(dst, "/") {
		return fmt.Errorf("source and destination paths must be absolute")
	}
	if PathsOverlap(src, dst) {
		return fmt.Errorf("source and destination must be disjoint")
	}
	// Bounds mirror the agent's fixed-size filter table (FILTER_MAX_RULES /
	// FILTER_PATTERN_MAX in agent/src/msgs.h). Enforcing them here keeps the
	// agent's buffers from ever truncating a pattern, which would silently
	// change matching and copy data the operator meant to exclude.
	if len(s.Spec.Filters) > maxFilterRules {
		return fmt.Errorf("filters: %d rules exceeds limit of %d",
			len(s.Spec.Filters), maxFilterRules)
	}
	for i, f := range s.Spec.Filters {
		if len(f) != 1 {
			return fmt.Errorf("filters[%d]: want exactly one of include:/exclude:", i)
		}
		for k, pat := range f {
			if k != "include" && k != "exclude" {
				return fmt.Errorf("filters[%d]: unknown key %q", i, k)
			}
			if pat == "" {
				return fmt.Errorf("filters[%d]: empty pattern", i)
			}
			if len(pat) > maxFilterPattern {
				return fmt.Errorf("filters[%d]: pattern length %d exceeds limit of %d",
					i, len(pat), maxFilterPattern)
			}
		}
	}
	switch s.Spec.Deletes.Mode {
	case "report", "mirror":
	default:
		return fmt.Errorf("deletes.mode must be report|mirror")
	}
	switch s.Spec.Verify.Mode {
	case "on", "off":
	default:
		return fmt.Errorf("verify.mode must be on|off")
	}
	if r := s.Spec.Verify.Checksum.SampleRate; r < 0 || r > 1 {
		return fmt.Errorf("verify.checksum.sample_rate must be in [0,1]")
	}
	switch s.Spec.Tuning.SpreadMode {
	case SpreadAuto, SpreadOff, SpreadAlways:
	default:
		return fmt.Errorf("tuning.spread_mode must be auto|off|always")
	}
	n := s.Spec.Notifications
	if n.Enabled() && len(n.Recipients) == 0 {
		return fmt.Errorf("notifications: on_pass_complete/on_job_complete set but recipients is empty")
	}
	for i, addr := range n.Recipients {
		if !emailRe.MatchString(addr) {
			return fmt.Errorf("notifications.recipients[%d]: %q is not a valid email address", i, addr)
		}
	}
	return nil
}

// emailRe is a deliberately permissive sanity check (not RFC 5322) — it rejects
// obvious typos (missing @, spaces, no dot in the domain) without pretending to
// fully validate addressing, which the SMTP server does authoritatively.
var emailRe = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)

// ToJobOptions resolves the spec into the protobuf options agents consume,
// stamping job identity and the options hash (DESIGN-jobspec.md §3).
func (s *JobSpec) ToJobOptions(jobID uint64, dryRun bool) (*drsyncpb.JobOptions, error) {
	sp := &s.Spec
	o := &drsyncpb.JobOptions{
		JobId:   jobID,
		JobName: s.Metadata.Name,
		SrcRoot: sp.Source.Path,
		DstRoot: sp.Destination.Path,
		Copy: &drsyncpb.CopyOptions{
			ChunkThreshold: uint64(sp.Copy.ChunkThreshold),
			ChunkSize:      uint64(sp.Copy.ChunkSize),
			BufferSize:     uint64(sp.Copy.BufferSize),
			PreserveSparse: *sp.Copy.PreserveSparse,
			TempPrefix:     sp.Copy.TempNaming,
		},
		Metadata: &drsyncpb.MetadataOptions{
			Owner:  *sp.Metadata.Owner,
			Mode:   *sp.Metadata.Mode,
			Times:  *sp.Metadata.Times,
			Xattrs: *sp.Metadata.Xattrs,
			Acls: &drsyncpb.AclOptions{
				Posix: *sp.Metadata.ACLs.Posix,
				Nfs4:  *sp.Metadata.ACLs.NFS4,
			},
			Specials: *sp.Metadata.Specials,
		},
		Verify: &drsyncpb.VerifyOptions{
			SampleRatePpm: uint32(sp.Verify.Checksum.SampleRate * 1_000_000),
		},
		Limits: &drsyncpb.LimitOptions{
			BandwidthPerAgent: uint64(sp.Limits.BandwidthPerAgent),
			IopsPerAgent:      sp.Limits.IOPSPerAgent,
		},
		Tuning: &drsyncpb.TuningOptions{
			ShardBudget:       sp.Tuning.ShardBudget,
			DirSplitThreshold: sp.Tuning.DirSplitThreshold,
			StatxBatch:        sp.Tuning.StatxBatch,
			MtimeSlopNs:       sp.Tuning.MtimeSlopNS,
		},
		DryRun: dryRun,
	}
	switch sp.Copy.ServerSideCopy {
	case "auto":
		o.Copy.ServerSideCopy = drsyncpb.CopyOptions_SSC_AUTO
	case "off":
		o.Copy.ServerSideCopy = drsyncpb.CopyOptions_SSC_OFF
	case "require":
		o.Copy.ServerSideCopy = drsyncpb.CopyOptions_SSC_REQUIRE
	default:
		return nil, fmt.Errorf("copy.server_side_copy must be auto|off|require")
	}
	if sp.Copy.Fsync == "per_file" {
		o.Copy.FsyncMode = drsyncpb.CopyOptions_FSYNC_PER_FILE
	} else {
		o.Copy.FsyncMode = drsyncpb.CopyOptions_FSYNC_BATCHED
	}
	switch sp.Metadata.ACLs.Untranslatable {
	case "warn":
		o.Metadata.Acls.Untranslatable = drsyncpb.AclOptions_UNTRANSLATABLE_WARN
	case "fail":
		o.Metadata.Acls.Untranslatable = drsyncpb.AclOptions_UNTRANSLATABLE_FAIL
	case "skip":
		o.Metadata.Acls.Untranslatable = drsyncpb.AclOptions_UNTRANSLATABLE_SKIP
	default:
		return nil, fmt.Errorf("metadata.acls.untranslatable must be warn|fail|skip")
	}
	if sp.Verify.Checksum.OnMismatch == "fail" {
		o.Verify.OnMismatch = drsyncpb.VerifyOptions_ON_MISMATCH_FAIL
	} else {
		o.Verify.OnMismatch = drsyncpb.VerifyOptions_ON_MISMATCH_RECOPY
	}
	for _, f := range sp.Filters {
		for k, pat := range f {
			o.Filters = append(o.Filters, &drsyncpb.FilterRule{
				Exclude: k == "exclude",
				Pattern: pat,
			})
		}
	}
	// Deterministic hash over everything above; agents cache options by it.
	blob, err := proto.MarshalOptions{Deterministic: true}.Marshal(o)
	if err != nil {
		return nil, err
	}
	h := fnv.New64a()
	h.Write(blob)
	o.OptionsHash = h.Sum64()
	return o, nil
}
