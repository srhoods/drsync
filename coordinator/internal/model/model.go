// Package model holds shared state enums and the YAML job spec.
package model

type JobState string

const (
	JobCreated   JobState = "CREATED"
	JobReady     JobState = "READY"
	JobRunning   JobState = "RUNNING"
	JobPaused    JobState = "PAUSED"
	JobCompleted JobState = "COMPLETED"
	JobCancelled JobState = "CANCELLED"
	JobFailed    JobState = "FAILED"
)

type PassState string

const (
	PassPending  PassState = "PENDING"
	PassProbing  PassState = "PROBING" // per-agent mount probes gate the root shard
	PassScanning PassState = "SCANNING"
	PassDirfix   PassState = "DIRFIX"
	PassVerify   PassState = "VERIFY"
	PassDelete   PassState = "DELETE"
	PassComplete PassState = "COMPLETE"
)

type ShardState string

const (
	ShardQueued ShardState = "QUEUED"
	ShardLeased ShardState = "LEASED"
	ShardDone   ShardState = "DONE"
	ShardParked ShardState = "PARKED"
)

type ShardKind string

const (
	KindDir       ShardKind = "dir"
	KindEntryList ShardKind = "entrylist"
	KindChunk     ShardKind = "chunk"
	KindDirfix    ShardKind = "dirfix"
	KindVerify    ShardKind = "verify"
	KindDelete    ShardKind = "delete"
	KindProbe     ShardKind = "probe"
)

// Scheduling priorities (higher = granted first). Chunk tasks outrank walk
// shards so a huge file's chunks saturate the fleet (DESIGN-coordinator §4).
// Delete (orphan-removal) shards outrank chunk work so a mirror-mode delete
// pass reclaims destination space promptly; the mount probe still outranks
// everything, gating pass start.
func (k ShardKind) Priority() int {
	switch k {
	case KindChunk:
		return 10
	case KindDelete:
		return 15
	case KindProbe:
		return 20
	default:
		return 0
	}
}

// phaseRank orders PassState by pipeline progress, for comparing "how far
// along" two stage values are (as opposed to string/enum equality).
var phaseRank = map[PassState]int{
	PassPending:  0,
	PassProbing:  1,
	PassScanning: 2,
	PassDirfix:   3,
	PassVerify:   4,
	PassDelete:   5,
	PassComplete: 6,
}

// phaseOfKind maps a shard kind to the pass phase it belongs to, for shard
// kinds whose mere presence in the queue proves the pass has reached that
// phase (dir/entrylist/chunk/probe shards can appear again mid-SCANNING via
// recursive directory expansion, so they carry no such signal).
var phaseOfKind = map[ShardKind]PassState{
	KindDirfix: PassDirfix,
	KindVerify: PassVerify,
	KindDelete: PassDelete,
}

// EffectiveState reports the further-along of the pass's stored state and
// whatever phase its live shard-kind mix proves it has already reached.
//
// advance() seeds a phase's shards before flipping passes.state to that
// phase (deliberately — see passctrl.advance — so a tick can't see the new
// phase with an empty queue and skip it), and LeaseShards has no phase gate,
// so agents start leasing/working those shards immediately. That leaves a
// window, bounded by the passctrl tick interval, where the queue already
// shows the next phase's shards while the stored state still names the
// previous one. Reporting surfaces call this instead of reading State raw so
// the displayed stage can't visibly lag what the queue shows.
func (st PassState) EffectiveState(kindsPresent map[ShardKind]bool) PassState {
	eff := st
	for k, present := range kindsPresent {
		if !present {
			continue
		}
		if ph, ok := phaseOfKind[k]; ok && phaseRank[ph] > phaseRank[eff] {
			eff = ph
		}
	}
	return eff
}
