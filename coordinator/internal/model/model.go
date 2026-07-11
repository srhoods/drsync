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
func (k ShardKind) Priority() int {
	switch k {
	case KindChunk:
		return 10
	case KindProbe:
		return 20
	default:
		return 0
	}
}
