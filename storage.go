package pqueue

import (
	"context"
	"time"
	"uuid"
)

// Storage defines the persistence layer for the job queue.
type Storage interface {
	// InsertJob inserts a single job and returns the persisted record.
	InsertJob(
		ctx context.Context,
		queryer QueryRower,
		id uuid.UUID,
		queue string,
		payload []byte,
		options JobOptions,
	) (*Job, error)
	// InsertBatchJobs inserts multiple jobs in a single database round-trip
	// and returns the persisted records in the same order as the input.
	InsertBatchJobs(
		ctx context.Context,
		batcher BatchSender,
		jobs []PreparedBatchJob,
	) ([]*Job, error)

	// ListActiveJobs atomically claims and returns up to batchSize pending or
	// stuck-running jobs for the specified queues. If no queues are given,
	// jobs from all queues are fetched.
	ListActiveJobs(ctx context.Context, queues []string, batchSize uint) ([]Job, error)

	// CompleteJob marks a job as completed.
	CompleteJob(ctx context.Context, id uuid.UUID) error
	// ReScheduleJob moves a job back to the pending state with a new schedule
	// and stores the error message from the last failed attempt.
	ReScheduleJob(ctx context.Context, id uuid.UUID, scheduledAt time.Time, errorMessage string) error
	// FailJob marks a job as failed (dead-letter) and records the error message.
	FailJob(ctx context.Context, id uuid.UUID, errorMessage string) error

	// CreatePartitions pre-creates weekly sub-partitions for the current week
	// plus the specified number of forward weeks across all partition groups.
	CreatePartitions(ctx context.Context, forwardWeeks uint) error
	// DropOldColdPartitions drops all completed-jobs weekly partitions whose
	// entire week precedes the cutoff date. Returns the number of dropped partitions.
	DropOldColdPartitions(ctx context.Context, cutoffDate time.Time) (uint, error)
	// DropOldDeadPartitions drops all failed-jobs weekly partitions whose
	// entire week precedes the cutoff date. Returns the number of dropped partitions.
	DropOldDeadPartitions(ctx context.Context, cutoffDate time.Time) (uint, error)
}
