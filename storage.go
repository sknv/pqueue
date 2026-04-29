package pqueue

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Storage interface {
	InsertJob(
		ctx context.Context,
		queryer QueryRower,
		id uuid.UUID,
		queue string,
		payload []byte,
		options JobOptions,
	) (*Job, error)
	InsertBatchJobs(
		ctx context.Context,
		batcher BatchSender,
		jobs []PreparedBatchJob,
	) ([]*Job, error)

	ListActiveJobs(ctx context.Context, batchSize uint) ([]Job, error)

	CompleteJob(ctx context.Context, id uuid.UUID) error
	ReScheduleJob(ctx context.Context, id uuid.UUID, scheduledAt time.Time, errorMessage string) error
	FailJob(ctx context.Context, id uuid.UUID, errorMessage string) error

	DeleteColdJobs(ctx context.Context, cutoffDate time.Time, limit uint) (uint, error)
	DeleteDeadJobs(ctx context.Context, cutoffDate time.Time, limit uint) (uint, error)
}
