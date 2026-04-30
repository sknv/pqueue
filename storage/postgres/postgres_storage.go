package postgres

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sknv/pqueue"
)

// Storage is a default Postgres-based implementation of pqueue interface.
type Storage struct {
	db *pgxpool.Pool
}

// NewStorage creates a new Postgres storage.
func NewStorage(db *pgxpool.Pool) *Storage {
	return &Storage{
		db: db,
	}
}

const _insertJobSQL = `
	INSERT INTO pqueue_jobs (
	  id,
	  queue,
	  payload,
	  priority,
	  max_attempts,
	  stuck_timeout_millis,
	  scheduled_at
	)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (id) DO UPDATE
	SET id = pqueue_jobs.id
	RETURNING id,
	          queue,
	          payload,
	          status,
	          priority,
	          attempts,
	          max_attempts,
	          stuck_timeout_millis,
	          scheduled_at,
	          run_at,
	          stuck_at,
	          completed_at,
	          error_message,
	          created_at,
	          updated_at
`

// InsertJob inserts a new job into storage.
func (s *Storage) InsertJob(
	ctx context.Context,
	queryer pqueue.QueryRower,
	id uuid.UUID,
	queue string,
	payload []byte,
	options pqueue.JobOptions,
) (*pqueue.Job, error) {
	var job pqueue.Job

	err := queryer.QueryRow(
		ctx,
		_insertJobSQL,
		id,
		queue,
		payload,
		options.Priority(),
		options.MaxAttempts(),
		options.StuckTimeoutMillis(),
		options.ScheduledAt(),
	).
		Scan(
			&job.ID,
			&job.Queue,
			&job.Payload,
			&job.Status,
			&job.Priority,
			&job.Attempts,
			&job.MaxAttempts,
			&job.StuckTimeoutMillis,
			&job.ScheduledAt,
			&job.RunAt,
			&job.StuckAt,
			&job.CompletedAt,
			&job.ErrorMessage,
			&job.CreatedAt,
			&job.UpdatedAt,
		)
	if err != nil {
		return nil, fmt.Errorf("exec job inserting query: %w", err)
	}

	return &job, nil
}

// InsertBatchJobs inserts a batch of jobs into storage.
func (s *Storage) InsertBatchJobs(
	ctx context.Context,
	batcher pqueue.BatchSender,
	jobs []pqueue.PreparedBatchJob,
) ([]*pqueue.Job, error) {
	// Build the pgx batch
	var batch pgx.Batch

	for _, job := range jobs {
		options := job.Options()

		batch.Queue(
			_insertJobSQL,
			job.ID(),
			job.Queue(),
			job.Payload(),
			options.Priority(),
			options.MaxAttempts(),
			options.StuckTimeoutMillis(),
			options.ScheduledAt(),
		)
	}

	batchResults := batcher.SendBatch(ctx, &batch)

	// Scan results in the same order the queries were queued
	insertedJobs := make([]*pqueue.Job, len(jobs))

	for i := range jobs {
		var job pqueue.Job

		err := batchResults.QueryRow().Scan(
			&job.ID,
			&job.Queue,
			&job.Payload,
			&job.Status,
			&job.Priority,
			&job.Attempts,
			&job.MaxAttempts,
			&job.StuckTimeoutMillis,
			&job.ScheduledAt,
			&job.RunAt,
			&job.StuckAt,
			&job.CompletedAt,
			&job.ErrorMessage,
			&job.CreatedAt,
			&job.UpdatedAt,
		)
		if err != nil {
			// Close drains remaining results before we return
			_ = batchResults.Close()

			return nil, fmt.Errorf("scan result for job at index %d (queue '%s'): %w", i, jobs[i].Queue(), err)
		}

		insertedJobs[i] = &job
	}

	// Close flushes any un-read results and returns any deferred server error
	if err := batchResults.Close(); err != nil {
		return nil, fmt.Errorf("close batch results for inserted jobs: %w", err)
	}

	return insertedJobs, nil
}

const _fetchJobsSQL = `
	WITH pre_candidates AS (
	  (
	    SELECT id, priority, scheduled_at
	    FROM pqueue_jobs
	    WHERE status = $1
	      AND scheduled_at <= now()
	    ORDER BY priority DESC, scheduled_at
	    LIMIT $3
	  )
	  UNION ALL
	  (
	    SELECT id, priority, scheduled_at
	    FROM pqueue_jobs
	    WHERE status = $2
	      AND stuck_at <= now()
	    ORDER BY priority DESC, scheduled_at
	    LIMIT $3
	  )
	),
	candidates AS (
	  SELECT id
	  FROM pre_candidates
	  ORDER BY priority DESC, scheduled_at
	  LIMIT $3
	  FOR NO KEY UPDATE SKIP LOCKED
	)

	UPDATE pqueue_jobs AS j
	SET status = $2,
	    attempts = attempts + 1,
	    run_at = now(),
	    stuck_at = now() + (stuck_timeout_millis * interval '1 millisecond')
	FROM candidates
	WHERE j.id = candidates.id
	RETURNING j.id,
	          j.queue,
	          j.payload,
	          j.status,
	          j.priority,
	          j.attempts,
	          j.max_attempts,
	          j.stuck_timeout_millis,
	          j.scheduled_at,
	          j.run_at,
	          j.stuck_at,
	          j.completed_at,
	          j.error_message,
	          j.created_at,
	          j.updated_at
`

// ListActiveJobs fetches a batch of active jobs from storage.
func (s *Storage) ListActiveJobs(ctx context.Context, batchSize uint) ([]pqueue.Job, error) {
	rows, err := s.db.Query(ctx, _fetchJobsSQL, pqueue.JobStatusPending, pqueue.JobStatusRunning, batchSize)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]pqueue.Job, 0, batchSize)

	for rows.Next() {
		var job pqueue.Job

		err = rows.Scan(
			&job.ID,
			&job.Queue,
			&job.Payload,
			&job.Status,
			&job.Priority,
			&job.Attempts,
			&job.MaxAttempts,
			&job.StuckTimeoutMillis,
			&job.ScheduledAt,
			&job.RunAt,
			&job.StuckAt,
			&job.CompletedAt,
			&job.ErrorMessage,
			&job.CreatedAt,
			&job.UpdatedAt,
		)
		if err != nil {
			log.Printf("[PQueue][ERROR] Failed to scan job: %v", err)

			continue
		}

		jobs = append(jobs, job)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate over jobs: %w", err)
	}

	return jobs, nil
}

// CompleteJob marks a job in storage as completed.
func (s *Storage) CompleteJob(ctx context.Context, id uuid.UUID) error {
	const sql = `
		UPDATE pqueue_jobs
		SET status = $2,
		    completed_at = now(),
		    error_message = NULL
		WHERE id = $1
	`

	cmd, err := s.db.Exec(ctx, sql, id, pqueue.JobStatusCompleted)
	if err != nil {
		return fmt.Errorf("exec job completing query: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("job with id '%s' was not marked as completed", id)
	}

	return nil
}

// ReScheduleJob schedules a job for a further reprocessing.
func (s *Storage) ReScheduleJob(
	ctx context.Context,
	id uuid.UUID,
	scheduledAt time.Time,
	errorMessage string,
) error {
	const sql = `
		UPDATE pqueue_jobs
		SET status = $2,
		    scheduled_at = $3,
		    error_message = $4
		WHERE id = $1
	`

	cmd, err := s.db.Exec(ctx, sql, id, pqueue.JobStatusPending, scheduledAt, errorMessage)
	if err != nil {
		return fmt.Errorf("exec job rescheduling query: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("job with id '%s' was not rescheduled", id)
	}

	return nil
}

// FailJob marks a job in storage as failed.
func (s *Storage) FailJob(ctx context.Context, id uuid.UUID, errorMessage string) error {
	const sql = `
		UPDATE pqueue_jobs
		SET status = $2,
		    completed_at = now(),
		    error_message = $3
		WHERE id = $1
	`

	cmd, err := s.db.Exec(ctx, sql, id, pqueue.JobStatusFailed, errorMessage)
	if err != nil {
		return fmt.Errorf("exec job failing query: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("job with id '%s' was not marked as failed", id)
	}

	return nil
}

// DeleteColdJobs removes completed jobs.
func (s *Storage) DeleteColdJobs(ctx context.Context, cutoffDate time.Time, limit uint) (uint, error) {
	return s.deleteJobs(ctx, pqueue.JobStatusCompleted, cutoffDate, limit)
}

// DeleteDeadJobs removes dead jobs.
func (s *Storage) DeleteDeadJobs(ctx context.Context, cutoffDate time.Time, limit uint) (uint, error) {
	return s.deleteJobs(ctx, pqueue.JobStatusFailed, cutoffDate, limit)
}

// deleteJobs removes jobs by status.
func (s *Storage) deleteJobs(
	ctx context.Context,
	status pqueue.JobStatus,
	cutoffDate time.Time,
	limit uint,
) (uint, error) {
	const sql = `
		DELETE FROM pqueue_jobs
		WHERE id IN (
		  SELECT id FROM pqueue_jobs
		  WHERE status = $1
		    AND created_at < $2
		  LIMIT $3
		)
	`

	cmd, err := s.db.Exec(ctx, sql, status, cutoffDate, limit)
	if err != nil {
		return 0, fmt.Errorf("exec jobs deleting query: %w", err)
	}

	rowsAffected := cmd.RowsAffected()

	return uint(rowsAffected), nil //nolint:gosec // signed i64 should always be enough
}
