package postgres

import (
	"context"
	"fmt"
	"time"
	"uuid"

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

// _insertJobSQL inserts a new job
// (fast path: plain INSERT, no idempotency check).
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
	VALUES (
	  job_id,
	  job_queue,
	  job_payload,
	  job_priority,
	  job_max_attempts,
	  job_stuck_timeout_millis,
	  job_scheduled_at
	)
  	RETURNING
	  id,
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

// _insertIdempotentJobSQL inserts a job via pqueue_enqueue_idempotent_job,
// which checks for an existing row by id across all partitions before inserting.
// If a job with the same id already exists, the existing row is returned without modification.
// Race-safe via ON CONFLICT (id, status) inside the function.
const _insertIdempotentJobSQL = `
	SELECT
	  id,
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
 	FROM pqueue_enqueue_idempotent_job($1, $2, $3, $4, $5, $6, $7)
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
	sql := _insertJobSQL
	if options.IsIdempotent() {
		sql = _insertIdempotentJobSQL
	}

	var job pqueue.Job

	err := queryer.QueryRow(
		ctx,
		sql,
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
//
//nolint:funlen // linear logic
func (s *Storage) InsertBatchJobs(
	ctx context.Context,
	batcher pqueue.BatchSender,
	jobs []pqueue.PreparedBatchJob,
) ([]*pqueue.Job, error) {
	// Build the pgx batch
	var batch pgx.Batch

	for _, job := range jobs {
		options := job.Options()

		sql := _insertJobSQL
		if options.IsIdempotent() {
			sql = _insertIdempotentJobSQL
		}

		batch.Queue(
			sql,
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

// _fetchJobsSQL atomically claims up to $3 pending or stuck-running jobs
// across all queues using FOR NO KEY UPDATE SKIP LOCKED, transitions them to
// running, and returns the updated rows.
//   - $1: pending status value
//   - $2: running status value
//   - $3: batch size (applied to each sub-query and the final candidate set)
const _fetchJobsSQL = `
	WITH pre_candidates AS (
	  (
	    SELECT id, priority, scheduled_at
	    FROM pqueue_jobs_hot
	    WHERE status = $1
	      AND scheduled_at <= now()
	    ORDER BY priority DESC, scheduled_at
	    LIMIT $3
	  )
	  UNION ALL
	  (
	    SELECT id, priority, scheduled_at
	    FROM pqueue_jobs_hot
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

	UPDATE pqueue_jobs_hot AS j
	SET status = $2,
	    attempts = attempts + 1,
	    run_at = now(),
	    stuck_at = now() + (stuck_timeout_millis * interval '1 millisecond')
	FROM candidates
	WHERE j.id = candidates.id
	RETURNING
	  j.id,
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

// _fetchJobsWithQueuesSQL is the queue-filtered variant of _fetchJobsSQL.
//   - $1: queue name list (text[])
//   - $2: pending status value
//   - $3: running status value
//   - $4: batch size
const _fetchJobsWithQueuesSQL = `
	WITH pre_candidates AS (
	  (
	    SELECT id, priority, scheduled_at
	    FROM pqueue_jobs_hot
	    WHERE queue = ANY($1)
	      AND status = $2
	      AND scheduled_at <= now()
	    ORDER BY priority DESC, scheduled_at
	    LIMIT $4
	  )
	  UNION ALL
	  (
	    SELECT id, priority, scheduled_at
	    FROM pqueue_jobs_hot
	    WHERE queue = ANY($1)
	      AND status = $3
	      AND stuck_at <= now()
	    ORDER BY priority DESC, scheduled_at
	    LIMIT $4
	  )
	),
	candidates AS (
	  SELECT id
	  FROM pre_candidates
	  ORDER BY priority DESC, scheduled_at
	  LIMIT $4
	  FOR NO KEY UPDATE SKIP LOCKED
	)

	UPDATE pqueue_jobs_hot AS j
	SET status = $3,
	    attempts = attempts + 1,
	    run_at = now(),
	    stuck_at = now() + (stuck_timeout_millis * interval '1 millisecond')
	FROM candidates
	WHERE j.id = candidates.id
	RETURNING
	  j.id,
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

// ListActiveJobs fetches a batch of active jobs from storage for the specified queues.
// If no queues specified jobs for all queues will be fetched.
//
//nolint:funlen // linear logic
func (s *Storage) ListActiveJobs(ctx context.Context, queues []string, batchSize uint) ([]pqueue.Job, error) {
	var (
		sql  string
		args []any
	)

	if len(queues) > 0 {
		sql = _fetchJobsWithQueuesSQL
		args = []any{
			queues,
			pqueue.JobStatusPending,
			pqueue.JobStatusRunning,
			batchSize,
		}
	} else {
		sql = _fetchJobsSQL
		args = []any{
			pqueue.JobStatusPending,
			pqueue.JobStatusRunning,
			batchSize,
		}
	}

	rows, err := s.db.Query(ctx, sql, args...)
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
			return nil, fmt.Errorf("scan job: %w", err)
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
		UPDATE pqueue_jobs_hot
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
		UPDATE pqueue_jobs_hot
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
		UPDATE pqueue_jobs_hot
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

// CreatePartitions pre-creates weekly sub-partitions for the current week plus
// the specified number of forward weeks across all partition groups.
func (s *Storage) CreatePartitions(ctx context.Context, forwardWeeks uint) error {
	const sql = `SELECT pqueue_create_weekly_partitions($1)`

	if _, err := s.db.Exec(ctx, sql, int(forwardWeeks)); err != nil {
		return fmt.Errorf("exec create weekly partitions query: %w", err)
	}

	return nil
}

// DropOldColdPartitions drops all completed-jobs weekly partitions whose entire
// week precedes the cutoff date. Returns the number of dropped partitions.
func (s *Storage) DropOldColdPartitions(ctx context.Context, cutoffDate time.Time) (uint, error) {
	return s.dropOldPartitions(ctx, "cold", cutoffDate)
}

// DropOldDeadPartitions drops all failed-jobs weekly partitions whose entire
// week precedes the cutoff date. Returns the number of dropped partitions.
func (s *Storage) DropOldDeadPartitions(ctx context.Context, cutoffDate time.Time) (uint, error) {
	return s.dropOldPartitions(ctx, "dead", cutoffDate)
}

// dropOldPartitions calls the pqueue_drop_old_partitions SQL function to drop
// all weekly sub-partitions of the given group whose entire week precedes the
// cutoff date. Returns the number of dropped partitions.
func (s *Storage) dropOldPartitions(
	ctx context.Context,
	groupName string,
	cutoffDate time.Time,
) (uint, error) {
	const sql = `SELECT pqueue_drop_old_partitions($1, $2)`

	var dropped int

	if err := s.db.QueryRow(ctx, sql, groupName, cutoffDate).Scan(&dropped); err != nil {
		return 0, fmt.Errorf("exec drop old %s partitions query: %w", groupName, err)
	}

	return uint(dropped), nil //nolint:gosec // partition count will never overflow uint
}
