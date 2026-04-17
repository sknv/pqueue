package pqueue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

//
// Jobs
//

// JobStatus represents the status of a job.
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
)

// Job represents a job in the queue.
type Job struct {
	ID                 int64
	Queue              string
	Payload            []byte
	Status             JobStatus
	Priority           int
	Attempts           int
	MaxAttempts        int
	StuckTimeoutMillis int64
	ScheduledAt        time.Time
	RunAt              *time.Time
	CompletedAt        *time.Time
	ErrorMessage       *string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// JobHandler defines a job processor.
type JobHandler interface {
	HandleJob(ctx context.Context, job *Job) error
	CalculateBackoff(attempt int) time.Duration
}

// JobOption is a function to configure job options.
type JobOption func(*jobOptions)

type jobOptions struct {
	priority     int
	maxAttempts  int
	stuckTimeout time.Duration
	scheduledAt  time.Time
}

func defaultJobOptions() *jobOptions {
	//nolint:mnd // default values
	return &jobOptions{
		priority:     0,
		maxAttempts:  10,
		stuckTimeout: time.Minute * 10,
		scheduledAt:  time.Now(),
	}
}

func (j *jobOptions) stuckTimeoutMillis() int64 {
	return int64(j.stuckTimeout / time.Millisecond)
}

// WithPriority sets the job priority.
func WithPriority(priority int) JobOption {
	return func(o *jobOptions) {
		o.priority = priority
	}
}

// WithScheduledAt sets when the job should be executed.
func WithScheduledAt(t time.Time) JobOption {
	return func(o *jobOptions) {
		o.scheduledAt = t
	}
}

// WithStuckTimeout sets when the job should be considered as stuck.
func WithStuckTimeout(t time.Duration) JobOption {
	return func(o *jobOptions) {
		o.stuckTimeout = t
	}
}

// WithMaxAttempts sets the maximum number of attempts.
func WithMaxAttempts(attempts int) JobOption {
	return func(o *jobOptions) {
		o.maxAttempts = attempts
	}
}

//
// Queue
//

// Encoder marshals and unmarshals job payload.
type Encoder interface {
	Encode(v any) ([]byte, error)
}

type (
	// PollConfig configures the poller loop.
	PollConfig struct {
		BatchSize    int           // number of jobs to claim per poll
		Concurrency  int           // max in-flight jobs
		PollInterval time.Duration // sleep when no jobs claimed
	}

	// CleanupConfig configures cold and dead partitions cleaning process.
	CleanupConfig struct {
		RetentionInterval time.Duration // time to keep jobs in partition
		CleanupBatchSize  int           // how many records should we delete at once
	}

	// QueueConfig holds configuration for the job queue.
	QueueConfig struct {
		Poll        PollConfig
		ColdCleanup CleanupConfig
		DeadCleanup CleanupConfig
	}
)

// DefaultConfig returns a default configuration.
func DefaultConfig() *QueueConfig {
	//nolint:mnd // default values
	return &QueueConfig{
		Poll: PollConfig{
			BatchSize:    10,
			Concurrency:  10,
			PollInterval: time.Second,
		},
		ColdCleanup: CleanupConfig{
			RetentionInterval: time.Hour * 24 * 7,
			CleanupBatchSize:  10_000,
		},
		DeadCleanup: CleanupConfig{
			RetentionInterval: time.Hour * 24 * 90,
			CleanupBatchSize:  10_000,
		},
	}
}

// QueueOption is a function to configure queue options.
type QueueOption func(*Queue)

// WithEncoder sets the encoder to marshal and unmarshal job payload.
func WithEncoder(encoder Encoder) QueueOption {
	return func(q *Queue) {
		q.encoder = encoder
	}
}

// Queue represents the job queue.
type Queue struct {
	db      *pgxpool.Pool
	config  *QueueConfig
	encoder Encoder

	handlers map[string]JobHandler
	wg       sync.WaitGroup
}

// NewQueue creates a new job queue.
func NewQueue(
	db *pgxpool.Pool,
	config *QueueConfig,
	opts ...QueueOption,
) *Queue {
	if config == nil {
		config = DefaultConfig()
	}

	queue := &Queue{
		db:      db,
		config:  config,
		encoder: &JsonEncoder{},

		handlers: make(map[string]JobHandler),
		wg:       sync.WaitGroup{},
	}

	for _, opt := range opts {
		opt(queue)
	}

	return queue
}

// RegisterHandler registers a handler for a job type.
func (q *Queue) RegisterHandler(jobType string, handler JobHandler) {
	q.handlers[jobType] = handler
}

const _enqueueSQL = `
	INSERT INTO pqueue_jobs (queue, payload, priority, max_attempts, stuck_timeout_millis, scheduled_at)
	VALUES ($1, $2, $3, $4, $5, $6)
	RETURNING id, queue, payload, status, priority, attempts, max_attempts, stuck_timeout_millis,
	          scheduled_at, run_at, completed_at, error_message, created_at, updated_at
`

// Enqueue adds a new job to the queue.
func (q *Queue) Enqueue(
	ctx context.Context,
	queryer QueryRower,
	queue string,
	payload any,
	opts ...JobOption,
) (*Job, error) {
	if queue == "" {
		return nil, errors.New("queue name must not be empty")
	}

	options := defaultJobOptions()
	for _, opt := range opts {
		opt(options)
	}

	payloadBytes, err := q.encoder.Encode(payload)
	if err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}

	var job Job

	err = queryer.QueryRow(
		ctx,
		_enqueueSQL,
		queue,
		payloadBytes,
		options.priority,
		options.maxAttempts,
		options.stuckTimeoutMillis(),
		options.scheduledAt,
	).
		Scan(
			&job.ID, &job.Queue, &job.Payload, &job.Status, &job.Priority,
			&job.Attempts, &job.MaxAttempts, &job.StuckTimeoutMillis, &job.ScheduledAt, &job.RunAt,
			&job.CompletedAt, &job.ErrorMessage, &job.CreatedAt, &job.UpdatedAt,
		)
	if err != nil {
		return nil, fmt.Errorf("insert job: %w", err)
	}

	return &job, nil
}

// BatchJob describes a single job to be enqueued as part of a batch.
type BatchJob struct {
	Queue   string
	Payload any
	Opts    []JobOption
}

// EnqueueBatch inserts all provided jobs in a single database round-trip.
// It returns the persisted Job records in the same order as the input slice.
// The call is atomic when sender is a pgx.Tx.
//
//nolint:funlen // linear logic
func (q *Queue) EnqueueBatch(
	ctx context.Context,
	sender BatchSender,
	jobs []BatchJob,
) ([]*Job, error) {
	if len(jobs) == 0 {
		return nil, nil
	}

	// Encode every payload up front so we don't partially send the batch on an encoding error
	type preparedJob struct {
		queue   string
		payload []byte
		options *jobOptions
	}

	prepared := make([]preparedJob, len(jobs))
	for i, batchJob := range jobs {
		if batchJob.Queue == "" {
			return nil, fmt.Errorf("job at index %d: queue name must not be empty", i)
		}

		options := defaultJobOptions()
		for _, opt := range batchJob.Opts {
			opt(options)
		}

		payloadBytes, err := q.encoder.Encode(batchJob.Payload)
		if err != nil {
			return nil, fmt.Errorf("job at index %d: encode payload: %w", i, err)
		}

		prepared[i] = preparedJob{
			queue:   batchJob.Queue,
			payload: payloadBytes,
			options: options,
		}
	}

	// Build the pgx batch
	var batch pgx.Batch

	for _, p := range prepared {
		batch.Queue(
			_enqueueSQL,
			p.queue,
			p.payload,
			p.options.priority,
			p.options.maxAttempts,
			p.options.stuckTimeoutMillis(),
			p.options.scheduledAt,
		)
	}

	batchResults := sender.SendBatch(ctx, &batch)

	// Scan results in the same order the queries were queued
	enqueued := make([]*Job, len(jobs))

	for i := range prepared {
		var job Job

		err := batchResults.QueryRow().Scan(
			&job.ID, &job.Queue, &job.Payload, &job.Status, &job.Priority,
			&job.Attempts, &job.MaxAttempts, &job.StuckTimeoutMillis, &job.ScheduledAt, &job.RunAt,
			&job.CompletedAt, &job.ErrorMessage, &job.CreatedAt, &job.UpdatedAt,
		)
		if err != nil {
			// Close drains remaining results before we return
			_ = batchResults.Close()

			return nil, fmt.Errorf("scan result for job at index %d (queue %q): %w", i, prepared[i].queue, err)
		}

		enqueued[i] = &job
	}

	// Close flushes any un-read results and returns any deferred server error
	if err := batchResults.Close(); err != nil {
		return nil, fmt.Errorf("close batch results: %w", err)
	}

	return enqueued, nil
}

// Start begins processing jobs.
func (q *Queue) Start(ctx context.Context) {
	// Start handler worker
	q.wg.Go(func() {
		q.runHandlerWorker(ctx)
	})
}

// Stop gracefully stops the queue.
func (q *Queue) Stop(ctx context.Context) error {
	stopped := make(chan struct{}, 1)

	go func() {
		q.wg.Wait()

		stopped <- struct{}{}
	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context done: %w", ctx.Err())
	}
}

// runHandlerWorker starts a worker to process the jobs.
func (q *Queue) runHandlerWorker(ctx context.Context) {
	ticker := time.NewTicker(q.config.Poll.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				processed := q.processJobs(ctx)
				if processed == 0 {
					break // no more jobs, wait for the next timer tick
				}
				// There are more jobs to process, handle them immediately
			}
		}
	}
}

// handlerJobs fetches batch of jobs from db and routes them to handlers. Returns a total count of fetched jobs.
func (q *Queue) processJobs(ctx context.Context) int {
	// Fetch jobs from db first
	jobs, err := q.fetchJobs(ctx)
	if err != nil {
		log.Printf("[PQueue][ERROR] Failed to fetch jobs: %v", err)

		return 0
	}

	if len(jobs) == 0 {
		return 0
	}

	// Process jobs concurrently respecting concurrency limit
	gr := errgroup.Group{}
	gr.SetLimit(q.config.Poll.Concurrency)

	for i := range jobs {
		gr.Go(func() error {
			job := &jobs[i]

			if jobErr := q.handleJob(ctx, job); jobErr != nil {
				log.Printf("[PQueue][ERROR] Failed to handle job with id '%d': %v", job.ID, err)
			}

			return nil
		})
	}

	if err = gr.Wait(); err != nil {
		log.Printf("[PQueue][ERROR] Failed to wait for all jobs to complete: %v", err)

		return 0
	}

	return len(jobs)
}

// fetchJobs fetches batch of jobs from db.
func (q *Queue) fetchJobs(ctx context.Context) ([]Job, error) {
	const (
		dbTimeout = time.Second * 30
	)

	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	sql := `
		WITH candidates AS (
		  SELECT id
		  FROM pqueue_jobs_hot
		  WHERE scheduled_at <= now()
		    AND (
		      status = $1 OR (status = $2 AND run_at <= now() - stuck_timeout_millis * interval '1 millisecond')
		    )
		  ORDER BY priority DESC, scheduled_at
		  FOR NO KEY UPDATE SKIP LOCKED
		  LIMIT $3
		)
		UPDATE pqueue_jobs_hot AS j
		SET status = $2,
		    attempts = attempts + 1,
		    run_at = now(),
		    updated_at = now()
		FROM candidates
		WHERE j.id = candidates.id
		RETURNING j.id, j.queue, j.payload, j.status, j.priority, j.attempts, j.max_attempts, j.stuck_timeout_millis,
		          j.scheduled_at, j.run_at, j.completed_at, j.error_message, j.created_at, j.updated_at
		`

	rows, err := q.db.Query(ctx, sql, JobStatusPending, JobStatusRunning, q.config.Poll.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("fetch jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]Job, 0, q.config.Poll.BatchSize)

	for rows.Next() {
		var job Job

		err = rows.Scan(
			&job.ID, &job.Queue, &job.Payload, &job.Status, &job.Priority,
			&job.Attempts, &job.MaxAttempts, &job.StuckTimeoutMillis, &job.ScheduledAt, &job.RunAt,
			&job.CompletedAt, &job.ErrorMessage, &job.CreatedAt, &job.UpdatedAt,
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

// handleJob processes the provided job by handing it to the corresponding handler
// and finishes it depending on the handling result.
func (q *Queue) handleJob(ctx context.Context, job *Job) error {
	handler, exists := q.handlers[job.Queue]
	if !exists {
		return q.failJob(ctx, job, fmt.Errorf("no handler registered for job: %s", job.Queue))
	}

	if err := handler.HandleJob(ctx, job); err != nil {
		return q.handleJobError(ctx, handler, job, err)
	}

	return q.completeJob(ctx, job)
}

// completeJob marks a job as completed and moves it to a cold partition.
func (q *Queue) completeJob(ctx context.Context, job *Job) error {
	const (
		dbTimeout = time.Second * 30
	)

	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	cmd, err := q.db.Exec(ctx, `
		UPDATE pqueue_jobs
		SET status = $2,
		    completed_at = now(),
		    error_message = NULL,
		    updated_at = now()
		WHERE id = $1
		  AND status IN ($3, $4)
	`, job.ID, JobStatusCompleted, JobStatusPending, JobStatusRunning)
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("job with id '%d' was not marked as completed", job.ID)
	}

	return nil
}

// handleJobError handles job processing errors with backoff.
func (q *Queue) handleJobError(
	ctx context.Context,
	jobHandler JobHandler,
	job *Job,
	err error,
) error {
	// Do not retry unrecoverable errors and jobs that reach their limit
	if IsUnrecoverable(err) || job.Attempts >= job.MaxAttempts {
		return q.failJob(ctx, job, err)
	}

	const (
		dbTimeout = time.Second * 30
	)

	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	errMsg := err.Error()

	// Calculate backoff duration
	backoff := jobHandler.CalculateBackoff(job.Attempts)
	nextSchedule := time.Now().Add(backoff)

	// Update a job with new schedule
	cmd, err := q.db.Exec(ctx, `
		UPDATE pqueue_jobs_hot
		SET status = $2,
		    scheduled_at = $3,
		    error_message = $4,
		    updated_at = now()
		WHERE id = $1
	`, job.ID, JobStatusPending, nextSchedule, errMsg)
	if err != nil {
		return fmt.Errorf("re-schedule job: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("job with id '%d' was not re-scheduled", job.ID)
	}

	return nil
}

// failJob immediately fails a job moving it to the dead letter queue (partition with dead status).
func (q *Queue) failJob(ctx context.Context, job *Job, err error) error {
	const (
		dbTimeout = time.Second * 30
	)

	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	errMsg := err.Error()

	cmd, err := q.db.Exec(ctx, `
		UPDATE pqueue_jobs
		SET status = $2,
		    error_message = $3,
		    updated_at = now()
		WHERE id = $1
		  AND status IN ($4, $5)
	`, job.ID, JobStatusFailed, errMsg, JobStatusPending, JobStatusRunning)
	if err != nil {
		return fmt.Errorf("fail job: %w", err)
	}

	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("job with id '%d' was not marked as failed", job.ID)
	}

	return nil
}

// CleanColdJobs removes old jobs from cold partition.
//
// Most of the times the function should be called from some sort of a cron job.
func (q *Queue) CleanColdJobs(ctx context.Context) error {
	// Keep jobs in partition if there is no retention
	if q.config.ColdCleanup.RetentionInterval <= 0 {
		return nil
	}

	const (
		dbTimeout = time.Second * 30
	)

	log.Printf("[PQueue][INFO] Running cold jobs cleaner...")

	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	cutoffDate := time.Now().Add(-q.config.ColdCleanup.RetentionInterval)
	sql := `
		DELETE FROM pqueue_jobs_cold
		WHERE id IN (
		  SELECT id FROM pqueue_jobs_cold
		  WHERE created_at < $1
		  LIMIT $2
		)
	`

	cmd, err := q.db.Exec(ctx, sql, cutoffDate, q.config.ColdCleanup.CleanupBatchSize)
	if err != nil {
		return fmt.Errorf("clean cold jobs: %w", err)
	}

	rowsAffected := cmd.RowsAffected()
	if rowsAffected == 0 {
		log.Printf("[PQueue][INFO] No cold jobs to be cleaned up")

		return nil
	}

	log.Printf("[PQueue][INFO] Cleaned up %d cold jobs", rowsAffected)

	return nil
}

// CleanDeadJobs removes old jobs from dead partition.
//
// Most of the times the function should be called from some sort of a cron job.
func (q *Queue) CleanDeadJobs(ctx context.Context) error {
	// Keep jobs in partition if there is no retention
	if q.config.DeadCleanup.RetentionInterval <= 0 {
		return nil
	}

	const (
		dbTimeout = time.Second * 30
	)

	log.Printf("[PQueue][INFO] Running dead jobs cleaner...")

	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	cutoffDate := time.Now().Add(-q.config.DeadCleanup.RetentionInterval)
	sql := `
		DELETE FROM pqueue_jobs_dead
		WHERE id IN (
		  SELECT id FROM pqueue_jobs_dead
		  WHERE created_at < $1
		  LIMIT $2
		)
	`

	cmd, err := q.db.Exec(ctx, sql, cutoffDate, q.config.DeadCleanup.CleanupBatchSize)
	if err != nil {
		return fmt.Errorf("clean dead jobs: %w", err)
	}

	rowsAffected := cmd.RowsAffected()
	if rowsAffected == 0 {
		log.Printf("[PQueue][INFO] No dead jobs to be cleaned up")

		return nil
	}

	log.Printf("[PQueue][INFO] Cleaned up %d dead jobs", rowsAffected)

	return nil
}
