package pqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

//
// Jobs
//

// JobStatus represents the status of a job.
type JobStatus string

const (
	// JobStatusPending indicates the job is waiting to be picked up.
	JobStatusPending JobStatus = "pending"
	// JobStatusRunning indicates the job is currently being processed.
	JobStatusRunning JobStatus = "running"
	// JobStatusCompleted indicates the job finished successfully.
	JobStatusCompleted JobStatus = "completed"
	// JobStatusFailed indicates the job exhausted all retry attempts and has been moved to the dead-letter queue.
	JobStatusFailed JobStatus = "failed"
)

// Job represents a job in the queue.
type Job struct {
	ID                 uuid.UUID
	IdempotencyKey     uuid.UUID
	Queue              string
	Payload            []byte
	Status             JobStatus
	Priority           int
	Attempts           uint
	MaxAttempts        uint
	StuckTimeoutMillis uint64
	ScheduledAt        time.Time
	RunAt              *time.Time
	StuckAt            *time.Time
	CompletedAt        *time.Time
	ErrorMessage       *string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// JobOptions holds the resolved options for a job.
type JobOptions struct {
	priority     int
	maxAttempts  uint
	stuckTimeout time.Duration
	scheduledAt  time.Time
}

// defaultJobOptions returns the default options for a job.
func defaultJobOptions() JobOptions {
	//nolint:mnd // default values
	return JobOptions{
		priority:     0,
		maxAttempts:  1,
		stuckTimeout: time.Minute * 5,
		scheduledAt:  time.Now(),
	}
}

// Priority returns job priority.
func (o JobOptions) Priority() int {
	return o.priority
}

// MaxAttempts returns job max attempt count.
func (o JobOptions) MaxAttempts() uint {
	return o.maxAttempts
}

// StuckTimeoutMillis returns job stuck timeout in milliseconds.
func (o JobOptions) StuckTimeoutMillis() int64 {
	return int64(o.stuckTimeout / time.Millisecond)
}

// ScheduledAt returns the time at which the job is scheduled to run.
func (o JobOptions) ScheduledAt() time.Time {
	return o.scheduledAt
}

// JobOption is a function to configure job options.
type JobOption func(*JobOptions)

// WithJobPriority sets the job priority.
func WithJobPriority(priority int) JobOption {
	return func(o *JobOptions) {
		o.priority = priority
	}
}

// WithJobMaxAttempts sets the maximum number of attempts.
func WithJobMaxAttempts(attempts uint) JobOption {
	return func(o *JobOptions) {
		o.maxAttempts = attempts
	}
}

// WithJobStuckTimeout sets when the job should be considered as stuck.
func WithJobStuckTimeout(t time.Duration) JobOption {
	return func(o *JobOptions) {
		o.stuckTimeout = t
	}
}

// WithJobScheduledAt sets when the job should be executed.
func WithJobScheduledAt(t time.Time) JobOption {
	return func(o *JobOptions) {
		o.scheduledAt = t
	}
}

//
// Job handler
//

type (
	// JobHandler defines a job processor.
	JobHandler func(ctx context.Context, job *Job) error

	// BackoffCalculator defines a function to calculate backoff for retries.
	BackoffCalculator func(attempt uint) time.Duration
)

// jobHandlerWrapper defines inner job processor implementation.
type jobHandlerWrapper struct {
	handler           JobHandler
	backoffCalculator BackoffCalculator
}

func (h *jobHandlerWrapper) calculateBackoff(attempt uint, defaultBackoff time.Duration) time.Duration {
	if h.backoffCalculator == nil {
		return defaultBackoff
	}

	return h.backoffCalculator(attempt)
}

// JobHandlerOption is a function to configure job handler options.
type JobHandlerOption func(*jobHandlerWrapper)

// WithJobHandlerBackoffCalculator sets a backoff calculator for job handler.
func WithJobHandlerBackoffCalculator(backoffCalculator BackoffCalculator) JobHandlerOption {
	return func(h *jobHandlerWrapper) {
		h.backoffCalculator = backoffCalculator
	}
}

//
// Queue
//

type (
	// PollConfig configures the poller loop.
	PollConfig struct {
		BatchSize    uint          // number of jobs to claim per poll
		Concurrency  int           // max in-flight jobs
		PollInterval time.Duration // sleep when no jobs claimed
	}

	// ProcessingConfig configures job processing.
	ProcessingConfig struct {
		DbTimeout      time.Duration // database timeout for background operations
		DefaultBackoff time.Duration // default job backoff
	}

	// CleanupConfig configures cold and dead jobs cleaning process.
	CleanupConfig struct {
		DbTimeout         time.Duration // database timeout for background operations
		RetentionInterval time.Duration // time to keep jobs in storage
		CleanupBatchSize  uint          // how many records should we delete at once
	}

	// QueueConfig holds configuration for the job queue.
	QueueConfig struct {
		Poll        PollConfig
		Processing  ProcessingConfig
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
		Processing: ProcessingConfig{
			DbTimeout:      time.Second * 10,
			DefaultBackoff: time.Second * 30,
		},
		ColdCleanup: CleanupConfig{
			DbTimeout:         time.Second * 30,
			RetentionInterval: time.Hour * 24 * 7,
			CleanupBatchSize:  10_000,
		},
		DeadCleanup: CleanupConfig{
			DbTimeout:         time.Second * 30,
			RetentionInterval: time.Hour * 24 * 90,
			CleanupBatchSize:  10_000,
		},
	}
}

// QueueOption is a function to configure queue options.
type QueueOption func(*Queue)

// WithQueueEncoder sets the encoder used to marshal job payloads.
func WithQueueEncoder(encoder Encoder) QueueOption {
	return func(q *Queue) {
		q.encoder = encoder
	}
}

// Queue represents the job queue.
type Queue struct {
	storage Storage
	config  *QueueConfig
	encoder Encoder

	handlers map[string]*jobHandlerWrapper
	wg       sync.WaitGroup
	stopped  chan struct{}
}

// NewQueue creates a new job queue.
func NewQueue(
	storage Storage,
	config *QueueConfig,
	opts ...QueueOption,
) *Queue {
	if config == nil {
		config = DefaultConfig()
	}

	queue := &Queue{
		storage: storage,
		config:  config,
		encoder: &JsonEncoder{},

		handlers: make(map[string]*jobHandlerWrapper),
		wg:       sync.WaitGroup{},
		stopped:  make(chan struct{}),
	}

	for _, opt := range opts {
		opt(queue)
	}

	return queue
}

// RegisterHandler registers a handler for a job type.
func (q *Queue) RegisterHandler(jobType string, handler JobHandler, opts ...JobHandlerOption) {
	handlerWrapper := &jobHandlerWrapper{
		handler:           handler,
		backoffCalculator: nil,
	}

	for _, opt := range opts {
		opt(handlerWrapper)
	}

	q.handlers[jobType] = handlerWrapper
}

// Enqueue adds a new job to the queue.
func (q *Queue) Enqueue(
	ctx context.Context,
	queryer QueryRower,
	queue string,
	idempotencyKey uuid.UUID,
	payload any,
	opts ...JobOption,
) (*Job, error) {
	if queue == "" {
		return nil, errors.New("queue name must not be empty")
	}

	var payloadBytes []byte

	if payload != nil {
		var err error

		payloadBytes, err = q.encoder.Encode(payload)
		if err != nil {
			return nil, fmt.Errorf("encode payload: %w", err)
		}
	}

	id := uuid.Must(uuid.NewV7())

	options := defaultJobOptions()
	for _, opt := range opts {
		opt(&options)
	}

	job, err := q.storage.InsertJob(
		ctx,
		queryer,
		id,
		idempotencyKey,
		queue,
		payloadBytes,
		options,
	)
	if err != nil {
		return nil, fmt.Errorf("insert job into storage: %w", err)
	}

	return job, nil
}

// BatchJob describes a single job to be enqueued as part of a batch.
type BatchJob struct {
	Queue          string
	IdempotencyKey uuid.UUID
	Payload        any
	Opts           []JobOption
}

// EnqueueBatch inserts all provided jobs in a single database round-trip.
// It returns the persisted Job records in the same order as the input slice.
// The call is atomic when batcher is a pgx.Tx.
func (q *Queue) EnqueueBatch(
	ctx context.Context,
	batcher BatchSender,
	jobs []BatchJob,
) ([]*Job, error) {
	if len(jobs) == 0 {
		return nil, nil
	}

	// Prepare batch of jobs first
	preparedJobs, err := q.prepareBatchJobs(jobs)
	if err != nil {
		return nil, fmt.Errorf("prepare batch jobs: %w", err)
	}

	enqueuedJobs, err := q.storage.InsertBatchJobs(ctx, batcher, preparedJobs)
	if err != nil {
		return nil, fmt.Errorf("insert batch jobs into storage: %w", err)
	}

	return enqueuedJobs, nil
}

// PreparedBatchJob is an internal representation of a batch job with its payload
// already encoded and options resolved. It is passed to Storage.InsertBatchJobs.
type PreparedBatchJob struct {
	id             uuid.UUID
	idempotencyKey uuid.UUID
	queue          string
	payload        []byte
	options        JobOptions
}

// ID returns an id of a prepared job.
func (j PreparedBatchJob) ID() uuid.UUID {
	return j.id
}

// IdempotencyKey returns an idempotency key of a prepared job.
func (j PreparedBatchJob) IdempotencyKey() uuid.UUID {
	return j.idempotencyKey
}

// Queue returns a queue of a prepared job.
func (j PreparedBatchJob) Queue() string {
	return j.queue
}

// Payload returns payload of a prepared job.
func (j PreparedBatchJob) Payload() []byte {
	return j.payload
}

// Options returns options of a prepared job.
func (j PreparedBatchJob) Options() JobOptions {
	return j.options
}

// prepareBatchJobs encodes every payload and applies options up front
// so we don't partially send the batch on an encoding error.
func (q *Queue) prepareBatchJobs(jobs []BatchJob) ([]PreparedBatchJob, error) {
	prepared := make([]PreparedBatchJob, len(jobs))
	for i, batchJob := range jobs {
		if batchJob.Queue == "" {
			return nil, fmt.Errorf("job with idempotency key %s: queue name must not be empty", batchJob.IdempotencyKey)
		}

		var payloadBytes []byte

		if batchJob.Payload != nil {
			var err error

			payloadBytes, err = q.encoder.Encode(batchJob.Payload)
			if err != nil {
				return nil, fmt.Errorf("job with idempotency key %s: encode payload: %w", batchJob.IdempotencyKey, err)
			}
		}

		id := uuid.Must(uuid.NewV7())

		options := defaultJobOptions()
		for _, opt := range batchJob.Opts {
			opt(&options)
		}

		prepared[i] = PreparedBatchJob{
			id:             id,
			idempotencyKey: batchJob.IdempotencyKey,
			queue:          batchJob.Queue,
			payload:        payloadBytes,
			options:        options,
		}
	}

	return prepared, nil
}

// Decoder return the queue decoder.
func (q *Queue) Decoder() Decoder {
	return q.encoder.Decode
}

// Start begins processing jobs.
// If optional queues argument provided worker will process only specified queues.
func (q *Queue) Start(ctx context.Context, queues ...string) {
	// Start handler worker
	q.wg.Go(func() {
		// Unlink original context cancellation to gracefully stop the worker later
		workerCtx := context.WithoutCancel(ctx)

		q.runHandlerWorker(workerCtx, queues)
	})
}

// Stop signals the worker to stop and waits until it finishes or the context expires.
// It waits for the polling loop AND all in-flight job goroutines to complete.
func (q *Queue) Stop(ctx context.Context) error {
	close(q.stopped) // stop signal

	// Try to wait for graceful shutdown
	done := make(chan struct{})

	go func() {
		q.wg.Wait()

		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context done: %w", ctx.Err())
	}
}

// runHandlerWorker is the main polling loop. It maintains a semaphore of size Concurrency.
//
// On every tick it checks how many slots are free and fetches up to that many jobs
// (capped at BatchSize). Each job is dispatched to its own goroutine immediately;
// the goroutine releases its slot when done so the next tick can fill it again.
//
// This means a slow job never blocks faster ones from being picked up: as soon as any
// goroutine finishes, its slot becomes available for new work on the very next poll.
func (q *Queue) runHandlerWorker(ctx context.Context, queues []string) {
	// sem is a counting semaphore. Sending acquires a slot; receiving releases it.
	// Its capacity is the maximum number of concurrently running job goroutines.
	sem := make(chan struct{}, q.config.Poll.Concurrency)

	ticker := time.NewTicker(q.config.Poll.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Force stop (should never happen though)
			return
		case <-q.stopped:
			// Graceful stop — in-flight goroutines are tracked by w.wg and will be
			// awaited by Stop() after this function returns.
			return
		case <-ticker.C:
			// Keep fetching as long as there are free slots and pending work.
			for {
				// How many goroutine slots are currently available?
				freeSlots := cap(sem) - len(sem)
				if freeSlots == 0 {
					break // all slots busy; wait for the next tick
				}

				// Fetch only as many jobs as we have room for, up to BatchSize.
				fetchSize := min(uint(freeSlots), q.config.Poll.BatchSize)

				jobs, err := q.fetchJobs(ctx, queues, fetchSize)
				if err != nil {
					slog.LogAttrs(ctx, slog.LevelError, "Failed to fetch jobs",
						slog.String("component", "floww"),
						slog.String("error", err.Error()),
					)

					break
				}

				if len(jobs) == 0 {
					break // queue is empty; wait for the next tick
				}

				// Dispatch each job to its own goroutine immediately.
				// We already verified there are enough free slots, so the send won't block.
				for i := range jobs {
					sem <- struct{}{} // acquire slot

					job := &jobs[i]

					q.wg.Go(func() {
						defer func() { <-sem }() // release slot when done

						if jobErr := q.handleJob(ctx, job); jobErr != nil {
							slog.LogAttrs(ctx, slog.LevelError, "Failed to handle a job",
								slog.String("component", "floww"),
								slog.String("job_id", job.ID.String()),
								slog.String("error", jobErr.Error()),
							)
						}
					})
				}

				// If the storage returned fewer jobs than we asked for, the queue is
				// drained for now — no point querying again this tick.
				if len(jobs) < int(fetchSize) {
					break
				}

				// Full batch received: loop immediately to fill any remaining free slots.
				select {
				case <-q.stopped:
					return
				default:
				}
			}
		}
	}
}

// fetchJobs fetches up to fetchSize jobs from storage.
func (q *Queue) fetchJobs(
	ctx context.Context,
	queues []string,
	fetchSize uint,
) ([]Job, error) {
	ctx, cancel := context.WithTimeout(ctx, q.config.Processing.DbTimeout)
	defer cancel()

	jobs, err := q.storage.ListActiveJobs(ctx, queues, fetchSize)
	if err != nil {
		return nil, fmt.Errorf("list active jobs from storage: %w", err)
	}

	return jobs, nil
}

// handleJob processes the provided job by handing it to the corresponding handler
// and finishes it depending on the handling result.
func (q *Queue) handleJob(ctx context.Context, job *Job) error {
	handler, exists := q.handlers[job.Queue]
	if !exists {
		slog.LogAttrs(ctx, slog.LevelError, "No handler registered for the queue, a job will be rescheduled",
			slog.String("component", "pqueue"),
			slog.String("queue", job.Queue),
			slog.String("job_id", job.ID.String()),
		)

		// Use a blank handler so handleJobError falls back to the configured default backoff.
		// The job will be retried until MaxAttempts is reached,
		// at which point it moves to the dead-letter queue like any other failure.
		//
		//nolint:exhaustruct // should be empty handler
		return q.handleJobError(
			ctx, &jobHandlerWrapper{}, job, fmt.Errorf("no handler registered for queue '%s'", job.Queue),
		)
	}

	if err := handler.handler(ctx, job); err != nil {
		return q.handleJobError(ctx, handler, job, err)
	}

	return q.completeJob(ctx, job)
}

// completeJob marks a job as completed.
func (q *Queue) completeJob(ctx context.Context, job *Job) error {
	ctx, cancel := context.WithTimeout(ctx, q.config.Processing.DbTimeout)
	defer cancel()

	if err := q.storage.CompleteJob(ctx, job.ID); err != nil {
		return fmt.Errorf("complete job in storage: %w", err)
	}

	return nil
}

// handleJobError handles job processing errors with backoff.
func (q *Queue) handleJobError(
	ctx context.Context,
	jobHandler *jobHandlerWrapper,
	job *Job,
	handleErr error,
) error {
	// Do not retry unrecoverable errors and jobs that reach their limit
	if IsUnrecoverable(handleErr) || job.Attempts >= job.MaxAttempts {
		return q.failJob(ctx, job, handleErr)
	}

	ctx, cancel := context.WithTimeout(ctx, q.config.Processing.DbTimeout)
	defer cancel()

	errMsg := handleErr.Error()

	// Calculate backoff duration
	backoff := jobHandler.calculateBackoff(job.Attempts, q.config.Processing.DefaultBackoff)
	nextSchedule := time.Now().Add(backoff)

	// Update a job with new schedule
	if err := q.storage.ReScheduleJob(ctx, job.ID, nextSchedule, errMsg); err != nil {
		return fmt.Errorf("reschedule job in storage: %w", err)
	}

	return nil
}

// failJob immediately fails a job moving it to the dead letter queue.
func (q *Queue) failJob(ctx context.Context, job *Job, handleErr error) error {
	ctx, cancel := context.WithTimeout(ctx, q.config.Processing.DbTimeout)
	defer cancel()

	errMsg := handleErr.Error()

	if err := q.storage.FailJob(ctx, job.ID, errMsg); err != nil {
		return fmt.Errorf("fail job in storage: %w", err)
	}

	return nil
}

// CleanColdJobs removes completed jobs.
//
// Most of the times the function should be called from some sort of a cron job.
func (q *Queue) CleanColdJobs(ctx context.Context) error {
	// Keep jobs if there is no retention
	if q.config.ColdCleanup.RetentionInterval <= 0 {
		return nil
	}

	slog.LogAttrs(ctx, slog.LevelInfo, "Running cold jobs cleaner...",
		slog.String("component", "pqueue"),
	)

	ctx, cancel := context.WithTimeout(ctx, q.config.ColdCleanup.DbTimeout)
	defer cancel()

	cutoffDate := time.Now().Add(-q.config.ColdCleanup.RetentionInterval)

	rowsAffected, err := q.storage.DeleteColdJobs(ctx, cutoffDate, q.config.ColdCleanup.CleanupBatchSize)
	if err != nil {
		return fmt.Errorf("delete cold jobs from storage: %w", err)
	}

	if rowsAffected == 0 {
		slog.LogAttrs(ctx, slog.LevelInfo, "No cold jobs to be cleaned up",
			slog.String("component", "pqueue"),
		)

		return nil
	}

	slog.LogAttrs(ctx, slog.LevelInfo, "Cold jobs cleaned up successfully",
		slog.String("component", "pqueue"),
		slog.Uint64("deleted_job_count", uint64(rowsAffected)),
	)

	return nil
}

// CleanDeadJobs removes failed jobs.
//
// Most of the times the function should be called from some sort of a cron job.
func (q *Queue) CleanDeadJobs(ctx context.Context) error {
	// Keep jobs if there is no retention
	if q.config.DeadCleanup.RetentionInterval <= 0 {
		return nil
	}

	slog.LogAttrs(ctx, slog.LevelInfo, "Running dead jobs cleaner...",
		slog.String("component", "pqueue"),
	)

	ctx, cancel := context.WithTimeout(ctx, q.config.DeadCleanup.DbTimeout)
	defer cancel()

	cutoffDate := time.Now().Add(-q.config.DeadCleanup.RetentionInterval)

	rowsAffected, err := q.storage.DeleteDeadJobs(ctx, cutoffDate, q.config.DeadCleanup.CleanupBatchSize)
	if err != nil {
		return fmt.Errorf("delete dead jobs from storage: %w", err)
	}

	if rowsAffected == 0 {
		slog.LogAttrs(ctx, slog.LevelInfo, "No dead jobs to be cleaned up",
			slog.String("component", "pqueue"),
		)

		return nil
	}

	slog.LogAttrs(ctx, slog.LevelInfo, "Dead jobs cleaned up successfully",
		slog.String("component", "pqueue"),
		slog.Uint64("deleted_job_count", uint64(rowsAffected)),
	)

	return nil
}
