# pqueue

A lightweight, PostgreSQL-backed priority job queue for Go. Jobs are stored durably in Postgres and processed concurrently by worker goroutines. The queue supports priorities, scheduled execution, automatic retries with configurable backoff, stuck-job recovery, idempotent enqueueing, and periodic cleanup of completed and dead jobs.

Uses a short-polling mechanism for fetching updates, making it compatible with connection poolers in transaction mode.

## Features

- **Priority scheduling** — higher-priority jobs are always picked up first
- **Delayed execution** — schedule jobs to run at any future time
- **Automatic retries** — failed jobs are rescheduled with configurable backoff; exhausted jobs move to a dead-letter state
- **Stuck-job recovery** — jobs that exceed their timeout are automatically re-queued
- **Idempotent enqueueing** — duplicate submissions with the same idempotency key are silently deduplicated
- **Batch enqueueing** — insert many jobs in a single database round-trip, optionally inside a transaction
- **Configurable concurrency** — control how many jobs run in parallel
- **Pluggable encoder** — JSON by default; swap in any `Encoder` implementation
- **Storage interface** — ship with the included Postgres backend or bring your own
- **Automatic cleanup** — built-in helpers to purge old completed and failed jobs

## Requirements

- Go 1.26+
- PostgreSQL with the `uuidv7()` function available
- [`pgx/v5`](https://github.com/jackc/pgx)

## Installation

```bash
go get github.com/sknv/pqueue
```

## Database Setup

Apply the migration file to your database to create the `pqueue_jobs` table, supporting indexes, and helper trigger:

```bash
psql -d your_database -f init_pqueue.up.sql
```

The migration creates:

- `pqueue_jobs` table with all job fields
- Partial indexes optimised for pending, running, completed, and failed queries
- An `updated_at` trigger that keeps the timestamp current automatically

## Quick Start

Take a look in the `example` folder.

## Enqueueing Jobs

### Single job

```go
job, err := q.Enqueue(ctx, db, "my-queue", idempotencyKey, payload,
    pqueue.WithJobPriority(10),
    pqueue.WithJobMaxAttempts(5),
    pqueue.WithJobStuckTimeout(2*time.Minute),
    pqueue.WithJobScheduledAt(time.Now().Add(1*time.Hour)),
)
```

### Batch (single round-trip)

```go
jobs, err := q.EnqueueBatch(ctx, db, []pqueue.BatchJob{
    {
        Queue:          "emails",
        IdempotencyKey: uuid.New(),
        Payload:        emailPayload,
        Opts: []pqueue.JobOption{
            pqueue.WithJobPriority(5),
        },
    },
    {
        Queue:          "notifications",
        IdempotencyKey: uuid.New(),
        Payload:        notifPayload,
    },
})
```

Pass a `pgx.Batch` as the `batcher` argument to make the entire batch atomic with your own transaction.

### Job options

| Option | Default | Description |
|---|---|---|
| `WithJobPriority(n)` | `0` | Higher values are processed first |
| `WithJobMaxAttempts(n)` | `1` | Total attempts before the job is marked failed |
| `WithJobStuckTimeout(d)` | `5m` | How long a running job may be silent before it is re-queued |
| `WithJobScheduledAt(t)` | `now()` | Earliest time the job will be picked up |

## Registering Handlers

Handlers are keyed by the queue name. One handler per queue.

```go
q.RegisterHandler("resize-images", func(ctx context.Context, job *pqueue.Job) error {
    // decode payload, do work, return nil on success
    return doWork(job.Payload)
}, pqueue.WithJobHandlerBackoffCalculator(func(attempt uint) time.Duration {
    // exponential backoff
    return time.Duration(attempt) * 30 * time.Second
}))
```

Return `pqueue.Unrecoverable(err)` to skip all remaining retries and move the job directly to the failed state:

```go
q.RegisterHandler("payments", func(ctx context.Context, job *pqueue.Job) error {
    if err := processPayment(job); err != nil {
        if isClientError(err) {
            return pqueue.Unrecoverable(err) // no point retrying
        }

        return err // will be retried
    }
    return nil
})
```

## Starting and Stopping

```go
// Process all queues
q.Start(ctx)

// Process only specific queues
q.Start(ctx, "emails", "notifications")

// Graceful shutdown — waits up to the deadline for in-flight jobs to finish
shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := q.Stop(shutdownCtx); err != nil {
    log.Printf("Shutdown timed out: %v", err)
}
```

## Configuration

```go
cfg := &pqueue.QueueConfig{
    Poll: pqueue.PollConfig{
        BatchSize:    20,             // jobs claimed per poll cycle
        Concurrency:  20,             // max in-flight goroutines
        PollInterval: 500*time.Millisecond,
    },
    Processing: pqueue.ProcessingConfig{
        DbTimeout:      10 * time.Second,
        DefaultBackoff: 30 * time.Second,
    },
    ColdCleanup: pqueue.CleanupConfig{ // completed jobs
        DbTimeout:         30 * time.Second,
        RetentionInterval: 7 * 24 * time.Hour,
        CleanupBatchSize:  10_000,
    },
    DeadCleanup: pqueue.CleanupConfig{ // failed jobs
        DbTimeout:         30 * time.Second,
        RetentionInterval: 90 * 24 * time.Hour,
        CleanupBatchSize:  10_000,
    },
}

q := pqueue.NewQueue(storage, cfg)
```

Pass `nil` as the config to use `pqueue.DefaultConfig()`.

### Default values

| Setting | Default |
|---|---|
| `Poll.BatchSize` | `10` |
| `Poll.Concurrency` | `10` |
| `Poll.PollInterval` | `1s` |
| `Processing.DbTimeout` | `10s` |
| `Processing.DefaultBackoff` | `30s` |
| `ColdCleanup.RetentionInterval` | `7 days` |
| `DeadCleanup.RetentionInterval` | `90 days` |
| `*Cleanup.CleanupBatchSize` | `10 000` |

## Cleanup

Completed and failed jobs accumulate over time. Call the cleanup methods from a cron job or a periodic goroutine. Setting `RetentionInterval` to `0` or a negative value disables cleanup entirely.

```go
// Remove completed jobs older than the configured retention interval
if err := q.CleanColdJobs(ctx); err != nil {
    log.Printf("cold job cleanup failed: %v", err)
}

// Remove failed jobs older than the configured retention interval
if err := q.CleanDeadJobs(ctx); err != nil {
    log.Printf("dead job cleanup failed: %v", err)
}
```

## Custom Encoder

The default encoder is JSON. Swap it out via a queue option:

```go
q := pqueue.NewQueue(storage, cfg,
    pqueue.WithQueueEncoder(myMsgpackEncoder{}),
)
```

Any type that satisfies the `Encoder` interface works:

```go
type Encoder interface {
    Encode(v any) ([]byte, error)
    Decode(data []byte, v any) error
}
```

## Custom Storage

Implement the `Storage` interface to use a different database backend.

## Job Lifecycle

```
Enqueue ──► pending ──► running ──► completed
                │           │
                │     (stuck timeout)
                │           │
                └◄──────────┘  (rescheduled, attempts < maxAttempts)
                │
                └──► failed   (attempts >= maxAttempts OR Unrecoverable error)
```

## License

MIT
