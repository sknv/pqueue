# pqueue

A lightweight, PostgreSQL-backed priority job queue for Go. Jobs are stored durably in Postgres and processed concurrently by worker goroutines. The queue supports priorities, scheduled execution, automatic retries with configurable backoff, stuck-job recovery, batch enqueueing, idempotent enqueueing and partitioned storage with O(1) cleanup via partition drops.

Uses a short-polling mechanism for fetching updates, making it compatible with connection poolers in transaction mode.

## Features

- **Priority scheduling** — higher-priority jobs are always picked up first
- **Delayed execution** — schedule jobs to run at any future time
- **Automatic retries** — failed jobs are rescheduled with configurable backoff; exhausted jobs move to a dead-letter state
- **Stuck-job recovery** — jobs that exceed their timeout are automatically re-queued
- **Batch enqueueing** — insert many jobs in a single database round-trip, optionally inside a transaction
- **Idempotent enqueueing** — provide a custom job id via `WithJobID`; re-enqueueing returns the existing job without modification
- **Configurable concurrency** — control how many jobs run in parallel
- **Pluggable encoder** — JSON by default; swap in any `Encoder` implementation
- **Storage interface** — ship with the included Postgres backend or bring your own
- **Partitioned storage** — jobs are split into hot, cold, and dead partitions by status; cold and dead are sub-partitioned by week for instant cleanup

## Requirements

- Go 1.26+
- PostgreSQL with the `uuidv7()` function available
- [`pgx/v5`](https://github.com/jackc/pgx)

## Installation

```bash
go get github.com/sknv/pqueue
```

## Database Setup

Apply the migration file to your database to create the `pqueue_jobs` partitioned table, supporting indexes, helper trigger, and partition management functions:

```bash
psql -d your_database -f init_pqueue.up.sql
```

The migration creates:

- `pqueue_jobs` table partitioned by `LIST(status)` with all job fields
- Three top-level partitions: `pqueue_jobs_hot` (pending/running), `pqueue_jobs_cold` (completed), `pqueue_jobs_dead` (failed)
- `pqueue_jobs_cold` and `pqueue_jobs_dead` are further sub-partitioned by `RANGE(created_at)` into weekly partitions
- Weekly sub-partitions for the current week plus 4 forward weeks (cold and dead only)
- `DEFAULT` sub-partitions for cold and dead as a safety net for dates outside created weekly partitions
- Partial indexes on `pqueue_jobs_hot` optimised for pending and running queries
- An `updated_at` trigger that keeps the timestamp current automatically
- `pqueue_create_weekly_partitions(forward_weeks)` — pre-creates future weekly partitions (cold and dead)
- `pqueue_drop_old_partitions(group_name, cutoff_date)` — drops old weekly partitions by group (cold or dead)
- `pqueue_enqueue_idempotent_job(...)` — idempotent insert: returns the existing row if a job with the same id already exists in any partition; used automatically when `WithJobID` is provided

## Quick Start

Take a look in the `example` folder.

## Enqueueing Jobs

### Single job

```go
job, err := q.Enqueue(ctx, db, "my-queue", payload,
    pqueue.WithJobPriority(10),
    pqueue.WithJobMaxAttempts(5),
    pqueue.WithJobStuckTimeout(2*time.Minute),
    pqueue.WithJobScheduledAt(time.Now().Add(1*time.Hour)),
)
```

Use `WithJobID` to provide a custom job id and enable idempotent enqueueing. Re-enqueueing a job with the same id returns the existing row without modification, regardless of its current status. Without `WithJobID`, a uuid v7 is auto-generated and the fast (non-idempotent) insert path is used:

```go
job, err := q.Enqueue(ctx, db, "my-queue", payload,
    pqueue.WithJobID(id),
)
```

### Batch (single round-trip)

```go
jobs, err := q.EnqueueBatch(ctx, db, []pqueue.BatchJob{
    {
        Queue:   "emails",
        Payload: emailPayload,
        Opts: []pqueue.JobOption{
            pqueue.WithJobPriority(5),
        },
    },
    {
        Queue:   "notifications",
        Payload: notifPayload,
    },
})
```

Pass a `pgx.Batch` as the `batcher` argument to make the entire batch atomic with your own transaction.

### Job options

| Option | Default | Description |
|---|---|---|
| `WithJobID(id)` | auto-generated uuid v7 | Custom job id; enables idempotent enqueueing |
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
    Partitions: pqueue.PartitionConfig{ // partition pre-creation
        ForwardWeeks: 4,
    },
    ColdCleanup: pqueue.PartitionCleanupConfig{ // completed jobs
        DbTimeout:         30 * time.Second,
        RetentionInterval: 7 * 24 * time.Hour,
    },
    DeadCleanup: pqueue.PartitionCleanupConfig{ // failed jobs
        DbTimeout:         30 * time.Second,
        RetentionInterval: 90 * 24 * time.Hour,
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
| `Partitions.ForwardWeeks` | `4` |
| `ColdCleanup.RetentionInterval` | `7 days` |
| `DeadCleanup.RetentionInterval` | `90 days` |
| `*Cleanup.DbTimeout` | `30s` |

## Partitioning

The `pqueue_jobs` table uses PostgreSQL declarative partitioning:

```
pqueue_jobs (PARTITION BY LIST (status))
├── pqueue_jobs_hot   FOR VALUES IN ('pending', 'running')   (plain table — no sub-partitioning)
├── pqueue_jobs_cold   FOR VALUES IN ('completed')           → PARTITION BY RANGE (created_at)
│   ├── pqueue_jobs_cold_20260901   (week starting Sep 1)
│   ├── pqueue_jobs_cold_20260908   (week starting Sep 8)
│   ├── ...
│   └── pqueue_jobs_cold_default    (catch-all safety net)
└── pqueue_jobs_dead   FOR VALUES IN ('failed')              → PARTITION BY RANGE (created_at)
    ├── pqueue_jobs_dead_20260901
    ├── ...
    └── pqueue_jobs_dead_default
```

- **Hot partition** holds `pending` and `running` jobs — the small, actively-scanned set (no sub-partitioning; jobs move in and out constantly)
- **Cold partition** holds `completed` jobs — sub-partitioned by week, dropped once retention expires
- **Dead partition** holds `failed` jobs — sub-partitioned by week, dropped once retention expires

Weekly sub-partitions are named `pqueue_jobs_{group}_{YYYYMMDD}` where `YYYYMMDD` is the Monday of that week. PostgreSQL's partition pruning ensures queries like `WHERE status = 'pending'` only scan the hot partition, and `created_at` ranges prune to specific weekly sub-partitions in cold and dead.

### Pre-creating partitions

PostgreSQL does not auto-create partitions. Call `CreatePartitions` from a cron job or periodic goroutine to ensure future weekly partitions always exist:

```go
if err := q.CreatePartitions(ctx); err != nil {
    log.Printf("partition creation failed: %v", err)
}
```

This calls the `pqueue_create_weekly_partitions(forward_weeks)` SQL function, which creates weekly sub-partitions for the cold and dead groups for the current week plus `ForwardWeeks` weeks ahead.

### Cleanup (partition drops)

Completed and failed jobs accumulate in their respective partitions. Instead of row-by-row deletion, call the partition drop methods to instantly drop old weekly partitions via `DROP TABLE` (O(1) per partition):

```go
// Drop completed-job partitions older than the configured retention interval
if err := q.DropOldColdPartitions(ctx); err != nil {
    log.Printf("cold partition cleanup failed: %v", err)
}

// Drop failed-job partitions older than the configured retention interval
if err := q.DropOldDeadPartitions(ctx); err != nil {
    log.Printf("dead partition cleanup failed: %v", err)
}
```

Setting `RetentionInterval` to `0` or a negative value disables cleanup entirely.

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
