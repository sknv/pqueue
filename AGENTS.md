# AGENTS.md

## Project

Go library (not an application) providing a PostgreSQL-backed priority job queue. Module path: `github.com/sknv/pqueue`. Requires Go 1.27+ and PostgreSQL with the `uuidv7()` function.

## Commands

```bash
task vendor     # go mod tidy && go mod vendor && go mod verify
task lint       # golangci-lint run (auto-fixes via issues.fix: true)
task test       # go test -v -failfast --tags=integration ./...
```

Lint uses golangci-lint v2.13.2 (install via `task tools`). There are currently no test files in the repo.

Build/vet without Taskfile: `go build ./... && go vet ./...`

## Architecture

- **`job_queue.go`** — Public API: `Queue`, `Job`, `JobStatus`, config types, worker loop, enqueue/batch/cleanup methods. This is the main entry point.
- **`storage.go`** — `Storage` interface that backends must implement.
- **`storage/postgres/postgres_storage.go`** — sole concrete backend. All raw SQL lives here as unexported string constants (`_insertJobSQL`, `_fetchJobsSQL`, etc.).
- **`queryer.go`** / **`encoder.go`** / **`json_encoder.go`** / **`unrecoverable_error.go`** — small supporting interfaces and types.
- **`example/`** — standalone module (`module example`) with its own `go.mod` that replaces `github.com/sknv/pqueue => ../`. Run from within `example/` directory.

No ORM. SQL is hand-written and executed via `pgx/v5` with `pgxpool` connection pooling.

Job insertion goes either through plain SQL query or PL/pgSQL function depending on whether `WithJobID` was provided:

- fast path: plain INSERT ... RETURNING, no dedup check. Used when `WithJobID` is not provided (auto-generated uuid v7).
- `pqueue_enqueue_idempotent_job` — idempotent path: SELECT across all partitions by `id`, then INSERT with `ON CONFLICT (id, status)` as race fallback. Used automatically when `WithJobID` is provided.

## Partitioning

`pqueue_jobs` is partitioned `LIST(status)`. The `cold` and `dead` groups are further sub-partitioned `RANGE(created_at)` weekly; `hot` is a plain partition (no sub-partitioning):

- `pqueue_jobs_hot` — `pending`/`running` (plain table, no weekly sub-partitions)
- `pqueue_jobs_cold` — `completed` (weekly sub-partitions, dropped after retention)
- `pqueue_jobs_dead` — `failed` (weekly sub-partitions, dropped after retention)

Weekly sub-partitions: `pqueue_jobs_{group}_{YYYYMMDD}` (Monday-based). `DEFAULT` sub-partitions exist for cold and dead as safety nets. PostgreSQL does not auto-create partitions — `CreatePartitions` must be called periodically.

The PK is `(id, status)` — required because `status` is a partition key.

Row movement: `pending <-> running` stays within the hot partition. `running -> completed` and `running -> failed` move rows across top-level partitions (acceptable — terminal transitions).

Cleanup is `DROP TABLE` (O(1)) via `DropOldColdPartitions` / `DropOldDeadPartitions`, not row-by-row `DELETE`.

## Style

- golangci-lint: all linters enabled except a disabled list (see `.golangci.yml`). `revive` exported/package-comments rules are disabled — missing doc comments on exports will not fail lint.
- Formatters: `gci`, `gofmt`, `gofumpt`, `goimports`, `golines` (max line length 120). Import ordering: std -> third-party -> `github.com/sknv/pqueue`.
- `nolint` directives are used sparingly with explanations (e.g. `//nolint:mnd // default values`).
- No comments in code unless they explain non-obvious logic.

## Migrations

Single migration pair in `migrations/`:
- `init_pqueue.up.sql` — creates enum, partitioned table, indexes, trigger, partition management functions, initial weekly partitions.
- `init_pqueue.down.sql` — drops everything in correct order.
