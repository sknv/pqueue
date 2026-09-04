-- Drop partition management and enqueue functions.
DROP FUNCTION IF EXISTS pqueue_enqueue_idempotent_job(uuid, text, bytea, int, int, bigint, timestamptz);
DROP FUNCTION IF EXISTS pqueue_drop_old_partitions(text, date);
DROP FUNCTION IF EXISTS pqueue_create_weekly_partitions(int);
DROP FUNCTION IF EXISTS pqueue_partition_week_start(date);

-- Drop the table (cascades to all partitions).
DROP TABLE IF EXISTS pqueue_jobs CASCADE;
DROP TYPE IF EXISTS pqueue_job_status;

-- Drop the trigger function.
DROP FUNCTION IF EXISTS pqueue_set_updated_at;
