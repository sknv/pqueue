-- Function for automatic updated_at timestamp update.
CREATE OR REPLACE FUNCTION pqueue_set_updated_at()
RETURNS trigger AS $$
BEGIN
  IF NEW IS DISTINCT FROM OLD THEN
    NEW.updated_at = now();
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--
-- Job queue (partitioned by status -> created_at)
--
-- Partition structure:
--   pqueue_jobs (PARTITION BY LIST (status))
--   ├── pqueue_jobs_hot   ('pending', 'running')
--   ├── pqueue_jobs_cold  ('completed')          -> PARTITION BY RANGE (created_at) — weekly
--   └── pqueue_jobs_dead  ('failed')             -> PARTITION BY RANGE (created_at) — weekly
--
-- Cold and dead groups are sub-partitioned by week (Monday-based).
--

CREATE TYPE pqueue_job_status AS ENUM (
  'pending', 'running', 'completed', 'failed'
);

CREATE TABLE pqueue_jobs (
  id                   uuid              NOT NULL DEFAULT uuidv7(),
  queue                text              NOT NULL,
  payload              bytea,
  status               pqueue_job_status NOT NULL DEFAULT 'pending',
  priority             int               NOT NULL DEFAULT 0,
  attempts             int               NOT NULL DEFAULT 0,
  max_attempts         int               NOT NULL DEFAULT 1,
  stuck_timeout_millis bigint            NOT NULL,
  scheduled_at         timestamptz       NOT NULL DEFAULT now(),
  run_at               timestamptz,
  stuck_at             timestamptz,
  completed_at         timestamptz,
  error_message        text,
  created_at           timestamptz       NOT NULL DEFAULT now(),
  updated_at           timestamptz       NOT NULL DEFAULT now(),

  PRIMARY KEY (id, status)
) PARTITION BY LIST (status);

COMMENT ON TABLE pqueue_jobs IS 'Job queue';
COMMENT ON COLUMN pqueue_jobs.queue IS 'Queue name';
COMMENT ON COLUMN pqueue_jobs.payload IS 'Job payload';
COMMENT ON COLUMN pqueue_jobs.priority IS 'Execution priority: higher value means higher priority';
COMMENT ON COLUMN pqueue_jobs.attempts IS 'Current number of job execution attempts';
COMMENT ON COLUMN pqueue_jobs.max_attempts IS 'Maximum number of job execution attempts';
COMMENT ON COLUMN pqueue_jobs.stuck_timeout_millis IS 'Time in milliseconds after which a job is considered stuck';
COMMENT ON COLUMN pqueue_jobs.scheduled_at IS 'Time when the job is scheduled to run';
COMMENT ON COLUMN pqueue_jobs.run_at IS 'Time when the job was started';
COMMENT ON COLUMN pqueue_jobs.stuck_at IS 'Time after which the job is considered stuck';
COMMENT ON COLUMN pqueue_jobs.completed_at IS 'Time when the job was completed';
COMMENT ON COLUMN pqueue_jobs.error_message IS 'Last error message';

-- Trigger for automatic updated_at timestamp update.
CREATE TRIGGER trg__pqueue_jobs__updated_at
BEFORE UPDATE ON pqueue_jobs
FOR EACH ROW
EXECUTE FUNCTION pqueue_set_updated_at();

--
-- Top-level partitions.
--

CREATE TABLE pqueue_jobs_hot
  PARTITION OF pqueue_jobs
  FOR VALUES IN ('pending', 'running');

CREATE TABLE pqueue_jobs_cold
  PARTITION OF pqueue_jobs
  FOR VALUES IN ('completed')
  PARTITION BY RANGE (created_at);

CREATE TABLE pqueue_jobs_dead
  PARTITION OF pqueue_jobs
  FOR VALUES IN ('failed')
  PARTITION BY RANGE (created_at);

-- DEFAULT partitions — safety net in case a weekly partition has not been created yet.

CREATE TABLE pqueue_jobs_cold_default
  PARTITION OF pqueue_jobs_cold DEFAULT;

CREATE TABLE pqueue_jobs_dead_default
  PARTITION OF pqueue_jobs_dead DEFAULT;

--
-- Indexes.
--

-- Main index for fetching jobs to execute.
CREATE INDEX IF NOT EXISTS idx__pqueue_jobs_hot__pending_worker
ON pqueue_jobs_hot (priority DESC, scheduled_at)
WHERE status = 'pending';

-- Index for fetching stuck jobs.
CREATE INDEX IF NOT EXISTS idx__pqueue_jobs_hot__stuck_worker
ON pqueue_jobs_hot (stuck_at)
WHERE status = 'running';

--
-- Partition management functions.
--

-- pqueue_partition_week_start(date) -> returns the Monday of the week for the given date.
CREATE OR REPLACE FUNCTION pqueue_partition_week_start(d date)
RETURNS date AS $$
BEGIN
  RETURN d - (extract(dow FROM d)::int - 1);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- pqueue_create_weekly_partitions(forward_weeks int DEFAULT 4)
-- Creates weekly partitions for two groups (cold, dead)
-- for the current week plus forward_weeks weeks ahead.
CREATE OR REPLACE FUNCTION pqueue_create_weekly_partitions(forward_weeks int DEFAULT 4)
RETURNS void AS $$
DECLARE
  group_names text[] := ARRAY['cold', 'dead'];
  group_name  text;
  week_start  date;
  part_name   text;
  week_idx    int;
BEGIN
  FOR week_idx IN 0..forward_weeks LOOP
    week_start := pqueue_partition_week_start(current_date) + (week_idx * 7);

    FOREACH group_name IN ARRAY group_names LOOP
      part_name := format('pqueue_jobs_%s_%s', group_name, to_char(week_start, 'YYYYMMDD'));

      EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF pqueue_jobs_%I '
        'FOR VALUES FROM (%L) TO (%L)',
        part_name, group_name, week_start, week_start + 7
      );
    END LOOP;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- pqueue_drop_old_partitions(group_name text, cutoff_date date)
-- Drops all weekly partitions of the given group (cold, dead)
-- whose entire week precedes cutoff_date.
-- Returns the number of dropped partitions.
CREATE OR REPLACE FUNCTION pqueue_drop_old_partitions(group_name text, cutoff_date date)
RETURNS int AS $$
DECLARE
  parent_oid  oid;
  child_rec   RECORD;
  part_date   date;
  dropped     int := 0;
BEGIN
  parent_oid := format('pqueue_jobs_%s', group_name)::regclass::oid;

  -- Delete records from the DEFAULT partition.
  -- The DEFAULT partition is a safety net for dates not covered by weekly partitions;
  -- it cannot be dropped via DROP TABLE, so we clean it row by row.
  EXECUTE format('DELETE FROM pqueue_jobs_%I_default', group_name);

  FOR child_rec IN
    SELECT c.oid::regclass::text AS child_name
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    WHERE i.inhparent = parent_oid
      AND c.relname !~ '_default$'
  LOOP
    BEGIN
      part_date := substring(child_rec.child_name FROM '([0-9]{8})$')::date;

      -- Drop only if the entire week is before cutoff_date.
      IF part_date + 7 <= cutoff_date THEN
        EXECUTE format('DROP TABLE IF EXISTS %I', child_rec.child_name);
        dropped := dropped + 1;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      -- Failed to parse date — skip.
      CONTINUE;
    END;
  END LOOP;

  RETURN dropped;
END;
$$ LANGUAGE plpgsql;

-- pqueue_enqueue_idempotent_job(job_id uuid, job_queue text, job_payload bytea,
--   job_priority int, job_max_attempts int, job_stuck_timeout_millis bigint,
--   job_scheduled_at timestamptz)
-- Idempotent insert: returns the existing row if a job with the same id
-- already exists in any partition (global idempotency on id).
-- Race-safe: two concurrent enqueues of the same id both miss the SELECT,
-- both try to INSERT as 'pending' — ON CONFLICT (id, status) catches the second.
CREATE OR REPLACE FUNCTION pqueue_enqueue_idempotent_job(
  job_id                   uuid,
  job_queue                text,
  job_payload              bytea,
  job_priority             int,
  job_max_attempts         int,
  job_stuck_timeout_millis bigint,
  job_scheduled_at         timestamptz
) RETURNS pqueue_jobs AS $$
DECLARE
  result pqueue_jobs%ROWTYPE;
BEGIN
  SELECT * INTO result FROM pqueue_jobs WHERE id = job_id LIMIT 1;
  IF FOUND THEN
    RETURN result;
  END IF;

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
  ON CONFLICT (id, status) DO UPDATE SET id = pqueue_jobs.id
  RETURNING * INTO result;

  RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Create initial partitions (current week plus 4 weeks ahead).
SELECT pqueue_create_weekly_partitions(4);
