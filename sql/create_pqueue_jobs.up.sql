CREATE TYPE pqueue_job_status AS ENUM (
  'pending', 'running', 'completed', 'failed'
);

CREATE TABLE IF NOT EXISTS pqueue_jobs
(
  id                   uuid              NOT NULL DEFAULT gen_random_uuid(), -- предпочтительно uuid v7
  queue                text              NOT NULL,
  payload              bytea             NOT NULL,
  status               pqueue_job_status NOT NULL DEFAULT 'pending',
  priority             int               NOT NULL DEFAULT 0,
  attempts             int               NOT NULL DEFAULT 0,
  max_attempts         int               NOT NULL,
  stuck_timeout_millis bigint            NOT NULL,
  scheduled_at         timestamptz       NOT NULL DEFAULT now(),
  run_at               timestamptz,
  completed_at         timestamptz,
  error_message        text,
  created_at           timestamptz       NOT NULL DEFAULT now(),
  updated_at           timestamptz       NOT NULL DEFAULT now(),

  PRIMARY KEY (id, status)
) PARTITION BY LIST (status);

COMMENT ON TABLE pqueue_jobs IS 'Очередь задач';
COMMENT ON COLUMN pqueue_jobs.queue IS 'Имя очереди';
COMMENT ON COLUMN pqueue_jobs.payload IS 'Нагрузка для задачи';
COMMENT ON COLUMN pqueue_jobs.priority IS 'Приоритет выполнения: чем больше, тем выше приоритет';
COMMENT ON COLUMN pqueue_jobs.attempts IS 'Текущее число попыток запуска задачи';
COMMENT ON COLUMN pqueue_jobs.max_attempts IS 'Максимальное число попыток запуска задачи';
COMMENT ON COLUMN pqueue_jobs.stuck_timeout_millis IS 'Время в миллисекундах, после которого задача считается зависшей';
COMMENT ON COLUMN pqueue_jobs.scheduled_at IS 'Время в которое запланирован запуск задачи';
COMMENT ON COLUMN pqueue_jobs.run_at IS 'Время в которое задача была запущена';
COMMENT ON COLUMN pqueue_jobs.completed_at IS 'Время в которое задача была завершена';
COMMENT ON COLUMN pqueue_jobs.error_message IS 'Последнее сообщение об ошибке';

-- Создаем горячую, холодную и партиции для мертвых сообщений

CREATE TABLE IF NOT EXISTS pqueue_jobs_hot PARTITION OF pqueue_jobs FOR VALUES IN ('pending', 'running');

CREATE TABLE IF NOT EXISTS pqueue_jobs_cold PARTITION OF pqueue_jobs FOR VALUES IN ('completed');

CREATE TABLE IF NOT EXISTS pqueue_jobs_dead PARTITION OF pqueue_jobs FOR VALUES IN ('failed');

-- Индексы для горячей партиции
CREATE INDEX IF NOT EXISTS idx__pqueue_jobs_hot__worker ON pqueue_jobs_hot (priority DESC, scheduled_at);

-- Индексы для холодной партиции
CREATE INDEX IF NOT EXISTS idx__pqueue_jobs_cold__cleaner ON pqueue_jobs_cold (created_at);

-- Индексы для партиции c мертвыми сообщениями
CREATE INDEX IF NOT EXISTS idx__pqueue_jobs_dead__cleaner ON pqueue_jobs_dead (created_at);
