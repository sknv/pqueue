-- Функция для автоматического обновления updated_at таймстемпа
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
-- Очередь задач
--

CREATE TYPE pqueue_job_status AS ENUM (
  'pending', 'running', 'completed', 'failed'
);

CREATE TABLE IF NOT EXISTS pqueue_jobs (
  id                   uuid              PRIMARY KEY DEFAULT uuidv7(),
  idempotency_key      uuid              NOT NULL CONSTRAINT chk__pqueue_jobs__unique_idempotency_key UNIQUE,
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
  updated_at           timestamptz       NOT NULL DEFAULT now()
);

COMMENT ON TABLE pqueue_jobs IS 'Очередь задач';
COMMENT ON COLUMN pqueue_jobs.idempotency_key IS 'Ключ идемпотентности';
COMMENT ON COLUMN pqueue_jobs.queue IS 'Имя очереди';
COMMENT ON COLUMN pqueue_jobs.payload IS 'Нагрузка для задачи';
COMMENT ON COLUMN pqueue_jobs.priority IS 'Приоритет выполнения: чем больше, тем выше приоритет';
COMMENT ON COLUMN pqueue_jobs.attempts IS 'Текущее число попыток запуска задачи';
COMMENT ON COLUMN pqueue_jobs.max_attempts IS 'Максимальное число попыток запуска задачи';
COMMENT ON COLUMN pqueue_jobs.stuck_timeout_millis IS 'Промежуток времени в миллисекундах, после которого задача считается зависшей';
COMMENT ON COLUMN pqueue_jobs.scheduled_at IS 'Время, в которое запланирован запуск задачи';
COMMENT ON COLUMN pqueue_jobs.run_at IS 'Время, в которое задача была запущена';
COMMENT ON COLUMN pqueue_jobs.stuck_at IS 'Время, после которого задача считается зависшей';
COMMENT ON COLUMN pqueue_jobs.completed_at IS 'Время, в которое задача была завершена';
COMMENT ON COLUMN pqueue_jobs.error_message IS 'Последнее сообщение об ошибке';

-- Триггер для автоматического обновления updated_at таймстемпа
CREATE TRIGGER trg__pqueue_jobs__updated_at
BEFORE UPDATE ON pqueue_jobs
FOR EACH ROW
EXECUTE FUNCTION pqueue_set_updated_at();

-- Основные индексы для получения задач на выполнение
CREATE INDEX IF NOT EXISTS idx__pqueue_jobs__pending_worker
ON pqueue_jobs (priority DESC, scheduled_at)
WHERE status = 'pending';

CREATE INDEX idx__pqueue_jobs__pending_queue_worker
ON pqueue_jobs (queue, priority DESC, scheduled_at)
WHERE status = 'pending';

-- Индексы для получения зависших задач
CREATE INDEX IF NOT EXISTS idx__pqueue_jobs__stuck_worker
ON pqueue_jobs (stuck_at)
WHERE status = 'running';

CREATE INDEX idx__pqueue_jobs__stuck_queue_worker
ON pqueue_jobs (queue, stuck_at)
WHERE status = 'running';

-- Индекс для очистки завершенных задач
CREATE INDEX IF NOT EXISTS idx__pqueue_jobs__completed_cleaner
ON pqueue_jobs (created_at)
WHERE status = 'completed';

-- Индекс для очистки мертвых задач
CREATE INDEX IF NOT EXISTS idx__pqueue_jobs__failed_cleaner
ON pqueue_jobs (created_at)
WHERE status = 'failed';
