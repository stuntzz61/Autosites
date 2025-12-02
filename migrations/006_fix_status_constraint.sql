-- =====================================================
-- Миграция: Исправление constraint на статус заявок
-- =====================================================

-- Удаляем старый constraint (если существует)
ALTER TABLE requests DROP CONSTRAINT IF EXISTS requests_status_check;

-- Добавляем новый constraint с расширенным списком статусов
ALTER TABLE requests ADD CONSTRAINT requests_status_check CHECK (
    status IN (
        'draft',
        'awaiting_photos',
        'collecting_info',
        'collecting_photos',
        'ready_to_generate',
        'in_queue',
        'queued',
        'generating',
        'generated_ok',
        'generated_error',
        'success',
        'error',
        'archived',
        'closed',
        'cancelled',
        'delivered'
    )
);

-- Создаём индекс для быстрого поиска по статусу
CREATE INDEX IF NOT EXISTS idx_requests_status_new ON requests(status);

COMMENT ON CONSTRAINT requests_status_check ON requests IS 'Допустимые статусы заявок';

