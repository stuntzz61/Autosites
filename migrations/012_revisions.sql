-- =====================================================
-- Migration: Revisions System
-- Система правок сайтов с интеграцией n8n
-- =====================================================

-- ==================== Revisions Table ====================
-- Основная таблица для хранения итераций правок
CREATE TABLE IF NOT EXISTS revisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Связи
    site_id UUID NOT NULL REFERENCES client_sites(id) ON DELETE CASCADE,
    request_id UUID REFERENCES requests(id) ON DELETE SET NULL,

    -- Номер итерации правок (1, 2, 3, ...)
    iteration INTEGER NOT NULL DEFAULT 1,

    -- Статусы: pending, in_progress, processing, completed, failed, cancelled
    status VARCHAR(50) NOT NULL DEFAULT 'pending',

    -- S3 хранилище
    s3_folder VARCHAR(500), -- sites/{site_id}/revisions/{iteration}/
    archive_s3_key VARCHAR(500), -- Путь к исходному архиву (до правок)
    result_archive_s3_key VARCHAR(500), -- Путь к результирующему архиву (после правок)

    -- n8n интеграция
    n8n_job_id VARCHAR(255), -- ID задачи в n8n для корреляции
    n8n_webhook_url VARCHAR(500), -- URL вебхука n8n куда отправили
    n8n_sent_at TIMESTAMP WITH TIME ZONE, -- Когда отправили в n8n
    n8n_response_at TIMESTAMP WITH TIME ZONE, -- Когда получили ответ

    -- Ошибки
    error_message TEXT,
    error_details JSONB,

    -- Метаданные
    source VARCHAR(50) DEFAULT 'telegram_bot', -- telegram_bot, webapp, api
    client_id VARCHAR(255), -- ID клиента в Telegram
    manager_id UUID REFERENCES users(id) ON DELETE SET NULL, -- Кто создал

    -- Временные метки
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,

    -- Уникальный индекс на site_id + iteration
    CONSTRAINT unique_site_iteration UNIQUE (site_id, iteration)
);

-- Индексы для revisions
CREATE INDEX IF NOT EXISTS idx_revisions_site_id ON revisions(site_id);
CREATE INDEX IF NOT EXISTS idx_revisions_status ON revisions(status);
CREATE INDEX IF NOT EXISTS idx_revisions_n8n_job_id ON revisions(n8n_job_id);
CREATE INDEX IF NOT EXISTS idx_revisions_created_at ON revisions(created_at DESC);

-- Комментарии
COMMENT ON TABLE revisions IS 'Итерации правок сайтов';
COMMENT ON COLUMN revisions.iteration IS 'Номер итерации правок (1, 2, 3, ...)';
COMMENT ON COLUMN revisions.s3_folder IS 'Путь к папке в S3 с файлами правок';
COMMENT ON COLUMN revisions.n8n_job_id IS 'ID задачи в n8n для корреляции запроса/ответа';

-- ==================== Revision Changes Table ====================
-- Отдельные правки внутри итерации
CREATE TABLE IF NOT EXISTS revision_changes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    revision_id UUID NOT NULL REFERENCES revisions(id) ON DELETE CASCADE,

    -- Тип правки: text_change, visual_change, layout_change, content_add, content_remove, style_change
    change_type VARCHAR(50) NOT NULL DEFAULT 'text_change',

    -- Локация элемента
    location_area VARCHAR(100), -- hero, header, footer, about, contacts, etc.
    location_selector VARCHAR(255), -- CSS селектор если есть
    location_description TEXT, -- Описание местоположения

    -- Описание правки от клиента
    client_description TEXT NOT NULL, -- "Не нравится текст внизу"

    -- Оригинальное и предлагаемое значение
    old_value TEXT, -- Текущий текст/значение если известно
    new_value_suggestion TEXT, -- Предлагаемое значение если есть

    -- Скриншот
    screenshot_s3_key VARCHAR(500), -- sites/{site_id}/revisions/{iteration}/screenshot_1.png
    screenshot_comment TEXT, -- Комментарий к скриншоту

    -- Приоритет и статус
    priority VARCHAR(20) DEFAULT 'normal', -- low, normal, high, critical
    status VARCHAR(50) DEFAULT 'pending', -- pending, applied, rejected, skipped

    -- AI-анализ (заполняется n8n/AI)
    ai_interpretation TEXT, -- Интерпретация AI что нужно сделать
    ai_confidence FLOAT, -- Уверенность AI (0-1)

    -- Метаданные
    metadata JSONB DEFAULT '{}',

    -- Временные метки
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Индексы для revision_changes
CREATE INDEX IF NOT EXISTS idx_revision_changes_revision_id ON revision_changes(revision_id);
CREATE INDEX IF NOT EXISTS idx_revision_changes_type ON revision_changes(change_type);
CREATE INDEX IF NOT EXISTS idx_revision_changes_status ON revision_changes(status);

-- Комментарии
COMMENT ON TABLE revision_changes IS 'Отдельные правки внутри итерации';
COMMENT ON COLUMN revision_changes.change_type IS 'Тип правки: text_change, visual_change, layout_change, etc.';
COMMENT ON COLUMN revision_changes.location_area IS 'Область сайта: hero, header, footer, about, etc.';

-- ==================== Revision History Table ====================
-- История изменений статусов правок
CREATE TABLE IF NOT EXISTS revision_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    revision_id UUID NOT NULL REFERENCES revisions(id) ON DELETE CASCADE,

    -- Изменение статуса
    old_status VARCHAR(50),
    new_status VARCHAR(50) NOT NULL,

    -- Кто изменил
    changed_by UUID REFERENCES users(id) ON DELETE SET NULL,
    change_source VARCHAR(50) DEFAULT 'system', -- system, user, n8n, webhook

    -- Дополнительная информация
    comment TEXT,
    metadata JSONB DEFAULT '{}',

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_revision_history_revision_id ON revision_history(revision_id);

-- ==================== Добавляем поля в client_sites ====================
-- Счётчик итераций правок
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'client_sites' AND column_name = 'revision_count'
    ) THEN
        ALTER TABLE client_sites ADD COLUMN revision_count INTEGER DEFAULT 0;
        COMMENT ON COLUMN client_sites.revision_count IS 'Количество итераций правок по сайту';
    END IF;
END $$;

-- Текущая активная ревизия
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'client_sites' AND column_name = 'current_revision_id'
    ) THEN
        ALTER TABLE client_sites ADD COLUMN current_revision_id UUID REFERENCES revisions(id) ON DELETE SET NULL;
        COMMENT ON COLUMN client_sites.current_revision_id IS 'ID текущей активной правки';
    END IF;
END $$;

-- Статус правок
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'client_sites' AND column_name = 'revision_status'
    ) THEN
        ALTER TABLE client_sites ADD COLUMN revision_status VARCHAR(50);
        COMMENT ON COLUMN client_sites.revision_status IS 'Статус текущей правки: pending, in_progress, completed';
    END IF;
END $$;

-- ==================== Views ====================

-- Представление для активных правок
CREATE OR REPLACE VIEW active_revisions AS
SELECT
    r.*,
    cs.company_name,
    cs.preview_url,
    cs.domain,
    cs.deploy_status,
    u.first_name as manager_first_name,
    u.last_name as manager_last_name,
    u.tg_id as manager_tg_id,
    (SELECT COUNT(*) FROM revision_changes WHERE revision_id = r.id) as changes_count
FROM revisions r
JOIN client_sites cs ON cs.id = r.site_id
LEFT JOIN users u ON u.id = r.manager_id
WHERE r.status IN ('pending', 'in_progress', 'processing')
ORDER BY r.created_at DESC;

COMMENT ON VIEW active_revisions IS 'Активные правки в работе';

-- Представление для статистики по правкам
CREATE OR REPLACE VIEW revision_stats AS
SELECT
    cs.id as site_id,
    cs.company_name,
    COUNT(r.id) as total_revisions,
    COUNT(r.id) FILTER (WHERE r.status = 'completed') as completed_revisions,
    COUNT(r.id) FILTER (WHERE r.status = 'failed') as failed_revisions,
    MAX(r.iteration) as last_iteration,
    MAX(r.completed_at) as last_revision_at
FROM client_sites cs
LEFT JOIN revisions r ON r.site_id = cs.id
GROUP BY cs.id, cs.company_name;

COMMENT ON VIEW revision_stats IS 'Статистика по правкам сайтов';

-- ==================== Функция для получения следующего номера итерации ====================
CREATE OR REPLACE FUNCTION get_next_revision_iteration(p_site_id UUID)
RETURNS INTEGER AS $$
DECLARE
    next_iteration INTEGER;
BEGIN
    SELECT COALESCE(MAX(iteration), 0) + 1 INTO next_iteration
    FROM revisions
    WHERE site_id = p_site_id;

    RETURN next_iteration;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_next_revision_iteration IS 'Возвращает следующий номер итерации для сайта';

-- ==================== Триггер для обновления updated_at ====================
CREATE OR REPLACE FUNCTION update_revision_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_revisions_updated_at ON revisions;
CREATE TRIGGER tr_revisions_updated_at
    BEFORE UPDATE ON revisions
    FOR EACH ROW
    EXECUTE FUNCTION update_revision_updated_at();

DROP TRIGGER IF EXISTS tr_revision_changes_updated_at ON revision_changes;
CREATE TRIGGER tr_revision_changes_updated_at
    BEFORE UPDATE ON revision_changes
    FOR EACH ROW
    EXECUTE FUNCTION update_revision_updated_at();

-- ==================== Триггер для обновления счётчика правок в client_sites ====================
CREATE OR REPLACE FUNCTION update_site_revision_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE client_sites
        SET revision_count = revision_count + 1,
            current_revision_id = NEW.id,
            revision_status = NEW.status
        WHERE id = NEW.site_id;
    ELSIF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
        UPDATE client_sites
        SET revision_status = NEW.status
        WHERE id = NEW.site_id AND current_revision_id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_update_site_revision_count ON revisions;
CREATE TRIGGER tr_update_site_revision_count
    AFTER INSERT OR UPDATE ON revisions
    FOR EACH ROW
    EXECUTE FUNCTION update_site_revision_count();

