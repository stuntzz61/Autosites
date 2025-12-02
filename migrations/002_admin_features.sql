-- =====================================================
-- Миграция: Админские функции и расширенные статусы
-- =====================================================

-- 1. Таблица логов активности
CREATE TABLE IF NOT EXISTS activity_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    action VARCHAR(50) NOT NULL,  -- 'request_created', 'request_updated', 'generation_started', etc.
    entity_type VARCHAR(30),      -- 'request', 'user', 'project'
    entity_id UUID,
    details JSONB DEFAULT '{}',
    ip_address VARCHAR(45),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_activity_log_user ON activity_log(user_id);
CREATE INDEX IF NOT EXISTS idx_activity_log_action ON activity_log(action);
CREATE INDEX IF NOT EXISTS idx_activity_log_created ON activity_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_activity_log_entity ON activity_log(entity_type, entity_id);

-- 2. Настройки менеджеров (блокировка, лимиты)
CREATE TABLE IF NOT EXISTS manager_settings (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    is_blocked BOOLEAN DEFAULT FALSE,
    block_reason TEXT,
    blocked_at TIMESTAMP,
    blocked_by UUID REFERENCES users(id),
    max_requests_per_day INTEGER DEFAULT 50,
    max_active_requests INTEGER DEFAULT 20,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 3. Архив заявок (для закрытых/завершённых)
CREATE TABLE IF NOT EXISTS request_archive (
    id UUID PRIMARY KEY,
    original_request_id UUID NOT NULL,
    project_id UUID,
    manager_id UUID,
    payload_json JSONB,
    status VARCHAR(30),
    result_url TEXT,           -- URL сгенерированного сайта
    archive_reason VARCHAR(50), -- 'completed', 'cancelled', 'expired'
    archived_by UUID REFERENCES users(id),
    archived_at TIMESTAMP DEFAULT NOW(),
    original_created_at TIMESTAMP,
    metadata JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_request_archive_manager ON request_archive(manager_id);
CREATE INDEX IF NOT EXISTS idx_request_archive_status ON request_archive(status);
CREATE INDEX IF NOT EXISTS idx_request_archive_date ON request_archive(archived_at DESC);

-- 4. Добавляем поля в requests для отслеживания генерации
ALTER TABLE requests ADD COLUMN IF NOT EXISTS generation_started_at TIMESTAMP;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS generation_completed_at TIMESTAMP;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS result_url TEXT;
ALTER TABLE requests ADD COLUMN IF NOT EXISTS error_message TEXT;

-- 5. Статистика по дням (материализованное представление для быстрых отчётов)
CREATE TABLE IF NOT EXISTS daily_stats (
    stat_date DATE PRIMARY KEY,
    total_requests INTEGER DEFAULT 0,
    completed_requests INTEGER DEFAULT 0,
    failed_requests INTEGER DEFAULT 0,
    new_users INTEGER DEFAULT 0,
    active_managers INTEGER DEFAULT 0,
    photos_uploaded INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- 6. Функция для подсчёта статистики менеджера
CREATE OR REPLACE FUNCTION get_manager_stats(manager_uuid UUID)
RETURNS TABLE (
    total_requests BIGINT,
    pending_requests BIGINT,
    completed_requests BIGINT,
    failed_requests BIGINT,
    total_photos BIGINT,
    avg_completion_time INTERVAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*)::BIGINT as total_requests,
        COUNT(*) FILTER (WHERE r.status IN ('draft', 'collecting_info', 'collecting_photos', 'ready_to_generate', 'queued'))::BIGINT as pending_requests,
        COUNT(*) FILTER (WHERE r.status = 'generated_ok')::BIGINT as completed_requests,
        COUNT(*) FILTER (WHERE r.status = 'generated_error')::BIGINT as failed_requests,
        COALESCE(SUM(jsonb_array_length(COALESCE(r.payload_json->'site'->'assets'->'images', '[]'::jsonb))), 0)::BIGINT as total_photos,
        AVG(r.generation_completed_at - r.generation_started_at) FILTER (WHERE r.generation_completed_at IS NOT NULL) as avg_completion_time
    FROM requests r
    JOIN projects p ON p.id = r.project_id
    WHERE p.manager_id = manager_uuid;
END;
$$ LANGUAGE plpgsql;

-- 7. Функция для общей статистики
CREATE OR REPLACE FUNCTION get_overall_stats()
RETURNS TABLE (
    total_users BIGINT,
    total_managers BIGINT,
    total_requests BIGINT,
    requests_today BIGINT,
    requests_this_week BIGINT,
    requests_this_month BIGINT,
    pending_generation BIGINT,
    completed_today BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        (SELECT COUNT(*) FROM users)::BIGINT,
        (SELECT COUNT(*) FROM users WHERE role = 'manager')::BIGINT,
        (SELECT COUNT(*) FROM requests)::BIGINT,
        (SELECT COUNT(*) FROM requests WHERE created_at >= CURRENT_DATE)::BIGINT,
        (SELECT COUNT(*) FROM requests WHERE created_at >= CURRENT_DATE - INTERVAL '7 days')::BIGINT,
        (SELECT COUNT(*) FROM requests WHERE created_at >= CURRENT_DATE - INTERVAL '30 days')::BIGINT,
        (SELECT COUNT(*) FROM requests WHERE status IN ('queued', 'generating'))::BIGINT,
        (SELECT COUNT(*) FROM requests WHERE status = 'generated_ok' AND generation_completed_at >= CURRENT_DATE)::BIGINT;
END;
$$ LANGUAGE plpgsql;

-- 8. Триггер для автообновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_manager_settings_updated_at ON manager_settings;
CREATE TRIGGER update_manager_settings_updated_at
    BEFORE UPDATE ON manager_settings
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 9. Вьюха для топ менеджеров
CREATE OR REPLACE VIEW top_managers AS
SELECT
    u.id,
    u.tg_id,
    u.first_name,
    u.last_name,
    u.contact,
    u.created_at as registered_at,
    COUNT(r.id) as total_requests,
    COUNT(r.id) FILTER (WHERE r.status = 'generated_ok') as completed,
    COUNT(r.id) FILTER (WHERE r.status = 'generated_error') as failed,
    COUNT(r.id) FILTER (WHERE r.status IN ('draft', 'collecting_info', 'collecting_photos', 'ready_to_generate', 'queued')) as in_progress,
    COALESCE(ms.is_blocked, false) as is_blocked
FROM users u
LEFT JOIN projects p ON p.manager_id = u.id
LEFT JOIN requests r ON r.project_id = p.id
LEFT JOIN manager_settings ms ON ms.user_id = u.id
WHERE u.role = 'manager'
GROUP BY u.id, u.tg_id, u.first_name, u.last_name, u.contact, u.created_at, ms.is_blocked
ORDER BY total_requests DESC;

-- 10. Индексы для производительности
CREATE INDEX IF NOT EXISTS idx_requests_status ON requests(status);
CREATE INDEX IF NOT EXISTS idx_requests_created ON requests(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_requests_generation ON requests(generation_started_at, generation_completed_at);

COMMENT ON TABLE activity_log IS 'Лог всех действий пользователей для аудита';
COMMENT ON TABLE manager_settings IS 'Настройки и ограничения для менеджеров';
COMMENT ON TABLE request_archive IS 'Архив завершённых/закрытых заявок';
COMMENT ON TABLE daily_stats IS 'Ежедневная агрегированная статистика';

