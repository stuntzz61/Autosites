-- =====================================================
-- Миграция: Дополнительные услуги и обратная связь
-- =====================================================

-- 1. Таблица справочника дополнительных услуг
CREATE TABLE IF NOT EXISTS additional_services (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code VARCHAR(50) UNIQUE NOT NULL,  -- 'logo_design', 'seo_promotion', 'business_automation'
    name VARCHAR(255) NOT NULL,         -- Название услуги
    description TEXT,                    -- Описание
    price_info VARCHAR(255),            -- Информация о цене
    icon VARCHAR(50),                   -- Иконка (emoji или имя иконки)
    is_active BOOLEAN DEFAULT TRUE,
    sort_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Добавляем стандартные услуги
INSERT INTO additional_services (code, name, description, price_info, icon, sort_order) VALUES
    ('logo_design', 'Разработка логотипа компании', 'Профессиональный дизайн логотипа с несколькими вариантами на выбор', 'По договорённости', '🎨', 1),
    ('seo_promotion', 'SEO продвижение', 'Комплексное продвижение сайта в поисковых системах', 'По договорённости', '📈', 2),
    ('business_automation', 'Автоматизация для бизнеса', 'Внедрение CRM, чат-ботов, интеграций с сервисами', 'По договорённости', '⚙️', 3)
ON CONFLICT (code) DO NOTHING;

-- 2. Связь заявок с дополнительными услугами
CREATE TABLE IF NOT EXISTS request_additional_services (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_id UUID NOT NULL REFERENCES requests(id) ON DELETE CASCADE,
    service_id UUID NOT NULL REFERENCES additional_services(id) ON DELETE CASCADE,
    status VARCHAR(30) DEFAULT 'pending',  -- 'pending', 'in_progress', 'completed', 'cancelled'
    notes TEXT,                            -- Примечания менеджера
    added_by UUID REFERENCES users(id),    -- Кто добавил
    price VARCHAR(100),                    -- Согласованная цена
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(request_id, service_id)
);

CREATE INDEX IF NOT EXISTS idx_request_services_request ON request_additional_services(request_id);
CREATE INDEX IF NOT EXISTS idx_request_services_service ON request_additional_services(service_id);
CREATE INDEX IF NOT EXISTS idx_request_services_status ON request_additional_services(status);

-- 3. Таблица обратной связи менеджер -> админ
CREATE TABLE IF NOT EXISTS manager_feedback (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    manager_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    subject VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    category VARCHAR(50) DEFAULT 'general',  -- 'general', 'bug', 'feature', 'complaint', 'question'
    priority VARCHAR(20) DEFAULT 'normal',   -- 'low', 'normal', 'high', 'urgent'
    status VARCHAR(30) DEFAULT 'new',        -- 'new', 'in_review', 'answered', 'closed'
    request_id UUID REFERENCES requests(id) ON DELETE SET NULL,  -- Связь с заявкой (опционально)
    admin_response TEXT,                     -- Ответ админа
    responded_by UUID REFERENCES users(id),
    responded_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_feedback_manager ON manager_feedback(manager_id);
CREATE INDEX IF NOT EXISTS idx_feedback_status ON manager_feedback(status);
CREATE INDEX IF NOT EXISTS idx_feedback_created ON manager_feedback(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_feedback_priority ON manager_feedback(priority);

-- 4. Триггер для updated_at
CREATE TRIGGER update_additional_services_updated_at
    BEFORE UPDATE ON additional_services
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_request_services_updated_at
    BEFORE UPDATE ON request_additional_services
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_manager_feedback_updated_at
    BEFORE UPDATE ON manager_feedback
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 5. Функция для подсчёта непрочитанных обращений
CREATE OR REPLACE FUNCTION count_new_feedback()
RETURNS BIGINT AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM manager_feedback WHERE status = 'new');
END;
$$ LANGUAGE plpgsql;

-- 6. Вьюха для обращений с информацией о менеджере
CREATE OR REPLACE VIEW feedback_with_manager AS
SELECT
    f.id,
    f.subject,
    f.message,
    f.category,
    f.priority,
    f.status,
    f.request_id,
    f.admin_response,
    f.responded_at,
    f.created_at,
    f.updated_at,
    u.id as manager_id,
    u.tg_id as manager_tg_id,
    u.first_name as manager_first_name,
    u.last_name as manager_last_name,
    u.username as manager_username,
    ru.first_name as responder_first_name,
    ru.last_name as responder_last_name
FROM manager_feedback f
JOIN users u ON u.id = f.manager_id
LEFT JOIN users ru ON ru.id = f.responded_by
ORDER BY
    CASE f.status WHEN 'new' THEN 0 ELSE 1 END,
    CASE f.priority
        WHEN 'urgent' THEN 0
        WHEN 'high' THEN 1
        WHEN 'normal' THEN 2
        ELSE 3
    END,
    f.created_at DESC;

-- Комментарии
COMMENT ON TABLE additional_services IS 'Справочник дополнительных услуг';
COMMENT ON TABLE request_additional_services IS 'Связь заявок с дополнительными услугами';
COMMENT ON TABLE manager_feedback IS 'Обратная связь от менеджеров к администраторам';

