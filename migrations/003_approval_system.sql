-- =====================================================
-- Миграция: Система одобрения регистраций
-- =====================================================

-- 1. Добавляем статус одобрения в users
ALTER TABLE users ADD COLUMN IF NOT EXISTS approval_status VARCHAR(20) DEFAULT 'pending';
-- Возможные значения: 'pending', 'approved', 'rejected'

ALTER TABLE users ADD COLUMN IF NOT EXISTS approved_by UUID REFERENCES users(id);
ALTER TABLE users ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP;
ALTER TABLE users ADD COLUMN IF NOT EXISTS rejection_reason TEXT;

-- 2. Обновляем существующих пользователей как одобренных
UPDATE users SET approval_status = 'approved' WHERE approval_status IS NULL OR approval_status = 'pending';

-- 3. Таблица для уведомлений админам
CREATE TABLE IF NOT EXISTS admin_notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_type VARCHAR(50) NOT NULL, -- 'new_registration', 'generation_complete', etc.
    title TEXT NOT NULL,
    message TEXT,
    entity_type VARCHAR(30),
    entity_id UUID,
    is_read BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_admin_notifications_unread ON admin_notifications(is_read, created_at DESC);

-- 4. Функция для подсчёта ожидающих одобрения
CREATE OR REPLACE FUNCTION count_pending_approvals()
RETURNS BIGINT AS $$
BEGIN
    RETURN (SELECT COUNT(*) FROM users WHERE approval_status = 'pending' AND role = 'manager');
END;
$$ LANGUAGE plpgsql;

-- 5. Список ожидающих одобрения
CREATE OR REPLACE VIEW pending_registrations AS
SELECT
    u.id,
    u.tg_id,
    u.first_name,
    u.last_name,
    u.contact,
    u.created_at,
    u.approval_status
FROM users u
WHERE u.approval_status = 'pending'
  AND u.role IN ('manager', 'guest')
ORDER BY u.created_at ASC;

-- 6. Индексы
CREATE INDEX IF NOT EXISTS idx_users_approval ON users(approval_status);

-- 7. Сохраняем tg_id админа для уведомлений (из конфига)
CREATE TABLE IF NOT EXISTS system_settings (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE admin_notifications IS 'Уведомления для администраторов';
COMMENT ON COLUMN users.approval_status IS 'Статус одобрения: pending, approved, rejected';

