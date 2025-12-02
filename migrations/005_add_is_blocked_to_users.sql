-- =====================================================
-- Миграция: Добавление is_blocked в таблицу users
-- =====================================================

-- Добавляем колонку is_blocked напрямую в users
-- (раньше было в manager_settings, но код ожидает в users)
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_blocked BOOLEAN DEFAULT FALSE;

-- Индекс для быстрого поиска заблокированных
CREATE INDEX IF NOT EXISTS idx_users_is_blocked ON users(is_blocked) WHERE is_blocked = TRUE;

-- Мигрируем данные из manager_settings если они есть
UPDATE users u
SET is_blocked = ms.is_blocked
FROM manager_settings ms
WHERE ms.user_id = u.id AND ms.is_blocked = TRUE;

COMMENT ON COLUMN users.is_blocked IS 'Флаг блокировки пользователя';

