-- =====================================================
-- Миграция: Добавление username для связи с менеджерами
-- =====================================================

-- Добавляем поле username
ALTER TABLE users ADD COLUMN IF NOT EXISTS username VARCHAR(100);

-- Индекс для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);

-- Функция для сохранения username при регистрации
-- (username будет обновляться автоматически при взаимодействии с ботом)

